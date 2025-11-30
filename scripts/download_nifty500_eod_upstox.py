import os
import time
from datetime import datetime, date, timedelta

import pandas as pd
import requests
from requests.exceptions import RequestException

# ============= USER / ENV SETTINGS =============

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

NIFTY500_LIST_FILE = os.path.join(REPO_ROOT, "data", "nifty500_list.csv")
EOD_ROOT = os.path.join(REPO_ROOT, "data", "eod")

BASE_URL_V3 = "https://api.upstox.com/v3"

# Access token from environment (GitHub secret)
ACCESS_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN")

MAX_RETRIES = 3

# Global earliest date (for first ever run)
GLOBAL_START_DATE = "2000-01-01"

# ===============================================


def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path)


def load_nifty500_list(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "ISIN" not in df.columns or "TckrSymb" not in df.columns:
        raise ValueError("nifty500_list.csv must have columns 'ISIN' and 'TckrSymb'")
    df = df[["ISIN", "TckrSymb"]].dropna()
    df["ISIN"] = df["ISIN"].astype(str).str.strip()
    df["TckrSymb"] = df["TckrSymb"].astype(str).str.strip()
    return df


def get_symbol_eod_path(symbol: str) -> str:
    """Return full path for symbol's EOD CSV, grouped by first letter."""
    first_char = symbol[0].upper() if symbol else "_"
    if not first_char.isalpha():
        first_char = "0-9"
    subdir = os.path.join(EOD_ROOT, first_char)
    ensure_dir(subdir)
    return os.path.join(subdir, f"{symbol}_EOD.csv")


def get_existing_last_date(symbol: str) -> date | None:
    """If symbol has a CSV, return last Date as a date object."""
    path = get_symbol_eod_path(symbol)
    if not os.path.exists(path):
        return None

    df = pd.read_csv(path)
    if "Date" not in df.columns or df.empty:
        return None

    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    return df["Date"].max()


def generate_date_windows(from_date_str: str, to_date_str: str, max_year_span: int = 10):
    """
    Split [from_date, to_date] into windows of at most `max_year_span` years.
    Needed because Upstox 'days' unit is limited to max 1 decade per request.
    """
    start = datetime.strptime(from_date_str, "%Y-%m-%d").date()
    end = datetime.strptime(to_date_str, "%Y-%m-%d").date()

    windows = []
    cur_start = start

    while cur_start <= end:
        next_start_year = cur_start.year + max_year_span
        try:
            next_start = date(next_start_year, cur_start.month, cur_start.day)
        except ValueError:
            next_start = date(next_start_year, cur_start.month, 28)

        cur_end = min(end, next_start - timedelta(days=1))
        windows.append((cur_start.isoformat(), cur_end.isoformat()))
        cur_start = next_start

    return windows


def fetch_candles_for_key(instrument_key: str, from_date: str, to_date: str) -> pd.DataFrame:
    """
    Call Upstox historical-candle endpoint for given instrument_key and date range,
    splitting into 10-year windows.
    /historical-candle/:instrument_key/days/1/:to_date/:from_date
    """
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {ACCESS_TOKEN}",
    }

    all_parts = []
    windows = generate_date_windows(from_date, to_date)  # list of (from, to)

    for win_from, win_to in windows:
        url = (
            f"{BASE_URL_V3}/historical-candle/"
            f"{instrument_key}/days/1/{win_to}/{win_from}"
        )

        last_exc = None
        success = False

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(url, headers=headers, timeout=60)
                if resp.status_code != 200:
                    last_exc = RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
                else:
                    data = resp.json()
                    inner = data.get("data") or {}
                    candles = inner.get("candles")

                    if not candles:
                        # request OK but no data for this window
                        success = True
                        break

                    df_part = pd.DataFrame(
                        candles,
                        columns=[
                            "timestamp", "Open", "High", "Low",
                            "Close", "Volume", "OI"
                        ][:len(candles[0])]
                    )
                    df_part["timestamp"] = pd.to_datetime(df_part["timestamp"])
                    df_part.rename(columns={"timestamp": "Date"}, inplace=True)
                    all_parts.append(df_part)
                    success = True
                    break
            except RequestException as e:
                last_exc = e

            time.sleep(1)

        if not success and last_exc:
            print(f"[WARN] {instrument_key} window {win_from}->{win_to} error: {last_exc}")

    if not all_parts:
        return pd.DataFrame()

    df_all = pd.concat(all_parts, ignore_index=True)
    df_all = df_all.drop_duplicates(subset=["Date"]).sort_values("Date")
    return df_all


def merge_with_existing(symbol: str, df_new: pd.DataFrame) -> pd.DataFrame:
    """
    Merge newly fetched DF with existing CSV (if any) and return full DF.
    """
    path = get_symbol_eod_path(symbol)
    if os.path.exists(path):
        df_old = pd.read_csv(path)
        if "Date" in df_old.columns:
            df_old["Date"] = pd.to_datetime(df_old["Date"])
        else:
            df_old["Date"] = pd.NaT
    else:
        df_old = pd.DataFrame(columns=["Date", "Open", "High", "Low", "Close", "Volume"])

    for col in ["Date", "Open", "High", "Low", "Close", "Volume"]:
        if col not in df_new.columns:
            df_new[col] = pd.NA

    df = pd.concat([df_old, df_new], ignore_index=True)
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.drop_duplicates(subset=["Date"]).sort_values("Date")

    return df


def save_symbol_data(symbol: str, df: pd.DataFrame):
    """
    Save DataFrame to CSV with columns: Symbol, Date, Open, High, Low, Close, Volume
    in grouped subfolders.
    """
    cols = ["Date", "Open", "High", "Low", "Close", "Volume"]
    cols = [c for c in cols if c in df.columns]
    df = df[cols]
    df.insert(0, "Symbol", symbol)

    out_path = get_symbol_eod_path(symbol)
    df.to_csv(out_path, index=False)


def main():
    if not ACCESS_TOKEN:
        raise RuntimeError("UPSTOX_ACCESS_TOKEN is not set in environment.")

    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    print("Today (UTC):", today_str)

    nifty_df = load_nifty500_list(NIFTY500_LIST_FILE)
    print(f"Total Nifty500 symbols: {len(nifty_df)}")

    ensure_dir(EOD_ROOT)

    for idx, row in nifty_df.iterrows():
        isin = row["ISIN"]
        symbol = row["TckrSymb"]
        instrument_key = f"NSE_EQ|{isin}"

        print(f"\n[{idx+1}/{len(nifty_df)}] {symbol} ({instrument_key})")

        last_date = get_existing_last_date(symbol)
        if last_date:
            from_date = (last_date + timedelta(days=1)).isoformat()
        else:
            from_date = GLOBAL_START_DATE

        to_date = today_str

        if from_date > to_date:
            print(f"  Up to date already (last_date={last_date}). Skipping.")
            continue

        print(f"  Fetching from {from_date} to {to_date}...")
        df_new = fetch_candles_for_key(instrument_key, from_date, to_date)
        if df_new.empty:
            print("  No new data returned.")
            continue

        df_full = merge_with_existing(symbol, df_new)
        save_symbol_data(symbol, df_full)
        print("  Saved EOD data.")


if __name__ == "__main__":
    main()
