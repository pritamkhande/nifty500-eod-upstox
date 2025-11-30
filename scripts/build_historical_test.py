import os
import json
from glob import glob

import pandas as pd

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GANN_DIR = os.path.join(REPO_ROOT, "data", "gann_trades")
OUTFILE = os.path.join(REPO_ROOT, "docs", "historical-test-data.json")


def compute_metrics(df: pd.DataFrame) -> dict | None:
    if df.empty or "R" not in df.columns:
        return None

    df = df.copy()

    # Basic stats
    total_trades = len(df)
    wins = (df["R"] > 0).sum()
    winrate = 100.0 * wins / total_trades if total_trades else 0.0
    avg_r = df["R"].mean() if total_trades else 0.0

    gross_win = df.loc[df["R"] > 0, "R"].sum()
    gross_loss = -df.loc[df["R"] < 0, "R"].sum()  # negative R
    profit_factor = (gross_win / gross_loss) if gross_loss > 0 else 999.0

    # Max drawdown on cumulative R
    cum = df["R"].cumsum()
    peak = cum.cummax()
    dd = (cum - peak)
    max_dd = float(dd.min() * 100.0) if len(dd) else 0.0

    # Date-based metrics
    if "signal_date" in df.columns:
        df["signal_date"] = pd.to_datetime(df["signal_date"], errors="coerce")
    elif "entry_date" in df.columns:
        df["signal_date"] = pd.to_datetime(df["entry_date"], errors="coerce")
    else:
        df["signal_date"] = pd.NaT

    df = df.dropna(subset=["signal_date"])
    if df.empty:
        last_1 = 0.0
        last_3 = 0.0
    else:
        today = pd.Timestamp.today().normalize()
        one_year_ago = today - pd.DateOffset(years=1)
        three_years_ago = today - pd.DateOffset(years=3)

        last1 = df[df["signal_date"] >= one_year_ago]
        last3 = df[df["signal_date"] >= three_years_ago]

        def wr(d: pd.DataFrame) -> float:
            return float((d["R"] > 0).mean() * 100.0) if not d.empty else 0.0

        last_1 = wr(last1)
        last_3 = wr(last3)

    return {
        "total_trades": int(total_trades),
        "winrate": round(winrate, 2),
        "avg_r": round(avg_r, 4),
        "profit_factor": round(profit_factor, 2),
        "max_dd": round(max_dd, 2),
        "last_1_year_winrate": round(last_1, 2),
        "last_3_year_winrate": round(last_3, 2),
    }


def main() -> None:
    if not os.path.isdir(GANN_DIR):
        print("No gann_trades directory:", GANN_DIR)
        return

    results = []

    pattern = os.path.join(GANN_DIR, "*_gann_trades.csv")
    for file in sorted(glob(pattern)):
        base = os.path.basename(file)
        symbol = base.replace("_gann_trades.csv", "")
        print("Historical metrics for", symbol)

        try:
            df = pd.read_csv(file)
        except Exception as e:
            print("  Failed to read:", e)
            continue

        metrics = compute_metrics(df)
        if not metrics:
            continue

        metrics["symbol"] = symbol
        metrics["link"] = f"stocks/{symbol}/index.html"
        results.append(metrics)

    os.makedirs(os.path.dirname(OUTFILE), exist_ok=True)
    with open(OUTFILE, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print("Saved historical metrics to", OUTFILE)


if __name__ == "__main__":
    main()
