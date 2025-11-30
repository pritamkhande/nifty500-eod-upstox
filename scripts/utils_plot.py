import os
from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio


def make_equity_and_dd_plots(
    df: pd.DataFrame,
    date_col: str,
    equity_col: str,
    out_equity_png: str,
    out_dd_png: str,
) -> None:
    os.makedirs(os.path.dirname(out_equity_png), exist_ok=True)

    eq_series = df[[date_col, equity_col]].dropna()
    if eq_series.empty:
        return

    dates = eq_series[date_col].values
    equity = eq_series[equity_col].values

    # Equity curve
    plt.figure(figsize=(8, 4))
    plt.plot(dates, equity)
    plt.title("Equity Curve (normalized)")
    plt.xlabel("Date")
    plt.ylabel("Equity")
    plt.tight_layout()
    plt.savefig(out_equity_png, dpi=120)
    plt.close()

    # Drawdown curve
    peaks = np.maximum.accumulate(equity)
    dd = (equity - peaks) / peaks

    plt.figure(figsize=(8, 4))
    plt.plot(dates, dd)
    plt.title("Drawdown")
    plt.xlabel("Date")
    plt.ylabel("Drawdown")
    plt.tight_layout()
    plt.savefig(out_dd_png, dpi=120)
    plt.close()


def _save_fig_html(fig: go.Figure, out_path: str) -> None:
    """
    Helper: save Plotly figure as standalone HTML using to_html().
    This avoids any signature issues with plotly.io.write_html.
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    html_str = pio.to_html(fig, include_plotlyjs="cdn", full_html=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html_str)


def generate_trade_charts(
    price_df: pd.DataFrame,
    trades_df: pd.DataFrame,
    date_col: str,
    open_col: str,
    high_col: str,
    low_col: str,
    close_col: str,
    out_dir: str = "docs/trades",
) -> None:
    """
    Per-trade charts:
    one HTML candlestick chart per trade with Signal / Entry / Exit markers.
    """
    if trades_df.empty:
        return

    os.makedirs(out_dir, exist_ok=True)

    for _, tr in trades_df.iterrows():
        trade_no = int(tr["trade_no"])
        sig_idx = int(tr["signal_index"])
        entry_idx = int(tr["entry_index"])
        exit_idx = int(tr["exit_index"])

        start_idx = max(0, sig_idx - 30)
        end_idx = min(len(price_df) - 1, exit_idx + 10)

        slice_df = price_df.loc[start_idx:end_idx].copy()
        slice_df = slice_df.reset_index(drop=True)

        # Map global indices to local within slice
        def to_local_idx(global_idx: int) -> Optional[int]:
            if global_idx < start_idx or global_idx > end_idx:
                return None
            return global_idx - start_idx

        local_sig = to_local_idx(sig_idx)
        local_entry = to_local_idx(entry_idx)
        local_exit = to_local_idx(exit_idx)

        fig = go.Figure(
            data=[
                go.Candlestick(
                    x=slice_df[date_col],
                    open=slice_df[open_col],
                    high=slice_df[high_col],
                    low=slice_df[low_col],
                    close=slice_df[close_col],
                    name="Price",
                )
            ]
        )

        # Signal (square)
        if local_sig is not None:
            fig.add_trace(
                go.Scatter(
                    x=[slice_df.loc[local_sig, date_col]],
                    y=[slice_df.loc[local_sig, close_col]],
                    mode="markers+text",
                    text=["Square"],
                    textposition="top center",
                    name="Square",
                )
            )

        # Entry
        if local_entry is not None:
            fig.add_trace(
                go.Scatter(
                    x=[slice_df.loc[local_entry, date_col]],
                    y=[slice_df.loc[local_entry, close_col]],
                    mode="markers+text",
                    text=["Entry"],
                    textposition="bottom center",
                    name="Entry",
                )
            )

        # Exit
        if local_exit is not None:
            fig.add_trace(
                go.Scatter(
                    x=[slice_df.loc[local_exit, date_col]],
                    y=[slice_df.loc[local_exit, close_col]],
                    mode="markers+text",
                    text=["Exit"],
                    textposition="bottom center",
                    name="Exit",
                )
            )

        fig.update_layout(
            title=f"Trade #{trade_no}",
            xaxis_title="Date",
            yaxis_title="Price",
            xaxis_rangeslider_visible=False,
        )

        out_path = os.path.join(out_dir, f"trade_{trade_no:03d}.html")
        _save_fig_html(fig, out_path)


def generate_all_trades_chart(
    price_df: pd.DataFrame,
    trades_df: pd.DataFrame,
    date_col: str,
    open_col: str,
    high_col: str,
    low_col: str,
    close_col: str,
    out_html: str,
) -> None:
    """
    Single combined interactive chart for ALL trades of one symbol.

    Background: full candlestick for entire history.
    Markers:
      * Signal (Square) at signal_index
      * Entry at entry_index
      * Exit at exit_index
    """
    if trades_df.empty or price_df.empty:
        return

    os.makedirs(os.path.dirname(out_html), exist_ok=True)

    # Base candlestick for entire price history
    fig = go.Figure(
        data=[
            go.Candlestick(
                x=price_df[date_col],
                open=price_df[open_col],
                high=price_df[high_col],
                low=price_df[low_col],
                close=price_df[close_col],
                name="Price",
            )
        ]
    )

    # Collect markers in 3 traces for performance
    square_x, square_y = [], []
    entry_x, entry_y = [], []
    exit_x, exit_y = [], []

    for _, tr in trades_df.iterrows():
        sig_idx = int(tr["signal_index"])
        entry_idx = int(tr["entry_index"])
        exit_idx = int(tr["exit_index"])

        # Bounds check
        if 0 <= sig_idx < len(price_df):
            square_x.append(price_df.loc[sig_idx, date_col])
            square_y.append(price_df.loc[sig_idx, close_col])

        if 0 <= entry_idx < len(price_df):
            entry_x.append(price_df.loc[entry_idx, date_col])
            entry_y.append(price_df.loc[entry_idx, close_col])

        if 0 <= exit_idx < len(price_df):
            exit_x.append(price_df.loc[exit_idx, date_col])
            exit_y.append(price_df.loc[exit_idx, close_col])

    if square_x:
        fig.add_trace(
            go.Scatter(
                x=square_x,
                y=square_y,
                mode="markers",
                marker=dict(symbol="triangle-up", size=9, color="yellow"),
                name="Signal (Square)",
            )
        )

    if entry_x:
        fig.add_trace(
            go.Scatter(
                x=entry_x,
                y=entry_y,
                mode="markers",
                marker=dict(symbol="circle", size=8, color="lime"),
                name="Entry",
            )
        )

    if exit_x:
        fig.add_trace(
            go.Scatter(
                x=exit_x,
                y=exit_y,
                mode="markers",
                marker=dict(symbol="x", size=8, color="red"),
                name="Exit",
            )
        )

    fig.update_layout(
        title="All Trades – Combined View",
        xaxis_title="Date",
        yaxis_title="Price",
        xaxis_rangeslider_visible=True,  # horizontal scroll / zoom
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1.0,
        ),
    )

    _save_fig_html(fig, out_html)
