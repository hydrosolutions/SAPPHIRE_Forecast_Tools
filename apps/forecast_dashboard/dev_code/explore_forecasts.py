"""Marimo notebook for interactive forecast data exploration.

Launch with:
    cd apps/forecast_dashboard
    uv run marimo edit dev_code/explore_forecasts.py
"""

import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import sys
    from pathlib import Path

    import marimo as mo
    import matplotlib.pyplot as plt
    import pandas as pd

    sys.path.insert(0, str(Path(__file__).parent))
    from fetch_data import fetch_forecasts, fetch_lr_forecasts, insert_gap_nans

    return fetch_forecasts, fetch_lr_forecasts, insert_gap_nans, mo, pd, plt


@app.cell
def _(mo):
    mo.md("""
    # Forecast Explorer
    """)
    return


@app.cell
def _(mo):
    api_url = mo.ui.text(value="http://localhost:8000/api", label="API URL")
    station = mo.ui.text(value="15102", label="Station code")
    horizon = mo.ui.dropdown(options=["pentad", "decade"], value="pentad", label="Horizon")
    start_date = mo.ui.text(value="2025-06-01", label="Start date")
    end_date = mo.ui.text(value="2026-03-01", label="End date")

    mo.hstack([api_url, station, horizon, start_date, end_date])
    return api_url, end_date, horizon, start_date, station


@app.cell
def _(
    api_url,
    end_date,
    fetch_forecasts,
    fetch_lr_forecasts,
    horizon,
    mo,
    pd,
    start_date,
    station,
):
    # ML / ensemble forecasts from the combined forecasts table
    df = fetch_forecasts(
        api_url.value,
        station.value,
        horizon.value,
        start_date.value,
        end_date.value,
    )

    # LR forecasts from the dedicated lr-forecast table
    try:
        lr = fetch_lr_forecasts(
            api_url.value,
            station.value,
            horizon.value,
            start_date.value,
            end_date.value,
        )
        if not lr.empty:
            common = [c for c in df.columns if c in lr.columns]
            df = pd.concat([df, lr[common]], ignore_index=True)
    except Exception:
        pass

    mo.md(f"**{len(df)} rows**, models: {sorted(df['model_short'].unique())}")
    return (df,)


@app.cell
def _(df, mo):
    model_filter = mo.ui.multiselect(
        options=sorted(df["model_short"].unique().tolist()),
        value=sorted(df["model_short"].unique().tolist()),
        label="Models",
    )
    model_filter  # noqa: B018 — marimo display expression
    return (model_filter,)


@app.cell
def _(df, horizon, insert_gap_nans, mo, model_filter, pd, plt):
    filtered = df[df["model_short"].isin(model_filter.value)]

    fig, ax = plt.subplots(figsize=(12, 6))
    colors = plt.cm.tab10.colors

    for i, model in enumerate(sorted(filtered["model_short"].unique())):
        subset = filtered[filtered["model_short"] == model].sort_values("date")
        subset = insert_gap_nans(subset, horizon=horizon.value)
        color = colors[i % len(colors)]
        eq = pd.to_numeric(subset["E[Q]"], errors="coerce")
        ax.plot(subset["date"], eq, label=model, color=color, marker=".", markersize=4)
        if "Q5" in subset.columns and "Q95" in subset.columns:
            q5 = pd.to_numeric(subset["Q5"], errors="coerce")
            q95 = pd.to_numeric(subset["Q95"], errors="coerce")
            ax.fill_between(subset["date"], q5, q95, alpha=0.15, color=color)

    ax.set_xlabel("Date")
    ax.set_ylabel("Discharge (m³/s)")
    ax.set_title("Forecast Time Series")
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()

    mo.md("## Forecast Time Series")
    fig  # noqa: B018 — marimo display expression
    return (filtered,)


@app.cell
def _(filtered, mo):
    mo.md("## Raw Data")
    mo.ui.table(filtered)
    return


if __name__ == "__main__":
    app.run()
