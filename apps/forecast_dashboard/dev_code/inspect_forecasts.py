#!/usr/bin/env python3
"""CLI tool for inspecting forecast data from the SAPPHIRE API.

Produces matplotlib plots:
  1. Forecast time series — forecasted discharge per model with quantile bands
  2. Model comparison — bar chart of forecast values across models for a date
  3. Forecast vs observed — scatter (when observed data available)

Usage:
    cd apps/forecast_dashboard
    uv run python dev_code/inspect_forecasts.py \
        --station 15102 --horizon pentad \
        --start-date 2025-06-01 --end-date 2026-03-01 \
        --models LR TFT NE \
        --output-dir /tmp/forecast_plots/

    # Interactive display (no file save):
    uv run python dev_code/inspect_forecasts.py \
        --station 15102 --horizon pentad --show
"""

import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

# Allow importing fetch_data from the same directory
sys.path.insert(0, str(Path(__file__).parent))
from fetch_data import fetch_forecasts, fetch_lr_forecasts, insert_gap_nans

DEFAULT_API = "http://localhost:8000/api"


def plot_forecast_time_series(df: pd.DataFrame, station: str, horizon: str = "pentad", ax=None):
    """Plot forecasted discharge over time per model with quantile bands."""
    if ax is None:
        _, ax = plt.subplots(figsize=(12, 6))

    models = df["model_short"].unique()
    colors = plt.cm.tab10.colors

    for i, model in enumerate(sorted(models)):
        subset = df[df["model_short"] == model].sort_values("date")
        subset = insert_gap_nans(subset, horizon=horizon)
        color = colors[i % len(colors)]

        ax.plot(
            subset["date"],
            subset["E[Q]"],
            label=model,
            color=color,
            marker=".",
            markersize=4,
        )

        # Quantile bands (if available)
        if "Q5" in subset.columns and "Q95" in subset.columns:
            q5 = pd.to_numeric(subset["Q5"], errors="coerce")
            q95 = pd.to_numeric(subset["Q95"], errors="coerce")
            ax.fill_between(
                subset["date"],
                q5,
                q95,
                alpha=0.15,
                color=color,
            )

    ax.set_xlabel("Date")
    ax.set_ylabel("Discharge (m³/s)")
    ax.set_title(f"Forecast Time Series — Station {station}")
    ax.legend(loc="best", fontsize=8)
    ax.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    return ax


def plot_model_comparison(df: pd.DataFrame, station: str, ax=None):
    """Bar chart of forecast values across models for the latest date."""
    if ax is None:
        _, ax = plt.subplots(figsize=(8, 5))

    latest = df[df["date"] == df["date"].max()]
    latest = latest.drop_duplicates(subset=["model_short"], keep="last")
    latest = latest.sort_values("model_short")

    eq = pd.to_numeric(latest["E[Q]"], errors="coerce")
    ax.bar(latest["model_short"], eq, color="steelblue", edgecolor="k")

    # Error bars from quantiles if available
    if "Q5" in latest.columns and "Q95" in latest.columns:
        q5 = pd.to_numeric(latest["Q5"], errors="coerce")
        q95 = pd.to_numeric(latest["Q95"], errors="coerce")
        yerr_lower = eq - q5
        yerr_upper = q95 - eq
        ax.errorbar(
            latest["model_short"],
            eq,
            yerr=[yerr_lower, yerr_upper],
            fmt="none",
            ecolor="gray",
            capsize=4,
        )

    ax.set_xlabel("Model")
    ax.set_ylabel("Discharge (m³/s)")
    latest_date = latest["date"].iloc[0]
    ax.set_title(f"Model Comparison — Station {station} ({latest_date:%Y-%m-%d})")
    ax.grid(True, alpha=0.3, axis="y")
    plt.tight_layout()
    return ax


def main():
    parser = argparse.ArgumentParser(description="Inspect forecast data from SAPPHIRE API")
    parser.add_argument("--station", required=True, help="Station code")
    parser.add_argument(
        "--horizon",
        default="pentad",
        choices=["pentad", "decade"],
    )
    parser.add_argument("--start-date", default="2025-06-01")
    parser.add_argument("--end-date", default="2026-03-01")
    parser.add_argument("--models", nargs="*", help="Filter to specific models")
    parser.add_argument("--output-dir", help="Save plots to this directory")
    parser.add_argument(
        "--api-url",
        default=DEFAULT_API,
        help="API base URL",
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="Show plots interactively",
    )
    args = parser.parse_args()

    print(
        f"Fetching forecasts for station {args.station} ({args.start_date} to {args.end_date})..."
    )

    df = fetch_forecasts(
        args.api_url,
        args.station,
        args.horizon,
        args.start_date,
        args.end_date,
        models=args.models,
    )

    if df.empty:
        print("No forecast data returned.")
        return

    print(f"  {len(df)} rows, models: {sorted(df['model_short'].unique())}")

    # Also fetch LR data and merge
    try:
        lr = fetch_lr_forecasts(
            args.api_url,
            args.station,
            args.horizon,
            args.start_date,
            args.end_date,
        )
        if not lr.empty:
            lr.rename(columns={"forecasted_discharge": "E[Q]"}, inplace=True)
            lr["model_short"] = "LR"
            lr["model_long"] = "Linear regression (LR)"
            # Keep only columns that exist in df
            common_cols = [c for c in df.columns if c in lr.columns]
            df = pd.concat([df, lr[common_cols]], ignore_index=True)
    except Exception as e:
        print(f"  (LR fetch failed: {e})")

    # --- Plot 1: Time series ---
    fig1, ax1 = plt.subplots(figsize=(12, 6))
    plot_forecast_time_series(df, args.station, horizon=args.horizon, ax=ax1)

    # --- Plot 2: Model comparison ---
    fig2, ax2 = plt.subplots(figsize=(8, 5))
    plot_model_comparison(df, args.station, ax=ax2)

    # Save or show
    if args.output_dir:
        out = Path(args.output_dir)
        out.mkdir(parents=True, exist_ok=True)
        fig1.savefig(out / f"{args.station}_time_series.png", dpi=150)
        fig2.savefig(out / f"{args.station}_model_comparison.png", dpi=150)
        print(f"Plots saved to {out}/")

    if args.show:
        plt.show()
    elif not args.output_dir:
        print("Use --show to display interactively or --output-dir to save.")


if __name__ == "__main__":
    main()
