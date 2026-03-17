#!/usr/bin/env python3
"""CLI tool for inspecting skill metrics from the SAPPHIRE API.

Produces matplotlib plots:
  - Line plot of metrics over pentad/decad, one line per model
  - Reference lines at threshold values (0.674 for sdivsigma, 80% accuracy)

Usage:
    cd apps/forecast_dashboard
    uv run python dev_code/inspect_skill.py \
        --station 15102 --horizon pentad \
        --metric sdivsigma accuracy

    uv run python dev_code/inspect_skill.py \
        --station 15102 --horizon pentad --show
"""

import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from fetch_data import fetch_skill_metrics

DEFAULT_API = "http://localhost:8000/api"

METRIC_THRESHOLDS = {
    "sdivsigma": {"value": 0.674, "label": "s/σ = 0.674", "color": "red"},
    "accuracy": {"value": 80.0, "label": "Accuracy = 80%", "color": "red"},
}

METRIC_LABELS = {
    "sdivsigma": "s/σ",
    "accuracy": "Accuracy (%)",
    "mae": "MAE (m³/s)",
    "nse": "NSE",
    "n_pairs": "Number of pairs",
}


def plot_skill_metric(
    df: pd.DataFrame,
    metric: str,
    station: str,
    horizon_col: str,
    ax=None,
):
    """Line plot of a skill metric over period, one line per model."""
    if ax is None:
        _, ax = plt.subplots(figsize=(10, 5))

    models = sorted(df["model_short"].unique())
    colors = plt.cm.tab10.colors

    for i, model in enumerate(models):
        subset = df[df["model_short"] == model].sort_values(horizon_col)
        if metric not in subset.columns:
            continue
        vals = pd.to_numeric(subset[metric], errors="coerce")
        ax.plot(
            subset[horizon_col],
            vals,
            label=model,
            color=colors[i % len(colors)],
            marker="o",
            markersize=4,
        )

    # Threshold reference line
    if metric in METRIC_THRESHOLDS:
        info = METRIC_THRESHOLDS[metric]
        ax.axhline(
            y=info["value"],
            color=info["color"],
            linestyle="--",
            alpha=0.7,
            label=info["label"],
        )

    ylabel = METRIC_LABELS.get(metric, metric)
    ax.set_xlabel(f"{horizon_col.replace('_', ' ').title()}")
    ax.set_ylabel(ylabel)
    ax.set_title(f"{ylabel} — Station {station}")
    ax.legend(loc="best", fontsize=8)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    return ax


def print_skill_summary(df: pd.DataFrame, metrics: list[str]):
    """Print a summary table to stdout."""
    cols = ["model_short"] + [m for m in metrics if m in df.columns]
    if "n_pairs" in df.columns and "n_pairs" not in cols:
        cols.append("n_pairs")

    summary = df.drop_duplicates(subset=["model_short"], keep="last")
    summary = summary[cols].sort_values("model_short")
    print("\n=== Skill Metric Summary ===")
    print(summary.to_string(index=False))
    print()


def main():
    parser = argparse.ArgumentParser(description="Inspect skill metrics from SAPPHIRE API")
    parser.add_argument("--station", required=True, help="Station code")
    parser.add_argument(
        "--horizon",
        default="pentad",
        choices=["pentad", "decade"],
    )
    parser.add_argument(
        "--metric",
        nargs="*",
        default=["sdivsigma", "accuracy"],
        help="Metrics to plot",
    )
    parser.add_argument("--start-date", default="2025-01-01")
    parser.add_argument("--end-date", default="2026-12-31")
    parser.add_argument("--output-dir", help="Save plots to this directory")
    parser.add_argument("--api-url", default=DEFAULT_API)
    parser.add_argument("--show", action="store_true")
    args = parser.parse_args()

    horizon_col = "decad_in_year" if args.horizon == "decade" else "pentad_in_year"

    print(f"Fetching skill metrics for station {args.station}...")
    df = fetch_skill_metrics(
        args.api_url,
        args.station,
        args.horizon,
        args.start_date,
        args.end_date,
    )

    if df.empty:
        print("No skill metric data returned.")
        return

    print(f"  {len(df)} rows, models: {sorted(df['model_short'].unique())}")
    print_skill_summary(df, args.metric)

    # One subplot per metric
    n_metrics = len(args.metric)
    fig, axes = plt.subplots(
        n_metrics,
        1,
        figsize=(10, 5 * n_metrics),
        squeeze=False,
    )

    for i, metric in enumerate(args.metric):
        plot_skill_metric(df, metric, args.station, horizon_col, ax=axes[i, 0])

    plt.tight_layout()

    if args.output_dir:
        out = Path(args.output_dir)
        out.mkdir(parents=True, exist_ok=True)
        fig.savefig(out / f"{args.station}_skill_metrics.png", dpi=150)
        print(f"Plot saved to {out}/")

    if args.show:
        plt.show()
    elif not args.output_dir:
        print("Use --show to display interactively or --output-dir to save.")


if __name__ == "__main__":
    main()
