"""Marimo notebook for interactive skill metrics exploration.

Launch with:
    cd apps/forecast_dashboard
    uv run marimo edit dev_code/explore_skill.py
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
    from fetch_data import fetch_skill_metrics

    return fetch_skill_metrics, mo, pd, plt


@app.cell
def _(mo):
    mo.md("""
    # Skill Metrics Explorer
    """)
    return


@app.cell
def _(mo):
    api_url = mo.ui.text(value="http://localhost:8000/api", label="API URL")
    station = mo.ui.text(value="15102", label="Station code")
    horizon = mo.ui.dropdown(options=["pentad", "decade"], value="pentad", label="Horizon")
    start_date = mo.ui.text(value="2025-01-01", label="Start date")
    end_date = mo.ui.text(value="2026-12-31", label="End date")

    mo.hstack([api_url, station, horizon, start_date, end_date])
    return api_url, end_date, horizon, start_date, station


@app.cell
def _(
    api_url,
    end_date,
    fetch_skill_metrics,
    horizon,
    mo,
    start_date,
    station,
):
    df = fetch_skill_metrics(
        api_url.value,
        station.value,
        horizon.value,
        start_date.value,
        end_date.value,
    )

    hin = "decad_in_year" if horizon.value == "decade" else "pentad_in_year"
    mo.md(f"**{len(df)} rows**, models: {sorted(df['model_short'].unique())}")  # noqa: B018
    return df, hin


@app.cell
def _(mo):
    metric_select = mo.ui.multiselect(
        options=["sdivsigma", "accuracy", "mae", "nse", "n_pairs"],
        value=["sdivsigma", "accuracy"],
        label="Metrics to plot",
    )
    metric_select  # noqa: B018 — marimo display expression
    return (metric_select,)


@app.cell
def _(df, hin, metric_select, mo, pd, plt):
    # accuracy is stored as a fraction [0, 1]; convert to percentage for display
    _df = df.copy()
    if "accuracy" in _df.columns:
        _df["accuracy"] = pd.to_numeric(_df["accuracy"], errors="coerce") * 100

    metrics = metric_select.value
    n = len(metrics)
    if n == 0:
        _output = mo.md("Select at least one metric.")
    else:
        thresholds = {"sdivsigma": 0.674, "accuracy": 80.0}
        labels = {
            "sdivsigma": "s/σ",
            "accuracy": "Accuracy (%)",
            "mae": "MAE (m³/s)",
            "nse": "NSE",
            "n_pairs": "n_pairs",
        }

        fig, axes = plt.subplots(n, 1, figsize=(10, 5 * n), squeeze=False)
        colors = plt.cm.tab10.colors
        models = sorted(_df["model_short"].unique())

        for idx, metric in enumerate(metrics):
            ax = axes[idx, 0]
            for i, model in enumerate(models):
                subset = _df[_df["model_short"] == model].sort_values(hin)
                if metric not in subset.columns:
                    continue
                vals = pd.to_numeric(subset[metric], errors="coerce")
                ax.plot(
                    subset[hin],
                    vals,
                    label=model,
                    color=colors[i % len(colors)],
                    marker="o",
                    markersize=4,
                )
            if metric in thresholds:
                ax.axhline(
                    y=thresholds[metric],
                    color="red",
                    linestyle="--",
                    alpha=0.7,
                )
            ax.set_ylabel(labels.get(metric, metric))
            ax.set_xlabel(hin.replace("_", " ").title())
            ax.set_title(labels.get(metric, metric))
            ax.legend(fontsize=8)
            ax.grid(True, alpha=0.3)

        plt.tight_layout()
        _output = mo.vstack([mo.md("## Skill Metrics Over Period"), fig])
    return (_output,)


@app.cell
def _(df, hin, mo, plt):
    # Heatmap: model x period for sdivsigma
    if "sdivsigma" in df.columns:
        pivot = df.pivot_table(
            values="sdivsigma",
            index="model_short",
            columns=hin,
            aggfunc="mean",
        )

        _fig, _ax = plt.subplots(figsize=(12, 4))
        _im = _ax.imshow(pivot.values, aspect="auto", cmap="RdYlGn_r")
        _ax.set_xticks(range(len(pivot.columns)))
        _ax.set_xticklabels(pivot.columns, rotation=45)
        _ax.set_yticks(range(len(pivot.index)))
        _ax.set_yticklabels(pivot.index)
        plt.colorbar(_im, ax=_ax, label="s/σ")
        _ax.set_title("s/σ Heatmap: Model × Period")
        plt.tight_layout()

        _output = mo.vstack([mo.md("## s/σ Heatmap"), _fig])
    else:
        _output = mo.md("No sdivsigma column in data.")
    return (_output,)


@app.cell
def _(df, mo):
    mo.vstack([mo.md("## Raw Data"), mo.ui.table(df)])
    return


if __name__ == "__main__":
    app.run()
