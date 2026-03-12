"""Marimo notebook for interactive long-term forecast exploration.

Launch with:
    cd apps/forecast_dashboard
    uv run marimo edit dev_code/explore_long_forecasts.py
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
    from fetch_data import fetch_long_forecasts

    return fetch_long_forecasts, mo, pd, plt


@app.cell
def _(mo):
    mo.md("""
    # Long-Term Forecast Explorer
    """)
    return


@app.cell
def _(mo):
    api_url = mo.ui.text(value="http://localhost:8000/api", label="API URL")
    station = mo.ui.text(value="15102", label="Station code")
    horizon_type = mo.ui.dropdown(
        options=["month", "quarter", "season"],
        value="month",
        label="Horizon type",
    )
    issue_day = mo.ui.dropdown(
        options=["all", "10th", "25th"],
        value="25th",
        label="Issue day (month only)",
    )
    horizon_value = mo.ui.dropdown(
        options=["all", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"],
        value="all",
        label="Lead time",
    )
    flag_filter = mo.ui.dropdown(
        options=["all", "1 (operational)", "0 (test/debug)"],
        value="all",
        label="Flag",
    )
    start_date = mo.ui.text(value="2025-01-01", label="Start date")
    end_date = mo.ui.text(value="2026-03-01", label="End date")

    mo.hstack([
        api_url, station, horizon_type, issue_day,
        horizon_value, flag_filter, start_date, end_date,
    ])
    return (
        api_url, end_date, flag_filter, horizon_type,
        horizon_value, issue_day, start_date, station,
    )


@app.cell
def _(
    api_url,
    end_date,
    fetch_long_forecasts,
    flag_filter,
    horizon_type,
    horizon_value,
    issue_day,
    mo,
    pd,
    start_date,
    station,
):
    hv = None if horizon_value.value == "all" else int(horizon_value.value)

    df = fetch_long_forecasts(
        api_url.value,
        station.value,
        horizon_type=horizon_type.value,
        horizon_value=hv,
        start_date=start_date.value,
        end_date=end_date.value,
    )

    # Filter by issue day when horizon_type is "month"
    if (
        not df.empty
        and horizon_type.value == "month"
        and issue_day.value != "all"
        and "date" in df.columns
    ):
        dates = pd.to_datetime(df["date"])
        if issue_day.value == "10th":
            df = df[dates.dt.day <= 15]
        else:  # "25th"
            df = df[dates.dt.day > 15]

    # Filter by flag
    if not df.empty and flag_filter.value != "all" and "flag" in df.columns:
        flag_val = int(flag_filter.value[0])  # "1 (operational)" -> 1
        df = df[df["flag"] == flag_val]

    if df.empty:
        mo.md("**No long-term forecast data found** for the selected parameters.")
    else:
        models = sorted(df["model_short"].unique()) if "model_short" in df.columns else []
        mo.md(f"**{len(df)} rows**, models: {models}")
    return (df,)


@app.cell
def _(df, mo):
    if df.empty or "model_short" not in df.columns:
        model_filter = mo.ui.multiselect(options=[], value=[], label="Models")
    else:
        model_filter = mo.ui.multiselect(
            options=sorted(df["model_short"].unique().tolist()),
            value=sorted(df["model_short"].unique().tolist()),
            label="Models",
        )
    model_filter  # noqa: B018 — marimo display expression
    return (model_filter,)


@app.cell
def _(df, mo, model_filter, pd, plt):
    if df.empty or "model_short" not in df.columns or not model_filter.value:
        filtered = df
        _output = mo.md("## Forecast by Validity Period\n\nNo data to plot.")
    else:
        filtered = df[df["model_short"].isin(model_filter.value)]

        fig, ax = plt.subplots(figsize=(14, 6))
        colors = plt.cm.tab10.colors

        for i, model in enumerate(sorted(filtered["model_short"].unique())):
            subset = filtered[filtered["model_short"] == model].sort_values("valid_from")
            color = colors[i % len(colors)]
            _eq = pd.to_numeric(subset["E[Q]"], errors="coerce")

            # Plot forecast as horizontal bars spanning validity periods
            for _, row in subset.iterrows():
                val = pd.to_numeric(row["E[Q]"], errors="coerce")
                if pd.isna(val):
                    continue
                ax.plot(
                    [row["valid_from"], row["valid_to"]],
                    [val, val],
                    color=color,
                    linewidth=2,
                    solid_capstyle="butt",
                )

            # Also plot as connected midpoints for trend visibility
            _mid = subset["valid_from"] + (subset["valid_to"] - subset["valid_from"]) / 2
            ax.plot(
                _mid,
                _eq,
                label=model,
                color=color,
                marker="o",
                markersize=4,
                linestyle="--",
                alpha=0.6,
            )

            # Confidence bands
            if "q05" in subset.columns and "q95" in subset.columns:
                _q05 = pd.to_numeric(subset["q05"], errors="coerce")
                _q95 = pd.to_numeric(subset["q95"], errors="coerce")
                ax.fill_between(_mid, _q05, _q95, alpha=0.1, color=color)

        ax.set_xlabel("Date")
        ax.set_ylabel("Discharge (m\u00b3/s)")
        ax.set_title("Long-Term Forecasts by Validity Period")
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.close(fig)

        _output = mo.vstack([mo.md("## Forecast by Validity Period"), mo.as_html(fig)])

    _output  # noqa: B018 — marimo display expression
    return (filtered,)


@app.cell
def _(filtered, mo, pd, plt):
    """Compare forecasts by lead time (horizon_value)."""
    if filtered.empty or "horizon_value" not in filtered.columns or "E[Q]" not in filtered.columns:
        _output = mo.md("## Lead-Time Comparison\n\nNo data to plot.")
    else:
        leads = sorted(filtered["horizon_value"].unique())
        if len(leads) < 2:
            _output = mo.md(
                "## Lead-Time Comparison\n\n"
                "Only one lead time present — select **all** in the Lead time dropdown."
            )
        else:
            fig2, ax2 = plt.subplots(figsize=(14, 6))
            colors2 = plt.cm.tab10.colors
            for j, lv in enumerate(leads):
                sub = filtered[filtered["horizon_value"] == lv].sort_values("valid_from")
                _mid = sub["valid_from"] + (sub["valid_to"] - sub["valid_from"]) / 2
                _eq = pd.to_numeric(sub["E[Q]"], errors="coerce")
                ax2.plot(
                    _mid,
                    _eq,
                    label=f"lead={lv}",
                    color=colors2[j % len(colors2)],
                    marker="o",
                    markersize=4,
                )
            ax2.set_xlabel("Validity Period Midpoint")
            ax2.set_ylabel("Discharge (m\u00b3/s)")
            ax2.set_title("Forecast by Lead Time")
            ax2.legend(fontsize=8)
            ax2.grid(True, alpha=0.3)
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.close(fig2)

            _output = mo.vstack([mo.md("## Lead-Time Comparison"), mo.as_html(fig2)])

    _output  # noqa: B018 — marimo display expression
    return


@app.cell
def _(filtered, mo, pd):
    """Show observed vs. forecast where q_obs is available."""
    if filtered.empty or "q_obs" not in filtered.columns:
        _output = mo.md("")
    else:
        obs = filtered[pd.to_numeric(filtered["q_obs"], errors="coerce").notna()]
        if obs.empty:
            _output = mo.md("")
        else:
            _output = mo.vstack(
                [
                    mo.md("## Observed vs. Forecast"),
                    mo.ui.table(
                        obs[
                            [
                                c
                                for c in [
                                    "model_short",
                                    "valid_from",
                                    "valid_to",
                                    "E[Q]",
                                    "q_obs",
                                    "horizon_value",
                                    "flag",
                                ]
                                if c in obs.columns
                            ]
                        ]
                    ),
                ]
            )
    _output  # noqa: B018 — marimo display expression
    return


@app.cell
def _(filtered, mo):
    mo.md("## Raw Data")
    mo.ui.table(filtered)
    return


if __name__ == "__main__":
    app.run()
