"""Streamlit dashboard for exploring forecast skill evaluation results.

Launch command (from repo root):
    uv run --project apps/forecast_skill_eval --with streamlit \\
        streamlit run apps/forecast_skill_eval/src/forecast_skill_eval/dashboard/app.py

CSV path resolution order:
    1. SKILL_EVAL_METRICS_CSV environment variable
    2. Sidebar path input
    3. Default: apps/forecast_skill_eval/artifacts/rerun_2026-06-30_phase2/contingency_metrics.csv
       (relative to this file's location, resolved at runtime)

Read-only: this dashboard never writes files or exports data.
"""

from __future__ import annotations

import math
import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

from forecast_skill_eval.dashboard.data import (
    available_options,
    filter_metrics,
    load_metrics,
    per_station,
    pooled_row,
    rank_stations,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# This file lives at src/forecast_skill_eval/dashboard/app.py inside the
# apps/forecast_skill_eval project.  Walk up 3 levels to reach the project
# root (apps/forecast_skill_eval/) where the artifacts/ directory lives.
_APP_DIR = Path(__file__).resolve().parents[3]
_DEFAULT_CSV = _APP_DIR / "artifacts" / "rerun_2026-06-30_phase2" / "contingency_metrics.csv"

_TABLE_COLUMNS = [
    "code",
    "basin",
    "n_pairs",
    "base_rate",
    "pod",
    "far",
    "hss",
    "pss",
    "pod_ci_lower",
    "pod_ci_upper",
]

_RANKABLE_METRICS = [
    "pod",
    "far",
    "hss",
    "pss",
    "csi",
    "pofd",
    "frequency_bias",
]

_UNDEFINED_FLAGS = {
    "pod": "pod_undefined",
    "far": "far_undefined",
    "hss": "hss_undefined",
    "pss": "pss_undefined",
    "csi": "csi_undefined",
    "pofd": "pofd_undefined",
    "frequency_bias": "frequency_bias_undefined",
    "base_rate": "base_rate_undefined",
    "pod_ci_lower": "pod_ci_undefined",
    "pod_ci_upper": "pod_ci_undefined",
}


def _nan_safe(value: float) -> str:
    """Return formatted float or 'n/a' for NaN."""
    if isinstance(value, float) and math.isnan(value):
        return "n/a"
    return f"{value:.3f}"


# ---------------------------------------------------------------------------
# Page setup
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Forecast Skill Evaluation",
    layout="wide",
)
st.title("Forecast Skill Evaluation — Station Explorer")

# ---------------------------------------------------------------------------
# CSV path resolution
# ---------------------------------------------------------------------------

env_path = os.environ.get("SKILL_EVAL_METRICS_CSV", "")
if env_path:
    default_csv_str = env_path
elif _DEFAULT_CSV.exists():
    default_csv_str = str(_DEFAULT_CSV)
else:
    default_csv_str = ""

with st.sidebar:
    st.header("Data source")
    csv_path_input = st.text_input(
        "Path to contingency_metrics.csv",
        value=default_csv_str,
        help=(
            "Absolute path to the contingency_metrics CSV produced by "
            "forecast_skill_eval. Override with SKILL_EVAL_METRICS_CSV env var."
        ),
    )

csv_path = Path(csv_path_input) if csv_path_input else None

if csv_path is None or not csv_path.exists():
    st.warning(
        "No valid CSV path provided. Set SKILL_EVAL_METRICS_CSV or enter a path in the sidebar."
    )
    st.stop()

# ---------------------------------------------------------------------------
# Load data (cached)
# ---------------------------------------------------------------------------


@st.cache_data(show_spinner="Loading metrics CSV …")
def _load(path: str) -> pd.DataFrame:
    return load_metrics(path)


df_all = _load(str(csv_path))

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("Filters")

    # Build selections incrementally so each widget only shows values that
    # exist in the data given all earlier choices (cascading filters).
    selections: dict[str, object] = {}

    # 1. Horizon — no upstream constraint yet.
    horizon_choices = available_options(df_all, "horizon", selections)
    horizon = st.selectbox("Horizon", horizon_choices, index=0)
    selections["horizon"] = horizon

    # 2. Event
    event_choices = available_options(df_all, "event", selections)
    event = st.selectbox("Event type", event_choices, index=0)
    selections["event"] = event

    # 3. Season
    season_choices = available_options(df_all, "season", selections)
    season = st.selectbox("Season", season_choices, index=0)
    selections["season"] = season

    # 4. Regime
    regime_choices = available_options(df_all, "regime", selections)
    regime = st.selectbox("Regime", regime_choices, index=0)
    selections["regime"] = regime

    # 5. Norm provenance — depends on horizon (e.g. 'official' only for month).
    norm_choices = available_options(df_all, "norm_provenance", selections)
    norm_provenance = st.selectbox("Norm provenance", norm_choices, index=0)
    selections["norm_provenance"] = norm_provenance

    # 6. Lead — auto-detected from the narrowed subset.
    #    available_options returns non-NaN leads only; empty → short-term.
    lead_opts = available_options(df_all, "lead", selections)
    lead: int | None = None
    if not lead_opts:
        st.caption("Short-term horizon — no lead dimension.")
    else:
        lead_int_choices = [int(v) for v in lead_opts]  # sorted by available_options
        lead = st.selectbox("Lead", lead_int_choices, index=0)
    selections["lead"] = lead

    # 7. Model — cascaded from all upstream filters (incl. lead).
    model_choices = available_options(df_all, "model", selections)
    selected_models = st.multiselect(
        "Model(s) — leave empty for all",
        model_choices,
        default=[],
    )
    model_filter = selected_models if selected_models else None

    st.header("Chart options")
    rank_metric = st.selectbox(
        "Rank stations by",
        _RANKABLE_METRICS,
        index=0,
    )
    ascending_rank = st.checkbox("Ascending (lower = better)", value=False)

# ---------------------------------------------------------------------------
# Filter data
# ---------------------------------------------------------------------------

df_filtered = filter_metrics(
    df_all,
    horizon=horizon,
    event=event,
    season=season,
    regime=regime,
    norm_provenance=norm_provenance,
    model=model_filter,
    lead=lead,
)

df_stations = per_station(df_filtered)
pooled = pooled_row(df_filtered)

if df_stations.empty:
    st.info("No per-station rows match the current filters.")
    st.stop()

# ---------------------------------------------------------------------------
# Summary line
# ---------------------------------------------------------------------------

n_stations = df_stations["code"].nunique()
n_models_shown = df_stations["model"].nunique()
st.markdown(
    f"**{n_stations}** stations · **{n_models_shown}** model(s) · "
    f"horizon `{horizon}` · season `{season}` · regime `{regime}`"
)

if pooled is not None:
    pooled_pod = _nan_safe(pooled.get("pod", float("nan")))
    pooled_hss = _nan_safe(pooled.get("hss", float("nan")))
    pooled_pss = _nan_safe(pooled.get("pss", float("nan")))
    st.markdown(
        f"**POOLED reference** — POD: `{pooled_pod}` · HSS: `{pooled_hss}` · PSS: `{pooled_pss}`"
    )

# ---------------------------------------------------------------------------
# (a) Per-station table
# ---------------------------------------------------------------------------

st.subheader("Per-station metrics table")

# Build a display-safe copy: replace undefined metric values with "n/a".
display_cols = [c for c in _TABLE_COLUMNS if c in df_stations.columns]
table_df = df_stations[display_cols + ["model"]].copy()

for col in display_cols:
    undef_col = _UNDEFINED_FLAGS.get(col)
    if undef_col and undef_col in df_stations.columns:
        undef_mask = df_stations[undef_col].astype(bool) | df_stations[col].isna()
        table_df.loc[undef_mask, col] = None  # Streamlit shows blank for None

st.dataframe(
    table_df,
    use_container_width=True,
    hide_index=True,
)

# ---------------------------------------------------------------------------
# (b) & (c) Bar chart — ranking by selected metric
# ---------------------------------------------------------------------------

st.subheader(f"Station ranking by `{rank_metric}`")

models_in_view = sorted(df_stations["model"].unique())

if len(models_in_view) <= 1:
    # Single model: simple bar chart.
    ranked = rank_stations(df_filtered, rank_metric, ascending=ascending_rank)
    if ranked.empty:
        st.info(f"All stations have undefined `{rank_metric}` — nothing to chart.")
    else:
        fig, ax = plt.subplots(figsize=(max(8, len(ranked) * 0.45), 4))
        bars = ax.bar(ranked["code"].astype(str), ranked[rank_metric], color="steelblue")

        # POOLED reference line.
        if pooled is not None:
            pval = pooled.get(rank_metric)
            undef_col = _UNDEFINED_FLAGS.get(rank_metric)
            is_undef = bool(
                (undef_col and pooled.get(undef_col, False))
                or (isinstance(pval, float) and math.isnan(pval))
            )
            if not is_undef and pval is not None:
                ax.axhline(
                    pval,
                    color="crimson",
                    linestyle="--",
                    linewidth=1.4,
                    label=f"POOLED = {pval:.3f}",
                )
                ax.legend(fontsize=8)

        ax.set_xlabel("Station code")
        ax.set_ylabel(rank_metric)
        ax.set_title(f"{rank_metric} by station")
        plt.xticks(rotation=45, ha="right", fontsize=7)
        plt.tight_layout()
        st.pyplot(fig)
        plt.close(fig)

else:
    # Multiple models: one subplot per model.
    n_cols = min(len(models_in_view), 3)
    n_rows = math.ceil(len(models_in_view) / n_cols)
    fig, axes = plt.subplots(
        n_rows,
        n_cols,
        figsize=(6 * n_cols, 4 * n_rows),
        squeeze=False,
    )

    pooled_val: float | None = None
    if pooled is not None:
        pval = pooled.get(rank_metric)
        undef_col = _UNDEFINED_FLAGS.get(rank_metric)
        is_undef = bool(
            (undef_col and pooled.get(undef_col, False))
            or (isinstance(pval, float) and math.isnan(pval))
        )
        if not is_undef and pval is not None:
            pooled_val = float(pval)

    for idx, mdl in enumerate(models_in_view):
        ax = axes[idx // n_cols][idx % n_cols]
        df_mdl = df_filtered[df_filtered["model"] == mdl]
        ranked_mdl = rank_stations(df_mdl, rank_metric, ascending=ascending_rank)

        if ranked_mdl.empty:
            ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
        else:
            ax.bar(ranked_mdl["code"].astype(str), ranked_mdl[rank_metric], color="steelblue")
            if pooled_val is not None:
                ax.axhline(
                    pooled_val,
                    color="crimson",
                    linestyle="--",
                    linewidth=1.2,
                    label=f"POOLED={pooled_val:.3f}",
                )
                ax.legend(fontsize=7)
            ax.set_xlabel("Station", fontsize=8)
            ax.set_ylabel(rank_metric, fontsize=8)
            plt.setp(ax.get_xticklabels(), rotation=45, ha="right", fontsize=6)

        ax.set_title(mdl, fontsize=9)

    # Hide unused axes.
    for idx in range(len(models_in_view), n_rows * n_cols):
        axes[idx // n_cols][idx % n_cols].set_visible(False)

    plt.suptitle(f"{rank_metric} by station — model comparison", fontsize=11)
    plt.tight_layout()
    st.pyplot(fig)
    plt.close(fig)
