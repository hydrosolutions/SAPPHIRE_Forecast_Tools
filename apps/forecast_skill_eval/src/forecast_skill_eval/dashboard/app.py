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

import altair as alt
import pandas as pd
import streamlit as st

from forecast_skill_eval.dashboard.aggregates import (
    FIG4_LEADS,
    FIG6_HORIZONS,
    HCOLORS,
    HORIZONS,
    LONG_TERM,
    get_baseline_refs,
    load_baselines,
    model_sort_key,
    prep_model_comparison_per_horizon,
    prep_op_vs_hindcast,
    prep_performance_diagram,
    prep_seasonal_pod,
    prep_skill_ladder,
)
from forecast_skill_eval.dashboard.data import (
    available_options,
    filter_metrics,
    load_metrics,
    per_station,
    pooled_row,
    rank_stations,
)

alt.data_transformers.disable_max_rows()

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
# Helper: fig4 bar chart renderer
# ---------------------------------------------------------------------------


def _render_fig4_bars(
    df_melt: pd.DataFrame,
    sorted_models: list[str],
    refs: dict,
    horizon: str,
    lead: int | None,
) -> None:
    """Render POD/FAR grouped bar chart with baseline reference rules.

    Args:
        df_melt: Long-format DataFrame with columns model, metric, value,
            family_label, and optionally n_pairs.
        sorted_models: Model names in display order.
        refs: Dict from :func:`get_baseline_refs` with optional baseline values.
        horizon: Horizon label for the chart title.
        lead: Integer lead (appended to title), or None for short-term.
    """
    metric_domain = ["pod", "far"]
    metric_range = ["#2ca02c", "#d62728"]
    n_pairs_col = "n_pairs" if "n_pairs" in df_melt.columns else None

    tooltip_fields = [
        alt.Tooltip("model:N", title="Model"),
        alt.Tooltip("metric:N", title="Metric"),
        alt.Tooltip("value:Q", title="Value", format=".3f"),
        alt.Tooltip("family_label:N", title="Family"),
    ]
    if n_pairs_col:
        tooltip_fields.append(alt.Tooltip("n_pairs:Q", title="n_pairs"))

    bar = (
        alt.Chart(df_melt)
        .mark_bar()
        .encode(
            x=alt.X("model:N", sort=sorted_models, title="Model"),
            xOffset=alt.XOffset("metric:N", sort=metric_domain),
            y=alt.Y("value:Q", title="Rate (0–1)", scale=alt.Scale(domain=[0.0, 1.1])),
            color=alt.Color(
                "metric:N",
                scale=alt.Scale(domain=metric_domain, range=metric_range),
                legend=alt.Legend(title="Metric"),
            ),
            tooltip=tooltip_fields,
        )
    )

    layers: list[alt.Chart] = [bar]

    # Persistence POD reference rule
    if refs.get("persistence_pod") is not None:
        pers_pod = refs["persistence_pod"]
        pers_rule = (
            alt.Chart(pd.DataFrame({"persistence_pod": [pers_pod]}))
            .mark_rule(color="darkorange", strokeDash=[6, 3], size=1.5)
            .encode(
                y=alt.Y("persistence_pod:Q"),
                tooltip=[alt.Tooltip("persistence_pod:Q", title="Persistence POD", format=".3f")],
            )
        )
        layers.append(pers_rule)

    lead_label = f" L{lead}" if lead is not None else ""
    title = f"{horizon}{lead_label} — POD (green) / FAR (red)"
    if refs.get("clim_base_rate") is not None:
        title += f"  |  Climatology: POD=0, base_rate={refs['clim_base_rate']:.2f}"
    if refs.get("persistence_pod") is not None and refs.get("persistence_hss") is not None:
        title += (
            f"  |  Persistence: POD={refs['persistence_pod']:.2f},"
            f" HSS={refs['persistence_hss']:.2f}"
        )

    chart = alt.layer(*layers).properties(title=title)
    st.altair_chart(chart, use_container_width=True)


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


@st.cache_data(show_spinner="Loading baselines …")
def _load_baselines(path: str) -> pd.DataFrame:
    return load_baselines(path)


baselines_df = _load_baselines(str(csv_path))

# ---------------------------------------------------------------------------
# View selector
# ---------------------------------------------------------------------------

view = st.radio("View", ["Per-station", "Aggregates (pooled)"], horizontal=True)

# ---------------------------------------------------------------------------
# Per-station view
# ---------------------------------------------------------------------------

if view == "Per-station":
    # Sidebar filters
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

    # Filter data
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

    # Summary line
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
            f"**POOLED reference** — POD: `{pooled_pod}` · HSS: `{pooled_hss}`"
            f" · PSS: `{pooled_pss}`"
        )

    # (a) Per-station table
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

    # (b) & (c) Bar chart — ranking by selected metric
    st.subheader(f"Station ranking by `{rank_metric}`")

    models_in_view = sorted(df_stations["model"].unique())

    if len(models_in_view) <= 1:
        # Single model: simple bar chart.
        ranked = rank_stations(df_filtered, rank_metric, ascending=ascending_rank)
        if ranked.empty:
            st.info(f"All stations have undefined `{rank_metric}` — nothing to chart.")
        else:
            bar_data = ranked[["code", rank_metric, "model", "n_pairs"]].copy()
            bar_data["code"] = bar_data["code"].astype(str)
            bar_data = bar_data.dropna(subset=[rank_metric])
            bar = (
                alt.Chart(bar_data)
                .mark_bar(color="steelblue")
                .encode(
                    x=alt.X(
                        "code:N",
                        sort="-y" if not ascending_rank else "y",
                        title="Station code",
                    ),
                    y=alt.Y(f"{rank_metric}:Q", title=rank_metric),
                    tooltip=[
                        alt.Tooltip("code:N", title="Station"),
                        alt.Tooltip(f"{rank_metric}:Q", title=rank_metric, format=".3f"),
                        alt.Tooltip("n_pairs:Q", title="n_pairs"),
                    ],
                )
            )
            layers: list[alt.Chart] = [bar]
            # POOLED reference
            if pooled is not None:
                pval = pooled.get(rank_metric)
                undef_col = _UNDEFINED_FLAGS.get(rank_metric)
                is_undef = bool(
                    (undef_col and pooled.get(undef_col, False))
                    or (isinstance(pval, float) and math.isnan(pval))
                )
                if not is_undef and pval is not None:
                    rule = (
                        alt.Chart(pd.DataFrame({"pooled": [float(pval)]}))
                        .mark_rule(color="crimson", strokeDash=[5, 3])
                        .encode(
                            y=alt.Y("pooled:Q"),
                            tooltip=[
                                alt.Tooltip(
                                    "pooled:Q",
                                    title=f"POOLED {rank_metric}",
                                    format=".3f",
                                )
                            ],
                        )
                    )
                    layers.append(rule)
            st.altair_chart(
                alt.layer(*layers).properties(title=f"{rank_metric} by station"),
                use_container_width=True,
            )

    else:
        # Multiple models: faceted bar chart.
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

        ranked_rows = []
        for mdl in models_in_view:
            df_mdl = df_filtered[df_filtered["model"] == mdl]
            ranked_mdl = rank_stations(df_mdl, rank_metric, ascending=ascending_rank)
            if not ranked_mdl.empty:
                ranked_rows.append(ranked_mdl)
        if not ranked_rows:
            st.info(f"All stations have undefined `{rank_metric}` — nothing to chart.")
        else:
            all_ranked = pd.concat(ranked_rows, ignore_index=True)
            all_ranked["code"] = all_ranked["code"].astype(str)
            all_ranked = all_ranked.dropna(subset=[rank_metric])
            bar = (
                alt.Chart(all_ranked)
                .mark_bar(color="steelblue")
                .encode(
                    x=alt.X("code:N", title="Station"),
                    y=alt.Y(f"{rank_metric}:Q", title=rank_metric),
                    tooltip=[
                        alt.Tooltip("code:N", title="Station"),
                        alt.Tooltip("model:N", title="Model"),
                        alt.Tooltip(f"{rank_metric}:Q", title=rank_metric, format=".3f"),
                        alt.Tooltip("n_pairs:Q", title="n_pairs"),
                    ],
                )
                .facet(
                    facet=alt.Facet("model:N", title="Model"),
                    columns=3,
                )
                .properties(title=f"{rank_metric} by station — model comparison")
            )
            st.altair_chart(bar, use_container_width=True)
            if pooled_val is not None:
                st.caption(
                    f"POOLED reference: {rank_metric} = {pooled_val:.3f}"
                    " (crimson dashed line not shown in faceted view)"
                )

# ---------------------------------------------------------------------------
# Aggregates (pooled) view
# ---------------------------------------------------------------------------

elif view == "Aggregates (pooled)":
    st.subheader("Aggregates (pooled)")

    # Aggregates-only selectors (not constrained by per-station horizon filter)
    agg_col1, agg_col2 = st.columns(2)
    with agg_col1:
        agg_event_choices = (
            sorted(df_all["event"].dropna().unique().tolist())
            if "event" in df_all.columns
            else ["below_norm"]
        )
        agg_event = st.selectbox(
            "Event (aggregates)",
            agg_event_choices,
            index=agg_event_choices.index("below_norm") if "below_norm" in agg_event_choices else 0,
            key="agg_event",
        )
    with agg_col2:
        agg_season_choices = (
            sorted(df_all["season"].dropna().unique().tolist())
            if "season" in df_all.columns
            else ["all"]
        )
        agg_season = st.selectbox(
            "Season (aggregates)",
            agg_season_choices,
            index=agg_season_choices.index("all") if "all" in agg_season_choices else 0,
            key="agg_season",
        )

    # --- Fig 1: Performance Diagram ---
    st.subheader("Fig 1 — Performance Diagram")
    df_perf = prep_performance_diagram(df_all, event=agg_event, season=agg_season)
    if df_perf.empty:
        st.info("No data available for the performance diagram with these filters.")
    else:
        # Diagonal line (bias=1: POD=SR)
        diag_data = pd.DataFrame({"x": [0.0, 1.0], "y": [0.0, 1.0]})
        diag = (
            alt.Chart(diag_data)
            .mark_line(color="gray", strokeDash=[4, 4], opacity=0.5)
            .encode(x="x:Q", y="y:Q")
        )
        scatter = (
            alt.Chart(df_perf)
            .mark_point(filled=True, size=80, opacity=0.85)
            .encode(
                x=alt.X(
                    "sr:Q",
                    title="Success Ratio (1 − FAR)",
                    scale=alt.Scale(domain=[0.0, 1.05]),
                ),
                y=alt.Y("pod:Q", title="POD", scale=alt.Scale(domain=[0.0, 1.05])),
                color=alt.Color(
                    "horizon:N",
                    scale=alt.Scale(
                        domain=list(HCOLORS.keys()),
                        range=list(HCOLORS.values()),
                    ),
                    legend=alt.Legend(title="Horizon"),
                ),
                shape=alt.Shape("model:N", legend=alt.Legend(title="Model")),
                tooltip=[
                    alt.Tooltip("horizon:N", title="Horizon"),
                    alt.Tooltip("model:N", title="Model"),
                    alt.Tooltip("lead_label:N", title="Lead"),
                    alt.Tooltip("pod:Q", title="POD", format=".3f"),
                    alt.Tooltip("far:Q", title="FAR", format=".3f"),
                    alt.Tooltip("csi:Q", title="CSI", format=".3f"),
                    alt.Tooltip("n_pairs:Q", title="n_pairs"),
                ],
            )
        )
        fig1_chart = (
            (diag + scatter)
            .properties(
                title=("Roebber performance diagram — POOLED operational (top-right = skillful)")
            )
            .interactive()
        )
        st.altair_chart(fig1_chart, use_container_width=True)
        st.caption(
            "Colour = horizon; shape = model. All horizons pooled;"
            " code=POOLED, no per-station data."
        )

    # --- Fig 4: Model comparison per horizon ---
    st.subheader("Fig 4 — Model comparison per horizon")
    horizon_choices_agg = [h for h in HORIZONS if h in df_all["horizon"].unique()]
    selected_horizon = st.selectbox("Horizon (fig 4)", horizon_choices_agg, key="fig4_horizon")

    df_h = prep_model_comparison_per_horizon(
        df_all, selected_horizon, event=agg_event, season=agg_season
    )
    if df_h.empty:
        st.info("No data for this horizon.")
    else:
        # Prepare long-format: melt POD and FAR
        id_cols = ["model", "lead_int", "family_label", "n_pairs"]
        id_cols = [c for c in id_cols if c in df_h.columns]
        melt_cols = []
        if "pod" in df_h.columns:
            melt_cols.append("pod")
        if "far" in df_h.columns:
            melt_cols.append("far")
        df_melt = df_h.melt(
            id_vars=id_cols,
            value_vars=melt_cols,
            var_name="metric",
            value_name="value",
        ).dropna(subset=["value"])

        sorted_models = sorted(df_h["model"].unique(), key=model_sort_key)

        if selected_horizon in LONG_TERM:
            # Show one chart per approved lead
            lead_tabs = FIG4_LEADS.get(selected_horizon, [])
            tab_labels = [f"Lead L{ll}" for ll in lead_tabs]
            if tab_labels:
                tabs = st.tabs(tab_labels)
                for tab, lead in zip(tabs, lead_tabs, strict=False):
                    with tab:
                        df_lead = df_melt[df_melt["lead_int"] == lead]
                        if df_lead.empty:
                            st.info(f"No data for lead L{lead}.")
                            continue
                        refs = get_baseline_refs(baselines_df, selected_horizon, lead=lead)
                        _render_fig4_bars(df_lead, sorted_models, refs, selected_horizon, lead)
        else:
            refs = get_baseline_refs(baselines_df, selected_horizon)
            _render_fig4_bars(df_melt, sorted_models, refs, selected_horizon, None)

    # --- Fig 5: Skill ladder ---
    st.subheader("Fig 5 — Skill ladder")
    df_ladder = prep_skill_ladder(df_all, baselines_df, event=agg_event, season=agg_season)
    df_ladder_valid = df_ladder[df_ladder["hss"].notna()].copy()
    if df_ladder_valid.empty:
        st.info("No skill data available.")
    else:
        h_order = [
            "day",
            "pentad",
            "decade",
            "month\nL0",
            "quarter\nL1",
            "season\nL0",
        ]
        series_color_map: dict[str, str] = {
            "Climatology": "#dddddd",
            "Persistence": "#888888",
        }
        # Best model series names vary; assign horizon color
        for _, row in df_ladder_valid.iterrows():
            s = row["series"]
            if s not in series_color_map:
                h = row["horizon"]
                series_color_map[s] = HCOLORS.get(h, "#333333")
        domain = list(series_color_map.keys())
        range_ = [series_color_map[k] for k in domain]

        ladder_chart = (
            alt.Chart(df_ladder_valid)
            .mark_bar()
            .encode(
                x=alt.X("h_display:N", sort=h_order, title="Horizon"),
                xOffset=alt.XOffset("series:N"),
                y=alt.Y("hss:Q", title="HSS", scale=alt.Scale(domain=[-0.15, 1.05])),
                color=alt.Color(
                    "series:N",
                    scale=alt.Scale(domain=domain, range=range_),
                    legend=alt.Legend(title="Series"),
                ),
                tooltip=[
                    alt.Tooltip("h_display:N", title="Horizon"),
                    alt.Tooltip("series:N", title="Series"),
                    alt.Tooltip("hss:Q", title="HSS", format=".3f"),
                ],
            )
            .properties(title="Three-way skill ladder: Climatology ◀ Persistence ◀ Best model")
        )
        zero_line = (
            alt.Chart(pd.DataFrame({"y": [0]}))
            .mark_rule(color="black", opacity=0.5)
            .encode(y="y:Q")
        )
        st.altair_chart((ladder_chart + zero_line), use_container_width=True)
        st.caption(
            "Operational regime, POOLED. Long-term uses canonical lead"
            " (month/season L0, quarter L1)."
        )

    # --- Fig 6: Seasonal POD ---
    st.subheader("Fig 6 — Seasonal POD (EM model, key horizons)")
    df_spod = prep_seasonal_pod(df_all, event=agg_event)
    df_spod_valid = df_spod[~df_spod["pod_undefined"]].copy()
    if df_spod_valid.empty:
        st.info("No seasonal POD data available (requires EM model rows).")
    else:
        season_domain = ["Oct–Mar", "All year", "Apr–Sep"]
        season_range = ["#4472C4", "#A5A5A5", "#E97132"]
        h_label_order = [h for _, _, h in FIG6_HORIZONS]

        spod_chart = (
            alt.Chart(df_spod_valid)
            .mark_bar()
            .encode(
                x=alt.X("h_label:N", sort=h_label_order, title="Horizon"),
                xOffset=alt.XOffset("season_label:N", sort=season_domain),
                y=alt.Y("pod:Q", title="POD", scale=alt.Scale(domain=[0.0, 1.15])),
                color=alt.Color(
                    "season_label:N",
                    scale=alt.Scale(domain=season_domain, range=season_range),
                    legend=alt.Legend(title="Season"),
                ),
                tooltip=[
                    alt.Tooltip("h_label:N", title="Horizon"),
                    alt.Tooltip("season_label:N", title="Season"),
                    alt.Tooltip("pod:Q", title="POD", format=".3f"),
                    alt.Tooltip("n_pairs:Q", title="n_pairs"),
                ],
            )
            .properties(title="POD by season — EM model, operational, POOLED")
        )
        st.altair_chart(spod_chart, use_container_width=True)
        st.caption(
            "EM model only. Irrigation = Apr–Sep; non-irrigation = Oct–Mar."
            " Season horizon excluded."
        )

    # --- Fig 3: Operational vs hindcast HSS ---
    st.subheader("Fig 3 — Operational vs hindcast HSS (long-term only)")
    df_ovh = prep_op_vs_hindcast(df_all, event=agg_event, season=agg_season)
    df_ovh_valid = (
        df_ovh[~df_ovh["hss_undefined"] & df_ovh["hss"].notna()].copy()
        if not df_ovh.empty
        else df_ovh
    )
    if df_ovh_valid.empty:
        st.info("No operational vs hindcast data available.")
    else:
        regime_domain = ["operational", "hindcast"]
        regime_range = ["#1f77b4", "#ff7f0e"]

        ovh_chart = (
            alt.Chart(df_ovh_valid)
            .mark_bar()
            .encode(
                x=alt.X("lead_label:N", sort=None, title="Lead"),
                xOffset=alt.XOffset("regime:N", sort=regime_domain),
                y=alt.Y("hss:Q", title="HSS"),
                color=alt.Color(
                    "regime:N",
                    scale=alt.Scale(domain=regime_domain, range=regime_range),
                    legend=alt.Legend(title="Regime"),
                ),
                facet=alt.Facet("horizon:N", columns=3, title="Horizon"),
                tooltip=[
                    alt.Tooltip("horizon:N", title="Horizon"),
                    alt.Tooltip("lead_label:N", title="Lead"),
                    alt.Tooltip("regime:N", title="Regime"),
                    alt.Tooltip("hss:Q", title="HSS", format=".3f"),
                    alt.Tooltip("n_pairs:Q", title="n_pairs"),
                    alt.Tooltip("model:N", title="Model"),
                ],
            )
            .properties(title="HSS by lead and regime (long-term horizons)")
        )
        st.altair_chart(ovh_chart, use_container_width=True)
        st.caption(
            "⚠ Operational and hindcast are NOT sample-matched (different stations/dates/n). "
            "Where operational ≥ hindcast it may be a sample-composition artifact. "
            "Hover to inspect n_pairs — operational n << hindcast n."
        )
