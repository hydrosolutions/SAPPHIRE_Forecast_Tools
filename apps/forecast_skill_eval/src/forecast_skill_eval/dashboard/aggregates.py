"""Aggregate data-prep helpers for the skill-eval dashboard.

Logic ported from
doc/plans/working/forecast_skill_eval_figures/make_figures.py
(Phase-2, 2026-06-30). No Streamlit or matplotlib dependency.
"""

from __future__ import annotations

import math
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SHORT_TERM: list[str] = ["day", "pentad", "decade"]
LONG_TERM: list[str] = ["month", "quarter", "season"]
HORIZONS: list[str] = SHORT_TERM + LONG_TERM

PROVENANCE: dict[str, str] = {
    "day": "calculated",
    "pentad": "calculated",
    "decade": "calculated",
    "month": "official",
    "quarter": "aggregated_from_monthly",
    "season": "aggregated_from_monthly",
}

HCOLORS: dict[str, str] = {
    "day": "#1f77b4",
    "pentad": "#2ca02c",
    "decade": "#17becf",
    "month": "#ff7f0e",
    "quarter": "#d62728",
    "season": "#9467bd",
}

MODEL_FAMILY: dict[str, str] = {
    "LR": "lr",
    "LR_Base": "lr",
    "LR_SM": "lr",
    "LR_SM_DT": "lr",
    "LR_SM_ROF": "lr",
    "MC_ALD": "lr",
    "TFT": "ml",
    "TSMixer": "ml",
    "TiDE": "ml",
    "GBT": "ml",
    "SM_GBT": "ml",
    "SM_GBT_LR": "ml",
    "SM_GBT_Norm": "ml",
    "EM": "ensemble",
    "NE": "ensemble",
    "Skilled Mean": "ensemble",
    "Naive Mean": "naive",
}

FAMILY_COLOR: dict[str, str] = {
    "lr": "#4472C4",
    "ml": "#E97132",
    "ensemble": "#70AD47",
    "naive": "#9467bd",
}

FAMILY_LABEL: dict[str, str] = {
    "lr": "LR / Statistical",
    "ml": "ML / GBT",
    "ensemble": "Ensemble (skill-weighted)",
    "naive": "Unweighted mean",
}

FAMILY_ORDER: dict[str, int] = {"lr": 0, "ml": 1, "ensemble": 2, "naive": 3}

# Approved lead ranges for long-term fig4 (L4-L12 for month excluded)
FIG4_LEADS: dict[str, list[int]] = {
    "month": [0, 1, 2, 3],
    "quarter": [1, 2, 3, 4],
    "season": [0, 1, 2, 3],
}

# Canonical lead used in fig5 / persistence lookups
CANONICAL_LEADS: dict[str, int] = {"month": 0, "quarter": 1, "season": 0}

# Models excluded from best-model selection
BASELINES: frozenset[str] = frozenset({"Naive Mean", "Climatology"})

# Horizons shown in fig6 (seasonal POD): (horizon, lead_as_str_or_None, display_label)
FIG6_HORIZONS: list[tuple[str, str | None, str]] = [
    ("pentad", None, "pentad"),
    ("decade", None, "decade"),
    ("month", "0", "month L0"),
]

_H_DISPLAY_MAP: dict[str, str] = {
    "day": "day",
    "pentad": "pentad",
    "decade": "decade",
    "month": "month\nL0",
    "quarter": "quarter\nL1",
    "season": "season\nL0",
}

_SEASON_LABELS: dict[str, str] = {
    "non_irrigation": "Oct–Mar",
    "all": "All year",
    "irrigation": "Apr–Sep",
}

_BASELINES_EMPTY_COLUMNS: list[str] = [
    "horizon",
    "model",
    "regime",
    "season",
    "code",
    "basin",
    "norm_provenance",
    "lead",
    "pod",
    "pod_undefined",
    "far",
    "far_undefined",
    "hss",
    "hss_undefined",
    "n_pairs",
    "base_rate",
    "base_rate_undefined",
    "baseline",
    "comparison_model",
]

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def model_family(model: str) -> str:
    """Return the family key for a model name.

    Args:
        model: Model short name (e.g. "LR", "TFT").

    Returns:
        Family key string ("lr", "ml", "ensemble", "naive", or "other").
    """
    return MODEL_FAMILY.get(model, "other")


def model_sort_key(model: str) -> tuple:
    """Sort key: family order first, then alphabetical within family.

    Args:
        model: Model short name.

    Returns:
        Tuple of (family_order_int, model_lower_str) for sorting.
    """
    fam = model_family(model)
    return (FAMILY_ORDER.get(fam, 99), model.lower())


def base_horizon(h_label: str) -> str:
    """Return the base horizon string from a display label.

    Args:
        h_label: Display label such as "month L0" or "pentad".

    Returns:
        Base horizon string, e.g. "month" for "month L0", "pentad" for "pentad".
    """
    for h in HORIZONS:
        if h_label == h or h_label.startswith(h + " L"):
            return h
    return h_label


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _add_h_label(df: pd.DataFrame) -> pd.DataFrame:
    """Add ``lead_int`` and ``h_label`` columns to df (in-place on a copy).

    Args:
        df: DataFrame containing ``horizon`` and ``lead`` columns.

    Returns:
        Copy of df with ``lead_int`` (int) and ``h_label`` (str) columns added.
        Short-term rows get ``lead_int=-1`` and ``h_label=horizon``.
        Long-term rows get ``lead_int`` from the lead column and
        ``h_label`` like ``"month L0"``.
    """
    df = df.copy()

    def _row_lead_int(row: pd.Series) -> int:
        if row["horizon"] in SHORT_TERM:
            return -1
        lead_val = row["lead"]
        if isinstance(lead_val, float) and math.isnan(lead_val):
            return -1
        return int(lead_val)

    def _row_h_label(row: pd.Series) -> str:
        if row["horizon"] in SHORT_TERM:
            return str(row["horizon"])
        lead_val = row["lead"]
        if isinstance(lead_val, float) and math.isnan(lead_val):
            return str(row["horizon"])
        return f"{row['horizon']} L{int(lead_val)}"

    if df.empty:
        df["lead_int"] = pd.Series(dtype=int)
        df["h_label"] = pd.Series(dtype=str)
        return df
    df["lead_int"] = df.apply(_row_lead_int, axis=1)
    df["h_label"] = df.apply(_row_h_label, axis=1)
    return df


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_baselines(metrics_csv_path: str | Path) -> pd.DataFrame:
    """Read the sibling ``baselines.csv`` next to *metrics_csv_path*.

    Tolerates absence: returns an empty DataFrame with a known set of columns.
    The ``lead`` column is parsed as numeric (coerced).

    Args:
        metrics_csv_path: Path to ``contingency_metrics.csv``; the sibling
            ``baselines.csv`` is resolved from the same directory.

    Returns:
        DataFrame with baseline rows, or empty DataFrame with fallback columns.
    """
    try:
        baselines_path = Path(metrics_csv_path).parent / "baselines.csv"
        df = pd.read_csv(baselines_path)
        df["lead"] = pd.to_numeric(df["lead"], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame(columns=_BASELINES_EMPTY_COLUMNS)


def prep_pooled(
    df_all: pd.DataFrame,
    event: str = "below_norm",
    season: str = "all",
) -> pd.DataFrame:
    """Filter to canonical POOLED rows and annotate with h_label columns.

    Keeps rows where:
    - ``code == "POOLED"``
    - ``basin == "all"``
    - ``season == season``
    - ``regime == "operational"``
    - ``event == event``
    - ``norm_provenance`` matches the canonical provenance for each horizon
      (from :data:`PROVENANCE`)

    Then calls :func:`_add_h_label` to add ``lead_int`` and ``h_label``.

    Args:
        df_all: Full metrics DataFrame from :func:`~dashboard.data.load_metrics`.
        event: Event type to filter on (default ``"below_norm"``).
        season: Season value to filter on (default ``"all"``).

    Returns:
        Filtered and annotated copy; may be empty if no rows match.
    """
    mask = (
        (df_all["code"] == "POOLED")
        & (df_all["basin"] == "all")
        & (df_all["season"] == season)
        & (df_all["regime"] == "operational")
    )
    if "event" in df_all.columns:
        mask &= df_all["event"] == event

    df = df_all[mask].copy()

    # Filter to canonical norm_provenance per horizon
    prov_mask = df.apply(
        lambda row: PROVENANCE.get(row["horizon"], "") == row["norm_provenance"],
        axis=1,
    )
    df = df[prov_mask].copy()

    return _add_h_label(df)


def prep_performance_diagram(
    df_all: pd.DataFrame,
    event: str = "below_norm",
    season: str = "all",
) -> pd.DataFrame:
    """Prepare data for a Roebber performance diagram scatter plot (fig1).

    Calls :func:`prep_pooled`, drops undefined POD/FAR rows, and adds display
    columns for colour, shape, and lead label.

    Args:
        df_all: Full metrics DataFrame.
        event: Event type filter.
        season: Season filter.

    Returns:
        DataFrame with added columns: ``sr``, ``family``, ``family_color``,
        ``family_label``, ``horizon_color``, ``lead_label``.
    """
    df = prep_pooled(df_all, event=event, season=season)
    if df.empty:
        return df

    # Drop rows where either POD or FAR is undefined
    undef_mask = pd.Series(False, index=df.index)
    if "pod_undefined" in df.columns:
        undef_mask |= df["pod_undefined"].astype(bool)
    if "far_undefined" in df.columns:
        undef_mask |= df["far_undefined"].astype(bool)
    df = df[~undef_mask].copy()

    df["sr"] = 1.0 - df["far"]
    df["family"] = df["model"].apply(model_family)
    df["family_color"] = df["family"].map(FAMILY_COLOR).fillna("#888888")
    df["family_label"] = df["family"].map(FAMILY_LABEL).fillna("Other")
    df["horizon_color"] = df["horizon"].map(HCOLORS).fillna("#888888")
    df["lead_label"] = df["lead_int"].apply(lambda li: "—" if li == -1 else f"L{li}")
    return df


def _baseline_rows(
    baselines_df: pd.DataFrame,
    horizon: str,
    baseline_name: str,
    lead: int | None,
) -> pd.DataFrame:
    """Filter baselines_df to matching rows for a given horizon and baseline.

    Args:
        baselines_df: DataFrame from :func:`load_baselines`.
        horizon: Horizon string (e.g. ``"pentad"``).
        baseline_name: Baseline type (e.g. ``"climatology"``, ``"persistence"``).
        lead: Integer lead to filter by, or None to keep NaN-lead rows.

    Returns:
        Filtered subset; may be empty.
    """
    if baselines_df.empty:
        return baselines_df

    mask = (
        (baselines_df["code"] == "POOLED")
        & (baselines_df["baseline"] == baseline_name)
        & (baselines_df["regime"] == "operational")
        & (baselines_df["basin"] == "all")
        & (baselines_df["horizon"] == horizon)
    )
    if "season" in baselines_df.columns:
        mask &= baselines_df["season"] == "all"
    if "norm_provenance" in baselines_df.columns:
        canonical_prov = PROVENANCE.get(horizon, "")
        if canonical_prov:
            mask &= baselines_df["norm_provenance"] == canonical_prov

    df = baselines_df[mask].copy()

    # Lead filter
    df = df[df["lead"].isna()] if lead is None else df[df["lead"] == float(lead)]

    return df


def get_baseline_refs(
    baselines_df: pd.DataFrame,
    horizon: str,
    lead: int | None = None,
) -> dict:
    """Extract reference values from the baselines DataFrame for annotations.

    Args:
        baselines_df: DataFrame from :func:`load_baselines`.
        horizon: Horizon string.
        lead: Lead for the climatology lookup; persistence always uses the
            canonical lead from :data:`CANONICAL_LEADS` (or None for short-term).

    Returns:
        Dict with keys ``clim_base_rate`` (float or None),
        ``persistence_pod`` (float or None), ``persistence_hss`` (float or None).
    """
    result: dict = {
        "clim_base_rate": None,
        "persistence_pod": None,
        "persistence_hss": None,
    }
    if baselines_df.empty:
        return result

    # Climatology base_rate at the given lead
    clim_lead = lead if horizon in LONG_TERM else None
    clim_rows = _baseline_rows(baselines_df, horizon, "climatology", clim_lead)
    if not clim_rows.empty and "base_rate" in clim_rows.columns:
        row = clim_rows.iloc[0]
        undef = bool(row.get("base_rate_undefined", False))
        val = row["base_rate"]
        if not undef and not (isinstance(val, float) and math.isnan(val)):
            result["clim_base_rate"] = float(val)

    # Persistence: always use canonical lead
    pers_lead = CANONICAL_LEADS.get(horizon) if horizon in LONG_TERM else None
    pers_rows = _baseline_rows(baselines_df, horizon, "persistence", pers_lead)
    if not pers_rows.empty:
        row = pers_rows.iloc[0]
        pod_undef = bool(row.get("pod_undefined", False))
        pod_val = row.get("pod", float("nan"))
        if not pod_undef and not (isinstance(pod_val, float) and math.isnan(pod_val)):
            result["persistence_pod"] = float(pod_val)
        hss_undef = bool(row.get("hss_undefined", False))
        hss_val = row.get("hss", float("nan"))
        if not hss_undef and not (isinstance(hss_val, float) and math.isnan(hss_val)):
            result["persistence_hss"] = float(hss_val)

    return result


def prep_model_comparison_per_horizon(
    df_all: pd.DataFrame,
    horizon: str,
    event: str = "below_norm",
    season: str = "all",
) -> pd.DataFrame:
    """Prepare pooled data for a per-horizon model comparison chart (fig4).

    Args:
        df_all: Full metrics DataFrame.
        horizon: The horizon to show (e.g. ``"pentad"``).
        event: Event type filter.
        season: Season filter.

    Returns:
        Filtered DataFrame with ``family``, ``family_color``, ``family_label``
        columns added.  Short-term: only NaN-lead rows.  Long-term: only
        :data:`FIG4_LEADS` rows.  Rows where BOTH pod and far are undefined
        are dropped.
    """
    df = prep_pooled(df_all, event=event, season=season)
    if df.empty:
        return df

    df = df[df["horizon"] == horizon].copy()
    if df.empty:
        return df

    if horizon in LONG_TERM:
        approved_leads = FIG4_LEADS.get(horizon, [])
        df = df[df["lead_int"].isin(approved_leads)].copy()
    else:
        df = df[df["lead_int"] == -1].copy()

    # Drop rows where BOTH pod_undefined AND far_undefined
    both_undef = pd.Series(False, index=df.index)
    if "pod_undefined" in df.columns and "far_undefined" in df.columns:
        both_undef = df["pod_undefined"].astype(bool) & df["far_undefined"].astype(bool)
    df = df[~both_undef].copy()

    df["family"] = df["model"].apply(model_family)
    df["family_color"] = df["family"].map(FAMILY_COLOR).fillna("#888888")
    df["family_label"] = df["family"].map(FAMILY_LABEL).fillna("Other")
    return df


def prep_skill_ladder(
    df_all: pd.DataFrame,
    baselines_df: pd.DataFrame,
    event: str = "below_norm",
    season: str = "all",
) -> pd.DataFrame:
    """Prepare the three-way skill ladder data (fig5).

    For each horizon returns three rows: Climatology (HSS=0), Persistence, and
    the best non-baseline model.

    Args:
        df_all: Full metrics DataFrame.
        baselines_df: Baseline DataFrame from :func:`load_baselines`.
        event: Event type filter.
        season: Season filter.

    Returns:
        Long DataFrame with columns: ``horizon``, ``h_display``, ``series``,
        ``hss``, ``model``.
    """
    pooled_all = prep_pooled(df_all, event=event, season=season)
    rows: list[dict] = []

    for horizon in HORIZONS:
        h_display = _H_DISPLAY_MAP.get(horizon, horizon)
        df_h = pooled_all[pooled_all["horizon"] == horizon].copy()

        # Filter to canonical lead for long-term
        if horizon in LONG_TERM:
            canonical_lead = CANONICAL_LEADS.get(horizon, 0)
            df_h = df_h[df_h["lead_int"] == canonical_lead].copy()

        # Exclude undefined HSS rows
        if "hss_undefined" in df_h.columns:
            df_h = df_h[~df_h["hss_undefined"].astype(bool)].copy()

        # Row 1: Climatology (HSS = 0 by definition)
        rows.append(
            {
                "horizon": horizon,
                "h_display": h_display,
                "series": "Climatology",
                "hss": 0.0,
                "model": "climatology",
            }
        )

        # Row 2: Persistence from baselines
        pers_lead = CANONICAL_LEADS.get(horizon) if horizon in LONG_TERM else None
        refs = get_baseline_refs(baselines_df, horizon, lead=pers_lead)
        pers_hss = refs.get("persistence_hss")
        rows.append(
            {
                "horizon": horizon,
                "h_display": h_display,
                "series": "Persistence",
                "hss": pers_hss if pers_hss is not None else float("nan"),
                "model": "persistence",
            }
        )

        # Row 3: Best non-baseline model
        best_hss: float = float("nan")
        best_model: str = ""
        if not df_h.empty and "hss" in df_h.columns:
            candidates = df_h[~df_h["model"].isin(BASELINES)].copy()
            if not candidates.empty:
                candidates = candidates.dropna(subset=["hss"])
                if not candidates.empty:
                    idx_max = candidates["hss"].idxmax()
                    best_hss = float(candidates.loc[idx_max, "hss"])
                    best_model = str(candidates.loc[idx_max, "model"])

        series_label = f"Best model ({best_model})" if best_model else "Best model"
        rows.append(
            {
                "horizon": horizon,
                "h_display": h_display,
                "series": series_label,
                "hss": best_hss,
                "model": best_model,
            }
        )

    return pd.DataFrame(rows)


def prep_seasonal_pod(
    df_all: pd.DataFrame,
    event: str = "below_norm",
) -> pd.DataFrame:
    """Prepare seasonal POD breakdown for the EM model (fig6).

    Reads all three season values for the horizons in :data:`FIG6_HORIZONS`.
    Does NOT filter by a single season.

    Args:
        df_all: Full metrics DataFrame.
        event: Event type filter.

    Returns:
        DataFrame with columns: ``h_label``, ``season``, ``season_label``,
        ``pod``, ``pod_ci_lower``, ``pod_ci_upper``, ``n_pairs``, ``pod_undefined``.
    """
    rows: list[dict] = []

    for h, lead_str, h_label in FIG6_HORIZONS:
        canonical_prov = PROVENANCE.get(h, "")
        for season in ["non_irrigation", "all", "irrigation"]:
            mask = (
                (df_all["code"] == "POOLED")
                & (df_all["basin"] == "all")
                & (df_all["regime"] == "operational")
                & (df_all["horizon"] == h)
                & (df_all["model"] == "EM")
                & (df_all["season"] == season)
            )
            if canonical_prov and "norm_provenance" in df_all.columns:
                mask &= df_all["norm_provenance"] == canonical_prov
            if "event" in df_all.columns:
                mask &= df_all["event"] == event

            df_s = df_all[mask].copy()

            # Lead filter
            if lead_str is None:
                df_s = df_s[df_s["lead"].isna()]
            else:
                df_s = df_s[df_s["lead"] == float(lead_str)]

            season_label = _SEASON_LABELS.get(season, season)

            if df_s.empty:
                rows.append(
                    {
                        "h_label": h_label,
                        "season": season,
                        "season_label": season_label,
                        "pod": float("nan"),
                        "pod_ci_lower": float("nan"),
                        "pod_ci_upper": float("nan"),
                        "n_pairs": 0,
                        "pod_undefined": True,
                    }
                )
                continue

            row = df_s.iloc[0]
            pod_undef = bool(row.get("pod_undefined", False))
            if pod_undef:
                rows.append(
                    {
                        "h_label": h_label,
                        "season": season,
                        "season_label": season_label,
                        "pod": float("nan"),
                        "pod_ci_lower": float("nan"),
                        "pod_ci_upper": float("nan"),
                        "n_pairs": int(row.get("n_pairs", 0)),
                        "pod_undefined": True,
                    }
                )
            else:
                rows.append(
                    {
                        "h_label": h_label,
                        "season": season,
                        "season_label": season_label,
                        "pod": float(row["pod"]),
                        "pod_ci_lower": float(row.get("pod_ci_lower", float("nan"))),
                        "pod_ci_upper": float(row.get("pod_ci_upper", float("nan"))),
                        "n_pairs": int(row.get("n_pairs", 0)),
                        "pod_undefined": False,
                    }
                )

    return pd.DataFrame(rows)


def prep_op_vs_hindcast(
    df_all: pd.DataFrame,
    event: str = "below_norm",
    season: str = "all",
) -> pd.DataFrame:
    """Prepare operational vs hindcast HSS comparison for long-term horizons (fig3).

    For each long-term horizon, finds the best model (preferring one appearing
    in both regimes) and collects one row per (regime, lead_int).

    Args:
        df_all: Full metrics DataFrame.
        event: Event type filter.
        season: Season filter.

    Returns:
        DataFrame with columns: ``horizon``, ``lead_int``, ``lead_label``,
        ``regime``, ``hss``, ``n_pairs``, ``model``, ``hss_undefined``.
        Empty DataFrame with those columns if no data is found.
    """
    _EMPTY_COLS = [
        "horizon",
        "lead_int",
        "lead_label",
        "regime",
        "hss",
        "n_pairs",
        "model",
        "hss_undefined",
    ]
    rows: list[dict] = []

    for horizon in LONG_TERM:
        canonical_prov = PROVENANCE.get(horizon, "")
        mask_base = (
            (df_all["code"] == "POOLED")
            & (df_all["basin"] == "all")
            & (df_all["horizon"] == horizon)
            & (df_all["season"] == season)
        )
        if canonical_prov and "norm_provenance" in df_all.columns:
            mask_base &= df_all["norm_provenance"] == canonical_prov
        if "event" in df_all.columns:
            mask_base &= df_all["event"] == event

        df_h = df_all[mask_base & df_all["regime"].isin(["operational", "hindcast"])].copy()
        if df_h.empty:
            continue

        df_h = _add_h_label(df_h)

        # For month: limit to lead_int <= 3
        if horizon == "month":
            df_h = df_h[df_h["lead_int"] <= 3].copy()

        df_op = df_h[df_h["regime"] == "operational"]
        df_hc = df_h[df_h["regime"] == "hindcast"]

        if df_op.empty and df_hc.empty:
            continue

        # Find best model: prefer one appearing in both regimes
        op_models = set(df_op["model"].unique()) if not df_op.empty else set()
        hc_models = set(df_hc["model"].unique()) if not df_hc.empty else set()
        shared_models = op_models & hc_models

        if shared_models:
            # Among shared models, pick the one with highest total n_pairs
            df_shared = df_h[df_h["model"].isin(shared_models)]
            model_npairs = df_shared.groupby("model")["n_pairs"].sum()
            best_model = str(model_npairs.idxmax())
        else:
            # Fallback: best operational model by n_pairs
            if not df_op.empty:
                model_npairs = df_op.groupby("model")["n_pairs"].sum()
                best_model = str(model_npairs.idxmax())
            elif not df_hc.empty:
                model_npairs = df_hc.groupby("model")["n_pairs"].sum()
                best_model = str(model_npairs.idxmax())
            else:
                continue

        df_best = df_h[df_h["model"] == best_model].copy()
        for _, row in df_best.iterrows():
            lead_i = int(row["lead_int"])
            hss_undef = bool(row.get("hss_undefined", False))
            hss_val = row.get("hss", float("nan"))
            rows.append(
                {
                    "horizon": horizon,
                    "lead_int": lead_i,
                    "lead_label": f"L{lead_i}",
                    "regime": str(row["regime"]),
                    "hss": float(hss_val),
                    "n_pairs": int(row.get("n_pairs", 0)),
                    "model": best_model,
                    "hss_undefined": hss_undef,
                }
            )

    if not rows:
        return pd.DataFrame(columns=_EMPTY_COLS)
    return pd.DataFrame(rows)
