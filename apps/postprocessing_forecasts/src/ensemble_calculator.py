"""Create ensemble forecasts from pre-calculated skill metrics.

Extracted from forecast_library.py calculate_skill_metrics() lines
1944-2176.  This module lets the operational entry point create ensembles
WITHOUT recalculating skill metrics from scratch.
"""

import logging

import numpy as np
import pandas as pd
from src.postprocessing_tools import enforce_quantile_monotonicity, forecast_target_date

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def composition_agg(model_shorts: pd.Series) -> str:
    """Build composition string from model_short values.

    >>> composition_agg(pd.Series(["LR", "TFT", "TiDE"]))
    'LR, TFT, TiDE'
    """
    return ", ".join(sorted(model_shorts.unique()))


def is_multi_model_composition(composition: str) -> bool:
    """True if composition contains 2+ models.

    >>> is_multi_model_composition('LR, TFT')
    True
    >>> is_multi_model_composition('TFT')
    False
    """
    return bool(composition) and "," in composition


# ---------------------------------------------------------------------------
# Main public functions
# ---------------------------------------------------------------------------


def filter_for_highly_skilled_forecasts(
    skill_stats: pd.DataFrame,
    threshold_sdivsigma: float | str | None = None,
    threshold_accuracy: float | str | None = None,
    threshold_nse: float | str | None = None,
) -> pd.DataFrame:
    """Filter skill metrics — delegates to skill_metrics module.

    Preserved for backward compatibility with existing callers.
    """
    from src.skill_metrics import (
        filter_for_highly_skilled_forecasts as _canonical,
    )

    overrides = {}
    if threshold_sdivsigma is not None:
        overrides["sdivsigma"] = threshold_sdivsigma
    if threshold_accuracy is not None:
        overrides["accuracy"] = threshold_accuracy
    if threshold_nse is not None:
        overrides["nse"] = threshold_nse
    return _canonical(skill_stats, **overrides)


def create_ensemble_forecasts(
    forecasts: pd.DataFrame,
    skill_stats: pd.DataFrame,
    period_col: str,
    period_in_month_col: str,
    get_period_in_month_func,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create ensemble mean (EM) forecasts using pre-calculated skill metrics.

    EM skill metrics are NOT calculated here — they are produced by the
    annual recalculation script (recalculate_skill_metrics.py). This
    function only creates the EM forecast rows and passes skill_stats
    through unchanged.

    Steps:
    1. Filter skill_stats for highly skilled models
    2. Use merge to get qualifying forecast rows
    3. Exclude NE (neural ensemble) from candidates
    4. Group by [date, code], mean(forecasted_discharge)
    5. Build composition string: "LR, TFT"
    6. Discard single-model or empty ensembles
    7. Calculate period_in_month for ensemble rows
    8. Merge ensemble rows into forecasts (outer join)

    Args:
        forecasts: Simulated/modelled forecasts with columns
            [code, date, <period_col>, forecasted_discharge,
             model_short, <period_in_month_col>].
        skill_stats: Pre-calculated skill metrics with columns
            [<period_col>, code, model_short, sdivsigma,
             nse, delta, accuracy, mae, n_pairs].
        period_col: 'pentad_in_year' or 'decad_in_year'.
        period_in_month_col: 'pentad_in_month' or 'decad_in_month'.
        get_period_in_month_func: Function to compute period in month
            from a date (e.g. tl.get_pentad or tl.get_decad_in_month).

    Returns:
        joint_forecasts: forecasts with ensemble rows appended.
        skill_stats: passed through unchanged.
    """
    # Step 1: filter for highly skilled models
    skill_stats_ensemble = filter_for_highly_skilled_forecasts(skill_stats)
    logger.debug("Highly skilled models: %d rows", len(skill_stats_ensemble))

    # Normalize merge key types to avoid object/int64 mismatches.
    # period_col values are integers (1-72 for pentad, 1-36 for decad).
    for df in (forecasts, skill_stats_ensemble):
        if period_col in df.columns:
            df[period_col] = pd.to_numeric(df[period_col], errors="coerce")
        if "code" in df.columns:
            df["code"] = df["code"].astype(str)

    # Step 2: use merge to get qualifying forecast rows
    merge_keys = [period_col, "code", "model_short"]
    qualifying = forecasts.merge(
        skill_stats_ensemble[merge_keys].drop_duplicates(),
        on=merge_keys,
        how="inner",
    )
    # Drop NaN forecasts
    qualifying = qualifying.dropna(subset=["forecasted_discharge"]).copy()

    # --- PP-027: Gate A diagnostic — codes lost at skill-merge + NaN drop ---
    _input_codes = set(forecasts["code"].unique())
    _post_merge_codes = set(qualifying["code"].unique()) if not qualifying.empty else set()
    _gate_a_skipped = _input_codes - _post_merge_codes
    if _gate_a_skipped:
        logger.info(
            "EM: %d station(s) have no skilled forecasts after skill-merge + NaN drop: %s",
            len(_gate_a_skipped),
            ", ".join(sorted(_gate_a_skipped)),
        )

    # Step 3: exclude NE (neural ensemble) from ensemble candidates
    qualifying = qualifying[qualifying["model_short"] != "NE"].copy()

    # --- PP-027: Gate B diagnostic — codes lost to NE-only models ---
    _post_ne_codes = set(qualifying["code"].unique()) if not qualifying.empty else set()
    _ne_only_codes = _post_merge_codes - _post_ne_codes
    if _ne_only_codes:
        logger.info(
            "EM: %d station(s) dropped — only NE model qualified: %s",
            len(_ne_only_codes),
            ", ".join(sorted(_ne_only_codes)),
        )

    if qualifying.empty:
        logger.info("No qualifying forecasts for ensemble creation")
        return forecasts.copy(), skill_stats.copy()

    # Step 4: group by [period, date, code], compute mean forecasted_discharge
    # Also average quantile columns when present (vincentization —
    # same pattern as monthly ensembles at line 276-278).
    from src.skill_metrics import _QUANTILE_COLS

    agg_dict = {
        "forecasted_discharge": "mean",
        "model_short": composition_agg,
    }
    for qcol in _QUANTILE_COLS:
        if qcol in qualifying.columns:
            agg_dict[qcol] = "mean"

    ensemble_avg = qualifying.groupby([period_col, "date", "code"]).agg(agg_dict).reset_index()
    ensemble_avg = enforce_quantile_monotonicity(
        ensemble_avg, [c for c in _QUANTILE_COLS if c in ensemble_avg.columns]
    )
    # model_short now holds the composition string (e.g. "LR, TFT")
    ensemble_avg = ensemble_avg.rename(columns={"model_short": "composition"})
    ensemble_avg["model_short"] = "EM"

    # Step 5+6: discard single-model or empty ensembles
    ensemble_avg = ensemble_avg[
        ensemble_avg["composition"].apply(is_multi_model_composition)
    ].copy()

    # --- PP-027: Gate C diagnostic — codes with only 1 qualifying model ---
    _post_comp_codes = set(ensemble_avg["code"].unique()) if not ensemble_avg.empty else set()
    _single_model_codes = _post_ne_codes - _post_comp_codes
    if _single_model_codes:
        logger.info(
            "EM: %d station(s) dropped — only 1 qualifying model (need 2+): %s",
            len(_single_model_codes),
            ", ".join(sorted(_single_model_codes)),
        )

    if ensemble_avg.empty:
        logger.info("No multi-model ensembles after filtering")
        return forecasts.copy(), skill_stats.copy()

    # Step 7: calculate period_in_month for ensemble rows
    # (production date -> target period start)
    ensemble_avg[period_in_month_col] = forecast_target_date(ensemble_avg["date"]).apply(
        get_period_in_month_func
    )

    # Step 8: append ensemble rows to forecasts via pd.concat
    # (matches the monthly pattern using _append_to_joint / pd.concat
    # instead of the fragile outer merge that put payload in join keys)
    if "composition" not in forecasts.columns:
        forecasts = forecasts.copy()
        forecasts["composition"] = ""

    joint_forecasts = pd.concat(
        [forecasts, ensemble_avg],
        ignore_index=True,
    )

    return joint_forecasts, skill_stats.copy()


def create_monthly_ensemble_forecasts(
    forecasts: pd.DataFrame,
    skill_stats: pd.DataFrame,
) -> pd.DataFrame:
    """Create EM, Skilled Mean, Naive Mean for monthly forecasts.

    Uses pre-calculated skill metrics to determine highly skilled
    models, then averages their forecasts (point + quantiles).
    Does NOT require observations — skill metrics are pre-calculated.

    Horizon-type filtering: This function receives ONLY monthly
    forecasts (pre-filtered by the data reader). It never mixes
    forecasts from different horizon types (pentad, decad, etc.).

    Args:
        forecasts: Monthly forecasts with columns:
            code, year, month, month_in_year, model_short,
            forecasted_discharge, q05-q95, valid_from, valid_to,
            date, flag.
            Must contain only monthly-horizon forecasts.
        skill_stats: Pre-calculated monthly skill metrics with:
            month_in_year, code, model_short, sdivsigma, nse,
            delta, accuracy, mae, n_pairs.

    Returns:
        DataFrame with ensemble rows appended to input forecasts.
        Ensemble rows have model_short in {'EM', 'Skilled Mean',
        'Naive Mean'} and a 'composition' column.
    """
    from src.skill_metrics import (
        _QUANTILE_COLS,
        _append_to_joint,
        filter_for_highly_skilled_forecasts,
    )

    if forecasts.empty:
        return pd.DataFrame()

    if skill_stats.empty:
        logger.warning("Empty skill metrics — returning forecasts without ensembles")
        return forecasts.copy()

    # Ensure month_in_year exists
    if "month_in_year" not in forecasts.columns and "month" in forecasts.columns:
        forecasts = forecasts.copy()
        forecasts["month_in_year"] = forecasts["month"]

    # Ensure forecasted_discharge exists (may be q50 from API)
    if "forecasted_discharge" not in forecasts.columns and "q50" in forecasts.columns:
        forecasts = forecasts.copy()
        forecasts["forecasted_discharge"] = forecasts["q50"].astype(float)

    joint = forecasts.copy()
    baselines = {"EM", "Naive Mean", "Skilled Mean"}

    # Build groupby keys — include horizon_value when available (PP-032).
    # Missing for CSV-sourced data or the maintenance path.
    group_cols = ["year", "month", "code"]
    if "horizon_value" in forecasts.columns:
        group_cols.append("horizon_value")

    # --- EM (threshold-filtered average) ---
    skill_filtered = filter_for_highly_skilled_forecasts(skill_stats)
    merge_keys = ["month_in_year", "code", "model_short"]

    # Normalize types for merge
    for df in (joint, skill_filtered):
        if "month_in_year" in df.columns:
            df["month_in_year"] = pd.to_numeric(df["month_in_year"], errors="coerce")
        if "code" in df.columns:
            df["code"] = df["code"].astype(str)

    qualifying = joint.merge(
        skill_filtered[merge_keys].drop_duplicates(),
        on=merge_keys,
        how="inner",
    )
    qualifying = qualifying[~qualifying["model_short"].isin(baselines)].copy()
    qualifying = qualifying.dropna(subset=["forecasted_discharge"]).copy()

    n_models = joint[~joint["model_short"].isin(baselines)]["model_short"].nunique()

    if n_models > 1 and not qualifying.empty:
        em_agg = {
            "month_in_year": "first",
            "forecasted_discharge": "mean",
            "model_short": composition_agg,
        }
        for qcol in _QUANTILE_COLS:
            if qcol in qualifying.columns:
                em_agg[qcol] = "mean"
        for dcol in ("valid_from", "valid_to", "date"):
            if dcol in qualifying.columns:
                # "first" for date: assumes all models in a
                # (year, month, code, horizon_value) group share
                # the same issue date (single pipeline invocation).
                em_agg[dcol] = "first"

        em_avg = qualifying.groupby(group_cols).agg(em_agg).reset_index()
        em_avg = enforce_quantile_monotonicity(
            em_avg, [c for c in _QUANTILE_COLS if c in em_avg.columns]
        )
        em_avg = em_avg.rename(columns={"model_short": "composition"})
        em_avg["model_short"] = "EM"

        # Discard single-model ensembles
        em_avg = em_avg[em_avg["composition"].apply(is_multi_model_composition)].copy()

        if not em_avg.empty:
            em_avg["flag"] = 0
            joint = _append_to_joint(joint, em_avg)

    # --- Skilled Mean (1/MAE weighted average) ---
    joint = _add_skilled_mean_monthly(
        joint,
        skill_filtered,
        baselines,
        _QUANTILE_COLS,
        group_cols,
    )

    # --- Naive Mean (unweighted all-model average) ---
    joint = _add_naive_mean_monthly(
        joint,
        baselines,
        _QUANTILE_COLS,
        group_cols,
    )

    return joint


def _add_skilled_mean_monthly(
    joint: pd.DataFrame,
    skill_filtered: pd.DataFrame,
    baselines: set,
    quantile_cols: list,
    group_cols: list[str] | None = None,
) -> pd.DataFrame:
    """Add Skilled Mean rows (1/MAE weighted) to joint forecasts.

    Uses the same threshold-filtered model pool as EM.
    """
    from src.skill_metrics import _append_to_joint

    if group_cols is None:
        group_cols = ["year", "month", "code"]

    filtered = skill_filtered[~skill_filtered["model_short"].isin(baselines)].copy()
    if filtered.empty:
        return joint

    mae_df = filtered[["month_in_year", "code", "model_short", "mae"]].copy()
    mae_df = mae_df.dropna(subset=["mae"])
    if mae_df.empty:
        return joint

    # Compute weights: w_i = 1 / (MAE_i + eps)
    mean_mae = mae_df["mae"].mean()
    eps = mean_mae / 100.0 if mean_mae > 0 else 1e-10
    mae_df["weight"] = 1.0 / (mae_df["mae"] + eps)

    qualifying_keys = mae_df[["month_in_year", "code", "model_short"]].drop_duplicates()

    # Filter joint (non-baseline) to qualifying models
    pool = joint[~joint["model_short"].isin(baselines)].copy()
    pool = pool.merge(
        qualifying_keys,
        on=["month_in_year", "code", "model_short"],
        how="inner",
    )
    pool = pool.dropna(subset=["forecasted_discharge"]).copy()
    if pool.empty:
        return joint

    # Attach weights
    pool = pool.merge(
        mae_df[["month_in_year", "code", "model_short", "weight"]],
        on=["month_in_year", "code", "model_short"],
        how="left",
    )

    def _weighted_mean(group, col):
        w = pool.loc[group.index, "weight"].to_numpy()
        d = group.to_numpy()
        return np.average(d, weights=w)

    sm_agg = {
        "month_in_year": ("month_in_year", "first"),
        "forecasted_discharge": (
            "forecasted_discharge",
            lambda x: _weighted_mean(x, "forecasted_discharge"),
        ),
        "composition": ("model_short", composition_agg),
    }
    for qcol in quantile_cols:
        if qcol in pool.columns:
            sm_agg[qcol] = (
                qcol,
                lambda x, _c=qcol: _weighted_mean(x, _c),
            )
    for dcol in ("valid_from", "valid_to", "date"):
        if dcol in pool.columns:
            # "first" for date: assumes all models in a
            # (year, month, code, horizon_value) group share
            # the same issue date (single pipeline invocation).
            sm_agg[dcol] = (dcol, "first")

    sm_avg = (
        pool.groupby(group_cols)
        .agg(
            **sm_agg,
        )
        .reset_index()
    )
    sm_avg = enforce_quantile_monotonicity(
        sm_avg, [c for c in quantile_cols if c in sm_avg.columns]
    )
    sm_avg["model_short"] = "Skilled Mean"

    # Discard single-model groups
    sm_avg = sm_avg[sm_avg["composition"].apply(is_multi_model_composition)].copy()

    if not sm_avg.empty:
        sm_avg["flag"] = 0
        joint = _append_to_joint(joint, sm_avg)

    return joint


def _add_naive_mean_monthly(
    joint: pd.DataFrame,
    baselines: set,
    quantile_cols: list,
    group_cols: list[str] | None = None,
) -> pd.DataFrame:
    """Add Naive Mean rows (unweighted all-model average) to joint."""
    from src.skill_metrics import _append_to_joint

    if group_cols is None:
        group_cols = ["year", "month", "code"]

    pool = joint[~joint["model_short"].isin(baselines)].copy()
    pool = pool.dropna(subset=["forecasted_discharge"]).copy()
    if pool.empty:
        return joint

    naive_agg = {
        "month_in_year": "first",
        "forecasted_discharge": "mean",
        "model_short": composition_agg,
    }
    for qcol in quantile_cols:
        if qcol in pool.columns:
            naive_agg[qcol] = "mean"
    for dcol in ("valid_from", "valid_to", "date"):
        if dcol in pool.columns:
            # "first" for date: assumes all models in a
            # (year, month, code, horizon_value) group share
            # the same issue date (single pipeline invocation).
            naive_agg[dcol] = "first"

    naive_avg = pool.groupby(group_cols).agg(naive_agg).reset_index()
    naive_avg = enforce_quantile_monotonicity(
        naive_avg, [c for c in quantile_cols if c in naive_avg.columns]
    )
    naive_avg = naive_avg.rename(columns={"model_short": "composition"})
    naive_avg["model_short"] = "Naive Mean"

    # Discard single-model groups
    naive_avg = naive_avg[naive_avg["composition"].apply(is_multi_model_composition)].copy()

    if not naive_avg.empty:
        naive_avg["flag"] = 0
        joint = _append_to_joint(joint, naive_avg)

    return joint


# ---------------------------------------------------------------------------
# Quarterly ensemble creation
# ---------------------------------------------------------------------------


def create_quarterly_ensemble_forecasts(
    forecasts: pd.DataFrame,
    skill_stats: pd.DataFrame,
) -> pd.DataFrame:
    """Create EM, Skilled Mean, Naive Mean for quarterly forecasts.

    Uses pre-calculated quarterly skill metrics. Same algorithm as
    monthly but with quarter_in_year instead of month_in_year.

    Args:
        forecasts: Quarterly forecasts with columns:
            code, year, quarter_in_year, model_short,
            forecasted_discharge, q05-q95.
        skill_stats: Pre-calculated quarterly skill metrics with:
            quarter_in_year, code, model_short, sdivsigma, nse,
            delta, accuracy, mae, n_pairs.

    Returns:
        DataFrame with ensemble rows appended to input forecasts.
    """
    return _create_aggregated_ensemble_forecasts(
        forecasts,
        skill_stats,
        period_col="quarter_in_year",
        time_group_cols=["year", "quarter_in_year", "code"],
    )


def create_seasonal_ensemble_forecasts(
    forecasts: pd.DataFrame,
    skill_stats: pd.DataFrame,
) -> pd.DataFrame:
    """Create EM, Skilled Mean, Naive Mean for seasonal forecasts.

    Uses pre-calculated seasonal skill metrics.

    Args:
        forecasts: Seasonal forecasts with columns:
            code, season_year, season_in_year, model_short,
            forecasted_discharge, q05-q95.
        skill_stats: Pre-calculated seasonal skill metrics with:
            season_in_year, code, model_short, sdivsigma, nse,
            delta, accuracy, mae, n_pairs.

    Returns:
        DataFrame with ensemble rows appended to input forecasts.
    """
    return _create_aggregated_ensemble_forecasts(
        forecasts,
        skill_stats,
        period_col="season_in_year",
        time_group_cols=["season_year", "season_in_year", "code"],
    )


def _create_aggregated_ensemble_forecasts(
    forecasts: pd.DataFrame,
    skill_stats: pd.DataFrame,
    period_col: str,
    time_group_cols: list[str],
) -> pd.DataFrame:
    """Shared implementation for quarterly/seasonal ensemble creation.

    Mirrors create_monthly_ensemble_forecasts() with parameterized
    column names.
    """
    from src.skill_metrics import (
        _QUANTILE_COLS,
        filter_for_highly_skilled_forecasts,
    )

    if forecasts.empty:
        return pd.DataFrame()

    if skill_stats.empty:
        logger.warning("Empty skill metrics — returning forecasts without ensembles")
        return forecasts.copy()

    # Ensure forecasted_discharge exists (may be q50 from API)
    if "forecasted_discharge" not in forecasts.columns and "q50" in forecasts.columns:
        forecasts = forecasts.copy()
        forecasts["forecasted_discharge"] = forecasts["q50"].astype(float)

    joint = forecasts.copy()
    baselines = {"EM", "Naive Mean", "Skilled Mean"}

    # --- EM (threshold-filtered average) ---
    skill_filtered = filter_for_highly_skilled_forecasts(skill_stats)
    merge_keys = [period_col, "code", "model_short"]

    # Normalize types for merge
    for df in (joint, skill_filtered):
        if period_col in df.columns:
            df[period_col] = pd.to_numeric(df[period_col], errors="coerce")
        if "code" in df.columns:
            df["code"] = df["code"].astype(str)

    qualifying = joint.merge(
        skill_filtered[merge_keys].drop_duplicates(),
        on=merge_keys,
        how="inner",
    )
    qualifying = qualifying[~qualifying["model_short"].isin(baselines)].copy()
    qualifying = qualifying.dropna(subset=["forecasted_discharge"]).copy()

    n_models = joint[~joint["model_short"].isin(baselines)]["model_short"].nunique()

    if n_models > 1 and not qualifying.empty:
        em_agg = {
            "forecasted_discharge": "mean",
            "model_short": composition_agg,
        }
        if period_col not in time_group_cols:
            em_agg[period_col] = "first"
        for qcol in _QUANTILE_COLS:
            if qcol in qualifying.columns:
                em_agg[qcol] = "mean"
        for dcol in ("valid_from", "valid_to", "date"):
            if dcol in qualifying.columns:
                em_agg[dcol] = "first"

        em_avg = qualifying.groupby(time_group_cols).agg(em_agg).reset_index()
        em_avg = enforce_quantile_monotonicity(
            em_avg, [c for c in _QUANTILE_COLS if c in em_avg.columns]
        )
        em_avg = em_avg.rename(columns={"model_short": "composition"})
        em_avg["model_short"] = "EM"

        em_avg = em_avg[em_avg["composition"].apply(is_multi_model_composition)].copy()

        if not em_avg.empty:
            em_avg["flag"] = 0
            joint = _append_aggregated_to_joint(joint, em_avg)

    # --- Skilled Mean (1/MAE weighted) ---
    joint = _add_skilled_mean_aggregated_ens(
        joint,
        skill_filtered,
        baselines,
        _QUANTILE_COLS,
        period_col,
        time_group_cols,
    )

    # --- Naive Mean (unweighted) ---
    joint = _add_naive_mean_aggregated_ens(
        joint,
        baselines,
        _QUANTILE_COLS,
        period_col,
        time_group_cols,
    )

    return joint


def _add_skilled_mean_aggregated_ens(
    joint,
    skill_filtered,
    baselines,
    quantile_cols,
    period_col,
    time_group_cols,
):
    """Add Skilled Mean rows for quarterly/seasonal ensemble creation."""
    filtered = skill_filtered[~skill_filtered["model_short"].isin(baselines)].copy()
    if filtered.empty:
        return joint

    mae_df = filtered[[period_col, "code", "model_short", "mae"]].copy()
    mae_df = mae_df.dropna(subset=["mae"])
    if mae_df.empty:
        return joint

    mean_mae = mae_df["mae"].mean()
    eps = mean_mae / 100.0 if mean_mae > 0 else 1e-10
    mae_df["weight"] = 1.0 / (mae_df["mae"] + eps)

    qualifying_keys = mae_df[[period_col, "code", "model_short"]].drop_duplicates()

    pool = joint[~joint["model_short"].isin(baselines)].copy()
    pool = pool.merge(
        qualifying_keys,
        on=[period_col, "code", "model_short"],
        how="inner",
    )
    pool = pool.dropna(subset=["forecasted_discharge"]).copy()
    if pool.empty:
        return joint

    pool = pool.merge(
        mae_df[[period_col, "code", "model_short", "weight"]],
        on=[period_col, "code", "model_short"],
        how="left",
    )

    def _weighted_mean(group, col):
        w = pool.loc[group.index, "weight"].to_numpy()
        d = group.to_numpy()
        return np.average(d, weights=w)

    sm_agg = {
        "forecasted_discharge": (
            "forecasted_discharge",
            lambda x: _weighted_mean(x, "forecasted_discharge"),
        ),
        "composition": ("model_short", composition_agg),
    }
    if period_col not in time_group_cols:
        sm_agg[period_col] = (period_col, "first")
    for qcol in quantile_cols:
        if qcol in pool.columns:
            sm_agg[qcol] = (qcol, lambda x, _c=qcol: _weighted_mean(x, _c))
    for dcol in ("valid_from", "valid_to", "date"):
        if dcol in pool.columns:
            sm_agg[dcol] = (dcol, "first")

    sm_avg = pool.groupby(time_group_cols).agg(**sm_agg).reset_index()
    sm_avg = enforce_quantile_monotonicity(
        sm_avg, [c for c in quantile_cols if c in sm_avg.columns]
    )
    sm_avg["model_short"] = "Skilled Mean"

    sm_avg = sm_avg[sm_avg["composition"].apply(is_multi_model_composition)].copy()

    if not sm_avg.empty:
        sm_avg["flag"] = 0
        joint = _append_aggregated_to_joint(joint, sm_avg)

    return joint


def _add_naive_mean_aggregated_ens(
    joint,
    baselines,
    quantile_cols,
    period_col,
    time_group_cols,
):
    """Add Naive Mean rows for quarterly/seasonal ensemble creation."""
    pool = joint[~joint["model_short"].isin(baselines)].copy()
    pool = pool.dropna(subset=["forecasted_discharge"]).copy()
    if pool.empty:
        return joint

    naive_agg = {
        "forecasted_discharge": "mean",
        "model_short": composition_agg,
    }
    if period_col not in time_group_cols:
        naive_agg[period_col] = "first"
    for qcol in quantile_cols:
        if qcol in pool.columns:
            naive_agg[qcol] = "mean"
    for dcol in ("valid_from", "valid_to", "date"):
        if dcol in pool.columns:
            naive_agg[dcol] = "first"

    naive_avg = pool.groupby(time_group_cols).agg(naive_agg).reset_index()
    naive_avg = enforce_quantile_monotonicity(
        naive_avg, [c for c in quantile_cols if c in naive_avg.columns]
    )
    naive_avg = naive_avg.rename(columns={"model_short": "composition"})
    naive_avg["model_short"] = "Naive Mean"

    naive_avg = naive_avg[naive_avg["composition"].apply(is_multi_model_composition)].copy()

    if not naive_avg.empty:
        naive_avg["flag"] = 0
        joint = _append_aggregated_to_joint(joint, naive_avg)

    return joint


def _append_aggregated_to_joint(
    joint_forecasts: pd.DataFrame,
    ensemble_df: pd.DataFrame,
) -> pd.DataFrame:
    """Append quarter/season ensemble rows without dropping period keys."""
    if ensemble_df.empty:
        return joint_forecasts
    if "composition" not in joint_forecasts.columns:
        joint_forecasts = joint_forecasts.copy()
        joint_forecasts["composition"] = ""
    cols = [c for c in ensemble_df.columns if c in joint_forecasts.columns]
    if not cols:
        return joint_forecasts
    return pd.concat([joint_forecasts, ensemble_df[cols]], ignore_index=True)
