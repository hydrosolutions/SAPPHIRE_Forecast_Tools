"""Create ensemble forecasts from pre-calculated skill metrics.

Extracted from forecast_library.py calculate_skill_metrics_pentad() lines
1944-2176.  This module lets the operational entry point create ensembles
WITHOUT recalculating skill metrics from scratch.
"""

import os
import logging
import datetime as dt

import pandas as pd

from src.postprocessing_tools import forecast_target_date

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def composition_agg(model_shorts: pd.Series) -> str:
    """Build composition string from model_short values.

    >>> composition_agg(pd.Series(["LR", "TFT", "TiDE"]))
    'LR, TFT, TiDE'
    """
    return ', '.join(sorted(model_shorts.unique()))


def is_multi_model_composition(composition: str) -> bool:
    """True if composition contains 2+ models.

    >>> is_multi_model_composition('LR, TFT')
    True
    >>> is_multi_model_composition('TFT')
    False
    """
    return bool(composition) and ',' in composition


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
        overrides['sdivsigma'] = threshold_sdivsigma
    if threshold_accuracy is not None:
        overrides['accuracy'] = threshold_accuracy
    if threshold_nse is not None:
        overrides['nse'] = threshold_nse
    return _canonical(skill_stats, **overrides)


def create_ensemble_forecasts(
    forecasts: pd.DataFrame,
    skill_stats: pd.DataFrame,
    observed: pd.DataFrame,
    period_col: str,
    period_in_month_col: str,
    get_period_in_month_func,
    calculate_all_metrics_func,
    # Deprecated params kept for backward compatibility
    sdivsigma_nse_func=None,
    mae_func=None,
    forecast_accuracy_hydromet_func=None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create ensemble mean (EM) forecasts using pre-calculated skill metrics.

    Steps (extracted from forecast_library.py:2067-2176):
    1. Filter skill_stats for highly skilled models
    2. Use merge to get qualifying forecast rows
    3. Exclude NE (neural ensemble) from candidates
    4. Group by [date, code], mean(forecasted_discharge)
    5. Build composition string: "Ens. Mean with LR, TFT (EM)"
    6. Discard single-model or empty ensembles
    7. Calculate period_in_month for ensemble rows
    8. Merge ensemble rows into forecasts (outer join)
    9. Recalculate skill metrics for ensemble

    Args:
        forecasts: Simulated/modelled forecasts with columns
            [code, date, <period_col>, forecasted_discharge,
             model_short, <period_in_month_col>].
        skill_stats: Pre-calculated skill metrics with columns
            [<period_col>, code, model_short, sdivsigma,
             nse, delta, accuracy, mae, n_pairs].
        observed: Observed data with columns
            [code, date, discharge_avg, delta].
        period_col: 'pentad_in_year' or 'decad_in_year'.
        period_in_month_col: 'pentad_in_month' or 'decad_in_month'.
        get_period_in_month_func: Function to compute period in month
            from a date (e.g. tl.get_pentad or tl.get_decad_in_month).
        calculate_all_metrics_func: forecast_library.calculate_all_skill_metrics
        sdivsigma_nse_func: Deprecated, ignored.
        mae_func: Deprecated, ignored.
        forecast_accuracy_hydromet_func: Deprecated, ignored.

    Returns:
        joint_forecasts: forecasts with ensemble rows appended.
        skill_stats_with_ensemble: skill_stats with ensemble metrics
            appended.
    """
    # Step 1: filter for highly skilled models
    skill_stats_ensemble = filter_for_highly_skilled_forecasts(skill_stats)
    logger.debug(
        "Highly skilled models: %d rows", len(skill_stats_ensemble)
    )

    # Normalize merge key types to avoid object/int64 mismatches.
    # period_col values are integers (1-72 for pentad, 1-36 for decad).
    for df in (forecasts, skill_stats_ensemble):
        if period_col in df.columns:
            df[period_col] = pd.to_numeric(df[period_col], errors='coerce')
        if 'code' in df.columns:
            df['code'] = df['code'].astype(str)

    # Step 2: use merge to get qualifying forecast rows
    merge_keys = [period_col, 'code', 'model_short']
    qualifying = forecasts.merge(
        skill_stats_ensemble[merge_keys].drop_duplicates(),
        on=merge_keys,
        how='inner',
    )
    # Drop NaN forecasts
    qualifying = qualifying.dropna(subset=['forecasted_discharge']).copy()

    # Step 3: exclude NE (neural ensemble) from ensemble candidates
    qualifying = qualifying[qualifying['model_short'] != 'NE'].copy()

    if qualifying.empty:
        logger.info("No qualifying forecasts for ensemble creation")
        return forecasts.copy(), skill_stats.copy()

    # Step 4: group by [date, code], compute mean forecasted_discharge
    ensemble_avg = qualifying.groupby(['date', 'code']).agg({
        period_col: 'first',
        'forecasted_discharge': 'mean',
        'model_short': composition_agg,
    }).reset_index()
    # model_short now holds the composition string (e.g. "LR, TFT")
    ensemble_avg = ensemble_avg.rename(
        columns={'model_short': 'composition'}
    )
    ensemble_avg['model_short'] = 'EM'

    # Step 5+6: discard single-model or empty ensembles
    ensemble_avg = ensemble_avg[
        ensemble_avg['composition'].apply(is_multi_model_composition)
    ].copy()

    if ensemble_avg.empty:
        logger.info("No multi-model ensembles after filtering")
        return forecasts.copy(), skill_stats.copy()

    # Step 9: recalculate skill metrics for the ensemble
    ensemble_merged = pd.merge(
        ensemble_avg,
        observed[['code', 'date', 'discharge_avg', 'delta']],
        on=['code', 'date'],
    )

    number_of_models = forecasts['model_short'].nunique()
    if number_of_models > 1 and not ensemble_merged.empty:
        ensemble_skill_stats = _calculate_ensemble_skill(
            ensemble_merged,
            period_col,
            calculate_all_metrics_func,
        )
        skill_stats_out = pd.concat(
            [skill_stats, ensemble_skill_stats], ignore_index=True
        )

        # Step 7: calculate period_in_month for ensemble rows
        # (production date -> target period start)
        ensemble_merged[period_in_month_col] = forecast_target_date(
            ensemble_merged['date']
        ).apply(get_period_in_month_func)

        # Step 8: outer join ensemble rows into forecasts
        # Ensure forecasts has composition column for the outer merge
        if 'composition' not in forecasts.columns:
            forecasts = forecasts.copy()
            forecasts['composition'] = ''
        join_cols = [
            'code', 'date', period_in_month_col, period_col,
            'forecasted_discharge', 'model_short', 'composition',
        ]
        joint_forecasts = pd.merge(
            forecasts,
            ensemble_merged[join_cols],
            on=join_cols,
            how='outer',
        )
    else:
        joint_forecasts = forecasts.copy()
        skill_stats_out = skill_stats.copy()

    return joint_forecasts, skill_stats_out


def _calculate_ensemble_skill(
    ensemble_df: pd.DataFrame,
    period_col: str,
    calculate_all_metrics_func,
) -> pd.DataFrame:
    """Calculate skill metrics for ensemble forecasts in a single pass.

    Uses calculate_all_skill_metrics to compute all 6 metrics
    (sdivsigma, nse, mae, n_pairs, delta, accuracy) in one groupby.
    """
    group_cols = [period_col, 'code', 'model_short', 'composition']
    needed_cols = ['discharge_avg', 'forecasted_discharge', 'delta']

    skill = ensemble_df.groupby(group_cols)[needed_cols].apply(
        calculate_all_metrics_func,
        observed_col='discharge_avg',
        simulated_col='forecasted_discharge',
        delta_col='delta',
    ).reset_index()

    return skill
