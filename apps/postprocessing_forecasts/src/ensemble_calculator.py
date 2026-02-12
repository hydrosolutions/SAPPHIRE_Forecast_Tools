"""Create ensemble forecasts from pre-calculated skill metrics.

Extracted from forecast_library.py calculate_skill_metrics_pentad() lines
1944-2176.  This module lets the operational entry point create ensembles
WITHOUT recalculating skill metrics from scratch.
"""

import os
import re
import logging
import datetime as dt

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper functions (extracted from inner scope of calculate_skill_metrics_*)
# ---------------------------------------------------------------------------

def extract_first_parentheses_content(string_list: list[str]) -> list[str]:
    """Extract first parenthesised text from each string.

    >>> extract_first_parentheses_content(["Linear regression (LR)"])
    ['LR']
    """
    pattern = r'\((.*?)\)'
    result = []
    for string in string_list:
        match = re.search(pattern, string)
        result.append(match.group(1) if match else '')
    return result


def model_long_agg(x: pd.Series) -> str:
    """Aggregate model_long values into ensemble composition string."""
    model_list = x.unique()
    short_model_list = extract_first_parentheses_content(model_list)
    unique_models = ', '.join(sorted(short_model_list))
    return f'Ens. Mean with {unique_models} (EM)'


def model_short_agg(x: pd.Series) -> str:
    """Return 'EM' for ensemble mean."""
    return 'EM'


# ---------------------------------------------------------------------------
# Main public functions
# ---------------------------------------------------------------------------

def filter_for_highly_skilled_forecasts(
    skill_stats: pd.DataFrame,
    threshold_sdivsigma: float | str | None = None,
    threshold_accuracy: float | str | None = None,
    threshold_nse: float | str | None = None,
) -> pd.DataFrame:
    """Filter skill metrics to models passing all thresholds.

    Extracted from forecast_library.py:1944-1974.

    Thresholds are read from env vars if not explicitly provided.
    A threshold set to the string 'False' disables that filter.

    Args:
        skill_stats: DataFrame with sdivsigma, accuracy, nse columns.
        threshold_sdivsigma: Max acceptable s/sigma (lower is better).
        threshold_accuracy: Min acceptable accuracy (higher is better).
        threshold_nse: Min acceptable NSE (higher is better).

    Returns:
        Filtered DataFrame containing only highly-skilled rows.
    """
    if threshold_sdivsigma is None:
        threshold_sdivsigma = os.getenv(
            'ieasyhydroforecast_efficiency_threshold', 0.6
        )
    if threshold_accuracy is None:
        threshold_accuracy = os.getenv(
            'ieasyhydroforecast_accuracy_threshold', 0.8
        )
    if threshold_nse is None:
        threshold_nse = os.getenv(
            'ieasyhydroforecast_nse_threshold', 0.8
        )

    result = skill_stats.copy()

    if str(threshold_sdivsigma) != 'False':
        result = result[
            result['sdivsigma'] < float(threshold_sdivsigma)
        ].copy()

    if str(threshold_accuracy) != 'False':
        result = result[
            result['accuracy'] > float(threshold_accuracy)
        ].copy()

    if str(threshold_nse) != 'False':
        result = result[
            result['nse'] > float(threshold_nse)
        ].copy()

    return result


def create_ensemble_forecasts(
    forecasts: pd.DataFrame,
    skill_stats: pd.DataFrame,
    observed: pd.DataFrame,
    period_col: str,
    period_in_month_col: str,
    get_period_in_month_func,
    sdivsigma_nse_func,
    mae_func,
    forecast_accuracy_hydromet_func,
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
             model_long, model_short, <period_in_month_col>].
        skill_stats: Pre-calculated skill metrics with columns
            [<period_col>, code, model_long, model_short, sdivsigma,
             nse, delta, accuracy, mae, n_pairs].
        observed: Observed data with columns
            [code, date, discharge_avg, delta].
        period_col: 'pentad_in_year' or 'decad_in_year'.
        period_in_month_col: 'pentad_in_month' or 'decad_in_month'.
        get_period_in_month_func: Function to compute period in month
            from a date (e.g. tl.get_pentad or tl.get_decad_in_month).
        sdivsigma_nse_func: forecast_library.sdivsigma_nse
        mae_func: forecast_library.mae
        forecast_accuracy_hydromet_func: forecast_library.forecast_accuracy_hydromet

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
    # We need forecasts that match (period_col, code, model_long, model_short)
    # with the highly-skilled skill_stats
    merge_keys = [period_col, 'code', 'model_long', 'model_short']
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
        'model_long': model_long_agg,
        'model_short': model_short_agg,
    }).reset_index()

    # Step 5+6: discard single-model or empty ensembles
    ensemble_avg = ensemble_avg[
        (ensemble_avg['model_long'] != 'Ens. Mean with  (EM)') &
        (ensemble_avg['model_long'] != 'Ens. Mean with LR (EM)')
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

    number_of_models = forecasts['model_long'].nunique()
    if number_of_models > 1 and not ensemble_merged.empty:
        ensemble_skill_stats = _calculate_ensemble_skill(
            ensemble_merged,
            period_col,
            sdivsigma_nse_func,
            mae_func,
            forecast_accuracy_hydromet_func,
        )
        skill_stats_out = pd.concat(
            [skill_stats, ensemble_skill_stats], ignore_index=True
        )

        # Step 7: calculate period_in_month for ensemble rows
        ensemble_merged[period_in_month_col] = (
            ensemble_merged['date'] + dt.timedelta(days=1.0)
        ).apply(get_period_in_month_func)

        # Step 8: outer join ensemble rows into forecasts
        join_cols = [
            'code', 'date', period_in_month_col, period_col,
            'forecasted_discharge', 'model_long', 'model_short',
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
    sdivsigma_nse_func,
    mae_func,
    forecast_accuracy_hydromet_func,
) -> pd.DataFrame:
    """Calculate skill metrics for ensemble forecasts.

    Mirrors the ensemble skill calculation in
    forecast_library.py:2117-2147.
    """
    group_cols = [period_col, 'code', 'model_long', 'model_short']
    obs_sim_cols = ['discharge_avg', 'forecasted_discharge']

    skill = ensemble_df.groupby(group_cols)[obs_sim_cols].apply(
        sdivsigma_nse_func,
        observed_col='discharge_avg',
        simulated_col='forecasted_discharge',
    ).reset_index()

    mae_stats = ensemble_df.groupby(group_cols)[obs_sim_cols].apply(
        mae_func,
        observed_col='discharge_avg',
        simulated_col='forecasted_discharge',
    ).reset_index()

    acc_cols = ['discharge_avg', 'forecasted_discharge', 'delta']
    accuracy_stats = ensemble_df.groupby(group_cols)[acc_cols].apply(
        forecast_accuracy_hydromet_func,
        observed_col='discharge_avg',
        simulated_col='forecasted_discharge',
        delta_col='delta',
    ).reset_index()

    skill = pd.merge(skill, mae_stats, on=group_cols)
    skill = pd.merge(skill, accuracy_stats, on=group_cols)
    return skill
