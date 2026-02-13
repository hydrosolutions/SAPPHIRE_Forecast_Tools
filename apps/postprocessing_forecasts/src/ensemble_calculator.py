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


def _is_multi_model_ensemble(model_long_str: str) -> bool:
    """True if the composition string contains 2+ models.

    Parses 'Ens. Mean with X, Y (EM)' and checks for a comma
    in the model list, indicating multiple contributing models.

    >>> _is_multi_model_ensemble('Ens. Mean with LR, TFT (EM)')
    True
    >>> _is_multi_model_ensemble('Ens. Mean with TFT (EM)')
    False
    """
    match = re.search(r'with\s+(.+?)\s+\(EM\)', model_long_str)
    if not match:
        return False
    model_list = match.group(1).strip()
    return bool(model_list) and ',' in model_list


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
        ensemble_avg['model_long'].apply(_is_multi_model_ensemble)
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
            calculate_all_metrics_func,
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
    calculate_all_metrics_func,
) -> pd.DataFrame:
    """Calculate skill metrics for ensemble forecasts in a single pass.

    Uses calculate_all_skill_metrics to compute all 6 metrics
    (sdivsigma, nse, mae, n_pairs, delta, accuracy) in one groupby.
    """
    group_cols = [period_col, 'code', 'model_long', 'model_short']
    needed_cols = ['discharge_avg', 'forecasted_discharge', 'delta']

    skill = ensemble_df.groupby(group_cols)[needed_cols].apply(
        calculate_all_metrics_func,
        observed_col='discharge_avg',
        simulated_col='forecasted_discharge',
        delta_col='delta',
    ).reset_index()

    return skill
