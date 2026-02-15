"""Skill metric calculations for postprocessing forecasts.

Extracted from forecast_library.py — these functions are exclusively
used by postprocessing_forecasts.
"""

import os
import re
import logging
import datetime as dt
from contextlib import contextmanager

import numpy as np
import pandas as pd

import tag_library as tl

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Individual metric helpers
# ---------------------------------------------------------------------------

def sdivsigma_nse(data: pd.DataFrame, observed_col: str, simulated_col: str):
    """
    Calculate the forecast efficacy and the Nash-Sutcliffe Efficiency (NSE) for the observed and simulated data.

    NSE = 1 - s/sigma

    Args:
        data (pandas.DataFrame): The input data containing the observed and simulated data.
        observed_col (str): The name of the column containing the observed data.
        simulated_col (str): The name of the column containing the simulated data.

    Returns:
        pandas.Series: A pandas Series containing the forecast efficacy and the NSE value.

    Raises:
        ValueError: If the input data is missing one or more required columns.

    """
    # Test the input. Make sure that the DataFrame contains the required columns
    if not all(column in data.columns for column in [observed_col, simulated_col]):
        raise ValueError(f'DataFrame is missing one or more required columns: {observed_col, simulated_col}')

    # Convert to numpy arrays for faster computation
    # Use float64 for better numerical stability
    observed = data[observed_col].to_numpy(dtype=np.float64)
    simulated = data[simulated_col].to_numpy(dtype=np.float64)

    # Check for empty data after dropping NaNs
    mask = ~(np.isnan(observed) | np.isnan(simulated))
    if not np.any(mask):
        return pd.Series([np.nan, np.nan], index=['sdivsigma', 'nse'])

    # Filter arrays using mask
    observed = observed[mask]
    simulated = simulated[mask]

    # Early return if not enough data points
    if len(observed) < 2:  # Need at least 2 points for std calculation
        logger.info(f"Not enough data points for sdivsigma_nse calculation.")
        return pd.Series([np.nan, np.nan], index=['sdivsigma', 'nse'])

    # Calculate mean once for reuse
    observed_mean = np.mean(observed)

    # Count the number of data points
    n = len(observed)

    # Calculate denominators
    denominator_nse = np.sum((observed - observed_mean) ** 2)
    # sigma: Standard deviation of the observed data
    denominator_sdivsigma = np.std(observed, ddof=1)  # ddof=1 for sample std

    # Check for numerical stability
    if denominator_nse < 1e-10 or denominator_sdivsigma < 1e-10:
        logger.debug(f"Numerical stability issue in sdivsigma_nse:")
        logger.debug(f"denominator_nse: {denominator_nse}")
        logger.debug(f"denominator_sdivsigma: {denominator_sdivsigma}")
        return pd.Series([np.nan, np.nan], index=['sdivsigma', 'nse'])

    try:
        # Calculate differences once for reuse
        differences = observed - simulated

        # Calculate NSE
        numerator_nse = np.sum(differences ** 2)
        nse_value = 1 - (numerator_nse / denominator_nse)

        # Calculate sdivsigma
        # s: Average of squared differences between observed and simulated data
        numerator_sdivsigma = np.sqrt(np.sum(differences ** 2) / (n - 1))
        # s/sigma: Efficacy of the model
        sdivsigma = numerator_sdivsigma / denominator_sdivsigma

        # Sanity checks
        if not (-np.inf < nse_value < np.inf) or not (0 <= sdivsigma < np.inf):
            return pd.Series([np.nan, np.nan], index=['sdivsigma', 'nse'])

        return pd.Series([sdivsigma, nse_value], index=['sdivsigma', 'nse'])

    except (RuntimeWarning, FloatingPointError) as e:
        logger.debug(f"Numerical computation error in sdivsigma_nse: {str(e)}")
        return pd.Series([np.nan, np.nan], index=['sdivsigma', 'nse'])

def forecast_accuracy_hydromet(data: pd.DataFrame, observed_col: str, simulated_col: str, delta_col: str):
    """
    Calculate the forecast accuracy for the observed and simulated data.

    Args:
        data (pandas.DataFrame): The input data containing the observed and simulated data.
        observed_col (str): The name of the column containing the observed data.
        simulated_col (str): The name of the column containing the simulated data.

    Returns:
        pandas.Series: A pandas Series containing the forecast accuracy.

    Raises:
        ValueError: If the input data is missing one or more required columns.

    """
    # Test the input. Make sure that the DataFrame contains the required columns
    if not all(column in data.columns for column in [observed_col, simulated_col, delta_col]):
        raise ValueError(f'DataFrame is missing one or more required columns: {observed_col, simulated_col, delta_col}')

    # Convert to numpy arrays for faster computation
    observed = data[observed_col].to_numpy(dtype=np.float64)
    simulated = data[simulated_col].to_numpy(dtype=np.float64)
    delta_values = data[delta_col].to_numpy(dtype=np.float64)

    # Check for empty data after dropping NaNs
    mask = ~(np.isnan(observed) | np.isnan(simulated) | np.isnan(delta_values))
    if not np.any(mask):
        return pd.Series([np.nan, np.nan], index=['delta', 'accuracy'])

    # Also drop rows where observed, simulated or delta_valus is inf
    mask = mask & ~(np.isinf(observed) | np.isinf(simulated) | np.isinf(delta_values))
    if not np.any(mask):
        return pd.Series([np.nan, np.nan], index=['delta', 'accuracy'])

    # Filter arrays using mask
    observed = observed[mask]
    simulated = simulated[mask]
    delta_values = delta_values[mask]

    # Early return if not enough data points
    if len(observed) < 1:
        return pd.Series([np.nan, np.nan], index=['delta', 'accuracy'])

    try:
        # Calculate absolute differences once
        abs_diff = np.abs(observed - simulated)

        # Calculate accuracy using vectorized operations
        accuracy = np.mean(abs_diff <= delta_values)

        # Get the last delta value (they are all the same)
        delta = delta_values[-1]

        # Sanity checks
        if not (0 <= accuracy <= 1) or not (0 <= delta < np.inf):
            return pd.Series([np.nan, np.nan], index=['delta', 'accuracy'])

        return pd.Series([delta, accuracy], index=['delta', 'accuracy'])

    except (RuntimeWarning, FloatingPointError) as e:
        logger.debug(f"Numerical computation error in forecast_accuracy_hydromet: {str(e)}")
        return pd.Series([np.nan, np.nan], index=['delta', 'accuracy'])

def mae(data: pd.DataFrame, observed_col: str, simulated_col: str):
    """
    Calculate the mean average error between observed and simulated data

    Args:
        data (pandas.DataFrame): The input data containing the observed and simulated data.
        observed_col (str): The name of the column containing the observed data.
        simulated_col (str): The name of the column containing the simulated data.

    Returns:
        pandas.Series: A series containing:
            - mae: mean average error between observed and simulated data
            - n_pairs: number of valid observed-simulated pairs used in calculation

    Raises:
        ValueError: If the input data is missing one or more required columns.
    """
    # Test the input. Make sure that the DataFrame contains the required columns
    if not all(column in data.columns for column in [observed_col, simulated_col]):
        raise ValueError(f'DataFrame is missing one or more required columns: {observed_col, simulated_col}')

    # Convert to numpy arrays for faster computation
    observed = data[observed_col].to_numpy(dtype=np.float64)
    simulated = data[simulated_col].to_numpy(dtype=np.float64)

    # Check for empty data after dropping NaNs
    mask = ~(np.isnan(observed) | np.isnan(simulated))
    if not np.any(mask):
        return pd.Series([np.nan, 0], index=['mae', 'n_pairs'])

    # Filter arrays using mask
    observed = observed[mask]
    simulated = simulated[mask]

    # Early return if not enough data points
    if len(observed) < 1:
        return pd.Series([np.nan, 0], index=['mae', 'n_pairs'])

    try:
        # Calculate MAE using vectorized operations
        mae_value = np.mean(np.abs(observed - simulated))

        # Sanity check
        if not (0 <= mae_value < np.inf):  # MAE must be non-negative
            return pd.Series([np.nan, 0], index=['mae', 'n_pairs'])

        return pd.Series([mae_value, len(observed)], index=['mae', 'n_pairs'])

    except (RuntimeWarning, FloatingPointError) as e:
        logger.debug(f"Numerical computation error in mae: {str(e)}")
        return pd.Series([np.nan, 0], index=['mae', 'n_pairs'])


# ---------------------------------------------------------------------------
# Combined single-pass metric calculation
# ---------------------------------------------------------------------------

def calculate_all_skill_metrics(
    data: pd.DataFrame,
    observed_col: str,
    simulated_col: str,
    delta_col: str,
) -> pd.Series:
    """Calculate all 6 skill metrics in a single pass over the data.

    Combines sdivsigma_nse(), mae(), and forecast_accuracy_hydromet()
    into one function to avoid repeated groupby/merge overhead.

    Args:
        data: DataFrame containing observed, simulated, and delta columns.
        observed_col: Column name for observed values.
        simulated_col: Column name for simulated values.
        delta_col: Column name for delta (tolerance) values.

    Returns:
        pd.Series with keys:
            sdivsigma, nse, mae, n_pairs, delta, accuracy
    """
    nan_result = pd.Series(
        [np.nan, np.nan, np.nan, 0, np.nan, np.nan],
        index=['sdivsigma', 'nse', 'mae', 'n_pairs', 'delta', 'accuracy'],
    )

    # Validate required columns
    required = [observed_col, simulated_col, delta_col]
    if not all(col in data.columns for col in required):
        raise ValueError(
            f'DataFrame is missing required columns: {required}'
        )

    # Convert to numpy arrays (float64 for numerical stability)
    observed = data[observed_col].to_numpy(dtype=np.float64)
    simulated = data[simulated_col].to_numpy(dtype=np.float64)
    delta_values = data[delta_col].to_numpy(dtype=np.float64)

    # Common NaN/inf mask for all metrics
    mask = (
        ~np.isnan(observed) & ~np.isnan(simulated) & ~np.isnan(delta_values)
        & ~np.isinf(observed) & ~np.isinf(simulated) & ~np.isinf(delta_values)
    )
    if not np.any(mask):
        return nan_result

    obs = observed[mask]
    sim = simulated[mask]
    deltas = delta_values[mask]
    n = len(obs)

    # --- MAE + n_pairs (need >= 1 point) ---
    if n < 1:
        return nan_result

    differences = obs - sim
    abs_diff = np.abs(differences)

    try:
        mae_value = float(np.mean(abs_diff))
        if not (0 <= mae_value < np.inf):
            mae_value = np.nan
    except (RuntimeWarning, FloatingPointError):
        mae_value = np.nan

    # --- Accuracy + delta (need >= 1 point) ---
    try:
        accuracy = float(np.mean(abs_diff <= deltas))
        delta = float(deltas[-1])
        if not (0 <= accuracy <= 1) or not (0 <= delta < np.inf):
            accuracy = np.nan
            delta = np.nan
    except (RuntimeWarning, FloatingPointError):
        accuracy = np.nan
        delta = np.nan

    # --- sdivsigma + NSE (need >= 2 points for std) ---
    if n < 2:
        return pd.Series(
            [np.nan, np.nan, mae_value, n, delta, accuracy],
            index=['sdivsigma', 'nse', 'mae', 'n_pairs', 'delta', 'accuracy'],
        )

    try:
        obs_mean = np.mean(obs)
        denominator_nse = np.sum((obs - obs_mean) ** 2)
        denominator_sdivsigma = np.std(obs, ddof=1)

        if denominator_nse < 1e-10 or denominator_sdivsigma < 1e-10:
            sdivsigma = np.nan
            nse_value = np.nan
        else:
            numerator_nse = np.sum(differences ** 2)
            nse_value = 1 - (numerator_nse / denominator_nse)
            numerator_sdivsigma = np.sqrt(np.sum(differences ** 2) / (n - 1))
            sdivsigma = numerator_sdivsigma / denominator_sdivsigma

            if (not (-np.inf < nse_value < np.inf)
                    or not (0 <= sdivsigma < np.inf)):
                sdivsigma = np.nan
                nse_value = np.nan
    except (RuntimeWarning, FloatingPointError):
        sdivsigma = np.nan
        nse_value = np.nan

    return pd.Series(
        [sdivsigma, nse_value, mae_value, n, delta, accuracy],
        index=['sdivsigma', 'nse', 'mae', 'n_pairs', 'delta', 'accuracy'],
    )


# ---------------------------------------------------------------------------
# Full pentad / decade skill metric pipelines
# ---------------------------------------------------------------------------

def calculate_skill_metrics_pentad(
        observed: pd.DataFrame, simulated: pd.DataFrame, timing_stats=None):
    """
    For each model and hydropost in the simulated DataFrame, calculates a number
    of skill metrics based on the observed DataFrame.

    Args:
        observed (pd.DataFrame): The DataFrame containing the observed data.
        simulated (pd.DataFrame): The DataFrame containing the simulated data.
        timing_stats (TimingStats, optional): Timing statistics collector

    Returns:
        pd.DataFrame: The DataFrame containing the skill metrics for each model
            and hydropost.
        pd.DataFrame: Combined forecasts and observations DataFrame
        timing_stats: Timing statistics collector
    """
    if timing_stats is None:
        @contextmanager
        def timer(stats, section):
            yield

    else:
        @contextmanager
        def timer(stats, section):
            stats.start(section)
            try:
                yield
            finally:
                stats.end(section)

    # Test the input. Make sure that the DataFrames contain the required columns
    if not all(column in observed.columns for column in ['code', 'date', 'discharge_avg', 'model_long', 'model_short', 'delta']):
        raise ValueError(f'Observed DataFrame is missing one or more required columns: {["code", "date", "discharge_avg", "model_long", "model_short", "delta"]}')
    if not all(column in simulated.columns for column in ['code', 'date', 'pentad_in_year', 'forecasted_discharge', 'model_long', 'model_short']):
        raise ValueError(f'Simulated DataFrame is missing one or more required columns: {["code", "date", "pentad_in_year", "forecasted_discharge", "model_long", "model_short"]}')

    # Local functions
    def test_for_tuples(df):
        # Identify tuples in each cell
        is_tuple = df.apply(lambda col: col.map(lambda x: isinstance(x, tuple)))
        # Check if there are any True values in is_tuple
        contains_tuples = is_tuple.any(axis=1).any()
        # Test if there are any tuples in the DataFrame
        if contains_tuples:
            logger.debug("There are tuples after the merge.")

            # Step 2: Filter rows that contain any tuples
            rows_with_tuples = df[is_tuple.any(axis=1)]

            # Print rows with tuples
            logger.debug(rows_with_tuples)
        else:
            logger.debug("No tuples found after the merge.")

    def extract_first_parentheses_content(string_list):
        pattern = r'\((.*?)\)'

        result = []
        for string in string_list:
            match = re.search(pattern, string)
            if match:
                result.append(match.group(1))
            else:
                result.append('')  # or None, or any other placeholder

        return result

    def model_long_agg(x):
        # Get unique models
        model_list = x.unique()
        # Only keep strings within brackets (), discard the rest of the string and the brackets
        short_model_list = extract_first_parentheses_content(model_list)
        # Concatenat the model names
        unique_models = ', '.join(sorted(short_model_list))
        return f'Ens. Mean with {unique_models} (EM)'

    def model_short_agg(x):
        return f'EM'

    def filter_for_highly_skilled_forecasts(skill_stats):
        """
        Filter the skill_stats DataFrame for highly skilled forecasts based on
        the thresholds set in the environment.
        """
        # Get thresholds from environment
        threshold_sdivsigma = os.getenv('ieasyhydroforecast_efficiency_threshold', 0.6)
        threshold_accuracy = os.getenv('ieasyhydroforecast_accuracy_threshold', 0.8)
        threshold_nse = os.getenv('ieasyhydroforecast_nse_threshold', 0.8)

        # Test if threshold_sdivsigma is equal to False
        if threshold_sdivsigma != 'False':
            # Filter for rows where sdivsigma is smaller than the threshold
            skill_stats_ensemble = skill_stats[skill_stats['sdivsigma'] < float(threshold_sdivsigma)].copy()
        else:
            skill_stats_ensemble = skill_stats.copy()

        if threshold_accuracy != 'False':
            # Filter for rows where accuracy is larger than the threshold
            skill_stats_ensemble = skill_stats_ensemble[skill_stats_ensemble['accuracy'] > float(threshold_accuracy)].copy()
        else:
            skill_stats_ensemble = skill_stats_ensemble.copy()

        if threshold_nse != 'False':
            # Filter for rows where nse is larger than the threshold
            skill_stats_ensemble = skill_stats_ensemble[skill_stats_ensemble['nse'] > float(threshold_nse)].copy()
        else:
            skill_stats_ensemble = skill_stats_ensemble.copy()

        return skill_stats_ensemble

    # Debugging prints:
    print(f"\n\n\n\n\n||||  DEBUGGING  - calculating skill metrics  ||||")
    # Print the latest date in the DataFrame
    latest_date_temp = simulated['date'].max()
    print(f"Latest date in simulated_df: {latest_date_temp}")
    # Print all unique forecast models (model_short) in the DataFrame
    unique_models = simulated['model_short'].unique()
    print(f"Unique forecast models in simulated_df: {unique_models}")
    # Print unique forecast models available for latest date
    latest_models = simulated[simulated['date'] == latest_date_temp]['model_short'].unique()
    print(f"Unique forecast models available for latest date ({latest_date_temp}): {latest_models}")
    print(f"\n\n\n\n\n\n")


    with timer(timing_stats, 'calculate_skill_metrics_pentad - Filter data'):
        # We calculate skill metrics only on forecasts after 2010
        # Filter observed and simulated DataFrames for dates after 2010
        observed = observed[observed['date'].dt.year >= 2010]
        simulated = simulated[simulated['date'].dt.year >= 2010]

    # Merge the observed and simulated DataFrames
    with timer(timing_stats, 'calculate_skill_metrics_pentad - Initially merge data'):
        skill_metrics_df = pd.merge(
            simulated,
            observed[['code', 'date', 'discharge_avg', 'delta']],
            on=['code', 'date'])
        test_for_tuples(skill_metrics_df)

    # Calculate all skill metrics in a single pass per group
    with timer(timing_stats, 'calculate_skill_metrics_pentad - Calculate all skill metrics'):
        skill_stats = skill_metrics_df. \
            groupby(['pentad_in_year', 'code', 'model_long', 'model_short'])[['discharge_avg', 'forecasted_discharge', 'delta']]. \
            apply(
                calculate_all_skill_metrics,
                observed_col='discharge_avg',
                simulated_col='forecasted_discharge',
                delta_col='delta'). \
            reset_index()
        test_for_tuples(skill_stats)

    with timer(timing_stats, 'calculate_skill_metrics_pentad - Calculate ensemble skill metrics for highly skilled forecasts'):
        skill_stats_ensemble = filter_for_highly_skilled_forecasts(skill_stats)

        # Now we get the rows from the skill_metrics_df where pentad_in_year, code,
        # model_long and model_short are the same as in skill_stats_ensemble
        merge_keys = ['pentad_in_year', 'code', 'model_long', 'model_short']
        skill_metrics_df_ensemble = skill_metrics_df.merge(
            skill_stats_ensemble[merge_keys].drop_duplicates(),
            on=merge_keys,
            how='inner',
        )
        # Filter out rows where forecasted_discharge is NaN
        skill_metrics_df_ensemble = skill_metrics_df_ensemble.dropna(subset=['forecasted_discharge']).copy()

        # Drop columns with model_short == NE (neural ensemble)
        skill_metrics_df_ensemble = skill_metrics_df_ensemble[skill_metrics_df_ensemble['model_short'] != 'NE'].copy()

        # Perform the aggregations and keep only the unique combinations
        skill_metrics_df_ensemble_avg = skill_metrics_df_ensemble.groupby(['date', 'code']).agg({
            'pentad_in_year': 'first',
            'forecasted_discharge': 'mean',
            'model_long': model_long_agg,
            'model_short': model_short_agg
        }).reset_index()

        # Discard rows with model_long equal to 'Ensemble Mean with  (EM)' or equal to Ensemble Mean with LR (EM)
        skill_metrics_df_ensemble_avg = skill_metrics_df_ensemble_avg[
            (skill_metrics_df_ensemble_avg['model_long'] != 'Ens. Mean with  (EM)') &
            (skill_metrics_df_ensemble_avg['model_long'] != 'Ens. Mean with LR (EM)')].copy()

        # Now recalculate the skill metrics for the ensemble
        ensemble_skill_metrics_df = pd.merge(
            skill_metrics_df_ensemble_avg,
            observed[['code', 'date', 'discharge_avg', 'delta']],
            on=['code', 'date'])
        print("DEBUG: ensemble_skill_metrics_df\n", ensemble_skill_metrics_df.columns)
        print("DEBUG: ensemble_skill_metrics_df\n", ensemble_skill_metrics_df.head(20))

        number_of_models = simulated['model_long'].nunique()
        print("DEBUG: number_of_models\n", number_of_models)
        if number_of_models > 1:
            # Single-pass ensemble skill metrics
            ensemble_skill_stats = ensemble_skill_metrics_df. \
                groupby(['pentad_in_year', 'code', 'model_long', 'model_short'])[['discharge_avg', 'forecasted_discharge', 'delta']]. \
                apply(
                    calculate_all_skill_metrics,
                    observed_col='discharge_avg',
                    simulated_col='forecasted_discharge',
                    delta_col='delta'). \
                reset_index()

            # Append the ensemble skill metrics to the skill metrics
            skill_stats = pd.concat([skill_stats, ensemble_skill_stats], ignore_index=True)

            # Calculate pentad in month (add 1 day to date)
            ensemble_skill_metrics_df['pentad_in_month'] = (ensemble_skill_metrics_df['date']+dt.timedelta(days=1.0)).apply(tl.get_pentad)

            # Join the two dataframes
            joint_forecasts = pd.merge(
                simulated,
                ensemble_skill_metrics_df[['code', 'date', 'pentad_in_month', 'pentad_in_year', 'forecasted_discharge', 'model_long', 'model_short']],
                on=['code', 'date', 'pentad_in_month', 'pentad_in_year', 'model_long', 'model_short', 'forecasted_discharge'],
                how='outer')
        else:
            joint_forecasts = simulated.copy()

    return skill_stats, joint_forecasts, timing_stats


def calculate_skill_metrics_decade(
        observed: pd.DataFrame, simulated: pd.DataFrame, timing_stats=None):
    """
    For each model and hydropost in the simulated DataFrame, calculates a number
    of skill metrics based on the observed DataFrame.

    Args:
        observed (pd.DataFrame): The DataFrame containing the observed data.
        simulated (pd.DataFrame): The DataFrame containing the simulated data.
        timing_stats (TimingStats, optional): Timing statistics collector

    Returns:
        pd.DataFrame: The DataFrame containing the skill metrics for each model
            and hydropost.
        pd.DataFrame: Combined forecasts and observations DataFrame
        timing_stats: Timing statistics collector
    """
    if timing_stats is None:
        @contextmanager
        def timer(stats, section):
            yield

    else:
        @contextmanager
        def timer(stats, section):
            stats.start(section)
            try:
                yield
            finally:
                stats.end(section)

    # Test the input. Make sure that the DataFrames contain the required columns
    if not all(column in observed.columns for column in ['code', 'date', 'discharge_avg', 'model_long', 'model_short', 'delta']):
        raise ValueError(f'Observed DataFrame is missing one or more required columns: {["code", "date", "discharge_avg", "model_long", "model_short", "delta"]}')
    if not all(column in simulated.columns for column in ['code', 'date', 'decad_in_year', 'forecasted_discharge', 'model_long', 'model_short']):
        raise ValueError(f'Simulated DataFrame is missing one or more required columns: {["code", "date", "decad_in_year", "forecasted_discharge", "model_long", "model_short"]}')

    # Print column names of simulated
    logger.debug(f"DEBUG: simulated.columns\n{simulated.columns}")

    # Local functions
    def test_for_tuples(df):
        # Identify tuples in each cell
        is_tuple = df.apply(lambda col: col.map(lambda x: isinstance(x, tuple)))
        # Check if there are any True values in is_tuple
        contains_tuples = is_tuple.any(axis=1).any()
        # Test if there are any tuples in the DataFrame
        if contains_tuples:
            logger.debug("There are tuples after the merge.")

            # Step 2: Filter rows that contain any tuples
            rows_with_tuples = df[is_tuple.any(axis=1)]

            # Print rows with tuples
            logger.debug(rows_with_tuples)
        else:
            logger.debug("No tuples found after the merge.")

    def extract_first_parentheses_content(string_list):
        pattern = r'\((.*?)\)'

        result = []
        for string in string_list:
            match = re.search(pattern, string)
            if match:
                result.append(match.group(1))
            else:
                result.append('')  # or None, or any other placeholder

        return result

    def model_long_agg(x):
        # Get unique models
        model_list = x.unique()
        # Only keep strings within brackets (), discard the rest of the string and the brackets
        short_model_list = extract_first_parentheses_content(model_list)
        # Concatenat the model names
        unique_models = ', '.join(sorted(short_model_list))
        return f'Ens. Mean with {unique_models} (EM)'

    def model_short_agg(x):
        return f'EM'

    def filter_for_highly_skilled_forecasts(skill_stats):
        # Get thresholds from environment
        threshold_sdivsigma = os.getenv('ieasyhydroforecast_efficiency_threshold', 0.6)
        threshold_accuracy = os.getenv('ieasyhydroforecast_accuracy_threshold', 0.8)
        threshold_nse = os.getenv('ieasyhydroforecast_nse_threshold', 0.8)

        # Test if threshold_sdivsigma is equal to False
        if threshold_sdivsigma != 'False':
            # Filter for rows where sdivsigma is smaller than the threshold
            skill_stats_ensemble = skill_stats[skill_stats['sdivsigma'] < float(threshold_sdivsigma)].copy()
        else:
            skill_stats_ensemble = skill_stats.copy()

        if threshold_accuracy != 'False':
            # Filter for rows where accuracy is larger than the threshold
            skill_stats_ensemble = skill_stats_ensemble[skill_stats_ensemble['accuracy'] > float(threshold_accuracy)].copy()
        else:
            skill_stats_ensemble = skill_stats_ensemble.copy()

        if threshold_nse != 'False':
            # Filter for rows where nse is larger than the threshold
            skill_stats_ensemble = skill_stats_ensemble[skill_stats_ensemble['nse'] > float(threshold_nse)].copy()
        else:
            skill_stats_ensemble = skill_stats_ensemble.copy()

        return skill_stats_ensemble

    with timer(timing_stats, 'calculate_skill_metrics_decade - Filter data'):
        # We calculate skill metrics only on forecasts after 2010
        # Filter observed and simulated DataFrames for dates after 2010
        observed = observed[observed['date'].dt.year >= 2010]
        simulated = simulated[simulated['date'].dt.year >= 2010]

    # Merge the observed and simulated DataFrames
    with timer(timing_stats, 'calculate_skill_metrics_decade - Initially merge data'):
        skill_metrics_df = pd.merge(
            simulated,
            observed[['code', 'date', 'discharge_avg', 'delta']],
            on=['code', 'date'])
        test_for_tuples(skill_metrics_df)

    # Calculate all skill metrics in a single pass per group
    with timer(timing_stats, 'calculate_skill_metrics_decad - Calculate all skill metrics'):
        skill_stats = skill_metrics_df. \
            groupby(['decad_in_year', 'code', 'model_long', 'model_short'])[['discharge_avg', 'forecasted_discharge', 'delta']]. \
            apply(
                calculate_all_skill_metrics,
                observed_col='discharge_avg',
                simulated_col='forecasted_discharge',
                delta_col='delta'). \
            reset_index()
        test_for_tuples(skill_stats)

    with timer(timing_stats, 'calculate_skill_metrics_decad - Calculate ensemble skill metrics for highly skilled forecasts'):
        skill_stats_ensemble = filter_for_highly_skilled_forecasts(skill_stats)

        # Now we get the rows from the skill_metrics_df where decad_in_year, code,
        # model_long and model_short are the same as in skill_stats_ensemble
        merge_keys = ['decad_in_year', 'code', 'model_long', 'model_short']
        skill_metrics_df_ensemble = skill_metrics_df.merge(
            skill_stats_ensemble[merge_keys].drop_duplicates(),
            on=merge_keys,
            how='inner',
        )

        # Drop columns with model_short == NE (neural ensemble)
        skill_metrics_df_ensemble = skill_metrics_df_ensemble[skill_metrics_df_ensemble['model_short'] != 'NE'].copy()

        # Perform the aggregations and keep only the unique combinations
        skill_metrics_df_ensemble_avg = skill_metrics_df_ensemble.groupby(['date', 'code']).agg({
            'decad_in_year': 'first',
            'forecasted_discharge': 'mean',
            'model_long': model_long_agg,
            'model_short': model_short_agg
        }).reset_index()

        # Discard rows with model_long equal to 'Ensemble Mean with  (EM)' or equal to Ensemble Mean with LR (EM)
        skill_metrics_df_ensemble_avg = skill_metrics_df_ensemble_avg[
            (skill_metrics_df_ensemble_avg['model_long'] != 'Ens. Mean with  (EM)') &
            (skill_metrics_df_ensemble_avg['model_long'] != 'Ens. Mean with LR (EM)')].copy()

        # Now recalculate the skill metrics for the ensemble
        ensemble_skill_metrics_df = pd.merge(
            skill_metrics_df_ensemble_avg,
            observed[['code', 'date', 'discharge_avg', 'delta']],
            on=['code', 'date'])

        number_of_models = simulated['model_long'].nunique()
        print("DEBUG: number_of_models\n", number_of_models)
        if number_of_models > 1:
            # Single-pass ensemble skill metrics
            ensemble_skill_stats = ensemble_skill_metrics_df. \
                groupby(['decad_in_year', 'code', 'model_long', 'model_short'])[['discharge_avg', 'forecasted_discharge', 'delta']]. \
                apply(
                    calculate_all_skill_metrics,
                    observed_col='discharge_avg',
                    simulated_col='forecasted_discharge',
                    delta_col='delta'). \
                reset_index()

            # Append the ensemble skill metrics to the skill metrics
            skill_stats = pd.concat([skill_stats, ensemble_skill_stats], ignore_index=True)

            # Calculate pentad in month (add 1 day to date)
            ensemble_skill_metrics_df['decad_in_month'] = (ensemble_skill_metrics_df['date']+dt.timedelta(days=1.0)).apply(tl.get_decad_in_month)

            # Join the two dataframes
            joint_forecasts = pd.merge(
                simulated,
                ensemble_skill_metrics_df[['code', 'date', 'decad_in_month', 'decad_in_year', 'forecasted_discharge', 'model_long', 'model_short']],
                on=['code', 'date', 'decad_in_month', 'decad_in_year', 'model_long', 'model_short', 'forecasted_discharge'],
                how='outer')

        else:
            joint_forecasts = simulated.copy()

    return skill_stats, joint_forecasts, timing_stats
