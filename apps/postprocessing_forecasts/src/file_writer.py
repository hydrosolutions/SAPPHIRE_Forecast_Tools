"""CSV write + save orchestration for postprocessing forecasts.

Extracted from forecast_library.py — these functions are exclusively
used by postprocessing_forecasts.
"""

import os
import logging
import tempfile
import shutil

import pandas as pd

import forecast_library as fl
from . import api_writer

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Atomic CSV writer
# ---------------------------------------------------------------------------

def atomic_write_csv(data: pd.DataFrame, filepath: str, **to_csv_kwargs) -> None:
    """
    Write a DataFrame to CSV atomically using temp file + rename pattern.

    This prevents data loss if a crash occurs during the write operation.
    The file is first written to a temporary location, then atomically
    moved to the final destination using os.replace().

    Args:
        data: DataFrame to write
        filepath: Final destination path for the CSV file
        **to_csv_kwargs: Additional arguments to pass to DataFrame.to_csv()

    Raises:
        Exception: If the write operation fails (temp file is cleaned up)
    """
    # Get the directory of the target file
    target_dir = os.path.dirname(filepath) or '.'

    # Ensure the target directory exists
    os.makedirs(target_dir, exist_ok=True)

    # Create a temp file in the same directory (ensures same filesystem for atomic rename)
    temp_fd, temp_path = tempfile.mkstemp(suffix='.tmp', dir=target_dir)

    try:
        # Close the file descriptor (we'll write via pandas)
        os.close(temp_fd)

        # Write to the temp file
        data.to_csv(temp_path, **to_csv_kwargs)

        # Atomic move: os.replace() is atomic on POSIX systems when src and dst
        # are on the same filesystem. On Windows, it's atomic if dst doesn't exist.
        # shutil.move uses os.rename under the hood for same-filesystem moves.
        shutil.move(temp_path, filepath)

        logger.debug(f"Atomically wrote {len(data)} rows to {filepath}")

    except Exception as e:
        # Clean up the temp file if something went wrong
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass  # Ignore cleanup errors
        raise e


# ---------------------------------------------------------------------------
# Latest-forecast extraction
# ---------------------------------------------------------------------------

def get_latest_forecasts(simulated_df, horizon_column_name='pentad_in_year'):
    """
    Extract the latest forecasts for each unique combination of code, pentad_in_year, and model_short.

    Args:
        simulated_df (pd.DataFrame): DataFrame containing forecast data with columns 'code',
                                    <horizon_column_name>, 'model_short', 'date', and forecast values
        horizon_column_name (str): Name of the column that represents the forecast horizon.
                                    Default is 'pentad_in_year'.

    Returns:
        pd.DataFrame: DataFrame containing only the most recent forecast for each unique
                     combination of code, pentad_in_year, and model_short
    """
    if simulated_df.empty:
        return pd.DataFrame()

    latest_date_temp = simulated_df['date'].max()
    unique_models = simulated_df['model_short'].unique()
    latest_models = simulated_df[simulated_df['date'] == latest_date_temp]['model_short'].unique()
    logger.debug(
        "Getting latest forecasts — latest date: %s, "
        "models: %s, models at latest date: %s",
        latest_date_temp, unique_models, latest_models,
    )

    # Ensure date is in datetime format
    if not pd.api.types.is_datetime64_any_dtype(simulated_df['date']):
        simulated_df = simulated_df.copy()
        simulated_df['date'] = pd.to_datetime(simulated_df['date'])

    # Sort by date in descending order first
    sorted_df = simulated_df.sort_values('date', ascending=False)
    latest_forecasts = sorted_df.drop_duplicates(
        subset=['code', horizon_column_name, 'model_short'], keep='first').copy()

    # Only keep lines where year of date is equal to the maximum year
    # Here we take data from second to last and last year
    latest_year = simulated_df['date'].max().year
    # Write year into column, derived from date column
    latest_forecasts.loc[:, 'year'] = latest_forecasts['date'].dt.year
    latest_forecasts = latest_forecasts[latest_forecasts['year'] >= (latest_year - 1)]

    logger.debug(
        "Latest year filter: %d, years in result: %s",
        latest_year, latest_forecasts['year'].unique(),
    )

    # Drop the 'year' column
    latest_forecasts = latest_forecasts.drop(columns=['year'])

    # Round numeric columns to 3 decimal places
    numeric_cols = latest_forecasts.select_dtypes(include=['float64', 'float32']).columns
    latest_forecasts[numeric_cols] = latest_forecasts[numeric_cols].round(3)

    return latest_forecasts


# ---------------------------------------------------------------------------
# Save forecast data
# ---------------------------------------------------------------------------

def save_forecast_data_pentad(simulated: pd.DataFrame):
    """
    Save observed pentadal runoff and simulated pentadal runoff for different models to csv.

    Args:
    simulated (pd.DataFrame): The DataFrame containing the simulated data.

    Returns:
    None
    """
    filename = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_combined_forecast_pentad_file"))

    # Round all float values to 3 decimal places
    simulated = simulated.round(3)

    # Ensure code is string without .0
    if 'code' in simulated.columns:
        simulated['code'] = simulated['code'].astype(str).str.replace(r'\.0$', '', regex=True)
    # Ensure date is in %Y-%m-%d format
    if 'date' in simulated.columns:
        simulated['date'] = pd.to_datetime(simulated['date'], errors='coerce').dt.strftime('%Y-%m-%d')

    # write the data to csv
    ret = simulated.to_csv(filename, index=False)

    # Select forecast of the latest date for each code, pentad_in_year, and model_short
    simulated_latest = get_latest_forecasts(simulated, horizon_column_name='pentad_in_year')

    # Edit filename by appending '_latest' to the filename
    filename_latest = filename.replace('.csv', '_latest.csv')

    # Write the latest data to a csv file
    ret = simulated_latest.to_csv(filename_latest, index=False)

    # Write to SAPPHIRE API (latest forecasts only)
    if api_writer.SAPPHIRE_API_AVAILABLE:
        try:
            api_writer._write_combined_forecast_to_api(simulated_latest, "pentad")
        except Exception as e:
            fl._handle_api_write_error(e, "pentadal combined forecasts")

    # --- Consistency Check ---
    consistency_check = os.getenv("SAPPHIRE_CONSISTENCY_CHECK", "false").lower() == "true"
    if consistency_check:
        logger.info("SAPPHIRE_CONSISTENCY_CHECK: Verifying write consistency for pentad combined forecasts")

        is_consistent, message = fl._verify_preprocessing_write_consistency(
            written_data=simulated_latest,
            csv_file_path=filename_latest,
            data_type="combined forecasts pentad",
            key_columns=['code', 'date', 'pentad_in_year', 'model_short'],
            value_columns=['forecasted_discharge'],
        )

        if is_consistent:
            logger.info("CONSISTENCY CHECK PASSED: %s", message)
        else:
            logger.error("CONSISTENCY CHECK FAILED: %s", message)

    return ret

def save_forecast_data_decade(simulated: pd.DataFrame):
    """
    Save observed decadal runoff and simulated decadal runoff for different models to csv.

    Args:
    simulated (pd.DataFrame): The DataFrame containing the simulated data.

    Returns:
    None
    """
    filename = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_combined_forecast_decad_file"))

    # Round all float values to 3 decimal places
    simulated = simulated.round(3)

    # Ensure code is string without .0
    if 'code' in simulated.columns:
        simulated['code'] = simulated['code'].astype(str).str.replace(r'\.0$', '', regex=True)
    # Ensure date is in %Y-%m-%d format
    if 'date' in simulated.columns:
        simulated['date'] = pd.to_datetime(simulated['date'], errors='coerce').dt.strftime('%Y-%m-%d')

    # Rename the column decad_in_month to decad
    simulated = simulated.rename(columns={'decad_in_month': 'decad'})

    # write the data to csv
    ret = simulated.to_csv(filename, index=False)

    # Select forecast of the latest date for each code, decad_in_year, and model_short
    simulated_latest = get_latest_forecasts(simulated, horizon_column_name='decad_in_year')

    # Edit filename by appending '_latest' to the filename
    filename_latest = filename.replace('.csv', '_latest.csv')

    # Write the latest data to a csv file
    ret = simulated_latest.to_csv(filename_latest, index=False)

    # Write to SAPPHIRE API (latest forecasts only)
    if api_writer.SAPPHIRE_API_AVAILABLE:
        try:
            api_writer._write_combined_forecast_to_api(simulated_latest, "decade")
        except Exception as e:
            fl._handle_api_write_error(e, "decadal combined forecasts")

    # --- Consistency Check ---
    consistency_check = os.getenv("SAPPHIRE_CONSISTENCY_CHECK", "false").lower() == "true"
    if consistency_check:
        logger.info("SAPPHIRE_CONSISTENCY_CHECK: Verifying write consistency for decad combined forecasts")

        is_consistent, message = fl._verify_preprocessing_write_consistency(
            written_data=simulated_latest,
            csv_file_path=filename_latest,
            data_type="combined forecasts decade",
            key_columns=['code', 'date', 'decad_in_year', 'model_short'],
            value_columns=['forecasted_discharge'],
        )

        if is_consistent:
            logger.info("CONSISTENCY CHECK PASSED: %s", message)
        else:
            logger.error("CONSISTENCY CHECK FAILED: %s", message)

    return ret


# ---------------------------------------------------------------------------
# Save skill metrics
# ---------------------------------------------------------------------------

def save_pentadal_skill_metrics(data: pd.DataFrame):
    """
    Saves pentadal skill metrics to a csv file.

    Args:
    data (pd.DataFrame): The data to be written to a csv file.

    Returns:
    None

    """

    # Round all values to 4 decimal places
    data = data.round(4)

    # Ensure code is string without .0
    if 'code' in data.columns:
        data['code'] = data['code'].astype(str).str.replace(r'\.0$', '', regex=True)
    # Ensure date is in %Y-%m-%d format
    if 'date' in data.columns:
        data['date'] = pd.to_datetime(data['date'], errors='coerce').dt.strftime('%Y-%m-%d')

    # convert pentad_in_year to int
    data['pentad_in_year'] = data['pentad_in_year'].astype(int)

    # Sort in ascending order by 'pentad_in_year', 'code', and 'model_short'
    data = data.sort_values(by=['pentad_in_year', 'code', 'model_short'])

    filepath = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_pentadal_skill_metrics_file"))

    # Write atomically (temp file + rename) to prevent data loss on crash
    try:
        atomic_write_csv(data, filepath, index=False)
        logger.info(f"Data written to {filepath}.")
    except Exception as e:
        logger.error(f"Could not write the data to {filepath}.")
        raise e

    # Write to SAPPHIRE API
    if api_writer.SAPPHIRE_API_AVAILABLE:
        try:
            api_writer._write_skill_metrics_to_api(data, "pentad")
        except Exception as e:
            fl._handle_api_write_error(e, "pentadal skill metrics")

    # --- Consistency Check ---
    consistency_check = os.getenv("SAPPHIRE_CONSISTENCY_CHECK", "false").lower() == "true"
    if consistency_check:
        logger.info("SAPPHIRE_CONSISTENCY_CHECK: Verifying write consistency for pentad skill metrics")

        is_consistent, message = fl._verify_preprocessing_write_consistency(
            written_data=data,
            csv_file_path=filepath,
            data_type="skill metrics pentad",
            key_columns=['code', 'pentad_in_year', 'model_short'],
            value_columns=['sdivsigma', 'nse', 'delta', 'accuracy', 'mae', 'n_pairs'],
        )

        if is_consistent:
            logger.info("CONSISTENCY CHECK PASSED: %s", message)
        else:
            logger.error("CONSISTENCY CHECK FAILED: %s", message)

    return None

def save_decadal_skill_metrics(data: pd.DataFrame):
    """
    Saves decadal skill metrics to a csv file.

    Args:
    data (pd.DataFrame): The data to be written to a csv file.

    Returns:
    None

    """

    # Round all values to 4 decimal places
    data = data.round(4)

    # Ensure code is string without .0
    if 'code' in data.columns:
        data['code'] = data['code'].astype(str).str.replace(r'\.0$', '', regex=True)
    # Ensure date is in %Y-%m-%d format
    if 'date' in data.columns:
        data['date'] = pd.to_datetime(data['date'], errors='coerce').dt.strftime('%Y-%m-%d')

    # convert decad_in_year to int
    data['decad_in_year'] = data['decad_in_year'].astype(int)

    # Sort in ascending order by 'decad_in_year', 'code', and 'model_short'
    data = data.sort_values(by=['decad_in_year', 'code', 'model_short'])

    filepath = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_decadal_skill_metrics_file"))

    # Write atomically (temp file + rename) to prevent data loss on crash
    try:
        atomic_write_csv(data, filepath, index=False)
        logger.info(f"Data written to {filepath}.")
    except Exception as e:
        logger.error(f"Could not write the data to {filepath}.")
        raise e

    # Write to SAPPHIRE API
    if api_writer.SAPPHIRE_API_AVAILABLE:
        try:
            api_writer._write_skill_metrics_to_api(data, "decade")
        except Exception as e:
            fl._handle_api_write_error(e, "decadal skill metrics")

    # --- Consistency Check ---
    consistency_check = os.getenv("SAPPHIRE_CONSISTENCY_CHECK", "false").lower() == "true"
    if consistency_check:
        logger.info("SAPPHIRE_CONSISTENCY_CHECK: Verifying write consistency for decad skill metrics")

        is_consistent, message = fl._verify_preprocessing_write_consistency(
            written_data=data,
            csv_file_path=filepath,
            data_type="skill metrics decade",
            key_columns=['code', 'decad_in_year', 'model_short'],
            value_columns=['sdivsigma', 'nse', 'delta', 'accuracy', 'mae', 'n_pairs'],
        )

        if is_consistent:
            logger.info("CONSISTENCY CHECK PASSED: %s", message)
        else:
            logger.error("CONSISTENCY CHECK FAILED: %s", message)

    return None
