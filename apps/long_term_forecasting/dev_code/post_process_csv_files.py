##################################################
# Post-Process Existing Hindcast and Forecast CSV Files
##################################################

## How to run this script:
# Set the environment variable ieasyhydroforecast_env_file_path to point to your .env file
# Then run the script with:
# ieasyhydroforecast_env_file_path="path_to_env" python post_process_csv_files.py --modes monthly --all
#
# This script loads existing hindcast/forecast CSV files, filters to keep only
# rows on the operational issue day, applies post-processing, and saves them back.

import argparse
import logging
import os
import shutil
import sys
from datetime import datetime
from typing import List, Optional

import pandas as pd

# Suppress graphviz debug warnings BEFORE importing any modules that use graphviz
logging.getLogger("graphviz").setLevel(logging.WARNING)
# Add parent directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.join(script_dir, '..')
sys.path.insert(0, parent_dir)

from __init__ import logger, initialize_today, get_today, LT_FORECAST_BASE_COLUMNS
from config_forecast import ForecastConfig
from data_interface import DataInterface
from post_process_lt_forecast import post_process_lt_forecast
from lt_utils import infer_q_columns

# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the setup_library module from the iEasyHydroForecast package
import setup_library as sl


def get_supported_modes() -> List[str]:
    """
    Get supported LT forecast modes from environment variable.

    Returns:
        List of supported mode names (e.g., ['monthly', 'seasonal'])
    """
    modes_str = os.getenv('ieasyhydroforecast_ml_long_term_supported_modes', '')
    if not modes_str:
        logger.warning("No supported modes found in ieasyhydroforecast_ml_long_term_supported_modes")
        return []
    return [m.strip() for m in modes_str.split(',') if m.strip()]


def process_single_file(
    file_path: str,
    forecast_config: ForecastConfig,
    discharge_data: pd.DataFrame,
    operational_issue_day: int,
    dry_run: bool = False
) -> bool:
    """
    Process a single hindcast or forecast CSV file.

    Args:
        file_path: Path to the CSV file
        forecast_config: ForecastConfig object
        discharge_data: Discharge data for post-processing
        operational_issue_day: Day of month for forecast issue
        dry_run: If True, don't save changes

    Returns:
        True if processing succeeded, False otherwise
    """
    if not os.path.exists(file_path):
        logger.warning(f"File not found: {file_path}")
        return False

    try:
        # Load the CSV file with date parsing
        df = pd.read_csv(file_path, parse_dates=['date'])
        original_rows = len(df)

        logger.info(f"Loaded {file_path} with {original_rows} rows")

        # Filter to keep only rows where date.day == operational_issue_day
        df_filtered = df[df['date'].dt.day == operational_issue_day].copy()
        filtered_rows = len(df_filtered)

        logger.info(f"Filtered to {filtered_rows} rows (day == {operational_issue_day})")

        if df_filtered.empty:
            logger.warning(f"No rows remain after filtering for issue day {operational_issue_day}")
            return False

        # Infer Q columns
        q_columns = infer_q_columns(df_filtered)
        if not q_columns:
            logger.warning(f"No Q columns found in {file_path}")
            return False

        logger.info(f"Found Q columns: {q_columns}")

        df_filtered = df_filtered[LT_FORECAST_BASE_COLUMNS + q_columns]

        logger.info(f"Retained columns: {df_filtered.columns.tolist()}")

        # Apply post-processing
        df_processed = post_process_lt_forecast(
            forecast_config=forecast_config,
            observed_discharge_data=discharge_data,
            raw_forecast=df_filtered
        )

        logger.info(f"Post-processing complete. Output has {len(df_processed)} rows")

        if dry_run:
            logger.info(f"[DRY RUN] Would save {len(df_processed)} rows to {file_path}")
            return True

        # Save the processed file
        # Convert date to string format for CSV
        df_processed['date'] = df_processed['date'].dt.strftime('%Y-%m-%d')
        df_processed.to_csv(file_path, index=False)
        logger.info(f"Saved processed file: {file_path}")

        return True

    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


def process_mode(
    mode: str,
    models_filter: Optional[List[str]] = None,
    dry_run: bool = False
) -> dict:
    """
    Process all models for a given forecast mode.

    Args:
        mode: Forecast mode (e.g., 'monthly')
        models_filter: Optional list of model names to process (None = all)
        dry_run: If True, don't save changes

    Returns:
        Dictionary mapping model names to success status
    """
    results = {}

    logger.info(f"\n{'='*60}")
    logger.info(f"Processing mode: {mode}")
    logger.info(f"{'='*60}")

    # Load the forecast config for this mode
    try:
        forecast_config = ForecastConfig()
        forecast_config.load_forecast_config(forecast_mode=mode)
    except Exception as e:
        logger.error(f"Failed to load forecast config for mode '{mode}': {e}")
        return results

    # Get the model execution order
    ordered_models = forecast_config.get_model_execution_order()

    # Filter models if specified
    if models_filter:
        ordered_models = [m for m in ordered_models if m in models_filter]
        logger.info(f"Filtered to models: {ordered_models}")

    if not ordered_models:
        logger.warning(f"No models to process for mode '{mode}'")
        return results

    # Get operational issue day
    operational_issue_day = forecast_config.get_operational_issue_day()
    logger.info(f"Operational issue day: {operational_issue_day}")

    # Load discharge data for post-processing
    logger.info("Loading discharge data...")
    data_interface = DataInterface()
    forcing_HRU = forecast_config.get_forcing_HRU()
    start_date = forecast_config.get_start_date()

    base_data_dict = data_interface.get_base_data(
        forcing_HRU=forcing_HRU,
        start_date=start_date
    )
    discharge_data = base_data_dict["temporal_data"]
    logger.info(f"Loaded discharge data with {len(discharge_data)} rows")

    # Process each model
    for model_name in ordered_models:
        logger.info(f"\n--- Processing model: {model_name} ---")

        output_path = forecast_config.get_output_path(model_name=model_name)

        # Process hindcast file
        hindcast_file = os.path.join(output_path, f"{model_name}_hindcast.csv")
        hindcast_success = process_single_file(
            file_path=hindcast_file,
            forecast_config=forecast_config,
            discharge_data=discharge_data,
            operational_issue_day=operational_issue_day,
            dry_run=dry_run
        )

        # Process forecast file
        forecast_file = os.path.join(output_path, f"{model_name}_forecast.csv")
        forecast_success = process_single_file(
            file_path=forecast_file,
            forecast_config=forecast_config,
            discharge_data=discharge_data,
            operational_issue_day=operational_issue_day,
            dry_run=dry_run
        )

        # Track results
        results[model_name] = {
            'hindcast': hindcast_success,
            'forecast': forecast_success
        }

    return results


def post_process_csv_files(
    modes: Optional[List[str]] = None,
    models: Optional[List[str]] = None,
    dry_run: bool = False,
) -> dict:
    """
    Main function to post-process CSV files.

    Args:
        modes: List of forecast modes to process (None = all from env var)
        models: List of model names to process (None = all)
        dry_run: If True, preview changes without saving

    Returns:
        Dictionary of results by mode and model
    """
    # Setup environment
    sl.load_environment()

    # Get modes to process
    if modes is None:
        modes = get_supported_modes()

    if not modes:
        logger.error("No forecast modes specified and none found in environment")
        return {}

    logger.info(f"Modes to process: {modes}")
    if models:
        logger.info(f"Models filter: {models}")
    if dry_run:
        logger.info("[DRY RUN MODE] No changes will be saved")

    all_results = {}

    for mode in modes:
        mode_results = process_mode(
            mode=mode,
            models_filter=models,
            dry_run=dry_run,
        )
        all_results[mode] = mode_results

    return all_results


def print_summary(results: dict):
    """Print a summary of processing results."""
    logger.info("\n" + "="*60)
    logger.info("POST-PROCESSING SUMMARY")
    logger.info("="*60)

    for mode, mode_results in results.items():
        logger.info(f"\nMode: {mode}")
        logger.info("-" * 40)

        for model_name, model_results in mode_results.items():
            hindcast_status = "SUCCESS" if model_results.get('hindcast') else "FAILED/SKIPPED"
            forecast_status = "SUCCESS" if model_results.get('forecast') else "FAILED/SKIPPED"
            logger.info(f"  {model_name}:")
            logger.info(f"    Hindcast: {hindcast_status}")
            logger.info(f"    Forecast: {forecast_status}")

    logger.info("\n" + "="*60)


def main():
    """
    Main entry point for command-line usage.

    Usage examples:
        # Process all modes and all models
        python post_process_csv_files.py

        # Process specific modes
        python post_process_csv_files.py --modes monthly seasonal

        # Process specific models
        python post_process_csv_files.py --models LR_1 SciRegressor_1

        # Preview changes without saving (dry run)
        python post_process_csv_files.py --dry-run

        # With environment file
        ieasyhydroforecast_env_file_path="path/to/.env" python post_process_csv_files.py --modes monthly
    """
    parser = argparse.ArgumentParser(
        description="Post-process existing hindcast and forecast CSV files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all modes and all models
  python post_process_csv_files.py

  # Process specific modes
  python post_process_csv_files.py --modes monthly seasonal

  # Process specific models within all modes
  python post_process_csv_files.py --models LR_1 SciRegressor_1

  # Preview changes without saving (dry run)
  python post_process_csv_files.py --dry-run

  # Combine options
  python post_process_csv_files.py --modes monthly --models LR_1 

  # With environment variables
  ieasyhydroforecast_env_file_path="path/to/.env" python post_process_csv_files.py
        """
    )

    parser.add_argument(
        '--modes',
        nargs='+',
        metavar='MODE',
        help='Forecast modes to process (default: all from ieasyhydroforecast_ml_long_term_supported_modes)'
    )

    parser.add_argument(
        '--models',
        nargs='+',
        metavar='MODEL_NAME',
        help='Model names to process (default: all models from config)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview what would be processed without making changes'
    )

    parser.add_argument(
        '--today',
        type=str,
        help='Override the "today" date in YYYY-MM-DD format (useful for testing)'
    )

    args = parser.parse_args()

    # Initialize today
    if args.today:
        today = datetime.strptime(args.today, '%Y-%m-%d').date()
    else:
        today = datetime.now().date()

    initialize_today(today)
    logger.info(f"Using 'today' date: {get_today()}")

    # Run post-processing
    try:
        results = post_process_csv_files(
            modes=args.modes,
            models=args.models,
            dry_run=args.dry_run
        )

        # Print summary
        print_summary(results)

        # Determine exit code
        all_success = True
        for mode_results in results.values():
            for model_results in mode_results.values():
                if not model_results.get('hindcast') and not model_results.get('forecast'):
                    all_success = False
                    break

        sys.exit(0 if all_success else 1)

    except Exception as e:
        logger.error(f"Fatal error during post-processing: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()
