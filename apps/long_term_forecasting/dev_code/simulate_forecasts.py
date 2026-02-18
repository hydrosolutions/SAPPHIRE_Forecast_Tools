"""Simulate long-term forecasts for past dates.

Re-runs the forecast pipeline as if "today" were a historical date,
using the same code path as production (run_forecast). Useful for:
- Validating model output after code changes
- Comparing results against a known-good baseline
- Diagnosing whether issues come from code, data, or model config

Run from the long_term_forecasting/ directory.

Environment variables (required):
    ieasyhydroforecast_env_file_path  Path to the .env config file
        (in the external data repo, e.g. kyg_data_forecast_tools/config/)
    lt_forecast_mode                  Which forecast mode to run.
        Valid values come from ieasyhydroforecast_ml_long_term_supported_modes
        in the .env file, typically: month_1, month_2, ... month_9
        (one JSON config per mode under the LT config directory).

Examples:
    # Run all models for Jan-Dec 2024, mode month_1:
    ieasyhydroforecast_env_file_path=~/path/to/.env \\
      lt_forecast_mode=month_1 \\
      python dev_code/simulate_forecasts.py --years 2024 --all

    # Run only the base linear regression for 2024-2025, first 3 months:
    ieasyhydroforecast_env_file_path=~/path/to/.env \\
      lt_forecast_mode=month_1 \\
      python dev_code/simulate_forecasts.py --years 2024 2025 \\
        --models LR_Base --num_months 3

    # Run stacking model and its dependency:
    ieasyhydroforecast_env_file_path=~/path/to/.env \\
      lt_forecast_mode=month_1 \\
      python dev_code/simulate_forecasts.py --years 2024 \\
        --models LR_Base SM_GBT

Valid --models names (defined per mode in the JSON config under
models_to_use; these are the names known at time of writing):
    Model types (model_type in general_config.json):
        linear_regression   -> LinearRegressionModel
        sciregressor         -> SciRegressor (gradient-boosted stacking)
        UncertaintyMixture   -> UncertaintyMixtureModel (MC_ALD)

    Typical model names (folder names under the model directory):
        LR_Base       Base linear regression
        LR_SM         Linear regression with snowmelt features
        LR_SM_DT      Linear regression with snowmelt + detrending
        LR_SM_ROF     Linear regression with snowmelt + runoff features
        SM_GBT        Stacking GBT (depends on base LR models)
        SM_GBT_LR     Stacking GBT variant with LR base
        SM_GBT_Norm   Stacking GBT with normalization
        MC_ALD        Monte Carlo Asymmetric Laplace (uncertainty model)
        GBT           Standalone gradient-boosted trees

    The actual set of available models depends on which model folders
    exist for the chosen forecast mode. With --all, the config's
    models_to_use dict is read and models are run in dependency order.

    When using --models, dependencies are NOT auto-included. If SM_GBT
    depends on LR_Base, you must list both: --models LR_Base SM_GBT
    (they will be sorted into the correct execution order).

Data requirement — discharge must cover the simulated date:
    The simulated "today" is set to day_of_forecast (from config) of each
    requested month. SciRegressor models (GBT, SM_GBT, etc.) extract
    rolling-window features (e.g. 3-day, 7-day means) from discharge data
    at that date. If discharge observations do not extend to the simulated
    "today", short-window features will be NaN. With the default
    allowable_missing_value_operational=0, even one NaN feature causes
    every basin to be skipped, producing "No prediction data available for
    any basin."

    This means simulate_forecasts only works reliably for dates where
    discharge data already exists in the database. For example, if today
    is 2026-02-18 and discharge data is current through that date:
      --years 2025 --num_months 12   # OK: all 2025 dates are in the past
      --years 2026 --num_months 1    # OK: Jan 25 is before Feb 18
      --years 2026 --num_months 2    # FAILS: Feb 25 is after Feb 18

    LinearRegression models are less affected because they handle missing
    features differently, but SciRegressor models will fail for any
    simulated date beyond the latest available discharge observation.

Output:
    Forecast CSVs are written to the path defined by
    ieasyhydroforecast_ml_long_term_output_path (relative to
    ieasyforecast_intermediate_data_path) as {model_name}_forecast.csv
    and {model_name}_hindcast.csv. If the SAPPHIRE API is available,
    forecasts are also written to the database.
"""
import argparse
import logging
import os
import shutil
import sys
from datetime import datetime
from typing import List, Optional, Dict, Any

import pandas as pd
from torch import mode
from tqdm import tqdm

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
from run_forecast import run_forecast


# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the setup_library module from the iEasyHydroForecast package
import setup_library as sl



def simulate_forecasts(
        years: List[int],
        forecast_all = True,
        models_to_run: List[str] = [],
        num_months: int = 1,
) -> Dict[bool,  str]:
    
    # Setup environment
    sl.load_environment()
    forecast_mode = os.getenv('lt_forecast_mode')

    
    logger.info(f"\n{'='*60}")
    logger.info(f"Processing mode: {forecast_mode}")
    logger.info(f"{'='*60}")


    # Load the forecast config for this mode
    try:
        forecast_config = ForecastConfig()
        forecast_config.load_forecast_config(forecast_mode=forecast_mode)
    except Exception as e:
        logger.error(f"Failed to load forecast config for mode '{forecast_mode}': {e}")
        return {False: f"Failed to load forecast config for mode '{forecast_mode}': {e}"}

    day_of_forecast = forecast_config.get_operational_issue_day()

    # 2. Iterate over years and run forecast for each year
    total_iterations = len(years) * num_months
    for year, month in tqdm(
        [(y, m) for y in years for m in range(1,  num_months + 1)],
        total=total_iterations,
        desc="Simulating forecasts"
    ):
        # 1. set today to the desired date
        today_date = datetime(year=year, month=month, day=day_of_forecast) 
        initialize_today(today_date)

        # 2. Run forecast
        run_forecast(
            forecast_all=forecast_all,
            models_to_run=models_to_run,
            forecast_mode=forecast_mode,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Simulate long-term forecasts for past dates.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Valid model names (typical, depends on config):\n"
            "  LR_Base, LR_SM, LR_SM_DT, LR_SM_ROF,\n"
            "  SM_GBT, SM_GBT_LR, SM_GBT_Norm,\n"
            "  MC_ALD, GBT\n\n"
            "Use --all to run every model in the config's "
            "models_to_use dict.\n"
            "With --models, list dependencies explicitly "
            "(e.g. --models LR_Base SM_GBT)."
        ),
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        required=True,
        help="List of years to simulate forecasts for (e.g., --years 2020 2021).",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="If set, run forecasts for all models. If not set, use --models to specify models.",
    )
    parser.add_argument(
        "--models",
        type=str,
        nargs="*",
        default=[],
        help="Model names to run, e.g. --models LR_Base SM_GBT. "
             "Must match folder names in the mode's model directory. "
             "Include dependencies explicitly. Mutually exclusive with --all.",
    )

    parser.add_argument(
        "--num_months",
        type=int,
        default=12,
        help="Number of months to simulate per year (1-12, default: 12). "
             "Months 1..num_months are iterated for each year.",
    )

    # all and models are mutually exclusive
    args = parser.parse_args()
    if args.all and args.models:
        logger.error("Arguments --all and --models are mutually exclusive. Please specify only one.")
        sys.exit(1)
    
    simulate_forecasts(
        years=args.years,
        forecast_all=args.all,
        models_to_run=args.models,
        num_months=args.num_months
    )

