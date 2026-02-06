### 
# RUN FORECAST FOR PAST DATES FOR SIMULATION PURPOSES

# How to run:
# ieasyhydroforecast_env_file_path="path_to_env" lt_forecast_mode=month_1 python dev_code/simulate_forecasts.py --years 2024 2025 --all
###
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
    parser = argparse.ArgumentParser(description="Simulate long-term forecasts for past dates.")
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
        help="List of specific models to run (e.g., --models LR SciRegressor). If empty, all models will be run.",
    )

    parser.add_argument(
        "--num_months",
        type=int,
        default=12,
        help="Number of months to simulate.",
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

