##################################################
# Run Long Term Forecast
##################################################

## How to run this script:
# Set the environment variable ieasyhydroforecast_env_file_path to point to your .env file
# Then run the script with:
# ieasyhydroforecast_env_file_path="path_to_env" lt_forecast_mode=monthly python run_forecast.py


from datetime import datetime
import logging

# Suppress graphviz debug warnings BEFORE importing any modules that use graphviz
logging.getLogger("graphviz").setLevel(logging.WARNING)

import os
import sys
import time
import glob
import traceback
import pandas as pd
import numpy as np
import json
from typing import List, Dict, Any, Tuple

# Import forecast models
from lt_forecasting.forecast_models.LINEAR_REGRESSION import LinearRegressionModel
from lt_forecasting.forecast_models.SciRegressor import SciRegressor
from lt_forecasting.forecast_models.deep_models.uncertainty_mixture import (
    UncertaintyMixtureModel,
)

from __init__ import logger 
from data_interface import DataInterface
from config_forecast import ForecastConfig
from lt_utils import create_model_instance


# set lt_forecasting logger level
logger_lt = logging.getLogger("lt_forecasting")
logger_lt.setLevel(logging.DEBUG)

# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the setup_library module from the iEasyHydroForecast package
import setup_library as sl


def run_single_model(data_interface: DataInterface,
                     forecast_configs: ForecastConfig,
                     model_name: str,
                     temporal_data: pd.DataFrame,
                     static_data: pd.DataFrame,
                     offset_base: int,
                     offset_discharge: int) -> Dict[str, Any]:
    
    """
    Run a single forecast model and return the results.
    """

    # Load configurations
    configs = forecast_configs.get_model_specific_config(model_name=model_name)
    model_type = configs["general_config"]["model_type"]

    # Set the model path
    model_path = forecast_configs.all_paths.get(model_name)
    # move up one level to model home path
    model_home_path = os.path.dirname(model_path)
    configs["path_config"]["model_home_path"] = model_home_path

    #################################################
    # This part will be replaced by a database query in future [DATABASE INTEGRATION]
    #################################################
    model_dependencies = forecast_configs.get_model_dependencies()
    all_dependencies_forecast_paths = []
    all_dependencies_hindcast_paths = []
    for dep in model_dependencies.get(model_name, []):
        dep_path = forecast_configs.get_output_path(model_name=dep)
        dep_file_forecast = os.path.join(dep_path, f"{dep}_forecast.csv")
        dep_file_hindcast = os.path.join(dep_path, f"{dep}_hindcast.csv")
        if not os.path.exists(dep_file_forecast):
            logger.error(f"Dependency file {dep_file_forecast} for model {model_name} not found.")
        if not os.path.exists(dep_file_hindcast):
            logger.error(f"Dependency file {dep_file_hindcast} for model {model_name} not found.")

        
        all_dependencies_forecast_paths.append(dep_file_forecast)
        all_dependencies_hindcast_paths.append(dep_file_hindcast)

    # Used by the GBT LR models which take the predictions of other models as input features
    configs["path_config"]["path_to_lr_predictors"] = all_dependencies_forecast_paths
    # Used by the Uncertainty Mixture models which take the hindcast of other models as input features
    # This is needed to compute the uncertainty based on past model errors
    configs["path_config"]["path_to_base_predictors"] = all_dependencies_hindcast_paths

    logger.info(f"Running model: {model_name} of type {model_type}")

    data_dependencies = forecast_configs.get_data_dependencies(model_name=model_name)
    can_be_run = True
    
    for input_type, offset in data_dependencies.items():
        if input_type == "SnowMapper":
            # Extend base data with snow data
            snow_HRUs = configs["path_config"].get("snow_HRUs", [])
            snow_variables = configs["path_config"].get("snow_variables", [])
            snow_result = data_interface.extend_base_data_with_snow(
                base_data=temporal_data,
                HRUs_snow=snow_HRUs,
                snow_variables=snow_variables
            )
            temporal_data = snow_result["temporal_data"]
            offset_snow = snow_result["offset_date_snow"]
            logger.info(f"Extended data with snow. Offset days: {offset_snow}")
            if offset_snow is not None and offset_snow > offset:
                logger.warning(f"Snow data offset ({offset_snow}) is greater than required offset ({offset})")
                can_be_run = False
        elif input_type == "Discharge":
            # Here we could implement additional logic for discharge data if needed
            if offset_discharge > offset:
                logger.warning(f"Discharge data offset ({offset_discharge}) is greater than required offset ({offset})")
                can_be_run = False
        elif input_type == "EMCWF_Forecast":
            # Here we could implement additional logic for EMCWF forecast data if needed
            if offset_base > offset:
                logger.warning(f"Base data offset ({offset_base}) is greater than required offset ({offset})")
                can_be_run = False
        else:
            logger.warning(f"Unknown data dependency type: {input_type}")

    if can_be_run:
        today = datetime.now()
        # Create model instance
        model_instance = create_model_instance(
            model_type=model_type,
            model_name=model_name,
            configs=configs,
            data=temporal_data,
            static_data=static_data,
        )

        # Run forecast
        forecast = model_instance.predict_operational(today=today)
        forecast = forecast.round(2)
        forecast['flag'] = 0
        success = True

    else:
        logger.error(f"Cannot run model {model_name} due to missing or outdated data.")
        forecast = pd.DataFrame()  # Empty DataFrame as placeholder
        forecast['flag'] = 2
        success = False
    

    #################################################
    # This part will be replaced by a database query in future [DATABASE INTEGRATION]
    #################################################
    # Save Forecast
    output_path = forecast_configs.get_output_path(model_name=model_name)
    os.makedirs(output_path, exist_ok=True)
    output_file = os.path.join(output_path, f"{model_name}_forecast.csv")
    forecast.to_csv(output_file, index=False)
    logger.info(f"Forecast for model {model_name} saved to {output_file}")

    # Append the forecast to the hindcast files
    hindcast_file = os.path.join(output_path, f"{model_name}_hindcast.csv")
    if os.path.exists(hindcast_file):
        df_hindcast = pd.read_csv(hindcast_file)
        df_hindcast['date'] = pd.to_datetime(df_hindcast['date'])
        df_hindcast['code'] = df_hindcast['code'].astype(int)
        df_combined = pd.concat([df_hindcast, forecast], ignore_index=True)
    else:
        logger.info(f"Hindcast file {hindcast_file} does not exist. Can not append forecast.")

    # Return success
    return success

def run_forecast(
        forecast_all: bool = True,
        models_to_run: List[str] = []
):

    # Setup Environment
    sl.load_environment()

    if forecast_all:
        if len(models_to_run) > 0:
            raise ValueError("If forecast_all is True, models_to_run should be empty.")

    # Now we setup the configurations
    forecast_config = ForecastConfig()

    forecast_mode = os.getenv('lt_forecast_mode')
    forecast_config.load_forecast_config(forecast_mode=forecast_mode)
    forcing_HRU = forecast_config.get_forcing_HRU()

    # Data Interface
    data_interface = DataInterface()
    base_data_dict = data_interface.get_base_data(forcing_HRU=forcing_HRU)

    temporal_data = base_data_dict["temporal_data"]
    static_data = base_data_dict["static_data"]
    offset_base = base_data_dict["offset_date_base"]
    offset_discharge = base_data_dict["offset_date_discharge"]

    ordered_models = forecast_config.get_model_execution_order()
    execution_is_success = {}
    model_dependencies = forecast_config.get_model_dependencies()

    if not forecast_all:
        # Filter ordered_models to only include those in models_to_run
        ordered_models = [m for m in ordered_models if m in models_to_run]
        # we check dependencies again in the run_single_model function
        ignore_initial_dependencies = True
    else:
        ignore_initial_dependencies = False

    for model_name in ordered_models:
        # Wait 5 seconds between model runs to avoid potential file access conflicts
        time.sleep(5)
        dependencies = model_dependencies.get(model_name, [])
        # Check if dependencies were successful
        deps_success = all(execution_is_success.get(dep, False) for dep in dependencies)
        if not deps_success and not ignore_initial_dependencies:
            logger.error(f"Skipping model {model_name} due to failed dependencies: {dependencies}")
            execution_is_success[model_name] = False
            continue

        try:
            sucess = run_single_model(
                data_interface=data_interface,
                forecast_configs=forecast_config,
                model_name=model_name,
                temporal_data=temporal_data.copy(),
                static_data=static_data,
                offset_base=offset_base,
                offset_discharge=offset_discharge
            )
            execution_is_success[model_name] = sucess
        except Exception as e:
            logger.error(f"Error running model {model_name}: {e}")
            # get the full traceback
            traceback_str = traceback.format_exc()
            logger.error(f"Traceback: {traceback_str}")
            execution_is_success[model_name] = False

    # Print summary
    logger.info("\n" + "="*50)
    logger.info("FORECAST SUMMARY")
    logger.info("="*50)
    for model_name, success in execution_is_success.items():
        status = "SUCCESS" if success else "FAILED"
        logger.info(f"{model_name}: {status}")
    logger.info("="*50 + "\n")

    logger.info("Forecast run completed.")



if __name__ == "__main__":

    import argparse
    
    parser = argparse.ArgumentParser(
        description="Run forecasts for long-term models",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run forecasts for all models
  python run_forecast.py --all
  
  # Run forecasts for specific models
  python run_forecast.py --models LinearRegressionModel SciRegressor
  
  # With environment variables
  ieasyhydroforecast_env_file_path="path/to/.env" lt_forecast_mode=monthly python run_forecast.py --all
        """
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--all',
        action='store_true',
        help='Run forecasts for all models'
    )
    group.add_argument(
        '--models',
        nargs='+',
        metavar='MODEL_NAME',
        help='List of model names to forecast'
    )
    
    args = parser.parse_args()
    
    # Determine recalibrate_all flag and models to run
    recalibrate_all = args.all
    models_to_run = args.models if args.models else []
    
    run_forecast(
        forecast_all=recalibrate_all,
        models_to_run=models_to_run
    )