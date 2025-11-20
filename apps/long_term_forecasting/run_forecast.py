##################################################
# Run Long Term Forecast
##################################################

## How to run this script:
# Set the environment variable ieasyhydroforecast_env_file_path to point to your .env file
# Then run the script with:
# ieasyhydroforecast_env_file_path="../../../kyg_data_forecast_tools/config/.env_develop_kghm" lt_forecast_mode=monthly python run_forecast.py


from datetime import datetime
import logging

# Suppress graphviz debug warnings BEFORE importing any modules that use graphviz
logging.getLogger("graphviz").setLevel(logging.WARNING)

import os
import sys
import time
import glob
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



def create_model_instance(
    model_type: str,
    model_name: str,
    configs: Dict[str, Any],
    data: pd.DataFrame,
    static_data: pd.DataFrame,
):
    """
    Create the appropriate model instance based on the model type.

    Args:
        model_type: 'LR' or 'SciRegressor'
        model_name: Name of the model configuration
        configs: All configuration dictionaries
        data: Time series data
        static_data: Static basin characteristics

    Returns:
        Model instance
    """
    general_config = configs["general_config"]
    model_config = configs["model_config"]
    feature_config = configs["feature_config"]
    path_config = configs["path_config"]

    # Set model name in general config
    general_config["model_name"] = model_name

    # Create model instance based on type
    if model_type == "linear_regression":
        model = LinearRegressionModel(
            data=data,
            static_data=static_data,
            general_config=general_config,
            model_config=model_config,
            feature_config=feature_config,
            path_config=path_config,
        )
    elif model_type == "sciregressor":
        model = SciRegressor(
            data=data,
            static_data=static_data,
            general_config=general_config,
            model_config=model_config,
            feature_config=feature_config,
            path_config=path_config,
        )
    elif model_type == "UncertaintyMixture":
        model = UncertaintyMixtureModel(
            data=data,
            static_data=static_data,
            general_config=general_config,
            model_config=model_config,
            feature_config=feature_config,
            path_config=path_config,
        )
    else:
        raise ValueError(f"Unknown model type: {model_type}")

    return model


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
    all_dependencies_paths = []
    for dep in model_dependencies.get(model_name, []):
        dep_path = forecast_configs.get_output_path(model_name=dep)
        dep_file = os.path.join(dep_path, f"{dep}_forecast.csv")
        if not os.path.exists(dep_file):
            logger.error(f"Dependency file {dep_file} for model {model_name} not found.")
        all_dependencies_paths.append(dep_file)

    configs["path_config"]["path_to_lr_predictors"] = all_dependencies_paths
    configs["path_config"]["path_to_base_predictors"] = all_dependencies_paths

        
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

def run_forecast():

    # Setup Environment
    sl.load_environment()

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


    for model_name in ordered_models:
        # Wait 5 seconds between model runs to avoid potential file access conflicts
        time.sleep(5)
        dependencies = model_dependencies.get(model_name, [])
        # Check if dependencies were successful
        deps_success = all(execution_is_success.get(dep, False) for dep in dependencies)
        if not deps_success:
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


    logger.info("Forecast run completed.")



if __name__ == "__main__":
    run_forecast()