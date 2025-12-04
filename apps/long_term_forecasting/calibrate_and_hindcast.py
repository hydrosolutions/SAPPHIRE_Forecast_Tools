##################################################
# Calibrate and Hindcast Long Term Forecast
##################################################

## How to run this script:
# Set the environment variable ieasyhydroforecast_env_file_path to point to your .env file
# Then run the script with:
# ieasyhydroforecast_env_file_path="path_to_env" lt_forecast_mode=monthly python calibrate_and_hindcast.py


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

from data_interface import DataInterface
from config_forecast import ForecastConfig
from lt_utils import create_model_instance

# set lt_forecasting logger level
logger_lt = logging.getLogger("lt_forecasting")
logger_lt.setLevel(logging.INFO)

# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the setup_library module from the iEasyHydroForecast package
import setup_library as sl

from __init__ import logger 

def tune_hyperparameters_model(
        model_name: str,
        forecast_configs: ForecastConfig,
        temporal_data: pd.DataFrame,
        static_data: pd.DataFrame,
) -> Tuple[bool, str]:
    """
    Tune hyperparameters for a given model instance.
    Args:
        model_name (str): The name of the forecast model.
        forecast_configs (ForecastConfig): The forecast configuration object.
        temporal_data (pd.DataFrame): The temporal data for the model.
        static_data (pd.DataFrame): The static data for the model.
    Returns:
        Tuple[bool, str]: A tuple containing a boolean indicating success and a message.
    """
    # Load configurations
    configs = forecast_configs.get_model_specific_config(model_name=model_name)
    model_type = configs["general_config"]["model_type"]

    logger.info(f"Tuning hyperparameters for model: {model_name} of type {model_type}")

    # Set the model path
    model_path = forecast_configs.all_paths.get(model_name)
    # move up one level to model home path
    model_home_path = os.path.dirname(model_path)
    configs["path_config"]["model_home_path"] = model_home_path

    today = datetime.now()
    this_year = today.year
    hparam_tuning_years = [int(this_year - i) for i in range(0, 4)]  # Last 4 years
    logger.info(f"Hyperparameter tuning years for model {model_name}: {hparam_tuning_years}")
    configs["general_config"]["hparam_tuning_years"] = hparam_tuning_years

    #################################################
    # This part will be replaced by a database query in future [DATABASE INTEGRATION]
    #################################################
    model_dependencies = forecast_configs.get_model_dependencies()
    all_dependencies_paths = []
    for dep in model_dependencies.get(model_name, []):
        dep_path = forecast_configs.get_output_path(model_name=dep)
        dep_file = os.path.join(dep_path, f"{dep}_hindcast.csv")
        if not os.path.exists(dep_file):
            logger.error(f"Dependency file {dep_file} for model {model_name} not found.")
        all_dependencies_paths.append(dep_file)

    configs["path_config"]["path_to_lr_predictors"] = all_dependencies_paths
    configs["path_config"]["path_to_base_predictors"] = all_dependencies_paths


    # Create model instance
    model_instance = create_model_instance(
        model_type=model_type,
        model_name=model_name,
        configs=configs,
        data=temporal_data,
        static_data=static_data,
    )

    # Run hyperparameter tuning
    try:
        success, message = model_instance.tune_hyperparameters()
    except Exception as e:
        logger.error(f"Error during hyperparameter tuning for model {model_name}: {e}")
        success = False
        message = str(e)

    return success, message


def calibrate_model(data_interface: DataInterface,
                    forecast_configs: ForecastConfig,
                    model_name: str,
                    temporal_data: pd.DataFrame,
                    static_data: pd.DataFrame,
                    offset_base: datetime,
                    offset_discharge: datetime) -> bool:
    
    """
    Run a single calibration model.
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
        dep_file = os.path.join(dep_path, f"{dep}_hindcast.csv")
        if not os.path.exists(dep_file):
            logger.error(f"Dependency file {dep_file} for model {model_name} not found.")
        all_dependencies_paths.append(dep_file)

    configs["path_config"]["path_to_lr_predictors"] = all_dependencies_paths
    configs["path_config"]["path_to_base_predictors"] = all_dependencies_paths

    logger.info(f"Running model: {model_name} of type {model_type}")

    data_dependencies = forecast_configs.get_data_dependencies(model_name=model_name)
    
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

    # Create model instance
    model_instance = create_model_instance(
        model_type=model_type,
        model_name=model_name,
        configs=configs,
        data=temporal_data,
        static_data=static_data,
    )

    # Run hindcast
    try:
        hindcast = model_instance.calibrate_model_and_hindcast()
        # round numerical cols to .1 decimal
        hindcast = hindcast.round(2)
        hindcast['flag'] = 1
        success = True
    except Exception as e:
        # raise the full error
        logger.error(f"Error during calibration and hindcast for model {model_name}: {e}")
        return False

    #################################################
    # This part will be replaced by a database query in future [DATABASE INTEGRATION]
    #################################################
    # Save Forecast
    output_path = forecast_configs.get_output_path(model_name=model_name)
    os.makedirs(output_path, exist_ok=True)
    output_file = os.path.join(output_path, f"{model_name}_hindcast.csv")
    hindcast.to_csv(output_file, index=False)
    logger.info(f"Hindcast  for model {model_name} saved to {output_file}")

    # Return success
    return success



def calibrate_and_hindcast(
        recalibrate_all: bool,
        models_to_run: List[str],
        tune_hyperparameters: bool = False) -> Dict[str, bool]:
    
    """
    Calibrate and hindcast for all models in the specified mode.
    Args:
        models_to_run (List[str]): List of model names to run.
    """
    if not recalibrate_all and len(models_to_run) == 0:
        raise ValueError("If recalibrate_all is False, models_to_run must be specified.")
    
    # Setup Environment
    sl.load_environment()

    # Now we setup the configurations
    forecast_config = ForecastConfig()

    forecast_mode = os.getenv('lt_forecast_mode')
    forecast_config.load_forecast_config(forecast_mode=forecast_mode)

    if recalibrate_all:
        logger.info("Recalibrate all models flag is set. All models will be recalibrated.")
        models_to_run = forecast_config.get_models_to_run()

    # Initialize calibration status for all models to False
    for model_name in models_to_run:
        forecast_config.update_calibration_status(
            model_name=model_name,
            status=False
        )

        if tune_hyperparameters:
            forecast_config.update_hyperparameter_tuning_status(
                model_name=model_name,
                status=False
            )
    
    # Write this to config
    forecast_config.write_updated_config()

    forcing_HRU = forecast_config.get_forcing_HRU()

    # Data Interface
    data_interface = DataInterface()
    start_date = forecast_config.get_start_date()
    logger.info(f"Using start date for base data: {start_date}")
    base_data_dict = data_interface.get_base_data(forcing_HRU=forcing_HRU, start_date=start_date)

    temporal_data = base_data_dict["temporal_data"]
    static_data = base_data_dict["static_data"]
    offset_base = base_data_dict["offset_date_base"]
    offset_discharge = base_data_dict["offset_date_discharge"]

    ordered_models = forecast_config.get_model_execution_order()
    execution_is_success = {}
    model_dependencies = forecast_config.get_model_dependencies()

    for model_name in ordered_models:
        if model_name not in models_to_run:
            logger.info(f"Skipping model {model_name} as it is not in the list of models to run.")
            continue

        # get the calibration status 
        calibration_status = forecast_config.get_calibration_status(model_name=model_name)
        if calibration_status:
            logger.info(f"Skipping model {model_name} as it is already calibrated.")
            execution_is_success[model_name] = True
            continue

        # Check if dependencies are met
        dependencies = model_dependencies.get(model_name, [])
        for dep in dependencies:
            calibration_status = forecast_config.get_calibration_status(model_name=dep)
            if not calibration_status:
                logger.error(f"Cannot run model {model_name} because dependency {dep} is not calibrated.")
                execution_is_success[model_name] = False
                continue

        if tune_hyperparameters:
            hparam_tuning_status = forecast_config.get_hyperparameter_tuning_status(model_name=model_name)
            if not hparam_tuning_status:
                logger.info(f"Tuning hyperparameters for model {model_name}.")
                success, message = tune_hyperparameters_model(
                    model_name=model_name,
                    forecast_configs=forecast_config,
                    temporal_data=temporal_data,
                    static_data=static_data,
                )
                if success:
                    logger.info(f"Hyperparameter tuning for model {model_name} completed successfully.")
                else:
                    logger.error(f"Hyperparameter tuning for model {model_name} failed: {message}")
                    logger.error("Skipping calibration due to failed hyperparameter tuning.")
                    execution_is_success[model_name] = False
                    continue
                
                # Update hyperparameter tuning status
                forecast_config.update_hyperparameter_tuning_status(
                    model_name=model_name,
                    status=success
                )

                # Write this to config
                forecast_config.write_updated_config()

                time.sleep(10)  # small delay to ensure output files are written properly

        success = calibrate_model(
            data_interface=data_interface,
            forecast_configs=forecast_config,
            model_name=model_name,
            temporal_data=temporal_data,
            static_data=static_data,
            offset_base=offset_base,
            offset_discharge=offset_discharge
        )

        execution_is_success[model_name] = success
        # Update calibration status
        forecast_config.update_calibration_status(
            model_name=model_name,
            status=success
        )

        # Write this to config
        forecast_config.write_updated_config()
        
    logger.info("Calibration and hindcast run completed.")
    
    
    return execution_is_success


def main():
    """
    Main function to run calibration and hindcast via command line.
    
    Usage examples:
        # Calibrate all models
        python calibrate_and_hindcast.py --all
        
        # Calibrate specific models
        python calibrate_and_hindcast.py --models LinearRegressionModel SciRegressor
        
        # With environment file
        ieasyhydroforecast_env_file_path="../../../kyg_data_forecast_tools/config/.env_develop_kghm" \
        lt_forecast_mode=monthly python calibrate_and_hindcast.py --all
    """
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Calibrate and hindcast long-term forecast models",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Calibrate all models
  python calibrate_and_hindcast.py --all
  
  # Calibrate specific models
  python calibrate_and_hindcast.py --models LinearRegressionModel SciRegressor
  
  # With environment variables
  ieasyhydroforecast_env_file_path="path/to/.env" lt_forecast_mode=monthly python calibrate_and_hindcast.py --all
        """
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--all',
        action='store_true',
        help='Calibrate all models'
    )
    group.add_argument(
        '--models',
        nargs='+',
        metavar='MODEL_NAME',
        help='List of model names to calibrate'
    )
    parser.add_argument(
        '--tune_hyperparameters',
        action='store_true',
        default=False,
        help='Flag to indicate if hyperparameter tuning should be performed before calibration'
    )
    args = parser.parse_args()
    
    # Determine recalibrate_all flag and models to run
    recalibrate_all = args.all
    models_to_run = args.models if args.models else []
    tune_hyperparameters = args.tune_hyperparameters
    
    # Run calibration
    try:
        results = calibrate_and_hindcast(
            recalibrate_all=recalibrate_all,
            models_to_run=models_to_run,
            tune_hyperparameters=tune_hyperparameters
        )
        
        # Print summary
        logger.info("\n" + "="*50)
        logger.info("CALIBRATION SUMMARY")
        logger.info("="*50)
        for model_name, success in results.items():
            status = "✓ SUCCESS" if success else "✗ FAILED"
            logger.info(f"{model_name}: {status}")
        logger.info("="*50 + "\n")
        
        # Exit with appropriate code
        if all(results.values()):
            sys.exit(0)
        else:
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error during calibration: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()