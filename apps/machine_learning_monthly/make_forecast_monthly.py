# --------------------------------------------------------------------
# MONTHLY FORECASTING WITH MACHINE LEARNING MODELS
# --------------------------------------------------------------------

# Useage:
# SAPPHIRE_OPDEV_ENV=True SAPPHIRE_MONTHLY_MODELS=LR_SWE SAPPHIRE_MONTHLY_HORIZON=1  python make_forecast.py
# SAPPHIRE_MONTHLY_MODELS: Can be a list (defined in the env file) or a single str value
# Some possible values: LR_Base, LR_SWE, LR_SWE_SLOPE, LR_SWE_500m ..., 
# LGBM, XGB, CatBoost, LGBM_with_LR, ...
# SAPPHIRE_MONTHLY_HORIZON: 1, 2, 3, 4, 5, 6, 
# This variables defines which month should be forecasted

# --------------------------------------------------------------------
# Load Libraries
# --------------------------------------------------------------------
import os
import sys
import glob
import pandas as pd
import numpy as np
import json

import datetime

import logging
from logging.handlers import TimedRotatingFileHandler
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
# Ensure the logs directory exists
logs_dir = 'logs'
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)
file_handler = TimedRotatingFileHandler('logs/log', when='midnight',
                                        interval=1, backupCount=30)
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger = logging.getLogger('make_forecast_monthly')
logger.setLevel(logging.DEBUG)
logger.handlers = []
logger.addHandler(file_handler)
#logger.addHandler(console_handler)

import warnings
warnings.filterwarnings("ignore")

# Print logging level of the logger
logger.info('Logging level: %s', logger.getEffectiveLevel())
# Level 10: DEBUG, Level 20: INFO, Level 30: WARNING, Level 40: ERROR, Level 50: CRITICAL
logger.debug('Debug message for logger level 10')

# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the setup_library module from the iEasyHydroForecast package
import setup_library as sl


# Custom Libraries
from base_config import PATH_CONFIG, GENERAL_CONFIG, MODEL_CONFIG
from forecast_models import LINEAR_REGRESSION

def make_forecast_monthly():
    logger.info(f'--------------------------------------------------------------------')
    logger.info(f"Starting make_forecast.py")

    # --------------------------------------------------------------------
    # DEFINE WHICH MODEL TO USE
    # --------------------------------------------------------------------
    MODEL_TO_USE = os.getenv('SAPPHIRE_MONTHLY_MODELS')
    logger.debug('Model to use: %s', MODEL_TO_USE)

    HORIZON = os.getenv('SAPPHIRE_MONTHLY_HORIZON')
    HORIZON = int(HORIZON)
    logger.debug('Horizon to use: %s', HORIZON)

    # Load the environment variables
    sl.load_environment()

    AVAILABLE_MODELS = os.getenv('SAPPHIRE_MONTHLY_MODELS_AVAILABLE')
    AVAILABLE_MODELS = [model.strip() for model in AVAILABLE_MODELS.split(',')]

    if MODEL_TO_USE not in AVAILABLE_MODELS:
        raise ValueError(f"Model {MODEL_TO_USE} is not available. Available models are: {AVAILABLE_MODELS}")
    
    configuration_path = os.getenv('ieasyforecast_configuration_path')
    # Access the environment variables
    intermediate_data_path = os.getenv('ieasyforecast_intermediate_data_path')
    
    PATH_TO_MONTHLY_MODELS = os.getenv('ieasyhydroforecast_ml_monthly_path_to_models')
    PATH_TO_MONTHLY_MODELS = os.path.join(configuration_path, PATH_TO_MONTHLY_MODELS)

    PATH_TO_MODEL = os.path.join(PATH_TO_MONTHLY_MODELS, MODEL_TO_USE)
    PATH_TO_MODEL = os.path.join(PATH_TO_MODEL, f"HORIZON_{HORIZON}")

    # check if the folder exists
    if not os.path.exists(PATH_TO_MODEL):
        raise ValueError(f"Model folder {PATH_TO_MODEL} does not exist. Please check the model name and horizon.")


    PATH_TO_STATIC_FEATURES = os.getenv('ieasyhydroforecast_PATH_TO_STATIC_FEATURES')
    # Path to the output directory
    OUTPUT_PATH_DISCHARGE = os.getenv('ieasyhydroforecast_OUTPUT_PATH_MONTHLY_FORECAST')
    # Downscaled weather data
    PATH_TO_QMAPPED_ERA5 = os.getenv('ieasyhydroforecast_PATH_TO_QMAPPED_ERA5')
    # PATH TO SNOW DATA
    PATH_TO_SNOW_DATA = os.getenv('ieasyhydroforecast_OUTPUT_PATH_SNOW')
    
    HRU_ML_MODELS = os.getenv('ieasyhydroforecast_HRU_MONTHLY_FORCING')

    PATH_TO_STATIC_FEATURES = os.path.join(PATH_TO_MONTHLY_MODELS, PATH_TO_STATIC_FEATURES)
    
    OUTPUT_PATH_DISCHARGE = os.path.join(intermediate_data_path, OUTPUT_PATH_DISCHARGE)
    #Extend the OUTPUT_PATH_DISCHARGE with the model name
    OUTPUT_PATH_DISCHARGE = os.path.join(OUTPUT_PATH_DISCHARGE, MODEL_TO_USE)

    PATH_TO_QMAPPED_ERA5 = os.path.join(intermediate_data_path, PATH_TO_QMAPPED_ERA5)

    PATH_TO_SNOW_DATA = os.path.join(intermediate_data_path, PATH_TO_SNOW_DATA)

if __name__ == "__main__":
    make_forecast_monthly()