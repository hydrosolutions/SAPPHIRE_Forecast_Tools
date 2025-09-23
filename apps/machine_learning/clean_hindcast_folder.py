# ----------------------------------------------------------------
# FILE: clean_hindcast_folder.py
# ----------------------------------------------------------------
# Description: This script cleans the hindcast folder by removing files. 
# Those files are only used temporarily during the hindcast process.
# It helps to free up disk space.
# ----------------------------------------------------------------
# USAGE:
# ieasyhydroforecast_env_file_path="your_env_path" SAPPHIRE_MODEL_TO_USE=TFT python clean_hindcast_folder.py
# ----------------------------------------------------------------


import os
import sys
import pandas as pd
import numpy as np
import datetime
import subprocess

import logging
from logging.handlers import TimedRotatingFileHandler
logging.getLogger("pytorch_lightning.utilities.rank_zero").setLevel(logging.WARNING)
logging.getLogger("pytorch_lightning.accelerators.cuda").setLevel(logging.WARNING)
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
logger = logging.getLogger('recalculate_nan_forecasts')
logger.setLevel(logging.DEBUG)
logger.handlers = []
logger.addHandler(file_handler)

import warnings
warnings.filterwarnings("ignore")




# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the setup_library module from the iEasyHydroForecast package
import setup_library as sl


def clean_hindcast_folder():
    """
    Cleans the hindcast folder by removing files that are only used temporarily during the hindcast process.
    This helps to free up disk space.
    """
    logger.info(f'--------------------------------------------------------------------')
    logger.info(f"Starting clean_hindcast_folder.py")

    # --------------------------------------------------------------------
    # DEFINE WHICH MODEL TO USE
    # --------------------------------------------------------------------
    MODEL_TO_USE = os.getenv('SAPPHIRE_MODEL_TO_USE')
    logger.info('Model to use: %s', MODEL_TO_USE)
    print('Model to use:', MODEL_TO_USE)

    if MODEL_TO_USE not in ['TFT', 'TIDE', 'TSMIXER', 'ARIMA']:
        raise ValueError('Model not supported')

    # --------------------------------------------------------------------
    # INITIALIZE THE ENVIRONMENT
    # --------------------------------------------------------------------
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Specify the path to the .env file
    sl.load_environment()

    # --------------------------------------------------------------------
    # GET THE LATEST FORECAST
    # --------------------------------------------------------------------
    intermediate_data_path = os.getenv('ieasyforecast_intermediate_data_path')

    # Path to the output directory
    OUTPUT_PATH_DISCHARGE = os.getenv('ieasyhydroforecast_OUTPUT_PATH_DISCHARGE')

    PATH_FORECAST = os.path.join(intermediate_data_path, OUTPUT_PATH_DISCHARGE)

    PATH_HINDCAST = os.path.join(PATH_FORECAST, 'hindcast', MODEL_TO_USE)

    # check if the path exists
    if not os.path.exists(PATH_HINDCAST):
        raise ValueError(f'Path to the hindcast folder does not exist: {PATH_HINDCAST}, Cannot clean it.')

    logger.info(f'Path to the hindcast folder: {PATH_HINDCAST}')

    # --------------------------------------------------------------------
    # check how many files are in the hindcast folder
    # --------------------------------------------------------------------
    files_in_hindcast = os.listdir(PATH_HINDCAST)

    # check if they are csv files
    csv_files_in_hindcast = [f for f in files_in_hindcast if f.endswith('.csv')]
    num_csv_files_in_hindcast = len(csv_files_in_hindcast)
    logger.info(f'Number of csv files in the hindcast folder: {num_csv_files_in_hindcast}')

    # the files must contain "hindcast" in their name
    hindcast_files_in_hindcast = [f for f in csv_files_in_hindcast if 'hindcast' in f]
    num_hindcast_files_in_hindcast = len(hindcast_files_in_hindcast)
    logger.info(f'Number of hindcast csv files in the hindcast folder with "hindcast" in name: {num_hindcast_files_in_hindcast}')

    # --------------------------------------------------------------------
    # Remove all files csv in the hindcast folder
    # --------------------------------------------------------------------
    for file in hindcast_files_in_hindcast:
        file_path = os.path.join(PATH_HINDCAST, file)
        try:
            os.remove(file_path)
            logger.info(f'Removed file: {file_path}')
        except Exception as e:
            logger.error(f'Error removing file {file_path}: {e}')

    # --------------------------------------------------------------------
    logger.info(f'Cleaning completed.')
    # --------------------------------------------------------------------

if __name__ == "__main__":
    clean_hindcast_folder()

