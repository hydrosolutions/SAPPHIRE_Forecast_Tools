# ----------------------------------------------------------------
# FILE: recalculate_nan_forecasts.py
# ----------------------------------------------------------------
#
# Description: This script checks if there are any nan values in the forecasts and then recalculates them.
# Nan values from operational forecasts have flag == 0, while nan values from hindcasts have flag == 1.
# This script checks if there are nan values in the forecasts and then recalculates them, by calling the hindcast script.
# The hindcast will return a file which is already flagged: 
# - flag == 3 for nan values even after the hindcast
# - flag == 4 for valid values after the hindcast
# ----------------------------------------------------------------
# USAGE:
# SAPPHIRE_OPDEV_ENV=True SAPPHIRE_MODEL_TO_USE=TFT SAPPHIRE_PREDICTION_MODE=PENTAD python recalculate_nan_forecasts.py
# ----------------------------------------------------------------
import os
import sys
import pandas as pd
import numpy as np
import datetime
import subprocess

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
logger = logging.getLogger('recalculate_nan_forecasts')
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

def call_hindcast_script(min_missing_date: str,
                         max_missing_date: str,
                         MODEL_TO_USE: str,
                         intermediate_data_path: str,
                         codes_with_nan: list,
                         PREDICTION_MODE: str) -> pd.DataFrame:

    # --------------------------------------------------------------------
    # CALL THE HINDCAST SCRIPT
    # --------------------------------------------------------------------
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    # Ensure the environment variable is set
    env = os.environ.copy()
    env['SAPPHIRE_MODEL_TO_USE'] = MODEL_TO_USE
    env['ieasyhydroforecast_START_DATE'] = min_missing_date
    env['ieasyhydroforecast_END_DATE'] = max_missing_date
    env['SAPPHIRE_HINDCAST_MODE'] = PREDICTION_MODE
    codes_hindcast = ','.join([str(code) for code in codes_with_nan])
    env['ieasyhydroforecast_NEW_STATIONS'] = codes_hindcast

    # Prepare the command
    if (os.getenv('IN_DOCKER') == 'True'):
        command = ['python', 'apps/machine_learning/hindcast_ML_models.py']
        print('Running in Docker, calling command:', command)
        logger.info('Running in Docker, calling command: %s', command)
    else:
        command = ['python', 'hindcast_ML_models.py']
        print('Running locally, calling command:', command)
        logger.info('Running locally, calling command: %s', command)


    # Call the script
    result = subprocess.run(command, capture_output=True, text=True, env=env)

    # Check if the script ran successfully
    if result.returncode == 0:
        print("Hindcast ran successfully!")
        print()
    else:
        print("Hindcast failed with return code", result.returncode)
        print("Error output:")
        print(result.stderr)
        print()

    # --------------------------------------------------------------------
    # GET THE HINDCAST
    # --------------------------------------------------------------------
    # Path to the output directory
    OUTPUT_PATH_DISCHARGE = os.getenv('ieasyhydroforecast_OUTPUT_PATH_DISCHARGE')

    PATH_FORECAST = os.path.join(intermediate_data_path, OUTPUT_PATH_DISCHARGE)

    PATH_HINDCAST = os.path.join(PATH_FORECAST, 'hindcast', MODEL_TO_USE)


    file_name = f'{MODEL_TO_USE}_{PREDICTION_MODE}_hindcast_daily_{min_missing_date}_{max_missing_date}.csv'

    hindcast = pd.read_csv(os.path.join(PATH_HINDCAST, file_name))

    return hindcast


def recalculate_nan_forecasts():
        # --------------------------------------------------------------------
    # DEFINE WHICH MODEL TO USE
    # --------------------------------------------------------------------
    MODEL_TO_USE = os.getenv('SAPPHIRE_MODEL_TO_USE')
    logger.info('Model to use: %s', MODEL_TO_USE)
    print('Model to use:', MODEL_TO_USE)

    if MODEL_TO_USE not in ['TFT', 'TIDE', 'TSMIXER', 'ARIMA']:
        raise ValueError('Model not supported')

    # --------------------------------------------------------------------
    # Define whch prediction mode to use
    # --------------------------------------------------------------------
    PREDICTION_MODE = os.getenv('SAPPHIRE_PREDICTION_MODE')
    logger.debug('Prediction mode: %s', PREDICTION_MODE)
    if PREDICTION_MODE not in ['PENTAD', 'DECAD']:
        raise ValueError('Prediction mode %s is not supported.\nPlease choose one of the following prediction modes: PENTAD, DECAD')

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
    PATH_FORECAST = os.path.join(PATH_FORECAST, MODEL_TO_USE)

    PATH_HINDCAST = os.path.join(PATH_FORECAST, 'hindcast', MODEL_TO_USE)

    # Get the current date
    current_date = datetime.datetime.now().date()
    current_date = current_date.strftime('%Y-%m-%d')

    if PREDICTION_MODE == 'PENTAD':
        prefix = 'pentad'
    else:
        prefix = 'decad'


    forecast_path = os.path.join(PATH_FORECAST, prefix + '_' +  MODEL_TO_USE + '_forecast.csv')

    try:
        forecast = pd.read_csv(forecast_path)
    except FileNotFoundError:
        logger.error('No forecast file found')
        return

    unique_codes = forecast['code'].unique()

    codes_with_nan = []
    min_missing_dates = []
    max_missing_dates = []

    forecast['flag'] = forecast['flag'].astype(int, errors='ignore')
    forecast['date'] = pd.to_datetime(forecast['date'])
    forecast['forecast_date'] = pd.to_datetime(forecast['forecast_date'])

    for code in unique_codes:
        #select the forecast for the specific code
        forecast_code = forecast[forecast['code'] == code].copy()

        #check where the flag is equal to 1
        nan_values = forecast_code[forecast_code['flag'].isin([1,2])]

        if nan_values.shape[0] > 0:
            min_missing_date = nan_values['forecast_date'].min()
            max_missing_date = nan_values['forecast_date'].max()

            min_missing_dates.append(min_missing_date)
            max_missing_dates.append(max_missing_date)
            codes_with_nan.append(code)


    if len(codes_with_nan) == 0:
        logger.debug('No forecasts to recalculate')
        logger.info('No forecasts to recalculate. Exiting recalculate_nan_forecasts.py\n')
        return

    #call the hindcast script
    max_date = max(max_missing_dates).strftime('%Y-%m-%d')
    #min date - 1 day
    min_date = min(min_missing_dates) - datetime.timedelta(days=1)
    min_date = min_date.strftime('%Y-%m-%d')

    logger.debug('Recalculating forecasts for codes %s', codes_with_nan)
    logger.debug('Min missing date: %s', min_date)
    logger.debug('Max missing date: %s', max_date)

    hindcast = call_hindcast_script(min_missing_date=min_date,
                                    max_missing_date=max_date,
                                    MODEL_TO_USE=MODEL_TO_USE,
                                    intermediate_data_path=intermediate_data_path,
                                    codes_with_nan=codes_with_nan,
                                    PREDICTION_MODE=PREDICTION_MODE)

    # --------------------------------------------------------------------
    # UPDATE THE FORECAST
    # Only replace the values with flag == 1
    hindcast['flag'] = hindcast['flag'].astype(int)
    hindcast['date'] = pd.to_datetime(hindcast['date'])
    hindcast['forecast_date'] = pd.to_datetime(hindcast['forecast_date'])

    def update_forecast(forecast_code, hindcast_code):
        # Fix the syntax error in value_cols definition
        value_cols = [col for col in forecast_code.columns if 'Q' in col]
        
        forecast_code = forecast_code.copy()
        hindcast_code = hindcast_code.copy()
        
        # Get dates where flag is 1
        forecast_dates_flag1 = forecast_code[forecast_code['flag'].isin([1,2])]['forecast_date'].unique()
        
        # Only update those specific dates
        for forecast_date in forecast_dates_flag1:
            mask = forecast_code['forecast_date'] == forecast_date
            hindcast_mask = hindcast_code['forecast_date'] == forecast_date
            
            if hindcast_mask.any():  # Check if we have matching hindcast data
                forecast_code.loc[mask, value_cols] = hindcast_code.loc[hindcast_mask, value_cols].values
                forecast_code.loc[mask, 'flag'] = hindcast_code.loc[hindcast_mask, 'flag'].values
        
        return forecast_code

    # Main loop
    for code in codes_with_nan:
        forecast_code = forecast[forecast['code'] == code].copy()
        hindcast_code = hindcast[hindcast['code'] == code].copy()
        forecast[forecast['code'] == code] = update_forecast(forecast_code, hindcast_code)


    # Save the updated forecast
    forecast['forecast_date'] = pd.to_datetime(forecast['forecast_date'])
    # sort the forecast by forecast_date
    forecast = forecast.sort_values(by='forecast_date')
    # save the forecast
    forecast.to_csv(os.path.join(PATH_FORECAST, prefix + '_' +  MODEL_TO_USE + '_forecast.csv'), index=False)

    logger.info('Nan Values are replaced. Exiting recalculate_nan_forecasts.py\n')

if __name__ == '__main__':
    recalculate_nan_forecasts()