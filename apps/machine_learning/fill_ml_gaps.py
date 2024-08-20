#
#
# ----------------------------------------------------------------
# FILE: fill_ml_gaps.py
# ----------------------------------------------------------------
#
# Description: This script checks if there are any missing forecasts from the ML models.
# If there are, it calls the hindcast script to fill in the missing forecasts
# in order to make the ML model forecasts continuous for evaluation.
#
# ----------------------------------------------------------------
# USAGE:
# ----------------------------------------------------------------
# SAPPHIRE_OPDEV_ENV=True SAPPHIRE_MODEL_TO_USE=TFT SAPPHIRE_PREDICTION_MODE=PENTAD 
# ieasyhydroforecasts_produce_daily_ml_hindcast=False python fill_ml_gaps.py
# TODO: So far this code only checks if there are missing forecast dates. It doesn't check if there are nan values due to insufficent input data.
# TODO: Think about how to handle nan values, in what frequency should this be checked?


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
logger = logging.getLogger('make_ml_hindcast')
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

def call_hindcast_script(min_missing_date: str,
                         max_missing_date: str,
                         MODEL_TO_USE: str,
                         intermediate_data_path: str,
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

    # Prepare the command
    command = ['python', 'hindcast_ML_models.py']

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




def fill_ml_gaps():
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


    # check if daily or pentadal hindcast should be produced
    ieasyhydroforecasts_produce_daily_ml_hindcast = os.getenv('ieasyhydroforecasts_produce_daily_ml_hindcast')

    # Get the current date
    current_date = datetime.datetime.now().date()
    current_date = current_date.strftime('%Y-%m-%d')

    if PREDICTION_MODE == 'PENTAD':
        prefix = 'pentad'
    else:
        prefix = 'decad'

    # Read the latest forecast
    if ieasyhydroforecasts_produce_daily_ml_hindcast == 'True':
        forecast_path = os.path.join(PATH_FORECAST, prefix + '_' +  MODEL_TO_USE + '_forecast.csv')
        limit_day_gap = 1
    else:
        forecast_path = os.path.join(PATH_FORECAST,prefix + '_' +  MODEL_TO_USE + '_forecast_' + prefix + '_intervall.csv')
        if PREDICTION_MODE == 'PENTAD':
            limit_day_gap = 6
        else:
            limit_day_gap = 11


    try:
        forecast = pd.read_csv(forecast_path)
    except FileNotFoundError:
        logger.error('No forecast file found')
        return


    forecast_dates = forecast['forecast_date'].unique()
    forecast_dates = pd.to_datetime(forecast_dates)
    forecast_dates = forecast_dates.sort_values()
    # check if there are any missing forecasts
    missing_forecasts = []

    for i in range(1, len(forecast_dates)):
        if (forecast_dates[i] - forecast_dates[i-1]).days > limit_day_gap:
            missing_tuple = (forecast_dates[i-1], forecast_dates[i])
            # append the previous date with a forecast
            # append the next date which has a forecast
            missing_forecasts.append(missing_tuple)

    if len(missing_forecasts) == 0:
        logger.info('No missing forecasts')
    else:

        for missing_days in missing_forecasts:
            start_date = missing_days[0]
            end_date = missing_days[1]

            # Subtract 1 day from the maximum date, as in the hindcast script, a hindcast is produced for the last day, but we already have a forecast for that day
            end_date = end_date - datetime.timedelta(days=1)

            start_date = start_date.strftime('%Y-%m-%d')
            end_date = end_date.strftime('%Y-%m-%d')

            print('Missing forecasts from', start_date, 'to', end_date)

            # Call the hindcast script
            hindcast = call_hindcast_script(start_date, end_date, MODEL_TO_USE, intermediate_data_path, PREDICTION_MODE)

            # Append the hindcast to the forecast
            forecast = pd.concat([forecast, hindcast], ignore_index=True)

            # sort the forecast by forecast_date
            forecast = forecast.sort_values(by='forecast_date')

            # save the forecast

            # TODO: Remove the test part in the name, after the modul is tested.
            if ieasyhydroforecasts_produce_daily_ml_hindcast == 'True':
                forecast.to_csv(os.path.join(PATH_FORECAST, prefix + '_' +  MODEL_TO_USE + '_forecast_test.csv'), index=False)
            else:
                forecast.to_csv(os.path.join(PATH_FORECAST,prefix + '_' +  MODEL_TO_USE + '_forecast_' + prefix + '_intervall_test.csv'), index=False)


        print("Missing forecasts filled in")



if __name__ == '__main__':
    fill_ml_gaps()