#
#
# ----------------------------------------------------------------
# FILE: fill_ml_gaps.py
# ----------------------------------------------------------------
#
# Description: This script checks if there are any missing forecasts from the ML models.
# If there are, it calls the hindcast script to fill in the missing forecasts
# in order to make the ML model forecasts continuous for evaluation.
# NOTE: This script only fills in the values which are not represented in the forecast file.
# If there are nan values in the forecast file, they will not be filled in.
# ----------------------------------------------------------------
# USAGE:
# SAPPHIRE_OPDEV_ENV=True SAPPHIRE_MODEL_TO_USE=TFT SAPPHIRE_PREDICTION_MODE=PENTAD python fill_ml_gaps.py
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
    env['ieasyhydroforecast_NEW_STATIONS'] = 'None'

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




def fill_ml_gaps():

    logger.info(f'--------------------------------------------------------------------')
    logger.info(f"Starting fill_ml_gaps.py")
    print(f'--------------------------------------------------------------------')
    print(f"Starting fill_ml_gaps.py")

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
    limit_day_gap = 1


    try:
        forecast = pd.read_csv(forecast_path)
    except FileNotFoundError:
        logger.error('No forecast file found')
        return

    missing_forecasts_dict = {}
    min_missing_date = None
    max_missing_date = None
    #iterate over the unique codes
    for code in forecast.code.unique():
        #select the forecast for the specific code
        forecast_code = forecast[forecast.code == code].copy()
        #get the unique forecast dates
        forecast_dates = forecast_code['forecast_date'].unique()
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

        # update the missing_forecasts_dict and min_missing_date and max_missing_date
        if len(missing_forecasts) > 0:
            missing_forecasts_dict[code] = missing_forecasts
            min_missing_date_current = missing_forecasts[0][0]
            max_missing_date_current = missing_forecasts[-1][1]

            if min_missing_date is None:
                min_missing_date = min_missing_date_current
                max_missing_date = max_missing_date_current
            else:
                min_missing_date = min(min_missing_date, min_missing_date_current)
                max_missing_date = max(max_missing_date, max_missing_date_current)

    # if there are no missing forecasts
    if len(missing_forecasts_dict) == 0:
        logger.info('No missing forecasts')
        print('No missing forecasts')

    # if there are missing forecasts
    else:

        # get the minimum and maximum missing dates
        min_missing_date = min_missing_date.strftime('%Y-%m-%d')
        max_missing_date = max_missing_date - datetime.timedelta(days=1)
        max_missing_date = max_missing_date.strftime('%Y-%m-%d')

        logger.info('Missing forecasts from %s to %s', min_missing_date, max_missing_date)
        print('Missing forecasts from', min_missing_date, 'to', max_missing_date)
        print("Missing forecasts for the following code:", list(missing_forecasts_dict.keys()))

        # trigger the hindcast script to fill in the missing forecasts
        hindcast = call_hindcast_script(min_missing_date, max_missing_date,
            MODEL_TO_USE, intermediate_data_path, PREDICTION_MODE)

        hindcast['forecast_date'] = pd.to_datetime(hindcast['forecast_date'])

        #now iterate and fill the missing forecasts,
        #this complicated way is needed to ensure that the original forecast are not overwrtitten by the hindcast
        for code, missing_forecasts in missing_forecasts_dict.items():
            mask_dates = pd.Series(False, index=hindcast.index)
            for missing_forecast in missing_forecasts:
                mask_dates = mask_dates | ((hindcast.forecast_date >= missing_forecast[0]) & (hindcast.forecast_date <= missing_forecast[1]))

            mask_fill = (hindcast.code == code) & mask_dates

            hindcast_missing = hindcast[mask_fill].copy()

            # append the missing forecasts to the original forecast
            forecast = pd.concat([forecast, hindcast_missing], axis=0)


        forecast['forecast_date'] = pd.to_datetime(forecast['forecast_date'])
        # sort the forecast by forecast_date
        forecast = forecast.sort_values(by='forecast_date')
        # save the forecast
        forecast.to_csv(os.path.join(PATH_FORECAST, prefix + '_' +  MODEL_TO_USE + '_forecast.csv'), index=False)


        logger.info('Missing forecasts filled in')

    logger.info('Script fill_ml_gaps.py finished at %s. Exiting.', datetime.datetime.now())
    logger.info(f'--------------------------------------------------------------------')
    print('Script fill_ml_gaps.py finished at', datetime.datetime.now())
    print(f'--------------------------------------------------------------------')


if __name__ == '__main__':
    fill_ml_gaps()