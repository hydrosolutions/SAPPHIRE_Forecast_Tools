# --------------------------------------------------------------------
# LOAD LIBRARIES
# --------------------------------------------------------------------
# USAGE: SAPPHIRE_OPDEV_ENV=True SAPPHIRE_MODEL_TO_USE=TIDE python add_new_station.py
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
from scr import utils_ml_forecast



def call_hindcast_script(start_date: str,
                         end_date: str,
                         codes_hindcast: list,
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
    env['ieasyhydroforecast_START_DATE'] = start_date
    env['ieasyhydroforecast_END_DATE'] = end_date
    env['SAPPHIRE_HINDCAST_MODE'] = PREDICTION_MODE
    #transform the list to a string
    codes_hindcast = ','.join([str(code) for code in codes_hindcast])
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


    file_name = f'{MODEL_TO_USE}_{PREDICTION_MODE}_hindcast_daily_{start_date}_{end_date}.csv'

    hindcast = pd.read_csv(os.path.join(PATH_HINDCAST, file_name))

    return hindcast



def main():

    logger.info(f'--------------------------------------------------------------------')
    logger.info('Starting the script to add new stations to the forecast')
    print(f'--------------------------------------------------------------------')
    print('Starting the script to add new stations to the forecast')

    # --------------------------------------------------------------------
    # INITIALIZE THE ENVIRONMENT
    # --------------------------------------------------------------------

    # Load the environment variables
    sl.load_environment()
    # --------------------------------------------------------------------
    # DEFINE WHICH MODEL TO USE
    # --------------------------------------------------------------------
    MODEL_TO_USE = os.getenv('SAPPHIRE_MODEL_TO_USE')
    logger.info('Model to use: %s', MODEL_TO_USE)

    if MODEL_TO_USE not in ['TFT', 'TIDE', 'TSMIXER', 'ARIMA']:
        raise ValueError('Model not supported')
    else:
        print('Model to use: ', MODEL_TO_USE)

    # Access the environment variables
    intermediate_data_path = os.getenv('ieasyforecast_intermediate_data_path')
    OUTPUT_PATH_DISCHARGE = os.getenv('ieasyhydroforecast_OUTPUT_PATH_DISCHARGE')
    OUTPUT_PATH_DISCHARGE = os.path.join(intermediate_data_path, OUTPUT_PATH_DISCHARGE)
    #Extend the OUTPUT_PATH_DISCHARGE with the model name,
    OUTPUT_PATH_DISCHARGE = os.path.join(OUTPUT_PATH_DISCHARGE, MODEL_TO_USE)

    PATH_ERA5_REANALYSIS = os.getenv('ieasyhydroforecast_OUTPUT_PATH_REANALYSIS')
    PATH_ERA5_REANALYSIS = os.path.join(intermediate_data_path, PATH_ERA5_REANALYSIS)

    # Check if the directory exists and there is a file in it
    if not os.path.exists(PATH_ERA5_REANALYSIS):
        raise ValueError('The directory does not exist: ', PATH_ERA5_REANALYSIS)

    if not os.listdir(PATH_ERA5_REANALYSIS):
       raise ValueError('No forcing data. Please run the script: get_era5_reanalysis_data.py')

    # --------------------------------------------------------------------
    # Read in the decad and pentad forecast files already created
    # --------------------------------------------------------------------

    # Read in the decad forecast
    file_name = f'decad_{MODEL_TO_USE}_forecast.csv'
    decad_forecast = pd.read_csv(os.path.join(OUTPUT_PATH_DISCHARGE, file_name), parse_dates=['date'])
    decad_forecast['forecast_date'] = pd.to_datetime(decad_forecast['forecast_date'])

    # Read in the pentad forecast
    file_name = f'pentad_{MODEL_TO_USE}_forecast.csv'
    pentad_forecast = pd.read_csv(os.path.join(OUTPUT_PATH_DISCHARGE, file_name), parse_dates=['date'])
    pentad_forecast['forecast_date'] = pd.to_datetime(pentad_forecast['forecast_date'])

    # --------------------------------------------------------------------
    # Define the start and end date
    # --------------------------------------------------------------------
    start_date = min(min(decad_forecast['forecast_date']), min(pentad_forecast['forecast_date'])).strftime('%Y-%m-%d')
    end_date = max(max(decad_forecast['forecast_date']), max(pentad_forecast['forecast_date'])).strftime('%Y-%m-%d')

    logger.info('Start date: %s', start_date)
    logger.info('End date: %s', end_date)
    # --------------------------------------------------------------------
    # Check which new codes are available
    # --------------------------------------------------------------------
    rivers_to_predict_pentad, rivers_to_predict_decad, hydroposts_available_for_ml_forecasting = utils_ml_forecast.get_hydroposts_for_pentadal_and_decadal_forecasts()
    # Combine rivers_to_predict_pentad and rivers_to_predict_decad to get all rivers to predict, only keep unique values
    rivers_to_predict = list(set(rivers_to_predict_pentad + rivers_to_predict_decad))
    #select only codes which the model can predict.
    mask_predictable = hydroposts_available_for_ml_forecasting[MODEL_TO_USE] == True
    codes_model_can_predict = hydroposts_available_for_ml_forecasting[mask_predictable]['code'].tolist()
    rivers_to_predict = list(set(rivers_to_predict) & set(codes_model_can_predict))
    #convert to int
    rivers_to_predict = [int(code) for code in rivers_to_predict]

    # --------------------------------------------------------------------
    # Compare which codes are new
    # --------------------------------------------------------------------
    # Get the codes that are already in the decad forecast
    codes_decad_forecast = decad_forecast['code'].unique()
    pentad_codes = pentad_forecast['code'].unique()

    # Get the codes that are new
    new_codes_decad = list(set(rivers_to_predict) - set(codes_decad_forecast))
    new_codes_pentad = list(set(rivers_to_predict) - set(pentad_codes))

    logger.info('New codes for decad forecast: %s', new_codes_decad)
    logger.info('New codes for pentad forecast: %s', new_codes_pentad)

    # --------------------------------------------------------------------
    # Call the hindcast script
    # --------------------------------------------------------------------
    #Pentad
    if len(new_codes_pentad) > 0:
        logger.info('Starting Hindcast Pentad in daily intervall')
        pentad_hindcast_daily = call_hindcast_script(start_date,
                                                        end_date,
                                                        new_codes_pentad,
                                                        MODEL_TO_USE,
                                                        intermediate_data_path,
                                                        'PENTAD')

        logger.info('Pentad hindcast daily is generated')
        pentad_hindcast_daily['forecast_date'] = pd.to_datetime(pentad_hindcast_daily['forecast_date'])
        pentad_hindcast_daily['date'] = pd.to_datetime(pentad_hindcast_daily['date'])

        # Append the new pentad forecast to the existing pentad forecast
        pentad_forecast = pd.concat([pentad_forecast, pentad_hindcast_daily], ignore_index=True)
        #Sort by forecast_date and date
        pentad_forecast = pentad_forecast.sort_values(by=['forecast_date', 'date'])
        # remove duplicates by code, date and forecast_date
        pentad_forecast = pentad_forecast.drop_duplicates(subset=['code', 'date', 'forecast_date'])

        # Save
        path_pentad_out_daily = os.path.join(OUTPUT_PATH_DISCHARGE, f'pentad_{MODEL_TO_USE}_forecast.csv')
        pentad_forecast.to_csv(path_pentad_out_daily, index=False)
        print(f'The forecast files for model pentadal {MODEL_TO_USE} are saved in the directory: {OUTPUT_PATH_DISCHARGE}')
    else:
        logger.info('No new codes for pentad forecast')

    if len(new_codes_decad) > 0:
        #Decad
        logger.info('Starting Hindcast Decad in daily intervall')
        decad_hindcast_daily = call_hindcast_script(start_date,
                                                    end_date,
                                                    new_codes_decad,
                                                    MODEL_TO_USE,
                                                    intermediate_data_path,
                                                    'DECAD')

        logger.info('Decad hindcast daily is generated')
        decad_hindcast_daily['forecast_date'] = pd.to_datetime(decad_hindcast_daily['forecast_date'])
        decad_hindcast_daily['date'] = pd.to_datetime(decad_hindcast_daily['date'])

        # Append the new decad forecast to the existing decad forecast
        decad_forecast = pd.concat([decad_forecast, decad_hindcast_daily], ignore_index=True)
        # Sort by forecast_date and date
        decad_forecast = decad_forecast.sort_values(by=['forecast_date', 'date'])
        # remove duplicates by code, date and forecast_date
        decad_forecast = decad_forecast.drop_duplicates(subset=['code', 'date', 'forecast_date'])

        # Save
        path_decad_out_daily = os.path.join(OUTPUT_PATH_DISCHARGE, f'decad_{MODEL_TO_USE}_forecast.csv')
        decad_forecast.to_csv(path_decad_out_daily, index=False)
        print(f'The forecast files for model decadal {MODEL_TO_USE} are saved in the directory: {OUTPUT_PATH_DISCHARGE}')

    else:
        logger.info('No new codes for decad forecast')

    logger.info('The script to add new stations to the forecast is finished')
    print('The script to add new stations to the forecast is finished')
    print(f'--------------------------------------------------------------------')
    logger.info(f'--------------------------------------------------------------------')


if __name__ == '__main__':
    main()

