# This script is used to initialize the forecast tool based on machine learning.
# It will call the hindcast method to generate the file, where the forecast will be stored.
# The script will ask the user to enter the start and end date for the forecast.
# 
# USAGE: SAPPHIRE_OPDEV_ENV=True SAPPHIRE_MODEL_TO_USE=TIDE python initialize_ml_tool.py
# 
# NOTE: This script may take some time to run, depending on the size of the data and the model used.

# --------------------------------------------------------------------
# LOAD LIBRARIES
# --------------------------------------------------------------------
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


def call_hindcast_script(start_date: str,
                         end_date: str, 
                         MODEL_TO_USE: str, 
                         intermediate_data_path: str,
                         daily_predictions: str,
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
    env['ieasyhydroforecasts_produce_daily_ml_hindcast'] = daily_predictions

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

    
    file_name = f'{MODEL_TO_USE}_{PREDICTION_MODE}_hindcast_daily_{start_date}_{end_date}.csv' 

    hindcast = pd.read_csv(os.path.join(PATH_HINDCAST, file_name))

    return hindcast



def main():
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
    # read the start and end date from the users input
    # --------------------------------------------------------------------
    print(f"Initialize Machine Learning Model {MODEL_TO_USE}: Produce Hindcast for the following time period")
    start_date = input("Enter the start date in the format YYYY-MM-DD: ")
    end_date = input("Enter the end date in the format YYYY-MM-DD: ")

    # --------------------------------------------------------------------
    # Call the hindcast script for the given model, in PENTAD, DECAD mode.
    # --------------------------------------------------------------------
    logger.info('Start date: %s', start_date)
    logger.info('End date: %s', end_date)
    logger.info('Model to use: %s', MODEL_TO_USE)

    print("Starting Hindcast Pentad in pentad intervall")
    pentad_hindcast = call_hindcast_script(start_date, 
                                           end_date, 
                                           MODEL_TO_USE, 
                                           intermediate_data_path, 
                                           'False', 
                                           'PENTAD')
    logger.info('Pentad hindcast is generated')
    
    # save the pentad hindcast to a csv file at the right location. So the operational script can access it.
    path_pentad_out = os.path.join(OUTPUT_PATH_DISCHARGE, f'pentad_{MODEL_TO_USE}_forecast_pentad_intervall.csv')
    pentad_hindcast.to_csv(path_pentad_out, index=False)
    del pentad_hindcast

    print("Starting Hindcast Pentad in daily intervall")
    pentad_hindcast_daily = call_hindcast_script(start_date,
                                                    end_date,
                                                    MODEL_TO_USE,
                                                    intermediate_data_path,
                                                    'True',
                                                    'PENTAD')
    logger.info('Pentad hindcast daily is generated')
    # save the pentad hindcast to a csv file at the right location. So the operational script can access it.
    path_pentad_out_daily = os.path.join(OUTPUT_PATH_DISCHARGE, f'pentad_{MODEL_TO_USE}_forecast.csv')
    pentad_hindcast_daily.to_csv(path_pentad_out_daily, index=False)
    del pentad_hindcast_daily

    print("Starting Hindcast Decad in decad intervall")
    decad_hindcast = call_hindcast_script(start_date,
                                            end_date,
                                            MODEL_TO_USE,
                                            intermediate_data_path,
                                            'False',
                                            'DECAD')
    logger.info('Decad hindcast is generated')
    # save the decad hindcast to a csv file at the right location. So the operational script can access it.
    path_decad_out = os.path.join(OUTPUT_PATH_DISCHARGE, f'decad_{MODEL_TO_USE}_forecast_decad_intervall.csv')
    decad_hindcast.to_csv(path_decad_out, index=False)
    del decad_hindcast
    print("Starting Hindcast Decad in daily intervall")
    decad_hindcast_daily = call_hindcast_script(start_date,
                                                end_date,
                                                MODEL_TO_USE,
                                                intermediate_data_path,
                                                'True',
                                                'DECAD')
    logger.info('Decad hindcast daily is generated')
    # save the decad hindcast to a csv file at the right location. So the operational script can access it.
    path_decad_out_daily = os.path.join(OUTPUT_PATH_DISCHARGE, f'decad_{MODEL_TO_USE}_forecast.csv')
    decad_hindcast_daily.to_csv(path_decad_out_daily, index=False)
    del decad_hindcast_daily

    print(f'The forecast files for model {MODEL_TO_USE} are saved in the directory: {OUTPUT_PATH_DISCHARGE}')

if __name__ == '__main__':
    main()

    
    
    



