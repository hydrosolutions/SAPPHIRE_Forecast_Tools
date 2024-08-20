# Title: extend_era5_reanalysis
# Author: sandro hunziker
# Description: Reads The Operational Data and the ERA5 Reanalysis Data and
#   appends the last 6 month of operational data to the ERA5 Reanalysis Data
#
# In operational mode, this file runs after Quantile_Mapping_OP.py
#
# --------------------------------------------------------------------
# USAGE
# SAPPHIRE_OPDEV_ENV=True  python extend_era5_reanalysis.py
# --------------------------------------------------------------------



# Import necessary libraries
import os
import sys
import json
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging
from logging.handlers import TimedRotatingFileHandler


# Local libraries
# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')
#print(script_dir)
#print(forecast_dir)

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the setup_library module from the iEasyHydroForecast package
import setup_library as sl


# Set up logging
# Configure the logging level and formatter
logging.basicConfig(level=logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Create the logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Create a file handler to write logs to a file
# A new log file is created every <interval> day at <when>. It is kept for <backupCount> days.
file_handler = TimedRotatingFileHandler('logs/log', when='midnight', interval=1, backupCount=30)
file_handler.setFormatter(formatter)

# Create a stream handler to print logs to the console
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Get the root logger and add the handlers to it
logger = logging.getLogger()
logger.handlers = []
logger.addHandler(file_handler)
logger.addHandler(console_handler)


def main():
    #--------------------------------------------------------------------
    # SETUP ENVIRONMENT
    #--------------------------------------------------------------------

    # Specify the path to the .env file
    # Loads the environment variables from the .env file
    sl.load_environment()

    #output_path for reanalysis
    OUTPUT_PATH_REANALYSIS = os.path.join(
        os.getenv('ieasyforecast_intermediate_data_path'),
        os.getenv('ieasyhydroforecast_OUTPUT_PATH_REANALYSIS'))
    # Test if the output path exists and create it if it doesn't
    if not os.path.exists(OUTPUT_PATH_REANALYSIS):
        os.makedirs(OUTPUT_PATH_REANALYSIS, exist_ok=True)

    #output_path for control member and ensemble
    OUTPUT_PATH_CM = os.path.join(
        os.getenv('ieasyforecast_intermediate_data_path'),
        os.getenv('ieasyhydroforecast_OUTPUT_PATH_CM'))
    # Test if the output path exists and create it if it doesn't
    if not os.path.exists(OUTPUT_PATH_CM):
        os.makedirs(OUTPUT_PATH_CM, exist_ok=True)

    logger.debug('OUTPUT_PATH_REANALYSIS: %s', OUTPUT_PATH_REANALYSIS)
    logger.debug('OUTPUT_PATH_CM: %s', OUTPUT_PATH_CM)

    #--------------------------------------------------------------------
    # READ IN THE FILES
    #--------------------------------------------------------------------
    CONTROL_MEMBER_HRUS = os.getenv('ieasyhydroforecast_HRU_CONTROL_MEMBER')
    control_member_hrus = [str(x) for x in CONTROL_MEMBER_HRUS.split(',')]


    for c_m in control_member_hrus:

        era5_reanalysis_P_file = os.path.join(OUTPUT_PATH_REANALYSIS, f'{c_m}_P_reanalysis.csv')
        era5_reanalysis_T_file = os.path.join(OUTPUT_PATH_REANALYSIS, f'{c_m}_T_reanalysis.csv')

        # Read in the ERA5 Reanalysis Data
        logger.debug('Reading in the ERA5 Reanalysis Data')
        era5_reanalysis_P = pd.read_csv(era5_reanalysis_P_file)
        era5_reanalysis_T = pd.read_csv(era5_reanalysis_T_file)

        # Read in the Operational Data
        operational_P_file = os.path.join(OUTPUT_PATH_CM, f'{c_m}_P_control_member.csv')
        operational_T_file = os.path.join(OUTPUT_PATH_CM, f'{c_m}_T_control_member.csv')

        logger.debug('Reading in the Operational Data')
        operational_P = pd.read_csv(operational_P_file)
        operational_T = pd.read_csv(operational_T_file)

        #--------------------------------------------------------------------
        operational_P['date'] = pd.to_datetime(operational_P['date'])
        operational_T['date'] = pd.to_datetime(operational_T['date'])

        era5_reanalysis_P['date'] = pd.to_datetime(era5_reanalysis_P['date'])
        era5_reanalysis_T['date'] = pd.to_datetime(era5_reanalysis_T['date'])

        #--------------------------------------------------------------------
        max_operational_date = operational_P['date'].max()

        date_threshold = max_operational_date - timedelta(days=195) # -6 months and - 15 days forecast

        operational_P = operational_P[operational_P['date'] < date_threshold].copy()
        operational_T = operational_T[operational_T['date'] < date_threshold].copy()

        #--------------------------------------------------------------------
        # APPEND THE OPERATIONAL DATA TO THE ERA5 REANALYSIS DATA
        #--------------------------------------------------------------------
        logger.debug('Appending the Operational Data to the ERA5 Reanalysis Data')
        era5_reanalysis_P = pd.concat([era5_reanalysis_P, operational_P], ignore_index=True)
        era5_reanalysis_T = pd.concat([era5_reanalysis_T, operational_T], ignore_index=True)

        # DROP DUPLICATES ON DATE AND CODE and keep the last one
        era5_reanalysis_P = era5_reanalysis_P.drop_duplicates(subset=['date', 'code'], keep='last')
        era5_reanalysis_T = era5_reanalysis_T.drop_duplicates(subset=['date', 'code'], keep='last')

        # sort by date and code
        era5_reanalysis_P = era5_reanalysis_P.sort_values(by=['date', 'code'])
        era5_reanalysis_T = era5_reanalysis_T.sort_values(by=['date', 'code'])

        #--------------------------------------------------------------------
        # SAVE THE APPENDED DATA
        #--------------------------------------------------------------------
        logger.debug('Saving the Appended Data')
        era5_reanalysis_P.to_csv(era5_reanalysis_P_file, index=False)
        era5_reanalysis_T.to_csv(era5_reanalysis_T_file, index=False)

    #--------------------------------------------------------------------
    # LOGGING
    #--------------------------------------------------------------------
    logger.debug('Finished')

if __name__ == '__main__':
    main()
