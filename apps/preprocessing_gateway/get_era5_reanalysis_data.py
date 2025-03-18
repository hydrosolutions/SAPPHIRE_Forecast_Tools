# Title: get_era5_reanalysis_data
# Author: sandro hunziker
# Description: This script downloads ERA5 reanalysis data from the data gateway
# and performs quantile mapping on it.
# Afterwards the script is save as a csv file.
#
# NOTE: This script is used to get the initial data for the reanalysis. 
# Afterwards the data is always extended with the operational reanalysis data
# This extension is done with the script: extend_era5_reanalysis.py
#
# --------------------------------------------------------------------
# USAGE
# SAPPHIRE_OPDEV_ENV=True ieasyhydroforecast_reanalysis_START_DATE=2009-01-01 ieasyhydroforecast_reanalysis_END_DATE=2023-12-31 python get_era5_reanalysis_data.py
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

import dg_utils

# Note that the sapphire data gateway client is currently a private repository
# Access to the repository is required to install the package
# Further, access to the data gateway through an API key is required to use the
# client. The API key is stored in a .env file in the root directory of the project.
# The forecast tools can be used without access to the sapphire data gateay but
# the full power of the tools is only available with access to the data gateway.
#pip install git+https://github.com/hydrosolutions/sapphire-dg-client.git
import sapphire_dg_client


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

    # Test if an API key is available and exit the program if it isn't
    if not os.getenv('ieasyhydroforecast_API_KEY_GATEAWAY'):
        logger.warning("No API key for the data gateway found. Exiting program.\nMachine learning or conceptual models will not be run.")
        sys.exit(1)
    else:
        API_KEY = os.getenv('ieasyhydroforecast_API_KEY_GATEAWAY')

    #output_path for the data from the data gateaway
    OUTPUT_PATH_DG = os.path.join(
        os.getenv('ieasyforecast_intermediate_data_path'),
        os.getenv('ieasyhydroforecast_OUTPUT_PATH_DG'))
    # Test if the output path exists and create it if it doesn't
    if not os.path.exists(OUTPUT_PATH_DG):
        os.makedirs(OUTPUT_PATH_DG, exist_ok=True)


    #output_path for reanalysis
    OUTPUT_PATH_REANALYSIS = os.path.join(
        os.getenv('ieasyforecast_intermediate_data_path'),
        os.getenv('ieasyhydroforecast_OUTPUT_PATH_REANALYSIS'))
    # Test if the output path exists and create it if it doesn't
    if not os.path.exists(OUTPUT_PATH_REANALYSIS):
        os.makedirs(OUTPUT_PATH_REANALYSIS, exist_ok=True)


    Q_MAP_PARAM_PATH = os.path.join(
        os.getenv('ieasyhydroforecast_models_and_scalers_path'),
        os.getenv('ieasyhydroforecast_Q_MAP_PARAM_PATH'))
    # Test if the output path exists. Raise an error if it doesn't
    if not os.path.exists(Q_MAP_PARAM_PATH):
        logger.warning(f"Path {Q_MAP_PARAM_PATH} does not exist.\nParameters for quantile mapping of ERA5 and ECMWF ensemble forecast are not available.\nProducing weather data files that are not downscaled.")
        perform_qmapping=False
    else:
        perform_qmapping=True

    CONTROL_MEMBER_HRUS = os.getenv('ieasyhydroforecast_HRU_CONTROL_MEMBER')

        #start date is the date where the first forecast is made
    start_date = os.getenv('ieasyhydroforecast_reanalysis_START_DATE')
    end_date = os.getenv('ieasyhydroforecast_reanalysis_END_DATE')

    #initialize the client
    client = sapphire_dg_client.client.SapphireDGClient(api_key= API_KEY)
    #get the codes for the HRU's
    control_member_hrus = [str(x) for x in CONTROL_MEMBER_HRUS.split(',')]


    #--------------------------------------------------------------------
    # CONTROL MEMBER MAPPING
    #--------------------------------------------------------------------
    logger.debug("Current working directory: %s", os.getcwd())
    logger.debug(f"Iterating over the control member HRUs: {control_member_hrus}")
    for c_m_hru in control_member_hrus:
        print(f"Processing  HRU: {c_m_hru}")
        #download the control member data
        logger.info(f"Processing  HRU: {c_m_hru}")
        # Initialize control_member_era5 to None
        control_member_era5 = None

        control_member_era5 = client.era5_land.get_era5_land(c_m_hru,
                date=start_date,
                end_date=end_date,
                directory=OUTPUT_PATH_DG
                )


        logger.debug(f"Control Member Data for HRU {c_m_hru} downloaded")
        logger.debug(f"for start_date: {start_date}")
        logger.debug(f"saved to directory: {OUTPUT_PATH_DG}")
        logger.debug(f"Control Member Data Path: {control_member_era5}")

        df_c_m = pd.read_csv(control_member_era5)
        #transform the data file
        transformed_data_file = dg_utils.transform_data_file_control_member(df_c_m)
        transformed_data_file['code'] = transformed_data_file['code'].astype(str)

        #get the parameters if available
        if perform_qmapping:
            P_params_hru = pd.read_csv(os.path.join(Q_MAP_PARAM_PATH, f'HRU{c_m_hru}_P_params.csv'))
            T_params_hru = pd.read_csv(os.path.join(Q_MAP_PARAM_PATH, f'HRU{c_m_hru}_T_params.csv'))

            #transform to string, as the other code is a string
            P_params_hru['code'] = P_params_hru['code'].astype(str)
            T_params_hru['code'] = T_params_hru['code'].astype(str)

            #perform the quantile mapping for the control member for the HRU's without Eleavtion bands
            P_data, T_data = dg_utils.do_quantile_mapping(transformed_data_file, P_params_hru, T_params_hru, ensemble=False)
        else:
            P_data = transformed_data_file[['date', 'P', 'code']].copy()
            T_data = transformed_data_file[['date', 'T', 'code']].copy()

        #check if there are nan values
        if P_data.isnull().values.any():
            print(f"Nan values in P data for HRU {c_m_hru}")
            print("Take Last Observation")
            P_data = P_data.ffill()

        if T_data.isnull().values.any():
            print(f"Nan values in T data for HRU {c_m_hru}")
            print("Take Last Observation")
            T_data = T_data.ffill()

        P_data.to_csv(os.path.join( OUTPUT_PATH_REANALYSIS, f"{c_m_hru}_P_reanalysis.csv"), index=False)
        T_data.to_csv(os.path.join( OUTPUT_PATH_REANALYSIS, f"{c_m_hru}_T_reanalysis.csv"), index=False)

        #clear memory
        del transformed_data_file

if __name__ == '__main__':
    main()