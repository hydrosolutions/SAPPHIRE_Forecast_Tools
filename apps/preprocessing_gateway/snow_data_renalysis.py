#USAGE: SAPPHIRE_OPDEV_ENV=True python snow_data_renalysis.py


# --------------------------------------------------------------------
# Snow Data Reanalysis
# This script downloads snow data from the Sapphire Data Gateway 
# for the defined HRU's and variables.
# The data is then transformed and saved to a csv file.
# The data is downloaded from 2000-01-01 to the current date - 180 days.
# The script processes the download in 5 year batches.

# --------------------------------------------------------------------
# Import Libraries
# --------------------------------------------------------------------
import os
import sys
import json
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging
from logging.handlers import TimedRotatingFileHandler
import traceback

# Custom Script for Data Gateway
import dg_utils

# Note that the sapphire data gateway client is currently a private repository
# Access to the repository is required to install the package
# Further, access to the data gateway through an API key is required to use the
# client. The API key is stored in a .env file in the root directory of the project.
# The forecast tools can be used without access to the sapphire data gateay but
# the full power of the tools is only available with access to the data gateway.
#pip install git+https://github.com/hydrosolutions/sapphire-dg-client.git
import sapphire_dg_client
from sapphire_dg_client import SapphireDGClient, snow_model

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
logging.basicConfig(level=logging.INFO)
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



def get_snow_data_reanalysis(client, 
                  hru, 
                  variable,
                  start_date, 
                  end_date,
                  dg_path, 
                  save_path):
    """
    Get the snow data for a given HRU and variable from the Sapphire Data Gateway.
    The snow data is then transformed in a file with following format:
    |date|variable|code|name|
    Variables can be SWE, HS, RoF
    The code is the unique basin identifier, and the name can be code_numhru,
    for shapefiles with different elevation bands.

    This file will then be saved in the path specified by save_path/variable/HRU_variable.csv
    There might be already a file with the same name, read it and append  the new file to the old file
    remove duplicates and sort the file by date and code.
    """
    file_path = os.path.join(save_path, variable, f"{hru}_{variable}.csv")

    # Check if the file already exists
    if os.path.exists(file_path):
        try:
            old_dataframe = pd.read_csv(file_path)
            old_dataframe['date'] = pd.to_datetime(old_dataframe['date'])
            old_dataframe = old_dataframe.sort_values(by=['date', 'code'])
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            return False
    else:
        old_dataframe = pd.DataFrame()

    try:
        outpath = client.get_snow_reanalysis(
                hru_code=hru, date=start_date, end_date=end_date,
                  parameter=variable, 
                directory = dg_path)
    except Exception as e:
        logger.error(f"Error getting snow data for {hru} {variable}: {e}")
        return False
    
    # Read the data from the file
    try:
        df = pd.read_csv(outpath)
    except Exception as e:
        logger.error(f"Error reading file {outpath}: {e}")
        return False
    
    # Transform the data
    df_transformed = dg_utils.transform_snow_data(df, variable)

    logger.debug(f"Head of transformed data:\n{df_transformed.head()}")

    # Sort the data by date and code
    df_transformed = df_transformed.sort_values(by=['date', 'code'])

    df_combined = pd.concat([old_dataframe, df_transformed], ignore_index=True)
    # Remove duplicates and keep the last value
    df_combined = df_combined.drop_duplicates(subset=['date', 'code'], keep='last')

    # Sort the data by date and code
    df_combined = df_combined.sort_values(by=['date', 'code'])

    #round data to 2 decimal places
    df_combined = df_combined.round(2)

    # Save the data to the file
    try:
        df_combined.to_csv(file_path, index=False)
    except Exception as e:
        logger.error(f"Error saving file {file_path}: {e}")
        return False

    return True




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

    API_HOST = os.getenv('SAPPHIRE_DG_HOST')

    intermediate_data_path = os.getenv('ieasyforecast_intermediate_data_path')
    #output_path for the data from the data gateaway
    OUTPUT_PATH_DG = os.path.join(
        intermediate_data_path,
        os.getenv('ieasyhydroforecast_OUTPUT_PATH_DG'))
    # Test if the output path exists and create it if it doesn't
    if not os.path.exists(OUTPUT_PATH_DG):
        os.makedirs(OUTPUT_PATH_DG, exist_ok=True)

    snow_data_path = os.getenv('ieasyhydroforecast_OUTPUT_PATH_SNOW')
    #OUTPUT_PATH for snow data
    OUTPUT_PATH_SNOW = os.path.join(
        intermediate_data_path,
        snow_data_path)
    # Test if the output path exists and create it if it doesn't
    if not os.path.exists(OUTPUT_PATH_SNOW):
        os.makedirs(OUTPUT_PATH_SNOW, exist_ok=True)
    
    # Get the HRUs for the snow data
    SNOW_HRUS = os.getenv('ieasyhydroforecast_HRU_SNOW_DATA')
    SNOW_HRUS = [str(x) for x in SNOW_HRUS.split(',')]

    # Get the snow vars
    SNOW_VARS = os.getenv('ieasyhydroforecast_SNOW_VARS')
    SNOW_VARS = [str(x) for x in SNOW_VARS.split(',')]

    logger.debug(f"Extracting snow data for HRUs: {SNOW_HRUS}")
    logger.debug(f"Extracting snow data for variables: {SNOW_VARS}")

    logger.debug(f"Output path of DG data: {OUTPUT_PATH_DG}")
    logger.debug(f"Output path of snow data: {OUTPUT_PATH_SNOW}")
    #iterate through the snow vars and check if a directory exists
    # if not, create it
    for snow_var in SNOW_VARS:
        snow_var_dir = os.path.join(OUTPUT_PATH_SNOW, snow_var)
        if not os.path.exists(snow_var_dir):
            os.makedirs(snow_var_dir, exist_ok=True)
            logger.debug(f"Creating directory for snow var: {snow_var_dir}")
        else:
            logger.debug(f"Directory for snow var already exists: {snow_var_dir}")

    client = snow_model.SapphireSnowModelClient(
        api_key=API_KEY,
        host = API_HOST)
    
    # today - 180 days
    start_date = "2000-01-01"
    start_date_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_dt = datetime.today() - timedelta(days=180)
    end_date = end_date_dt.strftime('%Y-%m-%d')
    
    logger.debug(f"Date for snow data from: {start_date} to {end_date}")

    # Define 5 year intervals between the start and end date
    date_intervals = []
    year_start = start_date_dt.year
    year_end = end_date_dt.year

    logger.debug(f"Batches of 5 years {date_intervals}")

    this_start = year_start
    while this_start <= year_end:
        this_end = this_start + 5
        if this_end > year_end:
            this_end = end_date
            date_intervals.append((f"{this_start}-01-01", this_end))
            break
        date_intervals.append((f"{this_start}-01-01", f"{this_end}-01-01"))
        this_start = this_end

    # Iterate through the HRUs and get the snow data
    for start_date, end_date in date_intervals:
        logger.debug(f"Batch {start_date} to {end_date}")
        for hru in SNOW_HRUS:
            for snow_var in SNOW_VARS:
                logger.debug(f"Getting snow data for HRU: {hru} and variable: {snow_var}")
                # Get the snow data
                success = get_snow_data_reanalysis(
                    client=client,
                    hru= hru, 
                    variable= snow_var,
                    start_date= start_date,
                    end_date= end_date,
                    dg_path= OUTPUT_PATH_DG,
                    save_path= OUTPUT_PATH_SNOW)
                if not success:
                    logger.error(f"Error getting snow data for HRU: {hru} and variable: {snow_var}")

    logger.info('Finished getting snow data')

    
if __name__ == "__main__":
    # Run the main function
    main()