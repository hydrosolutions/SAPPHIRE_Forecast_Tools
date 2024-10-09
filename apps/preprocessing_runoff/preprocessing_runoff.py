# Python 3.11

# This script is part of the SAPPHIRE forecast tools.
# It reads daily average discharge data from excel sheets and, if access to the
# iEasyHydro database is available, completes the discharge time series with
# operational daily average discharge from the database. From the current day,
# the morning discharge is also read from the database and appended to the time
# series.
#
# How to run:
# from apps/preprocessing_runoff call
# SAPPHIRE_OPDEV_ENV=True python preprocessing_runoff.py
#
# Details:
# The script performs the following steps:
# - Read daily data plus todays morning discharge into a dataframe
# - Calculate runoff for virtual stations
# - Filter for outliers by setting values exceeding the thresholds below to NaN,
#      whereby lower threshold: 25%ile – 2.5 * (75%ile – 25%ile)
#      and upper threshold: 75%ile + (75%ile – 25%ile)
# - Linearly interpolate NaNs, maximum gap size is 2
# - Calculate runoff statistics
# The script then saves the discharge data and runoff statistics (hydrograph
# data) to csv files for further use in the forecast tools.
#
# Important assumption:
# This module assumes that station codes can be converted to integers with 5
# digits.
#
# Developed within the frame of the SAPPHIRE Central Asia project funded by SDC.

# I/O
import os
import sys
import logging
from logging.handlers import TimedRotatingFileHandler

# SDK library for accessing the DB, installed with
# pip install git+https://github.com/hydrosolutions/ieasyhydro-python-sdk
from ieasyhydro_sdk.sdk import IEasyHydroSDK, IEasyHydroHFSDK

# Local methods
from src import src

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


def main():
    """
    Pre-processing of discharge data for the SAPPHIRE forecast tools.

    This script reads daily average discharge data from excel sheets and, if
    access to the iEasyHydro database is available, completes the discharge
    time series with operational daily average discharge from the database.
    From the current day, the morning discharge is also read from the database
    and appended to the time series.

    The script then saves the discharge data to a csv file for use in the
    forecast tools.

    The names of the columns to be written to the csv file are: 'code', 'date',
    and 'discharge'.
    """

    ## Configuration
    # Loads the environment variables from the .env file
    sl.load_environment()

    # Set up the iEasyHydro SDK
    ieh_sdk = IEasyHydroSDK()
    has_access_to_db = sl.check_database_access(ieh_sdk)
    if not has_access_to_db:
        ieh_sdk = None

    ## Data processing
    # Reading data from various sources
    runoff_data = src.get_runoff_data(
        ieh_sdk,
        date_col='date',
        discharge_col='discharge',
        name_col='name',
        code_col='code')

    # Test if there is any data
    if runoff_data is None:
        logger.error(f"No river runoff data found.\n"
                     f"No forecasts can be produced.\n"
                     f"Please check your configuration.\n")
        sys.exit(1)

    # Filtering for outliers
    filtered_data = src.filter_roughly_for_outliers(
        runoff_data, 'code', 'discharge')

    # Reformat to hydrograph data
    # In the future, daily norm discharge may be read from the iEH database. But
    # so far this is not possible.
    hydrograph = src.from_daily_time_series_to_hydrograph(
        data_df=filtered_data,
        date_col='date',
        discharge_col='discharge',
        code_col='code')

    # Get dangerous discharge values from iEasyHydro DB
    if ieh_sdk is not None:
        hydrograph = src.add_dangerous_discharge(
            ieh_sdk,
            hydrograph,
            code_col='code')

    ## Save the data
    # Daily time series data
    ret = src.write_daily_time_series_data_to_csv(
        data=filtered_data,
        column_list=['code', 'date', 'discharge'])

    # Daily hydrograph data
    ret = src.write_daily_hydrograph_data_to_csv(
        data=hydrograph,
        column_list=hydrograph.columns.tolist())

    # Update configuration files for selected sites (only if iEH HF is used)
    # Test if we read from iEasyHydro or iEasyHydro HF
    if os.getenv('ieasyhydroforecast_connect_to_iEH') == 'True':
        # Get information from iEH (no update of configuration files)
        # Do nothing and exit the script directly
        pass
    else:  # Get information from iEH HF
        ieh_hf_sdk = IEasyHydroHFSDK()
        sl.get_pentadal_forecast_sites_from_HF_SDK(ieh_hf_sdk)
        sl.get_decadal_forecast_sites_from_HF_SDK(ieh_hf_sdk)


    if ret is None:
        sys.exit(0) # Success
    else:
        sys.exit(1) # Failure

if __name__ == "__main__":
    main()