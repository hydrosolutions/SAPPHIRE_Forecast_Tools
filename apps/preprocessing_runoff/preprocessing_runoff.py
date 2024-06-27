# Python 3.11

# I/O
import os
import sys
import logging
from logging.handlers import TimedRotatingFileHandler

# SDK library for accessing the DB, installed with
# pip install git+https://github.com/hydrosolutions/ieasyhydro-python-sdk
from ieasyhydro_sdk.sdk import IEasyHydroSDK

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

    # Filtering for outliers
    filtered_data = src.filter_roughly_for_outliers(
        runoff_data, 'code', 'discharge')

    # Reformat to hydrograph data
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

    if ret is None:
        sys.exit(0) # Success
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()