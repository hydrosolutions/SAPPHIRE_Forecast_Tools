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
print(script_dir)
print(forecast_dir)

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
    Pre-processing of weather station data for the SAPPHIRE forecast tools.

    This script reads decadal precipitation sums and decadal temperature
    averages from excel sheets (if such are available) and, if access to the
    iEasyHydro database is available, completes the time series with operational
    decadal data from the database.

    The script then saves the decadal weather data to a csv file for use in the
    forecast tools.

    The names of the columns to be written to the csv file are: 'code', 'date',
    'variable', 'value', 'unit'.
    """

    ## Configuration
    # Loads the environment variables from the .env file
    sl.load_environment()

    # Set up the iEasyHydro SDK
    ieh_sdk = IEasyHydroSDK()
    has_access_to_db = sl.check_database_access(ieh_sdk)
    if not has_access_to_db:
        ieh_sdk = None
        # Log the error and exit as long as we don't have data to read from xlsx
        logger.error("No access to the iEasyHydro database.")
        sys.exit(1)

    ## Data processing
    weather_data = src.get_weather_station_data(
        ieh_sdk)

    # Filtering for outliers
    filtered_data = src.filter_roughly_for_outliers(
        weather_data, group_by=['code', 'variable'], filter_col='value',
        date_col='date')

    # Get norm P (currently not in iEasyHydro)
    # TODO To be implemented once the upload of norm P and T data is available in iEasyHydro HF
    # For now, we calculate the precipitation norm data from the decadal data in src.preprocess_station_meteo_data

    # Aggregate decadal data to monthly data
    monthly_data = src.aggregate_decadal_to_monthly(filtered_data)

    # Calculate percentage of last years data
    # TODO Also calculate percentage of norm data once it becomes available through iEasyHydro HF
    # TODO Reformat data to calculate percentage of last years data
    decadal_data_for_current_year = src.preprocess_station_meteo_data(filtered_data)
    monthly_data_for_current_year = src.preprocess_station_meteo_data(monthly_data)

    ## Kriging
    # TODO Here we can put the Kriging code
    # TODO Any other data processing that also needs to be done

    ## Save the data
    # monthly weather time series
    ret = src.write_monthly_station_data_to_csv(monthly_data_for_current_year)

    # any other data that might need saving at some later stage

    # Raster data to be visualized in the dashboard
    # TODO: Here we can write the results from Kriging

    if ret is None:
        sys.exit(0) # Success
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()