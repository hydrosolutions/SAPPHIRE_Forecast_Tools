# Python 3.11

# I/O
import os
import sys
import logging
from logging.handlers import TimedRotatingFileHandler

import datetime as dt

# SDK library for accessing the DB, installed with
# pip install git+https://github.com/hydrosolutions/ieasyhydro-python-sdk
from ieasyhydro_sdk.sdk import IEasyHydroSDK

# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the setup_library module from the iEasyHydroForecast package
import setup_library as sl

# Local methods
from src import config


# Configure the logging level and formatter
logging.basicConfig(level=logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Create the logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Create a file handler to write logs to a file
file_handler = TimedRotatingFileHandler('logs/logs.log', when='midnight',
                                        interval=1, backupCount=30)
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
    # Configuration
    sl.load_environment()

    # Set up the iEasyHydro SDK
    ieh_sdk = IEasyHydroSDK()
    has_access_to_db = sl.check_database_access(ieh_sdk)
    if not has_access_to_db:
        ieh_sdk = None

    # Get start and end dates for current call to the script
    forecast_date, date_end, bulletin_date = sl.define_run_dates()

    # Get forecast flags
    forecast_flags = sl.ForecastFlags.from_forecast_date_get_flags(forecast_date)

    # Iterate over the dates
    current_day = forecast_date
    while current_day <= date_end:
        # Call the forecast script with the current date as a command-line argument
        sl.run_forecast_script(current_day, ieh_sdk, has_access_to_db)

        # Move to the next day
        current_day += dt.timedelta(days=1)



if __name__ == "__main__":
    main()