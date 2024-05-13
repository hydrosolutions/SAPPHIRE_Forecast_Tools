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
import forecast_library as fl

# Local methods
#from src import src


# Configure the logging level and formatter
logging.basicConfig(level=logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Create the logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Create a file handler to write logs to a file
file_handler = TimedRotatingFileHandler('logs/log', when='midnight',
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

    logger.info("\n\n====== LINEAR REGRESSION =========================")

    # Configuration
    sl.load_environment()

    # Set up the iEasyHydro SDK
    ieh_sdk = IEasyHydroSDK()
    has_access_to_db = sl.check_database_access(ieh_sdk)
    if not has_access_to_db:
        ieh_sdk = None

    # Get start and end dates for current call to the script.
    # forecast_date: date for which the forecast is being run (typically today)
    # date_end: last date for which the forecast is being run (typically today)
    # bulletin_date: first date for which the forecast is valid (typically tomorrow)
    forecast_date, date_end, bulletin_date = sl.define_run_dates()

    # Get forecast flags (identify which forecasts to run based on the forecast date)
    forecast_flags = sl.ForecastFlags.from_forecast_date_get_flags(forecast_date)
    logging.debug(f"Forecast flags: {forecast_flags}")

    # Identify sites for which to produce forecasts
    # Gets us a list of site objects with the necessary information to write forecast outputs
    fc_sites = sl.get_pentadal_forecast_sites(ieh_sdk, has_access_to_db)

    # Read discharge data and filter for sites required to produce forecasts
    discharge_all = fl.read_daily_discharge_data_from_csv()

    # Iterate over the dates
    current_day = forecast_date
    while current_day <= date_end:

        logger.info(f"\n\n====== Forecast on {current_day} =========================")

        # Only run in hindcast mode
        if current_day < date_end:
            # Update the last run_date in the database
            current_date, date_end, bulletin_date = sl.define_run_dates()
            # Update the forecast flags
            forecast_flags = sl.ForecastFlags.from_forecast_date_get_flags(current_date)
            logging.debug(f"Forecast flags: {forecast_flags}")

        # Test if today is a forecast day for either pentadal and decadal forecasts
        # We only run through the rest of the code in the loop if current_date is a forecast day
        #if forecast_flags.pentad or forecast_flags.decad:
        #    logger.debug(f"Starting forecast for {current_day}. End date: {date_end}. Bulletin date: {bulletin_date}.")
            # Perform linear regression for the current forecast horizon
            # TODO

            # Generate the forecast for the current forecast horizon
            # TODO

            # Write output files for the current forecast horizon
            # TODO

        # Store the last run date
        sl.store_last_successful_run_date(current_day)

        # Move to the next day
        current_day += dt.timedelta(days=1)
        logger.debug(f"Forecast for {current_day} completed successfully.")



if __name__ == "__main__":
    main()