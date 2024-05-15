# Python 3.11

# I/O
import os
import sys
import logging
from logging.handlers import TimedRotatingFileHandler

import pandas as pd
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
import tag_library as tl

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

    logger.info(f"\n\n====== LINEAR REGRESSION =========================")
    logger.debug(f"Script started at {dt.datetime.now()}.")
    logger.info(f"\n\n------ Setting up --------------------------------")

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

    # Only perform the next steps if we have to produce a forecast.
    if not forecast_date:
        exit()

    # Get forecast flags (identify which forecasts to run based on the forecast date)
    forecast_flags = sl.ForecastFlags.from_forecast_date_get_flags(forecast_date)
    logger.debug(f"Forecast flags: {forecast_flags}")

    # Identify sites for which to produce forecasts
    # Gets us a list of site objects with the necessary information to write forecast outputs
    fc_sites_pentad, site_list_pentad = sl.get_pentadal_forecast_sites(ieh_sdk, has_access_to_db)
    fc_sites_decad, site_list_decad = sl.get_decadal_forecast_sites_from_pentadal_sites(fc_sites_pentad, site_list_pentad)

    # Get pentadal and decadal data for forecasting
    data_pentad, data_decad = fl.get_pentadal_and_decadal_data(
        forecast_flags=forecast_flags,
        site_list_pentad=site_list_pentad,
        site_list_decad=site_list_decad)

    logger.debug(f"Type of data_pentad: {type(data_pentad)}")
    logger.debug(f"Type of data_decad: {type(data_decad)}")
    logger.debug(f"Tail of data pentad: {data_pentad[data_pentad['code']=='16936'].tail()}")
    if forecast_flags.decad:
        logger.debug(f"Head of data decad: {data_decad.head()}")


    # Iterate over the dates
    current_day = forecast_date
    while current_day <= date_end:

        logger.info(f"\n\n------ Forecast on {current_day} --------------------")

        # Only run in hindcast mode
        if current_day < date_end:
            # Update the last run_date in the database
            current_date, date_end, bulletin_date = sl.define_run_dates()
            # Update the forecast flags
            forecast_flags = sl.ForecastFlags.from_forecast_date_get_flags(current_date)
            logger.debug(f"Forecast flags: {forecast_flags}")

        # Test if today is a forecast day for either pentadal and decadal forecasts
        # We only run through the rest of the code in the loop if current_date is a forecast day
        if forecast_flags.pentad:
            logger.info(f"Starting pentadal forecast for {current_day}. End date: {date_end}. Bulletin date: {bulletin_date}.")

            # Filter the discharge data for the sites we need to produce forecasts
            discharge_pentad = fl.filter_discharge_data_for_code_and_date(
                df=data_pentad,
                filter_sites=site_list_pentad,
                filter_date=current_day,
                code_col='code',
                date_col='date')
            # Print the tail of discharge_pentad for code 16936
            #logger.debug(f"Tail of discharge_pentad for code 16936: {discharge_pentad[discharge_pentad['code'] == '16936'].tail()}")

            # Write the predictor to the Site objects
            predictor_dates = fl.get_predictor_dates(current_day, forecast_flags)
            logger.debug(f"Predictor dates: {predictor_dates.pentad}")
            fl.get_predictors(
                data_df=discharge_pentad,
                start_date=max(predictor_dates.pentad),
                fc_sites=fc_sites_pentad,
                date_col='date',
                code_col='code',
                predictor_col='discharge_sum')

            # Calculate norm discharge for the pentad forecast
            forecast_pentad_of_year = tl.get_pentad_in_year(bulletin_date)
            fl.save_discharge_avg(
                discharge_pentad, fc_sites_pentad, group_id=forecast_pentad_of_year,
                code_col='code', group_col='pentad_in_year', value_col='discharge_avg')

            # Perform linear regression for the current forecast horizon
            # The linear regression is performed on past data. Here, the slope and
            # intercept of the linear regression model are calculated for each site for
            # the current forecast.
            linreg_pentad = fl.perform_linear_regression(
                data_df=discharge_pentad,
                station_col='code',
                pentad_col='pentad_in_year',
                predictor_col='discharge_sum',
                discharge_avg_col='discharge_avg',
                forecast_pentad=int(forecast_pentad_of_year))

            logger.debug(f"Linear regression for pentad: {linreg_pentad.head()}")

            # Generate the forecast for the current forecast horizon
            fl.perform_forecast(
                fc_sites_pentad,
                group_id=forecast_pentad_of_year,
                result_df=linreg_pentad,
                code_col='code',
                group_col='pentad_in_year')

            # Rename the column discharge_sum to predictor
            linreg_pentad.rename(columns={'discharge_sum': 'predictor'}, inplace=True)

            # Write output files for the current forecast horizon
            fl.write_pentad_forecast_data(
                linreg_pentad[['date', 'issue_date', 'pentad', 'pentad_in_year', 'code',
                               'predictor', 'discharge_avg', 'slope',
                               'intercept', 'forecasted_discharge']])

        if forecast_flags.decad:
            logger.info(f"Starting decadal forecast for {current_day}. End date: {date_end}. Bulletin date: {bulletin_date}.")

            # Filter the discharge data for the sites we need to produce forecasts
            discharge_decad = fl.filter_discharge_data_for_code_and_date(
                df=data_decad,
                filter_sites=site_list_decad,
                filter_date=current_day,
                code_col='code',
                date_col='date')

            # Write the predictor to the Site objects
            logger.info(f"Predictor dates: {predictor_dates.decad}")
            fl.get_predictors(
                data_df=discharge_decad,
                start_date=max(predictor_dates.decad),
                fc_sites=fc_sites_decad,
                date_col='date',
                code_col='code',
                predictor_col='predictor')

            # Calculate norm discharge for the decad forecast
            forecast_decad_of_year = tl.get_decad_in_year(bulletin_date)
            fl.save_discharge_avg(
                discharge_decad, fc_sites_decad, group_id=forecast_decad_of_year,
                code_col='code', group_col='decad_in_year', value_col='discharge_avg')


            # Perform linear regression for the current forecast horizon
            linreg_decad = fl.perform_linear_regression(
                data_df=discharge_decad,
                station_col='code',
                pentad_col='decad_in_year',
                predictor_col='predictor',
                discharge_avg_col='discharge_avg',
                forecast_pentad=int(forecast_decad_of_year))

            logger.debug(f"Linear regression for decad: {linreg_decad.head()}")

            # Generate the forecast for the current forecast horizon
            fl.perform_forecast(
                fc_sites_decad,
                group_id=forecast_decad_of_year,
                result_df=linreg_decad,
                code_col='code',
                group_col='decad_in_year')

            # Write output files for the current forecast horizon
            fl.write_decad_forecast_data(linreg_decad)

        # Store the last run date
        sl.store_last_successful_run_date(current_day)

        # Move to the next day
        current_day += dt.timedelta(days=1)
        logger.info(f"Forecast for {current_day} completed successfully.")



if __name__ == "__main__":
    main()