# Python 3.11
# Script to produce hydrological forecasts using linear regression.
# The script is run daily and produces forecasts for the next 5 or 10 days.
# The script is run with the following command:
# python linear_regression.py

# I/O
import os
import sys
import logging
from logging.handlers import TimedRotatingFileHandler

import pandas as pd
import datetime as dt

# SDK library for accessing the DB, installed with
# pip install git+https://github.com/hydrosolutions/ieasyhydro-python-sdk
from ieasyhydro_sdk.sdk import IEasyHydroSDK, IEasyHydroHFSDK

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
logging.basicConfig(level=logging.DEBUG)
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
    # Test if we read from iEasyHydro or iEasyHydro HF
    if os.getenv('ieasyhydroforecast_connect_to_iEH') == 'True':
        ieh_sdk = IEasyHydroSDK()
        has_access_to_db = sl.check_database_access(ieh_sdk)
        has_access_to_hf_db = False
        if not has_access_to_db:
            ieh_sdk = None
    else:
        # During development of iEH HF, we get static data from iEH HF and
        # operational data from iEH.
        # TODO: Remove this when iEH HF is operational.
        ieh_sdk = IEasyHydroSDK()
        ieh_hf_sdk = IEasyHydroHFSDK()
        has_access_to_db = sl.check_database_access(ieh_sdk)
        has_access_to_hf_db = sl.check_database_access(ieh_hf_sdk)
        if not has_access_to_db:
            ieh_sdk = None
        if not has_access_to_hf_db:
            ieh_hf_sdk = None

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
    if has_access_to_hf_db:
        # Use the iEH HF SDK to get the sites
        fc_sites_pentad, site_list_pentad = sl.get_pentadal_forecast_sites_from_HF_SDK(ieh_hf_sdk)
        fc_sites_decad, site_list_decad = sl.get_decadal_forecast_sites_from_pentadal_sites(fc_sites_pentad, site_list_pentad)
        print("DEBUG site_list_pentad\n", site_list_pentad)
        print("DEBUG site_list_decad\n", site_list_decad)
    else:
        # Use the iEH SDK to get the sites
        fc_sites_pentad, site_list_pentad = sl.get_pentadal_forecast_sites(ieh_sdk, has_access_to_db)
        fc_sites_decad, site_list_decad = sl.get_decadal_forecast_sites_from_pentadal_sites(fc_sites_pentad, site_list_pentad)

    # Get pentadal and decadal data for forecasting. This is currently done for
    # pentad as well as for decad forecasts, function overwrites forecast_flags.
    data_pentad, data_decad = fl.get_pentadal_and_decadal_data(
        forecast_flags=forecast_flags,
        site_list_pentad=site_list_pentad,
        site_list_decad=site_list_decad)

    logger.info(f"Tail of data pentad: {data_pentad.tail()}")
    if forecast_flags.decad:
        logger.info(f"Tail of data decad: {data_decad.tail()}")

    # Save pentadal data
    print("DEBUG data_pentad\n", data_pentad.tail(10))
    fl.write_pentad_hydrograph_data(data_pentad)
    fl.write_pentad_time_series_data(data_pentad)

    # Save decadal data
    if forecast_flags.decad:
        fl.write_decad_hydrograph_data(data_decad)
        fl.write_decad_time_series_data(data_decad)

    # Iterate over the dates
    current_day = forecast_date
    while current_day <= date_end:

        logger.info(f"\n\n------ Forecast on {current_day} --------------------")

        # Update the last run_date in the database
        current_date, date_end, bulletin_date = sl.define_run_dates()
        # Make sure we have a valid forecast date
        if not forecast_date:
            exit()
        # Update the forecast flags
        forecast_flags = sl.ForecastFlags.from_forecast_date_get_flags(current_date)
        logger.debug(f"Forecast flags: {forecast_flags}")

        # Test if today is a forecast day for either pentadal and decadal forecasts
        # We only run through the rest of the code in the loop if current_date is a forecast day
        if forecast_flags.pentad:
            logger.info(f"Starting pentadal forecast for {current_day}. End date: {date_end}. Bulletin date: {bulletin_date}.")
            #logger.debug(f'data_pentad.head(): \n{data_pentad.head()}')
            #logger.debug(f'data_pentad.tail(): \n{data_pentad.tail()}')

            # Filter the discharge data for the sites we need to produce forecasts
            discharge_pentad = fl.filter_discharge_data_for_code_and_date(
                df=data_pentad,
                filter_sites=site_list_pentad,
                filter_date=current_day,
                code_col='code',
                date_col='date')
            # Print the tail of discharge_pentad for code 16936
            logger.debug(f"discharge_pentad.head(): \n{discharge_pentad.head()}")
            logger.debug(f"discharge_pentad.tail(): \n{discharge_pentad.tail()}")

            # Print discharge_data for code == '15194' for april and may 2024
            #logger.info(f"discharge_pentad[discharge_pentad['code'] == '15194'].tail(50): \n{discharge_pentad[discharge_pentad['code'] == '15194'].tail(50)}")

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
            forecast_pentad_of_year = tl.get_pentad_in_year(current_day)
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

            #logger.debug(f"linreg_pentad.head: {linreg_pentad.head()}")
            logger.debug(f"linreg_pentad.tail (linreg): {linreg_pentad.tail()}")

            # Generate the forecast for the current forecast horizon
            fl.perform_forecast(
                fc_sites_pentad,
                group_id=forecast_pentad_of_year,
                result_df=linreg_pentad,
                code_col='code',
                group_col='pentad_in_year')

            # Rename the column discharge_sum to predictor
            linreg_pentad.rename(
                columns={'discharge_sum': 'predictor',
                         'pentad': 'pentad_in_month'}, inplace=True)
            logger.debug(f"linreeg_pentad.tail (forecast): {linreg_pentad}")

            # Write output files for the current forecast horizon
            fl.write_linreg_pentad_forecast_data(linreg_pentad)

        else:
            logger.info(f'No pentadal forecast for {current_day}.')

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
            logger.debug(f"Predictor dates: {predictor_dates.decad}")
            fl.get_predictors(
                data_df=discharge_decad,
                start_date=max(predictor_dates.decad),
                fc_sites=fc_sites_decad,
                date_col='date',
                code_col='code',
                predictor_col='predictor')

            # Calculate norm discharge for the decad forecast
            forecast_decad_of_year = tl.get_decad_in_year(current_day)
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

            #logger.debug(f"Linear regression for decad: {linreg_decad.head()}")

            # Generate the forecast for the current forecast horizon
            fl.perform_forecast(
                fc_sites_decad,
                group_id=forecast_decad_of_year,
                result_df=linreg_decad,
                code_col='code',
                group_col='decad_in_year')

            # Write output files for the current forecast horizon
            fl.write_linreg_decad_forecast_data(linreg_decad)

        else:
            logger.info(f'No decadal forecast for {current_day}.')

        # Store the last run date
        ret = sl.store_last_successful_run_date(current_day)

        # Move to the next day
        logger.info(f"Iteration for {current_day} completed successfully.")
        current_day += dt.timedelta(days=1)

    if ret is None:
        sys.exit(0) # Success
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()