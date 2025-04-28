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
# ieasyhydroforecast_env_file_path=<absolute/path/to/.env> python preprocessing_runoff.py
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
# Beatrice Marti, hydrosolutions, 2025

# I/O
import os
import sys
import time
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
    overall_start_time = time.time()
    sl.load_environment()
    end_time = time.time()
    time_load_environment = end_time - overall_start_time

    # Get time zone of organization for which the Forecast Tools are deployed
    target_time_zone = sl.get_local_timezone_from_env()
    logger.info(f"Target time zone: {target_time_zone}")

    # Update configuration files for selected sites (only if iEH HF is used)
    # Test if we read from iEasyHydro or iEasyHydro HF for getting operational data
    start_time = time.time()
    # As temporary fix to preprocessing_runoff.py, we use the old iEasyHydro SDK
    # Deprecating the old iEasyHydro SDK
    ieh_sdk = IEasyHydroSDK()
    has_access_to_db = sl.check_database_access(ieh_sdk)
    if not has_access_to_db:
        ieh_sdk = None
    # Get site information from iEH (no update of configuration files)
    #fc_sites_pentad, site_codes_pentad = sl.get_pentadal_forecast_sites(ieh_sdk, backend_has_access_to_db=has_access_to_db)
    #fc_sites_decad, site_codes_decad = sl.get_decadal_forecast_sites_from_pentadal_sites(
    #    fc_sites_pentad, site_codes_pentad)
    if os.getenv('ieasyhydroforecast_connect_to_iEH') == 'True':
        logger.warning("Reading from the old iEasyHydro is no longer supported.")
        logger.warning("Please use the iEasyHydro HF SDK instead.")
        logger.warning("The script will exit now.")
        sys.exit(1)
        # Deprecating the old iEasyHydro SDK
        #ieh_sdk = IEasyHydroSDK()
        #has_access_to_db = sl.check_database_access(ieh_sdk)
        #if not has_access_to_db:
        #    ieh_sdk = None
        # Get site information from iEH (no update of configuration files)
        # Do nothing and exit the script directly
        #fc_sites_pentad, site_codes_pentad = sl.get_pentadal_forecast_sites(ieh_sdk, backend_has_access_to_db=has_access_to_db)
        #fc_sites_decad, site_codes_decad = sl.get_decadal_forecast_sites_from_pentadal_sites(
        #    fc_sites_pentad, site_codes_pentad)
        #pass
    else:  # Get information from iEH HF, default behaviour
        try: 
            ieh_hf_sdk = IEasyHydroHFSDK()
            has_access_to_db = sl.check_database_access(ieh_hf_sdk)
            if not has_access_to_db:
                ieh_hf_sdk = None
            fc_sites_pentad, site_codes_pentad, site_ids_pentad = sl.get_pentadal_forecast_sites_from_HF_SDK(ieh_hf_sdk)
            fc_sites_decad, site_codes_decad, site_ids_decad = sl.get_decadal_forecast_sites_from_HF_SDK(ieh_hf_sdk)
        except Exception as e:
            logger.error(f"Error while accessing iEasyHydro HF SDK: {e}")
            sys.exit(1)

    # Concatenate the two lists
    fc_sites = fc_sites_pentad + fc_sites_decad
    site_codes = site_codes_pentad + site_codes_decad
    site_ids = site_ids_pentad + site_ids_decad
    end_time = time.time()
    time_get_forecast_sites = end_time - start_time

    ## Data processing
    # Reading data from various sources
    start_time = time.time()
    if os.getenv('ieasyhydroforecast_connect_to_iEH') == 'True':
        # Deprecated
        
        pass
    else: 
        runoff_data = src.get_runoff_data_for_sites_HF(
                ieh_hf_sdk,
                date_col='date',
                discharge_col='discharge',
                code_col='code',
                site_list=fc_sites,
                code_list=site_codes, 
                id_list=site_ids,
                target_timezone=target_time_zone,
        )
    print(runoff_data.head(5))
      
    end_time = time.time()
    time_get_runoff_data = end_time - start_time

    # Test if there is any data
    if runoff_data is None:
        logger.error(f"No river runoff data found.\n"
                     f"No forecasts can be produced.\n"
                     f"Please check your configuration.\n")
        sys.exit(1)

    # Filtering for outliers
    start_time = time.time()
    filtered_data = src.filter_roughly_for_outliers(
        runoff_data, 'code', 'discharge')
    end_time = time.time()
    time_filter_roughly_for_outliers = end_time - start_time

    # Reformat to hydrograph data
    # In the future, daily norm discharge may be read from the iEH database. But
    # so far this is not possible.
    start_time = time.time()
    hydrograph = src.from_daily_time_series_to_hydrograph(
        data_df=filtered_data,
        date_col='date',
        discharge_col='discharge',
        code_col='code')
    src.inspect_site_data(hydrograph, '15189')
    end_time = time.time()
    time_from_daily_time_series_to_hydrograph = end_time - start_time

    # Get dangerous discharge values from iEasyHydro DB
    # Only required for iEasyHydro, not for iEasyHydro HF
    if os.getenv('ieasyhydroforecast_connect_to_iEH') == 'True':
        start_time = time.time()
        if ieh_hf_sdk is not None:
            hydrograph = src.add_dangerous_discharge_from_sites(
                hydrograph,
                code_col='code',
                site_list=fc_sites,
                site_code_list=site_codes)
            #hydrograph = src.add_dangerous_discharge(
            #    ieh_sdk,
            #    hydrograph,
            #    code_col='code')
        end_time = time.time()
        time_add_dangerous_discharge = end_time - start_time

    # Debug print: 5 rows with latest date for site 15189
    print(hydrograph[hydrograph['code'] == 15189].tail(5))

    ## Save the data
    # Daily time series data
    start_time = time.time()
    ret = src.write_daily_time_series_data_to_csv(
        data=filtered_data,
        column_list=['code', 'date', 'discharge'])
    end_time = time.time()
    time_write_daily_time_series_data = end_time - start_time

    # Daily hydrograph data
    start_time = time.time()
    ret = src.write_daily_hydrograph_data_to_csv(
        data=hydrograph,
        column_list=hydrograph.columns.tolist())
    end_time = time.time()
    time_write_daily_hydrograph_data = end_time - start_time

    overall_end_time = time.time()
    print("\n")
    logger.info(f"Overall time: {overall_end_time - overall_start_time:.2f} seconds")
    logger.info(f"Time to load environment: {time_load_environment:.2f} seconds")
    logger.info(f"Time to get forecast sites: {time_get_forecast_sites:.2f} seconds")
    logger.info(f"Time to get runoff data: {time_get_runoff_data:.2f} seconds")
    logger.info(f"Time to filter roughly for outliers: {time_filter_roughly_for_outliers:.2f} seconds")
    logger.info(f"Time to reformat to hydrograph data: {time_from_daily_time_series_to_hydrograph:.2f} seconds")
    #logger.info(f"Time to add dangerous discharge: {time_add_dangerous_discharge:.2f} seconds")
    logger.info(f"Time to write daily time series data: {time_write_daily_time_series_data:.2f} seconds")
    logger.info(f"Time to write daily hydrograph data: {time_write_daily_hydrograph_data:.2f} seconds")

    if ret is None:
        sys.exit(0) # Success
    else:
        sys.exit(1) # Failure

if __name__ == "__main__":
    main()