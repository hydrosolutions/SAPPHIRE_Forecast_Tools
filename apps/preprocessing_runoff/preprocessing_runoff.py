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
import pandas as pd

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
console_handler.setLevel(logging.INFO)

# Get the root logger and add the handlers to it
logger = logging.getLogger()
logger.handlers = []
logger.addHandler(file_handler)
logger.addHandler(console_handler)
logger.info = print




def get_ieh_sdk():
    """
    Checks if an SSH tunnel is required and if it is running. Then checks if
    access to the iEasyHydro database is available. If so, it returns the
    iEasyHydro SDK object. If not, it returns None.

    Returns:
        IEasyHydroSDK or IEasyHydroHFSDK: The appropriate SDK object.
        bool: True if the backend has access to the database, False otherwise.
    """
    # Check environment to see if an ssh tunnel is required
    # If so, check if the tunnel is running
    needs_ssh_tunnel = sl.check_if_ssh_tunnel_is_required()
    if needs_ssh_tunnel:
        # Check if the SSH tunnel is running
        ssh_tunnel_running = sl.check_local_ssh_tunnels()
        if not ssh_tunnel_running:
            logger.error("SSH tunnel is not running. Please start the SSH tunnel.")
            sys.exit(1)
    # Check if we do have access to the database
    if os.getenv('ieasyhydroforecast_connect_to_iEH') == 'True':
        logger.info("Connecting to iEasyHydro SDK")
        ieh_sdk = IEasyHydroSDK()
        has_access_to_db = sl.check_database_access(ieh_sdk)
        if not has_access_to_db:
            ieh_sdk = None
        return ieh_sdk, has_access_to_db
    else:
        if os.getenv('ieasyhydroforecast_organization') == 'demo': 
            # Connection to the database is optional for demo organization
            try: 
                ieh_hf_sdk = IEasyHydroHFSDK()
                has_access_to_db = sl.check_database_access(ieh_hf_sdk)
                if not has_access_to_db:
                    ieh_hf_sdk = None
                return ieh_hf_sdk, has_access_to_db
            except Exception as e:
                logger.warning(f"Error while accessing iEasyHydro HF SDK: {e}")
                logger.warning("Continuing without database access.")
                ieh_hf_sdk = None
                has_access_to_db = False
                return ieh_hf_sdk, has_access_to_db
        else:
            logger.info("Connecting to iEasyHydro HF SDK")
            try:
                ieh_hf_sdk = IEasyHydroHFSDK()
                has_access_to_db = sl.check_database_access(ieh_hf_sdk)
                if not has_access_to_db:
                    ieh_hf_sdk = None
                return ieh_hf_sdk, has_access_to_db
            except Exception as e:
                logger.error(f"Error while accessing iEasyHydro HF SDK: {e}")
                sys.exit(1)


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
    # Get site information from iEH (no update of configuration files)
    if os.getenv('ieasyhydroforecast_connect_to_iEH') == 'True':
        logger.info("Reading forecast sites from iEasyHydro SDK")
        try: 
            # Get the iEasyHydro SDK object
            ieh_sdk, has_access_to_db = get_ieh_sdk()
            # Get site information from iEH (no update of configuration files)
            fc_sites_pentad, site_codes_pentad = sl.get_pentadal_forecast_sites(ieh_sdk, backend_has_access_to_db=has_access_to_db)
            fc_sites_decad, site_codes_decad = sl.get_decadal_forecast_sites_from_pentadal_sites(
                fc_sites_pentad, site_codes_pentad)
            logger.info(f"... done reading forecast sites from iEasyHydro SDK")
        except Exception as e:
            logger.error(f"Error while accessing iEasyHydro SDK: {e}")
            raise e
    else:  # Get information from iEH HF, default behaviour
        logger.info("Reading forecast sites from iEasyHydro HF SDK")
        try: 
            # Get the iEasyHydro HF SDK object
            ieh_hf_sdk, has_access_to_db = get_ieh_sdk()
            fc_sites_pentad, site_codes_pentad, site_ids_pentad = sl.get_pentadal_forecast_sites_from_HF_SDK(ieh_hf_sdk)
            fc_sites_decad, site_codes_decad, site_ids_decad = sl.get_decadal_forecast_sites_from_HF_SDK(ieh_hf_sdk)
            logger.info(f"... done reading forecast sites from iEasyHydro HF SDK")
        except Exception as e:
            logger.error(f"Error while accessing iEasyHydro HF SDK: {e}")
            raise e

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
        # Not recently used (or tested), but kept for reference
        # Todo: Work in standardization of date column format. 
        runoff_data = src.get_runoff_data_for_sites(
                ieh_sdk,
                date_col='date',
                discharge_col='discharge',
                name_col='name',
                code_col='code',
                site_list=fc_sites,
                code_list=site_codes,
        )
        # Non-HF SDK returns DataFrame directly; use empty DataFrame for new_data
        # (legacy mode - will skip API write since empty)
        new_data = pd.DataFrame()
        logger.info("Runoff data read from iEasyHydro SDK")

    else:
        runoff_result = src.get_runoff_data_for_sites_HF(
                ieh_hf_sdk,
                date_col='date',
                discharge_col='discharge',
                code_col='code',
                site_list=fc_sites,
                code_list=site_codes,
                id_list=site_ids,
                target_timezone=target_time_zone,
        )
        runoff_data = runoff_result.full_data
        new_data = runoff_result.new_data
        logger.info(f"Runoff data read from iEasyHydro HF SDK: {len(runoff_data)} total, {len(new_data)} new")
    print(f"head of runoff data:\n{runoff_data.head(5)}")
    print(f"tail of runoff data:\n{runoff_data.tail(5)}")
    print(f"columns of runoff data: {runoff_data.columns.tolist()}")
    print(f"types of columns in runoff data: {runoff_data.dtypes.to_dict()}")
    # Print whether or not the date column is in datetime format
    if 'date' in runoff_data.columns:
        if runoff_data['date'].dtype == 'datetime64[ns]':
            logger.info("Date column is in datetime format.")
        # Test if date is in pandas date format
        elif pd.api.types.is_datetime64_any_dtype(runoff_data['date']):
            logger.info("Date column is in pandas datetime format.")
        else:
            logger.warning("Date column is NOT in datetime format. "
                           "This may cause issues later on.")
    else: 
        logger.warning("Date column is missing in runoff data. "
                       "This will cause issues later on.")
        sys.exit(1)

    # Print the tail of runoff data for site 16059
    print(runoff_data[runoff_data['code'] == 16059].tail(10))
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
    # Also filter new_data for API writes (if it has data)
    if not new_data.empty:
        filtered_new_data = src.filter_roughly_for_outliers(
            new_data, 'code', 'discharge')
        logger.info(f"Filtered new data: {len(filtered_new_data)} records for API")
    else:
        filtered_new_data = new_data
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
    # A debug print to check the data
    #src.inspect_site_data(hydrograph, '16006')
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
    #print(hydrograph[hydrograph['code'] == 15189].tail(5))

    ## Save the data
    # Daily time series data
    start_time = time.time()
    ret = src.write_daily_time_series_data_to_csv(
        data=filtered_data,
        column_list=['code', 'date', 'discharge'],
        api_data=filtered_new_data)  # Only send new data to API
    if ret is None:
        logger.info("Daily time series data written successfully.")
    else:
        logger.error("Failed to write daily time series data.")
        sys.exit(1)
    end_time = time.time()
    time_write_daily_time_series_data = end_time - start_time

    # Daily hydrograph data
    start_time = time.time()
    # For API, only send today's hydrograph data (one row per station)
    # The full hydrograph (all 365/366 days) is written to CSV
    today = pd.Timestamp.now().normalize()
    today_str = today.strftime('%Y-%m-%d')
    # Filter hydrograph to today's date for API
    if 'date' in hydrograph.columns:
        hydrograph_api_data = hydrograph[
            pd.to_datetime(hydrograph['date']).dt.strftime('%Y-%m-%d') == today_str
        ].copy()
        logger.info(f"Filtered hydrograph for API: {len(hydrograph_api_data)} rows for {today_str}")
    else:
        hydrograph_api_data = pd.DataFrame()  # Skip API if no date column

    ret = src.write_daily_hydrograph_data_to_csv(
        data=hydrograph,
        column_list=hydrograph.columns.tolist(),
        api_data=hydrograph_api_data)  # Only send today's hydrograph to API
    if ret is None:
        logger.info("Daily hydrograph data written successfully.")
    else:
        logger.error("Failed to write daily hydrograph data.")
        sys.exit(1)
    end_time = time.time()
    time_write_daily_hydrograph_data = end_time - start_time

    # === Data Verification (Debug Mode) ===
    # When SAPPHIRE_DEBUG_VERIFY=true, compare CSV and API data to ensure consistency
    if os.getenv("SAPPHIRE_DEBUG_VERIFY", "false").lower() == "true":
        logger.info("Running data consistency verification (SAPPHIRE_DEBUG_VERIFY=true)...")

        # Verify runoff data
        runoff_verification = src.verify_runoff_data_consistency()
        if runoff_verification['status'] == 'match':
            logger.info(f"✓ Runoff: {runoff_verification['message']}")
        elif runoff_verification['status'] == 'mismatch':
            logger.warning(f"✗ Runoff: {runoff_verification['message']}")
            if 'sample_mismatches' in runoff_verification:
                for m in runoff_verification['sample_mismatches']:
                    logger.warning(f"  - {m}")
        elif runoff_verification['status'] == 'error':
            logger.error(f"✗ Runoff verification error: {runoff_verification['message']}")
        else:
            logger.info(f"Runoff: {runoff_verification['message']}")

        # Verify hydrograph data
        hydrograph_verification = src.verify_hydrograph_data_consistency()
        if hydrograph_verification['status'] == 'match':
            logger.info(f"✓ Hydrograph: {hydrograph_verification['message']}")
        elif hydrograph_verification['status'] == 'mismatch':
            logger.warning(f"✗ Hydrograph: {hydrograph_verification['message']}")
            if 'sample_mismatches' in hydrograph_verification:
                for m in hydrograph_verification['sample_mismatches']:
                    logger.warning(f"  - {m}")
        elif hydrograph_verification['status'] == 'error':
            logger.error(f"✗ Hydrograph verification error: {hydrograph_verification['message']}")
        else:
            logger.info(f"Hydrograph: {hydrograph_verification['message']}")

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
    logger.info("Preprocessing of runoff data completed successfully.")

    if ret is None:
        sys.exit(0) # Success
    else:
        sys.exit(1) # Failure

if __name__ == "__main__":
    main()