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
# ieasyhydroforecast_env_file_path=<absolute/path/to/.env> uv run python preprocessing_runoff.py
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
import argparse
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
from src.config import get_log_level, get_validation_settings, get_spot_check_settings, get_site_cache_settings
# Import profiling from src module to use the same instance that collects timing data
# (direct import from src.profiling would create a separate module instance)

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


# Configure logging
# Log level priority: config.yaml > env 'log_level' > default INFO
log_level = get_log_level()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Create the logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Create a file handler to write logs to a file
# A new log file is created every <interval> day at <when>. It is kept for <backupCount> days.
file_handler = TimedRotatingFileHandler('logs/log', when='midnight', interval=1, backupCount=30)
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)  # File captures all levels

# Create a stream handler to print logs to the console
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
console_handler.setLevel(log_level)  # Console respects configured level

# Get the root logger and add the handlers to it
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # Allow all levels, handlers will filter
logger.handlers = []
logger.addHandler(file_handler)
logger.addHandler(console_handler)




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
            logger.info("[CONFIG] Connecting to iEasyHydro HF SDK")
            try:
                ieh_hf_sdk = IEasyHydroHFSDK()
                has_access_to_db = sl.check_database_access(ieh_hf_sdk)
                if not has_access_to_db:
                    ieh_hf_sdk = None
                return ieh_hf_sdk, has_access_to_db
            except Exception as e:
                host = os.getenv('IEASYHYDROHF_HOST', 'not set')
                if 'Connection refused' in str(e):
                    logger.error(f"[CONFIG] Cannot connect to iEasyHydro HF API at {host}")
                    logger.error("[CONFIG] Check: (1) SSH tunnel running, (2) IEASYHYDROHF_HOST in .env is correct")
                else:
                    logger.error(f"[CONFIG] iEasyHydro HF SDK error: {e}")
                sys.exit(1)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Preprocess runoff data for SAPPHIRE forecast tools.'
    )
    parser.add_argument(
        '--maintenance',
        action='store_true',
        help='Run in maintenance mode (fetch lookback window, fill gaps). '
             'Default is operational mode (fetch only latest data).'
    )
    return parser.parse_args()


def get_mode(args) -> str:
    """
    Determine operating mode from CLI args and environment.

    Priority: CLI argument > environment variable > default (operational)

    Returns:
        str: 'operational' or 'maintenance'
    """
    if args.maintenance:
        return 'maintenance'
    return os.getenv('SAPPHIRE_SYNC_MODE', 'operational').lower()


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

    Supports two operating modes:
    - operational (default): Fast daily updates, fetch only latest data
    - maintenance (--maintenance flag): Full lookback window, gap filling
    """
    # Parse command line arguments
    args = parse_args()
    mode = get_mode(args)

    ## Configuration
    # Loads the environment variables from the .env file
    overall_start_time = time.time()

    # Debug: Check profiling status
    profiling_status = os.getenv('PREPROCESSING_PROFILING', 'not set')
    logger.debug(f"[CONFIG] PREPROCESSING_PROFILING = '{profiling_status}'")
    logger.debug(f"[CONFIG] profiling_enabled() = {src.profiling_enabled()}")

    sl.load_environment()

    # Log the operating mode
    logger.info(f"[CONFIG] Mode: {mode.upper()}")
    end_time = time.time()
    time_load_environment = end_time - overall_start_time

    # Get time zone of organization for which the Forecast Tools are deployed
    target_time_zone = sl.get_local_timezone_from_env()
    logger.info(f"[CONFIG] Timezone: {target_time_zone}")

    # Update configuration files for selected sites (only if iEH HF is used)
    # Test if we read from iEasyHydro or iEasyHydro HF for getting operational data
    start_time = time.time()
    # Get site information from iEH (no update of configuration files)
    if os.getenv('ieasyhydroforecast_connect_to_iEH') == 'True':
        logger.info("[CONFIG] Reading forecast sites from iEasyHydro SDK")
        try:
            # Get the iEasyHydro SDK object
            ieh_sdk, has_access_to_db = get_ieh_sdk()
            # Get site information from iEH (no update of configuration files)
            fc_sites_pentad, site_codes_pentad = sl.get_pentadal_forecast_sites(ieh_sdk, backend_has_access_to_db=has_access_to_db)
            fc_sites_decad, site_codes_decad = sl.get_decadal_forecast_sites_from_pentadal_sites(
                fc_sites_pentad, site_codes_pentad)
            logger.debug("[CONFIG] Forecast sites loaded from iEasyHydro SDK")

            # Combine and deduplicate for unified format (iEH doesn't have site_ids)
            seen = set()
            fc_sites = []
            site_codes = []
            for site, code in zip(fc_sites_pentad + fc_sites_decad, site_codes_pentad + site_codes_decad):
                if code not in seen:
                    seen.add(code)
                    fc_sites.append(site)
                    site_codes.append(code)
            site_ids = []  # iEH doesn't use site IDs
        except Exception as e:
            logger.error(f"[CONFIG] Error accessing iEasyHydro SDK: {e}")
            raise e
    else:  # Get information from iEH HF, default behaviour
        logger.info("[CONFIG] Reading forecast sites from iEasyHydro HF SDK")

        # Check cache settings (Phase 6)
        cache_config = get_site_cache_settings()
        cache_enabled = cache_config.get('enabled', True)
        intermediate_data_path = os.getenv("ieasyforecast_intermediate_data_path", ".")
        cache_file = os.path.join(
            intermediate_data_path,
            cache_config.get('cache_file', 'forecast_sites_cache.json')
        )
        max_age_days = cache_config.get('max_age_days', 7)

        # Always initialize SDK (needed for data fetching later)
        try:
            ieh_hf_sdk, has_access_to_db = get_ieh_sdk()
        except Exception as e:
            logger.error(f"[CONFIG] Error accessing iEasyHydro HF SDK: {e}")
            raise e

        # Try to use cache in operational mode (skips slow site-listing calls)
        cache_used = False
        if cache_enabled and mode == 'operational':
            cached_data = src.load_site_cache(cache_file, max_age_days)
            if cached_data is not None:
                # Use cached data (unified format: site_codes, site_ids)
                site_codes = cached_data['site_codes']
                site_ids = cached_data['site_ids']
                # Note: fc_sites objects are not cached, but we don't need them for data fetching
                fc_sites = []  # Empty - not needed for HF data fetching
                cache_used = True
                # Warn if cache is stale (still usable but may be outdated)
                if cached_data.get('is_stale', False):
                    logger.warning(f"[CONFIG] Using STALE cache ({len(site_codes)} sites) - consider running maintenance mode")
                else:
                    logger.info(f"[CONFIG] Loaded {len(site_codes)} sites from cache")
            else:
                logger.warning("[CONFIG] Site cache missing or expired - fetching from SDK (slow)")

        # Fetch all forecast sites from SDK if cache not used (Phase 7: unified function)
        if not cache_used:
            try:
                # Use unified function that gets ALL sites with ANY forecast type enabled
                fc_sites, site_codes, site_ids = sl.get_all_forecast_sites_from_HF_SDK(ieh_hf_sdk)
                logger.debug("[CONFIG] Forecast sites loaded from iEasyHydro HF SDK")

                # Save to cache in maintenance mode
                if mode == 'maintenance' and cache_enabled:
                    src.save_site_cache(cache_file, site_codes, site_ids)
                    logger.info(f"[CONFIG] Site cache updated: {cache_file}")
            except Exception as e:
                logger.error(f"[CONFIG] Error fetching site lists from SDK: {e}")
                raise e

        # For backward compatibility with validation (which uses fc_sites_pentad/decad)
        # Note: These are only used in maintenance mode for expected_sites calculation
        fc_sites_pentad = fc_sites if fc_sites else []
        fc_sites_decad = []  # All sites are now in fc_sites_pentad

    logger.info(f"[CONFIG] Total forecast sites: {len(site_codes)}")
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
        logger.info("Runoff data read from iEasyHydro SDK")

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
                mode=mode,
        )
        logger.info("Runoff data read from iEasyHydro HF SDK")

    # Check if we got any data before accessing DataFrame methods
    if runoff_data is None or (hasattr(runoff_data, 'empty') and runoff_data.empty):
        logger.error(f"No river runoff data found.\n"
                     f"No forecasts can be produced.\n"
                     f"Please check your configuration.\n")
        sys.exit(1)

    logger.debug(f"[DATA] head of runoff data:\n{runoff_data.head(5)}")
    logger.debug(f"[DATA] tail of runoff data:\n{runoff_data.tail(5)}")
    logger.debug(f"[DATA] columns of runoff data: {runoff_data.columns.tolist()}")
    logger.debug(f"[DATA] types of columns in runoff data: {runoff_data.dtypes.to_dict()}")
    # Print whether or not the date column is in datetime format
    if 'date' in runoff_data.columns:
        import pandas as pd
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

    # Log the tail of runoff data for site 16059
    logger.debug(f"[DATA] runoff data for site 16059:\n{runoff_data[runoff_data['code'] == 16059].tail(10)}")
    end_time = time.time()
    time_get_runoff_data = end_time - start_time

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
    # A debug print to check the data
    #src.inspect_site_data(hydrograph, '16006')
    end_time = time.time()
    time_from_daily_time_series_to_hydrograph = end_time - start_time

    # Get dangerous discharge values from iEasyHydro DB
    # Only required for iEasyHydro, not for iEasyHydro HF
    if os.getenv('ieasyhydroforecast_connect_to_iEH') == 'True':
        start_time = time.time()
        if ieh_sdk is not None:
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
        mode=mode)
    if ret is None:
        logger.info("Daily time series data written successfully.")
    else:
        logger.error("Failed to write daily time series data.")
        sys.exit(1)
    end_time = time.time()
    time_write_daily_time_series_data = end_time - start_time

    # Daily hydrograph data
    start_time = time.time()
    ret = src.write_daily_hydrograph_data_to_csv(
        data=hydrograph,
        column_list=hydrograph.columns.tolist(),
        mode=mode)
    if ret is None:
        logger.info("Daily hydrograph data written successfully.")
    else:
        logger.error("Failed to write daily hydrograph data.")
        sys.exit(1)
    end_time = time.time()
    time_write_daily_hydrograph_data = end_time - start_time

    # Post-write validation (Phase 3) - only in maintenance mode
    if mode == "maintenance":
        validation_config = get_validation_settings()
        if validation_config.get('enabled', True):
            logger.info("[DATA] Running post-write validation...")
            intermediate_data_path = os.getenv("ieasyforecast_intermediate_data_path")
            stats_file = os.path.join(
                intermediate_data_path,
                validation_config.get('stats_file', 'reliability_stats.json')
            )
            # Get unique site codes from the forecast sites
            expected_sites = list(set(str(s.code) for s in fc_sites_pentad + fc_sites_decad))
            src.run_post_write_validation(
                output_df=filtered_data,
                expected_sites=expected_sites,
                stats_file=stats_file,
                max_age_days=validation_config.get('max_age_days', 3),
                reliability_threshold=validation_config.get('reliability_threshold', 80.0)
            )

        # Spot-check validation (Phase 4) - requires SDK access
        spot_check_config = get_spot_check_settings()
        if spot_check_config.get('enabled', True) and ieh_hf_sdk is not None:
            logger.info("[DATA] Running spot-check validation...")
            src.run_spot_check_validation(
                sdk=ieh_hf_sdk,
                output_df=filtered_data,
                target_timezone=None  # Uses UTC in SDK responses
            )

    # When SAPPHIRE_DEBUG_VERIFY=true, compare CSV and API data
    if os.getenv("SAPPHIRE_DEBUG_VERIFY", "false").lower() == "true":
        logger.info(
            "Running data consistency verification "
            "(SAPPHIRE_DEBUG_VERIFY=true)..."
        )

        runoff_verification = src.verify_runoff_data_consistency()
        if runoff_verification['status'] == 'match':
            logger.info(f"Runoff: {runoff_verification['message']}")
        elif runoff_verification['status'] == 'match_with_virtual_lag':
            logger.info(f"Runoff: {runoff_verification['message']}")
        elif runoff_verification['status'] == 'mismatch':
            logger.warning(f"Runoff: {runoff_verification['message']}")
            if 'sample_mismatches' in runoff_verification:
                for m in runoff_verification['sample_mismatches']:
                    logger.warning(f"  - {m}")
        elif runoff_verification['status'] == 'error':
            logger.error(
                f"Runoff verification error: "
                f"{runoff_verification['message']}"
            )
        else:
            logger.info(f"Runoff: {runoff_verification['message']}")

        hydrograph_verification = (
            src.verify_hydrograph_data_consistency()
        )
        if hydrograph_verification['status'] == 'match':
            logger.info(
                f"Hydrograph: {hydrograph_verification['message']}"
            )
        elif hydrograph_verification['status'] == 'mismatch':
            logger.warning(
                f"Hydrograph: {hydrograph_verification['message']}"
            )
            if 'sample_mismatches' in hydrograph_verification:
                for m in hydrograph_verification['sample_mismatches']:
                    logger.warning(f"  - {m}")
        elif hydrograph_verification['status'] == 'error':
            logger.error(
                f"Hydrograph verification error: "
                f"{hydrograph_verification['message']}"
            )
        else:
            logger.info(
                f"Hydrograph: {hydrograph_verification['message']}"
            )

    overall_end_time = time.time()
    total_time = overall_end_time - overall_start_time
    logger.info(f"[TIMING] Total: {total_time:.1f}s (config: {time_load_environment:.1f}s, "
                f"sites: {time_get_forecast_sites:.1f}s, data: {time_get_runoff_data:.1f}s, "
                f"process: {time_filter_roughly_for_outliers + time_from_daily_time_series_to_hydrograph:.1f}s, "
                f"write: {time_write_daily_time_series_data + time_write_daily_hydrograph_data:.1f}s)")

    # Output profiling report if enabled (PREPROCESSING_PROFILING=true)
    if src.profiling_enabled():
        src.log_profiling_report()

    logger.info("[OUTPUT] Preprocessing completed successfully")

    if ret is None:
        sys.exit(0) # Success
    else:
        sys.exit(1) # Failure

if __name__ == "__main__":
    main()