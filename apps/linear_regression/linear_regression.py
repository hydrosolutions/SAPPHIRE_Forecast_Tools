"""
Linear Regression Forecast Module
=================================

Produces hydrological forecasts using linear regression for pentadal (5-day)
and/or decadal (10-day) periods.

USAGE
-----

1. FORECAST MODE (default) - Daily operational forecasting
   Runs from last_successful_run_date + 1 day to today.

   # Pentad forecasts only (5-day periods, 72 per year)
   SAPPHIRE_PREDICTION_MODE=PENTAD python linear_regression.py

   # Decad forecasts only (10-day periods, 36 per year)
   SAPPHIRE_PREDICTION_MODE=DECAD python linear_regression.py

   # Both pentad and decad forecasts (default)
   SAPPHIRE_PREDICTION_MODE=BOTH python linear_regression.py
   python linear_regression.py  # same as BOTH

   # With explicit .env file path
   ieasyhydroforecast_env_file_path=/path/to/.env SAPPHIRE_PREDICTION_MODE=PENTAD python linear_regression.py

2. HINDCAST MODE - Recalculate historical forecasts
   Does NOT update last_successful_run_date (preserves operational state).

   # Explicit date range
   python linear_regression.py --hindcast --start-date 2024-01-01 --end-date 2024-12-31

   # Short flags
   python linear_regression.py -H -s 2024-01-01 -e 2024-12-31

   # Auto-detect start date (from last forecast in output files)
   # End date defaults to yesterday
   python linear_regression.py --hindcast

   # Auto-detect with specific end date
   python linear_regression.py --hindcast --end-date 2024-06-30

   # Start from specific date to yesterday
   python linear_regression.py --hindcast --start-date 2024-01-01

ENVIRONMENT VARIABLES
---------------------

Required:
  ieasyhydroforecast_env_file_path   Path to .env configuration file

Prediction mode:
  SAPPHIRE_PREDICTION_MODE           PENTAD, DECAD, or BOTH (default: BOTH)

Hindcast auto-detection:
  ieasyhydroforecast_START_DATE      Default start date for new gauges without
                                     forecast history (format: YYYY-MM-DD)
                                     Example: ieasyhydroforecast_START_DATE=2000-01-01

COMMAND-LINE ARGUMENTS
----------------------

  --hindcast, -H        Run in hindcast mode
  --start-date, -s      Hindcast start date (YYYY-MM-DD)
  --end-date, -e        Hindcast end date (YYYY-MM-DD), defaults to yesterday

DOCKER
------

  # Forecast mode
  docker run <image> sh -c "SAPPHIRE_PREDICTION_MODE=PENTAD python linear_regression.py"

  # Hindcast mode
  docker run <image> sh -c "python linear_regression.py --hindcast -s 2024-01-01"

FORECAST DAYS
-------------

Forecasts are only produced on specific days:
  - PENTAD mode: Days 5, 10, 15, 20, 25, and last day of each month (6 days/month)
  - DECAD mode:  Days 10, 20, and last day of each month (3 days/month)
  - BOTH mode:   Same as PENTAD (pentad days include decad days)

In hindcast mode, the algorithm automatically skips non-forecast days for efficiency.
For example, running `--hindcast -s 2024-01-01 -e 2024-01-31` with PENTAD mode will
process only 6 dates: Jan 5, 10, 15, 20, 25, and 31.

NIGHTLY CATCH-UP
----------------

Running `python linear_regression.py --hindcast` nightly will:
1. Detect new gauges added to iEH HF → run hindcast from ieasyhydroforecast_START_DATE
2. Existing gauges → run from last forecast + 1 day to yesterday
3. Auto-skip to next forecast day if start date is not a forecast day
4. If all forecasts current → exit gracefully with "Nothing to do"

"""

# I/O
import os
import sys
import logging
import argparse
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
logger.info = print

# Set the logging level to DEBUG
logger.setLevel(logging.DEBUG)

# Print location where we find the logs
logger.info(f"Logs will be written to: {os.path.abspath('logs/log')}")


def parse_arguments():
    """
    Parse command-line arguments for the linear regression forecast module.

    Returns:
        argparse.Namespace: Parsed arguments with hindcast, start_date, end_date
    """
    parser = argparse.ArgumentParser(
        description='''Linear Regression Forecast Module for Hydrological Forecasting

To see this help message, run:
  python linear_regression.py --help
  python linear_regression.py -h

Produces pentadal (5-day) and/or decadal (10-day) discharge forecasts using
linear regression. Forecasts are only produced on specific days:
  - PENTAD: Days 5, 10, 15, 20, 25, and last day of each month
  - DECAD:  Days 10, 20, and last day of each month

MODES:
  Forecast Mode (default): Runs from last_successful_run_date + 1 to today.
                           Updates last_successful_run_date after each run.

  Hindcast Mode (-H):      Recalculates historical forecasts for a date range.
                           Does NOT update last_successful_run_date.
                           Automatically skips non-forecast days for efficiency.
''',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:

  Forecast mode (daily operational use):
    python linear_regression.py
    SAPPHIRE_PREDICTION_MODE=PENTAD python linear_regression.py
    SAPPHIRE_PREDICTION_MODE=DECAD python linear_regression.py

  Using a custom .env file:
    ieasyhydroforecast_env_file_path=/path/to/.env python linear_regression.py
    ieasyhydroforecast_env_file_path=./my_config.env python linear_regression.py --hindcast

  Hindcast mode with explicit date range:
    python linear_regression.py --hindcast --start-date 2024-01-01 --end-date 2024-12-31
    python linear_regression.py -H -s 2024-01-01 -e 2024-12-31

  Hindcast with auto-detection (finds last forecast date per gauge):
    python linear_regression.py --hindcast

  Hindcast from specific start date to yesterday:
    python linear_regression.py --hindcast --start-date 2024-01-01

ENVIRONMENT VARIABLES:

  ieasyhydroforecast_env_file_path  Path to .env configuration file (required)
                                    Example: ieasyhydroforecast_env_file_path=./config/.env
  SAPPHIRE_PREDICTION_MODE          PENTAD, DECAD, or BOTH (default: BOTH)
  ieasyhydroforecast_START_DATE     Default start date for new gauges in hindcast
                                    auto-detection (format: YYYY-MM-DD)

NIGHTLY CATCH-UP:

  Running 'python linear_regression.py --hindcast' nightly will:
  1. Detect new gauges -> run hindcast from ieasyhydroforecast_START_DATE
  2. Existing gauges -> run from last forecast + 1 day to yesterday
  3. If all forecasts current -> exit gracefully

DOCKER:

  docker run <image> sh -c "SAPPHIRE_PREDICTION_MODE=PENTAD python linear_regression.py"
  docker run <image> sh -c "ieasyhydroforecast_env_file_path=/data/.env python linear_regression.py"
  docker run <image> sh -c "python linear_regression.py --hindcast -s 2024-01-01"
        """
    )
    parser.add_argument(
        '--hindcast', '-H',
        action='store_true',
        help='Run in hindcast mode to recalculate historical forecasts. '
             'Does NOT update last_successful_run_date.'
    )
    parser.add_argument(
        '--start-date', '-s',
        type=str,
        metavar='YYYY-MM-DD',
        help='Start date for hindcast. If not specified, auto-detects from '
             'output files (uses earliest missing date across all gauges). '
             'New gauges use ieasyhydroforecast_START_DATE from .env.'
    )
    parser.add_argument(
        '--end-date', '-e',
        type=str,
        metavar='YYYY-MM-DD',
        help='End date for hindcast (defaults to yesterday). '
             'Must be before today.'
    )

    args = parser.parse_args()

    # Validate hindcast mode arguments
    if args.hindcast:
        # Parse start date if provided
        if args.start_date:
            try:
                args.start_date = dt.datetime.strptime(args.start_date, '%Y-%m-%d').date()
            except ValueError:
                parser.error(f"Invalid start date format: {args.start_date}. Use YYYY-MM-DD")

        # Parse end date or default to yesterday
        if args.end_date:
            try:
                args.end_date = dt.datetime.strptime(args.end_date, '%Y-%m-%d').date()
            except ValueError:
                parser.error(f"Invalid end date format: {args.end_date}. Use YYYY-MM-DD")
        else:
            args.end_date = dt.date.today() - dt.timedelta(days=1)

        # Validate date range if start_date is provided
        if args.start_date:
            if args.start_date > args.end_date:
                parser.error(f"Start date ({args.start_date}) must be before or equal to end date ({args.end_date})")

        if args.end_date >= dt.date.today():
            parser.error(f"End date ({args.end_date}) must be before today ({dt.date.today()})")

    return args


def get_last_forecast_dates_per_gauge(prediction_mode='BOTH'):
    """
    Get the last forecast date for each gauge from the output files.

    Args:
        prediction_mode: 'PENTAD', 'DECAD', or 'BOTH'

    Returns:
        dict: Dictionary mapping gauge code to last forecast date
              {code: datetime.date or None if no forecasts}
    """
    import os
    intermediate_path = os.getenv('ieasyforecast_intermediate_data_path', '')

    gauge_dates = {}

    # Check pentad forecast file
    if prediction_mode in ['PENTAD', 'BOTH']:
        pentad_file = os.path.join(intermediate_path, 'forecast_pentad_linreg.csv')
        if os.path.exists(pentad_file):
            try:
                df = pd.read_csv(pentad_file)
                if 'date' in df.columns and 'code' in df.columns and len(df) > 0:
                    df['date'] = pd.to_datetime(df['date'], errors='coerce')
                    df['code'] = df['code'].astype(str)
                    for code, group in df.groupby('code'):
                        max_date = group['date'].max()
                        if pd.notna(max_date):
                            if code not in gauge_dates or max_date.date() < gauge_dates[code]:
                                gauge_dates[code] = max_date.date()
            except Exception as e:
                logger.warning(f"Could not read pentad forecast file: {e}")

    # Check decad forecast file
    if prediction_mode in ['DECAD', 'BOTH']:
        decad_file = os.path.join(intermediate_path, 'forecast_decad_linreg.csv')
        if os.path.exists(decad_file):
            try:
                df = pd.read_csv(decad_file)
                if 'date' in df.columns and 'code' in df.columns and len(df) > 0:
                    df['date'] = pd.to_datetime(df['date'], errors='coerce')
                    df['code'] = df['code'].astype(str)
                    for code, group in df.groupby('code'):
                        max_date = group['date'].max()
                        if pd.notna(max_date):
                            if code not in gauge_dates or max_date.date() < gauge_dates[code]:
                                gauge_dates[code] = max_date.date()
            except Exception as e:
                logger.warning(f"Could not read decad forecast file: {e}")

    return gauge_dates


def get_forecast_days_for_month(year, month, prediction_mode='BOTH'):
    """
    Get the list of forecast days for a given month.

    Args:
        year: The year
        month: The month (1-12)
        prediction_mode: 'PENTAD', 'DECAD', or 'BOTH'

    Returns:
        list: Sorted list of days in the month that are forecast days
    """
    import calendar
    last_day = calendar.monthrange(year, month)[1]

    if prediction_mode == 'DECAD':
        # Decad days: 10, 20, last day
        return [10, 20, last_day]
    else:
        # Pentad days (includes decad days): 5, 10, 15, 20, 25, last day
        return [5, 10, 15, 20, 25, last_day]


def get_next_forecast_day(current_date, prediction_mode='BOTH'):
    """
    Get the next forecast day on or after current_date.

    Args:
        current_date: datetime.date to start searching from
        prediction_mode: 'PENTAD', 'DECAD', or 'BOTH'

    Returns:
        datetime.date: The next forecast day (could be current_date if it's a forecast day)
    """
    import calendar

    date = current_date
    # Search up to 31 days (worst case: last day of month to 5th of next month)
    for _ in range(32):
        last_day = calendar.monthrange(date.year, date.month)[1]

        if prediction_mode == 'DECAD':
            forecast_days = [10, 20, last_day]
        else:
            # PENTAD or BOTH - use pentad days (which include decad days)
            forecast_days = [5, 10, 15, 20, 25, last_day]

        if date.day in forecast_days:
            return date

        date += dt.timedelta(days=1)

    # Shouldn't reach here, but return original date as fallback
    return current_date


def get_hindcast_start_date_from_output(prediction_mode='BOTH', site_list=None):
    """
    Auto-detect the hindcast start date by finding the earliest date after
    the last forecast in the output files. Also considers new gauges that
    have no forecast history.

    Args:
        prediction_mode: 'PENTAD', 'DECAD', or 'BOTH'
        site_list: List of gauge codes to check (from iEH HF). If None, uses all gauges in output.

    Returns:
        datetime.date: The day after the earliest last forecast date found,
                       or the default start date for new gauges (from ieasyhydroforecast_START_DATE env var),
                       or None if no forecasts exist and no site_list provided.

    Environment variables used:
        ieasyhydroforecast_START_DATE: Default start date for new gauges (format: YYYY-MM-DD)
                                       Example: ieasyhydroforecast_START_DATE=2000-01-01
    """
    # Get default start date from environment variable
    env_default_start = os.getenv('ieasyhydroforecast_START_DATE')
    default_start_date = None
    if env_default_start:
        try:
            default_start_date = dt.datetime.strptime(env_default_start, '%Y-%m-%d').date()
            logger.info(f"Default start date from ieasyhydroforecast_START_DATE: {default_start_date}")
        except ValueError:
            logger.warning(f"Invalid ieasyhydroforecast_START_DATE format: {env_default_start}. Expected YYYY-MM-DD")

    # Get last forecast date per gauge from output files
    gauge_dates = get_last_forecast_dates_per_gauge(prediction_mode)

    if gauge_dates:
        logger.info(f"Found forecast history for {len(gauge_dates)} gauges")
        for code, last_date in sorted(gauge_dates.items()):
            logger.debug(f"  Gauge {code}: last forecast {last_date}")

    # If site_list is provided, check for new gauges without forecast history
    new_gauges = []
    if site_list:
        site_codes = set(str(s) for s in site_list)
        existing_codes = set(gauge_dates.keys())
        new_gauges = list(site_codes - existing_codes)
        if new_gauges:
            logger.info(f"Found {len(new_gauges)} new gauges without forecast history: {new_gauges}")

    # Determine start date
    if new_gauges:
        # New gauges need historical hindcast - use the default start date from env
        if default_start_date:
            logger.info(f"New gauges detected. Using ieasyhydroforecast_START_DATE: {default_start_date}")
            return default_start_date
        else:
            logger.error("New gauges detected but ieasyhydroforecast_START_DATE is not set.")
            logger.error("Please set ieasyhydroforecast_START_DATE=YYYY-MM-DD in your .env file")
            return None
    elif gauge_dates:
        # Use the earliest of the last dates (to ensure all gauges are covered)
        earliest_last = min(gauge_dates.values())
        start_date = earliest_last + dt.timedelta(days=1)
        logger.info(f"Auto-detected hindcast start date: {start_date}")
        return start_date
    else:
        # No existing forecasts - use default if available
        if default_start_date:
            logger.info(f"No existing forecasts. Using ieasyhydroforecast_START_DATE: {default_start_date}")
            return default_start_date
        else:
            logger.warning("No existing forecasts found and ieasyhydroforecast_START_DATE not set.")
            return None


def main():

    # Parse command-line arguments
    args = parse_arguments()

    logger.info(f"\n\n====== LINEAR REGRESSION =========================")
    logger.debug(f"Script started at {dt.datetime.now()}.")
    logger.info(f"\n\n------ Setting up --------------------------------")

    # Configuration
    sl.load_environment()

    # Check the prediction mode from environment
    prediction_mode = os.getenv('SAPPHIRE_PREDICTION_MODE', 'BOTH')
    logger.info(f"Running in {prediction_mode} prediction mode")

    # Log hindcast mode if enabled
    if args.hindcast:
        logger.info(f"*** HINDCAST MODE ENABLED ***")
        if args.start_date:
            logger.info(f"Hindcast start date: {args.start_date}")
        else:
            logger.info("Hindcast start date: auto-detect from output files")
        logger.info(f"Hindcast end date: {args.end_date}")

    run_pentad = prediction_mode in ['PENTAD', 'BOTH']
    run_decad = prediction_mode in ['DECAD', 'BOTH']

    # Set up the iEasyHydro SDK
    # Test if we need to connect via ssh tunnel
    if os.getenv('ieasyhydroforecast_ssh_to_iEH') == 'True':
        tunnels = sl.check_local_ssh_tunnels()
        if not tunnels:
            logger.error("No SSH tunnels found but required according to env variable ieasyhydroforecast_ssh_to_iEH. Exiting.")
            exit()
        else:
            logger.debug(f"SSH tunnels found: {tunnels}")

    # Test if we read from iEasyHydro or iEasyHydro HF
    if os.getenv('ieasyhydroforecast_connect_to_iEH') == 'True':
        ieh_sdk = IEasyHydroSDK()
        has_access_to_db = sl.check_database_access(ieh_sdk)
        has_access_to_hf_db = False
        if not has_access_to_db:
            ieh_sdk = None
    else:
        # Connect to iEasyHydro HF
        ieh_hf_sdk = IEasyHydroHFSDK()
        has_access_to_hf_db = sl.check_database_access(ieh_hf_sdk)
        has_access_to_db = False
        if not has_access_to_db:
            ieh_sdk = None
        if not has_access_to_hf_db:
            ieh_hf_sdk = None

    # Identify sites for which to produce forecasts FIRST
    # (Needed for hindcast auto-detection to check for new gauges)
    # Gets us a list of site objects with the necessary information to write forecast outputs
    if has_access_to_hf_db:
        # Use the iEH HF SDK to get the sites
        fc_sites_pentad, site_list_pentad, _ = sl.get_pentadal_forecast_sites_from_HF_SDK(ieh_hf_sdk) if run_pentad else ([], [], None)
        fc_sites_decad, site_list_decad, _ = sl.get_decadal_forecast_sites_from_HF_SDK(ieh_hf_sdk) if run_decad else ([], [], None)
    else:
        # Use the iEH SDK to get the sites
        fc_sites_pentad, site_list_pentad = sl.get_pentadal_forecast_sites(ieh_sdk, has_access_to_db) if run_pentad else ([], [])
        fc_sites_decad, site_list_decad = ([], [])
        if run_decad:
            if run_pentad:
                fc_sites_decad, site_list_decad = sl.get_decadal_forecast_sites_from_pentadal_sites(fc_sites_pentad, site_list_pentad)
            else:
                # If only running decadal forecasts, we need to get pentadal sites first for reference
                temp_fc_sites_pentad, temp_site_list_pentad = sl.get_pentadal_forecast_sites(ieh_sdk, has_access_to_db)
                fc_sites_decad, site_list_decad = sl.get_decadal_forecast_sites_from_pentadal_sites(temp_fc_sites_pentad, temp_site_list_pentad)

    # Combine site lists for hindcast auto-detection
    all_site_codes = list(set(site_list_pentad + site_list_decad))
    logger.info(f"Total sites from iEH: {len(all_site_codes)}")

    # Get start and end dates for current call to the script.
    # forecast_date: date for which the forecast is being run (typically today)
    # date_end: last date for which the forecast is being run (typically today)
    # bulletin_date: first date for which the forecast is valid (typically tomorrow)
    if args.hindcast:
        # Hindcast mode: use provided or auto-detected dates
        if args.start_date:
            forecast_date = args.start_date
        else:
            # Auto-detect start date from output files, passing site list to check for new gauges
            forecast_date = get_hindcast_start_date_from_output(prediction_mode, site_list=all_site_codes)
            if not forecast_date:
                logger.error("Could not auto-detect hindcast start date. Please provide --start-date or set ieasyhydroforecast_START_DATE.")
                sys.exit(1)
        date_end = args.end_date

        # Snap start date to next forecast day (optimization: skip non-forecast days)
        forecast_date = get_next_forecast_day(forecast_date, prediction_mode)
        bulletin_date = forecast_date + dt.timedelta(days=1)

        # Check if there's anything to do
        if forecast_date > date_end:
            logger.info(f"Hindcast mode: next forecast day ({forecast_date}) is after end date ({date_end}).")
            logger.info("All forecasts are already up to date. Nothing to do.")
            sys.exit(0)

        # Count forecast days for logging
        count_days = 0
        check_date = forecast_date
        while check_date <= date_end:
            count_days += 1
            check_date = get_next_forecast_day(check_date + dt.timedelta(days=1), prediction_mode)

        logger.info(f"Hindcast mode: running from {forecast_date} to {date_end}")
        logger.info(f"Hindcast mode: {count_days} forecast days to process")
    else:
        # Normal forecast mode
        forecast_date, date_end, bulletin_date = sl.define_run_dates(prediction_mode=prediction_mode)

    # Only perform the next steps if we have to produce a forecast.
    if not forecast_date:
        logger.info("No valid forecast date. Exiting.")
        exit()

    # Get forecast flags (identify which forecasts to run based on the forecast date)
    forecast_flags = sl.ForecastFlags.from_forecast_date_get_flags(forecast_date)
    forecast_flags.pentad = run_pentad
    forecast_flags.decad = run_decad
    logger.debug(f"Forecast flags: {forecast_flags}")

    # Get pentadal and decadal data for forecasting. This is currently done for
    # pentad as well as for decad forecasts, function overwrites forecast_flags.
    data_pentad, data_decad = fl.get_pentadal_and_decadal_data(
        forecast_flags=forecast_flags,
        site_list_pentad=site_list_pentad,
        site_list_decad=site_list_decad)
    # Test if either data_pentad or data_decad is empty
    if run_pentad and data_pentad.empty:
        logger.info("No pentadal data available. Exiting.")
        exit()
    if run_decad and data_decad.empty:
        logger.info("No decad data available. Exiting.")
        exit()

    if run_pentad:
        logger.info(f"Tail of data pentad: {data_pentad.tail()}")
    if run_decad:
        logger.info(f"Tail of data decad: {data_decad.tail()}")

    # Save pentadal data
    if run_pentad:
        fl.write_pentad_hydrograph_data(data_pentad, iehhf_sdk=ieh_hf_sdk)
        fl.write_pentad_time_series_data(data_pentad)

    # Save decadal data
    if run_decad:
        fl.write_decad_hydrograph_data(data_decad, iehhf_sdk=ieh_hf_sdk)
        fl.write_decad_time_series_data(data_decad)

    # Iterate over the dates
    current_day = forecast_date
    while current_day <= date_end:

        logger.info(f"\n\n------ Forecast on {current_day} --------------------")

        # Update dates - in hindcast mode, we use our iteration dates
        # In normal mode, we re-check from define_run_dates
        if args.hindcast:
            # In hindcast mode, use current_day directly (don't re-call define_run_dates)
            current_date = current_day
            bulletin_date = current_day + dt.timedelta(days=1)
        else:
            # Normal mode: Update the last run_date in the database
            current_date, date_end, bulletin_date = sl.define_run_dates(prediction_mode=prediction_mode)
            # Make sure we have a valid forecast date
            if not forecast_date:
                print("No valid forecast date. Exiting.")
                exit()

        # Update the forecast flags
        forecast_flags = sl.ForecastFlags.from_forecast_date_get_flags(current_date)
        logger.debug(f"Forecast flags: {forecast_flags}")

        # Get predictor dates for the current forecast
        predictor_dates = fl.get_predictor_dates(current_day, forecast_flags)

        # Test if today is a forecast day for either pentadal and decadal forecasts
        # We only run through the rest of the code in the loop if current_date is a forecast day
        if run_pentad and forecast_flags.pentad:
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
            #logger.debug(f"discharge_pentad.head(): \n{discharge_pentad.head()}")
            #logger.debug(f"discharge_pentad.tail(): \n{discharge_pentad.tail()}")

            # Print discharge_data for code == '15194' for april and may 2024
            #logger.info(f"discharge_pentad[discharge_pentad['code'] == '15194'].tail(50): \n{discharge_pentad[discharge_pentad['code'] == '15194'].tail(50)}")

            # Write the predictor to the Site objects
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
            # We take into account saved points from the pentad forecast dashboard.
            print("\n\n\n\n\n\n\n")
            logger.debug("Performing linear regression ...")
            print("Performing linear regression ...")
            linreg_pentad = fl.perform_linear_regression(
                data_df=discharge_pentad,
                station_col='code',
                horizon_col='pentad_in_year',
                predictor_col='discharge_sum',
                discharge_avg_col='discharge_avg',
                forecast_horizon_int=int(forecast_pentad_of_year))

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
            #logger.debug(f"linreg_pentad.tail (forecast): {linreg_pentad}")

            # Write output files for the current forecast horizon
            try:
                # Diagnostics: log env and date coverage about to be written
                env_intermediate = os.getenv('ieasyforecast_intermediate_data_path')
                env_last_success = os.getenv('ieasyforecast_last_successful_run_file')
                logger.info(f"[linreg] intermediate_data_path={env_intermediate}, last_run_file={env_last_success}")

                # Ensure date is datetime for diagnostics using robust parsing
                if 'date' in linreg_pentad.columns:
                    _dates = fl.parse_dates_robust(linreg_pentad['date'], 'date')
                    _years = sorted(set([int(y) for y in _dates.dt.year.dropna().unique()])) if not _dates.empty else []
                    logger.info(f"[linreg] pentad write rows={len(linreg_pentad)}, date_min={_dates.min()}, date_max={_dates.max()}, years={_years}")
                else:
                    logger.warning("[linreg] pentad write: 'date' column missing in output frame")

            except Exception as _e:
                logger.warning(f"[linreg] diagnostics before write failed: {_e}")

            fl.write_linreg_pentad_forecast_data(linreg_pentad)

        else:
            logger.info(f'No pentadal forecast for {current_day}.')

        if run_decad and forecast_flags.decad:
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
            # TODO: Once the decad forecast dashboard is finished, filter for
            # selected points.
            linreg_decad = fl.perform_linear_regression(
                data_df=discharge_decad,
                station_col='code',
                horizon_col='decad_in_year',
                predictor_col='predictor',
                discharge_avg_col='discharge_avg',
                forecast_horizon_int=int(forecast_decad_of_year))

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

        # Store the last run date (only in normal forecast mode, NOT in hindcast mode)
        if args.hindcast:
            logger.debug(f"Hindcast mode: NOT updating last_successful_run_date")
            ret = None
        else:
            ret = sl.store_last_successful_run_date(current_day, prediction_mode=prediction_mode)

        # Move to the next day (or next forecast day in hindcast mode)
        logger.info(f"Iteration for {current_day} completed successfully.")
        if args.hindcast:
            # Skip to next forecast day (more efficient than iterating every day)
            current_day = get_next_forecast_day(current_day + dt.timedelta(days=1), prediction_mode)
        else:
            current_day += dt.timedelta(days=1)

    if ret is None:
        sys.exit(0) # Success
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()