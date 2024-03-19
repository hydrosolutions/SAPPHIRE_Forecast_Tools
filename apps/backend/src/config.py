import os
import logging
import json
import sys
import datetime as dt
import forecast_library as fl
import tag_library as tl

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class ForecastFlags:
    """
    Class to store the forecast flags. We have flags for each forecast horizon.
    Depending on the date the forecast tools are called for, they identify the
    forecast horizons to service by changing the respective flag from False (no
    forecast produced) to True (forecast produced).
    """
    def __init__(self, pentad=False, decad=False, month=False, season=False):
        self.pentad = pentad
        self.decad = decad
        self.month = month
        self.season = season

    def __repr__(self):
        return self


def store_last_successful_run_date(date: dt.date):
    '''
    Store the last successful run date in a file.

    Args:
        date (date): The date of the last successful run.

    Raises:
        ValueError: If the date is not valid.
        FileNotFoundError: If the environment variables are not set.
        IOError: If the write operation fails.

    Returns:
        None

    Example:
        store_last_successful_run_date(dt.date(2022, 1, 1)) # Stores the date January 1, 2022
    '''
    # Check environment variables
    intermediate_data_path = os.getenv("ieasyforecast_intermediate_data_path")
    last_successful_run_file = os.getenv("ieasyforecast_last_successful_run_file")
    if intermediate_data_path is None or last_successful_run_file is None:
        raise FileNotFoundError("Environment variables not set")

    # Store last successful run date
    logger.info("Storing last successful run date ...")

    # Path to the file
    last_run_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_last_successful_run_file")
    )

    # Test if the date is valid and throw an error if it is not
    # Test if the date is valid and throw an error if it is not
    if not tl.is_gregorian_date(date):
        raise ValueError(f"Invalid date: {date}")

    # Overwrite the file with the current date
    with open(last_run_file, "w") as f1:
        ret = f1.write(date.isoformat())

    # Check if the write was successful
    if ret is None:
        raise IOError(f"Could not store last successful run date in {last_run_file}")

    logger.info("   ... done")
    return None


def parse_command_line_args() -> tuple[bool, dt.datetime]:
    """
    Parse command line arguments to get the calling script and the start date.

    Returns:
        tuple: Tuple containing the offline mode flag, and the start date.
    """
    # Get the command line arguments
    args = sys.argv[1:]

    # Get the name of the calling script
    calling_script = args[2]

    # Test if the string contains the word "run_offline_mode", and sets to True or False accordingly
    # Please note that the online_mode is being deprecated.
    offline_mode = "run_offline_mode" in calling_script

    logger.info("\n\n====================================================\n")
    logger.info(f"forecast_script called from: {calling_script}")

    # Run Main only if we're on the 5th, 10th, 15th, 20th, 25th or last day of
    # the month in operational mode. Otherwise, exit the program.
    # Always run the script in offline mode.
    # Get today's date and convert it to datetime
    start_date = dt.datetime.strptime(args[0], "%Y-%m-%d")
    #start_date = dt.datetime.strptime("2024-01-31", "%Y-%m-%d")  # todo temp remove so previous line remains

    # Initialize forecast_flags for each forecast horizon.
    forecast_flags = ForecastFlags()

    # Get the day of the month
    day = start_date.day
    # Get the last day of the month
    last_day = fl.get_last_day_of_month(start_date)
    # Get the list of days for which we want to run the forecast
    # pentadal forecasting
    days_pentads = [5, 10, 15, 20, 25, last_day.day]
    days_decads = [10, 20, last_day.day]

    # If today is not in days, exit the program.
    if day not in days_pentads:
        logger.info(f"Run for date {start_date}. No forecast date, no forecast will be run.")
        store_last_successful_run_date(start_date)
        exit()  # exit the program
    else:
        logger.info(f"Running forecast for {start_date}.")
        forecast_flags.pentad = True
        if day in days_decads:
            forecast_flags.decad = True

    return offline_mode, start_date, forecast_flags


def load_environment():
    """
    Load environment variables from a .env file based on the context (Docker or local development).

    Returns:
        str: The path to the environment file.
    Raises:
        FileNotFoundError: If the environment file is not found.
    """
    # Read the environment variable IN_DOCKER_CONTAINER to determine which .env file to use
    if os.getenv("IN_DOCKER_CONTAINER") == "True":
        env_file_path = "apps/config/.env"
    elif os.getenv("SAPPHIRE_TEST_ENV") == "True":
        env_file_path = "backend/tests/test_files/.env_develop_test"
    else:
        env_file_path = "../config/.env_develop"

    # Test if the file exists
    if not os.path.exists(env_file_path):
        raise FileNotFoundError(f"Environment file {env_file_path} not found")
    # Load the environment variables
    logger.info(f"Loading environment variables from {env_file_path}")
    res = load_dotenv(env_file_path)
    logger.debug(f"IEASYHYDRO_HOST: {os.getenv('IEASYHYDRO_HOST')}")
    # Test if the environment variables were loaded
    if not res:
        logger.warning(f"Could not load environment variables from {env_file_path}")
    # Test if specific environment variables were loaded
    if os.getenv("ieasyforecast_daily_discharge_path") is None:
        logger.error("config.load_environment(): Environment variable ieasyforecast_daily_discharge_path not set")
    return env_file_path


def get_bulletin_date(start_date: dt.datetime) -> str:
    """
    Add 1 day to the start date to get the bulletin date. For pentadal forecasts,
    the bulletin date first day of a pentad.

    Args:
        start_date (datetime): The start date of the forecast.

    Returns:
        str: The bulletin date.

    Raises:
        TypeError: If the start_date is not a datetime object.
    """
    # The forecast is done one day before the beginning of each pentad
    # That is on the 5th, 10th, 15th, 20th, 25th and on the last day of each month
    # Check that start_date is a datetime object
    if not isinstance(start_date, dt.datetime):
        raise TypeError("start_date must be a datetime object")

    bulletin_date = (start_date + dt.timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"The forecast bulletin date is: {bulletin_date}")
    return bulletin_date


def excel_output():
    """
    Determine whether to write Excel forecast sheets based on a configuration file.

    Returns:
        bool: True if Excel forecast sheets should be written, False otherwise.

    Raises:
        EnvironmentError: If necessary environment variables are not set.
        FileNotFoundError: If the configuration file does not exist.
        KeyError: If the 'write_excel' key is not in the configuration file.
    """
    # Check environment variables
    configuration_path = os.getenv("ieasyforecast_configuration_path")
    config_file_output = os.getenv("ieasyforecast_config_file_output")
    if configuration_path is None or config_file_output is None:
        raise EnvironmentError("Environment variables not set")

    # Read the configuration file
    config_output_file = os.path.join(configuration_path, config_file_output)
    try:
        with open(config_output_file, "r") as json_file:
            config = json.load(json_file)
    except FileNotFoundError:
        logger.error(f"Configuration file {config_output_file} not found.")
        raise

    # Check if write_excel is set to True or False (and nothing else)
    if config["write_excel"] not in [True, False]:
            raise ValueError(f"Invalid value for write_excel: {config['write_excel']}")

    # Check if we should write Excel forecast sheets
    try:
        write_excel = config["write_excel"]
    except KeyError:
        logger.error("'write_excel' key not found in configuration file.")
        raise

    # Log the decision and return it
    if write_excel:
        logger.info("Writing Excel forecast sheets.")
    else:
        logger.info("Not writing Excel forecast sheets.")
    return write_excel


