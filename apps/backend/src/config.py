import os
import logging
import json
import sys
import datetime as dt
import forecast_library as fl
import tag_library as tl

from dotenv import load_dotenv

logger = logging.getLogger(__name__)


def store_last_successful_run_date(date: dt.datetime):
    '''
    Store the last successful run date in a file.

    Args:
        date (datetime): The date of the last successful run.

    Raises:
        ValueError: If the date is not valid.
        FileNotFoundError: If the environment variables are not set.
        IOError: If the write operation fails.
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
    calling_script = args[1]

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

    # Get the day of the month
    day = start_date.day
    # Get the last day of the month
    last_day = fl.get_last_day_of_month(start_date)
    # Get the list of days for which we want to run the forecast
    days = [5, 10, 15, 20, 25, last_day.day]

    # If today is not in days, exit the program.
    if day not in days:
        logger.info(f"Run for date {start_date}. No forecast date, no forecast will be run.")
        store_last_successful_run_date(start_date)
        exit()  # exit the program
    else:
        logger.info(f"Running forecast for {start_date}.")

    return offline_mode, start_date


def load_environment():
    """
    Load environment variables from a .env file based on the context (Docker or local development).
    """
    # Read the environment variable IN_DOCKER_CONTAINER to determine which .env file to use
    if os.getenv("IN_DOCKER_CONTAINER") == "True":
        logger.info(f"Running in docker container. Loading environment variables from .env")
        env_file_path = "apps/config/.env"
        res = load_dotenv(env_file_path)
    else:
        logger.info(f"Running locally. Loading environment variables from .env_develop")
        # For development purposes, you can use an .env file to overwrite the
        # default environment variables. This is useful if you need to test the
        # access to the database from your local machine.
        env_file_path = "../config/.env_develop"
        # Note, we use the override=True flag here to overwrite the environment
        # variables read from run_offline_mode.py for debugging and testing purposes.
        res = load_dotenv(dotenv_path=env_file_path, override=True)
    logger.info(f"IEASYHYDRO_HOST: {os.getenv('IEASYHYDRO_HOST')}")
    if not res:
        logger.warning(f"Could not load environment variables from {env_file_path}")
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
