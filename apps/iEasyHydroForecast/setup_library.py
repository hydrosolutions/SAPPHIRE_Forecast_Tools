import os
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

def load_environment():
    """
    Load environment variables from a .env file based on the context.

    This function reads environment variables to determine the context and
    accordingly selects the appropriate .env file to load. The context could be
    Docker, testing, operational development, or local development.

    The function checks for the existence of the .env file and raises an error if
    the file is not found. It then attempts to load the environment variables from
    the file. If the loading is unsuccessful, it logs a warning. If the environment
    variable 'ieasyforecast_daily_discharge_path' is not set, it logs an error.

    Environment Variables:
        IN_DOCKER_CONTAINER: Set to "True" if running in a Docker container.
        SAPPHIRE_TEST_ENV: Set to "True" if running in the test environment.
        SAPPHIRE_OPDEV_ENV: Set to "True" if running in the operational development environment.

    Returns:
        str: The path to the .env file that was loaded.

    Raises:
        FileNotFoundError: If the .env file does not exist.
    """
    # Read the environment variable IN_DOCKER_CONTAINER to determine which .env file to use
    if os.getenv("IN_DOCKER_CONTAINER") == "True":
        env_file_path = "apps/config/.env"
    elif os.getenv("SAPPHIRE_TEST_ENV") == "True":
        env_file_path = "iEasyHydroForecast/tests/test_data/.env_develop_test"
    elif os.getenv("SAPPHIRE_OPDEV_ENV") == "True":
        env_file_path = "../../../sensitive_data_forecast_tools/config/.env_develop_kghm"
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

def check_database_access(ieh_sdk):
    """
    Check if the backend has access to an iEasyHydro database.

    Args:
        ieh_sdk: The iEasyHydro SDK.

    Returns:
        bool: True if the backend has access to the database, False otherwise.

    Raises:
        EnvironmentError: If necessary environment variables are not set.
        Exception: If there is an error connecting to the database.
    """
    # Check that ieh_sdk is not None
    if ieh_sdk is None:
        raise Exception("Invalid ieh_sdk object")

    # Test if the backand has access to an iEasyHydro database and set a flag accordingly.
    try:
        ieh_sdk.get_discharge_sites()
        logger.info(f"Access to iEasyHydro database.")
        return True
    except Exception as e:
        # Test if there are any files in the data/daily_runoff directory
        if os.listdir(os.getenv("ieasyforecast_daily_discharge_path")):
            logger.debug(f"No access to iEasyHydro database "
                        f"will use data from the ieasyforecast_daily_discharge_path for forecasting only.")
            return False
        else:
            logger.error(f"SAPPHIRE tools do not find any data in the ieasyforecast_daily_discharge_path directory "
                         f"nor does it have access to the iEasyHydro database.")
            logger.error(f"Please check the ieasyforecast_daily_discharge_path directory and/or the access to the iEasyHydro database.")
            logger.error(f"Error connecting to DB: {e}")
            raise e