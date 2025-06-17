# environment.py

import os
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

def load_configuration(env_file_path: str=None) -> None:
    """
    Loads the application configuration from an .env file.

    The function first checks if it's running in a Docker container by looking at the IN_DOCKER_CONTAINER environment variable.
    If it's running in a Docker container, it loads the configuration from "apps/config/.env".
    If it's not running in a Docker container, it checks the SAPPHIRE_TEST_ENV and SAPPHIRE_OPDEV_ENV environment variables to determine the path to the .env file.
    If neither of these variables are set, it loads the configuration from "../config/.env_develop".

    The function also checks if the .env file exists and if the environment was loaded successfully by checking if the ieasyforecast_hydrograph_day_file environment variable is set.

    If any of these checks fail, the function raises an exception.

    Returns:
        str: The value of the IN_DOCKER_CONTAINER environment variable or 'None'
           if it's not set.
    """
    # Print the environment variables
    logger.debug("IN_DOCKER_CONTAINER: %s", os.getenv("IN_DOCKER_CONTAINER"))
    logger.debug("SAPPHIRE_TEST_ENV: %s", os.getenv("SAPPHIRE_TEST_ENV"))
    logger.debug("SAPPHIRE_OPDEV_ENV: %s", os.getenv("SAPPHIRE_OPDEV_ENV"))
    print("IN_DOCKER_CONTAINER: ", os.getenv("IN_DOCKER_CONTAINER"), flush=True)
    print("SAPPHIRE_TEST_ENV: ", os.getenv("SAPPHIRE_TEST_ENV"), flush=True)
    print("SAPPHIRE_OPDEV_ENV: ", os.getenv("SAPPHIRE_OPDEV_ENV"), flush=True)

    in_docker_flag = str(os.getenv("IN_DOCKER_CONTAINER"))
    logger.debug("Current working directory: %s", os.getcwd())
    print("Current working directory: ", os.getcwd(), flush=True)
    # Test if env_file_path is set
    if env_file_path is not None:
        path_to_env_file = env_file_path
        logger.debug("Using custom .env file: %s", path_to_env_file)
        print("Using custom .env file: ", path_to_env_file, flush=True)
    else:
        if in_docker_flag == "True":
            if os.getenv("SAPPHIRE_OPDEV_ENV") == "True":
                logger.debug("Running in Docker container with SAPPHIRE_OPDEV_ENV")
                print("Running in Docker container with SAPPHIRE_OPDEV_ENV", flush=True)
                path_to_env_file = "/sensitive_data_forecast_tools/config/.env_develop_kghm"
                logger.debug("Path to .env file: %s", path_to_env_file)
                print("Path to .env file: ", path_to_env_file, flush=True)
            else:
                logger.debug("Running in Docker container with default environment")
                print("Running in Docker container with default environment", flush=True)
                path_to_env_file = "apps/config/.env"
        else:
            if os.getenv("SAPPHIRE_TEST_ENV") == "True":
                logger.debug("Running locally in test environment")
                print("Running locally in test environment", flush=True)
                path_to_env_file = "backend/tests/test_files/.env_develop_test"
            elif os.getenv("SAPPHIRE_OPDEV_ENV") == "True":
                logger.debug("Running locally in opdev environment")
                print("Running locally in opdev environment", flush=True)
                path_to_env_file = "../../../sensitive_data_forecast_tools/config/.env_develop_kghm"
            else:
                # Test if the default .env file exists
                path_to_env_file = "../config/.env_develop"
                if not os.path.isfile(path_to_env_file):
                    raise Exception("File not found: " + path_to_env_file)
                logger.debug("Running locally with public repository data")
                print("Running locally with public repository data", flush=True)

    res = load_dotenv(path_to_env_file)
    if res is None:
        raise Exception("Could not read .env file: ", path_to_env_file)
        # Print ieasyreports_templates_directory_path from the environment
        # variables
    logger.debug("Configuration read from : %s", os.getenv("ieasyforecast_configuration_path"))
    logger.info("ieasyhydroforecast_url: ", os.getenv("ieasyhydroforecast_url"), flush=True)
    print("Configuration read from : ", os.getenv("ieasyforecast_configuration_path"))

    # Test if the environment was loaded successfully
    if os.getenv("ieasyforecast_hydrograph_day_file") is None:
        raise Exception("Environment not loaded. Please check if the .env file is available and if the environment variable IN_DOCKER_CONTAINER is set correctly.")

    # Make sure the host environments are properly set
    # Get host name
    hostport = os.getenv("IEASYHYDRO_HOST")
    # Separate host from port by :
    if hostport is not None:
        host = hostport.split(":")[0]
        port = hostport.split(":")[1]
        # Set the environment variable IEASYHYDRO_PORT
        os.environ["IEASYHYDRO_PORT"] = port
        # Make sure we have system-consistent host names. In a docker container,
        # the host name is 'host.docker.internal'. In a local environment, the host
        # name is 'localhost'.
        if os.getenv('IN_DOCKER_CONTAINER') == "True":
            os.environ["IEASYHYDRO_HOST"] = "host.docker.internal:" + port
        else:
            os.environ["IEASYHYDRO_HOST"] = "localhost:" + port
        logger.info(f"IEASYHYDRO_HOST: {os.getenv('IEASYHYDRO_HOST')}")
    else:
        logger.info("IEASYHYDRO_HOST not set in the .env file")

    return in_docker_flag