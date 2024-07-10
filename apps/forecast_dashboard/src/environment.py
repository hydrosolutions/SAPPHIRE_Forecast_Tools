# environment.py

import os
from dotenv import load_dotenv

def load_configuration():
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
    print("IN_DOCKER_CONTAINER: ", os.getenv("IN_DOCKER_CONTAINER"))
    print("SAPPHIRE_TEST_ENV: ", os.getenv("SAPPHIRE_TEST_ENV"))
    print("SAPPHIRE_OPDEV_ENV: ", os.getenv("SAPPHIRE_OPDEV_ENV"))

    in_docker_flag = str(os.getenv("IN_DOCKER_CONTAINER"))
    print("Current working directory: ", os.getcwd())
    if in_docker_flag == "True":
        if os.getenv("SAPPHIRE_OPDEV_ENV") == "True":
            print("Running in Docker container with SAPPHIRE_OPDEV_ENV")
            path_to_env_file = "../../../sensitive_data_forecast_tools/config/.env_develop_kghm"
            print("Path to .env file: ", path_to_env_file)
        else:
            print("Running in Docker container with default environment")
            path_to_env_file = "apps/config/.env"
    else:
        if os.getenv("SAPPHIRE_TEST_ENV") == "True":
            print("Running locally in test environment")
            path_to_env_file = "backend/tests/test_files/.env_develop_test"
        elif os.getenv("SAPPHIRE_OPDEV_ENV") == "True":
            print("Running locally in opdev environment")
            path_to_env_file = "../../../sensitive_data_forecast_tools/config/.env_develop_kghm"
        else:
            # Test if the default .env file exists
            path_to_env_file = "../config/.env_develop"
            if not os.path.isfile(path_to_env_file):
                raise Exception("File not found: " + path_to_env_file)
            print("Running locally with public repository data")
    # The override flag in read_dotenv is set to allow switching between .env
    # files. Useful when testing different configurations.
    res = load_dotenv(path_to_env_file, override=True)
    if res is None:
        raise Exception("Could not read .env file: ", path_to_env_file)
        # Print ieasyreports_templates_directory_path from the environment
        # variables
    print("Configuration read from : ", os.getenv("ieasyforecast_configuration_path"))

    # Test if the environment was loaded successfully
    if os.getenv("ieasyforecast_hydrograph_day_file") is None:
        raise Exception("Environment not loaded. Please check if the .env file is available and if the environment variable IN_DOCKER_CONTAINER is set correctly.")

    return in_docker_flag