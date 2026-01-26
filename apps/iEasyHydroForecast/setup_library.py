import os
import logging
import pandas as pd
import numpy as np
import json
import datetime as dt
import fnmatch
import re
import subprocess
import socket
import urllib.parse
import platform
import pytz

from dotenv import load_dotenv

# Import iEasyHydroForecast libraries
import forecast_library as fl
import tag_library as tl

# SAPPHIRE API client for database operations
try:
    from sapphire_api_client import (
        SapphirePreprocessingClient,
        SapphirePostprocessingClient,
        SapphireAPIError
    )
    SAPPHIRE_API_AVAILABLE = True
except ImportError:
    SAPPHIRE_API_AVAILABLE = False
    SapphirePreprocessingClient = None
    SapphirePostprocessingClient = None
    SapphireAPIError = Exception  # Fallback for type hints

# Configure the logging level and formatter
logging.basicConfig(level=logging.WARNING)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Create a stream handler to print logs to the console
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Get the root logger and add the handlers to it
logger = logging.getLogger()
logger.handlers = []
logger.addHandler(console_handler)

# === Tools for the initialization of the linear regression forecast ===

# --- Load runtime environment ---------------------------------------------------
# region environment

def store_last_successful_run_date(date, prediction_mode='BOTH'):
    '''
    Store the last successful run date in a file.

    Args:
        date (date or datetime object): The date of the last successful run.

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
    logger.info("Storing last successful run date")

    # Path to the file
    last_run_file = os.path.join(
        intermediate_data_path,
        last_successful_run_file
    )
    # add _<prediction_mode> to the file name if prediction_mode is not BOTH
    if prediction_mode not in ['BOTH']:
        last_run_file = last_run_file.replace(".txt", f"_{prediction_mode}.txt")

    # Convert to datetime object if date is a string or a datetime object
    if isinstance(date, str):
        date = dt.datetime.strptime(date, "%Y-%m-%d").date()
    elif isinstance(date, dt.datetime):
        date = date.date()

    # Test if the date is valid and throw an error if it is not
    # Test if the date is valid and throw an error if it is not
    if not tl.is_gregorian_date(date):
        raise ValueError(f"Invalid date: {date}")

    # Overwrite the file with the current date
    with open(last_run_file, "w") as f1:
        ret = f1.write(date.strftime('%Y-%m-%d'))

    # Check if the write was successful
    if ret is None:
        raise IOError(f"Could not store last successful run date in {last_run_file}")

    return None

def get_last_run_date(prediction_mode='BOTH'):
    """
    Read the date of the last successful run of the linear regression forecast
    from the file ieasyforecast_last_successful_run_file. If the file is not
    available, set the last successful run date to yesterday.

    Returns:
    last_successful_run_date (datetime.date): The date of the last successful
    """
    last_run_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_last_successful_run_file")
        )
    # add _<prediction_mode> to the file name if prediction_mode is not BOTH
    if prediction_mode not in ['BOTH']:
        last_run_file = last_run_file.replace(".txt", f"_{prediction_mode}.txt")
    try:
        with open(last_run_file, "r") as file:
            last_successful_run_date = file.read()
            # We expect the date to be in the format YYYY-MM-DD. Let's allow dates
            # in the format YYYY_MM_DD as well.
            # If the date is in the format YYYY_MM_DD, replace the _ with -
            last_successful_run_date = last_successful_run_date.replace("_", "-")
            last_successful_run_date = dt.datetime.strptime(last_successful_run_date, "%Y-%m-%d").date()
    except FileNotFoundError:
        last_successful_run_date = dt.date.today() - dt.timedelta(days=1)

    logger.debug(f"Last successful run date: {last_successful_run_date}")

    return last_successful_run_date

def define_run_dates(prediction_mode='BOTH'):
    """
    Identifies the start and end dates for the current call to the linear
    regression tool.

    Returns:
    date_start (datetime.date): The start date for the forecast. In operational mode this is today.
    date_end (datetime.date): The end date for the forecast. In operational mode this is today.
    bulletin_date (datetime.date): The bulletin date is the first day of the period for which the forecast is produced. Typically tomorrow.
    """
    print(f"... define_run_dates: prediction_mode: {prediction_mode}")
    # The last successful run date is the last time, the forecast tools were
    # run successfully. This is typically yesterday.
    last_successful_run_date = get_last_run_date(prediction_mode=prediction_mode)
    print(f"... define_run_dates: Last successful run date: {last_successful_run_date}")

    # The day on which the forecast is produced. In operational mode, this is
    # day 0 or today. However, the tools can also be run in hindcast mode by
    # setting the last successful run date to a date in the past. In this case,
    # the forecast is produced for the day after the last successful run date.
    date_start = last_successful_run_date + dt.timedelta(days=1)
    #date_start = dt.date.today()

    # The last day for which a forecast is produced. This is always today.
    date_end = dt.date.today()
    print(f"... define_run_dates: date_start: {date_start}, date_end: {date_end}")

    # Basic sanity check in case the script is run multiple times.
    if date_end == last_successful_run_date:
        logger.info("The forecasts have allready been produced for today.\n"
                       "No forecast will be produced.\n"
                       "Please use the re-run forecast tool to re-run the forecast for today.")
        return None, None, None

    # The bulletin date is one day after the forecast date. It is the first day
    # of the preiod for which the forecast is produced.
    bulletin_date = date_start + dt.timedelta(days=1)

    logger.info("Running the forecast script for the following dates:")
    #logger.info(f"Last successful run date: {last_successful_run_date}")
    logger.info(f"Current forecast start date for forecast iteration: {date_start}")
    logger.info(f"End date for forecast iteration: {date_end}")
    logger.info(f"Current forecast bulletin date: {bulletin_date}")

    return date_start, date_end, bulletin_date

# Methods to check on which system the docker container is running
def check_users_mount():
    return os.path.exists("/Users") and os.listdir("/Users")

def check_hypervisor():
    try:
        result = subprocess.run(["lscpu"], capture_output=True, text=True)
        if result.returncode == 0:
            for line in result.stdout.splitlines():
                if "Hypervisor vendor" in line:
                    return line.split(":")[1].strip()
        return None
    except Exception as e:
        return f"Error: {e}"

def check_os_release():
    try:
        with open("/proc/sys/kernel/osrelease", "r") as f:
            os_release = f.read().strip()
        return os_release
    except Exception as e:
        return f"Error: {e}"

def identify_host_system():
    os_release = check_os_release()
    users_mount = check_users_mount()
    hypervisor = check_hypervisor()

    system_id = None

    if os_release is not None:
        if ("generic" in os_release.lower()) or ("aws" in os_release.lower()):
            # aws is returned when run in Docker container on AWS server
            logger.info("Likely running on a Linux system.")
            system_id = "Linux"
        elif ("darwin" in os_release.lower()) or ("linuxkit" in os_release.lower()):
            # Linuxkit is returned when run in Docker container on MacOS
            logger.info("Likely running on a macOS system.")
            system_id = "macOS"
        else:
            logger.info(f"Could not identify os_release from {os_release}. Defaulting to Linux.")
            system_id = "Linux"
    elif users_mount:
        logger.info("Likely running on a macOS system.")
        system_id = "macOS"
    elif hypervisor and "apple" in hypervisor.lower():
        logger.info("Likely running on a macOS system.")
        system_id = "macOS"
    else:
        logger.info("Could not identify the host system. Defaulting to Linux.")
        system_id = "Linux"

    return system_id

def check_organization():
    """
    Check the organization for which the forecast is produced.
    """
    org = os.getenv("ieasyhydroforecast_organization")
    if org is None:
        logger.error("Environment variable ieasyhydroforecast_organization not set.")
        raise EnvironmentError("Environment variable ieasyhydroforecast_organization not set.")
    return None

def check_connect_to_iEH_and_ssh():
    """Currently connection to iEH is only possible for kghm and through ssh tunnel."""
    # Check if the environment variable ieasyhydroforecast_connect_to_iEH is set
    org = os.getenv("ieasyhydroforecast_organization")
    connect_to_iEH = os.getenv("ieasyhydroforecast_connect_to_iEH")
    require_ssh = os.getenv("ieasyhydroforecast_ssh_to_iEH")

    if connect_to_iEH and connect_to_iEH.lower() == "true":
        if org != "kghm":
            logger.error("Connection to iEH is currently only possible for organization 'kghm'.\n    Please connect to iEH HF, to data files or select a different organization.")
            raise EnvironmentError("Environment variable ieasyhydroforecast_connect_to_iEH is not consistent with ieasyhydroforecast_organization.")
        if require_ssh and require_ssh.lower() == "false":
            logger.error("SSH tunnel is required for connection to iEH.\n    Please set ieasyhydroforecast_ssh_to_iEH to True.")
            raise EnvironmentError("Environment variable ieasyhydroforecast_ssh_to_iEH is not consistent with ieasyhydroforecast_connect_to_iEH.")

    if require_ssh and require_ssh.lower() == "true":
        if connect_to_iEH.lower() == "false":
            logger.error("SSH tunnel is required for connection to iEH.\n    Please set ieasyhydroforecast_connect_to_iEH to True.")
            raise EnvironmentError("Environment variable ieasyhydroforecast_ssh_to_iEH is not consistent with ieasyhydroforecast_connect_to_iEH.")

    return None

def validate_environment_variables(): 
    """
    Validate consistency of the environment variables.
    """
    try:
        check_organization()
    except EnvironmentError as e:
        raise e
    try:
        check_connect_to_iEH_and_ssh()
    except EnvironmentError as e:
        raise e
    
    return None

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
    logger.debug("Current working directory: " + os.getcwd())
    # If we find the path to env file from the environment variable, we can use it
    if os.getenv("ieasyhydroforecast_env_file_path") is not None:
        # Get path to .env file from the environment variable
        env_file_path = os.getenv("ieasyhydroforecast_env_file_path")
    else:
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
    # Test if the environment variables were loaded
    if not res:
        logger.warning(f"Could not load environment variables from {env_file_path}")

    # Get host name
    hostport = os.getenv("IEASYHYDRO_HOST")
    # Separate host from port by :
    if hostport is not None:
        host = hostport.split(":")[1]
        port = hostport.split(":")[2]
        # Set the environment variable IEASYHYDRO_PORT
        os.environ["IEASYHYDRO_PORT"] = port
        logger.debug(f"IEASYHYDRO_PORT: {os.getenv('IEASYHYDRO_PORT')}")
        # Make sure we have system-consistent host names. In a docker container,
        # the host name is 'host.docker.internal'. In a local environment, the host
        # name is 'localhost'.
        if os.getenv('IN_DOCKER_CONTAINER') == "True":
            host_system = identify_host_system()
            logger.info("Running in a Docker container.")
            # If run on Ubuntu.
            # As Docker containers run on Ubuntu, this will always return to 'Linux'
            #system = platform.system()
            if host_system == "Linux":
                os.environ["IEASYHYDRO_HOST"] = "http://localhost:" + port
            elif host_system == "macOS":
                os.environ["IEASYHYDRO_HOST"] = "http://host.docker.internal:" + port
            #os.environ["IEASYHYDRO_HOST"] = "http://host.docker.internal:" + port
        else:
            logger.info("Running in a local environment.")
            os.environ["IEASYHYDRO_HOST"] = "http://localhost:" + port
        logger.debug(f"IEASYHYDRO_HOST: {os.getenv('IEASYHYDRO_HOST')}")
    else:
        logger.warning("IEASYHYDRO_HOST not set in the .env file")

    # Test if specific environment variables were loaded
    if os.getenv("ieasyforecast_daily_discharge_path") is None:
        logger.error("config.load_environment(): Environment variable ieasyforecast_daily_discharge_path not set")
    
    logger.debug("Validating environment variables ...")
    validation = validate_environment_variables()
    if validation is not None:
        logger.error("Environment variables are not valid.")
        raise EnvironmentError("Environment variables are not valid.")
    logger.debug("Environment variables validated.")
    
    return env_file_path

def get_local_timezone_from_env(organization=None):
    """
    Gets the local time zone based on the IEASYHYDROFORECAST_ORGANIZATION environment variable.
    """
    if organization is None:
        organization = os.environ.get("ieasyhydroforecast_organization")
    if organization == "demo":
        return pytz.timezone("Europe/Zurich")  # Switzerland
    elif organization == "kghm":
        return pytz.timezone("Asia/Bishkek")  # Kyrgyzstan
    elif organization == "tjhm":
        return pytz.timezone("Asia/Dushanbe")  # Tajikistan
    else:
        logger.warning(f"Unknown organization: {organization}. Defaulting to UTC.")
        return pytz.utc  # Default to UTC if organization is unknown

# endregion


# --- Tools for accessing the iEasyHydro DB --------------------------------------
'''def check_local_ssh_tunnels(ssh_port=8881):
    """
    Check for local SSH tunnels by examining netstat output.

    Returns:
        list: List of found local SSH tunnels with their details
    """
    try:
        # Run netstat command
        if os.name == 'nt':  # Windows, not tested
            cmd = ['netstat -an']
        else:  # Unix/Linux/macOS
            cmd = ['netstat -an | grep LISTEN']

        output = subprocess.check_output(cmd, shell=True, universal_newlines=True)
        #logger.debug(f"Output from netstat: {output}")

        # Test if we have output
        if not output:
            logger.info("No output from netstat")
            return []

        # Look for listening ports that might be SSH tunnels
        tunnels = []
        for line in output.split('\n'):
            if f"127.0.0.1.{ssh_port}" in line or f"::1.8881" in line:
                tunnels.append({
                    'line': line.strip()
                })
        return tunnels
    except subprocess.CalledProcessError as e:
        print(f"Error running netstat: {e}")
        return []'''

def check_if_ssh_tunnel_is_required(): 
    """
    Check if SSH tunnel is required based on the environment variable.
    """
    var = os.getenv("ieasyhydroforecast_ssh_to_iEH")
    if var is None:
        logger.info("Environment variable ieasyhydroforecast_ssh_to_iEH not set. \n      Assuming that no ssh tunnel is required for connection with iEH or iEH HF.")
        return False
    elif var.lower() == "true":
        logger.debug("Environment variable ieasyhydroforecast_ssh_to_iEH is set to True. \n      Assuming that ssh tunnel is required for connection with iEH or iEH HF.")
        return True
    elif var.lower() == "false":
        logger.debug("Environment variable ieasyhydroforecast_ssh_to_iEH is set to False. \n      Assuming that no ssh tunnel is required for connection with iEH or iEH HF.")
        return False

def check_local_ssh_tunnels(addresses=None, port=None):
    """
    Check for local SSH tunnels using pure Python socket connections.

    Args:
        addresses (list, optional): List of addresses to check.
            Defaults to ['localhost', '127.0.0.1', 'host.docker.internal'].
        port (int, optional): The port number to check. If None, attempts to
            extract it from the IEASYHYDRO_HOST environment variable. Defaults to None.

    Returns:
        list: List of found local SSH tunnels with their details.
    """
    tunnels = []
    try:
        # Determine the port to check
        if port is None:
            host = os.getenv("IEASYHYDRO_HOST")
            if not host:
                logger.error("Environment variable IEASYHYDRO_HOST not set.")
                return []
            try:
                parsed_url = urllib.parse.urlparse(host)
                if parsed_url.port:
                    tunnel_port = parsed_url.port
                else:
                    # If no port is explicitly in the URL, assume default based on scheme
                    tunnel_port = 443 if parsed_url.scheme == 'https' else 80 if parsed_url.scheme == 'http' else 8881
            except Exception as e:
                logger.error(f"Error parsing IEASYHYDRO_HOST: {e}")
                return []
        else:
            tunnel_port = port

        # Use default addresses if none are provided
        if addresses is None:
            addresses = ['localhost', '127.0.0.1', 'host.docker.internal']

        for address in addresses:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2)  # 2 second timeout
                result = sock.connect_ex((address, tunnel_port))
                if result == 0:
                    tunnels.append({'port': tunnel_port, 'line': f'Port {tunnel_port} is listening on {address}'})
                    logger.info(f"SSH tunnel found on {address}:{tunnel_port}")
                else:
                    logger.debug(f"No SSH tunnel found on {address}:{tunnel_port}")
            except socket.gaierror as e:
                logger.warning(f"Address resolution error for {address}: {e}")
            except socket.error as e:
                logger.warning(f"Socket error while checking {address}:{tunnel_port}: {e}")
            finally:
                sock.close()

        if not tunnels:
            logger.info(f"No listening service found on any of the tested addresses for port {tunnel_port}")

    except Exception as e:
        logger.error(f"Error checking SSH tunnels: {e}")

    return tunnels


# region iEH_DB
def check_database_access(ieh_sdk):
    """
    Check if the backend has access to an iEasyHydro database. Also works for 
    testing access to iEasyHydro HF database.

    Args:
        ieh_sdk: The iEasyHydro SDK object or iEasyHydro HF SDK object.

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
        test = ieh_sdk.get_discharge_sites()
        logger.info(f"Access to iEasyHydro database.")
        return True
    except (ConnectionError, TimeoutError) as e:
        logger.error(f"Error connecting to DB: {e}")
        if os.getenv("ieasyhydroforecast_organization") == "demo": 
            logger.info(f"No access to iEasyHydro database but running forecast tools in demo mode.\n        Looking for runoff data files in the ieasyforecast_daily_discharge_path directory.")
            discharge_path = os.getenv("ieasyforecast_daily_discharge_path")
            try: 
                if os.listdir(discharge_path):
                    logger.info("No access to iEasyHydro database. Will use data from the ieasyforecast_daily_discharge_path for forecasting only.")
                    return False
                else: 
                    logger.error(f"No data in the {discharge_path} directory.")
                    return False
            except FileNotFoundError:
                logger.error(f"Directory {discharge_path} not found.")
                return False
        else: 
            logger.error("SAPPHIRE tools do not have access to the iEasyHydro database.")
            raise
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise

# The functions below are required for the old iEasyHydro App.
# For using the forecast tools with the new iEasyHydro HF App, we can read the
# station metadata from the database directly through an API.
def get_pentadal_forecast_sites_complicated_method(ieh_sdk, backend_has_access_to_db):
    """
    Validate the station metadata and filter for stations required to produce forecasts.

    Steps:
    1. Read station metadata from the DB and store it in a list of Site objects.
    2. Read station information of all available discharge stations from json.
    3. Merge information from db_sites and config_all.
    4. Filter db_sites for discharge sites.
    5. Optionally restrict the stations for forecasting (only useful for development).
    6. Overwrite the json file config_all and thus have a consistent list of stations available for forecasting.

    Args:
        ieh_sdk: The iEasyHydro SDK.
        backend_has_access_to_db: True if the backend has access to the database, False otherwise.

    Returns:
        db_sites (pandas.DataFrame): The filtered list of stations required to produce forecasts.
    """
    logger.debug("Validating station metadata and filtering for stations required to produce forecasts.")
    # Read station metadata from the DB and store it in a list of Site objects
    logger.debug("-Reading station metadata from the DB ...")

    # See if we have an environment variable ieasyhydroforecast_connect_to_iEH
    if os.getenv("ieasyhydroforecast_connect_to_iEH") == "False":
        # During development, iEH HF is available online. No need to connect to
        # a server.
        pass
    else:
        # Read the station details from API
        # Only do this if we have access to the database
        if backend_has_access_to_db:
            try:
                db_sites = ieh_sdk.get_discharge_sites()
            except Exception as e:
                logger.error(f"Error connecting to DB: {e}")
                raise e
            db_sites = pd.DataFrame.from_dict(db_sites)

            logger.debug(f"   {len(db_sites)} station(s) in DB, namely:\n{db_sites['site_code'].values}")
        else:
            # If we don't have access to the database, create an empty dataframe.
            db_sites = pd.DataFrame(
                columns=['site_code', 'site_name', 'river_name', 'punkt_name',
                         'latitude', 'longitude', 'region', 'basin'])

    # Read station information of all available discharge stations
    logger.debug("-Reading information about all stations from JSON...")

    config_all_file = os.path.join(
        os.getenv("ieasyforecast_configuration_path"),
        os.getenv("ieasyforecast_config_file_all_stations"))

    config_all = fl.load_all_station_data_from_JSON(config_all_file)

    logger.debug(f"   {len(config_all)} discharge station(s) found, namely\n{config_all['code'].values}")

    # Merge information from db_sites and config_all. Make sure that all sites
    # in config_all are present in db_sites.
    if backend_has_access_to_db:
        logger.debug("-Merging information from db_sites and config_all ...")

        # Find sites in config_all which are not present in db_sites.
        # This is a special case for Kygryz Hydromet.
        new_sites = config_all[~config_all['site_code'].isin(db_sites['site_code'])]

        # Add new sites (e.g. virtual station for inflow reservoirс) to forecast_sites.
        # Edit here if there is need to add new sites for short-term forecasting.
        new_sites_forecast = pd.DataFrame({
            'site_code': new_sites['site_code'],
            'basin': new_sites['basin'],
            'latitude': new_sites['lat'],
            'longitude': new_sites['long'],
            'country': new_sites['country'],
            'is_virtual': new_sites['is_virtual'],
            'region': new_sites['region'],
            'site_type': new_sites['site_type'],
            'site_name': new_sites['name_ru'],
            'organization_id': new_sites['organization_id'],
            'elevation': new_sites['elevation'],
        })
        logger.debug(f"Adding new sites to the list of stations available for forecasting, namely")
        logger.debug(f"{new_sites_forecast['site_code'].values}")
        db_sites = pd.concat([db_sites, new_sites_forecast])

    if backend_has_access_to_db:
        # Add information from config_all to db_sites
        db_sites = pd.merge(
            db_sites,
            config_all[['site_code', 'river_ru', 'punkt_ru', 'lat', 'long']],
            left_on='site_code',
            right_on='site_code',
            how='left'
        )
        # We give precedence to the information from db_sites (from the iEasyHydro
        # database) over the information read from the
        # .env_develop/<ieasyforecast_config_file_all_stations> file.
        # Where lat is not equal to latitude, replace latitude with lat
        db_sites['latitude'] = np.where(db_sites['latitude'] != db_sites['lat'], db_sites['lat'],
                                        db_sites['latitude'])
        # Where long is not equal to longitude, replace longitude with long
        db_sites['longitude'] = np.where(db_sites['longitude'] != db_sites['long'], db_sites['long'],
                                         db_sites['longitude'])

        # Drop the lat and long columns
        db_sites.drop(columns=['lat', 'long'], inplace=True)
    else:
        # If we don't have access to the database, we use the information from config_all
        db_sites = config_all
        # Rename lat to latitude, long to longitude
        db_sites.rename(
            columns={'name_ru': 'site_name', 'lat': 'latitude', 'long': 'longitude'},
            inplace=True)

    # Save db_sites to a json file. This overwrites the existing file.
    db_sites_to_json = db_sites
    # Convert each column to a list
    db_sites_to_json['code'] = db_sites_to_json['site_code'].astype(int)
    for col in db_sites.columns:
        if col != 'site_code':
            db_sites_to_json[col] = db_sites_to_json[col].apply(lambda x: [x])
    db_sites_to_json = db_sites_to_json.set_index('site_code')
    # Rename the site_name column to name_ru
    db_sites_to_json.rename(columns={'site_name': 'name_ru',
                                     'latitude': 'lat',
                                     'longitude': 'long'}, inplace=True)
    json_string = db_sites_to_json.to_json(orient='index', force_ascii=False)
    # Wrap the JSON string in another object
    json_dict = {"stations_available_for_forecast": json.loads(json_string)}
    # Convert the dictionary to a pretty-printed JSON string
    json_string_pretty = json.dumps(json_dict, ensure_ascii=False, indent=4)
    # Write the JSON string to a file
    with open(config_all_file, 'w', encoding='utf-8') as f:
        f.write(json_string_pretty)

    # Filter db_sites for discharge sites
    # NOTE: Important assumption: All discharge sites have a code starting with
    # 1. This is true in Kyrgyz Hydromet at the time of writing.
    db_sites = db_sites[db_sites['site_code'].astype(str).str.startswith('1')]

    # Read stations for forecasting
    logger.debug("-Reading stations for forecasting ...")

    config_selection_file = os.path.join(
        os.getenv("ieasyforecast_configuration_path"),
        os.getenv("ieasyforecast_config_file_station_selection"))

    with open(config_selection_file, "r") as json_file:
        config = json.load(json_file)
        stations = config["stationsID"]

    # Check for the stations filter in the environment variables
    # We start by reading the environment variable ieasyforecast_restrict_stations_file
    restrict_stations_file = os.getenv("ieasyforecast_restrict_stations_file")
    # Check if the environment variable is set to null
    if restrict_stations_file == "null":
        # If it is, we don't restrict the stations
        restrict_stations = []
    else:
        # Read the stations filter from the file
        config_restrict_station_file = os.path.join(
            os.getenv("ieasyforecast_configuration_path"),
            os.getenv("ieasyforecast_restrict_stations_file"))
        try: 
            with open(config_restrict_station_file, "r") as json_file:
                restrict_stations_config = json.load(json_file)
                restrict_stations = restrict_stations_config["stationsID"]
                logger.warning(f"Station selection for pentadal forecasting restricted to: ...")
                logger.warning(f"{restrict_stations}.")
                logger.warning(f" To remove restriction set ieasyforecast_restrict_stations_file in your .env file to null.")
        except FileNotFoundError:
            logger.warning(f"File {config_restrict_station_file} not found. No restriction on stations for forecasting.")
            restrict_stations = []

    # Only keep stations that are in the file ieasyforecast_restrict_stations_file
    if restrict_stations != []:
        stations = [station for station in stations if station in restrict_stations]

    logger.debug(f"   {len(stations)} station(s) selected for pentadal forecasting, namely: {stations}")

    # Filter db_sites for stations
    logger.debug("-Filtering db_sites for stations ...")
    stations_str = [str(station) for station in stations]
    db_sites = db_sites[db_sites["site_code"].isin(stations_str)]
    logger.debug(f"   Producing forecasts for {len(db_sites)} station(s), namely\n: {db_sites['site_code'].values}")

    return db_sites

def get_pentadal_forecast_sites(ieh_sdk, backend_has_access_to_db):
    """
    Get a list of Site objects and a list of strings for site IDs for which to produce forecasts.

    Args:
        ieh_sdk: The iEasyHydro SDK.
        backend_has_access_to_db: True if the backend has access to the database, False otherwise.

    Returns:
        fc_sites (list): A list of Site objects for which to produce forecasts.
        site_codes (list): A list of strings for site IDs for which to produce forecasts.

    """

    # Identify sites for which to produce forecasts
    db_sites = get_pentadal_forecast_sites_complicated_method(ieh_sdk, backend_has_access_to_db)

    # Formatting db_sites to a list of Sites objects
    logger.debug("-Formatting db_sites to a list of Sites objects ...")

    # Make sure the entries are not lists
    db_sites = db_sites.apply(lambda col: col.map(lambda x: x[0] if isinstance(x, list) else x))

    # Get the unique site codes
    site_codes = db_sites["site_code"].unique()

    # Create a list of Site objects
    fc_sites = fl.Site.from_dataframe(
        db_sites[["site_code", "site_name", "river_ru", "punkt_ru", "latitude", "longitude", "region", "basin"]]
    )
    logger.info(f' {len(fc_sites)} Site object(s) created for forecasting, namely:\n{[site.code for site in fc_sites]}')

    # Sort the fc_sites list by descending site code. They will then be sorted in
    # ascending order in the forecast bulletins and sheets.
    fc_sites.sort(key=lambda x: x.code, reverse=True)

    # Get dangerous discharge for each site
    # This can be done only if we have access to the database
    if backend_has_access_to_db:
        logger.debug("-Getting dangerous discharge from DB ...")
        # Call the from_DB_get_dangerous_discharge method on each Site object
        for site in fc_sites:
            fl.Site.from_DB_get_dangerous_discharge(ieh_sdk, site)

        logger.debug(f"   {len(fc_sites)} Dangerous discharge gotten from DB, namely:\n"
                       f"{[site.qdanger for site in fc_sites]}")
    else:
        # Assign " " to qdanger for each site
        for site in fc_sites:
            site.qdanger = " "
        logger.info("No access to iEasyHydro database. Therefore no dangerous discharge is assigned to sites.")

    return fc_sites, site_codes

def get_decadal_forecast_sites_from_HF_SDK(ieh_sdk):
    """
    Get a list of Site objects and a list of strings for site IDs for which to produce forecasts.

    Args:
        fc_sites_pentad (list): A list of Site objects for which to produce pentadal forecasts.
        site_list_decad (list): A list of strings for site IDs for which to produce pentadal forecasts.

    Returns:
        fc_sites_decad (list): A list of Site objects for which to produce forecasts.
        site_codes_decad (list): A list of strings for site IDs for which to produce forecasts.
        site_ids_decad (list): A list of strings for iEH HF site IDs required for API requests.

    """
    # Get the list of discharge sites from the iEH HF SDK
    discharge_sites = ieh_sdk.get_discharge_sites()
    logger.debug(f" {len(discharge_sites)} discharge site(s) found in iEH HF SDK, namely:\n{[site['site_code'] for site in discharge_sites]}")
    # Get the list of Site objects for pentadal or decadal forecasting
    fc_sites = fl.Site.decad_forecast_sites_from_iEH_HF_SDK(discharge_sites)

    # Read virtual stations to the list
    virtual_sites = ieh_sdk.get_virtual_sites()
    logger.debug(f"  {len(virtual_sites)} virtual site(s) found in iEH HF SDK, namely:\n{[site['site_code'] for site in virtual_sites]}")
    # Get list of virtual Site objects for pentadal or decadal forecasting
    virtual_sites = fl.Site.virtual_decad_forecast_sites_from_iEH_HF_SDK(virtual_sites)
    fc_sites.extend(virtual_sites)

    # Get the unique site codes
    site_codes = [site.code for site in fc_sites]

    # We can have automatic and manual sensors at the same hydropost site.
    # If we have duplicate site_codes, remove them from site_codes and from fc_sites.
    site_codes = list(set(site_codes))
    fc_sites = [site for site in fc_sites if site.code in site_codes]

    logger.info(f" {len(fc_sites)} Site object(s) created for decadal forecasting, namely:\n{[site.code for site in fc_sites]}")

    # Get unique site IDs
    site_ids = [site.iehhf_site_id for site in fc_sites]

    # Write the updated site selection to the config file
    json_file = os.path.join(
        os.getenv("ieasyforecast_configuration_path"),
        os.getenv("ieasyforecast_config_file_station_selection_decad")
    )
    # Create a dictionary with the key "stationsID" and the list of sites as the value
    data = {
        "stationsID": site_codes
    }

    # Write the dictionary to a JSON file
    with open(json_file, 'w') as json_file:
        json.dump(data, json_file, indent=2)

    return fc_sites, site_codes, site_ids

def get_decadal_forecast_sites_from_pentadal_sites(fc_sites_pentad=None, site_list_decad=None):
    """
    Get a list of Site objects and a list of strings for site IDs for which to produce forecasts.

    Args:
        fc_sites_pentad (list): A list of Site objects for which to produce pentadal forecasts.
        site_list_decad (list): A list of strings for site IDs for which to produce pentadal forecasts.

    Returns:
        fc_sites_decad (list): A list of Site objects for which to produce forecasts.
        site_codes_decad (list): A list of strings for site IDs for which to produce forecasts.

    """
    # From the environment variables, read the station IDs for which to produce
    # decadal forecasts.
    station_selection_file = os.path.join(
        os.getenv("ieasyforecast_configuration_path"),
        os.getenv("ieasyforecast_config_file_station_selection_decad"))

    with open(station_selection_file, "r") as json_file:
        config = json.load(json_file)
        stations = config["stationsID"]

    # Check for the stations filter in the environment variables
    # We start by reading the environment variable ieasyforecast_restrict_stations_decad_file
    restrict_stations_file = os.getenv("ieasyforecast_restrict_stations_decad_file")
    # Check if the environment variable is set to null
    if restrict_stations_file == "null":
        # If it is, we don't restrict the stations
        restrict_stations = []
    else:
        # Read the stations filter from the file
        config_restrict_station_file = os.path.join(
            os.getenv("ieasyforecast_configuration_path"),
            os.getenv("ieasyforecast_restrict_stations_decad_file"))
        try: 
            with open(config_restrict_station_file, "r") as json_file:
                restrict_stations_config = json.load(json_file)
                restrict_stations = restrict_stations_config["stationsID"]
                logger.warning(f"Station selection for decadal forecasting restricted to: ...")
                logger.warning(f"{restrict_stations}")
                logger.warning(f"To remove restriction set ieasyforecast_restrict_stations_decad_file in your .env file to null.")
        except FileNotFoundError:
            logger.warning(f"File {config_restrict_station_file} not found. No restriction on stations for forecasting.")
            restrict_stations = []

    # Only keep stations that are in the file ieasyforecast_restrict_stations_file
    if restrict_stations != []:
        # Filter stations for decadal forecasting
        stations = [station for station in stations if station in restrict_stations]

    # Make sure the stations for decadal forecasting are also present in the
    # stations lists for pentadal forecasting. Add them if they are not.
    for station in stations:
        if station not in site_list_decad:
            logger.error(f"Hydropost {station} selected for decadal forecasting but ...")
            logger.error(f"   ... not found in the list of stations for pentadal forecasting. ...")
            logger.error(f"   ... Please add station ID to the station selection config file and ...")
            logger.error(f"   ... make sure it is not filtered in the restrict station selection file. ")

    logger.debug(f"   {len(stations)} station(s) selected for decadal forecasting, namely: {stations}")

    # Filter fc_sites_pentad for stations
    logger.debug("-Filtering fc_sites_pentad for stations for decadal forecasting ...")
    fc_sites_decad = [site for site in fc_sites_pentad if site.code in stations]
    stations_decad = [site.code for site in fc_sites_decad]
    logger.debug(f"   Producing decadal forecasts for {len(fc_sites_decad)} station(s), namely\n: {[site.code for site in fc_sites_decad]}")

    return fc_sites_decad, stations_decad

def get_pentadal_forecast_sites_from_HF_SDK(ieh_hf_sdk):
    """
    Gets site attributes from iEH HF and writes them to list of site objects.

    Args:
    ieh_hf_sdk: The iEasyHydro HF SDK object.

    Returns:
    fc_sites (list): A list of Site objects for which to produce forecasts.
    site_codes (list): A list of strings for site CODEs for which to produce forecasts.
    site_ids (list): A list of strings for iEH HF site IDs for which to produce 
        forecasts. Required for iEH HF SDK. 
    """
    # Check if the ieh_hf_sdk object is None
    if ieh_hf_sdk is None:
        # TODO implement get discharge sites from config yaml files
        return None, None, None
    
    # Get the list of discharge sites from the iEH HF SDK
    discharge_sites = ieh_hf_sdk.get_discharge_sites()
    logger.debug(f" {len(discharge_sites)} discharge site(s) found in iEH HF SDK, namely:\n{[site['site_code'] for site in discharge_sites]}")
    
    # Get the list of Site objects for pentadal or decadal forecasting
    # Note that this only returns manual stations
    # Includes dangerous discharge
    fc_sites = fl.Site.pentad_forecast_sites_from_iEH_HF_SDK(discharge_sites)
    logger.debug(f"  First fc_sites object: \n{fc_sites[0]}")
    
    # Read virtual stations to the list
    virtual_sites = ieh_hf_sdk.get_virtual_sites()
    logger.debug(f"  {len(virtual_sites)} virtual site(s) found in iEH HF SDK, namely:\n{[site['site_code'] for site in virtual_sites]}")
    # Get list of virtual Site objects for pentadal or decadal forecasting
    virtual_sites = fl.Site.virtual_pentad_forecast_sites_from_iEH_HF_SDK(virtual_sites)
    fc_sites.extend(virtual_sites)

    # Get the unique site codes
    site_codes = [site.code for site in fc_sites]

    # Remove duplicates
    site_codes = list(set(site_codes))
    fc_sites = [site for site in fc_sites if site.code in site_codes]
    logger.info(f" {len(fc_sites)} Site object(s) created for pentadal forecasting, namely:\n{[site.code for site in fc_sites]}")

    # Get the unique site IDs
    site_ids = [site.iehhf_site_id for site in fc_sites]

    # Write the updated site selection to the config file
    json_file = os.path.join(
        os.getenv("ieasyforecast_configuration_path"),
        os.getenv("ieasyforecast_config_file_station_selection")
    )
    # Create a dictionary with the key "stationsID" and the list of sites as the value
    data = {
        "stationsID": site_codes
    }

    # Write the dictionary to a JSON file
    with open(json_file, 'w') as json_file:
        json.dump(data, json_file, indent=2)

    return fc_sites, site_codes, site_ids


# endregion

# --- Reading of forecast results ------------------------------------------------
# region Reading_forecast_results

# --- API Helper Functions for Postprocessing Data ---

def _read_lr_forecasts_from_api(
    horizon_type: str,
    site_codes: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Read linear regression forecast data from the SAPPHIRE Postprocessing API.

    Parameters:
    -----------
    horizon_type : str
        Either 'pentad' or 'decade'.
    site_codes : list[str] | None
        List of station codes to filter. If None, reads all stations.
    start_date : str | None
        Start date filter (YYYY-MM-DD format). If None, no start filter.
    end_date : str | None
        End date filter (YYYY-MM-DD format). If None, no end filter.

    Returns:
    --------
    pandas.DataFrame
        The LR forecast data with columns: code, date, forecasted_discharge,
        predictor, slope, intercept, horizon_value, horizon_in_year,
        discharge_avg, q_mean, q_std_sigma, delta, rsquared, model_long, model_short.

    Raises:
    -------
    SapphireAPIError
        If the API is not available or the request fails.
    RuntimeError
        If the sapphire-api-client is not installed.
    ValueError
        If horizon_type is invalid.
    """
    if horizon_type not in ('pentad', 'decade'):
        raise ValueError(f"horizon_type must be 'pentad' or 'decade', got: {horizon_type}")

    if not SAPPHIRE_API_AVAILABLE:
        raise RuntimeError(
            "sapphire-api-client is not installed. "
            "Install it with: pip install git+https://github.com/hydrosolutions/sapphire-api-client.git"
        )

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    client = SapphirePostprocessingClient(base_url=api_url)

    # Health check first - fail fast if API unavailable
    if not client.readiness_check():
        raise SapphireAPIError(f"SAPPHIRE API at {api_url} is not ready")

    logger.info(f"Reading LR forecasts ({horizon_type}) from SAPPHIRE API at {api_url}")

    # Collect all data with pagination
    all_data = []
    page_size = 10000

    # If site_codes provided, query per code for efficiency
    codes_to_query = site_codes if site_codes else [None]

    for code in codes_to_query:
        skip = 0
        while True:
            df_page = client.read_lr_forecasts(
                horizon=horizon_type,
                code=code,
                start_date=start_date,
                end_date=end_date,
                skip=skip,
                limit=page_size,
            )

            if df_page.empty:
                break

            all_data.append(df_page)
            logger.debug(
                f"Read {len(df_page)} LR forecast records for horizon={horizon_type}, code={code} (skip={skip})"
            )

            if len(df_page) < page_size:
                break

            skip += page_size

    if not all_data:
        logger.warning(f"No LR forecast data ({horizon_type}) returned from API")
        return pd.DataFrame()

    # Combine all pages
    forecast_data = pd.concat(all_data, ignore_index=True)

    # Remove duplicates (defensive)
    forecast_data = forecast_data.drop_duplicates(subset=['code', 'date'], keep='last')

    # Convert types
    if 'date' in forecast_data.columns:
        forecast_data['date'] = fl.parse_dates_robust(forecast_data['date'], 'date')
    forecast_data['code'] = forecast_data['code'].astype(str).str.replace(r'\.0$', '', regex=True)

    # Add model columns
    forecast_data["model_long"] = "Linear regression (LR)"
    forecast_data["model_short"] = "LR"

    # Sort by code and date
    forecast_data = forecast_data.sort_values(by=['code', 'date'])

    logger.info(f"LR forecast data ({horizon_type}) read from API: {len(forecast_data)} records")
    logger.info(f"Date range: {forecast_data['date'].min()} to {forecast_data['date'].max()}")
    logger.info(f"Stations: {forecast_data['code'].unique().tolist()}")

    return forecast_data


def _read_ml_forecasts_from_api(
    model: str,
    horizon_type: str,
    site_codes: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Read machine learning forecast data from the SAPPHIRE Postprocessing API.

    Parameters:
    -----------
    model : str
        Model type filter: 'TFT', 'TIDE', 'TSMIXER', or 'ARIMA'.
    horizon_type : str
        Either 'pentad' or 'decade'.
    site_codes : list[str] | None
        List of station codes to filter. If None, reads all stations.
    start_date : str | None
        Start date filter (YYYY-MM-DD format). If None, no start filter.
    end_date : str | None
        End date filter (YYYY-MM-DD format). If None, no end filter.

    Returns:
    --------
    pandas.DataFrame
        The ML forecast data with columns: code, date, forecasted_discharge,
        flag, q05, q25, q50, q75, q95, model_long, model_short.

    Raises:
    -------
    SapphireAPIError
        If the API is not available or the request fails.
    RuntimeError
        If the sapphire-api-client is not installed.
    ValueError
        If horizon_type or model is invalid.
    """
    if horizon_type not in ('pentad', 'decade'):
        raise ValueError(f"horizon_type must be 'pentad' or 'decade', got: {horizon_type}")

    # Model type mapping: API model_type -> (model_long, model_short)
    # - model_long: Full descriptive name for display
    # - model_short: Abbreviated name for column headers and compact display
    model_mapping = {
        'TFT': ('Temporal Fusion Transformer (TFT)', 'TFT'),
        'TIDE': ('Time-series Dense Encoder (TiDE)', 'TiDE'),
        'TSMIXER': ('Time-Series Mixer (TSMixer)', 'TSMixer'),
        'ARIMA': ('AutoRegressive Integrated Moving Average (ARIMA)', 'ARIMA'),
        'RRMAMBA': ('Rainfall-Runoff Mamba (RRMAMBA)', 'RRMAMBA'),
    }

    model_upper = model.upper()
    if model_upper not in model_mapping:
        raise ValueError(f"model must be one of {list(model_mapping.keys())}, got: {model}")

    model_long, model_short = model_mapping[model_upper]

    if not SAPPHIRE_API_AVAILABLE:
        raise RuntimeError(
            "sapphire-api-client is not installed. "
            "Install it with: pip install git+https://github.com/hydrosolutions/sapphire-api-client.git"
        )

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    client = SapphirePostprocessingClient(base_url=api_url)

    # Health check first - fail fast if API unavailable
    if not client.readiness_check():
        raise SapphireAPIError(f"SAPPHIRE API at {api_url} is not ready")

    logger.info(f"Reading {model} forecasts ({horizon_type}) from SAPPHIRE API at {api_url}")

    # Collect all data with pagination
    all_data = []
    page_size = 10000

    # If site_codes provided, query per code for efficiency
    codes_to_query = site_codes if site_codes else [None]

    for code in codes_to_query:
        skip = 0
        while True:
            df_page = client.read_forecasts(
                horizon=horizon_type,
                code=code,
                start_date=start_date,
                end_date=end_date,
                skip=skip,
                limit=page_size,
            )

            # Store original page length before filtering for pagination check
            original_page_len = len(df_page)

            if df_page.empty:
                break

            # Filter by model type if the API returns multiple models
            # (Note: The API may or may not support model filtering directly)
            if 'model_type' in df_page.columns:
                df_page = df_page[df_page['model_type'].str.upper() == model_upper]

            if not df_page.empty:
                all_data.append(df_page)
                logger.debug(
                    f"Read {len(df_page)} {model} forecast records for horizon={horizon_type}, code={code} (skip={skip})"
                )

            if original_page_len < page_size:
                break

            skip += page_size

    if not all_data:
        logger.warning(f"No {model} forecast data ({horizon_type}) returned from API")
        return pd.DataFrame()

    # Combine all pages
    forecast_data = pd.concat(all_data, ignore_index=True)

    # Remove duplicates (defensive)
    forecast_data = forecast_data.drop_duplicates(subset=['code', 'date'], keep='last')

    # Convert types
    if 'date' in forecast_data.columns:
        forecast_data['date'] = fl.parse_dates_robust(forecast_data['date'], 'date')
    forecast_data['code'] = forecast_data['code'].astype(str).str.replace(r'\.0$', '', regex=True)

    # Add model columns
    forecast_data["model_long"] = model_long
    forecast_data["model_short"] = model_short

    # Sort by code and date
    forecast_data = forecast_data.sort_values(by=['code', 'date'])

    logger.info(f"{model} forecast data ({horizon_type}) read from API: {len(forecast_data)} records")
    if not forecast_data.empty:
        logger.info(f"Date range: {forecast_data['date'].min()} to {forecast_data['date'].max()}")
        logger.info(f"Stations: {forecast_data['code'].unique().tolist()}")

    return forecast_data


def read_observed_pentadal_data():
    """
    Read the pentadal observed discharge data from API (default) or CSV fallback.

    By default, reads from the SAPPHIRE API runoff endpoint with horizon="pentad".
    Set SAPPHIRE_API_ENABLED=false to use CSV files instead.

    Returns:
    data (pandas.DataFrame): The pentadal data with columns including:
        - code: station code
        - date: observation date
        - discharge_avg: observed discharge value
        - model_long: "Observed (Obs)"
        - model_short: "Obs"
        - pentad_in_month: pentad within the month (1-6)
        - pentad_in_year: pentad within the year (1-72)

    Details:
    When using API, data is read via SapphirePreprocessingClient.read_runoff()
    with horizon="pentad". The 'discharge' column is renamed to 'discharge_avg'
    to match expected downstream format.
    When using CSV, the file to read is specified in the environment variable
    ieasyforecast_pentad_discharge_file. It is expected to have a column 'date'
    with the date of the hydrograph data. If the file has a column 'pentad', it
    is renamed to 'pentad_in_month'.
    """
    # Check if API is enabled (default: true)
    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"

    if api_enabled and SAPPHIRE_API_AVAILABLE:
        logger.info("Reading observed pentadal data from SAPPHIRE API (SAPPHIRE_API_ENABLED=true)")
        try:
            # Get API URL from environment, default to localhost
            api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
            client = SapphirePreprocessingClient(base_url=api_url)

            # Health check first - fail fast if API unavailable
            if not client.readiness_check():
                raise Exception(f"SAPPHIRE API at {api_url} is not ready")

            # Read pentad discharge data from runoff API with pagination
            all_data = []
            skip = 0
            page_size = 10000
            while True:
                df_page = client.read_runoff(
                    horizon="pentad",
                    skip=skip,
                    limit=page_size,
                )
                if df_page.empty:
                    break
                all_data.append(df_page)
                if len(df_page) < page_size:
                    break
                skip += page_size

            if all_data:
                data = pd.concat(all_data, ignore_index=True)

                # Rename discharge to discharge_avg for downstream compatibility
                if 'discharge' in data.columns:
                    data = data.rename(columns={'discharge': 'discharge_avg'})

                # Rename horizon columns to pentad-specific names
                if 'horizon_value' in data.columns and 'pentad_in_month' not in data.columns:
                    data = data.rename(columns={'horizon_value': 'pentad_in_month'})
                if 'horizon_in_year' in data.columns and 'pentad_in_year' not in data.columns:
                    data = data.rename(columns={'horizon_in_year': 'pentad_in_year'})

                # Apply robust date parsing and ensure code is string
                if 'date' in data.columns:
                    data['date'] = fl.parse_dates_robust(data['date'], 'date')
                if 'code' in data.columns:
                    data['code'] = data['code'].astype(str).str.replace(r'\.0$', '', regex=True)

                # Rename 'pentad' to 'pentad_in_month' if present for consistency
                if 'pentad' in data.columns and 'pentad_in_month' not in data.columns:
                    data.rename(columns={'pentad': 'pentad_in_month'}, inplace=True)
            else:
                data = pd.DataFrame(columns=['code', 'date', 'discharge_avg'])

            # Add model columns for observed data
            data["model_long"] = "Observed (Obs)"
            data["model_short"] = "Obs"

            logger.info(f"Read {len(data)} rows of observed data for the pentadal forecast horizon from API.")
            return data

        except Exception as e:
            logger.warning(f"Failed to read from API: {e}. Falling back to CSV.")
            # Fall through to CSV reading

    # CSV fallback: Read from file
    logger.info("Reading observed pentadal data from CSV (SAPPHIRE_API_ENABLED=false or API unavailable)")
    filepath = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_pentad_discharge_file")
    )
    data = pd.read_csv(filepath)

    # Apply robust date parsing and ensure code is string without .0 suffixes
    if 'date' in data.columns:
        data['date'] = fl.parse_dates_robust(data['date'], 'date')
    if 'code' in data.columns:
        data['code'] = data['code'].astype(str).str.replace(r'\.0$', '', regex=True)

    # Add a column model to the dataframe
    data["model_long"] = "Observed (Obs)"
    data["model_short"] = "Obs"

    # If there is a column name 'pentad', rename it to 'pentad_in_month'
    if 'pentad' in data.columns:
        data.rename(columns={'pentad': 'pentad_in_month'}, inplace=True)
    logger.info(f"Read {len(data)} rows of observed data for the pentadal forecast horizon from CSV.")

    return data

def read_observed_decadal_data():
    """
    Read the decadal observed discharge data from API (default) or CSV fallback.

    By default, reads from the SAPPHIRE API runoff endpoint with horizon="decade".
    Set SAPPHIRE_API_ENABLED=false to use CSV files instead.

    Returns:
    data (pandas.DataFrame): The decadal data with columns including:
        - code: station code
        - date: observation date
        - discharge_avg: observed discharge value
        - model_long: "Observed (Obs)"
        - model_short: "Obs"
        - decad_in_month: decade within the month (1-3)
        - decad_in_year: decade within the year (1-36)

    Details:
    When using API, data is read via SapphirePreprocessingClient.read_runoff()
    with horizon="decade". The 'discharge' column is renamed to 'discharge_avg'
    to match expected downstream format.
    When using CSV, the file to read is specified in the environment variable
    ieasyforecast_decad_discharge_file. It is expected to have a column 'date'
    with the date of the hydrograph data.
    """
    # Check if API is enabled (default: true)
    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"

    if api_enabled and SAPPHIRE_API_AVAILABLE:
        logger.info("Reading observed decadal data from SAPPHIRE API (SAPPHIRE_API_ENABLED=true)")
        try:
            # Get API URL from environment, default to localhost
            api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
            client = SapphirePreprocessingClient(base_url=api_url)

            # Health check first - fail fast if API unavailable
            if not client.readiness_check():
                raise Exception(f"SAPPHIRE API at {api_url} is not ready")

            # Read decade discharge data from runoff API with pagination
            all_data = []
            skip = 0
            page_size = 10000
            while True:
                df_page = client.read_runoff(
                    horizon="decade",
                    skip=skip,
                    limit=page_size,
                )
                if df_page.empty:
                    break
                all_data.append(df_page)
                if len(df_page) < page_size:
                    break
                skip += page_size

            if all_data:
                data = pd.concat(all_data, ignore_index=True)

                # Rename discharge to discharge_avg for downstream compatibility
                if 'discharge' in data.columns:
                    data = data.rename(columns={'discharge': 'discharge_avg'})

                # Rename horizon columns to decad-specific names
                if 'horizon_value' in data.columns and 'decad_in_month' not in data.columns:
                    data = data.rename(columns={'horizon_value': 'decad_in_month'})
                if 'horizon_in_year' in data.columns and 'decad_in_year' not in data.columns:
                    data = data.rename(columns={'horizon_in_year': 'decad_in_year'})

                # Apply robust date parsing and ensure code is string
                if 'date' in data.columns:
                    data['date'] = fl.parse_dates_robust(data['date'], 'date')
                if 'code' in data.columns:
                    data['code'] = data['code'].astype(str).str.replace(r'\.0$', '', regex=True)
            else:
                data = pd.DataFrame(columns=['code', 'date', 'discharge_avg'])

            # Add model columns for observed data
            data["model_long"] = "Observed (Obs)"
            data["model_short"] = "Obs"

            logger.info(f"Read {len(data)} rows of observed data for the decadal forecast horizon from API.")
            return data

        except Exception as e:
            logger.warning(f"Failed to read from API: {e}. Falling back to CSV.")
            # Fall through to CSV reading

    # CSV fallback: Read from file
    logger.info("Reading observed decadal data from CSV (SAPPHIRE_API_ENABLED=false or API unavailable)")
    filepath = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_decad_discharge_file")
    )
    data = pd.read_csv(filepath)

    # Apply robust date parsing and ensure code is string without .0 suffixes
    if 'date' in data.columns:
        data['date'] = fl.parse_dates_robust(data['date'], 'date')
    if 'code' in data.columns:
        data['code'] = data['code'].astype(str).str.replace(r'\.0$', '', regex=True)

    # Add a column model to the dataframe
    data["model_long"] = "Observed (Obs)"
    data["model_short"] = "Obs"

    logger.info(f"Read {len(data)} rows of observed data for the decadal forecast horizon from CSV.")

    return data

def read_linreg_forecasts_pentad():
    """
    Read the linear regression forecasts for the pentadal forecast horizon and
    adds the name of the model to the DataFrame.

    By default, reads from the SAPPHIRE Postprocessing API.
    Set SAPPHIRE_API_ENABLED=false to use CSV files instead.

    Since the linreg result file currently holds some general runoff statistics,
    we need to filter these out and return them in a separate DataFrame.

    Returns:
    forecasts (pandas.DataFrame): The linear regression forecasts for the
        pentadal forecast horizon with added model_long and model_short columns.
    stats (pandas.DataFrame): The statistics of the observed data for the
        pentadal forecast horizon.

    Details:
    When using API, data is read via _read_lr_forecasts_from_api(horizon_type="pentad").
    When using CSV, the file to read is specified in the environment variable
    ieasyforecast_analysis_pentad_file. It is expected to have a column 'date'
    with the date of the forecast.

    Generally, we expect the following columns in the forecast files:
    date: The date the forecast is produced for the following pentad
    code: The unique hydropost identifier
    forecasted_discharge: The forecasted discharge for the pentad

    Optional columns are, in the case of the linear regression method:
    predictor: The predictor used in the linear regression model
    discharge_avg: The average discharge for the pentad used in the linear regression model
    pentad_in_month: The pentad in the month for which the forecast is produced
    pentad_in_year: The pentad in the year for which the forecast is produced
    slope: The slope of the linear regression model
    intercept: The intercept of the linear regression model

    The following columns are in the linreg forecast result file but referr to
    general runoff statistics and are later merged to the observed DataFrame:
    q_mean: The mean discharge over the available data for the forecast pentad
    q_std_sigma: The standard deviation of the discharge over the available data for the forecast pentad
        Generally referred to as sigma in the hydromet.
    delta: The acceptable range for the forecast around the observed discharge.
        Calculated by the hydromet as 0.674 * q_std_sigma (assuming normal distribution of the pentadal discharge)
    """
    # Check if API is enabled (default: true)
    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"

    if api_enabled and SAPPHIRE_API_AVAILABLE:
        logger.info("Reading LR pentadal forecasts from SAPPHIRE API (SAPPHIRE_API_ENABLED=true)")
        try:
            # Read from API using the helper function
            data = _read_lr_forecasts_from_api(horizon_type="pentad")

            if data.empty:
                logger.warning("No LR pentadal forecast data returned from API, falling back to CSV")
            else:
                # Drop duplicate rows in date and code if they exist, keeping the last row
                data.drop_duplicates(subset=["date", "code"], keep="last", inplace=True)

                # Split the data into forecasts and statistics
                # The statistics are general runoff statistics and are later merged to the
                # observed DataFrame
                stats_cols = ["date", "code", "q_mean", "q_std_sigma", "delta"]
                stats_cols_present = [c for c in stats_cols if c in data.columns]
                if len(stats_cols_present) >= 3:  # At least date, code, and one stat column
                    stats = data[stats_cols_present].copy()
                else:
                    stats = pd.DataFrame(columns=["date", "code", "q_mean", "q_std_sigma", "delta"])

                # Remove stats columns and discharge_avg from forecasts if present
                cols_to_drop = ["q_mean", "q_std_sigma", "delta", "discharge_avg"]
                cols_to_drop_present = [c for c in cols_to_drop if c in data.columns]
                forecasts = data.drop(columns=cols_to_drop_present, errors='ignore')

                # Recalculate pentad in month and pentad in year
                forecasts["pentad_in_month"] = (forecasts["date"] + pd.Timedelta(days=1)).apply(tl.get_pentad)
                forecasts["pentad_in_year"] = (forecasts["date"] + pd.Timedelta(days=1)).apply(tl.get_pentad_in_year)

                # Save the most recent forecasts to CSV for comparison
                try:
                    save_most_recent_forecasts(forecasts, "LR")
                except ImportError:
                    logger.warning("Could not import save_most_recent_forecasts from src.postprocessing_tools")
                except Exception as e:
                    logger.warning(f"Error saving most recent LR forecasts: {e}")

                logger.info(f"Read {len(forecasts)} rows of linear regression forecasts for the pentadal forecast horizon from API.")
                logger.info(f"Read {len(stats)} rows of general runoff statistics for the pentadal forecast horizon from API.")
                logger.debug(f"Columns in the linear regression forecast data:\n{forecasts.columns}")
                logger.debug(f"Linear regression forecast data: \n{forecasts.head()}")
                logger.debug(f"Columns in the general runoff statistics data:\n{stats.columns}")
                logger.debug(f"General runoff statistics data: \n{stats.head()}")

                return forecasts, stats

        except Exception as e:
            logger.warning(f"Failed to read LR pentadal forecasts from API: {e}. Falling back to CSV.")
            # Fall through to CSV reading

    # CSV fallback: Read from file
    logger.info("Reading LR pentadal forecasts from CSV (SAPPHIRE_API_ENABLED=false or API unavailable)")
    filepath = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_analysis_pentad_file")
    )
    data = pd.read_csv(filepath)

    # Apply robust date parsing and ensure code is string without .0 suffixes
    if 'date' in data.columns:
        data['date'] = fl.parse_dates_robust(data['date'], 'date')
    if 'code' in data.columns:
        data['code'] = data['code'].astype(str).str.replace(r'\.0$', '', regex=True)

    # Drop duplicate rows in date and code if they exist, keeping the last row
    data.drop_duplicates(subset=["date", "code"], keep="last", inplace=True)

    # Add a column model to the dataframe
    data["model_long"] = "Linear regression (LR)"
    data["model_short"] = "LR"

    # Split the data into forecasts and statistics
    # The statistics are general runoff statistics and are later merged to the
    # observed DataFrame
    stats = data[["date", "code", "q_mean", "q_std_sigma", "delta"]]
    forecasts = data.drop(columns=["q_mean", "q_std_sigma", "delta", "discharge_avg"])

    # Add one day to date
    #forecasts.loc[:, "date"] = forecasts.loc[:, "date"] + pd.DateOffset(days=1)
    #stats.loc[:, "date"] = stats.loc[:, "date"] + pd.DateOffset(days=1)

    # Recalculate pentad in month and pentad in year
    forecasts["pentad_in_month"] = (forecasts["date"] + pd.Timedelta(days=1)).apply(tl.get_pentad)
    forecasts["pentad_in_year"] = (forecasts["date"] + pd.Timedelta(days=1)).apply(tl.get_pentad_in_year)

    # Save the most recent forecasts to CSV for comparison
    try:
        save_most_recent_forecasts(forecasts, "LR")
    except ImportError:
        logger.warning("Could not import save_most_recent_forecasts from src.postprocessing_tools")
    except Exception as e:
        logger.warning(f"Error saving most recent LR forecasts: {e}")

    logger.info(f"Read {len(forecasts)} rows of linear regression forecasts for the pentadal forecast horizon from CSV.")
    logger.info(f"Read {len(stats)} rows of general runoff statistics for the pentadal forecast horizon from CSV.")
    logger.debug(f"Columns in the linear regression forecast data:\n{forecasts.columns}")
    logger.debug(f"Linear regression forecast data: \n{forecasts.head()}")
    logger.debug(f"Columns in the general runoff statistics data:\n{stats.columns}")
    logger.debug(f"General runoff statistics data: \n{stats.head()}")

    return forecasts, stats

def read_linreg_forecasts_decade():
    """
    Read the linear regression forecasts for the decadal forecast horizon and
    adds the name of the model to the DataFrame.

    By default, reads from the SAPPHIRE Postprocessing API.
    Set SAPPHIRE_API_ENABLED=false to use CSV files instead.

    Since the linreg result file currently holds some general runoff statistics,
    we need to filter these out and return them in a separate DataFrame.

    Returns:
    forecasts (pandas.DataFrame): The linear regression forecasts for the
        decadal forecast horizon with added model_long and model_short columns.
    stats (pandas.DataFrame): The statistics of the observed data for the
        decadal forecast horizon.

    Details:
    When using API, data is read via _read_lr_forecasts_from_api(horizon_type="decade").
    When using CSV, the file to read is specified in the environment variable
    ieasyforecast_analysis_decad_file. It is expected to have a column 'date'
    with the date of the forecast.

    Generally, we expect the following columns in the forecast files:
    date: The date the forecast is produced for the following decade
    code: The unique hydropost identifier
    forecasted_discharge: The forecasted discharge for the decade

    Optional columns are, in the case of the linear regression method:
    predictor: The predictor used in the linear regression model
    discharge_avg: The average discharge for the decade used in the linear regression model
    decad_in_month: The decade in the month for which the forecast is produced
    decad_in_year: The decade in the year for which the forecast is produced
    slope: The slope of the linear regression model
    intercept: The intercept of the linear regression model

    The following columns are in the linreg forecast result file but referr to
    general runoff statistics and are later merged to the observed DataFrame:
    q_mean: The mean discharge over the available data for the forecast decade
    q_std_sigma: The standard deviation of the discharge over the available data for the forecast decade
        Generally referred to as sigma in the hydromet.
    delta: The acceptable range for the forecast around the observed discharge.
        Calculated by the hydromet as 0.674 * q_std_sigma (assuming normal distribution of the decadal discharge)
    """
    # Check if API is enabled (default: true)
    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"

    if api_enabled and SAPPHIRE_API_AVAILABLE:
        logger.info("Reading LR decadal forecasts from SAPPHIRE API (SAPPHIRE_API_ENABLED=true)")
        try:
            # Read from API using the helper function
            data = _read_lr_forecasts_from_api(horizon_type="decade")

            if data.empty:
                logger.warning("No LR decadal forecast data returned from API, falling back to CSV")
            else:
                # Drop duplicate rows in date and code if they exist, keeping the last row
                data.drop_duplicates(subset=["date", "code"], keep="last", inplace=True)

                # Split the data into forecasts and statistics
                # The statistics are general runoff statistics and are later merged to the
                # observed DataFrame
                stats_cols = ["date", "code", "q_mean", "q_std_sigma", "delta"]
                stats_cols_present = [c for c in stats_cols if c in data.columns]
                if len(stats_cols_present) >= 3:  # At least date, code, and one stat column
                    stats = data[stats_cols_present].copy()
                else:
                    stats = pd.DataFrame(columns=["date", "code", "q_mean", "q_std_sigma", "delta"])

                # Remove stats columns and discharge_avg from forecasts if present
                cols_to_drop = ["q_mean", "q_std_sigma", "delta", "discharge_avg"]
                cols_to_drop_present = [c for c in cols_to_drop if c in data.columns]
                forecasts = data.drop(columns=cols_to_drop_present, errors='ignore')

                # Recalculate decad in month and decad in year
                forecasts["decad_in_month"] = (forecasts["date"] + pd.Timedelta(days=1)).apply(tl.get_decad_in_month)
                forecasts["decad_in_year"] = (forecasts["date"] + pd.Timedelta(days=1)).apply(tl.get_decad_in_year)

                # Save the most recent forecasts to CSV for comparison
                try:
                    save_most_recent_forecasts_decade(forecasts, "LR")
                except ImportError:
                    logger.warning("Could not import save_most_recent_forecasts_decade from src.postprocessing_tools")
                except Exception as e:
                    logger.warning(f"Error saving most recent LR forecasts: {e}")

                logger.info(f"Read {len(forecasts)} rows of linear regression forecasts for the decadal forecast horizon from API.")
                logger.info(f"Read {len(stats)} rows of general runoff statistics for the decadal forecast horizon from API.")
                logger.debug(f"Columns in the linear regression forecast data:\n{forecasts.columns}")
                logger.debug(f"Linear regression forecast data: \n{forecasts.head()}")
                logger.debug(f"Columns in the general runoff statistics data:\n{stats.columns}")
                logger.debug(f"General runoff statistics data: \n{stats.head()}")

                return forecasts, stats

        except Exception as e:
            logger.warning(f"Failed to read LR decadal forecasts from API: {e}. Falling back to CSV.")
            # Fall through to CSV reading

    # CSV fallback: Read from file
    logger.info("Reading LR decadal forecasts from CSV (SAPPHIRE_API_ENABLED=false or API unavailable)")
    filepath = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_analysis_decad_file")
    )
    data = pd.read_csv(filepath)

    # Apply robust date parsing and ensure code is string without .0 suffixes
    if 'date' in data.columns:
        data['date'] = fl.parse_dates_robust(data['date'], 'date')
    if 'code' in data.columns:
        data['code'] = data['code'].astype(str).str.replace(r'\.0$', '', regex=True)

    # Drop duplicate rows in date and code if they exist, keeping the last row
    data.drop_duplicates(subset=["date", "code"], keep="last", inplace=True)

    # Add a column model to the dataframe
    data["model_long"] = "Linear regression (LR)"
    data["model_short"] = "LR"

    # Split the data into forecasts and statistics
    # The statistics are general runoff statistics and are later merged to the
    # observed DataFrame
    stats = data[["date", "code", "q_mean", "q_std_sigma", "delta"]]
    forecasts = data.drop(columns=["q_mean", "q_std_sigma", "delta", "discharge_avg"])

    # Add one day to date
    #forecasts.loc[:, "date"] = forecasts.loc[:, "date"] + pd.DateOffset(days=1)
    #stats.loc[:, "date"] = stats.loc[:, "date"] + pd.DateOffset(days=1)

    # Recalculate decad in month and decad in year
    forecasts["decad_in_month"] = (forecasts["date"] + pd.Timedelta(days=1)).apply(tl.get_decad_in_month)
    forecasts["decad_in_year"] = (forecasts["date"] + pd.Timedelta(days=1)).apply(tl.get_decad_in_year)

    # Save the most recent forecasts to CSV for comparison
    try:
        save_most_recent_forecasts_decade(forecasts, "LR")
    except ImportError:
        logger.warning("Could not import save_most_recent_forecasts_decade from src.postprocessing_tools")
    except Exception as e:
        logger.warning(f"Error saving most recent LR forecasts: {e}")

    logger.info(f"Read {len(forecasts)} rows of linear regression forecasts for the decadal forecast horizon from CSV.")
    logger.info(f"Read {len(stats)} rows of general runoff statistics for the decadal forecast horizon from CSV.")
    logger.debug(f"Columns in the linear regression forecast data:\n{forecasts.columns}")
    logger.debug(f"Linear regression forecast data: \n{forecasts.head()}")
    logger.debug(f"Columns in the general runoff statistics data:\n{stats.columns}")
    logger.debug(f"General runoff statistics data: \n{stats.head()}")

    return forecasts, stats

def read_daily_probabilistic_ml_forecasts_pentad(filepath, model, model_long, model_short):
    """
    Reads in forecast results from probabilistic machine learning models for the pentadal forecast.
    Added robust error handling.
    """
    import logging
    logger = logging.getLogger(__name__)

    try:
        # First read the data without date parsing to avoid format conflicts
        try:
            daily_data = pd.read_csv(filepath, on_bad_lines='skip', low_memory=False)
            logger.info(f"Successfully read raw data from {filepath}")
        except Exception as e:
            logger.warning(f"Error reading CSV file {filepath}: {e}")
            return pd.DataFrame()
        
        # We read the 'code' column as string. Discard .0 suffixes if they exist
        if 'code' in daily_data.columns:
            daily_data['code'] = daily_data['code'].astype(str).str.replace(r'\.0$', '', regex=True)
        else:
            logger.warning(f"code column missing from {filepath}")
            return pd.DataFrame()

        # Handle date columns flexibly without assuming format
        for col in ["date", "forecast_date"]:
            if col in daily_data.columns:
                try:
                    # Let pandas infer format for each value
                    daily_data[col] = pd.to_datetime(daily_data[col], errors='coerce')
                    
                    # Check for conversion issues
                    if daily_data[col].isna().any():
                        logger.warning(f"Some values in {col} couldn't be converted to dates")
                        # Drop rows with NaT values
                        daily_data = daily_data.dropna(subset=[col])
                except Exception as e:
                    logger.warning(f"Error converting {col} to datetime: {e}")
                    return pd.DataFrame()
            
        # Make sure date columns are properly converted to datetime
        # Check if forecast_date column exists and is a datetime column
        if "forecast_date" not in daily_data.columns:
            logger.warning(f"forecast_date column missing from {filepath}")
            return pd.DataFrame()
            
        if not pd.api.types.is_datetime64_any_dtype(daily_data["forecast_date"]):
            # Try to convert to datetime
            try:
                daily_data["forecast_date"] = pd.to_datetime(daily_data["forecast_date"], errors='coerce')
            except Exception as e:
                logger.warning(f"Error converting forecast_date to datetime: {e}")
                return pd.DataFrame()
                
        # Check for NaT values after conversion
        if daily_data["forecast_date"].isna().any():
            logger.warning(f"NaT values found in forecast_date column after conversion")
            # Drop rows with NaT values
            daily_data = daily_data.dropna(subset=["forecast_date"])
            
        # Do the same for date column
        if "date" not in daily_data.columns:
            logger.warning(f"date column missing from {filepath}")
            return pd.DataFrame()
            
        if not pd.api.types.is_datetime64_any_dtype(daily_data["date"]):
            # Try to convert to datetime
            try:
                daily_data["date"] = pd.to_datetime(daily_data["date"], errors='coerce')
            except Exception as e:
                logger.warning(f"Error converting date to datetime: {e}")
                return pd.DataFrame()
                
        # Check for NaT values after conversion
        if daily_data["date"].isna().any():
            logger.warning(f"NaT values found in date column after conversion")
            # Drop rows with NaT values
            daily_data = daily_data.dropna(subset=["date"])
        
        # Convert to date what needs to be date
        daily_data["forecast_date"] = daily_data["forecast_date"].dt.date
        daily_data["date"] = daily_data["date"].dt.date

        # Only keep the forecast rows for pentadal forecasts
        # Add a column last_day_of_month to daily_data
        daily_data["last_day_of_month"] = daily_data["forecast_date"].apply(fl.get_last_day_of_month)
        
        # Convert forecast_date to datetime for access to dt accessor
        # This step ensures the column is a datetime we can extract day from
        daily_data["forecast_date"] = pd.to_datetime(daily_data["forecast_date"])
        daily_data["day_of_month"] = daily_data["forecast_date"].dt.day

        # Keep rows that have forecast_date equal to either 5, 10, 15, 20, 25 or last_day_of_month
        data = daily_data[(daily_data["day_of_month"].isin([5, 10, 15, 20, 25])) | \
                        (daily_data["forecast_date"].dt.date == daily_data["last_day_of_month"])].copy()

        # Check if we have any data after filtering
        if data.empty:
            logger.warning(f"No pentadal forecast data found for {model} after filtering")
            return pd.DataFrame()

        # Convert forecast_date back to date
        data.loc[:, "forecast_date"] = data["forecast_date"].dt.date

        # Group by code and forecast_date and calculate the mean of all columns
        forecast = data \
            .drop(columns=["date", "day_of_month", "last_day_of_month"], errors='ignore') \
            .groupby(["code", "forecast_date"]) \
            .mean() \
            .reset_index()

        # Rename the column forecast_date to date and Q50 to forecasted_discharge.
        # In the case of the ARIMA model, we don't have quantiles but rename the column Q to forecasted_discharge.
        columns_to_rename = {}
        if "forecast_date" in forecast.columns:
            columns_to_rename["forecast_date"] = "date"
        if "Q50" in forecast.columns:
            columns_to_rename["Q50"] = "forecasted_discharge"
        if "Q" in forecast.columns:
            columns_to_rename["Q"] = "forecasted_discharge"

        forecast.rename(columns=columns_to_rename, inplace=True)

        # Add model information
        forecast["model_long"] = model_long
        forecast["model_short"] = model_short

        # Recalculate pentad in month and pentad in year if date column exists
        if "date" in forecast.columns:
            # Convert back to datetime for Timedelta operation
            forecast["date"] = pd.to_datetime(forecast["date"])
            forecast["pentad_in_month"] = (forecast["date"] + pd.Timedelta(days=1)).apply(tl.get_pentad)
            forecast["pentad_in_year"] = (forecast["date"] + pd.Timedelta(days=1)).apply(tl.get_pentad_in_year)

        logger.info(f"Read {len(forecast)} rows of {model} forecasts for the pentadal forecast horizon.")
        logger.debug(f"Columns in the {model} forecast data: {forecast.columns}")
        logger.debug(f"Read forecast data sample: {forecast.head()}")

        return forecast

    except Exception as e:
        logger.warning(f"Error processing {model} forecast data from {filepath}: {e}")
        return pd.DataFrame()

def read_daily_probabilistic_ml_forecasts_decade(filepath, model, model_long, model_short):
    """
    Reads in forecast results from probabilistic machine learning models for the decadal forecast.
    Added robust error handling.

    Args:
        filepath (str): The path to the file with the forecast results.
        model (str): The model to read the forecast results from.
        model_long (str): The long name of the model.
        model_short (str): The short name of the model.

    Returns:
        forecast (pandas.DataFrame): The forecast results or an empty DataFrame if error occurs.
    """
    import logging
    logger = logging.getLogger(__name__)

    try:
        # Read the forecast results with robust date parsing
        daily_data = pd.read_csv(filepath)
        
        # Apply robust date parsing and ensure code is string without .0 suffixes
        if 'date' in daily_data.columns:
            daily_data['date'] = fl.parse_dates_robust(daily_data['date'], 'date')
        if 'forecast_date' in daily_data.columns:
            daily_data['forecast_date'] = fl.parse_dates_robust(daily_data['forecast_date'], 'forecast_date')
        if 'code' in daily_data.columns:
            daily_data['code'] = daily_data['code'].astype(str).str.replace(r'\.0$', '', regex=True)

        # Only keep the forecast rows for pentadal forecasts
        # Add a column last_day_of_month to daily_data
        daily_data["last_day_of_month"] = daily_data["forecast_date"].apply(fl.get_last_day_of_month)
        daily_data["day_of_month"] = daily_data["forecast_date"].dt.day

        # Keep rows that have forecast_date equal to either 10, 20, or last_day_of_month
        data = daily_data[(daily_data["day_of_month"].isin([10, 20])) | \
                        (daily_data["forecast_date"] == daily_data["last_day_of_month"])]

        # Check if we have any data after filtering
        if data.empty:
            logger.warning(f"No decadal forecast data found for {model} after filtering")
            return pd.DataFrame()

        # Group by code and forecast_date and calculate the mean of all columns
        forecast = data \
            .drop(columns=["date", "day_of_month", "last_day_of_month"], errors='ignore') \
            .groupby(["code", "forecast_date"]) \
            .mean() \
            .reset_index()

        # Rename the column forecast_date to date and Q50 to forecasted_discharge.
        # In the case of the ARIMA model, we don't have quantiles but rename the column Q to forecasted_discharge.
        columns_to_rename = {}
        if "forecast_date" in forecast.columns:
            columns_to_rename["forecast_date"] = "date"
        if "Q50" in forecast.columns:
            columns_to_rename["Q50"] = "forecasted_discharge"
        if "Q" in forecast.columns:
            columns_to_rename["Q"] = "forecasted_discharge"

        forecast.rename(columns=columns_to_rename, inplace=True)

        # Add model information
        forecast["model_long"] = model_long
        forecast["model_short"] = model_short

        # Recalculate pentad in month and pentad in year if date column exists
        if "date" in forecast.columns:
            forecast["decad_in_month"] = (forecast["date"] + pd.Timedelta(days=1)).apply(tl.get_decad_in_month)
            forecast["decad_in_year"] = (forecast["date"] + pd.Timedelta(days=1)).apply(tl.get_decad_in_year)

        logger.info(f"Read {len(forecast)} rows of {model} forecasts for the decadal forecast horizon.")
        logger.debug(f"Columns in the {model} forecast data: {forecast.columns}")
        logger.debug(f"Read forecast data sample: {forecast.head()}")

        return forecast

    except Exception as e:
        logger.warning(f"Error processing {model} forecast data from {filepath}: {e}")
        return pd.DataFrame()

def read_csv_with_multiple_date_formats(filepath):
    """
    Read a CSV file with date fields that might be in different formats.
    Added robust error handling.

    Args:
        filepath (str): Path to the CSV file

    Returns:
        pandas.DataFrame: The read data or an empty DataFrame if error occurs
    """
    import logging
    logger = logging.getLogger(__name__)

    try:
        # First try to read the file
        daily_data = pd.read_csv(filepath)

        # Handle empty dataframe
        if daily_data.empty:
            logger.warning(f"CSV file {filepath} is empty")
            return pd.DataFrame()

        # Check if required columns exist
        required_columns = ['date', 'forecast_date']
        for col in required_columns:
            if col not in daily_data.columns:
                logger.warning(f"Column {col} missing from {filepath}")
                return pd.DataFrame()

        # Try to parse the 'date' column with the first format
        try:
            daily_data['date'] = pd.to_datetime(daily_data['date'], format='%d.%m.%Y')
        except ValueError:
            # If it fails, try the second format
            try:
                daily_data['date'] = pd.to_datetime(daily_data['date'], format='%Y-%m-%d')
            except ValueError:
                # If both fail, try a general approach
                try:
                    daily_data['date'] = pd.to_datetime(daily_data['date'], errors='coerce')
                    # Check if we have any valid dates
                    if daily_data['date'].isna().all():
                        logger.warning(f"Could not parse 'date' column in {filepath}")
                        return pd.DataFrame()
                except Exception as e:
                    logger.warning(f"Error parsing 'date' column in {filepath}: {e}")
                    return pd.DataFrame()

        # Try to parse the 'forecast_date' column with the first format
        try:
            daily_data['forecast_date'] = pd.to_datetime(daily_data['forecast_date'], format='%d.%m.%Y')
        except ValueError:
            # If it fails, try the second format
            try:
                daily_data['forecast_date'] = pd.to_datetime(daily_data['forecast_date'], format='%Y-%m-%d')
            except ValueError:
                # If both fail, try a general approach
                try:
                    daily_data['forecast_date'] = pd.to_datetime(daily_data['forecast_date'], errors='coerce')
                    # Check if we have any valid dates
                    if daily_data['forecast_date'].isna().all():
                        logger.warning(f"Could not parse 'forecast_date' column in {filepath}")
                        return pd.DataFrame()
                except Exception as e:
                    logger.warning(f"Error parsing 'forecast_date' column in {filepath}: {e}")
                    return pd.DataFrame()

        return daily_data

    except Exception as e:
        logger.warning(f"Error reading CSV file {filepath}: {e}")
        return pd.DataFrame()

def read_daily_probabilistic_conceptmod_forecasts_pentad(filepath, code, model_long, model_short):
    """
    Reads in forecast results from probabilistic conceptual models for the pentadal forecast.
    Added robust error handling.

    Args:
        filepath (str): The path to the file with the forecast results.
        code (str): The code of the hydropost for which to read the forecast results.
        model_long (str): The long name of the model.
        model_short (str): The short name of the model.

    Returns:
        forecast (pandas.DataFrame): The forecast results or an empty DataFrame if error occurs.
    """
    import logging
    logger = logging.getLogger(__name__)

    try:
        # Read the forecast results
        daily_data = read_csv_with_multiple_date_formats(filepath)

        # If the reading failed, return empty DataFrame
        if daily_data.empty:
            return pd.DataFrame()

        # Only keep the forecast rows for pentadal forecasts
        # Add a column last_day_of_month to daily_data
        daily_data["last_day_of_month"] = daily_data["forecast_date"].apply(fl.get_last_day_of_month)
        daily_data["day_of_month"] = daily_data["forecast_date"].dt.day

        # Keep rows that have forecast_date equal to either 5, 10, 15, 20, 25 or last_day_of_month
        data = daily_data[(daily_data["day_of_month"].isin([5, 10, 15, 20, 25])) | \
                        (daily_data["forecast_date"] == daily_data["last_day_of_month"])].copy()

        # If no data after filtering, return empty DataFrame
        if data.empty:
            logger.warning(f"No pentadal forecast data found for {filepath} after filtering")
            return pd.DataFrame()

        # Add code to the data, cast code to int
        try:
            data.loc[:, "code"] = int(code)
        except ValueError:
            # If code can't be converted to int, use as string
            data.loc[:, "code"] = str(code)

        # Add pentad of the forecasts to the data
        try:
            data.loc[:, "pentad_in_year"] = data["date"].apply(tl.get_pentad_in_year)
        except Exception as e:
            logger.warning(f"Error calculating pentad_in_year: {e}")
            # Try to continue without it
            data.loc[:, "pentad_in_year"] = 0

        # Group by code and forecast_date and pentad_in_year
        # We have to aggregate only data for the first pentad for each forecast date
        try:
            forecast = data \
                .drop(columns=["date", "day_of_month", "last_day_of_month"], errors='ignore') \
                .groupby(["code", "forecast_date", "pentad_in_year"]) \
                .mean() \
                .reset_index()

            # Keep only the first pentad that appears for each forecast_date
            forecast = forecast.groupby(["code", "forecast_date"]).first().reset_index()
        except Exception as e:
            logger.warning(f"Error grouping forecast data: {e}")
            return pd.DataFrame()

        # Rename columns
        columns_to_rename = {}
        if "forecast_date" in forecast.columns:
            columns_to_rename["forecast_date"] = "date"
        if "Q50" in forecast.columns:
            columns_to_rename["Q50"] = "forecasted_discharge"
        if "Q" in forecast.columns:
            columns_to_rename["Q"] = "forecasted_discharge"

        forecast.rename(columns=columns_to_rename, inplace=True)

        # Add model information
        forecast.loc[:, "model_long"] = model_long
        forecast.loc[:, "model_short"] = model_short

        # Recalculate pentad in month and pentad in year
        if "date" in forecast.columns:
            try:
                forecast.loc[:, "pentad_in_month"] = (forecast["date"] + pd.Timedelta(days=1)).apply(tl.get_pentad)
                forecast.loc[:, "pentad_in_year"] = (forecast["date"] + pd.Timedelta(days=1)).apply(tl.get_pentad_in_year)
            except Exception as e:
                logger.warning(f"Error calculating pentad values: {e}")
                # Set default values to avoid further errors
                forecast.loc[:, "pentad_in_month"] = 1
                forecast.loc[:, "pentad_in_year"] = 1

        logger.info(f"Read {len(forecast)} rows of {model_short} forecasts for the pentadal forecast horizon.")
        logger.debug(f"Columns in the {model_short} forecast data: {forecast.columns}")
        logger.debug(f"Read forecast data sample: {forecast.head()}")

        return forecast

    except Exception as e:
        logger.warning(f"Error processing conceptual model forecast data from {filepath}: {e}")
        return pd.DataFrame()
    
def read_daily_probabilistic_conceptmod_forecasts_decade(filepath, code, model_long, model_short):
    """
    Reads in forecast results from probabilistic conceptual models for the pentadal forecast.
    Added robust error handling.

    Args:
        filepath (str): The path to the file with the forecast results.
        code (str): The code of the hydropost for which to read the forecast results.
        model_long (str): The long name of the model.
        model_short (str): The short name of the model.

    Returns:
        forecast (pandas.DataFrame): The forecast results or an empty DataFrame if error occurs.
    """
    import logging
    logger = logging.getLogger(__name__)

    try:
        # Read the forecast results
        daily_data = read_csv_with_multiple_date_formats(filepath)

        # If the reading failed, return empty DataFrame
        if daily_data.empty:
            return pd.DataFrame()

        # Only keep the forecast rows for pentadal forecasts
        # Add a column last_day_of_month to daily_data
        daily_data["last_day_of_month"] = daily_data["forecast_date"].apply(fl.get_last_day_of_month)
        daily_data["day_of_month"] = daily_data["forecast_date"].dt.day

        # Keep rows that have forecast_date equal to either 10, 20 or last_day_of_month
        data = daily_data[(daily_data["day_of_month"].isin([10, 20])) | \
                        (daily_data["forecast_date"] == daily_data["last_day_of_month"])].copy()

        # If no data after filtering, return empty DataFrame
        if data.empty:
            logger.warning(f"No pentadal forecast data found for {filepath} after filtering")
            return pd.DataFrame()

        # Add code to the data, cast code to int
        try:
            data.loc[:, "code"] = int(code)
        except ValueError:
            # If code can't be converted to int, use as string
            data.loc[:, "code"] = str(code)

        # Add pentad of the forecasts to the data
        try:
            data.loc[:, "decad_in_year"] = data["date"].apply(tl.get_decad_in_year)
        except Exception as e:
            logger.warning(f"Error calculating decad_in_year: {e}")
            # Try to continue without it
            data.loc[:, "decad_in_year"] = 0

        # Group by code and forecast_date and pentad_in_year
        # We have to aggregate only data for the first pentad for each forecast date
        try:
            forecast = data \
                .drop(columns=["date", "day_of_month", "last_day_of_month"], errors='ignore') \
                .groupby(["code", "forecast_date", "decad_in_year"]) \
                .mean() \
                .reset_index()

            # Keep only the first pentad that appears for each forecast_date
            forecast = forecast.groupby(["code", "forecast_date"]).first().reset_index()
        except Exception as e:
            logger.warning(f"Error grouping forecast data: {e}")
            return pd.DataFrame()

        # Rename columns
        columns_to_rename = {}
        if "forecast_date" in forecast.columns:
            columns_to_rename["forecast_date"] = "date"
        if "Q50" in forecast.columns:
            columns_to_rename["Q50"] = "forecasted_discharge"
        if "Q" in forecast.columns:
            columns_to_rename["Q"] = "forecasted_discharge"

        forecast.rename(columns=columns_to_rename, inplace=True)

        # Add model information
        forecast.loc[:, "model_long"] = model_long
        forecast.loc[:, "model_short"] = model_short

        # Recalculate pentad in month and pentad in year
        if "date" in forecast.columns:
            try:
                forecast.loc[:, "decad_in_month"] = (forecast["date"] + pd.Timedelta(days=1)).apply(tl.get_decad_in_month)
                forecast.loc[:, "decad_in_year"] = (forecast["date"] + pd.Timedelta(days=1)).apply(tl.get_decad_in_year)
            except Exception as e:
                logger.warning(f"Error calculating pentad values: {e}")
                # Set default values to avoid further errors
                forecast.loc[:, "decad_in_month"] = 1
                forecast.loc[:, "decad_in_year"] = 1

        logger.info(f"Read {len(forecast)} rows of {model_short} forecasts for the decadal forecast horizon.")
        logger.debug(f"Columns in the {model_short} forecast data: {forecast.columns}")
        logger.debug(f"Read forecast data sample: {forecast.head()}")

        return forecast

    except Exception as e:
        logger.warning(f"Error processing conceptual model forecast data from {filepath}: {e}")
        return pd.DataFrame()

def extract_code_from_conceptmod_results_filename(filename):
    """
    Extract the code from the filename.
    Added robust error handling.

    Args:
        filename (str): The filename to extract the code from.

    Returns:
        str: The extracted code or None if extraction fails.
    """
    import re
    import logging
    logger = logging.getLogger(__name__)

    try:
        match = re.search(r'_(\d+)\.csv$', filename)
        if match:
            return match.group(1)
        else:
            logger.warning(f"Could not extract code from filename: {filename}")
            return None
    except Exception as e:
        logger.warning(f"Error extracting code from filename {filename}: {e}")
        return None


def read_conceptual_model_forecast_pentad(filepath):
    """
    Reads the forecast results from the conceptual model for the pentadal
    forecast horizon.

    Args:
    filepath (str): The path to the forecast file.

    Returns:
    forecast (pandas.DataFrame): The forecast results for the pentadal forecast horizon,
                              or an empty DataFrame if file can't be read.
    """
    import logging
    logger = logging.getLogger(__name__)

    # Test if the filepath exists
    if not os.path.exists(filepath):
        logger.warning(f"File {filepath} not found")
        return pd.DataFrame()

    # Get the filename from the filepath
    filename = os.path.basename(filepath)

    # Get the code from the filename
    code = extract_code_from_conceptmod_results_filename(filename)
    if code is None:
        logger.warning(f"Could not extract code from filename {filename}")
        return pd.DataFrame()

    logger.info(f"Reading forecast results from {filename}")
    logger.debug(f"{filepath}")

    try:
        forecast = read_daily_probabilistic_conceptmod_forecasts_pentad(
            filepath,
            code=code,
            model_long="Rainfall runoff assimilation model (RRAM)",
            model_short="RRAM"
        )

        logger.debug(f"Type of forecast: {type(forecast)}")
        logger.debug(f"Columns in forecast: {forecast.columns}")
        logger.debug(f"Head of forecast: {forecast.head()}")

        return forecast

    except Exception as e:
        logger.warning(f"Error reading forecast from {filepath}: {e}")
        return pd.DataFrame()
    
def read_conceptual_model_forecast_decade(filepath):
    """
    Reads the forecast results from the conceptual model for the decadal
    forecast horizon.

    Args:
    filepath (str): The path to the forecast file.

    Returns:
    forecast (pandas.DataFrame): The forecast results for the decadal forecast horizon,
                              or an empty DataFrame if file can't be read.
    """
    import logging
    logger = logging.getLogger(__name__)

    # Test if the filepath exists
    if not os.path.exists(filepath):
        logger.warning(f"File {filepath} not found")
        return pd.DataFrame()

    # Get the filename from the filepath
    filename = os.path.basename(filepath)

    # Get the code from the filename
    code = extract_code_from_conceptmod_results_filename(filename)
    if code is None:
        logger.warning(f"Could not extract code from filename {filename}")
        return pd.DataFrame()

    logger.info(f"Reading forecast results from {filename}")
    logger.debug(f"{filepath}")

    try:
        forecast = read_daily_probabilistic_conceptmod_forecasts_decade(
            filepath,
            code=code,
            model_long="Rainfall runoff assimilation model (RRAM)",
            model_short="RRAM"
        )

        logger.debug(f"Type of forecast: {type(forecast)}")
        logger.debug(f"Columns in forecast: {forecast.columns}")
        logger.debug(f"Head of forecast: {forecast.head()}")

        return forecast

    except Exception as e:
        logger.warning(f"Error reading forecast from {filepath}: {e}")
        return pd.DataFrame()

def get_files_in_subdirectories(directory, pattern):
    """
    Get a list of all files in subdirectories of a directory that match a pattern.
    Added robust error handling.

    Args:
        directory (str): The directory to search in.
        pattern (str): The pattern to match.

    Returns:
        list: A list of files that match the pattern or empty list if error occurs.
    """
    import fnmatch
    import logging
    import os
    logger = logging.getLogger(__name__)

    if not os.path.exists(directory):
        logger.warning(f"Directory {directory} does not exist")
        return []

    if not os.path.isdir(directory):
        logger.warning(f"{directory} is not a directory")
        return []

    try:
        files = []
        for root, _, filenames in os.walk(directory):
            for filename in fnmatch.filter(filenames, pattern):
                full_path = os.path.abspath(os.path.join(root, filename))
                files.append(full_path)

        logger.debug(f"Found {len(files)} files matching {pattern} in {directory}")
        return files

    except Exception as e:
        logger.warning(f"Error searching for files in {directory}: {e}")
        return []

def read_all_conceptual_model_forecasts_pentad():
    """
    From the folder, ieasyhydroforecast_PATH_TO_RESULT, reads all available
    forecast files. Returns an empty DataFrame if no files are found or the
    directory doesn't exist.

    Returns:
    forecasts (pandas.DataFrame): The forecast results from all conceptual models,
                                or an empty DataFrame if none are found.
    """
    import logging
    logger = logging.getLogger(__name__)

    # Get the path to the results directory
    path_to_results_dir = os.getenv("ieasyhydroforecast_PATH_TO_RESULT")

    # If path is not set, return empty DataFrame with warning
    if path_to_results_dir is None:
        logger.warning("Environment variable ieasyhydroforecast_PATH_TO_RESULT is not set. Skipping conceptual model forecasts.")
        return pd.DataFrame()

    # If directory doesn't exist, return empty DataFrame with warning
    if not os.path.exists(path_to_results_dir):
        logger.warning(f"Directory {path_to_results_dir} does not exist. Skipping conceptual model forecasts.")
        return pd.DataFrame()

    # Get a list of operational daily forecast files in subdirectories of path_to_results_dir
    try:
        files = get_files_in_subdirectories(path_to_results_dir, "daily_*.csv")
    except Exception as e:
        logger.warning(f"Error searching for daily_*.csv files in {path_to_results_dir}: {e}")
        files = []

    # If no files found, return empty DataFrame with warning
    if not files:
        logger.warning(f"No daily_*.csv files found in {path_to_results_dir}. Skipping conceptual model forecasts.")
        return pd.DataFrame()

    # Read the forecast results from all files
    forecasts = pd.DataFrame()  # Create an empty DataFrame to hold all forecasts

    for file in files:
        try:
            logger.debug(f"Reading forecast results from {file}")
            forecast = read_conceptual_model_forecast_pentad(file)

            if forecasts.empty:
                forecasts = forecast
            else:
                forecasts = pd.concat([forecasts, forecast])

        except Exception as e:
            logger.warning(f"Error reading forecast from {file}: {e}")
            # Continue to next file instead of failing

    # Also try to read hindcast files
    try:
        hindcast_files = get_files_in_subdirectories(path_to_results_dir, "hindcast_daily_*.csv")
    except Exception as e:
        logger.warning(f"Error searching for hindcast_daily_*.csv files: {e}")
        hindcast_files = []

    # Only read hindcast files if they exist
    if hindcast_files:
        hindcasts = pd.DataFrame()  # Create an empty DataFrame to hold all hindcasts

        # Read the hindcast results from all files
        for hindcast_file in hindcast_files:
            try:
                logger.debug(f"Reading hindcast results from {hindcast_file}")
                hindcast = read_conceptual_model_forecast_pentad(hindcast_file)

                if hindcasts.empty:
                    hindcasts = hindcast
                else:
                    hindcasts = pd.concat([hindcasts, hindcast])

            except Exception as e:
                logger.warning(f"Error reading hindcast from {hindcast_file}: {e}")
                # Continue to next file instead of failing

        # If we have any hindcasts, append them to forecasts
        if not hindcasts.empty:
            if forecasts.empty:
                forecasts = hindcasts
            else:
                # Append hindcasts to forecasts, if there are duplicates, keep the forecast
                # and discard the hindcast
                forecasts = pd.concat([forecasts, hindcasts]).drop_duplicates(
                    subset=["code", "date"], keep="first")

    # If we still have no data, return empty DataFrame with warning
    # If we still have no data, return empty DataFrame with warning
    if forecasts.empty:
        logger.warning("No valid conceptual model forecasts or hindcasts found.")
    else:
        # Save the most recent forecasts to CSV for comparison
        try:
            save_most_recent_forecasts(forecasts, "RRAM")
        except ImportError:
            logger.warning("Could not import save_most_recent_forecasts from src.postprocessing_tools")
        except Exception as e:
            logger.warning(f"Error saving most recent RRAM forecasts: {e}")

    return forecasts

def read_all_conceptual_model_forecasts_decade():
    """
    From the folder, ieasyhydroforecast_PATH_TO_RESULT, reads all available
    forecast files. Returns an empty DataFrame if no files are found or the
    directory doesn't exist.

    Returns:
    forecasts (pandas.DataFrame): The forecast results from all conceptual models,
                                or an empty DataFrame if none are found.
    """
    import logging
    logger = logging.getLogger(__name__)

    # Get the path to the results directory
    path_to_results_dir = os.getenv("ieasyhydroforecast_PATH_TO_RESULT")

    # If path is not set, return empty DataFrame with warning
    if path_to_results_dir is None:
        logger.warning("Environment variable ieasyhydroforecast_PATH_TO_RESULT is not set. Skipping conceptual model forecasts.")
        return pd.DataFrame()

    # If directory doesn't exist, return empty DataFrame with warning
    if not os.path.exists(path_to_results_dir):
        logger.warning(f"Directory {path_to_results_dir} does not exist. Skipping conceptual model forecasts.")
        return pd.DataFrame()

    # Get a list of operational daily forecast files in subdirectories of path_to_results_dir
    try:
        files = get_files_in_subdirectories(path_to_results_dir, "daily_*.csv")
    except Exception as e:
        logger.warning(f"Error searching for daily_*.csv files in {path_to_results_dir}: {e}")
        files = []

    # If no files found, return empty DataFrame with warning
    if not files:
        logger.warning(f"No daily_*.csv files found in {path_to_results_dir}. Skipping conceptual model forecasts.")
        return pd.DataFrame()

    # Read the forecast results from all files
    forecasts = pd.DataFrame()  # Create an empty DataFrame to hold all forecasts

    for file in files:
        try:
            logger.debug(f"Reading forecast results from {file}")
            forecast = read_conceptual_model_forecast_decade(file)

            if forecasts.empty:
                forecasts = forecast
            else:
                forecasts = pd.concat([forecasts, forecast])

        except Exception as e:
            logger.warning(f"Error reading forecast from {file}: {e}")
            # Continue to next file instead of failing

    # Also try to read hindcast files
    try:
        hindcast_files = get_files_in_subdirectories(path_to_results_dir, "hindcast_daily_*.csv")
    except Exception as e:
        logger.warning(f"Error searching for hindcast_daily_*.csv files: {e}")
        hindcast_files = []

    # Only read hindcast files if they exist
    if hindcast_files:
        hindcasts = pd.DataFrame()  # Create an empty DataFrame to hold all hindcasts

        # Read the hindcast results from all files
        for hindcast_file in hindcast_files:
            try:
                logger.debug(f"Reading hindcast results from {hindcast_file}")
                hindcast = read_conceptual_model_forecast_decade(hindcast_file)

                if hindcasts.empty:
                    hindcasts = hindcast
                else:
                    hindcasts = pd.concat([hindcasts, hindcast])

            except Exception as e:
                logger.warning(f"Error reading hindcast from {hindcast_file}: {e}")
                # Continue to next file instead of failing

        # If we have any hindcasts, append them to forecasts
        if not hindcasts.empty:
            if forecasts.empty:
                forecasts = hindcasts
            else:
                # Append hindcasts to forecasts, if there are duplicates, keep the forecast
                # and discard the hindcast
                forecasts = pd.concat([forecasts, hindcasts]).drop_duplicates(
                    subset=["code", "date"], keep="first")

    # If we still have no data, return empty DataFrame with warning
    # If we still have no data, return empty DataFrame with warning
    if forecasts.empty:
        logger.warning("No valid conceptual model forecasts or hindcasts found.")
    else:
        # Save the most recent forecasts to CSV for comparison
        try:
            save_most_recent_forecasts_decade(forecasts, "RRAM")
        except ImportError:
            logger.warning("Could not import save_most_recent_forecasts from src.postprocessing_tools")
        except Exception as e:
            logger.warning(f"Error saving most recent RRAM forecasts: {e}")

    return forecasts

def read_machine_learning_forecasts_pentad(model):
    '''
    Reads forecast results from the machine learning model for the pentadal
    forecast horizon with robust error handling.

    By default, reads from the SAPPHIRE Postprocessing API.
    Set SAPPHIRE_API_ENABLED=false to use CSV files instead.

    Args:
    model (str): The machine learning model to read the forecast results from.
        Allowed values are 'TFT', 'TIDE', 'TSMIXER', and 'ARIMA'.

    Returns:
    pandas.DataFrame: Forecast results or an empty DataFrame if files not found or error occurs.
    '''
    import logging
    logger = logging.getLogger(__name__)

    # Set model-specific parameters
    if model == 'TFT':
        filename = f"pentad_{model}_forecast.csv".format(model=model)
        hindcast_filename = f"{model}_PENTAD_hindcast_daily*.csv".format(model=model)
        model_long = "Temporal Fusion Transformer (TFT)"
        model_short = "TFT"
    elif model == 'TIDE':
        filename = f"pentad_{model}_forecast.csv".format(model=model)
        hindcast_filename = f"{model}_PENTAD_hindcast_daily*.csv".format(model=model)
        model_long = "Time-series Dense Encoder (TiDE)"
        model_short = "TiDE"
    elif model == 'TSMIXER':
        filename = f"pentad_{model}_forecast.csv".format(model=model)
        hindcast_filename = f"{model}_PENTAD_hindcast_daily*.csv".format(model=model)
        model_long = "Time-Series Mixer (TSMixer)"
        model_short = "TSMixer"
    elif model == 'ARIMA':
        filename = f"pentad_{model}_forecast.csv".format(model=model)
        hindcast_filename = f"{model}_PENTAD_hindcast_daily*.csv".format(model=model)
        model_long = "AutoRegressive Integrated Moving Average (ARIMA)"
        model_short = "ARIMA"
    else:
        logger.warning(f"Invalid model: {model}. Valid models are: 'TFT', 'TIDE', 'TSMIXER', 'ARIMA'")
        return pd.DataFrame()

    # Check if API is enabled (default: true)
    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"

    if api_enabled and SAPPHIRE_API_AVAILABLE:
        logger.info(f"Reading {model} pentadal forecasts from SAPPHIRE API (SAPPHIRE_API_ENABLED=true)")
        try:
            # Read from API using the helper function
            forecast = _read_ml_forecasts_from_api(model=model, horizon_type="pentad")

            if not forecast.empty:
                # Save the most recent forecasts to CSV for comparison
                try:
                    save_most_recent_forecasts(forecast, model_short)
                except ImportError:
                    logger.warning(f"Could not import save_most_recent_forecasts from src.postprocessing_tools")
                except Exception as e:
                    logger.warning(f"Error saving most recent {model_short} forecasts: {e}")

                logger.info(f"Read {len(forecast)} rows of {model} forecasts for the pentadal forecast horizon from API.")
                return forecast
            else:
                logger.warning(f"No {model} pentadal forecast data returned from API, falling back to CSV")
                # Fall through to CSV reading

        except Exception as e:
            logger.warning(f"Failed to read {model} pentadal forecasts from API: {e}. Falling back to CSV.")
            # Fall through to CSV reading

    # CSV fallback: Read from file
    logger.info(f"Reading {model} pentadal forecasts from CSV (SAPPHIRE_API_ENABLED=false or API unavailable)")

    # Read environment variables to construct the file path
    intermediate_data_path = os.getenv("ieasyforecast_intermediate_data_path")
    if intermediate_data_path is None:
        logger.warning("Environment variable ieasyforecast_intermediate_data_path is not set")
        return pd.DataFrame()

    subfolder = os.getenv("ieasyhydroforecast_OUTPUT_PATH_DISCHARGE")
    if subfolder is None:
        logger.warning("Environment variable ieasyhydroforecast_OUTPUT_PATH_DISCHARGE is not set")
        return pd.DataFrame()

    filepath = os.path.join(intermediate_data_path, subfolder, model, filename)

    # Test if the filepath exists
    if not os.path.exists(filepath):
        logger.warning(f"File {filepath} not found for model {model}")
        return pd.DataFrame()

    logger.info(f"Reading forecast results from {filename}")
    logger.debug(f"{filepath}")

    try:
        forecast = read_daily_probabilistic_ml_forecasts_pentad(filepath, model, model_long, model_short)
        # Save the most recent forecasts to CSV for comparison
        try:
            save_most_recent_forecasts(forecast, model_short)
        except ImportError:
            logger.warning(f"Could not import save_most_recent_forecasts from src.postprocessing_tools")
        except Exception as e:
            logger.warning(f"Error saving most recent {model_short} forecasts: {e}")

        logger.info(f"Read {len(forecast)} rows of {model} forecasts for the pentadal forecast horizon from CSV.")
        return forecast
    except Exception as e:
        logger.warning(f"Error reading {model} forecast from {filepath}: {e}")
        return pd.DataFrame()

def read_machine_learning_forecasts_decade(model):
    '''
    Reads forecast results from the machine learning model for the decadal
    forecast horizon with robust error handling.

    By default, reads from the SAPPHIRE Postprocessing API.
    Set SAPPHIRE_API_ENABLED=false to use CSV files instead.

    Args:
    model (str): The machine learning model to read the forecast results from.
        Allowed values are 'TFT', 'TIDE', 'TSMIXER', and 'ARIMA'.

    Returns:
    pandas.DataFrame: Forecast results or an empty DataFrame if files not found or error occurs.
    '''
    import logging
    logger = logging.getLogger(__name__)

    # Set model-specific parameters
    if model == 'TFT':
        filename = f"decad_{model}_forecast.csv".format(model=model)
        hindcast_filename = f"{model}_DECAD_hindcast_daily*.csv".format(model=model)
        model_long = "Temporal Fusion Transformer (TFT)"
        model_short = "TFT"
    elif model == 'TIDE':
        filename = f"decad_{model}_forecast.csv".format(model=model)
        hindcast_filename = f"{model}_DECAD_hindcast_daily*.csv".format(model=model)
        model_long = "Time-series Dense Encoder (TiDE)"
        model_short = "TiDE"
    elif model == 'TSMIXER':
        filename = f"decad_{model}_forecast.csv".format(model=model)
        hindcast_filename = f"{model}_DECAD_hindcast_daily*.csv".format(model=model)
        model_long = "Time-Series Mixer (TSMixer)"
        model_short = "TSMixer"
    elif model == 'ARIMA':
        filename = f"decad_{model}_forecast.csv".format(model=model)
        hindcast_filename = f"{model}_DECAD_hindcast_daily*.csv".format(model=model)
        model_long = "AutoRegressive Integrated Moving Average (ARIMA)"
        model_short = "ARIMA"
    else:
        logger.warning(f"Invalid model: {model}. Valid models are: 'TFT', 'TIDE', 'TSMIXER', 'ARIMA'")
        return pd.DataFrame()

    # Check if API is enabled (default: true)
    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"

    if api_enabled and SAPPHIRE_API_AVAILABLE:
        logger.info(f"Reading {model} decadal forecasts from SAPPHIRE API (SAPPHIRE_API_ENABLED=true)")
        try:
            # Read from API using the helper function
            forecast = _read_ml_forecasts_from_api(model=model, horizon_type="decade")

            if not forecast.empty:
                # Save the most recent forecasts to CSV for comparison
                try:
                    save_most_recent_forecasts_decade(forecast, model_short)
                except ImportError:
                    logger.warning(f"Could not import save_most_recent_forecasts_decade from src.postprocessing_tools")
                except Exception as e:
                    logger.warning(f"Error saving most recent {model_short} forecasts: {e}")

                logger.info(f"Read {len(forecast)} rows of {model} forecasts for the decadal forecast horizon from API.")
                return forecast
            else:
                logger.warning(f"No {model} decadal forecast data returned from API, falling back to CSV")
                # Fall through to CSV reading

        except Exception as e:
            logger.warning(f"Failed to read {model} decadal forecasts from API: {e}. Falling back to CSV.")
            # Fall through to CSV reading

    # CSV fallback: Read from file
    logger.info(f"Reading {model} decadal forecasts from CSV (SAPPHIRE_API_ENABLED=false or API unavailable)")

    # Read environment variables to construct the file path
    intermediate_data_path = os.getenv("ieasyforecast_intermediate_data_path")
    if intermediate_data_path is None:
        logger.warning("Environment variable ieasyforecast_intermediate_data_path is not set")
        return pd.DataFrame()

    subfolder = os.getenv("ieasyhydroforecast_OUTPUT_PATH_DISCHARGE")
    if subfolder is None:
        logger.warning("Environment variable ieasyhydroforecast_OUTPUT_PATH_DISCHARGE is not set")
        return pd.DataFrame()

    filepath = os.path.join(intermediate_data_path, subfolder, model, filename)

    # Test if the filepath exists
    if not os.path.exists(filepath):
        logger.warning(f"File {filepath} not found for model {model}")
        return pd.DataFrame()

    logger.info(f"Reading forecast results from {filename}")
    logger.debug(f"{filepath}")

    try:
        forecast = read_daily_probabilistic_ml_forecasts_decade(filepath, model, model_long, model_short)
        # Save the most recent forecasts to CSV for comparison
        try:
            save_most_recent_forecasts_decade(forecast, model_short)
        except ImportError:
            logger.warning(f"Could not import save_most_recent_forecasts_decade from src.postprocessing_tools")
        except Exception as e:
            logger.warning(f"Error saving most recent {model_short} forecasts: {e}")

        logger.info(f"Read {len(forecast)} rows of {model} forecasts for the decadal forecast horizon from CSV.")
        return forecast
    except Exception as e:
        logger.warning(f"Error reading {model} forecast from {filepath}: {e}")
        return pd.DataFrame()

def deprecated_read_linreg_forecasts_pentad_dummy(model):
    """
    Dummy function
    """
    if model == "A":
        filename = os.getenv("ieasyforecast_modelA_pentad_file")
        model_long = "Model A (MA)"
        model_short = "MA"
    elif model == "B":
        filename = os.getenv("ieasyforecast_modelB_pentad_file")
        model_long = "Model B (MB)"
        model_short = "MB"
    else:
        raise ValueError("Invalid model")

    # Read the linear regression forecasts for the pentadal forecast horizon with robust date parsing
    filepath = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        filename
    )
    data = pd.read_csv(filepath)
    
    # Apply robust date parsing and ensure code is string without .0 suffixes
    if 'date' in data.columns:
        data['date'] = fl.parse_dates_robust(data['date'], 'date')
    if 'code' in data.columns:
        data['code'] = data['code'].astype(str).str.replace(r'\.0$', '', regex=True)

    # Drop duplicate rows in date and code if they exist, keeping the last row
    data.drop_duplicates(subset=["date", "code"], keep="last", inplace=True)

    # Add a column model to the dataframe
    data["model_long"] = model_long
    data["model_short"] = model_short

    # Split the data into forecasts and statistics
    # The statistics are general runoff statistics and are later merged to the
    # observed DataFrame
    stats = data[["date", "code", "q_mean", "q_std_sigma", "delta"]]
    forecasts = data.drop(columns=["q_mean", "q_std_sigma", "delta", "discharge_avg"])

    # Add one day to date
    forecasts.loc[:, "date"] = forecasts.loc[:, "date"] + pd.DateOffset(days=1)
    stats.loc[:, "date"] = stats.loc[:, "date"] + pd.DateOffset(days=1)

    # Recalculate pentad in month and pentad in year
    forecasts["pentad_in_month"] = forecasts["date"].apply(tl.get_pentad)
    forecasts["pentad_in_year"] = forecasts["date"].apply(tl.get_pentad_in_year)

    return forecasts, stats

def calculate_neural_ensemble_forecast(forecasts):
    # Define the models we're interested in
    target_models = ['TiDE', 'TFT', 'TSMixer', 'TIDE', 'TSMIXER']

    # Filter forecasts to include only the target models if they exist
    available_target_models = [model for model in target_models if any(forecasts['model_short'].str.contains(model))]

    if not available_target_models:
        logger.warning("None of the specified models (TiDE, TFT, TSMixer) are present in the forecasts.")
        return forecasts

    filtered_forecasts = forecasts[forecasts['model_short'].str.contains('|'.join(available_target_models))]

    # Create a dataframe with unique date and codes from filtered forecasts
    ensemble_mean = filtered_forecasts[["date", "code", "pentad_in_month", "pentad_in_year"]]\
        .drop_duplicates(keep='last').copy()

    # Add model_long and model_short columns to the ensemble_mean dataframe
    model_names = ', '.join(available_target_models)
    ensemble_mean['model_long'] = f"Neural Ensemble with {model_names} (NE)"
    ensemble_mean['model_short'] = f"NE"

    # Calculate the ensemble mean over the filtered models
    ensemble_mean_q = filtered_forecasts \
        .groupby(["date", "code", "pentad_in_month", "pentad_in_year"]) \
            .agg({"forecasted_discharge": "mean"}).reset_index()

    # Merge ensemble_mean_q into ensemble_mean
    ensemble_mean = pd.merge(
        ensemble_mean,
        ensemble_mean_q,
        on=["date", "code", "pentad_in_month", "pentad_in_year"],
        how="left")

    # Append ensemble_mean to original forecasts
    forecasts = pd.concat([forecasts, ensemble_mean])

    logger.info(f"Calculated ensemble forecast for models: {model_names}")
    logger.debug(f"Columns of forecasts:\n{forecasts.columns}")
    logger.debug(f"Forecasts:\n{forecasts.loc[:,['date', 'code', 'model_long', 'forecasted_discharge']].head()}")
    logger.debug(f"Forecasts:\n{forecasts.loc[:,['date', 'code', 'model_long', 'forecasted_discharge']].tail()}")
    logger.debug(f"Unique models in forecasts:\n{forecasts['model_long'].unique()}")

    return forecasts

def calculate_neural_ensemble_forecast_decade(forecasts):
    # Define the models we're interested in
    target_models = ['TiDE', 'TFT', 'TSMixer', 'TIDE', 'TSMIXER']

    # Filter forecasts to include only the target models if they exist
    available_target_models = [model for model in target_models if any(forecasts['model_short'].str.contains(model))]

    if not available_target_models:
        logger.warning("None of the specified models (TiDE, TFT, TSMixer) are present in the forecasts.")
        return forecasts

    filtered_forecasts = forecasts[forecasts['model_short'].str.contains('|'.join(available_target_models))]

    # Create a dataframe with unique date and codes from filtered forecasts
    ensemble_mean = filtered_forecasts[["date", "code", "decad_in_month", "decad_in_year"]]\
        .drop_duplicates(keep='last').copy()

    # Add model_long and model_short columns to the ensemble_mean dataframe
    model_names = ', '.join(available_target_models)
    ensemble_mean['model_long'] = f"Neural Ensemble with {model_names} (NE)"
    ensemble_mean['model_short'] = f"NE"

    # Calculate the ensemble mean over the filtered models
    ensemble_mean_q = filtered_forecasts \
        .groupby(["date", "code", "decad_in_month", "decad_in_year"]) \
            .agg({"forecasted_discharge": "mean"}).reset_index()

    # Merge ensemble_mean_q into ensemble_mean
    ensemble_mean = pd.merge(
        ensemble_mean,
        ensemble_mean_q,
        on=["date", "code", "decad_in_month", "decad_in_year"],
        how="left")

    # Append ensemble_mean to original forecasts
    forecasts = pd.concat([forecasts, ensemble_mean])

    logger.info(f"Calculated decadal ensemble forecast for models: {model_names}")
    logger.debug(f"Columns of forecasts:\n{forecasts.columns}")
    logger.debug(f"Forecasts:\n{forecasts.loc[:,['date', 'code', 'model_long', 'forecasted_discharge']].head()}")
    logger.debug(f"Forecasts:\n{forecasts.loc[:,['date', 'code', 'model_long', 'forecasted_discharge']].tail()}")
    logger.debug(f"Unique models in forecasts:\n{forecasts['model_long'].unique()}")

    return forecasts

def calculate_ensemble_forecast(forecasts):
    # Create a dataframe with unique date and codes from forecasts
    ensemble_mean = forecasts[["date", "code", "pentad_in_month", "pentad_in_year"]]\
        .drop_duplicates(keep='last').copy()

    # Add model_long and model_short columns to the ensemble_mean dataframe
    ensemble_mean['model_long'] = "Ensemble mean (EM)"
    ensemble_mean['model_short'] = "EM"

    # Calculate the ensemble mean over all models
    ensemble_mean_q = forecasts \
        .groupby(["date", "code", "pentad_in_month", "pentad_in_year"]) \
            .agg({"forecasted_discharge": "mean"}).reset_index()

    # Merge ensemble_mean_q into ensemble_mean
    ensemble_mean = pd.merge(
        ensemble_mean,
        ensemble_mean_q,
        on=["date", "code", "pentad_in_month", "pentad_in_year"],
        how="left")

    # Append ensemble_mean to forecasts
    forecasts = pd.concat([forecasts, ensemble_mean])
    logger.info(f"Calculated ensemble forecast for the pentadal forecast horizon.")
    logger.debug(f"Columns of forecasts:\n{forecasts.columns}")
    logger.debug(f"Forecasts:\n{forecasts.loc[:,['date', 'code', 'model_long', 'forecasted_discharge']].head()}")
    logger.debug(f"Forecasts:\n{forecasts.loc[:,['date', 'code', 'model_long', 'forecasted_discharge']].tail()}")
    logger.debug(f"Unique models in forecasts:\n{forecasts['model_long'].unique()}")

    return forecasts

# region Dealing with virtual stations
def add_hydroposts(combined_data, check_hydroposts):
    """
    Check if the virtual hydroposts are in the combined_data and add them if not.

    This function checks if the virtual hydroposts are in the combined_data and
    adds them if they are not.

    Args:
    combined_data (pd.DataFrame): The input DataFrame. Must contain 'code' column.
    check_hydroposts (list): A list of the virtual hydroposts to check for.

    Returns:
    pd.DataFrame: The input DataFrame with the virtual hydroposts added.

    """
    # Get the earliest date for which we have data in the combined_data
    earliest_date = combined_data['date'].min()

    # Check if the virtual hydroposts are in the combined_data
    for hydropost in check_hydroposts:
        if hydropost not in combined_data['code'].values:
            logger.debug(f"Adding virtual hydropost {hydropost} to the list of stations.")
            # Add the virtual hydropost to the combined_data
            new_row = pd.DataFrame({
                'code': [hydropost],
                'date': [earliest_date],
                'discharge': [np.nan],
                #'name': [f'Virtual hydropost {hydropost}']
            })
            combined_data = pd.concat([combined_data, new_row], ignore_index=True)

    return combined_data

def calculate_virtual_stations_data(data_df: pd.DataFrame,
                                    code_col='code', discharge_col='forecasted_discharge',
                                    date_col='date', model_col='model_short'):
    """

    """
    # Test if we have a virtual stations file configured
    if os.getenv('ieasyforecast_virtual_stations') is None:
        logger.warning("No virtual stations file configured. Skipping virtual stations.")
        return data_df

    # Get configuration for virtual stations
    with open(os.path.join(os.getenv('ieasyforecast_configuration_path'),
                           os.getenv('ieasyforecast_virtual_stations')), 'r') as f:
        json_data = json.load(f)
        virtual_stations = json_data['virtualStations'].keys()
        instructions = json_data['virtualStations']

    # Add the virtual stations to the data if they are not already there
    data_df = add_hydroposts(data_df, virtual_stations)

    # Iterate over the station IDs
    for station in virtual_stations:
        # Get the instructions for the station
        instruction = instructions[station]
        #print(instruction)
        weigth_by_station = instruction['weightByStation']
        #print(weigth_by_station)

        # Currently, we only implement the combination function 'sum'. Throw an error if the function is not 'sum'
        if instruction['combinationFunction'] != 'sum':
            logger.error(f"Combination function for station {station} is not 'sum'.")
            logger.error(f"Please implement the combination function '{instruction['combinationFunction']}' for station {station}.")
            exit()

        # Get the data for the stations that contribute to the virtual station and multiply them with the weight
        for contributing_station, weight in weigth_by_station.items():
            # Make sure the contributing station is not equal to the virtual station
            if contributing_station == station:
                logger.error(f"Virtual station {station} cannot contribute to itself.")
                exit()

            #print(contributing_station, weight)
            # Get the data for the contributing station
            data_contributing_station = data_df[data_df[code_col] == contributing_station].copy()

            # Multiply the discharge data with the weight
            data_contributing_station[discharge_col] = data_contributing_station[discharge_col] * weight

            # Add the data to the virtual station if data_virtual_station exists
            if 'data_virtual_station' not in locals():
                data_virtual_station = data_contributing_station
                # Change code to the code of the virtual station
                data_virtual_station[code_col] = station
                # Change the name to the name of the virtual station
                #data_virtual_station['name'] = f'Virtual hydropost {station}'
            else:
                # Merge the data for the contributing station with the data_virtual_station
                data_virtual_station = pd.merge(data_virtual_station, data_contributing_station, on=date_col, how='outer', suffixes=('', '_y'))
                # Add discharge_y to discharge and discard all _y columns
                data_virtual_station[discharge_col] = data_virtual_station[discharge_col] + data_virtual_station[discharge_col + '_y']
                data_virtual_station.drop(columns=[col for col in data_virtual_station.columns if '_y' in col], inplace=True)

            #print("data_virtual_station.tail(10)\n", data_virtual_station.tail(10))

        # Discard the name column
        if 'name' in data_virtual_station.columns:
            data_virtual_station.drop(columns=['name'], inplace=True)

        # Get the number of models in the data_df
        models = data_df[model_col].unique()

        # Check if we already have data for the virtual station in the data_df
        # dataframe and fill gaps with data_virtual_station
        if station in data_df[code_col].values:

            for model in models:
                # Get the data for the virtual station
                data_station = data_df[(data_df[code_col] == station) & (data_df[model_col] == model)].copy()

                # Get the latest date for which we have data in the data_df for the virtual station
                last_date_station = data_station[date_col].max()

                # Get the data for the date from the other stations
                data_virtual_station = data_virtual_station[data_virtual_station[date_col] >= last_date_station].copy()

                # Merge the data for the virtual station with the data_df
                data_df = pd.concat([data_df, data_virtual_station], ignore_index=True)

        # Delete data_virtual_station
        del data_virtual_station

        # Remove rows where the model_long column is nan
        data_df = data_df[data_df['model_long'].notna()]

    return data_df

# endregion
def save_most_recent_forecasts(forecasts, model_name):
    """
    Save the most recent forecasts for a specific model.

    Args:
        forecasts (pd.DataFrame): DataFrame containing forecast data for a model
        model_name (str): Name of the model (e.g., 'LR', 'TFT', 'ARIMA')

    Returns:
        None
    """
    if forecasts.empty:
        logger.warning(f"No forecasts available for model {model_name} to save")
        return

    # Get the most recent date in the dataset
    most_recent_date = forecasts['date'].max()

    # Filter for the most recent date
    recent_forecasts = forecasts[forecasts['date'] == most_recent_date]

    if recent_forecasts.empty:
        logger.warning(f"No recent forecasts available for model {model_name}")
        return

    # Create directory if it doesn't exist
    raw_forecast_dir = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        "raw_forecast_logs"
    )
    os.makedirs(raw_forecast_dir, exist_ok=True)

    # Save the filtered data to CSV
    forecast_file = os.path.join(
        raw_forecast_dir,
        f"raw_{model_name}_ml_pentad_forecasts_{most_recent_date.strftime('%Y%m%d')}.csv"
    )

    # Save relevant columns only
    columns_to_save = ['code', 'date', 'pentad_in_month', 'pentad_in_year',
                     'forecasted_discharge', 'model_short', 'model_long']

    # Only keep columns that exist in the DataFrame
    columns_to_save = [col for col in columns_to_save if col in recent_forecasts.columns]

    # Save to CSV
    recent_forecasts[columns_to_save].to_csv(forecast_file, index=False)

    logger.info(f"Raw {model_name} forecasts saved to: {forecast_file}")
    logger.info(f"Number of stations with {model_name} forecasts: {len(recent_forecasts)}")

def save_most_recent_forecasts_decade(forecasts, model_name):
    """
    Save the most recent forecasts for a specific model.

    Args:
        forecasts (pandas.DataFrame): DataFrame containing forecast data for a model
        model_name (str): Name of the model (e.g., 'LR', 'TFT', 'ARIMA')

    Returns:
        None
    """
    if forecasts.empty:
        logger.warning(f"No forecasts available for model {model_name} to save")
        return

    # Get the most recent date in the dataset
    most_recent_date = forecasts['date'].max()

    # Filter for the most recent date
    recent_forecasts = forecasts[forecasts['date'] == most_recent_date]

    if recent_forecasts.empty:
        logger.warning(f"No recent forecasts available for model {model_name}")
        return

    # Create directory if it doesn't exist
    raw_forecast_dir = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        "raw_forecast_logs"
    )
    os.makedirs(raw_forecast_dir, exist_ok=True)

    # Save the filtered data to CSV
    forecast_file = os.path.join(
        raw_forecast_dir,
        f"raw_{model_name}_ml_decade_forecasts_{most_recent_date.strftime('%Y%m%d')}.csv"
    )

    # Save relevant columns only
    columns_to_save = ['code', 'date', 'pentad_in_month', 'pentad_in_year',
                     'forecasted_discharge', 'model_short', 'model_long']

    # Only keep columns that exist in the DataFrame
    columns_to_save = [col for col in columns_to_save if col in recent_forecasts.columns]

    # Save to CSV
    recent_forecasts[columns_to_save].to_csv(forecast_file, index=False)

    logger.info(f"Raw {model_name} decadal forecasts saved to: {forecast_file}")
    logger.info(f"Number of stations with {model_name} forecasts: {len(recent_forecasts)}")


def read_observed_and_modelled_data_pentade():
    """
    Reads results from all forecast methods into a dataframe.

    Returns:
    forecasts (pandas.DataFrame): The forecasts from all methods.
    """
    # Read the observed data
    observed = read_observed_pentadal_data()

    # Initialize all forecast DataFrames as empty
    linreg = pd.DataFrame()
    tide = pd.DataFrame()
    tft = pd.DataFrame()
    tsmixer = pd.DataFrame()
    arima = pd.DataFrame()
    rrmamba = pd.DataFrame()
    cm = pd.DataFrame()
    stats = pd.DataFrame()

    # Read the linear regression forecasts for the pentadal forecast horizon
    linreg, stats_linreg = read_linreg_forecasts_pentad()
    stats = stats_linreg

    # Debugging prints:
    print(f"\n\n\n\n\n||||  DEBUGGING  -  read_linreg_forecasts_pentad  ||||")
    # Print the latest date in the DataFrame
    latest_date_temp = linreg['date'].max()
    print(f"Latest date in simulated_df: {latest_date_temp}")
    # Print all unique forecast models (model_short) in the DataFrame
    unique_models = linreg['model_short'].unique()
    print(f"Unique forecast models in simulated_df: {unique_models}")
    # Print unique forecast models available for latest date
    latest_models = linreg[linreg['date'] == latest_date_temp]['model_short'].unique()
    print(f"Unique forecast models available for latest date ({latest_date_temp}): {latest_models}")
    print(f"\n\n\n\n\n\n")

    # Learn which modules are activated
    read_ml_results = os.getenv("ieasyhydroforecast_run_ML_models")
    if read_ml_results is None:
        logger.info("Environment variable ieasyhydroforecast_run_ML_models is not set. Assuming no ML forecasts to be read.")
    elif read_ml_results == "False":
        logger.info("Environment variable ieasyhydroforecast_run_ML_models is set to False. No ML forecasts to be read.")
    elif read_ml_results == "True":
        logger.info("Environment variable ieasyhydroforecast_run_ML_models is set to True. Reading ML forecasts.")
        
        # Read available ML models          
        available_ml_models = os.getenv("ieasyhydroforecast_available_ML_models")
        logger.debug(f"Available ML models: {available_ml_models}")
        
        if available_ml_models is None:
            logger.info("Environment variable ieasyhydroforecast_available_ML_models is not set. Assuming no ML models are available.")
            pass
        else:
            available_ml_models = available_ml_models.split(",")
            logger.info(f"Available ML models: {available_ml_models}")
        
        # Read TIDE results if TIDE in available models
        if not 'TIDE' in available_ml_models:        
            logger.debug("No TIDE results to be read. Skipping TIDE.")
        else: 
            tide = read_machine_learning_forecasts_pentad(model='TIDE')
        
        # Read TFT results
        if not 'TFT' in available_ml_models:
            logger.debug("No TFT results to be read. Skipping TFT.")
        else:
            tft = read_machine_learning_forecasts_pentad(model='TFT')
        
        # Read TSMIXER results
        if not 'TSMIXER' in available_ml_models:
            logger.debug("No TSMIXER results to be read. Skipping TSMIXER.")
        else:
            tsmixer = read_machine_learning_forecasts_pentad(model='TSMIXER')
    
        # Read ARIMA results
        if not 'ARIMA' in available_ml_models:
            logger.debug("No ARIMA results to be read. Skipping ARIMA.")
        else: 
            arima = read_machine_learning_forecasts_pentad(model='ARIMA')
        
        # Read RRMAMBA results
        if not 'RRMAMBA' in available_ml_models:
            logger.debug("No RRMAMBA results to be read. Skipping RRMAMBA.")
        else: 
            rrmamba = read_machine_learning_forecasts_pentad(model='RRMAMBA')
    
    else:
        logger.warning("Environment variable ieasyhydroforecast_run_ML_models is set to an invalid value. Assuming no ML forecasts to be read.")
        
    run_cm_models = os.getenv("ieasyhydroforecast_run_CM_models")
    if run_cm_models is None:
        logger.info("Environment variable ieasyhydroforecast_run_CM_models is not set. Assuming no CM forecasts to be read.")
    elif run_cm_models == "False":
        logger.info("Environment variable ieasyhydroforecast_run_CM_models is set to False. No CM forecasts to be read.")
    elif run_cm_models == "True":
        cm = read_all_conceptual_model_forecasts_pentad()
    else: 
        logger.warning("Environment variable ieasyhydroforecast_run_CM_models is set to an invalid value. Assuming no CM forecasts to be read.")
    
    # Only check for NaN values in the model_long column if the DataFrame is not empty
    available_forecasts = []

    # Test if there are any nans in the model long column of either linreg, tide, tft, tsmixer, arima and cm
    if not linreg.empty:
        if 'model_long' in linreg.columns and linreg['model_long'].isnull().values.any():
            logger.error("There are nans in the model_long column of linreg.")
        else:
            available_forecasts.append(linreg)
            
    if not tide.empty:
        if 'model_long' in tide.columns and tide['model_long'].isnull().values.any():
            logger.error("There are nans in the model_long column of tide.")
        else:
            available_forecasts.append(tide)
            
    if not tft.empty:
        if 'model_long' in tft.columns and tft['model_long'].isnull().values.any():
            logger.error("There are nans in the model_long column of tft.")
        else:
            available_forecasts.append(tft)
            
    if not tsmixer.empty:
        if 'model_long' in tsmixer.columns and tsmixer['model_long'].isnull().values.any():
            logger.error("There are nans in the model_long column of tsmixer.")
        else:
            available_forecasts.append(tsmixer)
            
    if not arima.empty:
        if 'model_long' in arima.columns and arima['model_long'].isnull().values.any():
            logger.error("There are nans in the model_long column of arima.")
        else:
            available_forecasts.append(arima)
            
    if not rrmamba.empty:
        if 'model_long' in rrmamba.columns and rrmamba['model_long'].isnull().values.any():
            logger.error("There are nans in the model_long column of rrmamba.")
        else:
            available_forecasts.append(rrmamba)
            
    if not cm.empty:
        if 'model_long' in cm.columns and cm['model_long'].isnull().values.any():
            logger.error("There are nans in the model_long column of cm.")
        else:
            available_forecasts.append(cm)

    # Only concatenate if we have forecasts available
    if available_forecasts:
        forecasts = pd.concat(available_forecasts)
        logger.debug(f"columns of forecasts concatenated:\n{forecasts.columns}")
        logger.debug(f"forecasts concatenated:\n{forecasts.loc[:, ['date', 'code', 'model_long']].head()}\n{forecasts.loc[:, ['date', 'code', 'model_long']].tail()}")

        # Calculate virtual stations forecasts if needed
        forecasts = calculate_virtual_stations_data(forecasts)
        
        # Test if we have any nans in the model_long column
        if 'model_long' in forecasts.columns and forecasts['model_long'].isnull().values.any():
            logger.error("There are nans in the model_long column of forecasts.")
            
        forecasts = calculate_neural_ensemble_forecast(forecasts)
    else: 
        logger.warning("No forecasts available to concatenate. Skipping concatenation.")
        forecasts = pd.DataFrame()

    # Merge the general runoff statistics to the observed DataFrame
    if not stats.empty and not observed.empty:
        observed = pd.merge(observed, stats, on=["date", "code"], how="left")

    return observed, forecasts

def read_observed_and_modelled_data_decade():
    """
    Reads results from all forecast methods into a dataframe.

    Returns:
    forecasts (pandas.DataFrame): The forecasts from all methods.
    """
    # Read the observed data
    observed = read_observed_decadal_data()

    # Initialize all forecast DataFrames as empty
    linreg = pd.DataFrame()
    tide = pd.DataFrame()
    tft = pd.DataFrame()
    tsmixer = pd.DataFrame()
    arima = pd.DataFrame()
    rrmamba = pd.DataFrame()
    cm = pd.DataFrame()
    stats = pd.DataFrame()

    # Read the linear regression forecasts for the decadal forecast horizon
    linreg, stats_linreg = read_linreg_forecasts_decade()
    stats = stats_linreg

    # Learn which modules are activated
    read_ml_results = os.getenv("ieasyhydroforecast_run_ML_models")
    if read_ml_results is None:
        logger.info("Environment variable ieasyhydroforecast_run_ML_models is not set. Assuming no ML forecasts to be read.")
    elif read_ml_results == "False":
        logger.info("Environment variable ieasyhydroforecast_run_ML_models is set to False. No ML forecasts to be read.")
    elif read_ml_results == "True":
        logger.info("Environment variable ieasyhydroforecast_run_ML_models is set to True. Reading ML forecasts.")

        # Read available ML models          
        available_ml_models = os.getenv("ieasyhydroforecast_available_ML_models")
        if available_ml_models is None:
            logger.info("Environment variable ieasyhydroforecast_available_ML_models is not set. Assuming no ML models are available.")
            pass
        else:
            available_ml_models = available_ml_models.split(",")
            logger.info(f"Available ML models: {available_ml_models}")
        
        # Read TIDE results
        if not 'TIDE' in available_ml_models:
            logger.debug("No TIDE results to be read. Skipping TIDE.")
        else: 
            tide = read_machine_learning_forecasts_decade(model='TIDE')
        
        # Read TFT results
        if not 'TFT' in available_ml_models:
            logger.debug("No TFT results to be read. Skipping TFT.")
        else:
            tft = read_machine_learning_forecasts_decade(model='TFT')
        
        # Read TSMIXER results
        if not 'TSMIXER' in available_ml_models:
            logger.debug("No TSMIXER results to be read. Skipping TSMIXER.")
        else:
            tsmixer = read_machine_learning_forecasts_decade(model='TSMIXER')
    
        # Read ARIMA results
        if not 'ARIMA' in available_ml_models:
            logger.debug("No ARIMA results to be read. Skipping ARIMA.")
        else: 
            arima = read_machine_learning_forecasts_decade(model='ARIMA')
        
        # Read RRMAMBA results
        if not 'RRMAMBA' in available_ml_models:
            logger.debug("No RRMAMBA results to be read. Skipping RRMAMBA.")
        else: 
            rrmamba = read_machine_learning_forecasts_decade(model='RRMAMBA')
    
    else:
        logger.warning("Environment variable ieasyhydroforecast_run_ML_models is set to an invalid value. Assuming no ML forecasts to be read.")
        
    run_cm_models = os.getenv("ieasyhydroforecast_run_CM_models")
    if run_cm_models is None:
        logger.info("Environment variable ieasyhydroforecast_run_CM_models is not set. Assuming no CM forecasts to be read.")
    elif run_cm_models == "False":
        logger.info("Environment variable ieasyhydroforecast_run_CM_models is set to False. No CM forecasts to be read.")
    elif run_cm_models == "True":
        cm = read_all_conceptual_model_forecasts_decade()
    else: 
        logger.warning("Environment variable ieasyhydroforecast_run_CM_models is set to an invalid value. Assuming no CM forecasts to be read.")
    
    # Only check for NaN values in the model_long column if the DataFrame is not empty
    available_forecasts = []

    # Test if there are any nans in the model long column of either linreg, tide, tft, tsmixer, arima and cm
    if not linreg.empty:
        if 'model_long' in linreg.columns and linreg['model_long'].isnull().values.any():
            logger.error("There are nans in the model_long column of linreg.")
        else:
            available_forecasts.append(linreg)
            
    if not tide.empty:
        if 'model_long' in tide.columns and tide['model_long'].isnull().values.any():
            logger.error("There are nans in the model_long column of tide.")
        else:
            available_forecasts.append(tide)
            
    if not tft.empty:
        if 'model_long' in tft.columns and tft['model_long'].isnull().values.any():
            logger.error("There are nans in the model_long column of tft.")
        else:
            available_forecasts.append(tft)
            
    if not tsmixer.empty:
        if 'model_long' in tsmixer.columns and tsmixer['model_long'].isnull().values.any():
            logger.error("There are nans in the model_long column of tsmixer.")
        else:
            available_forecasts.append(tsmixer)
            
    if not arima.empty:
        if 'model_long' in arima.columns and arima['model_long'].isnull().values.any():
            logger.error("There are nans in the model_long column of arima.")
        else:
            available_forecasts.append(arima)
            
    if not rrmamba.empty:
        if 'model_long' in rrmamba.columns and rrmamba['model_long'].isnull().values.any():
            logger.error("There are nans in the model_long column of rrmamba.")
        else:
            available_forecasts.append(rrmamba)
            
    if not cm.empty:
        if 'model_long' in cm.columns and cm['model_long'].isnull().values.any():
            logger.error("There are nans in the model_long column of cm.")
        else:
            available_forecasts.append(cm)

    # Only concatenate if we have forecasts available
    if available_forecasts:
        forecasts = pd.concat(available_forecasts)
        logger.debug(f"columns of forecasts concatenated:\n{forecasts.columns}")
        logger.debug(f"forecasts concatenated:\n{forecasts.loc[:, ['date', 'code', 'model_long']].head()}\n{forecasts.loc[:, ['date', 'code', 'model_long']].tail()}")

        # Calculate virtual stations forecasts if needed
        forecasts = calculate_virtual_stations_data(forecasts)
        
        # Test if we have any nans in the model_long column
        if 'model_long' in forecasts.columns and forecasts['model_long'].isnull().values.any():
            logger.error("There are nans in the model_long column of forecasts.")
            
        forecasts = calculate_neural_ensemble_forecast_decade(forecasts)
    else: 
        logger.warning("No forecasts available to concatenate. Skipping concatenation.")
        forecasts = pd.DataFrame()

    # Merge the general runoff statistics to the observed DataFrame
    if not stats.empty and not observed.empty:
        observed = pd.merge(observed, stats, on=["date", "code"], how="left")

    return observed, forecasts

'''def read_observed_and_modelled_data_decade():
    """
    Reads results from all forecast methods into a dataframe.

    Returns:
    forecasts (pandas.DataFrame): The forecasts from all methods.
    """
    # Read the observed data
    observed = read_observed_decadal_data()

    # Read the linear regression forecasts for the pentadal forecast horizon
    linreg, stats_linreg = read_linreg_forecasts_decade()

    # Read the forecasts from the other methods
    tide = read_machine_learning_forecasts_decade(model='TIDE')
    tft = read_machine_learning_forecasts_decade(model='TFT')
    tsmixer = read_machine_learning_forecasts_decade(model='TSMIXER')
    arima = read_machine_learning_forecasts_decade(model='ARIMA')
    cm = read_all_conceptual_model_forecasts_decade()

    #logger.debug(f"type of code in linreg: {linreg['code'].dtype}")
    #logger.debug(f"type of code in tide: {tide['code'].dtype}")
    #logger.debug(f"type of code in cm: {cm['code'].dtype}")

    # Test if there are any nans in the model long column of either linreg, tide, tft, tsmixer, arima and cm
    if linreg['model_long'].isnull().values.any():
        logger.error("There are nans in the model_long column of linreg.")
        exit()
    if tide['model_long'].isnull().values.any():
        logger.error("There are nans in the model_long column of tide.")
        exit()
    if tft['model_long'].isnull().values.any():
        logger.error("There are nans in the model_long column of tft.")
        exit()
    if tsmixer['model_long'].isnull().values.any():
        logger.error("There are nans in the model_long column of tsmixer.")
        exit()
    if arima['model_long'].isnull().values.any():
        logger.error("There are nans in the model_long column of arima.")
        exit()
    if cm['model_long'].isnull().values.any():
        logger.error("There are nans in the model_long column of cm.")
        exit()

    # Merge tide, tft, tsmixer and arima into linreg.
    # same columns are: date, code, pentad_in_month, pentad_in_year,
    # forecasted_discharge, model_long and model_short
    forecasts = pd.concat([linreg, tide, tft, tsmixer, arima, cm])
    #logger.debug(f"columns of forecasts concatenated:\n{forecasts.columns}")
    #logger.debug(f"forecasts concatenated:\n{forecasts.loc[:, ['date', 'code', 'model_long']].head()}\n{forecasts.loc[:, ['date', 'code', 'model_long']].tail()}")
    
    # Calculate virtual stations forecasts if needed
    forecasts = calculate_virtual_stations_data(forecasts)
    # Test if we have any nans in the model_long column
    if forecasts['model_long'].isnull().values.any():
        logger.error("There are nans in the model_long column of forecasts.")
        exit()

    stats = stats_linreg
    #logger.debug(f"columns of stats concatenated:\n{stats.columns}")
    #logger.debug(f"stats concatenated:\n{stats.head()}\n{stats.tail()}")
    #logger.info(f"Concatenated forecast results from all methods for the pentadal forecast horizon.")

    forecasts = calculate_neural_ensemble_forecast_decade(forecasts)

    # Merge the general runoff statistics to the observed DataFrame
    observed = pd.merge(observed, stats, on=["date", "code"], how="left")

    return observed, forecasts'''

# endregion

# --- Classes ----------------------------------------------------------
# region classes

class ForecastFlags:
    """
    Class to store the forecast flags. We have flags for each forecast horizon.
    Depending on the date the forecast tools are called for, they identify the
    forecast horizons to service by changing the respective flag from False (no
    forecast produced) to True (forecast produced).

    Example:
    # Set flags for daily and pentad forecasts
    flags = ForecastFlags(day=True, pentad=True)
    """
    def __init__(self, day=False, pentad=False, decad=False, month=False, season=False):
        self.day = day
        self.pentad = pentad
        self.decad = decad
        self.month = month
        self.season = season

    def __repr__(self):
        return f"day:{self.day}, pentad:{self.pentad}, decad:{self.decad}, month:{self.month}, season:{self.season}"

    @classmethod
    def from_forecast_date_get_flags(cls, start_date):

        forecast_flags = cls()

        # Get the day of the month
        day = start_date.day
        # Get the last day of the month
        last_day = fl.get_last_day_of_month(start_date)
        # Get the list of days for which we want to run the forecast
        # pentadal forecasting
        days_pentads = [5, 10, 15, 20, 25, last_day.day]
        # decadal forecasting
        days_decads = [10, 20, last_day.day]

        # If today is not in days, exit the program.
        if day in days_pentads:
            logger.info(f"Running pentadal forecast on {start_date}.")
            forecast_flags.pentad = True
            if day in days_decads:
                logger.info(f"Running decad forecast on {start_date}.")
                forecast_flags.decad = True

        return forecast_flags

# endregion




