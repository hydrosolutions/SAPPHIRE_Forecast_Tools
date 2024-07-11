import os
import logging
import pandas as pd
import numpy as np
import json
import datetime as dt

from dotenv import load_dotenv

# Import iEasyHydroForecast libraries
import forecast_library as fl
import tag_library as tl

logger = logging.getLogger(__name__)

# === Tools for the initialization of the linear regression forecast ===

# --- Load runtime environment ---------------------------------------------------
# region environment

def store_last_successful_run_date(date):
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
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_last_successful_run_file")
    )

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

def get_last_run_date():
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

def define_run_dates():
    """
    Identifies the start and end dates for the current call to the linear
    regression tool.

    Returns:
    date_start (datetime.date): The start date for the forecast. In operational mode this is today.
    date_end (datetime.date): The end date for the forecast. In operational mode this is today.
    bulletin_date (datetime.date): The bulletin date is the first day of the period for which the forecast is produced. Typically tomorrow.
    """
    # The last successful run date is the last time, the forecast tools were
    # run successfully. This is typically yesterday.
    last_successful_run_date = get_last_run_date()

    # The day on which the forecast is produced. In operational mode, this is
    # day 0 or today. However, the tools can also be run in hindcast mode by
    # setting the last successful run date to a date in the past. In this case,
    # the forecast is produced for the day after the last successful run date.
    date_start = last_successful_run_date + dt.timedelta(days=1)

    # The last day for which a forecast is produced. This is always today.
    date_end = dt.date.today()

    # Basic sanity check in case the script is run multiple times.
    if date_end == last_successful_run_date:
        logger.info("The forecasts have allready been produced for today. "
                       "No forecast will be produced."
                       "Please use the re-run forecast tool to re-run the forecast for today.")
        return None, None, None

    # The bulletin date is one day after the forecast date. It is the first day
    # of the preiod for which the forecast is produced.
    bulletin_date = date_start + dt.timedelta(days=1)

    logger.info("Running the forecast script for the following dates:")
    logger.info(f"Last successful run date: {last_successful_run_date}")
    logger.info(f"Current forecast start date for forecast iteration: {date_start}")
    logger.info(f"End date for forecast iteration: {date_end}")
    logger.info(f"Current forecast bulletin date: {bulletin_date}")

    return date_start, date_end, bulletin_date

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

# endregion


# --- Tools for accessing the iEasyHydro DB --------------------------------------
# region iEH_DB
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
        test = ieh_sdk.get_discharge_sites()
        logger.info(f"Access to iEasyHydro database.")
        return True
    except Exception as e:
        # Test if there are any files in the data/daily_runoff directory
        if os.listdir(os.getenv("ieasyforecast_daily_discharge_path")):
            logger.info(f"No access to iEasyHydro database. "
                        f"Will use data from the ieasyforecast_daily_discharge_path for forecasting only.")
            return False
        else:
            logger.error(f"SAPPHIRE tools do not find any data in the ieasyforecast_daily_discharge_path directory "
                         f"nor does it have access to the iEasyHydro database.")
            logger.error(f"Please check the ieasyforecast_daily_discharge_path directory and/or the access to the iEasyHydro database.")
            logger.error(f"Error connecting to DB: {e}")
            raise e

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
        restrict_stations = False
    else:
        # Read the stations filter from the file
        config_restrict_station_file = os.path.join(
            os.getenv("ieasyforecast_configuration_path"),
            os.getenv("ieasyforecast_restrict_stations_file"))
        with open(config_restrict_station_file, "r") as json_file:
            restrict_stations_config = json.load(json_file)
            restrict_stations = restrict_stations_config["stationsID"]
            logger.warning(f"Station selection for pentadal forecasting restricted to: ...")
            logger.warning(f"{restrict_stations}.")
            logger.warning(f" To remove restriction set ieasyforecast_restrict_stations_file in your .env file to null.")

    # Only keep stations that are in the file ieasyforecast_restrict_stations_file
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
    db_sites = db_sites.applymap(lambda x: x[0] if isinstance(x, list) else x)

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
        restrict_stations = False
    else:
        # Read the stations filter from the file
        config_restrict_station_file = os.path.join(
            os.getenv("ieasyforecast_configuration_path"),
            os.getenv("ieasyforecast_restrict_stations_decad_file"))
        with open(config_restrict_station_file, "r") as json_file:
            restrict_stations_config = json.load(json_file)
            restrict_stations = restrict_stations_config["stationsID"]
            logger.warning(f"Station selection for decadal forecasting restricted to: ...")
            logger.warning(f"{restrict_stations}")
            logger.warning(f"To remove restriction set ieasyforecast_restrict_stations_decad_file in your .env file to null.")

    # Only keep stations that are in the file ieasyforecast_restrict_stations_file
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
    logger.debug("-Filtering fc_sites_pentad for stations ...")
    fc_sites_decad = [site for site in fc_sites_pentad if site.code in stations]
    logger.debug(f"   Producing forecasts for {len(fc_sites_decad)} station(s), namely\n: {[site.code for site in fc_sites_decad]}")

    return fc_sites_decad, stations

# endregion

# --- Reading of forecast results ------------------------------------------------
# region Reading_forecast_results

def read_observed_pentadal_data():
    """
    Read the pentadal hydrograph data.

    Returns:
    data (pandas.DataFrame): The pentadal data.

    Details:
    The file to read is specified in the environment variable
    ieasyforecast_daily_discharge_file. It is expected to have a column 'date'
    with the date of the hydrograph data. If the file has a column 'pentad', it
    is renamed to 'pentad_in_month'.
    """
    # Read the pentadal hydrograph data
    filepath = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_pentad_discharge_file")
    )
    data = pd.read_csv(filepath, parse_dates=["date"])

    # Add a column model to the dataframe
    data["model_long"] = "Observed (Obs)"
    data["model_short"] = "Obs"

    # If there is a column name 'pentad', rename it to 'pentad_in_month'
    if 'pentad' in data.columns:
        data.rename(columns={'pentad': 'pentad_in_month'}, inplace=True)
    logger.info(f"Read {len(data)} rows of observed data for the pentadal forecast horizon.")

    return data

def read_linreg_forecasts_pentad():
    """
    Read the linear regression forecasts for the pentadal forecast horizon and
    adds the name of the model to the DataFrame.

    Since the linreg result file currently holds some general runoff statistics,
    we need to filter these out and return them in a separate DataFrame.

    Returns:
    forecasts (pandas.DataFrame): The linear regression forecasts for the
        pentadal forecast horizon with added model_long and model_short columns.
    stats (pandas.DataFrame): The statistics of the observed data for the
        pentadal forecast horizon.

    Details:
    The file to read is specified in the environment variable
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
    # Read the linear regression forecasts for the pentadal forecast horizon
    filepath = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_analysis_pentad_file")
    )
    data = pd.read_csv(filepath, parse_dates=["date"])

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
    forecasts.loc[:, "date"] = forecasts.loc[:, "date"] + pd.DateOffset(days=1)
    stats.loc[:, "date"] = stats.loc[:, "date"] + pd.DateOffset(days=1)

    # Recalculate pentad in month and pentad in year
    forecasts["pentad_in_month"] = forecasts["date"].apply(tl.get_pentad)
    forecasts["pentad_in_year"] = forecasts["date"].apply(tl.get_pentad_in_year)

    logger.info(f"Read {len(forecasts)} rows of linear regression forecasts for the pentadal forecast horizon.")
    logger.info(f"Read {len(stats)} rows of general runoff statistics for the pentadal forecast horizon.")

    return forecasts, stats

def read_linreg_forecasts_pentad_dummy(model):
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

    # Read the linear regression forecasts for the pentadal forecast horizon
    filepath = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        filename
    )
    data = pd.read_csv(filepath, parse_dates=["date"])

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

def calculate_ensemble_forecast(forecasts):
    # Create a dataframe with unique date and codes from forecasts
    ensemble_mean = forecasts[["date", "code", "pentad_in_month", "pentad_in_year"]].drop_duplicates()

    # Add model_long and model_short columns to the ensemble_mean dataframe
    ensemble_mean['model_long'] = "Ensemble mean (EM)"
    ensemble_mean['model_short'] = "EM"

    # Calculate the ensemble mean over all models
    ensemble_mean_q = forecasts.groupby(["date", "code"]).agg({"forecasted_discharge": "mean"}).reset_index()

    # Merge ensemble_mean_q into ensemble_mean
    ensemble_mean = pd.merge(ensemble_mean, ensemble_mean_q, on=["date", "code"], how="left")

    # Add ensemble_mean to forecasts
    forecasts = pd.concat([forecasts, ensemble_mean])
    logger.info(f"Calculated ensemble forecast for the pentadal forecast horizon.")

    return forecasts

def read_observed_and_modelled_data_pentade():
    """
    Reads results from all forecast methods into a dataframe.

    Returns:
    forecasts (pandas.DataFrame): The forecasts from all methods.
    """
    # Read the observed data
    observed = read_observed_pentadal_data()

    # Read the linear regression forecasts for the pentadal forecast horizon
    linreg, stats_linreg = read_linreg_forecasts_pentad()

    # Read the forecasts from the other methods
    # TODO
    # For now, we read dummy data
    modelA, statsA = read_linreg_forecasts_pentad_dummy(model='A')
    modelB, statsB = read_linreg_forecasts_pentad_dummy(model='B')
    logger.info(f"Read {len(modelA)} rows of model A forecasts for the pentadal forecast horizon.")
    logger.info(f"Read {len(modelB)} rows of model B forecasts for the pentadal forecast horizon.")

    # Concatenate the dataframes
    forecasts = pd.concat([linreg, modelA, modelB])
    stats = pd.concat([stats_linreg, statsA, statsB])
    logger.info(f"Concatenated forecast results from all methods for the pentadal forecast horizon.")

    forecasts = calculate_ensemble_forecast(forecasts)

    # Merge the general runoff statistics to the observed DataFrame
    observed = pd.merge(observed, stats, on=["date", "code"], how="left")

    return observed, forecasts

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




