import logging
import os
import json
import pandas as pd
import numpy as np

# Import the modules from the forecast library
import tag_library as tl
import forecast_library as fl
import datetime
logger = logging.getLogger(__name__)


class PredictorDates:
    """
    Store lists of predictor dates, depending on the forecast horizons which are
    active.
    """
    def __init__(self, pentad=[], decad=[], month=[], season=[]):
        self.pentad = pentad
        self.decad = decad
        self.month = month
        self.season = season

    def __repr__(self):
        return f"PredictorDates(pentad={self.pentad}, decad={self.decad}, month={self.month}, season={self.season})"


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

    # Test if another env variable is set
    if os.getenv("ieasyforecast_gis_directory_path") is None:
        logger.error("The environment variable ieasyforecast_gis_directory_path is not set. Please set it.")
        raise EnvironmentError("Environment variable not set")
    # Check environment variable
    if os.getenv("ieasyforecast_daily_discharge_path") is None:
        logger.error("data_processing.check_database_access(): The environment variable ieasyforecast_daily_discharge_dir is not set. Please set it.")
        raise EnvironmentError("Environment variable not set")
    daily_discharge_dir = os.getenv("ieasyforecast_daily_discharge_path")
    if daily_discharge_dir is None:
        raise EnvironmentError("Environment variable not set")

    # Test if the backand has access to an iEasyHydro database and set a flag accordingly.
    try:
        ieh_sdk.get_discharge_sites()
        logger.info(f"SAPPHIRE forecast tools will use operational data from the iEasyHydro database.")
        return True
    except Exception as e:
        # Test if there are any files in the data/daily_runoff directory
        if os.listdir(os.getenv("ieasyforecast_daily_discharge_path")):
            logger.info(f"SAPPHIRE forecast tools does not have access to the iEasyHydro database "
                        f"and will use data from the data/daily_runoff directory for forecasting only.")
            return False
        else:
            logger.error(f"SAPPHIRE tools do not find any data in the data/daily_runoff directory "
                         f"nor does it have access to the iEasyHydro database.")
            logger.error(f"Please check the data/daily_runoff directory and/or the access to the iEasyHydro database.")
            logger.error(f"Error connecting to DB: {e}")
            raise e


def get_db_sites(ieh_sdk, backend_has_access_to_db):
    # === Read forecast configuration ===
    # Read forecast configuration
    logger.info("Reading forecasting configuration ...")

    # Read station metadata from the DB and store it in a list of Site objects
    logger.info("-Reading station metadata from the DB ...")

    # Read the station details from API
    # Only do this if we have access to the database
    if backend_has_access_to_db:
        try:
            db_sites = ieh_sdk.get_discharge_sites()
        except Exception as e:
            logger.error(f"Error connecting to DB: {e}")
            raise e
        db_sites = pd.DataFrame.from_dict(db_sites)

        logger.info(f"   {len(db_sites)} station(s) in DB, namely:\n{db_sites['site_code'].values}")
    else:
        # If we don't have access to the database, create an empty dataframe.
        db_sites = pd.DataFrame(
            columns=['site_code', 'site_name', 'river_name', 'punkt_name',
                     'latitude', 'longitude', 'region', 'basin'])

    # Read station information of all available discharge stations
    logger.info("-Reading information about all stations from JSON...")

    config_all_file = os.path.join(
        os.getenv("ieasyforecast_configuration_path"),
        os.getenv("ieasyforecast_config_file_all_stations"))

    config_all = fl.load_all_station_data_from_JSON(config_all_file)

    logger.info(f"   {len(config_all)} discharge station(s) found, namely\n{config_all['code'].values}")

    # Merge information from db_sites and config_all. Make sure that all sites
    # in config_all are present in db_sites.
    if backend_has_access_to_db:
        logger.info("-Merging information from db_sites and config_all ...")

        # Find sites in config_all which are not present in db_sites.
        # This is a special case for Kygryz Hydromet.
        new_sites = config_all[~config_all['site_code'].isin(db_sites['site_code'])]

        # Add new sites (virtual station for inflow to Toktogul reservoir) to forecast_sites.
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
    # NOTE: Important assumption: All discharge sites have a code starting with 1.
    db_sites = db_sites[db_sites['site_code'].astype(str).str.startswith('1')]

    # Read stations for forecasting
    logger.info("-Reading stations for forecasting ...")

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
            logger.warning(f"Station selection for forecasting restricted to: {restrict_stations}. To remove "
                           f"restriction set ieasyforecast_restrict_stations_file in .env or .env_develop to null.")

    # Only keep stations that are in the file ieasyforecast_restrict_stations_file
    stations = [station for station in stations if station in restrict_stations]

    logger.info(f"   {len(stations)} station(s) selected for forecasting, namely: {stations}")

    # Filter db_sites for stations
    logger.info("-Filtering db_sites for stations ...")
    stations_str = [str(station) for station in stations]
    db_sites = db_sites[db_sites["site_code"].isin(stations_str)]
    logger.info(f"   Producing forecasts for {len(db_sites)} station(s), namely: {db_sites['site_code'].values}")
    return db_sites


def get_fc_sites(ieh_sdk, backend_has_access_to_db, db_sites):
    # Formatting db_sites to a list of Sites objects
    logger.info("-Formatting db_sites to a list of Sites objects ...")

    # Make sure the entries are not lists
    db_sites = db_sites.applymap(lambda x: x[0] if isinstance(x, list) else x)

    fc_sites = fl.Site.from_dataframe(
        db_sites[["site_code", "site_name", "river_ru", "punkt_ru", "latitude", "longitude", "region", "basin"]]
    )
    logger.info(f' {len(fc_sites)} Site object(s) created for forecasting, namely:\n{[site.code for site in fc_sites]}')

    # Get dangerous discharge for each site
    # This can be done only if we have access to the database
    if backend_has_access_to_db:
        logger.info("-Getting dangerous discharge from DB ...")
        # Call the from_DB_get_dangerous_discharge method on each Site object
        for site in fc_sites:
            fl.Site.from_DB_get_dangerous_discharge(ieh_sdk, site)

        logger.warning(f"   {len(fc_sites)} Dangerous discharge gotten from DB, namely:\n"
                       f"{[site.qdanger for site in fc_sites]}")
    else:
        # Assign " " to qdanger for each site
        for site in fc_sites:
            site.qdanger = " "
        logger.info("No access to iEasyHydro database. Therefore no dangerous discharge is assigned to sites.")
    return fc_sites

def get_predictor_dates(start_date, forecast_flags):
    """
    Gets dates for which to aggregate the predictors for the linear regression
    method.

    Details:
        For pentadal forecasts, the hydromet uses the sum of the last 2 days
        discharge plus the morning discharge of today.

    Arguments:
        start_date (datetime.date) Date on which the forecast is produced.
        forecast_flags (config.ForecastFlags) Flags that identify the forecast
            horizons serviced on start_date

    Return:
        list: A list of dates for which to aggregate the predictors for the
            linear regression method.
    """
    # Initialise the predictor_dates object
    predictor_dates = PredictorDates()
    # Get the dates to get the predictor from
    if forecast_flags.pentad:
        # For pentadal forecasts, the hydromet uses the sum of the last 2 days discharge.
        predictor_dates.pentad = fl.get_predictor_dates(start_date.strftime('%Y-%m-%d'), 3)
        # if predictor_dates is None, raise an error
        if predictor_dates.pentad is None:
            raise ValueError("The predictor dates are not valid.")
    if forecast_flags.decad:
        # For decad forecasts, the hydromet uses the average of the last 10 days discharge.
        predictor_dates.decad = fl.get_predictor_dates(start_date.strftime('%Y-%m-%d'), 10)
        # if predictor_dates is None, raise an error
        if predictor_dates.decad is None:
            raise ValueError("The predictor dates are not valid.")

    logger.debug(f"   Predictor dates for pentadal forecasts: {predictor_dates.pentad}")
    logger.debug(f"   Predictor dates for decad forecasts: {predictor_dates.decad}")

    logger.info("   ... done")
    return predictor_dates

def get_predictor_datetimes(start_date, forecast_flags):
    """
    Gets datetimes for which to aggregate the predictors for the linear regression
    method.

    Details:
        For pentadal forecasts, the hydromet uses the sum of the last 2 days
        discharge plus the morning discharge of today.

    Arguments:
        start_date (datetime.date) Date on which the forecast is produced.
        forecast_flags (config.ForecastFlags) Flags that identify the forecast
            horizons serviced on start_date

    Return:
        list: A list of dates for which to aggregate the predictors for the
            linear regression method.
    """
    # Initialise the predictor_dates object
    predictor_dates = PredictorDates()
    # Get the dates to get the predictor from
    if forecast_flags.pentad:
        # For pentadal forecasts, the hydromet uses the sum of the last 2 days discharge.
        predictor_dates.pentad = fl.get_predictor_datetimes(start_date.strftime('%Y-%m-%d'), 2)
        # if predictor_dates is None, raise an error
        if predictor_dates.pentad is None:
            raise ValueError("The predictor dates are not valid.")
    if forecast_flags.decad:
        # For decad forecasts, the hydromet uses the average of the last 10 days discharge.
        predictor_dates.decad = fl.get_predictor_datetimes(start_date.strftime('%Y-%m-%d'), 9)
        # if predictor_dates is None, raise an error
        if predictor_dates.decad is None:
            raise ValueError("The predictor dates are not valid.")

    logger.debug(f"   Predictor dates for pentadal forecasts: {predictor_dates.pentad}")
    logger.debug(f"   Predictor dates for decad forecasts: {predictor_dates.decad}")

    logger.info("   ... done")
    return predictor_dates

def read_discharge_from_excel_sheet(file_path, station, year):
    """
    Read discharge data from a single Excel sheet. The sheet is named after the
    year for which the data is available.

    Args:
        file_path (str): The path to the Excel file.
        station (str): The station code.
        year (int): The year for which the data is available.

    Returns:
        data (pd.DataFrame): A DataFrame with the discharge data. With columns
            'Date', 'Q_m3s', 'Year', and 'Code' of types 'datetime', 'float', 'int',
            and 'string'.

    Raises:
        ValueError: If the file_path is not a valid path.
        ValueError: If the file_path is not a valid Excel file.
        ValueError: If the file_path does not exist.
        ValueError: If the dates are not parsed correctly from the sheet.
        ValueError: If the first date is not January 1 of the year.
    """
    # Print current working directory
    #print("current working directory: ", os.getcwd())

    # Check if file_path is a valid path
    if not os.path.exists(file_path):
        raise ValueError(f"The file {file_path} does not exist.")

    # Check if the file_path is a valid Excel file
    _, ext = os.path.splitext(file_path)
    if ext.lower() != '.xlsx':
        raise ValueError(f"The file {file_path} is not an Excel file.")

    # Try to read the Excel file
    try:
        df = pd.read_excel(
            file_path, sheet_name=str(year), header=[0],
            names=['Date', 'Q_m3s'], parse_dates=['Date']
        )
    except FileNotFoundError:
        raise ValueError(f"Could not find file {file_path}.")
    except pd.errors.ParserError:
        raise ValueError(f"Could not parse file {file_path}. Please verify format.")

    # Check if dates are parsed correctly
    if df['Date'].dtype != 'datetime64[ns]':
        raise ValueError(f"Dates are not parsed correctly from the sheet {year} in the file {file_path}.")

    # Convert 'Q_m3s' column to float, replacing errors with np.nan
    df['Q_m3s'] = pd.to_numeric(df['Q_m3s'], errors='coerce')

    # Check if the first date is January 1 of the year
    if df['Date'].iloc[0] != pd.to_datetime(f'{year}-01-01'):
        raise ValueError(f"The first date in the sheet {year} in the file {file_path} is not January 1 {year}.")

    # Sort df by Date
    df.sort_values(by=['Date'], inplace=True)

    # Create the data DataFrame
    data = pd.DataFrame({
        "Date": df['Date'].values,
        "Q_m3s": df['Q_m3s'].values,
        "Year": year,
        "Code": station,
    })

    return data

def get_daily_discharge_files(backend_has_access_to_db, site_list):
    """
    Get a list of the Excel files containing the daily discharge data. We only
    read in excel data for the sites in the site_list.

    Args:
        backend_has_access_to_db (bool): Whether the backend has access to the iEasyHydro database.
        site_list (list): A list of Site objects for which to produce forecasts.

    Returns:
        list: A list of daily discharge files.

    Raises:
        EnvironmentError: If the necessary environment variable is not set.
        FileNotFoundError: If no files are found in the directory.
    """
    # Check environment variable
    daily_discharge_path = os.getenv("ieasyforecast_daily_discharge_path")
    if daily_discharge_path is None:
        logger.error("The environment variable ieasyforecast_daily_discharge_path is not set. Please set it.")
        raise EnvironmentError("Environment variable not set")

    # Get a list of the Excel files
    daily_discharge_files = os.listdir(daily_discharge_path)
    daily_discharge_files = [file for file in daily_discharge_files if file.startswith('1')]
    logger.info(f"daily_discharge_files: {daily_discharge_files}")

    # Ignore temporary files created by Excel on macOS
    daily_discharge_files = [file for file in daily_discharge_files if not file.startswith('~')]

    # Filter for site codes in the site_list. We only want to read in excel data
    # for the sites in the site_list. Test if the site_list is not empty
    if site_list:
        # Get a list of all site codes
        site_codes = [site.code for site in site_list]
        # Filter the daily_discharge_files list to only include files that start with a site code
        daily_discharge_files = [file for file in daily_discharge_files if any(file.startswith(code) for code in site_codes)]
    else:
        logger.warning(f"No site list found. Therefore all files in {{daily_discharge_path}} will be read.")

    # Test if we have duplicate codes in the daily_discharge_files list. Compare
    # the first string part (before "_") of each file name and raise an error
    # if there are duplicates.
    codes = [file.split("_")[0] for file in daily_discharge_files]
    if len(codes) != len(set(codes)):
        logger.error("Duplicate site codes found in the daily discharge files.")
        raise ValueError("Duplicate site codes found in the daily discharge files.")

    # Print a warning if daily_discharge_files is empty
    if not daily_discharge_files:
        logger.warning("No files found in the directory data/daily_runoff for the site list.")
        if not backend_has_access_to_db:
            logger.error("No files found in the directory data/daily_runoff and no access to the iEasyHydro database.")
            logger.error("Please check the data/daily_runoff directory and/or the access to the iEasyHydro database.")
            raise FileNotFoundError("No files found in the directory and no access to the iEasyHydro database.")

    return daily_discharge_files

def get_time_series_from_excel(library):
    """
    Get time series data from Excel files.

    Args:
        library: A dataframe with the station IDs and the file names in rows.

    Returns:
        DataFrame: A DataFrame containing the combined data from all Excel files.

    Raises:
        FileNotFoundError: If an Excel file does not exist.
    """
    # Initiate the dictionary for the data
    data_dict = {}

    # Create a vector with numbers from 2000 to the current year, that is the years for
    # which the data may be available in the Excel sheets.
    current_year = datetime.date.today().year
    years = np.arange(2000, (current_year + 1), 1)

    # Suggetsion to speed up the process by leaving out the interrow loop
    # Read the data for each station
    for year in years:
        for _, row in library.iterrows():
            file_path = row["file"]
            station = row["station"]

            try:
                data = read_discharge_from_excel_sheet(file_path, station, year)
                data_dict[station, year] = data
            except:
                continue

    # Check if there is any data in data_dict for each station
    for station in library["station"].unique():
        if (station, 2000) not in data_dict.keys():
            logger.warning(f"No data for station {station} in the Excel sheets.")

    # Test if data dict is empty
    if not data_dict:
        logger.error("No data found in the Excel sheets.")
        raise FileNotFoundError("No data found in the Excel sheets.")

    # Initialize a combined_data DataFrame
    combined_data = pd.DataFrame()

    # Test if there is only data for one station in the data_dict
    if len(data_dict) == 1:
        # Add station and year information to each DataFrame
        for key, df in data_dict.items():
            df['station'], df['year'] = key

        # Concatenate the DataFrames
        combined_data = pd.concat(data_dict.values(), ignore_index=True)

    else:
        # Combine all sheets into a single DataFrame
        combined_data = pd.concat(data_dict.values(), ignore_index=True)

    # Test if combined_data is empty and throw an error if it is
    if combined_data.empty:
        logger.error("No data found in the Excel sheets.")
        raise FileNotFoundError("No data found in the Excel sheets.")

    # Convert the Date column to datetime
    combined_data.loc[:,'Date'] = pd.to_datetime(combined_data.loc[:,'Date'])

    # Drop rows with missing discharge values
    combined_data.dropna(subset = 'Q_m3s', inplace=True)

    # Overwrite the Year column with the actual year based on the date
    combined_data.loc[:,'Year'] = pd.to_datetime(combined_data.loc[:,'Date']).dt.year

    return combined_data

def get_time_series_from_DB(ieh_sdk, library):
    """
    Get time series data from a database.

    Args:
        ieh_sdk (object): An object that provides a method to get data values for a site from a database.
        library (DataFrame): A DataFrame containing the sites to get data for.

    Returns:
        DataFrame: A DataFrame containing the combined data from all sites.

    """
    db_data = pd.DataFrame(columns=['Date', 'Q_m3s', 'Year', 'Code'])

    # Iterate over each site in library station
    for index, row in library.iterrows():

        try:
            db_raw = ieh_sdk.get_data_values_for_site(
                str(row["station"]),
                'discharge_daily_average')

            db_raw = db_raw['data_values']

            # Create a DataFrame from the predictor_discharge list
            db_df = pd.DataFrame(db_raw)

            # Rename the columns of df to match the columns of combined_data
            db_df.rename(columns={'local_date_time': 'Date', 'data_value': 'Q_m3s'}, inplace=True)

            # Drop the columns we don't need
            db_df.drop(columns=['utc_date_time'], inplace=True)

            # Convert the Date column to datetime
            db_df['Datetime'] = pd.to_datetime(db_df['Date'], format='%Y-%m-%d %H:%M:%S')
            db_df['Date'] = pd.to_datetime(db_df['Date'], format='%Y-%m-%d %H:%M:%S').dt.date

            # Add the Code and Year columns
            db_df['Code'] = row["station"]
            db_df['Year'] = db_df['Datetime'].dt.year

            # Remove the datetime column from db_df
            db_df.drop(columns=['Datetime'], inplace=True)

            # Convert Date column to datetime
            db_df['Date'] = pd.to_datetime(db_df['Date'])

            # Append the data to db_data
            db_data = pd.concat([db_data, db_df], ignore_index=True)

        except Exception:
            logger.info(f'    No data for site {row["station"]} in DB.')
            continue

    return db_data

def filter_roughly_for_outliers(combined_data, window_size=15):
    """
    Filters outliers in the 'Q_m3s' column of the input DataFrame.

    This function groups the input DataFrame by the 'Code' column and applies a rolling window outlier detection method to the 'Q_m3s' column of each group. Outliers are defined as values that are more than 3 standard deviations away from the rolling mean. These outliers are replaced with NaN.

    Parameters:
    combined_data (pd.DataFrame): The input DataFrame. Must contain 'Code' and 'Q_m3s' columns.
    window_size (int, optional): The size of the rolling window used for outlier detection. Default is 15.

    Returns:
    pd.DataFrame: The input DataFrame with outliers in the 'Q_m3s' column replaced with NaN.
    """
    # Preliminary filter for outliers
    def filter_group(group, window_size):
        # Calculate the rolling mean and standard deviation
        # Will put NaNs in the first window_size-1 values and therefore not
        # filter outliers in the first window_size-1 values
        rolling_mean = group['Q_m3s'].rolling(window_size).mean()
        rolling_std = group['Q_m3s'].rolling(window_size).std()

        # Calculate the upper and lower bounds for outliers
        num_std = 3.5  # Number of standard deviations
        upper_bound = rolling_mean + num_std * rolling_std
        lower_bound = rolling_mean - num_std * rolling_std

        #print("DEBUG: upper_bound: ", upper_bound)
        #print("DEBUG: lower_bound: ", lower_bound)
        #print("DEBUG: Q_m3s: ", group['Q_m3s'])

        # Set Q_m3s which exceeds lower and upper bounds to nan
        group.loc[group['Q_m3s'] > upper_bound, 'Q_m3s'] = np.nan
        group.loc[group['Q_m3s'] < lower_bound, 'Q_m3s'] = np.nan

        return group

    # Apply the function to each group
    combined_data = combined_data.groupby('Code').apply(filter_group, window_size)

    # Ungroup the DataFrame
    combined_data = combined_data.reset_index(drop=True)

    return combined_data


def add_pentad_issue_date(data_df, datetime_col):
    """
    Adds an 'issue_date' column to the DataFrame. The 'issue_date' column is True if the day in the date column
    identified by the string datetime_col is in 5, 10, 15, 20, 25, or the last day of the month. Otherwise, it's False.

    Parameters:
    data_df (DataFrame): The input DataFrame.
    datetime_col (str): The column identifier for the date column.

    Returns:
    DataFrame: The input DataFrame with the 'issue_date' column added.
    """
    # Check if the datetime_col is in the data_df columns
    if datetime_col not in data_df.columns:
        raise KeyError(f"The column {datetime_col} is not in the DataFrame.")
    # Ensure the datetime_col is of datetime type
    try:
        data_df[datetime_col] = pd.to_datetime(data_df[datetime_col], format = "%Y-%m-%d")
    except:
        raise TypeError(f"The column {datetime_col} cannot be converted to datetime type.")

    # Ensure the DataFrame is sorted by date
    data_df = data_df.sort_values(datetime_col)

    # Get the day of the month
    data_df['day'] = data_df[datetime_col].dt.day

    # Get the last day of each month
    #data_df['pdoffsetsMonthEnd'] = pd.offsets.MonthEnd(0)  # add one month end offset
    data_df['end_of_month'] = data_df[datetime_col] + pd.offsets.MonthEnd(0)  # add one month end offset
    data_df['is_end_of_month'] = data_df[datetime_col].dt.day == data_df['end_of_month'].dt.day

    # Set issue_date to True if the day is 5, 10, 15, 20, 25, or the last day of the month
    data_df['issue_date'] = data_df['day'].isin([5, 10, 15, 20, 25]) | data_df['is_end_of_month']

    # Drop the temporary columns
    data_df.drop(['day', 'end_of_month', 'is_end_of_month'], axis=1, inplace=True)

    return data_df

def calculate_3daydischargesum(data_df, datetime_col, discharge_col):
    """
    Calculate the 3-day discharge sum for each station in the input DataFrame.

    Args:
    data_df (pandas.DataFrame):
        The input DataFrame containing the data for each station.
    datetime_col (str):
        The name of the column containing the datetime information.
    discharge_col (str):
        The name of the column containing the discharge information.

    Returns:
    pandas.DataFrame (pandas.DataFrame):
        The modified DataFrame with the 3-day discharge sum for each station in
        column 'discharge_sum'.
    """
    # Raise a Type error if the datetime_col is not of type datetime
    if data_df[datetime_col].dtype != 'datetime64[ns]':
        raise TypeError(f"The column {datetime_col} is not of type datetime.")

    # Ensure the DataFrame is indexed by datetime
    data_df.set_index(datetime_col, inplace=True)

    # Calculate the rolling sum of the discharge values over a 3-day window
    data_df['discharge_sum'] = data_df[discharge_col].rolling('3D', closed='left').sum()

    # Set 'discharge_sum' to NaN for rows where 'issue_date' is False
    data_df.loc[~data_df['issue_date'], 'discharge_sum'] = np.nan

    # Reset the index
    data_df.reset_index(inplace=True)

    return data_df

def calculate_pentadaldischargeavg(data_df, datetime_col, discharge_col):
    """
    Calculate the 5-day discharge average for each station in the input DataFrame.

    Args:
    data_df (pandas.DataFrame):
        The input DataFrame containing the data for each station.
    datetime_col (str):
        The name of the column containing the datetime information.
    discharge_col (str):
        The name of the column containing the discharge information.

    Returns:
    pandas.DataFrame:
        The modified DataFrame with the 5-day discharge average for each station in
        column 'discharge_avg'.
    """
    data_df = data_df.copy(deep=True)
    # Ensure the DataFrame is indexed by datetime
    data_df.set_index(pd.DatetimeIndex(data_df[datetime_col]), inplace=True)

    # Reverse the DataFrame
    data_df = data_df.iloc[::-1]

    # Shift the discharge column by 1 day
    data_df['temp'] = data_df[discharge_col].shift(1)

    # Calculate the rolling average of the discharge values over a 5-day window
    data_df['discharge_avg'] = data_df['temp'].rolling('5D', closed='right').mean()

    # Drop the temporary column
    data_df.drop(columns='temp', inplace=True)

    # Reverse the DataFrame again
    data_df = data_df.iloc[::-1]

    # Reset the index
    data_df.reset_index(inplace=True, drop=True)

    # Set 'discharge_avg' to NaN for rows where 'issue_date' is False
    data_df.loc[~data_df['issue_date'], 'discharge_avg'] = np.nan

    return data_df


def generate_issue_and_forecast_dates(data_df: pd.DataFrame, datetime_col: str,
                                      station_col: str, discharge_col: str):
    """
    Generate issue and forecast dates for each station in the input DataFrame.

    Arg:
    data_df (pandas.DataFrame):
        The input DataFrame containing the data for each station.
    datetime_col (str):
        The name of the column containing the datetime information.
    station_col (str)
        The name of the column containing the station information.
    discharge_col (str):
        The name of the column containing the discharge information.

    Returns:
    pandas.DataFrame
        The modified DataFrame with the issue and forecast dates for each station.
    """
    def apply_calculation(data_df, datetime_col, discharge_col):

        # Set negative values to nan
        data_df[discharge_col] = data_df[discharge_col].apply(lambda x: np.nan if x < 0 else x)

        # Fill in data gaps of up to 3 days by linear interpolation
        data_df[discharge_col] = data_df[discharge_col].interpolate(
            method='linear', limit_direction='both', limit=3)

        # Round data to 3 numbers according to the custom of operational hydrology
        # in Kyrgyzstan.
        data_df.loc[:, discharge_col] = data_df.loc[:, discharge_col].apply(fl.round_discharge_to_float)

        data_df = add_pentad_issue_date(data_df, datetime_col)

        data_df = calculate_3daydischargesum(data_df, datetime_col, discharge_col)

        data_df = calculate_pentadaldischargeavg(data_df, datetime_col, discharge_col)

        return(data_df)

    # Test if the input data contains the required columns
    if not all(column in data_df.columns for column in [datetime_col, station_col, discharge_col]):
        raise ValueError(f'DataFrame is missing one or more required columns: {datetime_col, station_col, discharge_col}')

    # Apply the calculation function to each group based on the 'station' column
    modified_data = data_df.groupby(station_col).apply(apply_calculation, datetime_col = datetime_col, discharge_col = discharge_col)

    return modified_data


def get_station_data(ieh_sdk, backend_has_access_to_db, start_date, site_list):
    # === Read station data ===
    # region Read station data
    """
    We read older station data from Excel sheets and newer station
    data from the DB.

    Args:
        ieh_sdk: The iEasyHydro SDK.
        backend_has_access_to_db: Whether the backend has access to the iEasyHydro database.
        start_date: The start date for the forecast.
        site_list: A list of Site objects.

    Returns:
        DataFrame: A DataFrame containing the combined data from all stations.

    Raises:
        FileNotFoundError: If no files are found in the directory.

    """
    # Read station data from Excel sheets
    logger.info("Reading daily discharge data ...")
    logger.info("-Reading discharge data from Excel sheets ...")

    # Get a list of the Excel files to read data from
    daily_discharge_files = get_daily_discharge_files(backend_has_access_to_db, site_list)

    # Create a dataframe with the station IDs and the file names. The station
    # IDs are in the first part of the file names, before the first underscore.
    # The filenames are the full path to the files.
    library = pd.DataFrame(
        {
            "station": [file.split('_')[0] for file in daily_discharge_files],
            "file": [os.path.join(os.getenv("ieasyforecast_daily_discharge_path"), file)
                     for file in daily_discharge_files]
        })

    # Get the time series data from the Excel files
    combined_data = get_time_series_from_excel(library)

    # Get the latest daily data from DB in addition. The data from the DB takes
    # precedence over the data from the Excel sheets.
    logger.info("-Reading latest daily data from DB ...")
    # Initiate a data frame for the data from the database with the same format
    # as combined_data
    db_data = get_time_series_from_DB(ieh_sdk, library)

    # Combine the data from the DB and the Excel sheets. Check for duplicate
    # dates and keep the data from the DB.
    combined_data = pd.concat([combined_data, db_data]).drop_duplicates(subset=['Date', 'Code'], keep='first')

    # Test if station 16936 is in the list of stations. If it is, we check for
    # data gaps in the combined_data and fill them with a combination from other
    # stations.
    if '16936' in combined_data['Code'].values:
        # Go through the the combined_data for '16936' and check for data gaps.
        # From the last available date in the combined_data for '16936', we
        # look for data in the combined_data for other stations, namely '16059',
        # '16096', '16100', and '16093' and add their values up to write to
        # station '16936'.

        # Get latest date for which we have data in '16936'
        last_date_16936 = combined_data[combined_data['Code'] == '16936']['Date'].max()

        # Get the data for the date from the other stations
        # Test if station 16093 (Torkent) is in the list of stations
        # (This station is under construction at the time of writing this code)
        if '16059' in combined_data['Code'].values:
            data_16059 = combined_data[(combined_data['Code'] == '16059') & (combined_data['Date'] >= last_date_16936)]
        else:
            logger.error('Station 16059 is not in the list of stations for forecasting but it is required for forecasting 16936.\n -> Not able to calculate runoff for 16936.\n -> Please select station 16059 for forecasting.')
            exit()
        if '16096' in combined_data['Code'].values:
            data_16096 = combined_data[(combined_data['Code'] == '16096') & (combined_data['Date'] >= last_date_16936)]
        else:
            logger.error('Station 16096 is not in the list of stations for forecasting but it is required for forecasting 16936.\n -> Not able to calculate runoff for 16936.\n -> Please select station 16096 for forecasting.')
            exit()
        if '16100' in combined_data['Code'].values:
            data_16100 = combined_data[(combined_data['Code'] == '16100') & (combined_data['Date'] >= last_date_16936)]
        else:
            logger.error('Station 16100 is not in the list of stations for forecasting but it is required for forecasting 16936.\n -> Not able to calculate runoff for 16936.\n -> Please select station 16100 for forecasting.')
            exit()
        if '16093' in combined_data['Code'].values:
            data_16093 = combined_data[(combined_data['Code'] == '16093') & (combined_data['Date'] >= last_date_16936)]
        else:
            data_16093 = combined_data[(combined_data['Code'] == '16096') & (combined_data['Date'] >= last_date_16936)]
            data_16093.loc[:, 'Q_m3s'] = 0.6 * data_16093.loc[:,'Q_m3s']

        # Merge data_15059, data_16096, data_16100, and data_16093 by date
        data_Tok_combined = pd.merge(data_16059, data_16096, on='Date', how='left', suffixes=('_16059', '_16096'))
        data_16100.rename(columns={'Q_m3s': 'Q_m3s_16100'}, inplace=True)
        data_Tok_combined = pd.merge(data_Tok_combined, data_16100, on='Date', how='left')
        data_16093.rename(columns={'Q_m3s': 'Q_m3s_16093'}, inplace=True)
        data_Tok_combined = pd.merge(data_Tok_combined, data_16093, on='Date', how='left')

        # Sum up all the data and write to '16936'
        data_Tok_combined['sum'] = data_Tok_combined['Q_m3s_16059'] + \
            data_Tok_combined['Q_m3s_16096'] + \
                data_Tok_combined['Q_m3s_16100'] + \
                    data_Tok_combined['Q_m3s_16093']

        # Append the data_Tok_combined['sum'] to combined_data for '16936'
        data_Tok_combined['Code'] = '16936'
        data_Tok_combined['Year'] = data_Tok_combined['Year_16059']
        data_Tok_combined['Q_m3s'] = data_Tok_combined['sum']
        data_Tok_combined.drop(columns=['Year_16059', 'Code_16059', 'Year_16096',
                                        'Code_16096', 'Year_x', 'Code_x',
                                        'Year_y', 'Code_y',
                                        'Q_m3s_16059', 'Q_m3s_16096',
                                        'Q_m3s_16100', 'Q_m3s_16093', 'sum'],
                               inplace=True)

        combined_data = pd.concat([combined_data, data_Tok_combined], ignore_index=True)

    # Filter combined_data for dates before today (to simulate offline mode)
    combined_data = combined_data[combined_data['Date'] <= start_date]

    df_filtered = filter_roughly_for_outliers(combined_data, window_size=15)

    # Make sure the Date column is of type datetime
    df_filtered.loc[:, 'Date'] = pd.to_datetime(df_filtered.loc[:, 'Date'])
    # Cast to datetime64 type
    df_filtered.loc[:, 'Date'] = df_filtered.loc[:, 'Date'].values.astype('datetime64[D]')

    # Sort df_filter with ascending Code and Date
    df_filtered.sort_values(by=['Code', 'Date'], inplace=True)

    logger.info("-Writing issue and forecast dates...")

    #modified_data = fl.generate_issue_and_forecast_dates(pd.DataFrame(df_filtered), 'Date', 'Code', 'Q_m3s')
    # DEBUGGING generate_issue_and_forecast_dates
    modified_data = generate_issue_and_forecast_dates(pd.DataFrame(df_filtered), 'Date', 'Code', 'Q_m3s')

    # Drop the rows with 0 discharge_sum
    # data_df = modified_data[modified_data['discharge_sum'] != 0]

    # Calculate norm discharge for each pentad of the year
    logger.info("-Calculating norm discharge for each pentad of the year...")
    # For each Date in modified_data, calculate the pentad of the month
    modified_data['pentad'] = modified_data['Date'].apply(tl.get_pentad)
    modified_data['pentad_in_year'] = modified_data['Date'].apply(tl.get_pentad_in_year)

    return modified_data


def get_forecast_pentad_of_year(bulletin_date):
    # Get the pentad of the year of the bulletin_date. We perform the linear
    # regression on all data from the same pentad of the year.
    return tl.get_pentad_in_year(bulletin_date)


def save_discharge_avg(modified_data, fc_sites, forecast_pentad_of_year):
    # Group modified_data by Code and calculate the mean over discharge_avg
    # while ignoring NaN values
    norm_discharge = (
        modified_data.reset_index(drop=True).groupby(['Code', 'pentad_in_year'], as_index=False)['discharge_avg']
                      .apply(lambda x: x.mean(skipna=True))
    )

    # Now we need to write the discharge_avg for the current pentad to the site: Site
    for site in fc_sites:
        logger.info(f'    Calculating norm discharge for site {site.code} ...')
        fl.Site.from_df_get_norm_discharge(site, forecast_pentad_of_year, norm_discharge)

    logger.info(f'   {len(fc_sites)} Norm discharge calculated, namely:\n{[site1.qnorm for site1 in fc_sites]}')
    logger.info("   ... done")
