import logging
import os
import json
import pandas as pd
import numpy as np

# Import the modules from the forecast library
import tag_library as tl
import forecast_library as fl
logger = logging.getLogger(__name__)


def check_database_access(ieh_sdk):
    # Test if the backand has access to an iEasyHydro database and set a flag accordingly.
    try:
        ieh_sdk.get_discharge_sites()
        logger.info(f"SAPPHIRE forecast tools will use operational data from the iEasyHydro database.")
        return True
    except Exception as e:
        # Test if there are any files in the data/daily_runoff directory
        if os.listdir(os.getenv("ieasyforecast_daily_discharge_dir")):
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


def get_predictor_dates(start_date):
    # Get the dates to get the predictor from
    # For pentadal forecasts, the hydromet uses the sum of the last 3 days discharge.
    predictor_dates = fl.get_predictor_dates(start_date.strftime('%Y-%m-%d'), 3)
    # if predictor_dates is None, raise an error
    if predictor_dates is None:
        raise ValueError("The predictor dates are not valid.")

    logger.info(f"   Predictor dates: {predictor_dates}")

    logger.info("   ... done")
    return predictor_dates


def get_station_data(ieh_sdk, backend_has_access_to_db, start_date):
    # === Read station data ===
    # region Read station data
    """
    # For now we read older station data from Excel sheets and newer station
    # data from the DB.
    # Once the upload for daily station data is implemented, we can read the
    # daily data for forecasting from the DB.

    # We will want to save the station data in objects that can later be used
    # for bulletin writing.
    """
    # Read station data from Excel sheets
    logger.info("Reading daily discharge data ...")
    logger.info("-Reading discharge data from Excel sheets ...")

    # Get a list of the Excel files containing the daily discharge data available
    # in the data/daily_runoff directory
    daily_discharge_files = os.listdir(os.getenv("ieasyforecast_daily_discharge_path"))
    # Ignore files that do not start with "1"
    daily_discharge_files = [file for file in daily_discharge_files if file.startswith('1')]
    logger.info(f"daily_discharge_files: {daily_discharge_files}")

    # Print a warning if there are no files found in the ieasyforecast_daily_discharge_path
    if not daily_discharge_files:
        logger.warning("No files found in the directory data/daily_runoff.")
        # If in addition to seeing no Excel sheets, we do not have access to the
        # iEasyHydro database, throw an error and exit the script.
        if not backend_has_access_to_db:
            logger.error("No files found in the directory data/daily_runoff and no access to the iEasyHydro database.")
            logger.error("Please check the data/daily_runoff directory and/or the access to the iEasyHydro database.")
            logger.error("No forecasts possible. Exiting the script.")
            exit()

    # If an Excel file is open on macOS, it creates a temporary file with the
    # same name as the original file but starting with a tilde (~).
    # We want to ignore these files.
    daily_discharge_files = [file for file in daily_discharge_files if not file.startswith('~')]

    # Create a dataframe with the station IDs and the file names. The station
    # IDs are in the first part of the file names, before the first underscore.
    # The filenames are the full path to the files.
    library = pd.DataFrame(
        {
            "station": [file.split('_')[0] for file in daily_discharge_files],
            "file": [os.path.join(os.getenv("ieasyforecast_daily_discharge_path"), file)
                     for file in daily_discharge_files]
        })

    # Initiate the dictionary for the data
    data_dict = {}

    # Create a vector with numbers from 2000 to 2023, that is the years for
    # which the data is available in the Excel sheets.
    years = np.arange(2000, 2024, 1)

    # Get the file names for the stations
    for index, row in library.iterrows():
        file_path = row["file"]
        station = row["station"]

        # Read the data for the station
        for year in years:
            df = pd.read_excel(
                file_path, sheet_name=str(year), header=[0], skiprows=[1],
                names=['Date', 'Q_m3s'], parse_dates=['Date'],
                date_format="%d.%m.%Y"
            )
            # Sort df by Date
            df.sort_values(by=['Date'], inplace=True)

            datetime_column = df.iloc[:, 0]  # Dates are in the first column
            discharge_column = df.iloc[:, 1]  # Discharges are in the second column

            data = pd.DataFrame({
                "Date": datetime_column.values,
                "Q_m3s": discharge_column.values,
                "Year": year,
                "Code": station,
            })

            data_dict[station, year] = data

    # Combine all sheets into a single DataFrame
    combined_data = pd.concat(data_dict.values(), ignore_index=True)

    # Convert the Date column to datetime
    combined_data.loc[:, 'Date'] = pd.to_datetime(combined_data.loc[:, 'Date'])

    # Drop rows with missing discharge values
    combined_data.dropna(subset='Q_m3s', inplace=True)

    # Get the latest daily data from DB in addition. The data from the DB takes
    # precedence over the data from the Excel sheets.
    logger.info("-Reading latest daily data from DB ...")
    # Initiate a data frame for the data from the database with the same format
    # as combined_data
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

    # Combine the data from the DB and the Excel sheets. Check for duplicate
    # dates and keep the data from the DB.
    combined_data = pd.concat([combined_data, db_data]).drop_duplicates(subset=['Date', 'Code'], keep='first')

    # Filter combined_data for dates before today (to simulate offline mode)
    combined_data = combined_data[combined_data['Date'] <= start_date]

    # Preliminary filter for outliers
    # We try filtering out the outliers.
    # calculate rolling mean and standard deviation
    window_size = 15
    rolling_mean = combined_data['Q_m3s'].rolling(window_size).mean()
    rolling_std = combined_data['Q_m3s'].rolling(window_size).std()

    # calculate upper and lower bounds for outliers
    num_std = 3
    upper_bound = rolling_mean + num_std * rolling_std
    lower_bound = rolling_mean - num_std * rolling_std

    # Set Q_m3m which exceeds lower and upper bounds to nan
    combined_data.loc[combined_data['Q_m3s'] > upper_bound, 'Q_m3s'] = np.nan
    combined_data.loc[combined_data['Q_m3s'] < lower_bound, 'Q_m3s'] = np.nan
    df_filtered = combined_data

    # Make sure the Date column is of type datetime
    df_filtered.loc[:, 'Date'] = pd.to_datetime(df_filtered.loc[:, 'Date'])
    # Cast to datetime64 type
    df_filtered.loc[:, 'Date'] = df_filtered.loc[:, 'Date'].values.astype('datetime64[D]')

    # Sort df_filter with ascending Code and Date
    df_filtered.sort_values(by=['Code', 'Date'], inplace=True)

    logger.info("-Writing issue and forecast dates...")

    modified_data = fl.generate_issue_and_forecast_dates(pd.DataFrame(df_filtered), 'Date', 'Code', 'Q_m3s')
    # Print hydrograph data between June 1, 2023, and June 20, 2023, for site 15194

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