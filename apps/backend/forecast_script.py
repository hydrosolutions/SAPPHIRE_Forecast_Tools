# Python 3.10

# region Libraries
# Environment
import subprocess
from dotenv import load_dotenv

# I/O
import re
import pandas as pd
import numpy as np
import json
import datetime as dt
import random as rd
import logging
import os
import sys
import pickle

# ieasyreports, installed with
# pip install git+https://github.com/hydrosolutions/ieasyreports.git@main
from ieasyreports.core.tags.tag import Tag
from ieasyreports.utils import import_from_string
from ieasyreports.settings import Settings

from typing import Union, Any, List, Optional

from ieasyreports.core.report_generator import DefaultReportGenerator
from ieasyreports.exceptions import TemplateNotValidatedException



# SDK library for accessing the DB, installed with
# pip install git+https://github.com/hydrosolutions/ieasyhydro-python-sdk
from ieasyhydro_sdk.sdk import IEasyHydroSDK

# Modelling
from sklearn.linear_model import LinearRegression

# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the modules from the forecast library
import tag_library as tl
import forecast_library as fl

# endregion



# region Main
'''
The main forecasting script.

It is run once a day on the 5th, 10th, 15th, 20th, 25th and last day of each
month. It produces a forecast bulletin for the next pentad.

The script is run with the following command:
python forecast.py <date>
It is typically called from one of either files run_offline_mode.py or
run_online_mode.py to run it in offline or online mode respectively.

The online mode is used to produce the forecast bulletin for the current day.
It requires access to the database.

The offline mode is used to produce hindcasts. It is based on daily data read
from both the database and excel sheets. It can be configured to run on excel-
based data only to avoid network interruptions.

You may need to install the library iEasyHydroForcast before running this script
using the command (-e is for installing the iEasyHydroLibrary library in
editable mode):
    $ pip install -e ./iEasyHydroForecast
'''
if __name__ == "__main__":
    # Get the command line arguments
    args = sys.argv[1:]

    # Get the name of the calling script
    calling_script = args[1]

    # Test if the string contains the word "run_offline_mode"
    # Please note that the online_mode is being deprecated.
    if re.search("run_offline_mode", calling_script):
        # If it does, set the offline_mode flag to True
        offline_mode = True
    else:
        # Otherwise, set the offline_mode flag to False
        offline_mode = False

    # === Load environment ===
    # region Load environment
    print("\n\n====================================================\n")
    logging.info("\n\n====================================================")
    print(f"forecast_script called from: {calling_script}")
    logging.info(f"forecast_script called from: {calling_script}")

    # Read the environment varialbe IN_DOCKER_CONTAINER to determine which .env
    # file to use
    if os.getenv("IN_DOCKER_CONTAINER") == "True":
        print(f"Running in docker container. Loading environment variables from .env")
        env_file_path = "apps/config/.env"
        load_dotenv(env_file_path)

    else:
        print(f"Running locally. Loading environment variables from .env_develop")
        env_file_path = "../config/.env_develop"
        load_dotenv(env_file_path)

    # Configure logging
    logging.basicConfig(
        filename=os.getenv("log_file"), encoding="utf-8", level=os.getenv("log_level")
    )

    # Run Main only if we're on the 5th, 10th, 15th, 20th, 25th or last day of
    # the month in operational mode. Otherwise, exit the program.
    # Always run the script in offline mode.
    # Get today's date and convert it to datetime
    today = dt.datetime.strptime(args[0], "%Y-%m-%d")

    # Get the day of the month
    day = today.day
    # Get the last day of the month
    last_day = fl.get_last_day_of_month(today)
    # Get the list of days for which we want to run the forecast
    days = [5, 10, 15, 20, 25, last_day.day]

    # If today is not in days, exit the program.
    if day not in days :
        print(f"Run for date {today}. No forecast date, no forecast will be run.")
        logging.info(f"Run for date {today}. No forecast date, no forecast will be run.")
        # Store last successful run date
        print("Storing last successful run date ...")
        logging.info("Storing last successful run date ...")

        # Path to the file
        last_run_file = os.path.join(
            os.getenv("ieasyforecast_intermediate_data_path"),
            os.getenv("ieasyforecast_last_successful_run_file")
        )
        # Overwrite the file with the current date
        with open(last_run_file, "w") as f:
            f.write(today.strftime("%Y-%m-%d"))
            f.flush()

        print("   ... done")
        logging.info("   ... done")
        exit()  # exit the program
    else:
        print(f"Runnig forecast for {today}.")
        logging.info(f"Runnig forecast for {today}.")


    settings = Settings(_env_file=env_file_path)  # ieasyreports

    ieh_sdk = IEasyHydroSDK()  # ieasyhydro

    # Test if the backand has access to an iEasyHydro database and set a flag
    # accordingly.
    try:
        ieh_sdk.get_discharge_sites()
        print(f"SAPPHIRE forecast tools will use operational data from the iEasyHydro database.")
        #logging.info(f"SAPPHIRE forecast tools will use operational data from the iEasyHydro database.")
        backend_has_access_to_db = True
    except Exception as e:
        # Test if there are any files in the data/daily_runoff directory
        if len(os.listdir(os.getenv("ieasyforecast_daily_discharge_dir"))) > 0:
            print(f"INFO: SAPPHIRE forecast tools does not have access to the iEasyHydro database")
            print(f"      and will use data from the data/daily_runoff directory for forecasting only.")
            #logging.info(f"SAPPHIRE forecast tools will use data from the data/daily_runoff directory.")
            backend_has_access_to_db = False
        else:
            print(f"ERROR: SAPPHIRE tools do not find any data in the data/daily_runoff directory")
            print(f"       nor does it have access to the iEasyHydro database.")
            print(f"       Please check the data/daily_runoff directory and/or the access to the iEasyHydro database.")
            #logging.error(
            #    f"SAPPHIRE tools do not find any data in the data/daily_runoff \ndirectory nor does it have access to the iEasyHydro database. \nPlease check the data/daily_runoff directory and/or the access to the iEasyHydro database.")
            logging.error(f"Error connecting to DB: {e}")
            raise e

    # region overwrite default report writer
    class FakeHeaderTemplateGenerator(DefaultReportGenerator):
        # This class is a subclass of the DefaultReportGenerator class and provides a
        # method for generating a report based on a template. The
        # FakeHeaderTemplateGenerator class overrides the generate_report() method of
        # the DefaultReportGenerator class to provide custom functionality for
        # generating reports.
        def generate_report(
            self, list_objects: Optional[List[Any]] = None,
            output_path: Optional[str] = None, output_filename: Optional[str] = None
        ):
            # Generates a report based on a template.
            # Requires a statement settings = Settings() before calling the method.
            # Args:
            #     list_objects (list): A list of objects to be used to generate the
            #         report. The objects in the list should be of the same type as
            #         the objects used to validate the template.
            #     output_path (str): The path to the directory where the report will
            #         be saved. If no output_path is provided, the report will be
            #         saved in the current working directory.
            #     output_filename (str): The name of the report file. If no
            #         output_filename is provided, the report will be saved with the
            #         same name as the template file.

            if not self.validated:
                raise TemplateNotValidatedException(
                    "Template must be validated first. Did you forget to call the `.validate()` method?"
                )
            for tag, cells in self.general_tags.items():
                for cell in cells:
                    cell.value = tag.replace(cell.value)

            if self.header_tag_info:
                grouped_data = {}
                list_objects = list_objects if list_objects else []
                for list_obj in list_objects:
                    header_value = self.header_tag_info["tag"].replace(
                        self.header_tag_info["cell"].value,
                        special="HEADER",
                        obj=list_obj
                    )

                    if header_value not in grouped_data:
                        grouped_data[header_value] = []
                    grouped_data[header_value].append(list_obj)

                original_header_row = self.header_tag_info["cell"].row
                header_style = self.header_tag_info["cell"].font.copy()
                data_styles = [data_tag["cell"].font.copy() for data_tag in self.data_tags_info]

                for header_value, item_group in sorted(grouped_data.items()):
                    # write the header value
                    cell = self.sheet.cell(row=original_header_row, column=self.header_tag_info["cell"].column, value=header_value)
                    cell.font = header_style

                    self.sheet.delete_rows(original_header_row + 3)
                    for item in item_group:
                        for idx, data_tag in enumerate(self.data_tags_info):
                            tag = data_tag["tag"]
                            data = tag.replace(data_tag["cell"].value, obj=item, special=settings.data_tag)
                            cell = self.sheet.cell(row=original_header_row + 3, column=data_tag["cell"].column, value=data)
                            cell.font = data_styles[idx]

                        original_header_row += 1

            self.save_report(output_filename, output_path)
    # endregion


    # Find out if we are writing excel forecast sheets or not.
    # If we are, set the excel_output flag to True.
    # If we are not, set the excel_output flag to False.
    # Read the configuration file
    config_output_file = os.path.join(
        os.getenv("ieasyforecast_configuration_path"),
        os.getenv("ieasyforecast_config_file_output"),
    )
    with open(config_output_file, "r") as json_file:
        config = json.load(json_file)
        excel_output = config["write_excel"]
    if excel_output == True:
        print("Writing excel forecast sheets.")
        logging.info("Writing excel forecast sheets.")
    else:
        print("Not writing excel forecast sheets.")
        logging.info("Not writing excel forecast sheets.")

    # endregion

    # === Read forecast configuration ===
    # region Read forecast configuration
    print("Reading forecasting configuration ...")
    logging.info("Reading forecasting configuration ...")

    ## Read station metadata from the DB and store it in a list of Site objects
    print("-Reading station metadata from the DB ...")
    logging.info("-Reading station metadata from the DB ...")

    # Read the station details from API
    # Only do this if we have access to the database
    if backend_has_access_to_db == True:
        try:
            db_sites = ieh_sdk.get_discharge_sites()
        except Exception as e:
            logging.error(f"Error connecting to DB: {e}")
            raise e
        db_sites = pd.DataFrame.from_dict(db_sites)

        logging.info(
            f"   {len(db_sites)} station(s) in DB, namely:\n{db_sites['site_code'].values}"
        )
    else:
        # If we don't have access to the database, create an empty dataframe.
        db_sites = pd.DataFrame(
            columns=['site_code', 'site_name', 'river_name', 'punkt_name',
                     'latitude', 'longitude', 'region', 'basin'])

    ## Read station information of all available discharge stations
    print("-Reading information about all stations from JSON...")
    logging.info("-Reading information about all stations from JSON...")

    config_all_file = os.path.join(
        os.getenv("ieasyforecast_configuration_path"),
        os.getenv("ieasyforecast_config_file_all_stations"))

    config_all = fl.load_all_station_data_from_JSON(config_all_file)

    logging.info(f"   {len(config_all)} discharge station(s) found, namely\n{config_all['code'].values}")

    ## Merge information from db_sites and config_all. This is in fact only
    ## needed once to update the config_all file. This should be done manually
    ## when new stations are added to the DB.
    # Only do this if we have access to the database
    '''
    if backend_has_access_to_db == True:
        print("-Merging information from db_sites and config_all ...")
        logging.info("-Merging information from db_sites and config_all ...")

        # Find sites in config_all which are not present in db_sites.
        # This is a special case for Kygryz Hydromet.
        new_sites = config_all[~config_all['site_code'].isin(db_sites['site_code'])]

        # Add new sites (virtual station for inflow to Toktogul reservoir) to
        # forecast_sites.
        # Edit here if need to add new sites for short-term forecasting.
        new_sites_forecast = pd.DataFrame({
            'site_code': new_sites['site_code'],
            'basin': "Нарын",
            'latitude': new_sites['lat'],
            'longitude': new_sites['long'],
            'country': 'Кыргызстан',
            'is_virtual': True,
            'region': 'Нарынская область',
            'site_type': 'discharge',
            'site_name': new_sites['name_ru'],
            'organization_id': 1,
            'elevation': 988,
        })
        db_sites = pd.concat([db_sites, new_sites_forecast])
    '''

    if backend_has_access_to_db == True:
        # Add information from config_all to db_sites
        db_sites = pd.merge(
            db_sites,
            config_all[['site_code', 'river_ru', 'punkt_ru','lat','long']],
            left_on='site_code',
            right_on='site_code',
            how='left'
        )
        # We give precedence to the information from db_sites (from the iEasyHydro
        # database over the information read from the
        # .env_develop/<ieasyforecast_config_file_all_stations> file.
        # Where lat is not equal to latitute, replace latitude with lat
        db_sites['latitude'] = np.where(db_sites['latitude'] != db_sites['lat'], db_sites['lat'], db_sites['latitude'])
        # Where long is not equal to longitude, replace longitude with long
        db_sites['longitude'] = np.where(db_sites['longitude'] != db_sites['long'], db_sites['long'], db_sites['longitude'])

        # Drop the lat and long columns
        db_sites.drop(columns=['lat', 'long'], inplace=True)
    else:
        # If we don't have access to the database, we use the information from
        # config_all
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
    # 1.
    db_sites = db_sites[db_sites['site_code'].astype(str).str.startswith('1')]

    ## Read stations for forecasting
    print("-Reading stations for forecasting ...")
    logging.info("-Reading stations for forecasting ...")

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
        config_restirict_station_file = os.path.join(
            os.getenv("ieasyforecast_configuration_path"),
            os.getenv("ieasyforecast_restrict_stations_file"))
        with open(config_restirict_station_file, "r") as json_file:
            restrict_stations_config = json.load(json_file)
            restrict_stations = restrict_stations_config["stationsID"]
            print("WARNING: Station selection for forecasting restricted to: ", restrict_stations)
            print("         To remove restriction set ieasyforecast_restrict_stations_file in .env or .env_develop to null.")

    # Only keep stations that are in the file ieasyforecast_restrict_stations_file
    stations = [station for station in stations if station in restrict_stations]

    logging.info(
        f"   {len(stations)} station(s) selected for forecasting, namely: {stations}"
    )
    print(f"   {len(stations)} station(s) selected for forecasting, namely: {stations}")

    ## Filter db_sites for stations
    print("-Filtering db_sites for stations ...")
    logging.info("-Filtering db_sites for stations ...")

    stations_str = [str(station) for station in stations]

    db_sites = db_sites[db_sites["site_code"].isin(stations_str)]
    logging.info(
        f"   Producing forecasts for {len(db_sites)} station(s), namely: {db_sites['site_code'].values}"
    )

    ## Formatting db_sites to a list of Sites objects
    print("-Formatting db_sites to a list of Sites objects ...")
    logging.info("-Formatting db_sites to a list of Sites objects ...")

    # Make sure the entries are not lists
    for col in db_sites.columns:
        # Check if content is a list
        if isinstance(db_sites[col][0], list):
            db_sites[col] = db_sites[col].apply(lambda x: x[0])

    fc_sites = fl.Site.from_dataframe(
        db_sites[["site_code", "site_name", "river_ru", "punkt_ru", "latitude", "longitude", "region", "basin"]]
    )
    logging.info(
        f'   {len(fc_sites)} Site object(s) created for forecasting, namely:\n{[site.code for site in fc_sites]}')

    # Get dangerous discharge for each site
    # This can be done only if we have access to the database
    if backend_has_access_to_db == True:
        logging.info("-Getting dangerous discharge from DB ...")
        print("-Getting dangerous discharge from DB ...")
        # Call the from_DB_get_dangerous_discharge method on each Site object
        for site in fc_sites:
            fl.Site.from_DB_get_dangerous_discharge(ieh_sdk, site)

        print(
            f'   {len(fc_sites)} Dangerous discharge gotten from DB, namely:\n{[site.qdanger for site in fc_sites]}')
    else:
        # Assign " " to qdanger for each site
        for site in fc_sites:
            site.qdanger = " "
        print("INFO: No access to iEasyHydro database. Therefore no dangerous discharge is assigned to sites.")

    # The forecast is done one day before the beginning of each pentad
    # That is on the 5th, 10th, 15th, 20th, 25th and on the last day of each month
    bulletin_date = today + dt.timedelta(days=1)
    bulletin_date = bulletin_date.strftime(
        "%Y-%m-%d"
    )  # convert to string in the format 'YYYY-MM-DD'
    print(f"INFO: The forecast bulletin date is: {bulletin_date}")
    logging.info(f"   Forecast bulletin date: {bulletin_date}")

    # Get the dates to get the predictor from
    # For pentadal forecasts, the hydromet uses the sum of the last 3 days discharge.
    predictor_dates = fl.get_predictor_dates(today.strftime('%Y-%m-%d'), 3)
    # if predictor_dates is None, raise an error
    if predictor_dates is None:
        raise ValueError("The predictor dates are not valid.")


    logging.info(f"   Predictor dates: {predictor_dates}")

    logging.info("   ... done")
    print("   ... done")
    # endregion

    # === Define tags ===
    logging.info("Defining tags ...")
    print("Defining tags ...")
    # region Tag definitions
    pentad_tag = Tag(
        name="PENTAD",
        get_value_fn=tl.get_pentad(bulletin_date),
        description="Pentad of the month",
    )

    month_str_case1_tag = Tag(
        name="MONTH_STR_CASE1",
        get_value_fn=tl.get_month_str_case1(bulletin_date),
        description="Name of the month in a string in the first case",
    )

    month_str_case2_tag = Tag(
        name="MONTH_STR_CASE2",
        get_value_fn=tl.get_month_str_case2(bulletin_date),
        description="Name of the month in a string in the second case",
    )

    year_tag = Tag(
        name="YEAR",
        get_value_fn=tl.get_year(bulletin_date),
        description="Name of the month in a string in the second case",
    )

    day_start_pentad_tag = Tag(
        name="DAY_START",
        get_value_fn=tl.get_pentad_first_day(bulletin_date),
        description="Start day of the pentadal forecast",
    )

    day_end_pentad_tag = Tag(
        name="DAY_END",
        get_value_fn=tl.get_pentad_last_day(bulletin_date),
        description="End day of the pentadal forecast",
    )

    basin_tag = Tag(
        name="BASIN",
        get_value_fn=lambda obj: obj.basin,
        description="Basin of the gauge sites",
    )

    site_name_tag = Tag(
        name="PUNKT_NAME",
        get_value_fn=lambda obj: obj.punkt_name,
        description="Name of the gauge site",
    )

    river_name_tag = Tag(
        name="RIVER_NAME",
        get_value_fn=lambda obj: obj.river_name,
        description="Name of the river",
    )

    fc_qmin_tag = Tag(
        name="QMIN",
        get_value_fn=lambda obj: obj.fc_qmin,
        description="Minimum forecasted discharge range",
    )

    fc_qmax_tag = Tag(
        name="QMAX",
        get_value_fn=lambda obj: obj.fc_qmax,
        description="Maximum forecasted discharge range",
    )

    discharge_norm_tag = Tag(
        name="QNORM",
        get_value_fn=lambda obj: obj.qnorm,
        description="Norm discharge in current pentad",
    )

    percentage_of_norm_tag = Tag(
        name="PERC_NORM",
        get_value_fn=lambda obj: obj.perc_norm,
        description="Percentage of norm discharge in current pentad",
    )

    danger_level_tag = Tag(
        name="QDANGER",
        get_value_fn=lambda obj: obj.qdanger,
        description="Threshold for dangerous discharge",
    )

    dash_tag = Tag(
        name="DASH",
        get_value_fn="-",
        description="Dash"
    )


    logging.info("   ... done")
    print("   ... done")
    # endregion

    # === Read station data ===
    # region Read station data
    '''
    # For now we read older station data from excel sheets and newer station
    # data from the DB.
    # Once the upload for daily station data is implemented, we can read the
    # daily data for forecasting from the DB.

    # We will want to save the station data in objects that can later be used
    # for bulletin writing.
    '''
    # Read station data from Excel sheets
    logging.info("Reading daily discharge data ...")
    print("Reading daily discharge data ...")
    logging.info("-Reading discharge data from Excel sheets ...")
    print("-Reading discharge data from Excel sheets ...")

    # Get a list of the excel files containing the daily discharge data available
    # in the data/daily_runoff directory
    daily_discharge_files = os.listdir(os.getenv("ieasyforecast_daily_discharge_path"))

    # Print a warning if there are no files found in the ieasyforecast_daily_discharge_path
    if len(daily_discharge_files) == 0:
        print("WARNING: No files found in the directory data/daily_runoff.")
        # If in addition to seeing no excel sheets, we do not have access to the
        # iEasyHydro databae, throw an error and exit the script.
        if backend_has_access_to_db == False:
            print("ERROR: No files found in the directory data/daily_runoff and no access to the iEasyHydro database.")
            print("       Please check the data/daily_runoff directory and/or the access to the iEasyHydro database.")
            print("       No forecasts possible. Exiting the script.")
            logging.error("No files found in the directory data/daily_runoff and no access to the iEasyHydro database.")
            logging.error("Please check the data/daily_runoff directory and/or the access to the iEasyHydro database.")
            logging.error("No forecasts possible. Exiting the script.")
            exit()

    # If an excel file is open on Mac OS, it creates a temporary file with the
    # same name as the original file but starting with a tilde (~).
    # We want to ignore these files.
    daily_discharge_files = [file for file in daily_discharge_files if not file.startswith('~')]

    # Create a dataframe with the station IDs and the file names. The station
    # IDs are in the first part of the file names, before the first underscore.
    # The filenames are the full path to the files.
    library = pd.DataFrame(
        {
            "station": [file.split('_')[0] for file in daily_discharge_files],
            "file": [os.path.join(os.getenv("ieasyforecast_daily_discharge_path"), file) for file in daily_discharge_files]
        })

    # Initiate the dictionary for the data
    data_dict = {}

    # Create a vector with numbers from 2000 to 2023, that is the years for
    # which the data is available in the excel sheets.
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
    combined_data.loc[:,'Date'] = pd.to_datetime(combined_data.loc[:,'Date'])

    # Drop rows with missing discharge values
    combined_data.dropna(subset = 'Q_m3s', inplace=True)

    # Get the latest daily data from DB in addition. The data from the DB takes
    # precedence over the data from the Excel sheets.
    logging.info("-Reading latest daily data from DB ...")
    print('-Reading latest daily data from DB ...')
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

        except Exception as e:
            print(f'    No data for site {row["station"]} in DB.')
            continue

    # Combine the data from the DB and the Excel sheets. Check for duplicate
    # dates and keep the data from the DB.
    combined_data = pd.concat([combined_data, db_data]).drop_duplicates(subset=['Date', 'Code'], keep='first')

    # Filter combined_data for dates before today (to simulate offline mode)
    combined_data = combined_data[combined_data['Date'] <= today]

    # Prelimiary filter for outliers
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
    df_filtered.loc[:,'Date'] = pd.to_datetime(df_filtered.loc[:,'Date'])
    # Cast to datetime64 type
    df_filtered.loc[:,'Date'] = df_filtered.loc[:,'Date'].values.astype('datetime64[D]')

    # Sort df_filter with ascending Code and Date
    df_filtered.sort_values(by=['Code', 'Date'], inplace=True)

    logging.info("-Writing issue and forecast dates...")
    print("-Writing issue and forecast dates...")

    modified_data = fl.generate_issue_and_forecast_dates(pd.DataFrame(df_filtered), 'Date', 'Code', 'Q_m3s')
    # Print hydrdrograph data between June 1, 2023, and June 20, 2023, for site 15194

    # Drop the rows with 0 discharge_sum
    #data_df = modified_data[modified_data['discharge_sum'] != 0]

    # Calculate norm discharge for each pentad of the year
    logging.info("-Calculating norm discharge for each pentad of the year...")
    print("-Calculating norm discharge for each pentad of the year...")
    # For each Date in modified_data, calculate the pentad of the month
    modified_data['pentad'] = modified_data['Date'].apply(tl.get_pentad)
    modified_data['pentad_in_year'] = modified_data['Date'].apply(tl.get_pentad_in_year)

    # Groupp modified_data by Code and calculate the mean over discharge_avg
    # while ignoring NaN values
    norm_discharge = modified_data.reset_index(drop=True).groupby(['Code', 'pentad_in_year'], as_index=False)['discharge_avg'].apply(lambda x: x.mean(skipna=True))

    # Get the pentad of the bulletin date. This gives the pentad of the month.
    forecast_pentad = tl.get_pentad(bulletin_date)
    # Get the pentad of the year of the bulletin_date. We perform the linear
    # regression on all data from the same pentad of the year.
    forecast_pentad_of_year = tl.get_pentad_in_year(bulletin_date)

    # Now we need to write the discharge_avg for the current pentad to the site: Site
    for site in fc_sites:
        print(f'    Calculating norm discharge for site {site.code} ...')
        fl.Site.from_df_get_norm_discharge(site, forecast_pentad_of_year, norm_discharge)

    print(f'   {len(fc_sites)} Norm discharge calculated, namely:\n{[site.qnorm for site in fc_sites]}')
    logging.info(
        f'   {len(fc_sites)} Norm discharge gotten from df, namely:\n{[site.qnorm for site in fc_sites]}')

    logging.info("   ... done")
    print("   ... done")
    # endregion

    # === Write hydrograph data ===
    if (offline_mode == False) | (offline_mode == True):
        logging.info("Writing hydrgraph data ...")
        print("Writing hydrgraph data ...")
        # Reformat modified_data. Keep columns Q_m3s, Year, Code, discharge_avg, pentad.
        # Write the day of the year into a new column.
        hydrograph_data = modified_data
        # Convert the Date column to a datetime object
        hydrograph_data['Date'] = pd.to_datetime(hydrograph_data['Date'])

        # We do not filter February 29 in leap years here but in the dashboard.

        # Overwrite pentad in a month with pentad in a year
        hydrograph_data = tl.add_pentad_in_year_column(hydrograph_data)
        # print(hydrograph_data.head())
        hydrograph_data['day_of_year'] = hydrograph_data['Date'].dt.dayofyear
        hydrograph_data_day = hydrograph_data[['Code', 'Year', 'day_of_year', 'Q_m3s']]
        hydrograph_data_pentad = hydrograph_data[['Code', 'Year', 'pentad', 'discharge_avg']]

        # Reset the index of the hydrograph_data_pentad DataFrame
        hydrograph_data_pentad = hydrograph_data_pentad.reset_index(drop=True)
        hydrograph_data_day = hydrograph_data_day.reset_index(drop=True)

        # Reformat the data in the wide format. The day of the year, Code and pentad
        # are the index. The columns are the years. The values are the discharge_avg.
        hydrograph_pentad = hydrograph_data_pentad.pivot_table(index=['Code', 'pentad'], columns='Year', values='discharge_avg')
        hydrograph_day = hydrograph_data_day.pivot_table(index=['Code', 'day_of_year'], columns='Year', values='Q_m3s')

        # Reset the index of the hydrograph_pentad DataFrame
        hydrograph_pentad = hydrograph_pentad.reset_index()
        hydrograph_day = hydrograph_day.reset_index()

        # Convert pentad column to integer
        hydrograph_pentad['pentad'] = hydrograph_pentad['pentad'].astype(int)
        hydrograph_day['day_of_year'] = hydrograph_day['day_of_year'].astype(int)

        # Set 'Code' and 'pentad'/'day_of_year' as index again
        hydrograph_pentad.set_index(['Code', 'pentad'], inplace=True)
        hydrograph_day.set_index(['Code', 'day_of_year'], inplace=True)

        # Write this data to a dump (pickle the data) for subsequent visualization
        # in the forecast dashboard.
        hydrograph_pentad_file_csv = os.path.join(
            os.getenv("ieasyforecast_intermediate_data_path"),
            os.getenv("ieasyforecast_hydrograph_pentad_file"))

        # Write the hydrograph_pentad to csv
        hydrograph_pentad.to_csv(hydrograph_pentad_file_csv)

        hydrograph_day_file_csv = os.path.join(
            os.getenv("ieasyforecast_intermediate_data_path"),
            os.getenv("ieasyforecast_hydrograph_day_file"))

        # Write the hydrograph_day to csv
        hydrograph_day.to_csv(hydrograph_day_file_csv)


    logging.info("   ... done")
    print("   ... done")
    # === Perform linear regression ===
    # region Perform linear regression
    logging.info("Perform linear regression ...")
    print("Perform linear regression ...")

    # Returns a df filtered to the current pentad of the year
    result_df = fl.perform_linear_regression(
        modified_data, 'Code', 'pentad_in_year', 'discharge_sum', 'discharge_avg',
        int(forecast_pentad_of_year))

    logging.info("   ... done")
    print("   ... done")
    # endregion

    # === Calculate forecast ===
    logging.info("Getting predictor ...")
    print("Getting predictor ...")
    # region Calculate forecast
    # Get the sum of the last 3 days discharge from the data base
    # We get the predictor discharge from the DB only if we are not in
    # operational mode or if the date is after 2020-01-01 in offline mode. This
    # saves runtime.
    if offline_mode == False or today.date() > dt.datetime.strptime('2020-01-01', '%Y-%m-%d').date():
        logging.info("Getting predictor discharge from DB ...")
        # For each site in fc_sites, get the predictor discharge from the DB
        for site in fc_sites:
            fl.Site.from_DB_get_predictor(ieh_sdk, site, predictor_dates, lagdays=20)

        # Special case for virtual stations
        # For reservoir some inflows there is no data in the DB
        # Check if code 16936 is in fc_sites
        # If it is, then we need to get the predictor from the sum of other sites,
        # namely Naryn - Uch-Terek 16059 and lateral tributaries Chychkan 16096,
        # Uzun-Akmat 16100 and Torkent (no code in DB...)
        print('       ... from the DB ...')
        if '16936' in [site.code for site in fc_sites]:
            tok_contrib_sites = ["16059", "16096", "16100"]
            tok_sites = []
            for site in tok_contrib_sites:
                tok_site = fl.Site(site)
                tok_sites.append(tok_site)
            for site in tok_sites:
                fl.Site.from_DB_get_predictor(ieh_sdk, site, predictor_dates)
            # Sum the predictors of the contributing sites
            tok_predictor = sum([site.predictor for site in tok_sites])
            # Assign the sum to the predictor of the reservoir
            for site in fc_sites:
                if site.code == '16936':
                    site.predictor = tok_predictor

        logging.info(f'   {len(fc_sites)} Predictor discharge gotten from DB, namely:\n{[[site.code, site.predictor] for site in fc_sites]}')
        print(f'   {len(fc_sites)} Predictor discharge gotten from DB, namely:\n{[[site.code, site.predictor] for site in fc_sites]}')

    # If we don't find predictors in the database, we can check the data from
    # the Excel sheets in the result_df DataFrame.
    # Iterate through sites in fc_sites and see if we can find the predictor
    # in result_df.
    # This is mostly relevant for the offline mode.
    print('       ... from the df ...')
    for site in fc_sites:
        if site.predictor is None or float(site.predictor) <  0.0:
            logging.info(f'    No predictor found in DB for site {site.code}. Getting predictor from df ...')
            # print(site.code)
            # Get the data for the site
            # Here we need to use modified_data as result_df is filtered to the
            # current pentad of the year and does not contain predictor data.
            site_data = modified_data[modified_data['Code'] == site.code]
            # Get the predictor from the data for today
            fl.Site.from_df_get_predictor(site, site_data, [today])
            # print(fl.Site.from_df_get_predictor(site, site_data, predictor_dates))

    print(f'   {len(fc_sites)} Predictor discharge gotten from df, namely:\n{[[site.code, site.predictor] for site in fc_sites]}')
    logging.info(f'   {len(fc_sites)} Predictor discharge gotten from df, namely:\n{[[site.code, site.predictor] for site in fc_sites]}')
    # print result_df from December 24, 209 to January 6, 2010
    #print(result_df[(result_df['Date'] >= dt.datetime.strptime('2009-12-24', '%Y-%m-%d').date()) & (result_df['Date'] <= dt.datetime.strptime('2010-01-06', '%Y-%m-%d').date())])
    print('   ... done')
    logging.info("   ... done")


    # Perform forecast
    print("Performing forecast ...")
    logging.info("Performing forecast ...")

    # For each site, calculate the forecasted discharge
    for site in fc_sites:
        fl.Site.from_df_calculate_forecast(site, forecast_pentad_of_year, result_df)

    print(f'   {len(fc_sites)} Forecasts calculated, namely:\n{[[site.code, site.fc_qexp] for site in fc_sites]}')
    logging.info(f'   {len(fc_sites)} Forecasts calculated, namely:\n{[[site.code, site.fc_qexp] for site in fc_sites]}')

    print("   ... done")
    logging.info("   ... done")


    # endregion

    # === Get boundaries of forecast ===
    # region Calculate forecast boundaries
    logging.info("Calculating forecast boundaries ...")
    print("Calculating forecast boundaries ...")
    result2_df = fl.calculate_forecast_skill(result_df, 'Code', 'pentad_in_year', 'discharge_avg', 'forecasted_discharge')

    # Select columns in dataframe
    #print(result2_df.head(60)[['Date','Code','issue_date','discharge_sum','discharge_avg','pentad_in_year']])
    #print(result2_df.tail(60)[['Date','Code','issue_date','discharge_sum','discharge_avg','pentad_in_year']])
    #print(result2_df.columns)

    for site in fc_sites:
        fl.Site.from_df_get_qrange_discharge(site, forecast_pentad_of_year, result2_df)
        fl.Site.calculate_percentages_norm(site)

    logging.info("  ... done")
    print("  ... done")

    # endregion

    # === Write forecast bulletin ===
    # region Write forecast bulletin
    print("Writing forecast outputs ...")
    logging.info("Writing forecast bulletin ...")

    # Option to turn off bulletin writing, may be used during development only.
    write_bulletin = True

    # Format the date as a string in the format "YYYY_MM_DD"
    today_str = today.strftime("%Y_%m_%d")

    if write_bulletin == True:
        # Get the name of the template file from the environment variables
        bulletin_template_file = os.getenv("ieasyforecast_template_pentad_bulletin_file")

        # Construct the output filename using the formatted date
        bulletin_output_file = os.getenv("ieasyforecast_bulletin_file_name")
        filename = f"{today_str}_{bulletin_output_file}"

        report_generator = import_from_string(settings.template_generator_class)(
            tags=[pentad_tag, percentage_of_norm_tag, danger_level_tag, fc_qmax_tag,
                  fc_qmin_tag, day_end_pentad_tag, day_start_pentad_tag,
                discharge_norm_tag, basin_tag, river_name_tag, site_name_tag,
                month_str_case1_tag, month_str_case2_tag, year_tag,
                day_start_pentad_tag, day_end_pentad_tag, dash_tag],
            template=bulletin_template_file,
            requires_header=False,
            custom_settings=settings
        )

        report_generator.validate()
        report_generator.generate_report(list_objects=fc_sites, output_filename=filename)
        logging.info("   ... done")

    # If forecast sheets are written
    if excel_output == True:
        print("Writing forecast sheets ...")
        logging.info("Writing forecast sheets ...")

        # Get the name of the template file from the environment variables
        forecast_template_file = os.getenv("ieasyforecast_template_pentad_sheet_file")

        # Get the name of the output file from the environment variables
        bulletin_output_file = os.getenv("ieasyforecast_bulletin_file_name")

        # Tags
        fsheets_river_name_tag = Tag(
            name="FSHEETS_RIVER_NAME",
            get_value_fn=lambda obj: obj.get("river_name"),
            description="Name of the river",
            )

        pentad_fsheet_tag = Tag(
            name="PENTAD",
            get_value_fn=tl.get_pentad(bulletin_date),
            description="Pentad of the month",
            )

        year_fsheets_tag = Tag(
            name="YEARFSHEETS",
            get_value_fn=lambda obj: obj.get("year"),
            description="Year for which the forecast is produced"
            )

        qpavg_fsheets_tag = Tag(
            name="QPAVG",
            get_value_fn=lambda obj: obj.get("qpavg"),
            description="Average discharge for the current pentad and year"
            )

        qpsum_fsheets_tag = Tag(
            name="QPSUM",
            get_value_fn=lambda obj: obj.get("qpsum"),
            description="3-day discharge sum for the current pentad and year"
            )

        month_str_case1_fsheets_tag = Tag(
            name="MONTH_STR_CASE1",
            get_value_fn=tl.get_month_str_case1(bulletin_date),
            description="Name of the month in a string in the first case",
        )


        for site in fc_sites:

            # Construct the output filename using the formatted date
            filename = f"{today_str}_{site.code}_{bulletin_output_file}"

            # This tag is defined here because it's a general tag and it can't
            # receive a lambda function as a replace value, it needs to get a
            # concrete value so we create a new tag for each site
            punkt_name_tag = Tag('FSHEETS_PUNKT_NAME', site.punkt_name)

            # We need to use a trick here because we can use the ieasyreports
            # library only for printing one line per site. However, here we want
            # it to print several lines per site. Therefore, we create a dummy
            # Site object for each year in the data and print it.
            # Filter result2_df for the current site
            temp_df = result2_df[result2_df['Code'] == site.code].reset_index(drop=True)
            # Select columns from temp_df
            temp_df = temp_df[['Year', 'discharge_avg', 'discharge_sum', 'forecasted_discharge']]
            # the data frame is already filterd to the current pentad of the year
            temp_df = temp_df.dropna(subset=['forecasted_discharge'])

            site_data = []
            # iterate through all the years for the current site
            for year in temp_df['Year'].unique():
                df_year = temp_df[temp_df['Year'] == year]

                site_data.append({
                    'river_name': site.river_name + " " + site.punkt_name,
                    'year': str(year),
                    'qpavg': fl.round_discharge(df_year['discharge_avg'].mean()),
                    'qpsum': fl.round_discharge(df_year['discharge_sum'].mean())
                })

            # Add current year and current predictor to site_data
            site_data.append({
                'river_name': site.river_name + " " + site.punkt_name,
                'year': str(today.year),
                'qpavg': "",
                'qpsum': site.predictor
            })

            # directly instantiate the new generator
            report_generator = FakeHeaderTemplateGenerator(
                tags=[fsheets_river_name_tag, month_str_case1_fsheets_tag,
                      pentad_fsheet_tag,
                      year_fsheets_tag, qpavg_fsheets_tag, qpsum_fsheets_tag],
                template=forecast_template_file,
                requires_header=True,
                custom_settings=settings
            )
            report_generator.validate()
            report_generator.generate_report(
                list_objects=site_data,
                output_filename=filename
            )

        print("   ... done")
        logging.info("   ... done")

    # Write other output
    logging.info("Writing other output ...")

    # Write the forecasted discharge to a csv file. Print code, predictor,
    # fc_qmin, fc_qmax, fc_qexp, qnorm, perc_norm, qdanger for each site in
    # fc_sites
    # Write a file header if the file does not yet exist
    offline_forecast_results_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_results_file"))

    if os.path.exists(offline_forecast_results_file) == False:
        with open(offline_forecast_results_file, "w") as f:
            f.write("date,code,predictor,slope,intercept,delta,fc_qmin,fc_qmax,fc_qexp,qnorm,perc_norm,qdanger\n")
            f.flush()

    # Write the data to a csv file
    with open(offline_forecast_results_file, "a") as f:
        # Write the data
        for site in fc_sites:
            f.write(
                f"{today_str},{site.code},{site.predictor},{site.slope},{site.intercept},{site.delta},{site.fc_qmin},{site.fc_qmax},{site.fc_qexp},{site.qnorm},{site.perc_norm},{site.qdanger}\n"
            )
            f.flush()

    # endregion
    print("   ... done")

    # === Store last successful run date ===
    # region Store last successful run date
    print("Storing last successful run date ...")
    logging.info("Storing last successful run date ...")

    # Path to the file
    last_run_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_last_successful_run_file")
    )
    # Overwrite the file with the current date
    with open(last_run_file, "w") as f:
        f.write(today.strftime("%Y-%m-%d"))
        f.flush()

    print("   ... done")
    logging.info("   ... done")
    # endregion
# endregion
