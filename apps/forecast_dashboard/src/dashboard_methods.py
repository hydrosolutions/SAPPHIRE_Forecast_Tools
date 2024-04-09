import os
import sys
import pandas as pd
from dotenv import load_dotenv

# Get the absolute path of the directory containing the current script
cwd = os.getcwd()

# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', '..', 'iEasyHydroForecast')
# Test if the forecast dir exists and print a warning if it does not
if not os.path.isdir(forecast_dir):
    raise Exception("Directory not found: " + forecast_dir)

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)
# Import the modules from the forecast library
import tag_library as tl
import forecast_library as fl



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
    in_docker_flag = str(os.getenv("IN_DOCKER_CONTAINER"))
    if in_docker_flag == "True":
        path_to_env_file = "apps/config/.env"
        # Test if the .env file exists
        if not os.path.isfile(path_to_env_file):
            raise Exception("File not found: " + path_to_env_file)
        print("Running in Docker container")
    elif os.getenv("SAPPHIRE_TEST_ENV") == "True":
        path_to_env_file = "backend/tests/test_files/.env_develop_test"
    elif os.getenv("SAPPHIRE_OPDEV_ENV") == "True":
        path_to_env_file = "../config/.env_develop_kghm"
    else:
        # Test if the .env file exists
        path_to_env_file = "../config/.env_develop"
        if not os.path.isfile(path_to_env_file):
            raise Exception("File not found: " + path_to_env_file)
        print("Running locally")
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

def get_icon_path(in_docker_flag):
    # Icon
    if in_docker_flag == "True":
        icon_path = os.path.join("apps", "forecast_dashboard", "www", "Pentad.jpg")
    else:
        icon_path = os.path.join("www", "Pentad.jpg")

    # Test if file exists and thorw an error if not
    if not os.path.isfile(icon_path):
        raise Exception("File not found: " + icon_path)

    return icon_path

def read_hydrograph_day_file(today):

    hydrograph_day_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_hydrograph_day_file"))
    # Test if file exists and thorw an error if not
    if not os.path.isfile(hydrograph_day_file):
        raise Exception("File not found: " + hydrograph_day_file)

    hydrograph_day_all = pd.read_csv(hydrograph_day_file).reset_index(drop=True)
    # Test if hydrograph_day_all is empty
    if hydrograph_day_all.empty:
        raise Exception("File is empty: " + hydrograph_day_file)

    hydrograph_day_all["day_of_year"] = hydrograph_day_all["day_of_year"].astype(int)
    hydrograph_day_all['Code'] = hydrograph_day_all['Code'].astype(str)
    # Sort all columns in ascending Code and pentad order
    hydrograph_day_all = hydrograph_day_all.sort_values(by=["Code", "day_of_year"])
    # Remove the day 366 (only if we are not in a leap year)
    # Test if current year is a leap year
    if today.year % 4 != 0:
        hydrograph_day_all = hydrograph_day_all[hydrograph_day_all["day_of_year"] != 366]

    return hydrograph_day_all

def read_hydrograph_pentad_file():
    hydrograph_pentad_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_hydrograph_pentad_file"))
    # Test if file exists and thorw an error if not
    if not os.path.isfile(hydrograph_pentad_file):
        raise Exception("File not found: " + hydrograph_pentad_file)

    # Read hydrograph data - pentad
    hydrograph_pentad_all = pd.read_csv(hydrograph_pentad_file).reset_index(drop=True)
    # Test if hydrograph_pentad_all is empty
    if hydrograph_pentad_all.empty:
        raise Exception("File is empty: " + hydrograph_pentad_file)

    hydrograph_pentad_all["pentad"] = hydrograph_pentad_all["pentad"].astype(int)
    hydrograph_pentad_all['Code'] = hydrograph_pentad_all['Code'].astype(str)
    # Sort all columns in ascending Code and pentad order
    hydrograph_pentad_all = hydrograph_pentad_all.sort_values(by=["Code", "pentad"])

    return hydrograph_pentad_all

def read_forecast_results_file():
    forecast_results_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_results_file")
    )
    # Test if file exists and thorw an error if not
    if not os.path.isfile(forecast_results_file):
        raise Exception("File not found: " + forecast_results_file)
    # Read forecast results
    forecast_pentad = pd.read_csv(forecast_results_file)
    # Test if forecast_pentad is empty
    if forecast_pentad.empty:
        raise Exception("File is empty: " + forecast_results_file)
    # Convert the date column to datetime. The format of the date string is %Y-%m-%d.
    forecast_pentad['Date'] = pd.to_datetime(forecast_pentad['date'], format='%Y-%m-%d')
    # Make sure the date column is in datetime64 format
    forecast_pentad['Date'] = forecast_pentad['Date'].astype('datetime64[ns]')
    # The forecast date is the date on which the forecast was produced. For
    # visualization we need to have the date for which the forecast is valid.
    # We add 1 day to the forecast Date to get the valid date.
    forecast_pentad['Date'] = forecast_pentad['Date'] + pd.Timedelta(days=1)
    # Convert the code column to string
    forecast_pentad['code'] = forecast_pentad['code'].astype(str)
    # Check if there are duplicates for Date and code columns. If yes, only keep the
    # last one
    # Print all values where column code is 15102. Sort the values by Date in ascending order.
    # Print the last 20 values.
    forecast_pentad = forecast_pentad.drop_duplicates(subset=['Date', 'code'], keep='last').sort_values('Date')
    # Get the pentad of the year.
    forecast_pentad = tl.add_pentad_in_year_column(forecast_pentad)
    # Cast pentad column no number
    forecast_pentad['pentad'] = forecast_pentad['pentad'].astype(int)

    return forecast_pentad

def read_stations_from_file(station_list):

    all_stations_file = os.path.join(
        os.getenv("ieasyforecast_configuration_path"),
        os.getenv("ieasyforecast_config_file_all_stations")
    )
    # Test if file exists and thorw an error if not
    if not os.path.isfile(all_stations_file):
        raise Exception("File not found: " + all_stations_file)

    # Read stations json
    with open(all_stations_file, "r") as json_file:
        all_stations = fl.load_all_station_data_from_JSON(all_stations_file)
    # Convert the code column to string
    all_stations['code'] = all_stations['code'].astype(str)
    # Left-join all_stations['code', 'river_ru', 'punkt_ru'] by 'Code' = 'code'
    station_df=pd.DataFrame(station_list, columns=['Code'])
    station_df=station_df.merge(all_stations.loc[:,['code','river_ru','punkt_ru']],
                            left_on='Code', right_on='code', how='left')

    # Paste together the columns Code, river_ru and punkt_ru to a new column
    # station_labels. river and punkt names are currently only available in Russian.
    station_df['station_labels'] = station_df['Code'] + ' - ' + station_df['river_ru'] + ' ' + station_df['punkt_ru']

    # Update the station list with the new station labels
    station_list = station_df['station_labels'].tolist()

    return station_list, all_stations, station_df

def add_labels_to_hydrograph_day_all(hydrograph_day_all, all_stations):
    hydrograph_day_all = hydrograph_day_all.merge(
        all_stations.loc[:,['code','river_ru','punkt_ru']],
                            left_on='Code', right_on='code', how='left')
    hydrograph_day_all['station_labels'] = hydrograph_day_all['Code'] + ' - ' + hydrograph_day_all['river_ru'] + ' ' + hydrograph_day_all['punkt_ru']
    # Remove the columns river_ru and punkt_ru
    hydrograph_day_all = hydrograph_day_all.drop(columns=['code', 'river_ru', 'punkt_ru'])

    return hydrograph_day_all

def add_labels_to_hydrograph_pentad_all(hydrograph_pentad_all, all_stations):
    hydrograph_pentad_all = hydrograph_pentad_all.merge(
        all_stations.loc[:,['code','river_ru','punkt_ru']],
                            left_on='Code', right_on='code', how='left')
    hydrograph_pentad_all['station_labels'] = hydrograph_pentad_all['Code'] + ' - ' + hydrograph_pentad_all['river_ru'] + ' ' + hydrograph_pentad_all['punkt_ru']
    # Remove the columns river_ru and punkt_ru
    hydrograph_pentad_all = hydrograph_pentad_all.drop(columns=['code', 'river_ru', 'punkt_ru'])

    return hydrograph_pentad_all

def add_labels_to_forecast_pentad_df(forecast_pentad, all_stations):
    forecast_pentad = forecast_pentad.merge(
        all_stations.loc[:,['code','river_ru','punkt_ru']],
        left_on='code', right_on='code', how='left')
    forecast_pentad['station_labels'] = forecast_pentad['code'] + ' - ' + forecast_pentad['river_ru'] + ' ' + forecast_pentad['punkt_ru']
    forecast_pentad = forecast_pentad.drop(columns=['river_ru', 'punkt_ru'])

    return forecast_pentad

def preprocess_hydrograph_day_data(hydrograph_day, today):

    # Test that we only have data for one unique code in the data frame.
    # If we have more than one code, we raise an exception.
    if len(hydrograph_day["Code"].unique()) > 1:
        raise ValueError("Data frame contains more than one unique code.")

    # Drop the Code column
    hydrograph_day = hydrograph_day.drop(columns=["Code", "station_labels"])

    # Drop the index
    hydrograph_day = hydrograph_day.reset_index(drop=True)

    # Melt the DataFrame to simplify the column index
    hydrograph_day = hydrograph_day.melt(id_vars=["day_of_year"], var_name="Year", value_name="value")

    # Set index to day of year
    hydrograph_day = hydrograph_day.set_index("day_of_year")

    # Calculate norm and percentiles for each day_of_year over all Years in the hydrograph
    norm = hydrograph_day.groupby("day_of_year")["value"].mean()
    perc_05 = hydrograph_day.groupby("day_of_year")["value"].quantile(0.05)
    perc_25 = hydrograph_day.groupby("day_of_year")["value"].quantile(0.25)
    perc_75 = hydrograph_day.groupby("day_of_year")["value"].quantile(0.75)
    perc_95 = hydrograph_day.groupby("day_of_year")["value"].quantile(0.95)
    # For validation of the method, also print minimum and maximum values of the
    # hydrograph_day DataFrame
    min_val = hydrograph_day.groupby("day_of_year")["value"].min()
    max_val = hydrograph_day.groupby("day_of_year")["value"].max()

    # Create a new DataFrame with the calculated values
    hydrograph_norm_perc = pd.DataFrame({
        "day_of_year": norm.index,
        "Norm": norm.values,
        "Perc_05": perc_05.values,
        "Perc_25": perc_25.values,
        "Perc_75": perc_75.values,
        "Perc_95": perc_95.values,
        "Min": min_val.values,
        "Max": max_val.values
    })

    # Get the current year from the system date
    current_year = today.year
    current_year_col = str(current_year)

    # Add the current year data to the DataFrame
    # Attention: this can become an empty selection if we don't have any data
    # for the current year yet (e.g. when a new year has just started and no
    # forecasts have been produced for the new year yet).
    # To avoid the error, we need to check if the current year is in the
    # hydrograph_day DataFrame. If it is not, we display the latest year data.
    if current_year_col in hydrograph_day["Year"].values:
        current_year_data = hydrograph_day[hydrograph_day["Year"] == str(current_year_col)]["value"].values
    else:
        current_year_col = hydrograph_day["Year"].values[-1]
        current_year_data = hydrograph_day[hydrograph_day["Year"] == str(current_year_col)]["value"].values
        # Print a warning that the current year data is not available and that
        # the previous year data is shown as a fallback
        print("WARNING: No river runoff data available from the current year, " + str(current_year) + ". Showing data from the last year, " + current_year_col + ", data is available.")

    hydrograph_norm_perc["current_year"] = current_year_data

    return hydrograph_norm_perc

def preprocess_hydrograph_pentad_data(hydrograph_pentad: pd.DataFrame, today) -> pd.DataFrame:
    '''
    Calculates the norm and percentiles for each pentad over all Years in the
    input hydrograph. Note that the input hydrograph is filtered to only one
    station.
    '''
    # Test if we only have data for one unique code in the data frame.
    # If we have more than one code, we raise an exception.
    if len(hydrograph_pentad["Code"].unique()) > 1:
        raise ValueError("Data frame contains more than one unique code.")

    # Drop the Code column
    hydrograph_pentad = hydrograph_pentad.drop(columns=["Code", "station_labels"])
    # Melt the DataFrame to simplify the column index
    hydrograph_pentad = hydrograph_pentad.melt(id_vars=["pentad"], var_name="Year", value_name="value")

    # Set index to pentad of year
    hydrograph_pentad = hydrograph_pentad.set_index("pentad")

    # Calculate norm and percentiles for each day_of_year over all Years in the hydrograph
    norm = hydrograph_pentad.groupby("pentad")["value"].mean().reset_index(drop=True)
    perc_05 = hydrograph_pentad.groupby("pentad")["value"].quantile(0.05).reset_index(drop=True)
    perc_25 = hydrograph_pentad.groupby("pentad")["value"].quantile(0.25).reset_index(drop=True)
    perc_75 = hydrograph_pentad.groupby("pentad")["value"].quantile(0.75).reset_index(drop=True)
    perc_95 = hydrograph_pentad.groupby("pentad")["value"].quantile(0.95).reset_index(drop=True)
    min_val = hydrograph_pentad.groupby("pentad")["value"].min().reset_index(drop=True)
    max_val = hydrograph_pentad.groupby("pentad")["value"].max().reset_index(drop=True)

    # Create a new DataFrame with the calculated values
    hydrograph_norm_perc = pd.DataFrame({
        "pentad": norm.index,
        "Norm": norm.values,
        "Perc_05": perc_05.values,
        "Perc_25": perc_25.values,
        "Perc_75": perc_75.values,
        "Perc_95": perc_95.values,
        "Min": min_val.values,
        "Max": max_val.values
    })

    # Get the current year from the system date
    current_year = today.year
    current_year_col = str(current_year)

    # If current year data is not available, use the previous year data
    if current_year_col not in hydrograph_pentad["Year"].values:
        current_year_col = hydrograph_pentad["Year"].values[-1]
    current_year_data = hydrograph_pentad[hydrograph_pentad["Year"] == str(current_year_col)]["value"].values

    # Add the current year data to the DataFrame
    hydrograph_norm_perc["current_year"] = current_year_data
    hydrograph_norm_perc["pentad"] = hydrograph_norm_perc["pentad"] + 1

    return hydrograph_norm_perc






# Customization of the Bokeh plots

def remove_bokeh_logo(plot, element):
    plot.state.toolbar.logo = None






