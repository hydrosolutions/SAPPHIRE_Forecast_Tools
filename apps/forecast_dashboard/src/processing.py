import os
import sys
import pandas as pd
import numpy as np
import param
import re
from concurrent.futures import ThreadPoolExecutor

from .gettext_config import _

import panel as pn

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
import setup_library as sl
import tag_library as tl
import forecast_library as fl


class DataReloader(param.Parameterized):
    data_needs_reload = param.Boolean(default=False)


data_reloader = DataReloader()


# --- Configuration -----------------------------------------------------------
def get_icon_path(in_docker_flag):
    # Icon
    # if in_docker_flag == "True":
    #    icon_path = os.path.join("apps", "forecast_dashboard", "www", "Pentad.png")
    # else:
    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        icon_path = os.path.join("www", "Pentad.png")
    else:
        icon_path = os.path.join("www", "Dekad.png")

    # Test if file exists and thorw an error if not
    if not os.path.isfile(icon_path):
        raise Exception("File not found: " + icon_path)

    return icon_path


# --- Reading data -----------------------------------------------------------
def filter_dataframe_for_selected_stations(dataframe, code_col, selected_stations):
    """
    Filter the data frame for the selected stations.

    Args:
        dataframe (pd.DataFrame): The data frame.
        code_col (str): The column name for the station codes.
        selected_stations (list): The selected stations.

    Returns:
        pd.DataFrame: The filtered data frame.
    """
    return dataframe[dataframe[code_col].isin(selected_stations)]


def parse_dates(date_str):
    for fmt in ('%d.%m.%Y', '%Y-%m-%d'):
        try:
            return pd.to_datetime(date_str, format=fmt)
        except ValueError:
            continue
    return pd.NaT


def read_rram_forecast_data(file_mtime):
    """
    Reads the forecasts from the RRAM model from the intermediate data directory.
    """
    cache_key = 'rram_forecast_data'
    if cache_key in pn.state.cache:
        cached_mtime, rram_forecast = pn.state.cache[cache_key]
        if cached_mtime == file_mtime:
            return rram_forecast

    filepath = os.getenv('ieasyhydroforecast_PATH_TO_RESULT')
    # List all csv files in the directory and in the sub-directories
    # of the directory and read the forecast data
    rram_forecast = pd.DataFrame()
    for root, dirs, files in os.walk(filepath):
        # Extract the 5-digit number in the root
        code = re.findall(r'\d{5}', root)
        for file in files:
            if file.startswith('daily') and file.endswith('.csv'):
                filename = os.path.join(root, file)
                temp_data = pd.read_csv(filename)
                # Add a column code to the data frame
                temp_data['code'] = code[0]
                # Add a column model_short to the data frame
                temp_data['model_short'] = 'RRAM'
                rram_forecast = pd.concat([rram_forecast, temp_data], ignore_index=True)
    # Cast forecast_date and date columns to datetime
    # rram_forecast['forecast_date'] = rram_forecast['forecast_date'].apply(parse_dates)
    # rram_forecast['date'] = rram_forecast['date'].apply(parse_dates)
    rram_forecast['forecast_date'] = pd.to_datetime(rram_forecast['forecast_date'], errors='coerce')
    rram_forecast['date'] = pd.to_datetime(rram_forecast['date'], errors='coerce')

    # Only keep latest forecast for each station
    rram_forecast = rram_forecast.sort_values(by=['code', 'forecast_date']).groupby('code').tail(17)

    # Store in cache
    pn.state.cache[cache_key] = (file_mtime, rram_forecast)
    return rram_forecast


def read_daily_probabilistic_ml_forecasts_pentad(
        filepath,
        model,
        model_long,
        model_short):
    """
    Reads in forecast results from probabilistic machine learning models for the pentadal forecast

    Args:
    filepath (str): The path to the file with the forecast results.
    model (str): The model to read the forecast results from. Allowed values are
        'TFT', 'TIDE', 'TSMIXER', and 'ARIMA'.
    model_long (str): The long name of the model.
    model_short (str): The short name of the model.

    Returns:
    forecast (pandas.DataFrame): The forecast results for the pentadal forecast horizon.
    """
    # Read the forecast results
    # daily_data = pd.read_csv(filepath, parse_dates=["date", "forecast_date"])
    daily_data = pd.read_csv(filepath)
    daily_data['date'] = pd.to_datetime(daily_data['date'], errors='coerce')
    daily_data['forecast_date'] = pd.to_datetime(daily_data['forecast_date'], errors='coerce')

    # Rename the column forecast_date to date and Q50 to forecasted_discharge.
    # In the case of the ARIMA model, we don't have quantiles but rename the
    # column Q to forecasted_discharge.
    daily_data.rename(
        columns={"Q50": "forecasted_discharge",  # For ml models
                 "Q": "forecasted_discharge"},  # For the ARIMA model
        inplace=True)

    # Add a column model to the dataframe
    daily_data["model_long"] = model_long
    daily_data["model_short"] = model_short

    # Cast code column to string
    daily_data['code'] = daily_data['code'].astype(str)

    # Only keep the rows where forecast_date is equal to the most recent forecast
    # date for each station
    latest_forecast_date = daily_data['forecast_date'].max()
    daily_data = daily_data[daily_data['forecast_date'] == latest_forecast_date]

    return daily_data


def read_machine_learning_forecasts_pentad(model, file_mtime):
    '''
    Reads forecast results from the machine learning model for the pentadal
    forecast horizon.

    Args:
    model (str): The machine learning model to read the forecast results from.
        Allowed values are 'TFT', 'TIDE', 'TSMIXER', and 'ARIMA'.
    '''
    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    cache_key = f'read_machine_learning_forecasts_{horizon}_{model}'
    if cache_key in pn.state.cache:
        cached_mtime, forecast = pn.state.cache[cache_key]
        if cached_mtime == file_mtime:
            return forecast

    if model == 'TFT':
        filename = f"{horizon}_{model}_forecast_latest.csv"
        model_long = "Temporal-Fusion Transformer (TFT)"
        model_short = "TFT"
    elif model == 'TIDE':
        filename = f"{horizon}_{model}_forecast_latest.csv"
        model_long = "Time-Series Dense Encoder (TiDE)"
        model_short = "TiDE"
    elif model == 'TSMIXER':
        filename = f"{horizon}_{model}_forecast_latest.csv"
        model_long = "Time-Series Mixer (TSMixer)"
        model_short = "TSMixer"
    elif model == 'ARIMA':
        filename = f"{horizon}_{model}_forecast_latest.csv"
        model_long = "AutoRegressive Integrated Moving Average (ARIMA)"
        model_short = "ARIMA"
    else:
        raise ValueError("Invalid model. Valid models are: 'TFT', 'TIDE', 'TSMIXER', 'ARIMA'")

    # Read environment variables to construct the file path with forecast
    # results for the machine learning and ARIMA models
    intermediate_data_path = os.getenv("ieasyforecast_intermediate_data_path")
    subfolder = os.getenv("ieasyhydroforecast_OUTPUT_PATH_DISCHARGE")
    filepath = os.path.join(intermediate_data_path, subfolder, model, filename)

    # Test if the fielpath exists
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File {filepath} not found")

    forecast = read_daily_probabilistic_ml_forecasts_pentad(filepath, model, model_long, model_short)

    # Store in cache
    pn.state.cache[cache_key] = (file_mtime, forecast)
    return forecast


def read_ml_forecast_data(file_mtime):
    """
    Reads the forecasts from the ML model from the intermediate data directory.
    """
    cache_key = 'ml_forecast_data'
    if cache_key in pn.state.cache:
        cached_mtime, ml_forecast = pn.state.cache[cache_key]
        if cached_mtime == file_mtime:
            return ml_forecast

    # tide = read_machine_learning_forecasts_pentad('TIDE', file_mtime)
    # tft = read_machine_learning_forecasts_pentad('TFT', file_mtime)
    # tsmixer = read_machine_learning_forecasts_pentad('TSMIXER', file_mtime)
    # arima = read_machine_learning_forecasts_pentad('ARIMA', file_mtime)

    with ThreadPoolExecutor() as executor:
        # List of parameters for each forecast type
        forecast_types = ['TIDE', 'TFT', 'TSMIXER', 'ARIMA']

        # Use executor.map to apply read_machine_learning_forecasts_pentad concurrently
        results = executor.map(read_machine_learning_forecasts_pentad, forecast_types,
                               [file_mtime] * len(forecast_types))

        # Map the results to their respective variables
        tide, tft, tsmixer, arima = results

    # Calculate the Neural Ensemble (NE) forecast as the average of the TFT,
    # TIDE, and TSMIXER forecasts
    ne = pd.concat([tide, tft, tsmixer], ignore_index=True)
    ne = ne.drop(columns=['model_long', 'model_short']).groupby(['code', 'date', 'forecast_date']).mean().reset_index()
    ne['model_long'] = 'Neural Ensemble (NE)'
    ne['model_short'] = 'NE'
    # Concatenate the data frames
    ml_forecast = pd.concat([tide, tft, tsmixer, ne, arima], ignore_index=True)
    # print(f"Head of ml_forecast:\n{ml_forecast.head()}")
    # Cast forecast_date and date columns to datetime
    ml_forecast['forecast_date'] = ml_forecast['forecast_date'].apply(parse_dates)

    # Store in cache
    pn.state.cache[cache_key] = (file_mtime, ml_forecast)
    return ml_forecast


def read_hydrograph_day_file(file_mtime):
    """
    Reads the hydrograph_day file from the intermediate data directory.

    Filters the available data to only include the stations selected for
    pentadal forecasting.

    Returns:
        pd.DataFrame: The hydrograph_day data.

    Raises:
        Exception: If the file is not found or empty.
    """
    cache_key = 'hydrograph_day_file'
    if cache_key in pn.state.cache:
        cached_mtime, hydrograph_day_all = pn.state.cache[cache_key]
        if cached_mtime == file_mtime:
            return hydrograph_day_all

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
    hydrograph_day_all['code'] = hydrograph_day_all['code'].astype(str)
    # Sort all columns in ascending Code and pentad order
    hydrograph_day_all = hydrograph_day_all.sort_values(by=["code", "day_of_year"])

    # Cast date column to datetime
    hydrograph_day_all['date'] = pd.to_datetime(hydrograph_day_all['date'], format='%Y-%m-%d')

    # Print tail of hydrograph_day_all for code == 15194
    #print(
    #    f"DEBUG: read_hydrograph_day_file: hydrograph_day_all:\n{hydrograph_day_all[hydrograph_day_all['code'] == '15194'].head()}")
    #print(
    #    f"DEBUG: read_hydrograph_day_file: hydrograph_day_all:\n{hydrograph_day_all[hydrograph_day_all['code'] == '15194'].tail()}")

    # Store in cache
    pn.state.cache[cache_key] = (file_mtime, hydrograph_day_all)
    return hydrograph_day_all


def read_hydrograph_day_data_for_pentad_forecasting(iahhf_selected_stations, file_mtime):
    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    cache_key_base = f'hydrograph_day_data_{horizon}_forecasting'
    stations_tuple = tuple(iahhf_selected_stations) if iahhf_selected_stations is not None else None
    cache_key = (cache_key_base, stations_tuple)
    if cache_key in pn.state.cache:
        cached_mtime, hydrograph_day_all = pn.state.cache[cache_key]
        if cached_mtime == file_mtime:
            return hydrograph_day_all

    # Read hydrograph data with daily values
    hydrograph_day_all = read_hydrograph_day_file(file_mtime)
    #print(f"DEBUG: read_hydrograph_day_data_for_pentad_forecasting: hydrograph_day_all:\n{hydrograph_day_all.head()}")

    # if we get data from iEasyHydro, we do the following
    if iahhf_selected_stations is not None:
        print(f"DEBUG: read_hydrograph_day_data_for_pentad_forecasting: iahhf_selected_stations: {iahhf_selected_stations}")    
        # Filter the data frame for the selected stations
        hydrograph_day_all = filter_dataframe_for_selected_stations(
            hydrograph_day_all, "code", iahhf_selected_stations)
    else:
        print(f"DEBUG: read_hydrograph_day_data_for_pentad_forecasting: no iahhf_selected_stations")
        # Get station ids of stations selected for forecasting
        filepath = os.path.join(
            os.getenv("ieasyforecast_configuration_path"),
            os.getenv("ieasyforecast_config_file_station_selection"))
        selected_stations = fl.load_selected_stations_from_json(filepath)
        #print(f"\nDEBUG: read_hydrograph_day_data_for_pentad_forecasting: selected_stations: {selected_stations}")

        # Filter data for selected stations
        hydrograph_day_all = hydrograph_day_all[hydrograph_day_all["code"].isin(selected_stations)]

        # print unique values of code column
        #print(f"DEBUG: read_hydrograph_day_data_for_pentad_forecasting: unique values of code column:\n{hydrograph_day_all['code'].unique()}")
        
        # Test if there is an environment variable ieasyforecast_restrict_stations_file
        if os.getenv("ieasyforecast_restrict_stations_file"):
            filepath = os.path.join(
                os.getenv("ieasyforecast_configuration_path"),
                os.getenv("ieasyforecast_restrict_stations_file"))
            # Only read the file if it is present
            if os.path.isfile(filepath):
                # Read the restricted stations from the environment variable
                restricted_stations = fl.load_selected_stations_from_json(filepath)
                # Filter data for restricted stations
                hydrograph_day_all = hydrograph_day_all[hydrograph_day_all["code"].isin(restricted_stations)]

    #print(f"DEBUG: read_hydrograph_day_data_for_pentad_forecasting: selected_stations: {iahhf_selected_stations}")
    #print(f"DEBUG: hydrograph_day_all:\n{hydrograph_day_all.head()}")

    return hydrograph_day_all


def read_hydrograph_pentad_file(file_mtime):
    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        horizon_in_year = "pentad_in_year"
        hydrograph_file = os.getenv("ieasyforecast_hydrograph_pentad_file")
    else:
        horizon_in_year = "decad_in_year"
        hydrograph_file = os.getenv("ieasyforecast_hydrograph_decad_file")

    cache_key = f'hydrograph_{horizon}_file'
    if cache_key in pn.state.cache:
        cached_mtime, hydrograph_pentad_all = pn.state.cache[cache_key]
        if cached_mtime == file_mtime:
            return hydrograph_pentad_all

    hydrograph_pentad_file = os.path.join(os.getenv("ieasyforecast_intermediate_data_path"), hydrograph_file)
    # Test if file exists and thorw an error if not
    if not os.path.isfile(hydrograph_pentad_file):
        raise Exception("File not found: " + hydrograph_pentad_file)

    # Read hydrograph data - pentad
    hydrograph_pentad_all = pd.read_csv(hydrograph_pentad_file).reset_index(drop=True)
    # Test if hydrograph_pentad_all is empty
    if hydrograph_pentad_all.empty:
        raise Exception("File is empty: " + hydrograph_pentad_file)

    hydrograph_pentad_all[horizon_in_year] = hydrograph_pentad_all[horizon_in_year].astype(int)
    hydrograph_pentad_all['code'] = hydrograph_pentad_all['code'].astype(str)
    hydrograph_pentad_all['date'] = pd.to_datetime(hydrograph_pentad_all['date'], format='%Y-%m-%d')
    # Sort all columns in ascending Code and pentad order
    hydrograph_pentad_all = hydrograph_pentad_all.sort_values(by=["code", horizon_in_year])

    # Store in cache
    pn.state.cache[cache_key] = (file_mtime, hydrograph_pentad_all)
    return hydrograph_pentad_all


def read_hydrograph_pentad_data_for_pentad_forecasting(iahhf_selected_stations, file_mtime):
    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    cache_key_base = f'hydrograph_{horizon}_data_{horizon}_forecasting'
    stations_tuple = tuple(iahhf_selected_stations) if iahhf_selected_stations is not None else None
    cache_key = (cache_key_base, stations_tuple)
    if cache_key in pn.state.cache:
        cached_mtime, hydrograph_pentad_all = pn.state.cache[cache_key]
        if cached_mtime == file_mtime:
            return hydrograph_pentad_all

    # Read hydrograph data with pentad values
    hydrograph_pentad_all = read_hydrograph_pentad_file(file_mtime)

    if iahhf_selected_stations is not None:
        # Filter the data frame for the selected stations
        hydrograph_pentad_all = filter_dataframe_for_selected_stations(
            hydrograph_pentad_all, "code", iahhf_selected_stations)
    else:
        # Get station ids of stations selected for forecasting
        filepath = os.path.join(
            os.getenv("ieasyforecast_configuration_path"),
            os.getenv("ieasyforecast_config_file_station_selection"))
        selected_stations = fl.load_selected_stations_from_json(filepath)

        # Filter data for selected stations
        hydrograph_pentad_all = hydrograph_pentad_all[hydrograph_pentad_all["code"].isin(selected_stations)]

        # Test if there is an environment variable ieasyforecast_restrict_stations_file
        if os.getenv("ieasyforecast_restrict_stations_file"):
            filepath = os.path.join(
                os.getenv("ieasyforecast_configuration_path"),
                os.getenv("ieasyforecast_restrict_stations_file"))
            # Only read the file if it is present
            if os.path.isfile(filepath):
                # Read the restricted stations from the environment variable
                restricted_stations = fl.load_selected_stations_from_json(filepath)
                # Filter data for restricted stations
                hydrograph_pentad_all = hydrograph_pentad_all[hydrograph_pentad_all["code"].isin(restricted_stations)]

    # Store in cache
    pn.state.cache[cache_key] = (file_mtime, hydrograph_pentad_all)
    return hydrograph_pentad_all


def read_linreg_forecast_data(iehhf_selected_stations, file_mtime):
    cache_key_base = 'linreg_forecast_data'
    stations_tuple = tuple(iehhf_selected_stations) if iehhf_selected_stations is not None else None
    cache_key = (cache_key_base, stations_tuple)
    if cache_key in pn.state.cache:
        cached_mtime, linreg_forecast = pn.state.cache[cache_key]
        if cached_mtime == file_mtime:
            return linreg_forecast

    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        forecast_results_file = os.path.join(
            os.getenv("ieasyforecast_intermediate_data_path"),
            os.getenv("ieasyforecast_analysis_pentad_file")
        )
    else:
        forecast_results_file = os.path.join(
            os.getenv("ieasyforecast_intermediate_data_path"),
            os.getenv("ieasyforecast_analysis_decad_file")
        )
    # No possibility to shorten that file. Need all results for linear regression
    # pane in forecast tab
    # forecast_results_file = forecast_results_file.replace('.csv', '_latest.csv')

    # Test if file exists and thorw an error if not
    if not os.path.isfile(forecast_results_file):
        raise Exception("File not found: " + forecast_results_file)

    # Read the file
    linreg_forecast = pd.read_csv(forecast_results_file)

    # Test if linreg_forecast is empty
    if linreg_forecast.empty:
        raise Exception("File is empty: " + forecast_results_file)

    # Convert the date column to datetime. The format of the date string is %Y-%m-%d.
    linreg_forecast['date'] = pd.to_datetime(linreg_forecast['date'], format='%Y-%m-%d')

    # Date is the forecast issue date. To visualize the data for the correct pentad,
    # we need to add 1 day to the date and re-evaluate the pentad & pentad in year
    linreg_forecast['Date'] = linreg_forecast['date'] + pd.Timedelta(days=1)

    # Sort by code and Date
    linreg_forecast = linreg_forecast.sort_values(by=['code', 'Date'])

    # Drop duplicate rows for the same code and Date. Keep the last row.
    linreg_forecast = linreg_forecast.drop_duplicates(subset=['code', 'Date'], keep='last')

    # Convert code column to str
    linreg_forecast['code'] = linreg_forecast['code'].astype(str)

    if iehhf_selected_stations is not None:
        # Filter the data frame for the selected stations
        linreg_forecast = filter_dataframe_for_selected_stations(
            linreg_forecast, "code", iehhf_selected_stations)

    # print("DEBUG: read_linreg_forecast_data: linreg_forecast:\n", linreg_forecast.head())

    # We are only interested in the predictor & average discharge here. We drop the other columns.
    # linreg_forecast = linreg_forecast[['date', 'pentad_in_year', 'code', 'predictor', 'discharge_avg']]

    # For site 15059, we want to see the latest 10 rows of columns 'date', 'code', 'slope, intercept, forecasted_discharge', 'delta' only (no other columns)
    # print("\n\n\n\n\nread_linreg_forecast_data:\n", linreg_forecast[((linreg_forecast['code'] == '16059') & (linreg_forecast['pentad_in_year'] == 54))][['date', 'code', 'slope', 'intercept', 'forecasted_discharge', 'delta']].tail(10).to_string(index=False))
    # Store in cache
    pn.state.cache[cache_key] = (file_mtime, linreg_forecast)
    return linreg_forecast


def shift_date_by_n_days(linreg_predictor_orig, n=1):
    """
    Shift the date column of the linreg_predictor DataFrame by n days.

    Args:
        linreg_predictor (pd.DataFrame): The linreg_predictor DataFrame.
        n (int): The number of days to shift the date column.

    Returns:
        pd.DataFrame: The linreg_predictor DataFrame with the shifted date column.
    """
    # Make a copy of the input data frame in order not to change linreg_predictor
    linreg_predictor = linreg_predictor_orig.copy()
    linreg_predictor['date'] = linreg_predictor['date'] + pd.Timedelta(days=n)

    # If there is a column pentad_in_yer in the DataFrame, we need to update it as well
    if 'pentad_in_year' in linreg_predictor.columns:
        linreg_predictor['pentad_in_year'] = linreg_predictor['date'].apply(tl.get_pentad_in_year)
        # Cast the pentad_in_year column to int
        linreg_predictor['pentad_in_year'] = linreg_predictor['pentad_in_year'].astype(int)

    # If there are columns date, and possibly others, drop these
    if 'date' in linreg_predictor.columns:
        linreg_predictor = linreg_predictor.drop(columns=['date'])

    # Drop rows with NaN values in predictor or discharge_avg columns
    linreg_predictor = linreg_predictor.dropna(subset=['predictor', 'discharge_avg'])

    return linreg_predictor


def read_forecast_results_file(iehhf_selected_stations, file_mtime):
    cache_key_base = 'forecast_results_file'
    stations_tuple = tuple(iehhf_selected_stations) if iehhf_selected_stations is not None else None
    cache_key = (cache_key_base, stations_tuple)
    if cache_key in pn.state.cache:
        cached_mtime, forecast_pentad = pn.state.cache[cache_key]
        if cached_mtime == file_mtime:
            return forecast_pentad

    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        forecast_file = os.getenv("ieasyforecast_combined_forecast_pentad_file")
    else:
        forecast_file = os.getenv("ieasyforecast_combined_forecast_decad_file")
    forecast_results_file = os.path.join(os.getenv("ieasyforecast_intermediate_data_path"), forecast_file)
    forecast_results_file = forecast_results_file.replace('.csv', '_latest.csv')
    # Test if file exists and thorw an error if not
    if not os.path.isfile(forecast_results_file):
        raise Exception("File not found: " + forecast_results_file)
    # Read forecast results
    forecast_pentad = pd.read_csv(forecast_results_file)
    # Test if forecast_pentad is empty
    if forecast_pentad.empty:
        raise Exception("File is empty: " + forecast_results_file)
    # Convert the date column to datetime. The format of the date string is %Y-%m-%d.
    forecast_pentad['date'] = pd.to_datetime(forecast_pentad['date'], format='%Y-%m-%d')
    # Make sure the date column is in datetime64 format
    forecast_pentad['date'] = forecast_pentad['date'].astype('datetime64[ns]')
    # The forecast date is the date on which the forecast was produced. For
    # visualization we need to have the date for which the forecast is valid.
    # We add 1 day to the forecast Date to get the valid date.
    forecast_pentad['Date'] = forecast_pentad['date'] + pd.Timedelta(days=1)
    # Convert the code column to string
    forecast_pentad['code'] = forecast_pentad['code'].astype(str)
    # Check if there are duplicates for Date and code columns. If yes, only keep the
    # last one
    # Print all values where column code is 15102. Sort the values by Date in ascending order.
    # Print the last 20 values.
    forecast_pentad = forecast_pentad.drop_duplicates(subset=['Date', 'code', 'model_short'], keep='last').sort_values(
        'Date')
    # Get the pentad of the year. Refers to the pentad the forecast is produced for.
    if horizon == "pentad":
        forecast_pentad = tl.add_pentad_in_year_column(forecast_pentad)
    else:
        forecast_pentad = tl.add_decad_in_year_column(forecast_pentad)
        forecast_pentad["decad_in_year"] = forecast_pentad["decad_in_year"].astype(int)
    # Cast pentad column no number
    forecast_pentad[horizon] = forecast_pentad[horizon].astype(int)
    # Add a year column
    forecast_pentad['year'] = forecast_pentad['Date'].dt.year
    # Drop duplicates in Date, code and model_short columns
    forecast_pentad = forecast_pentad.drop_duplicates(subset=['Date', 'code', 'model_short'], keep='last')

    if iehhf_selected_stations is not None:
        # Filter the data frame for the selected stations
        forecast_pentad = filter_dataframe_for_selected_stations(
            forecast_pentad, "code", iehhf_selected_stations)

    # print(f"DEBUG: read_forecast_results_file: forecast_pentad:\n{forecast_pentad.tail()}")
    # print(f"DEBUG: read_forecast_results_file: unique models:\n{forecast_pentad['model_long'].unique()}")
    # Store in cache
    pn.state.cache[cache_key] = (file_mtime, forecast_pentad)
    return forecast_pentad


def read_forecast_stats_file(stations_iehhf, file_mtime):
    cache_key_base = 'forecast_stats_file'
    stations_tuple = tuple(stations_iehhf) if stations_iehhf is not None else None
    cache_key = (cache_key_base, stations_tuple)
    if cache_key in pn.state.cache:
        cached_mtime, forecast_stats = pn.state.cache[cache_key]
        if cached_mtime == file_mtime:
            return forecast_stats

    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        forecast_file = os.getenv("ieasyforecast_pentadal_skill_metrics_file")
    else:
        forecast_file = os.getenv("ieasyforecast_decadal_skill_metrics_file")
    forecast_stats_file = os.path.join(os.getenv("ieasyforecast_intermediate_data_path"), forecast_file)
    # Test if file exists and thorw an error if not
    if not os.path.isfile(forecast_stats_file):
        raise Exception("File not found: " + forecast_stats_file)

    # Read forecast results
    forecast_stats = pd.read_csv(forecast_stats_file)

    # Test if forecast_stats is empty
    if forecast_stats.empty:
        raise Exception("File is empty: " + forecast_stats_file)

    # Make sure code column is a string
    forecast_stats['code'] = forecast_stats['code'].astype(str)

    if stations_iehhf is not None:
        # Filter the data frame for the selected stations
        forecast_stats = filter_dataframe_for_selected_stations(
            forecast_stats, "code", stations_iehhf)

    # print("DEBUG: read_forecast_stats_file: forecast_stats:\n", forecast_stats.head())
    # Store in cache
    pn.state.cache[cache_key] = (file_mtime, forecast_stats)
    return forecast_stats


def add_predictor_dates(linreg_predictor, station, date):
    """
    Add start and end days for predictor period and forecast period to the
    linreg_predictor DataFrame.

    Args:
        linreg_predictor (pd.DataFrame): The linreg_predictor DataFrame.
        station (str): The station label.
        date (datetime.date): The date for which the predictor data is needed.

    Returns:
        pd.Series: The predictor data for the station.
    """

    # Get lates forecast date from linre_predictor
    linreg_predictor['date'] = pd.to_datetime(linreg_predictor['date'], format='%Y-%m-%d')
    latest_forecast_date = linreg_predictor['date'].max()

    # print(f"\n\nDEBUG: add_predictor_dates: station: {station}, date: {date}")
    # Filter the predictor data for the hydropost
    predictor = linreg_predictor[linreg_predictor['station_labels'] == station].copy()

    # Ensure predictor is not empty
    if predictor.empty:
        print(f"No predictor data available for station {station} up to date {date}.")
        return pd.DataFrame()

    # Identify the next lowest date in the predictor data
    predictor = predictor[predictor['date'] <= pd.Timestamp(date)]
    if predictor.empty:
        print(f"No predictor data available for station {station} before date {date}.")
        return pd.DataFrame()

    # Get the latest date in the predictor data
    predictor = predictor.sort_values('date').iloc[-1:]

    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        predictor_start_days = 2
        forecast_end_days = 5
        last_day = 26
    else:
        predictor_day = latest_forecast_date.day
        if predictor_day > 20:
            predictor_start_days = predictor_day % 20 - 1
        else:
            predictor_start_days = 9
        forecast_end_days = 10
        last_day = 21

    # Add start and end days for predictor period and forecast period
    predictor['predictor_start_date'] = latest_forecast_date - pd.DateOffset(days=predictor_start_days)
    predictor['forecast_start_date'] = latest_forecast_date + pd.DateOffset(days=1)
    predictor['forecast_end_date'] = latest_forecast_date + pd.DateOffset(days=forecast_end_days)

    # print(f"DEBUG: add_predictor_dates: predictor:\n{predictor}")

    # Round the predictor according to common rules
    predictor['predictor'] = fl.round_discharge_to_float(predictor['predictor'].values[0])

    # Test if the forecast_start_date is 26, set the forecast_end_date to the end of the month of the forecast_start_date
    if (predictor['forecast_start_date'].dt.day == last_day).any():
        predictor['forecast_end_date'] = predictor['forecast_start_date'] + pd.offsets.MonthEnd(0)

    # Add 23 hours and 58 minutes to the forecast_end_date
    predictor['forecast_end_date'] = predictor['forecast_end_date'] + pd.Timedelta(hours=23, minutes=58)

    # Define predictor end date to be 23 hours plus 58 minutes after for 'date'
    predictor['predictor_end_date'] = latest_forecast_date + pd.Timedelta(hours=23, minutes=58)

    # Add a day_of_year column to the predictor DataFrame
    predictor['day_of_year'] = predictor['date'].dt.dayofyear
    predictor['predictor_start_day_of_year'] = float(predictor['predictor_start_date'].dt.dayofyear.iloc[0]) - 0.2
    predictor['predictor_end_day_of_year'] = float(predictor['day_of_year'].iloc[0]) + 0.2
    predictor['forecast_start_day_of_year'] = predictor['forecast_start_date'].dt.dayofyear - 0.1
    predictor['forecast_end_day_of_year'] = predictor['forecast_end_date'].dt.dayofyear + 0.8

    # Test if 'date' is a leap year
    if (predictor['date'].dt.year % 4 != 0).any() or (predictor['date'].dt.year % 100 == 0).any() and (
            predictor['date'].dt.year % 400 != 0).any():
        predictor['leap_year'] = False
    else:
        predictor['leap_year'] = True

    # if isinstance(predictor, pd.Series):
    #    predictor = predictor.to_frame()

    print(f"\n\n")
    # Determine if the date is in a leap year
    # year = predictor['date'].dt.year.iloc[0]
    # predictor['leap_year'] = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
    return predictor


def add_predictor_dates_deprecating(linreg_predictor, station, date):
    """
    Add start and end days for predictor period and forecast period to the
    linreg_predictor DataFrame.

    Args:
        linreg_predictor (pd.DataFrame): The linreg_predictor DataFrame.
        station (str): The station label.
        date (datetime.date): The date for which the predictor data is needed.

    Returns:
        pd.Series: The predictor data for the station.
    """
    # print(f"\n\nDEBUG: add_predictor_dates: station: {station}, date: {date}")
    # Filter the predictor data for the hydropost
    predictor = linreg_predictor[linreg_predictor['station_labels'] == station].copy()

    # Cast date column to datetime
    predictor['date'] = pd.to_datetime(predictor['date'], format='%Y-%m-%d')

    # Ensure predictor is not empty
    if predictor.empty:
        print(f"No predictor data available for station {station} up to date {date}.")
        return pd.DataFrame()

    # Identify the next lowest date in the predictor data
    predictor = predictor[predictor['date'] <= pd.Timestamp(date)]
    if predictor.empty:
        print(f"No predictor data available for station {station} before date {date}.")
        return pd.DataFrame()

    # Get the latest date in the predictor data
    predictor = predictor.sort_values('date').iloc[-1:]

    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        predictor_start_days = 2
        forecast_end_days = 5
        last_day = 26
    else:
        predictor_day = predictor['date'].max().day
        if predictor_day > 20:
            predictor_start_days = predictor_day % 20 - 1
        else:
            predictor_start_days = 9
        forecast_end_days = 10
        last_day = 21

    # Add start and end days for predictor period and forecast period
    predictor['predictor_start_date'] = predictor['date'] - pd.DateOffset(days=predictor_start_days)
    predictor['forecast_start_date'] = predictor['date'] + pd.DateOffset(days=1)
    predictor['forecast_end_date'] = predictor['date'] + pd.DateOffset(days=forecast_end_days)

    # print(f"DEBUG: add_predictor_dates: predictor:\n{predictor}")

    # Round the predictor according to common rules
    predictor['predictor'] = fl.round_discharge_to_float(predictor['predictor'].values[0])

    # Test if the forecast_start_date is 26, set the forecast_end_date to the end of the month of the forecast_start_date
    if (predictor['forecast_start_date'].dt.day == last_day).any():
        predictor['forecast_end_date'] = predictor['forecast_start_date'] + pd.offsets.MonthEnd(0)

    # Add 23 hours and 58 minutes to the forecast_end_date
    predictor['forecast_end_date'] = predictor['forecast_end_date'] + pd.Timedelta(hours=23, minutes=58)

    # Define predictor end date to be 23 hours plus 58 minutes after for 'date'
    predictor['predictor_end_date'] = predictor['date'] + pd.Timedelta(hours=23, minutes=58)

    # Add a day_of_year column to the predictor DataFrame
    predictor['day_of_year'] = predictor['date'].dt.dayofyear
    predictor['predictor_start_day_of_year'] = float(predictor['predictor_start_date'].dt.dayofyear.iloc[0]) - 0.2
    predictor['predictor_end_day_of_year'] = float(predictor['day_of_year'].iloc[0]) + 0.2
    predictor['forecast_start_day_of_year'] = predictor['forecast_start_date'].dt.dayofyear - 0.1
    predictor['forecast_end_day_of_year'] = predictor['forecast_end_date'].dt.dayofyear + 0.8

    # Test if 'date' is a leap year
    if (predictor['date'].dt.year % 4 != 0).any() or (predictor['date'].dt.year % 100 == 0).any() and (
            predictor['date'].dt.year % 400 != 0).any():
        predictor['leap_year'] = False
    else:
        predictor['leap_year'] = True

    # if isinstance(predictor, pd.Series):
    #    predictor = predictor.to_frame()

    print(f"\n\n")
    # Determine if the date is in a leap year
    year = predictor['date'].dt.year.iloc[0]
    predictor['leap_year'] = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
    return predictor


def internationalize_forecast_model_names(_, forecasts_all,
                                          model_long_col='model_long',
                                          model_short_col='model_short'):
    """
    Replaces the strings in model_long and model_short columns with the
    corresponding translations.

    Args:
        forecasts_all (pd.DataFrame): The forecast results DataFrame.
        model_long_col (str): The column name for the long model names.
        model_short_col (str): The column name for the short model names.

    Returns:
        pd.DataFrame: The forecast results DataFrame with translated model names.
    """
    forecasts_all[model_long_col] = forecasts_all[model_long_col].apply(lambda x: _(x))
    forecasts_all[model_short_col] = forecasts_all[model_short_col].apply(lambda x: _(x))
    # print("Inernationalized forecast model names:\n", forecasts_all[model_long_col].unique())
    # print(forecasts_all[model_short_col].unique())

    return forecasts_all


def sapphire_sites_to_dataframe(sites_list):
    """
    Convert a list of SapphireSite objects to a pandas DataFrame.

    Args:
    sites_list (list): A list of SapphireSite objects.

    Returns:
    pd.DataFrame: A DataFrame where each row represents a SapphireSite object
                  and each column represents an attribute of the object.
    """
    cache_key = 'sapphire_sites_dataframe'
    sites_list_hash = hash(tuple([site.code for site in sites_list]))
    if cache_key in pn.state.cache:
        cached_hash, df = pn.state.cache[cache_key]
        if cached_hash == sites_list_hash:
            return df
    # Use a list comprehension to create a list of dictionaries
    # Each dictionary represents one SapphireSite object
    data = [vars(site) for site in sites_list]

    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(data)
    # Store in cache
    pn.state.cache[cache_key] = (sites_list_hash, df)
    return df


def read_all_stations_metadata_from_iehhf(station_list):
    cache_key = 'all_stations_metadata_iehhf'
    if cache_key in pn.state.cache:
        station_list_cached, all_stations, station_df, station_dict = pn.state.cache[cache_key]
        if station_list_cached == station_list:
            return station_list, all_stations, station_df, station_dict

    from ieasyhydro_sdk.sdk import IEasyHydroHFSDK
    iehhf = IEasyHydroHFSDK()
    # Get a list of site objects from iEH HF
    all_stations, _, _ = sl.get_pentadal_forecast_sites_from_HF_SDK(iehhf)
    # Cast all stations attributes to a dataframe
    all_stations = sapphire_sites_to_dataframe(all_stations)
    # Cast all_stations['code'] to string
    all_stations['code'] = all_stations['code'].astype(str)
    # print column names of all_stations
    # print(f"all_stations columns: {all_stations.columns}")
    # print(f"head of all_stations: {all_stations[['code', 'name', 'river_name', 'punkt_name']].head(20)}")

    # Rename
    all_stations.rename(columns={'name': 'station_labels'}, inplace=True)
    # print(f"all_stations columns: {all_stations.columns}")

    # Left-join
    station_df = pd.DataFrame(station_list, columns=['code'])
    station_df = station_df.merge(
        all_stations.loc[:, ['code', 'station_labels', 'basin']],
        left_on='code', right_on='code', how='left')
    station_df['station_labels'] = station_df['code'] + ' - ' + station_df['station_labels']

    # Update the station list with the new station labels
    station_list = station_df['station_labels'].tolist()

    # Create station dictionary
    station_dict = station_df.groupby('basin')['station_labels'].apply(list).to_dict()

    # Store in cache
    pn.state.cache[cache_key] = (station_list, all_stations, station_df, station_dict)
    return station_list, all_stations, station_df, station_dict


def read_all_stations_metadata_from_file(station_list):
    """It is used to connect to ieasyhydro"""
    cache_key = 'all_stations_metadata_file'
    if cache_key in pn.state.cache:
        station_list_cached, all_stations, station_df, station_dict = pn.state.cache[cache_key]
        if station_list_cached == station_list:
            return station_list, all_stations, station_df, station_dict

    all_stations_file = os.path.join(
        os.getenv("ieasyforecast_configuration_path"),
        os.getenv("ieasyforecast_config_file_all_stations")
    )
    # Test if file exists and thorw an error if not
    if not os.path.isfile(all_stations_file):
        raise Exception("File not found: " + all_stations_file)

    # Read stations json
    all_stations = fl.load_all_station_data_from_JSON(all_stations_file)
    # Convert the code column to string
    all_stations['code'] = all_stations['code'].astype(str)

    # Left-join all_stations['code', 'river_ru', 'punkt_ru'] by 'Code' = 'code'
    station_df = pd.DataFrame(station_list, columns=['code'])
    station_df = station_df.merge(
        all_stations.loc[:, ['code', 'river_ru', 'punkt_ru', 'basin']],
        left_on='code', right_on='code', how='left')

    # Create station labels
    station_df['station_labels'] = station_df['code'] + ' - ' + station_df['river_ru'] + ' ' + station_df['punkt_ru']

    # Update the station list with the new station labels
    station_list = station_df['station_labels'].tolist()

    # Create station dictionary
    station_dict = station_df.groupby('basin')['station_labels'].apply(list).to_dict()

    # Store in cache
    pn.state.cache[cache_key] = (station_list, all_stations, station_df, station_dict)
    return station_list, all_stations, station_df, station_dict


def add_labels_to_hydrograph(hydrograph, all_stations):
    hydrograph = hydrograph.merge(
        all_stations.loc[:, ['code', 'station_labels']],
        left_on='code', right_on='code', how='left').copy()
    hydrograph['station_labels'] = hydrograph['code'] + ' - ' + hydrograph['station_labels']

    return hydrograph


def add_labels_to_forecast_pentad_df(forecast_pentad, all_stations):
    forecast_pentad = forecast_pentad.merge(
        all_stations.loc[:, ['code', 'station_labels']],
        left_on='code', right_on='code', how='left').copy()
    forecast_pentad['station_labels'] = forecast_pentad['code'] + ' - ' + forecast_pentad['station_labels']

    return forecast_pentad


# --- Data processing --------------------------------------------------------
def calculate_forecast_range(_, forecast_table, range_type, range_slider):
    """
    Calculate the forecast range based on the selected range type and the
    range slider value.

    Args:
        _ (gettext.gettext): The gettext function.
        forecast_table (pd.DataFrame): The forecast table.
        range_type (str): The selected range type.
        range_slider (float): The selected range slider value.

    Returns:
        pd.DataFrame: The forecast table with the calculated forecast range.
    """

    # Check if range_type is a widget or a string
    if not isinstance(range_type, str):
        range_type = range_type.value

    if range_type == _('delta'):
        forecast_table['fc_lower'] = forecast_table['forecasted_discharge'] - forecast_table['delta']
        forecast_table['fc_upper'] = forecast_table['forecasted_discharge'] + forecast_table['delta']
    elif range_type == _("Manual range, select value below"):
        if hasattr(range_slider, 'value'):
            forecast_table['fc_lower'] = (1 - range_slider.value / 100.0) * forecast_table['forecasted_discharge']
            forecast_table['fc_upper'] = (1 + range_slider.value / 100.0) * forecast_table['forecasted_discharge']
        else:
            forecast_table['fc_lower'] = (1 - range_slider / 100.0) * forecast_table['forecasted_discharge']
            forecast_table['fc_upper'] = (1 + range_slider / 100.0) * forecast_table['forecasted_discharge']
    elif range_type == _("max[delta, %]"):
        if hasattr(range_slider, 'value'):
            forecast_table['fc_lower'] = np.maximum(forecast_table['forecasted_discharge'] - forecast_table['delta'],
                                                    (1 - range_slider.value / 100.0) * forecast_table[
                                                        'forecasted_discharge'])
            forecast_table['fc_upper'] = np.minimum(forecast_table['forecasted_discharge'] + forecast_table['delta'],
                                                    (1 + range_slider.value / 100.0) * forecast_table[
                                                        'forecasted_discharge'])
        else:
            forecast_table['fc_lower'] = np.minimum(forecast_table['forecasted_discharge'] - forecast_table['delta'],
                                                    (1 - range_slider / 100.0) * forecast_table['forecasted_discharge'])
            forecast_table['fc_upper'] = np.maximum(forecast_table['forecasted_discharge'] + forecast_table['delta'],
                                                    (1 + range_slider / 100.0) * forecast_table['forecasted_discharge'])
    else:
        forecast_table['fc_lower'] = forecast_table['forecasted_discharge'] - forecast_table['delta']
        forecast_table['fc_upper'] = forecast_table['forecasted_discharge'] + forecast_table['delta']

    return forecast_table


def update_model_dict_date(model_dict, forecasts_all, selected_station, selected_date):
    """
    Update the model_dict with the models we have results for for the selected station

    Note: This function may throw an error if processing of forecasts is not done yet.
    """
    #print(f"DEBUG: update_model_dict_date for date: {selected_date}")
    #print(f"Tail of forecasts_all:\n{forecasts_all.tail()}")
    #print(f"Type of forecasts_all['date']: {forecasts_all['date'].dtype}")
    #print(f"Type of selected_date: {type(selected_date)}")
    if hasattr(selected_station, 'value'):
        #print(f"selected_station has attribute value: {selected_station.value}")
        if hasattr(selected_date, 'value'):
            #print(f"selected_date has attribute value: {selected_date.value}")
            filtered_forecasts = forecasts_all[
                (forecasts_all['station_labels'] == selected_station.value) &
                (forecasts_all['date'] == pd.Timestamp(selected_date.value))
                ]
        else:
            #print(f"selected_date does not have attribute value: {selected_date}")
            filtered_forecasts = forecasts_all[
                (forecasts_all['station_labels'] == selected_station.value) &
                (forecasts_all['date'] == pd.Timestamp(selected_date))
                ]
    else:
        #print(f"selected_station does not have attribute value: {selected_station}")
        if hasattr(selected_date, 'value'):
            #print(f"selected_date has attribute value: {selected_date.value}")
            filtered_forecasts = forecasts_all[
                (forecasts_all['station_labels'] == selected_station) &
                (forecasts_all['date'] == pd.Timestamp(selected_date.value))
                ]
        else:
            #print(f"selected_date does not have attribute value: {selected_date}")
            filtered_forecasts = forecasts_all[
                (forecasts_all['station_labels'] == selected_station) &
                (forecasts_all['date'] == pd.Timestamp(selected_date))
                ]
    # Print filtered_forecasts after station & date filtering
    #print(f"--Filtered forecasts:\n{filtered_forecasts[['station_labels', 'date', 'model_long', 'model_short', 'forecasted_discharge']]}")

    # Remove duplicates in model_short
    filtered_forecasts = filtered_forecasts.drop_duplicates(subset=['model_short'], keep='last')
    model_dict = filtered_forecasts.set_index('model_long')['model_short'].to_dict()

    #print(f"\nDEBUG: update_model_dict:")
    #print(f"Filtered forecasts:\n{filtered_forecasts[['model_long', 'model_short', 'forecasted_discharge']]}")
    #print(f"Resulting model_dict: {model_dict}")

    return model_dict


def get_best_models_for_station_and_pentad(forecasts_all, selected_station, selected_pentad, selected_decad):
    """Returns a list of models with the best performance for the selected station and pentad"""
    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        horizon_in_year = "pentad_in_year"
        horizon_value = selected_pentad
    else:
        horizon_in_year = "decad_in_year"
        horizon_value = selected_decad
    print(f"\n  dbg: get_best_models_for_station_and_pentad: selected_station: {selected_station}")
    print(f"  dbg: get_best_models_for_station_and_pentad: selected_pentad: {horizon_value}")
    # Filter the forecast results for the selected station and pentad
    forecasts_local = forecasts_all[
        (forecasts_all['station_labels'] == selected_station) &
        (forecasts_all[horizon_in_year] == horizon_value)
        ].copy()
    # Drop duplicates (columns model_short) and only keep the last one
    forecasts_filtered = forecasts_local.drop_duplicates(subset=['model_short'], keep='last').copy()
    # Remove Nan forecasts before calculating the best model
    # If no forecasts are currently available, this will procude an empty DataFrame
    print(f"forecasts before dropping NaN forecasts:\n{forecasts_filtered[['model_short', 'forecasted_discharge']]}")
    print(f"column names: {forecasts_filtered.columns}")
    forecasts_filtered = forecasts_filtered.dropna(subset=['forecasted_discharge'])
    print(f"forecasts after dropping NaN forecasts:\n{forecasts_filtered[['model_short', 'forecasted_discharge']]}")

    forecasts_no_LR = forecasts_filtered[forecasts_filtered['model_short'] != 'LR']
    print(f"forecasts_no_LR:\n{forecasts_no_LR[['model_short', 'forecasted_discharge']]}")
    # Test if forecasts_no_LR is empty
    # Check if forecasts_no_LR is empty or contains only NaN accuracy
    if forecasts_no_LR.empty or forecasts_no_LR['accuracy'].isna().all():
        print(f"No valid models found for the given station and {horizon}.")
        return ['Linear regression (LR)']  # Return a fallback
    best_model = forecasts_no_LR.loc[forecasts_no_LR['accuracy'].idxmax(), 'model_short']
    print("best_model: ", best_model)
    best_models = [best_model, 'LR']
    # If length of best_models is 1, we only have the LR model
    if len(best_models) == 1:
        print(f"Only LR model available for the given station and {horizon}.")
        return ['Linear regression (LR)']
    # if forecasted_discharge of forecasts is NaN, we remove the model from best_models
    best_models = [
        model for model in best_models if not forecasts_filtered[
            forecasts_filtered['model_short'] == model]['forecasted_discharge'].isna().all()
    ]
    # Get long model name for short model names in best_models list into a list.
    # This should avoid issues with EM long names.
    best_models = [
        forecasts_filtered[forecasts_filtered['model_short'] == model]['model_long'].iloc[0]
        for model in best_models
    ]
    print(f"Best models for station {selected_station}: {best_models}")
    return best_models


def read_rainfall_data(file_mtime):
    cache_key = 'rainfall_data'
    if cache_key in pn.state.cache:
        cached_mtime, forcing = pn.state.cache[cache_key]
        if cached_mtime == file_mtime:
            return forcing

    # Get path to forcing files from environment variables
    # Reanalysis forcing
    filepath_hind = os.path.join(
        os.getenv('ieasyhydroforecast_PATH_TO_HIND'),
        os.getenv('ieasyhydroforecast_FILE_CF_HIND_P')
    )
    # Replace .csv with _dashboard.csv
    filepath_hind = filepath_hind.replace('.csv', '_dashboard.csv')
    if not os.path.isfile(filepath_hind):
        raise Exception("File not found: " + filepath_hind)
    # Read hindcast forcing data
    forcing = pd.read_csv(filepath_hind)

    # Convert the date column to datetime
    forcing['date'] = pd.to_datetime(forcing['date'], format='%Y-%m-%d', errors='coerce').dt.date

    # Convert the code column to string
    forcing['code'] = forcing['code'].astype(str)

    # Sort by code and date
    forcing = forcing.sort_values(by=['code', 'date'])

    # Store in cache
    pn.state.cache[cache_key] = (file_mtime, forcing)
    return forcing


def read_temperature_data(file_mtime):
    cache_key = 'temperature_data'
    if cache_key in pn.state.cache:
        cached_mtime, forcing = pn.state.cache[cache_key]
        if cached_mtime == file_mtime:
            return forcing

    # Get path to forcing files from environment variables
    # Reanalysis forcing
    filepath_hind = os.path.join(
        os.getenv('ieasyhydroforecast_PATH_TO_HIND'),
        os.getenv('ieasyhydroforecast_FILE_CF_HIND_T')
    )
    # Replace .csv with _dashboard.csv
    filepath_hind = filepath_hind.replace('.csv', '_dashboard.csv')
    if not os.path.isfile(filepath_hind):
        raise Exception("File not found: " + filepath_hind)
    # Read hindcast forcing data
    forcing = pd.read_csv(filepath_hind)

    # Convert the date column to datetime
    forcing['date'] = pd.to_datetime(forcing['date'], format='%Y-%m-%d', errors='coerce')

    # Convert the code column to string
    forcing['code'] = forcing['code'].astype(str)

    # Sort by code and date
    forcing = forcing.sort_values(by=['code', 'date'])

    # Store in cache
    pn.state.cache[cache_key] = (file_mtime, forcing)
    return forcing


# --- Bulletin preparation --------------------------------------------------------
def get_bulletin_header_info(date, sapphire_forecast_horizon):
    """Get information from date relevant for the bulletin header."""
    if sapphire_forecast_horizon == 'pentad':
        df = pd.DataFrame({
            "pentad": [tl.get_pentad(date)],
            "month_number": [tl.get_month_num(date)],
            "month_str_nom_ru": [tl.get_month_str_case1(date)],
            "month_str_gen_ru": [tl.get_month_str_case2(date)],
            "year": [tl.get_year(date)],
            "day_start_pentad": [tl.get_pentad_first_day(date)],
            "day_end_pentad": [tl.get_pentad_last_day(date)],
        })
    elif sapphire_forecast_horizon == 'decad':
        df = pd.DataFrame({
            'decad': [tl.get_decad_in_month(date)],
            'month_number': [tl.get_month_num(date)],
            'month_str_nom_ru': [tl.get_month_str_case1(date)],
            'month_str_gen_ru': [tl.get_month_str_case2(date)],
            'year': [tl.get_year(date)],
            'day_start_decad': [tl.get_decad_first_day(date)],
            'day_end_decad': [tl.get_decad_last_day(date)],
        })

    return df
