import os
import sys
import pandas as pd
import numpy as np
import param
import re

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
    #if in_docker_flag == "True":
    #    icon_path = os.path.join("apps", "forecast_dashboard", "www", "Pentad.png")
    #else:
    icon_path = os.path.join("www", "Pentad.png")

    # Test if file exists and thorw an error if not
    if not os.path.isfile(icon_path):
        raise Exception("File not found: " + icon_path)

    return icon_path

def get_station_codes_selected_for_pentadal_forecasts():
    """Read the station codes selected for pentadal forecasting from iEasyHydro High Frequency."""
    cache_key = 'station_codes_selected_for_pentadal_forecasts'
    if cache_key in pn.state.cache:
        return pn.state.cache[cache_key]

    from ieasyhydro_sdk.sdk import IEasyHydroHFSDK
    iehhf = IEasyHydroHFSDK()
    discharge_sites = iehhf.get_discharge_sites()
    # Convert the sites object to a DataFrame
    df = pd.DataFrame(discharge_sites)
    # Create a list of Site objects from the DataFrame
    sites = []
    for index, row in df.iterrows():
        row = pd.DataFrame(row).T

        # Test if the site has pentadal forecasts enabled and skip if not
        if row['enabled_forecasts'].values[0]['pentad_forecast'] == False:
            #print(f'Skipping site {row["site_code"].values[0]} as pentadal forecasts are not enabled.')
            #print(f'enabled_forecasts: {row["enabled_forecasts"].values[0]}')
            continue
        else:
            code = row['site_code'].values[0]
            sites.append(code)

    virtual_sites = iehhf.get_virtual_sites()
    # Convert the virtual_sites object to a DataFrame
    df = pd.DataFrame(virtual_sites)
    for index, row in df.iterrows():
        row = pd.DataFrame(row).T

        # Test if the site has pentadal forecasts enabled and skip if not
        if row['enabled_forecasts'].values[0]['pentad_forecast'] == False:
            #print(f'Skipping site {row["site_code"].values[0]} as pentadal forecasts are not enabled.')
            #print(f'enabled_forecasts: {row["enabled_forecasts"].values[0]}')
            continue
        else:
            code = row['site_code'].values[0]
            sites.append(code)

    # Cache the result
    pn.state.cache[cache_key] = sites
    return sites


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
            if file.endswith('.csv'):
                filename = os.path.join(root, file)
                temp_data = pd.read_csv(filename)
                # Add a column code to the data frame
                temp_data['code'] = code[0]
                # Add a column model_short to the data frame
                temp_data['model_short'] = 'RRAM'
                rram_forecast = pd.concat([rram_forecast, temp_data], ignore_index=True)
    # Cast forecast_date and date columns to datetime
    rram_forecast['forecast_date'] = rram_forecast['forecast_date'].apply(parse_dates)
    rram_forecast['date'] = rram_forecast['date'].apply(parse_dates)

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
    daily_data = pd.read_csv(filepath, parse_dates=["date", "forecast_date"])

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

    return daily_data

def read_machine_learning_forecasts_pentad(model, file_mtime):
    '''
    Reads forecast results from the machine learning model for the pentadal
    forecast horizon.

    Args:
    model (str): The machine learning model to read the forecast results from.
        Allowed values are 'TFT', 'TIDE', 'TSMIXER', and 'ARIMA'.
    '''
    cache_key = f'read_machine_learning_forecasts_pentad_{model}'
    if cache_key in pn.state.cache:
        cached_mtime, forecast = pn.state.cache[cache_key]
        if cached_mtime == file_mtime:
            return forecast

    if model == 'TFT':
        filename = f"pentad_{model}_forecast.csv"
        model_long = "Temporal-Fusion Transformer (TFT)"
        model_short = "TFT"
    elif model == 'TIDE':
        filename = f"pentad_{model}_forecast.csv"
        model_long = "Time-Series Dense Encoder (TiDE)"
        model_short = "TiDE"
    elif model == 'TSMIXER':
        filename = f"pentad_{model}_forecast.csv"
        model_long = "Time-Series Mixer (TSMixer)"
        model_short = "TSMixer"
    elif model == 'ARIMA':
        filename = f"pentad_{model}_forecast.csv"
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

    tide = read_machine_learning_forecasts_pentad('TIDE', file_mtime)
    tft = read_machine_learning_forecasts_pentad('TFT', file_mtime)
    tsmixer = read_machine_learning_forecasts_pentad('TSMIXER', file_mtime)
    # Calculate the Neural Ensemble (NE) forecast as the average of the TFT,
    # TIDE, and TSMIXER forecasts
    ne = pd.concat([tide, tft, tsmixer], ignore_index=True)
    ne = ne.drop(columns=['model_long', 'model_short']).groupby(['code', 'date', 'forecast_date']).mean().reset_index()
    ne['model_long'] = 'Neural Ensemble (NE)'
    ne['model_short'] = 'NE'
    arima = read_machine_learning_forecasts_pentad('ARIMA', file_mtime)
    # Concatenate the data frames
    ml_forecast = pd.concat([tide, tft, tsmixer, ne, arima], ignore_index=True)
    print(f"Head of ml_forecast:\n{ml_forecast.head()}")
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

    hydrograph_day_all = pd.read_csv(hydrograph_day_file, parse_dates=['date']).reset_index(drop=True)
    # Test if hydrograph_day_all is empty
    if hydrograph_day_all.empty:
        raise Exception("File is empty: " + hydrograph_day_file)

    hydrograph_day_all["day_of_year"] = hydrograph_day_all["day_of_year"].astype(int)
    hydrograph_day_all['code'] = hydrograph_day_all['code'].astype(str)
    # Sort all columns in ascending Code and pentad order
    hydrograph_day_all = hydrograph_day_all.sort_values(by=["code", "day_of_year"])

    # Store in cache
    pn.state.cache[cache_key] = (file_mtime, hydrograph_day_all)
    return hydrograph_day_all

def read_hydrograph_day_data_for_pentad_forecasting(iahhf_selected_stations, file_mtime):
    cache_key_base = 'hydrograph_day_data_pentad_forecasting'
    stations_tuple = tuple(iahhf_selected_stations) if iahhf_selected_stations is not None else None
    cache_key = (cache_key_base, stations_tuple)
    if cache_key in pn.state.cache:
        cached_mtime, hydrograph_day_all = pn.state.cache[cache_key]
        if cached_mtime == file_mtime:
            return hydrograph_day_all

    # Read hydrograph data with daily values
    hydrograph_day_all = read_hydrograph_day_file(file_mtime)

    # if we get data from iEasyHydro, we do the following
    if iahhf_selected_stations is not None:
        # Filter the data frame for the selected stations
        hydrograph_day_all = filter_dataframe_for_selected_stations(
            hydrograph_day_all, "code", iahhf_selected_stations)
    else:
        # Get station ids of stations selected for forecasting
        filepath = os.path.join(
            os.getenv("ieasyforecast_configuration_path"),
            os.getenv("ieasyforecast_config_file_station_selection"))
        selected_stations = fl.load_selected_stations_from_json(filepath)

        # Filter data for selected stations
        hydrograph_day_all = hydrograph_day_all[hydrograph_day_all["code"].isin(selected_stations)]

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
    cache_key = 'hydrograph_pentad_file'
    if cache_key in pn.state.cache:
        cached_mtime, hydrograph_pentad_all = pn.state.cache[cache_key]
        if cached_mtime == file_mtime:
            return hydrograph_pentad_all

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

    hydrograph_pentad_all["pentad_in_year"] = hydrograph_pentad_all["pentad_in_year"].astype(int)
    hydrograph_pentad_all['code'] = hydrograph_pentad_all['code'].astype(str)
    hydrograph_pentad_all['date'] = pd.to_datetime(hydrograph_pentad_all['date'], format='%Y-%m-%d')
    # Sort all columns in ascending Code and pentad order
    hydrograph_pentad_all = hydrograph_pentad_all.sort_values(by=["code", "pentad_in_year"])

    # Store in cache
    pn.state.cache[cache_key] = (file_mtime, hydrograph_pentad_all)
    return hydrograph_pentad_all

def read_hydrograph_pentad_data_for_pentad_forecasting(iahhf_selected_stations, file_mtime):
    cache_key_base = 'hydrograph_pentad_data_pentad_forecasting'
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

    forecast_results_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_analysis_pentad_file")
    )

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

    #print("DEBUG: read_linreg_forecast_data: linreg_forecast:\n", linreg_forecast.head())

    # We are only interested in the predictor & average discharge here. We drop the other columns.
    #linreg_forecast = linreg_forecast[['date', 'pentad_in_year', 'code', 'predictor', 'discharge_avg']]

    # For site 15059, we want to see the latest 10 rows of columns 'date', 'code', 'slope, intercept, forecasted_discharge', 'delta' only (no other columns)
    #print("\n\n\n\n\nread_linreg_forecast_data:\n", linreg_forecast[((linreg_forecast['code'] == '16059') & (linreg_forecast['pentad_in_year'] == 54))][['date', 'code', 'slope', 'intercept', 'forecasted_discharge', 'delta']].tail(10).to_string(index=False))
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

    forecast_results_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_combined_forecast_pentad_file")
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
    forecast_pentad = forecast_pentad.drop_duplicates(subset=['Date', 'code', 'model_short'], keep='last').sort_values('Date')
    # Get the pentad of the year. Refers to the pentad the forecast is produced for.
    forecast_pentad = tl.add_pentad_in_year_column(forecast_pentad)
    # Cast pentad column no number
    forecast_pentad['pentad'] = forecast_pentad['pentad'].astype(int)
    # Add a year column
    forecast_pentad['year'] = forecast_pentad['Date'].dt.year
    # Drop duplicates in Date, code and model_short columns
    forecast_pentad = forecast_pentad.drop_duplicates(subset=['Date', 'code', 'model_short'], keep='last')

    if iehhf_selected_stations is not None:
        # Filter the data frame for the selected stations
        forecast_pentad = filter_dataframe_for_selected_stations(
            forecast_pentad, "code", iehhf_selected_stations)

    #print(f"DEBUG: read_forecast_results_file: forecast_pentad:\n{forecast_pentad.tail()}")
    #print(f"DEBUG: read_forecast_results_file: unique models:\n{forecast_pentad['model_long'].unique()}")
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

    forecast_stats_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_pentadal_skill_metrics_file")
    )
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

    #print("DEBUG: read_forecast_stats_file: forecast_stats:\n", forecast_stats.head())
    # Store in cache
    pn.state.cache[cache_key] = (file_mtime, forecast_stats)
    return forecast_stats

# deprecated
def read_analysis_file():
    file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_analysis_pentad_file")
    )
    # Test if file exists and thorw an error if not
    if not os.path.isfile(file):
        raise Exception("File not found: " + file)
    # Read analysis results
    analysis_pentad = pd.read_csv(file)
    # Test if analysis_pentad is empty
    if analysis_pentad.empty:
        raise Exception("File is empty: " + file)
    # Convert the date column to datetime. The format of the date string is %Y-%m-%d.
    analysis_pentad['Date'] = pd.to_datetime(analysis_pentad['Date'], format='%Y-%m-%d')
    # Make sure the date column is in datetime64 format
    analysis_pentad['Date'] = analysis_pentad['Date'].astype('datetime64[ns]')
    # Cast Code column to string
    analysis_pentad['Code'] = analysis_pentad['Code'].astype(str)

    return analysis_pentad

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
    #print(f"\n\nDEBUG: add_predictor_dates: station: {station}, date: {date}")
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

    # Add start and end days for predictor period and forecast period
    predictor['predictor_start_date'] = predictor['date'] - pd.DateOffset(days=2)
    predictor['forecast_start_date'] = predictor['date'] + pd.DateOffset(days=1)
    predictor['forecast_end_date'] = predictor['date'] + pd.DateOffset(days=5)

    #print(f"DEBUG: add_predictor_dates: predictor:\n{predictor}")

    # Round the predictor according to common rules
    predictor['predictor'] = fl.round_discharge_to_float(predictor['predictor'].values[0])

    # Test if the forecast_start_date is 26, set the forecast_end_date to the end of the month of the forecast_start_date
    if (predictor['forecast_start_date'].dt.day == 26).any():
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
    if (predictor['date'].dt.year % 4 != 0).any() or (predictor['date'].dt.year % 100 == 0).any() and (predictor['date'].dt.year % 400 != 0).any():
        predictor['leap_year'] = False
    else:
        predictor['leap_year'] = True

    #if isinstance(predictor, pd.Series):
    #    predictor = predictor.to_frame()

    print(f"\n\n")
    # Determine if the date is in a leap year
    year = predictor['date'].dt.year.iloc[0]
    predictor['leap_year'] = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
    return predictor

def get_month_name_for_number(_, month_number):
    # Make sure month_number is an integer
    month_number = int(month_number)
    # Dictionary with month names in different languages
    month_names = {
        1: _("January"),
        2: _("February"),
        3: _("March"),
        4: _("April"),
        5: _("May"),
        6: _("June"),
        7: _("July"),
        8: _("August"),
        9: _("September"),
        10: _("October"),
        11: _("November"),
        12: _("December")
    }
    return month_names[month_number]

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
    #print("Inernationalized forecast model names:\n", forecasts_all[model_long_col].unique())
    #print(forecasts_all[model_short_col].unique())

    return forecasts_all

def deprecating_read_hydrograph_day_file(today):

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

def deprecating_read_hydrograph_pentad_file():
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

def deprecating_read_forecast_results_file():
    forecast_results_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_pentad_results_file")
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

def deprecating_read_analysis_file():
    file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_analysis_pentad_file")
    )
    # Test if file exists and thorw an error if not
    if not os.path.isfile(file):
        raise Exception("File not found: " + file)
    # Read analysis results
    analysis_pentad = pd.read_csv(file)
    # Test if analysis_pentad is empty
    if analysis_pentad.empty:
        raise Exception("File is empty: " + file)
    # Convert the date column to datetime. The format of the date string is %Y-%m-%d.
    analysis_pentad['Date'] = pd.to_datetime(analysis_pentad['Date'], format='%Y-%m-%d')
    # Make sure the date column is in datetime64 format
    analysis_pentad['Date'] = analysis_pentad['Date'].astype('datetime64[ns]')
    # Cast Code column to string
    analysis_pentad['Code'] = analysis_pentad['Code'].astype(str)

    return analysis_pentad

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
    all_stations, _ = sl.get_pentadal_forecast_sites_from_HF_SDK(iehhf)
    # Cast all stations attributes to a dataframe
    all_stations = sapphire_sites_to_dataframe(all_stations)
    # Cast all_stations['code'] to string
    all_stations['code'] = all_stations['code'].astype(str)
    # print column names of all_stations
    print(f"all_stations columns: {all_stations.columns}")
    #print(f"head of all_stations: {all_stations[['code', 'name', 'river_name', 'punkt_name']].head(20)}")

    # Rename
    all_stations.rename(columns={'name': 'station_labels'}, inplace=True)
    #print(f"all_stations columns: {all_stations.columns}")

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
    if range_type == _('delta'):
        forecast_table['fc_lower'] = forecast_table['forecasted_discharge'] - forecast_table['delta']
        forecast_table['fc_upper'] = forecast_table['forecasted_discharge'] + forecast_table['delta']
    elif range_type == _("Manual range, select value below"):
        forecast_table['fc_lower'] = (1 - range_slider/100.0) * forecast_table['forecasted_discharge']
        forecast_table['fc_upper'] = (1 + range_slider/100.0) * forecast_table['forecasted_discharge']
    elif range_type == _("max[delta, %]"):
        forecast_table['fc_lower'] = np.minimum(forecast_table['forecasted_discharge'] - forecast_table['delta'],
                                                (1 - range_slider/100.0) * forecast_table['forecasted_discharge'])
        forecast_table['fc_upper'] = np.maximum(forecast_table['forecasted_discharge'] + forecast_table['delta'],
                                                (1 + range_slider/100.0) * forecast_table['forecasted_discharge'])
    else:
        forecast_table['fc_lower'] = forecast_table['forecasted_discharge'] - forecast_table['delta']
        forecast_table['fc_upper'] = forecast_table['forecasted_discharge'] + forecast_table['delta']

    return forecast_table

def update_model_dict(model_dict, forecasts_all, selected_station, selected_pentad):
    """
    Update the model_dict with the models we have results for for the selected station
    """
    filtered_forecasts = forecasts_all[
        (forecasts_all['station_labels'] == selected_station) &
        (forecasts_all['pentad_in_year'] == selected_pentad)
    ]
    # Remove duplicates in model_short
    filtered_forecasts = filtered_forecasts.drop_duplicates(subset=['model_short'], keep='last')
    model_dict = filtered_forecasts.set_index('model_long')['model_short'].to_dict()
    return model_dict

def get_best_models_for_station_and_pentad(forecasts_all, selected_station, selected_pentad):
    """Returns a list of models with the best performance for the selected station and pentad"""
    # Filter the forecast results for the selected station and pentad
    forecasts = forecasts_all[
        (forecasts_all['station_labels'] == selected_station) &
        (forecasts_all['pentad_in_year'] == selected_pentad)
    ]
    forecasts_no_LR = forecasts[forecasts['model_long'] != 'Linear regression (LR)']
    # Test if forecasts_no_LR is empty
    # Check if forecasts_no_LR is empty or contains only NaN accuracy
    if forecasts_no_LR.empty or forecasts_no_LR['accuracy'].isna().all():
        print("No valid models found for the given station and pentad.")
        return ['Linear regression (LR)']  # Return a fallback
    best_model = forecasts_no_LR.loc[forecasts_no_LR['accuracy'].idxmax(), 'model_long']
    best_models = [best_model, 'Linear regression (LR)']
    return best_models

def add_labels_to_hydrograph_pentad_all(hydrograph_pentad_all, all_stations):
    hydrograph_pentad_all = hydrograph_pentad_all.merge(
        all_stations.loc[:, ['code', 'river_ru', 'punkt_ru']],
        left_on='code', right_on='code', how='left')
    hydrograph_pentad_all['station_labels'] = hydrograph_pentad_all['code'] + ' - ' + hydrograph_pentad_all['river_ru'] + ' ' + hydrograph_pentad_all['punkt_ru']
    # Remove the columns river_ru and punkt_ru
    hydrograph_pentad_all = hydrograph_pentad_all.drop(columns=['river_ru', 'punkt_ru'])
    return hydrograph_pentad_all

def add_labels_to_analysis_pentad_df(analysis_pentad, all_stations):
    analysis_pentad = analysis_pentad.merge(
        all_stations.loc[:, ['code', 'river_ru', 'punkt_ru']],
        left_on='code', right_on='code', how='left')
    analysis_pentad['station_labels'] = analysis_pentad['code'] + ' - ' + analysis_pentad['river_ru'] + ' ' + analysis_pentad['punkt_ru']
    analysis_pentad = analysis_pentad.drop(columns=['river_ru', 'punkt_ru'])

    return analysis_pentad

def deprecating_preprocess_hydrograph_day_data(hydrograph_day, today):

    print("hydrograph_day:\n", hydrograph_day.head())
    print(hydrograph_day.tail())

    # Test that we only have data for one unique code in the data frame.
    # If we have more than one code, we raise an exception.
    if len(hydrograph_day["code"].unique()) > 1:
        raise ValueError("Data frame contains more than one unique code.")

    # Drop the Code column
    hydrograph_day = hydrograph_day.drop(columns=["code", "station_labels"])

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
    if len(hydrograph_pentad["code"].unique()) > 1:
        raise ValueError("Data frame contains more than one unique code.")

    # Drop the code column
    hydrograph_pentad = hydrograph_pentad.drop(columns=["code", "station_labels"])
    # Melt the DataFrame to simplify the column index
    hydrograph_pentad = hydrograph_pentad.melt(id_vars=["pentad_in_year", "year"], var_name="Variable", value_name="value")

    # Set index to pentad_in_year
    hydrograph_pentad = hydrograph_pentad.set_index("pentad_in_year")

    # Calculate norm and percentiles for each pentad_in_year over all years
    norm = hydrograph_pentad.groupby("pentad_in_year")["value"].mean().reset_index(drop=True)
    perc_05 = hydrograph_pentad.groupby("pentad_in_year")["value"].quantile(0.05).reset_index(drop=True)
    perc_25 = hydrograph_pentad.groupby("pentad_in_year")["value"].quantile(0.25).reset_index(drop=True)
    perc_75 = hydrograph_pentad.groupby("pentad_in_year")["value"].quantile(0.75).reset_index(drop=True)
    perc_95 = hydrograph_pentad.groupby("pentad_in_year")["value"].quantile(0.95).reset_index(drop=True)
    min_val = hydrograph_pentad.groupby("pentad_in_year")["value"].min().reset_index(drop=True)
    max_val = hydrograph_pentad.groupby("pentad_in_year")["value"].max().reset_index(drop=True)

    # Create a new DataFrame with the calculated values
    hydrograph_norm_perc = pd.DataFrame({
        "pentad_in_year": norm.index,
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

    # If current year data is not available, use the previous year data
    if current_year not in hydrograph_pentad["year"].values:
        current_year = hydrograph_pentad["year"].values[-1]
    current_year_data = hydrograph_pentad[hydrograph_pentad["year"] == current_year]["value"].values

    # Add the current year data to the DataFrame
    hydrograph_norm_perc["current_year"] = current_year_data
    hydrograph_norm_perc["pentad_in_year"] = hydrograph_norm_perc["pentad_in_year"] + 1

    return hydrograph_norm_perc

def select_analysis_data(analysis_pentad_all, station_widget):
    analysis_pentad = analysis_pentad_all[analysis_pentad_all["station_labels"] == station_widget]

    # Select columns
    analysis_pentad = analysis_pentad[
        ["year", "discharge_sum", "discharge_avg", "accuracy", "sdivsigma", "pentad_in_year"]
    ]

    # Rename the columns
    analysis_pentad = analysis_pentad.rename(
        columns={
            "discharge_sum": "Predictor",
            "discharge_avg": "Q [m3/s]",
            "accuracy": "Accuracy",
            "sdivsigma": "Efficiency",
            "pentad_in_year": "Pentad"
        }
    )

    # Make sure year is an integer
    analysis_pentad["year"] = analysis_pentad["year"].astype(int)

    # Reset and drop index
    analysis_pentad = analysis_pentad.reset_index(drop=True)

    # Set the year column as index
    analysis_pentad = analysis_pentad.set_index("year")

    return analysis_pentad

def calculate_norm_stats(analysis_data):
    # Calculate the mean and standard deviation of the predictor and discharge_avg
    norm_stats = pd.DataFrame({
        "Pentad": [analysis_data["Pentad"].mean()],
        "Min": [analysis_data["Q [m3/s]"].min()],
        "Norm": [analysis_data["Q [m3/s]"].mean()],
        "Max": [analysis_data["Q [m3/s]"].max()]
    })
    # Set Pentad as index
    norm_stats = norm_stats.set_index("Pentad")

    return norm_stats

def calculate_fc_stats(analysis_data):
    # Calculate the mean and standard deviation of the predictor and discharge_avg
    fc_stats = pd.DataFrame({
        "Pentad": [analysis_data["Pentad"].mean()],
        "Model": "Lin. reg.",
        "Predictor": [analysis_data["Predictor"].mean()],
        "Forecast": "TODO",
        "±δ": "TODO",
        "Lower": "TODO",
        "Upper": "TODO",
        "%P": [analysis_data["Accuracy"].mean()],
        "s/σ": [analysis_data["Efficiency"].mean()],
    })
    # Set Pentad as index
    fc_stats = fc_stats.set_index("Pentad")

    return fc_stats

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
    if not os.path.isfile(filepath_hind):
        raise Exception("File not found: " + filepath_hind)
    # Read hindcast forcing data
    hindcast_forcing = pd.read_csv(filepath_hind)

    # Convert the date column to datetime
    hindcast_forcing['date'] = pd.to_datetime(hindcast_forcing['date'], format='%Y-%m-%d', errors='coerce').dt.date

    # Control member forcing
    filepath_cf = os.path.join(
        os.getenv('ieasyhydroforecast_PATH_TO_CF'),
        os.getenv('ieasyhydroforecast_FILE_CF_P')
    )
    if not os.path.isfile(filepath_cf):
        raise Exception("File not found: " + filepath_cf)
    # Read forecast forcing data
    forecast_forcing = pd.read_csv(filepath_cf)

    # Convert the date column to datetime
    forecast_forcing['date'] = pd.to_datetime(forecast_forcing['date'], format='%Y-%m-%d', errors='coerce').dt.date

    # Merge the two dataframes
    forcing = pd.merge(hindcast_forcing, forecast_forcing, how='outer', on=['date', 'code'])
    # Fill missing values in P_x with values from P_y
    forcing['P'] = forcing['P_x'].combine_first(forcing['P_y'])
    # Drop columns P_x and P_y
    forcing = forcing.drop(columns=['P_x', 'P_y'])

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
    if not os.path.isfile(filepath_hind):
        raise Exception("File not found: " + filepath_hind)
    # Read hindcast forcing data
    hindcast_forcing = pd.read_csv(filepath_hind)

    # Convert the date column to datetime
    hindcast_forcing['date'] = pd.to_datetime(hindcast_forcing['date'], format='%Y-%m-%d', errors='coerce')

    # Control member forcing
    filepath_cf = os.path.join(
        os.getenv('ieasyhydroforecast_PATH_TO_CF'),
        os.getenv('ieasyhydroforecast_FILE_CF_T')
    )
    if not os.path.isfile(filepath_cf):
        raise Exception("File not found: " + filepath_cf)
    # Read forecast forcing data
    forecast_forcing = pd.read_csv(filepath_cf)

    # Convert the date column to datetime
    forecast_forcing['date'] = pd.to_datetime(forecast_forcing['date'], format='%Y-%m-%d', errors='coerce')

    # Merge the two dataframes
    forcing = pd.merge(hindcast_forcing, forecast_forcing, how='outer', on=['date', 'code'])
    # Fill missing values in T_x with values from T_y
    forcing['T'] = forcing['T_x'].combine_first(forcing['T_y'])
    # Drop columns T_x and T_y
    forcing = forcing.drop(columns=['T_x', 'T_y'])

    # Convert the code column to string
    forcing['code'] = forcing['code'].astype(str)

    # Sort by code and date
    forcing = forcing.sort_values(by=['code', 'date'])

    # Store in cache
    pn.state.cache[cache_key] = (file_mtime, forcing)
    return forcing





# --- Bulletin preparation --------------------------------------------------------
def get_bulletin_header_info(date):
    """Get information from date relevant for the bulletin header."""
    df = pd.DataFrame({
        "pentad": [tl.get_pentad(date)],
        "month_number": [tl.get_month_num(date)],
        "month_str_nom_ru": [tl.get_month_str_case1(date)],
        "month_str_gen_ru": [tl.get_month_str_case2(date)],
        "year": [tl.get_year(date)],
        "day_start_pentad": [tl.get_pentad_first_day(date)],
        "day_end_pentad": [tl.get_pentad_last_day(date)],
    })

    return df

