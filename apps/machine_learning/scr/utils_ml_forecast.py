# --------------------------------------------------------------------
# UTILS FOR ML FORECAST
# --------------------------------------------------------------------
#        _
#      _( )_
#    _(     )_      /\
#   (_________)    /  \/\            /\
#     \  \  \     /      \_____/\   /  \
#       \  \     /                \/    \
#         \  \                           \
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#       ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# --------------------------------------------------------------------
# DESCRIPTION:
# This script contains utility functions for the machine learning forecast
# --------------------------------------------------------------------


# --------------------------------------------------------------------
# Load Libraries
# --------------------------------------------------------------------
import os
import glob
import json
import pandas as pd
import numpy as np
import darts
from darts import TimeSeries, concatenate
from darts.utils.timeseries_generation import datetime_attribute_timeseries
import matplotlib.pyplot as plt
from pe_oudin.PE_Oudin import PE_Oudin
from suntime import Sun, SunTimeException

from darts.models import TFTModel, TiDEModel
from pytorch_lightning.callbacks import Callback
from pytorch_lightning.callbacks import EarlyStopping
import pytorch_lightning as pl
from pytorch_lightning import Trainer
import torch
import datetime
import logging
logging.getLogger("pytorch_lightning.utilities.rank_zero").setLevel(logging.WARNING)
logging.getLogger("pytorch_lightning.accelerators.cuda").setLevel(logging.WARNING)
logging.getLogger().setLevel(logging.WARNING)
import warnings
warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------
# SAPPHIRE API Client Imports
# --------------------------------------------------------------------
try:
    from sapphire_api_client import (
        SapphirePostprocessingClient,
        SapphirePreprocessingClient,
        SapphireAPIError
    )
    SAPPHIRE_API_AVAILABLE = True
except ImportError:
    SAPPHIRE_API_AVAILABLE = False
    SapphirePostprocessingClient = None
    SapphirePreprocessingClient = None
    SapphireAPIError = Exception

# Import forecast_library for API data reading
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast'))
import forecast_library as fl



# --------------------------------------------------------------------
# CALLBACKS
# --------------------------------------------------------------------
class LossLogger(Callback):
    # This class is needed for the models initialization
    def __init__(self):
        self.train_loss = []
        self.val_loss = []

    # will automatically be called at the end of each epoch
    def on_train_epoch_end(self, trainer: "pl.Trainer", pl_module: "pl.LightningModule") -> None:
        self.train_loss.append(float(trainer.callback_metrics["train_loss"]))

    def on_validation_epoch_end(self, trainer: "pl.Trainer", pl_module: "pl.LightningModule") -> None:
        self.val_loss.append(float(trainer.callback_metrics["val_loss"]))


# --------------------------------------------------------------------
# Read meteo data combined (Temperature and Precipitation)
# --------------------------------------------------------------------
def read_meteo_data_combined(
    site_codes: list = None,
    start_date: str = None,
    end_date: str = None,
    csv_path_t: str = None,
    csv_path_p: str = None,
) -> pd.DataFrame:
    """
    Read both temperature and precipitation meteo data and merge them.

    This helper function:
    - Reads both T (temperature) and P (precipitation) meteo data
    - Merges them into a single DataFrame
    - Handles API vs CSV fallback based on SAPPHIRE_API_ENABLED env var

    Parameters:
    -----------
    site_codes : list | None
        List of station codes to filter (used only for API source).
    start_date : str | None
        Start date filter (used only for API source).
    end_date : str | None
        End date filter (used only for API source).
    csv_path_t : str | None
        Path to temperature CSV file (used only for CSV fallback).
    csv_path_p : str | None
        Path to precipitation CSV file (used only for CSV fallback).

    Returns:
    --------
    pandas.DataFrame
        Merged meteo data with columns 'code', 'date', 'T' (temperature), 'P' (precipitation).

    Raises:
    -------
    SapphireAPIError
        If API is enabled but unavailable.
    FileNotFoundError
        If using CSV and files don't exist.
    """
    # Read temperature data
    t_data = fl.read_meteo_data(
        meteo_type='T',
        site_codes=site_codes,
        start_date=start_date,
        end_date=end_date,
        csv_path=csv_path_t,
    )
    # Rename 'value' column to 'T'
    t_data = t_data.rename(columns={'value': 'T'})

    # Read precipitation data
    p_data = fl.read_meteo_data(
        meteo_type='P',
        site_codes=site_codes,
        start_date=start_date,
        end_date=end_date,
        csv_path=csv_path_p,
    )
    # Rename 'value' column to 'P'
    p_data = p_data.rename(columns={'value': 'P'})

    # Merge temperature and precipitation data on code and date
    # Use inner join to only keep rows where both T and P have data
    merged_data = pd.merge(t_data, p_data, on=['code', 'date'], how='inner')

    # Sort by code and date
    merged_data = merged_data.sort_values(by=['code', 'date'])

    logger.info(f"Combined meteo data loaded: {len(merged_data)} records")
    logger.info(f"Date range: {merged_data['date'].min()} to {merged_data['date'].max()}")
    logger.info(f"Stations: {merged_data['code'].unique().tolist()}")

    return merged_data


# --------------------------------------------------------------------
# Identify hydroposts for forecast
# --------------------------------------------------------------------
def get_hydroposts_for_pentadal_and_decadal_forecasts():
    """
    Creates station lists for pentadal and decadal forecasts based on the
    hydroposts selected for forecasts by the user and the hydroposts available
    for ML predictions.

    Returns:
    rivers_to_predict_pentad: list of hydroposts selected for pentadal forecasts
    rivers_to_predict_decad: list of hydroposts selected for decadal forecasts
    hydroposts_available_for_ml_forecasting: DataFrame with hydroposts available for ML predictions with attributes from ieasyhydroforecast_config_hydroposts_available_for_ml_forecasts
    """
    # Create path to rivers available for ML predictions json file from environment variables
    rivers_to_predict_file = os.path.join(
        os.getenv('ieasyforecast_configuration_path'),
        os.getenv('ieasyhydroforecast_config_hydroposts_available_for_ml_forecasts')
    )
    # Test if file exists
    if not os.path.exists(rivers_to_predict_file):
        raise FileNotFoundError(f"File {rivers_to_predict_file} not found.")

    # Read hydroposts_available_for_ml_forecasting from json file and store them in a list
    with open(rivers_to_predict_file, "r") as json_file:
        config = json.load(json_file)
        # Normalize the nested JSON data
        stations_data = []
        for station_id, attributes in config['stationsID'].items():
            attributes['code'] = station_id
            stations_data.append(attributes)
        # Convert the list of dictionaries to a pandas DataFrame
        hydroposts_available_for_ml_forecasting = pd.DataFrame(stations_data)
        # Move the 'code' column to the first position
        hydroposts_available_for_ml_forecasting = hydroposts_available_for_ml_forecasting[
            ['code'] + [col for col in hydroposts_available_for_ml_forecasting.columns if col != 'code']
        ]
        logger.debug('hydroposts_available_for_ml_forecasting[code]: %s', hydroposts_available_for_ml_forecasting['code'])

    # Get gauges selected for pentadal forecasts
    rivers_selected_for_pentadal_forecasts_file = os.path.join(
        os.getenv('ieasyforecast_configuration_path'),
        os.getenv('ieasyforecast_config_file_station_selection')
    )
    # Read rivers_selected_for_pentadal_forecasts from json file and store them in a list
    with open(rivers_selected_for_pentadal_forecasts_file, "r") as json_file:
        config = json.load(json_file)
        rivers_selected_for_pentadal_forecasts = config["stationsID"]
        logger.debug('rivers_selected_for_pentadal_forecasts: %s', rivers_selected_for_pentadal_forecasts)

    # Now filter the rivers availabale for forecasting for the ones that are selected for forecasting and write them to a list
    rivers_to_predict_pentad = list(set(hydroposts_available_for_ml_forecasting['code']) & set(rivers_selected_for_pentadal_forecasts))
    logger.debug('rivers_to_predict_pentad: %s', rivers_to_predict_pentad)

    # Get gauges selected for decadal forecasts
    rivers_selected_for_decadal_forecasts_file = os.path.join(
        os.getenv('ieasyforecast_configuration_path'),
        os.getenv('ieasyforecast_config_file_station_selection_decad')
    )
    # Read rivers_selected_for_decadal_forecasts from json file and store them in a list
    with open(rivers_selected_for_decadal_forecasts_file, "r") as json_file:
        config = json.load(json_file)
        rivers_selected_for_decadal_forecasts = config["stationsID"]
        logger.debug('rivers_selected_for_decadal_forecasts: %s', rivers_selected_for_decadal_forecasts)

    # Now filter the rivers availabale for forecasting for the ones that are selected for forecasting and write them to a list
    rivers_to_predict_decad = list(set(hydroposts_available_for_ml_forecasting['code']) & set(rivers_selected_for_decadal_forecasts))
    logger.debug('rivers_to_predict_decad: %s', rivers_to_predict_decad)

    return rivers_to_predict_pentad, rivers_to_predict_decad, hydroposts_available_for_ml_forecasting


# --------------------------------------------------------------------
# PET OUDIN
# --------------------------------------------------------------------
def calculate_pet_oudin(df_era5: pd.DataFrame, lat: float) -> np.array:
    #calculate the PET using the PE Oudin method
    df_era5 = df_era5.copy()
    temp = df_era5['T']
    date = df_era5['date']
    lat_unit = 'deg'
    out_units = 'mm/day'
    pet_oudin = PE_Oudin.pe_oudin(temp, date, lat, lat_unit, out_units)

    return pet_oudin


# --------------------------------------------------------------------
# DAYLIGHT HOURS
# --------------------------------------------------------------------
def get_daylight_hours(sun: Sun,  date: pd.Timestamp)-> float:
    #get sunrise and sunset time
    sunrise = sun.get_sunrise_time(date)
    sunset = sun.get_sunset_time(date)
    #calculate the daylight hours
    daylight_hours = (sunset - sunrise).seconds / 3600

    return daylight_hours

def calculate_daylight_hours(lat: float, lon: float, df_era5: pd.DataFrame) -> pd.DataFrame:
    df_era5 = df_era5.copy()
    #create sun object
    sun = Sun(lat, lon)
    #apply the function to get the daylight hours
    daylight_hours = df_era5['date'].apply(lambda x: get_daylight_hours(sun, x))

    return daylight_hours

# --------------------------------------------------------------------
# HELPER FUNCTIONS
# --------------------------------------------------------------------
def get_codes_to_use(past_discharge: pd.DataFrame, era5: pd.DataFrame, static_features: pd.DataFrame) -> list:
    # Extract unique codes from each DataFrame column
    codes_rivers = set(past_discharge['code'].unique())
    codes_era5 = set(era5['code'].unique())
    codes_static = set(static_features['code'].unique())

    # Find intersection of all three sets
    common_codes = codes_rivers & codes_era5 & codes_static

    # Convert the set back to a list
    return list(common_codes)

 

#--------------------------------------------------------------------------------
# MISSING DATA IMPUTATION
#--------------------------------------------------------------------------------

def gaps_imputation(df_discharge_org: pd.DataFrame) -> pd.DataFrame:
    """
    Imputation of gaps in the discharge data with simple linear interpolation
    Args:
    df_discharge_org: pd.DataFrame, dataframe with discharge data and dates (contains small gaps)

    Returns:
    df_discharge_interp: pd.DataFrame, dataframe with interpolated missing values
    """

    df_discharge_interp = df_discharge_org.copy()
    #interpolate the missing values
    df_discharge_interp['discharge'] = df_discharge_interp['discharge'].interpolate(method='linear')

    return df_discharge_interp

def check_for_nans(df: pd.DataFrame, threshhold: int) -> tuple[dict[str:bool], int] :
    """
    Check if there are any missing values in the dataframe and determine what type of missing values are present
    Args:
    df: pd.DataFrame, dataframe to check
    threshhold: int, threshhold for the number of missing values to exceed to not produce a forecast
                threshold is set to 30% of the length of the input chunk length (but atleast set to 3)

    Returns:
    dict:
        'exceeds_threshold':    bool, True if the number of missing values exceed the threshold -> no forecast is produced
        'nans_in_between':      bool, True if there are missing values in between the dataframe -> linear interpolation is used
        'nans_at_end':          bool, True if there are missing values at the end of the dataframe -> recursive imputation is used

    int: number of missing values at the end of the dataframe
    """

    sum_of_nans = df['discharge'].isnull().sum()

    exceeds_threshold = sum_of_nans > threshhold

    nans_at_at_end = 0
    #iterate over the last values of the dataframe
    for i in range(1, threshhold+1):
        if np.isnan(df['discharge'].iloc[-i]):
            nans_at_at_end += 1
        else:
            break

    has_nan_at_the_end = nans_at_at_end > 0

    missing_values_in_between = sum_of_nans - nans_at_at_end

    has_nan_in_between = missing_values_in_between > 0

    return {'exceeds_threshold': exceeds_threshold, 'nans_in_between': has_nan_in_between, 'nans_at_end': has_nan_at_the_end}, nans_at_at_end


# --------------------------------------------------------------------
# WRITE OUTPUT TXT FILE
# --------------------------------------------------------------------
def write_output_txt(output_path: str, pentad_no_success: list[int], decadal_no_success:list[int], missing_values: dict, exceeds_threshhold:dict, nans_at_end: dict):
    """
    Write a summary of possible errors in the forecast to a txt file
    """

    # Test if output path exists, create it if not
    if not os.path.exists(output_path):
        os.makedirs(output_path)


    output_path = os.path.join(output_path, 'response_ml_forecast.txt')

    with open(output_path, 'w') as f:
        f.write('Pentad Forecast was not successful for following codes: ' + str(pentad_no_success) + '\n')
        f.write("-----------------------------" + '\n')
        f.write('Decadal Forecast was not successful for following codes: ' + str(decadal_no_success) + '\n')
        f.write("-----------------------------" + '\n')
        f.write('Input Contained Missing Values for following Codes: ' + str(missing_values) + '\n')
        f.write("-----------------------------" + '\n')
        f.write('Input Exceeded tolerated Number of Missing Values for following codes: ' + str(exceeds_threshhold) + '\n')
        f.write("-----------------------------" + '\n')
        f.write('Following Codes have Nan-Values at the end -> Forecast will not be as accurate: ' + str(nans_at_end) + '\n')
        f.write("-----------------------------" + '\n')

# --------------------------------------------------------------------
# SAVE PENTAD FORECAST
# --------------------------------------------------------------------
def save_pentad_forecast()-> bool:
    """
    This function returns a boolean value, showing if the 5 day forecast should be saved or not
    """
    days_to_save = [5, 10, 15, 20, 25]

    today = datetime.datetime.now()
    today = today.date()

    #check if today is the last day of the month
    tomorrow = today + datetime.timedelta(days=1)
    is_last_day_of_month = tomorrow.month != today.month

    #check if today is in the list of days to save
    is_day_to_save = today.day in days_to_save

    return is_last_day_of_month or is_day_to_save

def save_decadal_forecast()-> bool:
    """
    This function returns a boolean value, showing if the 10 day forecast should be saved or not
    """
    days_to_save = [10, 20]

    today = datetime.datetime.now()
    today = today.date()

    #check if today is the last day of the month
    tomorrow = today + datetime.timedelta(days=1)
    is_last_day_of_month = tomorrow.month != today.month

    #check if today is in the list of days to save
    is_day_to_save = today.day in days_to_save

    return is_last_day_of_month or is_day_to_save


# --------------------------------------------------------------------
# SAPPHIRE API INTEGRATION
# --------------------------------------------------------------------

def calculate_pentad_from_date(date) -> tuple:
    """
    Calculate pentad_in_month and pentad_in_year from a date.

    Args:
        date: datetime, pd.Timestamp, or string in 'YYYY-MM-DD' format

    Returns:
        tuple: (pentad_in_month, pentad_in_year) as integers
    """
    if isinstance(date, str):
        date = pd.to_datetime(date)
    elif isinstance(date, datetime.date) and not isinstance(date, datetime.datetime):
        date = pd.Timestamp(date)

    # Calculate pentad in month (1-6)
    pentad_in_month = min((date.day - 1) // 5 + 1, 6)

    # Calculate pentad in year (1-72)
    pentad_in_year = (date.month - 1) * 6 + pentad_in_month

    return pentad_in_month, pentad_in_year


def calculate_decad_from_date(date) -> tuple:
    """
    Calculate decad_in_month and decad_in_year from a date.

    Args:
        date: datetime, pd.Timestamp, or string in 'YYYY-MM-DD' format

    Returns:
        tuple: (decad_in_month, decad_in_year) as integers
    """
    if isinstance(date, str):
        date = pd.to_datetime(date)
    elif isinstance(date, datetime.date) and not isinstance(date, datetime.datetime):
        date = pd.Timestamp(date)

    # Calculate decad in month (1-3)
    decad_in_month = min((date.day - 1) // 10 + 1, 3)

    # Calculate decad in year (1-36)
    decad_in_year = (date.month - 1) * 3 + decad_in_month

    return decad_in_month, decad_in_year


def _write_ml_forecast_to_api(data: pd.DataFrame, horizon_type: str, model_type: str) -> bool:
    """
    Write ML forecasts to SAPPHIRE postprocessing API.

    Args:
        data: DataFrame with ML forecast data. Expected columns:
            - code: station code
            - date: target date (when forecast is for)
            - forecast_date: when the forecast was made
            - flag: quality flag (0=ok, 1=NaN, 2=error)
            - Q5, Q25, Q50, Q75, Q95: quantile predictions
        horizon_type: Either "pentad" or "decade"
        model_type: ML model name (TFT, TIDE, TSMIXER)

    Returns:
        bool: True if successful, False otherwise
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.warning("sapphire-api-client not installed, skipping ML forecast API write")
        return False

    # Check if API writing is enabled (default: enabled)
    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
    if not api_enabled:
        logger.info("SAPPHIRE API writing disabled via SAPPHIRE_API_ENABLED=false")
        return False

    # Get API URL from environment
    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    client = SapphirePostprocessingClient(base_url=api_url)

    # Health check - non-blocking, skip if API unavailable
    if not client.readiness_check():
        logger.warning(
            f"SAPPHIRE API at {api_url} is not ready, "
            "skipping ML forecast write"
        )
        return False

    # Map model type to API format
    model_type_map = {
        "TFT": "TFT",
        "TIDE": "TiDE",
        "TSMIXER": "TSMixer"
    }
    api_model_type = model_type_map.get(model_type.upper(), model_type)

    # Prepare records for API
    records = []
    for _, row in data.iterrows():
        # Get dates
        target_date = pd.to_datetime(row['date'])
        forecast_date = pd.to_datetime(row['forecast_date'])

        # Calculate horizon values from target date
        if horizon_type == "pentad":
            horizon_value, horizon_in_year = calculate_pentad_from_date(target_date)
        elif horizon_type == "decade":
            horizon_value, horizon_in_year = calculate_decad_from_date(target_date)
        else:
            raise ValueError(f"Invalid horizon_type: {horizon_type}. Must be 'pentad' or 'decade'.")

        # Map quantile columns (ML uses Q5, Q25, etc.; API uses q05, q25, etc.)
        record = {
            "horizon_type": horizon_type,
            "code": str(int(row['code'])),
            "model_type": api_model_type,
            "date": forecast_date.strftime('%Y-%m-%d'),
            "target": target_date.strftime('%Y-%m-%d'),
            "flag": int(row['flag']) if pd.notna(row.get('flag')) else None,
            "horizon_value": horizon_value,
            "horizon_in_year": horizon_in_year,
            "q05": float(row['Q5']) if pd.notna(row.get('Q5')) else None,
            "q25": float(row['Q25']) if pd.notna(row.get('Q25')) else None,
            "q50": float(row['Q50']) if pd.notna(row.get('Q50')) else None,
            "q75": float(row['Q75']) if pd.notna(row.get('Q75')) else None,
            "q95": float(row['Q95']) if pd.notna(row.get('Q95')) else None,
            "forecasted_discharge": float(row['Q50']) if pd.notna(row.get('Q50')) else None,
        }
        records.append(record)

    # Write to API
    if records:
        count = client.write_forecasts(records)
        logger.info(f"Successfully wrote {count} ML forecast records to SAPPHIRE API ({model_type}, {horizon_type})")
        print(f"SAPPHIRE API: Successfully wrote {count} ML forecast records ({model_type}, {horizon_type})")
        return True
    else:
        logger.info("No ML forecast records to write to API")
        return False


def _check_ml_forecast_consistency(csv_data: pd.DataFrame, horizon_type: str, model_type: str) -> bool:
    """
    Check consistency between ML forecast data in CSV and API.

    This function reads back from the API after a write and compares with CSV data
    to verify data integrity. Only enabled when SAPPHIRE_CONSISTENCY_CHECK=true.

    Args:
        csv_data: DataFrame with forecast data from CSV
        horizon_type: Either "pentad" or "decade"
        model_type: ML model name (TFT, TIDE, TSMIXER)

    Returns:
        bool: True if data is consistent, False otherwise
    """
    # Check if consistency checking is enabled
    consistency_check = os.getenv("SAPPHIRE_CONSISTENCY_CHECK", "false").lower() == "true"
    if not consistency_check:
        return True

    if not SAPPHIRE_API_AVAILABLE:
        logger.warning("sapphire-api-client not installed, skipping consistency check")
        return True

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    client = SapphirePostprocessingClient(base_url=api_url)

    # Map model type to API format
    model_type_map = {
        "TFT": "TFT",
        "TIDE": "TiDE",
        "TSMIXER": "TSMixer"
    }
    api_model_type = model_type_map.get(model_type.upper(), model_type)

    # Get the date range from CSV data
    csv_data = csv_data.copy()
    csv_data['forecast_date'] = pd.to_datetime(csv_data['forecast_date'])
    latest_date = csv_data['forecast_date'].max()

    # Read from API - only latest forecast date
    try:
        api_data = client.read_forecasts(
            horizon=horizon_type,
            model=api_model_type,
            start_date=latest_date.strftime('%Y-%m-%d'),
            end_date=latest_date.strftime('%Y-%m-%d')
        )
    except Exception as e:
        logger.warning(f"Failed to read from API for consistency check: {e}")
        return True  # Don't fail on read errors

    if api_data.empty:
        logger.warning(f"No API data found for consistency check ({model_type}, {horizon_type})")
        return True

    # Track consistency
    is_consistent = True

    # Filter CSV to latest forecast date for comparison
    csv_latest = csv_data[csv_data['forecast_date'] == latest_date].copy()

    # Check row counts
    if len(api_data) != len(csv_latest):
        logger.warning(
            f"ML forecast consistency check: Row count mismatch. "
            f"API has {len(api_data)} rows, CSV has {len(csv_latest)} rows"
        )
        is_consistent = False

    # Convert API data for comparison
    api_data['code'] = api_data['code'].astype(str)
    csv_latest['code'] = csv_latest['code'].astype(str)
    api_data['target'] = pd.to_datetime(api_data['target'])
    csv_latest['date'] = pd.to_datetime(csv_latest['date'])

    # Merge on code and target date
    merged = csv_latest.merge(
        api_data,
        left_on=['code', 'date'],
        right_on=['code', 'target'],
        how='outer',
        suffixes=('_csv', '_api'),
        indicator=True
    )

    # Check for rows only in CSV
    only_csv = merged[merged['_merge'] == 'left_only']
    if len(only_csv) > 0:
        logger.warning(
            f"ML forecast consistency check: {len(only_csv)} rows in CSV but not in API"
        )
        is_consistent = False

    # Check for rows only in API
    only_api = merged[merged['_merge'] == 'right_only']
    if len(only_api) > 0:
        logger.warning(
            f"ML forecast consistency check: {len(only_api)} rows in API but not in CSV"
        )
        is_consistent = False

    # Check value mismatches for matching rows
    both = merged[merged['_merge'] == 'both']
    if len(both) > 0:
        # Compare Q50 values (main forecast)
        mismatches = []
        for _, row in both.iterrows():
            csv_q50 = row.get('Q50')
            api_q50 = row.get('q50')
            if pd.notna(csv_q50) and pd.notna(api_q50):
                if abs(csv_q50 - api_q50) > 0.001:  # Allow small floating point differences
                    # Get the target date from either merged column
                    target_date = row.get('target') if pd.notna(row.get('target')) else row.get('date_csv')
                    mismatches.append({
                        'code': row['code'],
                        'target_date': target_date,
                        'csv_q50': csv_q50,
                        'api_q50': api_q50
                    })

        if mismatches:
            logger.warning(
                f"ML forecast consistency check: {len(mismatches)} value mismatches found"
            )
            for m in mismatches[:5]:  # Log first 5
                logger.warning(f"  Code {m['code']}, target {m['target_date']}: CSV={m['csv_q50']}, API={m['api_q50']}")
            is_consistent = False

    if is_consistent:
        logger.info(f"ML forecast consistency check passed ({model_type}, {horizon_type})")

    return is_consistent