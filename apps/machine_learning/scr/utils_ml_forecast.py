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

#custom imports
#Custom Libraries
from scr import predictor_TFT as predictor_class

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
    codes_static = set(static_features['CODE'].unique())

    # Find intersection of all three sets
    common_codes = codes_rivers & codes_era5 & codes_static

    # Convert the set back to a list
    return list(common_codes)

#--------------------------------------------------------------------------------
# MISSING DATA IMPUTATION
#--------------------------------------------------------------------------------
def recursive_imputation(df_discharge_org: pd.DataFrame, df_swe: pd.DataFrame, df_era5: pd.DataFrame, n: int, model_predictor: predictor_class.PREDICTOR, make_plot: bool = False) -> pd.DataFrame:
    """
    Recursive imputation of missing values in the discharge column of the dataframe, n steps ahead
    This method can provide forecasts with n = 2x forecast horizon missing dates at the end. Altough it is questionable if the model can provide accurate forecasts for such a long period in a recursive manner.
    Args:
    df_discharge_org: pd.DataFrame, dataframe with discharge data and dates (contains n missing values at the end)
    df_swe: pd.DataFrame, dataframe with SWE data and dates
    df_era5: pd.DataFrame, dataframe with ERA5 data and dates
    n: int, number of missing values to predict -> cannot be greater than 2x forecast horizon
    model_predictor: PREDICTOR, model to predict the missing values
    make_plot: bool, if True a plot of the predictions will be shown

    Returns:
    df_discharge_nan: pd.DataFrame, dataframe with imputed missing values
    """
    df_discharge_nan = df_discharge_org.copy()
    if df_swe is not None:
        df_swe = df_swe.copy()
    df_era5 = df_era5.copy()
    forecast_horizon = model_predictor.get_max_forecast_horizon()
    if n > forecast_horizon:
        possible_n = forecast_horizon
        df_discharge_intermittent = df_discharge_nan.iloc[:-(n - possible_n)]
        n = n - possible_n
        df_discharge_partially = recursive_imputation(df_discharge_intermittent, df_swe, df_era5, possible_n, model_predictor, make_plot=make_plot)

        df_discharge_nan.loc[df_discharge_nan.index[:len(df_discharge_partially)], 'discharge'] = df_discharge_partially['discharge'].values


    df_discharge = df_discharge_nan.iloc[:-n]
    code = df_discharge['code'].values[0]

    #now we can predict the missing values
    predictions = model_predictor.predict(df_discharge, df_era5, df_swe, code=code, n=n, make_plot=make_plot)

    #replace the missing discharge data with the predictions['discharge_0.5'] data
    df_discharge_nan.loc[df_discharge_nan.index[-n:], 'discharge'] = predictions['discharge_0.5'].values


    return df_discharge_nan

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