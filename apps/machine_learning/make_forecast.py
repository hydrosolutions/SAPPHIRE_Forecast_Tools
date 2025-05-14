# --------------------------------------------------------------------
# FORECASTING WITH MACHINE LEARNING MODELS
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
# This script produces forecasts using machine learning models (TFT, TiDE , TSMixer )(and ARIMA -> yet to come)
# --------------------------------------------------------------------
# INPUTS:
# - Data:
#       Autoregressive Discharge Time Series
#       The Quantile Mapped ERA5 data
#       Static Basin Features
#       The Normalization Parameters
# - Model:
#       The trained model for TFT and TiDE
# --------------------------------------------------------------------
# OUTPUTS:
# - Forecasts:
#       The forecasts for today and the next 5 or 10 days with a resolution of 1 day (csv)
# --------------------------------------------------------------------
# Missing Values:
# - If there are missing values in the input of discharge data, there are 3 possible outcomes:
#       1. The number of missing values exceeds the threshold for the model ( This value is set in the .env file), then the model will not be able to make a forecast
#       2. There are missing values in the middle of the input data, these will be imputed using a simple linear interpolation
#       3. There are missing values at the end of the input data, these will be imputed using a recursive imputation, the model will be used to forecast the missing values
#          and the forecasted values will be used as input for the next forecast, There is also a threshold for the number of missing values at the end of the input data
#
# --------------------------------------------------------------------
# TODO:
# - Select only the codes which have the flag true for the model.
# --------------------------------------------------------------------

# Useage:
# ieasyhydroforecast_env_file_path=/path/to/.env SAPPHIRE_MODEL_TO_USE=TFT SAPPHIRE_PREDICTION_MODE=PENTAD python make_forecast.py
# Possible values for MODEL_TO_USE: TFT, TIDE, TSMIXER
# Possible values for MODEL_TO_USE: PENTAD, DECAD


# --------------------------------------------------------------------
# Load Libraries
# --------------------------------------------------------------------
import os
import sys
import glob
import pandas as pd
import numpy as np
import json
from typing import List, Dict, Any

import darts
from darts import TimeSeries, concatenate
from darts.utils.timeseries_generation import datetime_attribute_timeseries
import matplotlib.pyplot as plt
#from pe_oudin.PE_Oudin import PE_Oudin
#from suntime import Sun, SunTimeException

from darts.models import TFTModel, TiDEModel, TSMixerModel
from pytorch_lightning.callbacks import Callback
from pytorch_lightning.callbacks import EarlyStopping
import pytorch_lightning as pl
from pytorch_lightning import Trainer
import torch
import datetime

from darts.utils.likelihood_models import QuantileRegression
from darts.utils.likelihood_models.base import LikelihoodType
from torch.optim import Adam
from torch.optim.lr_scheduler import ReduceLROnPlateau
from torch.nn.modules.loss import MSELoss
from torchmetrics.collections import MetricCollection


torch.serialization.add_safe_globals([QuantileRegression, 
                                    LikelihoodType,
                                    Adam,
                                    ReduceLROnPlateau,
                                    MSELoss,
                                    MetricCollection])

import logging
from logging.handlers import TimedRotatingFileHandler
logging.getLogger("pytorch_lightning.utilities.rank_zero").setLevel(logging.WARNING)
logging.getLogger("pytorch_lightning.accelerators.cuda").setLevel(logging.WARNING)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
# Ensure the logs directory exists
logs_dir = 'logs'
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)
file_handler = TimedRotatingFileHandler('logs/log', when='midnight',
                                        interval=1, backupCount=30)
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger = logging.getLogger('make_ml_forecast')
logger.setLevel(logging.DEBUG)
logger.handlers = []
logger.addHandler(file_handler)
#logger.addHandler(console_handler)

import warnings
warnings.filterwarnings("ignore")

# Print logging level of the logger
logger.info('Logging level: %s', logger.getEffectiveLevel())
# Level 10: DEBUG, Level 20: INFO, Level 30: WARNING, Level 40: ERROR, Level 50: CRITICAL
logger.debug('Debug message for logger level 10')

#Custom Libraries
from scr import utils_ml_forecast
from scr import TFTPredictor, TSMixerPredictor, TiDEPredictor, predictor_ARIMA

# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the setup_library module from the iEasyHydroForecast package
import setup_library as sl

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



def write_pentad_forecast(OUTPUT_PATH_DISCHARGE, MODEL_TO_USE, forecast_pentad):
    """
    Save the pentad forecast data. If a forecast interval needs to be saved,
    it appends the new forecast to the existing interval forecast file.
    The function avoids overwriting by appending data and removing duplicates.

    Parameters:
    OUTPUT_PATH_DISCHARGE (str): Path to the output directory where forecast files are saved.
    MODEL_TO_USE (str): The name of the model used for the forecast.
    forecast_pentad (pd.DataFrame): The new forecast data to be saved.
    utils_ml_forecast (module): The module containing the save_pentad_forecast function or flag.

    Returns:
    None
    """

    # Read the latest forecast and append the new forecast
    forecast_file_path = os.path.join(OUTPUT_PATH_DISCHARGE, f'pentad_{MODEL_TO_USE}_forecast.csv')
    try:
        forecast_pentad_old = pd.read_csv(forecast_file_path)
    except FileNotFoundError:
        forecast_pentad_old = pd.DataFrame()

    # Append the new forecast to the old forecast and remove duplicates
    forecast_pentad = pd.concat([forecast_pentad_old, forecast_pentad], axis=0)
    # date to datetime
    forecast_pentad['date'] = pd.to_datetime(forecast_pentad['date'])
    forecast_pentad['forecast_date'] = pd.to_datetime(forecast_pentad['forecast_date'])
    forecast_pentad = forecast_pentad.drop_duplicates(subset=['forecast_date','date', 'code'], keep='last')
    # Save the updated forecast
    forecast_pentad.to_csv(forecast_file_path, index=False)


def write_decad_forecast(OUTPUT_PATH_DISCHARGE, MODEL_TO_USE, forecast_decad):
    """
    Save the decad forecast data. The function saves the forecast data to a new file.
    if there is already a forecast file, the new forecast will be appended to the existing file

    Parameters:
    OUTPUT_PATH_DISCHARGE (str): Path to the output directory where forecast files are saved.
    MODEL_TO_USE (str): The name of the model used for the forecast.
    forecast_decad (pd.DataFrame): The new forecast data to be saved.

    Returns:
    None
    """
    try:
        forecast_decad_old = pd.read_csv(os.path.join(OUTPUT_PATH_DISCHARGE, f'decad_{MODEL_TO_USE}_forecast.csv'))
    except FileNotFoundError:
        forecast_decad_old = pd.DataFrame()

    forecast_decad = pd.concat([forecast_decad_old, forecast_decad], axis=0)

    #date to datetime
    forecast_decad['date'] = pd.to_datetime(forecast_decad['date'])
    forecast_decad['forecast_date'] = pd.to_datetime(forecast_decad['forecast_date'])

    forecast_decad = forecast_decad.drop_duplicates(subset=['forecast_date','date', 'code'], keep='last')
    forecast_decad.to_csv(os.path.join(OUTPUT_PATH_DISCHARGE, f'decad_{MODEL_TO_USE}_forecast.csv'), index=False)



def prepare_forecast_data(
        past_discharge: pd.DataFrame,
        threshold_missing_days: int,
        threshold_missing_days_end: int,
        old_forecast: pd.DataFrame,
        code: int,
        forecast_horizon: int,
        input_chunk_length: int,
) -> (pd.DataFrame, int):
    """
    Workflow for data preparation for forecasting.
    1. Make time series continuous by reindexing -> missing days will be nan
    2. Check the nan values in the time series
    3. Check the conditions: if we exceed the threshold of missing days, or missing_days_end, we will not make a forecast
        -> this will return a dataframe with nans in it and the flag 1
    4. We take the old forecast file and replace the missing days with the latest forecasted values
    5. if we still have missing days we interpolate or if it is at the end we take the last value
    6. We return the prepared data and the flag 0
    """
    discharge_df = past_discharge.copy()

    try:
        prev_forecast = old_forecast[old_forecast['code'] == code].copy()
        prev_forecast['date'] = pd.to_datetime(prev_forecast['date'])
        prev_forecast['forecast_date'] = pd.to_datetime(prev_forecast['forecast_date'])
    except:
        prev_forecast = None

    #1: Make time series continouus.
    today = pd.to_datetime(datetime.datetime.now().date())
    lockback_start = today - pd.Timedelta(days=input_chunk_length+1)
    discharge_df = discharge_df[(discharge_df['date'] >= lockback_start) & (discharge_df['date'] <= today)]
    date_range = pd.date_range(start=lockback_start, end=today, freq='D')
    discharge_df.set_index('date', inplace=True)
    discharge_df = discharge_df.reindex(date_range)
    discharge_df.reset_index(inplace=True)
    discharge_df.rename(columns={'index': 'date'}, inplace=True)

    #2: Check for missing values
    missing_values, nans_at_end = utils_ml_forecast.check_for_nans(discharge_df.iloc[-input_chunk_length:], threshold_missing_days)

    #3: Check the conditions
    if missing_values['exceeds_threshold'] or nans_at_end >= threshold_missing_days_end:
        return discharge_df, 1

    #4: Replace missing values with the latest forecasted values (Q50)
    if prev_forecast is not None:
        days_with_nan = discharge_df[discharge_df['discharge'].isna()]['date']
        prev_forecast = prev_forecast[prev_forecast['date'].isin(days_with_nan)]
        #sort by forecast_date
        prev_forecast = prev_forecast.sort_values(by='forecast_date')
        prev_forecast = prev_forecast.drop_duplicates(subset=['date'], keep='last')

        col_name = 'Q50'
        #check if the column exists
        if col_name not in prev_forecast.columns:
            col_name = 'Q'
        try:
            # First method: Update all dates at once
            discharge_df.loc[discharge_df['date'].isin(prev_forecast['date']), 'discharge'] = prev_forecast[col_name].values
            logger.debug(f'Nans replaced with forecasted values 1st method: {len(prev_forecast)}')
        except Exception as e1:
            logger.debug(f"First method failed: {e1}")
            try:
                # Second method: Update date by date
                counter = 0
                for missing_date in days_with_nan:
                    discharge_df.loc[discharge_df['date'] == missing_date, 'discharge'] = prev_forecast[prev_forecast['date'] == missing_date][col_name].values[0]
                    counter += 1
                logger.debug(f'Nans replaced with forecasted values 2nd method: {counter}')
            except Exception as e2:
                logger.debug(f"Second method failed: {e2}")
                pass  # Both methods failed, moving on

    #5: Interpolate missing values and ffill missing values at the end
    # check again for missing values
    missing_values, nans_at_end = utils_ml_forecast.check_for_nans(discharge_df.iloc[-input_chunk_length:], threshold_missing_days)
    if missing_values['exceeds_threshold'] or nans_at_end >= threshold_missing_days_end:
        return discharge_df, 1

    if missing_values['nans_in_between']:
        print('Interpolating missing values')
        discharge_df = utils_ml_forecast.gaps_imputation(discharge_df)

    if missing_values['nans_at_end']:
        print(f'Filling missing values at the end: {nans_at_end}')
        discharge_df = discharge_df.ffill(limit_area='outside')

    #6: Return the prepared data
    return discharge_df, 0


def prepare_static_data(path_to_static_features : str):
    """Load and prepare static features data."""
    static_features = pd.read_csv(path_to_static_features)
    
    if 'cluster' in static_features.columns:
        static_features = static_features.drop(columns=['cluster'])
    if 'log_q' in static_features.columns:
        static_features = static_features.drop(columns=['log_q'])
    if 'CODE' in static_features.columns:
        static_features = static_features.rename(columns={'CODE': 'code'})
    
    static_features.index = static_features['code']
    
    return static_features


def load_control_member_data( path_to_qmapped_era5, hru_ml_models):
    """Load and prepare ERA5 data."""

    path_p = os.path.join(path_to_qmapped_era5, hru_ml_models + '_P_control_member.csv')
    path_t = os.path.join(path_to_qmapped_era5, hru_ml_models + '_T_control_member.csv')
    
    p_qmapped_era5 = pd.read_csv(path_p, parse_dates=['date'])
    t_qmapped_era5 = pd.read_csv(path_t, parse_dates=['date'])
    qmapped_era5 = pd.merge(p_qmapped_era5, t_qmapped_era5, on=['code', 'date'])
    
    return qmapped_era5


def prepare_forcing_data(
        intermediate_data_path : str, 
        path_to_qmapped_era5 : str, 
        hru_ml_models : str, 
        load_forcing_hindcast=False):
    """ Prepares the forcing data for the model.
     In this function ERA5 data gets loaded (operational file and on demand hindcast file) 
     Additionally the snow data can be loaded aswell."""
    
    qmapped_era5  = load_control_member_data( path_to_qmapped_era5, hru_ml_models)
    
    
    return qmapped_era5


def get_predictor_class(
        MODEL_TO_USE : str
        ):
    available_ML_models = os.getenv('ieasyhydroforecast_available_ML_models')
    available_ML_models = available_ML_models.split(',')
    
    if MODEL_TO_USE not in available_ML_models:
        raise ValueError('Model %s is not supported.\nPlease choose one of the following models: TFT, TIDE, TSMIXER, ARIMA')
    else:
        logger.debug('Model to use: %s', MODEL_TO_USE)
        #print('Model to use: ', MODEL_TO_USE)
        if MODEL_TO_USE == 'TFT':
            predictor_class = TFTPredictor.TFTPredictor
        elif MODEL_TO_USE == 'TIDE':
            predictor_class = TiDEPredictor.TiDEPredictor
        elif MODEL_TO_USE == 'TSMIXER':
            predictor_class = TSMixerPredictor.TSMIXERPredictor
        elif MODEL_TO_USE == 'ARIMA':
            predictor_class = predictor_ARIMA.PREDICTOR

    return predictor_class


def get_rivers_to_predict(
        MODEL_TO_USE : str,
        ):
    rivers_to_predict_pentad, rivers_to_predict_decad, hydroposts_available_for_ml_forecasting = utils_ml_forecast.get_hydroposts_for_pentadal_and_decadal_forecasts()
    # Combine rivers_to_predict_pentad and rivers_to_predict_decad to get all rivers to predict, only keep unique values
    rivers_to_predict = list(set(rivers_to_predict_pentad + rivers_to_predict_decad))
    #select only codes which the model can predict.
    mask_predictable = hydroposts_available_for_ml_forecasting[MODEL_TO_USE] == True
    codes_model_can_predict = hydroposts_available_for_ml_forecasting[mask_predictable]['code'].tolist()
    rivers_to_predict = list(set(rivers_to_predict) & set(codes_model_can_predict))
    #convert to int
    rivers_to_predict = [int(code) for code in rivers_to_predict]
    logger.debug('Rivers to predict pentad: %s', rivers_to_predict_pentad)
    logger.debug('Rivers to predict decad: %s', rivers_to_predict_decad)
    logger.debug('Rivers to predict: %s', rivers_to_predict)
    logger.debug('Hydroposts available for ML forecasting: \n%s', hydroposts_available_for_ml_forecasting)

    return rivers_to_predict, hydroposts_available_for_ml_forecasting

# --------------------------------------------------------------------
# MAIN FUNCTION
# --------------------------------------------------------------------
def make_ml_forecast():

    logger.info(f'--------------------------------------------------------------------')
    logger.info(f"Starting make_forecast.py")
    print(f'--------------------------------------------------------------------')
    print(f"Starting make_forecast.py")
    # Load the environment variables
    sl.load_environment()

    # --------------------------------------------------------------------
    # DEFINE WHICH MODEL TO USE
    # --------------------------------------------------------------------
    MODEL_TO_USE = os.getenv('SAPPHIRE_MODEL_TO_USE')
    logger.debug('Model to use: %s', MODEL_TO_USE)
    predictor_class = get_predictor_class(MODEL_TO_USE)


    # --------------------------------------------------------------------
    # DEFINE THE PREDICTION MODE
    # --------------------------------------------------------------------
    PREDICTION_MODE = os.getenv('SAPPHIRE_PREDICTION_MODE')
    logger.debug('Prediction mode: %s', PREDICTION_MODE)
    if PREDICTION_MODE not in ['PENTAD', 'DECAD']:
        raise ValueError('Prediction mode %s is not supported.\nPlease choose one of the following prediction modes: PENTAD, DECAD')
    else:
        logger.debug('Prediction mode: %s', PREDICTION_MODE)
        if PREDICTION_MODE == 'PENTAD':
            forecast_horizon = 6
        else:
            forecast_horizon = 11

    # --------------------------------------------------------------------
    # INITIALIZE THE PATHS
    # --------------------------------------------------------------------
    # Access the environment variables
    intermediate_data_path = os.getenv('ieasyforecast_intermediate_data_path')
    MODELS_AND_SCALERS_PATH = os.getenv('ieasyhydroforecast_models_and_scalers_path')
    PATH_TO_STATIC_FEATURES = os.getenv('ieasyhydroforecast_PATH_TO_STATIC_FEATURES')
    # Path to the output directory
    OUTPUT_PATH_DISCHARGE = os.getenv('ieasyhydroforecast_OUTPUT_PATH_DISCHARGE')
    # Downscaled weather data
    PATH_TO_QMAPPED_ERA5 = os.getenv('ieasyhydroforecast_PATH_TO_QMAPPED_ERA5')
    HRU_ML_MODELS = os.getenv('ieasyhydroforecast_HRU_CONTROL_MEMBER')

    logger.debug('Current working directory: %s', os.getcwd())
    logger.debug('MODELS_AND_SCALERS_PATH: %s' , MODELS_AND_SCALERS_PATH)
    logger.debug('PATH_TO_STATIC_FEATURES: %s' , PATH_TO_STATIC_FEATURES)
    logger.debug('OUTPUT_PATH_DISCHARGE: %s' , OUTPUT_PATH_DISCHARGE)
    logger.debug('PATH_TO_QMAPPED_ERA5: %s' , PATH_TO_QMAPPED_ERA5)
    logger.debug('HRU_ML_MODELS: %s' , HRU_ML_MODELS)

    PATH_TO_SCALER = os.getenv('ieasyhydroforecast_PATH_TO_SCALER_' + MODEL_TO_USE)
    # Append Decad to the scaler path if the prediction mode is DECAD
    if PREDICTION_MODE == 'DECAD' and MODEL_TO_USE != 'ARIMA':
        PATH_TO_SCALER = PATH_TO_SCALER + '_Decad'

    PATH_TO_SCALER = os.path.join(MODELS_AND_SCALERS_PATH, PATH_TO_SCALER)
    # Test if the path exists
    if not os.path.exists(PATH_TO_SCALER):
        raise FileNotFoundError(f"Directory {PATH_TO_SCALER} not found.")
    logger.debug('PATH_TO_SCALER: %s' , PATH_TO_SCALER)

    if MODEL_TO_USE != 'ARIMA':
        # select the file which ends on .pt
        PATH_TO_MODEL = glob.glob(os.path.join(PATH_TO_SCALER, '*.pt'))[0]
    else:
        PATH_TO_MODEL= os.getenv('ieasyhydroforecast_PATH_TO_' + MODEL_TO_USE)
        PATH_TO_MODEL = os.path.join(PATH_TO_SCALER, PATH_TO_MODEL)

    # Test if the directory exists
    if not os.path.exists(PATH_TO_MODEL):
        raise FileNotFoundError(f"Directory {PATH_TO_MODEL} not found.")
    logger.debug('PATH_TO_MODEL: %s' , PATH_TO_MODEL)

    PATH_TO_STATIC_FEATURES = os.path.join(MODELS_AND_SCALERS_PATH, PATH_TO_STATIC_FEATURES)
    OUTPUT_PATH_DISCHARGE = os.path.join(intermediate_data_path, OUTPUT_PATH_DISCHARGE)
    #Extend the OUTPUT_PATH_DISCHARGE with the model name
    OUTPUT_PATH_DISCHARGE = os.path.join(OUTPUT_PATH_DISCHARGE, MODEL_TO_USE)

    PATH_TO_QMAPPED_ERA5 = os.path.join(intermediate_data_path, PATH_TO_QMAPPED_ERA5)

    logger.debug('joined path_to_static_features: %s' , PATH_TO_STATIC_FEATURES)
    logger.debug('joined output_path_discharge: %s' , OUTPUT_PATH_DISCHARGE)
    logger.debug('joined path_to_qmapped_era5: %s' , PATH_TO_QMAPPED_ERA5)


    # --------------------------------------------------------------------
    # GET THE RIVERS TO PREDICT
    rivers_to_predict, hydroposts_available_for_ml_forecasting = get_rivers_to_predict(MODEL_TO_USE)

    # --------------------------------------------------------------------
    # LOAD AND PREPARE DATA
    # --------------------------------------------------------------------
    PATH_TO_PAST_DISCHARGE = os.getenv('ieasyforecast_daily_discharge_file')
    PATH_TO_PAST_DISCHARGE = os.path.join(intermediate_data_path, PATH_TO_PAST_DISCHARGE)


    past_discharge = pd.read_csv(PATH_TO_PAST_DISCHARGE, parse_dates=['date'])

    qmapped_era5 = prepare_forcing_data(
        intermediate_data_path = intermediate_data_path,
        path_to_qmapped_era5 = PATH_TO_QMAPPED_ERA5,
        hru_ml_models = HRU_ML_MODELS
    )

    static_features = prepare_static_data(PATH_TO_STATIC_FEATURES)

    #get the codes to use
    codes_to_use = utils_ml_forecast.get_codes_to_use(past_discharge, qmapped_era5, static_features)
    logger.debug('codes_to_use: %s', codes_to_use)


    # --------------------------------------------------------------------
    # Calculate PET Oudin and Daylight Hours
    # --------------------------------------------------------------------
    for code in codes_to_use:
        lat = static_features[static_features['code'] == code]['LAT'].values[0]
        lon = static_features[static_features['code'] == code]['LON'].values[0]
        pet_oudin = utils_ml_forecast.calculate_pet_oudin(qmapped_era5[qmapped_era5['code'] == code], lat)
        qmapped_era5.loc[qmapped_era5['code'] == code, 'PET'] = pet_oudin
        qmapped_era5.loc[qmapped_era5['code'] == code, 'daylight_hours'] = utils_ml_forecast.calculate_daylight_hours(lat, lon, qmapped_era5[qmapped_era5['code'] == code])

    # --------------------------------------------------------------------
    # LOAD SCALER
    # --------------------------------------------------------------------
    if MODEL_TO_USE == 'ARIMA':
        scaler = None
    else:
        scaler_discharge = pd.read_csv(os.path.join(PATH_TO_SCALER, 'scaler_stats_discharge.csv'))
        scaler_discharge.index = scaler_discharge['Unnamed: 0'].astype(int)
        scaler_era5 = pd.read_csv(os.path.join(PATH_TO_SCALER, 'scaler_stats_era5.csv'))
        scaler_era5.index = scaler_era5['Unnamed: 0']
        scaler_static = pd.read_csv(os.path.join(PATH_TO_SCALER, 'scaler_stats_static.csv'))
        scaler_static.index = scaler_static['Unnamed: 0']

    # --------------------------------------------------------------------
    # LOAD MODELS AND MAKE PREDICTORS
    # --------------------------------------------------------------------
    # Load pre-trained model
    if MODEL_TO_USE == 'TFT':
        model = TFTModel.load(os.path.join(PATH_TO_MODEL), map_location=torch.device('cpu'))
    elif MODEL_TO_USE == 'TIDE':
        model = TiDEModel.load(os.path.join(PATH_TO_MODEL), map_location=torch.device('cpu'))
    elif MODEL_TO_USE == 'TSMIXER':
        model = TSMixerModel.load(os.path.join(PATH_TO_MODEL), map_location=torch.device('cpu'))
    elif MODEL_TO_USE == 'ARIMA':
        model = None

    if MODEL_TO_USE == 'ARIMA':
        predictor = predictor_class(PATH_TO_MODEL)
    else:
        scalers = {
            'scaler_discharge': scaler_discharge,
            'scaler_covariates': scaler_era5,
            'scaler_static': scaler_static
        }

        # try the load the model_config.json file
        try:
            model_dir = os.path.dirname(PATH_TO_MODEL)
            with open(os.path.join(model_dir, 'model_config.json'), 'r') as f:
                model_config = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError('model_config.json not found, Please check the model directory')

        predictor = predictor_class(
            model=model,
            scalers=scalers,
            static_features=static_features,
            dl_config_params=model_config,
            unique_id_col='code'
        )
            

    # --------------------------------------------------------------------
    # FORECAST
    # --------------------------------------------------------------------
    forecast = pd.DataFrame()

    THRESHOLD_MISSING_DAYS = os.getenv('ieasyhydroforecast_THRESHOLD_MISSING_DAYS_' + MODEL_TO_USE)
    THRESHOLD_MISSING_DAYS_END = os.getenv('ieasyhydroforecast_THRESHOLD_MISSING_DAYS_END')

    #thresholds to ints
    THRESHOLD_MISSING_DAYS = int(THRESHOLD_MISSING_DAYS)
    THRESHOLD_MISSING_DAYS_END = int(THRESHOLD_MISSING_DAYS_END)

    #load the old forecast
    if PREDICTION_MODE == 'PENTAD':
        try:
            old_forecast = pd.read_csv(os.path.join(OUTPUT_PATH_DISCHARGE, f'pentad_{MODEL_TO_USE}_forecast.csv'))
        except FileNotFoundError:
            old_forecast = pd.DataFrame()
    else:
        try:
            old_forecast = pd.read_csv(os.path.join(OUTPUT_PATH_DISCHARGE, f'decad_{MODEL_TO_USE}_forecast.csv'))
        except FileNotFoundError:
            old_forecast = pd.DataFrame()

    logger.debug('Predicting for %s rivers', len(rivers_to_predict))
    logger.debug('Rivers to predict: %s', rivers_to_predict)
    for code in rivers_to_predict:
        # Cast code to int.
        code = int(code)

        logger.debug('Code: %s', code)

        #get the data
        past_discharge_code = past_discharge[past_discharge['code'] == code]
        qmapped_era5_code = qmapped_era5[qmapped_era5['code'] == code]

        #reformat the past discharge data
        past_discharge_code['date'] = pd.to_datetime(past_discharge_code['date'])

        #sort by date
        past_discharge_code = past_discharge_code.sort_values(by='date')
        qmapped_era5_code = qmapped_era5_code.sort_values(by='date')

        logger.debug('past_discharge_code: %s', past_discharge_code.tail())
        logger.debug('qmapped_era5_code: %s', qmapped_era5_code.tail())

        #get the input chunck length -> this can than be used to determine the relevant allowed missing values
        input_chunk_length = predictor.get_input_chunk_length()
        logger.debug('input_chunk_length: %s', input_chunk_length)

        #prepare the data
        past_discharge_code, flag = prepare_forecast_data(
            past_discharge = past_discharge_code,
            threshold_missing_days = THRESHOLD_MISSING_DAYS,
            threshold_missing_days_end = THRESHOLD_MISSING_DAYS_END,
            old_forecast = old_forecast,
            code = code,
            forecast_horizon = forecast_horizon,
            input_chunk_length = input_chunk_length
            )

        predictions = predictor.predict(
            df_rivers_org = past_discharge_code, 
            df_covariates = qmapped_era5_code,
            code = code,
            n = forecast_horizon
            )
            
        if len(predictions) == 0:
            #error in forecast - something else is wrong
            flag = 2
            logger.debug('Error in forecast for code: %s', code)
        elif predictions.isna().sum().sum() > 0:
            # nan values in the forecast
            flag = 1
            logger.debug('Nan values in the forecast for code: %s', code)
        else:
            flag = 0

        #add the code to the predictions
        predictions['code'] = code
        predictions['forecast_date'] = pd.to_datetime(datetime.datetime.now().date())
        if flag != 2:
            predictions['date'] = pd.to_datetime(predictions['date'])
        else:
            predictions['date'] = pd.to_datetime(datetime.datetime.now().date())
        predictions['flag'] = flag

        predictions['date'] = pd.to_datetime(predictions['date'])
        predictions['forecast_date'] = pd.to_datetime(predictions['forecast_date'])

        forecast = pd.concat([forecast, predictions], axis=0, ignore_index=True)

        # Check if for this code we have a twin vitrual gauge which is > 0
        test_value = hydroposts_available_for_ml_forecasting.loc[hydroposts_available_for_ml_forecasting['code'] == str(code), 'virtual_station_name_twin'].iloc[0]
        if test_value is not False:
            logger.debug('Forecast for twin virtual gauge: %s', predictions)

            predictions['code'] = int(test_value)
            predictions['forecast_date'] = datetime.datetime.now().date()
            forecast = pd.concat([forecast, predictions], axis=0)

            logger.debug('Copied data and appended: %s', predictions)




    # --------------------------------------------------------------------
    # SAVE FORECAST
    # --------------------------------------------------------------------
    if PREDICTION_MODE == 'PENTAD':
        # first save the latest forecast
        forecast_today_path = os.path.join(OUTPUT_PATH_DISCHARGE, f'pentad_{MODEL_TO_USE}_forecast_latest.csv')
        forecast.to_csv(forecast_today_path, index=False)
        # Append the new forecast to the existing forecast file
        write_pentad_forecast(OUTPUT_PATH_DISCHARGE, MODEL_TO_USE, forecast)
    else:
        forecast_today_path = os.path.join(OUTPUT_PATH_DISCHARGE, f'decad_{MODEL_TO_USE}_forecast_latest.csv')
        forecast.to_csv(forecast_today_path, index=False)
        write_decad_forecast(OUTPUT_PATH_DISCHARGE, MODEL_TO_USE, forecast)

    logger.info('Forecast saved successfully. Exiting make_forecast.py\n')
    logger.info('--------------------------------------------------------------------')

if __name__ == '__main__':
    make_ml_forecast()
