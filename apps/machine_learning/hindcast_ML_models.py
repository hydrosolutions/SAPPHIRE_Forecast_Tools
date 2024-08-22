# --------------------------------------------------------------------
# HINDECAST WITH MACHINE LEARNING MODELS
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
# This script produces hindecast using machine learning models (TFT, TiDE, TSMixer)
# --------------------------------------------------------------------
# INPUTS:
# - Data:
#       Autoregressive Discharge Time Series
#       The ERA5 data from data gateaway
#       Static Basin Features
#       The Normalization Parameters
# - Model:
#       The trained model for TFT and TiDE and TSMixer
# --------------------------------------------------------------------
# OUTPUTS:
# - Hindecast (Pentad):
#       The hindecast for a pentad period, average over the pentad for different quantiles
# --------------------------------------------------------------------
# TODO:
# --------------------------------------------------------------------

# Usage
# python hindcast_ML_models.py

# --------------------------------------------------------------------
# Load Libraries
# --------------------------------------------------------------------
import os
import sys
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

#pip install git+https://github.com/hydrosolutions/sapphire-dg-client.git
import sapphire_dg_client


from darts.models import TFTModel, TiDEModel, TSMixerModel, ARIMA
from pytorch_lightning.callbacks import Callback
from pytorch_lightning.callbacks import EarlyStopping
from pytorch_lightning import Trainer
import pytorch_lightning as pl
import torch
import datetime

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
logger = logging.getLogger('make_ml_hindcast')
logger.setLevel(logging.DEBUG)
logger.handlers = []
logger.addHandler(file_handler)

import warnings
warnings.filterwarnings("ignore")

#Custom Libraries
from scr import utils_ml_forecast
from scr import predictor_TFT

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

def prepare_benchmark(benchmark_org, code):

    benchmark = benchmark_org.copy()
    benchmark = benchmark[benchmark['code'] == code]

    benchmark['date'] = pd.to_datetime(benchmark['date'])

    benchmark = benchmark.drop_duplicates(subset=['date'], keep='last')
    benchmark = benchmark.sort_values(by='date')

    return benchmark

# --------------------------------------------------------------------
# REDICT PENTAD
# --------------------------------------------------------------------
def predict_pentad(date)-> bool:
    """
    This function returns a boolean value, and the number of days until the next forecast.
    """
    days_to_save = [5, 10, 15, 20, 25]


    #check if today is the last day of the month
    tomorrow = date + datetime.timedelta(days=1)
    is_last_day_of_month = tomorrow.month != date.month

    #check if today is in the list of days to save
    is_day_to_save = date.day in days_to_save

    make_forecast = is_last_day_of_month or is_day_to_save

    #days until next forecast
    if is_last_day_of_month:
        days_until_next_forecast = 5

    elif date.day in [5, 10, 15, 20]:
        days_until_next_forecast = 5

    else:
        # if the month has 30 days -> 5 days
        # if the month has 31 days -> 6 days
        # if the month has 28 days -> 3 days
        # if the month has 29 days -> 4 days

        if date.month in [1, 3, 5, 7, 8, 10, 12]:
            days_until_next_forecast = 6
        elif date.month in [2]:
            #check if the year is a leap year
            if date.year % 4 == 0:
                days_until_next_forecast = 4
            else:
                days_until_next_forecast = 3
        else:
            days_until_next_forecast = 5

    if not make_forecast:
        days_until_next_forecast = 0

    return make_forecast, days_until_next_forecast

# --------------------------------------------------------------------
# PREDICT DECAD
# --------------------------------------------------------------------
def predict_decad(date: pd.Timestamp) -> bool:
    """
    This function returns a boolean value, and the number of days until the next forecast.
    """
    days_to_save = [10, 20]

    #check if today is the last day of the month
    tomorrow = date + datetime.timedelta(days=1)
    is_last_day_of_month = tomorrow.month != date.month

    #check if today is in the list of days to save
    is_day_to_save = date.day in days_to_save

    make_forecast = is_last_day_of_month or is_day_to_save

    #days until next forecast
    if is_last_day_of_month:
        days_until_next_forecast = 10

    elif date.day in [10]:
        days_until_next_forecast = 10

    else:
        # if the month has 30 days -> 10 days
        # if the month has 31 days -> 11 days
        # if the month has 28 days -> 8 days
        # if the month has 29 days -> 9 days

        if date.month in [1, 3, 5, 7, 8, 10, 12]:
            days_until_next_forecast = 11
        elif date.month in [2]:
            #check if the year is a leap year
            if date.year % 4 == 0:
                days_until_next_forecast = 8
            else:
                days_until_next_forecast = 9
        else:
            days_until_next_forecast = 10

    if not make_forecast:
        days_until_next_forecast = 0

    return make_forecast, days_until_next_forecast
# --------------------------------------------------------------------
# HINDECAST
# --------------------------------------------------------------------

def make_hindecast_pentad(
        past_discharge: pd.DataFrame,
        era5: pd.DataFrame,
        predictor : predictor_TFT.PREDICTOR,
        threshold_missing_days : int,
        threshold_missing_days_end: int,
        pentad_no_success: list,
        decadal_no_success: list,
        exceeds_threshhold_dict: dict,
        code: int,
        missing_values_dict: dict,
        nans_at_end_dict: dict,
        recursive_rivers: list,
        days_until_next_forecast: int,
    )-> tuple[pd.DataFrame, list, list, dict]:

        #get the input chunck length -> this can than be used to determine the relevant allowed missing values
        input_chunk_length = predictor.get_input_chunk_length()

        #check for missing values, n = number of missing values at the end
        missing_values, nans_at_end = utils_ml_forecast.check_for_nans(past_discharge.iloc[-input_chunk_length:], threshold_missing_days)

        if missing_values['exceeds_threshold'] or nans_at_end >= threshold_missing_days_end:
            pentad_no_success.append(code)
            decadal_no_success.append(code)
            exceeds_threshhold_dict[code] = True
            #we predict and get nan values
            predictions_pentad = predictor.predict(past_discharge, era5, None , code, n=days_until_next_forecast, make_plot=False)
            return predictions_pentad, pentad_no_success, decadal_no_success, exceeds_threshhold_dict, missing_values_dict, nans_at_end_dict

        elif missing_values['nans_in_between']:
            past_discharge = utils_ml_forecast.gaps_imputation(past_discharge)

        elif missing_values['nans_at_end'] and code in recursive_rivers:
            decadal_forecast_is_possible = False
            nans_at_end_dict[code] = nans_at_end
            decadal_no_success.append(code)

            past_discharge = utils_ml_forecast.recursive_imputation(past_discharge, None, era5, nans_at_end, predictor, make_plot=False)



        predictions_pentad = predictor.predict(past_discharge, era5, None , code, n=days_until_next_forecast, make_plot=False)



        return predictions_pentad, pentad_no_success, decadal_no_success, exceeds_threshhold_dict, missing_values_dict, nans_at_end_dict



def make_hindecast_decad(
        past_discharge: pd.DataFrame,
        era5: pd.DataFrame,
        predictor : predictor_TFT.PREDICTOR,
        threshold_missing_days : int,
        threshold_missing_days_end: int,
        pentad_no_success: list,
        decadal_no_success: list,
        exceeds_threshhold_dict: dict,
        code: int,
        missing_values_dict: dict,
        nans_at_end_dict: dict,
        recursive_rivers: list,
        days_until_next_forecast: int,
    )-> tuple[pd.DataFrame, list, list, dict]:
        
        #get the input chunck length -> this can than be used to determine the relevant allowed missing values
        input_chunk_length = predictor.get_input_chunk_length()
        #check for missing values, n = number of missing values at the end
        missing_values, nans_at_end = utils_ml_forecast.check_for_nans(past_discharge.iloc[-input_chunk_length:], threshold_missing_days)

        if missing_values['exceeds_threshold'] or nans_at_end >= threshold_missing_days_end:
            pentad_no_success.append(code)
            decadal_no_success.append(code)
            exceeds_threshhold_dict[code] = True
            #we predict and get nan values
            predictions = predictor.predict(past_discharge, era5, None , code, n=days_until_next_forecast, make_plot=False)
            return predictions, pentad_no_success, decadal_no_success, exceeds_threshhold_dict, missing_values_dict, nans_at_end_dict

        elif missing_values['nans_in_between']:
            past_discharge = utils_ml_forecast.gaps_imputation(past_discharge)


        # no recursive imputation for the decadal forecast
        predictions = predictor.predict(past_discharge, era5, None , code, n=days_until_next_forecast, make_plot=False)

        return predictions, pentad_no_success, decadal_no_success, exceeds_threshhold_dict, missing_values_dict, nans_at_end_dict

# --------------------------------------------------------------------
# MAIN FUNCTION
# --------------------------------------------------------------------

def main():
    # --------------------------------------------------------------------
    # DEFINE WHICH MODEL TO USE
    # --------------------------------------------------------------------
    MODEL_TO_USE = os.getenv('SAPPHIRE_MODEL_TO_USE')
    logger.info('Model to use: %s', MODEL_TO_USE)

    if MODEL_TO_USE not in ['TFT', 'TIDE', 'TSMIXER', 'ARIMA']:
        raise ValueError('Model not supported')
    else:
        print('Model to use: ', MODEL_TO_USE)
        if MODEL_TO_USE == 'TFT':
            from scr import predictor_TFT as predictor_class
        elif MODEL_TO_USE == 'TIDE':
            from scr import predictor_TIDE as predictor_class
        elif MODEL_TO_USE == 'TSMIXER':
            from scr import predictor_TSMIXER as predictor_class
        elif MODEL_TO_USE == 'ARIMA':
            from scr import predictor_ARIMA as predictor_class

        # --------------------------------------------------------------------
    # DEFINE THE HINDCAST MODE
    # --------------------------------------------------------------------
    HINDCAST_MODE = os.getenv('SAPPHIRE_HINDCAST_MODE')
    logger.debug('Prediction mode: %s', HINDCAST_MODE)
    if HINDCAST_MODE not in ['PENTAD', 'DECAD']:
        raise ValueError('Prediction mode %s is not supported.\nPlease choose one of the following prediction modes: PENTAD, DECAD')
    else:
        logger.debug('Prediction mode: %s', HINDCAST_MODE)
        if HINDCAST_MODE == 'PENTAD':
            forecast_horizon = 5
        else:
            forecast_horizon = 10
    # --------------------------------------------------------------------
    # INITIALIZE THE ENVIRONMENT
    # --------------------------------------------------------------------
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Specify the path to the .env file
    sl.load_environment()

    # --------------------------------------------------------------------
    # LOAD IN ENVIROMENT VARIABLES
    # --------------------------------------------------------------------

    intermediate_data_path = os.getenv('ieasyforecast_intermediate_data_path')
    PATH_TO_PAST_DISCHARGE = os.getenv('ieasyforecast_daily_discharge_file')
    PATH_TO_PAST_DISCHARGE = os.path.join(intermediate_data_path, PATH_TO_PAST_DISCHARGE)
    logger.debug("PATH_TO_PAST_DISCHARGE: %s", PATH_TO_PAST_DISCHARGE)
    # Test if file exists
    if not os.path.exists(PATH_TO_PAST_DISCHARGE):
        raise FileNotFoundError(f"File {PATH_TO_PAST_DISCHARGE} not found.")

    MODELS_AND_SCALERS_PATH = os.getenv('ieasyhydroforecast_models_and_scalers_path')
    PATH_TO_STATIC_FEATURES = os.getenv('ieasyhydroforecast_PATH_TO_STATIC_FEATURES')
    logger.debug("MODELS_AND_SCALERS_PATH: %s", MODELS_AND_SCALERS_PATH)
    logger.debug("PATH_TO_STATIC_FEATURES: %s", PATH_TO_STATIC_FEATURES)
    PATH_TO_STATIC_FEATURES = os.path.join(MODELS_AND_SCALERS_PATH, PATH_TO_STATIC_FEATURES)
    # Test if path exists
    if not os.path.exists(PATH_TO_STATIC_FEATURES):
        raise FileExistsError(f"Directory {PATH_TO_STATIC_FEATURES} not found.")
    
    PATH_TO_SCALER = os.getenv('ieasyhydroforecast_PATH_TO_SCALER_' + MODEL_TO_USE)
    # Append Decad to the scaler path if the prediction mode is DECAD
    if HINDCAST_MODE == 'DECAD' and MODEL_TO_USE != 'ARIMA':
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

    #PATH_TO_MODEL= os.getenv('ieasyhydroforecast_PATH_TO_' + MODEL_TO_USE)
    #PATH_TO_MODEL = os.path.join(PATH_TO_SCALER, PATH_TO_MODEL)
    # Test if the directory exists
    if not os.path.exists(PATH_TO_MODEL):
        raise FileNotFoundError(f"Directory {PATH_TO_MODEL} not found.")
    logger.debug('PATH_TO_MODEL: %s' , PATH_TO_MODEL)

    CODES_HINDECAST = os.getenv('ieasyhydroforecast_CODES_HINDECAST')
    HRU_ML_MODELS = os.getenv('ieasyhydroforecast_HRU_CONTROL_MEMBER')
    Q_MAP_PARAM_PATH = os.getenv('ieasyhydroforecast_Q_MAP_PARAM_PATH')

    rivers_to_predict_pentad, rivers_to_predict_decad, hydroposts_available_for_ml_forecasting = utils_ml_forecast.get_hydroposts_for_pentadal_and_decadal_forecasts()
    # Combine rivers_to_predict_pentad and rivers_to_predict_decad to get all rivers to predict, only keep unique values
    rivers_to_predict = list(set(rivers_to_predict_pentad + rivers_to_predict_decad))
    logger.debug('Rivers to predict pentad: %s', rivers_to_predict_pentad)
    logger.debug('Rivers to predict decad: %s', rivers_to_predict_decad)
    logger.debug('Rivers to predict: %s', rivers_to_predict)
    logger.debug('Hydroposts available for ML forecasting: \n%s', hydroposts_available_for_ml_forecasting)

    # --------------------------------------------------------------------
    # READ IN FORCING DATA
    # --------------------------------------------------------------------
    #start date is the date where the first forecast is made
    # NOTE: The start date should be +lookback days before the first date in the era5 data 
    # Here we hardcode the lookback days to 60
    start_date = os.getenv('ieasyhydroforecast_START_DATE')
    #end date is the date where the last forecast is made
    # NOTE: The end date should be at maximum the last date in the era5 data - forecast horizon
    end_date = os.getenv('ieasyhydroforecast_END_DATE')

    PATH_ERA5_REANALYSIS = os.getenv('ieasyhydroforecast_OUTPUT_PATH_REANALYSIS')
    PATH_ERA5_REANALYSIS = os.path.join(intermediate_data_path, PATH_ERA5_REANALYSIS)

    P_era5_renalysis_file = f'{HRU_ML_MODELS}_P_reanalysis.csv'
    T_era5_renalysis_file = f'{HRU_ML_MODELS}_T_reanalysis.csv'

    P_era5_renalysis_file = os.path.join(PATH_ERA5_REANALYSIS, P_era5_renalysis_file)
    T_era5_renalysis_file = os.path.join(PATH_ERA5_REANALYSIS, T_era5_renalysis_file)

    # Test if the file exists
    if not os.path.exists(P_era5_renalysis_file):
        raise FileNotFoundError(f"File {P_era5_renalysis_file} not found.")
    if not os.path.exists(T_era5_renalysis_file):
        raise FileNotFoundError(f"File {T_era5_renalysis_file} not found.")
    
    P_reanalysis = pd.read_csv(P_era5_renalysis_file)
    T_reanalysis = pd.read_csv(T_era5_renalysis_file)

    era5_data_transformed_renalysis = pd.merge(P_reanalysis, T_reanalysis, on=['code', 'date'])

    # Read in The Operational Forcing Data
    PATH_OPERATIONAL_CONTROL_MEMBER = os.getenv('ieasyhydroforecast_OUTPUT_PATH_CM')
    PATH_OPERATIONAL_CONTROL_MEMBER = os.path.join(intermediate_data_path, PATH_OPERATIONAL_CONTROL_MEMBER)

    P_operational_file = f'{HRU_ML_MODELS}_P_control_member.csv'
    T_operational_file = f'{HRU_ML_MODELS}_T_control_member.csv'

    P_operational_file = os.path.join(PATH_OPERATIONAL_CONTROL_MEMBER, P_operational_file)
    T_operational_file = os.path.join(PATH_OPERATIONAL_CONTROL_MEMBER, T_operational_file)

    # if there is a control member, then use it, otherwise only use the reanalysis data
    use_operational_control_member = True
    # Test if the file exists
    if not os.path.exists(P_operational_file):
        print(f"File {P_operational_file} not found.")
        logger.warning(f"File {P_operational_file} not found.")
        use_operational_control_member = False
    if not os.path.exists(T_operational_file):
        print(f"File {T_operational_file} not found.")
        logger.warning(f"File {T_operational_file} not found.")
        use_operational_control_member = False
    
    if use_operational_control_member:
        P_control_member = pd.read_csv(P_operational_file)
        T_control_member = pd.read_csv(T_operational_file)

        era5_data_transformed_cm = pd.merge(P_control_member, T_control_member, on=['code', 'date'])

        # Concat the reanalysis and control member data
        era5_data_transformed_renalysis['date'] = pd.to_datetime(era5_data_transformed_renalysis['date'])
        era5_data_transformed_cm['date'] = pd.to_datetime(era5_data_transformed_cm['date'])

        era5_data_transformed = pd.concat([era5_data_transformed_renalysis, era5_data_transformed_cm], axis=0)
    else:
        era5_data_transformed = era5_data_transformed_renalysis.copy()

    # sort by date
    era5_data_transformed = era5_data_transformed.sort_values(by=['code', 'date'])

    #remove dublicates on date and code , keep last
    era5_data_transformed = era5_data_transformed.drop_duplicates(subset=['date', 'code'], keep='last')

    # check if the start and end date is in the era5 data
    if pd.to_datetime(start_date) < era5_data_transformed['date'].min() + pd.DateOffset(days=60):
        raise ValueError(f'The start date {start_date} is before the first date in the era5 data {era5_data_transformed["date"].min()} + 60 days')
    if pd.to_datetime(end_date) > era5_data_transformed['date'].max() - pd.DateOffset(days=forecast_horizon):
        raise ValueError(f'The end date {end_date} is after the last date in the era5 data {era5_data_transformed["date"].max()} - forecast horizon {forecast_horizon}')

    # --------------------------------------------------------------------
    # Preprocessing
    # --------------------------------------------------------------------
    # STATIC FEATURES

    static_features = pd.read_csv(PATH_TO_STATIC_FEATURES)
    static_features = static_features.drop(columns=['cluster', 'log_q'])
    static_features.index = static_features['CODE']

    if MODEL_TO_USE == 'ARIMA':
        scaler = None
    else:
        scaler_discharge = pd.read_csv(os.path.join(PATH_TO_SCALER, 'scaler_stats_discharge.csv'))
        scaler_discharge.index = scaler_discharge['Unnamed: 0'].astype(int)
        scaler_era5 = pd.read_csv(os.path.join(PATH_TO_SCALER, 'scaler_stats_era5.csv'))
        scaler_era5.index = scaler_era5['Unnamed: 0']
        scaler_static = pd.read_csv(os.path.join(PATH_TO_SCALER, 'scaler_stats_static.csv'))
        scaler_static.index = scaler_static['Unnamed: 0']


    #PATH TO OBSERVED DISCHARGE
    observed_discharge = pd.read_csv(PATH_TO_PAST_DISCHARGE)

    observed_discharge['code'] = observed_discharge['code'].astype(int)
    era5_data_transformed['code'] = era5_data_transformed['code'].astype(int)

    #get the codes to use
    codes_to_use = get_codes_to_use(observed_discharge, era5_data_transformed, static_features)


    # --------------------------------------------------------------------
    # Calculate PET Oudin and Daylight Hours
    # --------------------------------------------------------------------
    for code in era5_data_transformed['code'].unique():
        lat = static_features.loc[code]['LAT']
        lon = static_features.loc[code]['LON']
        pet_oudin = utils_ml_forecast.calculate_pet_oudin(era5_data_transformed[era5_data_transformed['code'] == code], lat)
        era5_data_transformed.loc[era5_data_transformed['code'] == code, 'PET'] = pet_oudin
        era5_data_transformed.loc[era5_data_transformed['code'] == code, 'daylight_hours'] = utils_ml_forecast.calculate_daylight_hours(lat, lon, era5_data_transformed[era5_data_transformed['code'] == code])



    # --------------------------------------------------------------------
    # LOAD MODELS AND MAKE PREDICTORS
    # --------------------------------------------------------------------
    # MODEL PREDICTOR
    if MODEL_TO_USE == 'TFT':
        model = TFTModel.load(os.path.join(PATH_TO_MODEL), map_location=torch.device('cpu'))
    elif MODEL_TO_USE == 'TIDE':
        model = TiDEModel.load(os.path.join(PATH_TO_MODEL), map_location=torch.device('cpu'))
    elif MODEL_TO_USE == 'TSMIXER':
        model = TSMixerModel.load(os.path.join(PATH_TO_MODEL), map_location=torch.device('cpu'))
    elif MODEL_TO_USE == 'ARIMA':
        model = None


    if MODEL_TO_USE == 'ARIMA':
        predictor = predictor_class.PREDICTOR(PATH_TO_MODEL)
 
    else:
        predictor = predictor_class.PREDICTOR(model, scaler_discharge, scaler_era5, scaler_static, static_features)


    # --------------------------------------------------------------------
    # HINDECAST
    # --------------------------------------------------------------------
    THRESHOLD_MISSING_DAYS = os.getenv('ieasyhydroforecast_THRESHOLD_MISSING_DAYS_' + MODEL_TO_USE)
    THRESHOLD_MISSING_DAYS_END = os.getenv('ieasyhydroforecast_THRESHOLD_MISSING_DAYS_END')

    # Get a list of codes for recursie imputation, depending on the MODEL_TO_USE
    if MODEL_TO_USE == 'TFT':
        RECURSIVE_RIVERS = hydroposts_available_for_ml_forecasting.loc[hydroposts_available_for_ml_forecasting['recursive_imputation_tft'], 'code'].dropna().astype(int).tolist()
    elif MODEL_TO_USE == 'TIDE':
        RECURSIVE_RIVERS = hydroposts_available_for_ml_forecasting.loc[hydroposts_available_for_ml_forecasting['recursive_imputation_tide'], 'code'].dropna().astype(int).tolist()
    elif MODEL_TO_USE == 'TSMIXER':
        RECURSIVE_RIVERS = hydroposts_available_for_ml_forecasting.loc[hydroposts_available_for_ml_forecasting['recursive_imputation_tsmixer'], 'code'].dropna().astype(int).tolist()
    elif MODEL_TO_USE == 'ARIMA':
        RECURSIVE_RIVERS = hydroposts_available_for_ml_forecasting.loc[hydroposts_available_for_ml_forecasting['recursive_imputation_arima'], 'code'].dropna().astype(int).tolist()

    logger.debug('Recursive rivers: %s', RECURSIVE_RIVERS)

    #thresholds to ints
    THRESHOLD_MISSING_DAYS= int(THRESHOLD_MISSING_DAYS)
    THRESHOLD_MISSING_DAYS_END = int(THRESHOLD_MISSING_DAYS_END)


    if CODES_HINDECAST != 'None':
        codes_hindecast = [int(code) for code in CODES_HINDECAST.split(',')]
    else:
        codes_hindecast = codes_to_use


    pred_date = pd.to_datetime(start_date)
    
    observed_discharge['date'] = pd.to_datetime(observed_discharge['date'])
    era5_data_transformed['date'] = pd.to_datetime(era5_data_transformed['date'])

    hindecast_df = pd.DataFrame()
    hindecast_daily_df = pd.DataFrame()

    current_year = pred_date.year
    print(f'Starting Hindcast for the dates: {start_date} to {end_date}')
    while pred_date <= pd.to_datetime(end_date):

            
        days_until_next_forecast = forecast_horizon # either 5 or 10

        if pred_date.year != current_year:
            current_year = pred_date.year
            print(f'Year: {current_year}')
       
        for code in codes_hindecast:

            # get the discharge data
            past_discharge = observed_discharge[observed_discharge['code'] == code]
            past_discharge = past_discharge[past_discharge['date'] <= pred_date]

            # get the era5 data
            era5 = era5_data_transformed[era5_data_transformed['code'] == code]
            date_end_forecast = pred_date + pd.DateOffset(days=15)
            era5 = era5[era5['date'] <= date_end_forecast]

            if HINDCAST_MODE == 'PENTAD':
                # make prediction with predictor
                predictions = make_hindecast_pentad(
                        past_discharge=past_discharge,
                        era5=era5,
                        predictor=predictor,
                        threshold_missing_days=THRESHOLD_MISSING_DAYS,
                        threshold_missing_days_end=THRESHOLD_MISSING_DAYS_END,
                        pentad_no_success=[],
                        decadal_no_success=[],
                        exceeds_threshhold_dict={},
                        code=code,
                        missing_values_dict={},
                        nans_at_end_dict={},
                        recursive_rivers=RECURSIVE_RIVERS,
                        days_until_next_forecast=days_until_next_forecast

                    )[0]

            else:
                predictions = make_hindecast_decad(
                        past_discharge=past_discharge,
                        era5=era5,
                        predictor=predictor,
                        threshold_missing_days=THRESHOLD_MISSING_DAYS,
                        threshold_missing_days_end=THRESHOLD_MISSING_DAYS_END,
                        pentad_no_success=[],
                        decadal_no_success=[],
                        exceeds_threshhold_dict={},
                        code=code,
                        missing_values_dict={},
                        nans_at_end_dict={},
                        recursive_rivers=RECURSIVE_RIVERS,
                        days_until_next_forecast=days_until_next_forecast
                    )[0]


            # Calculate the mean for each quantile column
            mean_values = predictions.drop(columns=['date']).mean(skipna=True)

            # Create a DataFrame with the mean values, the prediction date and the code
            hindecast = pd.DataFrame(mean_values).T
            hindecast['date'] = pred_date
            hindecast['forecast_date'] = pred_date  # the forecast date is the same as the prediction date
            hindecast['code'] = code

            # Append to the daily DataFrame
            hindecast_daily_df_temp = predictions.copy()
            # the date is already in the predictions
            hindecast_daily_df_temp['code'] = code
            hindecast_daily_df_temp['forecast_date'] = pred_date

            hindecast_daily_df = pd.concat([hindecast_daily_df, hindecast_daily_df_temp], axis=0)
            hindecast_df = pd.concat([hindecast_df, hindecast], axis=0)



        pred_date = pred_date + pd.DateOffset(days=1)

    # --------------------------------------------------------------------
    # SAVE HINDECAST
    # --------------------------------------------------------------------
    # Path to the output directory
    OUTPUT_PATH_DISCHARGE = os.getenv('ieasyhydroforecast_OUTPUT_PATH_DISCHARGE')
    OUTPUT_PATH_DISCHARGE = os.path.join(intermediate_data_path, OUTPUT_PATH_DISCHARGE)
    OUTPUT_PATH_DISCHARGE = os.path.join(OUTPUT_PATH_DISCHARGE, 'hindcast', MODEL_TO_USE)
    # Test if path exists and create it if it doesn't
    if not os.path.exists(OUTPUT_PATH_DISCHARGE):
        os.makedirs(OUTPUT_PATH_DISCHARGE, exist_ok=True)
    
    
    hindecast_df.to_csv(os.path.join(OUTPUT_PATH_DISCHARGE, f'{MODEL_TO_USE}_{HINDCAST_MODE}_hindcast_{start_date}_{end_date}.csv'), index=False)
    hindecast_daily_df.to_csv(os.path.join(OUTPUT_PATH_DISCHARGE, f'{MODEL_TO_USE}_{HINDCAST_MODE}_hindcast_daily_{start_date}_{end_date}.csv'), index=False)


    logger.info('Hindcast Done!')


if __name__ == '__main__':
    main()


