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
# Quantile Mapping
# --------------------------------------------------------------------
def ptf(x: np.array,  a: float, b:float ) -> np.array:
    return a * np.power(x, b)

def quantile_mapping_ptf(sce_data:np.array, a: float, b: float, wet_days: bool = True, wet_day_threshold: float = 0) -> np.array:
    """
    Perform quantile mapping for precipitation or temperature data.
    FORMULA: y_fit = a * y_era^b
    Inputs:
        sce_data: numpy array of shape (n,) with the data to be transformed.
        a: float
        b: float
        wet_days: boolean, if True, the transformation is performed only for wet days.
        wet_day_threshold: float, the threshold to define wet days.
    Outputs:
        transformed_sce: numpy array of shape (n,) with the transformed data.
    """
    if wet_days:
        dry_days = sce_data <= wet_day_threshold
        # dry days to zero
        sce_data[dry_days] = 0
        transformed_sce = ptf(sce_data, a, b)

    else:
        transformed_sce = ptf(sce_data, a, b)

    return transformed_sce

def do_quantile_mapping(era5_data: pd.DataFrame, P_param: pd.DataFrame, T_param: pd.DataFrame, ensemble: bool) -> pd.DataFrame:
    """
    Loop over all the stations and perform the quantile mapping for each station for the control member.
    Inputs:
        era5_data: pandas DataFrame with the ERA5 data.
        P_param: pandas DataFrame with the precipitation parameters.
        T_param: pandas DataFrame with the temperature parameters.
    Outputs:
        P_data: pandas DataFrame with the transformed precipitation data.
        T_data: pandas DataFrame with the transformed temperature data.
    """
    era5_data = era5_data.copy()
    #get the unique codes
    codes = era5_data['code'].unique()
    #iterate over the codes
    for code in codes:
        #get the data for the code
        code_data = era5_data[era5_data['code'] == code]

        #get the parameters for the code
        P_param_code = P_param[P_param['code'] == code]
        T_param_code = T_param[T_param['code'] == code]

        #get the parameters
        a_P = P_param_code['a'].values
        b_P = P_param_code['b'].values
        threshold_P = P_param_code['wet_day'].values

        a_T = T_param_code['a'].values
        b_T = T_param_code['b'].values

        #transform the data
        code_data.loc[:,'P'] = quantile_mapping_ptf(code_data['P'].values, a_P, b_P, wet_days=True, wet_day_threshold=threshold_P)

        #for temperature we need to tranform it to Kelvin
        T_data = code_data['T'].values + 273.15
        T_fitted = quantile_mapping_ptf(T_data, a_T, b_T, wet_days=False, wet_day_threshold=0)
        code_data.loc[:,'T'] = T_fitted - 273.15

        era5_data.loc[era5_data['code'] == code, 'P'] = code_data['P']
        era5_data.loc[era5_data['code'] == code, 'T'] = code_data['T']

    if ensemble:
        P_data = era5_data[['date', 'P', 'code', 'ensemble_member']].copy()
        T_data = era5_data[['date', 'T', 'code', 'ensemble_member']].copy()
    else:
        P_data = era5_data[['date', 'P', 'code']].copy()
        T_data = era5_data[['date', 'T', 'code']].copy()

    return P_data, T_data


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
# TRANSFORM DATA FILE
# --------------------------------------------------------------------
def transform_data_file_control_member(data_file:pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the data file from the data gateaway in a more handy format.
    Inputs:
        data_file: pd.DataFrame with the data from the data gateaway. columns Code XXXXX is T and columns Code XXXXX.1 is P
    Outputs:
        transformed_data: pd.DataFrame with the transformed data. Columns are 'date', 'P', 'T', 'code'
    """
    data_file = data_file.copy()
    # rename the Station column to 'date'
    data_file.rename(columns={'Station': 'date'}, inplace=True)

    #than we need to drop the first 7 rows of the era5 data
    data_file = data_file.iloc[7:]

    # now we need to convert the date column to a datetime object
    data_file['date'] = pd.to_datetime(data_file['date'], dayfirst=True)

    #sort by the date
    data_file = data_file.sort_values('date')


    transformed_data_file = pd.DataFrame()

    #unique codes
    codes = data_file.columns[1:]

    # if the ".1" is not in code
    codes = [code for code in codes if (code[-2:] != '.1' and code != 'Source')]

    #iterate over the codes
    for code in codes:
        # get the data for the code
        code_data = data_file[['date', code, code + '.1']].copy()
        # rename the columns
        code_data.rename(columns={code: 'T', code + '.1': 'P'}, inplace=True)
        # Add the 'code' column
        code_data['code'] = code
        # Convert 'T' and 'P' columns to numeric, coercing errors
        code_data['T'] = pd.to_numeric(code_data['T'], errors='coerce').astype(float)
        code_data['P'] = pd.to_numeric(code_data['P'], errors='coerce').astype(float)
        transformed_data_file = pd.concat([transformed_data_file, code_data], axis = 0)


    return transformed_data_file


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
    # INITIALIZE THE ENVIRONMENT
    # --------------------------------------------------------------------
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Specify the path to the .env file
    sl.load_environment()

    # --------------------------------------------------------------------
    # LOAD IN ENVIROMENT VARIABLES
    # --------------------------------------------------------------------
    # Test if an API key is available and exit the program if it isn't
    if not os.getenv('ieasyhydroforecast_API_KEY_GATEAWAY'):
        logger.warning("No API key for the data gateway found. Exiting program.\nMachine learning or conceptual models will not be run.")
        sys.exit(1)
    else:
        API_KEY = os.getenv('ieasyhydroforecast_API_KEY_GATEAWAY')

    intermediate_data_path = os.getenv('ieasyforecast_intermediate_data_path')
    PATH_TO_PAST_DISCHARGE = os.getenv('ieasyforecast_daily_discharge_file')
    PATH_TO_PAST_DISCHARGE = os.path.join(intermediate_data_path, PATH_TO_PAST_DISCHARGE)
    logger.debug("PATH_TO_PAST_DISCHARGE: %s", PATH_TO_PAST_DISCHARGE)
    # Test if file exists
    if not os.path.exists(PATH_TO_PAST_DISCHARGE):
        raise FileNotFoundError(f"File {PATH_TO_PAST_DISCHARGE} not found.")
    #output_path for the data from the data gateaway
    OUTPUT_PATH_DG = os.getenv('ieasyhydroforecast_OUTPUT_PATH_DG')
    PATH_TO_SCALER = os.getenv('ieasyhydroforecast_PATH_TO_SCALER_' + MODEL_TO_USE)
    MODELS_AND_SCALERS_PATH = os.getenv('ieasyhydroforecast_models_and_scalers_path')
    PATH_TO_STATIC_FEATURES = os.getenv('ieasyhydroforecast_PATH_TO_STATIC_FEATURES')
    logger.debug("MODELS_AND_SCALERS_PATH: %s", MODELS_AND_SCALERS_PATH)
    logger.debug("PATH_TO_STATIC_FEATURES: %s", PATH_TO_STATIC_FEATURES)
    PATH_TO_STATIC_FEATURES = os.path.join(MODELS_AND_SCALERS_PATH, PATH_TO_STATIC_FEATURES)
    # Test if path exists
    if not os.path.exists(PATH_TO_STATIC_FEATURES):
        raise FileExistsError(f"Directory {PATH_TO_STATIC_FEATURES} not found.")

    PATH_TO_SCALER = os.path.join(MODELS_AND_SCALERS_PATH, PATH_TO_SCALER)
    # Test if the path exists
    if not os.path.exists(PATH_TO_SCALER):
        raise FileNotFoundError(f"Directory {PATH_TO_SCALER} not found.")
    logger.debug('PATH_TO_SCALER: %s' , PATH_TO_SCALER)

    PATH_TO_MODEL= os.getenv('ieasyhydroforecast_PATH_TO_' + MODEL_TO_USE)
    PATH_TO_MODEL = os.path.join(PATH_TO_SCALER, PATH_TO_MODEL)
    # Test if the directory exists
    if not os.path.exists(PATH_TO_MODEL):
        raise FileNotFoundError(f"Directory {PATH_TO_MODEL} not found.")
    logger.debug('PATH_TO_MODEL: %s' , PATH_TO_MODEL)

    CODES_HINDECAST = os.getenv('ieasyhydroforecast_CODES_HINDECAST')
    HRU_ML_MODELS = os.getenv('ieasyhydroforecast_HRU_CONTROL_MEMBER')
    Q_MAP_PARAM_PATH = os.getenv('ieasyhydroforecast_Q_MAP_PARAM_PATH')

    OUTPUT_PATH_DG = os.path.join(
        os.getenv('ieasyforecast_intermediate_data_path'),
        os.getenv('ieasyhydroforecast_OUTPUT_PATH_DG'))
    # Test if path exists and raise an error if it does not
    if not os.path.exists(OUTPUT_PATH_DG):
        raise FileNotFoundError(f"The path {OUTPUT_PATH_DG} does not exist.")
    # Test if the folder OUTPUT_PATH_DG is empty
    if not os.listdir(OUTPUT_PATH_DG):
        raise FileNotFoundError(f"The directory {OUTPUT_PATH_DG} is unexpectedly empty.")

    Q_MAP_PARAM_PATH = os.path.join(
        os.getenv('ieasyhydroforecast_models_and_scalers_path'),
        os.getenv('ieasyhydroforecast_Q_MAP_PARAM_PATH'))

    logger.debug("OUTPUT_PATH_DG: %s", OUTPUT_PATH_DG)

    rivers_to_predict_pentad, rivers_to_predict_decad, hydroposts_available_for_ml_forecasting = utils_ml_forecast.get_hydroposts_for_pentadal_and_decadal_forecasts()
    # Combine rivers_to_predict_pentad and rivers_to_predict_decad to get all rivers to predict, only keep unique values
    rivers_to_predict = list(set(rivers_to_predict_pentad + rivers_to_predict_decad))
    logger.debug('Rivers to predict pentad: %s', rivers_to_predict_pentad)
    logger.debug('Rivers to predict decad: %s', rivers_to_predict_decad)
    logger.debug('Rivers to predict: %s', rivers_to_predict)
    logger.debug('Hydroposts available for ML forecasting: \n%s', hydroposts_available_for_ml_forecasting)

    # --------------------------------------------------------------------
    # GET ERA5 DATA FROM THE API
    # --------------------------------------------------------------------

    #start date is the date where the first forecast is made
    start_date = os.getenv('ieasyhydroforecast_START_DATE')
    #end date is the date where the last forecast is made
    end_date = os.getenv('ieasyhydroforecast_END_DATE')

    #check if the hindcast should be done on a daily basis or on a pentad basis
    HINDCAST_DAILY = os.getenv('ieasyhydroforecasts_produce_daily_ml_hindcast')
    if HINDCAST_DAILY == 'True':
        HINDCAST_DAILY = True
    else:
        HINDCAST_DAILY = False

    client = sapphire_dg_client.client.SapphireDGClient(api_key= API_KEY)

    #substract 60 days from the start date
    start_date = pd.to_datetime(start_date) - pd.DateOffset(days=60)
    start_date = start_date.strftime('%Y-%m-%d')

    #loads in the data from start_date -60 up to the current date + forecast
    era5_data_path = client.operational.get_control_spinup_and_forecast(HRU_ML_MODELS,
                                                                    date=start_date,
                                                                    directory=OUTPUT_PATH_DG)

    era5_data = pd.read_csv(era5_data_path)

    # Transform the data
    era5_data_transformed = transform_data_file_control_member(era5_data)

    era5_data_transformed['code'] = era5_data_transformed['code'].astype(str)
    #get the parameters
    P_params_hru = pd.read_csv(os.path.join(Q_MAP_PARAM_PATH, 'HRU'+HRU_ML_MODELS+ '_P_params.csv'))
    T_params_hru = pd.read_csv(os.path.join(Q_MAP_PARAM_PATH, 'HRU'+HRU_ML_MODELS + '_T_params.csv'))

    #transform to string, as the other code is a string
    P_params_hru['code'] = P_params_hru['code'].astype(str)
    T_params_hru['code'] = T_params_hru['code'].astype(str)

    #perform the quantile mapping for the control member for the HRU's without Eleavtion bands
    P_data, T_data = do_quantile_mapping(era5_data_transformed, P_params_hru, T_params_hru, ensemble=False)

    era5_data_transformed = pd.merge(P_data, T_data, on=['code', 'date'])


    # --------------------------------------------------------------------
    # Preprocessing
    # --------------------------------------------------------------------
    # STATIC FEATURES

    static_features = pd.read_csv(PATH_TO_STATIC_FEATURES)
    static_features = static_features.drop(columns=['cluster', 'log_q'])
    static_features.index = static_features['CODE']

    if MODEL_TO_USE == 'ARIMA':
        scaler = pd.read_csv(os.path.join(PATH_TO_SCALER, 'scalers_arima.csv'))
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
        model_pentad = TFTModel.load(os.path.join(PATH_TO_MODEL), map_location=torch.device('cpu'))
        model_decad = TFTModel.load(os.path.join(PATH_TO_MODEL), map_location=torch.device('cpu'))
    elif MODEL_TO_USE == 'TIDE':
        model_pentad = TiDEModel.load(os.path.join(PATH_TO_MODEL), map_location=torch.device('cpu'))
        model_decad = TiDEModel.load(os.path.join(PATH_TO_MODEL), map_location=torch.device('cpu'))
    elif MODEL_TO_USE == 'TSMIXER':
        model_pentad = TSMixerModel.load(os.path.join(PATH_TO_MODEL), map_location=torch.device('cpu'))
        model_decad = TSMixerModel.load(os.path.join(PATH_TO_MODEL), map_location=torch.device('cpu'))
    elif MODEL_TO_USE == 'ARIMA':
        model_pentad = None
        model_decad = None


    if MODEL_TO_USE == 'ARIMA':
        predictor_pentad = predictor_class.PREDICTOR(PATH_TO_MODEL, scaler)
    else:
        predictor_pentad = predictor_class.PREDICTOR(model_pentad, scaler_discharge, scaler_era5, scaler_static, static_features)


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


    pred_date = pd.to_datetime(start_date) + pd.DateOffset(days=60)

    observed_discharge['date'] = pd.to_datetime(observed_discharge['date'])
    era5_data_transformed['date'] = pd.to_datetime(era5_data_transformed['date'])

    hindecast_df = pd.DataFrame()
    hindecast_daily_df = pd.DataFrame()

    current_year = pred_date.year
    print(f'Starting Hindcast for the dates: {start_date} to {end_date}')
    while pred_date <= pd.to_datetime(end_date):

        make_forecast, days_until_next_forecast = predict_pentad(pred_date)

        if HINDCAST_DAILY:
            make_forecast = True
            days_until_next_forecast = 5

        if pred_date.year != current_year:
            current_year = pred_date.year
            print(f'Year: {current_year}')
        if make_forecast:
            for code in codes_hindecast:

                # get the discharge data
                past_discharge = observed_discharge[observed_discharge['code'] == code]
                past_discharge = past_discharge[past_discharge['date'] <= pred_date]

                # get the era5 data
                era5 = era5_data_transformed[era5_data_transformed['code'] == code]
                date_end_forecast = pred_date + pd.DateOffset(days=15)
                era5 = era5[era5['date'] <= date_end_forecast]

                # make prediction with predictor
                predictions_pentad = make_hindecast_pentad(
                    past_discharge=past_discharge,
                    era5=era5,
                    predictor=predictor_pentad,
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
                mean_values = predictions_pentad.drop(columns=['date']).mean(skipna=True)

                # Create a DataFrame with the mean values, the prediction date and the code
                hindecast = pd.DataFrame(mean_values).T
                hindecast['date'] = pred_date
                hindecast['forecast_date'] = pred_date  # the forecast date is the same as the prediction date
                hindecast['code'] = code

                # Append to the daily DataFrame
                hindecast_daily_df_temp = predictions_pentad.copy()
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
    hindecast_df.to_csv(os.path.join(OUTPUT_PATH_DISCHARGE, f'{MODEL_TO_USE}_hindcast_{start_date}_{end_date}.csv'), index=False)
    hindecast_daily_df.to_csv(os.path.join(OUTPUT_PATH_DISCHARGE, f'{MODEL_TO_USE}_hindcast_daily_{start_date}_{end_date}.csv'), index=False)


    logger.info('Hindcast Done!')


if __name__ == '__main__':
    main()


