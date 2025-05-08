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

# Usage
# SAPPHIRE_OPDEV_ENV=True SAPPHIRE_MODEL_TO_USE=TIDE SAPPHIRE_HINDCAST_MODE=PENTAD python hindcast_ML_models.py

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
        if MODEL_TO_USE == 'TFT':
            predictor_class = TFTPredictor.TFTPredictor
        elif MODEL_TO_USE == 'TIDE':
            predictor_class = TiDEPredictor.TiDEPredictor
        elif MODEL_TO_USE == 'TSMIXER':
            predictor_class = TSMixerPredictor.TSMIXERPredictor
        elif MODEL_TO_USE == 'ARIMA':
            predictor_class = predictor_ARIMA.PREDICTOR

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
            forecast_horizon = 6
        else:
            forecast_horizon = 11
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

    #CODES_HINDECAST = os.getenv('ieasyhydroforecast_CODES_HINDECAST')
    HRU_ML_MODELS = os.getenv('ieasyhydroforecast_HRU_CONTROL_MEMBER')
    Q_MAP_PARAM_PATH = os.getenv('ieasyhydroforecast_Q_MAP_PARAM_PATH')

    rivers_to_predict_pentad, rivers_to_predict_decad, hydroposts_available_for_ml_forecasting = utils_ml_forecast.get_hydroposts_for_pentadal_and_decadal_forecasts()
    # Combine rivers_to_predict_pentad and rivers_to_predict_decad to get all rivers to predict, only keep unique values
    rivers_to_predict = list(set(rivers_to_predict_pentad + rivers_to_predict_decad))
    #select only codes which the model can predict.
    mask_predictable = hydroposts_available_for_ml_forecasting[MODEL_TO_USE] == True
    codes_model_can_predict = hydroposts_available_for_ml_forecasting[mask_predictable]['code'].tolist()
    rivers_to_predict = list(set(rivers_to_predict) & set(codes_model_can_predict))
    #convert to int 
    rivers_to_predict = [int(code) for code in rivers_to_predict]

    # read the ieasyhydroforecast_NEW_STATIONS variable
    # this variables cantains the codes which should be hindcasted
    # this is only used when a new station is added to the config file and we only hindcast this station to save time
    NEW_STATIONS = os.getenv('ieasyhydroforecast_NEW_STATIONS')
    if NEW_STATIONS != 'None':
        new_stations = [int(code) for code in NEW_STATIONS.split(',')]
        rivers_to_predict = list(set(rivers_to_predict) & set(new_stations))

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

    if 'cluster' in static_features.columns:
        static_features = static_features.drop(columns=['cluster'])
    if 'log_q' in static_features.columns:
        static_features = static_features.drop(columns=['log_q'])
    if 'CODE' in static_features.columns:
        static_features = static_features.rename(columns={'CODE': 'code'})

    static_features.index = static_features['code']

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
        predictor = predictor_class(PATH_TO_MODEL)
    else:
        scalers = {
            'scaler_discharge': scaler_discharge,
            'scaler_covariates': scaler_era5,
            'scaler_static': scaler_static
        }
        predictor = predictor_class(
            model=model,
            scalers=scalers,
            static_features=static_features,
            dl_config_params=None,
            unique_id_col='code'
        )

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

    pred_date = pd.to_datetime(start_date)
    
    observed_discharge['date'] = pd.to_datetime(observed_discharge['date'])
    era5_data_transformed['date'] = pd.to_datetime(era5_data_transformed['date'])

    hindecast_df = pd.DataFrame()
    hindecast_daily_df = pd.DataFrame()

    current_year = pred_date.year
    logger.debug(f'Starting Hindcast for the dates: {start_date} to {end_date}')
    input_chunk_length = predictor.get_input_chunk_length()

    start_date_string = start_date
    end_date_string = end_date
    
    start_date = pd.to_datetime(start_date) - pd.DateOffset(days=input_chunk_length + 1)
    end_date = pd.to_datetime(end_date)

    logger.debug('Last Observation date: %s', observed_discharge['date'].max())

    # filter observed discharge to be only in the range of the start and end date
    observed_discharge = observed_discharge[observed_discharge['date'] >= start_date].copy()
    observed_discharge = observed_discharge[observed_discharge['date'] <= end_date].copy()

    logger.debug('Start date: %s', start_date)
    logger.debug('End date: %s', end_date)

    logger.debug('Observation discharge: %s', observed_discharge)

    
    timer_start = datetime.datetime.now()
    for code in rivers_to_predict:
    
        discharge = observed_discharge[observed_discharge['code'] == code].copy()
        
        #ensure that the discharge data has no missing dates in the range of the start and end date + forecast horizon
        # if it has missing dates, then fill the missing dates with nan
        # Create complete date range
        date_range = pd.date_range(start=start_date, end=end_date + pd.Timedelta(days=forecast_horizon), freq='D')

        #ensure that the discharge data has no missing dates in the range of the start and end date + forecast horizon
        # if it has missing dates, then fill the missing dates with nan
        date_range = pd.date_range(start=start_date, end=end_date + pd.Timedelta(days=forecast_horizon), freq='D')
        discharge.set_index('date', inplace=True)
        discharge = discharge.reindex(date_range)
        discharge.reset_index(inplace=True)
        discharge.rename(columns={'index': 'date'}, inplace=True)
        discharge['code'] = code

        era5 = era5_data_transformed[era5_data_transformed['code'] == code].copy()

        if era5.isnull().values.any():
            logger.debug(f'Era5 data for code {code} has missing values')
            logger.debug('Number of missing values: %s', era5.isnull().sum())
        
        if discharge.isnull().values.any():
            logger.debug(f'Discharge data for code {code} has missing values')
            logger.debug('Length of discharge: %s', len(discharge))
            logger.debug('Number of missing values: %s', discharge.isnull().sum())

        # make hindcast 
        hindcast_code = predictor.hindcast(
            df_rivers_org = discharge,
            df_covariates = era5,
            code = code,
            n = forecast_horizon,
        )

        hindcast_code['code'] = code

        logger.debug('Hindcast for code: %s', code)
        logger.debug('Hindcast: %s', hindcast_code)
        
        hindecast_daily_df = pd.concat([hindecast_daily_df, hindcast_code], axis=0)

        # Check if for this code we have a twin vitrual gauge which is > 0
        test_value = hydroposts_available_for_ml_forecasting.loc[hydroposts_available_for_ml_forecasting['code'] == str(code), 'virtual_station_name_twin'].iloc[0]
        if test_value is not False:
            logger.debug('Forecast for twin virtual gauge: %s', test_value)

            hindcast_code['code'] = int(test_value)
            hindecast_daily_df = pd.concat([hindecast_daily_df, hindcast_code], axis=0)

            logger.debug('Copied data and appended: %s', hindcast_code)
    
    timer_end = datetime.datetime.now()

    #Flagging:
    # if the forecast is nan -> flag = 3
    # if there is a value in the forecast: -> flag = 4
    hindecast_daily_df['flag'] = 4
    pred_cols = [col for col in hindecast_daily_df.columns if 'Q' in col]
    hindecast_daily_df.loc[hindecast_daily_df[pred_cols].isna().any(axis=1), 'flag'] = 3

    print(f'Time to make hindcast: {timer_end - timer_start}')
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

    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    
    #hindecast_df.to_csv(os.path.join(OUTPUT_PATH_DISCHARGE, f'{MODEL_TO_USE}_{HINDCAST_MODE}_hindcast_{start_date}_{end_date}.csv'), index=False)
    hindecast_daily_df.to_csv(os.path.join(OUTPUT_PATH_DISCHARGE, f'{MODEL_TO_USE}_{HINDCAST_MODE}_hindcast_daily_{start_date_string}_{end_date_string}.csv'), index=False)


    logger.info('Hindcast Done!')


if __name__ == '__main__':
    main()


