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
# - Run This Code with fake data
# - Implement the ARIMA Model
# - Set up real environment variables
# --------------------------------------------------------------------


# --------------------------------------------------------------------
# Load Libraries
# --------------------------------------------------------------------
import os
import sys
import glob
import pandas as pd
import numpy as np
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

import logging
from logging.handlers import TimedRotatingFileHandler
logging.getLogger("pytorch_lightning.utilities.rank_zero").setLevel(logging.WARNING)
logging.getLogger("pytorch_lightning.accelerators.cuda").setLevel(logging.WARNING)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler = TimedRotatingFileHandler('logs/log', when='midnight',
                                        interval=1, backupCount=30)
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger = logging.getLogger().setLevel(logging.DEBUG)
logger.handlers = []
logger.addHandler(file_handler)
logger.addHandler(console_handler)

import warnings
warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.DEBUG)

#Custom Libraries
from scr import utils_ml_forecast

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
    MODEL_TO_USE = os.getenv('MODEL_TO_USE')
    logger.debug('Model to use: ' + MODEL_TO_USE)

    if MODEL_TO_USE not in ['TFT', 'TIDE', 'TSMIXER']:
        raise ValueError('Model not supported')
    else:
        print('Model to use: ', MODEL_TO_USE)
        if MODEL_TO_USE == 'TFT':
            from scr import predictor_TFT as predictor_class
        elif MODEL_TO_USE == 'TIDE':
            from scr import predictor_TIDE as predictor_class
        elif MODEL_TO_USE == 'TSMIXER':
            from scr import predictor_TSMIXER as predictor_class


    # --------------------------------------------------------------------
    # INITIALIZE THE ENVIRONMENT
    # --------------------------------------------------------------------
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Load the environment variables
    sl.load_environment()

    exit()

    # Access the environment variables
    PATH_TO_STATIC_FEATURES = os.getenv('PATH_TO_STATIC_FEATURES')
    OUTPUT_PATH_DISCHARGE = os.getenv('OUTPUT_PATH_DISCHARGE')
    RIVERS_TO_PREDICT = os.getenv('RIVERS_TO_PREDICT')
    PATH_TO_QMAPPED_ERA5 = os.getenv('PATH_TO_QMAPPED_ERA5')
    HRU_ML_MODELS = os.getenv('HRU_ML_MODELS')

    PATH_TO_SCALER = os.getenv('PATH_TO_SCALER_' + MODEL_TO_USE)
    PATH_TO_MODEL= os.getenv('PATH_TO_' + MODEL_TO_USE)

    PATH_TO_STATIC_FEATURES = os.path.join(script_dir, PATH_TO_STATIC_FEATURES)
    OUTPUT_PATH_DISCHARGE = os.path.join(script_dir, OUTPUT_PATH_DISCHARGE)
    PATH_TO_QMAPPED_ERA5 = os.path.join(script_dir, PATH_TO_QMAPPED_ERA5)

    rivers_to_predict = [int(x) for x in RIVERS_TO_PREDICT.split(',')]


    # --------------------------------------------------------------------
    # LOAD DATA
    # --------------------------------------------------------------------
    #FAKE DATA SHOULD BE REPLACED WITH REAL DATA
    PATH_TO_PAST_DISCHARGE = os.getenv('PATH_TO_PAST_DISCHARGE')
    PATH_TO_PAST_DISCHARGE = os.path.join(script_dir, PATH_TO_PAST_DISCHARGE)


    past_discharge = pd.read_csv(PATH_TO_PAST_DISCHARGE, parse_dates=['date'])

    path_P = os.path.join(PATH_TO_QMAPPED_ERA5, HRU_ML_MODELS +'_P_control_member.csv')
    path_T = os.path.join(PATH_TO_QMAPPED_ERA5, HRU_ML_MODELS +'_T_control_member.csv')

    P_qmapped_era5 = pd.read_csv(path_P, parse_dates=['date'])
    T_qmapped_era5 = pd.read_csv(path_T, parse_dates=['date'])
    qmapped_era5 = pd.merge(P_qmapped_era5, T_qmapped_era5, on=['code', 'date'])
    static_features = pd.read_csv(PATH_TO_STATIC_FEATURES)

    static_features = static_features.drop(columns=['cluster', 'log_q'])
    static_features.index = static_features['CODE']
    #clear memory
    del P_qmapped_era5, T_qmapped_era5

    #get the codes to use
    codes_to_use = utils_ml_forecast.get_codes_to_use(past_discharge, qmapped_era5, static_features)

    # --------------------------------------------------------------------
    # Calculate PET Oudin and Daylight Hours
    # --------------------------------------------------------------------
    for code in codes_to_use:
        lat = static_features[static_features['CODE'] == code]['LAT'].values[0]
        lon = static_features[static_features['CODE'] == code]['LON'].values[0]
        pet_oudin = utils_ml_forecast.calculate_pet_oudin(qmapped_era5[qmapped_era5['code'] == code], lat)
        qmapped_era5.loc[qmapped_era5['code'] == code, 'PET'] = pet_oudin
        qmapped_era5.loc[qmapped_era5['code'] == code, 'daylight_hours'] = utils_ml_forecast.calculate_daylight_hours(lat, lon, qmapped_era5[qmapped_era5['code'] == code])

    # --------------------------------------------------------------------
    # LOAD SCALER
    # --------------------------------------------------------------------
    scaler_discharge = pd.read_csv(os.path.join(PATH_TO_SCALER, 'scaler_stats_discharge.csv'))
    scaler_discharge.index = scaler_discharge['Unnamed: 0'].astype(int)
    scaler_era5 = pd.read_csv(os.path.join(PATH_TO_SCALER, 'scaler_stats_era5.csv'))
    scaler_era5.index = scaler_era5['Unnamed: 0']
    scaler_static = pd.read_csv(os.path.join(PATH_TO_SCALER, 'scaler_stats_static.csv'))
    scaler_static.index = scaler_static['Unnamed: 0']

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

    predictor_pentad = predictor_class.PREDICTOR(model_pentad, scaler_discharge, scaler_era5, scaler_static, static_features)
    predictor_decad = predictor_class.PREDICTOR(model_decad, scaler_discharge, scaler_era5, scaler_static, static_features)

    # --------------------------------------------------------------------
    # FORECAST
    # --------------------------------------------------------------------
    forecast_pentad = pd.DataFrame()
    forecast_decad = pd.DataFrame()


    THRESHOLD_MISSING_DAYS = os.getenv('THRESHOLD_MISSING_DAYS_' + MODEL_TO_USE)
    CODES_RECURSIVE_IMPUTATION= os.getenv('CODES_RECURSIVE_IMPUTATION_' + MODEL_TO_USE)
    RECURSIVE_RIVERS = [int(x) for x in CODES_RECURSIVE_IMPUTATION.split(',')]
    THRESHOLD_MISSING_DAYS_END = os.getenv('THRESHOLD_MISSING_DAYS_END')

    #thresholds to ints
    THRESHOLD_MISSING_DAYS = int(THRESHOLD_MISSING_DAYS)
    THRESHOLD_MISSING_DAYS_END = int(THRESHOLD_MISSING_DAYS_END)


    used_decad_model_for_pentad_forecast = False
    decadal_forecast_is_possible = True
    pentad_no_success = []
    decadal_no_success = []
    missing_values_dict = {}
    exceeds_threshhold_dict = {}
    nans_at_end_dict = {}

    for code in rivers_to_predict:
        #get the data
        past_discharge_code = past_discharge[past_discharge['code'] == code]
        qmapped_era5_code = qmapped_era5[qmapped_era5['code'] == code]

        #sort by date
        past_discharge_code = past_discharge_code.sort_values(by='date')
        qmapped_era5_code = qmapped_era5_code.sort_values(by='date')

        #get the input chunck length -> this can than be used to determine the relevant allowed missing values
        input_chunk_length = predictor_pentad.get_input_chunk_length()

        #check for missing values, n = number of missing values at the end
        missing_values, nans_at_end = utils_ml_forecast.check_for_nans(past_discharge_code.iloc[-input_chunk_length:], THRESHOLD_MISSING_DAYS)

        if missing_values['exceeds_threshold'] or nans_at_end >= THRESHOLD_MISSING_DAYS_END:
            pentad_no_success.append(code)
            decadal_no_success.append(code)
            exceeds_threshhold_dict[code] = True
            predictions_pentad =  predictor_pentad.predict(past_discharge_code, qmapped_era5_code, None , code, n=6, make_plot=False)

        elif missing_values['nans_in_between']:
            missing_values_dict[code] = True
            past_discharge_code = utils_ml_forecast.gaps_imputation(past_discharge_code)

        elif missing_values['nans_at_end']:
            decadal_forecast_is_possible = False
            nans_at_end_dict[code] = nans_at_end
            decadal_no_success.append(code)

            if code in RECURSIVE_RIVERS:
                past_discharge_code = utils_ml_forecast.recursive_imputation(past_discharge_code, None, qmapped_era5_code, nans_at_end, predictor_pentad, make_plot=False)
            else:
                #use 10 days models to directly predict the pentad
                used_decad_model_for_pentad_forecast = True
                #predictions_tft_pentad = predictor_tft_decad.predict(past_discharge_code, qmapped_era5_code, None , code, n=11, make_plot=False)

        if not used_decad_model_for_pentad_forecast and code not in pentad_no_success:
            #pentad
            predictions_pentad = predictor_pentad.predict(past_discharge_code, qmapped_era5_code, None , code, n=6, make_plot=False)

        #decad
        if decadal_forecast_is_possible:
            #predictions_tft_decad = predictor_tft_decad.predict(past_discharge_code, qmapped_era5_code, None , code, n=11, make_plot=False)
            #predictions_tft_decad['code'] = code
            #forecast_decad_tft = pd.concat([forecast_decad_tft, predictions_tft_decad], axis=0)
            random_value = 1

        #add the code to the predictions
        predictions_pentad['code'] = code

        predictions_pentad['forecast_date'] = datetime.datetime.now().date()

        forecast_pentad = pd.concat([forecast_pentad, predictions_pentad], axis=0)




    utils_ml_forecast.write_output_txt(OUTPUT_PATH_DISCHARGE,
                                       pentad_no_success,
                                       decadal_no_success,
                                       missing_values_dict,
                                       exceeds_threshhold_dict,
                                       nans_at_end_dict)

    #read the latest forecast and append the new forecast
    try:
        forecast_pentad_old = pd.read_csv(os.path.join(OUTPUT_PATH_DISCHARGE, f'pentad_{MODEL_TO_USE}_forecast.csv'))

    except:
        forecast_pentad_old = pd.DataFrame()


    #check if we need to save the forecast for all 5 days -> no overwrite
    if utils_ml_forecast.save_pentad_forecast():
        try:
            forecast_pentad_old_intervall = pd.read_csv(os.path.join(OUTPUT_PATH_DISCHARGE, f'pentad_{MODEL_TO_USE}_forecast_pentad_intervall.csv'))
        except:
            forecast_pentad_old_intervall = pd.DataFrame()

        forecast_pentad_pentad_intervall = forecast_pentad
        forecast_pentad_pentad_intervall['prediction_date'] = datetime.datetime.now().date()

        forecast_pentad_pentad_intervall = pd.concat([forecast_pentad_old_intervall, forecast_pentad_pentad_intervall], axis=0)
        forecast_pentad_pentad_intervall = forecast_pentad_pentad_intervall.drop_duplicates(subset=['date', 'code'], keep='last')
        forecast_pentad_pentad_intervall.to_csv(os.path.join(OUTPUT_PATH_DISCHARGE, f'pentad_{MODEL_TO_USE}_forecast_pentad_intervall.csv'), index=False)



    forecast_pentad = pd.concat([forecast_pentad_old, forecast_pentad], axis=0)
    forecast_pentad = forecast_pentad.drop_duplicates(subset=['date', 'code'], keep='last')

    forecast_pentad.to_csv(os.path.join(OUTPUT_PATH_DISCHARGE, f'pentad_{MODEL_TO_USE}_forecast.csv'), index=False)
    forecast_decad.to_csv(os.path.join(OUTPUT_PATH_DISCHARGE, f'decadal_{MODEL_TO_USE}_forecast.csv'), index=False)



if __name__ == '__main__':
    main()