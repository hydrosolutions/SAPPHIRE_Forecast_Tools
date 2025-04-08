# --------------------------------------------------------------------
# MONTHLY FORECASTING WITH MACHINE LEARNING MODELS
# --------------------------------------------------------------------

# Useage:
# SAPPHIRE_OPDEV_ENV=True SAPPHIRE_MONTHLY_MODELS=LR SAPPHIRE_MONTHLY_HORIZON=1  python make_forecast_monthly.py
#
# SAPPHIRE_MONTHLY_MODELS: Can be a list (defined in the env file) or a single str value
# Some possible values: LR, LR_SWE, LR_SWE_SLOPE, LR_SWE_500m ..., 
# LGBM, XGB, CatBoost, LGBM_with_LR, ...
# SAPPHIRE_MONTHLY_HORIZON: 1, 2, 3, 4, 5, 6, 
# This variables defines which month should be forecasted

# --------------------------------------------------------------------
# Load Libraries
# --------------------------------------------------------------------
import os
import sys
import glob
import pandas as pd
import numpy as np
import json

import matplotlib.pyplot as plt
import datetime

# Shared logging
import logging
from log_config import setup_logging
setup_logging()  

logger = logging.getLogger(__name__)  # Use __name__ to get module-specific logger

# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the setup_library module from the iEasyHydroForecast package
import setup_library as sl


# Custom Libraries
from monthly_base_config import PATH_CONFIG, GENERAL_CONFIG, MODEL_CONFIG, FEATURE_CONFIG
from forecast_models import LINEAR_REGRESSION
from scr import data_loading


def make_forecast_monthly():
    logger.info(f'--------------------------------------------------------------------')
    logger.info(f"Starting make_forecast.py")

    # --------------------------------------------------------------------
    # DEFINE WHICH MODEL TO USE
    # --------------------------------------------------------------------
    MODEL_TO_USE = os.getenv('SAPPHIRE_MONTHLY_MODELS')
    logger.debug('Model to use: %s', MODEL_TO_USE)

    HORIZON = os.getenv('SAPPHIRE_MONTHLY_HORIZON')
    HORIZON = int(HORIZON)
    logger.debug('Horizon to use: %s', HORIZON)

    # Load the environment variables
    sl.load_environment()

    AVAILABLE_MODELS = os.getenv('ieasyhydroforecast_MONTHLY_MODELS_AVAILABLE')
    AVAILABLE_MODELS = [model.strip() for model in AVAILABLE_MODELS.split(',')]

    if MODEL_TO_USE not in AVAILABLE_MODELS:
        raise ValueError(f"Model {MODEL_TO_USE} is not available. Available models are: {AVAILABLE_MODELS}")
    
    configuration_path = os.getenv('ieasyforecast_configuration_path')
    # Access the environment variables
    intermediate_data_path = os.getenv('ieasyforecast_intermediate_data_path')
    
    # ----------------------------------------------------------
    # Load the models specific configuration
    # ----------------------------------------------------------
    PATH_TO_MONTHLY_MODELS = os.getenv('ieasyhydroforecast_ml_monthly_path_to_models')
    PATH_TO_MONTHLY_MODELS = os.path.join(configuration_path, PATH_TO_MONTHLY_MODELS)

    PATH_TO_MODEL = os.path.join(PATH_TO_MONTHLY_MODELS, MODEL_TO_USE)
    PATH_TO_MODEL = os.path.join(PATH_TO_MODEL, f"HORIZON_{HORIZON}")

    # check if the folder exists
    if not os.path.exists(PATH_TO_MODEL):
        raise ValueError(f"Model folder {PATH_TO_MODEL} does not exist. Please check the model name and horizon.")

    # Update the FEATURE_CONFIG with the model specific configuration
    feature_config_path = os.path.join(PATH_TO_MODEL, 'feature_config.json')
    if os.path.exists(feature_config_path):
        with open(feature_config_path, 'r') as f:
            feature_config = json.load(f)
            FEATURE_CONFIG.update(feature_config)
            logger.debug('Loaded the feature config')

    # Update the GENERAL_CONFIG with the model specific configuration
    general_config_path = os.path.join(PATH_TO_MODEL, 'general_config.json')
    if os.path.exists(general_config_path):
        with open(general_config_path, 'r') as f:
            general_config = json.load(f)
            GENERAL_CONFIG.update(general_config)
            logger.debug('Loaded the general config')

    # Update the MODEL_CONFIG with the model specific configuration
    model_config_path = os.path.join(PATH_TO_MODEL, 'model_config.json')
    if os.path.exists(model_config_path):
        with open(model_config_path, 'r') as f:
            model_config = json.load(f)
            MODEL_CONFIG.update(model_config)
            logger.debug('Loaded the model config')


    # ----------------------------------------------------------------
    # PATHS
    # ----------------------------------------------------------------
    PATH_TO_STATIC_FEATURES = os.getenv('ieasyhydroforecast_PATH_TO_STATIC_FEATURES')
    # Path to the output directory
    OUTPUT_PATH_DISCHARGE = os.getenv('ieasyhydroforecast_OUTPUT_PATH_MONTHLY_FORECAST')
    # Downscaled weather data
    PATH_TO_QMAPPED_ERA5 = os.getenv('ieasyhydroforecast_PATH_TO_QMAPPED_ERA5')
    PATH_ERA5_REANALYSIS = os.getenv('ieasyhydroforecast_OUTPUT_PATH_REANALYSIS')
    # PATH TO SNOW DATA
    PATH_TO_SNOW_DATA = os.getenv('ieasyhydroforecast_OUTPUT_PATH_SNOW')
    
    PATH_TO_PAST_DISCHARGE = os.getenv('ieasyforecast_daily_discharge_file')
    PATH_TO_PAST_DISCHARGE = os.path.join(intermediate_data_path, PATH_TO_PAST_DISCHARGE)
    
    HRU_ML_MODELS = os.getenv('ieasyhydroforecast_HRU_MONTHLY_FORCING')

    PATH_TO_STATIC_FEATURES = os.path.join(PATH_TO_MONTHLY_MODELS, PATH_TO_STATIC_FEATURES)
    
    OUTPUT_PATH_DISCHARGE = os.path.join(intermediate_data_path, OUTPUT_PATH_DISCHARGE)
    #Extend the OUTPUT_PATH_DISCHARGE with the model name
    OUTPUT_PATH_DISCHARGE = os.path.join(OUTPUT_PATH_DISCHARGE, MODEL_TO_USE)

    PATH_TO_QMAPPED_ERA5 = os.path.join(intermediate_data_path, PATH_TO_QMAPPED_ERA5)
    PATH_ERA5_REANALYSIS = os.path.join(intermediate_data_path, PATH_ERA5_REANALYSIS)
    
    path_P_operational = os.path.join(PATH_TO_QMAPPED_ERA5, HRU_ML_MODELS +'_P_control_member.csv')
    path_T_operational = os.path.join(PATH_TO_QMAPPED_ERA5, HRU_ML_MODELS +'_T_control_member.csv')

    path_P_reanalysis = os.path.join(PATH_ERA5_REANALYSIS, HRU_ML_MODELS +'_P_reanalysis.csv')
    path_T_reanalysis = os.path.join(PATH_ERA5_REANALYSIS, HRU_ML_MODELS +'_T_reanalysis.csv')
    
    PATH_TO_SNOW_DATA = os.path.join(intermediate_data_path, PATH_TO_SNOW_DATA)
    # -----------------------------------------------------------
    # DATA LOADING
    # -----------------------------------------------------------
    hydro_df, static_features = data_loading.load_data(
        path_discharge=PATH_TO_PAST_DISCHARGE,
        path_to_P_operational=path_P_operational,
        path_to_T_operational=path_T_operational,
        path_to_P_reanalysis=path_P_reanalysis,
        path_to_T_reanalysis=path_T_reanalysis,
        path_static_features=PATH_TO_STATIC_FEATURES,
        path_snow_data=PATH_TO_SNOW_DATA
    )

    logger.debug('Loaded the data')
    logger.debug('Head of the data: %s', hydro_df.head())
    logger.debug('Tail of the data: %s', hydro_df.tail())
    logger.debug('Head of the static features: %s', static_features.head())


    # -----------------------------------------------------------
    # Predictions
    # -----------------------------------------------------------

    # get the basins we want to predict 
    # TODO: Replace with Configuration
    codes_to_predict = static_features['code'].unique()
    codes_to_predict = [16936, 16100, 15102]
    logger.debug('Codes to predict: %s', codes_to_predict)

    hydro_df = hydro_df[hydro_df['code'].isin(codes_to_predict)]

    model_first = MODEL_TO_USE.split('_')[0]
    if model_first == 'LR':
        model_class = LINEAR_REGRESSION.LinearRegressionModel(
            data =hydro_df,
            static_data = static_features,
            general_config=GENERAL_CONFIG,
            model_config=MODEL_CONFIG,
            feature_config=FEATURE_CONFIG,
            path_config=PATH_CONFIG
        )
    else:
        raise ValueError(f"Model {MODEL_TO_USE} is not implemented yet.")


    forecast_df = model_class.predict_operational()

    logger.debug('Head of the forecast: %s', forecast_df.head())

    sys.exit(0)
    
    fig, ax = plt.subplots(len(codes_to_predict), 1, figsize=(10, 6))
    if len(codes_to_predict) == 1:
        ax = [ax]  # Convert to list when only one subplot is created
    else:
        ax = ax.flatten()
    

    for i, code in enumerate(forecast_df['code'].unique()):
        code_df = forecast_df[forecast_df['code'] == code]
        
        # Extract dates and convert to datetime
        valid_from = pd.to_datetime(code_df['valid_from'].values[0])  # Get first value
        valid_to = pd.to_datetime(code_df['valid_to'].values[0])      # Get first value
        Q = code_df['Q'].values[0]  # Get the forecast value

        logger.debug(f'Code: {code}, Valid from: {valid_from}, Valid to: {valid_to}, Q: {Q}')
        
        # Plot the horizontal forecast line using proper date formatting
        ax[i].hlines(y=Q, xmin=valid_from, xmax=valid_to, colors='red', label='Forecast', linewidth=2)
        
        # Plot past discharge data
        past_discharge_code = hydro_df[hydro_df['code'] == code]
        start_of_year = datetime.datetime.now().year
        past_discharge_code['year'] = past_discharge_code['date'].dt.year
        past_discharge_code = past_discharge_code[past_discharge_code['year'] == start_of_year]
        ax[i].plot(past_discharge_code['date'], past_discharge_code['discharge'], label='Past Discharge', color='black')
        
        # Set plot formatting
        ax[i].set_title(f'Code: {code}')
        ax[i].set_xlabel('Date')
        ax[i].set_ylabel('Discharge (m3/s)')
        ax[i].legend()


    # Adjust layout to prevent overlapping
    plt.tight_layout()
    plt.show()
        

if __name__ == "__main__":
    make_forecast_monthly()