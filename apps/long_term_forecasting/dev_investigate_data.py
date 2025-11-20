##################################################
# Investigate Data for Long Term Forecasting
# Development Tool - No production use
##################################################

## How to run this script:
# Set the environment variable ieasyhydroforecast_env_file_path to point to your .env file
# Then run the script with:
# ieasyhydroforecast_env_file_path="../../../kyg_data_forecast_tools/config/.env_develop_kghm" lt_forecast_mode=monthly python dev_investigate_data.py


from datetime import datetime
import logging

# Suppress graphviz debug warnings BEFORE importing any modules that use graphviz
logging.getLogger("graphviz").setLevel(logging.WARNING)

import os
import sys
import time
import glob
import pandas as pd
import numpy as np
import json
from typing import List, Dict, Any, Tuple

# Import forecast models
from lt_forecasting.forecast_models.LINEAR_REGRESSION import LinearRegressionModel
from lt_forecasting.forecast_models.SciRegressor import SciRegressor
from lt_forecasting.forecast_models.deep_models.uncertainty_mixture import (
    UncertaintyMixtureModel,
)

from __init__ import logger 
from data_interface import DataInterface
from config_forecast import ForecastConfig


# set lt_forecasting logger level
logger_lt = logging.getLogger("lt_forecasting")
logger_lt.setLevel(logging.WARNING)

# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the setup_library module from the iEasyHydroForecast package
import setup_library as sl



def investigate():

    # Setup Environment
    sl.load_environment()

    # Now we setup the configurations
    forecast_config = ForecastConfig()

    forecast_mode = os.getenv('lt_forecast_mode')
    forecast_config.load_forecast_config(forecast_mode=forecast_mode)
    forcing_HRU = forecast_config.get_forcing_HRU()

    # Data Interface
    data_interface = DataInterface()
    base_data_dict = data_interface.get_base_data(forcing_HRU=forcing_HRU)

    temporal_data = base_data_dict["temporal_data"]
    static_data = base_data_dict["static_data"]
    offset_base = base_data_dict["offset_date_base"]
    offset_discharge = base_data_dict["offset_date_discharge"]

    code_invest = 16936
    df_invest = temporal_data[temporal_data['code'] == code_invest]

    print(f"Investigating data for code: {code_invest}")
    print(df_invest.tail(20))

    # investigate the last year: plot discharge, P and T
    import matplotlib.pyplot as plt
    if False:
        df_invest['date'] = pd.to_datetime(df_invest['date'])
        df_invest.set_index('date', inplace=True)   
        df_last_year = df_invest.last('365D')
        plt.figure(figsize=(12, 8))
        plt.subplot(3, 1, 1)
        plt.plot(df_last_year.index, df_last_year['discharge'], label='Discharge', color='blue')
        plt.ylabel('Discharge')
        plt.legend()
        plt.subplot(3, 1, 2)
        plt.plot(df_last_year.index, df_last_year['P'], label='Precipitation', color='green')
        plt.ylabel('Precipitation')
        plt.legend()
        plt.subplot(3, 1, 3)
        plt.plot(df_last_year.index, df_last_year['T'], label='Temperature', color='red')
        plt.ylabel('Temperature')
        plt.legend()
        plt.xlabel('Date')
        plt.tight_layout()
        plt.show()


    # lets investigate the forecasts for some codes:
    # read in the forecast files
    ordered_models = forecast_config.get_model_execution_order()
    execution_is_success = {}
    model_dependencies = forecast_config.get_model_dependencies()

    all_forecasts = None

    for model_name in ordered_models:
        print(f"Reading forecast for model: {model_name}")
        output_path = forecast_config.get_output_path(model_name=model_name)
        output_file = os.path.join(output_path, f"{model_name}_forecast.csv")
        if os.path.exists(output_file):
            df_forecast = pd.read_csv(output_file)
            if all_forecasts is None:
                all_forecasts = df_forecast
            else:
                Q_cols = [col for col in df_forecast.columns if 'Q_' in col]
                df_forecast = df_forecast[['forecast_date', 'code'] + Q_cols]
                all_forecasts = pd.merge(all_forecasts, df_forecast, on=['forecast_date', 'code'], suffixes=('', f'_{model_name}'))

    if all_forecasts is None:
        print("No forecasts found to investigate.")
        return
    
    codes_to_investigate = [16936, 16100, 15054, 16070, 15256]

    for code in codes_to_investigate:
        forecast_code = all_forecasts[all_forecasts['code'] == code].copy()
        forecast_code['forecast_date'] = pd.to_datetime(forecast_code['forecast_date'])
        temporal_data_code = temporal_data[temporal_data['code'] == code].copy()
        valid_from = forecast_code['valid_from'].iloc[0]
        valid_to = forecast_code['valid_to'].iloc[0]

        # get the day and month start and and day and month end
        valid_from_dates = pd.to_datetime(valid_from)
        valid_from_month_day = valid_from_dates.strftime('%m-%d')
        valid_to_dates = pd.to_datetime(valid_to)
        valid_to_month_day = valid_to_dates.strftime('%m-%d')

        # calcualte the long term mean for this range and code
        mask = (temporal_data_code['date'].dt.strftime('%m-%d') >= valid_from_month_day) & (temporal_data_code['date'].dt.strftime('%m-%d') <= valid_to_month_day)
        long_term_mean = temporal_data_code.loc[mask, 'discharge'].mean()
        long_term_q10 = temporal_data_code.loc[mask, 'discharge'].quantile(0.1)
        long_term_q90 = temporal_data_code.loc[mask, 'discharge'].quantile(0.9)
        long_term_std = temporal_data_code.loc[mask, 'discharge'].std()

        # plot the long term mean as a horizontal line and all the forecasts as dots
        plt.figure(figsize=(10, 6))
        x_values = [0, 1, 2, 3, 4]
        plt.axhline(y=long_term_mean, color='r', linestyle='--', label='Long Term Mean')
        # fill between q10 and q90
        plt.fill_between(x_values, long_term_q10, long_term_q90, color='gray', alpha=0.3, label='Long Term Q10-Q90')
        # fill between mean +/- std
        plt.fill_between(x_values, long_term_mean - long_term_std, long_term_mean + long_term_std, color='gray', alpha=0.5, label='Long Term Mean ±1 Std Dev')
        Q_cols = [col for col in forecast_code.columns if 'Q_' in col]
        for col in Q_cols:
            plt.scatter(2, forecast_code[col], label=col)
        plt.title(f'Forecasts for Code {code} with Long Term Mean')
        plt.xlabel('Date')
        plt.ylabel('Discharge Forecast')
        plt.legend()
        plt.show()

    



    






if __name__ == "__main__":
    investigate()