# --------------------------------------------------------------------
# Load Libraries
# --------------------------------------------------------------------
import os
import glob
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import darts
from darts import TimeSeries, concatenate
from darts.utils.timeseries_generation import datetime_attribute_timeseries
import matplotlib.pyplot as plt
from pe_oudin.PE_Oudin import PE_Oudin
from suntime import Sun, SunTimeException

from darts.models import ARIMA

import datetime
import logging
import warnings
warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.WARNING)



# --------------------------------------------------------------------
# PREDICTOR CLASS
# --------------------------------------------------------------------
class PREDICTOR():
    def __init__(self, path_to_models, scaler):
        self.path_to_models = path_to_models
        self.scaler = scaler.copy()



    def scale_discharge(self, data: pd.DataFrame, scaler: pd.DataFrame, type: str ) -> pd.DataFrame:
        mask = scaler['feature'] == 'discharge'
        if type == 'standard':
            mean_ = scaler[mask]['mean'].values[0]
            std_ = scaler[mask]['std'].values[0]
            data_scaled = (data['discharge'] - mean_) / std
        
        elif type == 'minmax':
            min_ = scaler[mask]['min'].values[0]
            max_ = scaler[mask]['max'].values[0]
            data_scaled = (data['discharge'] - min_) / (max_ - min_)

        else:
            data_scaled = None
            print('ERROR IN SCALING')

        return data_scaled
    
   
    def get_input_chunk_length(self):
        return 20
    
    def get_max_forecast_horizon(self):
        return 10
    
    def scale_covariates(self, data: pd.DataFrame, scaler: pd.DataFrame, type: str, col: str) -> pd.DataFrame:
        mask = scaler['feature'] == col
        if type == 'standard':
            mean_ = scaler[mask]['mean'].values[0]
            std_ = scaler[mask]['std'].values[0]
            data_scaled = (data[col] - mean_) / std_
        
        if type == 'minmax':
            min_ = scaler[mask]['min'].values[0]
            max_ = scaler[mask]['max'].values[0]
            data_scaled = (data[col] - min_) / (max_ - min_)

        return data_scaled
     

    def rescale_predictions(self, predictions: np.array, code:int) -> np.array:
        #inverse scale
        scaler_discharge = self.scaler[self.scaler['code'] == code]
        min_discharge = scaler_discharge[scaler_discharge['feature'] == 'discharge']['min'].values[0]
        max_discharge = scaler_discharge[scaler_discharge['feature'] == 'discharge']['max'].values[0]

        prediction = predictions * (max_discharge - min_discharge) + min_discharge
        #reshape from 6, 1 to 6
        prediction = prediction.reshape(-1)
        return prediction
    
    def create_prediction_df(self, predictions: darts.TimeSeries, code: int) -> pd.DataFrame:

        dates = predictions.time_index

        preds_df = predictions.pd_dataframe()

        preds_df['date'] = dates

        preds_df['discharge'] = self.rescale_predictions(preds_df['discharge'].values, code)

        preds_df['code'] = code

        return preds_df
    
    def plot_predictions(self, df_predictions: pd.DataFrame, df_rivers: pd.DataFrame, code: int):
        input_length = self.get_input_chunk_length()
        plt.figure(figsize=(10,5))
        #plot df rivers to date of forecast
        plt.plot(df_rivers['date'].iloc[-input_length:], df_rivers['discharge'].iloc[-input_length:], label='Past Discharge', color='black')
        #plot the predictions
        plt.plot(df_predictions['date'], df_predictions['discharge'], label='Prediction', color='blue')
        plt.legend()
        plt.xlabel('Date')
        plt.ylabel('Discharge [m3/s]')
        plt.title('Discharge Prediction for Basin ' + str(code))
        
        plt.show()
        


    def predict(self, df_rivers_org: pd.DataFrame, df_era5: pd.DataFrame, df_swe:pd.DataFrame, code: int, n:int, make_plot: bool = False):
        #load the ARIMA model

        arima_model_path = os.path.join(self.path_to_models, f"ARIMA_{code}.pkl")
        try:
            model = ARIMA.load(arima_model_path)
        except:
            print(f"ARIMA model for basin {code} not found")
            return pd.DataFrame()
        

        #copy the dataframes
        df_rivers = df_rivers_org.copy()
        df_era5 = df_era5.copy()

        input_length = self.get_input_chunk_length()
        df_rivers = df_rivers.iloc[-input_length:].copy()
        min_obs_date = df_rivers['date'].min()
        df_era5 = df_era5[df_era5['date'] >= min_obs_date].copy()
  
        scaler_code = self.scaler[self.scaler['code'] == code].copy()    
        #scale the data
        df_rivers['discharge'] = self.scale_discharge(df_rivers,scaler_code , type='minmax')
       
        df_era5['T'] = self.scale_covariates(df_era5, scaler_code, type='minmax', col = 'T')



        #create the time series
        discharge = TimeSeries.from_dataframe(df_rivers, time_col='date', value_cols = 'discharge', freq='1D')

        #future covariates with month
        covariates_future = TimeSeries.from_dataframe(df_era5, time_col='date', value_cols = ['T'], freq='1D')

        #to np.float32
        discharge = discharge.astype(np.float32)
        covariates_future = covariates_future.astype(np.float32)

        #predict n steps
        predictions = model.predict(n = n,
                                    series = discharge,
                                    future_covariates=covariates_future,)

        df_predictions = self.create_prediction_df(predictions, code)

        if make_plot:
            self.plot_predictions(df_predictions, df_rivers_org, code)

        
        return df_predictions
    
