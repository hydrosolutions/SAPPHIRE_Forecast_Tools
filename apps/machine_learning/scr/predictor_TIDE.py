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



# --------------------------------------------------------------------
# PREDICTOR CLASS
# --------------------------------------------------------------------
class PREDICTOR():
    def __init__(self, model, scaler_discharge, scaler_era5, scaler_static, static_features):
        self.model = model
        self.scaler_discharge = scaler_discharge
        self.scaler_era5 = scaler_era5
        self.scaler_static = scaler_static
        self.static_features = static_features.copy()

        #scale the static features
        for col in self.static_features.columns.values[1:]:
            self.static_features[col] = self.scale_covariates(self.static_features, self.scaler_static, type='minmax', col = col)

    def get_max_forecast_horizon(self):
        return self.model.output_chunk_length
    
    def get_input_chunk_length(self):
        return self.model.input_chunk_length

    def scale_discharge(self, data: pd.DataFrame, scaler: pd.DataFrame, type: str ) -> pd.DataFrame:

        if type == 'standard':
            data_scaled = (data['discharge'] - scaler['mean']) / scaler['std']
        
        elif type == 'minmax':
            data_scaled = (data['discharge'] - scaler['min']) / (scaler['max'] - scaler['min'])

        else:
            data_scaled = None
            print('ERROR IN SCALING')

        return data_scaled
    
    def scale_covariates(self, data: pd.DataFrame, scaler: pd.DataFrame, type: str, col: str) -> pd.DataFrame:

        if type == 'standard':
            data_scaled = (data[col] - scaler.loc[col]['mean']) / scaler.loc[col]['std']
        
        if type == 'minmax':
            data_scaled = (data[col] - scaler.loc[col]['min']) / (scaler.loc[col]['max'] - scaler.loc[col]['min'])

        return data_scaled
    
    def calc_rolling_mean(self, df_rivers: pd.DataFrame, df_era5 : pd.DataFrame, window: int) -> pd.DataFrame:
        dates = df_rivers['date'].values
        dates = pd.to_datetime(dates)
        moving_average_discharge = df_rivers['discharge'].rolling(window=window).mean()
        moving_average_discharge.index = dates

        #fill the nan values by taking the next value wich is not nan
        moving_average_discharge = moving_average_discharge.bfill()
        #check if the moving average has the same length as the era5 data
        

        return moving_average_discharge.values    
    
    def calc_residuals(self,df_rivers :pd.DataFrame, df_covariates_past: pd.DataFrame) -> pd.DataFrame:
        #select the dates of df rivers
        dates = df_rivers['date'].values

        residuals = df_rivers['discharge'].values - df_covariates_past['moving_average_discharge'].values

        return residuals

    def add_month(self, ts):
        month =  datetime_attribute_timeseries(ts, attribute="month", one_hot=False) /12
        ts = ts.stack(month)
        return ts
    

    def rescale_predictions(self, predictions: np.array, code:int) -> np.array:
        #inverse scale
        mean = self.scaler_discharge.loc[code, 'mean']
        std = self.scaler_discharge.loc[code, 'std']
        prediction = predictions * std + mean
        #reshape from 6, 1 to 6
        prediction = prediction.reshape(-1)
        # round predictions to 2 decimal places
        prediction = np.round(prediction, 2)
        # clip negative values to 0
        prediction = np.clip(prediction, 0, None)
        return prediction
    
    def create_prediction_df(self, predictions: darts.TimeSeries, code: int) -> pd.DataFrame:

        quantiles = [0.05,0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9,  0.95]

        dates = predictions.time_index

        df_predictions = pd.DataFrame()

        for q in quantiles:
            quantile_pred = predictions.quantile_timeseries(q)
            quantile_pred = self.rescale_predictions(quantile_pred.values(), code)
            Q = int(q*100)
            df_predictions['Q' + str(Q)] = quantile_pred

        df_predictions['date'] = dates

        return df_predictions
    
    def plot_predictions(self, df_predictions: pd.DataFrame, df_rivers: pd.DataFrame, code: int):
        input_length = self.get_input_chunk_length()
        plt.figure(figsize=(10,5))
        #plot df rivers to date of forecast
        plt.plot(df_rivers['date'].iloc[-input_length:], df_rivers['discharge'].iloc[-input_length:], label='Past Discharge', color='black')
        #plot the predictions
        plt.plot(df_predictions['date'], df_predictions['Q50'], label='Prediction Median', color='blue')
        plt.fill_between(df_predictions['date'], df_predictions['Q25'], df_predictions['Q75'], color='blue', alpha=0.25, label='50% CI', linewidth=1)
        plt.fill_between(df_predictions['date'], df_predictions['Q1'], df_predictions['Q9'], color='blue', alpha=0.2, label='80% CI', linewidth=1)
        plt.fill_between(df_predictions['date'], df_predictions['Q05'], df_predictions['Q95'], color='blue', alpha=0.15, label='90% CI', linewidth=1)
        plt.legend()
        plt.xlabel('Date')
        plt.ylabel('Discharge [m3/s]')
        plt.title('Discharge Prediction for Basin ' + str(code))
        
        plt.show()
        


    def predict(self, df_rivers_org: pd.DataFrame, df_era5: pd.DataFrame, df_swe:pd.DataFrame, code: int, n:int, make_plot: bool = False):
        #copy the dataframes
        df_rivers = df_rivers_org.copy()
        df_era5 = df_era5.copy()
        if df_swe is not None:
            df_swe = df_swe.copy()
        
        #scale the data
        df_rivers['discharge'] = self.scale_discharge(df_rivers, self.scaler_discharge.loc[code], type='standard')
        for col in ['P', 'T', 'PET', 'daylight_hours']:
            df_era5[col] = self.scale_covariates(df_era5, self.scaler_era5, type='minmax', col = col)

        
        if df_swe is not None:
            df_swe['SWE'] = self.scale_covariates(df_swe, self.scaler_era5, type='minmax', col = 'SWE')

        df_covariates_past = pd.DataFrame()
        df_covariates_past['date'] = df_rivers['date'].values.copy()
  
        #moving average discharge
        df_covariates_past['moving_avr_dis_10'] = self.calc_rolling_mean(df_rivers, df_era5, window=10)
        df_covariates_past['moving_avr_dis_5'] = self.calc_rolling_mean(df_rivers, df_era5, window=5)
        df_covariates_past['moving_avr_dis_3'] = self.calc_rolling_mean(df_rivers, df_era5, window=3)

        #swe 
        #reindex swe with df_covariates_past dates
        if df_swe is not None: 
            df_swe = df_swe.set_index('date')
            df_swe = df_swe.reindex(df_covariates_past['date'])
            df_swe['SWE'] = df_swe['SWE'].shift(periods = 6).bfill()
            df_covariates_past['SWE'] = df_swe['SWE'].values

        #create the time series
        # It is really important for the TiDE Model, that the features are added in the same order as in the training 
        # past covariates: SWE, moving_average_discharge, residuals
        # future covariates: P, T, PET, month
        discharge = TimeSeries.from_dataframe(df_rivers, time_col='date', value_cols = 'discharge', freq='1D')
        #add static_features to the time series
        discharge = discharge.with_static_covariates(self.static_features.drop(columns=['CODE']).loc[code])

        #past covariates 
        if df_swe is not None:
            covariates_past = TimeSeries.from_dataframe(df_covariates_past, time_col='date', value_cols = [ 'SWE','moving_avr_dis_5','moving_avr_dis_10'], freq='1D')
        else:
            covariates_past = TimeSeries.from_dataframe(df_covariates_past, time_col='date', value_cols = ['moving_avr_dis_3','moving_avr_dis_5','moving_avr_dis_10'], freq='1D')

        #future covariates with month
        covariates_future = TimeSeries.from_dataframe(df_era5, time_col='date', value_cols = ['P', 'T', 'PET', 'daylight_hours'], freq='1D')
        #covariates_future = self.add_month(covariates_future)

        #to np.float32
        discharge = discharge.astype(np.float32)
        covariates_past = covariates_past.astype(np.float32)
        covariates_future = covariates_future.astype(np.float32)


        #predict n steps
        try:
            predictions = self.model.predict(n = n,
                                         series = discharge,
                                         past_covariates = covariates_past,
                                         future_covariates=covariates_future,
                                         num_samples=200,
                                         verbose=False,
                                         trainer = Trainer(accelerator='cpu',
                                                           logger=False,))
        except Exception as e:
            print(e)
            print("Error in predicting for code", code)
            return pd.DataFrame()

        df_predictions = self.create_prediction_df(predictions, code)

        if make_plot:
            self.plot_predictions(df_predictions, df_rivers_org, code)

        
        
        return df_predictions
    


    def hindcast(self, df_rivers_org: pd.DataFrame, df_era5: pd.DataFrame, df_swe:pd.DataFrame, code: int, n:int, make_plot: bool = False):
        #copy the dataframes
        df_rivers = df_rivers_org.copy()
        df_era5 = df_era5.copy()
        if df_swe is not None:
            df_swe = df_swe.copy()
        
        #scale the data
        df_rivers['discharge'] = self.scale_discharge(df_rivers, self.scaler_discharge.loc[code], type='standard')
        for col in ['P', 'T', 'PET', 'daylight_hours']:
            df_era5[col] = self.scale_covariates(df_era5, self.scaler_era5, type='minmax', col = col)

        
        if df_swe is not None:
            df_swe['SWE'] = self.scale_covariates(df_swe, self.scaler_era5, type='minmax', col = 'SWE')

        df_covariates_past = pd.DataFrame()
        df_covariates_past['date'] = df_rivers['date'].values.copy()
  
        #moving average discharge
        df_covariates_past['moving_avr_dis_10'] = self.calc_rolling_mean(df_rivers, df_era5, window=10)
        df_covariates_past['moving_avr_dis_5'] = self.calc_rolling_mean(df_rivers, df_era5, window=5)
        df_covariates_past['moving_avr_dis_3'] = self.calc_rolling_mean(df_rivers, df_era5, window=3)

        #swe 
        #reindex swe with df_covariates_past dates
        if df_swe is not None: 
            df_swe = df_swe.set_index('date')
            df_swe = df_swe.reindex(df_covariates_past['date'])
            df_swe['SWE'] = df_swe['SWE'].shift(periods = 6).bfill()
            df_covariates_past['SWE'] = df_swe['SWE'].values

        #create the time series
        # It is really important for the TiDE Model, that the features are added in the same order as in the training 
        # past covariates: SWE, moving_average_discharge, residuals
        # future covariates: P, T, PET, month

        discharge = TimeSeries.from_dataframe(df_rivers, time_col='date', value_cols = 'discharge', freq='1D')
        #add static_features to the time series
        discharge = discharge.with_static_covariates(self.static_features.drop(columns=['CODE']).loc[code])

        #past covariates 
        if df_swe is not None:
            covariates_past = TimeSeries.from_dataframe(df_covariates_past, time_col='date', value_cols = [ 'SWE','moving_avr_dis_5','moving_avr_dis_10'], freq='1D')
        else:
            covariates_past = TimeSeries.from_dataframe(df_covariates_past, time_col='date', value_cols = ['moving_avr_dis_3','moving_avr_dis_5','moving_avr_dis_10'], freq='1D')

        #future covariates with month
        covariates_future = TimeSeries.from_dataframe(df_era5, time_col='date', value_cols = ['P', 'T', 'PET', 'daylight_hours'], freq='1D')
        #covariates_future = self.add_month(covariates_future)

        #to np.float32
        discharge = discharge.astype(np.float32)
        covariates_past = covariates_past.astype(np.float32)
        covariates_future = covariates_future.astype(np.float32)

        predict_kwargs = {
            'trainer': Trainer(accelerator='cpu', logger=False,),
        }


        # hindcast the entire series
        try:
            hindcasts = self.model.historical_forecasts(
                series=discharge,
                past_covariates=covariates_past,
                future_covariates=covariates_future,
                num_samples=200,
                forecast_horizon=n,
                stride=1,
                retrain=False, # this is important, otherwise the model will be retrained
                verbose=False,
                last_points_only=False,
                predict_kwargs=predict_kwargs)
        except Exception as e:
            print(e)
            print("Error in predicting for code", code)
            return pd.DataFrame()

        # Returns 
        # List[List[TimeSeries]] 
        # – A list of lists of historical forecasts for a sequence of series and last_points_only=False.
        #  For each series, and historical forecast, it contains the entire horizon forecast_horizon. 
        # The outer list is over the series provided in the input sequence, and the inner lists contain the historical forecasts for each series.

        hindcast_df = pd.DataFrame()
        for hindcast in hindcasts:

            df_predictions = self.create_prediction_df(hindcast, code)

            min_date = df_predictions['date'].min()

            forecast_date = min_date - pd.DateOffset(days=1)

            df_predictions['forecast_date'] = forecast_date

            hindcast_df = pd.concat([hindcast_df, df_predictions])

        
        return hindcast_df