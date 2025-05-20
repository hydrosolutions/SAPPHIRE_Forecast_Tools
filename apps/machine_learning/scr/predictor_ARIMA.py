# --------------------------------------------------------------------
# Load Libraries
# --------------------------------------------------------------------
import os
import glob

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
    def __init__(self, path_to_models):
        self.path_to_models = path_to_models

        mean_daily_discharge_path = os.path.join(self.path_to_models, 'daily_mean_discharge.csv')
        self.mean_daily_discharge = pd.read_csv(mean_daily_discharge_path)
        self.mean_daily_discharge = self.mean_daily_discharge.rename(columns={'discharge': 'discharge_daily_mean'})

    def get_input_chunk_length(self):
        return 30
    
    def get_max_forecast_horizon(self):
        return 11
    
    def create_prediction_df(self, predictions: darts.TimeSeries, code: int) -> pd.DataFrame:

        dates = predictions.time_index

        preds_df = predictions.pd_dataframe()

        preds_df['date'] = dates

        preds_df['Q'] = np.round(preds_df['discharge'].values, 2)

        # drop the discharge column
        preds_df = preds_df.drop(columns=['discharge'])

        preds_df['code'] = code

        return preds_df
    
    def plot_predictions(self, df_predictions: pd.DataFrame, df_rivers: pd.DataFrame, code: int):
        input_length = self.get_input_chunk_length()
        plt.figure(figsize=(10,5))
        #plot df rivers to date of forecast
        plt.plot(df_rivers['date'].iloc[-input_length:], df_rivers['discharge'].iloc[-input_length:], label='Past Discharge', color='black')
        #plot the predictions
        plt.plot(df_predictions['date'], df_predictions['Q'], label='Prediction', color='blue')
        plt.legend()
        plt.xlabel('Date')
        plt.ylabel('Discharge [m3/s]')
        plt.title('Discharge Prediction for Basin ' + str(code))
        
        plt.show()
        

    def predict(self, df_rivers_org: pd.DataFrame, df_era5: pd.DataFrame, df_swe: pd.DataFrame, 
            code: int, n: int, make_plot: bool = False):
        """
        Modified predict function with robust day of year mapping
        """
        # Load the ARIMA model
        arima_model_path = os.path.join(self.path_to_models, f"ARIMA_{code}.pkl")
        try:
            model = ARIMA.load(arima_model_path)
        except:
            print(f"ARIMA model for basin {code} not found")
            return pd.DataFrame()

        # Copy the dataframes
        df_rivers = df_rivers_org.copy()
        df_era5 = df_era5.copy()

        # Ensure datetime format
        df_rivers['date'] = pd.to_datetime(df_rivers['date'])
        df_era5['date'] = pd.to_datetime(df_era5['date'])

        # Get the required input length and slice the data
        input_length = self.get_input_chunk_length()
        df_rivers = df_rivers.iloc[-input_length:].copy()
        min_obs_date = df_rivers['date'].min()
        df_era5 = df_era5[df_era5['date'] >= min_obs_date].copy()

        # Create a complete day of year lookup table
        days_lookup = pd.DataFrame({
            'day_of_year': range(1, 367),  # Include leap years
            'code': code
        })
        
        # Merge with mean_daily_discharge first to ensure complete coverage
        complete_daily_means = days_lookup.merge(
            self.mean_daily_discharge,
            on=['day_of_year', 'code'],
            how='left'
        )
        
        # Handle leap year (day 366) if needed
        if complete_daily_means['discharge_daily_mean'].isna().any():
            complete_daily_means.loc[
                complete_daily_means['day_of_year'] == 366, 
                'discharge_daily_mean'
            ] = complete_daily_means.loc[
                complete_daily_means['day_of_year'] == 365, 
                'discharge_daily_mean'
            ].iloc[0]
        
        # Process ERA5 data
        df_era5['day_of_year'] = df_era5['date'].dt.dayofyear
        
        # Merge ERA5 with the complete daily means
        df_era5 = df_era5.merge(
            complete_daily_means[['day_of_year', 'discharge_daily_mean']],
            on='day_of_year',
            how='left'
        )

        # Verify no missing values in key columns
        missing_values = df_era5[['P', 'T', 'discharge_daily_mean']].isna().sum()
        if missing_values.any():
            print(f"Warning: Missing values in ERA5 data after merge:\n{missing_values}")
            
        # Create time series
        discharge = TimeSeries.from_dataframe(
            df_rivers, 
            time_col='date', 
            value_cols='discharge', 
            freq='1D'
        )

        # Create future covariates
        exogene_features = ['P', 'T', 'discharge_daily_mean']
        covariates_future = TimeSeries.from_dataframe(
            df_era5, 
            time_col='date', 
            value_cols=exogene_features, 
            freq='1D'
        )

        # Convert to float32
        discharge = discharge.astype(np.float32)
        covariates_future = covariates_future.astype(np.float32)

        # Set random seed for reproducibility
        np.random.seed(42)
        
        # Make predictions
        try:
            predictions = model.predict(
                n=n,
                series=discharge,
                future_covariates=covariates_future,
            )
        except Exception as e:
            print(e)
            print("Error in predicting for code", code)
            return pd.DataFrame()

        # Create prediction DataFrame
        df_predictions = self.create_prediction_df(predictions, code)
        
        # Add validation columns
        df_predictions['day_of_year'] = pd.to_datetime(df_predictions['date']).dt.dayofyear

        if make_plot:
            self.plot_predictions(df_predictions, df_rivers_org, code)

        return df_predictions

        
    def hindcast(self, df_rivers_org: pd.DataFrame, df_era5: pd.DataFrame, df_swe: pd.DataFrame, 
                code: int, n: int, make_plot: bool = False):
        """
        Modified hindcast function with robust day of year merging
        """
        # Load the ARIMA model
        arima_model_path = os.path.join(self.path_to_models, f"ARIMA_{code}.pkl")
        try:
            model = ARIMA.load(arima_model_path)
        except:
            print(f"ARIMA model for basin {code} not found")
            return pd.DataFrame()

        # Copy the dataframes
        df_rivers = df_rivers_org.copy()
        df_era5 = df_era5.copy()

        # Create the time series using full data
        discharge = TimeSeries.from_dataframe(df_rivers, time_col='date', 
                                            value_cols='discharge', freq='1D')

        # Process era5 data with robust day of year merging
        # First, ensure we're working with datetime
        df_era5['date'] = pd.to_datetime(df_era5['date'])
        
        # Create a separate DataFrame for mean daily discharge lookup
        # This ensures we have a complete 365/366 day reference
        days_lookup = pd.DataFrame({
            'day_of_year': range(1, 367),  # Include leap years
            'code': code
        })
        
        # Merge with mean_daily_discharge first
        complete_daily_means = days_lookup.merge(
            self.mean_daily_discharge,
            on=['day_of_year', 'code'],
            how='left'
        )
        
        # Handle potential leap year missing values (day 366)
        if complete_daily_means['discharge_daily_mean'].isna().any():
            # Fill day 366 with day 365's value if missing
            complete_daily_means.loc[
                complete_daily_means['day_of_year'] == 366, 
                'discharge_daily_mean'
            ] = complete_daily_means.loc[
                complete_daily_means['day_of_year'] == 365, 
                'discharge_daily_mean'
            ].iloc[0]
        
        # Now process ERA5 data
        df_era5['day_of_year'] = df_era5['date'].dt.dayofyear
        
        # Merge ERA5 with the complete daily means
        df_era5 = df_era5.merge(
            complete_daily_means[['day_of_year', 'discharge_daily_mean']],
            on='day_of_year',
            how='left'
        )
        
        # Verify no missing values after merge
        missing_values = df_era5[['P', 'T', 'discharge_daily_mean']].isna().sum()
        if missing_values.any():
            print(f"Warning: Missing values after merge:\n{missing_values}")
        
        # Create future covariates TimeSeries
        exogene_features = ['P', 'T', 'discharge_daily_mean']  # Note: removed discharge_daily_mean as per your code
        covariates_future = TimeSeries.from_dataframe(
            df_era5, 
            time_col='date',
            value_cols=exogene_features,
            freq='1D'
        )

        # Convert to float32
        discharge = discharge.astype(np.float32)
        covariates_future = covariates_future.astype(np.float32)

        # Set random seed for reproducibility
        np.random.seed(42)
        
        # Perform historical forecasts
        try:
            hindcasts = model.historical_forecasts(
                series=discharge,
                future_covariates=covariates_future,
                forecast_horizon=n,
                stride=1,
                retrain=False,
                verbose=False,
                last_points_only=False
            )
        except Exception as e:
            print(e)
            print("Error in predicting for code", code)
            return pd.DataFrame()

        # Process results
        hindcast_df = pd.DataFrame()
        for hindcast in hindcasts:
            df_predictions = self.create_prediction_df(hindcast, code)
            min_date = df_predictions['date'].min()
            forecast_date = min_date - pd.DateOffset(days=1)
            df_predictions['forecast_date'] = forecast_date
            hindcast_df = pd.concat([hindcast_df, df_predictions])

        # Add validation columns
        hindcast_df['day_of_year'] = hindcast_df['date'].dt.dayofyear
        
        return hindcast_df