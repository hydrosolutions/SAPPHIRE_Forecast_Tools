"""
Base class for Darts-based Deep Learning time series forecasting predictors.
This class inherits from BasePredictor and adds functionality specific to DL models.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Union

import darts
from darts import TimeSeries
from darts.utils.timeseries_generation import datetime_attribute_timeseries
from pytorch_lightning import Trainer

import logging
logging.getLogger("pytorch_lightning.utilities.rank_zero").setLevel(logging.WARNING)
logging.getLogger("pytorch_lightning.accelerators.cuda").setLevel(logging.WARNING)
logging.getLogger().setLevel(logging.WARNING)
import warnings
warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.WARNING)

from scr.BasePredictor import BasePredictor


class BaseDartsDLPredictor(BasePredictor):
    """
    Base class for Darts-based Deep Learning predictors.
    
    This class implements common functionality for deep learning models 
    implemented in the Darts library (TSMIXER, TIDE, TFT).
    """
    
    def __init__(self, 
                 model, 
                 scalers : Dict, 
                 static_features : pd.DataFrame, 
                 dl_config_params=None,
                 unique_id_col: str = 'code'):
        """
        Initialize the DL predictor with model and scaling parameters.
        
        Args:
            model: The Darts model instance (e.g., TFTModel, TiDEModel)
            scalers (dict): Dictionary containing scaler for the different features
                Dict[key] : pd.DataFrame
            dl_config_params (dict, optional): DL-specific configuration parameters
            unique_id_col (str, optional): Column name for unique identifiers
        """
        super().__init__(model_config={"model": model}, common_params=dl_config_params)
        
        self.model = model
        self.scaler_discharge = scalers.get('scaler_discharge')
        self.scaler_covariates = scalers.get('scaler_covariates')
        self.scaler_static = scalers.get('scaler_static')
        self.unique_id_col = unique_id_col

        self.static_features = static_features.copy()
        
        # Configuration parameters with defaults
        self.dl_config = dl_config_params or {}
        self.num_samples = self.dl_config.get("num_samples", 200)
        self.quantiles = self.dl_config.get("quantiles", 
                                           [0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 
                                            0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95])
        
        self.scaling_type = self.dl_config.get("scaling_type", "standard")
        self.scaling_type_covariates = self.dl_config.get("scaling_type_covariates", "minmax")
        self.scaling_type_static = self.dl_config.get("scaling_type_static", "minmax")
        self.exogene_covariates_cols = self.dl_config.get("exogene_covariates_cols", ["P", "T", "PET", "daylight_hours"])
        self.past_covariates_cols = self.dl_config.get("past_covariates_cols",
                                                 ["moving_avr_dis_3", "moving_avr_dis_5", "moving_avr_dis_10"])
        self.future_covariates_cols = self.dl_config.get("future_covariates_cols",
                                                    ["P", "T", "PET", "daylight_hours"])
        self.window_sizes = self.dl_config.get("window_sizes", [3, 5, 10])
        self.trainer_config = self.dl_config.get("trainer_config", 
                                               {"accelerator": "cpu", "logger": False})
        
        # Scale the static features
        self._scale_static_features()
    
    def _scale_static_features(self):
        """Scale the static features using the provided scaler."""
        for col in self.static_features.columns.values:  
            if col == self.unique_id_col: # Skip unique ID column
                continue
            self.static_features[col] = self.scale_covariates(
                self.static_features, self.scaler_static, 
                type=self.scaling_type_static, col=col
            )
    
    def get_input_chunk_length(self):
        """Get the required input length for the model."""
        return self.model.input_chunk_length
    
    def get_max_forecast_horizon(self):
        """Get the maximum forecast horizon for the model."""
        return self.model.output_chunk_length
    
    def scale_discharge(self, 
                        data : pd.DataFrame, 
                        scaler : Union[pd.DataFrame, pd.Series], 
                        type: str ='standard'
                        ) -> pd.Series:
        """
        Scale discharge data using the provided scaler.
        
        Args:
            data (pd.DataFrame): Discharge data
            scaler (pd.DataFrame or pd.Series): Scaler parameters
            type (str): Scaling type ('standard' or 'minmax')
            
        Returns:
            pd.Series: Scaled discharge values
        """
        if type == 'standard':
            data_scaled = (data['discharge'] - scaler['mean']) / scaler['std']
        elif type == 'minmax':
            data_scaled = (data['discharge'] - scaler['min']) / (scaler['max'] - scaler['min'])
        else:
            raise ValueError(f"Unknown scaling type: {type}")
            
        return data_scaled
    
    def scale_covariates(self, 
                         data : pd.DataFrame, 
                         scaler : Union[pd.DataFrame, pd.Series], 
                         type : str, 
                         col : str
                         ) -> pd.Series:
        """
        Scale covariate data using the provided scaler.
        
        Args:
            data (pd.DataFrame): Covariate data
            scaler (pd.DataFrame): Scaler parameters
            type (str): Scaling type ('standard' or 'minmax')
            col (str): Column name to scale
            
        Returns:
            pd.Series: Scaled covariate values
        """
        if type == 'standard':
            data_scaled = (data[col] - scaler.loc[col]['mean']) / scaler.loc[col]['std']
        elif type == 'minmax':
            data_scaled = (data[col] - scaler.loc[col]['min']) / (scaler.loc[col]['max'] - scaler.loc[col]['min'])
        else:
            raise ValueError(f"Unknown scaling type: {type}")
            
        return data_scaled
    
    def calc_rolling_mean(self, 
                          df_rivers : pd.DataFrame, 
                          window : int
                          ) -> np.array:
        """
        Calculate rolling mean of discharge with specified window.
        
        Args:
            df_rivers (pd.DataFrame): River discharge data
            window (int): Window size for rolling mean
            
        Returns:
            np.array: Rolling mean values
        """
        dates = pd.to_datetime(df_rivers['date'].values)
        moving_average_discharge = df_rivers['discharge'].rolling(window=window).mean()
        moving_average_discharge.index = dates
        
        # Fill NaN values by taking the next value which is not NaN
        moving_average_discharge = moving_average_discharge.bfill()
        
        return moving_average_discharge.values
    
    def add_month(self, 
                  ts : TimeSeries
                  ) -> TimeSeries:
        """
        Add month information to the time series.
        
        Args:
            ts (darts.TimeSeries): Input time series
            
        Returns:
            darts.TimeSeries: Time series with month information added
        """
        month = datetime_attribute_timeseries(ts, attribute="month", one_hot=False) / 12
        ts = ts.stack(month)
        return ts
    
    def rescale_predictions(self, 
                            predictions : np.array, 
                            code: int
                            ) -> np.array:
        """
        Rescale the predictions back to the original scale.
        
        Args:
            predictions (np.array): Scaled predictions
            code (int): Basin code identifier
            
        Returns:
            np.array: Rescaled predictions
        """
        # Inverse scale
        mean = self.scaler_discharge.loc[code, 'mean']
        std = self.scaler_discharge.loc[code, 'std']
        prediction = predictions * std + mean
        
        # Reshape from (n, 1) to (n,)
        prediction = prediction.reshape(-1)
        
        # Round predictions to 2 decimal places
        prediction = np.round(prediction, 2)
        
        # Clip negative values to 0
        prediction = np.clip(prediction, 0, None)
        
        return prediction
    
    def create_prediction_df(self, 
                             predictions : TimeSeries, 
                             code : int
                             ) -> pd.DataFrame:
        """
        Create a DataFrame from model predictions.
        
        Args:
            predictions (darts.TimeSeries): Model predictions
            code (int): Basin code identifier
            
        Returns:
            pd.DataFrame: Formatted DataFrame containing predictions with quantiles
        """
        dates = predictions.time_index
        df_predictions = pd.DataFrame()
        
        for q in self.quantiles:
            quantile_pred = predictions.quantile_timeseries(q)
            quantile_pred = self.rescale_predictions(quantile_pred.values(), code)
            Q = int(q * 100)
            df_predictions[f'Q{Q}'] = quantile_pred
        
        df_predictions['date'] = dates
        
        return df_predictions
    
    
    def _prepare_data(self, 
                      df_rivers_org : pd.DataFrame, 
                      df_covariates : pd.DataFrame, 
                      code: int
                      ) -> tuple:
        """
        Prepare and scale data for model input.
        
        Args:
            df_rivers_org (pd.DataFrame): Original river discharge DataFrame
            df_covariates (pd.DataFrame): Covariates data DataFrame
            code (int): Basin code identifier
            
        Returns:
            tuple: Tuple containing processed data:
                (df_rivers, df_covariates, df_covariates_past)
        """
        # Copy the dataframes to avoid modifying the originals
        df_rivers = df_rivers_org.copy()
        df_covariates = df_covariates.copy()

        
        # Scale the discharge data
        df_rivers['discharge'] = self.scale_discharge(
            df_rivers, self.scaler_discharge.loc[code], type=self.scaling_type
        )
        
        # Scale the Exogeneous covariates
        for col in self.exogene_covariates_cols:
            df_covariates[col] = self.scale_covariates(
                df_covariates, self.scaler_covariates, type=self.scaling_type_covariates, col=col
            )
        
        # Create past covariates dataframe
        df_covariates_past = pd.DataFrame()
        df_covariates_past['date'] = df_rivers['date'].values.copy()
        
        # Calculate rolling mean discharge for different window sizes
        for window in self.window_sizes:
            df_covariates_past[f'moving_avr_dis_{window}'] = self.calc_rolling_mean(
                df_rivers, window=window
            )
        
        
        return df_rivers, df_covariates, df_covariates_past
    
    def _create_time_series(self, 
                            df_rivers : pd.DataFrame, 
                            df_covariates : pd.DataFrame, 
                            df_covariates_past : pd.DataFrame, 
                            code : int
                            ) -> tuple:
        """
        Create Darts TimeSeries objects from the processed data.
        
        Args:
            df_rivers (pd.DataFrame): Processed river discharge DataFrame
            df_covariates (pd.DataFrame): Processed covariates DataFrame
            df_covariates_past (pd.DataFrame): Past covariates DataFrame
            code (int): Basin code identifier
            
        Returns:
            tuple: Tuple containing TimeSeries objects:
                (discharge, covariates_past, covariates_future)
        """
        # Create discharge TimeSeries
        discharge = TimeSeries.from_dataframe(
            df_rivers, time_col='date', value_cols='discharge', freq='1D'
        )
        
        # Add static features
        discharge = discharge.with_static_covariates(
            self.static_features.drop(columns=[self.unique_id_col]).loc[code]
        )
        
        # Create past covariates TimeSeries
        covariates_past = TimeSeries.from_dataframe(
            df_covariates_past, time_col='date', value_cols=self.past_covariates_cols, freq='1D'
        )
        
        # Create future covariates TimeSeries
        covariates_future = TimeSeries.from_dataframe(
            df_covariates, time_col='date', value_cols=self.future_covariates_cols, freq='1D'
        )
        
        # Add month information if needed
        if getattr(self.model, 'add_encoders', None) is None:
            covariates_future = self.add_month(covariates_future)
        
        # Convert to np.float32
        discharge = discharge.astype(np.float32)
        covariates_past = covariates_past.astype(np.float32)
        covariates_future = covariates_future.astype(np.float32)
        
        return discharge, covariates_past, covariates_future
    
    def predict(self, 
                df_rivers_org : pd.DataFrame, 
                df_covariates : pd.DataFrame, 
                code : int, 
                n : int
                ) -> pd.DataFrame:
        """
        Generate predictions for a specified time horizon.
        
        Args:
            df_rivers_org (pd.DataFrame): Original river discharge DataFrame
            df_covariates (pd.DataFrame): Covariates data DataFrame
            code (int): Basin code identifier
            n (int): Number of time steps to predict
            
        Returns:
            pd.DataFrame: DataFrame containing the predictions
        """
        try:
            # Prepare data
            df_rivers, df_covariates, df_covariates_past = self._prepare_data(
                df_rivers_org = df_rivers_org, 
                df_covariates = df_covariates,
                code = code
            )
            
            # Create time series
            discharge, covariates_past, covariates_future = self._create_time_series(
                df_rivers = df_rivers,
                df_covariates = df_covariates,
                df_covariates_past = df_covariates_past,
                code = code
            )
            
            # Create trainer
            trainer = Trainer(**self.trainer_config)
            
            # Make predictions
            predictions = self.model.predict(
                n=n,
                series=discharge,
                past_covariates=covariates_past,
                future_covariates=covariates_future,
                num_samples=self.num_samples,
                verbose=False,
                trainer=trainer
            )
            
            # Create prediction DataFrame
            df_predictions = self.create_prediction_df(predictions, code)
            
            return df_predictions
            
        except Exception as e:
            print(e)
            print(f"Error in predicting for code {code}")
            return pd.DataFrame()
    
    def hindcast(self, 
                 df_rivers_org : pd.DataFrame, 
                 df_covariates : pd.DataFrame,
                 code : int, 
                 n : int
                 ) -> pd.DataFrame:
        """
        Generate historical forecasts (hindcasts) for evaluation.
        
        Args:
            df_rivers_org (pd.DataFrame): Original river discharge DataFrame
            df_covariates (pd.DataFrame): Covariates data DataFrame
            code (int): Basin code identifier
            n (int): Forecast horizon for hindcasts
            
        Returns:
            pd.DataFrame: DataFrame containing the hindcast predictions
        """
        try:
            # Prepare data
            df_rivers, df_covariates, df_covariates_past = self._prepare_data(
                df_rivers_org = df_rivers_org, 
                df_covariates = df_covariates,
                code = code
            )
            
            # Create time series
            discharge, covariates_past, covariates_future = self._create_time_series(
                df_rivers = df_rivers,
                df_covariates = df_covariates,
                df_covariates_past = df_covariates_past,
                code = code
            )
            
            # Create trainer and prediction kwargs
            predict_kwargs = {
                'trainer': Trainer(**self.trainer_config),
            }
            
            # Generate hindcasts
            hindcasts = self.model.historical_forecasts(
                series=discharge,
                past_covariates=covariates_past,
                future_covariates=covariates_future,
                num_samples=self.num_samples,
                forecast_horizon=n,
                stride=1,
                retrain=False,  # Important: don't retrain the model
                verbose=False,
                last_points_only=False,
                predict_kwargs=predict_kwargs
            )
            
            # Process the hindcasts
            hindcast_df = pd.DataFrame()
            for hindcast in hindcasts:
                df_predictions = self.create_prediction_df(hindcast, code)
                min_date = df_predictions['date'].min()
                forecast_date = min_date - pd.DateOffset(days=1)
                df_predictions['forecast_date'] = forecast_date
                hindcast_df = pd.concat([hindcast_df, df_predictions])

            return hindcast_df
            
        except Exception as e:
            print(e)
            print(f"Error in hindcasting for code {code}")
            return pd.DataFrame()