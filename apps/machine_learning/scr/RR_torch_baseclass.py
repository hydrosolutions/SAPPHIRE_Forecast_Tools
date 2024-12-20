from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
import json
import os
import torch
import datetime

class BasePredictor(ABC):
    def __init__(self, path_to_model):
        self.path_to_model = path_to_model
        self.path_weights = os.path.join(os.path.dirname(self.path_to_model), 'best_model.pt')
        self.path_to_dynamic_scaler = os.path.join(os.path.dirname(self.path_to_model), 'scaling_features.json')
        self.path_to_static_scaler = os.path.join(os.path.dirname(self.path_to_model), 'static_scaling_params.json')
        self.path_to_static_features = os.path.join(os.path.dirname(self.path_to_model), 'ML_static_attributes_CA.csv')
        self.path_to_config = os.path.join(os.path.dirname(self.path_to_model), 'config.json')

        # Load scalers and features
        self._load_scalers_and_features()
        
        # Load configuration
        self._load_config()
        
        # Load the model
        self.model = self.load_model()
        print('Model loaded with number of parameters: ', sum(p.numel() for p in self.model.parameters() if p.requires_grad))

    def _load_scalers_and_features(self):
        """Load scalers and static features from files"""
        with open(self.path_to_dynamic_scaler, 'r') as f:
            self.scaler = json.load(f)
        with open(self.path_to_static_scaler, 'r') as f:
            self.static_scaler = json.load(f)

        self.static_features = pd.read_csv(self.path_to_static_features)
        self.static_original = self.static_features.copy()
        self.num_static_features = len(self.static_scaler.keys())

    def _load_config(self):
        """Load configuration from config file"""
        self.config = json.load(open(self.path_to_config))
        self.input_chunk_length = self.config['input_length']
        self.forecast_length = self.config['output_length']
        self.features = self.config['features']
        self.hidden_size = self.config['hidden_size']
        self.dropout = self.config['dropout']

    @abstractmethod
    def load_model(self):
        """Load and return the specific model implementation"""
        pass

    def get_input_chunk_length(self):
        return self.input_chunk_length
    
    def get_max_forecast_horizon(self):
        return self.forecast_length

    def scale_dynamic_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Scale the dynamic features"""
        df_copy = df.copy()
        for feature in self.features:
            df_copy[feature] = (df_copy[feature] - self.scaler[feature][0]) / self.scaler[feature][1]
        return df_copy

    def scale_static_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Scale the static features using MINMAX"""
        df_copy = df.copy()
        for feature in self.static_scaler:
            min_val = self.static_scaler[feature][0]
            max_val = self.static_scaler[feature][1]
            df_copy[feature] = (df_copy[feature] - min_val) / (max_val - min_val)
        return df_copy

    def retransform_dischrge_m3(self, df: pd.DataFrame, code: int, col: str) -> pd.DataFrame:
        """Retransform the discharge values"""
        df_copy = df.copy()
        area_km2 = self.static_original[self.static_original['code'] == code]['area_km2'].values[0]
        df_copy[col] = df_copy[col] * area_km2 / 3.6 / 24
        return df_copy

    def assimilate_data(self, predictions: pd.DataFrame, df_rivers_org: pd.DataFrame, 
                       code: int, assim_factor: float = 0.75,
                       forecast_col: str = "Q", observed_col: str = "discharge",
                       forecast_date_col: str = "forecast_date") -> np.array:
        """Assimilate forecast data with observations"""
        df_rivers = df_rivers_org[df_rivers_org['code'] == code].copy()
        df_rivers = df_rivers.drop_duplicates(subset=['date'], keep='last')

        assimilated_forecast = []
        forecast_dates = predictions[forecast_date_col].unique()

        first_forecast = predictions[predictions[forecast_date_col] == forecast_dates[0]][forecast_col].values
        assimilated_forecast.append(first_forecast)

        for i in range(1, len(forecast_dates)):
            current_forecast_date = forecast_dates[i]
            previous_date = forecast_dates[i] - pd.DateOffset(days=1)
            previous_forecast_date = forecast_dates[i-1]

            previous_forecasts = predictions[predictions[forecast_date_col] == previous_forecast_date]
            current_forecast = predictions[predictions[forecast_date_col] == current_forecast_date][forecast_col].values
            previous_forecast = previous_forecasts[previous_forecasts['date'] == previous_date][forecast_col].values
            previous_observed = df_rivers[df_rivers['date'] == previous_date][observed_col].values
            
            if np.isnan(previous_forecast).any() or len(previous_observed) == 0:
                forecast_now_nan = np.zeros(len(current_forecast)) * np.nan
                assimilated_forecast.append(forecast_now_nan)
                continue

            if previous_date >= current_forecast_date:
                raise ValueError('Previous date is greater than current forecast date')
            
            delta_forecast = previous_observed - previous_forecast
            forecast_now = current_forecast + assim_factor * delta_forecast
            assimilated_forecast.append(forecast_now)

        return np.concatenate(assimilated_forecast)

    def predict(self, df_rivers_org: pd.DataFrame, df_era5: pd.DataFrame, df_swe: pd.DataFrame,
                code: int, n: int, make_plot: bool = False):
        """Make predictions using the model"""
        n = min(n, self.forecast_length)
        
        # Process input data
        df_era5 = self._process_era5_data(df_era5, code)
        static_features, static_features_cols = self._process_static_features(code)
        
        # Filter time
        df_era5 = self._filter_time_range(df_era5)
        
        # Create dataset and make predictions
        predictions = self._make_predictions(df_era5, static_features, static_features_cols, n, code)
        
        if len(predictions) > 0:
            predictions = self.retransform_dischrge_m3(predictions, code, 'Q')
            assimilated_forecast = self.assimilate_data(predictions, df_rivers_org, code)
            predictions['Q_ASSIM'] = assimilated_forecast
            
            today = pd.to_datetime(datetime.datetime.now().date())
            predictions = predictions[predictions['forecast_date'] == today].copy()
            
        return predictions

    def hindcast(self, df_rivers_org: pd.DataFrame, df_era5: pd.DataFrame, df_swe: pd.DataFrame,
                code: int, n: int, make_plot: bool = False):
        """Perform hindcasting using the model"""
        n = min(n, self.forecast_length)
        
        # Process input data
        df_era5 = self._process_era5_data(df_era5, code)
        static_features, static_features_cols = self._process_static_features(code)
        
        # Create dataset and make predictions
        hindcast_df = self._make_predictions(df_era5, static_features, static_features_cols, n, code)
        
        if len(hindcast_df) > 0:
            hindcast_df = self.retransform_dischrge_m3(hindcast_df, code, 'Q')
            assim_forecast = self.assimilate_data(hindcast_df, df_rivers_org, code)
            hindcast_df['Q_ASSIM'] = assim_forecast
            
        return hindcast_df

    def _process_era5_data(self, df_era5: pd.DataFrame, code: int) -> pd.DataFrame:
        """Process ERA5 data"""
        df_era5 = df_era5[['date', 'code'] + self.features].copy()
        df_era5 = df_era5[df_era5['code'] == code].copy()
        return self.scale_dynamic_features(df_era5)

    def _process_static_features(self, code: int):
        """Process static features"""
        static_features = self.static_features[self.static_features['code'] == code].copy()
        static_features_cols = [x for x in static_features if x not in ['lat', 'lon', 'code']]
        static_features = self.scale_static_features(static_features)
        return static_features, static_features_cols

    def _filter_time_range(self, df_era5: pd.DataFrame) -> pd.DataFrame:
        """Filter data for the appropriate time range"""
        today = pd.to_datetime(datetime.datetime.now().date())
        first_input_date = today - pd.DateOffset(days=self.input_chunk_length + 1)
        last_input_date = today + pd.DateOffset(days=9)
        return df_era5[(df_era5['date'] >= first_input_date) & (df_era5['date'] <= last_input_date)].copy()

    def _make_predictions(self, df_era5: pd.DataFrame, static_features: pd.DataFrame, 
                         static_features_cols: list, n: int, code: int) -> pd.DataFrame:
        """Make predictions using the processed data"""
        dataset = self._create_dataset(df_era5, static_features, static_features_cols)
        
        if len(dataset) == 0:
            return pd.DataFrame()

        self.model.eval()
        predictions = self._process_batches(dataset, n, code)
        return predictions

    @abstractmethod
    def _create_dataset(self, df_era5: pd.DataFrame, static_features: pd.DataFrame, 
                       static_features_cols: list):
        """Create the dataset for the specific model implementation"""
        pass

    @abstractmethod
    def _process_batches(self, dataset, n: int, code: int) -> pd.DataFrame:
        """Process batches for prediction - specific to model implementation"""
        pass