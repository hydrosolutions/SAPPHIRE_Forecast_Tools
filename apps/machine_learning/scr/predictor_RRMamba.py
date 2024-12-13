###
# IMPORTS
###

from typing import Dict, List, Optional, Sequence, Tuple, Union
import numpy as np
import pandas as pd
import torch
from torch import nn
import torch.nn.functional as F
from mambapy.mamba import Mamba, MambaConfig

import os
import glob
from dotenv import load_dotenv
import json

from pe_oudin.PE_Oudin import PE_Oudin
from suntime import Sun, SunTimeException



import datetime
import logging
import warnings
warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.WARNING)

#Import the custom torch helper functions
from scr import helper_torch as ht

###
# Pytorch Model
###
class RRMamba_Forecast(nn.Module):
    def __init__(
        self, 
        input_features,
        static_features,
        hidden_size = 16, 
        dropout=0.2, 
        forecast_length=1,
        n_layers = 2,
        ):
        super(RRMamba_Forecast, self).__init__()

        self.hidden_size = hidden_size
        self.input_features = input_features
        self.static_features = static_features
        self.forecast_length = forecast_length
        self.n_layers = n_layers

        self.dropout = nn.Dropout(dropout)
        self.activation = nn.SiLU()

        #  Linear Embedding
        self.embedding = nn.Linear(self.input_features, self.hidden_size)

        #  Static Embedding
        self.static_embedding = nn.Linear(self.static_features, self.hidden_size)

        # Linear Layer to combine the embeddings
        self.combined_embedding = nn.Sequential(
            nn.Linear(2*self.hidden_size, self.hidden_size),
            nn.SiLU(),
            nn.Dropout(dropout),
            nn.Linear(self.hidden_size, self.hidden_size)
        )

        #  Mamba
        self.mamba_config = MambaConfig(d_model=self.hidden_size, n_layers=self.n_layers)
        self.mamba = Mamba(self.mamba_config)

        #  Linear Output Layer
        self.output_layer = nn.Linear(self.hidden_size, 1)
        
    def forward(self, x, static_input):
        
        # Check for NaN values
        if torch.isnan(x).any():
            raise ValueError("Input contains NaN values")
            
        if torch.isnan(static_input).any():
            raise ValueError("Static input contains NaN values")
        
        # Input shape: (batch, time, features)
        batch_size, seq_length, _ = x.size()

        # 1. Linear Embedding (takes the z input)
        x = self.embedding(x)
        x = self.activation(x)


        # 2. Static Embedding
        static_emb = self.static_embedding(static_input)
        static_emb = self.dropout(static_emb)
        static_emb = self.activation(static_emb)
         # transform static_emb to the same shape as x, so from batch, hidden to batch, seq_length, hidden
        static_emb = static_emb.unsqueeze(1).expand(-1, seq_length, -1)

        # 3. Combine the embeddings
        x = torch.cat([x, static_emb], dim=-1)
        x = self.combined_embedding(x)
        x = self.dropout(x)
        x = self.activation(x)

        # 4. Mamba
        x = self.mamba(x)
        
        # 5. Linear Output Layer
        x = self.dropout(x)
        x = self.activation(x)
        x = self.output_layer(x)

        forecast = x[:, -self.forecast_length:]
        
        return {"output_discharge":forecast}  # Shape: (batch, time, 1)


# --------------------------------------------------------------------
# PREDICTOR CLASS
# --------------------------------------------------------------------
class PREDICTOR():
    def __init__(self, path_to_model):
        self.path_to_model = path_to_model
        self.path_weights = os.path.join(os.path.dirname(self.path_to_model), 'best_model.pt')
        self.path_to_dynamic_scaler = os.path.join(os.path.dirname(self.path_to_model), 'scaling_features.json')
        self.path_to_static_scaler = os.path.join(os.path.dirname(self.path_to_model), 'static_scaling_params.json')
        self.path_to_static_features = os.path.join(os.path.dirname(self.path_to_model), 'ML_static_attributes_CA.csv')
        self.path_to_config = os.path.join(os.path.dirname(self.path_to_model), 'config.json')


        #load the dynamic and static scalers
        with open(self.path_to_dynamic_scaler, 'r') as f:
            self.scaler = json.load(f)
        with open(self.path_to_static_scaler, 'r') as f:
            self.static_scaler = json.load(f)

        #load the static features
        self.static_features = pd.read_csv(self.path_to_static_features)
        self.static_original = self.static_features.copy()
        
        self.num_static_features = len(self.static_scaler.keys())

        self.config = json.load(open(self.path_to_config))

        self.input_chunk_length = self.config['input_length']
        self.forecast_length = self.config['output_length']
        self.features = self.config['features']
        self.hidden_size = self.config['hidden_size']
        self.dropout = self.config['dropout']

        # Load the model
        self.load_model()
        print('Model loaded with number of parameters: ', sum(p.numel() for p in self.model.parameters() if p.requires_grad))
        
    def load_model(self):
        # Load the model
        model = RRMamba_Forecast(
            input_features=len(self.features),
            static_features=self.num_static_features,
            hidden_size=self.hidden_size,
            dropout=self.dropout,
            forecast_length=self.forecast_length
        )

        pretrained_weights = torch.load(self.path_weights, map_location=torch.device('cpu'), weights_only=True)
        model_dict = model.state_dict()
        pretrained_dict = {k: v for k, v in pretrained_weights.items()}
        model_dict.update(pretrained_dict)
        model.load_state_dict(model_dict)
        
        self.model = model
        self.model.eval()

    def get_input_chunk_length(self):
        return self.input_chunk_length
    
    def get_max_forecast_horizon(self):
        return self.forecast_length
    
    def scale_dynamic_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Scale the dynamic features
        """
        for feature in self.features:
            df[feature] = (df[feature] - self.scaler[feature][0]) / self.scaler[feature][1]
        return df

    def scale_static_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Scale the static features MINMAX
        """
        for feature in self.static_scaler:
            min_val = self.static_scaler[feature][0]
            max_val = self.static_scaler[feature][1]
            df[feature] = (df[feature] - min_val) / (max_val - min_val)
        return df


    def predict(self, df_rivers_org: pd.DataFrame, df_era5: pd.DataFrame, df_swe: pd.DataFrame, 
            code: int, n: int, make_plot: bool = False):


        # Create prediction DataFrame
        df_predictions = self.create_prediction_df(predictions, code)
        


        return df_predictions

        
    def retransform_dischrge_m3(self, df: pd.DataFrame, code:int, col: str) -> pd.DataFrame:

        #acces the area_km2 from self.static_original
        area_km2 = self.static_original[self.static_original['code'] == code]['area_km2'].values[0]

        #retransform the discharge
        df[col] = df[col] * area_km2 / 3.6 / 24

        return df

    def assimilate_data(
        self, 
        predictions: pd.DataFrame, 
        df_rivers_org: pd.DataFrame, 
        code: int, assim_factor:float = 0.75,
        forecast_col: str = "Q", 
        observed_col: str = "discharge",
        forecast_date_col: str = "forecast_date") -> np.array:

        # Create a DataFrame with the assimilated data
        df_rivers = df_rivers_org.copy()
        df_rivers = df_rivers[df_rivers['code'] == code].copy()
        #remove duplicates on date
        df_rivers = df_rivers.drop_duplicates(subset=['date'], keep='last')

        assimilated_forecast = []
        forecast_dates = predictions[forecast_date_col].unique()

        first_forecast = predictions[predictions[forecast_date_col] == forecast_dates[0]][forecast_col].values
        assimilated_forecast.append(first_forecast)

        for i in range(1, len(forecast_dates)):
            
            # -- Get the current and previous forecast and observed values
            current_forecast_date = forecast_dates[i]
            previous_date = forecast_dates[i] - pd.DateOffset(days=1)
            previous_forecast_date = forecast_dates[i-1]

            # -- Get the current and previous forecast and observed values
            previous_forecasts = predictions[predictions[forecast_date_col] == previous_forecast_date]
            current_forecast = predictions[predictions[forecast_date_col] == current_forecast_date][forecast_col].values
            previous_forecast = previous_forecasts[previous_forecasts['date'] == previous_date][forecast_col].values
            previous_observed = df_rivers[df_rivers['date'] == previous_date][observed_col].values
            
            #if previous forecast is np.nan or 0 -> write nan values
            if np.isnan(previous_forecast).any() or len(previous_observed) == 0:
                forecast_now_nan = np.zeros(len(current_forecast)) * np.nan
                assimilated_forecast.append(forecast_now_nan)
                continue

            if previous_date >= current_forecast_date:
                raise ValueError('Previous date is greater than current forecast date')
            
            # -- Calculate the delta between the previous forecast and observed values
            delta_forecast = previous_observed - previous_forecast

            # -- Calculate the assimilated forecast
            forecast_now = current_forecast + assim_factor * delta_forecast

            # -- Append the assimilated forecast
            assimilated_forecast.append(forecast_now)

        # -- concatenate the assimilated forecast
        assimilated_forecast = np.concatenate(assimilated_forecast)

        # -- return the assimilated forecast
        return assimilated_forecast


    def predict(self, df_rivers_org: pd.DataFrame, df_era5: pd.DataFrame, df_swe: pd.DataFrame,
                code: int, n: int, make_plot: bool = False):

        # --------- Get max forecast horizon ---------
        n = min(n, self.forecast_length)

    
        # ----------------- ERA5 Processing -----------------
        df_era5 = df_era5[['date', 'code'] + self.features].copy()
        df_era5 = df_era5[df_era5['code'] == code].copy()
        #scale the features
        df_era5 = self.scale_dynamic_features(df_era5)

        # ----------------- Static Features -----------------
        static_features = self.static_features[self.static_features['code'] == code].copy()
        static_features_cols = [x for x in static_features if x not in [ 'lat', 'lon', 'code']]
        static_features = self.scale_static_features(static_features)

        # ----------------- Filter time -----------------
        today = pd.to_datetime(datetime.datetime.now().date())
        # the first input date is today - input_chunk_length - 1, we need to get t-1 for data assimilation
        first_input_date = today - pd.DateOffset(days=self.input_chunk_length + 1)
        last_input_date = today + pd.DateOffset(days=9)
        df_era5 = df_era5[df_era5['date'] >= first_input_date].copy()
        df_era5 = df_era5[df_era5['date'] <= last_input_date].copy()

        # ----------------- Create the dataset -----------------
        dataset = ht.HydroDataset( # Note that the dataset handles nan values
            dataframe = df_era5,
            static_data = static_features,
            input_length = self.input_chunk_length,
            output_length = self.forecast_length,
            features = self.features,
            target = None,
            static_features = static_features_cols)

        # ----------------- Predict -----------------
        if len(dataset) == 0:
            return pd.DataFrame()

        self.model.eval()
        all_forecasts = []
        all_dates = []
        all_codes = []
        all_forecast_dates = []
        for idx in range(len(dataset)):
            # ------ Get data -------
            data = dataset[idx]
            x = data['X']
            static_input = data['static']
            #add batch dimension
            x = x.unsqueeze(0)
            static_input = static_input.unsqueeze(0)

            # ------ Get dates -------
            dates_info = dataset.__getdate__(idx)
            dates = dates_info['dates']
            code = dates_info['code']

            # ------ Predict -------
            with torch.no_grad():
                output = self.model(x, static_input)
                #change output from batch,10, 1 to batch, 10
                output['output_discharge'] = output['output_discharge'].squeeze(-1)
                y_pred = output['output_discharge'].detach().cpu().numpy()
            

            # ------ Get true values -------
            forecast_date = dates[-self.forecast_length]
            dates_slice = dates[-self.forecast_length:]
            dates_slice = dates_slice[:n]
            codes_slice = [code]*n
            y_pred_flat = y_pred.reshape(-1)
            y_pred_flat = y_pred_flat[:n]
            forecast_dates_slice = [forecast_date]*n

            # Verify all lengths match
            assert len(dates_slice) == len(codes_slice) == len(y_pred_flat)  == len(forecast_dates_slice)         

            all_dates.append(dates_slice)
            all_codes.append(codes_slice)
            all_forecasts.append(y_pred_flat)
            all_forecast_dates.append(forecast_dates_slice)

        # Create DataFrame
        predictions = pd.DataFrame({
            'date': np.concatenate(all_dates),
            'code': np.concatenate(all_codes),
            'forecast_date': np.concatenate(all_forecast_dates),
            'Q': np.concatenate(all_forecasts),
        })



        # ----------------- Retransform discharge -----------------
        if len(predictions['forecast_date'].unique()) > 0:
            predictions = self.retransform_dischrge_m3(predictions, code, 'Q')

        # ---------------- Assimilate data -----------------
        assimilated_forecast = self.assimilate_data(predictions, df_rivers_org, code)
        predictions['Q_ASSIM'] = assimilated_forecast

        #only select forecast dates == today
        predictions = predictions[predictions['forecast_date'] == today].copy()

        return predictions


    def hindcast(self, df_rivers_org: pd.DataFrame, df_era5: pd.DataFrame, df_swe: pd.DataFrame, 
                code: int, n: int, make_plot: bool = False):

        # --------- Get max forecast horizon ---------
        n = min(n, self.forecast_length)

        # ----------------- ERA5 Processing -----------------
        df_era5 = df_era5[['date', 'code'] + self.features].copy()
        df_era5 = df_era5[df_era5['code'] == code].copy()
        #scale the features
        df_era5 = self.scale_dynamic_features(df_era5)

        # ----------------- Static Features -----------------
        static_features = self.static_features[self.static_features['code'] == code].copy()
        static_features_cols = [x for x in static_features if x not in [ 'lat', 'lon', 'code']]
        static_features = self.scale_static_features(static_features)

        # ----------------- Create the dataset -----------------
        dataset = ht.HydroDataset( #Note that the Dataset handles nan values
            dataframe = df_era5,
            static_data = static_features,
            input_length = self.input_chunk_length,
            output_length = self.forecast_length,
            features = self.features,
            target = None,
            static_features = static_features_cols)

        # ----------------- Predict -----------------
        self.model.eval()

        all_forecasts = []
        all_dates = []
        all_codes = []
        all_forecast_dates = []
        for idx in range(len(dataset)):
             
            # ------ Get data -------
            data = dataset[idx]
            x = data['X']
            static_input = data['static']
            #add batch dimension
            x = x.unsqueeze(0)
            static_input = static_input.unsqueeze(0)

            # ------ Get dates -------
            dates_info = dataset.__getdate__(idx)
            dates = dates_info['dates']
            code = dates_info['code']

            # ------ Predict -------
            with torch.no_grad():
                output = self.model(x, static_input)
                #change output from batch,10, 1 to batch, 10
                output['output_discharge'] = output['output_discharge'].squeeze(-1)
                y_pred = output['output_discharge'].detach().cpu().numpy()
            

            # ------ Get true values -------
            forecast_date = dates[-self.forecast_length]
            dates_slice = dates[-self.forecast_length:]
            dates_slice = dates_slice[:n]
            codes_slice = [code]*n
            y_pred_flat = y_pred.reshape(-1)
            y_pred_flat = y_pred_flat[:n]
            forecast_dates_slice = [forecast_date]*n

            # Verify all lengths match
            assert len(dates_slice) == len(codes_slice) == len(y_pred_flat)  == len(forecast_dates_slice)         

            all_dates.append(dates_slice)
            all_codes.append(codes_slice)
            all_forecasts.append(y_pred_flat)
            all_forecast_dates.append(forecast_dates_slice)

        # Create DataFrame
        hindcast_df = pd.DataFrame({
            'date': np.concatenate(all_dates),
            'code': np.concatenate(all_codes),
            'forecast_date': np.concatenate(all_forecast_dates),
            'Q': np.concatenate(all_forecasts),
        })

        # ----------------- Retransform discharge -----------------
        hindcast_df = self.retransform_dischrge_m3(hindcast_df, code, 'Q')


        # ----------------- Create a second Assimilated DataFrame -----------------
        assim_forecast = self.assimilate_data(hindcast_df, df_rivers_org, code)
        print('Assimilated forecast created', len(assim_forecast))
        hindcast_df['Q_ASSIM'] = assim_forecast

        return hindcast_df