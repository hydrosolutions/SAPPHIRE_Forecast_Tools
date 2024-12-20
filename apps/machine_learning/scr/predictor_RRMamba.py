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
from scr import RR_torch_baseclass as BASE

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
class PREDICTOR(BASE.BasePredictor):

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

        model.eval()
        return model

    def _create_dataset(self, df_era5: pd.DataFrame, static_features: pd.DataFrame, 
                       static_features_cols: list):
        """Create the Mamba-specific dataset"""
        return ht.HydroDataset(
            dataframe=df_era5,
            static_data=static_features,
            input_length=self.input_chunk_length,
            output_length=self.forecast_length,
            features=self.features,
            target=None,
            static_features=static_features_cols
        )

    def _process_batches(self, dataset, n: int, code: int) -> pd.DataFrame:
        """Process batches for Mamba model predictions"""
        all_forecasts = []
        all_dates = []
        all_codes = []
        all_forecast_dates = []

        for idx in range(len(dataset)):
            data = dataset[idx]
            x = data['X'].unsqueeze(0)
            static_input = data['static'].unsqueeze(0)

            dates_info = dataset.__getdate__(idx)
            dates = dates_info['dates']

            with torch.no_grad():
                output = self.model(x, static_input)
                output['output_discharge'] = output['output_discharge'].squeeze(-1)
                y_pred = output['output_discharge'].detach().cpu().numpy()

            forecast_date = dates[-self.forecast_length]
            dates_slice = dates[-self.forecast_length:][:n]
            codes_slice = [code] * n
            y_pred_flat = y_pred.reshape(-1)[:n]
            forecast_dates_slice = [forecast_date] * n

            all_dates.append(dates_slice)
            all_codes.append(codes_slice)
            all_forecasts.append(y_pred_flat)
            all_forecast_dates.append(forecast_dates_slice)

        return pd.DataFrame({
            'date': np.concatenate(all_dates),
            'code': np.concatenate(all_codes),
            'forecast_date': np.concatenate(all_forecast_dates),
            'Q': np.concatenate(all_forecasts),
        })
    

