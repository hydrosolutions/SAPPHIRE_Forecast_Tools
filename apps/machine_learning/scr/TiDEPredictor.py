"""
TiDE (Time-based Decomposition Encoder) Predictor implementation.
This class inherits from BaseDartsDLPredictor to leverage common DL functionality.
"""

import pandas as pd
import numpy as np
from typing import Dict, Union, Optional

from darts import TimeSeries
from darts.models import TiDEModel
from pytorch_lightning import Trainer

from scr.BaseDartsDLPredictor import BaseDartsDLPredictor


class TiDEPredictor(BaseDartsDLPredictor):
    """
    TiDE predictor class that implements the Time-based Decomposition Encoder model 
    for time series forecasting from Darts.
    """
    
    def __init__(self, 
                 model: TiDEModel, 
                 scalers: Dict, 
                 static_features: pd.DataFrame, 
                 dl_config_params: Optional[Dict] = None,
                 unique_id_col: str = 'code'):
        """
        Initialize the TiDE predictor with model and scaling parameters.
        
        Args:
            model (TiDEModel): The TiDE model instance
            scalers (dict): Dictionary containing scaler for the different features
            static_features (pd.DataFrame): Static features for the basins
            dl_config_params (dict, optional): DL-specific configuration parameters
            unique_id_col (str, optional): Column name for unique identifiers
        """
        super().__init__(model=model,
                         scalers=scalers,
                         static_features=static_features,
                         dl_config_params=dl_config_params,
                         unique_id_col=unique_id_col)
        
        # TiDE-specific configurations can be added here
    