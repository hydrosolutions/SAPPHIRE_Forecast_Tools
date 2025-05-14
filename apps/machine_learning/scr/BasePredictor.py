"""
Base abstract class for time series forecasting predictors.
This class serves as the foundation for all predictor models.
"""

from abc import ABC, abstractmethod
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import darts
from darts import TimeSeries
from typing import List, Dict, Tuple

class BasePredictor(ABC):
    """
    Abstract base class for all forecasting predictors.
    
    This class defines the common interface and functionality shared across
    all prediction models. Specific model implementations will inherit from
    this class and implement the required abstract methods.
    """
    
    def __init__(self,
                 model_config : Dict,
                 common_params: Dict = None
                 ):
        """
        Initialize the base predictor with common configuration parameters.
        
        Args:
            model_config (dict): Model-specific configuration such as paths or settings
            common_params (dict, optional): Common parameters used across all predictors
        """
        self.model_config = model_config
        self.common_params = common_params or {}
    
    @abstractmethod
    def get_input_chunk_length(self):
        """
        Get the required input length for the model.
        
        Returns:
            int: The input chunk length required by the model
        """
        pass
    
    @abstractmethod
    def get_max_forecast_horizon(self):
        """
        Get the maximum forecast horizon for the model.
        
        Returns:
            int: The maximum number of time steps the model can predict
        """
        pass
    
    
    @abstractmethod
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
        pass
    
    @abstractmethod
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
            df_era5 (pd.DataFrame): Covariates data DataFrame
            code (int): Basin code identifier
            n (int): Forecast horizon for hindcasts
            
        Returns:
            pd.DataFrame: DataFrame containing the hindcast predictions
        """
        pass