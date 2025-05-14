import os
import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, Any



class BaseForecastModel(ABC):
    """
    Abstract base class for forecast models.
    """

    def __init__(self, 
                 data: pd.DataFrame,
                 static_data: pd.DataFrame,
                 general_config: Dict[str, Any], 
                 model_config: Dict[str, Any],
                 feature_config: Dict[str, Any],
                 path_config: Dict[str, Any]) -> None:
        """
        Initialize the forecast model with a configuration dictionary.

        Args:
            config (Dict[str, Any]): Configuration dictionary for the model.
        """
        self.data = data
        self.static_data = static_data
        self.general_config = general_config
        self.model_config = model_config
        self.feature_config = feature_config
        self.path_config = path_config
        self.name = general_config['model_name']


    @abstractmethod
    def predict_operational(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Predict in operational mode.

        Args:
            data (pd.DataFrame): DataFrame containing the operational data.

        returns:
            forecast (pd.DataFrame): DataFrame containing the forecasted values.
                columns: ['date', 'model', 'code', 'Q_pred' (Optional: Q_05, Q_10, Q_50 ...)]
        """
        pass

    @abstractmethod
    def calibrate_model_and_hindcast(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Calibrate the model using the provided data.

        Args:
            data (pd.DataFrame): DataFrame containing the calibration data.

        Returns:
            hindcast (pd.DataFrame): DataFrame containing the hindcasted values.
                columns: ['date', 'model', 'code', 'Q_pred' (Optional: Q_05, Q_10, Q_50 ...)]
        """
        pass

    @abstractmethod
    def tune_hyperparameters(self, data: pd.DataFrame) -> None:
        """
        Tune the hyperparameters of the model using the provided data.

        Args:
            data (pd.DataFrame): DataFrame containing the data for hyperparameter tuning.

        Returns:
            None
        """
        pass

    @abstractmethod
    def save_model(self) -> None:
        """
        Save the model to a file.

        Returns:
            None
        """
        pass

    @abstractmethod
    def load_model(self) -> None:
        """
        Load the model from a file.

        Returns:
            None
        """
        pass