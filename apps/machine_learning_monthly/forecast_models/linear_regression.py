import os
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from typing import Dict, Any, List
import matplotlib.pyplot as plt

# Ensure the logs directory exists
import datetime

# Shared logging
import logging
from log_config import setup_logging
setup_logging()  

logger = logging.getLogger(__name__)  # Use __name__ to get module-specific logger


from forecast_models.base_class import BaseForecastModel
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.metrics import mean_squared_error, r2_score

from scr import FeatureExtractor as FE

class LinearRegressionModel(BaseForecastModel):
    """
    Linear Regression model for forecasting.
    """

    def __init__(self, 
                data: pd.DataFrame,
                static_data: pd.DataFrame,
                 general_config: Dict[str, Any], 
                 model_config: Dict[str, Any],
                 feature_config: Dict[str, Any],
                 path_config: Dict[str, Any]) -> None:
        """
        Initialize the Linear Regression model with a configuration dictionary.

        Args:
            general_config (Dict[str, Any]): General configuration for the model.
            model_config (Dict[str, Any]): Model-specific configuration.
            path_config (Dict[str, Any]): Path configuration for saving/loading data.
        """
        super().__init__(
            data=data,
            static_data=static_data,
            general_config=general_config,
            model_config=model_config,
            feature_config=feature_config,
            path_config=path_config,
        )

        logger.debug('Extracting features for Linear Regression model')
        self.__extract_features__()
        logger.debug('Features extracted successfully')
        
    def __extract_features__(self):
        """
        Extract features from the data.

        Args:
            data (pd.DataFrame): DataFrame containing the data.
        """
        # Implement feature extraction logic here
        extractor = FE.StreamflowFeatureExtractor(
            feature_configs=self.feature_config,
            prediction_horizon=self.general_config['prediction_horizon'],
            offset = self.general_config['offset'],
        )

        self.data = extractor.create_all_features(self.data)
        self.data['year'] = self.data['date'].dt.year
        self.data['month'] = self.data['date'].dt.month


    def get_highest_corr_features(self, 
                                  df_period: pd.DataFrame,
                                  target: str) -> List[str]:
        
        num_features = self.general_config['num_features']
        base_features = self.general_config['base_features']
        snow_vars = self.general_config['snow_vars']

        possible_features = []
        this_period = df_period['period'].unique()[0]

        for feat in base_features:
            possible_features += [col for col in df_period.columns if feat in col]
        for feat in snow_vars:
            possible_features += [col for col in df_period.columns if feat in col]

        corr = df_period[possible_features + [target]].corr()
        abs_corr_series = corr[target].abs()

        # Convert to a regular Python dictionary to avoid Series comparison issues
        abs_corr = {k: float(v) for k, v in abs_corr_series.items() if not pd.isna(v)}

        nan_test = possible_features.copy()
        nan_test.append(target)
        test_df = df_period.dropna(subset=nan_test, how='all').copy()
        len_data_points = len(test_df)

        if len_data_points < num_features * 2: # Need at least 2 data points per feature
            logger.warning(f"Not enough data points for {target} for the current period {this_period}.")


        feature_types = {}
        for feature in possible_features:
            if feature not in abs_corr: # Skip features with no correlation
                continue
            # Split by underscore to get variable type (like SWE, SCA, etc.)
            var_type = feature.split('_')[0]
            # If this is a new variable type or has higher correlation than previous best
            if var_type not in feature_types or abs_corr[feature] > abs_corr[feature_types[var_type]]:
                feature_types[var_type] = feature

        # Get the best features across all types (sorted by correlation)
        best_features = sorted(feature_types.values(), key=lambda x: abs_corr[x], reverse=True)
        # Select top features up to num_features
        features_week = best_features[:num_features]

        if len(features_week) < num_features:
            logger.warning(f"Warning: Only {len(features_week)} features found for {this_period}.")
            
        # Convert to Index to maintain compatibility with the rest of the code
        features_week = pd.Index(features_week)

        return features_week.to_list()
    

    def predict_on_sample(self, 
                        calibration_data: pd.DataFrame,
                        prediction_data: pd.DataFrame,
                        features: List[str],
                        target: str):
        """
        Predict on a sample of data.

        Args:
            calibration_data (pd.DataFrame): DataFrame containing the calibration data.
            prediction_data (pd.DataFrame): DataFrame containing the prediction data.
            features (List[str]): List of feature names.
            target (str): Target variable name.

        Returns:
            y_pred (np.ndarray): Predicted values.
            y_pred_calibration (np.ndarray): Predicted values for calibration data.
            r2_calibration (float): R-squared value for calibration data.
            model (sklearn.linear_model): Fitted model.
        """
        
        calibration_data = calibration_data.dropna(
            subset=features + [target], how='any').copy()
        
        prediction_data = prediction_data.dropna(
            subset=features, how='any').copy()
        if len(prediction_data) == 0:
            logger.warning(f"No data points for prediction.")
            return [np.nan], [np.nan], np.nan, None
        if len(calibration_data) < self.general_config['num_features'] * 2:
            logger.warning(f"Not enough data points for calibration.")
            return [np.nan], [np.nan], np.nan, None
        
        X_calibration = calibration_data[features]
        y_calibration = calibration_data[target]
        X_prediction = prediction_data[features]

        # Fit the model
        lr_type = self.model_config['lr_type']
        if lr_type == 'linear':
            model = LinearRegression()
        elif lr_type == 'ridge':
            model = Ridge(alpha=self.model_config['alpha'])
        elif lr_type == 'lasso':
            model = Lasso(alpha=self.model_config['alpha'])
        else:
            raise ValueError(f"Unknown model type: {lr_type}")
        
        model.fit(X_calibration, y_calibration)

        y_pred = model.predict(X_prediction)

        y_pred_calibration = model.predict(X_calibration)
        r2_calibration = r2_score(y_calibration, y_pred_calibration)
        return y_pred, y_pred_calibration, r2_calibration, model

    def predict_operational(self) -> pd.DataFrame:
        """
        Predict in operational mode.

        Args:
            data (pd.DataFrame): DataFrame containing the operational data.

        Returns:
            forecast (pd.DataFrame): DataFrame containing the forecasted values.
                columns: ['forecast_date', 'model_name', 'code','valid_from', 'valid_to', 'Q' (Optional: Q_05, Q_10, Q_50 ...)]
        """
        
        today = datetime.datetime.now()
        today_day = today.day
        today_month = today.month
        self.data['day'] = self.data['date'].dt.day
        period_name = f"{today_month}-{today_day}"
        self.data['period'] = self.data['month'].astype(str) + '-' + self.data['day'].astype(str)
        # Filter the data for the current period
        operational_data = self.data[self.data['period'] == period_name].copy()

        forecast = pd.DataFrame()
        if not self.general_config['offset']:
            self.general_config['offset'] = self.general_config['prediction_horizon']
        shift = self.general_config['offset'] - self.general_config['prediction_horizon'] 
        valid_from = today + datetime.timedelta(days=1) + datetime.timedelta(days=shift)
        valid_to = valid_from + datetime.timedelta(days=self.general_config['prediction_horizon'])

        valid_from = valid_from.strftime('%Y-%m-%d')
        valid_to = valid_to.strftime('%Y-%m-%d')

        logger.debug(f"Valid from: {valid_from}")
        logger.debug(f"Valid to: {valid_to}")

        codes_to_predict = operational_data['code'].unique()
        
        for code in codes_to_predict:
            
            # Filter the data for the current code
            code_data = operational_data[operational_data['code'] == code].copy()

            # Get the features for the current code
            features = self.get_highest_corr_features(code_data, 'target')
            logger.debug(f"Features for {code}: {features}")
            # Get current date (without time component)
            today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


            calibration_data = code_data[code_data['date'].dt.normalize() < today].copy()
            prediction_data = code_data[code_data['date'].dt.normalize() == today].copy()
            logger.debug(f"Prediction data for {code}: {prediction_data}")

            calibration_target_mean = calibration_data['target'].mean()
            if pd.isna(calibration_target_mean):
                calibration_target_mean = 0

            # Predict on the sample
            predictions, _ , r2_cali, model = self.predict_on_sample(
                calibration_data=calibration_data,
                prediction_data=prediction_data,
                features=features,
                target='target'
            )   
            if predictions[0] is not np.nan:
                predictions = np.clip(predictions, 0, None)  # Ensure predictions are non-negative
                predictions = np.round(predictions, 2)  # Round to 2 decimal places

                logger.debug(f"Predictions for {code}: {predictions[0]} m3/s")
                logger.debug(f"Prediction for {code} relative to long-term mean: {predictions[0] / calibration_target_mean}")
                logger.debug(f"R2 calibration for {code}: {r2_cali}")
                #construct the model and debug the equation
                model_equation = f"y = {model.intercept_:.3f} + " + " + ".join([f"{coef:.3f}*{feat}" for coef, feat in zip(model.coef_, features)])
                logger.debug(f"Model equation for {code}: {model_equation}")

            # Create a DataFrame for the predictions
            pred_df = pd.DataFrame({
                'forecast_date': [today],
                'model_name': [self.name],
                'code': [code],
                'valid_from': [valid_from],
                'valid_to': [valid_to],
                'Q': predictions,
            })

            forecast = pd.concat([forecast, pred_df], ignore_index=True)

        return forecast


    def calibrate_model_and_hindcast(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Calibrate the model using the provided data.

        Args:
            data (pd.DataFrame): DataFrame containing the calibration data.

        Returns:
            hindcast (pd.DataFrame): DataFrame containing the hindcasted values.
                columns: ['date', 'model', 'code', 'Q_pred' (Optional: Q_05, Q_10, Q_50 ...)]
        """
        # Implement the calibration and hindcasting logic here
        pass

    def tune_hyperparameters(self, data: pd.DataFrame) -> None:
        """
        Tune the hyperparameters of the model using the provided data.

        Args:
            data (pd.DataFrame): DataFrame containing the data for hyperparameter tuning.

        Returns:
            None
        """
        # Implement hyperparameter tuning logic here
        pass

    def save_model(self) -> None:
        """
        Save the model to a file.

        Returns:
            None
        """
        # Implement model saving logic here
        pass

    def load_model(self) -> None:
        """
        Load the model from a file.

        Returns:
            None
        """
        # Implement model loading logic here
        pass



    