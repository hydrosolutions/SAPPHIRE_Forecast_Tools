import pandas as pd
import numpy as np
import scipy.stats


# Calculate slope using linear regression on each rolling window
def rolling_slope(x):
    if len(x) < 2:  # Need at least 2 points for slope
        return np.nan
    x_values = np.arange(len(x))
    return np.polyfit(x_values, x, 1)[0]  # Return slope coefficient

def median_abs_deviation(signal):
    """Computes median absolute deviation of the signal.

    Feature computational cost: 1

    Parameters
    ----------
    signal : nd-array
        Input from which median absolute deviation is computed

    Returns
    -------
    float
        Mean absolute deviation result
    """
    return scipy.stats.median_abs_deviation(signal, scale=1)

def pk_pk_distance(signal):
    """Computes the peak to peak distance.

    Feature computational cost: 1

    Parameters
    ----------
    signal : nd-array
        Input from which peak to peak is computed

    Returns
    -------
    float
        peak to peak distance
    """
    return np.abs(np.max(signal) - np.min(signal))

def time_distance_from_peak(signal):
    loc_peak = np.argmax(signal)
    return len(signal) - loc_peak


class StreamflowFeatureExtractor:
    """
    Feature extraction pipeline for streamflow prediction.
    Creates rolling window features and target variable for prediction.
    """
    
    def __init__(self, feature_configs, prediction_horizon=30, offset=None):
        """
        Initialize feature extractor.
        
        Parameters:
        -----------
        prediction_horizon : int, default=30
            Number of days ahead to predict average streamflow
        feature_configs : dict
            Configuration for feature creation
        offset : int, default=None
            Number of days to offset the target variable (default: prediction_horizon)
        """
        self.prediction_horizon = prediction_horizon

        if offset is None:
            self.offset = self.prediction_horizon
        else:
            self.offset = offset
        
        assert self.offset >= self.prediction_horizon, "Offset must be greater or equal than prediction horizon"
        
        # Define feature configurations
        self.feature_configs = feature_configs
    
    def _create_rolling_features(self, df, column, config):
        """
        Create rolling window features for a single column, handling multiple basins.
        
        Parameters:
        -----------
        df : pandas.DataFrame
            Input DataFrame with 'code' column for basin identification
        column : str
            Column name to create features from
        config : dict
            Configuration for feature creation
            
        Returns:
        --------
        pandas.DataFrame
            DataFrame with rolling window features
        """
        features = pd.DataFrame(index=df.index)
        
        # Group by basin code and create features
        for code in df['code'].unique():
            basin_data = df[df['code'] == code][column]
            
            for window in config['windows']:
                
                feature_name = f"{column}_roll_{config['operation']}_{window}"
                
                min_periods = min(int(window * 0.5),15)
                # Calculate rolling feature for this basin
                if config['operation'] == 'mean':
                    basin_feature = basin_data.rolling(window=window, min_periods=min_periods).mean()
                elif config['operation'] == 'sum':
                    basin_feature = basin_data.rolling(window=window, min_periods=min_periods).sum()
                elif config['operation'] == 'min':
                    basin_feature = basin_data.rolling(window=window, min_periods=min_periods).min()
                elif config['operation'] == 'max':
                    basin_feature = basin_data.rolling(window=window, min_periods=min_periods).max()
                elif config['operation'] == 'std':
                    basin_feature = basin_data.rolling(window=window, min_periods=min_periods).std()
                elif config['operation'] == 'slope':
                    basin_feature = basin_data.rolling(window=window, min_periods=min_periods).apply(
                        rolling_slope, raw=True
                    )
                elif config['operation'] == 'peak_to_peak':
                    basin_feature = basin_data.rolling(window=window, min_periods=min_periods).apply(
                        pk_pk_distance, raw=True
                    )
                elif config['operation'] == 'median_abs_deviation':
                    basin_feature = basin_data.rolling(window=window, min_periods=min_periods).apply(
                        median_abs_deviation, raw=True
                    )
                elif config['operation'] == 'time_distance_from_peak':
                    basin_feature = basin_data.rolling(window=window, min_periods=min_periods).apply(
                        time_distance_from_peak, raw=True
                    )
                else:
                    raise ValueError(f"Unsupported operation: {config['operation']}")
            
                # First add the base feature (no lag)
                features.loc[basin_data.index, feature_name] = basin_feature
                
                # Handle lags, which could be a nested dictionary
                if config['lags']:
                    for window_key, lag_list in config['lags'].items():
                        # Only apply lags for the matching window size
                        if window == window_key:
                            for lag in lag_list:
                                # Create both positive (past) and negative (future) lags
                                lagged_feature = basin_feature.shift(lag)
                                lag_direction = "lag" if lag > 0 else "lead"
                                lag_value = abs(lag)
                                features.loc[basin_data.index, f"{feature_name}_{lag_direction}_{lag_value}"] = lagged_feature
                        
        return features
   
    def create_target(self, df, column='discharge'):
        """
        Create target variable: average discharge for next N days, by basin.
        
        Parameters:
        -----------
        df : pandas.DataFrame
            Input DataFrame with 'code' column
        column : str, default='discharge'
            Column to create target from
            
        Returns:
        --------
        pandas.Series
            Target variable
        """
        min_periods = min(int(self.prediction_horizon * 0.75), 15)
        target = pd.Series(index=df.index, dtype=float)
        
        # Calculate target for each basin separately
        for code in df['code'].unique():
            basin_data = df[df['code'] == code][column]
            
            # Calculate future average
            future_avg = basin_data.rolling(
                window=self.prediction_horizon, 
                min_periods=min_periods
            ).mean().shift(-self.offset)
            
            # Assign values back to the correct rows
            target.loc[basin_data.index] = future_avg
        
        return target
    

    def _create_time_features(self, df):
        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        df['month'] = df['date'].dt.month
        df['week'] = df['date'].dt.isocalendar().week

        df['day_of_year_sin'] = np.sin(2 * np.pi * df['date'].dt.dayofyear / 365)
        df['day_of_year_cos'] = np.cos(2 * np.pi * df['date'].dt.dayofyear / 365)

        df['week_sin'] = np.sin(2 * np.pi * df['week'] / 52)
        df['week_cos'] = np.cos(2 * np.pi * df['week'] / 52)

        df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
        df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)

        return df[['week_sin', 'week_cos', 'month_sin', 'month_cos', 'day_of_year_sin', 'day_of_year_cos']]


    def create_all_features(self, df):
        """
        Create all features based on configuration, handling multiple basins.
        
        Parameters:
        -----------
        df : pandas.DataFrame
            Input DataFrame containing all required columns and 'code' column
            
        Returns:
        --------
        pandas.DataFrame
            DataFrame with date index, code, features, and target columns
        """
        
        # Create features for each variable
        feature_dfs = []
        for column, config in self.feature_configs.items():
            features_with_column = [col for col in df.columns if column in col]
            for c in config:
                for col in features_with_column:
                    features = self._create_rolling_features(df, col, c)
                    feature_dfs.append(features)
        
        # Combine all features
        X = pd.concat(feature_dfs, axis=1)

        # Create target
        y = self.create_target(df)

        # Create time features
        time_features = self._create_time_features(df)

        # Combine everything into a single DataFrame in one operation
        # Start with base information
        base_data = {
            'date': df['date'],
            'code': df['code'],
            'target': y
        }

        # Add all feature columns from X
        for col in X.columns:
            base_data[col] = X[col]
            
        # Add all time feature columns
        for col in time_features.columns:
            base_data[col] = time_features[col]

        # Create the final DataFrame in one go
        final_df = pd.DataFrame(base_data, index=df.index)

        return final_df
    
    def get_feature_names(self):
        """
        Get list of all feature names that will be created.
        
        Returns:
        --------
        list
            List of feature names
        """
        feature_names = []
        for column, config in self.feature_configs.items():
            for window in config['windows']:
                feature_names.append(
                    f"{column}_roll_{config['operation']}_{window}"
                )
        return feature_names