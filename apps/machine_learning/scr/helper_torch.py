import numpy as np
import torch
from torch.utils.data import Dataset

class HydroDataset(Dataset):
    """
    Custom dataset for hydrological time series data with static catchment features.

    Input:
    - dataframe: pandas DataFrame with columns 'code', 'date', 'P', 'T', 'PET', 'daylight_hours', 'discharge'
    - static_data: pandas DataFrame with columns 'code' and static features (latitude, longitude, elevation, etc.)
    - input_length: number of time steps to use as input
    - output_length: number of time steps to predict
    - features: list of feature columns to use
    - target: target column to predict
    - static_features: list of static feature columns to use
    """
    def __init__(self, dataframe, static_data, input_length, output_length, 
                 features=['P', 'T', 'PET', 'daylight_hours'], 
                 target='discharge',
                 static_features=['lat', 'lon', 'elevation']):
        
        self.input_length = input_length
        self.output_length = output_length
        self.features = sorted(features)  # Sort feature names
        self.target = target
        self.static_features = sorted(static_features)  # Sort static feature names
        
        # Sort the dataframe by code and date
        df_sorted = dataframe.sort_values(['code', 'date'])
        
        # Process static data
        self.static_data = static_data.sort_values('code')  # Sort by code for consistency
        # Verify all codes in the time series data exist in static data
        assert set(np.unique(df_sorted['code'])).issubset(set(self.static_data['code'])), \
            "Some catchment codes in time series data are missing from static data"
        
        # Create a dictionary for quick lookup of static features
        self.static_features_dict = {
            row['code']: row[static_features].values 
            for _, row in self.static_data.iterrows()
        }

        # Convert to numpy arrays and find valid samples
        self.samples = []
        self.codes = []
        self.dates = []
        self.feature_data = []
        self.target_data = []
        
        total_possible_samples = 0
        total_valid_samples = 0
        
        # Process each catchment separately
        for code in df_sorted['code'].unique():
            catchment_data = df_sorted[df_sorted['code'] == code]
            
            # Get data as numpy arrays
            code_array = catchment_data['code'].values
            dates_array = catchment_data['date'].values
            features_array = catchment_data[features].values
            if self.target is not None:
                target_array = catchment_data[target].values
            else:
                target_array = np.zeros(len(features_array))
            
            # Find continuous valid periods (no NaN in features_array)
            valid_periods = self._find_valid_periods(
                features_array, 
                input_length, 
                output_length
            )
            
            catchment_possible = len(target_array) - (input_length + output_length) + 1
            total_possible_samples += max(0, catchment_possible)
            total_valid_samples += len(valid_periods)
            
            # Store valid samples
            for start_idx, end_idx in valid_periods:
                self.codes.append(code_array[start_idx:end_idx])
                self.dates.append(dates_array[start_idx:end_idx])
                self.feature_data.append(features_array[start_idx:end_idx])
                self.target_data.append(target_array[start_idx:end_idx])
                self.samples.append(len(self.samples))  # Store index for __getitem__
                
        # Convert lists of arrays to single arrays
        self.codes = np.concatenate(self.codes)
        self.dates = np.concatenate(self.dates)
        self.feature_data = np.concatenate(self.feature_data)
        self.target_data = np.concatenate(self.target_data)
        
    
    def _find_valid_periods(self, features_array, input_length, output_length):
        """
        Find continuous periods where there are no NaN values in the prediction window.
        Checks every possible time window.
        Returns list of (start_idx, end_idx) tuples for valid periods.
        """
        valid_periods = []
        total_length = input_length + output_length
        
        # Check every possible window
        for i in range(len(features_array) - total_length + 1):
            # Check if output window has any NaN
            output_window = features_array[i:i + total_length]
            if not np.any(np.isnan(output_window)):
                # Period is valid, add to list
                valid_periods.append((i, i + total_length))
        
        return valid_periods
    
    def __len__(self):
        return len(self.samples)
    
    def __getitem__(self, idx):
        sample_idx = self.samples[idx]
        start_idx = sample_idx * (self.input_length + self.output_length)
        end_idx = start_idx + self.input_length + self.output_length
        
        # Extract features (X) and target (y)
        #x is t-input_length to t+output_length where t is the date we do the forecast
        X = self.feature_data[start_idx:start_idx + self.input_length + self.output_length]
        y = self.target_data[start_idx + self.input_length:end_idx]
        
        # Get the corresponding dates and code
        dates = self.dates[start_idx + self.input_length:end_idx]
        code = self.codes[start_idx]
        
        # Get static features for the catchment
        static = self.static_features_dict[code]
        static = np.array(static, dtype=np.float32)

        
        return {
            'X': torch.FloatTensor(X),
            'y': torch.FloatTensor(y),
            'static': torch.FloatTensor(static),
          #  'code': torch.FloatTensor([code]),
        }

    def __getdate__(self, idx):
        sample_idx = self.samples[idx]
        start_idx = sample_idx * (self.input_length + self.output_length)
        end_idx = start_idx + self.input_length + self.output_length
        
        # Get the corresponding dates and code
        dates = self.dates[start_idx + self.input_length:end_idx]
        code = self.codes[start_idx]
        
        return {
            'dates': dates,
            'code': code,
        }

