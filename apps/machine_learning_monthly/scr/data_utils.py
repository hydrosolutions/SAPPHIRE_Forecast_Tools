import sys 
import glob
import os
import re

import numpy as np
import pandas as pd
import netCDF4 as nc
import xarray as xr
import geopandas as gpd
import rasterio
from scipy import stats

# Plotting
import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns

import tqdm
from collections import defaultdict


def discharge_m3_to_mm(df):
    pass


def discharge_mm_to_m3(df):
    pass






def get_elevation_bands_per_percentile(elevation_band_code, num_bands=3, rel_area_col='relative_a'):
    """
    Assigns elevation bands to percentiles based on the elevation.
    returns: a dictionary with {P1 : {bands : [1,2,3...], relative_area : [0.1, 0.2, 0.3...]}
    """
    # Calculate percentiles based on elevation
    elevation_band_code = elevation_band_code.copy()
    elevation_band_code['elevation_band'] = elevation_band_code['name'].apply(
        lambda x: int(str(x).split('_')[-1]) if '_' in str(x) else int(x)
    )
    elevation_band_code = elevation_band_code.sort_values(by='elevation_band')
    
    # Calculate cumulative area
    total_area = elevation_band_code[rel_area_col].sum()
    elevation_band_code['cum_area'] = elevation_band_code[rel_area_col].cumsum() / total_area
    
    # Define percentile thresholds based on num_bands
    thresholds = [i/num_bands for i in range(1, num_bands)]
    
    # Initialize result dictionary
    result = {}
    for i in range(num_bands):
        band_name = f'Perc_Elev_{i+1}'
        result[band_name] = {'bands': [], 'relative_area': []}
    
    # Assign bands to percentiles
    current_percentile = 0
    
    for i, row in elevation_band_code.iterrows():
        # Find which percentile this band belongs to
        cum_area = row['cum_area']
        band_idx = 0
        
        # Determine which percentile band this elevation belongs to
        for j, threshold in enumerate(thresholds):
            if cum_area <= threshold:
                band_idx = j
                break
            band_idx = j + 1
        
        band_name = f'Perc_Elev_{band_idx+1}'
        result[band_name]['bands'].append(int(row['elevation_band']))
        result[band_name]['relative_area'].append(row[rel_area_col])
    
    return result
    


def calculate_percentile_snow_bands(hydro_df, elevation_band_df, num_bands=3, col_name = 'SWE'):
    """
    Calculates SWE for n elevation bands based on area percentiles: example col name = SWE
        - SWE_P1: 0-p1 percentile
        - SWE_P2: p1-p2 percentile
        -....
        - SWE_Pn: pn-100 percentile
    """
    hydro_df = hydro_df.copy()
    
    # Initialize the new SWE percentile columns
    for i in range(1, num_bands + 1):
        hydro_df[f'{col_name}_Perc_Elev_{i}'] = 0.0
    
    # Process each basin code separately
    for code in hydro_df.code.unique():
        # Filter data for current basin
        basin_mask = elevation_band_df['code'] == code
        basin_elev_data = elevation_band_df[basin_mask].copy()
        
        if basin_elev_data.empty:
            continue
        
        # Get elevation bands per percentile for this basin
        percentile_bands = get_elevation_bands_per_percentile(
            basin_elev_data, num_bands=num_bands, rel_area_col='relative_a')
        
        # Calculate SWE for each percentile band
        hydro_mask = hydro_df['code'] == code
        
        for percentile, data in percentile_bands.items():
            percentile_col_name = f'{col_name}_{percentile}'
            bands = data['bands']
            rel_areas = data['relative_area']
            
            # Total weight for normalization
            total_weight = sum(rel_areas)
            
            if total_weight == 0:
                continue
            
            # Calculate weighted SWE for this percentile
            for band, rel_area in zip(bands, rel_areas):
                swe_col = f'{col_name}_{band}'
                
                if swe_col not in hydro_df.columns:
                    continue
                
                # Add weighted contribution
                hydro_df.loc[hydro_mask, percentile_col_name] += hydro_df.loc[hydro_mask, swe_col] * rel_area
            
            # Normalize by total weight
            hydro_df.loc[hydro_mask, percentile_col_name] /= total_weight
    
    return hydro_df


def normalize_features(df_train, df_test, features, target):
    """
    Normalize features and target using mean and standard deviation from training data.
    """
    df_train = df_train.copy()
    df_test = df_test.copy()
    
    scaler = {}
    # Pre-convert columns to float to avoid dtype warnings
    cols_to_normalize = features + [target]
    for col in cols_to_normalize:
        df_train[col] = df_train[col].astype(float)
        df_test[col] = df_test[col].astype(float)
        
        mean_ = df_train[col].mean()
        std_ = df_train[col].std()
        std_ = 1 if std_ == 0 else std_
        scaler[col] = (mean_, std_)

        df_train[col] = (df_train[col] - mean_) / std_
        df_test[col] = (df_test[col] - mean_) / std_
    
    return df_train, df_test, scaler

def normalize_features_per_basin(df_train, df_test, features, target):
    """
    Fast normalization of features and target per basin using vectorized operations.
    """
    
    df_train = df_train.copy()
    df_test = df_test.copy()
    
    # Pre-convert columns to float to avoid dtype warnings
    cols_to_normalize = features + [target]
    for col in cols_to_normalize:
        df_train[col] = df_train[col].astype(float)
        df_test[col] = df_test[col].astype(float)
    
    # Pre-compute statistics for all basins at once
    basin_stats = df_train.groupby('code')[cols_to_normalize].agg(['mean', 'std'])
    
    # Replace zero std with 1
    basin_stats.loc[:, (slice(None), 'std')] = basin_stats.loc[:, (slice(None), 'std')].replace(0, 1)
    
    # Initialize scaler dictionary
    scaler = {code: {} for code in df_train.code.unique()}
    
    # Faster implementation using vectorized operations
    for code in df_train.code.unique():
        # Get masks for this basin
        train_mask = df_train['code'] == code
        test_mask = df_test['code'] == code
        
        # Get all statistics for this basin
        means = basin_stats.loc[code, (slice(None), 'mean')].values
        stds = basin_stats.loc[code, (slice(None), 'std')].values
        
        # Store in scaler dictionary
        for i, col in enumerate(cols_to_normalize):
            scaler[code][col] = (means[i], stds[i])
        
        # Apply normalization to all columns at once for training data
        for i, col in enumerate(cols_to_normalize):
            mean_val = means[i]
            std_val = stds[i]
            df_train.loc[train_mask, col] = (df_train.loc[train_mask, col] - mean_val) / std_val
        
        # Apply normalization to all columns at once for test data
        for i, col in enumerate(cols_to_normalize):
            mean_val = means[i]
            std_val = stds[i]
            if test_mask.any():  # Check if test set has this basin
                df_test.loc[test_mask, col] = (df_test.loc[test_mask, col] - mean_val) / std_val
    
    return df_train, df_test, scaler



def calculate_elevation_band_areas(gdf_path):
    """
    Calculate relative areas for each elevation band by code.
    
    Parameters:
    -----------
    gdf : GeoDataFrame
        Geodataframe containing elevation band polygons with names in format 'code_band'
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame with columns for code and relative areas of each elevation band
    """
    # Read GeoDataFrame
    gdf = gpd.read_file(gdf_path)
    # Split name into code and band
    gdf['code'] = gdf['name'].str.split('_').str[0].astype(int)
    gdf['band'] = gdf['name'].str.split('_').str[1].astype(int)
    
    # Calculate areas
    gdf['area'] = gdf.geometry.area
    
    # Create dictionary to store results
    results = defaultdict(dict)
    
    # Process each code
    for code in gdf['code'].unique():
        # Get data for this code
        code_data = gdf[gdf['code'] == code]
        
        # Calculate total area for this code
        total_area = code_data['area'].sum()
        
        # Calculate relative areas for each band
        for _, row in code_data.iterrows():
            relative_area = row['area'] / total_area
            results[code][f'relative_area_{row["band"]}'] = relative_area
            
        # Add code identifier
        results[code]['code'] = code
    
    # Convert results to DataFrame
    df_results = pd.DataFrame.from_dict(results, orient='index')
    
    # Ensure all elevation bands are present (fill missing with 0)
    all_bands = [f'relative_area_{i}' for i in range(1, gdf['band'].max() + 1)]
    for band in all_bands:
        if band not in df_results.columns:
            df_results[band] = 0.0
    
    # Sort columns
    cols = ['code'] + sorted([col for col in df_results.columns if col != 'code'])
    df_results = df_results[cols]
    
    # nan to 0
    df_results = df_results.fillna(0)

    # Reset index
    df_results = df_results.reset_index(drop=True)

    
    return df_results

def average_by_elevation_band(df, elevation_band_areas):

    df = df.copy()

    for code in df.code.unique():
        mask_df = df.code == code
        mask_elev = elevation_band_areas.code == code

        for col in df.columns:
            if "_" in col:
                try:
                    band = int(col.split("_")[1])
                    relative_area = elevation_band_areas.loc[mask_elev, f'relative_area_{band}'].values[0]
                    df.loc[mask_df, col] = df.loc[mask_df, col] * relative_area
                except Exception as e:
                    print("-" * 50)
                    print(f"Error processing column {col}")
                    print(e)

    return df


def create_lag_features(df, features, lags):

    df = df.copy()

    for code in df.code.unique():
        mask = df.code == code

        for feature in features:
            for lag in lags:
                df.loc[mask, f'{feature}_lag_{lag}'] = df.loc[mask, feature].shift(lag)

    return df


def create_fourier_features(
    df_train : pd.DataFrame,
    df_test : pd.DataFrame,
    features : list,
    n_harmonics : int = 4,
    fit_on_month : bool = True):

    df_train = df_train.copy()
    df_test = df_test.copy()

    for code in df_train.code.unique():
        mask_train = df_train.code == code
        mask_test = df_test.code == code

        for feature in features:
            ff = FF.FourierFeatures(target_col = feature, n_harmonics=n_harmonics, fit_on_month=fit_on_month)
            ff.fit(df_train[mask_train])
            df_train.loc[mask_train, f'{feature}_fourier'] = ff.transform(df_train[mask_train])
            df_train.loc[mask_train, f'{feature}_anomaly'] = ff.get_anomaly(df_train[mask_train])
            df_test.loc[mask_test, f'{feature}_fourier'] = ff.transform(df_test[mask_test])
            df_test.loc[mask_test, f'{feature}_anomaly'] = ff.get_anomaly(df_test[mask_test])

    return df_train, df_test


def agg_with_min_obs(x, func='mean', min_obs=15):
    """
    Aggregate data only if there are enough valid observations
    
    Parameters:
    -----------
    x : Series
        Data to aggregate
    func : str
        'mean' or 'sum'
    min_obs : int
        Minimum number of non-NaN observations required
    
    Returns:
    --------
    float or NaN
        Aggregated value if enough observations, NaN otherwise
    """
    valid_count = x.notna().sum()
    if valid_count >= min_obs:
        if func == 'last':
            return np.nanmean(x.iloc[-5:])  # Get last value this way instead
        return x.agg(func)
    return np.nan


def create_monthly_df(df, feature_cols):

    df = df.copy()

    df['month'] = df['date'].dt.month
    df['year'] = df['date'].dt.year

    groupby_cols = ['year', 'month']

    # Base aggregations
    agg_dict = {
        'discharge': lambda y: agg_with_min_obs(y, 'mean'),
        'T': 'mean',
        'P': 'sum',
    }

    # Add feature column aggregations
    for col in feature_cols:
        df[f'{col}_mean'] = df[col].bfill().values
        #df[f'{col}_mean_last'] = df[col].bfill().values
        agg_dict[f'{col}_mean'] =  'mean'
        #agg_dict[f'{col}_mean_last'] = lambda y: agg_with_min_obs(y, 'last')
    
    # Apply the groupby with all aggregations
    hydro_df = df.groupby(groupby_cols).agg(agg_dict).reset_index()
    
    return hydro_df