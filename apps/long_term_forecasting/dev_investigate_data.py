##################################################
# Investigate Data for Long Term Forecasting
# Development Tool - No production use
##################################################

## How to run this script:
# Set the environment variable ieasyhydroforecast_env_file_path to point to your .env file
# Then run the script with:
# ieasyhydroforecast_env_file_path="../../../kyg_data_forecast_tools/config/.env_develop_kghm" lt_forecast_mode=monthly python dev_investigate_data.py


from datetime import datetime
import logging

# Suppress graphviz debug warnings BEFORE importing any modules that use graphviz
logging.getLogger("graphviz").setLevel(logging.WARNING)

import os
import sys
import time
import glob
import pandas as pd
import numpy as np
import json
from typing import List, Dict, Any, Tuple

# Import forecast models
from lt_forecasting.forecast_models.LINEAR_REGRESSION import LinearRegressionModel
from lt_forecasting.forecast_models.SciRegressor import SciRegressor
from lt_forecasting.forecast_models.deep_models.uncertainty_mixture import (
    UncertaintyMixtureModel,
)


from __init__ import logger 
from data_interface import DataInterface
from config_forecast import ForecastConfig


# set lt_forecasting logger level
logger_lt = logging.getLogger("lt_forecasting")
logger_lt.setLevel(logging.WARNING)

# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the setup_library module from the iEasyHydroForecast package
import setup_library as sl



def investigate():

    # Setup Environment
    sl.load_environment()

    # Now we setup the configurations
    forecast_config = ForecastConfig()

    forecast_mode = os.getenv('lt_forecast_mode')
    forecast_config.load_forecast_config(forecast_mode=forecast_mode)
    forcing_HRU = forecast_config.get_forcing_HRU()

    # Data Interface
    data_interface = DataInterface()
    base_data_dict = data_interface.get_base_data(forcing_HRU=forcing_HRU)

    temporal_data = base_data_dict["temporal_data"]
    static_data = base_data_dict["static_data"]
    offset_base = base_data_dict["offset_date_base"]
    offset_discharge = base_data_dict["offset_date_discharge"]

    # lets investigate the forecasts for some codes:
    # read in the forecast files
    ordered_models = forecast_config.get_model_execution_order()
    execution_is_success = {}
    model_dependencies = forecast_config.get_model_dependencies()

    all_forecasts = None

    for model_name in ordered_models:
        print(f"Reading forecast for model: {model_name}")
        output_path = forecast_config.get_output_path(model_name=model_name)
        output_file = os.path.join(output_path, f"{model_name}_forecast.csv")
        if os.path.exists(output_file):
            df_forecast = pd.read_csv(output_file)
            print(f"  Loaded {len(df_forecast)} rows, columns: {df_forecast.columns.tolist()}")
            print(f"  Unique codes: {df_forecast['code'].unique() if 'code' in df_forecast.columns else 'NO CODE COLUMN'}")
            print(f"  Date values: {df_forecast['date'].unique() if 'date' in df_forecast.columns else 'NO DATE COLUMN'}")
            
            if all_forecasts is None:
                all_forecasts = df_forecast.copy()
            else:
                Q_cols = [col for col in df_forecast.columns if 'Q' in col]
                print(f"  Q columns to merge: {Q_cols}")
                df_to_merge = df_forecast[['code'] + Q_cols].copy()
                # Use outer merge on code only, since all forecasts are for the same period
                all_forecasts = pd.merge(all_forecasts, df_to_merge, on=['code'], how='outer', suffixes=('', f'_{model_name}'))
                print(f"  After merge: {len(all_forecasts)} rows")
        else:
            print(f"Forecast file not found for model {model_name} at {output_file}")

    if all_forecasts is None:
        print("No forecasts found to investigate.")
        return
    
    print(f"\nFinal all_forecasts shape: {all_forecasts.shape}")
    print(f"Final all_forecasts columns: {all_forecasts.columns.tolist()}")
    print(f"Final all_forecasts head:\n{all_forecasts.head()}")
    
    all_codes = all_forecasts['code'].unique()
    print(f"Total unique codes in forecasts: {len(all_codes)}")
    
    if len(all_codes) == 0:
        print("ERROR: No codes found. Checking individual forecast files...")
        for model_name in ordered_models:
            output_path = forecast_config.get_output_path(model_name=model_name)
            output_file = os.path.join(output_path, f"{model_name}_forecast.csv")
            if os.path.exists(output_file):
                df = pd.read_csv(output_file)
                print(f"  {model_name}: {len(df)} rows, codes: {df['code'].unique() if 'code' in df.columns else 'N/A'}")
        return

    if 16936 in all_codes:
        region = "KGZ"
    elif 17084 in all_codes:
        region = "TJK"
    else:
        region = "UNKNOWN"

    output_path = "/Users/sandrohunziker/Documents/hydrosol_local/operational/forecast_vis"
    output_path = os.path.join(output_path, region)
    os.makedirs(output_path, exist_ok=True)

    # Collect all data for visualization
    summary_data = []
    
    for code in all_codes:
        print(f"Processing code: {code}")
        if code not in all_forecasts['code'].values:
            continue

        forecast_code = all_forecasts[all_forecasts['code'] == code].copy()
        forecast_code['date'] = pd.to_datetime(forecast_code['date'])
        temporal_data_code = temporal_data[temporal_data['code'] == code].copy()
        
        if temporal_data_code.empty:
            print(f"  No temporal data for code {code}, skipping.")
            continue
            
        valid_from = forecast_code['valid_from'].iloc[0]
        valid_to = forecast_code['valid_to'].iloc[0]

        valid_from_date = pd.to_datetime(valid_from)
        valid_to_date = pd.to_datetime(valid_to)
        
        # Extract month and day for filtering across years
        from_month = valid_from_date.month
        from_day = valid_from_date.day
        to_month = valid_to_date.month
        to_day = valid_to_date.day
        
        # Create day-of-year based mask that works across all years
        temporal_data_code['month'] = temporal_data_code['date'].dt.month
        temporal_data_code['day'] = temporal_data_code['date'].dt.day
        
        # Handle both same-year and cross-year ranges
        if (from_month, from_day) <= (to_month, to_day):
            # Normal case: e.g., May 1 to May 31
            mask = ((temporal_data_code['month'] > from_month) | 
                    ((temporal_data_code['month'] == from_month) & (temporal_data_code['day'] >= from_day))) & \
                   ((temporal_data_code['month'] < to_month) | 
                    ((temporal_data_code['month'] == to_month) & (temporal_data_code['day'] <= to_day)))
        else:
            # Cross-year case: e.g., Dec 1 to Jan 31
            mask = ((temporal_data_code['month'] > from_month) | 
                    ((temporal_data_code['month'] == from_month) & (temporal_data_code['day'] >= from_day))) | \
                   ((temporal_data_code['month'] < to_month) | 
                    ((temporal_data_code['month'] == to_month) & (temporal_data_code['day'] <= to_day)))
        
        filtered_data = temporal_data_code.loc[mask, 'discharge'].dropna()
        
        if filtered_data.empty:
            print(f"  No discharge data in period for code {code}, skipping.")
            continue
        
        long_term_mean = filtered_data.mean()
        long_term_q10 = filtered_data.quantile(0.1)
        long_term_q90 = filtered_data.quantile(0.9)
        long_term_std = filtered_data.std()
        
        print(f"  Code {code}: mean={long_term_mean:.2f}, q10={long_term_q10:.2f}, q90={long_term_q90:.2f}, n={len(filtered_data)}")

        # Normalize by long-term mean
        if long_term_mean > 0:
            norm_factor = long_term_mean
        else:
            print(f"  Code {code}: long_term_mean is 0 or negative, skipping normalization.")
            continue
        
        # Normalized values
        norm_mean_normalized = 1.0  # By definition
        norm_q10_normalized = long_term_q10 / norm_factor
        norm_q90_normalized = long_term_q90 / norm_factor
        norm_std_normalized = long_term_std / norm_factor

        # Collect deterministic forecasts (Q_ columns) - normalized
        Q_cols = [col for col in forecast_code.columns if 'Q_' in col and col != 'Q_loc']
        q_values = [forecast_code[col].values[0] / norm_factor for col in Q_cols 
                    if pd.notna(forecast_code[col].values[0])]

        # Collect probabilistic forecasts - normalized
        Q_prob_cols = ['Q5', 'Q10', 'Q25', 'Q50', 'Q75', 'Q90', 'Q95']
        prob_forecasts = {}
        if all(col in forecast_code.columns for col in Q_prob_cols):
            for col in Q_prob_cols:
                val = forecast_code[col].values[0]
                prob_forecasts[col] = val / norm_factor if pd.notna(val) else np.nan

        summary_data.append({
            'code': code,
            'valid_from': valid_from,
            'valid_to': valid_to,
            'norm_mean': norm_mean_normalized,
            'norm_q10': norm_q10_normalized,
            'norm_q90': norm_q90_normalized,
            'norm_std': norm_std_normalized,
            'ensemble_values': q_values,
            'ensemble_mean': np.mean(q_values) if q_values else np.nan,
            **prob_forecasts
        })

    if not summary_data:
        print("No valid data collected for visualization.")
        return

    summary_df = pd.DataFrame(summary_data)
    
    # Create multi-figure plots with max 10 codes per figure
    codes_per_figure = 10
    all_codes_list = summary_df['code'].tolist()
    num_figures = int(np.ceil(len(all_codes_list) / codes_per_figure))
    
    import matplotlib.pyplot as plt
    
    for fig_idx in range(num_figures):
        start_idx = fig_idx * codes_per_figure
        end_idx = min((fig_idx + 1) * codes_per_figure, len(all_codes_list))
        codes_subset = all_codes_list[start_idx:end_idx]
        df_subset = summary_df[summary_df['code'].isin(codes_subset)].reset_index(drop=True)
        
        fig, ax = plt.subplots(figsize=(14, 8))
        
        x_positions = np.arange(len(df_subset))
        bar_width = 0.35
        
        for i, row in df_subset.iterrows():
            x = x_positions[i]
            
            # Skip if norm_mean is NaN
            if pd.isna(row['norm_mean']):
                continue
            
            # Plot norm as bar with uncertainty
            ax.bar(x - bar_width/2, row['norm_mean'], width=bar_width, 
                   color='steelblue', alpha=0.7, label='Norm Mean' if i == 0 else '')
            
            # Calculate yerr with proper bounds (ensure non-negative)
            yerr_lower = max(0, row['norm_mean'] - row['norm_q10']) if pd.notna(row['norm_q10']) else 0
            yerr_upper = max(0, row['norm_q90'] - row['norm_mean']) if pd.notna(row['norm_q90']) else 0
            
            if yerr_lower > 0 or yerr_upper > 0:
                ax.errorbar(x - bar_width/2, row['norm_mean'], 
                           yerr=[[yerr_lower], [yerr_upper]],
                           fmt='none', color='darkblue', capsize=4, capthick=1.5, linewidth=1.5)
            
            # Plot ensemble predictions as scattered dots
            if row['ensemble_values']:
                jitter = np.random.uniform(-0.08, 0.08, len(row['ensemble_values']))
                ax.scatter([x + bar_width/2 + j for j in jitter], row['ensemble_values'], 
                          color='coral', alpha=0.6, s=30, edgecolors='k', linewidth=0.5,
                          label='Ensemble Members' if i == 0 else '')
            
            # Plot probabilistic forecast with error bars
            if 'Q50' in row and pd.notna(row.get('Q50')):
                q50 = row['Q50']
                
                # Calculate yerr with proper bounds for probabilistic forecasts
                yerr_25 = max(0, q50 - row['Q25']) if pd.notna(row.get('Q25')) else 0
                yerr_75 = max(0, row['Q75'] - q50) if pd.notna(row.get('Q75')) else 0
                yerr_10 = max(0, q50 - row['Q10']) if pd.notna(row.get('Q10')) else 0
                yerr_90 = max(0, row['Q90'] - q50) if pd.notna(row.get('Q90')) else 0
                yerr_5 = max(0, q50 - row['Q5']) if pd.notna(row.get('Q5')) else 0
                yerr_95 = max(0, row['Q95'] - q50) if pd.notna(row.get('Q95')) else 0
                
                # 25-75 percentile (thicker)
                ax.errorbar(x + bar_width/2 + 0.15, q50, 
                           yerr=[[yerr_25], [yerr_75]],
                           fmt='D', color='darkred', markersize=6, capsize=5, capthick=2, linewidth=2,
                           label='MC ALD (25-75%)' if i == 0 else '')
                # 10-90 percentile (medium)
                if yerr_10 > 0 or yerr_90 > 0:
                    ax.errorbar(x + bar_width/2 + 0.15, q50, 
                               yerr=[[yerr_10], [yerr_90]],
                               fmt='none', color='darkred', capsize=4, capthick=1.5, linewidth=1.5, alpha=0.7)
                # 5-95 percentile (thinner)
                if yerr_5 > 0 or yerr_95 > 0:
                    ax.errorbar(x + bar_width/2 + 0.15, q50, 
                               yerr=[[yerr_5], [yerr_95]],
                               fmt='none', color='darkred', capsize=3, capthick=1, linewidth=1, alpha=0.5)
        
        ax.set_xticks(x_positions)
        ax.set_xticklabels([str(c) for c in df_subset['code']], rotation=45, ha='right')
        ax.set_xlabel('Station Code')
        ax.set_ylabel('Normalized Discharge (ratio to norm)')
        
        # Add horizontal line at y=1 to show the norm reference
        ax.axhline(y=1.0, color='gray', linestyle='--', linewidth=1, alpha=0.7, label='Norm = 1.0')
        
        # Add period info to title
        valid_from_str = df_subset['valid_from'].iloc[0]
        valid_to_str = df_subset['valid_to'].iloc[0]
        ax.set_title(f'Forecast Comparison: {region} (Period: {valid_from_str} to {valid_to_str})\nFigure {fig_idx + 1}/{num_figures}')
        
        ax.legend(loc='upper right', fontsize=9)
        ax.grid(axis='y', alpha=0.3)
        
        plt.tight_layout()
        
        # Save figure
        output_file = os.path.join(output_path, f'forecast_summary_{region}_{fig_idx + 1}.png')
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        print(f"Saved figure to: {output_file}")
        plt.close()

    print(f"Generated {num_figures} figure(s) in {output_path}")


if __name__ == "__main__":
    investigate()