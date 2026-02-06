##################################################
# Development Tool: Visualize Month Mapping Post-Processing
# Compare original forecasts vs calendar-month adjusted forecasts
##################################################

## How to run this script:
# Set the environment variable ieasyhydroforecast_env_file_path to point to your .env file
# Then run the script with:
# ieasyhydroforecast_env_file_path="../../../your_env_path/.env" lt_forecast_mode=monthly python dev_month_mapping.py

import os
import sys
import logging

# Suppress graphviz debug warnings
logging.getLogger("graphviz").setLevel(logging.WARNING)

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

# Add parent directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.join(script_dir, '..')
sys.path.insert(0, parent_dir)

from __init__ import logger
from data_interface import DataInterface
from config_forecast import ForecastConfig
from post_process_lt_forecast import (
    post_process_lt_forecast,
    calculate_lt_statistics_fc_period,
    calculate_lt_statistics_calendar_month,
    infer_q_columns
)

# Suppress lt_forecasting logger
logger_lt = logging.getLogger("lt_forecasting")
logger_lt.setLevel(logging.WARNING)

# Add iEasyHydroForecast to path
forecast_dir = os.path.join(script_dir, '..', '..', 'iEasyHydroForecast')
sys.path.append(forecast_dir)
import setup_library as sl


class MockForecastConfig:
    """Mock config for testing without full config loading."""
    def __init__(self, operational_month_lead_time: int = 2):
        self.operational_month_lead_time = operational_month_lead_time


def load_forecasts(forecast_config) -> pd.DataFrame:
    """Load all model forecasts and merge them."""
    ordered_models = forecast_config.get_model_execution_order()
    all_forecasts = None

    for model_name in ordered_models:
        output_path = forecast_config.get_output_path(model_name=model_name)
        output_file = os.path.join(output_path, f"{model_name}_hindcast.csv")

        if os.path.exists(output_file):
            df_forecast = pd.read_csv(output_file)
            print(f"Loaded {model_name}: {len(df_forecast)} rows")

            df_forecast['date'] = pd.to_datetime(df_forecast['date'])   
            # only consider the 25 th days of each month
            df_forecast = df_forecast[df_forecast['date'].dt.day == 25]

            if all_forecasts is None:
                all_forecasts = df_forecast.copy()
            else:
                Q_cols = [col for col in df_forecast.columns if col.startswith('Q') and col != 'Q_obs' and col != 'Q_lgbm' and col != 'Q_catboost' and col != 'Q_xgb']
                df_to_merge = df_forecast[['code', 'date'] + Q_cols].copy()
                all_forecasts = pd.merge(
                    all_forecasts, df_to_merge,
                    on=['code', 'date'], how='inner'
                )
        else:
            print(f"Forecast file not found: {output_file}")

    return all_forecasts


def plot_forecast_comparison(code: int, year: int,
                              raw_forecast: pd.DataFrame,
                              adjusted_forecast: pd.DataFrame,
                              discharge_data: pd.DataFrame,
                              fc_period_stats: pd.DataFrame,
                              calendar_month_stats: pd.DataFrame,
                              q_columns: list):
    """
    Plot comparison of original vs adjusted forecasts for a single code/year.

    Shows:
    - Original forecast values (Q columns)
    - Forecast period climatology
    - Calendar month climatology
    - Adjusted forecast values
    """
    # Filter data for this code and year
    raw = raw_forecast[
        (raw_forecast['code'] == code) &
        (pd.to_datetime(raw_forecast['date']).dt.year == year)
    ]

    if raw.empty:
        print(f"No forecast data for code {code}, year {year}")
        return None

    adj = adjusted_forecast[
        (adjusted_forecast['code'] == code) &
        (pd.to_datetime(adjusted_forecast['date']).dt.year == year)
    ]

    # Get statistics
    fc_stats = fc_period_stats[
        (fc_period_stats['code'] == code) &
        (fc_period_stats['year'] == year)
    ]

    target_month = adj['target_month'].values[0] if 'target_month' in adj.columns else None
    cal_stats = calendar_month_stats[
        (calendar_month_stats['code'] == code) &
        (calendar_month_stats['year'] == year) &
        (calendar_month_stats['month'] == target_month)
    ] if target_month else pd.DataFrame()

    # Create figure
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))

    # Get Q values
    q_cols_present = [q for q in q_columns if q in raw.columns]

    # Plot 1: Original forecast values
    ax1 = axes[0]
    raw_values = [raw[q].values[0] for q in q_cols_present]
    x_pos = np.arange(len(q_cols_present))
    ax1.bar(x_pos, raw_values, color='steelblue', alpha=0.7)
    ax1.set_xticks(x_pos)
    ax1.set_xticklabels(q_cols_present, rotation=45, ha='right')
    ax1.set_ylabel('Discharge')
    ax1.set_title('Original Forecast')

    # Add fc_period_lt_mean line
    if not fc_stats.empty:
        fc_mean = fc_stats['fc_period_lt_mean'].values[0]
        ax1.axhline(fc_mean, color='red', linestyle='--', label=f'FC Period LT Mean: {fc_mean:.1f}')
        ax1.legend(fontsize=8)

    # Plot 2: Climatology comparison
    ax2 = axes[1]
    clim_labels = []
    clim_values = []

    if not fc_stats.empty:
        clim_labels.append('FC Period\nLT Mean')
        clim_values.append(fc_stats['fc_period_lt_mean'].values[0])

    if not cal_stats.empty:
        clim_labels.append(f'Calendar Month {target_month}\nLT Mean')
        clim_values.append(cal_stats['calendar_month_lt_mean'].values[0])

    if clim_values:
        colors = ['coral', 'forestgreen']
        ax2.bar(np.arange(len(clim_values)), clim_values, color=colors[:len(clim_values)], alpha=0.7)
        ax2.set_xticks(np.arange(len(clim_values)))
        ax2.set_xticklabels(clim_labels)
        ax2.set_ylabel('Discharge')
        ax2.set_title('Climatology')

        # Add ratio annotation
        if len(clim_values) == 2 and clim_values[0] > 0:
            ratio = clim_values[1] / clim_values[0]
            ax2.text(0.5, 0.95, f'Ratio: {ratio:.3f}', transform=ax2.transAxes,
                    ha='center', fontsize=10, bbox=dict(boxstyle='round', facecolor='wheat'))

    # Plot 3: Adjusted forecast values (now stored in same Q columns)
    ax3 = axes[2]
    adj_cols = [q for q in q_cols_present if q in adj.columns]

    if adj_cols:
        adj_values = [adj[col].values[0] for col in adj_cols]
        ax3.bar(x_pos[:len(adj_values)], adj_values, color='forestgreen', alpha=0.7)
        ax3.set_xticks(x_pos[:len(adj_values)])
        ax3.set_xticklabels(adj_cols, rotation=45, ha='right')
        ax3.set_ylabel('Discharge')
        ax3.set_title('Adjusted Forecast (Calendar Month)')

        # Add calendar_month_lt_mean line
        if not cal_stats.empty:
            cal_mean = cal_stats['calendar_month_lt_mean'].values[0]
            ax3.axhline(cal_mean, color='red', linestyle='--', label=f'Cal Month LT Mean: {cal_mean:.1f}')
            ax3.legend(fontsize=8)

    # Overall title
    valid_from = raw['valid_from'].values[0] if 'valid_from' in raw.columns else 'N/A'
    valid_to = raw['valid_to'].values[0] if 'valid_to' in raw.columns else 'N/A'
    fig.suptitle(f'Code: {code} | Year: {year} | Forecast Period: {valid_from} to {valid_to}',
                 fontsize=12, fontweight='bold')

    plt.tight_layout()
    return fig


def plot_time_series_comparison(code: int,
                                 raw_forecast: pd.DataFrame,
                                 adjusted_forecast: pd.DataFrame,
                                 discharge_data: pd.DataFrame,
                                 q_column: str = 'Q50'):
    """
    Plot time series comparison across multiple years (hindcast).

    Shows original vs adjusted forecasts over time, plus observed discharge.
    """
    # Filter for this code
    raw = raw_forecast[raw_forecast['code'] == code].copy()
    adj = adjusted_forecast[adjusted_forecast['code'] == code].copy()

    if raw.empty:
        print(f"No forecast data for code {code}")
        return None

    raw['date'] = pd.to_datetime(raw['date'])
    adj['date'] = pd.to_datetime(adj['date'])
    adj['valid_from'] = pd.to_datetime(adj['valid_from'])
    adj['valid_to'] = pd.to_datetime(adj['valid_to'])

    raw = raw.sort_values('date')
    adj = adj.sort_values('date')

    fig, ax = plt.subplots(figsize=(14, 6))

    # Calculate observed mean discharge for each forecast's valid period
    obs_discharge = discharge_data[discharge_data['code'] == code].copy()
    obs_discharge['date'] = pd.to_datetime(obs_discharge['date'])

    q_obs_values = []
    for _, row in adj.iterrows():
        valid_from = row['valid_from']
        valid_to = row['valid_to']
        # Get observed discharge in the forecast period
        mask = (obs_discharge['date'] >= valid_from) & (obs_discharge['date'] <= valid_to)
        obs_period = obs_discharge.loc[mask, 'discharge']
        q_obs = obs_period.mean() if len(obs_period) > 0 else np.nan
        q_obs_values.append(q_obs)

    adj['Q_obs'] = q_obs_values

    # Plot observed discharge
    valid_obs = adj[adj['Q_obs'].notna()]
    if not valid_obs.empty:
        ax.plot(valid_obs['date'], valid_obs['Q_obs'], 'D-', color='black',
                label='Observed (period mean)', markersize=8, linewidth=2)

    # Plot original forecast
    if q_column in raw.columns:
        ax.plot(raw['date'], raw[q_column], 'o-', color='steelblue',
                label=f'Original {q_column}', markersize=6)

    # Plot adjusted forecast (now stored in same Q column)
    if q_column in adj.columns:
        ax.plot(adj['date'], adj[q_column], 's-', color='forestgreen',
                label=f'Adjusted {q_column}', markersize=6)

    # Plot climatology lines
    if 'fc_period_lt_mean' in adj.columns:
        ax.plot(adj['date'], adj['fc_period_lt_mean'], '--', color='coral',
                label='FC Period LT Mean', alpha=0.7)

    if 'calendar_month_lt_mean' in adj.columns:
        ax.plot(adj['date'], adj['calendar_month_lt_mean'], '--', color='purple',
                label='Calendar Month LT Mean', alpha=0.7)

    ax.set_xlabel('Forecast Issue Date')
    ax.set_ylabel('Discharge')
    ax.set_title(f'Code {code}: {q_column} - Original vs Calendar Month Adjusted vs Observed')
    ax.legend(loc='best')
    ax.grid(alpha=0.3)

    plt.tight_layout()
    return fig


def investigate_month_mapping():
    """Main function to investigate month mapping post-processing."""

    # Setup Environment
    sl.load_environment()

    # Load forecast configuration
    forecast_config = ForecastConfig()
    forecast_mode = os.getenv('lt_forecast_mode', 'monthly')
    forecast_config.load_forecast_config(forecast_mode=forecast_mode)
    forcing_HRU = forecast_config.get_forcing_HRU()

    print(f"Forecast mode: {forecast_mode}")
    print(f"Forcing HRU: {forcing_HRU}")

    # Load data via DataInterface
    data_interface = DataInterface()
    base_data_dict = data_interface.get_base_data(forcing_HRU=forcing_HRU)

    temporal_data = base_data_dict["temporal_data"]
    print(f"Loaded temporal data: {len(temporal_data)} rows")

    # Prepare discharge data
    discharge_data = temporal_data[['date', 'code', 'discharge']].copy()
    discharge_data = discharge_data.dropna(subset=['discharge'])
    print(f"Discharge data after dropna: {len(discharge_data)} rows")

    # Load forecasts
    raw_forecast = load_forecasts(forecast_config)

    if raw_forecast is None or raw_forecast.empty:
        print("No forecasts found!")
        return

    print(f"\nLoaded {len(raw_forecast)} forecast rows")
    print(f"Unique codes: {raw_forecast['code'].nunique()}")
    print(f"Columns: {raw_forecast.columns.tolist()}")

    # Get operational_month_lead_time from config or use default
    # This should come from the forecast_config but we'll use a mock for now
    operational_month_lead_time = 2  # Adjust based on your config
    mock_config = MockForecastConfig(operational_month_lead_time=operational_month_lead_time)

    print(f"\nRunning post-processing with lead_time={operational_month_lead_time}...")

    # Run post-processing
    adjusted_forecast = post_process_lt_forecast(
        forecast_config=mock_config,
        observed_discharge_data=discharge_data,
        raw_forecast=raw_forecast
    )

    print(f"Adjusted forecast shape: {adjusted_forecast.shape}")

    # Infer Q columns
    q_columns = infer_q_columns(raw_forecast)
    print(f"Q columns: {q_columns}")

    # Calculate statistics for visualization
    raw_forecast_copy = raw_forecast.copy()
    raw_forecast_copy['date'] = pd.to_datetime(raw_forecast_copy['date'])
    prediction_years = raw_forecast_copy['date'].dt.year.unique().tolist()

    fc_period_stats = calculate_lt_statistics_fc_period(discharge_data, raw_forecast)
    calendar_month_stats = calculate_lt_statistics_calendar_month(discharge_data, prediction_years)

    print(f"\nFC period stats: {len(fc_period_stats)} rows")
    print(f"Calendar month stats: {len(calendar_month_stats)} rows")

    # Select codes and years to visualize
    all_codes = raw_forecast['code'].unique()
    all_years = prediction_years

    print(f"\nAvailable codes: {len(all_codes)}")
    print(f"Available years: {all_years}")

    # Visualize a subset
    codes_to_plot = all_codes[:5]  # First 5 codes
    years_to_plot = all_years[:3] if len(all_years) > 1 else all_years  # First 3 years or all

    print(f"\nPlotting codes: {codes_to_plot}")
    print(f"Plotting years: {years_to_plot}")

    # Enable interactive mode so figures display immediately
    plt.ion()

    # Collect all figures
    all_figures = []

    # Plot individual code/year comparisons
    for code in codes_to_plot:
        for year in years_to_plot:
            fig = plot_forecast_comparison(
                code=code,
                year=year,
                raw_forecast=raw_forecast,
                adjusted_forecast=adjusted_forecast,
                discharge_data=discharge_data,
                fc_period_stats=fc_period_stats,
                calendar_month_stats=calendar_month_stats,
                q_columns=q_columns
            )
            if fig:
                all_figures.append(fig)
                plt.show()
                plt.pause(0.1)  # Small pause to render

    # Plot time series for hindcast (if multiple years)
    if len(all_years) > 1:
        for code in codes_to_plot[:3]:  # First 3 codes
            fig = plot_time_series_comparison(
                code=code,
                raw_forecast=raw_forecast,
                adjusted_forecast=adjusted_forecast,
                discharge_data=discharge_data,
                q_column='Q50'
            )
            if fig:
                all_figures.append(fig)
                plt.show()
                plt.pause(0.1)


    # Print summary statistics
    print("\n" + "="*60)
    print("SUMMARY STATISTICS")
    print("="*60)

    for q_col in q_columns[:3]:  # First 3 Q columns
        if q_col in raw_forecast.columns and q_col in adjusted_forecast.columns:
            orig = raw_forecast[q_col].dropna()
            adj = adjusted_forecast[q_col].dropna()

            if len(orig) > 0 and len(adj) > 0:
                print(f"\n{q_col}:")
                print(f"  Original  - Mean: {orig.mean():.2f}, Std: {orig.std():.2f}")
                print(f"  Adjusted  - Mean: {adj.mean():.2f}, Std: {adj.std():.2f}")
                print(f"  Ratio (adj/orig) - Mean: {(adj / orig.values).mean():.3f}")

    # Keep plots open until user closes them
    print(f"\n{len(all_figures)} figures created. Close all figures to exit.")
    plt.ioff()  # Disable interactive mode
    plt.show(block=True)  # Block until all figures are closed


if __name__ == "__main__":
    investigate_month_mapping()
