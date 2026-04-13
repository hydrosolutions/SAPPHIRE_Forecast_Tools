"""
One-time script to write full historical data to the SAPPHIRE API.
Syncs daily runoff, and pentadal/decadal time series (observations).

Usage:
    cd apps/preprocessing_runoff
    ieasyhydroforecast_env_file_path=/path/to/your/.env \
        .venv/bin/python initial_api_sync.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "iEasyHydroForecast"))
import setup_library as sl

sl.load_environment()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
import src

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "iEasyHydroForecast"))
import forecast_library as fl

os.environ["SAPPHIRE_SYNC_MODE"] = "initial"

import pandas as pd

intermediate_path = os.getenv("ieasyforecast_intermediate_data_path")

# --- Daily runoff (src.py writer) ---
daily_file = os.getenv("ieasyforecast_daily_discharge_file", "")
if daily_file:
    csv_path = os.path.join(intermediate_path, daily_file)
    if os.path.exists(csv_path):
        data = pd.read_csv(csv_path)
        print(f"Daily runoff: {len(data)} records from {csv_path}")
        ok = src._write_runoff_to_api(data, mode=None)
        print(f"  -> {'OK' if ok else 'FAILED/SKIPPED'}")

# --- Pentadal time series (forecast_library.py writer) ---
pentad_file = os.getenv("ieasyforecast_pentad_discharge_file", "")
if pentad_file:
    csv_path = os.path.join(intermediate_path, pentad_file)
    if os.path.exists(csv_path):
        data = pd.read_csv(csv_path)
        print(f"Pentadal observations: {len(data)} records from {csv_path}")
        result = fl._write_runoff_to_api(data, "pentad", mode=None)
        print(f"  -> {'OK' if result is not None else 'FAILED/SKIPPED'}")

# --- Decadal time series (forecast_library.py writer) ---
decad_file = os.getenv("ieasyforecast_decad_discharge_file", "")
if decad_file:
    csv_path = os.path.join(intermediate_path, decad_file)
    if os.path.exists(csv_path):
        data = pd.read_csv(csv_path)
        print(f"Decadal observations: {len(data)} records from {csv_path}")
        result = fl._write_runoff_to_api(data, "decade", mode=None)
        print(f"  -> {'OK' if result is not None else 'FAILED/SKIPPED'}")
