#!/usr/bin/env bash

# Source the conda.sh script to initialize the shell with conda
# Replace /path/to/conda with the path to your conda installation
source /Users/bea/anaconda3/etc/profile.d/conda.sh

# Deactivate the current Python environment
conda deactivate

# Activate the Python environment for the backend module
conda activate sapphire_preprocessing_discharge

# Run the tests for the backend module
SAPPHIRE_TEST_ENV=True pytest preprocessing_runoff/test

# Deactivate the Python environment for the backend module
conda deactivate

# Activate the Python environment for the reset_forecast_run_date module
conda activate sapphire_reset_rundate

# Run the tests for the reset_forecast_run_date module
SAPPHIRE_TEST_ENV=True pytest reset_forecast_run_date/tests

# Deactivate the Python environment for the reset_forecast_run_date module
conda deactivate

# Activate the Python environment for the forecast_dashboard module
conda activate sapphire_dashboard

# Run the tests for the forecast_dashboard module
SAPPHIRE_TEST_ENV=True pytest forecast_dashboard/tests

# Deactivate the Python environment for the forecast_dashboard module
conda deactivate

# Activate the Python environment for the iEasyHydroForecast module
conda activate sapphire_ieasyhydroforecast

# Run the tests for the iEasyHydroForecast module
SAPPHIRE_TEST_ENV=True python -m unittest discover -s iEasyHydroForecast/tests -p 'test_*.py'

# Deactivate the Python environment for the iEasyHydroForecast module
conda deactivate