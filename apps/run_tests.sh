#!/usr/bin/env bash

# This script runs the tests for all the modules in the Sapphire project.
# It assumes that the conda.sh script is located in the etc/profile.d directory
# of the Anaconda installation. Replace /path/to/conda with the path to your
# Anaconda installation. The script also assumes that the Python environments
# for the preprocessing, linear regression, reset_forecast_run_date, and
# forecast_dashboard modules are named sapphire_preprocessing_discharge,
# sapphire_linear_regression, sapphire_reset_rundate, and sapphire_dashboard,
# respectively. Replace the environment names with the names of the environments
# you created.
#
# Useage:
# cd to the apps directory and run the script with the following command:
# $ bash run_tests.sh


# Source the conda.sh script to initialize the shell with conda
# Replace /path/to/conda with the path to your conda installation
source /Users/bea/anaconda3/etc/profile.d/conda.sh

# Deactivate the current Python environment
conda deactivate

# Activate the Python environment for the preprocessing module
# Replace the environment name with the name of the environment you created
conda activate sapphire_preprocessing_discharge

# Run the tests for the preprocessing module
SAPPHIRE_TEST_ENV=True pytest preprocessing_runoff/test
# SAPPHIRE_TEST_ENV=True pytest postprocessing_forecasts/test  # Currently no test here

# Deactivate the Python environment for the preprocessing module
conda deactivate

# Activate the Python environment for the linear regression module
conda activate sapphire_linear_regression

# Run the tests for the linear regression module
SAPPHIRE_TEST_ENV=True pytest linear_regression/test

# Deactivate the Python environment for the linear regression module
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