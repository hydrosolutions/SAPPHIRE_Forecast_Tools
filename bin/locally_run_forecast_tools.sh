#! /bin/bash

# Run from project root SAPPHIRE_Forecast_Tools

# Establish ssh conncetion to server

# -- Preprocessing runoff data --
# Activate the virtual environment
conda activate sapphire_preprocessing_discharge

# Change directory to the location of the script
cd ./apps/preprocessing_runoff

# Run the script
SAPPHIRE_OPDEV_ENV=True python preprocessing_runoff.py

# Deactivate the virtual environment
conda deactivate

# -- Preprocessing gateway data
# Activate the virtual environment
conda activate sapphire_qmap

# Change directory to the location of the script
cd ../preprocessing_gateway

# Run the script
SAPPHIRE_OPDEV_ENV=True python Quantile_Mapping_OP.py
SAPPHIRE_OPDEV_ENV=True python extend_era5_reanalysis.py

# Deactivate the virtual environment
conda deactivate

# -- Run the forecast models --
# Activate the virtual environment
conda activate sapphire_linear_regression

# Change directory to the location of the script
cd ../linear_regression

# Run the script
SAPPHIRE_OPDEV_ENV=True python linear_regression.py

# Deactivate the virtual environment
conda deactivate

# -- Conceptual model --
# Activate the virtual environment
conda activate hsol_py310

# Change directory to the location of the script
cd ../conceptual_model

# Run the script
SAPPHIRE_OPDEV_ENV=True Rscript run_operation_forecasting_CM.R

# Deactivate the virtual environment
conda deactivate

# -- Machine learning model --
# Activate the virtual environment
conda activate sapphire_ml_3.11

# Change directory to the location of the script
cd ../machine_learning

# Run the script
bash locally_run_ml_forecasts.sh

# Deactivate the virtual environment
conda deactivate

# -- Postprocessing --
# Activate the virtual environment
conda activate sapphire_preprocessing_discharge

# Change directory to the location of the script
cd ../postprocessing_forecasts

# Run the script
SAPPHIRE_OPDEV_ENV=True python postprocessing_forecasts.py

# Deactivate the virtual environment
conda deactivate

