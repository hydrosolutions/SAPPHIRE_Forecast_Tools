#!/bin/bash
#==============================================================================
# SAPPHIRE LOCAL FORECAST EXECUTION
#==============================================================================
# Description:
#   This script runs the SAPPHIRE forecasting workflow components directly on
#   the server in their respective conda environments, bypassing Docker
#   containerization. It's primarily used for development and debugging purposes
#   when direct access to the execution environment is required.
#
# Usage:
#   bash bin/locally_run_forecast_tools.sh
#
# Requirements:
#   - Must be run from the project root directory (SAPPHIRE_forecast_tools)
#   - Requires an active SSH connection to the server
#   - All conda environments must be pre-installed
#   - Anaconda/Miniconda must be installed at ~/anaconda3
#
# Process:
#   The script executes the following steps in sequence:
#   1. Preprocessing runoff data (sapphire_preprocessing_discharge env)
#   2. Preprocessing gateway data (sapphire_qmap env)
#   3. Linear regression forecasting (sapphire_linear_regression env)
#   4. Conceptual model forecasting (hsol_py310 env)
#   5. Machine learning model forecasting (sapphire_ml_3.11 env)
#   6. Forecast postprocessing (sapphire_preprocessing_discharge env)
#
# Conda Environments:
#   - sapphire_preprocessing_discharge: Data preprocessing and postprocessing
#   - sapphire_qmap: Quantile mapping and ERA5 reanalysis
#   - sapphire_linear_regression: Statistical forecasting models
#   - hsol_py310: Conceptual hydrological modeling
#   - sapphire_ml_3.11: Machine learning forecasting models
#
# Note:
#   This script is intended for development and debugging purposes only.
#   For operational forecasting, use the Docker-based workflow scripts.
#
# Contributors:
#   - Beatrice Marti: Implementation and integration
#   - Developed with assistance from AI pair programming tools
#==============================================================================

# Source the Conda initialization script
echo "| Initializing Conda environment"
source ~/anaconda3/etc/profile.d/conda.sh

#==============================================================================
# 1. PREPROCESSING RUNOFF DATA
#==============================================================================
echo "| 1. Starting runoff data preprocessing"
# Activate the virtual environment
conda activate sapphire_preprocessing_discharge

# Change directory to the location of the script
cd ./apps/preprocessing_runoff

# Run the script
SAPPHIRE_OPDEV_ENV=True python preprocessing_runoff.py

# Deactivate the virtual environment
conda deactivate
echo "| ✓ Runoff preprocessing complete"

#==============================================================================
# 2. PREPROCESSING GATEWAY DATA
#==============================================================================
echo "| 2. Starting gateway data preprocessing"
# Activate the virtual environment
conda activate sapphire_qmap

# Change directory to the location of the script
cd ../preprocessing_gateway

# Run the script
SAPPHIRE_OPDEV_ENV=True python Quantile_Mapping_OP.py
SAPPHIRE_OPDEV_ENV=True python extend_era5_reanalysis.py

# Deactivate the virtual environment
conda deactivate
echo "| ✓ Gateway preprocessing complete"

#==============================================================================
# 3. LINEAR REGRESSION FORECASTING
#==============================================================================
echo "| 3. Running linear regression models"
# Activate the virtual environment
conda activate sapphire_linear_regression

# Change directory to the location of the script
cd ../linear_regression

# Run the script
SAPPHIRE_OPDEV_ENV=True python linear_regression.py

# Deactivate the virtual environment
conda deactivate
echo "| ✓ Linear regression complete"

#==============================================================================
# 4. CONCEPTUAL MODEL FORECASTING
#==============================================================================
echo "| 4. Running conceptual hydrological model"
# Activate the virtual environment
conda activate hsol_py310

# Change directory to the location of the script
cd ../conceptual_model

# Run the script
SAPPHIRE_OPDEV_ENV=True Rscript run_operation_forecasting_CM.R

# Deactivate the virtual environment
conda deactivate
echo "| ✓ Conceptual model complete"

#==============================================================================
# 5. MACHINE LEARNING FORECASTING
#==============================================================================
echo "| 5. Running machine learning models"
# Activate the virtual environment
conda activate sapphire_ml_3.11

# Change directory to the location of the script
cd ../machine_learning

# Run the script
bash locally_run_ml_forecasts.sh

# Deactivate the virtual environment
conda deactivate
echo "| ✓ Machine learning models complete"

#==============================================================================
# 6. POSTPROCESSING FORECASTS
#==============================================================================
echo "| 6. Starting forecast postprocessing"
# Activate the virtual environment
conda activate sapphire_preprocessing_discharge

# Change directory to the location of the script
cd ../postprocessing_forecasts

# Run the script
SAPPHIRE_OPDEV_ENV=True python postprocessing_forecasts.py

# Deactivate the virtual environment
conda deactivate
echo "| ✓ Forecast postprocessing complete"

echo "| All SAPPHIRE forecasting components completed successfully"
exit 0