#! /bin/bash

# This script will run the machine learning forecasts locally for all available
# models. Note that this script is not used in operational mode.
#
# Need to be in folder apps/machine_learning
# All libraries in requirements.txt need to be installed
# preprocessing_runoff and preprocessing_gateway need to be run before this script

# How to run:
# bash locally_run_ml_forecasts.sh

# Details:
# We have 4 models available: TFT, TIDE, TSMIXER, ARIMA and 2 forecast horizons:
# PENTAD and DECAD. The script will run all models for both forecast horizons.

for model in TFT TIDE TSMIXER ARIMA; do
    for horizon in PENTAD DECAD; do
        echo "Running model $model for horizon $horizon"
        export SAPPHIRE_OPDEV_ENV=True SAPPHIRE_MODEL_TO_USE=$model SAPPHIRE_PREDICTION_MODE=$horizon
        python make_forecast.py && python fill_ml_gaps.py
    done
done

