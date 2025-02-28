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

# Function to get the last N lines from a file
get_last_lines() {
    local file=$1
    local lines=$2
    tail -n $lines "$file"
}


for model in TFT TIDE TSMIXER ARIMA; do
    for horizon in PENTAD DECAD; do
        echo "Running model $model for horizon $horizon"

        # Create a unique log file name
        log_file="logs/log_${model}_${horizon}_$(date +%Y%m%d_%H%M%S).log"

        export SAPPHIRE_OPDEV_ENV=True SAPPHIRE_MODEL_TO_USE=$model SAPPHIRE_PREDICTION_MODE=$horizon
        python make_forecast.py #&& python fill_ml_gaps.py && python add_new_station.py

        # Run commands and tee output to log file
        {
            echo "=== Running with model=$model, horizon=$horizon ==="
            echo "Command: python make_forecast.py && python fill_ml_gaps.py"
            echo "=== Output ==="
            python make_forecast.py && python fill_ml_gaps.py
        } 2>&1 | tee "$log_file"

        # Extract the last 30 lines and append to a summary file
        echo "=== Last 30 lines of output for model=$model, horizon=$horizon ===" >> logs/summary.log
        get_last_lines "$log_file" 30 >> logs/summary.log
        echo -e "\n\n" >> logs/summary.log

        echo "Full log saved to $log_file"
        echo "Last 30 lines appended to logs/summary.log"
        echo "----------------------------------------"
    done
done

echo "All runs completed. Check logs/summary.log for the last 30 lines of each run."

