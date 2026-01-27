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

# Function to get current timestamp in seconds
# Works on both Linux and macOS
get_timestamp() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        date +%s
    else
        # Linux
        date +%s
    fi
}

# Get ieasyhydroforecast_env_file_path from env if set, otherwise use default
if [ -z "$ieasyhydroforecast_env_file_path" ]; then
    ieasyhydroforecast_env_file_path=~/Documents/GitHub/kyg_data_forecast_tools/config/.env_develop_kghm
fi
export ieasyhydroforecast_env_file_path

# Get SAPPHIRE_PREDICTION_MODE from env if set, otherwise use default
if [ -z "$SAPPHIRE_PREDICTION_MODE" ]; then
    SAPPHIRE_PREDICTION_MODE=DECAD
fi
export SAPPHIRE_PREDICTION_MODE


for model in TFT TIDE TSMIXER; do
#for model in TIDE; do

    # Export the model name to the environment
    export SAPPHIRE_MODEL_TO_USE="$model"

    for horizon in $SAPPHIRE_PREDICTION_MODE; do
    #for horizon in PENTAD; do
        echo "Running model $model for horizon $horizon"

        # Create a unique log file name
        log_file="logs/log_${model}_${horizon}_$(date +%Y%m%d_%H%M%S).log"

        # Get start time and save to log file
        start_time=$(get_timestamp)
        echo "Start time: $(date)" >> "$log_file"

        # Record the run times for nan dealing
        start_nan_time=$(get_timestamp)
        echo "Starting nan dealing, time: $(date)" >> "$log_file"
        python recalculate_nan_forecasts.py 2>&1 | tee -a "$log_file"
        end_nan_time=$(get_timestamp)
        echo "End nan dealing, time: $(date)" >> "$log_file"

        start_make_forecast_time=$(get_timestamp)
        echo "Starting make_forecast, time: $(date)" >> "$log_file"
        python make_forecast.py 2>&1 | tee -a "$log_file"
        end_make_forecast_time=$(get_timestamp)
        echo "End make_forecast, time: $(date)" >> "$log_file"

        # Record the start time for fill_ml_gaps
        start_fill_ml_gaps_time=$(get_timestamp)
        echo "Starting fill_ml_gaps, time: $(date)" >> "$log_file"
        python fill_ml_gaps.py 2>&1 | tee -a "$log_file"
        end_fill_ml_gaps_time=$(get_timestamp)
        echo "End fill_ml_gaps, time: $(date)" >> "$log_file"

        # Record the start time for add_new_station
        start_add_new_station_time=$(get_timestamp)
        echo "Starting add_new_station, time: $(date)" >> "$log_file"
        python add_new_station.py 2>&1 | tee -a "$log_file"
        end_add_new_station_time=$(get_timestamp)
        echo "End add_new_station, time: $(date)" >> "$log_file"

        # Get end time and save to log file
        end_time=$(get_timestamp)
        echo "End time: $(date)" >> "$log_file"

        # Calculate run times
        total_run_time=$((end_time - start_time))
        nan_time=$((end_nan_time - start_nan_time))
        forecast_time=$((end_make_forecast_time - start_make_forecast_time))
        gaps_time=$((end_fill_ml_gaps_time - start_fill_ml_gaps_time))
        station_time=$((end_add_new_station_time - start_add_new_station_time))

        # Log all the timing information
        echo "---" >> "$log_file"
        echo "Total run time: $total_run_time seconds" >> "$log_file"
        echo "Nan dealing time: $nan_time seconds" >> "$log_file"
        echo "Make forecast time: $forecast_time seconds" >> "$log_file"
        echo "Fill ml gaps time: $gaps_time seconds" >> "$log_file"
        echo "Add new station time: $station_time seconds" >> "$log_file"
        echo "---" >> "$log_file"

        # Extract the last 10 lines and append to a summary file
        echo "=== Last 10 lines of output for model=$model, horizon=$horizon ===" >> logs/summary.log
        get_last_lines "$log_file" 10 >> logs/summary.log
        echo -e "\n\n" >> logs/summary.log

        echo "Full log saved to $log_file"
        echo "Last 10 lines appended to logs/summary.log"
        echo "----------------------------------------"
    done
done

echo "All runs completed. Check logs/summary.log for the last 10 lines of each run."

