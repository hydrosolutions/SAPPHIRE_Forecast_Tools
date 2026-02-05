#! /bin/bash

# Run the script: ieasyhydroforecast_env_file_path=<path/to/.env> bash initialize_models_local.sh

# Declare associative array to track results
declare -A results

run_lt_hparam_calibration() {
    local mode=$1
    
    # Print datetime and message to summary.log
    start_time=$(date +%s)
    start_time_readable=$(date)
    echo "$start_time_readable - Running long term forecasting module for mode: $mode" >> ../../summary.log
    
    # Activate the virtual environment
    #source /Users/sandrohunziker/Documents/sapphire_venvs/sapphire_lt_forecasting/bin/activate

    #pip install -e "/Users/sandrohunziker/hydrosolutions Dropbox/Sandro Hunziker/SAPPHIRE_Central_Asia_Technical_Work/code/machine_learning_hydrology/monthly_forecasting"
    # Change directory to the location of the script
    #cd ../../apps/long_term_forecasting


    # Run the script
    ieasyhydroforecast_env_file_path=$ieasyhydroforecast_env_file_path \
        lt_forecast_mode=$mode python calibrate_and_hindcast.py --models MC_ALD #--tune_hyperparameters
    

    # Capture exit status
    local exit_status=$?

    # Deactivate the virtual environment
    deactivate

    # Print datetime and message to summary.log
    end_time=$(date +%s)
    end_time_readable=$(date)
    # Calculate elapsed time
    elapsed_time=$((end_time - start_time))
    echo "Time taken to run long term forecasting module for mode $mode: $elapsed_time seconds" >> ../../summary.log  
    echo "$end_time_readable - Finished long term forecasting module for mode: $mode (exit status: $exit_status)" >> ../../summary.log

    return $exit_status
}

modes_to_run=("month_0" "month_1" "month_2" "month_3" "month_4" "month_5" "month_6" "month_7" "month_8" "month_9")
#modes_to_run=("month_4" "month_5" "month_6" "month_7" "month_8" "month_9")
#modes_to_run=("month_1" "month_2" "month_3" "month_4" "month_5" "month_6")
# Run calibration for each mode
for mode in "${modes_to_run[@]}"; do
    echo "=========================================="
    echo "Running mode: $mode"
    echo "=========================================="
    
    if run_lt_hparam_calibration "$mode"; then
        results[$mode]="SUCCESS"
    else
        results[$mode]="FAILED"
    fi
done

# Print summary
echo ""
echo "=========================================="
echo "SUMMARY OF RESULTS"
echo "=========================================="
for mode in "${modes_to_run[@]}"; do
    echo "$mode: ${results[$mode]}"
done
echo "=========================================="

# Also append summary to log file
echo "" >> ../../summary.log
echo "=========================================" >> ../../summary.log
echo "SUMMARY OF RESULTS" >> ../../summary.log
echo "=========================================" >> ../../summary.log
for mode in "${modes_to_run[@]}"; do
    echo "$mode: ${results[$mode]}" >> ../../summary.log
done
echo "=========================================" >> ../../summary.log