#! /bin/bash

# This script runs the forecast tools locally on the server.
#
# Details:
# The script activates the virtual environments, runs the scripts, and
# deactivates the virtual environments.
#
# How to run
# CD to project root SAPPHIRE_Forecast_Tools
# Establish ssh conncetion to server
# Run the script: SAPPHIRE_PREDICTION_MODE=PENTAD ieasyhydroforecast_env_file_path=<path/to/.env> bash bin/locally_run_forecast_tools.sh

# Pass SAPPHIRE_PREDICTION_MODE to all subprocesses if set
if [ ! -z "$SAPPHIRE_PREDICTION_MODE" ]; then
    export SAPPHIRE_PREDICTION_MODE
fi

# Define a function to run the preprocessing-runoff module
run_preprocessing_runoff() {
    # Print datetime and message to summary.log
    start_time=$(date +%s)
    start_time_readable=$(date)
    echo "-------------------------------" >> summary.log
    echo "$start_time_readable - Running preprocessing-runoff module" >> summary.log
    
    # Activate the virtual environment
    #conda activate sapphire_preprocessing_discharge
    source /Users/sandrohunziker/Documents/environments/preprocessing_runoff/bin/activate 
    # Change directory to the location of the script
    cd ./apps/preprocessing_runoff


    # Run the script
    echo "pwd: $(pwd)" >> ../../summary.log
    SAPPHIRE_PREDICTION_MODE=$SAPPHIRE_PREDICTION_MODE ieasyhydroforecast_env_file_path=$ieasyhydroforecast_env_file_path python preprocessing_runoff.py

    # Deactivate the virtual environment
    #conda deactivate
    deactivate

    # Print datetime and message to summary.log
    end_time=$(date +%s)
    end_time_readable=$(date)
    # Calculate elapsed time
    elapsed_time=$((end_time - start_time))
    echo "Time taken to run preprocessing-runoff module: $elapsed_time seconds" >> ../../summary.log  
    echo "$end_time_readable - Finished preprocessing-runoff module" >> ../../summary.log
}

run_preprocessing_gateway() {
    # Print datetime and message to summary.log
    start_time=$(date +%s)
    start_time_readable=$(date)
    echo "$start_time_readable - Running preprocessing-gateway module" >> ../../summary.log
    
    # Activate the virtual environment
    #conda activate sapphire_qmap
    source /Users/sandrohunziker/Documents/environments/sapphire_gateway/bin/activate

    # Change directory to the location of the script
    cd ../../apps/preprocessing_gateway

    # Run the script
    ieasyhydroforecast_env_file_path=$ieasyhydroforecast_env_file_path python Quantile_Mapping_OP.py
    ieasyhydroforecast_env_file_path=$ieasyhydroforecast_env_file_path python extend_era5_reanalysis.py
    ieasyhydroforecast_env_file_path=$ieasyhydroforecast_env_file_path python snow_data_operational.py

    # Deactivate the virtual environment
    #conda deactivate
    deactivate

    # Print datetime and message to summary.log
    end_time=$(date +%s)
    end_time_readable=$(date)
    # Calculate elapsed time
    elapsed_time=$((end_time - start_time))
    echo "Time taken to run preprocessing-gateway module: $elapsed_time seconds" >> ../../summary.log  
    echo "$end_time_readable - Finished preprocessing-gateway module" >> ../../summary.log
}

run_linear_regression() {
    # Print datetime and message to summary.log
    start_time=$(date +%s)
    start_time_readable=$(date)
    echo "$start_time_readable - Running linear-regression module" >> ../../summary.log
    
    # Activate the virtual environment
    conda activate sapphire_linear_regression

    # Change directory to the location of the script
    cd ../../apps/linear_regression

    # Run the script
    SAPPHIRE_PREDICTION_MODE=$SAPPHIRE_PREDICTION_MODE ieasyhydroforecast_env_file_path=$ieasyhydroforecast_env_file_path python linear_regression.py

    # Deactivate the virtual environment
    conda deactivate

    # Print datetime and message to summary.log
    end_time=$(date +%s)
    end_time_readable=$(date)
    # Calculate elapsed time
    elapsed_time=$((end_time - start_time))
    echo "Time taken to run linear-regression module: $elapsed_time seconds" >> ../../summary.log  
    echo "$end_time_readable - Finished linear-regression module" >> ../../summary.log
}

run_conceptual_model() {
    # Print datetime and message to summary.log
    start_time=$(date +%s)
    start_time_readable=$(date)
    echo "$start_time_readable - Running conceptual-model module" >> ../../summary.log
    
    # Activate the virtual environment
    conda activate hsol_py310

    # Change directory to the location of the script
    cd ../../apps/conceptual_model

    # Run the script
    ieasyhydroforecast_env_file_path=$ieasyhydroforecast_env_file_path Rscript run_operation_forecasting_CM.R

    # Deactivate the virtual environment
    conda deactivate

    # Print datetime and message to summary.log
    end_time=$(date +%s)
    end_time_readable=$(date)
    # Calculate elapsed time
    elapsed_time=$((end_time - start_time))
    echo "Time taken to run conceptual-model module: $elapsed_time seconds" >> ../../summary.log  
    echo "$end_time_readable - Finished conceptual-model module" >> ../../summary.log
}

run_machine_learning_models() {
    # Print datetime and message to summary.log
    start_time=$(date +%s)
    start_time_readable=$(date)
    echo "$start_time_readable - Running machine-learning module" >> ../../summary.log
    
    # Activate the virtual environment
    conda activate sapphire_ml_3.11

    # Change directory to the location of the script
    cd ../../apps/machine_learning

    # Run the script
    echo "pwd: $(pwd)" >> ../../summary.log
    export ieasyhydroforecast_env_file_path=$ieasyhydroforecast_env_file_path 
    export SAPPHIRE_PREDICTION_MODE=$SAPPHIRE_PREDICTION_MODE 
    bash locally_run_ml_forecasts.sh

    # Deactivate the virtual environment
    conda deactivate

    # Print datetime and message to summary.log
    end_time=$(date +%s)
    end_time_readable=$(date)
    # Calculate elapsed time
    elapsed_time=$((end_time - start_time))
    echo "Time taken to run machine-learning module: $elapsed_time seconds" >> ../../summary.log  
    echo "$end_time_readable - Finished machine-learning module" >> ../../summary.log
}

run_postprocessing() {
    # Print datetime and message to summary.log
    start_time=$(date +%s)
    start_time_readable=$(date)
    echo "$start_time_readable - Running postprocessing module" >> ../../summary.log
    
    # Activate the virtual environment
    conda activate sapphire_preprocessing_discharge

    # Change directory to the location of the script
    cd ../../apps/postprocessing_forecasts

    # Run the script
    SAPPHIRE_PREDICTION_MODE=$SAPPHIRE_PREDICTION_MODE ieasyhydroforecast_env_file_path=$ieasyhydroforecast_env_file_path python postprocessing_forecasts.py

    # Deactivate the virtual environment
    conda deactivate

    # Print datetime and message to summary.log
    end_time=$(date +%s)
    end_time_readable=$(date)
    # Calculate elapsed time
    elapsed_time=$((end_time - start_time))
    echo "Time taken to run postprocessing module: $elapsed_time seconds" >> ../../summary.log  
    echo "$end_time_readable - Finished postprocessing module" >> ../../summary.log
}


run_lt_forecasting() {
    # Print datetime and message to summary.log
    start_time=$(date +%s)
    start_time_readable=$(date)
    echo "$start_time_readable - Running long term forecasting module" >> ../../summary.log
    
    # Activate the virtual environment
    source /Users/sandrohunziker/Documents/environments/sapphire_lt_forecasting/.venv/bin/activate

    #pip install -e "/Users/sandrohunziker/hydrosolutions Dropbox/Sandro Hunziker/SAPPHIRE_Central_Asia_Technical_Work/code/machine_learning_hydrology/monthly_forecasting"
    # Change directory to the location of the script
    #cd ./apps/preprocessing_runoff
    cd ../../apps/long_term_forecasting

    # Run forecasts for each month, continue even if one fails
    for month in  1 2 3 4 5 6 7 8 9; do
        echo "Running forecast for month_$month" >> ../../summary.log
        if ! ieasyhydroforecast_env_file_path=$ieasyhydroforecast_env_file_path lt_forecast_mode=month_$month python run_forecast.py --all; then
            echo "WARNING: month_$month forecast failed, continuing with next month" >> ../../summary.log
        fi
    done
    #ieasyhydroforecast_env_file_path=$ieasyhydroforecast_env_file_path lt_forecast_mode=monthly python dev_investigate_data.py --all
    
    # Deactivate the virtual environment
    deactivate

    # Print datetime and message to summary.log
    end_time=$(date +%s)
    end_time_readable=$(date)
    # Calculate elapsed time
    elapsed_time=$((end_time - start_time))
    echo "Time taken to run long term forecasting module: $elapsed_time seconds" >> ../../summary.log  
    echo "$end_time_readable - Finished long term forecasting module" >> ../../summary.log

}

# Source the Conda initialization script


# -- Preprocessing runoff data --
run_preprocessing_runoff

# -- Preprocessing gateway data
run_preprocessing_gateway

# long term forecasting
run_lt_forecasting

# -- Run the forecast models --
#run_linear_regression
#run_conceptual_model
#run_machine_learning_models

# -- Postprocessing --
#run_postprocessing