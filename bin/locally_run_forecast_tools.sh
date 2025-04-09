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
# Run the script: ieasyhydroforecast_env_file_path=<path/to/.env> bash bin/locally_run_forecast_tools.sh

# Define a function to run the preprocessing-runoff module
run_preprocessing_runoff() {
    # Print datetime and message to summary.log
    start_time=$(date +%s)
    start_time_readable=$(date)
    echo "-------------------------------" >> summary.log
    echo "$start_time_readable - Running preprocessing-runoff module" >> summary.log
    
    # Activate the virtual environment
    conda activate sapphire_preprocessing_discharge

    # Change directory to the location of the script
    cd ./apps/preprocessing_runoff

    # Run the script
    echo "pwd: $(pwd)" >> ../../summary.log
    ieasyhydroforecast_env_file_path=$ieasyhydroforecast_env_file_path python preprocessing_runoff.py

    # Deactivate the virtual environment
    conda deactivate

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
    conda activate sapphire_qmap

    # Change directory to the location of the script
    cd ../../apps/preprocessing_gateway

    # Run the script
    ieasyhydroforecast_env_file_path=$ieasyhydroforecast_env_file_path python Quantile_Mapping_OP.py
    ieasyhydroforecast_env_file_path=$ieasyhydroforecast_env_file_path python extend_era5_reanalysis.py
    ieasyhydroforecast_env_file_path=$ieasyhydroforecast_env_file_path python snow_data_operational.py

    # Deactivate the virtual environment
    conda deactivate

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
    ieasyhydroforecast_env_file_path=$ieasyhydroforecast_env_file_path python linear_regression.py

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
    SAPPHIRE_OPDEV_ENV=True Rscript run_operation_forecasting_CM.R

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
    ieasyhydroforecast_env_file_path=$ieasyhydroforecast_env_file_path python postprocessing_forecasts.py

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

# Source the Conda initialization script
source ~/anaconda3/etc/profile.d/conda.sh

# -- Preprocessing runoff data --
run_preprocessing_runoff

# -- Preprocessing gateway data
run_preprocessing_gateway

# -- Run the forecast models --
run_linear_regression
run_conceptual_model
run_machine_learning_models

# -- Postprocessing --
run_postprocessing