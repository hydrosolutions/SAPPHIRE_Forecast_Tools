#!/bin/bash

# This script re-runs the latest forecast produced by the SAPPHIRE forecast tools.
#
# Useage:
# Run the script in your terminal:
# bash bin/rerun_latest_forecasts.sh <env_file_path>
# Run the script in the background:
# nohup bash bin/rerun_latest_forecasts.sh <env_file_path > output_rerun.log 2>&1 &
# note: nohup: no hangup, i.e. the process will not be terminated when the terminal is closed
# note: > output_rerun.log 2>&1: redirect stdout and stderr to a file called output_rerun.log
# note: &: run the process in the background
#
# Details: The script performs the following tasks:
# 1. Parse the argument <env_file_path> which is the absolute path to the ieasyforecast environment file
# 2. Clean up docker space (remove all superfluous containers and images)
# 3. Build the Docker images with the tag "latest"
# 4. Establish an SSH tunnel to the SAPPHIRE server
# 5. Start the Docker Compose service (start luigi daemon and the SAPPHIRE forecast tools)
# 6. Wait for the Docker Compose service to finish
# 7. Close the SSH tunnel
# 8. Down the Docker Compose service
#
# Note: The script uses the following helper scripts:
# 5. bin/docker-compose-luigi.yml
# 6. bin/.ssh/open_ssh_tunnel.sh
# 7. bin/.ssh/close_ssh_tunnel.sh
#
#
# Author: Beatrice Marti

# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"

# Print the banner
print_banner

# Read the configuration from the .env file
read_configuration $1

# Clean up docker space
clean_out_backend

# Establish SSH tunnel (if required)
establish_ssh_tunnel

# Set the trap to clean up processes on exit
trap cleanup EXIT

# Reset the run date
start_docker_container_reset_run_date

# Start the Docker Compose service for the forecasting pipeline
start_docker_compose_luigi

# Wait for forecasting pipeline to finish
wait $DOCKER_COMPOSE_LUIGI_PID

# Wait another 30 minutes
#echo "Waiting for 30 minutes before cleaning up..."
#sleep 1800

# Additional actions to be taken after Docker Compose service stops
echo "Docker Compose service has finished running"

# Close SSH tunnel (if required)
#echo "Closing the SSH tunnel"
#source ../sensitive_data_forecast_tools/bin/.ssh/close_ssh_tunnel.sh

# Clean up
#bash ./bin/clean_docker.sh