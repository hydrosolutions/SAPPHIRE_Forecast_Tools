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

echo "  ____    _    ____  ____  _   _ ___ ____  _____                "
echo " / ___|  / \  |  _ \|  _ \| | | |_ _|  _ \| ____|               "
echo " \___ \ / _ \ | |_) | |_) | |_| || || |_) |  _|                 "
echo "  ___) / ___ \|  __/|  __/|  _  || ||  _ <| |___                "
echo " |____/_/   \_\_|   |_|   |_| |_|___|_| \_\_____|        _      "
echo " |  ___|__  _ __ ___  ___ __ _ ___| |_  |_   _|__   ___ | |___  "
echo " | |_ / _ \| '__/ _ \/ __/ _\` / __| __|   | |/ _ \ / _ \| / __| "
echo " |  _| (_) | | |  __/ (_| (_| \__ \ |_    | | (_) | (_) | \__ \ "
echo " |_|  \___/|_|  \___|\___\__,_|___/\__|   |_|\___/ \___/|_|___/ "
echo "                                                                "
echo "Re-running latest forecasts with the SAPPHIRE forecast tools ..."
echo "Date: $(date '+%Y-%m-%d %H:%M:%S %Z')"

# If the argument is provided, write it to the environment variable
# ieasyhydroforecast_env_file_path. If not, check if the environment variable
# is set. If not, throw an error.
if [ -n "$1" ];
then
      env_file_path=$1
      container_env_file_path=/$(keep_last_three_elements "$env_file_path")
      # Test if there is a ieasyhydroforecast_env_file_path variable set
      if [ -z "$ieasyhydroforecast_env_file_path" ];
      then
            # Test if the new env_file_path is different from the old one
            if [ "$ieasyhydroforecast_env_file_path" != "$env_file_path" ];
            then
                  echo "WARNING: Updating ieasyhydroforecast_env_file_path\n   from $ieasyhydroforecast_env_file_path\n   to $container_env_file_path"
            fi
      fi
      # For use by the forecast tools (inside docker containers) we need to know
      # the env file path inside the docker containers as well.
      export ieasyhydroforecast_env_file_path=$container_env_file_path
      echo "Local path to .env read from argument: $env_file_path"
      echo "Container path to .env derived: $ieasyhydroforecast_env_file_path"
else
      echo "Error: No path to .env file was passed or was found in the environment!"
      exit 1
fi

# Derive ieasyhydroforecast_data_root_dir by removing the filename and 2 folder hierarchies
ieasyhydroforecast_data_root_dir=$(dirname "$env_file_path")
ieasyhydroforecast_data_ref_dir=$(dirname "$ieasyhydroforecast_data_root_dir")
ieasyhydroforecast_data_root_dir=$(dirname "$ieasyhydroforecast_data_ref_dir")

echo "ieasyhydroforecast_data_ref_dir: $ieasyhydroforecast_data_ref_dir"
echo "ieasyhydroforecast_data_root_dir: $ieasyhydroforecast_data_root_dir"
export ieasyhydroforecast_data_ref_dir
export ieasyhydroforecast_data_root_dir

# Load environment variables from the specified .env file
if [ -f "$env_file_path" ]; then
    source "$env_file_path"
else
    echo ".env file not found at $env_file_path!"
    exit 1
fi

# Clean up docker space
echo "Removing all superfluous containers and images"
docker compose -f bin/docker-compose-luigi.yml down

# Function to stop and remove a container if it exists
stop_and_remove_container() {
    container_name=$1
    if [ "$(docker ps -q -f name=$container_name)" ]; then
        docker stop $container_name
    fi
    if [ "$(docker ps -a -q -f name=$container_name)" ]; then
        docker rm $container_name
    fi
}
# List all containers that may be called in the pipeline
stop_and_remove_container preprunoff
stop_and_remove_container prepgateway
stop_and_remove_container linreg
stop_and_remove_container postprocessing

# Establish SSH tunnel (if required)
source $ieasyhydroforecast_data_ref_dir/bin/.ssh/open_ssh_tunnel.sh

# Function to start the Docker Compose service for the backend pipeline
start_docker_compose_luigi() {
  echo "Starting Docker Compose service for backend ..."
  docker compose -f bin/docker-compose-luigi.yml up -d &
  DOCKER_COMPOSE_LUIGI_PID=$!
  echo "Docker Compose service started with PID $DOCKER_COMPOSE_LUIGI_PID"
}

# Function to start the Docker container to re-set the run date
start_docker_container_reset_run_date() {
  echo "Starting Docker container to re-set the run date ..."
  docker run -d \
    -e SAPPHIRE_OPDEV_ENV=True \
    --name resetrundate \
    --network host \
    -v $ieasyhydroforecast_data_ref_dir/config:/sensitive_data_forecast_tools/config \
    -v $ieasyhydroforecast_data_ref_dir/intermediate_data:/sensitive_data_forecast_tools/intermediate_data \
    mabesa/sapphire-rerun:latest
  echo "Docker container started with name resetrundate"
}

# Trap to clean up processes on script exit
cleanup() {
  echo "Cleaning up..."
  if [ -n "$ieasyhydroforecast_ssh_tunnel_pid" ]; then
    kill $ieasyhydroforecast_ssh_tunnel_pid
  fi
  #if [ -n "$DOCKER_COMPOSE_PID" ]; then
    # Keep dashboards up and running: comment out the following line
    #docker compose -f bin/docker-compose.yml down
  #fi
}

# Set the trap to clean up processes on exit
trap cleanup EXIT

# Check for SSH tunnel availability
echo "Checking for SSH tunnel availability"
until nc -z localhost 8881; do
  echo "SSH tunnel is not available yet. Waiting..."
  sleep 1
done
echo "SSH tunnel is available."
echo "PID of ssh tunnel is $ieasyhydroforecast_ssh_tunnel_pid"

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