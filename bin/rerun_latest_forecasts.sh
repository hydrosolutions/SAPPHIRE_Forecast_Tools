#!/bin/bash

# This script re-runs the latest forecast produced by the SAPPHIRE forecast tools.
#
# Useage:
# Run the script in your terminal:
# bash bin/rerun_latest_forecasts.sh <ieasyhydroforecast_data_root_dir>
# Run the script in the background:
# nohup bash bin/rerun_latest_forecasts.sh <ieasyhydroforecast_data_root_dir > output_rerun.log 2>&1 &
# note: nohup: no hangup, i.e. the process will not be terminated when the terminal is closed
# note: > output_rerun.log 2>&1: redirect stdout and stderr to a file called output_rerun.log
# note: &: run the process in the background
#
# Details: The script performs the following tasks:
# 1. Parse the argument <ieasyhydroforecast_data_root_dir> which is the absolute path to the ieasyforecast data root directory
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



if test -z "$1"
then
      echo "Usage bash ./bin/rerun_latest_forecasts.sh ieasyhydroforecast_data_root_dir"
      echo "No tag was passed!"
      echo "Please pass the absolute path to your ieasyforecast data root directory to the script"
      echo "e.g. /Users/username/Documents/sapphire_data"
      echo "Typically you get the directory with pwd (print working directory) command"
      echo "from the terminal when you are in the ieasyforecast data root directory"
      echo "then go one directory up and copy the path"
      exit 1
fi

# Parse argument
export ieasyhydroforecast_data_root_dir=$1
echo $ieasyhydroforecast_data_root_dir

# Clean up docker space
echo "Removing all superfluous containers and images"
docker compose -f bin/docker-compose-luigi.yml down
# Module pre-processing runoff
docker stop preprunoff
docker rm preprunoff
# Module pre-processing meteorological data from data gateway
docker stop prepgateway
docker rm prepgateway
# Module Linear regression
docker stop linreg
docker rm linreg
# Module post-processing of forecasts
docker stop postprocessing
docker rm postprocessing

# Establish SSH tunnel (if required)
source ../sensitive_data_forecast_tools/bin/.ssh/open_ssh_tunnel.sh

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
    -v $ieasyhydroforecast_data_root_dir/sensitive_data_forecast_tools/config:/sensitive_data_forecast_tools/config \
    -v $ieasyhydroforecast_data_root_dir/sensitive_data_forecast_tools/intermediate_data:/sensitive_data_forecast_tools/intermediate_data \
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