#!/bin/bash

# This script runs the SAPPHIRE forecast tools in local deployment mode
# Working directory if the root of the repository, i.e. SAPPHIRE_forecast_tools
#
# Useage: bash bin/run_sapphire_forecast_tools.sh <ieasyhydroforecast_data_root_dir>
#
# Details: The script performs the following tasks and takes 4 minutes on a
# reasonably fast machine with a good internet connection:
# 1. Parse the argument <ieasyhydroforecast_data_root_dir> which is the absolute path to the ieasyforecast data root directory
# 2. Clean up docker space (remove all containers and images)
# 3. Build the Docker images with the tag "latest"
# 4. Establish an SSH tunnel to the SAPPHIRE server
# 5. Start the Docker Compose service (start luigi daemon and the SAPPHIRE forecast tools)
# 6. Wait for the Docker Compose service to finish
# 7. Close the SSH tunnel
# 8. Down the Docker Compose service
#
# Note: The script uses the following helper scripts:
# 1. bin/clean_docker.sh
# 4. bin/pull_docker_images.sh
# 5. bin/docker-compose.yml
# 6. bin/.ssh/open_ssh_tunnel.sh
# 7. bin/.ssh/close_ssh_tunnel.sh


if test -z "$1"
then
      echo "Usage bash ./bin/run.sh ieasyhydroforecast_data_root_dir"
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
echo "Removing all containers and images"
ieasyhydroforecast_data_root_dir=$ieasyhydroforecast_data_root_dir source ./bin/clean_docker.sh

# Pull (deployment mode) or build (development mode) & push images
echo "Pulling with TAG=latest"
# source ./bin/build_docker_images.sh latest  # Only for development mode
# bash ./bin/push_docker_images.sh latest  # ONLY allowed from amd64 architecture, i.e. not from M1/2/3 Macs
source ./bin/pull_docker_images.sh latest

# Establish SSH tunnel (if required)
source ../sensitive_data_forecast_tools/bin/.ssh/open_ssh_tunnel.sh

# Function to start the Docker Compose service for the backend pipeline
start_docker_compose_luigi() {
  echo "Starting Docker Compose service for backend ..."
  docker compose -f bin/docker-compose-luigi.yml up -d &
  DOCKER_COMPOSE_LUIGI_PID=$!
  echo "Docker Compose service started with PID $DOCKER_COMPOSE_PID"
}

# Function to start the Docker Compose service for the dashboards
start_docker_compose_dashboards() {
  echo "Starting Docker Compose service for the dashboards..."
  docker compose -f bin/docker-compose-dashboards.yml up -d &
  DOCKER_COMPOSE_DASHBOARD_PID=$!
  echo "Docker Compose service started with PID $DOCKER_COMPOSE_PID"
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

# Start the Docker Compose service for the forecasting pipeline
start_docker_compose_luigi

# Start the Docker Compose service for the dashboards
start_docker_compose_dashboards

# Wait for forecasting pipeline to finish
wait $DOCKER_COMPOSE_LUIGI_PID

# Wait another 30 minutes
echo "Waiting for 30 minutes before cleaning up..."
sleep 1800

# Additional actions to be taken after Docker Compose service stops
echo "Docker Compose service has finished running"

# Close SSH tunnel (if required)
#echo "Closing the SSH tunnel"
#source ../sensitive_data_forecast_tools/bin/.ssh/close_ssh_tunnel.sh

# Clean up
#bash ./bin/clean_docker.sh