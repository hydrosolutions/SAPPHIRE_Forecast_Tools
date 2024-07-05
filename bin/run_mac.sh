#!/bin/bash

# This script runs the SAPPHIRE forecast tools in local deployment mode
# Working directory if the root of the repository, i.e. SAPPHIRE_forecast_tools
#
# Useage: source bin/run_mac.sh <ieasyhydroforecast_data_root_dir>

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
#export ieasyhydroforecast_data_root_dir=$1
export ieasyhydroforecast_data_root_dir=/Users/bea/Documents/GitHub
echo $ieasyhydroforecast_data_root_dir

# Clean up docker space
echo "Removing all containers and images"
ieasyhydroforecast_data_root_dir=$ieasyhydroforecast_data_root_dir source ./bin/clean_docker.sh

# Pull (deployment mode) or build (development mode) & push images
echo "Building with TAG=latest"
source ./bin/build_docker_images.sh latest
# bash ./bin/push_docker_images.sh latest  # ONLY allowed from amd64 architecture, i.e. not from M1/2/3 Macs
# bash ./bin/pull_docker_images.sh latest

# Establish SSH tunnel (if required)
source ../sensitive_data_forecast_tools/bin/.ssh/open_ssh_tunnel.sh

# Function to start the Docker Compose service
start_docker_compose() {
  echo "Starting Docker Compose service..."
  docker compose -f bin/docker-compose.yml up &
  DOCKER_COMPOSE_PID=$!
  echo "Docker Compose service started with PID $DOCKER_COMPOSE_PID"
}

# Trap to clean up processes on script exit
cleanup() {
  echo "Cleaning up..."
  if [ -n "$ieasyhydroforecast_ssh_tunnel_pid" ]; then
    kill $ieasyhydroforecast_ssh_tunnel_pid
  fi
  if [ -n "$DOCKER_COMPOSE_PID" ]; then
    docker compose -f bin/docker-compose.yml down
  fi
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

# Start the Docker Compose service
start_docker_compose

# Wait for Docker Compose service to finish
wait $DOCKER_COMPOSE_PID

# Additional actions to be taken after Docker Compose service stops
echo "Docker Compose service has finished running"

# Close SSH tunnel (if required)
#echo "Closing the SSH tunnel"
#source ../sensitive_data_forecast_tools/bin/.ssh/close_ssh_tunnel.sh

# Clean up
#bash ./bin/clean_docker.sh