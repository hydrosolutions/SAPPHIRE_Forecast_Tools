#!/bin/bash

# This script runs the SAPPHIRE forecast tools in local deployment mode
# Working directory if the root of the repository, i.e. SAPPHIRE_forecast_tools
#
# Useage: bash bin/run_ubuntu.sh <ieasyhydroforecast_data_root_dir>

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
ieasyhydroforecast_data_root_dir=$1

# Clean up the docker workspace
source ./bin/clean_docker.sh

# Pull (deployment mode) or build (development mode) & push images
# bash ./bin/build_docker_images.sh latest
# bash ./bin/push_docker_images.sh latest  # ONLY allowed from amd64 architecture, i.e. not from M1/2/3 Macs
source ./bin/pull_docker_images.sh latest

# Establish SSH tunnel (if required)
source ../sensitive_data_forecast_tools/bin/.ssh/open_ssh_tunnel.sh

# Wait for 5 seconds
sleep 5

# Run the forecast tools
ieasyhydroforecast_data_root_dir=$ieasyhydroforecast_data_root_dir docker compose -f bin/docker-compose.yml up -d

# Close SSH tunnel (if required)
source ../sensitive_data_forecast_tools/bin/.ssh/close_ssh_tunnel.sh

# Clean up
#bash ./bin/clean_docker.sh