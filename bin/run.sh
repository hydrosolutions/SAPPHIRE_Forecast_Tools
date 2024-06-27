#!/bin/bash

# This script runs the SAPPHIRE forecast tools in local deployment mode
# Working directory if the root of the repository, i.e. SAPPHIRE_forecast_tools
#
# Useage: bash bin/run.sh <ieasyhydroforecast_data_root_dir>

if test -z "$1"
then
      echo "Usage bash ./bin/run.sh ieasyhydroforecast_data_root_dir"
      echo "No tag was passed! Please pass the absolute path to your ieasyforecast data root directory to the script e.g. /Users/username/Documents/sapphire_data"
      exit 1
fi

# Parse argument
ieasyhydroforecast_data_root_dir=$1

# Pull (deployment mode) or build (development mode) & push images
# bash ./bin/build_docker_images.sh latest
# bash ./bin/push_docker_images.sh latest
# bash ./bin/pull_docker_images.sh latest

# Establish SSH tunnel (if required)
bash ../sensitive_data_forecast_tools/bin/.ssh/open_ssh_tunnel.sh

# Run the forecast tools
ieasyhydroforecast_data_root_dir=$ieasyhydroforecast_data_root_dir docker-compose -f bin/docker-compose.yml up

# Close SSH tunnel (if required)
bash ../sensitive_data_forecast_tools/bin/.ssh/close_ssh_tunnel.sh

# Clean up
bash ./bin/clean_docker.sh