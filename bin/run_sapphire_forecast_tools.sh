#!/bin/bash

# This script runs the SAPPHIRE forecast tools in local daily deployment mode
# Working directory is the root of the repository, i.e. SAPPHIRE_forecast_tools
#
# Useage:
# Run the script in your terminal:
# bash bin/run_sapphire_forecast_tools.sh <env_file_path>
# Run the script in the background:
# nohup bash bin/run_sapphire_forecast_tools.sh <env_file_path> > output.log 2>&1 &
# note: nohup: no hangup, i.e. the process will not be terminated when the terminal is closed
# note: > output.log 2>&1: redirect stdout and stderr to a file called output.log
# note: &: run the process in the background
#
# Details: The script performs the following tasks and takes 4 minutes on a
# reasonably fast machine with a good internet connection:
# 1. Parse the argument <env_file_path> which is the absolute path to the .env
#    file containing your environment variables for the SAPPHIRE forecast tools
#    and derive the ieasyhydroforecast_data_root_dir and ieasyhydroforecast_data_ref_dir
#    from the env_file_path.
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
# 5. bin/docker-compose-luigi.yml and bin/docker-compose-dashboards.yml
# 6. bin/.ssh/open_ssh_tunnel.sh
# 7. bin/.ssh/close_ssh_tunnel.sh
#
# Note: The script assumes the location of the .env file  and the .ssh directory
# in the ieasyforecast data reference directory. The ieasyhydroforecast data
# root directory is assumed to be one level above the ieasyforecast data reference
# directory.
# Assumed data directory sturcture:
# ieasyhydroforecast_data_root_dir
#   |- SAPPHIRE_forecast_tools
#       |- apps
#       |- bin
#       |- ...
#   |- ieasyhydroforecast_data_ref_dir
#       |- config
#           |- .env  # <- env_file_path
#           |- ...
#       |- bin
#           |- .ssh
#               |- open_ssh_tunnel.sh
#               |- close_ssh_tunnel.sh
#           |- ...
#       |- ...
#
# Author: Beatrice Marti

# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"

# Print the banner
print_banner

# Read the configuration from the .env file
read_configuration $1

# Clean up backend containers
clean_out_backend

# Pull docker images
pull_docker_images

# Establish SSH tunnel (if required)
establish_ssh_tunnel

# Set the trap to clean up processes on exit
trap cleanup EXIT

export SAPPHIRE_PREDICTION_MODE
# Start the Docker Compose service for the forecasting pipeline
start_docker_compose_luigi

# Wait for forecasting pipeline to finish
wait $DOCKER_COMPOSE_LUIGI_PID

# Additional actions to be taken after Docker Compose service stops
echo "Docker Compose service for backend has finished running"


