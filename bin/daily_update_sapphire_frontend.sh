#!/bin/bash

# This script updates the dashboards of the SAPPHIRE forecast tools.
#
# Useage:
# Run the script in your terminal:
# bash bin/daily_update_sapphire_frontend.sh <env_file_path>
#
# argument <env_file_path> is the absolute path to the .env file containing your
# environment variables for the SAPPHIRE forecast tools. The argument is optional.
# If it is not passed the script will look for the <env_file_path> in the
# environment variale ieasyhydroforecast_env_file_path and throw an error if it
# is not set.
#
# Details: The script performs the following tasks:
# 1. Parse the argument <env_file_path> which is the absolute path to the ieasyforecast
#    environment file
# 2. Stop the dashboards if they are running
# 3. Pull the Docker images with the tag "latest"
# 4. Start the Docker Compose service for the dashboards
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

# Taking down the dashboards if they are running
echo "| Stoping the dashboards if they are running"
docker compose -f bin/docker-compose-dashboards.yml down

# Remove unused conatiners
echo "| Removing unused containers"
docker container prune -f

# Pullling the images with the tag $ieasyhydroforecast_frontend_docker_image_tag
echo "| Pulling with TAG=$ieasyhydroforecast_frontend_docker_image_tag"
#docker pull mabesa/sapphire-configuration:$ieasyhydroforecast_frontend_docker_image_tag
docker pull mabesa/sapphire-dashboard:$ieasyhydroforecast_frontend_docker_image_tag

# Removing old images
echo "| Removing old images"
docker image prune -f

# Start the Docker Compose service for the dashboards
echo "| Path to .ssh directory: $ieasyhydroforecast_container_data_ref_dir/bin/.ssh"
ieasyhydroforecast_container_data_ref_dir=$ieasyhydroforecast_container_data_ref_dir start_docker_compose_dashboards

# Wait for dashboards to finish
wait $DOCKER_COMPOSE_DASHBOARD_PID

echo "Frontend update completed!"
