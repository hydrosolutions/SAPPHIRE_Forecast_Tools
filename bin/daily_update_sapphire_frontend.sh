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
echo "Updating the frontend of the SAPPHIRE forecast tools ..."
echo "Date: $(date '+%Y-%m-%d %H:%M:%S %Z')"

keep_last_three_elements() {
    local path=$1
    local result=""

    for i in {1..3}; do
        result=$(basename "$path")/$result
        path=$(dirname "$path")
    done

    # Remove the trailing slash
    result=${result%/}
    echo "$result"
}


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

# Taking down the dashboards if they are running
echo "Stoping the dashboards if they are running"
docker compose -f bin/docker-compose-dashboards.yml down

# Remove unused conatiners
echo "Removing unused containers"
docker container prune -f

# Pullling the images with the tag $ieasyhydroforecast_frontend_docker_image_tag
echo "Pulling with TAG=$ieasyhydroforecast_frontend_docker_image_tag"
docker pull mabesa/sapphire-configuration:$ieasyhydroforecast_frontend_docker_image_tag
docker pull mabesa/sapphire-dashboard:$ieasyhydroforecast_frontend_docker_image_tag

# Removing old images
echo "Removing old images"
docker image prune -f

# Function to start the Docker Compose service for the dashboards
start_docker_compose_dashboards() {
  echo "Starting Docker Compose service for the dashboards..."
  docker compose -f bin/docker-compose-dashboards.yml up -d &
  DOCKER_COMPOSE_DASHBOARD_PID=$!
  echo "Docker Compose service started with PID $DOCKER_COMPOSE_DASHBOARD_PID"
}

# Start the Docker Compose service for the dashboards
start_docker_compose_dashboards

echo "Frontend update completed!"
