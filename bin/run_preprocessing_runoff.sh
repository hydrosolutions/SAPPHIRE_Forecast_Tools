#!/bin/bash

# This script runs only the RUNOFF preprocessing step for SAPPHIRE forecast tools
# This must run later (11:00) as it depends on daily runoff data
# Usage: bash bin/run_preprocessing_runoff.sh <env_file_path>

# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"

# Print the banner
print_banner
echo "| Running RUNOFF PREPROCESSING only"

# Read the configuration from the .env file
read_configuration $1

if [[ "$OSTYPE" == "darwin"* ]]; then
    export LUIGI_SCHEDULER_URL="http://host.docker.internal:8082"
    DOCKER_GID=$(stat -f '%g' /var/run/docker.sock)
else
    export LUIGI_SCHEDULER_URL="http://localhost:8082"
    DOCKER_GID=$(stat -c '%g' /var/run/docker.sock)
fi
echo "| Luigi scheduler URL set to: $LUIGI_SCHEDULER_URL"

# Establish SSH tunnel (if required)
establish_ssh_tunnel

# Set the trap to clean up processes on exit 
trap cleanup_preprocessing_containers EXIT

# Start the Docker Compose service for runoff preprocessing only
echo "| Starting runoff preprocessing workflow..."
echo "| Tasks will be submitted to Luigi daemon at $LUIGI_SCHEDULER_URL"

# Create a modified luigi.cfg file
echo "[core]" > temp_luigi.cfg
echo "default_scheduler_url = $LUIGI_SCHEDULER_URL" >> temp_luigi.cfg

# Regular command
docker compose -f bin/docker-compose-luigi.yml run \
    -v $(pwd)/temp_luigi.cfg:/app/luigi.cfg \
    -e PYTHONPATH="/home/appuser/.local/lib/python3.11/site-packages:${PYTHONPATH}" \
    --user root \
    --rm \
    preprocessing-runoff

echo "| Runoff preprocessing task submitted to Luigi daemon"
echo "| Check progress at: $LUIGI_SCHEDULER_URL"