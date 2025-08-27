#!/bin/bash

# This script runs the DECADAL forecasting for SAPPHIRE forecast tools
# Usage: bash bin/run_decadal_forecast.sh <env_file_path>

# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"

# Print the banner
print_banner
echo "| Running DECADAL forecasting"

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
trap cleanup_decadal_forecasting_containers EXIT

# Start the Docker Compose service for decadal forecasting
echo "| Starting decadal forecasting workflow..."
echo "| Luigi daemon will handle dependencies and ensure preprocessing is complete"

# Create a modified luigi.cfg file
echo "[core]" > temp_luigi.cfg
echo "default_scheduler_url = $LUIGI_SCHEDULER_URL" >> temp_luigi.cfg

# Run the decadal forecasting with proper configuration
docker compose -f bin/docker-compose-luigi.yml run \
    -v $(pwd)/temp_luigi.cfg:/app/luigi.cfg \
    -e PYTHONPATH="/home/appuser/.local/lib/python3.11/site-packages:${PYTHONPATH}" \
    -e SAPPHIRE_PREDICTION_MODE=DECAD \
    --user root \
    --rm \
    decadal

echo "| Decadal forecasting task submitted to Luigi daemon"
echo "| Check progress at: $LUIGI_SCHEDULER_URL"