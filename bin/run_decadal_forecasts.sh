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

# Set prediction mode
export SAPPHIRE_PREDICTION_MODE="DECAD"

# Establish SSH tunnel (if required)
establish_ssh_tunnel

# Set the trap to clean up processes on exit
trap cleanup_decadal_forecasting_containers EXIT

# Start the Docker Compose service for decadal forecasting
echo "| Starting decadal forecasting workflow..."
echo "| Luigi daemon will handle dependencies and ensure preprocessing is complete"

docker compose -f bin/docker-compose-luigi-daemon.yml run --rm decadal

echo "| Decadal forecasting task submitted to Luigi daemon"
echo "| Check progress at: http://localhost:8082"