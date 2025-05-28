#!/bin/bash

# This script runs the PENTADAL forecasting for SAPPHIRE forecast tools
# Usage: bash bin/run_pentadal_forecast.sh <env_file_path>

# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"

# Print the banner
print_banner
echo "| Running PENTADAL forecasting"

# Read the configuration from the .env file
read_configuration $1

# Establish SSH tunnel (if required)
establish_ssh_tunnel

# Set the trap to clean up processes on exit (closes SSH tunnel)
trap cleanup_pentadal_forecasting_containers EXIT

# Start the Docker Compose service for pentadal forecasting
echo "| Starting pentadal forecasting workflow..."
echo "| Luigi daemon will handle dependencies and ensure preprocessing is complete"

docker compose -f bin/docker-compose-luigi.yml run --rm pentadal

echo "| Pentadal forecasting task submitted to Luigi daemon"
echo "| Check progress at: http://localhost:8082"