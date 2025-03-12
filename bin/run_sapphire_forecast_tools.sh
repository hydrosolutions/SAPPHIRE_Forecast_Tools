#!/bin/bash
#==============================================================================
# SAPPHIRE OPERATIONAL HYDROLOGICAL FORECAST
#==============================================================================
# Description:
#   This script executes the operational hydrological forecasting workflow for
#   SAPPHIRE. It handles Docker container orchestration, data processing, and
#   forecast generation in a streamlined pipeline designed for daily operation.
#
# Usage:
#   Interactive mode:
#     bash bin/run_sapphire_forecast_tools.sh <env_file_path>
#
#   Background execution:
#     nohup bash bin/run_sapphire_forecast_tools.sh <env_file_path> > output.log 2>&1 &
#
# Parameters:
#   env_file_path - Absolute path to the .env configuration file containing
#                   environment variables for the SAPPHIRE forecast tools
#
# Process:
#   1. Reads configuration from the specified .env file
#   2. Cleans up existing backend containers
#   3. Uses pre-prepared Docker images (if available) or pulls them
#   4. Establishes SSH tunnel for data access (if configured)
#   5. Executes the forecasting pipeline via Docker Compose
#   6. Monitors execution and handles cleanup on completion
#
# Performance:
#   Typical execution time: ~4 minutes on standard hardware with cached images
#   Resource usage: Moderate CPU and memory during forecast calculations
#
# Dependencies:
#   - common_functions.sh - Core utility functions
#   - docker-compose-luigi.yml - Container orchestration definition
#   - Pre-prepared images (optional) - From daily_docker_image_update.sh
#
# Helper Scripts:
#   - bin/utils/common_functions.sh - Shared utility functions
#   - bin/utils/pull_docker_images.sh - Container image management
#   - bin/.ssh/open_ssh_tunnel.sh - Data access setup
#   - bin/.ssh/close_ssh_tunnel.sh - Network cleanup
#
# Directory Structure Requirements:
#   ieasyhydroforecast_data_root_dir
#   ├── SAPPHIRE_forecast_tools       # This repository
#   │   ├── apps
#   │   └── bin
#   └── ieasyhydroforecast_data_ref_dir
#       ├── config
#       │   └── .env                  # Environment file
#       └── bin
#           └── .ssh                  # SSH configuration
#
# Optimization:
#   This script checks for pre-prepared Docker images to improve startup time.
#   Run daily_docker_image_update.sh beforehand for best performance.
#
# Exit Codes:
#   0 - Success: Forecast completed successfully
#   1 - Failure: Error in setup or execution
#
# Recommended Scheduling:
#   Run daily during operational forecasting window (e.g., 4:00 AM)
#   Schedule after daily_maintenance_sapphire_backend.sh for optimal performance
#
# Contributors:
#   - Beatrice Marti: Implementation and integration
#   - Developed with assistance from AI pair programming tools
#==============================================================================


# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"

# Print the banner
print_banner

# Read the configuration from the .env file
read_configuration $1

# Clean up backend containers
clean_out_backend

TAG="${ieasyhydroforecast_backend_docker_image_tag:-latest}"
MARKER_FILE="/tmp/sapphire_backend_images_prepared_${TAG}"

if [ -f "${MARKER_FILE}" ]; then
    PREP_TIME=$(cat "${MARKER_FILE}")
    echo "| Using images prepared earlier at: ${PREP_TIME}"
    echo "| Skipping image pull and verification"
else
    echo "| No pre-prepared images found - pulling now"
    # Pull docker images using the existing function
    pull_docker_images
fi

# Establish SSH tunnel (if required)
establish_ssh_tunnel

# Set the trap to clean up processes on exit
trap cleanup EXIT

# Start the Docker Compose service for the forecasting pipeline
start_docker_compose_luigi

# Wait for forecasting pipeline to finish
wait $DOCKER_COMPOSE_LUIGI_PID

# Additional actions to be taken after Docker Compose service stops
echo "Docker Compose service for backend has finished running"


