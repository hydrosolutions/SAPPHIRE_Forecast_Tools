#!/bin/bash
#==============================================================================
# SAPPHIRE FORECAST RERUN UTILITY
#==============================================================================
# Description:
#   This script re-runs the latest forecast produced by the SAPPHIRE forecast
#   tools using the existing forecast data but with updated containers and
#   configurations. It's primarily used during development or for troubleshooting
#   to regenerate forecasts without waiting for new data.
#
# Usage:
#   Interactive mode:
#     bash bin/rerun_latest_forecasts.sh <env_file_path>
#
#   Background execution:
#     nohup bash bin/rerun_latest_forecasts.sh <env_file_path> > output_rerun.log 2>&1 &
#
# Parameters:
#   env_file_path - Absolute path to the .env configuration file containing
#                   environment variables for the SAPPHIRE forecast tools
#
# Process:
#   1. Reads configuration from the specified .env file
#   2. Cleans up existing backend containers
#   3. Pulls and verifies Docker images
#   4. Establishes SSH tunnel for data access (if configured)
#   5. Resets the run date to use latest available data
#   6. Executes the forecasting pipeline via Docker Compose
#   7. Monitors execution and handles cleanup on completion
#
# Directory Structure Requirements:
#   ieasyhydroforecast_data_root_dir/
#   ├── SAPPHIRE_forecast_tools/       # This repository
#   │   ├── apps/                      # Application code
#   │   ├── bin/                       # Scripts directory
#   │   │   ├── utils/                 # Helper functions
#   │   │   └── docker-compose-*.yml   # Docker compose files
#   │   └── ...
#   │
#   └── ieasyhydroforecast_data_ref_dir/
#       ├── config/
#       │   └── .env                   # Environment file
#       ├── bin/
#       │   └── .ssh/                  # SSH configuration
#       └── ...
#
# Dependencies:
#   - common_functions.sh - Core utility functions
#   - docker-compose-luigi.yml - Container orchestration definition
#   - Pre-prepared images (optional) - From daily_docker_image_update.sh
#
# Exit Codes:
#   0 - Success: Forecast rerun completed successfully
#   1 - Failure: Error in setup or execution
#
# Note:
#   This script is intended for development use only and is not part of the
#   operational forecasting pipeline.
#
# Contributors:
#   - Beatrice Marti: Implementation and integration
#   - Developed with assistance from AI pair programming tools
#==============================================================================


# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"

# Print the banner
print_banner
echo "| Starting forecast rerun at $(date)"

# Read the configuration from the .env file
read_configuration $1

# Clean up docker space
clean_out_backend

# Check if pre-prepared images are available
TAG="${ieasyhydroforecast_backend_docker_image_tag:-latest}"
MARKER_FILE="/tmp/sapphire_backend_images_prepared_${TAG}"

if [ -f "${MARKER_FILE}" ]; then
    PREP_TIME=$(cat "${MARKER_FILE}")
    echo "| Using images prepared earlier at: ${PREP_TIME}"
    echo "| Skipping image pull and verification"
else
    echo "| No pre-prepared images found - pulling now"
    # Setup cosign for verification
    setup_cosign
    # Pull docker images using the existing function
    pull_docker_images
fi

# Establish SSH tunnel (if required)
establish_ssh_tunnel

# Set the trap to clean up processes on exit
trap cleanup EXIT

# Reset the run date
echo "| Resetting run date to use latest available data"
start_docker_container_reset_run_date

# Start the Docker Compose service for the forecasting pipeline
echo "| Starting forecasting pipeline with reset date"
start_docker_compose_luigi

# Wait for forecasting pipeline to finish
echo "| Waiting for forecasting pipeline to complete"
wait $DOCKER_COMPOSE_LUIGI_PID

# Additional actions to be taken after Docker Compose service stops
echo "| Forecast rerun completed at $(date)"
exit 0
