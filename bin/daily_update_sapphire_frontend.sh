#!/bin/bash
#==============================================================================
# SAPPHIRE FRONTEND UPDATE
#==============================================================================
# Description:
#   This script updates the dashboards of the SAPPHIRE forecast tools by
#   pulling the latest Docker images, verifying their signatures, and
#   restarting dashboard services.
#
# Usage:
#   bash bin/daily_update_sapphire_frontend.sh <env_file_path>
#
#   Example:
#     bash bin/daily_update_sapphire_frontend.sh /path/to/config/.env
#
# Parameters:
#   env_file_path - Absolute path to the .env configuration file containing
#                   environment variables for the SAPPHIRE forecast tools.
#                   Optional if ieasyhydroforecast_env_file_path is set.
#
# Process:
#   1. Reads configuration from the specified .env file
#   2. Stops existing dashboard containers if running
#   3. Pulls and cryptographically verifies Docker images
#   4. Creates a marker file upon successful image preparation
#   5. Starts the Docker Compose service for dashboards
#
# Directory Structure Requirements:
#   The script expects a specific directory structure:
#
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
# Exit Codes:
#   0 - Success: Frontend updated successfully
#   1 - Failure: Error in setup or execution
#
# Dependencies:
#   - common_functions.sh - For utility functions
#   - docker-compose-dashboards.yml - Container orchestration definition
#
# Recommended Scheduling:
#   Run daily after data updates but before users typically access dashboards
#
# Contributors:
#   - Beatrice Marti: Implementation and integration
#   - Developed with assistance from AI pair programming tools
#==============================================================================


# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"

# Print the banner
print_banner
echo "| Starting frontend update at $(date)"

# Read the configuration from the .env file
read_configuration $1

# Taking down the dashboards if they are running
echo "| Stoping the dashboards if they are running"
docker compose -f bin/docker-compose-dashboards.yml down

# Check if pre-prepared images are available
TAG="${ieasyhydroforecast_frontend_docker_image_tag:-latest}"
MARKER_FILE="/tmp/sapphire_frontend_images_prepared_${TAG}"

# Try to use pre-prepared images
if check_frontend_images_prepared "$TAG"; then
    echo "| Using pre-prepared frontend images from: $(cat ${MARKER_FILE})"
    echo "| Skipping pull and verification"
else
    echo "| No pre-prepared frontend images found - pulling and verifying now"

    # Setup environment for signature verification
    setup_cosign

    # Remove unused containers
    echo "| Removing unused containers"
    docker container prune -f

    # Pull and verify the frontend images
    REPO="mabesa"
    IMAGES="sapphire-configuration sapphire-dashboard"

    echo "| Pulling and verifying frontend images with TAG=${TAG}"
    all_successful=true

    for image in $IMAGES; do
        echo "| Processing $image:${TAG}"
        if ! pull_and_verify_image "${REPO}/${image}:${TAG}" true; then
            echo "| ❌ Failed to pull/verify ${image}:${TAG}"
            all_successful=false
            break
        fi
        echo "| ✅ Successfully pulled and verified ${image}:${TAG}"
    done

    # Create marker file if all images were pulled and verified successfully
    if $all_successful; then
        echo "$(date)" > "${MARKER_FILE}"
        echo "| Frontend images prepared successfully"
        echo "| Marker created at: ${MARKER_FILE}"
    else
        echo "| ⚠️ Warning: Not all images were successfully pulled/verified"
        rm -f "${MARKER_FILE}"  # Remove marker file if it exists
    fi

    # Removing old images
    echo "| Removing old images"
    docker image prune -f
fi

# Start the Docker Compose service for the dashboards
start_docker_compose_dashboards

# Wait for dashboards to finish
wait $DOCKER_COMPOSE_DASHBOARD_PID

echo "Frontend update completed!"
