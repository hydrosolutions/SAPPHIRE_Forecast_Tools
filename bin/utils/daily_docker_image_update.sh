#!/bin/bash
#==============================================================================
# SAPPHIRE DOCKER IMAGE PREPARATION
#==============================================================================
# Description:
#   This script handles pulling and verifying Docker images in advance of
#   operational hydrological forecasting runs. By pre-downloading and verifying
#   container images, the main forecasting process runs faster and more reliably.
#
# Usage:
#   bash bin/utils/daily_docker_image_update.sh <env_file_path>
#
#   Example:
#     bash bin/utils/daily_docker_image_update.sh /path/to/config/.env
#
# Parameters:
#   env_file_path - Absolute path to the .env configuration file containing
#                   environment variables for the SAPPHIRE forecast tools
#
# Process:
#   1. Reads configuration from the specified .env file
#   2. Determines which Docker images to pull based on configuration
#   3. Downloads and cryptographically verifies image signatures
#   4. Creates a marker file upon successful completion
#   5. The marker file is used by operational forecast scripts to verify
#      that images are ready, avoiding redundant downloads
#
# Marker File:
#   Location: /tmp/sapphire_backend_images_prepared_${TAG}
#   Content:  Timestamp of successful preparation
#   Usage:    Operational scripts check for this file's existence
#
# Exit Codes:
#   0 - Success: All images pulled and verified successfully
#   1 - Failure: One or more images failed to pull or verify
#
# Dependencies:
#   - common_functions.sh - For utility functions
#   - Docker daemon - Must be running
#   - Cosign - For cryptographic verification (installed if missing)
#   - Internet connectivity - To download images
#
# Recommended Scheduling:
#   Run 2+ hours before operational forecasting to ensure images are ready
#
# Contributors:
#   - Beatrice Marti: Implementation and integration
#   - Developed with assistance from AI pair programming tools
#==============================================================================


# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"

# Print the banner
print_banner "SAPPHIRE Image Preparation"

# Log the start time
start_time=$(date)
echo "| Starting image preparation at $start_time"

# Read the configuration from the .env file
read_configuration $1

echo "| Starting image preparation at $(date)"

# Get the tag from environment or use default
TAG="${ieasyhydroforecast_backend_docker_image_tag:-latest}"

# Create a marker file to track successful preparation
MARKER_FILE="/tmp/sapphire_backend_images_prepared_${TAG}"

# Remove any existing marker file to avoid using partially prepared images
rm -f "${MARKER_FILE}"

# Setup cosign for verification
setup_cosign

# Pull docker images - using the existing function from common_functions.sh
# pull_docker_images pulls backend images only
pull_docker_images "${TAG}"
PULL_STATUS=$?

# Mark successful completion
if [ $PULL_STATUS -eq 0 ]; then
    echo "| Image preparation completed successfully at $(date)"
    echo "$(date)" > "${MARKER_FILE}"
    echo "| Images ready for fast operational forecasting"
    echo "| Marker created at: ${MARKER_FILE}"
    exit 0
else
    echo "| ⚠️ Image preparation FAILED at $(date)"
    echo "| Operational forecast will need to pull images"
    exit 1
fi