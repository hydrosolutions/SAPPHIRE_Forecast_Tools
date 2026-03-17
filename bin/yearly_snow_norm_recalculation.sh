#!/bin/bash
# Yearly Snow Norm Recalculation Script
#
# This script runs a full recalculation of climatological daily snow norms
# (SWE, HS, RoF) from historical reanalysis CSVs, then writes the norms
# to the SAPPHIRE preprocessing API.
#
# Designed to run once a year (e.g., end of August) after the snow
# reanalysis files have been updated for the previous season.
#
# Usage:
#   bash bin/yearly_snow_norm_recalculation.sh <env_file_path>
#
# Example:
#   bash bin/yearly_snow_norm_recalculation.sh /path/to/config/.env
#
# The script will:
# 1. Read configuration from the .env file
# 2. Compute climatological daily norms from historical snow CSVs
# 3. Write full-year norm records to the preprocessing API
# 4. Log all output to a timestamped log file
#
# Crontab example (run Aug 25 at 02:00):
#   0 2 25 8 * /path/to/bin/yearly_snow_norm_recalculation.sh /path/to/config/.env
#
# Author: Beatrice Marti

# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"

# Print the banner
print_banner
echo "| Running Yearly Snow Norm Recalculation"

# Read the configuration from the .env file
read_configuration $1

# Validate required environment variables
if [ -z "$ieasyhydroforecast_data_root_dir" ] || \
   [ -z "$ieasyhydroforecast_env_file_path" ] || \
   [ -z "$ieasyhydroforecast_data_ref_dir" ] || \
   [ -z "$ieasyhydroforecast_container_data_ref_dir" ]; then
    echo "| Error: Required environment variables are not set. Please check your .env file."
    exit 1
fi

# Create log directory if it doesn't exist
LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/snow_norm_recalc"
mkdir -p ${LOG_DIR}
echo "| Log directory: ${LOG_DIR}"

# Set main log file path with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
log_file="${LOG_DIR}/run_${TIMESTAMP}.log"

# Function to log messages to both console and log file
log_message() {
    echo "[$(date +"%Y-%m-%d %H:%M:%S")] $1" | tee -a "$log_file"
}

log_message "Starting Yearly Snow Norm Recalculation"

# Verify Docker is running
if ! docker info > /dev/null 2>&1; then
    log_message "ERROR: Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if the Docker image exists, pull if not
IMAGE_ID="mabesa/sapphire-pipeline:${ieasyhydroforecast_backend_docker_image_tag:-latest}"
if ! docker image inspect $IMAGE_ID > /dev/null 2>&1; then
    log_message "Image $IMAGE_ID not found locally, pulling..."
    docker pull $IMAGE_ID
    if [ $? -ne 0 ]; then
        log_message "ERROR: Failed to pull Docker image $IMAGE_ID"
        exit 1
    fi
fi

# Establish SSH tunnel (if required for database access)
establish_ssh_tunnel

# Set the trap to clean up processes on exit
trap cleanup EXIT

# Memory settings — snow norm recalculation is lightweight
MEMORY_LIMIT="4g"
MEMORY_SWAP="6g"

# macOS Docker compatibility
DOCKER_HOST_OVERRIDE=""
if [[ "$(uname)" == "Darwin" ]]; then
    if [[ "$IEASYHYDROHF_HOST" == *"localhost"* ]]; then
        DOCKER_IEASYHYDROHF_HOST="${IEASYHYDROHF_HOST//localhost/host.docker.internal}"
        log_message "macOS detected: overriding IEASYHYDROHF_HOST for Docker container"
        log_message "  Original: $IEASYHYDROHF_HOST"
        log_message "  Docker:   $DOCKER_IEASYHYDROHF_HOST"
        DOCKER_HOST_OVERRIDE="-e IEASYHYDROHF_HOST=${DOCKER_IEASYHYDROHF_HOST}"
    fi
fi

CONTAINER_NAME="prepgw-snow-norm-recalc"
SERVICE_LOG="${LOG_DIR}/${CONTAINER_NAME}_${TIMESTAMP}.log"

log_message "Starting snow norm recalculation..."
log_message "Container name: $CONTAINER_NAME"
log_message "Service log: $SERVICE_LOG"

# Remove any existing container with the same name
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    log_message "Removing existing container: $CONTAINER_NAME"
    docker rm -f $CONTAINER_NAME
fi

# Run the snow norm recalculation container
docker run \
    --name $CONTAINER_NAME \
    --network host \
    -e ieasyhydroforecast_data_root_dir=${ieasyhydroforecast_data_root_dir} \
    -e ieasyhydroforecast_env_file_path=${ieasyhydroforecast_env_file_path} \
    -e SAPPHIRE_OPDEV_ENV=True \
    -e IN_DOCKER=True \
    ${DOCKER_HOST_OVERRIDE} \
    -v ${ieasyhydroforecast_data_ref_dir}/config:${ieasyhydroforecast_container_data_ref_dir}/config \
    -v ${ieasyhydroforecast_data_ref_dir}/intermediate_data:${ieasyhydroforecast_container_data_ref_dir}/intermediate_data \
    --memory=${MEMORY_LIMIT} \
    --memory-swap=${MEMORY_SWAP} \
    ${IMAGE_ID} \
    uv run recalculate_snow_norms.py \
    2>&1 | tee "$SERVICE_LOG"

EXIT_CODE=$?

# Capture container exit code if different from tee exit code
CONTAINER_EXIT_CODE=$(docker inspect $CONTAINER_NAME --format='{{.State.ExitCode}}' 2>/dev/null || echo "$EXIT_CODE")

if [ "$CONTAINER_EXIT_CODE" -eq 0 ]; then
    log_message "Snow norm recalculation completed successfully"
else
    log_message "WARNING: Snow norm recalculation completed with exit code: $CONTAINER_EXIT_CODE"
    log_message "Check log file for details: $SERVICE_LOG"
fi

# Clean up the container
docker rm -f $CONTAINER_NAME 2>/dev/null

# Clean up old log files (older than 15 days)
log_message "Removing log files older than 15 days"
find $LOG_DIR -type f -mtime +15 -delete

log_message "Yearly Snow Norm Recalculation completed"
echo "|"
echo "| Recalculation complete. Check logs at: $LOG_DIR"
echo "|"
