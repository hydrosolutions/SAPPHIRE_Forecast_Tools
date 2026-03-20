#!/bin/bash
# Daily Preprocessing Runoff Maintenance Script
#
# This script runs the preprocessing runoff module in maintenance mode to fill
# gaps and update historical data. It should be run weekly or when data issues
# are suspected.
#
# Usage:
#   bash bin/daily_preprunoff_maintenance.sh <env_file_path>
#
# Example:
#   bash bin/daily_preprunoff_maintenance.sh /path/to/config/.env
#
# The script will:
# 1. Read configuration from the .env file
# 2. Run preprocessing runoff in maintenance mode (30-day lookback)
# 3. Fill gaps and update values that differ from the database
# 4. Log all output to a timestamped log file
#
# Author: Beatrice Marti

# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"

# Print the banner
print_banner
echo "| Running Preprocessing Runoff Maintenance Mode"

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
LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/preprunoff_maintenance"
mkdir -p ${LOG_DIR}
echo "| Log directory: ${LOG_DIR}"

# Set main log file path with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
log_file="${LOG_DIR}/run_${TIMESTAMP}.log"

# Function to log messages to both console and log file
log_message() {
    echo "[$(date +"%Y-%m-%d %H:%M:%S")] $1" | tee -a "$log_file"
}

log_message "Starting Preprocessing Runoff maintenance run"

# Verify Docker is running
if ! docker info > /dev/null 2>&1; then
    log_message "ERROR: Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if the Docker image exists, pull if not
IMAGE_ID="mabesa/sapphire-preprunoff:${ieasyhydroforecast_backend_docker_image_tag:-latest}"
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

# Memory settings for the container
MEMORY_LIMIT="4g"
MEMORY_SWAP="6g"

# macOS Docker compatibility: localhost inside container doesn't reach host's localhost
# On macOS, we need to use host.docker.internal instead of localhost.
# The iEasyHydro HF server must be configured to accept connections from host.docker.internal.
DOCKER_HOST_OVERRIDE=""
if [[ "$(uname)" == "Darwin" ]]; then
    # Check if IEASYHYDROHF_HOST uses localhost - if so, we need to override it
    if [[ "$IEASYHYDROHF_HOST" == *"localhost"* ]]; then
        DOCKER_IEASYHYDROHF_HOST="${IEASYHYDROHF_HOST//localhost/host.docker.internal}"
        log_message "macOS detected: overriding IEASYHYDROHF_HOST for Docker container"
        log_message "  Original: $IEASYHYDROHF_HOST"
        log_message "  Docker:   $DOCKER_IEASYHYDROHF_HOST"
        DOCKER_HOST_OVERRIDE="-e IEASYHYDROHF_HOST=${DOCKER_IEASYHYDROHF_HOST}"
    fi
fi

CONTAINER_NAME="preprunoff-maintenance"
SERVICE_LOG="${LOG_DIR}/${CONTAINER_NAME}_${TIMESTAMP}.log"

log_message "Starting preprocessing runoff in maintenance mode..."
log_message "Container name: $CONTAINER_NAME"
log_message "Service log: $SERVICE_LOG"

# Remove any existing container with the same name
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    log_message "Removing existing container: $CONTAINER_NAME"
    docker rm -f $CONTAINER_NAME
fi

# Run the preprocessing runoff container in maintenance mode
# SAPPHIRE_SYNC_MODE=maintenance triggers:
# - 30-day lookback window (configurable via PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS)
# - Gap filling and value updates from database
# - Excel/CSV file change detection
# DOCKER_HOST_OVERRIDE is set on macOS to replace localhost with host.docker.internal
docker run \
    --name $CONTAINER_NAME \
    --network host \
    -e ieasyhydroforecast_data_root_dir=${ieasyhydroforecast_data_root_dir} \
    -e ieasyhydroforecast_env_file_path=${ieasyhydroforecast_env_file_path} \
    -e SAPPHIRE_OPDEV_ENV=True \
    -e IN_DOCKER=True \
    -e SAPPHIRE_SYNC_MODE=maintenance \
    ${DOCKER_HOST_OVERRIDE} \
    -v ${ieasyhydroforecast_data_ref_dir}/config:${ieasyhydroforecast_container_data_ref_dir}/config \
    -v ${ieasyhydroforecast_data_ref_dir}/daily_runoff:${ieasyhydroforecast_container_data_ref_dir}/daily_runoff \
    -v ${ieasyhydroforecast_data_ref_dir}/intermediate_data:${ieasyhydroforecast_container_data_ref_dir}/intermediate_data \
    -v ${ieasyhydroforecast_data_ref_dir}/bin:${ieasyhydroforecast_container_data_ref_dir}/bin \
    --memory=${MEMORY_LIMIT} \
    --memory-swap=${MEMORY_SWAP} \
    ${IMAGE_ID} \
    2>&1 | tee "$SERVICE_LOG"

EXIT_CODE=$?

# Capture container exit code if different from tee exit code
CONTAINER_EXIT_CODE=$(docker inspect $CONTAINER_NAME --format='{{.State.ExitCode}}' 2>/dev/null || echo "$EXIT_CODE")

if [ "$CONTAINER_EXIT_CODE" -eq 0 ]; then
    log_message "Preprocessing runoff maintenance completed successfully"
else
    log_message "WARNING: Preprocessing runoff maintenance completed with exit code: $CONTAINER_EXIT_CODE"
    log_message "Check log file for details: $SERVICE_LOG"
fi

# Clean up the container
docker rm -f $CONTAINER_NAME 2>/dev/null

# Clean up old log files (older than 15 days)
log_message "Removing log files older than 15 days"
find $LOG_DIR -type f -mtime +15 -delete

log_message "Preprocessing Runoff maintenance run completed"
echo "|"
echo "| Maintenance run complete. Check logs at: $LOG_DIR"
echo "|"
