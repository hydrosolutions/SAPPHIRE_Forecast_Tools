#!/bin/bash
# Bimonthly Long-Term Postprocessing Script
#
# Creates monthly (long-term) ensemble forecasts from pre-calculated
# skill metrics. Runs maintenance (gap-fill) and/or operational (latest
# month) entry points.
#
# Usage:
#   bash bin/bimonthly_long_term_postprocessing.sh <env_file_path> [mode]
#
# Modes:
#   (none)        Run maintenance first, then operational (default)
#   maintenance   Run only the maintenance gap-fill
#   operational   Run only the operational latest-month path
#
# Examples:
#   bash bin/bimonthly_long_term_postprocessing.sh /path/to/config/.env
#   bash bin/bimonthly_long_term_postprocessing.sh /path/to/config/.env maintenance
#   bash bin/bimonthly_long_term_postprocessing.sh /path/to/config/.env operational
#
# Author: Beatrice Marti

# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"

# Print the banner
print_banner
echo "| Running Bimonthly Long-Term Postprocessing"

# Read the configuration from the .env file
read_configuration $1

# Parse mode argument
MODE="${2:-both}"
if [[ "$MODE" != "both" && "$MODE" != "maintenance" && "$MODE" != "operational" ]]; then
    echo "| Error: Invalid mode '$MODE'. Expected: maintenance, operational, or both."
    exit 1
fi
echo "| Mode: $MODE"

# Validate required environment variables
if [ -z "$ieasyhydroforecast_data_root_dir" ] || \
   [ -z "$ieasyhydroforecast_env_file_path" ] || \
   [ -z "$ieasyhydroforecast_data_ref_dir" ] || \
   [ -z "$ieasyhydroforecast_container_data_ref_dir" ]; then
    echo "| Error: Required environment variables are not set. Please check your .env file."
    exit 1
fi

# Create log directory if it doesn't exist
LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/long_term_postprocessing"
mkdir -p ${LOG_DIR}
echo "| Log directory: ${LOG_DIR}"

# Set main log file path with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
log_file="${LOG_DIR}/run_${TIMESTAMP}.log"

# Function to log messages to both console and log file
log_message() {
    echo "[$(date +"%Y-%m-%d %H:%M:%S")] $1" | tee -a "$log_file"
}

log_message "Starting Bimonthly Long-Term Postprocessing (mode: $MODE)"

# Verify Docker is running
if ! docker info > /dev/null 2>&1; then
    log_message "ERROR: Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if the Docker image exists, pull if not
IMAGE_ID="mabesa/sapphire-postprocessing:${ieasyhydroforecast_backend_docker_image_tag:-latest}"
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

# --- Helper: run a container ---
run_container() {
    local CONTAINER_NAME=$1
    local ENTRY_POINT=$2
    local MEMORY_LIMIT=$3
    local MEMORY_SWAP=$4

    local SERVICE_LOG="${LOG_DIR}/${CONTAINER_NAME}_${TIMESTAMP}.log"
    log_message "Starting $CONTAINER_NAME..."
    log_message "  Entry point: $ENTRY_POINT"
    log_message "  Service log: $SERVICE_LOG"

    # Remove any existing container with the same name
    if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        log_message "Removing existing container: $CONTAINER_NAME"
        docker rm -f $CONTAINER_NAME
    fi

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
        uv run $ENTRY_POINT \
        2>&1 | tee "$SERVICE_LOG"

    local CONTAINER_EXIT_CODE=$(docker inspect $CONTAINER_NAME --format='{{.State.ExitCode}}' 2>/dev/null || echo "1")

    if [ "$CONTAINER_EXIT_CODE" -eq 0 ]; then
        log_message "$CONTAINER_NAME completed successfully"
    else
        log_message "WARNING: $CONTAINER_NAME completed with exit code: $CONTAINER_EXIT_CODE"
        log_message "Check log file for details: $SERVICE_LOG"
    fi

    # Clean up the container
    docker rm -f $CONTAINER_NAME 2>/dev/null

    return $CONTAINER_EXIT_CODE
}

# --- Run maintenance (gap-fill) ---
if [[ "$MODE" == "both" || "$MODE" == "maintenance" ]]; then
    run_container "postprc-lt-maintenance" \
        "postprocessing_maintenance_long_term.py" \
        "8g" "12g"
fi

# --- Run operational (latest month) ---
if [[ "$MODE" == "both" || "$MODE" == "operational" ]]; then
    run_container "postprc-lt-operational" \
        "postprocessing_operational_long_term.py" \
        "4g" "6g"
fi

# Clean up old log files (older than 15 days)
log_message "Removing log files older than 15 days"
find $LOG_DIR -type f -mtime +15 -delete

log_message "Bimonthly Long-Term Postprocessing completed"
echo "|"
echo "| Postprocessing complete. Check logs at: $LOG_DIR"
echo "|"
