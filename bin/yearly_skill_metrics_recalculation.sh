#!/bin/bash
# Yearly Skill Metrics Recalculation Script
#
# This script runs a full recalculation of all skill metrics from historical
# data. This is the slow path — it reads ALL data, recalculates ALL metrics,
# and saves everything. Run annually or when historical data changes
# significantly.
#
# Usage:
#   bash bin/yearly_skill_metrics_recalculation.sh <env_file_path>
#
# Example:
#   bash bin/yearly_skill_metrics_recalculation.sh /path/to/config/.env
#
# The script will:
# 1. Read configuration from the .env file
# 2. Run full skill metrics recalculation for all models and stations
# 3. Save updated skill metrics to CSV and API
# 4. Log all output to a timestamped log file
#
# Author: Beatrice Marti

# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"
# Source the shared skill-metrics recalc helper
source "$(dirname "$0")/utils/run_skill_metrics_recalc.sh"

# Print the banner
print_banner
echo "| Running Yearly Skill Metrics Recalculation"

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
LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/skill_metrics_recalc"
mkdir -p ${LOG_DIR}
echo "| Log directory: ${LOG_DIR}"

# Set main log file path with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
log_file="${LOG_DIR}/run_${TIMESTAMP}.log"

# Function to log messages to both console and log file
log_message() {
    echo "[$(date +"%Y-%m-%d %H:%M:%S")] $1" | tee -a "$log_file"
}

log_message "Starting Yearly Skill Metrics Recalculation"

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

# Memory settings — skill recalculation is memory-intensive
MEMORY_LIMIT="8g"
MEMORY_SWAP="12g"

CONTAINER_NAME="postprc-skill-recalc"

log_message "Starting full skill metrics recalculation..."
log_message "WARNING: This may take several minutes depending on data volume."

# Delegate the docker-run invocation (including macOS IEASYHYDROHF_HOST
# override, stale-container cleanup, exit-code capture, and post-run container
# removal) to the shared helper. Info messages emitted inside the helper use
# plain `echo` and will reach cron stdout instead of the per-script log file.
run_skill_metrics_recalc_once \
    "${SAPPHIRE_PREDICTION_MODE:-BOTH}" \
    "${LOG_DIR}" \
    "${TIMESTAMP}" \
    "${CONTAINER_NAME}"
CONTAINER_EXIT_CODE=$?

if [ "$CONTAINER_EXIT_CODE" -eq 0 ]; then
    log_message "Skill metrics recalculation completed successfully"
else
    log_message "WARNING: Skill metrics recalculation completed with exit code: $CONTAINER_EXIT_CODE"
fi

# Clean up old log files (older than 15 days)
log_message "Removing log files older than 15 days"
find $LOG_DIR -type f -mtime +15 -delete

log_message "Yearly Skill Metrics Recalculation completed"
echo "|"
echo "| Recalculation complete. Check logs at: $LOG_DIR"
echo "|"
