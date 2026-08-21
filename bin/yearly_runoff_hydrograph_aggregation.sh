#!/usr/bin/env bash
# Yearly Runoff Hydrograph Aggregation Script
#
# This script runs the long-horizon runoff hydrograph writer, which
# fetches monthly discharge norms (plus per-month previous-year and
# current-year aggregates) from the iEasyHydro HF SDK and writes them
# to the SAPPHIRE preprocessing API. It additionally writes seasonal
# April-September hydrograph rows on top of the monthly triad.
#
# It REPLACES the previous norm-only path (`sync_monthly_norms.py`),
# which only populated the `norm` column and left `previous` /
# `current` as NULL. The new writer populates the full triad for
# every monthly row (one row per station per month) and adds a
# seasonal row per station per year.
#
# Designed to run once a year (e.g., 1 January 03:00 UTC) as part of
# the yearly maintenance window. Row existence is decoupled from the
# iEH HF SDK monthly norm call's outcome: a wrong-length/empty return
# (NORM_ABSENT) or the call itself raising (SDK_FAILED, PREPQ-015)
# both still write the station's month/quarter/season rows, preserving
# any previously stored norm via a read-merge rather than dropping the
# station. The run summary and exit code still distinguish the two --
# any SDK_FAILED station keeps the run's exit code non-zero (4, or 5
# if an API read/write failure also occurred) so the failure stays
# visible without withholding otherwise-computable data.
#
# Usage:
#   bash bin/yearly_runoff_hydrograph_aggregation.sh <env_file_path> [--target-year YYYY]
#
# Examples:
#   # Use current calendar year (default)
#   bash bin/yearly_runoff_hydrograph_aggregation.sh /path/to/config/.env
#
#   # Target a specific year (e.g., backfill 2024 rows)
#   bash bin/yearly_runoff_hydrograph_aggregation.sh /path/to/config/.env --target-year 2024
#
# The script will:
# 1. Read configuration from the .env file
# 2. Establish SSH tunnel to iEH HF (if required)
# 3. Pull the sapphire-preprunoff image if not present locally
# 4. Run the long-horizon hydrograph writer in a Docker container
# 5. Log all output to a timestamped log file
#
# Crontab example (run Jan 1 at 03:00):
#   0 3 1 1 * /path/to/bin/yearly_runoff_hydrograph_aggregation.sh /path/to/config/.env
#
# Prerequisites:
# - SAPPHIRE preprocessing API up and reachable
# - iEasyHydro HF SSH tunnel up (established by this script)
#
# Author: Beatrice Marti

set -euo pipefail

# Print help and exit if requested
if [ "${1-}" = "--help" ] || [ "${1-}" = "-h" ]; then
    sed -n '2,46p' "$0"
    exit 0
fi

# Parse positional .env path + optional --target-year YYYY
ENV_FILE_PATH="${1-}"
TARGET_YEAR=""

# Shift past the env file path so remaining args can be parsed.
if [ -n "$ENV_FILE_PATH" ]; then
    shift
fi

while [ $# -gt 0 ]; do
    case "$1" in
        --target-year)
            if [ $# -lt 2 ]; then
                echo "| Error: --target-year requires a YYYY argument" >&2
                exit 1
            fi
            TARGET_YEAR="$2"
            shift 2
            ;;
        --target-year=*)
            TARGET_YEAR="${1#*=}"
            shift
            ;;
        *)
            echo "| Error: unknown argument: $1" >&2
            echo "| Usage: $0 <env_file_path> [--target-year YYYY]" >&2
            exit 1
            ;;
    esac
done

# Source the common functions
# shellcheck source=bin/utils/common_functions.sh
source "$(dirname "$0")/utils/common_functions.sh"

# Print the banner
print_banner
echo "| Running Yearly Runoff Hydrograph Aggregation (monthly + seasonal triads)"

# Read the configuration from the .env file. common_functions.sh sources the
# env file and predates strict unset-variable checks, so relax -u only while
# loading configuration.
set +u
read_configuration "$ENV_FILE_PATH"
set -u

# Validate required environment variables
if [ -z "${ieasyhydroforecast_data_root_dir-}" ] || \
   [ -z "${ieasyhydroforecast_env_file_path-}" ] || \
   [ -z "${ieasyhydroforecast_data_ref_dir-}" ] || \
   [ -z "${ieasyhydroforecast_container_data_ref_dir-}" ]; then
    echo "| Error: Required environment variables are not set. Please check your .env file."
    exit 1
fi

# Create log directory if it doesn't exist
LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/runoff_hydrograph_aggregation"
mkdir -p "${LOG_DIR}"
echo "| Log directory: ${LOG_DIR}"

# Set main log file path with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
log_file="${LOG_DIR}/run_${TIMESTAMP}.log"

# Function to log messages to both console and log file
log_message() {
    echo "[$(date +"%Y-%m-%d %H:%M:%S")] $1" | tee -a "$log_file"
}

log_message "Starting Yearly Runoff Hydrograph Aggregation"
if [ -n "$TARGET_YEAR" ]; then
    log_message "Target year (override): $TARGET_YEAR"
else
    log_message "Target year: current calendar year (writer default)"
fi

# Verify Docker is running
if ! docker info > /dev/null 2>&1; then
    log_message "ERROR: Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if the Docker image exists, pull if not
IMAGE_ID="mabesa/sapphire-preprunoff:${ieasyhydroforecast_backend_docker_image_tag:-latest}"
if ! docker image inspect "$IMAGE_ID" > /dev/null 2>&1; then
    log_message "Image $IMAGE_ID not found locally, pulling..."
    if ! docker pull "$IMAGE_ID"; then
        log_message "ERROR: Failed to pull Docker image $IMAGE_ID"
        exit 1
    fi
fi

# Establish SSH tunnel (if required for database access)
establish_ssh_tunnel

# Set the trap to clean up processes on exit
trap cleanup EXIT

# Memory settings — long-horizon hydrograph writer is lightweight
MEMORY_LIMIT="4g"
MEMORY_SWAP="6g"

# macOS Docker compatibility
DOCKER_HOST_OVERRIDE=()
if [[ "$(uname)" == "Darwin" ]]; then
    if [[ "${IEASYHYDROHF_HOST-}" == *"localhost"* ]]; then
        DOCKER_IEASYHYDROHF_HOST="${IEASYHYDROHF_HOST//localhost/host.docker.internal}"
        log_message "macOS detected: overriding IEASYHYDROHF_HOST for Docker container"
        log_message "  Original: $IEASYHYDROHF_HOST"
        log_message "  Docker:   $DOCKER_IEASYHYDROHF_HOST"
        DOCKER_HOST_OVERRIDE=(-e "IEASYHYDROHF_HOST=${DOCKER_IEASYHYDROHF_HOST}")
    fi
fi

CONTAINER_NAME="maintenance-monthly-norms"
SERVICE_LOG="${LOG_DIR}/${CONTAINER_NAME}_${TIMESTAMP}.log"

log_message "Starting long-horizon hydrograph writer..."
log_message "Container name: $CONTAINER_NAME"
log_message "Service log: $SERVICE_LOG"

# Remove any existing container with the same name
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    log_message "Removing existing container: $CONTAINER_NAME"
    docker rm -f "$CONTAINER_NAME"
fi

# Build the command array — forward --target-year iff caller supplied one.
WRITER_CMD=(uv run sync_long_horizon_hydrograph.py)
if [ -n "$TARGET_YEAR" ]; then
    WRITER_CMD+=(--target-year "$TARGET_YEAR")
fi

# Run the long-horizon hydrograph writer container
set +e
docker run \
    --name "$CONTAINER_NAME" \
    --network host \
    -e ieasyhydroforecast_data_root_dir="${ieasyhydroforecast_data_root_dir}" \
    -e ieasyhydroforecast_env_file_path="${ieasyhydroforecast_env_file_path}" \
    -e SAPPHIRE_OPDEV_ENV=True \
    -e IN_DOCKER=True \
    "${DOCKER_HOST_OVERRIDE[@]}" \
    -v "${ieasyhydroforecast_data_ref_dir}/config:${ieasyhydroforecast_container_data_ref_dir}/config" \
    -v "${ieasyhydroforecast_data_ref_dir}/intermediate_data:${ieasyhydroforecast_container_data_ref_dir}/intermediate_data" \
    --memory="${MEMORY_LIMIT}" \
    --memory-swap="${MEMORY_SWAP}" \
    "${IMAGE_ID}" \
    "${WRITER_CMD[@]}" \
    2>&1 | tee "$SERVICE_LOG"

EXIT_CODE=$?
set -e

# Capture container exit code if different from tee exit code
CONTAINER_EXIT_CODE=$(docker inspect "$CONTAINER_NAME" --format='{{.State.ExitCode}}' 2>/dev/null || echo "$EXIT_CODE")

if [ "$CONTAINER_EXIT_CODE" -eq 0 ]; then
    log_message "Runoff hydrograph aggregation completed successfully"
else
    log_message "WARNING: Runoff hydrograph aggregation completed with exit code: $CONTAINER_EXIT_CODE"
    log_message "Check log file for details: $SERVICE_LOG"
fi

# Clean up the container
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

# Clean up old log files (older than 15 days)
log_message "Removing log files older than 15 days"
find "$LOG_DIR" -type f -mtime +15 -delete

log_message "Yearly Runoff Hydrograph Aggregation completed"
echo "|"
echo "| Aggregation complete. Check logs at: $LOG_DIR"
echo "|"

exit "$CONTAINER_EXIT_CODE"
