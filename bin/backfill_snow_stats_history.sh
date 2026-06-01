#!/bin/bash
# Historical Snow Stat Backfill Script
#
# This script runs the yearly snow norm/stat recalculation across historical
# years, skipping the current year and stamping completed years for resume.
#
# Usage:
#   ieasyhydroforecast_env_file_path=/path/to/config/.env \
#     bash bin/backfill_snow_stats_history.sh [--start-year YYYY]
#
# Example:
#   ieasyhydroforecast_env_file_path=/path/to/config/.env \
#     bash bin/backfill_snow_stats_history.sh --start-year 2010
#
# The script will:
# 1. Read configuration from ieasyhydroforecast_env_file_path
# 2. Loop from the start year through last calendar year
# 3. Run recalculate_snow_norms.py once per year
# 4. Record completed years in a progress file for resumability
#
# Author: Beatrice Marti

set -euo pipefail

# Source the common functions
# shellcheck source=bin/utils/common_functions.sh
source "$(dirname "$0")/utils/common_functions.sh"

usage() {
    echo "Usage: ieasyhydroforecast_env_file_path=/path/to/config/.env bash $0 [--start-year YYYY]"
}

START_YEAR=2010

while [[ $# -gt 0 ]]; do
    case "$1" in
        --start-year)
            if [[ $# -lt 2 ]]; then
                echo "| Error: --start-year requires a YYYY value"
                usage
                exit 1
            fi
            START_YEAR="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "| Error: Unknown argument: $1"
            usage
            exit 1
            ;;
    esac
done

if ! [[ "$START_YEAR" =~ ^[0-9]{4}$ ]]; then
    echo "| Error: --start-year must be a four-digit year, got '$START_YEAR'"
    exit 1
fi

if [[ -z "${ieasyhydroforecast_env_file_path:-}" ]]; then
    echo "| Error: ieasyhydroforecast_env_file_path must be set to a local .env file path"
    exit 1
fi

LOCAL_ENV_FILE="$ieasyhydroforecast_env_file_path"
if [[ ! -f "$LOCAL_ENV_FILE" ]]; then
    echo "| Error: .env file not found at $LOCAL_ENV_FILE"
    exit 1
fi

# Print the banner
print_banner
echo "| Running Historical Snow Stat Backfill"

# Read the configuration from the .env file. common_functions.sh predates
# strict unset-variable checks, so relax -u only while sourcing config.
set +u
read_configuration "$LOCAL_ENV_FILE"
set -u

if [[ -z "${ieasyhydroforecast_data_root_dir:-}" ]] || \
   [[ -z "${ieasyhydroforecast_env_file_path:-}" ]] || \
   [[ -z "${ieasyhydroforecast_data_ref_dir:-}" ]] || \
   [[ -z "${ieasyhydroforecast_container_data_ref_dir:-}" ]]; then
    echo "| Error: Required environment variables are not set. Please check your .env file."
    exit 1
fi

LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/snow_stat_backfill"
mkdir -p "$LOG_DIR"
PROGRESS="$LOG_DIR/backfill_progress.txt"
touch "$PROGRESS"

log_message() {
    echo "[$(date +"%Y-%m-%d %H:%M:%S")] $1"
}

END_YEAR=$(date +%Y)

log_message "Starting historical snow stat backfill"
log_message "Start year: $START_YEAR"
log_message "End year: $((END_YEAR - 1))"
log_message "Log directory: $LOG_DIR"
log_message "Progress file: $PROGRESS"

if (( START_YEAR >= END_YEAR )); then
    log_message "all years processed"
    exit 0
fi

# Verify Docker is running
if ! docker info > /dev/null 2>&1; then
    log_message "ERROR: Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if the Docker image exists, pull if not
IMAGE_ID="mabesa/sapphire-pipeline:${ieasyhydroforecast_backend_docker_image_tag:-latest}"
if ! docker image inspect "$IMAGE_ID" > /dev/null 2>&1; then
    log_message "Image $IMAGE_ID not found locally, pulling..."
    docker pull "$IMAGE_ID"
fi

# Establish SSH tunnel (if required for database access)
establish_ssh_tunnel

# Set the trap to clean up processes on exit
trap cleanup EXIT

# Memory settings - snow stat recalculation is lightweight
MEMORY_LIMIT="4g"
MEMORY_SWAP="6g"

# macOS Docker compatibility
DOCKER_HOST_OVERRIDE=()
if [[ "$(uname)" == "Darwin" ]]; then
    if [[ "${IEASYHYDROHF_HOST:-}" == *"localhost"* ]]; then
        DOCKER_IEASYHYDROHF_HOST="${IEASYHYDROHF_HOST//localhost/host.docker.internal}"
        log_message "macOS detected: overriding IEASYHYDROHF_HOST for Docker container"
        log_message "  Original: $IEASYHYDROHF_HOST"
        log_message "  Docker:   $DOCKER_IEASYHYDROHF_HOST"
        DOCKER_HOST_OVERRIDE=(-e "IEASYHYDROHF_HOST=${DOCKER_IEASYHYDROHF_HOST}")
    fi
fi

for ((year = START_YEAR; year < END_YEAR; year++)); do
    if grep -qx "$year" "$PROGRESS"; then
        log_message "year $year already completed, skipping"
        continue
    fi

    YEAR_LOG="$LOG_DIR/backfill_${year}.log"
    CONTAINER_NAME="prepgw-snow-stat-backfill-${year}"

    log_message "running recalc for year $year"
    log_message "year $year log: $YEAR_LOG"

    if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        log_message "Removing existing container: $CONTAINER_NAME"
        docker rm -f "$CONTAINER_NAME" > /dev/null 2>&1
    fi

    if docker run \
        --name "$CONTAINER_NAME" \
        --network host \
        -e "ieasyhydroforecast_data_root_dir=${ieasyhydroforecast_data_root_dir}" \
        -e "ieasyhydroforecast_env_file_path=${ieasyhydroforecast_env_file_path}" \
        -e "ieasyhydroforecast_SNOW_RECALC_YEAR=${year}" \
        -e SAPPHIRE_OPDEV_ENV=True \
        -e IN_DOCKER=True \
        "${DOCKER_HOST_OVERRIDE[@]}" \
        -v "${ieasyhydroforecast_data_ref_dir}/config:${ieasyhydroforecast_container_data_ref_dir}/config" \
        -v "${ieasyhydroforecast_data_ref_dir}/intermediate_data:${ieasyhydroforecast_container_data_ref_dir}/intermediate_data" \
        --memory="${MEMORY_LIMIT}" \
        --memory-swap="${MEMORY_SWAP}" \
        "$IMAGE_ID" \
        uv run recalculate_snow_norms.py \
        > "$YEAR_LOG" 2>&1; then
        echo "$year" >> "$PROGRESS"
        log_message "year $year completed"
    else
        log_message "year $year FAILED - see $YEAR_LOG"
        docker rm -f "$CONTAINER_NAME" > /dev/null 2>&1 || true
        exit 1
    fi

    docker rm -f "$CONTAINER_NAME" > /dev/null 2>&1 || true
done

log_message "all years processed"
