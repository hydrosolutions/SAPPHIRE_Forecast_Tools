#!/bin/bash
# Bimonthly Long-Term Skill Metrics Recalculation Script
#
# This script refreshes long-term (monthly-tier) skill metrics shortly after
# each new long-term forecast run. It iterates over the three long-term
# prediction modes — MONTHLY, QUARTERLY, SEASONAL — and invokes the shared
# skill-metrics recalc helper once per mode.
#
# Unlike the yearly recalc (which uses a single `SAPPHIRE_PREDICTION_MODE`
# value), this wrapper passes the mode to the helper as a positional argument
# and never exports `SAPPHIRE_PREDICTION_MODE` into its own environment — that
# would leak into other tools and violate the helper's "no ambient env"
# contract.
#
# Failure policy: log and continue. If a single mode fails, the remaining
# modes still run. The script exits non-zero at the end if any mode failed,
# so cron/log review still surfaces the problem.
#
# Usage:
#   bash bin/bimonthly_long_term_skill_metrics_recalculation.sh <env_file_path>
#
# Example:
#   bash bin/bimonthly_long_term_skill_metrics_recalculation.sh /path/to/config/.env
#
# The script will:
# 1. Read configuration from the .env file
# 2. Verify Docker is running and pull the postprocessing image (once)
# 3. Establish the SSH tunnel (once) for database access
# 4. Loop over MONTHLY, QUARTERLY, SEASONAL and run the recalc helper for each
# 5. Emit a summary line and exit non-zero if any mode failed
#
# Author: Beatrice Marti

# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"
# Source the shared skill-metrics recalc helper
source "$(dirname "$0")/utils/run_skill_metrics_recalc.sh"

# Print the banner
print_banner
echo "| Running BIMONTHLY Long-Term Skill Metrics Recalculation"

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

# Create log directory if it doesn't exist (distinct from the yearly recalc)
LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/skill_metrics_recalc_longterm"
mkdir -p "$LOG_DIR"
echo "| Log directory: ${LOG_DIR}"

# Set main log file path with timestamp (captured once; reused across modes)
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
log_file="${LOG_DIR}/run_${TIMESTAMP}.log"

# Function to log messages to both console and log file
log_message() {
    echo "[$(date +"%Y-%m-%d %H:%M:%S")] $1" | tee -a "$log_file"
}

log_message "Starting Bimonthly Long-Term Skill Metrics Recalculation"

# Verify Docker is running
if ! docker info > /dev/null 2>&1; then
    log_message "ERROR: Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if the Docker image exists, pull if not (once, not per-mode)
IMAGE_ID="mabesa/sapphire-postprocessing:${ieasyhydroforecast_backend_docker_image_tag:-latest}"
if ! docker image inspect $IMAGE_ID > /dev/null 2>&1; then
    log_message "Image $IMAGE_ID not found locally, pulling..."
    docker pull $IMAGE_ID
    if [ $? -ne 0 ]; then
        log_message "ERROR: Failed to pull Docker image $IMAGE_ID"
        exit 1
    fi
fi

# Establish SSH tunnel (if required for database access) — once, before the
# loop. See "Assumptions & risks" in the plan: the tunnel is NOT re-established
# per-mode to keep trap ownership simple. If the tunnel idle-drops mid-loop,
# later modes will fail and we will revisit in a follow-up.
establish_ssh_tunnel

# Set the trap to clean up processes on exit
trap cleanup EXIT

log_message "Starting long-term skill metrics recalculation across three modes..."
log_message "WARNING: This may take 60-120+ minutes depending on data volume."

# Iterate over the three long-term modes. Order matches run_locally.sh:
# MONTHLY first (most operationally visible), then QUARTERLY, then SEASONAL.
modes=(MONTHLY QUARTERLY SEASONAL)
failed_modes=()
for mode in "${modes[@]}"; do
    log_message "---- Mode: ${mode} ----"
    container_name="postprc-skill-recalc-${mode}"
    run_skill_metrics_recalc_once "$mode" "$LOG_DIR" "$TIMESTAMP" "$container_name"
    rc=$?
    if [ "$rc" -eq 0 ]; then
        log_message "[INFO] Mode ${mode}: success"
    else
        log_message "[WARN] Mode ${mode}: failed with exit ${rc}"
        failed_modes+=("$mode")
    fi
done

# Emit a summary line
if [ ${#failed_modes[@]} -eq 0 ]; then
    log_message "[SUMMARY] Completed 3/3 modes, 0 failures"
else
    log_message "[SUMMARY] Completed $((3 - ${#failed_modes[@]}))/3 modes, ${#failed_modes[@]} failure(s): ${failed_modes[*]}"
fi

# Clean up old log files (older than 15 days) — scoped to this job's LOG_DIR
log_message "Removing log files older than 15 days"
find "$LOG_DIR" -type f -mtime +15 -delete

log_message "Bimonthly Long-Term Skill Metrics Recalculation completed"
echo "|"
echo "| Recalculation complete. Check logs at: $LOG_DIR"
echo "|"

# Exit non-zero if any mode failed (placed AFTER log-rotation so a failure
# does not skip cleanup).
if [ ${#failed_modes[@]} -gt 0 ]; then
    exit 1
fi
exit 0
