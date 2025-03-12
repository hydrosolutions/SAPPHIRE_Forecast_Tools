#!/bin/bash
#==============================================================================
# SAPPHIRE BACKEND DAILY MAINTENANCE
#==============================================================================
# Description:
#   This script orchestrates the daily maintenance sequence for the SAPPHIRE
#   forecasting tools backend. It handles Docker image updates, ML model
#   maintenance, and log file management.
#
# Usage:
#   bash bin/daily_maintenance_sapphire_backend.sh <env_file_path>
#
#   Example:
#     bash bin/daily_maintenance_sapphire_backend.sh /path/to/config/.env
#
#   For unattended execution:
#     nohup bash bin/daily_maintenance_sapphire_backend.sh /path/to/config/.env > /dev/null 2>&1 &
#
# Features:
#   1. Pre-pulls & verifies Docker images for faster operational forecasts
#   2. Performs maintenance on ML models to keep predictions accurate
#   3. Manages log files (compression and cleanup)
#   4. Provides detailed logging of all operations
#   5. Handles errors gracefully with non-critical/critical designations
#
# Schedule:
#   This script should be scheduled to run daily before the operational
#   forecast (recommended: 2 hours before). Example crontab entry:
#   0 2 * * * cd /path/to/SAPPHIRE_forecast_tools && bash bin/daily_maintenance_sapphire_backend.sh /path/to/config/.env
#
# Maintenance Tasks:
#   - Docker Images:  Pulls and verifies latest images for forecast operations
#   - ML Maintenance: Updates model metadata, checks performance
#   - Log Management: Compresses logs >7 days old, deletes logs >15 days old
#
# Output:
#   All operations are logged to logs/daily_operations_YYYYMMDD.log
#
# Dependencies:
#   - Requires utility scripts in the bin/utils/ directory
#   - Needs write access to the logs directory
#   - Requires Docker to be running
#
# # Contributors:
#   - Beatrice Marti: Implementation and integration
#   - Developed with assistance from AI pair programming tools
#==============================================================================


# Exit on error
set -e

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Get the config file path
if [ -z "$1" ]; then
    echo "Usage: $0 <env_file_path>"
    exit 1
fi
ENV_FILE="$1"

# Set up logging
LOG_DIR="${PROJECT_ROOT}/logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +"%Y%m%d")
MASTER_LOG="${LOG_DIR}/daily_operations_${TIMESTAMP}.log"

# Function to run a script and log its output
# Assumes scripts are stored in bin/utils/ directory.
run_script() {
    local script_name="$1"
    local script_path="${SCRIPT_DIR}/utils/$script_name"
    local start_time=$(date +"%Y-%m-%d %H:%M:%S")

    echo "=========================================" | tee -a "$MASTER_LOG"
    echo "Starting: $script_name at $start_time" | tee -a "$MASTER_LOG"
    echo "=========================================" | tee -a "$MASTER_LOG"

    # Run the script and capture its exit status
    bash "$script_path" "$ENV_FILE" 2>&1 | tee -a "$MASTER_LOG"
    local status=${PIPESTATUS[0]}

    local end_time=$(date +"%Y-%m-%d %H:%M:%S")
    echo "----------------------------------------" | tee -a "$MASTER_LOG"
    if [ $status -eq 0 ]; then
        echo "✅ $script_name completed successfully at $end_time" | tee -a "$MASTER_LOG"
    else
        echo "❌ $script_name FAILED with status $status at $end_time" | tee -a "$MASTER_LOG"

        # Decide whether to continue or exit based on script criticality
        if [ "$2" == "critical" ]; then
            echo "Critical script failed - aborting sequence" | tee -a "$MASTER_LOG"
            exit $status
        else
            echo "Non-critical script failed - continuing sequence" | tee -a "$MASTER_LOG"
        fi
    fi
    echo "=========================================" | tee -a "$MASTER_LOG"

    return $status
}

# Function to clean up logs
clean_up_logs() {
    echo "Starting log maintenance..." | tee -a "$MASTER_LOG"

    # Compress logs older than 7 days but younger than 15 days
    echo "Compressing logs 7-15 days old..." | tee -a "$MASTER_LOG"
    find "$LOG_DIR" -name "*.log" -type f -mtime +7 -mtime -15 -exec gzip {} \; 2>/dev/null || true

    # Count compressed logs
    compressed_count=$(find "$LOG_DIR" -name "*.log.gz" -type f -mtime +7 -mtime -15 | wc -l)
    echo "Compressed $compressed_count log files" | tee -a "$MASTER_LOG"

    # Delete logs older than 15 days (both compressed and uncompressed)
    echo "Deleting logs older than 15 days..." | tee -a "$MASTER_LOG"
    old_logs_count=$(find "$LOG_DIR" -type f \( -name "*.log" -o -name "*.log.gz" \) -mtime +15 | wc -l)
    find "$LOG_DIR" -type f \( -name "*.log" -o -name "*.log.gz" \) -mtime +15 -delete
    echo "Deleted $old_logs_count log files older than 15 days" | tee -a "$MASTER_LOG"

    # Report current disk usage of the log directory
    log_size=$(du -sh "$LOG_DIR" | cut -f1)
    echo "Current log directory size: $log_size" | tee -a "$MASTER_LOG"

    echo "Log maintenance completed" | tee -a "$MASTER_LOG"
}

# Log the start of the daily operations
echo "=========================================" | tee -a "$MASTER_LOG"
echo "SAPPHIRE DAILY OPERATIONS - $(date +"%Y-%m-%d %H:%M:%S")" | tee -a "$MASTER_LOG"
echo "=========================================" | tee -a "$MASTER_LOG"

# Step 1: Update Docker Images (non-critical - if it fails, we'll still try to run forecasts)
run_script "daily_docker_image_update.sh" "non-critical"

# Step 2: Run the ml-maintenance script (non-critical - if it fails, we'll still try to run forecasts)
run_script "daily_ml_maintenance.sh" "non-critical"

# Step 3: Any additional scripts you want to add later (non-critical example)
# run_script "daily_linreg_maintenance.sh" "non-critical"

# Step 4: Clean up logs (keep system tidy)
clean_up_logs

# Log completion
echo "Daily operations completed at $(date +"%Y-%m-%d %H:%M:%S")" | tee -a "$MASTER_LOG"

exit 0