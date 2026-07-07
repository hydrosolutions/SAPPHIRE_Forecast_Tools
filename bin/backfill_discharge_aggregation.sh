#!/usr/bin/env bash
# Discharge-Aggregation Historical Backfill Wrapper (M4)
#
# Thin wrapper around apps/preprocessing_runoff/backfill_discharge_aggregation.py,
# which reuses the M2/M3 pentad/decad/month/quarter/season hydrograph writers via a
# write-capturing client to safely backfill past years: diff + snapshot BEFORE any
# live write, then a post-write verification that fails loudly on mismatch.
#
# Unlike the Docker-orchestrating maintenance scripts in this directory, this
# wrapper runs the backfill directly via `uv run` against the
# apps/preprocessing_runoff venv (no Docker, no SSH tunnel) -- the underlying
# script talks to the SAPPHIRE preprocessing API the same way other `uv run`
# invocations do when run outside Docker.
#
# Usage:
#   bash bin/backfill_discharge_aggregation.sh <env_file_path> [--years N] [--target-year YYYY] [--dry-run]
#
# Examples:
#   # Dry-run the default 3 most-recent complete years
#   bash bin/backfill_discharge_aggregation.sh /path/to/config/.env --dry-run
#
#   # Live backfill of a single year
#   bash bin/backfill_discharge_aggregation.sh /path/to/config/.env --target-year 2024
#
# Author: Beatrice Marti

set -euo pipefail

# Print help and exit if requested
if [ "${1-}" = "--help" ] || [ "${1-}" = "-h" ]; then
    sed -n '2,24p' "$0"
    exit 0
fi

# Parse positional .env path + optional --years N / --target-year YYYY / --dry-run
ENV_FILE_PATH="${1-}"
YEARS=""
TARGET_YEAR=""
DRY_RUN=""

if [ -n "$ENV_FILE_PATH" ]; then
    shift
fi

while [ $# -gt 0 ]; do
    case "$1" in
        --years)
            if [ $# -lt 2 ]; then
                echo "| Error: --years requires an integer argument" >&2
                exit 1
            fi
            YEARS="$2"
            shift 2
            ;;
        --years=*)
            YEARS="${1#*=}"
            shift
            ;;
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
        --dry-run)
            DRY_RUN="--dry-run"
            shift
            ;;
        *)
            echo "| Error: unknown argument: $1" >&2
            echo "| Usage: $0 <env_file_path> [--years N] [--target-year YYYY] [--dry-run]" >&2
            exit 1
            ;;
    esac
done

# Source the common functions
# shellcheck source=bin/utils/common_functions.sh
source "$(dirname "$0")/utils/common_functions.sh"

print_banner
echo "| Running Discharge-Aggregation Historical Backfill (M4)"

# Read the configuration from the .env file. common_functions.sh sources the
# env file and predates strict unset-variable checks, so relax -u only while
# loading configuration.
set +u
read_configuration "$ENV_FILE_PATH"
set -u

if [ -z "${ieasyhydroforecast_data_root_dir-}" ]; then
    echo "| Error: ieasyhydroforecast_data_root_dir is not set. Please check your .env file."
    exit 1
fi

LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/discharge_aggregation_backfill"
mkdir -p "${LOG_DIR}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${LOG_DIR}/backfill_${TIMESTAMP}.log"
echo "| Log file: ${LOG_FILE}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$SCRIPT_DIR/../apps/preprocessing_runoff"

RUNNER_CMD=(uv run backfill_discharge_aggregation.py)
if [ -n "$YEARS" ]; then
    RUNNER_CMD+=(--years "$YEARS")
fi
if [ -n "$TARGET_YEAR" ]; then
    RUNNER_CMD+=(--target-year "$TARGET_YEAR")
fi
if [ -n "$DRY_RUN" ]; then
    RUNNER_CMD+=(--dry-run)
fi

echo "| Command: ${RUNNER_CMD[*]} (run from $APP_DIR)"

set +e
(cd "$APP_DIR" && "${RUNNER_CMD[@]}") 2>&1 | tee "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}
set -e

if [ "$EXIT_CODE" -eq 0 ]; then
    echo "| Discharge-aggregation backfill completed successfully"
else
    echo "| WARNING: Discharge-aggregation backfill exited with code: $EXIT_CODE"
    echo "| Check log file for details: $LOG_FILE"
fi

exit "$EXIT_CODE"
