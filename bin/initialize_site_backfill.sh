#!/bin/bash
# Initialize Site Backfill Script
#
# Runs preprocessing-runoff, linear-regression (PENTAD + DECAD), and
# skill-metrics recalculation in `initial` mode (full hindcast from a given
# start date). Use this script to recover historical data after a site-data
# purge or any other situation that requires rebuilding the archive.
#
# The daily maintenance scripts use SAPPHIRE_SYNC_MODE=maintenance with short
# lookback windows (30/90 days). After a purge going back further than those
# windows, those scripts cannot close the gap. This script forces
# SAPPHIRE_SYNC_MODE=initial with a user-supplied ieasyhydroforecast_START_DATE,
# then triggers a full skill-metrics recalculation. It runs against ALL
# stations; UPSERT is a no-op for rows that are already correct.
#
# Usage:
#   bash bin/initialize_site_backfill.sh <env_file_path> \
#       [--start-date YYYY-MM-DD] [--site-code N] \
#       [--skip-preprunoff] [--skip-linreg] [--skip-skill] \
#       [-h|--help]
#
# Arguments:
#   env_file_path         Positional, required. Path to the .env_<org> file.
#
# Options:
#   --start-date YYYY-MM-DD
#                         Override start date for the hindcast. If omitted,
#                         falls back to ieasyhydroforecast_START_DATE from the
#                         env file. Aborts if still empty after fallback.
#   --site-code N         3-10 digit station code used ONLY for log readability
#                         and post-run verification SQL counts. Does NOT filter
#                         the underlying container runs.
#   --skip-preprunoff     Skip Phase 1 (preprocessing-runoff).
#   --skip-linreg         Skip Phase 2 (linear-regression PENTAD + DECAD).
#   --skip-skill          Skip Phase 3 (skill-metrics recalculation).
#   -h, --help            Print this help message and exit 0.
#
# Prerequisites:
#   - Docker daemon running
#   - sapphire-preprocessing-db and sapphire-postprocessing-db containers
#     running (for post-phase verification queries)
#
# Author: Beatrice Marti

set -eo pipefail

# ---------------------------------------------------------------------------
# Source shared helpers
# ---------------------------------------------------------------------------
# shellcheck disable=SC1091  # path computed at runtime; can't be statically resolved
source "$(dirname "$0")/utils/common_functions.sh"
# shellcheck disable=SC1091  # path computed at runtime; can't be statically resolved
source "$(dirname "$0")/utils/run_skill_metrics_recalc.sh"

# ---------------------------------------------------------------------------
# Colors (matching purge_site_data.sh / reset_sapphire_db.sh)
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

# ---------------------------------------------------------------------------
# Defaults / flag variables
# ---------------------------------------------------------------------------
ENV_FILE=""
START_DATE=""
SITE_CODE=""
SKIP_PREPRUNOFF=false
SKIP_LINREG=false
SKIP_SKILL=false

PHASE_RESULTS=()

# ---------------------------------------------------------------------------
# Log helper — defined at top-level scope so all phase functions can call it
# before main() runs.  The log file path (log_file) is set inside main() and
# must exist before the first log_message call; early calls will write to
# /dev/null via the fallback below if log_file is not yet set.
# ---------------------------------------------------------------------------

log_message() {
    echo "[$(date +"%Y-%m-%d %H:%M:%S")] $1" | tee -a "${log_file:-/dev/null}"
}

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------

print_usage() {
    cat <<'USAGE'
Usage: bash bin/initialize_site_backfill.sh <env_file_path> [OPTIONS]

Rebuild historical data by running preprocessing-runoff, linear-regression
(PENTAD + DECAD), and skill-metrics recalculation in full-hindcast (initial)
mode.

Arguments:
  env_file_path         Path to the .env_<org> file (required).

Options:
  --start-date YYYY-MM-DD
                        Start date for the hindcast. Falls back to
                        ieasyhydroforecast_START_DATE in the env file.
  --site-code N         3–10 digit station code for log readability and
                        verification queries only. Does NOT filter container runs.
  --skip-preprunoff     Skip Phase 1 (preprocessing-runoff).
  --skip-linreg         Skip Phase 2 (linear-regression PENTAD + DECAD).
  --skip-skill          Skip Phase 3 (skill-metrics recalculation).
  -h, --help            Print this message and exit 0.

Examples:
  bash bin/initialize_site_backfill.sh /data/config/.env_kghm \
      --start-date 2010-01-01 --site-code 19999
  bash bin/initialize_site_backfill.sh /data/config/.env_kghm \
      --start-date 2010-01-01 --skip-skill
USAGE
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

parse_args() {
    # Handle --help / -h before requiring the positional arg
    for arg in "$@"; do
        case "$arg" in
            -h|--help) print_usage; exit 0 ;;
        esac
    done

    if [[ $# -eq 0 ]]; then
        echo -e "${RED}Error: env_file_path is required.${NC}" >&2
        echo "" >&2
        print_usage >&2
        exit 1
    fi

    # First positional argument is the env file
    if [[ "$1" != -* ]]; then
        ENV_FILE="$1"
        shift
    else
        echo -e "${RED}Error: first argument must be the env_file_path (got: $1).${NC}" >&2
        echo "" >&2
        print_usage >&2
        exit 1
    fi

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --start-date)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --start-date requires a YYYY-MM-DD value.${NC}" >&2
                    exit 1
                fi
                START_DATE="$2"
                if [[ ! "$START_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
                    echo -e "${RED}Error: --start-date '${START_DATE}' is not in YYYY-MM-DD format.${NC}" >&2
                    exit 1
                fi
                shift 2
                ;;
            --site-code)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --site-code requires a numeric value.${NC}" >&2
                    exit 1
                fi
                SITE_CODE="$2"
                shift 2
                ;;
            --skip-preprunoff)  SKIP_PREPRUNOFF=true; shift ;;
            --skip-linreg)      SKIP_LINREG=true;     shift ;;
            --skip-skill)       SKIP_SKILL=true;       shift ;;
            -h|--help)          print_usage; exit 0 ;;
            *)
                echo -e "${RED}Error: unknown argument: $1${NC}" >&2
                echo "" >&2
                print_usage >&2
                exit 1
                ;;
        esac
    done
}

# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------

validate_inputs() {
    # Validate env file exists
    if [[ ! -f "$ENV_FILE" ]]; then
        echo -e "${RED}Error: env file not found: ${ENV_FILE}${NC}" >&2
        exit 1
    fi

    # Validate --site-code format if supplied
    if [[ -n "$SITE_CODE" ]]; then
        if [[ ! "$SITE_CODE" =~ ^[0-9]{3,10}$ ]]; then
            echo -e "${RED}Error: --site-code '${SITE_CODE}' must be 3–10 decimal digits.${NC}" >&2
            exit 1
        fi
    fi
}

# ---------------------------------------------------------------------------
# Phase-result helpers (matching purge_site_data.sh pattern)
# ---------------------------------------------------------------------------

record_phase() {
    local phase="$1"
    local status="$2"   # PASS, FAIL, WARN, SKIP
    PHASE_RESULTS+=("${status}|${phase}")
}

# ---------------------------------------------------------------------------
# DB verification helper (best-effort; never aborts on query failure)
# ---------------------------------------------------------------------------

verify_db_counts() {
    local db_container="$1"   # e.g. sapphire-preprocessing-db
    local database="$2"       # e.g. preprocessing_db
    local table="$3"
    local start_date="$4"
    local site_code="$5"      # may be empty

    local sql
    if [[ -n "$site_code" ]]; then
        sql="SELECT COUNT(*) FROM ${table} WHERE code='${site_code}' AND date >= '${start_date}';"
        local label="${table} (site ${site_code}, date >= ${start_date})"
    else
        sql="SELECT code, COUNT(*) FROM ${table} WHERE date >= '${start_date}' GROUP BY code ORDER BY code;"
        local label="${table} (all sites, date >= ${start_date})"
    fi

    log_message "  Verifying ${database}.${label}"
    local result
    if result=$(docker exec "${db_container}" \
            psql -U postgres -d "${database}" -t -A -F '|' -c "${sql}" 2>/dev/null); then
        log_message "    Result: ${result:-<no rows>}"
    else
        log_message "  WARN: verification query failed for ${database}.${table} (DB container may not be running)"
    fi
}

# ---------------------------------------------------------------------------
# Phase 1: Preprocessing-runoff init
# ---------------------------------------------------------------------------

run_preprunoff() {
    local phase_name="Phase 1: preprocessing-runoff init"

    if [[ "$SKIP_PREPRUNOFF" == true ]]; then
        log_message "Skipping ${phase_name} (--skip-preprunoff)"
        record_phase "$phase_name" "SKIP"
        return 0
    fi

    log_message "========================================"
    log_message "${phase_name}"
    log_message "========================================"

    local IMAGE_ID="mabesa/sapphire-preprunoff:${ieasyhydroforecast_backend_docker_image_tag:-latest}"

    # Pull image if not present locally
    if ! docker image inspect "$IMAGE_ID" > /dev/null 2>&1; then
        log_message "Image ${IMAGE_ID} not found locally, pulling..."
        if ! docker pull "$IMAGE_ID"; then
            log_message "ERROR: Failed to pull Docker image ${IMAGE_ID}"
            record_phase "$phase_name" "FAIL"
            return 1
        fi
    fi

    local CONTAINER_NAME="preprunoff-backfill"
    local SERVICE_LOG="${LOG_DIR}/${CONTAINER_NAME}_${TIMESTAMP}.log"

    log_message "Container name: ${CONTAINER_NAME}"
    log_message "Service log: ${SERVICE_LOG}"
    log_message "START_DATE: ${START_DATE}"

    # Remove any stale container
    if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        log_message "Removing existing container: ${CONTAINER_NAME}"
        docker rm -f "${CONTAINER_NAME}"
    fi

    # shellcheck disable=SC2086  # DOCKER_HOST_OVERRIDE intentionally unquoted: empty=no arg; non-empty word-splits to "-e KEY=VAL"
    docker run \
        --name "${CONTAINER_NAME}" \
        --network host \
        -e "ieasyhydroforecast_data_root_dir=${ieasyhydroforecast_data_root_dir}" \
        -e "ieasyhydroforecast_env_file_path=${ieasyhydroforecast_env_file_path}" \
        -e SAPPHIRE_OPDEV_ENV=True \
        -e IN_DOCKER=True \
        -e SAPPHIRE_SYNC_MODE=initial \
        -e "ieasyhydroforecast_START_DATE=${START_DATE}" \
        ${DOCKER_HOST_OVERRIDE} \
        -v "${ieasyhydroforecast_data_ref_dir}/config:${ieasyhydroforecast_container_data_ref_dir}/config" \
        -v "${ieasyhydroforecast_data_ref_dir}/daily_runoff:${ieasyhydroforecast_container_data_ref_dir}/daily_runoff" \
        -v "${ieasyhydroforecast_data_ref_dir}/intermediate_data:${ieasyhydroforecast_container_data_ref_dir}/intermediate_data" \
        -v "${ieasyhydroforecast_data_ref_dir}/bin:${ieasyhydroforecast_container_data_ref_dir}/bin" \
        --memory=4g \
        --memory-swap=6g \
        "${IMAGE_ID}" \
        2>&1 | tee "${SERVICE_LOG}"

    local EXIT_CODE=$?
    local CONTAINER_EXIT_CODE
    CONTAINER_EXIT_CODE=$(docker inspect "${CONTAINER_NAME}" --format='{{.State.ExitCode}}' 2>/dev/null || echo "${EXIT_CODE}")

    if [[ "$CONTAINER_EXIT_CODE" -eq 0 ]]; then
        log_message "Phase 1 completed successfully"
        record_phase "$phase_name" "PASS"
    else
        log_message "WARNING: Phase 1 completed with exit code: ${CONTAINER_EXIT_CODE}"
        log_message "Check log file for details: ${SERVICE_LOG}"
        record_phase "$phase_name" "FAIL"
    fi

    docker rm -f "${CONTAINER_NAME}" 2>/dev/null || true

    log_message "Post-phase verification — preprocessing_db:"
    verify_db_counts "sapphire-preprocessing-db" "preprocessing_db" "runoffs"     "${START_DATE}" "${SITE_CODE}"
    verify_db_counts "sapphire-preprocessing-db" "preprocessing_db" "hydrographs" "${START_DATE}" "${SITE_CODE}"

    log_message "------"
    return "$CONTAINER_EXIT_CODE"
}

# ---------------------------------------------------------------------------
# Phase 2: Linear-regression init (PENTAD + DECAD)
# ---------------------------------------------------------------------------

run_linreg() {
    local phase_name="Phase 2: linear-regression init (PENTAD + DECAD)"

    if [[ "$SKIP_LINREG" == true ]]; then
        log_message "Skipping ${phase_name} (--skip-linreg)"
        record_phase "$phase_name" "SKIP"
        return 0
    fi

    log_message "========================================"
    log_message "${phase_name}"
    log_message "========================================"

    local IMAGE_ID="mabesa/sapphire-linreg:${ieasyhydroforecast_backend_docker_image_tag:-latest}"

    if ! docker image inspect "$IMAGE_ID" > /dev/null 2>&1; then
        log_message "Image ${IMAGE_ID} not found locally, pulling..."
        if ! docker pull "$IMAGE_ID"; then
            log_message "ERROR: Failed to pull Docker image ${IMAGE_ID}"
            record_phase "$phase_name" "FAIL"
            return 1
        fi
    fi

    local PREDICTION_MODES=("PENTAD" "DECAD")
    local phase_exit_code=0

    for MODE in "${PREDICTION_MODES[@]}"; do
        local CONTAINER_NAME="linreg-backfill-${MODE}"
        local SERVICE_LOG="${LOG_DIR}/${CONTAINER_NAME}_${TIMESTAMP}.log"

        log_message "Starting linear-regression init for ${MODE} mode..."
        log_message "Container name: ${CONTAINER_NAME}"
        log_message "Service log: ${SERVICE_LOG}"

        # Remove any stale container
        if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
            log_message "Removing existing container: ${CONTAINER_NAME}"
            docker rm -f "${CONTAINER_NAME}"
        fi

        # shellcheck disable=SC2086  # DOCKER_HOST_OVERRIDE intentionally unquoted: empty=no arg; non-empty word-splits to "-e KEY=VAL"
        docker run \
            --name "${CONTAINER_NAME}" \
            --network host \
            -e "ieasyhydroforecast_data_root_dir=${ieasyhydroforecast_data_root_dir}" \
            -e "ieasyhydroforecast_env_file_path=${ieasyhydroforecast_env_file_path}" \
            -e SAPPHIRE_OPDEV_ENV=True \
            -e IN_DOCKER=True \
            -e "SAPPHIRE_PREDICTION_MODE=${MODE}" \
            -e RUN_MODE=maintenance \
            -e SAPPHIRE_SYNC_MODE=initial \
            -e "ieasyhydroforecast_START_DATE=${START_DATE}" \
            ${DOCKER_HOST_OVERRIDE} \
            -v "${ieasyhydroforecast_data_ref_dir}/config:${ieasyhydroforecast_container_data_ref_dir}/config" \
            -v "${ieasyhydroforecast_data_ref_dir}/daily_runoff:${ieasyhydroforecast_container_data_ref_dir}/daily_runoff" \
            -v "${ieasyhydroforecast_data_ref_dir}/intermediate_data:${ieasyhydroforecast_container_data_ref_dir}/intermediate_data" \
            -v "${ieasyhydroforecast_data_ref_dir}/bin:${ieasyhydroforecast_container_data_ref_dir}/bin" \
            --memory=4g \
            --memory-swap=6g \
            "${IMAGE_ID}" \
            sh -c "uv run linear_regression.py --hindcast --start-date ${START_DATE}" \
            2>&1 | tee "${SERVICE_LOG}"

        local EXIT_CODE=$?
        local CONTAINER_EXIT_CODE
        CONTAINER_EXIT_CODE=$(docker inspect "${CONTAINER_NAME}" --format='{{.State.ExitCode}}' 2>/dev/null || echo "${EXIT_CODE}")

        if [[ "$CONTAINER_EXIT_CODE" -eq 0 ]]; then
            log_message "${MODE} init completed successfully"
        else
            log_message "WARNING: ${MODE} init completed with exit code: ${CONTAINER_EXIT_CODE}"
            log_message "Check log file for details: ${SERVICE_LOG}"
            phase_exit_code="$CONTAINER_EXIT_CODE"
        fi

        docker rm -f "${CONTAINER_NAME}" 2>/dev/null || true
        log_message "Finished ${MODE} mode"
        log_message "------"
    done

    log_message "Post-phase verification — postprocessing_db:"
    verify_db_counts "sapphire-postprocessing-db" "postprocessing_db" "lr_forecasts" "${START_DATE}" "${SITE_CODE}"

    if [[ "$phase_exit_code" -eq 0 ]]; then
        record_phase "$phase_name" "PASS"
    else
        record_phase "$phase_name" "FAIL"
    fi

    return "$phase_exit_code"
}

# ---------------------------------------------------------------------------
# Phase 3: Skill-metrics recalculation
# ---------------------------------------------------------------------------

run_skill_recalc() {
    local phase_name="Phase 3: skill-metrics recalculation"

    if [[ "$SKIP_SKILL" == true ]]; then
        log_message "Skipping ${phase_name} (--skip-skill)"
        record_phase "$phase_name" "SKIP"
        return 0
    fi

    log_message "========================================"
    log_message "${phase_name}"
    log_message "========================================"

    # Pull the postprocessing image if not present
    local IMAGE_ID="mabesa/sapphire-postprocessing:${ieasyhydroforecast_backend_docker_image_tag:-latest}"
    if ! docker image inspect "$IMAGE_ID" > /dev/null 2>&1; then
        log_message "Image ${IMAGE_ID} not found locally, pulling..."
        if ! docker pull "$IMAGE_ID"; then
            log_message "ERROR: Failed to pull Docker image ${IMAGE_ID}"
            record_phase "$phase_name" "FAIL"
            return 1
        fi
    fi

    # Delegate to the shared helper (sourced at top of script)
    run_skill_metrics_recalc_once "BOTH" "${LOG_DIR}" "${TIMESTAMP}" "postprc-backfill"
    local SKILL_EXIT_CODE=$?

    if [[ "$SKILL_EXIT_CODE" -eq 0 ]]; then
        log_message "Phase 3 completed successfully"
        record_phase "$phase_name" "PASS"
    else
        log_message "WARNING: Phase 3 completed with exit code: ${SKILL_EXIT_CODE}"
        record_phase "$phase_name" "FAIL"
    fi

    log_message "Post-phase verification — postprocessing_db:"
    verify_db_counts "sapphire-postprocessing-db" "postprocessing_db" "skill_metrics" "${START_DATE}" "${SITE_CODE}"

    log_message "------"
    return "$SKILL_EXIT_CODE"
}

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

print_summary() {
    echo ""
    echo -e "${BOLD}========================================"
    echo -e " BACKFILL SUMMARY"
    echo -e "========================================${NC}"
    echo ""

    local any_failed=false
    for entry in "${PHASE_RESULTS[@]}"; do
        local status="${entry%%|*}"
        local phase="${entry#*|}"
        case "$status" in
            PASS) echo -e "  ${GREEN}PASS${NC}  ${phase}" ;;
            FAIL) echo -e "  ${RED}FAIL${NC}  ${phase}"; any_failed=true ;;
            WARN) echo -e "  ${YELLOW}WARN${NC}  ${phase}" ;;
            SKIP) echo -e "  ${BLUE}SKIP${NC}  ${phase}" ;;
        esac
    done

    echo ""

    if [[ "$any_failed" == true ]]; then
        echo -e "${RED}One or more phases failed. Check the logs at: ${LOG_DIR}${NC}"
        return 1
    fi

    echo -e "${GREEN}Backfill complete. Restart the dashboard to pick up the new data:${NC}"
    echo "  docker restart sapphire-dashboard"
    echo ""
    return 0
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
    parse_args "$@"
    validate_inputs

    # Print the banner (from common_functions.sh)
    print_banner
    echo "| Running Site Backfill (initial / full-hindcast mode)"

    # Load .env vars and derive ieasyhydroforecast_* path variables
    read_configuration "${ENV_FILE}"

    # Validate required env vars (same guard as daily scripts)
    if [[ -z "${ieasyhydroforecast_data_root_dir:-}" ]] || \
       [[ -z "${ieasyhydroforecast_env_file_path:-}" ]] || \
       [[ -z "${ieasyhydroforecast_data_ref_dir:-}" ]] || \
       [[ -z "${ieasyhydroforecast_container_data_ref_dir:-}" ]]; then
        echo "| Error: Required environment variables are not set. Please check your .env file."
        exit 1
    fi

    # Resolve start date: CLI flag wins; fall back to env var; abort if empty
    if [[ -z "$START_DATE" ]]; then
        START_DATE="${ieasyhydroforecast_START_DATE:-}"
        if [[ -z "$START_DATE" ]]; then
            echo -e "${RED}Error: --start-date was not provided and ieasyhydroforecast_START_DATE is not set in the env file.${NC}" >&2
            exit 1
        fi
        # Validate the date format sourced from env (the CLI path was already validated)
        if [[ ! "$START_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
            echo -e "${RED}Error: ieasyhydroforecast_START_DATE '${START_DATE}' is not in YYYY-MM-DD format.${NC}" >&2
            exit 1
        fi
    fi

    # Create log directory
    LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/site_backfill"
    mkdir -p "${LOG_DIR}"
    echo "| Log directory: ${LOG_DIR}"

    # Timestamped main log file
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    log_file="${LOG_DIR}/run_${TIMESTAMP}.log"

    log_message "Starting site backfill run"
    log_message "  env_file:   ${ENV_FILE}"
    log_message "  start_date: ${START_DATE}"
    [[ -n "$SITE_CODE" ]] && log_message "  site_code:  ${SITE_CODE} (log/verify only)"
    log_message "  skip flags: preprunoff=${SKIP_PREPRUNOFF} linreg=${SKIP_LINREG} skill=${SKIP_SKILL}"

    # Verify Docker is running
    if ! docker info > /dev/null 2>&1; then
        log_message "ERROR: Docker is not running. Please start Docker and try again."
        exit 1
    fi

    # macOS Docker compatibility — copy exact pattern from daily_linreg_maintenance.sh
    DOCKER_HOST_OVERRIDE=""
    if [[ "$(uname)" == "Darwin" ]]; then
        if [[ "${IEASYHYDROHF_HOST:-}" == *"localhost"* ]]; then
            local DOCKER_IEASYHYDROHF_HOST="${IEASYHYDROHF_HOST//localhost/host.docker.internal}"
            log_message "macOS detected: overriding IEASYHYDROHF_HOST for Docker container"
            log_message "  Original: ${IEASYHYDROHF_HOST}"
            log_message "  Docker:   ${DOCKER_IEASYHYDROHF_HOST}"
            DOCKER_HOST_OVERRIDE="-e IEASYHYDROHF_HOST=${DOCKER_IEASYHYDROHF_HOST}"
        fi
    fi

    # Establish SSH tunnel (no-op when ieasyhydroforecast_ssh_to_iEH != true)
    establish_ssh_tunnel

    # Register cleanup trap
    trap cleanup EXIT

    # ---------------------------------------------------------------------------
    # Run phases — each function uses PHASE_RESULTS, LOG_DIR, TIMESTAMP,
    # START_DATE, SITE_CODE, DOCKER_HOST_OVERRIDE, and log_message() from scope.
    # ---------------------------------------------------------------------------
    local overall_exit=0

    run_preprunoff  || overall_exit=1
    run_linreg      || overall_exit=1
    run_skill_recalc || overall_exit=1

    # Clean up old log files (older than 15 days)
    log_message "Removing log files older than 15 days"
    find "${LOG_DIR}" -type f -mtime +15 -delete

    log_message "Site backfill run finished"

    print_summary || overall_exit=1

    exit "$overall_exit"
}

main "$@"
