#!/usr/bin/env bash
# ============================================================================
# initialize_runoff_day_history.sh
#
# Runoff DAY CSV-to-API migration wrapper (P1a of the update-time migration
# toolkit). Reads <data_ref>/intermediate_data/runoff_day.csv (or a CSV with
# header code,date,discharge) and POSTs records to the preprocessing API.
#
# Uses the P0 helper (bin/utils/update_migration_helpers.sh) for image
# resolution, temp workspace, redacted logging, and uniform image log line.
# Mounts the P0 migration_py/ package into the prepgateway container so the
# per-row POST logic lives in importable stdlib-only Python — no embedded
# heredoc beyond a minimal entry shim.
#
# MODE detection: queries the target preprocessing-db once via docker exec
# psql. Empty (count=0 OR min_date IS NULL) -> full-import; populated ->
# pre-cutoff (cutoff = MIN(date)).
#
# Idempotency: the preprocessing service upserts on (horizon_type, code, date).
# Reruns are safe.
#
# Universal safe-write rule (architecture Q2 layer 2): only non-NULL fields
# are sent. Rows without a parseable discharge are skipped, not POSTed with
# discharge=null.
#
# Forward interface contract (P0 gi_draft): --station-filter <code> is the
# binding flag name reused by P1b / P1c / P3.
#
# Usage:
#   bash bin/initialize_runoff_day_history.sh <env_file_path> [OPTIONS]
#
# Arguments:
#   env_file_path        Path to the .env_<org> file (required).
#
# Options:
#   --dry-run            Read + filter; do NOT POST anything.
#   --api-url <URL>      Target endpoint (default: http://localhost:8002/runoff/).
#   --batch-size <N>     Records per POST batch (default: 500).
#   --image <IMAGE>      Docker image override.
#   --station-filter <CODE>
#                        Filter source CSV rows to a single station code.
#   -h, --help           Print this message and exit 0.
#
# Examples:
#   bash bin/initialize_runoff_day_history.sh /data/taj_data/config/.env_tjhm --dry-run
#   bash bin/initialize_runoff_day_history.sh /data/taj_data/config/.env_tjhm
#   bash bin/initialize_runoff_day_history.sh /data/taj_data/config/.env_tjhm \
#       --station-filter 19999 --dry-run
# ============================================================================

set -eo pipefail
# NOTE: set -u is intentionally omitted; common_functions.sh is not strict-mode safe.

# ---------------------------------------------------------------------------
# Source shared helpers (P0 foundation)
# ---------------------------------------------------------------------------
# shellcheck disable=SC1091  # path computed at runtime; can't be statically resolved
source "$(dirname "$0")/utils/update_migration_helpers.sh"

# ---------------------------------------------------------------------------
# Colors
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

# ---------------------------------------------------------------------------
# Defaults / flag variables
# ---------------------------------------------------------------------------
ENV_FILE=""
DRY_RUN=false
API_URL="http://localhost:8002/runoff/"
BATCH_SIZE=500
IMAGE_OVERRIDE=""
STATION_FILTER=""

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------

print_usage() {
    cat <<'USAGE'
Usage: bash bin/initialize_runoff_day_history.sh <env_file_path> [OPTIONS]

Migrate runoff DAY rows from intermediate_data/runoff_day.csv into the
preprocessing API. Honors MODE=full-import vs pre-cutoff based on the target
table state.

Arguments:
  env_file_path        Path to the .env_<org> file (required).

Options:
  --dry-run            Read + filter; do NOT POST anything.
  --api-url <URL>      Target endpoint (default: http://localhost:8002/runoff/).
  --batch-size <N>     Records per POST batch (default: 500).
  --image <IMAGE>      Docker image override.
  --station-filter <CODE>
                       Filter source CSV rows to a single station code
                       (binding interface contract from P0 — honored by all
                       CSV-source wrappers).
  -h, --help           Print this message and exit 0.

Examples:
  bash bin/initialize_runoff_day_history.sh /data/taj_data/config/.env_tjhm --dry-run
  bash bin/initialize_runoff_day_history.sh /data/taj_data/config/.env_tjhm
  bash bin/initialize_runoff_day_history.sh /data/taj_data/config/.env_tjhm \
      --station-filter 19999 --dry-run
USAGE
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

parse_args() {
    # Handle --help / -h before requiring the positional arg.
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

    # First positional argument is the env file.
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
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --api-url)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --api-url requires a URL value.${NC}" >&2
                    exit 1
                fi
                API_URL="$2"
                shift 2
                ;;
            --batch-size)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --batch-size requires a numeric value.${NC}" >&2
                    exit 1
                fi
                BATCH_SIZE="$2"
                shift 2
                ;;
            --image)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --image requires an image name.${NC}" >&2
                    exit 1
                fi
                IMAGE_OVERRIDE="$2"
                shift 2
                ;;
            --station-filter)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --station-filter requires a station code value.${NC}" >&2
                    exit 1
                fi
                STATION_FILTER="$2"
                shift 2
                ;;
            -h|--help)
                print_usage
                exit 0
                ;;
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
# Query the target preprocessing-db for MODE detection.
# Echoes "count<TAB>min_date_or_empty" on stdout (e.g. "0\t" or "100\t2024-01-15").
# Returns non-zero if the docker exec fails.
# ---------------------------------------------------------------------------
query_target_state() {
    docker exec sapphire-preprocessing-db psql \
        -U postgres -d preprocessing_db -P pager=off -t -A -F $'\t' \
        -c "SELECT COUNT(*), COALESCE(MIN(date)::text, '') FROM runoffs WHERE horizon_type='DAY';"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
    parse_args "$@"

    print_banner
    echo "| Running Runoff DAY History Initialization (CSV -> API push)"

    # Validate env file exists before loading.
    if [[ ! -f "$ENV_FILE" ]]; then
        echo -e "${RED}Error: env file not found: ${ENV_FILE}${NC}" >&2
        exit 1
    fi

    # Load .env vars and derive ieasyhydroforecast_* path variables.
    read_configuration "${ENV_FILE}"

    # Validate required env vars.
    if [[ -z "${ieasyhydroforecast_data_root_dir:-}" ]] || \
       [[ -z "${ieasyhydroforecast_data_ref_dir:-}" ]]; then
        echo -e "${RED}Error: Required environment variables are not set. Check your .env file.${NC}" >&2
        exit 1
    fi

    # Resolve image via the P0 helper (CLI override -> configured tag -> :latest).
    local IMAGE
    IMAGE="$(umh_resolve_image "$IMAGE_OVERRIDE" "${ieasyhydroforecast_backend_docker_image_tag:-}")"

    # Derived paths.
    local CSV_HOST="${ieasyhydroforecast_data_ref_dir}/intermediate_data/runoff_day.csv"
    local LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/runoff_day_history_init"
    mkdir -p "${LOG_DIR}"

    local TIMESTAMP
    TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
    log_file="${LOG_DIR}/runoff_day_history_init_${TIMESTAMP}.log"
    export log_file

    echo "| Log file: ${log_file}"
    echo ""

    umh_log_redacted "Starting runoff DAY history initialization"
    umh_log_redacted "  env_file:        ${ENV_FILE}"
    umh_log_redacted "  csv_path:        ${CSV_HOST}"
    umh_log_redacted "  api_url:         ${API_URL}"
    umh_log_redacted "  batch_size:      ${BATCH_SIZE}"
    umh_log_redacted "  station_filter:  ${STATION_FILTER:-<none>}"
    umh_log_redacted "  dry_run:         ${DRY_RUN}"
    umh_print_image_resolution_line "${IMAGE}"

    # -------------------------------------------------------------------------
    # Pre-flight checks
    # -------------------------------------------------------------------------

    # 1. CSV must exist.
    if [[ ! -f "$CSV_HOST" ]]; then
        umh_log_redacted "ERROR: CSV not found: ${CSV_HOST}"
        exit 1
    fi
    umh_log_redacted "Pre-flight: CSV OK (${CSV_HOST})"

    # 2. Docker must be running.
    if ! docker info > /dev/null 2>&1; then
        umh_log_redacted "ERROR: Docker is not running. Please start Docker and try again."
        exit 1
    fi
    umh_log_redacted "Pre-flight: Docker daemon OK"

    # 3. Pull image if not present locally.
    if ! docker image inspect "$IMAGE" > /dev/null 2>&1; then
        umh_log_redacted "Image ${IMAGE} not found locally, pulling..."
        if ! docker pull "$IMAGE"; then
            umh_log_redacted "ERROR: Failed to pull Docker image ${IMAGE}"
            exit 1
        fi
        umh_log_redacted "Pull complete: ${IMAGE}"
    else
        umh_log_redacted "Pre-flight: image present locally (${IMAGE})"
    fi

    # -------------------------------------------------------------------------
    # MODE detection — query target preprocessing-db.
    # -------------------------------------------------------------------------
    local MODE="full-import"
    local CUTOFF=""
    local TARGET_COUNT=0
    local TARGET_MIN_DATE=""

    if docker ps --filter "name=sapphire-preprocessing-db" --quiet | grep -q .; then
        local query_out
        if query_out="$(query_target_state 2>&1)"; then
            # query_out format: "<count>\t<min_date_or_empty>"
            TARGET_COUNT="${query_out%%$'\t'*}"
            TARGET_MIN_DATE="${query_out#*$'\t'}"
            # Trim any trailing newline / whitespace.
            TARGET_COUNT="${TARGET_COUNT//[$'\r\n ']/}"
            TARGET_MIN_DATE="${TARGET_MIN_DATE//[$'\r\n ']/}"
            umh_log_redacted "Target state: count=${TARGET_COUNT} min_date=${TARGET_MIN_DATE:-<null>}"
            if [[ "$TARGET_COUNT" != "0" && -n "$TARGET_MIN_DATE" ]]; then
                MODE="pre-cutoff"
                CUTOFF="$TARGET_MIN_DATE"
            else
                MODE="full-import"
                CUTOFF=""
            fi
        else
            umh_log_redacted "WARN: target query failed (${query_out}); assuming full-import"
            MODE="full-import"
            CUTOFF=""
        fi
    else
        umh_log_redacted "WARN: sapphire-preprocessing-db not running; assuming full-import"
        MODE="full-import"
        CUTOFF=""
    fi

    umh_log_redacted "MODE=${MODE}$( [[ -n "$CUTOFF" ]] && echo " (cutoff=${CUTOFF})" || echo " (target empty)")"

    # -------------------------------------------------------------------------
    # Acquire a temp workspace (mode 0o700, trap-cleaned on EXIT INT TERM).
    # -------------------------------------------------------------------------
    local TMPDIR_RUNOFF
    TMPDIR_RUNOFF="$(umh_acquire_temp_workspace runoff_day)"
    umh_log_redacted "Temp workspace: ${TMPDIR_RUNOFF}"

    # -------------------------------------------------------------------------
    # Run the Python helper inside the prepgateway image.
    # Mount migration_py/ read-only; mount the CSV read-only.
    # -------------------------------------------------------------------------
    local HELPER_DIR
    HELPER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/utils" && pwd)"

    local DRY_RUN_FLAG=""
    if [[ "$DRY_RUN" == true ]]; then
        DRY_RUN_FLAG="--dry-run"
    fi

    local CUTOFF_ARGS=()
    if [[ -n "$CUTOFF" ]]; then
        CUTOFF_ARGS=("--cutoff" "$CUTOFF")
    fi

    local STATION_ARGS=()
    if [[ -n "$STATION_FILTER" ]]; then
        STATION_ARGS=("--station-filter" "$STATION_FILTER")
    fi

    umh_log_redacted "========================================"
    umh_log_redacted "Running runoff DAY push via docker"
    umh_log_redacted "  image:        ${IMAGE}"
    umh_log_redacted "  csv:          ${CSV_HOST} -> /runoff_day.csv (ro)"
    umh_log_redacted "  migration_py: ${HELPER_DIR}/migration_py -> /opt/migration_py (ro)"
    umh_log_redacted "  api_url:      ${API_URL}"
    umh_log_redacted "  batch_size:   ${BATCH_SIZE}"
    if [[ "$DRY_RUN" == true ]]; then
        umh_log_redacted "  mode:         DRY RUN (no POSTs)"
    else
        umh_log_redacted "  mode:         REAL run"
    fi
    umh_log_redacted "========================================"

    # shellcheck disable=SC2086  # DRY_RUN_FLAG intentionally unquoted: empty=no arg
    docker run --rm --network host \
        -v "${CSV_HOST}:/runoff_day.csv:ro" \
        -v "${HELPER_DIR}/migration_py:/opt/migration_py:ro" \
        -e "PYTHONPATH=/opt" \
        "$IMAGE" \
        python3 -m migration_py.runoff_day \
            --csv-path /runoff_day.csv \
            --api-url "${API_URL}" \
            --batch-size "${BATCH_SIZE}" \
            "${CUTOFF_ARGS[@]}" \
            "${STATION_ARGS[@]}" \
            ${DRY_RUN_FLAG} \
        2>&1 | tee -a "${log_file}"

    local DOCKER_EXIT=${PIPESTATUS[0]}

    # -------------------------------------------------------------------------
    # Post-run summary.
    # -------------------------------------------------------------------------
    echo ""
    echo -e "${BOLD}========================================"
    echo -e " RUNOFF DAY HISTORY INIT COMPLETE"
    echo -e "========================================${NC}"
    echo ""

    if [[ "$DOCKER_EXIT" -ne 0 ]]; then
        echo -e "${RED}Docker run exited with code ${DOCKER_EXIT}. Check the log above and:${NC}"
        echo "  ${log_file}"
        echo ""
    else
        if [[ "$DRY_RUN" == true ]]; then
            echo -e "${YELLOW}DRY RUN complete — no records were POSTed.${NC}"
            echo "  Re-run without --dry-run to push data."
        else
            echo -e "${GREEN}Push complete.${NC}"
        fi
        echo ""
    fi

    echo "Verify rows in the preprocessing DB:"
    echo "  docker exec sapphire-preprocessing-db \\"
    echo "    psql -U postgres -d preprocessing_db -c \\"
    echo "    \"SELECT horizon_type, COUNT(*) AS rows, MIN(date), MAX(date) FROM runoffs WHERE horizon_type='DAY' GROUP BY horizon_type;\""
    echo ""
    echo "Log file: ${log_file}"

    exit "$DOCKER_EXIT"
}

main "$@"
