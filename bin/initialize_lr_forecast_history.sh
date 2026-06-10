#!/usr/bin/env bash
# ============================================================================
# initialize_lr_forecast_history.sh
#
# LR (linear-regression) forecast laptop-export -> CSV -> API migration
# wrapper (P4a of the update-time migration toolkit). Consumes a CSV exported
# on the operator's laptop (via bin/export_lr_forecast_history.sh) and pushes
# rows to the postprocessing API /lr-forecast/ endpoint.
#
# Uses the P0 helper (bin/utils/update_migration_helpers.sh) for image
# resolution, manifest validation, temp workspace, redacted logging, and the
# uniform image log line. Mounts the P0 migration_py/ package into the
# prepgateway container so the per-row POST logic lives in importable
# stdlib-only Python — no embedded heredoc beyond a minimal entry shim.
#
# IMPORTANT — source DB:
#   The lr_forecasts table lives in sapphire-postprocessing-db (NOT
#   preprocessing-db). All MODE-detection psql queries here go through
#   `docker exec sapphire-postprocessing-db psql ... postprocessing_db`.
#
# IMPORTANT — no model_type column:
#   lr_forecasts has the DB unique key (horizon_type, code, date). LR is
#   implicit in this table (which is why LR rows are filtered OUT of
#   combined_forecasts by the P-postprocessing combined_forecasts migrator).
#   Wrappers must NOT add a model_type filter or column.
#
# Universal safe-write rule (architecture Q2 layer 2):
#   LR forecast rows have many nullable model-stat fields (slope, intercept,
#   delta, rsquared, etc.). The wrapper sends ONLY non-NULL fields from the
#   source CSV. The service-side _has_changes + setattr path overwrites
#   existing non-NULL with incoming NULL, so the wrapper never injects null.
#   Enrichment-only: complete export rows preferred (architecture Q2 table
#   policy for lr_forecasts).
#
# Manifest validation (Stage E item #2):
#   The export CSV must be accompanied by a <csv>.manifest sidecar produced
#   by bin/export_lr_forecast_history.sh. The wrapper validates the manifest
#   BEFORE the docker run (export_type=lr_forecast; row_count; station_count;
#   date_min; date_max). Validation failure aborts before any POST.
#
# MODE detection: queries the target postprocessing-db once via docker exec
# psql, scoped to the requested horizon. Empty (count=0 OR min_date IS NULL)
# -> full-import; populated -> pre-cutoff (cutoff = MIN(date)).
#
# Idempotency: the postprocessing service upserts on (horizon_type, code,
# date). Reruns are safe.
#
# Usage:
#   bash bin/initialize_lr_forecast_history.sh <env_file_path> \
#       --from-export <path> \
#       --horizon pentad|decade \
#       [--dry-run] [--api-url URL] [--batch-size N] [--image IMAGE] \
#       [--station-filter CODE]
#
# Arguments:
#   env_file_path        Path to the .env_<org> file (required).
#
# Options:
#   --from-export <PATH> Path to the laptop-exported CSV (required).
#                        Must be under the deployment data-root logs tree
#                        (e.g. ${ieasyhydroforecast_data_root_dir}/logs/
#                        lr_forecast_tmp/<ts>/lr_forecast_pentad.csv).
#                        A sibling <PATH>.manifest must exist (validated).
#   --horizon <HORIZON>  pentad|decade (required). Picks the column map
#                        and the API enum value.
#   --dry-run            Read + filter; do NOT POST anything.
#   --api-url <URL>      Target endpoint (default:
#                        http://localhost:8003/lr-forecast/).
#   --batch-size <N>     Records per POST batch (default: 500).
#   --image <IMAGE>      Docker image override.
#   --station-filter <CODE>
#                        Filter source CSV rows to a single station code
#                        (binding interface contract from P0 — honored by
#                        all migration wrappers).
#   -h, --help           Print this message and exit 0.
#
# Examples:
#   bash bin/initialize_lr_forecast_history.sh /data/taj_data/config/.env_tjhm \
#       --from-export /data/taj_data/logs/lr_forecast_tmp/20260606T120000Z/lr_forecast_pentad.csv \
#       --horizon pentad --dry-run
#   bash bin/initialize_lr_forecast_history.sh /data/taj_data/config/.env_tjhm \
#       --from-export /data/taj_data/logs/lr_forecast_tmp/20260606T120000Z/lr_forecast_decade.csv \
#       --horizon decade
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
FROM_EXPORT=""
HORIZON=""
DRY_RUN=false
API_URL="http://localhost:8003/lr-forecast/"
BATCH_SIZE=500
IMAGE_OVERRIDE=""
STATION_FILTER=""

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------

print_usage() {
    cat <<'USAGE'
Usage: bash bin/initialize_lr_forecast_history.sh <env_file_path> \
           --from-export <PATH> --horizon pentad|decade [OPTIONS]

Migrate lr_forecasts rows from a laptop-exported CSV into the postprocessing
API /lr-forecast/ endpoint. Honors MODE=full-import vs pre-cutoff based on
the target table state (queried via docker exec sapphire-postprocessing-db
psql).

Arguments:
  env_file_path        Path to the .env_<org> file (required).

Options:
  --from-export <PATH> Path to the laptop-exported CSV (required).
                       A sibling <PATH>.manifest must exist (validated).
  --horizon <HORIZON>  pentad|decade (required). Picks the column map and
                       the API enum value.
  --dry-run            Read + filter; do NOT POST anything.
  --api-url <URL>      Target endpoint (default: http://localhost:8003/lr-forecast/).
  --batch-size <N>     Records per POST batch (default: 500).
  --image <IMAGE>      Docker image override.
  --station-filter <CODE>
                       Filter source CSV rows to a single station code
                       (binding interface contract from P0 — honored by all
                       migration wrappers).
  -h, --help           Print this message and exit 0.

Examples:
  bash bin/initialize_lr_forecast_history.sh /data/taj_data/config/.env_tjhm \
      --from-export /data/taj_data/logs/lr_forecast_tmp/20260606T120000Z/lr_forecast_pentad.csv \
      --horizon pentad --dry-run
  bash bin/initialize_lr_forecast_history.sh /data/taj_data/config/.env_tjhm \
      --from-export /data/taj_data/logs/lr_forecast_tmp/20260606T120000Z/lr_forecast_decade.csv \
      --horizon decade
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
            --from-export)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --from-export requires a path value.${NC}" >&2
                    exit 1
                fi
                FROM_EXPORT="$2"
                shift 2
                ;;
            --horizon)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --horizon requires pentad|decade.${NC}" >&2
                    exit 1
                fi
                HORIZON="$2"
                shift 2
                ;;
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

    # Required-arg validation.
    if [[ -z "$FROM_EXPORT" ]]; then
        echo -e "${RED}Error: --from-export is required.${NC}" >&2
        echo "" >&2
        print_usage >&2
        exit 1
    fi
    if [[ -z "$HORIZON" ]]; then
        echo -e "${RED}Error: --horizon is required (pentad|decade).${NC}" >&2
        echo "" >&2
        print_usage >&2
        exit 1
    fi
    case "$HORIZON" in
        pentad|decade) ;;
        *)
            echo -e "${RED}Error: --horizon must be 'pentad' or 'decade' (got: ${HORIZON}).${NC}" >&2
            exit 1
            ;;
    esac
}

# ---------------------------------------------------------------------------
# Query the target postprocessing-db for MODE detection.
# Echoes "count<TAB>min_date_or_empty" on stdout (e.g. "0\t" or "100\t2024-01-15").
# Returns non-zero if the docker exec fails.
# Scoped to the requested horizon (pentad|decade) — lr_forecasts has NO
# model_type column, so the only scoping dimension is horizon_type.
# ---------------------------------------------------------------------------
query_target_state() {
    local horizon_arg="$1"
    docker exec sapphire-postprocessing-db psql \
        -U postgres -d postprocessing_db -P pager=off -t -A -F $'\t' \
        -c "SELECT COUNT(*), COALESCE(MIN(date)::text, '') FROM lr_forecasts WHERE horizon_type='${horizon_arg}';"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
    parse_args "$@"

    print_banner
    echo "| Running LR Forecast History Initialization (CSV -> API push)"

    # Validate env file exists before loading.
    if [[ ! -f "$ENV_FILE" ]]; then
        echo -e "${RED}Error: env file not found: ${ENV_FILE}${NC}" >&2
        exit 1
    fi

    # Load .env vars and derive ieasyhydroforecast_* path variables.
    read_configuration "${ENV_FILE}"

    # Validate required env vars.
    if [[ -z "${ieasyhydroforecast_data_root_dir:-}" ]]; then
        echo -e "${RED}Error: ieasyhydroforecast_data_root_dir is not set. Check your .env file.${NC}" >&2
        exit 1
    fi

    # Resolve image via the P0 helper (CLI override -> configured tag -> :latest).
    local IMAGE
    IMAGE="$(umh_resolve_image "$IMAGE_OVERRIDE" "${ieasyhydroforecast_backend_docker_image_tag:-}")"

    # Derived paths.
    local LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/lr_forecast_history_init"
    mkdir -p "${LOG_DIR}"

    local TIMESTAMP
    TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
    log_file="${LOG_DIR}/lr_forecast_history_init_${TIMESTAMP}.log"
    export log_file

    echo "| Log file: ${log_file}"
    echo ""

    umh_log_redacted "Starting LR forecast history initialization"
    umh_log_redacted "  env_file:        ${ENV_FILE}"
    umh_log_redacted "  from_export:     ${FROM_EXPORT}"
    umh_log_redacted "  horizon:         ${HORIZON}"
    umh_log_redacted "  api_url:         ${API_URL}"
    umh_log_redacted "  batch_size:      ${BATCH_SIZE}"
    umh_log_redacted "  station_filter:  ${STATION_FILTER:-<none>}"
    umh_log_redacted "  dry_run:         ${DRY_RUN}"
    umh_print_image_resolution_line "${IMAGE}"

    # -------------------------------------------------------------------------
    # Pre-flight checks
    # -------------------------------------------------------------------------

    # 1. Export CSV must exist.
    if [[ ! -f "$FROM_EXPORT" ]]; then
        umh_log_redacted "ERROR: export CSV not found: ${FROM_EXPORT}"
        exit 1
    fi
    umh_log_redacted "Pre-flight: export CSV OK (${FROM_EXPORT})"

    # 2. Manifest validation (Stage E item #2) — abort before any POST if invalid.
    umh_log_redacted "Pre-flight: validating manifest <${FROM_EXPORT}>.manifest"
    umh_validate_export_manifest "$FROM_EXPORT" "lr_forecast"
    umh_log_redacted "Pre-flight: manifest OK"

    # 3. Docker must be running.
    if ! docker info > /dev/null 2>&1; then
        umh_log_redacted "ERROR: Docker is not running. Please start Docker and try again."
        exit 1
    fi
    umh_log_redacted "Pre-flight: Docker daemon OK"

    # 4. Pull image if not present locally.
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
    # MODE detection — query target postprocessing-db.
    # -------------------------------------------------------------------------
    local MODE="full-import"
    local CUTOFF=""
    local TARGET_COUNT=0
    local TARGET_MIN_DATE=""

    if docker ps --filter "name=sapphire-postprocessing-db" --quiet | grep -q .; then
        local query_out
        if query_out="$(query_target_state "$HORIZON" 2>&1)"; then
            # query_out format: "<count>\t<min_date_or_empty>"
            TARGET_COUNT="${query_out%%$'\t'*}"
            TARGET_MIN_DATE="${query_out#*$'\t'}"
            # Trim any trailing newline / whitespace.
            TARGET_COUNT="${TARGET_COUNT//[$'\r\n ']/}"
            TARGET_MIN_DATE="${TARGET_MIN_DATE//[$'\r\n ']/}"
            umh_log_redacted "Target state: horizon=${HORIZON} count=${TARGET_COUNT} min_date=${TARGET_MIN_DATE:-<null>}"
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
        umh_log_redacted "WARN: sapphire-postprocessing-db not running; assuming full-import"
        MODE="full-import"
        CUTOFF=""
    fi

    umh_log_redacted "MODE=${MODE}$( [[ -n "$CUTOFF" ]] && echo " (cutoff=${CUTOFF})" || echo " (target empty)")"

    # -------------------------------------------------------------------------
    # Acquire a temp workspace (mode 0o700, trap-cleaned on EXIT INT TERM).
    # The temp workspace is for any auxiliary artifacts; the export CSV is
    # read directly from its existing path (already inside data-root/logs/).
    # -------------------------------------------------------------------------
    local TMPDIR_LR
    TMPDIR_LR="$(umh_acquire_temp_workspace lr_forecast)"
    umh_log_redacted "Temp workspace: ${TMPDIR_LR}"

    # -------------------------------------------------------------------------
    # Run the Python helper inside the prepgateway image.
    # Mount migration_py/ read-only; mount the export CSV read-only.
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
    umh_log_redacted "Running LR forecast push via docker"
    umh_log_redacted "  image:        ${IMAGE}"
    umh_log_redacted "  csv:          ${FROM_EXPORT} -> /lr_forecast.csv (ro)"
    umh_log_redacted "  migration_py: ${HELPER_DIR}/migration_py -> /opt/migration_py (ro)"
    umh_log_redacted "  api_url:      ${API_URL}"
    umh_log_redacted "  horizon:      ${HORIZON}"
    umh_log_redacted "  batch_size:   ${BATCH_SIZE}"
    if [[ "$DRY_RUN" == true ]]; then
        umh_log_redacted "  mode:         DRY RUN (no POSTs)"
    else
        umh_log_redacted "  mode:         REAL run"
    fi
    umh_log_redacted "========================================"

    # shellcheck disable=SC2086  # DRY_RUN_FLAG intentionally unquoted: empty=no arg
    docker run --rm --network host \
        -v "${FROM_EXPORT}:/lr_forecast.csv:ro" \
        -v "${HELPER_DIR}/migration_py:/opt/migration_py:ro" \
        -e "PYTHONPATH=/opt" \
        "$IMAGE" \
        python3 -m migration_py.lr_forecast \
            --csv-path /lr_forecast.csv \
            --api-url "${API_URL}" \
            --horizon "${HORIZON}" \
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
    echo -e " LR FORECAST HISTORY INIT COMPLETE"
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

    echo "Verify rows in the postprocessing DB:"
    echo "  docker exec sapphire-postprocessing-db \\"
    echo "    psql -U postgres -d postprocessing_db -c \\"
    echo "    \"SELECT horizon_type, COUNT(*) AS rows, MIN(date), MAX(date) FROM lr_forecasts WHERE horizon_type='${HORIZON}' GROUP BY horizon_type;\""
    echo ""
    echo "Log file: ${log_file}"

    exit "$DOCKER_EXIT"
}

main "$@"
