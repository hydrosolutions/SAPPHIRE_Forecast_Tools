#!/usr/bin/env bash
# ============================================================================
# initialize_hydrograph_period_history.sh
#
# Hydrograph PENTAD/DECADE laptop-export CSV-to-API migration wrapper (P2b of
# the update-time migration toolkit). Sibling to P2a (runoff PENTAD/DECADE);
# same shape (laptop-export → CSV+manifest → server-side import), different
# target table (hydrographs) and payload (wide-stat row with quantiles +
# year-mapped previous/current).
#
# Reads a CSV+manifest pair produced by
# ``bin/export_hydrograph_period_history.sh`` on the operator's laptop, then
# transferred to the deployment server via scp. The manifest sidecar
# (<csv>.manifest, 5 required keys) is validated BEFORE any POST.
#
# Uses the P0 helper (bin/utils/update_migration_helpers.sh) for image
# resolution, temp workspace, redacted logging, manifest validation, and
# the uniform image log line. Mounts the P0 migration_py/ package into the
# prepgateway container so the per-row POST logic lives in importable
# stdlib-only Python — no embedded heredoc beyond a minimal entry shim.
#
# Year-column handling (per brief §4.2):
#   The laptop-side export emits ``previous`` / ``current`` as literal column
#   names (the DB row model uses those field names directly). NO year-column
#   discovery is performed here — that pattern is specific to the CSV-source
#   P3 hydrograph DAY wrapper which reads ``intermediate_data/hydrograph_day.csv``
#   (a wide CSV where columns rotate annually).
#
# Universal safe-write rule (architecture §Q2 layer 2):
#   Hydrograph rows have many nullable stat / quantile / year-mapped fields.
#   By default the wrapper sends ONLY non-NULL source fields. The service-
#   side _has_changes + setattr path overwrites existing non-NULL with
#   incoming NULL, so the wrapper never injects null.
#
# --strict-merge opt-in (future flag):
#   When passed, the Python helper logs a warning and falls back to
#   enrichment-only — read-before-merge is documented in the gi_draft +
#   runbook §6.2 as a deferred follow-up. See those for the design decision.
#
# MODE detection: queries the target preprocessing-db once via docker exec
# psql. Empty (count=0 OR min_date IS NULL) -> full-import; populated ->
# pre-cutoff (cutoff = MIN(date)).
#
# Idempotency: the preprocessing service upserts on
# (horizon_type, code, date). Reruns are safe.
#
# Forward interface contract (P0 gi_draft): --station-filter <code> is the
# binding flag name reused by P1b / P1c / P3 / P2a / P2b.
#
# Usage:
#   bash bin/initialize_hydrograph_period_history.sh <env_file_path> \
#       --from-export <csv_path> \
#       --horizon pentad|decade \
#       [OPTIONS]
#
# Arguments:
#   env_file_path        Path to the .env_<org> file (required, positional).
#
# Required flags:
#   --from-export <PATH> Path to the transferred export CSV. The sibling
#                        ``<PATH>.manifest`` file MUST exist.
#   --horizon pentad|decade
#                        Which horizon the export holds.
#
# Options:
#   --dry-run            Read + filter; do NOT POST anything.
#   --api-url <URL>      Target endpoint (default: http://localhost:8002/hydrograph/).
#   --batch-size <N>     Records per POST batch (default: 500).
#   --image <IMAGE>      Docker image override.
#   --station-filter <CODE>
#                        Filter source CSV rows to a single station code
#                        (binding interface contract from P0 — honored by
#                        all CSV-source AND DB-source wrappers).
#   --strict-merge       Opt-in read-before-merge (NOT YET IMPLEMENTED;
#                        falls back to enrichment-only with a warning).
#   -h, --help           Print this message and exit 0.
#
# Examples:
#   bash bin/initialize_hydrograph_period_history.sh /data/taj_data/config/.env_tjhm \
#       --from-export /data/taj_data/logs/hydrograph_period_pentad_20260606T120000Z.csv \
#       --horizon pentad --dry-run
#
#   bash bin/initialize_hydrograph_period_history.sh /data/taj_data/config/.env_tjhm \
#       --from-export /data/taj_data/logs/hydrograph_period_decade_20260606T120100Z.csv \
#       --horizon decade --station-filter 19999
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
API_URL="http://localhost:8002/hydrograph/"
BATCH_SIZE=500
IMAGE_OVERRIDE=""
STATION_FILTER=""
STRICT_MERGE=false

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------

print_usage() {
    cat <<'USAGE'
Usage: bash bin/initialize_hydrograph_period_history.sh <env_file_path> \
            --from-export <csv_path> \
            --horizon pentad|decade \
            [OPTIONS]

Server-side import of a hydrograph PENTAD or DECADE laptop-export CSV + manifest
pair into the deployment's preprocessing API. Honors MODE=full-import vs
pre-cutoff based on the target table state. Validates the sidecar
``<csv>.manifest`` (P0 contract — 5 required keys: export_type, row_count,
station_count, date_min, date_max) BEFORE any POST.

Arguments:
  env_file_path                Path to the .env_<org> file (required).

Required flags:
  --from-export <PATH>         Path to the transferred export CSV. The
                               sibling <PATH>.manifest file MUST exist.
  --horizon pentad|decade      Which horizon the export holds.

Options:
  --dry-run                    Read + filter; do NOT POST anything.
  --api-url <URL>              Target endpoint (default:
                               http://localhost:8002/hydrograph/).
  --batch-size <N>             Records per POST batch (default: 500).
  --image <IMAGE>              Docker image override.
  --station-filter <CODE>      Filter source CSV rows to a single station
                               code (binding interface contract from P0 —
                               honored by all CSV-source AND DB-source
                               wrappers).
  --strict-merge               Opt-in read-before-merge (NOT YET IMPLEMENTED;
                               falls back to enrichment-only with a warning).
  -h, --help                   Print this message and exit 0.

Examples:
  bash bin/initialize_hydrograph_period_history.sh /data/taj_data/config/.env_tjhm \
      --from-export /data/taj_data/logs/hydrograph_period_pentad_20260606T120000Z.csv \
      --horizon pentad --dry-run

  bash bin/initialize_hydrograph_period_history.sh /data/taj_data/config/.env_tjhm \
      --from-export /data/taj_data/logs/hydrograph_period_decade_20260606T120100Z.csv \
      --horizon decade --station-filter 19999
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
                    echo -e "${RED}Error: --from-export requires a CSV path.${NC}" >&2
                    exit 1
                fi
                FROM_EXPORT="$2"
                shift 2
                ;;
            --horizon)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --horizon requires 'pentad' or 'decade'.${NC}" >&2
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
            --strict-merge)
                STRICT_MERGE=true
                shift
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

    # Validate required.
    if [[ -z "$FROM_EXPORT" ]]; then
        echo -e "${RED}Error: --from-export <csv_path> is required.${NC}" >&2
        exit 1
    fi
    if [[ -z "$HORIZON" ]]; then
        echo -e "${RED}Error: --horizon pentad|decade is required.${NC}" >&2
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
# Query the target preprocessing-db for MODE detection (per-horizon).
# Echoes "count<TAB>min_date_or_empty" on stdout.
# Returns non-zero if the docker exec fails.
# ---------------------------------------------------------------------------
query_target_state() {
    local horizon_upper="$1"
    docker exec sapphire-preprocessing-db psql \
        -U postgres -d preprocessing_db -P pager=off -t -A -F $'\t' \
        -c "SELECT COUNT(*), COALESCE(MIN(date)::text, '') FROM hydrographs WHERE horizon_type='${horizon_upper}';"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
    parse_args "$@"

    print_banner
    echo "| Running Hydrograph PENTAD/DECADE History Initialization (laptop-export -> API push)"

    # Validate env file exists before loading.
    if [[ ! -f "$ENV_FILE" ]]; then
        echo -e "${RED}Error: env file not found: ${ENV_FILE}${NC}" >&2
        exit 1
    fi

    # Validate the export CSV + manifest exist on disk BEFORE invoking docker.
    if [[ ! -f "$FROM_EXPORT" ]]; then
        echo -e "${RED}Error: --from-export CSV not found: ${FROM_EXPORT}${NC}" >&2
        exit 1
    fi
    local MANIFEST_HOST="${FROM_EXPORT}.manifest"
    if [[ ! -f "$MANIFEST_HOST" ]]; then
        echo -e "${RED}Error: sidecar manifest not found: ${MANIFEST_HOST}${NC}" >&2
        echo "       The laptop-side export script must emit BOTH files; transfer them together." >&2
        exit 1
    fi

    # Manifest validation FIRST (Stage E #2 + brief §4.6). Fail loudly
    # before doing any image work or psql probes.
    umh_validate_export_manifest "$FROM_EXPORT" hydrograph_period

    # Load .env vars and derive ieasyhydroforecast_* path variables.
    read_configuration "${ENV_FILE}"

    # Validate required env vars.
    if [[ -z "${ieasyhydroforecast_data_root_dir:-}" ]]; then
        echo -e "${RED}Error: Required environment variable ieasyhydroforecast_data_root_dir is not set.${NC}" >&2
        exit 1
    fi

    # Resolve image via the P0 helper (CLI override -> configured tag -> :latest).
    local IMAGE
    IMAGE="$(umh_resolve_image "$IMAGE_OVERRIDE" "${ieasyhydroforecast_backend_docker_image_tag:-}")"

    local horizon_upper
    horizon_upper="$(echo "$HORIZON" | tr '[:lower:]' '[:upper:]')"

    local LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/hydrograph_period_history_init"
    mkdir -p "${LOG_DIR}"

    local TIMESTAMP
    TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
    log_file="${LOG_DIR}/hydrograph_period_history_init_${HORIZON}_${TIMESTAMP}.log"
    export log_file

    echo "| Log file: ${log_file}"
    echo ""

    umh_log_redacted "Starting hydrograph PENTAD/DECADE history initialization"
    umh_log_redacted "  env_file:        ${ENV_FILE}"
    umh_log_redacted "  from_export:     ${FROM_EXPORT}"
    umh_log_redacted "  manifest:        ${MANIFEST_HOST} (validated OK)"
    umh_log_redacted "  horizon:         ${HORIZON} (${horizon_upper})"
    umh_log_redacted "  api_url:         ${API_URL}"
    umh_log_redacted "  batch_size:      ${BATCH_SIZE}"
    umh_log_redacted "  station_filter:  ${STATION_FILTER:-<none>}"
    umh_log_redacted "  strict_merge:    ${STRICT_MERGE}"
    umh_log_redacted "  dry_run:         ${DRY_RUN}"
    umh_print_image_resolution_line "${IMAGE}"

    # -------------------------------------------------------------------------
    # Pre-flight checks
    # -------------------------------------------------------------------------

    # 1. Docker must be running.
    if ! docker info > /dev/null 2>&1; then
        umh_log_redacted "ERROR: Docker is not running. Please start Docker and try again."
        exit 1
    fi
    umh_log_redacted "Pre-flight: Docker daemon OK"

    # 2. Pull image if not present locally.
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
        if query_out="$(query_target_state "$horizon_upper" 2>&1)"; then
            # query_out format: "<count>\t<min_date_or_empty>"
            TARGET_COUNT="${query_out%%$'\t'*}"
            TARGET_MIN_DATE="${query_out#*$'\t'}"
            # Trim any trailing newline / whitespace.
            TARGET_COUNT="${TARGET_COUNT//[$'\r\n ']/}"
            TARGET_MIN_DATE="${TARGET_MIN_DATE//[$'\r\n ']/}"
            umh_log_redacted "Target state: count=${TARGET_COUNT} min_date=${TARGET_MIN_DATE:-<null>} (horizon_type=${horizon_upper})"
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
    local TMPDIR_HYDRO
    TMPDIR_HYDRO="$(umh_acquire_temp_workspace hydrograph_period)"
    umh_log_redacted "Temp workspace: ${TMPDIR_HYDRO}"

    # -------------------------------------------------------------------------
    # Run the Python helper inside the prepgateway image.
    # Mount migration_py/ read-only; mount the CSV + manifest read-only.
    # -------------------------------------------------------------------------
    local HELPER_DIR
    HELPER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/utils" && pwd)"

    local DRY_RUN_FLAG=""
    if [[ "$DRY_RUN" == true ]]; then
        DRY_RUN_FLAG="--dry-run"
    fi

    local STRICT_MERGE_FLAG=""
    if [[ "$STRICT_MERGE" == true ]]; then
        STRICT_MERGE_FLAG="--strict-merge"
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
    umh_log_redacted "Running hydrograph PENTAD/DECADE push via docker"
    umh_log_redacted "  image:        ${IMAGE}"
    umh_log_redacted "  csv:          ${FROM_EXPORT} -> /hydrograph_period.csv (ro)"
    umh_log_redacted "  manifest:     ${MANIFEST_HOST} -> /hydrograph_period.csv.manifest (ro)"
    umh_log_redacted "  migration_py: ${HELPER_DIR}/migration_py -> /opt/migration_py (ro)"
    umh_log_redacted "  horizon:      ${HORIZON}"
    umh_log_redacted "  api_url:      ${API_URL}"
    umh_log_redacted "  batch_size:   ${BATCH_SIZE}"
    if [[ "$DRY_RUN" == true ]]; then
        umh_log_redacted "  mode:         DRY RUN (no POSTs)"
    else
        umh_log_redacted "  mode:         REAL run"
    fi
    umh_log_redacted "========================================"

    # shellcheck disable=SC2086  # DRY_RUN_FLAG / STRICT_MERGE_FLAG intentionally unquoted: empty=no arg
    docker run --rm --network host \
        -v "${FROM_EXPORT}:/hydrograph_period.csv:ro" \
        -v "${MANIFEST_HOST}:/hydrograph_period.csv.manifest:ro" \
        -v "${HELPER_DIR}/migration_py:/opt/migration_py:ro" \
        -e "PYTHONPATH=/opt" \
        "$IMAGE" \
        python3 -m migration_py.hydrograph_period \
            --csv-path /hydrograph_period.csv \
            --manifest-path /hydrograph_period.csv.manifest \
            --horizon "${HORIZON}" \
            --api-url "${API_URL}" \
            --batch-size "${BATCH_SIZE}" \
            "${CUTOFF_ARGS[@]}" \
            "${STATION_ARGS[@]}" \
            ${STRICT_MERGE_FLAG} \
            ${DRY_RUN_FLAG} \
        2>&1 | tee -a "${log_file}"

    local DOCKER_EXIT=${PIPESTATUS[0]}

    # -------------------------------------------------------------------------
    # Post-run summary.
    # -------------------------------------------------------------------------
    echo ""
    echo -e "${BOLD}========================================"
    echo -e " HYDROGRAPH PENTAD/DECADE HISTORY INIT COMPLETE"
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
    echo "    \"SELECT horizon_type, COUNT(*) AS rows, MIN(date), MAX(date) FROM hydrographs WHERE horizon_type='${horizon_upper}' GROUP BY horizon_type;\""
    echo ""
    echo "Log file: ${log_file}"

    exit "$DOCKER_EXIT"
}

main "$@"
