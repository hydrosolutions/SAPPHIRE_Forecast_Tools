#!/usr/bin/env bash
# ============================================================================
# initialize_ml_forecast_history.sh
#
# ML forecast laptop-export-to-API migration wrapper (P4b of the update-time
# migration toolkit). Reads a CSV that was produced by
# ``bin/export_ml_forecast_history.sh`` on the laptop's
# ``sapphire-postprocessing-db`` (table ``forecasts``, filtered to
# ``model_type::text IN ('TFT','TIDE','TSMIXER')`` — PG enum LABELS, see
# Finding 11 below) and POSTs the rows to the deployment postprocessing
# API ``/forecast/`` endpoint as mixed-case ``TFT`` / ``TiDE`` / ``TSMixer``
# wire values.
#
# Uses the P0 helper (bin/utils/update_migration_helpers.sh) for image
# resolution, temp workspace, redacted logging, manifest validation, and the
# uniform image log line. Mounts the migration_py/ package into the
# prepgateway container so the per-row POST logic lives in importable
# stdlib-only Python — no embedded heredoc beyond a minimal entry shim.
#
# Two architectural quirks (see also bin/utils/migration_py/ml_forecast.py
# docstring):
#
# 1. Enum case (Stage A §E; Finding 11 — Tajik live test):
#    Two ``model_type`` representations are in play, and BOTH must stay
#    correct in this wrapper:
#      - PG enum LABELS (this script's raw SQL only): UPPERCASE
#        ``TFT`` / ``TIDE`` / ``TSMIXER``, compared via ``::text`` to
#        sidestep the enum-literal coercion error
#        (``invalid input value for enum modeltype: "TiDE"``) that bit
#        the live Tajik DB on real-data walkthrough.
#      - API wire values (JSON payload POSTed by the Python helper):
#        MIXED-CASE ``TFT`` / ``TiDE`` / ``TSMixer`` per
#        ``sapphire/services/postprocessing/app/models.py:23-24`` (see
#        the inline ``# TSMIXER(how it is stored in PostgreSQL),
#        TSMixer(how it is passed in API)`` comment).
#    ``MODEL_DIR_TO_API`` in the Python helper maps both spellings to the
#    mixed-case API form.
#
# 2. Default horizon = ``day`` (user-lock L6): modern ML CSV-derived writes
#    store as ``horizon_type='day'`` regardless of pentad / decade workflow.
#    To migrate the pre-1cb3495 legacy PENTAD/DECADE rows, pass
#    ``--preserve-legacy-ml-horizons`` — the wrapper emits a prominent
#    WARNING when this flag is active.
#
# Universal safe-write rule (architecture Q2 layer 2):
#   ML forecast rows have nullable quantile / discharge fields. The wrapper
#   sends ONLY non-NULL fields from the source CSV; the service-side upsert
#   never sees an incoming NULL for an absent field.
#
# MODE detection: queries the postprocessing-db once via docker exec psql.
# Empty (count=0 OR min_date IS NULL) -> full-import; populated ->
# pre-cutoff (cutoff = MIN(date) for ML-only rows).
#
# Idempotency: the postprocessing service upserts on
# ``(horizon_type, code, model_type, date, target)``. Reruns are safe.
#
# Forward interface contract (P0 gi_draft): --station-filter <code> is the
# binding flag name reused by every CSV-source wrapper.
#
# Usage:
#   bash bin/initialize_ml_forecast_history.sh <env_file_path> \
#       --from-export <path> [OPTIONS]
#
# Arguments:
#   env_file_path        Path to the .env_<org> file (required).
#
# Options:
#   --from-export <PATH> Path to the transferred export CSV (REQUIRED).
#                        A matching <PATH>.manifest sidecar must exist.
#   --model TFT|TiDE|TSMixer
#                        Restrict to a single ML model variant.
#   --dry-run            Read + filter; do NOT POST anything.
#   --api-url <URL>      Target endpoint (default: http://localhost:8003/forecast/).
#   --batch-size <N>     Records per POST batch (default: 500).
#   --image <IMAGE>      Docker image override.
#   --station-filter <CODE>
#                        Filter source CSV rows to a single station code
#                        (binding interface contract from P0).
#   --preserve-legacy-ml-horizons
#                        Opt-in: preserve PENTAD/DECADE horizon_type instead
#                        of the default user-lock L6 'day' storage. Emits a
#                        WARNING log line. Use only to migrate pre-1cb3495
#                        legacy rows.
#   -h, --help           Print this message and exit 0.
#
# Examples:
#   bash bin/initialize_ml_forecast_history.sh /data/taj_data/config/.env_tjhm \
#       --from-export /tmp/ml_forecast_20260606T120000Z.csv --dry-run
#
#   bash bin/initialize_ml_forecast_history.sh /data/taj_data/config/.env_tjhm \
#       --from-export /tmp/ml_forecast_20260606T120000Z.csv --station-filter 19999
#
#   # Legacy mode (PENTAD/DECADE rows only — explicit opt-in):
#   bash bin/initialize_ml_forecast_history.sh /data/taj_data/config/.env_tjhm \
#       --from-export /tmp/ml_forecast_legacy.csv --preserve-legacy-ml-horizons
# ============================================================================

set -eo pipefail
# NOTE: set -u is intentionally omitted; common_functions.sh is not strict-mode safe.

# shellcheck disable=SC1091
source "$(dirname "$0")/utils/update_migration_helpers.sh"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

# Defaults
ENV_FILE=""
FROM_EXPORT=""
MODEL_FILTER=""
DRY_RUN=false
API_URL="http://localhost:8003/forecast/"
BATCH_SIZE=500
IMAGE_OVERRIDE=""
STATION_FILTER=""
PRESERVE_LEGACY_HORIZONS=false

print_usage() {
    cat <<'USAGE'
Usage: bash bin/initialize_ml_forecast_history.sh <env_file_path> \
    --from-export <PATH> [OPTIONS]

Migrate ML forecast rows (TFT/TiDE/TSMixer) from a laptop-export CSV into
the postprocessing API. Honors MODE=full-import vs pre-cutoff based on the
target forecasts table state (filtered to the three ML model_types).

Arguments:
  env_file_path        Path to the .env_<org> file (required).

Options:
  --from-export <PATH> Path to the transferred export CSV (REQUIRED).
                       A matching <PATH>.manifest sidecar must exist
                       (export_type=ml_forecast, with row_count,
                       station_count, date_min, date_max keys).
  --model TFT|TiDE|TSMixer
                       Restrict to a single ML model variant.
  --dry-run            Read + filter; do NOT POST anything.
  --api-url <URL>      Target endpoint (default: http://localhost:8003/forecast/).
  --batch-size <N>     Records per POST batch (default: 500).
  --image <IMAGE>      Docker image override.
  --station-filter <CODE>
                       Filter source CSV rows to a single station code
                       (binding interface contract — forward-compatible
                       with all CSV-source wrappers).
  --preserve-legacy-ml-horizons
                       WARNING — opt-in. Preserve source PENTAD/DECADE
                       horizon_type values rather than the default
                       user-lock L6 'day' storage. Use ONLY to migrate the
                       pre-1cb3495 legacy rows that the modern operational
                       pipeline no longer produces. The wrapper emits a
                       prominent WARNING log line when this is active —
                       operator MUST read the log line before proceeding.
  -h, --help           Print this message and exit 0.

Default horizon storage = 'day' per user-lock L6 (matches the operational
writer at apps/machine_learning/scr/utils_ml_forecast.py).

Enum case mapping:
  on-disk dir TIDE      -> API TiDE
  on-disk dir TSMIXER   -> API TSMixer
  on-disk dir TFT       -> API TFT
The export CSV preserves the canonical API form (mixed case).

Examples:
  bash bin/initialize_ml_forecast_history.sh /data/taj_data/config/.env_tjhm \
      --from-export /tmp/ml_forecast.csv --dry-run

  bash bin/initialize_ml_forecast_history.sh /data/taj_data/config/.env_tjhm \
      --from-export /tmp/ml_forecast.csv --station-filter 19999

  bash bin/initialize_ml_forecast_history.sh /data/taj_data/config/.env_tjhm \
      --from-export /tmp/ml_legacy.csv --preserve-legacy-ml-horizons
USAGE
}

parse_args() {
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
                    echo -e "${RED}Error: --from-export requires a path.${NC}" >&2
                    exit 1
                fi
                FROM_EXPORT="$2"
                shift 2
                ;;
            --model)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --model requires a value.${NC}" >&2
                    exit 1
                fi
                MODEL_FILTER="$2"
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
            --preserve-legacy-ml-horizons)
                PRESERVE_LEGACY_HORIZONS=true
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
}

# Query the postprocessing-db for MODE detection (ML rows only).
# Echoes "count<TAB>min_date_or_empty" on stdout.
#
# Finding 11 (Tajik live test): ``model_type`` is a PostgreSQL enum whose
# LABELS are UPPERCASE — ``TFT`` / ``TIDE`` / ``TSMIXER`` — per the comment
# at ``sapphire/services/postprocessing/app/models.py:23-24``. We compare
# via ``::text`` to sidestep the enum-literal coercion error
# (``invalid input value for enum modeltype: "TiDE"``) that bit the live
# Tajik DB before this fix. The Python helper still POSTs the API
# mixed-case wire values; the two representations are intentional.
query_target_state() {
    docker exec sapphire-postprocessing-db psql \
        -U postgres -d postprocessing_db -P pager=off -t -A -F $'\t' \
        -c "SELECT COUNT(*), COALESCE(MIN(date)::text, '') FROM forecasts WHERE model_type::text IN ('TFT','TIDE','TSMIXER');"
}

main() {
    parse_args "$@"

    print_banner
    echo "| Running ML Forecast History Initialization (laptop-export -> CSV -> API)"

    # Validate env file exists before loading.
    if [[ ! -f "$ENV_FILE" ]]; then
        echo -e "${RED}Error: env file not found: ${ENV_FILE}${NC}" >&2
        exit 1
    fi

    if [[ -z "$FROM_EXPORT" ]]; then
        echo -e "${RED}Error: --from-export <PATH> is required (laptop-export pattern).${NC}" >&2
        exit 1
    fi

    # Load .env vars and derive ieasyhydroforecast_* path variables.
    read_configuration "${ENV_FILE}"

    if [[ -z "${ieasyhydroforecast_data_root_dir:-}" ]]; then
        echo -e "${RED}Error: ieasyhydroforecast_data_root_dir is not set. Check your .env file.${NC}" >&2
        exit 1
    fi

    # Resolve image via the P0 helper.
    local IMAGE
    IMAGE="$(umh_resolve_image "$IMAGE_OVERRIDE" "${ieasyhydroforecast_backend_docker_image_tag:-}")"

    local LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/ml_forecast_history_init"
    mkdir -p "${LOG_DIR}"

    local TIMESTAMP
    TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
    log_file="${LOG_DIR}/ml_forecast_history_init_${TIMESTAMP}.log"
    export log_file

    echo "| Log file: ${log_file}"
    echo ""

    umh_log_redacted "Starting ML forecast history initialization"
    umh_log_redacted "  env_file:                       ${ENV_FILE}"
    umh_log_redacted "  from_export:                    ${FROM_EXPORT}"
    umh_log_redacted "  api_url:                        ${API_URL}"
    umh_log_redacted "  batch_size:                     ${BATCH_SIZE}"
    umh_log_redacted "  station_filter:                 ${STATION_FILTER:-<none>}"
    umh_log_redacted "  model_filter:                   ${MODEL_FILTER:-<none>}"
    umh_log_redacted "  preserve_legacy_ml_horizons:    ${PRESERVE_LEGACY_HORIZONS}"
    umh_log_redacted "  dry_run:                        ${DRY_RUN}"
    umh_print_image_resolution_line "${IMAGE}"

    if [[ "$PRESERVE_LEGACY_HORIZONS" == true ]]; then
        umh_log_redacted "WARNING: --preserve-legacy-ml-horizons active. Source PENTAD/DECADE"
        umh_log_redacted "         horizon_type values will be preserved. Modern ML writes use"
        umh_log_redacted "         horizon_type='day' per user-lock L6 (commit 1cb3495). Use this"
        umh_log_redacted "         flag ONLY to migrate the pre-1cb3495 legacy rows."
    fi

    # -------------------------------------------------------------------------
    # Pre-flight checks
    # -------------------------------------------------------------------------

    if [[ ! -f "$FROM_EXPORT" ]]; then
        umh_log_redacted "ERROR: export CSV not found: ${FROM_EXPORT}"
        exit 1
    fi
    umh_log_redacted "Pre-flight: export CSV OK (${FROM_EXPORT})"

    # Manifest validation (P0 contract).
    umh_log_redacted "Validating export manifest..."
    if ! umh_validate_export_manifest "$FROM_EXPORT" "ml_forecast"; then
        umh_log_redacted "ERROR: manifest validation failed (see above)."
        exit 1
    fi
    umh_log_redacted "Pre-flight: manifest OK"

    if ! docker info > /dev/null 2>&1; then
        umh_log_redacted "ERROR: Docker is not running. Please start Docker and try again."
        exit 1
    fi
    umh_log_redacted "Pre-flight: Docker daemon OK"

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
    # MODE detection.
    # -------------------------------------------------------------------------
    local MODE="full-import"
    local CUTOFF=""
    local TARGET_COUNT=0
    local TARGET_MIN_DATE=""

    if docker ps --filter "name=sapphire-postprocessing-db" --quiet | grep -q .; then
        local query_out
        if query_out="$(query_target_state 2>&1)"; then
            TARGET_COUNT="${query_out%%$'\t'*}"
            TARGET_MIN_DATE="${query_out#*$'\t'}"
            TARGET_COUNT="${TARGET_COUNT//[$'\r\n ']/}"
            TARGET_MIN_DATE="${TARGET_MIN_DATE//[$'\r\n ']/}"
            umh_log_redacted "Target state: ml_count=${TARGET_COUNT} ml_min_date=${TARGET_MIN_DATE:-<null>}"
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
    # Temp workspace.
    # -------------------------------------------------------------------------
    local TMPDIR_ML
    TMPDIR_ML="$(umh_acquire_temp_workspace ml_forecast)"
    umh_log_redacted "Temp workspace: ${TMPDIR_ML}"

    # -------------------------------------------------------------------------
    # Run the Python helper inside the prepgateway image.
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

    local MODEL_ARGS=()
    if [[ -n "$MODEL_FILTER" ]]; then
        MODEL_ARGS=("--model" "$MODEL_FILTER")
    fi

    local LEGACY_ARGS=()
    if [[ "$PRESERVE_LEGACY_HORIZONS" == true ]]; then
        LEGACY_ARGS=("--preserve-legacy-ml-horizons")
    fi

    umh_log_redacted "========================================"
    umh_log_redacted "Running ML forecast push via docker"
    umh_log_redacted "  image:        ${IMAGE}"
    umh_log_redacted "  csv:          ${FROM_EXPORT} -> /ml_forecast.csv (ro)"
    umh_log_redacted "  migration_py: ${HELPER_DIR}/migration_py -> /opt/migration_py (ro)"
    umh_log_redacted "  api_url:      ${API_URL}"
    umh_log_redacted "  batch_size:   ${BATCH_SIZE}"
    if [[ "$DRY_RUN" == true ]]; then
        umh_log_redacted "  mode:         DRY RUN (no POSTs)"
    else
        umh_log_redacted "  mode:         REAL run"
    fi
    umh_log_redacted "========================================"

    # shellcheck disable=SC2086
    docker run --rm --network host \
        -v "${FROM_EXPORT}:/ml_forecast.csv:ro" \
        -v "${HELPER_DIR}/migration_py:/opt/migration_py:ro" \
        -e "PYTHONPATH=/opt" \
        "$IMAGE" \
        python3 -m migration_py.ml_forecast \
            --csv-path /ml_forecast.csv \
            --api-url "${API_URL}" \
            --batch-size "${BATCH_SIZE}" \
            "${CUTOFF_ARGS[@]}" \
            "${STATION_ARGS[@]}" \
            "${MODEL_ARGS[@]}" \
            "${LEGACY_ARGS[@]}" \
            ${DRY_RUN_FLAG} \
        2>&1 | tee -a "${log_file}"

    local DOCKER_EXIT=${PIPESTATUS[0]}

    # -------------------------------------------------------------------------
    # Post-run summary.
    # -------------------------------------------------------------------------
    echo ""
    echo -e "${BOLD}========================================"
    echo -e " ML FORECAST HISTORY INIT COMPLETE"
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
    echo "    \"SELECT horizon_type, model_type, COUNT(*) AS rows, MIN(date), MAX(date)"
    echo "       FROM forecasts WHERE model_type::text IN ('TFT','TIDE','TSMIXER')"
    echo "       GROUP BY horizon_type, model_type ORDER BY model_type, horizon_type;\""
    echo ""
    echo "Log file: ${log_file}"

    exit "$DOCKER_EXIT"
}

main "$@"
