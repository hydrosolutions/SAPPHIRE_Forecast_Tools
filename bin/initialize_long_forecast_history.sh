#!/usr/bin/env bash
# ============================================================================
# initialize_long_forecast_history.sh
#
# Long-term forecasts CSV-to-API migration wrapper (P5 of the update-time
# migration toolkit). Walks <data_ref>/config/long_term_configs/*.json,
# parses each mode's models_to_use, and POSTs every model's hindcast CSV
# (<data_ref>/intermediate_data/long_term_predictions/<mode>/<model>/
# <model>_hindcast.csv) to the postprocessing API.
#
# CONTRAST vs the P1a/P1b/P3 CSV-source siblings:
#   Those wrappers each consume ONE CSV file. P5 walks a directory tree
#   (one mode -> N models -> N CSV files) and is by far the most complex
#   CSV-ish wrapper of the toolkit. The mode-and-model discovery + hard-skip
#   logic lives in migration_py.long_forecast._discover_modes /
#   _load_mode_config / _discover_hindcast_csvs.
#
# Universal safe-write rule (architecture Q2 layer 2):
#   long_forecasts rows have many sparse model-specific quantile and
#   ensemble fields. Different model families populate different subsets
#   (LR: q + q05..q95; GBT: + q_xgb/q_lgbm/q_catboost; MC_ALD: + q_loc).
#   The wrapper sends ONLY non-NULL fields from each source row. Fields
#   absent from a model's CSV are simply omitted, never sent as null.
#
# Mode skip semantics:
#   - `monthly` is ALWAYS skipped (non-operational; see
#     `apps/long_term_forecasting/lt_schedule_query.py:54-91`).
#   - Modes without a JSON config are HARD-SKIPPED at the discovery layer
#     (architecture §Q3 lock — no synthetic configs from directory layout).
#   - `--skip-mode <name>` lets the operator additionally skip a configured
#     mode (comma-separated for multiple skips).
#   - `--mode <name>` restricts the run to a single configured mode.
#   - `--model <name>` restricts the run to a single model across all
#     selected modes.
#
# UZB no-op acceptance (Stage E item #12):
#   If zero modes match the discover-and-filter pipeline (e.g. UZB demo
#   profile with no configured long-term modes), the wrapper exits 0 with a
#   logged "no source data for this deployment" message — NOT an error.
#
# MODE detection: queries the target postprocessing-db via docker exec psql
# (long_forecasts lives in the postprocessing DB, not preprocessing). The
# primary path builds a grouped cutoff map keyed by
# (horizon_type, horizon_value, code). Missing map entries are treated as an
# empty target for that key. If grouped map generation fails, the retained
# legacy scalar cutoff path is gated by --allow-global-cutoff.
#
# Idempotency: the postprocessing service upserts on the full natural key
# (horizon_type, horizon_value, code, date, model_type, valid_from,
# valid_to). Reruns are safe.
#
# Forward interface contract (P0 gi_draft): --station-filter <code> is the
# binding flag name reused by P1a / P1b / P1c / P3 and now P5.
#
# Usage:
#   bash bin/initialize_long_forecast_history.sh <env_file_path> [OPTIONS]
#
# Arguments:
#   env_file_path        Path to the .env_<org> file (required).
#
# Options:
#   --dry-run            Read + filter; do NOT POST anything.
#   --api-url <URL>      Target endpoint (default: http://localhost:8003/long-forecast/).
#   --batch-size <N>     Records per POST batch (default: 500).
#   --image <IMAGE>      Docker image override.
#   --station-filter <CODE>
#                        Filter source CSV rows to a single station code
#                        (binding interface contract from P0 — honored by all
#                        CSV-source wrappers).
#   --mode <NAME>        Restrict the run to a single configured mode
#                        (e.g. month_1).
#   --model <NAME>       Restrict the run to a single model across all
#                        selected modes (e.g. LR_Base).
#   --skip-mode <LIST>   Comma-separated list of mode names to skip
#                        (in addition to the always-skipped `monthly`).
#   -h, --help           Print this message and exit 0.
#
# Examples:
#   bash bin/initialize_long_forecast_history.sh /data/taj_data/config/.env_tjhm --dry-run
#   bash bin/initialize_long_forecast_history.sh /data/taj_data/config/.env_tjhm
#   bash bin/initialize_long_forecast_history.sh /data/taj_data/config/.env_tjhm \
#       --mode month_1 --model LR_Base --dry-run
#   bash bin/initialize_long_forecast_history.sh /data/taj_data/config/.env_tjhm \
#       --skip-mode quarter,seasonal_april --dry-run
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
API_URL="http://localhost:8003/long-forecast/"
BATCH_SIZE=500
IMAGE_OVERRIDE=""
STATION_FILTER=""
MODE_FILTER=""
MODEL_FILTER=""
SKIP_MODE=""
ALLOW_GLOBAL_CUTOFF=false

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------

print_usage() {
    cat <<'USAGE'
Usage: bash bin/initialize_long_forecast_history.sh <env_file_path> [OPTIONS]

Migrate long-term forecast rows from configured-mode hindcast CSVs into the
postprocessing API. Walks <data_ref>/config/long_term_configs/*.json,
discovers (mode, model) pairs that have a matching hindcast CSV on disk,
and POSTs each (mode, model) batch. Honors MODE=full-import vs pre-cutoff
based on target table state: per-mode query when --mode is set, global
query otherwise (gated by --allow-global-cutoff for safety).

Mode skip semantics:
  - `monthly` is ALWAYS skipped (non-operational mode).
  - Modes without a JSON config are HARD-SKIPPED (no synthesis from
    directory layout).
  - `--skip-mode <name,...>` additionally skips configured modes.
  - `--mode <name>` restricts the run to a single configured mode.
  - `--model <name>` restricts the run to a single model across all
    selected modes.

UZB no-op acceptance: if zero modes match the discover-and-filter pipeline,
the wrapper exits 0 with a logged "no source data" message.

Arguments:
  env_file_path        Path to the .env_<org> file (required).

Options:
  --dry-run            Read + filter; do NOT POST anything.
  --api-url <URL>      Target endpoint (default: http://localhost:8003/long-forecast/).
  --batch-size <N>     Records per POST batch (default: 500).
  --image <IMAGE>      Docker image override.
  --station-filter <CODE>
                       Filter source CSV rows to a single station code
                       (binding interface contract from P0 — honored by all
                       CSV-source wrappers).
  --mode <NAME>        Restrict the run to a single configured mode
                       (e.g. month_1).
  --model <NAME>       Restrict the run to a single model across all
                       selected modes (e.g. LR_Base).
  --skip-mode <LIST>   Comma-separated list of mode names to skip
                       (in addition to the always-skipped `monthly`).
  --allow-global-cutoff
                       Opt-in to apply a single conservative cutoff (the
                       global MIN(date) across ALL horizon_value rows) to
                       every mode in this run. Without it, the wrapper
                       refuses to proceed when the target table has rows
                       AND no --mode filter is set. Resolution path:
                       run per-mode with --mode <name> separately, or
                       opt in here.
  -h, --help           Print this message and exit 0.

Examples:
  bash bin/initialize_long_forecast_history.sh /data/taj_data/config/.env_tjhm --dry-run
  bash bin/initialize_long_forecast_history.sh /data/taj_data/config/.env_tjhm
  bash bin/initialize_long_forecast_history.sh /data/taj_data/config/.env_tjhm \
      --mode month_1 --model LR_Base --dry-run
  bash bin/initialize_long_forecast_history.sh /data/taj_data/config/.env_tjhm \
      --skip-mode quarter,seasonal_april --dry-run
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
            --mode)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --mode requires a mode name value.${NC}" >&2
                    exit 1
                fi
                MODE_FILTER="$2"
                shift 2
                ;;
            --model)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --model requires a model name value.${NC}" >&2
                    exit 1
                fi
                MODEL_FILTER="$2"
                shift 2
                ;;
            --skip-mode)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --skip-mode requires a comma-separated list value.${NC}" >&2
                    exit 1
                fi
                SKIP_MODE="$2"
                shift 2
                ;;
            --allow-global-cutoff)
                # Opt in to the retained legacy scalar fallback only. The
                # primary grouped cutoff-map path is code-scoped and does not
                # require this flag.
                ALLOW_GLOBAL_CUTOFF=true
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

# ---------------------------------------------------------------------------
# Query the target postprocessing-db for grouped cutoff-map data.
# Echoes "horizon_type<TAB>horizon_value<TAB>code<TAB>min_date" rows.
# Returns non-zero if the docker exec fails.
#
# NOTE: long_forecasts is in the postprocessing DB (NOT preprocessing). The
# query is grouped by horizon_type, horizon_value, and code so missing codes
# are not filtered by a populated canary code in the same horizon.
# ---------------------------------------------------------------------------
query_cutoff_map_state() {
    local sql="SELECT horizon_type::text, horizon_value::text, code, COALESCE(MIN(date)::text, '') FROM long_forecasts"
    if [[ -n "${MODE_FILTER:-}" ]]; then
        sql="${sql} WHERE horizon_type::text='${MODE_HORIZON_ENUM}' AND horizon_value=${MODE_HORIZON_VALUE}"
    fi
    sql="${sql} GROUP BY horizon_type, horizon_value, code ORDER BY horizon_type::text, horizon_value, code;"

    docker exec sapphire-postprocessing-db psql \
        -U postgres -d postprocessing_db -P pager=off -t -A -F $'\t' \
        -c "$sql"
}

query_legacy_scalar_target_state() {
    local sql="SELECT COUNT(*), COALESCE(MIN(date)::text, '') FROM long_forecasts"
    if [[ -n "${MODE_FILTER:-}" ]]; then
        sql="${sql} WHERE horizon_type::text='${MODE_HORIZON_ENUM}' AND horizon_value=${MODE_HORIZON_VALUE}"
    fi
    sql="${sql};"

    docker exec sapphire-postprocessing-db psql \
        -U postgres -d postprocessing_db -P pager=off -t -A -F $'\t' \
        -c "$sql"
}

write_cutoff_map_json() {
    local raw_tsv="$1"
    local map_json="$2"
    python3 - "$raw_tsv" "$map_json" <<'PYEOF'
import datetime
import json
import sys


def parse_code(raw):
    s = (raw or "").strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    if "." in s:
        try:
            f = float(s)
        except ValueError:
            return s
        if f.is_integer():
            return str(int(f))
    return s


def parse_date(raw):
    s = (raw or "").strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    head = s.split("T", 1)[0].split(" ", 1)[0]
    datetime.date.fromisoformat(head)
    return head


raw_tsv, map_json = sys.argv[1:3]
cutoff_map = {}
with open(raw_tsv, encoding="utf-8") as f:
    for line_no, line in enumerate(f, start=1):
        line = line.rstrip("\r\n")
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) != 4:
            raise SystemExit(f"invalid grouped target row {line_no}: expected 4 tab fields")
        horizon_type_raw, horizon_value_raw, code_raw, cutoff_raw = parts
        horizon_type = horizon_type_raw.strip().lower()
        horizon_value = int(float(horizon_value_raw.strip()))
        code = parse_code(code_raw)
        cutoff = parse_date(cutoff_raw)
        if not horizon_type or not code or not cutoff:
            raise SystemExit(f"invalid grouped target row {line_no}: empty normalized key/cutoff")
        cutoff_map[f"{horizon_type}\t{horizon_value}\t{code}"] = cutoff

with open(map_json, "w", encoding="utf-8") as f:
    json.dump(cutoff_map, f, sort_keys=True)

min_date = min(cutoff_map.values()) if cutoff_map else ""
print(f"{len(cutoff_map)}\t{min_date}")
PYEOF
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
    parse_args "$@"

    print_banner
    echo "| Running Long-Term Forecast History Initialization (CSV -> API push)"

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
    local CONFIG_HOST="${ieasyhydroforecast_data_ref_dir}/config/long_term_configs"
    local DATA_HOST="${ieasyhydroforecast_data_ref_dir}/intermediate_data"
    local LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/long_forecast_history_init"
    mkdir -p "${LOG_DIR}"

    local TIMESTAMP
    TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
    log_file="${LOG_DIR}/long_forecast_history_init_${TIMESTAMP}.log"
    export log_file

    echo "| Log file: ${log_file}"
    echo ""

    umh_log_redacted "Starting long-term forecast history initialization"
    umh_log_redacted "  env_file:        ${ENV_FILE}"
    umh_log_redacted "  config_dir:      ${CONFIG_HOST}"
    umh_log_redacted "  data_dir:        ${DATA_HOST}"
    umh_log_redacted "  api_url:         ${API_URL}"
    umh_log_redacted "  batch_size:      ${BATCH_SIZE}"
    umh_log_redacted "  station_filter:  ${STATION_FILTER:-<none>}"
    umh_log_redacted "  mode_filter:     ${MODE_FILTER:-<none>}"
    umh_log_redacted "  model_filter:    ${MODEL_FILTER:-<none>}"
    umh_log_redacted "  skip_mode:       ${SKIP_MODE:-<none>}"
    umh_log_redacted "  dry_run:         ${DRY_RUN}"
    umh_print_image_resolution_line "${IMAGE}"

    # -------------------------------------------------------------------------
    # Pre-flight checks
    # -------------------------------------------------------------------------

    # 1. Config directory presence (absence is the UZB no-op path; the
    #    Python module exits 0 with a no-source message rather than
    #    erroring out, so we DO NOT fail here).
    if [[ ! -d "$CONFIG_HOST" ]]; then
        umh_log_redacted "Pre-flight: config dir not found (${CONFIG_HOST}); UZB no-op path will be taken"
    else
        umh_log_redacted "Pre-flight: config dir OK (${CONFIG_HOST})"
    fi

    # 2. Data directory presence (the Python module also tolerates absence
    #    per (mode, model) — missing hindcasts log "no hindcast for ..."
    #    and skip).
    if [[ ! -d "$DATA_HOST" ]]; then
        umh_log_redacted "Pre-flight: data dir not found (${DATA_HOST}); all hindcasts will be skipped"
    else
        umh_log_redacted "Pre-flight: data dir OK (${DATA_HOST})"
    fi

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
    #
    # The preferred path builds a grouped cutoff map. When --mode <name> is
    # set, the query is scoped to that mode's derived horizon enum label and
    # horizon_value. If the grouped path fails, fall back to the retained
    # scalar cutoff path and apply the --allow-global-cutoff gate below.
    # -------------------------------------------------------------------------
    local TARGET_MODE="full-import"
    local CUTOFF=""
    local CUTOFF_MAP_HOST=""
    local CUTOFF_MAP_CONTAINER=""
    local TARGET_COUNT=0
    local TARGET_MIN_DATE=""
    local LEGACY_SCALAR_FALLBACK=false
    local MODE_HORIZON_VALUE=""   # populated only when --mode is used
    local MODE_HORIZON_ENUM=""    # PG enum label derived from mode config

    # Acquire a temp workspace (mode 0o700, trap-cleaned on EXIT INT TERM).
    # The cutoff-map artifact can contain real station codes, so keep it in
    # the redacted temp workspace and never print its contents.
    local TMPDIR_LF
    local tmp_workspace_path_file="${LOG_DIR}/.long_forecast_tmp_path_${TIMESTAMP}"
    umh_acquire_temp_workspace long_forecast > "$tmp_workspace_path_file"
    IFS= read -r TMPDIR_LF < "$tmp_workspace_path_file"
    rm -f "$tmp_workspace_path_file"
    umh_log_redacted "Temp workspace: ${TMPDIR_LF}"

    if [[ -n "$MODE_FILTER" ]]; then
        # Per-mode detection: load horizon_value from the mode's config JSON
        # so the cutoff query targets only that mode's rows.
        local mode_config="${CONFIG_HOST}/${MODE_FILTER}.json"
        if [[ ! -f "$mode_config" ]]; then
            umh_log_redacted "ERROR: --mode '${MODE_FILTER}' has no config at ${mode_config}"
            exit 1
        fi
        # NOTE: keep this in sync with migration_py.long_forecast
        # ._load_mode_config — both parsers read 'operational_month_lead_time'
        # and normalize horizon_type before validating the same enum set. If
        # the Python helper's validation rules change, mirror them here so the
        # host-side per-mode query reflects the same semantics.
        local mode_config_out
        if ! mode_config_out=$(python3 - "$mode_config" <<'PYEOF' 2>&1
import json, sys
cfg = json.load(open(sys.argv[1]))
hv = cfg.get("operational_month_lead_time")
if hv is None:
    sys.exit("config is missing 'operational_month_lead_time'")
horizon_type = cfg.get("horizon_type", "month")
if not isinstance(horizon_type, str) or not horizon_type:
    horizon_type = "month"
print(f"{int(hv)}\t{horizon_type.lower()}")
PYEOF
        ); then
            umh_log_redacted "ERROR: failed to read mode horizon config from ${mode_config}: ${mode_config_out}"
            exit 1
        fi
        local mode_horizon_type=""
        IFS=$'\t' read -r MODE_HORIZON_VALUE mode_horizon_type <<< "$mode_config_out"
        case "$mode_horizon_type" in
            month)
                MODE_HORIZON_ENUM="MONTH"
                ;;
            quarter)
                MODE_HORIZON_ENUM="QUARTER"
                ;;
            season)
                MODE_HORIZON_ENUM="SEASON"
                ;;
            *)
                umh_log_redacted "ERROR: --mode '${MODE_FILTER}' has unsupported horizon_type '${mode_horizon_type}' in ${mode_config}"
                umh_log_redacted "ERROR: supported horizon_type values are: month, quarter, season"
                exit 1
                ;;
        esac
        umh_log_redacted "Per-mode detection: --mode=${MODE_FILTER} -> horizon_value=${MODE_HORIZON_VALUE} horizon_type_enum=${MODE_HORIZON_ENUM}"

    fi

    if docker ps --filter "name=sapphire-postprocessing-db" --quiet | grep -q .; then
        local query_out
        if query_out="$(query_cutoff_map_state 2>&1)"; then
            local raw_cutoff_tsv="${TMPDIR_LF}/long_forecast_cutoff_map.tsv"
            local cutoff_map_json="${TMPDIR_LF}/long_forecast_cutoff_map.json"
            printf '%s\n' "$query_out" > "$raw_cutoff_tsv"

            local map_summary
            if map_summary="$(write_cutoff_map_json "$raw_cutoff_tsv" "$cutoff_map_json" 2>&1)"; then
                TARGET_COUNT="${map_summary%%$'\t'*}"
                TARGET_MIN_DATE="${map_summary#*$'\t'}"
                TARGET_COUNT="${TARGET_COUNT//[$'\r\n ']/}"
                TARGET_MIN_DATE="${TARGET_MIN_DATE//[$'\r\n ']/}"
                if [[ "$TARGET_COUNT" != "0" ]]; then
                    TARGET_MODE="pre-cutoff"
                    CUTOFF_MAP_HOST="$cutoff_map_json"
                    CUTOFF_MAP_CONTAINER="/cutoff_map/$(basename "$cutoff_map_json")"
                fi
                umh_log_redacted "Target cutoff map: entries=${TARGET_COUNT} min_date=${TARGET_MIN_DATE:-<null>}"
            else
                umh_log_redacted "WARN: cutoff-map normalization failed (${map_summary}); trying legacy scalar fallback"
                LEGACY_SCALAR_FALLBACK=true
            fi
        else
            umh_log_redacted "WARN: grouped cutoff-map query failed (${query_out}); trying legacy scalar fallback"
            LEGACY_SCALAR_FALLBACK=true
        fi

        if [[ "$LEGACY_SCALAR_FALLBACK" == true ]]; then
            local legacy_out
            if legacy_out="$(query_legacy_scalar_target_state 2>&1)"; then
                TARGET_COUNT="${legacy_out%%$'\t'*}"
                TARGET_MIN_DATE="${legacy_out#*$'\t'}"
                TARGET_COUNT="${TARGET_COUNT//[$'\r\n ']/}"
                TARGET_MIN_DATE="${TARGET_MIN_DATE//[$'\r\n ']/}"
                umh_log_redacted "Legacy scalar target state: count=${TARGET_COUNT} min_date=${TARGET_MIN_DATE:-<null>}"
                if [[ "$TARGET_COUNT" != "0" && -n "$TARGET_MIN_DATE" ]]; then
                    TARGET_MODE="pre-cutoff"
                    CUTOFF="$TARGET_MIN_DATE"
                fi
            else
                umh_log_redacted "ERROR: grouped cutoff-map query failed and legacy scalar fallback also failed (${legacy_out})"
                exit 1
            fi
        fi
    else
        umh_log_redacted "WARN: sapphire-postprocessing-db not running; assuming full-import"
    fi

    if [[ -n "$CUTOFF_MAP_HOST" ]]; then
        umh_log_redacted "MODE=${TARGET_MODE} (cutoff_map_entries=${TARGET_COUNT})"
    else
        umh_log_redacted "MODE=${TARGET_MODE}$( [[ -n "$CUTOFF" ]] && echo " (legacy cutoff=${CUTOFF})" || echo " (target empty)")"
    fi

    # Fail-closed gate for the legacy scalar fallback only. The primary
    # cutoff-map path is code-scoped and needs no operator opt-in.
    #
    # Dry-run runs (--dry-run) keep their preview exemption on the retained
    # legacy path, but real runs must opt in via --allow-global-cutoff.
    if [[ "$TARGET_MODE" == "pre-cutoff" && "$LEGACY_SCALAR_FALLBACK" == true && "$ALLOW_GLOBAL_CUTOFF" != true && "$DRY_RUN" != true ]]; then
        # Shell-quote the script + env file paths so the suggested commands
        # survive copy-paste even when either contains spaces or other
        # metacharacters (round-3 review feedback).
        local script_q env_q
        printf -v script_q '%q' "$0"
        printf -v env_q '%q' "$ENV_FILE"
        umh_log_redacted "ERROR: cutoff-map generation failed and target long_forecasts has rows (count=${TARGET_COUNT})."
        umh_log_redacted "Falling back to one scalar cutoff can skip valid data for missing stations."
        umh_log_redacted ""
        umh_log_redacted "Resolution — pick ONE:"
        umh_log_redacted "  (a) Fix the cutoff-map query/normalization failure and rerun."
        umh_log_redacted "  (b) Accept the conservative legacy scalar cutoff (skips rows >= ${CUTOFF}):"
        umh_log_redacted "        bash ${script_q} ${env_q} --allow-global-cutoff"
        umh_log_redacted ""
        umh_log_redacted "Note: --dry-run is exempt from this gate; rerun with --dry-run to preview."
        exit 1
    fi
    if [[ "$TARGET_MODE" == "pre-cutoff" && "$LEGACY_SCALAR_FALLBACK" == true && "$DRY_RUN" == true && "$ALLOW_GLOBAL_CUTOFF" != true ]]; then
        umh_log_redacted "WARNING (dry-run exemption): cutoff-map failed + target has rows + no --allow-global-cutoff."
        umh_log_redacted "  A real run with these args would abort. Inventory below previews the legacy scalar cutoff effect."
    fi
    if [[ "$TARGET_MODE" == "pre-cutoff" && "$LEGACY_SCALAR_FALLBACK" == true && "$ALLOW_GLOBAL_CUTOFF" == true ]]; then
        umh_log_redacted "WARNING: applying legacy scalar cutoff (${CUTOFF}) because operator opted in via --allow-global-cutoff."
    fi

    # -------------------------------------------------------------------------
    # Run the Python helper inside the prepgateway image.
    # Mount migration_py/ read-only; mount the config + data dirs read-only.
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

    local CUTOFF_MAP_MOUNT_ARGS=()
    local CUTOFF_MAP_ARGS=()
    if [[ -n "$CUTOFF_MAP_HOST" ]]; then
        CUTOFF_MAP_MOUNT_ARGS=(-v "${TMPDIR_LF}:/cutoff_map:ro")
        CUTOFF_MAP_ARGS=("--cutoff-map" "$CUTOFF_MAP_CONTAINER")
    fi

    local STATION_ARGS=()
    if [[ -n "$STATION_FILTER" ]]; then
        STATION_ARGS=("--station-filter" "$STATION_FILTER")
    fi

    local MODE_ARGS=()
    if [[ -n "$MODE_FILTER" ]]; then
        MODE_ARGS=("--mode" "$MODE_FILTER")
    fi

    local MODEL_ARGS=()
    if [[ -n "$MODEL_FILTER" ]]; then
        MODEL_ARGS=("--model" "$MODEL_FILTER")
    fi

    local SKIP_MODE_ARGS=()
    if [[ -n "$SKIP_MODE" ]]; then
        SKIP_MODE_ARGS=("--skip-mode" "$SKIP_MODE")
    fi

    umh_log_redacted "========================================"
    umh_log_redacted "Running long-term forecast push via docker"
    umh_log_redacted "  image:        ${IMAGE}"
    umh_log_redacted "  config_dir:   ${CONFIG_HOST} -> /config/long_term_configs (ro)"
    umh_log_redacted "  data_dir:     ${DATA_HOST} -> /intermediate_data (ro)"
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
        -v "${CONFIG_HOST}:/config/long_term_configs:ro" \
        -v "${DATA_HOST}:/intermediate_data:ro" \
        "${CUTOFF_MAP_MOUNT_ARGS[@]}" \
        -v "${HELPER_DIR}/migration_py:/opt/migration_py:ro" \
        -e "PYTHONPATH=/opt" \
        "$IMAGE" \
        python3 -m migration_py.long_forecast \
            --config-dir /config/long_term_configs \
            --data-dir /intermediate_data \
            --api-url "${API_URL}" \
            --batch-size "${BATCH_SIZE}" \
            "${CUTOFF_ARGS[@]}" \
            "${CUTOFF_MAP_ARGS[@]}" \
            "${STATION_ARGS[@]}" \
            "${MODE_ARGS[@]}" \
            "${MODEL_ARGS[@]}" \
            "${SKIP_MODE_ARGS[@]}" \
            ${DRY_RUN_FLAG} \
        2>&1 | tee -a "${log_file}"

    local DOCKER_EXIT=${PIPESTATUS[0]}

    # -------------------------------------------------------------------------
    # Post-run summary.
    # -------------------------------------------------------------------------
    echo ""
    echo -e "${BOLD}========================================"
    echo -e " LONG FORECAST HISTORY INIT COMPLETE"
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
    echo "    \"SELECT horizon_type, horizon_value, model_type, COUNT(*) AS rows, MIN(date), MAX(date) FROM long_forecasts GROUP BY horizon_type, horizon_value, model_type ORDER BY horizon_type, horizon_value, model_type;\""
    echo ""
    echo "Log file: ${log_file}"

    exit "$DOCKER_EXIT"
}

main "$@"
