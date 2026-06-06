#!/usr/bin/env bash
# ============================================================================
# export_lr_forecast_history.sh
#
# LAPTOP-SIDE export wrapper for P4a (LR linear-regression forecast history).
#
# Connects to the laptop's local ``sapphire-postprocessing-db`` (NOT
# preprocessing-db — the ``lr_forecasts`` table lives in the postprocessing
# service) or any source DB reachable via the standard PG* environment
# variables, and dumps rows from the ``lr_forecasts`` table filtered on:
#   1. ``horizon_type``: 'pentad' or 'decade' (the --horizon flag,
#      lowercase — architecture §Q4 enum lock; the Pydantic boundary
#      rejects uppercase),
#   2. ``code`` IN (deployment's station list) — read line-by-line from the
#      ``--station-list-file`` argument (one code per line) when supplied.
#      Without a station list, no station filter is applied. The manifest
#      always records the realized distinct-station count from the CSV.
#
# Emits TWO files in the operator-chosen ``--output-dir``:
#   - ``lr_forecast_<horizon>_<UTC_timestamp>.csv``
#   - ``lr_forecast_<horizon>_<UTC_timestamp>.csv.manifest``
#
# Manifest sidecar follows the P0 contract (per
# ``migration_py._common.validate_manifest``); 5 required keys:
#   export_type=lr_forecast
#   row_count=<n>          # excluding header
#   station_count=<n>      # distinct ``code`` in CSV
#   date_min=<YYYY-MM-DD>
#   date_max=<YYYY-MM-DD>
#
# Source DB note (POSTPROCESSING, not preprocessing):
#   The ``lr_forecasts`` table is owned by the postprocessing service. On a
#   laptop with the full SAPPHIRE stack running, the matching local DB is
#   ``sapphire-postprocessing-db`` (typically port 5433 in the dev compose).
#   The export uses standard libpq PG* env vars so any reachable source DB
#   works. The location guard below treats either
#   ``sapphire-preprocessing-db`` or ``sapphire-postprocessing-db`` as a
#   "deployment host" trigger.
#
# Location guard (Stage E #6):
#   This script REFUSES to run on a host where any
#   ``sapphire-{preprocessing,postprocessing,api-gateway}-...`` container is
#   detected. It is meant to run on the OPERATOR LAPTOP / a jump host, NOT
#   on the deployment server (which has its own postprocessing-db and would
#   produce a confusing self-export). Bypass for the developer-laptop
#   test suite ONLY: ``_P4A_EXPORT_SKIP_LOCATION_GUARD=1`` — underscore
#   prefix, documented internal use only, never set in real workflows.
#
# Credentials and secret hygiene (per runbook §3):
#   - Uses PG* env vars + ``~/.pgpass`` (mode 0o600). No password in CLI.
#   - Invokes psql with ``-X`` (skip .psqlrc) and ``-v ON_ERROR_STOP=1``.
#   - Sets HISTFILE / PSQL_HISTORY to /dev/null.
#   - umask 077 for all writes.
#   - Never tees secrets into logs.
#
# Schema note (no model_type column):
#   ``lr_forecasts`` has the DB unique key (horizon_type, code, date).
#   LR is implicit in this table — there is NO ``model_type`` column.
#   (This is why LR rows are filtered OUT of ``combined_forecasts`` by
#   the postprocessing combined_forecasts migrator.) The export's
#   ``COPY (SELECT ...)`` does NOT include ``model_type``.
#
# Universal safe-write rule alignment (architecture §Q2 layer 2):
#   Nullable model-stat fields (``discharge_avg``, ``predictor``, ``slope``,
#   ``intercept``, ``forecasted_discharge``, ``q_mean``, ``q_std_sigma``,
#   ``delta``, ``rsquared``) are exported as their literal DB values; psql
#   COPY emits empty strings for NULL. The server-side Python helper
#   (`migration_py.lr_forecast`) OMITS empty / null-like fields from the
#   API payload, so the wrapper never injects ``null`` into the upsert
#   path (which would otherwise overwrite existing non-NULL targets via
#   the service-side ``_has_changes + setattr`` overwrite bug).
#
# Usage:
#   bash bin/export_lr_forecast_history.sh <env_file_path> \
#       --horizon pentad|decade \
#       [--output-dir <path>] \
#       [--station-list-file <path>] \
#       [--dry-run]
#
# Arguments:
#   env_file_path        Path to the operator's .env_<org> file. Used only
#                        to derive default --output-dir; PG credentials are
#                        sourced from the operator's shell environment +
#                        ~/.pgpass (NOT from the env file).
#
# Options:
#   --horizon <H>        REQUIRED. 'pentad' or 'decade' — the horizon_type
#                        enum (lowercase per architecture §Q4 lock).
#   --output-dir <PATH>  Destination dir for the CSV + manifest pair. Default:
#                        ``./lr_forecast_export_<UTC_timestamp>`` under the
#                        operator's CWD. Created with mode 0o700 if missing.
#   --station-list-file <PATH>
#                        File listing deployment station codes, one per line.
#                        When supplied, the COPY query filters
#                        ``code IN (<list>)`` so cross-org codes never leak.
#                        Lines containing an apostrophe are rejected outright
#                        (SQL-injection guard).
#   --dry-run            Run COUNT(*) only; do NOT write CSV or manifest.
#   --i-am-on-laptop     Acknowledge the location guard bypass (used only
#                        by the test suite via the env-var hook above).
#                        Operators do NOT set this.
#   -h, --help           Print this message and exit 0.
#
# Environment:
#   PGHOST / PGPORT / PGUSER / PGDATABASE
#       Standard libpq vars. No password in CLI; use ~/.pgpass (mode 0600).
#       For the canonical laptop SAPPHIRE stack the postprocessing-db typically
#       listens on PGPORT=5433.
#
# Examples:
#   # Pentad export, no station filter (use ONLY for trusted single-org laptops).
#   bash bin/export_lr_forecast_history.sh ~/.env_tjhm --horizon pentad
#
#   # Decade export with station list and a custom output dir.
#   bash bin/export_lr_forecast_history.sh ~/.env_tjhm \
#       --horizon decade \
#       --station-list-file ~/taj_stations.txt \
#       --output-dir /tmp/lr_export
#
# Server side (after scp of both files):
#   bash bin/initialize_lr_forecast_history.sh <env_file> \
#       --from-export <csv_path> --horizon pentad|decade
#
# ============================================================================

set -eo pipefail
# NOTE: set -u is intentionally omitted; we guard each variable with ${name:-}.

# ---------------------------------------------------------------------------
# Source shared helpers (P0 foundation) — for umh_log_redacted only. The
# laptop side does NOT call read_configuration (no Docker, no deployment
# env vars needed); PG credentials come from the operator's shell.
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
HORIZON=""
OUTPUT_DIR=""
STATION_LIST_FILE=""
DRY_RUN=false
LOCATION_GUARD_BYPASS=false

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------

print_usage() {
    cat <<'USAGE'
Usage: bash bin/export_lr_forecast_history.sh <env_file_path> \
           --horizon pentad|decade [OPTIONS]

LAPTOP-SIDE export of LR (linear regression) forecast history rows from
the operator's local sapphire-postprocessing-db.lr_forecasts table into a
CSV + manifest pair, for transfer to a deployment server via scp. Pair with
`bin/initialize_lr_forecast_history.sh` on the receiving side.

Arguments:
  env_file_path        Path to the operator's .env_<org> file. Only used to
                       derive default --output-dir; PG credentials come
                       from the shell environment + ~/.pgpass.

Options:
  --horizon pentad|decade
                       REQUIRED. Selects the lowercase horizon_type enum
                       (architecture §Q4 lock; Pydantic boundary rejects
                       uppercase).
  --output-dir PATH    Destination directory for the CSV + manifest pair.
                       Default: ./lr_forecast_export_<UTC_timestamp>.
                       Created with mode 0o700 if missing.
  --station-list-file PATH
                       Optional file listing deployment station codes, one
                       per line. When supplied, the COPY query filters
                       code IN (<list>) so cross-org codes never leak.
                       This is the binding interface contract from P0:
                       the --station-filter flag on the server-side
                       wrapper accepts a single code; --station-list-file
                       is the export-side equivalent for the whole list.
  --dry-run            Run COUNT(*) query only; do NOT write CSV / manifest.
  --i-am-on-laptop     Acknowledge the location guard bypass (testing-only;
                       not used by operators in real workflows).
  -h, --help           Print this message and exit 0.

Environment:
  PGHOST / PGPORT / PGUSER / PGDATABASE   Standard libpq vars. No password
                                          in CLI; use ~/.pgpass (mode 0600).

Refuses to run if any sapphire-{preprocessing,postprocessing,api-gateway}
container is detected locally (this script is for laptops / jump hosts,
NOT deployment servers — Stage E #6).
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

    # First positional arg is the env file path.
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
            --horizon)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --horizon requires 'pentad' or 'decade'.${NC}" >&2
                    exit 1
                fi
                HORIZON="$2"
                shift 2
                ;;
            --output-dir)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --output-dir requires a path.${NC}" >&2
                    exit 1
                fi
                OUTPUT_DIR="$2"
                shift 2
                ;;
            --station-list-file)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --station-list-file requires a path.${NC}" >&2
                    exit 1
                fi
                STATION_LIST_FILE="$2"
                shift 2
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --i-am-on-laptop)
                LOCATION_GUARD_BYPASS=true
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
    if [[ -z "$HORIZON" ]]; then
        echo -e "${RED}Error: --horizon is required (pentad|decade).${NC}" >&2
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
# Location guard (Stage E #6): refuse on a deployment-server host.
#
# Testing-only bypass: set ``_P4A_EXPORT_SKIP_LOCATION_GUARD=1`` in the
# environment to skip the docker-ps probe. This is intended ONLY for the
# unit-test suite. It is NEVER set by operators in real export workflows.
# ---------------------------------------------------------------------------
location_guard() {
    if [[ "${_P4A_EXPORT_SKIP_LOCATION_GUARD:-}" == "1" ]]; then
        # Testing-only bypass; see docstring above.
        return 0
    fi
    if [[ "$LOCATION_GUARD_BYPASS" == true ]]; then
        echo -e "${YELLOW}WARNING: --i-am-on-laptop set; location guard bypassed.${NC}" >&2
        return 0
    fi
    if ! command -v docker >/dev/null 2>&1; then
        # No docker — definitely not a deployment server. Proceed.
        return 0
    fi
    local detected=""
    for name in sapphire-preprocessing-db sapphire-postprocessing-db sapphire-api-gateway; do
        if docker ps --filter "name=${name}" --quiet 2>/dev/null | grep -q .; then
            detected="${detected}${name} "
        fi
    done
    if [[ -n "$detected" ]]; then
        echo -e "${RED}Error: deployment-server containers detected on this host: ${detected}${NC}" >&2
        echo "" >&2
        echo "This export script runs on the OPERATOR LAPTOP / jump host, not on the" >&2
        echo "deployment server. Use the server-side initialize wrapper instead:" >&2
        echo "    bash bin/initialize_lr_forecast_history.sh <env_file> ..." >&2
        echo "" >&2
        echo "If your laptop intentionally has the SAPPHIRE stack running locally," >&2
        echo "you can acknowledge the override with --i-am-on-laptop (testing use only)." >&2
        exit 1
    fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
    parse_args "$@"

    # Strict perms for everything we write below.
    umask 077

    # Suppress shell history that might capture credentials.
    export HISTFILE=/dev/null
    export PSQL_HISTORY=/dev/null

    # Location guard FIRST — fail loudly before any DB connection.
    location_guard

    # Validate env file exists (we don't source it; just confirm the path).
    if [[ ! -f "$ENV_FILE" ]]; then
        echo -e "${RED}Error: env file not found: ${ENV_FILE}${NC}" >&2
        exit 1
    fi

    # Default output dir if not supplied.
    if [[ -z "$OUTPUT_DIR" ]]; then
        local default_ts
        default_ts="$(date -u +%Y%m%dT%H%M%SZ)"
        OUTPUT_DIR="./lr_forecast_export_${default_ts}"
    fi

    if ! mkdir -p "$OUTPUT_DIR"; then
        echo -e "${RED}Error: cannot create output dir: ${OUTPUT_DIR}${NC}" >&2
        exit 1
    fi
    chmod 700 "$OUTPUT_DIR" 2>/dev/null || true

    # Validate station list file if supplied.
    if [[ -n "$STATION_LIST_FILE" && ! -f "$STATION_LIST_FILE" ]]; then
        echo -e "${RED}Error: station list file not found: ${STATION_LIST_FILE}${NC}" >&2
        exit 1
    fi

    # PG env vars must be set (no password in CLI; use ~/.pgpass).
    if [[ -z "${PGHOST:-}" || -z "${PGUSER:-}" || -z "${PGDATABASE:-}" ]]; then
        echo -e "${RED}Error: PGHOST / PGUSER / PGDATABASE must be set (use ~/.pgpass for password).${NC}" >&2
        exit 1
    fi

    local ts
    ts="$(date -u +%Y%m%dT%H%M%SZ)"

    local csv_path="${OUTPUT_DIR}/lr_forecast_${HORIZON}_${ts}.csv"
    local manifest_path="${csv_path}.manifest"

    # Log file (redacted; no codes ever appear here).
    local log_file="${OUTPUT_DIR}/export_lr_forecast_${HORIZON}_${ts}.log"
    export log_file

    echo -e "${BOLD}========================================"
    echo -e " LR FORECAST EXPORT (P4a) — horizon=${HORIZON}"
    echo -e "========================================${NC}"
    umh_log_redacted "  env_file:            ${ENV_FILE}"
    umh_log_redacted "  horizon:             ${HORIZON}"
    umh_log_redacted "  output_dir:          ${OUTPUT_DIR}"
    umh_log_redacted "  csv_path:            ${csv_path}"
    umh_log_redacted "  manifest_path:       ${manifest_path}"
    umh_log_redacted "  station_list_file:   ${STATION_LIST_FILE:-<none>}"
    umh_log_redacted "  dry_run:             ${DRY_RUN}"
    umh_log_redacted "  source_db:           ${PGUSER}@${PGHOST}:${PGPORT:-5432}/${PGDATABASE}"

    # Build a SQL-safe ``code IN (...)`` clause from the station list file.
    # Each line is single-quoted and joined by commas. Lines that contain a
    # single quote are rejected outright (defensive; station codes are
    # numeric strings in this codebase). Skipped if no file supplied.
    local stations_clause=""
    local station_count_in_file=0
    if [[ -n "$STATION_LIST_FILE" ]]; then
        while IFS= read -r line || [[ -n "$line" ]]; do
            # Trim whitespace.
            line="${line#"${line%%[![:space:]]*}"}"
            line="${line%"${line##*[![:space:]]}"}"
            [[ -z "$line" ]] && continue
            case "$line" in
                *\'*)
                    echo -e "${RED}Error: station list contains an apostrophe — rejecting (SQL-injection guard).${NC}" >&2
                    exit 1
                    ;;
            esac
            if [[ -z "$stations_clause" ]]; then
                stations_clause="'${line}'"
            else
                stations_clause="${stations_clause},'${line}'"
            fi
            station_count_in_file=$((station_count_in_file + 1))
        done < "$STATION_LIST_FILE"

        if [[ "$station_count_in_file" -eq 0 ]]; then
            echo -e "${RED}Error: station list file is empty (no codes after trim).${NC}" >&2
            exit 1
        fi
        umh_log_redacted "  station_count_in_file: ${station_count_in_file}"
    else
        umh_log_redacted "  (no --station-list-file; exporting all codes from the source DB)"
    fi

    # Build the WHERE clause: horizon_type plus optional station list.
    local where_clause="horizon_type='${HORIZON}'"
    if [[ -n "$stations_clause" ]]; then
        where_clause="${where_clause} AND code IN (${stations_clause})"
    fi

    # Dry-run path: COUNT(*) only.
    if [[ "$DRY_RUN" == true ]]; then
        umh_log_redacted "DRY RUN: running COUNT(*) only; no CSV / manifest will be written."
        local count_sql
        count_sql=$(cat <<SQL
SELECT COUNT(*) AS row_count,
       COUNT(DISTINCT code) AS station_count,
       COALESCE(MIN(date)::text, '') AS date_min,
       COALESCE(MAX(date)::text, '') AS date_max
FROM lr_forecasts
WHERE ${where_clause};
SQL
)
        psql -X -v ON_ERROR_STOP=1 -P pager=off -A -F $'\t' -c "${count_sql}" \
            | tee -a "${log_file}"
        echo ""
        echo -e "${YELLOW}DRY RUN complete — no files written.${NC}"
        exit 0
    fi

    # Real export: COPY (SELECT ...) TO STDOUT with CSV HEADER.
    # Column list mirrors LRForecastBase (no model_type — this table does
    # not have that column; LR is implicit). The lowercase horizon_type
    # value matches the API enum (architecture §Q4).
    #
    # The horizon-specific columns ``pentad_in_month`` / ``pentad_in_year``
    # vs ``decad_in_month`` / ``decad_in_year`` are NOT DB columns — they
    # exist only in the legacy CSV-source flow (data_migrator.py). On the
    # DB side, ``horizon_value`` / ``horizon_in_year`` are the literal
    # column names. We alias them OUT to the legacy CSV column names so
    # the server-side helper's column-mapping table (which expects the CSV
    # names) finds them.
    local horizon_value_alias
    local horizon_in_year_alias
    if [[ "$HORIZON" == "pentad" ]]; then
        horizon_value_alias="pentad_in_month"
        horizon_in_year_alias="pentad_in_year"
    else
        horizon_value_alias="decad_in_month"
        horizon_in_year_alias="decad_in_year"
    fi

    local copy_sql
    copy_sql=$(cat <<SQL
COPY (
    SELECT lower(horizon_type::text) AS horizon_type,
           code,
           date::text AS date,
           horizon_value AS ${horizon_value_alias},
           horizon_in_year AS ${horizon_in_year_alias},
           discharge_avg,
           predictor,
           slope,
           intercept,
           forecasted_discharge,
           q_mean,
           q_std_sigma,
           delta,
           rsquared
    FROM lr_forecasts
    WHERE ${where_clause}
    ORDER BY code, date
) TO STDOUT WITH CSV HEADER;
SQL
)

    umh_log_redacted "Running COPY to ${csv_path}"
    psql -X -v ON_ERROR_STOP=1 -P pager=off -c "${copy_sql}" > "${csv_path}"
    chmod 600 "${csv_path}"

    # Compute manifest fields from the produced CSV (single canonical
    # source: count -1 for header, count distinct ``code``, scan dates).
    local row_count
    row_count=$(($(wc -l < "${csv_path}") - 1))
    if [[ "$row_count" -lt 0 ]]; then
        row_count=0
    fi

    # Reject zero-row exports up front (review feedback): a blank date_min /
    # date_max manifest is non-ISO and gets rejected by the server-side
    # validator anyway. Surface this as a clear operator error before any
    # sidecar is written. The partial CSV (header only) is removed so re-runs
    # start clean.
    if [[ "$row_count" -eq 0 ]]; then
        umh_log_redacted "ERROR: no rows matched filter — nothing to export."
        umh_log_redacted "Check the station list, --horizon flag, and date range."
        rm -f "${csv_path}"
        exit 1
    fi

    # distinct codes + date range — single python pass for performance.
    local manifest_fields
    manifest_fields=$(python3 - "$csv_path" <<'PYEOF'
import csv, sys
codes = set()
date_min = None
date_max = None
with open(sys.argv[1], newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        c = (row.get("code") or "").strip()
        if c:
            codes.add(c)
        d = (row.get("date") or "").strip()[:10]
        if d:
            if date_min is None or d < date_min:
                date_min = d
            if date_max is None or d > date_max:
                date_max = d
print(f"{len(codes)}\t{date_min or ''}\t{date_max or ''}")
PYEOF
)
    local manifest_station_count manifest_date_min manifest_date_max
    manifest_station_count="${manifest_fields%%$'\t'*}"
    manifest_fields="${manifest_fields#*$'\t'}"
    manifest_date_min="${manifest_fields%%$'\t'*}"
    manifest_date_max="${manifest_fields#*$'\t'}"

    # Build the manifest. station_count and date_min/max are computed from
    # the produced CSV (NOT from the stations-file count) so manifest
    # validation on the server side catches any mid-flight tampering.
    cat > "${manifest_path}" <<MANIFEST
# Generated by bin/export_lr_forecast_history.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ)
export_type=lr_forecast
horizon=${HORIZON}
row_count=${row_count}
station_count=${manifest_station_count}
date_min=${manifest_date_min}
date_max=${manifest_date_max}
MANIFEST
    chmod 600 "${manifest_path}"

    umh_log_redacted "Wrote CSV:      ${csv_path} (${row_count} rows)"
    umh_log_redacted "Wrote manifest: ${manifest_path}"
    umh_log_redacted "  station_count (in csv):  ${manifest_station_count}"
    umh_log_redacted "  date_min:                ${manifest_date_min:-none}"
    umh_log_redacted "  date_max:                ${manifest_date_max:-none}"

    echo ""
    echo -e "${GREEN}Export complete.${NC} Transfer BOTH files together to the deployment server:"
    echo "  scp ${csv_path} ${manifest_path} <user>@<server>:<data_root>/logs/"
    echo ""
    echo "Then on the server:"
    echo "  bash bin/initialize_lr_forecast_history.sh \\"
    echo "      <env_file> --from-export <transferred_csv> --horizon ${HORIZON}"
}

main "$@"
