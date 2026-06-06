#!/usr/bin/env bash
# ============================================================================
# export_runoff_period_history.sh
#
# LAPTOP-SIDE export wrapper for P2a (runoff PENTAD/DECADE history).
#
# Connects to the laptop's local ``sapphire-preprocessing-db`` (or any source
# DB reachable via the standard PG* environment variables) and dumps rows
# from the ``runoffs`` table filtered on:
#   1. ``horizon_type``: 'PENTAD' or 'DECADE' (the --horizon flag),
#   2. ``code`` IN (deployment's station list) — read line-by-line from the
#      ``--stations-file`` argument; one code per line.
#
# Emits TWO files in the operator-chosen ``--out-dir``:
#   - ``runoff_period_<horizon>_<UTC_timestamp>.csv``
#   - ``runoff_period_<horizon>_<UTC_timestamp>.csv.manifest``
#
# Manifest sidecar follows the P0 contract (per
# ``migration_py._common.validate_manifest``); 5 required keys:
#   export_type=runoff_period
#   row_count=<n>          # excluding header
#   station_count=<n>      # distinct ``code`` in CSV
#   date_min=<YYYY-MM-DD>
#   date_max=<YYYY-MM-DD>
#
# Column-name correction (vs the earlier sub-orch brief):
#   The DB column on ``runoffs`` is ``discharge`` (NOT ``discharge_avg`` — the
#   ``discharge_avg`` form lives in the CSV-source migrator at
#   ``sapphire/services/preprocessing/app/data_migrator.py`` and is the wide
#   CSV column name only). Both the SELECT and the resulting CSV header use
#   ``discharge`` so the server-side wrapper maps the column straight through
#   to the API payload key without any column renaming.
#
# Location guard (Stage E #6):
#   This script REFUSES to run on a host where any
#   ``sapphire-{preprocessing,postprocessing,api-gateway}-...`` container is
#   detected. It is meant to run on the OPERATOR LAPTOP / a jump host, NOT on
#   the deployment server (which has its own DB and would produce a confusing
#   self-export).
#
# Credentials and secret hygiene (per runbook §3):
#   - Uses PG* env vars + ``~/.pgpass`` (mode 0o600). No password in CLI.
#   - Invokes psql with ``-X`` (skip .psqlrc) and ``-v ON_ERROR_STOP=1``.
#   - Sets HISTFILE / PSQL_HISTORY to /dev/null.
#   - umask 077 for all writes.
#   - Never tees secrets into logs.
#
# Usage:
#   bash bin/export_runoff_period_history.sh \
#       --horizon pentad|decade \
#       --stations-file PATH \
#       --out-dir PATH \
#       [--dry-run]
#
# Server side (after scp of both files):
#   bash bin/initialize_runoff_period_history.sh <env_file> \
#       --from-export <csv_path> \
#       --horizon pentad|decade
#
# ============================================================================

set -eo pipefail
# NOTE: set -u intentionally omitted; we guard each variable with ${name:-}.

# ---------------------------------------------------------------------------
# Source shared helpers (P0 foundation) — for umh_log_redacted + manifest
# helper functions. The laptop side does NOT call read_configuration, so we
# don't need the deployment env file here.
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
HORIZON=""
STATIONS_FILE=""
OUT_DIR=""
DRY_RUN=false

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------

print_usage() {
    cat <<'USAGE'
Usage: bash bin/export_runoff_period_history.sh [OPTIONS]

LAPTOP-SIDE export of runoff PENTAD or DECADE history from the local
sapphire-preprocessing-db into a CSV + manifest pair, for transfer to a
deployment server via scp. Pair with `bin/initialize_runoff_period_history.sh`
on the receiving side.

Options:
  --horizon pentad|decade   REQUIRED. Which horizon to export.
  --stations-file PATH      REQUIRED. File listing deployment station codes,
                            one per line. Used to filter the source DB rows
                            so cross-org codes never leak.
  --out-dir PATH            REQUIRED. Directory to write the CSV + manifest.
                            Created with mode 0o700 if missing.
  --dry-run                 Run the COUNT(*) query only; do NOT export rows.
  -h, --help                Print this message and exit 0.

Environment:
  PGHOST / PGPORT / PGUSER / PGDATABASE   Standard libpq vars. No password
                                          in CLI; use ~/.pgpass (mode 0600).

Refuses to run if any sapphire-* container is detected locally (this script
is for laptops / jump hosts, NOT deployment servers — Stage E #6).
USAGE
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

parse_args() {
    # Handle --help / -h before requiring args.
    for arg in "$@"; do
        case "$arg" in
            -h|--help) print_usage; exit 0 ;;
        esac
    done

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
            --stations-file)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --stations-file requires a path.${NC}" >&2
                    exit 1
                fi
                STATIONS_FILE="$2"
                shift 2
                ;;
            --out-dir)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --out-dir requires a path.${NC}" >&2
                    exit 1
                fi
                OUT_DIR="$2"
                shift 2
                ;;
            --dry-run)
                DRY_RUN=true
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
        echo -e "${RED}Error: --horizon is required.${NC}" >&2
        exit 1
    fi
    case "$HORIZON" in
        pentad|decade) ;;
        *)
            echo -e "${RED}Error: --horizon must be 'pentad' or 'decade' (got: ${HORIZON}).${NC}" >&2
            exit 1
            ;;
    esac
    if [[ -z "$STATIONS_FILE" ]]; then
        echo -e "${RED}Error: --stations-file is required.${NC}" >&2
        exit 1
    fi
    if [[ -z "$OUT_DIR" ]]; then
        echo -e "${RED}Error: --out-dir is required.${NC}" >&2
        exit 1
    fi
}

# ---------------------------------------------------------------------------
# Location guard (Stage E #6): refuse on a deployment-server host.
#
# Testing-only bypass: set ``_P2A_EXPORT_SKIP_LOCATION_GUARD=1`` in the
# environment to skip the docker-ps probe. This is intended ONLY for the
# unit-test suite (which runs on developer laptops that may themselves have
# the SAPPHIRE stack running locally for development); it is NEVER set by
# operators in real export workflows.
# ---------------------------------------------------------------------------
location_guard() {
    if [[ "${_P2A_EXPORT_SKIP_LOCATION_GUARD:-}" == "1" ]]; then
        # Testing-only bypass; see docstring above.
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
        echo "    bash bin/initialize_runoff_period_history.sh <env_file> ..." >&2
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

    # Validate inputs on disk.
    if [[ ! -f "$STATIONS_FILE" ]]; then
        echo -e "${RED}Error: stations file not found: ${STATIONS_FILE}${NC}" >&2
        exit 1
    fi
    if ! mkdir -p "$OUT_DIR"; then
        echo -e "${RED}Error: cannot create out-dir: ${OUT_DIR}${NC}" >&2
        exit 1
    fi
    chmod 700 "$OUT_DIR" 2>/dev/null || true

    # PG env vars must be set (no password in CLI; use ~/.pgpass).
    if [[ -z "${PGHOST:-}" || -z "${PGUSER:-}" || -z "${PGDATABASE:-}" ]]; then
        echo -e "${RED}Error: PGHOST / PGUSER / PGDATABASE must be set (use ~/.pgpass for password).${NC}" >&2
        exit 1
    fi

    local horizon_upper
    horizon_upper="$(echo "$HORIZON" | tr '[:lower:]' '[:upper:]')"

    local ts
    ts="$(date -u +%Y%m%dT%H%M%SZ)"

    local csv_path="${OUT_DIR}/runoff_period_${HORIZON}_${ts}.csv"
    local manifest_path="${csv_path}.manifest"

    # Log file (redacted; no codes ever appear here).
    local log_file="${OUT_DIR}/export_${HORIZON}_${ts}.log"
    export log_file

    echo -e "${BOLD}========================================"
    echo -e " RUNOFF ${horizon_upper} EXPORT (P2a)"
    echo -e "========================================${NC}"
    umh_log_redacted "  horizon:       ${HORIZON} (${horizon_upper})"
    umh_log_redacted "  stations_file: ${STATIONS_FILE}"
    umh_log_redacted "  out_dir:       ${OUT_DIR}"
    umh_log_redacted "  csv_path:      ${csv_path}"
    umh_log_redacted "  manifest_path: ${manifest_path}"
    umh_log_redacted "  dry_run:       ${DRY_RUN}"

    # Build a SQL-safe ``code IN (...)`` clause from the stations file.
    # Each line is single-quoted and joined by commas. Lines that contain a
    # single quote are rejected outright (defensive; station codes are
    # numeric strings in this codebase).
    local stations_clause=""
    local station_count=0
    while IFS= read -r line || [[ -n "$line" ]]; do
        # Trim whitespace.
        line="${line#"${line%%[![:space:]]*}"}"
        line="${line%"${line##*[![:space:]]}"}"
        [[ -z "$line" ]] && continue
        case "$line" in
            *\'*)
                echo -e "${RED}Error: stations file contains an apostrophe — rejecting (SQL-injection guard).${NC}" >&2
                exit 1
                ;;
        esac
        if [[ -z "$stations_clause" ]]; then
            stations_clause="'${line}'"
        else
            stations_clause="${stations_clause},'${line}'"
        fi
        station_count=$((station_count + 1))
    done < "$STATIONS_FILE"

    if [[ "$station_count" -eq 0 ]]; then
        echo -e "${RED}Error: stations file is empty (no codes after trim).${NC}" >&2
        exit 1
    fi

    umh_log_redacted "  station_count_in_stations_file: ${station_count}"

    # Dry-run path: COUNT(*) only.
    if [[ "$DRY_RUN" == true ]]; then
        umh_log_redacted "DRY RUN: running COUNT(*) only; no CSV / manifest will be written."
        local count_sql
        count_sql=$(cat <<SQL
SELECT COUNT(*) AS row_count,
       COUNT(DISTINCT code) AS station_count,
       MIN(date)::text AS date_min,
       MAX(date)::text AS date_max
FROM runoffs
WHERE horizon_type='${horizon_upper}'
  AND code IN (${stations_clause});
SQL
)
        psql -X -v ON_ERROR_STOP=1 -P pager=off -A -F $'\t' -c "${count_sql}" \
            | tee -a "${log_file}"
        echo ""
        echo -e "${YELLOW}DRY RUN complete — no files written.${NC}"
        exit 0
    fi

    # Real export: COPY (SELECT ...) TO STDOUT with CSV HEADER.
    # The SELECT lists ALL writable RunoffBase columns; the CSV column names
    # match the API payload keys so the server-side wrapper does NOT need to
    # rename anything. ``discharge`` and ``predictor`` are the canonical DB
    # column names (NOT ``discharge_avg``; see file docstring for the
    # correction note). The lowercase horizon_type form is emitted as
    # 'pentad' / 'decade' to match the payload convention.
    local copy_sql
    copy_sql=$(cat <<SQL
COPY (
    SELECT lower(horizon_type::text) AS horizon_type,
           code,
           date::text AS date,
           discharge,
           predictor,
           horizon_value,
           horizon_in_year
    FROM runoffs
    WHERE horizon_type='${horizon_upper}'
      AND code IN (${stations_clause})
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
        d = (row.get("date") or "").strip()
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
# Generated by bin/export_runoff_period_history.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ)
export_type=runoff_period
horizon=${HORIZON}
row_count=${row_count}
station_count=${manifest_station_count}
date_min=${manifest_date_min}
date_max=${manifest_date_max}
MANIFEST
    chmod 600 "${manifest_path}"

    umh_log_redacted "Wrote CSV:      ${csv_path} (${row_count} rows)"
    umh_log_redacted "Wrote manifest: ${manifest_path}"
    umh_log_redacted "  station_count (in csv): ${manifest_station_count}"
    umh_log_redacted "  date_min:      ${manifest_date_min:-none}"
    umh_log_redacted "  date_max:      ${manifest_date_max:-none}"

    echo ""
    echo -e "${GREEN}Export complete.${NC} Transfer BOTH files together to the deployment server:"
    echo "  scp ${csv_path} ${manifest_path} <user>@<server>:<data_root>/logs/"
    echo ""
    echo "Then on the server:"
    echo "  bash bin/initialize_runoff_period_history.sh \\"
    echo "      <env_file> --from-export <transferred_csv> --horizon ${HORIZON}"
}

main "$@"
