#!/usr/bin/env bash
# ============================================================================
# export_ml_forecast_history.sh
#
# Laptop-side export wrapper for the ML forecast migration toolkit (P4b).
# Pulls historical ML forecast rows from a laptop's
# ``sapphire-postprocessing-db`` table ``forecasts``, filtered to the three
# ML model variants (``model_type IN ('TFT','TiDE','TSMixer')``), and writes:
#
#   1. ``<out_dir>/ml_forecast_<timestamp>.csv`` — header + rows
#   2. ``<out_dir>/ml_forecast_<timestamp>.csv.manifest`` — sidecar with the
#      5 required keys validated by ``migration_py._common.validate_manifest``:
#      ``export_type=ml_forecast``, ``row_count``, ``station_count``,
#      ``date_min``, ``date_max``. Adds optional ``model_counts`` metadata.
#
# Location guard (Stage E item #6):
#   This script REFUSES TO RUN on a host where ``sapphire-postprocessing-db``
#   is currently running. The export pattern is intended for laptops only —
#   if the laptop and deployment server happen to be the same host, the
#   operator should use the in-place server wrapper instead. Override only
#   with ``--i-am-on-laptop`` if you know what you are doing.
#
# Default horizon filter (user-lock L6):
#   The export defaults to ``horizon_type='day'`` rows only — the canonical
#   modern storage shape. To ALSO grab pre-1cb3495 PENTAD/DECADE rows for
#   migration with ``--preserve-legacy-ml-horizons`` on the server side,
#   pass ``--include-legacy-horizons``. The exported CSV preserves the
#   source ``horizon_type`` cell so the server wrapper can filter as needed.
#
# Enum case (Stage A §E):
#   The exported ``model_type`` column uses MIXED-CASE values exactly as
#   stored in PostgreSQL: ``TFT``, ``TiDE``, ``TSMixer``. Downstream
#   ``migration_py.ml_forecast.MODEL_DIR_TO_API`` accepts both forms.
#
# Usage:
#   bash bin/export_ml_forecast_history.sh <out_dir> [OPTIONS]
#
# Arguments:
#   out_dir              Destination directory for the CSV + manifest pair.
#                        Must be writable; permissions tightened to 0o700
#                        before the COPY runs.
#
# Options:
#   --include-legacy-horizons
#                        ALSO export rows with horizon_type IN ('pentad','decade').
#                        Default exports only horizon_type='day' rows.
#   --station-filter <CODE>
#                        Filter to a single station code (binding P0 interface).
#   --model TFT|TiDE|TSMixer
#                        Restrict to a single ML model variant.
#   --start-date <ISO>   Lower bound (date >= start_date).
#   --end-date <ISO>     Upper bound (date < end_date; strict).
#   --db-host <HOST>     PostgreSQL host (default: localhost).
#   --db-port <PORT>     PostgreSQL port (default: 5432).
#   --db-user <USER>     PostgreSQL user (default: postgres).
#   --db-name <NAME>     Database name (default: postgres).
#   --i-am-on-laptop     BYPASS the location guard (use only when laptop and
#                        server are intentionally co-located on the same host).
#   --dry-run            Run COUNT(*) query only; do NOT export CSV or manifest.
#                        Matches the shared toolkit contract (runbook §6 lists
#                        --dry-run as a per-wrapper requirement).
#   -h, --help           Print this message and exit 0.
#
# Examples:
#   bash bin/export_ml_forecast_history.sh /tmp/ml_export --station-filter 19999
#   bash bin/export_ml_forecast_history.sh /tmp/ml_export --include-legacy-horizons
#
# Source-side credentials must be in your environment OR in ~/.pgpass:
#   export PGPASSWORD=<laptop_db_password>  # or use ~/.pgpass
#
# ============================================================================

set -eo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

# Defaults
OUT_DIR=""
INCLUDE_LEGACY_HORIZONS=false
STATION_FILTER=""
MODEL_FILTER=""
START_DATE=""
END_DATE=""
DB_HOST="localhost"
DB_PORT="5432"
DB_USER="postgres"
DB_NAME="postgres"
I_AM_ON_LAPTOP=false
DRY_RUN=false

print_usage() {
    cat <<'USAGE'
Usage: bash bin/export_ml_forecast_history.sh <out_dir> [OPTIONS]

Export ML forecast history rows (TFT, TiDE, TSMixer) from the laptop's
sapphire-postprocessing-db.forecasts table to a CSV + manifest pair.

Arguments:
  out_dir              Destination directory for the CSV + manifest pair.

Options:
  --include-legacy-horizons
                       ALSO export rows with horizon_type IN ('pentad','decade').
                       Default exports only horizon_type='day' rows.
  --station-filter <CODE>
                       Filter to a single station code (binding P0 interface).
  --model TFT|TiDE|TSMixer
                       Restrict to a single ML model variant.
  --start-date <ISO>   Lower bound (date >= start_date).
  --end-date <ISO>     Upper bound (date < end_date; strict).
  --db-host <HOST>     PostgreSQL host (default: localhost).
  --db-port <PORT>     PostgreSQL port (default: 5432).
  --db-user <USER>     PostgreSQL user (default: postgres).
  --db-name <NAME>     Database name (default: postgres).
  --i-am-on-laptop     BYPASS the location guard.
  --dry-run            Run COUNT(*) query only; do NOT export CSV or manifest.
                       Matches the shared toolkit dry-run contract.
  -h, --help           Print this message and exit 0.

Location guard:
  This script refuses to run when sapphire-postprocessing-db is running on
  this host. Override with --i-am-on-laptop.
USAGE
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

parse_args() {
    for arg in "$@"; do
        case "$arg" in
            -h|--help) print_usage; exit 0 ;;
        esac
    done

    if [[ $# -eq 0 ]]; then
        echo -e "${RED}Error: out_dir is required.${NC}" >&2
        echo "" >&2
        print_usage >&2
        exit 1
    fi

    if [[ "$1" != -* ]]; then
        OUT_DIR="$1"
        shift
    else
        echo -e "${RED}Error: first argument must be the out_dir (got: $1).${NC}" >&2
        echo "" >&2
        print_usage >&2
        exit 1
    fi

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --include-legacy-horizons)
                INCLUDE_LEGACY_HORIZONS=true
                shift
                ;;
            --station-filter)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --station-filter requires a value.${NC}" >&2
                    exit 1
                fi
                STATION_FILTER="$2"
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
            --start-date)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --start-date requires a value.${NC}" >&2
                    exit 1
                fi
                START_DATE="$2"
                shift 2
                ;;
            --end-date)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --end-date requires a value.${NC}" >&2
                    exit 1
                fi
                END_DATE="$2"
                shift 2
                ;;
            --db-host)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --db-host requires a value.${NC}" >&2
                    exit 1
                fi
                DB_HOST="$2"
                shift 2
                ;;
            --db-port)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --db-port requires a value.${NC}" >&2
                    exit 1
                fi
                DB_PORT="$2"
                shift 2
                ;;
            --db-user)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --db-user requires a value.${NC}" >&2
                    exit 1
                fi
                DB_USER="$2"
                shift 2
                ;;
            --db-name)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --db-name requires a value.${NC}" >&2
                    exit 1
                fi
                DB_NAME="$2"
                shift 2
                ;;
            --i-am-on-laptop)
                I_AM_ON_LAPTOP=true
                shift
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
}

# ---------------------------------------------------------------------------
# Validate --model against the known mixed-case enum values.
# ---------------------------------------------------------------------------
validate_model_filter() {
    if [[ -z "$MODEL_FILTER" ]]; then
        return 0
    fi
    case "$MODEL_FILTER" in
        TFT|TIDE|TiDE|TSMIXER|TSMixer)
            ;;
        *)
            echo -e "${RED}Error: --model ${MODEL_FILTER@Q} not recognized. Expected one of: TFT, TIDE, TiDE, TSMIXER, TSMixer.${NC}" >&2
            exit 1
            ;;
    esac
}

# ---------------------------------------------------------------------------
# Normalize the model filter to the canonical API value (mixed case).
# Echoes the normalized form on stdout.
# ---------------------------------------------------------------------------
normalize_model_filter() {
    case "$1" in
        TFT) echo "TFT" ;;
        TIDE|TiDE) echo "TiDE" ;;
        TSMIXER|TSMixer) echo "TSMixer" ;;
        *) echo "" ;;
    esac
}

# ---------------------------------------------------------------------------
# Build the WHERE clause for the COPY query.
# Echoes the clause on stdout.
# ---------------------------------------------------------------------------
build_where_clause() {
    local where="model_type IN ('TFT','TiDE','TSMixer')"

    if [[ "$INCLUDE_LEGACY_HORIZONS" == true ]]; then
        # Accept both 'day' (modern) AND 'pentad'/'decade' (legacy).
        where+=" AND horizon_type IN ('day','pentad','decade')"
    else
        where+=" AND horizon_type = 'day'"
    fi

    if [[ -n "$STATION_FILTER" ]]; then
        where+=" AND code = '${STATION_FILTER//\'/\'\'}'"
    fi
    if [[ -n "$MODEL_FILTER" ]]; then
        local m
        m="$(normalize_model_filter "$MODEL_FILTER")"
        where+=" AND model_type = '${m}'"
    fi
    if [[ -n "$START_DATE" ]]; then
        where+=" AND date >= '${START_DATE//\'/\'\'}'"
    fi
    if [[ -n "$END_DATE" ]]; then
        where+=" AND date < '${END_DATE//\'/\'\'}'"
    fi
    echo "$where"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
    parse_args "$@"
    validate_model_filter

    echo -e "${BOLD}========================================"
    echo " ML Forecast Laptop Export (P4b)"
    echo -e "========================================${NC}"

    # Location guard.
    if [[ "$I_AM_ON_LAPTOP" != true ]]; then
        if command -v docker >/dev/null 2>&1; then
            if docker ps --filter "name=sapphire-postprocessing-db" --quiet 2>/dev/null | grep -q .; then
                echo -e "${RED}ERROR: sapphire-postprocessing-db is running on this host.${NC}" >&2
                echo "This script is for laptop-side export only." >&2
                echo "If you really mean to export from a server, pass --i-am-on-laptop." >&2
                exit 1
            fi
        fi
    else
        echo -e "${YELLOW}WARNING: running on apparent deployment host — bypass enabled by operator (--i-am-on-laptop).${NC}" >&2
    fi

    if [[ -z "$OUT_DIR" ]]; then
        echo -e "${RED}Error: out_dir is required.${NC}" >&2
        exit 1
    fi

    mkdir -p "$OUT_DIR"
    chmod 700 "$OUT_DIR"
    umask 077

    local ts
    ts="$(date -u +%Y%m%dT%H%M%SZ)"
    local csv_path="${OUT_DIR}/ml_forecast_${ts}.csv"
    local manifest_path="${csv_path}.manifest"

    local where
    where="$(build_where_clause)"

    echo "  out_dir:                ${OUT_DIR}"
    echo "  csv_path:               ${csv_path}"
    echo "  manifest_path:          ${manifest_path}"
    echo "  db:                     ${DB_USER}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
    echo "  include_legacy_horizons: ${INCLUDE_LEGACY_HORIZONS}"
    echo "  station_filter:         ${STATION_FILTER:-<none>}"
    echo "  model_filter:           ${MODEL_FILTER:-<none>}"
    echo "  start_date:             ${START_DATE:-<none>}"
    echo "  end_date:               ${END_DATE:-<none>}"
    echo "  dry_run:                ${DRY_RUN}"
    echo ""

    # Dry-run path: COUNT(*) only, no CSV / manifest written
    # (review feedback round 2: shared toolkit dry-run contract).
    if [[ "$DRY_RUN" == true ]]; then
        echo -e "${YELLOW}DRY RUN: running COUNT(*) only; no CSV / manifest will be written.${NC}"
        local count_sql
        count_sql="SELECT COUNT(*) AS row_count,
                          COUNT(DISTINCT code) AS station_count,
                          MIN(date)::text AS date_min,
                          MAX(date)::text AS date_max
                   FROM forecasts
                   WHERE ${where};"
        if ! psql -X -P pager=off -A -F $'\t' \
            -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
            -c "$count_sql"; then
            echo -e "${RED}ERROR: COUNT query failed. Check connection and PGPASSWORD / ~/.pgpass.${NC}" >&2
            exit 1
        fi
        echo ""
        echo -e "${YELLOW}DRY RUN complete — no files written.${NC}"
        exit 0
    fi

    # COPY the data with header. The exported columns mirror what the
    # server-side migration_py.ml_forecast module expects.
    local copy_sql
    copy_sql="COPY (
        SELECT
            code,
            model_type,
            horizon_type,
            date,
            target,
            flag,
            q05 AS \"Q5\",
            q25 AS \"Q25\",
            forecasted_discharge AS \"Q50\",
            q75 AS \"Q75\",
            q95 AS \"Q95\",
            forecasted_discharge
        FROM forecasts
        WHERE ${where}
        ORDER BY code, model_type, date, target
    ) TO STDOUT WITH CSV HEADER"

    echo "Running COPY ..."
    if ! psql -X \
        -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
        -c "$copy_sql" > "$csv_path"; then
        echo -e "${RED}ERROR: COPY failed. Check connection and PGPASSWORD / ~/.pgpass.${NC}" >&2
        rm -f "$csv_path"
        exit 1
    fi
    chmod 600 "$csv_path"

    # Compute manifest summary stats inline (stdlib python).
    local summary_out
    if ! summary_out="$(python3 - "$csv_path" <<'PYEOF'
import csv
import sys

csv_path = sys.argv[1]
n = 0
codes = set()
date_min = None
date_max = None
model_counts = {}
with open(csv_path, newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        n += 1
        c = (row.get("code") or "").strip()
        if c:
            codes.add(c)
        d = (row.get("date") or "").strip()[:10]
        if d:
            if date_min is None or d < date_min:
                date_min = d
            if date_max is None or d > date_max:
                date_max = d
        m = (row.get("model_type") or "").strip()
        if m:
            model_counts[m] = model_counts.get(m, 0) + 1

# Emit on stdout as key=value (one per line) for shell consumption.
print(f"row_count={n}")
print(f"station_count={len(codes)}")
print(f"date_min={date_min or ''}")
print(f"date_max={date_max or ''}")
for m, c in sorted(model_counts.items()):
    print(f"model_count_{m}={c}")
PYEOF
    )"; then
        echo -e "${RED}ERROR: failed to compute manifest summary stats.${NC}" >&2
        rm -f "$csv_path"
        exit 1
    fi

    # Reject zero-row exports up front (review feedback): a blank
    # date_min/date_max produces a manifest the server-side validator rejects
    # as non-ISO. Surface this as a clear operator error before any sidecar
    # is written. The CSV header file is removed too so re-runs start clean.
    local row_count
    row_count="$(echo "$summary_out" | grep '^row_count=' | cut -d= -f2-)"
    if [[ "$row_count" == "0" ]]; then
        echo -e "${RED}ERROR: no rows matched filter — nothing to export.${NC}" >&2
        echo "Check the station list, horizon filter (--include-legacy-horizons), and date range." >&2
        rm -f "$csv_path"
        exit 1
    fi

    # Write the manifest.
    {
        echo "# ML forecast export manifest"
        echo "# Generated by bin/export_ml_forecast_history.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "export_type=ml_forecast"
        echo "$summary_out"
        echo "include_legacy_horizons=${INCLUDE_LEGACY_HORIZONS}"
        if [[ -n "$STATION_FILTER" ]]; then
            echo "station_filter=${STATION_FILTER}"
        fi
        if [[ -n "$MODEL_FILTER" ]]; then
            echo "model_filter=${MODEL_FILTER}"
        fi
    } > "$manifest_path"
    chmod 600 "$manifest_path"

    echo ""
    echo -e "${GREEN}Export complete.${NC}"
    echo "  CSV:      ${csv_path}"
    echo "  Manifest: ${manifest_path}"
    echo ""
    echo "Next steps:"
    echo "  1. Transfer both files to the deployment server (e.g. scp)."
    echo "  2. Run the server wrapper:"
    echo "       bash bin/initialize_ml_forecast_history.sh <env_file> \\"
    echo "           --from-export <transferred_csv_path>"
    echo ""
    if [[ "$INCLUDE_LEGACY_HORIZONS" == true ]]; then
        echo -e "${YELLOW}NOTE: --include-legacy-horizons was used. Pass --preserve-legacy-ml-horizons"
        echo -e "to the server wrapper to honor the source horizon_type cells.${NC}"
    fi
}

main "$@"
