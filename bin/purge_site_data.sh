#!/usr/bin/env bash

# =============================================================================
# SAPPHIRE Site Data Purge
# =============================================================================
#
# Deletes operational data for a single hydropost site from a given date
# forward, across preprocessing-db and postprocessing-db Postgres containers.
# Includes backup reminder, dry-run counts, typed confirmation prompt, and
# fully transactional deletes with rollback on error.
#
# Usage:
#   bash bin/purge_site_data.sh <site_code> <from_date> [FLAGS]
#
# Arguments:
#   site_code   Numeric station code (3–10 digits)
#   from_date   Start date in YYYY-MM-DD format (rows on/after this date deleted)
#
# Flags:
#   --dry-run                  Show row counts only; do NOT delete
#   --include-hydrographs      Also delete from hydrographs table
#   --preprocessing-only       Only touch preprocessing-db
#   --postprocessing-only      Only touch postprocessing-db
#   -y, --yes                  Skip confirmation prompt
#   -h, --help                 Show this help message and exit 0
#
# Examples:
#   bash bin/purge_site_data.sh 19999 2024-01-01 --dry-run
#   bash bin/purge_site_data.sh 19999 2024-01-01
#   bash bin/purge_site_data.sh 19999 2024-01-01 --include-hydrographs -y
#   bash bin/purge_site_data.sh 19999 2024-01-01 --postprocessing-only
#
# Prerequisites:
#   - Docker daemon running
#   - Run from repository root (parent of sapphire/)
#   - Containers sapphire-preprocessing-db and sapphire-postprocessing-db running
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Colors (matching reset_sapphire_db.sh)
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PREPROCESSING_DB="sapphire-preprocessing-db"
POSTPROCESSING_DB="sapphire-postprocessing-db"

# Flags (defaults)
DRY_RUN=false
INCLUDE_HYDROGRAPHS=false
PREPROCESSING_ONLY=false
POSTPROCESSING_ONLY=false
AUTO_YES=false

# Phase tracking
PHASE_RESULTS=()

# Timing
SCRIPT_START=0

# Populated after arg parsing
SITE_CODE=""
FROM_DATE=""
FROM_YEAR=""

# Dry-run counts (set in phase_counts, used in phase_confirm and phase_verify)
CNT_RUNOFFS=0
CNT_HYDROGRAPHS=0
CNT_FORECASTS=0
CNT_LONG_FORECASTS=0
CNT_LR_FORECASTS=0
CNT_SKILL_METRICS=0
CNT_BULLETINS=0
CNT_LR_VISIBILITY=0

# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

log() {
    local level="$1"
    shift
    local msg="$*"
    local ts
    ts="$(date '+%H:%M:%S')"

    case "$level" in
        INFO)  echo -e "${BLUE}[${ts}] ${msg}${NC}" ;;
        OK)    echo -e "${GREEN}[${ts}] ${msg}${NC}" ;;
        WARN)  echo -e "${YELLOW}[${ts}] ${msg}${NC}" ;;
        ERROR) echo -e "${RED}[${ts}] ${msg}${NC}" ;;
        *)     echo "[${ts}] ${msg}" ;;
    esac
}

banner() {
    local msg="$1"
    echo ""
    echo -e "${BOLD}========================================${NC}"
    echo -e "${BOLD} ${msg}${NC}"
    echo -e "${BOLD}========================================${NC}"
}

get_timestamp() {
    date +%s
}

record_phase() {
    local phase="$1"
    local status="$2"  # PASS, FAIL, WARN, SKIP
    PHASE_RESULTS+=("${status}|${phase}")
}

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------

print_usage() {
    cat <<'USAGE'
Usage: bash bin/purge_site_data.sh <site_code> <from_date> [FLAGS]

Delete operational data for a single hydropost site from a given date forward,
across preprocessing-db and postprocessing-db Postgres containers.

Arguments:
  site_code   Numeric station code (3–10 digits)
  from_date   Start date in YYYY-MM-DD format (rows on/after this date deleted)

Flags:
  --dry-run                Show row counts only; do NOT delete anything
  --include-hydrographs    Also delete from hydrographs table (default: skip)
  --preprocessing-only     Only touch preprocessing-db
  --postprocessing-only    Only touch postprocessing-db
  -y, --yes                Skip confirmation prompt
  -h, --help               Show this help message and exit 0

Tables purged:
  preprocessing_db : runoffs  [hydrographs if --include-hydrographs]
  postprocessing_db: forecasts, long_forecasts, lr_forecasts, skill_metrics,
                     bulletins (year >= from_year), lr_visibility (year >= from_year)

Examples:
  bash bin/purge_site_data.sh 19999 2024-01-01 --dry-run
  bash bin/purge_site_data.sh 19999 2024-01-01
  bash bin/purge_site_data.sh 19999 2024-01-01 --include-hydrographs -y
  bash bin/purge_site_data.sh 19999 2024-01-01 --postprocessing-only
USAGE
}

# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------

validate_inputs() {
    # site_code: numeric, 3–10 characters, no shell metacharacters
    if [[ -z "$SITE_CODE" ]]; then
        log ERROR "Missing required argument: site_code"
        print_usage
        exit 1
    fi
    if [[ ! "$SITE_CODE" =~ ^[0-9]{3,10}$ ]]; then
        log ERROR "Invalid site_code '${SITE_CODE}': must be 3–10 decimal digits."
        exit 1
    fi

    # from_date: YYYY-MM-DD
    if [[ -z "$FROM_DATE" ]]; then
        log ERROR "Missing required argument: from_date"
        print_usage
        exit 1
    fi
    if [[ ! "$FROM_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        log ERROR "Invalid from_date '${FROM_DATE}': expected YYYY-MM-DD."
        exit 1
    fi

    # Derive from_year
    FROM_YEAR="${FROM_DATE:0:4}"

    log OK "Inputs validated: site_code=${SITE_CODE}  from_date=${FROM_DATE}  from_year=${FROM_YEAR}"
}

# ---------------------------------------------------------------------------
# Phase 0: Pre-flight
# ---------------------------------------------------------------------------

phase_preflight() {
    banner "Phase 0: Pre-flight Checks"

    # Check docker binary
    if ! command -v docker >/dev/null 2>&1; then
        log ERROR "docker binary not found on PATH. Install Docker and try again."
        record_phase "Pre-flight" "FAIL"
        exit 1
    fi
    log OK "docker binary found"

    # Check docker daemon
    if ! docker info >/dev/null 2>&1; then
        log ERROR "Docker daemon is not running. Start Docker and try again."
        record_phase "Pre-flight" "FAIL"
        exit 1
    fi
    log OK "Docker daemon is running"

    # Check required containers
    local need_pre=true
    local need_post=true

    if [ "$POSTPROCESSING_ONLY" = true ]; then
        need_pre=false
    fi
    if [ "$PREPROCESSING_ONLY" = true ]; then
        need_post=false
    fi

    if [ "$need_pre" = true ]; then
        local found_pre
        found_pre=$(docker ps \
            --filter "name=^/${PREPROCESSING_DB}$" \
            --filter "status=running" \
            --format '{{.Names}}' 2>/dev/null || true)
        if [[ -z "$found_pre" ]]; then
            log ERROR "Container '${PREPROCESSING_DB}' is not running."
            log ERROR "Start the SAPPHIRE stack: docker compose -f sapphire/docker-compose.yml up -d"
            record_phase "Pre-flight" "FAIL"
            exit 1
        fi
        log OK "Container ${PREPROCESSING_DB} is running"
    fi

    if [ "$need_post" = true ]; then
        local found_post
        found_post=$(docker ps \
            --filter "name=^/${POSTPROCESSING_DB}$" \
            --filter "status=running" \
            --format '{{.Names}}' 2>/dev/null || true)
        if [[ -z "$found_post" ]]; then
            log ERROR "Container '${POSTPROCESSING_DB}' is not running."
            log ERROR "Start the SAPPHIRE stack: docker compose -f sapphire/docker-compose.yml up -d"
            record_phase "Pre-flight" "FAIL"
            exit 1
        fi
        log OK "Container ${POSTPROCESSING_DB} is running"
    fi

    # Backup reminder (always shown)
    echo ""
    echo -e "${YELLOW}  REMINDER: Back up your databases before purging!${NC}"
    echo -e "${YELLOW}  Run:  bash bin/backup_sapphire_db.sh${NC}"
    echo ""

    record_phase "Pre-flight" "PASS"
}

# ---------------------------------------------------------------------------
# Phase 1: Dry-run counts
# ---------------------------------------------------------------------------

# Run a psql query against a named container and return tab-separated output.
# Usage: psql_query <container> <database> <sql>
psql_query() {
    local container="$1"
    local database="$2"
    local sql="$3"
    docker exec -i "$container" \
        psql -U postgres -d "$database" -t -A -F '|' -c "$sql" 2>/dev/null
}

phase_counts() {
    banner "Phase 1: Row Count Preview"

    echo ""
    printf "  %-30s %10s\n" "Table" "Row count"
    printf "  %-30s %10s\n" "------------------------------" "----------"

    # --- preprocessing-db ---
    if [ "$POSTPROCESSING_ONLY" = false ]; then
        local pre_sql
        pre_sql="SELECT 'runoffs'::text, COUNT(*) FROM runoffs WHERE code='${SITE_CODE}' AND date >= '${FROM_DATE}'
UNION ALL
SELECT 'hydrographs'::text, COUNT(*) FROM hydrographs WHERE code='${SITE_CODE}' AND date >= '${FROM_DATE}';"

        local pre_out
        pre_out=$(psql_query "$PREPROCESSING_DB" "preprocessing_db" "$pre_sql") || {
            log ERROR "Failed to query ${PREPROCESSING_DB}"
            record_phase "Row counts" "FAIL"
            return 1
        }

        while IFS='|' read -r tbl cnt; do
            [[ -z "$tbl" ]] && continue
            cnt="${cnt//[[:space:]]/}"
            case "$tbl" in
                runoffs)     CNT_RUNOFFS="$cnt" ;;
                hydrographs) CNT_HYDROGRAPHS="$cnt" ;;
            esac
        done <<< "$pre_out"

        echo -e "  ${BOLD}preprocessing_db${NC}"
        printf "    %-28s %10s\n" "runoffs" "$CNT_RUNOFFS"
        if [ "$INCLUDE_HYDROGRAPHS" = true ]; then
            printf "    %-28s %10s\n" "hydrographs" "$CNT_HYDROGRAPHS"
        else
            printf "    %-28s %10s  %s\n" "hydrographs" "$CNT_HYDROGRAPHS" "(skipped — use --include-hydrographs)"
        fi
    fi

    # --- postprocessing-db ---
    if [ "$PREPROCESSING_ONLY" = false ]; then
        local post_sql
        post_sql="SELECT 'forecasts'::text,     COUNT(*) FROM forecasts      WHERE code='${SITE_CODE}' AND date >= '${FROM_DATE}'
UNION ALL
SELECT 'long_forecasts'::text,  COUNT(*) FROM long_forecasts   WHERE code='${SITE_CODE}' AND date >= '${FROM_DATE}'
UNION ALL
SELECT 'lr_forecasts'::text,    COUNT(*) FROM lr_forecasts     WHERE code='${SITE_CODE}' AND date >= '${FROM_DATE}'
UNION ALL
SELECT 'skill_metrics'::text,   COUNT(*) FROM skill_metrics    WHERE code='${SITE_CODE}' AND date >= '${FROM_DATE}'
UNION ALL
SELECT 'bulletins'::text,       COUNT(*) FROM bulletins        WHERE code='${SITE_CODE}' AND year >= ${FROM_YEAR}
UNION ALL
SELECT 'lr_visibility'::text,   COUNT(*) FROM lr_visibility    WHERE code='${SITE_CODE}' AND year >= ${FROM_YEAR};"

        local post_out
        post_out=$(psql_query "$POSTPROCESSING_DB" "postprocessing_db" "$post_sql") || {
            log ERROR "Failed to query ${POSTPROCESSING_DB}"
            record_phase "Row counts" "FAIL"
            return 1
        }

        while IFS='|' read -r tbl cnt; do
            [[ -z "$tbl" ]] && continue
            cnt="${cnt//[[:space:]]/}"
            case "$tbl" in
                forecasts)      CNT_FORECASTS="$cnt" ;;
                long_forecasts) CNT_LONG_FORECASTS="$cnt" ;;
                lr_forecasts)   CNT_LR_FORECASTS="$cnt" ;;
                skill_metrics)  CNT_SKILL_METRICS="$cnt" ;;
                bulletins)      CNT_BULLETINS="$cnt" ;;
                lr_visibility)  CNT_LR_VISIBILITY="$cnt" ;;
            esac
        done <<< "$post_out"

        echo -e "  ${BOLD}postprocessing_db${NC}"
        printf "    %-28s %10s\n" "forecasts"      "$CNT_FORECASTS"
        printf "    %-28s %10s\n" "long_forecasts" "$CNT_LONG_FORECASTS"
        printf "    %-28s %10s\n" "lr_forecasts"   "$CNT_LR_FORECASTS"
        printf "    %-28s %10s\n" "skill_metrics"  "$CNT_SKILL_METRICS"
        printf "    %-28s %10s  %s\n" "bulletins"  "$CNT_BULLETINS"  "(year >= ${FROM_YEAR})"
        printf "    %-28s %10s  %s\n" "lr_visibility" "$CNT_LR_VISIBILITY" "(year >= ${FROM_YEAR})"
    fi

    echo ""
    record_phase "Row counts" "PASS"

    if [ "$DRY_RUN" = true ]; then
        log INFO "--dry-run: no data will be deleted. Exiting."
        exit 0
    fi
}

# ---------------------------------------------------------------------------
# Phase 2: Confirmation
# ---------------------------------------------------------------------------

phase_confirm() {
    banner "Phase 2: Confirmation"

    echo ""
    echo -e "${BOLD}  About to DELETE the following rows for site ${SITE_CODE}:${NC}"
    echo ""

    if [ "$POSTPROCESSING_ONLY" = false ]; then
        echo -e "  ${BOLD}preprocessing_db:${NC}"
        printf "    %-28s %10s rows\n" "runoffs"     "$CNT_RUNOFFS"
        if [ "$INCLUDE_HYDROGRAPHS" = true ]; then
            printf "    %-28s %10s rows\n" "hydrographs" "$CNT_HYDROGRAPHS"
        else
            printf "    %-28s %10s rows  %s\n" "hydrographs" "$CNT_HYDROGRAPHS" "(skipped)"
        fi
    fi

    if [ "$PREPROCESSING_ONLY" = false ]; then
        echo -e "  ${BOLD}postprocessing_db:${NC}"
        printf "    %-28s %10s rows\n" "forecasts"      "$CNT_FORECASTS"
        printf "    %-28s %10s rows\n" "long_forecasts" "$CNT_LONG_FORECASTS"
        printf "    %-28s %10s rows\n" "lr_forecasts"   "$CNT_LR_FORECASTS"
        printf "    %-28s %10s rows\n" "skill_metrics"  "$CNT_SKILL_METRICS"
        printf "    %-28s %10s rows  %s\n" "bulletins"     "$CNT_BULLETINS"     "(year >= ${FROM_YEAR})"
        printf "    %-28s %10s rows  %s\n" "lr_visibility" "$CNT_LR_VISIBILITY" "(year >= ${FROM_YEAR})"
    fi

    echo ""

    if [ "$AUTO_YES" = false ]; then
        echo -e "${YELLOW}  REMINDER: Back up your databases if you haven't already:${NC}"
        echo -e "${YELLOW}  Run:  bash bin/backup_sapphire_db.sh${NC}"
        echo ""
        echo -ne "${BOLD}  Type the site code to confirm: ${NC}"
        local typed
        read -r typed
        if [[ "$typed" != "$SITE_CODE" ]]; then
            log ERROR "Confirmation failed: you typed '${typed}', expected '${SITE_CODE}'. Aborting."
            record_phase "Confirmation" "FAIL"
            exit 2
        fi
        log OK "Confirmed."
    else
        log INFO "--yes: skipping confirmation prompt."
    fi

    record_phase "Confirmation" "PASS"
}

# ---------------------------------------------------------------------------
# Phase 3: Deletes (transactional)
# ---------------------------------------------------------------------------

phase_delete_preprocessing() {
    if [ "$POSTPROCESSING_ONLY" = true ]; then
        record_phase "Delete preprocessing" "SKIP"
        return 0
    fi

    banner "Phase 3a: Delete from preprocessing_db"

    local hydrograph_stmt=""
    if [ "$INCLUDE_HYDROGRAPHS" = true ]; then
        hydrograph_stmt="DELETE FROM hydrographs WHERE code = :'site' AND date >= :'from_date';"
    fi

    # Use \set + :'var' quoting — psql substitutes and quotes the literal,
    # so user input never reaches SQL as raw string concatenation.
    local sql
    sql=$(cat <<SQL
\set site '${SITE_CODE}'
\set from_date '${FROM_DATE}'
BEGIN;
DELETE FROM runoffs WHERE code = :'site' AND date >= :'from_date';
${hydrograph_stmt}
COMMIT;
SQL
)

    local output
    if output=$(echo "$sql" | docker exec -i "$PREPROCESSING_DB" \
            psql -U postgres -d preprocessing_db \
            -v ON_ERROR_STOP=1 -X 2>&1); then
        # psql prints "DELETE N" for each statement
        while IFS= read -r line; do
            case "$line" in
                DELETE*)
                    log OK "  preprocessing_db: ${line}"
                    ;;
            esac
        done <<< "$output"
        record_phase "Delete preprocessing" "PASS"
    else
        log ERROR "Transaction failed on preprocessing_db:"
        echo "$output" >&2
        record_phase "Delete preprocessing" "FAIL"
        return 1
    fi
}

phase_delete_postprocessing() {
    if [ "$PREPROCESSING_ONLY" = true ]; then
        record_phase "Delete postprocessing" "SKIP"
        return 0
    fi

    banner "Phase 3b: Delete from postprocessing_db"

    local sql
    sql=$(cat <<SQL
\set site '${SITE_CODE}'
\set from_date '${FROM_DATE}'
\set from_year ${FROM_YEAR}
BEGIN;
DELETE FROM forecasts      WHERE code = :'site' AND date >= :'from_date';
DELETE FROM long_forecasts WHERE code = :'site' AND date >= :'from_date';
DELETE FROM lr_forecasts   WHERE code = :'site' AND date >= :'from_date';
DELETE FROM skill_metrics  WHERE code = :'site' AND date >= :'from_date';
DELETE FROM bulletins      WHERE code = :'site' AND year >= :from_year;
DELETE FROM lr_visibility  WHERE code = :'site' AND year >= :from_year;
COMMIT;
SQL
)

    local output
    if output=$(echo "$sql" | docker exec -i "$POSTPROCESSING_DB" \
            psql -U postgres -d postprocessing_db \
            -v ON_ERROR_STOP=1 -X 2>&1); then
        while IFS= read -r line; do
            case "$line" in
                DELETE*)
                    log OK "  postprocessing_db: ${line}"
                    ;;
            esac
        done <<< "$output"
        record_phase "Delete postprocessing" "PASS"
    else
        log ERROR "Transaction failed on postprocessing_db:"
        echo "$output" >&2
        record_phase "Delete postprocessing" "FAIL"
        return 1
    fi
}

# ---------------------------------------------------------------------------
# Phase 4: Verification
# ---------------------------------------------------------------------------

phase_verify() {
    banner "Phase 4: Verification"

    local any_failed=false

    # Re-run count queries and check all targeted tables are at zero.
    if [ "$POSTPROCESSING_ONLY" = false ]; then
        local pre_sql
        pre_sql="SELECT 'runoffs'::text, COUNT(*) FROM runoffs WHERE code='${SITE_CODE}' AND date >= '${FROM_DATE}';"
        if [ "$INCLUDE_HYDROGRAPHS" = true ]; then
            pre_sql="SELECT 'runoffs'::text, COUNT(*) FROM runoffs WHERE code='${SITE_CODE}' AND date >= '${FROM_DATE}'
UNION ALL
SELECT 'hydrographs'::text, COUNT(*) FROM hydrographs WHERE code='${SITE_CODE}' AND date >= '${FROM_DATE}';"
        fi

        local pre_out
        pre_out=$(psql_query "$PREPROCESSING_DB" "preprocessing_db" "$pre_sql") || {
            log ERROR "Verification query failed on ${PREPROCESSING_DB}"
            any_failed=true
        }

        if [[ -n "$pre_out" ]]; then
            while IFS='|' read -r tbl cnt; do
                [[ -z "$tbl" ]] && continue
                cnt="${cnt//[[:space:]]/}"
                if [[ "$cnt" != "0" ]]; then
                    log ERROR "  preprocessing_db.${tbl}: ${cnt} rows remain (expected 0)"
                    any_failed=true
                else
                    log OK "  preprocessing_db.${tbl}: 0 rows remain"
                fi
            done <<< "$pre_out"
        fi
    fi

    if [ "$PREPROCESSING_ONLY" = false ]; then
        local post_sql
        post_sql="SELECT 'forecasts'::text,     COUNT(*) FROM forecasts      WHERE code='${SITE_CODE}' AND date >= '${FROM_DATE}'
UNION ALL
SELECT 'long_forecasts'::text,  COUNT(*) FROM long_forecasts   WHERE code='${SITE_CODE}' AND date >= '${FROM_DATE}'
UNION ALL
SELECT 'lr_forecasts'::text,    COUNT(*) FROM lr_forecasts     WHERE code='${SITE_CODE}' AND date >= '${FROM_DATE}'
UNION ALL
SELECT 'skill_metrics'::text,   COUNT(*) FROM skill_metrics    WHERE code='${SITE_CODE}' AND date >= '${FROM_DATE}'
UNION ALL
SELECT 'bulletins'::text,       COUNT(*) FROM bulletins        WHERE code='${SITE_CODE}' AND year >= ${FROM_YEAR}
UNION ALL
SELECT 'lr_visibility'::text,   COUNT(*) FROM lr_visibility    WHERE code='${SITE_CODE}' AND year >= ${FROM_YEAR};"

        local post_out
        post_out=$(psql_query "$POSTPROCESSING_DB" "postprocessing_db" "$post_sql") || {
            log ERROR "Verification query failed on ${POSTPROCESSING_DB}"
            any_failed=true
        }

        if [[ -n "$post_out" ]]; then
            while IFS='|' read -r tbl cnt; do
                [[ -z "$tbl" ]] && continue
                cnt="${cnt//[[:space:]]/}"
                if [[ "$cnt" != "0" ]]; then
                    log ERROR "  postprocessing_db.${tbl}: ${cnt} rows remain (expected 0)"
                    any_failed=true
                else
                    log OK "  postprocessing_db.${tbl}: 0 rows remain"
                fi
            done <<< "$post_out"
        fi
    fi

    if [ "$any_failed" = true ]; then
        record_phase "Verification" "FAIL"
        return 1
    fi

    record_phase "Verification" "PASS"
}

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

print_summary() {
    banner "PURGE SUMMARY"
    echo ""

    for entry in "${PHASE_RESULTS[@]}"; do
        local status="${entry%%|*}"
        local phase="${entry#*|}"
        case "$status" in
            PASS) echo -e "  ${GREEN}PASS${NC}  ${phase}" ;;
            FAIL) echo -e "  ${RED}FAIL${NC}  ${phase}" ;;
            WARN) echo -e "  ${YELLOW}WARN${NC}  ${phase}" ;;
            SKIP) echo -e "  ${BLUE}SKIP${NC}  ${phase}" ;;
        esac
    done

    echo ""

    for entry in "${PHASE_RESULTS[@]}"; do
        local status="${entry%%|*}"
        if [ "$status" = "FAIL" ]; then
            log ERROR "One or more phases failed."
            return 1
        fi
    done

    echo -e "${GREEN}Site ${SITE_CODE} purged successfully.${NC}"
    echo ""
    echo "Re-run the pipeline to regenerate data:"
    echo "  bash apps/run_locally.sh maintenance"
    echo "  bash apps/run_locally.sh recalculate_skill_metrics"
    echo ""
    return 0
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
    # --- Consume positional args before flags ---
    if [[ $# -gt 0 && "$1" != -* ]]; then
        SITE_CODE="$1"
        shift
    fi
    if [[ $# -gt 0 && "$1" != -* ]]; then
        FROM_DATE="$1"
        shift
    fi

    # Parse flags
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --dry-run)               DRY_RUN=true ;;
            --include-hydrographs)   INCLUDE_HYDROGRAPHS=true ;;
            --preprocessing-only)    PREPROCESSING_ONLY=true ;;
            --postprocessing-only)   POSTPROCESSING_ONLY=true ;;
            -y|--yes)                AUTO_YES=true ;;
            -h|--help)               print_usage; exit 0 ;;
            *)
                log ERROR "Unknown argument: $1"
                print_usage
                exit 1
                ;;
        esac
        shift
    done

    # Mutually exclusive scope flags
    if [ "$PREPROCESSING_ONLY" = true ] && [ "$POSTPROCESSING_ONLY" = true ]; then
        log ERROR "--preprocessing-only and --postprocessing-only are mutually exclusive."
        exit 1
    fi

    # shellcheck disable=SC2034  # reserved for future elapsed-time reporting; not yet consumed
    SCRIPT_START=$(get_timestamp)

    banner "SAPPHIRE Site Data Purge"

    validate_inputs

    # Phase 0: Pre-flight
    phase_preflight || exit 1

    # Phase 1: Dry-run counts (also acts as early exit when --dry-run is set)
    phase_counts || exit 1

    # Phase 2: Confirmation
    phase_confirm || exit 1

    # Phase 3: Transactional deletes
    phase_delete_preprocessing || exit 1
    phase_delete_postprocessing || exit 1

    # Phase 4: Verification
    phase_verify || {
        log ERROR "Verification failed — some rows were not deleted."
        print_summary
        exit 1
    }

    # Summary
    print_summary
    exit $?
}

main "$@"
