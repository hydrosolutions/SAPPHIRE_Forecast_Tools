#!/usr/bin/env bash

# =============================================================================
# SAPPHIRE Database Reset & Migration
# =============================================================================
#
# Orchestrates a full DB reset cycle: stop services -> destroy volumes ->
# rebuild images -> start services -> wait for health -> run data migration.
#
# SAPPHIRE services manage schema with Alembic migrations at container startup.
# This script is a conservative full reset for long gaps, uncertain migration
# state, or when you want to recreate volumes and re-run the CSV data import.
#
# Usage:
#   bash bin/reset_sapphire_db.sh                       # Full reset (both DBs)
#   bash bin/reset_sapphire_db.sh --postprocessing-only # Postprocessing DB only
#   bash bin/reset_sapphire_db.sh --preprocessing-only  # Preprocessing DB only
#   bash bin/reset_sapphire_db.sh --skip-migration      # Reset DB, skip import
#   bash bin/reset_sapphire_db.sh --skip-rebuild        # Skip docker build
#   bash bin/reset_sapphire_db.sh -y                    # Skip confirmation
#
# Prerequisites:
#   - Docker daemon running
#   - Run from repository root (parent of sapphire/)
#   - docker-compose.yml has correct bind-mount paths for CSV data
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Colors (matching apps/run_docker_tests.sh)
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
COMPOSE_DIR="sapphire"
COMPOSE_FILE="${COMPOSE_DIR}/docker-compose.yml"
HEALTH_URL="http://localhost:8000/health/ready"
HEALTH_TIMEOUT=120  # seconds

# Container names
PREPROCESSING_API="sapphire-preprocessing-api"
POSTPROCESSING_API="sapphire-postprocessing-api"

# Volume names (docker compose prefixes with project directory name)
PREPROCESSING_VOL="sapphire_preprocessing-data"
POSTPROCESSING_VOL="sapphire_postprocessing-data"

# Flags
PREPROCESSING_ONLY=false
POSTPROCESSING_ONLY=false
SKIP_MIGRATION=false
SKIP_REBUILD=false
AUTO_YES=false

# Phase tracking
PHASE_RESULTS=()

# Timing
SCRIPT_START=0

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

format_duration() {
    local seconds=$1
    if (( seconds >= 3600 )); then
        printf "%dh %dm %ds" $((seconds/3600)) $((seconds%3600/60)) $((seconds%60))
    elif (( seconds >= 60 )); then
        printf "%dm %ds" $((seconds/60)) $((seconds%60))
    else
        printf "%ds" "$seconds"
    fi
}

record_phase() {
    local phase="$1"
    local status="$2"  # PASS, FAIL, WARN, SKIP
    PHASE_RESULTS+=("${status}|${phase}")
}

# ---------------------------------------------------------------------------
# Precondition checks
# ---------------------------------------------------------------------------

check_docker() {
    if ! docker info >/dev/null 2>&1; then
        log ERROR "Docker daemon is not running. Start Docker and try again."
        exit 1
    fi
    log OK "Docker daemon is running"
}

check_repo_root() {
    if [ ! -f "${COMPOSE_FILE}" ]; then
        log ERROR "Must run from the repository root (parent of sapphire/)."
        log ERROR "  Expected: ${COMPOSE_FILE}"
        exit 1
    fi
    log OK "Repository root detected"
}

# ---------------------------------------------------------------------------
# Phase 1: Stop services
# ---------------------------------------------------------------------------

phase_stop() {
    banner "Phase 1: Stop Services"

    log INFO "Stopping all SAPPHIRE services..."
    if docker compose -f "${COMPOSE_FILE}" down 2>&1 | while IFS= read -r line; do
        log INFO "  ${line}"
    done; then
        log OK "Services stopped"
        record_phase "Stop services" "PASS"
    else
        log ERROR "Failed to stop services"
        record_phase "Stop services" "FAIL"
        return 1
    fi
}

# ---------------------------------------------------------------------------
# Phase 2: Remove volumes
# ---------------------------------------------------------------------------

phase_remove_volumes() {
    banner "Phase 2: Remove Database Volumes"

    local volumes=()
    if [ "$POSTPROCESSING_ONLY" = false ]; then
        volumes+=("${PREPROCESSING_VOL}")
    fi
    if [ "$PREPROCESSING_ONLY" = false ]; then
        volumes+=("${POSTPROCESSING_VOL}")
    fi

    for vol in "${volumes[@]}"; do
        if docker volume inspect "$vol" >/dev/null 2>&1; then
            log INFO "Removing volume: ${vol}"
            if docker volume rm "$vol" >/dev/null 2>&1; then
                log OK "Removed: ${vol}"
            else
                log ERROR "Failed to remove volume: ${vol}"
                record_phase "Remove volumes" "FAIL"
                return 1
            fi
        else
            log WARN "Volume not found (already removed?): ${vol}"
        fi
    done

    record_phase "Remove volumes" "PASS"
}

# ---------------------------------------------------------------------------
# Phase 3: Rebuild images
# ---------------------------------------------------------------------------

phase_rebuild() {
    if [ "$SKIP_REBUILD" = true ]; then
        log INFO "Skipping rebuild (--skip-rebuild)"
        record_phase "Rebuild images" "SKIP"
        return 0
    fi

    banner "Phase 3: Rebuild Images"

    local services=()
    if [ "$POSTPROCESSING_ONLY" = false ]; then
        services+=("preprocessing-api")
    fi
    if [ "$PREPROCESSING_ONLY" = false ]; then
        services+=("postprocessing-api")
    fi
    # Always rebuild api-gateway (routes to both services)
    services+=("api-gateway")

    log INFO "Rebuilding: ${services[*]}"
    local start elapsed
    start=$(get_timestamp)

    if docker compose -f "${COMPOSE_FILE}" build --no-cache "${services[@]}" 2>&1 | \
        while IFS= read -r line; do
            # Show only key lines to avoid flooding output
            case "$line" in
                *"Step"*|*"Successfully"*|*"#"*"DONE"*|*"ERROR"*)
                    log INFO "  ${line}"
                    ;;
            esac
        done; then
        elapsed=$(( $(get_timestamp) - start ))
        log OK "Rebuild completed in $(format_duration $elapsed)"
        record_phase "Rebuild images" "PASS"
    else
        elapsed=$(( $(get_timestamp) - start ))
        log ERROR "Rebuild failed after $(format_duration $elapsed)"
        record_phase "Rebuild images" "FAIL"
        return 1
    fi
}

# ---------------------------------------------------------------------------
# Phase 4: Start services
# ---------------------------------------------------------------------------

phase_start() {
    banner "Phase 4: Start Services"

    log INFO "Starting services..."
    if docker compose -f "${COMPOSE_FILE}" up -d 2>&1 | while IFS= read -r line; do
        log INFO "  ${line}"
    done; then
        log OK "Services started"
        record_phase "Start services" "PASS"
    else
        log ERROR "Failed to start services"
        record_phase "Start services" "FAIL"
        return 1
    fi
}

# ---------------------------------------------------------------------------
# Phase 5: Health check
# ---------------------------------------------------------------------------

phase_health() {
    banner "Phase 5: Health Check"

    log INFO "Waiting for ${HEALTH_URL} (timeout: ${HEALTH_TIMEOUT}s)..."

    local start elapsed
    start=$(get_timestamp)

    while true; do
        elapsed=$(( $(get_timestamp) - start ))
        if (( elapsed >= HEALTH_TIMEOUT )); then
            log ERROR "Health check timed out after ${HEALTH_TIMEOUT}s"
            log ERROR "Check logs: docker compose -f ${COMPOSE_FILE} logs"
            record_phase "Health check" "FAIL"
            return 1
        fi

        if curl -sf "${HEALTH_URL}" >/dev/null 2>&1; then
            log OK "Services healthy after $(format_duration $elapsed)"
            record_phase "Health check" "PASS"
            return 0
        fi

        # Show progress every 10 seconds
        if (( elapsed % 10 == 0 && elapsed > 0 )); then
            log INFO "  Still waiting... (${elapsed}s / ${HEALTH_TIMEOUT}s)"
        fi
        sleep 2
    done
}

# ---------------------------------------------------------------------------
# Phase 6 & 7: Data migration
# ---------------------------------------------------------------------------

run_migration() {
    local container="$1"
    local description="$2"
    shift 2
    local commands=("$@")

    log INFO "Running ${description} migration (container: ${container})..."

    # Check container is running
    if ! docker ps --format '{{.Names}}' | grep -qx "$container"; then
        log ERROR "Container '${container}' is not running"
        return 1
    fi

    local any_failed=false
    for cmd in "${commands[@]}"; do
        log INFO "  => ${cmd}"
        if docker exec -i "$container" bash -lc "$cmd" 2>&1 | while IFS= read -r line; do
            echo -e "     ${line}"
        done; then
            log OK "  Done: ${cmd}"
        else
            log WARN "  FAILED: ${cmd}"
            any_failed=true
        fi
    done

    if [ "$any_failed" = true ]; then
        return 1
    fi
    return 0
}

phase_preprocessing_migration() {
    if [ "$SKIP_MIGRATION" = true ]; then
        log INFO "Skipping preprocessing migration (--skip-migration)"
        record_phase "Preprocessing migration" "SKIP"
        return 0
    fi
    if [ "$POSTPROCESSING_ONLY" = true ]; then
        record_phase "Preprocessing migration" "SKIP"
        return 0
    fi

    banner "Phase 6: Preprocessing Migration"

    local commands=(
        "python -u app/data_migrator.py --type runoff"
        "python -u app/data_migrator.py --type hydrograph"
        "python -u app/data_migrator.py --type meteo"
        "python -u app/data_migrator.py --type snow"
    )

    if run_migration "$PREPROCESSING_API" "preprocessing" "${commands[@]}"; then
        record_phase "Preprocessing migration" "PASS"
    else
        log WARN "Some preprocessing migrations failed (continuing)"
        record_phase "Preprocessing migration" "WARN"
    fi
}

phase_postprocessing_migration() {
    if [ "$SKIP_MIGRATION" = true ]; then
        log INFO "Skipping postprocessing migration (--skip-migration)"
        record_phase "Postprocessing migration" "SKIP"
        return 0
    fi
    if [ "$PREPROCESSING_ONLY" = true ]; then
        record_phase "Postprocessing migration" "SKIP"
        return 0
    fi

    banner "Phase 7: Postprocessing Migration"

    local commands=(
        "python -u app/data_migrator.py --type skillmetric --batch-size 1"
        "python -u app/data_migrator.py --type lrforecast"
        "python -u app/data_migrator.py --type combinedforecast"
        "python -u app/data_migrator.py --type forecast"
        "python -u app/data_migrator.py --type longforecast"
    )

    if run_migration "$POSTPROCESSING_API" "postprocessing" "${commands[@]}"; then
        record_phase "Postprocessing migration" "PASS"
    else
        log WARN "Some postprocessing migrations failed (continuing)"
        record_phase "Postprocessing migration" "WARN"
    fi
}

# ---------------------------------------------------------------------------
# Phase 8: Verify
# ---------------------------------------------------------------------------

phase_verify() {
    banner "Phase 8: Verify"

    local any_failed=false

    # Health check
    log INFO "Verifying health endpoint..."
    if curl -sf "${HEALTH_URL}" >/dev/null 2>&1; then
        log OK "Health check: OK"
    else
        log ERROR "Health check: FAILED"
        any_failed=true
    fi

    # Spot-check API responses
    if [ "$PREPROCESSING_ONLY" != true ]; then
        log INFO "Spot-checking postprocessing API..."
        local response
        response=$(curl -sf "http://localhost:8000/api/postprocessing/skill-metric/?limit=1" 2>&1) || true
        if [ -n "$response" ]; then
            log OK "Postprocessing API responding"
            # Check if response looks like valid JSON array
            if echo "$response" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null; then
                log OK "Postprocessing API returns valid JSON"
            else
                log WARN "Postprocessing API response is not valid JSON"
            fi
        else
            log WARN "Postprocessing API returned empty response (may be expected for fresh DB)"
        fi
    fi

    if [ "$POSTPROCESSING_ONLY" != true ]; then
        log INFO "Spot-checking preprocessing API..."
        local response
        response=$(curl -sf "http://localhost:8000/api/preprocessing/runoff/?limit=1" 2>&1) || true
        if [ -n "$response" ]; then
            log OK "Preprocessing API responding"
        else
            log WARN "Preprocessing API returned empty response (may be expected for fresh DB)"
        fi
    fi

    if [ "$any_failed" = true ]; then
        record_phase "Verify" "WARN"
    else
        record_phase "Verify" "PASS"
    fi
}

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

print_summary() {
    local total_elapsed=$(( $(get_timestamp) - SCRIPT_START ))

    banner "RESET SUMMARY"
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
    echo -e "Time: $(format_duration $total_elapsed)"
    echo ""

    # Check for any failures
    for entry in "${PHASE_RESULTS[@]}"; do
        local status="${entry%%|*}"
        if [ "$status" = "FAIL" ]; then
            log ERROR "One or more phases failed"
            return 1
        fi
    done

    log OK "Database reset complete"
    return 0
}

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------

print_usage() {
    cat <<'USAGE'
Usage: bash bin/reset_sapphire_db.sh [FLAGS]

Reset SAPPHIRE database volumes and re-run data migration.

Flags:
  --preprocessing-only   Only reset preprocessing DB
  --postprocessing-only  Only reset postprocessing DB
  --skip-migration       Reset DB but skip data import
  --skip-rebuild         Skip docker compose build
  -y, --yes              Skip confirmation prompt
  --help                 Show this help message

Examples:
  bash bin/reset_sapphire_db.sh                        # Full reset
  bash bin/reset_sapphire_db.sh --postprocessing-only  # Postprocessing only
  bash bin/reset_sapphire_db.sh --skip-migration -y    # Reset without import
  bash bin/reset_sapphire_db.sh --skip-rebuild         # Skip rebuild step
USAGE
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
    # Parse arguments
    while [ $# -gt 0 ]; do
        case "$1" in
            --preprocessing-only)   PREPROCESSING_ONLY=true ;;
            --postprocessing-only)  POSTPROCESSING_ONLY=true ;;
            --skip-migration)       SKIP_MIGRATION=true ;;
            --skip-rebuild)         SKIP_REBUILD=true ;;
            -y|--yes)               AUTO_YES=true ;;
            --help|-h)              print_usage; exit 0 ;;
            *)
                echo "Unknown flag: $1"
                print_usage
                exit 1
                ;;
        esac
        shift
    done

    # Validate flags
    if [ "$PREPROCESSING_ONLY" = true ] && [ "$POSTPROCESSING_ONLY" = true ]; then
        log ERROR "--preprocessing-only and --postprocessing-only are mutually exclusive."
        exit 1
    fi

    SCRIPT_START=$(get_timestamp)

    banner "SAPPHIRE Database Reset"

    # Precondition checks
    check_docker
    check_repo_root

    # Describe what will happen
    echo ""
    local scope="both preprocessing and postprocessing"
    if [ "$PREPROCESSING_ONLY" = true ]; then
        scope="preprocessing only"
    elif [ "$POSTPROCESSING_ONLY" = true ]; then
        scope="postprocessing only"
    fi

    log WARN "This will destroy and recreate the ${scope} database(s)."
    log WARN "User and auth databases will NOT be affected."

    if [ "$SKIP_MIGRATION" = true ]; then
        log INFO "Data migration will be skipped (--skip-migration)."
    fi
    if [ "$SKIP_REBUILD" = true ]; then
        log INFO "Image rebuild will be skipped (--skip-rebuild)."
    fi

    # Confirmation prompt
    if [ "$AUTO_YES" = false ]; then
        echo ""
        echo -ne "${BOLD}Proceed? [y/N] ${NC}"
        read -r confirm
        if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
            log INFO "Aborted by user."
            exit 0
        fi
    fi

    # Run phases sequentially (fail-fast on infrastructure, soft-fail on migration)
    phase_stop || exit 1
    phase_remove_volumes || exit 1
    phase_rebuild || exit 1
    phase_start || exit 1
    phase_health || exit 1
    phase_preprocessing_migration
    phase_postprocessing_migration
    phase_verify

    # Summary
    print_summary
    exit $?
}

main "$@"
