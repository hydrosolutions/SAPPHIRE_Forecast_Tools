#!/usr/bin/env bash

# =============================================================================
# SAPPHIRE Unified Validation Pipeline
# =============================================================================
#
# Orchestrates the full pre-commit / pre-merge validation workflow by calling
# the existing test scripts in sequence.
#
# Quick mode (default — Stage 1 only):
#   bash apps/run_validation.sh
#   bash apps/run_validation.sh quick
#
# Full mode (all stages):
#   ieasyhydroforecast_env_file_path=/path/to/.env \
#     bash apps/run_validation.sh full
#
# Note: Stage 1 (unit tests) always runs with SAPPHIRE_TEST_ENV=True and
# unsets ieasyhydroforecast_env_file_path so that tests use the test .env
# file, not the real config.
#
# Full mode with skips:
#   bash apps/run_validation.sh full --skip-pipeline --skip-ml
#   bash apps/run_validation.sh full --skip-docker
#
# Flags:
#   --skip-docker       Skip Docker smoke tests (Stage 2a) in full mode
#   --skip-pipeline     Skip local pipeline run (Stage 1b) in full mode
#   --skip-ml           Pass --skip-ml to Docker smoke tests
#   --continue-on-error Continue past stage failures (report all at the end)
#   --dry-run           Validate environment without running anything
#   --help              Show usage
#
# Stages:
#   Stage 1   Unit/integration tests       (run_tests.sh)
#   Stage 1b  Local pipeline run            (run_locally.sh all + maintenance)
#   Stage 2a  Docker build + smoke tests    (run_docker_tests.sh)
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Detect repo root and script directory
# ---------------------------------------------------------------------------

# Work from the repo root regardless of where the script is invoked from.
# The script lives at apps/run_validation.sh, so the repo root is one
# level up from the script's directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Verify we found the right place
if [ ! -f "$REPO_ROOT/apps/run_tests.sh" ]; then
    echo "ERROR: Cannot locate apps/run_tests.sh from repo root: $REPO_ROOT"
    exit 1
fi

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LOG_DIR="${SCRIPT_DIR}/logs"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="${LOG_DIR}/validation_${TIMESTAMP}.log"

# Flags
MODE="quick"
SKIP_DOCKER=false
SKIP_PIPELINE=false
SKIP_ML=false
CONTINUE_ON_ERROR=false
DRY_RUN=false

# Stage tracking
declare -a STAGE_NAMES=()
declare -a STAGE_STATUSES=()
declare -a STAGE_TIMES=()

# Colors (matching run_locally.sh conventions)
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

log() {
    local level="$1"
    shift
    local msg="$*"
    local ts
    ts="$(date '+%Y-%m-%d %H:%M:%S')"
    local line="[${ts}] [${level}] ${msg}"

    case "$level" in
        INFO)  echo -e "${BLUE}${line}${NC}" ;;
        OK)    echo -e "${GREEN}${line}${NC}" ;;
        WARN)  echo -e "${YELLOW}${line}${NC}" ;;
        ERROR) echo -e "${RED}${line}${NC}" ;;
        *)     echo "$line" ;;
    esac

    echo "$line" >> "$LOG_FILE"
}

banner() {
    local msg="$1"
    local sep
    sep="$(printf '=%.0s' {1..60})"
    log INFO "$sep"
    log INFO "$msg"
    log INFO "$sep"
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

record_stage() {
    local name="$1"
    local status="$2"
    local elapsed="$3"
    STAGE_NAMES+=("$name")
    STAGE_STATUSES+=("$status")
    STAGE_TIMES+=("$elapsed")
}

# ---------------------------------------------------------------------------
# Stage runners
# ---------------------------------------------------------------------------

run_stage_tests() {
    banner "Stage 1: Unit/Integration Tests"
    local start
    start=$(get_timestamp)

    log INFO "Running: cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh"

    local rc=0
    (
        cd "$SCRIPT_DIR"
        # Unset pipeline env vars so they don't leak into tests.
        # SAPPHIRE_TEST_ENV causes setup_library to use the test .env file.
        unset ieasyhydroforecast_env_file_path
        unset SAPPHIRE_OPDEV_ENV
        SAPPHIRE_TEST_ENV=True bash run_tests.sh
    ) 2>&1 | tee -a "$LOG_FILE" || rc=${PIPESTATUS[0]:-$?}

    local elapsed=$(( $(get_timestamp) - start ))

    if [ $rc -eq 0 ]; then
        log OK "Stage 1 passed in $(format_duration $elapsed)"
        record_stage "Unit tests" "PASS" "$elapsed"
    else
        log ERROR "Stage 1 failed (exit $rc) after $(format_duration $elapsed)"
        record_stage "Unit tests" "FAIL" "$elapsed"
    fi
    return $rc
}

run_stage_pipeline_all() {
    banner "Stage 1b: Local Pipeline (operational)"
    local start
    start=$(get_timestamp)

    log INFO "Running: SAPPHIRE_PREDICTION_MODE=BOTH bash apps/run_locally.sh all --continue-on-error"

    local rc=0
    (
        cd "$REPO_ROOT"
        SAPPHIRE_PREDICTION_MODE="${SAPPHIRE_PREDICTION_MODE:-BOTH}" \
            bash apps/run_locally.sh --continue-on-error all
    ) 2>&1 | tee -a "$LOG_FILE" || rc=${PIPESTATUS[0]:-$?}

    local elapsed=$(( $(get_timestamp) - start ))

    if [ $rc -eq 0 ]; then
        log OK "Stage 1b (operational) passed in $(format_duration $elapsed)"
        record_stage "Local pipeline" "PASS" "$elapsed"
    else
        log ERROR "Stage 1b (operational) failed (exit $rc) after $(format_duration $elapsed)"
        record_stage "Local pipeline" "FAIL" "$elapsed"
    fi
    return $rc
}

run_stage_pipeline_maintenance() {
    banner "Stage 1b: Local Pipeline (maintenance)"
    local start
    start=$(get_timestamp)

    log INFO "Running: SAPPHIRE_PREDICTION_MODE=BOTH bash apps/run_locally.sh maintenance --continue-on-error"

    local rc=0
    (
        cd "$REPO_ROOT"
        SAPPHIRE_PREDICTION_MODE="${SAPPHIRE_PREDICTION_MODE:-BOTH}" \
            bash apps/run_locally.sh --continue-on-error maintenance
    ) 2>&1 | tee -a "$LOG_FILE" || rc=${PIPESTATUS[0]:-$?}

    local elapsed=$(( $(get_timestamp) - start ))

    if [ $rc -eq 0 ]; then
        log OK "Stage 1b (maintenance) passed in $(format_duration $elapsed)"
        record_stage "Maintenance" "PASS" "$elapsed"
    else
        log ERROR "Stage 1b (maintenance) failed (exit $rc) after $(format_duration $elapsed)"
        record_stage "Maintenance" "FAIL" "$elapsed"
    fi
    return $rc
}

run_stage_docker() {
    banner "Stage 2a: Docker Smoke Tests"
    local start
    start=$(get_timestamp)

    local docker_flags=()
    if [ "$SKIP_ML" = true ]; then
        docker_flags+=(--skip-ml)
    fi

    log INFO "Running: bash apps/run_docker_tests.sh ${docker_flags[*]:-}"

    local rc=0
    (
        cd "$REPO_ROOT"
        bash apps/run_docker_tests.sh "${docker_flags[@]+"${docker_flags[@]}"}"
    ) 2>&1 | tee -a "$LOG_FILE" || rc=${PIPESTATUS[0]:-$?}

    local elapsed=$(( $(get_timestamp) - start ))

    if [ $rc -eq 0 ]; then
        log OK "Stage 2a passed in $(format_duration $elapsed)"
        record_stage "Docker smoke" "PASS" "$elapsed"
    else
        log ERROR "Stage 2a failed (exit $rc) after $(format_duration $elapsed)"
        record_stage "Docker smoke" "FAIL" "$elapsed"
    fi
    return $rc
}

# ---------------------------------------------------------------------------
# Dry-run validation
# ---------------------------------------------------------------------------

validate_environment() {
    local errors=0

    log INFO "Validating environment..."

    # Always check: run_tests.sh exists
    if [ -f "$SCRIPT_DIR/run_tests.sh" ]; then
        log OK "run_tests.sh found"
    else
        log ERROR "run_tests.sh not found in $SCRIPT_DIR"
        errors=$((errors + 1))
    fi

    if [ "$MODE" = "full" ]; then
        # Check run_locally.sh
        if [ -f "$SCRIPT_DIR/run_locally.sh" ]; then
            log OK "run_locally.sh found"
        else
            log ERROR "run_locally.sh not found in $SCRIPT_DIR"
            errors=$((errors + 1))
        fi

        # Check run_docker_tests.sh
        if [ "$SKIP_DOCKER" = false ]; then
            if [ -f "$SCRIPT_DIR/run_docker_tests.sh" ]; then
                log OK "run_docker_tests.sh found"
            else
                log ERROR "run_docker_tests.sh not found in $SCRIPT_DIR"
                errors=$((errors + 1))
            fi

            # Check Docker daemon
            if docker info >/dev/null 2>&1; then
                log OK "Docker daemon is running"
            else
                log WARN "Docker daemon is not running (Stage 2a will fail)"
            fi
        fi

        # Check env file for pipeline stages
        if [ "$SKIP_PIPELINE" = false ]; then
            if [ -z "${ieasyhydroforecast_env_file_path:-}" ]; then
                log ERROR "ieasyhydroforecast_env_file_path is not set (required for Stage 1b)"
                errors=$((errors + 1))
            elif [ ! -f "$ieasyhydroforecast_env_file_path" ]; then
                log ERROR "Env file not found: ${ieasyhydroforecast_env_file_path}"
                errors=$((errors + 1))
            else
                log OK "Env file: ${ieasyhydroforecast_env_file_path}"
            fi
        fi
    fi

    if [ $errors -gt 0 ]; then
        log ERROR "Validation failed with ${errors} error(s)"
        return 1
    fi

    log OK "Validation passed"
    return 0
}

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

print_summary() {
    local total_time=$1
    echo "" | tee -a "$LOG_FILE"

    local sep
    sep="$(printf '=%.0s' {1..40})"

    echo -e "${BOLD}${sep}${NC}" | tee -a "$LOG_FILE"
    echo -e "${BOLD}VALIDATION SUMMARY${NC}" | tee -a "$LOG_FILE"
    echo -e "${BOLD}${sep}${NC}" | tee -a "$LOG_FILE"

    local pass_count=0
    local fail_count=0
    local total_count=${#STAGE_NAMES[@]}

    for i in "${!STAGE_NAMES[@]}"; do
        local name="${STAGE_NAMES[$i]}"
        local status="${STAGE_STATUSES[$i]}"
        local elapsed="${STAGE_TIMES[$i]}"
        local duration
        duration="$(format_duration "$elapsed")"

        # Pad name for alignment
        local padded_name
        padded_name="$(printf '%-20s' "$name")"

        if [ "$status" = "PASS" ]; then
            echo -e "  ${padded_name} ${GREEN}PASS${NC}  (${duration})" | tee -a "$LOG_FILE"
            pass_count=$((pass_count + 1))
        else
            echo -e "  ${padded_name} ${RED}FAIL${NC}  (${duration})" | tee -a "$LOG_FILE"
            fail_count=$((fail_count + 1))
        fi
    done

    echo "" | tee -a "$LOG_FILE"
    echo -e "Total: ${pass_count}/${total_count} stages passed ($(format_duration "$total_time"))" | tee -a "$LOG_FILE"
    echo -e "Log: ${LOG_FILE}" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"

    if [ $fail_count -gt 0 ]; then
        return 1
    fi
    return 0
}

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------

print_usage() {
    cat <<'USAGE'
Usage: bash apps/run_validation.sh [MODE] [FLAGS]

Unified validation pipeline that orchestrates all test stages.

Modes:
  quick     (default) Stage 1 only: unit/integration tests
  full      All stages: tests -> local pipeline -> Docker smoke tests

Flags:
  --skip-docker       Skip Docker smoke tests in full mode
  --skip-pipeline     Skip local pipeline run in full mode
  --skip-ml           Pass --skip-ml to Docker smoke tests
  --continue-on-error Continue past stage failures
  --dry-run           Validate environment without running anything
  --help              Show this help message

Environment variables:
  ieasyhydroforecast_env_file_path   Path to .env config (required for full mode)
  SAPPHIRE_PREDICTION_MODE           Defaults to BOTH for full mode

Examples:
  # Quick validation (unit tests only)
  bash apps/run_validation.sh

  # Full validation
  ieasyhydroforecast_env_file_path=~/config/.env \
    bash apps/run_validation.sh full

  # Full but skip the local pipeline
  bash apps/run_validation.sh full --skip-pipeline --skip-ml

  # Dry run (check environment)
  bash apps/run_validation.sh full --dry-run
USAGE
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
    # Parse arguments
    while [ $# -gt 0 ]; do
        case "$1" in
            quick)               MODE="quick" ;;
            full)                MODE="full" ;;
            --skip-docker)       SKIP_DOCKER=true ;;
            --skip-pipeline)     SKIP_PIPELINE=true ;;
            --skip-ml)           SKIP_ML=true ;;
            --continue-on-error) CONTINUE_ON_ERROR=true ;;
            --dry-run)           DRY_RUN=true ;;
            --help|-h)           print_usage; exit 0 ;;
            -*)
                echo "Unknown flag: $1"
                print_usage
                exit 1
                ;;
            *)
                echo "Unknown argument: $1"
                print_usage
                exit 1
                ;;
        esac
        shift
    done

    # Create log directory
    mkdir -p "$LOG_DIR"

    banner "SAPPHIRE Validation Pipeline"
    log INFO "Mode: ${MODE}"
    log INFO "Continue on error: ${CONTINUE_ON_ERROR}"
    if [ "$MODE" = "full" ]; then
        log INFO "Skip pipeline: ${SKIP_PIPELINE}"
        log INFO "Skip Docker: ${SKIP_DOCKER}"
        log INFO "Skip ML: ${SKIP_ML}"
    fi
    log INFO "Log file: ${LOG_FILE}"

    # Validate environment
    if ! validate_environment; then
        exit 1
    fi

    # Dry run stops here
    if [ "$DRY_RUN" = true ]; then
        log OK "Dry run complete. Environment is valid."
        exit 0
    fi

    local pipeline_start
    pipeline_start=$(get_timestamp)
    local any_failed=false

    # --- Stage 1: Unit tests (always) ---
    if ! run_stage_tests; then
        any_failed=true
        if [ "$CONTINUE_ON_ERROR" = false ]; then
            local pipeline_elapsed=$(( $(get_timestamp) - pipeline_start ))
            print_summary "$pipeline_elapsed" || true
            exit 1
        fi
    fi

    # --- Full mode stages ---
    if [ "$MODE" = "full" ]; then

        # Stage 1b: Local pipeline (operational)
        if [ "$SKIP_PIPELINE" = false ]; then
            if ! run_stage_pipeline_all; then
                any_failed=true
                if [ "$CONTINUE_ON_ERROR" = false ]; then
                    local pipeline_elapsed=$(( $(get_timestamp) - pipeline_start ))
                    print_summary "$pipeline_elapsed" || true
                    exit 1
                fi
            fi

            # Stage 1b: Local pipeline (maintenance)
            if ! run_stage_pipeline_maintenance; then
                any_failed=true
                if [ "$CONTINUE_ON_ERROR" = false ]; then
                    local pipeline_elapsed=$(( $(get_timestamp) - pipeline_start ))
                    print_summary "$pipeline_elapsed" || true
                    exit 1
                fi
            fi
        fi

        # Stage 2a: Docker smoke tests
        if [ "$SKIP_DOCKER" = false ]; then
            if ! run_stage_docker; then
                any_failed=true
                if [ "$CONTINUE_ON_ERROR" = false ]; then
                    local pipeline_elapsed=$(( $(get_timestamp) - pipeline_start ))
                    print_summary "$pipeline_elapsed" || true
                    exit 1
                fi
            fi
        fi
    fi

    local pipeline_elapsed=$(( $(get_timestamp) - pipeline_start ))

    # Print summary
    if [ ${#STAGE_NAMES[@]} -gt 0 ]; then
        print_summary "$pipeline_elapsed"
        local summary_rc=$?
    fi

    if [ "$any_failed" = true ]; then
        exit 1
    fi
    exit 0
}

main "$@"
