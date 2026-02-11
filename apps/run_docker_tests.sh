#!/usr/bin/env bash

# =============================================================================
# SAPPHIRE Docker Smoke Tests (Stage 2a)
# =============================================================================
#
# Builds Docker images and runs import smoke tests locally, mirroring what CI
# does in build_test.yml. Gives faster feedback before pushing.
#
# Usage:
#   bash apps/run_docker_tests.sh                    # Build all + smoke test all
#   bash apps/run_docker_tests.sh --skip-ml          # Skip ML (saves ~10 min)
#   bash apps/run_docker_tests.sh --build-only       # Build only, no smoke tests
#   bash apps/run_docker_tests.sh --skip-build       # Smoke test existing images
#   bash apps/run_docker_tests.sh preprunoff         # Single target
#   bash apps/run_docker_tests.sh preprunoff linreg  # Multiple targets
#
# Flags:
#   --build-only    Build images, skip smoke tests
#   --skip-build    Smoke test existing images, skip builds
#   --skip-ml       Exclude machine_learning (huge image, ~10+ min build)
#   --help          Print usage
#
# Prerequisites:
#   - Docker daemon running
#   - Run from repository root (parent of apps/)
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Colors
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

IMAGE_TAG="local-test"
BASE_IMAGE="mabesa/sapphire-pythonbaseimage"

# Tracking arrays
BUILD_PASSED=()
BUILD_FAILED=()
BUILD_SKIPPED=()
SMOKE_PASSED=()
SMOKE_FAILED=()
SMOKE_SKIPPED=()

# Flags
BUILD_ONLY=false
SKIP_BUILD=false
SKIP_ML=false

# Timing
SCRIPT_START=0

# ---------------------------------------------------------------------------
# Image registry
# ---------------------------------------------------------------------------
# Each target: KEY|IMAGE_NAME|DOCKERFILE|SMOKE_CMD|VENV_PREFIX
#
# SMOKE_CMD is a Python expression run inside the container. For the base
# image, smoke tests use shell commands instead.
#
# VENV_PREFIX is the path to .venv/bin/python relative to the container's
# WORKDIR. Most modules set WORKDIR to /app/apps/<module>, so the prefix
# is empty (just ".venv/bin/python"). Pipeline keeps WORKDIR as /app.

TARGETS=(
    "base|mabesa/sapphire-pythonbaseimage|apps/docker_base_image/Dockerfile||"
    "pipeline|mabesa/sapphire-pipeline|apps/pipeline/Dockerfile|import luigi; import docker; import yaml; import requests; import tenacity|apps/pipeline/"
    "preprunoff|mabesa/sapphire-preprunoff|apps/preprocessing_runoff/Dockerfile|import preprocessing_runoff|"
    "prepgateway|mabesa/sapphire-prepgateway|apps/preprocessing_gateway/Dockerfile|import pandas; import numpy; import scipy; import sklearn; import luigi; import sapphire_dg_client|"
    "linreg|mabesa/sapphire-linreg|apps/linear_regression/Dockerfile|import pandas; import numpy; import docker; from ieasyhydro_sdk.sdk import IEasyHydroSDK|"
    "ml|mabesa/sapphire-ml|apps/machine_learning/Dockerfile|import torch; import darts; import pandas; import numpy|"
    "dashboard|mabesa/sapphire-dashboard|apps/forecast_dashboard/Dockerfile|import panel; import holoviews; import bokeh; import pandas; import numpy|"
    "postprocessing|mabesa/sapphire-postprocessing|apps/postprocessing_forecasts/Dockerfile|import pandas; import numpy; import openpyxl|"
)

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

# Parse a target entry: KEY|IMAGE|DOCKERFILE|SMOKE_CMD|VENV_PREFIX
target_key()          { echo "$1" | cut -d'|' -f1; }
target_image()        { echo "$1" | cut -d'|' -f2; }
target_dockerfile()   { echo "$1" | cut -d'|' -f3; }
target_smoke_cmd()    { echo "$1" | cut -d'|' -f4; }
target_venv_prefix()  { echo "$1" | cut -d'|' -f5; }

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
    if [ ! -f "apps/docker_base_image/Dockerfile" ]; then
        log ERROR "Must run from the repository root (parent of apps/)."
        log ERROR "  Expected: apps/docker_base_image/Dockerfile"
        exit 1
    fi
    log OK "Repository root detected"
}

# ---------------------------------------------------------------------------
# Build functions
# ---------------------------------------------------------------------------

build_image() {
    local key="$1"
    local image="$2"
    local dockerfile="$3"
    local start elapsed
    local build_log

    build_log=$(mktemp)

    log INFO "Building ${key} (${image}:${IMAGE_TAG}) ..."
    start=$(get_timestamp)

    # Child images use FROM mabesa/sapphire-pythonbaseimage:latest.
    # BuildKit may resolve that against the registry manifest (which may
    # lack the local platform, e.g. arm64).  --build-context overrides the
    # FROM reference to point at our locally-built base image instead.
    local build_ctx_flag=()
    if [ "$key" != "base" ]; then
        build_ctx_flag=(--build-context
            "${BASE_IMAGE}:latest=docker-image://${BASE_IMAGE}:${IMAGE_TAG}")
    fi

    if DOCKER_BUILDKIT=1 docker build \
        "${build_ctx_flag[@]}" \
        -t "${image}:${IMAGE_TAG}" \
        -f "${dockerfile}" . \
        >"$build_log" 2>&1; then
        elapsed=$(( $(get_timestamp) - start ))
        log OK "Built ${key} in $(format_duration $elapsed)"
        BUILD_PASSED+=("${key}")
        rm -f "$build_log"
        return 0
    else
        elapsed=$(( $(get_timestamp) - start ))
        log ERROR "FAILED to build ${key} after $(format_duration $elapsed)"
        log ERROR "Last 15 lines of build output:"
        tail -15 "$build_log" | while IFS= read -r line; do
            echo -e "  ${RED}${line}${NC}"
        done
        rm -f "$build_log"
        BUILD_FAILED+=("${key}")
        return 1
    fi
}

# ---------------------------------------------------------------------------
# Smoke test functions
# ---------------------------------------------------------------------------

smoke_test_base() {
    local image="$1"
    local tag="${IMAGE_TAG}"
    local ok=true

    log INFO "Smoke testing base image..."

    # Python version
    if docker run --rm "${image}:${tag}" python --version >/dev/null 2>&1; then
        log OK "  python --version: OK"
    else
        log ERROR "  python --version: FAILED"
        ok=false
    fi

    # uv version
    if docker run --rm "${image}:${tag}" uv --version >/dev/null 2>&1; then
        log OK "  uv --version: OK"
    else
        log ERROR "  uv --version: FAILED"
        ok=false
    fi

    if [ "$ok" = true ]; then
        SMOKE_PASSED+=("base")
        return 0
    else
        SMOKE_FAILED+=("base")
        return 1
    fi
}

smoke_test_module() {
    local key="$1"
    local image="$2"
    local smoke_cmd="$3"
    local venv_prefix="$4"
    local tag="${IMAGE_TAG}"
    local python_bin="${venv_prefix}.venv/bin/python"

    log INFO "Smoke testing ${key}..."

    # Modules install packages into a uv-managed .venv, so we must run
    # the venv Python (bare `python` would miss venv packages).
    if docker run --rm "${image}:${tag}" "${python_bin}" -c "${smoke_cmd}" >/dev/null 2>&1; then
        log OK "  ${key}: imports OK"
        SMOKE_PASSED+=("${key}")
        return 0
    else
        log ERROR "  ${key}: import smoke test FAILED"
        log ERROR "  Rerun with: docker run --rm ${image}:${tag} ${python_bin} -c \"${smoke_cmd}\""
        SMOKE_FAILED+=("${key}")
        return 1
    fi
}

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

print_summary() {
    local total_elapsed=$(( $(get_timestamp) - SCRIPT_START ))

    banner "DOCKER SMOKE TEST SUMMARY"

    # Build results
    if [ "$SKIP_BUILD" = false ]; then
        echo ""
        echo -e "${BOLD}Build Results:${NC}"
        for t in "${BUILD_PASSED[@]+"${BUILD_PASSED[@]}"}"; do
            echo -e "  ${GREEN}PASS${NC}  ${t}"
        done
        for t in "${BUILD_FAILED[@]+"${BUILD_FAILED[@]}"}"; do
            echo -e "  ${RED}FAIL${NC}  ${t}"
        done
        for t in "${BUILD_SKIPPED[@]+"${BUILD_SKIPPED[@]}"}"; do
            echo -e "  ${YELLOW}SKIP${NC}  ${t}"
        done
    fi

    # Smoke test results
    if [ "$BUILD_ONLY" = false ]; then
        echo ""
        echo -e "${BOLD}Smoke Test Results:${NC}"
        for t in "${SMOKE_PASSED[@]+"${SMOKE_PASSED[@]}"}"; do
            echo -e "  ${GREEN}PASS${NC}  ${t}"
        done
        for t in "${SMOKE_FAILED[@]+"${SMOKE_FAILED[@]}"}"; do
            echo -e "  ${RED}FAIL${NC}  ${t}"
        done
        for t in "${SMOKE_SKIPPED[@]+"${SMOKE_SKIPPED[@]}"}"; do
            echo -e "  ${YELLOW}SKIP${NC}  ${t}"
        done
    fi

    # Totals
    echo ""
    local build_pass=${#BUILD_PASSED[@]}
    local build_fail=${#BUILD_FAILED[@]}
    local build_skip=${#BUILD_SKIPPED[@]}
    local smoke_pass=${#SMOKE_PASSED[@]}
    local smoke_fail=${#SMOKE_FAILED[@]}
    local smoke_skip=${#SMOKE_SKIPPED[@]}

    if [ "$SKIP_BUILD" = false ]; then
        echo -e "Builds:  ${GREEN}${build_pass} passed${NC}, ${RED}${build_fail} failed${NC}, ${YELLOW}${build_skip} skipped${NC}"
    fi
    if [ "$BUILD_ONLY" = false ]; then
        echo -e "Smokes:  ${GREEN}${smoke_pass} passed${NC}, ${RED}${smoke_fail} failed${NC}, ${YELLOW}${smoke_skip} skipped${NC}"
    fi

    echo -e "Time:    $(format_duration $total_elapsed)"
    echo ""

    # Exit code
    if [ "$build_fail" -gt 0 ] || [ "$smoke_fail" -gt 0 ]; then
        return 1
    fi
    return 0
}

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------

print_usage() {
    cat <<'USAGE'
Usage: bash apps/run_docker_tests.sh [FLAGS] [TARGETS...]

Build Docker images and run import smoke tests locally.

Targets (default: all):
  base            Python base image
  pipeline        Luigi pipeline orchestrator
  preprunoff      Preprocessing runoff
  prepgateway     Preprocessing gateway
  linreg          Linear regression
  ml              Machine learning (large, ~10+ min)
  dashboard       Forecast dashboard
  postprocessing  Postprocessing forecasts

Flags:
  --build-only    Build images, skip smoke tests
  --skip-build    Smoke test existing images, skip builds
  --skip-ml       Exclude machine_learning target
  --help          Show this help message

Examples:
  bash apps/run_docker_tests.sh                    # Build all + smoke test all
  bash apps/run_docker_tests.sh --skip-ml          # Skip ML (saves time)
  bash apps/run_docker_tests.sh --build-only       # Build only
  bash apps/run_docker_tests.sh --skip-build       # Test existing images
  bash apps/run_docker_tests.sh preprunoff         # Single module
  bash apps/run_docker_tests.sh preprunoff linreg  # Multiple modules
USAGE
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
    local selected_targets=()

    # Parse arguments
    while [ $# -gt 0 ]; do
        case "$1" in
            --build-only)  BUILD_ONLY=true ;;
            --skip-build)  SKIP_BUILD=true ;;
            --skip-ml)     SKIP_ML=true ;;
            --help|-h)     print_usage; exit 0 ;;
            -*)
                echo "Unknown flag: $1"
                print_usage
                exit 1
                ;;
            *)  selected_targets+=("$1") ;;
        esac
        shift
    done

    # Validate flag combination
    if [ "$BUILD_ONLY" = true ] && [ "$SKIP_BUILD" = true ]; then
        log ERROR "--build-only and --skip-build are mutually exclusive."
        exit 1
    fi

    SCRIPT_START=$(get_timestamp)

    banner "SAPPHIRE Docker Smoke Tests"

    # Precondition checks
    check_docker
    check_repo_root

    # Resolve which targets to run
    local targets_to_run=()
    if [ ${#selected_targets[@]} -eq 0 ]; then
        # All targets
        for entry in "${TARGETS[@]}"; do
            local key
            key=$(target_key "$entry")
            if [ "$SKIP_ML" = true ] && [ "$key" = "ml" ]; then
                log WARN "Skipping ml (--skip-ml)"
                BUILD_SKIPPED+=("ml")
                SMOKE_SKIPPED+=("ml")
                continue
            fi
            targets_to_run+=("$entry")
        done
    else
        # Selected targets only
        for sel in "${selected_targets[@]}"; do
            local found=false
            for entry in "${TARGETS[@]}"; do
                if [ "$(target_key "$entry")" = "$sel" ]; then
                    if [ "$SKIP_ML" = true ] && [ "$sel" = "ml" ]; then
                        log WARN "Skipping ml (--skip-ml)"
                        BUILD_SKIPPED+=("ml")
                        SMOKE_SKIPPED+=("ml")
                    else
                        targets_to_run+=("$entry")
                    fi
                    found=true
                    break
                fi
            done
            if [ "$found" = false ]; then
                log ERROR "Unknown target: ${sel}"
                echo "Valid targets: base pipeline preprunoff prepgateway linreg ml dashboard postprocessing"
                exit 1
            fi
        done
    fi

    # -----------------------------------------------------------------------
    # BUILD PHASE
    # -----------------------------------------------------------------------
    if [ "$SKIP_BUILD" = false ]; then
        banner "BUILD PHASE"

        # Check if base is in the target list
        local base_in_targets=false
        local has_child_targets=false
        for entry in "${targets_to_run[@]}"; do
            local k
            k=$(target_key "$entry")
            if [ "$k" = "base" ]; then
                base_in_targets=true
            else
                has_child_targets=true
            fi
        done

        # Always build base first — child images need FROM ...:latest
        # If base isn't explicitly requested but child targets are, build it
        # automatically (mirrors what CI does in build_test.yml).
        local base_built=false
        local base_entry="${TARGETS[0]}"  # base is always first
        if [ "$base_in_targets" = true ] || [ "$has_child_targets" = true ]; then
            if build_image "base" "$(target_image "$base_entry")" "$(target_dockerfile "$base_entry")"; then
                base_built=true
                # If base was auto-added (not in user's target list), don't
                # count it in BUILD_PASSED — move it to a neutral note
                if [ "$base_in_targets" = false ]; then
                    # Remove from BUILD_PASSED (it was added by build_image)
                    local tmp=()
                    for p in "${BUILD_PASSED[@]}"; do
                        [ "$p" != "base" ] && tmp+=("$p")
                    done
                    BUILD_PASSED=("${tmp[@]+"${tmp[@]}"}")
                    log INFO "Base image built automatically (required by child images)"
                fi
            else
                log ERROR "Base image build failed. Cannot build child images."
                # Mark all child targets as skipped
                for other in "${targets_to_run[@]}"; do
                    local other_key
                    other_key=$(target_key "$other")
                    if [ "$other_key" != "base" ]; then
                        BUILD_SKIPPED+=("${other_key}")
                    fi
                done
                # Skip to summary
                print_summary || exit 1
                exit 0
            fi
        fi

        # Build remaining targets
        for entry in "${targets_to_run[@]}"; do
            local key
            key=$(target_key "$entry")
            [ "$key" = "base" ] && continue

            build_image "$key" "$(target_image "$entry")" "$(target_dockerfile "$entry")" || true
        done
    fi

    # -----------------------------------------------------------------------
    # SMOKE TEST PHASE
    # -----------------------------------------------------------------------
    if [ "$BUILD_ONLY" = false ]; then
        banner "SMOKE TEST PHASE"

        for entry in "${targets_to_run[@]}"; do
            local key image smoke_cmd venv_prefix
            key=$(target_key "$entry")
            image=$(target_image "$entry")
            smoke_cmd=$(target_smoke_cmd "$entry")
            venv_prefix=$(target_venv_prefix "$entry")

            # Skip if build failed (unless we skipped building)
            if [ "$SKIP_BUILD" = false ]; then
                local build_failed=false
                for f in "${BUILD_FAILED[@]+"${BUILD_FAILED[@]}"}"; do
                    if [ "$f" = "$key" ]; then
                        build_failed=true
                        break
                    fi
                done
                # Also skip if it was in BUILD_SKIPPED
                for s in "${BUILD_SKIPPED[@]+"${BUILD_SKIPPED[@]}"}"; do
                    if [ "$s" = "$key" ]; then
                        build_failed=true
                        break
                    fi
                done
                if [ "$build_failed" = true ]; then
                    log WARN "Skipping smoke test for ${key} (build failed/skipped)"
                    SMOKE_SKIPPED+=("${key}")
                    continue
                fi
            fi

            # Run the appropriate smoke test
            if [ "$key" = "base" ]; then
                smoke_test_base "$image" || true
            else
                smoke_test_module "$key" "$image" "$smoke_cmd" "$venv_prefix" || true
            fi
        done
    fi

    # -----------------------------------------------------------------------
    # SUMMARY
    # -----------------------------------------------------------------------
    print_summary
    exit $?
}

main "$@"
