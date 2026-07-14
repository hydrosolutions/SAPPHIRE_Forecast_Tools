#!/usr/bin/env bash

# This script runs the tests for all the modules in the Sapphire project.
# It uses uv-based virtual environments (.venv) in each module directory.
#
# Usage:
#   cd to the apps directory and run the script with the following command:
#   $ bash run_tests.sh
#
#   Or run tests for a specific module or service:
#   $ bash run_tests.sh iEasyHydroForecast
#   $ bash run_tests.sh pipeline
#   $ bash run_tests.sh service:postprocessing
#
# Integration Tests (forecast_dashboard):
#   By default, dashboard integration tests are SKIPPED. To run them, set
#   the appropriate environment variables:
#
#   $ TEST_LOCAL=true bash run_tests.sh forecast_dashboard
#       Runs local dashboard tests (requires server at localhost:5055 + data)
#
#   $ TEST_PENTAD=true bash run_tests.sh forecast_dashboard
#       Runs pentad production server tests
#
#   $ TEST_DECAD=true bash run_tests.sh forecast_dashboard
#       Runs decad production server tests
#
#   $ TEST_LOCAL=true TEST_PENTAD=true bash run_tests.sh forecast_dashboard
#       Runs both local and pentad tests
#
# Prerequisites:
#   - Each app module needs a .venv: cd <module> && uv sync --all-extras
#   - Each service needs a .venv: cd sapphire/services/<service> && uv sync --all-extras
#   - For dashboard tests: playwright install chromium

set -e  # Exit on first error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get the directory where the script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Track results
PASSED=()
FAILED=()
# ERRORED holds modules that could NOT be verified at all: missing/broken
# venv (no pytest), missing test directory, or a pytest run that collected
# zero tests. This is distinct from FAILED (tests actually ran and some
# failed assertions) and, unlike the old SKIPPED array, is NEVER treated as
# a passing/neutral outcome by the summary at the bottom of this script.
# This is the fix for the "silent success over zero tests" bug: previously
# these conditions were recorded in a SKIPPED array that the final success
# banner and exit code completely ignored.
ERRORED=()

run_module_tests() {
    local module=$1
    local venv_path="${module}/.venv/bin/pytest"

    echo ""
    echo "========================================"
    echo -e "${YELLOW}Testing: ${module}${NC}"
    echo "========================================"

    # Check if venv exists and has pytest installed
    if [ ! -f "$venv_path" ]; then
        echo -e "${RED}✗ ERROR ${module}: cannot run tests — no pytest found at ${venv_path}.${NC}"
        echo -e "${RED}  Fix: cd ${module} && uv sync --all-extras${NC}"
        ERRORED+=("$module")
        return 0
    fi

    # Find test directory (could be 'tests' or 'test')
    local test_dir=""
    if [ -d "${module}/tests" ]; then
        test_dir="${module}/tests"
    elif [ -d "${module}/test" ]; then
        test_dir="${module}/test"
    else
        echo -e "${RED}✗ ERROR ${module}: no test directory found (looked for 'tests' and 'test').${NC}"
        echo -e "${RED}  Module is listed in run_tests.sh but has no test suite to run.${NC}"
        ERRORED+=("$module")
        return 0
    fi

    # Run tests. Capture the real exit code (instead of branching directly in
    # the if-condition) so a "collected zero tests" run (pytest exit code 5)
    # can be told apart from a genuine test failure and reported as an error
    # rather than silently accepted or lumped in with real failures.
    local rc=0
    SAPPHIRE_TEST_ENV=True "$venv_path" "$test_dir" -v || rc=$?

    if [ "$rc" -eq 0 ]; then
        echo -e "${GREEN}✓ ${module} tests passed${NC}"
        PASSED+=("$module")
    elif [ "$rc" -eq 5 ]; then
        echo -e "${RED}✗ ERROR ${module}: pytest collected zero tests (exit code 5).${NC}"
        echo -e "${RED}  A run that verifies nothing is not a pass. Check the test path/markers.${NC}"
        ERRORED+=("$module")
    else
        echo -e "${RED}✗ ${module} tests failed${NC}"
        FAILED+=("$module")
    fi
}

run_service_tests() {
    local service=$1
    local service_dir="../sapphire/services/${service}"
    local venv_path="${service_dir}/.venv/bin/pytest"
    local display_name="service:${service}"

    echo ""
    echo "========================================"
    echo -e "${YELLOW}Testing: ${display_name}${NC}"
    echo "========================================"

    # Check if venv exists and has pytest installed
    if [ ! -f "$venv_path" ]; then
        echo -e "${RED}✗ ERROR ${display_name}: cannot run tests — no pytest found at ${venv_path}.${NC}"
        echo -e "${RED}  Fix: cd sapphire/services/${service} && uv sync --all-extras${NC}"
        ERRORED+=("$display_name")
        return 0
    fi

    # Find test directory
    local test_dir=""
    if [ -d "${service_dir}/tests" ]; then
        test_dir="${service_dir}/tests"
    elif [ -d "${service_dir}/test" ]; then
        test_dir="${service_dir}/test"
    else
        echo -e "${RED}✗ ERROR ${display_name}: no test directory found (looked for 'tests' and 'test').${NC}"
        echo -e "${RED}  Service is listed in run_tests.sh but has no test suite to run.${NC}"
        ERRORED+=("$display_name")
        return 0
    fi

    # Run tests (services don't need SAPPHIRE_TEST_ENV). Capture the real
    # exit code so "collected zero tests" (pytest exit code 5) is reported
    # as an error rather than silently accepted or lumped in with failures.
    local rc=0
    "$venv_path" "$test_dir" -v || rc=$?

    if [ "$rc" -eq 0 ]; then
        echo -e "${GREEN}✓ ${display_name} tests passed${NC}"
        PASSED+=("$display_name")
    elif [ "$rc" -eq 5 ]; then
        echo -e "${RED}✗ ERROR ${display_name}: pytest collected zero tests (exit code 5).${NC}"
        echo -e "${RED}  A run that verifies nothing is not a pass. Check the test path/markers.${NC}"
        ERRORED+=("$display_name")
    else
        echo -e "${RED}✗ ${display_name} tests failed${NC}"
        FAILED+=("$display_name")
    fi
}

# List of all app modules with tests
MODULES=(
    "iEasyHydroForecast"
    "preprocessing_runoff"
    "preprocessing_gateway"
    "linear_regression"
    "machine_learning"
    "forecast_skill_eval"
    "postprocessing_forecasts"
    "pipeline"
    "long_term_forecasting"
    "validate_pipeline"
    "forecast_dashboard"
)

# List of sapphire services with tests (in sapphire/services/<name>/)
SERVICE_MODULES=(
    "api-gateway"
    "postprocessing"
    "preprocessing"
    "user"
    "auth"
)

# main() wraps arg dispatch + run + summary so this file can be sourced by
# a test harness (test_run_tests.sh, in this same directory) without
# executing a real test run: sourcing re-uses the exact
# run_module_tests/run_service_tests logic
# above against synthetic fixtures. The guard at the bottom of this file
# calls main "$@" only when the script is executed directly, which is the
# only way it is invoked today (`bash run_tests.sh ...`), so normal usage
# is unaffected.
main() {
# If a specific module is provided as argument, only run that one
if [ -n "$1" ]; then
    # Check for service:name syntax
    if [[ "$1" == service:* ]]; then
        service_name="${1#service:}"
        valid_service=false
        for svc in "${SERVICE_MODULES[@]}"; do
            if [ "$service_name" == "$svc" ]; then
                valid_service=true
                break
            fi
        done

        if [ "$valid_service" = true ]; then
            run_service_tests "$service_name"
        else
            echo "Unknown service: $service_name"
            echo "Available services: ${SERVICE_MODULES[*]}"
            exit 1
        fi
    else
        # Check app modules
        valid_module=false
        for mod in "${MODULES[@]}"; do
            if [ "$1" == "$mod" ]; then
                valid_module=true
                break
            fi
        done

        if [ "$valid_module" = true ]; then
            run_module_tests "$1"
        else
            echo "Unknown module: $1"
            echo "Available modules: ${MODULES[*]}"
            echo "Available services: ${SERVICE_MODULES[*]} (use 'service:<name>' syntax)"
            exit 1
        fi
    fi
else
    # Run all app module tests
    echo "Running tests for all app modules..."
    echo ""

    for module in "${MODULES[@]}"; do
        run_module_tests "$module"
    done

    # Run all service tests
    echo ""
    echo ""
    echo "========================================"
    echo "SAPPHIRE SERVICE TESTS"
    echo "========================================"

    for service in "${SERVICE_MODULES[@]}"; do
        run_service_tests "$service"
    done
fi

# Print summary
echo ""
echo "========================================"
echo "TEST SUMMARY"
echo "========================================"

if [ ${#PASSED[@]} -gt 0 ]; then
    echo -e "${GREEN}Passed (${#PASSED[@]}):${NC} ${PASSED[*]}"
fi

if [ ${#FAILED[@]} -gt 0 ]; then
    echo -e "${RED}Failed (${#FAILED[@]}):${NC} ${FAILED[*]}"
fi

if [ ${#ERRORED[@]} -gt 0 ]; then
    echo -e "${RED}Errored — could not be verified (${#ERRORED[@]}):${NC} ${ERRORED[*]}"
    echo -e "${RED}These modules were NOT tested. Zero tests running is not success.${NC}"
fi

# A module that could not be verified (broken/missing venv, no test
# directory, or a run that collected zero tests) is exactly as bad as a
# module with failing tests: neither means the code was checked. The
# success banner below must never print, and the exit code must never be 0,
# when either array is non-empty.
if [ ${#FAILED[@]} -gt 0 ] || [ ${#ERRORED[@]} -gt 0 ]; then
    echo ""
    exit 1
fi

echo ""
echo -e "${GREEN}All tests completed successfully!${NC}"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
