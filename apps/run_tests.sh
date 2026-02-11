#!/usr/bin/env bash

# This script runs the tests for all the modules in the Sapphire project.
# It uses uv-based virtual environments (.venv) in each module directory.
#
# Usage:
#   cd to the apps directory and run the script with the following command:
#   $ bash run_tests.sh
#
#   Or run tests for a specific module:
#   $ bash run_tests.sh iEasyHydroForecast
#   $ bash run_tests.sh pipeline
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
#   - Each module needs a .venv with pytest installed: cd <module> && uv sync --all-extras
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
SKIPPED=()

run_module_tests() {
    local module=$1
    local venv_path="${module}/.venv/bin/pytest"

    echo ""
    echo "========================================"
    echo -e "${YELLOW}Testing: ${module}${NC}"
    echo "========================================"

    # Check if venv exists
    if [ ! -f "$venv_path" ]; then
        echo -e "${YELLOW}⚠ Skipping ${module}: No .venv found. Run 'cd ${module} && uv sync' first.${NC}"
        SKIPPED+=("$module")
        return 0
    fi

    # Find test directory (could be 'tests' or 'test')
    local test_dir=""
    if [ -d "${module}/tests" ]; then
        test_dir="${module}/tests"
    elif [ -d "${module}/test" ]; then
        test_dir="${module}/test"
    else
        echo -e "${YELLOW}⚠ Skipping ${module}: No test directory found (looked for 'tests' and 'test')${NC}"
        SKIPPED+=("$module")
        return 0
    fi

    # Run tests
    if SAPPHIRE_TEST_ENV=True "$venv_path" "$test_dir" -v; then
        echo -e "${GREEN}✓ ${module} tests passed${NC}"
        PASSED+=("$module")
    else
        echo -e "${RED}✗ ${module} tests failed${NC}"
        FAILED+=("$module")
    fi
}

# List of all modules with tests
MODULES=(
    "iEasyHydroForecast"
    "preprocessing_runoff"
    "preprocessing_gateway"
    "linear_regression"
    "machine_learning"
    "postprocessing_forecasts"
    "pipeline"
    "long_term_forecasting"
    "forecast_dashboard"
)

# If a specific module is provided as argument, only run that one
if [ -n "$1" ]; then
    # Check if module is valid
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
        exit 1
    fi
else
    # Run all module tests
    echo "Running tests for all modules..."
    echo ""

    for module in "${MODULES[@]}"; do
        run_module_tests "$module"
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

if [ ${#SKIPPED[@]} -gt 0 ]; then
    echo -e "${YELLOW}Skipped (${#SKIPPED[@]}):${NC} ${SKIPPED[*]}"
fi

if [ ${#FAILED[@]} -gt 0 ]; then
    echo -e "${RED}Failed (${#FAILED[@]}):${NC} ${FAILED[*]}"
    echo ""
    exit 1
fi

echo ""
echo -e "${GREEN}All tests completed successfully!${NC}"

# Check if integration tests were skipped and print warning
INTEGRATION_WARNING=false
if [[ " ${PASSED[*]} " =~ " forecast_dashboard " ]] || [[ "$1" == "forecast_dashboard" ]]; then
    if [ "${TEST_LOCAL:-false}" != "true" ] && [ "${TEST_PENTAD:-false}" != "true" ] && [ "${TEST_DECAD:-false}" != "true" ]; then
        INTEGRATION_WARNING=true
    fi
fi

if [ "$INTEGRATION_WARNING" = true ]; then
    echo ""
    echo -e "${YELLOW}⚠ NOTE: Dashboard integration tests were SKIPPED.${NC}"
    echo -e "${YELLOW}  To run integration tests, use environment variables:${NC}"
    echo -e "${YELLOW}    TEST_LOCAL=true bash run_tests.sh forecast_dashboard${NC}"
    echo -e "${YELLOW}    TEST_PENTAD=true bash run_tests.sh forecast_dashboard${NC}"
    echo -e "${YELLOW}    TEST_DECAD=true bash run_tests.sh forecast_dashboard${NC}"
fi
