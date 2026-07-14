#!/usr/bin/env bash
#
# Regression test for run_tests.sh's result accounting.
#
# Context: run_tests.sh used to have a "silent success over zero tests" bug.
# When a module's venv was missing/broken (no pytest), or had no test
# directory, the module was recorded in a SKIPPED array that the final
# summary completely ignored -- the script still printed "All tests
# completed successfully!" and exited 0, even though zero tests ran for
# that module. This test locks in the fix: such conditions must now be
# recorded as errors (ERRORED array) that make the overall run fail loudly
# (non-zero exit, no unqualified success banner), while a genuine passing
# run (including one containing pytest-level skips, e.g. the
# SAPPHIRE_API_AVAILABLE-gated skip pattern documented in CLAUDE.md) must
# still succeed.
#
# There is no existing shell-test framework/precedent in this repo (no
# bats, no shunit2), so this is a small dependency-free bash harness. It
# sources run_tests.sh (which guards its own "main" dispatch/summary logic
# behind a `[[ "${BASH_SOURCE[0]}" == "${0}" ]]` check, so sourcing it only
# defines functions/arrays and does not run a real test sweep) and drives
# it against synthetic fixture "modules" -- fake .venv/bin/pytest
# executables under a temp directory that deterministically exit with a
# chosen code, standing in for: missing pytest, a broken pytest, a run
# that collects zero tests (pytest exit code 5), a passing run, and a
# failing run.
#
# Usage: bash test_run_tests.sh
# Exits 0 if all cases behave as expected, non-zero (with a diagnostic) if
# any regress.

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXROOT="$(mktemp -d)"
trap 'rm -rf "$FIXROOT"' EXIT

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

FAILURES=0

# make_fake_pytest DIR EXIT_CODE [OUTPUT_LINE]
# Creates DIR/.venv/bin/pytest as an executable stub that ignores its
# arguments and exits with EXIT_CODE. Used to deterministically drive
# run_module_tests through each branch without depending on a real
# Python venv or real pytest behavior.
make_fake_pytest() {
    local dir="$1" code="$2" msg="${3:-}"
    mkdir -p "${dir}/.venv/bin"
    cat > "${dir}/.venv/bin/pytest" <<EOF
#!/usr/bin/env bash
echo "${msg}"
exit ${code}
EOF
    chmod +x "${dir}/.venv/bin/pytest"
}

# assert_case DESC MODULE_PATH EXPECT_EXIT EXPECT_BANNER(yes/no) EXPECT_NEEDLE
#
# Drives the *real*, unmodified main() (arg validation, dispatch,
# run_module_tests, summary, exit code) end to end. MODULES is overridden
# post-source to point at the synthetic fixture module -- MODULES is plain
# configuration data, not logic, so this exercises production code paths.
assert_case() {
    local desc="$1" module="$2" expect_exit="$3" expect_banner="$4" expect_needle="$5"
    local out rc

    out=$(cd "$SCRIPT_DIR" && bash -c '
        source ./run_tests.sh
        MODULES=("$1")
        main "$1"
    ' _ "$module" 2>&1)
    rc=$?

    local ok=1

    if [ "$rc" -ne "$expect_exit" ]; then
        echo -e "${RED}FAIL${NC} [$desc]: expected exit code $expect_exit, got $rc"
        ok=0
    fi

    if [ "$expect_banner" = "yes" ]; then
        if ! grep -q "All tests completed successfully" <<<"$out"; then
            echo -e "${RED}FAIL${NC} [$desc]: expected success banner, none printed"
            ok=0
        fi
    else
        if grep -q "All tests completed successfully" <<<"$out"; then
            echo -e "${RED}FAIL${NC} [$desc]: success banner printed but must not be (zero-verification condition)"
            ok=0
        fi
    fi

    if [ -n "$expect_needle" ] && ! grep -qF "$expect_needle" <<<"$out"; then
        echo -e "${RED}FAIL${NC} [$desc]: expected output to contain: $expect_needle"
        echo "--- actual output ---"
        echo "$out"
        echo "----------------------"
        ok=0
    fi

    if [ "$ok" -eq 1 ]; then
        echo -e "${GREEN}PASS${NC} [$desc]"
    else
        FAILURES=$((FAILURES + 1))
    fi
}

# --- Fixture 1: module directory exists but has no .venv at all -------
# This is the exact bug reproduction: previously this yielded an
# unqualified "All tests completed successfully!" over zero tests run.
NO_VENV_MODULE="${FIXROOT}/no_venv_module"
mkdir -p "${NO_VENV_MODULE}/tests"
touch "${NO_VENV_MODULE}/tests/test_dummy.py"

assert_case \
    "missing venv (no pytest) must ERROR, not silently pass" \
    "$NO_VENV_MODULE" 1 no "ERROR"

# --- Fixture 2: venv/pytest present, but no tests/ or test/ directory --
NO_TESTDIR_MODULE="${FIXROOT}/no_testdir_module"
make_fake_pytest "$NO_TESTDIR_MODULE" 0

assert_case \
    "missing test directory must ERROR, not silently pass" \
    "$NO_TESTDIR_MODULE" 1 no "ERROR"

# --- Fixture 3: pytest runs but collects zero tests (exit code 5) -----
ZERO_COLLECT_MODULE="${FIXROOT}/zero_collect_module"
mkdir -p "${ZERO_COLLECT_MODULE}/tests"
make_fake_pytest "$ZERO_COLLECT_MODULE" 5 "collected 0 items"

assert_case \
    "pytest collecting zero tests must ERROR, not silently pass" \
    "$ZERO_COLLECT_MODULE" 1 no "collected zero tests"

# --- Fixture 4: a real passing run (including pytest-level skips) -----
# Standing in for the ONE approved skip pattern (SAPPHIRE_API_AVAILABLE
# dependency-gated skips): pytest itself exits 0 even though some
# individual tests were skipped inside a run that DID happen. This must
# still be treated as a pass.
PASS_MODULE="${FIXROOT}/pass_module"
mkdir -p "${PASS_MODULE}/tests"
make_fake_pytest "$PASS_MODULE" 0 "12 passed, 2 skipped"

assert_case \
    "a real passing run (with internal pytest skips) must still pass" \
    "$PASS_MODULE" 0 yes "tests passed"

# --- Fixture 5: pytest runs and reports real test failures ------------
FAIL_MODULE="${FIXROOT}/fail_module"
mkdir -p "${FAIL_MODULE}/tests"
make_fake_pytest "$FAIL_MODULE" 1 "3 passed, 1 failed"

assert_case \
    "genuine test failures must FAIL (distinct from ERROR), not silently pass" \
    "$FAIL_MODULE" 1 no "tests failed"

# --- Fixture 6: run_service_tests shares the same missing-venv bug class
# Called directly (not through main()) so this never touches the real
# sapphire/services/ directory tree. Confirms the service-side function
# was fixed identically to the module-side one.
service_out=$(cd "$SCRIPT_DIR" && bash -c '
    source ./run_tests.sh
    run_service_tests "no-such-service-fixture-zzz"
    echo "ERRORED_COUNT=${#ERRORED[@]}"
    echo "PASSED_COUNT=${#PASSED[@]}"
    echo "FAILED_COUNT=${#FAILED[@]}"
' 2>&1)

if grep -q "ERRORED_COUNT=1" <<<"$service_out" \
    && grep -q "PASSED_COUNT=0" <<<"$service_out" \
    && grep -q "FAILED_COUNT=0" <<<"$service_out" \
    && grep -qF "ERROR" <<<"$service_out"; then
    echo -e "${GREEN}PASS${NC} [run_service_tests: missing venv must ERROR, not silently pass]"
else
    echo -e "${RED}FAIL${NC} [run_service_tests: missing venv must ERROR, not silently pass]"
    echo "--- actual output ---"
    echo "$service_out"
    echo "----------------------"
    FAILURES=$((FAILURES + 1))
fi

echo ""
if [ "$FAILURES" -eq 0 ]; then
    echo -e "${GREEN}All test_run_tests.sh cases passed.${NC}"
    exit 0
else
    echo -e "${RED}${FAILURES} test_run_tests.sh case(s) failed.${NC}"
    exit 1
fi
