#!/usr/bin/env bash
# WF2 gate dispatcher for SAPPHIRE's per-module uv/pytest harness.
#
# SAPPHIRE has no flat repo-root pytest: each app module runs in its own uv
# .venv, and the tests use paths relative to the `apps/` directory — so pytest
# MUST be invoked from `apps/` (exactly like apps/run_tests.sh), else module-
# level fixtures asserting on config/GIS/template dirs fail at collection.
#
# Usage:
#   wf_gate.sh lint "<base ref>"                          # ruff only on .py CHANGED vs base
#   wf_gate.sh regression "<pytest --ignore=... flags>"   # exclude not-yet-passing locked tests
#   wf_gate.sh acceptance                                  # full suite incl. locked tests
#
# Exit 0 = passed; non-zero = failed.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APPS="$REPO_ROOT/apps"

# Modules this feature touches (iEH-HF discharge-aggregation parity).
MODULES=(iEasyHydroForecast preprocessing_runoff forecast_dashboard linear_regression)

# Operator-gated integration tests that require a live dashboard server (localhost:5055)
# + a chromium browser (playwright). They ERROR (not skip) headless and are an intentional
# operator-only gate — always excluded from the automated regression/acceptance gates.
ALWAYS_IGNORE="--ignore=forecast_dashboard/tests/test_integration.py"

MODE="${1:-}"
ARG2="${2:-}"          # lint: base ref;  regression: pytest --ignore flags

fail=0

# Echo "<module>/tests" or "<module>/test" (whichever exists), relative to apps/.
resolve_testdir() {
  local m="$1"
  if   [ -d "$APPS/$m/tests" ]; then echo "$m/tests"
  elif [ -d "$APPS/$m/test"  ]; then echo "$m/test"
  else echo ""; fi
}

case "$MODE" in
  lint)
    # Lint ONLY the Python files this milestone changed vs its base — never pre-existing debt.
    BASE="${ARG2:-origin/develop_preprocessing_runoff_iehhf_hydrograph_parity}"
    cd "$REPO_ROOT"
    mapfile -t changed < <(git diff --name-only --diff-filter=ACMR "${BASE}...HEAD" -- '*.py' 2>/dev/null | while read -r f; do [ -f "$f" ] && echo "$f"; done)
    if [ "${#changed[@]}" -eq 0 ]; then
      echo ">>> lint: no changed .py files vs ${BASE} — pass"
    else
      echo ">>> ruff check (changed only): ${changed[*]}"
      uvx ruff check "${changed[@]}" || fail=1
    fi
    ;;

  regression|acceptance)
    cd "$APPS"   # MUST mirror run_tests.sh: tests use paths relative to apps/
    # WF2 passes {testIgnore} as repo-relative paths; strip leading apps/ so they
    # resolve against the apps/-relative test dirs pytest is collecting.
    IGN="${ARG2//apps\//}"
    for m in "${MODULES[@]}"; do
      venv="$APPS/$m/.venv/bin/pytest"
      td="$(resolve_testdir "$m")"
      if [ ! -x "$venv" ]; then echo ">>> SKIP $m: no .venv (cd apps/$m && uv sync --all-extras)"; continue; fi
      if [ -z "$td" ];      then echo ">>> SKIP $m: no test/ or tests/ dir"; continue; fi
      echo "======================================================"
      echo ">>> $MODE: $m ($td)"
      echo "======================================================"
      if [ "$MODE" = "regression" ]; then
        # shellcheck disable=SC2086 -- IGN/ALWAYS_IGNORE are intentionally word-split into --ignore=... flags
        SAPPHIRE_TEST_ENV=True "$venv" "$td" $ALWAYS_IGNORE $IGN -q || fail=1
      else
        # shellcheck disable=SC2086
        SAPPHIRE_TEST_ENV=True "$venv" "$td" $ALWAYS_IGNORE -q || fail=1
      fi
    done
    ;;

  *)
    echo "wf_gate.sh: unknown mode '$MODE' (expected: lint | regression | acceptance)" >&2
    exit 2
    ;;
esac

exit $fail
