# PP-006: Remove deprecated POSTPROCESSING_GAPFILL_WINDOW_DAYS references

**Status**: Done (2026-03-05)
**Module**: postprocessing_forecasts
**Priority**: Low
**Labels**: `cleanup`, `maintenance-mode`

---

## Summary

The original premise of this issue (deduplicate a hardcoded default across 3
files by creating a `config.yaml`) is **obsolete**.
`postprocessing_maintenance.py` now uses `POSTPROCESSING_GAPFILL_MAX_MONTHS`
(default `13`) instead of `POSTPROCESSING_GAPFILL_WINDOW_DAYS`. The Python
side already deprecates the old env var with a warning (lines 86-90).

The remaining work is removing stale `GAPFILL_WINDOW_DAYS` references from
shell scripts and the pipeline orchestrator.

## Context

The gap-fill lookback was migrated from a days-based window to a months-based
window. The Python entry point (`postprocessing_maintenance.py:86-93`) now:
- Logs a deprecation warning if `POSTPROCESSING_GAPFILL_WINDOW_DAYS` is set
- Uses `POSTPROCESSING_GAPFILL_MAX_MONTHS` (default `13`) for the actual
  lookback calculation

Three files still reference the deprecated env var with a hardcoded default:

| File | Line | Current code |
|------|------|-------------|
| `bin/daily_postprc_maintenance.sh` | 119 | `POSTPROCESSING_GAPFILL_WINDOW_DAYS=${POSTPROCESSING_GAPFILL_WINDOW_DAYS:-7}` |
| `apps/run_locally.sh` | 630 | `POSTPROCESSING_GAPFILL_WINDOW_DAYS=${POSTPROCESSING_GAPFILL_WINDOW_DAYS:-7}` |
| `apps/pipeline/pipeline_docker.py` | 1861 | `"POSTPROCESSING_GAPFILL_WINDOW_DAYS=7"` |

These pass a value that the Python code ignores (it uses `GAPFILL_MAX_MONTHS`
instead). They should be removed or replaced with the new env var.

## Desired Outcome

1. No references to `POSTPROCESSING_GAPFILL_WINDOW_DAYS` outside of the
   Python deprecation guard and its tests
2. Shell scripts pass `POSTPROCESSING_GAPFILL_MAX_MONTHS` if an override is
   needed (or omit it entirely and rely on the Python default of 13)

---

## Implementation Plan

### Step 1: Update `bin/daily_postprc_maintenance.sh` (line 119)

Remove the `POSTPROCESSING_GAPFILL_WINDOW_DAYS` line. Optionally add
`POSTPROCESSING_GAPFILL_MAX_MONTHS` only if the operator needs to override
the default (pass-through pattern):

```bash
${POSTPROCESSING_GAPFILL_MAX_MONTHS:+-e POSTPROCESSING_GAPFILL_MAX_MONTHS=${POSTPROCESSING_GAPFILL_MAX_MONTHS}} \
```

### Step 2: Update `apps/run_locally.sh` (line 630)

Same pattern — remove the deprecated var, optionally pass the new one.

### Step 3: Update `apps/pipeline/pipeline_docker.py` (line 1861)

Remove `"POSTPROCESSING_GAPFILL_WINDOW_DAYS=7"` from the environment list.

### Step 4: Update `apps/postprocessing_forecasts/README.md`

Update the env var table: mark `POSTPROCESSING_GAPFILL_WINDOW_DAYS` as
deprecated (or remove it) and document `POSTPROCESSING_GAPFILL_MAX_MONTHS`.

### Step 5: Verify

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
grep -rn "GAPFILL_WINDOW_DAYS" . --include="*.py" --include="*.sh"
# Should only appear in the deprecation guard + its test
```

---

## Risk

Minimal. The deprecated env var is already ignored by the Python code. Removing
the shell-script references simply stops passing a value that has no effect.

## Scope

Shell-script and pipeline cleanup only. No Python logic changes. The
`config.yaml` approach from the original plan is no longer needed — the
single env var `POSTPROCESSING_GAPFILL_MAX_MONTHS` with a Python-side default
is sufficient for a single setting.
