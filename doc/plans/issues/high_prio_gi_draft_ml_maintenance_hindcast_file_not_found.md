# ML maintenance mode: hindcast subprocess failure not handled, causes FileNotFoundError

**Status**: Draft
**Module**: machine_learning
**Priority**: High
**Labels**: `bug`, `maintenance-mode`, `error-handling`

---

## Summary

The ML maintenance script (`recalculate_nan_forecasts.py`) does not abort when the hindcast subprocess fails, causing a `FileNotFoundError` when it tries to read a CSV that was never produced.

## Context

The ML maintenance pipeline runs nightly via `bin/daily_ml_maintenance.sh`. For each model+horizon combination (e.g. TFT-PENTAD, TIDE-DECAD), it launches a Docker container that runs `recalculate_nan_forecasts.py`. This script:

1. Reads existing forecasts and finds entries with NaN values (flag 1 or 2)
2. Calls `hindcast_ML_models.py` as a subprocess to produce a hindcast CSV
3. Reads the hindcast CSV and merges results back into the forecast file

This is failing consistently (observed 2026-02-11 on the production server) for multiple model+horizon combinations.

## Problem

**Observed behavior:** Every model+horizon combination fails with:
```
FileNotFoundError: [Errno 2] No such file or directory:
  '../../../kyg_data_forecast_tools/intermediate_data/predictions/hindcast/TFT/TFT_PENTAD_hindcast_daily_2026-01-26_2026-02-11.csv'
```

**Root cause:** In `call_hindcast_script()` (line 99-107), the subprocess return code is checked and the failure is printed, but execution **continues unconditionally** to line 122 where it tries to `pd.read_csv()` on the output file. Since the hindcast subprocess failed, the CSV was never created.

**Why the hindcast subprocess fails** is a separate question that needs investigation with Sandro. The likely root cause is an incomplete migration to API — some sub-processes (e.g. `hindcast_ML_models.py`) may still expect CSV files that no longer exist now that data flows through the SAPPHIRE API. The immediate issue here is that the error handling doesn't short-circuit.

**Secondary issue:** The bash script (`daily_ml_maintenance.sh`) logs the container exit code (line 290) but does not flag the overall run as failed. The script always exits with "ML maintenance run completed successfully" (line 312), even when every single container failed.

## Desired Outcome

1. `call_hindcast_script()` raises an exception (or returns None) when the subprocess fails, so the caller can handle it gracefully
2. `recalculate_nan_forecasts()` logs the failure clearly and exits without crashing on FileNotFoundError
3. The bash maintenance script reports overall success/failure accurately
4. (Separate investigation) Determine why `hindcast_ML_models.py` itself is failing

---

## Technical Analysis

### Current Implementation

**Key files:**
- `apps/machine_learning/recalculate_nan_forecasts.py:65-124` - `call_hindcast_script()` function
- `apps/machine_learning/recalculate_nan_forecasts.py:251-256` - call site in `recalculate_nan_forecasts()`
- `bin/daily_ml_maintenance.sh:289-293` - exit code logging (informational only, no failure handling)
- `bin/daily_ml_maintenance.sh:300-312` - cleanup and "success" message

### Root Cause

In `call_hindcast_script()`, lines 99-107 check the subprocess return code:

```python
if result.returncode == 0:
    print("Hindcast ran successfully!")
    print()
else:
    print("Hindcast failed with return code", result.returncode)
    print("Error output:")
    print(result.stderr)
    print()
```

But then lines 112-122 run unconditionally:

```python
OUTPUT_PATH_DISCHARGE = os.getenv('ieasyhydroforecast_OUTPUT_PATH_DISCHARGE')
PATH_FORECAST = os.path.join(intermediate_data_path, OUTPUT_PATH_DISCHARGE)
PATH_HINDCAST = os.path.join(PATH_FORECAST, 'hindcast', MODEL_TO_USE)
file_name = f'{MODEL_TO_USE}_{PREDICTION_MODE}_hindcast_daily_{min_missing_date}_{max_missing_date}.csv'
hindcast = pd.read_csv(os.path.join(PATH_HINDCAST, file_name))
```

There is no early return, no exception raised, and no guard before the `read_csv`.

---

## Implementation Plan

### Approach

Minimal fix: raise an exception from `call_hindcast_script()` when the subprocess fails, and handle it in the caller. Also fix the bash script to track and report failures.

### Files to Modify

| File | Changes |
|------|---------|
| `apps/machine_learning/recalculate_nan_forecasts.py` | Add early return/exception on subprocess failure in `call_hindcast_script()` |
| `bin/daily_ml_maintenance.sh` | Track per-service exit codes, report overall success/failure at end |

### Implementation Steps

- [ ] Step 1: In `call_hindcast_script()` (line 103-107), raise an exception when `result.returncode != 0` instead of just printing
- [ ] Step 2: In `recalculate_nan_forecasts()` (line 251-256), catch the exception from step 1, log the error, and exit gracefully (return without crashing)
- [ ] Step 3: In `daily_ml_maintenance.sh`, add a failure counter that increments when a container exits non-zero (around line 290), and use it to set the final exit message and exit code (line 300-312)
- [ ] Step 4: Write tests for `call_hindcast_script()` failure path
- [ ] Step 5: (Separate investigation with Sandro) Determine why `hindcast_ML_models.py` itself is failing — is it a data issue, model issue, or environment issue?

### Code Examples

**Step 1 — raise on subprocess failure:**

```python
# In call_hindcast_script(), replace lines 99-107:
if result.returncode == 0:
    logger.info("Hindcast ran successfully")
else:
    logger.error(
        "Hindcast failed with return code %s. Stderr: %s",
        result.returncode, result.stderr
    )
    raise RuntimeError(
        f"Hindcast subprocess failed with return code "
        f"{result.returncode} for {MODEL_TO_USE} {PREDICTION_MODE}"
    )
```

**Step 2 — graceful handling in caller:**

```python
# In recalculate_nan_forecasts(), wrap the call at line 251:
try:
    hindcast = call_hindcast_script(
        min_missing_date=min_date,
        max_missing_date=max_date,
        MODEL_TO_USE=MODEL_TO_USE,
        intermediate_data_path=intermediate_data_path,
        codes_with_nan=codes_with_nan,
        PREDICTION_MODE=PREDICTION_MODE,
    )
except RuntimeError as e:
    logger.error("Aborting recalculation: %s", e)
    return
```

**Step 3 — bash script failure tracking:**

```bash
# After line 60, add:
FAILURE_COUNT=0

# At line 290, after capturing exit_code:
if [ "$exit_code" -ne 0 ]; then
    log_message "FAILED: $service exited with code $exit_code"
    FAILURE_COUNT=$((FAILURE_COUNT + 1))
fi

# Replace line 312:
if [ $FAILURE_COUNT -gt 0 ]; then
    log_message "WARNING: ML maintenance completed with $FAILURE_COUNT failures out of $TOTAL_SERVICES services"
    exit 1
else
    log_message "ML maintenance run completed successfully"
fi
```

---

## Testing

### Test Cases

- [ ] Test that `call_hindcast_script()` raises `RuntimeError` when subprocess returns non-zero exit code
- [ ] Test that `recalculate_nan_forecasts()` catches the error and returns gracefully (no crash, no traceback)
- [ ] Test that existing behavior (successful hindcast) is unchanged

### Testing Commands

```bash
cd apps
SAPPHIRE_TEST_ENV=True pytest machine_learning/test -v -k "test_recalculate"
```

### Manual Verification

1. Run maintenance mode locally with a deliberately broken hindcast (e.g. missing model file)
2. Verify: Python script logs the error and exits with code 1 (not with a traceback)
3. Verify: Bash script reports the failure in its summary

---

## Out of Scope

- Investigating **why** `hindcast_ML_models.py` fails (Step 5 — separate investigation with Sandro)
- Adding retry logic for transient failures
- Notification system for maintenance failures (e.g. email/Slack alerts)

## Dependencies

- Step 5 requires Sandro's input on the hindcast model failures. Likely root cause: incomplete CSV→API migration in `hindcast_ML_models.py` and related sub-processes. Investigate in a separate session.

## Acceptance Criteria

- [ ] When hindcast subprocess fails, `recalculate_nan_forecasts.py` logs a clear error message and exits cleanly (no `FileNotFoundError` traceback)
- [ ] The bash maintenance script exits with code 1 when any service fails, and logs a summary of failures
- [ ] Existing successful hindcast flow is unaffected
- [ ] New tests cover the failure path
- [ ] Code follows project conventions

---

## References

- Error logs from production server, 2026-02-11
- Related: LR-001 (linear regression maintenance mode — completed)
