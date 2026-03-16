# ML-004: Hindcast gap-fill never persists to API — silent write failure

**Status**: Review — Bugs A-D fixed in fill_ml_gaps.py and recalculate_nan_forecasts.py. Bug E tracked separately as ML-007. Verified by 3 pipeline runs (2026-03-13/14).
**Module**: machine_learning
**Priority**: Critical
**Labels**: `bug`, `api-integration`, `maintenance-mode`, `data-loss`

---

## Summary

The ML hindcast gap-fill (`fill_ml_gaps.py`) detects forecast gaps correctly and
generates hindcasts to fill them, but the filled data **never reaches the API**.
Every maintenance run re-detects the same gaps (2024 to present) because the API
write path either crashes before it executes or fails silently. The CSV write
works, but because gap detection reads from the API (with CSV as fallback), the
filled data is invisible to subsequent runs.

## Symptom

Every run of `fill_ml_gaps.py` reports the same gaps:
```
Missing forecasts for code XXXXX from 2024-XX-XX to 2026-XX-XX
```
The log never shows `"Successfully wrote X ML forecast records to SAPPHIRE API"`.

---

## Root Cause Analysis

Investigation confirmed:
- `sapphire-api-client` **IS** a required dependency (`pyproject.toml:26`) — the
  package is installed, so `SAPPHIRE_API_AVAILABLE = True`.
- The operational path (`make_forecast.py`) uses the same fire-and-forget API
  write pattern — return value ignored there too.
- `recalculate_nan_forecasts.py` has an identical API write block (lines 334-343).

Four bugs compound to create the failure loop:

### Bug A (Critical): Hindcast crash prevents API write from executing

When the hindcast subprocess fails, `call_hindcast_script()` prints the error
(lines 103-106) but **does not abort**. It proceeds unconditionally to
`pd.read_csv()` at line 122 on a file that was never created → `FileNotFoundError`.

This exception propagates through `fill_ml_gaps()` (no try/except around the
call at line 286), **crashing the function before the API write block at lines
324-333 is ever reached**. This explains why the user sees zero API write log
messages — not even the misleading ones.

**Code path:**
```
fill_ml_gaps.py:286  call_hindcast_script(...)
  → hindcast_ML_models.py fails (subprocess returncode != 0)
  → call_hindcast_script:103 prints "Hindcast failed" but continues
  → call_hindcast_script:122  pd.read_csv(nonexistent_file) → FileNotFoundError
  → propagates to fill_ml_gaps:286 → function crashes
  → lines 316-333 (CSV write + API write) NEVER EXECUTE
```

**Note**: ML-001 covers making `call_hindcast_script()` raise cleanly on
subprocess failure. This issue covers making the API write block **reachable
and observable** even when errors occur upstream.

### Bug B (Medium): API write return value ignored + misleading log

Even when the hindcast succeeds and line 324 IS reached, the API write is
fire-and-forget (`fill_ml_gaps.py:329`):

```python
_write_ml_forecast_to_api(filled_df, horizon_type, MODEL_TO_USE)
logger.info(f"Wrote {len(filled_df)} gap-filled forecasts to API")  # ALWAYS fires
```

The return value (`bool`) is discarded. The success log fires regardless of
whether `_write_ml_forecast_to_api()` returned `False` (API not ready, disabled,
empty records, etc.). The inner function logs at each gate, but the caller's log
is misleading.

`make_forecast.py` (lines 195-204, 240-249) and `recalculate_nan_forecasts.py`
(lines 334-343) have the **same pattern** — return value ignored everywhere.

### Bug C (Medium): Null-discharge filter is dead code

`fill_ml_gaps.py:221-228`:
```python
if "forecasted_discharge" in forecast.columns:  # never True
```

After `_read_ml_forecasts_from_api()` renames `forecasted_discharge` → `Q50`
(at `utils_ml_forecast.py:619`), this column doesn't exist. The CSV path also
uses `Q50` (`BaseDartsDLPredictor.create_prediction_df()` creates `Q{int}`
columns). The filter is dead code on **both** paths.

**Impact**: Phantom null-discharge rows from API are not filtered and could mask
real gaps. Latent bug — will bite when null rows enter the API.

### Bug D (Critical): Duplicate records cause PostgreSQL CardinalityViolation

`_write_ml_forecast_to_api()` sends all records in a single batch INSERT. If the
input DataFrame contains duplicate rows for the same unique key
(`horizon_type`, `code`, `model_type`, `date`, `target`), PostgreSQL raises:

```
CardinalityViolation: ON CONFLICT DO UPDATE command cannot affect row a second time
HINT: Ensure that no rows proposed for insertion within the same command have
      duplicate constrained values.
```

This is the **direct cause of the 500 error** observed in `make_forecast.py`.
Duplicates can enter the DataFrame from:
- Twin virtual gauge logic copying predictions (if same code appears)
- Multiple hindcast runs producing overlapping date ranges in `fill_ml_gaps.py`
- `recalculate_nan_forecasts.py` re-processing already-existing dates

The fix must deduplicate records by unique key before the API call, keeping the
last occurrence (most recent data wins). Both `_write_ml_forecast_to_api()` and
`_write_ml_daily_forecast_to_api()` in `utils_ml_forecast.py` are affected.

### Bug E (Medium): Non-deterministic API pagination causes inconsistent gap detection

`_read_ml_forecasts_from_api()` reads ALL codes in a single paginated query
without an `ORDER BY` guarantee from the API endpoint. With ~55 codes × 730
days of data, the DB returns pages in arbitrary order. Two consecutive runs
of the same query for code 16101 returned different date ranges — one included
2026 data, the other didn't.

**Impact**: Gap detection is unreliable. Some runs miss recent operational
records, so gaps appear to "end" early and the full gap window is not detected.

**Root cause**: The PostGreSQL query in `sapphire/services/postprocessing/`
has no explicit `ORDER BY` on the forecast endpoint.

**Fix**: Requires coordination with `sapphire/services/` owner (ownership
boundary). Options:
1. Add `ORDER BY id` to the forecast query endpoint (API-side)
2. Client-side: read per-code instead of all-at-once (expensive but reliable)
3. Client-side: increase page size to exceed total record count (fragile)

This bug is **out of scope** for ML-004 but should be tracked separately.

---

## The Failure Loop

```
Run N:
  fill_ml_gaps.py starts
    → Reads from API: gets some forecasts, sees gap 2024→today
    → Runs hindcast subprocess
    → SCENARIO 1: Subprocess fails
        → call_hindcast_script crashes with FileNotFoundError
        → CSV write never happens, API write never happens
        → Next run: same gaps
    → SCENARIO 2: Subprocess succeeds
        → Writes filled data to CSV ✓
        → API write block reached
        → _write_ml_forecast_to_api() called, returns False (e.g. API not ready)
        → Return value ignored, logs "Wrote N gap-filled forecasts" anyway
        → Next run: API still empty → same gaps detected

Run N+1:
  → API read returns data WITHOUT gap-filled rows (they were never written)
  → CSV fallback NOT triggered (API returned non-empty data)
  → Same gaps detected → cycle repeats
```

---

## Implementation Plan

### Approach

1. Wrap the hindcast+write section in proper error handling so the API write
   block is always reachable (both files)
2. Reorder write path: API write first (primary), CSV as deprecated fallback
3. Make every API write decision point observable in logs
4. Fix the null-discharge filter in `fill_ml_gaps.py`

### Files to Modify

| File | Changes |
|------|---------|
| `apps/machine_learning/fill_ml_gaps.py` | Wrap hindcast call in try/except; fix null-discharge filter; check API write return value; reorder to API-first write; log all decision points |
| `apps/machine_learning/recalculate_nan_forecasts.py` | Wrap hindcast call in try/except; check API write return value; reorder to API-first write; log all decision points |

### Phases

#### Phase 1: Fix `fill_ml_gaps.py` (3 sub-tasks)

**1a. Fix null-discharge filter column name (line 221-228)**

The column is `Q50` in both API and CSV paths. Use `Q50` directly:

```python
# Before (dead code — column never exists):
if "forecasted_discharge" in forecast.columns:
    n_nulls = forecast["forecasted_discharge"].isna().sum()
    if n_nulls > 0:
        logger.info(...)
        forecast = forecast[forecast["forecasted_discharge"].notna()].copy()

# After:
if "Q50" in forecast.columns:
    n_nulls = forecast["Q50"].isna().sum()
    if n_nulls > 0:
        logger.info(
            "fill_ml_gaps: Excluding %d null-discharge rows from gap detection",
            n_nulls,
        )
        forecast = forecast[forecast["Q50"].notna()].copy()
```

**1b. Wrap hindcast call in try/except (lines 285-322)**

Ensure the CSV write and API write blocks are reachable even if the hindcast
fails partway. If `call_hindcast_script()` raises, log the error and return
early (CSV was not written, nothing to persist to API).

```python
# Wrap the hindcast call so the function doesn't crash:
try:
    hindcast = call_hindcast_script(
        min_missing_date, max_missing_date,
        MODEL_TO_USE, intermediate_data_path, PREDICTION_MODE,
    )
except (FileNotFoundError, RuntimeError) as exc:
    logger.error(
        "fill_ml_gaps: Hindcast failed for %s %s (%s to %s): %s. "
        "Gap-fill aborted — gaps will persist until hindcast succeeds.",
        MODEL_TO_USE, PREDICTION_MODE,
        min_missing_date, max_missing_date, exc,
    )
    return
```

**Note**: This is a minimal guard in `fill_ml_gaps()`. ML-001 covers making
`call_hindcast_script()` itself fail cleanly.

**1c. Reorder to API-first write + observability (lines 316-333)**

The current code writes CSV first and API second. Since the codebase is
transitioning to API-primary (CSV is deprecated fallback), reverse the order:
API write first, then CSV write as safety net.

Replace the entire write section (lines 316-333) with:

```python
        # --- Write gap-filled forecasts: API first, CSV as deprecated fallback ---
        api_write_ok = False
        if len(all_filled_forecasts) > 0:
            filled_df = pd.concat(all_filled_forecasts, axis=0)
        else:
            filled_df = pd.DataFrame()

        # 1. API write (primary)
        if SAPPHIRE_API_AVAILABLE and not filled_df.empty:
            try:
                horizon_type = "pentad" if prefix == "pentad" else "decade"
                success = _write_ml_forecast_to_api(
                    filled_df, horizon_type, MODEL_TO_USE
                )
                if success:
                    logger.info(
                        "fill_ml_gaps: Wrote %d gap-filled forecasts to API",
                        len(filled_df),
                    )
                    api_write_ok = True
                else:
                    logger.error(
                        "fill_ml_gaps: API write returned False for %d "
                        "gap-filled rows (%s %s). Will fall back to CSV.",
                        len(filled_df), MODEL_TO_USE, horizon_type,
                    )
            except Exception as e:
                logger.error(
                    "fill_ml_gaps: Exception writing gap-filled forecasts "
                    "to API: %s. Will fall back to CSV.", e
                )
        elif not SAPPHIRE_API_AVAILABLE:
            logger.warning(
                "fill_ml_gaps: sapphire_api_client not available — "
                "gap-filled forecasts will be saved to CSV only (deprecated)."
            )
        elif filled_df.empty:
            logger.info(
                "fill_ml_gaps: No gap-filled forecast rows to write"
            )

        # 2. CSV write (deprecated fallback — will be removed)
        forecast["forecast_date"] = pd.to_datetime(forecast["forecast_date"])
        forecast = forecast.sort_values(by="forecast_date")
        forecast.to_csv(
            os.path.join(
                PATH_FORECAST, prefix + "_" + MODEL_TO_USE + "_forecast.csv"
            ),
            index=False,
        )
        if not api_write_ok and not filled_df.empty:
            logger.warning(
                "fill_ml_gaps: Gap-filled data saved to CSV only (deprecated). "
                "Gaps will be re-detected on next API-primary run."
            )
```

#### Phase 2: Fix `recalculate_nan_forecasts.py` (2 sub-tasks)

**2a. Wrap hindcast call in try/except (line 271-278)**

Same bug as `fill_ml_gaps.py`: if `call_hindcast_script()` raises
`FileNotFoundError` (subprocess failed → CSV not created), the entire
function crashes and lines 280-343 (update + CSV write + API write) never
execute.

```python
# Wrap the hindcast call so the function doesn't crash:
try:
    hindcast = call_hindcast_script(
        min_missing_date=min_date,
        max_missing_date=max_date,
        MODEL_TO_USE=MODEL_TO_USE,
        intermediate_data_path=intermediate_data_path,
        codes_with_nan=codes_with_nan,
        PREDICTION_MODE=PREDICTION_MODE,
    )
except (FileNotFoundError, RuntimeError) as exc:
    logger.error(
        "recalculate_nan_forecasts: Hindcast failed for %s %s (%s to %s): %s. "
        "NaN recalculation aborted — NaN forecasts will persist until "
        "hindcast succeeds.",
        MODEL_TO_USE, PREDICTION_MODE, min_date, max_date, exc,
    )
    return
```

**2b. Reorder to API-first write + observability (lines 325-343)**

Same reorder as Phase 1c: API write first (primary), CSV as deprecated
fallback. Replace the entire write section (lines 325-343) with:

```python
    # --- Write recalculated forecasts: API first, CSV as deprecated fallback ---
    api_write_ok = False

    # 1. API write (primary)
    if SAPPHIRE_API_AVAILABLE and len(hindcast) > 0:
        try:
            horizon_type = "pentad" if prefix == "pentad" else "decade"
            success = _write_ml_forecast_to_api(hindcast, horizon_type, MODEL_TO_USE)
            if success:
                logger.info(
                    "recalculate_nan_forecasts: Wrote %d recalculated "
                    "forecasts to API",
                    len(hindcast),
                )
                api_write_ok = True
            else:
                logger.error(
                    "recalculate_nan_forecasts: API write returned False "
                    "for %d rows (%s %s). Will fall back to CSV.",
                    len(hindcast), MODEL_TO_USE, horizon_type,
                )
        except Exception as e:
            logger.error(
                "recalculate_nan_forecasts: Exception writing recalculated "
                "forecasts to API: %s. Will fall back to CSV.", e
            )
    elif not SAPPHIRE_API_AVAILABLE:
        logger.warning(
            "recalculate_nan_forecasts: sapphire_api_client not available — "
            "recalculated forecasts will be saved to CSV only (deprecated)."
        )

    # 2. CSV write (deprecated fallback — will be removed)
    forecast["forecast_date"] = pd.to_datetime(forecast["forecast_date"])
    forecast = forecast.sort_values(by="forecast_date")
    forecast.to_csv(
        os.path.join(
            PATH_FORECAST, prefix + "_" + MODEL_TO_USE + "_forecast.csv"
        ),
        index=False,
    )
    if not api_write_ok and len(hindcast) > 0:
        logger.warning(
            "recalculate_nan_forecasts: Recalculated data saved to CSV only "
            "(deprecated). NaN flags will persist in API until next successful write."
        )
```

#### Phase 3: Add tests

Add tests to `apps/machine_learning/test/`. Build on existing patterns from
`test_api_integration.py` which already mocks `SapphirePostprocessingClient`.

**No conftest.py exists** in the ML test directory — create one if shared
fixtures are needed, otherwise keep tests self-contained.

**Test file**: `apps/machine_learning/test/test_fill_ml_gaps.py` (new file)

| # | Test | Asserts |
|---|------|---------|
| 1 | Null-discharge filter with Q50 column | Rows with `Q50=NaN` excluded from gap detection |
| 2 | Null-discharge filter with no Q50 column (empty DF) | No crash, no filtering |
| 3 | API write succeeds → return value True → success logged | Log contains "Wrote N gap-filled" |
| 4 | API write returns False → error logged | Log contains "API write returned False" |
| 5 | SAPPHIRE_API_AVAILABLE is False → warning logged | Log contains "not available" |
| 6 | all_filled_forecasts is list of empty DataFrames → warning | Log contains "empty DataFrame" |
| 7 | Hindcast call raises FileNotFoundError → caught, logged, returns | Log contains "Hindcast failed", no crash |
| 8 | API write exception → caught and logged | Log contains "Exception writing" |

**Test file**: `apps/machine_learning/test/test_recalculate_nan_api_write.py` (new file)

| # | Test | Asserts |
|---|------|---------|
| 9 | Hindcast call raises FileNotFoundError → caught, logged, returns | Log contains "Hindcast failed", no crash |
| 10 | API write succeeds → success logged | Same pattern as test 3 |
| 11 | API write returns False → error logged | Same pattern as test 4 |
| 12 | SAPPHIRE_API_AVAILABLE is False → warning logged | Log contains "not available" |

**Mocking strategy** (from existing `test_api_integration.py`):
- `@patch("scr.utils_ml_forecast.SapphirePostprocessingClient")`
- `Mock()` client with `readiness_check()` and `write_forecasts()`
- Skip with `pytest.skip("sapphire-api-client not installed")` when
  `SAPPHIRE_API_AVAILABLE is False`

#### Phase 4: Run full test suite

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning
```

All tests must pass with zero skips (except the valid `SAPPHIRE_API_AVAILABLE`
dependency gate).

---

## Acceptance Criteria

- [x] Null-discharge filter correctly uses `Q50` column (not `forecasted_discharge`)
- [x] `call_hindcast_script()` failure is caught in both `fill_ml_gaps()` and
      `recalculate_nan_forecasts()` — functions log error and return cleanly
      instead of crashing with FileNotFoundError
- [x] Return value of `_write_ml_forecast_to_api()` is checked in both
      `fill_ml_gaps.py` and `recalculate_nan_forecasts.py`
- [x] API write is the primary write path; CSV write is a deprecated fallback
- [x] Success log only appears on confirmed success (return True)
- [x] Failure log appears with actionable message when return is False
- [x] When `SAPPHIRE_API_AVAILABLE` is False, a WARNING is logged
- [x] All existing tests pass; new tests cover the failure paths
- [x] No changes to `sapphire/services/` (ownership boundary)

---

## Out of Scope

- Making `call_hindcast_script()` raise cleanly on subprocess failure (ML-001)
- Root cause of hindcast subprocess failures (ML-002)
- Migrating remaining CSV reads to API-primary (ML-003)
- Fixing the same pattern in `make_forecast.py` (operational path — lower risk
  because it runs daily and the data is also written to CSV which is read back)
- Adding retry logic for transient API failures
- Read-back verification after write (deferred — adds latency and complexity;
  the observability fixes here will reveal whether writes succeed or fail)
- Bug E: Non-deterministic API pagination in `_read_ml_forecasts_from_api()`
  (requires `sapphire/services/` coordination — tracked separately)

## Related Issues

- **ML-001**: Hindcast subprocess failure not handled (FileNotFoundError) —
  this issue adds a guard in the caller; ML-001 fixes the root cause
- **ML-002**: Root cause of hindcast subprocess failures — separate
- **ML-003**: Migrate maintenance scripts to API-primary reads — overlaps on
  read side; this issue covers the **write** side
- **INFRA-007**: ML forecast API reader alignment — Phase 3 cleanup related

## References

- `apps/machine_learning/fill_ml_gaps.py` — primary file with all bugs
- `apps/machine_learning/recalculate_nan_forecasts.py` — same API write pattern
- `apps/machine_learning/scr/utils_ml_forecast.py:629-716` — `_write_ml_forecast_to_api()`
- `apps/machine_learning/scr/utils_ml_forecast.py:530-626` — `_read_ml_forecasts_from_api()`
- `apps/machine_learning/scr/BaseDartsDLPredictor.py:221-246` — column naming (`Q50`)
- `apps/machine_learning/test/test_api_integration.py` — existing mock patterns
- `apps/machine_learning/make_forecast.py:195-204` — operational path (same pattern)

---

## Dependency Graph

```json
{
  "phases": {
    "phase_1a": {
      "title": "Fix null-discharge filter in fill_ml_gaps.py",
      "file": "apps/machine_learning/fill_ml_gaps.py",
      "lines": "221-228",
      "depends_on": [],
      "parallel_with": ["phase_1b", "phase_1c", "phase_2a", "phase_2b"]
    },
    "phase_1b": {
      "title": "Wrap hindcast call in try/except in fill_ml_gaps.py",
      "file": "apps/machine_learning/fill_ml_gaps.py",
      "lines": "285-292",
      "depends_on": [],
      "parallel_with": ["phase_2a", "phase_2b"],
      "note": "Touches same file as 1a and 1c — run as single agent with all three"
    },
    "phase_1c": {
      "title": "Reorder to API-first write + observability in fill_ml_gaps.py",
      "file": "apps/machine_learning/fill_ml_gaps.py",
      "lines": "316-333",
      "depends_on": [],
      "parallel_with": ["phase_2a", "phase_2b"],
      "note": "Touches same file as 1a and 1b — run as single agent with all three"
    },
    "phase_2a": {
      "title": "Wrap hindcast call in try/except in recalculate_nan_forecasts.py",
      "file": "apps/machine_learning/recalculate_nan_forecasts.py",
      "lines": "271-278",
      "depends_on": [],
      "parallel_with": ["phase_1a", "phase_1b", "phase_1c"],
      "note": "Touches same file as 2b — run as single agent with both"
    },
    "phase_2b": {
      "title": "Reorder to API-first write + observability in recalculate_nan_forecasts.py",
      "file": "apps/machine_learning/recalculate_nan_forecasts.py",
      "lines": "325-343",
      "depends_on": [],
      "parallel_with": ["phase_1a", "phase_1b", "phase_1c"],
      "note": "Touches same file as 2a — run as single agent with both"
    },
    "phase_3": {
      "title": "Add tests for both fixed files",
      "files": [
        "apps/machine_learning/test/test_fill_ml_gaps.py",
        "apps/machine_learning/test/test_recalculate_nan_api_write.py"
      ],
      "depends_on": ["phase_1a", "phase_1b", "phase_1c", "phase_2a", "phase_2b"]
    },
    "phase_4": {
      "title": "Run full ML test suite",
      "command": "cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning",
      "depends_on": ["phase_3"]
    }
  },
  "execution_groups": [
    {
      "group": 1,
      "parallel": true,
      "agents": [
        {
          "id": "agent_fill_ml_gaps",
          "phases": ["phase_1a", "phase_1b", "phase_1c"],
          "reason": "Same file — must be single agent to avoid merge conflicts"
        },
        {
          "id": "agent_recalculate_nan",
          "phases": ["phase_2a", "phase_2b"],
          "reason": "Same file — must be single agent; safe to parallelize with agent_fill_ml_gaps"
        }
      ]
    },
    {
      "group": 2,
      "parallel": false,
      "agents": [
        {
          "id": "agent_tests",
          "phases": ["phase_3"],
          "reason": "Tests must be written against the fixed code"
        }
      ]
    },
    {
      "group": 3,
      "parallel": false,
      "agents": [
        {
          "id": "agent_validation",
          "phases": ["phase_4"],
          "reason": "Final validation after all changes"
        }
      ]
    }
  ]
}
```
