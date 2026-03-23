# PREPG-003: Snow operational API write discards all data due to wall-clock-anchored 2-day window

**Status**: Draft
**Module**: `preprocessing_gateway`
**Priority**: High
**Labels**: `bug`, `data-integrity`, `api-migration`, `snow-data`
**Assigned to**: @sandrohuni

---

## Summary

`snow_data_operational.py` writes snow SWE/HS/RoF data to the preprocessing API using a 2-day window anchored to **wall clock time** (`date >= yesterday`). The SnowMapper Data Gateway model output typically lags 3-7 days behind real-time. Because the latest DG data is always older than yesterday, `write_snow_to_api()` discards every record and the API is never updated.

The CSV archive is correctly updated on every run (it receives all data). Only the API write path is broken. Downstream consumers reading from the API (primarily `long_term_forecasting/data_interface.py`) see only climatological norms (from `recalculate_snow_norms.py`) with `value=NULL` for all dates.

**Impact**: 6 of 8 long-term forecast models produce NaN output because their snow features are NULL. This cascades into MC_ALD (UncertaintyMixture) failing silently. See LTF-003.

---

## Problem Statement

### The 2-day operational window

In `dg_utils.py`, `write_snow_to_api()` lines 504-507:

```python
yesterday = ref - pd.Timedelta(days=1)
if sync_mode == "operational":
    data_to_write = data[data["date"] >= yesterday]
```

When called from `snow_data_operational.py:380`:
```python
written = dg_utils.write_snow_to_api(df_combined, variable, hru)
```

No `reference_date` is passed, so `ref` defaults to `pd.Timestamp.today().normalize()` (wall clock). The README describes this as "guards against data lag from the Data Gateway" — but the guard only covers a 1-day lag, while the actual DG lag is 3-7 days.

### The reanalysis script already has the fix

`snow_data_renalysis.py` lines 357-363 calls:
```python
dg_utils.write_snow_to_api(
    df_combined, variable, hru,
    mode="maintenance",
    reference_date=df_combined["date"].max()
)
```

By anchoring `reference_date` to the data's actual latest date, the 30-day maintenance window correctly covers whatever the DG returned — regardless of wall-clock lag.

### Diagnostic evidence

When the operational window is empty, `write_snow_to_api` logs (lines 521-532):
```
WARNING: No snow data for {yesterday} to {today} ({snow_type}, HRU {hru}).
CSV date range: {min_date} to {max_date}. Data gateway may not have returned recent data yet.
```

This warning fires on **every operational run** but is not treated as an error by the caller. The function returns `False`, which `get_snow_data_operational()` silently accepts (line 382 only checks for `True` to run consistency check).

---

## Proposed Fix

### Fix 1 (Required): Anchor operational window to data, not wall clock

**File**: `apps/preprocessing_gateway/snow_data_operational.py`
**Line**: 380

Change:
```python
written = dg_utils.write_snow_to_api(df_combined, variable, hru)
```

To:
```python
written = dg_utils.write_snow_to_api(
    df_combined, variable, hru,
    reference_date=df_combined["date"].max()
)
```

This keeps the operational mode's 2-day window but anchors it to the **latest date in the actual data** rather than the wall clock. If the DG returned data up to March 17, the window becomes `[March 16, March 17]` instead of `[March 22, March 23]`.

**Why not switch to `mode="maintenance"`**: The 30-day maintenance window would work but would re-upsert 30 days of records on every daily run. The 2-day window is correct for operational use — it just needs to be anchored correctly.

**Why this matches the reanalysis pattern**: `snow_data_renalysis.py` already passes `reference_date=df_combined["date"].max()`. This fix makes the operational script consistent.

### Fix 2 (Recommended): Log a warning when API write returns False

**File**: `apps/preprocessing_gateway/snow_data_operational.py`
**Lines**: 379-387

Currently, `write_snow_to_api` returning `False` is silently accepted. Add explicit logging:

```python
written = dg_utils.write_snow_to_api(
    df_combined, variable, hru,
    reference_date=df_combined["date"].max()
)
if not written:
    logger.warning(
        "Snow API write returned False for %s HRU %s — "
        "API may not have latest data",
        variable, hru,
    )
```

### Fix 3 (Optional): Wrap transform_snow_data in try/except

**File**: `apps/preprocessing_gateway/snow_data_operational.py`
**Line**: 340

`transform_snow_data()` is not wrapped in a try/except. A bad date format or non-numeric value in the DG CSV would crash the entire script. Consider wrapping with error logging and `continue` to the next HRU/variable combination.

---

## Implementation Plan

### Phase 1: Apply Fix 1 (the one-line fix)

**File**: `apps/preprocessing_gateway/snow_data_operational.py`, line 380
**Change**: Add `reference_date=df_combined["date"].max()`

### Phase 2: Apply Fix 2 (logging improvement)

**File**: Same file, lines 379-387
**Change**: Add warning log when `written` is `False`

### Phase 3: Verify

1. Run `snow_data_operational.py` locally with an env pointing to a real DG instance
2. Check logs — the "No snow data for..." warning should no longer appear
3. Query the preprocessing API: `curl "http://localhost:8000/api/preprocessing/snow/?code=15189&snow_type=SWE&limit=5"` — should now return records with `value` populated (not just `norm`)
4. Run long_term_forecasting and verify that models using snow features now get non-NULL SWE values

### Phase 4: Tests

Add a unit test in `apps/preprocessing_gateway/test/` that:
1. Creates a mock DG CSV with data ending 5 days ago
2. Calls `get_snow_data_operational()` or the write path
3. Asserts that `write_snow_to_api` is called with `reference_date` matching the data's max date
4. Asserts that the data written to the API includes the most recent DG records (not an empty set)

---

## Acceptance Criteria

- [ ] `write_snow_to_api` is called with `reference_date=df_combined["date"].max()` in the operational path
- [ ] The "No snow data for..." WARNING no longer fires during normal operational runs (when DG returns data within its expected 3-7 day lag)
- [ ] Snow records in the preprocessing API have non-NULL `value` fields for recent dates (within DG lag window)
- [ ] Long-term forecasting models that use snow features receive non-NULL SWE data from the API
- [ ] Existing norms are not clobbered by the operational write (verify `norm` column preserved)
- [ ] No changes to `sapphire/services/` (ownership boundary respected)

---

## Secondary Issue: Norm Clobber Risk

In `write_snow_to_api`, `_read_existing_norms()` (line 555) reads back norms from the API before writing, to avoid clobbering. However, if this read **fails** (network error, API timeout), it returns `{}` silently (line 416), and the subsequent write sends `norm=None` for all records — overwriting valid norms.

This is a separate issue (lower priority) but worth noting:
- The service-side `crud.create_snow` does a blind `setattr` update for all fields including `norm` (no `exclude_none`)
- The protection depends entirely on `_read_existing_norms` succeeding

**Mitigation options** (not in scope for this fix):
- Add `exclude_none=True` to `model_dump()` in the service upsert (requires colleague coordination)
- Or: retry `_read_existing_norms` on failure before proceeding with the write

---

## Related Issues

- **LTF-003**: `run_forecast.py` sets flag=0 on null forecasts — downstream impact of this bug
- **PREPG-002**: Snow SWE data not updated (observation from 2026-03-19) — this issue IS that observation
- **PREPG-001**: Yearly snow norm recalculation — the norms that DO appear in the API come from this

---

## Source

Discovered during observation triage on 2026-03-19 (observations.md entry "Preprocessing Gateway: Snow SWE Data Not Updated by Operational Run"). Root cause analysis completed 2026-03-23.

---

## Dependency Graph

```json
{
  "phases": {
    "phase_1": {
      "title": "Anchor operational window to data max date",
      "file": "apps/preprocessing_gateway/snow_data_operational.py",
      "changes": ["Add reference_date=df_combined['date'].max() at line 380"],
      "depends_on": [],
      "parallel_with": ["phase_2"]
    },
    "phase_2": {
      "title": "Add warning log when API write returns False",
      "file": "apps/preprocessing_gateway/snow_data_operational.py",
      "changes": ["Add logger.warning when written is False"],
      "depends_on": [],
      "parallel_with": ["phase_1"]
    },
    "phase_3": {
      "title": "Verify fix end-to-end",
      "depends_on": ["phase_1", "phase_2"],
      "parallel_with": []
    },
    "phase_4": {
      "title": "Add unit test for reference_date anchoring",
      "file": "apps/preprocessing_gateway/test/test_snow_api_window.py",
      "depends_on": ["phase_1"],
      "parallel_with": ["phase_3"]
    }
  },
  "execution_groups": [
    {"group": 1, "parallel": true, "phases": ["phase_1", "phase_2"]},
    {"group": 2, "parallel": true, "phases": ["phase_3", "phase_4"]}
  ]
}
```
