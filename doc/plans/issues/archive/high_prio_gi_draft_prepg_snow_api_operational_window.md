# PREPG-003: Snow operational API write discards all data due to wall-clock-anchored 2-day window

**Status**: Closed (Not a Bug)
**Module**: `preprocessing_gateway`
**Priority**: ~~High~~
**Labels**: `bug`, `data-integrity`, `api-migration`, `snow-data`

### Resolution (2026-03-24)

Investigation with actual DG data confirmed this is **not a bug**. The SnowMapper Data Gateway operational endpoint returns both reanalysis rows (lagged 3-7 days) AND forecast rows extending ~9 days beyond the last reanalysis date. The forecast rows cover dates at and beyond today, so the wall-clock-anchored `date >= yesterday` filter in `write_snow_to_api()` correctly captures ~10 forecast dates per run. The API write succeeds and the preprocessing database contains non-NULL snow values for both reanalysis and forecast dates.

Evidence from 2026-03-24 pipeline run:
- DG CSV: 327 reanalysis rows (2025-03-24 to 2026-03-17) + 15 forecast rows (2026-03-18 to 2026-04-01)
- `Source` column values: `SnowMapper-Reanalysis` and `SnowMapper=Forecast-2026-03-23-00`
- API query confirmed: SWE values populated through 2026-04-01 for station 15189
- The `transform_snow_data()` output preserves all 342 data rows (630 after expanding 63 station codes)

The original diagnosis assumed the DG returned only reanalysis data. The forecast rows were not accounted for.

---

## Summary

`snow_data_operational.py` writes snow SWE/HS/RoF data to the preprocessing API using a 2-day window anchored to **wall clock time** (`date >= yesterday`). The SnowMapper Data Gateway model output typically lags 3-7 days behind real-time. Because the latest reanalysis data is always older than yesterday, `write_snow_to_api()` discards every record and the API is never updated.

The CSV archive is correctly updated on every run (it receives all data including forecasts). Only the API write path is broken. Downstream consumers reading from the API (primarily `long_term_forecasting/data_interface.py`) see only climatological norms (from `recalculate_snow_norms.py`) with `value=NULL` for all dates.

**Impact**: 6 of 8 long-term forecast models produce NaN output because their snow features are NULL. This cascades into MC_ALD (UncertaintyMixture) failing silently. See LTF-003.

---

## Data Gateway Output Structure

The DG `get_operational()` endpoint (`api/calculations/snow-operational/...`) returns a CSV named `control_spinup_and_forecast_{hru}_{date}.csv` containing **both reanalysis and forecast rows**:

- **Reanalysis rows**: Past dates — SnowMapper model output (observations/spinup). Lag 3-7 days behind wall clock.
- **Forecast rows**: Future dates — SnowMapper model predictions for upcoming days.
- **`Source` column**: Distinguishes reanalysis from forecast rows. Currently stripped by `transform_snow_data()` (line 239 in `dg_utils.py`).

### Intended behavior: write all data, let upsert handle overwrites

Both reanalysis and forecast rows should be written to the preprocessing API. The DB upsert on `(snow_type, code, date)` ensures that on subsequent runs, newer reanalysis data naturally **overwrites** older forecast data for the same dates. This matches the conceptual pattern used for meteo data in `Quantile_Mapping_OP.py`.

### Comparison with meteo data flow (`Quantile_Mapping_OP.py`)

| Aspect | Meteo (P, T) | Snow (SWE, HS, RoF) |
|--------|-------------|---------------------|
| DG endpoint | `get_control_spinup_and_forecast()` | `get_operational()` |
| CSV contains | Spinup + forecast | Reanalysis + forecast |
| DG data lag | None (ECMWF IFS is current) | 3-7 days (SnowMapper lag) |
| API write window | `[yesterday, today]` wall-clock | `[yesterday, ∞)` wall-clock |
| Wall-clock anchor works? | **Yes** — data is current | **No** — data lags wall clock |
| API write function | `_write_meteo_to_api()` (local) | `dg_utils.write_snow_to_api()` |

The meteo write succeeds because ECMWF IFS data has no lag — today's data is always present in the `[yesterday, today]` window. The snow write fails because SnowMapper lags 3-7 days behind.

### Available DG endpoints (for reference)

The `sapphire_dg_client` provides three snow endpoints:

| Client method | DG endpoint | Used by | Returns |
|---------------|-------------|---------|---------|
| `get_snow_reanalysis()` | `snow-reanalysis` | `snow_data_renalysis.py` | Historical archive only |
| `get_operational()` | `snow-operational` | `snow_data_operational.py` | Reanalysis + forecast mixed |
| `get_snow_forecast()` | `snow-forecast` | **Nothing** | Dedicated forecast data |

---

## Problem Statement

### The 2-day operational window

In `dg_utils.py`, `write_snow_to_api()` lines 504-507:

```python
yesterday = ref - pd.Timedelta(days=1)
if sync_mode == "operational":
    # Include yesterday, today, and any forecast dates beyond today
    data_to_write = data[data["date"] >= yesterday]
```

When called from `snow_data_operational.py:380`:
```python
written = dg_utils.write_snow_to_api(df_combined, variable, hru)
```

No `reference_date` is passed, so `ref` defaults to `pd.Timestamp.today().normalize()` (wall clock). The comment on line 506 states the intent: *"Include yesterday, today, and any forecast dates beyond today"*. The design is correct — the `date >= yesterday` filter with no upper bound is meant to capture both recent reanalysis and all forecast rows. But the wall-clock anchor means "yesterday" is wall-clock yesterday, which is always ahead of the 3-7 day lagged reanalysis data.

### What the data looks like vs. what the window captures

Example: wall clock is March 24. DG returns reanalysis up to March 17 and forecasts for March 18-30.

| Data type | Date range | In `date >= Mar 23`? |
|-----------|-----------|---------------------|
| Reanalysis | ... to Mar 17 | **No** — missed |
| Forecast | Mar 18 to Mar 30 | Only Mar 23-30 |

The window misses all reanalysis AND the first 5 forecast days.

### The reanalysis script's approach

`snow_data_renalysis.py` uses `mode="maintenance"` with a 30-day window and `reference_date=df_combined["date"].max()`. This works for reanalysis-only data but is unnecessarily broad for daily operational use.

### Diagnostic evidence

When the operational window is empty, `write_snow_to_api` logs (lines 521-532):
```
WARNING: No snow data for {yesterday} to {today} ({snow_type}, HRU {hru}).
CSV date range: {min_date} to {max_date}. Data gateway may not have returned recent data yet.
```

This warning fires on **every operational run** but is not treated as an error by the caller. The function returns `False`, which `get_snow_data_operational()` silently accepts (line 382 only checks for `True` to run consistency check).

---

## Proposed Fix

### Fix 1 (Required): Write freshly-fetched data to API instead of windowing the merged CSV

**File**: `apps/preprocessing_gateway/snow_data_operational.py`
**Lines**: 378-386

Currently, the merged `df_combined` (entire CSV history + new data) is passed to `write_snow_to_api()`, which then applies a 2-day window. This is wrong for two reasons:
1. The window is wall-clock-anchored (the core bug)
2. Even with a corrected window, passing the full history means the window logic determines what gets written — fragile and indirect

**The fix**: Pass only the freshly-fetched `df_transformed` (the new data from this DG call) to the API write, not the merged `df_combined`. This writes exactly the data returned by the DG — both reanalysis and forecast rows — to the API via upsert. No windowing needed.

Change (lines 378-386):
```python
# Write to SAPPHIRE API (if enabled)
try:
    written = dg_utils.write_snow_to_api(df_combined, variable, hru)
    # Run consistency check only if data was actually written
    if written:
        _check_snow_consistency(df_combined, variable, hru)
except SapphireAPIError as e:
    logger.error("Error writing snow data to API (HRU %s, %s): %s", hru, variable, e)
    # Continue - CSV write succeeded, API failure is not fatal
```

To:
```python
# Write to SAPPHIRE API (if enabled)
# Write the freshly-fetched data (reanalysis + forecast) directly.
# The DB upsert on (snow_type, code, date) ensures that on subsequent
# runs, newer reanalysis data overwrites older forecast values for
# the same dates.
try:
    written = dg_utils.write_snow_to_api(
        df_transformed, variable, hru,
        mode="initial",
    )
    if not written:
        logger.warning(
            "Snow API write returned False for %s HRU %s — "
            "API may not have latest data",
            variable, hru,
        )
    else:
        _check_snow_consistency(df_transformed, variable, hru)
except SapphireAPIError as e:
    logger.error("Error writing snow data to API (HRU %s, %s): %s", hru, variable, e)
    # Continue - CSV write succeeded, API failure is not fatal
```

**Why `mode="initial"`**: The "initial" sync mode skips all date filtering and writes every row. Since we're already passing only the freshly-fetched data (not the full history), no windowing is needed — the upsert handles deduplication.

**Why this is safe**: The operational DG response is small (spinup period + forecast period, typically a few hundred rows per HRU/variable). Writing all of them on each run is not expensive.

**Why this matches the intended behavior**: The comment on `dg_utils.py:506` says *"Include yesterday, today, and any forecast dates beyond today"*. Writing all freshly-fetched data achieves exactly this — without depending on window arithmetic.

### Fix 2 (Already included above): Log a warning when API write returns False

Included in Fix 1 — the `if not written` warning is part of the new code block.

### Fix 3 (Optional): Wrap transform_snow_data in try/except

**File**: `apps/preprocessing_gateway/snow_data_operational.py`
**Line**: 340

`transform_snow_data()` is not wrapped in a try/except. A bad date format or non-numeric value in the DG CSV would crash the entire script. Consider wrapping with error logging and `continue` to the next HRU/variable combination.

---

## Implementation Plan

### Phase 1: Change API write to use `df_transformed` with `mode="initial"`

**File**: `apps/preprocessing_gateway/snow_data_operational.py`, lines 378-386
**Change**: Pass `df_transformed` instead of `df_combined`; use `mode="initial"`; add warning log when `written` is False.

### Phase 2: Verify

1. Run `snow_data_operational.py` locally with an env pointing to a real DG instance
2. Check logs — the "No snow data for..." warning should no longer appear
3. Query the preprocessing API: `curl "http://localhost:8000/api/preprocessing/snow/?code=15189&snow_type=SWE&limit=5"` — should now return records with `value` populated (not just `norm`), including both reanalysis and forecast dates
4. Run long_term_forecasting and verify that models using snow features now get non-NULL SWE values
5. Verify that on the next run, reanalysis values overwrite previously-written forecast values for overlapping dates

### Phase 3: Tests

Add a unit test in `apps/preprocessing_gateway/test/` that:
1. Creates a mock DG CSV with reanalysis rows (ending 5 days ago) and forecast rows (future dates)
2. Calls the write path with the freshly-fetched data
3. Asserts that `write_snow_to_api` is called with `mode="initial"` and receives the fresh data (not the merged history)
4. Asserts that the data written to the API includes both reanalysis and forecast records
5. Simulates a second run where reanalysis covers dates that were previously forecast — asserts the upsert overwrites correctly

---

## Acceptance Criteria

- [ ] `write_snow_to_api` receives only the freshly-fetched DG data (`df_transformed`), not the full merged CSV
- [ ] `mode="initial"` is used so no date windowing is applied
- [ ] Both reanalysis and forecast snow records appear in the preprocessing API
- [ ] The "No snow data for..." WARNING no longer fires during normal operational runs
- [ ] On subsequent runs, reanalysis values overwrite previously-written forecast values (upsert behavior verified)
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

Discovered during observation triage on 2026-03-19 (observations.md entry "Preprocessing Gateway: Snow SWE Data Not Updated by Operational Run"). Root cause analysis completed 2026-03-23. Updated 2026-03-24 after comparing with meteo data flow in `Quantile_Mapping_OP.py`: all DG data (reanalysis + forecast) should be written to the API, with upserts handling the overwrite cycle.

---

## Dependency Graph

```json
{
  "phases": {
    "phase_1": {
      "title": "Write df_transformed with mode=initial to API",
      "file": "apps/preprocessing_gateway/snow_data_operational.py",
      "changes": [
        "Pass df_transformed instead of df_combined to write_snow_to_api",
        "Use mode='initial' to skip date windowing",
        "Add logger.warning when written is False"
      ],
      "depends_on": [],
      "parallel_with": []
    },
    "phase_2": {
      "title": "Verify fix end-to-end",
      "depends_on": ["phase_1"],
      "parallel_with": ["phase_3"]
    },
    "phase_3": {
      "title": "Add unit test for fresh-data API write",
      "file": "apps/preprocessing_gateway/test/test_snow_api_window.py",
      "depends_on": ["phase_1"],
      "parallel_with": ["phase_2"]
    }
  },
  "execution_groups": [
    {"group": 1, "parallel": false, "phases": ["phase_1"]},
    {"group": 2, "parallel": true, "phases": ["phase_2", "phase_3"]}
  ]
}
```
