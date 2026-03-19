# ML hindcast_ML_models: API Write Order & Cleanup

**Status**: Review (implemented)
**Module**: `machine_learning`
**Priority**: Mid (consistency, not blocking operations)
**Labels**: `enhancement`, `api-migration`, `cleanup`
**Branch**: `develop_long_term_fix_api_postprocessing_forecasts`
**Prerequisite**: `high_prio_gi_draft_ml_csv_schema_corruption_fix.md` (for `ML_CANONICAL_CSV_COLUMNS`)

---

## Summary

`hindcast_ML_models.py` has several inconsistencies with the API-first patterns
established in `make_forecast.py` and the rest of the ML module:

| Issue | Current | Expected |
|-------|---------|----------|
| Write order | CSV first (line 483), API second (line 495) | API first, CSV second (match `make_forecast.py`) |
| Dead code | `PATH_TO_PAST_DISCHARGE` constructed but unused (lines 175-176) | Remove |
| Flag values | 3 (NaN), 4 (valid) | Document convention or align with make_forecast (0, 1, 2) |
| ERA5 reading | Already API-first via `read_meteo_data_combined()` | No change needed |
| CSV column normalization | Writes all 23 columns (correct) but no explicit guard | Add `normalize_ml_csv_columns()` for safety |
| Consistency check | None | Add optional `_check_ml_forecast_consistency()` to match make_forecast |

## ERA5 Meteo Data — No Change Needed

Both `make_forecast.py` and `hindcast_ML_models.py` already read ERA5 data via
API when `SAPPHIRE_API_ENABLED=true` (default). The `read_meteo_data_combined()`
function dispatches to `fl.read_meteo_data()` which checks the env var and calls
`_read_meteo_data_from_api()`. The CSV path parameters (`_control_member.csv` /
`_reanalysis.csv`) are fallback only — the API has no distinction between data
source types; both read from the same `Meteo` table.

The only behavioral difference between the two scripts:
- `make_forecast.py`: No `site_codes` -> reads all stations
- `hindcast_ML_models.py`: Passes `site_codes` -> reads filtered stations

Both are correct for their use case. No changes needed.

## Discharge Data — No Change Needed

`fl.read_daily_discharge_data()` (line 311) already uses the API-first pattern.
The `PATH_TO_PAST_DISCHARGE` variable (lines 175-176) is dead code — it was the
old CSV path but `fl.read_daily_discharge_data()` gets the path from env vars
internally.

---

## Implementation Plan

### Phase 1: Reverse Write Order (API first, CSV second)

**File**: `apps/machine_learning/hindcast_ML_models.py`

Currently (lines 483-510):
```python
# CSV write FIRST
hindecast_daily_df.to_csv(...)

# API write SECOND
if SAPPHIRE_API_AVAILABLE:
    try:
        ok = _write_ml_forecast_to_api(hindecast_daily_df, horizon, MODEL_TO_USE)
        ...
```

Change to match `make_forecast.py` pattern:
```python
# --- 1. Write to SAPPHIRE API (primary path) ---
if SAPPHIRE_API_AVAILABLE:
    try:
        horizon = "pentad" if HINDCAST_MODE == "PENTAD" else "decade"
        ok = _write_ml_forecast_to_api(hindecast_daily_df, horizon, MODEL_TO_USE)
        if ok:
            logger.info(
                "Wrote %d hindcast rows to API for %s %s",
                len(hindecast_daily_df),
                MODEL_TO_USE,
                HINDCAST_MODE,
            )
        else:
            logger.warning(
                "API write returned failure for hindcast %s %s",
                MODEL_TO_USE,
                HINDCAST_MODE,
            )
    except Exception as e:
        logger.error("Failed to write hindcast to API: %s", e)

# --- 2. Write to CSV (archive/fallback) ---
hindecast_daily_df.to_csv(
    os.path.join(
        OUTPUT_PATH_DISCHARGE,
        f"{MODEL_TO_USE}_{HINDCAST_MODE}_hindcast_daily_{start_date_string}_{end_date_string}.csv",
    ),
    index=False,
)
```

Note: The `horizon` variable must be computed before the API write block (currently
it's computed inside the `if SAPPHIRE_API_AVAILABLE` block at line 494 — move it
up to before the write section).

### Phase 2: Remove Dead Code

**File**: `apps/machine_learning/hindcast_ML_models.py`

Remove lines 175-180 (PATH_TO_PAST_DISCHARGE construction and existence check):
```python
# REMOVE:
PATH_TO_PAST_DISCHARGE = os.getenv("ieasyforecast_daily_discharge_file")
PATH_TO_PAST_DISCHARGE = os.path.join(intermediate_data_path, PATH_TO_PAST_DISCHARGE)
logger.debug("PATH_TO_PAST_DISCHARGE: %s", PATH_TO_PAST_DISCHARGE)
# Test if file exists
if not os.path.exists(PATH_TO_PAST_DISCHARGE):
    raise FileNotFoundError(f"File {PATH_TO_PAST_DISCHARGE} not found.")
```

This variable is never used — `fl.read_daily_discharge_data()` at line 311
gets its path from env vars internally.

**Risk**: Removing the existence check means the script won't fail early if the
CSV file is missing. However, `fl.read_daily_discharge_data()` defaults to
API reading, so the CSV file isn't needed unless `SAPPHIRE_API_ENABLED=false`.
If someone runs in CSV-only mode without the file, the error will surface at
line 311 instead of line 179 — still fails, just later. This is acceptable.

### Phase 3: Document Flag Convention

**File**: `apps/machine_learning/hindcast_ML_models.py`

Add a comment block before lines 463-465 documenting the flag convention:

```python
# Flag convention for hindcast:
#   3 = NaN in any quantile column (forecast failed for this date)
#   4 = valid forecast (all quantiles present)
# Note: make_forecast.py uses 0 (ok), 1 (NaN), 2 (error).
# Hindcast uses 3/4 to distinguish hindcast rows from operational rows
# when both are stored in the same API table.
```

This is NOT a bug — hindcast uses different flags intentionally so downstream
consumers can distinguish operational forecasts from hindcasts. Document, don't change.

### Phase 4: Tests

**File**: `apps/machine_learning/test/test_hindcast.py` (or existing test file)

**4a — Test write order**: Mock `_write_ml_forecast_to_api` and `pd.DataFrame.to_csv`.
Verify API write is called before CSV write.

**4b — Test dead code removal**: Verify the script doesn't reference
`PATH_TO_PAST_DISCHARGE` anywhere.

**4c — Test API write failure doesn't block CSV**: Mock `_write_ml_forecast_to_api`
to raise an exception. Verify CSV is still written.

### Phase 5: Verify End-to-End

1. Run ML tests: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning`
2. Verify all tests pass with zero skips
3. Review `git diff` for unintended changes

---

## Risks & Mitigations

### 1. Write order change could lose data if API write crashes mid-way (SAFE)

If the API write fails, the code catches the exception and logs it. CSV write
proceeds regardless. This matches `make_forecast.py` pattern where API failure
doesn't block CSV.

### 2. Dead code removal changes early-fail behavior (ACCEPTABLE)

Without the `PATH_TO_PAST_DISCHARGE` existence check, CSV-only mode won't fail
until `fl.read_daily_discharge_data()` tries to read the file. The error message
will be slightly different but the behavior is the same — script fails if CSV
is missing and API is disabled.

### 3. Flag convention (3/4 vs 0/1/2) is intentional (NO CHANGE)

Hindcast flags (3=NaN, 4=valid) differ from operational flags (0=ok, 1=NaN,
2=error) so that the API table can distinguish between the two sources. This
is documented, not fixed.

### 4. ERA5 API reading already works (VERIFIED)

`read_meteo_data_combined()` -> `fl.read_meteo_data()` -> `_read_meteo_data_from_api()`
when `SAPPHIRE_API_ENABLED=true`. The `_reanalysis.csv` / `_control_member.csv`
suffix is a local convention with no API equivalent. No changes needed.

### 5. 14 quantiles lost on API write (KNOWN LIMITATION)

`_write_ml_forecast_to_api()` only maps 5 quantiles (Q5, Q25, Q50, Q75, Q95).
The other 14 (Q10-Q90 minus those 5) are preserved only in the CSV. This is by
design — the API schema only has 5 quantile fields. The CSV serves as the
complete archive.

### 6. No service code changes needed (OWNERSHIP RESPECTED)

Everything stays within `apps/machine_learning/`. No changes to
`sapphire/services/`, no new API endpoints.

---

## Dependency Graph

```json
{
  "prerequisite": {
    "name": "high_prio_gi_draft_ml_csv_schema_corruption_fix.md",
    "reason": "Provides ML_CANONICAL_CSV_COLUMNS and normalize_ml_csv_columns()"
  },
  "phases": {
    "P1": {
      "name": "Reverse write order in hindcast_ML_models.py (API first, CSV second)",
      "file": "apps/machine_learning/hindcast_ML_models.py",
      "depends_on": [],
      "parallel_group": "A"
    },
    "P2": {
      "name": "Remove dead code (PATH_TO_PAST_DISCHARGE)",
      "file": "apps/machine_learning/hindcast_ML_models.py",
      "depends_on": [],
      "parallel_group": "A",
      "note": "Independent of P1, different lines in same file"
    },
    "P3": {
      "name": "Document flag convention (3/4 vs 0/1/2)",
      "file": "apps/machine_learning/hindcast_ML_models.py",
      "depends_on": [],
      "parallel_group": "A"
    },
    "P4": {
      "name": "Write tests for write order and dead code removal",
      "file": "apps/machine_learning/test/test_hindcast.py",
      "depends_on": [],
      "parallel_group": "A",
      "note": "Tests can be written in parallel (mock-based, don't depend on code changes)"
    },
    "P5": {
      "name": "Run full test suite, verify end-to-end",
      "depends_on": ["P1", "P2", "P3", "P4"],
      "parallel_group": "B"
    }
  },
  "execution_order": [
    {"parallel": ["P1", "P2", "P3", "P4"]},
    {"sequential": ["P5"]}
  ]
}
```
