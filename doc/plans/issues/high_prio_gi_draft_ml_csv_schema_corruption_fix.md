# Fix ML Forecast CSV Schema Corruption & Restore Historical Data

**Status**: Review (Phases 1-2 implemented; Phase 3 is server-side data restoration)
**Module**: `machine_learning`
**Priority**: Critical (blocks DB reset & migration)
**Labels**: `bug`, `data-integrity`, `csv-corruption`, `api-migration`
**Branch**: `develop_long_term_fix_api_postprocessing_forecasts`

---

## Summary

Commit `884dda6` (2026-03-05, ML-003) introduced a bug where `fill_ml_gaps.py`
reads forecasts from the API (which returns a 30-column API schema with only 5
quantiles) and writes that directly to the ML prediction CSVs, overwriting the
original 23-column format that has all 19 quantiles. This corrupted the CSVs and
broke the data migrator (which expects the old format).

## Root Cause

Three compounding bugs create a corruption loop:

1. **`_read_ml_forecasts_from_api()`** in `utils_ml_forecast.py` renames 5 quantile
   columns but does **not drop** API-only columns (`horizon_type`, `model_type`,
   `id`, `model_type_description`, `composition`, `horizon_value`, `horizon_in_year`).

2. **`fill_ml_gaps.py`** line 394 and **`recalculate_nan_forecasts.py`** line 415
   write the full API-schema DataFrame to CSV without any column filtering.

3. **`make_forecast.py`** `write_pentad_forecast()` and `write_decad_forecast()`
   read the corrupted CSV, concat with fresh predictions (producing a 30-column
   union), and write it back — **propagating corruption on every daily run**.

### The Corruption Loop

```
fill_ml_gaps reads from API → gets 30-column schema
  → writes to CSV (corrupted)
    → make_forecast reads CSV, concats with 23-col fresh predictions
      → writes 30-col union back to CSV
        → fill_ml_gaps reads CSV on next fallback → still corrupted
```

### CSV Fallback Bypass (Critical)

When the API is unavailable, `fill_ml_gaps.py` (line 200) and
`recalculate_nan_forecasts.py` (line 204) fall back to `pd.read_csv()` on the
**same corrupted CSV**, completely bypassing `_read_ml_forecasts_from_api()`.
Fixing only the API reader is insufficient — the CSV fallback path must also
be normalized.

## Canonical ML CSV Schema

The ML prediction CSV must always use this 23-column format:

```
Q5, Q10, Q15, Q20, Q25, Q30, Q35, Q40, Q45, Q50, Q55, Q60, Q65, Q70, Q75, Q80, Q85, Q90, Q95, date, code, forecast_date, flag
```

This is what:
- `make_forecast.py` produces from model predictions
- `hindcast_ML_models.py` produces (same 23 columns via `create_prediction_df()`)
- The data migrator expects (reads Q5, Q25, Q50, Q75, Q95, code, forecast_date, date, flag)
- All downstream consumers need

## Current State of CSV Files

| Model | Current decad (corrupted) | Old decad (conflicted copy, correct format) | Current pentad (corrupted) | Old pentad |
|-------|--------------------------|---------------------------------------------|---------------------------|------------|
| TFT   | 512k lines, 2024-03→2026-03 | 3.4M lines, 2009-12→2025-10 | 409k lines, 2024-03→2026-03 | **None** |
| TiDE  | 522k lines, 2024-03→2026-03 | 3.4M lines, 2009-12→2025-10 | 280k lines, 2024-03→2026-03 | **None** |
| TSMixer | 209k lines, 2024-03→2026-03 | 3.4M lines, 2009-12→2025-10 | 180k lines, 2024-03→2026-03 | **None** |

---

## Implementation Plan

### Phase 1: Define Canonical Columns & Fix API Reader

**File**: `apps/machine_learning/scr/utils_ml_forecast.py`

**Step 1a** — Define the canonical column constant (single source of truth):

```python
ML_CANONICAL_CSV_COLUMNS = [
    "Q5", "Q10", "Q15", "Q20", "Q25", "Q30", "Q35", "Q40", "Q45",
    "Q50", "Q55", "Q60", "Q65", "Q70", "Q75", "Q80", "Q85", "Q90",
    "Q95", "date", "code", "forecast_date", "flag",
]
```

**Step 1b** — In `_read_ml_forecasts_from_api()` (after the rename block at
lines 613-623), add a column filter:

```python
canonical = [c for c in ML_CANONICAL_CSV_COLUMNS if c in df.columns]
df = df[canonical]
```

This drops `horizon_type`, `model_type`, `id`, `model_type_description`,
`composition`, `horizon_value`, `horizon_in_year`. The returned DataFrame will
have only the 5 quantiles the API provides plus the 4 identifier columns.

**Callers affected** (all safe — none use dropped columns):
- `fill_ml_gaps.py` (line 187) — uses Q50, forecast_date, date, code
- `recalculate_nan_forecasts.py` (line 191) — uses Q-columns, forecast_date, date, code
- `add_new_station.py` — uses forecast_date, date, code

**Test updates**: `test_api_integration.py::TestReadMLForecastsFromAPI::test_returns_dataframe_on_success`
— assert API-only columns are NOT in the returned DataFrame.

### Phase 2: Fix All Three CSV Writers

This phase addresses all three scripts that write to the ML forecast CSVs.
A shared helper function normalizes any DataFrame to the canonical schema.

**Step 2a** — Add a helper in `utils_ml_forecast.py`:

```python
def normalize_ml_csv_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Filter DataFrame to canonical ML CSV columns, preserving column order."""
    return df[[c for c in ML_CANONICAL_CSV_COLUMNS if c in df.columns]]
```

**Step 2b** — Fix `fill_ml_gaps.py`:
- Import `normalize_ml_csv_columns` and `ML_CANONICAL_CSV_COLUMNS`
- **Before CSV write** (line 394): `forecast = normalize_ml_csv_columns(forecast)`
- **After CSV fallback read** (line 200): `forecast = normalize_ml_csv_columns(forecast)`

**Step 2c** — Fix `recalculate_nan_forecasts.py`:
- Import `normalize_ml_csv_columns`
- **Before CSV write** (line 415): `forecast = normalize_ml_csv_columns(forecast)`
- **After CSV fallback read** (line 204): `forecast = normalize_ml_csv_columns(forecast)`

**Step 2d** — Fix `make_forecast.py`:
- Import `normalize_ml_csv_columns`
- In `write_pentad_forecast()`: before `.to_csv()` (line 194), apply
  `forecast_combined = normalize_ml_csv_columns(forecast_combined)`
- In `write_decad_forecast()`: before `.to_csv()` (line 244), apply
  `forecast_combined = normalize_ml_csv_columns(forecast_combined)`

This is the **most critical fix** — `make_forecast.py` runs daily and is the
primary corruption propagator. Even if fill_ml_gaps is fixed, the daily runs
keep writing the corrupted schema until this is fixed.

**Test updates**:
- `test_fill_ml_gaps.py::TestApiFirstWrite` — assert CSV write has no API-only columns
- `test_write_forecast.py` — add test: corrupted old CSV with extra columns →
  verify output CSV has only canonical columns
- `test_recalculate_nan_api_write.py` — assert CSV write has no API-only columns

### Phase 3: Restore Decad CSVs from Conflicted Copies + Current Data

**This is a data operation using a one-off Python script (not committed).**

For each model (TFT, TiDE, TSMixer):

1. Back up the current corrupted file (rename to `.bak_corrupted`)
2. Read the old conflicted copy with pandas (correct 23-column format, 2009→2025-10)
3. Read the current corrupted file with pandas, filter to canonical columns
   (drops API-only columns; Q10-Q90 will be NaN for API-sourced rows)
4. Extract current rows with `forecast_date > 2025-10-21` (only these are
   missing from the conflicted copy)
5. Concat old + new-only, deduplicate on `(forecast_date, date, code)`, keep last
6. Write the merged file as the primary CSV
7. Verify: header matches canonical schema, row count >= old conflicted copy

**Important**: Read with pandas by column names, not positions. The old files
have `forecast_date` as column 22, the current as column 4.

**Pentad CSVs**: No old copies exist. After code fixes (Phases 1-2), the next
operational `make_forecast.py` run will start producing clean pentad CSVs.
Historical pentad data requires running `hindcast_ML_models.py` separately
(not part of this plan — and the data migrator only does decad anyway).

### Phase 4: Run Tests and Verify End-to-End

1. Run ML tests: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning`
2. Verify restored CSV headers: `head -1 predictions/*/decad_*_forecast.csv`
3. Verify no API-only columns in any ML CSV
4. Spot-check data migrator compatibility (verify it can parse the restored CSVs)

---

## Critical Review: Potential Issues & Mitigations

### 1. NaN quantiles in merged data (ACCEPTABLE)

After Phase 3, rows from 2025-10-22 to 2026-03-12 will have NaN for Q10-Q20,
Q30-Q45, Q55-Q90 (these came from the API which only stores 5 quantiles).

**Not a problem**: The data migrator, postprocessing pipeline, and combined
forecast builder all only use Q5, Q25, Q50, Q75, Q95. The 14 extra quantiles
are a bonus when produced by fresh model runs.

### 2. recalculate_nan_forecasts.py quantile detection (NEEDS ATTENTION)

`recalculate_nan_forecasts.py` line 336 uses
`value_cols = [col for col in forecast_code.columns if "Q" in col]` to find
columns needing recalculation. After our fix, when reading from API, the
DataFrame will have only Q5, Q25, Q50, Q75, Q95 (not the full 19). But when
reading from a clean CSV, it will have all 19.

**This is actually correct**: The recalculation only updates values that exist
in the forecast. If only 5 quantiles exist, only 5 get updated. The hindcast
produces all 19, and `update_forecast()` merges by column intersection. No
data loss.

### 3. CSV fallback path normalization prevents silent corruption (NEW)

The original plan only normalized the CSV write. This revision also normalizes
the CSV fallback READ in both `fill_ml_gaps.py` and `recalculate_nan_forecasts.py`.
This breaks the corruption loop even when the API is down: corrupted CSV →
read → normalize → only canonical columns in memory → write → clean CSV.

### 4. `_write_ml_daily_forecast_to_api()` — dead code (LEAVE ALONE)

Defined but never called. Has a duplicate `model_type_map`. Do not wire in
during this fix — separate cleanup ticket.

### 5. Column constant is single source of truth (NO DUPLICATION)

`ML_CANONICAL_CSV_COLUMNS` and `normalize_ml_csv_columns()` defined once in
`utils_ml_forecast.py`, imported by all three consumer files.

### 6. No service code changes (OWNERSHIP RESPECTED)

Everything stays within `apps/machine_learning/`. No changes to
`sapphire/services/`, no new API endpoints or schema changes.

### 7. old_forecast in make_forecast.py (SAFE)

`old_forecast` (line 661) is only used for discharge imputation in
`prepare_forecast_data()` — it looks up Q50 values for missing discharge dates.
Extra columns in a corrupted CSV are ignored. Fresh model predictions
(`forecast_pentad`/`forecast_decad`) passed to the write functions come directly
from `predictor.predict()` with the clean 23-column schema.

### 8. Hindcast output format (VERIFIED SAFE)

`hindcast_ML_models.py` produces the canonical 23-column format via
`create_prediction_df()` (19 quantiles + date, code, forecast_date, flag).
It writes to a separate `hindcast/{MODEL}/` subdirectory — no collision with
the operational CSV files.

---

## Dependency Graph

```json
{
  "phases": {
    "P1": {
      "name": "Define ML_CANONICAL_CSV_COLUMNS, normalize_ml_csv_columns(), fix _read_ml_forecasts_from_api()",
      "file": "apps/machine_learning/scr/utils_ml_forecast.py",
      "depends_on": [],
      "parallel_group": "A"
    },
    "P2": {
      "name": "Fix CSV writers + CSV fallback reads in fill_ml_gaps, recalculate_nan, make_forecast",
      "files": [
        "apps/machine_learning/fill_ml_gaps.py",
        "apps/machine_learning/recalculate_nan_forecasts.py",
        "apps/machine_learning/make_forecast.py"
      ],
      "depends_on": ["P1"],
      "parallel_group": "B",
      "note": "All three files import from P1. Steps 2b/2c/2d can be done in parallel."
    },
    "P3": {
      "name": "Restore decad CSVs from conflicted copies + current data",
      "file": "data files (not code)",
      "depends_on": [],
      "parallel_group": "A",
      "note": "Independent of code fixes; can run in parallel with P1"
    },
    "P4": {
      "name": "Run tests and verify end-to-end",
      "depends_on": ["P1", "P2", "P3"],
      "parallel_group": "C"
    }
  },
  "execution_order": [
    {"parallel": ["P1", "P3"]},
    {"sequential": ["P2"]},
    {"sequential": ["P4"]}
  ]
}
```
