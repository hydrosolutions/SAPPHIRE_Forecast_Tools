# ML make_forecast: Read Historical Forecasts from API with CSV Fallback

**Status**: Draft
**Module**: `machine_learning`
**Priority**: High (data flow consistency)
**Labels**: `enhancement`, `api-migration`, `data-flow`
**Branch**: `develop_long_term_fix_api_postprocessing_forecasts`
**Prerequisite**: `high_prio_gi_draft_ml_csv_schema_corruption_fix.md` must land first
(provides `ML_CANONICAL_CSV_COLUMNS`, `normalize_ml_csv_columns()`, and fixes the
API reader column leak).

---

## Summary

`make_forecast.py` reads historical forecasts (for missing-value imputation)
exclusively from CSV files (lines 660-674). Every other data source in the same
script already uses the API-first-CSV-fallback pattern:

| Data source | Read path | Status |
|-------------|-----------|--------|
| Discharge | API -> CSV fallback | Done |
| Meteo (T, P) | API -> CSV fallback | Done |
| **Old forecasts** | **CSV only** | **Gap (this issue)** |

> **ERA5 meteo data**: Both `make_forecast.py` and `hindcast_ML_models.py` already
> read ERA5 data via API when `SAPPHIRE_API_ENABLED=true` (default). The CSV
> paths (`_control_member.csv` / `_reanalysis.csv`) are fallback only — the API
> has no distinction between reanalysis and control member data; both read from
> the same `Meteo` table. No changes needed for ERA5 reading.

The `_read_ml_forecasts_from_api()` function already exists in
`utils_ml_forecast.py` (lines 528-626) and is used inline by `fill_ml_gaps.py`
and `recalculate_nan_forecasts.py`. The same inline pattern should be used in
`make_forecast.py` — no new wrapper function needed.

## Current Behavior (make_forecast.py:660-674)

```python
# CSV-only read — no API attempt
if PREDICTION_MODE == "PENTAD":
    try:
        old_forecast = pd.read_csv(
            os.path.join(OUTPUT_PATH_DISCHARGE, f"pentad_{MODEL_TO_USE}_forecast.csv")
        )
    except FileNotFoundError:
        old_forecast = pd.DataFrame()
else:
    try:
        old_forecast = pd.read_csv(
            os.path.join(OUTPUT_PATH_DISCHARGE, f"decad_{MODEL_TO_USE}_forecast.csv")
        )
    except FileNotFoundError:
        old_forecast = pd.DataFrame()
```

## Desired Behavior

```
1. Try API: _read_ml_forecasts_from_api(model_type, horizon_type, start_date=lookback)
2. If empty -> fall back to CSV (existing logic)
3. Normalize columns in CSV fallback path (strip corrupted API-only columns)
```

## How old_forecast Is Used Downstream

In `prepare_forecast_data()` (lines 249-348), `old_forecast` is filtered by
code (line 271), then used to fill missing discharge values via Q50 (lines
298-329). It accesses exactly 4 columns:

- **`code`** — filtering (line 271)
- **`date`** — matching missing discharge dates (line 272, 301)
- **`forecast_date`** — sorting to keep latest forecast per date (line 273, 303-304)
- **`Q50`** (or `Q` fallback) — the imputation values (lines 306-314)

Extra columns from the API response (e.g., `horizon_type`, `model_type`) are
harmlessly ignored. An empty DataFrame is handled safely — the code skips
to interpolation/ffill (line 331).

## Critical Design Decisions

### 1. No new wrapper function — use the inline pattern

`fill_ml_gaps.py` (lines 182-208) and `recalculate_nan_forecasts.py` (lines
186-212) both use the same inline API-first-CSV-fallback pattern:

```python
forecast = _read_ml_forecasts_from_api(
    model_type=MODEL_TO_USE,
    horizon_type=prefix,
    start_date=api_start,
)
if forecast.empty:
    try:
        forecast = pd.read_csv(csv_path)
    except FileNotFoundError:
        forecast = pd.DataFrame()
```

Following the same pattern in `make_forecast.py` is consistent with the module,
avoids creating a one-off helper (CLAUDE.md: "Don't create helpers for one-time
operations"), and makes all three call sites grep-able.

### 2. `horizon_type` parameter is for logging only

**`_read_ml_forecasts_from_api()` always queries with `horizon="day"`**
(hardcoded at line 581). The `horizon_type` parameter only appears in log
messages (line 607). This is correct because `_write_ml_forecast_to_api()`
stores all ML forecasts with `horizon_type="day"` (line 704).

In the call site, pass `"pentad"` or `"decad"` for log clarity, but understand
it does not filter the API query. The function returns ALL daily ML forecasts
for the given model within the date range.

### 3. Two separate CSV reads in make_forecast.py — by design

There are two CSV read sites in the forecast pipeline:

| Read | Location | Purpose | Should use API? |
|------|----------|---------|-----------------|
| **Imputation read** | Lines 660-674 | Business logic: fill missing discharge | **Yes** (this issue) |
| **Archive read** | Lines 182, 232 (inside write functions) | File maintenance: append to CSV archive | **No** — reads the file it's about to write to |

The write functions (`write_pentad_forecast`, `write_decad_forecast`) read
the CSV archive, concat with today's forecast, and write back. This is CSV
file maintenance, not business logic. These reads must stay CSV-based because
they maintain the local file. The API write is a separate step (lines 166-173).

### 4. CSV write normalization belongs in the corruption fix issue

Phase 2d of `high_prio_gi_draft_ml_csv_schema_corruption_fix.md` already adds
`normalize_ml_csv_columns()` before `.to_csv()` in `write_pentad_forecast()`
and `write_decad_forecast()`. This issue does NOT duplicate that work.

### 5. Lookback window: 60 days is sufficient

`prepare_forecast_data()` only needs forecasts for dates within
`input_chunk_length + 1` days of today (line 279). The max
`input_chunk_length` across models is ~30 days. A 60-day lookback provides
margin without fetching the entire archive. The `start_date` parameter
filters by `forecast_date` (issue date), not target date, so 60 days captures
all relevant recent forecasts.

---

## Implementation Plan

### Phase 1: Replace CSV-only read with API-first pattern in make_forecast.py

**File**: `apps/machine_learning/make_forecast.py`

**Add import** (alongside existing import at line 108):
```python
from scr.utils_ml_forecast import _read_ml_forecasts_from_api
```

**Replace lines 660-674** with the inline API-first-CSV-fallback pattern:

```python
# Load old forecast for missing-value imputation (API-first, CSV fallback)
prefix = "pentad" if PREDICTION_MODE == "PENTAD" else "decad"
forecast_csv_path = os.path.join(
    OUTPUT_PATH_DISCHARGE, f"{prefix}_{MODEL_TO_USE}_forecast.csv"
)
lookback_start = (
    pd.to_datetime(datetime.datetime.now().date()) - pd.Timedelta(days=60)
).strftime("%Y-%m-%d")

old_forecast = _read_ml_forecasts_from_api(
    model_type=MODEL_TO_USE,
    horizon_type=prefix,
    start_date=lookback_start,
)
if old_forecast.empty:
    logger.info(
        "API returned no %s %s forecasts for imputation — falling back to CSV",
        MODEL_TO_USE,
        prefix,
    )
    try:
        old_forecast = pd.read_csv(forecast_csv_path)
        old_forecast["forecast_date"] = pd.to_datetime(
            old_forecast["forecast_date"], format="mixed"
        )
        old_forecast["date"] = pd.to_datetime(
            old_forecast["date"], format="mixed"
        )
    except FileNotFoundError:
        old_forecast = pd.DataFrame()
else:
    logger.info(
        "Read %d %s %s forecast rows from API for imputation",
        len(old_forecast),
        MODEL_TO_USE,
        prefix,
    )
```

**Key details**:
- Follows the exact same pattern as `fill_ml_gaps.py` lines 182-208
- Uses `format="mixed"` for CSV date parsing (matches existing code at lines 187-190)
- Logs which source was used (API vs CSV)
- No column normalization needed on the API path — `_read_ml_forecasts_from_api()`
  already renames columns (after corruption fix lands, it will also filter them)
- CSV fallback path does NOT normalize columns — `prepare_forecast_data()` only
  accesses `code`, `date`, `forecast_date`, `Q50`/`Q`, so extra columns are harmless

### Phase 2: Tests

**File**: `apps/machine_learning/test/test_api_integration.py`

**2a — Test API-first path for old_forecast read**:
- Mock `_read_ml_forecasts_from_api` to return a DataFrame with `code`, `date`,
  `forecast_date`, `Q50` columns
- Verify the returned data is used (not CSV)
- Verify `start_date` parameter uses 60-day lookback

**2b — Test CSV fallback when API returns empty**:
- Mock `_read_ml_forecasts_from_api` to return empty DataFrame
- Provide a CSV file at the expected path
- Verify CSV data is used

**2c — Test both sources empty**:
- Mock API empty + no CSV file
- Verify `old_forecast` is empty DataFrame
- Verify `prepare_forecast_data()` proceeds without error (skips to interpolation)

**2d — Integration test: API-sourced old_forecast works with prepare_forecast_data()**:
- Create an API-format DataFrame (only Q5, Q25, Q50, Q75, Q95 — no Q10-Q90)
- Pass to `prepare_forecast_data()` as `old_forecast`
- Verify Q50 is found and used for imputation (line 306-314 should work)

### Phase 3: Verify End-to-End

1. Run ML tests: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning`
2. Verify all tests pass with zero skips
3. Review `git diff` to confirm no unintended changes

---

## Risks & Mitigations

### 1. API returns more data than needed (MITIGATED)

Without `start_date`, the API would return all historical forecasts (~2 years).
The 60-day lookback `start_date` parameter limits the response to ~60 days of
daily forecasts per station. For 20 stations x 60 days x 6 target days = ~7,200
rows — well within a single API page (5,000-row pages).

### 2. API-sourced old_forecast has only 5 quantiles (SAFE)

`prepare_forecast_data()` only uses Q50 (line 306). It doesn't care about
Q10-Q90. The "Q" column fallback (line 308-309) handles legacy CSVs that
predate quantile columns.

### 3. Column leak from _read_ml_forecasts_from_api (SAFE AFTER PREREQ)

The corruption fix issue adds column filtering to `_read_ml_forecasts_from_api()`.
Even without it, `prepare_forecast_data()` ignores extra columns — it only
accesses `code`, `date`, `forecast_date`, and `Q50`.

### 4. CSV fallback reads corrupted file (SAFE AFTER PREREQ)

The corruption fix issue normalizes CSV writes. After it lands, the CSV
archive will have clean 23-column format. Even if a corrupted CSV is read,
`prepare_forecast_data()` only uses 4 columns and ignores the rest.

### 5. No service code changes needed (OWNERSHIP RESPECTED)

Everything stays within `apps/machine_learning/`. No changes to
`sapphire/services/`, no new API endpoints or schema changes.

### 6. write_pentad_forecast / write_decad_forecast CSV reads unchanged (BY DESIGN)

These functions read the CSV for append-on-write (file maintenance). This is
separate from the business logic read. See "Critical Design Decisions #3" above.
CSV normalization for these writes is handled by the corruption fix issue.

### 7. ERA5 meteo reading is already API-first (NO CHANGE NEEDED)

`read_meteo_data_combined()` dispatches to `fl.read_meteo_data()` which checks
`SAPPHIRE_API_ENABLED` and uses the API by default. The `csv_path_t` / `csv_path_p`
parameters are only used when `SAPPHIRE_API_ENABLED=false`. The CSV filename
suffixes (`_control_member` vs `_reanalysis`) are a local convention with no
API equivalent — both call sites read the same `Meteo` table via
`SapphirePreprocessingClient.read_meteo()`.

---

## Dependency Graph

```json
{
  "prerequisite": {
    "name": "high_prio_gi_draft_ml_csv_schema_corruption_fix.md",
    "reason": "Provides ML_CANONICAL_CSV_COLUMNS, normalize_ml_csv_columns(), and fixes _read_ml_forecasts_from_api() column leak"
  },
  "phases": {
    "P1": {
      "name": "Replace CSV-only old_forecast read with API-first pattern in make_forecast.py",
      "file": "apps/machine_learning/make_forecast.py",
      "depends_on": [],
      "parallel_group": "A"
    },
    "P2": {
      "name": "Write tests for API-first old_forecast read and CSV fallback",
      "file": "apps/machine_learning/test/test_api_integration.py",
      "depends_on": [],
      "parallel_group": "A",
      "note": "Tests can be written in parallel with P1 (test against mocked interface)"
    },
    "P3": {
      "name": "Run full test suite, verify end-to-end",
      "depends_on": ["P1", "P2"],
      "parallel_group": "B"
    }
  },
  "execution_order": [
    {"parallel": ["P1", "P2"]},
    {"sequential": ["P3"]}
  ]
}
```
