# Fix ML Forecast API Reader & Align Write/Read Architecture

**Status**: DRAFT
**Modules**: `iEasyHydroForecast/setup_library.py`, `machine_learning/scr/utils_ml_forecast.py`,
`postprocessing_forecasts/src/api_writer.py`, `sapphire/services/postprocessing/app/data_migrator.py`
**Impact**: All ML models (TFT, TiDE, TSMixer, ARIMA), long-term models, postprocessing pipeline,
data migration
**Related tests**: `postprocessing_forecasts/tests/test_api_read.py`, `machine_learning/test/test_api_integration.py`

---

## Table of Contents

1. [Target Architecture](#target-architecture)
2. [Current vs Target Gap Analysis](#current-vs-target-gap-analysis)
3. [Phase 1: Fix the Postprocessing Reader](#phase-1-fix-the-postprocessing-reader)
4. [Phase 2: Migrate ML Writer to horizon_type=day](#phase-2-migrate-ml-writer-to-horizon_typeday)
5. [Phase 2b: Fix the Data Migration Script](#phase-2b-fix-the-data-migration-script)
6. [Phase 3: Clean up Duplicate Records](#phase-3-clean-up-duplicate-records)
7. [Long-term Forecasts: Current State](#long-term-forecasts-current-state)
8. [Verification Checklist](#verification-checklist)

---

## Target Architecture

The intended data flow separates **producers** (raw forecasts) from **consumers/aggregators**
(postprocessing):

```
                           ┌──────────────────────────────┐
                           │     Forecast table (API)      │
                           │                               │
  ML Module ──────────────►│  horizon_type = "day"         │
  (TFT, TiDE, TSMixer,    │  10-11 daily target rows      │
   ARIMA)                  │  per (code, forecast_date)    │
                           │                               │
                           ├───────────────────────────────┤
                           │                               │
  Postprocessing ─────────►│  horizon_type = "pentad"      │
  Module                   │  1 aggregated row per         │
                           │  (code, pentad_boundary)      │
                           │  for ALL models + EM + NE     │
                           │                               │
                           ├───────────────────────────────┤
                           │                               │
  Postprocessing ─────────►│  horizon_type = "decade"      │
  Module                   │  1 aggregated row per         │
                           │  (code, decad_boundary)       │
                           │  for ALL models + EM + NE     │
                           │                               │
                           └───────────────────────────────┘

                           ┌──────────────────────────────┐
                           │   LongForecast table (API)    │
                           │                               │
  Long-term Module ───────►│  horizon_type = "month"       │
  (GBT, LR_Base,          │  1 row per (code, date,       │
   MC_ALD, etc.)           │  model, valid_from, valid_to) │
                           │                               │
                           ├───────────────────────────────┤
                           │                               │
  Postprocessing ─────────►│  horizon_type = "month"       │
  Module                   │  Ensemble forecasts           │
                           │  (EM, Skilled Mean,           │
                           │   Naive Mean)                 │
                           │                               │
                           └───────────────────────────────┘
```

### Key Principles

1. **ML module writes only raw daily forecasts** (`horizon_type="day"`) — it does NOT
   pre-aggregate to pentad or decad. The 10-11 daily target values for each forecast run
   are stored individually with their respective target dates.

2. **Postprocessing module owns all aggregation** — it reads daily forecasts, aggregates
   to pentad/decad by averaging quantiles across daily targets, creates EM/NE ensembles,
   and writes the aggregated results with `horizon_type="pentad"` or `"decade"`.

3. **Long-term module writes monthly forecasts** to the `LongForecast` table with
   `horizon_type="month"`. Postprocessing reads these, creates ensemble averages
   (EM, Skilled Mean, Naive Mean), and writes those back to `LongForecast`.

4. **Separation of concerns**: Producing modules (ML, long-term) write raw outputs.
   The postprocessing module handles aggregation, ensembles, and skill metrics.

---

## Current vs Target Gap Analysis

### Short-term ML Forecasts

| Aspect | Current Implementation | Target Architecture |
|--------|----------------------|---------------------|
| **ML writer horizon_type** | `"pentad"` or `"decade"` (daily rows tagged with pentad/decad metadata) | `"day"` only (10-11 daily rows per forecast run) |
| **ML writer function** | `_write_ml_forecast_to_api()` writes with pentad/decade horizon | `_write_ml_daily_forecast_to_api()` (already exists, used only for Tier 2 decad metrics) becomes the primary writer |
| **Forecast table contents** | Mixed: ML daily rows with `horizon_type="pentad"/"decade"` AND postprocessing aggregated rows with same horizon_type but `target=date` | Clean separation: ML writes `horizon_type="day"`, postprocessing writes `horizon_type="pentad"/"decade"` |
| **Postprocessing reader** | `_read_ml_forecasts_from_api(horizon_type="pentad"/"decade")` — reads pentad/decade-tagged daily rows, does `drop_duplicates` (Bug: destroys data) | Reads `horizon_type="day"`, aggregates to pentad/decad, returns one row per (code, boundary_date) |
| **Data integrity** | ML writes 5 daily rows for pentad, postprocessing upserts 1 aggregated row with `target=date=boundary`. The upsert overwrites one ML row (where target=boundary) but leaves the other 4 orphaned. | No collision: ML rows have `horizon_type="day"`, postprocessing rows have `horizon_type="pentad"/"decade"` — different unique constraint space |

### Short-term ML: Detailed Data Collision Problem

Currently, the ML writer writes 5 daily records for a pentad:
```
(horizon_type=pentad, code=12345, model=TFT, date=2024-01-05, target=2024-01-01)
(horizon_type=pentad, code=12345, model=TFT, date=2024-01-05, target=2024-01-02)
(horizon_type=pentad, code=12345, model=TFT, date=2024-01-05, target=2024-01-03)
(horizon_type=pentad, code=12345, model=TFT, date=2024-01-05, target=2024-01-04)
(horizon_type=pentad, code=12345, model=TFT, date=2024-01-05, target=2024-01-05) ← this one
```

Then postprocessing writes the aggregated record:
```
(horizon_type=pentad, code=12345, model=TFT, date=2024-01-05, target=2024-01-05) ← upserts over ML row
```

The unique constraint `(horizon_type, code, model_type, date, target)` means the
postprocessing record upserts OVER the ML daily row where `target=2024-01-05` (last
day of pentad = boundary date). The other 4 ML daily rows remain orphaned.

**Result**: 5 rows exist where there should be 1 aggregated row. When the reader runs
`drop_duplicates(subset=['code', 'date'])`, it keeps one arbitrary row — which might
be a raw daily value, not the aggregated average.

### Long-term Forecasts

| Aspect | Current Implementation | Target Architecture |
|--------|----------------------|---------------------|
| **Long-term writer** | Writes to `LongForecast` table with `horizon_type="month"` | Same (already correct) |
| **Postprocessing monthly ensembles** | `_write_monthly_ensemble_to_api()` writes EM/Skilled Mean/Naive Mean to `LongForecast` table with `horizon_type="month"` | Same (already correct) |
| **Aggregation to other horizons** | Not yet implemented (quarter, season) | Future: postprocessing aggregates monthly to quarter/season and writes to `LongForecast` |

**Assessment**: Long-term forecast architecture already matches the target vision. The only
gap is future work to aggregate monthly forecasts to quarterly/seasonal horizons.

---

## Phase 1: Fix the Postprocessing Reader

**Goal**: Make `_read_ml_forecasts_from_api` correctly aggregate daily-resolution records
from the API, regardless of whether they're stored with `horizon_type="pentad"/"decade"`
(current) or `horizon_type="day"` (future).

**Scope**: `setup_library.py` + `test_api_read.py` only. No changes to writers.

This phase works with BOTH the current data layout and the future layout, making it safe
to deploy before Phase 2.

### Bug Summary (4 bugs)

1. **Bug 1 (Critical)**: `drop_duplicates(subset=['code', 'date'])` at line 1312 keeps one
   arbitrary daily row instead of averaging ~5 daily targets per pentad (or ~10 for decad).
2. **Bug 2 (Moderate)**: API response columns (`id`, `target`, `model_type`, `horizon_type`,
   `model_type_description`, `composition`) leak into output. The `target` column is
   meaningless after aggregation.
3. **Bug 3 (Minor)**: No server-side model filtering — downloads all models, filters client-side.
   Mock data uses `model` column but real API returns `model_type`, so the model filter test
   passes for the wrong reason.
4. **Bug 4 (Minor)**: Uses deprecated `read_forecasts()` instead of `read_short_term_forecasts()`.

### Step 1: Update `_read_ml_forecasts_from_api` in setup_library.py

**Signature** (unchanged):
```python
def _read_ml_forecasts_from_api(
    model: str,
    horizon_type: str,
    site_codes: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
```

**Changes in the pagination loop** (lines 1266–1297):

```python
# CHANGE: Use read_short_term_forecasts with model param for server-side filtering
df_page = client.read_short_term_forecasts(
    horizon=horizon_type,
    code=code,
    model=model_upper,       # <-- server-side model filtering
    start_date=start_date,
    end_date=end_date,
    skip=skip,
    limit=page_size,
)

# Store original page length before filtering for pagination
original_page_len = len(df_page)

if df_page.empty:
    break

# Defensive client-side model filter (server already filtered,
# but guard against API versions that don't support model param)
if 'model_type' in df_page.columns:
    df_page = df_page[df_page['model_type'].str.upper() == model_upper]

if not df_page.empty:
    all_data.append(df_page)
```

**Changes after concat** (replace lines 1309–1347):

```python
forecast_data = pd.concat(all_data, ignore_index=True)

# Convert types before aggregation
if 'date' in forecast_data.columns:
    forecast_data['date'] = fl.parse_dates_robust(forecast_data['date'], 'date')
if 'target' in forecast_data.columns:
    forecast_data['target'] = fl.parse_dates_robust(forecast_data['target'], 'target')
forecast_data['code'] = (
    forecast_data['code'].astype(str).str.replace(r'\.0$', '', regex=True)
)

# --- Aggregate daily targets to pentad/decad level ---
# The API stores one row per daily target. Downstream code expects one row
# per (code, pentad/decad_boundary) with averaged quantile values.
numeric_cols = ['q05', 'q25', 'q50', 'q75', 'q95', 'forecasted_discharge']
agg_dict = {}
for col in numeric_cols:
    if col in forecast_data.columns:
        agg_dict[col] = 'mean'
if 'flag' in forecast_data.columns:
    agg_dict['flag'] = 'max'   # worst quality flag
for col in ['horizon_value', 'horizon_in_year']:
    if col in forecast_data.columns:
        agg_dict[col] = 'first'  # identical within group

if agg_dict:
    forecast_data = (
        forecast_data
        .groupby(['code', 'date'], as_index=False)
        .agg(agg_dict)
    )

# Add model column
forecast_data["model_short"] = model_short

# Compute horizon columns from dates
if 'date' in forecast_data.columns and not forecast_data.empty:
    if horizon_type == "pentad":
        forecast_data["pentad_in_month"] = (
            forecast_data["date"] + pd.Timedelta(days=1)
        ).apply(tl.get_pentad)
        forecast_data["pentad_in_year"] = (
            forecast_data["date"] + pd.Timedelta(days=1)
        ).apply(tl.get_pentad_in_year)
    elif horizon_type == "decade":
        forecast_data["decad_in_month"] = (
            forecast_data["date"] + pd.Timedelta(days=1)
        ).apply(tl.get_decad_in_month)
        forecast_data["decad_in_year"] = (
            forecast_data["date"] + pd.Timedelta(days=1)
        ).apply(tl.get_decad_in_year)

# Sort by code and date
forecast_data = forecast_data.sort_values(by=['code', 'date'])
```

### Step 2: Update mock data in test_api_read.py

Replace `create_mock_ml_forecast_data()` with realistic multi-target data:

```python
def create_mock_ml_forecast_data():
    """Create sample ML forecast data matching API response structure.

    Simulates 5 daily targets per (code, date) pentad, matching what
    the ML writer produces. Two stations, one pentad each.
    """
    rows = []
    for i, code in enumerate(['12345', '12346']):
        base_q50 = 100.0 + i * 50  # 100 for 12345, 150 for 12346
        for day in range(1, 6):
            rows.append({
                'id': len(rows) + 1,
                'horizon_type': 'pentad',
                'code': code,
                'model_type': 'TFT',
                'date': pd.Timestamp('2024-01-05'),
                'target': pd.Timestamp(f'2024-01-0{day}'),
                'horizon_value': 1,
                'horizon_in_year': 1,
                'forecasted_discharge': base_q50 + day,  # varies per target
                'q05': base_q50 - 20 + day,
                'q25': base_q50 - 10 + day,
                'q50': base_q50 + day,
                'q75': base_q50 + 10 + day,
                'q95': base_q50 + 20 + day,
                'flag': 0,
            })
    return pd.DataFrame(rows)
```

Keep a backward-compatible helper for tests that need minimal data:

```python
def create_mock_ml_forecast_data_single_target():
    """Create minimal ML forecast data (1 row per station, already aggregated)."""
    return pd.DataFrame({
        'id': [1, 2],
        'horizon_type': ['pentad', 'pentad'],
        'code': ['12345', '12346'],
        'model_type': ['TFT', 'TFT'],
        'date': pd.to_datetime(['2024-01-05', '2024-01-05']),
        'target': pd.to_datetime(['2024-01-05', '2024-01-05']),
        'horizon_value': [1, 1],
        'horizon_in_year': [1, 1],
        'forecasted_discharge': [100.5, 150.2],
        'q05': [80.0, 120.0],
        'q25': [90.0, 135.0],
        'q50': [100.5, 150.2],
        'q75': [110.0, 165.0],
        'q95': [120.0, 180.0],
        'flag': [0, 0],
    })
```

### Step 3: Update existing tests

**Tests that need mock data column fix** (`model` -> `model_type`):
- `test_tide_model_mapping` (line 391)
- `test_tsmixer_model_mapping` (line 409)
- `test_arima_model_mapping` (line 427)
- `test_ml_api_filters_by_model_type` (line 1163)

**Tests that need assertion updates** for aggregation:
- `test_tft_model_read_success` (line 365): Assert `len(result) == 2` still holds
  (2 stations, each aggregated from 5 daily rows to 1 pentad row)
- `test_decade_horizon` (line 439): Same pattern

### Step 4: Add new tests

```python
class TestMLAggregation:
    """Tests for daily-to-pentad/decad aggregation in _read_ml_forecasts_from_api."""

    @patch('setup_library.SapphirePostprocessingClient')
    def test_pentad_aggregation_averages_quantiles(self, mock_client_class):
        """5 daily targets per (code, date) should produce 1 row with mean values."""
        # Assert: 2 output rows (one per station)
        # Assert: forecasted_discharge for station 12345 == mean([101,102,103,104,105]) == 103.0
        # Assert: q50 for station 12345 == 103.0

    @patch('setup_library.SapphirePostprocessingClient')
    def test_aggregation_flag_takes_worst(self, mock_client_class):
        """flag should be max (worst) across daily targets, not mean."""
        # Set flags to [0, 0, 1, 0, 0] for one station
        # Assert: aggregated flag == 1

    @patch('setup_library.SapphirePostprocessingClient')
    def test_aggregation_preserves_horizon_columns(self, mock_client_class):
        """horizon_value and horizon_in_year should survive aggregation."""

    @patch('setup_library.SapphirePostprocessingClient')
    def test_target_column_not_in_output(self, mock_client_class):
        """The raw 'target' column (daily dates) should not be in output."""

    @patch('setup_library.SapphirePostprocessingClient')
    def test_model_type_column_not_in_output(self, mock_client_class):
        """API metadata columns (id, model_type, horizon_type) should not leak."""

    @patch('setup_library.SapphirePostprocessingClient')
    def test_multiple_pentads_multiple_stations(self, mock_client_class):
        """2 stations x 2 pentads x 5 targets = 20 rows -> 4 aggregated rows."""

    @patch('setup_library.SapphirePostprocessingClient')
    def test_decade_aggregation(self, mock_client_class):
        """Decad should aggregate ~10 daily targets into 1 row."""

    @patch('setup_library.SapphirePostprocessingClient')
    def test_pentad_in_month_computed_after_aggregation(self, mock_client_class):
        """pentad_in_month should be correct for known dates."""

    @patch('setup_library.SapphirePostprocessingClient')
    def test_output_columns_match_csv_reader(self, mock_client_class):
        """API reader output columns should match CSV reader output."""
```

### Step 5: Verify

```bash
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh
```

---

## Phase 2: Migrate ML Writer to horizon_type=day

**Goal**: The ML module writes ALL daily forecasts with `horizon_type="day"` instead of
`horizon_type="pentad"/"decade"`. The postprocessing reader is updated to read from
`horizon_type="day"`.

**Scope**: `machine_learning/scr/utils_ml_forecast.py`, `machine_learning/make_forecast.py`,
`machine_learning/fill_ml_gaps.py`, `machine_learning/recalculate_nan_forecasts.py`,
`iEasyHydroForecast/setup_library.py`

**Prerequisite**: Phase 1 must be deployed and tested.

### Step 1: Consolidate ML writers

Currently there are two write functions:
- `_write_ml_forecast_to_api(data, horizon_type, model_type)` — writes with pentad/decade
  horizon, computes pentad/decad from target date
- `_write_ml_daily_forecast_to_api(data, model_type)` — writes with `horizon_type="day"`,
  computes day-of-year from target date

**Action**: Modify `_write_ml_forecast_to_api` to always write `horizon_type="day"`:

```python
# In _write_ml_forecast_to_api:
record = {
    "horizon_type": "day",                               # <-- always day
    "code": str(int(row['code'])),
    "model_type": api_model_type,
    "date": forecast_date.strftime('%Y-%m-%d'),          # issue date
    "target": target_date.strftime('%Y-%m-%d'),          # daily target
    "flag": int(row['flag']) if pd.notna(row.get('flag')) else None,
    "horizon_value": target_date.timetuple().tm_yday,    # day of year
    "horizon_in_year": target_date.timetuple().tm_yday,  # day of year
    "q05": ..., "q25": ..., "q50": ..., "q75": ..., "q95": ...,
    "forecasted_discharge": ...,
}
```

The `horizon_type` parameter on the function signature becomes informational only (it tells
the writer that this data was _produced for_ pentad or decad purposes, but the storage
horizon is always "day").

Alternatively, remove the pentad/decade write entirely and make all callers use
`_write_ml_daily_forecast_to_api()`.

### Step 2: Update callers

- `make_forecast.py:write_pentad_forecast()` — call writer with `horizon_type="day"`
- `make_forecast.py:write_decad_forecast()` — call writer with `horizon_type="day"`
  (remove the separate `_write_ml_daily_forecast_to_api` call since it's now redundant)
- `fill_ml_gaps.py` — call writer with `horizon_type="day"`
- `recalculate_nan_forecasts.py` — call writer with `horizon_type="day"`

### Step 3: Update the postprocessing reader

In `setup_library.py`, update `_read_ml_forecasts_from_api`:

```python
# Read from 'day' horizon (target architecture)
df_page = client.read_short_term_forecasts(
    horizon="day",           # <-- always read daily records
    code=code,
    model=model_upper,
    start_date=start_date,
    end_date=end_date,
    skip=skip,
    limit=page_size,
)
```

The aggregation logic from Phase 1 handles converting daily rows to pentad/decad rows.
The `horizon_type` parameter on the reader function still controls whether pentad or
decad columns are computed.

### Step 4: Transition strategy

During migration, the database may contain BOTH old (pentad/decade-tagged) and new
(day-tagged) records. Two options:

**Option A: Dual-read (safest)**
```python
# Try "day" first (new convention), fall back to pentad/decade (old convention)
df_page = client.read_short_term_forecasts(horizon="day", ...)
if df_page.empty:
    df_page = client.read_short_term_forecasts(horizon=horizon_type, ...)
```

**Option B: One-time migration script**
Write a SQL migration that re-tags existing records:
```sql
UPDATE forecasts
SET horizon_type = 'day',
    horizon_value = EXTRACT(DOY FROM target),
    horizon_in_year = EXTRACT(DOY FROM target)
WHERE model_type IN ('TFT', 'TiDE', 'TSMixer', 'ARIMA')
  AND horizon_type IN ('pentad', 'decade')
  AND target IS NOT NULL
  AND target != date;  -- only daily records, not postprocessing aggregated ones
```

**Recommendation**: Use Option A during the transition period, then run Option B
migration and remove the dual-read once all production servers are updated.

---

## Phase 2b: Fix the Data Migration Script

> **COORDINATION REQUIRED**: This phase touches `sapphire/services/postprocessing/` which
> is shared infrastructure. **Do NOT implement these changes in this branch.** Discuss the
> proposed fixes with your colleague first to avoid data integrity issues on production
> databases. This section is documentation only — a reference for that discussion.

**Goal**: Align `ForecastDataMigrator` in `data_migrator.py` with the target architecture
so that historical ML forecast CSVs are migrated with `horizon_type="day"`.

**File**: `sapphire/services/postprocessing/app/data_migrator.py`

### Current Problems in ForecastDataMigrator (lines 281–350)

The `ForecastDataMigrator` class migrates raw ML daily forecast CSVs (TFT, TiDE, TSMixer)
from files like `pentad_TFT_forecast_latest.csv` and `decad_TFT_forecast_latest.csv`.

**Problem 1: Wrong horizon_type**

Both `prepare_pentad_data` (line 288) and `prepare_decade_data` (line 310) set the
horizon_type to `"pentad"` or `"decade"` respectively. These CSV files contain
**daily-resolution** forecasts (one row per daily target), not aggregated pentad/decad
values. Per the target architecture, they should use `horizon_type="day"`.

```python
# Current (WRONG):
record = {
    "horizon_type": "pentad",   # line 288 — daily data tagged as pentad
    ...
}
record = {
    "horizon_type": "decade",   # line 310 — daily data tagged as decade
    ...
}
```

**Problem 2: Migrates both pentad AND decad CSVs — creates duplicates**

The pentad CSV (`pentad_TFT_forecast_latest.csv`) contains daily forecasts for ~5 days
within the current pentad window. The decad CSV (`decad_TFT_forecast_latest.csv`) contains
daily forecasts for ~10-11 days within the current decad window. When a pentad boundary
coincides with a decad boundary, the daily target dates overlap.

Since all records are daily-resolution predictions from the same model, migrating both
creates duplicate rows for the overlapping days. The decad CSV is a superset — it always
contains more or equal daily targets than the pentad CSV.

**Only the decadal CSV files should be migrated.**

**Problem 3: horizon_value and horizon_in_year are hardcoded to 0**

```python
"horizon_value": 0,        # line 294, 316
"horizon_in_year": 0,      # line 295, 317
```

These should be the day-of-year computed from the target date (`row['date']`).

**Problem 4: forecasted_discharge is set to None**

```python
"forecasted_discharge": None    # line 301, 323
```

The Q50 value is present in the CSV. `forecasted_discharge` should be set to Q50 for
consistency with the operational ML writer (`_write_ml_forecast_to_api` at line 551:
`"forecasted_discharge": float(row['Q50'])`).

### Note: CombinedForecastDataMigrator is correct

`CombinedForecastDataMigrator` (lines 167-228) migrates the `combined_forecasts_pentad_latest.csv`
and `combined_forecasts_decad_latest.csv` files. These contain **already-aggregated** pentad/decad
records written by the postprocessing module (one row per code/period with averaged values).
Using `horizon_type="pentad"/"decade"` is correct for these files. No changes needed.

### Fix

**Step 1**: Remove pentad CSVs from the `ForecastDataMigrator` configuration (lines 946-959):

```python
if args.type in ['forecast', 'all']:
    horizons = {
        # REMOVED: "pentad" — pentad CSVs are a subset of decad CSVs.
        # Only migrate decad CSVs to avoid duplicate daily records.
        "decade": {
            "TFT": "predictions/TFT/decad_TFT_forecast_latest.csv",
            "TiDE": "predictions/TIDE/decad_TIDE_forecast_latest.csv",
            "TSMixer": "predictions/TSMIXER/decad_TSMIXER_forecast_latest.csv"
        }
    }
    migrators_to_run.append(ForecastDataMigrator(
        API_URL, BATCH_SIZE, horizons, sub_url="forecast"
    ))
```

**Step 2**: Fix `prepare_decade_data` to use correct horizon_type and values:

```python
def prepare_decade_data(self, df: pd.DataFrame) -> List[Dict]:
    """Prepare daily ML forecast records for API migration.

    Despite the method name (kept for interface compatibility), this now
    produces horizon_type='day' records — matching the target architecture
    where ML models store raw daily forecasts.
    """
    records = []
    for _, row in df.iterrows():
        target_date = pd.to_datetime(row['date'])
        day_of_year = target_date.timetuple().tm_yday

        record = {
            "horizon_type": "day",                  # <-- fixed
            "code": str(row['code']),
            "model_type": self.model_type,
            "date": row['forecast_date'],           # issue date
            "target": row['date'],                  # daily target
            "flag": int(row['flag']) if pd.notna(row['flag']) else None,
            "horizon_value": day_of_year,            # <-- day of year
            "horizon_in_year": day_of_year,          # <-- day of year
            "q05": float(row['Q5']) if pd.notna(row['Q5']) else None,
            "q25": float(row['Q25']) if pd.notna(row['Q25']) else None,
            "q50": float(row['Q50']) if pd.notna(row['Q50']) else None,
            "q75": float(row['Q75']) if pd.notna(row['Q75']) else None,
            "q95": float(row['Q95']) if pd.notna(row['Q95']) else None,
            "forecasted_discharge": float(row['Q50']) if pd.notna(row['Q50']) else None,  # <-- fixed
        }
        records.append(record)
    return records
```

**Step 3**: `prepare_pentad_data` can be removed or left as a no-op (it won't be called
since pentad is removed from the horizons config). To be safe, make it raise:

```python
def prepare_pentad_data(self, df: pd.DataFrame) -> List[Dict]:
    raise NotImplementedError(
        "Pentad CSV migration is disabled. Only decad CSVs should be "
        "migrated — they contain the full set of daily forecasts."
    )
```

### Verification

After re-running the migration:
```bash
docker exec -it sapphire-postprocessing-api python -u app/data_migrator.py --type forecast
```

Check the database:
```sql
-- All ForecastDataMigrator records should have horizon_type='day'
SELECT horizon_type, COUNT(*)
FROM forecasts
WHERE model_type IN ('TFT', 'TiDE', 'TSMixer')
  AND target IS NOT NULL
  AND target != date
GROUP BY horizon_type;
-- Expected: only 'day' rows

-- Verify horizon_value is day-of-year, not 0
SELECT MIN(horizon_value), MAX(horizon_value)
FROM forecasts
WHERE horizon_type = 'day' AND model_type IN ('TFT', 'TiDE', 'TSMixer');
-- Expected: 1–366 (day of year range)
```

---

## Phase 3: Clean up Duplicate Records

**Goal**: Remove orphaned ML daily records that were written with `horizon_type="pentad"/"decade"`.

**Prerequisite**: Phase 2 fully deployed, all ML writers using `horizon_type="day"`.

### Identify orphaned records

After Phase 2, the Forecast table may contain:
1. Old ML daily records with `horizon_type="pentad"/"decade"` (from before migration)
2. Postprocessing aggregated records with `horizon_type="pentad"/"decade"` where `target=date`
3. New ML daily records with `horizon_type="day"` (from after migration)

Records from (1) are orphaned — they have `target != date` and `horizon_type` of
pentad/decade but contain raw daily values, not aggregated pentad/decad values.

### Cleanup SQL

```sql
-- Delete orphaned ML daily records that were stored with pentad/decade horizon
-- (identified by target != date, meaning they're daily-resolution, not aggregated)
DELETE FROM forecasts
WHERE model_type IN ('TFT', 'TiDE', 'TSMixer', 'ARIMA')
  AND horizon_type IN ('pentad', 'decade')
  AND target IS NOT NULL
  AND target != date;
```

### Verification

After cleanup, for any `horizon_type IN ('pentad', 'decade')`:
- All remaining records should have `target = date` (aggregated by postprocessing)
- ML raw records should only exist with `horizon_type = 'day'`

---

## Long-term Forecasts: Current State

The long-term forecast architecture **already matches the target vision**:

| Component | Implementation | Status |
|-----------|---------------|--------|
| Long-term models write to `LongForecast` with `horizon_type="month"` | `lt_utils.py:save_forecast_to_db()` via `client.write_long_forecasts()` | Done |
| Postprocessing creates monthly ensembles (EM, Skilled Mean, Naive Mean) | `api_writer.py:_write_monthly_ensemble_to_api()` via `client.write_long_forecasts()` | Done |
| Monthly skill metrics | `api_writer.py:_write_skill_metrics_to_api(horizon_type="month")` | Done |
| Aggregation to quarterly/seasonal horizons | Not implemented | Future work |

### Future work: Quarterly and seasonal aggregation

When quarterly/seasonal skill metrics are needed (Phase 4b in the unified plan):
1. Postprocessing reads monthly forecasts from `LongForecast` table
2. Aggregates to quarter (3-month average) or season (DJF/MAM/JJA/SON average)
3. Writes aggregated forecasts back to `LongForecast` with `horizon_type="quarter"` or `"season"`
4. Computes and writes skill metrics for the new horizons

This follows the same pattern as the short-term pipeline: producing modules write at their
native resolution, postprocessing handles all aggregation and ensemble creation.

---

## Column Flow Diagrams

### Short-term: Current (Phase 1 fix)

```
ML Writer                    Forecast table               Postprocessing Reader (fixed)
─────────                    ──────────────               ────────────────────────────
date (target)    ──map──►    target                       target ──drop──►
forecast_date    ──map──►    date                         date ──keep──► date (= pentad boundary)
Q50              ──map──►    q50, forecasted_discharge    q50 ──groupby mean──► q50
Q5/Q25/Q75/Q95   ──map──►   q05/q25/q75/q95             q05.. ──groupby mean──► q05..
flag             ──map──►    flag                         flag ──groupby max──► flag
                             horizon_type = "pentad"      ──used for API query──►
                             horizon_value                ──groupby first──► horizon_value
                             horizon_in_year              ──groupby first──► horizon_in_year
                             model_type                   ──drop──►
                             id                           ──drop──►
                                                          ──add──► model_short
                                                          ──add──► pentad_in_month/pentad_in_year
```

### Short-term: Target (after Phase 2)

```
ML Writer                    Forecast table               Postprocessing Reader
─────────                    ──────────────               ────────────────────
date (target)    ──map──►    target                       target ──drop──►
forecast_date    ──map──►    date                         date ──keep──► date (= pentad boundary)
Q50              ──map──►    q50, forecasted_discharge    q50 ──groupby mean──► q50
Q5/Q25/Q75/Q95   ──map──►   q05/q25/q75/q95             q05.. ──groupby mean──► q05..
flag             ──map──►    flag                         flag ──groupby max──► flag
                             horizon_type = "day"  ◄──    ──query horizon="day"──►
                             horizon_value (DOY)          ──drop (recomputed)──►
                             horizon_in_year (DOY)        ──drop (recomputed)──►
                             model_type                   ──drop──►
                             id                           ──drop──►
                                                          ──add──► model_short
                                                          ──add──► pentad_in_month/pentad_in_year
```

### Long-term (already correct)

```
Long-term Writer              LongForecast table          Postprocessing Reader
────────────────              ──────────────────          ────────────────────
forecast_date   ──map──►      date                       date ──keep──► date
valid_from      ──map──►      valid_from                 valid_from ──keep──►
valid_to        ──map──►      valid_to                   valid_to ──keep──►
Q_{model}       ──map──►      q                          q ──keep──►
Q_xgb/lgbm/cat  ──map──►     q_xgb/q_lgbm/q_catboost   kept for ensemble models
quantiles       ──map──►      q05..q95                   kept for MC_ALD
                              horizon_type = "month"      ──query horizon="month"──►
                              model_type                  ──map──► model_short
```

---

## Risk Assessment

| Risk | Phase | Likelihood | Mitigation |
|------|-------|-----------|------------|
| Phase 1 aggregation changes downstream ensemble calculations | 1 | None | Output format is identical to CSV reader output |
| `read_short_term_forecasts` not on installed client | 1 | Low | Fall back to `read_forecasts` with try/except |
| Server-side `model` param not supported | 1 | Low | Client-side filter remains as fallback |
| Phase 2 writer change breaks existing data consumers | 2 | Medium | Dual-read strategy during transition |
| Cleanup SQL deletes valid records | 3 | Low | WHERE clause is conservative (only target != date for ML models) |
| Decad daily count varies (10 or 11 days) | 1-2 | None | groupby averaging is count-agnostic |

---

## Verification Checklist

### Phase 1
- [ ] `_read_ml_forecasts_from_api` aggregates daily targets (groupby mean, not drop_duplicates)
- [ ] Output `date` column = pentad/decad boundary date (issue date from API)
- [ ] Output has no `target`, `id`, `model_type`, `horizon_type` columns
- [ ] `flag` uses max aggregation (worst quality flag)
- [ ] `horizon_value`, `horizon_in_year` preserved through aggregation
- [ ] pentad_in_month / pentad_in_year computed correctly after aggregation
- [ ] Server-side model filtering via `model` parameter
- [ ] Uses `read_short_term_forecasts` (not deprecated `read_forecasts`)
- [ ] Mock data uses `model_type` (not `model`) to match real API response
- [ ] Mock data has multiple daily targets per (code, date) to test aggregation
- [ ] All existing tests updated and passing
- [ ] New aggregation tests added (8+ tests)
- [ ] Full test suite: `SAPPHIRE_TEST_ENV=True bash run_tests.sh` passes with 0 skips

### Phase 2b (data_migrator.py — REQUIRES COORDINATION)

> **DO NOT implement Phase 2b changes in this branch.** The data migration script
> lives in `sapphire/services/postprocessing/` and any changes to the database
> services must be coordinated with the team to avoid data integrity issues on
> production. Discuss the proposed changes with your colleague before proceeding.
> This section documents the required fixes for future implementation.

- [ ] Discussed with colleague and agreed on migration approach
- [ ] `ForecastDataMigrator` pentad CSVs removed from horizons config
- [ ] `prepare_decade_data` uses `horizon_type="day"`
- [ ] `horizon_value` and `horizon_in_year` set to day-of-year from target date
- [ ] `forecasted_discharge` set to Q50 (not None)
- [ ] `prepare_pentad_data` raises NotImplementedError or is removed
- [ ] Tested on staging database before production
- [ ] Verified: no duplicate records from pentad/decad CSV overlap

### Phase 2
- [ ] ML writer uses `horizon_type="day"` for all daily forecasts
- [ ] `_write_ml_daily_forecast_to_api` is the sole ML writer (or the pentad/decade writer delegates to it)
- [ ] Postprocessing reader queries `horizon_type="day"` (with fallback to pentad/decade during transition)
- [ ] make_forecast.py, fill_ml_gaps.py, recalculate_nan_forecasts.py updated
- [ ] ML module tests updated
- [ ] Integration test: write daily → read daily → aggregate to pentad → verify values

### Phase 3
- [ ] Cleanup SQL tested on staging database
- [ ] Orphaned records identified and counted before deletion
- [ ] Dual-read fallback removed after cleanup
- [ ] Verification: no records with `horizon_type IN ('pentad','decade')` AND `target != date` for ML models
