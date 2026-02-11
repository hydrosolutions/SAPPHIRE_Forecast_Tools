# Postprocessing Forecasts Module Improvement Plan

## Overview

This plan covers the comprehensive refactoring of the `apps/postprocessing_forecasts` module to:

1. **Fix critical bugs** causing incorrect exit codes and data loss
2. **Improve performance** by eliminating N+1 queries and inefficient iterations
3. **Separate concerns** into maintenance tasks (overnight) and operational tasks (real-time)
4. **Add comprehensive testing** with unit tests for each method and integration tests for workflows

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Current Status](#current-status-2026-01-29)
3. [Current State Analysis](#current-state-analysis)
4. [Proposed Architecture](#proposed-architecture)
5. [Phase 1: Critical Bug Fixes](#phase-1-critical-bug-fixes)
6. [Phase 2: Performance Improvements](#phase-2-performance-improvements)
7. [Phase 3: Module Separation](#phase-3-module-separation)
8. [Phase 4: Testing Strategy](#phase-4-testing-strategy)
9. [Implementation Checklist](#implementation-checklist)
10. [Files Affected](#files-affected)
11. [Migration Strategy](#migration-strategy)

---

## Executive Summary

### Current Problems

| Category | Count | Severity |
|----------|-------|----------|
| Critical Bugs | 5 | HIGH |
| Error Handling Gaps | 12+ | HIGH |
| Performance Bottlenecks | 6 | MEDIUM-HIGH |
| Test Coverage Gaps | 10+ | MEDIUM |

### Key Issues

1. **Return value tracking bug** - Script exits success (0) even when earlier operations fail
2. **N+1 database queries** - Server-side CRUD performs 1000 queries for 1000 records
3. **Silent API failures** - Errors logged but execution continues as "success"
4. **Monolithic design** - Compute-intensive skill metrics run in same path as time-critical forecast writes

### Proposed Solution

Split into two modules:
- **Operational** (`postprocessing_operational.py`) - Fast, real-time forecast writes
- **Maintenance** (`postprocessing_maintenance.py`) - Overnight skill metric calculations

---

## Current Status (2026-01-29)

### Phase 1 Progress: Tier 1 Bugs Fixed

| Bug | Status | Commit | Notes |
|-----|--------|--------|-------|
| 1.1 Return value masking | ✅ Complete | a52597d | Error accumulation pattern implemented |
| 1.2 Uninitialized variable | ✅ Complete | a52597d | Covered by 1.1 fix |
| 1.3 Unsafe `.iloc[0]` | ✅ Complete | a52597d | Empty check before access |
| 1.4 Non-atomic file ops | ✅ Complete | a52597d | `atomic_write_csv()` helper added |
| 1.5 Silent API failures | ⏸️ Deferred | - | Lower priority, will address later |

### Tests Added

| Test File | Tests | Coverage |
|-----------|-------|----------|
| `test_error_accumulation.py` (new) | 9 | All save operations, all prediction modes |
| `test_postprocessing_tools.py` | +5 | NaT dates, missing codes, empty results |
| `test_forecast_library.py` | +6 | Atomic write operations |

### Next Steps

1. ~~**Server Testing** - Deploy to test server and verify fixes work in production environment~~ ✅ Verified (2026-02-03)
2. **Phase 2** - Performance improvements (after UV migration complete)
3. **Phase 3** - Module separation (operational/maintenance split)

---

## Current State Analysis

### Module Structure

```
apps/postprocessing_forecasts/
├── postprocessing_forecasts.py      # Main entry point (monolithic)
├── src/
│   └── postprocessing_tools.py      # Logging utilities
├── tests/
│   ├── test_api_integration.py      # 16 API tests
│   ├── test_postprocessing_tools.py # 3 tool tests
│   └── test_mock_postprocessing_forecasts.py  # 1 integration test
├── Dockerfile
├── pyproject.toml
└── uv.lock

# Core logic lives in iEasyHydroForecast:
apps/iEasyHydroForecast/
└── forecast_library.py              # 8500+ lines, skill metrics, API writes
```

### Critical Bugs

#### Bug 1: Return Value Masking

**Location:** `postprocessing_forecasts.py:163-222`

**Problem:** The `ret` variable is overwritten by each save operation. Only the last operation's return value determines the exit code.

```python
ret = fl.save_forecast_data_pentad(modelled)          # Line 163
# ... more code ...
ret = fl.save_pentadal_skill_metrics(skill_metrics)   # Line 170 - OVERWRITES
# ... more code ...
ret = fl.save_forecast_data_decade(modelled_decade)   # Line 196 - OVERWRITES
# ... more code ...
ret = fl.save_decadal_skill_metrics(skill_metrics_decade)  # Line 203 - OVERWRITES

if ret is None:  # Only checks LAST operation
    sys.exit(0)  # SUCCESS even if pentad failed!
```

**Impact:** Production scripts report success when earlier operations failed, masking data loss.

---

#### Bug 2: Uninitialized Variable

**Location:** `postprocessing_forecasts.py:217`

**Problem:** When `SAPPHIRE_PREDICTION_MODE=DECAD`, the `ret` variable is never initialized before the final check.

```python
if prediction_mode in ['DECAD', 'BOTH']:
    # ret assigned here for DECAD mode
    ret = fl.save_decadal_skill_metrics(...)

if ret is None:  # NameError if mode was 'DECAD' only and pentad block didn't run
```

**Impact:** `NameError: name 'ret' is not defined` crash at runtime.

---

#### Bug 3: Unsafe Array Access

**Location:** `postprocessing_tools.py:81, 206`

**Problem:** `.iloc[0]` called without checking if DataFrame has rows.

```python
# Line 81
latest_date = modelled_data[modelled_data['date'] == max_date].iloc[0]['date']
# If no rows match, this crashes with IndexError
```

**Impact:** `IndexError: single positional indexer is out-of-bounds` when filtering yields empty results.

---

#### Bug 4: Non-Atomic File Operations

**Location:** `forecast_library.py:6125-6132, 6203-6210, 5264-5271, 5877-5884`

**Problem:** Files are deleted before new content is written.

```python
if os.path.exists(filepath):
    os.remove(filepath)  # File deleted
# If crash happens here, data is lost forever
ret = data.to_csv(filepath, index=False)
```

**Impact:** Data loss if process crashes between delete and write.

---

#### Bug 5: Silent API Failures

**Location:** `forecast_library.py:6140-6143, 6216-6221, 6355-6360`

**Problem:** API write errors are caught but swallowed; function returns success.

```python
try:
    _write_skill_metrics_to_api(data, "pentad")
except Exception as e:
    logger.error(f"Failed to write: {e}")
    # No re-raise, no return error indicator
# Function continues and returns None (success)
```

**Impact:** Callers see success while API data is actually missing.

---

### Performance Bottlenecks

| Issue | Location | Impact | Severity |
|-------|----------|--------|----------|
| `groupby().apply()` pattern | `forecast_library.py:1930-1960` | 3x full data scans | CRITICAL |
| N+1 database queries | `crud.py:10-49` | 1000 SELECT + 1000 commits | CRITICAL |
| `.iterrows()` loop | `forecast_library.py:4149, 4267` | Slow iteration over 22K+ records | HIGH |
| Per-row regex | `forecast_library.py:4163-4169` | Regex compiled fresh each row | HIGH |
| Client reinstantiation | `forecast_library.py:4118, 4236` | New HTTP connection per function | MEDIUM |
| Health check per function | 8 locations | 2-4 extra HTTP requests | LOW |

### Test Coverage Gaps

| Area | Current | Required |
|------|---------|----------|
| API error scenarios | 2 tests | Network timeouts, partial failures, retries |
| Skill metric calculations | 0 tests | NSE, MAE, accuracy, sdivsigma |
| Ensemble logic | 1 basic test | EM composition, NE filtering, thresholds |
| Large datasets | 0 tests | Performance benchmarks with 10K+ records |
| Edge cases | 0 tests | Leap years, month boundaries, empty data |
| Function name mismatch | Bug | `log_most_recent_forecasts()` vs actual names |

---

## Proposed Architecture

### Current (Monolithic)

```
postprocessing_forecasts.py
├── read data (ALL historical + latest)
├── calculate ALL skill metrics  ← SLOW (groupby.apply 3x)
├── calculate ensembles (EM, NE)
├── save ALL to CSV              ← DISK I/O
├── save ALL to API              ← N+1 QUERIES
└── log recent forecasts
```

### Proposed (Separated)

```
┌─────────────────────────────────────────────────────────────────┐
│  OPERATIONAL (postprocessing_operational.py)                    │
│  ├── read LATEST forecast data only (today's forecasts)         │
│  ├── calculate LATEST ensembles (EM, NE) for today only         │
│  ├── write LATEST forecasts to API (batch upsert)              │
│  └── log recent forecasts for monitoring                        │
│                                                                 │
│  Schedule: After each forecast cycle (multiple times/day)       │
│  Target execution: < 30 seconds                                 │
│  Priority: Time-critical for dashboard updates                  │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  MAINTENANCE (postprocessing_maintenance.py)                    │
│  ├── read FULL historical data                                  │
│  ├── calculate ALL skill metrics (vectorized, optimized)        │
│  ├── recalculate ALL ensemble compositions                      │
│  ├── write skill metrics to API (batch upsert)                  │
│  ├── write full CSV archives                                    │
│  └── run consistency checks (API vs CSV)                        │
│                                                                 │
│  Schedule: Overnight (2 AM cron)                                │
│  Target execution: Can take 5-10 minutes                        │
│  Priority: Data completeness, not speed                         │
└─────────────────────────────────────────────────────────────────┘
```

### Shared Components

```
apps/postprocessing_forecasts/
├── src/
│   ├── api_writer.py           # Shared API write logic (batch upsert)
│   ├── skill_metrics.py        # Vectorized skill metric calculations
│   ├── ensemble_calculator.py  # EM/NE ensemble logic
│   ├── data_reader.py          # Data loading utilities
│   └── postprocessing_tools.py # Existing logging utilities
├── postprocessing_operational.py   # NEW: Operational entry point
├── postprocessing_maintenance.py   # NEW: Maintenance entry point
├── postprocessing_forecasts.py     # DEPRECATED: Legacy entry point
└── tests/
    ├── unit/
    │   ├── test_api_writer.py
    │   ├── test_skill_metrics.py
    │   ├── test_ensemble_calculator.py
    │   └── test_data_reader.py
    └── integration/
        ├── test_operational_workflow.py
        ├── test_maintenance_workflow.py
        └── test_end_to_end.py
```

---

## Phase 1: Critical Bug Fixes

### 1.1 Fix Return Value Tracking

**File:** `postprocessing_forecasts.py`

**Current:**
```python
ret = fl.save_forecast_data_pentad(modelled)
# ... overwrites ...
ret = fl.save_decadal_skill_metrics(skill_metrics_decade)
if ret is None:
    sys.exit(0)
```

**Fixed:**
```python
errors = []

if prediction_mode in ['PENTAD', 'BOTH']:
    ret = fl.save_forecast_data_pentad(modelled)
    if ret is not None:
        errors.append(f"Pentad forecast save failed: {ret}")

    ret = fl.save_pentadal_skill_metrics(skill_metrics)
    if ret is not None:
        errors.append(f"Pentad skill metrics save failed: {ret}")

if prediction_mode in ['DECAD', 'BOTH']:
    ret = fl.save_forecast_data_decade(modelled_decade)
    if ret is not None:
        errors.append(f"Decade forecast save failed: {ret}")

    ret = fl.save_decadal_skill_metrics(skill_metrics_decade)
    if ret is not None:
        errors.append(f"Decade skill metrics save failed: {ret}")

if errors:
    for error in errors:
        logger.error(error)
    logger.error(f"Script finished with {len(errors)} error(s)")
    sys.exit(1)

logger.info(f"Script finished successfully at {dt.datetime.now()}")
sys.exit(0)
```

**Tests Required:**
- `test_all_saves_succeed_returns_exit_0`
- `test_first_save_fails_returns_exit_1`
- `test_middle_save_fails_returns_exit_1`
- `test_last_save_fails_returns_exit_1`
- `test_multiple_saves_fail_logs_all_errors`

---

### 1.2 Fix Uninitialized Variable

**File:** `postprocessing_forecasts.py`

**Current:** Variable `ret` may not be defined if mode is `DECAD` only.

**Fixed:** Use error accumulation pattern from 1.1 (eliminates the issue entirely).

**Tests Required:**
- `test_decad_only_mode_no_name_error`
- `test_pentad_only_mode_works`
- `test_both_mode_works`

---

### 1.3 Fix Unsafe Array Access

**File:** `postprocessing_tools.py:81, 206`

**Current:**
```python
latest_date = modelled_data[modelled_data['date'] == max_date].iloc[0]['date']
```

**Fixed:**
```python
filtered = modelled_data[modelled_data['date'] == max_date]
if filtered.empty:
    logger.warning(f"No data found for date {max_date}")
    return None
latest_date = filtered.iloc[0]['date']
```

**Tests Required:**
- `test_log_most_recent_forecasts_empty_dataframe`
- `test_log_most_recent_forecasts_no_matching_dates`
- `test_log_most_recent_forecasts_valid_data`

---

### 1.4 Fix Non-Atomic File Operations

**File:** `forecast_library.py` (multiple locations)

**Current:**
```python
if os.path.exists(filepath):
    os.remove(filepath)
data.to_csv(filepath, index=False)
```

**Fixed:**
```python
import tempfile
import shutil

# Write to temp file first
temp_fd, temp_path = tempfile.mkstemp(suffix='.csv', dir=os.path.dirname(filepath))
try:
    data.to_csv(temp_path, index=False)
    os.close(temp_fd)
    # Atomic rename (on same filesystem)
    shutil.move(temp_path, filepath)
except Exception:
    os.close(temp_fd)
    if os.path.exists(temp_path):
        os.remove(temp_path)
    raise
```

**Tests Required:**
- `test_atomic_write_success`
- `test_atomic_write_crash_preserves_original`
- `test_atomic_write_permissions_error`

---

### 1.5 Fix Silent API Failures

**File:** `forecast_library.py:6140-6143, 6216-6221`

**Current:**
```python
try:
    _write_skill_metrics_to_api(data, "pentad")
except Exception as e:
    logger.error(f"Failed to write: {e}")
    # Silent failure - function returns success
```

**Fixed (Option A - Propagate errors):**
```python
api_errors = []

try:
    _write_skill_metrics_to_api(data, "pentad")
except SapphireAPIError as e:
    api_errors.append(f"API write failed: {e}")
    logger.error(f"Failed to write skill metrics to API: {e}")

# Return error indicator
if api_errors:
    return f"API errors: {'; '.join(api_errors)}"
return None  # Success
```

**Fixed (Option B - Configurable behavior):**
```python
api_failure_mode = os.getenv("SAPPHIRE_API_FAILURE_MODE", "warn")  # "warn", "fail", "ignore"

try:
    _write_skill_metrics_to_api(data, "pentad")
except SapphireAPIError as e:
    if api_failure_mode == "fail":
        raise  # Propagate to caller
    elif api_failure_mode == "warn":
        logger.warning(f"API write failed (continuing): {e}")
    # "ignore" - do nothing
```

**Tests Required:**
- `test_api_error_propagated_when_fail_mode`
- `test_api_error_logged_when_warn_mode`
- `test_api_error_silent_when_ignore_mode`
- `test_api_success_returns_none`

---

## Phase 2: Performance Improvements

### 2.1 Replace N+1 Queries with Batch Upsert (Server-Side)

**File:** `sapphire/services/postprocessing/app/crud.py`

**Current (N+1 pattern):**
```python
def create_forecast(db: Session, bulk_data: ForecastBulkCreate):
    for item in bulk_data.data:  # 1000 iterations
        existing = db.query(Forecast).filter(...).first()  # 1000 SELECTs
        if existing:
            # Update
        else:
            db.add(new_forecast)
        db.commit()  # 1000 commits!
```

**Fixed (Batch upsert):**
```python
from sqlalchemy.dialects.postgresql import insert

def upsert_forecasts(db: Session, records: list[dict]) -> int:
    """Batch upsert forecasts using PostgreSQL ON CONFLICT."""
    if not records:
        return 0

    stmt = insert(Forecast).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=['horizon_type', 'code', 'model_type', 'date', 'target'],
        set_={
            'horizon_value': stmt.excluded.horizon_value,
            'horizon_in_year': stmt.excluded.horizon_in_year,
            'forecasted_discharge': stmt.excluded.forecasted_discharge,
            'composition': stmt.excluded.composition,
            'q05': stmt.excluded.q05,
            'q25': stmt.excluded.q25,
            'q50': stmt.excluded.q50,
            'q75': stmt.excluded.q75,
            'q95': stmt.excluded.q95,
        }
    )
    result = db.execute(stmt)
    db.commit()
    return result.rowcount
```

**Tests Required:**
- `test_upsert_creates_new_records`
- `test_upsert_updates_existing_records`
- `test_upsert_handles_mixed_create_update`
- `test_upsert_performance_1000_records`
- `test_upsert_rollback_on_error`

---

### 2.2 Replace iterrows() with Vectorized Operations (Client-Side)

**File:** `forecast_library.py:4149-4190, 4267-4310`

**Current:**
```python
records = []
for _, row in data.iterrows():  # SLOW
    if pd.isna(row.get(horizon_value_col)):
        continue
    record = {
        "code": str(row['code']),
        "date": pd.to_datetime(row['date']).strftime('%Y-%m-%d'),
        # ... more fields
    }
    records.append(record)
```

**Fixed:**
```python
def build_forecast_records(data: pd.DataFrame, horizon_type: str) -> list[dict]:
    """Build API records using vectorized pandas operations."""
    if data.empty:
        return []

    # Determine column names based on horizon type
    if horizon_type == "pentad":
        horizon_value_col = 'pentad_in_month'
        horizon_in_year_col = 'pentad_in_year'
    else:
        horizon_value_col = 'decad'
        horizon_in_year_col = 'decad_in_year'

    # Pre-filter invalid rows (vectorized)
    mask = data[horizon_value_col].notna() & data[horizon_in_year_col].notna()
    valid_data = data[mask].copy()

    if valid_data.empty:
        return []

    # Vectorized model type mapping
    model_type_map = {
        "LR": "LR", "TFT": "TFT", "TIDE": "TiDE",
        "TSMIXER": "TSMixer", "EM": "EM", "NE": "NE", "RRAM": "RRAM"
    }
    valid_data['model_type'] = valid_data['model_short'].str.upper().map(model_type_map)

    # Vectorized date formatting
    valid_data['date_str'] = pd.to_datetime(valid_data['date']).dt.strftime('%Y-%m-%d')

    # Vectorized composition extraction for ensemble models
    valid_data['composition'] = None
    ensemble_mask = valid_data['model_short'].str.upper().isin(['EM', 'NE'])
    if ensemble_mask.any() and 'model_long' in valid_data.columns:
        valid_data.loc[ensemble_mask, 'composition'] = (
            valid_data.loc[ensemble_mask, 'model_long']
            .str.extract(r'with\s+(.+?)\s+\([EN][ME]\)', expand=False)
            .str.strip()
        )

    # Build records using to_dict (much faster than iterrows)
    records = []
    for _, row in valid_data.iterrows():  # Still iterrows but on filtered data
        record = {
            "horizon_type": horizon_type,
            "code": str(row['code']),
            "model_type": row['model_type'],
            "date": row['date_str'],
            "target": row['date_str'],
            "horizon_value": int(row[horizon_value_col]),
            "horizon_in_year": int(row[horizon_in_year_col]),
            "forecasted_discharge": None if pd.isna(row.get('forecasted_discharge')) else float(row['forecasted_discharge']),
            "composition": row['composition'],
        }
        records.append(record)

    return records
```

**Even better - fully vectorized:**
```python
def build_forecast_records_vectorized(data: pd.DataFrame, horizon_type: str) -> list[dict]:
    """Build API records using fully vectorized pandas operations."""
    # ... filtering and preparation same as above ...

    # Select and rename columns for API
    api_columns = {
        'code': 'code',
        'date_str': 'date',
        'model_type': 'model_type',
        horizon_value_col: 'horizon_value',
        horizon_in_year_col: 'horizon_in_year',
        'forecasted_discharge': 'forecasted_discharge',
        'composition': 'composition',
    }

    result = valid_data[list(api_columns.keys())].rename(columns=api_columns)
    result['horizon_type'] = horizon_type
    result['target'] = result['date']

    # Convert NaN to None and ensure correct types
    result = result.where(pd.notna(result), None)
    result['horizon_value'] = result['horizon_value'].astype(int)
    result['horizon_in_year'] = result['horizon_in_year'].astype(int)

    return result.to_dict('records')
```

**Tests Required:**
- `test_build_records_empty_dataframe`
- `test_build_records_filters_nan_values`
- `test_build_records_model_type_mapping`
- `test_build_records_composition_extraction`
- `test_build_records_performance_benchmark`

---

### 2.3 Optimize Skill Metric Calculation

**File:** `forecast_library.py:1930-1960` (pentad), `2234-2266` (decade)

**Current (3 separate groupby.apply operations):**
```python
skill_stats = df.groupby(group_cols).apply(sdivsigma_nse, ...)
skill_stats = skill_stats.merge(df.groupby(group_cols).apply(mae, ...), ...)
skill_stats = skill_stats.merge(df.groupby(group_cols).apply(forecast_accuracy, ...), ...)
```

**Fixed (Single groupby with multiple aggregations):**
```python
def calculate_all_metrics(group: pd.DataFrame, observed_col: str, simulated_col: str) -> pd.Series:
    """Calculate all skill metrics in a single pass."""
    observed = group[observed_col].dropna().values
    simulated = group[simulated_col].dropna().values

    # Align arrays
    mask = ~(np.isnan(observed) | np.isnan(simulated))
    obs = observed[mask]
    sim = simulated[mask]

    n_pairs = len(obs)
    if n_pairs < 2:
        return pd.Series({
            'sdivsigma': np.nan, 'nse': np.nan, 'mae': np.nan,
            'delta': np.nan, 'accuracy': np.nan, 'n_pairs': n_pairs
        })

    # Calculate all metrics
    mean_obs = obs.mean()
    std_obs = obs.std(ddof=1)

    residuals = obs - sim
    ss_res = np.sum(residuals ** 2)
    ss_tot = np.sum((obs - mean_obs) ** 2)

    # NSE and sdivsigma
    nse = 1 - (ss_res / ss_tot) if ss_tot > 1e-10 else np.nan
    rmse = np.sqrt(ss_res / (n_pairs - 1))
    sdivsigma = rmse / std_obs if std_obs > 1e-10 else np.nan

    # MAE
    mae = np.mean(np.abs(residuals))

    # Accuracy (if delta available)
    delta = group['delta'].iloc[0] if 'delta' in group.columns else np.nan
    if not np.isnan(delta):
        accuracy = np.mean(np.abs(residuals) <= delta)
    else:
        accuracy = np.nan

    return pd.Series({
        'sdivsigma': sdivsigma, 'nse': nse, 'mae': mae,
        'delta': delta, 'accuracy': accuracy, 'n_pairs': n_pairs
    })

# Single groupby operation
skill_stats = df.groupby(group_cols).apply(
    calculate_all_metrics,
    observed_col='discharge_avg',
    simulated_col='forecasted_discharge'
).reset_index()
```

**Tests Required:**
- `test_calculate_all_metrics_basic`
- `test_calculate_all_metrics_insufficient_data`
- `test_calculate_all_metrics_perfect_forecast`
- `test_calculate_all_metrics_matches_individual_functions`
- `test_skill_metrics_performance_benchmark`

---

### 2.4 Reuse API Client

**File:** `forecast_library.py`

**Current:** Client created in each function.

**Fixed:** Create module-level client factory with connection reuse.

```python
# At module level
_api_client = None

def get_api_client() -> SapphirePostprocessingClient:
    """Get or create a reusable API client."""
    global _api_client
    if _api_client is None:
        api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
        _api_client = SapphirePostprocessingClient(base_url=api_url)
    return _api_client

def reset_api_client():
    """Reset client (for testing or reconnection)."""
    global _api_client
    _api_client = None
```

**Tests Required:**
- `test_get_api_client_creates_once`
- `test_get_api_client_reuses_instance`
- `test_reset_api_client_clears_instance`

---

## Phase 3: Module Separation

### 3.1 Create Operational Module

**New File:** `postprocessing_operational.py`

```python
"""
Operational postprocessing for real-time forecast updates.

This module handles time-critical forecast writes to the SAPPHIRE API.
It processes only the latest forecasts and is designed for fast execution.

Schedule: After each forecast cycle
Target execution: < 30 seconds
"""

import datetime as dt
import logging
import os
import sys

from src.api_writer import write_forecasts_to_api
from src.ensemble_calculator import calculate_latest_ensembles
from src.data_reader import read_latest_forecasts
from src.postprocessing_tools import log_most_recent_forecasts_pentad, log_most_recent_forecasts_decade

logger = logging.getLogger(__name__)


def postprocessing_operational() -> int:
    """
    Run operational postprocessing for latest forecasts.

    Returns:
        0 on success, 1 on failure
    """
    errors = []
    prediction_mode = os.getenv("SAPPHIRE_PREDICTION_MODE", "BOTH").upper()

    logger.info(f"Starting operational postprocessing (mode: {prediction_mode})")
    start_time = dt.datetime.now()

    try:
        if prediction_mode in ['PENTAD', 'BOTH']:
            # Read only latest pentad forecasts
            latest_pentad = read_latest_forecasts("pentad")

            if not latest_pentad.empty:
                # Calculate ensembles for today only
                with_ensembles = calculate_latest_ensembles(latest_pentad, "pentad")

                # Write to API
                result = write_forecasts_to_api(with_ensembles, "pentad")
                if result.errors:
                    errors.extend(result.errors)
                logger.info(f"Wrote {result.success_count} pentad forecasts to API")

                # Log for monitoring
                log_most_recent_forecasts_pentad(with_ensembles)

        if prediction_mode in ['DECAD', 'BOTH']:
            # Same pattern for decad
            latest_decad = read_latest_forecasts("decade")

            if not latest_decad.empty:
                with_ensembles = calculate_latest_ensembles(latest_decad, "decade")
                result = write_forecasts_to_api(with_ensembles, "decade")
                if result.errors:
                    errors.extend(result.errors)
                logger.info(f"Wrote {result.success_count} decade forecasts to API")
                log_most_recent_forecasts_decade(with_ensembles)

    except Exception as e:
        logger.exception(f"Unexpected error in operational postprocessing: {e}")
        errors.append(str(e))

    elapsed = (dt.datetime.now() - start_time).total_seconds()

    if errors:
        logger.error(f"Operational postprocessing completed with {len(errors)} error(s) in {elapsed:.1f}s")
        for error in errors:
            logger.error(f"  - {error}")
        return 1

    logger.info(f"Operational postprocessing completed successfully in {elapsed:.1f}s")
    return 0


if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    sys.exit(postprocessing_operational())
```

---

### 3.2 Create Maintenance Module

**New File:** `postprocessing_maintenance.py`

```python
"""
Maintenance postprocessing for overnight skill metric calculations.

This module handles compute-intensive skill metric calculations and
full historical data processing. Designed for overnight execution.

Schedule: Nightly at 2 AM
Target execution: 5-10 minutes acceptable
"""

import datetime as dt
import logging
import os
import sys

from src.api_writer import write_skill_metrics_to_api, write_forecasts_to_api
from src.skill_metrics import calculate_skill_metrics_pentad, calculate_skill_metrics_decade
from src.ensemble_calculator import calculate_all_ensembles
from src.data_reader import read_all_observed_and_modelled
from src.file_writer import save_forecasts_to_csv, save_skill_metrics_to_csv
from src.consistency_checker import verify_api_csv_consistency

logger = logging.getLogger(__name__)


def postprocessing_maintenance() -> int:
    """
    Run maintenance postprocessing for skill metrics and full data sync.

    Returns:
        0 on success, 1 on failure
    """
    errors = []
    prediction_mode = os.getenv("SAPPHIRE_PREDICTION_MODE", "BOTH").upper()
    run_consistency_check = os.getenv("SAPPHIRE_CONSISTENCY_CHECK", "false").lower() == "true"

    logger.info(f"Starting maintenance postprocessing (mode: {prediction_mode})")
    start_time = dt.datetime.now()

    try:
        if prediction_mode in ['PENTAD', 'BOTH']:
            logger.info("Processing pentad data...")

            # Read all historical data
            observed, modelled = read_all_observed_and_modelled("pentad")

            # Calculate ensembles
            modelled_with_ensembles = calculate_all_ensembles(modelled, "pentad")

            # Calculate skill metrics (vectorized)
            skill_metrics = calculate_skill_metrics_pentad(observed, modelled_with_ensembles)

            # Save to CSV (atomic writes)
            csv_errors = save_forecasts_to_csv(modelled_with_ensembles, "pentad")
            errors.extend(csv_errors)

            csv_errors = save_skill_metrics_to_csv(skill_metrics, "pentad")
            errors.extend(csv_errors)

            # Write to API (batch upsert)
            result = write_forecasts_to_api(modelled_with_ensembles, "pentad")
            if result.errors:
                errors.extend(result.errors)

            result = write_skill_metrics_to_api(skill_metrics, "pentad")
            if result.errors:
                errors.extend(result.errors)

            # Optional consistency check
            if run_consistency_check:
                consistency_errors = verify_api_csv_consistency("pentad")
                errors.extend(consistency_errors)

        if prediction_mode in ['DECAD', 'BOTH']:
            logger.info("Processing decade data...")
            # Same pattern as pentad...
            observed, modelled = read_all_observed_and_modelled("decade")
            modelled_with_ensembles = calculate_all_ensembles(modelled, "decade")
            skill_metrics = calculate_skill_metrics_decade(observed, modelled_with_ensembles)

            csv_errors = save_forecasts_to_csv(modelled_with_ensembles, "decade")
            errors.extend(csv_errors)

            csv_errors = save_skill_metrics_to_csv(skill_metrics, "decade")
            errors.extend(csv_errors)

            result = write_forecasts_to_api(modelled_with_ensembles, "decade")
            if result.errors:
                errors.extend(result.errors)

            result = write_skill_metrics_to_api(skill_metrics, "decade")
            if result.errors:
                errors.extend(result.errors)

            if run_consistency_check:
                consistency_errors = verify_api_csv_consistency("decade")
                errors.extend(consistency_errors)

    except Exception as e:
        logger.exception(f"Unexpected error in maintenance postprocessing: {e}")
        errors.append(str(e))

    elapsed = (dt.datetime.now() - start_time).total_seconds()

    if errors:
        logger.error(f"Maintenance postprocessing completed with {len(errors)} error(s) in {elapsed:.1f}s")
        for error in errors:
            logger.error(f"  - {error}")
        return 1

    logger.info(f"Maintenance postprocessing completed successfully in {elapsed:.1f}s")
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    sys.exit(postprocessing_maintenance())
```

---

### 3.3 Create Shell Scripts

**New File:** `bin/run_operational_postprocessing.sh`

```bash
#!/bin/bash
# Run operational postprocessing after each forecast cycle
# Usage: ./bin/run_operational_postprocessing.sh /path/to/.env

set -e

ENV_FILE="${1:-$ieasyhydroforecast_env_file_path}"
if [ -z "$ENV_FILE" ]; then
    echo "Error: No .env file specified"
    exit 1
fi

export ieasyhydroforecast_env_file_path="$ENV_FILE"
export SAPPHIRE_POSTPROCESSING_MODE="operational"

cd "$(dirname "$0")/.."
PYTHONPATH=apps/iEasyHydroForecast python apps/postprocessing_forecasts/postprocessing_operational.py
```

**New File:** `bin/run_maintenance_postprocessing.sh`

```bash
#!/bin/bash
# Run maintenance postprocessing overnight
# Usage: ./bin/run_maintenance_postprocessing.sh /path/to/.env
# Cron: 0 2 * * * /path/to/bin/run_maintenance_postprocessing.sh /path/to/.env

set -e

ENV_FILE="${1:-$ieasyhydroforecast_env_file_path}"
if [ -z "$ENV_FILE" ]; then
    echo "Error: No .env file specified"
    exit 1
fi

export ieasyhydroforecast_env_file_path="$ENV_FILE"
export SAPPHIRE_POSTPROCESSING_MODE="maintenance"

cd "$(dirname "$0")/.."
PYTHONPATH=apps/iEasyHydroForecast python apps/postprocessing_forecasts/postprocessing_maintenance.py
```

---

## Phase 4: Testing Strategy

### 4.1 Unit Test Structure

```
apps/postprocessing_forecasts/tests/
├── unit/
│   ├── test_api_writer.py           # API write functions
│   ├── test_skill_metrics.py        # Skill metric calculations
│   ├── test_ensemble_calculator.py  # Ensemble logic
│   ├── test_data_reader.py          # Data loading
│   ├── test_file_writer.py          # CSV writing
│   └── test_postprocessing_tools.py # Logging utilities
└── integration/
    ├── test_operational_workflow.py
    ├── test_maintenance_workflow.py
    └── test_end_to_end.py
```

### 4.2 Unit Tests Required

#### test_api_writer.py

| Test | Description |
|------|-------------|
| `test_build_forecast_records_empty_df` | Empty DataFrame returns empty list |
| `test_build_forecast_records_filters_nan` | Rows with NaN required fields filtered |
| `test_build_forecast_records_model_mapping` | TIDE→TiDE, TSMIXER→TSMixer |
| `test_build_forecast_records_composition` | EM/NE get composition extracted |
| `test_write_forecasts_api_disabled` | Returns False when API disabled |
| `test_write_forecasts_api_not_ready` | Raises SapphireAPIError |
| `test_write_forecasts_success` | Returns count of written records |
| `test_write_forecasts_partial_failure` | Handles partial batch failures |
| `test_write_skill_metrics_empty_df` | Empty data returns False |
| `test_write_skill_metrics_nan_conversion` | NaN metrics become None |
| `test_write_skill_metrics_success` | Returns count of written records |
| `test_get_api_client_singleton` | Client reused across calls |

#### test_skill_metrics.py

| Test | Description |
|------|-------------|
| `test_calculate_nse_perfect_forecast` | NSE = 1.0 for perfect match |
| `test_calculate_nse_mean_forecast` | NSE = 0.0 for mean prediction |
| `test_calculate_nse_worse_than_mean` | NSE < 0 for poor prediction |
| `test_calculate_sdivsigma_zero_variance` | Handles zero variance in observed |
| `test_calculate_mae_basic` | Correct MAE calculation |
| `test_calculate_accuracy_all_within_delta` | Accuracy = 1.0 |
| `test_calculate_accuracy_none_within_delta` | Accuracy = 0.0 |
| `test_calculate_all_metrics_single_pass` | All metrics in one groupby |
| `test_calculate_all_metrics_matches_individual` | Same as separate calculations |
| `test_skill_metrics_pentad_grouping` | Correct groupby columns |
| `test_skill_metrics_decade_grouping` | Correct groupby columns |
| `test_skill_metrics_insufficient_data` | NaN for < 2 pairs |

#### test_ensemble_calculator.py

| Test | Description |
|------|-------------|
| `test_calculate_em_all_models` | EM = mean of all models |
| `test_calculate_ne_neural_only` | NE = mean of TFT, TiDE, TSMixer |
| `test_ensemble_composition_tracking` | Composition field populated |
| `test_filter_highly_skilled_efficiency` | sdivsigma threshold applied |
| `test_filter_highly_skilled_accuracy` | accuracy threshold applied |
| `test_filter_highly_skilled_nse` | nse threshold applied |
| `test_ensemble_missing_models` | Handles missing model gracefully |
| `test_latest_ensemble_single_date` | Only processes latest date |

#### test_data_reader.py

| Test | Description |
|------|-------------|
| `test_read_latest_forecasts_pentad` | Reads only latest date |
| `test_read_latest_forecasts_decade` | Reads only latest date |
| `test_read_all_observed_modelled` | Reads full historical data |
| `test_read_handles_missing_file` | Graceful error handling |
| `test_read_handles_empty_file` | Returns empty DataFrame |

#### test_file_writer.py

| Test | Description |
|------|-------------|
| `test_atomic_write_success` | File written correctly |
| `test_atomic_write_preserves_on_error` | Original preserved on failure |
| `test_save_forecasts_csv_columns` | Correct column order |
| `test_save_skill_metrics_csv_columns` | Correct column order |
| `test_save_handles_directory_creation` | Creates directory if missing |

#### test_postprocessing_tools.py

| Test | Description |
|------|-------------|
| `test_log_most_recent_forecasts_pentad` | Correct pivot and output |
| `test_log_most_recent_forecasts_decade` | Correct pivot and output |
| `test_log_empty_dataframe` | Handles empty data |
| `test_log_no_matching_dates` | Handles filter yielding nothing |
| `test_log_creates_output_directory` | Creates directory if missing |

### 4.3 Integration Tests Required

#### test_operational_workflow.py

| Test | Description |
|------|-------------|
| `test_operational_pentad_only` | Full pentad workflow |
| `test_operational_decade_only` | Full decade workflow |
| `test_operational_both_modes` | Pentad + decade workflow |
| `test_operational_api_failure_handling` | Continues on API error |
| `test_operational_empty_data` | Handles no new forecasts |
| `test_operational_execution_time` | Completes < 30 seconds |

#### test_maintenance_workflow.py

| Test | Description |
|------|-------------|
| `test_maintenance_full_pentad` | Full pentad workflow with skill |
| `test_maintenance_full_decade` | Full decade workflow with skill |
| `test_maintenance_both_modes` | Pentad + decade workflow |
| `test_maintenance_csv_api_consistency` | CSV matches API after run |
| `test_maintenance_ensemble_recalculation` | All ensembles updated |
| `test_maintenance_skill_metrics_updated` | All metrics recalculated |

#### test_end_to_end.py

| Test | Description |
|------|-------------|
| `test_operational_then_maintenance` | Sequential workflow |
| `test_dashboard_can_read_after_operational` | Dashboard API works |
| `test_historical_data_preserved` | No data loss in maintenance |
| `test_recovery_from_partial_failure` | Can resume after crash |

### 4.4 Test Fixtures

**File:** `tests/conftest.py`

```python
import pytest
import pandas as pd
import numpy as np
from datetime import date, timedelta

@pytest.fixture
def sample_forecast_data():
    """Generate sample forecast data for testing."""
    codes = ['15013', '15016', '15020']
    models = ['LR', 'TFT', 'TIDE', 'TSMIXER']
    dates = [date.today() - timedelta(days=i) for i in range(5)]

    data = []
    for code in codes:
        for model in models:
            for d in dates:
                for pentad in range(1, 7):
                    data.append({
                        'code': code,
                        'date': d,
                        'pentad_in_month': pentad,
                        'pentad_in_year': (d.month - 1) * 6 + pentad,
                        'model_short': model,
                        'model_long': f'{model} (Linear Regression)',
                        'forecasted_discharge': np.random.uniform(10, 100),
                    })
    return pd.DataFrame(data)

@pytest.fixture
def sample_observed_data():
    """Generate sample observed data for skill metric testing."""
    # ... similar structure ...

@pytest.fixture
def mock_api_client():
    """Create mock API client for testing."""
    from unittest.mock import Mock
    client = Mock()
    client.readiness_check.return_value = True
    client.write_forecasts.return_value = 100
    client.write_skill_metrics.return_value = 50
    return client

@pytest.fixture
def temp_data_directory(tmp_path):
    """Create temporary directory structure for testing."""
    (tmp_path / 'forecasts').mkdir()
    (tmp_path / 'skill_metrics').mkdir()
    (tmp_path / 'logs').mkdir()
    return tmp_path
```

### 4.5 Running Tests

```bash
# Run all unit tests
cd apps
SAPPHIRE_TEST_ENV=True pytest postprocessing_forecasts/tests/unit -v

# Run all integration tests
SAPPHIRE_TEST_ENV=True pytest postprocessing_forecasts/tests/integration -v

# Run with coverage
SAPPHIRE_TEST_ENV=True pytest postprocessing_forecasts/tests --cov=postprocessing_forecasts --cov-report=html

# Run performance benchmarks
SAPPHIRE_TEST_ENV=True pytest postprocessing_forecasts/tests -k "benchmark" -v --benchmark-only
```

---

## Implementation Checklist

### Phase 1: Critical Bug Fixes

- [x] **1.1** Fix return value tracking (accumulate errors) - Commit a52597d (2026-01-29)
- [x] **1.2** Fix uninitialized variable (covered by 1.1) - Commit a52597d (2026-01-29)
- [x] **1.3** Fix unsafe `.iloc[0]` access - Commit a52597d (2026-01-29)
- [x] **1.4** Implement atomic file writes - Commit a52597d (2026-01-29)
- [ ] **1.5** Make API failures configurable (fail/warn/ignore) - Deferred (lower priority)
- [x] **Tests** Write unit tests for all bug fixes - 20 tests added (9 error accumulation, 5 safe iloc, 6 atomic write)

**Status:** Bugs 1.1-1.4 implemented and tested locally. GitHub Actions CI running. Awaiting server deployment testing.

### Phase 2: Performance Improvements

- [ ] **2.1** Implement batch upsert in CRUD (server-side)
- [ ] **2.2** Replace iterrows() with vectorized operations
- [ ] **2.3** Combine skill metric calculations into single pass
- [ ] **2.4** Implement API client singleton
- [ ] **Tests** Write performance benchmarks

### Phase 3: Module Separation

- [ ] **3.1** Create `src/api_writer.py`
- [ ] **3.2** Create `src/skill_metrics.py`
- [ ] **3.3** Create `src/ensemble_calculator.py`
- [ ] **3.4** Create `src/data_reader.py`
- [ ] **3.5** Create `src/file_writer.py`
- [ ] **3.6** Create `postprocessing_operational.py`
- [ ] **3.7** Create `postprocessing_maintenance.py`
- [ ] **3.8** Create shell scripts
- [ ] **3.9** Update Dockerfile for dual entry points
- [ ] **3.10** Deprecate old `postprocessing_forecasts.py`
- [ ] **Tests** Write integration tests for both workflows

### Phase 4: Testing

- [ ] **4.1** Create test directory structure
- [ ] **4.2** Write all unit tests (50+ tests)
- [ ] **4.3** Write integration tests (12+ tests)
- [ ] **4.4** Write test fixtures and conftest.py
- [ ] **4.5** Add pytest configuration
- [ ] **4.6** Document test running procedures

---

## Files Affected

### Modified Files

| File | Changes |
|------|---------|
| `apps/postprocessing_forecasts/postprocessing_forecasts.py` | Bug fixes, then deprecation |
| `apps/postprocessing_forecasts/src/postprocessing_tools.py` | Fix unsafe array access |
| `apps/iEasyHydroForecast/forecast_library.py` | Atomic writes, vectorized operations |
| `sapphire/services/postprocessing/app/crud.py` | Batch upsert implementation |

### New Files

| File | Purpose |
|------|---------|
| `apps/postprocessing_forecasts/src/api_writer.py` | API write logic |
| `apps/postprocessing_forecasts/src/skill_metrics.py` | Vectorized skill calculations |
| `apps/postprocessing_forecasts/src/ensemble_calculator.py` | EM/NE ensemble logic |
| `apps/postprocessing_forecasts/src/data_reader.py` | Data loading utilities |
| `apps/postprocessing_forecasts/src/file_writer.py` | Atomic CSV writing |
| `apps/postprocessing_forecasts/postprocessing_operational.py` | Operational entry point |
| `apps/postprocessing_forecasts/postprocessing_maintenance.py` | Maintenance entry point |
| `bin/run_operational_postprocessing.sh` | Operational shell script |
| `bin/run_maintenance_postprocessing.sh` | Maintenance shell script |
| `apps/postprocessing_forecasts/tests/unit/*.py` | Unit test files |
| `apps/postprocessing_forecasts/tests/integration/*.py` | Integration test files |
| `apps/postprocessing_forecasts/tests/conftest.py` | Test fixtures |

---

## Migration Strategy

### Step 1: Bug Fixes (No Breaking Changes)

1. Apply all Phase 1 fixes to existing `postprocessing_forecasts.py`
2. Run existing tests to verify no regressions
3. Deploy to staging, verify with real data
4. Deploy to production

### Step 2: Performance Improvements (No Breaking Changes)

1. Apply server-side batch upsert
2. Apply client-side vectorization
3. Benchmark before/after
4. Deploy incrementally

### Step 3: Module Separation (Gradual Rollout)

1. Create new modules alongside existing code
2. Add environment variable to select mode:
   ```bash
   SAPPHIRE_POSTPROCESSING_ENTRY="legacy"      # Use old postprocessing_forecasts.py
   SAPPHIRE_POSTPROCESSING_ENTRY="operational" # Use new operational module
   SAPPHIRE_POSTPROCESSING_ENTRY="maintenance" # Use new maintenance module
   ```
3. Run both in parallel in staging, compare outputs
4. Gradually migrate production to new modules
5. Deprecate old module after validation period

### Step 4: Deprecation

1. Add deprecation warning to `postprocessing_forecasts.py`
2. Update all documentation
3. Remove old module after 1-2 release cycles

---

## Related Issues

- **PP-001**: Duplicate Skill Metrics for Ensemble Mean (`issues/gi_duplicate_skill_metrics_ensemble_composition.md`)
- **SAPPHIRE API Integration Plan**: `doc/plans/sapphire_api_integration_plan.md`

---

## Revision History

| Date | Author | Changes |
|------|--------|---------|
| 2026-01-24 | Claude | Initial plan created |
| 2026-01-29 | Claude | Phase 1 bugs 1.1-1.4 implemented, awaiting server testing |
