# Postprocessing Forecasts — Unified Refactoring Plan

> Integrates `postprocessing_forecasts_improvement_plan.md` and
> `postprocessing_refactoring_plan.md` into a single actionable document.

---

## Table of Contents

1. [Status Summary](#status-summary)
2. [Current Architecture](#current-architecture)
3. [Phase 1: Bug Fixes](#phase-1-bug-fixes-merge-main)
4. [Phase 2: Module Separation (Daily vs Maintenance)](#phase-2-module-separation)
5. [Phase 3: Performance Improvements](#phase-3-performance-improvements)
6. [Phase 4: Quarterly & Seasonal Aggregation](#phase-4-quarterly--seasonal-aggregation)
7. [Phase 5: Testing Strategy](#phase-5-testing-strategy)
8. [Implementation Checklist](#implementation-checklist)
9. [Files Affected](#files-affected)
10. [Migration Strategy](#migration-strategy)
11. [Appendix: Skill Metrics & Ensemble Details](#appendix-skill-metrics--ensemble-details)
12. [Appendix: Current Code Reference](#appendix-current-code-reference)
13. [Related Documents](#related-documents)

---

## Status Summary

| Item | Status |
|------|--------|
| Bugs 1–4 (return value masking, uninitialized var, unsafe `.iloc[0]`, non-atomic writes) | **DONE** (commit `a52597d`, merged via PR #290) |
| Bug 5 (silent API failures) | **DONE** — `SAPPHIRE_API_FAILURE_MODE` env var (warn/fail/ignore), 7 tests |
| Config fix: missing `ieasyforecast_decadal_skill_metrics_file` | **DONE** — added to `apps/config/.env` |
| API read tests (postprocessing) | **DONE** — 45 tests in `test_api_read.py`, 16 tests in `test_api_integration.py` |
| API write test fix | **DONE** — `test_api_read.py` mocks corrected to use `SapphirePreprocessingClient` (commit `ca29b5d`) |
| `sapphire-api-client` dependency | **DONE** — added to `iEasyHydroForecast/pyproject.toml` and `postprocessing_forecasts/pyproject.toml` |
| Module separation (daily vs maintenance) | TODO — both original plans agree on this |
| Server-side batch upsert (CRUD) | TODO |
| Client-side vectorization | TODO |
| Skill metrics single-pass optimization | TODO |
| Quarterly/seasonal aggregation | TODO (future) |
| Comprehensive test suite (50+ unit, 12+ integration) | **IN PROGRESS** — 79 postprocessing tests + 206 iEasyHydroForecast tests pass |
| Bulk-read API endpoints (for `long_term_forecasting`) | Planned — see `doc/plans/bulk_read_endpoints_instructions.md` |
| API integration | **DONE** — see `doc/plans/sapphire_api_integration_plan.md` |
| Duplicate skill metrics / ensemble composition issue | **RESOLVED** — see `doc/plans/issues/gi_duplicate_skill_metrics_ensemble_composition.md` |

### Pre-requisites (all completed)

1. ~~**Merge `main` into `develop_long_term`**~~ — Done (PR #290, commit `ab1e2ab`)
2. ~~Verify Tier 1 fixes pass on merged branch~~ — All 79 postprocessing + 206 iEasyHydroForecast tests pass

---

## Current Architecture

### Module Structure

```
apps/postprocessing_forecasts/
├── postprocessing_forecasts.py      # Main entry point (monolithic)
├── src/
│   ├── __init__.py                  # Package init
│   └── postprocessing_tools.py      # Logging utilities
├── tests/
│   ├── test_api_integration.py      # 16 tests (API write: skill metrics, combined forecasts)
│   ├── test_api_read.py             # 45 tests (API read: LR, ML, observed data, fallback)
│   ├── test_error_accumulation.py   # 9 tests (Tier 1: return value tracking)
│   ├── test_postprocessing_tools.py # 8 tests (3 existing + 5 Tier 1: safe .iloc[0])
│   └── test_mock_postprocessing_forecasts.py  # 1 integration test
├── pyproject.toml                   # Includes sapphire-api-client dependency
├── Dockerfile
└── requirements.txt

# Core logic lives in iEasyHydroForecast:
apps/iEasyHydroForecast/
├── forecast_library.py              # ~8100 lines, skill metrics, API writes
│   ├── _get_api_failure_mode()      # Bug 5: configurable API failure mode
│   └── _handle_api_write_error()    # Bug 5: centralized error handler (4 call sites)
├── setup_library.py                 # Configuration, data loading, API reads
├── tag_library.py                   # Date utilities (pentad, decad)
├── pyproject.toml                   # Includes sapphire-api-client dependency
└── tests/
    └── test_forecast_library.py     # 206 tests (includes 5 atomic write + 7 API failure mode)
```

### Current Execution Flow (Monolithic)

```
postprocessing_forecasts.py
├── read data (ALL historical + latest, from 2010)
├── calculate ALL skill metrics  ← SLOW (groupby.apply 3×)
├── calculate ensembles (EM, NE)
├── save ALL to CSV
├── save ALL to API
└── log recent forecasts
```

### Temporal Resolutions

| Resolution | Periods/Year | Status |
|------------|--------------|--------|
| Daily | 365 | Implemented |
| Pentadal (5-day) | 72 | Implemented |
| Decadal (10-day) | 36 | Implemented |
| Monthly | 12 | Implemented in `long_term_forecasting` |
| Quarterly | 4 | **Not yet implemented** |
| Seasonal (Apr–Sep) | 1 | **Not yet implemented** |

---

## Phase 1: Bug Fixes (Merge Main)

### Already Fixed on `main` (commit `a52597d`)

- [x] **Bug 1+2: Return value masking & uninitialized variable** — errors[] accumulation pattern, 9 tests
- [x] **Bug 3: Unsafe `.iloc[0]` access** — empty check before `.iloc[0]`, 5 tests
- [x] **Bug 4: Non-atomic file operations** — `atomic_write_csv()` helper with temp file + rename, 6 tests

### Bug 5: Silent API failures — DONE

**Implementation:** Two helper functions added to `forecast_library.py`:
- `_get_api_failure_mode()` — reads `SAPPHIRE_API_FAILURE_MODE` env var (default: `"warn"`)
- `_handle_api_write_error(e, description)` — centralized handler used at 4 API write sites

**Call sites updated:**
- `save_pentadal_skill_metrics()` (line ~6223)
- `save_decadal_skill_metrics()` (line ~6295)
- `save_forecast_data_pentad()` (line ~6434)
- `save_forecast_data_decade()` (line ~6509)

**Modes:**
- `"fail"` — re-raise exception, caller sees failure
- `"warn"` — log error, continue (default, preserves existing behavior)
- `"ignore"` — silent

**Tests (7 in `TestApiFailureMode` class):**
- `test_get_api_failure_mode_defaults_to_warn`
- `test_get_api_failure_mode_reads_env`
- `test_get_api_failure_mode_case_insensitive`
- `test_get_api_failure_mode_invalid_defaults_to_warn`
- `test_handle_api_write_error_fail_mode_reraises`
- `test_handle_api_write_error_warn_mode_logs`
- `test_handle_api_write_error_ignore_mode_silent`

### Configuration Bug Fix — DONE

- [x] ~~**Missing env variable:** `ieasyforecast_decadal_skill_metrics_file`~~ — added to `apps/config/.env`

---

## Phase 2: Module Separation

Both original plans agree: split the monolithic script into **daily (operational)** and **yearly (maintenance)** entry points.

### Target Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  OPERATIONAL (postprocessing_operational.py)                    │
│  ├── Read LATEST forecast data only (today's forecasts)        │
│  ├── Read EXISTING skill metrics from CSV/API                  │
│  ├── Create ensemble for today using pre-calculated skill      │
│  ├── Write forecasts + ensemble to API (batch upsert)          │
│  └── Log recent forecasts for monitoring                       │
│                                                                │
│  Schedule: After each forecast cycle (multiple times/day)      │
│  Target execution: < 30 seconds                                │
│  Priority: Time-critical for dashboard updates                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  MAINTENANCE (recalculate_skill_metrics.py)                     │
│  ├── Read FULL historical data (2010–present)                  │
│  ├── Calculate ALL skill metrics (vectorized, single-pass)     │
│  ├── Recalculate ALL ensemble compositions                     │
│  ├── Write skill metrics to API (batch upsert)                 │
│  ├── Write full CSV archives (atomic writes)                   │
│  └── Optional: run consistency checks (API vs CSV)             │
│                                                                │
│  Schedule: Once per year (November/December), or on demand     │
│  Target execution: Can take 5–10 minutes                       │
│  Priority: Data completeness, not speed                        │
└─────────────────────────────────────────────────────────────────┘
```

### File Structure After Separation

```
apps/postprocessing_forecasts/
├── postprocessing_operational.py      # NEW: Daily entry point
├── recalculate_skill_metrics.py       # NEW: Yearly maintenance entry point
├── postprocessing_forecasts.py        # DEPRECATED: Legacy entry point (keep as fallback)
├── src/
│   ├── api_writer.py                  # NEW: Shared API write logic (batch upsert)
│   ├── skill_metrics.py               # NEW: Vectorized skill metric calculations
│   ├── ensemble_calculator.py         # NEW: EM/NE ensemble logic
│   ├── data_reader.py                 # NEW: Data loading utilities
│   ├── file_writer.py                 # NEW: Atomic CSV writing (extracted from forecast_library)
│   └── postprocessing_tools.py        # Existing logging utilities
├── tests/
│   ├── unit/
│   │   ├── test_api_writer.py
│   │   ├── test_skill_metrics.py
│   │   ├── test_ensemble_calculator.py
│   │   ├── test_data_reader.py
│   │   └── test_file_writer.py
│   └── integration/
│       ├── test_operational_workflow.py
│       └── test_maintenance_workflow.py
└── Dockerfile                         # Update for dual entry points
```

### Key Design Decisions

1. **Maintenance runs yearly, not nightly.** Skill metrics only change when you have a full new year of forecast-observation pairs. Running more often is wasted computation.

2. **Daily script reads existing skill metrics, does NOT recalculate.** The ensemble composition for each pentad/decad is determined by the pre-calculated skill metrics.

3. **Shared modules minimize code duplication.** Both entry points import from `src/`.

4. **Legacy script preserved as fallback** with deprecation warning, removed after 1–2 release cycles.

### Docker & Pipeline Integration

**Current state:** The Dockerfile has a single `CMD` running `postprocessing_forecasts.py`. The Luigi pipeline (`apps/pipeline/pipeline_docker.py`) has a `PostProcessingForecasts` task that runs the `sapphire-postprocessing` Docker image.

**After separation:**

1. **Single Docker image, configurable entry point.** Keep one `sapphire-postprocessing` image. Use an environment variable or command override to select the entry point:
   ```dockerfile
   # Default to operational (daily) mode
   CMD ["python", "postprocessing_operational.py"]
   ```
   For maintenance: `docker run sapphire-postprocessing python recalculate_skill_metrics.py`

2. **Luigi pipeline changes:**
   - Update `PostProcessingForecasts` task to run `postprocessing_operational.py` (fast, daily)
   - Add a new `RecalculateSkillMetrics` task for yearly maintenance, or run manually via SSH:
     ```bash
     docker run sapphire-postprocessing python recalculate_skill_metrics.py
     ```

3. **PYTHONPATH:** The current Dockerfile sets `PYTHONPATH=/app/apps/iEasyHydroForecast`. The new `src/` modules should be self-contained where possible, importing from `iEasyHydroForecast` only for functions not yet extracted. This dependency shrinks as extraction progresses.

### Dependency: Skill Metrics → Ensemble

```
Skill metrics (pre-calculated, per pentad/decad/station/model)
        ↓
For each new forecast date:
  - Determine pentad_in_year (e.g., pentad 15)
  - Look up skill metrics for that specific pentad
  - Filter models passing thresholds for THAT pentad
        ↓
Calculate ensemble mean from qualifying models
        ↓
Save forecast (individual models + ensemble)
```

---

## Phase 3: Performance Improvements

### Server-Side: Batch Upsert (CRUD)

- [ ] **Replace N+1 queries with batch upsert**

**File:** `sapphire/services/postprocessing/app/crud.py`

**Current:** For 1000 records → 1000 SELECT + 1000 INSERT/UPDATE + 1000 commits.

**Fix:** PostgreSQL `ON CONFLICT DO UPDATE` in a single statement.

```python
from sqlalchemy.dialects.postgresql import insert

def upsert_forecasts(db: Session, records: list[dict]) -> int:
    stmt = insert(Forecast).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=['horizon_type', 'code', 'model_type', 'date', 'target'],
        set_={col: stmt.excluded[col] for col in update_cols}
    )
    result = db.execute(stmt)
    db.commit()
    return result.rowcount
```

**Database prerequisite:** The `ON CONFLICT` clause requires a unique constraint or unique index on `(horizon_type, code, model_type, date, target)`. Verify this exists before deploying; if not, add a migration.

**Safety note:** The current N+1 loop already overwrites existing records, so batch upsert doesn't introduce new risk — it just makes it faster. Safeguards: (1) validate records before sending (reject NaN discharge, impossible dates, empty codes); (2) the operational script only writes today's forecasts, limiting blast radius; (3) skill metrics can always be regenerated from observations.

### Client-Side: Vectorized Record Building

- [ ] **Replace `iterrows()` with vectorized pandas**

**File:** `forecast_library.py:4149-4190, 4267-4310`

**Current:** Loop with per-row regex and string operations.

**Fix:** Pre-filter with vectorized masks, vectorized model type mapping, `to_dict('records')` instead of row-by-row append.

### Skill Metrics: Single-Pass Calculation

- [ ] **Combine triple `groupby.apply()` into single operation**

**Files:** `forecast_library.py:1909-1957` (pentad), `2214-2264` (decad)

**Current:** Three separate groupby-apply operations (sdivsigma_nse, mae, forecast_accuracy), then two merges.

**Fix:** Single `groupby.apply()` with one function that calculates all metrics:

```python
def calculate_all_metrics(group, observed_col, simulated_col):
    # Calculate NSE, sdivsigma, MAE, accuracy in one pass
    return pd.Series({'sdivsigma': ..., 'nse': ..., 'mae': ..., 'accuracy': ..., 'n_pairs': ...})

skill_stats = df.groupby(group_cols).apply(calculate_all_metrics, ...)
```

### Other Performance Fixes

| Bottleneck | Location | Fix |
|-----------|----------|-----|
| Concat in loops (O(N²)) | `setup_library.py:1961-2009` | Collect in list, concat once |
| Nested loops + concat (O(N³)) | `setup_library.py:2490-2585` (virtual stations) | Vectorize with merge/pivot |
| Multiple `.isin()` filters | `forecast_library.py:1968-1972` | Replace with merge |
| Client reinstantiation | `forecast_library.py:4118, 4236` | Module-level singleton |
| Health check per function | 8 locations | Single check at startup |

### API Client Singleton

- [ ] **Reuse API client across functions**

```python
_api_client = None

def get_api_client() -> SapphirePostprocessingClient:
    global _api_client
    if _api_client is None:
        _api_client = SapphirePostprocessingClient(base_url=os.getenv("SAPPHIRE_API_URL"))
    return _api_client
```

---

## Phase 4: Quarterly & Seasonal Aggregation

> Future work. Not needed for immediate refactoring but planned.

### Quarterly Forecasts (Average runoff over next 3 months)

| Forecast Date | Quarter Covered |
|---------------|-----------------|
| Dec 25 | Jan–Feb–Mar (Q1) |
| Mar 25 | Apr–May–Jun (Q2) |
| Jun 25 | Jul–Aug–Sep (Q3) |
| Sep 25 | Oct–Nov–Dec (Q4) |

### Seasonal Forecasts (Average runoff April–September)

| Forecast Date | Period Covered |
|---------------|----------------|
| Jan 10 | Apr–Sep |
| Feb 10 | Apr–Sep |
| Mar 10 | Apr–Sep |
| Apr 10 | Apr–Sep |
| May 10 | Apr–Sep |

### Implementation Steps

1. **Date utilities** — Add to `tag_library.py`:
   - `get_quarter(date)`, `is_quarterly_forecast_date(date)`
   - `is_seasonal_forecast_date(date)`, `get_season_months()`

2. **Aggregation functions** — Add to `forecast_library.py`:
   - `aggregate_monthly_to_quarterly()`
   - `calculate_seasonal_discharge_avg()`

3. **Skill metrics** — Extend:
   - `calculate_skill_metrics_quarterly()`
   - `calculate_skill_metrics_seasonal()`

4. **Pipeline** — Add Luigi tasks:
   - `RunQuarterlyWorkflow`, `RunSeasonalWorkflow`

5. **Configuration** — Add env vars for quarterly/seasonal output file paths.

---

## Phase 5: Testing Strategy

### Existing Tests (79 postprocessing + 206 iEasyHydroForecast, all passing)

| File | Tests | Covers |
|------|-------|--------|
| `postprocessing_forecasts/tests/test_api_read.py` | 45 | API read: LR/ML/observed, pagination, CSV fallback, data consistency, edge cases |
| `postprocessing_forecasts/tests/test_api_integration.py` | 16 | API write: skill metrics, combined forecasts, field mapping, NaN handling |
| `postprocessing_forecasts/tests/test_error_accumulation.py` | 9 | Error accumulation, exit codes |
| `postprocessing_forecasts/tests/test_postprocessing_tools.py` | 8 | Safe `.iloc[0]`, NaT dates, missing codes |
| `postprocessing_forecasts/tests/test_mock_postprocessing_forecasts.py` | 1 | Combined forecast consistency |
| `iEasyHydroForecast/tests/test_forecast_library.py` | 206 total | Includes 5 atomic write + 7 API failure mode tests (Bug 5) |

### New Tests Needed

#### Unit Tests (target: 50+)

| Module | Test File | Tests | Coverage |
|--------|-----------|-------|----------|
| API writer | `test_api_writer.py` | 12 | Record building, vectorization, API error handling, singleton |
| Skill metrics | `test_skill_metrics.py` | 12 | NSE, MAE, sdivsigma, accuracy, single-pass, edge cases |
| Ensemble | `test_ensemble_calculator.py` | 8 | EM/NE calculation, composition tracking, threshold filtering |
| Data reader | `test_data_reader.py` | 5 | Latest-only reads, full historical, missing files |
| File writer | `test_file_writer.py` | 5 | Atomic writes, directory creation, column order |

#### Integration Tests (target: 12+)

| Test File | Tests | Coverage |
|-----------|-------|----------|
| `test_operational_workflow.py` | 6 | Pentad/decad/both modes, API failure, empty data, timing |
| `test_maintenance_workflow.py` | 6 | Full pentad/decad, CSV-API consistency, ensemble recalc |

#### Performance Benchmarks

| Benchmark | Target |
|-----------|--------|
| Operational workflow end-to-end | < 30 seconds |
| Batch upsert 1000 records | < 2 seconds |
| Single-pass skill metrics (10K groups) | < 5 seconds |
| Vectorized record building (22K rows) | < 1 second |

---

## Implementation Checklist

### Phase 1: Bug Fixes

- [x] ~~Bug 1+2: Return value masking~~ (on `main`, commit `a52597d`)
- [x] ~~Bug 3: Unsafe `.iloc[0]` access~~ (on `main`, commit `a52597d`)
- [x] ~~Bug 4: Non-atomic file operations~~ (on `main`, commit `a52597d`)
- [x] ~~**Merge `main` into `develop_long_term`**~~ (PR #290, commit `ab1e2ab`)
- [x] ~~Bug 5: Silent API failures~~ (`SAPPHIRE_API_FAILURE_MODE` env var, `_handle_api_write_error()` helper, 7 tests)
- [x] ~~Config fix: Add `ieasyforecast_decadal_skill_metrics_file` to `.env`~~

### Phase 2: Module Separation

- [ ] Create `src/skill_metrics.py` (extract from `forecast_library.py`)
- [ ] Create `src/ensemble_calculator.py` (extract from `forecast_library.py`)
- [ ] Create `src/data_reader.py` (extract from `setup_library.py`)
- [ ] Create `src/api_writer.py` (extract from `forecast_library.py`)
- [ ] Create `src/file_writer.py` (extract `atomic_write_csv` + CSV save logic)
- [ ] Create `postprocessing_operational.py` (daily entry point)
- [ ] Create `recalculate_skill_metrics.py` (yearly maintenance entry point)
- [ ] Update Dockerfile for dual entry points
- [ ] Add deprecation warning to legacy `postprocessing_forecasts.py`

### Phase 3: Performance Improvements

- [ ] Batch upsert in CRUD (server-side)
- [ ] Replace `iterrows()` with vectorized operations (client-side)
- [ ] Combine triple `groupby.apply()` into single-pass
- [ ] Fix concat-in-loop patterns (`setup_library.py`)
- [ ] Fix nested loops in virtual station calculation
- [ ] Replace multiple `.isin()` with merge
- [ ] Implement API client singleton

### Phase 4: Quarterly & Seasonal (Future)

- [ ] Date utilities in `tag_library.py`
- [ ] Aggregation functions
- [ ] Quarterly/seasonal skill metrics
- [ ] Pipeline tasks
- [ ] Configuration updates

### Phase 5: Testing

- [ ] Unit test structure (`tests/unit/`, `tests/integration/`)
- [ ] Unit tests for all new `src/` modules (50+ tests)
- [ ] Integration tests for both workflows (12+ tests)
- [ ] Performance benchmarks
- [ ] Test fixtures and `conftest.py`

---

## Files Affected

### Modified

| File | Changes |
|------|---------|
| `postprocessing_forecasts.py` | Add deprecation warning, then phase out |
| `postprocessing_tools.py` | Already fixed (Tier 1) |
| `forecast_library.py` | Extract skill metrics, ensemble, API writer, vectorize |
| `setup_library.py` | Extract data reader, fix concat-in-loop, fix virtual stations |
| `crud.py` (postprocessing service) | Batch upsert implementation |
| `tag_library.py` | Add quarterly/seasonal date utilities (Phase 4) |

### New Files

| File | Purpose |
|------|---------|
| `src/api_writer.py` | Shared API write logic |
| `src/skill_metrics.py` | Vectorized skill calculations |
| `src/ensemble_calculator.py` | EM/NE ensemble logic |
| `src/data_reader.py` | Data loading utilities |
| `src/file_writer.py` | Atomic CSV writing |
| `postprocessing_operational.py` | Daily operational entry point |
| `recalculate_skill_metrics.py` | Yearly maintenance entry point |
| `tests/unit/*.py` | Unit tests |
| `tests/integration/*.py` | Integration tests |
| `tests/conftest.py` | Test fixtures |

---

## Migration Strategy

### Step 1: Merge Main (No New Code) — DONE

1. ~~Merge `main` into `develop_long_term`~~ — PR #290, commit `ab1e2ab`
2. ~~Verify Tier 1 bug fixes and tests pass~~ — 79 postprocessing + 206 iEasyHydroForecast tests pass
3. ~~Resolve merge conflicts~~ — Done

### Step 2: Bug 5 Fix (No Breaking Changes) — DONE

1. ~~Add `SAPPHIRE_API_FAILURE_MODE` env var support~~ — `_get_api_failure_mode()` + `_handle_api_write_error()`
2. ~~Default to `"warn"` (preserves current behavior)~~ — Yes
3. ~~Test with all three modes~~ — 7 tests in `TestApiFailureMode`

### Step 3: Module Separation (Gradual Rollout)

1. Create shared `src/` modules (extract, don't rewrite)
2. Create `postprocessing_operational.py` alongside existing code
3. Create `recalculate_skill_metrics.py` alongside existing code
4. Run new operational script in parallel with legacy in staging
5. Compare outputs
6. Switch over when confident

### Step 4: Performance (Independent)

1. Deploy batch upsert (server-side, no client changes needed)
2. Deploy client-side vectorization
3. Deploy skill metrics optimization (maintenance script only)
4. Benchmark before/after

### Step 5: Deprecation

1. Add deprecation warning to legacy `postprocessing_forecasts.py`
2. Update all documentation
3. Remove legacy script after 1–2 release cycles

### Rollback Strategy

Each phase can be rolled back independently:

- **Phase 1 (Bug 5):** Revert to `"warn"` mode (default) — no data impact.
- **Phase 2 (Module separation):** Legacy `postprocessing_forecasts.py` is preserved. To rollback: repoint the Luigi task / Docker CMD back to the legacy script. No schema changes involved.
- **Phase 3 (Batch upsert):** The new CRUD endpoint is additive. To rollback: revert the API server code; the client-side changes (vectorization) are independent and harmless.
- **Phase 4 (Quarterly/seasonal):** Entirely additive — new tables, new functions. Rollback = stop running the new tasks.

For database-level issues: PostgreSQL WAL-based point-in-time recovery can restore to any moment before a bad write.

---

## Appendix: Skill Metrics & Ensemble Details

### Skill Metrics Structure

Skill metrics are calculated **per model, per station, per pentad/decad of the year** — NOT a single value per model.

**Grouping keys:**
- Pentadal: `['pentad_in_year', 'code', 'model_long', 'model_short']`
- Decadal: `['decad_in_year', 'code', 'model_long', 'model_short']`

**Example:** 72 pentads × 50 stations × 4 models = **14,400 skill metric records**

| Metric | Function | Description | Threshold for ensemble |
|--------|----------|-------------|----------------------|
| sdivsigma | `sdivsigma_nse()` | RMSE / StdDev of observations | < 0.6 (lower is better) |
| NSE | `sdivsigma_nse()` | Nash-Sutcliffe Efficiency | > 0.8 (higher is better) |
| MAE | `mae()` | Mean Absolute Error | (no threshold) |
| accuracy | `forecast_accuracy_hydromet()` | Fraction within ±delta | > 0.8 (higher is better) |
| n_pairs | — | Number of forecast-observation pairs | — |

### Why Different Skills per Pentad/Decad?

Models perform differently depending on the time of year:
- **Snowmelt periods:** Some models capture spring dynamics better
- **Low-flow periods:** Different models may excel during baseflow
- **Monsoon/wet seasons:** Model performance varies with precipitation patterns

**Example:**
```
Station 15102, Model TFT:
  Pentad 15 (mid-March): sdivsigma=0.45, accuracy=0.85 → INCLUDED in ensemble
  Pentad 45 (early August): sdivsigma=0.72, accuracy=0.65 → EXCLUDED from ensemble
```

### Ensemble Creation Logic

1. For each (pentad_in_year, code), select models where ALL thresholds pass for **that specific pentad**
2. Exclude Neural Ensemble (NE) from constituent models
3. Calculate ensemble mean = arithmetic mean of qualifying models' `forecasted_discharge`
4. Record which models composed the ensemble in `composition` field

**Threshold env vars:**
```
ieasyhydroforecast_efficiency_threshold=0.6   # sdivsigma
ieasyhydroforecast_accuracy_threshold=0.8     # accuracy
ieasyhydroforecast_nse_threshold=0.8          # NSE
```

### Yearly Maintenance Workflow

```
November/December:
  1. Run recalculate_skill_metrics.py
  2. New skill_metrics_pentad.csv and skill_metrics_decad.csv generated
  3. Write updated skill metrics to API
  4. These are used for ensemble selection throughout the next year

Daily:
  1. Generate new forecasts (upstream modules)
  2. Read existing skill metrics (from CSV or API)
  3. Use pentad-specific skill to select models for ensemble
  4. Save forecast + ensemble to API and CSV
```

---

## Appendix: Current Code Reference

### Existing Date Utilities (`tag_library.py`)

| Function | Purpose |
|----------|---------|
| `get_pentad(date)` | Returns pentad of month (1-6) |
| `get_pentad_in_year(date)` | Returns pentad of year (1-72) |
| `get_decad_in_month(date)` | Returns decad of month (1-3) |
| `get_date_for_pentad(pentad, year)` | Converts pentad number to date |
| `get_date_for_decad(decad, year)` | Converts decad number to date |

### Existing Aggregation Functions

| Function | Location | Purpose |
|----------|----------|---------|
| `calculate_pentadaldischargeavg()` | `forecast_library.py` | Daily to 5-day average |
| `calculate_decadaldischargeavg()` | `forecast_library.py` | Daily to 10-day average |
| `aggregate_decadal_to_monthly()` | `preprocessing_station_forcing/src.py` | Decadal to monthly |

### Data Models

```python
class ForecastFlags:
    pentad: bool    # 5-day forecast
    decad: bool     # 10-day forecast
    month: bool     # Monthly forecast (exists but not fully used)
    season: bool    # Seasonal forecast (exists but not fully used)

class PredictorDates:
    pentad: list    # Pentad predictor dates
    decad: list     # Decad predictor dates
    month: list     # Monthly predictor dates (exists)
    season: list    # Seasonal predictor dates (exists)
```

### Output Files

| File | Purpose |
|------|---------|
| `combined_forecasts_pentad.csv` | Combined pentadal forecasts |
| `combined_forecasts_decad.csv` | Combined decadal forecasts |
| `skill_metrics_pentad.csv` | Pentadal skill metrics |
| `skill_metrics_decad.csv` | Decadal skill metrics |

### Pentadal/Decadal Aggregation Trigger Dates

| Aggregation | Trigger Date |
|-------------|--------------|
| Pentadal | Days 5, 10, 15, 20, 25, last day of month |
| Decadal | Days 10, 20, last day of month |

### Monthly Update Schedule (for Quarterly/Seasonal)

| Date | Task |
|------|------|
| 25th of each month | Initial quarterly/seasonal forecast |
| 10th of each month | Update forecast for same period |

### Existing Timing Infrastructure

`postprocessing_forecasts.py` includes a `TimingStats` class that tracks per-section execution time. Sections tracked: `'reading pentadal data'`, `'calculating skill metrics pentads'`, `'saving pentad results'`, and equivalents for decadal.

---

## Related Documents

| Document | Status |
|----------|--------|
| `doc/plans/sapphire_api_integration_plan.md` | COMPLETE (Phase 6 pending: remove CSV fallback) |
| `doc/plans/postprocessing_api_integration_test_plan.md` | COMPLETE (all 7 tests passed) |
| `doc/plans/issues/gi_duplicate_skill_metrics_ensemble_composition.md` | RESOLVED |
| `doc/plans/issues/gi_draft_prepg_yearly_norm_recalculation.md` | NOT STARTED |
| `doc/plans/bulk_read_endpoints_instructions.md` | READY for implementation |

## Superseded Documents

The following plans are **superseded** by this unified plan (moved to `archive/`):
- `doc/plans/archive/postprocessing_forecasts_improvement_plan.md` — detailed code examples for Bugs 1–4, vectorization, full module implementations
- `doc/plans/archive/postprocessing_refactoring_plan.md` — Option A vs B analysis, detailed performance bottleneck code

---

## Revision History

| Date | Author | Changes |
|------|--------|---------|
| 2026-01-24 | Claude | Original improvement plan created |
| 2026-01-27 | Claude | Original refactoring plan created |
| 2026-02-06 | Claude | Unified plan: integrated both plans, marked Tier 1 bugs as done, aligned module separation approach |
| 2026-02-06 | Claude | Review fixes: corrected test file names/counts, added Docker/pipeline integration, DB prerequisites, rollback strategy, code reference appendix |
| 2026-02-12 | Claude | Phase 1 complete: updated all status fields, Bug 5 done (7 tests), config fix done, API read tests (45) + write tests (16) documented, test counts updated (79 postprocessing + 206 iEasyHydroForecast), migration steps 1–2 marked done, `sapphire-api-client` dependency added to pyproject.toml files |
