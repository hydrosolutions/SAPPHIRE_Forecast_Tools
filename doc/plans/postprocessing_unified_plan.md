# Postprocessing Forecasts — Unified Refactoring Plan

> Integrates `postprocessing_forecasts_improvement_plan.md` and
> `postprocessing_refactoring_plan.md` into a single actionable document.

---

## Table of Contents

1. [Status Summary](#status-summary)
2. [Current Architecture](#current-architecture)
3. [Phase 1: Bug Fixes](#phase-1-bug-fixes-merge-main)
4. [Phase 2: Module Separation (Operational / Nightly / Yearly)](#phase-2-module-separation)
5. [Phase 3: Performance Improvements](#phase-3-performance-improvements)
6. [Phase 4: Monthly, Quarterly & Seasonal Skill Metrics](#phase-4-monthly-quarterly--seasonal-skill-metrics)
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
| Module separation (operational / nightly gap-fill / yearly recalc) | **DONE** — 3 entry points + 4 src modules + 2 shell scripts, 180 tests (commit `9ce63c8`, `eae7158`) |
| Server-side batch upsert (CRUD) | **DONE** — `_bulk_upsert()` with PG ON CONFLICT + N+1 fallback (commit `eae7158`) |
| Client-side vectorization | **DONE** — vectorized record building in 4 `_write_*_to_api()` functions (commit `eae7158`) |
| Skill metrics single-pass optimization | **DONE** — `calculate_all_skill_metrics()` replaces triple groupby+merge (commit `eae7158`) |
| Monthly/quarterly/seasonal skill metrics | TODO — Phase 4 (point + CRPS, configurable season) |
| Bug 6: Single-model ensemble filter only rejects LR | **DONE** — `_is_multi_model_ensemble()` helper replaces hardcoded check |
| Comprehensive test suite (50+ unit, 12+ integration) | **DONE** — 270 postprocessing tests + 206 iEasyHydroForecast tests pass |
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

## Phase 1: Bug Fixes (Merge Main) - DONE 

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

### Bug 6: Single-model ensemble filter only rejects LR — DONE

**File:** `src/ensemble_calculator.py`

**Problem (lines 198-202):** The single-model ensemble filter was hardcoded to reject only `'Ens. Mean with LR (EM)'`. Single-TFT, single-TiDE, or single-TSMixer ensembles could slip through and create meaningless "ensemble" rows containing only one model's forecast.

**Fix:** Added `_is_multi_model_ensemble()` helper near line 48 that uses regex to extract the model list from `'Ens. Mean with X, Y (EM)'` and checks for a comma (indicating 2+ models). Replaced the hardcoded string comparison with `.apply(_is_multi_model_ensemble)`.

**Tests (10 new):**
- `TestHelpers`: `test_is_multi_model_two_models`, `test_is_multi_model_three_models`, `test_is_multi_model_single`, `test_is_multi_model_empty`
- `TestCreateEnsembleForecasts`: `test_single_tft_ensemble_discarded`, `test_single_tide_ensemble_discarded`
- `TestSingleModelEnsembleBug` (integration): `test_single_tft_rejected`, `test_single_tide_rejected`, `test_two_ml_models_accepted`
- `TestModelNameConsistency`: `test_model_short_to_long_covers_core_types`, `test_api_model_type_mapping_consistent`

### Configuration Bug Fix — DONE

- [x] ~~**Missing env variable:** `ieasyforecast_decadal_skill_metrics_file`~~ — added to `apps/config/.env`

---

## Phase 2: Module Separation

Split the monolithic script into three entry points: **operational (daily)**, **nightly gap-fill (maintenance)**, and **yearly skill recalculation**.

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
│  MAINTENANCE — NIGHTLY GAP-FILL (postprocessing_maintenance.py)│
│  ├── Scan recent window (e.g. last 7 days) for missing         │
│  │   ensemble forecasts (data arrived late)                    │
│  ├── Read EXISTING skill metrics (same as operational)         │
│  ├── For each gap: calculate ensemble from now-available data  │
│  ├── Write filled forecasts + ensembles to API (batch upsert)  │
│  └── Log what was filled for audit trail                       │
│                                                                │
│  Schedule: Nightly (e.g. 02:00), after all data feeds close   │
│  Target execution: < 2 minutes                                 │
│  Priority: Data completeness for recent dates                  │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  MAINTENANCE — YEARLY RECALC (recalculate_skill_metrics.py)    │
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
├── postprocessing_maintenance.py      # NEW: Nightly gap-fill entry point
├── recalculate_skill_metrics.py       # NEW: Yearly skill recalculation entry point
├── postprocessing_forecasts.py        # DEPRECATED: Legacy entry point (keep as fallback)
├── src/
│   ├── api_writer.py                  # NEW: Shared API write logic (batch upsert)
│   ├── skill_metrics.py               # NEW: Vectorized skill metric calculations
│   ├── ensemble_calculator.py         # NEW: EM/NE ensemble logic
│   ├── data_reader.py                 # NEW: Data loading utilities
│   ├── gap_detector.py                # NEW: Detect missing ensembles in recent window
│   ├── file_writer.py                 # NEW: Atomic CSV writing (extracted from forecast_library)
│   └── postprocessing_tools.py        # Existing logging utilities
├── tests/
│   ├── unit/
│   │   ├── test_api_writer.py
│   │   ├── test_skill_metrics.py
│   │   ├── test_ensemble_calculator.py
│   │   ├── test_data_reader.py
│   │   ├── test_gap_detector.py
│   │   └── test_file_writer.py
│   └── integration/
│       ├── test_operational_workflow.py
│       ├── test_maintenance_workflow.py
│       └── test_yearly_recalc_workflow.py
└── Dockerfile                         # Update for triple entry points
```

### Key Design Decisions

1. **Three workflows, not two.** Maintenance is split into nightly gap-fill and yearly recalculation. They serve different purposes and run at very different frequencies.

2. **Nightly gap-fill catches late-arriving data.** External data feeds sometimes deliver observations a day late. The nightly script scans a recent window (configurable, default 7 days) for dates where forecast data exists but ensembles are missing, then fills them. It does NOT recalculate skill metrics — it uses the same pre-calculated metrics as the operational script.

3. **Yearly recalculation updates skill metrics once per year.** Skill metrics only meaningfully change when you accumulate a full new year of forecast-observation pairs. Running in November/December ensures the new metrics are ready before the next water year's forecast season.

4. **Daily script reads existing skill metrics, does NOT recalculate.** The ensemble composition for each pentad/decad is determined by the pre-calculated skill metrics.

5. **Shared modules minimize code duplication.** All three entry points import from `src/`. The gap-fill script reuses the same ensemble calculation and API write logic as the operational script.

6. **Legacy script preserved as fallback** with deprecation warning, removed after 1–2 release cycles.

### Docker & Pipeline Integration

**Current state:** The Dockerfile has a single `CMD` running `postprocessing_forecasts.py`. The Luigi pipeline (`apps/pipeline/pipeline_docker.py`) has a `PostProcessingForecasts` task that runs the `sapphire-postprocessing` Docker image.

**After separation:**

1. **Single Docker image, configurable entry point.** Keep one `sapphire-postprocessing` image. Use an environment variable or command override to select the entry point:
   ```dockerfile
   # Default to operational (daily) mode
   CMD ["python", "postprocessing_operational.py"]
   ```
   For nightly gap-fill: `docker run sapphire-postprocessing python postprocessing_maintenance.py`
   For yearly recalc: `docker run sapphire-postprocessing python recalculate_skill_metrics.py`

2. **Luigi pipeline:** Only the operational entry point runs through the Luigi pipeline. Update the existing `PostProcessingForecasts` task to run `postprocessing_operational.py`. No new Luigi tasks needed for maintenance or yearly recalculation.

3. **Maintenance shell scripts** (in `bin/`, following the pattern of `daily_preprunoff_maintenance.sh`):

   - **`bin/daily_postprc_maintenance.sh`** — nightly gap-fill. Structure:
     - Source `bin/utils/common_functions.sh` (banner, config, SSH tunnel, cleanup)
     - Read configuration from `.env` file passed as argument
     - Validate required env vars
     - Create timestamped log directory under `${ieasyhydroforecast_data_root_dir}/logs/postprc_maintenance/`
     - Verify Docker is running, pull image if needed
     - Establish SSH tunnel (if required for DB access)
     - Run `sapphire-postprocessing` container with `SAPPHIRE_SYNC_MODE=maintenance` and appropriate volume mounts
     - Capture exit code, log result, clean up container
     - Prune logs older than 15 days
     - Scheduled via cron (e.g. `0 2 * * *`)

   - **`bin/yearly_skill_metrics_recalculation.sh`** — yearly full recalculation. Same structure as above, but:
     - Log directory: `${ieasyhydroforecast_data_root_dir}/logs/skill_metrics_recalc/`
     - Container name: `postprc-skill-recalc`
     - Runs `python recalculate_skill_metrics.py` as the Docker command override
     - No `SAPPHIRE_SYNC_MODE` needed (the script itself is the full recalculation)
     - Run manually or scheduled once in November/December:
       ```bash
       bash bin/yearly_skill_metrics_recalculation.sh /path/to/config/.env
       ```

4. **PYTHONPATH:** The current Dockerfile sets `PYTHONPATH=/app/apps/iEasyHydroForecast`. The new `src/` modules should be self-contained where possible, importing from `iEasyHydroForecast` only for functions not yet extracted. This dependency shrinks as extraction progresses.

5. **Gap-fill configuration:** A `config.yaml` in `apps/postprocessing_forecasts/` controls the lookback window, following the same pattern as `preprocessing_runoff/config.yaml`:
   ```yaml
   maintenance:
     # Number of days to look back for missing ensembles
     # Override with: POSTPROCESSING_GAPFILL_WINDOW_DAYS
     lookback_days: 7
   ```
   The nightly shell script passes the env var override to the container if set; otherwise the Python code reads `config.yaml` directly.

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

## Phase 4: Monthly, Quarterly & Seasonal Skill Metrics

### Scope

Extend `postprocessing_forecasts` to calculate skill metrics for all temporal resolutions produced by the forecast system:

| Resolution | Forecasts produced by | Point metrics | CRPS |
|------------|----------------------|---------------|------|
| Pentadal (5-day) | `linear_regression`, `machine_learning` | **Done** | Blocked — quantile columns not yet populated |
| Decadal (10-day) | `linear_regression`, `machine_learning` | **Done** | Blocked — quantile columns not yet populated |
| Monthly | `long_term_forecasting` | **TODO** | **TODO** — quantiles available |
| Quarterly | Aggregated from monthly, or direct from `long_term_forecasting` | **TODO** | **TODO** — quantiles available |
| Seasonal | Aggregated from monthly, or direct from `long_term_forecasting` | **TODO** | **TODO** — quantiles available |

### Key Design Decisions

1. **Skill metrics calculated in `postprocessing_forecasts`**, consistent with pentad/decad. The module reads long-term forecasts from the postprocessing API and daily observations from the preprocessing API.

2. **CRPS as a cross-cutting metric for all resolutions where quantile information is available.** CRPS (Continuous Ranked Probability Score) is not limited to long-term forecasts — it applies wherever we have a quantile distribution to evaluate against observations:
   - **Monthly/quarterly/seasonal:** Quantiles (Q5–Q95) are produced by `long_term_forecasting`. CRPS is always calculated.
   - **Pentad/decad:** The `Forecast` table already has quantile columns (q05, q25, q50, q75, q95) but they are **not yet populated** by `linear_regression` / `machine_learning`. Once these modules produce prediction intervals or ensemble quantiles, CRPS can be calculated for short-term forecasts too. Until then, pentad/decad use traditional metrics only.
   - **Traditional (point-based):** Always calculated for all resolutions. For long-term forecasts, Q50 (median) is used as the point forecast for NSE, MAE, sdivsigma, accuracy.

3. **Aggregation-first for quarterly/seasonal, with direct forecast support.** Start by averaging monthly forecast quantiles to produce quarterly (3-month) and seasonal values. However, `long_term_forecasting` may already produce forecasts with `horizon_type='season'` directly (target horizon of 1 season). If such records exist in the `long_forecasts` table, `postprocessing_forecasts` uses them directly instead of aggregating from monthly. The same applies to quarterly forecasts.

   > **Note:** The `long_term_forecasting` module is still under active development. The exact output structure (which horizon types are produced, whether seasonal records exist directly) needs to be verified by inspecting the module's CSV output once it is complete. **Refine this integration plan once `long_term_forecasting` is finalized.**

4. **Configurable season definition.** Season start/end months are defined in `config.yaml` (not hardcoded), supporting different deployments (Central Asia Apr–Sep, Nepal Jun–Sep, Switzerland Apr–Oct, etc.).

5. **Monthly observations aggregated on-the-fly.** Daily discharge from the preprocessing API (`runoffs` table) is grouped by year/month. A month requires ≥50% non-missing days to be valid (same rule as `long_term_forecasting/post_process_lt_forecast.py:calculate_lt_statistics_calendar_month()`).

### Infrastructure Already in Place

| Component | Status | Location |
|-----------|--------|----------|
| `HorizonType` enum (MONTH, QUARTER, SEASON) | Exists | `sapphire/services/postprocessing/app/models.py` |
| `SkillMetric` table supports all horizon types | Exists | Same file |
| `LongForecast` table with quantile columns (Q5–Q95) | Exists | Same file |
| `Forecast` table with quantile columns (q05–q95) | Schema exists, **columns not yet populated** | Same file |
| `ForecastFlags.month`, `.season` | Exists | `setup_library.py:3773` |
| `PredictorDates.month`, `.season` | Exists | `forecast_library.py:8373` |
| Monthly aggregation logic (daily → monthly mean) | Exists | `long_term_forecasting/post_process_lt_forecast.py:168` |

**Gaps to fill:**
- `ForecastFlags` and `PredictorDates` lack a `quarter` field
- `tag_library.py` has no quarterly/seasonal date utilities
- No skill metric functions for monthly/quarterly/seasonal
- No CRPS implementation
- Pentad/decad quantile columns not populated (CRPS for short-term blocked on this)
- `long_term_forecasting` output needs inspection for direct seasonal/quarterly records

### Quarterly Forecasts (Average runoff over next 3 months)

| Forecast Date | Quarter Covered |
|---------------|-----------------|
| Dec 25 | Jan–Feb–Mar (Q1) |
| Mar 25 | Apr–May–Jun (Q2) |
| Jun 25 | Jul–Aug–Sep (Q3) |
| Sep 25 | Oct–Nov–Dec (Q4) |

### Seasonal Forecasts (Configurable period, default April–September)

| Forecast Date | Period Covered |
|---------------|----------------|
| Jan 10 | Apr–Sep (default) |
| Feb 10 | Apr–Sep |
| Mar 10 | Apr–Sep |
| Apr 10 | Apr–Sep |
| May 10 | Apr–Sep |

Season months configured via `config.yaml`:
```yaml
seasonal:
  # Configurable per deployment region
  start_month: 4   # April (Central Asia default)
  end_month: 9     # September
  # Forecast issue dates: 10th of each month from Jan to start_month+1
```

### Data Flow

```
┌──────────────────────────────────────────────────────────────────┐
│  INPUTS                                                          │
│                                                                  │
│  long_forecasts table ──→ Monthly forecasts (Q5–Q95 quantiles)  │
│    (via postprocessing API)   per station, per model, per month  │
│                                                                  │
│  runoffs table ──→ Daily observed discharge                      │
│    (via preprocessing API)   aggregated to monthly means         │
│                              (≥50% non-missing days required)    │
└──────────────────────┬───────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│  MONTHLY SKILL METRICS                                           │
│                                                                  │
│  Group by: [month_in_year, code, model_type]                    │
│                                                                  │
│  Point metrics (Q50 vs observed):                                │
│    NSE, sdivsigma, MAE, accuracy                                │
│                                                                  │
│  Probabilistic metrics (full quantile distribution vs observed): │
│    CRPS                                                          │
│                                                                  │
│  → Write to skill_metrics table (horizon_type='month')          │
└──────────────────────┬───────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│  QUARTERLY / SEASONAL                                            │
│                                                                  │
│  Step 1: Check long_forecasts table for direct records with      │
│    horizon_type='quarter' or 'season'. If present, use those.    │
│                                                                  │
│  Step 2 (fallback): If no direct records, aggregate from monthly:│
│    Quarterly: average 3 monthly forecast quantiles + 3 monthly   │
│      observed means → skill metrics per quarter                  │
│    Seasonal: average N monthly forecast quantiles (configurable  │
│      months) + N monthly observed means → skill metrics          │
│                                                                  │
│  Both paths → same skill metrics (point + CRPS)                  │
│  → Write to skill_metrics table (horizon_type='quarter'/'season')│
└──────────────────────────────────────────────────────────────────┘

  ⚠ long_term_forecasting integration note:
  The long_term_forecasting module is under active development.
  Whether it produces direct seasonal/quarterly records needs to be
  verified once the module is complete. Refine this integration
  plan at that point.
```

### Integration with Phase 2 Entry Points

- **Operational (daily):** No change. Monthly/quarterly/seasonal skill metrics are NOT recalculated daily — they are pre-calculated and read from the API, same as pentad/decad.
- **Nightly gap-fill:** Extended to check for missing monthly ensemble calculations when monthly forecast data arrives late.
- **Yearly recalculation (`recalculate_skill_metrics.py`):** Extended to recalculate monthly, quarterly, and seasonal skill metrics alongside pentad/decad.

### Implementation Steps

1. **Date utilities** — Add to `tag_library.py`:
   - `get_quarter(date)` → 1–4
   - `get_quarter_months(quarter)` → (start_month, end_month)
   - `is_quarterly_forecast_date(date)` → True on 25th of Dec/Mar/Jun/Sep
   - `is_seasonal_forecast_date(date, config)` → True on 10th of relevant months
   - `get_season_months(config)` → (start_month, end_month) from config

2. **Monthly observation aggregation** — Add to `src/data_reader.py`:
   - `read_monthly_observations(codes, start_year, end_year)` — reads daily discharge from preprocessing API, aggregates to monthly means with ≥50% coverage filter
   - Reuse logic from `long_term_forecasting/post_process_lt_forecast.py:calculate_lt_statistics_calendar_month()`

3. **CRPS implementation** — Add to `src/skill_metrics.py`:
   - `calculate_crps(quantiles, quantile_levels, observed)` — CRPS from quantile forecast
   - Quantile levels: [0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95]
   - Cross-cutting: used for monthly/quarterly/seasonal now; will also apply to pentad/decad once those modules populate quantile columns in the `Forecast` table

4. **Monthly skill metrics** — Add to `src/skill_metrics.py`:
   - `calculate_monthly_skill_metrics(forecasts_df, observations_df)` — both point (Q50) and probabilistic (CRPS) metrics
   - Grouping: `[month_in_year, code, model_type]` (12 months × N stations × M models)

5. **Quarterly/seasonal aggregation** — Add to `src/skill_metrics.py`:
   - `aggregate_monthly_to_quarterly(monthly_forecasts, monthly_obs)` — average quantiles over 3 months
   - `aggregate_monthly_to_seasonal(monthly_forecasts, monthly_obs, config)` — average over configurable season months
   - `calculate_quarterly_skill_metrics(...)`, `calculate_seasonal_skill_metrics(...)`

6. **Update data classes:**
   - Add `quarter` field to `ForecastFlags` (`setup_library.py`)
   - Add `quarter` field to `PredictorDates` (`forecast_library.py`)

7. **Configuration** — Add to `postprocessing_forecasts/config.yaml`:
   ```yaml
   seasonal:
     start_month: 4
     end_month: 9
   ```
   Add env vars for output file paths:
   ```
   ieasyforecast_monthly_skill_metrics_file
   ieasyforecast_quarterly_skill_metrics_file
   ieasyforecast_seasonal_skill_metrics_file
   ```

8. **Extend yearly recalculation** — Update `recalculate_skill_metrics.py` to call monthly/quarterly/seasonal skill metric functions after pentad/decad.

---

## Phase 5: Testing Strategy

### Current Tests (270 postprocessing + 206 iEasyHydroForecast, all passing)

| File | Tests | Covers |
|------|-------|--------|
| `postprocessing_forecasts/tests/test_api_read.py` | 45 | API read: LR/ML/observed, pagination, CSV fallback, data consistency, edge cases |
| `postprocessing_forecasts/tests/test_api_integration.py` | 16 | API write: skill metrics, combined forecasts, field mapping, NaN handling |
| `postprocessing_forecasts/tests/test_error_accumulation.py` | 9 | Error accumulation, exit codes (legacy entry point) |
| `postprocessing_forecasts/tests/test_postprocessing_tools.py` | 8 | Safe `.iloc[0]`, NaT dates, missing codes |
| `postprocessing_forecasts/tests/test_mock_postprocessing_forecasts.py` | 1 | Combined forecast consistency (legacy entry point) |
| `postprocessing_forecasts/tests/test_ensemble_calculator.py` | 25 | Helper functions, threshold filtering, ensemble creation, NE exclusion, single-model discard (LR/TFT/TiDE), composition string, decad, `_is_multi_model_ensemble` helper, model name consistency |
| `postprocessing_forecasts/tests/test_data_reader.py` | 8 | CSV read, API fallback, model mapping, empty/corrupt files |
| `postprocessing_forecasts/tests/test_gap_detector.py` | 6 | Missing EM detection, lookback window, multi-code gaps, date conversion |
| `postprocessing_forecasts/tests/test_operational_workflow.py` | 6 | Pentad/decad/both modes, error accumulation, empty skill metrics, invalid mode |
| `postprocessing_forecasts/tests/test_maintenance_workflow.py` | 8 | Gap detection, no-gap idempotency, lookback window, empty combined forecasts, invalid mode, BOTH/DECAD modes, save error |
| `postprocessing_forecasts/tests/test_recalc_workflow.py` | 6 | Calls calculate_skill_metrics, saves skill metrics, both mode, error accumulation, invalid mode, DECAD-only mode |
| `postprocessing_forecasts/tests/test_integration_postprocessing.py` | 42 | Data routing (operational/maintenance/API fallback/failure modes), single-model ensemble bug, edge case inputs, year/month boundaries, quantile fields, recalc entry point, decadal operational pipeline, maintenance full gap-fill pipeline |
| `postprocessing_forecasts/tests/test_edge_cases.py` | 46 | Empty/single-row, NaN handling, discharge boundaries (zero/negative/large), date boundaries, duplicates, thresholds, period coercion, code normalization, delta edge cases (NaN/zero/negative/varying), NaT dates in gap detector, missing columns |
| `postprocessing_forecasts/tests/test_calculate_all_skill_metrics.py` | 18 | Unit tests for `calculate_all_skill_metrics()`: happy path (hand-calculated), single point, all-NaN, missing column, constant observations, inf values, return type |
| `postprocessing_forecasts/tests/test_performance.py` | 6 | Benchmarks: triple-groupby vs single-pass, isin vs merge, iterrows vs vectorized |
| `postprocessing_forecasts/tests/test_constants.py` | — | Shared constants (model names, thresholds, delta) |
| `iEasyHydroForecast/tests/test_forecast_library.py` | 206 total | Includes ~15 sdivsigma_nse, ~15 MAE, ~8 accuracy, 5 atomic write, 7 API failure mode tests |

### Integration Test Data Flow Coverage Audit

The integration tests in `test_integration_postprocessing.py` must cover every step of the three entry points. This section maps the full data flow for each entry point and identifies what is covered vs missing.

#### Operational Entry Point (`postprocessing_operational.py`)

```
READ skill_metrics CSV/API → FILTER by thresholds → CREATE ensemble mean
→ WRITE combined forecasts CSV + API
```

| Step | Covered? | Test class/method |
|------|----------|-------------------|
| Read skill metrics from CSV | Yes | `TestOperationalDataRouting.test_skill_metrics_read_from_csv` |
| Read skill metrics from API (fallback) | Yes | `TestSkillMetricsFallback.test_api_fallback_when_csv_missing` |
| Threshold filtering (models pass/fail) | Yes | `TestOperationalDataRouting.test_two_stations_independent_filtering`, `test_no_ensemble_when_all_models_fail_threshold`, `test_threshold_boundary_values_excluded` |
| Ensemble mean calculation | Yes | `TestOperationalDataRouting.test_ensemble_created_and_written_to_csv`, `test_three_models_all_pass_ensemble` |
| Single-model ensemble rejection | Yes | `TestSingleModelEnsembleBug.test_single_tft_rejected`, `test_single_tide_rejected` |
| NaN discharge in averaging | Yes | `TestOperationalDataRouting.test_nan_discharge_dropped_before_averaging` |
| Composition string generation | Yes | `TestOperationalDataRouting.test_composition_survives_csv_roundtrip`, `test_composition_in_api_records` |
| CSV write (combined forecasts) | Yes | `TestOperationalDataRouting.test_ensemble_created_and_written_to_csv` |
| API write (combined forecasts) | Yes | `TestOperationalDataRouting.test_api_receives_correct_forecast_records` |
| CSV write when API disabled | Yes | `TestOperationalDataRouting.test_csv_still_written_when_api_disabled` |
| API failure modes (warn/fail/ignore) | Yes | `TestApiFailureModes` (3 tests) |
| Empty inputs (forecasts/skill/observed) | Yes | `TestEdgeCaseInputs` (3 tests) |
| Decadal mode (entire operational path) | Yes | `TestDecadalOperationalPipeline.test_decadal_ensemble_created_and_written_to_csv`, `test_decadal_api_records_correct` |
| **Ensemble skill metric recalculation** | **Partial** | EM rows exist but metric *values* (NSE/MAE/accuracy) are not numerically verified |

#### Maintenance Entry Point (`postprocessing_maintenance.py`)

```
READ combined_forecasts CSV → DETECT gaps → READ data for gap dates
→ CREATE ensemble for gaps → WRITE gap-filled forecasts CSV + API
```

| Step | Covered? | Test class/method |
|------|----------|-------------------|
| Read combined forecasts CSV | Yes | `TestMaintenanceDataRouting.test_gap_detected_and_filled` |
| Detect missing ensembles | Yes | `TestMaintenanceDataRouting.test_gap_detected_and_filled`, `test_no_gaps_returns_empty` |
| Lookback window limits scope | Yes | `TestMaintenanceDataRouting.test_lookback_window_limits_scope` |
| Preserve existing data | Yes | `TestMaintenanceDataRouting.test_gap_fill_preserves_existing_data` |
| Year boundary gap detection | Yes | `TestYearAndMonthBoundaries.test_year_boundary_gap_detection`, `test_year_boundary_lookback_window` |
| Full gap-fill write path (detect → read → ensemble → save) | Yes | `TestMaintenanceFullGapFill.test_gap_detected_ensemble_created_and_saved` |
| **Decadal mode (entire maintenance path)** | **NO** | All tests use pentad only |

#### Recalculate Entry Point (`recalculate_skill_metrics.py`)

```
READ all observed + modelled data → CALCULATE all skill metrics
→ CREATE ensembles → WRITE skill metrics CSV + API → WRITE forecasts CSV + API
```

| Step | Covered? | Test class/method |
|------|----------|-------------------|
| Entry point calls correct functions (pentad) | Yes | `TestRecalculateSkillMetricsIntegration.test_pentad_mode_calls_correct_functions` |
| Error exit code on save failure | Yes | `TestRecalculateSkillMetricsIntegration.test_save_error_exits_with_error_code` |
| **Full recalc pipeline with realistic data** | **NO** | `read_observed_and_modelled_data_pentade()` is mocked to return empty DataFrames |
| **Skill metric save path (CSV + API)** | **NO** | `save_pentadal_skill_metrics()` is mocked, never tested with real logic |
| **Decadal mode (entire recalc path)** | **NO** | Only pentad mode tested |

#### Cross-cutting: New Phase 3 Functions

| Function | Unit tests | Integration tests | Notes |
|----------|-----------|-------------------|-------|
| `calculate_all_skill_metrics()` | Yes (18 tests) | Indirect | `test_calculate_all_skill_metrics.py`: hand-calculated verification, single point, all-NaN, missing column, constant obs, inf values, return type |
| `_get_preprocessing_client()` | **NO** | **NO** | Singleton behavior untested: lazy init, env var read, `SAPPHIRE_API_AVAILABLE=False` |
| `_get_postprocessing_client()` | **NO** | **NO** | Same as above |
| `_reset_api_clients()` | **NO** | Indirect only | Called in conftest fixtures but no test verifies globals are cleared |
| `_bulk_upsert()` (crud.py) | **NO** | **NO** | PG ON CONFLICT path, SQLite fallback, empty batch, mixed insert+update — all untested |
| `_fallback_upsert()` (crud.py) | **NO** | **NO** | N+1 ORM pattern for non-PG backends — untested |
| Vectorized `_write_lr_forecast_to_api()` | Yes (comprehensive) | No | Existing unit tests cover the vectorized version |
| Vectorized `_write_runoff_to_api()` | Yes (comprehensive) | No | Existing unit tests cover the vectorized version |
| Vectorized `_write_combined_forecast_to_api()` | Yes (comprehensive) | No | Existing unit tests cover the vectorized version |
| Vectorized `_write_skill_metrics_to_api()` | Yes (comprehensive) | No | Existing unit tests cover the vectorized version |

### Critical Review Findings (2026-02-13)

Full review of integration test quality, edge case coverage, and branch coverage identified the following issues. Fixes are tracked as missing tests below.

#### Assertion Quality Issues — RESOLVED

Of the 8 tests originally flagged, 3 had genuinely weak assertions that were strengthened. The remaining 5 were already adequate upon closer inspection (existing exact counts or value checks were sufficient).

| Test | Status | Fix Applied |
|------|--------|-------------|
| `test_two_stations_independent_filtering` | **FIXED** | Added exact EM row count (`== 2`) and discharge spot-check for specific date |
| `test_api_records_contain_target_date` | **FIXED** | Added verification that both specific dates appear in API records |
| `test_warn_mode_continues_after_api_error` | **FIXED** | Reads CSV back and verifies code, model_short, and discharge values |
| `test_api_fallback_when_csv_missing` | **FIXED** | Pre-existing failure (missing `create=True` on mock) fixed |
| `test_ensemble_created_and_written_to_csv` | OK | Already has exact station set check and EM presence |
| `test_api_receives_correct_forecast_records` | OK | Already checks key fields |
| `test_gap_detected_and_filled` | OK | Already checks gap date |
| `test_gap_fill_preserves_existing_data` | OK | Row count + code/date check sufficient |

#### Edge Case Categories

| Category | Status | Tests |
|----------|--------|-------|
| **Value boundaries** | **DONE** | `test_edge_cases.py`: zero, near-zero (0.001), large (10000+), negative discharge |
| **Leap year** | MISSING | Feb 29 → Mar 1 pentad/decad numbering |
| **Single-row DataFrame** | **DONE** | `test_edge_cases.py::TestEmptyAndSingleRowData`, `test_calculate_all_skill_metrics.py::TestCalculateAllSkillMetricsSinglePoint` |
| **All-NaN columns** | **DONE** | `test_edge_cases.py::TestNaNHandling`, `test_calculate_all_skill_metrics.py::TestCalculateAllSkillMetricsAllNaN` |
| **Duplicate (date, code, model) rows** | **DONE** | `test_edge_cases.py::TestDuplicateHandling` |
| **NaN/zero/negative delta** | **DONE** | `test_edge_cases.py::TestDeltaEdgeCases` (5 tests: NaN, zero strict, zero exact, negative, varying) |
| **Missing required columns** | **PARTIAL** | `test_calculate_all_skill_metrics.py::TestCalculateAllSkillMetricsMissingColumn` (3 tests), `test_edge_cases.py::TestMissingRequiredColumns` (gap_detector KeyError documented) |
| **NaT dates in gap_detector** | **DONE** | `test_edge_cases.py::TestNaTDatesInGapDetector` (2 tests: graceful drop, all-NaT → empty) |

#### Workflow Branches

| Branch | Entry point | Status | Tests |
|--------|------------|--------|-------|
| `load_environment()` failure | All three | MISSING | Exception propagates with no sys.exit — test crash behavior |
| Invalid `SAPPHIRE_PREDICTION_MODE` | All three | **DONE** | `test_operational_workflow.py`, `test_maintenance_workflow.py`, `test_recalc_workflow.py` — all verify `sys.exit(1)` |
| Maintenance `BOTH` mode | maintenance | **DONE** | `test_maintenance_workflow.py::test_both_mode_processes_both` |
| Maintenance `DECAD` mode | maintenance | **DONE** | `test_maintenance_workflow.py::test_decad_mode_only` |
| Maintenance gap-fill save error | maintenance | **DONE** | `test_maintenance_workflow.py::test_save_error_causes_exit_1` |
| Recalc `DECAD`-only mode | recalc | **DONE** | `test_recalc_workflow.py::test_decad_only_mode` |
| Maintenance: gap dates but empty forecast data | maintenance | MISSING | `modelled_filtered.empty` after filtering |
| Maintenance: gap dates but empty skill metrics | maintenance | MISSING | `skill_metrics.empty` in gap-fill path |
| Default lookback (7 days) | maintenance | MISSING | Only custom value (14) tested |
| Empty data from read functions (with non-empty skill) | operational | MISSING | Could call `create_ensemble_forecasts()` with empty data |
| Save success path (returns None) | All three | MISSING | Only error returns tested |

#### Test Infrastructure Issues

| Issue | Files affected | Fix |
|-------|---------------|-----|
| 60+ tests use `os.environ[k] = v` / try-finally instead of `patch.dict` | `test_api_integration.py`, `test_api_read.py` | Migrate to `@patch.dict(os.environ, {...})` |
| `SAPPHIRE_TEST_ENV` not set | `test_api_integration.py`, `test_api_read.py` | Add to fixtures to prevent accidental real API calls |

### Missing Tests (Updated)

Tests below are ordered by priority. Each test should use real logic for everything inside the boundary and only mock external API clients and filesystem paths.

#### High Priority — Assertion quality + data flow gaps

| # | Test | File | Status | Description |
|---|------|------|--------|-------------|
| 1 | **Strengthen weak assertions** | `test_integration_postprocessing.py` | **DONE** | 3 tests strengthened (exact EM counts, discharge spot-checks, CSV content verification). Pre-existing `test_api_fallback_when_csv_missing` failure fixed. |
| 2 | **Value boundary edge cases** | `test_edge_cases.py` | **DONE** | `TestDischargeValueBoundaries`: zero, near-zero, large, negative discharge. `TestDeltaEdgeCases`: 5 delta edge cases. |
| 3 | **Duplicate forecast rows** | `test_edge_cases.py` | **DONE** | `TestDuplicateHandling` (pre-existing, 3 tests) |
| 4 | **NaN/zero delta values** | `test_edge_cases.py` | **DONE** | `TestDeltaEdgeCases`: NaN delta, zero delta (strict + exact), negative delta → NaN, varying delta per row. |
| 5 | **`calculate_all_skill_metrics()` unit tests** | `test_calculate_all_skill_metrics.py` | **DONE** | 18 tests in 7 classes: happy path (hand-calculated, 5-point verification of all 6 metrics), perfect forecast, partial accuracy, single point (MAE valid, NSE/sdivsigma NaN), all-NaN (obs/sim/delta/mixed), missing columns (ValueError), constant observations, inf values, return type. |
| 6 | **Decadal operational pipeline** | `test_integration_postprocessing.py` | **DONE** | `TestDecadalOperationalPipeline`: 2 tests (ensemble created + CSV verified, API records correct). |
| 7 | **Maintenance full gap-fill pipeline** | `test_integration_postprocessing.py` | **DONE** | `TestMaintenanceFullGapFill.test_gap_detected_ensemble_created_and_saved`: end-to-end detect → ensemble → save → verify. |
| 8 | **Recalculate with realistic data** | `test_integration_postprocessing.py` | TODO | Feed small but realistic observed + modelled DataFrames (5 stations × 3 pentads × 2 models, ~30 rows) into `calculate_skill_metrics_pentad()` with real logic. |
| 9 | **Skill metric save path (CSV + API)** | `test_integration_postprocessing.py` | TODO | Call `save_pentadal_skill_metrics()` with known skill_stats DataFrame. Verify CSV written with correct columns/sort order. |

#### Medium Priority — Edge cases, workflow branches, new functions

| # | Test | File | Status | Description |
|---|------|------|--------|-------------|
| 10 | **Ensemble skill metric numerical verification** | `test_integration_postprocessing.py` | TODO | Given known observed (100, 110) and ensemble forecast (105, 108), assert specific NSE, MAE, accuracy, sdivsigma values for the EM row. |
| 11 | **Leap year boundary** | `test_integration_postprocessing.py` | TODO | Forecasts on Feb 29 (pentad 12) and Mar 1 (pentad 13) in a leap year → ensemble created for both dates with correct pentad_in_year values. |
| 12 | **Single-row DataFrame** | `test_edge_cases.py`, `test_calculate_all_skill_metrics.py` | **DONE** | Single-row tested in both edge cases and skill metrics unit tests (n=1 → NSE/sdivsigma NaN). |
| 13 | **All-NaN column** | `test_edge_cases.py`, `test_calculate_all_skill_metrics.py` | **DONE** | `TestNaNHandling` (9 tests) + `TestCalculateAllSkillMetricsAllNaN` (4 tests). |
| 14 | **Missing required columns** | `test_calculate_all_skill_metrics.py`, `test_edge_cases.py` | **PARTIAL** | `calculate_all_skill_metrics` raises ValueError (3 tests). `gap_detector` raises KeyError (1 test, documents current behavior). data_reader still untested. |
| 15 | **NaT dates in gap_detector** | `test_edge_cases.py` | **DONE** | `TestNaTDatesInGapDetector`: NaT rows dropped gracefully, all-NaT returns empty. |
| 16 | **API client singleton behavior** | `test_forecast_library.py` | TODO | Singleton lazy init, reset, `SAPPHIRE_API_AVAILABLE=False`. |
| 17 | **Invalid SAPPHIRE_PREDICTION_MODE** | Workflow test files | **DONE** | All three entry points: `test_invalid_mode_exits_with_error` verifies `sys.exit(1)` and no data processing. |
| 18 | **Maintenance BOTH and DECAD modes** | `test_maintenance_workflow.py` | **DONE** | `test_both_mode_processes_both` (verifies both horizons read), `test_decad_mode_only` (verifies pentad not read). |
| 19 | **Maintenance gap-fill save error** | `test_maintenance_workflow.py` | **DONE** | `test_save_error_causes_exit_1`: save returns error → `sys.exit(1)`. |
| 20 | **Decadal maintenance gap-fill** | `test_integration_postprocessing.py` | TODO | Same as #7 but for decad. |
| 21 | **Decadal recalculate pipeline** | `test_integration_postprocessing.py` | **PARTIAL** | `test_recalc_workflow.py::test_decad_only_mode` covers workflow branch. Realistic-data integration still TODO. |

#### Lower Priority — Infrastructure + CRUD

| # | Test | File | Description |
|---|------|------|-------------|
| 22 | **Migrate API tests to patch.dict** | `test_api_integration.py`, `test_api_read.py` | Replace 60+ `os.environ[k]=v` / try-finally patterns with `@patch.dict(os.environ, {...})`. Add `SAPPHIRE_TEST_ENV=True` to fixtures. |
| 23 | **`_bulk_upsert` insert-only** | `sapphire/services/postprocessing/tests/test_crud.py` (new file) | Insert 10 new forecast records via `create_forecast()`, verify all 10 returned and queryable. |
| 24 | **`_bulk_upsert` update-only** | `test_crud.py` | Insert 5 records, then upsert same 5 with changed `forecasted_discharge` → verify updated values. |
| 25 | **`_bulk_upsert` mixed insert+update** | `test_crud.py` | Insert 3 records, then upsert 5 (3 existing + 2 new) → verify 5 total records with correct values. |
| 26 | **`_bulk_upsert` empty batch** | `test_crud.py` | Call `create_forecast()` with empty `data` list → returns empty list, no DB error. |
| 27 | **`_fallback_upsert` (SQLite path)** | `test_crud.py` | Force `PG_AVAILABLE=False` or use SQLite backend. Verify insert, update, and mixed insert+update all work correctly via the N+1 ORM path. |
| 28 | **CRUD get with filters** | `test_crud.py` | Insert forecasts with varied horizon/code/date, then query with different filter combinations. Verify correct records returned. |

### Remaining Test Gaps (non-integration)

| Gap | Priority | Notes |
|-----|----------|-------|
| `src/api_writer.py` tests | Deferred | Module not yet extracted (Phase 3) |
| `src/skill_metrics.py` tests | Deferred | Module not yet extracted (Phase 3) |
| `src/file_writer.py` tests | Deferred | Module not yet extracted (Phase 3) |

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
- [x] ~~Bug 6: Single-model ensemble filter~~ (`_is_multi_model_ensemble()` helper, 10 tests)
- [x] ~~Config fix: Add `ieasyforecast_decadal_skill_metrics_file` to `.env`~~

### Phase 2: Module Separation

- [x] ~~Create `src/ensemble_calculator.py`~~ (extracted ensemble creation from `forecast_library.py`, commit `9ce63c8`)
- [x] ~~Create `src/data_reader.py`~~ (reads pre-calculated skill metrics from CSV/API, commit `9ce63c8`)
- [x] ~~Create `src/gap_detector.py`~~ (scan recent window for missing ensembles, commit `9ce63c8`)
- [x] ~~Create `postprocessing_operational.py`~~ (daily entry point, commit `9ce63c8`)
- [x] ~~Create `postprocessing_maintenance.py`~~ (nightly gap-fill entry point, commit `9ce63c8`)
- [x] ~~Create `recalculate_skill_metrics.py`~~ (yearly skill recalculation entry point, commit `9ce63c8`)
- [x] ~~Create `bin/daily_postprc_maintenance.sh`~~ (nightly gap-fill runner, commit `9ce63c8`)
- [x] ~~Create `bin/yearly_skill_metrics_recalculation.sh`~~ (yearly recalc runner, commit `9ce63c8`)
- [x] ~~Update Dockerfile for triple entry points~~ (default CMD → `postprocessing_operational.py`, commit `9ce63c8`)
- [x] ~~Add deprecation warning to legacy `postprocessing_forecasts.py`~~ (commit `9ce63c8`)
- [ ] Create `src/skill_metrics.py` (extract from `forecast_library.py`) — deferred to Phase 3
- [ ] Create `src/api_writer.py` (extract from `forecast_library.py`) — deferred to Phase 3
- [ ] Create `src/file_writer.py` (extract `atomic_write_csv` + CSV save logic) — deferred to Phase 3

### Phase 3: Performance Improvements

- [x] ~~Batch upsert in CRUD (server-side)~~ — `_bulk_upsert()` with PG `ON CONFLICT DO UPDATE` + N+1 fallback for SQLite (commit `eae7158`)
- [x] ~~Replace `iterrows()` with vectorized operations (client-side)~~ — vectorized record building in `_write_lr_forecast_to_api`, `_write_runoff_to_api`, `_write_combined_forecast_to_api`, `_write_skill_metrics_to_api` (commit `eae7158`)
- [x] ~~Combine triple `groupby.apply()` into single-pass~~ — `calculate_all_skill_metrics()` in pentad + decad + ensemble paths (commit `eae7158`)
- [x] ~~Fix concat-in-loop patterns (`setup_library.py`)~~ — `add_hydroposts()` + `calculate_virtual_stations_data()` (commit `eae7158`)
- [ ] Fix nested loops in virtual station calculation — further vectorization with merge/pivot (deferred)
- [x] ~~Replace multiple `.isin()` with merge~~ — ensemble filtering in pentad + decad (commit `eae7158`)
- [x] ~~Implement API client singleton~~ — `_get_preprocessing_client()` + `_get_postprocessing_client()` + `_reset_api_clients()` (commit `eae7158`)

### Phase 4: Monthly, Quarterly & Seasonal Skill Metrics

- [ ] Date utilities in `tag_library.py` (`get_quarter`, `is_quarterly_forecast_date`, `is_seasonal_forecast_date`, `get_season_months`)
- [ ] Monthly observation aggregation in `src/data_reader.py` (daily → monthly means, ≥50% coverage filter)
- [ ] CRPS implementation in `src/skill_metrics.py`
- [ ] Monthly skill metrics: point (Q50 → NSE/MAE/accuracy) + probabilistic (CRPS)
- [ ] Quarterly aggregation + skill metrics (average 3 monthly quantiles)
- [ ] Seasonal aggregation + skill metrics (configurable month range)
- [ ] Add `quarter` field to `ForecastFlags` and `PredictorDates`
- [ ] Seasonal config in `postprocessing_forecasts/config.yaml` (`start_month`, `end_month`)
- [ ] Env vars for monthly/quarterly/seasonal output file paths
- [ ] Extend `recalculate_skill_metrics.py` for monthly/quarterly/seasonal

### Phase 5: Testing

#### Completed

- [x] ~~Unit tests for `src/ensemble_calculator.py`~~ (25 tests — 15 original + 4 `_is_multi_model_ensemble` + 2 single-TFT/TiDE discard + 2 model name consistency + 2 existing)
- [x] ~~Unit tests for `src/data_reader.py`~~ (8 tests)
- [x] ~~Unit tests for `src/gap_detector.py`~~ (6 tests)
- [x] ~~Integration tests for operational workflow~~ (5 tests)
- [x] ~~Integration tests for maintenance workflow~~ (4 tests)
- [x] ~~Integration tests for recalc workflow~~ (4 tests)
- [x] ~~Integration test hardening~~ (35 tests in `test_integration_postprocessing.py` — 14 original + 21 new: single-model ensemble bug (3), extended operational routing (7), edge case inputs (3), year/month boundaries (4), quantile fields (2), recalc entry point (2))
- [x] ~~Bug 6 fix: single-model ensemble filter~~ (`_is_multi_model_ensemble()` helper in `ensemble_calculator.py`, unit + integration tests)

#### High Priority — Assertion quality + data flow gaps (#1–#9)

- [x] ~~Strengthen weak assertions~~ (3 tests strengthened, 1 pre-existing failure fixed)
- [x] ~~Value boundary edge cases~~ (zero, 0.001, 10000+, negative discharge + 5 delta edge cases)
- [x] ~~Duplicate forecast rows~~ (pre-existing `TestDuplicateHandling`, 3 tests)
- [x] ~~NaN/zero delta values~~ (`TestDeltaEdgeCases`: NaN, zero, negative, varying)
- [x] ~~`calculate_all_skill_metrics()` unit tests~~ (18 tests in 7 classes, hand-calculated verification)
- [x] ~~Decadal operational pipeline integration test~~ (`TestDecadalOperationalPipeline`, 2 tests)
- [x] ~~Maintenance full gap-fill pipeline~~ (`TestMaintenanceFullGapFill`, detect → ensemble → save)
- [ ] Recalculate with realistic data
- [ ] Skill metric save path integration test (CSV + API)

#### Medium Priority — Edge cases, branches, new functions (#10–#21)

- [ ] Ensemble skill metric numerical verification (assert specific NSE/MAE/accuracy values for EM)
- [ ] Leap year boundary (Feb 29 pentad/decad handling)
- [x] ~~Single-row DataFrame edge case~~ (edge cases + skill metrics unit tests)
- [x] ~~All-NaN column edge case~~ (`TestNaNHandling` + `TestCalculateAllSkillMetricsAllNaN`)
- [x] ~~Missing required columns validation~~ (partial: skill metrics ValueError, gap_detector KeyError documented)
- [x] ~~NaT dates in gap_detector~~ (`TestNaTDatesInGapDetector`, 2 tests)
- [ ] API client singleton behavior tests
- [x] ~~Invalid SAPPHIRE_PREDICTION_MODE → sys.exit(1)~~ (all 3 entry points)
- [x] ~~Maintenance BOTH and DECAD modes~~ (`test_both_mode_processes_both`, `test_decad_mode_only`)
- [x] ~~Maintenance gap-fill save error~~ (`test_save_error_causes_exit_1`)
- [ ] Decadal maintenance gap-fill (integration test with realistic data)

#### Lower Priority — Infrastructure + CRUD (#22–#28)

- [ ] Migrate 60+ API tests from os.environ/try-finally to patch.dict
- [ ] `_bulk_upsert` tests: insert-only, update-only, mixed, empty batch (`test_crud.py`)
- [ ] `_fallback_upsert` tests: SQLite N+1 path
- [ ] CRUD get with filter combinations
- [ ] Unit tests for deferred `src/` modules (`api_writer`, `skill_metrics`, `file_writer`)

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
| `src/gap_detector.py` | Detect missing ensembles in recent window |
| `src/file_writer.py` | Atomic CSV writing |
| `postprocessing_operational.py` | Daily operational entry point |
| `postprocessing_maintenance.py` | Nightly gap-fill entry point |
| `recalculate_skill_metrics.py` | Yearly skill recalculation entry point |
| `bin/daily_postprc_maintenance.sh` | Shell runner for nightly gap-fill (cron) |
| `bin/yearly_skill_metrics_recalculation.sh` | Shell runner for yearly skill recalculation |
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
3. Create `postprocessing_maintenance.py` (nightly gap-fill) alongside existing code
4. Create `recalculate_skill_metrics.py` (yearly recalc) alongside existing code
5. Create `bin/daily_postprc_maintenance.sh` (modelled on `bin/daily_preprunoff_maintenance.sh`)
6. Create `bin/yearly_skill_metrics_recalculation.sh` (same pattern)
7. Run new operational script in parallel with legacy in staging
8. Compare outputs
9. Add `daily_postprc_maintenance.sh` to cron schedule
10. Switch over when confident

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
- **Phase 2 (Module separation):** Legacy `postprocessing_forecasts.py` is preserved. To rollback: repoint the Luigi task / Docker CMD back to the legacy script. No schema changes involved. The nightly gap-fill can be disabled independently by commenting out its cron entry — no impact on operational or yearly recalculation. The yearly script is run manually, so there's nothing to disable.
- **Phase 3 (Batch upsert):** The new CRUD endpoint is additive. To rollback: revert the API server code; the client-side changes (vectorization) are independent and harmless.
- **Phase 4 (Monthly/quarterly/seasonal skill metrics):** Entirely additive — new skill metric records in existing tables, new functions. Rollback = remove the new records from `skill_metrics` table (filter by `horizon_type IN ('month', 'quarter', 'season')`) and revert code.

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

### Operational & Maintenance Workflows

```
Daily (operational — postprocessing_operational.py):
  1. Generate new forecasts (upstream modules)
  2. Read existing skill metrics (from CSV or API)
  3. Use pentad-specific skill to select models for ensemble
  4. Save forecast + ensemble to API and CSV

Nightly (gap-fill — postprocessing_maintenance.py):
  1. Scan last N days (default 7) for dates with forecast data
     but missing ensemble calculations
  2. Common cause: data feed delivered observations a day late,
     so ensemble couldn't be calculated during operational run
  3. Read existing skill metrics (same as operational)
  4. Calculate ensembles for gap dates using now-available data
  5. Write filled forecasts + ensembles to API
  6. Log what was filled (dates, stations, models) for audit

November/December (yearly recalc — recalculate_skill_metrics.py):
  1. Read FULL historical data (2010–present)
  2. Calculate ALL skill metrics (vectorized, single-pass)
  3. New skill_metrics_pentad.csv and skill_metrics_decad.csv generated
  4. Write updated skill metrics to API
  5. Recalculate ALL ensemble compositions
  6. These are used for ensemble selection throughout the next year
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
| 2026-02-12 | Bea/Claude | Phase 2 target architecture: split maintenance into nightly gap-fill (postprocessing_maintenance.py) and yearly recalculation (recalculate_skill_metrics.py). Added gap_detector module, POSTPROCESSING_GAPFILL_WINDOW_DAYS env var, updated file structure/tests/rollback for three entry points. Shell runners (`bin/daily_postprc_maintenance.sh`, `bin/yearly_skill_metrics_recalculation.sh`) instead of Luigi tasks for maintenance, following `daily_preprunoff_maintenance.sh` pattern |
| 2026-02-12 | Bea/Claude | Phase 4 expanded: renamed to "Monthly, Quarterly & Seasonal Skill Metrics". Monthly skill metrics calculated in postprocessing_forecasts (reads long_forecasts from API). Dual metrics: Q50-based traditional (NSE/MAE/accuracy) + CRPS. CRPS is cross-cutting — applies to pentad/decad too once quantile columns are populated (currently blocked). Quarterly/seasonal: use direct records from long_term_forecasting if available, otherwise aggregate from monthly. Note added: refine long_term_forecasting integration once module is finalized. Configurable season definition via config.yaml. Monthly observations aggregated on-the-fly from daily discharge (≥50% coverage) |
| 2026-02-12 | Bea/Claude | Post-implementation review: updated Phase 2 checklist (10 items done, 3 deferred to Phase 3). Updated Phase 5 test inventory to actual counts (131 postprocessing tests). Documented remaining test gaps: ensemble skill metric numerical verification, `_calculate_ensemble_skill()` isolation test. Updated status summary test counts. |
| 2026-02-13 | Bea/Claude | Bug 6 fix: single-model ensemble filter in `ensemble_calculator.py` — added `_is_multi_model_ensemble()` helper replacing hardcoded LR-only rejection. Integration test hardening: 21 new tests across 6 classes (single-model bug e2e, extended data routing, edge case inputs, year/month boundaries, quantile fields, recalc entry point). Model name consistency tests added. Total: 180 postprocessing tests, all passing. |
| 2026-02-13 | Bea/Claude | Phase 3 performance items marked DONE (batch upsert, vectorized writes, single-pass metrics, concat fixes, .isin→merge, API singletons). Phase 5 rewritten: full data flow audit per entry point (operational/maintenance/recalc), coverage matrix showing 35 covered steps and 16 missing integration tests prioritised into high/medium/lower tiers. Key gaps: decadal mode (all entry points), maintenance full write path, recalc with realistic data, calculate_all_skill_metrics unit tests, crud.py tests (zero coverage). |
| 2026-02-13 | Bea/Claude | Critical review of integration test quality (6-agent audit). Found: 8 tests with weak assertions that pass with broken code, 8 missing edge case categories (value boundaries completely untested, leap year, single-row, all-NaN, duplicates, NaN delta, missing columns, NaT dates), 11 untested workflow branches (load_environment failure, invalid mode, maintenance BOTH/DECAD, save error paths). Test infrastructure: 60+ API tests use unsafe os.environ pattern. Updated CLAUDE.md with assertion quality requirements. Expanded missing tests from 16 to 28 items across 3 priority tiers. |
| 2026-02-13 | Bea/Claude | Phase 5 test implementation: 233 → 270 tests (+37). New files: `test_calculate_all_skill_metrics.py` (18 tests), `test_constants.py` (shared constants). Modified: `test_edge_cases.py` (+9 tests: delta edge cases, NaT dates, negative discharge, missing columns), `test_integration_postprocessing.py` (+7: 3 strengthened assertions, 2 decadal pipeline, 1 maintenance gap-fill, 1 pre-existing fix), `test_operational_workflow.py` (+1: invalid mode), `test_maintenance_workflow.py` (+4: invalid mode, BOTH/DECAD modes, save error), `test_recalc_workflow.py` (+2: invalid mode, DECAD-only). Completed high-priority items #1–#7, medium-priority items #12–#15, #17–#19, #21 (partial). Remaining: #8–#11, #16, #20, #22–#28. |
