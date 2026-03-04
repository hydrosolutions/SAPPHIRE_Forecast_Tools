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
6. [Phase 4: Monthly, Quarterly, Seasonal & Additional Skill Metrics](#phase-4-monthly-quarterly--seasonal-skill-metrics)
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
| API read tests (postprocessing) | **DONE** — 32 tests in `test_api_read.py`, 17 tests in `test_api_integration.py` |
| API write test fix | **DONE** — `test_api_read.py` mocks corrected to use `SapphirePreprocessingClient` (commit `ca29b5d`) |
| `sapphire-api-client` dependency | **DONE** — added to `iEasyHydroForecast/pyproject.toml` and `postprocessing_forecasts/pyproject.toml` |
| Module separation (operational / nightly gap-fill / yearly recalc) | **DONE** — 3 entry points + 7 src modules + 2 shell scripts, 375 tests (commits `9ce63c8`–`41d782e`) |
| Server-side batch upsert (CRUD) | **DONE** — `_bulk_upsert()` with PG ON CONFLICT + N+1 fallback (commit `eae7158`) |
| Client-side vectorization | **DONE** — vectorized record building in 4 `_write_*_to_api()` functions (commit `eae7158`) |
| Skill metrics single-pass optimization | **DONE** — `calculate_all_skill_metrics()` replaces triple groupby+merge (commit `eae7158`) |
| Remove `model_long` from apps (INFRA-005, revised) | **DONE** — `model_long` removed from `postprocessing_forecasts/src/`, `setup_library.py`, and all test data. Apps use `model_short` + `composition` column. 405 postprocessing tests pass, 161 iEasyHydroForecast tests pass. Commit `2c52d2a`. |
| Metrics registry refactoring | **DONE** — `METRIC_REGISTRY`, `METRIC_ORDER`, `THRESHOLD_METRICS` in `skill_metrics.py`. Consolidated 3 copies of `filter_for_highly_skilled_forecasts()`. Deleted 4 dead `model_long`-era functions from `ensemble_calculator.py`. 392 postprocessing tests pass, 0 skips. Commit `f70b29f`. |
| Monthly skill metrics (Phase 4a) | **DONE** — all 10 steps complete. Monthly readers, CRPS, calculate_monthly_skill_metrics, Skilled Mean (inverse-MAE weighted), Naive Mean, EM baselines, API writer (month horizon + LT model types), file writer, save + log monthly forecasts, recalculate entry point (MONTHLY/ALL modes). 638 postprocessing tests, 0 skips. See [`postprocessing_unified_plan_detailMonthlyForecasts.md`](postprocessing_unified_plan_detailMonthlyForecasts.md) for details. |
| Quarterly + seasonal skill metrics (Phase 4b) | TODO — ready to plan. Phase 4a done. DB already has quarterly (90-91d span) and seasonal (182d span, Apr-Sep) forecasts from `LR_BASE`/`LR_SM`, stored as `horizon_type='MONTH'`. Postprocessing will aggregate monthly forecasts and write with correct horizon tags. See Phase 4b checklist. |
| Tier 1 additional metrics: PBIAS, KGElf, NSE_log (Phase 4c) | **DONE** — 3 informational metrics implemented in `skill_metrics.py`, integrated through full pipeline (API writer, file writer, DB schema). 47 new unit tests in `test_tier1_metrics.py`. DB columns added (`crps`, `pbias`, `kgelf`, `nse_log`) to `SkillMetric` model/schema. CRPS DB column bundled with this phase as planned. 818 postprocessing tests, 93 CRUD tests, 0 skips. |
| Tier 2 additional metrics: FHV, FLV, F1/CSI, low-flow contingency (Phase 4d) | **DONE** — 6 metric functions (`fdc_fhv`, `fdc_flv`, `estimate_return_period_thresholds`, `binary_contingency`, `lowflow_quantiles`, `calculate_daily_skill_metrics`) in `skill_metrics.py` with `DAILY_METRIC_REGISTRY`. Daily readers in `data_reader.py`, "day" horizon in `api_writer.py`, `fhv`/`flv` DB columns, threshold skill metrics writer, `save_daily_skill_metrics()` in `file_writer.py`. ML module writes daily-resolution records via `_write_ml_daily_forecast_to_api()`. DAILY mode in `recalculate_skill_metrics.py`. 38 new tests in `test_tier2_metrics.py`. Commit `55a27a4`. |
| Tier 3 deferred metrics: drought events, SSI, BSS (Phase 4e) | DEFERRED — revisit after Tiers 1–2 are operational |
| Dashboard metrics visualization (FD-002) | TODO — depends on 4d (now complete). See [`gi_draft_dashboard_skill_metrics_visualization.md`](issues/gi_draft_dashboard_skill_metrics_visualization.md) |
| Bug 6: Single-model ensemble filter only rejects LR | **DONE** — `_is_multi_model_ensemble()` helper replaces hardcoded check |
| Comprehensive test suite (50+ unit, 12+ integration) | **DONE** — 818 postprocessing tests, 0 skips (all API tests pass via module venv). CRUD service: 93 tests. |
| Bulk-read API endpoints (for `long_term_forecasting`) | Planned — see `doc/plans/bulk_read_endpoints_instructions.md` |
| API integration | **DONE** — see `doc/plans/sapphire_api_integration_plan.md` |
| Duplicate skill metrics / ensemble composition issue | **RESOLVED** — see `doc/plans/issues/gi_duplicate_skill_metrics_ensemble_composition.md` |
| Debug `print()` cleanup | **DONE** — all `print()` in `src/` replaced with `logger.debug()` or removed (commit `41d782e`) |
| API data-loss warnings | **DONE** — `api_writer.py` `dropna` warnings now log dropped codes/dates for operator investigation (commit `41d782e`) |
| Test quality refactoring | **DONE** — 12 test files refactored: mock-heavy tests replaced with real file I/O (`test_file_writer.py`, `test_postprocessing_tools.py`, `test_api_integration.py`), weak `if not df.empty` checks replaced with `assert not df.empty`, `os.environ` management replaced with `monkeypatch` |
| Unified validation script | **DONE** — `apps/run_validation.sh` orchestrates all 3 validation stages (unit tests → local pipeline → Docker smoke tests) with flags (`--skip-docker`, `--skip-pipeline`, `--dry-run`). Documented in `doc/dev/testing_workflow.md` |
| PP-010: Pentad/decad reads should use API | **DONE** — `data_reader.read_observed_and_modelled_data()` reads observations from preprocessing API and LR/ML forecasts from postprocessing API (API-first, CSV fallback). All 3 entry points (operational, maintenance, recalculation) use the new readers. NE and virtual station calculations remain in `setup_library`, called explicitly from entry points. 960 postprocessing tests, 0 skips. |
| PP-011: Skill metrics API unique key includes date | **DONE** — API schema uses `UniqueConstraint("horizon_type", "code", "model_type", "date", "horizon_in_year")`. Client-side `api_writer._write_skill_metrics_to_api()` computes per-row `date` from `horizon_in_year` + target year. Different recalculation years produce distinct records. |

### Pre-requisites (all completed)

1. ~~**Merge `main` into `develop_long_term`**~~ — Done (PR #290, commit `ab1e2ab`)
2. ~~Verify Tier 1 fixes pass on merged branch~~ — All 79 postprocessing + 206 iEasyHydroForecast tests pass

---

## Current Architecture

### Module Structure

```
apps/postprocessing_forecasts/
├── postprocessing_operational.py      # Daily entry point (fast path)
├── postprocessing_maintenance.py      # Nightly gap-fill entry point
├── recalculate_skill_metrics.py       # Yearly skill recalculation entry point
├── postprocessing_forecasts.py        # DEPRECATED legacy entry point
├── src/
│   ├── __init__.py                    # Package init
│   ├── api_writer.py                  # API write logic (singleton client, batch upsert)
│   ├── data_reader.py                 # Read skill metrics from CSV/API
│   ├── ensemble_calculator.py         # EM/NE ensemble creation + threshold filtering
│   ├── file_writer.py                 # Atomic CSV writes + save orchestration
│   ├── gap_detector.py                # Detect missing ensemble forecasts in recent window
│   ├── postprocessing_tools.py        # TimingStats, logging utilities
│   └── skill_metrics.py              # Skill metric calculations (single-pass)
├── tests/
│   ├── conftest.py                    # API singleton reset fixture
│   ├── test_api_integration.py        # 17 tests (API write: forecasts + skill metrics)
│   ├── test_api_read.py               # 32 tests (API read: LR, ML, observed, fallback)
│   ├── test_calculate_all_skill_metrics.py  # 22 tests (single-pass metrics, hand-calculated, registry)
│   ├── test_constants.py              # Shared test constants (model names, thresholds)
│   ├── test_data_reader.py            # 21 tests (CSV read, API fallback, normalization)
│   ├── test_edge_cases.py             # 46 tests (empty, NaN, boundaries, duplicates, delta)
│   ├── test_ensemble_calculator.py    # 9 tests (filtering, creation, NE exclusion, model name consistency)
│   ├── test_error_accumulation.py     # 9 tests (return value tracking, legacy entry point)
│   ├── test_file_writer.py            # 6 tests (atomic write, latest extraction)
│   ├── test_gap_detector.py           # 6 tests (missing EM detection, lookback window)
│   ├── test_integration_postprocessing.py  # 63 tests (full pipeline data routing)
│   ├── test_maintenance_workflow.py   # 8 tests (gap-fill entry point)
│   ├── test_mock_postprocessing_forecasts.py  # 1 test (legacy combined forecast)
│   ├── test_operational_workflow.py   # 12 tests (daily entry point)
│   ├── test_performance.py            # 6 benchmarks (groupby, filter, vectorized)
│   ├── test_postprocessing_tools.py   # 8 tests (logging, safe .iloc[0])
│   ├── test_recalc_workflow.py        # 8 tests (yearly recalc entry point)
│   ├── test_model_long_removal.py     # 26 tests (INFRA-005 characterization + target tests)
│   ├── test_skill_metrics.py          # 7 tests (pentad calculation, ensemble creation)
│   ├── test_tier1_metrics.py          # 47 tests (PBIAS, KGE, KGElf, NSE_log: unit + registry + pipeline)
│   ├── test_wiring_integration.py     # 23 tests (entry point wiring with real internals)
│   ├── test_workflow_integration.py   # 16 tests (full E2E with real CSV I/O)
│   ├── generate_test_data.py          # Test data generator (realistic biases)
│   └── test_data/                     # Committed test CSVs (3 stations × 4 models)
├── pyproject.toml                     # Includes sapphire-api-client dependency
├── Dockerfile
└── requirements.txt

# Shared library (still used for environment, data loading, date utilities):
apps/iEasyHydroForecast/
├── forecast_library.py              # Shared helpers (_handle_api_write_error, etc.)
├── setup_library.py                 # Configuration, data loading, API reads
├── tag_library.py                   # Date utilities (pentad, decad)
└── pyproject.toml                   # Includes sapphire-api-client dependency
```

### Execution Flow (Three Entry Points)

```
postprocessing_operational.py (DAILY — fast path):
├── read LATEST forecast data (today's forecasts via setup_library)
├── read PRE-CALCULATED skill metrics (CSV primary, API fallback)
├── create ensemble for today using skill thresholds
├── save forecasts + ensemble to CSV + API
└── log recent forecasts for monitoring

postprocessing_maintenance.py (NIGHTLY — gap-fill):
├── read combined forecasts CSV
├── detect missing ensembles in lookback window (default 7 days)
├── for each gap: read data, create ensemble, save
└── log what was filled for audit trail

recalculate_skill_metrics.py (YEARLY — slow path):
├── read ALL historical data (2010–present)
├── calculate ALL skill metrics (single-pass groupby)
├── create ensembles from qualifying models
├── save skill metrics + forecasts to CSV + API
└── used for ensemble selection throughout the next year
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

### File Structure After Separation — DONE

```
apps/postprocessing_forecasts/
├── postprocessing_operational.py      # Daily entry point
├── postprocessing_maintenance.py      # Nightly gap-fill entry point
├── recalculate_skill_metrics.py       # Yearly skill recalculation entry point
├── postprocessing_forecasts.py        # DEPRECATED: Legacy entry point (keep as fallback)
├── src/
│   ├── api_writer.py                  # Shared API write logic (singleton client)
│   ├── skill_metrics.py               # Skill metric calculations (single-pass)
│   ├── ensemble_calculator.py         # EM/NE ensemble logic + threshold filtering
│   ├── data_reader.py                 # Read skill metrics from CSV/API
│   ├── gap_detector.py                # Detect missing ensembles in recent window
│   ├── file_writer.py                 # Atomic CSV writes + save orchestration
│   └── postprocessing_tools.py        # TimingStats, logging utilities
├── tests/                             # Flat layout (24 files, 375 tests)
│   ├── conftest.py                    # API singleton reset fixture
│   ├── test_api_integration.py        # API write path
│   ├── test_api_read.py               # API read + CSV fallback
│   ├── test_calculate_all_skill_metrics.py  # Single-pass metrics
│   ├── test_data_reader.py            # CSV/API reading
│   ├── test_edge_cases.py             # Boundary conditions
│   ├── test_ensemble_calculator.py    # Ensemble creation
│   ├── test_file_writer.py            # Atomic writes
│   ├── test_gap_detector.py           # Gap detection
│   ├── test_integration_postprocessing.py  # Full pipeline data routing
│   ├── test_wiring_integration.py     # Entry point wiring (real internals)
│   ├── test_workflow_integration.py   # Full E2E (real CSV I/O)
│   ├── test_*_workflow.py             # Entry point orchestration
│   ├── generate_test_data.py          # Test data generator
│   └── test_data/                     # Committed test CSVs
└── Dockerfile                         # Default CMD → postprocessing_operational.py
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

## Phase 3: Performance Improvements — DONE

All Phase 3 items are complete. The extracted `src/` modules in `postprocessing_forecasts` use the optimized implementations directly.

### Server-Side: Batch Upsert (CRUD) — DONE

- [x] **Replace N+1 queries with batch upsert** — `_bulk_upsert()` with PG `ON CONFLICT DO UPDATE` + `_fallback_upsert()` for SQLite (commit `eae7158`)

### Client-Side: Vectorized Record Building — DONE

- [x] **Replace `iterrows()` with vectorized pandas** — vectorized record building in `src/api_writer.py` (`_write_combined_forecast_to_api`, `_write_skill_metrics_to_api`)

### Skill Metrics: Single-Pass Calculation — DONE

- [x] **Combine triple `groupby.apply()` into single operation** — `calculate_all_skill_metrics()` in `src/skill_metrics.py` calculates all 6 metrics in one pass

### Other Performance Fixes — DONE

| Bottleneck | Status | Fix |
|-----------|--------|-----|
| Concat in loops (O(N²)) | **DONE** | `setup_library.py`: collect in list, concat once |
| Nested loops + concat (O(N³)) | Deferred | Virtual station vectorization (merge/pivot) — future |
| Multiple `.isin()` filters | **DONE** | Replaced with merge in ensemble filtering |
| Client reinstantiation | **DONE** | Module-level singleton in `src/api_writer.py` |
| Health check per function | **DONE** | Single check at startup |

### API Client Singleton — DONE

- [x] **Reuse API client across functions** — `_get_postprocessing_client()` in `src/api_writer.py` with `_reset_api_client()` for test isolation

---

## Phase 4: Monthly, Quarterly, Seasonal & Additional Skill Metrics

### Pre-requisite Ordering (decided 2026-02-16)

Phase 4a has two pre-requisites that must be completed **before** implementation begins:

1. ~~**INFRA-005 (revised): Remove `model_long` from apps modules.**~~ **DONE** (commit `2c52d2a`). Removed `model_long` from `postprocessing_forecasts/src/` (ensemble_calculator, skill_metrics, data_reader, api_writer), `setup_library.py` (~18 assignments across ~14 functions), and all test data/fixtures. Replaced with `model_short` + `composition` column for ensemble composition info. `model_type_description` stays server-side. 405 postprocessing tests + 161 iEasyHydroForecast tests pass. Residuals: `forecast_dashboard/` (separate cleanup), 4 deprecated `model_long=None` function parameters in `setup_library.py`.

2. **Metrics registry refactoring** (new task). Restructure `calculate_all_skill_metrics()` in `src/skill_metrics.py` into a `METRIC_REGISTRY` dict pattern (see [metrics registry note](#implementation-steps) below). This must be done before Phase 4a so that CRPS is added as a registry entry rather than a 7th hardcoded metric.

**Implementation order:** ~~INFRA-005 (remove `model_long`)~~ DONE → metrics registry → Phase 4a (monthly skill metrics).

### Scope

Extend `postprocessing_forecasts` to calculate skill metrics for all temporal resolutions produced by the forecast system:

| Resolution | Forecasts produced by | Point metrics | CRPS | Tier 1 (PBIAS, KGElf, NSE_log) | Tier 2 (FHV, FLV, F1/CSI, low-flow CSI) |
|------------|----------------------|---------------|------|-------------------------------|------------------------------------------|
| Daily | `linear_regression`, `machine_learning` | **Done** | Blocked — quantile columns not yet populated | **Done** (Phase 4c) | **TODO** (Phase 4d) |
| Pentadal (5-day) | `linear_regression`, `machine_learning` | **Done** | Blocked — quantile columns not yet populated | **Done** (Phase 4c) | N/A — insufficient temporal resolution |
| Decadal (10-day) | `linear_regression`, `machine_learning` | **Done** | Blocked — quantile columns not yet populated | **Done** (Phase 4c) | N/A — insufficient temporal resolution |
| Monthly | `long_term_forecasting` | **Done** (Phase 4a) | **Done** (Phase 4a) — quantiles available | **Done** (Phase 4c) | N/A — insufficient temporal resolution |
| Quarterly | Aggregated from monthly, or direct from `long_term_forecasting` | **TODO** (Phase 4b) | **TODO** (Phase 4b) — quantiles available | **Done** (Phase 4c) — computed if data available | N/A — insufficient temporal resolution |
| Seasonal | Aggregated from monthly, or direct from `long_term_forecasting` | **TODO** (Phase 4b) | **TODO** (Phase 4b) — quantiles available | **Done** (Phase 4c) — computed if data available | N/A — insufficient temporal resolution |

### Key Design Decisions

1. **Skill metrics calculated in `postprocessing_forecasts`**, consistent with pentad/decad. The module reads long-term forecasts from the postprocessing API and daily observations from the preprocessing API.

2. **CRPS as a cross-cutting metric for all resolutions where quantile information is available.** CRPS (Continuous Ranked Probability Score) is not limited to long-term forecasts — it applies wherever we have a quantile distribution to evaluate against observations:
   - **Monthly/quarterly/seasonal:** Quantiles (Q5–Q95) are produced by `long_term_forecasting`. CRPS is always calculated.
   - **Pentad/decad:** The `Forecast` table already has quantile columns (q05, q25, q50, q75, q95) but they are **not yet populated** by `linear_regression` / `machine_learning`. Once these modules produce prediction intervals or ensemble quantiles, CRPS can be calculated for short-term forecasts too. Until then, pentad/decad use traditional metrics only.
   - **Traditional (point-based):** Always calculated for all resolutions. For long-term forecasts, Q50 (median) is used as the point forecast for NSE, MAE, sdivsigma, accuracy.

3. **Aggregation from monthly forecasts.** Quarterly and seasonal values
   are produced by aggregating monthly forecast quantiles (averaging Q5–Q95
   across the target months). The `long_forecasts` table already contains
   multi-month records (90-91 day quarterly spans and 182-day Apr-Sep
   seasonal spans from `LR_BASE`/`LR_SM`), but these are stored with
   `horizon_type='MONTH'` and are **not** consumed by postprocessing.
   Instead, postprocessing reads only the single-month records (27-30 day
   spans) and aggregates them into quarterly/seasonal values, then writes
   the results with `horizon_type='quarter'`/`'season'`.

   > **Data verified (2026-03-04):** The `long_forecasts` table contains:
   > - 1,307,069 monthly records (27-30d span) from 9 models (GBT, LR_BASE, LR_SM, LR_SM_DT, LR_SM_ROF, MC_ALD, SM_GBT, SM_GBT_LR, SM_GBT_NORM)
   > - 18,204 quarterly records (90-91d, rolling 3-month windows Apr-Dec) from 2 models (LR_BASE, LR_SM) — these are direct outputs, not used by postprocessing
   > - 10,854 seasonal records (182d, Apr 1 - Sep 30) from 2 models (LR_BASE, LR_SM) — these are direct outputs, not used by postprocessing
   >
   > All records use `horizon_type='MONTH'`. Postprocessing aggregates from
   > the monthly records to ensure all 9 models contribute to ensemble
   > candidates, not just the 2 models that produce direct multi-month
   > records.

4. **Configurable season definition.** Season start/end months are defined in `config.yaml` (not hardcoded), supporting different deployments (Central Asia Apr–Sep, Nepal Jun–Sep, Switzerland Apr–Oct, etc.).

5. **Monthly observations aggregated on-the-fly.** Daily discharge from the preprocessing API (`runoffs` table) is grouped by year/month. A month requires ≥50% non-missing days to be valid (same rule as `long_term_forecasting/post_process_lt_forecast.py:calculate_lt_statistics_calendar_month()`).

6. **Accuracy + delta computed for all resolutions, including monthly (decided 2026-02-16).** The accuracy metric (fraction of forecasts within ±delta of observed) is the standard method used by Central Asian hydromet services and must be supported at all temporal resolutions. For monthly metrics, delta is computed on-the-fly: `delta = 0.674 * std(monthly_observed_discharge)` per (station, month_in_year), using the same observations aggregated in step 5. This is self-contained — no cross-module dependency on `long_term_forecasting`.

7. **`Skilled Mean` and `Naive Mean` computed in postprocessing (decided 2026-02-16).** These reference baselines exist in the API schema (`ModelType` enum) but are not produced by `long_term_forecasting`. Phase 4a computes them during skill metric calculation:
   - **Naive Mean**: Climatological mean monthly discharge (mean of all years' observations for that station+month). This is the no-skill baseline.
   - **Skilled Mean**: Weighted average of individual model forecasts, weighted by inverse MAE: `w_i = 1/(MAE_i + eps)` where `eps = mean(MAE)/100`. Uses the same threshold-filtered model pool as EM.
   Both are written to the `long_forecasts` table and evaluated alongside individual models.

8. **CRPS supported as an additional metric wherever quantile information is available (decided 2026-02-16).** Both accuracy+delta (point-based, hydromet standard) and CRPS (probabilistic) are computed. They serve different audiences: accuracy for operational hydromet reporting, CRPS for scientific forecast verification.

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

  ℹ long_term_forecasting integration (verified 2026-03-04):
  The long_forecasts table has direct quarterly (90-91d) and seasonal
  (182d) records from LR_BASE/LR_SM, but only 2 of 9 models produce
  them. Postprocessing aggregates from single-month records (all 9
  models) to ensure full ensemble candidate coverage.
```

### `SAPPHIRE_PREDICTION_MODE` Semantics (decided 2026-02-16)

| Value | Resolutions | Backward compatible? |
|-------|-------------|---------------------|
| `PENTAD` | Pentad only | Yes (unchanged) |
| `DECAD` | Decad only | Yes (unchanged) |
| `BOTH` | Pentad + decad | Yes (unchanged, short-term only) |
| `MONTHLY` | Monthly only | **New in Phase 4a** |
| `ALL` | Pentad + decad + monthly (+ quarterly + seasonal when Phase 4b is done) | **New in Phase 4a** |

**Entry point support:**
- **`recalculate_skill_metrics.py`:** Supports all modes including `MONTHLY` and `ALL`. This is the definite entry point for monthly skill metric recalculation.
- **`postprocessing_operational.py`:** Supports `PENTAD`, `DECAD`, `BOTH` only for now. Whether monthly postprocessing needs its own operational entry point (running ~2x/month) is an **open question** — monthly forecasts arrive less frequently than pentad/decad, so the operational/maintenance patterns may differ. To be decided when Phase 4a implementation begins.
- **`postprocessing_maintenance.py`:** Same as operational — pentad/decad only for now. Monthly gap-fill may not be needed if the recalculation is fast enough. To be verified.

### Integration with Phase 2 Entry Points

- **Operational (daily):** No change for pentad/decad. Monthly operational postprocessing is an open question (see above).
- **Nightly gap-fill:** No change for pentad/decad. Monthly gap-fill deferred pending speed verification.
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

   > **Skill metrics registry (pre-requisite — implement BEFORE Phase 4a, decided 2026-02-16):**
   > The metrics registry must be built as a separate task before Phase 4a begins,
   > so that CRPS is added as a registry entry rather than a 7th hardcoded metric.
   > This is listed as pre-requisite #2 in the [Pre-requisite Ordering](#pre-requisite-ordering-decided-2026-02-16) section above.
   >
   > **Design:**
   > - Extract each metric into its own named function with a uniform signature
   >   `(obs, sim, **kw) -> float` (already partially done for `sdivsigma_nse`, `mae`,
   >   `forecast_accuracy_hydromet`).
   > - Add a `METRIC_REGISTRY` dict mapping metric name -> callable + metadata
   >   (`min_points`, `higher_is_better`).
   > - `calculate_all_skill_metrics()` iterates the registry instead of hardcoding
   >   the 6 metrics. Shared intermediates (NaN mask, `obs - sim`, `abs_diff`) are
   >   computed once and passed via a context dict.
   > - `filter_for_highly_skilled_forecasts()` reads `higher_is_better` from the
   >   registry to apply thresholds generically (replacing the current hardcoded
   >   `< threshold_sdivsigma` vs `> threshold_accuracy`).
   > - Threshold env var naming convention: `ieasyhydroforecast_{metric_name}_threshold`.
   >
   > **What this does NOT include** (intentionally):
   > - No dynamic plugin loading or entry points — metrics are registered in Python code
   > - No automatic API schema changes — adding a metric to the registry still requires
   >   updating the postprocessing API model/migration (this is the real bottleneck, and
   >   a registry doesn't change that)
   >
   > **Benefits:** Adding a new point metric becomes: write the function, add one
   > `register_metric()` call, add the API column. The compositor, ensemble filter,
   > CSV writer, and tests all pick it up automatically.
   >
   > **Detailed issue to be created in `doc/plans/issues/` before implementation.**

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

### Current Tests (818 postprocessing + 76 iEasyHydroForecast + 93 CRUD service, all passing, 0 skips)

| File | Tests | Covers |
|------|-------|--------|
| `postprocessing_forecasts/tests/test_api_read.py` | 32 | API read: LR/ML/observed, pagination, CSV fallback, data consistency, edge cases |
| `postprocessing_forecasts/tests/test_api_integration.py` | 17 | API write: skill metrics, combined forecasts, field mapping, NaN handling |
| `postprocessing_forecasts/tests/test_error_accumulation.py` | 9 | Error accumulation, exit codes (legacy entry point) |
| `postprocessing_forecasts/tests/test_postprocessing_tools.py` | 8 | Safe `.iloc[0]`, NaT dates, missing codes |
| `postprocessing_forecasts/tests/test_mock_postprocessing_forecasts.py` | 1 | Combined forecast consistency (legacy entry point) |
| `postprocessing_forecasts/tests/test_ensemble_calculator.py` | 25 | Helper functions, threshold filtering, ensemble creation, NE exclusion, single-model discard (LR/TFT/TiDE), composition string, decad, `_is_multi_model_ensemble` helper, model name consistency |
| `postprocessing_forecasts/tests/test_data_reader.py` | 21 | CSV read, API fallback, model mapping, empty/corrupt files, missing/extra columns, numeric code cleanup, API normalize graceful degradation |
| `postprocessing_forecasts/tests/test_gap_detector.py` | 6 | Missing EM detection, lookback window, multi-code gaps, date conversion |
| `postprocessing_forecasts/tests/test_operational_workflow.py` | 12 | Pentad/decad/both modes, error accumulation, empty skill metrics, invalid mode, concurrent errors, edge cases |
| `postprocessing_forecasts/tests/test_maintenance_workflow.py` | 8 | Gap detection, no-gap idempotency, lookback window, empty combined forecasts, invalid mode, BOTH/DECAD modes, save error |
| `postprocessing_forecasts/tests/test_recalc_workflow.py` | 8 | Calls calculate_skill_metrics, saves skill metrics, both mode, error accumulation, invalid mode, DECAD-only mode, edge cases |
| `postprocessing_forecasts/tests/test_integration_postprocessing.py` | 63 | Data routing (operational/maintenance/API fallback/failure modes), single-model ensemble bug, edge case inputs, year/month boundaries, quantile fields, recalc entry point, decadal operational pipeline, maintenance full gap-fill pipeline, realistic recalculate (3 stations × 2 pentads × 2 models, EM creation verification), skill metric save path (CSV columns/rounding/sort, API args, failure resilience), ensemble skill metric numerical verification (hand-calculated), leap year boundary (Feb 29/Mar 1), decadal maintenance gap-fill, decadal recalculate with realistic data |
| `postprocessing_forecasts/tests/test_edge_cases.py` | 46 | Empty/single-row, NaN handling, discharge boundaries (zero/negative/large), date boundaries, duplicates, thresholds, period coercion, code normalization, delta edge cases (NaN/zero/negative/varying), NaT dates in gap detector, missing columns |
| `postprocessing_forecasts/tests/test_calculate_all_skill_metrics.py` | 18 | Unit tests for `calculate_all_skill_metrics()`: happy path (hand-calculated), single point, all-NaN, missing column, constant observations, inf values, return type |
| `postprocessing_forecasts/tests/test_wiring_integration.py` | 23 | Wiring integration: real internal modules, mocked external boundaries. Operational (4), maintenance (4), exception propagation (2), mismatched shapes (2), recalculate (4), surplus data gap-fill (1), NE exclusion (1), cross-workflow roundtrip (1), varying delta accuracy (1), log_most_recent_forecasts no-crash (3) |
| `postprocessing_forecasts/tests/test_workflow_integration.py` | 16 | Full E2E with real CSV I/O: operational (6), maintenance (6), recalculate (6 incl. cross-workflow), edge cases (5). Uses committed test_data/ with 3 stations × 4 models. Only `load_environment()` mocked. |
| `postprocessing_forecasts/tests/test_tier1_metrics.py` | 47 | Phase 4c: PBIAS (10), KGE (7), KGElf (10), NSE_log (9), registry (5), pipeline (6). Hand-calculated verification + edge cases |
| `postprocessing_forecasts/tests/test_performance.py` | 6 | Benchmarks: triple-groupby vs single-pass, isin vs merge, iterrows vs vectorized |
| `postprocessing_forecasts/tests/test_constants.py` | — | Shared constants (model names, thresholds, delta) |
| `iEasyHydroForecast/tests/test_forecast_library.py` | 76 | Includes sdivsigma_nse, MAE, accuracy, atomic write, API failure mode, API client singleton tests |
| `sapphire/services/postprocessing/tests/test_crud.py` | 91 | CRUD: Forecast, LongForecast, LRForecast, SkillMetric, edge cases, _fallback_upsert direct, combined filters, large batch, new metric fields round-trip |
| `sapphire/services/postprocessing/tests/test_endpoints.py` | — | Endpoint tests including Tier 1 metric POST/GET round-trip |

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
| **Ensemble skill metric recalculation** | Yes | `TestEnsembleSkillMetricVerification`: hand-calculated MAE, accuracy, NSE, sdivsigma verified to 4+ decimal places |

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
| **Decadal mode (entire maintenance path)** | Yes | `TestDecadalMaintenanceFullGapFill.test_decadal_gap_detected_ensemble_created_and_saved` |

#### Recalculate Entry Point (`recalculate_skill_metrics.py`)

```
READ all observed + modelled data → CALCULATE all skill metrics
→ CREATE ensembles → WRITE skill metrics CSV + API → WRITE forecasts CSV + API
```

| Step | Covered? | Test class/method |
|------|----------|-------------------|
| Entry point calls correct functions (pentad) | Yes | `TestRecalculateSkillMetricsIntegration.test_pentad_mode_calls_correct_functions` |
| Error exit code on save failure | Yes | `TestRecalculateSkillMetricsIntegration.test_save_error_exits_with_error_code` |
| **Full recalc pipeline with realistic data** | Yes | `TestRecalculateWithRealisticData` (pentad) + `TestDecadalRecalculateWithRealisticData` (decad) |
| **Skill metric save path (CSV + API)** | Yes | `TestSkillMetricSavePath` (6 tests): CSV columns/sort, rounding, code cleanup, API args, failure resilience |
| **Decadal mode (entire recalc path)** | Yes | `TestDecadalRecalculateWithRealisticData` (5 tests): shape, EM for good station, no EM for bad, discharge values, CSV save |

#### Cross-cutting: New Phase 3 Functions

| Function | Unit tests | Integration tests | Notes |
|----------|-----------|-------------------|-------|
| `calculate_all_skill_metrics()` | Yes (18 tests) | Indirect | `test_calculate_all_skill_metrics.py`: hand-calculated verification, single point, all-NaN, missing column, constant obs, inf values, return type |
| `_get_preprocessing_client()` | Yes (5 tests) | **NO** | `TestApiClientSingleton`: lazy init, cached singleton, returns None when unavailable/class is None, custom URL |
| `_get_postprocessing_client()` | Yes (5 tests) | **NO** | Same as above, plus default URL test and reset-then-new-instance test |
| `_reset_api_clients()` | Yes (2 tests) | Indirect | Direct test verifies both globals cleared; reset-then-new-instance verifies fresh creation |
| `_bulk_upsert()` (crud.py) | Yes (15 tests) | **NO** | `test_crud.py`: `TestFallbackUpsertDirect` (6 tests), `TestCombinedFilters` (6 tests), `TestLargeBatch` (2 tests), plus pre-existing edge case tests |
| `_fallback_upsert()` (crud.py) | Yes (6 tests) | **NO** | `TestFallbackUpsertDirect`: insert-only, update-only, mixed, empty, cross-model SkillMetric, refresh verification |
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
| **Leap year** | **DONE** | `test_integration_postprocessing.py::TestLeapYearBoundary` (3 tests: ensemble on Feb 29/Mar 1, pentad_in_year verification, pentad_in_month verification) |
| **Single-row DataFrame** | **DONE** | `test_edge_cases.py::TestEmptyAndSingleRowData`, `test_calculate_all_skill_metrics.py::TestCalculateAllSkillMetricsSinglePoint` |
| **All-NaN columns** | **DONE** | `test_edge_cases.py::TestNaNHandling`, `test_calculate_all_skill_metrics.py::TestCalculateAllSkillMetricsAllNaN` |
| **Duplicate (date, code, model) rows** | **DONE** | `test_edge_cases.py::TestDuplicateHandling` |
| **NaN/zero/negative delta** | **DONE** | `test_edge_cases.py::TestDeltaEdgeCases` (5 tests: NaN, zero strict, zero exact, negative, varying) |
| **Missing required columns** | **DONE** | `test_calculate_all_skill_metrics.py::TestCalculateAllSkillMetricsMissingColumn` (3 tests), `test_edge_cases.py::TestMissingRequiredColumns` (1 test), `test_data_reader.py::TestDataReaderMissingColumns` (5 tests: missing code, extra columns, numeric cleanup, API graceful degradation) |
| **NaT dates in gap_detector** | **DONE** | `test_edge_cases.py::TestNaTDatesInGapDetector` (2 tests: graceful drop, all-NaT → empty) |

#### Workflow Branches

| Branch | Entry point | Status | Tests |
|--------|------------|--------|-------|
| `load_environment()` failure | All three | **DONE** | `TestOperationalEdgeCases::test_load_environment_failure_propagates`, `TestMaintenanceEdgeCases::test_load_environment_failure_propagates`, `TestRecalcEdgeCases::test_load_environment_failure_propagates` — all verify FileNotFoundError propagates uncaught |
| Invalid `SAPPHIRE_PREDICTION_MODE` | All three | **DONE** | `test_operational_workflow.py`, `test_maintenance_workflow.py`, `test_recalc_workflow.py` — all verify `sys.exit(1)` |
| Maintenance `BOTH` mode | maintenance | **DONE** | `test_maintenance_workflow.py::test_both_mode_processes_both` |
| Maintenance `DECAD` mode | maintenance | **DONE** | `test_maintenance_workflow.py::test_decad_mode_only` |
| Maintenance gap-fill save error | maintenance | **DONE** | `test_maintenance_workflow.py::test_save_error_causes_exit_1` |
| Recalc `DECAD`-only mode | recalc | **DONE** | `test_recalc_workflow.py::test_decad_only_mode` |
| Maintenance: gap dates but empty forecast data | maintenance | **DONE** | `TestMaintenanceEdgeCases::test_gap_dates_no_matching_forecast_data` — verifies early return, no skill read or ensemble creation |
| Maintenance: gap dates but empty skill metrics | maintenance | **DONE** | `TestMaintenanceEdgeCases::test_gap_dates_empty_skill_metrics` — verifies skill read called but empty → no ensemble creation |
| Default lookback (7 days) | maintenance | DEFERRED | Only custom value (14) tested. Tracked in PP-006 (`gi_draft_pp_config_yaml.md`) — adding config.yaml will include tests for default, yaml override, and env override. |
| Empty data from read functions (with non-empty skill) | operational | **DONE** | `TestOperationalEdgeCases::test_empty_modelled_with_nonempty_skill` — verifies `create_ensemble_forecasts()` still called with empty data |
| Save success path (returns None) | All three | **DONE** | `TestOperationalEdgeCases::test_save_success_path`, `TestMaintenanceEdgeCases::test_save_success_path`, `TestRecalcEdgeCases::test_save_success_path` — verify save called + exit 0 |

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
| 8 | **Recalculate with realistic data** | `test_integration_postprocessing.py` | **DONE** | `TestRecalculateWithRealisticData`: 5 tests. 3 stations × 2 pentads × 2 models (LR, TFT) with known observed values. Verifies: skill stats shape/groups (12 base groups, n_pairs=5), EM created for station 15001 (both models pass thresholds), no EM for station 15003 (both bad), EM discharge = mean(LR, TFT), full save pipeline (CSV + API). |
| 9 | **Skill metric save path (CSV + API)** | `test_integration_postprocessing.py` | **DONE** | `TestSkillMetricSavePath`: 6 tests. Verifies: CSV columns and sort order (pentad_in_year, code, model_short), values rounded to 4 decimals, code cleaned (no .0), API called with correct args (DataFrame + 'pentad'), API failure doesn't prevent CSV, date formatted as YYYY-MM-DD. |

#### Medium Priority — Edge cases, workflow branches, new functions

| # | Test | File | Status | Description |
|---|------|------|--------|-------------|
| 10 | **Ensemble skill metric numerical verification** | `test_integration_postprocessing.py` | **DONE** | `TestEnsembleSkillMetricVerification`: 3 dates, hand-calculated MAE (11/3), accuracy (1.0), NSE (1-45/168), sdivsigma (sqrt(22.5/84)) verified to 4+ decimal places. |
| 11 | **Leap year boundary** | `test_integration_postprocessing.py` | **DONE** | `TestLeapYearBoundary`: 3 tests. Feb 29 → pentad 12, Mar 1 → pentad 13, ensemble created for both. tag_library verification included. |
| 12 | **Single-row DataFrame** | `test_edge_cases.py`, `test_calculate_all_skill_metrics.py` | **DONE** | Single-row tested in both edge cases and skill metrics unit tests (n=1 → NSE/sdivsigma NaN). |
| 13 | **All-NaN column** | `test_edge_cases.py`, `test_calculate_all_skill_metrics.py` | **DONE** | `TestNaNHandling` (9 tests) + `TestCalculateAllSkillMetricsAllNaN` (4 tests). |
| 14 | **Missing required columns** | `test_calculate_all_skill_metrics.py`, `test_edge_cases.py`, `test_data_reader.py` | **DONE** | `calculate_all_skill_metrics` raises ValueError (3 tests). `gap_detector` raises KeyError (1 test). `TestDataReaderMissingColumns` (5 tests): missing code column, extra columns, numeric code cleanup, API normalize graceful degradation for missing model_type and horizon_in_year. |
| 15 | **NaT dates in gap_detector** | `test_edge_cases.py` | **DONE** | `TestNaTDatesInGapDetector`: NaT rows dropped gracefully, all-NaT returns empty. |
| 16 | **API client singleton behavior** | `test_forecast_library.py` | **DONE** | `TestApiClientSingleton`: 11 tests. Lazy init, cached singleton, reset clears both, new instance after reset, returns None when API unavailable or class is None, default URL, custom URL. |
| 17 | **Invalid SAPPHIRE_PREDICTION_MODE** | Workflow test files | **DONE** | All three entry points: `test_invalid_mode_exits_with_error` verifies `sys.exit(1)` and no data processing. |
| 18 | **Maintenance BOTH and DECAD modes** | `test_maintenance_workflow.py` | **DONE** | `test_both_mode_processes_both` (verifies both horizons read), `test_decad_mode_only` (verifies pentad not read). |
| 19 | **Maintenance gap-fill save error** | `test_maintenance_workflow.py` | **DONE** | `test_save_error_causes_exit_1`: save returns error → `sys.exit(1)`. |
| 20 | **Decadal maintenance gap-fill** | `test_integration_postprocessing.py` | **DONE** | `TestDecadalMaintenanceFullGapFill`: end-to-end detect → ensemble → save for decad. Gap in Jan 20 detected, EM created (mean=105), CSV saved. |
| 21 | **Decadal recalculate pipeline** | `test_integration_postprocessing.py` | **DONE** | `TestDecadalRecalculateWithRealisticData`: 5 tests. 3 stations × 2 decads × 2 models. Verifies skill stats shape, EM for good station, no EM for bad station, EM discharge = mean(LR, TFT), save to CSV. |

#### Lower Priority — Infrastructure + CRUD

| # | Test | File | Description |
|---|------|------|-------------|
| 23 | ~~**`_bulk_upsert` insert-only**~~ | `test_crud.py` | **DONE** — `TestFallbackUpsertDirect.test_insert_only_batch`, `TestLargeBatch.test_fifty_record_batch` |
| 24 | ~~**`_bulk_upsert` update-only**~~ | `test_crud.py` | **DONE** — `TestFallbackUpsertDirect.test_update_only_batch` |
| 25 | ~~**`_bulk_upsert` mixed insert+update**~~ | `test_crud.py` | **DONE** — `TestFallbackUpsertDirect.test_mixed_insert_and_update` |
| 26 | ~~**`_bulk_upsert` empty batch**~~ | `test_crud.py` | **DONE** — `TestFallbackUpsertDirect.test_empty_batch` |
| 27 | ~~**`_fallback_upsert` (SQLite path)**~~ | `test_crud.py` | **DONE** — `TestFallbackUpsertDirect` (6 tests): insert, update, mixed, empty, SkillMetric cross-model, refresh verification. All use SQLite in-memory. |
| 28 | ~~**CRUD get with filters**~~ | `test_crud.py` | **DONE** — `TestCombinedFilters` (6 tests): code+date+model, target=null, horizon+code, skill metric combined, LR date range, LongForecast combined filters. |

### Integration Depth Review (2026-02-15)

A critical review of whether the integration tests give confidence that the module does what it's intended to do. The test suite is large (~520 tests) and covers the right *scenarios*, but several gaps exist at the *wiring depth* level — code paths that are tested in isolation but never exercised through the actual entry point with real internal modules.

#### Summary

| Area | Confidence | Key Gap |
|------|-----------|---------|
| Ensemble creation logic | **High** | ~~NE exclusion not integration-tested~~ DONE: `TestNEExclusionIntegration` |
| Threshold filtering | **High** | — |
| Skill metric calculation | **High** | ~~Only tested outside recalc entry point~~ DONE: `TestRecalcWiringIntegration` |
| Gap detection | **High** | — |
| Operational entry point wiring | **High** | — |
| Maintenance entry point wiring | **High** | ~~Filter-to-gap-dates untested~~ DONE: `TestMaintenanceSurplusData` |
| Recalculate entry point wiring | **High** | ~~No wiring test~~ DONE: `TestRecalcWiringIntegration` (4 tests) |
| CSV/API save correctness | **Medium** | Maintenance partial-save behavior unknown |
| Cross-workflow compatibility | **High** | ~~Not tested~~ DONE: `TestCrossWorkflowRoundtrip` |

#### Gap Details

**G1: Recalculate entry point has no wiring integration test.** — **RESOLVED**
~~`test_recalc_workflow.py` mocks `forecast_library` entirely.~~ Fixed: `TestRecalcWiringIntegration` (4 tests) in `test_wiring_integration.py` wires real `fl.calculate_skill_metrics_pentad` through the entry point. Verifies skill metrics saved with correct structure/values, EM rows in forecasts, timing_stats handoff, and save error → exit 1.

**G2: Maintenance "filter to gap dates" logic is untested through the entry point.** — **RESOLVED**
~~The wiring test provides modelled data that already matches the gap dates.~~ Fixed: `TestMaintenanceSurplusData.test_fills_only_gap_dates_not_surplus` in `test_wiring_integration.py`. Combined CSV has 3 dates (2 complete, 1 gap). Verifies only the gap date gets EM, non-gap dates are not in saved output.

**G3: Weak `or`-chain assertion in `test_wiring_integration.py:790-793`.** — **ALREADY FIXED**
The assertion was already a clean `assert saved_df.empty` when reviewed — no `or` chain present. The gap description was based on an earlier version of the code.

**G4: NE (Neural Ensemble) exclusion not tested at integration level.** — **RESOLVED**
~~All integration tests use only LR, TFT, and TiDE.~~ Fixed: `TestNEExclusionIntegration.test_ne_passing_thresholds_excluded_from_em` in `test_wiring_integration.py`. Skill CSV has LR + TFT + NE all passing. Verifies EM = mean(LR, TFT) = 105, not mean(LR, TFT, NE) = 110.

**G5: No cross-workflow sequential test.** — **RESOLVED**
~~No test runs recalculate → operational sequence.~~ Fixed: `TestCrossWorkflowRoundtrip.test_recalc_saves_skill_csv_data_reader_reads_it` in `test_wiring_integration.py`. Recalculate writes skill CSV via captured `save_pentadal_skill_metrics`, then `data_reader.read_skill_metrics()` reads it back. Verifies column names, model coverage, and station codes survive the roundtrip.

**G6: Uniform delta values hide potential correctness issues.** — **RESOLVED**
~~Every test uses delta=5.0.~~ Fixed: `TestVaryingDelta.test_accuracy_with_varying_delta` in `test_wiring_integration.py`. Two stations with delta=[3.0, 8.0], LR forecasts off by 5.0. Hand-calculated: station 15001 (delta=3) → accuracy=0.0, station 15002 (delta=8) → accuracy=1.0.

**G7: `log_most_recent_forecasts_*` is a silent crash risk.** — **RESOLVED**
~~No integration test verifies this doesn't crash.~~ Fixed: `TestLogMostRecentForecasts` (3 tests) in `test_wiring_integration.py`. Tests pentad with EM rows (typical data), empty DataFrame, and decade data. All verify no crash and correct return type.

#### New Test Items

| # | Test | File | Priority | Status | Description |
|---|------|------|----------|--------|-------------|
| 29 | ~~**Recalculate wiring integration**~~ | `test_wiring_integration.py` | High | **DONE** | `TestRecalcWiringIntegration` (4 tests): real `fl.calculate_skill_metrics_pentad` through entry point, skill metrics saved with correct shape/values, EM rows in forecasts, timing_stats handoff, save error → exit 1. |
| 30 | ~~**Maintenance filter-to-gap-dates with surplus data**~~ | `test_wiring_integration.py` | High | **DONE** | `TestMaintenanceSurplusData.test_fills_only_gap_dates_not_surplus`: 3 dates (2 complete + 1 gap), only gap date gets EM. |
| 31 | ~~**Fix weak `or`-chain assertion**~~ | `test_wiring_integration.py` | High | **ALREADY FIXED** | The assertion at line 790-793 is already a clean `assert saved_df.empty` — no `or` chain present. |
| 32 | ~~**NE exclusion integration test**~~ | `test_wiring_integration.py` | Medium | **DONE** | `TestNEExclusionIntegration`: LR+TFT+NE all pass, EM = mean(LR,TFT) = 105, NE excluded from composition. |
| 33 | ~~**Cross-workflow roundtrip**~~ | `test_wiring_integration.py` | Medium | **DONE** | `TestCrossWorkflowRoundtrip`: recalc saves skill CSV → data_reader reads back, verifies model coverage and station codes. |
| 34 | ~~**Varying delta in ensemble accuracy**~~ | `test_wiring_integration.py` | Low | **DONE** | `TestVaryingDelta`: 2 stations, delta=[3,8], LR off by 5. Hand-calculated: accuracy=[0.0, 1.0]. |
| 35 | ~~**`log_most_recent_forecasts` no-crash**~~ | `test_wiring_integration.py` | Low | **DONE** | `TestLogMostRecentForecasts` (3 tests): pentad+EM, empty DataFrame, decade data. |

### Remaining Test Gaps (non-integration)

| Gap | Priority | Notes |
|-----|----------|-------|
| `src/api_writer.py` tests | **DONE** | Extracted; API tests in `test_api_integration.py` |
| `src/skill_metrics.py` tests | **DONE** | `test_skill_metrics.py` (7 tests) + `test_calculate_all_skill_metrics.py` (18 tests) + `test_tier1_metrics.py` (47 tests) |
| `src/file_writer.py` tests | **DONE** | `test_file_writer.py` (6 tests) |

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
- [x] ~~Create `src/skill_metrics.py` (extract from `forecast_library.py`)~~ — DONE (Phase 2 completion, 4 commits: `144436d`, `05c4d7a`, `ecb7c6a`, `5f3b9ff`)
- [x] ~~Create `src/api_writer.py` (extract from `forecast_library.py`)~~ — DONE (Phase 2 completion)
- [x] ~~Create `src/file_writer.py` (extract `atomic_write_csv` + CSV save logic)~~ — DONE (Phase 2 completion)

### Phase 3: Performance Improvements

- [x] ~~Batch upsert in CRUD (server-side)~~ — `_bulk_upsert()` with PG `ON CONFLICT DO UPDATE` + N+1 fallback for SQLite (commit `eae7158`)
- [x] ~~Replace `iterrows()` with vectorized operations (client-side)~~ — vectorized record building in `_write_lr_forecast_to_api`, `_write_runoff_to_api`, `_write_combined_forecast_to_api`, `_write_skill_metrics_to_api` (commit `eae7158`)
- [x] ~~Combine triple `groupby.apply()` into single-pass~~ — `calculate_all_skill_metrics()` in pentad + decad + ensemble paths (commit `eae7158`)
- [x] ~~Fix concat-in-loop patterns (`setup_library.py`)~~ — `add_hydroposts()` + `calculate_virtual_stations_data()` (commit `eae7158`)
- [ ] Fix nested loops in virtual station calculation — further vectorization with merge/pivot (deferred)
- [x] ~~Replace multiple `.isin()` with merge~~ — ensemble filtering in pentad + decad (commit `eae7158`)
- [x] ~~Implement API client singleton~~ — `_get_preprocessing_client()` + `_get_postprocessing_client()` + `_reset_api_clients()` (commit `eae7158`)

### Phase 4: Monthly, Quarterly, Seasonal & Additional Skill Metrics

Split into sub-phases: **4a (monthly)** is fully planned, **4b (quarterly + seasonal)** deferred.

#### Pre-requisites for Phase 4a (must be completed first)

- [x] **INFRA-005 (revised): Remove `model_long` from apps** — DONE (commit `2c52d2a`). `postprocessing_forecasts/src/`, `setup_library.py`, all test data cleaned. 405 + 161 tests pass.
- [x] **Metrics registry refactoring** — DONE (commit `f70b29f`). `METRIC_REGISTRY`/`METRIC_ORDER`/`THRESHOLD_METRICS` added to `skill_metrics.py`. 3 copies of `filter_for_highly_skilled_forecasts()` consolidated. 4 dead `model_long`-era functions removed from `ensemble_calculator.py`. 392 tests pass, 0 skips.
- [ ] `sapphire-api-client`: `read_long_forecasts()` + `write_long_forecasts()` (separate repo, LLM instructions provided) → bump pinned hash

#### Phase 4a: Monthly Skill Metrics

> **Detailed plan:** [`postprocessing_unified_plan_detailMonthlyForecasts.md`](postprocessing_unified_plan_detailMonthlyForecasts.md)
> Contains: step-by-step implementation (7 steps), function signatures, data flow, model mappings, test requirements, and LLM instructions for the sapphire-api-client pre-requisite.
>
> **Note:** The detail plan needs updating to reflect decisions made 2026-02-16: delta computed on-the-fly, `Skilled Mean`/`Naive Mean` computed in postprocessing, metrics registry as pre-requisite.

- [x] `src/data_reader.py`: `read_monthly_observations()` — daily→monthly aggregation (≥50% coverage), also compute `delta = 0.674 * std` per (station, month)
- [x] `src/data_reader.py`: `read_monthly_forecasts()` — read from `long_forecasts` table via API
- [x] `src/skill_metrics.py`: `calculate_crps()` — quantile-based CRPS (registered in `METRIC_REGISTRY`)
- [x] `src/skill_metrics.py`: `calculate_monthly_skill_metrics()` — point (Q50→NSE/MAE/accuracy/sdivsigma) + CRPS
- [x] `src/skill_metrics.py`: compute `Naive Mean` (climatological mean) and `Skilled Mean` (skill-weighted model average) baselines
- [x] `src/api_writer.py`: extend horizon_type mapping to "month" (LT model types come from model registry)
- [x] `src/file_writer.py`: `save_monthly_skill_metrics()` (env var: `ieasyforecast_monthly_skill_metrics_file`)
- [x] `apps/config/.env`: add `ieasyforecast_monthly_skill_metrics_file` env var
- [x] `recalculate_skill_metrics.py`: add monthly block (`SAPPHIRE_PREDICTION_MODE=MONTHLY|ALL`)
- [x] `src/file_writer.py`: `save_monthly_forecast_data()` (env var: `ieasyforecast_monthly_combined_forecast_file`) — CSV only, no API write (monthly forecasts already in `long_forecasts` table)
- [x] `src/postprocessing_tools.py`: `log_most_recent_forecasts_monthly()` — pivot by (year, month), save to forecast_logs/
- [x] `src/data_reader.py`: readiness checks in `_read_daily_runoff_api()` and `_read_long_forecasts_api()`
- [x] Tests: unit + edge case + integration + API failure (818 postprocessing tests, 93 CRUD tests, 0 skips)

#### Phase 4b: Quarterly & Seasonal Skill Metrics (ready to plan)

**Approach:** Aggregate from single-month records in `long_forecasts` table.
All 9 models produce monthly forecasts; only 2 produce direct multi-month
records. Aggregating from monthly ensures full ensemble candidate coverage.

**Data flow:**
1. Read single-month forecasts (27-30d span) from `long_forecasts` via API
2. Group by quarter (3 months) or season (configurable months)
3. Average quantiles (Q5-Q95) and point forecasts across target months
4. Create ensembles (EM, Skilled Mean, Naive Mean)
5. Calculate skill metrics (point + CRPS) against aggregated observations
6. Write forecasts with `horizon_type='quarter'`/`'season'` to API
7. Write skill metrics with `horizon_type='quarter'`/`'season'` to API

**Implementation checklist:**

_Data reader (src/data_reader.py):_
- [ ] `read_quarterly_forecasts(codes, start_year, end_year)` — read monthly
  forecasts from API, aggregate to quarterly (avg Q5-Q95 per quarter)
- [ ] `read_quarterly_observations(codes, start_year, end_year)` — read daily
  runoff from API, aggregate to quarterly means (≥50% coverage per month,
  ≥2/3 months per quarter required)
- [ ] `read_seasonal_forecasts(codes, start_year, end_year, start_month,
  end_month)` — same as quarterly but for configurable month range
- [ ] `read_seasonal_observations(codes, start_year, end_year, start_month,
  end_month)` — daily → seasonal mean (≥50% coverage per month, ≥50%
  months required)
- [ ] `read_quarterly_combined_forecasts()` — for maintenance gap detection
- [ ] `read_seasonal_combined_forecasts()` — for maintenance gap detection

_Ensemble calculator (src/ensemble_calculator.py):_
- [ ] `create_quarterly_ensemble_forecasts(forecasts, skill_stats)` — EM +
  Skilled Mean + Naive Mean for quarterly horizon (reuse monthly pattern)
- [ ] `create_seasonal_ensemble_forecasts(forecasts, skill_stats)` — same
  for seasonal horizon

_Skill metrics (src/skill_metrics.py):_
- [ ] `calculate_quarterly_skill_metrics(obs, fc, timing_stats)` — point
  metrics (NSE, MAE, sdivsigma, accuracy, PBIAS, KGElf, NSE_log) + CRPS,
  grouped by (quarter_in_year, code, model_short)
- [ ] `calculate_seasonal_skill_metrics(obs, fc, timing_stats)` — same
  grouped by (season_id, code, model_short). Season ID = 1 for the single
  configured season (may be extended later for multiple seasons)

_Writers (src/api_writer.py, src/file_writer.py):_
- [ ] `api_writer`: extend horizon_type mapping to include 'quarter' and
  'season'
- [ ] `file_writer`: `save_quarterly_forecast_data()`,
  `save_quarterly_skill_metrics()`, `save_seasonal_forecast_data()`,
  `save_seasonal_skill_metrics()`

_Entry points:_
- [ ] `recalculate_skill_metrics.py`: add `QUARTERLY` and `SEASONAL` blocks,
  update `VALID_MODES` and `ALL` mode to include them
- [ ] `postprocessing_operational_long_term.py`: extend to process quarterly
  and seasonal ensembles alongside monthly (same entry point, sequential
  processing per horizon)
- [ ] `postprocessing_maintenance_long_term.py`: extend gap detection and
  gap-fill for quarterly and seasonal horizons

_Gap detection (src/gap_detector.py):_
- [ ] `detect_missing_quarterly_ensembles(combined, lookback)` — detect
  missing EM/Skilled Mean/Naive Mean for (year, quarter, code) tuples
- [ ] `detect_missing_seasonal_ensembles(combined, lookback)` — detect
  missing ensembles for (year, code) tuples (one season per year)

_Configuration:_
- [ ] Seasonal config: `start_month`, `end_month` as env vars
  (`SAPPHIRE_SEASON_START_MONTH`, `SAPPHIRE_SEASON_END_MONTH`) with
  Central Asia defaults (4, 9). Migrate to `config.yaml` with PP-006.
- [ ] Env vars for output file paths:
  `ieasyforecast_quarterly_combined_forecast_file`,
  `ieasyforecast_quarterly_skill_metrics_file`,
  `ieasyforecast_seasonal_combined_forecast_file`,
  `ieasyforecast_seasonal_skill_metrics_file`

_Date utilities:_
- [ ] Date utilities in `tag_library.py` (`get_quarter`,
  `is_quarterly_forecast_date`, `is_seasonal_forecast_date`,
  `get_season_months`)
- [ ] Add `quarter` field to `ForecastFlags` and `PredictorDates`

_Diagnostics:_
- [ ] `write_diagnostics.py`: extend `_PERIOD_COLUMN` mapping for
  'quarter' and 'season'

_Tests:_
- [ ] Unit tests for aggregation functions (quarterly/seasonal averaging)
- [ ] Unit tests for ensemble creation (quarterly/seasonal)
- [ ] Unit tests for skill metric calculation (quarterly/seasonal)
- [ ] Edge cases: incomplete months, single-model groups, empty quarters
- [ ] Integration tests: full pipeline from monthly data → ensembles →
  skill metrics → API write

#### Phase 4c: Tier 1 Additional Metrics — PBIAS, KGElf, NSE_log (all scales, yearly calculation) — DONE

> **Decided 2026-02-17:** Based on operational hydrologist review. These metrics are calculated yearly via `recalculate_skill_metrics.py` and available via API. Dashboard visualization is planned separately (FD-002) but deferred until metrics are operational.
>
> **CRPS DB column** bundled with this phase's migration as planned — `crps`, `pbias`, `kgelf`, `nse_log` all added to `SkillMetric` model and schema in one change.

**Rationale:** The current metrics (NSE, accuracy, s/sigma, MAE, CRPS) evaluate overall forecast quality but miss two critical dimensions: (1) **volume bias** — does the model systematically over/underestimate? and (2) **low-flow performance** — is the model reliable during irrigation-critical baseflow periods?

**Metrics:**

| Metric | Formula | Interpretation | Perfect | Thresholds (Moriasi et al.) |
|--------|---------|----------------|---------|----------------------------|
| `pbias` | `100 * SUM(obs - sim) / SUM(obs)` | Percent volume bias. Positive = underestimation. | 0 | V.Good: <10%, Good: <15%, Fair: <25% |
| `kgelf` | `[KGE(Q) + KGE(1/Q)] / 2` | Composite KGE emphasizing low flows. Garcia et al. (2017). | 1.0 | V.Good: >0.75, Good: >0.50, Fair: >0.00 |
| `nse_log` | `NSE(log(obs+eps), log(sim+eps))` | NSE on log-transformed flows. Reduces high-flow dominance. | 1.0 | V.Good: >0.75, Good: >0.65, Fair: >0.50 |

**Why KGE(1/Q) instead of KGE(log Q)?** Santos et al. (2018) showed three critical pitfalls of log-transformed KGE: (1) instability when mean(log Q) approaches zero, (2) loss of unit invariance, (3) erratic sensitivity to epsilon. The inverse transformation avoids all three. NSE_log does not have these pitfalls because NSE uses additive residuals, not multiplicative ratios.

**Epsilon handling for zero flows:** `eps = mean(obs) / 100` (Pushpalatha et al., 2012).

**Implementation:**

- [x] **`src/skill_metrics.py`**: Add `pbias()`, `_kge()`, `kge_lf()`, `nse_log()` functions — DONE. `_kge()` is internal helper (not registered). Epsilon = `mean(obs)/100` for inverse transform and log transform.
- [x] **`src/skill_metrics.py`**: Register in `METRIC_REGISTRY` — DONE. `METRIC_ORDER` now has 9 metrics. All three are informational (`higher_is_better=None` for pbias, `True` for kgelf/nse_log; no `env_var` or `default_threshold`).
- [x] **`src/skill_metrics.py`**: Extend `calculate_all_skill_metrics()` — DONE. Single-pass calculation includes all 9 metrics.
- [x] **DB migration**: Add `crps`, `pbias`, `kgelf`, `nse_log` columns to `SkillMetric` table — DONE (`models.py`, all `Float`, nullable).
- [x] **API schema**: Add fields to `SkillMetricBase` — DONE (`schemas.py`, all `Optional[float]`).
- [x] **`src/api_writer.py`**: Include new fields in skill metric API writes — DONE. `_write_skill_metrics_to_api()` handles nullable float columns for all 4 new fields.
- [x] **`src/file_writer.py`**: Include new columns in CSV output — DONE. `save_pentadal_skill_metrics()`, `save_decadal_skill_metrics()`, `save_monthly_skill_metrics()` all include `pbias`, `kgelf`, `nse_log`.
- [x] **Data migrator**: Updated `_load_skill_metrics_from_csv()` and `_load_monthly_skill_metrics_from_csv()` — DONE. Graceful fallback for legacy CSVs without new columns (`if 'crps' in row and pd.notna(row['crps'])`).
- [x] **Tests**: 47 new tests in `test_tier1_metrics.py` — DONE. 5 test classes: `TestPbias` (10), `TestKge` (7), `TestKgeLf` (10), `TestNseLog` (9), `TestNewMetricsInRegistry` (5), `TestCalculateAllWithNewMetrics` (6). Hand-calculated verification for each metric. Edge cases: zero obs, constant obs/sim, negative sim, single point, empty array, min_points boundary. Plus 4 CRUD service tests (`test_crud.py`, `test_endpoints.py`).

**Edge cases tested** (all in `test_tier1_metrics.py`):
- [x] All-zero observed flows (PBIAS: division by zero → NaN)
- [x] Constant observed flow (KGE correlation undefined → NaN)
- [x] Negative sim values (ML model edge case — PBIAS/NSE_log handle gracefully)
- [x] Single data point (below `min_points` → NaN)
- [x] Empty array (→ NaN)
- [x] Min_points boundary (kgelf: 9 points → NaN, 10 points → computes)
- [x] Hand-calculated verification for each metric (known inputs → exact expected output)

**References:**
- Moriasi et al. (2007, 2015) — PBIAS performance thresholds
- Garcia et al. (2017) — KGElf composite criterion
- Santos et al. (2018) — Pitfalls of log-transformed KGE
- Pushpalatha et al. (2012) — Epsilon for zero-flow handling

#### Phase 4d: Tier 2 Additional Metrics — FHV, FLV, F1/CSI, Low-Flow Contingency (daily/sub-daily, yearly calculation)

> **Decided 2026-02-17:** Calculated yearly, API access only. Dashboard visualization deferred (FD-002). F1/CSI limited to 2-year and 5-year return periods based on operational hydrologist recommendation — higher return periods lack sufficient events for meaningful statistics.

**Rationale:** These metrics answer "How well does this model detect floods and droughts?" — a model evaluation concern for annual review and model selection, not daily operations.

**Metrics:**

| Metric | Formula | What it evaluates | Temporal scale |
|--------|---------|-------------------|----------------|
| `fhv` | `100 * SUM(sim_high - obs_high) / SUM(obs_high)` | Peak flow bias (top 2% of FDC) | Daily/sub-daily |
| `flv` | Log-FDC bias on bottom 30% of flows | Low-flow volume bias | Daily/sub-daily |
| `f1_2yr` | F1 score for 2-year return period exceedance | Flood detection (frequent events) | Daily/sub-daily |
| `f1_5yr` | F1 score for 5-year return period exceedance | Flood detection (moderate events) | Daily/sub-daily |
| `precision_2yr`, `recall_2yr` | Components of F1 | False alarm ratio / detection rate | Daily/sub-daily |
| `precision_5yr`, `recall_5yr` | Components of F1 | False alarm ratio / detection rate | Daily/sub-daily |
| `csi_2yr`, `csi_5yr` | `TP / (TP + FP + FN)` | Critical Success Index (= Threat Score) | Daily/sub-daily |
| `lowflow_csi_q90` | CSI for flows below Q90 | Drought condition detection | Daily/sub-daily |
| `lowflow_csi_q95` | CSI for flows below Q95 | Severe low-flow detection | Daily/sub-daily |

**F1 and CSI relationship:** F1 = 2*CSI / (1 + CSI). They are monotonically equivalent — rankings are identical. Both are computed and stored because F1 is the standard ML term and CSI/Threat Score is the standard hydrology term (Waller et al., 2024).

**Return period threshold estimation:**

```python
from scipy.stats import genextreme

def estimate_return_period_thresholds(
    annual_maxima: np.ndarray,
    return_periods: list[int] = [2, 5],
) -> dict[int, float]:
    """Fit GEV to annual maxima, return discharge thresholds."""
    shape, loc, scale = genextreme.fit(annual_maxima)
    return {
        T: genextreme.ppf(1.0 - 1.0 / T, shape, loc, scale)
        for T in return_periods
    }
```

- Requires annual maximum series per station from historical observations
- Minimum 15 years recommended for reliable GEV fit; flag stations with < 15 years
- Thresholds are re-estimated each yearly recalculation as more data accumulates

**Data requirements:** These metrics need daily forecast-observation pairs. They apply to models that produce daily or sub-daily forecasts. They do **not** apply to pentadal, decadal, or monthly aggregated metrics (insufficient temporal resolution for FDC percentiles and event detection).

**Schema: Separate table for threshold-based metrics**

Threshold-based metrics are parameterized (by return period or flow quantile), so they need a separate table rather than additional columns on `SkillMetric`:

```python
class ThresholdSkillMetric(Base):
    __tablename__ = "threshold_skill_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(50))           # Station code
    model_type = Column(Enum(ModelType))
    horizon_type = Column(Enum(HorizonType))  # 'day' for daily
    threshold_type = Column(String(30)) # 'flood_2yr', 'flood_5yr', 'lowflow_q90', 'lowflow_q95'
    threshold_value = Column(Float)     # Actual discharge threshold (m3/s)
    date = Column(Date)                 # Recalculation date
    f1 = Column(Float)
    precision = Column(Float)           # = 1 - FAR (False Alarm Ratio)
    recall = Column(Float)              # = POD (Probability of Detection)
    csi = Column(Float)                 # = Threat Score
    tp = Column(Integer)
    fp = Column(Integer)
    fn = Column(Integer)
    tn = Column(Integer)
    n_years = Column(Integer)           # Years of data used (data sufficiency flag)
```

FHV and FLV are scalar metrics (not parameterized by threshold), so they go in the existing `SkillMetric` table alongside PBIAS/KGElf/NSE_log:

```python
# Additional columns in SkillMetric (only populated for daily horizon_type)
fhv = Column(Float)   # Peak flow bias (%), top 2% of FDC
flv = Column(Float)   # Low-flow volume bias (%), bottom 30% of FDC
```

**Implementation:**

- [ ] **`src/skill_metrics.py`**: Add `fdc_fhv()`, `fdc_flv()` functions (from Yilmaz et al., 2008)
- [ ] **`src/skill_metrics.py`**: Add `estimate_return_period_thresholds()` (GEV fit via scipy)
- [ ] **`src/skill_metrics.py`**: Add `evaluate_threshold_exceedance()` — binary classification + F1/CSI/precision/recall
- [ ] **`src/skill_metrics.py`**: Add `evaluate_lowflow_contingency()` — same framework for below-threshold events
- [ ] **`src/skill_metrics.py`**: Register `fhv`, `flv` in `METRIC_REGISTRY`
- [ ] **DB migration**: Add `fhv`, `flv` columns to `SkillMetric` table
- [ ] **DB migration**: Create `ThresholdSkillMetric` table
- [ ] **API schema**: Add `ThresholdSkillMetricBase` schema + CRUD endpoints
- [ ] **`src/api_writer.py`**: Write threshold metrics to API
- [ ] **`recalculate_skill_metrics.py`**: Add daily threshold metric calculation block
- [ ] **Tests**: Unit tests for GEV fitting, binary classification, FDC metrics; integration tests for full pipeline; edge cases (insufficient years for GEV, no exceedance events, all-below-threshold)

**Data sufficiency rules:**
- GEV fit: require >= 15 annual maxima. Stations with fewer years get `n_years` flag and threshold metrics set to NaN
- F1/CSI for 2-year: require >= 5 observed exceedance events (TP + FN >= 5)
- F1/CSI for 5-year: require >= 3 observed exceedance events (TP + FN >= 3). Flag as "limited data" if < 5
- Low-flow contingency: require >= 30 below-threshold observations

**Why only 2-year and 5-year return periods?** (decided 2026-02-17)
- A 10-year flood occurs ~2 times in 20 years of data — the contingency table is too sparse for meaningful F1
- The 2-year threshold (~bankfull discharge) is the most operationally relevant for flood warning
- The 5-year threshold covers moderate floods relevant for infrastructure protection
- Higher return periods can be added later if data records grow sufficiently

**References:**
- Yilmaz et al. (2008) — FHV, FLV (flow duration curve metrics)
- Waller et al. (2024) — F1 = CSI monotonic equivalence
- NeuralHydrology — Reference Python implementations
- NOAA hydrotools — Hit window approach for temporal alignment

#### Phase 4e: Tier 3 Metrics — DEFERRED

> **Decided 2026-02-17:** Deferred until Tiers 1–2 are operational and proven useful. Low-flow contingency (Tier 2) covers 80% of the drought detection ground more simply.

The following metrics are **not planned** for near-term implementation but documented here for future reference:

| Metric | Category | Why deferred |
|--------|----------|--------------|
| Drought event verification (duration/deficit bias) | Event-based | High implementation complexity; event-matching is subjective. Low-flow contingency covers most practical needs. |
| Standardized Streamflow Index (SSI) | Drought index | Not yet part of operational practice in Central Asian hydromet services. Requires distribution fitting per station/month. |
| Brier Skill Score for drought | Probabilistic | Depends on SSI + probabilistic drought forecasts. Three layers of dependency before it becomes useful. |

**Revisit criteria:** Consider implementing when (1) Tier 1/2 metrics are actively used by hydromet services, (2) seasonal drought forecasting becomes an operational priority, (3) probabilistic forecast infrastructure (CRPS) is proven for all temporal scales.

### Phase 5: Testing

#### Completed

- [x] ~~Unit tests for `src/ensemble_calculator.py`~~ (9 tests — 4 filter + 8 creation + 2 model name consistency; 16 dead-code `TestHelpers` tests removed in metrics registry refactoring)
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
- [x] ~~Recalculate with realistic data~~ (`TestRecalculateWithRealisticData`, 5 tests: skill stats shape, EM creation, no EM for bad station, EM discharge verification, full save pipeline)
- [x] ~~Skill metric save path integration test (CSV + API)~~ (`TestSkillMetricSavePath`, 6 tests: CSV columns/sort, rounding, code cleanup, API args, failure resilience, date format)

#### Medium Priority — Edge cases, branches, new functions (#10–#21)

- [x] ~~Ensemble skill metric numerical verification~~ (`TestEnsembleSkillMetricVerification`: hand-calculated MAE, accuracy, NSE, sdivsigma verified to 4+ decimal places)
- [x] ~~Leap year boundary~~ (`TestLeapYearBoundary`: 3 tests, Feb 29/Mar 1 pentad values, ensemble creation)
- [x] ~~Single-row DataFrame edge case~~ (edge cases + skill metrics unit tests)
- [x] ~~All-NaN column edge case~~ (`TestNaNHandling` + `TestCalculateAllSkillMetricsAllNaN`)
- [x] ~~Missing required columns validation~~ (partial: skill metrics ValueError, gap_detector KeyError documented)
- [x] ~~NaT dates in gap_detector~~ (`TestNaTDatesInGapDetector`, 2 tests)
- [x] ~~API client singleton behavior tests~~ (`TestApiClientSingleton`: 11 tests in `test_forecast_library.py`)
- [x] ~~Invalid SAPPHIRE_PREDICTION_MODE → sys.exit(1)~~ (all 3 entry points)
- [x] ~~Maintenance BOTH and DECAD modes~~ (`test_both_mode_processes_both`, `test_decad_mode_only`)
- [x] ~~Maintenance gap-fill save error~~ (`test_save_error_causes_exit_1`)
- [x] ~~Decadal maintenance gap-fill~~ (`TestDecadalMaintenanceFullGapFill`: end-to-end detect → ensemble → save for decad)

#### Integration Depth — Wiring + cross-workflow gaps (#29–#35)

- [x] ~~Recalculate wiring integration test~~ (`TestRecalcWiringIntegration` in `test_wiring_integration.py`, 4 tests: skill calculated+saved, EM in forecasts, timing_stats handoff, save error exit 1)
- [x] ~~Maintenance filter-to-gap-dates with surplus data~~ (`TestMaintenanceSurplusData` in `test_wiring_integration.py`: 3 dates, 2 complete + 1 gap → only gap gets EM)
- [x] ~~Fix weak `or`-chain assertion~~ (already fixed — line 790-793 is clean `assert saved_df.empty`, no `or` chain)
- [x] ~~NE exclusion integration test~~ (`TestNEExclusionIntegration` in `test_wiring_integration.py`: LR+TFT+NE all pass thresholds, EM = mean(LR,TFT) only)
- [x] ~~Cross-workflow roundtrip~~ (`TestCrossWorkflowRoundtrip` in `test_wiring_integration.py`: recalc saves skill CSV → data_reader reads it back)
- [x] ~~Varying delta in ensemble accuracy~~ (`TestVaryingDelta` in `test_wiring_integration.py`: delta=[3,8], hand-calculated accuracy=[0.0, 1.0])
- [x] ~~`log_most_recent_forecasts` no-crash test~~ (`TestLogMostRecentForecasts` in `test_wiring_integration.py`, 3 tests: pentad+EM, empty, decade)

#### Lower Priority — Infrastructure + CRUD (#22–#28)

- [x] ~~Migrate API tests from os.environ/try-finally to monkeypatch~~ — DONE. `test_api_integration.py` fully refactored to use `monkeypatch` fixture + autouse env setup. `test_postprocessing_tools.py` converted to `tmp_path` + `monkeypatch`. Other test files converted from `if not df.empty` to explicit `assert`.
- [x] ~~`_bulk_upsert` tests~~ (tested via `_fallback_upsert` path in SQLite; 15 new tests in `test_crud.py`: `TestFallbackUpsertDirect` (6), `TestCombinedFilters` (6), `TestLargeBatch` (2), plus pre-existing edge cases)
- [x] ~~`_fallback_upsert` tests~~ (`TestFallbackUpsertDirect`: insert-only, update-only, mixed, empty, cross-model SkillMetric, refresh verification)
- [x] ~~CRUD get with filter combinations~~ (`TestCombinedFilters`: code+date+model, target=null, horizon+code, skill metric combined, LR date range, LongForecast combined)
- [x] ~~Unit tests for deferred `src/` modules (`api_writer`, `skill_metrics`, `file_writer`)~~ — DONE (Phase 2 completion)

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
- **Phase 4a/4b (Monthly/quarterly/seasonal skill metrics):** Entirely additive — new skill metric records in existing tables, new functions. Rollback = remove the new records from `skill_metrics` table (filter by `horizon_type IN ('month', 'quarter', 'season')`) and revert code.
- **Phase 4c (Tier 1 metrics: PBIAS, KGElf, NSE_log):** Additive columns on `SkillMetric` table. Rollback = set new columns to NULL via `UPDATE skill_metrics SET pbias=NULL, kgelf=NULL, nse_log=NULL` and revert code. Old columns unaffected.
- **Phase 4d (Tier 2 metrics: FHV, FLV, F1/CSI, low-flow CSI):** New `ThresholdSkillMetric` table + additive columns (`fhv`, `flv`) on `SkillMetric`. Rollback = drop `threshold_skill_metrics` table, set `fhv=NULL, flv=NULL`, revert code.

For database-level issues: PostgreSQL WAL-based point-in-time recovery can restore to any moment before a bad write.

---

## Appendix: Skill Metrics & Ensemble Details

### Skill Metrics Structure

Skill metrics are calculated **per model, per station, per pentad/decad of the year** — NOT a single value per model.

**Grouping keys** (INFRA-005 complete):
- Pentadal: `['pentad_in_year', 'code', 'model_short']`
- Decadal: `['decad_in_year', 'code', 'model_short']`

**Example:** 72 pentads × 50 stations × 4 models = **14,400 skill metric records**

| Metric | Function | Description | Threshold for ensemble | Phase |
|--------|----------|-------------|----------------------|-------|
| sdivsigma | `sdivsigma_nse()` | RMSE / StdDev of observations | < 0.6 (lower is better) | Done |
| NSE | `sdivsigma_nse()` | Nash-Sutcliffe Efficiency | > 0.8 (higher is better) | Done |
| MAE | `mae()` | Mean Absolute Error | (no threshold) | Done |
| accuracy | `forecast_accuracy_hydromet()` | Fraction within ±delta | > 0.8 (higher is better) | Done |
| n_pairs | — | Number of forecast-observation pairs | — | Done |
| CRPS | `calculate_crps()` | Continuous Ranked Probability Score | (no threshold) | Done (4a calc, 4c DB column) |
| pbias | `pbias()` | Percent volume bias (0 = perfect) | (no threshold — informational) | Done (4c) |
| kgelf | `kge_lf()` | KGE composite for low flows: [KGE(Q) + KGE(1/Q)] / 2 | (no threshold — informational) | Done (4c) |
| nse_log | `nse_log()` | NSE on log-transformed flows | (no threshold — informational) | Done (4c) |
| **fhv** | `fdc_fhv()` | Peak flow bias (top 2% of FDC). Daily only. | (no threshold — informational) | **4d** |
| **flv** | `fdc_flv()` | Low-flow volume bias (bottom 30% FDC). Daily only. | (no threshold — informational) | **4d** |
| **f1/csi** | `evaluate_threshold_exceedance()` | Flood detection for 2yr/5yr return periods. Daily only. Separate table. | (no threshold — informational) | **4d** |
| **lowflow_csi** | `evaluate_lowflow_contingency()` | Drought detection for Q90/Q95. Daily only. Separate table. | (no threshold — informational) | **4d** |

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

## Phase 6: Horizon Type Parameterization — NOT STARTED

### Problem

Significant code duplication between pentad and decade code paths:

| File | Duplicated lines | Functions |
|------|-----------------|-----------|
| `src/skill_metrics.py` | ~200 | `calculate_skill_metrics_pentad()` vs `calculate_skill_metrics_decade()` |
| `src/file_writer.py` | ~130 | `save_forecast_data_pentad()` vs `save_forecast_data_decade()`, `save_pentadal_skill_metrics()` vs `save_decadal_skill_metrics()` |
| `src/postprocessing_tools.py` | ~160 | `log_most_recent_forecasts_pentad()` vs `log_most_recent_forecasts_decade()` |
| `src/ensemble_calculator.py` | 0 | Already parameterized via `period_col` / `get_period_in_month_func` |

These pairs differ only in column names (`pentad_in_year` vs `decad_in_year`, `pentad_in_month` vs `decad_in_month`), env var names, and the period-computation function (`get_pentad` vs `get_decad_in_month`).

### Solution

Introduce a `HorizonConfig` dataclass that encapsulates all horizon-specific parameters:

```python
@dataclass(frozen=True)
class HorizonConfig:
    name: str                    # "pentad", "decad", "month"
    period_col: str              # "pentad_in_year", "decad_in_year", ...
    period_in_month_col: str     # "pentad_in_month", "decad_in_month", ...
    get_period_func: Callable    # tl.get_pentad, tl.get_decad_in_month, ...
    combined_csv_env: str        # env var for combined forecast CSV
    latest_csv_env: str          # env var for latest forecast CSV
    skill_csv_env: str           # env var for skill metrics CSV
    horizon_column_name: str     # column used for get_latest_forecasts()
```

Then collapse each pair into a single parameterized function:

- `calculate_skill_metrics(config: HorizonConfig, ...)`
- `save_forecast_data(config: HorizonConfig, ...)`
- `save_skill_metrics(config: HorizonConfig, ...)`
- `log_most_recent_forecasts(config: HorizonConfig, ...)`

### Affected files

| File | Change |
|------|--------|
| `src/skill_metrics.py` | Merge pentad/decade `calculate_skill_metrics_*()` into one |
| `src/file_writer.py` | Merge pentad/decade `save_forecast_data_*()` and `save_*_skill_metrics()` into one each |
| `src/postprocessing_tools.py` | Merge pentad/decade `log_most_recent_forecasts_*()` into one |
| `postprocessing_operational.py` | Pass `HorizonConfig` instead of branching on mode |
| `postprocessing_maintenance.py` | Same |
| `recalculate_skill_metrics.py` | Same |
| All test files | Update to use parameterized functions with config objects |

### Prerequisites

- All 8 issues from the pipeline inconsistency PR (atomic writes, date round-trip, decad naming, delta validation, monthly read-back, forecast_target_date helper)
- Phase 4a monthly skill metrics pipeline
- `ensemble_calculator.py` already parameterized (serves as the reference pattern)

---

## API Transition Gaps (CSV → API)

The postprocessing module is transitioning from CSV-based I/O to API-first
with CSV as fallback, eventually deprecating CSV entirely. The following
read/write paths still use CSV as primary or CSV-only:

| Path | Current State | File | Resolved |
|------|--------------|------|----------|
| Skill metrics read (pentad/decad) | **API primary, CSV fallback** | `data_reader.py` `read_skill_metrics()` | PP-014 |
| Skill metrics read (monthly) | **API primary, CSV fallback** | `data_reader.py` `read_monthly_skill_metrics()` | PP-014 |
| Gap detection (pentad/decad) | **API primary, CSV fallback** | `data_reader.py` `read_combined_forecasts()` | PP-007 |
| Gap detection (monthly) | **API primary, CSV fallback** | `data_reader.py` `read_monthly_combined_forecasts()` | PP-013 |
| Maintenance gap-fill read | **API primary, CSV fallback** | `data_reader.py` via `read_combined_forecasts()` / `read_monthly_combined_forecasts()` | PP-007, PP-013 |
| Observed + modelled data read (pentad/decad) | **API primary, CSV fallback** | `data_reader.py` `read_observed_and_modelled_data()` | PP-010 |
| Combined forecast CSV output | Dual-write (CSV + API) | `file_writer.py` | — (pending) |

**All read paths are now API-primary.** The only remaining CSV dependency
is the dual-write output path, which will be removed in Phase 6 of
`sapphire_api_integration_plan.md` once API integration is fully validated.

---

## Related Documents

| Document | Status |
|----------|--------|
| [`postprocessing_unified_plan_detailMonthlyForecasts.md`](postprocessing_unified_plan_detailMonthlyForecasts.md) | Phase 4a detail plan — monthly skill metrics implementation steps + sapphire-api-client LLM instructions |
| `doc/plans/sapphire_api_integration_plan.md` | COMPLETE (Phase 6 pending: remove CSV fallback) |
| `doc/plans/postprocessing_api_integration_test_plan.md` | COMPLETE (all 7 tests passed) |
| `doc/plans/issues/gi_duplicate_skill_metrics_ensemble_composition.md` | RESOLVED |
| `doc/plans/issues/gi_draft_prepg_yearly_norm_recalculation.md` | NOT STARTED |
| `doc/plans/bulk_read_endpoints_instructions.md` | READY for implementation |
| [`doc/plans/issues/gi_draft_dashboard_skill_metrics_visualization.md`](issues/gi_draft_dashboard_skill_metrics_visualization.md) | Draft — FD-002: Dashboard visualization of new skill metrics with plain-language interpretation. Blocked by Phases 4c/4d. |

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
| 2026-02-13 | Bea/Claude | High-priority items #8–#9 done: 270 → 281 tests (+11). `TestRecalculateWithRealisticData` (5 tests): 3 stations × 2 pentads × 2 models fed through real `calculate_skill_metrics_pentad()`, verifies skill stats shape, EM creation for qualifying station, no EM for bad stations, EM discharge = mean(models), full save pipeline. `TestSkillMetricSavePath` (6 tests): verifies `save_pentadal_skill_metrics()` CSV output (columns, sort order, 4-decimal rounding, code cleanup, date format) and API integration (correct args, failure resilience). All high-priority items now DONE. Remaining: medium #10–#11, #14, #16, #20; lower #22–#28. |
| 2026-02-15 | Bea/Claude | Integration depth review: critical review of whether integration tests give confidence the module works as intended. Found 7 gaps (G1–G7) at the wiring/cross-workflow level: recalculate entry point has no wiring integration test (G1), maintenance filter-to-gap-dates never exercised with surplus data (G2), weak `or`-chain assertion in `test_wiring_integration.py:790` (G3), NE exclusion untested at integration level (G4), no cross-workflow sequential test (G5), uniform delta masks accuracy issues (G6), `log_most_recent_forecasts` crash risk untested (G7). Added 7 new test items (#29–#35) across high/medium/low priority. Fixed 4 stale checkboxes (#10, #11, #16, #20 — already DONE but still unchecked). Confidence assessment: High for core logic, Medium for entry-point wiring, Low for recalculate wiring and cross-workflow compatibility. |
| 2026-02-15 | Bea/Claude | Phase 3 Batch 1 complete: all integration depth items (#29–#35) and CRUD tests (#22–#28) done. CRUD service: 23 → 38 tests (+15): `TestFallbackUpsertDirect` (6), `TestCombinedFilters` (6), `TestLargeBatch` (2), plus pre-existing. Wiring integration: 16 → 23 tests (+7): `TestRecalcWiringIntegration` (4), `TestMaintenanceSurplusData` (1), `TestNEExclusionIntegration` (1), `TestCrossWorkflowRoundtrip` (1), `TestVaryingDelta` (1), `TestLogMostRecentForecasts` (3). Item #31 (or-chain assertion) already fixed in prior session. Updated confidence: High across all areas except maintenance partial-save (Medium). Remaining Phase 3 work: module extractions (`src/skill_metrics.py`, `src/api_writer.py`, `src/file_writer.py`) + virtual station vectorization (deferred). |
| 2026-02-16 | Bea/Claude | Critical data-flow review of postprocessing module. Findings: (1) High confidence in data flow — three layers of integration tests (workflow E2E, wiring, pipeline) all use real logic for internal transformations, only mocking external boundaries. 375 tests pass with 0 skips when run via module venv. (2) Debug `print()` cleanup: replaced all 16 `print()` calls in `src/skill_metrics.py` and `src/file_writer.py` with `logger.debug()` or removed redundant `print()` alongside existing `logger.info/error`. Also fixed f-string logger calls to use lazy `%s` formatting. (3) API data-loss warnings improved: `api_writer.py` `dropna` warnings now say "after repair attempt" and log up to 10 dropped (code, date) or (code, model) pairs for operator investigation. (4) `sapphire-api-client` confirmed installed in module `.venv` — the 49 "skipped" tests only appeared when running bare `pytest` instead of `run_tests.sh`. Updated status summary, Current Architecture (file structure now reflects extracted `src/` modules and 24 test files), Phase 3 (marked DONE with checkbox cleanup), Phase 5 test counts. Commit `41d782e`. |
| 2026-02-16 | Bea/Claude | Unified cross-references between master plan and Phase 4a detail document. Phase 4a status summary now links to detail doc with content summary. Phase 4a checklist clarifies sapphire-api-client is a separate repo (LLM instructions provided, not implemented here). Detail document (`postprocessing_unified_plan_detailMonthlyForecasts.md`) restructured: added Quick Reference table for fast agent navigation, parent document link, pre-requisite section rewritten as LLM instructions for the separate `hydrosolutions/sapphire-api-client` repo, Files Modified table marks api-client as out of scope. Added detail doc to Related Documents table. |
| 2026-02-16 | Bea/Claude | **INFRA-005 complete** (commit `2c52d2a`). Removed `model_long` from entire app pipeline in 7 steps: (1) characterization tests, (2) `composition_agg`/`is_multi_model_composition` helpers, (3) `ensemble_calculator.py` refactored to `model_short`+`composition`, (4) `skill_metrics.py` refactored, (5) `data_reader.py`/`api_writer.py` cleaned, (6) `setup_library.py` ~18 assignments removed across ~14 functions, (7) all test data/fixtures/CSVs cleaned. 405 postprocessing + 161 iEasyHydroForecast tests pass. Residuals: `forecast_dashboard/` (separate cleanup), 4 deprecated `model_long=None` params in `setup_library.py`. Test coverage review identified 4 gaps to address: ensemble skill metric value assertions, decade ensemble via `calculate_skill_metrics_decade`, API writer composition in combined forecasts, setup_library deprecated param coverage. Updated status summary, Phase 4 pre-requisites, grouping keys appendix, test count (375→405). |
| 2026-02-16 | Bea/Claude | Phase 4 refinement — 5 decisions recorded: (1) **INFRA-005 revised**: instead of consolidating model-name dicts into a registry, **remove `model_long` from apps modules entirely**. Apps use `model_short` only; `model_type_description` stays server-side for API consumers. This eliminates the 5 scattered mapping dicts rather than consolidating them. Must be done before Phase 4a. (2) **Delta/accuracy for monthly**: compute `delta = 0.674 * std(monthly_obs)` on-the-fly per (station, month); accuracy metric supported at all resolutions (Central Asian hydromet standard). (3) **Metrics registry before Phase 4a**: restructure `calculate_all_skill_metrics()` into `METRIC_REGISTRY` pattern as a separate pre-requisite task, so CRPS is added as a registry entry. (4) **Skilled Mean / Naive Mean**: computed in postprocessing as reference baselines (not produced by `long_term_forecasting`). Naive Mean = climatological mean, Skilled Mean = skill-weighted model average. (5) **SAPPHIRE_PREDICTION_MODE**: `BOTH` stays pentad+decad (backward compat), add `MONTHLY` and `ALL` (= everything). Recalculate entry point supports all modes; operational/maintenance for monthly is an open question. Updated: Phase 4 key design decisions (added #6–#8), pre-requisite ordering section, Phase 4a checklist (3 pre-reqs + env var + baselines), prediction mode semantics table. |
| 2026-02-16 | Bea/Claude | **Metrics registry refactoring complete** (commit `f70b29f`). Added `METRIC_REGISTRY`, `METRIC_ORDER`, `THRESHOLD_METRICS` to `skill_metrics.py` as single source of truth for metric metadata (`min_points`, `higher_is_better`, `env_var`, `default_threshold`). `calculate_all_skill_metrics()` return index now uses `METRIC_ORDER`. Consolidated 3 copies of `filter_for_highly_skilled_forecasts()` into one module-level function driven by `THRESHOLD_METRICS`; `ensemble_calculator.py` version now a thin wrapper. Deleted 4 dead `model_long`-era functions (`extract_first_parentheses_content`, `model_long_agg`, `model_short_agg`, `_is_multi_model_ensemble`) and `re` import from `ensemble_calculator.py`. Cleaned up 3 test files: deleted `TestHelpers` class (16 tests), 4 characterization tests, `_LEGACY_MODEL_LONG_NAMES`, dead imports. Added `TestMetricRegistry` (4 tests). Test count: 405→392 (net -16: removed 20 dead, added 4 registry). All Phase 4a pre-requisites except `sapphire-api-client` long-forecast endpoints are now complete. **Next up:** Phase 4a monthly skill metrics implementation (pending `sapphire-api-client` `read_long_forecasts()`/`write_long_forecasts()` endpoints). |
| 2026-02-17 | Bea/Claude | **Phases 4c/4d/4e added** — additional skill metrics based on operational hydrologist review. **Phase 4c (Tier 1):** PBIAS, KGElf, NSE_log — all temporal scales, yearly calculation, registered in `METRIC_REGISTRY`. Rationale: volume bias (PBIAS) and low-flow evaluation (KGElf, NSE_log) address critical gaps for irrigation regions (Central Asia) and flood regions (Nepal, Switzerland). KGE(1/Q) chosen over KGE(log Q) per Santos et al. (2018). **Phase 4d (Tier 2):** FHV (peak flow bias, top 2% FDC), FLV (low-flow bias, bottom 30% FDC), F1/CSI for 2yr/5yr return periods only (higher return periods lack sufficient events), low-flow contingency (CSI for Q90/Q95). Daily/sub-daily only. Separate `ThresholdSkillMetric` table for parameterized metrics. GEV fit for return period thresholds, minimum 15 years data. **Phase 4e (Tier 3, deferred):** drought event verification, SSI, BSS — revisit after Tiers 1-2 proven useful. **FD-002 dashboard issue created:** `gi_draft_dashboard_skill_metrics_visualization.md` — plain-language interpretation templates for each metric (e.g., "The model overestimates total runoff by 12%"), quality categories (Very good/Good/Fair/Poor), i18n support (Russian). Blocked by 4c/4d. Updated: status summary (6 new items), skill metrics appendix table (13 metrics), related documents. |
| 2026-02-17 | Bea/Claude | **Monthly pipeline completion: Skilled Mean + cleanup.** (A) Implemented `_add_skilled_mean()` in `skill_metrics.py` — inverse-MAE weighted ensemble baseline (`w_i = 1/(MAE_i + eps)`, same threshold-filtered pool as EM). Added to EM exclusion list and call site between EM and Naive Mean. (B) Cleanup: readiness checks in `data_reader.py` (`_read_daily_runoff_api`, `_read_long_forecasts_api`), `n_pairs` type fix (`float→int` in `schemas.py`), `save_monthly_forecast_data()` in `file_writer.py` (CSV-only, monthly forecasts already in `long_forecasts` table), `log_most_recent_forecasts_monthly()` in `postprocessing_tools.py`, monthly CRUD tests in service test_crud.py, CSV-fallback-on-API-failure test. Test count: 613→638 postprocessing (+25), 86→89 CRUD (+3). CRPS DB column deferred to Phase 4c migration. |
| 2026-02-17 | Bea/Claude | **Pipeline inconsistency fixes** (8 issues from code review). (1) Atomic CSV writes: `save_forecast_data_pentad/decade()` now use `atomic_write_csv()` with try/except, removed dead `ret =` code. (2) Date string round-trip: moved `get_latest_forecasts()` before date-to-string conversion. (3) Standardized "decad" naming: added `HORIZON_TYPE_TO_API` translation layer in `api_writer.py`, internal code uses "decad" everywhere, translated to "decade" at API boundary. (4) Delta validation: `deltas[-1]` → `deltas[0]` with `np.ptp()` warning in both `forecast_accuracy_hydromet()` and `calculate_all_skill_metrics()`. (5) Monthly skill metrics read-back: added `read_monthly_skill_metrics()` to `data_reader.py` (CSV primary, API fallback), extended `SAPPHIRE_PREDICTION_MODE` to accept MONTHLY and ALL. (6) Extracted `forecast_target_date()` helper in `postprocessing_tools.py`, replaced inline `+ dt.timedelta(days=1.0)` in 3 locations. (7) **Phase 6 added**: Horizon Type Parameterization plan — `HorizonConfig` dataclass to eliminate ~490 lines of pentad/decade duplication. Test count: 598→613. |
| 2026-02-19 | Bea/Claude | **Phase 4c complete: Tier 1 informational metrics (PBIAS, KGElf, NSE_log).** (A) **Metric implementation** in `skill_metrics.py`: `pbias()` (percent volume bias, positive=underestimation), `_kge()` (internal Kling-Gupta helper), `kge_lf()` (average of KGE(Q) and KGE(1/(Q+eps)), per Garcia et al. 2017), `nse_log()` (NSE on log-transformed flows with eps=mean(obs)/100). All 3 registered in `METRIC_REGISTRY` as informational (no thresholds, not used for ensemble selection). `METRIC_ORDER` now 9 metrics. (B) **Full pipeline integration**: `api_writer.py` handles nullable float columns for crps/pbias/kgelf/nse_log; `file_writer.py` includes 3 new columns in pentadal/decadal/monthly CSV output; `data_reader.py` docstrings updated. (C) **DB schema**: `crps`, `pbias`, `kgelf`, `nse_log` columns added to `SkillMetric` model + `SkillMetricBase` schema (all `Optional[float]`). CRPS DB column bundled here as planned. Data migrator updated with graceful fallback for legacy CSVs. (D) **47 new tests** in `test_tier1_metrics.py`: `TestPbias` (10), `TestKge` (7), `TestKgeLf` (10), `TestNseLog` (9), `TestNewMetricsInRegistry` (5), `TestCalculateAllWithNewMetrics` (6). Hand-calculated verification for each metric. Edge cases: zero obs, constant obs/sim, negative sim, min_points boundary. 4 new CRUD service tests (metric fields round-trip + null defaults). (E) **Test quality refactoring** across 12 test files: mock-heavy tests replaced with real file I/O (`test_file_writer.py` now checks actual CSV content, `test_postprocessing_tools.py` uses `tmp_path` fixture, `test_api_integration.py` uses `monkeypatch`); weak `if not df.empty` checks replaced with `assert not df.empty`; `os.environ` try/finally replaced with `monkeypatch.setenv()`. (F) **Unified validation script** `apps/run_validation.sh` (529 lines): orchestrates run_tests.sh → run_locally.sh → run_docker_tests.sh with flags (`--skip-docker`, `--skip-pipeline`, `--dry-run`), timestamped logging, color output. Documented in `doc/dev/testing_workflow.md`. Test count: 638→818 postprocessing (+180), 89→93 CRUD (+4). |
| 2026-02-27 | Bea/Claude | **PP-010 complete: Pentad/decad reads migrated to API-first.** `data_reader.read_observed_and_modelled_data()` composes `read_short_term_observations()` (preprocessing API) + `read_individual_model_forecasts()` (postprocessing API for LR + env-gated ML models). All 3 entry points (operational, maintenance, recalculation) now use the new readers. NE and virtual station calculations remain in `setup_library`, called explicitly from entry points. API Transition Gaps table updated: **all read paths are now API-primary** — only dual-write output path remains as the last CSV dependency. 960 postprocessing tests, 0 skips. |
| 2026-02-27 | Bea/Claude | **PP-011 verified: Skill metrics unique key includes date.** Colleague's API schema has `UniqueConstraint("horizon_type", "code", "model_type", "date", "horizon_in_year")` with matching CRUD upsert. Client-side `api_writer._write_skill_metrics_to_api()` computes per-row `date` from `horizon_in_year` + target year via `tl.get_date_for_pentad/decad()` or `date(year, month, 1)`. End-to-end alignment confirmed. PP-008 (backfill audit trail) remains blocked — no `is_backfilled` or `backfilled_at` field in Forecast model. PP-010 status corrected from Complete to Review (awaiting server deployment verification). |
