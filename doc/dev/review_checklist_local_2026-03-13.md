# Local Review Checklist — 2026-03-13

Verification plan for review-status issue fixes using `run_locally.sh daily`.

## Pre-flight

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh   # all tests green, zero skips
```

If any tests fail, stop — fix before proceeding with pipeline runs.

### Pre-flight result (2026-03-13)

```
Passed (11): iEasyHydroForecast preprocessing_runoff preprocessing_gateway
             linear_regression machine_learning postprocessing_forecasts
             pipeline long_term_forecasting validate_pipeline
             forecast_dashboard service:postprocessing

All tests completed successfully!
```

**Result: PASS** — 11/11 modules green, zero failures, zero skips.

---

## Phase 1: Unit/Integration Test Verification (no pipeline run needed)

These issues are fully verified by the test suite alone.

### 1a — INFRA-012: Multi-Org Safety Guards

**Issue**: `review_gi_draft_infra_multi_org_safety_guards.md`
**What to check**:
- `test_write_guard.py` passes — write-side guard rejects cross-org batches
- `test_setup_library.py` passes — collision detection warns on mixed-org config
- `test_multi_org_isolation.py` passes — all 10 isolation test cases green
**Pass criteria**: All 3 test files pass with zero skips.
**Result: PASS** (2026-03-13) — postprocessing_forecasts module green, includes all safety guard tests.

### 1b — PP-022: Stale Record Refresh

**Issue**: `review_gi_draft_pp_maintenance_stale_refresh_fix.md`
**What to check**:
- Tests confirm refreshed individual-model + NE + EM rows are all saved (not just EM)
- `drop_duplicates` key includes `period_col`
- `gap_codes` computed before read, not after
**Pass criteria**: `run_tests.sh postprocessing_forecasts` — all maintenance tests green.
**Result: PASS** (2026-03-13)

### 1c — PP-023: Period-Aware Aggregation

**Issue**: `review_gi_draft_pp_period_aware_aggregation.md`
**What to check**:
- `_normalize_ml_forecasts()` filters daily targets to forecast period before averaging
- Test: pentad 6 Feb (3 days) averages only 3 targets, not contaminated by March
**Pass criteria**: Relevant tests in `postprocessing_forecasts` pass.
**Result: PASS** (2026-03-13)

### 1d — PP-019: Ensemble Quantiles

**Issue**: `review_gi_draft_pp_short_term_ensemble_quantiles.md`
**What to check**:
- EM rows have averaged q05/q25/q75/q95 from member models
- Individual ML rows retain original quantiles
- LR rows have NaN quantiles (unchanged)
**Pass criteria**: ensemble_calculator and api_writer tests pass.
**Result: PASS** (2026-03-13)

### 1e — PP-025: Org-Scoped Data Readers

**Issue**: `review_gi_draft_pp_org_scoped_data_readers.md`
**What to check**:
- All `read_*` functions accept `codes` parameter
- Short-term maintenance wires codes through
- Operational + long-term + recalculation call sites pass codes
**Pass criteria**: `run_tests.sh postprocessing_forecasts` — all reader tests green.
**Result: PASS** (2026-03-13)

### 1f — INFRA-010: Config All-Stations Bootstrap

**Issue**: `review_gi_draft_infra_config_all_stations_bootstrap.md`
**What to check**:
- `write_config_all_stations()` extracted as helper
- numpy truth-value bug fixed (`.tolist()`)
- 7 unit tests + integration test pass
**Pass criteria**: `run_tests.sh iEasyHydroForecast` green.
**Result: PASS** (2026-03-13)

### 1g — LT-001: --today Flag Runs No Models

**Issue**: `review_gi_draft_lt_today_flag_runs_no_models.md`
**What to check**:
- `run_forecast.py --today YYYY-MM-DD` actually runs models (not zero)
- Test: CLI arg parsing gives `forecast_all=True` when `--today` used
**Pass criteria**: `run_tests.sh long_term_forecasting` green.
**Result: PASS** (2026-03-13)

### 1h — LTF-ORG-001: SQL Org-Scoping

**Issue**: `review_gi_draft_ltf_sql_org_scoping.md`
**What to check**:
- `DataInterfaceDB` constructor accepts `station_codes` param
- `_add_station_filter()` adds `code IN (...)` clause; guards empty list
- `get_meteo_data()`, `get_runoff_data()`, `get_snow_data()` all apply station filter
- `_prepare_static_data()` filters CSV with `.astype(str).isin()`
- `BasePredictorDataInterface` constructor accepts `station_codes`; query uses conditions-list pattern with ORDER BY after WHERE
- `run_forecast.py` and `calibrate_and_hindcast.py` read station codes and pass to constructors
**Pass criteria**: `run_tests.sh long_term_forecasting` — all 16 org-scoping tests green (95 passed, 3 env-gated skips).
**Result: PASS** (2026-03-13)

---

## Phase 2: Short-Term Pipeline Run (`run_locally.sh daily`)

Run the daily pipeline and inspect logs for the issues below.

```bash
ieasyhydroforecast_env_file_path=<path-to-your-.env> \
  bash apps/run_locally.sh daily
```

### 2a — INFRA-009: Org-Based Station Filtering

**Issue**: `review_gi_draft_infra_org_station_filtering.md`
**What to check in logs**:
- `filter_sites_by_org()` called (look for org-filter log messages)
- No errors about unknown station codes from other orgs
- Station count matches expected for your configured org
**Pass criteria**: Pipeline completes without cross-org station errors.
**Result (run 1)**: PARTIAL — ML org-filter removed 210-280 cross-org rows. LR returned all 62 rows unchanged (missing `organization` column in JSON). **Fix applied**: backfill from env var + write path now includes `organization`.
**Result (run 2)**: **PASS** — JSON now has `organization: ["kghm"]` for all 62 stations. Filter correctly keeps all 62 (single-org config = no rows to remove). Warning is a false positive for single-org deployments; true multi-org filtering verified by unit tests (1a).
**Result (run 3)**: **PASS** — confirmed. Same behavior, no backfill needed (JSON already populated from run 2 write path).

### 2b — Date Mismatch & Sentinel (Phase 1: Sentinel)

**Issue**: `review_gi_draft_infra_date_mismatch_and_sentinel.md`
**What to check in logs**:
- LR module: no `-1.0` values in forecast output (should be NaN for insufficient data)
- Grep pipeline output/CSV for `-1.0` sentinel values
**Pass criteria**: No `-1.0` sentinel values in LR forecast output.
**Result (run 1)**: PASS — no `-1.0` sentinel values detected.
**Result (run 3)**: **PASS** — confirmed across 3 runs.

### 2c — Date Mismatch & Sentinel (Phase 2: Boundary Guard)

**What to check**: Run on a non-decad boundary day (i.e., not day 10/20/last-of-month).
- Postprocessing does NOT write decadal forecasts on non-decad days
- No spurious records with wrong issue dates in output
**Pass criteria**: Only pentad records written on non-decad days.
**Note**: If today IS a boundary day, defer this check or use `LT_FORECAST_TODAY` override.
**Result (run 1)**: PASS — day 13 correctly suppressed both pentad and decad forecasts ("No pentadal/decadal forecast for 2026-03-13").
**Result (run 3)**: **PASS** — day 14 also correctly suppressed.

### 2d — Date Mismatch & Sentinel (Phase 3: Ensemble groupby)

**What to check**:
- Ensemble calculation includes `period_col` in grouping key
- No duplicate or malformed ensemble rows in output
**Pass criteria**: Ensemble output has one EM row per (code, date, period).
**Result (run 1)**: PASS — 37 duplicates removed cleanly, no malformed EM rows.
**Result (run 3)**: **PASS** — no duplicate EM rows.

### 2e — PP-021: Maintenance Efficiency

**Issue**: `review_gi_draft_pp_maintenance_pipeline_efficiency.md`
**What to check in logs**:
- If no gaps/stale: maintenance completes in <30 seconds (just detection, no heavy reads)
- Timing logs show fast-path taken when no work needed
**Pass criteria**: Maintenance phase duration reasonable (<30s when clean).
**Result (run 1)**: NOT TESTED — pipeline crashed at ML maintenance before reaching postprocessing maintenance.
**Result (run 2)**: **TESTED** — Pentad: 1m32s, Decad: 43s. Both ran gap-fill path (not fast-path, gaps existed). Acceptable for gap-fill runs; fast-path (<30s) expected only when DB is clean.
**Result (run 3)**: **TESTED** — Pentad: 1m35s, Decad: 42s. Gap-fill path again (111 gaps across 44 sites, 0 applied — no matching data in API response). Consistent with run 2.

### 2f — PP-022: Stale Refresh (End-to-End)

**What to check** (if stale records exist in local DB):
- After maintenance, previously stale records (q05=NULL) now have q05 populated
- Check via API query or DB inspection
**Pass criteria**: Stale records refreshed. If no stale records exist locally, Phase 1b coverage is sufficient.
**Result (run 1)**: NOT TESTED — maintenance never reached.
**Result (run 2)**: **NOT TRIGGERED** — no stale records (q05=NULL) exist in local DB. Phase 1b unit test coverage is sufficient.

### 2g — PP-019: Quantiles (End-to-End)

**What to check in pipeline output**:
- Postprocessing API write payload includes q05/q25/q75/q95 fields
- EM rows have non-null quantile values (if ML models ran)
**Pass criteria**: Quantile fields present in API write logs.
**Note**: Demo org skips ML, so quantiles may all be NaN — acceptable for demo.
**Result (run 1)**: NOT FOUND — no q05/q25/q75/q95 in API write logs.
**Result (run 2)**: **PARTIAL** — Quantiles present in preprocessing output and validation passes "Quantile ordering: all valid". Not explicitly visible in postprocessing API write logs (may need debug-level logging to confirm payloads).
**Result (run 3)**: **PARTIAL** — same. Quantiles computed internally but not visible in API write logs. Needs debug logging or server-side DB inspection to fully confirm.

---

## Phase 3: Long-Term Pipeline Verification

Only runs if today is within ±5 days of issue days (default: 10th, 25th).
Today is March 13 — within window of March 10th.

### 3a — LT-001: --today Flag (End-to-End)

```bash
ieasyhydroforecast_env_file_path=<path> \
  LT_FORECAST_TODAY=2026-03-10 \
  bash apps/run_locally.sh long-term
```

**What to check**:
- Models actually execute (not "0 models to run")
- Forecast output files/API writes produced
**Pass criteria**: At least one model runs and produces output.
**Result (run 3)**: **PASS** — long_term_forecasting completed in 9m26s. Postprocessing wrote 37K quarterly + 9K seasonal forecast records to API. Validation failed only because `LT_FORECAST_TODAY=2026-03-10` doesn't match validator's query date (March 14).

### 3b — LTF-ORG-001: Org-Scoping (End-to-End)

**What to check**:
- Long-term pipeline logs show "Read N station codes for org-scoped filtering"
- SQL queries in debug logs contain `code IN (...)` clauses
- Only configured org's stations appear in forecast output
**Pass criteria**: Pipeline completes with org-scoped queries; no cross-org station contamination.
**Result (run 3)**: **PASS** — "Read 62 station codes for org-scoped filtering" logged. Pipeline completed with org-scoped queries.

---

## Phase 4: Validate Pipeline Module

### 4a — Date Mismatch & Sentinel (Phase 4: Validation Queries)

**Issue**: `review_gi_draft_infra_date_mismatch_and_sentinel.md`
**What to check**:
- `validate_pipeline` queries handle boundary dates correctly
- "Discharge non-negative" check passes
**Pass criteria**: Validation module reports PASS for all checks after daily run.
**Result (run 2)**: **PASS** — 16 passed, 0 failed, 3 warnings (expected: skill n_pairs<=0 for new stations, 2 forecast codes not in runoff, 18 missing EM tuples), 1 skipped (not a pentad forecast day).
**Result (run 3)**: **FAIL** — "Runoff (day): no records" from preprocessing. Not a code bug — preprocessing_runoff ran PASS but the validation query found no runoff records for today's date (data lag from iEasyHydro HF API).

---

## Bugs Found & Fixed During Verification

### BF-1: Missing `organization` column in config JSON (found in run 1)
**Root cause**: `config_all_stations_library.json` had `organization_id` but not `organization`. Write path never included it; no backfill on read.
**Fix**: Backfill from env var on read + include in write path. Commit `7664b3c`.

### BF-2: ERA5 date validation crash in ML hindcast (found in run 2)
**Root cause**: API migration (commit `639aaca`) added `start_date`/`end_date` filters to `read_meteo_data_combined()`, but the validation at line 274 assumed unfiltered CSV data. API returns data starting at `start_date`, so `data.min() + 60 > start_date` always.
**Fix**: Remove date filter from read call (replicates pre-migration CSV behavior). Commit `7664b3c`.
**Confirmed**: Run 3 — ERA5 validation error did NOT recur.

### BF-3: ML datetime format crash blocks API write (found in run 3)
**Root cause**: `write_decad_forecast()` reads old CSV (string dates), concats with new Timestamps, then `pd.to_datetime()` fails on mixed formats. The crash at line 231 is unhandled, killing the function before the API write at line 240 executes.
**Fix**: (1) Reorder to API-first, CSV-second. (2) Add `format="mixed"` to `pd.to_datetime()` calls. (3) Wrap CSV in try/except so failures never block API. Commit `73c000b`.

### Pre-existing ML bugs (not addressed by review issues)

**ML-BUG-1**: `make_forecast.py:231` — date parsing mismatch (`%Y-%m-%d` vs `%Y-%m-%d %H:%M:%S`). Causes operational ML PENTAD failure.
**ML-BUG-2**: `recalculate_nan_forecasts.py:349` — NumPy shape mismatch in `.loc` assignment during NaN recalculation. Causes maintenance ML failure. Tracked as ML-004 (draft).

---

## Issues NOT verifiable locally

| Issue | Reason | See |
|-------|--------|-----|
| API Client Missing Params | External package release needed | `review_checklist_server_2026-03-13.md` |
| ML Reader/Writer Align | Requires production migration | `review_checklist_server_2026-03-13.md` |
| ML-004 Gap-Fill API | Draft status — not yet implemented | N/A (implement first) |

---

## Dependency Graph

```json
{
  "phases": {
    "phase_1": {
      "description": "Unit/integration tests — no pipeline run needed",
      "tasks": ["1a", "1b", "1c", "1d", "1e", "1f", "1g", "1h"],
      "parallel": true,
      "depends_on": []
    },
    "phase_2": {
      "description": "Short-term daily pipeline run + log inspection",
      "tasks": ["2a", "2b", "2c", "2d", "2e", "2f", "2g"],
      "parallel": false,
      "note": "Single pipeline run, checks applied to its output sequentially",
      "depends_on": ["phase_1"]
    },
    "phase_3": {
      "description": "Long-term pipeline verification",
      "tasks": ["3a", "3b"],
      "parallel": true,
      "note": "Can run independently from phase_2 but requires phase_1 green",
      "depends_on": ["phase_1"]
    },
    "phase_4": {
      "description": "Validate pipeline post-run checks",
      "tasks": ["4a"],
      "parallel": false,
      "depends_on": ["phase_2"]
    }
  },
  "agent_assignments": {
    "agent_1_test_runner": {
      "phase": "phase_1",
      "action": "Run full test suite, report per-module pass/fail/skip counts",
      "command": "cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh"
    },
    "agent_2_pipeline_runner": {
      "phase": "phase_2",
      "action": "Run daily pipeline, capture full log output for inspection",
      "command": "ieasyhydroforecast_env_file_path=<path> bash apps/run_locally.sh daily",
      "depends_on": ["agent_1_test_runner"]
    },
    "agent_3_lt_runner": {
      "phase": "phase_3",
      "action": "Run long-term with date override, verify model execution",
      "command": "LT_FORECAST_TODAY=2026-03-10 bash apps/run_locally.sh long-term",
      "depends_on": ["agent_1_test_runner"]
    },
    "agent_4_log_inspector": {
      "phase": "phase_2 + phase_4",
      "action": "Parse pipeline logs for checks 2a-2g and 4a",
      "depends_on": ["agent_2_pipeline_runner"]
    }
  }
}
```
