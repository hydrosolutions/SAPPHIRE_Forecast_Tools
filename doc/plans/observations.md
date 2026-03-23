# Observations Log

A running log of observations, warnings, and notes discovered during development and operations.
Periodically review and triage into formal issues in `module_issues.md` or GitHub Issues.

## How to Use

1. **Capture quickly** — Add observations with date and context
2. **Review periodically** — Weekly or before releases, review and triage
3. **Triage outcomes**:
   - Create formal issue in `module_issues.md` → mark as `[TRIAGED: XX-001]`
   - Not actionable → mark as `[WONTFIX]` with reason
   - Already fixed → mark as `[RESOLVED]`

---

## 2026-02-18

### Long-Term Forecasting: `code` Column Forced to int Breaks Alphanumeric Codes

**Source**: Investigation of KGZ500 snow data returning None in preprocessing_gateway
**Date**: 2026-02-18

`long_term_forecasting/data_interface.py:580` (CSV-based `load_snow_data`) has:
```python
df["code"] = df["code"].astype(int)
```
This crashes with `ValueError` for any HRU whose station codes are alphanumeric (e.g., KGZ500). The API-based version (line ~204) does NOT have this issue.

**Convention**: Station codes must always be strings throughout the pipeline. Numeric codes like `12345` should be stored as `"12345"`, not as integers. This is consistent with the API writer (`"code": str(row['code'])`) and the consistency checks (both sides cast to `str`).

**Fix needed**: Change `.astype(int)` to `.astype(str)` at line 580. Also audit any other `.astype(int)` on `code` columns in the module.

**Assessment**: Will break KGZ500 snow data ingestion in long_term_forecasting. Colleague may need to address.
**Status**: Needs fix

---

### Preprocessing Gateway: KGZ500 Snow Data Returns None Despite Code Fixes

**Source**: Running `snow_data_operational.py` with HRU KGZ500
**Date**: 2026-02-18

After fixing three bugs in `dg_utils.transform_snow_data` (station code `int()` conversion, elevation band `int()` conversion, and naive `split("_")` logic), HRU KGZ500 still returns None values while HRU 00003 works fine.

**Fixes applied** (in `dg_utils.py`):
1. `int(new_col[0])` → `str(new_col[0])` for station codes
2. Replaced `split("_")` + `int(new_col[1])` with robust `rsplit("_", 1)` + validation (suffix must be 1-14 to be treated as elevation band)

**What remains**: The Data Gateway itself may not return valid data for KGZ500, or the CSV format from the DG differs from what `transform_snow_data` expects (e.g., different number of header rows, different column naming convention). Without access to the actual DG response for KGZ500, this cannot be diagnosed further from code alone.

**Next step**: Run with verbose logging and inspect the raw DG CSV file for KGZ500 (saved in `OUTPUT_PATH_DG`) to see the actual column format and data content.
**Status**: Needs investigation with real DG data

---

## 2026-02-16

### Pipeline: Decadal Forecasts Not Run by run_locally.sh

**Source**: Observation during development
**Date**: 2026-02-16

Decadal forecasts have not been run with `apps/run_locally.sh`. Needs investigation to determine whether this is a configuration issue, a missing pipeline step, or whether decadal runs were never wired into the local execution script.

**Questions to answer**:
- Does `run_locally.sh` include decadal forecast steps at all?
- If not, was this intentional (e.g., decadal only runs in Docker/production)?
- Are there any decadal-specific modules or flags that need to be added?

**Assessment**: Potential gap in local testing coverage — if decadal forecasts aren't exercised locally, regressions could go undetected until deployment.
**Status**: Needs investigation

---

## 2026-02-13

### Linear Regression: No Pentadal Forecasts Produced in February 2026

**Source**: Production observation
**Date**: 2026-02-13

The linear regression module did not produce any forecasts for the pentadal prediction horizon in February 2026, despite data being available. Decadal forecasts appear to work correctly.

**Assessment**: Needs a proper bug report and investigation. Likely a date/period calculation or data selection issue specific to pentadal horizons. Should also create a proper integration test that verifies forecasts are produced for all prediction horizons when input data is available.
**Status**: Needs investigation and bug report

---

## 2026-02-02

### Dashboard: Stale Forecast Data and Data Gaps (Ubuntu Server)

**Source**: Ubuntu production server, pentad dashboard
**Date**: 2026-02-02
**Branch**: local branch deployed

**Observations**:
- Pentad dashboard shows forecast for **5th pentad of January** (stale - should be February)
- Data gaps for station **17082** and other sites
- Snow .env configuration was missing - ✅ Fixed, dashboard now loads

**Investigation**:
- Contacted local contact to verify which iEasyHydro HF version is operational (cloud vs local)
- Data gaps may be caused by using cloud API while local iEH HF has more recent data (or vice versa)

**Assessment**: Likely iEasyHydro HF data source mismatch. Waiting for response from local contact.
**Status**: Investigating - awaiting response on iEH HF version

---

### Dashboard: Panel/HoloViews Markdown Object Error (Ubuntu Server)

**Source**: Ubuntu production server, pentad AND decadal dashboards
**Date**: 2026-02-02
**Branch**: local branch deployed

**Error** (identical on both pentad and decadal):
```
2026-02-02 09:24:16,877 - ERROR - Error running application handler: 'Markdown' object has no attribute 'opts'
File 'holoviews.py', line 251, in _update_responsive:
opts = obj.opts.get('plot', backend=backend).kwargs

AttributeError: 'Markdown' object has no attribute 'opts'
```

**Stack trace points to**:
- `forecast_dashboard.py:2011` → `update_active_tab(None)`
- `forecast_dashboard.py:2000` → `snow_plot_panes[var].object = pn.pane.Markdown(_("No snow data from SAPPHIRE Data Gateway available."))`
- Panel's `holoviews.py:251` → `_update_responsive` tries to call `.opts.get()` on a Markdown object

**Additional context from decadal run**:
- Bulletin table creation succeeds
- ML forecasts missing: TSMixer, TiDE, TFT, NE (separate issue)
- Error occurs specifically when no snow data is available and the code tries to display a Markdown message

**Analysis**: Panel's responsive layout handler is treating a `pn.pane.Markdown` object as if it were a HoloViews object. This happens when assigning a Markdown pane to `snow_plot_panes[var].object`. The `snow_plot_panes` were likely initialized as HoloViews panes, and Panel's watcher assumes all objects assigned to `.object` will have HoloViews `.opts` attribute.

**Potential fix**: Instead of assigning `pn.pane.Markdown(...)` directly to a HoloViews pane's object, may need to wrap or handle the "no data" case differently.

**Assessment**: Panel/HoloViews type mismatch when displaying "no data" message. Affects both pentad and decadal dashboards.
**Status**: Needs investigation - likely requires code fix in `forecast_dashboard.py`

---

### Dashboard: Missing Forecast Skill Lines in Decadal Figures (Sapphire Server)

**Source**: Sapphire production server, decadal dashboard
**Date**: 2026-02-02
**Branch**: local branch deployed

**Problem**: Forecast skill lines are not displayed in figures for decadal forecasts. Pentad dashboard appears OK.

**Assessment**: Data or rendering issue specific to decadal skill metrics. Deferred to after Maxat's dashboard refactoring (maxat_sapphire_2 branch).
**Status**: DEFERRED - will reassess after dashboard refactoring merge

---

### 📋 Deployment Status Tracking

**Purpose**: Track deployment status across servers after server update plan

| Server | Type | Branch | Last Checked | Status |
|--------|------|--------|--------------|--------|
| **ubuntu** | Operational | local | 2026-02-02 | ⚠️ Dashboard error (Markdown/opts) |
| **sapphire** | Operational | local | 2026-02-02 | ⚠️ Decadal: missing forecast skill lines |
| **zurich** | Testing | main | 2026-02-02 | ✅ Both dashboards OK |

Will update this table as observations are collected.

---

## 2026-01-30

### ~~🚨 URGENT: Preprocessing Gateway Runs Twice (Fix Monday)~~ ✅ RESOLVED

**Source**: Ubuntu production server observation
**Date**: 2026-01-30
**Resolved**: 2026-02-03

**Problem**: Preprocessing gateway module now runs twice in the operational pipeline. The marker file logic that prevented unnecessary reruns within the same day appears to have been removed.

**Resolution**: Implemented `get_gateway_dependency()` helper function that checks for marker files before deciding whether to run gateway preprocessing. Verified working on Zurich test server.

**Assessment**: ~~Urgent - affects production performance~~ Fixed
**Status**: RESOLVED - See `doc/plans/archive/gi_P-002_gateway_double_run_RESOLVED_2026-02-03.md`

---

### Configuration: Review iEasyHydro HF Requirements

**Source**: Server deployment documentation
**Date**: 2026-01-30

SAPPHIRE Forecast Tools can run as a standalone tool without iEasyHydro HF connectivity. However, certain organization configurations (e.g., `kghm`, `tjhm`) require access to iEasyHydro HF for data retrieval.

**Review needed**: The `.env` configuration and the "requires access to iEasyHydro HF or not" relationship needs closer examination. Currently unclear which exact configurations require iEH HF access and which can operate standalone.

**Questions to answer**:
- Which `ieasyhydroforecast_organization` values require iEH HF access?
- What happens if iEH HF is unavailable for configured organizations?
- Can a deployment be configured to use local data only?

**Assessment**: Documentation/configuration clarity issue. Planning documents created.
**Status**: TRIAGED - See `ieasyhydro_hf_migration_plan.md` and `configuration_update_plan.md`

---

## 2026-01-29

### Pipeline: Docker Image Comparison and Download Bug

**Source**: UV migration testing, production observation
**Date**: 2026-01-29, updated 2026-02-02

Pipeline machine learning part prints "The Docker Hub image is newer than the local image." even when it is not. The image comparison logic appears to be incorrect.

**Additional observation (2026-02-02)**: Pipeline shows "image on dockerhub newer than local image, downloading image" but does not actually download any image. This is not just a message issue - the download action itself doesn't execute.

**Root cause identified**: The comparison logic was comparing incompatible dates:
- Docker Hub: `tag_last_pushed` (when tag was pushed to registry)
- Local: `attrs['Created']` (when image was built)

This caused false positives when an image was re-pushed without rebuilding.

**Fix implemented**: Replaced timestamp comparison with digest-based comparison (sha256). Digests uniquely identify image content - same digest = identical images. Added fallback to timestamp for locally-built images without RepoDigests.

**Files modified**:
- `apps/pipeline/src/pipeline_utils.py` - Added `get_docker_hub_image_digest()`, `get_local_image_digest()`, modified `there_is_a_newer_image_on_docker_hub()`
- `apps/pipeline/tests/test_pipeline_utils.py` - Added 17 new tests for digest comparison

**Status**: RESOLVED - Fix implemented on branch `fix/gi_P-002_gateway_double_run`

---

### Dashboard: Data Not Displaying After PREPQ-005 Fix

**Source**: Server testing after preprocessing_runoff fix deployment
**Date**: 2026-01-29

After deploying the PREPQ-005 fix (seasonal filtering bug in `filter_roughly_for_outliers()`), all upstream modules run successfully:
- ✅ preprocessing_runoff: March-November data now preserved in `runoff_day.csv` and `hydrograph_day.csv`
- ✅ linear_regression: Forecasts generated with continuous data
- ✅ machine_learning: ML forecasts completed successfully

However, the forecast dashboard does not display the data correctly. This is a **separate issue** from PREPQ-005 - the preprocessing data is correct but the dashboard is not rendering it.

**Assessment**: Dashboard rendering issue, not a data issue. Needs separate investigation.
**Status**: Needs investigation - new issue, not related to PREPQ-005

---

### Postprocessing: Tier 1 Bug Fixes Verified on Server

**Source**: Server testing after commit a52597d deployment
**Date**: 2026-01-29, updated 2026-02-03

Deployed Phase 1 Tier 1 bug fixes from `postprocessing_bug_fixes` branch:
- ✅ Bug 1+2 (return value masking): Error accumulation pattern working
- ✅ Bug 3 (unsafe .iloc[0]): Safe DataFrame access working
- ✅ Bug 4 (non-atomic file ops): Atomic writes working

**Postprocessing module ran without errors on production server (verified 2026-02-03).**

However, **machine learning hindcasts still failing** - this is a separate, pre-existing issue (see ML Maintenance observation from 2026-01-27 below). The postprocessing fixes do not address ML hindcast failures.

**Assessment**: Postprocessing bug fixes verified working on production.
**Status**: RESOLVED

---

## 2026-01-27

### Dashboard: Predictor Data Gaps and Missing Snow Data (Production Server)

**Source**: Testing current main on production server
**Date**: 2026-01-27

Two issues observed in dashboard:

1. **Data gaps in visualizations**: Data shows only for beginning and end of year with huge gaps in between. Gaps are filled with straight lines (linear interpolation).
   - Affects: Predictors tab, Skill metrics figures
   - Possible cause: Problem in hydrograph file?

3. **No ML forecasts for stations 16059, 15189**: ML forecasts missing for these stations.

2. ~~**No snow data displayed**~~: **RESOLVED** - Fixed by adding missing `.env` variables to test server configuration.

**Investigation findings**:

Linear regression module status:
- `linreg_last_successful_run_linreg_PENTAD.txt` and `_DECAD.txt` - datetime stamp from today ✅
- `.env` confirms: `ieasyforecast_last_successful_run_file=linreg_last_successful_run_linreg.txt`
- Note: Old files `linreg_last_successful_run_PENTAD.txt` (without `_linreg`) have timestamps from Oct 2, 2025 - these are NOT used
- Conclusion: Linear regression module ran today

**Critical finding - Data gap pattern**:
`forecast_pentad_linreg_latest.csv` for station 16059 shows:
- Data present: Jan-Feb 2025, then Nov 25 2025 - Jan 2026
- **Missing data: March 10, 2025 to November 25, 2025** (shows as empty predictor columns, flag=1.0, forecast=-1.0)

Same pattern in `forecast_decad_linreg_latest.csv`.

**Root cause identified**: `hydrograph_day.csv` shows the SAME missing data pattern (March 10 - Nov 25, 2025).

~~Previous assessment that preprocessing_runoff is working correctly is FALSE.~~

**Assessment**: Bug in preprocessing_runoff maintenance mode - producing data with large gaps. This is NOT a dashboard issue.
**Status**: RESOLVED - Fixed in PREPQ-005 (seasonal filtering bug in `filter_roughly_for_outliers()`)

---

### Pipeline: Luigi Deprecation Warnings

**Source**: `bash run_tests.sh` on branch `dependency_updates_Jan_26`

```
pipeline/.venv/lib/python3.12/site-packages/luigi/parameter.py:408
  DeprecationWarning: datetime.datetime.utcfromtimestamp() is deprecated and scheduled
  for removal in a future version. Use timezone-aware objects to represent datetimes in
  UTC: datetime.datetime.fromtimestamp(timestamp, datetime.UTC).

pipeline/.venv/lib/python3.12/site-packages/luigi/__init__.py:87
  DeprecationWarning: Autoloading range tasks by default has been deprecated and will be
  removed in a future version. To get the behavior now add an option to luigi.cfg:
    [core]
      autoload_range: false
```

**Assessment**: These are upstream Luigi warnings, not our code. Monitor for Luigi updates that fix these.

---

### Pipeline: Test Class Collection Warning

**Source**: `bash run_tests.sh`

```
pipeline/tests/test_preprocessing.py:6
  PytestCollectionWarning: cannot collect test class 'TestPreprocessingGateway' because
  it has a __init__ constructor (from: tests/test_preprocessing.py)
    class TestPreprocessingGateway(luigi.Task):
```

**Assessment**: `TestPreprocessingGateway` is a Luigi Task, not a pytest test class. The naming is confusing pytest. Consider renaming to avoid `Test` prefix, or add `# noqa` marker.

---

### Linear Regression: No Tests

**Source**: `bash run_tests.sh`

```
tests failed (1) linear_regression. No tests collected.
```

**Assessment**: `apps/linear_regression/test/test_config.py` exists but contains no test functions - only imports and path setup. Either add actual tests or remove the file to avoid false failures.

---

### Monitoring Script: Rename and Enhance preprunoff.sh

**Source**: Server testing
**Date**: 2026-01-27

Running `bin/monitoring/preprunoff.sh` on the server shows only errors in ML maintenance run (no other errors detected).

**Enhancement request**: `bin/monitoring/preprunoff.sh` should be renamed - it's actually a general log file monitor, not specific to preprunoff.

**Desired functionality**:
- Run for all recent logs of a given date
- Return a summary of errors in each log file
- Enable quick triage before detailed analysis of specific logs

**Status**: Enhancement needed

---

### ML Maintenance: Hindcast File Not Found

**Source**: `daily_ml_maintenance.sh` on testing server
**Date**: 2026-01-27

All ML maintenance jobs fail with `FileNotFoundError` when trying to read hindcast output files.

**Error pattern** (same for TFT, TIDE, TSMIXER × PENTAD, DECAD):
```
FileNotFoundError: [Errno 2] No such file or directory:
'../../../kyg_data_forecast_tools/intermediate_data/predictions/hindcast/TFT/TFT_PENTAD_hindcast_daily_2026-01-26_2026-01-27.csv'
```

**Call chain**:
1. `recalculate_nan_forecasts.py` detects missing forecasts for stations [15020, 15194, 15030, 15025, 15013]
2. Calls `hindcast_ML_models.py` via subprocess
3. Tries to read hindcast output file → **FileNotFoundError**

**Known issue**: This error occurs intermittently (every few months). Usually resolved by running ML modules locally and updating files on the server. Paths are correct - this is not a path bug.

**Workaround**: Run ML modules locally, sync updated files to server.

**Assessment**: Intermittent state/sync issue. Workaround exists but root cause unknown.
**Status**: Needs proper investigation - recurring issue is disruptive

---

## 2026-03-20

### Postprocessing Forecasts: S2 (16059) EM Decad Not Written for Recent Dates

**Source**: Local pipeline run (`run_locally.sh postprocessing_forecasts` with DECAD horizon), API validation
**Date**: 2026-03-20

After running `postprocessing_forecasts` for DECAD, station 15189 (S1) has 3 EM decad
records in the recent window (2026-03-10, 03-19, 03-20) with valid quantiles. Station
16059 (S2) has **0 EM decad records** in the same window — the last S2 EM decad record
is from 2026-03-09.

Meanwhile, S2 has individual model records (TFT, TiDE, TSMixer) and NE records for
today. Only EM is missing for S2.

**Investigation needed**:
- Why does EM computation succeed for S1 but fail silently for S2?
- Check postprocessing_forecasts logs for S2-specific errors during ensemble computation
- Could be related to LR availability (S2 LR decad exists at `/lr-forecast/` but
  postprocessing may fail to read it for S2)

**Investigation results (2026-03-20)**:

Queried skill metrics for both stations. The root cause is **insufficient qualifying
models for S2 in the current period**, not a bug in the EM writer.

The EM computation requires 2+ individual models (excl. NE) to pass all three skill
thresholds (`sdivsigma < 0.6`, `nse > 0.8`, `accuracy > 0.8`) for the same
`(period_in_year, code)` tuple. If only 0-1 models pass, the `is_multi_model_composition()`
guard in `ensemble_calculator.py:159-161` silently discards the single-model row.

**Comparison (pentad skill metrics, verified with correct API parameter)**:

| Metric | S1 (15189) | S2 (16059) |
|--------|-----------|-----------|
| Valid records | 482 | 463 |
| Passing all 3 thresholds | 411 (85%) | 209 (45%) |
| Periods with 2+ qualifying models | 64/67 | 39/46 |
| **Pentad 16 (current, Mar 16-20)** | **4 models** | **0 models** |

S2 has dramatically weaker skill metrics — almost half pass rate vs S1. S2 pentad 16
has zero qualifying individual models, so EM cannot be produced at all. Key S2 gaps:
pentads 1, 2, 4, 5, 7, 11, 12, 16-18, 20 (zero qualifying models); pentads 8, 13, 31,
52, 57, 64, 71 (single model only → EM dropped).

**Decad picture is worse**: S2 decad has only 122/319 (38%) passing, with 16/19 periods
having 2+ models. Decads 1-4, 8-10 have zero qualifying models.

NE is unaffected because it is computed unconditionally in
`sl.calculate_neural_ensemble_forecast()` *before* any skill-metric check — it simply
averages all available ML models with no threshold gate.

**Assessment**: Not a bug — working as designed. S2 lacks sufficient model skill for EM
in current period (0 qualifying models at pentad 16 vs S1's 4). The silent skip is an
observability gap (no per-station warning logged).
**Status**: Investigated — observability improvement needed (see PP-027 draft)

---

### Postprocessing Forecasts: NE Decad Produced on Non-Boundary Day (2026-03-20)

**Source**: Local pipeline run (`run_locally.sh postprocessing_forecasts` with DECAD horizon), API validation
**Date**: 2026-03-20

March 20 is not a decad boundary day (boundaries are 1st, 11th, 21st). LR correctly
wrote no new forecasts for today. However, `postprocessing_forecasts` produced NE
(norm-error) decad records dated 2026-03-19 and 2026-03-20 for both stations, and EM
decad records for S1 on those same dates.

**Investigation needed**:
- Does `postprocessing_forecasts` have its own boundary-day gate, or does it always
  produce combined forecasts whenever ML models have new output?
- If it always runs: is this by design (daily EM/NE updates using latest ML data)?
- If it should only run on boundary days: the gate is missing or broken
- Check whether these non-boundary-day records cause issues downstream (dashboard,
  skill metrics, data consumers)

**Assessment**: Possible design question or missing gate. Needs clarification on whether
combined forecasts should only be produced on boundary days.
**Status**: [TRIAGED: INFRA-006] — covered by Issue B (postprocessing writes decad on non-decad days)

---

### Postprocessing Forecasts: Skill Metrics Have model=None and n_pairs=0

**Source**: Local pipeline run, API query for decad skill metrics
**Date**: 2026-03-20

Querying decad skill metrics for both stations returns records with `model=None` and
`n_pairs=0.0`:
```
S1 decad skill: 5 records — all model=None, n_pairs=0.0
S2 decad skill: 5 records — all model=None, n_pairs=0.0
```

The validation script reported 5000 total skill metric records with 50 having
`n_pairs <= 0`.

**Investigation needed**:
- `model=None` suggests the model field was not populated during write or migration.
  Check whether the skill metric writer in `postprocessing_forecasts` sets the model
  field, or whether this is a data migration artifact.
- `n_pairs=0` could be legitimate for new stations with no historical pairs, but
  combined with `model=None` it looks like a schema or migration issue.
- Check the skill metric API response schema — is `model` a string field or a
  foreign key? Does the API serializer return `None` when the FK is null?

**Assessment**: Likely a migration or API writer bug. `model=None` is not expected.
**Status**: [TRIAGED: PP-028] — draft issue created for skill metrics writer bugs (model=None, rmse=None, decad/monthly n_pairs=0)

---

### Postprocessing Forecasts: LR Forecast Validation FAIL at /forecast/ Endpoint

**Source**: `validate_pipeline.py --module postprocessing_forecasts` (DECAD horizon)
**Date**: 2026-03-20

Validation reports `[FAIL] Forecasts (LR, decade): no records` at the `/forecast/`
endpoint. LR forecasts are stored at `/lr-forecast/` (a separate endpoint), so this
may be expected. However, it needs clarification:

**Investigation needed**:
- Does `postprocessing_forecasts` write an LR row to `/forecast/` as part of ensemble
  input, or does it only read from `/lr-forecast/`?
- If LR is not expected at `/forecast/`, the validator should be updated to check
  `/lr-forecast/` instead, or this check should be removed/adjusted.
- If LR *should* appear at `/forecast/` (e.g., as a normalized combined record),
  the postprocessing writer is not producing it.

**Assessment**: Likely a validator configuration issue (checking wrong endpoint), but
could indicate a missing write step in postprocessing.
**Status**: [TRIAGED: INFRA-006] — covered by Issue D (validation script query scope)

---

### Long-Term Forecasting: Root Cause Analysis — Missing/Null Monthly Forecasts

**Source**: Local pipeline run (`run_locally.sh long-term-operational`), API query,
code investigation, simulate_forecasts.py verification
**Date**: 2026-03-20 (observation), 2026-03-22 (investigation)

After running `long_term_forecasting` operationally (gate date 2026-03-10), both
stations (15189 and 16059) have 8 long-forecast records each. All records are dated
2026-03-10 with `horizon_type=month`, `valid_from=2026-03-01`, `valid_to=2026-03-31`.

**Three distinct problems identified:**

#### Problem 1: Six Models Write Null Records with flag=0 (Bug)

Only 2 of 8 model types produced actual forecast values:
- `LR_SM`: q=1.92 (S1), with quantiles q05=1.77 through q95=2.07
- `SM_GBT`: q=1.91 (S1), no quantiles

The other 6 models wrote skeleton records with all-null Q fields but `flag=0`.

**Root cause — unconditional flag=0**: In `run_forecast.py:272-275`:
```python
forecast = model_instance.predict_operational(today=today)
forecast = forecast.round(2)
forecast["flag"] = 0  # ← SET UNCONDITIONALLY, even when forecast is all NaN
```

There is **no NaN check** before setting flag=0. The flag indicates "valid forecast"
(0=forecast, 1=hindcast, 2=error), but NaN predictions are still marked as success.

**Root cause — NaN predictions per model type**:

| Model | model_type | Why NaN | Category |
|-------|-----------|---------|----------|
| LR_Base | linear_regression | Discharge rolling lags (30d) contain NaN when data ends March 19 → `dropna(subset=features)` removes all rows → returns `[np.nan]` | Feature gap |
| LR_SM_DT | linear_regression | Same as LR_Base + SWE lag structure also needs discharge offset | Feature gap |
| LR_SM_ROF | linear_regression | Same + ROF depends on discharge rolling means → double NaN | Feature gap |
| GBT | sciregressor | `allowable_missing_value_operational=0` (strict) → any NaN feature → basin skipped → empty prediction | Feature gap |
| SM_GBT_Norm | sciregressor | Relative scaling normalization propagates NaN through scalers → model.predict() returns NaN | Feature gap |
| SM_GBT_LR | sciregressor | Cascading failure: depends on LR_Base output (which failed) → feature set incomplete | Cascading |
| **LR_SM** | **linear_regression** | **SWE is point-in-time (not lag-dependent) → model fits with SWE + minimal features** | **Success** |
| **SM_GBT** | **sciregressor** | **Feature-rich model tolerates partial NaN in feature set** | **Success** |

**Why LR_SM succeeds**: SWE data is point-in-time, not dependent on discharge rolling
lags. Feature selection ranks SWE highest when discharge lags are missing. With
`num_features=3` and SWE being top-correlated, the model has enough valid data.

**Database write behavior**: `prepare_long_forecast_records()` (lt_utils.py:354-362)
checks `pd.notna(row.get(q_model_col))` before setting the `q` field, but **appends
the record regardless**. Result: skeleton record with `{flag: 0, q: None}` written
to the API.

**Fix needed (Bug)**:
1. Add NaN check in `run_forecast.py:275` before setting flag=0 — set flag=2 or flag=3
   when all Q values are NaN
2. Optionally: skip writing records entirely when model output is all-NaN

#### Problem 2: MC_ALD Missing from Monthly Records (Expected — cascading failure)

MC_ALD is configured in `month_1.json` under "Uncertainty" with
`model_type=UncertaintyMixture`. It depends on all 8 other models. MC_ALD has trained
weights (`final_model.ckpt`, 64KB) and `simulate_forecasts.py` confirms it produces
valid monthly output when dependencies have valid data (tested with 2024 data: 39-40
stations produced valid Q values per month).

**Root cause**: Cascading failure from Problem 1.

1. 6 of 8 dependency models produced NaN output on March 20
2. `run_forecast.py:382-387` checks dependency success before running MC_ALD:
   ```python
   deps_success = all(execution_is_success.get(dep, False) for dep in dependencies)
   ```
   Since 6/8 deps reported success=True (flag=0 was set), MC_ALD may have attempted to run.
3. `load_all_dependencies_database()` loads dependency predictions via INNER JOIN —
   with mostly-NaN dependency data, the merged features are NaN.
4. UncertaintyMixture's `dropna(subset=self.features)` removes all rows → empty
   `operational_data` → `predict_operational()` returns empty DataFrame.
5. Empty DataFrame → no records written (unlike NaN which writes skeleton records).

**Not a config issue**: MC_ALD is in all monthly mode configs (month_0 through month_9).
**Not a weight issue**: trained model weights exist.

**Assessment**: Expected behavior — MC_ALD correctly produces nothing when its inputs
are all NaN. The real fix is Problem 1 (making dependency models produce valid output
or setting flag≠0 on failure so MC_ALD's dependency check can detect failure).

#### Problem 3: MC_ALD Quarterly/Seasonal Records Origin (Historical data)

MC_ALD has valid quarterly/seasonal records despite having no new monthly records:
- Quarterly: q=2.012/6.902 (S1)
- Seasonal: q=10.59 (S1)

**Root cause**: These come from **historical monthly MC_ALD records in the database**
(migrated from earlier pipeline runs, spanning 2000–2025). The quarterly/seasonal
aggregation in `data_reader.py:2506-2562` reads monthly forecasts from the API over
a lookback window and aggregates them — it doesn't require new monthly records from
today's run.

**Assessment**: Expected behavior — postprocessing correctly aggregates whatever
monthly records exist. As the current monthly pipeline fails to produce new MC_ALD
records, the quarterly/seasonal values will gradually become stale.

#### Full Per-Model Status Table (March 20 Run)

| Model | model_type | Config mode | Monthly output | Root cause | Classification |
|-------|-----------|-------------|----------------|------------|----------------|
| LR_SM | linear_regression | month_1 | q=1.92 ✓ | SWE point-in-time feature available | Success |
| SM_GBT | sciregressor | month_1 | q=1.91 ✓ | Feature-rich, tolerates partial NaN | Success |
| LR_Base | linear_regression | month_1 | q=None, flag=0 | Discharge 30d rolling lag NaN | **Bug**: flag=0 on NaN |
| LR_SM_DT | linear_regression | month_1 | q=None, flag=0 | Discharge lag + SWE offset NaN | **Bug**: flag=0 on NaN |
| LR_SM_ROF | linear_regression | month_1 | q=None, flag=0 | Discharge lag + ROF NaN | **Bug**: flag=0 on NaN |
| GBT | sciregressor | month_1 | q=None, flag=0 | Strict NaN tolerance (0) → skip | **Bug**: flag=0 on NaN |
| SM_GBT_Norm | sciregressor | month_1 | q=None, flag=0 | Scaler propagates NaN | **Bug**: flag=0 on NaN |
| SM_GBT_LR | sciregressor | month_1 | q=None, flag=0 | Cascading: LR deps failed | **Bug**: flag=0 on NaN |
| MC_ALD | UncertaintyMixture | month_1 | Not present | Cascading: 6/8 deps NaN → empty | Expected (cascading) |

#### Issues to Create

1. **Bug (high priority)**: `run_forecast.py` sets `flag=0` unconditionally — add NaN
   check before setting flag. When `predict_operational()` returns all-NaN, set flag=2
   or flag=3. This affects downstream consumers (postprocessing, skill metrics, dashboards).

2. **Improvement (medium priority)**: `prepare_long_forecast_records()` should skip
   writing records when all Q fields are None. Currently writes misleading skeleton
   records with flag=0 and no data.

3. **Improvement (low priority)**: Investigate whether the 6 failing models can be
   made more robust to 1-day data gaps (e.g., by using March 19 data instead of
   requiring March 20 discharge). This is a model design question, not a code bug.

**Status**: [TRIAGED: LTF-003] — draft issue created for flag=0-on-null bug (item 1). Item 2 (skip skeleton records) included in LTF-003.

---

### Long-Term Forecasting: Monthly Skill Metrics All Empty

**Source**: Local pipeline run, API query after `recalculate_skill_metrics`
**Date**: 2026-03-20

Monthly skill metrics for both stations return `n_pairs=0`, `mae=None`, `nse=None`,
`model=None` — identical to the decad skill metric issue. 10 records per station,
all empty.

This is part of a broader pattern: **decad and monthly skill metrics are all empty,
while pentad skill metrics work correctly** (pentad has real n_pairs=9–17 with valid
mae/nse values).

Additionally, `model=None` affects ALL skill metrics across all horizons (pentad,
decad, monthly). Even pentad records that have valid mae/nse show `model=None`.
The `rmse` field is also `None` across all pentad records despite mae and nse being
populated.

**Investigation needed**:
- The skill metric writer does not set the `model` field. Check the API schema — is
  `model` a required field? Is it a FK to a model type table? The long-forecast
  endpoint returns `model_type` correctly (e.g., "LR_SM"), so the issue is specific
  to the skill-metric writer.
- Decad and monthly `n_pairs=0` could be caused by: (a) no forecast-observation
  pairs found for matching, (b) date/horizon filter mismatch in the recalculation
  query, or (c) the recalculation only running for pentad.
- `rmse=None` on pentad: check if RMSE is computed but not written, or not computed.

**Related issues**:
- S2 EM decad missing (this observation, above) — fewer EM records means fewer
  pairs for decad skill evaluation
- 6 of 8 long-term models null (this observation, above) — no valid forecasts
  means no pairs for monthly skill evaluation

**Assessment**: Likely multiple overlapping issues: (1) skill metric writer missing
`model` field, (2) decad/monthly recalculation not finding pairs, (3) RMSE not
computed. Priority: the `model=None` issue affects all horizons and should be fixed
first.
**Status**: [TRIAGED: PP-028] — consolidated with model=None and rmse=None into single skill metrics issue

---

## 2026-03-19

### Machine Learning: Recurring Gap-Fill Between 2024 and 2026 on Every Run

**Source**: Local pipeline run (`run_locally.sh machine_learning` with `SAPPHIRE_PREDICTION_MODE=DECAD`)
**Date**: 2026-03-19

`fill_ml_gaps.py` detects a large gap in ML forecasts between ~2024-03 and
~2026-03 and triggers `hindcast_ML_models.py` to fill it on every operational
run. This makes the ML module extremely slow (hindcasting ~2 years of data)
and should not be necessary — the gap has been "filled" in previous runs but
keeps reappearing.

**Likely causes**:
1. The hindcast rows may be written to the API but the gap detection reads from
   a different source (CSV vs API) or with a different date/code filter, so it
   never "sees" the filled data.
2. The API read in `fill_ml_gaps.py` uses a 730-day lookback
   (`_read_ml_forecasts_from_api` with `start_date = today - 730 days`). If the
   filled rows are outside this window or the API pagination doesn't return them
   all, the gap persists.
3. The CSV schema corruption (ML-009, now fixed) may have caused previous gap-fill
   writes to produce corrupted rows that are silently dropped on re-read.
4. Org-scoped filtering in `fill_ml_gaps.py` (lines 220-242) may be excluding
   hindcast rows written by a different org context.

**Impact**: Every ML run takes much longer than necessary. On the server this
blocks the entire daily pipeline for hours.

**Relation to existing issues**:
- ML-009 (CSV schema corruption) — now fixed, but previously corrupted CSVs may
  still trigger the gap detection
- `review_gi_draft_ml_hindcast_api_write_broken.md` (ML-004) — hindcast API write
  bugs were fixed, but the gap recurrence suggests data isn't persisting correctly
- `mid_prio_gi_draft_ml_hindcast_api_consistency.md` — write order was just fixed
  (API first, CSV second)

**Investigation results (2026-03-20)**:

After implementing ML-007 (per-code API reads), the pagination non-determinism
is resolved. However, the gaps persist and are now confirmed to be **real data
gaps** — not phantom artifacts from pagination.

For station 15189 TFT (730-day window): 8,041 total rows, of which **2,563 have
`forecasted_discharge=null` (flag=3)**. After excluding nulls (as `fill_ml_gaps`
does), 3 genuine gaps remain:
- 2024-09-16 to 2024-10-21 (35 days)
- 2024-12-02 to 2025-01-03 (32 days)
- 2025-08-14 to 2026-01-30 (169 days)

**Root cause**: The hindcast runs for these gap periods but produces null output
(flag=3 = NaN). On the next run, `fill_ml_gaps` excludes null-discharge rows
(correctly), sees the same gaps again, and re-triggers the hindcast — which
again produces nulls. This is an infinite loop: hindcast writes null -> gap
detection excludes nulls -> gap detected -> hindcast writes null.

**The fix is NOT in gap detection** — it's in the hindcast trigger logic. The
code should recognize that flag=3 rows already exist for those dates and not
re-trigger a hindcast that will produce the same null output.

**Related issues**:
- PP-026 (clean null-discharge phantom forecasts from DB)
- ML-007 (pagination non-determinism — now resolved)
- ML-009 (CSV schema corruption — resolved)

**Assessment**: Confirmed as a hindcast trigger logic bug, not a data read bug.
The hindcast should skip date ranges where previous runs produced flag=3 output.
**Status**: Needs draft issue — new issue for "fill_ml_gaps should not re-trigger
hindcast for periods with existing flag=3 (null) forecasts"

---

### Preprocessing Gateway: Snow SWE Data Not Updated by Operational Run

**Source**: Local pipeline run (`run_locally.sh preprocessing_gateway`), API query for stations 15189 and 16059
**Date**: 2026-03-19

After running `preprocessing_gateway` operationally, the SWE snow data in the preprocessing API only contains climatological norm records (day-of-year indexed with year-2000 dates, e.g. `2000-01-01`, `2000-01-02`). No current-year snow observations appear for 2026.

**Verification commands used**:
```bash
curl -s "http://localhost:8000/api/preprocessing/snow/?code=15189&snow_type=SWE&limit=5" | python3 -m json.tool
curl -s "http://localhost:8000/api/preprocessing/snow/?code=16059&snow_type=SWE&limit=5" | python3 -m json.tool
```

Both stations return only norm data (dates starting at 2000-01-01). Expected: current-year SWE observations for 2026 should also be present after an operational run of `snow_data_operational.py`.

**Possible causes**:
- Snow data ingestion from the Data Gateway may not be returning data for these stations (similar to the KGZ500 issue from 2026-02-18)
- The API write for snow data may not be wired up in `snow_data_operational.py`
- The SWE endpoint may only store norms, not operational observations

**Assessment**: Needs investigation. Snow data may only contain norms by design, or operational snow ingestion may be broken. Check `snow_data_operational.py` and the Data Gateway response for these stations.
**Status**: Needs investigation

---

## 2026-03-23

### `_write_ml_daily_forecast_to_api` is dead code

**Source**: ML module data flow audit
**Date**: 2026-03-23

During ML module data flow audit, found that `_write_ml_daily_forecast_to_api()` (defined at `utils_ml_forecast.py:800`) is **never called** anywhere in the machine_learning module. Zero call sites.

- The function's docstring claims it is "Used by the decad pipeline to write 11 daily predictions as day-level records for Tier 2 skill metric computation" — but `write_decad_forecast()` in `make_forecast.py` only calls `_write_ml_forecast_to_api()`, not the daily variant.
- The PP-026 null-Q50 filter (line 875) was added to this dead function, meaning the ML-side write guard from PP-026 Phase 1a is non-functional.
- **Revised understanding**: The null-Q50 filter should NOT be added to the active write function either. Null-discharge records with flag=1/2/3 are intentional ML state machine entries. See revised PP-026 plan.

**Action**: Remove `_write_ml_daily_forecast_to_api` as dead code cleanup. Low priority — it does no harm, just adds confusion.

**Related**: PP-026, ML-008b

**Assessment**: Dead code — can be removed without functional impact. The PP-026 Phase 1a null-Q50 guard was placed in this dead function and is therefore non-functional; PP-026 plan needs revision.
**Status**: Needs dead code removal (low priority) and PP-026 plan revision

---

## Template

```markdown
### [Module]: [Brief Title]

**Source**: [command/file/observation context]
**Date**: YYYY-MM-DD

[Description or error output]

**Assessment**: [Initial thoughts on cause/fix/priority]
**Status**: [blank | TRIAGED: XX-001 | WONTFIX: reason | RESOLVED]
```

---

## Triaged Items

| Date | Observation | Outcome |
|------|-------------|---------|
| 2026-01-30 | Configuration: Review iEasyHydro HF Requirements | `ieasyhydro_hf_migration_plan.md`, `configuration_update_plan.md` |

---

*Last updated: 2026-03-23 (triaged 5 observations: PP-028 skill metrics, LTF-003 flag-zero-on-null, INFRA-006 NE boundary + LR validator)*
