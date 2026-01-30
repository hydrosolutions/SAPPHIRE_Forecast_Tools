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

## 2026-01-30

### 🚨 URGENT: Preprocessing Gateway Runs Twice (Fix Monday)

**Source**: Ubuntu production server observation
**Date**: 2026-01-30

**Problem**: Preprocessing gateway module now runs twice in the operational pipeline. The marker file logic that prevented unnecessary reruns within the same day appears to have been removed.

**Impact**:
- Slows down operational forecasting pipeline unnecessarily
- Gateway preprocessing is expensive - should only run once per day

**Context from investigation**:
- Recent commit `a357c62` (2026-01-26) removed marker file checks to "always run fresh preprocessing"
- This was done for `preprocessing_runoff` but may have affected gateway logic too
- The rationale was that preprocessing is now fast enough to run every time - but this may not apply to gateway

**Action needed Monday**:
1. Investigate which commit removed the gateway marker file check
2. Restore marker file logic for `PreprocessingGatewayQuantileMapping` to prevent same-day reruns
3. Test on staging before deploying to production

**Files to check**:
- `apps/pipeline/pipeline_docker.py` - `PreprocessingGatewayQuantileMapping` class
- `ExternalPreprocessingGateway` task and its marker file logic
- Recent git history for marker file changes

**Assessment**: Urgent - affects production performance
**Status**: TO FIX MONDAY

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

### Pipeline: Incorrect Docker Image Comparison Message

**Source**: UV migration testing
**Date**: 2026-01-29

Pipeline machine learning part prints "The Docker Hub image is newer than the local image." even when it is not. The image comparison logic appears to be incorrect.

**Assessment**: Low priority cosmetic/logging issue. Does not affect functionality.
**Status**: Address when debugging or refactoring pipeline module

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
**Date**: 2026-01-29

Deployed Phase 1 Tier 1 bug fixes from `postprocessing_bug_fixes` branch:
- ✅ Bug 1+2 (return value masking): Error accumulation pattern working
- ✅ Bug 3 (unsafe .iloc[0]): Safe DataFrame access working
- ✅ Bug 4 (non-atomic file ops): Atomic writes working

**Postprocessing module ran without errors on the test server.**

However, **machine learning hindcasts still failing** - this is a separate, pre-existing issue (see ML Maintenance observation from 2026-01-27 below). The postprocessing fixes do not address ML hindcast failures.

**Assessment**: Postprocessing bug fixes verified working. ML hindcast issue is unrelated and tracked separately.
**Status**: RESOLVED (postprocessing fixes) - ML issue remains open

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

*Last updated: 2026-01-30*
