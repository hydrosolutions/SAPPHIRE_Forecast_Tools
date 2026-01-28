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

## 2026-01-27

### Dashboard: Predictor Data Gaps and Missing Snow Data (Production Server)

**Source**: Testing current main on production server
**Date**: 2026-01-27

Two issues observed in dashboard:

1. **Data gaps in visualizations**: Data shows only for beginning and end of year with huge gaps in between. Gaps are filled with straight lines (linear interpolation).
   - Affects: Predictors tab, Skill metrics figures
   - Possible cause: Problem in hydrograph file?

3. **No ML forecasts for stations 16059, 15189**: ML forecasts missing for these stations.

2. **No snow data displayed**: Snow data not showing for any station in the dashboard.
   - ~~Check preprocessing_gateway logs for errors~~ ✅ No errors
   - ~~Check if snow data exists in intermediate files~~ ✅ Data exists: `intermediate_data/snow_data/SWE/00003_SWE.csv`
   - Issue is in dashboard - either reading the snow files or rendering the visualization

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
**Status**: TRIAGED - see PREPQ-005

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
| — | — | — |

---

*Last updated: 2026-01-27*
