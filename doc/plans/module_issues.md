# Module Issues Index

This file is an **index** of known issues. For detailed implementation plans, see the corresponding file in `issues/`.

For the full workflow, see [README.md](README.md).

---

## Issue Statuses

| Status | Meaning |
|--------|---------|
| Open | Known issue, not yet planned |
| Draft | Detailed plan in progress |
| Ready | Plan complete, ready for GitHub publication |
| In Progress | Being implemented |
| Complete | Resolved and closed |

---

## Pipeline Module (`p`)

### P-001: Marker files owned by root not cleaned up
**Status**: Open
**Priority**: Medium
**Discovered**: 2025-12-18
**File**: —

Marker files written by the Docker pipeline are owned by root and accumulate over time without cleanup.

---

## Preprocessing Runoff Module (`prepq`)

### PREPQ-001: Runoff data not updated in Docker container
**Status**: Complete
**Priority**: High
**Discovered**: 2025-12-18
**Resolved**: 2025-01-09
**File**: [`issues/gi_draft_preprunoff_operational_modes.md`](issues/gi_draft_preprunoff_operational_modes.md)

Module didn't update runoff data in Docker due to file timestamp check. Fixed by adding operational/maintenance modes. SDK limitations discovered during testing are tracked in PREPQ-003.

---

### PREPQ-002: Slow data retrieval from iEasyHydro HF
**Status**: Superseded by PREPQ-003
**Priority**: Medium
**Discovered**: 2025-01-05

Initial diagnosis was incorrect (page_size=1000 is not valid - API limit is 10). Comprehensive fix tracked in PREPQ-003.

---

### PREPQ-003: iEasyHydro HF Data Retrieval Validation
**Status**: Ready for Server Testing
**Priority**: High
**Discovered**: 2025-01-09
**File**: [`issues/gi_PR-002_data_retrieval_validation.md`](issues/gi_PR-002_data_retrieval_validation.md)
**GitHub**: —

Comprehensive data retrieval improvements including:
- SDK best practices (page_size=10 hard limit, use site_codes filter)
- Parallel pagination for performance
- Duplicate site code handling (prefer manual over automatic)
- Data validation and logging
- Site caching for operational mode
- Reliability fixes (empty DataFrame check, undefined variable, stale cache warning)

All phases complete. Needs server deployment testing before archiving.

---

### PREPQ-004: Swiss Data Source Integration & Module Refactoring
**Status**: Not Started
**Priority**: Medium
**Discovered**: 2025-01-12
**File**: [`issues/gi_PR-003_swiss_data_source_refactor.md`](issues/gi_PR-003_swiss_data_source_refactor.md)
**GitHub**: —

Add Swiss demo data as a data source while refactoring `src/src.py` (~4037 lines) into logical modules:
- Remove legacy iEasyHydro code (only HF supported going forward)
- Extract API clients, processing, I/O into separate modules
- Create abstract DataSource interface for multiple backends
- Establish pattern for future data sources (Nepal)

Blocked by: Swiss data source API documentation/access

---

## Conceptual Model Module (`cm`)

### CM-001: CI/CD builds disabled - R dependencies broken
**Status**: Open (Workaround Applied)
**Priority**: Low
**Discovered**: 2026-01-27
**File**: —

R dependency installation fails during Docker build due to upstream rocker/tidyverse changes (urllib update incompatibility). CI builds disabled in `build_test.yml`, `deploy_main.yml`, and `scheduled_security_rebuild.yml`. Existing Docker images frozen at current state.

**Workaround**: Jobs commented out in all CI workflows. Module remains functional using existing frozen images.

**To fix**: Debug and update `install_packages.R` to work with current R package ecosystem, then uncomment CI jobs.

**Note**: Module is already in maintenance-only mode and planned for phase-out. Fix is low priority unless customer demand requires it.

---

## Linear Regression Module (`lr`)

### LR-001: [Title TBD]
**Status**: Draft
**Priority**: TBD
**File**: [`issues/gi_draft_linreg_bugfix.md`](issues/gi_draft_linreg_bugfix.md)
**GitHub**: —

See detailed plan file for description.

---

## Module Abbreviations

| Module | Abbreviation |
|--------|--------------|
| conceptual_model | `cm` |
| preprocessing_runoff | `prepq` |
| preprocessing_gateway | `prepg` |
| preprocessing_station_forcing | `prepf` |
| linear_regression | `lr` |
| machine_learning | `ml` |
| postprocessing_forecasts | `pp` |
| forecast_dashboard | `fd` |
| configuration_dashboard | `cd` |
| pipeline | `p` |
| iEasyHydroForecast | `iEHF` |
| reset_forecast_run_date | `r` |
| cross-module/infrastructure | `infra` |

---

## Collection of issues while working on modules, to be planned for soonish

branch dependency_updates_Jan_26
bash run_tests.sh

module: pipeline

pipeline/.venv/lib/python3.12/site-packages/luigi/parameter.py:408
  /Users/bea/Documents/GitHub/SAPPHIRE_forecast_tools/apps/pipeline/.venv/lib/python3.12/site-packages/luigi/parameter.py:408: DeprecationWarning: datetime.datetime.utcfromtimestamp() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.fromtimestamp(timestamp, datetime.UTC).
    _UNIX_EPOCH = datetime.datetime.utcfromtimestamp(0)

pipeline/.venv/lib/python3.12/site-packages/luigi/__init__.py:87
  /Users/bea/Documents/GitHub/SAPPHIRE_forecast_tools/apps/pipeline/.venv/lib/python3.12/site-packages/luigi/__init__.py:87: DeprecationWarning: 
          Autoloading range tasks by default has been deprecated and will be removed in a future version.
          To get the behavior now add an option to luigi.cfg:
  
            [core]
              autoload_range: false
  
          Alternately set the option to true to continue with existing behaviour and suppress this warning.
      
    warnings.warn(warning_message, DeprecationWarning)

pipeline/tests/test_preprocessing.py:6
  /Users/bea/Documents/GitHub/SAPPHIRE_forecast_tools/apps/pipeline/tests/test_preprocessing.py:6: PytestCollectionWarning: cannot collect test class 'TestPreprocessingGateway' because it has a __init__ constructor (from: tests/test_preprocessing.py)
    class TestPreprocessingGateway(luigi.Task):

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html

module linear regressison
bash run_tests.sh

tests failed (1) linear_regression. No tests collected.

*Last updated: 2026-01-27*
