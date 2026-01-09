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
**Status**: In Progress
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

*Last updated: 2025-01-09*
