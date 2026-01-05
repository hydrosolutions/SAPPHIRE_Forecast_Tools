# Module Issues Tracker

This file tracks known issues discovered during py312 migration testing.

---

## Pipeline Module

### Issue P-001: Marker files owned by root not cleaned up
**Status**: Open
**Priority**: Medium
**Discovered**: 2025-12-18

**Description**:
Marker files written by the Docker pipeline are owned by root and are not removed automatically. These files accumulate over time.

**Location**: `apps/pipeline/pipeline_docker.py:272` (marker file handling)

**Expected behavior**:
Marker files should be cleaned up after a configurable number of days (e.g., 7 days).

**Proposed solutions**:
1. Add a cleanup task to the pipeline that removes marker files older than N days
2. Use a cron job on the server to clean up old marker files
3. Change file ownership when creating marker files (may require running as non-root)

**Notes**:
- Marker files are used to track pipeline execution state
- Root ownership prevents non-root processes from deleting them

---

## Preprocessing Runoff Module

### Issue PR-001: Runoff data not updated in Docker container
**Status**: Solution Designed
**Priority**: High
**Discovered**: 2025-12-18
**Updated**: 2025-12-18

**Description**:
The preprocessing runoff module does not update runoff data when running in the Docker container on the server, but works correctly when run locally on laptop.

**Symptoms**:
- Module runs without errors
- Output files are not updated with new data
- Log shows: "No changes in the daily_discharge directory, using previous data."
- Works correctly in local development environment

**Root Cause**:
The `should_reprocess_input_files()` function in `apps/preprocessing_runoff/src/src.py:135-163` only checks if **local Excel/CSV input files** have been modified. It does NOT check whether new data is available in the iEasyHydro HF database.

When `should_reprocess_input_files()` returns `False`:
- Code reads from cached output file (lines 2050-2086)
- Database fetch still runs but only from `latest_date + 1` onwards
- In Docker with stable file timestamps, function always returns `False`

---

**Proposed Solution: Two Operating Modes**

The module should support two distinct operating modes:

#### Mode 1: Operational Mode (Default, Daily Runs)
**Purpose**: Fast daily updates for operational forecasting
**Behavior**:
- Fetch only yesterday's daily average runoff (`WDDA` variable)
- Fetch today's morning runoff (`WDD` variable)
- Append to existing data (no full reprocessing)
- Skip Excel file reading entirely

**When to use**: Daily automated pipeline runs

#### Mode 2: Maintenance Mode (Weekly/On-Demand)
**Purpose**: Gap filling and data quality updates
**Behavior**:
- Re-read Excel/CSV input files if modified
- Fetch last 30 days of operational data from iEasyHydro HF
- Update/replace values if database values differ from cached values
- Fill any gaps in the time series

**When to use**:
- Weekly scheduled maintenance
- After data corrections in iEasyHydro
- Manual trigger when data issues suspected

---

**Implementation Plan**

#### Step 1: Add Module Configuration File

Create `apps/preprocessing_runoff/config.yaml`:
```yaml
# Configuration for the preprocessing_runoff module

maintenance:
  lookback_days: 30  # Number of days to fetch during maintenance mode

operational:
  fetch_yesterday: true   # Fetch yesterday's daily average
  fetch_morning: true     # Fetch today's morning discharge
```

**Why inside the module**: The config travels with the module - important when not all modules are used in every deployment.

#### Step 2: Add Config Loading

Create `apps/preprocessing_runoff/src/config.py`:
- Load YAML from module directory (`../config.yaml` relative to src)
- Provide sensible defaults if file missing
- Allow environment variable overrides (e.g., `PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS`)

#### Step 3: Add Mode Parameter

Support both CLI argument and environment variable:

```bash
# CLI argument (preferred)
python preprocessing_runoff.py --maintenance

# Environment variable (for Docker/pipeline)
PREPROCESSING_MODE=maintenance python preprocessing_runoff.py
```

**Mode triggers**:
- **Operational**: Default (no flag needed)
- **Maintenance**: `--maintenance` flag or `PREPROCESSING_MODE=maintenance`
  - Nightly cron job
  - Manual trigger from forecast dashboard

**Note**: Using `--maintenance` for clarity. Linear_regression uses `--hindcast` but should be updated to accept `--maintenance` for consistency (future task).

#### Step 4: Refactor Data Fetching

Modify `get_runoff_data_for_sites_HF()`:
- **Operational mode**: Skip `should_reprocess_input_files()`, fetch only yesterday's WDDA + today's morning WDD
- **Maintenance mode**: Check for input file changes, fetch configurable lookback window, update differing values

#### Step 5: Update Pipeline Orchestration

- Add maintenance scheduling option to `pipeline_docker.py`
- Support dashboard-triggered maintenance runs

---

**Files to create**:
- `apps/preprocessing_runoff/config.yaml` - Module configuration
- `apps/preprocessing_runoff/src/config.py` - Config loader

**Files to modify**:
- `apps/preprocessing_runoff/src/src.py`:
  - `should_reprocess_input_files()` - Only used in maintenance mode
  - `get_runoff_data_for_sites_HF()` - Add mode logic
  - Add new function for operational mode data fetch
- `apps/preprocessing_runoff/preprocessing_runoff.py` - Handle mode parameter, load config
- `apps/preprocessing_runoff/Dockerfile.py312` - Ensure config.yaml is copied
- `apps/pipeline/pipeline_docker.py` - Add maintenance schedule option
- `apps/forecast_dashboard/` - Add manual maintenance trigger button (future)

**Testing**:
1. Unit tests for config loading with defaults
2. Unit tests for both modes
3. Test operational mode fetches only latest data
4. Test maintenance mode fills gaps and updates values
5. Test Docker runs with both modes
6. Verify speed improvement in operational mode

**Performance Expectations**:
- Operational mode: ~5-10 seconds (minimal DB queries)
- Maintenance mode: ~30-60 seconds (full lookback fetch + Excel read if needed)

---

### Issue PR-002: Slow data retrieval from iEasyHydro HF
**Status**: ✅ Resolved
**Priority**: Medium
**Discovered**: 2025-01-05
**Resolved**: 2025-01-05

**Description**:
Data retrieval from the Kyrgyz Hydromet iEasyHydro HF server was extremely slow.

**Root cause**:
The SDK's default `page_size` (~100 records) caused excessive pagination - 11 API round-trips per request.

**Solution**:
Added configurable `page_size=1000` in `config.yaml`, reducing pagination to 1 page per request.

**Files modified**:
- `apps/preprocessing_runoff/config.yaml` - Added `api.page_size: 1000`
- `apps/preprocessing_runoff/src/config.py` - Added `get_api_page_size()` getter
- `apps/preprocessing_runoff/src/src.py` - Updated WDDA and WDD filters

**Results**:

| Mode | Before | After | Improvement |
|------|--------|-------|-------------|
| Operational | ~100s | ~1s | 99% faster |
| Maintenance | ~850s | ~1.6s | 99.8% faster |

---

## Issue Template

### Issue XX-NNN: [Brief title]
**Status**: Open | In Progress | Resolved | Won't Fix
**Priority**: Critical | High | Medium | Low
**Discovered**: YYYY-MM-DD

**Description**:
[Detailed description of the issue]

**Expected behavior**:
[What should happen]

**Actual behavior**:
[What actually happens]

**Steps to reproduce**:
1.
2.
3.

**Investigation**:
- [ ] Step 1
- [ ] Step 2

**Resolution**:
[How it was fixed, if resolved]

---

*Last updated: 2025-01-05*
