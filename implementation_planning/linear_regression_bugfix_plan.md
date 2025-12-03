# Linear Regression Module Improvement Plan

## Overview

This plan covers:
1. **Bug fixes** for date handling in hydrograph output files
2. **New feature:** Add hindcast mode for recalculating historical forecasts

---

# Part 1: Bug Fixes - COMPLETED

## Summary of Issues Found and Fixed

The exploration identified **3 critical bugs** in the linear regression module related to date handling in hydrograph output files. All have been fixed.

---

## Issue 1: Leap Year day_of_year Alignment Bug - FIXED

**Location:** `apps/iEasyHydroForecast/forecast_library.py` lines 3203-3227 (pentad) and 3369-3393 (decad)

**Problem:** The original code only handled one direction of leap year mismatch:
```python
# OLD CODE - only handled non-leap current, leap last
if not is_leap_year(current_year):
    data = data[~((data['date'].dt.month == 2) & (data['date'].dt.day == 29))]
    data.loc[(data['date'].dt.month > 2), 'day_of_year'] -= 1
```

**Fix implemented:** Bidirectional leap year alignment that handles both directions:
- **Case 1:** Current=non-leap (2025), Last=leap (2024) → Map Feb 29→Feb 28, subtract 1 from day_of_year for Mar+
- **Case 2:** Current=leap (2024), Last=non-leap (2023) → Add 1 to day_of_year for Mar+
- **Case 3:** Both same type → No adjustment needed

**Key insight:** Feb 29 data is NO LONGER DROPPED - it's mapped to Feb 28 so it contributes to statistics.

---

## Issue 2: Last Year Date Reconstruction Bug - FIXED

**Location:** `apps/iEasyHydroForecast/forecast_library.py` lines 3283-3291 (pentad) and 3476-3483 (decad)

**Problem:** Last year's data was reconstructed using day_of_year arithmetic which fails at year boundaries:
```python
# OLD CODE - fails for leap year Dec 31 (day 366)
last_year_data['date'] = pd.Timestamp(str(current_year)) + pd.to_timedelta(day_of_year - 1, unit='D')
# 2024 Dec 31 (day 366) → 2025 + 365 = Jan 1, 2026 (WRONG!)
```

**Fix implemented:** Use `pd.DateOffset(years=1)` which correctly handles leap years:
```python
# NEW CODE
last_year_data['date'] = last_year_data['date'] + pd.DateOffset(years=1)
# Feb 29 → Feb 28 handled explicitly if needed
```

---

## Issue 3: Missing Date/Pentad/Decad/day_of_year Columns - FIXED

**Location:** `apps/iEasyHydroForecast/forecast_library.py` lines 3354-3387 (pentad) and 3716-3753 (decad)

**Problem:** When no discharge data exists for a station/period, the `date`, `pentad`, `decad`, and `day_of_year` columns were empty.

**Fix implemented:** Added helper functions to reconstruct these values from `pentad_in_year` / `decad_in_year`:

### New Helper Functions Added (lines 3046-3187):
- `get_issue_date_from_pentad(pentad_in_year, year)` - Returns issue date (last day of previous pentad)
- `get_issue_date_from_decad(decad_in_year, year)` - Returns issue date (last day of previous decad)
- `get_day_of_year_from_pentad(pentad_in_year, year)` - Returns day_of_year for the issue date
- `get_day_of_year_from_decad(decad_in_year, year)` - Returns day_of_year for the issue date
- `get_pentad_from_pentad_in_year(pentad_in_year)` - Returns pentad (1-6) within month
- `get_decad_from_decad_in_year(decad_in_year)` - Returns decad (1-3) within month

### Columns now always populated:
| Column | Pentad Function | Decad Function |
|--------|-----------------|----------------|
| `date` | Filled from `pentad_in_year` | Filled from `decad_in_year` |
| `pentad` (1-6) | Filled from `pentad_in_year` | N/A |
| `decad` (1-3) | N/A | Filled from `decad_in_year` |
| `day_of_year` | Filled from `pentad_in_year` | Filled from `decad_in_year` |

### Integer casting enforced:
- `pentad_in_year`, `pentad`, `day_of_year` → `int` (pentad function)
- `decad_in_year`, `decad`, `day_of_year` → `int` (decad function)

---

## Test Results

### Unit Tests Created: `apps/iEasyHydroForecast/tests/test_leap_year_handling.py`

**20 tests, all passing:**

| Test Class | Tests | Status |
|------------|-------|--------|
| `TestLeapYearAlignment` | 4 tests | ✅ PASS |
| `TestDateReconstruction` | 4 tests | ✅ PASS |
| `TestFeb29Handling` | 2 tests | ✅ PASS |
| `TestIssueDateReconstruction` | 6 tests | ✅ PASS |
| `TestIntegrationScenarios` | 1 test | ✅ PASS |
| `TestHydrographDataOutput` | 3 tests | ✅ PASS |

### Existing Tests: `apps/iEasyHydroForecast/tests/test_forecast_library.py`
- **108 tests passing** (no regressions)

---

## Files Modified

| File | Changes |
|------|---------|
| `apps/iEasyHydroForecast/forecast_library.py` | All bug fixes implemented |
| `apps/iEasyHydroForecast/tests/test_leap_year_handling.py` | New test file created |
| `apps/iEasyHydroForecast/tests/test_forecast_library.py` | Fixed path issue in existing test |

---

## Implementation Checklist - COMPLETED

- [x] **Fix 1** - Bidirectional leap year day_of_year alignment
- [x] **Fix 2** - Use DateOffset for last year date reconstruction
- [x] **Fix 3** - Reconstruct missing dates from pentad_in_year/decad_in_year
- [x] **Fix 4** - Reconstruct missing day_of_year from pentad_in_year/decad_in_year
- [x] **Fix 5** - Reconstruct missing pentad (1-6) from pentad_in_year
- [x] **Fix 6** - Reconstruct missing decad (1-3) from decad_in_year
- [x] **Fix 7** - Ensure pentad, decad, day_of_year are integers (not floats)
- [x] **Test with unit tests** - 20 new tests, all passing
- [x] **Verify no regressions** - 108 existing tests still pass

---

# Part 2: Hindcast Mode Feature - COMPLETED

## Overview

Added command-line interface to run the linear regression module in **hindcast mode**, allowing recalculation of historical forecasts without affecting the operational forecast state.

## Usage

```bash
# Normal forecast mode (default) - runs from last successful run to today
python linear_regression.py

# Hindcast mode with explicit date range
python linear_regression.py --hindcast --start-date 2024-01-01 --end-date 2024-12-31

# Hindcast with short flags
python linear_regression.py -H -s 2024-01-01 -e 2024-12-31

# Hindcast with auto-detection (finds last forecast date from output files)
python linear_regression.py --hindcast

# Hindcast from specific start date to yesterday
python linear_regression.py --hindcast --start-date 2024-01-01
```

## Features Implemented

### 1. Command-Line Arguments (`parse_arguments()`)

| Argument | Short | Description |
|----------|-------|-------------|
| `--hindcast` | `-H` | Enable hindcast mode |
| `--start-date` | `-s` | Start date (YYYY-MM-DD), auto-detected if not provided |
| `--end-date` | `-e` | End date (YYYY-MM-DD), defaults to yesterday |

### 2. Auto-Detection of Start Date (`get_hindcast_start_date_from_output()`)

When `--hindcast` is used without `--start-date`, the algorithm:
1. Gets the list of gauges from iEasyHydro HF
2. Reads existing forecast output files (`forecast_pentad_linreg.csv`, `forecast_decad_linreg.csv`)
3. Finds the latest forecast date **per gauge** in each file
4. Compares gauge list from iEH HF to gauges in output files
5. **If new gauges are detected** (no forecast history): uses `ieasyhydroforecast_START_DATE` from .env
6. **If all gauges have history**: uses the earliest of the latest dates + 1 day
7. This ensures all gauges (including new ones) are covered from their appropriate start dates

### 3. Environment Variables for Hindcast

| Variable | Description | Example |
|----------|-------------|---------|
| `ieasyhydroforecast_START_DATE` | Default start date for new gauges without forecast history | `2000-01-01` |
| `ieasyhydroforecast_END_DATE` | (Not used by hindcast - uses `--end-date` or yesterday) | `2024-05-08` |

### 4. Hindcast Mode Behavior

- **Does NOT update `last_successful_run_date`** - operational state is preserved
- **Writes to same output files** as forecast mode (one row per gauge/date, updates existing rows)
- **Skips `define_run_dates()` re-call** inside the while loop to preserve hindcast date range
- **Detects new gauges** and uses `ieasyhydroforecast_START_DATE` for historical hindcast
- **Gracefully exits** if all forecasts are up to date (start > end)
- **Full logging** of hindcast mode status and date ranges

### 5. New Helper Functions

| Function | Description |
|----------|-------------|
| `get_last_forecast_dates_per_gauge()` | Returns dict mapping gauge code → last forecast date |
| `get_hindcast_start_date_from_output()` | Auto-detects start date, handles new gauges |

## Implementation Details

### Changes to `linear_regression.py`

1. **Added `import argparse`** at top of file

2. **Added `parse_arguments()` function** (lines 74-155):
   - Parses `--hindcast`, `--start-date`, `--end-date` arguments
   - Validates date formats and ranges
   - Defaults end date to yesterday if not specified

3. **Added `get_last_forecast_dates_per_gauge()` function** (lines 158-208):
   - Reads forecast output files
   - Groups by gauge code
   - Returns dict of {code: last_forecast_date}

4. **Added `get_hindcast_start_date_from_output()` function** (lines 211-280):
   - Gets gauge list from iEH HF
   - Compares to gauges in output files
   - Detects new gauges without forecast history
   - Uses `ieasyhydroforecast_START_DATE` for new gauges
   - Returns appropriate start date

5. **Reorganized `main()` function**:
   - Gets site list BEFORE determining dates (needed for new gauge detection)
   - Uses hindcast dates when `args.hindcast` is True
   - Skips `define_run_dates()` inside loop in hindcast mode
   - Skips `store_last_successful_run_date()` in hindcast mode
   - Exits gracefully if nothing to do (start > end)

## Docker Compatibility

The existing Dockerfile uses `CMD`, so arguments can be passed via `docker run`:

```bash
docker run <image> sh -c "PYTHONPATH=/app/apps/iEasyHydroForecast python apps/linear_regression/linear_regression.py --hindcast -s 2024-01-01"
```

## Forecast Day Handling

Forecasts are only produced on specific days:
- **PENTAD mode:** Days 5, 10, 15, 20, 25, and last day of each month (6 days/month, ~72/year)
- **DECAD mode:** Days 10, 20, and last day of each month (3 days/month, ~36/year)
- **BOTH mode:** Same as PENTAD (pentad days include decad days)

In hindcast mode, the algorithm automatically:
1. Snaps the start date to the next forecast day
2. Iterates only through forecast days (not every calendar day)
3. Logs how many forecast days will be processed

This makes hindcast mode efficient - processing a full year only requires ~72 iterations instead of 365.

## Nightly Hindcast Mode

Running `python linear_regression.py --hindcast` every night will:
1. Check for new gauges added to iEH HF → run full hindcast from `ieasyhydroforecast_START_DATE`
2. Check existing gauges → run from last forecast + 1 day to yesterday
3. Auto-skip to next forecast day if start date is not a forecast day
4. If all forecasts are current → exit gracefully with "Nothing to do"

This makes it safe to run hindcast mode nightly as a "catch-up" mechanism.

## Files Modified

| File | Change |
|------|--------|
| `apps/linear_regression/linear_regression.py` | Added argparse, hindcast mode logic, new gauge detection |

---

## Implementation Priority

1. ~~**First:** Fix the leap year and date bugs (Part 1) + comprehensive testing~~ **COMPLETED**
2. ~~**Second:** Add hindcast mode (Part 2)~~ **COMPLETED**

---

# Part 3: Testing Summary

## Tests Created

### New Test File: `apps/iEasyHydroForecast/tests/test_leap_year_handling.py`

| Test Class | Test Name | Description |
|------------|-----------|-------------|
| `TestLeapYearAlignment` | `test_is_leap_year_helper` | Verify is_leap_year function |
| `TestLeapYearAlignment` | `test_day_of_year_nonleap_current_leap_last` | 2025 vs 2024 alignment |
| `TestLeapYearAlignment` | `test_day_of_year_leap_current_nonleap_last` | 2024 vs 2023 alignment |
| `TestLeapYearAlignment` | `test_same_type_years_no_adjustment` | No adjustment for same type |
| `TestDateReconstruction` | `test_date_offset_leap_to_nonleap` | DateOffset from 2024→2025 |
| `TestDateReconstruction` | `test_date_offset_nonleap_to_leap` | DateOffset from 2023→2024 |
| `TestDateReconstruction` | `test_old_day_of_year_method_fails_at_year_end` | Demonstrates old bug |
| `TestDateReconstruction` | `test_new_date_offset_method_works` | New method works correctly |
| `TestFeb29Handling` | `test_feb29_not_dropped` | Feb 29 data preserved |
| `TestFeb29Handling` | `test_feb29_contributes_to_decad7_statistics` | Feb 29 in statistics |
| `TestIssueDateReconstruction` | `test_get_issue_date_from_pentad` | Pentad issue dates |
| `TestIssueDateReconstruction` | `test_get_issue_date_from_decad` | Decad issue dates |
| `TestIssueDateReconstruction` | `test_get_day_of_year_from_pentad` | Day of year from pentad |
| `TestIssueDateReconstruction` | `test_get_day_of_year_from_decad` | Day of year from decad |
| `TestIssueDateReconstruction` | `test_get_pentad_from_pentad_in_year` | Pentad (1-6) from pentad_in_year |
| `TestIssueDateReconstruction` | `test_get_decad_from_decad_in_year` | Decad (1-3) from decad_in_year |
| `TestIntegrationScenarios` | `test_scenario_2025_with_2024_data` | Full integration test |
| `TestHydrographDataOutput` | `test_pentad_all_dates_and_day_of_year_filled` | All pentad columns filled |
| `TestHydrographDataOutput` | `test_decad_all_dates_and_day_of_year_filled` | All decad columns filled |
| `TestHydrographDataOutput` | `test_decad_7_leap_year` | Decad 7 in leap year |

## Test Results Summary

```
======================== 20 passed, 1 warning =========================
```

All bug fixes verified with comprehensive unit tests.

---

## Manual Verification Checklist

After implementing fixes, verify with real data:

- [x] `hydrograph_decad.csv` has dates for all 36 decads per station
- [x] `hydrograph_pentad.csv` has dates for all 72 pentads per station
- [x] All `date` columns are populated (no empty values)
- [x] All `pentad` columns are populated as integers
- [x] All `decad` columns are populated as integers
- [x] All `day_of_year` columns are populated as integers
- [x] Station 15013: decad 7 has date = Feb 28 (non-leap year issue date)
- [x] Station 15016: decad 7 has date = Feb 28 (non-leap year issue date)
- [x] Station 15020: all decads have dates, missing discharge shows as NaN
- [x] Run hindcast mode to regenerate historical forecasts (COMPLETED - Part 2)
