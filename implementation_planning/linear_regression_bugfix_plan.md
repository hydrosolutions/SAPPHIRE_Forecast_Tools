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

# Part 2: Hindcast Mode Feature - PENDING

## Current Architecture

The linear regression module currently operates in **forecast mode** only:

```
linear_regression.py
├── main()
│   ├── load_environment()
│   ├── define_run_dates()  → returns (forecast_date, date_end, bulletin_date)
│   │   └── date_start = last_successful_run_date + 1 day
│   │   └── date_end = today
│   ├── get_pentadal_and_decadal_data()
│   ├── write_hydrograph_data()
│   └── LOOP: current_day = forecast_date to date_end
│       ├── filter_discharge_data()
│       ├── perform_linear_regression()
│       ├── perform_forecast()
│       ├── write_linreg_forecast_data()
│       └── store_last_successful_run_date()
```

**Current behavior:**
- Reads `last_successful_run_date` from file
- Runs from `last_successful_run_date + 1` to `today`
- Updates `last_successful_run_date` after each successful day

## Proposed Modes

### 1. Forecast Mode (current, default)
- Runs daily forecasts from last run to today
- Updates last_successful_run_date
- Appends to existing output files

### 2. Hindcast Mode (new)
- Recalculates forecasts for a historical period
- Does NOT update last_successful_run_date
- Overwrites or creates separate output files
- Useful for:
  - Validating model performance
  - Regenerating historical data after bug fixes
  - Filling gaps in historical forecasts

## Implementation Options

### Option A: Command-line argument (RECOMMENDED)
```bash
# Forecast mode (default)
python linear_regression.py

# Hindcast mode with date range
python linear_regression.py --mode hindcast --start 2020-01-01 --end 2024-12-31
```

### Option B: Environment variable
```bash
SAPPHIRE_RUN_MODE=hindcast \
SAPPHIRE_HINDCAST_START=2020-01-01 \
SAPPHIRE_HINDCAST_END=2024-12-31 \
python linear_regression.py
```

### Option C: Separate script
```bash
python linear_regression_hindcast.py --start 2020-01-01 --end 2024-12-31
```

**Decision:** Option A (command-line arguments) - cleanest and most explicit.

**Output files:** Same files as forecast mode. The output keeps one row per (gauge, date) combination - if a forecast for the same gauge and date already exists, it gets overwritten with the latest. This works for hindcast too (regenerating historical forecasts will update those rows).

## Key Changes Required

### 1. Add argument parsing to linear_regression.py
```python
import argparse

def parse_args():
    parser = argparse.ArgumentParser(description='Linear regression forecast tool')
    parser.add_argument('--mode', choices=['forecast', 'hindcast'], default='forecast',
                        help='Run mode: forecast (default) or hindcast')
    parser.add_argument('--start', type=str, help='Hindcast start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, help='Hindcast end date (YYYY-MM-DD)')
    parser.add_argument('--output-suffix', type=str, default='_hindcast',
                        help='Suffix for hindcast output files')
    return parser.parse_args()
```

### 2. Modify define_run_dates() in setup_library.py
```python
def define_run_dates(prediction_mode='BOTH', run_mode='forecast',
                     hindcast_start=None, hindcast_end=None):
    if run_mode == 'hindcast':
        if not hindcast_start or not hindcast_end:
            raise ValueError("Hindcast mode requires --start and --end dates")
        date_start = dt.datetime.strptime(hindcast_start, '%Y-%m-%d').date()
        date_end = dt.datetime.strptime(hindcast_end, '%Y-%m-%d').date()
        bulletin_date = date_start + dt.timedelta(days=1)
        return date_start, date_end, bulletin_date
    else:
        # Existing forecast mode logic
        ...
```

### 3. Skip last_successful_run_date updates in hindcast mode
```python
if run_mode == 'forecast':
    sl.store_last_successful_run_date(current_day, prediction_mode=prediction_mode)
# In hindcast mode, don't update the last run date
```

### 4. Handle define_run_dates() inside the while loop

**Important:** In `linear_regression.py` line 183, `define_run_dates()` is called AGAIN inside the while loop:
```python
current_date, date_end, bulletin_date = sl.define_run_dates(prediction_mode=prediction_mode)
```

This will **override** the hindcast start/end dates with the normal forecast dates. In hindcast mode, this re-initialization must be skipped or handled differently:

```python
# In hindcast mode, don't re-call define_run_dates() inside the loop
if run_mode == 'forecast':
    current_date, date_end, bulletin_date = sl.define_run_dates(prediction_mode=prediction_mode)
# In hindcast mode, current_date is already set by the loop iteration
```

### 5. Separate output files for hindcast (optional)
```python
if run_mode == 'hindcast':
    output_file = f"forecast_pentad_linreg{output_suffix}.csv"
else:
    output_file = "forecast_pentad_linreg.csv"
```

## Hindcast Mode Considerations

1. **Data availability:** Hindcast can only run for dates where historical discharge data exists

2. **Output handling options:**
   - Overwrite existing files (dangerous)
   - Create separate hindcast files (recommended)
   - Append with a "hindcast" flag column

3. **Performance:** Running hindcast for many years could be slow. Consider:
   - Progress logging
   - Batch processing by year
   - Parallel processing (future enhancement)

4. **Validation:** Hindcast results can be compared against actual observations to calculate skill metrics

## Files to Modify for Hindcast Mode

| File | Change |
|------|--------|
| `apps/linear_regression/linear_regression.py` | Add argparse, pass mode to functions |
| `apps/iEasyHydroForecast/setup_library.py` | Modify `define_run_dates()` |
| `apps/iEasyHydroForecast/forecast_library.py` | Optional: separate output files |

---

## Implementation Priority

1. ~~**First:** Fix the leap year and date bugs (Part 1) + comprehensive testing~~ **COMPLETED**
2. **Second:** Add hindcast mode (Part 2) - **PENDING**

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
- [ ] Run hindcast mode to regenerate historical forecasts (PENDING - Part 2)
