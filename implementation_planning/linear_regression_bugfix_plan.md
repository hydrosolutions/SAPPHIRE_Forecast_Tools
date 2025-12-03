# Linear Regression Module Improvement Plan

## Overview

This plan covers:
1. **Bug fixes** for date handling in hydrograph output files
2. **New feature:** Add hindcast mode for recalculating historical forecasts

---

# Part 1: Bug Fixes

## Summary of Issues Found

The exploration identified **3 critical bugs** in the linear regression module related to date handling in hydrograph output files.

---

## Evidence from Output Files

From `hydrograph_decad.csv`:
```
Station 15013:
- decad_in_year 1: date=2025-12-31 → CORRECT (issue date for decad 1 of 2026)
- decad_in_year 7: date=EMPTY, 2024=EMPTY (Feb 21-28 missing) → BUG

Station 15016:
- decad_in_year 7: date=EMPTY (Feb 21-28 missing) → BUG

Station 15020:
- decad_in_year 2-11: dates=EMPTY (Jan-Apr missing) → Source data (runoff_day.csv) has empty
  discharge values from 2024-12-02 through 2025-02-28. BUT the date column should still be
  populated with the issue date even when there's no discharge data.
```

---

## Issue 1: Date Column Should Always Be Populated

**Location:** `apps/iEasyHydroForecast/forecast_library.py` - `write_pentad_hydrograph_data()` and `write_decad_hydrograph_data()`

**Clarification from user:** The `date` column shows the **forecast issue date** (last day of previous pentad/decad). This date is deterministic from the calendar and should ALWAYS be present, even if there's no discharge data for that pentad/decad.

**Current behavior:** The date column is empty when:
- No current year data exists (station 15020, decads 2-11)
- Feb 29 data was dropped (stations 15013, 15016, decad 7)

**Expected behavior:** The date column should show the issue date for every pentad/decad:
- decad 1: Dec 31 (or Dec 30 in some years)
- decad 2: Jan 10
- decad 3: Jan 20
- decad 4: Jan 31
- ...
- decad 7: Feb 28 (or Feb 29 in leap years)
- ...etc.

**Root causes:**
1. **Leap year handling** (Issue 2) drops Feb 29 data entirely, leaving decad 7 empty
2. **Merge logic** only includes dates from actual data, not generating a complete calendar scaffold

---

## Issue 2: Leap Year February 29 Handling Bug (PRIMARY BUG)

**Location:** `apps/iEasyHydroForecast/forecast_library.py` lines 3086-3088 (pentad) and 3273-3275 (decad)

**Problem:** The leap year check only considers the current year:
```python
if not is_leap_year(current_year):
    data = data[~((data['date'].dt.month == 2) & (data['date'].dt.day == 29))]
    data.loc[(data['date'].dt.month > 2), 'day_of_year'] -= 1
```

This logic:
1. If current year (2025) is NOT a leap year, it drops ALL Feb 29 data from ALL years (including valid Feb 29 from 2024, 2020, etc.)
2. If current year IS a leap year, it does NOT adjust day_of_year for historical non-leap years

**This is likely causing the empty decad 7 (Feb 21-28) for stations 15013 and 15016.**

### Understanding the Requirement

The hydrograph files need to display data for **every pentad/decad of the year**, with a flexible "last day of February":
- In leap years: pentad 12 / decad 7 covers Feb 21-29
- In non-leap years: pentad 12 / decad 7 covers Feb 21-28

When current year is 2025 (non-leap), historical data from 2024 (leap year) Feb 29 should be **mapped to Feb 28** (not dropped) so it contributes to the statistics for pentad 12 / decad 7.

### The Tricky Part

The issue date for the last pentad/decad of February is:
- Feb 28 in non-leap years (covers Feb 26-28 for pentad, Feb 21-28 for decad)
- Feb 29 in leap years (covers Feb 26-29 for pentad, Feb 21-29 for decad)

When we have:
- Current year 2025 (non-leap): issue date should be Feb 28
- Last year 2024 (leap): issue date was Feb 29

We need to **treat 2024's Feb 29 as Feb 28** for the purpose of aligning with 2025's calendar.

---

## Issue 3: Last Year Date Reconstruction Bug

**Location:** `apps/iEasyHydroForecast/forecast_library.py` lines 3146 (pentad) and 3393 (decad)

**Problem:** Last year's data is reconstructed using day_of_year:
```python
last_year_data.loc[:, 'date'] = pd.Timestamp(str(current_year)) + pd.to_timedelta(last_year_data['day_of_year'] - 1, unit='D')
```

When leap/non-leap years are mixed:
- Last year (2024, leap) Dec 31 has day_of_year = 366
- Reconstructing with current year (2025, non-leap): 2025 + 365 days = Jan 1, 2026 (WRONG!)

**Consequence:** Incorrect date mapping for last year data, especially at year-end.

---

## Proposed Fixes

### Fix 1: Leap Year Handling - Proper day_of_year Alignment

**Problem:** Current code only handles one direction of leap year mismatch and drops Feb 29 data entirely.

**The `day_of_year` alignment problem:**

The code uses `day_of_year` to reconstruct dates from last year into current year (line 3146):
```python
last_year_data['date'] = pd.Timestamp(current_year) + pd.to_timedelta(day_of_year - 1, unit='D')
```

This fails when leap/non-leap years don't match:

**Case 1: Current=non-leap (2025), Last=leap (2024)**
| 2024 date | day_of_year | Reconstructed 2025 | Expected | Fix |
|-----------|-------------|--------------------|----------|-----|
| Feb 29    | 60          | Mar 1 ❌            | Feb 28   | -1  |
| Mar 1     | 61          | Mar 2 ❌            | Mar 1    | -1  |
| Dec 31    | 366         | Jan 1, 2026 ❌      | Dec 31   | -1  |

**Case 2: Current=leap (2024), Last=non-leap (2023)**
| 2023 date | day_of_year | Reconstructed 2024 | Expected | Fix |
|-----------|-------------|--------------------|----------|-----|
| Mar 1     | 60          | Feb 29 ❌           | Mar 1    | +1  |
| Mar 2     | 61          | Mar 1 ❌            | Mar 2    | +1  |
| Dec 31    | 365         | Dec 30 ❌           | Dec 31   | +1  |

**Case 3: Both leap or both non-leap** → No adjustment needed ✓

**Proposed fix:**
```python
data['day_of_year'] = data['date'].dt.dayofyear

# Adjust day_of_year based on leap year mismatch between row's year and current year
for year in data['date'].dt.year.unique():
    year_mask = data['date'].dt.year == year
    row_is_leap = is_leap_year(year)
    current_is_leap = is_leap_year(current_year)

    if row_is_leap and not current_is_leap:
        # Leap → non-leap: subtract 1 for Feb 29 and later
        feb29_or_later = (data['date'].dt.month > 2) | \
                        ((data['date'].dt.month == 2) & (data['date'].dt.day == 29))
        data.loc[year_mask & feb29_or_later, 'day_of_year'] -= 1
    elif not row_is_leap and current_is_leap:
        # Non-leap → leap: add 1 for Mar 1 and later
        mar1_or_later = data['date'].dt.month > 2
        data.loc[year_mask & mar1_or_later, 'day_of_year'] += 1

# DON'T drop Feb 29 data - it's valid and day_of_year is now aligned
```

**Key insight:** The adjustment depends on BOTH the row's year AND the current year, not just current year.

**Files to modify:**
- `apps/iEasyHydroForecast/forecast_library.py` lines 3084-3088 (pentad)
- `apps/iEasyHydroForecast/forecast_library.py` lines 3271-3275 (decad)

### Fix 2: Ensure Date Column is Always Populated

**Problem:** The date column is empty when no data exists for a pentad/decad.

**Approach:** Reconstruct the issue date from `decad_in_year` / `pentad_in_year` column.

```python
def get_issue_date_from_decad(decad_in_year, year):
    """
    Given decad_in_year (1-36) and year, return the issue date.
    Issue date = last day of the PREVIOUS decad.

    decad_in_year -> month, decad_in_month
    1 -> Jan, decad 1 (days 1-10)  -> issue date = Dec 31 of previous year
    2 -> Jan, decad 2 (days 11-20) -> issue date = Jan 10
    3 -> Jan, decad 3 (days 21-31) -> issue date = Jan 20
    4 -> Feb, decad 1 (days 1-10)  -> issue date = Jan 31
    ...
    7 -> Feb, decad 3 (days 21-28/29) -> issue date = Feb 20
    ...
    """
    month = (decad_in_year - 1) // 3 + 1
    decad_in_month = (decad_in_year - 1) % 3 + 1

    if decad_in_month == 1:
        # Issue date is last day of previous month
        if month == 1:
            return pd.Timestamp(year=year-1, month=12, day=31)
        else:
            prev_month_last = pd.Timestamp(year=year, month=month, day=1) - pd.Timedelta(days=1)
            return prev_month_last
    elif decad_in_month == 2:
        return pd.Timestamp(year=year, month=month, day=10)
    else:  # decad_in_month == 3
        return pd.Timestamp(year=year, month=month, day=20)

# After merging, fill missing dates by reconstructing from decad_in_year
missing_date_mask = runoff_stats['date'].isna()
runoff_stats.loc[missing_date_mask, 'date'] = runoff_stats.loc[missing_date_mask, 'decad_in_year'].apply(
    lambda d: get_issue_date_from_decad(d, current_year)
)
```

**Files to modify:**
- `apps/iEasyHydroForecast/forecast_library.py` - `write_pentad_hydrograph_data()`
- `apps/iEasyHydroForecast/forecast_library.py` - `write_decad_hydrograph_data()`

### Fix 3: Correct Date Reconstruction for Last Year Data

**Problem:** Using `day_of_year` arithmetic fails across leap/non-leap year boundaries.

Example: 2024 (leap) day 366 (Dec 31) → 2025 + 365 days = Jan 1, 2026 (WRONG!)

**Approach:** Use `pd.DateOffset(years=1)` which handles leap years correctly.

```python
# Current code (line 3146):
last_year_data.loc[:, 'date'] = pd.Timestamp(str(current_year)) + pd.to_timedelta(last_year_data['day_of_year'] - 1, unit='D')

# Fixed code:
last_year_data.loc[:, 'date'] = last_year_data['date'] + pd.DateOffset(years=1)
# Note: Feb 29 from leap year will become Feb 28 (or Mar 1 depending on pandas version)
# We should handle this explicitly by mapping Feb 29 → Feb 28
```

**Files to modify:**
- `apps/iEasyHydroForecast/forecast_library.py` line 3146 (pentad)
- `apps/iEasyHydroForecast/forecast_library.py` line 3393 (decad)

---

## Implementation Steps

- [ ] **Fix 1** - Map Feb 29 to Feb 28 instead of dropping (leap year handling)
- [ ] **Fix 2** - Reconstruct missing dates from decad_in_year / pentad_in_year
- [ ] **Fix 3** - Use DateOffset for last year date reconstruction
- [ ] **Test with real data** - Run the module and verify hydrograph output
- [ ] **Add unit tests** for edge cases:
  - [ ] Leap year Feb 29 → Feb 28 mapping
  - [ ] Missing data still has valid issue dates
  - [ ] Year boundary (Dec 31 → Jan 1) for last year data

---

## Files to Modify

| File | Lines | Change |
|------|-------|--------|
| `apps/iEasyHydroForecast/forecast_library.py` | 3084-3088 | Feb 29 → Feb 28 mapping (pentad) |
| `apps/iEasyHydroForecast/forecast_library.py` | ~3170 | Fill missing dates from pentad_in_year |
| `apps/iEasyHydroForecast/forecast_library.py` | 3146 | Use DateOffset for last year (pentad) |
| `apps/iEasyHydroForecast/forecast_library.py` | 3271-3275 | Feb 29 → Feb 28 mapping (decad) |
| `apps/iEasyHydroForecast/forecast_library.py` | ~3410 | Fill missing dates from decad_in_year |
| `apps/iEasyHydroForecast/forecast_library.py` | 3393 | Use DateOffset for last year (decad) |

---

## Notes / Questions for Discussion

1. **Historical data consistency:** When mapping Feb 29 → Feb 28, if both Feb 28 and Feb 29 have data, they will both map to Feb 28 and be aggregated together in the statistics (mean, min, max, etc.). This should be fine since they're in the same pentad/decad anyway.

2. **Helper function location:** The `get_issue_date_from_decad()` and `get_issue_date_from_pentad()` functions could be added to `tag_library.py` for reuse elsewhere, or kept in `forecast_library.py` as private functions.

---

# Part 2: Hindcast Mode Feature

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

### Option A: Command-line argument
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

1. **First:** Fix the leap year and date bugs (Part 1) + comprehensive testing
2. **Second:** Add hindcast mode (Part 2)

The bug fixes are more urgent as they affect current operations.

---

# Part 3: Testing Plan

## Testing Strategy for Bug Fixes

### Unit Tests

Create test file: `apps/iEasyHydroForecast/tests/test_leap_year_handling.py`

#### Test 1: day_of_year alignment
```python
def test_day_of_year_leap_to_nonleap():
    """Current=2025 (non-leap), data from 2024 (leap)"""
    # Feb 29, 2024 (day 60) → should become day 59 (Feb 28 in 2025 terms)
    # Mar 1, 2024 (day 61) → should become day 60 (Mar 1 in 2025 terms)
    # Dec 31, 2024 (day 366) → should become day 365 (Dec 31 in 2025 terms)

def test_day_of_year_nonleap_to_leap():
    """Current=2024 (leap), data from 2023 (non-leap)"""
    # Mar 1, 2023 (day 60) → should become day 61 (Mar 1 in 2024 terms)
    # Dec 31, 2023 (day 365) → should become day 366 (Dec 31 in 2024 terms)

def test_day_of_year_same_type():
    """Both leap or both non-leap → no adjustment"""
    # No changes expected
```

#### Test 2: Feb 29 data preservation
```python
def test_feb29_not_dropped():
    """Feb 29 data should NOT be dropped, only adjusted"""
    # Create data with Feb 29 dates
    # Run through the function
    # Assert Feb 29 rows still exist (mapped to Feb 28)

def test_feb29_contributes_to_statistics():
    """Feb 29 data should contribute to decad 7 / pentad 12 statistics"""
    # Create data with Feb 28 and Feb 29
    # Run statistics calculation
    # Assert both contribute to the same pentad/decad
```

#### Test 3: Issue date reconstruction
```python
def test_get_issue_date_from_decad():
    """Test issue date calculation from decad_in_year"""
    assert get_issue_date_from_decad(1, 2025) == pd.Timestamp('2024-12-31')  # Dec 31
    assert get_issue_date_from_decad(2, 2025) == pd.Timestamp('2025-01-10')  # Jan 10
    assert get_issue_date_from_decad(3, 2025) == pd.Timestamp('2025-01-20')  # Jan 20
    assert get_issue_date_from_decad(4, 2025) == pd.Timestamp('2025-01-31')  # Jan 31
    assert get_issue_date_from_decad(7, 2025) == pd.Timestamp('2025-02-20')  # Feb 20
    assert get_issue_date_from_decad(36, 2025) == pd.Timestamp('2025-12-20') # Dec 20

def test_get_issue_date_from_pentad():
    """Test issue date calculation from pentad_in_year"""
    assert get_issue_date_from_pentad(1, 2025) == pd.Timestamp('2024-12-31')  # Dec 31
    assert get_issue_date_from_pentad(12, 2025) == pd.Timestamp('2025-02-25') # Feb 25
    assert get_issue_date_from_pentad(72, 2025) == pd.Timestamp('2025-12-25') # Dec 25
```

#### Test 4: Missing date filling
```python
def test_missing_dates_filled():
    """Rows with missing dates should get reconstructed dates"""
    # Create runoff_stats with some NaN dates
    # Run the date filling logic
    # Assert all dates are populated
```

### Integration Tests

Create test file: `apps/iEasyHydroForecast/tests/test_hydrograph_output.py`

#### Test: Full hydrograph generation
```python
def test_write_decad_hydrograph_complete():
    """All 36 decads should have data for each station"""
    # Run write_decad_hydrograph_data with test data
    # Read output file
    # Assert each station has exactly 36 rows
    # Assert no date column is empty

def test_write_pentad_hydrograph_complete():
    """All 72 pentads should have data for each station"""
    # Similar to above
```

### Real Data Tests

#### Test with actual data files
```python
def test_with_kyg_data():
    """Test with actual Kyrgyzstan data"""
    # Load runoff_day.csv
    # Run the full pipeline
    # Check hydrograph_decad.csv output:
    #   - Station 15013: decad 7 should have date and data
    #   - Station 15016: decad 7 should have date and data
    #   - Station 15020: all decads should have dates (even if no discharge data)
```

### Manual Verification Checklist

After implementing fixes, manually verify:

- [ ] `hydrograph_decad.csv` has 36 rows per station
- [ ] `hydrograph_pentad.csv` has 72 rows per station
- [ ] All `date` columns are populated (no empty values)
- [ ] Station 15013: decad 7 has date = 2025-02-20 (issue date)
- [ ] Station 15016: decad 7 has date = 2025-02-20
- [ ] Station 15020: all decads have dates, missing discharge shows as NaN
- [ ] Run with 2024 data (leap year) and verify Feb 29 handling
- [ ] Compare statistics before/after fix to ensure no regression

### Test Data Scenarios

| Scenario | Current Year | Last Year | Expected Behavior |
|----------|--------------|-----------|-------------------|
| A | 2025 (non-leap) | 2024 (leap) | Feb 29 data preserved, day_of_year -1 for Mar+ |
| B | 2024 (leap) | 2023 (non-leap) | day_of_year +1 for Mar+ |
| C | 2025 (non-leap) | 2023 (non-leap) | No adjustment |
| D | 2024 (leap) | 2020 (leap) | No adjustment |

### Edge Cases to Test

1. **Year boundary:** Dec 31 → Jan 1 transition
2. **Leap year Feb 29:** Data from Feb 29 of leap year
3. **Missing source data:** Station with gaps in daily data
4. **First run:** No historical data in output file
5. **Multi-year data:** Statistics spanning leap and non-leap years

---

## Implementation Order

1. [ ] Create test file structure
2. [ ] Implement Fix 1 (leap year day_of_year alignment)
3. [ ] Write and run unit tests for Fix 1
4. [ ] Implement Fix 2 (date column always populated)
5. [ ] Write and run unit tests for Fix 2
6. [ ] Implement Fix 3 (DateOffset for last year)
7. [ ] Write and run unit tests for Fix 3
8. [ ] Run integration tests
9. [ ] Manual verification with real data
10. [ ] Document any changes to output format
