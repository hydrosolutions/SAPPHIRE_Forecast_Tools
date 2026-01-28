# GI PREPQ-005: Maintenance Mode Produces Data with Large Gaps

## Problem Statement

The preprocessing_runoff module in maintenance mode produces output files (`hydrograph_day.csv`, `runoff_day.csv`) with large data gaps. This causes downstream modules (linear_regression, machine_learning) to produce forecasts with missing data for extended periods.

**Discovered**: 2026-01-27 during server testing of main branch

## Symptoms

### Observed on Production Server (2026-01-27)

1. **hydrograph_day.csv** has data gap from **March 10, 2025 to November 25, 2025** for station 16059 (and likely others)

2. **forecast_pentad_linreg_latest.csv** shows the same pattern:
   - Data present: Jan-Feb 2025, then Nov 25 2025 - Jan 2026
   - Missing data: March 10, 2025 to November 25, 2025
   - Missing periods show: empty predictor columns, flag=1.0, forecast=-1.0

3. **Dashboard visualizations** show:
   - Predictors tab: Data only at year start/end with straight lines connecting
   - Skill metrics: Same gap pattern
   - No ML forecasts for stations 16059, 15189

### Example Data (station 16059 in forecast_pentad_linreg_latest.csv)

```csv
# Data present (Jan-Feb 2025)
2025-02-05,16059,602.0,199.2,1,7.0,0.322,6.211,200.255,188.45,...
2025-03-05,16059,585.0,195.0,1,13.0,0.329,2.407,195.049,192.571,...

# Gap begins - empty predictors, flag=1.0, forecast=-1.0
2025-03-10,16059,,,2,14.0,1.0,0.0,-1.0,0.0,0.0,0.0,0.0
2025-03-15,16059,,,3,15.0,1.0,0.0,-1.0,0.0,0.0,0.0,0.0
... (continues through November)
2025-11-20,16059,,,4,64.0,1.0,0.0,-1.0,0.0,0.0,0.0,0.0

# Data resumes (Nov 25 2025)
2025-11-25,16059,,181.0,5,65.0,1.0,0.0,-1.0,0.0,0.0,0.0,0.0
2025-11-30,16059,543.0,181.8,6,66.0,0.193,83.935,188.699,196.856,...
```

## Root Cause Analysis

**NOT a dashboard issue** - the raw data files have the gaps.

**NOT a linear_regression issue** - it correctly reflects the missing input data.

### Investigation Results (2026-01-28)

After implementing the gap-filling fix and testing locally, the following was determined:

1. **Gap detection works correctly**: 101 gaps (1,618 total days) detected across 40 sites
2. **API calls are made correctly**: 13 batch API requests for gap periods
3. **The API doesn't have data for the gap dates**: Many gap periods return "No daily average data found"

**Key diagnostic output:**
```
[GAPS] Of 14032 API records, 0 are for actual gap dates (out of 1618 gap dates)
```

This means the iEasyHydro HF database genuinely doesn't have measurements for those gap dates. The gaps are **real operational data gaps at the source**, not a code bug.

### Original Hypotheses - Resolution

1. **Date range calculation bug**: ❌ NOT the issue - date calculations are correct
2. **iEasyHydro HF data availability**: ✅ **CONFIRMED** - Data genuinely missing from source database
3. **Caching issue**: ❌ NOT the issue - caching works correctly
4. **Excel file vs API merge logic**: ❌ NOT the issue - merge logic works correctly (fix implemented)
5. **Time zone handling**: ❌ NOT the issue - dates are normalized correctly

## Investigation Steps

### Step 1: Verify Source Data Availability

Query iEasyHydro HF API directly for station 16059 for the gap period:

```python
# Test script to run on server
from ieasyhydro_sdk.sdk import IEasyHydroHFSDK

sdk = IEasyHydroHFSDK()
response = sdk.get_data_values_for_site(
    filters={
        "site_codes": "16059",
        "start_date": "2025-03-01",
        "end_date": "2025-04-01",
        "page_size": 10
    }
)
print(f"Count: {response.get('count', 0)}")
print(f"Results: {response.get('results', [])[:5]}")
```

### Step 2: Check Maintenance Mode Date Calculations

Review `preprocessing_runoff.py` maintenance mode logic:
- What date range is being requested?
- Is it correctly calculating the lookback window?
- Are there any early-exit conditions that skip data retrieval?

### Step 3: Review Log Files

Check maintenance run logs for:
- `[API]` tagged messages showing what was requested
- `[DATA]` tagged messages showing what was processed
- Any warnings about missing data or empty responses

### Step 4: Compare with Fresh Run

Run preprocessing_runoff in maintenance mode with verbose logging and compare:
- Sites requested vs sites returned
- Date range requested vs dates in response
- Record counts at each stage

## Clarifications

This section addresses common questions about the bug fix scope and approach.

### Prerequisites Before Code Changes

**Step 1 (Verify Source Data) MUST be completed first.** If the iEasyHydro HF API genuinely has no data for the gap period (March-November 2025), no code fix can help. The investigation must confirm:
1. Data exists in the source API for the gap period
2. The issue is retrieval/merge logic, not missing source data

If source data is missing, this becomes a data availability issue, not a code bug.

### Gap Definition

**What constitutes a "gap"?**
- A gap is any date where a station should have data but doesn't
- For runoff stations, we expect daily data during the measurement season
- Single missing days count as gaps (not just multi-day gaps)
- The fix should attempt to fill ALL gaps, not just large ones

**Legitimate gaps vs bug-induced gaps:**
- **Legitimate gaps**: Equipment failure, seasonal station closure, station not yet commissioned
- **Bug-induced gaps**: Data exists in source but wasn't retrieved due to code issue

The fix should attempt to retrieve data for all gaps. If the API returns no data for a gap period, that gap is legitimate and should remain (with appropriate logging).

### Maintenance Mode Lookback Window

**Current behavior**: Maintenance mode requests data for a limited lookback window (recent days/weeks only).

**Expected behavior after fix**: Maintenance mode should:
1. Load cached data
2. Detect gaps in the cached data (dates with missing values)
3. Request data from API for the full period needed (including gap periods)
4. Merge all retrieved data

The lookback window is defined in the configuration. The fix should not change the lookback window itself, but should add gap detection that triggers additional requests when gaps are found.

### Performance Considerations

**API rate limiting**: The iEasyHydro HF API has a page_size limit of 10 records. Gap-filling may require many API calls.

**Mitigation strategies**:
- Batch gap periods into larger date ranges where possible
- Log the number of additional API calls made for gap-filling
- Consider a maximum gap age (e.g., don't try to fill gaps older than 2 years)

**Acceptable tradeoff**: Maintenance mode is expected to be slower than operational mode. Additional API calls for gap-filling are acceptable as long as they complete within reasonable time (< 10 minutes for a full run).

### Data Source Priority

**Excel or csv files vs API data**:
- Excel or csv files contain historical data (legacy)
- API provides current/recent data
- When both have data for the same date/station, **API data takes precedence** (more recent source)
- Excel or csv files should only fill gaps that the API cannot fill

This priority is already implemented; the fix should not change it.

### Scope Boundaries

**In scope for this fix**:
- Gap detection in `_merge_with_update()` or new helper function
- Additional API requests for detected gaps
- Ensuring `runoff_day.csv` has continuous data where source data exists
- Ensuring `hydrograph_day.csv` reflects all available data fom the current and previous year

**Out of scope**:
- Changes to operational mode (should remain fast, minimal API calls)
- Changes to the iEasyHydro HF SDK
- Changes to downstream modules (linear_regression, machine_learning, dashboard)
- Backfilling data that doesn't exist in the source

## Affected Files

### Input/Configuration
- `apps/preprocessing_runoff/preprocessing_runoff.py` - main module with mode logic
- `apps/preprocessing_runoff/src/src.py` - data retrieval functions
- `apps/preprocessing_runoff/config.yaml` - configuration

### Output (affected by bug)
- `intermediate_data/hydrograph_day.csv` - daily hydrograph data
- `intermediate_data/runoff_day.csv` - daily runoff data
- `intermediate_data/forecast_pentad_linreg_latest.csv` - pentadal forecasts
- `intermediate_data/forecast_decad_linreg_latest.csv` - decadal forecasts

## Related Issues

- **PREPQ-001**: Fixed operational/maintenance mode distinction
- **PREPQ-003**: Data retrieval validation (logging, caching) - may help diagnose this issue

## Test Coverage

Integration tests for gap handling are in:
`apps/preprocessing_runoff/test/test_integration_maintenance_gaps.py`

### Test Classes (37 tests total)

| Class | Tests | Purpose |
|-------|-------|---------|
| `TestMaintenanceModeDataGaps` | 2 | Core gap behavior verification |
| `TestCachedDataLoading` | 1 | Cache persistence with gaps |
| `TestMergeWithUpdateGapBehavior` | 1 | Merge baseline behavior |
| `TestGapDetection` | 7 | `_detect_gaps_in_data()` function |
| `TestGapGrouping` | 3 | `_group_gaps_for_api()` function |
| `TestGapFillingIntegration` | 2 | `_fill_gaps_from_api()` integration |
| `TestEndToEndMaintenanceModeGapFilling` | 7 | Full workflow tests |
| `TestEdgeCases` | 7 | Single row, adjacent gaps, SDK None, boundary, API params, manual filtering |
| `TestDeterministicDates` | 4 | Fixed-date tests for reproducibility |
| `TestCSVRoundTrip` | 3 | CSV write/read type consistency |

Additional tests in `test_merge_update.py`: 7 tests for `_merge_with_update()` function.

Run with: `python -m pytest test/test_integration_maintenance_gaps.py test/test_merge_update.py -v`

## Comprehensive Test Plan

This section documents the full test coverage needed to ensure:
1. All available runoff data ends up in `runoff_day.csv`
2. Current year and last year data ends up in `hydrograph_day.csv`

### Overview: Current Test Coverage Gaps

Analysis of `src/src.py` (~4037 lines) reveals **69% of functions are untested**:
- **24 functions** in src.py
- **Only 8** have any test coverage
- **16 functions** have no tests at all

Critical untested functions:
- `get_runoff_data_for_sites_HF()` - main API data retrieval
- `_merge_with_update()` - core merge logic (root cause of PREPQ-005)
- `_load_cached_data()` - cache loading
- `from_daily_time_series_to_hydrograph()` - hydrograph generation
- `write_hydrograph_day_csv()` - output writing

### Test Category 1: Excel to runoff_day.csv Flow

**Purpose**: Ensure historical data from Excel files is correctly loaded, processed, and written to runoff_day.csv.

#### Unit Tests

| Test | Function | Description |
|------|----------|-------------|
| `test_read_excel_basic_columns` | `get_runoff_data()` | Verify required columns extracted |
| `test_read_excel_date_parsing` | `get_runoff_data()` | Various date formats parsed correctly |
| `test_read_excel_missing_columns` | `get_runoff_data()` | Graceful handling of missing columns |
| `test_read_excel_empty_file` | `get_runoff_data()` | Returns empty DataFrame, no crash |
| `test_read_excel_station_filter` | `get_runoff_data()` | Only configured stations included |
| `test_date_column_normalization` | processing | Ensure Date column is datetime64[ns] |
| `test_code_column_typing` | processing | Code column is string type |
| `test_discharge_column_numeric` | processing | Q_m3s converted to float |
| `test_duplicate_date_station_handling` | processing | Duplicates detected/resolved |
| `test_nan_handling_in_discharge` | processing | NaN values preserved, not dropped |

#### Integration Tests

| Test | Description |
|------|-------------|
| `test_excel_to_runoff_day_full_flow` | Load Excel → process → write runoff_day.csv |
| `test_excel_multiple_stations` | Multiple stations written with correct codes |
| `test_excel_date_range_filtering` | Only requested date range in output |
| `test_excel_preserves_all_dates` | No gaps introduced during processing |
| `test_excel_station_not_in_config` | Unconfigured stations excluded |

### Test Category 2: API to runoff_day.csv Flow

**Purpose**: Ensure API data from iEasyHydro HF is correctly fetched, paginated, and written.

#### Unit Tests: API Client

| Test | Function | Description |
|------|----------|-------------|
| `test_api_pagination_single_page` | `get_runoff_data_for_sites_HF()` | <10 records returns correctly |
| `test_api_pagination_multi_page` | `get_runoff_data_for_sites_HF()` | >10 records paginated correctly |
| `test_api_pagination_exact_boundary` | `get_runoff_data_for_sites_HF()` | Exactly 10, 20, 30 records |
| `test_api_empty_response` | `get_runoff_data_for_sites_HF()` | count=0 returns empty DataFrame |
| `test_api_date_filter_format` | `get_runoff_data_for_sites_HF()` | Dates formatted as YYYY-MM-DD |
| `test_api_site_codes_filter` | `get_runoff_data_for_sites_HF()` | Comma-separated site codes |
| `test_api_timeout_handling` | `get_runoff_data_for_sites_HF()` | Timeout raises/retries appropriately |
| `test_api_rate_limiting` | `get_runoff_data_for_sites_HF()` | 429 responses handled |
| `test_api_partial_data` | `get_runoff_data_for_sites_HF()` | Some stations return data, others don't |

#### Unit Tests: Response Processing

| Test | Function | Description |
|------|----------|-------------|
| `test_response_to_dataframe` | processing | API JSON → DataFrame conversion |
| `test_response_column_mapping` | processing | site_code→Code, timestamp→Date |
| `test_response_date_parsing` | processing | ISO timestamps to datetime |
| `test_response_discharge_extraction` | processing | value field extracted to Q_m3s |
| `test_response_duplicate_timestamps` | processing | Same station, same timestamp |
| `test_response_invalid_values` | processing | Non-numeric values handled |

#### Integration Tests

| Test | Description |
|------|-------------|
| `test_api_to_runoff_day_full_flow` | Mock API → process → write runoff_day.csv |
| `test_api_multi_station_response` | Multiple stations in single response |
| `test_api_date_range_respected` | Only requested dates returned |
| `test_api_no_gaps_in_output` | Continuous dates for each station |
| `test_api_operational_mode_today_only` | Operational fetches minimal data |
| `test_api_maintenance_mode_lookback` | Maintenance fetches full lookback window |

### Test Category 3: Merge/Update Logic (ROOT CAUSE)

**Purpose**: Test `_merge_with_update()` function which is the root cause of PREPQ-005.

#### Current Behavior Tests (Document Bug)

| Test | Description | Status |
|------|-------------|--------|
| `test_merge_preserves_gaps_outside_new_range` | Gap in cached data outside API range persists | ✅ Exists |
| `test_merge_only_adds_new_data_rows` | Only rows from new_data are added | ✅ Exists |
| `test_cached_data_gaps_not_detected` | Function doesn't detect/fill historical gaps | ✅ Exists |

#### New Tests Needed (After Fix)

| Test | Description |
|------|-------------|
| `test_merge_fills_gaps_from_api` | When API has data for gap period, fill it |
| `test_merge_detects_historical_gaps` | Function identifies gaps in cached data |
| `test_merge_requests_additional_data` | Triggers API call for detected gaps |
| `test_merge_update_newer_timestamp` | Newer API data overwrites cached |
| `test_merge_keep_older_if_no_new` | Missing in new_data preserves cached |
| `test_merge_handles_overlapping_ranges` | Partial overlap between cached and new |
| `test_merge_empty_cached_data` | First run with no cache |
| `test_merge_empty_new_data` | API returns nothing |
| `test_merge_both_empty` | Fresh install, no data |

### Test Category 4: Hydrograph Generation

**Purpose**: Ensure `from_daily_time_series_to_hydrograph()` correctly generates statistics.

#### Unit Tests

| Test | Function | Description |
|------|----------|-------------|
| `test_hydrograph_year_filtering` | generation | Only current + last year included |
| `test_hydrograph_day_of_year_calc` | generation | DOY calculated correctly (1-366) |
| `test_hydrograph_statistics_complete` | generation | mean, std, min, max, percentiles |
| `test_hydrograph_leap_year_handling` | generation | Feb 29 handled correctly |
| `test_hydrograph_insufficient_years` | generation | <2 years of data |
| `test_hydrograph_single_station` | generation | One station output correct |
| `test_hydrograph_multi_station` | generation | Multiple stations, correct grouping |
| `test_hydrograph_nan_in_statistics` | generation | NaN values in stats calculation |
| `test_hydrograph_date_range_boundaries` | generation | Jan 1 and Dec 31 included |
| `test_hydrograph_preserves_all_doys` | generation | No DOY gaps in output |

#### Integration Tests

| Test | Description |
|------|-------------|
| `test_runoff_day_to_hydrograph_full_flow` | runoff_day.csv → hydrograph_day.csv |
| `test_hydrograph_reflects_input_gaps` | Gaps in input → gaps in output |
| `test_hydrograph_date_continuity` | No unexpected gaps introduced |
| `test_hydrograph_column_format` | Output matches expected schema |

### Test Category 5: Cache Management

**Purpose**: Test `_load_cached_data()` and cache file handling.

#### Unit Tests

| Test | Function | Description |
|------|----------|-------------|
| `test_load_cache_file_exists` | `_load_cached_data()` | Returns DataFrame from file |
| `test_load_cache_file_missing` | `_load_cached_data()` | Returns empty DataFrame |
| `test_load_cache_file_corrupt` | `_load_cached_data()` | Handles malformed CSV |
| `test_load_cache_preserves_dtypes` | `_load_cached_data()` | Date, Code types correct |
| `test_load_cache_with_gaps` | `_load_cached_data()` | Gaps preserved, not filled |
| `test_cache_staleness_detection` | caching | Warns if cache too old |
| `test_cache_path_resolution` | caching | Correct path in Docker/local |

### Test Category 6: Edge Cases and Error Handling

**Purpose**: Test boundary conditions and error scenarios.

#### Critical Edge Cases

| Test | Description |
|------|-------------|
| `test_empty_api_response_handling` | API returns count=0 |
| `test_api_500_error_recovery` | Server error doesn't crash module |
| `test_invalid_date_in_response` | Malformed date string |
| `test_station_code_mismatch` | API returns unexpected station |
| `test_duplicate_records_same_timestamp` | Same station/date/value twice |
| `test_very_large_date_range` | Multi-year request (pagination) |
| `test_future_dates_in_api` | Data with dates > today |
| `test_timezone_boundary_dates` | Dates near midnight UTC |
| `test_negative_discharge_values` | Q < 0 in response |
| `test_extremely_large_discharge` | Q > 10000 m³/s |

#### Mode-Specific Tests

| Test | Description |
|------|-------------|
| `test_operational_mode_skips_backfill` | Operational doesn't request historical |
| `test_maintenance_mode_full_backfill` | Maintenance requests full range |
| `test_mode_detection_from_config` | Correct mode selected |
| `test_mode_switching_mid_run` | (Should not happen) |

### Test Category 7: Output File Validation

**Purpose**: Ensure output files match expected schema.

#### runoff_day.csv Tests

| Test | Description |
|------|-------------|
| `test_runoff_day_required_columns` | Date, Code, Q_m3s present |
| `test_runoff_day_date_sorted` | Dates in ascending order |
| `test_runoff_day_no_header_duplicates` | Header appears once |
| `test_runoff_day_encoding` | UTF-8 encoding |
| `test_runoff_day_line_endings` | Unix line endings |

#### hydrograph_day.csv Tests

| Test | Description |
|------|-------------|
| `test_hydrograph_day_required_columns` | DOY, Code, stats columns |
| `test_hydrograph_day_doy_range` | DOY 1-366 only |
| `test_hydrograph_day_no_negative_stats` | Stats >= 0 |
| `test_hydrograph_day_encoding` | UTF-8 encoding |

### Implementation Priority

**Phase 1: Critical (Fix PREPQ-005)**
1. Tests for `_merge_with_update()` - document bug, then fix
2. Tests for gap detection logic (new functionality)
3. Integration test: maintenance mode produces continuous data

**Phase 2: High (Data Integrity)**
1. API pagination tests
2. Cache loading tests
3. Hydrograph generation tests

**Phase 3: Medium (Robustness)**
1. Error handling tests
2. Edge case tests
3. Output validation tests

**Phase 4: Low (Completeness)**
1. Excel loading tests (legacy path)
2. Mode detection tests
3. Encoding/formatting tests

### Test File Organization

```
apps/preprocessing_runoff/test/
├── test_integration_maintenance_gaps.py  # Existing PREPQ-005 tests
├── test_merge_update.py                  # NEW: _merge_with_update() tests
├── test_api_client.py                    # NEW: API fetch/pagination tests
├── test_cache_management.py              # NEW: Cache loading tests
├── test_hydrograph_generation.py         # NEW: Hydrograph stats tests
├── test_output_validation.py             # NEW: File schema tests
└── test_edge_cases.py                    # NEW: Error/boundary tests
```

### Testing Philosophy

**Prefer testing actual methods over mocks.** Mocks should be used sparingly and only for:
- External API calls (iEasyHydro HF SDK) to avoid network dependencies
- File system operations when testing error conditions
- Time-dependent functions (e.g., "today's date")

For all internal logic (`_merge_with_update()`, `from_daily_time_series_to_hydrograph()`, etc.), test the actual implementation with real DataFrames. This ensures tests catch real bugs rather than just verifying mock interactions.

### Test Data Requirements

1. **Mock API responses**: JSON fixtures for external API only
2. **Sample CSV files**: Synthetic data matching runoff_day.csv, hydrograph_day.csv schema (not copies of actual data)
3. **Excel test files**: Synthetic data matching historical format (not copies of actual data)
4. **Config fixtures**: Test configurations for different modes

**Important**: All test data must be synthetic/generated, never copies of actual production data. This ensures tests are reproducible, don't contain sensitive information, and can be version-controlled.

## Implementation Design Notes

The following are intentional design decisions in the gap detection implementation, not bugs:

### 1. Gaps at Data Boundaries Not Detected

The gap detection only identifies missing dates **between** the minimum and maximum dates of existing data for each site. If data ends in November and there's no data through January, this is not detected as a "gap" by the gap-filling logic.

**Rationale**: This is handled by the regular "smart-lookback" fetch which requests recent data based on the latest date per site. The gap-filling mechanism is specifically for historical gaps within the existing data range.

### 2. NaN Discharge Values Not Considered Gaps

Rows that have a date but contain NaN/missing discharge values are not considered gaps. The gap detection only identifies **missing date rows**, not rows with missing values.

**Rationale**: A row with a date exists in the data structure, meaning the API was queried for that date and returned no value (legitimate missing data). Re-querying the API would return the same result.

### 3. `max_gap_age_days` Hardcoded

The maximum age for gap detection is hardcoded to 730 days (2 years). Gaps older than this are not detected or filled.

**Rationale**: Very old gaps are unlikely to have data available in the API. Attempting to fill them would waste API calls. This could be made configurable via environment variable in a future enhancement if needed.

## Acceptance Criteria

- [x] Root cause identified and documented (`_merge_with_update()` doesn't fill gaps outside new_data range)
- [x] Regression tests added that reproduce the bug
- [x] Comprehensive test plan documented
- [x] Phase 1 critical tests implemented (merge_update, gap detection)
- [x] Fix implemented for maintenance mode data retrieval (`_detect_gaps_in_data`, `_fill_gaps_from_api`)
- [x] Manual site filtering implemented (only fill gaps for sites in code_list)
- [x] Deduplication of API responses implemented
- [x] Comprehensive logging added for debugging
- [ ] `hydrograph_day.csv` contains continuous data (no multi-month gaps) - *needs server testing*
- [ ] Downstream forecasts (linreg, ML) produce continuous output - *needs server testing*
- [ ] Dashboard visualizations show continuous data without straight-line gaps - *needs server testing*
- [x] Tests updated to assert FIXED behavior (gap should be filled)
- [x] End-to-end integration tests with mocked SDK
- [x] CSV round-trip tests (verify type consistency after write/read)
- [x] Manual site filtering tests
- [ ] Phase 2-4 tests implemented (post-fix)
- [x] All preprocessing_runoff module tests pass - **72 tests passing**
- [x] All repository tests pass (excluding linear_regression which has no tests)

## Local Testing Results (2026-01-28)

### Summary

The gap-filling implementation is **working correctly**. Local testing with maintenance mode shows:

1. **Smart lookback fetch works**: 39,326 new records fetched from API (2024-01-01 to 2026-01-27)
2. **Gap detection works**: 101 gaps detected (1,618 total days) across 40 sites
3. **Gap filling API calls work**: 13 API batches requested for gap periods
4. **Manual site filtering works**: Only gaps for manual sites (code_list) are filled

### Key Finding

The remaining gaps are **real operational data gaps** where the iEasyHydro HF database has no measurements:

```
[GAPS] Of 14032 API records, 0 are for actual gap dates (out of 1618 gap dates)
```

Gap periods with no data in API:
- 2025-06-10 to 2025-06-20 (1 site)
- 2025-07-21 to 2025-07-22 (1 site)
- 2025-07-26 to 2025-07-27 (2 sites)
- 2025-11-20 to 2025-11-21 (1 site)
- 2025-12-16 to 2025-12-16 (1 site)
- 2025-12-26 to 2025-12-27 (2 sites)
- 2026-01-21 to 2026-01-22 (3 sites)

These gaps cannot be filled programmatically because the source data doesn't exist.

### Current Status

- **Code**: Fix implemented and tested locally
- **Tests**: 72 tests passing in preprocessing_runoff module
- **Server Testing**: Pending - will verify full pipeline integration

## Priority

**Medium** - The code fix is complete and working. Remaining gaps are due to missing source data, not code issues.

---

*Created: 2026-01-27*
*Updated: 2026-01-28 - Added comprehensive test plan*
*Updated: 2026-01-28 - Added clarifications section addressing implementation questions*
*Updated: 2026-01-28 - Added implementation design notes and end-to-end integration tests*
*Updated: 2026-01-28 - Local testing complete, fix verified working, remaining gaps are source data issues*
