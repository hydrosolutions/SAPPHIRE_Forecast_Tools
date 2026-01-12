# GI PR-002: iEasyHydro HF Data Retrieval Validation

## Problem Statement

The preprocessing_runoff module fetches data from iEasyHydro HF database, but we lack robust validation to ensure data is being retrieved correctly. The API has complex limitations (page_size, bulk request limits) that can silently cause incomplete data retrieval. We need monitoring and validation to catch issues early.

## Objectives

1. Validate that data fetched from API matches what's written to output
2. Improve logging for debugging data flow issues
3. Add post-write validation for hindcast/maintenance mode
4. Enable spot-check validation against known reliable sites
5. Improve diagnostic test script usability
6. Ensure ALL forecast-enabled sites get data (including daily, monthly, seasonal forecasts)

## Implementation Plan

### Phase 0: SDK Data Retrieval Best Practices

**Goal**: Implement proven best practices for iEasyHydro HF SDK data retrieval based on testing.

**Findings from SDK Testing** (January 2025):

1. **Filter Parameters are Equivalent**:
   - `site_codes` and `station__station_code__in` produce identical results
   - Use `site_codes` as the canonical parameter (cleaner, documented)

2. **Page Size Limit**:
   - API has an undocumented hard limit of `page_size=10`
   - Larger values result in 422 errors
   - Always use `page_size=10` in production code

3. **Duplicate Site Codes in `get_discharge_sites()`**:
   - API returns both `manual` and `automatic` stations with the same site code
   - Example: 60 unique codes → 72 site records (3 duplicates: 15194, 16169, 16681)
   - **Resolution**: When duplicates exist, choose `manual` site type

4. **Manual Sites Only for Forecasts**:
   - Current implementation provides forecasts for manual sites only
   - This is a known limitation that must be documented
   - Automatic stations may be supported in future versions

5. **Pagination with Parallelization**:
   - Fetch page 1 to get total count
   - Calculate total pages: `(count + 9) // 10`
   - Fetch pages 2-N in parallel using `ThreadPoolExecutor`
   - Significant performance improvement over sequential pagination

6. **Handle Empty Records**:
   - Skip records where `data_value` is None or N/A
   - API returns records for dates without data (null values)

7. **API Returns Both Hydro AND Meteo Records** (discovered January 2025):
   - When requesting discharge data (WDDA/WDD), the API returns BOTH:
     - `station_type='hydro'` records (actual discharge data)
     - `station_type='meteo'` records (temperature, precipitation, etc.) for the same sites
   - The `count` field in API response is total station-result objects, NOT unique sites
   - Example: Requesting 62 sites returns `count=100` (some sites appear twice - once hydro, once meteo)
   - **Filtering by `station_type='hydro'` is correct** - this extracts only discharge data
   - Sites that return ONLY meteo records have **no discharge data in the database**
   - These are data quality issues to report to iEasyHydro HF administrators

8. **Sites Without Discharge Data** (example from KGHM, January 2025):
   - 9 of 62 discharge sites returned no WDDA data over 120 days:
     `['15020', '15025', '15194', '15213', '15217', '15954', '15960', '16936', '22222']`
   - These sites are registered as discharge sites but have no data entered
   - **Action**: Report to iEasyHydro HF administrators for investigation

**Implementation Requirements**:

```python
# Pagination helper pattern
def fetch_all_with_pagination(sdk, filters, parallel=True, max_workers=10):
    PAGE_SIZE = 10  # API limit - cannot be changed

    # Step 1: Fetch first page for count
    response = sdk.get_data_values_for_site(filters={**filters, "page": 1, "page_size": PAGE_SIZE})
    total_count = response.get('count', 0)
    total_pages = (total_count + PAGE_SIZE - 1) // PAGE_SIZE

    # Step 2: Fetch remaining pages in parallel
    if parallel and total_pages > 1:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # ... fetch pages 2-N in parallel
```

**Documentation Updates Required**:

1. **Module README** (`apps/preprocessing_runoff/README.md`):
   - Add section: "Supported Site Types"
   - Document that forecasts are generated for manual sites only
   - Explain the distinction between manual and automatic stations

2. **Documentation Improvement Plan** (`doc/documentation_improvement_plan.md`):
   - Add under "Coupling with iEasyHydro HF" → "Limitations" section:
     - Manual sites only (automatic station support TBD)
     - Page size limit of 10
     - Duplicate code handling (prefer manual)

**Files to modify**:
- `apps/preprocessing_runoff/src/src.py` (implement pagination helper)
- `apps/preprocessing_runoff/preprocessing_runoff.py` (use helper, deduplicate codes)
- `apps/preprocessing_runoff/README.md` (document limitations)
- `doc/documentation_improvement_plan.md` (add limitations section)

---

### Phase 1: Count Validation

**Goal**: Track record counts at each stage and flag discrepancies.

**Changes to `src/src.py`**:

```python
# In fetch_hydro_HF_data_robust():
# Track: sites_requested, sites_returned, records_fetched

# In get_daily_average_discharge_from_iEH_HF_for_multiple_sites():
# Log: records_from_api, records_after_processing

# In get_runoff_data_for_sites_HF():
# Log: records_from_excel, records_from_db, records_merged, records_final
```

**Validation checks**:
- [ ] Log count of sites requested vs sites with data returned
- [ ] Log count of records fetched vs records after DataFrame processing
- [ ] Warn if >20% of requested sites return no data
- [ ] Warn if date range in response doesn't match requested range

**Files to modify**:
- `apps/preprocessing_runoff/src/src.py`

---

### Phase 2: Improved Logging

**Status**: ✅ COMPLETE

**Goal**: Structured logging at key points for easier debugging.

**Implementation** (January 2025):

1. **Stage Tags**: All log messages use consistent stage tags for easy filtering:
   - `[CONFIG]` - Configuration, mode, timezone
   - `[API]` - iEasyHydro HF requests/responses, COUNT VALIDATION
   - `[DATA]` - DataFrame transformations, processing, outlier filtering
   - `[MERGE]` - Combining data sources
   - `[OUTPUT]` - Writing results, file paths
   - `[TIMING]` - Performance metrics

2. **Log Level Configuration** (hierarchical priority):
   - Module-specific in `config.yaml` (`logging.log_level`)
   - System-wide in `.env` (`log_level`)
   - Default: INFO

3. **Log Levels Used**:
   - `DEBUG`: Detailed data (DataFrame info, intermediate values, COUNT VALIDATION details)
   - `INFO`: Normal operation milestones, counts, summaries
   - `WARNING`: Potential issues (e.g., >20% of sites missing data)
   - `ERROR`: Failures that affect results

4. **Print Statement Conversion**: All debug print statements in production code converted to `logger.debug()` with appropriate stage tags. Test files and profiling utilities retain print statements intentionally.

**Files modified**:
- `apps/preprocessing_runoff/config.yaml` - Added logging configuration section
- `apps/preprocessing_runoff/src/config.py` - Added `get_log_level()`, `VALID_LOG_LEVELS`, `DEFAULT_LOG_LEVEL`
- `apps/preprocessing_runoff/preprocessing_runoff.py` - Fixed logging setup, added stage tags
- `apps/preprocessing_runoff/src/src.py` - Converted ~30 print statements to logger calls with stage tags
- `apps/preprocessing_runoff/README.md` - Added comprehensive logging documentation section

---

### Phase 3: Post-Write Validation (Hindcast Mode)

**Status**: ✅ COMPLETE

**Goal**: Validate `runoff_day.csv` after writing and track site data reliability over time.

**Critical requirement**: Forecasts require data from the last 3 days. Missing recent data will cause forecast failures.

**Implementation** (January 2025):

1. **Validation Functions** added to `src/src.py`:
   - `validate_recent_data()` - Check each site has data within N days
   - `load_reliability_stats()` - Load stats from JSON file
   - `save_reliability_stats()` - Save stats to JSON file
   - `update_reliability_stats()` - Update stats based on validation
   - `log_validation_summary()` - Log results with `[DATA]` stage tags
   - `run_post_write_validation()` - Main entry point combining all above

2. **Configuration** in `config.yaml`:
   ```yaml
   validation:
     enabled: true
     max_age_days: 3
     reliability_threshold: 80.0
     stats_file: "reliability_stats.json"
   ```

3. **Integration** in `preprocessing_runoff.py`:
   - Runs after data writing in maintenance mode only
   - Logs validation summary with sites missing recent data
   - Flags low reliability sites (<80% over recent checks)
   - Stores reliability stats in `intermediate_data/reliability_stats.json`

**Output format**:
```
[DATA] === Recent Data Validation ===
[DATA] Sites checked: 63
[DATA] Sites with data in last 3 days: 61 (96.8%)
[DATA] Sites missing recent data: 2
[DATA] MISSING RECENT DATA (forecast may fail):
[DATA]   - 15013: Last data 2025-12-25 (reliability: 93%)
[DATA]   - 16100: Last data 2025-12-20 (reliability: 50%)
[DATA] LOW RELIABILITY SITES (< 80% over recent checks):
[DATA]   - 16100: 50% reliability
```

**Files modified**:
- `apps/preprocessing_runoff/src/src.py` - Added 6 validation functions (~270 lines)
- `apps/preprocessing_runoff/src/config.py` - Added validation settings support
- `apps/preprocessing_runoff/config.yaml` - Added validation configuration section
- `apps/preprocessing_runoff/preprocessing_runoff.py` - Integrated validation call

---

### Phase 4: Spot-Check Validation

**Status**: ✅ COMPLETE

**Goal**: Verify data for known reliable sites matches direct API query.

**Implementation** (January 2025):

1. **Spot-Check Functions** added to `src/src.py`:
   - `get_spot_check_sites()` - Read site codes from environment variables
   - `spot_check_sites()` - Fetch latest API value and compare with output
   - `log_spot_check_summary()` - Log results with `[DATA]` stage tags
   - `run_spot_check_validation()` - Main entry point for spot-check validation

2. **Configuration**:
   - `config.yaml`: Added `spot_check.enabled` setting
   - Environment variable for site codes:
     - `IEASYHYDRO_SPOTCHECK_SITES` (comma-separated site codes, e.g., "15166,16159,15189")
   - Each deployment sets its own sites in their `.env` file
   - Skip spot-check if no sites configured

3. **Integration** in `preprocessing_runoff.py`:
   - Runs after post-write validation in maintenance mode
   - Requires SDK access (`ieh_hf_sdk is not None`)
   - Compares output values with direct API queries
   - Reports mismatches with both values

**Output format**:
```
[DATA] === Spot-Check Validation ===
[DATA] Spot-checking 3 sites: ['15166', '16159', '15189']
[DATA] Spot-check results: 3 matched, 0 mismatched
[DATA] SPOT-CHECK PASSED: All values match API
```

Or if mismatches:
```
[DATA] === Spot-Check Validation ===
[DATA] Spot-checking 3 sites: ['15166', '16159', '15189']
[DATA] Spot-check results: 2 matched, 1 mismatched
[DATA] SPOT-CHECK MISMATCH DETECTED:
[DATA]   - 15166: output=42.5, api=43.2, diff=0.7 (1.6%)
```

**Files modified**:
- `apps/preprocessing_runoff/src/src.py` - Added 4 spot-check functions (~260 lines)
- `apps/preprocessing_runoff/src/config.py` - Added spot_check settings support
- `apps/preprocessing_runoff/config.yaml` - Added spot_check configuration section
- `apps/preprocessing_runoff/preprocessing_runoff.py` - Integrated spot-check call

---

### Phase 5: Improve Diagnostic Test Script

**Status**: ✅ COMPLETE (minimal changes)

**Goal**: Make `testspecial_sdk_data_retrieval.py` more usable and informative.

**Implementation** (January 2025):

Minimal improvements made:
1. Added **expected runtime** to header docstring (single site: ~5s, CHECK_ALL_SITES: ~30s, DIAGNOSE_422: ~2-5 min)
2. Added **mode indicator** at startup showing which test mode is running

Note: The script already had comprehensive section headers, progress indicators, and summaries from previous development. Full refactoring was not needed.

**Files modified**:
- `apps/preprocessing_runoff/test/testspecial_sdk_data_retrieval.py`

---

### Phase 6: Site Caching for Performance

**Status**: ✅ COMPLETE

**Goal**: Cache forecast site list to avoid slow SDK calls in operational mode.

**Problem**: Fetching forecast sites from SDK takes ~10 seconds, and the site list rarely changes. In operational mode (daily runs), we should use cached values. Total runtime was 750 seconds with 728 seconds spent on data retrieval.

**Implementation**:

```python
# Cache file location (in intermediate data directory)
SITE_CACHE_FILE = "{intermediate_data_path}/forecast_sites_cache.json"

# Cache structure
{
    "cached_at": "2025-12-30T08:00:00+06:00",
    "cache_version": 1,
    "pentad": {
        "site_codes": ["15013", "16159", ...],
        "site_count": 62
    },
    "decad": {
        "site_codes": ["15013", "16159", ...],
        "site_count": 65
    },
    "combined_unique_codes": ["15013", "16159", ...],
    "combined_unique_count": 60
}
```

**Behavior by mode**:

| Mode | Site Loading | Data Fetching |
|------|--------------|---------------|
| Operational | Load from cache | Last 1-2 days only |
| Maintenance | Fetch from SDK, update cache | Full lookback window |

**Cache staleness**:
- Cache expires after N days (configurable via `SITE_CACHE_MAX_AGE_DAYS`, default 7)
- Operational mode fails if cache missing, warns if stale
- Maintenance mode always refreshes cache

**Additional optimization - deduplicate site codes**:
Current output shows duplicates: `['16169', '16681', '16681', '16169', '15194', '15194'...]`
Deduplicating 124 → ~60 unique codes would halve API requests.

**Functions to add**:

```python
def load_site_cache(cache_file: str, max_age_days: int = 7) -> dict | None:
    """Load cached site data. Returns None if cache missing or expired."""

def save_site_cache(cache_file: str, pentad_codes: list, decad_codes: list):
    """Save site codes to cache with timestamp."""

def get_unique_site_codes(pentad_codes: list, decad_codes: list) -> list:
    """Combine and deduplicate site codes from pentad and decad lists."""
```

**Files to modify**:
- `apps/preprocessing_runoff/preprocessing_runoff.py` (use caching, deduplicate codes)
- `apps/preprocessing_runoff/src/src.py` (add cache functions)

---

### Phase 7: All Forecast Site Selection

**Status**: ✅ COMPLETE

**Goal**: Ensure data is retrieved for ALL sites with ANY forecast type enabled (not just pentad/decad).

**Problem**: Current implementation only considers `pentad_forecast` and `decadal_forecast` flags. Sites with ONLY `daily_forecast`, `monthly_forecast`, or `seasonal_forecast` enabled are NOT included, causing missing data for those forecasts.

**Available flags in iEH HF API** (`enabled_forecasts` object):
- `daily_forecast` - Daily forecasts
- `pentad_forecast` - 5-day (pentadal) forecasts
- `decadal_forecast` - 10-day (decadal) forecasts
- `monthly_forecast` - Monthly forecasts
- `seasonal_forecast` - Seasonal forecasts

**Implementation options**:

1. **Option A: New unified function** (recommended):
   Create `get_all_forecast_sites_from_HF_SDK()` that returns sites with ANY forecast flag enabled.
   - Cleaner, single function to maintain
   - Used by preprocessing_runoff instead of combining pentad + decad

2. **Option B: Modify existing functions**:
   Add daily/monthly/seasonal checks to existing `pentad_forecast_sites_from_iEH_HF_SDK()`.
   - More risk of breaking existing pentad/decad-specific logic

**Functions to add/modify**:

```python
# In setup_library.py:
def get_all_forecast_sites_from_HF_SDK(ieh_hf_sdk):
    """
    Gets all sites with ANY forecast type enabled from iEH HF API.

    Returns sites where any of these flags are True:
    - daily_forecast
    - pentad_forecast
    - decadal_forecast
    - monthly_forecast
    - seasonal_forecast

    Returns:
        fc_sites (list): Site objects for all forecast-enabled sites
        site_codes (list): Site codes (strings) for API queries
        site_ids (list): iEH HF site IDs
    """
```

**Files to modify**:
- `apps/iEasyHydroForecast/setup_library.py` (add new function)
- `apps/iEasyHydroForecast/forecast_library.py` (add Site class method if needed)
- `apps/preprocessing_runoff/preprocessing_runoff.py` (use new function)

---

### Phase 8: Package Updates & Deprecation Warnings

**Status**: ✅ COMPLETE

**Goal**: Update dependencies to latest compatible versions and fix deprecation warnings.

**Findings** (January 2025):

All dependencies are up-to-date:
- pandas 2.3.3 (req: >=2.2.2)
- numpy 2.3.5 (req: >=1.26.4)
- ieasyhydro-sdk 0.3.2 (latest from git)
- All other packages at or above minimum versions

**Deprecation warnings addressed**:
- FutureWarning for `DataFrameGroupBy.apply` - suppressed with context manager in `filter_roughly_for_outliers()` (src.py:292-296). Full fix deferred as it would require refactoring the groupby logic.

**Remaining warnings (not deprecation)**:
- ResourceWarning about unclosed Excel files in tests - these are test cleanup issues, not deprecation warnings. Low priority.

**Files reviewed**:
- `apps/preprocessing_runoff/pyproject.toml` - dependencies are current
- `apps/preprocessing_runoff/src/src.py` - FutureWarning suppressed

---

### Phase 9: Code Architecture Review

**Goal**: Improve code structure and maintainability while working on this module.

**Status**: COMPLETED (reliability fixes only - refactoring deferred)

**Review Findings** (January 2025):

1. **File Size**:
   - `src/src.py` is now ~4037 lines (guideline: 300 max)
   - Not addressed in this PR - requires careful refactoring to avoid breaking changes
   - Full refactoring should be a separate issue

2. **Reliability Issues Fixed**:
   - **CRITICAL**: Empty DataFrame check was after code that accessed DataFrame methods (line 346+)
     - Fixed: Moved check immediately after data fetch
   - **CRITICAL**: `ieh_hf_sdk` undefined in legacy iEH path (line 408)
     - Fixed: Changed to `ieh_sdk` (correct variable in that scope)
   - **IMPORTANT**: Stale cache used without warning
     - Fixed: Added warning when `is_stale=True` in cached data

3. **Not Addressed** (deferred to future issues):
   - DataFrame modified in place in `from_daily_time_series_to_hydrograph` - low risk
   - Race condition in cache file operations - low risk (single-process typical usage)
   - ThreadPoolExecutor exceptions - existing handling is adequate
   - File splitting/refactoring - needs careful planning

**Future Extensibility Notes**:
- Module will need to support additional data sources (Swiss demo, Nepal)
- Legacy iEasyHydro support (`ieasyhydroforecast_connect_to_iEH`) will be **removed** in future refactoring
- Only iEasyHydro HF will be supported going forward
- Future: support for high-frequency (sub-daily) data using datetime instead of date

**Follow-up issue**: See `gi_PR-003_swiss_data_source_refactor.md` for:
- Swiss data source integration
- Module refactoring (`src.py` splitting)
- Legacy iEasyHydro code removal
- Abstract DataSource interface for multiple backends

**Files modified**:
- `apps/preprocessing_runoff/preprocessing_runoff.py` - reliability fixes (3 changes)

---

## Acceptance Criteria

### Phase 0: SDK Data Retrieval Best Practices
- [x] Use `site_codes` filter parameter (not `station__station_code__in`)
- [x] Deduplicate site codes before API requests (already implemented in preprocessing_runoff.py)
- [x] When duplicate codes exist, prefer `manual` site type over `automatic` (already implemented in forecast_library.py)
- [x] Implement pagination with `page_size=10` (API hard limit)
- [x] Parallelize page fetching for performance
- [x] Skip records with null/empty `data_value`
- [x] Document "manual sites only" limitation in module README
- [x] Add limitations section to documentation improvement plan

### Phase 1: Count Validation
- [x] Record counts logged at each stage (API → DataFrame → Merge → Output)
- [x] Warning logged when >20% of sites return no data

### Phase 2: Improved Logging
- [x] Structured logging at key points (request, response, processing, merge, output)
- [x] Logs include site codes, date ranges, record counts
- [x] Stage tags implemented: `[CONFIG]`, `[API]`, `[DATA]`, `[MERGE]`, `[OUTPUT]`, `[TIMING]`
- [x] Log level configurable: module-specific in `config.yaml` > system-wide in `.env` > default INFO
- [x] All debug print statements converted to proper `logger.debug()` calls
- [x] README updated with logging documentation

### Phase 3: Post-Write Validation
- [x] Validation runs after writing in maintenance mode
- [x] Sites missing data in last 3 days are reported
- [x] Site reliability statistics tracked over time in JSON file
- [x] Low reliability sites (<80%) are flagged
- [x] Validation configurable via config.yaml (enabled, max_age_days, threshold)

### Phase 4: Spot-Check Validation
- [x] Spot-check sites configurable via `.env` (`IEASYHYDRO_SPOTCHECK_SITES`)
- [x] Spot-check compares output values against direct API query
- [x] Mismatches are reported with both values

### Phase 5: Diagnostic Script (minimal changes)
- [x] Script documents expected runtime in header
- [x] Mode indicator shows which test is running at startup
- [~] Progress indicators already existed from prior development
- [~] Section headers already existed from prior development

### Phase 6: Site Caching
- [x] Site cache saved in maintenance mode
- [x] Site cache loaded in operational mode
- [x] Cache expiry configurable via config.yaml (max_age_days)
- [x] Site codes deduplicated before API requests
- [x] Operational mode warns if cache missing/stale, falls back to SDK

### Phase 7: All Forecast Site Selection
- [x] Sites with daily_forecast, monthly_forecast, or seasonal_forecast enabled are included
- [x] New `get_all_forecast_sites_from_HF_SDK()` function implemented in setup_library.py
- [x] `all_forecast_sites_from_iEH_HF_SDK()` and `virtual_all_forecast_sites_from_iEH_HF_SDK()` added to Site class
- [x] preprocessing_runoff uses unified function instead of combining pentad + decad
- [x] Virtual stations with any forecast flag also included
- [x] Cache format updated to v2 (unified site list instead of pentad/decad split)
- [x] Backward compatibility: v1 cache files are auto-converted to v2 format

### Phase 8: Package Updates & Deprecation Warnings
- [x] Dependencies reviewed - all at or above minimum versions (pandas 2.3.3, numpy 2.3.5, etc.)
- [x] Tests pass (31/31)
- [x] FutureWarning for DataFrameGroupBy.apply suppressed (full fix deferred)
- [~] ResourceWarnings in tests - not deprecation, low priority

### Phase 9: Code Architecture
- [x] Architecture review documented
- [x] Critical reliability issues fixed (empty DataFrame check, undefined variable, stale cache warning)
- [~] Refactoring deferred - module functional but src.py needs splitting in future

### General
- [x] All existing tests still pass (31/31)
- [~] README updated with new configuration options (site caching documented in Phase 6)

---

## Testing

1. Run preprocessing in maintenance mode, verify logs show counts
2. Delete rows from `runoff_day.csv`, run maintenance, verify gap filling works
3. Run multiple times, verify reliability statistics accumulate
4. Configure spot-check sites, verify validation runs and reports mismatches
5. Run diagnostic script, verify progress indicators and summary
6. Run full test suite after package updates
7. Verify sites with only daily/monthly/seasonal forecasts are now included in data retrieval

---

## Notes

- Spot-check sites should be chosen carefully - sites with consistent, reliable data
- Validation should warn but not fail by default (configurable)
- Keep diagnostic script runtime reasonable (<5 minutes for full run)
- Reliability statistics file should be in a data directory, not tracked in git
- Code architecture review creates follow-up issues, not immediate refactoring → see gi_PR-003
