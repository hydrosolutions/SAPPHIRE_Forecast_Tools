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

**Goal**: Structured logging at key points for easier debugging.

**Log points to add/improve**:

1. **API Request Stage**:
   - Site codes being requested (count + first few)
   - Date range requested
   - Variable name (WDDA/WDD)

2. **API Response Stage**:
   - Response status (success/error code)
   - Total count from API
   - Number of pages fetched
   - Sites with data vs without

3. **Data Processing Stage**:
   - Records before/after each transformation
   - Column names and dtypes

4. **Merge Stage**:
   - Records from Excel/cached data
   - Records from DB
   - Records after merge (new + updated + unchanged)

5. **Output Stage**:
   - Final record count
   - Date range in output
   - Sites in output

**Files to modify**:
- `apps/preprocessing_runoff/src/src.py`
- `apps/preprocessing_runoff/preprocessing_runoff.py`

---

### Phase 3: Post-Write Validation (Hindcast Mode)

**Goal**: Validate `runoff_day.csv` after writing and track site data reliability over time.

**Critical requirement**: Forecasts require data from the last 3 days. Missing recent data will cause forecast failures.

**Validation function** `validate_recent_data()`:

```python
def validate_recent_data(output_df, expected_sites, date_col, code_col):
    """
    Validate that each site has data within the last 3 days.

    Returns:
        dict with:
        - sites_with_recent_data: list of sites with data in last 3 days
        - sites_missing_recent_data: list of sites WITHOUT data in last 3 days
        - latest_date_per_site: dict of site_code -> latest date
    """
```

**Site reliability tracking**:

Track statistics over time to identify unreliable sites:

```python
# reliability_stats.json (persisted)
{
    "15013": {
        "checks": 30,           # Total validation runs
        "recent_data_present": 28,  # Times data was present in last 3 days
        "reliability_pct": 93.3,
        "last_gap_date": "2025-12-25"
    },
    "16100": {
        "checks": 30,
        "recent_data_present": 15,
        "reliability_pct": 50.0,
        "last_gap_date": "2025-12-30"
    }
}
```

**Output**:
```
=== Recent Data Validation ===
Sites checked: 63
Sites with data in last 3 days: 61 (96.8%)
Sites missing recent data: 2

MISSING RECENT DATA (forecast may fail):
  - 15013: Last data 2025-12-25 (reliability: 93%)
  - 16100: Last data 2025-12-20 (reliability: 50%)

LOW RELIABILITY SITES (< 80% over last 30 checks):
  - 16100: 50% reliability
  - 15292: 73% reliability
```

**Integration**:
- Call after `write_data_to_csv()` in maintenance mode
- Update reliability statistics file
- Log sites missing recent data
- Optionally fail run if critical sites missing data (configurable)

**Files to modify**:
- `apps/preprocessing_runoff/src/src.py` (add validation function)
- `apps/preprocessing_runoff/preprocessing_runoff.py` (call validation)
- New: `apps/preprocessing_runoff/data/reliability_stats.json` (tracked statistics)

---

### Phase 4: Spot-Check Validation

**Goal**: Verify data for known reliable sites matches direct API query.

**Configuration** (in `.env`):

```bash
# Spot-check validation sites (comma-separated site codes)
# These should be sites known to have reliable, recent data
IEASYHYDRO_SPOTCHECK_SITES_KGHM=15166,16159,15189
IEASYHYDRO_SPOTCHECK_SITES_TJHM=17462,17001
```

**Validation function** `spot_check_sites()`:

```python
def spot_check_sites(sdk, site_codes, output_df, date_col, code_col, value_col):
    """
    For each spot-check site:
    1. Fetch latest value directly from API
    2. Compare with value in output_df
    3. Report match/mismatch

    Returns:
        dict with site_code -> {api_value, output_value, match}
    """
```

**Configuration loading**:
- Read from environment variable based on organization
- Fall back to empty list if not configured
- Skip spot-check if no sites configured

**Files to modify**:
- `apps/preprocessing_runoff/src/src.py` (add spot-check function)
- `apps/preprocessing_runoff/preprocessing_runoff.py` (call spot-check)
- `.env` templates (add spot-check site configuration)

---

### Phase 5: Improve Diagnostic Test Script

**Goal**: Make `testspecial_sdk_data_retrieval.py` more usable and informative.

**Improvements**:

1. **Better documentation at top of file**:
   - What the script tests
   - Expected runtime (several minutes)
   - How to interpret results

2. **Progress indicators**:
   ```python
   print(f"\n[Step 1/5] Testing SDK connection...")
   print(f"[Step 2/5] Testing page_size limits... (this may take 30 seconds)")
   print(f"[Step 3/5] Testing bulk vs batched requests... (this may take 2-3 minutes)")
   ```

3. **Clear section headers**:
   ```
   ======================================================================
   STEP 3: BULK VS BATCHED REQUEST COMPARISON
   ======================================================================
   Purpose: Determine if bulk requests work or if batching is needed
   Expected: Either bulk succeeds, or fallback to batched/individual works
   ----------------------------------------------------------------------
   ```

4. **Summary at end**:
   ```
   ======================================================================
   FINAL SUMMARY
   ======================================================================
   SDK Connection:     OK
   Page Size Limit:    10 (larger values fail with 422)
   Bulk Requests:      FAILED (using batched fallback)
   Data Retrieved:     63/63 sites, 3,780 records
   Latest Data Date:   2025-12-30

   RECOMMENDATION: Use batch_size=10 for reliable data retrieval
   ======================================================================
   ```

5. **Configurable verbosity**:
   - `VERBOSE=1` for detailed output
   - Default: summary only

**Files to modify**:
- `apps/preprocessing_runoff/test/testspecial_sdk_data_retrieval.py`

---

### Phase 6: Site Caching for Performance

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

**Goal**: Update dependencies to latest compatible versions and fix deprecation warnings.

**Tasks**:
- [ ] Review `pyproject.toml` for outdated packages
- [ ] Update pandas, numpy, and other core dependencies
- [ ] Update ieasyhydro-sdk to latest version
- [ ] Run tests after updates to verify compatibility
- [ ] Update `uv.lock` file
- [ ] Fix deprecation warnings in preprocessing output (e.g., pandas FutureWarnings)

**Files to modify**:
- `apps/preprocessing_runoff/pyproject.toml`
- `apps/preprocessing_runoff/uv.lock`
- Any files producing deprecation warnings

---

### Phase 9: Code Architecture Review

**Goal**: Improve code structure and maintainability while working on this module.

**Areas to review**:

1. **Function organization in `src/src.py`**:
   - File is large (~2400 lines) - consider splitting into modules
   - Potential modules: `api_client.py`, `data_processing.py`, `validation.py`, `io.py`

2. **Error handling**:
   - Ensure consistent error handling patterns
   - Use custom exceptions for API errors vs data errors
   - Improve error messages with actionable information

3. **Configuration management**:
   - Review `config.py` and `config.yaml` usage
   - Ensure all configurable values are properly externalized
   - Document configuration options in README

4. **Code duplication**:
   - `get_daily_average_discharge_*` and `get_todays_morning_discharge_*` have similar patterns
   - Consider refactoring shared logic into helper functions

5. **Type hints**:
   - Add type hints to public functions
   - Consider using TypedDict for complex return types

6. **Docstrings**:
   - Ensure all public functions have docstrings
   - Use consistent docstring format (Google style)

**Proposed new structure** (for consideration):
```
apps/preprocessing_runoff/src/
├── __init__.py
├── src.py              # Main orchestration (reduced)
├── api_client.py       # iEasyHydro HF API interactions
├── data_processing.py  # DataFrame transformations
├── validation.py       # Data validation functions
├── io.py               # File reading/writing
└── config.py           # Configuration (existing)
```

**Note**: Full refactoring is out of scope for this issue. Document findings and create follow-up issues for significant changes.

**Files to review**:
- `apps/preprocessing_runoff/src/src.py`
- `apps/preprocessing_runoff/preprocessing_runoff.py`
- `apps/preprocessing_runoff/src/config.py`

---

## Acceptance Criteria

### Phase 1: Count Validation
- [ ] Record counts logged at each stage (API → DataFrame → Merge → Output)
- [ ] Warning logged when >20% of sites return no data

### Phase 2: Improved Logging
- [ ] Structured logging at key points (request, response, processing, merge, output)
- [ ] Logs include site codes, date ranges, record counts

### Phase 3: Post-Write Validation
- [ ] Validation runs after writing in maintenance mode
- [ ] Sites missing data in last 3 days are reported
- [ ] Site reliability statistics tracked over time in JSON file
- [ ] Low reliability sites (<80%) are flagged

### Phase 4: Spot-Check Validation
- [ ] Spot-check sites configurable via `.env` (separate for KGHM/TJHM)
- [ ] Spot-check compares output values against direct API query
- [ ] Mismatches are reported with both values

### Phase 5: Diagnostic Script
- [ ] Progress indicators with step numbers and estimated time
- [ ] Clear section headers explaining purpose of each test
- [ ] Final summary with recommendations
- [ ] Script documents expected runtime in header

### Phase 6: Site Caching
- [ ] Site cache saved in maintenance mode
- [ ] Site cache loaded in operational mode
- [ ] Cache expiry configurable via environment variable
- [ ] Site codes deduplicated before API requests
- [ ] Operational mode fails gracefully if cache missing

### Phase 7: All Forecast Site Selection
- [ ] Sites with daily_forecast, monthly_forecast, or seasonal_forecast enabled are included
- [ ] New `get_all_forecast_sites_from_HF_SDK()` function implemented
- [ ] preprocessing_runoff uses unified function instead of combining pentad + decad
- [ ] Virtual stations with any forecast flag also included

### Phase 8: Package Updates & Deprecation Warnings
- [ ] Dependencies updated to latest compatible versions
- [ ] Tests pass after updates
- [ ] Deprecation warnings in preprocessing output are fixed

### Phase 9: Code Architecture
- [ ] Architecture review documented
- [ ] Follow-up issues created for significant refactoring

### General
- [ ] All existing tests still pass
- [ ] README updated with new configuration options

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
- Code architecture review creates follow-up issues, not immediate refactoring
