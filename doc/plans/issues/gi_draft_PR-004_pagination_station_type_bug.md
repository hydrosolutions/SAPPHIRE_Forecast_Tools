# GI PR-004: Fix Pagination Bug - Same Site Returns Different station_type Across Pages

## Problem Statement

When fetching discharge data from iEasyHydro HF API, sites that have **both** hydrological and meteorological sensors can appear on different pages with different `station_type` values. The current implementation processes each page independently and filters by `station_type='hydro'`, causing data loss when a site's hydro data appears on a different page than expected.

### Evidence

**Spot-check validation (2026-01-13)**:
```
Spot-check 16059: OK - value=181.0 (date: 2025-12-30)
```

Yet during bulk fetch, the same site was classified as "meteo-only":
```
[DATA] process_hydro_HF_data: Meteo-only site codes: [..., '16059', ...]
```

The spot-check (direct single-site API query) confirms the site **does have discharge data (WDDA)**, but during paginated bulk fetch it was misclassified.

### Root Cause

The iEasyHydro HF API returns paginated results where:
1. Each page contains 10 site-result objects (API limit)
2. A site can appear **multiple times** across pages with different `station_type`:
   - Once with `station_type='hydro'` (discharge data)
   - Once with `station_type='meteo'` (temperature, precipitation, etc.)
3. The `count` in API response is total station-result objects, not unique sites
   - Example: 62 sites requested → `count=100` returned (some sites appear twice)

### Current Behavior (Bug)

In `process_hydro_HF_data()` at `src/src.py`, classification happens **per page**:

```python
# Classification only considers current page
meteo_only = meteo_sites - hydro_sites  # BUG: per-page, not global!
```

If a site appears as `meteo` on page 3 but as `hydro` on page 7, page 3's processing incorrectly classifies it as "meteo-only" and discards any data it might have.

### Impact

1. **Data Loss**: Discharge data for mixed hydro/meteo sites may be partially or fully lost
2. **Inconsistent Results**: Different runs may get different data depending on page ordering
3. **Misleading Logs**: Sites incorrectly flagged as "meteo-only" when they do have discharge data
4. **Forecast Failures**: Missing discharge data causes forecasts to fail or use stale values

---

## Proposed Solution

### Aggregate All Pages Before Classification

Modify the pagination logic to:
1. Fetch **all pages** and collect raw results
2. Classify sites based on **complete data across all pages**
3. A site is "meteo-only" only if it has NO hydro records across ALL pages
4. Keep discharge records for any site that has hydro data **anywhere**

**Advantages**:
- Simple conceptual change - aggregate before filter, not filter per page
- Accurate classification based on complete data
- No additional API calls needed
- Minimal code changes

**Note**: The SDK does not support filtering by `station_type` in the request - it's only available in the response. Therefore, we must handle this in post-processing.

---

## Implementation Plan

### Phase 1: Restructure Page Aggregation ✅ COMPLETE

**Goal**: Collect all page results before calling `process_hydro_HF_data()`.

**Old flow** (buggy):
```
Page 1 → process_hydro_HF_data() → DataFrame 1
Page 2 → process_hydro_HF_data() → DataFrame 2
...
Concatenate DataFrames
```

**New flow** (fixed):
```
Page 1 → collect raw results
Page 2 → collect raw results
...
All results → process_hydro_HF_data() → Single DataFrame
```

**Changes made to `src/src.py`**:

1. Modified `fetch_and_format_hydro_HF_data()` (lines 1272-1422):
   - Collect all raw page results into `all_raw_results` list
   - `fetch_page_raw()` now returns raw results instead of processing them
   - Call `process_hydro_HF_data()` once with combined `{'results': all_raw_results}`
   - Parallel page fetching still works (no performance regression)

**Files modified**:
- `apps/preprocessing_runoff/src/src.py`

### Phase 2: Improve Classification Logging ✅ COMPLETE

**Goal**: Make the classification logging more accurate and informative.

**Changes made to `process_hydro_HF_data()` (lines 929-1007)**:
1. Log total unique sites across all pages
2. Log breakdown: hydro-only + dual-type + meteo-only = total
3. Log dual-type sites at DEBUG level (sites with BOTH hydro and meteo data)
4. Log meteo-only sites at INFO level (these truly have no discharge data)
5. Updated docstring to explain classification logic

**New log format**:
```
[DATA] Site classification across ALL pages: 62 unique sites = 50 hydro-only + 8 dual-type + 4 meteo-only
[DATA] Meteo-only sites (no discharge data): ['site1', 'site2', ...]
```

**Files modified**:
- `apps/preprocessing_runoff/src/src.py`

### Phase 3: Add Tests (OPTIONAL)

**Goal**: Ensure the fix works and prevent regression.

**Test cases** (can be added if regressions occur):
1. Site appears as hydro on page 1, meteo on page 2 → should keep hydro data
2. Site appears as meteo on page 1, hydro on page 2 → should keep hydro data
3. Site appears as meteo only across all pages → should be classified as meteo-only
4. Site appears as hydro only across all pages → should keep all data

**Files to add/modify**:
- `apps/preprocessing_runoff/test/`

**Status**: All 31 existing tests pass with the fix. Specific pagination tests are optional unless regressions are observed.

---

## Acceptance Criteria

- [x] Sites with discharge data are never misclassified as "meteo-only" (fixed via aggregation)
- [ ] Spot-check sites have consistent data in output (needs local test)
- [x] Logging accurately reports which sites truly have no discharge data
- [x] Dual-type sites (both hydro and meteo) are logged for awareness
- [x] No performance regression (parallel page fetching still works)
- [x] All existing tests pass (31/31)
- [ ] New tests cover the pagination edge case (optional, can add if needed)

---

## Testing

1. **Before fix**: Run maintenance mode, note sites classified as "meteo-only"
2. **After fix**: Same sites should NOT be classified as "meteo-only" if they have discharge data
3. **Spot-check validation**: All spot-check sites should pass (value and date match)
4. **Compare record counts**: Verify more records are retrieved for previously misclassified sites
5. **Performance**: Verify runtime is not significantly increased

---

## Related Issues

- **GI PR-002**: iEasyHydro HF Data Retrieval Validation
  - Phase 0, Finding 7: "API Returns Both Hydro AND Meteo Records"
  - Documents the dual station_type behavior but didn't anticipate pagination edge case

---

## Notes

- This bug was discovered through production monitoring (2026-01-13)
- The spot-check validation feature (PR-002) helped identify the discrepancy
- The API behavior (returning both station types) is documented but the pagination edge case was not anticipated
- Consider reporting this API behavior to iEasyHydro HF maintainers for documentation
- A separate issue exists with server vs local data freshness (server shows data up to 2025-12-13, local shows 2026-01-13) - this is unrelated to the pagination bug
