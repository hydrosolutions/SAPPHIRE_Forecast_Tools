# PREPG-005: Meteo API write discards all forecast rows — only observations reach the database

**Status**: Review
**Module**: `preprocessing_gateway`
**Priority**: High
**Labels**: `bug`, `data-integrity`, `api-migration`, `meteo-data`

---

## Summary

`Quantile_Mapping_OP.py` downloads ECMWF IFS control member data (P, T) containing both ERA5 reanalysis/spinup and forecast rows for future dates. The API write function `_write_meteo_to_api()` filters to `yesterday <= date <= today` (line 309), explicitly discarding all forecast rows. Only the `[yesterday, today]` slice reaches the preprocessing API; forecast data is available only in CSVs.

Downstream consumers (`machine_learning`, `long_term_forecasting`) that read from the same preprocessing database never see forecast meteo data. The `machine_learning` module reads via the preprocessing API; `long_term_forecasting` reads via SQLAlchemy against the same PostgreSQL database. Both expect future-dated meteo rows to be present.

**Impact**: Both short-term ML and long-term forecasting models that need ECMWF IFS forecast P/T for future dates cannot get them from the database.

---

## Data Flow: Current vs. Intended

### Current behavior

```
DG get_control_spinup_and_forecast()
  → CSV: spinup (365 days) + forecast (~15 days)     ← full data ✓
  → API: _write_meteo_to_api() filters [yesterday, today]  ← forecast dropped ✗
```

The upper bound `data["date"] <= today` on line 309 of `_write_meteo_to_api()` removes all forecast rows (future dates). The same bug exists on line 320 (the fallback for unknown sync modes).

### Intended behavior

All freshly-fetched data (reanalysis + forecast) should be written to the API. The DB upsert on `(meteo_type, code, date)` ensures that on subsequent runs, newer observations naturally overwrite older forecast values for the same dates — same pattern as the snow fix in PREPG-003.

### Comparison with snow data flow (PREPG-003)

| Aspect | Snow (PREPG-003) | Meteo (this issue) |
|--------|-----------------|-------------------|
| Bug | Wall-clock anchor + DG lag → empty window | Upper bound `<= today` → forecasts dropped |
| DG data lag | 3-7 days | None |
| Forecast rows present in DG response? | Yes | Yes |
| Forecast rows written to API? | No (window misses them) | No (upper bound excludes them) |
| Fix pattern | Drop upper bound: `date >= yesterday` (one-sided) | Same — drop `<= today` upper bound |

### Execution-order dependency: QM → ERA5 extension

Both `Quantile_Mapping_OP.py` and `extend_era5_reanalysis.py` write to the same `(meteo_type, code, date)` keys using the same HRU codes. The pipeline runs them in order (`run_locally.sh:490`):

```
Quantile_Mapping_OP.py  →  extend_era5_reanalysis.py  →  snow_data_operational.py
```

QM writes forecast rows with `norm=None` (it has no climatological data). ERA5 extension then writes dashboard data for the full current year (Jan 1 – Dec 31) with both `value` and `norm`. Because ERA5 extension runs second, it overwrites QM's `norm=None` with actual norm values for all dates it covers — including the forecast dates.

**If ERA5 extension fails or is skipped**, forecast-dated rows remain in the DB with `norm=None`. This affects the dashboard (no climatological reference line for those dates) but does **not** affect downstream forecast consumers (`machine_learning`, `long_term_forecasting`), which only read `value`. This is an acceptable degradation — the forecast data is still available.

---

## Problem Statement

### The `date <= today` upper bound

In `Quantile_Mapping_OP.py`, `_write_meteo_to_api()` lines 306-309:

```python
today = pd.Timestamp.today().normalize()
yesterday = today - pd.Timedelta(days=1)
if sync_mode == "operational":
    data_to_write = data[(data["date"] >= yesterday) & (data["date"] <= today)]
```

The same two-sided filter appears in the fallback for unknown sync modes (line 320):

```python
else:
    logger.warning("Unknown sync mode '%s', defaulting to operational", sync_mode)
    data_to_write = data[(data["date"] >= yesterday) & (data["date"] <= today)]
```

The control member CSV (`control_spinup_and_forecast_{hru}_{date}.csv`) contains:
- ~365 days of ERA5 spinup/reanalysis (past)
- ~15 days of ECMWF IFS forecast (future)

The `date <= today` filter drops all forecast rows. The `date >= yesterday` filter drops all but the last 2 days of spinup. Only 1-2 rows per code are written.

### Downstream impact

| Consumer | Data source | Gets forecasts? |
|----------|------------|----------------|
| `machine_learning` | Preprocessing API (fail-fast, no CSV fallback when `SAPPHIRE_API_ENABLED=true`) | **No** — forecast rows missing from DB |
| `long_term_forecasting` | SQLAlchemy direct query against same PostgreSQL DB | **No** — forecast rows missing from DB |

The `machine_learning` module uses `utils_ml_forecast.read_meteo_data_combined()` → `fl.read_meteo_data()` which reads from the preprocessing API with no date filter, expecting all available data including future dates. When `SAPPHIRE_API_ENABLED=true` (default), there is no CSV fallback — API failure raises `SapphireAPIError` immediately.

The `long_term_forecasting` module reads from the same preprocessing PostgreSQL database via its `DataInterface` (SQLAlchemy), not via the REST API. It explicitly expects data up to `today + ECMWF_IFS_lead_time` days (default 15). Both modules are affected because the underlying database is the same — forecast rows are never written to it.

### Ensemble forecasts not written at all

The 50-member ECMWF ensemble forecasts are written to CSV only (`{code}_P_ensemble_forecast.csv`, `{code}_T_ensemble_forecast.csv`). They are **never** written to the API. This is a separate concern — the ensemble data has an `ensemble_member` dimension that the current meteo API schema doesn't support. This issue focuses on the control member only.

---

## Proposed Fix

### Fix 1 (Required): Remove the upper bound in operational mode and fallback

**File**: `apps/preprocessing_gateway/Quantile_Mapping_OP.py`
**Function**: `_write_meteo_to_api()`, lines 309 and 320

The fix mirrors the proven PREPG-003 snow pattern in `dg_utils.py:write_snow_to_api()` (line 505). Change both the operational filter and the fallback from a two-sided window to a one-sided filter:

```python
# Before (bug — line 309):
if sync_mode == "operational":
    data_to_write = data[(data["date"] >= yesterday) & (data["date"] <= today)]

# After (fix — matches snow pattern):
if sync_mode == "operational":
    # Include yesterday, today, and any forecast dates beyond today
    data_to_write = data[data["date"] >= yesterday]

# Before (bug — line 320, fallback):
else:
    data_to_write = data[(data["date"] >= yesterday) & (data["date"] <= today)]

# After (fix — same one-sided filter):
else:
    data_to_write = data[data["date"] >= yesterday]
```

This drops the `<= today` upper bound so ECMWF IFS forecast rows (future dates) pass through. The DB upsert on `(meteo_type, code, date)` ensures that on subsequent runs, observation values overwrite previously-written forecast values — same proven pattern as snow.

The write payload grows from ~2 rows to ~17 rows per code (yesterday + today + ~15 forecast days), which is negligible.

### Fix 2 (Required): Update `_check_meteo_consistency()` for forecast dates

**File**: `apps/preprocessing_gateway/Quantile_Mapping_OP.py`
**Function**: `_check_meteo_consistency()`, lines 413-416 and 448-452

The consistency check has two date-windowed operations that must match the write:

1. **CSV filter** (line 416): `csv_recent = csv_data[(csv_data["date"] >= yesterday) & (csv_data["date"] <= today)]`
   → Change to: `csv_recent = csv_data[csv_data["date"] >= yesterday]`

2. **API read** (line 448-452): `client.read_meteo(..., start_date=yesterday, end_date=today)`
   → Change to: `client.read_meteo(..., start_date=yesterday)` (drop `end_date` or set to max forecast date)

Without this, the consistency check would pass (checking only [yesterday, today]) while forecast rows could silently fail to write.

---

## Implementation Plan

### Phase 1: Remove upper bound in operational meteo write

**Goal**: Allow forecast rows through the operational date filter.
**File**: `apps/preprocessing_gateway/Quantile_Mapping_OP.py`, `_write_meteo_to_api()`
**Changes**:
- Line 309: Drop `& (data["date"] <= today)` from the operational filter
- Line 320: Drop `& (data["date"] <= today)` from the fallback filter
**Acceptance**: Function writes ~17 rows per code (yesterday + today + ~15 forecast days) instead of ~2.

### Phase 2: Update consistency check

**Goal**: Verify forecast rows reach the API, not just [yesterday, today].
**File**: Same file, `_check_meteo_consistency()`
**Changes**:
- Line 416: Change CSV filter to one-sided: `csv_data["date"] >= yesterday`
- Lines 448-452: Drop `end_date=today` from the `client.read_meteo()` call (or set `end_date` to `csv_recent["date"].max()`)
**Acceptance**: Consistency check verifies forecast-dated rows are present in API.

### Phase 3: Update existing tests

**Goal**: Fix tests that assert the old 2-row operational behavior.
**Files**: `apps/preprocessing_gateway/test/test_api_coverage_gaps.py`, `apps/preprocessing_gateway/test/test_api_integration.py`
**Changes**:
- `test_api_coverage_gaps.py:TestQMWritesRecentDays.test_qm_writes_yesterday_and_today` (line 71): Update test data to include future dates, assert that records include forecast rows (not just yesterday+today)
- `test_api_coverage_gaps.py:TestQMWritesRecentDays.test_qm_skips_when_recent_days_not_in_data` (line 116): This test uses data with only tomorrow — after the fix, this should assert `result=True` (forecast row is written), not `result=False`
- `test_api_integration.py:TestQMMeteoAPIWrite` (line 1087): Update assertions that expect exactly 2 records in operational mode
- `test_api_integration.py:TestQMMeteoAPISyncMode` (line 2178): Update operational-mode assertions
**Acceptance**: All existing tests pass with updated assertions reflecting the new behavior.

### Phase 4: Add new tests for forecast meteo API write

**Goal**: Cover the new forecast-write behavior and edge cases.
**File**: `apps/preprocessing_gateway/test/test_api_coverage_gaps.py` (extend existing file, don't create new)
**Tests**:
1. Creates mock control member data with spinup (past) and forecast (future) rows — verifies API write includes forecast-dated rows
2. Verifies that `norm=None` is set for all QM-written records (QM has no climatological data)
3. Verifies fallback (unknown sync mode) also includes forecast rows
**Acceptance**: New tests pass, covering the forecast-write path.

### Phase 5: Verify end-to-end

1. Run `Quantile_Mapping_OP.py` locally
2. Query the preprocessing API: `curl "http://localhost:8000/api/preprocessing/meteo/?meteo_type=T&code=15189&limit=20"` — should now include rows with future dates (forecast)
3. Verify that on the next run, observation values overwrite previously-written forecast values for overlapping dates
4. Run `machine_learning` and confirm it reads forecast meteo from the API
5. Run `long_term_forecasting` and confirm it reads forecast meteo from the database (same PostgreSQL, accessed via SQLAlchemy)

---

## CSV/API Consistency After Fix

| Data slice | CSV | API (operational) before fix | API (operational) after fix |
|------------|-----|-----------------------------|-----------------------------|
| Spinup (~365 days past) | Full | Last 2 days only | Last 2 days only — **intentional** |
| Forecast (~15 days future) | Full | **Missing** | **Present** |
| Norms | Not in CM CSV | Written by ERA5 ext | Written by ERA5 ext — unchanged |

**Intentional remaining gap**: In operational mode, only yesterday+today of historical spinup reach the API. The CSV has the full ~365-day spinup. This is by design — operational mode is incremental. Use `mode="initial"` or `mode="maintenance"` to backfill historical data.

---

## Acceptance Criteria

- [x] `_write_meteo_to_api()` writes both recent observations and forecast rows to the API
- [x] The `date <= today` upper bound is removed from both the operational filter (line 309) and the fallback filter (line 320)
- [x] Forecast-dated meteo records (P, T) appear in the preprocessing API *(verified: forecast values through 2026-04-07 for code 15013)*
- [x] On subsequent runs, observation values overwrite previously-written forecast values (upsert verified) *(confirmed: ERA5 extension overwrote QM's norm=None with real norms on same keys)*
- [x] Existing norms are not clobbered (ERA5 extension runs after QM and restores norms)
- [x] Consistency check covers the same date range as the write (one-sided filter + wider API read)
- [x] Existing tests updated to reflect new behavior (no assertion failures)
- [x] No changes to `sapphire/services/` (ownership boundary respected)

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| ERA5 extension fails → forecast rows have `norm=None` | Low | Cosmetic (dashboard only) | Downstream consumers read `value` only; `norm=None` does not affect forecasts |
| Execution order changes (ERA5 runs before QM) | Very low | `norm` values overwritten with `None` | Pipeline order is defined in `run_locally.sh:490`; document dependency |
| Upsert overwrites all fields including `norm` | By design | N/A | This is the intended behavior — last writer wins, same as snow pattern |
| Write payload increase (~2 → ~17 rows) | N/A | Negligible | ~15 extra rows per code per type per run |

---

## Out of Scope

- **Ensemble forecast API integration**: The 50-member ensemble data has an `ensemble_member` dimension not supported by the current meteo API schema. This requires a schema change and is tracked separately.
- **Lead time / source field**: The meteo API schema has no field to distinguish observation from forecast. This is acceptable — the upsert overwrite cycle means the latest data always wins, same as the snow pattern.
- **Historical spinup backfill in operational mode**: Operational mode intentionally writes only recent data (yesterday onward). The full 365-day spinup is available via `mode="initial"` or `mode="maintenance"`.

---

## Related Issues

- **PREPG-003**: Snow operational API write — same class of bug (forecast data not reaching API), fix pattern is identical
- **PREPG-002**: Snow SWE data not updated — sibling observation

---

## Source

Identified 2026-03-24 while investigating PREPG-003 (snow API write). Comparison of `_write_meteo_to_api()` with `write_snow_to_api()` revealed the same pattern: forecast rows are present in the DG response but excluded from the API write by date windowing.

---

## Dependency Graph

```json
{
  "phases": {
    "phase_1": {
      "title": "Remove upper bound in operational meteo write (lines 309 + 320)",
      "file": "apps/preprocessing_gateway/Quantile_Mapping_OP.py",
      "changes": [
        "Drop date <= today upper bound from operational filter (line 309)",
        "Drop date <= today upper bound from fallback filter (line 320)",
        "Mirror snow pattern: data[data['date'] >= yesterday]"
      ],
      "depends_on": [],
      "parallel_with": ["phase_2"]
    },
    "phase_2": {
      "title": "Update consistency check window",
      "file": "apps/preprocessing_gateway/Quantile_Mapping_OP.py",
      "changes": [
        "Change CSV filter to one-sided: csv_data['date'] >= yesterday (line 416)",
        "Drop end_date from client.read_meteo() call (lines 448-452)"
      ],
      "depends_on": [],
      "parallel_with": ["phase_1"]
    },
    "phase_3": {
      "title": "Update existing tests for new behavior",
      "files": [
        "apps/preprocessing_gateway/test/test_api_coverage_gaps.py",
        "apps/preprocessing_gateway/test/test_api_integration.py"
      ],
      "changes": [
        "Update assertions expecting 2 records to expect ~17 (yesterday + today + forecast)",
        "Update test_qm_skips_when_recent_days_not_in_data to expect result=True",
        "Update TestQMMeteoAPIWrite and TestQMMeteoAPISyncMode operational assertions"
      ],
      "depends_on": ["phase_1", "phase_2"],
      "parallel_with": ["phase_4"]
    },
    "phase_4": {
      "title": "Add new tests for forecast meteo API write",
      "file": "apps/preprocessing_gateway/test/test_api_coverage_gaps.py",
      "changes": [
        "Test forecast rows included in operational write",
        "Test norm=None for all QM-written records",
        "Test fallback filter also includes forecast rows"
      ],
      "depends_on": ["phase_1"],
      "parallel_with": ["phase_3"]
    },
    "phase_5": {
      "title": "Verify fix end-to-end",
      "depends_on": ["phase_3", "phase_4"],
      "parallel_with": []
    }
  },
  "execution_groups": [
    {"group": 1, "parallel": true, "phases": ["phase_1", "phase_2"]},
    {"group": 2, "parallel": true, "phases": ["phase_3", "phase_4"]},
    {"group": 3, "parallel": false, "phases": ["phase_5"]}
  ]
}
