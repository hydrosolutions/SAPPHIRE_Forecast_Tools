# PREPG-005: Meteo API write discards all forecast rows — only observations reach the database

**Status**: Draft
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

The upper bound `data["date"] <= today` on line 309 of `_write_meteo_to_api()` removes all forecast rows (future dates).

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

### Fix 1 (Required): Remove the upper bound in operational mode

**File**: `apps/preprocessing_gateway/Quantile_Mapping_OP.py`
**Function**: `_write_meteo_to_api()`, line 309

The fix mirrors the proven PREPG-003 snow pattern in `dg_utils.py:write_snow_to_api()` (line 505). Change the operational filter from a two-sided window to a one-sided filter:

```python
# Before (bug):
if sync_mode == "operational":
    data_to_write = data[(data["date"] >= yesterday) & (data["date"] <= today)]

# After (fix — matches snow pattern):
if sync_mode == "operational":
    # Include yesterday, today, and any forecast dates beyond today
    data_to_write = data[data["date"] >= yesterday]
```

This drops the `<= today` upper bound so ECMWF IFS forecast rows (future dates) pass through. The DB upsert on `(meteo_type, code, date)` ensures that on subsequent runs, observation values overwrite previously-written forecast values — same proven pattern as snow.

The write payload grows from ~2 rows to ~17 rows per code (yesterday + today + ~15 forecast days), which is negligible.

### Fix 2 (Recommended): Update `_check_meteo_consistency()` for forecast dates

**File**: `apps/preprocessing_gateway/Quantile_Mapping_OP.py`
**Lines**: 413-416

The consistency check also uses `[yesterday, today]` window. Once forecasts are written, the check should cover the same date range as the write.

---

## Implementation Plan

### Phase 1: Remove upper bound in operational meteo write

**File**: `apps/preprocessing_gateway/Quantile_Mapping_OP.py`, `_write_meteo_to_api()` line 309
**Change**: Drop `& (data["date"] <= today)` from the operational filter, matching the snow pattern in `dg_utils.py:505`.

### Phase 2: Update consistency check

**File**: Same file, `_check_meteo_consistency()`
**Change**: Align the verification window with the write window.

### Phase 3: Verify

1. Run `Quantile_Mapping_OP.py` locally
2. Query the preprocessing API: `curl "http://localhost:8000/api/preprocessing/meteo/?meteo_type=T&code=15189&limit=20"` — should now include rows with future dates (forecast)
3. Verify that on the next run, observation values overwrite previously-written forecast values for overlapping dates
4. Run `machine_learning` and confirm it reads forecast meteo from the API
5. Run `long_term_forecasting` and confirm it reads forecast meteo from the database (same PostgreSQL, accessed via SQLAlchemy)

### Phase 4: Tests

Add a unit test in `apps/preprocessing_gateway/test/` that:
1. Creates mock control member data with spinup (past) and forecast (future) rows
2. Verifies that the API write includes forecast-dated rows
3. Simulates a second run where observations cover previously-forecast dates — asserts upsert overwrites correctly

---

## Acceptance Criteria

- [ ] `_write_meteo_to_api()` writes both recent observations and forecast rows to the API
- [ ] The `date <= today` upper bound is removed (or bypassed via `mode="initial"`)
- [ ] Forecast-dated meteo records (P, T) appear in the preprocessing API
- [ ] On subsequent runs, observation values overwrite previously-written forecast values (upsert verified)
- [ ] Existing norms are not clobbered
- [ ] Consistency check covers the same date range as the write
- [ ] No changes to `sapphire/services/` (ownership boundary respected)

---

## Out of Scope

- **Ensemble forecast API integration**: The 50-member ensemble data has an `ensemble_member` dimension not supported by the current meteo API schema. This requires a schema change and is tracked separately.
- **Lead time / source field**: The meteo API schema has no field to distinguish observation from forecast. This is acceptable — the upsert overwrite cycle means the latest data always wins, same as the snow pattern.

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
      "title": "Remove upper bound in operational meteo write",
      "file": "apps/preprocessing_gateway/Quantile_Mapping_OP.py",
      "changes": [
        "Drop date <= today upper bound from operational filter in _write_meteo_to_api()",
        "Mirror snow pattern: data[data['date'] >= yesterday]"
      ],
      "depends_on": [],
      "parallel_with": ["phase_2"]
    },
    "phase_2": {
      "title": "Update consistency check window",
      "file": "apps/preprocessing_gateway/Quantile_Mapping_OP.py",
      "changes": ["Align _check_meteo_consistency date range with write range"],
      "depends_on": [],
      "parallel_with": ["phase_1"]
    },
    "phase_3": {
      "title": "Verify fix end-to-end",
      "depends_on": ["phase_1", "phase_2"],
      "parallel_with": ["phase_4"]
    },
    "phase_4": {
      "title": "Add unit tests for forecast meteo API write",
      "file": "apps/preprocessing_gateway/test/test_meteo_api_forecast.py",
      "depends_on": ["phase_1"],
      "parallel_with": ["phase_3"]
    }
  },
  "execution_groups": [
    {"group": 1, "parallel": true, "phases": ["phase_1", "phase_2"]},
    {"group": 2, "parallel": true, "phases": ["phase_3", "phase_4"]}
  ]
}
```
