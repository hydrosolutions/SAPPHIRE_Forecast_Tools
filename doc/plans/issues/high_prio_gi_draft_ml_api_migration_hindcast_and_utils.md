# ML module: complete API migration for hindcast, add_new_station, and initialize_ml_tool

**Status**: Draft
**Module**: machine_learning
**Priority**: High
**Labels**: `api-migration`, `csv-deprecation`

---

## Summary

Several scripts in `apps/machine_learning/` still use CSV-only I/O for reads and/or writes. All operational data must flow through the SAPPHIRE API (with CSV as deprecated fallback). This issue tracks migrating the remaining gaps.

## Context

`make_forecast.py` is the reference implementation — it reads meteo data via `read_meteo_data_combined()` (API-primary) and writes forecasts via `_write_ml_forecast_to_api()`. Three other scripts have not been migrated:

| Script | Meteo Read | Forecast/Hindcast Write |
|--------|:---:|:---:|
| `hindcast_ML_models.py` | CSV only (4 separate files) | CSV only |
| `add_new_station.py` | N/A (calls hindcast subprocess) | CSV only |
| `initialize_ml_tool.py` | N/A (calls hindcast subprocess) | CSV only |

## Problem

1. `hindcast_ML_models.py` reads ERA5 reanalysis and control member data from 4 separate CSV files (lines 270-335) instead of using `read_meteo_data_combined()` which reads from the meteo API table
2. `hindcast_ML_models.py` writes hindcast output to CSV only (line 531) — no API write
3. `add_new_station.py` writes merged forecasts to CSV only (lines 255, 286)
4. `initialize_ml_tool.py` writes initial forecasts to CSV only (lines 167, 180)

### Key Insight: The Meteo Table Simplifies ERA5 Reads

The meteo table contains **both** reanalysis (historical) and control member (operational forecast) data, unified in a single table. The `preprocessing_gateway` writes both data types to the same table via upsert on `(meteo_type, code, date)`. When dates overlap, control member data overwrites reanalysis.

This means the hindcast script's 3-step CSV process (read reanalysis → read control member → concat + dedup) is an artifact of the file-based approach. With the API, a single `read_meteo_data_combined(site_codes, start_date, end_date)` call returns the unified result.

---

## Design Decisions

- **Scaler CSVs** (`scaler_stats_discharge.csv`, `scaler_stats_era5.csv`, `scaler_stats_static.csv`) are model artifacts — stay as files
- **Static features CSV** — model artifact, stays as file
- **Model files** (`.pt`) — binary artifacts, stay as files
- **Flag values** 3 (NaN after hindcast) and 4 (valid after hindcast) pass through to the API as-is
- **CSV writes are kept** as deprecated fallback alongside new API writes

---

## Implementation Plan

### Phase 1: `hindcast_ML_models.py` — Migrate ERA5 reads to API

Replace the 4-file CSV read + concat + dedup block (lines ~270-335) with a single call to `read_meteo_data_combined()` from `scr/utils_ml_forecast.py`.

**Current code (lines 270-335):**
1. Reads `{HRU}_P_reanalysis.csv` + `{HRU}_T_reanalysis.csv` → merges on `[code, date]`
2. Reads `{HRU}_P_control_member.csv` + `{HRU}_T_control_member.csv` → merges on `[code, date]`
3. Concats reanalysis + control member vertically
4. Deduplicates on `[date, code]` keeping last

**New code:**
```python
era5_data_transformed = read_meteo_data_combined(
    site_codes=site_codes,
    start_date=start_date_str,
    end_date=end_date_str,
)
```

When called without `csv_path_t` / `csv_path_p`, the function reads from the meteo API table, which already contains unified reanalysis + control member data.

**Important:** The PET and daylight_hours computation (lines 372-377) remains unchanged — it operates on the resulting DataFrame regardless of source.

**Reference implementation:** `apps/machine_learning/make_forecast.py` — `load_control_member_data()` (lines 361-393) calls `read_meteo_data_combined()`.

**Files to modify:** `apps/machine_learning/hindcast_ML_models.py`

### Phase 2: `hindcast_ML_models.py` — Add API write for hindcast output

After the existing CSV write (line 531), add API write using `_write_ml_forecast_to_api()`. Keep CSV as deprecated fallback.

The hindcast output DataFrame already has compatible columns: `code`, `date`, `forecast_date`, `flag`, `Q5`, `Q25`, `Q50`, `Q75`, `Q95`.

**Files to modify:** `apps/machine_learning/hindcast_ML_models.py`
**Imports needed:** `_write_ml_forecast_to_api`, `SAPPHIRE_API_AVAILABLE` from `scr.utils_ml_forecast`

### Phase 3: `add_new_station.py` — Add API write

After merging hindcast with existing forecast and writing CSV (lines 255, 286), also write to API using `_write_ml_forecast_to_api()`.

**Files to modify:** `apps/machine_learning/add_new_station.py`
**Imports needed:** `_write_ml_forecast_to_api`, `SAPPHIRE_API_AVAILABLE` from `scr.utils_ml_forecast`

### Phase 4: `initialize_ml_tool.py` — Add API write

After creating initial forecast from hindcast and writing CSV (lines 167, 180), also write to API using `_write_ml_forecast_to_api()`.

**Files to modify:** `apps/machine_learning/initialize_ml_tool.py`
**Imports needed:** `_write_ml_forecast_to_api`, `SAPPHIRE_API_AVAILABLE` from `scr.utils_ml_forecast`

### Phase 5: Tests

Add/update tests for all modified scripts:
- Phase 1: mock `forecast_library.read_meteo_data()`, verify hindcast gets correct format (columns: `code`, `date`, `T`, `P`)
- Phase 2: mock `_write_ml_forecast_to_api`, verify called with hindcast output DataFrame
- Phase 3/4: mock API write, verify called after CSV write; verify API failure does not block CSV fallback

**Files:** `apps/machine_learning/test/`

---

## Risks

- **Date range coverage**: Hindcast needs historical data (potentially years). The API read must cover the full lookback period. Verify `read_meteo_data_combined()` pagination handles large date ranges.
- **API pagination**: `forecast_library.read_meteo_data()` paginates at 10,000 rows. For many stations × many years, verify this is sufficient.
- **Column name consistency**: CSV reads produce `T`, `P` columns. Verify `read_meteo_data_combined()` returns the same column names when reading from API.

## Out of Scope

- Deduplicating `call_hindcast_script()` across 4 files (code smell, separate cleanup)
- Investigating why `hindcast_ML_models.py` subprocess fails on production (separate issue, requires Sandro)
- Removing CSV reads/writes entirely (they remain as deprecated fallback during transition)
- Adding a `source` column to the meteo table to distinguish reanalysis from control member

## Dependency Graph

Phases 1→2 are sequential (same file). Phases 3 and 4 are independent. Phase 5 depends on all.

```json
{
  "phases": {
    "1": {
      "description": "hindcast_ML_models.py: replace 4-file CSV ERA5 reads with single read_meteo_data_combined() API call",
      "file": "apps/machine_learning/hindcast_ML_models.py",
      "depends_on": [],
      "risk": "medium — verify date range coverage and unified reanalysis+control_member data"
    },
    "2": {
      "description": "hindcast_ML_models.py: add API write for hindcast output after existing CSV write",
      "file": "apps/machine_learning/hindcast_ML_models.py",
      "depends_on": ["1"]
    },
    "3": {
      "description": "add_new_station.py: add API write after CSV writes at lines 255 and 286",
      "file": "apps/machine_learning/add_new_station.py",
      "depends_on": []
    },
    "4": {
      "description": "initialize_ml_tool.py: add API write after CSV writes at lines 167 and 180",
      "file": "apps/machine_learning/initialize_ml_tool.py",
      "depends_on": []
    },
    "5": {
      "description": "Tests: meteo API read format, API write calls, fallback behavior",
      "file": "apps/machine_learning/test/",
      "depends_on": ["1", "2", "3", "4"]
    }
  },
  "execution": {
    "parallel_group_1": ["1", "3", "4"],
    "sequential_after_group_1": ["2"],
    "sequential_after_2": ["5"]
  }
}
```
