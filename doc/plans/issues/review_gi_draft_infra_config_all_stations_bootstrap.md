# INFRA-010: Bootstrap config_all_stations_library.json from HF SDK

**Status**: Implemented (all phases verified 2026-03-13)
**Module**: infra (cross-module: preprocessing_runoff, iEasyHydroForecast)
**Priority**: High
**Labels**: `bug`, `cross-module`, `configuration`

---

## Summary

`config_all_stations_library.json` is required by linear_regression (and any module
using `get_pentadal_forecast_sites_complicated_method`) but is never generated when
iEH SDK is unavailable. preprocessing_runoff, which runs first and has access to
iEH HF SDK, should populate this file so downstream modules can use it.

## Root Cause

`get_all_forecast_sites_from_HF_SDK()` (setup_library.py:1236) fetches sites and writes
`config_file_station_selection` (the simple `{"stationsID": [...]}` file) but never writes
`config_all_stations_library.json` (the full metadata file). The metadata write only
happens inside `get_pentadal_forecast_sites_complicated_method()` (setup_library.py:840-873)
which requires iEH SDK access (`backend_has_access_to_db=True`).

When `ieasyhydroforecast_connect_to_iEH=False` (default), linear_regression calls
`get_pentadal_forecast_sites()` → `get_pentadal_forecast_sites_complicated_method()` →
reads config file → empty → 0 sites → crash.

## Solution: Option A — write config from HF SDK path

### Key Finding: Site objects have all needed metadata

The `Site` objects returned by `get_all_forecast_sites_from_HF_SDK()` contain:
- `code`, `name_nat` (name in national language → maps to `name_ru`)
- `lat`, `lon` (→ maps to `long`)
- `basin_nat` (→ `basin`), `region_nat` (→ `region`)
- `river_name_nat` (→ `river_ru`), `punkt_name_nat` (→ `punkt_ru`)
- `site_type`, `iehhf_site_id` (→ `id`)

Fields NOT in Site objects (set to defaults):
- `elevation` → `None`
- `organization_id` → `None`
- `country` → `None`
- `is_virtual` → `False` (or `True` for virtual sites)

This is sufficient — consumers only require `name_ru`, `lat`, `long`, `code`.

---

## Implementation Plan

### Phase 1: Extract config write helper (setup_library.py)

**Goal**: Extract the config-write logic from `get_pentadal_forecast_sites_complicated_method()`
into a reusable function that can be called from both SDK paths.

**Cross-reference**: INFRA-009 Phase 1a will subsequently add an `organization`
field to the dict returned by `_sites_to_config_dict()`. Ensure the extracted
function is easy to extend with additional fields.

**File**: `apps/iEasyHydroForecast/setup_library.py`

**New function** `write_config_all_stations(fc_sites, config_all_file=None)`:

```python
def write_config_all_stations(fc_sites: list, config_all_file: str | None = None) -> str:
    """
    Write station metadata from Site objects to config_all_stations_library.json.

    Converts a list of Site objects to the expected JSON format with list-wrapped
    values, preserves manual site entries, backs up the existing file, and writes.

    Args:
        fc_sites: List of Site objects with metadata (code, lat, lon, etc.).
        config_all_file: Path to the config file. If None, resolved from env vars.

    Returns:
        Path to the written config file.
    """
```

**Implementation details**:
1. Resolve `config_all_file` from env vars if not provided (lines 742-745 pattern)
2. Build a dict from Site objects → JSON format:
   - Key: `str(site.code)`
   - Values: list-wrapped fields matching the schema:
     `code`, `name_ru`, `lat`, `long`, `basin`, `region`, `river_ru`, `punkt_ru`,
     `site_type`, `id`, `header`, `elevation`, `organization_id`, `country`, `is_virtual`
   - Map: `site.name_nat` → `name_ru`, `site.lon` → `long`, `site.basin_nat` → `basin`,
     `site.region_nat` → `region`, `site.river_name_nat` → `river_ru`,
     `site.punkt_name_nat` → `punkt_ru`, `site.iehhf_site_id` → `id`
3. Call `_read_manual_entries_from_config()` to preserve manual entries
4. Merge manual entries (same collision check as lines 830-838)
5. Back up existing file (line 866-867 pattern)
6. Write pretty-printed JSON (line 870-873 pattern)

**Also**: Refactor `get_pentadal_forecast_sites_complicated_method()` lines 840-873 to
call this new function instead of duplicating the write logic. The refactor converts
the existing `db_sites` DataFrame to a list of pseudo-Site objects or passes the
DataFrame directly (internal helper overload).

**Design decision**: Two approaches for the refactor:
- **(a)** Make `write_config_all_stations` accept either `list[Site]` or `pd.DataFrame`
  with a type check. Simpler but less clean.
- **(b)** Keep the DataFrame write as an internal `_write_config_all_from_dataframe()`
  and have `write_config_all_stations()` as the public Site-based API. The existing
  `get_pentadal_forecast_sites_complicated_method` calls the internal one.

**Recommended**: Option (b) — two internal functions, one public entry point.

```python
def write_config_all_stations(fc_sites: list, config_all_file: str | None = None) -> str:
    """Public API: writes config from Site objects."""
    stations_dict = _sites_to_config_dict(fc_sites)
    _write_config_all(stations_dict, config_all_file)

def _sites_to_config_dict(fc_sites: list) -> dict:
    """Convert Site objects to config_all JSON dict."""
    stations = {}
    for site in fc_sites:
        stations[str(site.code)] = {
            "code": [int(site.code)],
            "name_ru": [site.name_nat or site.name or ""],
            "lat": [site.lat],
            "long": [site.lon],
            "basin": [site.basin_nat or site.basin or ""],
            "region": [site.region_nat or site.region or ""],
            "river_ru": [site.river_name_nat or site.river_name or ""],
            "punkt_ru": [site.punkt_name_nat or site.punkt_name or ""],
            "site_type": [getattr(site, "site_type", "")],
            "id": [getattr(site, "iehhf_site_id", None)],
            "header": [str(site.code)],
            "elevation": [None],
            "organization_id": [None],
            "country": [None],
            "is_virtual": [getattr(site, "is_virtual", False)],
            "data_source": ["ieh_hf"],
        }
    return stations

def _write_config_all(stations_dict: dict, config_all_file: str | None = None) -> str:
    """Write stations dict + manual entries to config file."""
    if config_all_file is None:
        config_all_file = os.path.join(
            os.getenv("ieasyforecast_configuration_path"),
            os.getenv("ieasyforecast_config_file_all_stations"),
        )
    manual_entries = _read_manual_entries_from_config()
    # Collision check
    if manual_entries:
        for code in list(manual_entries.keys()):
            if code in stations_dict:
                logger.warning(f"Site {code} in both SDK and manual config — preferring SDK.")
                del manual_entries[code]
    json_dict = {"stations_available_for_forecast": stations_dict}
    if manual_entries:
        json_dict["stations_available_for_forecast"].update(manual_entries)
        logger.info(f"Preserved {len(manual_entries)} manual site(s): {list(manual_entries.keys())}")
    # Backup and write
    if os.path.exists(config_all_file):
        shutil.copy2(config_all_file, config_all_file + ".bak")
    with open(config_all_file, "w", encoding="utf-8") as f:
        json.dump(json_dict, f, ensure_ascii=False, indent=4)
    logger.info(f"Wrote {len(stations_dict)} station(s) to {config_all_file}")
    return config_all_file
```

**Refactor existing write** (lines 840-873): Replace with:
```python
# Build stations_dict from db_sites DataFrame (existing format)
stations_dict = {}
for _, row in db_sites.iterrows():
    code_str = str(row["site_code"])
    stations_dict[code_str] = {
        "code": [int(row["site_code"])],
        "name_ru": [row.get("site_name", "")],
        "lat": [row.get("latitude", None)],
        "long": [row.get("longitude", None)],
        ...  # all other fields from existing logic
    }
_write_config_all(stations_dict, config_all_file)
```

### Phase 2: Call config write from preprocessing_runoff

**Status**: **Already implemented.** `preprocessing_runoff.py` line 319 (in the HF SDK fetch block) already
calls `sl.write_config_all_stations(fc_sites)` inside the `if not cache_used:`
block after the HF SDK fetch. No changes needed.

**Original goal** (for reference): After `get_all_forecast_sites_from_HF_SDK()`
returns, write the config file. This is already done at line 319.

### Phase 3: Fix numpy truth-value bug (setup_library.py)

**Goal**: Prevent `ValueError: The truth value of an empty array is ambiguous`.

**File**: `apps/iEasyHydroForecast/setup_library.py`

**Fix at `get_pentadal_forecast_sites_complicated_method()` (near line 963)**: Convert numpy array to list:
```python
# Before:
site_codes = db_sites["site_code"].unique()

# After:
site_codes = db_sites["site_code"].unique().tolist()
```

This ensures `site_codes` is always a Python list, which has unambiguous truthiness.
The return type at `get_pentadal_forecast_sites_complicated_method()` (near line 1008) then matches the docstring ("list of strings").

**Secondary check**: Verify `get_pentadal_and_decadal_data()` in forecast_library.py
(line 1255) doesn't need a separate fix. With `site_codes` as a list, the `or []`
pattern works correctly. But add a defensive guard anyway:

```python
# In forecast_library.py, get_pentadal_and_decadal_data():
# Convert to plain lists to avoid numpy truth-value ambiguity
pentad_codes = list(site_list_pentad) if site_list_pentad is not None and len(site_list_pentad) > 0 else []
decad_codes = list(site_list_decad) if site_list_decad is not None and len(site_list_decad) > 0 else []
all_site_codes = list(set(pentad_codes + decad_codes))
```

### Phase 4: Tests

**Goal**: Cover the new function and the integration path.

#### 4a: Unit tests for `write_config_all_stations` and `_write_config_all`

**File**: `apps/iEasyHydroForecast/tests/test_setup_library.py`

**New test class**: `TestWriteConfigAllStations`

Tests:
1. **test_writes_valid_json_from_site_objects** — Create 2 mock Site objects, call
   `write_config_all_stations(sites, tmp_file)`, read back, verify schema and values.
2. **test_preserves_manual_entries** — Write a config with a manual entry
   (`data_source: ["google_sheets"]`), call with SDK sites, verify manual entry preserved.
3. **test_sdk_collision_removes_manual** — Manual entry has same code as an SDK site →
   SDK wins, manual entry removed.
4. **test_backs_up_existing_file** — Existing config → backup created at `.bak`.
5. **test_creates_file_when_missing** — No existing file → creates new one.
6. **test_empty_site_list_writes_manual_only** — Empty `fc_sites` + manual entries →
   only manual entries in output.

#### 4b: Unit test for numpy fix

**File**: `apps/iEasyHydroForecast/tests/test_setup_library.py`

**New test**: `test_get_pentadal_forecast_sites_returns_list` — Verify `site_codes`
return value is a Python list, not numpy array.

#### 4c: Integration test for preprocessing_runoff config write

**File**: `apps/preprocessing_runoff/test/test_config_all_bootstrap.py` (new file)

Tests:
1. **test_hf_sdk_fetch_writes_config_all** — Mock `sl.get_all_forecast_sites_from_HF_SDK`
   to return fake Site objects, run the relevant preprocessing_runoff code path, verify
   `config_all_stations_library.json` was written with correct content.
2. **test_cache_mode_skips_config_write** — When cache is used, verify config write is
   not called (since fc_sites is empty).

---

## Files Involved

| File | Changes |
|------|---------|
| `apps/iEasyHydroForecast/setup_library.py` | Add `write_config_all_stations()`, `_sites_to_config_dict()`, `_write_config_all()`. Refactor lines 840-873 to use `_write_config_all()`. Fix line 963 `.tolist()`. |
| `apps/iEasyHydroForecast/forecast_library.py` | Fix `get_pentadal_and_decadal_data()` line 1255 — defensive list conversion. |
| `apps/preprocessing_runoff/preprocessing_runoff.py` | Add call to `sl.write_config_all_stations(fc_sites)` after line 314. |
| `apps/iEasyHydroForecast/tests/test_setup_library.py` | Add `TestWriteConfigAllStations` class (6 tests) + numpy fix test. |
| `apps/preprocessing_runoff/test/test_config_all_bootstrap.py` | New file: integration tests for config write path (2 tests). |

## Acceptance Criteria

- [ ] `config_all_stations_library.json` is populated after `preprocessing_runoff` runs
      (regardless of which SDK path is taken)
- [ ] `linear_regression` runs successfully after `preprocessing_runoff` in a local
      setup with `ieasyhydroforecast_connect_to_iEH=False`
- [ ] Manual site entries (Phase 1 Google Sheets) are preserved across regenerations
- [ ] The numpy truth-value error in `get_pentadal_and_decadal_data` is fixed
- [ ] Existing tests continue to pass
- [ ] New tests pass with zero skips

---

## Dependency Graph

```json
{
  "phases": {
    "phase_1": {
      "name": "Extract config write helper",
      "files": [
        "apps/iEasyHydroForecast/setup_library.py"
      ],
      "depends_on": [],
      "parallelizable_with": ["phase_3"]
    },
    "phase_2": {
      "name": "Call config write from preprocessing_runoff",
      "files": [
        "apps/preprocessing_runoff/preprocessing_runoff.py"
      ],
      "depends_on": ["phase_1"]
    },
    "phase_3": {
      "name": "Fix numpy truth-value bug",
      "files": [
        "apps/iEasyHydroForecast/setup_library.py",
        "apps/iEasyHydroForecast/forecast_library.py"
      ],
      "depends_on": [],
      "parallelizable_with": ["phase_1"]
    },
    "phase_4a": {
      "name": "Unit tests for write_config_all_stations",
      "files": [
        "apps/iEasyHydroForecast/tests/test_setup_library.py"
      ],
      "depends_on": ["phase_1"]
    },
    "phase_4b": {
      "name": "Unit test for numpy fix",
      "files": [
        "apps/iEasyHydroForecast/tests/test_setup_library.py"
      ],
      "depends_on": ["phase_3"]
    },
    "phase_4c": {
      "name": "Integration test for preprocessing config write",
      "files": [
        "apps/preprocessing_runoff/test/test_config_all_bootstrap.py"
      ],
      "depends_on": ["phase_1", "phase_2"]
    }
  },
  "execution_order": [
    {"parallel": ["phase_1", "phase_3"]},
    {"parallel": ["phase_2", "phase_4a", "phase_4b"]},
    {"sequential": ["phase_4c"]},
    {"sequential": ["run_all_tests"]}
  ],
  "notes": {
    "phase_1_and_3_conflict": "Both touch setup_library.py. If run in parallel via worktrees, merge carefully. Alternatively, run phase_1 first, then phase_3 sequentially on the same branch to avoid merge conflicts.",
    "recommended_order": "Phase 1 → Phase 3 (same file) → Phase 2 + 4a + 4b (parallel) → Phase 4c → tests"
  }
}
```
