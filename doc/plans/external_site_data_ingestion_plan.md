# External Site Data Ingestion Plan

## Overview

Enable SAPPHIRE to produce forecasts for sites that are **not** in the local
iEasyHydro HF database. The immediate use case is a new hydromet deployment
with 4 iEH HF sites + 1 additional site whose discharge data is provided
manually by the operator.

**Created**: 2026-03-05
**Status**: Ready for implementation

### Resolved Outstanding Items (from review 2026-03-06, resolved 2026-03-06)

All four items have been audited against the codebase and resolved:

1. **Guard function execution order** (§1.2–1.3): **No order constraint
   needed.** Functions #2/#3/#4 (`get_pentadal_forecast_sites_from_HF_SDK`,
   `get_all_forecast_sites_from_HF_SDK`, `get_decadal_forecast_sites_from_HF_SDK`)
   do NOT currently read `config_all_stations_library.json` — they get data
   directly from the iEH HF SDK API. The call paths are also mutually
   exclusive: function #1 runs via the config-file path
   (`has_access_to_hf_db == False`), while #2/#3/#4 run via the HF SDK path
   (`has_access_to_hf_db == True`). The plan's proposed `_get_manual_site_codes()`
   helper introduces a new read dependency on the library JSON for #2/#3/#4,
   but this is safe: in the HF SDK path, function #1 doesn't run, so the
   library JSON is stable; in the config-file path, function #1 runs in
   `preprocessing_runoff` (earlier in `run_locally.sh`) before
   `linear_regression` calls the others.

2. **Collision half-state** (§1.4): **Use both env var AND library JSON as
   a conjunction.** A site must be listed in `GOOGLE_SHEETS_SITE_CODES`
   (env var) AND marked `"data_source": "manual"` in the library JSON to
   trigger Google Sheets ingestion. If a collision removes the `manual`
   marker (§1.4 edge case), subsequent runs skip the Google Sheets fetch
   for that code even if it remains in the env var. The reader logs a
   warning: `"Site 99001 is in GOOGLE_SHEETS_SITE_CODES but not marked
   manual in config — skipping Google Sheets fetch"`. This eliminates the
   half-state: the library JSON is the enforcement point, the env var is
   the feature toggle.

3. **Minimum viable manual site entry** (§3.5): **All 8 fields are
   required.** Two validation layers enforce this:
   - `load_all_station_data_from_JSON()` requires: `code`, `name_ru`,
     `lat`, `long`
   - `Site.from_dataframe()` requires the full 8: `site_code`, `site_name`,
     `river_ru`, `punkt_ru`, `latitude`, `longitude`, `region`, `basin`
   If any field is missing, the pipeline raises `ValueError`. See §1.1 for
   the minimum viable JSON entry with all 8 fields plus `data_source`.

4. **Phase 2 `site_ids` return value** (§2.3): **`site_ids` is actively
   consumed by `preprocessing_runoff` but NOT by `linear_regression`.**
   `preprocessing_runoff.py` passes `site_ids` as `id_list` to
   `get_runoff_data_for_sites_HF()`, which validates it is non-empty and
   uses it for SDK fetch calls. `linear_regression` and `forecast_dashboard`
   discard `site_ids` with `_`. The Phase 2 switch (LR from HF SDK path to
   config-file path) is safe — the return value change (3→2) has no effect
   since LR already discards the third value. For Phase 3,
   `preprocessing_runoff` must relax the `id_list` validation: allow empty
   `id_list` when manual sites exist (they have no `iehhf_site_id`), and
   skip the SDK fetch path entirely when `id_list` is empty.

## Problem Statement

Today, every module assumes a single iEasyHydro HF instance as the sole source
of both site metadata and discharge observations. This means:

1. **Site discovery** — `linear_regression` instantiates `IEasyHydroHFSDK` to
   discover which sites to forecast for, duplicating work that
   `preprocessing_runoff` already does.
2. **Config file refresh** — `setup_library` overwrites
   `config_all_stations_library.json` from the SDK, **dropping** any
   manually-added sites that are not in the database.
3. **No manual data path** — there is no supported way for an operator
   (without SSH access) to provide discharge data for a site outside iEH HF.

## Scope

| Item | In scope | Out of scope |
|------|----------|--------------|
| Add a non-iEH-HF site to config | Yes | |
| Protect manually-added sites from config refresh | Yes | |
| Operator data entry (private/sensitive data) | Yes | |
| Remove iEH HF SDK dependency from `linear_regression` | Yes | |
| Remote iEH HF instances (cross-hydromet) | Design noted, not implemented | |
| External forecast ingestion (display-only) | Design noted, not implemented | |
| Data source registry abstraction | Future work | |

## Affected Modules

| Module | Change type |
|--------|------------|
| `iEasyHydroForecast/setup_library.py` | Guard config writes against manual sites (4 functions) |
| `iEasyHydroForecast/forecast_library.py` | No changes expected |
| `linear_regression/linear_regression.py` | Remove HF SDK instantiation, use config-file path |
| `preprocessing_runoff/src/src.py` | Add Google Sheets reader; skip SDK fetch for manual sites |
| `preprocessing_runoff/src/google_sheets_reader.py` | New: reader module |
| `preprocessing_runoff/pyproject.toml` | Add `gspread` as optional dependency |
| `forecast_dashboard/` | Optional: data-entry card (Phase 4) |

**Modules investigated — no changes needed:**

| Module | Why no changes |
|--------|---------------|
| `machine_learning` | Reads config JSON files only (no SDK). Manual sites need to be in `config_hydroposts_available_for_ml_forecasts` — operator adds them manually during setup. |
| `long_term_forecasting` | Loads all sites from DB/CSV without config filtering. Once `preprocessing_runoff` writes manual site data to CSV and API, `long_term_forecasting` picks it up automatically. |
| `postprocessing_forecasts` | Reads forecast results from upstream modules — no site discovery of its own. |

---

## Design Decisions

### Why not a full data source registry?

A per-site `data_source` registry with pluggable adapters would be the
theoretically clean solution, but today:

- Only `preprocessing_runoff` fetches discharge data (one module).
- Only 3 modules instantiate `IEasyHydroHFSDK` at all.
- The immediate need is **one** extra site.

A registry would be premature abstraction. Instead, we make minimal, localized
changes that leave room for the registry if it becomes needed (see Future Work).

### Operator data entry: Google Sheets (service account) recommended

The operator knows how to edit a spreadsheet but cannot SSH into the server.
The discharge data is **sensitive** and must remain private.

**Primary recommendation: Google Sheets + service account.**

- Operator edits a private Google Sheet from any browser.
- A small reader in `preprocessing_runoff` fetches it via `gspread` + Google
  service account (authenticated, no public access).
- Pipeline reads the sheet at execution time — no timing dependency on the
  operator being online at a specific hour.
- ~80-100 lines of new code.

**Alternative: Dashboard file upload (Phase 4).**

- A Panel `FileInput` widget in the forecast dashboard, behind existing auth.
- Operator uploads an Excel file through the browser.
- Saved to `ieasyforecast_daily_discharge_path`; `preprocessing_runoff` reads
  it with the existing Excel reader.
- More development (~200-300 lines) but fully self-contained (no external
  service).
- Can be added later without changing the pipeline.

### Configuration approach: `.env` vs `config.yaml`

Phase 3 needs configuration for the Google Sheets integration (sheet ID,
credentials path, site codes, enabled flag). Two options:

**Option A: `.env` variables only** (consistent with all other modules)

```bash
GOOGLE_SHEETS_ENABLED=true
GOOGLE_SHEETS_DISCHARGE_ID=1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
GOOGLE_SHEETS_CREDENTIALS_PATH=/etc/sapphire/google_credentials.json
GOOGLE_SHEETS_SITE_CODES=99001,99002
```

Pro: Same pattern as everything else in the project. No new config file format.
Con: Comma-separated list in an env var is slightly awkward.

**Option B: New `config.yaml`** (as originally proposed in Phase 3.2)

Pro: Cleaner for structured data (list of site codes).
Con: Introduces a new config pattern that no other module uses. Adds a
dependency on PyYAML (or reading raw YAML). One more file to deploy.

**Decision**: Option A (`.env` only). Credentials are sensitive data and
belong in `.env`. All variables are optional — deployments without Google
Sheets simply omit them and the feature stays disabled.

### Discharge data type

The Google Sheet provides **daily average discharge** only (equivalent to
WDDA from iEH HF). Morning discharge (WDD) is not available for manual sites.
Downstream modules that use morning discharge must handle the absence
gracefully (treat as NaN).

### Security considerations

**Credential management:**

- The Google service account JSON key must be stored outside the repo (e.g.,
  `/etc/sapphire/google_credentials.json`). Never commit it to git.
- **Credential rotation**: If the key is compromised or expires, the reader
  returns an empty DataFrame — this looks identical to "no data." To avoid
  silent auth failures, the reader must catch `gspread` authentication errors
  specifically and log at ERROR level (not WARNING): `"Google Sheets auth
  failed — check credentials at {path}"`. Operators should monitor logs for
  this message.
- **Credential path validation**: Before passing `GOOGLE_SHEETS_CREDENTIALS_PATH`
  to `gspread.service_account()`, validate that: (a) the path exists, (b) it
  is a regular file (not a symlink or directory), (c) it ends in `.json`.
  Reject with a clear error message otherwise.
- The service account should have **Viewer** (read-only) access to the Google
  Sheet. Do NOT grant Editor access — this prevents accidental modification
  of operator data by the pipeline.

**Input validation:**

- **Site code format**: Validate that each entry in `GOOGLE_SHEETS_SITE_CODES`
  matches `^\d+$` (digits only) before using it as a worksheet tab name, file
  path component, or in log messages. Reject non-numeric codes with a clear
  error. This prevents path traversal or injection via crafted site codes.
- **Discharge values**: The existing validation (§3.4) covers negative and
  extreme values. Additionally, enforce that discharge values are numeric
  after NaN replacement — log and skip any row where the value cannot be
  parsed as a float.

---

## Config Files Affected

Four `setup_library` functions write config files with full overwrites. All
four need the manual-site guard:

| Function | Config file written | Content |
|----------|-------------------|---------|
| `get_pentadal_forecast_sites_complicated_method()` | `config_all_stations_library.json` | All station metadata |
| `get_pentadal_forecast_sites_from_HF_SDK()` | `config_station_selection.json` | Pentadal site codes |
| `get_all_forecast_sites_from_HF_SDK()` | `config_station_selection.json` | All forecast-type site codes |
| `get_decadal_forecast_sites_from_HF_SDK()` | `config_station_selection_decad` | Decadal site codes |

**Critical**: functions #2 and #3 both write to the same file
(`config_station_selection.json`). The last writer wins. The guard must be
applied in both, since we cannot control which runs last.

---

## Implementation

### Phase 1: Protect manually-added sites in config refresh

**Goal**: A site added manually to `config_all_stations_library.json` and
the station selection files must survive SDK-triggered refreshes.

**Files changed**: `apps/iEasyHydroForecast/setup_library.py`

#### 1.1 Mark manual sites in `config_all_stations_library.json`

Add a `"data_source"` field to the site's JSON entry. Convention:

```json
{
  "stations_available_for_forecast": {
    "12176": {
      "data_source": "ieh_hf",
      "site_name": ["Station A"],
      "lat": [42.5], "long": [74.6],
      "...": "..."
    },
    "99001": {
      "data_source": "manual",
      "code": [99001],
      "name_ru": ["River Name - Location Name"],
      "site_name": ["River Name - Location Name"],
      "river_ru": ["River Name"],
      "punkt_ru": ["Location Name"],
      "lat": [41.2], "long": [73.1],
      "basin": ["Basin Name"],
      "region": ["Region Name"]
    }
  }
}
```

**Minimum required fields for a manual entry** (all 8 are mandatory — pipeline
raises `ValueError` if any are missing):

| JSON field | Maps to `Site.from_dataframe()` column | Notes |
|------------|---------------------------------------|-------|
| `code` | `site_code` | Station code (int or str) |
| `name_ru` | `site_name` | Display name |
| `river_ru` | `river_ru` | River name |
| `punkt_ru` | `punkt_ru` | Location name |
| `lat` | `latitude` | Decimal degrees |
| `long` | `longitude` | Decimal degrees |
| `region` | `region` | Region name |
| `basin` | `basin` | Basin name |

Plus `data_source: "manual"` to enable protection. Optional fields (`country`,
`is_virtual`, `site_type`, `organization_id`, `elevation`) can be added but
are not required for the pipeline to function.

**Classification rule** (explicit only — no inference):

- `"data_source": "manual"` → manually-managed, protected from overwrite
- `"data_source": "ieh_hf"` or **field absent** → SDK-managed, normal overwrite

A site missing `data_source` defaults to `"ieh_hf"`. This is important: if the
SDK temporarily fails to return a site (network glitch, DB outage), the site
must NOT be reclassified as manual. Only an explicit `"data_source": "manual"`
(or any value other than `"ieh_hf"`) triggers protection.

#### 1.2 Guard `get_pentadal_forecast_sites_complicated_method()`

This function (starts at line ~613; the list-wrapping and JSON write logic is
around lines 747–766) overwrites `config_all_stations_library.json` with a
merged DataFrame from the SDK. The change:

- [ ] Before writing, read existing JSON and extract entries where
      `data_source` is present AND != `"ieh_hf"`.
- [ ] **Back up** the existing JSON to a `.bak` file (overwriting any
      previous backup) before writing the new version. This enables
      recovery if the merge logic has a bug.
- [ ] After constructing the new JSON from SDK data, **merge back** the
      manual entries (preserving their `data_source` field).
- [ ] Write the combined result.

**Caution**: This function wraps each column value in a list (line ~753).
The `data_source` field must be handled specially — either excluded from
the list-wrapping logic, or stored as `["manual"]` consistently. Choose
one convention and apply it to both read and write paths.

**Convention chosen**: Store as `["manual"]` (list-wrapped), consistent with
all other fields in the JSON. The helper `_get_manual_site_codes()` must
unwrap with `val[0] if isinstance(val, list) else val` when reading.

- [ ] Verify that all consumers of this JSON can tolerate the new
      `data_source` field (check `Site.from_dataframe()` and any code
      that iterates over station keys).

**`Site.from_dataframe()` compatibility** (verified): The classmethod
(forecast_library.py, line ~6243) maps exactly 8 named columns
(`site_code`, `site_name`, `river_ru`, `punkt_ru`, `latitude`,
`longitude`, `region`, `basin`). It does NOT iterate over all columns,
so the extra `data_source` column is silently ignored. The `iehhf_site_id`
attribute defaults to `-999` for sites created this way — this is safe
because no downstream module accesses `iehhf_site_id` on config-file-path
Site objects. This behavior is intentional and should not be "fixed" by
adding `data_source` to the Site class.

#### 1.3 Guard all station-selection writers

Three functions write station selection files with full overwrite. Each needs
the same guard pattern: read existing codes, identify manual codes (present in
`config_all_stations_library.json` with `data_source != "ieh_hf"`), append
them to the new SDK codes before writing.

| Function | File written | Guard needed |
|----------|-------------|-------------|
| `get_pentadal_forecast_sites_from_HF_SDK()` | `config_station_selection.json` | Yes |
| `get_all_forecast_sites_from_HF_SDK()` | `config_station_selection.json` | Yes |
| `get_decadal_forecast_sites_from_HF_SDK()` | `config_station_selection_decad` | Yes |

For each:

- [ ] Before writing, read `config_all_stations_library.json` and collect
      codes where `data_source` is present AND != `"ieh_hf"`.
- [ ] Append these manual codes to the SDK-derived code list.
- [ ] Write the combined list.

**Implementation note**: Extract the guard logic into a shared helper
(e.g., `_get_manual_site_codes()`) to avoid duplicating the read-and-filter
logic in four places.

#### 1.4 Tests

- [ ] Unit test: manual site survives `get_pentadal_forecast_sites_complicated_method()` refresh
- [ ] Unit test: manual site code survives `get_pentadal_forecast_sites_from_HF_SDK()` selection refresh
- [ ] Unit test: manual site code survives `get_decadal_forecast_sites_from_HF_SDK()` selection refresh
- [ ] Unit test: manual site code survives `get_all_forecast_sites_from_HF_SDK()` selection refresh
- [ ] Edge case: empty SDK response does not wipe manual sites
- [ ] Edge case: manual site with same code as SDK site — warn and prefer
      SDK (overwrite the entry entirely, removing `data_source: manual`).
      The site code must also be removed from `GOOGLE_SHEETS_SITE_CODES`
      by the operator; log a warning if a collision is detected so the
      operator knows to update the `.env` file.
- [ ] Edge case: `data_source` field absent on existing entries — treated as `"ieh_hf"` (no protection)
- [ ] Unit test: `data_source` list-wrapping round-trip — write a manual entry
      with `["manual"]`, read it back via `_get_manual_site_codes()`, verify
      the code is correctly identified as manual
- [ ] Unit test: `config_station_selection.json` concurrent write —
      functions #2 and #3 both preserve manual codes regardless of execution
      order (run both in sequence, verify manual code present after each)
- [ ] Integration: full pipeline run via `run_locally.sh all` with a test manual site in config

---

### Phase 2: Remove iEH HF SDK dependency from `linear_regression`

**Goal**: `linear_regression` uses config JSON files for site discovery
instead of instantiating `IEasyHydroHFSDK` itself. This is safe because
`preprocessing_runoff` always runs first (guaranteed by pipeline ordering in
`run_locally.sh`, and cron orchestration).

**Files changed**: `apps/linear_regression/linear_regression.py`

#### 2.1 Remove HF SDK instantiation

The current branching logic (lines 576-622) has two paths controlled by
`ieasyhydroforecast_connect_to_iEH`:

- `True` → legacy `IEasyHydroSDK` path
- `False` → `IEasyHydroHFSDK` path

The change removes the HF SDK path:

- [ ] Remove `IEasyHydroHFSDK` from the import (keep `IEasyHydroSDK`)
- [ ] Remove `ieh_hf_sdk = IEasyHydroHFSDK()` instantiation (line ~585)
- [ ] Remove the `has_access_to_hf_db` branch that calls
      `sl.get_pentadal_forecast_sites_from_HF_SDK(ieh_hf_sdk)` and
      `sl.get_decadal_forecast_sites_from_HF_SDK(ieh_hf_sdk)`
- [ ] Always use the config-file path:
      `sl.get_pentadal_forecast_sites(ieh_sdk, has_access_to_db)` and
      `sl.get_decadal_forecast_sites_from_pentadal_sites()`

**Keep `IEasyHydroSDK` (legacy)**: It is needed for `qdanger` lookup via
`Site.from_DB_get_dangerous_discharge()`. For manual sites, `qdanger` will be
`None` — downstream code already handles this (dangerous discharge is optional
in forecast output).

#### 2.2 Verify Site object attributes

The config-file path creates Site objects via `Site.from_dataframe()`, which
produces objects with: `code`, `name`, `lat`, `lon`, `region`, `basin`,
and metadata fields. The HF SDK path additionally populates `iehhf_site_id`.

Attributes accessed by `linear_regression.py` during execution:

| Attribute | Source | Risk |
|-----------|--------|------|
| `code` | Config-file path | Safe |
| `predictor`, `slope`, `intercept` | Set during computation | Safe |
| `fc_qexp`, `fc_qmin`, `fc_qmax` | Set during computation | Safe |
| `qnorm`, `qmin`, `qmax` | Set from historical data | Safe |
| `qdanger` | Legacy SDK (`from_DB_get_dangerous_discharge`) | Safe — `None` for manual sites |
| `delta`, `perc_norm` | Set during computation | Safe |
| `iehhf_site_id` | HF SDK only | Safe — LR discards `site_ids` with `_` (verified: lines 599, 602) |

- [ ] Audit all `site.` attribute accesses in `linear_regression.py`
- [ ] Confirm no dependency on other HF-SDK-only fields

#### 2.3 Decadal site discovery

The current HF path calls `get_decadal_forecast_sites_from_HF_SDK()`. The
config-file alternative is `get_decadal_forecast_sites_from_pentadal_sites()`,
which reads from `config_station_selection_decad` and filters the pentadal
site list. This function already exists and does NOT require SDK access.

**`site_ids` audit (resolved)**: The HF SDK functions return 3 values
(`fc_sites, site_codes, site_ids`). `linear_regression` and
`forecast_dashboard` discard `site_ids` with `_`. Only
`preprocessing_runoff` actively consumes `site_ids` (passes as `id_list`
to `get_runoff_data_for_sites_HF()`). The switch from HF SDK path to
config-file path in LR is safe — no third return value is needed.

- [ ] Switch decadal path from `get_decadal_forecast_sites_from_HF_SDK()`
      to `get_decadal_forecast_sites_from_pentadal_sites()`

#### 2.4 Tests

- [ ] Existing `linear_regression` tests pass without iEH HF SDK available
- [ ] Integration test: pipeline runs with a mix of iEH HF and manual sites
- [ ] Verify decadal forecasts still work with the config-file path
- [ ] Unit test: LR produces valid forecast output when `qdanger is None`
      (manual site without dangerous discharge threshold)
- [ ] Full pipeline run: `run_locally.sh short-term` with test manual site

---

### Phase 3: Google Sheets data ingestion

**Goal**: `preprocessing_runoff` reads daily average discharge data for manual
sites from a private Google Sheet, authenticated via service account.

**Files changed**:
- `apps/preprocessing_runoff/src/google_sheets_reader.py` (new)
- `apps/preprocessing_runoff/src/src.py` (integration)
- `apps/preprocessing_runoff/pyproject.toml` (dependency)

#### 3.1 Google Sheet format (operator-facing)

The operator maintains a Google Sheet with this structure:

```
| date       | discharge |
|------------|-----------|
| 01.03.2026 | 45.2      |
| 02.03.2026 | 43.8      |
| 03.03.2026 | -         |   <- missing value
```

- **One sheet per station** (tab name = station code, e.g., "99001").
- **Date format**: `DD.MM.YYYY` (matches existing Excel convention).
- **Discharge**: daily average in m3/s, or `-` for missing.
- **Header row**: first row is headers (skipped during reading).
- **No morning discharge column** — manual sites provide daily averages only.
- **Timezone convention**: Dates are treated as **local time** (same as the
  deployment's operational timezone). The reader parses dates as
  timezone-naive and does not apply UTC conversion. This matches the
  existing Excel reader and SDK behavior.

If multiple manual sites exist, each gets its own tab in the same spreadsheet.

#### 3.2 Configuration

All Google Sheets config lives in the deployment `.env` file. Every variable
is **optional** — omit them entirely for deployments that don't use the feature.

```bash
# Optional: Google Sheets discharge data for manual sites
GOOGLE_SHEETS_ENABLED=true
GOOGLE_SHEETS_DISCHARGE_ID=1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
GOOGLE_SHEETS_CREDENTIALS_PATH=/etc/sapphire/google_credentials.json
GOOGLE_SHEETS_SITE_CODES=99001,99002
```

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GOOGLE_SHEETS_ENABLED` | No | `false` | Set to `true` to enable Google Sheets ingestion |
| `GOOGLE_SHEETS_DISCHARGE_ID` | If enabled | — | Spreadsheet ID (from the Google Sheets URL) |
| `GOOGLE_SHEETS_CREDENTIALS_PATH` | If enabled | — | Path to service account JSON key file |
| `GOOGLE_SHEETS_SITE_CODES` | If enabled | — | Comma-separated site codes; each must match a tab name |

The reader parses `GOOGLE_SHEETS_SITE_CODES` with
`os.getenv("GOOGLE_SHEETS_SITE_CODES", "").split(",")`, filtering out empty
strings. If `GOOGLE_SHEETS_ENABLED` is not `"true"` (or absent), the entire
Google Sheets path is skipped.

#### 3.3 Reader module

Create `apps/preprocessing_runoff/src/google_sheets_reader.py`:

```python
"""Read discharge data from a private Google Sheet via service account."""

import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)


def read_discharge_from_google_sheet(
    sheet_id: str,
    site_codes: list[str],
    credentials_path: str,
) -> pd.DataFrame:
    """
    Fetch daily average discharge for manual sites from a Google Sheet.

    Each site_code corresponds to a worksheet (tab) in the spreadsheet.
    Expected columns: date (DD.MM.YYYY), discharge (float or '-').

    Args:
        sheet_id: Google Sheets spreadsheet ID.
        site_codes: List of site codes; each must match a tab name.
        credentials_path: Path to Google service account JSON file.

    Returns:
        DataFrame with columns: code, date, discharge.
        Empty DataFrame if fetch fails (logged, not raised).
    """
    ...
```

Key implementation details:

- [ ] Guard the `gspread` import with try/except ImportError so the module
      loads without `gspread` installed (for deployments that don't use it):
      ```python
      try:
          import gspread
      except ImportError:
          gspread = None
      ```
      Return empty DataFrame with a clear log message if `gspread` is None
      and the feature is enabled.
- [ ] **Validate `credentials_path`** before calling gspread: check that the
      file exists and is a regular file (`os.path.isfile()`). Log ERROR and
      return empty DataFrame if not.
- [ ] **Validate `site_codes`**: reject any code that does not match
      `re.fullmatch(r"\d+", code)`. Log ERROR per invalid code and exclude
      it from processing. Continue with remaining valid codes.
- [ ] Use `gspread.service_account(filename=credentials_path)` for auth
- [ ] **Catch authentication errors specifically** (e.g.,
      `gspread.exceptions.APIError` with 401/403 status): log at ERROR level
      with message including `credentials_path` so operators can distinguish
      auth failures from network errors.
- [ ] Open sheet by ID: `gc.open_by_key(sheet_id)`
- [ ] For each site code, read the matching tab: `sh.worksheet(site_code)`
- [ ] Parse dates with `pd.to_datetime(col, format="%d.%m.%Y")`
- [ ] Replace `"-"` with `NaN` for missing discharge values
- [ ] **Validate discharge values are numeric** after NaN replacement: log
      WARNING and drop rows where the value cannot be parsed as float.
- [ ] Return DataFrame with columns `(code, date, discharge)` — same as
      the existing Excel reader output
- [ ] Wrap in try/except: log errors but return empty DataFrame (pipeline
      continues for iEH HF sites; operator sees warning in logs)

Dependency:

- [ ] Add `gspread` as an optional dependency in `pyproject.toml`:
      `gspread = {version = ">=6.0,<7", optional = true}`
- [ ] Add to Docker image only for deployments that use it (or include
      universally — it's small, ~2 MB with deps)

#### 3.4 Integration in `preprocessing_runoff`

The integration must respect the execution order in `get_runoff_data_for_sites_HF()`.

**Current code order** (verified against source, lines ~3270–3454):

1. Validate `id_list` parameter (line ~3311)
2. Load cached CSV / re-read input files depending on mode (lines ~3325–3350)
3. **Virtual station computation** (lines ~3356–3369)
4. SDK fetch for iEH HF sites (lines ~3375–3454)

**Required order with Google Sheets** — Google Sheets data must be injected
**before** virtual station computation (step 3), since a virtual station
could depend on a manual site as an input:

1. Validate parameters (relaxed guard — see below)
2. Load cached data (existing logic)
3. **NEW**: Fetch Google Sheets data for manual sites, merge into cached data
4. Virtual station computation (existing logic — now sees manual site data)
5. SDK fetch for iEH HF sites (existing logic)

**Why step 5 after step 4 is safe**: Virtual station computation (`add_hydroposts`)
operates on `read_data`, which is loaded from the cached CSV (step 2). The cached
CSV already contains all historical data from previous iEH HF fetches — step 5
only adds the **most recent day(s)** of new data. So a virtual station that
depends on both a manual site and an iEH HF site will have:
- Manual site: current data from Google Sheets (step 3)
- iEH HF site: historical data from cached CSV (step 2), but NOT today's data

The one-day lag for iEH HF inputs to virtual stations is the **existing behavior**
(virtual stations have always been computed before the SDK fetch). Adding Google
Sheets data does not change this. If this lag becomes a problem in the future,
the fix is to move the SDK fetch before virtual station computation — but that
is out of scope for this plan.

**Operational mode** (daily run):
1. Load cached CSV (existing)
2. **NEW**: Fetch Google Sheets, filter to rows newer than latest cached
   date per manual site
3. Concatenate into cached data
4. Virtual station computation (existing)
5. SDK fetch for iEH HF sites (existing)

**Maintenance mode** (gap-fill):
1. Re-read input files (existing)
2. **NEW**: Fetch full Google Sheets history for manual sites
3. Concatenate into read data
4. Virtual station computation (existing)
5. SDK fetch for iEH HF sites (existing)

**Identifier clarification**: The function receives `id_list` (iehhf_site_ids)
at the entry point, but the actual HF SDK fetch call uses `code_list` (site
codes). Manual sites have codes but no `iehhf_site_id`. Both parameters need
correct handling:

- `id_list` — used only for the entry-point validation guard
- `code_list` — used for the actual SDK `get_daily_average_discharge` call

Specific changes in `get_runoff_data_for_sites_HF()`:

- [ ] **Relax `id_list` validation guard** (line ~3311). The function
      currently raises `ValueError` if `id_list is None or empty`. Manual
      sites have no `iehhf_site_id`, so when ALL sites are manual the list
      would be empty. Change: allow empty `id_list` when manual sites exist
      (determined by reading the library JSON for `data_source: "manual"`
      entries) — skip the SDK fetch path entirely. When `id_list` contains
      some IDs (mixed iEH HF + manual), SDK fetch proceeds for those only.
- [ ] **Insert Google Sheets fetch before virtual station computation**
      (between lines ~3350 and ~3356). Determine eligible manual sites
      using the **conjunction rule**: a site must appear in BOTH
      `GOOGLE_SHEETS_SITE_CODES` (env var) AND be marked
      `"data_source": "manual"` in `config_all_stations_library.json`.
      If a code is in the env var but NOT marked manual in the JSON, log
      a warning and skip it (handles the collision half-state where SDK
      has reclaimed the code but the operator hasn't updated `.env`).
      For eligible sites, call `read_discharge_from_google_sheet()` and
      concatenate the result into `read_data` before `add_hydroposts()`.
- [ ] In operational mode: filter Google Sheets data to rows newer than
      the latest cached date for each manual site code.
- [ ] Skip SDK fetch (`code_list` filtering) for site codes marked
      `"data_source": "manual"` in the library JSON.
- [ ] Continue with existing pipeline (outlier filtering, hydrograph
      calculation, CSV + API write).

**Known limitation — backfill**: In operational mode, only rows newer than
the latest cached date are ingested. If the operator backfills historical
data (inserts a row for a past date), it will NOT be picked up until the
next maintenance run. Document this for operators.

**Discharge validation**: Unlike SDK data (which is vetted by iEH HF),
Google Sheets data is manually entered and error-prone. After reading:

- [ ] Log a warning for negative discharge values (likely typos).
- [ ] Log a warning for discharge values > 10× the site's historical
      maximum (if available from cached data), as a sanity check.
- [ ] Do not silently drop invalid rows — log them and let the existing
      outlier filtering handle removal downstream.

**Operator feedback**: After a successful Google Sheets fetch, log an
info-level summary per site: number of rows ingested and the date range.
Example: `"Google Sheets: site 99001 — 3 new rows (2026-03-03 to 2026-03-05)"`.
This lets the operator or admin verify ingestion without inspecting raw data.

**Staleness monitoring**: Track consecutive empty fetches per manual site
(use the cached CSV's latest date as a proxy — if it hasn't advanced in N
runs, the site is stale). After 3 consecutive runs with zero new rows for a
manual site, escalate the log from INFO to WARNING:
`"Site 99001: no new data for 3 consecutive runs — check Google Sheet"`.
This prevents silent data gaps from going unnoticed.

#### 3.5 Deployment setup (one-time, per hydromet)

Document in a deployment guide:

1. Create a Google Cloud project and enable Sheets API + Drive API
2. Create a service account and download the JSON key file
3. Place the key file on the server (outside the repo, e.g.,
   `/etc/sapphire/google_credentials.json`)
4. **Never commit the credentials file to git** — add the path to
   `.gitignore` if it's anywhere near the repo
5. Create the Google Sheet with the expected format
6. Share the sheet with the service account email (Viewer access)
7. Set `GOOGLE_SHEETS_*` environment variables in the deployment `.env` file
8. Add site codes to `GOOGLE_SHEETS_SITE_CODES` and `config_station_selection.json`
9. Add site metadata to `config_all_stations_library.json` with
   `"data_source": "manual"`
10. If the site should get ML forecasts: add it to
    `config_hydroposts_available_for_ml_forecasts` as well

**Credential rotation**: To rotate the service account key:
1. Create a new key in Google Cloud Console for the same service account
2. Place the new JSON file at the same path (or update
   `GOOGLE_SHEETS_CREDENTIALS_PATH`)
3. Delete the old key from Google Cloud Console
4. Verify by checking logs after the next pipeline run — an ERROR log
   mentioning the credential path indicates auth failure

**Timezone guidance**: Dates in the Google Sheet represent the **hydrological
observation date** in local time. If the operator enters data while traveling
in a different timezone, they must still use the observation date (the
calendar date at the gauging station), not the date at their current location.

#### 3.6 Tests

- [ ] Unit test: `read_discharge_from_google_sheet()` with mocked gspread
      client returns correct DataFrame
- [ ] Unit test: missing tab for a site code returns empty DataFrame + warning
- [ ] Unit test: malformed date/discharge values handled gracefully
- [ ] Unit test: `gspread` not installed — returns empty DataFrame + log
- [ ] Integration test: Google Sheets data merges correctly with SDK data
- [ ] Integration test: operational mode filters to new rows only
- [ ] Edge case: Google Sheet is empty (no data rows)
- [ ] Edge case: network error during fetch (pipeline continues without
      manual site data)
- [ ] Edge case: negative discharge value logs warning
- [ ] Edge case: out-of-order historical backfill not picked up in
      operational mode (known limitation — verify maintenance mode catches it)
- [ ] Unit test: operator feedback log message contains site code, row count,
      and date range
- [ ] Unit test: conjunction rule — site in `GOOGLE_SHEETS_SITE_CODES` but
      NOT marked `manual` in library JSON → skipped with warning log
- [ ] Unit test: conjunction rule — site marked `manual` in library JSON but
      NOT in `GOOGLE_SHEETS_SITE_CODES` → skipped (no data source configured)
- [ ] Unit test: non-numeric site code in `GOOGLE_SHEETS_SITE_CODES` →
      rejected with error log, other valid codes still processed
- [ ] Unit test: `GOOGLE_SHEETS_ENABLED=true` but `GOOGLE_SHEETS_SITE_CODES`
      is empty/whitespace → no fetch attempted, info log
- [ ] Unit test: authentication failure (expired/invalid credentials) →
      ERROR-level log mentioning credential path, empty DataFrame returned
- [ ] Unit test: `GOOGLE_SHEETS_CREDENTIALS_PATH` points to non-existent
      file or symlink → clear error before attempting gspread auth
- [ ] Unit test: discharge column contains non-numeric string (not `-`) →
      row skipped with warning, other rows processed
- [ ] Full pipeline: `run_locally.sh all` with a mock Google Sheet

---

### Phase 4 (optional): Dashboard data-entry card

**Goal**: Provide a browser-based alternative for operators who cannot or
prefer not to use Google Sheets.

**Not detailed here** — can be planned separately if needed. High-level:

- New tab in `forecast_dashboard` (visible only when manual sites are
  configured)
- Panel `FileInput` widget for Excel upload, or `DatePicker` +
  `FloatInput` for direct entry
- Writes to `ieasyforecast_daily_discharge_path` (Excel) or directly to
  preprocessing API
- Behind existing dashboard authentication

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|-----------|
| Config guard bug wipes all sites | High — no forecasts produced | Manual site guard (1.2/1.3) prevents loss. Full pipeline test with manual site before merge. |
| `data_source` field breaks JSON consumers | Medium — modules fail to parse config | Test all consumers (Site.from_dataframe, list-wrapping logic). Default absent field to `"ieh_hf"`. |
| `qdanger` unavailable for manual sites | Low — affects dangerous discharge display only | Already optional in output. Log info message. Test LR output with `qdanger=None`. |
| `gspread` unavailable at runtime | Low — manual site data missing | Guard import. Return empty DataFrame. Pipeline continues for iEH HF sites. |
| Google Sheets API rate limit / outage | Low — temporary data gap | Log warning, continue pipeline. Cached CSV retains historical data. |
| Manual site data enters after virtual station computation | Medium — incorrect virtual station values | Ensure Google Sheets fetch happens before virtual station step in `get_runoff_data_for_sites_HF()`. See §3.4 execution order note. |
| Operator enters invalid discharge (negative, extreme) | Low — bad forecast for one site | Log warnings for negative and extreme values; validate numeric type; rely on existing outlier filter downstream. |
| Operator backfills historical data in Google Sheet | Low — data not picked up until maintenance run | Document as known limitation. Operational mode only ingests rows newer than cached latest date. |
| Manual site code collides with future SDK site | Low — ambiguous data source | Warn on collision, prefer SDK, log message telling operator to update `.env`. |
| Credential expiry / compromise | Medium — silent data gap for manual sites | Catch auth errors specifically at ERROR level (not WARNING). Include credential path in log message so operators can distinguish auth failures from network errors. Document rotation procedure in deployment guide. |
| Malicious or malformed site code in env var | Low — potential path traversal in CSV filenames or log injection | Validate `GOOGLE_SHEETS_SITE_CODES` entries against `^\d+$`. Reject non-numeric codes before use as tab names, file path components, or log values. |
| Consecutive empty fetches go unnoticed | Medium — manual site forecasts go stale silently | After 3+ consecutive runs where a manual site returns zero rows, escalate from INFO to WARNING: `"Site 99001: no new data for 3 consecutive runs — check Google Sheet"`. |
| Config JSON corruption during write | Medium — loss of manual site metadata | Back up `config_all_stations_library.json` to a `.bak` file before each write in `get_pentadal_forecast_sites_complicated_method()`. |
| Concurrent writers to `config_station_selection.json` | Low — manual codes dropped if cron overlap | Functions #2 and #3 both write the same file. Document as known limitation: pipeline must not run concurrently (enforced by cron scheduling). |
| Operator enters dates from wrong timezone | Low — off-by-one day error | Document in deployment guide and Google Sheet template: dates are hydrological observation dates (local time), not entry dates. |

---

## Future Work: Other Cloud Spreadsheet Services

The same service-account pattern works with Microsoft 365 / OneDrive via the
Microsoft Graph API. If the hydromet uses Microsoft rather than Google, the
reader module can be swapped. The pipeline interface (returns a DataFrame with
`code`, `date`, `discharge` columns) stays the same.

## Future Work: Data Source Registry

When multiple external data sources become common (remote iEH HF instances,
Google Sheets, dashboard uploads, other APIs), introduce a formal registry:

```yaml
data_sources:
  local_ieh_hf:
    type: ieh_hf
    host_env: IEASYHYDROHF_HOST
    credentials_env_prefix: IEASYHYDROHF
  hydromet_b:
    type: ieh_hf
    host_env: REMOTE_IEASYHYDROHF_HOST
    credentials_env_prefix: REMOTE_IEASYHYDROHF
  manual_sheets:
    type: google_sheets
    sheet_id_env: GOOGLE_SHEETS_DISCHARGE_ID
    credentials_env: GOOGLE_SHEETS_CREDENTIALS_PATH
```

Each site in the config would reference its data source by name. The pipeline
would group sites by source, call the appropriate adapter, merge results, and
continue. This is a natural evolution of the Phase 3 design — the Google
Sheets reader becomes one adapter among several.

## Future Work: External Forecast Ingestion

When a hydromet receives forecasts from another hydromet (rather than raw
discharge data), these enter the system at the **postprocessing** level:

- A small ingestion script reads external forecasts (from API, shared file,
  or spreadsheet)
- Writes them to the postprocessing API with a distinct `model_short`
  (e.g., `"EXT_KGH"`)
- The dashboard already displays all models from the API — external
  forecasts appear automatically
- Skill metrics can optionally be computed if observations are available

This is architecturally independent from the data ingestion changes above.

---

## Execution Order

| Phase | Depends on |
|-------|-----------|
| Phase 1: Config protection | None |
| Phase 2: LR SDK removal | Phase 1 |
| Phase 3: Google Sheets reader | Phase 1 |
| Phase 4: Dashboard card | Phase 3 (optional) |

Phases 1 and 3 are tightly coupled (manual sites need both config protection
and a data source). Phase 2 (LR SDK removal) is independently valuable code
hygiene — it can be committed with Phase 1 or as a separate PR. Each phase
must pass `run_locally.sh all` with a test manual site before merging.

---

## Related Documents

- `doc/plans/configuration_update_plan.md` — config system improvements
- `doc/plans/ieasyhydro_hf_migration_plan.md` — iEH HF SDK migration history
- `doc/plans/sapphire_api_integration_plan.md` — API integration patterns
- `doc/plans/issues/gi_draft_infra_model_registry.md` — model registry plan

*Last updated: 2026-03-07 (security review: added credential validation/rotation, site code input sanitization, auth error logging, staleness monitoring, config backup, conjunction rule tests, qdanger regression test, data_source round-trip test, virtual station execution order analysis, timezone guidance)*
