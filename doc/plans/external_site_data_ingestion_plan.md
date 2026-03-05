# External Site Data Ingestion Plan

## Overview

Enable SAPPHIRE to produce forecasts for sites that are **not** in the local
iEasyHydro HF database. The immediate use case is a new hydromet deployment
with 4 iEH HF sites + 1 additional site whose discharge data is provided
manually by the operator.

**Created**: 2026-03-05
**Status**: Ready for implementation

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
| `preprocessing_runoff/config.yaml` | Add external data source configuration |
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

**Alternative: Other cloud spreadsheet services.**

The same service-account pattern works with Microsoft 365 / OneDrive via the
Microsoft Graph API. If the hydromet uses Microsoft rather than Google, the
reader module can be swapped. The pipeline interface (returns a DataFrame with
`code`, `date`, `discharge` columns) stays the same.

### Discharge data type

The Google Sheet provides **daily average discharge** only (equivalent to
WDDA from iEH HF). Morning discharge (WDD) is not available for manual sites.
Downstream modules that use morning discharge must handle the absence
gracefully (treat as NaN).

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
      "site_name": ["Manual Station B"],
      "lat": [41.2], "long": [73.1],
      "river_ru": ["River Name"],
      "punkt_ru": ["Location Name"],
      "basin": ["Basin Name"],
      "region": ["Region Name"],
      "...": "..."
    }
  }
}
```

**Classification rule** (explicit only — no inference):

- `"data_source": "manual"` → manually-managed, protected from overwrite
- `"data_source": "ieh_hf"` or **field absent** → SDK-managed, normal overwrite

A site missing `data_source` defaults to `"ieh_hf"`. This is important: if the
SDK temporarily fails to return a site (network glitch, DB outage), the site
must NOT be reclassified as manual. Only an explicit `"data_source": "manual"`
(or any value other than `"ieh_hf"`) triggers protection.

#### 1.2 Guard `get_pentadal_forecast_sites_complicated_method()`

This function (line ~765) overwrites `config_all_stations_library.json` with
a merged DataFrame from the SDK. The change:

- [ ] Before writing, read existing JSON and extract entries where
      `data_source` is present AND != `"ieh_hf"`.
- [ ] After constructing the new JSON from SDK data, **merge back** the
      manual entries (preserving their `data_source` field).
- [ ] Write the combined result.

**Caution**: This function wraps each column value in a list (line ~753).
The `data_source` field must be handled specially — either excluded from
the list-wrapping logic, or stored as `["manual"]` consistently. Choose
one convention and apply it to both read and write paths.

- [ ] Verify that all consumers of this JSON can tolerate the new
      `data_source` field (check `Site.from_dataframe()` and any code
      that iterates over station keys).

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

#### 1.4 Config file backup validation

As a safety net during the write:

- [ ] Before overwriting, count entries in the existing file.
- [ ] After constructing the new data, verify the new entry count is
      >= the manual site count. Log a warning if total count drops
      significantly (e.g., >50% reduction), which may indicate an SDK
      failure returning partial data.

This is a log-level warning, not a hard block — the write still proceeds.

#### 1.5 Tests

- [ ] Unit test: manual site survives `get_pentadal_forecast_sites_complicated_method()` refresh
- [ ] Unit test: manual site code survives `get_pentadal_forecast_sites_from_HF_SDK()` selection refresh
- [ ] Unit test: manual site code survives `get_decadal_forecast_sites_from_HF_SDK()` selection refresh
- [ ] Unit test: manual site code survives `get_all_forecast_sites_from_HF_SDK()` selection refresh
- [ ] Edge case: empty SDK response does not wipe manual sites
- [ ] Edge case: manual site with same code as SDK site (should not happen; warn and prefer SDK)
- [ ] Edge case: `data_source` field absent on existing entries — treated as `"ieh_hf"` (no protection)
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
| `iehhf_site_id` | HF SDK only | **Check**: verify LR does not use this directly |

- [ ] Audit all `site.` attribute accesses in `linear_regression.py`
- [ ] Confirm `iehhf_site_id` is not accessed directly (the `site_ids` list
      returned by `get_pentadal_forecast_sites_from_HF_SDK()` includes it,
      but `get_pentadal_forecast_sites()` does not — verify nothing downstream
      requires it)
- [ ] Confirm no dependency on other HF-SDK-only fields

#### 2.3 Decadal site discovery

The current HF path calls `get_decadal_forecast_sites_from_HF_SDK()`. The
config-file alternative is `get_decadal_forecast_sites_from_pentadal_sites()`,
which reads from `config_station_selection_decad` and filters the pentadal
site list. This function already exists and does NOT require SDK access.

- [ ] Switch decadal path from `get_decadal_forecast_sites_from_HF_SDK()`
      to `get_decadal_forecast_sites_from_pentadal_sites()`
- [ ] Verify return signature compatibility (both return
      `fc_sites, site_codes` — but the HF version also returns `site_ids`;
      check if the third return value is used)

#### 2.4 Tests

- [ ] Existing `linear_regression` tests pass without iEH HF SDK available
- [ ] Integration test: pipeline runs with a mix of iEH HF and manual sites
- [ ] Verify decadal forecasts still work with the config-file path
- [ ] Full pipeline run: `run_locally.sh short-term` with test manual site

---

### Phase 3: Google Sheets data ingestion

**Goal**: `preprocessing_runoff` reads daily average discharge data for manual
sites from a private Google Sheet, authenticated via service account.

**Files changed**:
- `apps/preprocessing_runoff/src/google_sheets_reader.py` (new)
- `apps/preprocessing_runoff/src/src.py` (integration)
- `apps/preprocessing_runoff/config.yaml` (configuration)
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

If multiple manual sites exist, each gets its own tab in the same spreadsheet.

#### 3.2 Configuration

Add to `preprocessing_runoff/config.yaml`:

```yaml
external_data_sources:
  google_sheets:
    enabled: false
    # Google Sheet ID (from the URL)
    sheet_id_env: "GOOGLE_SHEETS_DISCHARGE_ID"
    # Path to service account JSON credentials
    credentials_env: "GOOGLE_SHEETS_CREDENTIALS_PATH"
    # Site codes to read from Google Sheets (tab names must match)
    site_codes: []
```

Add to the deployment `.env` file:

```bash
GOOGLE_SHEETS_DISCHARGE_ID=1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
GOOGLE_SHEETS_CREDENTIALS_PATH=/path/to/service_account.json
```

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
- [ ] Use `gspread.service_account(filename=credentials_path)` for auth
- [ ] Open sheet by ID: `gc.open_by_key(sheet_id)`
- [ ] For each site code, read the matching tab: `sh.worksheet(site_code)`
- [ ] Parse dates with `pd.to_datetime(col, format="%d.%m.%Y")`
- [ ] Replace `"-"` with `NaN` for missing discharge values
- [ ] Return DataFrame with columns `(code, date, discharge)` — same as
      the existing Excel reader output
- [ ] Wrap in try/except: log errors but return empty DataFrame (pipeline
      continues for iEH HF sites; operator sees warning in logs)

Dependency:

- [ ] Add `gspread` as an optional dependency in `pyproject.toml`:
      `gspread = {version = ">=6.0", optional = true}`
- [ ] Add to Docker image only for deployments that use it (or include
      universally — it's small, ~2 MB with deps)

#### 3.4 Integration in `preprocessing_runoff`

The integration must respect the two-mode logic in `get_runoff_data_for_sites_HF()`:

**Operational mode** (daily run):
1. Load cached CSV (existing logic)
2. Fetch yesterday's data from SDK for iEH HF sites (existing logic)
3. **NEW**: Fetch full Google Sheet for manual sites, filter to only new
   rows (dates > latest date in cached CSV for that site code)
4. Concatenate, deduplicate, continue

**Maintenance mode** (gap-fill):
1. Re-read input files (existing logic)
2. Fetch historical data from SDK for iEH HF sites (existing logic)
3. **NEW**: Fetch full Google Sheet for manual sites
4. Concatenate, deduplicate, continue

Specific changes:

In `get_runoff_data_for_sites_HF()`:

- [ ] Skip SDK fetch for site codes listed in
      `external_data_sources.google_sheets.site_codes` (they have no
      `iehhf_site_id` and the SDK call would fail)
- [ ] After SDK fetch, check config for
      `external_data_sources.google_sheets.enabled`
- [ ] If enabled, call `read_discharge_from_google_sheet()` with configured
      sheet ID, site codes, and credentials path
- [ ] In operational mode: filter Google Sheets data to rows newer than
      the latest cached date for each manual site (avoids re-reading
      full history daily)
- [ ] Concatenate the Google Sheets DataFrame with the SDK DataFrame
- [ ] Continue with existing pipeline (outlier filtering, hydrograph
      calculation, CSV + API write)

**Important**: Google Sheets data must be fetched **before** virtual station
computation, since a virtual station could depend on a manual site.

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
7. Set environment variables in the deployment `.env` file
8. Add site codes to `config.yaml` and `config_station_selection.json`
9. Add site metadata to `config_all_stations_library.json` with
   `"data_source": "manual"`
10. If the site should get ML forecasts: add it to
    `config_hydroposts_available_for_ml_forecasts` as well

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
| Config guard bug wipes all sites | High — no forecasts produced | Backup validation (1.4): log warning on >50% count drop. Full pipeline test before merge. |
| `data_source` field breaks JSON consumers | Medium — modules fail to parse config | Test all consumers (Site.from_dataframe, list-wrapping logic). Default absent field to `"ieh_hf"`. |
| `qdanger` unavailable for manual sites | Low — affects dangerous discharge display only | Already optional in output. Log info message. |
| `gspread` unavailable at runtime | Low — manual site data missing | Guard import. Return empty DataFrame. Pipeline continues for iEH HF sites. |
| Google Sheets API rate limit / outage | Low — temporary data gap | Log warning, continue pipeline. Cached CSV retains historical data. |
| Manual site data enters after virtual station computation | Medium — incorrect virtual station values | Ensure Google Sheets fetch happens before virtual station step in `get_runoff_data_for_sites_HF()`. |

---

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

| Phase | Depends on | Estimated effort |
|-------|-----------|-----------------|
| Phase 1: Config protection | None | 1-1.5 days |
| Phase 2: LR SDK removal | Phase 1 | 0.5-1 day |
| Phase 3: Google Sheets reader | Phase 1 | 1-2 days |
| Phase 4: Dashboard card | Phase 3 (optional) | 2-3 days |

Phases 1 and 2 can be committed together. Phase 3 can follow immediately
or in a separate PR. Each phase must pass `run_locally.sh all` with a test
manual site before merging.

---

## Related Documents

- `doc/plans/configuration_update_plan.md` — config system improvements
- `doc/plans/ieasyhydro_hf_migration_plan.md` — iEH HF SDK migration history
- `doc/plans/sapphire_api_integration_plan.md` — API integration patterns
- `doc/plans/issues/gi_draft_infra_model_registry.md` — model registry plan

*Last updated: 2026-03-05*
