# Plan: Organization-Based Station Filtering (Option A — App-Side)

**Status**: draft
**Branch**: `develop_long_term_fix_api_postprocessing_forecasts`
**Module**: cross-cutting (iEasyHydroForecast, postprocessing_forecasts, config)

## Context

With 3 hydromets' data in one database, forecast modules error because they
encounter stations belonging to other organizations. The iEasyHydro HF SDK
already scopes its responses by organization (via `self.organization_uuid`),
but this org identity is **lost** when configs are written and **ignored** when
modules load station lists. The SAPPHIRE postprocessing API returns data for
ALL organizations when queried without station codes.

**Goal**: Filter stations by organization at the app layer so each pipeline run
only processes its own org's stations. No changes to `sapphire/services/`.

---

## Phase 0 Results (Completed)

Script: `apps/iEasyHydroForecast/check_org_id.py`

| Org | `ieasyhydroforecast_organization` | `ORGANIZATION_ID` (env) | `organization_uuid` (login) | Discharge Sites | Virtual |
|-----|-----------------------------------|------------------------|-----------------------------|-----------------|---------|
| kghm | kghm | 1 | fe12cadc-5dd0-415d-91b1-73cc3b476c13 | 63 | 6 |
| tjhm | tjhm | 1 | 0d37371c-b8f8-48e6-9ffe-3ac569f1673f | 35 | 0 |
| demo | demo | 1 (no HF SDK) | — | — | — |

**Key finding**: `ORGANIZATION_ID=1` for ALL orgs — the integer is meaningless
for cross-org differentiation. The HF SDK differentiates via UUID at login
time. Each SDK instance only returns its own org's sites.

**Decision**: Use `ieasyhydroforecast_organization` env var (string: "demo",
"kghm", "tjhm") as the org tag for station filtering, not the integer
`ORGANIZATION_ID`.

---

## Key Findings

| Fact | Detail |
|------|--------|
| `ieasyhydroforecast_organization` | **Primary org identifier**: "demo", "kghm", "tjhm" |
| `ORGANIZATION_ID` env var | Integer `1` for all orgs — useless for differentiation |
| `config_all_stations_library.json` | Has `organization_id` field (list-wrapped `[1]`) — will add `organization` field with org name |
| `_sites_to_config_dict()` line 730 | Hardcodes `organization_id: [None]` — loses org info |
| `startswith("1")` filter in `get_pentadal_forecast_sites_complicated_method()` (near line 1054) | Kyrgyz-specific hack — see "Why replacing startswith is safe" below |
| `read_observed_and_modelled_data()` | Already accepts `codes` param — postprocessing passes `None` |
| HF SDK | Already org-scoped via UUID — returned sites belong to current org |
| Site class | No `organization` attribute — needs adding |
| Old SDK write path in `get_pentadal_forecast_sites_complicated_method()` (lines 1028-1048) | Also lacks `organization` — **deprecated, will not be updated** |

---

## Decisions (from review 2026-03-12)

| # | Decision | Rationale |
|---|----------|-----------|
| D1 | **Migration: treat `None` as "matches current org"** | During transition, stations written before Phase 1 have `organization=None`. Dropping them silently would lose stations. Treat `None` as "belongs to current org" so old config files work until regenerated. |
| D2 | **Per-horizon station selection files** | DECAD uses `ieasyforecast_config_file_station_selection_decad` in upstream modules. Phase 3 must read the horizon-appropriate file via `ShortTermHorizonConfig`. |
| D3 | **`recalculate_skill_metrics.py` short-term path in scope** | Its `_run_short_term_recalc()` also calls `read_observed_and_modelled_data()` without `codes=`. Add as Phase 4. |
| D4 | **Old SDK write path (lines 1028-1048) is deprecated** | No need to add `organization` to the old-SDK `stations_dict` block. It will be removed in a future cleanup. |

---

## The Two-File Contract

Two config files work together as a **pair** for station management. The plan
must treat them consistently.

### `config_all_stations_library.json` — Station Registry (source of truth)

- **Semantic**: "All known stations and their metadata"
- **Content**: Dict mapping station code → full metadata (lat, lon, basin,
  data_source, organization, etc.). All values list-wrapped per legacy convention.
- **Writers**: `setup_library.write_config_all_stations()` (called from
  preprocessing_runoff after HF SDK fetch), `_try_bootstrap_from_hf_sdk()`
  (fallback when file missing). ~~`get_pentadal_forecast_sites_complicated_method()`
  (legacy iEH SDK path)~~ — **deprecated** (Decision D4).
- **Manual stations**: Stored here with `data_source: ["google_sheets"]` or
  `["manual"]`. Preserved across SDK rewrites by `_read_manual_entries_from_config()`.
- **Org-scoped?**: **Not yet** — `organization_id` hardcoded to `[None]`.
  Phase 1 adds an explicit `organization` field.

### `config_station_selection.json` / `config_station_selection_decad.json` — Operational Filter (derived)

- **Semantic**: "Which stations are we actually forecasting for this run?"
- **Content**: `{"stationsID": ["12176", "12256", ...]}` — flat list of codes
- **Files**: Two separate files, one per horizon:
  - Pentad: env var `ieasyforecast_config_file_station_selection`
  - Decad: env var `ieasyforecast_config_file_station_selection_decad`
- **Writers**: `setup_library.get_pentadal_forecast_sites_from_HF_SDK()`,
  `get_all_forecast_sites_from_HF_SDK()`, **configuration_dashboard.R** (user edits)
- **Derivation**: SDK codes + `_get_manual_site_codes()` (which reads
  all_stations and extracts codes where `data_source != "ieh_hf"`)
- **Org-scoped?**: **Implicitly yes** — HF SDK returns only current org's
  stations; manual codes come from all_stations entries that were added for
  this org. But no explicit org tag in the file itself.

### Relationship

`config_station_selection` is a **strict subset** of `config_all_stations`
keys. Every code in the selection must exist in the registry.

### Why Phase 3 uses `config_station_selection.json` (not `config_all_stations`)

For postprocessing API queries, we need a **flat list of station codes** to
pass as the `codes=` parameter. `config_station_selection.json` provides
exactly this — it's the operational filter that says "these are the stations
this pipeline run cares about."

Using `config_all_stations_library.json` would require:
1. Parsing the nested metadata dict
2. Filtering by organization field (which doesn't exist yet until Phase 1)
3. Extracting just the codes — duplicating what `config_station_selection.json`
   already provides

The implicit org-scoping of `config_station_selection.json` is sufficient
because it's **derived from** org-scoped sources (HF SDK + manual entries
added for this org). Phase 1's explicit org tag on `config_all_stations` is
the **upstream fix** that makes this derivation auditable.

This also follows the existing pattern: `postprocessing_operational_long_term.py`
already reads `config_station_selection.json` at lines 56-66 and passes codes
to the data reader.

---

## Why Replacing `startswith("1")` with Org Filtering Is Safe

The `startswith("1")` filter at line 1054 in `get_pentadal_forecast_sites_complicated_method()`
appears to filter for discharge-only sites, but investigation shows it actually
guards against a **different problem**: stations from other orgs leaking in via
the config_all merge.

### What happens at line 1054

1. **`ieh_sdk.get_discharge_sites()`** (line 914) returns only hydrological
   stations — the SDK endpoint is `/stations/{uuid}/hydrological` (HF SDK) or
   `/discharge_sites` (old SDK). **Site-type filtering is already done server-side.**

2. **Lines 965-993**: Entries from `config_all_stations_library.json` that are
   NOT in `db_sites` get appended (virtual stations, manual entries). In a
   multi-org deployment, config_all could contain other orgs' stations if the
   file is shared across org runs.

3. **Line 1054**: `startswith("1")` catches those leaked entries — but only
   because Kyrgyz station codes happen to start with "1". It's a proxy for
   "belongs to this org" based on a naming convention, not a general-purpose
   filter.

### Why `filter_sites_by_org()` is the correct replacement

- **SDK results** are already org-scoped (HF SDK authenticates per-org via UUID)
- **Config_all entries** will carry an explicit `organization` field after Phase 1
- **`filter_sites_by_org()`** checks the `organization` column — this is
  semantically correct for any org's station codes, not just Kyrgyz ones
- **Graceful degradation**: if `organization` column is missing or all-None,
  the filter returns the DataFrame unfiltered (same behavior as removing
  `startswith("1")` entirely)
- **Migration safety**: `None` values are treated as "belongs to current org"
  (Decision D1), so stations from old config files are not silently dropped

### Site-type values (for reference)

| SDK | Values returned by `get_discharge_sites()` |
|-----|---------------------------------------------|
| Old (`IEasyHydroSDK`) | `"automatic-discharge"` (from API `siteType` field) |
| New (`IEasyHydroHFSDK`) | `"manual"`, `"automatic"` (resolved from `'M'`/`'A'`) |

Meteo (`"meteo"`) and virtual (`"virtual"`) stations come from separate SDK
endpoints (`get_meteo_sites()`, `get_virtual_sites()`) — they do NOT appear in
`get_discharge_sites()` results.

---

## Phases

### Phase 1a: Tag Site Objects with Organization

**Goal**: Site objects carry the org name; config JSON includes `organization`.

**Files to modify**:

1. `apps/iEasyHydroForecast/forecast_library.py`
   - `Site.__init__` (line 5613): Add `organization=None` param + `self.organization = organization`
   - All HF SDK classmethods that create Site objects:
     - `pentad_forecast_sites_from_iEH_HF_SDK` (line 6973)
     - `decad_forecast_sites_from_iEH_HF_SDK` (line 6868)
     - `all_forecast_sites_from_iEH_HF_SDK` (line 7120)
     - `virtual_decad_forecast_sites_from_iEH_HF_SDK` (line 7229)
     - `virtual_pentad_forecast_sites_from_iEH_HF_SDK` (line 7334)
     - `virtual_all_forecast_sites_from_iEH_HF_SDK` (line 7476)
   - In each classmethod: `organization=os.getenv("ieasyhydroforecast_organization")`
   - **Note**: `Site.from_dataframe()` (line 6820) does NOT need updating — it creates
     Site objects from `db_sites` after the org filter has already run. Leaving
     `organization=None` on these objects is acceptable.

2. `apps/iEasyHydroForecast/setup_library.py`
   - `_sites_to_config_dict()` (def at line 684, target statement at line 730):
     Add `"organization": [getattr(site, "organization", None)]`.
     Keep `"organization_id"` as-is for backward compat.
   - `_try_bootstrap_from_hf_sdk()` (def at line 821):
     Add `"organization"` to the `empty_cols` list that begins at line 834.

**Test requirements** (in `apps/iEasyHydroForecast/tests/test_setup_library.py`):
- Test `Site.__init__` directly: `Site(organization="demo")` → `site.organization == "demo"`;
  `Site()` → `site.organization is None`
- Test `_sites_to_config_dict()` directly (currently **untested**):
  - Site with `organization="kghm"` → JSON has `"organization": ["kghm"]`
  - Site without attribute → `"organization": [None]`
  - Assert `"organization_id"` is still present (backward compat)
- Fix **both** `_make_site()` helpers (lines ~1794 and ~1991) to explicitly set
  `site.organization = None`. MagicMock auto-creates attributes as mock objects,
  which would silently corrupt JSON output.

---

### Phase 1b: Propagate Organization Through DataFrame Flow

**Goal**: The `organization` column reaches `db_sites` at line 1054 so Phase 2
can filter on it.

**Files to modify**:

1. `apps/iEasyHydroForecast/setup_library.py`
   - `new_sites_forecast` construction (lines 976-990): Add `"organization"` column
     sourced from `config_all["organization"]` so virtual/manual entries from config_all
     carry their org tag into `db_sites` after the `pd.concat` at line 993.
   - Merge at line 997: Add `"organization"` to the column list:
     `config_all[["site_code", "river_ru", "punkt_ru", "lat", "long", "organization"]]`
     so SDK-sourced rows in `db_sites` also get their org from config_all.

**Note on `load_all_station_data_from_JSON()`** (forecast_library.py line ~1781):
This function already unwraps list-wrapped values to scalars. After Phase 1a adds
`"organization"` to the JSON, this function will automatically include it as a
scalar column in the loaded DataFrame. No changes needed to this function.

**Test requirements** (in `apps/iEasyHydroForecast/tests/test_setup_library.py`):
- Test that `db_sites` DataFrame at the point of the `startswith("1")` filter
  (line 1054) has an `"organization"` column after the config_all merge.
- **Warning**: `test_forecast_library.py:425-429` uses `assertCountEqual` on exact
  columns from `load_all_station_data_from_JSON()` against a test fixture that lacks
  `"organization"`. This test reads its own fixture file and is unaffected unless the
  fixture is changed — do NOT modify the test fixture.

---

### Phase 2: Add Org-Filtering Helpers + Replace `startswith("1")`

**Goal**: Filter station DataFrames by org name; remove Kyrgyz-specific hack.

**Files to modify**:

1. `apps/iEasyHydroForecast/setup_library.py`

**Add** (near `check_organization()` at line 255):

```python
def _get_current_org() -> str | None:
    """Return ieasyhydroforecast_organization from env, or None."""
    return os.getenv("ieasyhydroforecast_organization")

def filter_sites_by_org(df: pd.DataFrame, org: str | None = None) -> pd.DataFrame:
    """Filter station DataFrame by organization name.

    Graceful degradation: returns df unfiltered if org is None,
    column is missing, or all values are None.

    Migration safety (Decision D1): rows with organization=None are
    treated as "belongs to current org" and kept. This prevents
    silently dropping stations from config files written before
    the organization field was added.
    """
    if org is None:
        org = _get_current_org()
    if org is None or "organization" not in df.columns:
        return df
    col = df["organization"].apply(
        lambda v: v[0] if isinstance(v, list) else v
    )
    if not col.notna().any():
        return df
    # Keep rows that match current org OR have None (migration safety)
    return df[col.isna() | (col == org)].copy()
```

**Modify** `get_pentadal_forecast_sites_complicated_method()`:
- **Line 1054**: Replace
  `db_sites = db_sites[db_sites["site_code"].astype(str).str.startswith("1")]`
  with `db_sites = filter_sites_by_org(db_sites)`.
  Log a warning if filter returns all rows unchanged (means org field missing).

**Test requirements**:
- `filter_sites_by_org()` unit tests (use real DataFrames, not mocks):
  - Matching org → only matching rows returned
  - Non-matching org → empty DataFrame (only non-matching, non-None rows)
  - `org=None` and env unset → passthrough (returns input unchanged)
  - Missing `"organization"` column → passthrough
  - All-None `"organization"` column with org env set → passthrough (migration safety)
  - List-wrapped values `["kghm"]` → correctly unwrapped and filtered
  - Mixed None/string values → **None rows kept** + matching rows kept (Decision D1)
  - Mixed orgs with None → None rows kept, non-matching org rows excluded
- `_get_current_org()`: set env → returns value; unset env → returns None
- Regression test for `startswith("1")` removal: construct `db_sites` with
  multi-org data (some codes starting with "1", some not), set org env,
  verify `filter_sites_by_org` keeps correct-org rows regardless of code prefix.
  Must exercise the DB-access path (not the `db_sites = config_all` fallback).
- Warning assertion: when filter returns all rows unchanged, assert
  `logger.warning` is called.

---

### Phase 3: Pass Per-Horizon Station Codes to Short-Term Postprocessing

**Goal**: Short-term postprocessing only reads its own org's forecasts from the API,
using the correct station selection file for each horizon (pentad vs decad).

**Rationale**: `config_station_selection.json` (pentad) and
`config_station_selection_decad.json` (decad) are the correct sources because
they're the operational filters derived from org-scoped sources (see "The
Two-File Contract" above). This matches the existing pattern in
`postprocessing_operational_long_term.py` lines 56-66.

**Files to modify**:

1. `apps/postprocessing_forecasts/src/horizon_config.py`
   - Add `station_selection_env: str` field to the frozen `ShortTermHorizonConfig`
     dataclass. **Critical**: because the dataclass uses `frozen=True`, ALL
     existing PENTAD/DECAD singleton instantiations across all files must be
     updated in the same commit, or imports will crash with
     `TypeError: __init__() got an unexpected keyword argument`.
     Files with PENTAD/DECAD singletons that need updating:
     - `postprocessing_operational.py` (lines 65-84)
     - `recalculate_skill_metrics.py` (lines 62-81)
     - `postprocessing_maintenance.py` (check for singletons)
     ```python
     station_selection_env: str  # env var for station selection config file
     ```

2. `apps/postprocessing_forecasts/postprocessing_operational.py`
   - Update PENTAD/DECAD singletons:
     ```python
     PENTAD = ShortTermHorizonConfig(
         ...
         station_selection_env="ieasyforecast_config_file_station_selection",
     )
     DECAD = ShortTermHorizonConfig(
         ...
         station_selection_env="ieasyforecast_config_file_station_selection_decad",
     )
     ```
   - Add a `_read_station_codes(config)` helper (copy the pattern from
     `postprocessing_operational_long_term.py:56-66`; do not extract to shared
     utility yet):
     ```python
     def _read_station_codes(config: ShortTermHorizonConfig) -> list[str]:
         config_path = os.path.join(
             os.getenv("ieasyforecast_configuration_path", ""),
             os.getenv(config.station_selection_env, ""),
         )
         with open(config_path) as f:
             station_config = json.load(f)
         codes = [str(c) for c in station_config.get("stationsID", [])]
         logger.info("Read %d station codes for %s", len(codes), config.name)
         return codes
     ```
   - `_run_short_term_postprocessing()` (~line 87): Call `_read_station_codes(config)`
     and pass result as `codes=` to `data_reader.read_observed_and_modelled_data()`

3. `apps/postprocessing_forecasts/tests/test_wiring_integration.py`
   - Update `env_setup` fixture (lines 269-286) to set:
     - `ieasyforecast_configuration_path` → `str(tmp_path)`
     - `ieasyforecast_config_file_station_selection` → `"config_station_selection.json"`
     - `ieasyforecast_config_file_station_selection_decad` → `"config_station_selection_decad.json"`
   - Write both config files to `tmp_path` with station codes matching test fixtures
     (e.g., `{"stationsID": ["15001"]}`).
   - Without this, **all existing wiring integration tests will break** with
     `FileNotFoundError`.

**Test requirements**:
- Unit test: mock `_read_station_codes()` to return `["10001", "10002"]`;
  assert `read_observed_and_modelled_data` is called with `codes=["10001", "10002"]`
- Per-horizon test: verify PENTAD reads from `ieasyforecast_config_file_station_selection`
  and DECAD reads from `ieasyforecast_config_file_station_selection_decad`
- Edge case: `stationsID` key missing → returns empty list (not KeyError)
- Edge case: config file missing → `FileNotFoundError` propagates (fail-fast)
- **Guard for empty codes**: Log a warning if `codes=[]` — this means the
  station selection file exists but has no stations, which is likely a
  misconfiguration. `read_observed_and_modelled_data(codes=[])` silently
  returns empty data.

---

### Phase 4: Pass Per-Horizon Station Codes to Short-Term Skill Recalculation

**Goal**: `recalculate_skill_metrics.py` short-term path also uses org-scoped
station codes, matching the operational path (Phase 3).

**Context**: `_run_short_term_recalc()` (line 112) calls
`data_reader.read_observed_and_modelled_data(config.name)` without `codes=`.
The file already has a `_read_station_codes()` helper (line 95) used by the
monthly/quarterly/seasonal paths, but it reads only the pentad station selection
file and is not used for short-term recalculation.

**Files to modify**:

1. `apps/postprocessing_forecasts/recalculate_skill_metrics.py`
   - Update PENTAD/DECAD singletons to include `station_selection_env` (same
     as Phase 3 — these are separate instances of `ShortTermHorizonConfig`).
   - Replace `_read_station_codes()` (line 95) with a config-aware version
     matching Phase 3's `_read_station_codes(config)` pattern. Duplicate
     `_read_station_codes(config)` locally in this file (same pattern as
     Phase 3). Do NOT extract to a shared utility — deduplication is tracked
     as separate tech debt in the Out of Scope section.
   - `_run_short_term_recalc()` (line 112): Accept `codes` param and pass to
     `data_reader.read_observed_and_modelled_data(config.name, codes=codes)`.
   - At the call sites (lines 177-181):
     ```python
     if prediction_mode in ["PENTAD", "BOTH", "ALL"]:
         codes = _read_station_codes(PENTAD)
         timing_stats = _run_short_term_recalc(PENTAD, ..., codes=codes)
     if prediction_mode in ["DECAD", "BOTH", "ALL"]:
         codes = _read_station_codes(DECAD)
         timing_stats = _run_short_term_recalc(DECAD, ..., codes=codes)
     ```

2. `apps/postprocessing_forecasts/tests/test_recalc_workflow.py`
   - Update `_setup_mocks()` to handle the new `codes` argument.
   - Add assertions that `read_observed_and_modelled_data` is called with
     `codes=` for PENTAD, DECAD, and BOTH modes.
   - Update env fixtures to include station selection config paths and files
     (same pattern as Phase 3's test fixture updates).

**Test requirements**:
- Verify PENTAD and DECAD use their respective station selection files
- Verify `_run_short_term_recalc` passes `codes` through to data reader
- Existing tests must continue to pass (mock accepts `**kwargs` transparently)

---

## Out of Scope (Follow-Up Issues)

- `postprocessing_maintenance.py :: _fill_gaps_for_horizon()` calls
  `read_combined_forecasts()` without `codes=` — that function's signature
  doesn't accept codes. Org-scoping the maintenance path requires adding a
  `codes=` param to `read_combined_forecasts()`. Track separately.
- `read_skill_metrics()` reads all orgs' skill data unconditionally (9 call
  sites across `postprocessing_operational.py`, `recalculate_skill_metrics.py`,
  `postprocessing_operational_long_term.py`, `postprocessing_maintenance.py`,
  `postprocessing_maintenance_long_term.py`). Lower impact — affects ensemble
  weights, not forecast production. Requires adding `codes` param to function
  and underlying API call. Track separately.
- `read_monthly_combined_forecasts()`, `read_quarterly_combined_forecasts()`,
  `read_seasonal_combined_forecasts()` have no `codes` parameter. Called by
  `postprocessing_operational_long_term.py` (lines 120, 164, 200) and
  `postprocessing_maintenance_long_term.py` (lines 90, 245, 303). Returns all
  orgs' combined forecasts for ensemble calculations. Track separately.
- `machine_learning/scr/utils_ml_forecast.py` line 867: `client.read_forecasts()`
  reads forecasts without station code filter (only horizon + model + date range).
  Used for ML consistency checks. Could compare against wrong org's data. Track
  separately.
- Old SDK write path (lines 1028-1048 in `get_pentadal_forecast_sites_complicated_method()`)
  does not include `organization` — deprecated per Decision D4, will be removed.
- `Site.from_dataframe()` (line 6820) does not propagate `organization` into Site
  objects — acceptable because org filtering occurs before Site construction.
- Extraction of `_read_station_codes()` into a shared utility — 5 copies will
  exist after all plans complete: `postprocessing_operational.py` (INFRA-009
  Phase 3), `postprocessing_operational_long_term.py`, `postprocessing_maintenance.py`
  (PP-025 Phase 2), `postprocessing_maintenance_long_term.py`,
  `recalculate_skill_metrics.py`. Low priority tech debt.
- `long_term_forecasting` raw SQL scoping — see INFRA-011 "Deferred:
  long_term_forecasting Raw SQL Scoping" section and INFRA-012 deferred note
  for canonical tracking.
- `forecast_dashboard/src/processing.py :: get_all_stations_from_file()` loads
  `all_stations.pkl` without org filter, showing all orgs' stations in the UI
  dropdown. Deferred — display-only, no data mutation.
- **Station code disjointness**: All plans assume station codes are naturally
  disjoint across orgs (Kyrgyz start with "1", Tajik with "2"). This is by
  convention, not enforced at the database level. A lightweight startup assertion
  should be added to pipeline entry points (`run_locally.sh` or module `main()`
  functions) that warns if any of the current org's configured station codes
  already exist in the DB under a different org's data. This prevents silent
  cross-org overwrites if codes ever collide. Low priority — convention has held
  so far.
- **Config file isolation**: Config files (`config_all_stations_library.json`,
  `config_station_selection.json`) are isolated only by `.env` directory paths.
  There is no file-level locking and no programmatic check that two concurrent
  pipeline runs don't share a config directory. This is safe for the current
  sequential pipeline design but should be documented as an operational
  constraint: **never run two org pipelines concurrently against the same config
  directory**.
- **Decision D1 expiry**: Once all orgs have regenerated their
  `config_all_stations_library.json` (i.e., run preprocessing_runoff at least
  once after INFRA-009 deployment), the `None`-means-current-org migration
  behavior in `filter_sites_by_org()` should be tightened to log a warning
  when `None` values are encountered, signaling stale config files. Track
  separately.

---

## Verification

1. **Unit tests**: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh iEasyHydroForecast`
2. **Postprocessing tests**: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts`
3. **Full test suite**: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh`
4. **Manual verification**: Run `apps/run_locally.sh short-term` with demo org and confirm
   only demo stations are processed

---

## Dependency Graph

```json
{
  "phases": {
    "phase_1a": {
      "name": "Tag Site objects with organization + update _sites_to_config_dict",
      "depends_on": ["INFRA-010 Phase 1"],
      "note": "INFRA-010 extracts _sites_to_config_dict() into a standalone helper. Phase 1a adds the organization field to the dict returned by that helper. If INFRA-010 is not yet complete, the modification targets the inline dict construction at line 730 instead.",
      "files": [
        "apps/iEasyHydroForecast/forecast_library.py (Site.__init__, 6 HF SDK classmethods)",
        "apps/iEasyHydroForecast/setup_library.py (_sites_to_config_dict, _try_bootstrap_from_hf_sdk)",
        "apps/iEasyHydroForecast/tests/test_setup_library.py (fix _make_site helpers, add _sites_to_config_dict tests)"
      ]
    },
    "phase_1b": {
      "name": "Propagate organization column through DataFrame flow",
      "depends_on": ["phase_1a"],
      "files": [
        "apps/iEasyHydroForecast/setup_library.py (new_sites_forecast cols, merge cols)"
      ]
    },
    "phase_2": {
      "name": "Add filter_sites_by_org helper + replace startswith filter",
      "depends_on": ["phase_1b"],
      "files": [
        "apps/iEasyHydroForecast/setup_library.py (filter_sites_by_org, _get_current_org, line 1054)",
        "apps/iEasyHydroForecast/tests/test_setup_library.py"
      ]
    },
    "phase_3": {
      "name": "Pass per-horizon station codes to short-term postprocessing",
      "depends_on": [],
      "note": "Independent of phases 1-2 (different modules, different files)",
      "files": [
        "apps/postprocessing_forecasts/src/horizon_config.py (add station_selection_env field)",
        "apps/postprocessing_forecasts/postprocessing_operational.py (PENTAD/DECAD singletons, _read_station_codes, _run_short_term_postprocessing)",
        "apps/postprocessing_forecasts/tests/test_wiring_integration.py (fixture update)"
      ]
    },
    "phase_4": {
      "name": "Pass per-horizon station codes to short-term skill recalculation",
      "depends_on": ["phase_3"],
      "note": "Depends on Phase 3 because it uses the same ShortTermHorizonConfig.station_selection_env field",
      "files": [
        "apps/postprocessing_forecasts/recalculate_skill_metrics.py (PENTAD/DECAD singletons, _read_station_codes, _run_short_term_recalc)",
        "apps/postprocessing_forecasts/tests/test_recalc_workflow.py (fixture + assertion updates)"
      ]
    }
  },
  "execution_order": [
    {"parallel": ["phase_1a", "phase_3"]},
    {"sequential": "phase_1b"},
    {"parallel": ["phase_2", "phase_4"]},
    "note: phase_2 depends on phase_1b; phase_4 depends on phase_3; both are ready after step 2"
  ]
}
```

Phase 1a and Phase 3 can run in parallel (different modules, no file overlap).
Phase 1b must follow Phase 1a (uses the `organization` field that Phase 1a adds).
Phase 2 and Phase 4 can run in parallel (Phase 2 depends on 1b, Phase 4 depends on 3 — both satisfied after step 2).

**Orchestration note**: Phases 1a → 1b → 2 are sequential on `setup_library.py` —
no worktree isolation possible between them. Phase 3 and Phase 4 touch only
`postprocessing_forecasts/` files, so Phase 3 can use worktree isolation if run
in parallel with Phase 1a.
