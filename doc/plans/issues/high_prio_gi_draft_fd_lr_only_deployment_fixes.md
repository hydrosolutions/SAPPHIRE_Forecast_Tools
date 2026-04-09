# Fix forecast dashboard for LR-only deployments (no ML models)

**Status**: Draft
**Module**: forecast_dashboard, iEasyHydroForecast
**Priority**: High
**Labels**: `uzhm`, `forecast_dashboard`, `LR-only`

---

## Summary

The forecast dashboard crashes or displays broken UI when deployed with only linear regression models (no ML/CM), as is the case for the uzhm organization.

## Context

SAPPHIRE is being deployed for Uzbek Hydromet (uzhm) which runs only linear regression. The forecast dashboard was designed with the assumption that ML models are always present. Six bugs surface on LR-only deployments (five crashes, one cosmetic):

1. `KeyError: 'pentad_in_month'` when clicking the Forecast tab
2. Manual stations (like 10001) missing from the dashboard station list
3. `KeyError: 'sdivsigma'` in skill metrics table when forecast_stats is empty
4. `horizon_in_month = 'decad_in_year'` copy-paste bug (affects all decad deployments)
5. Hydrograph on Predictors tab takes too much vertical space
6. `KeyError: 'delta'` when forecast range uses delta/Q25/Q75 columns absent from LR data
7. `KeyError: 'delta'` in `create_forecast_summary_table` — skill metric columns (`delta`, `sdivsigma`, `mae`, `accuracy`) missing when `forecast_stats` is empty and merge is skipped
8. `KeyError: 'delta'` in `calculate_forecast_range` — same root cause as Bug 7, but crashes earlier at `processing.py:1184`

## Problem

The dashboard is unusable for uzhm: it crashes when navigating to the Forecast tab and when the skill table renders. These are all pre-existing assumptions about ML model presence or data completeness that were never tested on an LR-only deployment.

## Desired Outcome

- Dashboard starts and navigates without errors for LR-only deployments
- Manual stations (Google Sheets) appear in the station selector
- Skill table gracefully handles empty stats DataFrames
- Decad `horizon_in_month` correctly set to `'decad_in_month'`
- Hydrograph row height reduced to ~250px
- Forecast range gracefully handles missing delta/Q25/Q75 columns (zero-width range instead of crash)
- Summary table gracefully handles missing skill metric columns (NaN cells instead of crash)
- `calculate_forecast_range` gracefully handles missing delta column (zero-width range instead of crash)

---

## Technical Analysis

### Bug 1: `KeyError: 'pentad_in_month'` on Forecast tab

**Root cause:** Column naming inconsistency between ML and LR paths in `apps/forecast_dashboard/src/db.py`.

- **ML path** (`db.py:336`): sets `hv_col = "decade" if horizon == "decade" else "pentad_in_month"`, used at `db.py:359` in rename dict
- **LR path** (`db.py:370`): sets `lr_hv = "decade" if horizon == "decade" else "pentad"` (not `"pentad_in_month"`), used at `db.py:377` in rename dict

When only LR data exists, `pd.concat` produces a DataFrame with column `"pentad"` but **no** `"pentad_in_month"`.

**Crash points in `vizualization.py` function `plot_pentad_forecast_hydrograph_data_v2`:**
- Line 2516: `forecasts_current[horizon_in_month]` in ML block — **crashes**
- Line 2497: `forecasts_current[horizon_in_month]` in RRAM block — **also crashes** if RRAM is enabled

`horizon_in_month` is set to `'pentad_in_month'` at line 2341.

**Confirmed:** No downstream code in the dashboard reads a column named `"pentad"` (only `"pentad_in_month"` and `"pentad_in_year"`). The rename is safe.

**Note on decad path:** The ML path (`db.py:336`) sets `hv_col = "decade"` for decad. The LR path (`db.py:370`) also sets `lr_hv = "decade"` for decad. These match — no inconsistency between ML/LR for decad. However, the column name `"decade"` is inconsistent with the pentad pattern (`"pentad_in_month"`). This matters for Bug 4 — see below.

**Fix (two parts):**

1. **Column naming in `db.py`**: Normalize BOTH pentad and decad column names to the `*_in_month` pattern:
   - Line 336 (ML): change `"decade"` to `"decad_in_month"` → `hv_col = "decad_in_month" if horizon == "decade" else "pentad_in_month"`
   - Line 370 (LR): change `"decade"` to `"decad_in_month"` and `"pentad"` to `"pentad_in_month"` → `lr_hv = "decad_in_month" if horizon == "decade" else "pentad_in_month"`
   - **Verified safe:** No downstream code reads a column named `"decade"` — only `db.py:336` and `db.py:370` define it.

2. **Defensive guards in `vizualization.py`**: Add a column-existence check at lines 2497 and 2516. These lines already have inline `if not forecasts_current.empty else None` ternaries — the missing guard is column existence (`horizon_in_month in forecasts_current.columns`), which should be added to the existing condition. **Note:** When this guard triggers, the value is set to `None` (becomes NaN in the DataFrame). This produces a silent no-op — the RRAM/ML forecast point simply won't appear on the plot. This is acceptable behavior when no RRAM/ML data exists.

### Bug 4 (related): `horizon_in_month` copy-paste bug for decad

**File:** `apps/forecast_dashboard/src/vizualization.py:2346`

```python
horizon_in_month = 'decad_in_year'   # WRONG — should be 'decad_in_month'
```

The pentad branch correctly sets `horizon_in_month = 'pentad_in_month'` (line 2341). The decad branch incorrectly sets it to `'decad_in_year'`, which is the same as `horizon_in_year`. This means lines 2497/2516 would read/write the wrong column for decad forecasts. This bug affects all decad deployments, not just LR-only.

**Why `'decad_in_month'` is correct (not `'decade'`):** The data from `get_forecasts_all()` currently has a column `"decade"` (from `db.py:336`), which would make `horizon_in_month = 'decade'` seem like the minimal fix. However, `"decade"` is a naming inconsistency — Bug 1's fix renames it to `"decad_in_month"` in `db.py` (see above). The `create_skill_table` function already uses `'decad_in_month'` (line 4470, 4494). After the Bug 1 db.py fix, the data column IS `"decad_in_month"`.

**Fix:** Change `'decad_in_year'` to `'decad_in_month'` at line 2346. This depends on the Bug 1 db.py column rename being applied first (both are in Phase 1).

### Bug 2: Manual stations missing from `all_stations.pkl`

**File:** `apps/iEasyHydroForecast/setup_library.py`

The same bug exists in **three** functions:

| Function | Lines | `site_codes` updated | `fc_sites` updated |
|----------|-------|---------------------|-------------------|
| `get_pentadal_forecast_sites_from_HF_SDK` | 1490-1494 | Yes | **No** |
| `get_decadal_forecast_sites_from_HF_SDK` | 1332-1336 | Yes | **No** |
| `get_all_forecast_sites_from_HF_SDK` | 1567-1571 | Yes | **No** |

Manual codes (from `_get_manual_site_codes()`) are appended to `site_codes` (list of strings) but NOT to `fc_sites` (list of Site objects). The dashboard calls `get_pentadal_forecast_sites_from_HF_SDK` (`processing.py:943`) and pickles `fc_sites` as `all_stations.pkl`. Manual stations are excluded.

There is only one shared `all_stations.pkl` file (no separate decad pickle).

**IMPORTANT: `site_ids` must NOT include manual stations.** `site_ids` is passed to the iEasyHydro HF API via `get_runoff_data_for_sites_HF()` → `fetch_hydro_HF_data_robust()`. The code at `src.py:3664-3680` already filters manual stations out of `sdk_code_list` before API calls, but `site_ids` is used directly in API calls that expect valid iEH HF database IDs. Appending sentinel values (e.g., `-999`) would cause API failures.

**Fix:** Add manual Site objects in the **dashboard's** `processing.py` only — do NOT modify `setup_library.py`.

**Why not modify `setup_library.py`:** The three `get_*_forecast_sites_from_HF_SDK` functions are called by both the dashboard and the pipeline (`preprocessing_runoff.py:313`). The pipeline passes `fc_sites` to `write_config_all_stations()`, which converts Sites to JSON and writes `config_all_stations_library.json`. Adding manual Sites with partial data (default `name_nat="Name_nat"`, etc.) to `fc_sites` would overwrite the properly configured manual entries already in the config file. It would also risk polluting `site_ids` in callers that build it from `fc_sites`.

**Dashboard-only fix in `processing.py`:** Add a local helper `_create_manual_sites() -> list[fl.Site]` that reads manual entries from config and creates Site objects. Call this helper in **both** station-loading paths:

1. **`get_all_stations_from_iehhf()` (line 943):** After `get_pentadal_forecast_sites_from_HF_SDK` returns `all_stations`, append manual Sites before `save_stations_to_file()` at line 945. This ensures the pickle includes manual stations.
2. **`get_all_stations_from_file()` (line 991+):** After loading from pickle, append manual Sites that aren't already in the list. This ensures manual stations are present on initial startup (from stale/pre-existing pickle) even before the background refresh runs.

Both paths must augment, because: the SDK refresh at line 943 overwrites the pickle with SDK-only data, and the file load at line 991 reads whatever is in the pickle. Without augmenting both, manual stations would be intermittently missing depending on timing.

**Accessing `_read_manual_entries_from_config`:** This is a private function in `setup_library.py` (underscore prefix). `processing.py` already imports `setup_library as sl` (line 30) and `forecast_library as fl` (line 32). Call via `sl._read_manual_entries_from_config()`. The function requires env var `ieasyforecast_configuration_path` — this is guaranteed to be set by `config.py:113` which raises `ValueError` if missing (dashboard won't start without it).

The helper logic should be a local function in `processing.py`, not in `setup_library.py`, to avoid affecting the pipeline.

**Field mapping** (JSON `config_all_stations_library.json` → `fl.Site.__init__`):

| JSON field | Site param | Notes |
|-----------|-----------|-------|
| dict key (e.g., `"10001"`) | `code` | String |
| `name_ru[0]` | `name` | Unwrap list |
| `river_ru[0]` | `river_name` | Unwrap list |
| `punkt_ru[0]` | `punkt_name` | Unwrap list |
| `lat[0]` | `lat` | Unwrap list |
| `long[0]` | `lon` | Note: JSON key `long`, param `lon` |
| `region[0]` | `region` | Unwrap list |
| `basin[0]` | `basin` | Unwrap list |
| `qdanger[0]` | `qdanger` | Unwrap list; optional — use `None` if absent (fl.Site converts to -10000.0) |
| `bulletin_order[0]` | `bulletin_order` | Unwrap list; optional — use `0` if absent |

**`_nat` fields:** Leave `name_nat`, `river_name_nat`, `punkt_name_nat`, `region_nat`, `basin_nat` at their constructor defaults. Only the `_ru` fields are mapped. The `_nat` fields are not displayed in the dashboard for manual stations.

All other `fl.Site` params have acceptable defaults.

### Bug 3: `KeyError: 'sdivsigma'` in skill table

**File:** `apps/forecast_dashboard/src/vizualization.py:4506`

**Root cause:** When `db.get_forecast_stats` has no data for a station, it returns an empty fallback DataFrame (`db.py:421-424`) with only 4 columns: `["code", horizon_in_year, "model_short", "model_long"]`. The metric columns (`sdivsigma`, `nse`, `delta`, `accuracy`, `mae`) and layout columns (`month`, `horizon_in_month`) are absent from this stub — they ARE present in the real API response schema (`SkillMetricBase` in `sapphire/services/postprocessing/app/schemas.py:169-173`), but the fallback bypasses the API.

The crash at `vizualization.py:4506` tries to select 9 columns (`code`, `model_short`, `month`, `horizon_in_month`, plus 5 metrics) from the 4-column fallback stub. Additionally, the `pn.widgets.Tabulator` at lines 4532-4538 has hard-coded formatters referencing these columns.

**Fix (single change — `vizualization.py` only):**

**`vizualization.py:~4484`**: Add an early-return guard at the top of `create_skill_table` (`def create_skill_table(_, horizon, forecast_stats)`): if `forecast_stats.empty`, return an empty `pn.widgets.Tabulator(pd.DataFrame())` instead of proceeding to column selection and Tabulator construction. **Do NOT return `pn.pane.Markdown`** — the caller in `plot_manager.py:103` calls `.download_menu()` on the return value, which only exists on `Tabulator`, not on `Markdown`. Returning the wrong type would crash the dashboard on every page load.

**Do NOT change `db.py:421-424`.** The current 4-column fallback (`["code", horizon_in_year, "model_short", "model_long"]`) with zero rows must be preserved. The data flows through `internationalize_forecast_model_names()` (`processing.py:865`) at `db.py:454` before reaching `create_skill_table`. That function accesses `df["model_long"]` and `df["model_short"]` — if the DataFrame had **no columns** (`pd.DataFrame()`), it would raise `KeyError` before `create_skill_table` is ever called. The 4-column stub with 0 rows is safe: `.empty` is True, `.apply()` on empty Series is a no-op, and the early-return guard in `create_skill_table` handles it correctly.

### Bug 5: Hydrograph height on Predictors tab

**File:** `apps/forecast_dashboard/src/layout.py:335`

```python
min_height=400 if pm.daily_hydrograph.object is not None else 0,
```

The hydrograph card is too tall — needs about 1/3 of current height.

**Fix:** Reduce `min_height` from `400` to `250`.

### Bug 6: `KeyError: 'delta'` in forecast range calculation (LR-only)

**Files:** `apps/forecast_dashboard/src/vizualization.py:2381-2398` (and legacy function at lines 2730-2745)

**Root cause:** The forecast range calculation assumes `delta`, `Q25`, and `Q75` columns exist. These columns come from ML forecasts; LR forecasts have none of them (`delta` is in the LR API response but is **dropped** at `db.py:379`).

**Two crash scenarios depending on data composition:**

**Scenario A — Pure LR-only** (ML API returns empty): `df_ml = pd.DataFrame()` (line 356, zero columns). After concat, Q25/Q75/delta columns **don't exist at all**. The `'Q25' in forecasts.columns` check is False → enters else-branch → tries `forecasts['delta']` → `KeyError`.

**Scenario B — Mixed mode** (some ML data exists for other stations but not the filtered one): Q25/Q75 columns exist from ML rows but are **NaN for all LR rows** in the filtered `forecasts` DataFrame. The `'Q25' in forecasts.columns` check is True → enters if-branch → `.where(~isna())` evaluates to False for all rows → falls to replacement value `forecasts['delta']` → `KeyError` (delta doesn't exist — dropped from LR at `db.py:379`, not present in ML schema).

Both scenarios crash. The `delta` column doesn't exist because it's dropped from LR data in `db.py:379` and ML forecasts don't have it either. The crash triggers in **four places** across three range_type branches:

| Branch | Lines | Column access |
|--------|-------|--------------|
| `"delta"` (Q25/Q75 `.where()` replacement) | 2381, 2384 | `forecasts['delta']` |
| `"delta"` (else, no Q25/Q75) | 2388-2389 | `forecasts['delta']` |
| `"min[delta, %]"` | 2396, 2398 | `forecasts['delta']` |

The `"Manual range"` branch (lines 2392-2394) is safe — it only uses `forecasted_discharge`.

**Fix:** Add a `has_delta` guard and `delta_offset` helper **before** the range_type branching (insert at line 2375, before the comment at line 2376):

```python
has_delta = 'delta' in forecasts.columns and not forecasts['delta'].isna().all()
delta_offset = forecasts['delta'] if has_delta else 0
```

Then replace all 4 occurrences of `forecasts['delta']` with `delta_offset`:
- Lines 2381, 2384: inside `.where()` replacement → `forecasts['forecasted_discharge'] - delta_offset` / `+ delta_offset`
- Lines 2388-2389: else-branch → same pattern
- Lines 2396, 2398: `min[delta, %]` branch → same pattern

When `delta_offset` is `0`, `fc_lower = fc_upper = forecasted_discharge` (zero-width range). This avoids nested ternaries inside `.where()` calls and works uniformly across all branches.

Apply the same pattern in the legacy `plot_pentad_forecast_hydrograph_data` function at lines 2730-2745.

**Note:** The fallback handler at lines 2401-2408 (`if 'fc_lower' not in forecasts.columns`) already uses only `forecasted_discharge` — it's safe and doesn't need changes.

### Bug 7: `KeyError: 'delta'` in `create_forecast_summary_table` (missing skill metric columns)

**File:** `apps/forecast_dashboard/src/vizualization.py:2980-2981`

**Root cause:** `create_forecast_summary_table` selects 8 columns at line 2980:

```python
forecast_table = forecast_table[
    ['model_short', 'forecasted_discharge', 'fc_lower', 'fc_upper', 'delta', 'sdivsigma', 'mae', 'accuracy']]
```

The last four columns (`delta`, `sdivsigma`, `mae`, `accuracy`) are skill metric columns that come from the LEFT JOIN merge of `forecasts_all` with `forecast_stats` at `db.py:468`. When `forecast_stats` is the empty 4-column fallback (no skill data for the station), the merge guard at `db.py:461-466` evaluates `can_merge = False` (because `forecast_stats.empty` is True), so the merge is **skipped entirely**. The skill metric columns are never added to `forecasts_all`.

In LR-only mode with no skill data, `forecasts_all` has exactly 15 columns: `code`, `date`, `predictor`, `slope`, `intercept`, `forecasted_discharge`, `rsquared`, `pentad`/`decade`, `pentad_in_year`/`decad_in_year`, `model_short`, `model_long`, `flag`, `Date`, `year`, `station_labels`. None of the skill metric columns exist.

**This function is NOT legacy code.** It is actively called from `define_tabs_2` via `plot_manager.py:154` → `create_forecast_summary_tabulator()` (line 3019).

**Note:** This crash is reached only if Bug 8 is also fixed, because `calculate_forecast_range` (called at line 2977, before line 2980) crashes first on the same missing `delta`.

**Fix:** Replace the hard-coded column selection at line 2980 with `reindex`:

```python
expected_cols = ['model_short', 'forecasted_discharge', 'fc_lower', 'fc_upper',
                 'delta', 'sdivsigma', 'mae', 'accuracy']
forecast_table = forecast_table.reindex(columns=expected_cols)
```

`reindex` fills missing columns with NaN instead of raising KeyError. The subsequent `.round()` calls at lines 2984-2986 handle NaN gracefully (`NaN.round()` → NaN). The Tabulator displays empty cells for NaN values — correct behavior when no skill data exists.

### Bug 8: `KeyError: 'delta'` in `calculate_forecast_range` (processing.py)

**File:** `apps/forecast_dashboard/src/processing.py:1164-1210`

**Root cause:** `calculate_forecast_range()` is called from `create_forecast_summary_table` at `vizualization.py:2977`. It accesses `forecast_table['delta']` in **every branch except "Manual range"**:

- Line 1184: `forecast_table['forecasted_discharge'] - forecast_table['delta']` (delta branch)
- Line 1185: `forecast_table['forecasted_discharge'] + forecast_table['delta']` (delta branch)
- Line 1195: `forecast_table['forecasted_discharge'] - forecast_table['delta']` (max[delta, %] branch)
- Line 1198: `forecast_table['forecasted_discharge'] + forecast_table['delta']` (max[delta, %] branch)
- Line 1201: `forecast_table['forecasted_discharge'] - forecast_table['delta']` (max[delta, %] branch, else)
- Line 1204: `forecast_table['forecasted_discharge'] + forecast_table['delta']` (max[delta, %] branch, else)
- Line 1207: `forecast_table['forecasted_discharge'] - forecast_table['delta']` (else/fallback branch)
- Line 1208: `forecast_table['forecasted_discharge'] + forecast_table['delta']` (else/fallback branch)

The `delta` here is the **skill metric delta** from `forecast_stats`, added to `forecasts_all` by the merge at `db.py:468`. When the merge is skipped (no skill data), this column doesn't exist.

**This is the same class of issue as Bug 6** but in a different code path. Bug 6 affects the hydrograph plot functions; Bug 8 affects the summary table function.

**Fix:** Insert a `delta_offset` guard at the top of the function body (after line 1182, before line 1183):

```python
has_delta = 'delta' in forecast_table.columns and not forecast_table['delta'].isna().all()
delta_offset = forecast_table['delta'] if has_delta else 0
```

Then replace all 8 occurrences of `forecast_table['delta']` (lines 1184, 1185, 1195, 1198, 1201, 1204, 1207, 1208) with `delta_offset`. When delta is unavailable, `fc_lower = fc_upper = forecasted_discharge` (zero-width range).

---

## Implementation Plan

### Approach

Fix each bug with targeted changes. The column naming fix in `db.py` (Bug 1) addresses the root cause rather than papering over it with guards. All other fixes are defensive guards or early-returns. Bug 2 uses a shared helper to avoid triplicating the manual-station-to-Site logic.

### Files to Modify

| File | Phase | Changes |
|------|-------|---------|
| `apps/forecast_dashboard/src/db.py` | P1 | Bug 1: normalize column names in both paths — line 336 (ML): `"decade"` → `"decad_in_month"`; line 370 (LR): `"decade"` → `"decad_in_month"` and `"pentad"` → `"pentad_in_month"`. (Bug 3 fallback at line 421 is left unchanged — see analysis.) |
| `apps/forecast_dashboard/src/vizualization.py` | P1 | Bug 1: add `horizon_in_month in forecasts_current.columns` to existing ternary guards at lines 2497, 2516. Bug 4: fix `'decad_in_year'` → `'decad_in_month'` at line 2346. Bug 3: early-return guard in `create_skill_table` (line 4484) — if `forecast_stats.empty`, return empty Tabulator. Bug 6: add `delta` column guard in range calculation at lines 2381-2398 and 2730-2745. Bug 7: replace hard-coded column selection at line 2980 with `reindex` to handle missing skill metric columns. |
| `apps/forecast_dashboard/src/layout.py` | P1 | Bug 5: reduce `min_height` from `400` to `250` at line 335. |
| `apps/forecast_dashboard/src/processing.py` | P1 | Bug 8: add `delta_offset` guard in `calculate_forecast_range` (lines 1184-1208). Same pattern as Bug 6. |
| `apps/forecast_dashboard/src/processing.py` | P2 | Bug 2: add `_create_manual_sites()` helper; call in both `get_all_stations_from_iehhf()` (line 943, before pickle write) and `get_all_stations_from_file()` (line 996, before DataFrame conversion). Do NOT modify `setup_library.py`. |

### Phases

**Phase 1 — Dashboard display fixes (Bugs 1, 3, 4, 5, 6, 7, 8)** `[depends_on: []]`

All seven bugs touch `vizualization.py`, `db.py`, `processing.py`, and/or `layout.py` — grouping them in one phase avoids merge conflicts.

- **Goal**: Fix `KeyError: 'pentad_in_month'` crash, decad copy-paste bug, skill table crash on empty stats, hydrograph height, delta/Q25/Q75 range crash, summary table crash on missing skill metrics, and `calculate_forecast_range` crash on missing delta
- **Files**: `db.py`, `vizualization.py`, `layout.py`, `processing.py`
- **Agent 1**:
  - Bug 1: Normalize column names in `db.py` — line 336 (ML): `"decade"` → `"decad_in_month"`; line 370 (LR): `"decade"` → `"decad_in_month"` and `"pentad"` → `"pentad_in_month"`. Both paths should use: `lr_hv = "decad_in_month" if horizon == "decade" else "pentad_in_month"` (and same pattern for `hv_col`).
  - Bug 1: Add column-existence check to existing ternaries at `vizualization.py:2497` and `vizualization.py:2516` (these already guard on `not forecasts_current.empty` — add `and horizon_in_month in forecasts_current.columns`)
  - Bug 4: Fix `'decad_in_year'` → `'decad_in_month'` at `vizualization.py:2346`
  - Bug 3: Do NOT change `db.py:421-424` (the 4-column fallback is needed by `i18n_models` at `db.py:454`). Only add early-return guard in `create_skill_table` (`vizualization.py:4484`): if `forecast_stats.empty`, return `pn.widgets.Tabulator(pd.DataFrame())`. Do NOT return `pn.pane.Markdown` — the caller in `plot_manager.py:103` calls `.download_menu()` on the result, which requires a Tabulator.
  - Bug 5: Reduce `min_height` from `400` to `250` at `layout.py:335`
  - Bug 6: In the forecast range calculation (`vizualization.py:2377-2399`), insert at line 2375 (before the `# Calculate the forecast ranges` comment): `has_delta = 'delta' in forecasts.columns and not forecasts['delta'].isna().all()` and `delta_offset = forecasts['delta'] if has_delta else 0`. Then replace all 4 occurrences of `forecasts['delta']` (lines 2381, 2384, 2388-2389, 2396/2398) with `delta_offset`. When delta is unavailable, `delta_offset = 0` produces zero-width range (fc_lower = fc_upper = forecasted_discharge). Apply the same pattern in the legacy function `plot_pentad_forecast_hydrograph_data` at lines 2730-2745.
  - Bug 7: In `create_forecast_summary_table` (`vizualization.py:2980`), replace the hard-coded column list `forecast_table = forecast_table[['model_short', ...]]` with `forecast_table = forecast_table.reindex(columns=['model_short', 'forecasted_discharge', 'fc_lower', 'fc_upper', 'delta', 'sdivsigma', 'mae', 'accuracy'])`. This fills missing skill metric columns with NaN instead of crashing.
  - Bug 8: In `calculate_forecast_range` (`processing.py:1183`), insert `has_delta = 'delta' in forecast_table.columns and not forecast_table['delta'].isna().all()` and `delta_offset = forecast_table['delta'] if has_delta else 0` before the range_type branching. Replace all 8 occurrences of `forecast_table['delta']` (lines 1184, 1185, 1195, 1198, 1201, 1204, 1207, 1208) with `delta_offset`.
- **Acceptance**: No `KeyError` on Forecast tab with LR-only data; decad path uses correct column; skill table shows empty Tabulator when no stats exist; hydrograph card is shorter; forecast range handles missing delta/Q25/Q75 without crash; summary table handles missing skill metric columns without crash; `calculate_forecast_range` handles missing delta without crash

**Phase 2 — Manual stations in dashboard station list (Bug 2)** `[depends_on: []]`
- **Goal**: Manual stations appear in `all_stations.pkl` and station selector
- **Files**: `apps/forecast_dashboard/src/processing.py`
- **Agent 1**: Add a local helper function `_create_manual_sites() -> list[fl.Site]` in `processing.py`. Call it in **both** station-loading paths. Do NOT modify `setup_library.py`.
  
  **Helper implementation:**
  - Call `sl._read_manual_entries_from_config()` (private, line 747 in `setup_library.py`) — returns `{code_str: entry_dict}` with list-wrapped fields like `"name_ru": ["Station Name"]`, `"lat": [42.0]`, etc. Requires env var `ieasyforecast_configuration_path`.
  - For each entry, create `fl.Site(code=code, name=entry["name_ru"][0], river_name=entry["river_ru"][0], ...)` — see Bug 2 field mapping table for all fields. Read `qdanger` from `entry.get("qdanger", [None])[0]` and `bulletin_order` from `entry.get("bulletin_order", [0])[0]`. Log warning if `basin` is missing (Risk R4).
  - `fl.Site.__init__` (line 5740 in `forecast_library.py`): all params except `code` have defaults.
  - Return list of Sites. Return empty list on any exception (config file missing, env var unset).
  
  **Two insert points (both required):** Both functions load `list[fl.Site]` then convert to DataFrame via `sapphire_sites_to_dataframe()`. Manual Sites must be appended BEFORE the conversion.
  1. **`get_all_stations_from_iehhf()` (between lines 943 and 945):** After `all_stations, _, _ = sl.get_pentadal_forecast_sites_from_HF_SDK(iehhf)`, call `manual_sites = _create_manual_sites()`. Filter out codes already in `all_stations` (check `site.code`), then `all_stations.extend(manual_sites)`. This must happen BEFORE `save_stations_to_file()` at line 945 so the pickle includes manual stations. (The Site→DataFrame conversion happens later at line 966.)
  2. **`get_all_stations_from_file()` (between lines 996 and 1015):** After `all_stations = load_stations_from_file(...)` loads `list[fl.Site]` from pickle (line 996), call `manual_sites = _create_manual_sites()`. Filter out codes already in the loaded list, then extend. This must happen BEFORE `sapphire_sites_to_dataframe(all_stations)` at line 1015 which converts to DataFrame. This ensures manual stations are present on initial startup from a stale pickle that predates the fix.
  
  **Imports already available:** `setup_library as sl` (line 30), `forecast_library as fl` (line 32).

- **Acceptance**: Manual station codes (e.g., 10001) included in `all_stations.pkl` and visible in dashboard station selector in both fresh-SDK and cached-pickle paths

**Phase 3 — Tests** `[depends_on: ["P1", "P2"]]`
- **Goal**: Verify all fixes, write tests
- **Files**: `apps/forecast_dashboard/tests/`
- **Agent 1**: Write tests in `apps/forecast_dashboard/tests/`. Read `conftest.py` first — it has fixtures: `sample_forecast_df` (4 rows: TFT, TiDE, NE, LR with Q5/Q25/Q75/Q95 for ML and NaN for LR), `sample_skill_df` (3 rows with skill metrics), `identity_gettext` (no-op i18n), `mock_api_response` (factory for fake HTTP responses). Existing test files follow the pattern in `test_processing.py`, `test_db.py`, `test_site.py`.
  
  **Tests to write** (create `test_vizualization.py` and/or add to existing files):
  - **Bug 3**: `create_skill_table(identity_gettext, 'pentad', pd.DataFrame())` → assert returns `pn.widgets.Tabulator` with empty DataFrame (not crash). Also test with `sample_skill_df` to verify normal path still works.
  - **Bug 6**: Create a DataFrame with only LR columns (no delta/Q25/Q75/Q95). Call the range calculation logic or verify `delta_offset = 0` produces zero-width range. Test both the "delta column missing" and "delta column exists but all NaN" scenarios.
  - **Bug 1**: Create a DataFrame with column `"pentad"` (not `"pentad_in_month"`) and verify the column rename in `db.py` normalizes it. Or verify that `vizualization.py` guards handle missing `horizon_in_month` column gracefully.
  - **Bug 2**: Test `_create_manual_sites()` (in `processing.py`) by mocking env vars and writing a temp config JSON with `monkeypatch` + `tmp_path`. Assert returned Sites have correct `code`, `name`, `basin`, and `qdanger=None`. Also test the deduplication logic: if a code already exists in `all_stations`, it should not be added again.
  - Do NOT test Panel server rendering or Playwright integration — only test data transformation logic.
- ~~Agent 2~~: `bootstrap_stations.py` deletion deferred — still referenced in `high_prio_gi_draft_infra_new_deployment_initialization.md` as a workaround for new deployments. Will be removed after that plan is executed.
- **Acceptance**: `SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` passes with zero failures and zero unexpected skips

```json
{
  "phases": {
    "P1": { "goal": "Dashboard display fixes (Bugs 1+3+4+5+6+7+8)", "depends_on": [], "parallel_agents": 1 },
    "P2": { "goal": "Manual stations in fc_sites (Bug 2)", "depends_on": [], "parallel_agents": 1 },
    "P3": { "goal": "Tests", "depends_on": ["P1", "P2"], "parallel_agents": 1 }
  }
}
```

### Risks

- **R1 (Bug 1)**: Renaming `"pentad"` to `"pentad_in_month"` in `db.py` could break downstream code that reads column `"pentad"`. **Mitigated:** verified no consumers depend on raw `"pentad"` column name in the dashboard codebase.
- **R2 (Bug 2)**: `fl.Site.__init__` converts `qdanger=None` to `-10000.0` (sentinel) at line 5824. No TypeError occurs. The helper should read `qdanger` from config when the field is present (it's optional — many stations lack it). When absent, the sentinel `-10000.0` is used, which displays as `-10000` in bulletins — acceptable for stations that aren't bulletin targets. The helper should also read `bulletin_order` from config (controls site ordering in bulletin reports at `bulletins.py:276-285`; defaults to `0` when absent). The shared helper must NOT add entries to `site_ids` — those are passed to the iEH HF API which would reject invalid IDs.
- **R3 (Bug 3)**: The early-return returns an empty Tabulator instead of a populated one. The download button will produce an empty file, which is acceptable for new deployments with no skill data.
- **R4 (Bug 2)**: Manual stations with missing `basin` field in config JSON will default to `basin="Basin"`, creating a spurious dropdown category in the station selector (`processing.py:980` groups stations by basin). **Mitigation:** The config documentation (`doc/configuration.md`) should note that `basin` is required for correct dropdown grouping. The helper should log a warning if `basin` is missing.
- **R5 (Bug 6)**: Zero-width range (fc_lower = fc_upper = forecasted_discharge) means no uncertainty band is drawn for LR forecasts. This is correct — LR forecasts genuinely lack quantile/delta information. The "Manual range" option still works (it uses a percentage slider, no delta needed).
- **R6 (Bug 7)**: `reindex` fills missing skill metric columns with NaN. The Tabulator shows empty cells, and the download produces NaN values in the CSV. This is acceptable — the alternative is a crash, and NaN correctly represents "no data".
- **R7 (Bug 8)**: The `delta` used in `calculate_forecast_range` is the skill metric delta from `forecast_stats`, not the forecast uncertainty delta from LR forecasts. When it's missing (no skill data), `delta_offset=0` produces zero-width range. This is the same behavior as Bug 6's fix.

---

## Testing

### Test Cases

- [ ] Dashboard starts without error for LR-only deployment (no ML env vars)
- [ ] Forecast tab renders without crash when only LR forecasts exist (no ML)
- [ ] Forecast tab renders correctly for decad horizon (Bug 4 regression)
- [ ] Skill table shows placeholder when `forecast_stats` is empty
- [ ] Skill table renders correctly when API returns actual skill metrics with LR data
- [ ] Skill table renders correctly for decad horizon (uses `decad_in_month` column)
- [ ] Manual station (10001) appears in station selector dropdown
- [ ] Hydrograph card height is visually proportional (~250px)
- [ ] Forecast range dropdown ("delta", "min[delta, %]") doesn't crash with LR-only data
- [ ] kghm/tjhm dashboards still work correctly (no regressions)
- [ ] Summary table renders without crash when skill metric columns are absent (LR-only, no stats)
- [ ] `calculate_forecast_range` handles missing delta column (LR-only, no stats)

### Testing Commands

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard
```

### Manual Verification

1. Set `ieasyhydroforecast_run_ML_models=False` and `ieasyhydroforecast_run_CM_models=False`
2. Start dashboard, select a station, switch between Predictors and Forecast tabs
3. Verify no Python tracebacks in console
4. Verify 10001 appears in station list
5. Verify skill table shows placeholder or actual LR metrics
6. Switch to decad horizon — verify no crash
7. Switch range_type dropdown to "delta" and "min[delta, %]" — verify no crash (zero-width range expected)

---

## Documentation Impact

- [ ] Update `doc/configuration.md`: document that the `basin` field is required in manual station config entries for correct dashboard dropdown grouping (see Risk R4)

## Out of Scope

- Adding ML model support for uzhm
- Refactoring the dashboard to be fully model-agnostic (larger effort)
- Fixing the `define_tabs` layout function — only `define_tabs_2` is used for tab layout. Note: `plot_pentad_forecast_hydrograph_data` (the non-v2 function) IS actively called from `define_tabs_2` for the aggregated view (`plot_manager.py:133`) and is fixed in Phase 1.
- The pre-existing `Site.__init__` bug where `name_nat` is set to `name` instead of `name_nat` parameter (line 5813) — cosmetic, does not affect dashboard display

## Dependencies

- None — all fixes are in dashboard code only (`forecast_dashboard/src/`)

## Acceptance Criteria

- [ ] Dashboard loads and navigates without errors for LR-only deployment
- [ ] Manual stations appear in the station picker
- [ ] Skill table handles empty stats gracefully (empty Tabulator or actual metrics)
- [ ] Forecast range handles missing delta/Q25/Q75 columns (no crash)
- [ ] Decad `horizon_in_month` correctly resolves to `'decad_in_month'`
- [ ] All existing dashboard tests pass
- [ ] No regressions for kghm/tjhm deployments
- [ ] Summary table handles missing skill metric columns gracefully (NaN cells)
- [ ] `calculate_forecast_range` handles missing delta column (zero-width range)

---

## References

- uzhm adapter plan: `doc/plans/issues/high_prio_gi_draft_prepq_uzhm_wide_matrix_adapter.md`
- Deployment initialization: `doc/plans/issues/high_prio_gi_draft_infra_new_deployment_initialization.md`
- Column naming inconsistency: `apps/forecast_dashboard/src/db.py:336` (ML) vs `db.py:370` (LR) — affects both pentad (`pentad_in_month` vs `pentad`) and decad (`decade` naming)
- Manual station gap: `apps/iEasyHydroForecast/setup_library.py:1490`, `1332`, `1567` (root cause; fix applied in `processing.py` to avoid pipeline impact)
- Skill metrics API schema: `sapphire/services/postprocessing/app/schemas.py:169-173`
- Empty fallback stub: `apps/forecast_dashboard/src/db.py:419-424` (preserved as-is — needed by `i18n_models`)
- Summary table crash: `apps/forecast_dashboard/src/vizualization.py:2980` — selects skill metric columns that don't exist when merge is skipped
- Range calculation crash: `apps/forecast_dashboard/src/processing.py:1184` — accesses `forecast_table['delta']` which comes from merged skill metrics
- Active legacy function: `apps/forecast_dashboard/dashboard/plot_manager.py:133` — `plot_pentad_forecast_hydrograph_data` called from `define_tabs_2` aggregate view
