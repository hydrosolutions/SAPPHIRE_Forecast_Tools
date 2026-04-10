# Add Uzbek Hydromet (uzhm) wide-matrix Excel adapter to preprocessing_runoff

**Status**: Complete — adapter implemented, tested, and merged (commit 69d1334, 2026-04-07)
**Module**: preprocessing_runoff
**Priority**: High
**Labels**: `uzhm`, `adapter`, `preprocessing`

---

## Summary

Add a new reader for the Uzbek Hydromet (uzhm) daily runoff Excel format — a day×month wide matrix with one sheet per year — and wire it into the organization-based routing in `preprocessing_runoff`. Additionally, register `uzhm` across the pipeline orchestration layer (timezone, module skip logic, Docker build, notification email) so that it runs only linear regression models.

## Context

SAPPHIRE is being deployed for Uzbek Hydromet (uzhm). They provide historical daily runoff data in Excel files that use a fundamentally different layout from the Kyrgyz (kghm) or Tajik (tjhm) formats. The existing readers expect a **long format** (date column + discharge column), while the Uzbek files use a **wide matrix** with days as rows, months as columns, and one sheet per year.

There are currently 4 stations: `16022`, `16198`, `16230`, `16300`. Each file has 26 sheets (2000–2025) with the same layout.

A one-time conversion script already exists in `dev_code/convert_daily_runoff.py` for a different Uzbek station (10001), but we need a native pipeline adapter that reads the wide format directly during preprocessing, triggered by `ieasyhydroforecast_organization=uzhm`.

## Problem

The `_read_runoff_data_by_organization()` router in `src/src.py:2938` only supports `kghm`, `tjhm`, and `demo`. Setting `organization=uzhm` raises a `ValueError`. No existing reader can parse the day×month matrix layout.

## Desired Outcome

- `ieasyhydroforecast_organization=uzhm` reads all `.xlsx` files from the daily discharge directory using the wide-matrix reader
- Output DataFrame has the same schema as other readers: columns `[date, discharge, code, name]`
- Integrates seamlessly with the rest of the preprocessing pipeline (caching, gap-filling, API merging)

---

## Technical Analysis

### Uzbek Excel Format (confirmed from all 4 files)

Each `.xlsx` file is named `{station_code}.xlsx` (e.g., `16022.xlsx`). Each sheet is named by year (e.g., `"2000"`, `"2025"`).

**Sheet layout:**

| Row | Col A    | Col B   | Col C   | ... | Col M   | Col N  |
|-----|----------|---------|---------|-----|---------|--------|
| 1   | (empty)  | (empty) | (empty) | ... | `16022 Syrdariya-Chinaz` (merged, around col E) | |
| 2   | (empty)  | (empty) | (empty) | ... | (empty) | (empty)|
| 3   | `Day`    | (empty) | (empty) | ... | `Month` | (empty)|
| 4   | (empty)  | `1`     | `2`     | ... | `12`    | (empty)|
| 5   | `1`      | 832     | 912     | ... | 482     | (empty)|
| 6   | `2`      | 851     | 816     | ... | 623     | (empty)|
| ... | ...      | ...     | ...     | ... | ...     | ...    |
| 35  | `31`     | (value) | (empty) | ... | (value) | (empty)|
| 36+ | Possible summary/average rows | | | | | |

**Key properties:**
- Station code + name in row 1, typically in a merged cell around column E
- Month numbers (1–12) in row 4, columns B–M
- Day numbers (1–31) in column A, starting at row 5
- Data rows: 5 through up to 35 (31 days max)
- Invalid dates (e.g., Feb 30, Apr 31) appear as `None` values
- Decimal separator: period (`.`)
- Possible extra rows after day 31 (summary/average rows) — must be ignored
- Col N (14th column) is empty/padding

### Pipeline Organization Routing (beyond the reader)

The following locations contain organization-dependent logic that must be updated for `uzhm`:

**Timezone mapping:** `apps/iEasyHydroForecast/setup_library.py:503-517`

```python
def get_local_timezone_from_env():
    if organization == "demo":    return pytz.timezone("Europe/Zurich")
    elif organization == "kghm":  return pytz.timezone("Asia/Bishkek")
    elif organization == "tjhm":  return pytz.timezone("Asia/Dushanbe")
    else:                         return pytz.utc
```

`uzhm` currently falls back to UTC. Needs an explicit `Asia/Tashkent` branch.

**Local pipeline skip logic:** `apps/run_locally.sh:166-173 (variables) and 356-364 (function)`

```bash
DEMO_SKIP_MODULES=(preprocessing_gateway machine_learning long_term_forecasting)

should_skip_module() {
    local module="$1"
    if [ "$ORG" = "demo" ]; then
        for skip in "${DEMO_SKIP_MODULES[@]}"; do
            [ "$module" = "$skip" ] && return 0
        done
    fi
    return 1
}
```

Only `demo` has a skip list. An unrecognized org runs **all modules** including ML and long-term forecasting. `uzhm` needs the same skip profile as `demo` (LR only).

**Docker build script:** `bin/utils/build_docker_images.sh:27`

```bash
if [ "$ieasyhydroforecast_organization" != "demo" ]; then
    docker build ... sapphire-lt-forecasting ...
fi
```

This builds `lt-forecasting` for any non-demo org. Must also exclude `uzhm`.

**Docker pull script:** `bin/utils/pull_docker_images.sh:20-31` — Three branches: `kghm` pulls 4 images (including conceptmod), `tjhm` pulls 3 images (no conceptmod), and the `else` branch (for all other orgs) pulls no extra images. This is correct for `uzhm` — no change needed.

**Pipeline notification email:** `apps/pipeline/pipeline_docker.py:1368-1382`

The completion email lists different task sets per org. Only `demo` and `kghm` are handled. `tjhm` is also absent from this block (pre-existing gap). Adding `uzhm` is consistent with this pattern. `uzhm` needs a branch listing `PreprocessingRunoff`, `LinearRegression`, `PostProcessingForecasts`, `LogFileCleanup` (same as demo).

**Pipeline task routing:** `apps/pipeline/pipeline_docker.py` — ML and CM tasks are gated by env vars `RUN_ML_MODELS`/`RUN_CM_MODELS`, not by org name. Setting both to `False` in the uzhm `.env` is sufficient. No code change needed.

**SDK/DB connection handling:** `apps/preprocessing_runoff/preprocessing_runoff.py:128` and `apps/iEasyHydroForecast/setup_library.py:685` — Only `demo` gets graceful SDK failure handling. In `preprocessing_runoff.py:128`, non-demo orgs call `sys.exit(1)` on HF SDK connection failure. In `setup_library.py:685`, non-demo orgs re-raise the caught exception (there is no `sys.exit` in that file). iEasyHydro will be available for uzhm, so `uzhm` should use the strict (non-demo) path — but note that the first deployment **must** have DB connectivity or the module will crash on startup. **No change needed.**

**Timeout manager:** `apps/pipeline/src/timeout_manager.py:64-86` — Unknown orgs fall back to `demo_ch` (600s base timeout). Acceptable for initial deployment. **No change needed now.**

**Operational script URL routing:** `bin/utils/common_functions.sh:113-122`

```bash
if [ "$env_ending" == "kghm" ]; then
    export ieasyhydroforecast_url_pentad=kyg.fc.pentad.$ieasyhydroforecast_url
    export ieasyhydroforecast_url_decad=demo.fc.decade.$ieasyhydroforecast_url
elif [ "$env_ending" == "tjhm" ]; then
    export ieasyhydroforecast_url_pentad=taj.fc.pentad.$ieasyhydroforecast_url
    export ieasyhydroforecast_url_decad=taj.fc.decade.$ieasyhydroforecast_url
else
    echo "| Error: Unknown hm in env_file_path: $env_file_path"
    exit 1
fi
```

This file is sourced by **15+ operational scripts** including `run_pentadal_forecasts.sh`, `run_decadal_forecasts.sh`, `run_long_term_forecasts.sh`, `daily_update_sapphire_frontend.sh`, `setup_docker.sh`, and all daily maintenance scripts. For `uzhm`, every one of these scripts will hard-exit on startup. `apps/run_locally.sh` does NOT source this file, making this a deployment-only failure. Needs a `uzhm` branch with `uzb.fc.pentad` / `uzb.fc.decade` subdomains.

**Note:** Line 125 has a pre-existing bug — kghm's decad URL is set to `demo.fc.decade.$ieasyhydroforecast_url` instead of the expected `kyg.fc.decade.$ieasyhydroforecast_url`. This is out of scope for this issue.

**Debug site selection:** `apps/iEasyHydroForecast/forecast_library.py:1174-1177 and 1280-1283`

Organization-conditional debug site selections for kghm and tjhm. For unknown orgs (including uzhm), `debug_site` stays `None` and no debug output is produced. This is purely logging and does not affect data flow. **No change needed.**

**Docker push script:** `bin/utils/push_docker_images.sh:55`

Has kghm-specific conditional for pushing prepgateway. uzhm does not use prepgateway. **No change needed.**

**Database access fallback:** `apps/iEasyHydroForecast/setup_library.py:685`

For demo org, provides graceful "No access to iEasyHydro database" fallback (returns `False`). For non-demo orgs, re-raises the caught exception (not `sys.exit`). uzhm requires DB access, which is the correct behavior. **No change needed.**

**iEH connection guard:** `apps/iEasyHydroForecast/setup_library.py:365-371`

Raises error if `ieasyhydroforecast_connect_to_iEH=True` AND org != kghm. This provides an additional safety net: uzhm cannot accidentally enter the legacy iEH SDK path even if `connect_to_iEH` is misconfigured. **No change needed.**

### Current Implementation

**Organization router:** `apps/preprocessing_runoff/src/src.py:2938-2971`

```python
def _read_runoff_data_by_organization(
    organization, date_col, discharge_col, name_col, code_col, code_list
):
    if organization == "kghm":
        read_data = read_all_runoff_data_from_excel(...)
    elif organization == "tjhm":
        read_data = read_all_runoff_data_from_csv(...)
    elif organization == "demo":
        read_data = read_all_runoff_data_from_excel(...)
    else:
        raise ValueError(...)
    return read_data
```

**Cache fallback path:** `_load_cached_data()` (`src.py:2527`) is called by the active
`get_runoff_data_for_sites_HF()` in operational mode (line 3365). If the cache is empty
or unreadable, it falls through to `_read_runoff_data_by_organization()` — so updating
the helper is sufficient to cover this path.

**Maintenance vs. operational mode:** `_read_runoff_data_by_organization()` is only called
directly in **maintenance mode** (line 3354). In operational mode, `_load_cached_data()` is
called instead (line 3365). The cache fallback (line 2560) calls
`_read_runoff_data_by_organization()` when the cache is empty, so the first run (before any
cache exists) will trigger the new reader. Implementers should be aware that during normal
operational runs, the reader is NOT invoked — only the cached CSV is read.

**Existing reader output contract** (from `read_runoff_data_from_single_river_xlsx` and downstream consumers):
- Per-file readers return `pd.DataFrame` with columns: `[date_col, discharge_col, code_col, name_col]`
- `date_col`: datetime-coercible values (downstream applies `pd.to_datetime().dt.normalize()` at `src.py:3369`)
- `discharge_col`: float values (NaN for missing)
- `code_col`: `int64` (existing readers cast to int; downstream casts to `str` at `src.py:3666`)
- `name_col`: `str`
- **Critical**: per-file readers must return `pd.DataFrame` (never `None`) — `parallel_read_excel_files` checks `df.empty` on the return value (line 782), which raises `AttributeError` on `None`
- The top-level `read_all_*` functions may return `None` when no data is found (existing pattern at `src.py:939-940`) — this is a pre-existing latent bug in callers but must be preserved for consistency

**File selection pattern** (from `read_all_runoff_data_from_excel`, line 894–901):
- Files starting with a digit → single-river reader
- The Uzbek files (`16022.xlsx`, etc.) start with a digit, so they'd be routed to `read_runoff_data_from_single_river_xlsx` if we reused the existing `read_all_runoff_data_from_excel` — but that reader can't parse the wide matrix format. Hence the need for a separate reader.

**`parallel_read_excel_files` contract** (`src.py:724-801`):
- Calls reader with exactly these kwargs: `filename, code_list, date_col, discharge_col, name_col, code_col`
- Error handling: per-file try/except — one file failing is logged and skipped, not a batch crash
- Returns `pd.concat(results)` or empty DataFrame

**Downstream processing after reader returns** (`get_runoff_data_for_sites_HF`):
1. `pd.to_datetime(read_data[date_col]).dt.normalize()` — date normalization (`src.py:3369`) (also repeated at line 3676, idempotent)
2. Optional Google Sheets merge (`src.py:3372-3450`)
3. Optional virtual station calculation (`src.py:3452-3469`)
4. Optional iEH HF SDK fetch and merge (`src.py:3490-3680`)
5. Drop rows with `code == "NA"` (`src.py:3663`)
6. **Cast code to str** (`src.py:3666`) — this is the definitive cast
7. Round discharge to 3 dp (`src.py:3673`)
8. Deduplicate on `[code, date]`, keep last (`src.py:3679`)
9. Sort by `[code, date]` ascending (`src.py:3682`)

### Existing dev_code Reference

`apps/preprocessing_runoff/dev_code/convert_daily_runoff.py:125-179` contains `read_calendar_file()` which parses a similar (but not identical) wide format. Key differences from the new 4 files:
- The dev_code version expects year in cell A1; the new files have station name in row 1 and year as the sheet name
- The dev_code version expects data starting at row 3 (0-indexed row 2); the new files start data at row 5

This function is a useful reference but cannot be reused directly.

### Risks Identified During Review

**R1 (Critical): File selection filter collision.** The uzhm data directory contains
`10001_Zeravshan_Inflow_Rovatkhodzha_UZB_SYSTEM.xlsx` (long-format, from the conversion
script). A naive "starts with digit" filter (`f[0].isdigit()`) would pick this up and the
wide-matrix reader would crash or produce garbage. **Mitigation:** filter for files whose
entire stem (before `.xlsx`) consists only of digits: `re.fullmatch(r"\d+", Path(f).stem)`.
This matches `16022.xlsx` but rejects `10001_..._UZB_SYSTEM.xlsx`.

Other files in the directory that are safely excluded:
- `~$16022.xlsx` — excluded by existing `not f.startswith("~")` guard
- `Inflow_Rovatkhodzha_daily_2010_2023.xlsx` — does not start with digit
- `Zeravshan - Inflow to Rovatkhodzha.xlsx` — does not start with digit
- `Daily_Discharge_Uz.zip` — not `.xlsx`
- `EB73359888.pdf` — not `.xlsx`

**R2 (Medium): Per-file reader must never return None.** `parallel_read_excel_files` checks
`df.empty` on the reader's return value (line 782) — if the reader returns `None` instead of
a DataFrame, this raises `AttributeError: 'NoneType' has no attribute 'empty'`. Existing
readers always return DataFrames (empty on skip). The new reader must do the same.

**R3 (Low): Station name in unpredictable merged-cell column.** Row 1 has the name in a
merged cell whose column position varies across files. Must scan the entire row for the
first non-None value rather than reading a fixed column.

**R4 (Info): Code dtype.** Existing readers return `code` as `int64`. Downstream casts to
`str` at `src.py:3666`, so either int or str works, but int matches existing convention.

**R5 (Medium): `code_list` is `list[str]`, not `list[int]`.** All callers construct `code_list`
as `list[str]` (from API responses, JSON config, or manual codes with explicit `str()` casts).
Every existing reader explicitly compares as string before the membership check. The new reader's
`code_list` filter must compare `Path(filename).stem` (a string) directly against `code_list`
(list of strings) — do NOT cast to int before comparison.

**R6 (Medium): `run_locally.sh` runs all modules for unknown orgs.** If `uzhm` is not added to
the skip logic, the local pipeline will attempt to run `preprocessing_gateway`, `machine_learning`,
and `long_term_forecasting` — all of which will fail or produce incorrect results for uzhm.

**R7 (Low): Dead code block in `src.py`.** There is a triple-quoted dead version of
`get_runoff_data_for_sites_HF` at lines 3097–3269. The live function starts at line 3272.
Implementation agents must be aware of this to avoid editing the wrong function.

**R8 (Info): Error message omits `demo`.** The current error in `_read_runoff_data_by_organization`
at line 2969 says `'kghm' or 'tjhm'` — it does not list `'demo'` even though that's a valid branch.
When adding `uzhm`, update the error message to list all four valid organizations.

**R9 (Info): `get_runoff_data_for_sites()` has unhandled inline org dispatch.** The legacy
function `get_runoff_data_for_sites()` (lines 2301–2435) contains three copy-pasted inline
`if kghm / elif tjhm / else raise ValueError` blocks (lines 2335, 2380, 2412) that do NOT
call `_read_runoff_data_by_organization()`. Neither `demo` nor `uzhm` is handled there.
This function is gated by `ieasyhydroforecast_connect_to_iEH == "True"` in the caller
(`preprocessing_runoff.py:358`). The active path for all current deployments is
`get_runoff_data_for_sites_HF()` (line 3272), which uses `_read_runoff_data_by_organization()`.
Additionally, `setup_library.py:365-371` raises an error if `connect_to_iEH=True` AND
org != kghm, providing a second safety net that prevents uzhm from entering the legacy path
even if misconfigured. **No change needed** — but the uzhm `.env` must NOT set
`ieasyhydroforecast_connect_to_iEH=True`.

---

## Implementation Plan

### Approach

Add a new reader function `read_runoff_data_from_uzhm_wide_xlsx()` that:
1. Parses the station code and name from the filename and/or row 1 header
2. Iterates over sheets (one per year), extracting the day×month matrix
3. Unpivots to long format: one row per (date, discharge) pair
4. Skips invalid dates and None values
5. Returns a DataFrame matching the existing output contract

Then add a top-level `read_all_runoff_data_from_uzhm_excel()` that scans the directory and calls the per-file reader, following the same pattern as `read_all_runoff_data_from_excel()`.

Wire it into `_read_runoff_data_by_organization()` under `organization == "uzhm"`.

**Why this approach over conversion scripts:** The conversion script approach (like `convert_daily_runoff.py`) creates duplicate files and requires a manual step. A native adapter reads the source files directly, is less error-prone, and works for future data updates without user intervention.

### Files to Modify

| File | Changes |
|------|---------|
| `apps/preprocessing_runoff/src/src.py` | Add `read_runoff_data_from_uzhm_wide_xlsx()`, `read_all_runoff_data_from_uzhm_excel()`, and `uzhm` branch in `_read_runoff_data_by_organization()`. Update error message (R8). |
| `apps/iEasyHydroForecast/setup_library.py` | Add `uzhm` → `Asia/Tashkent` branch in `get_local_timezone_from_env()` (line 503) |
| `apps/run_locally.sh` | Add `uzhm` to module skip logic in `should_skip_module()` (line 356) |
| `apps/pipeline/pipeline_docker.py` | Add `uzhm` branch in notification email text (line 1368) |
| `bin/utils/build_docker_images.sh` | Exclude `uzhm` from `lt-forecasting` build (line 27) |
| `apps/preprocessing_runoff/test/test_src.py` | Add tests for the new reader functions |
| `bin/utils/common_functions.sh` | Add `uzhm` branch in URL routing (line 113) |

### Implementation Steps

- [ ] **Step 1**: Add `read_runoff_data_from_uzhm_wide_xlsx()` to `src/src.py`
  - Signature: `(filename, code_list, date_col="date", discharge_col="discharge", name_col="name", code_col="code")` — matches existing readers and the `parallel_read_excel_files` contract (all six kwargs by exact name)
  - Parse station code from filename: `Path(filename).stem` (e.g., `"16022"`)
  - Parse station name from row 1 of first sheet: **scan entire row for first non-None cell** (the merged cell column position varies), then split on first space to separate code from name (e.g., `"16022 Syrdariya-Chinaz"` → name = `"Syrdariya-Chinaz"`)
  - Skip files whose code is not in `code_list` — return **empty `pd.DataFrame()`**, never `None` (see R2)
  - For each sheet: try `int(sheet_name)` to extract year — if it fails (e.g., a "Summary" sheet), skip with a log warning
  - Read day×month matrix: iterate rows starting at row 5 (1-indexed), col A = day number, cols B–M = months 1–12
  - Guard against non-numeric day values in col A (summary/average rows after day 31) — skip row if `int(day_val)` fails
  - Use `calendar.monthrange(year, month)[1]` to skip invalid days (Feb 30, Apr 31, etc.)
  - Skip cells where discharge is `None` (these represent impossible dates or missing data)
  - Cast discharge to `float`, code to `int` (matches existing reader convention, see R4)
  - Return DataFrame with columns `[date_col, discharge_col, code_col, name_col]`

- [ ] **Step 2**: Add `read_all_runoff_data_from_uzhm_excel()` to `src/src.py`
  - Signature: `(date_col, discharge_col, name_col, code_col, code_list)` — matches `read_all_runoff_data_from_excel()`
  - Scan `ieasyforecast_daily_discharge_path` for `.xlsx` files using a **pure-digits stem filter**: `re.fullmatch(r"\d+", Path(f).stem)` — this matches `16022.xlsx` but rejects `10001_Zeravshan_..._UZB_SYSTEM.xlsx` and other non-pure-code filenames (see R1)
  - Also apply existing guards: `f.endswith(".xlsx")` and `not f.startswith("~")`
  - Use `parallel_read_excel_files()` with `read_runoff_data_from_uzhm_wide_xlsx` as the reader
  - Return combined DataFrame, or `None` if no data found (matches existing `read_all_runoff_data_from_excel` pattern at `src.py:939-940`)

- [ ] **Step 3**: Add `uzhm` branch to `_read_runoff_data_by_organization()` at `src/src.py:2966`
  - Route to `read_all_runoff_data_from_uzhm_excel()`
  - Update the error message to list all four valid organizations: `'kghm'`, `'tjhm'`, `'demo'`, `'uzhm'` (see R8)

- [ ] **Step 3b**: Add `uzhm` timezone in `apps/iEasyHydroForecast/setup_library.py:503-517`
  - Add `elif organization == "uzhm": return pytz.timezone("Asia/Tashkent")` before the `else` fallback

- [ ] **Step 3c**: Add `uzhm` to local pipeline skip logic in `apps/run_locally.sh:166-173 (variables) and 356-364 (function)`
  - Extend `should_skip_module()` so that `uzhm` skips `preprocessing_gateway`, `machine_learning`, and `long_term_forecasting` (same profile as `demo`)
  - Update the help text at line ~1523 to document `uzhm`

- [ ] **Step 3d**: Add `uzhm` to notification email in `apps/pipeline/pipeline_docker.py:1368-1382`
  - Add `elif ORGANIZATION == "uzhm":` branch listing `PreprocessingRunoff`, `LinearRegression`, `PostProcessingForecasts`, `LogFileCleanup` (same as demo)

- [ ] **Step 3e**: Exclude `uzhm` from lt-forecasting Docker build in `bin/utils/build_docker_images.sh:27`
  - Change `if [ "$ieasyhydroforecast_organization" != "demo" ]` to `if [ "$ieasyhydroforecast_organization" != "demo" ] && [ "$ieasyhydroforecast_organization" != "uzhm" ]`

- [ ] **Step 3f**: Add `uzhm` to URL routing in `bin/utils/common_functions.sh:113-122`
  - Add `elif [ "$env_ending" == "uzhm" ]` branch with:
    - `export ieasyhydroforecast_url_pentad=uzb.fc.pentad.$ieasyhydroforecast_url`
    - `export ieasyhydroforecast_url_decad=uzb.fc.decade.$ieasyhydroforecast_url`
  - Without this change, all 15+ operational scripts that source this file will hard-exit for uzhm deployments

- [ ] **Step 4**: Write unit tests in `test/test_src.py`
  - Create a small test fixture: a temporary `.xlsx` file with 1–2 sheets in the wide matrix format (use openpyxl to create programmatically in a pytest fixture)
  - Test `read_runoff_data_from_uzhm_wide_xlsx()`:
    - Happy path: verify correct date construction, discharge values, code/name extraction
    - Output dtypes: `code` is `int64`, `discharge` is `float64`, `date` is datetime-coercible
    - Invalid dates (Feb 30, Apr 31): verify they produce no rows (not NaN rows)
    - None/missing discharge values: verify they are excluded from output
    - Station code not in code_list: verify empty DataFrame returned (not None)
    - Non-year sheet names (e.g., "Summary"): verify graceful skip with no crash
    - Summary rows beyond day 31 or with non-numeric col A: verify ignored
  - Test `read_all_runoff_data_from_uzhm_excel()`:
    - Multiple files in directory: verify all are read and combined
    - **Mixed directory**: pure-code file (`16022.xlsx`) + long-format file (`10001_Name_SYSTEM.xlsx`) → only pure-code file is read (R1 regression test)
  - Test `_read_runoff_data_by_organization()` with `organization="uzhm"`: verify it routes correctly

### Code Examples

```python
def read_runoff_data_from_uzhm_wide_xlsx(
    filename,
    code_list,
    date_col="date",
    discharge_col="discharge",
    name_col="name",
    code_col="code",
):
    """
    Read daily runoff from Uzbek Hydromet wide-matrix Excel format.

    Each file contains one station. Each sheet represents one year
    (sheet name = year string, e.g., "2000"). Layout per sheet:
    - Row 1: station code + name in a merged cell (position varies),
      e.g., "16022 Syrdariya-Chinaz"
    - Row 3: "Day" label in col A, "Month" label around col G
    - Row 4: month numbers 1–12 in columns B–M
    - Rows 5–35: day number (1–31) in column A, discharge in cols B–M
    - Rows 36+: possible summary rows (ignored)

    Sheets whose name cannot be parsed as an integer year are skipped.
    Invalid dates (e.g., Feb 30) and None discharge cells are excluded.

    Note: This function is called by ``parallel_read_excel_files`` which
    passes exactly these six keyword arguments. The signature must not
    change without updating that caller.

    Args:
        filename: Path to the .xlsx file (e.g., "16022.xlsx").
        code_list: List of station codes (strings) to include. Files
            whose code is not in this list return an empty DataFrame.
        date_col: Output column name for dates.
        discharge_col: Output column name for discharge values.
        name_col: Output column name for station name.
        code_col: Output column name for station code.

    Returns:
        DataFrame with columns [date_col, discharge_col, code_col,
        name_col]. code_col dtype is int64, discharge_col is float64.
        Returns empty DataFrame (never None) if station code not in
        code_list or file contains no valid data.
    """
    ...


def read_all_runoff_data_from_uzhm_excel(
    date_col="date",
    discharge_col="discharge",
    name_col="name",
    code_col="code",
    code_list=None,
):
    """
    Read daily runoff from all Uzbek-format Excel files in the daily
    discharge directory.

    Scans for .xlsx files whose entire stem (filename without extension)
    consists only of digits — e.g., ``16022.xlsx`` matches but
    ``10001_Name_SYSTEM.xlsx`` does not. This prevents accidentally
    parsing long-format converted files that coexist in the same
    directory.

    Args:
        date_col: Output column name for dates.
        discharge_col: Output column name for discharge values.
        name_col: Output column name for station name.
        code_col: Output column name for station code.
        code_list: List of station codes to include.

    Returns:
        Combined DataFrame, or None if no matching files or no data
        found. (Matches the return convention of
        ``read_all_runoff_data_from_excel``.)
    """
    ...
```

---

## Testing

### Test Cases

- [ ] **Unit: happy path** — fixture with 2 sheets (2 years), 3 days × 12 months → verify correct dates, values, code, name
- [ ] **Unit: invalid dates** — Feb 30, Feb 31, Apr 31, Jun 31 → no rows produced
- [ ] **Unit: missing values** — None cells in matrix → excluded from output
- [ ] **Unit: code filtering** — code not in code_list → empty DataFrame
- [ ] **Unit: non-numeric sheet** — sheet named "Summary" → skipped gracefully
- [ ] **Unit: summary rows** — rows beyond day 31 or with non-numeric col A → ignored
- [ ] **Integration: directory scan** — 2 fixture files → combined output with both stations
- [ ] **Integration: organization router** — `organization="uzhm"` routes to the new reader

### Testing Commands

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff
```

### Manual Verification

1. Place the 4 Uzbek `.xlsx` files in the daily discharge directory
2. Set `ieasyhydroforecast_organization=uzhm` in `.env`
3. Run the preprocessing pipeline and verify the output DataFrame has the expected number of rows (~26 years × ~365 days × 4 stations ≈ 38,000 rows)
4. Spot-check: verify `date(2000, 1, 1)` for station 16022 has discharge 832.0

---

## Documentation Impact

- [ ] Module README (`apps/preprocessing_runoff/README.md`) — add uzhm format description
- [ ] `CLAUDE.md` — no change needed (organization routing is an internal detail)
- [ ] Configuration docs (`doc/configuration.md`) — add `uzhm` to recognized `ieasyhydroforecast_organization` values if documented there
- [ ] `apps/run_locally.sh` help text (~line 1523) — add `uzhm` to documented org values

---

## Out of Scope

- Converting the existing station 10001 data (already handled by `convert_daily_runoff.py`)
- Google Sheets integration for uzhm
- iEasyHydro HF API integration for uzhm (separate setup — iEasyHydro will be available for uzhm soon)
- Graceful SDK/DB failure handling for uzhm (not needed — iEasyHydro will be available; uzhm uses the strict kghm/tjhm path)
- Handling the `~$16022.xlsx` lock file pattern (already excluded by existing `not f.startswith("~")` guard)
- Changes to `sapphire/services/` (ownership boundary)
- Timeout manager changes for uzhm (falls back to `demo_ch` / 600s base — acceptable for initial deployment)
- Debug station for uzhm in `forecast_library.py` (kghm/tjhm have org-specific debug stations; not needed now)

## Dependencies

- None — changes are purely additive across `apps/`, `bin/`, and pipeline config

## Acceptance Criteria

**Reader (Steps 1–3):**
- [ ] `ieasyhydroforecast_organization=uzhm` reads all 4 Uzbek `.xlsx` files without error
- [ ] Output DataFrame schema matches other readers: `[date, discharge, code, name]` with `code` as `int64` and `discharge` as `float64`
- [ ] `code_list` filter compares `str` stem against `list[str]` code_list — no int cast before comparison (R5)
- [ ] Invalid dates (Feb 30, etc.) produce no rows
- [ ] Pure-digits stem filter correctly excludes `10001_..._UZB_SYSTEM.xlsx` from the file list (R1 regression test)
- [ ] Per-file reader always returns DataFrame, never None (R2 contract)
- [ ] Error message in `_read_runoff_data_by_organization` lists all four valid organizations (R8)

**Pipeline integration (Steps 3b–3e):**
- [ ] `get_local_timezone_from_env()` returns `Asia/Tashkent` for `uzhm`
- [ ] `run_locally.sh` skips `preprocessing_gateway`, `machine_learning`, `long_term_forecasting` for `uzhm`
- [ ] `build_docker_images.sh` does NOT build `lt-forecasting` for `uzhm`
- [ ] `pipeline_docker.py` notification email lists correct task set for `uzhm`
- [ ] `common_functions.sh` URL routing handles `uzhm` with `uzb.fc.pentad` / `uzb.fc.decade` subdomains, no hard-exit

**General:**
- [ ] All existing tests still pass (zero failures, zero unexpected skips)
- [ ] New tests cover the reader function, directory scanner, and router branch
- [ ] Code follows project conventions (ruff clean)

---

## References

- Existing dev conversion script: `apps/preprocessing_runoff/dev_code/convert_daily_runoff.py`
- Organization router: `apps/preprocessing_runoff/src/src.py:2938`
- Existing single-river reader: `apps/preprocessing_runoff/src/src.py:629`
- Timezone mapping: `apps/iEasyHydroForecast/setup_library.py:503`
- Local pipeline skip logic: `apps/run_locally.sh:166-173 (variables) and 356-364 (function)`
- Pipeline notification email: `apps/pipeline/pipeline_docker.py:1368`
- Docker build script: `bin/utils/build_docker_images.sh:27`
- Dead code block (do not edit): `apps/preprocessing_runoff/src/src.py:3097-3269`
- Source data: `~/Documents/GitHub/uzb_data_forecast_tools/daily_runoff/`
