# PREPGW-001: Snow SWE Data Not Updated by Operational Run

**Priority**: Mid
**Module**: `preprocessing_gateway`
**Status**: Draft — Investigation
**Date**: 2026-03-19
**Branch**: `develop_long_term_fix_api_postprocessing_forecasts`

---

## Problem Statement

After running `preprocessing_gateway` operationally via `run_locally.sh`, the
SWE snow data in the preprocessing API contains **only records with year-2000
dates** of unclear origin. No current-year (2026) snow observations appear for
stations 15189 and 16059. The year-2000 records may be migration artifacts
rather than climatological norms — see H6.

**Verification commands used:**
```bash
curl -s "http://localhost:8000/api/preprocessing/snow/?code=15189&snow_type=SWE&limit=5" | python3 -m json.tool
curl -s "http://localhost:8000/api/preprocessing/snow/?code=16059&snow_type=SWE&limit=5" | python3 -m json.tool
```

Both return only records with year-2000 dates (starting at 2000-01-01).

---

## Background: Snow Data Pipeline

### Write Path

1. `snow_data_operational.py` fetches 365 days of snow data from the **Data
   Gateway (DG)** for each configured HRU and variable (SWE, HS, RoF).
2. DG response is a CSV parsed by `dg_utils.transform_snow_data()`.
3. Result is merged with existing CSV (dedup on `date+code`).
4. **Current order (WRONG)**: CSV write first (line 373), API write second
   (line 380). API failure is non-fatal; CSV failure aborts. This treats CSV
   as primary and API as best-effort — the opposite of what we want.
5. **Required order**: API write first, CSV write second as deprecation-path
   backup. See "Write-Order Fix" section below.
6. `dg_utils.write_snow_to_api()` writes to the preprocessing API:
   - **Operational mode** (default): only data from `yesterday` onward (2-day
     window).
   - **Maintenance mode**: last 30 days.
   - **Initial mode**: all data.

### Norm Path

- `recalculate_snow_norms.py` computes climatological daily norms from
  historical CSVs and writes a full year (365/366 records) to the API with
  `value=None` and `norm=computed_mean`.
- Norm records use **real-year dates** (e.g., `2026-01-15`) — the code builds
  a date range from `f"{year}-01-01"` (line 126) and formats with
  `dt.strftime("%Y-%m-%d")` (line 166).
- The year-2000 dates visible in the API are **NOT** from
  `recalculate_snow_norms.py`. They likely originate from `data_migrator.py`
  (which migrates CSV data without populating the `norm` field) or from
  `snow_data_renalysis.py`. See H6 below.

### Key Code Paths

| File | Function | Role |
|------|----------|------|
| `snow_data_operational.py` | `get_snow_data_operational()` (line 281) | Fetch + transform + merge + write |
| `dg_utils.py` | `write_snow_to_api()` (line 420) | API write with sync-mode windowing |
| `dg_utils.py` | `transform_snow_data()` (line 223) | Parse DG CSV format |
| `recalculate_snow_norms.py` | `recalculate_norms()` | Annual norm computation |
| `dg_utils.py` | `_read_existing_norms()` (line 380) | Preserve norms during writes |

---

## Hypotheses

### H1: Data Gateway returns no data for these stations
The DG API call may return empty/invalid data for HRUs containing stations
15189 and 16059. This is similar to the KGZ500 issue from 2026-02-18 where
`transform_snow_data` failed on alphanumeric codes.

**Check**: Inspect the raw DG CSV file saved in `OUTPUT_PATH_DG` after a run.

### H2: Operational write window misses the data
In operational mode, `write_snow_to_api` only writes data from `yesterday`
onward. If the DG returns historical data but nothing for today/yesterday,
`data_to_write` will be empty and the API write silently returns `False`.

**Check**: Look at the date range in the DG response — does it include
today/yesterday?

### H3: `transform_snow_data` fails silently for these HRUs
The transformation may return an empty DataFrame for certain column naming
patterns, causing `df_combined` to have no rows and the API write to skip.

**Check**: Log the DataFrame shape after transformation.

### H4: **CONFIRMED DEFICIENCY** — Snow API endpoint missing ORDER BY (not the root cause)

> **Note**: This is a confirmed service-layer bug that complicates the
> investigation but is NOT the root cause of missing 2026 SWE data.

The snow endpoint in `crud.py:262` has **no `order_by()` clause**, unlike all
other preprocessing endpoints (runoff, hydrograph, meteo all use
`order_by(Model.code, Model.date)`). This means:

- `limit=5` returns records in **arbitrary insertion order**, not date order
- Pagination with `skip`/`limit` is unpredictable
- Recent data may be hidden beyond the default `limit=100` window

This is a **service-layer bug** in `sapphire/services/preprocessing/app/crud.py`.
Per CLAUDE.md ownership boundaries, this requires coordination with the
colleague who manages `sapphire/services/` — it should be filed as a separate
issue rather than fixed directly.

**Impact on this investigation**: Phase 1 verification queries must use
`start_date` filters rather than relying on `limit` to surface recent records.

### H5: snow_data_operational.py is not called at all
If the DG API key (`ieasyhydroforecast_API_KEY_GATEAWAY`) is missing or the
script errors before reaching the API write, no snow data is written.

**Check**: Examine run_locally.sh logs for the preprocessing_gateway phase.

Two distinct failure modes:
- **Hard exit**: Missing `ieasyhydroforecast_API_KEY_GATEAWAY` triggers
  `sys.exit(1)`, which aborts the entire `run_locally.sh` preprocessing
  gateway loop — visible as a non-zero exit code in logs.
- **Silent per-HRU failure**: DG call raises exception → `return False` for
  that HRU/variable combination, but processing continues for others.

### H6: Year-2000 records are migration artifacts, not norms

The year-2000 dates in the API do not match any current code path:
- `recalculate_snow_norms.py` writes real-year dates (2026-01-15)
- `snow_data_operational.py` writes dates from the DG response
- `data_migrator.py` migrates CSV data but does NOT populate the `norm` field

These records may be migration artifacts with incorrect dates, or they may
come from an older version of `snow_data_renalysis.py`. Understanding their
origin matters for two reasons: (1) they may confuse
API queries that don't filter by date (see H4), and (2) in **maintenance or
initial mode**, `_read_existing_norms()` queries the API using the date range
of `data_to_write` — if data spans a wide range, it could read back and
re-write these artifacts. In **operational mode**, the query window is only
yesterday/today, so year-2000 artifacts cannot be picked up.

**Check**: Query the API for records with dates before 2001-01-01. Inspect
`_read_existing_norms()` to see if it filters by date range or returns all
records including year-2000 artifacts.

### H7: ~~Dedup concat order silently discards fresh DG data~~ — **ELIMINATED**

**Status**: Pre-eliminated during code review (2026-03-19).

`get_snow_data_operational()` (line 354) uses
`pd.concat([old_dataframe, df_transformed])` with `keep="last"` — new data is
appended second and preserved. Same order confirmed in `snow_data_renalysis.py`
(line 336). No further investigation needed.

### H8: ~~Date dtype mismatch after transform silently empties the write window~~ — **ELIMINATED**

**Status**: Pre-eliminated during safety review (2026-03-19).

Three layers of defense already prevent this:
1. `transform_snow_data()` coerces at line 234: `pd.to_datetime(df["date"], dayfirst=True)`
2. Both callers (operational line 341, reanalysis) call `pd.to_datetime()` before passing to `write_snow_to_api()`
3. `write_snow_to_api()` itself coerces at line 484: `data["date"] = pd.to_datetime(data["date"])`

Additionally, `test_data_transforms.py` line 579 explicitly asserts
`is_datetime64_any_dtype(result["date"])`. No further investigation needed.

---

## Investigation Plan

### Phase 0: Check Existing Logs (manual, ~2 min)

Check for log output from the operational run that surfaced the issue. If
`run_locally.sh` was already run, logs may still exist and can immediately
narrow the hypothesis space without requiring a live DG.

```bash
# Check for recent preprocessing_gateway log output
# (location depends on run_locally.sh log configuration)
ls -lt /tmp/sapphire_logs/ 2>/dev/null || echo "No log dir found"
# Also check terminal scrollback from the run that surfaced the issue
```

### Phase 1: Quick API Verification (manual, ~5 min)

**Prerequisite**: Services must be running (`cd sapphire && docker-compose up -d`).

Confirm whether the issue is a query problem (H4) or a real data absence.
Because the snow endpoint lacks ORDER BY (H4 confirmed), use `start_date`
filters instead of relying on `limit` alone.

> **Caveat**: These queries assume the `start_date` filter itself works
> correctly. The unfiltered count-by-year query below provides a ground-truth
> check independent of any filter logic.

```bash
# Check for current-year data (bypasses ordering issue)
curl -s "http://localhost:8000/api/preprocessing/snow/?code=15189&snow_type=SWE&start_date=2026-01-01&limit=100" | python3 -m json.tool

# Ground-truth: unfiltered count by year (independent of filter bugs)
curl -s "http://localhost:8000/api/preprocessing/snow/?code=15189&snow_type=SWE&limit=10000" | python3 -c "
import sys, json
from collections import Counter
d = json.load(sys.stdin)
years = Counter(r['date'][:4] for r in d)
print(f'Total: {len(d)}')
for y, c in sorted(years.items()):
    print(f'  {y}: {c} records')
"

# Check for year-2000 artifacts (H6)
curl -s "http://localhost:8000/api/preprocessing/snow/?code=15189&snow_type=SWE&end_date=2001-01-01&limit=10" | python3 -m json.tool

# Repeat for station 16059
curl -s "http://localhost:8000/api/preprocessing/snow/?code=16059&snow_type=SWE&start_date=2026-01-01&limit=100" | python3 -m json.tool
```

Also check the **source CSV files** for year-2000 records (tests H6 directly):

```bash
# Check if source CSVs contain year-2000 dates
head -5 $OUTPUT_PATH_SNOW/SWE/*_SWE.csv 2>/dev/null
grep "^2000-" $OUTPUT_PATH_SNOW/SWE/*_SWE.csv 2>/dev/null | head -10
```

### Phase 2: Trace the Data Flow (agent + manual)

Split into code-reading (agent-executable) and runtime verification (manual).
H7 was pre-eliminated during review — concat order is `[old, new]` with
`keep="last"` in both operational (line 354) and reanalysis (line 336).

#### Phase 2a-i: Concat/dedup + logging coverage (agent)

1. **Verify date dtype after transform** (H8): In `transform_snow_data()`
   (line 223), check whether the returned DataFrame has `date` column as
   `datetime64` or `object` (string). If string, the comparison
   `data["date"] >= yesterday` in `write_snow_to_api()` (line 507) may
   silently fail. Also check if `write_snow_to_api` coerces the date column
   before filtering.
2. **Verify logging coverage**: Check that existing logging in
   `get_snow_data_operational()` and `write_snow_to_api()` is sufficient to
   diagnose the issue without adding debug statements. Key log messages:
   - "Data Gateway returned N rows" (DG response)
   - "Combined CSV: N rows" (after merge)
   - "Snow API sync mode: X" and "writing N snow records" (API write)
   - Pre-filter empty → `logger.info` (H3 path)
   - Post-filter empty → `logger.warning` (H2 path, operational mode only)
   - **Gap identified**: No log between transform and date-window filter to
     catch H8 (date dtype mismatch).

#### Phase 2a-ii: DG date range + write window (agent)

1. **Check DG date range vs operational window**: The DG client fetches 365
   days of data. The operational write window filters to `>= yesterday` (line
   507 in `dg_utils.py`). If the DG returns only historical data with no
   rows for yesterday/today, the window will always be empty. Read the DG
   client code to determine what date range it actually returns.
2. **Inspect `_read_existing_norms()` behavior** (line 380): It queries the
   API using `data_to_write["date"].min()` and `.max()` as start/end dates.
   In operational mode, this means it only queries yesterday/today — it
   **cannot** pick up year-2000 artifacts. Confirm this and narrow H6's
   concern to maintenance/initial modes only.

#### Phase 2a-iii: Callers + norms + migrator (agent)

1. **Check all callers of `write_snow_to_api()`**: Besides
   `snow_data_operational.py`, `snow_data_renalysis.py` (line 357) also calls
   it with `mode="maintenance"`. Note that reanalysis passes
   `reference_date=df_combined["date"].max()`, which shifts the 30-day
   maintenance window relative to the data's max date (not wall clock). If
   reanalysis was run with historical data where `date.max()` is old, the
   maintenance window could overlap with year-2000 mystery records if the
   CSV data was malformed.
2. **Inspect `data_migrator.py` date handling**: The `SnowDataMigrator`
   (line 521 in `sapphire/services/preprocessing/app/data_migrator.py`) reads
   dates directly from CSV without transformation or validation. If source
   CSVs had year-2000 dates, migrator would write them as-is. Check what CSV
   files the migrator reads and whether they could contain year-2000 records.

#### Phase 2b: Runtime Verification (manual, requires live DG)

**Prerequisites**: Running services, valid `.env` with
`ieasyhydroforecast_API_KEY_GATEAWAY`, DG accessible.

**Depends on**: Phase 2a-i/ii/iii findings (targeted logging based on code
analysis — especially H8 date dtype check).

1. Run `snow_data_operational.py` and inspect log output for the key messages
   identified in Phase 2a-i step 2.
2. Inspect the raw DG CSV at `OUTPUT_PATH_DG` for stations 15189/16059.
3. Inspect the merged CSV at `OUTPUT_PATH_SNOW/SWE/{hru}_SWE.csv` for 2026
   data.
4. Check the API write return value in the logs.

### Phase 3: Check Configuration (agent, no dependencies)

> **Note**: Environment variable values are in deployment-specific `.env` files
> (path set by `ieasyhydroforecast_env_file_path`), not checked into the repo.
> The agent can verify which env vars the code reads; checking their actual
> values requires manual inspection.

1. **Trace HRU→station mapping**: `ieasyhydroforecast_HRU_SNOW_DATA` is read
   at `snow_data_operational.py:428` as a comma-separated list. Trace: env
   var → HRU list → DG query parameters → how station codes appear in the DG
   response. If 15189/16059 are station codes but the HRU list uses different
   identifiers, no data would ever be fetched for them.
2. **Verify DG API key**: Is `ieasyhydroforecast_API_KEY_GATEAWAY` set in the
   `.env` file?
3. **Check if maintenance mode is also affected**: Does
   `bin/daily_gateway_maintenance.sh` also fail to write snow data? (Confirmed
   the script exists and sets `SAPPHIRE_SYNC_MODE=maintenance` at line 111.)
4. **Check `snow_data_renalysis.py`**: Has reanalysis ever been run? This
   script also calls `write_snow_to_api()` with `mode="maintenance"` (line
   357). Note: it passes `reference_date=df_combined["date"].max()`, so the
   30-day window is relative to the data, not wall clock.
5. **Check `SAPPHIRE_SYNC_MODE` env var**: The write window in
   `write_snow_to_api()` depends on `os.getenv("SAPPHIRE_SYNC_MODE",
   "operational")` (line 496 in `dg_utils.py`). Verify this is set correctly
   in the `.env` file — an unexpected value changes the write window.

### Phase 4a: Write-Order Fix (agent, no investigation dependencies)

**Confirmed safe to implement immediately** — does not depend on root cause
investigation. The reorder is safe because:
- `write_snow_to_api()` works on the in-memory DataFrame, never reads CSV
- `_read_existing_norms()` reads from API, not CSV
- `_check_snow_consistency()` receives DataFrame as parameter, compares to API
- No code between CSV and API write depends on the CSV file existing

**Changes required in two files** (snow-only, not other gateway modules):

#### `snow_data_operational.py` (lines 371-388)

Current:
```python
# CSV first (primary)
try:
    df_combined.to_csv(file_path, index=False)
except Exception as e:
    logger.error("Error saving file %s: %s", file_path, e)
    return False

# API second (best-effort)
try:
    written = dg_utils.write_snow_to_api(df_combined, variable, hru)
    if written:
        _check_snow_consistency(df_combined, variable, hru)
except SapphireAPIError as e:
    logger.error(...)
```

Required (API first, CSV backup):
```python
# API first (primary)
try:
    written = dg_utils.write_snow_to_api(df_combined, variable, hru)
    if written:
        _check_snow_consistency(df_combined, variable, hru)
except SapphireAPIError as e:
    logger.error("Error writing snow data to API (HRU %s, %s): %s",
                 hru, variable, e)

# CSV second (backup — will be deprecated once API is proven reliable)
try:
    df_combined.to_csv(file_path, index=False)
except Exception as e:
    logger.error("Error saving CSV backup %s: %s", file_path, e)
    # CSV failure is no longer fatal — API write is the primary path
```

**Key behavior changes**:
- API failure is non-fatal (same as before — logged, continues)
- CSV failure is **no longer fatal** — was `return False`, now just logs
- Function always returns `True` if it reaches the write section (data was
  fetched and transformed successfully)
- CSV write is preserved unchanged (same `.to_csv()` call, same path)

#### `snow_data_renalysis.py` (lines 348-369)

Same reorder pattern. Keep `mode="maintenance"` and
`reference_date=df_combined["date"].max()` parameters unchanged.

#### `recalculate_snow_norms.py`

**No change needed** — already writes to API only (line 201), no CSV write
path exists.

### Phase 4b: Root Cause Investigation Fix (agent, depends on Phases 1, 2b, 3)

Based on investigation findings:
- If H1 (DG returns no data): Document which stations/HRUs are affected and
  whether this is a DG configuration issue or a code bug.
- If H2 (window misses data): Consider whether operational mode should use a
  wider window, or whether the DG latency means data is always >1 day old.
- If H3 (transform fails): Fix `transform_snow_data` for the affected column
  pattern.
- If H4 (missing ORDER BY): File as a **separate issue** for the
  `sapphire/services/` maintainer. Per CLAUDE.md ownership boundaries, do not
  edit `crud.py` directly. Document the inconsistency and propose the fix
  (`query.order_by(Snow.code, Snow.date)`) in the issue description.
- If H5 (script not called): Fix `run_locally.sh` or environment configuration.
- If H6 (year-2000 artifacts): Determine if `_read_existing_norms()` needs a
  date range guard to avoid re-writing artifacts. If so, fix in `dg_utils.py`
  (which is in `apps/`, not services). **Also plan cleanup** of bad records
  from the API — this touches `sapphire/services/` (ownership boundary) so
  coordinate with colleague. Options: API DELETE endpoint, direct DB query,
  or re-run migrator with corrected source CSVs.

### Phase 5: Write Tests (agent, depends on Phase 4a + 4b)

- Add/update tests for the **write-order change**: verify API is called before
  CSV write, verify CSV failure no longer aborts, verify API failure still
  allows CSV write to proceed
- If a root-cause code fix was needed: add unit test for the identified failure
- Ensure test covers the specific station/HRU pattern that failed
- Tests must not touch `sapphire/services/`

---

## Acceptance Criteria

- [ ] **Write-order fix**: API write happens before CSV write in both
      `snow_data_operational.py` and `snow_data_renalysis.py`
- [ ] **Write-order fix**: CSV write preserved unchanged (same call, same path)
- [ ] **Write-order fix**: CSV failure no longer fatal (log only)
- [ ] **Write-order fix**: Tests verify new order and error-handling behavior
- [ ] Root cause identified and documented
- [ ] If code bug: fix implemented with tests
- [ ] If configuration issue: documented with instructions
- [ ] Observation in `doc/plans/observations.md` updated with findings
- [ ] Separate issue filed for `sapphire/services/` maintainer: missing
      `order_by(Snow.code, Snow.date)` in `crud.py:get_snow` (H4)
- [ ] If year-2000 artifacts confirmed (H6): cleanup plan coordinated with
      `sapphire/services/` maintainer
- [ ] Manual verification: stations 15189/16059 show 2026 SWE data after
      operational run (requires live DG — cannot be agent-verified)

---

## Dependency Graph

```json
{
  "phases": {
    "phase_0": {
      "name": "Check Existing Logs",
      "type": "manual",
      "depends_on": [],
      "description": "Check for log output from the operational run that surfaced the issue."
    },
    "phase_1": {
      "name": "Quick API Verification",
      "type": "manual",
      "depends_on": [],
      "description": "Query API with date filters + unfiltered count-by-year + source CSV check. Requires running services."
    },
    "phase_2a_i": {
      "name": "Code: date dtype + logging coverage",
      "type": "agent",
      "depends_on": [],
      "description": "Verify date dtype after transform_snow_data (H8 — pre-eliminated, confirm), check logging coverage gaps."
    },
    "phase_2a_ii": {
      "name": "Code: DG date range + write window",
      "type": "agent",
      "depends_on": [],
      "description": "Trace DG response date range vs operational window filter, _read_existing_norms behavior."
    },
    "phase_2a_iii": {
      "name": "Code: callers + norms + migrator",
      "type": "agent",
      "depends_on": [],
      "description": "All callers of write_snow_to_api (incl. reference_date effect), data_migrator.py date handling."
    },
    "phase_3": {
      "name": "Check Configuration",
      "type": "agent",
      "depends_on": [],
      "description": "Trace HRU→station mapping, env vars, SAPPHIRE_SYNC_MODE, maintenance mode. Agent checks code; actual env values require manual inspection."
    },
    "phase_4a": {
      "name": "Write-Order Fix",
      "type": "agent",
      "depends_on": [],
      "description": "Reorder snow writes: API first, CSV backup second. Two files: snow_data_operational.py, snow_data_renalysis.py. Do not change CSV write logic itself. CSV failure becomes non-fatal (log only)."
    },
    "phase_2b": {
      "name": "Runtime Verification",
      "type": "manual",
      "depends_on": ["phase_2a_i", "phase_2a_ii", "phase_2a_iii"],
      "description": "Run snow_data_operational.py with targeted logging based on code analysis. Requires live DG and valid .env."
    },
    "phase_4b": {
      "name": "Root Cause Investigation Fix",
      "type": "agent",
      "depends_on": ["phase_1", "phase_2b", "phase_3"],
      "description": "Implement root-cause fix based on investigation findings. File ORDER BY issue (ownership boundary). Fix apps/ code if needed."
    },
    "phase_5": {
      "name": "Write Tests",
      "type": "agent",
      "depends_on": ["phase_4a", "phase_4b"],
      "description": "Tests for write-order change (API before CSV, CSV failure non-fatal) + root-cause fix. Must not touch sapphire/services/."
    }
  },
  "graph": {
    "phase_0": [],
    "phase_1": [],
    "phase_2a_i": [],
    "phase_2a_ii": [],
    "phase_2a_iii": [],
    "phase_3": [],
    "phase_4a": [],
    "phase_2b": ["phase_2a_i", "phase_2a_ii", "phase_2a_iii"],
    "phase_4b": ["phase_1", "phase_2b", "phase_3"],
    "phase_5": ["phase_4a", "phase_4b"]
  }
}
```

**Execution diagram:**
```
phase_0 (manual) ──────────────────────────────────────────────────────┐
phase_1 (manual) ──────────────────────────────────────────────────────┤
phase_2a_i (agent) ───┐                                               │
phase_2a_ii (agent) ──┼── phase_2b (manual) ──── phase_4b (agent) ──┤
phase_2a_iii (agent) ─┘                                               │
phase_3 (agent) ──────────────────────────────── phase_4b ───────────┤
phase_4a (agent) ─────────────────────────────────────────────────────┤
                                                                       └── phase_5 (agent)
```

**Parallelism**: 5 agent tasks (2a-i, 2a-ii, 2a-iii, 3, **4a**) + 2 manual
tasks (0, 1) can all start immediately. Phase 4a (write-order fix) has **no
investigation dependencies** — it is a confirmed-safe structural change that
can execute in the first wave alongside the investigation phases.
