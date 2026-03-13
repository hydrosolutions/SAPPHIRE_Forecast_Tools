# Plan: Org-Scoped Data Readers (PP-025)

**Status**: In Progress — long-term readers have codes param; short-term read_skill_metrics/read_combined_forecasts still missing it
**Branch**: `develop_long_term_fix_api_postprocessing_forecasts`
**Module**: postprocessing_forecasts, machine_learning
**Depends on**: INFRA-009 (Phases 3-4 must be complete before Phases 2-3 here)

## Context

INFRA-009 (Organization-Based Station Filtering) fixes the upstream station
selection so each pipeline run only processes its own org's stations. However,
several `data_reader` functions still read ALL orgs' data from the API because
they lack a `codes` parameter:

- `read_skill_metrics()` — 8 call sites across 5 files
- `read_combined_forecasts()` — 1 call site
- `read_monthly_combined_forecasts()` — 2 call sites
- `read_quarterly_combined_forecasts()` — 2 call sites
- `read_seasonal_combined_forecasts()` — 2 call sites
- `read_observed_and_modelled_data()` — 2 call sites (missed in original draft)
- ML `_check_ml_forecast_consistency()` — 1 call site
- ML `_read_ml_forecasts_from_api()` — 3 call sites (missed in original draft)

**Key finding**: The API client already supports `code` (singular) filtering on
all relevant methods. The data_reader wrapper functions simply don't pass it
through. The fix is mechanical: add `codes` parameter, loop through codes in
the API helper, update call sites.

---

## Alternatives Analysis

### Deployment Context

Production deploys are **single-org per server** — each hydromet service runs its
own pipeline against its own database. Cross-org contamination only occurs in
**local dev** where a developer switches between orgs (kghm/tjhm/demo) using
different `.env` files while sharing one PostgreSQL instance.

Station codes are naturally disjoint by convention (Kyrgyz codes start with "1",
Tajik with "2", Swiss demo uses different format), but this is not enforced at
the database level.

### Option A: Implement Org-Scoped Reads (This Plan)

**Effort**: ~15 functions modified, ~20 call sites wired
**Pros**: Correct by construction; efficient (reads only needed data); prevents
  subtle bugs in ensemble weight calculation; future-proofs for shared-DB multi-org
**Cons**: Many mechanical changes; depends on INFRA-009 for `codes` variable at
  call sites

### Option B: Reset Database Between Org Switches

**Effort**: Minimal code changes — document the workflow
**Approach**: Run `bin/reset_sapphire_db.sh` when switching between orgs locally.
  Re-migrate from CSV files for the target org.
**Pros**: Zero code changes; already works today
**Cons**: Migration takes ~3h (longforecast bottleneck); CSV files are global
  (no per-org directories), so re-migration imports ALL orgs' data anyway;
  doesn't fix the efficiency issue (reading all data when only 20 stations needed)
**Verdict**: **Does not actually solve the problem** — CSV files and migrators
  are not org-aware, so the DB ends up with all orgs' data regardless.

### Option C: Separate Database Per Org (Docker volumes)

**Effort**: Deployment/infra change, no code changes
**Approach**: Name Docker volumes per org (`sapphire_postprocessing-data-kghm`,
  etc.) and switch via env var or script.
**Pros**: Perfect isolation; no code changes
**Cons**: Must re-migrate per volume; ~3h per org; disk-heavy; fragile if dev
  forgets to switch volumes; doesn't fix reads that pull entire tables (performance)
**Verdict**: Viable for isolation but doesn't address read efficiency.

### Option D: Add `org_id` Column to API Schema

**Effort**: Large — requires changes to `sapphire/services/` (colleague-managed)
**Approach**: Add `organization` column to all 9 tables; update all unique
  constraints; require org_id on every read/write.
**Pros**: Database-level isolation; impossible to leak across orgs
**Cons**: Breaks ownership boundary; massive schema migration; CSV migrators
  need org awareness; station code already serves as natural org boundary
**Verdict**: Over-engineered. Station code filtering achieves the same result
  with much less disruption.

### Option E: Filter After Read at Entry Points Only

**Effort**: ~5 entry points modified (operational, maintenance, recalculate × 2 horizons)
**Approach**: Keep reads unscoped; add `df = df[df["code"].isin(codes)]` after
  every data_reader call in the entry scripts.
**Pros**: Fewest code changes; no data_reader.py modifications
**Cons**: Still reads entire tables (wasteful); filter logic duplicated at every
  call site; easy to miss a site; doesn't fix ML module
**Verdict**: Pragmatic quick fix but doesn't address root cause.

### Recommendation

**Option A** is the right long-term fix. The changes are mechanical and the API
client already supports `code` filtering. However, Option A depends on INFRA-009
Phase 3 (for `_read_station_codes(config)` at call sites).

**Interim**: Option E can be applied immediately as a stopgap while INFRA-009
completes — it requires only that `codes` is available at each entry point
(which it already is in long-term scripts and will be after INFRA-009 Phase 3
for short-term scripts).

---

## API Client Methods (Already Support `code`)

| Client method | File | Accepts `code`? |
|---|---|---|
| `read_skill_metrics(horizon, code, ...)` | `postprocessing_base.py:39` | Yes |
| `read_short_term_forecasts(horizon, code, ...)` | `short_term.py:40` | Yes |
| `read_long_term_forecasts(horizon_type, code, ...)` | `long_term.py:39` | Yes |
| `read_forecasts(horizon, code, ...)` | deprecated alias for above | Yes |

---

## Station Codes Source

All postprocessing modules use `_read_station_codes()` to get station codes from
`config_station_selection.json`. After INFRA-009, this function exists in:

| File | Status after INFRA-009 |
|---|---|
| `postprocessing_operational.py` | Added by INFRA-009 Phase 3 (config-aware) |
| `postprocessing_operational_long_term.py` | Already exists (line 56) |
| `postprocessing_maintenance_long_term.py` | Already exists (line 58) |
| `recalculate_skill_metrics.py` | Already exists (line 95) |
| `postprocessing_maintenance.py` | **Missing — must add in Phase 2** |

For the ML module, station codes are already available as `rivers_to_predict_pentad`
and `rivers_to_predict_decad` within the forecast loop.

---

## Phases

### Phase 1a: Add `codes` Param to Skill Metrics Read Functions

**Goal**: `read_skill_metrics()` and all internal skill metric helpers accept
an optional `codes` parameter and filter API reads accordingly.

**File to modify**: `apps/postprocessing_forecasts/src/data_reader.py`

**Public function** (line 31):
```python
def read_skill_metrics(
    horizon_type: str,
    codes: list[str] | None = None,
) -> pd.DataFrame:
```

**Internal API helpers** to update:
- `_read_skill_metrics_api(horizon_type, codes)` (line 116): If `codes` is not
  None, loop through codes calling `client.read_skill_metrics(horizon=..., code=code, ...)`
  per code instead of reading all. Concatenate results.
- `_read_monthly_skill_metrics_api(codes)` (find by name): Same pattern.
- `_read_quarterly_skill_metrics_api(codes)` (find by name): Same pattern.
- `_read_seasonal_skill_metrics_api(codes)` (find by name): Same pattern.

**CSV fallback**: `_read_skill_metrics_csv(horizon_type)` — add post-read
filter: `if codes: df = df[df["code"].isin(codes)]`.

**Dispatch** (lines 49-54): Pass `codes` through to month/quarter/season helpers.

**Performance note**: The per-code API loop pattern (calling
`client.read_skill_metrics(code=code)` per station) means 63 API calls for
kghm instead of 1. This is acceptable because:
1. The API client already supports only single-code filtering — there is no
   `code__in` batch parameter.
2. The alternative (read all + filter in Python) defeats the purpose of
   org scoping — it still transfers all orgs' data over the wire.
3. If performance becomes an issue, batch `code__in` support should be added
   to the API client as a separate improvement.

For kghm (63 stations), this means ~63 API calls per `read_skill_metrics()`
invocation. With 8 call sites across 5 files in a full pipeline run, worst-case
is ~504 skill metric API calls + ~63 per combined forecast call site. Total
estimated: ~700 additional API calls per pipeline run. Each call is lightweight
(single-station filter on indexed column), so wall-clock impact is estimated at
<30 seconds total assuming 40ms per call. If this proves too slow, batch
`code__in` support should be added to the API client (see API-001).

**Test requirements** (in `apps/postprocessing_forecasts/tests/`):
- Test with `codes=None` returns all data (backward compat)
- Test with `codes=["10001"]` returns only that station's data
- Test with `codes=[]` returns empty DataFrame (or logs warning)
- Test CSV fallback path filters correctly when `codes` is provided
- Mock the API client to verify `code=code` is passed in the per-code loop

---

### Phase 1b: Add `codes` Param to Combined Forecast Read Functions

**Goal**: `read_combined_forecasts()` and its internal helper accept an optional
`codes` parameter.

**File to modify**: `apps/postprocessing_forecasts/src/data_reader.py`

**Public function** `read_combined_forecasts()` (near line 326):
```python
def read_combined_forecasts(
    horizon_type: str,
    codes: list[str] | None = None,
) -> pd.DataFrame:
```

**Internal helper** `_read_combined_forecasts_api(horizon_type, codes)` (line 380):
If `codes` is not None, loop through codes calling
`client.read_short_term_forecasts(horizon=..., code=code, ...)` per code.

**CSV fallback**: Add post-read filter on `code` column.

**Test requirements**: Same pattern as Phase 1a.

---

### Phase 1c: Add `codes` Param to Long-Term Combined Forecast Read Functions

**Goal**: `read_monthly_combined_forecasts()`, `read_quarterly_combined_forecasts()`,
and `read_seasonal_combined_forecasts()` accept an optional `codes` parameter.

**File to modify**: `apps/postprocessing_forecasts/src/data_reader.py`

**Public functions** `read_monthly_combined_forecasts()`, `read_quarterly_combined_forecasts()`, `read_seasonal_combined_forecasts()` (near lines 1019, 2436, 2452):
```python
def read_monthly_combined_forecasts(
    codes: list[str] | None = None,
) -> pd.DataFrame:

def read_quarterly_combined_forecasts(
    codes: list[str] | None = None,
) -> pd.DataFrame:

def read_seasonal_combined_forecasts(
    codes: list[str] | None = None,
) -> pd.DataFrame:
```

**Internal helpers**:
- `_read_monthly_combined_forecasts_api(codes)` (line 1055): If `codes` is not
  None, loop through codes calling
  `client.read_long_term_forecasts(horizon_type="month", code=code, ...)` per code.
- `_read_long_combined_forecasts_api(horizon_type, codes)` (line 2468): Same
  pattern for quarterly/seasonal.

No CSV fallback (API-only for quarterly/seasonal).
Monthly CSV fallback: add post-read filter on `code` column.

**Test requirements**: Same pattern as Phase 1a.

---

### Phase 2: Wire Codes Through Short-Term Maintenance

**Goal**: `postprocessing_maintenance.py` reads station codes and passes them to
all data_reader calls.

**Depends on**: Phase 1a + Phase 1b + INFRA-009 Phase 3 (for `ShortTermHorizonConfig.station_selection_env`)

**File to modify**: `apps/postprocessing_forecasts/postprocessing_maintenance.py`

1. **Add `_read_station_codes(config)`** helper (same pattern as INFRA-009 Phase 3):
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

2. **Update PENTAD/DECAD singletons** to include `station_selection_env` (same
   as INFRA-009 Phase 3 pattern — if not already done by the time this runs).

3. **Wire codes through `_fill_gaps_for_horizon()`** (the main maintenance function):
   - Read codes at start: `codes = _read_station_codes(config)`
   - Pass to `data_reader.read_combined_forecasts(config.name, codes=codes)` (~line 158)
   - Pass to `data_reader.read_skill_metrics(config.name, codes=codes)` (~line 259)

4. **Update test fixtures** in `tests/` to include station selection env vars and
   config files (same pattern as INFRA-009 Phase 3 test updates).

**Test requirements**:
- Verify `_read_station_codes(config)` works for PENTAD and DECAD configs
- Verify `read_combined_forecasts` and `read_skill_metrics` are called with `codes=`
- Existing maintenance tests must continue to pass

---

### Phase 3: Wire Codes Through Remaining Call Sites

**Goal**: All remaining unscoped data_reader calls pass station codes.

**Depends on**: Phases 1a-1c + INFRA-009 Phases 3-4

#### 3a. `postprocessing_operational.py` (1 call site — line 103 only)

The `codes` variable already exists after INFRA-009 Phase 3 (from
`_read_station_codes(config)`). Pass it through:

- Line 103: `skill_stats = data_reader.read_skill_metrics(config.name, codes=codes)`

**Shared dependency note**: Line 93 (`read_observed_and_modelled_data`) is
wired by INFRA-009 Phase 3 — it adds `codes=codes` to that call as part of
the short-term postprocessing scoping work. PP-025 Phase 3a adds `codes=`
to line 103 (`read_skill_metrics`) only. **Recommendation**: implement
INFRA-009 first, then PP-025 Phase 3a adds `codes=` to the additional
skill metrics call.

**Cross-issue coordination note**: PP-025 Phase 3a and INFRA-009 Phases 3-4
overlap on the same call sites in `postprocessing_operational.py` and
`recalculate_skill_metrics.py`. Both modify the same lines. INFRA-009 wires
`codes=` to `read_observed_and_modelled_data()` calls; PP-025 wires `codes=`
to `read_skill_metrics()` and `read_combined_forecasts()` calls on adjacent
lines. Implement INFRA-009 first, then PP-025 Phase 3a adds `codes=` to the
remaining calls.

#### 3b. `postprocessing_operational_long_term.py` (6 call sites)

The `codes` variable already exists (line 81, from `_read_station_codes()`).
Pass to all unfiltered calls:

- Line 88: `skill_stats = data_reader.read_skill_metrics("month", codes=codes)`
- Line 120: `existing = data_reader.read_monthly_combined_forecasts(codes=codes)`
- Line 153: `quarterly_skill = data_reader.read_skill_metrics("quarter", codes=codes)`
- Line 164: `existing_q = data_reader.read_quarterly_combined_forecasts(codes=codes)`
- Line 189: `seasonal_skill = data_reader.read_skill_metrics("season", codes=codes)`
- Line 200: `existing_s = data_reader.read_seasonal_combined_forecasts(codes=codes)`

#### 3c. `postprocessing_maintenance_long_term.py` (6 call sites)

The `codes` variable already exists (line 85, from `_read_station_codes()`).
Pass to all unfiltered calls:

- Line 90: `combined = data_reader.read_monthly_combined_forecasts(codes=codes)`
- Line 118: `skill_stats = data_reader.read_skill_metrics("month", codes=codes)`
- Line 245: `q_combined = data_reader.read_quarterly_combined_forecasts(codes=codes)`
- Line 253: `q_skill = data_reader.read_skill_metrics("quarter", codes=codes)`
- Line 303: `s_combined = data_reader.read_seasonal_combined_forecasts(codes=codes)`
- Line 311: `s_skill = data_reader.read_skill_metrics("season", codes=codes)`

#### 3d. `recalculate_skill_metrics.py` (1 call site)

The `codes` variable already exists (from `_read_station_codes()` at line 95).
Pass it through:

- Line 118: `observed, modelled = data_reader.read_observed_and_modelled_data(config.name, codes=codes)`

**Note**: `read_observed_and_modelled_data()` already accepts `codes`. All other
calls in this file already pass `codes`. This is the only missed site.

**Test requirements**:
- Update wiring integration tests to verify `codes=` is passed to all calls
- For long-term files: existing tests should pass since `codes=None` is backward-compatible

---

### Phase 4: ML Module Org-Scoping

**Goal**: All ML API reads filter by station code.

**Independent of Phases 1-3** (different module, different API client method).

#### 4a. `_check_ml_forecast_consistency()` (1 call site)

**File**: `apps/machine_learning/scr/utils_ml_forecast.py`

**Current code** in `_check_ml_forecast_consistency()` (~line 867):
```python
api_data = client.read_forecasts(
    horizon=horizon_type,
    model=api_model_type,
    start_date=latest_date.strftime("%Y-%m-%d"),
    end_date=latest_date.strftime("%Y-%m-%d"),
)
```

**Fix**: Extract unique codes from `csv_data` DataFrame and pass to API:
```python
codes = csv_data["code"].unique().tolist() if "code" in csv_data.columns else []
frames = []
for code in codes:
    page = client.read_forecasts(
        horizon=horizon_type,
        code=code,
        model=api_model_type,
        start_date=latest_date.strftime("%Y-%m-%d"),
        end_date=latest_date.strftime("%Y-%m-%d"),
    )
    if not page.empty:
        frames.append(page)
api_data = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
```

#### 4b. `fill_ml_gaps.py` and `recalculate_nan_forecasts.py` (2 call sites)

Both files call `_read_ml_forecasts_from_api()` at line 187 without `code=`.

**Important**: Neither script has a per-station loop enclosing this call. Both
read ALL forecasts first, then iterate over `forecast["code"].unique()`. The
variable `river_code` does not exist in either file.

**Fix**: Add `get_hydroposts_for_pentadal_and_decadal_forecasts()` call at the
top of each script's main function to establish the permitted code set, then
post-filter the API results:

In both `fill_ml_gaps.py` and `recalculate_nan_forecasts.py` (after line 187):
```python
# Get permitted codes from org-scoped config
rivers_pentad, rivers_decad, _ = get_hydroposts_for_pentadal_and_decadal_forecasts()
permitted_codes = set(str(c) for c in rivers_pentad + rivers_decad)

forecast = _read_ml_forecasts_from_api(
    model_type=MODEL_TO_USE,
    horizon_type=prefix,
    start_date=api_start,
)
# Filter to current org's stations only
if permitted_codes:
    forecast = forecast[forecast["code"].astype(str).isin(permitted_codes)]
```

Additionally, in `recalculate_nan_forecasts.py`, add an empty-DataFrame guard
before `hindcast["date"]` access (~line 298):
```python
if hindcast.empty or "date" not in hindcast.columns:
    logger.warning("Hindcast returned empty — skipping codes %s", codes_with_nan)
    return
hindcast["date"] = pd.to_datetime(hindcast["date"])
```

#### 4c. `add_new_station.py` (2 call sites — LOW RISK, defer)

Lines 154 and 158 call `_read_ml_forecasts_from_api()` without `code=`.
However, these reads serve **date-range discovery** — they determine `start_date`
and `end_date` across all existing forecasts. Filtering by a single code here
would return only that station's range (likely empty for a new station), breaking
the date window calculation.

The "new codes" detection (lines 201-223) is already gated by the org-scoped
config intersection via `get_hydroposts_for_pentadal_and_decadal_forecasts()`.
Cross-org codes in the API response don't leak into `new_codes_*`.

**Recommendation**: Defer. The cross-org data in these reads is harmless — it
only affects the date range (making it wider, not narrower). The new-station
detection is correctly scoped by config.

**Test requirements**:
- Verify `read_forecasts` / `read_short_term_forecasts` is called with `code=`
- Existing ML tests must continue to pass

---

## Write Operations Analysis

**All writes in postprocessing are implicitly scoped** — they write whatever
DataFrame was computed from reads. If reads are correctly scoped, writes will be
too. Write functions (`api_writer._write_combined_forecast_to_api()`,
`_write_skill_metrics_to_api()`, etc.) do not need `codes` parameters because
they write per-record with the `code` field from each row.

**Risk**: If a read is unscoped and downstream computation doesn't filter by
station code before writing, wrong data could be written. This is mitigated by:
1. Unique constraints in the DB (keyed by `code + date + model_type + ...`)
2. Ensemble/skill calculations that work per-station within the DataFrame
3. No cross-station aggregation in any write path

**Defense-in-depth (optional)**: As a guard against missed call sites or future
regressions, add a lightweight write-side assertion to `api_writer.py` that
checks all `code` values in a batch belong to the configured station list.
This should be a warning log (not a hard error) to avoid breaking production
if the assertion is wrong. Implementation: read `codes` from
`config_station_selection.json` once at module init; before each
`client.write_*()` call, check `set(batch_codes) <= set(configured_codes)`.

**Accepted risk**: The write-side assertion is advisory only. Read-side scoping
remains the primary control. Station code disjointness across orgs is by
convention, not enforced at the database level (see INFRA-009 for a startup
assertion proposal).

---

## Out-of-Scope Modules

The following modules have unscoped API reads/writes but are excluded from this
plan. They receive their station lists from upstream config files which are
implicitly org-scoped (created by HF SDK login). A separate issue
(INFRA-011) should track these if shared-DB multi-org becomes a production
requirement.

| Module | Unscoped calls | Why out of scope |
|---|---|---|
| `linear_regression` | `client.read_runoff()`, `client.write_lr_forecasts()`, `client.write_hydrograph()`, `client.write_runoff()` | Writes are per-station from upstream config; reads are scoped by station loop |
| `preprocessing_gateway` | `client.read_meteo()`, `client.write_meteo()`, `client.read_snow()`, `client.write_snow()` | Reads/writes scoped by station loop from config |
| `preprocessing_runoff` | `client.write_runoff()`, `client.write_hydrograph()` | Writes scoped by HF SDK auth (returns only current org's stations) |
| `iEasyHydroForecast` | Multiple `client.read_*()`, `client.write_*()` | All reads loop per-station; writes per-station |
| `validate_pipeline` | All `client.read_*()` methods | Intentionally reads all data globally for validation. **Risk**: gives false positives in multi-org DB (Org A validation passes if Org B has data). Tracked in INFRA-011 with `--org` flag proposal. |
| `forecast_dashboard` | `requests.get()` to API endpoints | Display-only; individual data fetches are per-station. **Risk**: station list dropdown (`all_stations.pkl`) shows ALL orgs' stations — no org filter on the list itself. Deferred — dashboard is display-only and low risk. |
| `long_term_forecasting` | `get_meteo_data()`, `get_runoff_data()` in `data_interface.py` | Raw SQL scoping deferred — see INFRA-011 "Deferred: long_term_forecasting Raw SQL Scoping" section and INFRA-012 deferred note for canonical tracking. |

---

## Verification

1. **Unit tests**: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts`
2. **ML tests**: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning`
3. **Full test suite**: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh`

---

## Dependency Graph

```json
{
  "phases": {
    "phase_1a": {
      "name": "Add codes param to skill_metrics read functions",
      "depends_on": [],
      "note": "Pure plumbing — adds optional param with None default, no call sites change",
      "files": [
        "apps/postprocessing_forecasts/src/data_reader.py (read_skill_metrics + _read_skill_metrics_api + _read_monthly_skill_metrics_api + _read_quarterly_skill_metrics_api + _read_seasonal_skill_metrics_api + _read_skill_metrics_csv)"
      ]
    },
    "phase_1b": {
      "name": "Add codes param to read_combined_forecasts",
      "depends_on": [],
      "note": "Pure plumbing — adds optional param with None default, no call sites change",
      "files": [
        "apps/postprocessing_forecasts/src/data_reader.py (read_combined_forecasts + _read_combined_forecasts_api + CSV fallback)"
      ]
    },
    "phase_1c": {
      "name": "Add codes param to monthly/quarterly/seasonal combined reads",
      "depends_on": [],
      "note": "Pure plumbing — adds optional param with None default, no call sites change",
      "files": [
        "apps/postprocessing_forecasts/src/data_reader.py (read_monthly_combined_forecasts + read_quarterly_combined_forecasts + read_seasonal_combined_forecasts + _read_monthly_combined_forecasts_api + _read_long_combined_forecasts_api)"
      ]
    },
    "phase_2": {
      "name": "Wire codes through short-term maintenance",
      "depends_on": ["phase_1a", "phase_1b", "INFRA-009 Phase 3"],
      "note": "Adds _read_station_codes to maintenance.py, passes codes to read_combined_forecasts and read_skill_metrics",
      "files": [
        "apps/postprocessing_forecasts/postprocessing_maintenance.py",
        "apps/postprocessing_forecasts/tests/ (fixture updates)"
      ]
    },
    "phase_3": {
      "name": "Wire codes through operational + long-term + recalculation call sites",
      "depends_on": ["phase_1a", "phase_1b", "phase_1c", "INFRA-009 Phases 3-4"],
      "note": "codes variable already exists in all files — add codes= to existing calls. Includes read_observed_and_modelled_data in operational.py:93 and recalculate_skill_metrics.py:118",
      "files": [
        "apps/postprocessing_forecasts/postprocessing_operational.py (1 call: read_skill_metrics — line 93 read_observed_and_modelled_data is wired by INFRA-009 Phase 3, do not duplicate)",
        "apps/postprocessing_forecasts/postprocessing_operational_long_term.py (6 calls)",
        "apps/postprocessing_forecasts/postprocessing_maintenance_long_term.py (6 calls)",
        "apps/postprocessing_forecasts/recalculate_skill_metrics.py (1 call: read_observed_and_modelled_data)"
      ]
    },
    "phase_4": {
      "name": "ML module org-scoping (all unscoped reads)",
      "depends_on": [],
      "note": "Independent — different module. 4a: consistency check, 4b: fill_ml_gaps + recalculate_nan, 4c: add_new_station",
      "files": [
        "apps/machine_learning/scr/utils_ml_forecast.py (_check_ml_forecast_consistency)",
        "apps/machine_learning/fill_ml_gaps.py (pass code to _read_ml_forecasts_from_api)",
        "apps/machine_learning/recalculate_nan_forecasts.py (pass code to _read_ml_forecasts_from_api)",
        "apps/machine_learning/add_new_station.py (pass code to _read_ml_forecasts_from_api)"
      ]
    }
  },
  "execution_order": [
    {"parallel": ["phase_1a", "phase_1b", "phase_1c", "phase_4"]},
    {"parallel": ["phase_2", "phase_3"]}
  ],
  "notes": [
    "Phase 1a/1b/1c and Phase 4 are independent — different files or different modules.",
    "Phase 1a/1b/1c all modify data_reader.py but touch DIFFERENT functions — can run in parallel if agents work on separate function groups. However, if git conflicts are a concern, run sequentially.",
    "Phases 2 and 3 both depend on Phase 1 completion + INFRA-009.",
    "Phases 2 and 3 can run in parallel (different files).",
    "Phase 1 is the largest phase (single file, many functions). Phase 4 is small and self-contained."
  ]
}
```

Phase 1a/1b/1c and Phase 4 can run in parallel (different files/modules).
Phases 2 and 3 can run in parallel after Phase 1 completes (different files).
Phases 2 and 3 also require INFRA-009 to be complete (for `station_selection_env`
on `ShortTermHorizonConfig` and `_read_station_codes(config)` in operational files).

**Orchestration note**: Phase 1a/1b/1c are the largest phases (single file, many
functions). They CAN be parallelized in separate git worktrees since they touch
different functions, but sequential execution avoids merge conflicts. Phases 2-3
are mechanical wiring. Phase 4 is a small, self-contained fix.
