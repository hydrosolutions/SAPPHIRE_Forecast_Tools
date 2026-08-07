# ML-007: Non-Deterministic API Pagination Causes Inconsistent ML Gap Detection

**Status**: Review (Option 2 implemented 2026-03-20)
**Module**: `machine_learning` / infra (cross-cutting — involves
`sapphire/services/` coordination)
**Priority**: Medium
**Labels**: `bug`, `api-integration`, `reliability`
**Blocked By**: Option 1 (API-side ORDER BY) still requires coordination
with `sapphire/services/` owner. Option 2 (per-code reads) is implemented.

---

## Summary

`_read_ml_forecasts_from_api()` in
`apps/machine_learning/scr/utils_ml_forecast.py` reads all station codes
in a single paginated query. The underlying PostgreSQL endpoint in
`sapphire/services/postprocessing/` has no explicit `ORDER BY`, so DB
pages are returned in arbitrary order. With ~55 codes × 730 days ×
multiple target dates per forecast = **~442,000 rows** (89 pages at
5,000 rows/page), two consecutive runs return different subsets per code.
Gap detection in `fill_ml_gaps.py` and NaN detection in
`recalculate_nan_forecasts.py` are therefore unreliable.

### Verified 2026-03-19

- The postprocessing API contains **~442,000 TFT day-horizon rows** for
  the 730-day window — data is present for the entire 2024–2026 range
- Pagination is **confirmed non-deterministic**: consecutive page fetches
  return overlapping/missing row IDs with no stable ordering
- The `code` query parameter works correctly end-to-end (exact string
  match on indexed `String(10)` column in PostgreSQL)
- Per-code queries return ≤730 rows — well within a single page,
  eliminating the pagination non-determinism

---

## Reconfirmed 2026-07-23 — still unfixed, and wider than ML

Independent reconfirmation during a local pipeline health review (taj + kyg,
`maxat_sapphire_2` @ `16fb9a9b`):

- **Direct control experiment**: two consecutive full-window reads of
  `/long-forecast/` with **no writes in between** returned **47,975 vs 45,460 keys**
  (15,553 "added", 18,068 "removed"). Pagination instability is trivially reproducible
  today; Option 1 is still not implemented.
- **Concrete scope for the Option 1 fix**: `sapphire/services/postprocessing/app/crud.py`
  has **0 of 6** paginated list readers with an `order_by` (forecast, long forecast, LR
  forecast, skill metric, bulletin, LR visibility). By contrast
  `sapphire/services/preprocessing/app/crud.py` has **4 of 4** ordered (runoff,
  hydrograph, meteo, snow) — so preprocessing is the working template for the fix.
- **Not only ML**: `apps/postprocessing_forecasts/src/data_reader.py:1446`
  (`_read_long_forecasts_api`) paginates per code in 1,000-row batches. ML-007's Option 2
  (per-code reads) is only safe while each per-code result stays within one page; a
  multi-year, multi-model, multi-lead long-forecast result exceeds that, so the
  postprocessing recalc path is exposed too.
- **Consequence**: this instability blocks attribution of **PP-049** (long-term skill
  recalc output varying across identical invocations). PP-049 cannot be confirmed as an
  independent defect until ML-007 is fixed or bypassed.

Still **colleague-owned** (`sapphire/services/`) — coordination required, no edits made.

## Root Cause Analysis

### Observed behaviour

Two consecutive invocations of `_read_ml_forecasts_from_api()` for the
same station code returned different date ranges — one included 2026
data, the other did not. The query parameters were identical.

### Why pagination produces non-determinism

PostgreSQL does not guarantee row order unless `ORDER BY` is specified.
When a paginated query uses `LIMIT`/`OFFSET` (or keyset pagination)
without an explicit ordering clause, the set of rows falling on each
page is arbitrary and may vary between requests as a result of:

- Planner choosing different index scans
- Buffer pool state
- Concurrent writes filling gaps in heap pages

With ~442,000 rows split across 89 pages, any single page boundary can
land at a different point in the dataset on each call. The caller
receives a structurally valid response (correct number of rows per page)
but with a silently different date range per station code.

### Where the bug manifests

| File | Location | Effect |
|------|----------|--------|
| `utils_ml_forecast.py` | `_read_ml_forecasts_from_api()` | Paginated read with no ordering guarantee |
| `fill_ml_gaps.py` | Gap detection logic | Consumes non-deterministic read; detects phantom gaps on each run, triggering multi-hour hindcasts |
| `recalculate_nan_forecasts.py` | NaN detection logic | Same non-deterministic read; may miss NaN records |

### Affected callers — blast radius

| Caller | Lookback | Estimated rows | Risk |
|--------|----------|---------------|------|
| `fill_ml_gaps.py` | 730 days | ~442K (89 pages) | **Critical** — phantom gaps trigger multi-hour hindcasts |
| `recalculate_nan_forecasts.py` | 730 days | ~442K (89 pages) | **High** — misses NaN records non-deterministically |
| `add_new_station.py` | No date filter | All rows | Not in scope (see below) |
| `make_forecast.py` | 60 days | ~5K (1 page) | **Not affected** — fits in one page |

### Ownership note

The PostgreSQL query lives in
`sapphire/services/postprocessing/` (colleague-managed). Per CLAUDE.md,
this code must not be modified without coordination. Option 1 below
requires an explicit coordination step before implementation.

---

## Implementation Plan

### Option 1 — API-side fix (preferred, future)

**What**: Ask the `sapphire/services/` owner to add
`ORDER BY id` (or `ORDER BY code, date`) to the forecast list
endpoint in `sapphire/services/postprocessing/`. No client-side changes
required.

**Status**: Blocked — requires coordination with colleague. Will be
pursued separately. In the meantime, Option 2 provides a reliable
client-side workaround.

---

### Option 2 — Client-side per-code reads (implementing now)

**What**: Change the two affected callers (`fill_ml_gaps.py` and
`recalculate_nan_forecasts.py`) to load org-permitted station codes
from config first, then call `_read_ml_forecasts_from_api(..., code=X)`
once per code. Each per-code query returns ≤730 rows (fits in one page),
eliminating the non-determinism.

**Design decisions**:

1. **Extract shared helper**: Both files have byte-for-byte identical
   org-filter blocks (lines 221–244). Extract a
   `get_permitted_station_codes() -> set[str] | None` helper into
   `utils_ml_forecast.py` and call it from both files. Returns `None`
   when config is unavailable (not empty set — `None` means "no filter
   available", empty set means "org has no stations").

2. **Keep existing `_read_ml_forecasts_from_api` unchanged**: The
   function already accepts `code: str | None` — no signature change
   needed. Callers loop and concatenate.

3. **Fallback when config unavailable**: If `get_permitted_station_codes()`
   returns `None`, fall back to the current all-codes-in-one-query
   behavior with a warning. This preserves existing behavior in edge
   cases (missing config files) and doesn't make things worse.

4. **CSV fallback preserved with org-filter**: If the per-code API read
   returns empty for ALL codes, the existing CSV fallback path still
   triggers. Because the CSV contains all orgs' data, we must re-apply
   the org-filter after the CSV fallback. Use `permitted_codes` (already
   loaded) to filter the CSV DataFrame — same logic as current code, but
   using the shared helper's result instead of re-loading config.

5. **Post-read org-filter replaced, not blindly removed**: The inline
   org-filter block (lines 221–244) is removed from the API-read path
   because filtering now happens at the query level. However, it is
   **retained for two fallback paths**:
   - CSV fallback: CSV contains all orgs → must filter
   - Config-unavailable fallback: all-codes API read → already
     unfiltered in current code, so no regression

**What is NOT changed**:

- `_read_ml_forecasts_from_api()` signature and behavior — unchanged
- `make_forecast.py` — 60-day window fits in one page, not affected
- `add_new_station.py` — needs to discover absent codes by reading all
  forecasts and comparing against config; per-code reads would break
  this (can't query for codes that don't exist). It's also a manual
  utility, not part of the operational pipeline.
- `setup_library.py` — separate implementation in `iEasyHydroForecast`,
  different function with `site_codes: list[str]` parameter

**Performance**: ~55 codes × 1 API call × ~100ms/call ≈ 5.5 seconds.
Acceptable compared to the multi-hour hindcasts triggered by phantom
gaps. No batching needed.

**Known overhead (accepted)**: `_read_ml_forecasts_from_api()` performs
a readiness check and creates a new `SapphirePostprocessingClient` on
every call. With 55 per-code calls, this adds ~55 health-check
round-trips (~5.5s) on top of the data calls. Total ~11s. This is still
far better than multi-hour hindcasts and does not warrant changing the
shared function's internal logic. A future optimization could pass a
pre-checked client or add a `skip_readiness_check` flag.

---

## Phased Implementation

### Phase 1: Extract `get_permitted_station_codes()` helper

**What**: Extract the duplicated org-filter config loading logic from
`fill_ml_gaps.py` lines 221–244 (and identical code in
`recalculate_nan_forecasts.py`) into a shared helper function in
`utils_ml_forecast.py`.

**Function contract**:

```python
def get_permitted_station_codes() -> set[str] | None:
    """Load org-scoped station codes from config files.

    Reads station codes from the pentad and decad config files
    specified by environment variables. Returns the union of both.

    Returns:
        Set of station code strings, or None if config is unavailable
        (missing env vars, missing files, malformed JSON). None signals
        "no filter available" — callers should fall back to reading all
        codes. An empty set means "org has no stations configured".
    """
```

**Implementation details**:

- Read `ieasyforecast_configuration_path` + `ieasyforecast_config_file_station_selection`
  → parse `stationsID` as list, convert to `set[str]`
- Optionally read `ieasyforecast_config_file_station_selection_decad`
  → union into the same set
- On any exception: log at DEBUG level, return `None`
- No caching (matches existing behavior — config is read fresh each call)
- `json` is already imported at module level in `utils_ml_forecast.py`
  (line 25) — no import change needed

**Files changed**:

- `apps/machine_learning/scr/utils_ml_forecast.py` — add function
  (no `__all__` exists in this file — function is available via direct
  import, no export boilerplate needed)

**Verification**: Function is pure config-loading, no callers changed
yet. Existing tests must still pass.

---

### Phase 2: Per-code reads in `fill_ml_gaps.py`

**What**: Replace the single all-codes API call with a per-code loop.

**Before** (lines 186–244):
```python
api_start = (datetime.date.today() - timedelta(days=730)).isoformat()
forecast = _read_ml_forecasts_from_api(
    model_type=MODEL_TO_USE, horizon_type=prefix, start_date=api_start,
)
# ... CSV fallback ...
# ... org-scoped filter (lines 221-244) ...
```

**After**:
```python
api_start = (datetime.date.today() - timedelta(days=730)).isoformat()
permitted_codes = get_permitted_station_codes()

if permitted_codes is not None and len(permitted_codes) > 0:
    # Per-code reads — each query ≤730 rows, fits in one page
    frames = []
    for code in sorted(permitted_codes):
        df = _read_ml_forecasts_from_api(
            model_type=MODEL_TO_USE,
            horizon_type=prefix,
            start_date=api_start,
            code=code,
        )
        if not df.empty:
            frames.append(df)
    forecast = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
else:
    # Config unavailable — fall back to all-codes query (existing behavior)
    logger.warning(
        "fill_ml_gaps: org config unavailable — falling back to all-codes read "
        "(non-deterministic pagination may produce phantom gaps)"
    )
    forecast = _read_ml_forecasts_from_api(
        model_type=MODEL_TO_USE, horizon_type=prefix, start_date=api_start,
    )

# CSV fallback (unchanged — triggers if API returned empty)
if forecast.empty:
    # ... existing CSV fallback code (lines 195-210, unchanged) ...
    #
    # IMPORTANT: CSV contains all orgs' data. Re-apply org-filter after
    # CSV read if permitted_codes is available:
    if permitted_codes is not None and len(permitted_codes) > 0 and not forecast.empty:
        forecast = forecast[forecast["code"].astype(str).isin(permitted_codes)]

# Second emptiness guard (lines 212-219, PRESERVED — do not remove).
# Catches the case where both API and CSV returned no data (or CSV
# data was entirely filtered out by the org-filter above).
if forecast.empty:
    logger.warning(
        "fill_ml_gaps: Both API and CSV returned no data for %s %s. "
        "Gap detection skipped — forecasts may be missing.",
        MODEL_TO_USE, prefix,
    )
    return

# Old inline org-filter block (lines 221-244) REMOVED — filtering now
# happens either at the API query level (per-code reads) or after CSV
# fallback (above). The config-unavailable fallback reads all codes,
# matching current behavior (current org-filter also silently skips).
```

**Key behavioral differences from current code**:

1. Reads per-code instead of all-at-once → deterministic results
2. Org-scoped filtering happens BEFORE the API call, not after
3. Sorted iteration over codes for deterministic logging
4. When config is unavailable, behavior is identical to current code

**Risk assessment**:

| Risk | Mitigation |
|------|-----------|
| Config returns empty set (no stations) | Check `len(permitted_codes) > 0` — fall back to all-codes read |
| Per-code read returns partial data for one code | Each per-code query fits in one page — no pagination needed |
| API is slow/unavailable | Same timeout behavior as before; ~11s for 55 codes (incl. health checks) is acceptable |
| Codes in config but not in DB | API returns empty DataFrame for unknown codes — harmless |
| Codes in DB but not in config | Not read — same as current behavior (org-filter drops them) |
| CSV fallback returns all orgs' data | Re-apply org-filter using `permitted_codes` after CSV read |
| Readiness check × 55 | Accepted overhead (~5.5s); doesn't warrant changing shared function |

**Files changed**:

- `apps/machine_learning/fill_ml_gaps.py` — replace API call + remove
  org-filter block

**Verification**: Run `SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning`

---

### Phase 3: Per-code reads in `recalculate_nan_forecasts.py`

**What**: Same API-call + org-filter replacement as Phase 2, applied to
`recalculate_nan_forecasts.py`. The replacement region is the same
(lines ~187–244: API call, CSV fallback, org-filter block).

**IMPORTANT — differences from `fill_ml_gaps.py`**: The code AFTER line
244 differs between the two files. Do NOT copy Phase 2's diff blindly.
Specifically:

| Aspect | `fill_ml_gaps.py` (Phase 2) | `recalculate_nan_forecasts.py` (Phase 3) |
|--------|----------------------------|----------------------------------------|
| CSV fallback date parsing | Lines 202-203: explicit `pd.to_datetime` for `forecast_date` and `date` | Not present — dates are parsed later at lines 253-260 |
| Post-filter logic | Null-discharge filter (lines 246-255), then gap detection | Date parsing + dedup (lines 251-267), then NaN flag detection |

The agent must **only replace lines ~190–244** (API call + CSV fallback
+ org-filter) and leave everything after line 244 untouched. The CSV
fallback in `recalculate_nan_forecasts.py` does not have the
`pd.to_datetime` calls — do not add them.

As in Phase 2, **preserve the second `if forecast.empty:` guard**
(lines 211-218) — it catches the case where both API and CSV are empty.

**Files changed**:

- `apps/machine_learning/recalculate_nan_forecasts.py` — replace API
  call + remove org-filter block (same pattern as Phase 2, respecting
  the CSV fallback differences noted above)

**Verification**: Run `SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning`

---

### Phase 4: Tests

**What**: Add tests for the new helper and verify all existing tests pass.

**Tests to add**:

1. **`get_permitted_station_codes()` unit tests** (in existing test file
   or new `test_utils_ml_forecast.py`):
   - Valid config with pentad + decad files → returns union of codes
   - Valid config with pentad only (no decad env var) → returns pentad codes
   - Missing config file → returns `None`
   - Malformed JSON → returns `None`
   - Empty `stationsID` list → returns empty set (not `None`)

2. **Verify all 37 existing ML tests pass with zero skips**

**Files changed**:

- `apps/machine_learning/test/` — add tests for helper

**Verification**: `SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning`

---

## Acceptance Criteria

- [ ] `get_permitted_station_codes()` returns correct codes from config
- [ ] `fill_ml_gaps.py` reads forecasts per-code when config is available
- [ ] `recalculate_nan_forecasts.py` reads forecasts per-code when config is available
- [ ] Both files fall back to all-codes read when config is unavailable
- [ ] CSV fallback still works when API returns empty
- [ ] No increase in false-negative gap detections
- [ ] All existing ML tests pass with zero skips
- [ ] New unit tests for `get_permitted_station_codes()` pass

---

## Out of Scope

- **`add_new_station.py`**: Needs to discover absent codes by reading
  all forecasts. Per-code reads would break this. It's a manual utility,
  not part of the operational pipeline.
- **`make_forecast.py`**: 60-day lookback fits in one API page. Not
  affected by the pagination bug.
- **`setup_library.py`**: Separate `_read_ml_forecasts_from_api()`
  implementation in `iEasyHydroForecast` with different signature
  (`site_codes: list[str]`). Not in scope.
- **API-side ORDER BY (Option 1)**: Requires coordination with
  `sapphire/services/` owner. Will be pursued separately.
- **Performance optimisation**: 55 sequential API calls (~5.5s) is
  acceptable. Async/parallel calls are a future enhancement.

---

## Dependency Graph

```json
{
  "phases": {
    "phase_1_extract_helper": {
      "name": "Extract get_permitted_station_codes() into utils_ml_forecast.py",
      "depends_on": [],
      "files": [
        "apps/machine_learning/scr/utils_ml_forecast.py"
      ],
      "notes": "Pure addition — no callers changed, no behavior change"
    },
    "phase_2_fill_ml_gaps": {
      "name": "Per-code reads in fill_ml_gaps.py",
      "depends_on": ["phase_1_extract_helper"],
      "files": [
        "apps/machine_learning/fill_ml_gaps.py"
      ],
      "notes": "Replace all-codes API call with per-code loop; remove post-read org-filter"
    },
    "phase_3_recalculate_nan": {
      "name": "Per-code reads in recalculate_nan_forecasts.py",
      "depends_on": ["phase_1_extract_helper"],
      "files": [
        "apps/machine_learning/recalculate_nan_forecasts.py"
      ],
      "notes": "Same lines ~187-244 replaced as phase 2, but CSV fallback differs (no pd.to_datetime). Do NOT copy phase 2 diff blindly. Can run in parallel with phase 2."
    },
    "phase_4_tests": {
      "name": "Add tests and verify all existing tests pass",
      "depends_on": ["phase_2_fill_ml_gaps", "phase_3_recalculate_nan"],
      "files": [
        "apps/machine_learning/test/"
      ],
      "notes": "Unit tests for get_permitted_station_codes(); verify 37 existing tests pass"
    }
  },
  "out_of_scope": [
    "add_new_station.py — needs to discover absent codes; per-code reads would break this",
    "make_forecast.py — 60-day lookback fits in one page",
    "setup_library.py — separate implementation in iEasyHydroForecast",
    "Option 1 (API-side ORDER BY) — requires colleague coordination"
  ]
}
```

---

## Related Issues

- **ML-004** — Root issue investigation (Bug E) where non-deterministic
  pagination was first observed
- **ML-003** — ML gap filling and API read alignment
- **Observation 2026-03-19** — "Machine Learning: Recurring Gap-Fill
  Between 2024 and 2026 on Every Run" in `doc/plans/observations.md`
