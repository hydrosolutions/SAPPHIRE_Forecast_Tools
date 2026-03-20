# ML-008: `fill_ml_gaps.py` triggers infinite hindcast loop on flag=3 rows

**Status**: Draft
**Module**: `machine_learning`
**Priority**: High
**Labels**: `bug`, `infinite-loop`, `operational-pipeline`, `data-integrity`

---

## Summary

`fill_ml_gaps.py` enters an infinite retry loop because its gap-detection
logic excludes null-discharge rows (`flag=3`) from the date sequence, then
detects the resulting date gaps and triggers `hindcast_ML_models.py` to fill
them. The hindcast runs but produces `flag=3` output again for the same dates
(the model cannot produce valid forecasts when input data is absent). On the
next pipeline run the same exclusion, the same gaps, and the same hindcast
trigger repeat — indefinitely.

Confirmed on station 15189 TFT over a 730-day window:

| Metric | Count |
|--------|-------|
| Total rows | 8,041 |
| flag=3 (null discharge) | 2,563 |
| Valid rows | 5,478 |
| Spurious gaps after null exclusion | 3 |

The three spurious gaps span 35, 32, and 169 days respectively. Across all
53 operational stations the cumulative wasted hindcast compute is hours per
run.

---

## Root Cause Analysis

### The loop, step by step

```
┌─────────────────────────────────────────────────────────────────┐
│  fill_ml_gaps() — one pipeline run                              │
│                                                                 │
│  1. Read API (730-day window) → DataFrame with flag=3 rows      │
│                                                                 │
│  2. Line 257: exclude rows where Q50 is null                    │
│     → flag=3 rows disappear from the DataFrame                  │
│                                                                 │
│  3. Lines 263-296: iterate codes, detect date gaps              │
│     → gaps appear because flag=3 dates are absent               │
│                                                                 │
│  4. Lines 315-331: call hindcast_ML_models.py for gap range     │
│     → hindcast produces flag=3 output (model cannot succeed)    │
│                                                                 │
│  5. Lines 338-354: merge hindcast rows into forecast DataFrame  │
│     → flag=3 hindcast rows written to API / CSV                 │
│                                                                 │
│  ↺  Next run: go to step 1 — nothing has changed               │
└─────────────────────────────────────────────────────────────────┘
```

### The key lines

**Null-discharge exclusion** (`fill_ml_gaps.py` lines 248-257):

```python
# CURRENT — excludes flag=3 rows before gap detection
if "Q50" in forecast.columns:
    n_nulls = forecast["Q50"].isna().sum()
    if n_nulls > 0:
        logger.info(
            "fill_ml_gaps: Excluding %d null-discharge rows from gap detection",
            n_nulls,
        )
        forecast = forecast[forecast["Q50"].notna()].copy()
```

This removes every row where Q50 is null. Because flag=3 rows carry `Q50=NULL`
by design, they are entirely removed from the working DataFrame before gap
detection begins.

**Gap detection loop** (`fill_ml_gaps.py` lines 259-296):

```python
for code in forecast.code.unique():
    forecast_code = forecast[forecast.code == code].copy()
    forecast_dates = forecast_code["forecast_date"].unique()
    forecast_dates = pd.DatetimeIndex(forecast_dates).sort_values()
    missing_forecasts = []

    for i in range(1, len(forecast_dates)):
        if (forecast_dates[i] - forecast_dates[i - 1]).days > limit_day_gap:
            missing_tuple = (forecast_dates[i - 1], forecast_dates[i])
            missing_forecasts.append(missing_tuple)
    ...
```

After the null exclusion, the gap detection sees only valid rows. Every
contiguous block of flag=3 dates appears as a gap, triggering an unnecessary
hindcast.

**Hindcast trigger** (`fill_ml_gaps.py` lines 304-331):

```python
try:
    hindcast = call_hindcast_script(
        min_missing_date,
        max_missing_date,
        MODEL_TO_USE,
        intermediate_data_path,
        PREDICTION_MODE,
    )
```

`call_hindcast_script()` launches `hindcast_ML_models.py` as a subprocess.
The subprocess writes `flag=3` rows to the API / CSV for every date where
input data is absent. Those flag=3 rows are re-read at step 1 of the next
run and excluded again, restarting the loop.

### Why flag=3 rows exist

The hindcast script uses the comment header (lines 6-10 of
`recalculate_nan_forecasts.py`) to describe the flag convention:

- `flag=3` — NaN value even after hindcast attempt (model produced no output)
- `flag=4` — valid value produced by hindcast

`fill_ml_gaps.py` has no awareness of this convention. It sees a null Q50
and treats the row as if no attempt had ever been made.

---

## Related Problem: `recalculate_nan_forecasts.py`

`recalculate_nan_forecasts.py` has a structurally similar, though less
severe, problem. It searches for rows with `flag=1` or `flag=2` (lines
276-284), triggers a hindcast for those date ranges, and then replaces the
flag=1/2 rows with the hindcast result via `update_forecast()`.

If the hindcast produces `flag=3` output for some of those rows, the next run
will find the same dates with `flag=3` but will NOT trigger another hindcast
— because `flag=3` is not in the `[1, 2]` filter. **This script does not
share the infinite-loop bug.** The hindcast replaces flag=1/2 with either
flag=4 (success) or flag=3 (permanent failure); both exit the retry cycle.

No fix is required for `recalculate_nan_forecasts.py` with respect to this
issue. It is mentioned here for completeness.

---

## Implementation Plan

### Approach: Remove the null-Q50 filter (restore original intent)

The null-discharge exclusion filter at lines 248-257 was added in commit
`0adf23b9` (Mar 12, 2026) during the API migration. It was intended to
prevent "phantom" API rows from interfering with gap detection. However,
**it directly contradicts the file's own documented contract** (lines 10-11):

> NOTE: This script only fills in the values which are not represented in
> the forecast file. If there are nan values in the forecast file, they
> will not be filled in.

The filter removes null-Q50 rows (including flag=3, flag=1, flag=2), making
those dates "not represented" and triggering gap detection for them. This
violates the separation of concerns:

- **`fill_ml_gaps.py`** fills DATE GAPS — dates with no row at all
- **`recalculate_nan_forecasts.py`** fixes NaN VALUES — dates with rows but
  bad data (flag=1/2)

The correct fix is to **delete the filter entirely**, restoring the original
behavior where any row (regardless of Q50 value or flag) counts as a
"represented" date for gap detection purposes.

### Phase 1: Remove the null-Q50 filter

**File**: `apps/machine_learning/fill_ml_gaps.py`
**Lines**: 248-257

Delete the entire null-discharge exclusion block:

```python
# DELETE these lines (248-257):
    # Treat null-discharge rows as missing — they are phantom records
    # that should not count as valid forecasts for gap detection.
    if "Q50" in forecast.columns:
        n_nulls = forecast["Q50"].isna().sum()
        if n_nulls > 0:
            logger.info(
                "fill_ml_gaps: Excluding %d null-discharge rows from gap detection",
                n_nulls,
            )
            forecast = forecast[forecast["Q50"].notna()].copy()
```

No replacement code is needed. The gap detection loop (lines 259-296) works
correctly on the unfiltered DataFrame:

- Dates with flag=3 rows (null Q50) are present → no phantom gap
- Dates with flag=1/2 rows (operational NaN) are present → no gap; handled
  by `recalculate_nan_forecasts.py` instead
- Dates with no row at all → gap detected → hindcast triggered (correct)

**Why this is safe**: In the original CSV-only code, the operational forecast
CSV contained all rows (including NaN-valued ones). Gap detection worked on
this full set. The API returns the same data — removing the filter restores
the original behavior.

### Phase 1.5: Harden API error logging in hindcast and gap-fill scripts

The gap detection fix depends on API data being complete: if `fill_ml_gaps.py`
cannot read from the API and falls back to CSV, the CSV may have stale or
incomplete data, potentially missing rows that would prevent false gaps. If
`hindcast_ML_models.py` cannot write to the API, gap-fill results may be lost
before the next run reads them back.

Currently both scenarios log at WARNING level, which is easily missed.
Escalate to ERROR.

**Step 1.5a — Escalate hindcast API write failures to ERROR**

**File**: `apps/machine_learning/hindcast_ML_models.py`
**Lines**: 495-500, 514-519

Change `logger.warning` to `logger.error` in two places:

1. When `_write_ml_forecast_to_api` returns `False` (line ~496):
```python
# BEFORE:
logger.warning(
    "API write returned failure for hindcast %s %s",
    MODEL_TO_USE,
    HINDCAST_MODE,
)
# AFTER:
logger.error(
    "hindcast_ML_models: API write returned failure for %s %s "
    "— hindcast rows may not be visible to fill_ml_gaps on next run",
    MODEL_TO_USE,
    HINDCAST_MODE,
)
```

2. When the API was unavailable and data exists only in CSV (line ~515):
```python
# BEFORE:
logger.warning(
    "Hindcast data for %s/%s exists only in CSV — API write failed or unavailable",
    MODEL_TO_USE,
    HINDCAST_MODE,
)
# AFTER:
logger.error(
    "hindcast_ML_models: Hindcast data for %s/%s exists only in CSV "
    "— API write failed or unavailable; fill_ml_gaps gap detection "
    "may be incomplete until data is in the API",
    MODEL_TO_USE,
    HINDCAST_MODE,
)
```

Note: The exception handler at line ~502 already logs at ERROR level — no
change needed there.

**Step 1.5b — Escalate fill_ml_gaps API read failure to ERROR**

**File**: `apps/machine_learning/fill_ml_gaps.py`
**Lines**: 218-222

Change the CSV-fallback warning from `logger.warning` to `logger.error`:

```python
# BEFORE:
logger.warning(
    "fill_ml_gaps: API returned no %s %s forecasts — falling back to CSV",
    MODEL_TO_USE,
    prefix,
)
# AFTER:
logger.error(
    "fill_ml_gaps: API returned no %s %s forecasts — falling back to CSV. "
    "Gap detection may be unreliable without complete API data.",
    MODEL_TO_USE,
    prefix,
)
```

**Rationale**: These are not cosmetic changes. Gap detection requires a
complete picture of which dates have forecast rows. If the API is unavailable
and the CSV is stale, gaps may be incorrectly detected or missed. ERROR-level
logging ensures operators detect this immediately.

---

### Phase 2: Tests

**File**: `apps/machine_learning/test/test_fill_ml_gaps_null_loop.py` (new)

**Import preamble**: The new test file requires the same `sys.modules` mock
setup as `test_fill_ml_gaps.py` (lines 23-39) to handle heavy dependencies
(darts, pytorch_lightning, torch, setup_library) that are imported at module
level by `fill_ml_gaps.py`.

Test that removing the null-Q50 filter resolves the infinite loop. Tests
exercise the full `fill_ml_gaps()` function with mocked API reads and
hindcast calls. Follow Arrange → Act → Assert.

| # | Scenario | Asserts |
|---|----------|---------|
| 1 | Contiguous dates, some with null Q50 (flag=3) → no gap detected | `call_hindcast_script` not called |
| 2 | Genuine date gap (missing dates, no rows at all) → hindcast triggered | `call_hindcast_script` called with correct date range |
| 3 | Mix of flag=3 rows and genuine gaps → only genuine gaps trigger hindcast | `call_hindcast_script` called once, with gap range only |
| 4 | All dates present (some flag=0, some flag=3) → no hindcast | `call_hindcast_script` not called |
| 5 | API read fallback to CSV → ERROR logged | `logger.error` emitted with "falling back to CSV" message |

Mocking strategy: patch `_read_ml_forecasts_from_api` to return synthetic
DataFrames; patch `call_hindcast_script` to verify it is/isn't called;
capture log records with `caplog` (pytest).

---

### Phase 3: Verify end-to-end

**Step 3.1 — Run the test suite**

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning
```

All tests must pass with zero skips (except valid `SAPPHIRE_API_AVAILABLE`
dependency-gate skips).

**Step 3.2 — Local pipeline smoke test**

With Docker services running and station 15189 data present:

```bash
bash apps/run_locally.sh all
```

Observe that `fill_ml_gaps.py` does NOT launch `hindcast_ML_models.py` for
the three previously-spurious gaps (35-day, 32-day, 169-day on station
15189). These dates have flag=3 rows in the API — with the filter removed,
those rows make the dates "represented" and no gap is detected.

**Step 3.3 — Idempotency check**

Run `fill_ml_gaps.py` twice in succession. The second run must produce
identical output — no new hindcast, no new rows written.

---

## Acceptance Criteria

- [ ] The null-Q50 filter (lines 248-257) is removed from `fill_ml_gaps.py`
- [ ] `fill_ml_gaps.py` does not trigger a hindcast for dates that have any
      row in the database (regardless of Q50 value or flag)
- [ ] Genuinely missing dates (no row at all) still trigger a hindcast
- [ ] Running `fill_ml_gaps.py` twice in succession on the same data produces
      identical results (idempotent)
- [ ] All 5 new unit tests pass
- [ ] Existing `test_fill_ml_gaps.py` tests still pass (check that the
      `TestNullDischargeFilter` tests are updated or removed to match)
- [ ] Full ML test suite passes with zero skips
- [ ] No changes to `sapphire/services/` (ownership boundary respected)
- [ ] The fix is compatible with the per-code API reads implemented in ML-007
- [ ] `hindcast_ML_models.py` logs at ERROR level (not WARNING) when API
      write fails or is unavailable
- [ ] `fill_ml_gaps.py` logs at ERROR level (not WARNING) when falling back
      to CSV because the API returned no data

---

## Risks and Mitigations

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Null-Q50 rows included in gap detection dates | Intended | Matches Sandro's original design — gap detection checks date existence, not value quality |
| CSV write now includes flag=3 rows from API | Low impact | Same as original CSV-only behavior; CSV is deprecated fallback, will be removed |
| Existing `TestNullDischargeFilter` tests break | Expected | These tests validated the filter we're removing; update or remove them |
| API write fails in hindcast → rows lost before next run | Medium | Phase 1.5 escalates to ERROR so operators detect immediately |
| API read fails in fill_ml_gaps → stale CSV used | Medium | Phase 1.5 escalates to ERROR; stale CSV may trigger unnecessary hindcasts but not infinite loops |

---

## Out of Scope

- Making the hindcast subprocess itself succeed for periods with missing
  input data — that is a data-availability problem, not a control-flow bug
- Changing flag semantics or adding new flag values (requires
  `sapphire/services/` coordination)
- `recalculate_nan_forecasts.py` — does not share this infinite-loop bug
  (see "Related Problem" section above)
- Purging existing flag=3 rows from the database — a separate operational
  decision
- Removing the unused `_write_ml_daily_forecast_to_api` function in
  `utils_ml_forecast.py` — dead code, separate cleanup task
- Flag=2 semantic collision in `make_forecast.py` — pre-existing convention
  issue in original code, not a regression from API migration. Reviewed
  2026-03-20: deleted ML-011 (no behavioral impact; `.isin([1, 2])` catches
  both flag values identically)

---

## Related Issues

- **ML-007**: Per-code API reads in `fill_ml_gaps.py` and
  `recalculate_nan_forecasts.py` — prerequisite; this fix depends on the
  per-code read pattern being in place
- **ML-006**: Shape mismatch in `recalculate_nan_forecasts.py` — sibling
  issue in the same module; fix there serves as reference for the loop
  structure
- **ML-001**: Hindcast subprocess failure handling — the suppression logic
  in this fix reduces the frequency of subprocess calls; both issues improve
  operational reliability
- ~~**ML-011**~~: Deleted 2026-03-20 — flag=2 collision has no behavioral
  impact (`.isin([1, 2])` catches both values; no code change needed)
- **ML-012**: `recalculate_nan_forecasts.py` line 334 crashes on NaN flags —
  `hindcast["flag"].astype(int)` without `errors="ignore"` raises ValueError
  if any hindcast row has missing flag
- **ML-013**: `recalculate_nan_forecasts.py` API write sends full hindcast
  DataFrame (line 405), not just replaced rows — can overwrite valid flag=0
  operational data in the API
- ~~**ML-014**~~: Covered by ML-012 — flag=None dtype cascade is the root
  cause that ML-012 fixes; no separate issue needed

---

## References

- `apps/machine_learning/fill_ml_gaps.py:7-11` — original file documentation
  specifying that NaN values should NOT be filled (only missing dates)
- `apps/machine_learning/fill_ml_gaps.py:248-257` — null-discharge exclusion
  block (the code to remove — introduced in commit `0adf23b9`, Mar 12 2026)
- `apps/machine_learning/fill_ml_gaps.py:259-296` — gap detection loop
  (unchanged by this fix)
- `apps/machine_learning/fill_ml_gaps.py:304-331` — hindcast trigger
  (unchanged by this fix)
- `apps/machine_learning/recalculate_nan_forecasts.py:6-10` — flag
  convention documentation (flag=3 = permanent hindcast failure)
- `apps/machine_learning/recalculate_nan_forecasts.py:271-284` — flag=1/2
  detection in the sibling script (correctly exits the retry cycle)

---

## Dependency Graph

```json
{
  "phases": {
    "phase_1": {
      "title": "Remove null-Q50 filter from fill_ml_gaps.py",
      "file": "apps/machine_learning/fill_ml_gaps.py",
      "changes": [
        "Delete lines 248-257 (null-discharge exclusion block)",
        "Update or remove TestNullDischargeFilter tests in test_fill_ml_gaps.py"
      ],
      "depends_on": [],
      "parallel_with": ["phase_1_5"]
    },
    "phase_1_5": {
      "title": "Harden API error logging in hindcast and gap-fill scripts",
      "files": [
        "apps/machine_learning/hindcast_ML_models.py",
        "apps/machine_learning/fill_ml_gaps.py"
      ],
      "changes": [
        "Escalate hindcast API write failure from WARNING to ERROR (2 places in hindcast_ML_models.py)",
        "Escalate fill_ml_gaps API read fallback from WARNING to ERROR (1 place in fill_ml_gaps.py)"
      ],
      "depends_on": [],
      "parallel_with": ["phase_1"]
    },
    "phase_2": {
      "title": "Add 5 unit tests verifying filter removal fixes the loop",
      "file": "apps/machine_learning/test/test_fill_ml_gaps_null_loop.py",
      "depends_on": ["phase_1", "phase_1_5"],
      "parallel_with": []
    },
    "phase_3": {
      "title": "Run test suite and verify",
      "commands": [
        "cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning"
      ],
      "depends_on": ["phase_2"],
      "parallel_with": []
    }
  },
  "execution_groups": [
    {
      "group": 1,
      "parallel": true,
      "agents": [
        {
          "id": "agent_fix",
          "phases": ["phase_1"],
          "reason": "Delete null-Q50 filter + update existing tests"
        },
        {
          "id": "agent_error_logging",
          "phases": ["phase_1_5"],
          "reason": "Log-level changes in two files, independent of Phase 1"
        }
      ]
    },
    {
      "group": 2,
      "parallel": false,
      "agents": [
        {
          "id": "agent_tests",
          "phases": ["phase_2"],
          "reason": "New tests verifying the loop is broken"
        }
      ]
    },
    {
      "group": 3,
      "parallel": false,
      "agents": [
        {
          "id": "agent_validation",
          "phases": ["phase_3"],
          "reason": "Final validation after all changes"
        }
      ]
    }
  ]
}
```
