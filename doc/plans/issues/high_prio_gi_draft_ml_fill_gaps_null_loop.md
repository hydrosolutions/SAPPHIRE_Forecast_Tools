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

### Phase 1: Extract `_detect_genuine_gaps` helper with flag=3 suppression

**File**: `apps/machine_learning/fill_ml_gaps.py`
**Lines**: 248-296 (null exclusion block + gap detection loop)

**Approach (Option C — recommended)**

Keep the current null-discharge exclusion so that flag=3 rows do not count as
valid forecasts for gap detection purposes. After gaps are identified, check
whether every date in the gap interval is already covered by a flag=3 row in
the unfiltered DataFrame. If a gap is entirely covered by flag=3 rows, skip
it and log a warning instead of triggering a hindcast.

Extract the gap-detection + flag=3-suppression logic into a testable helper
`_detect_genuine_gaps()` so that Phase 2 can test it directly without calling
`fill_ml_gaps()`.

This approach:
- Preserves the ability to detect genuinely missing dates (dates with NO row)
- Skips hindcasts for dates that have already been attempted and failed
- Remains idempotent: running `fill_ml_gaps` twice on the same data produces
  the same outcome
- Logs clearly so operators know which gaps were skipped and why

**Step 1.1 — Retain the full DataFrame for flag=3 lookup + coerce flag type**

Rename the filtered DataFrame and keep a reference to the original. Coerce
the `flag` column to numeric to handle CSV fallback (where flag may be read
as string or float):

```python
# BEFORE (lines 248-257):
if "Q50" in forecast.columns:
    n_nulls = forecast["Q50"].isna().sum()
    if n_nulls > 0:
        logger.info(
            "fill_ml_gaps: Excluding %d null-discharge rows from gap detection",
            n_nulls,
        )
        forecast = forecast[forecast["Q50"].notna()].copy()

# AFTER:
forecast_all = forecast.copy()   # ← keep full dataset including flag=3 rows
# Coerce flag to numeric — CSV fallback may produce string or float values
if "flag" in forecast_all.columns:
    forecast_all["flag"] = pd.to_numeric(forecast_all["flag"], errors="coerce")
if "Q50" in forecast.columns:
    n_nulls = forecast["Q50"].isna().sum()
    if n_nulls > 0:
        logger.info(
            "fill_ml_gaps: Excluding %d null-discharge rows from "
            "gap detection; flag=3 rows will suppress spurious hindcasts",
            n_nulls,
        )
        forecast = forecast[forecast["Q50"].notna()].copy()
```

**Step 1.2 — Build a per-code set of flag=3 dates**

After the null exclusion block, build a lookup of all flag=3 forecast dates
per code:

```python
# Dates that already have a flag=3 row — hindcast was attempted and failed
flag3_dates: dict[str, set] = {}
if "flag" in forecast_all.columns:
    flag3_rows = forecast_all[forecast_all["flag"] == 3]
    for code, grp in flag3_rows.groupby("code"):
        flag3_dates[str(code)] = set(grp["forecast_date"].dt.normalize())
```

**Step 1.3 — Extract `_detect_genuine_gaps()` helper**

Add a new module-level function that encapsulates gap detection + flag=3
suppression. This is what `fill_ml_gaps()` will call and what Phase 2 tests
directly:

```python
def _detect_genuine_gaps(
    forecast_valid: pd.DataFrame,
    flag3_dates: dict[str, set],
    limit_day_gap: int = 1,
) -> tuple[dict, pd.Timestamp | None, pd.Timestamp | None]:
    """Detect date gaps in forecast data, suppressing flag=3-covered gaps.

    Args:
        forecast_valid: Forecast DataFrame with null-Q50 rows already excluded.
        flag3_dates: {code_str: set of normalized flag=3 forecast_dates}.
        limit_day_gap: Minimum day gap to consider (default 1).

    Returns:
        (missing_forecasts_dict, min_missing_date, max_missing_date).
        Gaps entirely covered by flag=3 rows are suppressed and logged.
    """
    missing_forecasts_dict = {}
    min_missing_date = None
    max_missing_date = None

    for code in forecast_valid.code.unique():
        forecast_code = forecast_valid[forecast_valid.code == code].copy()
        forecast_dates = pd.DatetimeIndex(
            forecast_code["forecast_date"].unique()
        ).sort_values()
        missing_forecasts = []

        for i in range(1, len(forecast_dates)):
            if (forecast_dates[i] - forecast_dates[i - 1]).days > limit_day_gap:
                missing_forecasts.append(
                    (forecast_dates[i - 1], forecast_dates[i])
                )

        # --- Flag=3 suppression: skip gaps already attempted ---
        if len(missing_forecasts) > 0:
            code_flag3 = flag3_dates.get(str(code), set())
            genuine_gaps = []
            for gap_start, gap_end in missing_forecasts:
                # Interior dates (exclusive of endpoints which are last/next
                # valid dates)
                gap_range = pd.date_range(
                    gap_start + datetime.timedelta(days=1),
                    gap_end - datetime.timedelta(days=1),
                    freq="D",
                )
                gap_dates_norm = set(gap_range.normalize())
                if len(gap_dates_norm) > 0 and gap_dates_norm.issubset(code_flag3):
                    logger.warning(
                        "fill_ml_gaps: Skipping gap %s–%s for code=%s — all "
                        "%d interior dates already have flag=3 rows (hindcast "
                        "previously attempted and failed).",
                        gap_start.date(),
                        gap_end.date(),
                        code,
                        len(gap_dates_norm),
                    )
                else:
                    genuine_gaps.append((gap_start, gap_end))
            missing_forecasts = genuine_gaps

        if len(missing_forecasts) > 0:
            missing_forecasts_dict[code] = missing_forecasts
            min_current = missing_forecasts[0][0]
            max_current = missing_forecasts[-1][1]

            if min_missing_date is None:
                min_missing_date = min_current
                max_missing_date = max_current
            else:
                min_missing_date = min(min_missing_date, min_current)
                max_missing_date = max(max_missing_date, max_current)

    return missing_forecasts_dict, min_missing_date, max_missing_date
```

**Step 1.4 — Replace inline gap detection with helper call**

Replace the gap detection loop (lines 259-296) in `fill_ml_gaps()` with a
call to the helper:

```python
missing_forecasts_dict, min_missing_date, max_missing_date = (
    _detect_genuine_gaps(forecast, flag3_dates, limit_day_gap)
)

# Log per-code gap info (preserves existing log output)
for code, gaps in missing_forecasts_dict.items():
    min_date = gaps[0][0].strftime("%Y-%m-%d")
    max_date = gaps[-1][1].strftime("%Y-%m-%d")
    logger.info("Missing forecasts for code %s from %s to %s", code, min_date, max_date)
    print("Missing forecasts for code", code, "from", min_date, "to", max_date)
```

**Complete diff summary for `fill_ml_gaps.py`**:

| Old line(s) | Change |
|-------------|--------|
| 248-257 | Add `forecast_all = forecast.copy()` + flag coercion before the `if "Q50"` block; update log message |
| After 257 | Insert `flag3_dates` lookup dict (Step 1.2) |
| Between `call_hindcast_script()` (line ~127) and `fill_ml_gaps()` (line ~129) | Add `_detect_genuine_gaps()` helper function (Step 1.3) |
| 259-296 | Replace inline gap detection loop with `_detect_genuine_gaps()` call (Step 1.4) |

The hindcast trigger, the merge, and the write paths (lines 304-409) are
unchanged.

**Note on partial flag=3 coverage**: If a gap has some dates with flag=3 and
some with no row at all, the gap is NOT suppressed — the hindcast runs for
the entire range. This means some flag=3 dates will be re-attempted, producing
duplicate flag=3 records. This is acceptable because the API upsert pattern
handles duplicates, and the alternative (splitting gaps) adds complexity
without operational benefit.

---

### Phase 1.5: Harden API error logging in hindcast and gap-fill scripts

The flag=3 suppression fix depends on API data consistency: `hindcast_ML_models.py`
must write flag=3 rows to the API, and `fill_ml_gaps.py` must read them back.
If either side silently falls back to CSV, the suppression logic has no flag=3
data to work with — and operators won't notice because the current log level
is only `WARNING`.

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
    "— flag=3 rows may not be visible to fill_ml_gaps",
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
    "— API write failed or unavailable; fill_ml_gaps flag=3 "
    "suppression will not work until data is in the API",
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
    "Flag=3 suppression may be unreliable without API data.",
    MODEL_TO_USE,
    prefix,
)
```

**Rationale**: These are not cosmetic changes. The ML-008 flag=3 suppression
logic requires that:
1. `hindcast_ML_models.py` successfully writes flag=3 rows to the API
2. `fill_ml_gaps.py` successfully reads those rows back from the API

If either fails, the suppression cannot work and the infinite loop may recur.
ERROR-level logging ensures operators and monitoring systems detect this
immediately.

---

### Phase 2: Tests

**File**: `apps/machine_learning/test/test_fill_ml_gaps_null_loop.py` (new)

**Import preamble**: The new test file requires the same `sys.modules` mock
setup as `test_fill_ml_gaps.py` (lines 23-39) to handle heavy dependencies
(darts, pytorch_lightning, torch, setup_library) that are imported at module
level by `fill_ml_gaps.py`.

Test the `_detect_genuine_gaps()` helper extracted in Phase 1 directly —
no subprocess calls, no file I/O, no API mocks needed. Build minimal
DataFrames in each test. Follow Arrange → Act → Assert.

The helper is already extracted (Phase 1, Step 1.3), so tests import and
call `_detect_genuine_gaps()` with synthetic DataFrames and `flag3_dates`
dicts.

| # | Scenario | Asserts |
|---|----------|---------|
| 1 | No null rows, no gaps → empty dict | `missing_forecasts_dict == {}`, `min_date is None` |
| 2 | Gap covered entirely by flag=3 → suppressed | `missing_forecasts_dict == {}`, warning logged |
| 3 | Gap with zero flag=3 coverage → genuine | `missing_forecasts_dict` contains the gap |
| 4 | Gap partially covered by flag=3 → still genuine | `missing_forecasts_dict` contains the gap |
| 5 | Two codes: one gap suppressed, one genuine | Only genuine code in `missing_forecasts_dict` |
| 6 | Flag=3 rows in different code → not suppressed for other code | Gap retained for the other code |
| 7 | All gaps across all codes suppressed → no hindcast trigger | `min_date is None`, `max_date is None` |
| 8 | `flag` column absent → caller passes `flag3_dates == {}`, all gaps genuine | All gaps in dict |
| 9 | API read fallback to CSV → ERROR logged | `logger.error` emitted with "falling back to CSV" message |

Mocking strategy: pure `pd.DataFrame` construction; capture `logging.WARNING`
calls with `caplog` (pytest) to verify the skip message is emitted.

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

Observe that `fill_ml_gaps.py` logs "Skipping gap … all N interior dates
already have flag=3 rows" for the three known spurious gaps (35-day,
32-day, 169-day) and does NOT launch `hindcast_ML_models.py`.

**Step 3.3 — Idempotency check**

Run `fill_ml_gaps.py` twice in succession. The second run must produce
identical output — no new hindcast, no new rows written.

---

## Acceptance Criteria

- [ ] `fill_ml_gaps.py` does not trigger a hindcast for date ranges where
      every interior date already has a flag=3 row in the database
- [ ] A clear `WARNING`-level log message is emitted for each suppressed gap,
      identifying the station code and date range
- [ ] Genuinely missing dates (no row at all, not even flag=3) still trigger
      a hindcast as before
- [ ] Running `fill_ml_gaps.py` twice in succession on the same data produces
      identical results (idempotent)
- [ ] All 8 new unit tests pass
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
| `flag` column absent (CSV fallback path, old schema) | Low | Guard: if `flag` column not present, `flag3_dates` defaults to empty dict and all gaps are treated as genuine — safe degradation |
| `flag` column has wrong dtype (string/float from CSV) | Medium | `pd.to_numeric(errors="coerce")` coercion added in Step 1.1 — matches `recalculate_nan_forecasts.py` line 253 which also coerces flag |
| A flag=3 date later becomes valid (input data arrives retroactively) | Very low | Partially-covered gap check (test case #4) means any date without a flag=3 row still triggers a hindcast for that interval |
| Partial flag=3 coverage triggers redundant hindcast for already-attempted dates | Low | API upsert pattern handles duplicate flag=3 records; splitting gaps adds complexity without operational benefit |
| `forecast_all` memory overhead for 53 stations × 730 days | Negligible | ~8k rows total, well within pandas memory budget |
| `pd.date_range` produces unexpected results at DST boundaries | Low | `normalize()` strips time components; date arithmetic is timezone-naive throughout |
| API write fails in hindcast → flag=3 rows only in CSV → suppression ineffective | Medium | Phase 1.5 escalates to ERROR so operators detect immediately; CSV fallback preserves subprocess communication |
| API read fails in fill_ml_gaps → CSV fallback may lack flag column | Medium | Phase 1.5 escalates to ERROR; flag3_dates defaults to empty dict (safe degradation — all gaps treated as genuine) |

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
- **ML-011**: Flag=2 semantic collision in `make_forecast.py` — uses flag=2
  for "empty predictions (model error)" but convention reserves flag=2 for
  "NaN from hindcast"; causes spurious recalculation triggers
- **ML-012**: `recalculate_nan_forecasts.py` line 334 crashes on NaN flags —
  `hindcast["flag"].astype(int)` without `errors="ignore"` raises ValueError
  if any hindcast row has missing flag
- **ML-013**: `recalculate_nan_forecasts.py` API write sends full hindcast
  DataFrame (line 405), not just replaced rows — can overwrite valid flag=0
  operational data in the API
- **ML-014**: `flag=None` dtype cascade — nullable flag column in DB forces
  float64 on readback, breaking downstream `astype(int)` calls

---

## References

- `apps/machine_learning/fill_ml_gaps.py:248-257` — null-discharge exclusion
  block (the root of the problem)
- `apps/machine_learning/fill_ml_gaps.py:259-296` — gap detection loop
- `apps/machine_learning/fill_ml_gaps.py:304-331` — hindcast trigger
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
      "title": "Extract _detect_genuine_gaps helper with flag=3 suppression",
      "file": "apps/machine_learning/fill_ml_gaps.py",
      "changes": [
        "Add forecast_all = forecast.copy() before null exclusion (line 248)",
        "Add flag type coercion with pd.to_numeric(errors='coerce')",
        "Build flag3_dates dict after null exclusion",
        "Add _detect_genuine_gaps() helper function (between call_hindcast_script and fill_ml_gaps)",
        "Replace inline gap detection loop (lines 259-296) with helper call"
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
        "Escalate fill_ml_gaps API read fallback from WARNING to ERROR (1 place in fill_ml_gaps.py)",
        "Update error messages to reference flag=3 suppression dependency"
      ],
      "depends_on": [],
      "parallel_with": ["phase_1"]
    },
    "phase_2": {
      "title": "Add 9 unit tests for _detect_genuine_gaps helper and error logging",
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
          "reason": "Single file edit — extract helper with flag=3 logic"
        },
        {
          "id": "agent_error_logging",
          "phases": ["phase_1_5"],
          "reason": "Log-level changes in two files, independent of Phase 1 logic"
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
          "reason": "Tests written against the extracted helper and error logging"
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
