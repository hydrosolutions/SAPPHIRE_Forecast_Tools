# Fix Postprocessing Boundary Day Guard, LR Sentinel, and Validation Queries

**Status**: Review
**Module**: `infra` (cross-module: `iEasyHydroForecast`, `postprocessing_forecasts`, `validate_pipeline`)
**Priority**: High
**Labels**: `bug`, `data-integrity`, `api`

---

## Summary

Four related issues cause validation failures after daily pipeline runs:
(A) `perform_linear_regression` uses `-1.0` as a sentinel that propagates
as negative discharge, (B) postprocessing writes decadal combined forecasts
on non-decad days (creating records with wrong issue dates), (C) the
ensemble calculator does not include the forecast period in its grouping
key, and (D) the validation script only queries `date=today` and misses
models issued on the correct boundary date.

## Context

### Forecast issue date semantics

- **`date`** = forecast issue date (the day the forecast was produced).
  For short-term forecasts, this is the boundary date (last day of the
  observation period).
- **`target`** = first day of the following pentad or decade (the date
  the forecast is produced *for*).

### Boundary day rules

Forecasts are only aggregated to pentad/decad level on boundary days:
- **Pentad boundaries**: 5, 10, 15, 20, 25, last day of month
- **Decad boundaries**: 10, 20, last day of month

On Feb 20 (both pentad AND decad boundary): aggregate both horizons.
Both pentadal and decadal forecasts have `target = 2026-02-21`.
On Feb 25 (pentad boundary, NOT decad): aggregate pentad only.
On Feb 21 (neither): no aggregation, no ensembles.

## Problem

### Issue A: Sentinel -1.0 in LR forecasts

`forecast_library.py:perform_linear_regression` initializes
`forecasted_discharge=-1.0` (line 1403). For stations with insufficient
data (15213, 15217), the loop `continue`s before the real forecast is
computed, leaving `-1.0` intact. This propagates to both API tables and
triggers the validation's "Discharge non-negative" FAIL (4 records:
2 stations x 2 tables).

### Issue B: Postprocessing runs decad on non-decad days

The daily pipeline (`run_locally.sh:867-874`) unconditionally runs both
PENTAD and DECAD modes:

```bash
for mode in PENTAD DECAD; do
    export SAPPHIRE_PREDICTION_MODE="$mode"
    run_machine_learning || ...
    run_linear_regression || ...
    run_postprocessing_forecasts || ...
done
```

On Feb 25 (NOT a decad boundary), postprocessing in DECAD mode:
1. Reads ML day-level forecasts (`date=2026-02-25`)
2. Aggregates them to decad level (`decad_in_year=6`)
3. Writes combined forecasts with `horizon_type=decade, date=2026-02-25`

This is wrong: decadal combined forecasts should only have issue dates
that are actual decad boundaries. LR correctly skips on non-decad days
(it only produces forecasts on boundary dates), so LR's latest decad
data has `date=2026-02-20`. The result is:

| Model | `date` in combined table | Correct? |
|-------|-------------------------|----------|
| LR | 2026-02-20 (boundary) | Yes |
| TFT, TiDE, TSMixer | 2026-02-25 (non-boundary) | **No** |
| NE | 2026-02-25 (non-boundary) | **No** |
| EM | Not created (date mismatch) | Bug |

The EM ensemble calculator groups by `['date', 'code']`. Since LR has
`date=Feb 20` and ML has `date=Feb 25`, they land in separate groups.
LR's single-model group gets discarded, so EM is never created.

**Root cause**: Postprocessing has no boundary day guard — it processes
whatever horizon `SAPPHIRE_PREDICTION_MODE` tells it to.

### Issue C: Ensemble calculator missing period in groupby

The ensemble calculator (`ensemble_calculator.py:152`) groups by
`['date', 'code']`:

```python
ensemble_avg = qualifying.groupby(['date', 'code']).agg({
    period_col: 'first',
    'forecasted_discharge': 'mean',
    'model_short': composition_agg,
}).reset_index()
```

The `period_col` (`pentad_in_year` or `decad_in_year`) is taken as
`'first'` instead of being part of the group key. This means:
- Forecasts from different periods but the same date (unlikely but
  theoretically possible) would be incorrectly averaged together.
- The groupby intent is unclear — it implicitly depends on the
  assumption that each `(date, code)` pair maps to exactly one period.

The period column should be an explicit part of the groupby key.

### Issue D: Validation queries only `date=today`

`validate_pipeline.py` queries `start_date=today, end_date=today`.
On non-decad days, it finds ML's spurious records (`date=today`) but
misses LR's correct records (`date=boundary`). The SKIP logic masks
this as "not a forecast day" instead of flagging the incorrect state.
The `check_expected_models` function reports LR and EM as "missing"
even though LR data exists at the boundary date.

## Desired Outcome

1. Stations with insufficient data produce `NaN` forecasts (not `-1.0`)
2. Postprocessing only writes pentad/decad combined forecasts on the
   corresponding boundary days — no spurious records with non-boundary dates
3. Ensemble calculator explicitly groups by forecast period
4. Validation queries the most recent boundary date to verify all data exists
5. All existing tests pass, new tests cover the changes

---

## Technical Analysis

### Issue A: Sentinel -1.0

**File**: `apps/iEasyHydroForecast/forecast_library.py:1399-1408`

```python
data_dfp = data_dfp.assign(
    slope=1.0,
    intercept=0.0,
    forecasted_discharge=-1.0,  # negative sentinel
    q_mean=0.0,
    q_std_sigma=0.0,
    delta=0.0,
    rsquared=0.0
)
```

Early `continue` paths at lines 1432-1434 and 1446-1448 leave defaults
untouched. Downstream NaN handling is already safe (`_opt_col` pattern,
`.where(notna())`).

### Issue B: Missing boundary day guard

**File**: `apps/postprocessing_forecasts/postprocessing_operational.py:73-201`

The entry point checks `SAPPHIRE_PREDICTION_MODE` to decide which horizons
to process (lines 85, 144), but does **not** check whether today is a
boundary day for that horizon.

**Caller**: `apps/run_locally.sh:867-874` — `run_daily_pipeline` loops
through both PENTAD and DECAD unconditionally.

### Issue C: Ensemble calculator groupby

**File**: `apps/postprocessing_forecasts/src/ensemble_calculator.py:151-156`

The groupby uses `['date', 'code']` with `period_col: 'first'` in the agg.
The period column should be in the groupby key to make the grouping
semantics explicit and robust.

### Issue D: Validation date query

**File**: `apps/validate_pipeline/validate_pipeline.py:253-262`

All forecast queries use `start_date=fd, end_date=fd` where `fd = today`.

**File**: `apps/validate_pipeline/validate_pipeline.py:432-458`

`check_expected_models` treats SKIP'd results (data=None) as missing models.

---

## Implementation Plan

### Phase 1: Fix sentinel -1.0

**File to modify**: `apps/iEasyHydroForecast/forecast_library.py`

- [x] **Step 1.1**: Change all defaults to `np.nan` at line 1399-1408:

```python
data_dfp = data_dfp.assign(
    slope=np.nan,
    intercept=np.nan,
    forecasted_discharge=np.nan,
    q_mean=np.nan,
    q_std_sigma=np.nan,
    delta=np.nan,
    rsquared=np.nan
)
```

Rationale: All seven values are computed together. When regression is
skipped, none of them are valid. `slope=1.0` and `intercept=0.0` are as
misleading as `forecasted_discharge=-1.0`.

- [x] **Step 1.2**: Search codebase for `== -1.0` or `== -1` checks on
forecasted_discharge that might need updating.

### Phase 2: Add boundary day guard to postprocessing

The guard should be in `postprocessing_operational.py` (the module itself
knows its domain rules) rather than `run_locally.sh` (which should remain
horizon-agnostic).

**File to modify**: `apps/postprocessing_forecasts/postprocessing_operational.py`

- [x] **Step 2.1**: Import `calendar` and add boundary day check functions
(or import from `tag_library` if equivalent functions exist):

```python
import calendar

def is_pentad_boundary(d: dt.date) -> bool:
    """Return True if d is a pentad boundary (5/10/15/20/25/last)."""
    last_day = calendar.monthrange(d.year, d.month)[1]
    return d.day in (5, 10, 15, 20, 25, last_day)

def is_decad_boundary(d: dt.date) -> bool:
    """Return True if d is a decad boundary (10/20/last)."""
    last_day = calendar.monthrange(d.year, d.month)[1]
    return d.day in (10, 20, last_day)
```

- [x] **Step 2.2**: Add boundary day guard before pentad processing
(around line 85):

```python
today = dt.date.today()

if prediction_mode in ['PENTAD', 'BOTH', 'ALL']:
    if not is_pentad_boundary(today):
        logger.info(
            "Skipping pentad postprocessing: %s is not a pentad "
            "boundary day", today,
        )
    else:
        # ... existing pentad processing block (lines 86-142) ...
```

- [x] **Step 2.3**: Add boundary day guard before decad processing
(around line 144):

```python
if prediction_mode in ['DECAD', 'BOTH', 'ALL']:
    if not is_decad_boundary(today):
        logger.info(
            "Skipping decad postprocessing: %s is not a decad "
            "boundary day", today,
        )
    else:
        # ... existing decad processing block (lines 145-201) ...
```

- [x] **Step 2.4**: Ensure the exit code remains 0 when skipping (this is
normal operation, not an error).

- [x] **Step 2.5**: Check whether `postprocessing_maintenance.py` also
needs a guard. Maintenance is for gap-filling, so it should process
pending periods regardless of today's date. Verify it doesn't create the
same spurious records.

### Phase 3: Add period column to ensemble groupby

**File to modify**: `apps/postprocessing_forecasts/src/ensemble_calculator.py`

- [x] **Step 3.1**: Change the EM groupby from `['date', 'code']` to
`[period_col, 'date', 'code']` (line 152):

```python
# Before:
ensemble_avg = qualifying.groupby(['date', 'code']).agg({
    period_col: 'first',
    'forecasted_discharge': 'mean',
    'model_short': composition_agg,
}).reset_index()

# After:
ensemble_avg = qualifying.groupby(
    [period_col, 'date', 'code']
).agg({
    'forecasted_discharge': 'mean',
    'model_short': composition_agg,
}).reset_index()
```

Note: `period_col` moves from the agg dict to the groupby key. After
`reset_index()`, it becomes a regular column (same as before). The
`'first'` aggregation is no longer needed since the period is constant
within each group.

- [x] **Step 3.2**: Verify the downstream observed merge (line 173) and
outer join (line 201-210) still work. The merge uses `on=['code', 'date']`
— adding `period_col` to the groupby doesn't change the `date` or `code`
columns, so these joins are unaffected.

- [x] **Step 3.3**: Verify `period_in_month_col` computation (line 192)
still works. It uses `forecast_target_date(ensemble_merged['date'])` which
doesn't depend on the groupby keys.

### Phase 4: Fix validation to query boundary dates

**File to modify**: `apps/validate_pipeline/validate_pipeline.py`

- [x] **Step 4.1**: Add `from datetime import timedelta` to imports.

- [x] **Step 4.2**: Add boundary date helper functions (after line 98):

```python
def most_recent_pentad_boundary(d: date) -> date:
    """Return the most recent pentad boundary date <= d."""
    last_day = calendar.monthrange(d.year, d.month)[1]
    boundaries = [5, 10, 15, 20, 25, last_day]
    for b in reversed(boundaries):
        if b <= d.day:
            return date(d.year, d.month, b)
    # Current day is before day 5: wrap to previous month
    prev_month_last = d.replace(day=1) - timedelta(days=1)
    return prev_month_last


def most_recent_decad_boundary(d: date) -> date:
    """Return the most recent decad boundary date <= d."""
    last_day = calendar.monthrange(d.year, d.month)[1]
    boundaries = [10, 20, last_day]
    for b in reversed(boundaries):
        if b <= d.day:
            return date(d.year, d.month, b)
    # Current day is before day 10: wrap to previous month
    prev_month_last = d.replace(day=1) - timedelta(days=1)
    return prev_month_last
```

- [x] **Step 4.3**: Modify `run_tier1_short_term` to query from boundary
date to today for forecast checks (not preprocessing — those use daily
data and keep `date=today`):

```python
# Compute boundary date for forecast queries
if horizon == "pentad":
    boundary = most_recent_pentad_boundary(forecast_date)
elif horizon == "decade":
    boundary = most_recent_decad_boundary(forecast_date)
else:
    boundary = forecast_date
bd = str(boundary)

# Postprocessing forecast checks — boundary to today
for model in SHORT_TERM_MODELS:
    results.append(
        check_presence(
            post_client, "read_short_term_forecasts",
            f"Forecasts ({model}, {horizon})",
            module=model_modules.get(model, ""),
            horizon=horizon, model=model,
            start_date=bd, end_date=fd,
        )
    )

# LR details — boundary to today
results.append(
    check_presence(
        post_client, "read_lr_forecasts",
        f"LR details ({horizon})",
        module="linear_regression",
        horizon=horizon, start_date=bd, end_date=fd,
    )
)
```

- [x] **Step 4.4**: Fix `check_expected_models` to exclude SKIP'd models
from the expected set:

```python
def check_expected_models(results, horizon):
    found_models = set()
    skipped_models = set()
    for r in results:
        if r.data is None or r.data.empty:
            if (r.status == "SKIP"
                    and r.name.startswith("Forecasts (")):
                model = r.name.split("(")[1].split(",")[0]
                skipped_models.add(model)
            continue
        if "model_type" in r.data.columns:
            found_models.update(r.data["model_type"].unique())
        elif "model_short" in r.data.columns:
            found_models.update(r.data["model_short"].unique())

    expected = set(SHORT_TERM_MODELS) - skipped_models
    missing = expected - found_models
    ...
```

---

## Testing

### Phase 1 Tests (in `iEasyHydroForecast/tests/`)

- [x] Test that valid stations still get correct forecast values (regression)
      — covered by existing `test_perform_linear_regression_with_simple_data`
      and `test_perform_linear_regression_with_complex_data` (166 tests pass)
- [x] Search codebase for `== -1.0` sentinel checks — none found
- [x] Test `perform_linear_regression` returns NaN (not -1.0) for stations
      with empty data after filtering — `TestNaNSentinelForInsufficientData`
- [x] Test `perform_linear_regression` returns NaN for stations where
      `dropna()` yields empty — 4 tests in `TestNaNSentinelForInsufficientData`

### Phase 2 Tests (in `postprocessing_forecasts/tests/`)

- [x] Test that operational entry point runs pentad on pentad boundary day
      — existing tests with `module.is_pentad_boundary = lambda d: True`
- [x] Test that operational entry point runs decad on decad boundary day
      — existing tests with `module.is_decad_boundary = lambda d: True`
- [x] Test that operational entry point skips pentad processing on non-pentad
      day — `TestBoundaryDaySkipBehavior` (5 tests in test_operational_workflow.py)
- [x] Test that operational entry point skips decad on non-decad day
      — `test_decad_skips_on_non_decad_day`
- [x] Test combined boundaries: pentad only, decad only, neither
      — `test_both_mode_pentad_only_on_pentad_boundary`,
      `test_both_mode_decad_only_on_decad_boundary`,
      `test_both_mode_skips_both_on_non_boundary_day`

### Phase 3 Tests (in `postprocessing_forecasts/tests/`)

- [x] Test EM groupby includes period_col (existing tests pass — 871 tests)
- [x] Regression test: normal single-period case works as before
- [x] Test that forecasts with same (date, code) but different period_col
      values are NOT averaged together — `TestPeriodAwareEnsemble` (2 tests)

### Phase 4 Tests (in `validate_pipeline/test/`)

- [x] Test `most_recent_pentad_boundary`:
      13 parametrized cases + 4 edge case tests in `TestMostRecentPentadBoundary`
- [x] Test `most_recent_decad_boundary`:
      10 parametrized cases + 2 edge case tests in `TestMostRecentDecadBoundary`
- [x] Test validation uses boundary-to-today range for forecast queries
      — existing `test_tier1_short_term_returns_expected_check_count` verifies
      the query parameters via mock assertions
- [x] Test `check_expected_models` excludes SKIP'd models
      — 3 tests in `TestCheckExpectedModelsSkipExclusion`

### Testing Commands

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh
```

### Manual Verification

After all phases, run on a non-decad boundary day:

```bash
bash apps/run_locally.sh daily
```

Expected:
- "Discharge non-negative" → PASS
- Postprocessing logs "Skipping decad postprocessing" on non-decad days
- No spurious decadal records with non-boundary dates
- Validation uses boundary date, finds all models
- 0 FAILs

---

## Out of Scope

- Changing `date` semantics in ML or LR writers (both are correct)
- Cleaning up existing spurious records (will be overwritten on next
  boundary day by upsert)
- Removing CSV fallback paths (separate issue, API-003)
- Changing the API schema
- Boundary day guard in `postprocessing_maintenance.py` — evaluated in
  Step 2.5, NOT needed (gap detection implicitly scopes to boundary dates)
- Boundary date filtering in ML API reader (`_read_ml_forecasts_from_api`)
  — daily ML forecasts are stored as-is; aggregation is controlled by
  the boundary guard at the entry point level

## Additional Changes (beyond original plan)

- **`forecast_library.py:1447`**: Fixed `dropna()` to use
  `subset=[predictor_col, discharge_avg_col]` — the NaN sentinel change
  would have caused the blanket `dropna()` to drop ALL rows (including
  valid data) since the newly-assigned output columns are all NaN.
- **`skill_metrics.py:1652, 1819`**: Applied the same Phase 3 groupby fix
  to `calculate_skill_metrics_pentad()` and `calculate_skill_metrics_decade()`
  — these had the same `groupby(['date', 'code'])` pattern as
  `ensemble_calculator.py`.

## Dependencies

- No external dependencies
- Phases are independent and can be implemented in any order
- Phase 2 is the primary architectural fix that prevents the date mismatch
- Phase 3 is a defensive improvement that makes EM grouping explicit

## Acceptance Criteria

- [x] No forecast records with `forecasted_discharge < 0` in the API
- [x] Postprocessing skips non-boundary horizons with clear log message
- [x] Ensemble calculator groups by `[period_col, 'date', 'code']`
- [x] On boundary days, all models share the same `date`, EM is created
- [ ] Validation produces 0 FAILs on `run_locally.sh daily` (pending
      manual verification on next pipeline run)
- [x] All existing tests pass (`run_tests.sh`) — 11/11 modules, 0 failures
- [x] New tests cover sentinel fix, boundary guard, ensemble groupby,
      and validation helpers — all TODOs resolved

---

## References

- Existing plan: [`gi_draft_fix_ml_forecast_api_reader.md`](gi_draft_fix_ml_forecast_api_reader.md)
- Data flow: [`doc/data_flow_short_term.md`](../../data_flow_short_term.md)
- Validation log: `apps/logs/run_locally_20260225_151700.log`
