# Fix `horizon_type` keyword in daily forecast API reader

**Status**: In Progress — fix committed (e87fdda), pending PR and server test
**Module**: postprocessing_forecasts
**Priority**: High
**Labels**: `bug`, `api-integration`, `skill-metrics`

---

## Summary

`read_daily_forecasts()` passes `horizon_type="day"` to
`SapphirePostprocessingClient.read_forecasts()`, but the client method
expects `horizon="day"`. The call silently fails, caught by a bare
`except`, so daily skill metrics are never computed during
`recalculate_skill_metrics`.

## Context

The skill recalculation pipeline (`recalculate_skill_metrics.py`)
computes skill metrics for all horizon types when
`SAPPHIRE_PREDICTION_MODE=ALL`. For the `day` horizon, it calls
`data_reader.read_daily_forecasts()` to fetch ML model forecasts
(TFT, TiDE, TSMixer) stored with `horizon=day`.

## Problem

In `data_reader.py:758-765`, the API call uses the wrong keyword
argument:

```python
df_batch = client.read_forecasts(
    horizon_type="day",  # WRONG — client expects `horizon`
    code=code,
    ...
)
```

`SapphirePostprocessingClient.read_forecasts()` accepts `horizon` (not
`horizon_type`). Python raises `TypeError` for the unexpected keyword,
which is caught at line 780 and logged as:

```
Failed to read daily forecasts from API:
  SapphirePostprocessingClient.read_forecasts() got an unexpected
  keyword argument 'horizon_type'
```

The function returns an empty DataFrame, so daily skill metrics are
silently skipped.

## Impact

- Daily skill metrics are **never written** during recalculation.
- The recalculation still reports PASS because the error is caught.
- Dashboard users see no daily skill metrics.

## Desired Outcome

- `read_daily_forecasts()` calls the API with the correct `horizon`
  parameter.
- Daily skill metrics are computed and written during recalculation.

---

## Technical Analysis

### Current Implementation

**Caller** — `recalculate_skill_metrics.py:370`:
```python
daily_fc = data_reader.read_daily_forecasts(codes, start_year, end_year)
```

**Bug location** — `data_reader.py:758-765`:
```python
df_batch = client.read_forecasts(
    horizon_type="day",   # ← wrong keyword
    code=code,
    start_date=start_date,
    end_date=end_date,
    skip=skip,
    limit=batch_size,
)
```

**Client method signature** (sapphire_api_client `postprocessing.py:50-68`):
```python
def read_forecasts(
    self,
    horizon: Optional[str] = None,  # ← correct keyword
    code: Optional[str] = None,
    model: Optional[str] = None,
    start_date: Optional[Union[str, date]] = None,
    end_date: Optional[Union[str, date]] = None,
    ...
) -> pd.DataFrame:
```

Note: `read_forecasts()` is itself a deprecated wrapper around
`read_short_term_forecasts()`. A follow-up refactor could switch to
the non-deprecated method, but fixing the keyword is the immediate
priority.

### Root Cause

Naming mismatch: the postprocessing API uses `horizon_type` in its
database schema and URL parameters, but the Python client library uses
`horizon` as the keyword argument name.

---

## Implementation Plan

### Files to Modify

| File | Changes |
|------|---------|
| `apps/postprocessing_forecasts/src/data_reader.py:759` | Change `horizon_type="day"` → `horizon="day"` |
| `apps/postprocessing_forecasts/tests/test_recalc_workflow.py` | Verify existing test covers this path; add assertion if not |

### Implementation Steps

- [ ] Step 1: In `data_reader.py:759`, change `horizon_type="day"` to `horizon="day"`
- [ ] Step 2: Run `SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` — all tests pass
- [ ] Step 3: Run a local `recalculate_skill_metrics` with `SAPPHIRE_PREDICTION_MODE=ALL` and verify the log no longer shows the `horizon_type` error
- [ ] Step 4: Verify daily skill metrics are actually written to the API after recalculation

---

## Testing

### Test Cases

- [ ] Existing test `test_recalc_workflow.py:372` mocks `read_daily_forecasts` — verify it still passes
- [ ] Manual: run recalculation and confirm no `horizon_type` error in logs
- [ ] Manual: query `skill-metric/?horizon=day` and confirm records exist after recalculation

### Testing Commands

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
```

---

## Documentation Impact

- [ ] No documentation impact — one-line parameter name fix

## Out of Scope

- Migrating from deprecated `read_forecasts()` to `read_short_term_forecasts()` (separate refactor)
- Adding daily skill metrics to the forecast dashboard display

## Acceptance Criteria

- [ ] `data_reader.py` calls `client.read_forecasts(horizon="day", ...)`
- [ ] `recalculate_skill_metrics` with `ALL` mode produces no `horizon_type` error in logs
- [ ] Daily skill metric records are written to the postprocessing API
- [ ] All existing tests pass
