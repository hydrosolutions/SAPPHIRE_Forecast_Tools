# ML-006: NumPy shape mismatch in `recalculate_nan_forecasts.py` during `.loc` assignment

**Status**: Review
**Module**: `machine_learning`
**Priority**: High
**Labels**: `bug`, `maintenance-mode`, `data-integrity`

---

## Summary

`recalculate_nan_forecasts.py` crashes during the `update_forecast()` call
(lines 335-368) whenever the forecast DataFrame and the hindcast DataFrame
have a different number of target-date rows for the same `forecast_date`.
The `.loc[mask, value_cols] = hindcast.loc[hindcast_mask, value_cols].values`
assignment requires identical row counts on both sides. When N ≠ M the
NumPy broadcast raises a shape mismatch ValueError. The exception is caught
at line 366 and immediately re-raised at line 368, which kills the entire
`recalculate_nan_forecasts()` call before the API write at lines 374-399
ever executes.

First observed in the operational pipeline run on 2026-03-13.

---

## Root Cause Analysis

### The Shape Mismatch

`update_forecast(forecast_code, hindcast_code)` iterates over
`forecast_dates_flag1` and for each `forecast_date` does:

```python
mask = forecast_code["forecast_date"] == forecast_date
hindcast_mask = hindcast_code["forecast_date"] == forecast_date

forecast_code.loc[mask, value_cols] = (
    hindcast_code.loc[hindcast_mask, value_cols].values
)
```

The assignment calls `.values` on the right-hand side, producing a
2-D NumPy array of shape `(M, len(value_cols))`. NumPy then tries to
broadcast this into the `N` slots selected by `mask`. When N ≠ M the
assignment raises:

```
ValueError: could not broadcast input array from shape (M, K)
            into shape (N, K)
```

### Why N and M can diverge

The forecast table has one row per `(code, forecast_date, target_date)`.
The hindcast is produced independently for the same `forecast_date` but
may cover a different set of target dates:

- Pentad forecasts conventionally span 6 target dates (pentad + outlook);
  if the hindcast script was called with a date range that produces 5 or
  7 target dates, M ≠ N.
- The forecast DataFrame may already contain rows for some target dates
  (only a subset have `flag=1`), while the hindcast contains the full
  set for that period.
- Rounding or boundary-day differences in how the hindcast script
  computes its output window can shift the count by ±1.

### Why the bug is silent until the re-raise

The try/except at lines 364-368 catches the ValueError and logs it, but
then immediately re-raises, propagating the exception out of the loop.
No codes after the failing one are processed, and the API write block
(lines 374-399) is never reached. From the operator's perspective the
pipeline exits with an error and NaN forecasts remain unfilled.

---

## Implementation Plan

### Phase 1: Fix the shape mismatch in `update_forecast()`

**File**: `apps/machine_learning/recalculate_nan_forecasts.py`
**Lines**: 335-358

Replace the broadcast assignment with a merge-based approach that aligns
rows on `(forecast_date, date)` before copying values. This tolerates any
difference in the target-date sets: only rows present in both DataFrames
are updated; rows missing from the hindcast are left unchanged.

```python
def update_forecast(forecast_code, hindcast_code):
    value_cols = [
        col for col in forecast_code.columns if "Q" in col
    ]
    forecast_code = forecast_code.copy()
    hindcast_code = hindcast_code.copy()

    forecast_dates_flag1 = forecast_code[
        forecast_code["flag"].isin([1, 2])
    ]["forecast_date"].unique()

    for forecast_date in forecast_dates_flag1:
        fc_mask = forecast_code["forecast_date"] == forecast_date
        hc_mask = hindcast_code["forecast_date"] == forecast_date

        if not hc_mask.any():
            continue

        fc_rows = forecast_code.loc[fc_mask].copy()
        hc_rows = hindcast_code.loc[hc_mask][
            ["date"] + value_cols + ["flag"]
        ].copy()

        # Align on target date — inner join so only matching rows update
        merged = fc_rows[["date"]].merge(
            hc_rows, on="date", how="left", suffixes=("", "_hc")
        )
        merged = merged.set_index(fc_rows.index)

        for col in value_cols:
            hc_col = col + "_hc" if col + "_hc" in merged.columns else col
            valid = merged[hc_col].notna()
            forecast_code.loc[
                fc_mask & valid.reindex(forecast_code.index, fill_value=False),
                col,
            ] = merged.loc[valid, hc_col].values

        flag_col = "flag_hc" if "flag_hc" in merged.columns else "flag"
        valid_flag = merged[flag_col].notna()
        forecast_code.loc[
            fc_mask
            & valid_flag.reindex(forecast_code.index, fill_value=False),
            "flag",
        ] = merged.loc[valid_flag, flag_col].values

    return forecast_code
```

Key properties of the new implementation:

- No broadcast: each cell is assigned individually via aligned indices.
- Missing target dates in hindcast → `NaN` after left-merge → skipped by
  `notna()` guard → original forecast value is preserved.
- Extra target dates in hindcast (M > N) → merged rows with no
  corresponding forecast index → silently dropped; no out-of-bounds write.
- `flag` is updated only for rows that received a new value.

Also remove the bare `raise e` at line 368. After the merge-based fix the
loop should not crash; if it does, log and continue to the next code rather
than aborting the entire function:

```python
    for code in codes_with_nan:
        forecast_code = forecast[forecast["code"] == code].copy()
        hindcast_code = hindcast[hindcast["code"] == code].copy()
        try:
            updated = update_forecast(forecast_code, hindcast_code)
            forecast[forecast["code"] == code] = updated
        except Exception as e:
            logger.error(
                "recalculate_nan_forecasts: update_forecast failed for "
                "code=%s: %s — skipping code, NaN records preserved.",
                code, e,
            )
            # Do NOT re-raise; allow remaining codes to be processed
            # and the API write to execute
```

### Phase 2: Add tests

**File**: `apps/machine_learning/test/test_recalculate_nan_shape_mismatch.py`
(new file)

Build minimal DataFrames in each test — no subprocess calls, no file I/O.

| # | Scenario | Asserts |
|---|----------|---------|
| 1 | N == M: hindcast has same target dates as forecast | All `flag=1` rows updated; shape preserved |
| 2 | N > M: forecast has more target dates than hindcast | Only matching target dates updated; extra rows unchanged |
| 3 | N < M: hindcast has more target dates than forecast | Only matching target dates updated; no index error |
| 4 | `hindcast_mask.any()` is False (no hindcast for date) | Row unchanged; no crash |
| 5 | All codes have matching hindcast → whole loop completes | Return value equals input shape |
| 6 | One code raises inside `update_forecast` → continue loop | Remaining codes are still processed; error logged |
| 7 | value_cols contains `Q05`, `Q50`, `Q95` — all updated | All three columns updated correctly where target dates match |

Mocking strategy: use pure `pd.DataFrame` construction; no mocks needed
for Phase 2 tests. Follow the Arrange → Act → Assert structure from
`doc/dev/testing_workflow.md`.

### Phase 3: Run test suite

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning
```

All tests must pass with zero skips (except the valid
`SAPPHIRE_API_AVAILABLE` dependency-gate pattern).

---

## Acceptance Criteria

- [ ] `update_forecast()` does not raise `ValueError` when forecast and
      hindcast have different numbers of target-date rows for the same
      `forecast_date`
- [ ] Only target dates present in both forecast and hindcast are updated;
      all other rows are left unchanged
- [ ] The bare `raise e` at the call site is removed; a per-code error log
      replaces it, allowing the loop to continue to remaining codes
- [ ] The API write block at lines 374-399 is reached and executes even
      when one or more codes failed during `update_forecast()`
- [ ] All 7 new unit tests pass
- [ ] Full ML test suite passes with zero skips
- [ ] No changes to `sapphire/services/` (ownership boundary respected)

---

## Out of Scope

- The hindcast subprocess itself and why it may produce a different number
  of target dates (ML-002)
- Making `call_hindcast_script()` raise cleanly on subprocess failure
  (ML-001 — already guarded in the current code at lines 304-318)
- Migrating remaining CSV reads to API-primary (ML-003)
- Non-deterministic API pagination in `_read_ml_forecasts_from_api()`
  (tracked separately — requires `sapphire/services/` coordination)

---

## Related Issues

- **ML-001**: Hindcast subprocess failure not handled (FileNotFoundError) —
  guards the upstream call; this issue fixes the downstream assignment
- **ML-002**: Root cause of hindcast subprocess failures (why target-date
  counts differ) — may resolve the trigger but this fix is still needed
  for defensive correctness
- **ML-004**: Hindcast gap-fill never persists to API — same `raise e`
  anti-pattern; fix here serves as reference implementation

---

## References

- `apps/machine_learning/recalculate_nan_forecasts.py:335-368` —
  `update_forecast()` definition and call site with bare `raise e`
- `apps/machine_learning/recalculate_nan_forecasts.py:374-399` —
  API write block that is never reached when the loop crashes
- `apps/machine_learning/test/test_api_integration.py` — existing mock
  patterns for ML tests
- `doc/dev/testing_workflow.md` — test structure conventions

---

## Dependency Graph

```json
{
  "phases": {
    "phase_1": {
      "title": "Fix update_forecast() shape mismatch and remove bare raise",
      "file": "apps/machine_learning/recalculate_nan_forecasts.py",
      "lines": "335-368",
      "depends_on": [],
      "parallel_with": []
    },
    "phase_2": {
      "title": "Add unit tests for shape-mismatch scenarios",
      "file": "apps/machine_learning/test/test_recalculate_nan_shape_mismatch.py",
      "depends_on": ["phase_1"],
      "parallel_with": []
    },
    "phase_3": {
      "title": "Run full ML test suite",
      "command": "cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning",
      "depends_on": ["phase_2"],
      "parallel_with": []
    }
  },
  "execution_groups": [
    {
      "group": 1,
      "parallel": false,
      "agents": [
        {
          "id": "agent_fix",
          "phases": ["phase_1"],
          "reason": "Single file edit — no parallelism needed"
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
          "reason": "Tests must be written against the fixed implementation"
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
          "reason": "Final validation after all changes are in place"
        }
      ]
    }
  ]
}
```
