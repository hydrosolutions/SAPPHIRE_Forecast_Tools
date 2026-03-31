
# PP-033: Gap detector should only flag boundary dates as missing ensembles

**Status**: Won't Fix
**Module**: postprocessing_forecasts
**Priority**: Low
**Labels**: `enhancement`, `postprocessing`, `maintenance`

---

## Resolution (2026-03-31): Won't Fix

The problem is low-impact (only unnecessary API fetches + log noise) and
self-resolving (DB cleanup of pre-PP-031 spurious records removes the root
cause; PP-031 prevents new ones). The proposed fix would add fragile
cross-module coupling (importing private `_is_pentad_boundary` /
`_is_decad_boundary` helpers from `data_reader.py` into `gap_detector.py`)
and silently change the function's contract by dropping dates — a hidden
behavior change that could break future callers or tests. Not worth the
complexity for a transient issue.

---

## Summary

`detect_missing_ensembles()` in `gap_detector.py` builds a universe of all `(date, code)` pairs from `combined_forecasts` and `modelled_forecasts`, then flags dates where EM/NE is absent. It has no concept of pentad/decad boundary days — it flags non-boundary dates where spurious ML pentad/decad records exist but EM is (correctly) absent.

After PP-031 (boundary filter in `_normalize_ml_forecasts`), no *new* spurious records are created. But existing spurious records in the DB still cause the gap detector to flag non-boundary dates, triggering unnecessary maintenance work. The maintenance path is harmless (the boundary filter drops the rows, producing empty results), but it wastes API reads and log noise.

## Context

- **PP-031** (prerequisite): Adds boundary-day filter to `_normalize_ml_forecasts()`. Prevents new spurious records. Does NOT change the gap detector.
- **DB cleanup** (separate follow-up): Deletes existing spurious non-boundary records. Once done, the gap detector would stop flagging them even without this fix.
- **This issue**: Makes the gap detector itself boundary-aware, so it never flags non-boundary dates regardless of DB state.

## Problem

In `gap_detector.py:87-99`, the universe of `(date, code)` pairs is built from all dates in the data. No filter checks whether a date is a pentad/decad issue day. On non-boundary dates where ML pentad records exist (from the pre-PP-031 era), the gap detector flags them as missing EM/NE.

**Impact**: Low. Maintenance reads through `_normalize_ml_forecasts` (which now has the boundary filter), so flagged non-boundary dates produce empty results — no harm. But it causes unnecessary API fetches and noisy gap detection logs.

## Desired Outcome

- `detect_missing_ensembles()` only flags boundary dates (pentad: 5/10/15/20/25/EOM; decad: 10/20/EOM)
- Non-boundary dates in the `(date, code)` universe are silently excluded
- Log message reports how many non-boundary pairs were excluded

---

## Implementation Plan

### Files to Modify

| File | Changes |
|------|---------|
| `apps/postprocessing_forecasts/src/gap_detector.py` | Import boundary helpers from `data_reader`; filter `all_pairs` to boundary dates after line 99 |
| `apps/postprocessing_forecasts/tests/test_gap_detector.py` | Add `TestBoundaryDateGapDetection` class |

### Implementation Steps

- [ ] Import `_is_pentad_boundary` and `_is_decad_boundary` from `src.data_reader` (added in PP-031)
- [ ] After `all_pairs` is built (line 99), filter to boundary dates based on `horizon_type`
- [ ] Add log message for excluded non-boundary pairs
- [ ] Add test: non-boundary date with ML but no EM → NOT flagged as gap
- [ ] Add test: boundary date with ML but no EM → flagged as gap (existing behavior preserved)
- [ ] Run tests: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts`

### Code Example

```python
# After line 99 in gap_detector.py:
all_pairs = pd.concat(pair_frames, ignore_index=True).drop_duplicates()

# PP-033: Only flag boundary dates as gaps
from src.data_reader import _is_pentad_boundary, _is_decad_boundary

if horizon_type == "pentad":
    boundary_mask = all_pairs["date"].apply(_is_pentad_boundary)
else:
    boundary_mask = all_pairs["date"].apply(_is_decad_boundary)

n_non_boundary = (~boundary_mask).sum()
if n_non_boundary > 0:
    logger.info(
        "Gap detection %s: excluded %d non-boundary (date, code) pairs",
        horizon_type,
        n_non_boundary,
    )
all_pairs = all_pairs[boundary_mask]

if all_pairs.empty:
    return empty
```

### Test Cases

```python
class TestBoundaryDateGapDetection:
    """PP-033: Gap detector should not flag non-boundary dates."""

    def test_non_boundary_date_not_flagged_as_gap(self):
        """Combined has ML on non-boundary Jan 4 but no EM → not a gap."""
        df = pd.DataFrame({
            "date": pd.to_datetime(["2024-01-04"] * 2 + ["2024-01-05"] * 3),
            "code": ["10001"] * 5,
            "model_short": ["TFT", "TiDE", "LR", "TFT", "EM"],
            "forecasted_discharge": [1.0] * 5,
        })
        result = detect_missing_ensembles(df, horizon_type="pentad")
        gap_dates = result["date"].tolist()
        assert pd.Timestamp("2024-01-04") not in gap_dates

    def test_boundary_date_still_flagged(self):
        """Combined has ML on boundary Jan 5 but no EM → flagged as gap."""
        df = pd.DataFrame({
            "date": pd.to_datetime(["2024-01-05"] * 2),
            "code": ["10001"] * 2,
            "model_short": ["TFT", "TiDE"],
            "forecasted_discharge": [1.0, 2.0],
        })
        result = detect_missing_ensembles(df, horizon_type="pentad")
        gap_dates = set(result["date"].tolist())
        assert pd.Timestamp("2024-01-05") in gap_dates
```

---

## Dependencies

- PP-031 (boundary filter in `_normalize_ml_forecasts`) — provides `_is_pentad_boundary` / `_is_decad_boundary` helpers in `data_reader.py`

## Acceptance Criteria

- [ ] Non-boundary dates are excluded from gap detection universe
- [ ] Boundary dates are still flagged when EM/NE is missing
- [ ] Existing gap detector tests pass unchanged
- [ ] Log message reports excluded non-boundary pairs

---

## References

- Gap detector: `apps/postprocessing_forecasts/src/gap_detector.py:15-145`
- Maintenance caller: `apps/postprocessing_forecasts/postprocessing_maintenance.py:192-267`
- PP-031: boundary filter in `_normalize_ml_forecasts` (prerequisite)
