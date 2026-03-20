# ML-011: `make_forecast.py` flag=2 semantic collision with hindcast convention

**Status**: Draft
**Module**: `machine_learning`
**Priority**: Mid
**Labels**: `bug`, `data-integrity`, `flag-convention`

---

## Summary

`make_forecast.py` assigns `flag=2` to rows where the ML model produces empty
predictions (len(predictions) == 0, a model error). However, the documented
flag convention reserves flag=2 for "NaN value from hindcast (needs
recalculation)".

This semantic collision causes `recalculate_nan_forecasts.py` to treat
operational model errors as hindcast NaN rows and trigger unnecessary
hindcast attempts for them.

---

## Root Cause

**Flag convention** (from `recalculate_nan_forecasts.py` lines 6-10):

| Flag | Documented meaning |
|------|--------------------|
| 0 | Operational forecast, valid value |
| 1 | NaN from operational forecast (needs recalculation) |
| 2 | NaN from hindcast (needs recalculation) |
| 3 | NaN even after hindcast attempt (permanent failure) |
| 4 | Valid value produced by hindcast |

**`make_forecast.py` lines 744-752** (actual assignment):

```python
if len(predictions) == 0:
    flag = 2          # ← should be flag=1, not flag=2
elif predictions.isna().sum().sum() > 0:
    flag = 1
else:
    flag = 0
```

Both `flag=1` (NaN predictions) and `flag=2` (empty predictions) represent
operational forecast failures. The convention says both should use flag=1.
Flag=2 is reserved for hindcast output only — it should only be set by
`hindcast_ML_models.py`.

**Downstream impact**: `recalculate_nan_forecasts.py` line 276 filters on
`forecast_code["flag"].isin([1, 2])`. This correctly catches flag=1 rows
(operational NaN) and flag=2 rows (hindcast NaN). But because `make_forecast.py`
incorrectly uses flag=2 for operational errors, the recalculation treats them
as hindcast failures rather than operational failures. The hindcast will likely
produce the same empty result (flag=3) since the underlying data availability
issue is the same.

---

## Implementation Plan

### Phase 1: Fix flag assignment in `make_forecast.py`

**File**: `apps/machine_learning/make_forecast.py`
**Lines**: ~744-746

Change `flag = 2` to `flag = 1` for the empty-predictions case:

```python
# BEFORE:
if len(predictions) == 0:
    flag = 2
# AFTER:
if len(predictions) == 0:
    flag = 1
```

Both "NaN predictions" and "empty predictions" are operational forecast
failures and should use flag=1.

### Phase 2: Update stale docstring in `recalculate_nan_forecasts.py`

**File**: `apps/machine_learning/recalculate_nan_forecasts.py`
**Lines**: 6-7

The file header says "Nan values from operational forecasts have flag == 0,
while nan values from hindcasts have flag == 1". This is wrong — it should
say flag=1 for operational NaN and flag=2 for hindcast NaN (matching the
actual code at line 276).

```python
# BEFORE (lines 6-7):
# Nan values from operational forecasts have flag == 0, while nan values from hindcasts have flag == 1.
# AFTER:
# Nan values from operational forecasts have flag == 1, while nan values from hindcasts have flag == 2.
```

### Phase 3: Tests and verification

Run the ML test suite:
```bash
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning
```

---

## Acceptance Criteria

- [ ] `make_forecast.py` assigns flag=1 (not flag=2) for empty predictions
- [ ] `recalculate_nan_forecasts.py` docstring matches the five-value flag
      convention
- [ ] All ML tests pass
- [ ] No changes to `sapphire/services/`

---

## Risks and Mitigations

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Existing flag=2 rows from `make_forecast.py` in the DB won't be recalculated after fix | Low | `recalculate_nan_forecasts.py` still filters on `[1, 2]`, so existing flag=2 rows are still eligible for recalculation |
| Changing flag=2→1 changes the semantic meaning of existing data | Low | Both flags trigger the same recalculation path; the behavioral outcome is identical |

---

## Related Issues

- **ML-008**: Infinite hindcast loop on flag=3 rows — sibling flag-handling issue
- **ML-012**: `astype(int)` crash on NaN flags in `recalculate_nan_forecasts.py`

---

## Dependency Graph

```json
{
  "phases": {
    "phase_1": {
      "title": "Fix flag=2 to flag=1 in make_forecast.py",
      "file": "apps/machine_learning/make_forecast.py",
      "depends_on": [],
      "parallel_with": ["phase_2"]
    },
    "phase_2": {
      "title": "Update stale docstring in recalculate_nan_forecasts.py",
      "file": "apps/machine_learning/recalculate_nan_forecasts.py",
      "depends_on": [],
      "parallel_with": ["phase_1"]
    },
    "phase_3": {
      "title": "Run test suite",
      "depends_on": ["phase_1", "phase_2"],
      "parallel_with": []
    }
  },
  "execution_groups": [
    {
      "group": 1,
      "parallel": true,
      "agents": [
        {"id": "agent_flag_fix", "phases": ["phase_1"]},
        {"id": "agent_docstring", "phases": ["phase_2"]}
      ]
    },
    {
      "group": 2,
      "parallel": false,
      "agents": [
        {"id": "agent_tests", "phases": ["phase_3"]}
      ]
    }
  ]
}
```
