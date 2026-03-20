# ML-012: `recalculate_nan_forecasts.py` crashes on NaN flag values

**Status**: Draft
**Module**: `machine_learning`
**Priority**: High
**Labels**: `bug`, `crash`, `operational-pipeline`, `data-integrity`

---

## Summary

`recalculate_nan_forecasts.py` line 334 calls `hindcast["flag"].astype(int)`
without error handling. If any hindcast row has a missing or NaN flag value,
this raises a `ValueError` and aborts the entire recalculation run for that
model/mode combination.

The crash is triggered by the dtype cascade: when the API stores `flag=NULL`
(which happens when `_write_ml_forecast_to_api` receives NaN flags), the
readback returns `float64` with `NaN` entries. The `astype(int)` on line 334
cannot convert `NaN` to `int`, causing the crash.

---

## Root Cause

### The dtype cascade

1. `_write_ml_forecast_to_api` (line 774): `"flag": int(row["flag"]) if pd.notna(row.get("flag")) else None` — writes `None` to API when flag is NaN
2. DB stores `NULL` (column is `Column(Integer)`, nullable, no default)
3. `_read_ml_forecasts_from_api` returns DataFrame where `flag` column is `float64` (pandas promotes `int64` to `float64` when NaN values are present)
4. `recalculate_nan_forecasts.py` line 253: `forecast["flag"].astype(int, errors="ignore")` — silently fails because `float64` with NaN cannot be cast to `int`; column stays `float64`
5. Line 276: `forecast_code["flag"].isin([1, 2])` — works because `1.0 == 1` in pandas
6. Line 334: `hindcast["flag"].astype(int)` — **crashes** because there is no `errors="ignore"` guard

### The two coercion lines

```python
# Line 253 — has guard, but guard masks the problem
forecast["flag"] = forecast["flag"].astype(int, errors="ignore")

# Line 334 — no guard, crashes on NaN
hindcast["flag"] = hindcast["flag"].astype(int)
```

---

## Implementation Plan

### Phase 1: Fix the crash and standardize flag coercion

**File**: `apps/machine_learning/recalculate_nan_forecasts.py`

**Step 1.1 — Fix line 253: use `pd.to_numeric` instead of `astype`**

```python
# BEFORE (line 253):
forecast["flag"] = forecast["flag"].astype(int, errors="ignore")

# AFTER:
forecast["flag"] = pd.to_numeric(forecast["flag"], errors="coerce")
```

`pd.to_numeric(errors="coerce")` converts valid values to numeric and
invalid/missing values to `NaN`. This is more explicit than `astype(int,
errors="ignore")` which silently does nothing when NaN is present.

**Step 1.2 — Fix line 334: add the same coercion**

```python
# BEFORE (line 334):
hindcast["flag"] = hindcast["flag"].astype(int)

# AFTER:
hindcast["flag"] = pd.to_numeric(hindcast["flag"], errors="coerce")
```

**Step 1.3 — Add the same coercion to `fill_ml_gaps.py`**

This is already covered by ML-008 Phase 1 Step 1.1 (`pd.to_numeric` on
`forecast_all["flag"]`). No additional change needed if ML-008 is
implemented first.

### Phase 2: Tests

**File**: `apps/machine_learning/test/test_recalculate_nan_forecasts.py` or
new file `test_recalculate_nan_flag_coercion.py`

| # | Scenario | Asserts |
|---|----------|---------|
| 1 | Hindcast DataFrame with `flag=NaN` row → no crash | Function completes, NaN-flag rows handled gracefully |
| 2 | Hindcast DataFrame with `flag=3.0` (float) → correctly recognized | `flag.isin([3])` matches `3.0` |
| 3 | Forecast DataFrame with mixed int/NaN flags → `.isin([1, 2])` still works | Correct rows filtered |

### Phase 3: Run test suite

```bash
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning
```

---

## Acceptance Criteria

- [ ] `recalculate_nan_forecasts.py` does not crash when hindcast has NaN flag
      values
- [ ] Flag coercion uses `pd.to_numeric(errors="coerce")` consistently
- [ ] All ML tests pass
- [ ] No changes to `sapphire/services/`

---

## Risks and Mitigations

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| `pd.to_numeric` changes dtype from `int64` to `float64` when NaN present | Expected | `.isin([1, 2])` works on both int and float; `int()` cast in write path handles float→int |
| NaN-flag rows slip through `.isin([1, 2])` filter | None | `NaN.isin([1, 2])` returns `False` — NaN rows are correctly excluded |

---

## Related Issues

- **ML-008**: Infinite hindcast loop — uses the same `pd.to_numeric` coercion
  pattern for `fill_ml_gaps.py`
- **ML-011**: Flag=2 semantic collision in `make_forecast.py`
- **ML-014**: `flag=None` dtype cascade (root cause of this crash)

---

## Dependency Graph

```json
{
  "phases": {
    "phase_1": {
      "title": "Fix flag coercion in recalculate_nan_forecasts.py",
      "file": "apps/machine_learning/recalculate_nan_forecasts.py",
      "depends_on": [],
      "parallel_with": []
    },
    "phase_2": {
      "title": "Add tests for NaN flag handling",
      "depends_on": ["phase_1"],
      "parallel_with": []
    },
    "phase_3": {
      "title": "Run test suite",
      "depends_on": ["phase_2"],
      "parallel_with": []
    }
  },
  "execution_groups": [
    {
      "group": 1,
      "parallel": false,
      "agents": [
        {"id": "agent_fix", "phases": ["phase_1"]}
      ]
    },
    {
      "group": 2,
      "parallel": false,
      "agents": [
        {"id": "agent_tests", "phases": ["phase_2"]}
      ]
    },
    {
      "group": 3,
      "parallel": false,
      "agents": [
        {"id": "agent_validation", "phases": ["phase_3"]}
      ]
    }
  ]
}
```
