# ML-013: `recalculate_nan_forecasts.py` API write overwrites valid operational rows

**Status**: Review
**Module**: `machine_learning`
**Priority**: High
**Labels**: `bug`, `data-integrity`, `operational-pipeline`

---

## Summary

`recalculate_nan_forecasts.py` line 405 writes the **entire raw hindcast
DataFrame** to the API, not just the rows that replaced flag=1/2 entries.
This can overwrite valid `flag=0` operational forecast rows in the database
with `flag=3` or `flag=4` hindcast data for dates that had no NaN issues.

The CSV write path (line 419-420) correctly uses the surgically-merged
`forecast` DataFrame where only flag=1/2 rows are replaced. The API write
path does not.

---

## Root Cause

### The two write paths diverge

```python
# Line 405 — API write: sends FULL hindcast (all dates, all codes in range)
_write_ml_forecast_to_api(hindcast, horizon_type, MODEL_TO_USE)

# Lines 419-420 — CSV write: sends MERGED forecast (only flag=1/2 replaced)
forecast = normalize_ml_csv_columns(forecast)
forecast.to_csv(csv_path, index=False)
```

The `hindcast` DataFrame contains rows for the full date range
(`min_missing_date` to `max_missing_date`) across all `codes_with_nan`.
If only a subset of dates within that range had flag=1/2, the hindcast
still covers the entire range. When the full hindcast is upserted to the
API, it overwrites any existing rows for those dates — including valid
flag=0 operational forecasts.

### Example scenario

1. Station 15189 has forecasts from Jan 1-30:
   - Jan 1-10: flag=0 (valid operational)
   - Jan 11-15: flag=1 (operational NaN)
   - Jan 16-30: flag=0 (valid operational)
2. `recalculate_nan_forecasts.py` detects flag=1 rows for Jan 11-15
3. Hindcast runs for Jan 10-15 (min_date - 1 day to max_date)
4. Hindcast produces rows for Jan 10-15, all with flag=3 or flag=4
5. **API write**: all 6 hindcast rows upserted → Jan 10's valid flag=0 row
   is overwritten with flag=3/4 hindcast data
6. **CSV write**: only Jan 11-15 rows replaced in the merged DataFrame →
   Jan 10 retains its flag=0 value

After this, the API and CSV are **inconsistent** for Jan 10.

---

## Implementation Plan

### Phase 1: Write only the replaced rows to the API

**File**: `apps/machine_learning/recalculate_nan_forecasts.py`
**Lines**: ~396-426 (write section)

Instead of writing the raw `hindcast` to the API, collect only the rows
that actually replaced flag=1/2 entries in the `update_forecast` loop,
and write those.

**Step 1.1 — Collect replaced rows in the update loop**

Modify the main loop (lines 380-394) to collect the hindcast rows that
were actually used as replacements:

```python
# Main loop — collect rows that were actually applied
replaced_rows = []
for code in codes_with_nan:
    forecast_code = forecast[forecast["code"] == code].copy()
    hindcast_code = hindcast[hindcast["code"] == code].copy()
    try:
        updated, applied = update_forecast(forecast_code, hindcast_code)
        forecast[forecast["code"] == code] = updated
        replaced_rows.append(applied)
    except Exception as e:
        logger.error(
            "recalculate_nan_forecasts: update_forecast failed for "
            "code=%s: %s — skipping code, NaN records preserved.",
            code,
            e,
        )
```

This requires `update_forecast` to also return the hindcast rows that were
actually applied (the subset of `hindcast_code` rows whose dates matched
flag=1/2 forecast dates).

**Step 1.2 — Modify `update_forecast` to return applied rows**

Add a second return value: the subset of **updated forecast rows** (not raw
hindcast rows) for `forecast_dates_flag1` where the hindcast had matching
data. These are the rows whose flag changed from 1/2 to 3/4 — they carry
the correct merged Q values and flag values from the merge-based
`update_forecast()` (already implemented by ML-006).

**Why updated forecast rows, not raw hindcast rows**: The API upserts on
`(code, forecast_date, date)`. Writing the updated forecast rows ensures
the correct merged flag (3 or 4) and Q values are sent. Raw hindcast rows
may have different flag values or cover dates that weren't actually needed.

**Implementation**: After the merge loop in `update_forecast()`, collect
the rows where `forecast_code["flag"]` changed from its original value
(was in [1, 2], now is 3 or 4). Return these as the second value.

**Step 1.3 — Write only replaced rows to API**

```python
# BEFORE (line 405):
api_write_ok = _write_ml_forecast_to_api(hindcast, horizon_type, MODEL_TO_USE)

# AFTER:
if replaced_rows:
    api_data = pd.concat(replaced_rows, ignore_index=True)
    api_write_ok = _write_ml_forecast_to_api(api_data, horizon_type, MODEL_TO_USE)
```

### Phase 2: Tests

| # | Scenario | Asserts |
|---|----------|---------|
| 1 | Hindcast covers wider range than flag=1/2 rows | Only flag=1/2-matching rows written to API |
| 2 | Some codes have no matching hindcast rows | Those codes contribute no rows to API write |
| 3 | All flag=1/2 rows replaced | API write contains exactly the replaced rows |

### Phase 3: Run test suite

```bash
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning
```

---

## Acceptance Criteria

- [ ] API write sends only rows that replaced flag=1/2 entries, not the full
      hindcast DataFrame
- [ ] CSV write continues to use the merged forecast (existing behavior)
- [ ] Valid flag=0 operational rows are never overwritten by hindcast data
- [ ] All ML tests pass
- [ ] No changes to `sapphire/services/`

---

## Risks and Mitigations

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| `update_forecast` refactor introduces regression | Medium | Phase 2 tests validate the return value contract |
| Empty `replaced_rows` list → no API write | Low | This is correct behavior: if no rows were replaced, nothing should be written |
| Existing overwritten flag=0 rows in DB | Already happened | Separate operational cleanup — re-run operational forecasts for affected date ranges |

---

## Related Issues

- **ML-008**: Infinite hindcast loop in `fill_ml_gaps.py` — sibling issue,
  independent (different file)
- **ML-012**: Flag coercion crash in the same file (line 334) — must be
  applied first; line 334 crash blocks execution before line 405
- ~~**ML-011**~~: Deleted 2026-03-20 — no behavioral impact
- ~~**ML-014**~~: Covered by ML-012

---

## Dependency Graph

```json
{
  "phases": {
    "phase_1": {
      "title": "Write only replaced rows to API",
      "file": "apps/machine_learning/recalculate_nan_forecasts.py",
      "depends_on": ["ML-012"],
      "note": "ML-012 must be applied first — line 334 crash blocks execution before this code is reached",
      "parallel_with": []
    },
    "phase_2": {
      "title": "Add tests for selective API write",
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
        {"id": "agent_fix", "phases": ["phase_1"], "reason": "Requires ML-012 fix on line 334 to be in place first"}
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
