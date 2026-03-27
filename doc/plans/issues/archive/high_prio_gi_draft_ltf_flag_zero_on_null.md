# LTF-003: `run_forecast.py` sets flag=0 unconditionally — null forecasts marked as valid

**Status**: Draft
**Module**: `long_term_forecasting`
**Priority**: High
**Labels**: `bug`, `data-integrity`, `null-forecast`, `cascading-failure`, `operational-pipeline`
**Assigned to**: @sandrohuni

---

## Summary

In `long_term_forecasting/run_forecast.py`, the `flag` field is set to `0`
unconditionally after `predict_operational()` returns. When a model produces
all-NaN output (because input features contain NaN due to data gaps), the
record is still marked `flag=0` — indistinguishable from a valid operational
forecast. Downstream consumers, dependency checks, and the database all treat
it as valid.

A secondary issue exists in `prepare_long_forecast_records()` (`lt_utils.py`):
the function skips the `q` field for NaN rows but still appends the record,
writing skeleton records `{flag: 0, q: None}` to the API.

---

## Problem Statement

### The unconditional flag assignment

In `long_term_forecasting/run_forecast.py` at lines 272-275:

```python
forecast = model_instance.predict_operational(today=today)
forecast = forecast.round(2)
forecast["flag"] = 0  # ← SET UNCONDITIONALLY, even when forecast is all NaN
```

Flag semantics: `0` = operational forecast, `1` = hindcast, `2` = error/missing
data. When `predict_operational()` returns all-NaN values (because features
contain NaN due to data gaps), the flag is still set to `0`, marking the record
as a valid operational forecast.

### The secondary write-side issue

In `lt_utils.py`, `prepare_long_forecast_records()` at lines 354-362 checks
`pd.notna(row.get(q_model_col))` before setting the `q` field, but appends
the record regardless:

```python
# q field skipped for NaN rows, but record is still appended
records.append({
    "flag": int(row["flag"]),  # 0 — set unconditionally above
    "q": ...                   # None — because the notna check failed
})
```

Result: a skeleton record `{flag: 0, q: None}` is written to the API.

---

## Impact

### 1. Misleading data

Downstream consumers — postprocessing, skill metrics, dashboards — treat
`flag=0` records as valid forecasts. A skeleton record with
`{flag: 0, q: None}` is indistinguishable from a valid forecast at the API
level. No alarm is raised.

### 2. Cascading failure in MC_ALD

`MC_ALD` (UncertaintyMixture model) checks dependency success via:

```python
execution_is_success.get(dep, False)
```

Since `flag=0` is set on NaN output, each dependency appears successful even
when it produced nothing. `MC_ALD` then attempts to run on NaN inputs and
produces no output itself — silently.

### 3. Database pollution

Skeleton records with `{flag: 0, q: None}` are written to the API and
accumulate in the long-term forecasts table. They block any future retry logic
(the slot appears filled) and distort coverage statistics.

---

## Observed on 2026-03-20

8 models ran for the monthly horizon. Only 2 produced valid output:

| Model | Result | Reason |
|-------|--------|--------|
| LR_SM | q=1.92 (S1) | SWE point-in-time feature available |
| SM_GBT | q=1.91 (S1) | Feature-rich, tolerates partial NaN |

6 models produced NaN output but wrote `flag=0` records:

| Model | Failure reason |
|-------|---------------|
| LR_Base | Discharge 30d rolling lag contains NaN |
| LR_SM_DT | Discharge 30d rolling lag contains NaN |
| LR_SM_ROF | Discharge 30d rolling lag contains NaN |
| GBT | `allowable_missing_value_operational=0` — strict NaN tolerance → skip → empty |
| SM_GBT_Norm | Scaler propagates NaN |
| SM_GBT_LR | Cascading failure from LR_Base |

`MC_ALD` (UncertaintyMixture): absent from monthly records — cascading failure
from 6 of 8 NaN-producing dependencies.

---

## Root Cause

The root cause is in `run_forecast.py` — the flag assignment does not check
whether the forecast output contains any valid (non-NaN) Q values.

The secondary cause is in `prepare_long_forecast_records()` (`lt_utils.py`) —
it should skip writing records entirely when all Q fields are `None`, rather
than appending skeleton records.

---

## Proposed Fix

### Fix 1 (Required): NaN-aware flag assignment in `run_forecast.py`

**File**: `apps/long_term_forecasting/run_forecast.py`
**Lines**: 272-275

After `predict_operational()`, inspect the Q columns. If all values are NaN,
set `flag=2` (error/missing data). Otherwise set `flag=0` (valid operational).

```python
forecast = model_instance.predict_operational(today=today)
forecast = forecast.round(2)
# Determine whether the model produced any valid output
q_cols = [c for c in forecast.columns if c.startswith("q")]
if q_cols and forecast[q_cols].isna().all().all():
    forecast["flag"] = 2  # error: model ran but produced no valid output
    success = False        # cascade failure to dependent models (MC_ALD)
else:
    forecast["flag"] = 0  # valid operational forecast
    success = True
```

This ensures:
- `flag=0` only on records with at least one non-NaN Q value
- `flag=2` on records where the model returned all NaN (data gap, scaler
  failure, strict NaN tolerance, etc.)
- Downstream dependency checks (`execution_is_success`) correctly identify
  failed models and do not pass NaN inputs to `MC_ALD`
- `execution_is_success[model_name]` is set to `False` for NaN-producing
  models, correctly breaking the dependency chain for MC_ALD and other
  downstream models

### Fix 2 (Recommended): Skip writing skeleton records in `lt_utils.py`

**File**: `apps/long_term_forecasting/lt_utils.py`
**Function**: `prepare_long_forecast_records()`, lines 354-362

Add a guard that skips appending any record where all Q fields are `None`:

```python
# Skip records where all Q fields are None — no valid forecast to write
q_values = [
    rec.get(q_col)
    for q_col in [c for c in row.index if c.startswith("q")]
]
if all(v is None for v in q_values):
    logger.debug(
        "prepare_long_forecast_records: skipping all-None record "
        "for code=%s model=%s date=%s",
        row.get("code"),
        row.get("model_short"),
        row.get("date"),
    )
    continue
records.append(...)
```

This is a defense-in-depth measure. After Fix 1, `flag=2` records will be
distinguishable, so callers can choose to skip them at the API write layer.
Fix 2 ensures nothing is written to the API when there is no forecast value
at all, regardless of flag.

---

## Implementation Plan

### Phase 1: Code fixes

#### Phase 1a — NaN-aware flag assignment

**File**: `apps/long_term_forecasting/run_forecast.py`

Apply Fix 1 as described above. Confirm the flag value `2` matches the
established long-term forecast flag convention in the API schema (cross-check
`sapphire/services/postprocessing/app/models.py` — do not edit the service,
only verify).

**Critical**: Both `flag` and `success` must be set together. Setting `flag=2`
without `success=False` would still leave `execution_is_success[model_name]=True`,
allowing MC_ALD to run on NaN inputs.

#### Phase 1b — Skip skeleton record writes

**File**: `apps/long_term_forecasting/lt_utils.py`

Apply Fix 2 as described above. Add a `logger.debug` line (not `logger.info`)
to avoid noise in normal operation — this path fires for every failed model on
every station.

### Phase 2: Tests

**File**: `apps/long_term_forecasting/test/test_run_forecast_flag.py` (new)

| # | Scenario | Asserts |
|---|----------|---------|
| 1 | `predict_operational()` returns all-NaN q columns | `flag=2` set; `flag=0` not set |
| 2 | `predict_operational()` returns valid (non-NaN) values | `flag=0` set |
| 3 | `predict_operational()` returns mixed NaN/non-NaN (partial output) | `flag=0` set (at least one valid value) |
| 4 | `prepare_long_forecast_records()` receives all-None Q row | Record not appended; count unchanged |
| 5 | `prepare_long_forecast_records()` receives valid Q row | Record appended correctly |
| 6 | MC_ALD dependency check with `flag=2` model output | `execution_is_success` returns `False` for that model |

Follow Arrange → Act → Assert. Use synthetic DataFrames — no real forecast
models or API calls in unit tests.

### Phase 3: Verify end-to-end

**Step 3.1 — Run the test suite**

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh long_term_forecasting
```

All tests must pass with zero skips (except valid `SAPPHIRE_API_AVAILABLE`
dependency-gate skips).

**Step 3.2 — Simulate forecasts**

Run `simulate_forecasts.py` for a date that is known to produce NaN output
(e.g., a date where the 30-day discharge rolling lag is incomplete):

```bash
ieasyhydroforecast_env_file_path=~/Documents/GitHub/kyg_data_forecast_tools/config/.env_develop_kghm \
lt_forecast_mode=month_1 \
uv run python dev_code/simulate_forecasts.py --years 2024 --model LR_Base --num_months 1
```

Observe that the written record has `flag=2` (not `flag=0`) and no `q` field
(or `q=None` with the write guard in place — no skeleton record written).

**Step 3.3 — Verify MC_ALD no longer runs on NaN dependencies**

After the fix, `execution_is_success` should return `False` for any model that
produced `flag=2` output. Confirm in the logs that `MC_ALD` is skipped or
correctly identified as having failed dependencies.

---

## Acceptance Criteria

- [ ] `flag=2` is set (not `flag=0`) when `predict_operational()` returns all-NaN Q columns
- [ ] `flag=0` is set only when at least one Q value is non-NaN
- [ ] `success = False` is set alongside `flag=2`, ensuring `execution_is_success` correctly propagates failure
- [ ] `prepare_long_forecast_records()` does not append records where all Q fields are `None`
- [ ] `MC_ALD` dependency check correctly identifies `flag=2` model output as failure
- [ ] All 6 new unit tests pass
- [ ] Full long-term forecasting test suite passes with zero skips
- [ ] No skeleton `{flag: 0, q: None}` records are written to the API during a pipeline run with data gaps
- [ ] No changes to `sapphire/services/` (ownership boundary respected)

---

## Risks and Mitigations

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| `flag=2` not a valid value in the API schema for long-term forecasts | Low | Verify against `postprocessing` service models before implementing; coordinate with colleague if schema update is needed |
| Fix 1 partially-NaN case: some Q columns NaN, some not | Low | Treat as valid (`flag=0`) — the model produced at least one output; document this decision |
| Existing tests that assert `flag=0` unconditionally | Expected | Update those tests to match the new conditional logic |
| MC_ALD dependency check uses a different field than `execution_is_success` | Medium | Audit `run_forecast.py` to confirm exactly how dependency success is evaluated before implementing |

---

## Out of Scope

- Filling NaN output from individual models (data availability problem, not a
  flag-setting problem)
- Changing the flag value schema in `sapphire/services/` (requires colleague
  coordination; verify current schema supports `flag=2` for long-term forecasts
  before implementing)
- Backfilling existing `{flag: 0, q: None}` records in the database (separate
  operational cleanup task; do not conflate with this fix)
- Fixing the underlying data gaps that cause `predict_operational()` to return
  NaN (separate issue: data pipeline coverage)

---

## Related Issues

- **API-006**: Flag field on long-term forecasts — covers flag queryability and
  semantics at the API level
- **PP-026**: Clean null-discharge phantom forecasts — same pattern in the
  short-term pipeline (ML module writing `q=None` records with valid-looking
  flags)
- **ML-008**: `fill_ml_gaps.py` infinite loop on flag=3 rows — analogous bug in
  the short-term ML pipeline where null-discharge records are treated as valid
  date slots
- `simulate_forecasts.py` backfill path inherits this bug (calls `run_forecast`
  which sets `flag=0` unconditionally); the fix here also covers that path

---

## Source

Discovered during local pipeline review on 2026-03-20. Root cause analysis
completed on 2026-03-22. Documented in
`doc/plans/observations.md` entry "Long-Term Forecasting: Root Cause Analysis —
Missing/Null Monthly Forecasts".

---

## Dependency Graph

```json
{
  "phases": {
    "phase_1a": {
      "title": "NaN-aware flag assignment in run_forecast.py",
      "file": "apps/long_term_forecasting/run_forecast.py",
      "changes": [
        "Replace unconditional flag=0 with NaN check at lines 272-275",
        "Set flag=2 when all Q columns are NaN, flag=0 otherwise"
      ],
      "depends_on": [],
      "parallel_with": ["phase_1b"]
    },
    "phase_1b": {
      "title": "Skip skeleton record writes in lt_utils.py",
      "file": "apps/long_term_forecasting/lt_utils.py",
      "changes": [
        "Add guard in prepare_long_forecast_records() to skip all-None Q records",
        "Add logger.debug for skipped records"
      ],
      "depends_on": [],
      "parallel_with": ["phase_1a"]
    },
    "phase_2": {
      "title": "Add 6 unit tests",
      "file": "apps/long_term_forecasting/test/test_run_forecast_flag.py",
      "depends_on": ["phase_1a", "phase_1b"],
      "parallel_with": []
    },
    "phase_3": {
      "title": "Run test suite and verify end-to-end",
      "commands": [
        "cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh long_term_forecasting"
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
          "id": "agent_flag_fix",
          "phases": ["phase_1a"],
          "reason": "NaN-aware flag logic in run_forecast.py"
        },
        {
          "id": "agent_write_guard",
          "phases": ["phase_1b"],
          "reason": "Skeleton record guard in lt_utils.py — independent of flag logic"
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
          "reason": "Unit tests covering both fixes"
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
          "reason": "Final test suite run and simulation verification"
        }
      ]
    }
  ]
}
```
