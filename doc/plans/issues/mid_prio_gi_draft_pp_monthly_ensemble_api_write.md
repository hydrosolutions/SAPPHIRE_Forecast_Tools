# PP-032: Monthly ensemble forecasts not written to API

**Status**: Draft
**Module**: postprocessing_forecasts
**Priority**: High (upgraded from Medium — this is blocking LT ensemble visibility)
**Labels**: `bug`, `postprocessing`, `api`, `long-term`

---

## Summary

The long-term postprocessing script (`postprocessing_operational_long_term.py`) computes monthly ensemble forecasts (EM, Skilled Mean, Naive Mean) and calls `save_monthly_forecast_data()` which *does* have an API write path — but two bugs prevent the ensembles from reaching the database:

1. **Early-return bug**: When CSV env vars are not configured (common on server), the function returns before reaching the API write call.
2. **Field semantic mismatches**: Even when the API writer runs, `horizon_value` and `date` use different semantics than the individual model records, making the data inconsistent.

## Context

During the 2026-03-30 local pipeline review:

- The script logged "Monthly CSV path not configured ... skipping CSV save"
- Querying `/api/postprocessing/long-forecast/` for `horizon_type=month` showed only individual model records (GBT, LR_Base, LR_SM, etc.) — **no ensemble models (EM, NE, NM)**
- Quarterly forecasts (38,532 records) were successfully written to the API by `save_quarterly_forecast_data()`, which has no CSV dependency

## Bug 1: Early-return skips API write

**File:** `apps/postprocessing_forecasts/src/file_writer.py:440-451`

```python
csv_dir = os.getenv("ieasyforecast_intermediate_data_path")
csv_file = os.getenv("ieasyforecast_monthly_combined_forecast_file")
if not csv_dir or not csv_file:
    logger.warning(
        "Monthly CSV path not configured ..., skipping CSV save",
        csv_dir, csv_file,
    )
    return None  # <-- EXITS HERE, API write at line 501 never reached
```

The API write call exists at line 501:
```python
ret = api_writer._write_monthly_ensemble_to_api(simulated)
```

But it's placed **after** the CSV write block, behind the early return. On most deployments, CSV env vars are unset, so the function always exits at line 451.

**Contrast with quarterly/seasonal writers**: `save_quarterly_forecast_data()` (line 664) and `save_seasonal_forecast_data()` (line 696) are API-only — no CSV dependency, no early return.

**Fix**: Move the API write call before the CSV-path check, or restructure so the CSV absence doesn't block the API path. The API write should be unconditional (guarded only by API availability checks inside `_write_monthly_ensemble_to_api` itself).

**Tests**: `test_file_writer.py::TestSaveMonthlyForecastDataApiWithoutCsv` — two tests that currently FAIL, proving the bug. They will pass after the fix.

## Bug 2: `horizon_value` semantic mismatch

**File:** `apps/postprocessing_forecasts/src/api_writer.py:834`

```python
record = {
    "horizon_type": "month",
    "horizon_value": month,  # <-- absolute calendar month (1-12)
    ...
}
```

Individual model forecasts written by `long_term_forecasting` use `horizon_value` as a **month offset** (0=current month, 1=next month, etc.), set from `get_operational_month_lead_time()`.

Evidence from the 2026-03-30 review:
- Individual model records: `horizon_value=0` for month_0, `horizon_value=1` for month_1
- The ensemble writer would produce `horizon_value=4` for an April forecast

This means a query like `?horizon_value=1` would return individual models but NOT the ensemble — they're stored under different `horizon_value` keys for the same target.

**Root cause**: `_normalize_monthly_forecasts()` (data_reader.py:1086) extracts `month = valid_from.dt.month` (absolute month) from the API response. The original `horizon_value` is **dropped** at line 1316 during `_read_monthly_combined_forecasts_api()`. So the ensemble calculator never sees the offset, and the writer falls back to using the absolute month.

**Fix**: Preserve `horizon_value` through the data reader normalization, or compute the offset from the issue date and target month. The ensemble writer should use the same offset as the individual models it aggregates.

**Tests**: `test_api_integration.py::TestMonthlyEnsembleHorizonValueConsistency` — documents current behavior with TODO markers for the fix.

## Bug 3: `date` field mismatch

**File:** `apps/postprocessing_forecasts/src/api_writer.py:836`

```python
record = {
    ...
    "date": valid_from,  # <-- first day of target month
    ...
}
```

Individual model records use `date` = **forecast issue date** (e.g. `2026-03-25`). The ensemble writer sets `date = valid_from` (e.g. `2026-04-01`). This means queries filtering by issue date won't find both individual and ensemble records together.

**Fix**: Use the `date` column from the input DataFrame (which carries the issue date from the original forecasts) instead of `valid_from`.

**Tests**: `test_api_integration.py::TestMonthlyEnsembleDateFieldConsistency` — documents current behavior with TODO markers for the fix.

## NaN guard (PP-029 dependency)

The seasonal writer crashed with "cannot convert float NaN to integer". The monthly writer already has NaN guards via `pd.notna()` checks on `forecasted_discharge` and quantile columns (api_writer.py:841-851). These work correctly — NaN produces `q=None` and NaN quantiles are excluded from the record.

**Tests**: `test_api_integration.py::TestMonthlyEnsembleNaNGuard` — 3 tests confirming NaN handling works.

## Data flow (corrected)

```
long_term_forecasting module
  -> writes individual model forecasts to /long-forecast/
     with horizon_value=offset (0,1,2...), date=issue_date           OK

postprocessing_operational_long_term.py
  -> reads individual forecasts + skill metrics
  -> creates monthly ensembles (EM, Skilled Mean, Naive Mean)        OK
  -> save_monthly_forecast_data()
    -> CSV path check: return early if not configured                BUG 1
    -> CSV write (when configured)                                   OK
    -> api_writer._write_monthly_ensemble_to_api(simulated)          UNREACHABLE
      -> horizon_value = absolute month (1-12)                       BUG 2
      -> date = valid_from (not issue_date)                          BUG 3
      -> NaN guard on q and quantiles                                OK
```

## Desired Outcome

After fixing all three bugs:
1. `save_monthly_forecast_data()` writes ensembles to API regardless of CSV configuration
2. Ensemble records use `horizon_value` matching the individual models they aggregate
3. Ensemble records use `date` = issue date, matching individual model convention
4. Querying `/api/postprocessing/long-forecast/?horizon_type=month&horizon_value=1` returns both individual models AND ensemble models for the same target month

## Implementation Plan

### Phase 1: Fix early-return (Bug 1)
- Move the API write call in `save_monthly_forecast_data()` before the CSV-path check
- Or restructure: API write first (unconditional), then CSV write (conditional on env vars)

### Phase 2: Fix field semantics (Bugs 2 & 3)
- Preserve `horizon_value` from the API response through `_normalize_monthly_forecasts()`
- In `_write_monthly_ensemble_to_api()`: use preserved `horizon_value` (or compute offset), use `date` column instead of `valid_from`
- Update existing test `test_ensemble_record_format` (line 1467: `horizon_value == 6`) to assert the correct offset

### Phase 3: Update tests
- Flip the failing early-return tests to expect success
- Update `horizon_value` and `date` consistency tests from "documents current behavior" to "asserts correct behavior"
- Run full test suite

## Related Issues

- **PP-029** — NaN guard in seasonal/quarterly API write. Monthly NaN guard already works correctly.
- **PP-017** (Complete) — Quarterly forecast postprocessing. Reference implementation for API-only write pattern.
