# PP-032: Monthly ensemble forecasts not written to API

**Status**: Complete
**Module**: postprocessing_forecasts
**Priority**: High (upgraded from Medium — this is blocking LT ensemble visibility)
**Labels**: `bug`, `postprocessing`, `api`, `long-term`

---

## Summary

The long-term postprocessing script (`postprocessing_operational_long_term.py`) computes monthly ensemble forecasts (EM, Skilled Mean, Naive Mean) and calls `save_monthly_forecast_data()` which *does* have an API write path — but four bugs prevent the ensembles from reaching the database correctly:

1. **Early-return bug**: When CSV env vars are not configured (common on server), the function returns before reaching the API write call.
2. **Ensemble groupby missing `horizon_value`**: The ensemble calculator groups by `["year", "month", "code"]` instead of `["year", "month", "code", "horizon_value"]`, mixing forecasts with different lead times into the same ensemble.
3. **`horizon_value` semantic mismatch in writer**: The API writer uses absolute calendar month (1-12) instead of the lead time offset from the individual models.
4. **`date` field mismatch in writer**: The API writer uses `valid_from` (target month start) instead of the forecast issue date.

## Context

During the 2026-03-30 local pipeline review:

- The script logged "Monthly CSV path not configured ... skipping CSV save"
- Querying `/api/postprocessing/long-forecast/` for `horizon_type=month` showed only individual model records (GBT, LR_Base, LR_SM, etc.) — **no ensemble models (EM, NE, NM)**
- Quarterly forecasts (38,532 records) were successfully written to the API by `save_quarterly_forecast_data()`, which has no CSV dependency

API schema example confirms semantics (`sapphire/services/postprocessing/app/schemas.py:76-82`):
- `horizon_value: 1` — lead time offset, NOT absolute month
- `date: "2026-01-22"` — issue date, NOT validity start
- `valid_from: "2026-02-01"` — first day of target period

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

**Fix**: Restructure `save_monthly_forecast_data()` so the API write runs unconditionally after the empty-data guard and basic cleanup (rounding, code cleanup), before the CSV-path check. The API write is guarded internally by `SAPPHIRE_API_ENABLED` / `SAPPHIRE_API_AVAILABLE` checks inside `_write_monthly_ensemble_to_api`.

**Tests**: `test_file_writer.py::TestSaveMonthlyForecastDataApiWithoutCsv` — two tests that currently FAIL, proving the bug. They will pass after the fix.

## Bug 2: Ensemble groupby missing `horizon_value`

**File:** `apps/postprocessing_forecasts/src/ensemble_calculator.py:311,413,459`

The ensemble calculator groups by `["year", "month", "code"]` for all three ensemble types:

```python
# EM (line 311)
em_avg = qualifying.groupby(["year", "month", "code"]).agg(em_agg).reset_index()

# Skilled Mean (line 413)
pool.groupby(["year", "month", "code"])

# Naive Mean (line 459)
naive_avg = pool.groupby(["year", "month", "code"]).agg(naive_agg).reset_index()
```

**Problem**: `read_latest_monthly_forecasts()` (`data_reader.py:1116`) reads ALL monthly forecasts from the API for the latest target month. The API query (`_read_long_forecasts_api:1061`) does NOT filter by `horizon_value`. If both `month_0` (issued in April for April, `horizon_value=0`) and `month_1` (issued in March for April, `horizon_value=1`) records exist for the same target month, they are all returned and mixed into the same ensemble.

**Domain rationale**: An ensemble should combine models that share the same forecast context — same issue date, same lead time, same target. A month_0 forecast has different skill characteristics than a month_1 forecast for the same target month. They must be ensembled separately.

**Data availability**: `horizon_value` IS available in the operational path. `_normalize_monthly_forecasts()` (`data_reader.py:1087`) does NOT drop `horizon_value`. It is only dropped in `_normalize_monthly_forecasts_for_gap_detection()` (`data_reader.py:1317`) — the maintenance path. So the operational entry point passes `horizon_value` through to the ensemble calculator; it just isn't used in the groupby.

**Fix**: Add `"horizon_value"` to the groupby keys in all three ensemble functions. This produces separate ensembles per lead time. In the aggregation dicts, `horizon_value` becomes a groupby key so it does NOT need a `"first"` entry.

**Fallback**: If `horizon_value` is absent (backward compatibility with CSV data or maintenance path), skip adding it to the groupby. A simple guard:
```python
group_cols = ["year", "month", "code"]
if "horizon_value" in qualifying.columns:
    group_cols.append("horizon_value")
```

**Comment requirement**: The `"first"` aggregation on `date` (line 309) assumes all models in a `(year, month, code, horizon_value)` group share the same issue date. This holds when models run in a single pipeline invocation. Add a comment above `em_agg[dcol] = "first"` (and corresponding lines in the helper functions) documenting this assumption:
```python
# "first" for date: assumes all models in a (year, month, code, horizon_value)
# group share the same issue date (single pipeline invocation).
```

**Compare with short-term**: The short-term ensemble path (`ensemble_calculator.py:174`) groups by `[period_col, "date", "code"]` — `date` implicitly carries the issue-date identity. The monthly path should follow this pattern by including `horizon_value`.

## Bug 3: `horizon_value` semantic mismatch in API writer

**File:** `apps/postprocessing_forecasts/src/api_writer.py:834`

```python
record = {
    "horizon_type": "month",
    "horizon_value": month,  # <-- absolute calendar month (1-12)
    ...
}
```

Individual model forecasts written by `long_term_forecasting` use `horizon_value` as a **month offset** (0=current month, 1=next month, etc.), set from `get_operational_month_lead_time()` (`lt_utils.py:312,345`).

Evidence from the 2026-03-30 review:
- Individual model records: `horizon_value=0` for month_0, `horizon_value=1` for month_1
- The ensemble writer would produce `horizon_value=4` for an April forecast

This means a query like `?horizon_value=1` would return individual models but NOT the ensemble — they're stored under different `horizon_value` keys for the same target.

**Fix**: Use `row["horizon_value"]` from the DataFrame (now preserved through the ensemble groupby from Bug 2). Fall back to `month` only if `horizon_value` is absent (backward compatibility):
```python
"horizon_value": (
    int(row["horizon_value"])
    if "horizon_value" in row.index and pd.notna(row.get("horizon_value"))
    else month
),
```

**Tests**: `test_api_integration.py::TestMonthlyEnsembleHorizonValueConsistency` — documents current behavior with TODO markers for the fix.

## Bug 4: `date` field mismatch in API writer

**File:** `apps/postprocessing_forecasts/src/api_writer.py:836`

```python
record = {
    ...
    "date": valid_from,  # <-- first day of target month
    ...
}
```

Individual model records use `date` = **forecast issue date** (e.g. `2026-03-25`, `lt_utils.py:347`). The ensemble writer sets `date = valid_from` (e.g. `2026-04-01`). This means queries filtering by issue date won't find both individual and ensemble records together.

**Data availability**: The ensemble calculator preserves `date` via `"first"` aggregation (`ensemble_calculator.py:309`). After the Bug 2 fix (groupby includes `horizon_value`), all models in a group share the same issue date, so `"first"` is correct.

**Fix**: Use `row["date"]` if present, fall back to `valid_from` if absent:
```python
"date": (
    str(row["date"])[:10]
    if "date" in row.index and pd.notna(row.get("date"))
    else valid_from
),
```

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
  -> read_latest_monthly_forecasts()
     -> _read_long_forecasts_api() — no horizon_value filter          OK (reads all)
     -> _normalize_monthly_forecasts() — horizon_value preserved      OK
     -> filter to latest (year, month) — may include mixed h_v        OK (by design)
  -> create_monthly_ensemble_forecasts()
     -> groupby(["year", "month", "code"])                            BUG 2 (missing horizon_value)
     -> "first" agg on date, valid_from, valid_to                     OK (after Bug 2 fix, all same h_v)
  -> save_monthly_forecast_data()
     -> CSV path check: return early if not configured                BUG 1
     -> CSV write (when configured)                                   OK
     -> api_writer._write_monthly_ensemble_to_api(simulated)          UNREACHABLE (Bug 1)
       -> horizon_value = absolute month (1-12)                       BUG 3
       -> date = valid_from (not issue_date)                          BUG 4
       -> NaN guard on q and quantiles                                OK
```

## Desired Outcome

After fixing all four bugs:
1. `save_monthly_forecast_data()` writes ensembles to API regardless of CSV configuration
2. Ensembles are computed per (horizon_value, year, month, code) — separate ensembles for different lead times
3. Ensemble records use `horizon_value` matching the individual models they aggregate
4. Ensemble records use `date` = issue date, matching individual model convention
5. Querying `/api/postprocessing/long-forecast/?horizon_type=month&horizon_value=1` returns both individual models AND ensemble models for the same target month

---

## Technical Analysis

### Key Files

| File | Lines | Role |
|------|-------|------|
| `apps/postprocessing_forecasts/src/file_writer.py` | 421-532 | `save_monthly_forecast_data()` — early-return bug |
| `apps/postprocessing_forecasts/src/ensemble_calculator.py` | 311, 413, 459 | Monthly ensemble groupby — missing `horizon_value` |
| `apps/postprocessing_forecasts/src/api_writer.py` | 760-879 | `_write_monthly_ensemble_to_api()` — field mismatches |
| `apps/postprocessing_forecasts/src/data_reader.py` | 1299-1323 | `_normalize_monthly_forecasts_for_gap_detection()` — drops `horizon_value` (maintenance path only; operational path is fine) |

### Callers of modified functions

| Function | Callers | Effect of fix |
|----------|---------|---------------|
| `save_monthly_forecast_data` | `postprocessing_operational_long_term.py:141` | API write now executes |
| `create_monthly_ensemble_forecasts` | `postprocessing_operational_long_term.py:113` | Separate ensembles per horizon_value |
| `_add_skilled_mean_monthly` | `create_monthly_ensemble_forecasts:326` | Inherits corrected groupby |
| `_add_naive_mean_monthly` | `create_monthly_ensemble_forecasts:334` | Inherits corrected groupby |
| `_write_monthly_ensemble_to_api` | `save_monthly_forecast_data:501` | Correct horizon_value + date |

### API schema reference

From `sapphire/services/postprocessing/app/schemas.py:48-102`:
```python
class LongForecastBase(BaseModel):
    horizon_type: HorizonType     # "month"
    horizon_value: int            # lead time offset (0, 1, 2...)
    code: str                     # station code
    date: DateType                # forecast issue date
    valid_from: DateType          # target period start
    valid_to: DateType            # target period end
```

Schema example: `horizon_value=1, date="2026-01-22", valid_from="2026-02-01"`.

---

## Implementation Plan

### Phase 1: Fix early-return (Bug 1)

**Files**: `file_writer.py`

In `save_monthly_forecast_data()`:
1. Keep the empty-data guard (line 436-438) — it should stay first.
2. Move rounding (line 456) and code cleanup (line 459-460) right after the empty guard.
3. Insert the API write call (lines 500-508) immediately after cleanup, BEFORE the CSV-path check.
4. The CSV-path check (line 440-451) becomes a guard for CSV-only logic. Change `return None` to just `pass` through (or restructure the remaining CSV block under the `if csv_dir and csv_file:` condition).

**Restructured flow**:
```python
def save_monthly_forecast_data(simulated):
    if simulated is None or simulated.empty:
        return None

    simulated = simulated.round(3)
    # ... code cleanup ...

    # API write (unconditional — internally guarded)
    ret = api_writer._write_monthly_ensemble_to_api(simulated)
    # ... log result ...

    # CSV write (conditional on env vars)
    csv_dir = os.getenv(...)
    csv_file = os.getenv(...)
    if not csv_dir or not csv_file:
        logger.warning("... skipping CSV save")
        return None
    # ... CSV writes + consistency check ...
```

**Acceptance**: The two failing tests in `TestSaveMonthlyForecastDataApiWithoutCsv` pass.

### Phase 2: Fix ensemble groupby (Bug 2)

**Files**: `ensemble_calculator.py`

Add `"horizon_value"` to the groupby keys in all three monthly ensemble functions, with a backward-compatible guard:

```python
# In create_monthly_ensemble_forecasts, before the EM block:
group_cols = ["year", "month", "code"]
if "horizon_value" in forecasts.columns:
    group_cols.append("horizon_value")
```

Then use `group_cols` instead of `["year", "month", "code"]` at:
- Line 311: EM groupby
- Line 413: Skilled Mean groupby (inside `_add_skilled_mean_monthly`)
- Line 459: Naive Mean groupby (inside `_add_naive_mean_monthly`)

The `group_cols` list must be passed to `_add_skilled_mean_monthly` and `_add_naive_mean_monthly` as a parameter (they currently hardcode the groupby keys).

**No change to short-term ensembles**: Line 174 (`groupby([period_col, "date", "code"])`) is a different code path and is not affected.

**Acceptance**: When input has mixed `horizon_value`, separate ensembles are produced per horizon_value.

### Phase 3: Fix API writer field semantics (Bugs 3 & 4)

**Files**: `api_writer.py`

In `_write_monthly_ensemble_to_api()`, change lines 834 and 836:

```python
# Bug 3 fix: use horizon_value from data if available
"horizon_value": (
    int(row["horizon_value"])
    if "horizon_value" in row.index and pd.notna(row.get("horizon_value"))
    else month
),

# Bug 4 fix: use date (issue date) from data if available
"date": (
    str(row["date"])[:10]
    if "date" in row.index and pd.notna(row.get("date"))
    else valid_from
),
```

**Acceptance**: Ensemble records have `horizon_value` and `date` matching the individual models.

### Phase 4: Update tests

**Files**: `test_file_writer.py`, `test_api_integration.py`

1. The two `TestSaveMonthlyForecastDataApiWithoutCsv` tests should now pass (Bug 1 fix) — no test code change needed.

2. Update `test_ensemble_record_format` (line 1467): `assert em_rec["horizon_value"] == 6` → assert the correct value based on input data.

3. Flip `TestMonthlyEnsembleHorizonValueConsistency::test_horizon_value_uses_absolute_month_currently`:
   - Provide input with `horizon_value=1`
   - Assert `record["horizon_value"] == 1` (not `== 4`)

4. Flip `TestMonthlyEnsembleDateFieldConsistency::test_date_equals_valid_from_currently`:
   - Assert `record["date"] == "2024-03-25"` (not `== "2024-04-01"`)

5. **New tests** (see Testing section below).

### Phase 5: Run full test suite

```bash
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
```

Zero failures, zero unexpected skips.

---

## Implementation Safety Guardrails

### Constraints for the implementing agent

1. **Do NOT change `postprocessing_operational_long_term.py`** — the entry point is correct; only the called functions are buggy.

2. **Do NOT change `_normalize_monthly_forecasts()`** (`data_reader.py:1087`) — it already preserves `horizon_value`. The drop at line 1317 is in the gap detection normalizer, which is a different function and out of scope.

3. **Do NOT change the short-term ensemble path** — `ensemble_calculator.py:174` (pentad/decad groupby) is correct and must not be touched.

4. **Do NOT change the quarterly/seasonal write paths** — `_write_aggregated_forecasts_to_api()` has the same `date=valid_from` pattern (line 1044) but fixing it is out of scope. Note: this is tracked as a follow-up.

5. **Do NOT change any API schemas or service code** in `sapphire/services/`.

6. **Backward compatibility**: All fixes must handle missing `horizon_value` and `date` columns gracefully (fall back to current behavior). Older data or CSV-sourced data may lack these columns.

7. **The `_add_skilled_mean_monthly` and `_add_naive_mean_monthly` functions** need the `group_cols` list passed as a parameter. Do NOT duplicate the `group_cols` construction — compute it once in `create_monthly_ensemble_forecasts` and pass it down.

### Code examples

#### Bug 1 fix (`file_writer.py`)

```python
def save_monthly_forecast_data(simulated: pd.DataFrame):
    if simulated is None or simulated.empty:
        logger.info("No monthly forecast data to save")
        return None

    # Round all float values to 3 decimal places
    simulated = simulated.round(3)

    # Ensure code is string without .0
    if "code" in simulated.columns:
        simulated["code"] = simulated["code"].astype(str).str.replace(
            r"\.0$", "", regex=True
        )

    # Write ensemble rows (EM, Naive Mean, Skilled Mean) to API
    # This runs unconditionally — internal guards check API availability
    ret = api_writer._write_monthly_ensemble_to_api(simulated)
    if ret:
        logger.info("Monthly ensemble forecasts written to API successfully.")
    else:
        logger.warning(
            "Monthly ensemble forecasts API write returned False "
            "(disabled, unavailable, or failed)."
        )

    # CSV write — conditional on env vars
    csv_dir = os.getenv("ieasyforecast_intermediate_data_path")
    csv_file = os.getenv("ieasyforecast_monthly_combined_forecast_file")
    if not csv_dir or not csv_file:
        logger.warning(
            "Monthly CSV path not configured "
            "(ieasyforecast_intermediate_data_path=%s, "
            "ieasyforecast_monthly_combined_forecast_file=%s), "
            "skipping CSV save",
            csv_dir,
            csv_file,
        )
        return None

    # ... rest of CSV write logic unchanged ...
```

#### Bug 2 fix (`ensemble_calculator.py`)

```python
# In create_monthly_ensemble_forecasts, compute group_cols once:
group_cols = ["year", "month", "code"]
if "horizon_value" in forecasts.columns:
    group_cols.append("horizon_value")

# EM groupby (line 311):
em_avg = qualifying.groupby(group_cols).agg(em_agg).reset_index()

# Pass group_cols to helper functions:
joint = _add_skilled_mean_monthly(joint, skill_filtered, baselines, _QUANTILE_COLS, group_cols)
joint = _add_naive_mean_monthly(joint, baselines, _QUANTILE_COLS, group_cols)
```

```python
# In _add_skilled_mean_monthly (add group_cols parameter):
def _add_skilled_mean_monthly(
    joint, skill_filtered, baselines, quantile_cols, group_cols,
):
    # ... existing code ...
    # Replace hardcoded groupby:
    sm_avg = qualifying.groupby(group_cols).agg(sm_agg).reset_index()
```

```python
# Same pattern for _add_naive_mean_monthly:
def _add_naive_mean_monthly(joint, baselines, quantile_cols, group_cols):
    # ... existing code ...
    naive_avg = pool.groupby(group_cols).agg(naive_agg).reset_index()
```

---

## Required Tests

All tests use **fake DataFrames** (no mocks on the function under test). Existing patterns in `test_api_integration.py` and `test_file_writer.py` are followed.

### A. Bug 1: Early-return fix (existing failing tests — will pass after fix)

Already in `test_file_writer.py::TestSaveMonthlyForecastDataApiWithoutCsv`:
- `test_api_write_called_when_csv_not_configured` — mocks API writer, verifies it's called
- `test_api_receives_ensemble_data_without_csv` — verifies DataFrame passed to API writer contains ensemble rows

### B. Bug 1: API write also runs when CSV IS configured

New test in `test_file_writer.py::TestSaveMonthlyForecastDataApiWithoutCsv` (or new class):

```python
def test_api_write_called_when_csv_is_configured(self, monthly_ensemble_data, tmp_path):
    """API write must happen even when CSV path IS configured."""
    overrides = {
        "ieasyforecast_intermediate_data_path": str(tmp_path),
        "ieasyforecast_monthly_combined_forecast_file": "test_monthly.csv",
        "SAPPHIRE_API_ENABLED": "true",
        "SAPPHIRE_CONSISTENCY_CHECK": "false",
    }
    with patch.dict(os.environ, overrides):
        with patch(
            "src.api_writer._write_monthly_ensemble_to_api",
            return_value=True,
        ) as mock_api:
            file_writer.save_monthly_forecast_data(monthly_ensemble_data)
            mock_api.assert_called_once()
```

### C. Bug 2: Ensemble groupby with mixed horizon_values

New test class in a new file `test_monthly_ensemble_groupby.py` or appended to an existing test file:

```python
class TestMonthlyEnsembleHorizonValueGroupby:
    """PP-032: Ensembles must be computed per horizon_value."""

    def test_mixed_horizon_values_produce_separate_ensembles(self):
        """month_0 and month_1 for same target get separate EM rows."""
        forecasts = pd.DataFrame({
            "code": ["15013"] * 4,
            "year": [2024] * 4,
            "month": [4] * 4,
            "month_in_year": [4] * 4,
            "model_short": ["GBT", "LR_Base", "GBT", "LR_Base"],
            "horizon_value": [0, 0, 1, 1],
            "date": ["2024-04-10", "2024-04-10", "2024-03-25", "2024-03-25"],
            "forecasted_discharge": [100.0, 110.0, 90.0, 95.0],
            "valid_from": ["2024-04-01"] * 4,
            "valid_to": ["2024-04-30"] * 4,
            "flag": [0] * 4,
        })
        # Skill stats that qualify both models
        # Thresholds: sdivsigma < 0.6, nse > 0.8, accuracy > 0.8
        skill_stats = pd.DataFrame({
            "month_in_year": [4, 4],
            "code": ["15013", "15013"],
            "model_short": ["GBT", "LR_Base"],
            "sdivsigma": [0.3, 0.4],
            "nse": [0.85, 0.9],
            "delta": [0.1, 0.2],
            "accuracy": [0.85, 0.9],
            "mae": [5.0, 6.0],
            "n_pairs": [10, 10],
        })
        result = create_monthly_ensemble_forecasts(forecasts, skill_stats)

        # --- EM ---
        em_rows = result[result["model_short"] == "EM"]
        # Should have 2 EM rows: one for horizon_value=0, one for horizon_value=1
        assert len(em_rows) == 2
        assert set(em_rows["horizon_value"]) == {0, 1}
        # horizon_value=0 ensemble: mean(100, 110) = 105
        em_hv0 = em_rows[em_rows["horizon_value"] == 0]
        assert em_hv0["forecasted_discharge"].iloc[0] == pytest.approx(105.0)
        # horizon_value=1 ensemble: mean(90, 95) = 92.5
        em_hv1 = em_rows[em_rows["horizon_value"] == 1]
        assert em_hv1["forecasted_discharge"].iloc[0] == pytest.approx(92.5)

        # --- Skilled Mean ---
        sm_rows = result[result["model_short"] == "Skilled Mean"]
        assert len(sm_rows) == 2, "Skilled Mean must also separate by horizon_value"
        assert set(sm_rows["horizon_value"]) == {0, 1}

        # --- Naive Mean ---
        nm_rows = result[result["model_short"] == "Naive Mean"]
        assert len(nm_rows) == 2, "Naive Mean must also separate by horizon_value"
        assert set(nm_rows["horizon_value"]) == {0, 1}

    def test_single_horizon_value_works_unchanged(self):
        """When all records have the same horizon_value, behavior is unchanged."""
        # Same structure as existing tests but with horizon_value column
        forecasts = pd.DataFrame({
            "code": ["15013"] * 2,
            "year": [2024] * 2,
            "month": [4] * 2,
            "month_in_year": [4] * 2,
            "model_short": ["GBT", "LR_Base"],
            "horizon_value": [1, 1],
            "date": ["2024-03-25"] * 2,
            "forecasted_discharge": [100.0, 110.0],
            "valid_from": ["2024-04-01"] * 2,
            "valid_to": ["2024-04-30"] * 2,
            "flag": [0] * 2,
        })
        # Thresholds: sdivsigma < 0.6, nse > 0.8, accuracy > 0.8
        skill_stats = pd.DataFrame({
            "month_in_year": [4, 4],
            "code": ["15013", "15013"],
            "model_short": ["GBT", "LR_Base"],
            "sdivsigma": [0.3, 0.4],
            "nse": [0.85, 0.9],
            "delta": [0.1, 0.2],
            "accuracy": [0.85, 0.9],
            "mae": [5.0, 6.0],
            "n_pairs": [10, 10],
        })
        result = create_monthly_ensemble_forecasts(forecasts, skill_stats)
        em_rows = result[result["model_short"] == "EM"]
        assert len(em_rows) == 1
        assert em_rows["horizon_value"].iloc[0] == 1

    def test_no_horizon_value_column_backward_compat(self):
        """When horizon_value column is absent, groupby uses (year, month, code) only."""
        forecasts = pd.DataFrame({
            "code": ["15013"] * 2,
            "year": [2024] * 2,
            "month": [4] * 2,
            "month_in_year": [4] * 2,
            "model_short": ["GBT", "LR_Base"],
            "date": ["2024-03-25"] * 2,
            "forecasted_discharge": [100.0, 110.0],
            "valid_from": ["2024-04-01"] * 2,
            "valid_to": ["2024-04-30"] * 2,
            "flag": [0] * 2,
        })
        # Thresholds: sdivsigma < 0.6, nse > 0.8, accuracy > 0.8
        skill_stats = pd.DataFrame({
            "month_in_year": [4, 4],
            "code": ["15013", "15013"],
            "model_short": ["GBT", "LR_Base"],
            "sdivsigma": [0.3, 0.4],
            "nse": [0.85, 0.9],
            "delta": [0.1, 0.2],
            "accuracy": [0.85, 0.9],
            "mae": [5.0, 6.0],
            "n_pairs": [10, 10],
        })
        result = create_monthly_ensemble_forecasts(forecasts, skill_stats)
        em_rows = result[result["model_short"] == "EM"]
        assert len(em_rows) == 1
        # No horizon_value column in output
        # (or if propagated, it should not be present)
```

### D. Bugs 3 & 4: API writer uses correct horizon_value and date

Update existing tests in `test_api_integration.py`:

```python
# TestMonthlyEnsembleHorizonValueConsistency:
# Replace test_horizon_value_uses_absolute_month_currently with:
def test_horizon_value_uses_offset_from_data(self, ...):
    """horizon_value from input data is used, not absolute month."""
    data = pd.DataFrame({
        "code": ["15013"],
        "year": [2024],
        "month": [4],
        "month_in_year": [4],
        "forecasted_discharge": [102.5],
        "model_short": ["EM"],
        "horizon_value": [1],
        "date": ["2024-03-25"],
        "valid_from": ["2024-04-01"],
        "valid_to": ["2024-04-30"],
    })
    # ... mock setup ...
    record = records[0]
    assert record["horizon_value"] == 1  # offset, not 4

# TestMonthlyEnsembleDateFieldConsistency:
# Replace test_date_equals_valid_from_currently with:
def test_date_uses_issue_date_from_data(self, ...):
    """date field uses the issue date, not valid_from."""
    # ... same data with date="2024-03-25", valid_from="2024-04-01" ...
    record = records[0]
    assert record["date"] == "2024-03-25"  # issue date, not "2024-04-01"
    assert record["valid_from"] == "2024-04-01"  # unchanged
```

### E. Fallback tests for missing columns

```python
def test_horizon_value_falls_back_to_month_when_absent(self, ...):
    """When input lacks horizon_value column, falls back to month."""
    data = pd.DataFrame({
        "code": ["15013"],
        "year": [2024],
        "month": [6],
        "month_in_year": [6],
        "forecasted_discharge": [102.5],
        "model_short": ["EM"],
        # No horizon_value column
        "valid_from": ["2024-06-01"],
        "valid_to": ["2024-06-30"],
    })
    # ... mock setup ...
    record = records[0]
    assert record["horizon_value"] == 6  # fallback to absolute month

def test_date_falls_back_to_valid_from_when_absent(self, ...):
    """When input lacks date column, falls back to valid_from."""
    data = pd.DataFrame({
        "code": ["15013"],
        "year": [2024],
        "month": [6],
        "month_in_year": [6],
        "forecasted_discharge": [102.5],
        "model_short": ["EM"],
        "valid_from": ["2024-06-01"],
        "valid_to": ["2024-06-30"],
        # No date column
    })
    # ... mock setup ...
    record = records[0]
    assert record["date"] == "2024-06-01"  # fallback to valid_from
```

### F. Update existing test_ensemble_record_format

The existing test at line 1467 asserts `em_rec["horizon_value"] == 6` (absolute month). After the fix, since the fixture data has no `horizon_value` column, the fallback kicks in and the assertion stays `== 6`. This test verifies backward compatibility — no change needed.

---

## Risk Analysis

### Downstream consumers — safe

| Consumer | Risk from this fix |
|----------|--------------------|
| **Quarterly/seasonal writer** | Not touched. Note: has same `date=valid_from` pattern (line 1044) — separate follow-up |
| **Short-term ensembles** | Not touched. Different code path (line 174) |
| **Gap detector** | Not touched. Uses `_normalize_monthly_forecasts_for_gap_detection` which drops `horizon_value` — this is OK for gap detection purposes |
| **Dashboard** | Benefits — ensemble records now queryable alongside individual models |
| **Skill metrics** | Not affected — reads from different tables |
| **Maintenance path** | `read_monthly_combined_forecasts` goes through `_normalize_monthly_forecasts_for_gap_detection` which drops `horizon_value`. Maintenance ensembles will use the (year, month, code)-only groupby (backward-compat guard). This is acceptable — maintenance is gap-fill, not precision |

### Backward compatibility

All fixes include fallbacks for missing columns:
- `horizon_value` absent → fall back to `month` (current behavior)
- `date` absent → fall back to `valid_from` (current behavior)
- `group_cols` without `horizon_value` when column is absent → current groupby behavior

### Performance

No performance impact. The groupby adds one more key, which may slightly reduce group sizes. The API writer iterates the same number of rows.

---

## Documentation Impact

- [ ] No documentation impact — this is a bug fix

## Out of Scope

- Fixing `date=valid_from` in `_write_aggregated_forecasts_to_api()` (quarterly/seasonal) — separate follow-up
- Adding `horizon_value` filter to `_read_long_forecasts_api()` — not needed; the ensemble groupby correctly separates them
- Fixing `_normalize_monthly_forecasts_for_gap_detection` to preserve `horizon_value` — not needed for maintenance path
- DB cleanup of existing incorrect ensemble records — separate follow-up after fix is deployed

## Dependencies

- PP-029 (complete) — NaN guard, already working for monthly

## Follow-up Tasks

- Fix `date=valid_from` in `_write_aggregated_forecasts_to_api()` for quarterly/seasonal (same Bug 4 pattern)
- DB cleanup: delete existing monthly ensemble records with wrong `horizon_value` / `date`
- Consider adding `horizon_value` parameter to `_read_long_forecasts_api()` for more targeted queries

## Acceptance Criteria

- [x] `save_monthly_forecast_data()` calls API writer even when CSV env vars are unset
- [x] `save_monthly_forecast_data()` calls API writer even when CSV env vars ARE set (both paths execute)
- [x] Ensemble groupby includes `horizon_value` when column is present
- [x] Mixed `horizon_value` input produces separate ensembles per value
- [x] Missing `horizon_value` column falls back to current groupby behavior
- [x] API records have `horizon_value` from input data (not absolute month)
- [x] API records have `date` from input data (not `valid_from`)
- [x] Missing `horizon_value` in API writer falls back to `month`
- [x] Missing `date` in API writer falls back to `valid_from`
- [x] All existing tests pass (including the 2 previously-failing early-return tests)
- [x] No changes to `postprocessing_operational_long_term.py`, short-term ensemble path, or quarterly/seasonal writers
- [x] Full test suite: zero failures, zero unexpected skips

---

## Review Notes (2026-03-31)

### Review 1: Initial plan expansion

**Reviewer**: Opus orchestrator, critical review before implementation.

**Verdict**: Original plan had correct diagnosis for Bugs 1, 3, 4 but incomplete scope. Bug 2 (ensemble groupby) was missing entirely — this is the most important fix because without it, ensembles mix forecasts with different lead times.

**Findings incorporated**:

1. **Must-fix (added)**: Bug 2 — `ensemble_calculator.py` groups by `["year", "month", "code"]` without `horizon_value`. Added to scope with backward-compatible guard.
2. **Must-fix (expanded)**: Bug 3 — `horizon_value` preservation requires changes in `ensemble_calculator.py` (groupby + parameter passing to helper functions), not just `api_writer.py`.
3. **Must-fix (added)**: Fallback behavior for missing columns — older data or CSV-sourced data may lack `horizon_value` and `date`.
4. **Should-note (added)**: Quarterly/seasonal writer has same `date=valid_from` pattern — tracked as follow-up.
5. **Verified safe**: `_normalize_monthly_forecasts()` (operational path) already preserves `horizon_value`. The drop at line 1317 is maintenance-path only.
6. **Verified safe**: Short-term ensemble path (`ensemble_calculator.py:174`) is a different code path and not affected.

**Evidence for Bug 2**:
- Each `long_term_forecasting` pipeline run writes ONE `horizon_value` (`run_forecast.py:312`)
- But `read_latest_monthly_forecasts` does NOT filter by `horizon_value` (`_read_long_forecasts_api:1061`)
- Mixed `horizon_value` records for the same target month CAN reach the ensemble calculator
- Confirmed: `horizon_value` never appears in `ensemble_calculator.py` (grep returns 0 matches)

### Review 2: Pre-implementation critical review

**Reviewer**: Opus orchestrator, full source code verification.

**Verdict**: Plan is correct and safe to implement, with three corrections applied.

**Source code verification performed**:
- Read `file_writer.py:421-532` — Bug 1 confirmed (early return at line 451 before API write at line 501)
- Read `ensemble_calculator.py:270-473` — Bug 2 confirmed (groupby at lines 311, 413, 459 all use `["year", "month", "code"]` without `horizon_value`)
- Read `api_writer.py:760-879` — Bugs 3 & 4 confirmed (line 834: `horizon_value: month`, line 836: `date: valid_from`)
- Read `data_reader.py:1087-1108` — `_normalize_monthly_forecasts` preserves `horizon_value` (only renames `model_type`, extracts year/month)
- Read `data_reader.py:1116-1176` — `read_latest_monthly_forecasts` filters by `(year, month)` only, mixed horizon_values reach ensemble calculator
- Ran existing tests: 2 FAILED (`TestSaveMonthlyForecastDataApiWithoutCsv`), 1295 passed — confirms Bug 1
- Grep: `_add_skilled_mean_monthly` and `_add_naive_mean_monthly` only called from `create_monthly_ensemble_forecasts` — signature change is safe

**Corrections applied to plan**:

1. **Test gap fixed**: `test_mixed_horizon_values_produce_separate_ensembles` now asserts on **all three** ensemble types (EM, Skilled Mean, Naive Mean), not just EM. Without this, a bug in `_add_skilled_mean_monthly` or `_add_naive_mean_monthly` (e.g., forgetting to pass `group_cols`) would go undetected.

2. **Comment requirement added**: The `"first"` aggregation on `date` assumes all models in a `(year, month, code, horizon_value)` group share the same issue date. This holds in practice (single pipeline invocation) but is not guaranteed if a model is re-run independently. Added requirement to document this assumption in the code.

3. **This review section added** to record verification evidence.

**Risks verified as low**:
- `_weighted_mean` closure in `_add_skilled_mean_monthly` captures `pool` from outer scope — safe because `group.index` preserves original DataFrame indices regardless of groupby keys
- Named aggregation syntax (`**sm_agg`) in Skilled Mean — `horizon_value` as groupby key is correctly excluded from agg dict (becomes column after `reset_index()`)
- NaN in `horizon_value` column — cannot happen in operational path (API schema constraint); CSV path drops the column entirely (maintenance normalizer)
- Skill weights are per `(month_in_year, code, model_short)`, not per `horizon_value` — same weights applied regardless of lead time, which is correct (skill is a model property, not a lead-time property)

---

## References

- Related completed issues: PP-029 (NaN guard), PP-017 (quarterly postprocessing)
- Discovered: `review_checklist_local_2026-03-30.md`
- API schema: `sapphire/services/postprocessing/app/schemas.py:48-102`
- Individual model write: `apps/long_term_forecasting/lt_utils.py:342-352`
- Ensemble calculator: `apps/postprocessing_forecasts/src/ensemble_calculator.py:222-340`
