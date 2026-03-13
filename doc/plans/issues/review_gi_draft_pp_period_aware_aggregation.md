# PP-023: Period-aware aggregation of ML daily targets

**Status**: Implemented (all steps verified 2026-03-13)
**Module**: postprocessing_forecasts
**Priority**: Critical
**Labels**: `bug`, `data-quality`, `postprocessing`

---

## Summary

`_normalize_ml_forecasts()` in `data_reader.py` averages ALL daily ML targets
for a given (code, date), regardless of whether those targets fall within the
pentad or decade period. This systematically biases pentad forecasts (and to a
lesser extent decade forecasts) by including targets from the adjacent period.

## Context

The ML module writes **daily-resolution** forecasts to the `forecasts` table
with `horizon_type=day`. Each forecast run produces a fixed number of daily
target rows:

| Mode | `forecast_horizon` | Example boundary | Targets written |
|------|--------------------|-----------------|-----------------|
| Pentad | 6 | Feb 25 | Feb 26 -- Mar 3 |
| Decade | 11 | Feb 20 | Feb 21 -- Mar 3 |

Source: `apps/machine_learning/hindcast_ML_models.py:169-171`.

The postprocessing module reads these daily rows and aggregates them to
pentad/decade level in `_normalize_ml_forecasts()`. The aggregation is supposed
to average only the targets that fall within the forecast period:

> *"Determine which targets belong to the current pentad/decade...
> Average only the targets within the period."*
> -- `doc/data_flow_short_term.md:240-243`

## Problem

`_normalize_ml_forecasts()` (`data_reader.py:1672-1673`) groups by
`(code, date)` and averages all targets indiscriminately:

```python
df = df.groupby(["code", "date"], as_index=False).agg(agg_dict)
```

The `target` column (which identifies the specific day being forecast) is
available in the raw API response but is never used for filtering.

### Impact

**Pentad**: 6 targets written, but pentad length varies from 3 to 6 days.

| Pentad | Days in period | Targets in period | Targets outside | Contamination |
|--------|---------------|-------------------|-----------------|---------------|
| 1 (days 1-5) | 5 | 5 | 1 | 17% |
| 2 (days 6-10) | 5 | 5 | 1 | 17% |
| 3 (days 11-15) | 5 | 5 | 1 | 17% |
| 4 (days 16-20) | 5 | 5 | 1 | 17% |
| 5 (days 21-25) | 5 | 5 | 1 | 17% |
| 6 (days 26-EOM) | 3-6 | 3-6 | 0-3 | 0-50% |

For pentad 6 of February (3 days: 26-28), 3 of 6 targets are from March
(pentad 1 of March). The "pentad forecast" is actually a 50/50 blend of the
current and next pentad.

**Decade**: 11 targets written, decade length varies from 8 to 11 days.

| Decade | Days in period | Targets in period | Targets outside | Contamination |
|--------|---------------|-------------------|-----------------|---------------|
| 1 (days 1-10) | 10 | 10 | 1 | 9% |
| 2 (days 11-20) | 10 | 10 | 1 | 9% |
| 3 (days 21-EOM) | 8-11 | 8-11 | 0-3 | 0-27% |

Less severe than pentad but still incorrect for decade 3, especially in
February.

### Downstream effects

- **NE (Neural Ensemble)**: Created from contaminated ML averages, so also
  biased.
- **EM (Ensemble Mean)**: If ML models pass skill thresholds, EM inherits
  the bias.
- **Skill metrics**: Annual recalculation compares contaminated forecasts
  against observations for the correct period, so skill scores are slightly
  wrong.

---

## Technical Analysis

### Current flow

```
API response (horizon_type=day):
  code=X, date=2026-02-25, target=2026-02-26, Q=3.1   <-- in pentad 6
  code=X, date=2026-02-25, target=2026-02-27, Q=3.2   <-- in pentad 6
  code=X, date=2026-02-25, target=2026-02-28, Q=3.0   <-- in pentad 6
  code=X, date=2026-02-25, target=2026-03-01, Q=2.9   <-- NEXT pentad
  code=X, date=2026-02-25, target=2026-03-02, Q=2.8   <-- NEXT pentad
  code=X, date=2026-02-25, target=2026-03-03, Q=2.7   <-- NEXT pentad

_normalize_ml_forecasts groups by (code, date=2026-02-25):
  forecasted_discharge = mean(3.1, 3.2, 3.0, 2.9, 2.8, 2.7) = 2.95

Should be:
  forecasted_discharge = mean(3.1, 3.2, 3.0) = 3.10
```

### Where the `target` column lives

1. **ML writer** (`utils_ml_forecast.py:695`): writes `target` as the
   individual day being forecast.
2. **API response**: returns `target` column.
3. **`_read_ml_forecasts_pp_api`** (`data_reader.py:1446`): returns raw
   DataFrame including `target`.
4. **`_normalize_ml_forecasts`** (`data_reader.py:1622`): receives `target`
   in the input DataFrame but never uses it for filtering. After the groupby,
   `target` is implicitly dropped (not in `agg_dict`).

### Period boundary logic

`tag_library.py` already has functions to determine which period a date
belongs to:

- `get_pentad(date)` -> pentad in month (1-6)
- `get_pentad_in_year(date)` -> pentad in year (1-72)
- `get_decad_in_month(date)` -> decade in month (1-3)
- `get_decad_in_year(date)` -> decade in year (1-36)

The existing code already computes `period_in_year` from `date + 1` (the
first day of the forecast period). The same logic can determine which
`target` dates fall within the period.

### Filtering approach

For each row, compute the period of `target`. Keep only rows where
`period_of(target) == period_of(date + 1)`. Then aggregate.

```python
# Compute period of each target date
df["target_period"] = df["target"].apply(get_period_in_year)

# Compute expected period from the forecast boundary date
df["expected_period"] = (df["date"] + pd.Timedelta(days=1)).apply(get_period_in_year)

# Filter to targets within the period
df = df[df["target_period"] == df["expected_period"]]

# Then aggregate
df = df.groupby(["code", "date"], as_index=False).agg(agg_dict)
```

---

## Implementation Plan

### Files to Modify

| File | Changes |
|------|---------|
| `apps/postprocessing_forecasts/src/data_reader.py` | Add target filtering in `_normalize_ml_forecasts` |
| `apps/postprocessing_forecasts/tests/test_data_reader_ml_aggregation.py` | New test file for aggregation logic |
| `doc/data_flow_short_term.md` | Update to reflect the fix (doc was already correct about intent) |

### Implementation Steps

- [x] **Step 1: Add period-aware target filtering to `_normalize_ml_forecasts`**

  In `data_reader.py`, `_normalize_ml_forecasts()` (line ~1645), after parsing
  dates and before the groupby:

  ```python
  # Filter daily targets to the forecast period boundary.
  # The forecast date is the last day of the previous period;
  # date+1 is the first day of the target period.
  if TAG_LIBRARY_AVAILABLE and "target" in df.columns and "date" in df.columns:
      df["target"] = pd.to_datetime(df["target"])

      if horizon_type == "pentad":
          period_func = tl.get_pentad_in_year
      else:
          period_func = tl.get_decad_in_year

      expected_period = (df["date"] + pd.Timedelta(days=1)).apply(period_func)
      target_period = df["target"].apply(period_func)

      in_period = target_period == expected_period
      n_dropped = (~in_period).sum()
      if n_dropped > 0:
          logger.info(
              "Filtered %d/%d daily targets outside %s boundary for %s",
              n_dropped, len(df), horizon_type, model,
          )
      df = df[in_period].copy()
  ```

  Keep the existing groupby and aggregation logic unchanged.

- [x] **Step 2: Handle edge case — no targets remain after filtering**

  After filtering, if the DataFrame is empty for a (code, date), that means
  no ML targets fell within the period. This can happen if the ML module's
  forecast horizon doesn't reach the target period (shouldn't happen in
  practice given horizon >= period length, but guard defensively):

  ```python
  if df.empty:
      logger.warning(
          "No %s targets within period for model %s after filtering",
          horizon_type, model,
      )
      return pd.DataFrame()
  ```

- [x] **Step 3: Write unit tests**

  New file `tests/test_data_reader_ml_aggregation.py`:

  ```
  test_pentad_filters_targets_to_period
      Given 6 daily targets spanning two pentads, assert only in-period
      targets are averaged.

  test_decad_filters_targets_to_period
      Given 11 daily targets spanning two decades, assert only in-period
      targets are averaged.

  test_pentad6_february_3_day_period
      Worst case: pentad 6 of Feb (3 days). Assert average uses only
      3 targets, not 6.

  test_all_targets_in_period
      When all targets fall within the period (e.g., pentad 1-5 with 5
      of 6 targets in period), assert 5 are kept and 1 is dropped.

  test_no_target_column_skips_filtering
      When target column is absent (legacy data), fall through to
      existing groupby-all behavior.

  test_empty_after_filter
      When no targets are in the period, return empty DataFrame.
  ```

- [x] **Step 4: Run tests**

  ```bash
  cd apps
  SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
  ```

- [x] **Step 5: Update documentation**

  In `doc/data_flow_short_term.md`, no text change needed (the doc already
  describes the correct intended behavior). Add a note in the "Key Data
  Transformations" section confirming filtering is now implemented.

---

## Testing

### Test Cases

- [ ] Pentad: 6 targets, 5 in period, 1 outside -> average of 5
- [ ] Pentad 6 Feb: 6 targets, 3 in period (Feb 26-28), 3 outside (Mar 1-3) -> average of 3
- [ ] Decade: 11 targets, 10 in period, 1 outside -> average of 10
- [ ] Decade 3 Feb: 11 targets, 8 in period (Feb 21-28), 3 outside -> average of 8
- [ ] No `target` column (backward compat): all targets averaged (existing behavior)
- [ ] All targets in period: no change to result
- [ ] Quantile columns (q05, q25, q75, q95) filtered the same way as forecasted_discharge
- [ ] Multiple codes in same batch: filtering is per-row, not per-group

### Testing Commands

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
```

---

## Acceptance Criteria

- [x] `_normalize_ml_forecasts` filters daily targets to the forecast period
  before aggregating
- [x] Pentad forecasts are the mean of only the days within the pentad
- [x] Decade forecasts are the mean of only the days within the decade
- [x] Quantile columns are filtered identically to forecasted_discharge
- [x] Backward compatible: missing `target` column (legacy) falls through to
  existing behavior
- [x] All existing tests pass; new tests cover filtering logic
- [x] No regression in operational or maintenance pipeline

---

## Out of Scope

- Changing the ML module's forecast horizon (it writes 6/11 days by design)
- Retroactively fixing historical aggregated forecasts in the database
  (would require a migration script)
- Changing how the annual skill recalculation reads ML data (it uses a
  different code path via `_read_ml_day_forecasts_for_recalc`)

## Dependencies

- None (self-contained fix in `data_reader.py`)

## References

- Data flow doc: `doc/data_flow_short_term.md` (lines 239-247)
- ML forecast writer: `apps/machine_learning/scr/utils_ml_forecast.py`
- ML forecast horizon: `apps/machine_learning/hindcast_ML_models.py:169-171`
- Tag library period functions: `apps/iEasyHydroForecast/tag_library.py`
