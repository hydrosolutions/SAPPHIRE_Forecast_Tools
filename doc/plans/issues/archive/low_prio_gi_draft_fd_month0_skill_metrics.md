# FD: Display skill metrics for month_0 forecasts

| Field       | Value                                                   |
|-------------|---------------------------------------------------------|
| **Module**  | `forecast_dashboard`                                    |
| **Priority**| Low                                                     |
| **Status**  | Implemented — ready for visual verification             |
| **Branch**  | `fix_fd_monthly_skill_metrics`                          |

## Problem

The forecast dashboard displays skill metrics for month_1 (next-month) forecasts but not for month_0 (current-month) forecasts. When a user selects the monthly horizon and a station with month_0 data, the month_0 summary table shows forecasts but no skill metric columns (NSE, MAE, accuracy, etc.).

Skill metrics in the database are indexed by `(code, month_in_year, model_short)` where `month_in_year` is the calendar month (1-12). They are not specific to a lead time (horizon_value) — the same skill metric applies to both month_0 and month_1 forecasts for the same target calendar month. The dashboard already fetches these skill metrics but only merges them into the month_1 forecast DataFrame.

## Root Cause (pre-fix state)

In `apps/forecast_dashboard/src/db.py`, `_get_data_monthly()`:

1. **month_1 path**: Fetched forecasts, fetched skill metrics, merged on `["code", "month_in_year", "model_short"]` — worked correctly.
2. **month_0 path**: Fetched forecasts only, stored raw DataFrame without skill metric merge.

```python
# month_1: skill metrics merged ✓
forecasts_all = i18n_models(add_labels(get_long_forecasts(station, horizon_value=1)))
forecast_stats = i18n_models(get_forecast_stats("month", station))
if can_merge:
    forecasts_all = forecasts_all.merge(forecast_stats, on=merge_keys, ...)

# month_0: no merge ✗  (BEFORE fix)
if "month_0" in supported_modes:
    data["long_forecasts_m0"] = i18n_models(
        add_labels(get_long_forecasts(station, horizon_value=0))
    )
```

## Why the same skill metrics apply to month_0

Both month_0 and month_1 forecasts derive `month_in_year` from `valid_from.dt.month` (db.py:484). A month_0 forecast for April has `month_in_year=4`, same as a month_1 forecast for April. The skill metrics for `month_in_year=4` represent historical forecast quality for April regardless of lead time.

The skill metrics pipeline (`recalculate_skill_metrics.py`) does not filter by `horizon_value` — it pools all monthly forecasts for a given `(code, month, model)` into one skill computation (data_reader.py:1323 drops `horizon_value` before skill calculation).

## Implementation (complete)

### Phase 1: Merge skill metrics into month_0 forecasts — done

**File modified**: `apps/forecast_dashboard/src/db.py`, lines 575-590

The month_0 block now reuses `forecast_stats` (already fetched at line 544) and merges with the same `merge_keys` as month_1:

```python
if "month_0" in supported_modes:
    m0 = i18n_models(add_labels(get_long_forecasts(station, horizon_value=0)))
    can_merge_m0 = (
        not m0.empty
        and not forecast_stats.empty
        and all(k in m0.columns for k in merge_keys)
        and all(k in forecast_stats.columns for k in merge_keys)
    )
    if can_merge_m0:
        m0 = m0.merge(
            forecast_stats,
            on=merge_keys,
            how="left",
            suffixes=("", "_stats"),
        )
    data["long_forecasts_m0"] = m0
```

**No changes needed in**: `plot_manager.py`, `widget_manager.py`, `data_manager.py` — all already pass m0 to the same tabulator factory.

### Phase 2: Tests — done

**File modified**: `apps/forecast_dashboard/tests/test_db.py`

Three tests added to `TestGetDataMonthly`:

1. **`test_merges_skill_metrics_into_month0_forecasts`** (line 286): Verifies skill metric columns (`delta`, `sdivsigma`, `mae`, `accuracy`) appear in `long_forecasts_m0` and values match fixtures.
2. **`test_month0_without_skill_metrics_still_returns_forecasts`** (line 307): Empty skill-metric API response — forecasts present, no skill columns (graceful fallback).
3. **`test_month0_disabled_returns_empty_dataframe`** (line 328): `month_0` not in supported modes — `long_forecasts_m0` is empty.

## Verification

1. `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` — all tests pass
2. Run dashboard locally with `ieasyhydroforecast_ml_long_term_supported_modes=month_0,month_1`
3. Select monthly horizon, station 15189
4. month_1 table: skill metrics visible (unchanged from before)
5. month_0 table: skill metrics now visible (same values as month_1 for same calendar month)

## Out of Scope

- **Lead-time-specific skill metrics**: Computing separate skill metrics for month_0 vs month_1 would require splitting by `horizon_value` in the skill metrics pipeline. Currently both lead times pool into the same `(code, month_in_year, model)` skill metric. This is a valid future enhancement but a separate concern.
- **month_0 CRPS/reliability display**: If month_0 forecasts don't have quantiles for some models, CRPS won't be available. This is expected and already handled gracefully (shown as NaN).
