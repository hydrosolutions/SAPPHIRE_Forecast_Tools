# GitHub Issue: FD-004

**Title**: `fix(forecast_dashboard): LR forecast bounds and stats missing due to dropped delta column`

**Labels**: `bug`, `forecast_dashboard`, `high-priority`

**Assignee**: @maxatp

**Status**: Draft

---

## Summary

The forecast dashboard shows no upper/lower bounds and no stats (delta, sdivsigma, mae, accuracy) for Linear Regression (LR) forecasts in the summary forecast panel. The root cause is that `get_forecasts_all()` explicitly drops the `delta` column from LR data before it reaches the display layer.

## Root Cause

In `src/db.py:get_forecasts_all()`, line 369:

```python
df_lr.drop(columns=["horizon_type", "discharge_avg", "q_mean", "q_std_sigma", "delta", "id"],
        inplace=True, errors="ignore")
```

`delta` is the **only** uncertainty metric available for LR forecasts (the `lr_forecasts` table has no quantile columns like q05/q25/q75/q95). Dropping it here means downstream code cannot compute bounds.

### How bounds are computed

`src/processing.py:calculate_forecast_range()` (lines 1167-1213) computes bounds from `delta`:

```python
if range_type == _('delta'):
    forecast_table['fc_lower'] = forecast_table['forecasted_discharge'] - forecast_table['delta']
    forecast_table['fc_upper'] = forecast_table['forecasted_discharge'] + forecast_table['delta']
```

Since `delta` is NaN for LR rows after the drop, both `fc_lower` and `fc_upper` are NaN.

### Data model comparison

| Field | ML forecasts (`forecasts` table) | LR forecasts (`lr_forecasts` table) |
|-------|----------------------------------|-------------------------------------|
| `forecasted_discharge` | Yes | Yes |
| `q05, q25, q75, q95` | Yes | **No** |
| `delta` | No (comes from skill metrics merge) | **Yes** (stored directly) |
| `q_mean, q_std_sigma` | No | Yes |

The LR table stores `delta` as an intrinsic property of the regression (standard error of the prediction). This is the correct source for LR bounds — it does not need to come from the skill metrics merge.

### Stats columns

The stats columns (`sdivsigma`, `mae`, `accuracy`) come from the skill metrics merge in `get_data()` (line 436). Whether LR gets stats depends on whether skill metrics are written for model_type=LR. This is a separate concern from the bounds issue — see also FD-003 for the merge fan-out bug that may affect stats display for all models.

## Proposed Fix

In `src/db.py:get_forecasts_all()`, keep `delta` when processing LR forecasts. Change line 369 from:

```python
df_lr.drop(columns=["horizon_type", "discharge_avg", "q_mean", "q_std_sigma", "delta", "id"],
        inplace=True, errors="ignore")
```

to:

```python
df_lr.drop(columns=["horizon_type", "discharge_avg", "q_mean", "q_std_sigma", "id"],
        inplace=True, errors="ignore")
```

This preserves `delta` so that `calculate_forecast_range()` can compute `fc_lower` and `fc_upper` for LR forecasts.

### Verification

The LR module correctly writes `delta` to the API — see `iEasyHydroForecast/forecast_library.py:_write_lr_forecast_to_api()` (lines 3315-3319). The value is present in the database; it just gets discarded during dashboard read.

## Related Issues

- **FD-003** (skill metric merge fan-out): affects stats display for all models including LR

## Tasks

- [ ] Remove `"delta"` from the drop list in `get_forecasts_all()` for LR forecasts (`src/db.py:369`)
- [ ] Verify locally: run `panel serve` and confirm LR row shows `fc_lower` and `fc_upper` in the summary table
- [ ] Check that the delta-based bounds are sensible (fc_lower > 0, range is plausible)
- [ ] Run tests: `SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard`
