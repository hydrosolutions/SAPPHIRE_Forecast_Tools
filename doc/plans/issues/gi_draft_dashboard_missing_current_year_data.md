# GitHub Issue: FD-001

**Title**: `fix(forecast_dashboard): Handle missing current year data gracefully in weather plots`

**Labels**: `bug`, `forecast_dashboard`, `user-experience`, `low-priority`

**Status**: Draft

---

## Summary

The forecast dashboard crashes with `AttributeError: 'float' object has no attribute 'round'` when there is no current year data in the linear regression predictor file. Instead of crashing, the dashboard should display a helpful message directing users to add data to their operational data source.

## Context

**Note**: This issue will be resolved as part of the upcoming database migration, where all data will be read from the database instead of CSV files. The fix described here is a temporary workaround for the current file-based implementation.

## Error

```
Traceback (most recent call last):
  File ".../forecast_dashboard.py", line 1994, in update_active_tab
    daily_temperature_plot.object = viz.plot_daily_temperature_data(_, temp, station.value, date_picker.value, linreg_predictor)
  File ".../src/vizualization.py", line 2043, in plot_daily_temperature_data
    current_year_text = f"{_('Current year')}, {current_period}: {predictor_rainfall['T'].mean().round()} °C"
AttributeError: 'float' object has no attribute 'round'
```

## Root Cause

1. `linreg_predictor` contains forecast data with dates only up to previous year (e.g., 2025-12-25)
2. `add_predictor_dates()` calculates predictor dates based on `linreg_predictor['date'].max()`
3. When filtering current year data by predictor dates from previous year, result is empty DataFrame
4. `.mean()` on empty DataFrame returns `NaN` (Python float), which lacks `.round()` method

Affects both `plot_daily_temperature_data()` (line 2043) and `plot_daily_rainfall_data()` (line 1895).

## Proposed Solution (Temporary)

Add minimal defensive check before the problematic lines:

```python
# Check for empty/NaN predictor data
if predictor_rainfall.empty or predictor_rainfall['T'].isna().all():
    return hv.Curve([]).opts(
        title=_("No current year data available. Please add data to your operational data source."),
        hooks=[remove_bokeh_logo]
    )
```

## User-Facing Message

**English**: "No current year data available. Please add data to your operational data source (iEasyHydro HF for Central Asian hydromets, or your organization's operational database)."

**Russian**: "Данные за текущий год отсутствуют. Пожалуйста, добавьте данные в оперативный источник данных."

## Tasks

- [ ] Add check for empty/NaN predictor data in `plot_daily_temperature_data()`
- [ ] Add check for empty/NaN predictor data in `plot_daily_rainfall_data()`
- [ ] Add translated message

## Acceptance Criteria

- [ ] Dashboard does not crash when current year data is missing
- [ ] User sees a clear message explaining the issue

## Notes

- Low priority: Will be resolved in database version of the code
- Temporary fix only - keep implementation minimal
