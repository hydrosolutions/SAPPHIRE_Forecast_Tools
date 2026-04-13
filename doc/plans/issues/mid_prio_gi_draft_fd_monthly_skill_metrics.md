# FD: Load skill metrics for monthly forecasts in dashboard

| Field       | Value                                                   |
|-------------|---------------------------------------------------------|
| **Module**  | `forecast_dashboard`                                    |
| **Priority**| Mid                                                     |
| **Status**  | Draft                                                   |
| **Branch**  | `fix_fd_monthly_skill_metrics`                          |

## Problem

The forecast dashboard shows all skill metric columns (δ, s/σ, MAE, Accuracy) as "-" for monthly forecasts. The root cause is that `_get_data_monthly()` in `db.py` hardcodes `forecast_stats: pd.DataFrame()` — it never calls the skill-metric API for the monthly horizon.

The skill-metric API endpoint supports `horizon=month` and stores monthly skill metrics with `horizon_in_year` = month number (1-12). The data exists in the database but is never fetched.

## Root Cause Analysis

Three gaps prevent monthly skill metrics from reaching the dashboard table:

### Gap 1: `_horizon_in_year_col()` has no `"month"` case

**File**: `apps/forecast_dashboard/src/db.py:32-33`

```python
def _horizon_in_year_col(horizon: str) -> str:
    return "decad_in_year" if horizon == "decade" else "pentad_in_year"
```

For `horizon="month"`, this returns `"pentad_in_year"`, which is wrong. It should return `"month_in_year"`.

### Gap 2: `get_long_forecasts()` drops the join key

**File**: `apps/forecast_dashboard/src/db.py:477`

```python
df.drop(columns=["id", "horizon_type", "horizon_value"], inplace=True, errors="ignore")
```

`horizon_value` is the month number needed to join forecasts with skill metrics. It gets dropped here. It should be renamed to `month_in_year` instead.

### Gap 3: `_get_data_monthly()` never fetches or merges skill metrics

**File**: `apps/forecast_dashboard/src/db.py:546`

```python
"forecast_stats":       pd.DataFrame(),
```

This is hardcoded empty. It should call `get_forecast_stats("month", station)` and then merge the result into `forecasts_all`, similar to how pentad/decad does it in `get_data()` (lines 510-525).

## Implementation Plan

### Phase 1: Fix the three gaps in `db.py`

**Files to modify**: `apps/forecast_dashboard/src/db.py`

**Do NOT change any existing function signatures, data flow logic, or control flow for pentad/decad horizons. Changes must only affect the `"month"` code path.**

#### Step 1a: Add `"month"` case to `_horizon_in_year_col()`

```python
def _horizon_in_year_col(horizon: str) -> str:
    if horizon == "decade":
        return "decad_in_year"
    if horizon == "month":
        return "month_in_year"
    return "pentad_in_year"
```

#### Step 1b: Retain `horizon_value` as `month_in_year` in `get_long_forecasts()`

In `get_long_forecasts()`, instead of dropping `horizon_value`, rename it:

```python
df.rename(columns={"horizon_value": "month_in_year"}, inplace=True)
df.drop(columns=["id", "horizon_type"], inplace=True, errors="ignore")
```

#### Step 1c: Fetch and merge skill metrics in `_get_data_monthly()`

Replace the hardcoded empty DataFrame with an API call and merge:

```python
def _get_data_monthly(station, all_stations, add_labels, i18n_models) -> dict:
    """Load data for monthly horizon — only long forecasts + daily hydrograph."""
    supported_modes = os.getenv(
        "ieasyhydroforecast_ml_long_term_supported_modes", ""
    ).split(",")

    forecasts_all = i18n_models(add_labels(get_long_forecasts(station, horizon_value=1)))
    forecast_stats = i18n_models(get_forecast_stats("month", station))

    # Merge skill metrics into forecasts (same pattern as pentad/decad in get_data)
    hin = "month_in_year"
    merge_keys = ["code", hin, "model_short"]
    can_merge = (
        not forecasts_all.empty
        and not forecast_stats.empty
        and all(k in forecasts_all.columns for k in merge_keys)
        and all(k in forecast_stats.columns for k in merge_keys)
    )
    if can_merge:
        forecasts_all = forecasts_all.merge(
            forecast_stats,
            on=merge_keys,
            how="left",
            suffixes=("", "_stats"),
        )

    data = {
        "hydrograph_day_all":   add_labels(get_hydrograph_day_all(station)),
        "hydrograph_pentad_all": pd.DataFrame(),
        "rain":                 get_rain(station),
        "temp":                 get_temp(station),
        "snow_data":            get_snow_data(station),
        "ml_forecast":          pd.DataFrame(),
        "linreg_predictor":     pd.DataFrame(),
        "forecasts_all":        forecasts_all,
        "forecast_stats":       forecast_stats,
        "long_forecasts_m0":    pd.DataFrame(),
    }
    if "month_0" in supported_modes:
        data["long_forecasts_m0"] = i18n_models(
            add_labels(get_long_forecasts(station, horizon_value=0))
        )
    return data
```

### Phase 2: Verify table rendering handles merged columns

**File**: `apps/forecast_dashboard/src/vizualization.py:2990-2996`

The existing code already handles the case where skill metric columns are present:

```python
for col in ('delta', 'sdivsigma', 'mae', 'accuracy'):
    if col not in forecast_table.columns:
        forecast_table[col] = np.nan
```

After the merge, these columns will come from the skill-metric data and the `if col not in` guard will skip them. **No changes needed here** — verify only.

## Dependency Graph

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1, "note": "Fix three gaps in db.py" },
    "P2": { "depends_on": ["P1"], "parallel_agents": 0, "note": "Verify vizualization.py — read-only check" }
  }
}
```

## Verification

1. `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` — all tests pass, zero skips
2. Run dashboard locally, select "month" horizon, station 15013 — skill metric columns should show values (if skill metrics exist in DB for that station/month)
3. Confirm pentad/decad horizons still work unchanged (no regression)

## Out of Scope

- Adding LR model forecasts to the monthly table (LR forecasts are short-term only)
- Creating monthly skill metrics if they don't exist in the database (pipeline concern, not dashboard)
- Refactoring the pentad/decad merge logic to share code with monthly (tech debt, separate issue)
