# LT: Add climatological quantile bounds for GBT forecasts

| Field       | Value                                                   |
|-------------|---------------------------------------------------------|
| **Module**  | `long_term_forecasting`                                 |
| **Priority**| Mid                                                     |
| **Status**  | Review — P1+P2 implemented 2026-04-14, 108 tests pass (10 new), awaiting server verification (P3) |
| **Branch**  | `fix_fd_monthly_skill_metrics`                          |

## Problem

GBT forecasts have no quantile bounds (q25, q75 = NULL) in the database. The dashboard shows no upper/lower bounds for any GBT-family model (GBT, SM_GBT, SM_GBT_LR, SM_GBT_Norm, MC_ALD). LR-family models produce quantiles natively via Bayesian Ridge (`LINEAR_REGRESSION.py:314-370`), but GBT uses point-prediction models (XGBoost, LightGBM, CatBoost) with no built-in uncertainty estimation.

The dashboard only uses Q25 and Q75 for monthly forecast display:
- Tabulator: `fc_lower = Q25`, `fc_upper = Q75` (`vizualization.py:2990-2993`)
- Note: `vizualization.py:2381-2392` handles Q25/Q75 for pentad/decad hydrographs, NOT monthly. Monthly forecasts have no hydrograph plot — only the Tabulator table.

## Root Cause

- `SciRegressor.predict_operational()` (in external `lt-forecasting` package) outputs only `Q_GBT`, `Q_GBT_xgb`, `Q_GBT_lgbm`, `Q_GBT_catboost` — no quantile columns.
- `prepare_long_forecast_records()` (`lt_utils.py:309-318`) maps Q25→q25, Q75→q75 etc. via `static_column_mapping`. If the columns existed in the forecast DataFrame, they would flow to the API automatically.
- `adjust_forecast_to_calendar_month()` (`post_process_lt_forecast.py:495-683`) classifies Q25/Q75 as `quantile_cols` via `re.compile(r'^Q(\d+)$')` and applies ratio-based adjustment. When Q50 is absent, `delta = np.zeros(n_rows)` (line 635) — quantile columns get ratio-adjusted independently. This matches the LR long-term convention (Bayesian Ridge also omits Q50).

## Approach: Climatological std from observed discharge

### Why not hindcast residuals (original plan)

The original plan proposed computing delta from GBT hindcast residuals via the postprocessing API. **Critical blocker**: `q_obs` is NULL for every row in the `long_forecasts` table — neither `prepare_long_forecast_records()` nor the data migrator writes observed discharge to long-forecast records. The plan's step "filter to rows where q_obs is not null" would return zero rows.

### Climatological approach

Use the standard deviation of historical observed monthly mean discharge — the same approach used across the codebase:
- Short-term LR: `0.674 * np.std(discharge_avg)` per (station, pentad) — `forecast_library.py:1805`
- Long-term monthly postprocessing: `0.674 * std(discharge_avg)` per (code, month_in_year) — `data_reader.py:975`
- Long-term quarterly postprocessing: `0.674 * std(discharge_avg)` per (code, quarter) — `aggregation.py:137`

### Delta computation

For each `(code, month)` group, using `temporal_data` already loaded in memory at `run_forecast.py:300`:
1. Derive `year`, `month` from `temporal_data["date"]`
2. Aggregate to monthly means per `(code, year, month)` — require ≥ 50% non-missing days (matching `calculate_lt_statistics_calendar_month()` at `post_process_lt_forecast.py:332-406`)
3. Compute `std_monthly = std(monthly_mean_discharge)` across years per `(code, month)`, using pandas `.std()` (ddof=1, sample std) to match the long-term postprocessing patterns in `data_reader.py:975` and `aggregation.py:137`. Note: `forecast_library.py:1805` uses `np.std()` (ddof=0, population std) — a pre-existing inconsistency in the codebase; we follow the long-term convention.
4. Require `n >= 3` years of data per group — skip groups with insufficient data
5. Define quantile offsets using normal approximation:
   - `Q25 = Q_{model_name} - 0.674 * std_monthly`
   - `Q75 = Q_{model_name} + 0.674 * std_monthly`
6. Clip all quantiles to `>= 0` (discharge cannot be negative)

**Note on approach**: This uses climatological spread (variability of observed discharge across years), NOT model-specific prediction error. The bounds reflect how variable discharge is for a given station and month, regardless of model accuracy. This is the same convention used for short-term LR and long-term monthly postprocessing — a well-tested pattern in this codebase. Note that `calculate_lt_statistics_calendar_month()` in `post_process_lt_forecast.py` returns raw std (no `0.674 *` factor) for a different purpose (Student's t confidence intervals). The `0.674 *` factor here matches the postprocessing patterns in `data_reader.py` and `aggregation.py`.

**Tradeoff vs. residual approach**: Climatological bounds are wider when discharge is naturally variable and narrower when it is stable — but they do not narrow when the model is more accurate. This is acceptable for dashboard display and sharpness_50 computation. A future enhancement could switch to residual-based bounds once `q_obs` is backfilled in the database.

### No Q50 column

Following the LR long-term convention: Bayesian Ridge produces Q5, Q10, Q25, Q75, Q90, Q95 but NOT Q50 (`LINEAR_REGRESSION.py:44-45`). The main prediction `Q_{model_name}` serves as the central estimate. In `adjust_forecast_to_calendar_month()`, absent Q50 means `delta = 0` — quantile columns get ratio-adjusted independently, which is the existing behavior for all long-term models.

### Only Q25/Q75

Only Q25 and Q75 are needed:
- Dashboard tabulator and plot only render Q25/Q75 for monthly forecasts
- Skill metric `sharpness_50` (Q75 - Q25) will work
- CRPS requires all 7 quantiles and will remain NaN for GBT (already the case)
- Sharpness_90 (Q95 - Q05) will remain NaN for GBT (already the case)

### Where to insert

**File**: `apps/long_term_forecasting/run_forecast.py`
**Location**: After `model_instance.predict_operational(today)` returns and flag is set (line 288) and before `post_process_lt_forecast()` is called (line 298).

At this point, available in scope:
- `forecast` — DataFrame with `Q_{model_name}` column, `code`, `date`, `valid_from`, `valid_to`, `flag` (already rounded to 2 decimals at line 274)
- `temporal_data` — daily DataFrame with `date`, `code`, `discharge` columns plus ECMWF forcing columns and optional snow columns (full historical record, already loaded and extended)
- `model_type` — `"sciregressor"` for GBT models, `"linear_regression"` for LR
- `model_name` — e.g. `"GBT"`, `"SM_GBT"`, `"LR_Base"`
- `today` — forecast issue date as `pd.Timestamp`

**Note**: When `can_be_run = False`, `forecast` is an empty DataFrame (line 292). The function must handle this gracefully.

**Guard condition**: Only run for `model_type == "sciregressor"` AND monthly mode (`calendar_month_adjustment == True`) — this covers all GBT-family models (GBT, SM_GBT, SM_GBT_LR, SM_GBT_Norm) in monthly mode and excludes LR-family models that already produce quantiles natively. Quarterly and seasonal modes are excluded because the function computes monthly climatological std, which is not appropriate for multi-month aggregate forecasts (quarterly/seasonal forecasts predict 90-day or 183-day averages and would need quarterly/seasonal variability instead).

**MC_ALD exception**: `UncertaintyMixture` models (model_type `"UncertaintyMixture"`) already produce Q25/Q75 via their own mechanism. The guard `model_type == "sciregressor"` correctly excludes them.

Once Q25/Q75 columns exist in the forecast DataFrame:
- `infer_q_columns()` picks them up — `Q25[1]='2'` is a digit, `Q75[1]='7'` is a digit (confirmed: `lt_utils.py:122-131`)
- `adjust_forecast_to_calendar_month()` classifies them as `quantile_cols` via regex `^Q(\d+)$` and ratio-adjusts them (with delta=0 since no Q50)
- `columns_to_retain = LT_FORECAST_BASE_COLUMNS + q_columns` retains them in final output
- `prepare_long_forecast_records()` maps `Q25→q25`, `Q75→q75` via `static_column_mapping`
- **No changes needed in post-processing, API writing, or dashboard code**

### Which models need this

All GBT-family models that use `SciRegressor` (model_type = `"sciregressor"`):
- GBT
- SM_GBT, SM_GBT_LR, SM_GBT_Norm

LR-family models (model_type = `"linear_regression"`) produce quantiles via Bayesian Ridge when `lr_type == "bayesian_ridge"`. Other LR subtypes (linear, ridge, lasso, elasticnet, pca_lr) return no quantiles — they will continue to have NULL Q25/Q75. The guard `model_type == "sciregressor"` correctly excludes all LR models regardless of subtype. UncertaintyMixture models (MC_ALD) already produce Q25/Q75 natively. Neither needs this fix.

## Implementation Plan

### Phase 1: Add quantile synthesis function

**File to modify**: `apps/long_term_forecasting/run_forecast.py`

Add a module-level helper function `_add_climatological_quantile_bounds(forecast, temporal_data, model_name, today)`:

1. Extract the target month from the forecast's `valid_from` column (the month being forecast)
2. Filter `temporal_data` to rows where `discharge` is not NaN
3. Derive `year`, `month` from `temporal_data["date"]`, derive `days_in_month`
4. Aggregate to monthly means per `(code, year, month)` — filter to months with ≥ 50% non-missing days
5. Exclude the forecast year (leave-one-out, matching `calculate_lt_statistics_calendar_month`)
6. Compute `std_monthly` per `(code, month)` from the yearly monthly means, also compute `n` (number of years)
7. Filter to groups with `n >= 3`
8. Merge `std_monthly` into `forecast` on `(code)` — the target month is already known from step 1
9. Compute `Q25 = Q_{model_name} - 0.674 * std_monthly`, `Q75 = Q_{model_name} + 0.674 * std_monthly`
10. Clip `Q25` and `Q75` to `>= 0`
11. Drop the temporary `std_monthly` column
12. Return modified forecast — groups with insufficient data will have Q25/Q75 as NaN (no bounds, same as today)

**Call site**: In `run_single_model()`, after line 288 (`success = True`) and before line 298 (`post_process_lt_forecast`):

```python
is_monthly = forecast_configs.get_calendar_month_adjustment()
if model_type == "sciregressor" and success and is_monthly:
    forecast = _add_climatological_quantile_bounds(
        forecast=forecast,
        temporal_data=temporal_data,
        model_name=model_name,
        today=today,
    )
```

**Constraints**:
- Do NOT change any existing function signatures
- Do NOT modify `post_process_lt_forecast.py`, `lt_utils.py`, or any other file
- The function must be purely additive — it adds Q25/Q75 columns if they don't exist
- If forecast is empty (0 rows), return it unchanged immediately
- If any step fails (empty data, no valid groups), return the forecast unchanged (no bounds = same as today)
- Use pandas `.std()` (ddof=1, sample std) to match the long-term postprocessing convention
- Do NOT round Q25/Q75 in this function — post-processing already rounds all numeric columns to 2 decimals at line 304-305
- Use `logger` for warnings when data is insufficient

### Phase 2: Write tests

**File to create**: `apps/long_term_forecasting/tests/test_quantile_bounds.py`

Test that:
1. **GBT forecast gets Q25/Q75**: Given a sciregressor forecast DataFrame and historical discharge, Q25/Q75 columns are added
2. **Quantile ordering**: `Q25 <= Q_{model_name} <= Q75` for all rows
3. **Non-negativity**: All Q25, Q75 values >= 0
4. **LR models unaffected**: When model_type is `"linear_regression"`, the function is not called (or returns input unchanged)
5. **Quarterly/seasonal excluded**: When `calendar_month_adjustment` is `False`, the function is not called — quarterly/seasonal GBT forecasts continue to have NULL Q25/Q75
6. **Insufficient data fallback**: When a (code, month) group has < 3 years, Q25/Q75 are NaN for that group
6. **Leave-one-out**: The forecast year's observations are excluded from std computation
7. **50% coverage filter**: Months with < 50% non-missing days are excluded from std computation
8. **Empty temporal_data**: Function returns forecast unchanged, no crash
9. **End-to-end column survival**: Q25/Q75 survive through `infer_q_columns()` and `prepare_long_forecast_records()` column mapping

### Phase 3: Verify on server

1. Run long-term forecast for GBT: `lt_forecast_mode=month_1 uv run python dev_code/simulate_forecasts.py --years 2024 --model GBT --num_months 2`
2. Check DB: `SELECT q, q25, q75 FROM long_forecasts WHERE model_type='GBT' AND horizon_value=1 LIMIT 10` — q25/q75 non-NULL
3. Dashboard: select monthly horizon, station 15189 — GBT now shows bounds
4. Verify LR models unchanged: `SELECT q, q25, q75 FROM long_forecasts WHERE model_type='LR_Base' AND horizon_value=1 LIMIT 5` — values identical to before

## Dependency Graph

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1, "note": "Add _add_climatological_quantile_bounds() in run_forecast.py" },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1, "note": "Write tests in test_quantile_bounds.py" },
    "P3": { "depends_on": ["P1", "P2"], "parallel_agents": 0, "note": "Verify on server (manual)" }
  }
}
```

## Risk Assessment

| Risk | Mitigation | Verified |
|------|-----------|----------|
| Breaks existing LR quantiles | Guard: `model_type == "sciregressor"` — LR path untouched. Only Bayesian Ridge LR produces quantiles; other LR subtypes already have NULL Q25/Q75 — unaffected either way | Yes |
| Breaks existing GBT forecasts | Function is purely additive — adds columns, never modifies existing ones | Yes |
| Bad quantile values from short records | Guard: `n >= 3` years required, otherwise NaN (same as today) | Yes |
| Post-processing mishandles new columns | `infer_q_columns()` picks up Q25/Q75 (digit check confirmed); `adjust_forecast_to_calendar_month()` ratio-adjusts them with delta=0 (no Q50); `columns_to_retain` includes them | Yes |
| API silently drops Q25/Q75 | `LongForecastBase` schema has `q25: float | None` and `q75: float | None`; DB model has both columns; `sapphire_api_client` includes them in `quantile_cols` | Yes |
| Dashboard rendering issues | Monthly path uses only Tabulator (not hydrograph plot). Tabulator uses `.get('Q25', np.nan)` — transition from NaN to real values shows bounds instead of empty cells (desired) | Yes |
| Quarterly/seasonal GBT forecasts get wrong bounds | Guard: `is_monthly = forecast_configs.get_calendar_month_adjustment()` — function only runs in monthly mode. Monthly climatological std is not appropriate for multi-month aggregate forecasts | Yes |
| Rounding inconsistency | Post-processing already rounds all numeric columns to 2 decimals at line 304-305; no separate rounding needed in the function | Yes |
| Empty forecast DataFrame | Early return when `forecast` has 0 rows | Mitigated |
| Performance impact | Negligible: `temporal_data` is already in memory; groupby + std on a few hundred rows per station is sub-millisecond | Yes |

## Changes Summary

| File | Change |
|------|--------|
| `apps/long_term_forecasting/run_forecast.py` | Add `_add_climatological_quantile_bounds()` function + 4-line call site |
| `apps/long_term_forecasting/tests/test_quantile_bounds.py` | New test file |
| No other files modified | |

## Out of Scope

- **Quarterly/seasonal GBT quantile bounds**: This plan only covers monthly mode (`calendar_month_adjustment=True`). Quarterly and seasonal GBT forecasts predict 90-day or 183-day aggregates — their climatological bounds would need quarterly/seasonal variability (matching the pattern in `aggregation.py:137`), not monthly std. Worth adding as a follow-up using the same approach but with period-appropriate grouping.
- **Residual-based quantiles**: Computing bounds from model-specific prediction errors (q_obs - q) requires backfilling `q_obs` in the `long_forecasts` table first. Worth pursuing as a follow-up — the bounds would be tighter for accurate models.
- **Full quantile set (Q5-Q95)**: Only Q25/Q75 are needed for dashboard display and sharpness_50.
- **Proper quantile regression for GBT**: Using quantile loss functions in XGBoost/LightGBM would produce statistically better uncertainty estimates but requires retraining all GBT models in the upstream `lt-forecasting` package.
- **Conformal prediction intervals**: A more sophisticated alternative that provides calibrated intervals. Worth exploring in a future iteration.
- **Backfilling `q_obs`**: Populating observed discharge in existing long-forecast records to enable residual-based quantiles later.
