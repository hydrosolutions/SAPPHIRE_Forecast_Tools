# FD: Load skill metrics for monthly forecasts in dashboard

| Field       | Value                                                   |
|-------------|---------------------------------------------------------|
| **Module**  | `forecast_dashboard`                                    |
| **Priority**| Mid                                                     |
| **Status**  | Complete                                                |
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

This affects two downstream functions:
- `get_forecast_stats("month", station)` — renames `horizon_in_year` to wrong column name and deduplicates on it
- The merge in `_get_data_monthly()` — needs a consistent join key between forecasts and skill metrics

### Gap 2: `_get_data_monthly()` never fetches or merges skill metrics

**File**: `apps/forecast_dashboard/src/db.py:546`

```python
"forecast_stats":       pd.DataFrame(),
```

This is hardcoded empty. It should call `get_forecast_stats("month", station)` and then merge the result into `forecasts_all`, similar to how pentad/decad does it in `get_data()` (lines 510-525).

### Gap 3: `get_long_forecasts()` has no `month_in_year` column to join on

The `LongForecast` DB model (`sapphire/services/postprocessing/app/models.py:105-157`) does **not** have a `horizon_in_year` column — unlike the regular `Forecast` model. It stores the target period via `valid_from`/`valid_to` date columns instead. After `get_long_forecasts()` returns, the DataFrame has no month-number column that can serve as the merge key with skill metrics.

The month number must be **computed** from `valid_from` (e.g. `valid_from.month`).

Note: `horizon_value` (0 or 1) is the forecast lead time (month_0 vs month_1), **not** the month number.

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

This ensures `get_forecast_stats("month", station)` renames `horizon_in_year` → `month_in_year` in the skill-metric response.

#### Step 1b: Compute `month_in_year` in `get_long_forecasts()`

The `LongForecast` model has no `horizon_in_year` column. Derive the month number from `valid_from` after the existing renames (line ~477, after the drop).

`_read_data()` only auto-converts the `date` column to datetime (db.py:78-79). `valid_from` arrives as a string and needs explicit conversion:

```python
df.drop(columns=["id", "horizon_type", "horizon_value"], inplace=True, errors="ignore")
df["valid_from"] = pd.to_datetime(df["valid_from"])
df["month_in_year"] = df["valid_from"].dt.month
```

This gives `forecasts_all` a `month_in_year` column that matches the one `get_forecast_stats("month", station)` produces from the skill-metric API's `horizon_in_year` field.

Also add `"month_in_year"` and `"valid_from"` to the empty fallback DataFrame in `get_long_forecasts()` (the early return when the API returns no data):

```python
return pd.DataFrame(columns=[
    "code", "date", "Date", "year",
    "model_short", "model_long",
    "forecasted_discharge", "flag",
    "Q5", "Q25", "Q75", "Q95", "E[Q]",
    "valid_from", "month_in_year",
])
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

### Phase 2: Verify no fan-out risk and table rendering handles merged columns

**File**: `apps/forecast_dashboard/src/vizualization.py:2990-2996`

The existing code already handles the case where skill metric columns are present:

```python
for col in ('delta', 'sdivsigma', 'mae', 'accuracy'):
    if col not in forecast_table.columns:
        forecast_table[col] = np.nan
```

After the merge, these columns will come from the skill-metric data and the `if col not in` guard will skip them. **No changes needed here** — verify only.

**Fan-out risk (FD-003)**: `get_forecast_stats()` already deduplicates at line 438 via `drop_duplicates(subset=["code", hin, "model_short"], keep="last")`, keeping only the latest recalculation per key. The monthly merge reuses this function, so fan-out is not a concern.

**`create_skill_table` (vizualization.py:4507)**: This function has no `"month"` branch and would crash if called with non-empty monthly stats. However, it is called only once at startup (`_init_skill_table`, plot_manager.py:32) and the card is hidden for monthly (`set_forecast_cards_visibility`, plot_manager.py:58-64). The crash is unreachable. Fixing it is a separate feature — see Out of Scope.

## Dependency Graph

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1, "note": "Fix three gaps in db.py" },
    "P2": { "depends_on": ["P1"], "parallel_agents": 0, "note": "Verify vizualization.py — read-only check" }
  }
}
```

## Tests

**File to modify**: `apps/forecast_dashboard/tests/test_db.py`

All tests mock `requests.get` via `monkeypatch` to avoid live API calls. Tests use inline DataFrame/JSON construction following the existing conftest patterns.

### Test 1: `TestHorizonInYearCol::test_month`

Verify the new mapping returns `"month_in_year"`.

### Test 2: `TestGetLongForecasts::test_month_in_year_computed_from_valid_from`

Mock `requests.get` to return a long-forecast JSON with `valid_from: "2026-04-01"`. Assert:
- `month_in_year` column exists with value `4`
- `horizon_value` column does NOT exist (dropped)
- `forecasted_discharge` exists (renamed from `q`)

### Test 3: `TestGetLongForecasts::test_empty_api_response`

Mock `requests.get` to return `[]`. Assert the returned DataFrame is empty with expected fallback columns.

### Test 4: `TestGetForecastStats::test_month_horizon_renames_to_month_in_year`

Mock `requests.get` to return a skill-metric JSON with `horizon_type: "month"`, `horizon_in_year: 4`. Assert:
- `month_in_year` column exists with value `4`
- `pentad_in_year` column does NOT exist
- Skill columns (`delta`, `sdivsigma`, `mae`, `accuracy`) are present
- `date` column does NOT exist (dropped)

### Test 5: `TestGetForecastStats::test_month_deduplicates_keeping_latest`

Mock API to return 2 skill-metric rows for same (code, month_in_year=4, model=GBT) but different dates (2026-03-01 and 2026-03-15). Assert only the row with the latest date (2026-03-15) survives.

### Test 6: `TestGetDataMonthly::test_merges_skill_metrics_into_forecasts`

Integration test. Mock `requests.get` to dispatch by URL path:
- `/long-forecast/` → long-forecast fixture with `valid_from` in April
- `/skill-metric/` → skill-metric fixture with `horizon_in_year: 4`, model GBT
- All other endpoints → `[]`

Also mock `processing.add_labels_to_hydrograph` as `lambda df, stations: df` and `processing.internationalize_forecast_model_names` as `lambda fn, df, **kw: df` (must match their 2-arg signatures).

Call `get_data("month", "99001", all_stations_df)`. Assert:
- `data["forecasts_all"]` has `delta`, `sdivsigma`, `mae`, `accuracy` columns
- The `delta` value for model GBT matches the skill-metric fixture
- `data["forecast_stats"]` is not empty

### Test 7: `TestGetDataMonthly::test_no_skill_metrics_still_returns_forecasts`

Same as Test 6 but skill-metric endpoint returns `[]`. Assert:
- `forecasts_all` still has forecast data (no crash)
- Skill metric columns are **absent** from `forecasts_all` (e.g. `"delta" not in data["forecasts_all"].columns`). The merge is skipped when `forecast_stats` is empty, so these columns won't exist. (vizualization.py adds NaN later in the summary table via its guard at line 2994.)

## Verification

1. `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` — all tests pass, zero skips
2. Run dashboard locally, select "month" horizon, station 15013 — skill columns should show values if metrics exist in DB
3. Switch to pentad/decad — confirm no regression

## Out of Scope

- Adding LR model forecasts to the monthly table (LR forecasts are short-term only)
- Creating monthly skill metrics if they don't exist in the database (pipeline concern, not dashboard)
- Refactoring the pentad/decad merge logic to share code with monthly (tech debt, separate issue)
- Showing the skill table card for monthly (`create_skill_table` has no month branch; the card is hidden by `set_forecast_cards_visibility`; fixing requires both the function and UI wiring)
- Merging skill metrics into `long_forecasts_m0` (month_0 forecasts share the same per-target-month skill metrics as month_1; this is a design question)
