# LR-005: LR hindcast NaN API write skip has misleading log message

**Status**: Archived — Issues A and C resolved (2026-03-16). Issue B (misleading NaN log) remains but is low priority.
**Module**: linear_regression
**Priority**: Low
**Labels**: `improvement`, `api-integration`, `hindcast`

---

## Summary

The LR hindcast mode produces NaN forecasts when predictor (runoff) data is not
yet available for the forecast period. When this happens, the API write is skipped
with a misleading log message: "Skipping LR pentad write: no data for forecast
year 2026 (last_line date_max=2025-03-15)". The message should clearly indicate
that the forecasts are NaN due to missing predictor data, not that there is "no data".

The original config restriction (Issue A) and hindcast detection scope (Issue C)
have been resolved by removing `config_development_restrict_station_selection.json`.

## Investigation (2026-03-24)

### DB verification — no missing LR forecasts for KGH

Queried the postprocessing API (`/lr-forecast/` endpoint on port 8003):

- **61 stations** have LR forecasts current through **2026-03-20** (last pentad boundary before today 2026-03-24). This is correct.
- **Station 10001** is behind (stopped at 2026-03-10) — this is a Uzbek hydromet station, not needed for KGH. Not a bug.
- ML models (TFT/TiDE/TSMixer) are at 2026-03-24 (daily cadence), LR is per-pentad — the 4-day gap is expected.
- EM (Ensemble Mean) max date matches LR (2026-03-20), which is expected since EM depends on LR.
- Several stations (15020, 15194, 15217) have fewer records because they were added to the pipeline recently, not because of gaps.

### Config verification

The restrict file was disabled by renaming to `_config_development_restrict_station_selection.json` (underscore prefix). The env file still references the old name without underscore → file not found at runtime → all 64 stations in `config_station_selection.json` are processed. Issues A and C are confirmed resolved.

### Remaining Issue B: misleading NaN log

The guard at `forecast_library.py:3701-3713` checks whether `last_line["date"].dt.year == forecast_date.year`. When discharge observations end before the current year, the year check fails and the function returns early with "no data for forecast year". The message is misleading — it should distinguish between "no rows for current year" and "rows exist but forecasts are NaN". Low severity since it doesn't cause data loss (NaN forecasts aren't useful anyway).

---

## Three Distinct Issues

### Issue A: Development restrict file limits LR to 3 stations (Config)

**RESOLVED** (2026-03-16): The restrict file `config_development_restrict_station_selection.json` was renamed with underscore prefix. LR now processes all stations in `config_station_selection.json`.

### Issue B: LR API write skipped when all forecasts are NaN (Code gap)

**OPEN — Low priority.** When all forecasted_discharge values are NaN, the `last_line`
(tail of the output DataFrame) has `date_max` from the previous year. The API write
check then skips with "no data for forecast year".

**Root cause**: `forecast_library.py:3701-3713` — the year-guard checks date year, not
forecast value validity.

**Impact**: Low. NaN forecasts aren't useful, so skipping the write is correct behavior.
Only the log message is misleading.

### Issue C: Hindcast detects ALL gauge dates but processes only restricted set (Inefficiency)

**RESOLVED** (2026-03-16): With Issue A resolved, hindcast detects and processes the same set of stations.

---

## Implementation Plan (Issue B only)

### Phase 1: Improve NaN forecast logging

**File**: `apps/iEasyHydroForecast/forecast_library.py`

In the LR API write path, distinguish between "no rows for current year"
and "rows exist but all values are NaN":

```python
# Replace the "Skipping LR pentad write" warning:
current_year_rows = last_line[last_line["date"].dt.year == current_year]
if current_year_rows.empty:
    logger.warning(
        "Skipping LR %s write: no rows for forecast year %d "
        "(last_line date_max=%s). Possible data lag.",
        horizon, current_year, last_line["date"].max()
    )
elif current_year_rows["forecasted_discharge"].isna().all():
    logger.warning(
        "Skipping LR %s API write: all %d forecasts for year %d are NaN. "
        "Predictor data may be missing for the forecast period.",
        horizon, len(current_year_rows), current_year
    )
```

### Phase 2: Tests

Add tests to `apps/linear_regression/tests/`:

| # | Test | Asserts |
|---|------|---------|
| 1 | LR API write path logs correctly for NaN forecasts | Warning mentions NaN specifically |
| 2 | LR API write path succeeds for valid forecasts | API records written |
