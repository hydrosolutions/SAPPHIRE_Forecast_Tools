# LR-005: LR hindcast NaN API write skip has misleading log message

**Status**: Draft — Issue A (config restriction) resolved by removing restrict file. Issue C resolved as consequence. Issue B (misleading NaN log) remains.
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

## Evidence

### Pipeline log (2026-03-16, LR hindcast PENTAD)

**Station restriction:**
```
Station selection for pentadal forecasting restricted to:
['15013', '15016', '15034'].
To remove restriction set ieasyforecast_restrict_stations_file to null.
3 station(s) selected for pentadal forecasting
```

**Config file:**
```json
// kyg_data_forecast_tools/config/config_development_restrict_station_selection.json
{"stationsID": ["15013", "15016", "15034"]}
```

**Hindcast detected 61 gauges but only processed 3:**
```
Gauge 16059: last forecast 2026-03-05   → start from 2026-03-06  [NOT PROCESSED]
Gauge 15189: last forecast 2026-03-05   → start from 2026-03-06  [NOT PROCESSED]
Gauge 15013: last forecast 2026-03-10   → start from 2026-03-11  [PROCESSED]
Gauge 15016: last forecast 2026-03-10   → start from 2026-03-11  [PROCESSED]
Gauge 15034: last forecast 2026-03-10   → start from 2026-03-11  [PROCESSED]
```

**March 10 iteration (success):**
```
Forecasts calculated: [['15034', '2.92'], ['15016', '3.81'], ['15013', '2.82']]
SAPPHIRE API: Successfully wrote 3 LR forecast records (pentad)
```

**March 15 iteration (NaN + API write skipped):**
```
Forecasts calculated: [['15034', 'nan'], ['15016', 'nan'], ['15013', 'nan']]
Skipping LR pentad write: no data for forecast year 2026
  (last_line date_max=2025-03-15 00:00:00). Daily discharge data may be missing.
```

The predictor date was 2026-03-15 but discharge_sum for March 15 was NaN
(data not yet available from the HF database).

### DB verification (sites 16059, 15189)

```sql
SELECT code, horizon_type, MAX(date), COUNT(*)
FROM lr_forecasts WHERE code IN ('16059', '15189')
GROUP BY code, horizon_type;

-- Results:
-- 15189 | PENTAD | 2026-03-05 | 74
-- 15189 | DECADE | 2026-02-28 | 37
-- 16059 | PENTAD | 2026-03-05 | 73
-- 16059 | DECADE | 2026-02-28 | 37
```

Both stations are missing March 10 and March 15 pentad forecasts because
they were excluded by the restrict file.

---

## Three Distinct Issues

### Issue A: Development restrict file limits LR to 3 stations (Config)

**RESOLVED** (2026-03-16): The restrict file `config_development_restrict_station_selection.json` was removed. LR will now process all stations in `config_station_selection.json`.

**Not a code bug.** The `.env_develop_kghm` config uses
`ieasyforecast_restrict_stations_file=config_development_restrict_station_selection.json`
which limits pentad forecasting to 3 stations. On the production server, this
file either contains all stations or `ieasyforecast_restrict_stations_file=null`.

**Action**: No code change needed. For local development testing with all
stations, either:
1. Set `ieasyforecast_restrict_stations_file=null` in `.env_develop_kghm`
2. Add all station codes to the restrict file

### Issue B: LR API write skipped when all forecasts are NaN (Code gap)

When all forecasted_discharge values are NaN, the `last_line` (tail of the
output DataFrame) has `date_max` from the previous year (2025-03-15 instead
of 2026-03-15). The API write check then skips with "no data for forecast
year 2026".

**Root cause**: The LR API write path (`write_linreg_pentad` in
`forecast_library.py`) filters to `years >= current_year - 1` and checks
if the latest row has the current year. When all forecasts are NaN, the
row for the current year exists in the CSV but has NaN values, so it gets
filtered out.

**Impact**: Even when future pipeline runs have valid data, the skipped
dates won't be retried unless the hindcast catches up again. The CSV has
the data (399 rows including historical), but the API doesn't.

**Severity**: Low in this specific case (NaN means no valid forecast to
write), but the logic should distinguish between "no valid data" and
"data exists but is NaN" for proper logging.

### Issue C: Hindcast detects ALL gauge dates but processes only restricted set (Inefficiency)

**RESOLVED** (2026-03-16): With Issue A resolved, the hindcast will only detect and process the same set of stations, eliminating the mismatch.

`get_hindcast_start_date_from_output()` reads the CSV to detect per-gauge
last forecast dates for ALL 61 gauges, including the 58 that will be
filtered out by `get_pentadal_forecast_sites()`. The "second earliest"
start date heuristic (line 515) then picks the wrong global start date.

**Impact**: Misleading logs — all 61 gauges appear as needing backfill,
but only 3 are processed. The global start date is based on all 61 gauges'
dates rather than just the 3 that will actually run.

**Fix**: Pass the restricted site list to `get_hindcast_start_date_from_output()`
so it only detects dates for stations that will be processed.

---

## Implementation Plan

### Phase 1: Improve NaN forecast logging (Issue B)

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

### Phase 3: Run tests

```bash
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh linear_regression
```

---

## Dependency Graph

```json
{
  "phases": {
    "1": {
      "title": "Improve NaN forecast logging in LR API write",
      "file": "apps/iEasyHydroForecast/forecast_library.py",
      "depends_on": []
    },
    "2": {
      "title": "Tests + run LR test suite",
      "file": "apps/linear_regression/tests/",
      "depends_on": ["1"]
    }
  },
  "execution_groups": [
    {
      "group": 1,
      "parallel": false,
      "agents": [
        {"id": "agent_lr_logging", "phases": ["1"]},
        {"id": "agent_tests", "phases": ["2"]}
      ]
    }
  ]
}
```
