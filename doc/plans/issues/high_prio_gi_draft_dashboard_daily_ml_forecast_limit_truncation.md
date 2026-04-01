# GitHub Issue: FD-005

**Title**: `fix(forecast_dashboard): Daily ML forecasts not displayed due to API limit truncation`

**Labels**: `bug`, `forecast_dashboard`, `high-priority`

**Assignee**: @maxatp

**Status**: Draft

---

## Summary

The forecast dashboard does not display any daily ML forecasts. The `get_ml_forecast()` function fetches data with `limit=1000` over a 13+ month date range, but a single station can have ~3,900+ daily forecast rows in that period. The API returns rows oldest-first, so the 1000-row limit truncates at approximately Dec 31, 2025 — the latest forecast data (March 2026) is never fetched.

## Root Cause

In `src/db.py:get_ml_forecast()` (lines 255-262):

```python
df = _read_data("postprocessing", "forecast", {
    "horizon": "day",
    "code": code,
    "start_date": f"{PREVIOUS_YEAR}-12-01",
    "end_date": f"{CURRENT_YEAR}-12-31",
    "limit": 1000,
})
```

The query spans ~13 months. Each station has ~33 rows per forecast_date (3 models x 11 target days). Over 120 days that's ~3,900 rows — far exceeding `limit=1000`.

The API (`crud.py:get_forecast()`, line 97) returns results ordered by primary key (oldest first) and applies the limit via `query.offset(skip).limit(limit)`. The first 1000 rows cover only December 2025.

After fetching, line 284 filters to the latest forecast_date:

```python
df = df[df["forecast_date"] == df["forecast_date"].max()]
```

But the "latest" in the truncated set is ~Dec 31, 2025. When the dashboard tries to display forecasts for the current date (April 2026), nothing matches.

### Verification

```bash
# Station 15013 has 3,933 rows in the full date range
curl "http://localhost:8000/api/postprocessing/forecast/?horizon=day&code=15013&start_date=2025-12-01&end_date=2026-12-31&limit=5000" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))"
# → 3933

# With limit=1000, only Dec 2025 is returned
curl "http://localhost:8000/api/postprocessing/forecast/?horizon=day&code=15013&start_date=2025-12-01&end_date=2026-12-31&limit=1000" | python3 -c "
import sys,json; data=json.load(sys.stdin)
dates=sorted(set(r['date'] for r in data))
print(f'Range: {dates[0]} to {dates[-1]}')
"
# → Range: 2025-12-01 to 2025-12-31
```

## Proposed Fix

Since only the latest forecast_date is needed (line 284 discards all others), narrow the query window to the last 14 days instead of 13+ months. For a single station this yields ~462 rows (14 days x 33 rows/day) — well within the 1000-row limit.

```python
from datetime import datetime, timedelta

@_timed
def get_ml_forecast(horizon, station) -> pd.DataFrame:
    code = _resolve_station(station)
    recent_start = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
    df = _read_data("postprocessing", "forecast", {
        "horizon": "day",
        "code": code,
        "start_date": recent_start,
        "end_date": f"{CURRENT_YEAR}-12-31",
        "limit": 1000,
    })
    # ... rest unchanged
```

### Why 14 days is safe

- The operational pipeline runs daily, so the latest forecast_date is at most 1 day old
- 14 days provides generous buffer for pipeline outages or maintenance windows
- 14 days x 3 models x 11 targets = ~462 rows per station — well under `limit=1000`

### Alternative considered

Increasing `limit` to 5000+ would also work but fetches ~4x more data than needed, slowing dashboard load. Narrowing the window is both faster and more robust.

## Tasks

- [ ] Change `start_date` in `get_ml_forecast()` from `f"{PREVIOUS_YEAR}-12-01"` to a 14-day lookback (`src/db.py:260`)
- [ ] Verify locally: run `panel serve` and confirm daily ML forecasts appear on the hydrograph plot
- [ ] Run tests: `SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard`
