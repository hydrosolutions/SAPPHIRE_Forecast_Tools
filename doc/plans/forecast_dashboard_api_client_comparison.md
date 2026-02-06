# Forecast Dashboard API Client Comparison

## Overview

This document compares the current API usage in `forecast_dashboard/src/db.py` with the methods available in `sapphire-api-client`.

## Current Dashboard Implementation (db.py)

The dashboard uses raw `requests.get()` calls via a `read_data()` helper function:

```python
def read_data(service_type: str, data_type: str, params: dict = None):
    response = requests.get(
        f"http://localhost:8000/api/{service_type}/{data_type}/",
        params=params,
        timeout=30
    )
```

### API Calls Used

| Function | Endpoint | Parameters |
|----------|----------|------------|
| `get_hydrograph_day_all()` | `GET /api/preprocessing/hydrograph/` | horizon, code, start_date, end_date, limit |
| `get_hydrograph_pentad_all()` | `GET /api/preprocessing/hydrograph/` | horizon, code, start_date, end_date, limit |
| `get_rain()` | `GET /api/preprocessing/meteo/` | meteo_type, code, start_date, end_date, limit |
| `get_temp()` | `GET /api/preprocessing/meteo/` | meteo_type, code, start_date, end_date, limit |
| `get_snow_data()` | `GET /api/preprocessing/snow/` | snow_type, code, start_date, end_date, limit |
| `get_ml_forecast()` | `GET /api/postprocessing/forecast/` | horizon, code, start_date, end_date, limit |
| `get_linreg_predictor()` | `GET /api/postprocessing/lr-forecast/` | horizon, code, start_date, end_date, limit |
| `get_forecasts_all()` | `GET /api/postprocessing/forecast/` | horizon, code, start_date, end_date, **target**, limit |
| `get_forecasts_all()` | `GET /api/postprocessing/lr-forecast/` | horizon, code, start_date, end_date, limit |
| `get_forecast_stats()` | `GET /api/postprocessing/skill-metric/` | horizon, code, start_date, end_date, limit |

---

## sapphire-api-client Methods

### SapphirePreprocessingClient

| Method | Endpoint | Parameters |
|--------|----------|------------|
| `read_runoff()` | `/runoff/` | horizon, code, start_date, end_date, skip, limit |
| `read_hydrograph()` | `/hydrograph/` | horizon, code, start_date, end_date, skip, limit |
| `read_meteo()` | `/meteo/` | meteo_type, code, start_date, end_date, skip, limit |
| `read_snow()` | `/snow/` | snow_type, code, start_date, end_date, skip, limit |

### SapphirePostprocessingClient

| Method | Endpoint | Parameters |
|--------|----------|------------|
| `read_forecasts()` | `/forecast/` | horizon, code, start_date, end_date, skip, limit |
| `read_lr_forecasts()` | `/lr-forecast/` | horizon, code, start_date, end_date, skip, limit |
| `read_skill_metrics()` | `/skill-metric/` | horizon, code, **model**, skip, limit |

---

## Consistency Analysis

### ✅ Fully Consistent

| Data Type | Dashboard Params | Client Params | Status |
|-----------|------------------|---------------|--------|
| Hydrograph | horizon, code, start_date, end_date, limit | horizon, code, start_date, end_date, skip, limit | ✅ Match |
| Meteo | meteo_type, code, start_date, end_date, limit | meteo_type, code, start_date, end_date, skip, limit | ✅ Match |
| Snow | snow_type, code, start_date, end_date, limit | snow_type, code, start_date, end_date, skip, limit | ✅ Match |
| LR Forecasts | horizon, code, start_date, end_date, limit | horizon, code, start_date, end_date, skip, limit | ✅ Match |

### ⚠️ Partial Mismatch

#### 1. Forecasts (`read_forecasts`)

**Dashboard uses but client lacks:**
- `target` - Used in `get_forecasts_all()` with `target: "null"` to filter forecasts

**API endpoint supports (from main.py):**
```python
def read_forecast(
    horizon: str = None,
    code: str = None,
    model: str = None,           # ← Missing in client
    start_date: str = None,
    end_date: str = None,
    start_target: str = None,    # ← Missing in client
    end_target: str = None,      # ← Missing in client
    target: str = None,          # ← Missing in client
    skip: int = 0,
    limit: int = 100,
)
```

**Missing in client:**
- `model` (filter by model_type)
- `target` (filter by target date)
- `start_target`, `end_target` (target date range)

#### 2. Skill Metrics (`read_skill_metrics`)

**Dashboard uses but client lacks:**
- `start_date`, `end_date` - Used for date filtering

**API endpoint supports (from main.py):**
```python
def read_skill_metric(
    horizon: str = None,
    code: str = None,
    model: str = None,
    start_date: str = None,      # ← Missing in client
    end_date: str = None,        # ← Missing in client
    skip: int = 0,
    limit: int = 100,
)
```

**Missing in client:**
- `start_date`, `end_date` (date range filtering)

---

## Features in sapphire-api-client (Advantages)

| Feature | Description |
|---------|-------------|
| **Automatic retry** | Exponential backoff on connection errors and 502/503/504 status codes |
| **Batch posting** | Automatic batching for bulk writes (default 1000 records/batch) |
| **Health checks** | `health_check()` and `readiness_check()` methods |
| **Authentication** | Optional Bearer token support for future auth requirements |
| **Error handling** | `SapphireAPIError` with status code and response details |
| **Type hints** | Full type annotations for better IDE support |

---

## Recommendations

### 1. Update sapphire-api-client

Add missing parameters to `read_forecasts()`:
```python
def read_forecasts(
    self,
    horizon: Optional[str] = None,
    code: Optional[str] = None,
    model: Optional[str] = None,           # ADD
    start_date: Optional[Union[str, date]] = None,
    end_date: Optional[Union[str, date]] = None,
    target: Optional[Union[str, date]] = None,  # ADD
    start_target: Optional[Union[str, date]] = None,  # ADD
    end_target: Optional[Union[str, date]] = None,  # ADD
    skip: int = 0,
    limit: int = 100,
) -> pd.DataFrame:
```

Add missing parameters to `read_skill_metrics()`:
```python
def read_skill_metrics(
    self,
    horizon: Optional[str] = None,
    code: Optional[str] = None,
    model: Optional[str] = None,
    start_date: Optional[Union[str, date]] = None,  # ADD
    end_date: Optional[Union[str, date]] = None,    # ADD
    skip: int = 0,
    limit: int = 100,
) -> pd.DataFrame:
```

### 2. Migrate Dashboard to Use Client

Once client is updated, replace `db.py` functions with client calls:

```python
# Before (db.py)
def get_hydrograph_day_all(station):
    params = {"horizon": "day", "code": station, ...}
    hydrograph = read_data("preprocessing", "hydrograph", params)
    ...

# After (using client)
from sapphire_api_client import SapphirePreprocessingClient

client = SapphirePreprocessingClient(base_url="http://localhost:8000")

def get_hydrograph_day_all(station):
    hydrograph = client.read_hydrograph(
        horizon="day",
        code=station,
        start_date="2026-01-01",
        end_date="2026-12-31",
        limit=1000
    )
    ...
```

### 3. Benefits of Migration

1. **Consistency** - All SAPPHIRE modules use the same client
2. **Reliability** - Automatic retry logic handles transient failures
3. **Maintainability** - Single source of truth for API interaction
4. **Future-proofing** - Authentication support already built in
5. **Type safety** - Better IDE support and error detection

---

## Summary

| Aspect | Status |
|--------|--------|
| Preprocessing methods | ✅ Fully consistent |
| Postprocessing forecasts | ⚠️ Missing: model, target, start_target, end_target |
| Postprocessing skill metrics | ⚠️ Missing: start_date, end_date |
| Additional client features | ✅ Retry, batching, health checks, auth |

**Action Required:** Update `sapphire-api-client` to add missing forecast and skill metric parameters before migrating the dashboard.
