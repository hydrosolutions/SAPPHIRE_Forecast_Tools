---
name: ieasyhydro-sdk
description: "Expert guide for the ieasyhydro-python-sdk library for connecting to iEasyHydro High Frequency (HF) database. Use when: (1) Making API requests to iEasyHydro HF, (2) Retrieving discharge/meteorological data, (3) Debugging 422 or other API errors, (4) Formatting filters for get_data_values_for_site(), (5) Working with code in preprocessing_runoff that fetches from iEasyHydro HF, (6) Understanding response structure from the SDK. Triggers: ieasyhydro, iEasyHydro, SDK, HF database, WDDA, WDD, discharge data retrieval, site_codes filter."
---

# iEasyHydro Python SDK Guide

SDK repository: https://github.com/hydrosolutions/ieasyhydro-python-sdk

## Installation

```bash
pip install git+https://github.com/hydrosolutions/ieasyhydro-python-sdk
```

## Configuration & Initialization

**Environment variables:**
```dotenv
IEASYHYDROHF_HOST=https://hf.ieasyhydro.org/api/v1/
IEASYHYDROHF_USERNAME=username
IEASYHYDROHF_PASSWORD=password
```

**Initialization options:**
```python
from ieasyhydro_sdk.sdk import IEasyHydroHFSDK

# Option 1: From environment variables
sdk = IEasyHydroHFSDK()

# Option 2: Explicit configuration
sdk = IEasyHydroHFSDK(
    host='https://hf.ieasyhydro.org/api/v1/',
    username='username',
    password='password',
)
```

## Key Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `get_discharge_sites()` | List[dict] | Discharge stations (manual measurement sites) |
| `get_virtual_sites()` | List[dict] | Virtual/calculated stations |
| `get_meteo_sites()` | List[dict] | Meteorological stations |
| `get_data_values_for_site(filters)` | dict | Time-series data matching filters |
| `get_norm_for_site(site_code, norm_type, norm_period, automatic)` | List[float] | Historical norm values |

---

## Fetching Sites

```python
discharge_sites = sdk.get_discharge_sites()
virtual_sites = sdk.get_virtual_sites()
meteo_sites = sdk.get_meteo_sites()
```

### Site Response Structure

```python
{
    'id': 96,                           # Unique internal ID
    'site_code': '15054',               # Station code (use this for queries)
    'official_name': 'Station Name',
    'site_type': 'manual',              # 'manual' or 'automatic'
    'latitude': 42.8746,
    'longitude': 74.5698,
    'elevation': 0.0,
    'country': 'Country',
    'basin': {'official_name': 'Basin', 'national_name': ''},
    'region': {'official_name': 'Region', 'national_name': ''},
    'dangerous_discharge': 100.0,
    'enabled_forecasts': {
        'daily_forecast': False,
        'decadal_forecast': False,
        'monthly_forecast': False,
        'pentad_forecast': False,
        'seasonal_forecast': False
    },
    # Associations: for virtual/calculated stations only
    # Formula: discharge = sum(weight_i * discharge_i)
    'associations': [
        {'name': 'Name', 'id': 9, 'weight': 0.9, 'station_code': '12346'}
    ]
}
```

**Note:** Meteo sites have `enabled_forecasts: None`.

---

## Fetching Data Values

### Basic Usage

```python
filters = {
    "site_codes": ["15013", "16159"],
    "variable_names": ["WDDA"],
    "local_date_time__gte": "2024-01-01T00:00:00Z",
    "local_date_time__lte": "2024-12-31T23:59:59Z",
}

response = sdk.get_data_values_for_site(filters=filters)
```

### All Filter Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `site_codes` | List[str] | No* | Station codes like `["15013", "16159"]` |
| `site_ids` | List[int] | No* | Station IDs (internal database IDs) |
| `variable_names` | List[str] | **Yes** | Metric codes: `WDDA`, `WDD`, `WLD`, etc. |
| `local_date_time__gte` | str | **Yes**+ | Local timestamp >= (ISO format) |
| `local_date_time__gt` | str | No | Local timestamp > |
| `local_date_time__lte` | str | No | Local timestamp <= |
| `local_date_time__lt` | str | No | Local timestamp < |
| `local_date_time` | str | No | Exact local timestamp match |
| `utc_date_time__gte` | str | No | UTC timestamp >= |
| `utc_date_time__lte` | str | No | UTC timestamp <= |
| `page` | int | No | Page number for pagination |
| `page_size` | int | No | Items per page (max 1000, default 10) |

Either `site_codes` or `site_ids` can be used, but `site_codes` is recommended.
At least one timestamp filter is required.

### CRITICAL: Timestamp Handling

**`local_date_time` should NOT include local timezone — treat it as UTC:**

```python
import datetime

# To get 8AM local metrics:
local_date_time = datetime.datetime(
    2025, 9, 25, 8, 0, tzinfo=datetime.timezone.utc
).isoformat()
# Result: "2025-09-25T08:00:00+00:00"
```

### Response Structure

```python
{
    "count": 42,
    "next": "https://...?page=2",   # None if last page
    "previous": None,
    "results": [
        {
            "station_id": 123,
            "station_code": "16159",
            "station_name": "Station Name",
            "station_type": "hydro",
            "data": [
                {
                    "variable_code": "WDDA",
                    "unit": "m3/s",
                    "values": [
                        {
                            "value": 156.0,
                            "value_type": "M",
                            "timestamp_local": "2024-03-01T08:00:00",
                            "timestamp_utc": "2024-03-01T02:00:00Z",
                            "value_code": None
                        }
                    ]
                }
            ]
        }
    ]
}
```

**Response notes:**
- Non-existent station codes are silently omitted from results
- Stations with no data for requested variables return empty `values: []`

---

## Fetching Norms

```python
# Decadal discharge norm (default)
norm = sdk.get_norm_for_site("11194", "discharge")

# Monthly discharge norm
norm = sdk.get_norm_for_site("11194", "discharge", norm_period="m")

# Pentad norms for automatic station
norm = sdk.get_norm_for_site("11194", "discharge", norm_period="p", automatic=True)
```

| Parameter | Values | Description |
|-----------|--------|-------------|
| `norm_type` | `discharge`, `water_level`, `precipitation`, `temperature` | Type of norm |
| `norm_period` | `d` (decadal, default), `p` (pentad), `m` (monthly) | Time period |
| `automatic` | `True`/`False` | Automatic vs manual station |

Returns a list of floats: 36 (decadal), 12 (monthly), or 72 (pentadal) values. Missing norms are `None`.

---

## Key Gotchas Summary

1. **Page size max is 1000** — use this for better performance
2. **Use `site_codes` (strings) not `site_ids`** — both work but codes are more reliable
3. **`local_date_time` acts as UTC** — don't include local timezone offset
4. **At least one timestamp filter required** — or you get 422 error
5. **At least one variable_name required** — or you get 422 error
6. **Non-existent stations are silently omitted** — no error, just missing from results
7. **Large bulk requests may fail** — use batching for many sites + long date ranges
8. **Associations are for calculation** — they define how virtual station discharge is computed

---

## Detailed References

For detailed information, load these reference files:
- **Metric codes**: @references/metric_codes.md — all variable codes and value type codes
- **Pagination**: @references/pagination.md — sequential and parallel pagination patterns
- **Error handling**: @references/error_handling.md — error formats, common errors, robust fetching strategy
- **Examples**: @references/examples.md — complete working example
