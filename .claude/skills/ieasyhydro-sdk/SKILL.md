---
name: ieasyhydro-sdk
description: |
  Expert guide for the ieasyhydro-python-sdk library for connecting to iEasyHydro High Frequency (HF) database.
  Use when: (1) Making API requests to iEasyHydro HF, (2) Retrieving discharge/meteorological data,
  (3) Debugging 422 or other API errors, (4) Formatting filters for get_data_values_for_site(),
  (5) Working with code in preprocessing_runoff that fetches from iEasyHydro HF,
  (6) Understanding response structure from the SDK.
  Triggers: ieasyhydro, iEasyHydro, SDK, HF database, WDDA, WDD, discharge data retrieval, site_codes filter.
---

# iEasyHydro Python SDK Guide

SDK repository: https://github.com/hydrosolutions/ieasyhydro-python-sdk

## Connection Setup

```python
from ieasyhydro_sdk.sdk import IEasyHydroHFSDK

# Uses environment variables: IEASYHYDROHF_HOST, IEASYHYDROHF_USERNAME, IEASYHYDROHF_PASSWORD
sdk = IEasyHydroHFSDK()
```

## Key Methods

| Method | Returns |
|--------|---------|
| `get_discharge_sites()` | List of discharge stations with `id`, `site_code`, `name` |
| `get_meteo_sites()` | List of meteorological stations |
| `get_data_values_for_site(filters)` | Time-series data matching filters |

## Filter Format for get_data_values_for_site()

**CRITICAL: Use `site_codes` (strings), NOT `site_ids` (integers)**

```python
filters = {
    "site_codes": ["15013", "16159"],     # List of station CODE strings
    "variable_names": ["WDDA"],            # Required: metric codes
    "local_date_time__gte": "2024-01-01T00:00:00+06:00",  # Start (inclusive)
    "local_date_time__lte": "2024-12-31T23:59:59+06:00",  # End (inclusive)
}
```

### Filter Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `site_codes` | list[str] | No* | Station codes like `["15013", "16159"]` |
| `variable_names` | list[str] | **Yes** | Metric codes: `WDDA`, `WDD`, `WLD`, etc. |
| `local_date_time__gte` | str | **Yes*** | Start datetime (inclusive, ISO format with timezone) |
| `local_date_time__lte` | str | No | End datetime (inclusive, ISO format with timezone) |
| `local_date_time__lt` | str | No | End datetime (exclusive, ISO format with timezone) |
| `page` | int | No | Page number for pagination |
| `page_size` | int | No | Records per page (default ~100) |

*At least one timestamp filter required. `site_codes` optional but recommended.

**Date filter notes:** Use `__lte` (less than or equal) when you want to include the end date in results. Use `__lt` (less than) for exclusive end dates.

### Common Variable Names

| Code | Description |
|------|-------------|
| `WDDA` | Water Discharge Daily Average (m³/s) |
| `WDD` | Water Discharge Daily (morning reading) |
| `WLD` | Water Level Daily (cm) |
| `WLDA` | Water Level Daily Average (cm) |

## Response Structure

```python
{
    "count": 42,
    "next": "url...",      # Next page URL or None
    "previous": None,
    "results": [
        {
            "station_id": 37,
            "station_code": "15013",
            "station_name": "Джыргалан-с.Советское",
            "data": [
                {
                    "variable_code": "WDDA",
                    "unit": "m3/s",
                    "values": [
                        {
                            "value": 1.9,
                            "value_type": "M",  # M=Manual, A=Auto, E=Estimated
                            "timestamp_local": "2024-01-01T08:00:00",
                            "timestamp_utc": "2024-01-01T02:00:00Z"
                        }
                    ]
                }
            ]
        }
    ]
}
```

## Error Handling

**422 Error** - "Some data is invalid or missing":
- Check filter parameter names (`site_codes` not `site_ids`)
- Ensure `variable_names` is provided
- Verify at least one timestamp filter exists
- Check date format (ISO 8601 with timezone)

**Error response format:**
```python
{"status_code": 422, "text": '{"detail": "Some data is invalid or missing", "code": "schema_error"}'}
```

## Working Example

```python
from ieasyhydro_sdk.sdk import IEasyHydroHFSDK
from datetime import datetime, timezone

sdk = IEasyHydroHFSDK()

# Get all discharge sites
sites = sdk.get_discharge_sites()
site_codes = [s['site_code'] for s in sites]

# Query WDDA data
filters = {
    "site_codes": site_codes,
    "variable_names": ["WDDA"],
    "local_date_time__gte": "2024-11-01T00:00:00+06:00",
    "local_date_time__lte": "2024-12-31T23:59:59+06:00",  # Use __lte to include end date
}

response = sdk.get_data_values_for_site(filters=filters)

# Check for errors
if isinstance(response, dict) and 'status_code' in response:
    print(f"Error: {response}")
else:
    for station in response.get('results', []):
        code = station['station_code']
        for var in station.get('data', []):
            values = var.get('values', [])
            print(f"{code}: {len(values)} records")
```

## Pagination

For large requests, iterate through pages:

```python
page = 1
all_results = []

while True:
    filters['page'] = page
    response = sdk.get_data_values_for_site(filters=filters)

    if 'results' in response:
        all_results.extend(response['results'])

    if not response.get('next'):
        break
    page += 1
```

## API Limitations

The API has complex limits that cause 422 errors based on **combinations** of:
- Number of site codes in request
- Date range length
- Page size value

**Known constraints:**
- `page_size` values > 10 may cause 422 errors (use default or 10)
- Large bulk requests (60+ sites with long date ranges) often fail
- Individual site requests always work

**Recommended approach for production code:**

```python
# Strategy: Try bulk, fall back to batches, then individual requests
def fetch_robust(sdk, site_codes, filters):
    # 1. Try bulk (no page_size specified - use default)
    bulk_filters = {**filters, "site_codes": site_codes}
    response = sdk.get_data_values_for_site(filters=bulk_filters)

    if 'status_code' not in response:
        return response  # Success

    # 2. Try batches of 10
    for batch in chunks(site_codes, 10):
        batch_filters = {**filters, "site_codes": batch}
        # ... fetch each batch

    # 3. Fall back to parallel individual requests
    for code in site_codes:
        single_filters = {**filters, "site_codes": [code]}
        # ... fetch individually
```

See `fetch_hydro_HF_data_robust()` in `src/src.py` for the full implementation.
