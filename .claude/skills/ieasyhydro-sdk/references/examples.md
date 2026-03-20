# Complete Working Example

```python
from ieasyhydro_sdk.sdk import IEasyHydroHFSDK
from datetime import datetime, timezone, timedelta

sdk = IEasyHydroHFSDK()

# 1. Get all discharge sites
sites = sdk.get_discharge_sites()
site_codes = list(set(s['site_code'] for s in sites))  # Unique codes
print(f"Found {len(site_codes)} unique discharge sites")

# 2. Define date range
end_date = datetime.now(timezone.utc)
start_date = end_date - timedelta(days=30)

# 3. Build filters
filters = {
    "site_codes": site_codes,
    "variable_names": ["WDDA"],
    "local_date_time__gte": start_date.strftime('%Y-%m-%dT00:00:00Z'),
    "local_date_time__lte": end_date.strftime('%Y-%m-%dT23:59:59Z'),
}

# 4. Fetch with pagination
all_results = []
page = 1
while True:
    filters['page'] = page
    filters['page_size'] = 1000  # Max allowed

    response = sdk.get_data_values_for_site(filters=filters)

    if isinstance(response, dict) and 'status_code' in response:
        print(f"Error: {response}")
        break

    all_results.extend(response.get('results', []))

    if not response.get('next'):
        break
    page += 1

print(f"Fetched {len(all_results)} station records in {page} pages")

# 5. Process results
for station in all_results:
    code = station['station_code']
    name = station['station_name']
    for var in station.get('data', []):
        values = var.get('values', [])
        if values:
            latest = max(v['timestamp_local'][:10] for v in values)
            print(f"{code} ({name}): {len(values)} values, latest: {latest}")
```
