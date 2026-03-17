# Error Handling

## API Requirements

1. **At least one timestamp filter must be present**
2. **At least one variable name must be specified**
3. **Variable names must be valid** (see @references/metric_codes.md)

## Error Response Format

```python
{
    'status_code': 422,
    'text': '{"detail": "Some data is invalid or missing", "code": "schema_error"}'
}
```

## Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| 422 "Some data is invalid" | Missing timestamp filter | Add `local_date_time__gte` or similar |
| 422 "Some data is invalid" | Missing variable_names | Add `variable_names: ["WDDA"]` |
| 422 "Invalid metric names" | Wrong variable code | Check metric codes table |
| 422 with large requests | Too many sites + large date range | Use batching or individual requests |

## Robust Fetching Strategy

```python
def fetch_robust(sdk, site_codes, base_filters, batch_size=10):
    """Try bulk, then batches, then individual requests."""

    # 1. Try bulk request
    bulk_filters = {**base_filters, "site_codes": site_codes}
    response = sdk.get_data_values_for_site(filters=bulk_filters)

    if isinstance(response, dict) and 'status_code' not in response:
        return fetch_all_with_pagination(sdk, bulk_filters)

    # 2. Try batches
    all_results = []
    for i in range(0, len(site_codes), batch_size):
        batch = site_codes[i:i + batch_size]
        batch_filters = {**base_filters, "site_codes": batch}
        results = fetch_all_with_pagination(sdk, batch_filters)
        all_results.extend(results)

    if all_results:
        return all_results

    # 3. Fall back to individual requests
    for code in site_codes:
        single_filters = {**base_filters, "site_codes": [code]}
        results = fetch_all_with_pagination(sdk, single_filters)
        all_results.extend(results)

    return all_results
```
