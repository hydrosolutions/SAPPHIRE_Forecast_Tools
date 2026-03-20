# Pagination Patterns

## Page Size Limit

**The API supports `page_size` up to 1000 (updated January 2026).**

- Default page size is 10
- Using `page_size=1000` significantly improves performance (reduced maintenance run from 800s to 364s)
- You may still need pagination for very large result sets

## Sequential Pagination

```python
def fetch_all_with_pagination(sdk, filters):
    """Fetch all results with pagination."""
    all_results = []
    page = 1

    while True:
        paginated_filters = {**filters, "page": page, "page_size": 1000}
        response = sdk.get_data_values_for_site(filters=paginated_filters)

        # Check for API error
        if isinstance(response, dict) and 'status_code' in response:
            print(f"Error: {response}")
            break

        results = response.get('results', [])
        all_results.extend(results)

        # Check if there are more pages
        if not response.get('next'):
            break
        page += 1

    return all_results
```

## Parallel Pagination

For better performance, fetch pages in parallel after getting the count from page 1:

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_all_parallel(sdk, filters, max_workers=10):
    """Fetch all results using parallel page requests."""
    PAGE_SIZE = 1000

    # Step 1: Get first page to learn total count
    first_response = sdk.get_data_values_for_site(
        filters={**filters, "page": 1, "page_size": PAGE_SIZE}
    )

    if isinstance(first_response, dict) and 'status_code' in first_response:
        return []  # Error

    total_count = first_response.get('count', 0)
    all_results = first_response.get('results', [])

    total_pages = (total_count + PAGE_SIZE - 1) // PAGE_SIZE
    if total_pages <= 1:
        return all_results

    # Step 2: Fetch remaining pages in parallel
    def fetch_page(page_num):
        resp = sdk.get_data_values_for_site(
            filters={**filters, "page": page_num, "page_size": PAGE_SIZE}
        )
        if isinstance(resp, dict) and 'status_code' not in resp:
            return resp.get('results', [])
        return []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_page, p) for p in range(2, total_pages + 1)]
        for future in as_completed(futures):
            all_results.extend(future.result())

    return all_results
```
