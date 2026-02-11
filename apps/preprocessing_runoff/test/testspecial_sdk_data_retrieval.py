#!/usr/bin/env python3
"""
Diagnostic test script to verify iEasyHydro HF SDK data retrieval.

Tests that we can retrieve the latest N days of data for a gauge,
or scan all available sites to check data freshness.

Expected Runtime:
-----------------
    Single site test:       ~5 seconds
    CHECK_ALL_SITES=1:      ~30 seconds
    DIAGNOSE_422=1:         ~2-5 minutes (tests each site individually)

Required Environment Variables (in .env file):
----------------------------------------------
    IEASYHYDROHF_HOST       iEasyHydro HF API endpoint URL
                            Example: http://localhost:<port>/api/v1/

    IEASYHYDROHF_USERNAME   API username for authentication

    IEASYHYDROHF_PASSWORD   API password for authentication

Optional Environment Variables:
-------------------------------
    ieasyhydroforecast_env_file_path    Path to .env file to load
    TEST_SITE_CODE                      Site code to query (default: 17462)
    TEST_DAYS                           Number of days to look back (default: 20)
    CHECK_ALL_SITES                     Set to '1' to scan all sites for recent data
    MAX_AGE_DAYS                        Max age in days for "current" data (default: 5)
    DIAGNOSE_422                        Set to '1' to diagnose 422 API errors
    TEST_SITE_IDS                       Comma-separated site IDs to test (for DIAGNOSE_422)

Usage:
------
    cd apps/preprocessing_runoff

    # Test a single site:
    ieasyhydroforecast_env_file_path=/path/to/.env uv run python test/testspecial_sdk_data_retrieval.py

    # Test a specific site:
    TEST_SITE_CODE=15013 ieasyhydroforecast_env_file_path=/path/to/.env uv run python test/testspecial_sdk_data_retrieval.py

    # Check all sites for recent data:
    CHECK_ALL_SITES=1 ieasyhydroforecast_env_file_path=/path/to/.env uv run python test/testspecial_sdk_data_retrieval.py

    # Diagnose 422 errors - tests each site ID individually to find invalid ones:
    DIAGNOSE_422=1 ieasyhydroforecast_env_file_path=/path/to/.env uv run python test/testspecial_sdk_data_retrieval.py

    # Diagnose specific site IDs:
    DIAGNOSE_422=1 TEST_SITE_IDS="123,456,789" ieasyhydroforecast_env_file_path=/path/to/.env uv run python test/testspecial_sdk_data_retrieval.py

SDK Documentation:
------------------
    https://github.com/hydrosolutions/ieasyhydro-python-sdk
"""

import os
import sys
from datetime import datetime, timedelta

# Load .env if path provided
env_file = os.getenv('ieasyhydroforecast_env_file_path')
if env_file and os.path.exists(env_file):
    print(f"Loading environment from: {env_file}")
    from dotenv import load_dotenv
    load_dotenv(env_file)

from ieasyhydro_sdk.sdk import IEasyHydroHFSDK


def test_sdk_connection():
    """Test basic SDK connection."""
    print("\n--- SDK Connection ---")
    try:
        sdk = IEasyHydroHFSDK()
        print("OK: SDK initialized")
        return sdk
    except Exception as e:
        print(f"FAILED: {e}")
        print(f"\nCheck: IEASYHYDROHF_HOST={os.getenv('IEASYHYDROHF_HOST', 'NOT SET')}")
        sys.exit(1)


def test_data_retrieval(sdk, site_code, days=10):
    """Test data retrieval for a specific gauge."""
    print(f"\n{'='*60}")
    print(f"TEST: Single Site Data Retrieval")
    print(f"{'='*60}")
    print(f"  Gauge: {site_code}")
    print(f"  Period: Last {days} days")
    print(f"  Filter: station__station_code__in (testing alternative to site_codes)")

    # Calculate date range (use timezone-aware UTC)
    from datetime import timezone
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days)

    # Format timestamps as ISO with Z suffix (as per SDK docs)
    start_str = start_date.strftime('%Y-%m-%dT00:00:00Z')
    end_str = end_date.strftime('%Y-%m-%dT23:59:59Z')

    print(f"  Date range: {start_str} to {end_str}")
    print(f"\nQuerying API...")

    # Use filters as per SDK documentation
    # WDDA = Water Discharge Daily Average
    # Using station__station_code__in instead of site_codes
    filters = {
        "station__station_code__in": [site_code],
        "variable_names": ["WDDA"],
        "local_date_time__gte": start_str,
        "local_date_time__lt": end_str,
    }

    try:
        response = sdk.get_data_values_for_site(filters=filters)

        # Handle error responses
        if isinstance(response, dict) and 'status_code' in response:
            print(f"ERROR: API returned status {response['status_code']}")
            if 'text' in response:
                print(f"  {response['text'][:500]}")
            return None

        # Extract results from response
        if isinstance(response, dict):
            if 'results' in response:
                data = response['results']
            elif 'data_values' in response:
                data = response['data_values']
            else:
                print(f"Response keys: {response.keys()}")
                data = []
        elif isinstance(response, list):
            data = response
        else:
            print(f"Unexpected response: {type(response)}")
            return None

        if not data:
            print("WARNING: No data returned")
            return []

        # Extract and display values from nested structure
        # Structure: record['data'][0]['values'][i] has timestamp_local, value
        print(f"{'Date':<12} {'Value':>10}")
        print("-" * 24)

        dates_found = []
        for station_record in data:
            station_name = station_record.get('station_name', 'Unknown')
            for var_data in station_record.get('data', []):
                for val in var_data.get('values', []):
                    date_val = val.get('timestamp_local', 'N/A')
                    if isinstance(date_val, str) and len(date_val) >= 10:
                        date_val = date_val[:10]
                    value = val.get('value', 'N/A')
                    if isinstance(value, (int, float)):
                        value_str = f"{value:.2f}"
                    else:
                        value_str = str(value)
                    print(f"{date_val:<12} {value_str:>10}")
                    if date_val != 'N/A':
                        dates_found.append(date_val)

        # Summary
        print("-" * 24)
        if dates_found:
            latest = max(dates_found)
            today = datetime.now().strftime('%Y-%m-%d')
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

            print(f"Latest: {latest}")
            print(f"Today:  {today}")

            if latest >= yesterday:
                print("\nRESULT: Data is CURRENT")
            else:
                try:
                    days_old = (datetime.now() - datetime.strptime(latest, '%Y-%m-%d')).days
                    print(f"\nWARNING: Data is {days_old} days old")
                except:
                    print(f"\nCould not parse date: {latest}")

        return data

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None


def check_all_sites_for_recent_data(sdk, max_age_days=5):
    """Check all available sites for data younger than max_age_days."""
    print(f"\n{'='*60}")
    print(f"TEST: Check All Sites for Recent Data")
    print(f"{'='*60}")
    print(f"  Filter: station__station_code__in (testing alternative to site_codes)")
    print(f"  Looking for data < {max_age_days} days old")
    print(f"\nFetching list of discharge sites from API...")

    from datetime import timezone

    # Get all discharge sites
    discharge_sites = sdk.get_discharge_sites()
    print(f"  Found {len(discharge_sites)} discharge sites")

    # Calculate cutoff date
    cutoff = (datetime.now(timezone.utc) - timedelta(days=max_age_days)).strftime('%Y-%m-%d')
    today = datetime.now().strftime('%Y-%m-%d')

    print(f"Looking for data newer than: {cutoff}")
    print(f"Today: {today}\n")

    # Query recent window
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=max_age_days + 5)  # A bit more to catch edge cases
    start_str = start_date.strftime('%Y-%m-%dT00:00:00Z')
    end_str = end_date.strftime('%Y-%m-%dT23:59:59Z')

    # Collect all site codes (remove duplicates)
    site_codes_raw = [s.get('site_code') for s in discharge_sites if s.get('site_code')]
    site_codes = list(dict.fromkeys(site_codes_raw))  # Remove duplicates, preserve order
    print(f"  Unique site codes: {len(site_codes)} (removed {len(site_codes_raw) - len(site_codes)} duplicates)")

    # Query all sites at once
    # Using station__station_code__in instead of site_codes
    print(f"\nQuerying API for all {len(site_codes)} sites at once...")
    print(f"  (This may take a moment depending on data volume)")
    filters = {
        "station__station_code__in": site_codes,
        "variable_names": ["WDDA"],
        "local_date_time__gte": start_str,
        "local_date_time__lt": end_str,
    }

    response = sdk.get_data_values_for_site(filters=filters)

    # Handle error
    if isinstance(response, dict) and 'status_code' in response:
        print(f"ERROR: {response.get('text', 'Unknown error')[:200]}")
        return

    # Extract results
    if isinstance(response, dict) and 'results' in response:
        data = response['results']
    else:
        data = response if isinstance(response, list) else []

    # Analyze each site
    results = []
    for station_record in data:
        site_code = station_record.get('station_code', 'Unknown')
        site_name = station_record.get('station_name', 'Unknown')

        latest_date = None
        for var_data in station_record.get('data', []):
            for val in var_data.get('values', []):
                date_val = val.get('timestamp_local', '')
                if date_val and len(date_val) >= 10:
                    date_str = date_val[:10]
                    if latest_date is None or date_str > latest_date:
                        latest_date = date_str

        if latest_date:
            is_recent = latest_date >= cutoff
            results.append({
                'code': site_code,
                'name': site_name[:30],
                'latest': latest_date,
                'recent': is_recent
            })

    # Sort by latest date descending
    results.sort(key=lambda x: x['latest'], reverse=True)

    # Display results
    print(f"{'Code':<8} {'Latest':<12} {'Status':<10} {'Name'}")
    print("-" * 70)

    recent_count = 0
    for r in results:
        status = "CURRENT" if r['recent'] else "STALE"
        if r['recent']:
            recent_count += 1
        print(f"{r['code']:<8} {r['latest']:<12} {status:<10} {r['name']}")

    print("-" * 70)
    print(f"\nSummary: {recent_count}/{len(results)} sites have data < {max_age_days} days old")

    if recent_count == 0:
        print("\nWARNING: No sites have recent data!")

    return results


def diagnose_422_error(sdk, site_ids):
    """
    Test each site ID individually to find which ones cause 422 errors.

    This helps diagnose "Some data is invalid or missing" errors from the API.

    Args:
        sdk: IEasyHydroHFSDK instance
        site_ids: List of site IDs to test
    """
    print(f"\n{'='*60}")
    print(f"TEST: Diagnose 422 Errors (Individual Site Testing)")
    print(f"{'='*60}")
    print(f"  Filter: station__station_code__in (testing alternative to site_codes)")
    print(f"  Sites to test: {len(site_ids)}")
    print(f"\n  WHY THIS TAKES TIME: Testing each site individually to identify")
    print(f"  which specific sites cause API errors. This requires {len(site_ids)} API calls.")

    from datetime import timezone
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=10)
    start_str = start_date.strftime('%Y-%m-%dT00:00:00Z')
    end_str = end_date.strftime('%Y-%m-%dT23:59:59Z')

    # Build a map of site_id -> site_code
    print(f"\nStep 1/2: Fetching site code mapping...")
    discharge_sites = sdk.get_discharge_sites()
    id_to_code = {s.get('id'): s.get('site_code', 'Unknown') for s in discharge_sites}
    print(f"  Mapped {len(id_to_code)} site IDs to codes")

    failed_codes = []
    success_codes = []

    print(f"\nStep 2/2: Testing each site individually...")
    print(f"  Date range: {start_str} to {end_str}")
    print(f"\n{'Site Code':<12} {'Status':<12} {'Details'}")
    print("-" * 60)

    for site_id in site_ids:
        site_code = id_to_code.get(site_id, str(site_id))

        # Test with station__station_code__in (alternative API parameter)
        filters = {
            "station__station_code__in": [site_code],
            "variable_names": ["WDDA"],
            "local_date_time__gte": start_str,
            "local_date_time__lt": end_str,
        }

        response = sdk.get_data_values_for_site(filters=filters)

        if isinstance(response, dict) and 'status_code' in response:
            status_code = response.get('status_code', 'Unknown')
            text = response.get('text', '')[:80]
            print(f"{site_code:<12} {'FAILED':<12} {status_code}: {text}")
            failed_codes.append(site_code)
        elif isinstance(response, dict) and 'results' in response:
            count = len(response.get('results', []))
            if count > 0:
                # Get latest date
                latest = None
                for r in response['results']:
                    for d in r.get('data', []):
                        for v in d.get('values', []):
                            ts = v.get('timestamp_local', '')[:10] if v.get('timestamp_local') else None
                            if ts and (latest is None or ts > latest):
                                latest = ts
                print(f"{site_code:<12} {'OK':<12} {count} record(s), latest: {latest}")
            else:
                print(f"{site_code:<12} {'EMPTY':<12} No data returned")
            success_codes.append(site_code)
        else:
            print(f"{site_code:<12} {'UNKNOWN':<12} Unexpected response type")
            failed_codes.append(site_code)

    print("-" * 60)
    print(f"\nSummary:")
    print(f"  Success: {len(success_codes)} sites")
    print(f"  Failed:  {len(failed_codes)} sites")

    if failed_codes:
        print(f"\nFailed site codes: {failed_codes}")
        print("\nThese sites may not exist in iEasyHydro HF database.")
        print("Check if they're in your forecast site configuration but not in the DB.")

    return failed_codes, success_codes


def fetch_all_with_pagination(sdk, filters, description="data", parallel=True, max_workers=10):
    """
    Fetch all results from the SDK, handling pagination.

    The iEasyHydro HF API has a page size limit of 10 (undocumented).
    This function fetches page 1 to get the total count, then fetches
    remaining pages in parallel for speed.

    Args:
        sdk: IEasyHydroHFSDK instance
        filters: Dict of filter parameters (without page/page_size)
        description: String describing what we're fetching (for logging)
        parallel: If True, fetch pages in parallel (much faster)
        max_workers: Number of parallel workers for page fetching

    Returns:
        tuple: (all_results, total_count, pages_fetched, success)
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time

    PAGE_SIZE = 10  # API limit - cannot be changed

    # Step 1: Fetch first page to get total count
    start_time = time.time()
    first_page_filters = {**filters, "page": 1, "page_size": PAGE_SIZE}
    response = sdk.get_data_values_for_site(filters=first_page_filters)

    # Check for API error
    if isinstance(response, dict) and 'status_code' in response:
        print(f"    ERROR on page 1: {response.get('status_code')}")
        return [], 0, 0, False

    # Extract first page results and total count
    all_results = response.get('results', []) if isinstance(response, dict) else []
    total_count = response.get('count', 0) if isinstance(response, dict) else len(all_results)

    # Calculate how many pages we need
    total_pages = (total_count + PAGE_SIZE - 1) // PAGE_SIZE  # ceiling division

    if total_pages <= 1:
        elapsed = time.time() - start_time
        print(f"    {total_count} {description} fetched in 1 page ({elapsed:.1f}s)")
        return all_results, total_count, 1, True

    print(f"    Total {description}: {total_count} (need {total_pages} pages)")

    # Step 2: Fetch remaining pages
    if parallel and total_pages > 1:
        # PARALLEL: Fetch pages 2..N simultaneously
        print(f"    Fetching pages 2-{total_pages} in parallel ({max_workers} workers)...")

        def fetch_page(page_num):
            """Fetch a single page."""
            page_filters = {**filters, "page": page_num, "page_size": PAGE_SIZE}
            resp = sdk.get_data_values_for_site(filters=page_filters)
            if isinstance(resp, dict) and 'status_code' not in resp:
                return resp.get('results', [])
            return []

        pages_to_fetch = list(range(2, total_pages + 1))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_page, p): p for p in pages_to_fetch}
            completed = 0
            for future in as_completed(futures):
                completed += 1
                try:
                    page_results = future.result()
                    all_results.extend(page_results)
                except Exception as e:
                    print(f"    Error fetching page {futures[future]}: {e}")

                # Progress every 20% or so
                if completed % max(1, len(pages_to_fetch) // 5) == 0:
                    print(f"    Progress: {completed}/{len(pages_to_fetch)} pages...")

        elapsed = time.time() - start_time
        print(f"    Fetched {len(all_results)}/{total_count} records in {elapsed:.1f}s")

    else:
        # SEQUENTIAL: Fetch pages one by one (slower, but useful for debugging)
        print(f"    Fetching pages 2-{total_pages} sequentially...")
        for page in range(2, total_pages + 1):
            page_filters = {**filters, "page": page, "page_size": PAGE_SIZE}
            response = sdk.get_data_values_for_site(filters=page_filters)

            if isinstance(response, dict) and 'status_code' in response:
                print(f"    ERROR on page {page}: {response.get('status_code')}")
                break

            page_results = response.get('results', []) if isinstance(response, dict) else []
            all_results.extend(page_results)

            if page % 10 == 0:
                print(f"    Page {page}/{total_pages}: {len(all_results)} records...")

        elapsed = time.time() - start_time
        print(f"    Fetched {len(all_results)}/{total_count} records in {elapsed:.1f}s")

    return all_results, total_count, total_pages, True


def test_filter_comparison(sdk, site_codes, start_datetime, end_datetime):
    """
    Compare 'site_codes' vs 'station__station_code__in' filters.

    Tests if both filters return equivalent results.
    """
    print(f"\n{'='*70}")
    print("TEST: Comparing 'site_codes' vs 'station__station_code__in' filters")
    print(f"{'='*70}")
    print(f"  Testing with {len(site_codes)} site codes")
    print(f"  Date range: {start_datetime.strftime('%Y-%m-%d')} to {end_datetime.strftime('%Y-%m-%d')}")

    # Test 1: Using site_codes filter
    print(f"\n--- Test A: Using 'site_codes' filter ---")
    filters_site_codes = {
        "site_codes": site_codes,
        "variable_names": ["WDDA"],
        "local_date_time__gte": start_datetime.isoformat(),
        "local_date_time__lte": end_datetime.isoformat(),
    }

    results_a, count_a, pages_a, success_a = fetch_all_with_pagination(
        sdk, filters_site_codes, "records (site_codes)"
    )

    if success_a:
        print(f"  Result: {len(results_a)} records, API count: {count_a}, pages: {pages_a}")
    else:
        print(f"  FAILED to fetch with site_codes filter")

    # Test 2: Using station__station_code__in filter
    print(f"\n--- Test B: Using 'station__station_code__in' filter ---")
    filters_station_code = {
        "station__station_code__in": site_codes,
        "variable_names": ["WDDA"],
        "local_date_time__gte": start_datetime.isoformat(),
        "local_date_time__lte": end_datetime.isoformat(),
    }

    results_b, count_b, pages_b, success_b = fetch_all_with_pagination(
        sdk, filters_station_code, "records (station__station_code__in)"
    )

    if success_b:
        print(f"  Result: {len(results_b)} records, API count: {count_b}, pages: {pages_b}")
    else:
        print(f"  FAILED to fetch with station__station_code__in filter")

    # Compare results
    print(f"\n{'='*70}")
    print("COMPARISON RESULTS:")
    print(f"{'='*70}")

    if not success_a and not success_b:
        print("  BOTH FILTERS FAILED")
        return None, None

    if not success_a:
        print("  'site_codes' FAILED, 'station__station_code__in' WORKED")
        print("  --> Use 'station__station_code__in'")
        return None, results_b

    if not success_b:
        print("  'site_codes' WORKED, 'station__station_code__in' FAILED")
        print("  --> Use 'site_codes'")
        return results_a, None

    # Both succeeded - compare
    print(f"  {'Filter':<30} {'API Count':<12} {'Retrieved':<12} {'Pages'}")
    print(f"  {'-'*66}")
    print(f"  {'site_codes':<30} {count_a:<12} {len(results_a):<12} {pages_a}")
    print(f"  {'station__station_code__in':<30} {count_b:<12} {len(results_b):<12} {pages_b}")

    # Check if counts match
    if count_a == count_b and len(results_a) == len(results_b):
        print(f"\n  RESULT: FILTERS ARE EQUIVALENT")
        print(f"  Both return the same number of records.")
    else:
        print(f"\n  RESULT: FILTERS DIFFER!")
        print(f"  Difference: {abs(count_a - count_b)} records")

        # Analyze which codes are different
        codes_a = set(r.get('station_code') for r in results_a)
        codes_b = set(r.get('station_code') for r in results_b)

        only_in_a = codes_a - codes_b
        only_in_b = codes_b - codes_a

        if only_in_a:
            print(f"  Codes only in site_codes result: {only_in_a}")
        if only_in_b:
            print(f"  Codes only in station__station_code__in result: {only_in_b}")

    return results_a, results_b


def test_bulk_vs_individual(sdk, site_ids):
    """
    Test data retrieval with station__station_code__in filter and pagination.

    Focus: Verify we can get ALL data using pagination (page size limit = 10).
    """
    print(f"\n{'='*70}")
    print(f"TEST: Data Retrieval with Pagination")
    print(f"{'='*70}")
    print(f"  Filter: station__station_code__in (testing alternative to site_codes)")
    print(f"  Sites to test: {len(site_ids)}")
    print(f"\n  IMPORTANT: API has undocumented page_size limit of 10.")
    print(f"  This test verifies we can get ALL data by paging through results.")

    from datetime import timezone
    import pytz

    # Use the same date format as preprocessing_runoff (isoformat with timezone)
    try:
        target_tz = pytz.timezone('Asia/Bishkek')  # Kyrgyzstan timezone
    except:
        target_tz = timezone.utc

    # Allow setting specific start/end dates via environment variables
    # Format: YYYY-MM-DD
    start_date_str = os.getenv('TEST_START_DATE')
    end_date_str = os.getenv('TEST_END_DATE')

    if start_date_str and end_date_str:
        # Use specific dates
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').replace(hour=0, minute=1)
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59)
        start_datetime = target_tz.localize(start_date)
        end_datetime = target_tz.localize(end_date)
        print(f"\nDate range (from env vars):")
    else:
        # Use lookback from today
        end_datetime = datetime.now(target_tz)
        lookback_days = int(os.getenv('TEST_LOOKBACK_DAYS', '120'))
        start_date = datetime.now().replace(hour=0, minute=1) - timedelta(days=lookback_days)
        start_datetime = target_tz.localize(start_date) if start_date.tzinfo is None else start_date
        print(f"\nDate range (lookback {lookback_days} days):")

    print(f"  Start: {start_datetime.strftime('%Y-%m-%d %H:%M')} ({target_tz})")
    print(f"  End:   {end_datetime.strftime('%Y-%m-%d %H:%M')} ({target_tz})")

    # Build a map of site_id -> site_code from the discharge sites
    print(f"\n" + "-"*70)
    print(f"Step 1/4: Fetching site code mapping from API...")
    print("-"*70)
    discharge_sites = sdk.get_discharge_sites()
    id_to_code = {s.get('id'): s.get('site_code', 'Unknown') for s in discharge_sites}
    id_to_name = {s.get('id'): s.get('name', 'Unknown')[:30] for s in discharge_sites}
    print(f"  Retrieved {len(discharge_sites)} sites from API")

    # Convert site_ids to site_codes for the API (remove duplicates)
    site_codes_raw = [id_to_code.get(sid) for sid in site_ids if id_to_code.get(sid)]
    site_codes = list(dict.fromkeys(site_codes_raw))  # Remove duplicates, preserve order

    print(f"  Mapped {len(site_ids)} site IDs -> {len(site_codes)} unique codes")
    if len(site_codes_raw) != len(site_codes):
        print(f"  (Removed {len(site_codes_raw) - len(site_codes)} duplicate codes)")

    # Check for any None or empty codes
    invalid_codes = [c for c in site_codes if not c or c == 'Unknown']
    if invalid_codes:
        print(f"  WARNING: Found {len(invalid_codes)} invalid codes")
        site_codes = [c for c in site_codes if c and c != 'Unknown']

    print(f"  First 5 codes: {site_codes[:5]}")

    # FIRST: Compare the two filter parameters
    test_filter_comparison(sdk, site_codes, start_datetime, end_datetime)

    # Test 1: Single site with pagination
    print(f"\n" + "-"*70)
    print(f"Step 2/4: Test pagination with a SINGLE site")
    print("-"*70)
    test_site = site_codes[0] if site_codes else None
    if test_site:
        print(f"  Testing site: {test_site}")
        single_filters = {
            "station__station_code__in": [test_site],
            "variable_names": ["WDDA"],
            "local_date_time__gte": start_datetime.isoformat(),
            "local_date_time__lte": end_datetime.isoformat(),
        }

        results, total, pages, success = fetch_all_with_pagination(sdk, single_filters, "records")

        if success:
            print(f"  Result: {len(results)} records fetched in {pages} page(s)")
            if results:
                # Show date range of returned data
                dates = []
                for r in results:
                    for d in r.get('data', []):
                        for v in d.get('values', []):
                            ts = v.get('timestamp_local', '')
                            if ts:
                                dates.append(ts[:10])
                if dates:
                    print(f"  Data range: {min(dates)} to {max(dates)}")
            print(f"  PAGINATION TEST: {'PASSED' if len(results) == total else 'FAILED'} (got {len(results)}/{total})")
        else:
            print(f"  FAILED to fetch data for site {test_site}")

    # Test 2: All sites with pagination
    print(f"\n" + "-"*70)
    print(f"Step 3/4: Test pagination with ALL {len(site_codes)} sites")
    print("-"*70)
    print(f"  This will page through all results (page_size=10)...")

    all_sites_filters = {
        "station__station_code__in": site_codes,
        "variable_names": ["WDDA"],
        "local_date_time__gte": start_datetime.isoformat(),
        "local_date_time__lte": end_datetime.isoformat(),
    }

    all_results, total_count, pages_fetched, success = fetch_all_with_pagination(
        sdk, all_sites_filters, "station records"
    )

    if not success:
        print(f"  FAILED: Could not fetch data with station__station_code__in filter")
        print(f"  This might mean the filter parameter is not supported.")
        return [], []

    print(f"\n  Pagination complete:")
    print(f"    Pages fetched: {pages_fetched}")
    print(f"    Total from API: {total_count}")
    print(f"    Records retrieved: {len(all_results)}")
    print(f"    PAGINATION TEST: {'PASSED' if len(all_results) == total_count else 'FAILED'}")

    # Analyze the response
    print(f"\n" + "-"*70)
    print(f"Step 4/4: RESULTS ANALYSIS")
    print("-"*70)
    results = all_results
    print(f"  Total station records retrieved: {len(results)}")

    # Extract which site IDs returned data (using id_to_code from earlier)
    sites_with_data = {}
    for station_record in results:
        site_id = station_record.get('station_id')
        site_code = station_record.get('station_code', id_to_code.get(site_id, 'Unknown'))
        site_name = station_record.get('station_name', id_to_name.get(site_id, 'Unknown'))[:30]

        # Count values and find latest date
        value_count = 0
        latest_date = None
        for var_data in station_record.get('data', []):
            for val in var_data.get('values', []):
                value_count += 1
                ts = val.get('timestamp_local', '')
                if ts and len(ts) >= 10:
                    date_str = ts[:10]
                    if latest_date is None or date_str > latest_date:
                        latest_date = date_str

        sites_with_data[site_id] = {
            'code': site_code,
            'name': site_name,
            'values': value_count,
            'latest': latest_date
        }

    # Find sites that didn't return data
    sites_without_data = [sid for sid in site_ids if sid not in sites_with_data]
    sites_without_data_codes = [id_to_code.get(sid, str(sid)) for sid in sites_without_data]

    # Display results
    print(f"\n{'='*70}")
    print(f"SITES WITH DATA ({len(sites_with_data)}/{len(site_ids)}):")
    print(f"{'='*70}")
    print(f"{'Code':<10} {'Values':<8} {'Latest':<12} {'Name'}")
    print("-" * 70)

    for site_id, info in sorted(sites_with_data.items(), key=lambda x: x[1]['latest'] or '', reverse=True):
        print(f"{info['code']:<10} {info['values']:<8} {info['latest'] or 'N/A':<12} {info['name']}")

    if sites_without_data:
        print(f"\n{'='*70}")
        print(f"SITES WITHOUT DATA ({len(sites_without_data)}/{len(site_ids)}):")
        print(f"{'='*70}")
        print(f"Site codes: {sites_without_data_codes}")
        print("\nThese sites exist in iEasyHydro HF but have no WDDA data for the date range.")
        print("This may cause 422 errors if the API validation is strict.")

        # Check if these sites have WDD data instead of WDDA
        print("\nAdditional check: Do these sites have WDD (morning) data instead of WDDA (daily avg)?")
        # Remove duplicates from sites_without_data_codes
        unique_sites_without_data_codes = list(dict.fromkeys(sites_without_data_codes))
        print(f"  Querying {len(unique_sites_without_data_codes)} sites for WDD variable (with pagination)...")
        wdd_filters = {
            "station__station_code__in": unique_sites_without_data_codes,
            "variable_names": ["WDD"],
            "local_date_time__gte": start_datetime.isoformat(),
            "local_date_time__lte": end_datetime.isoformat(),
        }

        wdd_results, wdd_total, wdd_pages, wdd_success = fetch_all_with_pagination(
            sdk, wdd_filters, "WDD records"
        )

        if wdd_success:
            wdd_site_codes = [r.get('station_code') for r in wdd_results]
            wdd_value_counts = {}
            for station in wdd_results:
                code = station.get('station_code')
                count = sum(len(v.get('values', [])) for v in station.get('data', []))
                wdd_value_counts[code] = count

            print(f"  Found {len(wdd_results)} sites with WDD data (fetched {wdd_pages} page(s)):")
            for code, count in sorted(wdd_value_counts.items(), key=lambda x: -x[1])[:10]:
                print(f"    {code}: {count} WDD records")
            if len(wdd_value_counts) > 10:
                print(f"    ... and {len(wdd_value_counts) - 10} more")

            # Sites with neither WDDA nor WDD
            sites_with_wdd = set(wdd_site_codes)
            truly_empty = [c for c in sites_without_data_codes if c not in sites_with_wdd]
            if truly_empty:
                print(f"\n  Sites with NEITHER WDDA nor WDD data ({len(truly_empty)}):")
                print(f"    {truly_empty[:20]}")
        else:
            print(f"  WDD query failed")

        # Test if these sites cause the 422 individually
        print("\nTesting first 5 sites without data individually (to check for errors)...")
        for site_id in sites_without_data[:5]:  # Test first 5 only
            site_code = id_to_code.get(site_id, str(site_id))
            test_filters = {
                "station__station_code__in": [site_code],
                "variable_names": ["WDDA"],
                "local_date_time__gte": start_datetime.isoformat(),
                "local_date_time__lte": end_datetime.isoformat(),
            }
            resp = sdk.get_data_values_for_site(filters=test_filters)
            if isinstance(resp, dict) and 'status_code' in resp:
                print(f"  {site_code}: FAILED - {resp.get('status_code')}")
            else:
                res_count = len(resp.get('results', [])) if isinstance(resp, dict) else 0
                print(f"  {site_code}: OK (empty response, {res_count} results)")

    print(f"\n{'='*70}")
    print("SUMMARY:")
    print(f"{'='*70}")
    print(f"  Filter used:   station__station_code__in")
    print(f"  Requested:     {len(site_ids)} site IDs ({len(site_codes)} unique codes)")
    print(f"  API total:     {total_count} station records")
    print(f"  Retrieved:     {len(all_results)} station records")
    print(f"  Pages fetched: {pages_fetched}")
    print(f"  With data:     {len(sites_with_data)} sites")
    print(f"  Without data:  {len(sites_without_data)} sites")

    if sites_with_data:
        latest_dates = [info['latest'] for info in sites_with_data.values() if info['latest']]
        if latest_dates:
            print(f"  Latest date:   {max(latest_dates)}")
            print(f"  Oldest date:   {min(latest_dates)}")

    # Final pagination verdict
    print(f"\n  PAGINATION: {'COMPLETE' if len(all_results) == total_count else 'INCOMPLETE - DATA MISSING!'}")
    if len(all_results) != total_count:
        print(f"  WARNING: Expected {total_count} records but only got {len(all_results)}")

    return sites_without_data, list(sites_with_data.keys())


def main():
    print("=" * 70)
    print("iEasyHydro HF SDK Diagnostic Test")
    print("=" * 70)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    # Show what mode we're running in
    if os.getenv('DIAGNOSE_422', '').lower() in ('1', 'true', 'yes'):
        print("\nMode: DIAGNOSE_422 - Testing each site individually (~2-5 min)")
    elif os.getenv('CHECK_ALL_SITES', '').lower() in ('1', 'true', 'yes'):
        print("\nMode: CHECK_ALL_SITES - Scanning all sites for recent data (~30 sec)")
    else:
        print("\nMode: Single site test (~5 sec)")

    sdk = test_sdk_connection()

    # Check if user wants to diagnose 422 errors
    if os.getenv('DIAGNOSE_422', '').lower() in ('1', 'true', 'yes'):
        # Get site IDs from environment or use defaults
        site_ids_str = os.getenv('TEST_SITE_IDS', '')
        if site_ids_str:
            site_ids = [int(x.strip()) for x in site_ids_str.split(',') if x.strip()]
        else:
            # Default: Get IDs from the SDK's discharge sites
            print("\nFetching all discharge sites from SDK...")
            discharge_sites = sdk.get_discharge_sites()
            site_ids = [s.get('id') for s in discharge_sites if s.get('id')]
            print(f"Found {len(site_ids)} site IDs")

            # Show structure of discharge_sites response
            print(f"\n{'='*70}")
            print("DISCHARGE SITES from sdk.get_discharge_sites():")
            print(f"{'='*70}")
            if discharge_sites:
                print(f"Record keys: {list(discharge_sites[0].keys())}")

                # Show sites
                print(f"\n{'ID':<6} {'Code':<10} {'Type':<8} {'Name':<40}")
                print("-" * 70)
                for site in discharge_sites:
                    site_id = site.get('id', '?')
                    code = site.get('site_code', '?')
                    site_type = site.get('site_type', '?')[:6]
                    name = site.get('official_name', site.get('name', '?'))[:38]
                    print(f"{site_id:<6} {code:<10} {site_type:<8} {name:<40}")

                # Summary
                codes = [s.get('site_code') for s in discharge_sites]
                unique_codes = set(codes)
                print(f"\nTotal sites: {len(discharge_sites)}, Unique codes: {len(unique_codes)}")
                if len(codes) != len(unique_codes):
                    print(f"WARNING: {len(codes) - len(unique_codes)} duplicate site_codes!")
                    from collections import Counter
                    duplicates = [(code, count) for code, count in Counter(codes).items() if count > 1]
                    for code, count in duplicates[:5]:
                        dup_sites = [s for s in discharge_sites if s.get('site_code') == code]
                        names = [s.get('official_name', '?')[:30] for s in dup_sites]
                        print(f"  {code} appears {count}x: {names}")
            print(f"{'='*70}\n")

        test_bulk_vs_individual(sdk, site_ids)
        return 0

    # Check if user wants to scan all sites
    if os.getenv('CHECK_ALL_SITES', '').lower() in ('1', 'true', 'yes'):
        max_age = int(os.getenv('MAX_AGE_DAYS', '20'))
        check_all_sites_for_recent_data(sdk, max_age_days=max_age)
        return 0

    site_code = os.getenv('TEST_SITE_CODE', '17462')
    days = int(os.getenv('TEST_DAYS', '20'))

    result = test_data_retrieval(sdk, site_code, days=days)

    print("\n" + "=" * 50)
    if result and len(result) > 0:
        print("TEST PASSED")
        return 0
    else:
        print("TEST FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(main())
