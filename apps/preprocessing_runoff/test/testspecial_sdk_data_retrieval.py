#!/usr/bin/env python3
"""
Quick test script to verify iEasyHydro HF SDK data retrieval.

Tests that we can retrieve the latest N days of data for a gauge,
or scan all available sites to check data freshness.

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
    print(f"\n--- Data Retrieval: Gauge {site_code}, Last {days} Days ---")

    # Calculate date range (use timezone-aware UTC)
    from datetime import timezone
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days)

    # Format timestamps as ISO with Z suffix (as per SDK docs)
    start_str = start_date.strftime('%Y-%m-%dT00:00:00Z')
    end_str = end_date.strftime('%Y-%m-%dT23:59:59Z')

    print(f"Query: {start_str} to {end_str}")

    # Use filters as per SDK documentation
    # WDDA = Water Discharge Daily Average
    filters = {
        "site_codes": [site_code],
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
    print(f"\n--- Checking All Sites for Data < {max_age_days} Days Old ---")

    from datetime import timezone

    # Get all discharge sites
    discharge_sites = sdk.get_discharge_sites()
    print(f"Found {len(discharge_sites)} discharge sites")

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

    # Collect all site codes
    site_codes = [s.get('site_code') for s in discharge_sites if s.get('site_code')]

    # Query all sites at once
    filters = {
        "site_codes": site_codes,
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
    print(f"\n--- Diagnosing 422 Errors for {len(site_ids)} Site IDs ---")

    from datetime import timezone
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=10)
    start_str = start_date.strftime('%Y-%m-%dT00:00:00Z')
    end_str = end_date.strftime('%Y-%m-%dT23:59:59Z')

    # Build a map of site_id -> site_code
    print("Fetching site code mapping...")
    discharge_sites = sdk.get_discharge_sites()
    id_to_code = {s.get('id'): s.get('site_code', 'Unknown') for s in discharge_sites}

    failed_codes = []
    success_codes = []

    print(f"\nTesting site_ids filter (as used in preprocessing_runoff)...")
    print(f"Date range: {start_str} to {end_str}")
    print(f"\n{'Site Code':<12} {'Status':<12} {'Details'}")
    print("-" * 60)

    for site_id in site_ids:
        site_code = id_to_code.get(site_id, str(site_id))

        # Test with site_codes (correct API parameter)
        filters = {
            "site_codes": [site_code],
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


def test_bulk_vs_individual(sdk, site_ids):
    """
    Compare bulk request (all IDs at once) vs individual requests.

    This helps identify if the 422 error happens only with bulk requests.
    """
    print(f"\n--- Testing Bulk vs Individual Requests ---")

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
        print(f"\nUsing specified date range:")
    else:
        # Use lookback from today
        end_datetime = datetime.now(target_tz)
        lookback_days = int(os.getenv('TEST_LOOKBACK_DAYS', '120'))
        start_date = datetime.now().replace(hour=0, minute=1) - timedelta(days=lookback_days)
        start_datetime = target_tz.localize(start_date) if start_date.tzinfo is None else start_date
        print(f"\nUsing lookback of {lookback_days} days:")

    print(f"  Start: {start_datetime.strftime('%Y-%m-%d %H:%M')} ({target_tz})")
    print(f"  End:   {end_datetime.strftime('%Y-%m-%d %H:%M')} ({target_tz})")

    # Build a map of site_id -> site_code from the discharge sites
    print("\n   Fetching site code mapping...")
    discharge_sites = sdk.get_discharge_sites()
    id_to_code = {s.get('id'): s.get('site_code', 'Unknown') for s in discharge_sites}
    id_to_name = {s.get('id'): s.get('name', 'Unknown')[:30] for s in discharge_sites}

    # Convert site_ids to site_codes for the API
    site_codes_raw = [id_to_code.get(sid) for sid in site_ids if id_to_code.get(sid)]
    # Remove duplicates while preserving order
    seen = set()
    site_codes = []
    for code in site_codes_raw:
        if code not in seen:
            seen.add(code)
            site_codes.append(code)

    print(f"   Mapped {len(site_ids)} site IDs to {len(site_codes_raw)} site codes ({len(site_codes)} unique)")

    # Check for any None or empty codes
    invalid_codes = [c for c in site_codes if not c or c == 'Unknown']
    if invalid_codes:
        print(f"   WARNING: Found {len(invalid_codes)} invalid codes: {invalid_codes}")
        site_codes = [c for c in site_codes if c and c != 'Unknown']

    # Show first few codes for debugging
    print(f"   First 10 codes: {site_codes[:10]}")
    print(f"   All codes are strings: {all(isinstance(c, str) for c in site_codes)}")

    # Test bulk request using site_codes (the correct API parameter)
    # First, test what page_size values the API accepts
    print(f"\n   Testing what page_size values API accepts...")
    page_sizes_to_test = [10, 15, 20, 25, 30, 50, 100]
    max_working_page_size = 10  # default

    for ps in page_sizes_to_test:
        test_filters = {
            "site_codes": site_codes[:5],  # Use small subset
            "variable_names": ["WDDA"],
            "local_date_time__gte": start_datetime.isoformat(),
            "local_date_time__lte": end_datetime.isoformat(),
            "page_size": ps,
        }
        resp = sdk.get_data_values_for_site(filters=test_filters)
        if isinstance(resp, dict) and 'status_code' in resp:
            print(f"   page_size={ps}: FAILED ({resp.get('status_code')})")
            break
        else:
            count = len(resp.get('results', [])) if isinstance(resp, dict) else 0
            print(f"   page_size={ps}: OK ({count} results)")
            max_working_page_size = ps

    print(f"\n   --> Maximum working page_size: {max_working_page_size}")

    # Now try bulk with the working page_size
    page_sizes_to_try = [max_working_page_size]
    bulk_success = False
    all_results = []

    for page_size in page_sizes_to_try:
        print(f"\n1. Bulk request with {len(site_codes)} site codes (page_size={page_size})...")
        filters = {
            "site_codes": site_codes,
            "variable_names": ["WDDA"],
            "local_date_time__gte": start_datetime.isoformat(),
            "local_date_time__lte": end_datetime.isoformat(),
            "page": 1,
            "page_size": page_size,
        }

        response = sdk.get_data_values_for_site(filters=filters)

        if isinstance(response, dict) and 'status_code' in response:
            print(f"   FAILED with page_size={page_size}: {response.get('status_code')}")
            continue

        # Success! Fetch all pages
        bulk_success = True
        all_results = []
        page = 1
        while True:
            filters["page"] = page
            response = sdk.get_data_values_for_site(filters=filters)

            if isinstance(response, dict) and 'status_code' in response:
                print(f"   Page {page} failed, stopping pagination")
                break

            page_results = response.get('results', []) if isinstance(response, dict) else []
            all_results.extend(page_results)

            has_next = response.get('next') if isinstance(response, dict) else None
            total_count = response.get('count', 0) if isinstance(response, dict) else 0

            if page == 1:
                print(f"   Total count from API: {total_count}")

            if not has_next:
                break
            page += 1

        print(f"   SUCCESS: Got {len(all_results)} station records (after {page} page(s))")
        break

    # If all bulk requests failed, use parallel individual requests
    if not bulk_success:
        print(f"\n   All bulk requests failed. Using parallel individual requests...")
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def fetch_single_site(site_code):
            """Fetch data for a single site."""
            single_filters = {
                "site_codes": [site_code],
                "variable_names": ["WDDA"],
                "local_date_time__gte": start_datetime.isoformat(),
                "local_date_time__lte": end_datetime.isoformat(),
            }
            resp = sdk.get_data_values_for_site(filters=single_filters)
            if isinstance(resp, dict) and 'status_code' not in resp:
                return resp.get('results', [])
            return []

        all_results = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(fetch_single_site, code): code for code in site_codes}
            completed = 0
            for future in as_completed(futures):
                completed += 1
                if completed % 20 == 0:
                    print(f"   Progress: {completed}/{len(site_codes)} sites...")
                try:
                    results_for_site = future.result()
                    all_results.extend(results_for_site)
                except Exception as e:
                    print(f"   Error for {futures[future]}: {e}")

        print(f"   Parallel fetch complete: Got {len(all_results)} station records")

    # Analyze the response
    results = all_results
    print(f"\n   Total: {len(results)} station records")

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
        print("\nChecking if sites have WDD (morning) data instead of WDDA (daily avg)...")
        wdd_filters = {
            "site_codes": sites_without_data_codes,
            "variable_names": ["WDD"],
            "local_date_time__gte": start_datetime.isoformat(),
            "local_date_time__lte": end_datetime.isoformat(),
        }
        wdd_response = sdk.get_data_values_for_site(filters=wdd_filters)

        if isinstance(wdd_response, dict) and 'status_code' not in wdd_response:
            wdd_results = wdd_response.get('results', [])
            wdd_site_codes = [r.get('station_code') for r in wdd_results]
            wdd_value_counts = {}
            for station in wdd_results:
                code = station.get('station_code')
                count = sum(len(v.get('values', [])) for v in station.get('data', []))
                wdd_value_counts[code] = count

            print(f"   Found {len(wdd_results)} sites with WDD data:")
            for code, count in sorted(wdd_value_counts.items(), key=lambda x: -x[1])[:10]:
                print(f"     {code}: {count} WDD records")
            if len(wdd_value_counts) > 10:
                print(f"     ... and {len(wdd_value_counts) - 10} more")

            # Sites with neither WDDA nor WDD
            sites_with_wdd = set(wdd_site_codes)
            truly_empty = [c for c in sites_without_data_codes if c not in sites_with_wdd]
            if truly_empty:
                print(f"\n   Sites with NEITHER WDDA nor WDD data ({len(truly_empty)}):")
                print(f"     {truly_empty[:20]}")
        else:
            print(f"   WDD query failed: {wdd_response}")

        # Test if these sites cause the 422 individually
        print("\nTesting sites without data individually...")
        for site_id in sites_without_data[:5]:  # Test first 5 only
            site_code = id_to_code.get(site_id, str(site_id))
            test_filters = {
                "site_codes": [site_code],
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
    print(f"  Requested:     {len(site_ids)} site IDs")
    print(f"  With data:     {len(sites_with_data)} sites")
    print(f"  Without data:  {len(sites_without_data)} sites")

    if sites_with_data:
        latest_dates = [info['latest'] for info in sites_with_data.values() if info['latest']]
        if latest_dates:
            print(f"  Latest date:   {max(latest_dates)}")
            print(f"  Oldest date:   {min(latest_dates)}")

    return sites_without_data, list(sites_with_data.keys())


def main():
    print("=" * 50)
    print("iEasyHydro HF SDK Test")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 50)

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
