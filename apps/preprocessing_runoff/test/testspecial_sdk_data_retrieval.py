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

Usage:
------
    cd apps/preprocessing_runoff

    # Test a single site:
    ieasyhydroforecast_env_file_path=/path/to/.env uv run python test/testspecial_sdk_data_retrieval.py

    # Test a specific site:
    TEST_SITE_CODE=15013 ieasyhydroforecast_env_file_path=/path/to/.env uv run python test/testspecial_sdk_data_retrieval.py

    # Check all sites for recent data:
    CHECK_ALL_SITES=1 ieasyhydroforecast_env_file_path=/path/to/.env uv run python test/testspecial_sdk_data_retrieval.py

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


def main():
    print("=" * 50)
    print("iEasyHydro HF SDK Test")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 50)

    sdk = test_sdk_connection()

    # Check if user wants to scan all sites
    if os.getenv('CHECK_ALL_SITES', '').lower() in ('1', 'true', 'yes'):
        max_age = int(os.getenv('MAX_AGE_DAYS', '5'))
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
