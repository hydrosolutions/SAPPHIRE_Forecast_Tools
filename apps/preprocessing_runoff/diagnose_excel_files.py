#!/usr/bin/env python3
"""
Diagnostic script to compare Excel files between server and local environments.

This script inspects Excel files in the daily_runoff directory and reports:
- File names and sizes
- Sheet names and station codes
- Date ranges per station
- Data gaps (periods with no data)

Run this on both server and local to compare output.

Usage:
    python diagnose_excel_files.py /path/to/daily_runoff [--site-code 16059]
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd


def get_station_code_from_cell(xls, sheet_name):
    """Extract station code from cell A1 (first 5 chars if numeric)."""
    try:
        df = pd.read_excel(xls, sheet_name, nrows=1, usecols="A", header=None)
        full_name = str(df.iloc[0, 0])
        if full_name[:5].isdigit():
            return full_name[:5]
        return None
    except Exception:
        return None


def get_date_range_from_sheet(xls, sheet_name):
    """Get min/max dates and count of non-null values from a sheet."""
    try:
        df = pd.read_excel(xls, sheet_name, header=1, usecols=[0, 1], names=['date', 'discharge'])
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['discharge'] = pd.to_numeric(df['discharge'], errors='coerce')

        valid_data = df.dropna(subset=['date', 'discharge'])
        all_dates = df.dropna(subset=['date'])

        if valid_data.empty:
            return None, None, 0, len(all_dates)

        return valid_data['date'].min(), valid_data['date'].max(), len(valid_data), len(all_dates)
    except Exception as e:
        return None, None, 0, 0


def find_data_gaps(xls, sheet_name, gap_threshold_days=30):
    """Find gaps in the time series larger than threshold."""
    try:
        df = pd.read_excel(xls, sheet_name, header=1, usecols=[0, 1], names=['date', 'discharge'])
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['discharge'] = pd.to_numeric(df['discharge'], errors='coerce')

        valid_data = df.dropna(subset=['date', 'discharge']).sort_values('date')

        if len(valid_data) < 2:
            return []

        gaps = []
        dates = valid_data['date'].values
        for i in range(1, len(dates)):
            gap_days = (pd.Timestamp(dates[i]) - pd.Timestamp(dates[i-1])).days
            if gap_days > gap_threshold_days:
                gaps.append({
                    'start': pd.Timestamp(dates[i-1]).strftime('%Y-%m-%d'),
                    'end': pd.Timestamp(dates[i]).strftime('%Y-%m-%d'),
                    'gap_days': gap_days
                })

        return gaps
    except Exception:
        return []


def analyze_excel_file(filepath, target_codes=None):
    """Analyze a single Excel file."""
    results = {
        'filename': os.path.basename(filepath),
        'size_kb': os.path.getsize(filepath) / 1024,
        'modified': datetime.fromtimestamp(os.path.getmtime(filepath)).strftime('%Y-%m-%d %H:%M:%S'),
        'sheets': []
    }

    try:
        xls = pd.ExcelFile(filepath)

        for sheet_name in xls.sheet_names:
            code = get_station_code_from_cell(xls, sheet_name)

            # Skip if filtering by code and this isn't a match
            if target_codes and code not in target_codes:
                continue

            min_date, max_date, valid_count, total_rows = get_date_range_from_sheet(xls, sheet_name)
            gaps = find_data_gaps(xls, sheet_name)

            sheet_info = {
                'sheet_name': sheet_name,
                'station_code': code,
                'min_date': min_date.strftime('%Y-%m-%d') if min_date else None,
                'max_date': max_date.strftime('%Y-%m-%d') if max_date else None,
                'valid_records': valid_count,
                'total_rows': total_rows,
                'gaps_over_30_days': gaps
            }
            results['sheets'].append(sheet_info)

    except Exception as e:
        results['error'] = str(e)

    return results


def main():
    parser = argparse.ArgumentParser(description='Diagnose Excel files in daily_runoff directory')
    parser.add_argument('directory', help='Path to daily_runoff directory')
    parser.add_argument('--site-code', '-s', action='append', dest='site_codes',
                        help='Filter to specific site code(s). Can be specified multiple times.')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Show detailed output including all gaps')
    args = parser.parse_args()

    directory = Path(args.directory)
    if not directory.exists():
        print(f"Error: Directory '{directory}' does not exist")
        sys.exit(1)

    # Find all Excel files
    xlsx_files = list(directory.glob('*.xlsx'))
    xlsx_files = [f for f in xlsx_files if not f.name.startswith('~')]  # Skip temp files

    print(f"=" * 80)
    print(f"Excel File Diagnosis Report")
    print(f"Directory: {directory.absolute()}")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if args.site_codes:
        print(f"Filtering for site codes: {args.site_codes}")
    print(f"=" * 80)
    print(f"\nFound {len(xlsx_files)} Excel files\n")

    all_stations = {}

    for filepath in sorted(xlsx_files):
        print(f"\n--- {filepath.name} ---")
        result = analyze_excel_file(filepath, args.site_codes)

        print(f"  Size: {result['size_kb']:.1f} KB")
        print(f"  Modified: {result['modified']}")

        if 'error' in result:
            print(f"  ERROR: {result['error']}")
            continue

        if not result['sheets']:
            if args.site_codes:
                print(f"  No matching site codes found")
            else:
                print(f"  No valid sheets found")
            continue

        for sheet in result['sheets']:
            code = sheet['station_code'] or 'UNKNOWN'
            print(f"\n  Sheet: {sheet['sheet_name']}")
            print(f"    Station code: {code}")
            print(f"    Date range: {sheet['min_date']} to {sheet['max_date']}")
            print(f"    Valid records: {sheet['valid_records']} / {sheet['total_rows']} rows")

            if sheet['gaps_over_30_days']:
                print(f"    Gaps > 30 days: {len(sheet['gaps_over_30_days'])}")
                if args.verbose:
                    for gap in sheet['gaps_over_30_days']:
                        print(f"      - {gap['start']} to {gap['end']} ({gap['gap_days']} days)")

            # Aggregate by station
            if code != 'UNKNOWN':
                if code not in all_stations:
                    all_stations[code] = {
                        'min_date': sheet['min_date'],
                        'max_date': sheet['max_date'],
                        'total_records': sheet['valid_records'],
                        'sources': []
                    }
                else:
                    if sheet['min_date'] and (not all_stations[code]['min_date'] or sheet['min_date'] < all_stations[code]['min_date']):
                        all_stations[code]['min_date'] = sheet['min_date']
                    if sheet['max_date'] and (not all_stations[code]['max_date'] or sheet['max_date'] > all_stations[code]['max_date']):
                        all_stations[code]['max_date'] = sheet['max_date']
                    all_stations[code]['total_records'] += sheet['valid_records']
                all_stations[code]['sources'].append(f"{result['filename']}:{sheet['sheet_name']}")

    # Summary by station
    print(f"\n" + "=" * 80)
    print("SUMMARY BY STATION CODE")
    print("=" * 80)

    for code in sorted(all_stations.keys()):
        info = all_stations[code]
        print(f"\nStation {code}:")
        print(f"  Date range: {info['min_date']} to {info['max_date']}")
        print(f"  Total records: {info['total_records']}")
        print(f"  Sources: {len(info['sources'])} sheet(s)")
        if args.verbose:
            for src in info['sources']:
                print(f"    - {src}")

    # Check specifically for March-November gaps (the problem period)
    if args.site_codes:
        print(f"\n" + "=" * 80)
        print("DETAILED GAP ANALYSIS FOR MARCH-NOVEMBER")
        print("=" * 80)

        for filepath in sorted(xlsx_files):
            result = analyze_excel_file(filepath, args.site_codes)
            for sheet in result.get('sheets', []):
                if not sheet['gaps_over_30_days']:
                    continue
                for gap in sheet['gaps_over_30_days']:
                    # Check if gap overlaps March-November period
                    gap_start = datetime.strptime(gap['start'], '%Y-%m-%d')
                    gap_end = datetime.strptime(gap['end'], '%Y-%m-%d')
                    # March is month 3, November is month 11
                    if gap_start.month <= 11 and gap_end.month >= 3:
                        print(f"\n  Station {sheet['station_code']} ({result['filename']}):")
                        print(f"    Gap: {gap['start']} to {gap['end']} ({gap['gap_days']} days)")


if __name__ == '__main__':
    main()
