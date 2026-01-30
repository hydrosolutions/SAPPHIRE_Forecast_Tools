#!/usr/bin/env python3
"""
Add fake recent data to runoff_day.csv and hydrograph_day.csv for testing.

This script:
1. Reads the current data files
2. Finds the last date for each station
3. Generates fake data from last date to today
4. Backs up original files
5. Updates the CSV files

Usage:
    cd apps/preprocessing_runoff
    python test/add_fake_recent_data.py /path/to/intermediate_data

    # Or with environment variable:
    python test/add_fake_recent_data.py ~/Documents/GitHub/kyg_data_forecast_tools/intermediate_data
"""

import os
import sys
import shutil
import random
from datetime import datetime, timedelta
import pandas as pd


def backup_file(filepath):
    """Create a backup of a file with .bak extension."""
    backup_path = filepath + '.bak'
    if os.path.exists(filepath):
        shutil.copy2(filepath, backup_path)
        print(f"  Backed up: {backup_path}")
    return backup_path


def generate_fake_value(last_value, variation_pct=5):
    """Generate a fake value with slight random variation."""
    if pd.isna(last_value) or last_value == 0:
        return last_value
    variation = last_value * (variation_pct / 100)
    fake_value = last_value + random.uniform(-variation, variation)
    return round(fake_value, 2)


def add_fake_runoff_data(data_path, target_date=None):
    """Add fake data to runoff_day.csv from last known date to target_date."""

    runoff_file = os.path.join(data_path, 'runoff_day.csv')

    if not os.path.exists(runoff_file):
        print(f"ERROR: {runoff_file} not found")
        return None

    print(f"\n--- Processing runoff_day.csv ---")

    # Read current data
    df = pd.read_csv(runoff_file)
    df['date'] = pd.to_datetime(df['date'])

    print(f"  Current rows: {len(df)}")
    print(f"  Stations: {df['code'].nunique()}")
    print(f"  Date range: {df['date'].min().date()} to {df['date'].max().date()}")

    # Target date (default: today)
    if target_date is None:
        target_date = datetime.now().date()
    elif isinstance(target_date, str):
        target_date = datetime.strptime(target_date, '%Y-%m-%d').date()

    # Get last date and value for each station
    last_data = df.groupby('code').agg({
        'date': 'max',
        'discharge': 'last'
    }).reset_index()

    # Find global last date
    global_last_date = df['date'].max().date()

    print(f"  Last data date: {global_last_date}")
    print(f"  Target date: {target_date}")

    if global_last_date >= target_date:
        print("  Data is already current, no fake data needed")
        return df

    # Generate fake data for each station
    new_rows = []
    current_date = global_last_date + timedelta(days=1)

    while current_date <= target_date:
        for _, row in last_data.iterrows():
            code = row['code']
            last_value = row['discharge']

            # Get the last known value for this specific station
            station_last = df[df['code'] == code]['discharge'].iloc[-1]
            fake_value = generate_fake_value(station_last)

            new_rows.append({
                'code': code,
                'date': current_date,
                'discharge': fake_value
            })

        current_date += timedelta(days=1)

    print(f"  Generated {len(new_rows)} fake rows")

    # Backup and append
    backup_file(runoff_file)

    new_df = pd.DataFrame(new_rows)
    combined_df = pd.concat([df, new_df], ignore_index=True)
    combined_df['date'] = pd.to_datetime(combined_df['date']).dt.strftime('%Y-%m-%d')
    combined_df.to_csv(runoff_file, index=False)

    print(f"  New total rows: {len(combined_df)}")
    print(f"  Updated: {runoff_file}")

    return combined_df


def add_fake_hydrograph_data(data_path, runoff_df, target_date=None):
    """Update hydrograph_day.csv with fake 2026 values."""

    hydrograph_file = os.path.join(data_path, 'hydrograph_day.csv')

    if not os.path.exists(hydrograph_file):
        print(f"ERROR: {hydrograph_file} not found")
        return None

    print(f"\n--- Processing hydrograph_day.csv ---")

    # Read current data
    df = pd.read_csv(hydrograph_file)

    print(f"  Current rows: {len(df)}")

    # Target date (default: today)
    if target_date is None:
        target_date = datetime.now().date()
    elif isinstance(target_date, str):
        target_date = datetime.strptime(target_date, '%Y-%m-%d').date()

    # Get the fake data we just added for 2026
    if runoff_df is not None:
        runoff_df = runoff_df.copy()
        runoff_df['date'] = pd.to_datetime(runoff_df['date'])
        fake_2026 = runoff_df[runoff_df['date'].dt.year == 2026].copy()
        fake_2026['day_of_year'] = fake_2026['date'].dt.dayofyear

        print(f"  Fake 2026 data rows: {len(fake_2026)}")

        if len(fake_2026) == 0:
            print("  No 2026 data to update")
            return df

        # Backup
        backup_file(hydrograph_file)

        # Update the 2026 column for matching (code, day_of_year)
        updates = 0
        for _, fake_row in fake_2026.iterrows():
            code = fake_row['code']
            doy = fake_row['day_of_year']
            value = fake_row['discharge']

            mask = (df['code'] == code) & (df['day_of_year'] == doy)
            if mask.any():
                df.loc[mask, '2026'] = value
                updates += 1

        print(f"  Updated {updates} rows in 2026 column")

        # Save
        df.to_csv(hydrograph_file, index=False)
        print(f"  Updated: {hydrograph_file}")

    return df


def restore_backups(data_path):
    """Restore original files from backups."""
    print(f"\n--- Restoring backups ---")

    for filename in ['runoff_day.csv', 'hydrograph_day.csv']:
        filepath = os.path.join(data_path, filename)
        backup_path = filepath + '.bak'

        if os.path.exists(backup_path):
            shutil.copy2(backup_path, filepath)
            print(f"  Restored: {filepath}")
        else:
            print(f"  No backup found: {backup_path}")


def main():
    print("=" * 60)
    print("Add Fake Recent Data for Testing")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    # Parse arguments
    if len(sys.argv) < 2:
        print("\nUsage: python add_fake_recent_data.py <intermediate_data_path> [--restore]")
        print("\nExample:")
        print("  python add_fake_recent_data.py ~/Documents/GitHub/kyg_data_forecast_tools/intermediate_data")
        print("  python add_fake_recent_data.py ~/Documents/GitHub/kyg_data_forecast_tools/intermediate_data --restore")
        sys.exit(1)

    data_path = os.path.expanduser(sys.argv[1])

    if not os.path.isdir(data_path):
        print(f"ERROR: Directory not found: {data_path}")
        sys.exit(1)

    print(f"\nData path: {data_path}")

    # Check for restore flag
    if '--restore' in sys.argv:
        restore_backups(data_path)
        print("\nDone!")
        return 0

    # Add fake data
    target_date = datetime.now().date()
    print(f"Target date: {target_date}")

    # Process runoff_day.csv
    runoff_df = add_fake_runoff_data(data_path, target_date)

    # Process hydrograph_day.csv
    add_fake_hydrograph_data(data_path, runoff_df, target_date)

    print("\n" + "=" * 60)
    print("Done! Fake data added.")
    print("\nTo restore original files:")
    print(f"  python {sys.argv[0]} {data_path} --restore")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
