"""Verify converted daily runoff data against original source files.

Reads the two source files and the converted output, then checks:
1. Row counts match per year
2. Every discharge value in the sources appears in the output
3. No unexpected values were introduced
4. Date continuity (no gaps within covered periods)

Usage
-----
::

    cd apps/preprocessing_runoff
    uv run python dev_code/verify_conversion.py \\
        --data-dir "/path/to/data_forecast_tools/daily_runoff"
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from convert_daily_runoff import (
    FILE_CALENDAR,
    FILE_PIVOT,
    make_output_filename,
    read_calendar_file,
    read_pivot_file,
)


def load_converted(path: Path) -> pd.DataFrame:
    """Load the converted single-river Excel file into a DataFrame."""
    xls = pd.ExcelFile(path)
    frames = []
    for sheet in xls.sheet_names:
        df = pd.read_excel(xls, sheet, header=0, usecols=[0, 1], names=["date", "discharge"])
        frames.append(df)
    df = pd.concat(frames, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"], format="%d.%m.%Y")
    df["discharge"] = pd.to_numeric(df["discharge"], errors="coerce")
    return df.sort_values("date").reset_index(drop=True)


def verify(data_dir: Path) -> bool:
    """Run all verification checks. Returns True if all pass."""
    pivot_path = data_dir / FILE_PIVOT
    calendar_path = data_dir / FILE_CALENDAR
    converted_path = data_dir / make_output_filename()

    if not converted_path.exists():
        print(f"FAIL: Converted file not found: {converted_path}")
        return False

    # Load all three datasets
    print("Loading source files...")
    df_pivot = read_pivot_file(pivot_path)
    df_pivot["date"] = pd.to_datetime(df_pivot["date"])

    df_calendar = read_calendar_file(calendar_path)
    df_calendar["date"] = pd.to_datetime(df_calendar["date"])

    print("Loading converted file...")
    df_converted = load_converted(converted_path)

    # Combine sources (same logic as convert())
    df_source = pd.concat([df_pivot, df_calendar], ignore_index=True)
    df_source = (
        df_source.sort_values("date")
        .drop_duplicates(subset="date", keep="last")
        .reset_index(drop=True)
    )

    all_ok = True

    # --- Check 1: Total row count ---
    print("\n--- Check 1: Row counts ---")
    print(f"  Source (combined, deduplicated): {len(df_source)}")
    print(f"  Converted:                      {len(df_converted)}")
    if len(df_source) != len(df_converted):
        print("  FAIL: Row count mismatch!")
        all_ok = False
    else:
        print("  OK")

    # --- Check 2: Row counts per year ---
    print("\n--- Check 2: Row counts per year ---")
    src_years = df_source["date"].dt.year.value_counts().sort_index()
    conv_years = df_converted["date"].dt.year.value_counts().sort_index()

    for year in sorted(set(src_years.index) | set(conv_years.index)):
        src_n = src_years.get(year, 0)
        conv_n = conv_years.get(year, 0)
        status = "OK" if src_n == conv_n else "FAIL"
        if status == "FAIL":
            all_ok = False
        print(f"  {year}: source={src_n}, converted={conv_n}  {status}")

    # --- Check 3: Value-by-value comparison ---
    print("\n--- Check 3: Value-by-value comparison ---")
    merged = pd.merge(
        df_source,
        df_converted,
        on="date",
        how="outer",
        suffixes=("_src", "_conv"),
    )

    # Dates only in source
    only_source = merged[merged["discharge_conv"].isna() & merged["discharge_src"].notna()]
    if len(only_source) > 0:
        print(f"  FAIL: {len(only_source)} dates in source but not converted:")
        for _, row in only_source.head(10).iterrows():
            print(f"    {row['date'].strftime('%Y-%m-%d')}: {row['discharge_src']}")
        all_ok = False

    # Dates only in converted
    only_conv = merged[merged["discharge_src"].isna() & merged["discharge_conv"].notna()]
    if len(only_conv) > 0:
        print(f"  FAIL: {len(only_conv)} dates in converted but not source:")
        for _, row in only_conv.head(10).iterrows():
            print(f"    {row['date'].strftime('%Y-%m-%d')}: {row['discharge_conv']}")
        all_ok = False

    # Value mismatches (allow tiny float tolerance)
    both = merged.dropna(subset=["discharge_src", "discharge_conv"])
    mismatches = both[(both["discharge_src"] - both["discharge_conv"]).abs() > 0.01]
    if len(mismatches) > 0:
        print(f"  FAIL: {len(mismatches)} value mismatches:")
        for _, row in mismatches.head(10).iterrows():
            print(
                f"    {row['date'].strftime('%Y-%m-%d')}: "
                f"source={row['discharge_src']}, "
                f"converted={row['discharge_conv']}"
            )
        all_ok = False

    if len(only_source) == 0 and len(only_conv) == 0 and len(mismatches) == 0:
        print(f"  OK: All {len(both)} values match")

    # --- Check 4: Date continuity ---
    print("\n--- Check 4: Date continuity ---")
    # Check per year (except the last partial year)
    full_years = sorted(df_converted["date"].dt.year.unique())
    last_year = full_years[-1]
    gaps_found = False
    for year in full_years:
        year_dates = df_converted[df_converted["date"].dt.year == year]["date"].sort_values()
        if len(year_dates) < 2:
            continue

        expected_days = (year_dates.iloc[-1] - year_dates.iloc[0]).days + 1
        actual_days = len(year_dates)
        if actual_days != expected_days:
            label = " (partial)" if year == last_year else ""
            print(
                f"  {year}{label}: expected {expected_days} days "
                f"(from {year_dates.iloc[0].strftime('%m/%d')} "
                f"to {year_dates.iloc[-1].strftime('%m/%d')}), "
                f"got {actual_days}"
            )
            gaps_found = True
            if year != last_year:
                all_ok = False

    if not gaps_found:
        print("  OK: No gaps in any year")

    # --- Check 5: Basic statistics ---
    print("\n--- Check 5: Summary statistics ---")
    print(
        f"  Date range: {df_converted['date'].min().strftime('%Y-%m-%d')} "
        f"to {df_converted['date'].max().strftime('%Y-%m-%d')}"
    )
    print(
        f"  Discharge range: {df_converted['discharge'].min():.1f} "
        f"- {df_converted['discharge'].max():.1f} m3/s"
    )
    print(f"  Mean discharge: {df_converted['discharge'].mean():.1f} m3/s")
    print(f"  Missing values: {df_converted['discharge'].isna().sum()}")

    # --- Final verdict ---
    print(f"\n{'=' * 50}")
    if all_ok:
        print("ALL CHECKS PASSED")
    else:
        print("SOME CHECKS FAILED - review output above")
    print(f"{'=' * 50}")

    return all_ok


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify converted daily runoff against sources.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        required=True,
        help="Directory containing source and converted Excel files.",
    )
    args = parser.parse_args()

    ok = verify(args.data_dir)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
