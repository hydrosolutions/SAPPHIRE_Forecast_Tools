"""
Tests for the filter_roughly_for_outliers function, specifically for the
seasonal data preservation bug fix (PREPQ-005 related).

Bug description:
- The original function grouped data by (Code, season) and reindexed each group
- The 'winter' group (Dec, Jan, Feb) had a date range spanning the full year
- Reindexing created NaN rows for all months including Mar-Nov
- When groups were concatenated and deduplicated with keep='last', winter's NaN
  values overwrote valid spring/summer/autumn data

Fix:
- Separate IQR filtering (per season) from reindex/interpolation (per station)
- IQR filtering still uses seasonal groups for better statistics
- Reindex happens per station code, not per seasonal group
"""

import numpy as np
import pandas as pd
import pytest
import sys
import os

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import src


class TestSeasonalDataPreservation:
    """Tests that verify data from all seasons is preserved after filtering."""

    def test_winter_only_data_preserved(self):
        """Test that winter-only data is preserved without creating spurious NaN rows."""
        # Data only in January and February (winter)
        dates = pd.date_range('2020-01-01', '2020-02-28', freq='D')
        df = pd.DataFrame({
            'Code': 16059,
            'date': dates,
            'Q_m3s': np.random.uniform(100, 200, len(dates))
        })

        result = src.filter_roughly_for_outliers(df)

        # All original dates should have data
        for date in dates:
            val = result[(result['Code'] == 16059) & (result['date'] == date)]['Q_m3s'].values
            assert len(val) == 1, f"Missing row for {date}"
            assert not pd.isna(val[0]), f"Unexpected NaN for {date}"

    def test_autumn_data_not_overwritten_by_winter(self):
        """Test that autumn data (Nov) is NOT overwritten by winter group's NaN values.

        This is the core bug that was fixed. The old code would:
        1. Process autumn group (Nov data) -> valid data
        2. Process winter group (Dec, Jan, Feb) -> reindex creates NaN for Nov
        3. Concatenate and keep='last' -> winter's NaN overwrites autumn's valid data
        """
        # Data in Jan, Feb (winter) and Nov (autumn) and Dec (winter)
        dates = (
            pd.date_range('2020-01-01', '2020-02-28', freq='D').tolist() +  # Winter
            pd.date_range('2020-11-01', '2020-11-30', freq='D').tolist() +  # Autumn
            pd.date_range('2020-12-01', '2020-12-31', freq='D').tolist()    # Winter
        )
        df = pd.DataFrame({
            'Code': 16059,
            'date': dates,
            'Q_m3s': np.random.uniform(100, 200, len(dates))
        })

        result = src.filter_roughly_for_outliers(df)

        # November data MUST be preserved (this was the bug)
        nov_data = result[(result['Code'] == 16059) & (result['date'].dt.month == 11)]
        assert len(nov_data) == 30, f"Expected 30 November rows, got {len(nov_data)}"
        assert nov_data['Q_m3s'].notna().sum() == 30, \
            f"Expected 30 November rows with data, got {nov_data['Q_m3s'].notna().sum()}"

    def test_spring_summer_autumn_data_preserved(self):
        """Test that spring (Mar-May), summer (Jun-Aug), and autumn (Sep-Nov) data
        is preserved when winter data spans the full year.
        """
        # Data for all seasons
        dates = (
            pd.date_range('2020-01-01', '2020-01-31', freq='D').tolist() +  # Winter (Jan)
            pd.date_range('2020-03-01', '2020-03-31', freq='D').tolist() +  # Spring
            pd.date_range('2020-06-01', '2020-06-30', freq='D').tolist() +  # Summer
            pd.date_range('2020-09-01', '2020-09-30', freq='D').tolist() +  # Autumn
            pd.date_range('2020-12-01', '2020-12-31', freq='D').tolist()    # Winter (Dec)
        )
        df = pd.DataFrame({
            'Code': 16059,
            'date': dates,
            'Q_m3s': np.random.uniform(100, 200, len(dates))
        })

        result = src.filter_roughly_for_outliers(df)

        # Check each seasonal month has data preserved
        for month, expected_days in [(1, 31), (3, 31), (6, 30), (9, 30), (12, 31)]:
            month_data = result[(result['Code'] == 16059) & (result['date'].dt.month == month)]
            with_data = month_data['Q_m3s'].notna().sum()
            assert with_data == expected_days, \
                f"Month {month}: expected {expected_days} rows with data, got {with_data}"

    def test_multiple_stations_seasonal_data_preserved(self):
        """Test that seasonal data is preserved for multiple stations."""
        stations = [16059, 15189, 17123]
        dfs = []

        for code in stations:
            dates = (
                pd.date_range('2020-01-01', '2020-02-28', freq='D').tolist() +
                pd.date_range('2020-11-01', '2020-12-31', freq='D').tolist()
            )
            dfs.append(pd.DataFrame({
                'Code': code,
                'date': dates,
                'Q_m3s': np.random.uniform(100, 200, len(dates))
            }))

        df = pd.concat(dfs, ignore_index=True)
        result = src.filter_roughly_for_outliers(df)

        # Check November data preserved for each station
        for code in stations:
            nov_data = result[(result['Code'] == code) & (result['date'].dt.month == 11)]
            assert nov_data['Q_m3s'].notna().sum() == 30, \
                f"Station {code}: November data not preserved"


class TestReindexBehavior:
    """Tests for the reindex behavior (per station, not per season)."""

    def test_reindex_fills_gaps_within_station(self):
        """Test that reindex fills date gaps within a station's data range."""
        # Data with a gap
        dates = (
            pd.date_range('2020-01-01', '2020-01-10', freq='D').tolist() +
            pd.date_range('2020-01-15', '2020-01-20', freq='D').tolist()  # Gap: Jan 11-14
        )
        df = pd.DataFrame({
            'Code': 16059,
            'date': dates,
            'Q_m3s': np.random.uniform(100, 200, len(dates))
        })

        result = src.filter_roughly_for_outliers(df)

        # Result should have continuous dates from Jan 1 to Jan 20
        result_dates = result[result['Code'] == 16059]['date'].sort_values()
        expected_dates = pd.date_range('2020-01-01', '2020-01-20', freq='D')
        assert len(result_dates) == len(expected_dates), \
            f"Expected {len(expected_dates)} dates, got {len(result_dates)}"

    def test_interpolation_fills_small_gaps(self):
        """Test that 2-day gaps are interpolated."""
        # Data with a 2-day gap
        dates = ['2020-01-01', '2020-01-02', '2020-01-05', '2020-01-06']
        df = pd.DataFrame({
            'Code': 16059,
            'date': pd.to_datetime(dates),
            'Q_m3s': [100.0, 110.0, 130.0, 140.0]  # Gap on Jan 3-4
        })

        result = src.filter_roughly_for_outliers(df)

        # Jan 3 and 4 should be interpolated
        jan3 = result[(result['Code'] == 16059) & (result['date'] == '2020-01-03')]['Q_m3s'].values[0]
        jan4 = result[(result['Code'] == 16059) & (result['date'] == '2020-01-04')]['Q_m3s'].values[0]

        assert not pd.isna(jan3), "Jan 3 should be interpolated"
        assert not pd.isna(jan4), "Jan 4 should be interpolated"

    def test_large_gaps_remain_nan(self):
        """Test that gaps larger than interpolation limit remain partially NaN.

        Note: The function applies interpolation with limit=2 twice (once after
        outlier detection, once after 300% change detection), so a gap needs to
        be larger than 4 days to have any NaN values remaining in the middle.
        """
        # Data with a 10-day gap (large enough to have NaN in middle after 2x limit=2 interpolation)
        dates = ['2020-01-01', '2020-01-02', '2020-01-13', '2020-01-14']
        df = pd.DataFrame({
            'Code': 16059,
            'date': pd.to_datetime(dates),
            'Q_m3s': [100.0, 105.0, 150.0, 155.0]  # Gap on Jan 3-12 (10 days)
        })

        result = src.filter_roughly_for_outliers(df)

        # Jan 7 (middle of large gap) should still be NaN
        jan7 = result[(result['Code'] == 16059) & (result['date'] == '2020-01-07')]['Q_m3s'].values[0]
        assert pd.isna(jan7), "Middle of large gap should not be interpolated"

        # Count NaN values in the gap - should have some remaining
        gap_data = result[(result['Code'] == 16059) &
                         (result['date'] >= '2020-01-03') &
                         (result['date'] <= '2020-01-12')]
        nan_count = gap_data['Q_m3s'].isna().sum()
        assert nan_count > 0, "Large gap should have some NaN values remaining"


class TestIQRFiltering:
    """Tests for the IQR-based outlier filtering."""

    def test_iqr_uses_seasonal_statistics(self):
        """Test that IQR filtering uses seasonal statistics (not global).

        This ensures the seasonal grouping for IQR calculation is preserved.
        """
        # Create data where summer values are naturally higher than winter
        winter_dates = pd.date_range('2020-01-01', '2020-02-28', freq='D')
        summer_dates = pd.date_range('2020-06-01', '2020-08-31', freq='D')

        df = pd.DataFrame({
            'Code': 16059,
            'date': list(winter_dates) + list(summer_dates),
            'Q_m3s': (
                list(np.random.uniform(50, 100, len(winter_dates))) +  # Winter: low values
                list(np.random.uniform(200, 300, len(summer_dates)))   # Summer: high values
            )
        })

        result = src.filter_roughly_for_outliers(df)

        # Neither winter nor summer should have values removed as outliers
        # (they're only outliers relative to each other, not within their seasons)
        winter_result = result[(result['Code'] == 16059) & (result['date'].dt.month.isin([1, 2]))]
        summer_result = result[(result['Code'] == 16059) & (result['date'].dt.month.isin([6, 7, 8]))]

        # Most values should be preserved (some might be flagged by 300% change rule)
        assert winter_result['Q_m3s'].notna().sum() > len(winter_dates) * 0.9
        assert summer_result['Q_m3s'].notna().sum() > len(summer_dates) * 0.9


class TestEdgeCases:
    """Edge case tests."""

    def test_single_day_data(self):
        """Test handling of single-day data per station."""
        df = pd.DataFrame({
            'Code': [16059],
            'date': pd.to_datetime(['2020-06-15']),
            'Q_m3s': [150.0]
        })

        result = src.filter_roughly_for_outliers(df)

        assert len(result) == 1
        assert result['Q_m3s'].values[0] == 150.0

    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        df = pd.DataFrame({
            'Code': pd.Series([], dtype=int),
            'date': pd.Series([], dtype='datetime64[ns]'),
            'Q_m3s': pd.Series([], dtype=float)
        })

        result = src.filter_roughly_for_outliers(df)
        assert len(result) == 0

    def test_all_same_values(self):
        """Test handling when all values are identical (IQR=0)."""
        dates = pd.date_range('2020-01-01', '2020-01-31', freq='D')
        df = pd.DataFrame({
            'Code': 16059,
            'date': dates,
            'Q_m3s': [100.0] * len(dates)  # All same value
        })

        result = src.filter_roughly_for_outliers(df)

        # All values should be preserved (no outliers when IQR=0)
        assert result['Q_m3s'].notna().sum() == len(dates)

    def test_nan_in_input_preserved(self):
        """Test that NaN values in input are handled correctly."""
        dates = pd.date_range('2020-01-01', '2020-01-10', freq='D')
        values = [100.0, 110.0, np.nan, 130.0, 140.0, 150.0, 160.0, 170.0, 180.0, 190.0]
        df = pd.DataFrame({
            'Code': 16059,
            'date': dates,
            'Q_m3s': values
        })

        result = src.filter_roughly_for_outliers(df)

        # The NaN on day 3 should be interpolated (gap of 1 day)
        day3 = result[(result['Code'] == 16059) & (result['date'] == '2020-01-03')]['Q_m3s'].values[0]
        assert not pd.isna(day3), "Single-day NaN should be interpolated"


class TestMultiYearData:
    """Tests with multi-year data spanning multiple seasons."""

    def test_multi_year_seasonal_data_preserved(self):
        """Test that seasonal data is preserved across multiple years."""
        dfs = []
        for year in [2019, 2020, 2021]:
            for month, days in [(1, 31), (6, 30), (11, 30), (12, 31)]:
                # Handle month boundaries correctly
                if month == 2:
                    end_day = 29 if year % 4 == 0 else 28
                else:
                    end_day = days
                dates = pd.date_range(f'{year}-{month:02d}-01', periods=end_day, freq='D')
                dfs.append(pd.DataFrame({
                    'Code': 16059,
                    'date': dates,
                    'Q_m3s': np.random.uniform(100, 200, len(dates))
                }))

        df = pd.concat(dfs, ignore_index=True)
        result = src.filter_roughly_for_outliers(df)

        # Check that each year's seasonal data is preserved
        for year in [2019, 2020, 2021]:
            for month in [1, 6, 11, 12]:
                month_data = result[
                    (result['Code'] == 16059) &
                    (result['date'].dt.year == year) &
                    (result['date'].dt.month == month)
                ]
                # Most data should be preserved
                assert month_data['Q_m3s'].notna().sum() > 20, \
                    f"Year {year}, month {month}: too many values lost"


class TestRegressionBug:
    """Regression tests for the specific bug that was fixed."""

    def test_regression_november_data_not_lost(self):
        """Regression test: November data should NOT be lost when December data exists.

        This is the exact scenario from the production bug where station 16059
        had data in Jan-Feb and Nov-Dec but the output showed Nov as NaN.
        """
        # Exact reproduction of the bug scenario
        dates = (
            pd.date_range('2020-01-01', '2020-02-28', freq='D').tolist() +
            pd.date_range('2020-11-01', '2020-12-31', freq='D').tolist()
        )
        df = pd.DataFrame({
            'Code': 16059,
            'date': dates,
            'Q_m3s': np.random.uniform(100, 200, len(dates))
        })

        result = src.filter_roughly_for_outliers(df)

        # This was the bug: November had 0 rows with data
        nov_with_data = result[
            (result['Code'] == 16059) &
            (result['date'].dt.month == 11)
        ]['Q_m3s'].notna().sum()

        assert nov_with_data == 30, \
            f"REGRESSION: November data lost! Expected 30 rows with data, got {nov_with_data}"

    def test_regression_all_seasons_have_data_when_input_has_data(self):
        """Regression test: All seasons that have input data should have output data."""
        # Create data for all seasons
        season_months = {
            'winter': [1, 2, 12],
            'spring': [3, 4, 5],
            'summer': [6, 7, 8],
            'autumn': [9, 10, 11]
        }

        dfs = []
        for month in range(1, 13):
            dates = pd.date_range(f'2020-{month:02d}-01', periods=28, freq='D')
            dfs.append(pd.DataFrame({
                'Code': 16059,
                'date': dates,
                'Q_m3s': np.random.uniform(100, 200, len(dates))
            }))

        df = pd.concat(dfs, ignore_index=True)
        result = src.filter_roughly_for_outliers(df)

        # Every month should have data preserved
        for month in range(1, 13):
            month_with_data = result[
                (result['Code'] == 16059) &
                (result['date'].dt.month == month)
            ]['Q_m3s'].notna().sum()

            assert month_with_data >= 25, \
                f"Month {month}: expected >=25 rows with data, got {month_with_data}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
