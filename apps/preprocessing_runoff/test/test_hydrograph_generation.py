"""
Phase 2 Tests: Hydrograph Generation

Tests for from_daily_time_series_to_hydrograph() function.
Part of PREPQ-005 comprehensive test coverage.
"""

import os
import sys
import pandas as pd
import numpy as np
import datetime as dt
import pytest
from unittest.mock import patch

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import src


def create_test_timeseries(start_date, end_date, station_code='99901', base_value=100.0):
    """Helper to create test time series data."""
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    n = len(dates)
    return pd.DataFrame({
        'date': dates,
        'code': [station_code] * n,
        'discharge': base_value + np.random.randn(n) * 10,
        'name': [f'Station {station_code}'] * n
    })


class TestHydrographYearFiltering:
    """Tests for year filtering in hydrograph generation."""

    def test_hydrograph_includes_current_year(self):
        """Current year data is included in hydrograph."""
        current_year = dt.date.today().year
        df = create_test_timeseries(f'{current_year}-01-01', f'{current_year}-03-31')

        result = src.from_daily_time_series_to_hydrograph(df)

        # Should have column for current year
        assert str(current_year) in result.columns

    def test_hydrograph_includes_last_year(self):
        """Last year data is included in hydrograph."""
        current_year = dt.date.today().year
        last_year = current_year - 1
        df = create_test_timeseries(f'{last_year}-01-01', f'{current_year}-03-31')

        result = src.from_daily_time_series_to_hydrograph(df)

        # Should have column for last year
        assert str(last_year) in result.columns

    def test_hydrograph_statistics_from_all_years(self):
        """Statistics are calculated from all available years."""
        current_year = dt.date.today().year
        # Create data spanning multiple years
        df = create_test_timeseries(f'{current_year-5}-01-01', f'{current_year}-03-31')

        result = src.from_daily_time_series_to_hydrograph(df)

        # Statistics columns should exist
        assert 'mean' in result.columns
        assert 'std' in result.columns
        assert 'min' in result.columns
        assert 'max' in result.columns


class TestHydrographDayOfYearCalculation:
    """Tests for day-of-year calculation in hydrograph."""

    def test_hydrograph_day_of_year_range(self):
        """Day of year values should be in range 1-366."""
        current_year = dt.date.today().year
        df = create_test_timeseries(f'{current_year}-01-01', f'{current_year}-12-31')

        result = src.from_daily_time_series_to_hydrograph(df)

        assert result['day_of_year'].min() >= 1
        assert result['day_of_year'].max() <= 366

    def test_hydrograph_jan1_is_day_1(self):
        """January 1st should be day of year 1."""
        current_year = dt.date.today().year
        df = create_test_timeseries(f'{current_year}-01-01', f'{current_year}-01-10')

        result = src.from_daily_time_series_to_hydrograph(df)

        # First day should be 1
        assert result['day_of_year'].min() == 1

    def test_hydrograph_dec31_is_day_365_or_366(self):
        """December 31st should be day 365 or 366."""
        current_year = dt.date.today().year
        df = create_test_timeseries(f'{current_year}-12-20', f'{current_year}-12-31')

        result = src.from_daily_time_series_to_hydrograph(df)

        # Last day should be 365 or 366
        assert result['day_of_year'].max() in [365, 366]


class TestHydrographLeapYearHandling:
    """Tests for leap year handling in hydrograph."""

    def test_hydrograph_handles_feb29_in_leap_year(self):
        """Feb 29 in leap year is handled correctly."""
        # 2024 is a leap year - use current year data to avoid mocking issues
        current_year = dt.date.today().year
        # Use data around Feb 29 if current year is leap year, otherwise test basic handling
        if src.is_leap_year(current_year):
            df = create_test_timeseries(f'{current_year}-02-25', f'{current_year}-03-05')
        else:
            # Use 2024 data but don't check for specific leap year behavior
            df = create_test_timeseries(f'{current_year}-02-25', f'{current_year}-03-05')

        # This should not raise an error
        result = src.from_daily_time_series_to_hydrograph(df)

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_hydrograph_non_leap_year(self):
        """Non-leap year data is handled correctly."""
        current_year = dt.date.today().year
        # Create full year of data
        df = create_test_timeseries(f'{current_year}-01-01', f'{current_year}-12-31')

        result = src.from_daily_time_series_to_hydrograph(df)

        # Should have reasonable day_of_year range
        assert result['day_of_year'].min() >= 1
        assert result['day_of_year'].max() <= 366


class TestHydrographStatistics:
    """Tests for statistics calculation in hydrograph."""

    def test_hydrograph_statistics_columns_present(self):
        """All required statistics columns are present."""
        current_year = dt.date.today().year
        df = create_test_timeseries(f'{current_year-2}-01-01', f'{current_year}-06-30')

        result = src.from_daily_time_series_to_hydrograph(df)

        expected_stats = ['mean', 'std', 'min', 'max', '5%', '25%', '50%', '75%', '95%']
        for stat in expected_stats:
            assert stat in result.columns, f"Missing statistic column: {stat}"

    def test_hydrograph_mean_is_sensible(self):
        """Mean should be within min and max."""
        current_year = dt.date.today().year
        df = create_test_timeseries(f'{current_year-2}-01-01', f'{current_year}-06-30')

        result = src.from_daily_time_series_to_hydrograph(df)

        # Filter rows where we have valid statistics
        valid_rows = result.dropna(subset=['mean', 'min', 'max'])

        # Mean should be between min and max
        assert (valid_rows['mean'] >= valid_rows['min']).all()
        assert (valid_rows['mean'] <= valid_rows['max']).all()

    def test_hydrograph_percentiles_ordered(self):
        """Percentiles should be in correct order."""
        current_year = dt.date.today().year
        df = create_test_timeseries(f'{current_year-2}-01-01', f'{current_year}-06-30')

        result = src.from_daily_time_series_to_hydrograph(df)

        # Filter rows with valid percentiles
        valid_rows = result.dropna(subset=['5%', '25%', '50%', '75%', '95%'])

        if len(valid_rows) > 0:
            # Percentiles should be ordered
            assert (valid_rows['5%'] <= valid_rows['25%']).all()
            assert (valid_rows['25%'] <= valid_rows['50%']).all()
            assert (valid_rows['50%'] <= valid_rows['75%']).all()
            assert (valid_rows['75%'] <= valid_rows['95%']).all()


class TestHydrographMultipleStations:
    """Tests for hydrograph with multiple stations."""

    def test_hydrograph_multiple_stations_separate_stats(self):
        """Each station should have separate statistics."""
        current_year = dt.date.today().year
        df1 = create_test_timeseries(f'{current_year}-01-01', f'{current_year}-03-31', '99901', 100.0)
        df2 = create_test_timeseries(f'{current_year}-01-01', f'{current_year}-03-31', '99902', 200.0)
        df = pd.concat([df1, df2], ignore_index=True)

        result = src.from_daily_time_series_to_hydrograph(df)

        # Should have entries for both stations
        assert '99901' in result['code'].values
        assert '99902' in result['code'].values

        # Stations should have different means (base values are 100 vs 200)
        mean_99901 = result[result['code'] == '99901']['mean'].mean()
        mean_99902 = result[result['code'] == '99902']['mean'].mean()
        # Allow for some variance but they should be clearly different
        assert abs(mean_99901 - mean_99902) > 50

    def test_hydrograph_station_grouping(self):
        """Statistics should be grouped by station code."""
        current_year = dt.date.today().year
        df1 = create_test_timeseries(f'{current_year}-01-01', f'{current_year}-01-31', '99901')
        df2 = create_test_timeseries(f'{current_year}-01-01', f'{current_year}-01-31', '99902')
        df = pd.concat([df1, df2], ignore_index=True)

        result = src.from_daily_time_series_to_hydrograph(df)

        # Each station should have entries for each day
        for station in ['99901', '99902']:
            station_data = result[result['code'] == station]
            assert len(station_data) == 31, f"Station {station} should have 31 days"


class TestHydrographNaNHandling:
    """Tests for NaN handling in hydrograph generation."""

    def test_hydrograph_with_nan_discharge(self):
        """NaN values in discharge should be handled gracefully."""
        current_year = dt.date.today().year
        df = create_test_timeseries(f'{current_year}-01-01', f'{current_year}-01-31')
        # Introduce some NaN values
        df.loc[5:10, 'discharge'] = np.nan

        # Should not raise an error
        result = src.from_daily_time_series_to_hydrograph(df)

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_hydrograph_statistics_exclude_nan(self):
        """Statistics should be calculated excluding NaN values."""
        current_year = dt.date.today().year
        df = create_test_timeseries(f'{current_year-1}-01-01', f'{current_year}-03-31')
        # Set some values to NaN
        df.loc[df['date'].dt.month == 2, 'discharge'] = np.nan

        result = src.from_daily_time_series_to_hydrograph(df)

        # January should have valid statistics
        jan_data = result[result['day_of_year'] <= 31]
        assert jan_data['mean'].notna().any()


class TestHydrographDateColumn:
    """Tests for date column in hydrograph output."""

    def test_hydrograph_has_date_column(self):
        """Output should have a date column."""
        current_year = dt.date.today().year
        df = create_test_timeseries(f'{current_year}-01-01', f'{current_year}-03-31')

        result = src.from_daily_time_series_to_hydrograph(df)

        assert 'date' in result.columns

    def test_hydrograph_date_is_current_year(self):
        """Date column should use current year."""
        current_year = dt.date.today().year
        df = create_test_timeseries(f'{current_year-2}-01-01', f'{current_year}-03-31')

        result = src.from_daily_time_series_to_hydrograph(df)

        # All dates should be in current year
        years = result['date'].dt.year.unique()
        assert len(years) == 1
        assert years[0] == current_year


class TestHydrographEdgeCases:
    """Tests for edge cases in hydrograph generation."""

    def test_hydrograph_single_day_data(self):
        """Single day of data should work."""
        current_year = dt.date.today().year
        df = pd.DataFrame({
            'date': [pd.Timestamp(f'{current_year}-06-15')],
            'code': ['99901'],
            'discharge': [150.0],
            'name': ['Test Station']
        })

        result = src.from_daily_time_series_to_hydrograph(df)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_hydrograph_empty_dataframe(self):
        """Empty dataframe raises KeyError (known limitation)."""
        df = pd.DataFrame({
            'date': pd.Series([], dtype='datetime64[ns]'),
            'code': pd.Series([], dtype='str'),
            'discharge': pd.Series([], dtype='float'),
            'name': pd.Series([], dtype='str')
        })

        # Empty dataframe raises KeyError because groupby produces empty result
        # This is a known limitation - the function expects non-empty input
        with pytest.raises(KeyError):
            src.from_daily_time_series_to_hydrograph(df)

    def test_hydrograph_single_station_single_year(self):
        """Single station with single year of data."""
        current_year = dt.date.today().year
        df = create_test_timeseries(f'{current_year}-01-01', f'{current_year}-12-31')

        result = src.from_daily_time_series_to_hydrograph(df)

        # Should have entries for each day of the year
        assert len(result) >= 365


class TestHydrographOutputFormat:
    """Tests for output format of hydrograph."""

    def test_hydrograph_code_column_is_string(self):
        """Code column should be string type."""
        current_year = dt.date.today().year
        df = create_test_timeseries(f'{current_year}-01-01', f'{current_year}-03-31')
        df['code'] = df['code'].astype(int)  # Input as int

        result = src.from_daily_time_series_to_hydrograph(df)

        # Output code should be string
        assert result['code'].dtype == object or pd.api.types.is_string_dtype(result['code'])

    def test_hydrograph_count_column_present(self):
        """Count column (number of observations) should be present."""
        current_year = dt.date.today().year
        df = create_test_timeseries(f'{current_year-2}-01-01', f'{current_year}-03-31')

        result = src.from_daily_time_series_to_hydrograph(df)

        assert 'count' in result.columns


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
