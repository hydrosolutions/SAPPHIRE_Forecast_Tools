"""
Phase 3 Tests: Edge Cases and Error Handling

Tests for boundary conditions and error scenarios.
Part of PREPQ-005 comprehensive test coverage.
"""

import os
import sys
import pandas as pd
import numpy as np
import datetime as dt
import pytest
from unittest.mock import patch, MagicMock

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import src


class TestEmptyDataHandling:
    """Tests for handling empty data scenarios."""

    def test_empty_dataframe_filter_outliers(self):
        """filter_roughly_for_outliers should handle empty DataFrame."""
        df = pd.DataFrame({
            'date': pd.Series([], dtype='datetime64[ns]'),
            'Code': pd.Series([], dtype='str'),
            'Q_m3s': pd.Series([], dtype='float'),
        })

        result = src.filter_roughly_for_outliers(df)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_single_row_dataframe(self):
        """Single row DataFrame should be handled."""
        df = pd.DataFrame({
            'date': [pd.Timestamp('2024-06-15')],
            'Code': ['99901'],
            'Q_m3s': [150.0],
        })

        result = src.filter_roughly_for_outliers(df)

        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 1


class TestStandardizeDateColumn:
    """Tests for date standardization function."""

    def test_standardize_date_removes_timezone(self):
        """Timezone should be removed from dates."""
        df = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01 12:00:00+05:00', '2024-01-02 08:00:00+05:00'])
        })

        result = src.standardize_date_column(df, 'date')

        # Timezone should be None
        assert result['date'].dt.tz is None

    def test_standardize_date_normalizes_time(self):
        """Time component should be set to midnight."""
        df = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01 15:30:45', '2024-01-02 23:59:59'])
        })

        result = src.standardize_date_column(df, 'date')

        # All times should be midnight
        for ts in result['date']:
            assert ts.hour == 0
            assert ts.minute == 0
            assert ts.second == 0

    def test_standardize_date_preserves_date(self):
        """Date part should be preserved."""
        df = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-15', '2024-06-20', '2024-12-31'])
        })

        result = src.standardize_date_column(df, 'date')

        assert result['date'].iloc[0].day == 15
        assert result['date'].iloc[1].month == 6
        assert result['date'].iloc[2].year == 2024


class TestDischargeValueBoundaries:
    """Tests for discharge value boundary conditions."""

    def test_zero_discharge_values(self):
        """Zero discharge values should be preserved."""
        df = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=5, freq='D'),
            'Code': ['99901'] * 5,
            'Q_m3s': [100.0, 0.0, 50.0, 0.0, 75.0],
        })

        result = src.filter_roughly_for_outliers(df)

        # Zero values should be preserved
        assert (result['Q_m3s'] == 0.0).sum() >= 1

    def test_very_small_discharge_values(self):
        """Very small positive values should be handled."""
        df = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=5, freq='D'),
            'Code': ['99901'] * 5,
            'Q_m3s': [0.001, 0.002, 0.001, 0.003, 0.002],
        })

        result = src.filter_roughly_for_outliers(df)

        assert isinstance(result, pd.DataFrame)
        # Values should still be small (not modified significantly)
        assert result['Q_m3s'].max() < 1.0

    def test_very_large_discharge_values(self):
        """Very large discharge values should be handled."""
        df = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=5, freq='D'),
            'Code': ['99901'] * 5,
            'Q_m3s': [10000.0, 10500.0, 10200.0, 10800.0, 10300.0],
        })

        result = src.filter_roughly_for_outliers(df)

        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 5


class TestDateBoundaries:
    """Tests for date boundary conditions."""

    def test_year_boundary_dates(self):
        """Dates at year boundaries should be handled."""
        df = pd.DataFrame({
            'date': pd.to_datetime([
                '2023-12-30', '2023-12-31', '2024-01-01', '2024-01-02'
            ]),
            'Code': ['99901'] * 4,
            'Q_m3s': [100.0, 105.0, 110.0, 108.0],
        })

        result = src.filter_roughly_for_outliers(df)

        assert isinstance(result, pd.DataFrame)
        # All dates should be present or accounted for
        assert len(result) >= 4

    def test_leap_year_feb29(self):
        """February 29 in leap year should be handled."""
        # 2024 is a leap year
        df = pd.DataFrame({
            'date': pd.to_datetime([
                '2024-02-28', '2024-02-29', '2024-03-01'
            ]),
            'Code': ['99901'] * 3,
            'Q_m3s': [100.0, 102.0, 105.0],
        })

        result = src.filter_roughly_for_outliers(df)

        assert isinstance(result, pd.DataFrame)
        # Feb 29 should be handled without error


class TestMergeWithUpdateEdgeCases:
    """Tests for _merge_with_update edge cases."""

    def test_merge_both_empty(self):
        """Both existing and new data empty should return empty."""
        existing = pd.DataFrame({
            'code': pd.Series([], dtype='str'),
            'date': pd.Series([], dtype='datetime64[ns]'),
            'discharge': pd.Series([], dtype='float')
        })
        new = pd.DataFrame({
            'code': pd.Series([], dtype='str'),
            'date': pd.Series([], dtype='datetime64[ns]'),
            'discharge': pd.Series([], dtype='float')
        })

        result = src._merge_with_update(existing, new, 'code', 'date', 'discharge')

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_merge_existing_empty_returns_new(self):
        """Empty existing data should return new data."""
        existing = pd.DataFrame({
            'code': pd.Series([], dtype='str'),
            'date': pd.Series([], dtype='datetime64[ns]'),
            'discharge': pd.Series([], dtype='float')
        })
        new = pd.DataFrame({
            'code': ['99901', '99901'],
            'date': pd.to_datetime(['2024-01-01', '2024-01-02']),
            'discharge': [100.0, 110.0]
        })

        result = src._merge_with_update(existing, new, 'code', 'date', 'discharge')

        assert len(result) == 2

    def test_merge_new_empty_returns_existing(self):
        """Empty new data should return existing data."""
        existing = pd.DataFrame({
            'code': ['99901', '99901'],
            'date': pd.to_datetime(['2024-01-01', '2024-01-02']),
            'discharge': [100.0, 110.0]
        })
        new = pd.DataFrame({
            'code': pd.Series([], dtype='str'),
            'date': pd.Series([], dtype='datetime64[ns]'),
            'discharge': pd.Series([], dtype='float')
        })

        result = src._merge_with_update(existing, new, 'code', 'date', 'discharge')

        assert len(result) == 2

    def test_merge_overlapping_dates_updates(self):
        """Overlapping dates should be updated from new data."""
        existing = pd.DataFrame({
            'code': ['99901', '99901'],
            'date': pd.to_datetime(['2024-01-01', '2024-01-02']),
            'discharge': [100.0, 110.0]
        })
        new = pd.DataFrame({
            'code': ['99901', '99901'],
            'date': pd.to_datetime(['2024-01-02', '2024-01-03']),
            'discharge': [115.0, 120.0]  # Updated value for Jan 2
        })

        result = src._merge_with_update(existing, new, 'code', 'date', 'discharge')

        # Jan 2 should have the new value (115.0)
        jan2_row = result[result['date'] == pd.Timestamp('2024-01-02')]
        assert len(jan2_row) == 1
        assert jan2_row['discharge'].iloc[0] == 115.0


class TestMultipleStationsEdgeCases:
    """Tests for edge cases with multiple stations."""

    def test_single_station_many_dates(self):
        """Single station with many dates should work."""
        dates = pd.date_range('2020-01-01', '2024-12-31', freq='D')
        df = pd.DataFrame({
            'date': dates,
            'Code': ['99901'] * len(dates),
            'Q_m3s': np.random.randn(len(dates)) * 10 + 100,
        })

        result = src.filter_roughly_for_outliers(df)

        assert isinstance(result, pd.DataFrame)
        assert len(result) >= len(dates)  # May have more due to gap filling

    def test_many_stations_single_date(self):
        """Many stations with single date should work."""
        stations = [f'{10000 + i}' for i in range(50)]
        df = pd.DataFrame({
            'date': [pd.Timestamp('2024-06-15')] * 50,
            'Code': stations,
            'Q_m3s': np.random.randn(50) * 10 + 100,
        })

        result = src.filter_roughly_for_outliers(df)

        assert isinstance(result, pd.DataFrame)
        # All stations should be present
        assert len(result['Code'].unique()) == 50


class TestNaNHandling:
    """Tests for NaN value handling."""

    def test_all_nan_discharge(self):
        """All NaN discharge values should be handled."""
        df = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=5, freq='D'),
            'Code': ['99901'] * 5,
            'Q_m3s': [np.nan] * 5,
        })

        result = src.filter_roughly_for_outliers(df)

        assert isinstance(result, pd.DataFrame)

    def test_mixed_nan_values(self):
        """Mixed valid and NaN values should be handled."""
        df = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=10, freq='D'),
            'Code': ['99901'] * 10,
            'Q_m3s': [100.0, np.nan, 102.0, np.nan, 104.0,
                     np.nan, 106.0, np.nan, 108.0, np.nan],
        })

        result = src.filter_roughly_for_outliers(df)

        assert isinstance(result, pd.DataFrame)
        # Should have at least the valid values
        non_nan_count = result['Q_m3s'].notna().sum()
        assert non_nan_count >= 5


class TestSeasonTransitions:
    """Tests for season transition handling."""

    def test_winter_to_spring_transition(self):
        """Data across winter-spring boundary should be preserved."""
        # Feb 28 to March 5 crosses winter->spring boundary
        dates = pd.date_range('2024-02-25', '2024-03-10', freq='D')
        df = pd.DataFrame({
            'date': dates,
            'Code': ['99901'] * len(dates),
            'Q_m3s': [100.0 + i for i in range(len(dates))],
        })

        result = src.filter_roughly_for_outliers(df)

        # All dates should be preserved
        for date in dates:
            matching = result[result['date'] == date]
            assert len(matching) >= 1, f"Date {date} missing from result"

    def test_autumn_to_winter_transition(self):
        """Data across autumn-winter boundary should be preserved."""
        # Nov 25 to Dec 5 crosses autumn->winter boundary
        dates = pd.date_range('2024-11-25', '2024-12-05', freq='D')
        df = pd.DataFrame({
            'date': dates,
            'Code': ['99901'] * len(dates),
            'Q_m3s': [100.0 + i for i in range(len(dates))],
        })

        result = src.filter_roughly_for_outliers(df)

        # All dates should be preserved
        for date in dates:
            matching = result[result['date'] == date]
            assert len(matching) >= 1, f"Date {date} missing from result"


class TestDuplicateHandling:
    """Tests for duplicate record handling."""

    def test_duplicate_date_station_rows(self):
        """Duplicate date-station combinations should be handled."""
        df = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-02']),
            'Code': ['99901', '99901', '99901'],  # Duplicate for Jan 1
            'Q_m3s': [100.0, 105.0, 110.0],
        })

        result = src.filter_roughly_for_outliers(df)

        assert isinstance(result, pd.DataFrame)
        # Should handle duplicates without error


class TestIsLeapYear:
    """Tests for leap year detection."""

    def test_leap_year_2024(self):
        """2024 should be detected as leap year."""
        assert src.is_leap_year(2024) == True

    def test_non_leap_year_2023(self):
        """2023 should not be a leap year."""
        assert src.is_leap_year(2023) == False

    def test_century_leap_year_2000(self):
        """2000 should be a leap year (divisible by 400)."""
        assert src.is_leap_year(2000) == True

    def test_century_non_leap_year_1900(self):
        """1900 should not be a leap year (divisible by 100 but not 400)."""
        assert src.is_leap_year(1900) == False


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
