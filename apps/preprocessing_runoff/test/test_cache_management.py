"""
Phase 2 Tests: Cache Management

Tests for _load_cached_data() function and cache file handling.
Part of PREPQ-005 comprehensive test coverage.
"""

import os
import sys
import tempfile
import pandas as pd
import numpy as np
import pytest
from unittest.mock import patch, MagicMock

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import src


class TestLoadCachedDataFileExists:
    """Tests for _load_cached_data when cache file exists."""

    def test_load_cache_returns_dataframe(self, tmp_path):
        """When cache file exists and is valid, returns DataFrame."""
        # Create a valid cache file
        cache_file = tmp_path / "runoff_day.csv"
        df = pd.DataFrame({
            'date': ['2024-01-01', '2024-01-02', '2024-01-03'],
            'code': ['99901', '99901', '99901'],
            'discharge': [100.0, 110.0, 105.0],
        })
        df.to_csv(cache_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_daily_discharge_file': 'runoff_day.csv',
            'ieasyhydroforecast_organization': 'test_org'
        }):
            result = src._load_cached_data(
                date_col='date',
                discharge_col='discharge',
                name_col='name',
                code_col='code',
                code_list=['99901']
            )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert 'date' in result.columns
        assert 'code' in result.columns
        assert 'discharge' in result.columns

    def test_load_cache_preserves_dtypes(self, tmp_path):
        """Cache loading preserves correct data types."""
        cache_file = tmp_path / "runoff_day.csv"
        df = pd.DataFrame({
            'date': ['2024-01-01', '2024-01-02'],
            'code': ['99901', '99901'],
            'discharge': [100.5, 110.2],
        })
        df.to_csv(cache_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_daily_discharge_file': 'runoff_day.csv',
            'ieasyhydroforecast_organization': 'test_org'
        }):
            result = src._load_cached_data(
                date_col='date',
                discharge_col='discharge',
                name_col='name',
                code_col='code',
                code_list=['99901']
            )

        # Date column should be datetime
        assert pd.api.types.is_datetime64_any_dtype(result['date'])
        # Code column should be string
        assert result['code'].dtype == object or pd.api.types.is_string_dtype(result['code'])
        # Discharge column should be float
        assert pd.api.types.is_float_dtype(result['discharge'])

    def test_load_cache_with_gaps_preserves_gaps(self, tmp_path):
        """Gaps in cache file are preserved, not filled."""
        cache_file = tmp_path / "runoff_day.csv"
        # Data with a gap (Jan 2 missing)
        df = pd.DataFrame({
            'date': ['2024-01-01', '2024-01-03', '2024-01-04'],
            'code': ['99901', '99901', '99901'],
            'discharge': [100.0, 105.0, 108.0],
        })
        df.to_csv(cache_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_daily_discharge_file': 'runoff_day.csv',
            'ieasyhydroforecast_organization': 'test_org'
        }):
            result = src._load_cached_data(
                date_col='date',
                discharge_col='discharge',
                name_col='name',
                code_col='code',
                code_list=['99901']
            )

        # Should have exactly 3 rows (gap not filled)
        assert len(result) == 3
        dates = result['date'].dt.strftime('%Y-%m-%d').tolist()
        assert '2024-01-02' not in dates

    def test_load_cache_normalizes_dates(self, tmp_path):
        """Date column is normalized (time component removed)."""
        cache_file = tmp_path / "runoff_day.csv"
        # Include time component that should be stripped
        df = pd.DataFrame({
            'date': ['2024-01-01 12:30:00', '2024-01-02 08:00:00'],
            'code': ['99901', '99901'],
            'discharge': [100.0, 110.0],
        })
        df.to_csv(cache_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_daily_discharge_file': 'runoff_day.csv',
            'ieasyhydroforecast_organization': 'test_org'
        }):
            result = src._load_cached_data(
                date_col='date',
                discharge_col='discharge',
                name_col='name',
                code_col='code',
                code_list=['99901']
            )

        # All times should be midnight (normalized)
        for ts in result['date']:
            assert ts.hour == 0
            assert ts.minute == 0
            assert ts.second == 0


class TestLoadCachedDataFileMissing:
    """Tests for _load_cached_data when cache file is missing."""

    def test_load_cache_file_missing_falls_back(self, tmp_path):
        """When cache file missing, falls back to reading organization files."""
        # Don't create the cache file
        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_daily_discharge_file': 'nonexistent.csv',
            'ieasyhydroforecast_organization': 'test_org'
        }):
            # Mock the fallback function
            with patch.object(src, '_read_runoff_data_by_organization') as mock_fallback:
                mock_fallback.return_value = pd.DataFrame({
                    'date': pd.to_datetime(['2024-01-01']),
                    'code': ['99901'],
                    'discharge': [100.0],
                    'name': ['Test Station']
                })

                result = src._load_cached_data(
                    date_col='date',
                    discharge_col='discharge',
                    name_col='name',
                    code_col='code',
                    code_list=['99901']
                )

                # Should have called the fallback
                mock_fallback.assert_called_once()
                assert len(result) == 1


class TestLoadCachedDataFileCorrupt:
    """Tests for _load_cached_data when cache file is corrupt."""

    def test_load_cache_file_corrupt_falls_back(self, tmp_path):
        """When cache file is corrupt, falls back to organization files."""
        cache_file = tmp_path / "runoff_day.csv"
        # Write invalid CSV content
        cache_file.write_text("this,is,not,valid\ncsv,content,with,wrong,columns")

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_daily_discharge_file': 'runoff_day.csv',
            'ieasyhydroforecast_organization': 'test_org'
        }):
            with patch.object(src, '_read_runoff_data_by_organization') as mock_fallback:
                mock_fallback.return_value = pd.DataFrame({
                    'date': pd.to_datetime(['2024-01-01']),
                    'code': ['99901'],
                    'discharge': [100.0],
                    'name': ['Test Station']
                })

                result = src._load_cached_data(
                    date_col='date',
                    discharge_col='discharge',
                    name_col='name',
                    code_col='code',
                    code_list=['99901']
                )

                # Should have called the fallback due to error
                mock_fallback.assert_called_once()


class TestLoadCachedDataEmpty:
    """Tests for _load_cached_data when cache file is empty."""

    def test_load_cache_empty_file_falls_back(self, tmp_path):
        """When cache file is empty, falls back to organization files."""
        cache_file = tmp_path / "runoff_day.csv"
        # Write empty CSV (headers only)
        pd.DataFrame(columns=['date', 'code', 'discharge']).to_csv(cache_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_daily_discharge_file': 'runoff_day.csv',
            'ieasyhydroforecast_organization': 'test_org'
        }):
            with patch.object(src, '_read_runoff_data_by_organization') as mock_fallback:
                mock_fallback.return_value = pd.DataFrame({
                    'date': pd.to_datetime(['2024-01-01']),
                    'code': ['99901'],
                    'discharge': [100.0],
                    'name': ['Test Station']
                })

                result = src._load_cached_data(
                    date_col='date',
                    discharge_col='discharge',
                    name_col='name',
                    code_col='code',
                    code_list=['99901']
                )

                # Should have called the fallback due to empty data
                mock_fallback.assert_called_once()


class TestLoadCachedDataMultipleStations:
    """Tests for _load_cached_data with multiple stations."""

    def test_load_cache_multiple_stations(self, tmp_path):
        """Cache with multiple stations loads correctly."""
        cache_file = tmp_path / "runoff_day.csv"
        df = pd.DataFrame({
            'date': ['2024-01-01', '2024-01-01', '2024-01-02', '2024-01-02'],
            'code': ['99901', '99902', '99901', '99902'],
            'discharge': [100.0, 200.0, 105.0, 210.0],
        })
        df.to_csv(cache_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_daily_discharge_file': 'runoff_day.csv',
            'ieasyhydroforecast_organization': 'test_org'
        }):
            result = src._load_cached_data(
                date_col='date',
                discharge_col='discharge',
                name_col='name',
                code_col='code',
                code_list=['99901', '99902']
            )

        assert len(result) == 4
        assert set(result['code'].unique()) == {'99901', '99902'}


class TestLoadCachedDataNaNValues:
    """Tests for _load_cached_data with NaN values."""

    def test_load_cache_with_nan_discharge(self, tmp_path):
        """NaN values in discharge column are preserved."""
        cache_file = tmp_path / "runoff_day.csv"
        df = pd.DataFrame({
            'date': ['2024-01-01', '2024-01-02', '2024-01-03'],
            'code': ['99901', '99901', '99901'],
            'discharge': [100.0, np.nan, 105.0],
        })
        df.to_csv(cache_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_daily_discharge_file': 'runoff_day.csv',
            'ieasyhydroforecast_organization': 'test_org'
        }):
            result = src._load_cached_data(
                date_col='date',
                discharge_col='discharge',
                name_col='name',
                code_col='code',
                code_list=['99901']
            )

        # NaN should be preserved
        assert result['discharge'].isna().sum() == 1
        assert pd.isna(result.iloc[1]['discharge'])


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
