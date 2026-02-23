"""
Tests for recalculate_snow_norms.py — yearly snow norm computation
and API write.

The script:
1. Reads environment to find snow CSV paths and variables
2. Calls dg_utils.calculate_snow_norms() on historical CSVs
3. Writes full-year norms to the API, preserving existing values
"""
import os
import sys
import pandas as pd
import numpy as np
from datetime import date
from unittest.mock import Mock, patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast')
)

# Mock the sapphire_dg_client module before importing
sys.modules['sapphire_dg_client'] = MagicMock()
sys.modules['sapphire_dg_client.SapphireDGClient'] = MagicMock()
sys.modules['sapphire_dg_client.snow_model'] = MagicMock()

import dg_utils
import recalculate_snow_norms as rsn


class TestRecalculateSnowNorms:
    """End-to-end tests for recalculate_snow_norms.py."""

    def _make_snow_csv(self, path, variable, hru, n_years=3):
        """Create a realistic snow CSV with n_years of daily data."""
        var_dir = os.path.join(path, variable)
        os.makedirs(var_dir, exist_ok=True)

        rows = []
        for year in range(2020, 2020 + n_years):
            start = date(year, 1, 1)
            end = date(year, 12, 31)
            days = pd.date_range(start, end, freq='D')
            for d in days:
                rows.append({
                    'date': d,
                    'code': '15013',
                    variable: 50.0 + d.dayofyear * 0.1,
                })

        df = pd.DataFrame(rows)
        csv_path = os.path.join(var_dir, f'{hru}_{variable}.csv')
        df.to_csv(csv_path, index=False)
        return csv_path

    @patch.object(dg_utils, 'SAPPHIRE_API_AVAILABLE', True)
    @patch('recalculate_snow_norms.dg_utils.SapphirePreprocessingClient')
    def test_happy_path_norms_written_to_api(
        self, mock_client_class, tmp_path
    ):
        """Full flow: CSVs exist, norms computed, written to API."""

        snow_path = str(tmp_path / 'snow')
        self._make_snow_csv(snow_path, 'SWE', 'HRU01')

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_snow.return_value = pd.DataFrame()
        mock_client.write_snow.return_value = 365
        mock_client_class.return_value = mock_client

        env = {
            'SAPPHIRE_API_ENABLED': 'true',
            'SAPPHIRE_API_URL': 'http://localhost:8000',
        }

        result = rsn.recalculate_norms(
            snow_path=snow_path,
            variables=['SWE'],
            hru_codes=['HRU01'],
            year=2024,
            env_overrides=env,
        )

        assert result is True
        mock_client.write_snow.assert_called()
        records = mock_client.write_snow.call_args[0][0]
        assert len(records) > 0

        # Every record should have a non-None norm
        for r in records:
            assert r['norm'] is not None
            assert r['snow_type'] == 'SWE'

        # Dates should span Jan 1 to Dec 31 of the target year
        dates = sorted(r['date'] for r in records)
        assert dates[0] == '2024-01-01'
        assert dates[-1] == '2024-12-31'

    @patch.object(dg_utils, 'SAPPHIRE_API_AVAILABLE', True)
    @patch('recalculate_snow_norms.dg_utils.SapphirePreprocessingClient')
    def test_preserves_existing_values_from_api(
        self, mock_client_class, tmp_path
    ):
        """Existing API values are preserved; only norm is added."""

        snow_path = str(tmp_path / 'snow')
        self._make_snow_csv(snow_path, 'SWE', 'HRU01')

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        # API returns existing record with value but no norm
        mock_client.read_snow.return_value = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-15']),
            'code': ['15013'],
            'snow_type': ['SWE'],
            'value': [88.8],
            'norm': [np.nan],
        })
        mock_client.write_snow.return_value = 365
        mock_client_class.return_value = mock_client

        env = {
            'SAPPHIRE_API_ENABLED': 'true',
            'SAPPHIRE_API_URL': 'http://localhost:8000',
        }

        result = rsn.recalculate_norms(
            snow_path=snow_path,
            variables=['SWE'],
            hru_codes=['HRU01'],
            year=2024,
            env_overrides=env,
        )

        assert result is True
        records = mock_client.write_snow.call_args[0][0]
        # Find the Jan 15 record
        jan15 = [r for r in records if r['date'] == '2024-01-15']
        assert len(jan15) == 1
        # Value should be preserved from API
        assert jan15[0]['value'] == 88.8
        # Norm should be computed (not None)
        assert jan15[0]['norm'] is not None

    def test_api_unavailable_returns_false(self, tmp_path):
        """When API is unavailable, returns False gracefully."""
        snow_path = str(tmp_path / 'snow')
        self._make_snow_csv(snow_path, 'SWE', 'HRU01')

        env = {
            'SAPPHIRE_API_ENABLED': 'false',
        }

        result = rsn.recalculate_norms(
            snow_path=snow_path,
            variables=['SWE'],
            hru_codes=['HRU01'],
            year=2024,
            env_overrides=env,
        )

        assert result is False

    def test_no_csv_data_returns_false(self, tmp_path):
        """When no CSV files exist, returns False."""
        snow_path = str(tmp_path / 'empty_snow')
        os.makedirs(snow_path, exist_ok=True)

        result = rsn.recalculate_norms(
            snow_path=snow_path,
            variables=['SWE'],
            hru_codes=['HRU01'],
            year=2024,
            env_overrides={'SAPPHIRE_API_ENABLED': 'false'},
        )

        assert result is False

    @patch.object(dg_utils, 'SAPPHIRE_API_AVAILABLE', True)
    @patch('recalculate_snow_norms.dg_utils.SapphirePreprocessingClient')
    def test_leap_year_includes_day_366(
        self, mock_client_class, tmp_path
    ):
        """Leap year norms include day 366 (Dec 31)."""

        snow_path = str(tmp_path / 'snow')
        self._make_snow_csv(snow_path, 'SWE', 'HRU01', n_years=5)

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_snow.return_value = pd.DataFrame()
        mock_client.write_snow.return_value = 366
        mock_client_class.return_value = mock_client

        env = {
            'SAPPHIRE_API_ENABLED': 'true',
            'SAPPHIRE_API_URL': 'http://localhost:8000',
        }

        result = rsn.recalculate_norms(
            snow_path=snow_path,
            variables=['SWE'],
            hru_codes=['HRU01'],
            year=2024,  # leap year
            env_overrides=env,
        )

        assert result is True
        records = mock_client.write_snow.call_args[0][0]
        dates = sorted(r['date'] for r in records)
        assert dates[-1] == '2024-12-31'
        # 2024 is a leap year, so 366 days
        assert len(dates) == 366

    @patch.object(dg_utils, 'SAPPHIRE_API_AVAILABLE', True)
    @patch('recalculate_snow_norms.dg_utils.SapphirePreprocessingClient')
    def test_multiple_variables_and_codes(
        self, mock_client_class, tmp_path
    ):
        """Multiple variables and HRU codes produce norms for each."""

        snow_path = str(tmp_path / 'snow')
        self._make_snow_csv(snow_path, 'SWE', 'HRU01')
        self._make_snow_csv(snow_path, 'HS', 'HRU01')

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_snow.return_value = pd.DataFrame()
        mock_client.write_snow.return_value = 365
        mock_client_class.return_value = mock_client

        env = {
            'SAPPHIRE_API_ENABLED': 'true',
            'SAPPHIRE_API_URL': 'http://localhost:8000',
        }

        result = rsn.recalculate_norms(
            snow_path=snow_path,
            variables=['SWE', 'HS'],
            hru_codes=['HRU01'],
            year=2023,
            env_overrides=env,
        )

        assert result is True
        # write_snow should be called at least twice (once per variable)
        assert mock_client.write_snow.call_count >= 2
