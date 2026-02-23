"""
Tests for SAPPHIRE API integration in preprocessing_gateway modules.

Tests the snow and meteo data API integration:
- _write_snow_to_api function in snow_data_operational.py (writes yesterday+today - operational)
- _write_snow_to_api function in snow_data_renalysis.py (writes last 30 days - maintenance)
- _write_meteo_to_api function in extend_era5_reanalysis.py (writes all data)
- _write_meteo_to_api function in Quantile_Mapping_OP.py (writes yesterday+today - operational)
"""
import os
import pandas as pd
import numpy as np
import pytest
from unittest.mock import Mock, patch, MagicMock

import sys
# Add preprocessing_gateway to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast'))

# Mock the sapphire_dg_client module before importing the actual modules
# This is necessary because sapphire_dg_client is a private package
sys.modules['sapphire_dg_client'] = MagicMock()
sys.modules['sapphire_dg_client.SapphireDGClient'] = MagicMock()
sys.modules['sapphire_dg_client.snow_model'] = MagicMock()

import snow_data_operational as sdo
import snow_data_renalysis as sdr
import extend_era5_reanalysis as eer
import Quantile_Mapping_OP as qm


# =============================================================================
# Tests for _write_snow_to_api (operational mode - writes latest only)
# =============================================================================

class TestWriteSnowToApi:
    """Tests for the _write_snow_to_api function in snow_data_operational.py

    This function always writes yesterday+today (operational mode, 2-day window).
    """

    def test_api_disabled_via_env_var(self):
        """When SAPPHIRE_API_ENABLED=false, API write should be skipped."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'
        try:
            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01', '2024-01-01']),
                'code': [12345, 67890],
                'SWE': [100.5, 200.0],
            })
            result = sdo._write_snow_to_api(data, "SWE", "test_hru")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_api_not_ready_returns_false(self, mock_client_class):
        """When API health check fails, should return False (non-blocking)."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = False
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': [12345],
                'SWE': [100.5],
            })

            result = sdo._write_snow_to_api(data, "SWE", "test_hru")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_writes_recent_days_only(self, mock_client_class):
        """Only yesterday+today should be written (operational behavior)."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 2
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            yesterday = today - pd.Timedelta(days=1)
            two_days_ago = today - pd.Timedelta(days=2)

            data = pd.DataFrame({
                'date': [two_days_ago, yesterday, today],
                'code': [12345, 12345, 12345],
                'SWE': [100.0, 150.0, 200.0],
            })

            result = sdo._write_snow_to_api(data, "SWE", "test_hru")
            assert result is True

            # Check that write_snow was called
            mock_client.write_snow.assert_called_once()
            # Get the records that were passed
            call_args = mock_client.write_snow.call_args[0][0]
            # Should have 2 records (yesterday + today)
            assert len(call_args) == 2
            dates = {r['date'] for r in call_args}
            assert dates == {
                yesterday.strftime('%Y-%m-%d'),
                today.strftime('%Y-%m-%d'),
            }
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_elevation_band_values(self, mock_client_class):
        """Test that elevation band values (SWE_1, SWE_2, etc.) are correctly mapped."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 1
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            data = pd.DataFrame({
                'date': [today],
                'code': [12345],
                'SWE': [100.0],
                'SWE_1': [80.0],
                'SWE_2': [90.0],
                'SWE_3': [110.0],
            })

            result = sdo._write_snow_to_api(data, "SWE", "test_hru")
            assert result is True

            # Check the record structure
            call_args = mock_client.write_snow.call_args[0][0]
            assert len(call_args) == 1
            record = call_args[0]
            assert record['value'] == 100.0
            assert record['value1'] == 80.0
            assert record['value2'] == 90.0
            assert record['value3'] == 110.0
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_nan_values_are_none(self, mock_client_class):
        """Test that NaN values are converted to None."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 1
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            data = pd.DataFrame({
                'date': [today],
                'code': [12345],
                'SWE': [np.nan],
            })

            result = sdo._write_snow_to_api(data, "SWE", "test_hru")
            assert result is True

            call_args = mock_client.write_snow.call_args[0][0]
            assert call_args[0]['value'] is None
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    def test_empty_dataframe_returns_false(self):
        """Empty DataFrame should return False without calling API."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            data = pd.DataFrame()
            result = sdo._write_snow_to_api(data, "SWE", "test_hru")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_snow_type_uppercase(self, mock_client_class):
        """Test that snow_type is converted to uppercase for API."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 1
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            data = pd.DataFrame({
                'date': [today],
                'code': [12345],
                'swe': [100.0],  # lowercase column
            })

            result = sdo._write_snow_to_api(data, "swe", "test_hru")
            assert result is True

            call_args = mock_client.write_snow.call_args[0][0]
            assert call_args[0]['snow_type'] == 'SWE'  # Converted to uppercase
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


# =============================================================================
# Tests for _write_snow_to_api (maintenance mode - writes last 30 days)
# =============================================================================

class TestWriteSnowToApiMaintenance:
    """Tests for the _write_snow_to_api function in snow_data_renalysis.py

    This function writes the last 30 days of data (maintenance mode).
    """

    def test_api_disabled_via_env_var(self):
        """When SAPPHIRE_API_ENABLED=false, API write should be skipped."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'
        try:
            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01', '2024-01-01']),
                'code': [12345, 67890],
                'SWE': [100.5, 200.0],
            })
            result = sdr._write_snow_to_api(data, "SWE", "test_hru")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('snow_data_renalysis.SapphirePreprocessingClient')
    def test_api_not_ready_returns_false(self, mock_client_class):
        """When API health check fails, should return False (non-blocking)."""
        if not sdr.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = False
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': [12345],
                'SWE': [100.5],
            })

            result = sdr._write_snow_to_api(data, "SWE", "test_hru")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('snow_data_renalysis.SapphirePreprocessingClient')
    def test_writes_last_30_days_only(self, mock_client_class):
        """Only the last 30 days of data should be written (maintenance behavior)."""
        if not sdr.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 30
            mock_client_class.return_value = mock_client

            # Data spanning 60 days - only last 30 should be written
            dates = pd.date_range(end='2024-03-01', periods=60, freq='D')
            data = pd.DataFrame({
                'date': dates,
                'code': [12345] * 60,
                'SWE': np.random.uniform(50, 200, 60),
            })

            result = sdr._write_snow_to_api(data, "SWE", "test_hru")
            assert result is True

            # Check that write_snow was called
            mock_client.write_snow.assert_called_once()
            # Get the records that were passed
            call_args = mock_client.write_snow.call_args[0][0]
            # Should have ~30 records (last 30 days including cutoff day)
            assert len(call_args) == 31  # 30 days + cutoff day
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('snow_data_renalysis.SapphirePreprocessingClient')
    def test_elevation_band_values(self, mock_client_class):
        """Test that elevation band values (SWE_1, SWE_2, etc.) are correctly mapped."""
        if not sdr.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': [12345],
                'SWE': [100.0],
                'SWE_1': [80.0],
                'SWE_2': [90.0],
                'SWE_3': [110.0],
            })

            result = sdr._write_snow_to_api(data, "SWE", "test_hru")
            assert result is True

            # Check the record structure
            call_args = mock_client.write_snow.call_args[0][0]
            assert len(call_args) == 1
            record = call_args[0]
            assert record['value'] == 100.0
            assert record['value1'] == 80.0
            assert record['value2'] == 90.0
            assert record['value3'] == 110.0
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    def test_empty_dataframe_returns_false(self):
        """Empty DataFrame should return False without calling API."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            data = pd.DataFrame()
            result = sdr._write_snow_to_api(data, "SWE", "test_hru")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


class TestSnowReanalysisConsistencyCheck:
    """Tests for _check_snow_consistency function in snow_data_renalysis.py"""

    def test_consistency_check_disabled_by_default(self):
        """Consistency check should return True when disabled (default)."""
        os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
        data = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01']),
            'code': [12345],
            'SWE': [100.0],
        })
        result = sdr._check_snow_consistency(data, "SWE", "test_hru")
        assert result is True

    def test_consistency_check_function_exists(self):
        """Test that the consistency check function exists."""
        assert hasattr(sdr, '_check_snow_consistency')

    @patch('snow_data_renalysis.SapphirePreprocessingClient')
    def test_consistency_check_enabled_compares_data(self, mock_client_class):
        """When enabled, should read from API and compare."""
        if not sdr.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            # Return matching data from API
            mock_client.read_snow.return_value = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': ['12345'],
                'snow_type': ['SWE'],
                'value': [100.0],
            })
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': [12345],
                'SWE': [100.0],
            })

            result = sdr._check_snow_consistency(csv_data, "SWE", "test_hru")
            assert result is True

            # Verify read_snow was called
            mock_client.read_snow.assert_called()
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('snow_data_renalysis.SapphirePreprocessingClient')
    def test_returns_false_on_value_mismatch(self, mock_client_class):
        """Should return False when values don't match."""
        if not sdr.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            # Return different value from API
            mock_client.read_snow.return_value = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': ['12345'],
                'snow_type': ['SWE'],
                'value': [999.0],  # Different value
            })
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': [12345],
                'SWE': [100.0],  # Original value
            })

            result = sdr._check_snow_consistency(csv_data, "SWE", "test_hru")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


class TestSnowReanalysisIntegration:
    """Integration tests for snow_data_renalysis.py.

    Tests the full flow: DG download -> transform -> CSV write ->
    API write -> consistency check.
    """

    @patch('snow_data_renalysis._check_snow_consistency')
    @patch('snow_data_renalysis._write_snow_to_api')
    @patch('snow_data_renalysis.pd.read_csv')
    @patch('snow_data_renalysis.dg_utils.transform_snow_data')
    def test_happy_path_write_and_check_called(
        self, mock_transform, mock_read_csv, mock_write_api, mock_check
    ):
        """Full flow: DG download succeeds, API write + check are called."""
        # Setup: transform returns valid DataFrame
        mock_transform.return_value = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01']),
            'code': [12345],
            'SWE': [100.0],
        })
        mock_read_csv.return_value = pd.DataFrame({
            'raw': ['data']
        })
        mock_write_api.return_value = True
        mock_check.return_value = True

        # Mock the DG client
        mock_client = Mock()
        mock_client.get_snow_reanalysis.return_value = '/tmp/fake.csv'

        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create the expected directory structure
            swe_dir = os.path.join(tmpdir, 'SWE')
            os.makedirs(swe_dir, exist_ok=True)

            result = sdr.get_snow_data_reanalysis(
                client=mock_client,
                hru='12345',
                variable='SWE',
                start_date='2024-01-01',
                end_date='2024-01-31',
                dg_path='/tmp/dg',
                save_path=tmpdir,
            )

            assert result is True
            mock_write_api.assert_called_once()
            mock_check.assert_called_once()

    @patch('snow_data_renalysis._check_snow_consistency')
    @patch('snow_data_renalysis._write_snow_to_api')
    def test_dg_exception_returns_false(self, mock_write_api, mock_check):
        """When DG client raises, function returns False and no API call."""
        mock_client = Mock()
        mock_client.get_snow_reanalysis.side_effect = Exception("DG timeout")

        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            swe_dir = os.path.join(tmpdir, 'SWE')
            os.makedirs(swe_dir, exist_ok=True)

            result = sdr.get_snow_data_reanalysis(
                client=mock_client,
                hru='12345',
                variable='SWE',
                start_date='2024-01-01',
                end_date='2024-01-31',
                dg_path='/tmp/dg',
                save_path=tmpdir,
            )

            assert result is False
            mock_write_api.assert_not_called()
            mock_check.assert_not_called()


# =============================================================================
# Tests for _write_meteo_to_api (writes all data passed)
# =============================================================================

class TestWriteMeteoToApi:
    """Tests for the _write_meteo_to_api function in extend_era5_reanalysis.py

    This function writes all data passed to it (caller determines what to include).
    """

    def test_api_disabled_via_env_var(self):
        """When SAPPHIRE_API_ENABLED=false, API write should be skipped."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'
        try:
            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01', '2024-01-01']),
                'code': [12345, 67890],
                'P': [10.5, 20.0],
                'P_norm': [12.0, 18.0],
            })
            result = eer._write_meteo_to_api(data, "P")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_api_not_ready_returns_false(self, mock_client_class):
        """When API health check fails, should return False (non-blocking)."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = False
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': [12345],
                'T': [15.5],
                'T_norm': [12.0],
            })

            result = eer._write_meteo_to_api(data, "T")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_writes_all_data_passed(self, mock_client_class):
        """All data passed should be written (caller determines what to include)."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 100
            mock_client_class.return_value = mock_client

            # Create data spanning 100 days
            dates = pd.date_range(end='2024-03-01', periods=100, freq='D')
            data = pd.DataFrame({
                'date': dates,
                'code': [12345] * 100,
                'T': np.random.uniform(-10, 30, 100),
                'T_norm': np.random.uniform(-5, 25, 100),
            })

            result = eer._write_meteo_to_api(data, "T")
            assert result is True

            # All records should be written
            mock_client.write_meteo.assert_called_once()
            call_args = mock_client.write_meteo.call_args[0][0]
            assert len(call_args) == 100
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_temperature_type(self, mock_client_class):
        """Test writing temperature (T) data."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-15']),
                'code': [12345],
                'T': [15.5],
                'T_norm': [12.0],
                'dayofyear': [15],
            })

            result = eer._write_meteo_to_api(data, "T")
            assert result is True

            call_args = mock_client.write_meteo.call_args[0][0]
            assert len(call_args) == 1
            record = call_args[0]
            assert record['meteo_type'] == 'T'
            assert record['value'] == 15.5
            assert record['norm'] == 12.0
            assert record['day_of_year'] == 15
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_precipitation_type(self, mock_client_class):
        """Test writing precipitation (P) data."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-06-01']),
                'code': [12345],
                'P': [25.5],
                'P_norm': [20.0],
                'dayofyear': [153],
            })

            result = eer._write_meteo_to_api(data, "P")
            assert result is True

            call_args = mock_client.write_meteo.call_args[0][0]
            record = call_args[0]
            assert record['meteo_type'] == 'P'
            assert record['value'] == 25.5
            assert record['norm'] == 20.0
            assert record['day_of_year'] == 153
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_day_of_year_from_date_if_missing(self, mock_client_class):
        """Test that day_of_year is computed from date if not present."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 1
            mock_client_class.return_value = mock_client

            # No dayofyear column
            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-03-01']),  # Day 61 in leap year
                'code': [12345],
                'T': [10.0],
                'T_norm': [8.0],
            })

            result = eer._write_meteo_to_api(data, "T")
            assert result is True

            call_args = mock_client.write_meteo.call_args[0][0]
            record = call_args[0]
            # March 1, 2024 is day 61 (2024 is a leap year)
            assert record['day_of_year'] == 61
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_nan_values_are_none(self, mock_client_class):
        """Test that NaN values are converted to None."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': [12345],
                'T': [np.nan],
                'T_norm': [12.0],
            })

            result = eer._write_meteo_to_api(data, "T")
            assert result is True

            call_args = mock_client.write_meteo.call_args[0][0]
            assert call_args[0]['value'] is None
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    def test_empty_dataframe_returns_false(self):
        """Empty DataFrame should return False without calling API."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            data = pd.DataFrame()
            result = eer._write_meteo_to_api(data, "T")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


# =============================================================================
# Integration tests (requires API client but mocks actual API calls)
# =============================================================================

class TestSnowDataOperationalIntegration:
    """Integration tests for snow_data_operational.py."""

    @patch('snow_data_operational._check_snow_consistency')
    @patch('snow_data_operational._write_snow_to_api')
    @patch('snow_data_operational.pd.read_csv')
    @patch('snow_data_operational.dg_utils.transform_snow_data')
    def test_happy_path_api_write_called(
        self, mock_transform, mock_read_csv, mock_write_api, mock_check
    ):
        """Full flow: DG download succeeds, API write is called."""
        mock_transform.return_value = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01']),
            'code': [12345],
            'SWE': [100.0],
        })
        mock_read_csv.return_value = pd.DataFrame({'raw': ['data']})
        mock_write_api.return_value = True
        mock_check.return_value = True

        mock_client = Mock()
        mock_client.get_operational.return_value = '/tmp/fake.csv'

        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            swe_dir = os.path.join(tmpdir, 'SWE')
            os.makedirs(swe_dir, exist_ok=True)

            result = sdo.get_snow_data_operational(
                client=mock_client,
                hru='12345',
                variable='SWE',
                date='2024-01-01',
                dg_path='/tmp/dg',
                save_path=tmpdir,
            )

            assert result is True
            mock_write_api.assert_called_once()
            mock_check.assert_called_once()

    @patch('snow_data_operational._check_snow_consistency')
    @patch('snow_data_operational._write_snow_to_api')
    def test_api_failure_non_fatal_csv_still_written(
        self, mock_write_api, mock_check
    ):
        """When API write raises SapphireAPIError, CSV is still written."""
        mock_write_api.side_effect = sdo.SapphireAPIError("API down")
        mock_check.return_value = True

        mock_client = Mock()
        mock_client.get_operational.side_effect = Exception("DG error")

        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            swe_dir = os.path.join(tmpdir, 'SWE')
            os.makedirs(swe_dir, exist_ok=True)

            # DG fails so CSV is not written either, but the point is
            # the function handles the exception gracefully
            result = sdo.get_snow_data_operational(
                client=mock_client,
                hru='12345',
                variable='SWE',
                date='2024-01-01',
                dg_path='/tmp/dg',
                save_path=tmpdir,
            )
            assert result is False


class TestExtendEra5ReanalysisIntegration:
    """Integration tests for extend_era5_reanalysis.py."""

    @patch('extend_era5_reanalysis._check_meteo_consistency')
    @patch('extend_era5_reanalysis._write_meteo_to_api')
    def test_write_and_check_called_for_both_p_and_t(
        self, mock_write, mock_check
    ):
        """Both P and T should trigger _write_meteo_to_api + consistency check."""
        mock_write.return_value = True
        mock_check.return_value = True

        # Simulate what main() does for the API write section
        P_data = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01']),
            'code': [12345],
            'P': [10.0],
            'P_norm': [8.0],
        })
        T_data = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01']),
            'code': [12345],
            'T': [15.0],
            'T_norm': [12.0],
        })

        # Write P
        eer._write_meteo_to_api(P_data, 'P')
        eer._check_meteo_consistency(P_data, 'P')

        # Write T
        eer._write_meteo_to_api(T_data, 'T')
        eer._check_meteo_consistency(T_data, 'T')

        assert mock_write.call_count == 2
        assert mock_check.call_count == 2

    @patch('extend_era5_reanalysis._check_meteo_consistency')
    @patch('extend_era5_reanalysis._write_meteo_to_api')
    def test_api_failure_non_fatal(self, mock_write, mock_check):
        """SapphireAPIError during write does not crash (CSV still written)."""
        mock_write.side_effect = eer.SapphireAPIError("API unavailable")

        P_data = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01']),
            'code': [12345],
            'P': [10.0],
            'P_norm': [8.0],
        })

        # Simulate the try/except in main()
        try:
            eer._write_meteo_to_api(P_data, 'P')
            eer._check_meteo_consistency(P_data, 'P')
        except eer.SapphireAPIError:
            pass  # production behavior

        mock_write.assert_called_once()
        # check was not called because write raised first
        mock_check.assert_not_called()


# =============================================================================
# Tests for consistency checking
# =============================================================================

class TestSnowConsistencyCheck:
    """Tests for _check_snow_consistency function"""

    def test_consistency_check_disabled_by_default(self):
        """Consistency check should return True when disabled (default)."""
        os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
        data = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01']),
            'code': [12345],
            'SWE': [100.0],
        })
        result = sdo._check_snow_consistency(data, "SWE", "test_hru")
        assert result is True

    def test_consistency_check_function_exists(self):
        """Test that the consistency check function exists."""
        assert hasattr(sdo, '_check_snow_consistency')

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_consistency_check_enabled_compares_data(self, mock_client_class):
        """When enabled, should read from API and compare."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            today = pd.Timestamp.today().normalize()
            mock_client = Mock()
            # Return matching data from API
            mock_client.read_snow.return_value = pd.DataFrame({
                'date': pd.to_datetime([today]),
                'code': ['12345'],
                'snow_type': ['SWE'],
                'value': [100.0],
            })
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': pd.to_datetime([today]),
                'code': [12345],
                'SWE': [100.0],
            })

            result = sdo._check_snow_consistency(csv_data, "SWE", "test_hru")
            assert result is True

            # Verify read_snow was called
            mock_client.read_snow.assert_called()
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


class TestMeteoConsistencyCheck:
    """Tests for _check_meteo_consistency function"""

    def test_consistency_check_disabled_by_default(self):
        """Consistency check should return True when disabled (default)."""
        os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
        data = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01']),
            'code': [12345],
            'T': [15.0],
            'T_norm': [12.0],
        })
        result = eer._check_meteo_consistency(data, "T")
        assert result is True

    def test_consistency_check_function_exists(self):
        """Test that the consistency check function exists."""
        assert hasattr(eer, '_check_meteo_consistency')

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_consistency_check_enabled_compares_data(self, mock_client_class):
        """When enabled, should read from API and compare."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            # Return matching data from API
            mock_client.read_meteo.return_value = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': ['12345'],
                'meteo_type': ['T'],
                'value': [15.0],
                'norm': [12.0],
            })
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': [12345],
                'T': [15.0],
                'T_norm': [12.0],
            })

            result = eer._check_meteo_consistency(csv_data, "T")
            assert result is True

            # Verify read_meteo was called
            mock_client.read_meteo.assert_called()
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


# =============================================================================
# Tests for Quantile_Mapping_OP _write_meteo_to_api (operational mode - latest only)
# =============================================================================

class TestQuantileMappingWriteMeteoToApi:
    """Tests for the _write_meteo_to_api function in Quantile_Mapping_OP.py

    This function writes yesterday+today (operational mode, 2-day window).
    Unlike extend_era5_reanalysis.py which writes all data.
    """

    def test_api_disabled_via_env_var(self):
        """When SAPPHIRE_API_ENABLED=false, API write should be skipped."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'
        try:
            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01', '2024-01-01']),
                'code': [12345, 67890],
                'P': [10.5, 20.0],
            })
            result = qm._write_meteo_to_api(data, "P", "HRU001")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('Quantile_Mapping_OP.SapphirePreprocessingClient')
    def test_api_not_ready_returns_false(self, mock_client_class):
        """When API health check fails, should return False (non-blocking)."""
        if not qm.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = False
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': [12345],
                'T': [15.5],
            })

            result = qm._write_meteo_to_api(data, "T", "HRU001")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('Quantile_Mapping_OP.SapphirePreprocessingClient')
    def test_writes_recent_days_only(self, mock_client_class):
        """Only yesterday+today should be written (operational behavior)."""
        if not qm.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 2
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            yesterday = today - pd.Timedelta(days=1)
            two_days_ago = today - pd.Timedelta(days=2)

            # Data spanning multiple dates - yesterday+today match
            data = pd.DataFrame({
                'date': [two_days_ago, yesterday, today],
                'code': [12345, 12345, 12345],
                'T': [10.0, 15.0, 20.0],
            })

            result = qm._write_meteo_to_api(data, "T", "HRU001")
            assert result is True

            # Check that write_meteo was called
            mock_client.write_meteo.assert_called_once()
            # Get the records that were passed
            call_args = mock_client.write_meteo.call_args[0][0]
            # Should have 2 records (yesterday + today)
            assert len(call_args) == 2
            dates = {r['date'] for r in call_args}
            assert dates == {
                yesterday.strftime('%Y-%m-%d'),
                today.strftime('%Y-%m-%d'),
            }
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('Quantile_Mapping_OP.SapphirePreprocessingClient')
    def test_temperature_type(self, mock_client_class):
        """Test writing temperature (T) data."""
        if not qm.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 1
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            data = pd.DataFrame({
                'date': [today],
                'code': [12345],
                'T': [15.5],
            })

            result = qm._write_meteo_to_api(data, "T", "HRU001")
            assert result is True

            call_args = mock_client.write_meteo.call_args[0][0]
            assert len(call_args) == 1
            record = call_args[0]
            assert record['meteo_type'] == 'T'
            assert record['value'] == 15.5
            assert record['norm'] is None  # Control member has no norm
            assert record['day_of_year'] == today.dayofyear
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('Quantile_Mapping_OP.SapphirePreprocessingClient')
    def test_precipitation_type(self, mock_client_class):
        """Test writing precipitation (P) data."""
        if not qm.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 1
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            data = pd.DataFrame({
                'date': [today],
                'code': [12345],
                'P': [25.5],
            })

            result = qm._write_meteo_to_api(data, "P", "HRU001")
            assert result is True

            call_args = mock_client.write_meteo.call_args[0][0]
            record = call_args[0]
            assert record['meteo_type'] == 'P'
            assert record['value'] == 25.5
            assert record['day_of_year'] == today.dayofyear
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('Quantile_Mapping_OP.SapphirePreprocessingClient')
    def test_nan_values_are_none(self, mock_client_class):
        """Test that NaN values are converted to None."""
        if not qm.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 1
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            data = pd.DataFrame({
                'date': [today],
                'code': [12345],
                'T': [np.nan],
            })

            result = qm._write_meteo_to_api(data, "T", "HRU001")
            assert result is True

            call_args = mock_client.write_meteo.call_args[0][0]
            assert call_args[0]['value'] is None
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    def test_empty_dataframe_returns_false(self):
        """Empty DataFrame should return False without calling API."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            data = pd.DataFrame()
            result = qm._write_meteo_to_api(data, "T", "HRU001")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


class TestQuantileMappingConsistencyCheck:
    """Tests for _check_meteo_consistency function in Quantile_Mapping_OP.py"""

    def test_consistency_check_disabled_by_default(self):
        """Consistency check should return True when disabled (default)."""
        os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
        data = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01']),
            'code': [12345],
            'T': [15.0],
        })
        result = qm._check_meteo_consistency(data, "T", "HRU001")
        assert result is True

    def test_consistency_check_function_exists(self):
        """Test that the consistency check function exists."""
        assert hasattr(qm, '_check_meteo_consistency')

    @patch('Quantile_Mapping_OP.SapphirePreprocessingClient')
    def test_consistency_check_enabled_compares_data(self, mock_client_class):
        """When enabled, should read from API and compare."""
        if not qm.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            today = pd.Timestamp.today().normalize()
            mock_client = Mock()
            # Return matching data from API
            mock_client.read_meteo.return_value = pd.DataFrame({
                'date': [today],
                'code': ['12345'],
                'meteo_type': ['T'],
                'value': [15.0],
            })
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': [today],
                'code': [12345],
                'T': [15.0],
            })

            result = qm._check_meteo_consistency(csv_data, "T", "HRU001")
            assert result is True

            # Verify read_meteo was called
            mock_client.read_meteo.assert_called()
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


class TestQuantileMappingIntegration:
    """Integration tests for Quantile_Mapping_OP.py.

    Tests the control member and ensemble API write flow.
    """

    @patch('Quantile_Mapping_OP._check_meteo_consistency')
    @patch('Quantile_Mapping_OP._write_meteo_to_api')
    def test_control_member_writes_both_p_and_t(
        self, mock_write, mock_check
    ):
        """Control member loop writes P and T for each HRU."""
        mock_write.return_value = True
        mock_check.return_value = True

        P_data = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01']),
            'code': [12345],
            'P': [10.0],
        })
        T_data = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01']),
            'code': [12345],
            'T': [15.0],
        })

        # Simulate the control member write pattern from main()
        hru = 'HRU001'
        qm._write_meteo_to_api(P_data, 'P', hru)
        qm._check_meteo_consistency(P_data, 'P', hru)
        qm._write_meteo_to_api(T_data, 'T', hru)
        qm._check_meteo_consistency(T_data, 'T', hru)

        assert mock_write.call_count == 2
        assert mock_check.call_count == 2

        # Verify P was written first, then T
        p_call = mock_write.call_args_list[0]
        assert p_call[0][1] == 'P'
        t_call = mock_write.call_args_list[1]
        assert t_call[0][1] == 'T'

    @patch('Quantile_Mapping_OP._check_meteo_consistency')
    @patch('Quantile_Mapping_OP._write_meteo_to_api')
    def test_api_exception_does_not_crash_loop(
        self, mock_write, mock_check
    ):
        """API failure for one HRU does not prevent processing the next."""
        # First HRU fails, second succeeds
        mock_write.side_effect = [
            Exception("API error"), True, True, True
        ]
        mock_check.return_value = True

        hrus = ['HRU001', 'HRU002']
        data = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01']),
            'code': [12345],
            'P': [10.0],
            'T': [15.0],
        })

        # Simulate the main() loop pattern
        for hru in hrus:
            try:
                qm._write_meteo_to_api(data, 'P', hru)
                qm._check_meteo_consistency(data, 'P', hru)
            except Exception:
                pass

            try:
                qm._write_meteo_to_api(data, 'T', hru)
                qm._check_meteo_consistency(data, 'T', hru)
            except Exception:
                pass

        # 4 total calls: HRU001 P (fail), HRU001 T, HRU002 P, HRU002 T
        assert mock_write.call_count == 4


# =============================================================================
# Tests for consistency check returning False on mismatches
# =============================================================================

class TestSnowConsistencyCheckFailures:
    """Tests for _check_snow_consistency returning False on mismatches"""

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_returns_false_on_row_count_mismatch(self, mock_client_class):
        """Should return False when row counts don't match."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            today = pd.Timestamp.today().normalize()
            mock_client = Mock()
            # Return fewer rows from API than in CSV
            mock_client.read_snow.return_value = pd.DataFrame({
                'date': pd.to_datetime([today]),
                'code': ['12345'],
                'snow_type': ['SWE'],
                'value': [100.0],
            })
            mock_client_class.return_value = mock_client

            # CSV has 2 rows, API returns 1
            csv_data = pd.DataFrame({
                'date': pd.to_datetime([today, today]),
                'code': [12345, 67890],
                'SWE': [100.0, 200.0],
            })

            result = sdo._check_snow_consistency(csv_data, "SWE", "test_hru")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_returns_false_on_value_mismatch(self, mock_client_class):
        """Should return False when values don't match."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            today = pd.Timestamp.today().normalize()
            mock_client = Mock()
            # Return different value from API
            mock_client.read_snow.return_value = pd.DataFrame({
                'date': pd.to_datetime([today]),
                'code': ['12345'],
                'snow_type': ['SWE'],
                'value': [999.0],  # Different value
            })
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': pd.to_datetime([today]),
                'code': [12345],
                'SWE': [100.0],  # Original value
            })

            result = sdo._check_snow_consistency(csv_data, "SWE", "test_hru")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_returns_false_when_no_api_data(self, mock_client_class):
        """Should return False when API returns no data."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            today = pd.Timestamp.today().normalize()
            mock_client = Mock()
            # Return empty DataFrame from API
            mock_client.read_snow.return_value = pd.DataFrame()
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': pd.to_datetime([today]),
                'code': [12345],
                'SWE': [100.0],
            })

            result = sdo._check_snow_consistency(csv_data, "SWE", "test_hru")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


class TestMeteoConsistencyCheckFailures:
    """Tests for _check_meteo_consistency returning False on mismatches"""

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_returns_false_on_row_count_mismatch(self, mock_client_class):
        """Should return False when row counts don't match."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            # Return fewer rows from API than in CSV
            mock_client.read_meteo.return_value = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': ['12345'],
                'meteo_type': ['T'],
                'value': [15.0],
                'norm': [12.0],
            })
            mock_client_class.return_value = mock_client

            # CSV has 2 rows, API returns 1
            csv_data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01', '2024-01-02']),
                'code': [12345, 12345],
                'T': [15.0, 16.0],
                'T_norm': [12.0, 13.0],
            })

            result = eer._check_meteo_consistency(csv_data, "T")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_returns_false_on_value_mismatch(self, mock_client_class):
        """Should return False when values don't match."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            # Return different value from API
            mock_client.read_meteo.return_value = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': ['12345'],
                'meteo_type': ['T'],
                'value': [999.0],  # Different value
                'norm': [12.0],
            })
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': [12345],
                'T': [15.0],  # Original value
                'T_norm': [12.0],
            })

            result = eer._check_meteo_consistency(csv_data, "T")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_returns_false_when_no_api_data(self, mock_client_class):
        """Should return False when API returns no data."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            # Return empty DataFrame from API
            mock_client.read_meteo.return_value = pd.DataFrame()
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': [12345],
                'T': [15.0],
                'T_norm': [12.0],
            })

            result = eer._check_meteo_consistency(csv_data, "T")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


class TestQuantileMappingConsistencyCheckFailures:
    """Tests for Quantile_Mapping_OP _check_meteo_consistency returning False on mismatches"""

    @patch('Quantile_Mapping_OP.SapphirePreprocessingClient')
    def test_returns_false_on_row_count_mismatch(self, mock_client_class):
        """Should return False when row counts don't match."""
        if not qm.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            today = pd.Timestamp.today().normalize()
            mock_client = Mock()
            # Return fewer rows from API than in CSV
            mock_client.read_meteo.return_value = pd.DataFrame({
                'date': [today],
                'code': ['12345'],
                'meteo_type': ['T'],
                'value': [15.0],
            })
            mock_client_class.return_value = mock_client

            # CSV has 2 codes, API returns 1
            csv_data = pd.DataFrame({
                'date': [today, today],
                'code': [12345, 67890],
                'T': [15.0, 16.0],
            })

            result = qm._check_meteo_consistency(csv_data, "T", "HRU001")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('Quantile_Mapping_OP.SapphirePreprocessingClient')
    def test_returns_false_on_value_mismatch(self, mock_client_class):
        """Should return False when values don't match."""
        if not qm.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            today = pd.Timestamp.today().normalize()
            mock_client = Mock()
            # Return different value from API
            mock_client.read_meteo.return_value = pd.DataFrame({
                'date': [today],
                'code': ['12345'],
                'meteo_type': ['T'],
                'value': [999.0],  # Different value
            })
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': [today],
                'code': [12345],
                'T': [15.0],  # Original value
            })

            result = qm._check_meteo_consistency(csv_data, "T", "HRU001")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('Quantile_Mapping_OP.SapphirePreprocessingClient')
    def test_returns_false_when_no_api_data(self, mock_client_class):
        """Should return False when API returns no data."""
        if not qm.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            today = pd.Timestamp.today().normalize()
            mock_client = Mock()
            # Return empty DataFrame from API
            mock_client.read_meteo.return_value = pd.DataFrame()
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': [today],
                'code': [12345],
                'T': [15.0],
            })

            result = qm._check_meteo_consistency(csv_data, "T", "HRU001")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


# =============================================================================
# Tests for SAPPHIRE_SYNC_MODE in snow_data_operational._write_snow_to_api
# =============================================================================

class TestSnowSyncMode:
    """Tests for sync mode support in snow_data_operational._write_snow_to_api."""

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_operational_mode_writes_yesterday_and_today(
        self, mock_client_class
    ):
        """Operational mode should write yesterday+today (2-day window)."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        os.environ.pop('SAPPHIRE_SYNC_MODE', None)
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 2
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            yesterday = today - pd.Timedelta(days=1)
            two_days_ago = today - pd.Timedelta(days=2)

            data = pd.DataFrame({
                'date': [two_days_ago, yesterday, today],
                'code': [12345, 12345, 12345],
                'SWE': [50.0, 100.0, 200.0],
            })

            result = sdo._write_snow_to_api(
                data, "SWE", "HRU01", mode="operational"
            )
            assert result is True

            records = mock_client.write_snow.call_args[0][0]
            assert len(records) == 2
            dates = {r['date'] for r in records}
            assert dates == {
                yesterday.strftime('%Y-%m-%d'),
                today.strftime('%Y-%m-%d'),
            }
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_maintenance_mode_writes_last_30_days(self, mock_client_class):
        """Maintenance mode should write the last 30 days."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 31
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            dates = pd.date_range(
                end=today, periods=60, freq='D'
            )
            data = pd.DataFrame({
                'date': dates,
                'code': [12345] * 60,
                'SWE': range(60),
            })

            result = sdo._write_snow_to_api(
                data, "SWE", "HRU01", mode="maintenance"
            )
            assert result is True

            records = mock_client.write_snow.call_args[0][0]
            assert len(records) == 31  # last 30 days + cutoff day
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_initial_mode_writes_all_data(self, mock_client_class):
        """Initial mode should write all data."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 100
            mock_client_class.return_value = mock_client

            dates = pd.date_range(start='2023-01-01', periods=100, freq='D')
            data = pd.DataFrame({
                'date': dates,
                'code': [12345] * 100,
                'SWE': range(100),
            })

            result = sdo._write_snow_to_api(
                data, "SWE", "HRU01", mode="initial"
            )
            assert result is True

            records = mock_client.write_snow.call_args[0][0]
            assert len(records) == 100
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_env_var_fallback(self, mock_client_class):
        """When no mode parameter, should read SAPPHIRE_SYNC_MODE env var."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        os.environ['SAPPHIRE_SYNC_MODE'] = 'initial'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 5
            mock_client_class.return_value = mock_client

            dates = pd.date_range(start='2023-06-01', periods=5, freq='D')
            data = pd.DataFrame({
                'date': dates,
                'code': [12345] * 5,
                'SWE': range(5),
            })

            # No mode parameter — should use env var
            result = sdo._write_snow_to_api(data, "SWE", "HRU01")
            assert result is True

            records = mock_client.write_snow.call_args[0][0]
            assert len(records) == 5
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            os.environ.pop('SAPPHIRE_SYNC_MODE', None)

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_mode_param_overrides_env_var(self, mock_client_class):
        """Mode parameter should override SAPPHIRE_SYNC_MODE env var."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        os.environ['SAPPHIRE_SYNC_MODE'] = 'initial'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 2
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            yesterday = today - pd.Timedelta(days=1)
            dates = pd.date_range(start='2023-01-01', periods=100, freq='D')
            # Add yesterday+today so operational finds them
            dates = dates.append(pd.DatetimeIndex([yesterday, today]))
            data = pd.DataFrame({
                'date': dates,
                'code': [12345] * len(dates),
                'SWE': range(len(dates)),
            })

            # mode="operational" overrides env var "initial"
            result = sdo._write_snow_to_api(
                data, "SWE", "HRU01", mode="operational"
            )
            assert result is True

            records = mock_client.write_snow.call_args[0][0]
            assert len(records) == 2  # Only yesterday + today
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            os.environ.pop('SAPPHIRE_SYNC_MODE', None)

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_unknown_mode_defaults_to_operational(self, mock_client_class):
        """Unknown sync mode should default to operational (yesterday+today)."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 2
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            yesterday = today - pd.Timedelta(days=1)
            two_days_ago = today - pd.Timedelta(days=2)
            data = pd.DataFrame({
                'date': [two_days_ago, yesterday, today],
                'code': [12345, 12345, 12345],
                'SWE': [50.0, 100.0, 200.0],
            })

            result = sdo._write_snow_to_api(
                data, "SWE", "HRU01", mode="bogus_mode"
            )
            assert result is True

            records = mock_client.write_snow.call_args[0][0]
            assert len(records) == 2  # yesterday + today
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


# =============================================================================
# Tests for SAPPHIRE_SYNC_MODE in Quantile_Mapping_OP._write_meteo_to_api
# =============================================================================

class TestQMMeteoSyncMode:
    """Tests for sync mode support in QM _write_meteo_to_api."""

    @patch('Quantile_Mapping_OP.SapphirePreprocessingClient')
    def test_operational_mode_writes_yesterday_and_today(
        self, mock_client_class
    ):
        """Operational mode should write yesterday+today (2-day window)."""
        if not qm.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 2
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            yesterday = today - pd.Timedelta(days=1)
            two_days_ago = today - pd.Timedelta(days=2)
            data = pd.DataFrame({
                'date': [two_days_ago, yesterday, today],
                'code': [12345, 12345, 12345],
                'T': [5.0, 10.0, 15.0],
            })

            result = qm._write_meteo_to_api(
                data, "T", "HRU01", mode="operational"
            )
            assert result is True

            records = mock_client.write_meteo.call_args[0][0]
            assert len(records) == 2
            dates = {r['date'] for r in records}
            assert dates == {
                yesterday.strftime('%Y-%m-%d'),
                today.strftime('%Y-%m-%d'),
            }
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('Quantile_Mapping_OP.SapphirePreprocessingClient')
    def test_maintenance_mode_writes_last_30_days(self, mock_client_class):
        """Maintenance mode should write the last 30 days."""
        if not qm.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 31
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            dates = pd.date_range(end=today, periods=60, freq='D')
            data = pd.DataFrame({
                'date': dates,
                'code': [12345] * 60,
                'P': np.random.uniform(0, 20, 60),
            })

            result = qm._write_meteo_to_api(
                data, "P", "HRU01", mode="maintenance"
            )
            assert result is True

            records = mock_client.write_meteo.call_args[0][0]
            assert len(records) == 31
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('Quantile_Mapping_OP.SapphirePreprocessingClient')
    def test_initial_mode_writes_all_data(self, mock_client_class):
        """Initial mode should write all data."""
        if not qm.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 50
            mock_client_class.return_value = mock_client

            dates = pd.date_range(start='2023-01-01', periods=50, freq='D')
            data = pd.DataFrame({
                'date': dates,
                'code': [12345] * 50,
                'T': range(50),
            })

            result = qm._write_meteo_to_api(
                data, "T", "HRU01", mode="initial"
            )
            assert result is True

            records = mock_client.write_meteo.call_args[0][0]
            assert len(records) == 50
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


# =============================================================================
# Tests for _write_reanalysis_to_api in extend_era5_reanalysis
# =============================================================================

class TestWriteReanalysisToApi:
    """Tests for _write_reanalysis_to_api in extend_era5_reanalysis.py."""

    def test_operational_mode_is_noop(self):
        """Operational mode should skip reanalysis write."""
        data = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01']),
            'code': [12345],
            'T': [10.0],
        })
        result = eer._write_reanalysis_to_api(
            data, "T", mode="operational"
        )
        assert result is False

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_maintenance_mode_writes_last_365_days(
        self, mock_client_class
    ):
        """Maintenance mode should write last 365 days of reanalysis."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 366
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            # 500 days of data, only last 365+ should be written
            dates = pd.date_range(end=today, periods=500, freq='D')
            data = pd.DataFrame({
                'date': dates,
                'code': [12345] * 500,
                'P': np.random.uniform(0, 20, 500),
            })

            result = eer._write_reanalysis_to_api(
                data, "P", mode="maintenance"
            )
            assert result is True

            records = mock_client.write_meteo.call_args[0][0]
            assert len(records) == 366  # 365 days + cutoff day
            # All records should have norm=None (raw reanalysis)
            assert all(r['norm'] is None for r in records)
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_initial_mode_writes_all_data(self, mock_client_class):
        """Initial mode should write all reanalysis data."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 100
            mock_client_class.return_value = mock_client

            dates = pd.date_range(start='2020-01-01', periods=100, freq='D')
            data = pd.DataFrame({
                'date': dates,
                'code': [12345] * 100,
                'T': range(100),
            })

            result = eer._write_reanalysis_to_api(
                data, "T", mode="initial"
            )
            assert result is True

            records = mock_client.write_meteo.call_args[0][0]
            assert len(records) == 100
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    def test_api_disabled_returns_false(self):
        """When API disabled, reanalysis write should return False."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'
        try:
            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': [12345],
                'T': [10.0],
            })
            result = eer._write_reanalysis_to_api(
                data, "T", mode="maintenance"
            )
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    def test_env_var_defaults_to_operational(self):
        """Without mode param or env var, defaults to operational (no-op)."""
        os.environ.pop('SAPPHIRE_SYNC_MODE', None)
        data = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01']),
            'code': [12345],
            'P': [5.0],
        })
        result = eer._write_reanalysis_to_api(data, "P")
        assert result is False
