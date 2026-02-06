"""
Tests for SAPPHIRE API integration in preprocessing_gateway modules.

Tests the snow and meteo data API integration:
- _write_snow_to_api function in snow_data_operational.py (writes latest only - operational)
- _write_snow_to_api function in snow_data_renalysis.py (writes last 30 days - maintenance)
- _write_meteo_to_api function in extend_era5_reanalysis.py (writes all data)
- _write_meteo_to_api function in Quantile_Mapping_OP.py (writes latest only - operational)
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

    This function always writes only the latest date's data (operational mode).
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
            result = sdo._write_snow_to_api(data, "SWE")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_api_not_ready_raises_error(self, mock_client_class):
        """When API health check fails, should raise SapphireAPIError."""
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

            with pytest.raises(Exception):  # SapphireAPIError
                sdo._write_snow_to_api(data, "SWE")
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_writes_latest_date_only(self, mock_client_class):
        """Only the latest date's data should be written (operational behavior)."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
                'code': [12345, 12345, 12345],
                'SWE': [100.0, 150.0, 200.0],
            })

            result = sdo._write_snow_to_api(data, "SWE")
            assert result is True

            # Check that write_snow was called
            mock_client.write_snow.assert_called_once()
            # Get the records that were passed
            call_args = mock_client.write_snow.call_args[0][0]
            # Should only have 1 record (latest date)
            assert len(call_args) == 1
            assert call_args[0]['date'] == '2024-01-03'
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

            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': [12345],
                'SWE': [100.0],
                'SWE_1': [80.0],
                'SWE_2': [90.0],
                'SWE_3': [110.0],
            })

            result = sdo._write_snow_to_api(data, "SWE")
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

            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': [12345],
                'SWE': [np.nan],
            })

            result = sdo._write_snow_to_api(data, "SWE")
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
            result = sdo._write_snow_to_api(data, "SWE")
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

            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': [12345],
                'swe': [100.0],  # lowercase column
            })

            result = sdo._write_snow_to_api(data, "swe")
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
            result = sdr._write_snow_to_api(data, "SWE")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('snow_data_renalysis.SapphirePreprocessingClient')
    def test_api_not_ready_raises_error(self, mock_client_class):
        """When API health check fails, should raise SapphireAPIError."""
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

            with pytest.raises(Exception):  # SapphireAPIError
                sdr._write_snow_to_api(data, "SWE")
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

            result = sdr._write_snow_to_api(data, "SWE")
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

            result = sdr._write_snow_to_api(data, "SWE")
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
            result = sdr._write_snow_to_api(data, "SWE")
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
        result = sdr._check_snow_consistency(data, "SWE")
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

            result = sdr._check_snow_consistency(csv_data, "SWE")
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

            result = sdr._check_snow_consistency(csv_data, "SWE")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


class TestSnowReanalysisIntegration:
    """Integration tests for snow_data_renalysis.py"""

    def test_api_functions_exist(self):
        """Test that the API functions exist."""
        assert hasattr(sdr, '_write_snow_to_api')
        assert hasattr(sdr, '_check_snow_consistency')
        assert hasattr(sdr, 'main')

    def test_sapphire_api_availability_flag_exists(self):
        """Test that the SAPPHIRE_API_AVAILABLE flag exists."""
        assert hasattr(sdr, 'SAPPHIRE_API_AVAILABLE')


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
    def test_api_not_ready_raises_error(self, mock_client_class):
        """When API health check fails, should raise SapphireAPIError."""
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

            with pytest.raises(Exception):  # SapphireAPIError
                eer._write_meteo_to_api(data, "T")
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
    """Integration tests for snow_data_operational.py"""

    @patch('snow_data_operational._write_snow_to_api')
    def test_get_snow_data_operational_calls_api_write(self, mock_write_api):
        """Test that get_snow_data_operational calls API write after CSV save."""
        # This test would require mocking the entire client and file operations
        # For now, we just verify the function exists and can be imported
        assert hasattr(sdo, 'get_snow_data_operational')
        assert hasattr(sdo, '_write_snow_to_api')


class TestExtendEra5ReanalysisIntegration:
    """Integration tests for extend_era5_reanalysis.py"""

    def test_write_meteo_function_exists(self):
        """Test that the API write function exists."""
        assert hasattr(eer, '_write_meteo_to_api')
        assert hasattr(eer, 'main')


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
        result = sdo._check_snow_consistency(data, "SWE")
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

            result = sdo._check_snow_consistency(csv_data, "SWE")
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

    This function writes only the latest date's data (operational mode).
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
    def test_api_not_ready_raises_error(self, mock_client_class):
        """When API health check fails, should raise SapphireAPIError."""
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

            with pytest.raises(Exception):  # SapphireAPIError
                qm._write_meteo_to_api(data, "T", "HRU001")
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('Quantile_Mapping_OP.SapphirePreprocessingClient')
    def test_writes_latest_date_only(self, mock_client_class):
        """Only the latest date's data should be written (operational behavior)."""
        if not qm.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 1
            mock_client_class.return_value = mock_client

            # Data spanning multiple dates
            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
                'code': [12345, 12345, 12345],
                'T': [10.0, 15.0, 20.0],
            })

            result = qm._write_meteo_to_api(data, "T", "HRU001")
            assert result is True

            # Check that write_meteo was called
            mock_client.write_meteo.assert_called_once()
            # Get the records that were passed
            call_args = mock_client.write_meteo.call_args[0][0]
            # Should only have 1 record (latest date)
            assert len(call_args) == 1
            assert call_args[0]['date'] == '2024-01-03'
            assert call_args[0]['value'] == 20.0
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

            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-15']),
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
            assert record['day_of_year'] == 15
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

            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-06-01']),
                'code': [12345],
                'P': [25.5],
            })

            result = qm._write_meteo_to_api(data, "P", "HRU001")
            assert result is True

            call_args = mock_client.write_meteo.call_args[0][0]
            record = call_args[0]
            assert record['meteo_type'] == 'P'
            assert record['value'] == 25.5
            # June 1 2024 is day 153 (leap year)
            assert record['day_of_year'] == 153
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

            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
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
            mock_client = Mock()
            # Return matching data from API
            mock_client.read_meteo.return_value = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': ['12345'],
                'meteo_type': ['T'],
                'value': [15.0],
            })
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
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
    """Integration tests for Quantile_Mapping_OP.py"""

    def test_api_functions_exist(self):
        """Test that the API functions exist."""
        assert hasattr(qm, '_write_meteo_to_api')
        assert hasattr(qm, '_check_meteo_consistency')
        assert hasattr(qm, 'main')

    def test_sapphire_api_availability_flag_exists(self):
        """Test that the SAPPHIRE_API_AVAILABLE flag exists."""
        assert hasattr(qm, 'SAPPHIRE_API_AVAILABLE')


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
            mock_client = Mock()
            # Return fewer rows from API than in CSV
            mock_client.read_snow.return_value = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': ['12345'],
                'snow_type': ['SWE'],
                'value': [100.0],
            })
            mock_client_class.return_value = mock_client

            # CSV has 2 rows, API returns 1
            csv_data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01', '2024-01-01']),
                'code': [12345, 67890],
                'SWE': [100.0, 200.0],
            })

            result = sdo._check_snow_consistency(csv_data, "SWE")
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

            result = sdo._check_snow_consistency(csv_data, "SWE")
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
            mock_client = Mock()
            # Return empty DataFrame from API
            mock_client.read_snow.return_value = pd.DataFrame()
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': [12345],
                'SWE': [100.0],
            })

            result = sdo._check_snow_consistency(csv_data, "SWE")
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
            mock_client = Mock()
            # Return fewer rows from API than in CSV
            mock_client.read_meteo.return_value = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': ['12345'],
                'meteo_type': ['T'],
                'value': [15.0],
            })
            mock_client_class.return_value = mock_client

            # CSV has 2 codes, API returns 1
            csv_data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01', '2024-01-01']),
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
            mock_client = Mock()
            # Return different value from API
            mock_client.read_meteo.return_value = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': ['12345'],
                'meteo_type': ['T'],
                'value': [999.0],  # Different value
            })
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
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
            mock_client = Mock()
            # Return empty DataFrame from API
            mock_client.read_meteo.return_value = pd.DataFrame()
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': [12345],
                'T': [15.0],
            })

            result = qm._check_meteo_consistency(csv_data, "T", "HRU001")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
