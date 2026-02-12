"""
Tests for SAPPHIRE API integration in forecast_library.py

Tests the linear regression forecast API integration:
- _write_lr_forecast_to_api helper function
- write_linreg_pentad_forecast_data with api_data parameter
- write_linreg_decad_forecast_data with api_data parameter
"""
import os
import tempfile
import pandas as pd
import numpy as np
import datetime as dt
import pytest
from unittest.mock import Mock, patch, MagicMock

import sys
# Add iEasyHydroForecast to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast'))

import forecast_library as fl


# =============================================================================
# Tests for _write_lr_forecast_to_api
# =============================================================================

class TestWriteLrForecastToApi:
    """Tests for the _write_lr_forecast_to_api helper function."""

    def test_api_disabled_via_env_var(self):
        """When SAPPHIRE_API_ENABLED=false, API write should be skipped."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'
        try:
            data = pd.DataFrame({
                'code': ['12345', '67890'],
                'date': pd.to_datetime(['2024-01-01', '2024-01-01']),
                'pentad_in_month': [1, 1],
                'pentad_in_year': [1, 1],
                'discharge_avg': [10.5, 20.0],
                'predictor': [5.2, 10.1],
                'slope': [0.5, 0.6],
                'intercept': [2.0, 3.0],
                'forecasted_discharge': [12.0, 22.0],
                'q_mean': [11.0, 21.0],
                'q_std_sigma': [1.5, 2.0],
                'delta': [0.1, 0.2],
                'rsquared': [0.85, 0.90]
            })
            result = fl._write_lr_forecast_to_api(data, "pentad")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    def test_invalid_horizon_type_raises_error(self):
        """Invalid horizon_type should raise ValueError."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            data = pd.DataFrame({
                'code': ['12345'],
                'date': pd.to_datetime(['2024-01-01']),
                'pentad_in_month': [1],
                'pentad_in_year': [1]
            })

            with pytest.raises(ValueError, match="Invalid horizon_type"):
                fl._write_lr_forecast_to_api(data, "invalid")
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePostprocessingClient')
    def test_api_not_ready_returns_false(self, mock_client_class):
        """When API health check fails, should return False (non-blocking)."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = False
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': ['12345'],
                'date': pd.to_datetime(['2024-01-01']),
                'pentad_in_month': [1],
                'pentad_in_year': [1],
                'discharge_avg': [10.5],
                'predictor': [5.2],
                'slope': [0.5],
                'intercept': [2.0],
                'forecasted_discharge': [12.0],
                'q_mean': [11.0],
                'q_std_sigma': [1.5],
                'delta': [0.1],
                'rsquared': [0.85]
            })

            result = fl._write_lr_forecast_to_api(data, "pentad")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePostprocessingClient')
    def test_api_write_pentad_success(self, mock_client_class):
        """When API write succeeds for pentad, should return True with correct records."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_lr_forecasts.return_value = 2
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': ['12345', '67890'],
                'date': pd.to_datetime(['2024-01-05', '2024-01-05']),
                'pentad_in_month': [1, 1],
                'pentad_in_year': [1, 1],
                'discharge_avg': [10.5, 20.0],
                'predictor': [5.2, 10.1],
                'slope': [0.5, 0.6],
                'intercept': [2.0, 3.0],
                'forecasted_discharge': [12.0, 22.0],
                'q_mean': [11.0, 21.0],
                'q_std_sigma': [1.5, 2.0],
                'delta': [0.1, 0.2],
                'rsquared': [0.85, 0.90]
            })

            result = fl._write_lr_forecast_to_api(data, "pentad")

            assert result is True
            mock_client.write_lr_forecasts.assert_called_once()

            # Verify records were prepared correctly
            call_args = mock_client.write_lr_forecasts.call_args[0][0]
            assert len(call_args) == 2

            # Check first record
            assert call_args[0]['horizon_type'] == 'pentad'
            assert call_args[0]['code'] == '12345'
            assert call_args[0]['date'] == '2024-01-05'
            assert call_args[0]['horizon_value'] == 1
            assert call_args[0]['horizon_in_year'] == 1
            assert call_args[0]['discharge_avg'] == 10.5
            assert call_args[0]['predictor'] == 5.2
            assert call_args[0]['slope'] == 0.5
            assert call_args[0]['intercept'] == 2.0
            assert call_args[0]['forecasted_discharge'] == 12.0
            assert call_args[0]['q_mean'] == 11.0
            assert call_args[0]['q_std_sigma'] == 1.5
            assert call_args[0]['delta'] == 0.1
            assert call_args[0]['rsquared'] == 0.85
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePostprocessingClient')
    def test_api_write_decade_success(self, mock_client_class):
        """When API write succeeds for decade, should use correct column names."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_lr_forecasts.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': ['12345'],
                'date': pd.to_datetime(['2024-01-10']),
                'decad_in_month': [1],
                'decad_in_year': [1],
                'discharge_avg': [15.5],
                'predictor': [7.2],
                'slope': [0.7],
                'intercept': [4.0],
                'forecasted_discharge': [18.0],
                'q_mean': [16.0],
                'q_std_sigma': [2.5],
                'delta': [0.15],
                'rsquared': [0.92]
            })

            result = fl._write_lr_forecast_to_api(data, "decade")

            assert result is True

            # Verify decade uses correct column names
            call_args = mock_client.write_lr_forecasts.call_args[0][0]
            assert call_args[0]['horizon_type'] == 'decade'
            assert call_args[0]['horizon_value'] == 1
            assert call_args[0]['horizon_in_year'] == 1
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePostprocessingClient')
    def test_api_handles_nan_values(self, mock_client_class):
        """NaN values in data should be converted to None in API records."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_lr_forecasts.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': ['12345'],
                'date': pd.to_datetime(['2024-01-05']),
                'pentad_in_month': [1],
                'pentad_in_year': [1],
                'discharge_avg': [np.nan],
                'predictor': [np.nan],
                'slope': [0.5],
                'intercept': [2.0],
                'forecasted_discharge': [np.nan],
                'q_mean': [11.0],
                'q_std_sigma': [1.5],
                'delta': [0.1],
                'rsquared': [0.85]
            })

            result = fl._write_lr_forecast_to_api(data, "pentad")

            assert result is True
            call_args = mock_client.write_lr_forecasts.call_args[0][0]
            assert call_args[0]['discharge_avg'] is None
            assert call_args[0]['predictor'] is None
            assert call_args[0]['forecasted_discharge'] is None
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


# =============================================================================
# Tests for write_linreg_pentad_forecast_data with API integration
# =============================================================================

class TestWriteLinregPentadForecastWithApi:
    """Tests for write_linreg_pentad_forecast_data with api_data parameter."""

    @pytest.fixture
    def sample_pentad_data(self):
        """Create sample pentad forecast data."""
        return pd.DataFrame({
            'code': ['12345', '12345', '67890', '67890'],
            'date': pd.to_datetime(['2024-01-01', '2024-01-05', '2024-01-01', '2024-01-05']),
            'issue_date': [False, True, False, True],
            'discharge': [8.0, 10.0, 18.0, 20.0],
            'pentad_in_month': [1, 1, 1, 1],
            'pentad_in_year': [1, 1, 1, 1],
            'discharge_avg': [10.5, 10.5, 20.0, 20.0],
            'predictor': [5.2, 5.2, 10.1, 10.1],
            'slope': [0.5, 0.5, 0.6, 0.6],
            'intercept': [2.0, 2.0, 3.0, 3.0],
            'forecasted_discharge': [12.0, 12.0, 22.0, 22.0],
            'q_mean': [11.0, 11.0, 21.0, 21.0],
            'q_std_sigma': [1.5, 1.5, 2.0, 2.0],
            'delta': [0.1, 0.1, 0.2, 0.2],
            'rsquared': [0.85, 0.85, 0.90, 0.90]
        })

    @pytest.fixture
    def temp_env(self, tmp_path):
        """Set up temporary environment variables for testing."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        os.environ['ieasyforecast_intermediate_data_path'] = str(output_dir)
        os.environ['ieasyforecast_analysis_pentad_file'] = 'forecast_pentad_linreg.csv'
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'  # Disable API by default

        yield output_dir

        os.environ.pop('ieasyforecast_intermediate_data_path', None)
        os.environ.pop('ieasyforecast_analysis_pentad_file', None)
        os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library._write_lr_forecast_to_api')
    def test_default_api_data_uses_last_line(self, mock_api_write, sample_pentad_data, temp_env):
        """When api_data is None, should use last_line for API."""
        mock_api_write.return_value = True
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'

        fl.write_linreg_pentad_forecast_data(sample_pentad_data)

        mock_api_write.assert_called_once()
        api_data_arg = mock_api_write.call_args[0][0]
        horizon_type_arg = mock_api_write.call_args[0][1]

        # Should have 2 records (one per station code)
        assert len(api_data_arg) == 2
        assert horizon_type_arg == "pentad"

    @patch('forecast_library._write_lr_forecast_to_api')
    def test_empty_api_data_skips_api_write(self, mock_api_write, sample_pentad_data, temp_env):
        """When api_data is empty DataFrame, should skip API write."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'

        fl.write_linreg_pentad_forecast_data(sample_pentad_data, api_data=pd.DataFrame())

        mock_api_write.assert_not_called()

    @patch('forecast_library._write_lr_forecast_to_api')
    def test_custom_api_data_passed_through(self, mock_api_write, sample_pentad_data, temp_env):
        """When api_data is provided, should use it instead of last_line."""
        mock_api_write.return_value = True
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'

        custom_api_data = pd.DataFrame({
            'code': ['99999'],
            'date': pd.to_datetime(['2024-01-05']),
            'pentad_in_month': [1],
            'pentad_in_year': [1],
            'discharge_avg': [100.0],
            'predictor': [50.0],
            'slope': [1.0],
            'intercept': [5.0],
            'forecasted_discharge': [55.0],
            'q_mean': [52.0],
            'q_std_sigma': [3.0],
            'delta': [0.05],
            'rsquared': [0.95]
        })

        fl.write_linreg_pentad_forecast_data(sample_pentad_data, api_data=custom_api_data)

        mock_api_write.assert_called_once()
        api_data_arg = mock_api_write.call_args[0][0]

        # Should use custom data
        assert len(api_data_arg) == 1
        assert api_data_arg['code'].iloc[0] == '99999'

    @patch('forecast_library._write_lr_forecast_to_api')
    def test_csv_still_written_when_api_fails(self, mock_api_write, sample_pentad_data, temp_env):
        """CSV should still be written even if API write fails."""
        mock_api_write.side_effect = Exception("API error")
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'

        fl.write_linreg_pentad_forecast_data(sample_pentad_data)

        # CSV should exist
        csv_path = temp_env / 'forecast_pentad_linreg.csv'
        assert csv_path.exists()


# =============================================================================
# Tests for write_linreg_decad_forecast_data with API integration
# =============================================================================

class TestWriteLinregDecadForecastWithApi:
    """Tests for write_linreg_decad_forecast_data with api_data parameter."""

    @pytest.fixture
    def sample_decad_data(self):
        """Create sample decad forecast data."""
        return pd.DataFrame({
            'code': ['12345', '12345', '67890', '67890'],
            'date': pd.to_datetime(['2024-01-01', '2024-01-10', '2024-01-01', '2024-01-10']),
            'issue_date': [False, True, False, True],
            'discharge': [8.0, 10.0, 18.0, 20.0],
            'decad_in_month': [1, 1, 1, 1],
            'decad_in_year': [1, 1, 1, 1],
            'discharge_avg': [10.5, 10.5, 20.0, 20.0],
            'predictor': [5.2, 5.2, 10.1, 10.1],
            'slope': [0.5, 0.5, 0.6, 0.6],
            'intercept': [2.0, 2.0, 3.0, 3.0],
            'forecasted_discharge': [12.0, 12.0, 22.0, 22.0],
            'q_mean': [11.0, 11.0, 21.0, 21.0],
            'q_std_sigma': [1.5, 1.5, 2.0, 2.0],
            'delta': [0.1, 0.1, 0.2, 0.2],
            'rsquared': [0.85, 0.85, 0.90, 0.90]
        })

    @pytest.fixture
    def temp_env(self, tmp_path):
        """Set up temporary environment variables for testing."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        os.environ['ieasyforecast_intermediate_data_path'] = str(output_dir)
        os.environ['ieasyforecast_analysis_decad_file'] = 'forecast_decad_linreg.csv'
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'

        yield output_dir

        os.environ.pop('ieasyforecast_intermediate_data_path', None)
        os.environ.pop('ieasyforecast_analysis_decad_file', None)
        os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library._write_lr_forecast_to_api')
    def test_default_api_data_uses_last_line(self, mock_api_write, sample_decad_data, temp_env):
        """When api_data is None, should use last_line for API."""
        mock_api_write.return_value = True
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'

        fl.write_linreg_decad_forecast_data(sample_decad_data)

        mock_api_write.assert_called_once()
        api_data_arg = mock_api_write.call_args[0][0]
        horizon_type_arg = mock_api_write.call_args[0][1]

        # Should have 2 records (one per station code)
        assert len(api_data_arg) == 2
        assert horizon_type_arg == "decade"

    @patch('forecast_library._write_lr_forecast_to_api')
    def test_empty_api_data_skips_api_write(self, mock_api_write, sample_decad_data, temp_env):
        """When api_data is empty DataFrame, should skip API write."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'

        fl.write_linreg_decad_forecast_data(sample_decad_data, api_data=pd.DataFrame())

        mock_api_write.assert_not_called()

    @patch('forecast_library._write_lr_forecast_to_api')
    def test_csv_still_written_when_api_fails(self, mock_api_write, sample_decad_data, temp_env):
        """CSV should still be written even if API write fails."""
        mock_api_write.side_effect = Exception("API error")
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'

        fl.write_linreg_decad_forecast_data(sample_decad_data)

        # CSV should exist
        csv_path = temp_env / 'forecast_decad_linreg.csv'
        assert csv_path.exists()


# =============================================================================
# Tests for _read_daily_discharge_from_api
# =============================================================================

class TestReadDailyDischargeFromApi:
    """Tests for the _read_daily_discharge_from_api helper function."""

    @patch('forecast_library.SapphirePreprocessingClient')
    def test_api_read_success(self, mock_client_class):
        """When API read succeeds, should return DataFrame with correct columns."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            # First call returns data, second call returns empty (end of pagination)
            mock_client.read_runoff.side_effect = [
                pd.DataFrame({
                    'id': [1, 2],
                    'horizon_type': ['day', 'day'],
                    'code': ['12345', '67890'],
                    'date': ['2024-01-01', '2024-01-02'],
                    'discharge': [10.5, 20.0],
                    'predictor': [None, None],
                    'horizon_value': [1, 2],
                    'horizon_in_year': [1, 2],
                }),
                pd.DataFrame()  # Empty = end of pagination
            ]
            mock_client_class.return_value = mock_client

            result = fl._read_daily_discharge_from_api()

            assert isinstance(result, pd.DataFrame)
            assert 'code' in result.columns
            assert 'date' in result.columns
            assert 'discharge' in result.columns
            assert len(result) == 2
            assert result['code'].iloc[0] == '12345'
            assert result['discharge'].iloc[0] == 10.5
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePreprocessingClient')
    def test_api_read_with_site_codes(self, mock_client_class):
        """When site_codes provided, should query per code."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.read_runoff.return_value = pd.DataFrame()
            mock_client_class.return_value = mock_client

            fl._read_daily_discharge_from_api(site_codes=['12345', '67890'])

            # Should be called once per code (plus once more each for empty result)
            calls = mock_client.read_runoff.call_args_list
            codes_queried = [call.kwargs.get('code') for call in calls]
            assert '12345' in codes_queried
            assert '67890' in codes_queried
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePreprocessingClient')
    def test_api_not_ready_raises_error(self, mock_client_class):
        """When API health check fails, should raise SapphireAPIError."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = False
            mock_client_class.return_value = mock_client

            with pytest.raises(fl.SapphireAPIError, match="not ready"):
                fl._read_daily_discharge_from_api()
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    def test_api_not_available_raises_runtime_error(self):
        """When sapphire-api-client not installed, should raise RuntimeError."""
        original = fl.SAPPHIRE_API_AVAILABLE
        try:
            fl.SAPPHIRE_API_AVAILABLE = False

            with pytest.raises(RuntimeError, match="sapphire-api-client is not installed"):
                fl._read_daily_discharge_from_api()
        finally:
            fl.SAPPHIRE_API_AVAILABLE = original


# =============================================================================
# Tests for read_daily_discharge_data (unified function)
# =============================================================================

class TestReadDailyDischargeData:
    """Tests for the read_daily_discharge_data unified function."""

    @patch('forecast_library._read_daily_discharge_from_api')
    def test_api_enabled_uses_api(self, mock_api_read):
        """When SAPPHIRE_API_ENABLED=true, should use API."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_api_read.return_value = pd.DataFrame({
                'code': ['12345'],
                'date': pd.to_datetime(['2024-01-01']),
                'discharge': [10.5]
            })

            result = fl.read_daily_discharge_data()

            mock_api_read.assert_called_once()
            assert len(result) == 1
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.read_daily_discharge_data_from_csv')
    def test_api_disabled_uses_csv(self, mock_csv_read):
        """When SAPPHIRE_API_ENABLED=false, should use CSV."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'
        try:
            mock_csv_read.return_value = pd.DataFrame({
                'code': ['12345'],
                'date': pd.to_datetime(['2024-01-01']),
                'discharge': [10.5]
            })

            result = fl.read_daily_discharge_data()

            mock_csv_read.assert_called_once()
            assert len(result) == 1
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library._read_daily_discharge_from_api')
    def test_api_fail_raises_error_no_fallback(self, mock_api_read):
        """When API fails with SAPPHIRE_API_ENABLED=true, should raise error (no CSV fallback)."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_api_read.side_effect = fl.SapphireAPIError("API unavailable")

            with pytest.raises(fl.SapphireAPIError):
                fl.read_daily_discharge_data()
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library._read_daily_discharge_from_api')
    def test_site_codes_passed_to_api(self, mock_api_read):
        """When site_codes provided, should pass them to API function."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_api_read.return_value = pd.DataFrame({
                'code': ['12345'],
                'date': pd.to_datetime(['2024-01-01']),
                'discharge': [10.5]
            })

            fl.read_daily_discharge_data(site_codes=['12345', '67890'])

            mock_api_read.assert_called_once_with(
                site_codes=['12345', '67890'],
                start_date=None,
                end_date=None,
            )
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library._read_daily_discharge_from_api')
    def test_default_api_enabled_true(self, mock_api_read):
        """When SAPPHIRE_API_ENABLED not set, should default to API."""
        # Ensure env var is not set
        os.environ.pop('SAPPHIRE_API_ENABLED', None)
        try:
            mock_api_read.return_value = pd.DataFrame({
                'code': ['12345'],
                'date': pd.to_datetime(['2024-01-01']),
                'discharge': [10.5]
            })

            fl.read_daily_discharge_data()

            mock_api_read.assert_called_once()
        finally:
            pass


# =============================================================================
# Consistency Tests: CSV vs API Data
# =============================================================================

class TestWriteConsistency:
    """Tests to verify CSV and API writes contain the same data."""

    @pytest.fixture
    def sample_forecast_data(self):
        """Create sample forecast data for consistency testing."""
        return pd.DataFrame({
            'code': ['12345', '12345', '67890', '67890'],
            'date': pd.to_datetime(['2024-01-01', '2024-01-05', '2024-01-01', '2024-01-05']),
            'issue_date': [False, True, False, True],
            'discharge': [8.0, 10.0, 18.0, 20.0],
            'pentad_in_month': [1, 1, 1, 1],
            'pentad_in_year': [1, 1, 1, 1],
            'discharge_avg': [10.5, 10.5, 20.0, 20.0],
            'predictor': [5.2, 5.2, 10.1, 10.1],
            'slope': [0.5, 0.5, 0.6, 0.6],
            'intercept': [2.0, 2.0, 3.0, 3.0],
            'forecasted_discharge': [12.0, 12.0, 22.0, 22.0],
            'q_mean': [11.0, 11.0, 21.0, 21.0],
            'q_std_sigma': [1.5, 1.5, 2.0, 2.0],
            'delta': [0.1, 0.1, 0.2, 0.2],
            'rsquared': [0.85, 0.85, 0.90, 0.90]
        })

    @pytest.fixture
    def temp_env_for_consistency(self, tmp_path):
        """Set up temporary environment for consistency tests."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        os.environ['ieasyforecast_intermediate_data_path'] = str(output_dir)
        os.environ['ieasyforecast_analysis_pentad_file'] = 'forecast_pentad_linreg.csv'
        os.environ['ieasyforecast_analysis_decad_file'] = 'forecast_decad_linreg.csv'

        yield output_dir

        os.environ.pop('ieasyforecast_intermediate_data_path', None)
        os.environ.pop('ieasyforecast_analysis_pentad_file', None)
        os.environ.pop('ieasyforecast_analysis_decad_file', None)

    @patch('forecast_library._write_lr_forecast_to_api')
    def test_pentad_csv_and_api_data_match(self, mock_api_write, sample_forecast_data, temp_env_for_consistency):
        """Verify pentad CSV data matches API data for the same input."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_api_write.return_value = True
            captured_api_data = []

            def capture_api_data(data, horizon_type):
                captured_api_data.append(data.copy())
                return True

            mock_api_write.side_effect = capture_api_data

            fl.write_linreg_pentad_forecast_data(sample_forecast_data)

            # Read back the CSV
            csv_path = temp_env_for_consistency / 'forecast_pentad_linreg.csv'
            csv_data = pd.read_csv(csv_path)

            # Get the API data that was sent
            assert len(captured_api_data) == 1
            api_data = captured_api_data[0]

            # The CSV has all data (issue_date column is dropped during processing)
            # Compare by matching codes - CSV should have same stations as API
            csv_codes = set(csv_data['code'].astype(str).unique())
            api_codes = set(api_data['code'].astype(str).unique())
            assert csv_codes == api_codes, f"Code mismatch: CSV={csv_codes}, API={api_codes}"

            # Compare key forecast columns for matching codes
            key_columns = ['discharge_avg', 'predictor', 'slope', 'intercept',
                          'forecasted_discharge', 'q_mean', 'q_std_sigma', 'delta', 'rsquared']

            for code in api_codes:
                api_row = api_data[api_data['code'].astype(str) == code].iloc[0]
                csv_row = csv_data[csv_data['code'].astype(str) == code].iloc[-1]  # Last row for this code

                for col in key_columns:
                    if col in csv_row and col in api_row:
                        csv_val = csv_row[col]
                        api_val = api_row[col]
                        if pd.notna(csv_val) and pd.notna(api_val):
                            assert abs(float(csv_val) - float(api_val)) < 0.001, \
                                f"Column {col} mismatch for code {code}: CSV={csv_val}, API={api_val}"

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library._write_lr_forecast_to_api')
    def test_decad_csv_and_api_data_match(self, mock_api_write, temp_env_for_consistency):
        """Verify decad CSV data matches API data for the same input."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        # Create decad-specific test data
        sample_decad_data = pd.DataFrame({
            'code': ['12345', '12345', '67890', '67890'],
            'date': pd.to_datetime(['2024-01-01', '2024-01-10', '2024-01-01', '2024-01-10']),
            'issue_date': [False, True, False, True],
            'discharge': [8.0, 10.0, 18.0, 20.0],
            'decad_in_month': [1, 1, 1, 1],
            'decad_in_year': [1, 1, 1, 1],
            'discharge_avg': [10.5, 10.5, 20.0, 20.0],
            'predictor': [5.2, 5.2, 10.1, 10.1],
            'slope': [0.5, 0.5, 0.6, 0.6],
            'intercept': [2.0, 2.0, 3.0, 3.0],
            'forecasted_discharge': [12.0, 12.0, 22.0, 22.0],
            'q_mean': [11.0, 11.0, 21.0, 21.0],
            'q_std_sigma': [1.5, 1.5, 2.0, 2.0],
            'delta': [0.1, 0.1, 0.2, 0.2],
            'rsquared': [0.85, 0.85, 0.90, 0.90]
        })

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_api_write.return_value = True
            captured_api_data = []

            def capture_api_data(data, horizon_type):
                captured_api_data.append(data.copy())
                return True

            mock_api_write.side_effect = capture_api_data

            fl.write_linreg_decad_forecast_data(sample_decad_data)

            # Read back the CSV
            csv_path = temp_env_for_consistency / 'forecast_decad_linreg.csv'
            csv_data = pd.read_csv(csv_path)

            # Get the API data that was sent
            assert len(captured_api_data) == 1
            api_data = captured_api_data[0]

            # Compare by matching codes - CSV should have same stations as API
            csv_codes = set(csv_data['code'].astype(str).unique())
            api_codes = set(api_data['code'].astype(str).unique())
            assert csv_codes == api_codes, f"Code mismatch: CSV={csv_codes}, API={api_codes}"

            # Compare key forecast columns for matching codes
            key_columns = ['discharge_avg', 'predictor', 'slope', 'intercept',
                          'forecasted_discharge', 'q_mean', 'q_std_sigma', 'delta', 'rsquared']

            for code in api_codes:
                api_row = api_data[api_data['code'].astype(str) == code].iloc[0]
                csv_row = csv_data[csv_data['code'].astype(str) == code].iloc[-1]  # Last row for this code

                for col in key_columns:
                    if col in csv_row and col in api_row:
                        csv_val = csv_row[col]
                        api_val = api_row[col]
                        if pd.notna(csv_val) and pd.notna(api_val):
                            assert abs(float(csv_val) - float(api_val)) < 0.001, \
                                f"Column {col} mismatch for code {code}: CSV={csv_val}, API={api_val}"

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePostprocessingClient')
    def test_api_record_structure_matches_csv_columns(self, mock_client_class, sample_forecast_data, temp_env_for_consistency):
        """Verify API records have equivalent structure to CSV columns."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_lr_forecasts.return_value = 2
            mock_client_class.return_value = mock_client

            fl.write_linreg_pentad_forecast_data(sample_forecast_data)

            # Get the records that would be sent to API
            assert mock_client.write_lr_forecasts.called, "API write should have been called"
            api_records = mock_client.write_lr_forecasts.call_args[0][0]

            # Read back the CSV
            csv_path = temp_env_for_consistency / 'forecast_pentad_linreg.csv'
            csv_data = pd.read_csv(csv_path)

            # Verify each API record has corresponding CSV data
            for record in api_records:
                code = str(record['code'])

                # Find matching CSV row by code (issue_date was dropped)
                csv_rows = csv_data[csv_data['code'].astype(str) == code]
                assert len(csv_rows) >= 1, f"No CSV row found for code={code}"

                # Verify numeric values match (within tolerance for floats)
                csv_row = csv_rows.iloc[-1]  # Last row for this code
                if record['discharge_avg'] is not None:
                    assert abs(record['discharge_avg'] - csv_row['discharge_avg']) < 0.001
                if record['forecasted_discharge'] is not None:
                    assert abs(record['forecasted_discharge'] - csv_row['forecasted_discharge']) < 0.001
                if record['rsquared'] is not None:
                    assert abs(record['rsquared'] - csv_row['rsquared']) < 0.001

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


class TestReadConsistency:
    """Tests to verify CSV and API reads return consistent data."""

    @pytest.fixture
    def sample_discharge_csv(self, tmp_path):
        """Create a sample discharge CSV file."""
        csv_path = tmp_path / "daily_discharge.csv"
        data = pd.DataFrame({
            'code': ['12345', '12345', '67890', '67890'],
            'date': ['2024-01-01', '2024-01-02', '2024-01-01', '2024-01-02'],
            'discharge': [10.5, 11.2, 20.0, 21.5]
        })
        data.to_csv(csv_path, index=False)
        return csv_path, data

    @pytest.fixture
    def temp_env_for_read(self, tmp_path, sample_discharge_csv):
        """Set up environment for read consistency tests."""
        csv_path, _ = sample_discharge_csv

        os.environ['ieasyforecast_intermediate_data_path'] = str(tmp_path)
        os.environ['ieasyforecast_daily_discharge_file'] = csv_path.name

        yield tmp_path

        os.environ.pop('ieasyforecast_intermediate_data_path', None)
        os.environ.pop('ieasyforecast_daily_discharge_file', None)

    def test_csv_and_api_return_same_columns(self, temp_env_for_read, sample_discharge_csv):
        """Verify CSV and API reads return DataFrames with same columns."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        _, expected_data = sample_discharge_csv

        # Read from CSV
        csv_result = fl.read_daily_discharge_data_from_csv()

        # Mock API to return equivalent data
        with patch('forecast_library.SapphirePreprocessingClient') as mock_client_class:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.read_runoff.side_effect = [
                pd.DataFrame({
                    'id': [1, 2, 3, 4],
                    'horizon_type': ['day', 'day', 'day', 'day'],
                    'code': ['12345', '12345', '67890', '67890'],
                    'date': ['2024-01-01', '2024-01-02', '2024-01-01', '2024-01-02'],
                    'discharge': [10.5, 11.2, 20.0, 21.5],
                    'predictor': [None, None, None, None],
                    'horizon_value': [1, 2, 1, 2],
                    'horizon_in_year': [1, 2, 1, 2],
                }),
                pd.DataFrame()  # End of pagination
            ]
            mock_client_class.return_value = mock_client

            api_result = fl._read_daily_discharge_from_api()

        # Both should have the same columns
        assert set(csv_result.columns) == set(api_result.columns), \
            f"Column mismatch: CSV={set(csv_result.columns)}, API={set(api_result.columns)}"

        # Both should have required columns
        required_columns = ['code', 'date', 'discharge']
        for col in required_columns:
            assert col in csv_result.columns, f"CSV missing column: {col}"
            assert col in api_result.columns, f"API missing column: {col}"

    def test_csv_and_api_return_same_data_values(self, temp_env_for_read, sample_discharge_csv):
        """Verify CSV and API reads return equivalent data values."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        # Read from CSV
        csv_result = fl.read_daily_discharge_data_from_csv()

        # Mock API to return the same data
        with patch('forecast_library.SapphirePreprocessingClient') as mock_client_class:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.read_runoff.side_effect = [
                pd.DataFrame({
                    'id': [1, 2, 3, 4],
                    'horizon_type': ['day', 'day', 'day', 'day'],
                    'code': ['12345', '12345', '67890', '67890'],
                    'date': ['2024-01-01', '2024-01-02', '2024-01-01', '2024-01-02'],
                    'discharge': [10.5, 11.2, 20.0, 21.5],
                    'predictor': [None, None, None, None],
                    'horizon_value': [1, 2, 1, 2],
                    'horizon_in_year': [1, 2, 1, 2],
                }),
                pd.DataFrame()
            ]
            mock_client_class.return_value = mock_client

            api_result = fl._read_daily_discharge_from_api()

        # Sort both for comparison
        csv_sorted = csv_result.sort_values(['code', 'date']).reset_index(drop=True)
        api_sorted = api_result.sort_values(['code', 'date']).reset_index(drop=True)

        # Compare code values
        assert csv_sorted['code'].tolist() == api_sorted['code'].tolist(), "Code mismatch"

        # Compare discharge values
        csv_discharge = csv_sorted['discharge'].tolist()
        api_discharge = api_sorted['discharge'].tolist()
        for i, (csv_val, api_val) in enumerate(zip(csv_discharge, api_discharge)):
            assert abs(csv_val - api_val) < 0.001, f"Discharge mismatch at index {i}: CSV={csv_val}, API={api_val}"

    def test_csv_and_api_return_same_date_types(self, temp_env_for_read, sample_discharge_csv):
        """Verify CSV and API reads return dates in the same format."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        # Read from CSV
        csv_result = fl.read_daily_discharge_data_from_csv()

        # Mock API to return the same data
        with patch('forecast_library.SapphirePreprocessingClient') as mock_client_class:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.read_runoff.side_effect = [
                pd.DataFrame({
                    'id': [1, 2, 3, 4],
                    'horizon_type': ['day', 'day', 'day', 'day'],
                    'code': ['12345', '12345', '67890', '67890'],
                    'date': ['2024-01-01', '2024-01-02', '2024-01-01', '2024-01-02'],
                    'discharge': [10.5, 11.2, 20.0, 21.5],
                    'predictor': [None, None, None, None],
                    'horizon_value': [1, 2, 1, 2],
                    'horizon_in_year': [1, 2, 1, 2],
                }),
                pd.DataFrame()
            ]
            mock_client_class.return_value = mock_client

            api_result = fl._read_daily_discharge_from_api()

        # Both should have datetime dtype for date column
        assert pd.api.types.is_datetime64_any_dtype(csv_result['date']), "CSV date should be datetime"
        assert pd.api.types.is_datetime64_any_dtype(api_result['date']), "API date should be datetime"

        # Date values should match
        csv_sorted = csv_result.sort_values(['code', 'date']).reset_index(drop=True)
        api_sorted = api_result.sort_values(['code', 'date']).reset_index(drop=True)

        for i in range(len(csv_sorted)):
            csv_date = pd.Timestamp(csv_sorted['date'].iloc[i])
            api_date = pd.Timestamp(api_sorted['date'].iloc[i])
            # Compare just the date part (ignore time component)
            assert csv_date.date() == api_date.date(), f"Date mismatch at index {i}"

    def test_csv_and_api_return_same_code_types(self, temp_env_for_read, sample_discharge_csv):
        """Verify CSV and API reads return codes as strings."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        # Read from CSV
        csv_result = fl.read_daily_discharge_data_from_csv()

        # Mock API to return the same data
        with patch('forecast_library.SapphirePreprocessingClient') as mock_client_class:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.read_runoff.side_effect = [
                pd.DataFrame({
                    'id': [1, 2, 3, 4],
                    'horizon_type': ['day', 'day', 'day', 'day'],
                    'code': ['12345', '12345', '67890', '67890'],
                    'date': ['2024-01-01', '2024-01-02', '2024-01-01', '2024-01-02'],
                    'discharge': [10.5, 11.2, 20.0, 21.5],
                    'predictor': [None, None, None, None],
                    'horizon_value': [1, 2, 1, 2],
                    'horizon_in_year': [1, 2, 1, 2],
                }),
                pd.DataFrame()
            ]
            mock_client_class.return_value = mock_client

            api_result = fl._read_daily_discharge_from_api()

        # Both should have string dtype for code column
        assert csv_result['code'].dtype == 'object' or str(csv_result['code'].dtype) == 'string', \
            f"CSV code should be string, got {csv_result['code'].dtype}"
        assert api_result['code'].dtype == 'object' or str(api_result['code'].dtype) == 'string', \
            f"API code should be string, got {api_result['code'].dtype}"

    def test_unified_function_returns_consistent_format(self, temp_env_for_read, sample_discharge_csv):
        """Verify unified read function returns consistent format regardless of source."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        # Read via CSV mode
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'
        csv_result = fl.read_daily_discharge_data()

        # Read via API mode (mocked)
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        with patch('forecast_library.SapphirePreprocessingClient') as mock_client_class:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.read_runoff.side_effect = [
                pd.DataFrame({
                    'id': [1, 2, 3, 4],
                    'horizon_type': ['day', 'day', 'day', 'day'],
                    'code': ['12345', '12345', '67890', '67890'],
                    'date': ['2024-01-01', '2024-01-02', '2024-01-01', '2024-01-02'],
                    'discharge': [10.5, 11.2, 20.0, 21.5],
                    'predictor': [None, None, None, None],
                    'horizon_value': [1, 2, 1, 2],
                    'horizon_in_year': [1, 2, 1, 2],
                }),
                pd.DataFrame()
            ]
            mock_client_class.return_value = mock_client

            api_result = fl.read_daily_discharge_data()

        os.environ.pop('SAPPHIRE_API_ENABLED', None)

        # Both results should have identical structure
        assert set(csv_result.columns) == set(api_result.columns), "Column mismatch"
        assert len(csv_result) == len(api_result), "Row count mismatch"

        # Both should be usable in the same way downstream
        assert 'code' in csv_result.columns
        assert 'date' in csv_result.columns
        assert 'discharge' in csv_result.columns


# =============================================================================
# Tests for _write_hydrograph_to_api
# =============================================================================

class TestWriteHydrographToApi:
    """Tests for the _write_hydrograph_to_api helper function."""

    @pytest.fixture
    def sample_pentad_hydrograph_data(self):
        """Create sample pentad hydrograph data."""
        return pd.DataFrame({
            'code': ['12345', '67890'],
            'date': pd.to_datetime(['2024-01-05', '2024-01-05']),
            'pentad': [1, 1],
            'pentad_in_year': [1, 1],
            'day_of_year': [5, 5],
            'count': [10, 15],
            'mean': [100.5, 200.0],
            'std': [10.5, 20.0],
            'min': [80.0, 160.0],
            'max': [120.0, 240.0],
            'q05': [82.0, 165.0],
            'q25': [90.0, 180.0],
            'q50': [100.0, 200.0],
            'q75': [110.0, 220.0],
            'q95': [118.0, 235.0],
            'norm': [95.0, 190.0],
            '2025': [98.0, 195.0],
            '2026': [102.0, 205.0],
        })

    @pytest.fixture
    def sample_decad_hydrograph_data(self):
        """Create sample decade hydrograph data."""
        return pd.DataFrame({
            'code': ['12345', '67890'],
            'date': pd.to_datetime(['2024-01-10', '2024-01-10']),
            'decad': [1, 1],
            'decad_in_year': [1, 1],
            'day_of_year': [10, 10],
            'count': [10, 15],
            'mean': [100.5, 200.0],
            'std': [10.5, 20.0],
            'min': [80.0, 160.0],
            'max': [120.0, 240.0],
            'q05': [82.0, 165.0],
            'q25': [90.0, 180.0],
            'q50': [100.0, 200.0],
            'q75': [110.0, 220.0],
            'q95': [118.0, 235.0],
            'norm': [95.0, 190.0],
            '2025': [98.0, 195.0],
            '2026': [102.0, 205.0],
        })

    def test_api_disabled_via_env_var(self, sample_pentad_hydrograph_data):
        """When SAPPHIRE_API_ENABLED=false, API write should be skipped."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'
        try:
            result = fl._write_hydrograph_to_api(sample_pentad_hydrograph_data, "pentad")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    def test_invalid_horizon_type_raises_error(self, sample_pentad_hydrograph_data):
        """Invalid horizon_type should raise ValueError."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            with pytest.raises(ValueError, match="Invalid horizon_type"):
                fl._write_hydrograph_to_api(sample_pentad_hydrograph_data, "invalid")
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePreprocessingClient')
    def test_api_not_ready_returns_false(self, mock_client_class, sample_pentad_hydrograph_data):
        """When API health check fails, should return False (non-blocking)."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = False
            mock_client_class.return_value = mock_client

            result = fl._write_hydrograph_to_api(sample_pentad_hydrograph_data, "pentad")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePreprocessingClient')
    def test_api_write_pentad_success(self, mock_client_class, sample_pentad_hydrograph_data):
        """When API write succeeds for pentad, should return True with correct records."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_hydrograph.return_value = 2
            mock_client_class.return_value = mock_client

            result = fl._write_hydrograph_to_api(sample_pentad_hydrograph_data, "pentad")

            assert result is True
            mock_client.write_hydrograph.assert_called_once()

            # Verify records were prepared correctly
            call_args = mock_client.write_hydrograph.call_args[0][0]
            assert len(call_args) == 2

            # Check first record
            assert call_args[0]['horizon_type'] == 'pentad'
            assert call_args[0]['code'] == '12345'
            assert call_args[0]['date'] == '2024-01-05'
            assert call_args[0]['horizon_value'] == 1
            assert call_args[0]['horizon_in_year'] == 1
            assert call_args[0]['mean'] == 100.5
            assert call_args[0]['min'] == 80.0
            assert call_args[0]['max'] == 120.0
            assert call_args[0]['q50'] == 100.0
            assert call_args[0]['norm'] == 95.0
            assert call_args[0]['current'] == 102.0  # 2026 value
            assert call_args[0]['previous'] == 98.0  # 2025 value
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePreprocessingClient')
    def test_api_write_decade_success(self, mock_client_class, sample_decad_hydrograph_data):
        """When API write succeeds for decade, should use correct column names."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_hydrograph.return_value = 2
            mock_client_class.return_value = mock_client

            result = fl._write_hydrograph_to_api(sample_decad_hydrograph_data, "decade")

            assert result is True

            # Verify decade uses correct column names
            call_args = mock_client.write_hydrograph.call_args[0][0]
            assert call_args[0]['horizon_type'] == 'decade'
            assert call_args[0]['horizon_value'] == 1
            assert call_args[0]['horizon_in_year'] == 1
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePreprocessingClient')
    def test_api_handles_nan_values(self, mock_client_class):
        """NaN values in data should be converted to None in API records."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_hydrograph.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': ['12345'],
                'date': pd.to_datetime(['2024-01-05']),
                'pentad': [1],
                'pentad_in_year': [1],
                'day_of_year': [5],
                'count': [10],
                'mean': [np.nan],
                'std': [np.nan],
                'min': [80.0],
                'max': [120.0],
                'q05': [np.nan],
                'q25': [90.0],
                'q50': [100.0],
                'q75': [110.0],
                'q95': [np.nan],
                'norm': [95.0],
            })

            result = fl._write_hydrograph_to_api(data, "pentad")

            assert result is True
            call_args = mock_client.write_hydrograph.call_args[0][0]
            assert call_args[0]['mean'] is None
            assert call_args[0]['std'] is None
            assert call_args[0]['q05'] is None
            assert call_args[0]['q95'] is None
            assert call_args[0]['min'] == 80.0
            assert call_args[0]['max'] == 120.0
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


# =============================================================================
# Tests for _write_runoff_to_api
# =============================================================================

class TestWriteRunoffToApi:
    """Tests for the _write_runoff_to_api helper function."""

    @pytest.fixture
    def sample_pentad_runoff_data(self):
        """Create sample pentad runoff data with multiple dates."""
        dates = pd.date_range('2024-01-01', periods=10, freq='5D')
        return pd.DataFrame({
            'code': ['12345'] * len(dates) + ['67890'] * len(dates),
            'date': list(dates) * 2,
            'pentad': [1, 2, 3, 4, 5, 6, 1, 2, 3, 4] * 2,
            'pentad_in_year': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] * 2,
            'discharge_avg': [100.0 + i for i in range(20)],
            'predictor': [50.0 + i for i in range(20)],
        })

    @pytest.fixture
    def sample_decad_runoff_data(self):
        """Create sample decade runoff data with multiple dates."""
        dates = pd.date_range('2024-01-01', periods=6, freq='10D')
        return pd.DataFrame({
            'code': ['12345'] * len(dates) + ['67890'] * len(dates),
            'date': list(dates) * 2,
            'decad_in_month': [1, 2, 3, 1, 2, 3] * 2,
            'decad_in_year': [1, 2, 3, 4, 5, 6] * 2,
            'discharge_avg': [100.0 + i for i in range(12)],
            'predictor': [50.0 + i for i in range(12)],
        })

    def test_api_disabled_via_env_var(self, sample_pentad_runoff_data):
        """When SAPPHIRE_API_ENABLED=false, API write should return None."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'
        try:
            result = fl._write_runoff_to_api(sample_pentad_runoff_data, "pentad")
            assert result is None
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    def test_invalid_horizon_type_raises_error(self, sample_pentad_runoff_data):
        """Invalid horizon_type should raise ValueError."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            with pytest.raises(ValueError, match="Invalid horizon_type"):
                fl._write_runoff_to_api(sample_pentad_runoff_data, "invalid")
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePreprocessingClient')
    def test_api_not_ready_returns_none(self, mock_client_class, sample_pentad_runoff_data):
        """When API health check fails, should return None (non-blocking)."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = False
            mock_client_class.return_value = mock_client

            result = fl._write_runoff_to_api(sample_pentad_runoff_data, "pentad")
            assert result is None
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.pd.Timestamp')
    @patch('forecast_library.SapphirePreprocessingClient')
    def test_operational_mode_writes_today_only(self, mock_client_class, mock_timestamp):
        """Operational mode should only write today's data."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        # Mock today to match data dates
        fake_today = pd.Timestamp('2024-02-14')
        mock_timestamp.today.return_value = fake_today
        # Preserve pd.to_datetime behavior
        mock_timestamp.side_effect = lambda *a, **kw: pd.Timestamp(*a, **kw)

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        os.environ['SAPPHIRE_SYNC_MODE'] = 'operational'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_runoff.return_value = 2
            mock_client_class.return_value = mock_client

            # Create data with one date matching "today"
            data = pd.DataFrame({
                'code': ['12345', '67890', '12345', '67890'],
                'date': pd.to_datetime(['2024-02-09', '2024-02-09', '2024-02-14', '2024-02-14']),
                'pentad': [2, 2, 3, 3],
                'pentad_in_year': [8, 8, 9, 9],
                'discharge_avg': [100.0, 101.0, 102.0, 103.0],
                'predictor': [50.0, 51.0, 52.0, 53.0],
            })

            result = fl._write_runoff_to_api(data, "pentad")

            # Should return DataFrame with only today's records
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert all(result['date'] == fake_today)

            # Verify API was called with only 2 records
            call_args = mock_client.write_runoff.call_args[0][0]
            assert len(call_args) == 2
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            os.environ.pop('SAPPHIRE_SYNC_MODE', None)

    @patch('forecast_library.pd.Timestamp')
    @patch('forecast_library.SapphirePreprocessingClient')
    def test_maintenance_mode_writes_last_30_days(self, mock_client_class, mock_timestamp):
        """Maintenance mode should write the last 30 days from today."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        # Mock today
        fake_today = pd.Timestamp('2024-02-19')
        mock_timestamp.today.return_value = fake_today
        mock_timestamp.side_effect = lambda *a, **kw: pd.Timestamp(*a, **kw)

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        os.environ['SAPPHIRE_SYNC_MODE'] = 'maintenance'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_runoff.return_value = 14
            mock_client_class.return_value = mock_client

            # Create data spanning more than 30 days
            dates = pd.date_range('2024-01-01', periods=10, freq='5D')
            data = pd.DataFrame({
                'code': ['12345'] * len(dates) + ['67890'] * len(dates),
                'date': list(dates) * 2,
                'pentad': [1, 2, 3, 4, 5, 6, 1, 2, 3, 4] * 2,
                'pentad_in_year': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] * 2,
                'discharge_avg': [100.0 + i for i in range(20)],
                'predictor': [50.0 + i for i in range(20)],
            })

            result = fl._write_runoff_to_api(data, "pentad")

            # Should return DataFrame
            assert isinstance(result, pd.DataFrame)
            # Cutoff is today - 30 days = 2024-01-20
            cutoff = fake_today - pd.Timedelta(days=30)
            expected_records = data[pd.to_datetime(data['date']) >= cutoff]
            assert len(result) == len(expected_records)
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            os.environ.pop('SAPPHIRE_SYNC_MODE', None)

    @patch('forecast_library.SapphirePreprocessingClient')
    def test_initial_mode_writes_all_data(self, mock_client_class, sample_pentad_runoff_data):
        """Initial mode should write all data."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        os.environ['SAPPHIRE_SYNC_MODE'] = 'initial'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_runoff.return_value = 20
            mock_client_class.return_value = mock_client

            result = fl._write_runoff_to_api(sample_pentad_runoff_data, "pentad")

            # Should return DataFrame with all data
            assert isinstance(result, pd.DataFrame)
            assert len(result) == len(sample_pentad_runoff_data)

            # Verify API was called with all records
            call_args = mock_client.write_runoff.call_args[0][0]
            assert len(call_args) == 20
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            os.environ.pop('SAPPHIRE_SYNC_MODE', None)

    @patch('forecast_library.SapphirePreprocessingClient')
    def test_api_write_decade_success(self, mock_client_class, sample_decad_runoff_data):
        """When API write succeeds for decade, should use correct column names."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        os.environ['SAPPHIRE_SYNC_MODE'] = 'initial'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_runoff.return_value = 12
            mock_client_class.return_value = mock_client

            result = fl._write_runoff_to_api(sample_decad_runoff_data, "decade")

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 12

            # Verify decade uses correct column names
            call_args = mock_client.write_runoff.call_args[0][0]
            assert call_args[0]['horizon_type'] == 'decade'
            assert 'horizon_value' in call_args[0]
            assert 'horizon_in_year' in call_args[0]
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            os.environ.pop('SAPPHIRE_SYNC_MODE', None)

    @patch('forecast_library.SapphirePreprocessingClient')
    def test_returns_dataframe_for_consistency_checking(self, mock_client_class):
        """Should return DataFrame of written data for consistency checking."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_runoff.return_value = 4
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': ['12345', '67890', '12345', '67890'],
                'date': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-05', '2024-01-05']),
                'pentad': [1, 1, 1, 1],
                'pentad_in_year': [1, 1, 1, 1],
                'discharge_avg': [100.0, 101.0, 102.0, 103.0],
                'predictor': [50.0, 51.0, 52.0, 53.0],
            })

            # Use initial mode to write all data (avoids date dependency on today)
            result = fl._write_runoff_to_api(data, "pentad", mode="initial")

            # Should be a DataFrame, not a bool
            assert isinstance(result, pd.DataFrame)
            # Should have the same columns as input (at least the key ones)
            assert 'code' in result.columns
            assert 'date' in result.columns
            assert 'discharge_avg' in result.columns
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    def test_empty_data_returns_none(self):
        """Empty DataFrame should return None without API call."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            result = fl._write_runoff_to_api(pd.DataFrame(), "pentad")
            assert result is None
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePreprocessingClient')
    def test_api_handles_nan_values(self, mock_client_class):
        """NaN values in data should be converted to None in API records."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        os.environ['SAPPHIRE_SYNC_MODE'] = 'initial'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_runoff.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': ['12345'],
                'date': pd.to_datetime(['2024-01-05']),
                'pentad': [1],
                'pentad_in_year': [1],
                'discharge_avg': [np.nan],
                'predictor': [50.0],
            })

            result = fl._write_runoff_to_api(data, "pentad")

            assert isinstance(result, pd.DataFrame)
            call_args = mock_client.write_runoff.call_args[0][0]
            assert call_args[0]['discharge'] is None
            assert call_args[0]['predictor'] == 50.0
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            os.environ.pop('SAPPHIRE_SYNC_MODE', None)

    @patch('forecast_library.pd.Timestamp')
    @patch('forecast_library.SapphirePreprocessingClient')
    def test_default_sync_mode_is_operational(self, mock_client_class, mock_timestamp):
        """Default sync mode should be operational (only today's data)."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        # Mock today to match data dates
        fake_today = pd.Timestamp('2024-01-05')
        mock_timestamp.today.return_value = fake_today
        mock_timestamp.side_effect = lambda *a, **kw: pd.Timestamp(*a, **kw)

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        os.environ.pop('SAPPHIRE_SYNC_MODE', None)  # Ensure not set
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_runoff.return_value = 2
            mock_client_class.return_value = mock_client

            # Create data with one date matching "today"
            data = pd.DataFrame({
                'code': ['12345', '67890', '12345', '67890'],
                'date': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-05', '2024-01-05']),
                'pentad': [1, 1, 1, 1],
                'pentad_in_year': [1, 1, 1, 1],
                'discharge_avg': [100.0, 101.0, 102.0, 103.0],
                'predictor': [50.0, 51.0, 52.0, 53.0],
            })

            result = fl._write_runoff_to_api(data, "pentad")

            # Should only write 2 records (one per station for today)
            call_args = mock_client.write_runoff.call_args[0][0]
            assert len(call_args) == 2
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePreprocessingClient')
    def test_mode_parameter_overrides_env_var(self, mock_client_class):
        """mode parameter should override SAPPHIRE_SYNC_MODE env var."""
        if not fl.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        os.environ['SAPPHIRE_SYNC_MODE'] = 'operational'  # Env says operational
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_runoff.return_value = 4
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': ['12345', '67890', '12345', '67890'],
                'date': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-05', '2024-01-05']),
                'pentad': [1, 1, 1, 1],
                'pentad_in_year': [1, 1, 1, 1],
                'discharge_avg': [100.0, 101.0, 102.0, 103.0],
                'predictor': [50.0, 51.0, 52.0, 53.0],
            })

            # Pass mode='initial' to override env var
            result = fl._write_runoff_to_api(data, "pentad", mode="initial")

            # initial mode writes ALL data
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 4
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            os.environ.pop('SAPPHIRE_SYNC_MODE', None)
