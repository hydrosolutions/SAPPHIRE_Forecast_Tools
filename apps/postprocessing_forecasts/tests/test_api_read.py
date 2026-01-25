"""
Tests for SAPPHIRE API reading functionality in setup_library.py.

These tests verify that the forecast and hydrograph data API reading functions
work correctly, including:
- _read_lr_forecasts_from_api helper function
- _read_ml_forecasts_from_api helper function
- read_linreg_forecasts_pentad and read_linreg_forecasts_decade
- read_machine_learning_forecasts_pentad and read_machine_learning_forecasts_decade
- read_observed_pentadal_data and read_observed_decadal_data
- Environment variable handling (SAPPHIRE_API_ENABLED)
- API health checks
- Pagination handling
- CSV fallback behavior
"""

import os
import sys
import tempfile
from unittest.mock import Mock, patch, MagicMock

import numpy as np
import pandas as pd
import pytest

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast'))

# Import the module under test
import setup_library as sl

# Check if SAPPHIRE API is available
SAPPHIRE_API_AVAILABLE = sl.SAPPHIRE_API_AVAILABLE


# =============================================================================
# Sample Mock Data for Tests
# =============================================================================

def create_mock_lr_forecast_data():
    """Create sample LR forecast data that matches API response structure."""
    return pd.DataFrame({
        'id': [1, 2],
        'horizon_type': ['pentad', 'pentad'],
        'code': ['12345', '12346'],
        'date': pd.to_datetime(['2024-01-05', '2024-01-05']),
        'horizon_value': [1, 1],
        'horizon_in_year': [1, 1],
        'forecasted_discharge': [100.5, 150.2],
        'predictor': [80.0, 120.0],
        'slope': [1.2, 1.1],
        'intercept': [5.0, 10.0],
        'q_mean': [95.0, 140.0],
        'q_std_sigma': [15.0, 20.0],
        'delta': [10.0, 13.0],
        'rsquared': [0.85, 0.90],
        'discharge_avg': [90.0, 130.0],
    })


def create_mock_ml_forecast_data():
    """Create sample ML forecast data that matches API response structure."""
    return pd.DataFrame({
        'id': [1, 2],
        'horizon_type': ['pentad', 'pentad'],
        'code': ['12345', '12346'],
        'date': pd.to_datetime(['2024-01-05', '2024-01-05']),
        'target': pd.to_datetime(['2024-01-06', '2024-01-06']),
        'model': ['TFT', 'TFT'],
        'horizon_value': [1, 1],
        'horizon_in_year': [1, 1],
        'forecasted_discharge': [100.5, 150.2],
        'q05': [80.0, 120.0],
        'q25': [90.0, 135.0],
        'q50': [100.5, 150.2],
        'q75': [110.0, 165.0],
        'q95': [120.0, 180.0],
        'flag': [0, 0],
    })


def create_mock_hydrograph_data():
    """Create sample hydrograph data that matches API response structure."""
    return pd.DataFrame({
        'id': [1, 2],
        'horizon_type': ['pentad', 'pentad'],
        'code': ['12345', '12346'],
        'date': pd.to_datetime(['2024-01-05', '2024-01-05']),
        'horizon_value': [1, 1],
        'horizon_in_year': [1, 1],
        'mean': [100.0, 150.0],
        'std': [10.0, 15.0],
        'min': [80.0, 120.0],
        'max': [120.0, 180.0],
        'q05': [82.0, 122.0],
        'q25': [90.0, 135.0],
        'q50': [100.0, 150.0],
        'q75': [110.0, 165.0],
        'q95': [118.0, 178.0],
        'norm': [95.0, 145.0],
        'current': [102.0, 155.0],
        'previous': [98.0, 148.0],
    })


# =============================================================================
# Tests for _read_lr_forecasts_from_api
# =============================================================================

class TestReadLRForecastsFromApi:
    """Tests for the _read_lr_forecasts_from_api helper function."""

    def test_invalid_horizon_type_raises_error(self):
        """Invalid horizon_type should raise ValueError."""
        with pytest.raises(ValueError, match="horizon_type must be 'pentad' or 'decade'"):
            sl._read_lr_forecasts_from_api(horizon_type="invalid")

    def test_api_not_installed_raises_runtime_error(self):
        """When sapphire-api-client is not installed, should raise RuntimeError."""
        original = sl.SAPPHIRE_API_AVAILABLE
        try:
            sl.SAPPHIRE_API_AVAILABLE = False
            with pytest.raises(RuntimeError, match="sapphire-api-client is not installed"):
                sl._read_lr_forecasts_from_api(horizon_type="pentad")
        finally:
            sl.SAPPHIRE_API_AVAILABLE = original

    @patch('setup_library.SapphirePostprocessingClient')
    def test_api_not_ready_raises_error(self, mock_client_class):
        """When API health check fails, should raise SapphireAPIError."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = False
        mock_client_class.return_value = mock_client

        with pytest.raises(sl.SapphireAPIError, match="not ready"):
            sl._read_lr_forecasts_from_api(horizon_type="pentad")

    @patch('setup_library.SapphirePostprocessingClient')
    def test_pentad_read_success(self, mock_client_class):
        """When API read succeeds for pentad, should return DataFrame with correct columns."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_lr_forecasts.side_effect = [
            create_mock_lr_forecast_data(),
            pd.DataFrame()  # End of pagination
        ]
        mock_client_class.return_value = mock_client

        result = sl._read_lr_forecasts_from_api(horizon_type="pentad")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'code' in result.columns
        assert 'date' in result.columns
        assert 'forecasted_discharge' in result.columns
        assert 'model_long' in result.columns
        assert 'model_short' in result.columns
        assert result['model_short'].iloc[0] == 'LR'
        assert result['model_long'].iloc[0] == 'Linear regression (LR)'

    @patch('setup_library.SapphirePostprocessingClient')
    def test_decade_read_success(self, mock_client_class):
        """When API read succeeds for decade, should return DataFrame."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_data = create_mock_lr_forecast_data()
        mock_data['horizon_type'] = 'decade'

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_lr_forecasts.side_effect = [
            mock_data,
            pd.DataFrame()  # End of pagination
        ]
        mock_client_class.return_value = mock_client

        result = sl._read_lr_forecasts_from_api(horizon_type="decade")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    @patch('setup_library.SapphirePostprocessingClient')
    def test_pagination_handling(self, mock_client_class):
        """Should handle pagination correctly when multiple site_codes are provided."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        # Create data for different codes (pagination happens per code)
        # Each code has 2 unique dates, 2 codes = 4 unique code+date combinations
        code1_data = pd.DataFrame({
            'id': [1, 2],
            'horizon_type': ['pentad', 'pentad'],
            'code': ['12345', '12345'],
            'date': pd.to_datetime(['2024-01-05', '2024-01-10']),
            'horizon_value': [1, 2],
            'horizon_in_year': [1, 2],
            'forecasted_discharge': [100.5, 110.5],
            'predictor': [80.0, 85.0],
            'slope': [1.2, 1.2],
            'intercept': [5.0, 5.0],
            'q_mean': [95.0, 100.0],
            'q_std_sigma': [15.0, 16.0],
            'delta': [10.0, 11.0],
            'rsquared': [0.85, 0.86],
            'discharge_avg': [90.0, 95.0],
        })

        code2_data = pd.DataFrame({
            'id': [3, 4],
            'horizon_type': ['pentad', 'pentad'],
            'code': ['67890', '67890'],
            'date': pd.to_datetime(['2024-01-05', '2024-01-10']),
            'horizon_value': [1, 2],
            'horizon_in_year': [1, 2],
            'forecasted_discharge': [150.2, 160.2],
            'predictor': [120.0, 125.0],
            'slope': [1.1, 1.1],
            'intercept': [10.0, 10.0],
            'q_mean': [140.0, 145.0],
            'q_std_sigma': [20.0, 21.0],
            'delta': [13.0, 14.0],
            'rsquared': [0.90, 0.91],
            'discharge_avg': [130.0, 135.0],
        })

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        # Since each page has < page_size records (2 < 10000), the loop breaks
        # after the first call per code. So we need only 2 values in side_effect.
        mock_client.read_lr_forecasts.side_effect = [
            code1_data,  # For code '12345' - breaks immediately since 2 < 10000
            code2_data,  # For code '67890' - breaks immediately since 2 < 10000
        ]
        mock_client_class.return_value = mock_client

        result = sl._read_lr_forecasts_from_api(
            horizon_type="pentad",
            site_codes=['12345', '67890']
        )

        # Should combine data from both codes (4 unique code+date records)
        assert len(result) == 4
        assert set(result['code'].unique()) == {'12345', '67890'}

    @patch('setup_library.SapphirePostprocessingClient')
    def test_site_codes_filter(self, mock_client_class):
        """Should query per site code when site_codes provided."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_lr_forecasts.return_value = pd.DataFrame()
        mock_client_class.return_value = mock_client

        sl._read_lr_forecasts_from_api(
            horizon_type="pentad",
            site_codes=['12345', '67890']
        )

        # Should be called for each code
        calls = mock_client.read_lr_forecasts.call_args_list
        codes_queried = [call.kwargs.get('code') for call in calls]
        assert '12345' in codes_queried
        assert '67890' in codes_queried

    @patch('setup_library.SapphirePostprocessingClient')
    def test_empty_result_returns_empty_dataframe(self, mock_client_class):
        """When API returns no data, should return empty DataFrame."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_lr_forecasts.return_value = pd.DataFrame()
        mock_client_class.return_value = mock_client

        result = sl._read_lr_forecasts_from_api(horizon_type="pentad")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    @patch('setup_library.SapphirePostprocessingClient')
    def test_code_type_conversion(self, mock_client_class):
        """Should convert code to string and remove .0 suffix."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_data = create_mock_lr_forecast_data()
        mock_data['code'] = [12345.0, 12346.0]  # Numeric with decimal

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_lr_forecasts.side_effect = [mock_data, pd.DataFrame()]
        mock_client_class.return_value = mock_client

        result = sl._read_lr_forecasts_from_api(horizon_type="pentad")

        # Codes should be strings without .0 suffix
        assert result['code'].dtype == 'object'
        assert '12345.0' not in result['code'].values
        assert '12345' in result['code'].values


# =============================================================================
# Tests for _read_ml_forecasts_from_api
# =============================================================================

class TestReadMLForecastsFromApi:
    """Tests for the _read_ml_forecasts_from_api helper function."""

    def test_invalid_horizon_type_raises_error(self):
        """Invalid horizon_type should raise ValueError."""
        with pytest.raises(ValueError, match="horizon_type must be 'pentad' or 'decade'"):
            sl._read_ml_forecasts_from_api(model="TFT", horizon_type="invalid")

    def test_invalid_model_raises_error(self):
        """Invalid model should raise ValueError."""
        with pytest.raises(ValueError, match="model must be one of"):
            sl._read_ml_forecasts_from_api(model="INVALID", horizon_type="pentad")

    def test_api_not_installed_raises_runtime_error(self):
        """When sapphire-api-client is not installed, should raise RuntimeError."""
        original = sl.SAPPHIRE_API_AVAILABLE
        try:
            sl.SAPPHIRE_API_AVAILABLE = False
            with pytest.raises(RuntimeError, match="sapphire-api-client is not installed"):
                sl._read_ml_forecasts_from_api(model="TFT", horizon_type="pentad")
        finally:
            sl.SAPPHIRE_API_AVAILABLE = original

    @patch('setup_library.SapphirePostprocessingClient')
    def test_api_not_ready_raises_error(self, mock_client_class):
        """When API health check fails, should raise SapphireAPIError."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = False
        mock_client_class.return_value = mock_client

        with pytest.raises(sl.SapphireAPIError, match="not ready"):
            sl._read_ml_forecasts_from_api(model="TFT", horizon_type="pentad")

    @patch('setup_library.SapphirePostprocessingClient')
    def test_tft_model_read_success(self, mock_client_class):
        """When API read succeeds for TFT, should return DataFrame with correct model names."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_forecasts.side_effect = [
            create_mock_ml_forecast_data(),
            pd.DataFrame()  # End of pagination
        ]
        mock_client_class.return_value = mock_client

        result = sl._read_ml_forecasts_from_api(model="TFT", horizon_type="pentad")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert result['model_short'].iloc[0] == 'TFT'
        assert result['model_long'].iloc[0] == 'Temporal Fusion Transformer (TFT)'

    @patch('setup_library.SapphirePostprocessingClient')
    def test_tide_model_mapping(self, mock_client_class):
        """TIDE model should map to TiDE display name."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_data = create_mock_ml_forecast_data()
        mock_data['model'] = 'TIDE'

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_forecasts.side_effect = [mock_data, pd.DataFrame()]
        mock_client_class.return_value = mock_client

        result = sl._read_ml_forecasts_from_api(model="TIDE", horizon_type="pentad")

        assert result['model_short'].iloc[0] == 'TiDE'
        assert 'TiDE' in result['model_long'].iloc[0]

    @patch('setup_library.SapphirePostprocessingClient')
    def test_tsmixer_model_mapping(self, mock_client_class):
        """TSMIXER model should map to TSMixer display name."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_data = create_mock_ml_forecast_data()
        mock_data['model'] = 'TSMIXER'

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_forecasts.side_effect = [mock_data, pd.DataFrame()]
        mock_client_class.return_value = mock_client

        result = sl._read_ml_forecasts_from_api(model="TSMIXER", horizon_type="pentad")

        assert result['model_short'].iloc[0] == 'TSMixer'
        assert 'TSMixer' in result['model_long'].iloc[0]

    @patch('setup_library.SapphirePostprocessingClient')
    def test_arima_model_mapping(self, mock_client_class):
        """ARIMA model should have correct display names."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_data = create_mock_ml_forecast_data()
        mock_data['model'] = 'ARIMA'

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_forecasts.side_effect = [mock_data, pd.DataFrame()]
        mock_client_class.return_value = mock_client

        result = sl._read_ml_forecasts_from_api(model="ARIMA", horizon_type="pentad")

        assert result['model_short'].iloc[0] == 'ARIMA'
        assert 'ARIMA' in result['model_long'].iloc[0]

    @patch('setup_library.SapphirePostprocessingClient')
    def test_decade_horizon(self, mock_client_class):
        """Should work correctly for decade horizon."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_data = create_mock_ml_forecast_data()
        mock_data['horizon_type'] = 'decade'

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_forecasts.side_effect = [mock_data, pd.DataFrame()]
        mock_client_class.return_value = mock_client

        result = sl._read_ml_forecasts_from_api(model="TFT", horizon_type="decade")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    @patch('setup_library.SapphirePostprocessingClient')
    def test_empty_result_returns_empty_dataframe(self, mock_client_class):
        """When API returns no data, should return empty DataFrame."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_forecasts.return_value = pd.DataFrame()
        mock_client_class.return_value = mock_client

        result = sl._read_ml_forecasts_from_api(model="TFT", horizon_type="pentad")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


# =============================================================================
# Tests for read_linreg_forecasts_pentad/decade API Integration
# =============================================================================

class TestReadLinregForecastsApiIntegration:
    """Tests for read_linreg_forecasts_pentad and read_linreg_forecasts_decade."""

    @pytest.fixture
    def temp_csv_env(self, tmp_path):
        """Set up temporary environment for CSV fallback tests."""
        csv_dir = tmp_path / "intermediate_data"
        csv_dir.mkdir()

        # Create sample CSV data
        sample_data = pd.DataFrame({
            'code': ['12345', '12346'],
            'date': ['2024-01-05', '2024-01-05'],
            'forecasted_discharge': [100.5, 150.2],
            'predictor': [80.0, 120.0],
            'slope': [1.2, 1.1],
            'intercept': [5.0, 10.0],
            'q_mean': [95.0, 140.0],
            'q_std_sigma': [15.0, 20.0],
            'delta': [10.0, 13.0],
            'rsquared': [0.85, 0.90],
            'discharge_avg': [90.0, 130.0],
            'pentad_in_month': [1, 1],
            'pentad_in_year': [1, 1],
        })
        pentad_path = csv_dir / "forecast_pentad_linreg.csv"
        sample_data.to_csv(pentad_path, index=False)

        # Create decad version
        decad_data = sample_data.copy()
        decad_data['decad_in_month'] = [1, 1]
        decad_data['decad_in_year'] = [1, 1]
        decad_data = decad_data.drop(columns=['pentad_in_month', 'pentad_in_year'])
        decad_path = csv_dir / "forecast_decad_linreg.csv"
        decad_data.to_csv(decad_path, index=False)

        # Set environment variables
        os.environ['ieasyforecast_intermediate_data_path'] = str(csv_dir)
        os.environ['ieasyforecast_analysis_pentad_file'] = 'forecast_pentad_linreg.csv'
        os.environ['ieasyforecast_analysis_decad_file'] = 'forecast_decad_linreg.csv'

        yield csv_dir

        # Cleanup
        os.environ.pop('ieasyforecast_intermediate_data_path', None)
        os.environ.pop('ieasyforecast_analysis_pentad_file', None)
        os.environ.pop('ieasyforecast_analysis_decad_file', None)

    def test_pentad_api_disabled_uses_csv(self, temp_csv_env):
        """When SAPPHIRE_API_ENABLED=false, should use CSV."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'
        try:
            forecasts, stats = sl.read_linreg_forecasts_pentad()

            assert isinstance(forecasts, pd.DataFrame)
            assert isinstance(stats, pd.DataFrame)
            assert len(forecasts) == 2
            assert 'model_long' in forecasts.columns
            assert 'model_short' in forecasts.columns
            assert forecasts['model_short'].iloc[0] == 'LR'
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    def test_decade_api_disabled_uses_csv(self, temp_csv_env):
        """When SAPPHIRE_API_ENABLED=false, should use CSV for decade."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'
        try:
            forecasts, stats = sl.read_linreg_forecasts_decade()

            assert isinstance(forecasts, pd.DataFrame)
            assert isinstance(stats, pd.DataFrame)
            assert len(forecasts) == 2
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('setup_library._read_lr_forecasts_from_api')
    @patch('setup_library.save_most_recent_forecasts')
    def test_pentad_api_enabled_uses_api(self, mock_save, mock_api_read, temp_csv_env):
        """When SAPPHIRE_API_ENABLED=true, should use API."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_api_data = create_mock_lr_forecast_data()
            mock_api_data['model_long'] = 'Linear regression (LR)'
            mock_api_data['model_short'] = 'LR'
            mock_api_read.return_value = mock_api_data

            forecasts, stats = sl.read_linreg_forecasts_pentad()

            mock_api_read.assert_called_once_with(horizon_type="pentad")
            assert len(forecasts) == 2
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('setup_library._read_lr_forecasts_from_api')
    @patch('setup_library.save_most_recent_forecasts_decade')
    def test_decade_api_enabled_uses_api(self, mock_save, mock_api_read, temp_csv_env):
        """When SAPPHIRE_API_ENABLED=true, should use API for decade."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_api_data = create_mock_lr_forecast_data()
            mock_api_data['horizon_type'] = 'decade'
            mock_api_data['model_long'] = 'Linear regression (LR)'
            mock_api_data['model_short'] = 'LR'
            mock_api_read.return_value = mock_api_data

            forecasts, stats = sl.read_linreg_forecasts_decade()

            mock_api_read.assert_called_once_with(horizon_type="decade")
            assert len(forecasts) == 2
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('setup_library._read_lr_forecasts_from_api')
    def test_pentad_api_fail_falls_back_to_csv(self, mock_api_read, temp_csv_env):
        """When API fails, should fall back to CSV."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_api_read.side_effect = Exception("API error")

            forecasts, stats = sl.read_linreg_forecasts_pentad()

            # Should still return data from CSV fallback
            assert len(forecasts) == 2
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('setup_library._read_lr_forecasts_from_api')
    def test_pentad_api_empty_falls_back_to_csv(self, mock_api_read, temp_csv_env):
        """When API returns empty, should fall back to CSV."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_api_read.return_value = pd.DataFrame()

            forecasts, stats = sl.read_linreg_forecasts_pentad()

            # Should return data from CSV fallback
            assert len(forecasts) == 2
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('setup_library._read_lr_forecasts_from_api')
    @patch('setup_library.save_most_recent_forecasts')
    def test_pentad_returns_tuple_with_stats(self, mock_save, mock_api_read, temp_csv_env):
        """Should return tuple of (forecasts, stats) DataFrames."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_api_data = create_mock_lr_forecast_data()
            mock_api_data['model_long'] = 'Linear regression (LR)'
            mock_api_data['model_short'] = 'LR'
            mock_api_read.return_value = mock_api_data

            result = sl.read_linreg_forecasts_pentad()

            assert isinstance(result, tuple)
            assert len(result) == 2
            forecasts, stats = result
            assert isinstance(forecasts, pd.DataFrame)
            assert isinstance(stats, pd.DataFrame)
            # Stats should have q_mean, q_std_sigma, delta columns
            assert 'q_mean' in stats.columns
            assert 'q_std_sigma' in stats.columns
            assert 'delta' in stats.columns
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


# =============================================================================
# Tests for read_machine_learning_forecasts_pentad/decade API Integration
# =============================================================================

class TestReadMLForecastsApiIntegration:
    """Tests for read_machine_learning_forecasts_pentad and read_machine_learning_forecasts_decade."""

    @pytest.fixture
    def temp_csv_env(self, tmp_path):
        """Set up temporary environment for CSV fallback tests."""
        csv_dir = tmp_path / "intermediate_data"
        output_dir = csv_dir / "OUTPUT" / "TFT"
        output_dir.mkdir(parents=True)

        # Create sample CSV data
        sample_data = pd.DataFrame({
            'code': ['12345', '12346'],
            'date': pd.to_datetime(['2024-01-06', '2024-01-06']),
            'forecast_date': pd.to_datetime(['2024-01-05', '2024-01-05']),
            'Q5': [80.0, 120.0],
            'Q25': [90.0, 135.0],
            'Q50': [100.5, 150.2],
            'Q75': [110.0, 165.0],
            'Q95': [120.0, 180.0],
            'flag': [0, 0],
        })
        pentad_path = output_dir / "pentad_TFT_forecast.csv"
        sample_data.to_csv(pentad_path, index=False)

        decad_path = output_dir / "decad_TFT_forecast.csv"
        sample_data.to_csv(decad_path, index=False)

        # Set environment variables
        os.environ['ieasyforecast_intermediate_data_path'] = str(csv_dir)
        os.environ['ieasyhydroforecast_OUTPUT_PATH_DISCHARGE'] = 'OUTPUT'

        yield csv_dir

        # Cleanup
        os.environ.pop('ieasyforecast_intermediate_data_path', None)
        os.environ.pop('ieasyhydroforecast_OUTPUT_PATH_DISCHARGE', None)

    def test_invalid_model_returns_empty_dataframe(self):
        """Invalid model should return empty DataFrame."""
        result = sl.read_machine_learning_forecasts_pentad(model="INVALID")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    @patch('setup_library._read_ml_forecasts_from_api')
    @patch('setup_library.save_most_recent_forecasts')
    def test_pentad_api_enabled_uses_api(self, mock_save, mock_api_read, temp_csv_env):
        """When SAPPHIRE_API_ENABLED=true, should use API."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_api_data = create_mock_ml_forecast_data()
            mock_api_data['model_long'] = 'Temporal Fusion Transformer (TFT)'
            mock_api_data['model_short'] = 'TFT'
            mock_api_read.return_value = mock_api_data

            result = sl.read_machine_learning_forecasts_pentad(model="TFT")

            mock_api_read.assert_called_once_with(model="TFT", horizon_type="pentad")
            assert len(result) == 2
            assert 'model_short' in result.columns
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('setup_library._read_ml_forecasts_from_api')
    @patch('setup_library.save_most_recent_forecasts_decade')
    def test_decade_api_enabled_uses_api(self, mock_save, mock_api_read, temp_csv_env):
        """When SAPPHIRE_API_ENABLED=true, should use API for decade."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_api_data = create_mock_ml_forecast_data()
            mock_api_data['horizon_type'] = 'decade'
            mock_api_data['model_long'] = 'Temporal Fusion Transformer (TFT)'
            mock_api_data['model_short'] = 'TFT'
            mock_api_read.return_value = mock_api_data

            result = sl.read_machine_learning_forecasts_decade(model="TFT")

            mock_api_read.assert_called_once_with(model="TFT", horizon_type="decade")
            assert len(result) == 2
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('setup_library._read_ml_forecasts_from_api')
    def test_pentad_api_fail_falls_back_to_csv(self, mock_api_read, temp_csv_env):
        """When API fails, should fall back to CSV."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_api_read.side_effect = Exception("API error")

            result = sl.read_machine_learning_forecasts_pentad(model="TFT")

            # Should return data from CSV fallback
            assert isinstance(result, pd.DataFrame)
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('setup_library._read_ml_forecasts_from_api')
    def test_pentad_api_empty_falls_back_to_csv(self, mock_api_read, temp_csv_env):
        """When API returns empty, should fall back to CSV."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_api_read.return_value = pd.DataFrame()

            result = sl.read_machine_learning_forecasts_pentad(model="TFT")

            # Should return data from CSV fallback
            assert isinstance(result, pd.DataFrame)
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('setup_library._read_ml_forecasts_from_api')
    @patch('setup_library.save_most_recent_forecasts')
    def test_different_model_types(self, mock_save, mock_api_read, temp_csv_env):
        """Should work with different model types: TIDE, TSMIXER, ARIMA."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            for model in ['TIDE', 'TSMIXER', 'ARIMA']:
                mock_api_data = create_mock_ml_forecast_data()
                mock_api_data['model'] = model
                mock_api_read.return_value = mock_api_data
                mock_api_read.reset_mock()

                result = sl.read_machine_learning_forecasts_pentad(model=model)

                mock_api_read.assert_called_once_with(model=model, horizon_type="pentad")
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


# =============================================================================
# Tests for read_observed_pentadal_data/decadal_data API Integration
# =============================================================================

class TestReadObservedDataApiIntegration:
    """Tests for read_observed_pentadal_data and read_observed_decadal_data."""

    @pytest.fixture
    def temp_csv_env(self, tmp_path):
        """Set up temporary environment for CSV fallback tests."""
        csv_dir = tmp_path / "intermediate_data"
        csv_dir.mkdir()

        # Create sample CSV data
        sample_data = pd.DataFrame({
            'code': ['12345', '12346'],
            'date': ['2024-01-05', '2024-01-05'],
            'pentad': [1, 1],
            'pentad_in_year': [1, 1],
            'discharge_avg': [100.0, 150.0],
        })
        pentad_path = csv_dir / "pentad_discharge.csv"
        sample_data.to_csv(pentad_path, index=False)

        decad_data = sample_data.drop(columns=['pentad', 'pentad_in_year'])
        decad_data['decad'] = [1, 1]
        decad_data['decad_in_year'] = [1, 1]
        decad_path = csv_dir / "decad_discharge.csv"
        decad_data.to_csv(decad_path, index=False)

        # Set environment variables
        os.environ['ieasyforecast_intermediate_data_path'] = str(csv_dir)
        os.environ['ieasyforecast_pentad_discharge_file'] = 'pentad_discharge.csv'
        os.environ['ieasyforecast_decad_discharge_file'] = 'decad_discharge.csv'

        yield csv_dir

        # Cleanup
        os.environ.pop('ieasyforecast_intermediate_data_path', None)
        os.environ.pop('ieasyforecast_pentad_discharge_file', None)
        os.environ.pop('ieasyforecast_decad_discharge_file', None)

    def test_pentad_api_disabled_uses_csv(self, temp_csv_env):
        """When SAPPHIRE_API_ENABLED=false, should use CSV."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'
        try:
            result = sl.read_observed_pentadal_data()

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert 'model_long' in result.columns
            assert 'model_short' in result.columns
            assert result['model_short'].iloc[0] == 'Obs'
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    def test_decadal_api_disabled_uses_csv(self, temp_csv_env):
        """When SAPPHIRE_API_ENABLED=false, should use CSV for decadal."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'
        try:
            result = sl.read_observed_decadal_data()

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert result['model_short'].iloc[0] == 'Obs'
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('setup_library.fl.read_hydrograph_data')
    def test_pentad_api_enabled_uses_fl_read_hydrograph_data(self, mock_read_hydro, temp_csv_env):
        """When SAPPHIRE_API_ENABLED=true, should use fl.read_hydrograph_data()."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_read_hydro.return_value = create_mock_hydrograph_data()

            result = sl.read_observed_pentadal_data()

            mock_read_hydro.assert_called_once_with(horizon_type="pentad")
            assert 'model_short' in result.columns
            assert result['model_short'].iloc[0] == 'Obs'
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('setup_library.fl.read_hydrograph_data')
    def test_decadal_api_enabled_uses_fl_read_hydrograph_data(self, mock_read_hydro, temp_csv_env):
        """When SAPPHIRE_API_ENABLED=true, should use fl.read_hydrograph_data() for decadal."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_read_hydro.return_value = create_mock_hydrograph_data()

            result = sl.read_observed_decadal_data()

            mock_read_hydro.assert_called_once_with(horizon_type="decade")
            assert result['model_short'].iloc[0] == 'Obs'
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('setup_library.fl.read_hydrograph_data')
    def test_pentad_api_fail_falls_back_to_csv(self, mock_read_hydro, temp_csv_env):
        """When API fails, should fall back to CSV."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_read_hydro.side_effect = Exception("API error")

            result = sl.read_observed_pentadal_data()

            # Should still return data from CSV fallback
            assert len(result) == 2
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('setup_library.fl.read_hydrograph_data')
    def test_pentad_renames_pentad_to_pentad_in_month(self, mock_read_hydro, temp_csv_env):
        """Should rename 'pentad' column to 'pentad_in_month' for consistency."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_data = create_mock_hydrograph_data()
            mock_data['pentad'] = [1, 1]
            mock_read_hydro.return_value = mock_data

            result = sl.read_observed_pentadal_data()

            assert 'pentad_in_month' in result.columns
            assert 'pentad' not in result.columns
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


# =============================================================================
# Tests for Default API Behavior
# =============================================================================

class TestDefaultApiBehavior:
    """Tests for default API enabled behavior when env var not set."""

    @patch('setup_library._read_lr_forecasts_from_api')
    @patch('setup_library.save_most_recent_forecasts')
    def test_default_uses_api_when_available(self, mock_save, mock_api_read, tmp_path):
        """When SAPPHIRE_API_ENABLED is not set, should default to API."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        # Ensure env var is not set
        os.environ.pop('SAPPHIRE_API_ENABLED', None)

        # Set up CSV fallback in case API check passes but something else fails
        csv_dir = tmp_path / "intermediate_data"
        csv_dir.mkdir()
        sample_data = pd.DataFrame({
            'code': ['12345'],
            'date': ['2024-01-05'],
            'forecasted_discharge': [100.5],
            'predictor': [80.0],
            'slope': [1.2],
            'intercept': [5.0],
            'q_mean': [95.0],
            'q_std_sigma': [15.0],
            'delta': [10.0],
            'rsquared': [0.85],
            'discharge_avg': [90.0],
            'pentad_in_month': [1],
            'pentad_in_year': [1],
        })
        pentad_path = csv_dir / "forecast_pentad_linreg.csv"
        sample_data.to_csv(pentad_path, index=False)
        os.environ['ieasyforecast_intermediate_data_path'] = str(csv_dir)
        os.environ['ieasyforecast_analysis_pentad_file'] = 'forecast_pentad_linreg.csv'

        try:
            mock_api_data = create_mock_lr_forecast_data()
            mock_api_data['model_long'] = 'Linear regression (LR)'
            mock_api_data['model_short'] = 'LR'
            mock_api_read.return_value = mock_api_data

            sl.read_linreg_forecasts_pentad()

            # Should call the API
            mock_api_read.assert_called_once()
        finally:
            os.environ.pop('ieasyforecast_intermediate_data_path', None)
            os.environ.pop('ieasyforecast_analysis_pentad_file', None)


# =============================================================================
# Tests for Data Consistency
# =============================================================================

class TestDataConsistency:
    """Tests to verify API and CSV return consistent data formats."""

    @pytest.fixture
    def temp_csv_env(self, tmp_path):
        """Set up temporary environment for consistency tests."""
        csv_dir = tmp_path / "intermediate_data"
        csv_dir.mkdir()

        # Create sample CSV data
        sample_data = pd.DataFrame({
            'code': ['12345', '12346'],
            'date': ['2024-01-05', '2024-01-05'],
            'forecasted_discharge': [100.5, 150.2],
            'predictor': [80.0, 120.0],
            'slope': [1.2, 1.1],
            'intercept': [5.0, 10.0],
            'q_mean': [95.0, 140.0],
            'q_std_sigma': [15.0, 20.0],
            'delta': [10.0, 13.0],
            'rsquared': [0.85, 0.90],
            'discharge_avg': [90.0, 130.0],
            'pentad_in_month': [1, 1],
            'pentad_in_year': [1, 1],
        })
        pentad_path = csv_dir / "forecast_pentad_linreg.csv"
        sample_data.to_csv(pentad_path, index=False)

        os.environ['ieasyforecast_intermediate_data_path'] = str(csv_dir)
        os.environ['ieasyforecast_analysis_pentad_file'] = 'forecast_pentad_linreg.csv'

        yield csv_dir

        os.environ.pop('ieasyforecast_intermediate_data_path', None)
        os.environ.pop('ieasyforecast_analysis_pentad_file', None)

    def test_csv_date_type_is_datetime(self, temp_csv_env):
        """CSV reader should convert date to datetime."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'
        try:
            forecasts, stats = sl.read_linreg_forecasts_pentad()

            assert pd.api.types.is_datetime64_any_dtype(forecasts['date'])
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    def test_csv_code_type_is_string(self, temp_csv_env):
        """CSV reader should have code as string type."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'
        try:
            forecasts, stats = sl.read_linreg_forecasts_pentad()

            assert forecasts['code'].dtype == 'object'
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('setup_library._read_lr_forecasts_from_api')
    @patch('setup_library.save_most_recent_forecasts')
    def test_api_and_csv_return_same_columns(self, mock_save, mock_api_read, temp_csv_env):
        """API and CSV should return DataFrames with same key columns."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        # Get CSV result
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'
        csv_forecasts, csv_stats = sl.read_linreg_forecasts_pentad()

        # Get API result
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_api_data = create_mock_lr_forecast_data()
            mock_api_data['model_long'] = 'Linear regression (LR)'
            mock_api_data['model_short'] = 'LR'
            mock_api_read.return_value = mock_api_data

            api_forecasts, api_stats = sl.read_linreg_forecasts_pentad()

            # Key columns should be present in both
            key_columns = ['code', 'date', 'forecasted_discharge', 'model_short', 'model_long']
            for col in key_columns:
                assert col in csv_forecasts.columns, f"CSV missing column: {col}"
                assert col in api_forecasts.columns, f"API missing column: {col}"

            # Stats columns should match
            stats_key_columns = ['date', 'code']
            for col in stats_key_columns:
                assert col in csv_stats.columns, f"CSV stats missing column: {col}"
                assert col in api_stats.columns, f"API stats missing column: {col}"
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


# =============================================================================
# Edge Case Tests
# =============================================================================

class TestEdgeCases:
    """Tests for edge cases and error handling."""

    @patch('setup_library.SapphirePostprocessingClient')
    def test_lr_api_handles_duplicate_records(self, mock_client_class):
        """Should handle duplicate records by keeping the last one."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_data = create_mock_lr_forecast_data()
        # Add a duplicate record
        duplicate_row = mock_data.iloc[[0]].copy()
        duplicate_row['forecasted_discharge'] = 999.9  # Different value
        mock_data = pd.concat([mock_data, duplicate_row], ignore_index=True)

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_lr_forecasts.side_effect = [mock_data, pd.DataFrame()]
        mock_client_class.return_value = mock_client

        result = sl._read_lr_forecasts_from_api(horizon_type="pentad")

        # Should only have 2 records (duplicates removed)
        assert len(result) == 2

    @patch('setup_library.SapphirePostprocessingClient')
    def test_ml_api_filters_by_model_type(self, mock_client_class):
        """When API returns multiple models, should filter by requested model."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_data = create_mock_ml_forecast_data()
        # Add records for different models
        other_model_data = mock_data.copy()
        other_model_data['model'] = 'TIDE'
        other_model_data['id'] = [3, 4]
        combined_data = pd.concat([mock_data, other_model_data], ignore_index=True)

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_forecasts.side_effect = [combined_data, pd.DataFrame()]
        mock_client_class.return_value = mock_client

        result = sl._read_ml_forecasts_from_api(model="TFT", horizon_type="pentad")

        # Should only have TFT records
        assert len(result) == 2

    def test_ml_read_pentad_missing_env_vars_returns_empty(self):
        """When environment variables are missing, should return empty DataFrame."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'
        os.environ.pop('ieasyforecast_intermediate_data_path', None)
        os.environ.pop('ieasyhydroforecast_OUTPUT_PATH_DISCHARGE', None)
        try:
            result = sl.read_machine_learning_forecasts_pentad(model="TFT")

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
