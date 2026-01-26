"""
Tests for SAPPHIRE API integration in postprocessing_forecasts module.

These tests verify that the combined forecast and skill metrics API writing functions
work correctly, including:
- Environment variable handling (SAPPHIRE_API_ENABLED)
- API health checks
- Correct field mapping for API payloads
- Model type mapping (TIDE -> TiDE, TSMIXER -> TSMixer)
- Handling of empty data and NaN values
"""

import os
import sys
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast'))

# Import the functions under test
from forecast_library import (
    _write_combined_forecast_to_api,
    _write_skill_metrics_to_api,
    SAPPHIRE_API_AVAILABLE,
)


class TestWriteCombinedForecastToApi:
    """Tests for the _write_combined_forecast_to_api function.

    This function writes combined forecasts from all models to the SAPPHIRE
    postprocessing API.
    """

    def test_api_disabled_via_env_var(self):
        """When SAPPHIRE_API_ENABLED=false, API write should be skipped."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'
        try:
            data = pd.DataFrame({
                'code': [12345],
                'date': pd.to_datetime(['2024-01-06']),
                'pentad_in_month': [1],
                'pentad_in_year': [1],
                'forecasted_discharge': [100.0],
                'model_short': ['LR'],
            })
            result = _write_combined_forecast_to_api(data, "pentad")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePostprocessingClient')
    def test_api_not_ready_raises_error(self, mock_client_class):
        """When API health check fails, should raise SapphireAPIError."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = False
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': [12345],
                'date': pd.to_datetime(['2024-01-06']),
                'pentad_in_month': [1],
                'pentad_in_year': [1],
                'forecasted_discharge': [100.0],
                'model_short': ['LR'],
            })

            with pytest.raises(Exception):  # SapphireAPIError
                _write_combined_forecast_to_api(data, "pentad")
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePostprocessingClient')
    def test_pentad_forecast_correct_fields(self, mock_client_class):
        """Test that pentadal forecast records have correct field mapping."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_forecasts.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': [12345],
                'date': pd.to_datetime(['2024-01-06']),
                'pentad_in_month': [2],
                'pentad_in_year': [2],
                'forecasted_discharge': [100.0],
                'model_short': ['LR'],
            })

            result = _write_combined_forecast_to_api(data, "pentad")
            assert result is True

            # Check that write_forecasts was called
            mock_client.write_forecasts.assert_called_once()

            # Get the records that were passed
            call_args = mock_client.write_forecasts.call_args[0][0]
            assert len(call_args) == 1
            record = call_args[0]

            # Check field mapping
            assert record['horizon_type'] == 'pentad'
            assert record['code'] == '12345'
            assert record['model_type'] == 'LR'
            assert record['date'] == '2024-01-06'
            assert record['target'] == '2024-01-06'
            assert record['horizon_value'] == 2
            assert record['horizon_in_year'] == 2
            assert record['forecasted_discharge'] == 100.0

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePostprocessingClient')
    def test_decade_forecast_correct_fields(self, mock_client_class):
        """Test that decadal forecast records have correct field mapping."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_forecasts.return_value = 1
            mock_client_class.return_value = mock_client

            # Note: save_forecast_data_decade renames decad_in_month to decad
            data = pd.DataFrame({
                'code': [12345],
                'date': pd.to_datetime(['2024-01-15']),
                'decad': [2],  # After rename from decad_in_month
                'decad_in_year': [2],
                'forecasted_discharge': [150.0],
                'model_short': ['TFT'],
            })

            result = _write_combined_forecast_to_api(data, "decade")
            assert result is True

            # Get the records that were passed
            call_args = mock_client.write_forecasts.call_args[0][0]
            record = call_args[0]

            # Check field mapping
            assert record['horizon_type'] == 'decade'
            assert record['code'] == '12345'
            assert record['model_type'] == 'TFT'
            assert record['horizon_value'] == 2
            assert record['horizon_in_year'] == 2
            assert record['forecasted_discharge'] == 150.0

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePostprocessingClient')
    def test_model_type_mapping(self, mock_client_class):
        """Test that model types are correctly mapped to API format."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_forecasts.return_value = 6
            mock_client_class.return_value = mock_client

            # Test all model types
            data = pd.DataFrame({
                'code': [12345, 12345, 12345, 12345, 12345, 12345],
                'date': pd.to_datetime(['2024-01-06'] * 6),
                'pentad_in_month': [1] * 6,
                'pentad_in_year': [1] * 6,
                'forecasted_discharge': [100.0] * 6,
                'model_short': ['LR', 'TFT', 'TIDE', 'TSMIXER', 'EM', 'NE'],
            })

            _write_combined_forecast_to_api(data, "pentad")

            call_args = mock_client.write_forecasts.call_args[0][0]

            # Check model type mappings
            model_types = [r['model_type'] for r in call_args]
            assert 'LR' in model_types
            assert 'TFT' in model_types
            assert 'TiDE' in model_types  # TIDE -> TiDE
            assert 'TSMixer' in model_types  # TSMIXER -> TSMixer
            assert 'EM' in model_types
            assert 'NE' in model_types

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePostprocessingClient')
    def test_rows_with_missing_required_fields_are_skipped(self, mock_client_class):
        """Test that rows with missing horizon values are skipped."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client_class.return_value = mock_client

            # Row with NaN required fields should be skipped
            data = pd.DataFrame({
                'code': [12345],
                'date': pd.to_datetime(['2024-01-06']),
                'pentad_in_month': [np.nan],
                'pentad_in_year': [np.nan],
                'forecasted_discharge': [100.0],
                'model_short': ['LR'],
            })

            result = _write_combined_forecast_to_api(data, "pentad")
            assert result is False  # No records to write

            # write_forecasts should not be called for skipped data
            mock_client.write_forecasts.assert_not_called()

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePostprocessingClient')
    def test_nan_optional_values_converted_to_none(self, mock_client_class):
        """Test that NaN optional values (forecasted_discharge) are converted to None."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_forecasts.return_value = 1
            mock_client_class.return_value = mock_client

            # Valid required fields, but NaN optional field
            data = pd.DataFrame({
                'code': [12345],
                'date': pd.to_datetime(['2024-01-06']),
                'pentad_in_month': [2],
                'pentad_in_year': [2],
                'forecasted_discharge': [np.nan],
                'model_short': ['LR'],
            })

            _write_combined_forecast_to_api(data, "pentad")

            call_args = mock_client.write_forecasts.call_args[0][0]
            record = call_args[0]

            # Optional field NaN should be converted to None
            assert record['forecasted_discharge'] is None
            # Required fields should have values
            assert record['horizon_value'] == 2
            assert record['horizon_in_year'] == 2

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePostprocessingClient')
    def test_empty_data_returns_false(self, mock_client_class):
        """Test that empty data returns False without calling API."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client_class.return_value = mock_client

            data = pd.DataFrame(columns=['code', 'date', 'pentad_in_month', 'pentad_in_year', 'forecasted_discharge', 'model_short'])

            result = _write_combined_forecast_to_api(data, "pentad")
            assert result is False

            # write_forecasts should not be called for empty data
            mock_client.write_forecasts.assert_not_called()

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


class TestWriteSkillMetricsToApi:
    """Tests for the _write_skill_metrics_to_api function.

    This function writes skill metrics to the SAPPHIRE postprocessing API.
    """

    def test_api_disabled_via_env_var(self):
        """When SAPPHIRE_API_ENABLED=false, API write should be skipped."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'
        try:
            data = pd.DataFrame({
                'code': [12345],
                'pentad_in_year': [1],
                'model_short': ['LR'],
                'sdivsigma': [0.5],
                'nse': [0.8],
                'delta': [0.1],
                'accuracy': [0.9],
                'mae': [5.0],
                'n_pairs': [100],
            })
            result = _write_skill_metrics_to_api(data, "pentad")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePostprocessingClient')
    def test_api_not_ready_raises_error(self, mock_client_class):
        """When API health check fails, should raise SapphireAPIError."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = False
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': [12345],
                'pentad_in_year': [1],
                'model_short': ['LR'],
                'sdivsigma': [0.5],
                'nse': [0.8],
                'delta': [0.1],
                'accuracy': [0.9],
                'mae': [5.0],
                'n_pairs': [100],
            })

            with pytest.raises(Exception):  # SapphireAPIError
                _write_skill_metrics_to_api(data, "pentad")
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePostprocessingClient')
    def test_pentad_skill_metrics_correct_fields(self, mock_client_class):
        """Test that pentadal skill metric records have correct field mapping."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_skill_metrics.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': [12345],
                'pentad_in_year': [5],
                'model_short': ['LR'],
                'sdivsigma': [0.5],
                'nse': [0.85],
                'delta': [0.12],
                'accuracy': [0.92],
                'mae': [4.5],
                'n_pairs': [150],
            })

            result = _write_skill_metrics_to_api(data, "pentad")
            assert result is True

            # Check that write_skill_metrics was called
            mock_client.write_skill_metrics.assert_called_once()

            # Get the records that were passed
            call_args = mock_client.write_skill_metrics.call_args[0][0]
            assert len(call_args) == 1
            record = call_args[0]

            # Check field mapping
            assert record['horizon_type'] == 'pentad'
            assert record['code'] == '12345'
            assert record['model_type'] == 'LR'
            assert 'date' in record  # Today's date
            assert record['horizon_in_year'] == 5
            assert record['sdivsigma'] == 0.5
            assert record['nse'] == 0.85
            assert record['delta'] == 0.12
            assert record['accuracy'] == 0.92
            assert record['mae'] == 4.5
            assert record['n_pairs'] == 150

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePostprocessingClient')
    def test_decade_skill_metrics_correct_fields(self, mock_client_class):
        """Test that decadal skill metric records have correct field mapping."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_skill_metrics.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': [12345],
                'decad_in_year': [10],
                'model_short': ['TFT'],
                'sdivsigma': [0.6],
                'nse': [0.75],
                'delta': [0.15],
                'accuracy': [0.88],
                'mae': [6.2],
                'n_pairs': [120],
            })

            result = _write_skill_metrics_to_api(data, "decade")
            assert result is True

            # Get the records that were passed
            call_args = mock_client.write_skill_metrics.call_args[0][0]
            record = call_args[0]

            # Check field mapping
            assert record['horizon_type'] == 'decade'
            assert record['code'] == '12345'
            assert record['model_type'] == 'TFT'
            assert record['horizon_in_year'] == 10

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePostprocessingClient')
    def test_model_type_mapping(self, mock_client_class):
        """Test that model types are correctly mapped to API format."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_skill_metrics.return_value = 6
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': [12345] * 6,
                'pentad_in_year': [1] * 6,
                'model_short': ['LR', 'TFT', 'TIDE', 'TSMIXER', 'EM', 'NE'],
                'sdivsigma': [0.5] * 6,
                'nse': [0.8] * 6,
                'delta': [0.1] * 6,
                'accuracy': [0.9] * 6,
                'mae': [5.0] * 6,
                'n_pairs': [100] * 6,
            })

            _write_skill_metrics_to_api(data, "pentad")

            call_args = mock_client.write_skill_metrics.call_args[0][0]

            # Check model type mappings
            model_types = [r['model_type'] for r in call_args]
            assert 'LR' in model_types
            assert 'TFT' in model_types
            assert 'TiDE' in model_types  # TIDE -> TiDE
            assert 'TSMixer' in model_types  # TSMIXER -> TSMixer
            assert 'EM' in model_types
            assert 'NE' in model_types

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePostprocessingClient')
    def test_nan_values_converted_to_none(self, mock_client_class):
        """Test that NaN values are converted to None."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_skill_metrics.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': [12345],
                'pentad_in_year': [1],
                'model_short': ['LR'],
                'sdivsigma': [np.nan],
                'nse': [np.nan],
                'delta': [np.nan],
                'accuracy': [np.nan],
                'mae': [np.nan],
                'n_pairs': [np.nan],
            })

            _write_skill_metrics_to_api(data, "pentad")

            call_args = mock_client.write_skill_metrics.call_args[0][0]
            record = call_args[0]

            # NaN should be converted to None
            assert record['sdivsigma'] is None
            assert record['nse'] is None
            assert record['delta'] is None
            assert record['accuracy'] is None
            assert record['mae'] is None
            assert record['n_pairs'] is None

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePostprocessingClient')
    def test_empty_data_returns_false(self, mock_client_class):
        """Test that empty data returns False without calling API."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client_class.return_value = mock_client

            data = pd.DataFrame(columns=['code', 'pentad_in_year', 'model_short', 'sdivsigma', 'nse', 'delta', 'accuracy', 'mae', 'n_pairs'])

            result = _write_skill_metrics_to_api(data, "pentad")
            assert result is False

            # write_skill_metrics should not be called for empty data
            mock_client.write_skill_metrics.assert_not_called()

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('forecast_library.SapphirePostprocessingClient')
    def test_composition_extracted_from_model_long(self, mock_client_class):
        """Test that composition is extracted from model_long for ensemble models."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_skill_metrics.return_value = 3
            mock_client_class.return_value = mock_client

            # Test data with model_long containing composition info
            data = pd.DataFrame({
                'code': [12345, 12345, 12345],
                'pentad_in_year': [1, 1, 1],
                'model_short': ['EM', 'NE', 'TFT'],
                'model_long': [
                    'Ens. Mean with TFT, TiDE, TSMixer (EM)',
                    'Neural Ensemble with LR, TFT (NE)',
                    'Temporal Fusion Transformer (TFT)'
                ],
                'sdivsigma': [0.1, 0.2, 0.3],
                'nse': [0.9, 0.8, 0.7],
                'delta': [0.1, 0.2, 0.3],
                'accuracy': [0.9, 0.8, 0.7],
                'mae': [5.0, 6.0, 7.0],
                'n_pairs': [100, 100, 100],
            })

            _write_skill_metrics_to_api(data, "pentad")

            call_args = mock_client.write_skill_metrics.call_args[0][0]

            # EM should have composition extracted
            em_record = next(r for r in call_args if r['model_type'] == 'EM')
            assert em_record['composition'] == 'TFT, TiDE, TSMixer'

            # NE should have composition extracted
            ne_record = next(r for r in call_args if r['model_type'] == 'NE')
            assert ne_record['composition'] == 'LR, TFT'

            # Non-ensemble (TFT) should have no composition
            tft_record = next(r for r in call_args if r['model_type'] == 'TFT')
            assert tft_record['composition'] is None

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
