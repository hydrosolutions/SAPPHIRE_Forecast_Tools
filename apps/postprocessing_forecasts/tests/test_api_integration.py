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
from src.api_writer import (
    _write_combined_forecast_to_api,
    _write_skill_metrics_to_api,
    MODEL_TYPE_MAP,
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

    @patch('src.api_writer.SapphirePostprocessingClient')
    def test_api_not_ready_returns_false(self, mock_client_class):
        """When API health check fails, should return False (non-blocking)."""
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

            result = _write_combined_forecast_to_api(data, "pentad")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('src.api_writer.SapphirePostprocessingClient')
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

    @patch('src.api_writer.SapphirePostprocessingClient')
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

            result = _write_combined_forecast_to_api(data, "decad")
            assert result is True

            # Get the records that were passed
            call_args = mock_client.write_forecasts.call_args[0][0]
            record = call_args[0]

            # Check field mapping — "decad" translates to "decade" at boundary
            assert record['horizon_type'] == 'decade'
            assert record['code'] == '12345'
            assert record['model_type'] == 'TFT'
            assert record['horizon_value'] == 2
            assert record['horizon_in_year'] == 2
            assert record['forecasted_discharge'] == 150.0

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('src.api_writer.SapphirePostprocessingClient')
    def test_em_forecast_includes_composition(self, mock_client_class):
        """EM forecast record includes composition from DataFrame column."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_forecasts.return_value = 2
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': [12345, 12345],
                'date': pd.to_datetime(['2024-01-06', '2024-01-06']),
                'pentad_in_month': [2, 2],
                'pentad_in_year': [2, 2],
                'forecasted_discharge': [100.0, 105.0],
                'model_short': ['LR', 'EM'],
                'composition': ['', 'LR, TFT'],
            })

            result = _write_combined_forecast_to_api(data, "pentad")
            assert result is True

            call_args = mock_client.write_forecasts.call_args[0][0]
            assert len(call_args) == 2

            # LR record: empty composition → None
            lr_rec = [r for r in call_args if r['model_type'] == 'LR'][0]
            assert lr_rec['composition'] is None or lr_rec['composition'] == ''

            # EM record: composition = 'LR, TFT'
            em_rec = [r for r in call_args if r['model_type'] == 'EM'][0]
            assert em_rec['composition'] == 'LR, TFT'
            assert em_rec['forecasted_discharge'] == 105.0
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('src.api_writer.SapphirePostprocessingClient')
    def test_em_forecast_warns_on_missing_composition(self, mock_client_class):
        """EM row without composition column logs a warning."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_forecasts.return_value = 1
            mock_client_class.return_value = mock_client

            # EM row with NO composition column
            data = pd.DataFrame({
                'code': [12345],
                'date': pd.to_datetime(['2024-01-06']),
                'pentad_in_month': [2],
                'pentad_in_year': [2],
                'forecasted_discharge': [105.0],
                'model_short': ['EM'],
            })

            import logging
            with patch.object(
                logging.getLogger('src.api_writer'), 'warning'
            ) as mock_warn:
                result = _write_combined_forecast_to_api(data, "pentad")
                assert result is True
                # Should warn about missing composition
                mock_warn.assert_called_once()
                assert 'ensemble forecast rows' in str(
                    mock_warn.call_args
                )
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('src.api_writer.SapphirePostprocessingClient')
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

    @patch('src.api_writer.SapphirePostprocessingClient')
    def test_missing_horizon_values_repaired_from_date(self, mock_client_class):
        """Test that missing horizon values are computed from valid dates."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_forecasts.return_value = 1
            mock_client_class.return_value = mock_client

            # Row with NaN horizon values but a valid date — should be repaired
            data = pd.DataFrame({
                'code': [12345],
                'date': pd.to_datetime(['2024-01-06']),
                'pentad_in_month': [np.nan],
                'pentad_in_year': [np.nan],
                'forecasted_discharge': [100.0],
                'model_short': ['LR'],
            })

            result = _write_combined_forecast_to_api(data, "pentad")
            assert result is True

            call_args = mock_client.write_forecasts.call_args[0][0]
            record = call_args[0]
            # Horizon values should be computed from the date
            assert record['horizon_value'] is not None
            assert record['horizon_in_year'] is not None

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('src.api_writer.SapphirePostprocessingClient')
    def test_rows_with_invalid_date_and_missing_horizon_are_skipped(
        self, mock_client_class
    ):
        """Test that rows with NaT date AND missing horizon are skipped."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client_class.return_value = mock_client

            # Row with NaT date and NaN horizon — cannot be repaired
            data = pd.DataFrame({
                'code': [12345],
                'date': [pd.NaT],
                'pentad_in_month': [np.nan],
                'pentad_in_year': [np.nan],
                'forecasted_discharge': [100.0],
                'model_short': ['LR'],
            })

            result = _write_combined_forecast_to_api(data, "pentad")
            assert result is False

            mock_client.write_forecasts.assert_not_called()

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('src.api_writer.SapphirePostprocessingClient')
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

    @patch('src.api_writer.SapphirePostprocessingClient')
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

    @patch('src.api_writer.SapphirePostprocessingClient')
    def test_api_not_ready_returns_false(self, mock_client_class):
        """When API health check fails, should return False (non-blocking)."""
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

            result = _write_skill_metrics_to_api(data, "pentad")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('src.api_writer.SapphirePostprocessingClient')
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

    @patch('src.api_writer.SapphirePostprocessingClient')
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

            result = _write_skill_metrics_to_api(data, "decad")
            assert result is True

            # Get the records that were passed
            call_args = mock_client.write_skill_metrics.call_args[0][0]
            record = call_args[0]

            # Check field mapping — "decad" translates to "decade" at boundary
            assert record['horizon_type'] == 'decade'
            assert record['code'] == '12345'
            assert record['model_type'] == 'TFT'
            assert record['horizon_in_year'] == 10

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('src.api_writer.SapphirePostprocessingClient')
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

    @patch('src.api_writer.SapphirePostprocessingClient')
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

    @patch('src.api_writer.SapphirePostprocessingClient')
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

    @patch('src.api_writer.SapphirePostprocessingClient')
    def test_composition_passed_through_for_ensembles(self, mock_client_class):
        """Test that composition column is passed through for ensemble models."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_skill_metrics.return_value = 3
            mock_client_class.return_value = mock_client

            # Test data with composition column (model_long is no longer used)
            data = pd.DataFrame({
                'code': [12345, 12345, 12345],
                'pentad_in_year': [1, 1, 1],
                'model_short': ['EM', 'NE', 'TFT'],
                'composition': [
                    'TFT, TiDE, TSMixer',
                    'LR, TFT',
                    None,
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

            # EM should have composition passed through
            em_record = next(r for r in call_args if r['model_type'] == 'EM')
            assert em_record['composition'] == 'TFT, TiDE, TSMixer'

            # NE should have composition passed through
            ne_record = next(r for r in call_args if r['model_type'] == 'NE')
            assert ne_record['composition'] == 'LR, TFT'

            # Non-ensemble (TFT) should have no composition
            tft_record = next(r for r in call_args if r['model_type'] == 'TFT')
            assert tft_record['composition'] is None

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('src.api_writer.SapphirePostprocessingClient')
    def test_skill_metrics_api_exception_propagates(
        self, mock_client_class
    ):
        """API client raises RuntimeError -> exception propagates.

        Unlike the combined forecast writer, _write_skill_metrics_to_api
        does NOT catch exceptions from client.write_skill_metrics(). The
        caller is responsible for handling the failure mode via
        SAPPHIRE_API_FAILURE_MODE.
        """
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_skill_metrics.side_effect = RuntimeError(
                "API connection failed"
            )
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

            with pytest.raises(RuntimeError, match="API connection failed"):
                _write_skill_metrics_to_api(data, "pentad")

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    def test_invalid_horizon_type_raises_value_error(self):
        """Invalid horizon_type raises ValueError with descriptive message."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

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

        with pytest.raises(ValueError, match="Invalid horizon_type"):
            _write_skill_metrics_to_api(data, "weekly")

    @patch('src.api_writer.SapphirePostprocessingClient')
    def test_skill_metrics_empty_data_returns_false(
        self, mock_client_class
    ):
        """Empty DataFrame returns False, API client never called."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client_class.return_value = mock_client

            data = pd.DataFrame(columns=[
                'code', 'pentad_in_year', 'model_short',
                'sdivsigma', 'nse', 'delta', 'accuracy', 'mae', 'n_pairs',
            ])

            result = _write_skill_metrics_to_api(data, "pentad")
            assert result is False
            mock_client.write_skill_metrics.assert_not_called()

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


class TestModelTypeMap:
    """Tests for MODEL_TYPE_MAP completeness and correctness."""

    def test_short_term_models_present(self):
        """All short-term model types are in the map."""
        for key in ["LR", "TFT", "TIDE", "TSMIXER", "EM", "NE", "RRAM"]:
            assert key in MODEL_TYPE_MAP, f"Missing short-term model: {key}"

    def test_long_term_models_present(self):
        """All long-term model types are in the map."""
        lt_models = {
            "GBT": "GBT",
            "LR_BASE": "LR_Base",
            "LR_SM": "LR_SM",
            "LR_SM_DT": "LR_SM_DT",
            "LR_SM_ROF": "LR_SM_ROF",
            "MC_ALD": "MC_ALD",
            "SM_GBT": "SM_GBT",
            "SM_GBT_LR": "SM_GBT_LR",
            "SM_GBT_NORM": "SM_GBT_Norm",
        }
        for key, expected_value in lt_models.items():
            assert key in MODEL_TYPE_MAP, f"Missing LT model: {key}"
            assert MODEL_TYPE_MAP[key] == expected_value, (
                f"MODEL_TYPE_MAP['{key}'] = '{MODEL_TYPE_MAP[key]}', "
                f"expected '{expected_value}'"
            )

    def test_baseline_models_present(self):
        """Naive Mean and Skilled Mean baselines are in the map."""
        assert "NAIVE MEAN" in MODEL_TYPE_MAP
        assert MODEL_TYPE_MAP["NAIVE MEAN"] == "Naive Mean"
        assert "SKILLED MEAN" in MODEL_TYPE_MAP
        assert MODEL_TYPE_MAP["SKILLED MEAN"] == "Skilled Mean"

    def test_skilled_mean_model_type_mapping(self):
        """Skilled Mean maps correctly through MODEL_TYPE_MAP."""
        # The key is uppercase, the value preserves original casing
        assert MODEL_TYPE_MAP["SKILLED MEAN"] == "Skilled Mean"
        # Verify it's distinct from Naive Mean
        assert MODEL_TYPE_MAP["SKILLED MEAN"] != MODEL_TYPE_MAP["NAIVE MEAN"]


class TestWriteMonthlySkillMetricsToApi:
    """Tests for _write_skill_metrics_to_api with horizon_type='month'.

    Monthly skill metrics come from calculate_monthly_skill_metrics() and
    have month_in_year (1-12) instead of pentad_in_year or decad_in_year.
    """

    @patch('src.api_writer.SapphirePostprocessingClient')
    def test_monthly_skill_metrics_correct_fields(self, mock_client_class):
        """Monthly skill metrics produce records with horizon_type='month'."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_skill_metrics.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': ['15013'],
                'month_in_year': [6],
                'model_short': ['GBT'],
                'sdivsigma': [0.45],
                'nse': [0.82],
                'delta': [12.5],
                'accuracy': [0.75],
                'mae': [8.3],
                'n_pairs': [10],
                'crps': [15.2],
            })

            result = _write_skill_metrics_to_api(data, "month")
            assert result is True

            mock_client.write_skill_metrics.assert_called_once()
            call_args = mock_client.write_skill_metrics.call_args[0][0]
            assert len(call_args) == 1
            record = call_args[0]

            assert record['horizon_type'] == 'month'
            assert record['code'] == '15013'
            assert record['model_type'] == 'GBT'
            assert record['horizon_in_year'] == 6
            assert record['sdivsigma'] == 0.45
            assert record['nse'] == 0.82
            assert record['mae'] == 8.3
            assert record['n_pairs'] == 10
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('src.api_writer.SapphirePostprocessingClient')
    def test_crps_not_sent_to_api(self, mock_client_class):
        """CRPS column is not included in API records (schema doesn't support it yet)."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_skill_metrics.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': ['15013'],
                'month_in_year': [3],
                'model_short': ['GBT'],
                'sdivsigma': [0.5],
                'nse': [0.8],
                'delta': [10.0],
                'accuracy': [0.85],
                'mae': [6.0],
                'n_pairs': [8],
                'crps': [12.0],
            })

            _write_skill_metrics_to_api(data, "month")

            call_args = mock_client.write_skill_metrics.call_args[0][0]
            record = call_args[0]
            assert 'crps' not in record
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('src.api_writer.SapphirePostprocessingClient')
    def test_lt_model_type_mapping(self, mock_client_class):
        """Long-term model types are mapped correctly to API format."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_skill_metrics.return_value = 5
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': ['15013'] * 5,
                'month_in_year': [6] * 5,
                'model_short': [
                    'GBT', 'LR_Base', 'SM_GBT', 'MC_ALD', 'SM_GBT_Norm',
                ],
                'sdivsigma': [0.5] * 5,
                'nse': [0.8] * 5,
                'delta': [10.0] * 5,
                'accuracy': [0.85] * 5,
                'mae': [6.0] * 5,
                'n_pairs': [10] * 5,
            })

            _write_skill_metrics_to_api(data, "month")

            call_args = mock_client.write_skill_metrics.call_args[0][0]
            model_types = {r['model_type'] for r in call_args}

            assert 'GBT' in model_types
            assert 'LR_Base' in model_types
            assert 'SM_GBT' in model_types
            assert 'MC_ALD' in model_types
            assert 'SM_GBT_Norm' in model_types
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('src.api_writer.SapphirePostprocessingClient')
    def test_naive_mean_model_type_mapping(self, mock_client_class):
        """Naive Mean model maps correctly despite space in name."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_skill_metrics.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': ['15013'],
                'month_in_year': [6],
                'model_short': ['Naive Mean'],
                'sdivsigma': [0.7],
                'nse': [0.3],
                'delta': [10.0],
                'accuracy': [0.5],
                'mae': [15.0],
                'n_pairs': [10],
            })

            _write_skill_metrics_to_api(data, "month")

            call_args = mock_client.write_skill_metrics.call_args[0][0]
            record = call_args[0]
            assert record['model_type'] == 'Naive Mean'
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('src.api_writer.SapphirePostprocessingClient')
    def test_em_ensemble_with_composition(self, mock_client_class):
        """Monthly EM ensemble passes composition to API."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_skill_metrics.return_value = 2
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': ['15013', '15013'],
                'month_in_year': [6, 6],
                'model_short': ['GBT', 'EM'],
                'composition': [None, 'GBT, LR_Base, SM_GBT'],
                'sdivsigma': [0.5, 0.4],
                'nse': [0.8, 0.85],
                'delta': [10.0, 9.0],
                'accuracy': [0.85, 0.90],
                'mae': [6.0, 5.5],
                'n_pairs': [10, 10],
            })

            _write_skill_metrics_to_api(data, "month")

            call_args = mock_client.write_skill_metrics.call_args[0][0]

            em_rec = next(r for r in call_args if r['model_type'] == 'EM')
            assert em_rec['composition'] == 'GBT, LR_Base, SM_GBT'

            gbt_rec = next(r for r in call_args if r['model_type'] == 'GBT')
            assert gbt_rec['composition'] is None
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('src.api_writer.SapphirePostprocessingClient')
    def test_monthly_empty_data_returns_false(self, mock_client_class):
        """Empty monthly DataFrame returns False."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client_class.return_value = mock_client

            data = pd.DataFrame(columns=[
                'code', 'month_in_year', 'model_short',
                'sdivsigma', 'nse', 'delta', 'accuracy', 'mae', 'n_pairs',
            ])

            result = _write_skill_metrics_to_api(data, "month")
            assert result is False
            mock_client.write_skill_metrics.assert_not_called()
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('src.api_writer.SapphirePostprocessingClient')
    def test_monthly_nan_metrics_converted_to_none(self, mock_client_class):
        """NaN metric values in monthly data are converted to None for API."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_skill_metrics.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': ['15013'],
                'month_in_year': [1],
                'model_short': ['GBT'],
                'sdivsigma': [np.nan],
                'nse': [np.nan],
                'delta': [np.nan],
                'accuracy': [np.nan],
                'mae': [np.nan],
                'n_pairs': [np.nan],
                'crps': [np.nan],
            })

            _write_skill_metrics_to_api(data, "month")

            call_args = mock_client.write_skill_metrics.call_args[0][0]
            record = call_args[0]

            assert record['sdivsigma'] is None
            assert record['nse'] is None
            assert record['mae'] is None
            assert record['n_pairs'] is None
            assert 'crps' not in record
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


class TestHorizonTypeToApiMapping:
    """Tests for HORIZON_TYPE_TO_API translation layer."""

    def test_horizon_type_to_api_mapping(self):
        """Constant maps internal names to API enum values."""
        from src.api_writer import HORIZON_TYPE_TO_API
        assert HORIZON_TYPE_TO_API == {
            "pentad": "pentad",
            "decad": "decade",
            "month": "month",
        }

    def test_old_decade_string_raises_combined_forecast(self):
        """Passing 'decade' (old API name) to combined forecast raises."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        data = pd.DataFrame({
            'code': [12345],
            'date': pd.to_datetime(['2024-01-15']),
            'decad': [2],
            'decad_in_year': [2],
            'forecasted_discharge': [150.0],
            'model_short': ['TFT'],
        })
        with pytest.raises(ValueError, match="Invalid horizon_type"):
            _write_combined_forecast_to_api(data, "decade")

    def test_old_decade_string_raises_skill_metrics(self):
        """Passing 'decade' (old API name) to skill metrics raises."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

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
        with pytest.raises(ValueError, match="Invalid horizon_type"):
            _write_skill_metrics_to_api(data, "decade")

    @patch('src.api_writer.SapphirePostprocessingClient')
    def test_decad_translates_to_decade_in_api_records(
        self, mock_client_class
    ):
        """Internal 'decad' becomes 'decade' in the API record."""
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
                'date': pd.to_datetime(['2024-01-15']),
                'decad': [2],
                'decad_in_year': [2],
                'forecasted_discharge': [150.0],
                'model_short': ['TFT'],
            })

            _write_combined_forecast_to_api(data, "decad")
            record = mock_client.write_forecasts.call_args[0][0][0]
            assert record['horizon_type'] == 'decade'
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
