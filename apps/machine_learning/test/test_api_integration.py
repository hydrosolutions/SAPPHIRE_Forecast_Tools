"""
Tests for SAPPHIRE API integration in machine_learning module.

Tests the ML forecast API integration:
- _write_ml_forecast_to_api function (operational: today's data only)
- _check_ml_forecast_consistency function
- calculate_pentad_from_date and calculate_decad_from_date helpers
"""
import os
import pandas as pd
import numpy as np
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

import sys

# Mock heavy dependencies before importing the module
sys.modules['darts'] = MagicMock()
sys.modules['darts.TimeSeries'] = MagicMock()
sys.modules['darts.concatenate'] = MagicMock()
sys.modules['darts.utils'] = MagicMock()
sys.modules['darts.utils.timeseries_generation'] = MagicMock()
sys.modules['darts.models'] = MagicMock()
sys.modules['pytorch_lightning'] = MagicMock()
sys.modules['pytorch_lightning.callbacks'] = MagicMock()
sys.modules['torch'] = MagicMock()
sys.modules['pe_oudin'] = MagicMock()
sys.modules['pe_oudin.PE_Oudin'] = MagicMock()
sys.modules['suntime'] = MagicMock()

# Add machine_learning and scr to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scr'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast'))

from scr.utils_ml_forecast import (
    _write_ml_forecast_to_api,
    _check_ml_forecast_consistency,
    calculate_pentad_from_date,
    calculate_decad_from_date,
    SAPPHIRE_API_AVAILABLE
)


# =============================================================================
# Tests for calculate_pentad_from_date
# =============================================================================

class TestCalculatePentadFromDate:
    """Tests for the calculate_pentad_from_date helper function."""

    def test_first_pentad_of_month(self):
        """Days 1-5 should be pentad 1."""
        for day in range(1, 6):
            date = datetime(2024, 1, day)
            pentad_in_month, pentad_in_year = calculate_pentad_from_date(date)
            assert pentad_in_month == 1
            assert pentad_in_year == 1  # January pentad 1

    def test_second_pentad_of_month(self):
        """Days 6-10 should be pentad 2."""
        for day in range(6, 11):
            date = datetime(2024, 1, day)
            pentad_in_month, _ = calculate_pentad_from_date(date)
            assert pentad_in_month == 2

    def test_sixth_pentad_includes_end_of_month(self):
        """Days 26-31 should be pentad 6."""
        # Test day 31 (January)
        date = datetime(2024, 1, 31)
        pentad_in_month, _ = calculate_pentad_from_date(date)
        assert pentad_in_month == 6

        # Test day 28 (February, non-leap)
        date = datetime(2023, 2, 28)
        pentad_in_month, _ = calculate_pentad_from_date(date)
        assert pentad_in_month == 6

    def test_pentad_in_year_calculation(self):
        """Test pentad_in_year is calculated correctly (1-72)."""
        # January pentad 1 = 1
        date = datetime(2024, 1, 1)
        _, pentad_in_year = calculate_pentad_from_date(date)
        assert pentad_in_year == 1

        # February pentad 1 = 7 (6 pentads in Jan + 1)
        date = datetime(2024, 2, 1)
        _, pentad_in_year = calculate_pentad_from_date(date)
        assert pentad_in_year == 7

        # December pentad 6 = 72
        date = datetime(2024, 12, 31)
        _, pentad_in_year = calculate_pentad_from_date(date)
        assert pentad_in_year == 72

    def test_string_date_input(self):
        """Should handle string date input."""
        pentad_in_month, pentad_in_year = calculate_pentad_from_date('2024-03-15')
        assert pentad_in_month == 3  # Days 11-15
        assert pentad_in_year == 15  # March pentad 3


# =============================================================================
# Tests for calculate_decad_from_date
# =============================================================================

class TestCalculateDecadFromDate:
    """Tests for the calculate_decad_from_date helper function."""

    def test_first_decad_of_month(self):
        """Days 1-10 should be decad 1."""
        for day in range(1, 11):
            date = datetime(2024, 1, day)
            decad_in_month, decad_in_year = calculate_decad_from_date(date)
            assert decad_in_month == 1
            assert decad_in_year == 1  # January decad 1

    def test_second_decad_of_month(self):
        """Days 11-20 should be decad 2."""
        for day in range(11, 21):
            date = datetime(2024, 1, day)
            decad_in_month, _ = calculate_decad_from_date(date)
            assert decad_in_month == 2

    def test_third_decad_includes_end_of_month(self):
        """Days 21-31 should be decad 3."""
        date = datetime(2024, 1, 31)
        decad_in_month, _ = calculate_decad_from_date(date)
        assert decad_in_month == 3

    def test_decad_in_year_calculation(self):
        """Test decad_in_year is calculated correctly (1-36)."""
        # January decad 1 = 1
        date = datetime(2024, 1, 1)
        _, decad_in_year = calculate_decad_from_date(date)
        assert decad_in_year == 1

        # December decad 3 = 36
        date = datetime(2024, 12, 31)
        _, decad_in_year = calculate_decad_from_date(date)
        assert decad_in_year == 36


# =============================================================================
# Tests for _write_ml_forecast_to_api
# =============================================================================

class TestWriteMLForecastToApi:
    """Tests for the _write_ml_forecast_to_api function.

    This function writes ML forecasts to the SAPPHIRE postprocessing API.
    """

    def test_api_disabled_via_env_var(self):
        """When SAPPHIRE_API_ENABLED=false, API write should be skipped."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'
        try:
            data = pd.DataFrame({
                'code': [12345],
                'date': pd.to_datetime(['2024-01-06']),
                'forecast_date': pd.to_datetime(['2024-01-01']),
                'flag': [0],
                'Q5': [50.0],
                'Q25': [80.0],
                'Q50': [100.0],
                'Q75': [120.0],
                'Q95': [150.0],
            })
            result = _write_ml_forecast_to_api(data, "pentad", "TFT")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('scr.utils_ml_forecast.SapphirePostprocessingClient')
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
                'forecast_date': pd.to_datetime(['2024-01-01']),
                'flag': [0],
                'Q5': [50.0],
                'Q25': [80.0],
                'Q50': [100.0],
                'Q75': [120.0],
                'Q95': [150.0],
            })

            result = _write_ml_forecast_to_api(data, "pentad", "TFT")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('scr.utils_ml_forecast.SapphirePostprocessingClient')
    def test_writes_forecast_with_correct_fields(self, mock_client_class):
        """Test that forecast records have correct field mapping."""
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
                'date': pd.to_datetime(['2024-01-06']),  # Target date
                'forecast_date': pd.to_datetime(['2024-01-01']),  # When forecast was made
                'flag': [0],
                'Q5': [50.0],
                'Q25': [80.0],
                'Q50': [100.0],
                'Q75': [120.0],
                'Q95': [150.0],
            })

            result = _write_ml_forecast_to_api(data, "pentad", "TFT")
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
            assert record['model_type'] == 'TFT'
            assert record['date'] == '2024-01-01'  # forecast_date
            assert record['target'] == '2024-01-06'  # date
            assert record['flag'] == 0
            assert record['q05'] == 50.0
            assert record['q25'] == 80.0
            assert record['q50'] == 100.0
            assert record['q75'] == 120.0
            assert record['q95'] == 150.0
            assert record['forecasted_discharge'] == 100.0  # Same as Q50

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('scr.utils_ml_forecast.SapphirePostprocessingClient')
    def test_model_type_mapping(self, mock_client_class):
        """Test that model types are correctly mapped to API format."""
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
                'forecast_date': pd.to_datetime(['2024-01-01']),
                'flag': [0],
                'Q50': [100.0],
            })

            # Test TIDE -> TiDE
            _write_ml_forecast_to_api(data, "pentad", "TIDE")
            call_args = mock_client.write_forecasts.call_args[0][0]
            assert call_args[0]['model_type'] == 'TiDE'

            # Test TSMIXER -> TSMixer
            _write_ml_forecast_to_api(data, "pentad", "TSMIXER")
            call_args = mock_client.write_forecasts.call_args[0][0]
            assert call_args[0]['model_type'] == 'TSMixer'

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('scr.utils_ml_forecast.SapphirePostprocessingClient')
    def test_decade_horizon_values(self, mock_client_class):
        """Test that decade horizon values are calculated correctly."""
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
                'date': pd.to_datetime(['2024-01-15']),  # Target date in decad 2
                'forecast_date': pd.to_datetime(['2024-01-10']),
                'flag': [0],
                'Q50': [100.0],
            })

            _write_ml_forecast_to_api(data, "decade", "TFT")

            call_args = mock_client.write_forecasts.call_args[0][0]
            record = call_args[0]

            assert record['horizon_type'] == 'decade'
            assert record['horizon_value'] == 2  # Days 11-20 = decad 2
            assert record['horizon_in_year'] == 2  # January decad 2

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('scr.utils_ml_forecast.SapphirePostprocessingClient')
    def test_nan_values_converted_to_none(self, mock_client_class):
        """Test that NaN values are converted to None."""
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
                'forecast_date': pd.to_datetime(['2024-01-01']),
                'flag': [1],
                'Q5': [np.nan],
                'Q25': [np.nan],
                'Q50': [np.nan],
                'Q75': [np.nan],
                'Q95': [np.nan],
            })

            _write_ml_forecast_to_api(data, "pentad", "TFT")

            call_args = mock_client.write_forecasts.call_args[0][0]
            record = call_args[0]

            # NaN should be converted to None
            assert record['q05'] is None
            assert record['q50'] is None
            assert record['forecasted_discharge'] is None

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('scr.utils_ml_forecast.SapphirePostprocessingClient')
    def test_empty_data_returns_false(self, mock_client_class):
        """Test that empty data returns False without calling API."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client_class.return_value = mock_client

            data = pd.DataFrame(columns=['code', 'date', 'forecast_date', 'flag', 'Q50'])

            result = _write_ml_forecast_to_api(data, "pentad", "TFT")
            assert result is False

            # write_forecasts should not be called for empty data
            mock_client.write_forecasts.assert_not_called()

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


# =============================================================================
# Tests for _check_ml_forecast_consistency
# =============================================================================

class TestCheckMLForecastConsistency:
    """Tests for the _check_ml_forecast_consistency function."""

    def test_consistency_check_disabled_by_default(self):
        """When SAPPHIRE_CONSISTENCY_CHECK is not set, should return True."""
        os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)

        data = pd.DataFrame({
            'code': [12345],
            'date': pd.to_datetime(['2024-01-06']),
            'forecast_date': pd.to_datetime(['2024-01-01']),
            'Q50': [100.0],
        })

        result = _check_ml_forecast_consistency(data, "pentad", "TFT")
        assert result is True

    def test_consistency_check_disabled_via_env(self):
        """When SAPPHIRE_CONSISTENCY_CHECK=false, should return True."""
        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'false'
        try:
            data = pd.DataFrame({
                'code': [12345],
                'date': pd.to_datetime(['2024-01-06']),
                'forecast_date': pd.to_datetime(['2024-01-01']),
                'Q50': [100.0],
            })

            result = _check_ml_forecast_consistency(data, "pentad", "TFT")
            assert result is True
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)

    @patch('scr.utils_ml_forecast.SapphirePostprocessingClient')
    def test_consistency_check_passes_on_match(self, mock_client_class):
        """When API data matches CSV data, should return True."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            # Return matching data from API
            mock_client.read_forecasts.return_value = pd.DataFrame({
                'code': ['12345'],
                'target': pd.to_datetime(['2024-01-06']),
                'date': pd.to_datetime(['2024-01-01']),
                'q50': [100.0],
            })
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'code': [12345],
                'date': pd.to_datetime(['2024-01-06']),
                'forecast_date': pd.to_datetime(['2024-01-01']),
                'Q50': [100.0],
            })

            result = _check_ml_forecast_consistency(csv_data, "pentad", "TFT")
            assert result is True

        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('scr.utils_ml_forecast.SapphirePostprocessingClient')
    def test_consistency_check_fails_on_row_count_mismatch(self, mock_client_class):
        """When API has different row count, should return False."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            # Return different row count from API
            mock_client.read_forecasts.return_value = pd.DataFrame({
                'code': ['12345', '12345'],  # 2 rows
                'target': pd.to_datetime(['2024-01-06', '2024-01-07']),
                'date': pd.to_datetime(['2024-01-01', '2024-01-01']),
                'q50': [100.0, 110.0],
            })
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'code': [12345],  # 1 row
                'date': pd.to_datetime(['2024-01-06']),
                'forecast_date': pd.to_datetime(['2024-01-01']),
                'Q50': [100.0],
            })

            result = _check_ml_forecast_consistency(csv_data, "pentad", "TFT")
            assert result is False

        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('scr.utils_ml_forecast.SapphirePostprocessingClient')
    def test_consistency_check_fails_on_value_mismatch(self, mock_client_class):
        """When API has different values, should return False."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            # Return different value from API
            mock_client.read_forecasts.return_value = pd.DataFrame({
                'code': ['12345'],
                'target': pd.to_datetime(['2024-01-06']),
                'date': pd.to_datetime(['2024-01-01']),
                'q50': [200.0],  # Different from CSV
            })
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'code': [12345],
                'date': pd.to_datetime(['2024-01-06']),
                'forecast_date': pd.to_datetime(['2024-01-01']),
                'Q50': [100.0],  # Different from API
            })

            result = _check_ml_forecast_consistency(csv_data, "pentad", "TFT")
            assert result is False

        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
