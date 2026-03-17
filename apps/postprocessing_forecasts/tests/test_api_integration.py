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
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

# Import the functions under test
from src.api_writer import (
    MODEL_TYPE_MAP,
    SAPPHIRE_API_AVAILABLE,
    _write_combined_forecast_to_api,
    _write_skill_metrics_to_api,
)


class TestWriteCombinedForecastToApi:
    """Tests for the _write_combined_forecast_to_api function.

    This function writes combined forecasts from all models to the SAPPHIRE
    postprocessing API.
    """

    @pytest.fixture(autouse=True)
    def _set_api_env(self, monkeypatch):
        """Enable API by default; individual tests can override."""
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")

    def test_api_disabled_via_env_var(self, monkeypatch):
        """When SAPPHIRE_API_ENABLED=false, API write should be skipped."""
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "false")
        data = pd.DataFrame(
            {
                "code": [12345],
                "date": pd.to_datetime(["2024-01-06"]),
                "pentad_in_month": [1],
                "pentad_in_year": [1],
                "forecasted_discharge": [100.0],
                "model_short": ["TFT"],
            }
        )
        result = _write_combined_forecast_to_api(data, "pentad")
        assert result is False

    def test_lr_only_data_returns_false(self):
        """LR rows are excluded — LR-only input returns False."""
        data = pd.DataFrame(
            {
                "code": [12345],
                "date": pd.to_datetime(["2024-01-06"]),
                "pentad_in_month": [1],
                "pentad_in_year": [1],
                "forecasted_discharge": [100.0],
                "model_short": ["LR"],
            }
        )
        result = _write_combined_forecast_to_api(data, "pentad")
        assert result is False

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_api_not_ready_returns_false(self, mock_client_class):
        """When API health check fails, should return False (non-blocking)."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = False
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": [12345],
                "date": pd.to_datetime(["2024-01-06"]),
                "pentad_in_month": [1],
                "pentad_in_year": [1],
                "forecasted_discharge": [100.0],
                "model_short": ["TFT"],
            }
        )

        result = _write_combined_forecast_to_api(data, "pentad")
        assert result is False

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_pentad_forecast_correct_fields(self, mock_client_class):
        """Test that pentadal forecast records have correct field mapping."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_forecasts.return_value = 1
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": [12345],
                "date": pd.to_datetime(["2024-01-06"]),
                "pentad_in_month": [2],
                "pentad_in_year": [2],
                "forecasted_discharge": [100.0],
                "model_short": ["TFT"],
            }
        )

        result = _write_combined_forecast_to_api(data, "pentad")
        assert result is True

        # Check that write_forecasts was called
        mock_client.write_forecasts.assert_called_once()

        # Get the records that were passed
        call_args = mock_client.write_forecasts.call_args[0][0]
        assert len(call_args) == 1
        record = call_args[0]

        # Check field mapping
        assert record["horizon_type"] == "pentad"
        assert record["code"] == "12345"
        assert record["model_type"] == "TFT"
        assert record["date"] == "2024-01-06"
        assert record["target"] == "2024-01-07"  # date + 1 day
        assert record["horizon_value"] == 2
        assert record["horizon_in_year"] == 2
        assert record["forecasted_discharge"] == 100.0

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_decade_forecast_correct_fields(self, mock_client_class):
        """Test that decadal forecast records have correct field mapping."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_forecasts.return_value = 1
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": [12345],
                "date": pd.to_datetime(["2024-01-15"]),
                "decad_in_month": [2],
                "decad_in_year": [2],
                "forecasted_discharge": [150.0],
                "model_short": ["TFT"],
            }
        )

        result = _write_combined_forecast_to_api(data, "decad")
        assert result is True

        # Get the records that were passed
        call_args = mock_client.write_forecasts.call_args[0][0]
        record = call_args[0]

        # Check field mapping — "decad" translates to "decade" at boundary
        assert record["horizon_type"] == "decade"
        assert record["code"] == "12345"
        assert record["model_type"] == "TFT"
        assert record["horizon_value"] == 2
        assert record["horizon_in_year"] == 2
        assert record["forecasted_discharge"] == 150.0

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_em_forecast_includes_composition(self, mock_client_class):
        """EM forecast record includes composition from DataFrame column."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_forecasts.return_value = 2
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": [12345, 12345],
                "date": pd.to_datetime(["2024-01-06", "2024-01-06"]),
                "pentad_in_month": [2, 2],
                "pentad_in_year": [2, 2],
                "forecasted_discharge": [100.0, 105.0],
                "model_short": ["TFT", "EM"],
                "composition": ["", "LR, TFT"],
            }
        )

        result = _write_combined_forecast_to_api(data, "pentad")
        assert result is True

        call_args = mock_client.write_forecasts.call_args[0][0]
        assert len(call_args) == 2

        # TFT record: empty composition passes through as empty string
        tft_rec = [r for r in call_args if r["model_type"] == "TFT"][0]
        assert tft_rec["composition"] == ""

        # EM record: composition = 'LR, TFT'
        em_rec = [r for r in call_args if r["model_type"] == "EM"][0]
        assert em_rec["composition"] == "LR, TFT"
        assert em_rec["forecasted_discharge"] == 105.0

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_em_forecast_warns_on_missing_composition(self, mock_client_class):
        """EM row without composition column logs a warning."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_forecasts.return_value = 1
        mock_client_class.return_value = mock_client

        # EM row with NO composition column
        data = pd.DataFrame(
            {
                "code": [12345],
                "date": pd.to_datetime(["2024-01-06"]),
                "pentad_in_month": [2],
                "pentad_in_year": [2],
                "forecasted_discharge": [105.0],
                "model_short": ["EM"],
            }
        )

        import logging

        with patch.object(logging.getLogger("src.api_writer"), "warning") as mock_warn:
            result = _write_combined_forecast_to_api(data, "pentad")
            assert result is True
            # Should warn about missing composition
            mock_warn.assert_called_once()
            assert "ensemble forecast rows" in str(mock_warn.call_args)

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_model_type_mapping(self, mock_client_class):
        """Test that model types are correctly mapped to API format.

        LR is excluded from the combined forecast write (it lives in
        the lr_forecasts table), so only ML/ensemble models are tested.
        """
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_forecasts.return_value = 5
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": [12345, 12345, 12345, 12345, 12345],
                "date": pd.to_datetime(["2024-01-06"] * 5),
                "pentad_in_month": [1] * 5,
                "pentad_in_year": [1] * 5,
                "forecasted_discharge": [100.0] * 5,
                "model_short": ["TFT", "TIDE", "TSMIXER", "EM", "NE"],
            }
        )

        _write_combined_forecast_to_api(data, "pentad")

        call_args = mock_client.write_forecasts.call_args[0][0]

        model_types = [r["model_type"] for r in call_args]
        assert "TFT" in model_types
        assert "TiDE" in model_types  # TIDE -> TiDE
        assert "TSMixer" in model_types  # TSMIXER -> TSMixer
        assert "EM" in model_types
        assert "NE" in model_types

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_missing_horizon_values_repaired_from_date(self, mock_client_class):
        """Test that missing horizon values are computed from valid dates."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_forecasts.return_value = 1
        mock_client_class.return_value = mock_client

        # Row with NaN horizon values but a valid date — should be repaired
        data = pd.DataFrame(
            {
                "code": [12345],
                "date": pd.to_datetime(["2024-01-06"]),
                "pentad_in_month": [np.nan],
                "pentad_in_year": [np.nan],
                "forecasted_discharge": [100.0],
                "model_short": ["TFT"],
            }
        )

        result = _write_combined_forecast_to_api(data, "pentad")
        assert result is True

        call_args = mock_client.write_forecasts.call_args[0][0]
        record = call_args[0]
        # Horizon values should be computed from the date
        assert record["horizon_value"] is not None
        assert record["horizon_in_year"] is not None

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_rows_with_invalid_date_and_missing_horizon_are_skipped(self, mock_client_class):
        """Test that rows with NaT date AND missing horizon are skipped."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client_class.return_value = mock_client

        # Row with NaT date and NaN horizon — cannot be repaired
        data = pd.DataFrame(
            {
                "code": [12345],
                "date": [pd.NaT],
                "pentad_in_month": [np.nan],
                "pentad_in_year": [np.nan],
                "forecasted_discharge": [100.0],
                "model_short": ["TFT"],
            }
        )

        result = _write_combined_forecast_to_api(data, "pentad")
        assert result is False

        mock_client.write_forecasts.assert_not_called()

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_nan_discharge_rows_are_dropped(self, mock_client_class):
        """Test that rows with NaN forecasted_discharge are dropped, not written."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client_class.return_value = mock_client

        # All rows have NaN discharge — nothing should be written
        data = pd.DataFrame(
            {
                "code": [12345],
                "date": pd.to_datetime(["2024-01-06"]),
                "pentad_in_month": [2],
                "pentad_in_year": [2],
                "forecasted_discharge": [np.nan],
                "model_short": ["TFT"],
            }
        )

        result = _write_combined_forecast_to_api(data, "pentad")
        assert result is False
        mock_client.write_forecasts.assert_not_called()

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_empty_data_returns_false(self, mock_client_class):
        """Test that empty data returns False without calling API."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            columns=[
                "code",
                "date",
                "pentad_in_month",
                "pentad_in_year",
                "forecasted_discharge",
                "model_short",
            ]
        )

        result = _write_combined_forecast_to_api(data, "pentad")
        assert result is False

        # write_forecasts should not be called for empty data
        mock_client.write_forecasts.assert_not_called()


class TestCombinedForecastTarget:
    """Tests that target = date + 1 (first day of forecast period)."""

    @pytest.fixture(autouse=True)
    def _set_api_env(self, monkeypatch):
        """Enable API by default."""
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")

    def _get_record(self, mock_client_class, data, horizon_type):
        """Helper: write data and return the first record sent to API."""
        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_forecasts.return_value = 1
        mock_client_class.return_value = mock_client

        _write_combined_forecast_to_api(data, horizon_type)

        call_args = mock_client.write_forecasts.call_args[0][0]
        return call_args[0]

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_pentad_target_is_day_after_boundary(self, mock_client_class):
        """date=2024-01-20 (end of pentad 4) -> target=2024-01-21."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        data = pd.DataFrame(
            {
                "code": [12345],
                "date": pd.to_datetime(["2024-01-20"]),
                "pentad_in_month": [5],
                "pentad_in_year": [5],
                "forecasted_discharge": [100.0],
                "model_short": ["TFT"],
            }
        )

        record = self._get_record(mock_client_class, data, "pentad")

        assert record["date"] == "2024-01-20"
        assert record["target"] == "2024-01-21"

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_decade_target_is_day_after_boundary(self, mock_client_class):
        """date=2024-01-20 (end of decade 2) -> target=2024-01-21."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        data = pd.DataFrame(
            {
                "code": [12345],
                "date": pd.to_datetime(["2024-01-20"]),
                "decad": [2],
                "decad_in_year": [2],
                "forecasted_discharge": [100.0],
                "model_short": ["TFT"],
            }
        )

        record = self._get_record(mock_client_class, data, "decad")

        assert record["date"] == "2024-01-20"
        assert record["target"] == "2024-01-21"

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_pentad_target_crosses_month_boundary(self, mock_client_class):
        """date=2024-02-29 (end of Feb, leap year) -> target=2024-03-01."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        data = pd.DataFrame(
            {
                "code": [12345],
                "date": pd.to_datetime(["2024-02-29"]),
                "pentad_in_month": [6],
                "pentad_in_year": [12],
                "forecasted_discharge": [100.0],
                "model_short": ["TFT"],
            }
        )

        record = self._get_record(mock_client_class, data, "pentad")

        assert record["date"] == "2024-02-29"
        assert record["target"] == "2024-03-01"

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_decade_target_crosses_month_boundary(self, mock_client_class):
        """date=2024-01-31 (end of Jan) -> target=2024-02-01."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        data = pd.DataFrame(
            {
                "code": [12345],
                "date": pd.to_datetime(["2024-01-31"]),
                "decad": [3],
                "decad_in_year": [3],
                "forecasted_discharge": [100.0],
                "model_short": ["TFT"],
            }
        )

        record = self._get_record(mock_client_class, data, "decad")

        assert record["date"] == "2024-01-31"
        assert record["target"] == "2024-02-01"


class TestWriteSkillMetricsToApi:
    """Tests for the _write_skill_metrics_to_api function.

    This function writes skill metrics to the SAPPHIRE postprocessing API.
    """

    @pytest.fixture(autouse=True)
    def _set_api_env(self, monkeypatch):
        """Enable API by default; individual tests can override."""
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")

    def test_api_disabled_via_env_var(self, monkeypatch):
        """When SAPPHIRE_API_ENABLED=false, API write should be skipped."""
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "false")
        data = pd.DataFrame(
            {
                "code": [12345],
                "pentad_in_year": [1],
                "model_short": ["LR"],
                "sdivsigma": [0.5],
                "nse": [0.8],
                "delta": [0.1],
                "accuracy": [0.9],
                "mae": [5.0],
                "n_pairs": [100],
            }
        )
        result = _write_skill_metrics_to_api(data, "pentad", 2024)
        assert result is False

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_api_not_ready_returns_false(self, mock_client_class):
        """When API health check fails, should return False (non-blocking)."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = False
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": [12345],
                "pentad_in_year": [1],
                "model_short": ["LR"],
                "sdivsigma": [0.5],
                "nse": [0.8],
                "delta": [0.1],
                "accuracy": [0.9],
                "mae": [5.0],
                "n_pairs": [100],
            }
        )

        result = _write_skill_metrics_to_api(data, "pentad", 2024)
        assert result is False

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_pentad_skill_metrics_correct_fields(self, mock_client_class):
        """Test that pentadal skill metric records have correct field mapping."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 1
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": [12345],
                "pentad_in_year": [5],
                "model_short": ["LR"],
                "sdivsigma": [0.5],
                "nse": [0.85],
                "delta": [0.12],
                "accuracy": [0.92],
                "mae": [4.5],
                "n_pairs": [150],
            }
        )

        result = _write_skill_metrics_to_api(data, "pentad", 2024)
        assert result is True

        # Check that write_skill_metrics was called
        mock_client.write_skill_metrics.assert_called_once()

        # Get the records that were passed
        call_args = mock_client.write_skill_metrics.call_args[0][0]
        assert len(call_args) == 1
        record = call_args[0]

        # Check field mapping
        assert record["horizon_type"] == "pentad"
        assert record["code"] == "12345"
        assert record["model_type"] == "LR"
        assert record["date"] == "2024-01-21"  # pentad 5, year 2024
        assert record["horizon_in_year"] == 5
        assert record["sdivsigma"] == 0.5
        assert record["nse"] == 0.85
        assert record["delta"] == 0.12
        assert record["accuracy"] == 0.92
        assert record["mae"] == 4.5
        assert record["n_pairs"] == 150

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_decade_skill_metrics_correct_fields(self, mock_client_class):
        """Test that decadal skill metric records have correct field mapping."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 1
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": [12345],
                "decad_in_year": [10],
                "model_short": ["TFT"],
                "sdivsigma": [0.6],
                "nse": [0.75],
                "delta": [0.15],
                "accuracy": [0.88],
                "mae": [6.2],
                "n_pairs": [120],
            }
        )

        result = _write_skill_metrics_to_api(data, "decad", 2024)
        assert result is True

        # Get the records that were passed
        call_args = mock_client.write_skill_metrics.call_args[0][0]
        record = call_args[0]

        # Check field mapping — "decad" translates to "decade" at boundary
        assert record["horizon_type"] == "decade"
        assert record["code"] == "12345"
        assert record["model_type"] == "TFT"
        assert record["date"] == "2024-04-01"  # decad 10, year 2024
        assert record["horizon_in_year"] == 10

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_model_type_mapping(self, mock_client_class):
        """Test that model types are correctly mapped to API format."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 6
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": [12345] * 6,
                "pentad_in_year": [1] * 6,
                "model_short": ["LR", "TFT", "TIDE", "TSMIXER", "EM", "NE"],
                "sdivsigma": [0.5] * 6,
                "nse": [0.8] * 6,
                "delta": [0.1] * 6,
                "accuracy": [0.9] * 6,
                "mae": [5.0] * 6,
                "n_pairs": [100] * 6,
            }
        )

        _write_skill_metrics_to_api(data, "pentad", 2024)

        call_args = mock_client.write_skill_metrics.call_args[0][0]

        # Check model type mappings
        model_types = [r["model_type"] for r in call_args]
        assert "LR" in model_types
        assert "TFT" in model_types
        assert "TiDE" in model_types  # TIDE -> TiDE
        assert "TSMixer" in model_types  # TSMIXER -> TSMixer
        assert "EM" in model_types
        assert "NE" in model_types

        # All records should have pentad 1 date
        for record in call_args:
            assert record["date"] == "2024-01-01"

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_nan_values_converted_to_none(self, mock_client_class):
        """Test that NaN values are converted to None."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 1
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": [12345],
                "pentad_in_year": [1],
                "model_short": ["LR"],
                "sdivsigma": [np.nan],
                "nse": [np.nan],
                "delta": [np.nan],
                "accuracy": [np.nan],
                "mae": [np.nan],
                "n_pairs": [np.nan],
            }
        )

        _write_skill_metrics_to_api(data, "pentad", 2024)

        call_args = mock_client.write_skill_metrics.call_args[0][0]
        record = call_args[0]

        # NaN should be converted to None
        assert record["sdivsigma"] is None
        assert record["nse"] is None
        assert record["delta"] is None
        assert record["accuracy"] is None
        assert record["mae"] is None
        assert record["n_pairs"] is None

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_empty_data_returns_false(self, mock_client_class):
        """Test that empty data returns False without calling API."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            columns=[
                "code",
                "pentad_in_year",
                "model_short",
                "sdivsigma",
                "nse",
                "delta",
                "accuracy",
                "mae",
                "n_pairs",
            ]
        )

        result = _write_skill_metrics_to_api(data, "pentad", 2024)
        assert result is False

        # write_skill_metrics should not be called for empty data
        mock_client.write_skill_metrics.assert_not_called()

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_composition_passed_through_for_ensembles(self, mock_client_class):
        """Test that composition column is passed through for ensemble models."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 3
        mock_client_class.return_value = mock_client

        # Test data with composition column (model_long is no longer used)
        data = pd.DataFrame(
            {
                "code": [12345, 12345, 12345],
                "pentad_in_year": [1, 1, 1],
                "model_short": ["EM", "NE", "TFT"],
                "composition": [
                    "TFT, TiDE, TSMixer",
                    "LR, TFT",
                    None,
                ],
                "sdivsigma": [0.1, 0.2, 0.3],
                "nse": [0.9, 0.8, 0.7],
                "delta": [0.1, 0.2, 0.3],
                "accuracy": [0.9, 0.8, 0.7],
                "mae": [5.0, 6.0, 7.0],
                "n_pairs": [100, 100, 100],
            }
        )

        _write_skill_metrics_to_api(data, "pentad", 2024)

        call_args = mock_client.write_skill_metrics.call_args[0][0]

        # EM should have composition passed through
        em_record = next(r for r in call_args if r["model_type"] == "EM")
        assert em_record["composition"] == "TFT, TiDE, TSMixer"

        # NE should have composition passed through
        ne_record = next(r for r in call_args if r["model_type"] == "NE")
        assert ne_record["composition"] == "LR, TFT"

        # Non-ensemble (TFT) should have no composition
        tft_record = next(r for r in call_args if r["model_type"] == "TFT")
        assert tft_record["composition"] is None

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_skill_metrics_api_exception_propagates(self, mock_client_class):
        """API client raises RuntimeError -> exception propagates.

        Unlike the combined forecast writer, _write_skill_metrics_to_api
        does NOT catch exceptions from client.write_skill_metrics(). The
        caller is responsible for handling the failure mode via
        SAPPHIRE_API_FAILURE_MODE.
        """
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.side_effect = RuntimeError("API connection failed")
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": [12345],
                "pentad_in_year": [1],
                "model_short": ["LR"],
                "sdivsigma": [0.5],
                "nse": [0.8],
                "delta": [0.1],
                "accuracy": [0.9],
                "mae": [5.0],
                "n_pairs": [100],
            }
        )

        with pytest.raises(RuntimeError, match="API connection failed"):
            _write_skill_metrics_to_api(data, "pentad", 2024)

    def test_invalid_horizon_type_raises_value_error(self):
        """Invalid horizon_type raises ValueError with descriptive message."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        data = pd.DataFrame(
            {
                "code": [12345],
                "pentad_in_year": [1],
                "model_short": ["LR"],
                "sdivsigma": [0.5],
                "nse": [0.8],
                "delta": [0.1],
                "accuracy": [0.9],
                "mae": [5.0],
                "n_pairs": [100],
            }
        )

        with pytest.raises(ValueError, match="Invalid horizon_type"):
            _write_skill_metrics_to_api(data, "weekly", 2024)

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_skill_metrics_empty_data_returns_false(self, mock_client_class):
        """Empty DataFrame returns False, API client never called."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            columns=[
                "code",
                "pentad_in_year",
                "model_short",
                "sdivsigma",
                "nse",
                "delta",
                "accuracy",
                "mae",
                "n_pairs",
            ]
        )

        result = _write_skill_metrics_to_api(data, "pentad", 2024)
        assert result is False
        mock_client.write_skill_metrics.assert_not_called()

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_multi_pentad_rows_get_different_dates(self, mock_client_class):
        """Rows with different pentad_in_year values get different dates."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 3
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": ["15001", "15001", "15001"],
                "pentad_in_year": [1, 5, 72],
                "model_short": ["LR", "LR", "LR"],
                "sdivsigma": [0.5, 0.5, 0.5],
                "nse": [0.8, 0.8, 0.8],
                "delta": [5.0, 5.0, 5.0],
                "accuracy": [0.9, 0.9, 0.9],
                "mae": [3.0, 3.0, 3.0],
                "n_pairs": [50, 50, 50],
            }
        )

        _write_skill_metrics_to_api(data, "pentad", 2024)

        records = mock_client.write_skill_metrics.call_args[0][0]
        dates = [r["date"] for r in records]
        assert dates[0] == "2024-01-01"  # pentad 1
        assert dates[1] == "2024-01-21"  # pentad 5
        assert dates[2] == "2024-12-26"  # pentad 72
        assert len(set(dates)) == 3  # all different

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_multi_decad_rows_get_different_dates(self, mock_client_class):
        """Rows with different decad_in_year values get different dates."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 2
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": ["15001", "15001"],
                "decad_in_year": [1, 36],
                "model_short": ["LR", "LR"],
                "sdivsigma": [0.5, 0.5],
                "nse": [0.8, 0.8],
                "delta": [5.0, 5.0],
                "accuracy": [0.9, 0.9],
                "mae": [3.0, 3.0],
                "n_pairs": [50, 50],
            }
        )

        _write_skill_metrics_to_api(data, "decad", 2024)

        records = mock_client.write_skill_metrics.call_args[0][0]
        assert records[0]["date"] == "2024-01-01"  # decad 1
        assert records[1]["date"] == "2024-12-21"  # decad 36

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_multi_month_rows_get_different_dates(self, mock_client_class):
        """Rows with different month_in_year values get different dates."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 12
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": ["15001"] * 12,
                "month_in_year": list(range(1, 13)),
                "model_short": ["GBT"] * 12,
                "sdivsigma": [0.5] * 12,
                "nse": [0.8] * 12,
                "delta": [5.0] * 12,
                "accuracy": [0.9] * 12,
                "mae": [3.0] * 12,
                "n_pairs": [50] * 12,
            }
        )

        _write_skill_metrics_to_api(data, "month", 2024)

        records = mock_client.write_skill_metrics.call_args[0][0]
        assert len(records) == 12
        for i, rec in enumerate(records):
            expected = f"2024-{i + 1:02d}-01"
            assert rec["date"] == expected, f"month {i + 1}: {rec['date']} != {expected}"


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
                f"MODEL_TYPE_MAP['{key}'] = '{MODEL_TYPE_MAP[key]}', expected '{expected_value}'"
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

    @pytest.fixture(autouse=True)
    def _set_api_env(self, monkeypatch):
        """Enable API by default; individual tests can override."""
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_monthly_skill_metrics_correct_fields(self, mock_client_class):
        """Monthly skill metrics produce records with horizon_type='month'."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 1
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": ["15013"],
                "month_in_year": [6],
                "model_short": ["GBT"],
                "sdivsigma": [0.45],
                "nse": [0.82],
                "delta": [12.5],
                "accuracy": [0.75],
                "mae": [8.3],
                "n_pairs": [10],
                "crps": [15.2],
            }
        )

        result = _write_skill_metrics_to_api(data, "month", 2024)
        assert result is True

        mock_client.write_skill_metrics.assert_called_once()
        call_args = mock_client.write_skill_metrics.call_args[0][0]
        assert len(call_args) == 1
        record = call_args[0]

        assert record["horizon_type"] == "month"
        assert record["code"] == "15013"
        assert record["model_type"] == "GBT"
        assert record["date"] == "2024-06-01"  # month 6, year 2024
        assert record["horizon_in_year"] == 6
        assert record["sdivsigma"] == 0.45
        assert record["nse"] == 0.82
        assert record["mae"] == 8.3
        assert record["n_pairs"] == 10

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_crps_sent_to_api(self, mock_client_class):
        """CRPS column is included in API records."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 1
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": ["15013"],
                "month_in_year": [3],
                "model_short": ["GBT"],
                "sdivsigma": [0.5],
                "nse": [0.8],
                "delta": [10.0],
                "accuracy": [0.85],
                "mae": [6.0],
                "n_pairs": [8],
                "crps": [12.0],
            }
        )

        _write_skill_metrics_to_api(data, "month", 2024)

        call_args = mock_client.write_skill_metrics.call_args[0][0]
        record = call_args[0]
        assert record["crps"] == 12.0
        assert record["date"] == "2024-03-01"  # month 3, year 2024

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_lt_model_type_mapping(self, mock_client_class):
        """Long-term model types are mapped correctly to API format."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 5
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": ["15013"] * 5,
                "month_in_year": [6] * 5,
                "model_short": [
                    "GBT",
                    "LR_Base",
                    "SM_GBT",
                    "MC_ALD",
                    "SM_GBT_Norm",
                ],
                "sdivsigma": [0.5] * 5,
                "nse": [0.8] * 5,
                "delta": [10.0] * 5,
                "accuracy": [0.85] * 5,
                "mae": [6.0] * 5,
                "n_pairs": [10] * 5,
            }
        )

        _write_skill_metrics_to_api(data, "month", 2024)

        call_args = mock_client.write_skill_metrics.call_args[0][0]
        model_types = {r["model_type"] for r in call_args}

        assert "GBT" in model_types
        assert "LR_Base" in model_types
        assert "SM_GBT" in model_types
        assert "MC_ALD" in model_types
        assert "SM_GBT_Norm" in model_types

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_naive_mean_model_type_mapping(self, mock_client_class):
        """Naive Mean model maps correctly despite space in name."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 1
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": ["15013"],
                "month_in_year": [6],
                "model_short": ["Naive Mean"],
                "sdivsigma": [0.7],
                "nse": [0.3],
                "delta": [10.0],
                "accuracy": [0.5],
                "mae": [15.0],
                "n_pairs": [10],
            }
        )

        _write_skill_metrics_to_api(data, "month", 2024)

        call_args = mock_client.write_skill_metrics.call_args[0][0]
        record = call_args[0]
        assert record["model_type"] == "Naive Mean"

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_em_ensemble_with_composition(self, mock_client_class):
        """Monthly EM ensemble passes composition to API."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 2
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": ["15013", "15013"],
                "month_in_year": [6, 6],
                "model_short": ["GBT", "EM"],
                "composition": [None, "GBT, LR_Base, SM_GBT"],
                "sdivsigma": [0.5, 0.4],
                "nse": [0.8, 0.85],
                "delta": [10.0, 9.0],
                "accuracy": [0.85, 0.90],
                "mae": [6.0, 5.5],
                "n_pairs": [10, 10],
            }
        )

        _write_skill_metrics_to_api(data, "month", 2024)

        call_args = mock_client.write_skill_metrics.call_args[0][0]

        em_rec = next(r for r in call_args if r["model_type"] == "EM")
        assert em_rec["composition"] == "GBT, LR_Base, SM_GBT"
        assert em_rec["date"] == "2024-06-01"

        gbt_rec = next(r for r in call_args if r["model_type"] == "GBT")
        assert gbt_rec["composition"] is None
        assert gbt_rec["date"] == "2024-06-01"

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_monthly_empty_data_returns_false(self, mock_client_class):
        """Empty monthly DataFrame returns False."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            columns=[
                "code",
                "month_in_year",
                "model_short",
                "sdivsigma",
                "nse",
                "delta",
                "accuracy",
                "mae",
                "n_pairs",
            ]
        )

        result = _write_skill_metrics_to_api(data, "month", 2024)
        assert result is False
        mock_client.write_skill_metrics.assert_not_called()

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_monthly_nan_metrics_converted_to_none(self, mock_client_class):
        """NaN metric values in monthly data are converted to None for API."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 1
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": ["15013"],
                "month_in_year": [1],
                "model_short": ["GBT"],
                "sdivsigma": [np.nan],
                "nse": [np.nan],
                "delta": [np.nan],
                "accuracy": [np.nan],
                "mae": [np.nan],
                "n_pairs": [np.nan],
                "crps": [np.nan],
            }
        )

        _write_skill_metrics_to_api(data, "month", 2024)

        call_args = mock_client.write_skill_metrics.call_args[0][0]
        record = call_args[0]

        assert record["sdivsigma"] is None
        assert record["nse"] is None
        assert record["mae"] is None
        assert record["n_pairs"] is None
        assert record["crps"] is None


class TestHorizonTypeToApiMapping:
    """Tests for HORIZON_TYPE_TO_API translation layer."""

    @pytest.fixture(autouse=True)
    def _set_api_env(self, monkeypatch):
        """Enable API by default; individual tests can override."""
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")

    def test_horizon_type_to_api_mapping(self):
        """Constant maps internal names to API enum values."""
        from src.api_writer import HORIZON_TYPE_TO_API

        assert HORIZON_TYPE_TO_API == {
            "pentad": "pentad",
            "decad": "decade",
            "month": "month",
            "day": "day",
            "quarter": "quarter",
            "season": "season",
        }

    def test_old_decade_string_raises_combined_forecast(self):
        """Passing 'decade' (old API name) to combined forecast raises."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        data = pd.DataFrame(
            {
                "code": [12345],
                "date": pd.to_datetime(["2024-01-15"]),
                "decad": [2],
                "decad_in_year": [2],
                "forecasted_discharge": [150.0],
                "model_short": ["TFT"],
            }
        )
        with pytest.raises(ValueError, match="Invalid horizon_type"):
            _write_combined_forecast_to_api(data, "decade")

    def test_old_decade_string_raises_skill_metrics(self):
        """Passing 'decade' (old API name) to skill metrics raises."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        data = pd.DataFrame(
            {
                "code": [12345],
                "decad_in_year": [10],
                "model_short": ["TFT"],
                "sdivsigma": [0.6],
                "nse": [0.75],
                "delta": [0.15],
                "accuracy": [0.88],
                "mae": [6.2],
                "n_pairs": [120],
            }
        )
        with pytest.raises(ValueError, match="Invalid horizon_type"):
            _write_skill_metrics_to_api(data, "decade", 2024)

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_decad_translates_to_decade_in_api_records(self, mock_client_class):
        """Internal 'decad' becomes 'decade' in the API record."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_forecasts.return_value = 1
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": [12345],
                "date": pd.to_datetime(["2024-01-15"]),
                "decad": [2],
                "decad_in_year": [2],
                "forecasted_discharge": [150.0],
                "model_short": ["TFT"],
            }
        )

        _write_combined_forecast_to_api(data, "decad")
        record = mock_client.write_forecasts.call_args[0][0][0]
        assert record["horizon_type"] == "decade"


class TestWriteMonthlyEnsembleToApi:
    """Tests for _write_monthly_ensemble_to_api().

    Writes ensemble forecast rows (EM, Naive Mean, Skilled Mean) to the
    long_forecasts table via client.write_long_forecasts().
    """

    @pytest.fixture(autouse=True)
    def _set_api_env(self, monkeypatch):
        """Enable API by default; individual tests can override."""
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")

    @pytest.fixture
    def ensemble_data(self):
        """Monthly joint forecasts with ensemble + regular model rows."""
        return pd.DataFrame(
            {
                "code": ["15013", "15013", "15013", "15013"],
                "year": [2024, 2024, 2024, 2024],
                "month": [6, 6, 6, 6],
                "month_in_year": [6, 6, 6, 6],
                "forecasted_discharge": [100.0, 102.5, 101.0, 103.0],
                "model_short": ["GBT", "EM", "Naive Mean", "Skilled Mean"],
                "composition": ["", "GBT, LR_Base", "GBT, LR_Base", "GBT, LR_Base"],
                "q05": [70.0, 72.5, 71.0, 73.0],
                "q10": [75.0, 77.5, 76.0, 78.0],
                "q25": [85.0, 87.5, 86.0, 88.0],
                "q50": [100.0, 102.5, 101.0, 103.0],
                "q75": [115.0, 117.5, 116.0, 118.0],
                "q90": [125.0, 127.5, 126.0, 128.0],
                "q95": [130.0, 132.5, 131.0, 133.0],
                "valid_from": ["2024-06-01"] * 4,
                "valid_to": ["2024-06-30"] * 4,
            }
        )

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_filters_to_ensemble_rows_only(self, mock_client_class, ensemble_data):
        """Only EM/Naive Mean/Skilled Mean rows sent to API, not GBT."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_long_forecasts.return_value = 3
        mock_client_class.return_value = mock_client

        from src.api_writer import _write_monthly_ensemble_to_api

        result = _write_monthly_ensemble_to_api(ensemble_data)
        assert result is True

        records = mock_client.write_long_forecasts.call_args[0][0]
        assert len(records) == 3
        model_types = {r["model_type"] for r in records}
        assert "GBT" not in model_types
        assert "EM" in model_types
        assert "Naive Mean" in model_types
        assert "Skilled Mean" in model_types

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_ensemble_record_format(self, mock_client_class, ensemble_data):
        """Verify record has all required LongForecast fields."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_long_forecasts.return_value = 3
        mock_client_class.return_value = mock_client

        from src.api_writer import _write_monthly_ensemble_to_api

        _write_monthly_ensemble_to_api(ensemble_data)

        records = mock_client.write_long_forecasts.call_args[0][0]
        em_rec = next(r for r in records if r["model_type"] == "EM")
        assert em_rec["horizon_type"] == "month"
        assert em_rec["horizon_value"] == 6
        assert em_rec["code"] == "15013"
        assert em_rec["date"] == "2024-06-01"
        assert em_rec["valid_from"] == "2024-06-01"
        assert em_rec["valid_to"] == "2024-06-30"
        assert em_rec["flag"] == 0
        assert em_rec["q"] == 102.5

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_quantile_values_preserved(self, mock_client_class, ensemble_data):
        """Input quantile values are exactly preserved in records."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_long_forecasts.return_value = 3
        mock_client_class.return_value = mock_client

        from src.api_writer import _write_monthly_ensemble_to_api

        _write_monthly_ensemble_to_api(ensemble_data)

        records = mock_client.write_long_forecasts.call_args[0][0]
        em_rec = next(r for r in records if r["model_type"] == "EM")
        assert em_rec["q05"] == 72.5
        assert em_rec["q10"] == 77.5
        assert em_rec["q25"] == 87.5
        assert em_rec["q50"] == 102.5
        assert em_rec["q75"] == 117.5
        assert em_rec["q90"] == 127.5
        assert em_rec["q95"] == 132.5

    def test_returns_false_when_disabled(self, monkeypatch, ensemble_data):
        """SAPPHIRE_API_ENABLED=false -> returns False."""
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "false")
        from src.api_writer import _write_monthly_ensemble_to_api

        result = _write_monthly_ensemble_to_api(ensemble_data)
        assert result is False

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_returns_false_when_not_ready(self, mock_client_class, ensemble_data):
        """readiness_check=False -> returns False."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = False
        mock_client_class.return_value = mock_client

        from src.api_writer import _write_monthly_ensemble_to_api

        result = _write_monthly_ensemble_to_api(ensemble_data)
        assert result is False

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_returns_false_on_exception(self, mock_client_class, ensemble_data):
        """API raises -> returns False, no crash."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_long_forecasts.side_effect = RuntimeError("Connection refused")
        mock_client_class.return_value = mock_client

        from src.api_writer import _write_monthly_ensemble_to_api

        result = _write_monthly_ensemble_to_api(ensemble_data)
        assert result is False

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_synthesizes_valid_from_valid_to(self, mock_client_class):
        """valid_from/valid_to synthesized from year+month if missing."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_long_forecasts.return_value = 1
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": ["15013"],
                "year": [2024],
                "month": [2],
                "month_in_year": [2],
                "forecasted_discharge": [100.0],
                "model_short": ["EM"],
            }
        )

        from src.api_writer import _write_monthly_ensemble_to_api

        _write_monthly_ensemble_to_api(data)

        records = mock_client.write_long_forecasts.call_args[0][0]
        record = records[0]
        assert record["valid_from"] == "2024-02-01"
        assert record["valid_to"] == "2024-02-29"  # 2024 is leap year

    def test_empty_data_returns_false(self):
        """Empty DataFrame returns False."""
        from src.api_writer import _write_monthly_ensemble_to_api

        result = _write_monthly_ensemble_to_api(pd.DataFrame())
        assert result is False

    def test_none_data_returns_false(self):
        """None input returns False."""
        from src.api_writer import _write_monthly_ensemble_to_api

        result = _write_monthly_ensemble_to_api(None)
        assert result is False
