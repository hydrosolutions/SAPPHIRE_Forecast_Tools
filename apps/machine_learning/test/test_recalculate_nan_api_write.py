"""Tests for recalculate_nan_forecasts.py bug fixes (ML-004).

Covers:
1. Hindcast call wrapped in try/except — FileNotFoundError caught
2. API write succeeds — success logged
3. API write returns False — warning logged
4. SAPPHIRE_API_AVAILABLE is False — CSV-only warning logged
"""

import logging
import os
import sys
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

# Mock heavy dependencies before importing the module under test
sys.modules["darts"] = MagicMock()
sys.modules["darts.TimeSeries"] = MagicMock()
sys.modules["darts.concatenate"] = MagicMock()
sys.modules["darts.utils"] = MagicMock()
sys.modules["darts.utils.timeseries_generation"] = MagicMock()
sys.modules["darts.models"] = MagicMock()
sys.modules["pytorch_lightning"] = MagicMock()
sys.modules["pytorch_lightning.callbacks"] = MagicMock()
sys.modules["torch"] = MagicMock()
sys.modules["pe_oudin"] = MagicMock()
sys.modules["pe_oudin.PE_Oudin"] = MagicMock()
sys.modules["suntime"] = MagicMock()
sys.modules["setup_library"] = MagicMock()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scr"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

import recalculate_nan_forecasts

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _make_forecast_df_with_nans():
    """Build a forecast DataFrame with some NaN-flagged rows.

    Returns a DataFrame with two codes: one has flag=1 rows (needs
    recalculation), the other is clean (flag=0).
    """
    dates = pd.to_datetime(["2024-06-01", "2024-06-06", "2024-06-11"])
    forecast_dates = pd.to_datetime(["2024-05-31", "2024-06-05", "2024-06-10"])
    return pd.DataFrame(
        {
            "code": [12345, 12345, 12345],
            "date": dates,
            "forecast_date": forecast_dates,
            "flag": [0, 1, 1],
            "Q5": [10.0, np.nan, np.nan],
            "Q25": [20.0, np.nan, np.nan],
            "Q50": [30.0, np.nan, np.nan],
            "Q75": [40.0, np.nan, np.nan],
            "Q95": [50.0, np.nan, np.nan],
        }
    )


def _make_hindcast_df():
    """Build a hindcast DataFrame that replaces the NaN rows."""
    dates = pd.to_datetime(["2024-06-06", "2024-06-11"])
    forecast_dates = pd.to_datetime(["2024-06-05", "2024-06-10"])
    return pd.DataFrame(
        {
            "code": [12345, 12345],
            "date": dates,
            "forecast_date": forecast_dates,
            "flag": [4, 4],
            "Q5": [11.0, 12.0],
            "Q25": [21.0, 22.0],
            "Q50": [31.0, 32.0],
            "Q75": [41.0, 42.0],
            "Q95": [51.0, 52.0],
        }
    )


# Common env-var dict used by all tests to satisfy early validation
_BASE_ENV = {
    "SAPPHIRE_MODEL_TO_USE": "TFT",
    "SAPPHIRE_PREDICTION_MODE": "PENTAD",
    "ieasyforecast_intermediate_data_path": "/tmp/test_intermediate",
    "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestHindcastCallFailure:
    """Phase 2a: call_hindcast_script wrapped in try/except."""

    @patch.dict(os.environ, _BASE_ENV, clear=False)
    @patch("recalculate_nan_forecasts._write_ml_forecast_to_api")
    @patch("recalculate_nan_forecasts.call_hindcast_script")
    @patch("recalculate_nan_forecasts._read_ml_forecasts_from_api")
    @patch("recalculate_nan_forecasts.sl")
    def test_file_not_found_caught_and_logged(
        self,
        mock_sl,
        mock_read_api,
        mock_call_hindcast,
        mock_write_api,
        caplog,
    ):
        """When call_hindcast_script raises FileNotFoundError the
        function logs the error and returns without crashing.
        """
        # Arrange
        mock_sl.load_environment.return_value = None
        mock_read_api.return_value = _make_forecast_df_with_nans()
        mock_call_hindcast.side_effect = FileNotFoundError("hindcast CSV not found")

        # Act
        with caplog.at_level(logging.ERROR, logger="recalculate_nan_forecasts"):
            recalculate_nan_forecasts.recalculate_nan_forecasts()

        # Assert
        assert mock_call_hindcast.called
        assert any("Hindcast call failed" in msg for msg in caplog.messages), (
            f"Expected 'Hindcast call failed' in log, got: {caplog.messages}"
        )
        # API write must NOT have been attempted
        mock_write_api.assert_not_called()

    @patch.dict(os.environ, _BASE_ENV, clear=False)
    @patch("recalculate_nan_forecasts._write_ml_forecast_to_api")
    @patch("recalculate_nan_forecasts.call_hindcast_script")
    @patch("recalculate_nan_forecasts._read_ml_forecasts_from_api")
    @patch("recalculate_nan_forecasts.sl")
    def test_runtime_error_caught_and_logged(
        self,
        mock_sl,
        mock_read_api,
        mock_call_hindcast,
        mock_write_api,
        caplog,
    ):
        """When call_hindcast_script raises RuntimeError the function
        logs the error and returns without crashing.
        """
        mock_sl.load_environment.return_value = None
        mock_read_api.return_value = _make_forecast_df_with_nans()
        mock_call_hindcast.side_effect = RuntimeError("subprocess failed")

        with caplog.at_level(logging.ERROR, logger="recalculate_nan_forecasts"):
            recalculate_nan_forecasts.recalculate_nan_forecasts()

        assert any("Hindcast call failed" in msg for msg in caplog.messages), (
            f"Expected 'Hindcast call failed' in log, got: {caplog.messages}"
        )
        mock_write_api.assert_not_called()


class TestApiWriteSuccess:
    """Phase 2b: API write is primary — success path."""

    @patch.dict(os.environ, _BASE_ENV, clear=False)
    @patch("recalculate_nan_forecasts.SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_nan_forecasts._write_ml_forecast_to_api")
    @patch("recalculate_nan_forecasts.call_hindcast_script")
    @patch("recalculate_nan_forecasts._read_ml_forecasts_from_api")
    @patch("recalculate_nan_forecasts.sl")
    def test_api_write_success_logged(
        self,
        mock_sl,
        mock_read_api,
        mock_call_hindcast,
        mock_write_api,
        caplog,
        tmp_path,
    ):
        """When _write_ml_forecast_to_api returns True, the success
        message is logged.
        """
        # Arrange
        mock_sl.load_environment.return_value = None
        mock_read_api.return_value = _make_forecast_df_with_nans()
        mock_call_hindcast.return_value = _make_hindcast_df()
        mock_write_api.return_value = True

        # Point CSV output to tmp_path so to_csv does not fail
        forecast_dir = tmp_path / "output" / "TFT"
        forecast_dir.mkdir(parents=True)
        env_override = {
            **_BASE_ENV,
            "ieasyforecast_intermediate_data_path": str(tmp_path),
        }

        # Act
        with patch.dict(os.environ, env_override, clear=False):
            with caplog.at_level(logging.INFO, logger="recalculate_nan_forecasts"):
                recalculate_nan_forecasts.recalculate_nan_forecasts()

        # Assert
        assert mock_write_api.called
        assert any(
            "Wrote" in msg and "recalculated forecasts to API" in msg for msg in caplog.messages
        ), f"Expected API success log, got: {caplog.messages}"


class TestApiWriteReturnsFailure:
    """Phase 2b: API write returns False — warning path."""

    @patch.dict(os.environ, _BASE_ENV, clear=False)
    @patch("recalculate_nan_forecasts.SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_nan_forecasts._write_ml_forecast_to_api")
    @patch("recalculate_nan_forecasts.call_hindcast_script")
    @patch("recalculate_nan_forecasts._read_ml_forecasts_from_api")
    @patch("recalculate_nan_forecasts.sl")
    def test_api_write_failure_warning_logged(
        self,
        mock_sl,
        mock_read_api,
        mock_call_hindcast,
        mock_write_api,
        caplog,
        tmp_path,
    ):
        """When _write_ml_forecast_to_api returns False, a warning is
        logged about the failure and a second warning about CSV-only
        persistence.
        """
        # Arrange
        mock_sl.load_environment.return_value = None
        mock_read_api.return_value = _make_forecast_df_with_nans()
        mock_call_hindcast.return_value = _make_hindcast_df()
        mock_write_api.return_value = False

        forecast_dir = tmp_path / "output" / "TFT"
        forecast_dir.mkdir(parents=True)
        env_override = {
            **_BASE_ENV,
            "ieasyforecast_intermediate_data_path": str(tmp_path),
        }

        # Act
        with patch.dict(os.environ, env_override, clear=False):
            with caplog.at_level(logging.WARNING, logger="recalculate_nan_forecasts"):
                recalculate_nan_forecasts.recalculate_nan_forecasts()

        # Assert — two warnings expected
        assert any("API write returned failure" in msg for msg in caplog.messages), (
            f"Expected 'API write returned failure' in log, got: {caplog.messages}"
        )
        assert any("API write unsuccessful" in msg for msg in caplog.messages), (
            f"Expected 'API write unsuccessful' in log, got: {caplog.messages}"
        )


class TestApiUnavailableFallback:
    """Phase 2b: SAPPHIRE_API_AVAILABLE is False — CSV-only warning."""

    @patch.dict(os.environ, _BASE_ENV, clear=False)
    @patch("recalculate_nan_forecasts.SAPPHIRE_API_AVAILABLE", False)
    @patch("recalculate_nan_forecasts._write_ml_forecast_to_api")
    @patch("recalculate_nan_forecasts.call_hindcast_script")
    @patch("recalculate_nan_forecasts._read_ml_forecasts_from_api")
    @patch("recalculate_nan_forecasts.sl")
    def test_csv_only_warning_when_api_unavailable(
        self,
        mock_sl,
        mock_read_api,
        mock_call_hindcast,
        mock_write_api,
        caplog,
        tmp_path,
    ):
        """When SAPPHIRE_API_AVAILABLE is False the API write is
        skipped entirely and a warning about CSV-only persistence is
        logged.
        """
        # Arrange
        mock_sl.load_environment.return_value = None
        mock_read_api.return_value = _make_forecast_df_with_nans()
        mock_call_hindcast.return_value = _make_hindcast_df()

        forecast_dir = tmp_path / "output" / "TFT"
        forecast_dir.mkdir(parents=True)
        env_override = {
            **_BASE_ENV,
            "ieasyforecast_intermediate_data_path": str(tmp_path),
        }

        # Act
        with patch.dict(os.environ, env_override, clear=False):
            with caplog.at_level(logging.WARNING, logger="recalculate_nan_forecasts"):
                recalculate_nan_forecasts.recalculate_nan_forecasts()

        # Assert
        mock_write_api.assert_not_called()
        assert any("API write unsuccessful" in msg for msg in caplog.messages), (
            f"Expected 'API write unsuccessful' in log, got: {caplog.messages}"
        )


# ---------------------------------------------------------------------------
# Test class: Code type mismatch regression (Bug D)
# ---------------------------------------------------------------------------


class TestCodeTypeMismatchFix:
    """Regression test: hindcast int codes must match forecast string codes."""

    @patch.dict(os.environ, _BASE_ENV, clear=False)
    @patch("recalculate_nan_forecasts.SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_nan_forecasts._write_ml_forecast_to_api")
    @patch("recalculate_nan_forecasts.call_hindcast_script")
    @patch("recalculate_nan_forecasts._read_ml_forecasts_from_api")
    @patch("recalculate_nan_forecasts.sl")
    def test_int_hindcast_codes_match_string_forecast_codes(
        self,
        mock_sl,
        mock_read_api,
        mock_call_hindcast,
        mock_write_api,
        caplog,
        tmp_path,
    ):
        """Hindcast with int codes must work when forecast has string codes.

        Before the fix, hindcast["code"] was int (from pd.read_csv) while
        forecast["code"] was str (from the API). The per-code filter
        ``hindcast["code"] == code`` was always False, so no rows were
        updated and the API write was called with the original NaN rows
        rather than the recalculated values — or skipped entirely.
        """
        mock_sl.load_environment.return_value = None

        # Forecast with STRING codes (as API returns)
        forecast = _make_forecast_df_with_nans()
        forecast["code"] = forecast["code"].astype(str)
        mock_read_api.return_value = forecast

        # Hindcast with INTEGER codes (as CSV produces)
        hindcast = _make_hindcast_df()
        hindcast["code"] = hindcast["code"].astype(int)
        mock_call_hindcast.return_value = hindcast

        mock_write_api.return_value = True

        forecast_dir = tmp_path / "output" / "TFT"
        forecast_dir.mkdir(parents=True)
        env_override = {**_BASE_ENV, "ieasyforecast_intermediate_data_path": str(tmp_path)}

        with patch.dict(os.environ, env_override, clear=False):
            with caplog.at_level(logging.INFO, logger="recalculate_nan_forecasts"):
                recalculate_nan_forecasts.recalculate_nan_forecasts()

        # Assert — API write MUST have been called (old bug caused it to be skipped)
        assert mock_write_api.called, (
            "API write was not called — type mismatch may still be present"
        )


# ---------------------------------------------------------------------------
# Test class: call_hindcast_script() raises on subprocess failure
# ---------------------------------------------------------------------------


class TestCallHindcastScriptRaisesOnFailure:
    """Regression: call_hindcast_script raises RuntimeError on non-zero exit."""

    @patch.dict(
        os.environ,
        {
            **_BASE_ENV,
            "IN_DOCKER": "False",
            "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
        },
        clear=False,
    )
    @patch("recalculate_nan_forecasts.subprocess.run")
    def test_raises_runtime_error_on_nonzero_returncode(self, mock_run):
        """When subprocess returns non-zero, RuntimeError is raised."""
        mock_run.return_value = MagicMock(
            returncode=1,
            stderr="Model checkpoint not found",
        )

        import pytest

        with pytest.raises(RuntimeError, match="Hindcast subprocess failed"):
            recalculate_nan_forecasts.call_hindcast_script(
                min_missing_date="2024-06-01",
                max_missing_date="2024-06-10",
                MODEL_TO_USE="TFT",
                intermediate_data_path="/tmp/test",
                codes_with_nan=[12345],
                PREDICTION_MODE="PENTAD",
            )

    @patch.dict(
        os.environ,
        {
            **_BASE_ENV,
            "IN_DOCKER": "False",
            "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
        },
        clear=False,
    )
    @patch("recalculate_nan_forecasts.subprocess.run")
    @patch("recalculate_nan_forecasts.pd.read_csv")
    def test_success_reads_csv(self, mock_read_csv, mock_run):
        """When subprocess succeeds, the hindcast CSV is read and returned."""
        mock_run.return_value = MagicMock(returncode=0)
        expected_df = pd.DataFrame({"code": [12345], "Q50": [10.0]})
        mock_read_csv.return_value = expected_df

        result = recalculate_nan_forecasts.call_hindcast_script(
            min_missing_date="2024-06-01",
            max_missing_date="2024-06-10",
            MODEL_TO_USE="TFT",
            intermediate_data_path="/tmp/test",
            codes_with_nan=[12345],
            PREDICTION_MODE="PENTAD",
        )

        assert result.equals(expected_df)
        mock_read_csv.assert_called_once()
