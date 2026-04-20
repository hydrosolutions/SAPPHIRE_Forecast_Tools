"""Tests for recalculate_nan_forecasts.py bug fixes (ML-004).

Covers:
1. Hindcast call wrapped in try/except — FileNotFoundError caught
2. API write succeeds — success logged
3. API write returns False — warning logged
4. SAPPHIRE_API_AVAILABLE is False — CSV-only warning logged
"""

import logging
import os
import subprocess
import sys
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

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


class TestCsvColumnsAreCanonical:
    """Phase 2 (ML-003): CSV written by recalculate_nan_forecasts must
    contain only canonical ML columns — no API-only columns allowed.
    """

    @patch.dict(os.environ, _BASE_ENV, clear=False)
    @patch("recalculate_nan_forecasts.SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_nan_forecasts._write_ml_forecast_to_api")
    @patch("recalculate_nan_forecasts.call_hindcast_script")
    @patch("recalculate_nan_forecasts._read_ml_forecasts_from_api")
    @patch("recalculate_nan_forecasts.sl")
    def test_csv_write_has_only_canonical_columns(
        self,
        mock_sl,
        mock_read_api,
        mock_call_hindcast,
        mock_write_api,
        tmp_path,
    ):
        """API-only columns (horizon_type, model_type, id) must not appear
        in the CSV file written by recalculate_nan_forecasts.
        """
        from scr.utils_ml_forecast import ML_CANONICAL_CSV_COLUMNS

        # Arrange — forecast from the API that carries extra API-only columns
        forecast = _make_forecast_df_with_nans()
        forecast["horizon_type"] = "day"
        forecast["model_type"] = "TFT"
        forecast["id"] = range(len(forecast))
        mock_sl.load_environment.return_value = None
        mock_read_api.return_value = forecast

        # Hindcast replaces the NaN rows (clean, no extra columns)
        mock_call_hindcast.return_value = _make_hindcast_df()
        mock_write_api.return_value = True

        forecast_dir = tmp_path / "output" / "TFT"
        forecast_dir.mkdir(parents=True)
        env_override = {
            **_BASE_ENV,
            "ieasyforecast_intermediate_data_path": str(tmp_path),
        }

        # Act
        with patch.dict(os.environ, env_override, clear=False):
            recalculate_nan_forecasts.recalculate_nan_forecasts()

        # Assert — read the CSV and verify no API-only columns are present
        csv_path = forecast_dir / "pentad_TFT_forecast.csv"
        assert csv_path.exists(), "Expected CSV file was not written"
        written = pd.read_csv(csv_path)
        api_only = {
            "horizon_type",
            "model_type",
            "id",
            "model_type_description",
            "composition",
            "horizon_value",
            "horizon_in_year",
        }
        leaked = api_only & set(written.columns)
        assert not leaked, f"API-only columns leaked into CSV: {leaked}"
        non_canonical = set(written.columns) - set(ML_CANONICAL_CSV_COLUMNS)
        assert not non_canonical, f"Non-canonical columns in CSV: {non_canonical}"


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


# ---------------------------------------------------------------------------
# Test class: call_hindcast_script() timeout guard (ML-014)
# ---------------------------------------------------------------------------


class TestCallHindcastScriptTimeout:
    """ML-014: subprocess.run has timeout; TimeoutExpired → RuntimeError."""

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
    def test_timeout_raises_runtime_error(self, mock_run):
        """TimeoutExpired from subprocess is wrapped in RuntimeError."""
        mock_run.side_effect = subprocess.TimeoutExpired(
            cmd=["python", "hindcast_ML_models.py"], timeout=14400
        )

        with pytest.raises(RuntimeError, match="timed out after 14400s"):
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
            "SAPPHIRE_HINDCAST_TIMEOUT_SECONDS": "",
        },
        clear=False,
    )
    @patch("recalculate_nan_forecasts.subprocess.run")
    @patch("recalculate_nan_forecasts.pd.read_csv")
    def test_empty_env_var_defaults_to_14400(self, mock_read_csv, mock_run):
        """Empty SAPPHIRE_HINDCAST_TIMEOUT_SECONDS falls back to 14400."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        mock_read_csv.return_value = pd.DataFrame()

        recalculate_nan_forecasts.call_hindcast_script(
            min_missing_date="2024-06-01",
            max_missing_date="2024-06-10",
            MODEL_TO_USE="TFT",
            intermediate_data_path="/tmp/test",
            codes_with_nan=[12345],
            PREDICTION_MODE="PENTAD",
        )

        # Verify timeout= was passed to subprocess.run
        _, kwargs = mock_run.call_args
        assert kwargs["timeout"] == 14400

    @patch.dict(
        os.environ,
        {
            **_BASE_ENV,
            "IN_DOCKER": "False",
            "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
            "SAPPHIRE_HINDCAST_TIMEOUT_SECONDS": "7200",
        },
        clear=False,
    )
    @patch("recalculate_nan_forecasts.subprocess.run")
    @patch("recalculate_nan_forecasts.pd.read_csv")
    def test_custom_timeout_from_env_var(self, mock_read_csv, mock_run):
        """SAPPHIRE_HINDCAST_TIMEOUT_SECONDS=7200 is parsed correctly."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        mock_read_csv.return_value = pd.DataFrame()

        recalculate_nan_forecasts.call_hindcast_script(
            min_missing_date="2024-06-01",
            max_missing_date="2024-06-10",
            MODEL_TO_USE="TFT",
            intermediate_data_path="/tmp/test",
            codes_with_nan=[12345],
            PREDICTION_MODE="PENTAD",
        )

        _, kwargs = mock_run.call_args
        assert kwargs["timeout"] == 7200

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
    def test_missing_env_var_defaults_to_14400(self, mock_read_csv, mock_run):
        """Missing SAPPHIRE_HINDCAST_TIMEOUT_SECONDS falls back to 14400."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        mock_read_csv.return_value = pd.DataFrame()

        # Ensure the key is NOT in the environment
        os.environ.pop("SAPPHIRE_HINDCAST_TIMEOUT_SECONDS", None)

        recalculate_nan_forecasts.call_hindcast_script(
            min_missing_date="2024-06-01",
            max_missing_date="2024-06-10",
            MODEL_TO_USE="TFT",
            intermediate_data_path="/tmp/test",
            codes_with_nan=[12345],
            PREDICTION_MODE="PENTAD",
        )

        _, kwargs = mock_run.call_args
        assert kwargs["timeout"] == 14400

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
    def test_success_logs_hindcast_ran(self, mock_read_csv, mock_run, caplog):
        """On success (returncode=0) the 'Hindcast ran successfully' message is logged.

        stdout/stderr are inherited (fd inheritance, not captured), so
        result.stdout and result.stderr are None — no post-hoc log forwarding.
        """
        mock_run.return_value = MagicMock(returncode=0, stdout=None, stderr=None)
        mock_read_csv.return_value = pd.DataFrame()

        with caplog.at_level(logging.INFO, logger="recalculate_nan_forecasts"):
            recalculate_nan_forecasts.call_hindcast_script(
                min_missing_date="2024-06-01",
                max_missing_date="2024-06-10",
                MODEL_TO_USE="TFT",
                intermediate_data_path="/tmp/test",
                codes_with_nan=[12345],
                PREDICTION_MODE="PENTAD",
            )

        info_messages = [r.message for r in caplog.records if r.levelno == logging.INFO]
        assert any("Hindcast ran successfully" in m for m in info_messages)


# ---------------------------------------------------------------------------
# Test class: Selective API write — only replaced rows sent (ML-013)
# ---------------------------------------------------------------------------


def _make_wide_hindcast_df():
    """Hindcast covering wider range than flag=1 dates — includes flag=0 dates."""
    dates = pd.to_datetime(["2024-06-01", "2024-06-06", "2024-06-11"])
    forecast_dates = pd.to_datetime(["2024-05-31", "2024-06-05", "2024-06-10"])
    return pd.DataFrame(
        {
            "code": [12345, 12345, 12345],
            "date": dates,
            "forecast_date": forecast_dates,
            "flag": [4, 4, 4],
            "Q5": [11.0, 12.0, 13.0],
            "Q25": [21.0, 22.0, 23.0],
            "Q50": [31.0, 32.0, 33.0],
            "Q75": [41.0, 42.0, 43.0],
            "Q95": [51.0, 52.0, 53.0],
        }
    )


class TestSelectiveApiWrite:
    """ML-013: API write sends only the rows whose flag changed from 1/2."""

    @patch.dict(os.environ, _BASE_ENV, clear=False)
    @patch("recalculate_nan_forecasts.SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_nan_forecasts._write_ml_forecast_to_api")
    @patch("recalculate_nan_forecasts.call_hindcast_script")
    @patch("recalculate_nan_forecasts._read_ml_forecasts_from_api")
    @patch("recalculate_nan_forecasts.sl")
    def test_api_write_sends_only_replaced_rows(
        self,
        mock_sl,
        mock_read_api,
        mock_call_hindcast,
        mock_write_api,
        tmp_path,
    ):
        """Only the 2 flag=1 rows that were replaced should reach the API —
        not all 3 rows from the hindcast (which covers the flag=0 row too).
        """
        # Arrange
        mock_sl.load_environment.return_value = None
        mock_read_api.return_value = _make_forecast_df_with_nans()
        # Hindcast covers all 3 dates including the flag=0 row
        mock_call_hindcast.return_value = _make_wide_hindcast_df()
        mock_write_api.return_value = True

        forecast_dir = tmp_path / "output" / "TFT"
        forecast_dir.mkdir(parents=True)
        env_override = {**_BASE_ENV, "ieasyforecast_intermediate_data_path": str(tmp_path)}

        # Act
        with patch.dict(os.environ, env_override, clear=False):
            recalculate_nan_forecasts.recalculate_nan_forecasts()

        # Assert — only the 2 originally-flag=1 rows should be sent
        assert mock_write_api.called, "API write was not called"
        args, kwargs = mock_write_api.call_args
        api_data = args[0]
        assert len(api_data) == 2, f"Expected 2 replaced rows sent to API, got {len(api_data)}"

    @patch.dict(os.environ, _BASE_ENV, clear=False)
    @patch("recalculate_nan_forecasts.SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_nan_forecasts._write_ml_forecast_to_api")
    @patch("recalculate_nan_forecasts.call_hindcast_script")
    @patch("recalculate_nan_forecasts._read_ml_forecasts_from_api")
    @patch("recalculate_nan_forecasts.sl")
    def test_no_matching_hindcast_means_no_api_write(
        self,
        mock_sl,
        mock_read_api,
        mock_call_hindcast,
        mock_write_api,
        tmp_path,
    ):
        """When hindcast is empty the early return at line 324-326 fires
        and the API write is never reached.
        """
        # Arrange
        mock_sl.load_environment.return_value = None
        mock_read_api.return_value = _make_forecast_df_with_nans()
        # Empty hindcast triggers the early return
        mock_call_hindcast.return_value = pd.DataFrame()

        forecast_dir = tmp_path / "output" / "TFT"
        forecast_dir.mkdir(parents=True)
        env_override = {**_BASE_ENV, "ieasyforecast_intermediate_data_path": str(tmp_path)}

        # Act
        with patch.dict(os.environ, env_override, clear=False):
            recalculate_nan_forecasts.recalculate_nan_forecasts()

        # Assert — early return must have prevented any API call
        mock_write_api.assert_not_called()

    @patch.dict(os.environ, _BASE_ENV, clear=False)
    @patch("recalculate_nan_forecasts.SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_nan_forecasts._write_ml_forecast_to_api")
    @patch("recalculate_nan_forecasts.call_hindcast_script")
    @patch("recalculate_nan_forecasts._read_ml_forecasts_from_api")
    @patch("recalculate_nan_forecasts.sl")
    def test_api_write_sends_updated_flag_values(
        self,
        mock_sl,
        mock_read_api,
        mock_call_hindcast,
        mock_write_api,
        tmp_path,
    ):
        """The DataFrame reaching the API must carry the hindcast flag=4,
        not the original flag=1 values.
        """
        # Arrange
        mock_sl.load_environment.return_value = None
        mock_read_api.return_value = _make_forecast_df_with_nans()
        # Hindcast provides flag=4 replacements for both flag=1 rows
        mock_call_hindcast.return_value = _make_hindcast_df()
        mock_write_api.return_value = True

        forecast_dir = tmp_path / "output" / "TFT"
        forecast_dir.mkdir(parents=True)
        env_override = {**_BASE_ENV, "ieasyforecast_intermediate_data_path": str(tmp_path)}

        # Act
        with patch.dict(os.environ, env_override, clear=False):
            recalculate_nan_forecasts.recalculate_nan_forecasts()

        # Assert — flag values in the API payload must be 4 (from hindcast)
        assert mock_write_api.called, "API write was not called"
        args, kwargs = mock_write_api.call_args
        api_data = args[0]
        assert set(api_data["flag"].unique()) == {4}, (
            f"Expected flag=4 in API payload, got: {api_data['flag'].unique()}"
        )
