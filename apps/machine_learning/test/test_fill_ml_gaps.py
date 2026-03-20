"""Tests for fill_ml_gaps.py bug fixes (ML-004).

Covers three fixes:
- Bug C (Phase 1a): Null-discharge filter uses Q50 instead of
  forecasted_discharge.
- Bug A (Phase 1b): call_hindcast_script() wrapped in try/except for
  FileNotFoundError and RuntimeError.
- Bug B (Phase 1c): API write is primary, CSV is fallback. Return value
  of _write_ml_forecast_to_api() is checked and logged.
"""

import logging
import os
import sys
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Mock heavy dependencies before importing fill_ml_gaps
# ---------------------------------------------------------------------------
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

# Mock setup_library so fill_ml_gaps can be imported without env setup
_mock_sl = MagicMock()
_mock_sl.load_environment = MagicMock()
sys.modules["setup_library"] = _mock_sl

# Add paths so imports resolve
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scr"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

import fill_ml_gaps  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_forecast_df(codes=None, n_days=5, include_q50=True, nan_rows=None):
    """Build a minimal forecast DataFrame for testing.

    Args:
        codes: List of station codes. Defaults to [12345].
        n_days: Number of consecutive forecast dates per code.
        include_q50: Whether to include the Q50 column.
        nan_rows: Indices (0-based) where Q50 should be NaN.

    Returns:
        DataFrame matching the expected forecast schema.
    """
    if codes is None:
        codes = ["12345"]
    rows = []
    for code in codes:
        for i in range(n_days):
            row = {
                "code": str(code),
                "forecast_date": pd.Timestamp("2024-01-01") + pd.Timedelta(days=i),
                "date": pd.Timestamp("2024-01-06") + pd.Timedelta(days=i),
                "flag": 0,
                "Q5": 50.0,
                "Q25": 80.0,
                "Q75": 120.0,
                "Q95": 150.0,
            }
            if include_q50:
                row["Q50"] = 100.0
            rows.append(row)
    df = pd.DataFrame(rows)
    if nan_rows is not None and include_q50:
        for idx in nan_rows:
            df.loc[idx, "Q50"] = np.nan
    return df


_BASE_ENV = {
    "SAPPHIRE_MODEL_TO_USE": "TFT",
    "SAPPHIRE_PREDICTION_MODE": "PENTAD",
    "ieasyforecast_intermediate_data_path": "/tmp/test_ml004",
    "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
}


def _make_hindcast_df(codes=None, n_days=3, start_date="2024-01-03"):
    """Build a minimal hindcast DataFrame returned by call_hindcast_script.

    Args:
        codes: List of station codes. Defaults to [12345].
        n_days: Number of hindcast days per code.
        start_date: First forecast_date in the hindcast.

    Returns:
        DataFrame matching the hindcast schema.
    """
    if codes is None:
        codes = [12345]
    rows = []
    for code in codes:
        for i in range(n_days):
            rows.append(
                {
                    "code": code,
                    "forecast_date": pd.Timestamp(start_date) + pd.Timedelta(days=i),
                    "date": pd.Timestamp(start_date) + pd.Timedelta(days=5 + i),
                    "flag": 0,
                    "Q5": 45.0,
                    "Q25": 75.0,
                    "Q50": 95.0,
                    "Q75": 115.0,
                    "Q95": 145.0,
                }
            )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Test class: Null-discharge filter (Bug C)
# ---------------------------------------------------------------------------


class TestNullDischargeFilter:
    """ML-008b: null-Q50 rows must NOT be excluded from gap detection.

    The null-discharge filter was removed because flag=3 rows (null Q50)
    are legitimate forecast records that should count as "represented dates"
    for gap detection purposes. Removing them caused an infinite hindcast loop.
    """

    @patch.dict(os.environ, _BASE_ENV, clear=False)
    @patch("fill_ml_gaps.SAPPHIRE_API_AVAILABLE", True)
    @patch("fill_ml_gaps._write_ml_forecast_to_api")
    @patch("fill_ml_gaps.call_hindcast_script")
    @patch("fill_ml_gaps._read_ml_forecasts_from_api")
    @patch("fill_ml_gaps.get_permitted_station_codes", return_value=None)
    @patch("fill_ml_gaps.sl")
    def test_null_q50_rows_do_not_trigger_hindcast(
        self,
        mock_sl,
        mock_permitted,
        mock_read_api,
        mock_hindcast,
        mock_write_api,
    ):
        """Contiguous dates with some NaN Q50 (flag=3) must not be
        detected as gaps. The gap detector should see all dates as
        present regardless of Q50 value.
        """
        mock_sl.load_environment.return_value = None
        # 5 consecutive days, rows 1 and 3 have NaN Q50
        forecast = _make_forecast_df(n_days=5, nan_rows=[1, 3])
        mock_read_api.return_value = forecast

        fill_ml_gaps.fill_ml_gaps()

        mock_hindcast.assert_not_called()

    @patch.dict(os.environ, _BASE_ENV, clear=False)
    @patch("fill_ml_gaps.SAPPHIRE_API_AVAILABLE", True)
    @patch("fill_ml_gaps._write_ml_forecast_to_api")
    @patch("fill_ml_gaps.call_hindcast_script")
    @patch("fill_ml_gaps._read_ml_forecasts_from_api")
    @patch("fill_ml_gaps.get_permitted_station_codes", return_value=None)
    @patch("fill_ml_gaps.sl")
    def test_no_q50_column_does_not_crash(
        self,
        mock_sl,
        mock_permitted,
        mock_read_api,
        mock_hindcast,
        mock_write_api,
    ):
        """When Q50 column is absent, gap detection still works without crash."""
        mock_sl.load_environment.return_value = None
        forecast = _make_forecast_df(n_days=3, include_q50=False)
        mock_read_api.return_value = forecast

        fill_ml_gaps.fill_ml_gaps()

        mock_hindcast.assert_not_called()


# ---------------------------------------------------------------------------
# Test class: Hindcast call error handling (Bug A)
# ---------------------------------------------------------------------------


class TestHindcastCallErrorHandling:
    """Tests for try/except around call_hindcast_script (Bug A, Phase 1b)."""

    @patch.dict(
        os.environ,
        {
            "SAPPHIRE_MODEL_TO_USE": "TFT",
            "SAPPHIRE_PREDICTION_MODE": "PENTAD",
            "ieasyforecast_intermediate_data_path": "/tmp/test_ml004",
            "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
        },
    )
    @patch("fill_ml_gaps._read_ml_forecasts_from_api")
    @patch("fill_ml_gaps.call_hindcast_script")
    def test_file_not_found_is_caught_and_logged(
        self, mock_hindcast, mock_read_api, caplog, tmp_path
    ):
        """FileNotFoundError from hindcast must be caught, logged, and
        the function returns without crashing.
        """
        # Arrange -- forecast with a gap so the hindcast branch is entered
        forecast_with_gap = _make_forecast_df(n_days=2)
        # Insert a 3-day gap between the two forecast dates
        forecast_with_gap.loc[1, "forecast_date"] = pd.Timestamp("2024-01-05")
        mock_read_api.return_value = forecast_with_gap
        mock_hindcast.side_effect = FileNotFoundError("hindcast script missing")

        csv_dir = tmp_path / "output" / "TFT"
        csv_dir.mkdir(parents=True)

        with patch.dict(
            os.environ,
            {
                "ieasyforecast_intermediate_data_path": str(tmp_path),
            },
        ):
            with caplog.at_level(logging.ERROR, logger="fill_ml_gaps"):
                # Act
                fill_ml_gaps.fill_ml_gaps()

        # Assert
        assert any("hindcast call failed" in rec.message for rec in caplog.records), (
            f"Expected 'hindcast call failed' in logs, got: {[r.message for r in caplog.records]}"
        )

    @patch.dict(
        os.environ,
        {
            "SAPPHIRE_MODEL_TO_USE": "TFT",
            "SAPPHIRE_PREDICTION_MODE": "PENTAD",
            "ieasyforecast_intermediate_data_path": "/tmp/test_ml004",
            "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
        },
    )
    @patch("fill_ml_gaps._read_ml_forecasts_from_api")
    @patch("fill_ml_gaps.call_hindcast_script")
    def test_runtime_error_is_caught_and_logged(
        self, mock_hindcast, mock_read_api, caplog, tmp_path
    ):
        """RuntimeError from hindcast must be caught and logged."""
        # Arrange
        forecast_with_gap = _make_forecast_df(n_days=2)
        forecast_with_gap.loc[1, "forecast_date"] = pd.Timestamp("2024-01-05")
        mock_read_api.return_value = forecast_with_gap
        mock_hindcast.side_effect = RuntimeError("subprocess failed")

        with patch.dict(
            os.environ,
            {
                "ieasyforecast_intermediate_data_path": str(tmp_path),
            },
        ):
            with caplog.at_level(logging.ERROR, logger="fill_ml_gaps"):
                fill_ml_gaps.fill_ml_gaps()

        assert any("hindcast call failed" in rec.message for rec in caplog.records), (
            f"Expected 'hindcast call failed' in logs, got: {[r.message for r in caplog.records]}"
        )


# ---------------------------------------------------------------------------
# Test class: API-first write path (Bug B)
# ---------------------------------------------------------------------------


class TestApiFirstWrite:
    """Tests for API-primary / CSV-fallback write path (Bug B, Phase 1c)."""

    def _run_fill_ml_gaps_with_gap(
        self,
        mock_read_api,
        mock_hindcast,
        mock_write_api,
        api_available,
        api_return,
        tmp_path,
        caplog,
        api_side_effect=None,
    ):
        """Shared helper that sets up a gap, runs fill_ml_gaps, and
        returns the caplog records.

        Args:
            mock_read_api: Patched _read_ml_forecasts_from_api.
            mock_hindcast: Patched call_hindcast_script.
            mock_write_api: Patched _write_ml_forecast_to_api.
            api_available: Value for SAPPHIRE_API_AVAILABLE.
            api_return: Return value for _write_ml_forecast_to_api.
            tmp_path: Pytest tmp_path fixture.
            caplog: Pytest caplog fixture.
            api_side_effect: Optional exception for _write_ml_forecast_to_api.
        """
        # Forecast with a 3-day gap to trigger hindcast
        forecast_with_gap = _make_forecast_df(n_days=2)
        forecast_with_gap.loc[1, "forecast_date"] = pd.Timestamp("2024-01-05")
        mock_read_api.return_value = forecast_with_gap

        # Hindcast returns data for the gap
        mock_hindcast.return_value = _make_hindcast_df(start_date="2024-01-02", n_days=3)

        if api_side_effect is not None:
            mock_write_api.side_effect = api_side_effect
        else:
            mock_write_api.return_value = api_return

        csv_dir = tmp_path / "output" / "TFT"
        csv_dir.mkdir(parents=True)

        with patch.dict(
            os.environ,
            {
                "ieasyforecast_intermediate_data_path": str(tmp_path),
            },
        ):
            with patch("fill_ml_gaps.SAPPHIRE_API_AVAILABLE", api_available):
                with caplog.at_level(logging.DEBUG, logger="fill_ml_gaps"):
                    fill_ml_gaps.fill_ml_gaps()

    @patch.dict(
        os.environ,
        {
            "SAPPHIRE_MODEL_TO_USE": "TFT",
            "SAPPHIRE_PREDICTION_MODE": "PENTAD",
            "ieasyforecast_intermediate_data_path": "/tmp/test_ml004",
            "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
        },
    )
    @patch("fill_ml_gaps._write_ml_forecast_to_api")
    @patch("fill_ml_gaps.call_hindcast_script")
    @patch("fill_ml_gaps._read_ml_forecasts_from_api")
    def test_api_write_success_logged(
        self, mock_read_api, mock_hindcast, mock_write_api, caplog, tmp_path
    ):
        """When API write returns True, a success log line is emitted."""
        self._run_fill_ml_gaps_with_gap(
            mock_read_api,
            mock_hindcast,
            mock_write_api,
            api_available=True,
            api_return=True,
            tmp_path=tmp_path,
            caplog=caplog,
        )

        assert mock_write_api.called
        assert any(
            "Wrote" in rec.message and "gap-filled" in rec.message for rec in caplog.records
        ), f"Expected success log, got: {[r.message for r in caplog.records]}"

    @patch.dict(
        os.environ,
        {
            "SAPPHIRE_MODEL_TO_USE": "TFT",
            "SAPPHIRE_PREDICTION_MODE": "PENTAD",
            "ieasyforecast_intermediate_data_path": "/tmp/test_ml004",
            "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
        },
    )
    @patch("fill_ml_gaps._write_ml_forecast_to_api")
    @patch("fill_ml_gaps.call_hindcast_script")
    @patch("fill_ml_gaps._read_ml_forecasts_from_api")
    def test_api_write_returns_false_warns(
        self, mock_read_api, mock_hindcast, mock_write_api, caplog, tmp_path
    ):
        """When API write returns False, a warning is logged."""
        self._run_fill_ml_gaps_with_gap(
            mock_read_api,
            mock_hindcast,
            mock_write_api,
            api_available=True,
            api_return=False,
            tmp_path=tmp_path,
            caplog=caplog,
        )

        assert any("API write returned failure" in rec.message for rec in caplog.records), (
            f"Expected failure warning, got: {[r.message for r in caplog.records]}"
        )

    @patch.dict(
        os.environ,
        {
            "SAPPHIRE_MODEL_TO_USE": "TFT",
            "SAPPHIRE_PREDICTION_MODE": "PENTAD",
            "ieasyforecast_intermediate_data_path": "/tmp/test_ml004",
            "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
        },
    )
    @patch("fill_ml_gaps._write_ml_forecast_to_api")
    @patch("fill_ml_gaps.call_hindcast_script")
    @patch("fill_ml_gaps._read_ml_forecasts_from_api")
    def test_api_unavailable_skips_api_and_warns_csv_only(
        self, mock_read_api, mock_hindcast, mock_write_api, caplog, tmp_path
    ):
        """When SAPPHIRE_API_AVAILABLE is False, no API call is made and
        the CSV-only warning is emitted.
        """
        self._run_fill_ml_gaps_with_gap(
            mock_read_api,
            mock_hindcast,
            mock_write_api,
            api_available=False,
            api_return=None,
            tmp_path=tmp_path,
            caplog=caplog,
        )

        mock_write_api.assert_not_called()
        assert any("exists only in CSV" in rec.message for rec in caplog.records), (
            f"Expected CSV-only warning, got: {[r.message for r in caplog.records]}"
        )

    @patch.dict(
        os.environ,
        {
            "SAPPHIRE_MODEL_TO_USE": "TFT",
            "SAPPHIRE_PREDICTION_MODE": "PENTAD",
            "ieasyforecast_intermediate_data_path": "/tmp/test_ml004",
            "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
        },
    )
    @patch("fill_ml_gaps._write_ml_forecast_to_api")
    @patch("fill_ml_gaps.call_hindcast_script")
    @patch("fill_ml_gaps._read_ml_forecasts_from_api")
    def test_api_write_exception_caught_and_logged(
        self, mock_read_api, mock_hindcast, mock_write_api, caplog, tmp_path
    ):
        """When _write_ml_forecast_to_api raises, the exception is caught
        and logged, and execution continues to CSV write.
        """
        self._run_fill_ml_gaps_with_gap(
            mock_read_api,
            mock_hindcast,
            mock_write_api,
            api_available=True,
            api_return=None,
            tmp_path=tmp_path,
            caplog=caplog,
            api_side_effect=ConnectionError("API down"),
        )

        assert any("API write raised an exception" in rec.message for rec in caplog.records), (
            f"Expected exception log, got: {[r.message for r in caplog.records]}"
        )

    @patch.dict(
        os.environ,
        {
            "SAPPHIRE_MODEL_TO_USE": "TFT",
            "SAPPHIRE_PREDICTION_MODE": "PENTAD",
            "ieasyforecast_intermediate_data_path": "/tmp/test_ml004",
            "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
        },
    )
    @patch("fill_ml_gaps._write_ml_forecast_to_api")
    @patch("fill_ml_gaps.call_hindcast_script")
    @patch("fill_ml_gaps._read_ml_forecasts_from_api")
    def test_csv_write_has_only_canonical_columns(
        self, mock_read_api, mock_hindcast, mock_write_api, tmp_path
    ):
        """CSV written after gap-fill must contain only canonical columns.

        When the gap-filled data carries API-only columns (e.g., from a
        hindcast that added ``horizon_type``, ``model_type``, or ``id``),
        those columns must be stripped before the CSV is written to disk.
        """
        from scr.utils_ml_forecast import ML_CANONICAL_CSV_COLUMNS

        # Arrange — forecast with a 3-day gap to trigger hindcast path
        forecast_with_gap = _make_forecast_df(n_days=2)
        forecast_with_gap.loc[1, "forecast_date"] = pd.Timestamp("2024-01-05")
        mock_read_api.return_value = forecast_with_gap

        # Hindcast returns a DataFrame that includes API-only extra columns
        hindcast = _make_hindcast_df(start_date="2024-01-02", n_days=3)
        hindcast["horizon_type"] = "day"
        hindcast["model_type"] = "TFT"
        hindcast["id"] = range(len(hindcast))
        mock_hindcast.return_value = hindcast

        mock_write_api.return_value = True

        csv_dir = tmp_path / "output" / "TFT"
        csv_dir.mkdir(parents=True)

        with patch.dict(
            os.environ,
            {"ieasyforecast_intermediate_data_path": str(tmp_path)},
        ):
            with patch("fill_ml_gaps.SAPPHIRE_API_AVAILABLE", True):
                fill_ml_gaps.fill_ml_gaps()

        # Assert — read back the CSV and check column names
        csv_path = csv_dir / "pentad_TFT_forecast.csv"
        assert csv_path.exists(), "Expected CSV file was not written"
        written = __import__("pandas").read_csv(csv_path)
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
        # All written columns must be from the canonical set
        non_canonical = set(written.columns) - set(ML_CANONICAL_CSV_COLUMNS)
        assert not non_canonical, f"Non-canonical columns in CSV: {non_canonical}"

    @patch.dict(
        os.environ,
        {
            "SAPPHIRE_MODEL_TO_USE": "TFT",
            "SAPPHIRE_PREDICTION_MODE": "PENTAD",
            "ieasyforecast_intermediate_data_path": "/tmp/test_ml004",
            "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
        },
    )
    @patch("fill_ml_gaps._write_ml_forecast_to_api")
    @patch("fill_ml_gaps._read_ml_forecasts_from_api")
    def test_no_gaps_skips_api_write(self, mock_read_api, mock_write_api, caplog, tmp_path):
        """When there are no gaps (all_filled_forecasts is empty), no
        API write is attempted.
        """
        # Arrange -- contiguous forecast dates with no gap
        forecast_no_gap = _make_forecast_df(n_days=5)
        mock_read_api.return_value = forecast_no_gap

        with patch.dict(
            os.environ,
            {
                "ieasyforecast_intermediate_data_path": str(tmp_path),
            },
        ):
            with caplog.at_level(logging.DEBUG, logger="fill_ml_gaps"):
                fill_ml_gaps.fill_ml_gaps()

        mock_write_api.assert_not_called()
        # No warning about CSV-only since there is nothing to write
        assert not any("exists only in CSV" in rec.message for rec in caplog.records)


# ---------------------------------------------------------------------------
# Test class: Code type mismatch regression (Bug D)
# ---------------------------------------------------------------------------


class TestCodeTypeMismatchFix:
    """Regression test: hindcast int codes must match forecast string codes."""

    @patch.dict(
        os.environ,
        {
            "SAPPHIRE_MODEL_TO_USE": "TFT",
            "SAPPHIRE_PREDICTION_MODE": "PENTAD",
            "ieasyforecast_intermediate_data_path": "/tmp/test_ml004",
            "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
        },
    )
    @patch("fill_ml_gaps._write_ml_forecast_to_api")
    @patch("fill_ml_gaps.call_hindcast_script")
    @patch("fill_ml_gaps._read_ml_forecasts_from_api")
    def test_int_hindcast_codes_match_string_forecast_codes(
        self, mock_read_api, mock_hindcast, mock_write_api, caplog, tmp_path
    ):
        """Hindcast with int codes must still produce gap-filled rows
        when forecast has string codes from the API.

        Before the fix, hindcast["code"] was int while forecast["code"]
        was str (as returned by the API). The comparison
        ``hindcast.code == code`` was always False, so filled_df was
        empty and the API write was never called.
        """
        # Arrange — forecast with string codes and a gap
        forecast_with_gap = _make_forecast_df(n_days=2)
        forecast_with_gap["code"] = forecast_with_gap["code"].astype(str)
        forecast_with_gap.loc[1, "forecast_date"] = pd.Timestamp("2024-01-05")
        mock_read_api.return_value = forecast_with_gap

        # Hindcast with INTEGER codes (as pd.read_csv would produce)
        hindcast = _make_hindcast_df(start_date="2024-01-02", n_days=3)
        hindcast["code"] = hindcast["code"].astype(int)  # explicit int
        mock_hindcast.return_value = hindcast

        mock_write_api.return_value = True

        csv_dir = tmp_path / "output" / "TFT"
        csv_dir.mkdir(parents=True)

        with patch.dict(os.environ, {"ieasyforecast_intermediate_data_path": str(tmp_path)}):
            with patch("fill_ml_gaps.SAPPHIRE_API_AVAILABLE", True):
                with caplog.at_level(logging.DEBUG, logger="fill_ml_gaps"):
                    fill_ml_gaps.fill_ml_gaps()

        # Assert — API write MUST have been called (the old bug skipped it)
        assert mock_write_api.called, (
            "API write was not called — type mismatch may still be present"
        )
        # The "possible code-type or date-range mismatch" warning must NOT appear
        assert not any(
            "code-type or date-range mismatch" in rec.message for rec in caplog.records
        ), (
            "Mismatch warning was emitted — astype(str) fix may be missing: "
            f"{[r.message for r in caplog.records]}"
        )
