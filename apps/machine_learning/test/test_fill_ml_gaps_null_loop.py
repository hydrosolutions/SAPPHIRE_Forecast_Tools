"""Tests for fill_ml_gaps.py — ML-008b: null-Q50 rows must not trigger
spurious hindcast loops.

Covers five scenarios:
- Contiguous dates with some null-Q50 (flag=3) rows → no hindcast.
- Genuine date gap (no null-Q50 rows) → hindcast called once.
- Mix of null-Q50 rows AND a genuine date gap → hindcast called once,
  only for the genuine gap.
- All dates present, all Q50 valid → no hindcast.
- API returns empty DataFrame → ERROR log containing "falling back to CSV".
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
# Shared env-var baseline — applied to every test via @patch.dict
# ---------------------------------------------------------------------------

_BASE_ENV = {
    "SAPPHIRE_MODEL_TO_USE": "TFT",
    "SAPPHIRE_PREDICTION_MODE": "PENTAD",
    "ieasyforecast_intermediate_data_path": "/tmp/test_intermediate",
    "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_forecast_df(
    codes=None,
    dates=None,
    flags=None,
    q50_values=None,
):
    """Build a forecast DataFrame with explicit control over dates and Q50 values.

    Args:
        codes: List of station codes. Defaults to ["15189"].
        dates: List of forecast_date values (pd.Timestamp-compatible).
            Defaults to 10 consecutive days starting 2024-01-01.
        flags: List of flag values per row. Defaults to all 0.
            Length must match len(codes) * len(dates) or be per-row.
        q50_values: List of Q50 values per row. Defaults to all 100.0.
            Use np.nan to represent a null-Q50 (flag=3) row.

    Returns:
        DataFrame with columns: code, forecast_date, date, flag,
        Q5, Q25, Q50, Q75, Q95.
    """
    if codes is None:
        codes = ["15189"]
    if dates is None:
        dates = [pd.Timestamp("2024-01-01") + pd.Timedelta(days=i) for i in range(10)]

    rows = []
    idx = 0
    for code in codes:
        for d in dates:
            flag = flags[idx] if flags is not None else 0
            q50 = q50_values[idx] if q50_values is not None else 100.0
            rows.append(
                {
                    "code": str(code),
                    "forecast_date": pd.Timestamp(d),
                    "date": pd.Timestamp(d) + pd.Timedelta(days=5),
                    "flag": flag,
                    "Q5": 50.0,
                    "Q25": 80.0,
                    "Q50": q50,
                    "Q75": 120.0,
                    "Q95": 150.0,
                }
            )
            idx += 1
    return pd.DataFrame(rows)


def _make_hindcast_return_df(start_date="2024-01-06", n_days=2):
    """Build a minimal hindcast DataFrame for use as call_hindcast_script return value.

    Args:
        start_date: First forecast_date in the hindcast.
        n_days: Number of hindcast rows.

    Returns:
        DataFrame with the full forecast schema.
    """
    rows = []
    for i in range(n_days):
        rows.append(
            {
                "code": "15189",
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
# Test class: null-Q50 rows must not produce phantom gaps (ML-008b)
# ---------------------------------------------------------------------------


class TestNullQ50NoHindcastLoop:
    """ML-008b: flag=3 rows (null Q50) are valid forecast dates.

    They must count as "represented dates" for gap detection and must NOT
    be silently excluded, which would create phantom gaps and trigger
    spurious hindcast calls.
    """

    @patch.dict(os.environ, _BASE_ENV, clear=False)
    @patch("fill_ml_gaps.get_permitted_station_codes")
    @patch("fill_ml_gaps.sl")
    @patch("fill_ml_gaps.call_hindcast_script")
    @patch("fill_ml_gaps._read_ml_forecasts_from_api")
    def test_contiguous_dates_with_null_q50_no_hindcast(
        self,
        mock_read_api,
        mock_hindcast,
        mock_sl,
        mock_get_codes,
    ):
        """10 consecutive days; days 3-5 have Q50=NaN and flag=3.

        No date gap exists, so call_hindcast_script must NOT be called.
        """
        # Arrange
        dates = [pd.Timestamp("2024-01-01") + pd.Timedelta(days=i) for i in range(10)]
        flags = [3 if 2 <= i <= 4 else 0 for i in range(10)]
        q50s = [np.nan if 2 <= i <= 4 else 100.0 for i in range(10)]
        forecast = _make_forecast_df(dates=dates, flags=flags, q50_values=q50s)

        mock_read_api.return_value = forecast
        mock_get_codes.return_value = None  # all-codes mode
        mock_sl.load_environment = MagicMock()

        # Act
        fill_ml_gaps.fill_ml_gaps()

        # Assert
        mock_hindcast.assert_not_called()

    @patch.dict(os.environ, _BASE_ENV, clear=False)
    @patch("fill_ml_gaps._write_ml_forecast_to_api")
    @patch("fill_ml_gaps.get_permitted_station_codes")
    @patch("fill_ml_gaps.sl")
    @patch("fill_ml_gaps.call_hindcast_script")
    @patch("fill_ml_gaps._read_ml_forecasts_from_api")
    def test_genuine_date_gap_triggers_hindcast(
        self,
        mock_read_api,
        mock_hindcast,
        mock_sl,
        mock_get_codes,
        mock_write_api,
        tmp_path,
    ):
        """Days 1-5 and days 8-10 present (gap on days 6-7), all Q50 valid.

        call_hindcast_script must be called exactly once.
        """
        # Arrange — days 0-4 then days 7-9 (two-day gap between day 4 and day 7)
        dates = [pd.Timestamp("2024-01-01") + pd.Timedelta(days=i) for i in range(5)] + [
            pd.Timestamp("2024-01-01") + pd.Timedelta(days=i) for i in range(7, 10)
        ]
        forecast = _make_forecast_df(dates=dates)

        mock_read_api.return_value = forecast
        mock_get_codes.return_value = None
        mock_sl.load_environment = MagicMock()
        mock_hindcast.return_value = _make_hindcast_return_df(start_date="2024-01-06", n_days=2)
        mock_write_api.return_value = True

        csv_dir = tmp_path / "output" / "TFT"
        csv_dir.mkdir(parents=True)

        with patch.dict(
            os.environ,
            {"ieasyforecast_intermediate_data_path": str(tmp_path)},
        ):
            with patch("fill_ml_gaps.SAPPHIRE_API_AVAILABLE", True):
                # Act
                fill_ml_gaps.fill_ml_gaps()

        # Assert
        mock_hindcast.assert_called_once()

    @patch.dict(os.environ, _BASE_ENV, clear=False)
    @patch("fill_ml_gaps._write_ml_forecast_to_api")
    @patch("fill_ml_gaps.get_permitted_station_codes")
    @patch("fill_ml_gaps.sl")
    @patch("fill_ml_gaps.call_hindcast_script")
    @patch("fill_ml_gaps._read_ml_forecasts_from_api")
    def test_mix_of_null_rows_and_genuine_gap(
        self,
        mock_read_api,
        mock_hindcast,
        mock_sl,
        mock_get_codes,
        mock_write_api,
        tmp_path,
    ):
        """Days 1-10 present except day 6 (genuine gap). Days 3-4 have Q50=NaN, flag=3.

        call_hindcast_script must be called exactly once — only for the
        genuine date gap, not for the null-Q50 rows.
        """
        # Arrange — days 0-4 and 6-9 (skip day 5 → genuine one-day gap)
        all_days = list(range(5)) + list(range(6, 10))
        dates = [pd.Timestamp("2024-01-01") + pd.Timedelta(days=i) for i in all_days]
        flags = [3 if i in (2, 3) else 0 for i in range(len(dates))]
        q50s = [np.nan if i in (2, 3) else 100.0 for i in range(len(dates))]
        forecast = _make_forecast_df(dates=dates, flags=flags, q50_values=q50s)

        mock_read_api.return_value = forecast
        mock_get_codes.return_value = None
        mock_sl.load_environment = MagicMock()
        mock_hindcast.return_value = _make_hindcast_return_df(start_date="2024-01-06", n_days=1)
        mock_write_api.return_value = True

        csv_dir = tmp_path / "output" / "TFT"
        csv_dir.mkdir(parents=True)

        with patch.dict(
            os.environ,
            {"ieasyforecast_intermediate_data_path": str(tmp_path)},
        ):
            with patch("fill_ml_gaps.SAPPHIRE_API_AVAILABLE", True):
                # Act
                fill_ml_gaps.fill_ml_gaps()

        # Assert — exactly one hindcast call for the genuine gap
        mock_hindcast.assert_called_once()

    @patch.dict(os.environ, _BASE_ENV, clear=False)
    @patch("fill_ml_gaps.get_permitted_station_codes")
    @patch("fill_ml_gaps.sl")
    @patch("fill_ml_gaps.call_hindcast_script")
    @patch("fill_ml_gaps._read_ml_forecasts_from_api")
    def test_all_dates_present_no_hindcast(
        self,
        mock_read_api,
        mock_hindcast,
        mock_sl,
        mock_get_codes,
    ):
        """10 consecutive days, all flag=0, all Q50 valid.

        No gap exists → call_hindcast_script must NOT be called.
        """
        # Arrange
        forecast = _make_forecast_df()  # default: 10 consecutive days, Q50=100.0

        mock_read_api.return_value = forecast
        mock_get_codes.return_value = None
        mock_sl.load_environment = MagicMock()

        # Act
        fill_ml_gaps.fill_ml_gaps()

        # Assert
        mock_hindcast.assert_not_called()

    @patch.dict(os.environ, _BASE_ENV, clear=False)
    @patch("fill_ml_gaps.get_permitted_station_codes")
    @patch("fill_ml_gaps.sl")
    @patch("fill_ml_gaps._read_ml_forecasts_from_api")
    def test_csv_fallback_logs_error(
        self,
        mock_read_api,
        mock_sl,
        mock_get_codes,
        caplog,
    ):
        """When _read_ml_forecasts_from_api returns empty, an ERROR log
        containing 'falling back to CSV' must be emitted before the CSV
        read is attempted.

        The CSV path does not exist, so the function exits early after
        logging the warning — that is acceptable; the key assertion is
        the ERROR log on the API-empty branch.
        """
        # Arrange — API returns nothing, no CSV file on disk
        mock_read_api.return_value = pd.DataFrame()
        mock_get_codes.return_value = None
        mock_sl.load_environment = MagicMock()

        with caplog.at_level(logging.ERROR, logger="fill_ml_gaps"):
            # Act
            fill_ml_gaps.fill_ml_gaps()

        # Assert — the CSV-fallback ERROR must appear
        assert any("falling back to CSV" in rec.message for rec in caplog.records), (
            "Expected an ERROR log containing 'falling back to CSV', "
            f"got: {[r.message for r in caplog.records]}"
        )
