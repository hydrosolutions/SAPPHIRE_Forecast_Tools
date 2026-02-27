"""
Integration tests for main() in linear_regression.py.

Tests the orchestration logic: mode branching, loop control, early exits,
and store_last_successful_run_date behavior. All external boundaries
(sl, fl, tl, SDK) are mocked; the internal wiring runs for real.
"""

import datetime as dt
import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import linear_regression as lr_module

# ============================================================================
# Helpers
# ============================================================================


def _make_fake_site(code="15013"):
    """Create a minimal mock site object."""
    site = MagicMock()
    site.code = code
    return site


def _make_data_df(codes=None):
    """Create a small non-empty DataFrame resembling discharge data."""
    if codes is None:
        codes = ["15013"]
    rows = []
    for code in codes:
        rows.append(
            {
                "code": code,
                "date": "2024-01-05",
                "discharge_sum": 100.0,
                "discharge_avg": 50.0,
                "pentad_in_year": 1,
                "decad_in_year": 1,
                "predictor": 100.0,
            }
        )
    return pd.DataFrame(rows)


def _make_linreg_df():
    """Create a small result DataFrame from perform_linear_regression."""
    return pd.DataFrame(
        {
            "code": ["15013"],
            "date": ["2024-01-05"],
            "discharge_sum": [100.0],
            "discharge_avg": [50.0],
            "pentad_in_year": [1],
            "pentad": [1],
        }
    )


def _make_predictor_dates():
    """Create a mock predictor_dates object with .pentad and .decad."""
    pd_obj = MagicMock()
    pd_obj.pentad = [dt.date(2024, 1, 1), dt.date(2024, 1, 5)]
    pd_obj.decad = [dt.date(2024, 1, 1), dt.date(2024, 1, 10)]
    return pd_obj


def _setup_common_mocks(mock_sl, mock_fl, mock_tl, mock_sdk_hf, prediction_mode="PENTAD"):
    """Configure the standard mock behavior for a successful pipeline run.

    Args:
        mock_sl: Mock for setup_library
        mock_fl: Mock for forecast_library
        mock_tl: Mock for tag_library
        mock_sdk_hf: Mock for IEasyHydroHFSDK class
        prediction_mode: PENTAD, DECAD, or BOTH
    """
    # setup_library
    mock_sl.load_environment.return_value = None
    mock_sl.check_database_access.return_value = True
    mock_sl.ForecastFlags.from_forecast_date_get_flags.return_value = MagicMock(
        pentad=True, decad=True
    )

    fake_sites = [_make_fake_site("15013")]
    mock_sl.get_pentadal_forecast_sites_from_HF_SDK.return_value = (fake_sites, ["15013"], None)
    mock_sl.get_decadal_forecast_sites_from_HF_SDK.return_value = (fake_sites, ["15013"], None)
    mock_sl.store_last_successful_run_date.return_value = None

    # forecast_library
    data = _make_data_df()
    mock_fl.get_pentadal_and_decadal_data.return_value = (data, data)
    mock_fl.filter_discharge_data_for_code_and_date.return_value = data
    mock_fl.get_predictor_dates.return_value = _make_predictor_dates()
    mock_fl.get_predictors.return_value = None
    mock_fl.save_discharge_avg.return_value = None
    mock_fl.perform_linear_regression.return_value = _make_linreg_df()
    mock_fl.perform_forecast.return_value = None
    mock_fl.parse_dates_robust.return_value = pd.to_datetime(pd.Series(["2024-01-05"]))
    mock_fl.write_linreg_pentad_forecast_data.return_value = None
    mock_fl.write_linreg_decad_forecast_data.return_value = None
    mock_fl.write_pentad_hydrograph_data.return_value = None
    mock_fl.write_pentad_time_series_data.return_value = None
    mock_fl.write_decad_hydrograph_data.return_value = None
    mock_fl.write_decad_time_series_data.return_value = None

    # tag_library
    mock_tl.get_pentad_in_year.return_value = 1
    mock_tl.get_decad_in_year.return_value = 1

    # SDK
    mock_sdk_hf.return_value = MagicMock()


# Common env vars for all tests
_BASE_ENV = {
    "ieasyhydroforecast_ssh_to_iEH": "False",
    "ieasyhydroforecast_connect_to_iEH": "False",
    "ieasyforecast_intermediate_data_path": "/tmp/test",
    "ieasyforecast_last_successful_run_file": "/tmp/test/last_run.txt",
}


# ============================================================================
# Forecast mode
# ============================================================================


class TestMainForecastMode:
    """Tests for normal (non-hindcast) forecast mode."""

    @patch.object(lr_module, "IEasyHydroHFSDK")
    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_forecast_mode_calls_store_last_run_date(self, mock_sl, mock_fl, mock_tl, mock_sdk_hf):
        """In forecast mode, store_last_successful_run_date IS called."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl, mock_sdk_hf)
        # define_run_dates returns a single day (today=Jan 5 which is a
        # pentad day)
        forecast_date = dt.date(2024, 1, 5)
        mock_sl.define_run_dates.return_value = (
            forecast_date,
            forecast_date,
            forecast_date + dt.timedelta(days=1),
        )

        env = {**_BASE_ENV, "SAPPHIRE_PREDICTION_MODE": "PENTAD"}
        with patch("sys.argv", ["linear_regression.py"]), patch.dict(os.environ, env, clear=False):
            with pytest.raises(SystemExit) as exc_info:
                lr_module.main()
            # main() exits with 1 when ret is not None from the
            # final check; store returns None → ret=None → exit(0)
            # but since we set store to return None, ret is None
            # from the else branch, so exit(0)
            assert exc_info.value.code == 0

        mock_sl.store_last_successful_run_date.assert_called()

    @patch.object(lr_module, "IEasyHydroHFSDK")
    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_forecast_mode_pentad_only(self, mock_sl, mock_fl, mock_tl, mock_sdk_hf):
        """PENTAD mode → pentad write called, decad write NOT called."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl, mock_sdk_hf)
        forecast_date = dt.date(2024, 1, 5)
        mock_sl.define_run_dates.return_value = (
            forecast_date,
            forecast_date,
            forecast_date + dt.timedelta(days=1),
        )

        env = {**_BASE_ENV, "SAPPHIRE_PREDICTION_MODE": "PENTAD"}
        with patch("sys.argv", ["linear_regression.py"]), patch.dict(os.environ, env, clear=False):
            with pytest.raises(SystemExit):
                lr_module.main()

        mock_fl.write_linreg_pentad_forecast_data.assert_called()
        mock_fl.write_linreg_decad_forecast_data.assert_not_called()

    @patch.object(lr_module, "IEasyHydroHFSDK")
    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_forecast_mode_decad_only(self, mock_sl, mock_fl, mock_tl, mock_sdk_hf):
        """DECAD mode → decad write called, pentad write NOT called."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl, mock_sdk_hf)
        forecast_date = dt.date(2024, 1, 10)  # 10th is a decad day
        mock_sl.define_run_dates.return_value = (
            forecast_date,
            forecast_date,
            forecast_date + dt.timedelta(days=1),
        )
        # For DECAD mode, ForecastFlags needs decad=True
        mock_sl.ForecastFlags.from_forecast_date_get_flags.return_value = MagicMock(
            pentad=True, decad=True
        )

        env = {**_BASE_ENV, "SAPPHIRE_PREDICTION_MODE": "DECAD"}
        with patch("sys.argv", ["linear_regression.py"]), patch.dict(os.environ, env, clear=False):
            with pytest.raises(SystemExit):
                lr_module.main()

        mock_fl.write_linreg_decad_forecast_data.assert_called()
        mock_fl.write_linreg_pentad_forecast_data.assert_not_called()


# ============================================================================
# Hindcast mode
# ============================================================================


class TestMainHindcastMode:
    """Tests for hindcast mode behavior."""

    @patch.object(lr_module, "IEasyHydroHFSDK")
    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_hindcast_does_not_store_last_run_date(self, mock_sl, mock_fl, mock_tl, mock_sdk_hf):
        """In hindcast mode, store_last_successful_run_date is NOT called."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl, mock_sdk_hf)

        env = {**_BASE_ENV, "SAPPHIRE_PREDICTION_MODE": "PENTAD"}
        with (
            patch(
                "sys.argv",
                [
                    "linear_regression.py",
                    "--hindcast",
                    "--start-date",
                    "2024-01-05",
                    "--end-date",
                    "2024-01-05",
                ],
            ),
            patch.dict(os.environ, env, clear=False),
        ):
            with pytest.raises(SystemExit) as exc_info:
                lr_module.main()
            assert exc_info.value.code == 0

        mock_sl.store_last_successful_run_date.assert_not_called()

    @patch.object(lr_module, "IEasyHydroHFSDK")
    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_hindcast_skips_non_forecast_days(self, mock_sl, mock_fl, mock_tl, mock_sdk_hf):
        """Hindcast Jan 1-10 with PENTAD → only processes Jan 5 and Jan 10."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl, mock_sdk_hf)

        env = {**_BASE_ENV, "SAPPHIRE_PREDICTION_MODE": "PENTAD"}
        with (
            patch(
                "sys.argv",
                [
                    "linear_regression.py",
                    "--hindcast",
                    "--start-date",
                    "2024-01-01",
                    "--end-date",
                    "2024-01-10",
                ],
            ),
            patch.dict(os.environ, env, clear=False),
        ):
            with pytest.raises(SystemExit) as exc_info:
                lr_module.main()
            assert exc_info.value.code == 0

        # write_linreg_pentad_forecast_data called exactly twice
        # (Jan 5 and Jan 10)
        assert mock_fl.write_linreg_pentad_forecast_data.call_count == 2

    @patch.object(lr_module, "IEasyHydroHFSDK")
    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_hindcast_up_to_date_exits_cleanly(self, mock_sl, mock_fl, mock_tl, mock_sdk_hf):
        """Start date already past end → SystemExit(0) with nothing to do."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl, mock_sdk_hf)

        # Use Jan 30 as start — next PENTAD day is Jan 31, but end is Jan 29
        env = {**_BASE_ENV, "SAPPHIRE_PREDICTION_MODE": "PENTAD"}
        with (
            patch(
                "sys.argv",
                [
                    "linear_regression.py",
                    "--hindcast",
                    "--start-date",
                    "2024-01-26",
                    "--end-date",
                    "2024-01-30",
                ],
            ),
            patch.dict(os.environ, env, clear=False),
        ):
            with pytest.raises(SystemExit) as exc_info:
                lr_module.main()
            assert exc_info.value.code == 0

        # No forecast processing should have happened
        mock_fl.write_linreg_pentad_forecast_data.assert_not_called()


# ============================================================================
# Early exits
# ============================================================================


class TestMainEarlyExits:
    """Tests for early exit conditions in main()."""

    @patch.object(lr_module, "IEasyHydroHFSDK")
    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_exits_when_no_forecast_date(self, mock_sl, mock_fl, mock_tl, mock_sdk_hf):
        """define_run_dates returns (None, None, None) → exits."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl, mock_sdk_hf)
        mock_sl.define_run_dates.return_value = (None, None, None)

        env = {**_BASE_ENV, "SAPPHIRE_PREDICTION_MODE": "PENTAD"}
        with patch("sys.argv", ["linear_regression.py"]), patch.dict(os.environ, env, clear=False):
            # main() calls exit() (not sys.exit) for this case,
            # which also raises SystemExit
            with pytest.raises(SystemExit):
                lr_module.main()

        # No forecast processing should have happened
        mock_fl.get_pentadal_and_decadal_data.assert_not_called()

    @patch.object(lr_module, "IEasyHydroHFSDK")
    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_exits_when_no_pentad_data(self, mock_sl, mock_fl, mock_tl, mock_sdk_hf):
        """Empty pentad DataFrame with PENTAD mode → exits."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl, mock_sdk_hf)
        mock_sl.define_run_dates.return_value = (
            dt.date(2024, 1, 5),
            dt.date(2024, 1, 5),
            dt.date(2024, 1, 6),
        )
        # Return empty pentad data
        mock_fl.get_pentadal_and_decadal_data.return_value = (pd.DataFrame(), _make_data_df())

        env = {**_BASE_ENV, "SAPPHIRE_PREDICTION_MODE": "PENTAD"}
        with patch("sys.argv", ["linear_regression.py"]), patch.dict(os.environ, env, clear=False):
            with pytest.raises(SystemExit):
                lr_module.main()

        # Should not reach the forecast loop
        mock_fl.perform_linear_regression.assert_not_called()

    @patch.object(lr_module, "IEasyHydroHFSDK")
    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_exits_when_hindcast_autodetect_fails(self, mock_sl, mock_fl, mock_tl, mock_sdk_hf):
        """get_hindcast_start_date_from_output returns None → SystemExit(1)."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl, mock_sdk_hf)

        with patch.object(
            lr_module,
            "get_hindcast_start_date_from_output",
            return_value=None,
        ):
            env = {**_BASE_ENV, "SAPPHIRE_PREDICTION_MODE": "PENTAD"}
            with (
                patch(
                    "sys.argv",
                    [
                        "linear_regression.py",
                        "--hindcast",
                    ],
                ),
                patch.dict(os.environ, env, clear=False),
            ):
                with pytest.raises(SystemExit) as exc_info:
                    lr_module.main()
                assert exc_info.value.code == 1
