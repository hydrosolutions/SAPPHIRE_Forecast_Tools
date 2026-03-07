"""
Integration tests for main() in linear_regression.py.

Tests the orchestration logic: mode branching, loop control, early exits.
All external boundaries (sl, fl, tl, SDK) are mocked; the internal wiring
runs for real.
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


class _FakeDate(dt.date):
    """A date subclass that overrides today() for testing."""

    _today = dt.date(2024, 1, 5)  # default: pentad day

    @classmethod
    def today(cls):
        return cls._today


def _make_fake_date(target):
    """Create a FakeDate class pinned to the given date."""

    class PinnedDate(_FakeDate):
        _today = target

    return PinnedDate


def _setup_common_mocks(mock_sl, mock_fl, mock_tl):
    """Configure the standard mock behavior for a successful pipeline run.

    Args:
        mock_sl: Mock for setup_library
        mock_fl: Mock for forecast_library
        mock_tl: Mock for tag_library
    """
    # setup_library
    mock_sl.load_environment.return_value = None
    mock_sl.check_database_access.return_value = False
    mock_sl.ForecastFlags.from_forecast_date_get_flags.return_value = MagicMock(
        pentad=True, decad=True
    )

    fake_sites = [_make_fake_site("15013")]
    # Config-file path: get_pentadal_forecast_sites returns (sites, codes)
    mock_sl.get_pentadal_forecast_sites.return_value = (fake_sites, ["15013"])
    mock_sl.get_decadal_forecast_sites_from_pentadal_sites.return_value = (fake_sites, ["15013"])

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


# Common env vars for all tests
_BASE_ENV = {
    "ieasyhydroforecast_ssh_to_iEH": "False",
    "ieasyhydroforecast_connect_to_iEH": "False",
    "ieasyforecast_intermediate_data_path": "/tmp/test",
    "ieasyforecast_last_successful_run_file": "/tmp/test/last_run.txt",
}


# ============================================================================
# Forecast (operational) mode
# ============================================================================


class TestMainForecastMode:
    """Tests for normal (non-hindcast) operational mode.

    Operational mode runs for today only (no define_run_dates, no
    store_last_successful_run_date).
    """

    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_operational_runs_for_today(self, mock_sl, mock_fl, mock_tl):
        """Operational mode uses date.today() as the forecast date."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl)

        # Pin today to Jan 5 (a pentad day)
        fake_date = _make_fake_date(dt.date(2024, 1, 5))
        env = {**_BASE_ENV, "SAPPHIRE_PREDICTION_MODE": "PENTAD"}
        with (
            patch("sys.argv", ["linear_regression.py"]),
            patch.dict(os.environ, env, clear=False),
            patch.object(lr_module.dt, "date", fake_date),
        ):
            with pytest.raises(SystemExit) as exc_info:
                lr_module.main()
            assert exc_info.value.code == 0

        # Pentad write was called with forecast_date=Jan 5
        mock_fl.write_linreg_pentad_forecast_data.assert_called_once()
        call_kwargs = mock_fl.write_linreg_pentad_forecast_data.call_args
        assert call_kwargs[1]["forecast_date"] == dt.date(2024, 1, 5)

    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_operational_no_define_run_dates(self, mock_sl, mock_fl, mock_tl):
        """Operational mode never calls define_run_dates."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl)

        fake_date = _make_fake_date(dt.date(2024, 1, 5))
        env = {**_BASE_ENV, "SAPPHIRE_PREDICTION_MODE": "PENTAD"}
        with (
            patch("sys.argv", ["linear_regression.py"]),
            patch.dict(os.environ, env, clear=False),
            patch.object(lr_module.dt, "date", fake_date),
        ):
            with pytest.raises(SystemExit):
                lr_module.main()

        mock_sl.define_run_dates.assert_not_called()

    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_operational_no_store_call(self, mock_sl, mock_fl, mock_tl):
        """Operational mode does not call store_last_successful_run_date."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl)

        fake_date = _make_fake_date(dt.date(2024, 1, 5))
        env = {**_BASE_ENV, "SAPPHIRE_PREDICTION_MODE": "PENTAD"}
        with (
            patch("sys.argv", ["linear_regression.py"]),
            patch.dict(os.environ, env, clear=False),
            patch.object(lr_module.dt, "date", fake_date),
        ):
            with pytest.raises(SystemExit) as exc_info:
                lr_module.main()
            assert exc_info.value.code == 0

        mock_sl.store_last_successful_run_date.assert_not_called()

    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_operational_idempotent(self, mock_sl, mock_fl, mock_tl):
        """Running twice on the same day produces the same forecast_date."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl)

        fake_date = _make_fake_date(dt.date(2024, 1, 5))
        env = {**_BASE_ENV, "SAPPHIRE_PREDICTION_MODE": "PENTAD"}

        # First run
        with (
            patch("sys.argv", ["linear_regression.py"]),
            patch.dict(os.environ, env, clear=False),
            patch.object(lr_module.dt, "date", fake_date),
        ):
            with pytest.raises(SystemExit):
                lr_module.main()

        first_date = mock_fl.write_linreg_pentad_forecast_data.call_args[1]["forecast_date"]

        # Reset mocks for second run
        _setup_common_mocks(mock_sl, mock_fl, mock_tl)

        # Second run — same day, same result
        with (
            patch("sys.argv", ["linear_regression.py"]),
            patch.dict(os.environ, env, clear=False),
            patch.object(lr_module.dt, "date", fake_date),
        ):
            with pytest.raises(SystemExit):
                lr_module.main()

        second_date = mock_fl.write_linreg_pentad_forecast_data.call_args[1]["forecast_date"]
        assert first_date == second_date == dt.date(2024, 1, 5)

    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_forecast_mode_pentad_only(self, mock_sl, mock_fl, mock_tl):
        """PENTAD mode -> pentad write called, decad write NOT called."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl)

        fake_date = _make_fake_date(dt.date(2024, 1, 5))
        env = {**_BASE_ENV, "SAPPHIRE_PREDICTION_MODE": "PENTAD"}
        with (
            patch("sys.argv", ["linear_regression.py"]),
            patch.dict(os.environ, env, clear=False),
            patch.object(lr_module.dt, "date", fake_date),
        ):
            with pytest.raises(SystemExit):
                lr_module.main()

        mock_fl.write_linreg_pentad_forecast_data.assert_called()
        mock_fl.write_linreg_decad_forecast_data.assert_not_called()

    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_forecast_mode_decad_only(self, mock_sl, mock_fl, mock_tl):
        """DECAD mode -> decad write called, pentad write NOT called."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl)

        # Pin today to 10th (a decad day)
        fake_date = _make_fake_date(dt.date(2024, 1, 10))
        env = {**_BASE_ENV, "SAPPHIRE_PREDICTION_MODE": "DECAD"}
        with (
            patch("sys.argv", ["linear_regression.py"]),
            patch.dict(os.environ, env, clear=False),
            patch.object(lr_module.dt, "date", fake_date),
        ):
            with pytest.raises(SystemExit):
                lr_module.main()

        mock_fl.write_linreg_decad_forecast_data.assert_called()
        mock_fl.write_linreg_pentad_forecast_data.assert_not_called()


# ============================================================================
# Hindcast mode
# ============================================================================


class TestMainHindcastMode:
    """Tests for hindcast mode behavior."""

    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_hindcast_does_not_store_last_run_date(self, mock_sl, mock_fl, mock_tl):
        """In hindcast mode, store_last_successful_run_date is NOT called."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl)

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

    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_hindcast_skips_non_forecast_days(self, mock_sl, mock_fl, mock_tl):
        """Hindcast Jan 1-10 with PENTAD -> only processes Jan 5 and Jan 10."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl)

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

    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_hindcast_up_to_date_exits_cleanly(self, mock_sl, mock_fl, mock_tl):
        """Start date already past end -> SystemExit(0) with nothing to do."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl)

        # Use Jan 30 as start -- next PENTAD day is Jan 31, but end is Jan 29
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

    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_exits_when_no_pentad_data(self, mock_sl, mock_fl, mock_tl):
        """Empty pentad DataFrame with PENTAD mode -> exits."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl)
        # Return empty pentad data
        mock_fl.get_pentadal_and_decadal_data.return_value = (pd.DataFrame(), _make_data_df())

        fake_date = _make_fake_date(dt.date(2024, 1, 5))
        env = {**_BASE_ENV, "SAPPHIRE_PREDICTION_MODE": "PENTAD"}
        with (
            patch("sys.argv", ["linear_regression.py"]),
            patch.dict(os.environ, env, clear=False),
            patch.object(lr_module.dt, "date", fake_date),
        ):
            with pytest.raises(SystemExit):
                lr_module.main()

        # Should not reach the forecast loop
        mock_fl.perform_linear_regression.assert_not_called()

    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_exits_when_hindcast_autodetect_fails(self, mock_sl, mock_fl, mock_tl):
        """get_hindcast_start_date_from_output returns None -> SystemExit(1)."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl)

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
