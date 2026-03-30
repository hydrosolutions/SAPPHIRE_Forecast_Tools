"""
Tests for API write failure reporting in linear_regression.py and forecast_library.py.

Covers:
  - Six wrapper functions in forecast_library returning bool (True on success,
    False when an API exception is caught in warn/ignore mode)
  - Integration: main() exits with code 1 when any write returns False, and
    logs a CRITICAL message.
"""

import datetime as dt
import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ── module under test ──────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import linear_regression as lr_module  # noqa: E402

# Import forecast_library directly so we can call the wrapper functions
_FL_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast")
sys.path.insert(0, os.path.abspath(_FL_DIR))
import forecast_library as fl_module  # noqa: E402

# ============================================================================
# Data-factory helpers (reused across all test classes)
# ============================================================================

_PENTAD_COLS = [
    "code",
    "date",
    "discharge_sum",
    "discharge_avg",
    "pentad_in_year",
    "predictor",
    "issue_date",
    "discharge",
    "forecasted_discharge",
]

_DECAD_COLS = [
    "code",
    "date",
    "discharge_sum",
    "discharge_avg",
    "decad_in_year",
    "predictor",
    "issue_date",
    "discharge",
    "forecasted_discharge",
]

_HYDROGRAPH_PENTAD_COLS = [
    "code",
    "date",
    "discharge_avg",
    "discharge_sum",
    "discharge",
    "pentad_in_year",
    "decad_in_year",
    "issue_date",
]

_HYDROGRAPH_DECAD_COLS = [
    "code",
    "date",
    "discharge_avg",
    "discharge",
    "decad_in_year",
    "pentad_in_year",
    "issue_date",
]

_TS_PENTAD_COLS = [
    "code",
    "date",
    "discharge_avg",
    "discharge_sum",
    "discharge",
    "pentad_in_year",
    "issue_date",
]

_TS_DECAD_COLS = [
    "code",
    "date",
    "discharge_avg",
    "discharge_sum",
    "discharge",
    "decad_in_year",
    "issue_date",
]


def _make_linreg_pentad_df(year: int = 2024) -> pd.DataFrame:
    """Return a minimal DataFrame accepted by write_linreg_pentad_forecast_data."""
    return pd.DataFrame(
        {
            "code": ["15013"],
            "date": pd.to_datetime([f"{year}-01-05"]),
            "discharge_sum": [100.0],
            "discharge_avg": [50.0],
            "pentad_in_year": [1],
            "predictor": [100.0],
            "issue_date": [True],
            "discharge": [100.0],
            "forecasted_discharge": [55.0],
        }
    )


def _make_linreg_decad_df(year: int = 2024) -> pd.DataFrame:
    """Return a minimal DataFrame accepted by write_linreg_decad_forecast_data."""
    return pd.DataFrame(
        {
            "code": ["15013"],
            "date": pd.to_datetime([f"{year}-01-10"]),
            "discharge_sum": [100.0],
            "discharge_avg": [50.0],
            "decad_in_year": [1],
            "predictor": [100.0],
            "issue_date": [True],
            "discharge": [100.0],
            "forecasted_discharge": [55.0],
        }
    )


def _make_hydrograph_df(years=(2023, 2024), pentad=True) -> pd.DataFrame:
    """Return a multi-year DataFrame for hydrograph write functions."""
    rows = []
    for yr in years:
        month = 1
        day = 5 if pentad else 10
        rows.append(
            {
                "code": "15013",
                "date": pd.Timestamp(yr, month, day),
                "discharge_avg": 50.0,
                "discharge_sum": 100.0,
                "discharge": 100.0,
                "pentad_in_year": 1,
                "decad_in_year": 1,
                "issue_date": True,
            }
        )
    return pd.DataFrame(rows)


def _make_ts_df(pentad=True) -> pd.DataFrame:
    """Return a minimal DataFrame for time-series write functions."""
    return pd.DataFrame(
        {
            "code": ["15013"],
            "date": pd.to_datetime(["2024-01-05"]),
            "discharge_avg": [50.0],
            "discharge_sum": [100.0],
            "discharge": [100.0],
            "pentad_in_year": [1],
            "decad_in_year": [1],
            "issue_date": [True],
        }
    )


# ── env setup helpers ──────────────────────────────────────────────────────────


def _set_linreg_pentad_env(monkeypatch, tmp_path):
    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    monkeypatch.setenv("ieasyforecast_analysis_pentad_file", "analysis_pentad.csv")
    monkeypatch.setenv("ieasyforecast_analysis_pentad_file_latest", "analysis_pentad_latest.csv")
    monkeypatch.setenv("SAPPHIRE_API_FAILURE_MODE", "warn")


def _set_linreg_decad_env(monkeypatch, tmp_path):
    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    monkeypatch.setenv("ieasyforecast_analysis_decad_file", "analysis_decad.csv")
    monkeypatch.setenv("ieasyforecast_analysis_decad_file_latest", "analysis_decad_latest.csv")
    monkeypatch.setenv("SAPPHIRE_API_FAILURE_MODE", "warn")


def _set_hydrograph_pentad_env(monkeypatch, tmp_path):
    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    monkeypatch.setenv("ieasyforecast_hydrograph_pentad_file", "hydrograph_pentad.csv")
    monkeypatch.setenv("SAPPHIRE_API_FAILURE_MODE", "warn")


def _set_hydrograph_decad_env(monkeypatch, tmp_path):
    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    monkeypatch.setenv("ieasyforecast_hydrograph_decad_file", "hydrograph_decad.csv")
    monkeypatch.setenv("SAPPHIRE_API_FAILURE_MODE", "warn")


def _set_ts_pentad_env(monkeypatch, tmp_path):
    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    monkeypatch.setenv("ieasyforecast_pentad_discharge_file", "pentad_discharge.csv")
    monkeypatch.setenv("SAPPHIRE_API_FAILURE_MODE", "warn")


def _set_ts_decad_env(monkeypatch, tmp_path):
    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    monkeypatch.setenv("ieasyforecast_decad_discharge_file", "decad_discharge.csv")
    monkeypatch.setenv("SAPPHIRE_API_FAILURE_MODE", "warn")


# ============================================================================
# Unit tests: write_linreg_pentad_forecast_data
# ============================================================================


class TestWriteLinregPentadForecastDataReturnsBool:
    """write_linreg_pentad_forecast_data returns bool based on API exceptions."""

    def test_api_write_succeeds_returns_true(self, tmp_path, monkeypatch):
        """When _write_lr_forecast_to_api succeeds (no exception), return True."""
        _set_linreg_pentad_env(monkeypatch, tmp_path)
        data = _make_linreg_pentad_df()

        with patch.object(fl_module, "_write_lr_forecast_to_api", return_value=None) as mock_api:
            result = fl_module.write_linreg_pentad_forecast_data(
                data, forecast_date=dt.date(2024, 1, 5)
            )

        mock_api.assert_called_once()
        assert result is True

    def test_api_write_raises_returns_false(self, tmp_path, monkeypatch):
        """When _write_lr_forecast_to_api raises, return False (warn mode)."""
        _set_linreg_pentad_env(monkeypatch, tmp_path)
        monkeypatch.setenv("SAPPHIRE_API_FAILURE_MODE", "warn")
        data = _make_linreg_pentad_df()

        with patch.object(
            fl_module, "_write_lr_forecast_to_api", side_effect=Exception("API down")
        ):
            result = fl_module.write_linreg_pentad_forecast_data(
                data, forecast_date=dt.date(2024, 1, 5)
            )

        assert result is False
        # CSV was still written
        csv_path = tmp_path / "analysis_pentad.csv"
        assert csv_path.exists()

    def test_no_data_for_api_returns_true(self, tmp_path, monkeypatch):
        """When api_data=empty DataFrame, API call is skipped and True is returned."""
        _set_linreg_pentad_env(monkeypatch, tmp_path)
        data = _make_linreg_pentad_df()

        with patch.object(fl_module, "_write_lr_forecast_to_api") as mock_api:
            result = fl_module.write_linreg_pentad_forecast_data(
                data, api_data=pd.DataFrame(), forecast_date=dt.date(2024, 1, 5)
            )

        mock_api.assert_not_called()
        assert result is True


# ============================================================================
# Unit tests: write_linreg_decad_forecast_data
# ============================================================================


class TestWriteLinregDecadForecastDataReturnsBool:
    """write_linreg_decad_forecast_data returns bool based on API exceptions."""

    def test_api_write_succeeds_returns_true(self, tmp_path, monkeypatch):
        """When _write_lr_forecast_to_api succeeds (no exception), return True."""
        _set_linreg_decad_env(monkeypatch, tmp_path)
        data = _make_linreg_decad_df()

        with patch.object(fl_module, "_write_lr_forecast_to_api", return_value=None) as mock_api:
            result = fl_module.write_linreg_decad_forecast_data(
                data, forecast_date=dt.date(2024, 1, 10)
            )

        mock_api.assert_called_once()
        assert result is True

    def test_api_write_raises_returns_false(self, tmp_path, monkeypatch):
        """When _write_lr_forecast_to_api raises, return False (warn mode)."""
        _set_linreg_decad_env(monkeypatch, tmp_path)
        monkeypatch.setenv("SAPPHIRE_API_FAILURE_MODE", "warn")
        data = _make_linreg_decad_df()

        with patch.object(
            fl_module, "_write_lr_forecast_to_api", side_effect=Exception("API down")
        ):
            result = fl_module.write_linreg_decad_forecast_data(
                data, forecast_date=dt.date(2024, 1, 10)
            )

        assert result is False
        # CSV was still written
        csv_path = tmp_path / "analysis_decad.csv"
        assert csv_path.exists()

    def test_no_data_for_api_returns_true(self, tmp_path, monkeypatch):
        """When api_data=empty DataFrame, API call is skipped and True is returned."""
        _set_linreg_decad_env(monkeypatch, tmp_path)
        data = _make_linreg_decad_df()

        with patch.object(fl_module, "_write_lr_forecast_to_api") as mock_api:
            result = fl_module.write_linreg_decad_forecast_data(
                data, api_data=pd.DataFrame(), forecast_date=dt.date(2024, 1, 10)
            )

        mock_api.assert_not_called()
        assert result is True


# ============================================================================
# Unit tests: write_pentad_hydrograph_data
# ============================================================================


class TestWritePentadHydrographDataReturnsBool:
    """write_pentad_hydrograph_data returns bool based on API exceptions."""

    def test_api_write_succeeds_returns_true(self, tmp_path, monkeypatch):
        """When _write_hydrograph_to_api succeeds (no exception), return True."""
        _set_hydrograph_pentad_env(monkeypatch, tmp_path)
        data = _make_hydrograph_df(years=(2023, 2024), pentad=True)

        with patch.object(fl_module, "_write_hydrograph_to_api", return_value=None) as mock_api:
            result = fl_module.write_pentad_hydrograph_data(data)

        mock_api.assert_called_once()
        assert result is True

    def test_api_write_raises_returns_false(self, tmp_path, monkeypatch):
        """When _write_hydrograph_to_api raises, return False (warn mode)."""
        _set_hydrograph_pentad_env(monkeypatch, tmp_path)
        monkeypatch.setenv("SAPPHIRE_API_FAILURE_MODE", "warn")
        data = _make_hydrograph_df(years=(2023, 2024), pentad=True)

        with patch.object(fl_module, "_write_hydrograph_to_api", side_effect=Exception("API down")):
            result = fl_module.write_pentad_hydrograph_data(data)

        assert result is False
        # CSV was still written despite API failure
        csv_path = tmp_path / "hydrograph_pentad.csv"
        assert csv_path.exists()


# ============================================================================
# Unit tests: write_decad_hydrograph_data
# ============================================================================


class TestWriteDecadHydrographDataReturnsBool:
    """write_decad_hydrograph_data returns bool based on API exceptions."""

    def test_api_write_succeeds_returns_true(self, tmp_path, monkeypatch):
        """When _write_hydrograph_to_api succeeds (no exception), return True."""
        _set_hydrograph_decad_env(monkeypatch, tmp_path)
        data = _make_hydrograph_df(years=(2023, 2024), pentad=False)

        with patch.object(fl_module, "_write_hydrograph_to_api", return_value=None) as mock_api:
            result = fl_module.write_decad_hydrograph_data(data)

        mock_api.assert_called_once()
        assert result is True

    def test_api_write_raises_returns_false(self, tmp_path, monkeypatch):
        """When _write_hydrograph_to_api raises, return False (warn mode)."""
        _set_hydrograph_decad_env(monkeypatch, tmp_path)
        monkeypatch.setenv("SAPPHIRE_API_FAILURE_MODE", "warn")
        data = _make_hydrograph_df(years=(2023, 2024), pentad=False)

        with patch.object(fl_module, "_write_hydrograph_to_api", side_effect=Exception("API down")):
            result = fl_module.write_decad_hydrograph_data(data)

        assert result is False
        csv_path = tmp_path / "hydrograph_decad.csv"
        assert csv_path.exists()


# ============================================================================
# Unit tests: write_pentad_time_series_data
# ============================================================================


class TestWritePentadTimeSeriesDataReturnsBool:
    """write_pentad_time_series_data returns bool based on API exceptions."""

    def test_api_write_succeeds_returns_true(self, tmp_path, monkeypatch):
        """When _write_runoff_to_api succeeds (no exception), return True."""
        _set_ts_pentad_env(monkeypatch, tmp_path)
        data = _make_ts_df(pentad=True)

        with patch.object(fl_module, "_write_runoff_to_api", return_value=None) as mock_api:
            result = fl_module.write_pentad_time_series_data(data)

        mock_api.assert_called_once()
        assert result is True

    def test_api_write_raises_returns_false(self, tmp_path, monkeypatch):
        """When _write_runoff_to_api raises, return False (warn mode)."""
        _set_ts_pentad_env(monkeypatch, tmp_path)
        monkeypatch.setenv("SAPPHIRE_API_FAILURE_MODE", "warn")
        data = _make_ts_df(pentad=True)

        with patch.object(fl_module, "_write_runoff_to_api", side_effect=Exception("API down")):
            result = fl_module.write_pentad_time_series_data(data)

        assert result is False
        csv_path = tmp_path / "pentad_discharge.csv"
        assert csv_path.exists()


# ============================================================================
# Unit tests: write_decad_time_series_data
# ============================================================================


class TestWriteDecadTimeSeriesDataReturnsBool:
    """write_decad_time_series_data returns bool based on API exceptions."""

    def test_api_write_succeeds_returns_true(self, tmp_path, monkeypatch):
        """When _write_runoff_to_api succeeds (no exception), return True."""
        _set_ts_decad_env(monkeypatch, tmp_path)
        data = _make_ts_df(pentad=False)

        with patch.object(fl_module, "_write_runoff_to_api", return_value=None) as mock_api:
            result = fl_module.write_decad_time_series_data(data)

        mock_api.assert_called_once()
        assert result is True

    def test_api_write_raises_returns_false(self, tmp_path, monkeypatch):
        """When _write_runoff_to_api raises, return False (warn mode)."""
        _set_ts_decad_env(monkeypatch, tmp_path)
        monkeypatch.setenv("SAPPHIRE_API_FAILURE_MODE", "warn")
        data = _make_ts_df(pentad=False)

        with patch.object(fl_module, "_write_runoff_to_api", side_effect=Exception("API down")):
            result = fl_module.write_decad_time_series_data(data)

        assert result is False
        csv_path = tmp_path / "decad_discharge.csv"
        assert csv_path.exists()


# ============================================================================
# Integration helpers (mirrored from test_integration_main.py)
# ============================================================================


def _make_fake_site(code="15013"):
    site = MagicMock()
    site.code = code
    return site


def _make_data_df(codes=None):
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
    pd_obj = MagicMock()
    pd_obj.pentad = [dt.date(2024, 1, 1), dt.date(2024, 1, 5)]
    pd_obj.decad = [dt.date(2024, 1, 1), dt.date(2024, 1, 10)]
    return pd_obj


class _FakeDate(dt.date):
    _today = dt.date(2024, 1, 5)

    @classmethod
    def today(cls):
        return cls._today


def _make_fake_date(target):
    class PinnedDate(_FakeDate):
        _today = target

    return PinnedDate


_BASE_ENV = {
    "ieasyhydroforecast_ssh_to_iEH": "False",
    "ieasyhydroforecast_connect_to_iEH": "False",
    "ieasyforecast_intermediate_data_path": "/tmp/test",
    "ieasyforecast_last_successful_run_file": "/tmp/test/last_run.txt",
}


def _setup_common_mocks(mock_sl, mock_fl, mock_tl):
    """Configure the standard mock behavior for a successful pipeline run."""
    mock_sl.load_environment.return_value = None
    mock_sl.check_database_access.return_value = False
    mock_sl.ForecastFlags.from_forecast_date_get_flags.return_value = MagicMock(
        pentad=True, decad=True
    )

    fake_sites = [_make_fake_site("15013")]
    mock_sl.get_pentadal_forecast_sites.return_value = (fake_sites, ["15013"])
    mock_sl.get_decadal_forecast_sites_from_pentadal_sites.return_value = (fake_sites, ["15013"])

    data = _make_data_df()
    mock_fl.get_pentadal_and_decadal_data.return_value = (data, data)
    mock_fl.filter_discharge_data_for_code_and_date.return_value = data
    mock_fl.get_predictor_dates.return_value = _make_predictor_dates()
    mock_fl.get_predictors.return_value = None
    mock_fl.save_discharge_avg.return_value = None
    mock_fl.perform_linear_regression.return_value = _make_linreg_df()
    mock_fl.perform_forecast.return_value = None
    mock_fl.parse_dates_robust.return_value = pd.to_datetime(pd.Series(["2024-01-05"]))
    mock_fl.write_linreg_pentad_forecast_data.return_value = True
    mock_fl.write_linreg_decad_forecast_data.return_value = True
    mock_fl.write_pentad_hydrograph_data.return_value = True
    mock_fl.write_pentad_time_series_data.return_value = True
    mock_fl.write_decad_hydrograph_data.return_value = True
    mock_fl.write_decad_time_series_data.return_value = True

    mock_tl.get_pentad_in_year.return_value = 1
    mock_tl.get_decad_in_year.return_value = 1


# ============================================================================
# Integration tests: exit code behavior
# ============================================================================


class TestApiWriteFailureExitCode:
    """Integration: verify pipeline exit code reflects API write failures."""

    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_api_write_failure_exits_nonzero(self, mock_sl, mock_fl, mock_tl, capsys):
        """When a write function returns False, main() exits with code 1."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl)

        # Make one of the write functions signal failure
        mock_fl.write_pentad_hydrograph_data.return_value = False

        fake_date = _make_fake_date(dt.date(2024, 1, 5))
        env = {**_BASE_ENV, "SAPPHIRE_PREDICTION_MODE": "PENTAD"}

        with (
            patch("sys.argv", ["linear_regression.py"]),
            patch.dict(os.environ, env, clear=False),
            patch.object(lr_module.dt, "date", fake_date),
        ):
            with pytest.raises(SystemExit) as exc_info:
                lr_module.main()

        assert exc_info.value.code == 1

    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_linreg_write_failure_exits_nonzero(self, mock_sl, mock_fl, mock_tl):
        """When write_linreg_pentad_forecast_data returns False, main() exits 1."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl)

        # Fail the forecast-data write (happens inside the per-day loop)
        mock_fl.write_linreg_pentad_forecast_data.return_value = False

        fake_date = _make_fake_date(dt.date(2024, 1, 5))
        env = {**_BASE_ENV, "SAPPHIRE_PREDICTION_MODE": "PENTAD"}

        with (
            patch("sys.argv", ["linear_regression.py"]),
            patch.dict(os.environ, env, clear=False),
            patch.object(lr_module.dt, "date", fake_date),
        ):
            with pytest.raises(SystemExit) as exc_info:
                lr_module.main()

        assert exc_info.value.code == 1

    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_all_writes_succeed_exits_zero(self, mock_sl, mock_fl, mock_tl):
        """When all write functions return True, main() exits with code 0."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl)
        # All writes return True by default from _setup_common_mocks

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

    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_multiple_write_failures_exit_nonzero(self, mock_sl, mock_fl, mock_tl):
        """When multiple write functions return False, main() still exits 1 (not >1)."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl)

        mock_fl.write_pentad_hydrograph_data.return_value = False
        mock_fl.write_pentad_time_series_data.return_value = False
        mock_fl.write_linreg_pentad_forecast_data.return_value = False

        fake_date = _make_fake_date(dt.date(2024, 1, 5))
        env = {**_BASE_ENV, "SAPPHIRE_PREDICTION_MODE": "PENTAD"}

        with (
            patch("sys.argv", ["linear_regression.py"]),
            patch.dict(os.environ, env, clear=False),
            patch.object(lr_module.dt, "date", fake_date),
        ):
            with pytest.raises(SystemExit) as exc_info:
                lr_module.main()

        assert exc_info.value.code == 1

    @patch.object(lr_module, "tl")
    @patch.object(lr_module, "fl")
    @patch.object(lr_module, "sl")
    def test_decad_write_failure_exits_nonzero(self, mock_sl, mock_fl, mock_tl):
        """Decad write failure also triggers exit code 1."""
        _setup_common_mocks(mock_sl, mock_fl, mock_tl)

        mock_fl.write_decad_hydrograph_data.return_value = False

        # Jan 10 is both a pentad and decad day
        fake_date = _make_fake_date(dt.date(2024, 1, 10))
        env = {**_BASE_ENV, "SAPPHIRE_PREDICTION_MODE": "BOTH"}

        with (
            patch("sys.argv", ["linear_regression.py"]),
            patch.dict(os.environ, env, clear=False),
            patch.object(lr_module.dt, "date", fake_date),
        ):
            with pytest.raises(SystemExit) as exc_info:
                lr_module.main()

        assert exc_info.value.code == 1
