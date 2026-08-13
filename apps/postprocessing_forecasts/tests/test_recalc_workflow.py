"""Tests for recalculate_skill_metrics.py — yearly entry point."""

import importlib.util
import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import src.horizon_config as _real_horizon_config

SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, SCRIPT_DIR)


def import_recalc_module():
    """Import the recalculate_skill_metrics module."""
    spec = importlib.util.spec_from_file_location(
        "recalculate_skill_metrics_module",
        os.path.join(SCRIPT_DIR, "recalculate_skill_metrics.py"),
    )
    module = importlib.util.module_from_spec(spec)
    return module, spec


@pytest.fixture
def mock_data():
    return pd.DataFrame(
        {
            "code": ["10001"],
            "date": pd.to_datetime(["2024-01-05"]),
            "forecasted_discharge": [100.0],
        }
    )


@pytest.fixture
def mock_skill():
    return pd.DataFrame(
        {
            "pentad_in_year": [1],
            "code": ["10001"],
            "sdivsigma": [0.3],
        }
    )


@pytest.fixture
def mock_monthly_obs():
    return pd.DataFrame(
        {
            "code": ["10001"],
            "year": [2025],
            "month": [6],
            "month_in_year": [6],
            "discharge_avg": [50.0],
            "delta": [5.0],
        }
    )


@pytest.fixture
def mock_monthly_forecasts():
    return pd.DataFrame(
        {
            "code": ["10001"],
            "year": [2025],
            "month": [6],
            "model_short": ["GBT"],
            "q50": [52.0],
            "q05": [40.0],
            "q10": [42.0],
            "q25": [46.0],
            "q75": [58.0],
            "q90": [62.0],
            "q95": [65.0],
        }
    )


@pytest.fixture
def mock_monthly_skill():
    return pd.DataFrame(
        {
            "month_in_year": [6],
            "code": ["10001"],
            "model_short": ["GBT"],
            "sdivsigma": [0.4],
            "nse": [0.85],
            "delta": [5.0],
            "accuracy": [0.9],
            "mae": [3.0],
            "n_pairs": [5],
            "crps": [8.0],
        }
    )


def _setup_mocks(mock_data, mock_skill):
    """Set up mocks for the recalculate module."""
    mock_sl = MagicMock()
    mock_skill_metrics = MagicMock()
    mock_file_writer = MagicMock()
    mock_data_reader = MagicMock()

    mock_sl.load_environment.return_value = None
    mock_sl.calculate_virtual_stations_data.side_effect = lambda x: x
    mock_sl.calculate_neural_ensemble_forecast.side_effect = lambda x: x
    mock_sl.calculate_neural_ensemble_forecast_decade.side_effect = lambda x: x

    mock_data_reader.read_observed_and_modelled_data.return_value = (mock_data, mock_data)

    mock_skill_metrics.calculate_skill_metrics.return_value = (mock_skill, mock_data, None)
    mock_file_writer.save_forecast_data.return_value = None
    mock_file_writer.save_skill_metrics.return_value = True
    mock_file_writer.save_monthly_forecast_data.return_value = None
    mock_file_writer.save_monthly_skill_metrics.return_value = True

    # Quarterly/seasonal mocks
    mock_data_reader.read_quarterly_observations.return_value = pd.DataFrame()
    mock_data_reader.read_quarterly_forecasts.return_value = pd.DataFrame()
    mock_data_reader.read_seasonal_observations.return_value = pd.DataFrame()
    mock_data_reader.read_seasonal_forecasts.return_value = pd.DataFrame()
    mock_skill_metrics.calculate_quarterly_skill_metrics.return_value = (
        pd.DataFrame(),
        pd.DataFrame(),
        None,
    )
    mock_skill_metrics.calculate_seasonal_skill_metrics.return_value = (
        pd.DataFrame(),
        pd.DataFrame(),
        None,
    )
    mock_file_writer.save_quarterly_forecast_data.return_value = None
    mock_file_writer.save_quarterly_skill_metrics.return_value = True
    mock_file_writer.save_seasonal_forecast_data.return_value = None
    mock_file_writer.save_seasonal_skill_metrics.return_value = True

    mock_pt_module = MagicMock()
    mock_pt_module.TimingStats.return_value.summary.return_value = ([], 0)

    mock_src = MagicMock()
    mock_src.skill_metrics = mock_skill_metrics
    mock_src.file_writer = mock_file_writer
    mock_src.data_reader = mock_data_reader

    sys.modules["setup_library"] = mock_sl
    sys.modules["tag_library"] = MagicMock()
    sys.modules["src"] = mock_src
    sys.modules["src.skill_metrics"] = mock_skill_metrics
    sys.modules["src.file_writer"] = mock_file_writer
    sys.modules["src.data_reader"] = mock_data_reader
    sys.modules["src.postprocessing_tools"] = mock_pt_module
    sys.modules["src.horizon_config"] = _real_horizon_config

    return {
        "sl": mock_sl,
        "skill_metrics": mock_skill_metrics,
        "file_writer": mock_file_writer,
        "data_reader": mock_data_reader,
    }


def _set_long_term_env(monkeypatch, tmp_path, modes):
    config_dir = tmp_path / "long_term"
    config_dir.mkdir()
    monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_configuration", "long_term")
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", ",".join(modes))
    for name, lead in {
        "seasonal_january": 3,
        "seasonal_february": 2,
        "seasonal_march": 1,
        "seasonal_april": 0,
    }.items():
        if name in modes:
            (config_dir / f"{name}.json").write_text(f'{{"operational_month_lead_time": {lead}}}')


class TestRecalcWorkflow:
    """Tests for the yearly recalculation entry point."""

    def test_calls_calculate_skill_metrics(self, mock_data, mock_skill):
        """Recalc calls skill_metrics.calculate_skill_metrics (the slow path)."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001"])

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 0
                mocks["skill_metrics"].calculate_skill_metrics.assert_called_once()
                mocks["file_writer"].save_skill_metrics.assert_called_once()

    def test_saves_skill_metrics(self, mock_data, mock_skill):
        """Recalc saves skill metrics (not just forecasts)."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "BOTH"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001"])

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 0
                assert mocks["file_writer"].save_skill_metrics.call_count == 2

    def test_both_mode_processes_both(self, mock_data, mock_skill):
        """BOTH mode processes pentad and decad."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "BOTH"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001"])

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 0
                assert mocks["skill_metrics"].calculate_skill_metrics.call_count == 2

    def test_save_error_accumulation(self, mock_data, mock_skill):
        """Save errors cause exit code 1."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)
                mocks["file_writer"].save_skill_metrics.return_value = False

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001"])

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 1

    def test_invalid_mode_exits_with_error(self, mock_data, mock_skill):
        """Invalid SAPPHIRE_PREDICTION_MODE exits with code 1."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "INVALID"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001"])

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 1
                # No calculation should have occurred
                mocks["skill_metrics"].calculate_skill_metrics.assert_not_called()

    def test_decad_only_mode(self, mock_data, mock_skill):
        """DECAD mode only recalculates decad metrics."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "DECAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001"])

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 0
                mocks["skill_metrics"].calculate_skill_metrics.assert_called_once()
                mocks["file_writer"].save_skill_metrics.assert_called_once()


class TestRecalcEdgeCases:
    """Edge case tests for recalculate entry point branches."""

    def test_load_environment_failure_propagates(self, mock_data, mock_skill):
        """When load_environment() raises, exception propagates uncaught."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)
                mocks["sl"].load_environment.side_effect = FileNotFoundError("missing .env")

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001"])

                with pytest.raises(FileNotFoundError, match="missing .env"):
                    module.recalculate_skill_metrics()

    def test_save_success_path(self, mock_data, mock_skill):
        """All saves succeed → exit 0, all four saves called.

        save_forecast_data is an unconverted sibling and still returns
        None (success under `if ret is None:`); save_skill_metrics has
        been converted (PP-051 P2) and returns True (success under
        `if ret is False:`) via _setup_mocks' default.
        """
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "BOTH"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001"])

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 0
                # Both pentad + decad saves called (2 each)
                assert mocks["file_writer"].save_forecast_data.call_count == 2
                assert mocks["file_writer"].save_skill_metrics.call_count == 2


class TestRecalcMonthly:
    """Tests for monthly skill metrics recalculation."""

    def test_monthly_mode_calls_monthly_pipeline(
        self,
        mock_data,
        mock_skill,
        mock_monthly_obs,
        mock_monthly_forecasts,
        mock_monthly_skill,
    ):
        """MONTHLY mode reads obs + forecasts, calculates, and saves."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "MONTHLY"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)

                mocks["data_reader"].read_monthly_observations.return_value = mock_monthly_obs
                mocks["data_reader"].read_monthly_forecasts.return_value = mock_monthly_forecasts
                mocks["skill_metrics"].calculate_monthly_skill_metrics.return_value = (
                    mock_monthly_skill,
                    pd.DataFrame(),
                    None,
                )

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)

                # Patch _read_station_codes to return test codes
                module._read_station_codes = MagicMock(return_value=["10001"])

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 0

                # Monthly pipeline called
                mocks["data_reader"].read_monthly_observations.assert_called_once()
                mocks["data_reader"].read_monthly_forecasts.assert_called_once()
                mocks["skill_metrics"].calculate_monthly_skill_metrics.assert_called_once()
                mocks["file_writer"].save_monthly_skill_metrics.assert_called_once()

                # Pentad/decad NOT called
                mocks["skill_metrics"].calculate_skill_metrics.assert_not_called()

    def test_all_mode_runs_pentad_decad_and_monthly(
        self,
        mock_data,
        mock_skill,
        mock_monthly_obs,
        mock_monthly_forecasts,
        mock_monthly_skill,
    ):
        """ALL mode runs pentad + decad + monthly + daily."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "ALL"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)

                mocks["data_reader"].read_monthly_observations.return_value = mock_monthly_obs
                mocks["data_reader"].read_monthly_forecasts.return_value = mock_monthly_forecasts
                mocks["skill_metrics"].calculate_monthly_skill_metrics.return_value = (
                    mock_monthly_skill,
                    pd.DataFrame(),
                    None,
                )

                # Daily mocks
                mocks["data_reader"].read_daily_observations.return_value = pd.DataFrame(
                    columns=["code", "date", "discharge_avg"]
                )
                mocks["data_reader"].read_daily_forecasts.return_value = pd.DataFrame(
                    columns=["code", "date", "model_short", "forecasted_discharge"]
                )
                mocks["skill_metrics"].calculate_daily_skill_metrics.return_value = (
                    pd.DataFrame(),
                    pd.DataFrame(),
                )

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)

                module._read_station_codes = MagicMock(return_value=["10001"])
                module._supported_seasonal_issue_leads = MagicMock(return_value=[])

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 0

                # All four pipelines called
                assert mocks["skill_metrics"].calculate_skill_metrics.call_count == 2
                mocks["skill_metrics"].calculate_monthly_skill_metrics.assert_called_once()
                mocks["skill_metrics"].calculate_daily_skill_metrics.assert_called_once()
                assert mocks["file_writer"].save_skill_metrics.call_count == 2
                mocks["file_writer"].save_monthly_skill_metrics.assert_called_once()

    def test_both_mode_does_not_run_monthly(self, mock_data, mock_skill):
        """BOTH mode runs pentad + decad only (backward compat)."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "BOTH"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001"])

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 0
                mocks["skill_metrics"].calculate_monthly_skill_metrics.assert_not_called()
                mocks["file_writer"].save_monthly_skill_metrics.assert_not_called()

    def test_monthly_save_error_causes_exit_1(
        self,
        mock_data,
        mock_skill,
        mock_monthly_obs,
        mock_monthly_forecasts,
        mock_monthly_skill,
    ):
        """Monthly save error causes exit code 1."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "MONTHLY"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)

                mocks["data_reader"].read_monthly_observations.return_value = mock_monthly_obs
                mocks["data_reader"].read_monthly_forecasts.return_value = mock_monthly_forecasts
                mocks["skill_metrics"].calculate_monthly_skill_metrics.return_value = (
                    mock_monthly_skill,
                    pd.DataFrame(),
                    None,
                )
                mocks["file_writer"].save_monthly_skill_metrics.return_value = False

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)

                module._read_station_codes = MagicMock(return_value=["10001"])

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 1

    def test_monthly_year_range_passed_to_readers(
        self,
        mock_data,
        mock_skill,
        mock_monthly_obs,
        mock_monthly_forecasts,
        mock_monthly_skill,
    ):
        """Year range is passed correctly to data readers."""
        with patch.dict(
            os.environ,
            {
                "SAPPHIRE_PREDICTION_MODE": "MONTHLY",
                "SAPPHIRE_RECALC_START_YEAR": "2020",
                "SAPPHIRE_RECALC_END_YEAR": "2025",
            },
        ):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)

                mocks["data_reader"].read_monthly_observations.return_value = mock_monthly_obs
                mocks["data_reader"].read_monthly_forecasts.return_value = mock_monthly_forecasts
                mocks["skill_metrics"].calculate_monthly_skill_metrics.return_value = (
                    mock_monthly_skill,
                    pd.DataFrame(),
                    None,
                )

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)

                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 0

                # Verify year range
                obs_call = mocks["data_reader"].read_monthly_observations.call_args
                assert obs_call[0][1] == 2020  # start_year
                assert obs_call[0][2] == 2025  # end_year

                fc_call = mocks["data_reader"].read_monthly_forecasts.call_args
                assert fc_call[0][1] == 2020
                assert fc_call[0][2] == 2025

                # Verify codes
                assert obs_call[0][0] == ["10001", "10002"]

    def test_empty_monthly_observations_skips_gracefully(
        self,
        mock_data,
        mock_skill,
        mock_monthly_forecasts,
        mock_monthly_skill,
    ):
        """Empty monthly observations skip calculation gracefully."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "MONTHLY"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)

                # Empty observations
                mocks["data_reader"].read_monthly_observations.return_value = pd.DataFrame()
                mocks["data_reader"].read_monthly_forecasts.return_value = mock_monthly_forecasts
                # calculate_monthly_skill_metrics handles empty inputs
                mocks["skill_metrics"].calculate_monthly_skill_metrics.return_value = (
                    pd.DataFrame(),
                    pd.DataFrame(),
                    None,
                )

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)

                module._read_station_codes = MagicMock(return_value=["10001"])

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                # Should still exit 0 (empty is not an error)
                assert exc_info.value.code == 0
                mocks["skill_metrics"].calculate_monthly_skill_metrics.assert_called_once()


class TestRecalcQuarterlySeasonalSkillFailure:
    """PP-051 P1: save_quarterly_skill_metrics / save_seasonal_skill_metrics
    now return True/False (Contract 7) instead of bare None. Their call
    sites must convert that into the shared errors-list/exit-code
    mechanism via the pinned `if ret is False:` predicate (Contract 10) --
    not the old, dead `if ret is None:` branch. These two functions are
    API-only (no CSV fallback), so a swallowed failure here was total loss
    of that recalc's output for the horizon."""

    def test_quarterly_save_failure_causes_exit_1(self, mock_data, mock_skill):
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "QUARTERLY"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)
                mocks["file_writer"].save_quarterly_skill_metrics.return_value = False

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001"])

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 1

    def test_quarterly_save_success_exit_0(self, mock_data, mock_skill):
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "QUARTERLY"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)
                mocks["file_writer"].save_quarterly_skill_metrics.return_value = True

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001"])

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 0

    def test_quarterly_failure_message_does_not_print_bare_bool(
        self, mock_data, mock_skill, capsys
    ):
        """Contract 9: the logged error and the errors-list entry must
        reference the mode/horizon and point at the detail logged one
        level down, not interpolate the discarded bool directly (a bare
        `False` has no diagnostic value on the line an operator reads
        first).

        Uses capsys (stderr), not caplog: recalculate_skill_metrics.py
        does `logger.handlers = []` on the ROOT logger at import time
        (its own logging setup, unrelated to this fix) -- since this test
        harness reloads the module fresh per test via exec_module, that
        wipes caplog's handler every time. The console handler it installs
        instead still writes to stderr, which capsys captures.
        """
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "QUARTERLY"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)
                mocks["file_writer"].save_quarterly_skill_metrics.return_value = False

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001"])

                with pytest.raises(SystemExit):
                    module.recalculate_skill_metrics()

        stderr_lines = [
            line for line in capsys.readouterr().err.splitlines() if "quarterly" in line.lower()
        ]
        assert stderr_lines, "expected a quarterly-skill-metrics error log line"
        assert not any(line.strip().endswith("False") for line in stderr_lines)

    def test_seasonal_save_failure_causes_exit_1(
        self, mock_data, mock_skill, monkeypatch, tmp_path
    ):
        # SEASONAL mode unconditionally calls _supported_seasonal_issue_leads()
        # (not flag-gated), which reads the long-term configuration env vars --
        # needs the same setup as TestRecalcSeasonalLeadSelection below.
        _set_long_term_env(monkeypatch, tmp_path, ["seasonal_april"])
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "SEASONAL"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)
                mocks["file_writer"].save_seasonal_skill_metrics.return_value = False

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001"])

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 1

    def test_seasonal_save_success_exit_0(self, mock_data, mock_skill, monkeypatch, tmp_path):
        _set_long_term_env(monkeypatch, tmp_path, ["seasonal_april"])
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "SEASONAL"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)
                mocks["file_writer"].save_seasonal_skill_metrics.return_value = True

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001"])

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 0

    def test_seasonal_failure_message_does_not_print_bare_bool(
        self, mock_data, mock_skill, monkeypatch, tmp_path, capsys
    ):
        """Contract 9, seasonal side of the same check (see the quarterly
        test above for why capsys is used instead of caplog)."""
        _set_long_term_env(monkeypatch, tmp_path, ["seasonal_april"])
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "SEASONAL"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)
                mocks["file_writer"].save_seasonal_skill_metrics.return_value = False

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001"])

                with pytest.raises(SystemExit):
                    module.recalculate_skill_metrics()

        stderr_lines = [
            line for line in capsys.readouterr().err.splitlines() if "seasonal" in line.lower()
        ]
        assert stderr_lines, "expected a seasonal-skill-metrics error log line"
        assert not any(line.strip().endswith("False") for line in stderr_lines)


class TestRecalcSeasonalLeadSelection:
    def test_seasonal_recalc_reads_each_supported_issue_lead(
        self, mock_data, mock_skill, monkeypatch, tmp_path
    ):
        _set_long_term_env(
            monkeypatch,
            tmp_path,
            [
                "seasonal_january",
                "seasonal_february",
                "seasonal_march",
                "seasonal_april",
            ],
        )
        with patch.dict(
            os.environ,
            {
                "SAPPHIRE_PREDICTION_MODE": "SEASONAL",
                "SAPPHIRE_RECALC_START_YEAR": "2024",
                "SAPPHIRE_RECALC_END_YEAR": "2024",
            },
        ):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)
                mocks["data_reader"].read_seasonal_observations.return_value = pd.DataFrame(
                    {"code": ["10001"], "season_year": [2024], "discharge_avg": [10.0]}
                )
                mocks["data_reader"].read_seasonal_forecasts.side_effect = [
                    pd.DataFrame(
                        {
                            "code": ["10001"],
                            "season_year": [2024],
                            "season_in_year": [lead],
                            "model_short": ["LR"],
                            "q50": [10.0],
                        }
                    )
                    for lead in [3, 2, 1, 0]
                ]

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001"])

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 0
                calls = mocks["data_reader"].read_seasonal_forecasts.call_args_list
                assert [call.kwargs["horizon_value"] for call in calls] == [3, 2, 1, 0]

    def test_seasonal_recalc_reads_only_april_issue_for_single_issue_deployment(
        self, mock_data, mock_skill, monkeypatch, tmp_path
    ):
        _set_long_term_env(monkeypatch, tmp_path, ["seasonal_april"])
        with patch.dict(
            os.environ,
            {
                "SAPPHIRE_PREDICTION_MODE": "SEASONAL",
                "SAPPHIRE_RECALC_START_YEAR": "2024",
                "SAPPHIRE_RECALC_END_YEAR": "2024",
            },
        ):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)
                mocks["data_reader"].read_seasonal_observations.return_value = pd.DataFrame(
                    {"code": ["10001"], "season_year": [2024], "discharge_avg": [10.0]}
                )
                mocks["data_reader"].read_seasonal_forecasts.return_value = pd.DataFrame(
                    {
                        "code": ["10001"],
                        "season_year": [2024],
                        "season_in_year": [0],
                        "model_short": ["LR"],
                        "q50": [10.0],
                    }
                )

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001"])

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 0
                calls = mocks["data_reader"].read_seasonal_forecasts.call_args_list
                assert [call.kwargs["horizon_value"] for call in calls] == [0]
