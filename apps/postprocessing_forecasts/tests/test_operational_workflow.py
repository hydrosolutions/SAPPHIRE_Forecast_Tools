"""Tests for postprocessing_operational.py — daily operational entry point."""

import importlib.util
import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, SCRIPT_DIR)

import src.horizon_config as _real_horizon_config


def import_operational_module():
    """Import the postprocessing_operational module."""
    spec = importlib.util.spec_from_file_location(
        "postprocessing_operational_module",
        os.path.join(SCRIPT_DIR, "postprocessing_operational.py"),
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
            "model_short": ["LR"],
            "sdivsigma": [0.3],
        }
    )


def _setup_mocks(prediction_mode, mock_data, mock_skill):
    """Set up mocks for the operational module."""
    mock_sl = MagicMock()
    mock_tl = MagicMock()
    mock_data_reader = MagicMock()
    mock_ensemble_calc = MagicMock()
    mock_skill_metrics = MagicMock()
    mock_file_writer = MagicMock()

    mock_sl.load_environment.return_value = None
    mock_sl.calculate_virtual_stations_data.side_effect = lambda x: x
    mock_sl.calculate_neural_ensemble_forecast.side_effect = lambda x: x
    mock_sl.calculate_neural_ensemble_forecast_decade.side_effect = lambda x: x

    mock_data_reader.read_observed_and_modelled_data.return_value = (mock_data, mock_data)

    mock_data_reader.read_skill_metrics.return_value = mock_skill
    mock_ensemble_calc.create_ensemble_forecasts.return_value = (mock_data, mock_skill)

    mock_file_writer.save_forecast_data.return_value = None

    mock_pt_module = MagicMock()
    mock_pt_module.TimingStats.return_value.summary.return_value = ([], 0)

    mock_src = MagicMock()
    mock_src.postprocessing_tools = mock_pt_module
    mock_src.data_reader = mock_data_reader
    mock_src.ensemble_calculator = mock_ensemble_calc
    mock_src.skill_metrics = mock_skill_metrics
    mock_src.file_writer = mock_file_writer

    sys.modules["setup_library"] = mock_sl
    sys.modules["tag_library"] = mock_tl
    sys.modules["src"] = mock_src
    sys.modules["src.postprocessing_tools"] = mock_pt_module
    sys.modules["src.data_reader"] = mock_data_reader
    sys.modules["src.ensemble_calculator"] = mock_ensemble_calc
    sys.modules["src.skill_metrics"] = mock_skill_metrics
    sys.modules["src.file_writer"] = mock_file_writer
    sys.modules["src.horizon_config"] = _real_horizon_config

    return {
        "sl": mock_sl,
        "pt": mock_pt_module,
        "data_reader": mock_data_reader,
        "ensemble_calc": mock_ensemble_calc,
        "skill_metrics": mock_skill_metrics,
        "file_writer": mock_file_writer,
    }


class TestOperationalWorkflow:
    """Tests for the operational entry point."""

    def test_pentad_mode_no_skill_recalc(self, mock_data, mock_skill):
        """PENTAD mode should NOT call calculate_skill_metrics."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks("PENTAD", mock_data, mock_skill)

                module, spec = import_operational_module()
                spec.loader.exec_module(module)
                module._read_station_codes = lambda config: ["10001"]
                module.is_pentad_boundary = lambda d: True

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0
                # Key assertion: no skill metric recalculation
                mocks["skill_metrics"].calculate_skill_metrics.assert_not_called()
                # But skill metrics were READ
                mocks["data_reader"].read_skill_metrics.assert_called()

    def test_decad_mode_works(self, mock_data, mock_skill):
        """DECAD mode processes decadal data only."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "DECAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks("DECAD", mock_data, mock_skill)

                module, spec = import_operational_module()
                spec.loader.exec_module(module)
                module._read_station_codes = lambda config: ["10001"]
                module.is_decad_boundary = lambda d: True

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0
                # DECAD mode: only "decad" horizon should be read
                calls = mocks["data_reader"].read_observed_and_modelled_data.call_args_list
                horizons = [c[0][0] for c in calls]
                assert "decad" in horizons
                assert "pentad" not in horizons

    def test_both_mode_processes_both(self, mock_data, mock_skill):
        """BOTH mode processes pentad and decad on a boundary day."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "BOTH"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks("BOTH", mock_data, mock_skill)

                module, spec = import_operational_module()
                spec.loader.exec_module(module)
                module._read_station_codes = lambda config: ["10001"]
                # Override boundary checks so both horizons process
                module.is_pentad_boundary = lambda d: True
                module.is_decad_boundary = lambda d: True

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0
                calls = mocks["data_reader"].read_observed_and_modelled_data.call_args_list
                horizons = [c[0][0] for c in calls]
                assert "pentad" in horizons
                assert "decad" in horizons

    def test_error_accumulation(self, mock_data, mock_skill):
        """Save errors are accumulated and cause exit code 1."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks("PENTAD", mock_data, mock_skill)
                mocks["file_writer"].save_forecast_data.return_value = "Error: write failed"

                module, spec = import_operational_module()
                spec.loader.exec_module(module)
                module._read_station_codes = lambda config: ["10001"]
                module.is_pentad_boundary = lambda d: True

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 1

    def test_empty_skill_metrics_skips_ensemble(self, mock_data):
        """When skill metrics are empty, ensemble creation is skipped."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                empty_skill = pd.DataFrame()
                mocks = _setup_mocks("PENTAD", mock_data, empty_skill)
                mocks["data_reader"].read_skill_metrics.return_value = pd.DataFrame()

                module, spec = import_operational_module()
                spec.loader.exec_module(module)
                module._read_station_codes = lambda config: ["10001"]
                module.is_pentad_boundary = lambda d: True

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0
                mocks["ensemble_calc"].create_ensemble_forecasts.assert_not_called()

    def test_monthly_mode_redirects_to_long_term(self, mock_data, mock_skill):
        """MONTHLY mode logs redirect and exits cleanly (no processing)."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "MONTHLY"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks("MONTHLY", mock_data, mock_skill)

                module, spec = import_operational_module()
                spec.loader.exec_module(module)
                module._read_station_codes = lambda config: ["10001"]
                module.is_pentad_boundary = lambda d: True
                module.is_decad_boundary = lambda d: True

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0
                # Monthly is now handled by the long-term entry point
                mocks["data_reader"].read_monthly_skill_metrics.assert_not_called()
                # Pentad/decad data NOT processed
                mocks["data_reader"].read_observed_and_modelled_data.assert_not_called()

    def test_all_mode_processes_pentad_and_decad(self, mock_data, mock_skill):
        """ALL mode processes pentad and decad on a boundary day."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "ALL"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks("ALL", mock_data, mock_skill)

                module, spec = import_operational_module()
                spec.loader.exec_module(module)
                module._read_station_codes = lambda config: ["10001"]
                # Override boundary checks so both horizons process
                module.is_pentad_boundary = lambda d: True
                module.is_decad_boundary = lambda d: True

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0
                calls = mocks["data_reader"].read_observed_and_modelled_data.call_args_list
                horizons = [c[0][0] for c in calls]
                assert "pentad" in horizons
                assert "decad" in horizons
                # Monthly is now handled by the long-term entry point
                mocks["data_reader"].read_monthly_skill_metrics.assert_not_called()

    def test_invalid_mode_exits_with_error(self, mock_data, mock_skill):
        """Invalid SAPPHIRE_PREDICTION_MODE exits with code 1."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "INVALID"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks("INVALID", mock_data, mock_skill)

                module, spec = import_operational_module()
                spec.loader.exec_module(module)
                module._read_station_codes = lambda config: ["10001"]
                module.is_pentad_boundary = lambda d: True
                module.is_decad_boundary = lambda d: True

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 1
                # No data processing should have occurred
                mocks["data_reader"].read_observed_and_modelled_data.assert_not_called()


class TestOperationalConcurrentErrors:
    """BOTH mode where one or both horizons fail."""

    def test_both_mode_pentad_fails_decad_succeeds(self, mock_data, mock_skill):
        """BOTH mode: pentad save fails, decad succeeds => exit 1."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "BOTH"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks("BOTH", mock_data, mock_skill)
                # Pentad is called first, then decad
                mocks["file_writer"].save_forecast_data.side_effect = [
                    "Error: pentad write failed",
                    None,
                ]

                module, spec = import_operational_module()
                spec.loader.exec_module(module)
                module._read_station_codes = lambda config: ["10001"]
                module.is_pentad_boundary = lambda d: True
                module.is_decad_boundary = lambda d: True

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 1

    def test_both_mode_pentad_succeeds_decad_fails(self, mock_data, mock_skill):
        """BOTH mode: pentad succeeds, decad save fails => exit 1."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "BOTH"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks("BOTH", mock_data, mock_skill)
                # Pentad is called first, then decad
                mocks["file_writer"].save_forecast_data.side_effect = [
                    None,
                    "Error: decad write failed",
                ]

                module, spec = import_operational_module()
                spec.loader.exec_module(module)
                module._read_station_codes = lambda config: ["10001"]
                module.is_pentad_boundary = lambda d: True
                module.is_decad_boundary = lambda d: True

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 1

    def test_both_mode_both_fail(self, mock_data, mock_skill):
        """BOTH mode: both saves fail => exit 1."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "BOTH"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks("BOTH", mock_data, mock_skill)
                # Pentad is called first, then decad
                mocks["file_writer"].save_forecast_data.side_effect = [
                    "Error: pentad write failed",
                    "Error: decad write failed",
                ]

                module, spec = import_operational_module()
                spec.loader.exec_module(module)
                module._read_station_codes = lambda config: ["10001"]
                module.is_pentad_boundary = lambda d: True
                module.is_decad_boundary = lambda d: True

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 1


class TestOperationalEdgeCases:
    """Edge case tests for operational entry point branches."""

    def test_load_environment_failure_propagates(self, mock_data, mock_skill):
        """When load_environment() raises, exception propagates uncaught."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks("PENTAD", mock_data, mock_skill)
                mocks["sl"].load_environment.side_effect = FileNotFoundError("missing .env")

                module, spec = import_operational_module()
                spec.loader.exec_module(module)
                module._read_station_codes = lambda config: ["10001"]
                module.is_pentad_boundary = lambda d: True

                with pytest.raises(FileNotFoundError, match="missing .env"):
                    module.postprocessing_operational()

    def test_empty_modelled_with_nonempty_skill(self, mock_data, mock_skill):
        """Empty observed/modelled + non-empty skill → ensemble still called."""
        empty_df = pd.DataFrame()
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks("PENTAD", mock_data, mock_skill)
                mocks["data_reader"].read_observed_and_modelled_data.return_value = (
                    empty_df,
                    empty_df,
                )

                module, spec = import_operational_module()
                spec.loader.exec_module(module)
                module._read_station_codes = lambda config: ["10001"]
                module.is_pentad_boundary = lambda d: True

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0
                # Ensemble creation called despite empty modelled data
                mocks["ensemble_calc"].create_ensemble_forecasts.assert_called_once()

    def test_save_success_path(self, mock_data, mock_skill):
        """Save returning None → exit 0, save was called."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks("PENTAD", mock_data, mock_skill)

                module, spec = import_operational_module()
                spec.loader.exec_module(module)
                module._read_station_codes = lambda config: ["10001"]
                module.is_pentad_boundary = lambda d: True

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0
                mocks["file_writer"].save_forecast_data.assert_called_once()


class TestBoundaryDaySkipBehavior:
    """Phase 2 (INFRA-006): Operational entry point skips processing
    when the current day is not a boundary day for the given horizon.

    Boundary days:
    - Pentad: 5, 10, 15, 20, 25, last day of month
    - Decad: 10, 20, last day of month
    """

    def test_pentad_skips_on_non_pentad_day(self, mock_data, mock_skill):
        """PENTAD mode on a non-pentad day (e.g., day 7) skips processing."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks("PENTAD", mock_data, mock_skill)

                module, spec = import_operational_module()
                spec.loader.exec_module(module)
                module._read_station_codes = lambda config: ["10001"]
                # Force non-pentad day
                module.is_pentad_boundary = lambda d: False

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0
                # No data reading or writing should occur
                mocks["data_reader"].read_observed_and_modelled_data.assert_not_called()
                mocks["file_writer"].save_forecast_data.assert_not_called()
                mocks["data_reader"].read_skill_metrics.assert_not_called()

    def test_decad_skips_on_non_decad_day(self, mock_data, mock_skill):
        """DECAD mode on a non-decad day skips processing."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "DECAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks("DECAD", mock_data, mock_skill)

                module, spec = import_operational_module()
                spec.loader.exec_module(module)
                module._read_station_codes = lambda config: ["10001"]
                module.is_decad_boundary = lambda d: False

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0
                mocks["data_reader"].read_observed_and_modelled_data.assert_not_called()
                mocks["file_writer"].save_forecast_data.assert_not_called()

    def test_both_mode_skips_both_on_non_boundary_day(
        self,
        mock_data,
        mock_skill,
    ):
        """BOTH mode when today is neither pentad nor decad boundary."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "BOTH"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks("BOTH", mock_data, mock_skill)

                module, spec = import_operational_module()
                spec.loader.exec_module(module)
                module._read_station_codes = lambda config: ["10001"]
                module.is_pentad_boundary = lambda d: False
                module.is_decad_boundary = lambda d: False

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0
                mocks["data_reader"].read_observed_and_modelled_data.assert_not_called()
                mocks["file_writer"].save_forecast_data.assert_not_called()

    def test_both_mode_pentad_only_on_pentad_boundary(
        self,
        mock_data,
        mock_skill,
    ):
        """BOTH mode on day 25 (pentad boundary, NOT decad boundary).

        Only pentad processing should run.
        """
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "BOTH"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks("BOTH", mock_data, mock_skill)

                module, spec = import_operational_module()
                spec.loader.exec_module(module)
                module._read_station_codes = lambda config: ["10001"]
                module.is_pentad_boundary = lambda d: True
                module.is_decad_boundary = lambda d: False

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0
                calls = mocks["data_reader"].read_observed_and_modelled_data.call_args_list
                horizons = [c[0][0] for c in calls]
                assert "pentad" in horizons
                assert "decad" not in horizons

    def test_both_mode_decad_only_on_decad_boundary(
        self,
        mock_data,
        mock_skill,
    ):
        """BOTH mode on day 10 (both pentad and decad boundary).

        Both should run since 10 is both a pentad and decad boundary.
        But if we force pentad=False, only decad should run.
        """
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "BOTH"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks("BOTH", mock_data, mock_skill)

                module, spec = import_operational_module()
                spec.loader.exec_module(module)
                module._read_station_codes = lambda config: ["10001"]
                module.is_pentad_boundary = lambda d: False
                module.is_decad_boundary = lambda d: True

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0
                calls = mocks["data_reader"].read_observed_and_modelled_data.call_args_list
                horizons = [c[0][0] for c in calls]
                assert "pentad" not in horizons
                assert "decad" in horizons
