"""Tests for postprocessing_maintenance.py — nightly gap-fill entry point."""

import importlib.util
import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, SCRIPT_DIR)


def import_maintenance_module():
    """Import the postprocessing_maintenance module."""
    spec = importlib.util.spec_from_file_location(
        "postprocessing_maintenance_module",
        os.path.join(SCRIPT_DIR, "postprocessing_maintenance.py"),
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
            "model_short": ["LR"],
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


def _setup_mocks(mock_data, mock_skill, combined=None, gaps=None):
    """Set up mocks for the maintenance module."""
    mock_sl = MagicMock()
    mock_skill_metrics = MagicMock()
    mock_file_writer = MagicMock()
    mock_tl = MagicMock()
    mock_gap_detector = MagicMock()
    mock_data_reader = MagicMock()
    mock_ensemble_calc = MagicMock()

    mock_sl.load_environment.return_value = None
    mock_sl.calculate_virtual_stations_data.side_effect = lambda x: x
    mock_sl.calculate_neural_ensemble_forecast.side_effect = lambda x: x
    mock_sl.calculate_neural_ensemble_forecast_decade.side_effect = lambda x: x

    mock_data_reader.read_observed_and_modelled_data.return_value = (mock_data, mock_data)

    if combined is None:
        combined = pd.DataFrame()
    if gaps is None:
        gaps = pd.DataFrame(columns=["date", "code", "model_short"])

    mock_gap_detector.detect_missing_ensembles.return_value = gaps

    mock_data_reader.read_combined_forecasts.return_value = combined
    mock_data_reader.read_skill_metrics.return_value = mock_skill
    # Ensemble result must include EM rows so the merge logic proceeds
    ensemble_result = pd.concat(
        [
            mock_data,
            pd.DataFrame(
                {
                    "code": ["10001"],
                    "date": pd.to_datetime(["2024-01-05"]),
                    "forecasted_discharge": [100.0],
                    "model_short": ["EM"],
                }
            ),
        ],
        ignore_index=True,
    )
    mock_ensemble_calc.create_ensemble_forecasts.return_value = (ensemble_result, mock_skill)

    mock_file_writer.save_forecast_data_pentad.return_value = None
    mock_file_writer.save_forecast_data_decade.return_value = None

    mock_pt_module = MagicMock()
    mock_pt_module.TimingStats.return_value.summary.return_value = ([], 0)

    mock_src = MagicMock()
    mock_src.postprocessing_tools = mock_pt_module
    mock_src.data_reader = mock_data_reader
    mock_src.ensemble_calculator = mock_ensemble_calc
    mock_src.gap_detector = mock_gap_detector
    mock_src.skill_metrics = mock_skill_metrics
    mock_src.file_writer = mock_file_writer

    sys.modules["setup_library"] = mock_sl
    sys.modules["tag_library"] = mock_tl
    sys.modules["src"] = mock_src
    sys.modules["src.postprocessing_tools"] = mock_pt_module
    sys.modules["src.data_reader"] = mock_data_reader
    sys.modules["src.ensemble_calculator"] = mock_ensemble_calc
    sys.modules["src.gap_detector"] = mock_gap_detector
    sys.modules["src.skill_metrics"] = mock_skill_metrics
    sys.modules["src.file_writer"] = mock_file_writer

    return {
        "sl": mock_sl,
        "skill_metrics": mock_skill_metrics,
        "file_writer": mock_file_writer,
        "gap_detector": mock_gap_detector,
        "data_reader": mock_data_reader,
        "ensemble_calc": mock_ensemble_calc,
    }


class TestMaintenanceWorkflow:
    """Tests for the maintenance gap-fill entry point."""

    def test_no_gaps_skips_processing(self, mock_data, mock_skill):
        """When no gaps detected, skips data reading and ensemble creation."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["EM"],
            }
        )
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(
                    mock_data,
                    mock_skill,
                    combined=combined,
                    gaps=pd.DataFrame(columns=["date", "code", "model_short"]),
                )

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                # No gap-fill needed, so no data reading for gap-fill
                mocks["data_reader"].read_observed_and_modelled_data.assert_not_called()
                mocks["ensemble_calc"].create_ensemble_forecasts.assert_not_called()

    def test_gaps_trigger_ensemble_creation(self, mock_data, mock_skill):
        """When gaps found, reads data and creates ensembles."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 2),
                "code": ["10001"] * 2,
                "model_short": ["LR", "TFT"],
            }
        )
        gaps = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["EM"],
            }
        )
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(
                    mock_data,
                    mock_skill,
                    combined=combined,
                    gaps=gaps,
                )

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                mocks["ensemble_calc"].create_ensemble_forecasts.assert_called_once()

    def test_lookback_env_var(self, mock_data, mock_skill):
        """POSTPROCESSING_GAPFILL_WINDOW_DAYS env var is respected."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["LR"],
            }
        )
        with patch.dict(
            os.environ,
            {
                "SAPPHIRE_PREDICTION_MODE": "PENTAD",
                "POSTPROCESSING_GAPFILL_WINDOW_DAYS": "14",
            },
        ):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(
                    mock_data,
                    mock_skill,
                    combined=combined,
                    gaps=pd.DataFrame(columns=["date", "code", "model_short"]),
                )

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                # Verify lookback was passed to detect_missing_ensembles
                # _fill_gaps_for_horizon passes lookback as positional arg
                call_args = mocks["gap_detector"].detect_missing_ensembles.call_args
                assert len(call_args[0]) >= 2, "lookback should be passed as positional arg"
                assert call_args[0][1] == 14, f"Expected lookback=14, got {call_args[0][1]}"

    def test_empty_combined_forecasts_skips(self, mock_data, mock_skill):
        """When no combined forecasts file exists, skips gracefully."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(
                    mock_data,
                    mock_skill,
                    combined=pd.DataFrame(),
                )

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                mocks["gap_detector"].detect_missing_ensembles.assert_not_called()

    def test_invalid_mode_exits_with_error(self, mock_data, mock_skill):
        """Invalid SAPPHIRE_PREDICTION_MODE exits with code 1."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "INVALID"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 1
                mocks["data_reader"].read_combined_forecasts.assert_not_called()

    def test_both_mode_processes_both(self, mock_data, mock_skill):
        """BOTH mode processes pentad and decad gap detection."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["EM"],
            }
        )
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "BOTH"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(
                    mock_data,
                    mock_skill,
                    combined=combined,
                )

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                # Both pentad and decad combined forecasts should be read
                calls = mocks["data_reader"].read_combined_forecasts.call_args_list
                horizons = [c[0][0] for c in calls]
                assert "pentad" in horizons
                assert "decad" in horizons

    def test_decad_mode_only(self, mock_data, mock_skill):
        """DECAD mode only reads decad combined forecasts."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["EM"],
            }
        )
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "DECAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(
                    mock_data,
                    mock_skill,
                    combined=combined,
                )

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                calls = mocks["data_reader"].read_combined_forecasts.call_args_list
                horizons = [c[0][0] for c in calls]
                assert "decad" in horizons
                assert "pentad" not in horizons

    def test_save_error_causes_exit_1(self, mock_data, mock_skill):
        """Save failure during gap-fill causes exit code 1."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 2),
                "code": ["10001"] * 2,
                "model_short": ["LR", "TFT"],
            }
        )
        gaps = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["EM"],
            }
        )
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(
                    mock_data,
                    mock_skill,
                    combined=combined,
                    gaps=gaps,
                )
                mocks["file_writer"].save_forecast_data_pentad.return_value = "Error: disk full"

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 1


class TestMaintenanceConcurrentErrors:
    """BOTH mode where one horizon's gap-fill save fails."""

    def _make_gaps_setup(self, mock_data, mock_skill):
        """Set up mocks with gaps found in both horizons."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 2),
                "code": ["10001"] * 2,
                "model_short": ["LR", "TFT"],
            }
        )
        gaps = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["EM"],
            }
        )
        return _setup_mocks(
            mock_data,
            mock_skill,
            combined=combined,
            gaps=gaps,
        )

    def test_both_mode_pentad_gap_fill_fails_decad_succeeds(self, mock_data, mock_skill):
        """BOTH mode: pentad save fails, decad succeeds => exit 1."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "BOTH"}):
            with patch.dict(sys.modules, {}):
                mocks = self._make_gaps_setup(mock_data, mock_skill)
                mocks[
                    "file_writer"
                ].save_forecast_data_pentad.return_value = "Error: pentad write failed"
                mocks["file_writer"].save_forecast_data_decade.return_value = None

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 1

    def test_both_mode_pentad_succeeds_decad_gap_fill_fails(self, mock_data, mock_skill):
        """BOTH mode: pentad succeeds, decad save fails => exit 1."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "BOTH"}):
            with patch.dict(sys.modules, {}):
                mocks = self._make_gaps_setup(mock_data, mock_skill)
                mocks["file_writer"].save_forecast_data_pentad.return_value = None
                mocks[
                    "file_writer"
                ].save_forecast_data_decade.return_value = "Error: decad write failed"

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 1


class TestMaintenanceEdgeCases:
    """Edge case tests for maintenance gap-fill branches."""

    def test_load_environment_failure_propagates(self, mock_data, mock_skill):
        """When load_environment() raises, exception propagates uncaught."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)
                mocks["sl"].load_environment.side_effect = FileNotFoundError("missing .env")

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)

                with pytest.raises(FileNotFoundError, match="missing .env"):
                    module.postprocessing_maintenance()

    def test_gap_dates_no_matching_forecast_data(self, mock_data, mock_skill):
        """Gaps found but modelled data doesn't match gap dates → skip."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05", "2024-01-10"]),
                "code": ["10001", "10001"],
                "model_short": ["LR", "LR"],
            }
        )
        # Gap at Jan 10, but mock_data only has date Jan 5
        gaps = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-10"]),
                "code": ["10001"],
                "model_short": ["EM"],
            }
        )
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(
                    mock_data,
                    mock_skill,
                    combined=combined,
                    gaps=gaps,
                )

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                # Data was read but no rows match gap dates
                mocks["data_reader"].read_observed_and_modelled_data.assert_called_once()
                # Early return before skill read or ensemble creation
                mocks["data_reader"].read_skill_metrics.assert_not_called()
                mocks["ensemble_calc"].create_ensemble_forecasts.assert_not_called()

    def test_gap_dates_empty_skill_metrics(self, mock_data, mock_skill):
        """Gaps + matching forecast data but empty skill metrics → skip."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 2),
                "code": ["10001"] * 2,
                "model_short": ["LR", "TFT"],
            }
        )
        gaps = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["EM"],
            }
        )
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(
                    mock_data,
                    mock_skill,
                    combined=combined,
                    gaps=gaps,
                )
                mocks["data_reader"].read_skill_metrics.return_value = pd.DataFrame()

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                # Skill metrics read but empty → no ensemble
                mocks["data_reader"].read_skill_metrics.assert_called_once()
                mocks["ensemble_calc"].create_ensemble_forecasts.assert_not_called()

    def test_save_success_path(self, mock_data, mock_skill):
        """Save returning None → exit 0, save was called."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 2),
                "code": ["10001"] * 2,
                "model_short": ["LR", "TFT"],
            }
        )
        gaps = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["EM"],
            }
        )
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(
                    mock_data,
                    mock_skill,
                    combined=combined,
                    gaps=gaps,
                )

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                mocks["file_writer"].save_forecast_data_pentad.assert_called_once()
