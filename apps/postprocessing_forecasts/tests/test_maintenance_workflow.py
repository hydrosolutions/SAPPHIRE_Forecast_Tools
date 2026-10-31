"""Tests for postprocessing_maintenance.py — nightly gap-fill entry point."""

import importlib.util
import os
import sys
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

if TYPE_CHECKING:
    import requests

import pandas as pd
import pytest

SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, SCRIPT_DIR)

import src.horizon_config as _real_horizon_config


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
            "pentad_in_year": [1],
            "decad_in_year": [1],
            "q05": [85.0],
            "q50": [100.0],
            "q95": [115.0],
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
    mock_api_writer = MagicMock()
    mock_api_writer.SAPPHIRE_API_AVAILABLE = True
    mock_api_writer._write_combined_forecast_to_api.return_value = True

    mock_sl.load_environment.return_value = None
    mock_sl.calculate_virtual_stations_data.side_effect = lambda x: x
    mock_sl.calculate_neural_ensemble_forecast.side_effect = lambda x: x
    mock_sl.calculate_neural_ensemble_forecast_decade.side_effect = lambda x: x

    mock_data_reader.read_observed_and_modelled_data.return_value = (mock_data, mock_data)
    # PP-021: maintenance now calls read_individual_model_forecasts_for_dates
    mock_data_reader.read_individual_model_forecasts_for_dates.return_value = (
        mock_data,
        pd.DataFrame(),
    )

    if combined is None:
        combined = pd.DataFrame()
    if gaps is None:
        gaps = pd.DataFrame(columns=["date", "code", "model_short"])

    mock_gap_detector.detect_missing_ensembles.return_value = gaps
    # PP-021: maintenance now also calls detect_stale_quantiles
    mock_gap_detector.detect_stale_quantiles.return_value = pd.DataFrame(
        columns=["date", "code", "model_short"]
    )

    mock_data_reader.read_combined_forecasts.return_value = combined
    mock_data_reader.read_skill_metrics.return_value = mock_skill
    # Ensemble result must include individual + NE + EM rows
    ensemble_result = pd.concat(
        [
            mock_data,
            pd.DataFrame(
                {
                    "code": ["10001"],
                    "date": pd.to_datetime(["2024-01-05"]),
                    "forecasted_discharge": [105.0],
                    "model_short": ["NE"],
                    "pentad_in_year": [1],
                    "decad_in_year": [1],
                    "q05": [90.0],
                    "q50": [105.0],
                    "q95": [120.0],
                }
            ),
            pd.DataFrame(
                {
                    "code": ["10001"],
                    "date": pd.to_datetime(["2024-01-05"]),
                    "forecasted_discharge": [100.0],
                    "model_short": ["EM"],
                    "pentad_in_year": [1],
                    "decad_in_year": [1],
                    "q05": [85.0],
                    "q50": [100.0],
                    "q95": [115.0],
                }
            ),
        ],
        ignore_index=True,
    )
    mock_ensemble_calc.create_ensemble_forecasts.return_value = (ensemble_result, mock_skill)

    mock_file_writer.save_forecast_data.return_value = None

    mock_pt_module = MagicMock()
    mock_pt_module.TimingStats.return_value.summary.return_value = ([], 0)

    mock_src = MagicMock()
    mock_src.postprocessing_tools = mock_pt_module
    mock_src.data_reader = mock_data_reader
    mock_src.ensemble_calculator = mock_ensemble_calc
    mock_src.gap_detector = mock_gap_detector
    mock_src.skill_metrics = mock_skill_metrics
    mock_src.file_writer = mock_file_writer
    mock_src.api_writer = mock_api_writer

    sys.modules["setup_library"] = mock_sl
    sys.modules["tag_library"] = mock_tl
    sys.modules["src"] = mock_src
    sys.modules["src.postprocessing_tools"] = mock_pt_module
    sys.modules["src.data_reader"] = mock_data_reader
    sys.modules["src.ensemble_calculator"] = mock_ensemble_calc
    sys.modules["src.gap_detector"] = mock_gap_detector
    sys.modules["src.skill_metrics"] = mock_skill_metrics
    sys.modules["src.file_writer"] = mock_file_writer
    sys.modules["src.api_writer"] = mock_api_writer
    sys.modules["src.horizon_config"] = _real_horizon_config

    return {
        "sl": mock_sl,
        "skill_metrics": mock_skill_metrics,
        "file_writer": mock_file_writer,
        "gap_detector": mock_gap_detector,
        "data_reader": mock_data_reader,
        "ensemble_calc": mock_ensemble_calc,
        "api_writer": mock_api_writer,
    }


class TestMaintenanceWorkflow:
    """Tests for the maintenance gap-fill entry point."""

    def test_no_gaps_skips_processing(self, mock_data, mock_skill):
        """When no gaps detected, skips ensemble creation."""
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
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                # PP-021: modelled data is NOT read when no gaps exist
                mocks["data_reader"].read_observed_and_modelled_data.assert_not_called()
                mocks["data_reader"].read_individual_model_forecasts_for_dates.assert_not_called()
                mocks["ensemble_calc"].create_ensemble_forecasts.assert_not_called()

    def test_gaps_trigger_ensemble_creation(self, mock_data, mock_skill):
        """When gaps found, reads data and creates ensembles."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 2),
                "code": ["10001"] * 2,
                "model_short": ["LR", "TFT"],
                "pentad_in_year": [1, 1],
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
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                mocks["ensemble_calc"].create_ensemble_forecasts.assert_called_once()
                # Verify saved data contains EM rows from the ensemble result
                mocks["file_writer"].save_forecast_data.assert_called_once()
                saved_df = mocks["file_writer"].save_forecast_data.call_args[0][1]
                assert "EM" in saved_df["model_short"].values
                # Original combined rows (LR, TFT) should also be preserved
                assert "LR" in saved_df["model_short"].values
                assert "TFT" in saved_df["model_short"].values

    def test_lookback_env_var(self, mock_data, mock_skill):
        """POSTPROCESSING_GAPFILL_MAX_MONTHS env var is respected."""
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
                "POSTPROCESSING_GAPFILL_MAX_MONTHS": "6",
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
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                call_args = mocks["gap_detector"].detect_missing_ensembles.call_args
                assert call_args.kwargs["max_lookback_months"] == 6

    def test_old_env_var_deprecation_warning(self, mock_data, mock_skill, caplog):
        """Setting old POSTPROCESSING_GAPFILL_WINDOW_DAYS logs deprecation."""
        import logging

        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["EM"],
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
                _setup_mocks(
                    mock_data,
                    mock_skill,
                    combined=combined,
                    gaps=pd.DataFrame(columns=["date", "code", "model_short"]),
                )

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                # Re-add caplog handler (exec_module replaces root handlers)
                logging.getLogger().addHandler(caplog.handler)
                caplog.set_level(logging.WARNING)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                assert "POSTPROCESSING_GAPFILL_WINDOW_DAYS is deprecated" in caplog.text

    def test_empty_combined_forecasts_skips(self, mock_data, mock_skill):
        """When no combined or modelled data exists, skips gracefully."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(
                    mock_data,
                    mock_skill,
                    combined=pd.DataFrame(),
                )
                # Also make modelled data empty so both-empty path is hit
                mocks["data_reader"].read_observed_and_modelled_data.return_value = (
                    pd.DataFrame(),
                    pd.DataFrame(),
                )

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

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
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

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
                "pentad_in_year": [1],
                "decad_in_year": [1],
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
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

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
                "pentad_in_year": [1],
                "decad_in_year": [1],
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
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

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
                "pentad_in_year": [1, 1],
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
                mocks["file_writer"].save_forecast_data.return_value = "Error: disk full"

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 1


class TestMaintenanceEarlyExitAndStaleDetection:
    """PP-021: early-exit and stale quantile detection tests."""

    def test_early_exit_when_no_gaps_and_no_stale(self, mock_data, mock_skill):
        """No gaps + no stale records → early exit, no data read."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["EM"],
                "forecasted_discharge": [100.0],
                "q05": [80.0],  # has quantiles — not stale
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
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                # Neither expensive read nor ensemble creation triggered
                mocks["data_reader"].read_individual_model_forecasts_for_dates.assert_not_called()
                mocks["ensemble_calc"].create_ensemble_forecasts.assert_not_called()

    def test_stale_individual_records_trigger_refresh(self, mock_data, mock_skill):
        """Stale NE/individual records trigger scoped data read and EM refresh."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["EM"],
                "forecasted_discharge": [100.0],
                "pentad_in_year": [1],
            }
        )
        stale = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["NE"],
            }
        )
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(
                    mock_data,
                    mock_skill,
                    combined=combined,
                    # detect_missing_ensembles returns no EM gaps
                    gaps=pd.DataFrame(columns=["date", "code", "model_short"]),
                )
                # But detect_stale_quantiles returns stale NE record
                mocks["gap_detector"].detect_stale_quantiles.return_value = stale

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                # Scoped read triggered (stale NE causes work to be done)
                mocks["data_reader"].read_individual_model_forecasts_for_dates.assert_called_once()
                mocks["ensemble_calc"].create_ensemble_forecasts.assert_called_once()

    def test_stale_em_records_trigger_refresh(self, mock_data, mock_skill):
        """Stale EM records (q05=NULL) trigger scoped data read."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["EM"],
                "forecasted_discharge": [100.0],
                "q05": [float("nan")],  # stale EM: has discharge but no quantiles
                "pentad_in_year": [1],
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
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                # Stale EM drives a scoped read
                mocks["data_reader"].read_individual_model_forecasts_for_dates.assert_called_once()
                mocks["ensemble_calc"].create_ensemble_forecasts.assert_called_once()

    def test_scoped_read_called_with_only_gap_dates(self, mock_data, mock_skill):
        """When EM gaps exist, scoped read is called (not full history read)."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 2),
                "code": ["10001"] * 2,
                "model_short": ["LR", "TFT"],
                "pentad_in_year": [1, 1],
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
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                # Scoped read (not full-history read) was used
                mocks["data_reader"].read_individual_model_forecasts_for_dates.assert_called_once()
                mocks["data_reader"].read_observed_and_modelled_data.assert_not_called()
                mocks["ensemble_calc"].create_ensemble_forecasts.assert_called_once()


class TestMaintenanceConcurrentErrors:
    """BOTH mode where one horizon's gap-fill save fails."""

    def _make_gaps_setup(self, mock_data, mock_skill):
        """Set up mocks with gaps found in both horizons."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 2),
                "code": ["10001"] * 2,
                "model_short": ["LR", "TFT"],
                "pentad_in_year": [1, 1],
                "decad_in_year": [1, 1],
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
                mock_fw = mocks["file_writer"]
                mock_fw.save_forecast_data.side_effect = [
                    "Error: pentad write failed",
                    None,
                ]

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 1

    def test_both_mode_pentad_succeeds_decad_gap_fill_fails(self, mock_data, mock_skill):
        """BOTH mode: pentad succeeds, decad save fails => exit 1."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "BOTH"}):
            with patch.dict(sys.modules, {}):
                mocks = self._make_gaps_setup(mock_data, mock_skill)
                mock_fw = mocks["file_writer"]
                mock_fw.save_forecast_data.side_effect = [
                    None,
                    "Error: decad write failed",
                ]

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

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
                "pentad_in_year": [1, 2],
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
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                # PP-021: scoped read called for gap dates
                mocks["data_reader"].read_individual_model_forecasts_for_dates.assert_called_once()
                # mock_data only has Jan 5, but gap is Jan 10 → filtered to empty
                # → early return before skill read or ensemble creation
                mocks["data_reader"].read_skill_metrics.assert_not_called()
                mocks["ensemble_calc"].create_ensemble_forecasts.assert_not_called()

    def test_gap_dates_empty_skill_metrics_still_saves_stale(self, mock_data, mock_skill):
        """Stale records + empty skill metrics → EM skipped, stale individual refreshed."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 2),
                "code": ["10001"] * 2,
                "model_short": ["LR", "TFT"],
                "pentad_in_year": [1, 1],
                "forecasted_discharge": [100.0, 110.0],
                "q05": [None, None],  # stale
            }
        )
        stale = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05", "2024-01-05"]),
                "code": ["10001", "10001"],
                "model_short": ["LR", "TFT"],
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
                mocks["gap_detector"].detect_stale_quantiles.return_value = stale
                mocks["data_reader"].read_skill_metrics.return_value = pd.DataFrame()

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                # Skill metrics read but empty → no ensemble creation
                mocks["data_reader"].read_skill_metrics.assert_called_once()
                mocks["ensemble_calc"].create_ensemble_forecasts.assert_not_called()
                # But stale individual rows are still refreshed and saved
                mocks["file_writer"].save_forecast_data.assert_called_once()

    def test_save_success_path(self, mock_data, mock_skill):
        """Save returning None → exit 0, save was called."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 2),
                "code": ["10001"] * 2,
                "model_short": ["LR", "TFT"],
                "pentad_in_year": [1, 1],
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
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                mocks["file_writer"].save_forecast_data.assert_called_once()


class TestPP022StaleRefreshFixes:
    """PP-022: stale-record refresh saves all row types, not just EM."""

    def test_save_includes_stale_individual_ne_and_em(self, mock_data, mock_skill):
        """Stale individual/NE rows + EM gap → all refreshed in saved output."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 3),
                "code": ["10001"] * 3,
                "model_short": ["LR", "NE", "TFT"],
                "pentad_in_year": [1, 1, 1],
                "forecasted_discharge": [100.0, 105.0, 110.0],
                "q05": [None, None, 90.0],  # LR and NE stale, TFT ok
            }
        )
        stale = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05", "2024-01-05"]),
                "code": ["10001", "10001"],
                "model_short": ["LR", "NE"],
            }
        )
        gaps = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["EM"],
            }
        )
        # Modelled data includes both LR and NE rows with quantiles
        modelled_with_ne = pd.concat(
            [
                mock_data,
                pd.DataFrame(
                    {
                        "code": ["10001"],
                        "date": pd.to_datetime(["2024-01-05"]),
                        "forecasted_discharge": [105.0],
                        "model_short": ["NE"],
                        "pentad_in_year": [1],
                        "decad_in_year": [1],
                        "q05": [90.0],
                        "q50": [105.0],
                        "q95": [120.0],
                    }
                ),
            ],
            ignore_index=True,
        )
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(
                    mock_data,
                    mock_skill,
                    combined=combined,
                    gaps=gaps,
                )
                mocks["gap_detector"].detect_stale_quantiles.return_value = stale
                mocks["data_reader"].read_individual_model_forecasts_for_dates.return_value = (
                    modelled_with_ne,
                    pd.DataFrame(),
                )

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                saved_df = mocks["file_writer"].save_forecast_data.call_args[0][1]
                saved_models = set(saved_df["model_short"].unique())
                # Must contain refreshed LR, NE, and new EM
                assert "LR" in saved_models
                assert "NE" in saved_models
                assert "EM" in saved_models

    def test_stale_individual_rows_are_replaced(self, mock_data, mock_skill):
        """Stale individual-model rows (q05=NULL) are replaced with fresh rows."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 2),
                "code": ["10001"] * 2,
                "model_short": ["LR", "EM"],
                "forecasted_discharge": [100.0, 105.0],
                "q05": [None, 85.0],  # LR stale, EM ok
                "pentad_in_year": [1, 1],
            }
        )
        stale = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["LR"],
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
                mocks["gap_detector"].detect_stale_quantiles.return_value = stale

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                saved_df = mocks["file_writer"].save_forecast_data.call_args[0][1]
                # The saved LR row should have q05 populated (from mock_data)
                lr_rows = saved_df[saved_df["model_short"] == "LR"]
                assert not lr_rows.empty
                assert lr_rows["q05"].notna().all()

    def test_stale_ne_rows_are_replaced(self, mock_data, mock_skill):
        """Stale NE rows (q05=NULL) are replaced when modelled data exists."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["NE"],
                "forecasted_discharge": [105.0],
                "q05": [None],  # stale NE
                "pentad_in_year": [1],
            }
        )
        stale = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["NE"],
            }
        )
        # Modelled data must include an NE row with quantiles
        modelled_with_ne = pd.concat(
            [
                mock_data,
                pd.DataFrame(
                    {
                        "code": ["10001"],
                        "date": pd.to_datetime(["2024-01-05"]),
                        "forecasted_discharge": [105.0],
                        "model_short": ["NE"],
                        "pentad_in_year": [1],
                        "decad_in_year": [1],
                        "q05": [90.0],
                        "q50": [105.0],
                        "q95": [120.0],
                    }
                ),
            ],
            ignore_index=True,
        )
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(
                    mock_data,
                    mock_skill,
                    combined=combined,
                    gaps=pd.DataFrame(columns=["date", "code", "model_short"]),
                )
                mocks["gap_detector"].detect_stale_quantiles.return_value = stale
                mocks["data_reader"].read_individual_model_forecasts_for_dates.return_value = (
                    modelled_with_ne,
                    pd.DataFrame(),
                )

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                saved_df = mocks["file_writer"].save_forecast_data.call_args[0][1]
                ne_rows = saved_df[saved_df["model_short"] == "NE"]
                assert not ne_rows.empty
                # The refreshed NE row (from modelled) has q05 populated
                # keep="last" in dedup means the joint NE replaces the stale one
                assert ne_rows.iloc[-1]["q05"] == pytest.approx(90.0)

    def test_ne_gaps_are_filled(self, mock_data, mock_skill):
        """Missing NE rows (gap, not stale) trigger data read and NE creation."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["LR"],
                "pentad_in_year": [1],
            }
        )
        # NE gap (missing row, not stale)
        gaps = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["NE"],
            }
        )
        # neural_ensemble_func adds NE rows from individual-model data
        ne_row = pd.DataFrame(
            {
                "code": ["10001"],
                "date": pd.to_datetime(["2024-01-05"]),
                "forecasted_discharge": [105.0],
                "model_short": ["NE"],
                "pentad_in_year": [1],
                "decad_in_year": [1],
                "q05": [90.0],
                "q50": [105.0],
                "q95": [120.0],
            }
        )

        def _add_ne(df):
            return pd.concat([df, ne_row], ignore_index=True)

        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(
                    mock_data,
                    mock_skill,
                    combined=combined,
                    gaps=gaps,
                )
                mocks["sl"].calculate_neural_ensemble_forecast.side_effect = _add_ne

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                # Data was read and NE gap was filled
                mocks["data_reader"].read_individual_model_forecasts_for_dates.assert_called_once()
                mocks["file_writer"].save_forecast_data.assert_called_once()
                saved_df = mocks["file_writer"].save_forecast_data.call_args[0][1]
                ne_rows = saved_df[saved_df["model_short"] == "NE"]
                assert not ne_rows.empty, "NE gap should have been filled"

    def test_stale_refresh_without_skill_metrics(self, mock_data, mock_skill):
        """Individual/NE rows refreshed even when skill metrics are empty."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["LR"],
                "forecasted_discharge": [100.0],
                "q05": [None],  # stale
                "pentad_in_year": [1],
            }
        )
        stale = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["LR"],
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
                mocks["gap_detector"].detect_stale_quantiles.return_value = stale
                mocks["data_reader"].read_skill_metrics.return_value = pd.DataFrame()

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                # EM not created (no skill metrics)
                mocks["ensemble_calc"].create_ensemble_forecasts.assert_not_called()
                # But individual rows are still saved
                mocks["file_writer"].save_forecast_data.assert_called_once()
                saved_df = mocks["file_writer"].save_forecast_data.call_args[0][1]
                assert "EM" not in saved_df["model_short"].values

    def test_gap_codes_passed_to_data_reader(self, mock_data, mock_skill):
        """read_individual_model_forecasts_for_dates receives gap codes."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 2),
                "code": ["10001"] * 2,
                "model_short": ["LR", "TFT"],
                "pentad_in_year": [1, 1],
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
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                call_kwargs = mocks[
                    "data_reader"
                ].read_individual_model_forecasts_for_dates.call_args.kwargs
                assert "codes" in call_kwargs
                assert "10001" in call_kwargs["codes"]

    def test_dedup_key_includes_period_col(self, mock_data, mock_skill):
        """drop_duplicates uses period_col in the dedup key."""
        # Two rows with same date/code/model but different pentad_in_year
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 2),
                "code": ["10001"] * 2,
                "model_short": ["LR", "LR"],
                "pentad_in_year": [1, 2],  # different periods
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
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                saved_df = mocks["file_writer"].save_forecast_data.call_args[0][1]
                # Both LR rows with different pentad_in_year should be preserved
                lr_rows = saved_df[saved_df["model_short"] == "LR"]
                assert len(lr_rows) >= 2


class TestPP024DirectAPIWrite:
    """PP-024: refreshed rows are written directly to the API."""

    def _gaps_setup(self, mock_data, mock_skill):
        """Common setup: gaps trigger ensemble creation → joint is non-empty."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 2),
                "code": ["10001"] * 2,
                "model_short": ["LR", "TFT"],
                "pentad_in_year": [1, 1],
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

    def test_gap_fill_writes_refreshed_rows_to_api(self, mock_data, mock_skill):
        """Refreshed rows are written directly to API, not just via
        save_forecast_data's latest filter."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = self._gaps_setup(mock_data, mock_skill)

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                mock_aw = mocks["api_writer"]
                mock_aw._write_combined_forecast_to_api.assert_called_once()
                # The DataFrame passed should contain EM rows
                written_df = mock_aw._write_combined_forecast_to_api.call_args[0][0]
                assert "EM" in written_df["model_short"].values
                # Horizon type should be "pentad"
                assert mock_aw._write_combined_forecast_to_api.call_args[0][1] == "pentad"

    def test_api_write_failure_does_not_block_csv_save(self, mock_data, mock_skill):
        """If direct API write raises, CSV save still proceeds."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = self._gaps_setup(mock_data, mock_skill)
                mocks["api_writer"]._write_combined_forecast_to_api.side_effect = RuntimeError(
                    "API timeout"
                )

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                # Exit 1 because the API error is appended to errors list
                assert exc_info.value.code == 1
                # CSV save still called despite API failure
                mocks["file_writer"].save_forecast_data.assert_called_once()

    def test_api_write_returns_false_logged_as_warning(self, mock_data, mock_skill, caplog):
        """When _write_combined_forecast_to_api returns False, no error is raised."""
        import logging

        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = self._gaps_setup(mock_data, mock_skill)
                mocks["api_writer"]._write_combined_forecast_to_api.return_value = False

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["10001", "10002"])

                logging.getLogger().addHandler(caplog.handler)
                caplog.set_level(logging.WARNING)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                # Not fatal — exit 0
                assert exc_info.value.code == 0
                # CSV save still called
                mocks["file_writer"].save_forecast_data.assert_called_once()
                assert "direct API write returned False" in caplog.text


class TestMaintenanceEmptyModelledGuard:
    """Regression tests for empty scoped modelled reads during gap-fill."""

    def test_decad_columnless_empty_read_returns_before_virtual_and_neural(
        self,
        mock_data,
        mock_skill,
        caplog,
    ):
        """Columnless DECAD scoped read no-ops before virtual/NE processing."""
        import logging

        affected_date = pd.Timestamp("2024-01-10")
        combined = pd.DataFrame(
            {
                "date": [affected_date],
                "code": ["19999"],
                "model_short": ["LR"],
                "forecasted_discharge": [1.0],
                "q05": [1.0],
                "decad_in_month": [1],
                "decad_in_year": [1],
            }
        )
        gaps = pd.DataFrame(
            {
                "date": [affected_date],
                "code": ["19999"],
                "model_short": ["EM"],
            }
        )

        with patch.dict(sys.modules, {}):
            mocks = _setup_mocks(
                mock_data,
                mock_skill,
                combined=combined,
                gaps=gaps,
            )
            mocks["data_reader"].read_individual_model_forecasts_for_dates.return_value = (
                pd.DataFrame(),
                pd.DataFrame(),
            )
            mocks["sl"].calculate_virtual_stations_data.side_effect = AssertionError(
                "virtual stations should not run for empty modelled data"
            )
            mocks["sl"].calculate_neural_ensemble_forecast_decade.side_effect = KeyError(
                "model_short"
            )

            module, spec = import_maintenance_module()
            spec.loader.exec_module(module)
            logging.getLogger().addHandler(caplog.handler)
            caplog.set_level(logging.WARNING)

            module._fill_gaps_for_horizon(
                module.DECAD,
                max_lookback_months=13,
                errors=[],
                codes=["19999"],
            )

            mocks["sl"].calculate_virtual_stations_data.assert_not_called()
            mocks["sl"].calculate_neural_ensemble_forecast_decade.assert_not_called()
            assert "DECAD" in caplog.text
            assert "1 affected date" in caplog.text
            assert "1 gap code" in caplog.text


# ===========================================================================
# P2: Recovery mode routing tests (mock-based, no real API calls)
# ===========================================================================


def _setup_recovery_routing_mocks(mock_data, mock_skill):
    """Set up mocks for recovery routing tests.

    Patches sys.modules so that postprocessing_maintenance.py can be imported.
    _run_recovery_preview is NOT patched here; callers patch it selectively.
    """
    mocks = _setup_mocks(mock_data, mock_skill)
    return mocks


class TestRecoveryRoutingMocks:
    """Top-level recovery mode routing — off/preview/execute/bad-config.

    These tests verify that:
    - POSTPROCESSING_RECOVERY_MODE=off (or unset) runs ordinary maintenance unchanged
    - execute mode exits 1 before any client construction
    - preview mode exits 0 and never calls ordinary sinks
    - bad config/bounds exits 1
    All ordinary-path contracts from existing classes remain unchanged.
    """

    @staticmethod
    def _make_preview_result_for_module(module):
        """Return a minimal RecoveryPreviewResult using the loaded module's classes."""
        import datetime as _dt

        plan_mock = MagicMock()
        plan_mock.plan_error = None
        plan_mock.missing = set()
        plan_mock.authorized_refresh = []
        plan_mock.protected = set()
        plan_mock.excluded = {}
        plan_mock.blocked = {}

        hr = module.HorizonPreviewResult(
            plan=plan_mock,
            source_statuses={},
            candidate_count=0,
            missing_count=0,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_reason=None,
            blocked_units={},
        )
        return module.RecoveryPreviewResult(
            forecast_date=_dt.date(2026, 1, 5),
            start_date=_dt.date(2026, 1, 1),
            end_date=_dt.date(2026, 1, 5),
            horizons=["pentad"],
            horizon_results={"pentad": hr},
            blocked_reason=None,
        )

    def test_recovery_off_runs_ordinary_maintenance(self, mock_data, mock_skill):
        """Unset recovery mode runs ordinary maintenance (fill_gaps called)."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-01-05"]),
                "code": ["19999"],
                "model_short": ["EM"],
            }
        )
        env = {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}
        with patch.dict(os.environ, env):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill, combined=combined)
                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["19999"])
                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()
                assert exc_info.value.code == 0
                # Ordinary path ran — combined forecasts read
                mocks["data_reader"].read_combined_forecasts.assert_called()

    def test_execute_mode_exits_1_before_any_sink(self, mock_data, mock_skill):
        """POSTPROCESSING_RECOVERY_MODE=execute exits 1 immediately."""
        env = {
            "SAPPHIRE_PREDICTION_MODE": "PENTAD",
            "POSTPROCESSING_RECOVERY_MODE": "execute",
            "POSTPROCESSING_RECOVERY_START_DATE": "2026-01-01",
            "POSTPROCESSING_RECOVERY_END_DATE": "2026-01-05",
        }
        with patch.dict(os.environ, env):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)
                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["19999"])
                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()
                assert exc_info.value.code == 1
                # Ordinary sinks never reached
                mocks["file_writer"].save_forecast_data.assert_not_called()
                mocks["api_writer"]._write_combined_forecast_to_api.assert_not_called()
                mocks["data_reader"].read_combined_forecasts.assert_not_called()

    def test_preview_mode_exits_0_no_ordinary_sinks(self, mock_data, mock_skill):
        """preview mode: exits 0 and never calls file_writer or api_writer."""
        env = {
            "SAPPHIRE_PREDICTION_MODE": "PENTAD",
            "POSTPROCESSING_RECOVERY_MODE": "preview",
            "POSTPROCESSING_RECOVERY_START_DATE": "2026-01-01",
            "POSTPROCESSING_RECOVERY_END_DATE": "2026-01-05",
            "SAPPHIRE_FORECAST_DATE": "2026-01-05",
        }
        with patch.dict(os.environ, env):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)
                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["19999"])
                # Mock _run_recovery_preview to avoid real API calls
                module._run_recovery_preview = MagicMock(
                    return_value=self._make_preview_result_for_module(module)
                )
                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()
                assert exc_info.value.code == 0
                mocks["file_writer"].save_forecast_data.assert_not_called()
                mocks["api_writer"]._write_combined_forecast_to_api.assert_not_called()
                mocks["data_reader"].read_combined_forecasts.assert_not_called()

    def test_preview_both_mode(self, mock_data, mock_skill):
        """BOTH prediction mode in preview exits 0, no sinks called."""
        import datetime as _dt

        env = {
            "SAPPHIRE_PREDICTION_MODE": "BOTH",
            "POSTPROCESSING_RECOVERY_MODE": "preview",
            "POSTPROCESSING_RECOVERY_START_DATE": "2026-01-01",
            "POSTPROCESSING_RECOVERY_END_DATE": "2026-01-05",
            "SAPPHIRE_FORECAST_DATE": "2026-01-05",
        }
        with patch.dict(os.environ, env):
            with patch.dict(sys.modules, {}):
                _setup_mocks(mock_data, mock_skill)
                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["19999"])
                dummy_hr = module.HorizonPreviewResult(
                    plan=None,
                    source_statuses={},
                    candidate_count=0,
                    missing_count=0,
                    protected_count=0,
                    em_blocked_reason="skill_vintage_unresolved",
                    diagnostics=[],
                    blocked_reason=None,
                    blocked_units={},
                )
                both_result = module.RecoveryPreviewResult(
                    forecast_date=_dt.date(2026, 1, 5),
                    start_date=_dt.date(2026, 1, 1),
                    end_date=_dt.date(2026, 1, 5),
                    horizons=["pentad", "decad"],
                    horizon_results={"pentad": dummy_hr, "decad": dummy_hr},
                    blocked_reason=None,
                )
                module._run_recovery_preview = MagicMock(return_value=both_result)
                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()
                assert exc_info.value.code == 0
                # Verify BOTH was passed to preview
                call_args = module._run_recovery_preview.call_args
                assert call_args[0][0] == "BOTH"

    def test_bad_recovery_mode_exits_1(self, mock_data, mock_skill):
        """Unknown POSTPROCESSING_RECOVERY_MODE value exits 1."""
        env = {
            "SAPPHIRE_PREDICTION_MODE": "PENTAD",
            "POSTPROCESSING_RECOVERY_MODE": "activate",
        }
        with patch.dict(os.environ, env):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)
                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()
                assert exc_info.value.code == 1
                mocks["data_reader"].read_combined_forecasts.assert_not_called()

    def test_missing_start_date_exits_1(self, mock_data, mock_skill):
        """Missing POSTPROCESSING_RECOVERY_START_DATE exits 1."""
        env = {
            "SAPPHIRE_PREDICTION_MODE": "PENTAD",
            "POSTPROCESSING_RECOVERY_MODE": "preview",
            "POSTPROCESSING_RECOVERY_END_DATE": "2026-01-05",
        }
        with patch.dict(os.environ, env):
            with patch.dict(sys.modules, {}):
                _setup_mocks(mock_data, mock_skill)
                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()
                assert exc_info.value.code == 1

    def test_reversed_bounds_exits_1(self, mock_data, mock_skill):
        """start_date > end_date exits 1."""
        env = {
            "SAPPHIRE_PREDICTION_MODE": "PENTAD",
            "POSTPROCESSING_RECOVERY_MODE": "preview",
            "POSTPROCESSING_RECOVERY_START_DATE": "2026-01-10",
            "POSTPROCESSING_RECOVERY_END_DATE": "2026-01-05",
            "SAPPHIRE_FORECAST_DATE": "2026-01-10",
        }
        with patch.dict(os.environ, env):
            with patch.dict(sys.modules, {}):
                _setup_mocks(mock_data, mock_skill)
                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()
                assert exc_info.value.code == 1

    def test_future_end_date_exits_1(self, mock_data, mock_skill):
        """end_date > forecast_date exits 1."""
        env = {
            "SAPPHIRE_PREDICTION_MODE": "PENTAD",
            "POSTPROCESSING_RECOVERY_MODE": "preview",
            "POSTPROCESSING_RECOVERY_START_DATE": "2026-01-01",
            "POSTPROCESSING_RECOVERY_END_DATE": "2026-12-31",
            "SAPPHIRE_FORECAST_DATE": "2026-01-05",
        }
        with patch.dict(os.environ, env):
            with patch.dict(sys.modules, {}):
                _setup_mocks(mock_data, mock_skill)
                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()
                assert exc_info.value.code == 1

    def test_preview_blocked_result_exits_1(self, mock_data, mock_skill):
        """When _run_recovery_preview returns a blocked result, exit 1."""
        import datetime as _dt

        env = {
            "SAPPHIRE_PREDICTION_MODE": "PENTAD",
            "POSTPROCESSING_RECOVERY_MODE": "preview",
            "POSTPROCESSING_RECOVERY_START_DATE": "2026-01-01",
            "POSTPROCESSING_RECOVERY_END_DATE": "2026-01-05",
            "SAPPHIRE_FORECAST_DATE": "2026-01-05",
        }
        with patch.dict(os.environ, env):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)
                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["19999"])
                blocked_result = module.RecoveryPreviewResult(
                    forecast_date=_dt.date(2026, 1, 5),
                    start_date=_dt.date(2026, 1, 1),
                    end_date=_dt.date(2026, 1, 5),
                    horizons=[],
                    horizon_results={},
                    blocked_reason="SAPPHIRE_API_ENABLED=false",
                )
                module._run_recovery_preview = MagicMock(return_value=blocked_result)
                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()
                assert exc_info.value.code == 1
                mocks["file_writer"].save_forecast_data.assert_not_called()

    def test_decad_preview_mode(self, mock_data, mock_skill):
        """DECAD prediction mode in preview runs and exits 0."""
        env = {
            "SAPPHIRE_PREDICTION_MODE": "DECAD",
            "POSTPROCESSING_RECOVERY_MODE": "preview",
            "POSTPROCESSING_RECOVERY_START_DATE": "2026-01-01",
            "POSTPROCESSING_RECOVERY_END_DATE": "2026-01-10",
            "SAPPHIRE_FORECAST_DATE": "2026-01-10",
        }
        with patch.dict(os.environ, env):
            with patch.dict(sys.modules, {}):
                _setup_mocks(mock_data, mock_skill)
                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["19999"])
                module._run_recovery_preview = MagicMock(
                    return_value=self._make_preview_result_for_module(module)
                )
                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()
                assert exc_info.value.code == 0
                call_args = module._run_recovery_preview.call_args
                assert call_args[0][0] == "DECAD"


# ===========================================================================
# P2: Recovery preview real-chain tests
# ===========================================================================
# These tests call data_reader/gap_detector functions directly with a fake
# client, proving the reader→normalizer→virtual→NE→planner chain without
# mock composition. They do not go through postprocessing_maintenance().
# ===========================================================================


def _make_fake_client(rows_by_request: dict):
    """Return a minimal fake client keyed by (path, params-without-skip-limit).

    rows_by_request: {(path, params_dict): [row_dict, ...]}
    skip/limit are handled automatically for pagination.
    Raises AssertionError on any unregistered request so test bugs are loud.
    Register an endpoint with an empty list to allow confirmed-empty responses.
    """

    class FakeClient:
        def __init__(self, registry):
            self._registry = {}
            for (path, params_fs), rows in registry.items():
                key = (
                    path,
                    frozenset((k, v) for k, v in params_fs if k not in ("skip", "limit")),
                )
                self._registry[key] = list(rows)

        def _get(self, path, params=None):
            if params is None:
                params = {}
            skip = params.get("skip", 0)
            limit = params.get("limit", 500)
            key = (
                path,
                frozenset((k, v) for k, v in params.items() if k not in ("skip", "limit")),
            )
            if key not in self._registry:
                raise AssertionError(f"Unexpected _get({path!r}, {dict(params)!r})")
            return self._registry[key][skip : skip + limit]

    return FakeClient(rows_by_request)


@pytest.fixture
def real_modules():
    """Import real data_reader and gap_detector; restores sys.modules on teardown.

    Purges any MagicMock installed by _setup_mocks for data_reader, gap_detector,
    and tag_library before re-importing so data_reader.py binds the real tag_library.
    """
    import importlib

    _script_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if _script_dir not in sys.path:
        sys.path.insert(0, _script_dir)
    _KEYS = ("src.data_reader", "src.gap_detector", "tag_library")
    _saved = {k: sys.modules.get(k) for k in _KEYS}
    for k in _KEYS:
        if k in sys.modules and isinstance(sys.modules[k], MagicMock):
            del sys.modules[k]
    dr = importlib.import_module("src.data_reader")
    gd = importlib.import_module("src.gap_detector")
    yield {"dr": dr, "gd": gd}
    for k, v in _saved.items():
        if v is None:
            sys.modules.pop(k, None)
        else:
            sys.modules[k] = v


def _make_day_row(
    code,
    date_str,
    target_str,
    model,
    fd,
    q05,
    q25,
    q75,
    q95,
    horizon_value=1,
    horizon_in_year=1,
    flag=None,
):
    """Build a minimal valid DAY row dict for fake client pages."""
    return {
        "code": code,
        "date": date_str,
        "target": target_str,
        "model_type": model,
        "horizon_type": "day",
        "forecasted_discharge": fd,
        "q05": q05,
        "q25": q25,
        "q75": q75,
        "q95": q95,
        "horizon_value": horizon_value,
        "horizon_in_year": horizon_in_year,
        "flag": flag,
    }


def _make_period_row(
    code,
    date_str,
    target_str,
    model,
    horizon_type,
    fd,
    q05,
    q25,
    q75,
    q95,
    horizon_value=1,
    horizon_in_year=1,
    flag=None,
    composition=None,
):
    """Build a minimal valid combined-period row dict."""
    api_ht = "decade" if horizon_type == "decad" else horizon_type
    return {
        "code": code,
        "date": date_str,
        "target": target_str,
        "model_type": model,
        "horizon_type": api_ht,
        "forecasted_discharge": fd,
        "q05": q05,
        "q25": q25,
        "q75": q75,
        "q95": q95,
        "horizon_value": horizon_value,
        "horizon_in_year": horizon_in_year,
        "flag": flag,
        "composition": composition,
    }


class TestRecoveryPreviewRealChain:
    """Exercises the real reader → normalizer → virtual/NE → planner chain.

    Uses fake clients and calls data_reader / gap_detector functions directly.
    No mocks for data_reader, gap_detector, or _normalize_ml_forecasts.
    """

    def test_disabled_api_returns_blocked_result(self, real_modules):
        """SAPPHIRE_API_AVAILABLE=False causes zero client construction."""
        dr = real_modules["dr"]
        original = dr.SAPPHIRE_API_AVAILABLE
        dr.SAPPHIRE_API_AVAILABLE = False
        try:
            # _run_recovery_preview with no injected client should return blocked
            # We test via the data_reader gate directly (no client → DISABLED).
            status, raw, reason = dr.read_day_forecasts_for_recovery(
                None,
                ["19999"],
                "TFT",
                __import__("datetime").date(2026, 1, 1),
                __import__("datetime").date(2026, 1, 5),
                __import__("datetime").date(2026, 1, 5),
            )
            assert status == dr.RECOVERY_STATUS_DISABLED
        finally:
            dr.SAPPHIRE_API_AVAILABLE = original

    def test_disabled_ml_models_zero_reads(self, mock_data, mock_skill):
        """When run_ML_models=false, _get_enabled_ml_models returns empty list."""
        with patch.dict(sys.modules, {}):
            _setup_mocks(mock_data, mock_skill)
            module, spec = import_maintenance_module()
            spec.loader.exec_module(module)
            # run_ML_models=false → empty list → no DAY reads attempted in preview
            with patch.dict(os.environ, {"ieasyhydroforecast_run_ML_models": "false"}):
                result = module._get_enabled_ml_models()
            assert result == []

    def test_enabled_ml_models_returns_canonical_names(self, mock_data, mock_skill):
        """_get_enabled_ml_models normalizes TIDE→TiDE, TSMIXER→TSMixer."""
        with patch.dict(sys.modules, {}):
            _setup_mocks(mock_data, mock_skill)
            module, spec = import_maintenance_module()
            spec.loader.exec_module(module)
            env = {
                "ieasyhydroforecast_run_ML_models": "true",
                "ieasyhydroforecast_available_ML_models": "TFT,TIDE,TSMIXER",
            }
            with patch.dict(os.environ, env):
                result = module._get_enabled_ml_models()
            assert result == ["TFT", "TiDE", "TSMixer"]

    def test_whole_missing_date_empty_combined_creates_candidates(self, real_modules):
        """Empty combined + complete DAY source → ML candidate planned as missing."""
        import datetime

        dr = real_modules["dr"]
        gd = real_modules["gd"]

        # Pentad boundary: Jan 5 is a boundary day (day 5 of month).
        # Coverage threshold for Jan 5 pentad = ceil(0.5 * 5) = 3 targets.
        # Supply all 5 targets (Jan 6-10) to guarantee threshold is met.
        issue = datetime.date(2026, 1, 5)
        target = datetime.date(2026, 1, 6)
        forecast_date = datetime.date(2026, 1, 5)

        # Five DAY rows covering the full Jan 6-10 pentad period.
        day_rows = [
            _make_day_row(
                "19999",
                "2026-01-05",
                f"2026-01-{d:02d}",
                "TFT",
                100.0,
                80.0,
                90.0,
                110.0,
                120.0,
                horizon_value=datetime.date(2026, 1, d).timetuple().tm_yday,
                horizon_in_year=datetime.date(2026, 1, d).timetuple().tm_yday,
            )
            for d in range(6, 11)
        ]

        fake_client = _make_fake_client(
            {
                (
                    "/forecast/",
                    frozenset(
                        {
                            "horizon": "day",
                            "model": "TFT",
                            "code": "19999",
                            "start_date": "2026-01-05",
                            "end_date": "2026-01-05",
                        }.items()
                    ),
                ): day_rows,
            }
        )

        status, raw, reason = dr.read_day_forecasts_for_recovery(
            fake_client,
            ["19999"],
            "TFT",
            issue,
            issue,
            forecast_date,
        )
        assert status == dr.RECOVERY_STATUS_COMPLETE, reason
        assert raw is not None and not raw.empty

        normed = dr._normalize_ml_forecasts(raw, "TFT", "pentad")
        assert not normed.empty, (
            "Normalization produced empty output — tag_library may be absent or "
            "coverage threshold not met; check real_modules fixture and DAY rows"
        )
        assert "code" in normed.columns
        assert "model_short" in normed.columns

        row = normed.iloc[0]
        assert str(row["code"]) == "19999"
        assert row["model_short"] == "TFT"

        # Plan: empty existing (CONFIRMED_EMPTY), one candidate derived from normed row.
        # horizon_value/horizon_in_year come from pentad_in_month/pentad_in_year
        # (computed by _normalize_ml_forecasts as get_pentad(Jan 6)=2 and
        # get_pentad_in_year(Jan 6)=2 respectively).
        candidate = {
            "horizon_type": "pentad",
            "code": "19999",
            "model_type": "TFT",
            "date": issue,
            "target": target,
            "forecasted_discharge": float(row["forecasted_discharge"]),
            "q05": float(row["q05"]),
            "q25": float(row["q25"]),
            "q75": float(row["q75"]),
            "q95": float(row["q95"]),
            "flag": None,
            "composition": None,
            "horizon_value": int(row.get("pentad_in_month", 2)),
            "horizon_in_year": int(row.get("pentad_in_year", 2)),
        }
        authorized_keys = {("pentad", "19999", "TFT", issue, target)}

        plan = gd.plan_period_refresh(
            [],
            [candidate],
            authorized_keys,
            set(),
            start_date=issue,
            end_date=issue,
            forecast_date=forecast_date,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None, plan.plan_error
        assert ("pentad", "19999", "TFT", issue, target) in plan.missing

    def test_source_failed_read_blocks_model_not_others(self, real_modules):
        """FAILED DAY read for one model does not affect the other model's read."""
        import datetime

        dr = real_modules["dr"]

        issue = datetime.date(2026, 1, 5)
        forecast_date = datetime.date(2026, 1, 5)

        # Fake client returns nothing for TFT (→ CONFIRMED_EMPTY is fine;
        # FAILED is tested by passing a client that raises)
        class ErrorClient:
            def _get(self, path, params=None):
                raise RuntimeError("network error")

        status_tft, _, _ = dr.read_day_forecasts_for_recovery(
            ErrorClient(),
            ["19999"],
            "TFT",
            issue,
            issue,
            forecast_date,
        )
        # Should fail, not empty
        assert status_tft == dr.RECOVERY_STATUS_FAILED

        # Other model (TiDE) with a good fake client is independent.
        # A single target row is sufficient to prove the read succeeds; coverage
        # is a normalizer concern, not a reader concern.
        tide_row = _make_day_row(
            "19999",
            "2026-01-05",
            "2026-01-06",
            "TiDE",
            105.0,
            85.0,
            95.0,
            115.0,
            125.0,
            horizon_value=6,
            horizon_in_year=6,
        )
        good_client = _make_fake_client(
            {
                (
                    "/forecast/",
                    frozenset(
                        {
                            "horizon": "day",
                            "model": "TiDE",
                            "code": "19999",
                            "start_date": "2026-01-05",
                            "end_date": "2026-01-05",
                        }.items()
                    ),
                ): [tide_row],
            }
        )
        status_tide, raw_tide, reason_tide = dr.read_day_forecasts_for_recovery(
            good_client,
            ["19999"],
            "TiDE",
            issue,
            issue,
            forecast_date,
        )
        assert status_tide == dr.RECOVERY_STATUS_COMPLETE, reason_tide
        assert raw_tide is not None and not raw_tide.empty

    def test_existing_complete_record_protected_not_refresh(self, real_modules):
        """Existing complete period record → plan.protected, not authorized_refresh."""
        import datetime

        gd = real_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        target = datetime.date(2026, 1, 6)
        forecast_date = datetime.date(2026, 1, 5)

        # For pentad target Jan 6 (day=6, ≤10): horizon_value=2 (second pentad in
        # January), horizon_in_year=2 (second pentad of the year).
        # _plan_valid_meta_fields checks exp_hv == hv and exp_hiy == hiy strictly.
        candidate = {
            "horizon_type": "pentad",
            "code": "19999",
            "model_type": "TFT",
            "date": issue,
            "target": target,
            "forecasted_discharge": 100.0,
            "q05": 80.0,
            "q25": 90.0,
            "q75": 110.0,
            "q95": 120.0,
            "flag": None,
            "composition": None,
            "horizon_value": 2,
            "horizon_in_year": 2,
        }
        # Existing record with a DIFFERENT forecasted_discharge → protected
        existing = {
            "horizon_type": "pentad",
            "code": "19999",
            "model_type": "TFT",
            "date": issue,
            "target": target,
            "forecasted_discharge": 999.0,  # differs from candidate → protected
            "q05": 80.0,
            "q25": 90.0,
            "q75": 110.0,
            "q95": 120.0,
            "flag": 2,
            "composition": "some_comp",
            "horizon_value": 2,
            "horizon_in_year": 2,
        }
        authorized_keys = {("pentad", "19999", "TFT", issue, target)}

        plan = gd.plan_period_refresh(
            [existing],
            [candidate],
            authorized_keys,
            set(),  # no explicit_refresh_keys → differ → protected
            start_date=issue,
            end_date=issue,
            forecast_date=forecast_date,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is None, plan.plan_error
        # With no explicit_refresh_keys, a differing existing record is protected
        assert ("pentad", "19999", "TFT", issue, target) in plan.protected
        assert len(plan.authorized_refresh) == 0

    def test_em_always_blocked_skill_vintage(self, mock_data, mock_skill):
        """EM constant is 'skill_vintage_unresolved'; preview result carries it."""
        with patch.dict(sys.modules, {}):
            _setup_mocks(mock_data, mock_skill)
            module, spec = import_maintenance_module()
            spec.loader.exec_module(module)
            assert module._EM_BLOCKED_REASON_SKILL == "skill_vintage_unresolved"
            assert module._RECOVERY_PREVIEW_DEADLINE_SECONDS > 0
            # HorizonPreviewResult carries em_blocked_reason
            hr = module.HorizonPreviewResult(
                plan=None,
                source_statuses={},
                candidate_count=0,
                missing_count=0,
                protected_count=0,
                em_blocked_reason=module._EM_BLOCKED_REASON_SKILL,
                diagnostics=[],
            )
            assert hr.em_blocked_reason == "skill_vintage_unresolved"

    def test_confirmed_empty_combined_read(self, real_modules):
        """Empty combined period read returns CONFIRMED_EMPTY, not FAILED."""
        import datetime

        dr = real_modules["dr"]

        issue = datetime.date(2026, 1, 5)
        forecast_date = datetime.date(2026, 1, 5)

        # Register the expected endpoint with empty rows → CONFIRMED_EMPTY.
        empty_client = _make_fake_client(
            {
                (
                    "/forecast/",
                    frozenset(
                        {
                            "horizon": "pentad",
                            "model": "TFT",
                            "code": "19999",
                            "start_date": "2026-01-05",
                            "end_date": "2026-01-05",
                        }.items()
                    ),
                ): [],
            }
        )

        status, raw, reason = dr.read_combined_period_forecasts_for_recovery(
            empty_client,
            ["19999"],
            "TFT",
            "pentad",
            issue,
            issue,
            forecast_date,
        )
        assert status == dr.RECOVERY_STATUS_EMPTY
        assert raw is not None
        assert raw.empty

    def test_shared_deadline_blocks_late_reads(self, real_modules):
        """An already-expired deadline causes FAILED status immediately."""
        import datetime
        import time

        dr = real_modules["dr"]

        issue = datetime.date(2026, 1, 5)
        forecast_date = datetime.date(2026, 1, 5)

        # Set deadline in the past
        past_deadline = time.monotonic() - 1.0

        class UnreachableClient:
            def _get(self, *a, **kw):
                raise AssertionError("should not be called with expired deadline")

        status, raw, reason = dr.read_day_forecasts_for_recovery(
            UnreachableClient(),
            ["19999"],
            "TFT",
            issue,
            issue,
            forecast_date,
            total_deadline=past_deadline,
        )
        assert status == dr.RECOVERY_STATUS_FAILED
        assert "deadline" in reason.lower()

    def test_lr_read_independent_of_ml_enablement(self, real_modules):
        """LR read is independent — can succeed even when ML is disabled."""
        import datetime

        dr = real_modules["dr"]

        issue = datetime.date(2026, 1, 5)
        forecast_date = datetime.date(2026, 1, 5)

        lr_row = {
            "code": "19999",
            "date": "2026-01-05",
            "horizon_type": "pentad",
            "horizon_value": 2,
            "horizon_in_year": 2,
            "forecasted_discharge": 100.0,
        }
        fake_client = _make_fake_client(
            {
                (
                    "/lr-forecast/",
                    frozenset(
                        {
                            "horizon": "pentad",
                            "code": "19999",
                            "start_date": "2026-01-05",
                            "end_date": "2026-01-05",
                        }.items()
                    ),
                ): [lr_row],
            }
        )

        with patch.dict(os.environ, {"ieasyhydroforecast_run_ML_models": "false"}):
            status, raw, reason = dr.read_lr_forecasts_for_recovery(
                fake_client,
                ["19999"],
                "pentad",
                issue,
                issue,
                forecast_date,
            )
        assert status == dr.RECOVERY_STATUS_COMPLETE, reason
        assert raw is not None and not raw.empty


# ===========================================================================
# NEWP2: _run_recovery_preview integration tests — actual preview calls
# ===========================================================================
# These tests call module._run_recovery_preview (or _run_recovery_preview_for_horizon)
# directly with a fake client.  No MagicMock for data_reader, gap_detector,
# setup_library, or NE/virtual helpers.  Sinks are patched to raise so no data
# is written.
# ===========================================================================


@pytest.fixture
def preview_chain_modules(monkeypatch):
    """Load maintenance module with real setup_library, data_reader, gap_detector.

    Installs real tag_library, setup_library, src.data_reader, src.gap_detector
    into sys.modules then loads postprocessing_maintenance under a fresh name.
    Restores sys.modules on teardown.  Returns dict with 'module', 'dr', 'gd'.
    """
    import importlib

    _script_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    _forecast_dir = os.path.abspath(os.path.join(_script_dir, "..", "iEasyHydroForecast"))
    for _p in (_script_dir, _forecast_dir):
        if _p not in sys.path:
            sys.path.insert(0, _p)

    # Keys that must be real (not MagicMock) for the chain to work.
    _real_keys = (
        "tag_library",
        "setup_library",
        "src.data_reader",
        "src.gap_detector",
        "src.horizon_config",
    )
    _saved = {k: sys.modules.get(k) for k in _real_keys}

    # Remove any MagicMock stubs left by other tests.
    for k in _real_keys:
        if k in sys.modules and isinstance(sys.modules[k], MagicMock):
            del sys.modules[k]

    # Import real modules (order matters: tag_library before data_reader).
    tl_mod = importlib.import_module("tag_library")
    sl_mod = importlib.import_module("setup_library")
    dr_mod = importlib.import_module("src.data_reader")
    gd_mod = importlib.import_module("src.gap_detector")
    hc_mod = importlib.import_module("src.horizon_config")

    # Install minimal mocks for the parts the maintenance module needs at
    # import time but that we don't exercise in these tests.
    mock_pt = MagicMock()
    mock_pt.TimingStats.return_value.summary.return_value = ([], 0)
    mock_sm = MagicMock()
    mock_fw = MagicMock()
    mock_fw.save_forecast_data.side_effect = AssertionError(
        "save_forecast_data must not be called during recovery preview"
    )
    mock_aw = MagicMock()
    mock_aw._write_combined_forecast_to_api.side_effect = AssertionError(
        "_write_combined_forecast_to_api must not be called during recovery preview"
    )
    mock_aw.SAPPHIRE_API_AVAILABLE = True

    mock_src = MagicMock()
    mock_src.postprocessing_tools = mock_pt
    mock_src.data_reader = dr_mod
    mock_src.ensemble_calculator = MagicMock()
    mock_src.gap_detector = gd_mod
    mock_src.skill_metrics = mock_sm
    mock_src.file_writer = mock_fw
    mock_src.api_writer = mock_aw
    mock_src.horizon_config = hc_mod

    _mock_keys = {
        "src": mock_src,
        "src.postprocessing_tools": mock_pt,
        "src.ensemble_calculator": MagicMock(),
        "src.skill_metrics": mock_sm,
        "src.file_writer": mock_fw,
        "src.api_writer": mock_aw,
    }
    # Install mocks for non-real keys.
    _saved_mock = {k: sys.modules.get(k) for k in _mock_keys}
    for k, v in _mock_keys.items():
        sys.modules[k] = v

    # Re-install real modules so maintenance module import sees them.
    sys.modules["tag_library"] = tl_mod
    sys.modules["setup_library"] = sl_mod
    sys.modules["src.data_reader"] = dr_mod
    sys.modules["src.gap_detector"] = gd_mod
    sys.modules["src.horizon_config"] = hc_mod

    # Load the maintenance module fresh under a unique name.
    _mod_name = "postprocessing_maintenance_p2_chain"
    sys.modules.pop(_mod_name, None)
    spec = importlib.util.spec_from_file_location(
        _mod_name,
        os.path.join(_script_dir, "postprocessing_maintenance.py"),
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[_mod_name] = module
    spec.loader.exec_module(module)

    yield {"module": module, "dr": dr_mod, "gd": gd_mod}

    # Teardown: assert sinks were never called (even on test failure), then restore.
    _sink_error = None
    try:
        assert mock_fw.save_forecast_data.call_count == 0, (
            f"save_forecast_data called {mock_fw.save_forecast_data.call_count} "
            "time(s) during recovery preview"
        )
        assert mock_aw._write_combined_forecast_to_api.call_count == 0, (
            f"_write_combined_forecast_to_api called "
            f"{mock_aw._write_combined_forecast_to_api.call_count} "
            "time(s) during recovery preview"
        )
    except AssertionError as _e:
        _sink_error = _e
    finally:
        for k, v in _saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v
        for k, v in _saved_mock.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v
        sys.modules.pop(_mod_name, None)
    if _sink_error is not None:
        raise _sink_error


def _make_full_day_fan_pentad(code, issue_str, model):
    """Return 5 DAY rows covering the full Jan pentad fan for issue_str (Jan 5)."""
    import datetime as _dt

    # issue Jan 5 → targets Jan 6-10 (5 days in period, ceil(0.5*5)=3 threshold)
    return [
        _make_day_row(
            code,
            issue_str,
            f"2026-01-{d:02d}",
            model,
            100.0 + d,
            80.0,
            90.0,
            110.0,
            120.0,
            horizon_value=_dt.date(2026, 1, d).timetuple().tm_yday,
            horizon_in_year=_dt.date(2026, 1, d).timetuple().tm_yday,
        )
        for d in range(6, 11)
    ]


def _make_full_day_fan_decad(code, issue_str, model):
    """Return 10 DAY rows covering the full Jan decad fan for issue_str (Jan 10)."""
    import datetime as _dt

    # issue Jan 10 → targets Jan 11-20 (10 days, ceil(0.5*10)=5 threshold)
    return [
        _make_day_row(
            code,
            issue_str,
            f"2026-01-{d:02d}",
            model,
            100.0 + d,
            80.0,
            90.0,
            110.0,
            120.0,
            horizon_value=_dt.date(2026, 1, d).timetuple().tm_yday,
            horizon_in_year=_dt.date(2026, 1, d).timetuple().tm_yday,
        )
        for d in range(11, 21)
    ]


class TestNewP2ActualPreviewCalls:
    """P2 integration: calls module._run_recovery_preview, not manual pipeline."""

    @pytest.mark.parametrize(
        "horizon,issue_str,fan_func",
        [
            ("PENTAD", "2026-01-05", _make_full_day_fan_pentad),
            ("DECAD", "2026-01-10", _make_full_day_fan_decad),
        ],
    )
    def test_A_three_models_full_fan_no_existing_all_missing(
        self,
        preview_chain_modules,
        monkeypatch,
        horizon,
        issue_str,
        fan_func,
    ):
        """3 ML models + NE: complete DAY fans, no existing period rows → all missing.

        Contract:
        - candidate_payloads contains individual rows for TFT, TiDE, TSMixer + NE row.
        - plan.missing has exact (horizon_type, code, canonical_model, issue, target+1) keys
          for TFT, TIDE, TSMIXER, NE.
        - NE composition is not None (real NE helper ran).
        - No sinks called (file_writer, api_writer).
        """
        import datetime

        module = preview_chain_modules["module"]

        code = "19999"
        issue = datetime.date.fromisoformat(issue_str)
        ht_lower = horizon.lower()
        api_ht = "decade" if ht_lower == "decad" else ht_lower

        # Build fake client registry: DAY rows for TFT/TiDE/TSMixer, empty period rows.
        registry = {}
        for model in ["TFT", "TiDE", "TSMixer"]:
            rows = fan_func(code, issue_str, model)
            registry[
                (
                    "/forecast/",
                    frozenset(
                        {
                            "horizon": "day",
                            "model": model,
                            "code": code,
                            "start_date": issue_str,
                            "end_date": issue_str,
                        }.items()
                    ),
                )
            ] = rows
            # Existing combined-period read: empty.
            registry[
                (
                    "/forecast/",
                    frozenset(
                        {
                            "horizon": api_ht,
                            "model": model,
                            "code": code,
                            "start_date": issue_str,
                            "end_date": issue_str,
                        }.items()
                    ),
                )
            ] = []
        # NE existing period read: empty.
        registry[
            (
                "/forecast/",
                frozenset(
                    {
                        "horizon": api_ht,
                        "model": "NE",
                        "code": code,
                        "start_date": issue_str,
                        "end_date": issue_str,
                    }.items()
                ),
            )
        ] = []
        # LR read: empty.
        registry[
            (
                "/lr-forecast/",
                frozenset(
                    {
                        "horizon": ht_lower,
                        "code": code,
                        "start_date": issue_str,
                        "end_date": issue_str,
                    }.items()
                ),
            )
        ] = []

        fake_client = _make_fake_client(registry)

        # Patch _read_station_codes to return our test code.
        monkeypatch.setattr(module, "_read_station_codes", lambda cfg: [code])

        env = {
            "SAPPHIRE_PREDICTION_MODE": horizon,
            "SAPPHIRE_API_ENABLED": "true",
            "ieasyhydroforecast_run_ML_models": "true",
            "ieasyhydroforecast_available_ML_models": "TFT,TIDE,TSMIXER",
        }
        with patch.dict(os.environ, env, clear=False):
            result = module._run_recovery_preview(
                horizon,
                issue,
                issue,
                issue,
                _client=fake_client,
                _virtual_sources={},
            )

        assert result.blocked_reason is None, f"preview blocked: {result.blocked_reason}"
        assert ht_lower in result.horizon_results, (
            f"missing horizon key: {result.horizon_results.keys()}"
        )
        hr = result.horizon_results[ht_lower]
        assert hr.plan is not None
        assert hr.plan.plan_error is None, hr.plan.plan_error

        # candidate_payloads must have TFT, TiDE/TSMixer models + NE
        models_in_candidates = {p["model_type"] for p in hr.candidate_payloads}
        assert "TFT" in models_in_candidates, models_in_candidates
        # TiDE / TSMixer may appear as their display name from normalizer
        tide_present = any(m in models_in_candidates for m in ("TiDE", "TIDE"))
        tsmixer_present = any(m in models_in_candidates for m in ("TSMixer", "TSMIXER"))
        assert tide_present, f"TiDE missing from candidates: {models_in_candidates}"
        assert tsmixer_present, f"TSMixer missing from candidates: {models_in_candidates}"
        ne_candidates = [p for p in hr.candidate_payloads if p["model_type"] == "NE"]
        assert ne_candidates, "NE not in candidate_payloads"
        assert ne_candidates[0]["composition"] is not None, "NE composition is None"

        # plan.missing must have canonical keys for all 4 model types
        missing_model_keys = {k[2] for k in hr.plan.missing}
        assert "TFT" in missing_model_keys, f"TFT not in plan.missing: {missing_model_keys}"
        assert "TIDE" in missing_model_keys, f"TIDE not in plan.missing: {missing_model_keys}"
        assert "TSMIXER" in missing_model_keys, f"TSMIXER not in plan.missing: {missing_model_keys}"
        assert "NE" in missing_model_keys, f"NE not in plan.missing: {missing_model_keys}"

        # All missing keys must have target = issue + 1 day
        target_expected = issue + datetime.timedelta(days=1)
        for k in hr.plan.missing:
            assert k[4] == target_expected, f"unexpected target in missing key {k}"

    def test_B_tft_raises_tide_complete_only_tide_missing(
        self,
        preview_chain_modules,
        monkeypatch,
    ):
        """TFT DAY raises → blocked; TiDE complete → TiDE in missing, NE blocked.

        Also: TiDE existing fails + TFT complete → TFT missing survives failed TiDE existing.
        """
        import datetime

        module = preview_chain_modules["module"]

        code = "19999"
        issue = datetime.date(2026, 1, 5)
        issue_str = "2026-01-05"

        # Client that raises for TFT DAY reads, returns TiDE rows normally.
        tide_rows = _make_full_day_fan_pentad(code, issue_str, "TiDE")

        class TftRaisesClient:
            def _get(self, path, params=None):
                params = params or {}
                if params.get("model") == "TFT":
                    raise RuntimeError("TFT network error")
                # TiDE DAY fan
                if params.get("model") == "TiDE" and params.get("horizon") == "day":
                    skip = params.get("skip", 0)
                    limit = params.get("limit", 500)
                    return tide_rows[skip : skip + limit]
                # Existing period read for TiDE: empty
                if params.get("horizon") == "pentad" and params.get("model") == "TiDE":
                    return []
                # LR read: empty
                if path == "/lr-forecast/":
                    return []
                raise AssertionError(f"Unexpected _get({path!r}, {params!r})")

        monkeypatch.setattr(module, "_read_station_codes", lambda cfg: [code])

        env = {
            "SAPPHIRE_PREDICTION_MODE": "PENTAD",
            "SAPPHIRE_API_ENABLED": "true",
            "ieasyhydroforecast_run_ML_models": "true",
            "ieasyhydroforecast_available_ML_models": "TFT,TIDE",
        }
        with patch.dict(os.environ, env, clear=False):
            result = module._run_recovery_preview(
                "PENTAD",
                issue,
                issue,
                issue,
                _client=TftRaisesClient(),
                _virtual_sources={},
            )

        assert result.blocked_reason is None, result.blocked_reason
        hr = result.horizon_results["pentad"]
        assert hr.plan is not None

        # TFT must be in blocked_units; TiDE must NOT be blocked.
        tft_blocked = any(m == "TFT" for (_, m) in hr.blocked_units)
        assert tft_blocked, f"TFT not blocked: {hr.blocked_units}"
        tide_blocked = any(m in ("TiDE", "TIDE") for (_, m) in hr.blocked_units)
        assert not tide_blocked, f"TiDE unexpectedly blocked: {hr.blocked_units}"

        # TiDE must appear in plan.missing.
        missing_models = {k[2] for k in hr.plan.missing}
        assert "TIDE" in missing_models, f"TiDE not in plan.missing: {missing_models}"

        # NE must be blocked (TFT source unknown → NE blocked for that code).
        ne_blocked = any(m == "NE" for (_, m) in hr.blocked_units)
        assert ne_blocked, f"NE not blocked despite failed TFT: {hr.blocked_units}"

        # No NE in plan.missing (blocked NE rows removed before planner).
        assert "NE" not in missing_models, f"NE unexpectedly in plan.missing: {missing_models}"

        # Subcase: TiDE existing read fails, TFT complete → TFT still appears in missing.
        tide_rows_2 = _make_full_day_fan_pentad(code, issue_str, "TiDE")
        tft_rows_2 = _make_full_day_fan_pentad(code, issue_str, "TFT")

        class TiDEExistingFailsClient:
            def _get(self, path, params=None):
                params = params or {}
                model = params.get("model", "")
                horizon = params.get("horizon", "")
                if horizon == "day" and model == "TFT":
                    skip, limit = params.get("skip", 0), params.get("limit", 500)
                    return tft_rows_2[skip : skip + limit]
                if horizon == "day" and model == "TiDE":
                    skip, limit = params.get("skip", 0), params.get("limit", 500)
                    return tide_rows_2[skip : skip + limit]
                if horizon == "pentad" and model == "TFT":
                    return []  # existing TFT: empty
                if horizon == "pentad" and model == "TiDE":
                    raise RuntimeError("TiDE existing read error")
                if path == "/lr-forecast/":
                    return []
                raise AssertionError(f"Unexpected _get({path!r}, {params!r})")

        with patch.dict(os.environ, env, clear=False):
            result2 = module._run_recovery_preview(
                "PENTAD",
                issue,
                issue,
                issue,
                _client=TiDEExistingFailsClient(),
                _virtual_sources={},
            )

        assert result2.blocked_reason is None, result2.blocked_reason
        hr2 = result2.horizon_results["pentad"]
        assert hr2.plan is not None
        missing2 = {k[2] for k in hr2.plan.missing}
        assert "TFT" in missing2, f"TFT not in plan.missing after TiDE existing failure: {missing2}"
        # TiDE is blocked (existing read failed) → not in missing
        assert "TIDE" not in missing2, f"TiDE in missing despite failed existing read: {missing2}"

    def test_C_virtual_station_weighted_sum(
        self,
        preview_chain_modules,
        monkeypatch,
        tmp_path,
    ):
        """Virtual station V from physical A+B with weights 2/3, combinationFunction=sum.

        Contract:
        - Fake DAY rows for A and B complete; direct V reads empty.
        - Real virtual helper computes V.
        - Only V appears in candidate_payloads (A/B not in output allowlist).
        - plan.missing codes contain only V, not A or B.
        - NE composition for V is not None (real NE ran).
        - Config reads from actual temp JSON file (no injected mapping bypass).
        """
        import datetime
        import json as _json

        module = preview_chain_modules["module"]

        code_a = "19991"
        code_b = "19992"
        code_v = "19993"
        issue = datetime.date(2026, 1, 5)
        issue_str = "2026-01-05"

        # Write the virtual station config to a real temp file.
        vconfig = {
            "virtualStations": {
                code_v: {
                    "combinationFunction": "sum",
                    "weightByStation": {
                        code_a: 2,
                        code_b: 3,
                    },
                }
            }
        }
        vconfig_file = tmp_path / "virtual_stations.json"
        vconfig_file.write_text(_json.dumps(vconfig))

        rows_a = _make_full_day_fan_pentad(code_a, issue_str, "TFT")
        rows_b = _make_full_day_fan_pentad(code_b, issue_str, "TFT")

        class VirtualClient:
            def _get(self, path, params=None):
                params = params or {}
                model = params.get("model", "")
                horizon = params.get("horizon", "")
                req_code = params.get("code", "")
                if horizon == "day" and model == "TFT" and req_code == code_a:
                    skip, limit = params.get("skip", 0), params.get("limit", 500)
                    return rows_a[skip : skip + limit]
                if horizon == "day" and model == "TFT" and req_code == code_b:
                    skip, limit = params.get("skip", 0), params.get("limit", 500)
                    return rows_b[skip : skip + limit]
                # V direct DAY: empty (V is virtual, no direct ML rows)
                if horizon == "day" and model == "TFT" and req_code == code_v:
                    return []
                # Existing period reads for V: empty
                if horizon == "pentad" and req_code == code_v:
                    return []
                if path == "/lr-forecast/":
                    return []
                raise AssertionError(f"Unexpected _get({path!r}, {params!r})")

        # Patch station codes to return only V (V is the output allowlist).
        monkeypatch.setattr(module, "_read_station_codes", lambda cfg: [code_v])

        env = {
            "SAPPHIRE_PREDICTION_MODE": "PENTAD",
            "SAPPHIRE_API_ENABLED": "true",
            "ieasyhydroforecast_run_ML_models": "true",
            "ieasyhydroforecast_available_ML_models": "TFT",
            "ieasyforecast_virtual_stations": vconfig_file.name,
            "ieasyforecast_configuration_path": str(tmp_path),
        }
        with patch.dict(os.environ, env, clear=False):
            result = module._run_recovery_preview(
                "PENTAD",
                issue,
                issue,
                issue,
                _client=VirtualClient(),
                # Do NOT inject _virtual_sources — let it read from the real config file.
            )

        assert result.blocked_reason is None, result.blocked_reason
        hr = result.horizon_results["pentad"]
        assert hr.plan is not None
        assert hr.plan.plan_error is None, hr.plan.plan_error

        # Only V must appear in candidate_payloads.
        candidate_codes = {p["code"] for p in hr.candidate_payloads}
        assert code_v in candidate_codes, f"V not in candidates: {candidate_codes}"
        assert code_a not in candidate_codes, f"physical A in candidates: {candidate_codes}"
        assert code_b not in candidate_codes, f"physical B in candidates: {candidate_codes}"

        # NE composition for V must not be None.
        ne_v = [p for p in hr.candidate_payloads if p["model_type"] == "NE" and p["code"] == code_v]
        assert ne_v, "No NE candidate for virtual station V"
        assert ne_v[0]["composition"] is not None, "NE composition for V is None"

        # plan.missing contains only V codes (not A/B).
        missing_codes = {k[1] for k in hr.plan.missing}
        assert code_v in missing_codes, f"V not in plan.missing: {missing_codes}"
        assert code_a not in missing_codes, f"physical A in plan.missing: {missing_codes}"
        assert code_b not in missing_codes, f"physical B in plan.missing: {missing_codes}"

    def test_D_api_disabled_zero_client_calls(
        self,
        preview_chain_modules,
        monkeypatch,
    ):
        """SAPPHIRE_API_ENABLED=false → blocked result, injected client never called."""
        import datetime

        module = preview_chain_modules["module"]

        issue = datetime.date(2026, 1, 5)

        class GuardClient:
            def _get(self, *a, **kw):
                raise AssertionError("client._get called despite API disabled")

        monkeypatch.setattr(module, "_read_station_codes", lambda cfg: ["19999"])

        with patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "false"}, clear=False):
            result = module._run_recovery_preview(
                "PENTAD",
                issue,
                issue,
                issue,
                _client=GuardClient(),
                _virtual_sources={},
            )

        assert result.blocked_reason is not None, "expected blocked_reason with API disabled"
        assert (
            "false" in result.blocked_reason.lower() or "disabled" in result.blocked_reason.lower()
        )
        assert result.horizon_results == {}


# ===========================================================================
# NEWP2: Unusable-output diagnostics tests
# ===========================================================================


class TestNewP2UnusableDiagnostics:
    """P2 unusable-unit diagnostics: absent expected outputs are reported explicitly."""

    def test_NEWP2_all_day_empty_no_actionable_unusable_diagnostic(
        self,
        preview_chain_modules,
        monkeypatch,
    ):
        """All DAY reads return empty → no candidate_payloads → unusable_units populated."""
        import datetime

        module = preview_chain_modules["module"]

        code = "19999"
        issue = datetime.date(2026, 1, 5)
        issue_str = "2026-01-05"

        registry = {
            (
                "/forecast/",
                frozenset(
                    {
                        "horizon": "day",
                        "model": "TFT",
                        "code": code,
                        "start_date": issue_str,
                        "end_date": issue_str,
                    }.items()
                ),
            ): [],
            (
                "/lr-forecast/",
                frozenset(
                    {
                        "horizon": "pentad",
                        "code": code,
                        "start_date": issue_str,
                        "end_date": issue_str,
                    }.items()
                ),
            ): [],
        }
        fake_client = _make_fake_client(registry)
        monkeypatch.setattr(module, "_read_station_codes", lambda cfg: [code])

        env = {
            "SAPPHIRE_API_ENABLED": "true",
            "ieasyhydroforecast_run_ML_models": "true",
            "ieasyhydroforecast_available_ML_models": "TFT",
        }
        with patch.dict(os.environ, env, clear=False):
            result = module._run_recovery_preview(
                "PENTAD",
                issue,
                issue,
                issue,
                _client=fake_client,
                _virtual_sources={},
            )

        assert result.blocked_reason is None
        hr = result.horizon_results["pentad"]
        assert hr.candidate_count == 0, "no candidates expected"
        assert len(hr.unusable_units) > 0, "unusable_units must be populated for empty DAY"
        # Every unusable key should reference code and TFT or NE
        models_in_unusable = {k[2] for k in hr.unusable_units}
        assert "TFT" in models_in_unusable or "NE" in models_in_unusable, models_in_unusable
        # Reason for TFT should mention confirmed empty
        tft_entry = next((v for k, v in hr.unusable_units.items() if k[2] == "TFT"), None)
        assert tft_entry is not None
        assert "empty" in tft_entry.lower(), f"unexpected reason: {tft_entry}"

    def test_NEWP2_one_day_target_insufficient_unusable(
        self,
        preview_chain_modules,
        monkeypatch,
    ):
        """Only 1 DAY target row (below coverage threshold) → normalizer empty → unusable."""
        import datetime

        module = preview_chain_modules["module"]

        code = "19999"
        issue = datetime.date(2026, 1, 5)
        issue_str = "2026-01-05"

        # Supply only 1 target row — Jan 5 pentad needs ceil(0.5*5)=3; 1 is insufficient.
        one_row = [
            _make_day_row(
                code,
                issue_str,
                "2026-01-06",
                "TFT",
                100.0,
                80.0,
                90.0,
                110.0,
                120.0,
                horizon_value=6,
                horizon_in_year=6,
            )
        ]

        registry = {
            (
                "/forecast/",
                frozenset(
                    {
                        "horizon": "day",
                        "model": "TFT",
                        "code": code,
                        "start_date": issue_str,
                        "end_date": issue_str,
                    }.items()
                ),
            ): one_row,
            (
                "/lr-forecast/",
                frozenset(
                    {
                        "horizon": "pentad",
                        "code": code,
                        "start_date": issue_str,
                        "end_date": issue_str,
                    }.items()
                ),
            ): [],
        }
        fake_client = _make_fake_client(registry)
        monkeypatch.setattr(module, "_read_station_codes", lambda cfg: [code])

        env = {
            "SAPPHIRE_API_ENABLED": "true",
            "ieasyhydroforecast_run_ML_models": "true",
            "ieasyhydroforecast_available_ML_models": "TFT",
        }
        with patch.dict(os.environ, env, clear=False):
            result = module._run_recovery_preview(
                "PENTAD",
                issue,
                issue,
                issue,
                _client=fake_client,
                _virtual_sources={},
            )

        assert result.blocked_reason is None
        hr = result.horizon_results["pentad"]
        # DAY read was COMPLETE (rows returned) but normalizer rejected them → unusable
        assert len(hr.unusable_units) > 0, "unusable_units must be populated"
        tft_entry = next((v for k, v in hr.unusable_units.items() if k[2] == "TFT"), None)
        assert tft_entry is not None
        assert "normalizer" in tft_entry.lower() or "eligible" in tft_entry.lower(), tft_entry

    def test_NEWP2_two_boundary_dates_one_complete_missing_other_unusable(
        self,
        preview_chain_modules,
        monkeypatch,
    ):
        """Bounds [Jan 5, Jan 10]: Jan 5 full fan → candidate, Jan 10 empty → unusable.

        Jan 5 and Jan 10 are valid pentad boundaries (day 5, day 10).
        Jan 6–9 are NOT boundaries and must NOT appear in unusable_units.
        Jan 10 has no DAY data → confirmed-empty unusable entry.
        """
        import datetime

        module = preview_chain_modules["module"]

        code = "19999"
        start = datetime.date(2026, 1, 5)
        end = datetime.date(2026, 1, 10)
        forecast_date = datetime.date(2026, 1, 10)
        start_str, end_str = "2026-01-05", "2026-01-10"

        jan5_rows = _make_full_day_fan_pentad(code, "2026-01-05", "TFT")

        registry = {
            (
                "/forecast/",
                frozenset(
                    {
                        "horizon": "day",
                        "model": "TFT",
                        "code": code,
                        "start_date": start_str,
                        "end_date": end_str,
                    }.items()
                ),
            ): jan5_rows,
            # Existing period reads: empty for both issue dates
            (
                "/forecast/",
                frozenset(
                    {
                        "horizon": "pentad",
                        "model": "TFT",
                        "code": code,
                        "start_date": start_str,
                        "end_date": end_str,
                    }.items()
                ),
            ): [],
            (
                "/forecast/",
                frozenset(
                    {
                        "horizon": "pentad",
                        "model": "NE",
                        "code": code,
                        "start_date": start_str,
                        "end_date": end_str,
                    }.items()
                ),
            ): [],
            (
                "/lr-forecast/",
                frozenset(
                    {
                        "horizon": "pentad",
                        "code": code,
                        "start_date": start_str,
                        "end_date": end_str,
                    }.items()
                ),
            ): [],
        }
        fake_client = _make_fake_client(registry)
        monkeypatch.setattr(module, "_read_station_codes", lambda cfg: [code])

        env = {
            "SAPPHIRE_API_ENABLED": "true",
            "ieasyhydroforecast_run_ML_models": "true",
            "ieasyhydroforecast_available_ML_models": "TFT",
        }
        with patch.dict(os.environ, env, clear=False):
            result = module._run_recovery_preview(
                "PENTAD",
                forecast_date,
                start,
                end,
                _client=fake_client,
                _virtual_sources={},
            )

        assert result.blocked_reason is None
        hr = result.horizon_results["pentad"]
        assert hr.plan is not None

        # Jan 5 TFT must appear in plan.missing (candidate derived, no existing)
        missing_keys = hr.plan.missing
        jan5_tft_missing = any(
            k[1] == code and k[2] == "TFT" and k[3] == start for k in missing_keys
        )
        assert jan5_tft_missing, f"Jan 5 TFT not in plan.missing: {missing_keys}"

        # Jan 10 must appear in unusable_units (valid boundary but no candidate derived)
        jan10 = datetime.date(2026, 1, 10)
        jan10_unusable = {k: v for k, v in hr.unusable_units.items() if k[3] == jan10}
        assert jan10_unusable, (
            f"Jan 10 issue date not in unusable_units. All unusable: {hr.unusable_units}"
        )

        # Jan 6–9 must NOT appear in unusable_units (not valid pentad boundaries)
        for _day in (6, 7, 8, 9):
            _date = datetime.date(2026, 1, _day)
            _bad = {k: v for k, v in hr.unusable_units.items() if k[3] == _date}
            assert not _bad, f"Jan {_day} (non-boundary) unexpectedly in unusable_units: {_bad}"

    def test_NEWP2_valid_candidate_no_unusable(
        self,
        preview_chain_modules,
        monkeypatch,
    ):
        """Successfully derived candidate is NOT in unusable_units."""
        import datetime

        module = preview_chain_modules["module"]

        code = "19999"
        issue = datetime.date(2026, 1, 5)
        issue_str = "2026-01-05"

        day_rows = _make_full_day_fan_pentad(code, issue_str, "TFT")

        registry = {
            (
                "/forecast/",
                frozenset(
                    {
                        "horizon": "day",
                        "model": "TFT",
                        "code": code,
                        "start_date": issue_str,
                        "end_date": issue_str,
                    }.items()
                ),
            ): day_rows,
            (
                "/forecast/",
                frozenset(
                    {
                        "horizon": "pentad",
                        "model": "TFT",
                        "code": code,
                        "start_date": issue_str,
                        "end_date": issue_str,
                    }.items()
                ),
            ): [],
            (
                "/forecast/",
                frozenset(
                    {
                        "horizon": "pentad",
                        "model": "NE",
                        "code": code,
                        "start_date": issue_str,
                        "end_date": issue_str,
                    }.items()
                ),
            ): [],
            (
                "/lr-forecast/",
                frozenset(
                    {
                        "horizon": "pentad",
                        "code": code,
                        "start_date": issue_str,
                        "end_date": issue_str,
                    }.items()
                ),
            ): [],
        }
        fake_client = _make_fake_client(registry)
        monkeypatch.setattr(module, "_read_station_codes", lambda cfg: [code])

        env = {
            "SAPPHIRE_API_ENABLED": "true",
            "ieasyhydroforecast_run_ML_models": "true",
            "ieasyhydroforecast_available_ML_models": "TFT",
        }
        with patch.dict(os.environ, env, clear=False):
            result = module._run_recovery_preview(
                "PENTAD",
                issue,
                issue,
                issue,
                _client=fake_client,
                _virtual_sources={},
            )

        assert result.blocked_reason is None
        hr = result.horizon_results["pentad"]
        assert hr.candidate_count > 0, "expected candidates to be derived"
        # TFT candidate exists → must NOT be in unusable_units
        tft_unusable = [k for k in hr.unusable_units if k[2] == "TFT"]
        assert not tft_unusable, f"TFT unexpectedly in unusable_units: {tft_unusable}"

    def test_NEWP2_invalid_week_bounds_exits_before_client(self, mock_data, mock_skill):
        """ISO week-format POSTPROCESSING_RECOVERY_START_DATE → exit 1 before any client."""
        env = {
            "SAPPHIRE_PREDICTION_MODE": "PENTAD",
            "POSTPROCESSING_RECOVERY_MODE": "preview",
            "POSTPROCESSING_RECOVERY_START_DATE": "2026-W01",
            "POSTPROCESSING_RECOVERY_END_DATE": "2026-01-05",
            "SAPPHIRE_FORECAST_DATE": "2026-01-05",
        }
        with patch.dict(os.environ, env):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)
                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["19999"])
                # Guard: _run_recovery_preview must not be called
                module._run_recovery_preview = MagicMock(
                    side_effect=AssertionError("_run_recovery_preview called with invalid bounds")
                )
                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()
                assert exc_info.value.code == 1
                module._run_recovery_preview.assert_not_called()
                mocks["file_writer"].save_forecast_data.assert_not_called()

    def test_NEWP2_unexpected_preview_error_fails_closed(self, mock_data, mock_skill):
        """Unexpected exception from _run_recovery_preview → exit 1, no ordinary fallthrough."""
        env = {
            "SAPPHIRE_PREDICTION_MODE": "PENTAD",
            "POSTPROCESSING_RECOVERY_MODE": "preview",
            "POSTPROCESSING_RECOVERY_START_DATE": "2026-01-01",
            "POSTPROCESSING_RECOVERY_END_DATE": "2026-01-05",
            "SAPPHIRE_FORECAST_DATE": "2026-01-05",
        }
        with patch.dict(os.environ, env):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)
                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)
                module._read_station_codes = MagicMock(return_value=["19999"])
                module._run_recovery_preview = MagicMock(
                    side_effect=RuntimeError("unexpected internal error")
                )
                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()
                assert exc_info.value.code == 1
                # Ordinary sinks must not be reached
                mocks["file_writer"].save_forecast_data.assert_not_called()
                mocks["data_reader"].read_combined_forecasts.assert_not_called()

    def test_NEWP2_expired_diagnostics_clock(
        self,
        preview_chain_modules,
        monkeypatch,
    ):
        """Expired deadline at unusable-diagnostics loop start → explicit diagnostic, no entries.

        Calls _run_recovery_preview_for_horizon directly with a past-expired deadline
        and all DAY reads returning empty (so the early-exit unusable loop is entered).
        Verifies the loop breaks immediately with a deadline diagnostic rather than
        silently completing or raising.
        """
        import datetime
        import time

        module = preview_chain_modules["module"]

        code = "19999"
        issue = datetime.date(2026, 1, 5)
        issue_str = "2026-01-05"

        registry = {
            (
                "/forecast/",
                frozenset(
                    {
                        "horizon": "day",
                        "model": "TFT",
                        "code": code,
                        "start_date": issue_str,
                        "end_date": issue_str,
                    }.items()
                ),
            ): [],
            (
                "/lr-forecast/",
                frozenset(
                    {
                        "horizon": "pentad",
                        "code": code,
                        "start_date": issue_str,
                        "end_date": issue_str,
                    }.items()
                ),
            ): [],
        }
        fake_client = _make_fake_client(registry)

        past_deadline = time.monotonic() - 1.0

        env = {
            "SAPPHIRE_API_ENABLED": "true",
            "ieasyhydroforecast_run_ML_models": "true",
            "ieasyhydroforecast_available_ML_models": "TFT",
        }
        with patch.dict(os.environ, env, clear=False):
            hr = module._run_recovery_preview_for_horizon(
                module.PENTAD,
                [code],
                issue,
                issue,
                issue,
                past_deadline,
                _client=fake_client,
                _virtual_sources={},
            )

        # The deadline-expiry diagnostic must appear in diagnostics.
        assert any("deadline" in d.lower() for d in hr.diagnostics), (
            f"No deadline diagnostic in: {hr.diagnostics}"
        )
        # Loop broke before producing any unusable entries.
        assert hr.unusable_units == {}, (
            f"unusable_units should be empty when deadline blocks loop: {hr.unusable_units}"
        )


# ===========================================================================
# NEWP2 P2-review regression tests — findings 1, 2, 3
# ===========================================================================


class TestNewP2P2ReviewRegressions:
    """P2-review finding regressions: deadline-in-enumeration, NE dependency propagation."""

    def test_E_deadline_after_reads_sets_blocked_reason(
        self,
        preview_chain_modules,
        monkeypatch,
    ):
        """Deadline expires during enumeration (after all reads complete) → blocked_reason set.

        Contract: reads complete normally (all DAY EMPTY), _iter_issue_dates is the
        enumeration entry point.  When time.monotonic() returns expired only after
        _iter_issue_dates is first called, blocked_reason must be set and entry_has_issues
        must be True (→ CLI would exit 1).  Deadline hitting only during enumeration and
        NOT during reads is what distinguishes this from the existing expired-deadline test.
        """
        import datetime
        import time as _real_time

        module = preview_chain_modules["module"]

        code = "19999"
        issue = datetime.date(2026, 1, 5)
        issue_str = "2026-01-05"

        registry = {
            (
                "/forecast/",
                frozenset(
                    {
                        "horizon": "day",
                        "model": "TFT",
                        "code": code,
                        "start_date": issue_str,
                        "end_date": issue_str,
                    }.items()
                ),
            ): [],
            (
                "/lr-forecast/",
                frozenset(
                    {
                        "horizon": "pentad",
                        "code": code,
                        "start_date": issue_str,
                        "end_date": issue_str,
                    }.items()
                ),
            ): [],
        }
        fake_client = _make_fake_client(registry)
        monkeypatch.setattr(module, "_read_station_codes", lambda cfg: [code])

        # Controlled clock: returns base-1000 during reads (before enumeration),
        # base+1000 once _iter_issue_dates is first called (enumeration phase).
        _in_enumeration = {"value": False}
        original_iter = module._iter_issue_dates

        def _marking_iter(start, end):
            _in_enumeration["value"] = True
            yield from original_iter(start, end)

        monkeypatch.setattr(module, "_iter_issue_dates", _marking_iter)

        _base = _real_time.monotonic()

        def _controlled_clock():
            if _in_enumeration["value"]:
                return _base + 1000  # well past deadline
            return 0  # deadline = 0 + 300 = 300; reads see 0 < 300

        monkeypatch.setattr(module.time, "monotonic", _controlled_clock)

        env = {
            "SAPPHIRE_API_ENABLED": "true",
            "ieasyhydroforecast_run_ML_models": "true",
            "ieasyhydroforecast_available_ML_models": "TFT",
        }
        with patch.dict(os.environ, env, clear=False):
            result = module._run_recovery_preview(
                "PENTAD",
                issue,
                issue,
                issue,
                _client=fake_client,
                _virtual_sources={},
            )

        assert result.blocked_reason is None, result.blocked_reason
        hr = result.horizon_results["pentad"]
        # blocked_reason must be set: deadline expired during enumeration
        assert hr.blocked_reason is not None, (
            "blocked_reason must be set when deadline expires during enumeration; "
            f"diagnostics: {hr.diagnostics}"
        )
        assert "deadline" in hr.blocked_reason.lower(), hr.blocked_reason
        # CLI _has_issues check includes blocked_reason → would exit 1
        _has_issues = (
            hr.blocked_reason
            or hr.blocked_units
            or hr.unusable_units
            or (hr.plan is not None and hr.plan.plan_error is not None)
        )
        assert _has_issues, "entry_has_issues must be True when blocked_reason is set"

    def test_F_sole_model_failed_ne_in_blocked_units_not_unusable(
        self,
        preview_chain_modules,
        monkeypatch,
    ):
        """Sole enabled model DAY read fails → NE in blocked_units, not unusable_units.

        Contract: if (code, TFT) is in blocked_units (source unknown/failed), NE for
        that code must also be in blocked_units.  NE must NOT appear in unusable_units.
        """
        import datetime

        module = preview_chain_modules["module"]

        code = "19999"
        issue = datetime.date(2026, 1, 5)

        class TftFailsClient:
            def _get(self, path, params=None):
                params = params or {}
                if params.get("model") == "TFT" and params.get("horizon") == "day":
                    raise RuntimeError("TFT network error")
                if path == "/lr-forecast/":
                    return []
                raise AssertionError(f"Unexpected _get({path!r}, {params!r})")

        monkeypatch.setattr(module, "_read_station_codes", lambda cfg: [code])

        env = {
            "SAPPHIRE_API_ENABLED": "true",
            "ieasyhydroforecast_run_ML_models": "true",
            "ieasyhydroforecast_available_ML_models": "TFT",
        }
        with patch.dict(os.environ, env, clear=False):
            result = module._run_recovery_preview(
                "PENTAD",
                issue,
                issue,
                issue,
                _client=TftFailsClient(),
                _virtual_sources={},
            )

        assert result.blocked_reason is None, result.blocked_reason
        hr = result.horizon_results["pentad"]
        assert hr.candidate_count == 0, "no candidates expected when sole model fails"

        # TFT source read failed → must be in blocked_units
        tft_blocked = any(m == "TFT" for (_, m) in hr.blocked_units)
        assert tft_blocked, f"TFT must be in blocked_units; got {hr.blocked_units}"

        # NE must be in blocked_units (TFT unknown → NE unknown)
        ne_in_blocked = any(m == "NE" for (_, m) in hr.blocked_units)
        assert ne_in_blocked, (
            f"NE must be in blocked_units when sole source failed; "
            f"blocked_units={hr.blocked_units}, unusable_units keys={list(hr.unusable_units)}"
        )

        # NE must NOT be in unusable_units
        ne_in_unusable = any(k[2] == "NE" for k in hr.unusable_units)
        assert not ne_in_unusable, (
            f"NE must not appear in unusable_units when source is unknown; "
            f"unusable NE entries: {[k for k in hr.unusable_units if k[2] == 'NE']}"
        )

    def test_G_virtual_helper_failure_ne_blocked_direct_twin_preserved(
        self,
        preview_chain_modules,
        monkeypatch,
        tmp_path,
    ):
        """Virtual helper raises SystemExit → NE blocked for virtual code, direct twin survives.

        Contract: when sl.calculate_virtual_stations_data raises SystemExit, virtual_helper_blocked
        is set, NE for the virtual code V is in blocked_units (not unusable_units), and V's
        independent direct TFT candidate (from a direct DAY read for code V) survives in
        candidate_payloads.
        """
        import datetime
        import json as _json

        module = preview_chain_modules["module"]

        code_a = "19991"
        code_v = "19993"  # virtual (contributor A), AND has its own direct TFT DAY rows
        issue = datetime.date(2026, 1, 5)
        issue_str = "2026-01-05"

        vconfig = {
            "virtualStations": {
                code_v: {
                    "combinationFunction": "sum",
                    "weightByStation": {code_a: 1},
                }
            }
        }
        vconfig_file = tmp_path / "virtual_stations.json"
        vconfig_file.write_text(_json.dumps(vconfig))

        rows_a = _make_full_day_fan_pentad(code_a, issue_str, "TFT")
        rows_v_direct = _make_full_day_fan_pentad(code_v, issue_str, "TFT")

        class DirectTwinClient:
            def _get(self, path, params=None):
                params = params or {}
                model = params.get("model", "")
                horizon = params.get("horizon", "")
                req_code = params.get("code", "")
                if horizon == "day" and model == "TFT" and req_code == code_a:
                    skip, limit = params.get("skip", 0), params.get("limit", 500)
                    return rows_a[skip : skip + limit]
                if horizon == "day" and model == "TFT" and req_code == code_v:
                    skip, limit = params.get("skip", 0), params.get("limit", 500)
                    return rows_v_direct[skip : skip + limit]
                if horizon == "pentad" and req_code == code_v and model == "TFT":
                    return []
                if horizon == "pentad" and req_code == code_v and model == "NE":
                    return []
                if path == "/lr-forecast/":
                    return []
                raise AssertionError(f"Unexpected _get({path!r}, {params!r})")

        def _failing_virtual_helper(frame):
            raise SystemExit("invalid virtual instruction (test)")

        monkeypatch.setattr(module.sl, "calculate_virtual_stations_data", _failing_virtual_helper)
        monkeypatch.setattr(module, "_read_station_codes", lambda cfg: [code_v])

        env = {
            "SAPPHIRE_PREDICTION_MODE": "PENTAD",
            "SAPPHIRE_API_ENABLED": "true",
            "ieasyhydroforecast_run_ML_models": "true",
            "ieasyhydroforecast_available_ML_models": "TFT",
            "ieasyforecast_virtual_stations": vconfig_file.name,
            "ieasyforecast_configuration_path": str(tmp_path),
        }
        with patch.dict(os.environ, env, clear=False):
            result = module._run_recovery_preview(
                "PENTAD",
                issue,
                issue,
                issue,
                _client=DirectTwinClient(),
            )

        assert result.blocked_reason is None, result.blocked_reason
        hr = result.horizon_results["pentad"]
        assert hr.plan is not None, "plan must be produced even with virtual helper failure"

        # Virtual helper failure must be recorded in diagnostics
        assert any(
            "virtual" in d.lower() and ("exit" in d.lower() or "fail" in d.lower())
            for d in hr.diagnostics
        ), f"No virtual failure diagnostic: {hr.diagnostics}"

        # NE for V must be in blocked_units (virtual construction failed → NE unresolvable)
        ne_blocked_for_v = any(c == code_v and m == "NE" for (c, m) in hr.blocked_units)
        assert ne_blocked_for_v, (
            f"NE for virtual code V must be blocked when helper fails; "
            f"blocked_units={hr.blocked_units}"
        )

        # NE for V must NOT be in unusable_units
        ne_unusable_for_v = any(k[1] == code_v and k[2] == "NE" for k in hr.unusable_units)
        assert not ne_unusable_for_v, (
            f"NE for V must not be in unusable_units (it is unknown, not confirmed absent); "
            f"unusable_units keys with V: {[k for k in hr.unusable_units if k[1] == code_v]}"
        )

        # Direct TFT twin for V must survive as an independent candidate
        v_tft = [
            p for p in hr.candidate_payloads if p["code"] == code_v and p["model_type"] == "TFT"
        ]
        assert v_tft, (
            f"V's direct TFT candidate must survive virtual helper failure; "
            f"candidate codes/models: {[(p['code'], p['model_type']) for p in hr.candidate_payloads]}"
        )

    def test_H_unreadable_virtual_config_tft_preserved_ne_blocked_tide_not_unusable(
        self,
        preview_chain_modules,
        monkeypatch,
        tmp_path,
    ):
        """Configured virtual file unreadable: TFT candidate preserved, NE blocked, TiDE absent blocked not unusable.

        Contract:
        - virtual config file exists but contains malformed JSON → file_ok=False → virtual_config_unknown=True
        - TFT has a complete DAY fan → TFT candidate survives in candidate_payloads
        - NE is blocked for the output code (virtual_config_unknown triggers NE block) → not in candidates
        - TiDE DAY read is EMPTY → with virtual_config_unknown, absence is 'unknown/blocked', not 'unusable'
        - No sinks called (enforced by fixture teardown)
        """
        import datetime

        module = preview_chain_modules["module"]

        code = "19999"
        issue = datetime.date(2026, 1, 5)
        issue_str = "2026-01-05"

        # Malformed JSON — file_ok=False path in _preflight_virtual_instructions
        bad_config = tmp_path / "virtual_stations_bad.json"
        bad_config.write_text("{ this is not valid json !!!")

        # Patch _get_virtual_config_path so the production code reads this bad file
        monkeypatch.setattr(module, "_get_virtual_config_path", lambda: str(bad_config))

        # Fake client: TFT full fan present, TiDE empty, LR empty, TFT existing empty
        registry = {
            (
                "/forecast/",
                frozenset(
                    {
                        "horizon": "day",
                        "model": "TFT",
                        "code": code,
                        "start_date": issue_str,
                        "end_date": issue_str,
                    }.items()
                ),
            ): _make_full_day_fan_pentad(code, issue_str, "TFT"),
            (
                "/forecast/",
                frozenset(
                    {
                        "horizon": "day",
                        "model": "TiDE",
                        "code": code,
                        "start_date": issue_str,
                        "end_date": issue_str,
                    }.items()
                ),
            ): [],
            (
                "/lr-forecast/",
                frozenset(
                    {
                        "horizon": "pentad",
                        "code": code,
                        "start_date": issue_str,
                        "end_date": issue_str,
                    }.items()
                ),
            ): [],
            # Existing combined reads for the TFT candidate unit
            (
                "/forecast/",
                frozenset(
                    {
                        "horizon": "pentad",
                        "model": "TFT",
                        "code": code,
                        "start_date": issue_str,
                        "end_date": issue_str,
                    }.items()
                ),
            ): [],
        }
        fake_client = _make_fake_client(registry)

        env = {
            "ieasyhydroforecast_run_ML_models": "true",
            "ieasyhydroforecast_available_ML_models": "TFT,TIDE",
        }
        with patch.dict(os.environ, env, clear=False):
            hr = module._run_recovery_preview_for_horizon(
                module.PENTAD,
                [code],
                issue,
                issue,
                issue,
                __import__("time").monotonic() + 60.0,
                _client=fake_client,
            )

        # TFT candidate must be preserved
        tft_candidates = [p for p in hr.candidate_payloads if p["model_type"] == "TFT"]
        assert tft_candidates, (
            f"TFT candidate must be preserved when virtual config is unreadable; "
            f"candidates: {[(p['model_type'], p['code']) for p in hr.candidate_payloads]}"
        )

        # NE must not appear as a candidate
        ne_candidates = [p for p in hr.candidate_payloads if p["model_type"] == "NE"]
        assert not ne_candidates, (
            f"NE must be blocked (not a candidate) when virtual config is unreadable; "
            f"ne_candidates: {ne_candidates}"
        )

        # NE must be in blocked_units (virtual_config_unknown triggers conservative NE block)
        ne_blocked = any(m == "NE" for (_, m) in hr.blocked_units)
        assert ne_blocked, (
            f"NE must appear in blocked_units when virtual config is unreadable; "
            f"blocked_units keys: {list(hr.blocked_units.keys())}"
        )

        # TiDE absent must NOT be in unusable_units (unknown, not confirmed empty)
        tide_unusable = any(k[2] in ("TiDE", "TIDE") for k in hr.unusable_units)
        assert not tide_unusable, (
            f"TiDE absent must not be in unusable_units when virtual config is unreadable; "
            f"unusable_units keys: {list(hr.unusable_units.keys())}"
        )

        # TiDE absent must be in blocked_units (classified as unknown)
        tide_blocked = any(m in ("TiDE", "TIDE") for (_, m) in hr.blocked_units)
        assert tide_blocked, (
            f"TiDE absent must be in blocked_units (unknown) when virtual config is unreadable; "
            f"blocked_units keys: {list(hr.blocked_units.keys())}"
        )


# ---------------------------------------------------------------------------
# P3: select_recovery_records — selection/serialization tests
# ---------------------------------------------------------------------------


@pytest.fixture
def real_recovery_modules():
    """Import real api_writer, gap_detector, data_reader; restores sys.modules on teardown."""
    import importlib

    _script_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if _script_dir not in sys.path:
        sys.path.insert(0, _script_dir)
    _KEYS = ("src.api_writer", "src.gap_detector", "src.data_reader", "tag_library")
    _saved = {k: sys.modules.get(k) for k in _KEYS}
    for k in _KEYS:
        if k in sys.modules and isinstance(sys.modules[k], MagicMock):
            del sys.modules[k]
    aw = importlib.import_module("src.api_writer")
    gd = importlib.import_module("src.gap_detector")
    yield {"aw": aw, "gd": gd}
    for k, v in _saved.items():
        if v is None:
            sys.modules.pop(k, None)
        else:
            sys.modules[k] = v


_PAYLOAD_SENTINEL = object()


def _derive_period_ints(horizon_type, target_date):
    """Compute (horizon_value, horizon_in_year) from target_date and horizon_type."""
    d = target_date
    if horizon_type == "pentad":
        hv = min((d.day - 1) // 5 + 1, 6)
        hiy = (d.month - 1) * 6 + hv
    elif horizon_type == "decad":
        hv = 1 if d.day <= 10 else (2 if d.day <= 20 else 3)
        hiy = (d.month - 1) * 3 + hv
    else:
        hv = 1
        hiy = 1
    return hv, hiy


def _make_plan_payload(
    horizon_type="pentad",
    code="19999",
    model_type="TFT",
    issue_date=None,
    target_date=None,
    fd=100.0,
    q05=80.0,
    q25=90.0,
    q75=110.0,
    q95=120.0,
    flag=None,
    composition=None,
    horizon_value=_PAYLOAD_SENTINEL,
    horizon_in_year=_PAYLOAD_SENTINEL,
):
    """Build a minimal valid plan payload dict (date objects).

    horizon_value and horizon_in_year are derived from target_date and
    horizon_type when left at their sentinel (not explicitly provided).
    Pass any explicit value (including None, True, 1.5) to override.
    """
    import datetime

    if issue_date is None:
        issue_date = datetime.date(2026, 1, 5)
    if target_date is None:
        target_date = issue_date + datetime.timedelta(days=1)
    if horizon_value is _PAYLOAD_SENTINEL or horizon_in_year is _PAYLOAD_SENTINEL:
        hv, hiy = _derive_period_ints(horizon_type, target_date)
        if horizon_value is _PAYLOAD_SENTINEL:
            horizon_value = hv
        if horizon_in_year is _PAYLOAD_SENTINEL:
            horizon_in_year = hiy
    return {
        "horizon_type": horizon_type,
        "code": code,
        "model_type": model_type,
        "date": issue_date,
        "target": target_date,
        "forecasted_discharge": fd,
        "q05": q05,
        "q25": q25,
        "q75": q75,
        "q95": q95,
        "flag": flag,
        "composition": composition,
        "horizon_value": horizon_value,
        "horizon_in_year": horizon_in_year,
    }


class TestNewP3SelectRecoveryRecords:
    """P3: select_recovery_records — selection/serialization, no transport."""

    def test_missing_key_exact_14_fields_wire_aliases(self, real_recovery_modules):
        """plan.missing TFT pentad → serialized record has exactly 14 fields, wire aliases."""
        import datetime

        aw = real_recovery_modules["aw"]
        gd = real_recovery_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        forecast_date = datetime.date(2026, 1, 5)

        payload = _make_plan_payload(issue_date=issue)
        # Canonical key for TFT pentad
        key = gd._plan_key(payload)
        assert key is not None

        plan = gd.plan_period_refresh(
            existing_payloads=[],
            candidate_payloads=[payload],
            authorized_keys={key},
            explicit_refresh_keys=set(),
            start_date=issue,
            end_date=issue,
            forecast_date=forecast_date,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert key in plan.missing

        records = aw.select_recovery_records(
            [payload],
            plan,
            {key},
            start_date=issue,
            end_date=issue,
            forecast_date=forecast_date,
        )
        assert len(records) == 1
        r = records[0]
        # Exactly 14 fields
        assert set(r.keys()) == set(aw._RECOVERY_OUTPUT_FIELDS)
        # Wire aliases
        assert r["horizon_type"] == "pentad"
        assert r["model_type"] == "TFT"
        # ISO date strings
        assert r["date"] == "2026-01-05"
        assert r["target"] == "2026-01-06"
        # Values preserved (no rounding)
        assert r["forecasted_discharge"] == 100.0
        assert r["q05"] == 80.0
        assert r["q25"] == 90.0
        assert r["q75"] == 110.0
        assert r["q95"] == 120.0
        # Explicit None for flag/composition
        assert r["flag"] is None
        assert r["composition"] is None
        assert r["horizon_value"] == 2
        assert r["horizon_in_year"] == 2

    @pytest.mark.parametrize(
        "model_input,wire_model,ht_input,wire_ht",
        [
            ("TIDE", "TiDE", "pentad", "pentad"),
            ("TiDE", "TiDE", "pentad", "pentad"),
            ("TSMIXER", "TSMixer", "decad", "decade"),
            ("TSMixer", "TSMixer", "decad", "decade"),
            ("ENSEMBLE_MEAN", "EM", "pentad", "pentad"),
            ("EM", "EM", "decad", "decade"),
            ("NE", "NE", "pentad", "pentad"),
        ],
    )
    def test_wire_aliases_parametrized(
        self,
        real_recovery_modules,
        model_input,
        wire_model,
        ht_input,
        wire_ht,
    ):
        """Wire aliases applied for all supported model/horizon variants."""
        import datetime

        aw = real_recovery_modules["aw"]
        gd = real_recovery_modules["gd"]

        # Pick a valid boundary date: Jan 5 = pentad boundary, Jan 10 = decad boundary
        issue = datetime.date(2026, 1, 5) if ht_input == "pentad" else datetime.date(2026, 1, 10)
        payload = _make_plan_payload(
            horizon_type=ht_input,
            model_type=model_input,
            issue_date=issue,
        )
        key = gd._plan_key(payload)
        assert key is not None, f"_plan_key returned None for {ht_input}/{model_input}"

        plan = gd.plan_period_refresh(
            existing_payloads=[],
            candidate_payloads=[payload],
            authorized_keys={key},
            explicit_refresh_keys=set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None

        records = aw.select_recovery_records(
            [payload],
            plan,
            {key},
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
        )
        assert len(records) == 1
        assert records[0]["horizon_type"] == wire_ht
        assert records[0]["model_type"] == wire_model

    def test_authorized_refresh_uses_merged_payload_not_candidate(self, real_recovery_modules):
        """authorized_refresh uses the planner-merged payload (preserves existing flag/horizon_ints)."""
        import datetime

        aw = real_recovery_modules["aw"]
        gd = real_recovery_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        existing_payload = _make_plan_payload(issue_date=issue, fd=50.0, flag=3)
        candidate_payload = _make_plan_payload(issue_date=issue, fd=100.0, flag=None)
        key = gd._plan_key(candidate_payload)

        plan = gd.plan_period_refresh(
            existing_payloads=[existing_payload],
            candidate_payloads=[candidate_payload],
            authorized_keys={key},
            explicit_refresh_keys={key},
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is None
        assert any(k == key for k, _ in plan.authorized_refresh), (
            f"expected key in authorized_refresh; plan: missing={plan.missing}, "
            f"identical={plan.identical}, protected={plan.protected}"
        )

        records = aw.select_recovery_records(
            [candidate_payload],
            plan,
            {key},
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
        )
        assert len(records) == 1
        r = records[0]
        # Merged record: ALL5 from candidate, flag preserved from existing
        assert r["forecasted_discharge"] == 100.0
        assert r["flag"] == 3  # preserved from existing

    def test_protected_identical_never_selected(self, real_recovery_modules):
        """Keys in plan.identical are not selected; plan.protected keys are not selected."""
        import datetime

        aw = real_recovery_modules["aw"]
        gd = real_recovery_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        identical_payload = _make_plan_payload(issue_date=issue, model_type="TFT")
        key = gd._plan_key(identical_payload)

        plan = gd.plan_period_refresh(
            existing_payloads=[identical_payload],
            candidate_payloads=[identical_payload],
            authorized_keys={key},
            explicit_refresh_keys=set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is None
        assert key in plan.identical

        # Key is identical, so nothing eligible → []
        records = aw.select_recovery_records(
            [identical_payload],
            plan,
            {key},
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
        )
        assert records == []

    def test_plan_error_raises(self, real_recovery_modules):
        """plan.plan_error causes ValueError regardless of candidate_payloads."""
        import datetime

        aw = real_recovery_modules["aw"]
        gd = real_recovery_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        # Trigger plan_error by passing bad existing_read_status
        plan = gd.plan_period_refresh(
            existing_payloads=[],
            candidate_payloads=[],
            authorized_keys=set(),
            explicit_refresh_keys=set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="UNVERIFIED",
        )
        assert plan.plan_error is not None

        with pytest.raises(ValueError, match="plan_error"):
            aw.select_recovery_records(
                [],
                plan,
                set(),
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
            )

    def test_missing_candidate_raises(self, real_recovery_modules):
        """plan.missing key with no matching candidate raises ValueError."""
        import datetime

        aw = real_recovery_modules["aw"]
        gd = real_recovery_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        payload = _make_plan_payload(issue_date=issue)
        key = gd._plan_key(payload)

        plan = gd.plan_period_refresh(
            existing_payloads=[],
            candidate_payloads=[payload],
            authorized_keys={key},
            explicit_refresh_keys=set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert key in plan.missing

        # Pass empty candidate list — key is missing → ValueError
        with pytest.raises(ValueError, match="no candidate"):
            aw.select_recovery_records(
                [],
                plan,
                {key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
            )

    def test_conflicting_alias_candidates_raise(self, real_recovery_modules):
        """Two candidates for same key with differing forecasted_discharge raise ValueError."""
        import datetime

        aw = real_recovery_modules["aw"]
        gd = real_recovery_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        p1 = _make_plan_payload(issue_date=issue, fd=100.0)
        p2 = _make_plan_payload(issue_date=issue, fd=200.0)
        key = gd._plan_key(p1)

        plan = gd.plan_period_refresh(
            existing_payloads=[],
            candidate_payloads=[p1],
            authorized_keys={key},
            explicit_refresh_keys=set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert key in plan.missing

        with pytest.raises(ValueError, match="conflicting duplicate"):
            aw.select_recovery_records(
                [p1, p2],
                plan,
                {key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
            )

    def test_exact_duplicate_candidates_collapse(self, real_recovery_modules):
        """Two identical candidates for same key collapse to one output record."""
        import datetime

        aw = real_recovery_modules["aw"]
        gd = real_recovery_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        payload = _make_plan_payload(issue_date=issue)
        key = gd._plan_key(payload)

        plan = gd.plan_period_refresh(
            existing_payloads=[],
            candidate_payloads=[payload],
            authorized_keys={key},
            explicit_refresh_keys=set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        records = aw.select_recovery_records(
            [payload, payload],
            plan,
            {key},
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
        )
        assert len(records) == 1

    def test_zero_eligible_returns_empty_no_side_effect(self, real_recovery_modules):
        """No missing, no authorized_refresh → returns []."""
        import datetime

        aw = real_recovery_modules["aw"]
        gd = real_recovery_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        plan = gd.plan_period_refresh(
            existing_payloads=[],
            candidate_payloads=[],
            authorized_keys=set(),
            explicit_refresh_keys=set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        records = aw.select_recovery_records(
            [],
            plan,
            set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
        )
        assert records == []

    @pytest.mark.parametrize("bad_model", ["LR", "GBT", "LR_Base", "DAY", "month", "NAIVE MEAN"])
    def test_unsupported_model_raises(self, real_recovery_modules, bad_model):
        """LR, LT, DAY model types in candidate raise ValueError during serialization."""
        import datetime

        aw = real_recovery_modules["aw"]

        issue = datetime.date(2026, 1, 5)
        # Build a payload with a bad model_type that bypasses _plan_key (plan is bypassed)
        # We directly test _recovery_wire_serialize for these unsupported types.
        payload = _make_plan_payload(issue_date=issue, model_type="TFT")
        payload_bad = dict(payload)
        payload_bad["model_type"] = bad_model

        with pytest.raises(ValueError, match="unsupported model_type"):
            aw._recovery_wire_serialize(payload_bad)

    @pytest.mark.parametrize("bad_ht", ["day", "month", "quarter", "season", "PENTAD"])
    def test_unsupported_horizon_raises(self, real_recovery_modules, bad_ht):
        """Non-short-term horizon types raise ValueError during serialization."""
        import datetime

        aw = real_recovery_modules["aw"]

        issue = datetime.date(2026, 1, 5)
        payload = _make_plan_payload(issue_date=issue)
        payload_bad = dict(payload)
        payload_bad["horizon_type"] = bad_ht

        with pytest.raises(ValueError, match="horizon_type"):
            aw._recovery_wire_serialize(payload_bad)

    def test_malformed_flag_raises(self, real_recovery_modules):
        """Non-int flag (e.g. str/bool) raises ValueError."""
        import datetime

        aw = real_recovery_modules["aw"]

        issue = datetime.date(2026, 1, 5)
        payload = _make_plan_payload(issue_date=issue, flag="bad")
        with pytest.raises(ValueError, match="flag"):
            aw._recovery_wire_serialize(payload)

    def test_bool_flag_raises(self, real_recovery_modules):
        """Bool flag raises ValueError (bool is subclass of int, must be rejected)."""
        import datetime

        aw = real_recovery_modules["aw"]

        issue = datetime.date(2026, 1, 5)
        payload = _make_plan_payload(issue_date=issue, flag=True)
        with pytest.raises(ValueError, match="flag"):
            aw._recovery_wire_serialize(payload)

    def test_nonfinite_quantity_raises(self, real_recovery_modules):
        """NaN/Inf in ALL5 fields raises ValueError."""
        import datetime

        aw = real_recovery_modules["aw"]

        issue = datetime.date(2026, 1, 5)
        for bad_val in (float("nan"), float("inf"), float("-inf")):
            payload = _make_plan_payload(issue_date=issue, fd=bad_val)
            with pytest.raises(ValueError, match="non-finite|ALL5"):
                aw._recovery_wire_serialize(payload)

    def test_bool_quantity_raises(self, real_recovery_modules):
        """Bool in ALL5 field raises ValueError."""
        import datetime

        aw = real_recovery_modules["aw"]

        issue = datetime.date(2026, 1, 5)
        payload = _make_plan_payload(issue_date=issue, fd=True)
        with pytest.raises(ValueError, match="ALL5|bool"):
            aw._recovery_wire_serialize(payload)

    def test_none_quantity_raises(self, real_recovery_modules):
        """None in ALL5 field raises ValueError."""
        import datetime

        aw = real_recovery_modules["aw"]

        issue = datetime.date(2026, 1, 5)
        payload = _make_plan_payload(issue_date=issue)
        payload["q05"] = None
        with pytest.raises(ValueError, match="ALL5|None"):
            aw._recovery_wire_serialize(payload)

    def test_malformed_date_raises(self, real_recovery_modules):
        """Non-date type in date field raises ValueError (int is not a date or ISO string)."""
        import datetime

        aw = real_recovery_modules["aw"]

        issue = datetime.date(2026, 1, 5)
        payload = _make_plan_payload(issue_date=issue)
        payload["date"] = 20260105  # integer — not a date, datetime, Timestamp, or ISO string
        with pytest.raises(ValueError, match="unparseable"):
            aw._recovery_wire_serialize(payload)

    def test_target_wrong_raises(self, real_recovery_modules):
        """target != issue + 1 raises ValueError."""
        import datetime

        aw = real_recovery_modules["aw"]

        issue = datetime.date(2026, 1, 5)
        payload = _make_plan_payload(issue_date=issue)
        payload["target"] = datetime.date(2026, 1, 10)  # wrong target
        with pytest.raises(ValueError, match="target"):
            aw._recovery_wire_serialize(payload)

    def test_unknown_extra_field_raises(self, real_recovery_modules):
        """Payload with extra unknown writable field raises ValueError."""
        import datetime

        aw = real_recovery_modules["aw"]

        issue = datetime.date(2026, 1, 5)
        payload = _make_plan_payload(issue_date=issue)
        payload["extra_undocumented_field"] = "should_fail"
        with pytest.raises(ValueError, match="unknown extra field"):
            aw._recovery_wire_serialize(payload)

    def test_known_response_only_fields_ignored(self, real_recovery_modules):
        """id and model_type_description are silently dropped (response-only)."""
        import datetime

        aw = real_recovery_modules["aw"]

        issue = datetime.date(2026, 1, 5)
        payload = _make_plan_payload(issue_date=issue)
        payload["id"] = 42
        payload["model_type_description"] = "Temporal Fusion Transformer (TFT)"
        result = aw._recovery_wire_serialize(payload)
        assert "id" not in result
        assert "model_type_description" not in result
        assert set(result.keys()) == set(aw._RECOVERY_OUTPUT_FIELDS)

    def test_decad_year_boundary_dec31_to_jan1(self, real_recovery_modules):
        """Decad issue Dec-31, target Jan-1 is valid; horizon_type wire = 'decade'."""
        import datetime

        aw = real_recovery_modules["aw"]
        gd = real_recovery_modules["gd"]

        # Dec 31 is a valid decad boundary (last day of month = boundary)
        issue = datetime.date(2025, 12, 31)
        target = datetime.date(2026, 1, 1)
        payload = _make_plan_payload(
            horizon_type="decad",
            model_type="TFT",
            issue_date=issue,
            target_date=target,
            horizon_value=1,
            horizon_in_year=1,
        )
        key = gd._plan_key(payload)
        assert key is not None

        plan = gd.plan_period_refresh(
            existing_payloads=[],
            candidate_payloads=[payload],
            authorized_keys={key},
            explicit_refresh_keys=set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None

        records = aw.select_recovery_records(
            [payload],
            plan,
            {key},
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
        )
        assert len(records) == 1
        assert records[0]["horizon_type"] == "decade"
        assert records[0]["date"] == "2025-12-31"
        assert records[0]["target"] == "2026-01-01"

    def test_pentad_year_boundary_dec30_to_dec31(self, real_recovery_modules):
        """Pentad issue Dec-30 (last day of month for non-31-day month equiv), target Dec-31."""
        import datetime

        aw = real_recovery_modules["aw"]
        gd = real_recovery_modules["gd"]

        # Dec 30 is a valid pentad boundary (day 30 of a month with 31 days = last pentad start)
        # Actually Dec 25 is pentad boundary; Dec 31 is the last day (boundary). Let's use Dec 25.
        issue = datetime.date(2025, 12, 25)
        target = datetime.date(2025, 12, 26)
        payload = _make_plan_payload(
            horizon_type="pentad",
            model_type="TFT",
            issue_date=issue,
            target_date=target,
            horizon_value=6,
            horizon_in_year=72,
        )
        key = gd._plan_key(payload)
        assert key is not None

        plan = gd.plan_period_refresh(
            existing_payloads=[],
            candidate_payloads=[payload],
            authorized_keys={key},
            explicit_refresh_keys=set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None

        records = aw.select_recovery_records(
            [payload],
            plan,
            {key},
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
        )
        assert len(records) == 1
        assert records[0]["date"] == "2025-12-25"
        assert records[0]["target"] == "2025-12-26"

    def test_high_precision_preserved(self, real_recovery_modules):
        """High-precision float values are not rounded."""
        import datetime

        aw = real_recovery_modules["aw"]
        gd = real_recovery_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        fd_precise = 123.456789012345
        payload = _make_plan_payload(issue_date=issue, fd=fd_precise, q05=10.123456789)
        key = gd._plan_key(payload)

        plan = gd.plan_period_refresh(
            existing_payloads=[],
            candidate_payloads=[payload],
            authorized_keys={key},
            explicit_refresh_keys=set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        records = aw.select_recovery_records(
            [payload],
            plan,
            {key},
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
        )
        assert records[0]["forecasted_discharge"] == fd_precise
        assert records[0]["q05"] == 10.123456789

    def test_composition_and_flag_preserved(self, real_recovery_modules):
        """Non-None composition and flag values are preserved exactly in output."""
        import datetime

        aw = real_recovery_modules["aw"]
        gd = real_recovery_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        payload = _make_plan_payload(
            issue_date=issue,
            model_type="ENSEMBLE_MEAN",
            flag=2,
            composition="TFT,TiDE",
        )
        key = gd._plan_key(payload)

        plan = gd.plan_period_refresh(
            existing_payloads=[],
            candidate_payloads=[payload],
            authorized_keys={key},
            explicit_refresh_keys=set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        records = aw.select_recovery_records(
            [payload],
            plan,
            {key},
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
        )
        assert len(records) == 1
        assert records[0]["model_type"] == "EM"
        assert records[0]["flag"] == 2
        assert records[0]["composition"] == "TFT,TiDE"

    def test_candidate_unknown_extra_field_raises_even_if_duplicate(self, real_recovery_modules):
        """Candidate with unknown extra field raises ValueError, not silent collapse (both orders)."""
        import datetime

        aw = real_recovery_modules["aw"]
        gd = real_recovery_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        valid = _make_plan_payload(issue_date=issue)
        bad = dict(valid)
        bad["unknown_extra"] = "surprise"
        key = gd._plan_key(valid)

        plan = gd.plan_period_refresh(
            existing_payloads=[],
            candidate_payloads=[valid],
            authorized_keys={key},
            explicit_refresh_keys=set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None

        # Order 1: valid first, bad second (bad is a duplicate of valid)
        with pytest.raises(ValueError, match="unknown extra field"):
            aw.select_recovery_records(
                [valid, bad],
                plan,
                {key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
            )

        # Order 2: bad first, valid second
        with pytest.raises(ValueError, match="unknown extra field"):
            aw.select_recovery_records(
                [bad, valid],
                plan,
                {key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
            )

    def test_candidate_missing_composition_field_raises_even_if_duplicate(
        self, real_recovery_modules
    ):
        """Candidate missing required 'composition' field raises ValueError, both orders."""
        import datetime

        aw = real_recovery_modules["aw"]
        gd = real_recovery_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        valid = _make_plan_payload(issue_date=issue, composition=None)
        bad = {k: v for k, v in valid.items() if k != "composition"}
        key = gd._plan_key(valid)

        plan = gd.plan_period_refresh(
            existing_payloads=[],
            candidate_payloads=[valid],
            authorized_keys={key},
            explicit_refresh_keys=set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None

        # Order 1: valid first, bad (missing composition) second
        with pytest.raises(ValueError):
            aw.select_recovery_records(
                [valid, bad],
                plan,
                {key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
            )

        # Order 2: bad first, valid second
        with pytest.raises(ValueError):
            aw.select_recovery_records(
                [bad, valid],
                plan,
                {key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
            )


# ---------------------------------------------------------------------------
# P3 transport: submit_recovery_batch — fake HTTP adapter tests
# ---------------------------------------------------------------------------

try:
    from sapphire_api_client.postprocessing import SapphirePostprocessingClient as _SPC

    _SAPPHIRE_CLIENT_AVAILABLE = True
except ImportError:
    _SPC = None
    _SAPPHIRE_CLIENT_AVAILABLE = False

_skip_no_client = pytest.mark.skipif(
    not _SAPPHIRE_CLIENT_AVAILABLE,
    reason="sapphire-api-client not installed",
)


class _FakeStore:
    """In-memory upsert store keyed by 5-part identity. Thread-unsafe; for tests only."""

    def __init__(self):
        self._records: dict[tuple, dict] = {}
        self.post_calls: list[dict] = []  # each call's JSON body

    def upsert(self, record: dict) -> dict:
        key = (
            record.get("horizon_type"),
            record.get("code"),
            record.get("model_type"),
            record.get("date"),
            record.get("target"),
        )
        self._records[key] = dict(record)
        r = dict(record)
        r["id"] = abs(hash(str(key))) % 100000
        r["model_type_description"] = record.get("model_type", "")
        return r

    def get_records(self) -> list[dict]:
        return list(self._records.values())


def _make_fake_response(status_code: int, json_body) -> "requests.Response":
    import json as _json

    import requests as _req

    resp = _req.Response()
    resp.status_code = status_code
    resp._content = _json.dumps(json_body).encode("utf-8")
    resp.headers["Content-Type"] = "application/json"
    return resp


def _make_fake_adapter(store: _FakeStore, raise_on_post=None, commit_then_raise=None):
    """
    Return a requests.adapters.BaseAdapter that intercepts POST /forecast/
    and records calls in store.post_calls.
    raise_on_post: if not None, raise this exception on POST.
    commit_then_raise: if not None, commit then raise this exception.
    """
    import requests.adapters

    class _FakeAdapter(requests.adapters.BaseAdapter):
        def send(self, prepared_request, **kwargs):
            import json as _json

            if prepared_request.method == "POST":
                if raise_on_post is not None:
                    raise raise_on_post
                body = _json.loads(prepared_request.body)
                store.post_calls.append(body)
                data = body.get("data", [])
                if commit_then_raise is not None:
                    for rec in data:
                        store.upsert(rec)
                    raise commit_then_raise
                result = [store.upsert(rec) for rec in data]
                return _make_fake_response(200, result)
            # GET: not needed here
            return _make_fake_response(200, [])

        def close(self):
            pass

    return _FakeAdapter()


@pytest.fixture
def real_transport_modules():
    """Import real api_writer; restore on teardown. Gate: sapphire-api-client installed."""
    if not _SAPPHIRE_CLIENT_AVAILABLE:
        pytest.skip("sapphire-api-client not installed")
    import importlib

    _script_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if _script_dir not in sys.path:
        sys.path.insert(0, _script_dir)
    _saved = {}
    for k in list(sys.modules):
        if "api_writer" in k or "gap_detector" in k or "data_reader" in k:
            _saved[k] = sys.modules[k]
    for k in list(_saved):
        if k in sys.modules and isinstance(sys.modules[k], MagicMock):
            del sys.modules[k]
    aw = importlib.import_module("src.api_writer")
    gd = importlib.import_module("src.gap_detector")
    yield {"aw": aw, "gd": gd}
    for k, v in _saved.items():
        if v is None:
            sys.modules.pop(k, None)
        else:
            sys.modules[k] = v


def _make_valid_wire_record(issue_date=None, model_type="TFT", horizon_type="pentad"):
    """Return a valid 14-field wire record (already serialized, date strings)."""
    import datetime

    if issue_date is None:
        issue_date = datetime.date(2026, 1, 5)
    target_date = issue_date + datetime.timedelta(days=1)
    hv, hiy = _derive_period_ints(horizon_type, target_date)
    wire_ht = "pentad" if horizon_type == "pentad" else "decade"
    wire_model = {
        "TFT": "TFT",
        "TIDE": "TiDE",
        "TSMIXER": "TSMixer",
        "NE": "NE",
        "ENSEMBLE_MEAN": "EM",
        "EM": "EM",
    }.get(model_type.upper(), model_type)
    return {
        "horizon_type": wire_ht,
        "code": "19999",
        "model_type": wire_model,
        "date": issue_date.isoformat(),
        "target": target_date.isoformat(),
        "forecasted_discharge": 100.0,
        "q05": 80.0,
        "q25": 90.0,
        "q75": 110.0,
        "q95": 120.0,
        "flag": None,
        "composition": None,
        "horizon_value": hv,
        "horizon_in_year": hiy,
    }


class TestNewP3SubmitRecoveryBatch:
    """P3: submit_recovery_batch — fake HTTP transport tests."""

    def _make_timing(self, _clock_val=0.0, budget=60.0, reserve=10.0):
        """Return (write_cutoff, deadline) with sufficient time for a timeout=5 request."""
        write_cutoff = _clock_val + budget
        deadline = write_cutoff + reserve
        return write_cutoff, deadline

    def _make_evidence_path(self, tmp_path):
        return str(tmp_path / "evidence_attempt.json")

    @_skip_no_client
    def test_success_one_post_14_fields_evidence_before_send(
        self, real_transport_modules, tmp_path
    ):
        """Success: one POST, 14 fields, evidence file exists before adapter.send."""
        import datetime
        import json

        import requests

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)
        assert key is not None

        store = _FakeStore()
        evidence_path = self._make_evidence_path(tmp_path)
        write_cutoff, deadline = self._make_timing()

        # Intercept to assert evidence exists before send
        evidence_exists_before_send = []

        class _EvidenceCheckAdapter(requests.adapters.BaseAdapter):
            def send(self, prepared_request, **kwargs):
                import os

                evidence_exists_before_send.append(os.path.exists(evidence_path))
                body = json.loads(prepared_request.body)
                store.post_calls.append(body)
                data = body.get("data", [])
                result = [store.upsert(rec) for rec in data]
                return _make_fake_response(200, result)

            def close(self):
                pass

        original_init = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            original_init(self_client, **kwargs)
            self_client.session.mount("http://", _EvidenceCheckAdapter())
            self_client.session.mount("https://", _EvidenceCheckAdapter())

        import unittest.mock

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            result = aw.submit_recovery_batch(
                [record],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                batch_limit=100,
                write_cutoff=write_cutoff,
                deadline=deadline,
                evidence_path=evidence_path,
                timeout=5,
                _clock=lambda: 0.0,
            )

        assert result["attempted"] is True
        assert result["transport_status"] == "acknowledged"
        assert result["acknowledged_count"] == 1
        assert result["evidence_path"] == evidence_path
        assert result["error"] is None

        # Evidence existed before adapter.send
        assert evidence_exists_before_send == [True], "evidence must exist before POST is sent"

        # Evidence file exists and contains payload
        import os

        assert os.path.exists(evidence_path)
        with open(evidence_path) as f:
            ev = json.load(f)
        assert ev["n_records"] == 1
        assert "attempt_id" in ev
        # No auth token in evidence
        assert "auth_token" not in str(ev)
        assert "Authorization" not in str(ev)

        # Exactly one POST
        assert len(store.post_calls) == 1
        post_body = store.post_calls[0]
        assert "data" in post_body
        assert len(post_body["data"]) == 1
        sent = post_body["data"][0]
        assert set(sent.keys()) == set(aw._RECOVERY_OUTPUT_FIELDS)
        assert sent["horizon_type"] == "pentad"
        assert sent["model_type"] == "TFT"
        assert sent["date"] == "2026-01-05"
        assert sent["target"] == "2026-01-06"
        assert sent["forecasted_discharge"] == 100.0
        assert sent["flag"] is None
        assert sent["composition"] is None

    @_skip_no_client
    def test_ordinary_singleton_unchanged(self, real_transport_modules, tmp_path):
        """submit_recovery_batch must not modify the ordinary singleton client."""
        import datetime

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        # Snapshot ordinary singleton state before (force lazy init)
        before_client = aw._postprocessing_client

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)
        store = _FakeStore()

        _orig_init_singleton = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            _orig_init_singleton(self_client, **kwargs)
            self_client.session.mount("http://", _make_fake_adapter(store))

        import unittest.mock

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            aw.submit_recovery_batch(
                [record],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                batch_limit=100,
                write_cutoff=1000.0,
                deadline=1060.0,
                evidence_path=self._make_evidence_path(tmp_path),
                timeout=5,
                _clock=lambda: 0.0,
            )

        # Singleton must not have changed
        assert aw._postprocessing_client is before_client

    @_skip_no_client
    def test_commit_then_timeout_uncertain_no_resend(self, real_transport_modules, tmp_path):
        """Adapter commits then raises Timeout → uncertain, one POST, no resend."""
        import datetime

        import requests

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)
        store = _FakeStore()

        _orig_init_timeout = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            _orig_init_timeout(self_client, **kwargs)
            adapter = _make_fake_adapter(
                store, commit_then_raise=requests.Timeout("simulated timeout")
            )
            self_client.session.mount("http://", adapter)

        import unittest.mock

        evidence_path = self._make_evidence_path(tmp_path)
        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            result = aw.submit_recovery_batch(
                [record],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                batch_limit=100,
                write_cutoff=1000.0,
                deadline=1060.0,
                evidence_path=evidence_path,
                timeout=5,
                _clock=lambda: 0.0,
            )

        assert result["attempted"] is True
        assert result["transport_status"] == "uncertain"
        assert result["acknowledged_count"] is None
        # Evidence still present (no overwrite)
        import os

        assert os.path.exists(evidence_path)
        # Exactly one POST attempt (max_retries=1)
        assert len(store.post_calls) == 1
        # Record was committed to store despite timeout
        assert len(store.get_records()) == 1

    @_skip_no_client
    def test_short_count_response_uncertain(self, real_transport_modules, tmp_path):
        """Response list shorter than submitted count → uncertain."""
        import datetime

        import requests

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        r1 = _make_valid_wire_record(issue_date=issue, model_type="TFT")
        r2 = _make_valid_wire_record(
            issue_date=issue,
            model_type="TIDE",
        )
        # Give r2 a different code to avoid duplicate key
        r2 = dict(r2)
        r2["code"] = "19998"

        k1 = gd._plan_key(r1)
        k2 = gd._plan_key(r2)

        class _ShortCountAdapter(requests.adapters.BaseAdapter):
            def send(self, prepared_request, **kwargs):
                import json

                json.loads(prepared_request.body)
                # Return only one record even if two were submitted
                return _make_fake_response(200, [{"id": 1, "model_type_description": "x"}])

            def close(self):
                pass

        _orig_init_short = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            _orig_init_short(self_client, **kwargs)
            self_client.session.mount("http://", _ShortCountAdapter())

        import unittest.mock

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            result = aw.submit_recovery_batch(
                [r1, r2],
                authorized_keys={k1, k2},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                batch_limit=100,
                write_cutoff=1000.0,
                deadline=1060.0,
                evidence_path=self._make_evidence_path(tmp_path),
                timeout=5,
                _clock=lambda: 0.0,
            )

        assert result["transport_status"] == "uncertain"
        assert result["attempted"] is True

    @_skip_no_client
    def test_malformed_response_body_uncertain(self, real_transport_modules, tmp_path):
        """Response body is not a list → uncertain."""
        import datetime

        import requests

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)

        class _BadBodyAdapter(requests.adapters.BaseAdapter):
            def send(self, prepared_request, **kwargs):
                return _make_fake_response(200, {"not": "a list"})

            def close(self):
                pass

        _orig_init_bad = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            _orig_init_bad(self_client, **kwargs)
            self_client.session.mount("http://", _BadBodyAdapter())

        import unittest.mock

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            result = aw.submit_recovery_batch(
                [record],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                batch_limit=100,
                write_cutoff=1000.0,
                deadline=1060.0,
                evidence_path=self._make_evidence_path(tmp_path),
                timeout=5,
                _clock=lambda: 0.0,
            )

        assert result["transport_status"] == "uncertain"

    @_skip_no_client
    def test_empty_batch_no_post(self, real_transport_modules, tmp_path):
        """Empty records list → not_attempted, zero client/network calls."""
        import datetime

        aw = real_transport_modules["aw"]
        store = _FakeStore()

        result = aw.submit_recovery_batch(
            [],
            authorized_keys=set(),
            start_date=datetime.date(2026, 1, 1),
            end_date=datetime.date(2026, 1, 31),
            forecast_date=datetime.date(2026, 1, 31),
            base_url="http://fake-sapphire.local",
            batch_limit=100,
            write_cutoff=1000.0,
            deadline=1060.0,
            evidence_path=self._make_evidence_path(tmp_path),
            timeout=5,
        )

        assert result["attempted"] is False
        assert result["transport_status"] == "not_attempted"
        assert result["acknowledged_count"] == 0
        assert len(store.post_calls) == 0

    @_skip_no_client
    def test_oversize_batch_rejected(self, real_transport_modules, tmp_path):
        """Batch larger than batch_limit → not_attempted, no POST."""
        import datetime

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)

        result = aw.submit_recovery_batch(
            [record],
            authorized_keys={key},
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            base_url="http://fake-sapphire.local",
            batch_limit=0,  # batch_limit=0 is invalid → not_attempted
            write_cutoff=1000.0,
            deadline=1060.0,
            evidence_path=self._make_evidence_path(tmp_path),
            timeout=5,
        )
        assert result["attempted"] is False
        assert result["transport_status"] == "not_attempted"

        # batch_limit=1 but 2 records
        import datetime

        r2 = dict(_make_valid_wire_record(issue_date=issue, model_type="TIDE"))
        r2["code"] = "19998"
        k2 = gd._plan_key(r2)
        result2 = aw.submit_recovery_batch(
            [record, r2],
            authorized_keys={key, k2},
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            base_url="http://fake-sapphire.local",
            batch_limit=1,
            write_cutoff=1000.0,
            deadline=1060.0,
            evidence_path=self._make_evidence_path(tmp_path) + "_b",
            timeout=5,
        )
        assert result2["attempted"] is False
        assert "exceeds batch_limit" in (result2["error"] or "")

    @_skip_no_client
    def test_at_cutoff_no_post(self, real_transport_modules, tmp_path):
        """Clock at or past write_cutoff → not_attempted."""
        import datetime

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)

        # Inject a clock that returns exactly at write_cutoff
        cutoff = 100.0
        result = aw.submit_recovery_batch(
            [record],
            authorized_keys={key},
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            base_url="http://fake-sapphire.local",
            batch_limit=100,
            write_cutoff=cutoff,
            deadline=cutoff + 60.0,
            evidence_path=self._make_evidence_path(tmp_path),
            timeout=5,
            _clock=lambda: cutoff,  # exactly at cutoff
        )
        assert result["attempted"] is False
        assert result["transport_status"] == "not_attempted"

    @_skip_no_client
    def test_existing_evidence_path_no_post(self, real_transport_modules, tmp_path):
        """Existing evidence_path → not_attempted (no resend)."""
        import datetime

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)

        evidence_path = self._make_evidence_path(tmp_path)
        # Pre-create the evidence file
        with open(evidence_path, "w") as f:
            f.write("{}")

        result = aw.submit_recovery_batch(
            [record],
            authorized_keys={key},
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            base_url="http://fake-sapphire.local",
            batch_limit=100,
            write_cutoff=1000.0,
            deadline=1060.0,
            evidence_path=evidence_path,
            timeout=5,
            _clock=lambda: 0.0,
        )

        assert result["attempted"] is False
        assert result["transport_status"] == "not_attempted"
        assert "already exists" in (result["error"] or "")

    @_skip_no_client
    def test_redirect_no_second_network_request(self, real_transport_modules, tmp_path):
        """max_redirects=0 on isolated client: TooManyRedirects treated as uncertain, not resent."""
        import datetime

        import requests

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)

        post_call_count = []

        class _RedirectAdapter(requests.adapters.BaseAdapter):
            def send(self, prepared_request, **kwargs):
                # Return a 302 redirect; requests with max_redirects=0 raises TooManyRedirects
                post_call_count.append(1)
                resp = _make_fake_response(302, [])
                resp.headers["Location"] = "http://fake-sapphire.local/other/"
                return resp

            def close(self):
                pass

        _orig_init_redir = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            _orig_init_redir(self_client, **kwargs)
            self_client.session.mount("http://", _RedirectAdapter())

        import unittest.mock

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            result = aw.submit_recovery_batch(
                [record],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                batch_limit=100,
                write_cutoff=1000.0,
                deadline=1060.0,
                evidence_path=self._make_evidence_path(tmp_path),
                timeout=5,
                _clock=lambda: 0.0,
            )

        # The isolated client has max_redirects=0, so one 302 → TooManyRedirects
        # This is treated as uncertain (transport failure), not a second request
        assert result["attempted"] is True
        assert result["transport_status"] == "uncertain"
        # Only one network attempt (no follow-up redirect request)
        assert len(post_call_count) == 1

    @_skip_no_client
    def test_late_post_beyond_deadline_uncertain(self, real_transport_modules, tmp_path):
        """POST completes after deadline → uncertain (late completion ≠ success)."""
        import datetime

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)

        store = _FakeStore()
        # [budget-check, after-evidence, pre-POST-recheck, post-POST] — post-POST past deadline
        clock_values = iter([0.0, 0.0, 0.0, 200.0])

        def _advancing_clock():
            try:
                return next(clock_values)
            except StopIteration:
                return 200.0

        _orig_init_late = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            _orig_init_late(self_client, **kwargs)
            self_client.session.mount("http://", _make_fake_adapter(store))

        import unittest.mock

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            result = aw.submit_recovery_batch(
                [record],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                batch_limit=100,
                write_cutoff=50.0,
                deadline=100.0,
                evidence_path=self._make_evidence_path(tmp_path),
                timeout=5,
                _clock=_advancing_clock,
            )

        # POST happened (clock 0.0 < cutoff 50.0), but clock after call was 200.0 > deadline 100.0
        assert result["attempted"] is True
        assert result["transport_status"] == "uncertain"


# ---------------------------------------------------------------------------
# P3 guards: new omission tests (no real API calls; real client import gated)
# ---------------------------------------------------------------------------


class TestNewP3Guards:
    """P3 guards for submit_recovery_batch: URL, evidence parent, write loop, types."""

    def _make_timing(self):
        return 1000.0, 1060.0  # write_cutoff, deadline

    def _base_args(self, tmp_path):
        import datetime

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        write_cutoff, deadline = self._make_timing()
        return dict(
            records=[record],
            authorized_keys=set(),  # not reached in rejected-early tests
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            base_url="http://fake-sapphire.local",
            batch_limit=100,
            write_cutoff=write_cutoff,
            deadline=deadline,
            evidence_path=str(tmp_path / "ev.json"),
            timeout=5,
            _clock=lambda: 0.0,
        )

    @_skip_no_client
    def test_url_with_credentials_rejected_no_effects(self, real_transport_modules, tmp_path):
        """base_url containing credentials → not_attempted, zero constructor/POST/evidence."""
        import datetime
        import os

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)
        evidence_path = str(tmp_path / "ev_cred.json")
        write_cutoff, deadline = self._make_timing()

        constructor_called = []
        original_init = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            constructor_called.append(1)
            original_init(self_client, **kwargs)

        import unittest.mock

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            result = aw.submit_recovery_batch(
                [record],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://user:pass@fake-sapphire.local",
                batch_limit=100,
                write_cutoff=write_cutoff,
                deadline=deadline,
                evidence_path=evidence_path,
                timeout=5,
                _clock=lambda: 0.0,
            )

        assert result["attempted"] is False
        assert result["transport_status"] == "not_attempted"
        assert "credential" in (result["error"] or "").lower()
        assert len(constructor_called) == 0, "constructor must not be called for invalid URL"
        assert not os.path.exists(evidence_path), "evidence must not be created for invalid URL"

    @_skip_no_client
    def test_url_with_query_rejected_no_effects(self, real_transport_modules, tmp_path):
        """base_url with query string → not_attempted, zero constructor/POST/evidence."""
        import datetime
        import os

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)
        evidence_path = str(tmp_path / "ev_query.json")
        write_cutoff, deadline = self._make_timing()

        constructor_called = []
        original_init = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            constructor_called.append(1)
            original_init(self_client, **kwargs)

        import unittest.mock

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            result = aw.submit_recovery_batch(
                [record],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local?token=secret",
                batch_limit=100,
                write_cutoff=write_cutoff,
                deadline=deadline,
                evidence_path=evidence_path,
                timeout=5,
                _clock=lambda: 0.0,
            )

        assert result["attempted"] is False
        assert result["transport_status"] == "not_attempted"
        assert len(constructor_called) == 0, "constructor must not be called for URL with query"
        assert not os.path.exists(evidence_path)

    @_skip_no_client
    def test_evidence_parent_group_writable_rejected(self, real_transport_modules, tmp_path):
        """Evidence parent dir with group write bit → not_attempted, no POST."""
        import datetime
        import os

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)

        # Create a subdirectory with group-writable permissions (0755 has group rx but not w)
        # Use 0o775 which has group-write bit set
        ev_dir = tmp_path / "ev_dir_755"
        ev_dir.mkdir(mode=0o755)
        # chmod to add group-write
        os.chmod(ev_dir, 0o775)
        evidence_path = str(ev_dir / "evidence.json")
        write_cutoff, deadline = self._make_timing()

        constructor_called = []
        original_init = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            constructor_called.append(1)
            original_init(self_client, **kwargs)

        import unittest.mock

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            result = aw.submit_recovery_batch(
                [record],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                batch_limit=100,
                write_cutoff=write_cutoff,
                deadline=deadline,
                evidence_path=evidence_path,
                timeout=5,
                _clock=lambda: 0.0,
            )

        assert result["attempted"] is False
        assert result["transport_status"] == "not_attempted"
        assert (
            "permission" in (result["error"] or "").lower()
            or "group" in (result["error"] or "").lower()
        )
        assert len(constructor_called) == 0, (
            "constructor must not be called when parent perms invalid"
        )
        assert not os.path.exists(evidence_path)

    @_skip_no_client
    def test_evidence_inside_git_ancestor_rejected(self, real_transport_modules, tmp_path):
        """Evidence path inside a Git checkout → not_attempted, no POST."""
        import datetime
        import os

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)

        # Create a fake git repo inside tmp_path
        git_root = tmp_path / "fakerepo"
        git_root.mkdir(mode=0o700)
        (git_root / ".git").mkdir()
        ev_dir = git_root / "data"
        ev_dir.mkdir(mode=0o700)
        evidence_path = str(ev_dir / "evidence.json")
        write_cutoff, deadline = self._make_timing()

        constructor_called = []
        original_init = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            constructor_called.append(1)
            original_init(self_client, **kwargs)

        import unittest.mock

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            result = aw.submit_recovery_batch(
                [record],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                batch_limit=100,
                write_cutoff=write_cutoff,
                deadline=deadline,
                evidence_path=evidence_path,
                timeout=5,
                _clock=lambda: 0.0,
            )

        assert result["attempted"] is False
        assert result["transport_status"] == "not_attempted"
        assert "git" in (result["error"] or "").lower()
        assert len(constructor_called) == 0, "constructor must not be called for git-ancestor path"
        assert not os.path.exists(evidence_path)

    @_skip_no_client
    def test_zero_byte_write_raises_oserror_no_post(self, real_transport_modules, tmp_path):
        """os.write returning 0 raises OSError; failure path preserves evidence path, no POST."""
        import datetime
        import unittest.mock

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)
        evidence_path = str(tmp_path / "ev_zero.json")
        write_cutoff, deadline = self._make_timing()

        constructor_called = []
        original_init = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            constructor_called.append(1)
            original_init(self_client, **kwargs)

        # Monkeypatch os.write to return 0 once then restore
        call_count = [0]

        def _zero_write(fd, buf):
            call_count[0] += 1
            return 0  # triggers the guard

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            with unittest.mock.patch("os.write", _zero_write):
                result = aw.submit_recovery_batch(
                    [record],
                    authorized_keys={key},
                    start_date=issue,
                    end_date=issue,
                    forecast_date=issue,
                    base_url="http://fake-sapphire.local",
                    batch_limit=100,
                    write_cutoff=write_cutoff,
                    deadline=deadline,
                    evidence_path=evidence_path,
                    timeout=5,
                    _clock=lambda: 0.0,
                )

        assert result["attempted"] is False
        assert result["transport_status"] == "not_attempted"
        assert "evidence" in (result["error"] or "").lower()
        # evidence_path is preserved in result (file may or may not exist depending on O_CREAT)
        assert result["evidence_path"] == evidence_path or result["evidence_path"] is None
        assert len(constructor_called) == 0, (
            "constructor must not be called after evidence write failure"
        )

    @_skip_no_client
    def test_partial_write_loops_complete_evidence_parseable(
        self, real_transport_modules, tmp_path
    ):
        """os.write returning partial bytes loops until complete; evidence is valid JSON."""
        import datetime
        import json
        import os
        import unittest.mock

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]
        store = _FakeStore()

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)
        evidence_path = str(tmp_path / "ev_partial.json")
        write_cutoff, deadline = self._make_timing()

        real_os_write = os.write
        write_call_results = []

        def _partial_write(fd, buf):
            # Return half the bytes the first call, then the rest
            if not write_call_results:
                n = max(1, len(buf) // 2)
                written = real_os_write(fd, buf[:n])
                write_call_results.append(written)
                return written
            return real_os_write(fd, buf)

        original_init = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            original_init(self_client, **kwargs)
            self_client.session.mount("http://", _make_fake_adapter(store))

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            with unittest.mock.patch("os.write", _partial_write):
                result = aw.submit_recovery_batch(
                    [record],
                    authorized_keys={key},
                    start_date=issue,
                    end_date=issue,
                    forecast_date=issue,
                    base_url="http://fake-sapphire.local",
                    batch_limit=100,
                    write_cutoff=write_cutoff,
                    deadline=deadline,
                    evidence_path=evidence_path,
                    timeout=5,
                    _clock=lambda: 0.0,
                )

        assert result["attempted"] is True
        assert result["transport_status"] == "acknowledged"
        assert os.path.exists(evidence_path)
        with open(evidence_path) as f:
            ev = json.load(f)
        assert ev["n_records"] == 1
        # Partial write was used (at least one partial call happened)
        assert len(write_call_results) >= 1

    @_skip_no_client
    def test_records_not_list_rejected_before_effects(self, real_transport_modules, tmp_path):
        """records=tuple → not_attempted, zero constructor/POST/evidence."""
        import datetime
        import os
        import unittest.mock

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)
        evidence_path = str(tmp_path / "ev_notlist.json")
        write_cutoff, deadline = self._make_timing()

        constructor_called = []
        original_init = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            constructor_called.append(1)
            original_init(self_client, **kwargs)

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            result = aw.submit_recovery_batch(
                (record,),  # tuple, not list
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                batch_limit=100,
                write_cutoff=write_cutoff,
                deadline=deadline,
                evidence_path=evidence_path,
                timeout=5,
                _clock=lambda: 0.0,
            )

        assert result["attempted"] is False
        assert result["transport_status"] == "not_attempted"
        assert "list" in (result["error"] or "").lower()
        assert len(constructor_called) == 0, "constructor must not be called for invalid records"
        assert not os.path.exists(evidence_path)

    @_skip_no_client
    def test_constructor_raises_not_attempted_evidence_retained(
        self, real_transport_modules, tmp_path
    ):
        """SapphirePostprocessingClient.__init__ raises → not_attempted, evidence_path retained."""
        import datetime
        import os
        import unittest.mock

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]
        store = _FakeStore()

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)
        evidence_path = str(tmp_path / "ev_ctorr.json")
        write_cutoff, deadline = self._make_timing()

        def _failing_init(self_client, **kwargs):
            raise RuntimeError("constructor intentionally failed")

        with unittest.mock.patch.object(_SPC, "__init__", _failing_init):
            result = aw.submit_recovery_batch(
                [record],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                batch_limit=100,
                write_cutoff=write_cutoff,
                deadline=deadline,
                evidence_path=evidence_path,
                timeout=5,
                _clock=lambda: 0.0,
            )

        assert result["attempted"] is False, "constructor failure must yield attempted=False"
        assert result["transport_status"] == "not_attempted"
        # Evidence was created before constructor was called
        assert os.path.exists(evidence_path), (
            "evidence file must be retained after constructor failure"
        )
        assert result["evidence_path"] == evidence_path
        # attempted=False → attempted_records must be empty, not _validated
        assert result["attempted_records"] == [], (
            "constructor failure (attempted=False) must return attempted_records=[], "
            f"got {result['attempted_records']!r}"
        )
        # No POST happened (store is empty)
        assert len(store.post_calls) == 0

    @_skip_no_client
    def test_datetime_as_start_date_rejected(self, real_transport_modules, tmp_path):
        """start_date as datetime (not date) → not_attempted, zero constructor/POST/evidence."""
        import datetime
        import os
        import unittest.mock

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        issue_dt = datetime.datetime(2026, 1, 5, 0, 0, 0)  # datetime, not date
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)
        evidence_path = str(tmp_path / "ev_dtstart.json")
        write_cutoff, deadline = self._make_timing()

        constructor_called = []
        original_init = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            constructor_called.append(1)
            original_init(self_client, **kwargs)

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            result = aw.submit_recovery_batch(
                [record],
                authorized_keys={key},
                start_date=issue_dt,  # datetime, not date
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                batch_limit=100,
                write_cutoff=write_cutoff,
                deadline=deadline,
                evidence_path=evidence_path,
                timeout=5,
                _clock=lambda: 0.0,
            )

        assert result["attempted"] is False
        assert result["transport_status"] == "not_attempted"
        assert "start_date" in (result["error"] or "")
        assert "datetime" in (result["error"] or "").lower()
        assert len(constructor_called) == 0
        assert not os.path.exists(evidence_path)


# ---------------------------------------------------------------------------
# NEWP3: reconcile_recovery_batches — fake HTTP adapter tests
# ---------------------------------------------------------------------------


def _make_readback_adapter(store: _FakeStore, fail_get=False, fail_second_page=False):
    """Return a requests adapter that handles GET /forecast/ from _FakeStore.

    fail_get: raise requests.Timeout on every GET.
    fail_second_page: raise on GET with skip > 0 (simulates page-2 failure).
    """
    import json as _json

    import requests
    import requests.adapters

    class _ReadbackAdapter(requests.adapters.BaseAdapter):
        def send(self, prepared_request, **kwargs):
            from urllib.parse import parse_qs, urlparse

            if prepared_request.method == "POST":
                body = _json.loads(prepared_request.body)
                store.post_calls.append(body)
                data = body.get("data", [])
                result = [store.upsert(rec) for rec in data]
                return _make_fake_response(200, result)
            elif prepared_request.method == "GET":
                if fail_get:
                    raise requests.Timeout("fake GET timeout")
                parsed = urlparse(prepared_request.url)
                params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
                skip = int(params.get("skip", 0))
                limit = int(params.get("limit", 500))
                if fail_second_page and skip > 0:
                    raise requests.Timeout("fake second-page timeout")
                # Filter store by query params
                horizon = params.get("horizon", "")
                model = params.get("model", "")
                code = params.get("code", "")
                start_date = params.get("start_date", "")
                end_date = params.get("end_date", "")
                matching = []
                for rec in store.get_records():
                    # horizon match: API uses decade for decad
                    rec_ht = rec.get("horizon_type", "")
                    if horizon and rec_ht != horizon:
                        continue
                    if model and rec.get("model_type", "") != model:
                        continue
                    if code and rec.get("code", "") != code:
                        continue
                    rec_date = str(rec.get("date", ""))
                    if start_date and rec_date < start_date:
                        continue
                    if end_date and rec_date > end_date:
                        continue
                    matching.append(rec)
                page = matching[skip : skip + limit]
                return _make_fake_response(200, page)
            return _make_fake_response(405, [])

        def close(self):
            pass

    return _ReadbackAdapter()


class TestNewP3ReconcileRecoveryBatches:
    """NEWP3: reconcile_recovery_batches — real native client + fake HTTP adapter."""

    def _make_timing(self, budget=60.0):
        import time

        now = time.monotonic()
        return now + budget

    @_skip_no_client
    def test_full_flow_plan_submit_reconcile_verified(self, real_transport_modules, tmp_path):
        """Full flow: real plan_period_refresh → select_recovery_records → submit → reconcile.

        - One missing record (plan.missing) whose candidate payload goes through the planner.
        - One explicit refresh whose merged metadata differs from the raw candidate so that
          bypassing the planner's merged payload would produce a different wire record.
        - Healthy unauthorized record must not be touched.
        - Exactly one POST (both records are in the same model/horizon scope).
        - Readback must verify both submitted keys.
        """
        import datetime
        import unittest.mock

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        # issue Jan 5 → target Jan 6: pentad-in-month=2, pentad-in-year=2
        issue_missing = datetime.date(2026, 1, 5)
        target_missing = issue_missing + datetime.timedelta(days=1)
        # issue Jan 10 → target Jan 11: pentad-in-month=3, pentad-in-year=3
        issue_refresh = datetime.date(2026, 1, 10)
        target_refresh = issue_refresh + datetime.timedelta(days=1)
        forecast_date = datetime.date(2026, 1, 10)

        key_missing = ("pentad", "19999", "TFT", issue_missing, target_missing)
        key_refresh = ("pentad", "19999", "TFT", issue_refresh, target_refresh)

        # Candidate for the missing key (no existing row → plan.missing).
        candidate_missing = {
            "horizon_type": "pentad",
            "code": "19999",
            "model_type": "TFT",
            "date": issue_missing,
            "target": target_missing,
            "forecasted_discharge": 100.0,
            "q05": 80.0,
            "q25": 90.0,
            "q75": 110.0,
            "q95": 120.0,
            "flag": None,
            "composition": None,
            "horizon_value": 2,
            "horizon_in_year": 2,
        }

        # For the refresh key: the candidate has forecasted_discharge=200.0, flag=None.
        # The existing row has forecasted_discharge=999.0, flag=7.
        # Planner merges: copies existing (keeping flag=7), then overwrites ALL5 with
        # candidate's values (fd=200.0) and composition if candidate's is non-None.
        # So merged_payload["flag"] == 7 (existing's value) ≠ candidate's None.
        candidate_refresh = {
            "horizon_type": "pentad",
            "code": "19999",
            "model_type": "TFT",
            "date": issue_refresh,
            "target": target_refresh,
            "forecasted_discharge": 200.0,
            "q05": 160.0,
            "q25": 180.0,
            "q75": 220.0,
            "q95": 240.0,
            "flag": None,
            "composition": None,
            "horizon_value": 3,
            "horizon_in_year": 3,
        }
        existing_refresh = dict(candidate_refresh, forecasted_discharge=999.0, flag=7)

        # Unauthorized healthy record — must not be touched.
        issue_healthy = datetime.date(2026, 1, 5)

        authorized_keys = {key_missing, key_refresh}
        explicit_refresh_keys = {key_refresh}

        plan = gd.plan_period_refresh(
            [existing_refresh],  # existing: only the refresh record differs
            [candidate_missing, candidate_refresh],
            authorized_keys,
            explicit_refresh_keys,
            start_date=issue_missing,
            end_date=issue_refresh,
            forecast_date=forecast_date,
            existing_read_status="COMPLETE",
        )
        assert not plan.plan_error, plan.plan_error
        assert key_missing in plan.missing, plan.missing
        # The refresh key lands in authorized_refresh (existing differs, key is in
        # explicit_refresh_keys)
        refresh_keys_in_plan = {k for k, _ in plan.authorized_refresh}
        assert key_refresh in refresh_keys_in_plan, plan.authorized_refresh

        # Verify the planner's merged payload carries flag=7 from existing (not None from candidate).
        merged_payload = next(p for k, p in plan.authorized_refresh if k == key_refresh)
        assert merged_payload["flag"] == 7, (
            f"merged_payload must inherit flag=7 from existing, got {merged_payload['flag']!r}"
        )
        assert candidate_refresh["flag"] is None, "sanity: candidate flag is None"

        # select_recovery_records must exercise the planner's merged payload for refresh.
        selected = aw.select_recovery_records(
            [candidate_missing, candidate_refresh],
            plan,
            authorized_keys,
            start_date=issue_missing,
            end_date=issue_refresh,
            forecast_date=forecast_date,
        )
        assert len(selected) == 2, f"expected 2 selected records, got {len(selected)}: {selected}"
        selected_by_date = {r["date"]: r for r in selected}
        assert issue_missing.isoformat() in selected_by_date
        assert issue_refresh.isoformat() in selected_by_date

        # Verify the refresh record payload equals the planner's merged result:
        # forecasted_discharge from candidate=200.0, flag=7 from existing.
        refresh_wire = selected_by_date[issue_refresh.isoformat()]
        assert refresh_wire["forecasted_discharge"] == 200.0, (
            f"refresh wire must carry candidate fd=200.0, got {refresh_wire['forecasted_discharge']!r}"
        )
        assert refresh_wire["flag"] == 7, (
            f"refresh wire must carry existing flag=7, got {refresh_wire['flag']!r}"
        )

        # Verify all 14 fields of refresh wire equal the serialized merged_payload.
        serialized_merged = aw._recovery_wire_serialize(merged_payload)
        assert refresh_wire == serialized_merged, (
            f"refresh wire must equal serialized merged_payload; diff: "
            f"{set(refresh_wire.items()) ^ set(serialized_merged.items())}"
        )

        _SPC = aw.SapphirePostprocessingClient
        store = _FakeStore()

        # Preload existing_refresh and healthy_unauthorized into store so we can
        # assert they are unchanged (not merely absent) after the submit.
        existing_refresh_wire = aw._recovery_wire_serialize(existing_refresh)
        store.upsert(existing_refresh_wire)

        healthy_record = dict(
            _make_valid_wire_record(issue_date=issue_healthy, model_type="TFT"),
            code="19998",
        )
        healthy_store_key = (
            healthy_record["horizon_type"],
            healthy_record["code"],
            healthy_record["model_type"],
            healthy_record["date"],
            healthy_record["target"],
        )
        store.upsert(healthy_record)
        healthy_before = dict(store._records[healthy_store_key])

        evidence_path = str(tmp_path / "ev_reconcile.json")
        deadline = 100.0
        write_cutoff = 60.0

        original_init = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            original_init(self_client, **kwargs)
            adapter = _make_readback_adapter(store)
            self_client.session.mount("http://", adapter)

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            submit_result = aw.submit_recovery_batch(
                selected,
                authorized_keys=authorized_keys,
                start_date=issue_missing,
                end_date=issue_refresh,
                forecast_date=forecast_date,
                base_url="http://fake-sapphire.local",
                batch_limit=100,
                write_cutoff=write_cutoff,
                deadline=deadline,
                evidence_path=evidence_path,
                timeout=5,
                _clock=lambda: 0.0,
            )

        assert submit_result["transport_status"] == "acknowledged", submit_result
        assert len(store.post_calls) == 1, "exactly one POST for all records in scope"

        # Reconcile: readback should verify both submitted keys.
        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            rec_result = aw.reconcile_recovery_batches(
                [submit_result],
                authorized_keys=authorized_keys,
                start_date=issue_missing,
                end_date=issue_refresh,
                forecast_date=forecast_date,
                base_url="http://fake-sapphire.local",
                deadline=100.0,
                timeout=5,
                _clock=lambda: 0.0,
            )

        assert rec_result["reconciliation_status"] == "verified", rec_result
        assert key_missing in rec_result["verified_keys"]
        assert key_refresh in rec_result["verified_keys"]
        assert key_missing not in rec_result["unverified_keys"]
        assert key_refresh not in rec_result["unverified_keys"]
        assert key_missing not in rec_result["original_transport_uncertain"]
        assert key_refresh not in rec_result["original_transport_uncertain"]

        # Healthy unauthorized record must be unchanged (preloaded → still equal to before).
        assert healthy_store_key in store._records, (
            "healthy unauthorized record must still be present in the store"
        )
        assert store._records[healthy_store_key] == healthy_before, (
            "healthy unauthorized record must be unchanged after submit"
        )

    @_skip_no_client
    def test_commit_then_timeout_uncertain_transport_readback_verifies(
        self, real_transport_modules, tmp_path
    ):
        """Commit-then-timeout: one POST, uncertain transport, readback verifies.

        Original transport uncertainty must be retained in original_transport_uncertain
        even when readback confirms the key is written.
        """
        import datetime
        import unittest.mock

        import requests

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)
        store = _FakeStore()
        evidence_path = str(tmp_path / "ev_ctt.json")
        deadline = self._make_timing()
        write_cutoff = deadline - 10.0

        original_init = _SPC.__init__

        def _patched_init_commit_then_raise(self_client, **kwargs):
            original_init(self_client, **kwargs)
            adapter = _make_fake_adapter(
                store, commit_then_raise=requests.Timeout("fake timeout after commit")
            )
            self_client.session.mount("http://", adapter)

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init_commit_then_raise):
            submit_result = aw.submit_recovery_batch(
                [record],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                batch_limit=100,
                write_cutoff=write_cutoff,
                deadline=deadline,
                evidence_path=evidence_path,
                timeout=5,
            )

        # Exactly one POST, transport uncertain (Timeout)
        assert submit_result["attempted"] is True
        assert submit_result["transport_status"] == "uncertain"
        assert len(store.post_calls) == 1, "must not resend"

        # Record WAS committed to store despite the Timeout
        committed = store.get_records()
        assert len(committed) == 1

        # Reconcile: readback should verify the key
        def _patched_init_readback(self_client, **kwargs):
            original_init(self_client, **kwargs)
            adapter = _make_readback_adapter(store)
            self_client.session.mount("http://", adapter)

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init_readback):
            rec_result = aw.reconcile_recovery_batches(
                [submit_result],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                deadline=self._make_timing(),
                timeout=5,
            )

        assert key in rec_result["verified_keys"], rec_result
        # Original transport uncertainty retained
        assert key in rec_result["original_transport_uncertain"]
        assert rec_result["reconciliation_status"] == "verified"

    @_skip_no_client
    def test_earlier_ack_batch_and_ambiguous_last_both_readback(
        self, real_transport_modules, tmp_path
    ):
        """Two batches: earlier acknowledged + ambiguous last; both verified by readback."""
        import datetime
        import unittest.mock

        import requests

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue1 = datetime.date(2026, 1, 5)
        issue2 = datetime.date(2026, 1, 10)
        r1 = _make_valid_wire_record(issue_date=issue1, model_type="TFT")
        r2 = _make_valid_wire_record(issue_date=issue2, model_type="TFT")
        key1 = gd._plan_key(r1)
        key2 = gd._plan_key(r2)
        store = _FakeStore()

        original_init = _SPC.__init__

        # First batch: acknowledged
        def _init_ack(self_client, **kwargs):
            original_init(self_client, **kwargs)
            self_client.session.mount("http://", _make_readback_adapter(store))

        ep1 = str(tmp_path / "ev1.json")
        deadline = self._make_timing()
        wc = deadline - 10.0

        with unittest.mock.patch.object(_SPC, "__init__", _init_ack):
            b1 = aw.submit_recovery_batch(
                [r1],
                authorized_keys={key1},
                start_date=issue1,
                end_date=issue1,
                forecast_date=issue2,
                base_url="http://fake-sapphire.local",
                batch_limit=100,
                write_cutoff=wc,
                deadline=deadline,
                evidence_path=ep1,
                timeout=5,
            )
        assert b1["transport_status"] == "acknowledged"

        # Second batch: commit-then-timeout
        def _init_ctt(self_client, **kwargs):
            original_init(self_client, **kwargs)
            adapter = _make_fake_adapter(
                store, commit_then_raise=requests.Timeout("timeout batch 2")
            )
            self_client.session.mount("http://", adapter)

        ep2 = str(tmp_path / "ev2.json")
        with unittest.mock.patch.object(_SPC, "__init__", _init_ctt):
            b2 = aw.submit_recovery_batch(
                [r2],
                authorized_keys={key2},
                start_date=issue2,
                end_date=issue2,
                forecast_date=issue2,
                base_url="http://fake-sapphire.local",
                batch_limit=100,
                write_cutoff=wc,
                deadline=deadline,
                evidence_path=ep2,
                timeout=5,
            )
        assert b2["transport_status"] == "uncertain"

        # Reconcile both
        def _init_rb(self_client, **kwargs):
            original_init(self_client, **kwargs)
            self_client.session.mount("http://", _make_readback_adapter(store))

        with unittest.mock.patch.object(_SPC, "__init__", _init_rb):
            rec = aw.reconcile_recovery_batches(
                [b1, b2],
                authorized_keys={key1, key2},
                start_date=issue1,
                end_date=issue2,
                forecast_date=issue2,
                base_url="http://fake-sapphire.local",
                deadline=self._make_timing(),
                timeout=5,
            )

        assert key1 in rec["verified_keys"], rec
        assert key2 in rec["verified_keys"], rec
        # key1 acknowledged, key2 uncertain
        assert key1 not in rec["original_transport_uncertain"]
        assert key2 in rec["original_transport_uncertain"]
        assert rec["reconciliation_status"] == "verified"

    @_skip_no_client
    def test_changed_quantile_unverified(self, real_transport_modules, tmp_path):
        """Readback returns different q05 value → key unverified."""
        import datetime
        import unittest.mock

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)
        store = _FakeStore()

        original_init = _SPC.__init__

        def _init_success(self_client, **kwargs):
            original_init(self_client, **kwargs)
            self_client.session.mount("http://", _make_readback_adapter(store))

        ep = str(tmp_path / "ev_q05.json")
        deadline = self._make_timing()
        wc = deadline - 10.0

        with unittest.mock.patch.object(_SPC, "__init__", _init_success):
            sub = aw.submit_recovery_batch(
                [record],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                batch_limit=100,
                write_cutoff=wc,
                deadline=deadline,
                evidence_path=ep,
                timeout=5,
            )
        assert sub["transport_status"] == "acknowledged"

        # Tamper the store's q05 to simulate a different value being returned
        store_key = (
            record["horizon_type"],
            record["code"],
            record["model_type"],
            record["date"],
            record["target"],
        )
        store._records[store_key]["q05"] = 99999.0

        def _init_rb(self_client, **kwargs):
            original_init(self_client, **kwargs)
            self_client.session.mount("http://", _make_readback_adapter(store))

        with unittest.mock.patch.object(_SPC, "__init__", _init_rb):
            rec = aw.reconcile_recovery_batches(
                [sub],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                deadline=self._make_timing(),
                timeout=5,
            )

        assert key in rec["unverified_keys"], rec
        assert "q05" in rec["reasons"].get(key, "")
        assert rec["reconciliation_status"] == "unverified"

    @_skip_no_client
    def test_confirmed_empty_read_unverified(self, real_transport_modules, tmp_path):
        """Readback returns empty (nothing written) → all keys unverified."""
        import datetime
        import unittest.mock

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)

        # Store starts empty — nothing ever written
        empty_store = _FakeStore()
        original_init = _SPC.__init__

        # Fake a submit_result as if something was attempted
        fake_submit = {
            "attempted": True,
            "transport_status": "uncertain",
            "acknowledged_count": None,
            "attempted_records": [record],
            "evidence_path": str(tmp_path / "fake_ev.json"),
            "error": "fake timeout",
        }

        def _init_rb(self_client, **kwargs):
            original_init(self_client, **kwargs)
            self_client.session.mount("http://", _make_readback_adapter(empty_store))

        with unittest.mock.patch.object(_SPC, "__init__", _init_rb):
            rec = aw.reconcile_recovery_batches(
                [fake_submit],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                deadline=self._make_timing(),
                timeout=5,
            )

        assert key in rec["unverified_keys"], rec
        assert rec["reconciliation_status"] == "unverified"

    @_skip_no_client
    def test_deadline_exhausted_stops_reads_leaves_unverified(
        self, real_transport_modules, tmp_path
    ):
        """Deadline already passed at call time → all keys unverified, no GET calls."""
        import datetime
        import time
        import unittest.mock

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)

        store = _FakeStore()
        get_called = []

        original_init = _SPC.__init__

        class _TrackingAdapter(_make_readback_adapter(store).__class__):
            pass

        import requests.adapters

        class _TrackGetAdapter(requests.adapters.BaseAdapter):
            def send(self, prepared_request, **kwargs):
                if prepared_request.method == "GET":
                    get_called.append(1)
                return _make_fake_response(200, [])

            def close(self):
                pass

        def _init_track(self_client, **kwargs):
            original_init(self_client, **kwargs)
            self_client.session.mount("http://", _TrackGetAdapter())

        past_deadline = time.monotonic() - 1.0

        fake_submit = {
            "attempted": True,
            "transport_status": "acknowledged",
            "acknowledged_count": 1,
            "attempted_records": [record],
            "evidence_path": str(tmp_path / "fake_ev2.json"),
            "error": None,
        }

        with unittest.mock.patch.object(_SPC, "__init__", _init_track):
            rec = aw.reconcile_recovery_batches(
                [fake_submit],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                deadline=past_deadline,
                timeout=5,
            )

        assert key in rec["unverified_keys"], rec
        assert "deadline" in rec["reasons"].get(key, "").lower()
        assert len(get_called) == 0, "no GET should be made when deadline is already past"

    @_skip_no_client
    def test_no_attempted_records_noop(self, real_transport_modules, tmp_path):
        """All batches have attempted=False → no_attempted_records, no reads."""
        import datetime
        import unittest.mock

        aw = real_transport_modules["aw"]

        issue = datetime.date(2026, 1, 5)
        get_called = []

        original_init = _SPC.__init__

        import requests.adapters

        class _TrackGetAdapter(requests.adapters.BaseAdapter):
            def send(self, prepared_request, **kwargs):
                if prepared_request.method == "GET":
                    get_called.append(1)
                return _make_fake_response(200, [])

            def close(self):
                pass

        def _init_track(self_client, **kwargs):
            original_init(self_client, **kwargs)
            self_client.session.mount("http://", _TrackGetAdapter())

        not_attempted = {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": "cutoff passed",
        }

        with unittest.mock.patch.object(_SPC, "__init__", _init_track):
            rec = aw.reconcile_recovery_batches(
                [not_attempted],
                authorized_keys=set(),
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                deadline=self._make_timing(),
                timeout=5,
            )

        assert rec["reconciliation_status"] == "no_attempted_records"
        assert rec["verified_keys"] == []
        assert rec["unverified_keys"] == []
        assert len(get_called) == 0

    @_skip_no_client
    def test_dangling_symlink_evidence_path_rejected(self, real_transport_modules, tmp_path):
        """Dangling symlink as evidence_path → not_attempted, no POST."""
        import datetime
        import os
        import unittest.mock

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)

        # Create a dangling symlink (points to non-existent target)
        symlink_path = str(tmp_path / "dangling_link.json")
        os.symlink("/nonexistent_target_1234567890abcdef.json", symlink_path)
        assert os.path.lexists(symlink_path)
        assert not os.path.exists(symlink_path)  # confirms dangling

        store = _FakeStore()
        constructor_called = []
        original_init = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            constructor_called.append(1)
            original_init(self_client, **kwargs)
            self_client.session.mount("http://", _make_readback_adapter(store))

        import time

        wc = time.monotonic() + 60.0
        dl = wc + 10.0

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            result = aw.submit_recovery_batch(
                [record],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                batch_limit=100,
                write_cutoff=wc,
                deadline=dl,
                evidence_path=symlink_path,
                timeout=5,
            )

        assert result["attempted"] is False
        assert result["transport_status"] == "not_attempted"
        assert result["error"] is not None
        assert len(constructor_called) == 0, "constructor must not be called for dangling symlink"
        assert len(store.post_calls) == 0

    # -----------------------------------------------------------------------
    # NEWP3 negative cases: input validation — zero reads, zero POST
    # -----------------------------------------------------------------------

    @_skip_no_client
    def test_unauthorized_key_unverified_no_reads(self, real_transport_modules):
        """Attempted record with key not in authorized_keys → unverified, zero reads."""
        import datetime
        import unittest.mock

        aw = real_transport_modules["aw"]

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)

        get_called = []
        original_init = _SPC.__init__

        import requests.adapters

        class _TrackGet(requests.adapters.BaseAdapter):
            def send(self, req, **kw):
                if req.method == "GET":
                    get_called.append(1)
                return _make_fake_response(200, [])

            def close(self):
                pass

        def _patched_init(self_client, **kwargs):
            original_init(self_client, **kwargs)
            self_client.session.mount("http://", _TrackGet())

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            rec = aw.reconcile_recovery_batches(
                [
                    {
                        "attempted": True,
                        "transport_status": "acknowledged",
                        "acknowledged_count": 1,
                        "attempted_records": [record],
                        "evidence_path": "/tmp/fake_ev_auth.json",
                        "error": None,
                    }
                ],
                authorized_keys=set(),  # key not authorized
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                deadline=self._make_timing(),
                timeout=5,
            )

        assert rec["reconciliation_status"] == "unverified"
        assert "authorized" in (rec.get("error") or "").lower()
        assert len(get_called) == 0, "zero GET on unauthorized key"

    @_skip_no_client
    def test_attempted_true_empty_records_unverified_not_noop(self, real_transport_modules):
        """attempted=True with empty attempted_records → unverified (not no_attempted_records)."""
        import datetime

        aw = real_transport_modules["aw"]
        issue = datetime.date(2026, 1, 5)

        rec = aw.reconcile_recovery_batches(
            [
                {
                    "attempted": True,
                    "transport_status": "acknowledged",
                    "acknowledged_count": 0,
                    "attempted_records": [],
                    "evidence_path": "/tmp/fake_ev_empty.json",
                    "error": None,
                }
            ],
            authorized_keys=set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            base_url="http://fake-sapphire.local",
            deadline=self._make_timing(),
            timeout=5,
        )
        assert rec["reconciliation_status"] == "unverified"
        assert rec.get("error") is not None

    @_skip_no_client
    def test_non_bool_attempted_unverified(self, real_transport_modules):
        """batch['attempted'] = integer 1 (not bool True) → unverified."""
        import datetime

        aw = real_transport_modules["aw"]
        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)

        rec = aw.reconcile_recovery_batches(
            [
                {
                    "attempted": 1,
                    "transport_status": "acknowledged",  # int, not bool
                    "acknowledged_count": 1,
                    "attempted_records": [record],
                    "evidence_path": "/tmp/fake_ev_nb.json",
                    "error": None,
                }
            ],
            authorized_keys=set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            base_url="http://fake-sapphire.local",
            deadline=self._make_timing(),
            timeout=5,
        )
        assert rec["reconciliation_status"] == "unverified"
        assert "bool" in (rec.get("error") or "").lower()

    @_skip_no_client
    def test_issue_date_after_end_date_unverified(self, real_transport_modules):
        """Attempted record with issue date after end_date → unverified, zero reads."""
        import datetime
        import unittest.mock

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        late_issue = datetime.date(2026, 1, 15)
        record = _make_valid_wire_record(issue_date=late_issue)
        key = gd._plan_key(record)

        get_called = []
        original_init = _SPC.__init__

        import requests.adapters

        class _TrackGet(requests.adapters.BaseAdapter):
            def send(self, req, **kw):
                if req.method == "GET":
                    get_called.append(1)
                return _make_fake_response(200, [])

            def close(self):
                pass

        def _patched_init(self_client, **kwargs):
            original_init(self_client, **kwargs)
            self_client.session.mount("http://", _TrackGet())

        end = datetime.date(2026, 1, 10)  # record is after end_date
        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            rec = aw.reconcile_recovery_batches(
                [
                    {
                        "attempted": True,
                        "transport_status": "acknowledged",
                        "acknowledged_count": 1,
                        "attempted_records": [record],
                        "evidence_path": "/tmp/fake_ev_late.json",
                        "error": None,
                    }
                ],
                authorized_keys={key},
                start_date=datetime.date(2026, 1, 1),
                end_date=end,
                forecast_date=late_issue,
                base_url="http://fake-sapphire.local",
                deadline=self._make_timing(),
                timeout=5,
            )

        assert rec["reconciliation_status"] == "unverified"
        assert rec.get("error") is not None
        assert len(get_called) == 0, "zero GET on out-of-bounds key"

    # -----------------------------------------------------------------------
    # NEWP3 negative cases: canonicalized comparison — zero POST always
    # -----------------------------------------------------------------------

    @_skip_no_client
    def test_changed_flag_unverified_zero_post(self, real_transport_modules, tmp_path):
        """Readback returns different flag value → key unverified; zero POST."""
        import datetime
        import unittest.mock

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)

        store = _FakeStore()
        store_key = (
            record["horizon_type"],
            record["code"],
            record["model_type"],
            record["date"],
            record["target"],
        )
        store._records[store_key] = dict(record, flag=7)  # tamper flag

        original_init = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            original_init(self_client, **kwargs)
            self_client.session.mount("http://", _make_readback_adapter(store))

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            rec = aw.reconcile_recovery_batches(
                [
                    {
                        "attempted": True,
                        "transport_status": "acknowledged",
                        "acknowledged_count": 1,
                        "attempted_records": [record],
                        "evidence_path": str(tmp_path / "ev_flag.json"),
                        "error": None,
                    }
                ],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                deadline=self._make_timing(),
                timeout=5,
            )

        assert key in rec["unverified_keys"], rec
        assert "flag" in rec["reasons"].get(key, "")
        assert rec["reconciliation_status"] == "unverified"
        assert len(store.post_calls) == 0, "zero POST always"

    @_skip_no_client
    def test_changed_composition_unverified_zero_post(self, real_transport_modules, tmp_path):
        """Readback returns different composition → key unverified; zero POST."""
        import datetime
        import unittest.mock

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        record = dict(
            _make_valid_wire_record(issue_date=issue, model_type="ENSEMBLE_MEAN"),
            composition="TFT,TIDE",
        )
        key = gd._plan_key(record)

        store = _FakeStore()
        store_key = (
            record["horizon_type"],
            record["code"],
            record["model_type"],
            record["date"],
            record["target"],
        )
        store._records[store_key] = dict(record, composition="TFT")  # tamper composition

        original_init = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            original_init(self_client, **kwargs)
            self_client.session.mount("http://", _make_readback_adapter(store))

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            rec = aw.reconcile_recovery_batches(
                [
                    {
                        "attempted": True,
                        "transport_status": "acknowledged",
                        "acknowledged_count": 1,
                        "attempted_records": [record],
                        "evidence_path": str(tmp_path / "ev_comp.json"),
                        "error": None,
                    }
                ],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                deadline=self._make_timing(),
                timeout=5,
            )

        assert key in rec["unverified_keys"], rec
        assert "composition" in rec["reasons"].get(key, "")
        assert rec["reconciliation_status"] == "unverified"
        assert len(store.post_calls) == 0, "zero POST always"

    @_skip_no_client
    def test_all_five_quantities_changed_unverified_zero_post(
        self, real_transport_modules, tmp_path
    ):
        """Readback returns all 5 quantities changed → key unverified; zero POST."""
        import datetime
        import unittest.mock

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)

        store = _FakeStore()
        store_key = (
            record["horizon_type"],
            record["code"],
            record["model_type"],
            record["date"],
            record["target"],
        )
        store._records[store_key] = dict(
            record,
            forecasted_discharge=999.0,
            q05=999.0,
            q25=999.0,
            q75=999.0,
            q95=999.0,
        )

        original_init = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            original_init(self_client, **kwargs)
            self_client.session.mount("http://", _make_readback_adapter(store))

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            rec = aw.reconcile_recovery_batches(
                [
                    {
                        "attempted": True,
                        "transport_status": "acknowledged",
                        "acknowledged_count": 1,
                        "attempted_records": [record],
                        "evidence_path": str(tmp_path / "ev_all5.json"),
                        "error": None,
                    }
                ],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                deadline=self._make_timing(),
                timeout=5,
            )

        assert key in rec["unverified_keys"], rec
        assert rec["reconciliation_status"] == "unverified"
        assert len(store.post_calls) == 0, "zero POST always"


# ===========================================================================
# P3 reconcile_recovery_batches — narrow readback correction tests
# ===========================================================================


class TestReconcileRecoveryBatchesP3Corrections:
    """Targeted regression tests for the three P3 corrections to reconcile_recovery_batches.

    1. Canonical serialisation stored in key_to_records (TIDE/TiDE alias + date object variants).
    2. Scope-bounds order: start_date <= end_date <= forecast_date enforced before reads.
    3. Exception-handler partition: already-verified keys stay verified after later-scope failure.

    Also covers two missing fake-HTTP readback tests:
    4. Malformed (non-list) raw API response → key in unverified_keys, zero POSTs.
    5. Read failure on a later pagination page → no keys in failed scope, zero POSTs.
    """

    # ------------------------------------------------------------------
    # Helpers shared across tests
    # ------------------------------------------------------------------

    @staticmethod
    def _make_timing(budget: float = 100.0):
        """Return a finite deadline; pair with _clock=lambda:0.0 for all reads."""
        return budget

    @staticmethod
    def _base_record(
        code="19999",
        date_str="2026-01-05",
        target_str="2026-01-06",
        model="TFT",
        ht="pentad",
        hv=2,
        hiy=2,
    ):
        """Minimal valid 14-field wire record for code=19999."""
        return {
            "horizon_type": ht,
            "code": code,
            "model_type": model,
            "date": date_str,
            "target": target_str,
            "forecasted_discharge": 55.0,
            "q05": 40.0,
            "q25": 48.0,
            "q75": 62.0,
            "q95": 70.0,
            "flag": None,
            "composition": None,
            "horizon_value": hv,
            "horizon_in_year": hiy,
        }

    # ------------------------------------------------------------------
    # Correction 1: Canonical serialisation — alias/date-object variants
    # ------------------------------------------------------------------

    def test_tide_alias_exact_duplicate_collapses_not_conflicts(self, real_modules):
        """TIDE and TiDE variants for the same record are exact duplicates after
        serialisation and must collapse to one entry rather than raise conflict."""
        import datetime as dt
        import sys
        import unittest.mock

        _script_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        if _script_dir not in sys.path:
            sys.path.insert(0, _script_dir)
        import importlib

        aw = importlib.import_module("src.api_writer")
        _SPC = aw.SapphirePostprocessingClient

        issue = dt.date(2026, 1, 5)
        rec_tide = self._base_record(model="TiDE")
        rec_TIDE = dict(rec_tide, model_type="TIDE")

        # After serialisation both canonicalise to model_type="TiDE"; they are
        # exact duplicates — second should collapse, not conflict.
        from src.gap_detector import _plan_key

        key_tide = _plan_key(rec_tide)
        key_TIDE = _plan_key(rec_TIDE)
        assert key_tide == key_TIDE, "both must map to the same canonical key"

        auth_keys = {key_tide}

        store = _FakeStore()
        store.upsert(rec_tide)
        original_init = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            original_init(self_client, **kwargs)
            self_client.session.mount("http://", _make_readback_adapter(store))

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            result = aw.reconcile_recovery_batches(
                [
                    {
                        "attempted": True,
                        "transport_status": "acknowledged",
                        "acknowledged_count": 1,
                        "attempted_records": [rec_tide],
                        "evidence_path": "/tmp/ev_tide1.json",
                        "error": None,
                    },
                    {
                        "attempted": True,
                        "transport_status": "acknowledged",
                        "acknowledged_count": 1,
                        "attempted_records": [rec_TIDE],
                        "evidence_path": "/tmp/ev_tide2.json",
                        "error": None,
                    },
                ],
                authorized_keys=auth_keys,
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                deadline=self._make_timing(),
                timeout=5,
                _clock=lambda: 0.0,
            )

        # Must not have raised; the key must be verified (both batches are identical)
        assert key_tide in result["verified_keys"], result
        assert key_tide not in result["unverified_keys"], result
        assert len(store.post_calls) == 0

    def test_date_object_and_iso_string_variants_are_exact_duplicate(self, real_modules):
        """A date object in 'date'/'target' and the same ISO string are the same
        canonical record and must collapse, not conflict."""
        import datetime as dt
        import importlib
        import sys
        import unittest.mock

        _script_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        if _script_dir not in sys.path:
            sys.path.insert(0, _script_dir)
        aw = importlib.import_module("src.api_writer")
        _SPC = aw.SapphirePostprocessingClient

        issue = dt.date(2026, 1, 5)
        rec_str = self._base_record(model="TFT")
        rec_obj = dict(rec_str, date=issue, target=dt.date(2026, 1, 6))

        from src.gap_detector import _plan_key

        key_str = _plan_key(rec_str)
        key_obj = _plan_key(rec_obj)
        assert key_str == key_obj

        store = _FakeStore()
        store.upsert(rec_str)
        original_init = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            original_init(self_client, **kwargs)
            self_client.session.mount("http://", _make_readback_adapter(store))

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            result = aw.reconcile_recovery_batches(
                [
                    {
                        "attempted": True,
                        "transport_status": "acknowledged",
                        "acknowledged_count": 1,
                        "attempted_records": [rec_str],
                        "evidence_path": "/tmp/ev_dateobj1.json",
                        "error": None,
                    },
                    {
                        "attempted": True,
                        "transport_status": "acknowledged",
                        "acknowledged_count": 1,
                        "attempted_records": [rec_obj],
                        "evidence_path": "/tmp/ev_dateobj2.json",
                        "error": None,
                    },
                ],
                authorized_keys={key_str},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                deadline=self._make_timing(),
                timeout=5,
                _clock=lambda: 0.0,
            )

        assert key_str in result["verified_keys"], result
        assert key_str not in result["unverified_keys"], result
        assert len(store.post_calls) == 0

    def test_differing_discharge_variants_conflict(self, real_modules):
        """Two records for the same key with different forecasted_discharge
        must be detected as a conflict and the key become unverified."""
        import datetime as dt
        import importlib
        import sys

        _script_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        if _script_dir not in sys.path:
            sys.path.insert(0, _script_dir)
        aw = importlib.import_module("src.api_writer")

        issue = dt.date(2026, 1, 5)
        rec_a = self._base_record(model="TFT")
        rec_b = dict(rec_a, forecasted_discharge=999.0)

        from src.gap_detector import _plan_key

        key = _plan_key(rec_a)

        result = aw.reconcile_recovery_batches(
            [
                {
                    "attempted": True,
                    "transport_status": "acknowledged",
                    "acknowledged_count": 1,
                    "attempted_records": [rec_a],
                    "evidence_path": "/tmp/ev_conflict_a.json",
                    "error": None,
                },
                {
                    "attempted": True,
                    "transport_status": "acknowledged",
                    "acknowledged_count": 1,
                    "attempted_records": [rec_b],
                    "evidence_path": "/tmp/ev_conflict_b.json",
                    "error": None,
                },
            ],
            authorized_keys={key},
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            base_url="http://fake-sapphire.local",
            deadline=self._make_timing(),
            timeout=5,
            _clock=lambda: 0.0,
        )

        assert key in result["unverified_keys"], result
        assert key not in result["verified_keys"], result

    # ------------------------------------------------------------------
    # Correction 2: Scope-bounds order
    # ------------------------------------------------------------------

    def test_reversed_bounds_returns_structured_unverified(self, real_modules):
        """start_date > end_date must return structured error before any reads."""
        import datetime as dt
        import importlib
        import sys

        _script_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        if _script_dir not in sys.path:
            sys.path.insert(0, _script_dir)
        aw = importlib.import_module("src.api_writer")

        result = aw.reconcile_recovery_batches(
            [],
            authorized_keys=set(),
            start_date=dt.date(2026, 1, 10),
            end_date=dt.date(2026, 1, 5),
            forecast_date=dt.date(2026, 1, 10),
            base_url="http://fake-sapphire.local",
            deadline=self._make_timing(),
            timeout=5,
            _clock=lambda: 0.0,
        )

        assert result["error"] is not None
        assert "start_date" in result["error"] or "scope bounds" in result["error"]
        assert result["reconciliation_status"] == "unverified"

    def test_end_date_after_forecast_date_returns_structured_unverified(self, real_modules):
        """end_date > forecast_date must return structured error before any reads."""
        import datetime as dt
        import importlib
        import sys

        _script_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        if _script_dir not in sys.path:
            sys.path.insert(0, _script_dir)
        aw = importlib.import_module("src.api_writer")

        result = aw.reconcile_recovery_batches(
            [],
            authorized_keys=set(),
            start_date=dt.date(2026, 1, 1),
            end_date=dt.date(2026, 12, 31),
            forecast_date=dt.date(2026, 1, 5),
            base_url="http://fake-sapphire.local",
            deadline=self._make_timing(),
            timeout=5,
            _clock=lambda: 0.0,
        )

        assert result["error"] is not None
        assert result["reconciliation_status"] == "unverified"

    # ------------------------------------------------------------------
    # Correction 3: Exception-handler partition disjointness
    # ------------------------------------------------------------------

    def test_first_scope_verified_later_scope_exception_first_stays_verified(
        self, real_modules, tmp_path
    ):
        """A key verified in scope-1 must stay in verified_keys when scope-2
        raises an exception.  The two partitions must be disjoint."""
        import datetime as dt
        import importlib
        import sys
        import unittest.mock

        _script_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        if _script_dir not in sys.path:
            sys.path.insert(0, _script_dir)
        aw = importlib.import_module("src.api_writer")
        _SPC = aw.SapphirePostprocessingClient

        issue = dt.date(2026, 1, 5)
        rec_tft = self._base_record(code="19999", model="TFT")
        rec_ne = self._base_record(code="19998", model="NE")

        from src.gap_detector import _plan_key

        key_tft = _plan_key(rec_tft)
        key_ne = _plan_key(rec_ne)
        auth_keys = {key_tft, key_ne}

        import json as _json_scope
        from urllib.parse import parse_qs as _parse_qs_scope
        from urllib.parse import urlparse as _urlparse_scope

        import requests as _req_scope
        import requests.adapters as _ra_scope

        class _ScopeFailAdapter(_ra_scope.BaseAdapter):
            def send(self_adapter, prepared_request, **kwargs):
                _params = {
                    k: v[0]
                    for k, v in _parse_qs_scope(_urlparse_scope(prepared_request.url).query).items()
                }
                if _params.get("model") == "NE":
                    raise RuntimeError("simulated read failure on NE scope")
                resp = _req_scope.models.Response()
                resp.status_code = 200
                resp._content = _json_scope.dumps([rec_tft]).encode("utf-8")
                resp.encoding = "utf-8"
                return resp

            def close(self_adapter):
                pass

        original_init = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            original_init(self_client, **kwargs)
            self_client.session.mount("http://", _ScopeFailAdapter())

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            result = aw.reconcile_recovery_batches(
                [
                    {
                        "attempted": True,
                        "transport_status": "acknowledged",
                        "acknowledged_count": 1,
                        "attempted_records": [rec_tft],
                        "evidence_path": "/tmp/ev_scope1.json",
                        "error": None,
                    },
                    {
                        "attempted": True,
                        "transport_status": "acknowledged",
                        "acknowledged_count": 1,
                        "attempted_records": [rec_ne],
                        "evidence_path": "/tmp/ev_scope2.json",
                        "error": None,
                    },
                ],
                authorized_keys=auth_keys,
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                deadline=self._make_timing(),
                timeout=5,
                _clock=lambda: 0.0,
            )

        # Partitions must be disjoint
        verified_set = set(result["verified_keys"])
        unverified_set = set(result["unverified_keys"])
        assert verified_set.isdisjoint(unverified_set), (
            f"partitions overlap: {verified_set & unverified_set}"
        )
        # TFT was verified before the exception — must remain verified
        assert key_tft in verified_set, result
        # NE was unclassified when exception fired — must be in unverified
        assert key_ne in unverified_set, result

    def test_original_transport_uncertain_retained_separately_from_verified(
        self, real_modules, tmp_path
    ):
        """A key whose transport was uncertain but readback verifies it must
        appear in both verified_keys and original_transport_uncertain."""
        import datetime as dt
        import importlib
        import sys
        import unittest.mock

        _script_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        if _script_dir not in sys.path:
            sys.path.insert(0, _script_dir)
        aw = importlib.import_module("src.api_writer")
        _SPC = aw.SapphirePostprocessingClient

        issue = dt.date(2026, 1, 5)
        rec = self._base_record(model="TFT")

        from src.gap_detector import _plan_key

        key = _plan_key(rec)
        auth_keys = {key}

        _store_unc = _FakeStore()
        _store_unc.upsert(rec)
        original_init = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            original_init(self_client, **kwargs)
            self_client.session.mount("http://", _make_readback_adapter(_store_unc))

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            result = aw.reconcile_recovery_batches(
                [
                    {
                        "attempted": True,
                        "transport_status": "uncertain",
                        "acknowledged_count": None,
                        "attempted_records": [rec],
                        "evidence_path": "/tmp/ev_uncertain.json",
                        "error": "timeout",
                    }
                ],
                authorized_keys=auth_keys,
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                deadline=self._make_timing(),
                timeout=5,
                _clock=lambda: 0.0,
            )

        assert key in result["verified_keys"], result
        assert key in result["original_transport_uncertain"], result
        assert key not in result["unverified_keys"], result

    # ------------------------------------------------------------------
    # Missing fake-HTTP readback tests
    # ------------------------------------------------------------------

    def test_malformed_raw_response_key_unverified_zero_post(self, real_modules, tmp_path):
        """A readback that returns a non-list body (malformed) must place
        the key in unverified_keys and must not issue any POST."""
        import datetime as dt
        import importlib
        import sys
        import unittest.mock

        import requests
        from requests.adapters import BaseAdapter

        _script_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        if _script_dir not in sys.path:
            sys.path.insert(0, _script_dir)
        aw = importlib.import_module("src.api_writer")
        _SPC = aw.SapphirePostprocessingClient

        issue = dt.date(2026, 1, 5)
        rec = self._base_record(model="TFT")

        from src.gap_detector import _plan_key

        key = _plan_key(rec)

        post_calls = []

        class _MalformedAdapter(BaseAdapter):
            def send(self, request, **kwargs):
                if request.method == "POST":
                    post_calls.append(request)
                    raise AssertionError("no POST expected")
                resp = requests.models.Response()
                resp.status_code = 200
                resp._content = b'{"not_a_list": true}'
                resp.encoding = "utf-8"
                return resp

            def close(self):
                pass

        original_init = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            original_init(self_client, **kwargs)
            self_client.session.mount("http://", _MalformedAdapter())

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            result = aw.reconcile_recovery_batches(
                [
                    {
                        "attempted": True,
                        "transport_status": "acknowledged",
                        "acknowledged_count": 1,
                        "attempted_records": [rec],
                        "evidence_path": "/tmp/ev_malformed.json",
                        "error": None,
                    }
                ],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                deadline=self._make_timing(),
                timeout=5,
                _clock=lambda: 0.0,
            )

        assert key in result["unverified_keys"], result
        assert key not in result["verified_keys"], result
        assert len(post_calls) == 0

    def test_failure_on_second_pagination_page_no_keys_in_failed_scope_zero_post(
        self, real_modules, tmp_path
    ):
        """When the first page succeeds but the second raises, the key for that
        scope must be in unverified_keys (read incomplete) and zero POSTs issued."""
        import datetime as dt
        import importlib
        import json as _json
        import sys
        import unittest.mock

        import requests
        from requests.adapters import BaseAdapter

        _script_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        if _script_dir not in sys.path:
            sys.path.insert(0, _script_dir)
        aw = importlib.import_module("src.api_writer")
        _SPC = aw.SapphirePostprocessingClient

        issue = dt.date(2026, 1, 5)
        rec = self._base_record(model="TFT")

        from src.gap_detector import _plan_key

        key = _plan_key(rec)

        post_calls = []
        get_call_count = {"n": 0}

        class _PaginationFailAdapter(BaseAdapter):
            def send(self, request, **kwargs):
                if request.method == "POST":
                    post_calls.append(request)
                    raise AssertionError("no POST expected")
                get_call_count["n"] += 1
                if get_call_count["n"] == 1:
                    # First GET: return the record as first page
                    resp = requests.models.Response()
                    resp.status_code = 200
                    resp._content = _json.dumps([rec]).encode("utf-8")
                    resp.encoding = "utf-8"
                    return resp
                # Second GET (second page): simulate server error
                resp = requests.models.Response()
                resp.status_code = 500
                resp._content = b'{"detail": "server error"}'
                resp.encoding = "utf-8"
                return resp

            def close(self):
                pass

        original_init = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            # Use small page_size so pagination is forced
            kwargs["batch_size"] = 1
            original_init(self_client, **kwargs)
            self_client.session.mount("http://", _PaginationFailAdapter())

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            result = aw.reconcile_recovery_batches(
                [
                    {
                        "attempted": True,
                        "transport_status": "acknowledged",
                        "acknowledged_count": 1,
                        "attempted_records": [rec],
                        "evidence_path": "/tmp/ev_page2fail.json",
                        "error": None,
                    }
                ],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                deadline=self._make_timing(),
                timeout=5,
                page_size=1,
                _clock=lambda: 0.0,
            )

        # Key must be unverified (read was not COMPLETE due to page-2 failure)
        assert key in result["unverified_keys"], result
        assert key not in result["verified_keys"], result
        assert len(post_calls) == 0


# ===========================================================================
# Independent-review regression tests (three findings + one test-gap fix)
# ===========================================================================


class TestReviewFindingRegressions:
    """Regression tests for the three independent-review findings.

    Finding 1: reconcile_recovery_batches must validate transport_status consistency
               before skipping or collecting any batch.
    Finding 2: submit_recovery_batch response hook must require 2xx, not merely <400.
    Finding 3: submit_recovery_batch final return must yield attempted_records=[]
               when attempted=False (including constructor failure).
    """

    # ------------------------------------------------------------------
    # Finding 1: transport_status validation — zero reads, structured error
    # ------------------------------------------------------------------

    @pytest.mark.parametrize(
        "attempted_val,raw_status,recs_val,description",
        [
            # Contradictory status/attempted combinations
            (False, "uncertain", True, "false_uncertain_nonempty"),
            (True, "not_attempted", True, "true_not_attempted"),
            (False, "nonexistent_status", False, "false_unknown_status"),
            (True, None, True, "true_missing_status"),
            # attempted=False + not_attempted but invalid attempted_records type
            (False, "not_attempted", None, "false_not_attempted_none_records"),
            (False, "not_attempted", 0, "false_not_attempted_zero_records"),
            (False, "not_attempted", {}, "false_not_attempted_dict_records"),
        ],
    )
    def test_reconcile_contradictory_batch_status_zero_reads(
        self, attempted_val, raw_status, recs_val, description, real_transport_modules
    ):
        """All contradictory/missing/invalid-records cases → structured error, zero reads/POSTs."""
        import datetime
        import unittest.mock

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]
        _SPC = aw.SapphirePostprocessingClient

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)

        if recs_val is True:
            recs = [record]
        elif recs_val is False:
            recs = []
        else:
            recs = recs_val  # None, 0, or {}

        constructor_calls = []

        def _spy_init(self_client, **kwargs):
            constructor_calls.append(1)
            raise AssertionError("constructor must not be called for invalid batch input")

        with unittest.mock.patch.object(_SPC, "__init__", _spy_init):
            result = aw.reconcile_recovery_batches(
                [
                    {
                        "attempted": attempted_val,
                        "transport_status": raw_status,
                        "acknowledged_count": None,
                        "attempted_records": recs,
                        "evidence_path": "/tmp/ev_contra.json",
                        "error": None,
                    }
                ],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                deadline=200.0,
                timeout=5,
                _clock=lambda: 0.0,
            )

        assert result["reconciliation_status"] == "unverified", (
            f"case {description!r}: expected unverified, got {result['reconciliation_status']!r}"
        )
        assert result.get("error") is not None, (
            f"case {description!r}: expected structured error, got None"
        )
        # No reads were attempted: verified and unverified keys lists must both be empty
        assert result["verified_keys"] == [], f"case {description!r}: {result}"
        assert result["unverified_keys"] == [], f"case {description!r}: {result}"
        assert len(constructor_calls) == 0, (
            f"case {description!r}: constructor must not be called, got {len(constructor_calls)} calls"
        )

    # ------------------------------------------------------------------
    # Finding 2: response hook must require 2xx, not merely <400
    # ------------------------------------------------------------------

    @_skip_no_client
    def test_302_without_location_is_uncertain_after_exactly_one_request(
        self, real_transport_modules, tmp_path
    ):
        """A 302 without Location + valid JSON list of correct length → uncertain transport.

        The hook must require 2xx; a 302 with matching-length valid JSON list must
        not be classified as acknowledged. Exactly one POST must be made (no resend).
        """
        import datetime
        import json as _json
        import unittest.mock

        import requests

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]
        _SPC = aw.SapphirePostprocessingClient

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)

        store = _FakeStore()
        post_count = {"n": 0}

        import requests.adapters

        class _Redirect302Adapter(requests.adapters.BaseAdapter):
            def send(self, prepared_request, **kwargs):
                if prepared_request.method == "POST":
                    post_count["n"] += 1
                    body = _json.loads(prepared_request.body)
                    data = body if isinstance(body, list) else body.get("data", [])
                    # Commit to store so readback could verify, but return 302
                    result_rows = [store.upsert(rec) for rec in data]
                    # 302 without Location header, valid JSON list of correct length
                    return _make_fake_response(302, result_rows)
                return _make_fake_response(200, [])

            def close(self):
                pass

        original_init = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            original_init(self_client, **kwargs)
            self_client.session.mount("http://", _Redirect302Adapter())

        evidence_path = str(tmp_path / "ev_302.json")
        deadline = 100.0
        write_cutoff = 60.0

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            result = aw.submit_recovery_batch(
                [record],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                batch_limit=100,
                write_cutoff=write_cutoff,
                deadline=deadline,
                evidence_path=evidence_path,
                timeout=5,
                _clock=lambda: 0.0,
            )

        assert result["attempted"] is True, result
        assert result["transport_status"] == "uncertain", (
            f"302 without Location must yield uncertain transport, got "
            f"{result['transport_status']!r}: {result.get('error')!r}"
        )
        assert post_count["n"] == 1, (
            f"exactly one POST must be made (no resend), got {post_count['n']}"
        )

    # ------------------------------------------------------------------
    # Finding 3: constructor failure → attempted_records=[] (regression)
    # ------------------------------------------------------------------

    @_skip_no_client
    def test_constructor_failure_attempted_records_empty(self, real_transport_modules, tmp_path):
        """Constructor failure → attempted=False, attempted_records=[], evidence retained.

        This is a focused regression test for Finding 3: the final return must
        not expose _validated when attempted is False.
        """
        import datetime
        import os
        import unittest.mock

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]
        _SPC = aw.SapphirePostprocessingClient

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)
        evidence_path = str(tmp_path / "ev_ctor_f3.json")

        deadline = 100.0
        write_cutoff = 60.0

        def _failing_init(self_client, **kwargs):
            raise RuntimeError("constructor failed for Finding 3 regression")

        with unittest.mock.patch.object(_SPC, "__init__", _failing_init):
            result = aw.submit_recovery_batch(
                [record],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                batch_limit=100,
                write_cutoff=write_cutoff,
                deadline=deadline,
                evidence_path=evidence_path,
                timeout=5,
                _clock=lambda: 0.0,
            )

        assert result["attempted"] is False
        assert result["transport_status"] == "not_attempted"
        assert result["attempted_records"] == [], (
            "Finding 3: constructor failure (attempted=False) must return "
            f"attempted_records=[], got {result['attempted_records']!r}"
        )
        assert os.path.exists(evidence_path), "evidence_path must be retained"
        assert result["evidence_path"] == evidence_path

    # ------------------------------------------------------------------
    # Finding A: pre-POST budget gate (clock advances after evidence write)
    # ------------------------------------------------------------------

    @_skip_no_client
    def test_prepost_budget_gate_no_post_empty_attempted_records_evidence_retained(
        self, real_transport_modules, tmp_path
    ):
        """Clock past write_cutoff after evidence write → no POST, attempted_records=[].

        Regression: the pre-POST budget return inside the try block (after evidence
        write, after client construction, before POST) must yield attempted_records=[]
        consistent with the final attempted=False return path, not _validated.
        """
        import datetime
        import os
        import unittest.mock

        aw = real_transport_modules["aw"]
        gd = real_transport_modules["gd"]
        _SPC = aw.SapphirePostprocessingClient

        issue = datetime.date(2026, 1, 5)
        record = _make_valid_wire_record(issue_date=issue)
        key = gd._plan_key(record)
        evidence_path = str(tmp_path / "ev_budget_gate.json")

        # Clock: mutable now=[0.0] so pre-constructor checks see 0.0; the
        # constructor sets now[0]=60.0 (past write_cutoff=50.0), modelling setup
        # consuming budget without relying on a specific call count.
        now = [0.0]

        def _clock():
            return now[0]

        post_calls = []
        ctor_reached = [0]

        original_init = _SPC.__init__

        def _patched_init(self_client, **kwargs):
            original_init(self_client, **kwargs)
            now[0] = 60.0  # advance past write_cutoff after construction
            ctor_reached[0] += 1
            import requests.adapters

            class _SpyAdapter(requests.adapters.BaseAdapter):
                def send(self, prepared_request, **kwargs):
                    post_calls.append(prepared_request)
                    return _make_fake_response(200, [])

                def close(self):
                    pass

            self_client.session.mount("http://", _SpyAdapter())

        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            result = aw.submit_recovery_batch(
                [record],
                authorized_keys={key},
                start_date=issue,
                end_date=issue,
                forecast_date=issue,
                base_url="http://fake-sapphire.local",
                batch_limit=100,
                write_cutoff=50.0,
                deadline=100.0,
                evidence_path=evidence_path,
                timeout=5,
                _clock=_clock,
            )

        assert ctor_reached[0] == 1, (
            f"constructor must be reached exactly once (test is not vacuous), got {ctor_reached[0]}"
        )
        assert result["attempted"] is False, (
            f"pre-POST budget gate must yield attempted=False, got {result['attempted']!r}"
        )
        assert result["transport_status"] == "not_attempted"
        assert result["attempted_records"] == [], (
            "pre-POST budget gate (attempted=False) must return attempted_records=[], "
            f"got {result['attempted_records']!r}"
        )
        assert os.path.exists(evidence_path), "evidence_path must be retained"
        assert result["evidence_path"] == evidence_path
        assert len(post_calls) == 0, (
            f"no POST must be made after budget gate, got {len(post_calls)} POST calls"
        )
