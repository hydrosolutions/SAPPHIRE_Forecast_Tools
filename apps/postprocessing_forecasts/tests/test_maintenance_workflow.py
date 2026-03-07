"""Tests for postprocessing_maintenance.py — nightly gap-fill entry point."""

import importlib.util
import os
import sys
from unittest.mock import MagicMock, patch

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

                logging.getLogger().addHandler(caplog.handler)
                caplog.set_level(logging.WARNING)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                # Not fatal — exit 0
                assert exc_info.value.code == 0
                # CSV save still called
                mocks["file_writer"].save_forecast_data.assert_called_once()
                assert "direct API write returned False" in caplog.text
