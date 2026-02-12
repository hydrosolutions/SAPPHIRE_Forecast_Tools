"""Tests for postprocessing_maintenance.py — nightly gap-fill entry point."""

import os
import sys
import importlib.util

import pandas as pd
import pytest
from unittest.mock import patch, MagicMock


SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
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
    return pd.DataFrame({
        'code': ['10001'],
        'date': pd.to_datetime(['2024-01-05']),
        'forecasted_discharge': [100.0],
        'model_short': ['LR'],
    })


@pytest.fixture
def mock_skill():
    return pd.DataFrame({
        'pentad_in_year': [1],
        'code': ['10001'],
        'model_short': ['LR'],
        'sdivsigma': [0.3],
    })


def _setup_mocks(mock_data, mock_skill, combined=None, gaps=None):
    """Set up mocks for the maintenance module."""
    mock_sl = MagicMock()
    mock_fl = MagicMock()
    mock_tl = MagicMock()
    mock_gap_detector = MagicMock()
    mock_data_reader = MagicMock()
    mock_ensemble_calc = MagicMock()

    mock_sl.load_environment.return_value = None
    mock_sl.read_observed_and_modelled_data_pentade.return_value = (
        mock_data, mock_data
    )
    mock_sl.read_observed_and_modelled_data_decade.return_value = (
        mock_data, mock_data
    )

    if combined is None:
        combined = pd.DataFrame()
    if gaps is None:
        gaps = pd.DataFrame(columns=['date', 'code'])

    mock_gap_detector.read_combined_forecasts.return_value = combined
    mock_gap_detector.detect_missing_ensembles.return_value = gaps

    mock_data_reader.read_skill_metrics.return_value = mock_skill
    mock_ensemble_calc.create_ensemble_forecasts.return_value = (
        mock_data, mock_skill
    )

    mock_fl.save_forecast_data_pentad.return_value = None
    mock_fl.save_forecast_data_decade.return_value = None

    mock_pt_module = MagicMock()
    mock_pt_module.TimingStats.return_value.summary.return_value = ([], 0)

    mock_src = MagicMock()
    mock_src.postprocessing_tools = mock_pt_module
    mock_src.data_reader = mock_data_reader
    mock_src.ensemble_calculator = mock_ensemble_calc
    mock_src.gap_detector = mock_gap_detector

    sys.modules['setup_library'] = mock_sl
    sys.modules['forecast_library'] = mock_fl
    sys.modules['tag_library'] = mock_tl
    sys.modules['src'] = mock_src
    sys.modules['src.postprocessing_tools'] = mock_pt_module
    sys.modules['src.data_reader'] = mock_data_reader
    sys.modules['src.ensemble_calculator'] = mock_ensemble_calc
    sys.modules['src.gap_detector'] = mock_gap_detector

    return {
        'sl': mock_sl, 'fl': mock_fl,
        'gap_detector': mock_gap_detector,
        'data_reader': mock_data_reader,
        'ensemble_calc': mock_ensemble_calc,
    }


class TestMaintenanceWorkflow:
    """Tests for the maintenance gap-fill entry point."""

    def test_no_gaps_skips_processing(self, mock_data, mock_skill):
        """When no gaps detected, skips data reading and ensemble creation."""
        combined = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-05']),
            'code': ['10001'],
            'model_short': ['EM'],
        })
        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'PENTAD'}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(
                    mock_data, mock_skill,
                    combined=combined,
                    gaps=pd.DataFrame(columns=['date', 'code']),
                )

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                # No gap-fill needed, so no data reading for gap-fill
                mocks['sl'].read_observed_and_modelled_data_pentade.assert_not_called()
                mocks['ensemble_calc'].create_ensemble_forecasts.assert_not_called()

    def test_gaps_trigger_ensemble_creation(self, mock_data, mock_skill):
        """When gaps found, reads data and creates ensembles."""
        combined = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-05'] * 2),
            'code': ['10001'] * 2,
            'model_short': ['LR', 'TFT'],
        })
        gaps = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-05']),
            'code': ['10001'],
        })
        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'PENTAD'}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(
                    mock_data, mock_skill,
                    combined=combined,
                    gaps=gaps,
                )

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                mocks['ensemble_calc'].create_ensemble_forecasts.assert_called_once()

    def test_lookback_env_var(self, mock_data, mock_skill):
        """POSTPROCESSING_GAPFILL_WINDOW_DAYS env var is respected."""
        combined = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-05']),
            'code': ['10001'],
            'model_short': ['LR'],
        })
        with patch.dict(os.environ, {
            'SAPPHIRE_PREDICTION_MODE': 'PENTAD',
            'POSTPROCESSING_GAPFILL_WINDOW_DAYS': '14',
        }):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(
                    mock_data, mock_skill,
                    combined=combined,
                    gaps=pd.DataFrame(columns=['date', 'code']),
                )

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                # Verify lookback was passed to detect_missing_ensembles
                call_args = mocks['gap_detector'].detect_missing_ensembles.call_args
                assert call_args[1].get('lookback_days', call_args[0][1] if len(call_args[0]) > 1 else None) == 14 or \
                    (len(call_args[0]) > 1 and call_args[0][1] == 14)

    def test_empty_combined_forecasts_skips(self, mock_data, mock_skill):
        """When no combined forecasts file exists, skips gracefully."""
        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'PENTAD'}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(
                    mock_data, mock_skill,
                    combined=pd.DataFrame(),
                )

                module, spec = import_maintenance_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                mocks['gap_detector'].detect_missing_ensembles.assert_not_called()
