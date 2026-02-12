"""Tests for recalculate_skill_metrics.py — yearly entry point."""

import os
import sys
import importlib.util

import pandas as pd
import pytest
from unittest.mock import patch, MagicMock


SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
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
    return pd.DataFrame({
        'code': ['10001'],
        'date': pd.to_datetime(['2024-01-05']),
        'forecasted_discharge': [100.0],
    })


@pytest.fixture
def mock_skill():
    return pd.DataFrame({
        'pentad_in_year': [1],
        'code': ['10001'],
        'sdivsigma': [0.3],
    })


def _setup_mocks(mock_data, mock_skill):
    """Set up mocks for the recalculate module."""
    mock_sl = MagicMock()
    mock_fl = MagicMock()

    mock_sl.load_environment.return_value = None
    mock_sl.read_observed_and_modelled_data_pentade.return_value = (
        mock_data, mock_data
    )
    mock_sl.read_observed_and_modelled_data_decade.return_value = (
        mock_data, mock_data
    )

    mock_fl.calculate_skill_metrics_pentad.return_value = (
        mock_skill, mock_data, None
    )
    mock_fl.calculate_skill_metrics_decade.return_value = (
        mock_skill, mock_data, None
    )
    mock_fl.save_forecast_data_pentad.return_value = None
    mock_fl.save_pentadal_skill_metrics.return_value = None
    mock_fl.save_forecast_data_decade.return_value = None
    mock_fl.save_decadal_skill_metrics.return_value = None

    mock_pt_module = MagicMock()
    mock_pt_module.TimingStats.return_value.summary.return_value = ([], 0)

    sys.modules['setup_library'] = mock_sl
    sys.modules['forecast_library'] = mock_fl
    sys.modules['tag_library'] = MagicMock()
    sys.modules['src'] = MagicMock()
    sys.modules['src.postprocessing_tools'] = mock_pt_module

    return {'sl': mock_sl, 'fl': mock_fl}


class TestRecalcWorkflow:
    """Tests for the yearly recalculation entry point."""

    def test_calls_calculate_skill_metrics(self, mock_data, mock_skill):
        """Recalc calls fl.calculate_skill_metrics_pentad (the slow path)."""
        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'PENTAD'}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 0
                mocks['fl'].calculate_skill_metrics_pentad.assert_called_once()
                mocks['fl'].save_pentadal_skill_metrics.assert_called_once()

    def test_saves_skill_metrics(self, mock_data, mock_skill):
        """Recalc saves skill metrics (not just forecasts)."""
        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'BOTH'}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 0
                mocks['fl'].save_pentadal_skill_metrics.assert_called_once()
                mocks['fl'].save_decadal_skill_metrics.assert_called_once()

    def test_both_mode_processes_both(self, mock_data, mock_skill):
        """BOTH mode processes pentad and decad."""
        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'BOTH'}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 0
                mocks['fl'].calculate_skill_metrics_pentad.assert_called_once()
                mocks['fl'].calculate_skill_metrics_decade.assert_called_once()

    def test_save_error_accumulation(self, mock_data, mock_skill):
        """Save errors cause exit code 1."""
        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'PENTAD'}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks(mock_data, mock_skill)
                mocks['fl'].save_pentadal_skill_metrics.return_value = (
                    "Error: write failed"
                )

                module, spec = import_recalc_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 1
