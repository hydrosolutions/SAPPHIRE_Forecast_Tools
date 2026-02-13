"""Tests for postprocessing_operational.py — daily operational entry point."""

import os
import sys
import importlib.util

import pandas as pd
import pytest
from unittest.mock import patch, MagicMock


SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, SCRIPT_DIR)


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
        'model_short': ['LR'],
        'sdivsigma': [0.3],
    })


def _setup_mocks(prediction_mode, mock_data, mock_skill):
    """Set up mocks for the operational module."""
    mock_sl = MagicMock()
    mock_fl = MagicMock()
    mock_pt = MagicMock()
    mock_tl = MagicMock()
    mock_data_reader = MagicMock()
    mock_ensemble_calc = MagicMock()

    mock_sl.load_environment.return_value = None
    mock_sl.read_observed_and_modelled_data_pentade.return_value = (
        mock_data, mock_data
    )
    mock_sl.read_observed_and_modelled_data_decade.return_value = (
        mock_data, mock_data
    )

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

    sys.modules['setup_library'] = mock_sl
    sys.modules['forecast_library'] = mock_fl
    sys.modules['tag_library'] = mock_tl
    sys.modules['src'] = mock_src
    sys.modules['src.postprocessing_tools'] = mock_pt_module
    sys.modules['src.data_reader'] = mock_data_reader
    sys.modules['src.ensemble_calculator'] = mock_ensemble_calc

    return {
        'sl': mock_sl, 'fl': mock_fl, 'pt': mock_pt_module,
        'data_reader': mock_data_reader,
        'ensemble_calc': mock_ensemble_calc,
    }


class TestOperationalWorkflow:
    """Tests for the operational entry point."""

    def test_pentad_mode_no_skill_recalc(self, mock_data, mock_skill):
        """PENTAD mode should NOT call calculate_skill_metrics_pentad."""
        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'PENTAD'}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks('PENTAD', mock_data, mock_skill)

                module, spec = import_operational_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0
                # Key assertion: no skill metric recalculation
                mocks['fl'].calculate_skill_metrics_pentad.assert_not_called()
                # But skill metrics were READ
                mocks['data_reader'].read_skill_metrics.assert_called()

    def test_decad_mode_works(self, mock_data, mock_skill):
        """DECAD mode processes decadal data only."""
        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'DECAD'}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks('DECAD', mock_data, mock_skill)

                module, spec = import_operational_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0
                mocks['sl'].read_observed_and_modelled_data_pentade.assert_not_called()

    def test_both_mode_processes_both(self, mock_data, mock_skill):
        """BOTH mode processes pentad and decad."""
        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'BOTH'}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks('BOTH', mock_data, mock_skill)

                module, spec = import_operational_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0
                mocks['sl'].read_observed_and_modelled_data_pentade.assert_called_once()
                mocks['sl'].read_observed_and_modelled_data_decade.assert_called_once()

    def test_error_accumulation(self, mock_data, mock_skill):
        """Save errors are accumulated and cause exit code 1."""
        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'PENTAD'}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks('PENTAD', mock_data, mock_skill)
                mocks['fl'].save_forecast_data_pentad.return_value = (
                    "Error: write failed"
                )

                module, spec = import_operational_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 1

    def test_empty_skill_metrics_skips_ensemble(self, mock_data):
        """When skill metrics are empty, ensemble creation is skipped."""
        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'PENTAD'}):
            with patch.dict(sys.modules, {}):
                empty_skill = pd.DataFrame()
                mocks = _setup_mocks('PENTAD', mock_data, empty_skill)
                mocks['data_reader'].read_skill_metrics.return_value = (
                    pd.DataFrame()
                )

                module, spec = import_operational_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0
                mocks['ensemble_calc'].create_ensemble_forecasts.assert_not_called()

    def test_invalid_mode_exits_with_error(self, mock_data, mock_skill):
        """Invalid SAPPHIRE_PREDICTION_MODE exits with code 1."""
        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'INVALID'}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_mocks('INVALID', mock_data, mock_skill)

                module, spec = import_operational_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 1
                # No data processing should have occurred
                mocks['sl'].read_observed_and_modelled_data_pentade.assert_not_called()
                mocks['sl'].read_observed_and_modelled_data_decade.assert_not_called()
