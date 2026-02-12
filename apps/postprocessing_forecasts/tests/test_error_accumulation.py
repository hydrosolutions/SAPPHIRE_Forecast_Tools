"""
Tests for the error accumulation fix in postprocessing_forecasts.py

This tests Bug 1+2: Return value masking where the script would exit with
success (code 0) even when earlier save operations failed, because the
`ret` variable was being overwritten by each save operation.

The fix uses an errors list to accumulate all failures and check at the end.
"""

import os
import sys
import importlib
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

# Get the absolute path to the postprocessing_forecasts directory
SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, SCRIPT_DIR)


@pytest.fixture
def mock_data():
    """Create minimal mock data for testing."""
    return pd.DataFrame({
        'code': ['15102'],
        'date': ['2023-05-25'],
        'forecasted_discharge': [100.0]
    })


@pytest.fixture
def mock_skill_metrics():
    """Create minimal mock skill metrics for testing."""
    return pd.DataFrame({
        'code': ['15102'],
        'nse': [0.85],
        'mae': [5.0]
    })


def import_postprocessing_module():
    """Import the postprocessing_forecasts module (not the package)."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "postprocessing_forecasts_module",
        os.path.join(SCRIPT_DIR, "postprocessing_forecasts.py")
    )
    module = importlib.util.module_from_spec(spec)
    return module, spec


class TestErrorAccumulation:
    """Tests for the error accumulation pattern in postprocessing_forecasts."""

    def test_all_saves_succeed_returns_exit_0(self, mock_data, mock_skill_metrics):
        """When all save operations succeed, script should exit with code 0."""
        # We need to patch before the module is fully loaded
        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'BOTH'}):
            with patch.dict(sys.modules, {}):  # Clear cached modules
                # Set up mocks for imported modules
                mock_sl = MagicMock()
                mock_fl = MagicMock()
                mock_pt = MagicMock()

                mock_sl.load_environment.return_value = None
                mock_sl.read_observed_and_modelled_data_pentade.return_value = (mock_data, mock_data)
                mock_sl.read_observed_and_modelled_data_decade.return_value = (mock_data, mock_data)

                mock_fl.calculate_skill_metrics_pentad.return_value = (mock_skill_metrics, mock_data, None)
                mock_fl.calculate_skill_metrics_decade.return_value = (mock_skill_metrics, mock_data, None)

                # All saves succeed (return None)
                mock_fl.save_forecast_data_pentad.return_value = None
                mock_fl.save_pentadal_skill_metrics.return_value = None
                mock_fl.save_forecast_data_decade.return_value = None
                mock_fl.save_decadal_skill_metrics.return_value = None

                # Pre-populate sys.modules with our mocks
                sys.modules['setup_library'] = mock_sl
                sys.modules['forecast_library'] = mock_fl
                sys.modules['tag_library'] = MagicMock()
                mock_pt.TimingStats.return_value.summary.return_value = ([], 0)
                sys.modules['src'] = MagicMock()
                sys.modules['src.postprocessing_tools'] = mock_pt

                # Import and run
                module, spec = import_postprocessing_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_forecasts()

                assert exc_info.value.code == 0

    def test_first_save_fails_returns_exit_1(self, mock_data, mock_skill_metrics):
        """When the first save operation fails, script should exit with code 1."""
        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'BOTH'}):
            with patch.dict(sys.modules, {}):
                mock_sl = MagicMock()
                mock_fl = MagicMock()
                mock_pt = MagicMock()

                mock_sl.load_environment.return_value = None
                mock_sl.read_observed_and_modelled_data_pentade.return_value = (mock_data, mock_data)
                mock_sl.read_observed_and_modelled_data_decade.return_value = (mock_data, mock_data)

                mock_fl.calculate_skill_metrics_pentad.return_value = (mock_skill_metrics, mock_data, None)
                mock_fl.calculate_skill_metrics_decade.return_value = (mock_skill_metrics, mock_data, None)

                # FIRST save fails, rest succeed
                mock_fl.save_forecast_data_pentad.return_value = "Error: Could not write file"
                mock_fl.save_pentadal_skill_metrics.return_value = None
                mock_fl.save_forecast_data_decade.return_value = None
                mock_fl.save_decadal_skill_metrics.return_value = None

                sys.modules['setup_library'] = mock_sl
                sys.modules['forecast_library'] = mock_fl
                sys.modules['tag_library'] = MagicMock()
                mock_pt.TimingStats.return_value.summary.return_value = ([], 0)
                sys.modules['src'] = MagicMock()
                sys.modules['src.postprocessing_tools'] = mock_pt

                module, spec = import_postprocessing_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_forecasts()

                assert exc_info.value.code == 1

    def test_middle_save_fails_returns_exit_1(self, mock_data, mock_skill_metrics):
        """When a middle save operation fails, script should exit with code 1."""
        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'BOTH'}):
            with patch.dict(sys.modules, {}):
                mock_sl = MagicMock()
                mock_fl = MagicMock()
                mock_pt = MagicMock()

                mock_sl.load_environment.return_value = None
                mock_sl.read_observed_and_modelled_data_pentade.return_value = (mock_data, mock_data)
                mock_sl.read_observed_and_modelled_data_decade.return_value = (mock_data, mock_data)

                mock_fl.calculate_skill_metrics_pentad.return_value = (mock_skill_metrics, mock_data, None)
                mock_fl.calculate_skill_metrics_decade.return_value = (mock_skill_metrics, mock_data, None)

                # MIDDLE save (pentad skill metrics) fails, rest succeed
                mock_fl.save_forecast_data_pentad.return_value = None
                mock_fl.save_pentadal_skill_metrics.return_value = "Error: Skill metrics write failed"
                mock_fl.save_forecast_data_decade.return_value = None
                mock_fl.save_decadal_skill_metrics.return_value = None

                sys.modules['setup_library'] = mock_sl
                sys.modules['forecast_library'] = mock_fl
                sys.modules['tag_library'] = MagicMock()
                mock_pt.TimingStats.return_value.summary.return_value = ([], 0)
                sys.modules['src'] = MagicMock()
                sys.modules['src.postprocessing_tools'] = mock_pt

                module, spec = import_postprocessing_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_forecasts()

                assert exc_info.value.code == 1

    def test_last_save_fails_returns_exit_1(self, mock_data, mock_skill_metrics):
        """When the last save operation fails, script should exit with code 1."""
        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'BOTH'}):
            with patch.dict(sys.modules, {}):
                mock_sl = MagicMock()
                mock_fl = MagicMock()
                mock_pt = MagicMock()

                mock_sl.load_environment.return_value = None
                mock_sl.read_observed_and_modelled_data_pentade.return_value = (mock_data, mock_data)
                mock_sl.read_observed_and_modelled_data_decade.return_value = (mock_data, mock_data)

                mock_fl.calculate_skill_metrics_pentad.return_value = (mock_skill_metrics, mock_data, None)
                mock_fl.calculate_skill_metrics_decade.return_value = (mock_skill_metrics, mock_data, None)

                # LAST save fails, rest succeed
                mock_fl.save_forecast_data_pentad.return_value = None
                mock_fl.save_pentadal_skill_metrics.return_value = None
                mock_fl.save_forecast_data_decade.return_value = None
                mock_fl.save_decadal_skill_metrics.return_value = "Error: Decade skill metrics failed"

                sys.modules['setup_library'] = mock_sl
                sys.modules['forecast_library'] = mock_fl
                sys.modules['tag_library'] = MagicMock()
                mock_pt.TimingStats.return_value.summary.return_value = ([], 0)
                sys.modules['src'] = MagicMock()
                sys.modules['src.postprocessing_tools'] = mock_pt

                module, spec = import_postprocessing_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_forecasts()

                assert exc_info.value.code == 1

    def test_multiple_saves_fail_logs_all_errors(self, mock_data, mock_skill_metrics):
        """When multiple saves fail, all errors should be logged and exit code 1."""
        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'BOTH'}):
            with patch.dict(sys.modules, {}):
                mock_sl = MagicMock()
                mock_fl = MagicMock()
                mock_pt = MagicMock()

                mock_sl.load_environment.return_value = None
                mock_sl.read_observed_and_modelled_data_pentade.return_value = (mock_data, mock_data)
                mock_sl.read_observed_and_modelled_data_decade.return_value = (mock_data, mock_data)

                mock_fl.calculate_skill_metrics_pentad.return_value = (mock_skill_metrics, mock_data, None)
                mock_fl.calculate_skill_metrics_decade.return_value = (mock_skill_metrics, mock_data, None)

                # Multiple saves fail
                mock_fl.save_forecast_data_pentad.return_value = "Error 1: Pentad forecast failed"
                mock_fl.save_pentadal_skill_metrics.return_value = "Error 2: Pentad skill metrics failed"
                mock_fl.save_forecast_data_decade.return_value = None  # This one succeeds
                mock_fl.save_decadal_skill_metrics.return_value = "Error 3: Decade skill metrics failed"

                sys.modules['setup_library'] = mock_sl
                sys.modules['forecast_library'] = mock_fl
                sys.modules['tag_library'] = MagicMock()
                mock_pt.TimingStats.return_value.summary.return_value = ([], 0)
                sys.modules['src'] = MagicMock()
                sys.modules['src.postprocessing_tools'] = mock_pt

                module, spec = import_postprocessing_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_forecasts()

                # Exit with error since multiple operations failed
                assert exc_info.value.code == 1


class TestPredictionModes:
    """Tests for different SAPPHIRE_PREDICTION_MODE values."""

    def test_pentad_only_mode_works(self, mock_data, mock_skill_metrics):
        """PENTAD mode should only process pentad data and exit successfully."""
        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'PENTAD'}):
            with patch.dict(sys.modules, {}):
                mock_sl = MagicMock()
                mock_fl = MagicMock()
                mock_pt = MagicMock()

                mock_sl.load_environment.return_value = None
                mock_sl.read_observed_and_modelled_data_pentade.return_value = (mock_data, mock_data)

                mock_fl.calculate_skill_metrics_pentad.return_value = (mock_skill_metrics, mock_data, None)

                # Pentad saves succeed
                mock_fl.save_forecast_data_pentad.return_value = None
                mock_fl.save_pentadal_skill_metrics.return_value = None

                sys.modules['setup_library'] = mock_sl
                sys.modules['forecast_library'] = mock_fl
                sys.modules['tag_library'] = MagicMock()
                mock_pt.TimingStats.return_value.summary.return_value = ([], 0)
                sys.modules['src'] = MagicMock()
                sys.modules['src.postprocessing_tools'] = mock_pt

                module, spec = import_postprocessing_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_forecasts()

                assert exc_info.value.code == 0

                # Decade functions should NOT be called
                mock_sl.read_observed_and_modelled_data_decade.assert_not_called()
                mock_fl.calculate_skill_metrics_decade.assert_not_called()
                mock_fl.save_forecast_data_decade.assert_not_called()
                mock_fl.save_decadal_skill_metrics.assert_not_called()

    def test_decad_only_mode_no_name_error(self, mock_data, mock_skill_metrics):
        """DECAD mode should work without NameError for uninitialized variables."""
        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'DECAD'}):
            with patch.dict(sys.modules, {}):
                mock_sl = MagicMock()
                mock_fl = MagicMock()
                mock_pt = MagicMock()

                mock_sl.load_environment.return_value = None
                mock_sl.read_observed_and_modelled_data_decade.return_value = (mock_data, mock_data)

                mock_fl.calculate_skill_metrics_decade.return_value = (mock_skill_metrics, mock_data, None)

                # Decade saves succeed
                mock_fl.save_forecast_data_decade.return_value = None
                mock_fl.save_decadal_skill_metrics.return_value = None

                sys.modules['setup_library'] = mock_sl
                sys.modules['forecast_library'] = mock_fl
                sys.modules['tag_library'] = MagicMock()
                mock_pt.TimingStats.return_value.summary.return_value = ([], 0)
                sys.modules['src'] = MagicMock()
                sys.modules['src.postprocessing_tools'] = mock_pt

                module, spec = import_postprocessing_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_forecasts()

                # Should exit with 0, not raise NameError
                assert exc_info.value.code == 0

                # Pentad functions should NOT be called
                mock_sl.read_observed_and_modelled_data_pentade.assert_not_called()
                mock_fl.calculate_skill_metrics_pentad.assert_not_called()
                mock_fl.save_forecast_data_pentad.assert_not_called()
                mock_fl.save_pentadal_skill_metrics.assert_not_called()

    def test_both_mode_works(self, mock_data, mock_skill_metrics):
        """BOTH mode should process both pentad and decade data."""
        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'BOTH'}):
            with patch.dict(sys.modules, {}):
                mock_sl = MagicMock()
                mock_fl = MagicMock()
                mock_pt = MagicMock()

                mock_sl.load_environment.return_value = None
                mock_sl.read_observed_and_modelled_data_pentade.return_value = (mock_data, mock_data)
                mock_sl.read_observed_and_modelled_data_decade.return_value = (mock_data, mock_data)

                mock_fl.calculate_skill_metrics_pentad.return_value = (mock_skill_metrics, mock_data, None)
                mock_fl.calculate_skill_metrics_decade.return_value = (mock_skill_metrics, mock_data, None)

                # All saves succeed
                mock_fl.save_forecast_data_pentad.return_value = None
                mock_fl.save_pentadal_skill_metrics.return_value = None
                mock_fl.save_forecast_data_decade.return_value = None
                mock_fl.save_decadal_skill_metrics.return_value = None

                sys.modules['setup_library'] = mock_sl
                sys.modules['forecast_library'] = mock_fl
                sys.modules['tag_library'] = MagicMock()
                mock_pt.TimingStats.return_value.summary.return_value = ([], 0)
                sys.modules['src'] = MagicMock()
                sys.modules['src.postprocessing_tools'] = mock_pt

                module, spec = import_postprocessing_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_forecasts()

                assert exc_info.value.code == 0

                # Both pentad and decade functions SHOULD be called
                mock_sl.read_observed_and_modelled_data_pentade.assert_called_once()
                mock_sl.read_observed_and_modelled_data_decade.assert_called_once()
                mock_fl.save_forecast_data_pentad.assert_called_once()
                mock_fl.save_pentadal_skill_metrics.assert_called_once()
                mock_fl.save_forecast_data_decade.assert_called_once()
                mock_fl.save_decadal_skill_metrics.assert_called_once()

    def test_invalid_mode_exits_with_error(self):
        """Invalid SAPPHIRE_PREDICTION_MODE should exit with code 1."""
        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'INVALID'}):
            with patch.dict(sys.modules, {}):
                mock_sl = MagicMock()
                mock_sl.load_environment.return_value = None

                sys.modules['setup_library'] = mock_sl
                sys.modules['forecast_library'] = MagicMock()
                sys.modules['tag_library'] = MagicMock()
                sys.modules['src'] = MagicMock()
                sys.modules['src.postprocessing_tools'] = MagicMock()

                module, spec = import_postprocessing_module()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_forecasts()

                assert exc_info.value.code == 1
