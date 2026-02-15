"""Wiring integration tests for postprocessing entry points.

These tests call the actual entry point functions but let the *internal*
modules (data_reader, ensemble_calculator, gap_detector) run with real
logic against CSV files written to tmp_path.  Only external boundaries
(setup_library, forecast_library save/load, tag_library) are mocked.

This catches bugs in how the entry-point scripts wire internal modules
together — a class of defect that all-mock workflow tests miss.
"""

import os
import sys
import importlib.util

import numpy as np
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast')
)
sys.path.insert(0, os.path.dirname(__file__))

from test_constants import MODEL_LONG_NAMES

SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _import_operational():
    spec = importlib.util.spec_from_file_location(
        "postprocessing_operational_module",
        os.path.join(SCRIPT_DIR, "postprocessing_operational.py"),
    )
    module = importlib.util.module_from_spec(spec)
    return module, spec


def _import_maintenance():
    spec = importlib.util.spec_from_file_location(
        "postprocessing_maintenance_module",
        os.path.join(SCRIPT_DIR, "postprocessing_maintenance.py"),
    )
    module = importlib.util.module_from_spec(spec)
    return module, spec


def _write_csv(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)


def _make_skill_csv(
    tmp_path, horizon_type='pentad', stations=None, models=None,
):
    """Write a skill metrics CSV where specified models pass thresholds.

    Default: station 15001 has LR + TFT both passing.
    """
    if stations is None:
        stations = ['15001']
    if models is None:
        models = ['LR', 'TFT']

    period_col = (
        'pentad_in_year' if horizon_type == 'pentad' else 'decad_in_year'
    )
    rows = []
    for station in stations:
        for model_short in models:
            rows.append({
                period_col: 1,
                'code': station,
                'model_long': MODEL_LONG_NAMES[model_short],
                'model_short': model_short,
                'sdivsigma': 0.3,
                'nse': 0.9,
                'delta': 5.0,
                'accuracy': 0.95,
                'mae': 2.0,
                'n_pairs': 10,
            })
    df = pd.DataFrame(rows)

    if horizon_type == 'pentad':
        filename = 'skill_pentad.csv'
    else:
        filename = 'skill_decad.csv'
    _write_csv(df, os.path.join(str(tmp_path), filename))
    return df


def _make_modelled_df(
    stations=None, models=None, horizon_type='pentad',
    dates=None, discharge_values=None,
):
    """Build a modelled forecasts DataFrame."""
    if stations is None:
        stations = ['15001']
    if models is None:
        models = ['LR', 'TFT']
    if dates is None:
        dates = [pd.Timestamp('2024-01-05')]
    if discharge_values is None:
        discharge_values = {'LR': 100.0, 'TFT': 110.0}

    period_col = (
        'pentad_in_year' if horizon_type == 'pentad' else 'decad_in_year'
    )
    period_in_month_col = (
        'pentad_in_month' if horizon_type == 'pentad' else 'decad_in_month'
    )
    rows = []
    for date in dates:
        for station in stations:
            for model_short in models:
                rows.append({
                    'code': station,
                    'date': date,
                    period_col: 1,
                    period_in_month_col: '1',
                    'forecasted_discharge': discharge_values.get(
                        model_short, 100.0
                    ),
                    'model_long': MODEL_LONG_NAMES[model_short],
                    'model_short': model_short,
                })
    return pd.DataFrame(rows)


def _make_observed_df(stations=None, dates=None):
    """Build an observed DataFrame."""
    if stations is None:
        stations = ['15001']
    if dates is None:
        dates = [pd.Timestamp('2024-01-05')]
    rows = []
    for date in dates:
        for station in stations:
            rows.append({
                'code': station,
                'date': date,
                'discharge_avg': 95.0,
                'delta': 5.0,
            })
    return pd.DataFrame(rows)


def _make_combined_csv(
    tmp_path, horizon_type='pentad', rows_data=None,
):
    """Write a combined forecasts CSV for gap detection."""
    if rows_data is None:
        rows_data = []
    df = pd.DataFrame(rows_data)
    if horizon_type == 'pentad':
        filename = 'combined_pentad.csv'
    else:
        filename = 'combined_decad.csv'
    _write_csv(df, os.path.join(str(tmp_path), filename))
    return df


def _setup_real_internal_mocks(
    tmp_path, prediction_mode, observed_pentad=None,
    modelled_pentad=None, observed_decad=None, modelled_decad=None,
):
    """Set up sys.modules with real src.* modules and mocked externals.

    Real: data_reader, ensemble_calculator, gap_detector
    Mocked: setup_library, forecast_library, tag_library
    """
    from src import data_reader as real_data_reader
    from src import ensemble_calculator as real_ensemble_calc
    from src import gap_detector as real_gap_detector
    from src import postprocessing_tools as real_pt
    import tag_library as real_tl
    import forecast_library as real_fl

    mock_sl = MagicMock()
    mock_fl = MagicMock()
    # Wire calculate_all_skill_metrics to the real implementation
    mock_fl.calculate_all_skill_metrics = real_fl.calculate_all_skill_metrics

    mock_sl.load_environment.return_value = None

    if observed_pentad is not None:
        mock_sl.read_observed_and_modelled_data_pentade.return_value = (
            observed_pentad, modelled_pentad
        )
    else:
        mock_sl.read_observed_and_modelled_data_pentade.return_value = (
            pd.DataFrame(), pd.DataFrame()
        )
    if observed_decad is not None:
        mock_sl.read_observed_and_modelled_data_decade.return_value = (
            observed_decad, modelled_decad
        )
    else:
        mock_sl.read_observed_and_modelled_data_decade.return_value = (
            pd.DataFrame(), pd.DataFrame()
        )

    mock_fl.save_forecast_data_pentad.return_value = None
    mock_fl.save_forecast_data_decade.return_value = None

    sys.modules['setup_library'] = mock_sl
    sys.modules['forecast_library'] = mock_fl
    sys.modules['tag_library'] = real_tl

    # Use real src submodules
    real_src = MagicMock()
    real_src.postprocessing_tools = real_pt
    real_src.data_reader = real_data_reader
    real_src.ensemble_calculator = real_ensemble_calc
    real_src.gap_detector = real_gap_detector

    sys.modules['src'] = real_src
    sys.modules['src.postprocessing_tools'] = real_pt
    sys.modules['src.data_reader'] = real_data_reader
    sys.modules['src.ensemble_calculator'] = real_ensemble_calc
    sys.modules['src.gap_detector'] = real_gap_detector

    return {'sl': mock_sl, 'fl': mock_fl}


# ---------------------------------------------------------------------------
# Shared env fixture
# ---------------------------------------------------------------------------
@pytest.fixture
def env_setup(tmp_path):
    """Set env vars pointing to tmp_path for CSV reads."""
    overrides = {
        'ieasyforecast_intermediate_data_path': str(tmp_path),
        'ieasyforecast_pentadal_skill_metrics_file': 'skill_pentad.csv',
        'ieasyforecast_decadal_skill_metrics_file': 'skill_decad.csv',
        'ieasyforecast_combined_forecast_pentad_file': 'combined_pentad.csv',
        'ieasyforecast_combined_forecast_decad_file': 'combined_decad.csv',
        'ieasyhydroforecast_efficiency_threshold': '0.6',
        'ieasyhydroforecast_accuracy_threshold': '0.8',
        'ieasyhydroforecast_nse_threshold': '0.8',
        'SAPPHIRE_API_ENABLED': 'false',
        'SAPPHIRE_CONSISTENCY_CHECK': 'false',
        'SAPPHIRE_TEST_ENV': 'True',
    }
    with patch.dict(os.environ, overrides):
        yield tmp_path


# ===================================================================
# TestOperationalWiringIntegration
# ===================================================================
class TestOperationalWiringIntegration:
    """Entry point calls real data_reader + ensemble_calculator."""

    def test_pentad_real_ensemble_created(self, env_setup):
        """Operational PENTAD: real ensemble EM created, EM discharge = mean.

        Skill CSV has LR + TFT passing for station 15001.
        Modelled: LR=100, TFT=110 => EM = 105.
        """
        tmp_path = env_setup
        _make_skill_csv(tmp_path, 'pentad')

        observed = _make_observed_df()
        modelled = _make_modelled_df(
            discharge_values={'LR': 100.0, 'TFT': 110.0}
        )

        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'PENTAD'}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path, 'PENTAD',
                    observed_pentad=observed,
                    modelled_pentad=modelled,
                )

                module, spec = _import_operational()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0

                # Verify save was called
                mocks['fl'].save_forecast_data_pentad.assert_called_once()
                saved_df = (
                    mocks['fl'].save_forecast_data_pentad.call_args[0][0]
                )

                # Real ensemble should have been created
                em_rows = saved_df[saved_df['model_short'] == 'EM']
                assert len(em_rows) == 1, (
                    f"Expected 1 EM row, got {len(em_rows)}"
                )
                em_discharge = em_rows['forecasted_discharge'].iloc[0]
                assert em_discharge == pytest.approx(105.0), (
                    f"Expected EM discharge=105.0, got {em_discharge}"
                )

    def test_pentad_empty_skill_csv_skips_ensemble(self, env_setup):
        """Empty skill CSV => save called but no EM rows."""
        tmp_path = env_setup
        # Write an empty skill CSV (header only)
        empty_skill = pd.DataFrame(columns=[
            'pentad_in_year', 'code', 'model_long', 'model_short',
            'sdivsigma', 'nse', 'delta', 'accuracy', 'mae', 'n_pairs',
        ])
        _write_csv(
            empty_skill,
            os.path.join(str(tmp_path), 'skill_pentad.csv'),
        )

        observed = _make_observed_df()
        modelled = _make_modelled_df()

        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'PENTAD'}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path, 'PENTAD',
                    observed_pentad=observed,
                    modelled_pentad=modelled,
                )

                module, spec = _import_operational()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0

                # Save is called with the original modelled (no EM)
                mocks['fl'].save_forecast_data_pentad.assert_called_once()
                saved_df = (
                    mocks['fl'].save_forecast_data_pentad.call_args[0][0]
                )
                em_rows = saved_df[saved_df['model_short'] == 'EM']
                assert len(em_rows) == 0

    def test_both_mode_creates_pentad_and_decad_ensembles(self, env_setup):
        """BOTH mode: skill CSVs for both horizons => both save with EM."""
        tmp_path = env_setup
        _make_skill_csv(tmp_path, 'pentad')
        _make_skill_csv(tmp_path, 'decad')

        observed = _make_observed_df()
        modelled_pentad = _make_modelled_df(
            horizon_type='pentad',
            discharge_values={'LR': 100.0, 'TFT': 110.0},
        )
        modelled_decad = _make_modelled_df(
            horizon_type='decad',
            discharge_values={'LR': 200.0, 'TFT': 220.0},
        )

        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'BOTH'}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path, 'BOTH',
                    observed_pentad=observed,
                    modelled_pentad=modelled_pentad,
                    observed_decad=observed,
                    modelled_decad=modelled_decad,
                )

                module, spec = _import_operational()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0

                # Both save functions called
                mocks['fl'].save_forecast_data_pentad.assert_called_once()
                mocks['fl'].save_forecast_data_decade.assert_called_once()

                # Pentad EM
                pentad_df = (
                    mocks['fl'].save_forecast_data_pentad.call_args[0][0]
                )
                pentad_em = pentad_df[pentad_df['model_short'] == 'EM']
                assert len(pentad_em) == 1
                assert pentad_em['forecasted_discharge'].iloc[0] == (
                    pytest.approx(105.0)
                )

                # Decad EM
                decad_df = (
                    mocks['fl'].save_forecast_data_decade.call_args[0][0]
                )
                decad_em = decad_df[decad_df['model_short'] == 'EM']
                assert len(decad_em) == 1
                assert decad_em['forecasted_discharge'].iloc[0] == (
                    pytest.approx(210.0)
                )

    def test_save_error_with_real_ensemble(self, env_setup):
        """Real ensemble created but save returns error => exit 1."""
        tmp_path = env_setup
        _make_skill_csv(tmp_path, 'pentad')

        observed = _make_observed_df()
        modelled = _make_modelled_df(
            discharge_values={'LR': 100.0, 'TFT': 110.0}
        )

        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'PENTAD'}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path, 'PENTAD',
                    observed_pentad=observed,
                    modelled_pentad=modelled,
                )
                mocks['fl'].save_forecast_data_pentad.return_value = (
                    "Error: disk full"
                )

                module, spec = _import_operational()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 1


# ===================================================================
# TestMaintenanceWiringIntegration
# ===================================================================
class TestMaintenanceWiringIntegration:
    """Entry point calls real gap_detector + ensemble_calculator."""

    def test_pentad_gap_detected_and_filled(self, env_setup):
        """Gap at Jan 5 => real gap detection finds it, real ensemble fills.

        Combined CSV has LR + TFT at Jan 5 but NO EM.
        Skill CSV has LR + TFT passing.
        Modelled data for gap date: LR=100, TFT=110 => EM=105.
        """
        tmp_path = env_setup
        _make_skill_csv(tmp_path, 'pentad')

        # Combined has individual models but no EM
        _make_combined_csv(tmp_path, 'pentad', rows_data=[
            {
                'date': '2024-01-05', 'code': '15001',
                'model_short': 'LR', 'forecasted_discharge': 100.0,
                'pentad_in_year': 1,
            },
            {
                'date': '2024-01-05', 'code': '15001',
                'model_short': 'TFT', 'forecasted_discharge': 110.0,
                'pentad_in_year': 1,
            },
        ])

        observed = _make_observed_df()
        modelled = _make_modelled_df(
            discharge_values={'LR': 100.0, 'TFT': 110.0}
        )

        with patch.dict(os.environ, {
            'SAPPHIRE_PREDICTION_MODE': 'PENTAD',
            'POSTPROCESSING_GAPFILL_WINDOW_DAYS': '7',
        }):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path, 'PENTAD',
                    observed_pentad=observed,
                    modelled_pentad=modelled,
                )

                module, spec = _import_maintenance()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0

                mocks['fl'].save_forecast_data_pentad.assert_called_once()
                saved_df = (
                    mocks['fl'].save_forecast_data_pentad.call_args[0][0]
                )
                em_rows = saved_df[saved_df['model_short'] == 'EM']
                assert len(em_rows) == 1, (
                    f"Expected 1 EM gap-fill row, got {len(em_rows)}"
                )
                assert em_rows['forecasted_discharge'].iloc[0] == (
                    pytest.approx(105.0)
                )

    def test_no_gaps_skips_data_reading(self, env_setup):
        """Combined has EM for all dates => no gap-fill needed."""
        tmp_path = env_setup
        _make_skill_csv(tmp_path, 'pentad')

        # Combined has EM already
        _make_combined_csv(tmp_path, 'pentad', rows_data=[
            {
                'date': '2024-01-05', 'code': '15001',
                'model_short': 'LR', 'forecasted_discharge': 100.0,
                'pentad_in_year': 1,
            },
            {
                'date': '2024-01-05', 'code': '15001',
                'model_short': 'EM', 'forecasted_discharge': 105.0,
                'pentad_in_year': 1,
            },
        ])

        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'PENTAD'}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path, 'PENTAD',
                )

                module, spec = _import_maintenance()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                # No gaps => sl.read_observed_and_modelled_data_pentade
                # should NOT have been called
                mocks['sl'].read_observed_and_modelled_data_pentade \
                    .assert_not_called()

    def test_gap_dates_no_matching_modelled_data(self, env_setup):
        """Gap detected but modelled data has no matching rows => no save."""
        tmp_path = env_setup
        _make_skill_csv(tmp_path, 'pentad')

        # Gap at Jan 10 (no EM)
        _make_combined_csv(tmp_path, 'pentad', rows_data=[
            {
                'date': '2024-01-10', 'code': '15001',
                'model_short': 'LR', 'forecasted_discharge': 100.0,
                'pentad_in_year': 2,
            },
        ])

        # Modelled data only has Jan 5 — doesn't match Jan 10 gap
        observed = _make_observed_df(dates=[pd.Timestamp('2024-01-05')])
        modelled = _make_modelled_df(dates=[pd.Timestamp('2024-01-05')])

        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'PENTAD'}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path, 'PENTAD',
                    observed_pentad=observed,
                    modelled_pentad=modelled,
                )

                module, spec = _import_maintenance()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                # Data was read but no rows match gap dates
                mocks['sl'].read_observed_and_modelled_data_pentade \
                    .assert_called_once()
                # Early return — no save
                mocks['fl'].save_forecast_data_pentad.assert_not_called()

    def test_both_mode_fills_pentad_and_decad_gaps(self, env_setup):
        """BOTH mode with gaps in both horizons => both filled."""
        tmp_path = env_setup
        _make_skill_csv(tmp_path, 'pentad')
        _make_skill_csv(tmp_path, 'decad')

        # Pentad gap: LR + TFT at Jan 5, no EM
        _make_combined_csv(tmp_path, 'pentad', rows_data=[
            {
                'date': '2024-01-05', 'code': '15001',
                'model_short': 'LR', 'forecasted_discharge': 100.0,
                'pentad_in_year': 1,
            },
            {
                'date': '2024-01-05', 'code': '15001',
                'model_short': 'TFT', 'forecasted_discharge': 110.0,
                'pentad_in_year': 1,
            },
        ])
        # Decad gap: LR + TFT at Jan 10, no EM
        _make_combined_csv(tmp_path, 'decad', rows_data=[
            {
                'date': '2024-01-10', 'code': '15001',
                'model_short': 'LR', 'forecasted_discharge': 200.0,
                'decad_in_year': 1,
            },
            {
                'date': '2024-01-10', 'code': '15001',
                'model_short': 'TFT', 'forecasted_discharge': 220.0,
                'decad_in_year': 1,
            },
        ])

        observed_pentad = _make_observed_df()
        modelled_pentad = _make_modelled_df(
            horizon_type='pentad',
            discharge_values={'LR': 100.0, 'TFT': 110.0},
        )
        observed_decad = _make_observed_df(
            dates=[pd.Timestamp('2024-01-10')]
        )
        modelled_decad = _make_modelled_df(
            horizon_type='decad',
            dates=[pd.Timestamp('2024-01-10')],
            discharge_values={'LR': 200.0, 'TFT': 220.0},
        )

        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'BOTH'}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path, 'BOTH',
                    observed_pentad=observed_pentad,
                    modelled_pentad=modelled_pentad,
                    observed_decad=observed_decad,
                    modelled_decad=modelled_decad,
                )

                module, spec = _import_maintenance()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0

                # Both saved
                mocks['fl'].save_forecast_data_pentad.assert_called_once()
                mocks['fl'].save_forecast_data_decade.assert_called_once()

                # Pentad EM
                pentad_df = (
                    mocks['fl'].save_forecast_data_pentad.call_args[0][0]
                )
                pentad_em = pentad_df[pentad_df['model_short'] == 'EM']
                assert len(pentad_em) == 1
                assert pentad_em['forecasted_discharge'].iloc[0] == (
                    pytest.approx(105.0)
                )

                # Decad EM
                decad_df = (
                    mocks['fl'].save_forecast_data_decade.call_args[0][0]
                )
                decad_em = decad_df[decad_df['model_short'] == 'EM']
                assert len(decad_em) == 1
                assert decad_em['forecasted_discharge'].iloc[0] == (
                    pytest.approx(210.0)
                )


# ===================================================================
# TestExceptionPropagation (Gap 3)
# ===================================================================
class TestExceptionPropagation:
    """Verify internal exceptions are not silently swallowed."""

    def test_maintenance_gap_read_exception_propagates(self, env_setup):
        """RuntimeError in read_combined_forecasts propagates uncaught."""
        tmp_path = env_setup

        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'PENTAD'}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path, 'PENTAD',
                )

                module, spec = _import_maintenance()
                spec.loader.exec_module(module)

                # Patch gap_detector.read_combined_forecasts after exec
                target = 'src.gap_detector.read_combined_forecasts'
                with patch(
                    target,
                    side_effect=RuntimeError("corrupt CSV"),
                ):
                    with pytest.raises(RuntimeError, match="corrupt CSV"):
                        module.postprocessing_maintenance()

    def test_operational_data_reader_exception_propagates(self, env_setup):
        """IOError in data_reader.read_skill_metrics propagates uncaught."""
        tmp_path = env_setup

        observed = _make_observed_df()
        modelled = _make_modelled_df()

        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'PENTAD'}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path, 'PENTAD',
                    observed_pentad=observed,
                    modelled_pentad=modelled,
                )

                module, spec = _import_operational()
                spec.loader.exec_module(module)

                target = 'src.data_reader.read_skill_metrics'
                with patch(
                    target,
                    side_effect=IOError("permission denied"),
                ):
                    with pytest.raises(IOError, match="permission denied"):
                        module.postprocessing_operational()


# ===================================================================
# TestMismatchedInputShapes (Gap 5)
# ===================================================================
class TestMismatchedInputShapes:
    """Verify graceful handling of empty observed/modelled from setup_library."""

    def test_operational_empty_observed_nonempty_modelled(self, env_setup):
        """Empty observed + real modelled => no crash, save called.

        Ensemble calculator should still run; whether EM rows appear
        depends on whether skill metrics can match — but it must not crash.
        """
        tmp_path = env_setup
        _make_skill_csv(tmp_path, 'pentad')

        empty_observed = pd.DataFrame(
            columns=['code', 'date', 'discharge_avg', 'delta']
        )
        modelled = _make_modelled_df()

        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'PENTAD'}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path, 'PENTAD',
                    observed_pentad=empty_observed,
                    modelled_pentad=modelled,
                )

                module, spec = _import_operational()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0
                mocks['fl'].save_forecast_data_pentad.assert_called_once()

    def test_operational_nonempty_observed_empty_modelled(self, env_setup):
        """Real observed + empty modelled => no crash, save called."""
        tmp_path = env_setup
        _make_skill_csv(tmp_path, 'pentad')

        observed = _make_observed_df()
        empty_modelled = pd.DataFrame(columns=[
            'code', 'date', 'pentad_in_year', 'pentad_in_month',
            'forecasted_discharge', 'model_long', 'model_short',
        ])

        with patch.dict(os.environ, {'SAPPHIRE_PREDICTION_MODE': 'PENTAD'}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path, 'PENTAD',
                    observed_pentad=observed,
                    modelled_pentad=empty_modelled,
                )

                module, spec = _import_operational()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0
                mocks['fl'].save_forecast_data_pentad.assert_called_once()
                saved_df = (
                    mocks['fl'].save_forecast_data_pentad.call_args[0][0]
                )
                # No modelled data => no EM rows
                assert saved_df.empty or (
                    'model_short' not in saved_df.columns
                    or saved_df[saved_df['model_short'] == 'EM'].empty
                )
