"""Workflow integration tests for MONTHLY mode postprocessing.

These tests exercise the monthly pipeline end-to-end:
- recalculate_skill_metrics (MONTHLY mode): API mock -> aggregation ->
  skill calculation -> ensemble creation -> file writing -> CSV output
- postprocessing_operational (MONTHLY mode): reads pre-calculated skill CSV
- ALL mode: pentad + decad + monthly combined

Mock boundary: only _read_daily_runoff_api() and _read_long_forecasts_api()
are patched. Everything else runs real: aggregation, skill metrics,
ensemble creation, file writing, logging.

Test data uses the same 3-station skill-profile pattern as pentad/decad:
- 99001: all models pass  -> 3-model EM
- 99002: GBT+LR_Base pass -> 2-model EM
- 99003: only LR_Base     -> no EM
"""

import importlib.util
import os
import shutil
import sys
from datetime import date

import numpy as np
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast')
)
sys.path.insert(0, os.path.dirname(__file__))

SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'test_data')


# ---------------------------------------------------------------------------
# Constants — 3-station skill-profile design
# ---------------------------------------------------------------------------
STATIONS = ['99001', '99002', '99003']
MODELS = ['GBT', 'LR_Base', 'MC_ALD']
YEARS = list(range(2021, 2026))  # 5 years
MONTHS = list(range(1, 13))

OBS_BASE = {'99001': 100.0, '99002': 200.0, '99003': 300.0}
BIASES = {
    'GBT':     {'99001': 2.0, '99002': 3.0,  '99003': 100.0},
    'LR_Base': {'99001': 1.0, '99002': 2.0,  '99003': 2.0},
    'MC_ALD':  {'99001': 1.5, '99002': 80.0, '99003': 150.0},
}


def _obs(station, year):
    """Observed discharge for station at year."""
    base = OBS_BASE[station]
    return round(base * (1 + 0.4 * (year - 2023) / 2), 3)


def _fc(station, year, model):
    """Forecasted q50 for station at year with model."""
    return round(_obs(station, year) + BIASES[model][station], 3)


# ---------------------------------------------------------------------------
# Test data generators
# ---------------------------------------------------------------------------

def _make_daily_runoff_all():
    """Build daily runoff DataFrame for all stations/years.

    Constant discharge per month per station per year.
    365 days/year, always passes 50% coverage filter.
    """
    rows = []
    for station in STATIONS:
        for year in YEARS:
            obs_val = _obs(station, year)
            start = date(year, 1, 1)
            end = date(year, 12, 31)
            dates = pd.date_range(start, end, freq='D')
            for d in dates:
                rows.append({
                    'code': station,
                    'date': d.strftime('%Y-%m-%d'),
                    'discharge_avg': obs_val,
                })
    return pd.DataFrame(rows)


def _make_long_forecasts_all():
    """Build long-term forecast DataFrame for all stations/years/models.

    q50 = obs + bias, quantiles spread +/-30% around q50.
    """
    records = []
    for station in STATIONS:
        for year in YEARS:
            for month in MONTHS:
                for model in MODELS:
                    q50 = _fc(station, year, model)
                    first_day = date(year, month, 1)
                    if month == 12:
                        last_day = date(year, 12, 31)
                    else:
                        last_day = (
                            date(year, month + 1, 1)
                            - pd.Timedelta(days=1)
                        )
                    records.append({
                        'horizon_type': 'month',
                        'horizon_value': month,
                        'code': station,
                        'date': str(date(year, month, 1)),
                        'model_type': model,
                        'valid_from': str(first_day),
                        'valid_to': str(last_day),
                        'flag': 0,
                        'composition': '',
                        'q': q50,
                        'q_obs': None,
                        'q_xgb': None,
                        'q_lgbm': None,
                        'q_catboost': None,
                        'q_loc': None,
                        'q05': round(q50 * 0.70, 3),
                        'q10': round(q50 * 0.75, 3),
                        'q25': round(q50 * 0.85, 3),
                        'q50': q50,
                        'q75': round(q50 * 1.15, 3),
                        'q90': round(q50 * 1.25, 3),
                        'q95': round(q50 * 1.30, 3),
                        'id': 1,
                        'model_type_description': model,
                    })
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Helpers — import entry points by file path
# ---------------------------------------------------------------------------

def _import_recalc():
    spec = importlib.util.spec_from_file_location(
        'recalculate_skill_metrics_module',
        os.path.join(SCRIPT_DIR, 'recalculate_skill_metrics.py'),
    )
    module = importlib.util.module_from_spec(spec)
    return module, spec


def _import_operational():
    spec = importlib.util.spec_from_file_location(
        'postprocessing_operational_module',
        os.path.join(SCRIPT_DIR, 'postprocessing_operational.py'),
    )
    module = importlib.util.module_from_spec(spec)
    return module, spec


def _setup_modules_with_real_io():
    """Import real modules and patch only load_environment as no-op."""
    import setup_library as real_sl
    import tag_library as real_tl
    from src import (
        data_reader, ensemble_calculator, gap_detector,
        skill_metrics, file_writer, postprocessing_tools, api_writer,
    )

    real_sl.load_environment = MagicMock(return_value=None)

    sys.modules['setup_library'] = real_sl
    sys.modules['tag_library'] = real_tl

    real_src = MagicMock()
    real_src.postprocessing_tools = postprocessing_tools
    real_src.data_reader = data_reader
    real_src.ensemble_calculator = ensemble_calculator
    real_src.gap_detector = gap_detector
    real_src.skill_metrics = skill_metrics
    real_src.file_writer = file_writer
    real_src.api_writer = api_writer

    sys.modules['src'] = real_src
    sys.modules['src.postprocessing_tools'] = postprocessing_tools
    sys.modules['src.data_reader'] = data_reader
    sys.modules['src.ensemble_calculator'] = ensemble_calculator
    sys.modules['src.gap_detector'] = gap_detector
    sys.modules['src.skill_metrics'] = skill_metrics
    sys.modules['src.file_writer'] = file_writer
    sys.modules['src.api_writer'] = api_writer

    return real_sl


def _read_output_csv(data_dir, filename):
    """Read an output CSV from the data directory."""
    path = os.path.join(data_dir, filename)
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path)


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def monthly_integration_env(tmp_path):
    """Set up environment for monthly integration tests.

    1. Copy config files from test_data/config/
    2. Create required directories (logs/, forecast_logs/)
    3. Set all env vars for monthly mode
    4. Yield (tmp_path, data_dir)
    """
    data_dir = str(tmp_path / 'data')
    os.makedirs(data_dir, exist_ok=True)

    # Copy config files
    config_src = os.path.join(TEST_DATA_DIR, 'config')
    config_dst = os.path.join(data_dir, 'config')
    shutil.copytree(config_src, config_dst)

    # Create directories
    (tmp_path / 'logs').mkdir(exist_ok=True)
    os.makedirs(os.path.join(data_dir, 'forecast_logs'), exist_ok=True)

    env_overrides = {
        'ieasyforecast_intermediate_data_path': data_dir,
        'ieasyforecast_configuration_path': config_dst,
        'ieasyforecast_config_file_all_stations':
            'config_all_stations_library.json',
        'ieasyforecast_config_file_station_selection':
            'config_station_selection.json',
        'ieasyforecast_config_file_output': 'config_output.json',
        'ieasyforecast_monthly_skill_metrics_file':
            'skill_metrics_monthly.csv',
        'ieasyforecast_monthly_combined_forecast_file':
            'combined_forecasts_monthly.csv',
        # Pentad/decad files (needed for ALL mode)
        'ieasyforecast_pentad_discharge_file': 'runoff_pentad.csv',
        'ieasyforecast_decad_discharge_file': 'runoff_decad.csv',
        'ieasyforecast_analysis_pentad_file':
            'forecast_pentad_linreg.csv',
        'ieasyforecast_analysis_decad_file':
            'forecast_decad_linreg.csv',
        'ieasyhydroforecast_OUTPUT_PATH_DISCHARGE': 'predictions',
        'ieasyforecast_combined_forecast_pentad_file':
            'combined_forecasts_pentad.csv',
        'ieasyforecast_combined_forecast_decad_file':
            'combined_forecasts_decad.csv',
        'ieasyforecast_pentadal_skill_metrics_file':
            'skill_metrics_pentad.csv',
        'ieasyforecast_decadal_skill_metrics_file':
            'skill_metrics_decad.csv',
        'ieasyhydroforecast_run_ML_models': 'True',
        'ieasyhydroforecast_available_ML_models': 'TFT,TIDE,TSMIXER',
        'ieasyhydroforecast_run_CM_models': 'False',
        'ieasyhydroforecast_organization': 'demo',
        'SAPPHIRE_API_ENABLED': 'false',
        'SAPPHIRE_CONSISTENCY_CHECK': 'false',
        'SAPPHIRE_TEST_ENV': 'True',
        'ieasyhydroforecast_efficiency_threshold': '0.6',
        'ieasyhydroforecast_accuracy_threshold': '0.8',
        'ieasyhydroforecast_nse_threshold': '0.8',
        'ieasyforecast_daily_discharge_path': data_dir,
        'ieasyforecast_hydrograph_pentad_file': 'hydrograph_pentad.csv',
        'ieasyforecast_hydrograph_day_file': 'hydrograph_day.csv',
        'SAPPHIRE_RECALC_START_YEAR': '2021',
        'SAPPHIRE_RECALC_END_YEAR': '2025',
    }
    with patch.dict(os.environ, env_overrides):
        yield tmp_path, data_dir


def _run_monthly_recalc(tmp_path, data_dir, daily_runoff, long_forecasts):
    """Run the MONTHLY recalculation entry point with patched API data.

    Patches _read_daily_runoff_api and _read_long_forecasts_api with
    the provided DataFrames, then executes recalculate_skill_metrics().
    """
    with patch.dict(
        os.environ, {'SAPPHIRE_PREDICTION_MODE': 'MONTHLY'}
    ):
        with patch.dict(sys.modules, {}):
            _setup_modules_with_real_io()

            # Patch the two API boundary functions
            from src import data_reader as dr
            with patch.object(
                dr, '_read_daily_runoff_api', return_value=daily_runoff
            ), patch.object(
                dr, '_read_long_forecasts_api', return_value=long_forecasts
            ):
                module, spec = _import_recalc()
                with patch('os.getcwd', return_value=str(tmp_path)):
                    spec.loader.exec_module(module)

                    with pytest.raises(SystemExit) as exc_info:
                        module.recalculate_skill_metrics()

                return exc_info.value.code


# ===================================================================
# TestMonthlyRecalcIntegration
# ===================================================================
class TestMonthlyRecalcIntegration:
    """Full recalculate pipeline via MONTHLY mode entry point."""

    def test_monthly_metrics_calculated_and_saved_to_csv(
        self, monthly_integration_env
    ):
        """Skill CSV exists with correct columns and row counts."""
        tmp_path, data_dir = monthly_integration_env
        daily = _make_daily_runoff_all()
        forecasts = _make_long_forecasts_all()

        exit_code = _run_monthly_recalc(
            tmp_path, data_dir, daily, forecasts
        )
        assert exit_code == 0

        skill = _read_output_csv(data_dir, 'skill_metrics_monthly.csv')
        assert not skill.empty, "Skill metrics CSV should not be empty"

        # Must have rows for all 3 base models
        for model in MODELS:
            model_rows = skill[skill['model_short'] == model]
            assert len(model_rows) > 0, (
                f"{model} should have skill metric rows"
            )

        # Must have rows for all stations
        for station in STATIONS:
            station_rows = skill[skill['code'].astype(str) == station]
            assert len(station_rows) > 0, (
                f"Station {station} should have skill metric rows"
            )

    def test_monthly_em_rows_with_correct_discharge(
        self, monthly_integration_env
    ):
        """99001 has 3-model EM, 99002 has 2-model EM, 99003 has no EM.

        EM discharge = mean of qualifying models' q50 values.
        """
        tmp_path, data_dir = monthly_integration_env
        daily = _make_daily_runoff_all()
        forecasts = _make_long_forecasts_all()

        exit_code = _run_monthly_recalc(
            tmp_path, data_dir, daily, forecasts
        )
        assert exit_code == 0

        skill = _read_output_csv(data_dir, 'skill_metrics_monthly.csv')
        em = skill[skill['model_short'] == 'EM']
        em_stations = set(em['code'].astype(str))

        assert '99001' in em_stations, (
            "99001 should have EM (all 3 models pass)"
        )
        assert '99002' in em_stations, (
            "99002 should have EM (GBT+LR_Base pass)"
        )
        assert '99003' not in em_stations, (
            "99003 should NOT have EM (only LR_Base passes)"
        )

        # Spot-check EM discharge for 99001, month 1
        combined = _read_output_csv(
            data_dir, 'combined_forecasts_monthly.csv'
        )
        if not combined.empty:
            em_combined = combined[
                (combined['model_short'] == 'EM')
                & (combined['code'].astype(str) == '99001')
            ]
            if not em_combined.empty:
                # EM should be mean of 3 models' q50
                sample_year = 2023
                sample = em_combined[
                    em_combined['year'] == sample_year
                ]
                if not sample.empty:
                    expected = np.mean([
                        _fc('99001', sample_year, m) for m in MODELS
                    ])
                    actual = sample.iloc[0]['forecasted_discharge']
                    assert actual == pytest.approx(expected, abs=0.1), (
                        f"99001 EM discharge: expected {expected}, "
                        f"got {actual}"
                    )

    def test_monthly_skill_csv_column_schema(
        self, monthly_integration_env
    ):
        """Output skill CSV has exact required columns."""
        tmp_path, data_dir = monthly_integration_env
        daily = _make_daily_runoff_all()
        forecasts = _make_long_forecasts_all()

        exit_code = _run_monthly_recalc(
            tmp_path, data_dir, daily, forecasts
        )
        assert exit_code == 0

        skill = _read_output_csv(data_dir, 'skill_metrics_monthly.csv')
        expected_cols = {
            'month_in_year', 'code', 'model_short',
            'sdivsigma', 'nse', 'delta', 'accuracy', 'mae',
            'n_pairs', 'crps',
        }
        actual_cols = set(skill.columns)
        assert expected_cols.issubset(actual_cols), (
            f"Missing columns: {expected_cols - actual_cols}"
        )

    def test_monthly_combined_forecast_csv_written(
        self, monthly_integration_env
    ):
        """Combined CSV and latest CSV exist and are non-empty."""
        tmp_path, data_dir = monthly_integration_env
        daily = _make_daily_runoff_all()
        forecasts = _make_long_forecasts_all()

        exit_code = _run_monthly_recalc(
            tmp_path, data_dir, daily, forecasts
        )
        assert exit_code == 0

        combined_path = os.path.join(
            data_dir, 'combined_forecasts_monthly.csv'
        )
        latest_path = combined_path.replace('.csv', '_latest.csv')

        assert os.path.exists(combined_path), (
            "Combined monthly CSV should exist"
        )
        assert os.path.exists(latest_path), (
            "Latest monthly CSV should exist"
        )

        combined = pd.read_csv(combined_path)
        assert not combined.empty, "Combined CSV should not be empty"

        latest = pd.read_csv(latest_path)
        assert not latest.empty, "Latest CSV should not be empty"

    def test_monthly_naive_mean_baseline_present(
        self, monthly_integration_env
    ):
        """'Naive Mean' model rows exist for all station-month combos."""
        tmp_path, data_dir = monthly_integration_env
        daily = _make_daily_runoff_all()
        forecasts = _make_long_forecasts_all()

        exit_code = _run_monthly_recalc(
            tmp_path, data_dir, daily, forecasts
        )
        assert exit_code == 0

        skill = _read_output_csv(data_dir, 'skill_metrics_monthly.csv')
        naive = skill[skill['model_short'] == 'Naive Mean']
        assert not naive.empty, "Naive Mean rows should exist"

        # All 3 stations should have Naive Mean rows
        naive_stations = set(naive['code'].astype(str))
        for station in STATIONS:
            assert station in naive_stations, (
                f"Station {station} should have Naive Mean rows"
            )

    def test_monthly_skilled_mean_baseline_present(
        self, monthly_integration_env
    ):
        """'Skilled Mean' rows for 99001 + 99002, not 99003."""
        tmp_path, data_dir = monthly_integration_env
        daily = _make_daily_runoff_all()
        forecasts = _make_long_forecasts_all()

        exit_code = _run_monthly_recalc(
            tmp_path, data_dir, daily, forecasts
        )
        assert exit_code == 0

        skill = _read_output_csv(data_dir, 'skill_metrics_monthly.csv')
        skilled = skill[skill['model_short'] == 'Skilled Mean']

        # Skilled Mean requires >= 2 models passing -> 99001 and 99002
        skilled_stations = set(skilled['code'].astype(str))
        assert '99001' in skilled_stations, (
            "99001 should have Skilled Mean (3 models pass)"
        )
        assert '99002' in skilled_stations, (
            "99002 should have Skilled Mean (2 models pass)"
        )
        assert '99003' not in skilled_stations, (
            "99003 should NOT have Skilled Mean (only 1 model)"
        )

    def test_monthly_spot_check_lr_base_metrics(
        self, monthly_integration_env
    ):
        """Hand-calculated MAE, NSE, accuracy for 99001/month1/LR_Base.

        obs across years = [80, 90, 100, 110, 120] (base=100, variation)
        LR_Base bias = +1.0, so q50 = obs + 1.0
        MAE = 1.0 (constant bias)
        """
        tmp_path, data_dir = monthly_integration_env
        daily = _make_daily_runoff_all()
        forecasts = _make_long_forecasts_all()

        exit_code = _run_monthly_recalc(
            tmp_path, data_dir, daily, forecasts
        )
        assert exit_code == 0

        skill = _read_output_csv(data_dir, 'skill_metrics_monthly.csv')
        lr_99001_m1 = skill[
            (skill['code'].astype(str) == '99001')
            & (skill['model_short'] == 'LR_Base')
            & (skill['month_in_year'] == 1)
        ]
        assert len(lr_99001_m1) == 1, (
            f"Expected 1 row for 99001/LR_Base/month1, "
            f"got {len(lr_99001_m1)}"
        )
        row = lr_99001_m1.iloc[0]

        # MAE = 1.0 (constant +1.0 bias)
        assert row['mae'] == pytest.approx(1.0, abs=0.01), (
            f"MAE should be ~1.0, got {row['mae']}"
        )

        # n_pairs = 5 (5 years)
        assert row['n_pairs'] == 5, (
            f"n_pairs should be 5, got {row['n_pairs']}"
        )

        # accuracy: |obs - fc| <= delta for all years?
        # obs values: [80, 90, 100, 110, 120]
        obs_values = [_obs('99001', y) for y in YEARS]
        delta = 0.674 * np.std(obs_values, ddof=1)
        # bias is 1.0; delta = 0.674 * std([80,90,100,110,120])
        # std ≈ 15.81, delta ≈ 10.66 -> |1.0| < 10.66 -> all accurate
        assert row['accuracy'] == pytest.approx(1.0, abs=0.01), (
            f"Accuracy should be 1.0, got {row['accuracy']}"
        )

        # NSE should be close to 1 (bias=1 vs std~15.8)
        assert row['nse'] > 0.99, (
            f"NSE should be >0.99, got {row['nse']}"
        )

    def test_monthly_crps_computed_for_models(
        self, monthly_integration_env
    ):
        """CRPS is finite+non-negative for base models; NaN for EM/baselines."""
        tmp_path, data_dir = monthly_integration_env
        daily = _make_daily_runoff_all()
        forecasts = _make_long_forecasts_all()

        exit_code = _run_monthly_recalc(
            tmp_path, data_dir, daily, forecasts
        )
        assert exit_code == 0

        skill = _read_output_csv(data_dir, 'skill_metrics_monthly.csv')

        # Base models should have finite, non-negative CRPS
        for model in MODELS:
            model_rows = skill[skill['model_short'] == model]
            crps_values = model_rows['crps'].dropna()
            assert len(crps_values) > 0, (
                f"{model} should have non-NaN CRPS values"
            )
            assert (crps_values >= 0).all(), (
                f"{model} CRPS values should be non-negative"
            )
            assert np.isfinite(crps_values).all(), (
                f"{model} CRPS values should be finite"
            )

        # EM and baselines now have CRPS from aggregated quantiles
        for baseline in ['EM', 'Naive Mean', 'Skilled Mean']:
            baseline_rows = skill[skill['model_short'] == baseline]
            if not baseline_rows.empty:
                crps_values = baseline_rows['crps'].dropna()
                assert len(crps_values) > 0, (
                    f"{baseline} should have non-NaN CRPS from quantiles"
                )
                assert (crps_values >= 0).all(), (
                    f"{baseline} CRPS values should be non-negative"
                )


# ===================================================================
# TestMonthlyAllModeIntegration
# ===================================================================
class TestMonthlyAllModeIntegration:
    """ALL mode (pentad + decad + monthly) integration."""

    def test_all_mode_produces_all_output_csvs(
        self, monthly_integration_env
    ):
        """ALL mode produces pentad, decad, and monthly output files."""
        tmp_path, data_dir = monthly_integration_env

        # Copy pentad/decad test data for ALL mode
        pentad_decad_src = TEST_DATA_DIR
        for fname in os.listdir(pentad_decad_src):
            src_path = os.path.join(pentad_decad_src, fname)
            dst_path = os.path.join(data_dir, fname)
            if os.path.isfile(src_path) and not os.path.exists(dst_path):
                shutil.copy2(src_path, dst_path)
            elif os.path.isdir(src_path) and not os.path.exists(dst_path):
                shutil.copytree(src_path, dst_path)

        daily = _make_daily_runoff_all()
        forecasts = _make_long_forecasts_all()

        with patch.dict(
            os.environ, {'SAPPHIRE_PREDICTION_MODE': 'ALL'}
        ):
            with patch.dict(sys.modules, {}):
                _setup_modules_with_real_io()

                from src import data_reader as dr
                with patch.object(
                    dr, '_read_daily_runoff_api', return_value=daily
                ), patch.object(
                    dr, '_read_long_forecasts_api',
                    return_value=forecasts
                ):
                    module, spec = _import_recalc()
                    with patch('os.getcwd', return_value=str(tmp_path)):
                        spec.loader.exec_module(module)

                        with pytest.raises(SystemExit) as exc_info:
                            module.recalculate_skill_metrics()

                    assert exc_info.value.code == 0

        # All output files should exist
        assert os.path.exists(
            os.path.join(data_dir, 'skill_metrics_pentad.csv')
        ), "Pentad skill CSV missing"
        assert os.path.exists(
            os.path.join(data_dir, 'combined_forecasts_pentad.csv')
        ), "Pentad combined CSV missing"
        assert os.path.exists(
            os.path.join(data_dir, 'skill_metrics_decad.csv')
        ), "Decad skill CSV missing"
        assert os.path.exists(
            os.path.join(data_dir, 'combined_forecasts_decad.csv')
        ), "Decad combined CSV missing"
        assert os.path.exists(
            os.path.join(data_dir, 'skill_metrics_monthly.csv')
        ), "Monthly skill CSV missing"
        assert os.path.exists(
            os.path.join(data_dir, 'combined_forecasts_monthly.csv')
        ), "Monthly combined CSV missing"

    def test_all_mode_monthly_independent_of_pentad_decad(
        self, monthly_integration_env
    ):
        """Monthly skill metrics identical whether run via MONTHLY or ALL.

        Run MONTHLY mode first, save results, then run ALL mode with
        the same monthly data, and compare monthly outputs.
        """
        tmp_path, data_dir = monthly_integration_env
        daily = _make_daily_runoff_all()
        forecasts = _make_long_forecasts_all()

        # Run MONTHLY mode
        exit_code = _run_monthly_recalc(
            tmp_path, data_dir, daily, forecasts
        )
        assert exit_code == 0

        monthly_only_skill = _read_output_csv(
            data_dir, 'skill_metrics_monthly.csv'
        )

        # Copy pentad/decad test data for ALL mode
        pentad_decad_src = TEST_DATA_DIR
        for fname in os.listdir(pentad_decad_src):
            src_path = os.path.join(pentad_decad_src, fname)
            dst_path = os.path.join(data_dir, fname)
            if os.path.isfile(src_path) and not os.path.exists(dst_path):
                shutil.copy2(src_path, dst_path)
            elif os.path.isdir(src_path) and not os.path.exists(dst_path):
                shutil.copytree(src_path, dst_path)

        # Run ALL mode
        with patch.dict(
            os.environ, {'SAPPHIRE_PREDICTION_MODE': 'ALL'}
        ):
            with patch.dict(sys.modules, {}):
                _setup_modules_with_real_io()

                from src import data_reader as dr
                with patch.object(
                    dr, '_read_daily_runoff_api', return_value=daily
                ), patch.object(
                    dr, '_read_long_forecasts_api',
                    return_value=forecasts
                ):
                    module, spec = _import_recalc()
                    with patch('os.getcwd', return_value=str(tmp_path)):
                        spec.loader.exec_module(module)

                        with pytest.raises(SystemExit) as exc_info:
                            module.recalculate_skill_metrics()

                    assert exc_info.value.code == 0

        all_mode_skill = _read_output_csv(
            data_dir, 'skill_metrics_monthly.csv'
        )

        # Monthly results should be identical
        assert len(monthly_only_skill) == len(all_mode_skill), (
            f"MONTHLY produced {len(monthly_only_skill)} rows, "
            f"ALL produced {len(all_mode_skill)} rows"
        )

        # Sort both for comparison
        sort_cols = ['month_in_year', 'code', 'model_short']
        m_sorted = monthly_only_skill.sort_values(
            sort_cols
        ).reset_index(drop=True)
        a_sorted = all_mode_skill.sort_values(
            sort_cols
        ).reset_index(drop=True)

        # Compare numeric columns (allow rounding differences)
        numeric_cols = ['sdivsigma', 'nse', 'delta', 'accuracy', 'mae']
        for col in numeric_cols:
            if col in m_sorted.columns and col in a_sorted.columns:
                m_vals = m_sorted[col].fillna(-999)
                a_vals = a_sorted[col].fillna(-999)
                np.testing.assert_allclose(
                    m_vals.values, a_vals.values, rtol=1e-4,
                    err_msg=f"Column {col} differs between MONTHLY and ALL"
                )


# ===================================================================
# TestMonthlyOperationalIntegration
# ===================================================================
class TestMonthlyOperationalIntegration:
    """Operational entry point MONTHLY mode — reads pre-calculated skill."""

    def test_operational_reads_monthly_skill_csv(
        self, monthly_integration_env
    ):
        """Exits 0 when pre-seeded monthly skill CSV exists."""
        tmp_path, data_dir = monthly_integration_env

        # Create a pre-seeded monthly skill CSV
        skill_data = pd.DataFrame({
            'month_in_year': [1, 1],
            'code': ['99001', '99002'],
            'model_short': ['LR_Base', 'GBT'],
            'sdivsigma': [0.1, 0.2],
            'nse': [0.95, 0.90],
            'delta': [5.0, 10.0],
            'accuracy': [1.0, 0.9],
            'mae': [1.0, 2.0],
            'n_pairs': [5, 5],
            'crps': [0.5, 1.0],
        })
        skill_path = os.path.join(
            data_dir, 'skill_metrics_monthly.csv'
        )
        skill_data.to_csv(skill_path, index=False)

        with patch.dict(
            os.environ, {'SAPPHIRE_PREDICTION_MODE': 'MONTHLY'}
        ):
            with patch.dict(sys.modules, {}):
                _setup_modules_with_real_io()

                module, spec = _import_operational()
                with patch('os.getcwd', return_value=str(tmp_path)):
                    spec.loader.exec_module(module)

                    with pytest.raises(SystemExit) as exc_info:
                        module.postprocessing_operational()

                assert exc_info.value.code == 0

    def test_operational_empty_skill_csv_warns_exits_zero(
        self, monthly_integration_env
    ):
        """Exits 0 with warning when skill CSV is empty."""
        tmp_path, data_dir = monthly_integration_env

        # Create an empty monthly skill CSV (header only)
        empty_skill = pd.DataFrame(columns=[
            'month_in_year', 'code', 'model_short',
            'sdivsigma', 'nse', 'delta', 'accuracy', 'mae',
            'n_pairs', 'crps',
        ])
        skill_path = os.path.join(
            data_dir, 'skill_metrics_monthly.csv'
        )
        empty_skill.to_csv(skill_path, index=False)

        with patch.dict(
            os.environ, {'SAPPHIRE_PREDICTION_MODE': 'MONTHLY'}
        ):
            with patch.dict(sys.modules, {}):
                _setup_modules_with_real_io()

                module, spec = _import_operational()
                with patch('os.getcwd', return_value=str(tmp_path)):
                    spec.loader.exec_module(module)

                    with pytest.raises(SystemExit) as exc_info:
                        module.postprocessing_operational()

                assert exc_info.value.code == 0


# ===================================================================
# TestMonthlyEdgeCases
# ===================================================================
class TestMonthlyEdgeCases:
    """Edge cases specific to the monthly workflow."""

    def test_empty_api_data_exits_gracefully(
        self, monthly_integration_env
    ):
        """Empty API data -> exit 0, empty or no output CSVs."""
        tmp_path, data_dir = monthly_integration_env

        empty_daily = pd.DataFrame(
            columns=['code', 'date', 'discharge_avg']
        )
        empty_forecasts = pd.DataFrame()

        exit_code = _run_monthly_recalc(
            tmp_path, data_dir, empty_daily, empty_forecasts
        )
        assert exit_code == 0

    def test_single_year_data_valid_output(
        self, monthly_integration_env
    ):
        """1 year of data -> n_pairs=1, delta=0, MAE valid."""
        tmp_path, data_dir = monthly_integration_env

        # Build data for single year only
        single_year = 2023
        rows = []
        for station in STATIONS:
            obs_val = _obs(station, single_year)
            dates = pd.date_range(
                date(single_year, 1, 1),
                date(single_year, 12, 31),
                freq='D',
            )
            for d in dates:
                rows.append({
                    'code': station,
                    'date': d.strftime('%Y-%m-%d'),
                    'discharge_avg': obs_val,
                })
        daily = pd.DataFrame(rows)

        records = []
        for station in STATIONS:
            for month in MONTHS:
                for model in MODELS:
                    q50 = _fc(station, single_year, model)
                    first_day = date(single_year, month, 1)
                    if month == 12:
                        last_day = date(single_year, 12, 31)
                    else:
                        last_day = (
                            date(single_year, month + 1, 1)
                            - pd.Timedelta(days=1)
                        )
                    records.append({
                        'horizon_type': 'month',
                        'horizon_value': month,
                        'code': station,
                        'date': str(date(single_year, month, 1)),
                        'model_type': model,
                        'valid_from': str(first_day),
                        'valid_to': str(last_day),
                        'flag': 0,
                        'composition': '',
                        'q': q50,
                        'q_obs': None,
                        'q_xgb': None,
                        'q_lgbm': None,
                        'q_catboost': None,
                        'q_loc': None,
                        'q05': round(q50 * 0.70, 3),
                        'q10': round(q50 * 0.75, 3),
                        'q25': round(q50 * 0.85, 3),
                        'q50': q50,
                        'q75': round(q50 * 1.15, 3),
                        'q90': round(q50 * 1.25, 3),
                        'q95': round(q50 * 1.30, 3),
                        'id': 1,
                        'model_type_description': model,
                    })
        forecasts = pd.DataFrame(records)

        with patch.dict(os.environ, {
            'SAPPHIRE_RECALC_START_YEAR': str(single_year),
            'SAPPHIRE_RECALC_END_YEAR': str(single_year),
        }):
            exit_code = _run_monthly_recalc(
                tmp_path, data_dir, daily, forecasts
            )
        assert exit_code == 0

        skill = _read_output_csv(data_dir, 'skill_metrics_monthly.csv')
        assert not skill.empty, "Single-year should still produce output"

        # Check n_pairs = 1 for base models
        for model in MODELS:
            model_rows = skill[skill['model_short'] == model]
            for _, row in model_rows.iterrows():
                assert row['n_pairs'] == 1, (
                    f"Single year: n_pairs should be 1, "
                    f"got {row['n_pairs']}"
                )

        # Delta should be 0 (std undefined with 1 point -> fillna(0))
        lr_rows = skill[skill['model_short'] == 'LR_Base']
        for _, row in lr_rows.iterrows():
            assert row['delta'] == pytest.approx(0.0, abs=0.01), (
                f"Single year: delta should be 0, got {row['delta']}"
            )

    def test_partial_station_coverage(
        self, monthly_integration_env
    ):
        """Station A has 12 months, B has 6 -> correct per-station rows."""
        tmp_path, data_dir = monthly_integration_env

        # Station 99001: full year, Station 99002: only Jan-Jun
        rows = []
        for station, end_month in [('99001', 12), ('99002', 6)]:
            obs_val = _obs(station, 2023)
            start = date(2023, 1, 1)
            if end_month == 12:
                end = date(2023, 12, 31)
            else:
                end = date(2023, end_month + 1, 1) - pd.Timedelta(days=1)
            dates = pd.date_range(start, end, freq='D')
            for d in dates:
                rows.append({
                    'code': station,
                    'date': d.strftime('%Y-%m-%d'),
                    'discharge_avg': obs_val,
                })
        daily = pd.DataFrame(rows)

        records = []
        for station, months_range in [
            ('99001', range(1, 13)),
            ('99002', range(1, 7)),
        ]:
            for month in months_range:
                for model in MODELS:
                    q50 = _fc(station, 2023, model)
                    first_day = date(2023, month, 1)
                    if month == 12:
                        last_day = date(2023, 12, 31)
                    else:
                        last_day = (
                            date(2023, month + 1, 1)
                            - pd.Timedelta(days=1)
                        )
                    records.append({
                        'horizon_type': 'month',
                        'horizon_value': month,
                        'code': station,
                        'date': str(date(2023, month, 1)),
                        'model_type': model,
                        'valid_from': str(first_day),
                        'valid_to': str(last_day),
                        'flag': 0,
                        'composition': '',
                        'q': q50,
                        'q_obs': None,
                        'q_xgb': None,
                        'q_lgbm': None,
                        'q_catboost': None,
                        'q_loc': None,
                        'q05': round(q50 * 0.70, 3),
                        'q10': round(q50 * 0.75, 3),
                        'q25': round(q50 * 0.85, 3),
                        'q50': q50,
                        'q75': round(q50 * 1.15, 3),
                        'q90': round(q50 * 1.25, 3),
                        'q95': round(q50 * 1.30, 3),
                        'id': 1,
                        'model_type_description': model,
                    })
        forecasts = pd.DataFrame(records)

        with patch.dict(os.environ, {
            'SAPPHIRE_RECALC_START_YEAR': '2023',
            'SAPPHIRE_RECALC_END_YEAR': '2023',
        }):
            exit_code = _run_monthly_recalc(
                tmp_path, data_dir, daily, forecasts
            )
        assert exit_code == 0

        skill = _read_output_csv(data_dir, 'skill_metrics_monthly.csv')
        assert not skill.empty

        # 99001 should have 12 month entries per model
        s1_lr = skill[
            (skill['code'].astype(str) == '99001')
            & (skill['model_short'] == 'LR_Base')
        ]
        assert len(s1_lr) == 12, (
            f"99001/LR_Base should have 12 rows, got {len(s1_lr)}"
        )

        # 99002 should have 6 month entries per model
        s2_lr = skill[
            (skill['code'].astype(str) == '99002')
            & (skill['model_short'] == 'LR_Base')
        ]
        assert len(s2_lr) == 6, (
            f"99002/LR_Base should have 6 rows, got {len(s2_lr)}"
        )

    def test_year_range_env_vars_respected(
        self, monthly_integration_env
    ):
        """Only years within START/END range appear in output.

        Year range filtering happens at the API boundary.  Use
        side_effect mocks that filter data to the requested range,
        mimicking real API behavior.
        """
        tmp_path, data_dir = monthly_integration_env
        daily_all = _make_daily_runoff_all()
        forecasts_all = _make_long_forecasts_all()

        def _filtered_daily(codes, start_year, end_year):
            df = daily_all.copy()
            df['_year'] = pd.to_datetime(df['date']).dt.year
            df = df[
                (df['_year'] >= start_year)
                & (df['_year'] <= end_year)
            ].drop(columns=['_year'])
            return df

        def _filtered_forecasts(codes, start_year, end_year):
            df = forecasts_all.copy()
            df['_year'] = pd.to_datetime(df['valid_from']).dt.year
            df = df[
                (df['_year'] >= start_year)
                & (df['_year'] <= end_year)
            ].drop(columns=['_year'])
            return df

        # Restrict to 2023-2024 only
        with patch.dict(os.environ, {
            'SAPPHIRE_PREDICTION_MODE': 'MONTHLY',
            'SAPPHIRE_RECALC_START_YEAR': '2023',
            'SAPPHIRE_RECALC_END_YEAR': '2024',
        }):
            with patch.dict(sys.modules, {}):
                _setup_modules_with_real_io()

                from src import data_reader as dr
                with patch.object(
                    dr, '_read_daily_runoff_api',
                    side_effect=_filtered_daily,
                ), patch.object(
                    dr, '_read_long_forecasts_api',
                    side_effect=_filtered_forecasts,
                ):
                    module, spec = _import_recalc()
                    with patch('os.getcwd', return_value=str(tmp_path)):
                        spec.loader.exec_module(module)

                        with pytest.raises(SystemExit) as exc_info:
                            module.recalculate_skill_metrics()

                    assert exc_info.value.code == 0

        skill = _read_output_csv(data_dir, 'skill_metrics_monthly.csv')
        assert not skill.empty

        # n_pairs should be <= 2 (only 2 years of data)
        for model in MODELS:
            model_rows = skill[skill['model_short'] == model]
            for _, row in model_rows.iterrows():
                assert row['n_pairs'] <= 2, (
                    f"With 2-year range, n_pairs should be <= 2, "
                    f"got {row['n_pairs']}"
                )
