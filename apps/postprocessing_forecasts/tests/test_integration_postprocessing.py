"""Integration tests for postprocessing_forecasts data routing.

Validates that data flows correctly through the pipeline:
skill CSV read -> threshold filter -> ensemble create -> CSV + API write.

These tests use real logic for everything inside the boundary (CSV I/O,
filtering, ensemble creation, skill metric calculation) and only mock
the external API client.
"""

import os
import sys
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast')
)
sys.path.insert(0, os.path.dirname(__file__))

from src import data_reader
from src import gap_detector
from src import ensemble_calculator
import forecast_library as fl
import tag_library as tl

from test_constants import MODEL_LONG_NAMES


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
STATIONS = ['15001', '15002']
MODELS_LONG = {
    k: MODEL_LONG_NAMES[k] for k in ('LR', 'TFT', 'TiDE')
}
PENTAD_DATES = pd.to_datetime(['2026-01-05', '2026-01-10'])
PENTAD_IN_YEAR = [1, 2]
PENTAD_IN_MONTH = ['1', '2']


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _write_csv(df, path):
    """Write DataFrame to CSV, ensuring parent directory exists."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)


def _make_ensemble(forecasts, skill_stats, observed):
    """Call create_ensemble_forecasts with pentad defaults."""
    return ensemble_calculator.create_ensemble_forecasts(
        forecasts=forecasts,
        skill_stats=skill_stats,
        observed=observed,
        period_col='pentad_in_year',
        period_in_month_col='pentad_in_month',
        get_period_in_month_func=tl.get_pentad,
        calculate_all_metrics_func=fl.calculate_all_skill_metrics,
    )


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def env_setup(tmp_path):
    """Set env vars pointing to tmp_path, yield, restore."""
    overrides = {
        'ieasyforecast_intermediate_data_path': str(tmp_path),
        'ieasyforecast_combined_forecast_pentad_file': 'combined_pentad.csv',
        'ieasyforecast_combined_forecast_decad_file': 'combined_decad.csv',
        'ieasyforecast_pentadal_skill_metrics_file': 'skill_pentad.csv',
        'ieasyforecast_decadal_skill_metrics_file': 'skill_decad.csv',
        'ieasyhydroforecast_efficiency_threshold': '0.6',
        'ieasyhydroforecast_accuracy_threshold': '0.8',
        'ieasyhydroforecast_nse_threshold': '0.8',
        'SAPPHIRE_API_ENABLED': 'false',
        'SAPPHIRE_CONSISTENCY_CHECK': 'false',
        'SAPPHIRE_TEST_ENV': 'True',
    }
    with patch.dict(os.environ, overrides):
        yield tmp_path


@pytest.fixture
def pentad_skill_csv(env_setup):
    """Write skill metrics CSV and return the DataFrame.

    Station 15001: LR + TFT pass thresholds -> eligible for EM.
    Station 15002: Only LR passes (TFT + TiDE fail) -> no EM.
    """
    tmp_path = env_setup
    rows = []
    for station in STATIONS:
        for pentad in PENTAD_IN_YEAR:
            # LR passes at both stations
            rows.append({
                'pentad_in_year': pentad, 'code': station,
                'model_long': MODELS_LONG['LR'], 'model_short': 'LR',
                'sdivsigma': 0.3, 'nse': 0.95, 'delta': 5.0,
                'accuracy': 0.95, 'mae': 2.0, 'n_pairs': 10,
            })
            # TFT: passes at 15001, fails at 15002
            tft_skilled = station == '15001'
            rows.append({
                'pentad_in_year': pentad, 'code': station,
                'model_long': MODELS_LONG['TFT'], 'model_short': 'TFT',
                'sdivsigma': 0.4 if tft_skilled else 0.7,
                'nse': 0.9 if tft_skilled else 0.7,
                'delta': 5.0,
                'accuracy': 0.88 if tft_skilled else 0.7,
                'mae': 3.0 if tft_skilled else 6.0,
                'n_pairs': 10,
            })
            # TiDE fails at both stations
            rows.append({
                'pentad_in_year': pentad, 'code': station,
                'model_long': MODELS_LONG['TiDE'], 'model_short': 'TiDE',
                'sdivsigma': 0.9, 'nse': 0.5, 'delta': 5.0,
                'accuracy': 0.6, 'mae': 8.0, 'n_pairs': 10,
            })

    df = pd.DataFrame(rows)
    filepath = os.path.join(str(tmp_path), 'skill_pentad.csv')
    _write_csv(df, filepath)
    return df


@pytest.fixture
def pentad_forecasts():
    """3 models x 2 dates x 2 stations = 12 forecast rows."""
    discharges = {
        ('15001', 0): {'LR': 100.0, 'TFT': 110.0, 'TiDE': 90.0},
        ('15001', 1): {'LR': 120.0, 'TFT': 130.0, 'TiDE': 95.0},
        ('15002', 0): {'LR': 200.0, 'TFT': 210.0, 'TiDE': 180.0},
        ('15002', 1): {'LR': 220.0, 'TFT': 230.0, 'TiDE': 185.0},
    }
    rows = []
    for station in STATIONS:
        for i, (date, pentad, pim) in enumerate(
            zip(PENTAD_DATES, PENTAD_IN_YEAR, PENTAD_IN_MONTH)
        ):
            for model_short, model_long in MODELS_LONG.items():
                rows.append({
                    'code': station,
                    'date': date,
                    'pentad_in_year': pentad,
                    'pentad_in_month': pim,
                    'forecasted_discharge': discharges[
                        (station, i)
                    ][model_short],
                    'model_long': model_long,
                    'model_short': model_short,
                })
    return pd.DataFrame(rows)


@pytest.fixture
def pentad_observed():
    """Observed discharge: 2 dates x 2 stations = 4 rows."""
    obs_vals = {
        ('15001', 0): 105.0, ('15001', 1): 125.0,
        ('15002', 0): 205.0, ('15002', 1): 225.0,
    }
    rows = []
    for station in STATIONS:
        for i, date in enumerate(PENTAD_DATES):
            rows.append({
                'code': station, 'date': date,
                'discharge_avg': obs_vals[(station, i)], 'delta': 5.0,
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# TestOperationalDataRouting
# ---------------------------------------------------------------------------
class TestOperationalDataRouting:
    """Validates the operational pipeline:
    skill CSV read -> threshold filter -> ensemble create -> CSV + API write.
    """

    def test_skill_metrics_read_from_csv(
        self, pentad_skill_csv, env_setup
    ):
        """read_skill_metrics reads CSV with correct columns and row count."""
        df = data_reader.read_skill_metrics('pentad')
        assert not df.empty
        expected_cols = {
            'pentad_in_year', 'code', 'model_long', 'model_short',
            'sdivsigma', 'nse', 'delta', 'accuracy', 'mae', 'n_pairs',
        }
        assert expected_cols.issubset(set(df.columns))
        assert len(df) == len(pentad_skill_csv)

    def test_ensemble_created_and_written_to_csv(
        self, pentad_skill_csv, pentad_forecasts, pentad_observed,
        env_setup,
    ):
        """Full pipeline: read skills -> ensemble -> save to CSV.

        Verifies: CSV written at tmp_path, EM rows present,
        EM discharge = mean(LR, TFT) for station 15001,
        composition string correct, _latest.csv also exists.
        """
        skill_metrics = data_reader.read_skill_metrics('pentad')
        joint, _ = _make_ensemble(
            pentad_forecasts, skill_metrics, pentad_observed
        )

        # EM should exist for station 15001
        em_rows = joint[
            (joint['model_short'] == 'EM') & (joint['code'] == '15001')
        ]
        assert not em_rows.empty, "Expected EM rows for station 15001"

        # Verify EM discharge = mean(LR, TFT)
        for _, row in em_rows.iterrows():
            date = row['date']
            lr = pentad_forecasts[
                (pentad_forecasts['date'] == date)
                & (pentad_forecasts['code'] == '15001')
                & (pentad_forecasts['model_short'] == 'LR')
            ]['forecasted_discharge'].iloc[0]
            tft = pentad_forecasts[
                (pentad_forecasts['date'] == date)
                & (pentad_forecasts['code'] == '15001')
                & (pentad_forecasts['model_short'] == 'TFT')
            ]['forecasted_discharge'].iloc[0]
            assert abs(row['forecasted_discharge'] - (lr + tft) / 2) < 0.01

        # Composition string
        comp = em_rows.iloc[0]['model_long']
        assert comp.startswith('Ens. Mean with ')
        assert comp.endswith(' (EM)')
        assert 'LR' in comp and 'TFT' in comp

        # Save to CSV (API disabled via env_setup)
        fl.save_forecast_data_pentad(joint)

        csv_path = os.path.join(str(env_setup), 'combined_pentad.csv')
        latest_path = csv_path.replace('.csv', '_latest.csv')
        assert os.path.exists(csv_path)
        assert os.path.exists(latest_path)

        # Verify written CSV contains EM rows
        saved = pd.read_csv(csv_path)
        assert 'EM' in saved['model_short'].values

    def test_api_receives_correct_forecast_records(
        self, pentad_skill_csv, pentad_forecasts, pentad_observed,
        env_setup,
    ):
        """Patched API client receives records with correct fields."""
        skill_metrics = data_reader.read_skill_metrics('pentad')
        joint, _ = _make_ensemble(
            pentad_forecasts, skill_metrics, pentad_observed
        )

        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_forecasts.return_value = len(joint)

        with patch.object(fl, 'SAPPHIRE_API_AVAILABLE', True), \
             patch.object(
                 fl, '_get_postprocessing_client',
                 return_value=mock_client,
             ), \
             patch.dict(os.environ, {'SAPPHIRE_API_ENABLED': 'true'}):
            fl.save_forecast_data_pentad(joint)

        mock_client.write_forecasts.assert_called_once()
        records = mock_client.write_forecasts.call_args[0][0]

        # 2 stations x 2 dates x 3 models = 12, + 2 EM (station 15001 only)
        assert len(records) == 14

        # EM model type present
        model_types = {r['model_type'] for r in records}
        assert 'EM' in model_types

        # All records have required fields
        for r in records:
            assert 'code' in r
            assert 'model_type' in r
            assert 'date' in r
            assert r['forecasted_discharge'] is not None

        # EM records: exactly 2 (one per date, station 15001 only)
        em_records = [r for r in records if r['model_type'] == 'EM']
        assert len(em_records) == 2
        for r in em_records:
            assert r['code'] == '15001'
            assert r.get('composition') is not None
            assert 'LR' in r['composition']

        # Spot-check EM discharge for 2026-01-05: mean(LR=100, TFT=110) = 105
        em_jan05 = [
            r for r in em_records
            if '2026-01-05' in str(r['date'])
        ]
        assert len(em_jan05) == 1
        assert abs(em_jan05[0]['forecasted_discharge'] - 105.0) < 0.01

    def test_csv_still_written_when_api_disabled(
        self, pentad_skill_csv, pentad_forecasts, pentad_observed,
        env_setup,
    ):
        """CSV written when SAPPHIRE_API_ENABLED=false; no API call."""
        skill_metrics = data_reader.read_skill_metrics('pentad')
        joint, _ = _make_ensemble(
            pentad_forecasts, skill_metrics, pentad_observed
        )

        mock_client = MagicMock()
        with patch.object(
            fl, '_get_postprocessing_client', return_value=mock_client
        ):
            fl.save_forecast_data_pentad(joint)

        mock_client.write_forecasts.assert_not_called()

        csv_path = os.path.join(str(env_setup), 'combined_pentad.csv')
        assert os.path.exists(csv_path)

    def test_two_stations_independent_filtering(
        self, pentad_skill_csv, pentad_forecasts, pentad_observed,
        env_setup,
    ):
        """Station 15001 gets EM (LR+TFT), 15002 doesn't (LR only)."""
        skill_metrics = data_reader.read_skill_metrics('pentad')
        joint, _ = _make_ensemble(
            pentad_forecasts, skill_metrics, pentad_observed
        )

        em_rows = joint[joint['model_short'] == 'EM']
        em_stations = set(em_rows['code'].unique())
        assert em_stations == {'15001'}, (
            f"Only station 15001 should get EM, got {em_stations}"
        )
        # Exactly 2 EM rows: 1 per date for station 15001
        assert len(em_rows) == 2, (
            f"Expected 2 EM rows (2 dates x 1 station), got {len(em_rows)}"
        )
        # Spot-check EM discharge for 2026-01-05: mean(LR=100, TFT=110) = 105
        em_jan05 = em_rows[
            em_rows['date'] == pd.Timestamp('2026-01-05')
        ]
        assert len(em_jan05) == 1
        assert abs(
            em_jan05.iloc[0]['forecasted_discharge'] - 105.0
        ) < 0.01

    def test_no_ensemble_when_all_models_fail_threshold(
        self, pentad_forecasts, pentad_observed, env_setup
    ):
        """No EM rows when all models fail all thresholds."""
        all_fail = pd.DataFrame({
            'pentad_in_year': [1, 1, 1],
            'code': ['15001', '15001', '15001'],
            'model_long': list(MODELS_LONG.values()),
            'model_short': ['LR', 'TFT', 'TiDE'],
            'sdivsigma': [0.9, 0.9, 0.9],
            'nse': [0.5, 0.5, 0.5],
            'delta': [5.0, 5.0, 5.0],
            'accuracy': [0.6, 0.6, 0.6],
            'mae': [8.0, 8.0, 8.0],
            'n_pairs': [10, 10, 10],
        })

        joint, _ = _make_ensemble(
            pentad_forecasts, all_fail, pentad_observed
        )
        assert 'EM' not in joint['model_short'].values
        # Original model rows preserved
        assert len(joint) == len(pentad_forecasts)

    def test_three_models_all_pass_ensemble(
        self, pentad_forecasts, pentad_observed, env_setup
    ):
        """LR + TFT + TiDE all pass -> EM = mean of all three."""
        all_pass = pd.DataFrame({
            'pentad_in_year': [1, 1, 1, 2, 2, 2],
            'code': ['15001'] * 6,
            'model_long': list(MODELS_LONG.values()) * 2,
            'model_short': ['LR', 'TFT', 'TiDE'] * 2,
            'sdivsigma': [0.3, 0.4, 0.3, 0.3, 0.4, 0.3],
            'nse': [0.95, 0.9, 0.95, 0.95, 0.9, 0.95],
            'delta': [5.0] * 6,
            'accuracy': [0.95, 0.88, 0.95, 0.95, 0.88, 0.95],
            'mae': [2.0, 3.0, 2.0, 2.0, 3.0, 2.0],
            'n_pairs': [10] * 6,
        })

        joint, _ = _make_ensemble(
            pentad_forecasts, all_pass, pentad_observed
        )
        em_rows = joint[
            (joint['model_short'] == 'EM') & (joint['code'] == '15001')
        ]
        assert len(em_rows) == 2, f"Expected 2 EM rows, got {len(em_rows)}"

        # EM discharge = mean of all three models
        em_sorted = em_rows.sort_values('date').reset_index(drop=True)
        # Date 2026-01-05: mean(LR=100, TFT=110, TiDE=90) = 100.0
        assert abs(em_sorted.iloc[0]['forecasted_discharge'] - 100.0) < 0.01
        # Date 2026-01-10: mean(LR=120, TFT=130, TiDE=95) = 115.0
        assert abs(em_sorted.iloc[1]['forecasted_discharge'] - 115.0) < 0.01

        # Composition includes all three
        comp = em_rows.iloc[0]['model_long']
        assert 'LR' in comp
        assert 'TFT' in comp
        assert 'TiDE' in comp

    def test_composition_survives_csv_roundtrip(
        self, pentad_skill_csv, pentad_forecasts, pentad_observed,
        env_setup,
    ):
        """Save EM to CSV, read back, verify composition string preserved."""
        skill_metrics = data_reader.read_skill_metrics('pentad')
        joint, _ = _make_ensemble(
            pentad_forecasts, skill_metrics, pentad_observed
        )
        fl.save_forecast_data_pentad(joint)

        csv_path = os.path.join(str(env_setup), 'combined_pentad.csv')
        saved = pd.read_csv(csv_path)
        em_saved = saved[saved['model_short'] == 'EM']
        assert not em_saved.empty
        comp = em_saved.iloc[0]['model_long']
        assert comp.startswith('Ens. Mean with ')
        assert comp.endswith(' (EM)')

    def test_composition_in_api_records(
        self, pentad_skill_csv, pentad_forecasts, pentad_observed,
        env_setup,
    ):
        """Mock API, verify EM records have non-null composition."""
        skill_metrics = data_reader.read_skill_metrics('pentad')
        joint, _ = _make_ensemble(
            pentad_forecasts, skill_metrics, pentad_observed
        )

        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_forecasts.return_value = len(joint)

        with patch.object(fl, 'SAPPHIRE_API_AVAILABLE', True), \
             patch.object(
                 fl, '_get_postprocessing_client',
                 return_value=mock_client,
             ), \
             patch.dict(os.environ, {'SAPPHIRE_API_ENABLED': 'true'}):
            fl.save_forecast_data_pentad(joint)

        records = mock_client.write_forecasts.call_args[0][0]
        em_records = [r for r in records if r['model_type'] == 'EM']
        assert len(em_records) == 2
        for r in em_records:
            assert r['composition'] == 'LR, TFT'

    def test_api_records_contain_target_date(
        self, pentad_skill_csv, pentad_forecasts, pentad_observed,
        env_setup,
    ):
        """All API records have 'target' == 'date', both dates present."""
        skill_metrics = data_reader.read_skill_metrics('pentad')
        joint, _ = _make_ensemble(
            pentad_forecasts, skill_metrics, pentad_observed
        )

        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_forecasts.return_value = len(joint)

        with patch.object(fl, 'SAPPHIRE_API_AVAILABLE', True), \
             patch.object(
                 fl, '_get_postprocessing_client',
                 return_value=mock_client,
             ), \
             patch.dict(os.environ, {'SAPPHIRE_API_ENABLED': 'true'}):
            fl.save_forecast_data_pentad(joint)

        records = mock_client.write_forecasts.call_args[0][0]
        for r in records:
            assert 'target' in r
            assert r['target'] == r['date']

        # Both input dates appear in API records
        api_dates = {str(r['date'])[:10] for r in records}
        assert '2026-01-05' in api_dates, (
            f"2026-01-05 should appear in API records, got {api_dates}"
        )
        assert '2026-01-10' in api_dates, (
            f"2026-01-10 should appear in API records, got {api_dates}"
        )

    def test_nan_discharge_dropped_before_averaging(
        self, pentad_skill_csv, pentad_observed, env_setup,
    ):
        """TiDE has NaN discharge -> EM = mean(LR, TFT) only."""
        # Build skill stats where all 3 pass for station 15001
        rows = []
        for pentad in PENTAD_IN_YEAR:
            for ms, ml in MODELS_LONG.items():
                rows.append({
                    'pentad_in_year': pentad, 'code': '15001',
                    'model_long': ml, 'model_short': ms,
                    'sdivsigma': 0.3, 'nse': 0.95, 'delta': 5.0,
                    'accuracy': 0.95, 'mae': 2.0, 'n_pairs': 10,
                })
        skill_all_pass = pd.DataFrame(rows)

        # Forecasts with NaN discharge for TiDE
        frows = []
        for i, (date, pentad, pim) in enumerate(
            zip(PENTAD_DATES, PENTAD_IN_YEAR, PENTAD_IN_MONTH)
        ):
            frows.append({
                'code': '15001', 'date': date,
                'pentad_in_year': pentad, 'pentad_in_month': pim,
                'forecasted_discharge': 100.0 + i * 20,
                'model_long': MODELS_LONG['LR'], 'model_short': 'LR',
            })
            frows.append({
                'code': '15001', 'date': date,
                'pentad_in_year': pentad, 'pentad_in_month': pim,
                'forecasted_discharge': 110.0 + i * 20,
                'model_long': MODELS_LONG['TFT'], 'model_short': 'TFT',
            })
            frows.append({
                'code': '15001', 'date': date,
                'pentad_in_year': pentad, 'pentad_in_month': pim,
                'forecasted_discharge': np.nan,  # TiDE NaN
                'model_long': MODELS_LONG['TiDE'], 'model_short': 'TiDE',
            })
        forecasts_nan = pd.DataFrame(frows)

        joint, _ = _make_ensemble(
            forecasts_nan, skill_all_pass, pentad_observed
        )
        em_rows = joint[joint['model_short'] == 'EM']
        assert len(em_rows) == 2, f"Expected 2 EM rows, got {len(em_rows)}"

        # EM = mean(LR, TFT) since TiDE NaN was dropped
        em_sorted = em_rows.sort_values('date').reset_index(drop=True)
        # Date 2026-01-05: mean(LR=100.0, TFT=110.0) = 105.0
        assert abs(em_sorted.iloc[0]['forecasted_discharge'] - 105.0) < 0.01
        # Date 2026-01-10: mean(LR=120.0, TFT=130.0) = 125.0
        assert abs(em_sorted.iloc[1]['forecasted_discharge'] - 125.0) < 0.01

    def test_threshold_boundary_values_excluded(
        self, pentad_observed, env_setup,
    ):
        """Metrics exactly at boundary are excluded (strict < / >)."""
        # sdivsigma=0.6 (not < 0.6), accuracy=0.8 (not > 0.8),
        # nse=0.8 (not > 0.8)
        boundary_skill = pd.DataFrame({
            'pentad_in_year': [1, 1],
            'code': ['15001', '15001'],
            'model_long': [MODELS_LONG['LR'], MODELS_LONG['TFT']],
            'model_short': ['LR', 'TFT'],
            'sdivsigma': [0.6, 0.6],   # exactly at threshold
            'nse': [0.8, 0.8],         # exactly at threshold
            'delta': [5.0, 5.0],
            'accuracy': [0.8, 0.8],    # exactly at threshold
            'mae': [2.0, 3.0],
            'n_pairs': [10, 10],
        })
        forecasts = pd.DataFrame({
            'code': ['15001', '15001'],
            'date': pd.to_datetime(['2026-01-05', '2026-01-05']),
            'pentad_in_year': [1, 1],
            'pentad_in_month': ['1', '1'],
            'forecasted_discharge': [100.0, 110.0],
            'model_long': [MODELS_LONG['LR'], MODELS_LONG['TFT']],
            'model_short': ['LR', 'TFT'],
        })

        joint, _ = _make_ensemble(forecasts, boundary_skill, pentad_observed)
        assert 'EM' not in joint['model_short'].values

    def test_three_stations_heterogeneous_outcomes(
        self, pentad_observed, env_setup,
    ):
        """Station A: LR+TFT pass -> EM. Station B: only LR -> no EM.
        Station C: no models pass -> no EM."""
        # Skill stats: A gets LR+TFT, B gets only LR, C gets nothing
        skill = pd.DataFrame([
            # Station 15001 (A): LR+TFT pass
            {'pentad_in_year': 1, 'code': '15001',
             'model_long': MODELS_LONG['LR'], 'model_short': 'LR',
             'sdivsigma': 0.3, 'nse': 0.95, 'delta': 5.0,
             'accuracy': 0.95, 'mae': 2.0, 'n_pairs': 10},
            {'pentad_in_year': 1, 'code': '15001',
             'model_long': MODELS_LONG['TFT'], 'model_short': 'TFT',
             'sdivsigma': 0.4, 'nse': 0.9, 'delta': 5.0,
             'accuracy': 0.88, 'mae': 3.0, 'n_pairs': 10},
            # Station 15002 (B): only LR passes
            {'pentad_in_year': 1, 'code': '15002',
             'model_long': MODELS_LONG['LR'], 'model_short': 'LR',
             'sdivsigma': 0.3, 'nse': 0.95, 'delta': 5.0,
             'accuracy': 0.95, 'mae': 2.0, 'n_pairs': 10},
            {'pentad_in_year': 1, 'code': '15002',
             'model_long': MODELS_LONG['TFT'], 'model_short': 'TFT',
             'sdivsigma': 0.9, 'nse': 0.5, 'delta': 5.0,
             'accuracy': 0.6, 'mae': 8.0, 'n_pairs': 10},
            # Station 15003 (C): no models pass
            {'pentad_in_year': 1, 'code': '15003',
             'model_long': MODELS_LONG['LR'], 'model_short': 'LR',
             'sdivsigma': 0.9, 'nse': 0.5, 'delta': 5.0,
             'accuracy': 0.6, 'mae': 8.0, 'n_pairs': 10},
        ])
        date = pd.to_datetime('2026-01-05')
        forecasts = pd.DataFrame([
            {'code': '15001', 'date': date, 'pentad_in_year': 1,
             'pentad_in_month': '1', 'forecasted_discharge': 100.0,
             'model_long': MODELS_LONG['LR'], 'model_short': 'LR'},
            {'code': '15001', 'date': date, 'pentad_in_year': 1,
             'pentad_in_month': '1', 'forecasted_discharge': 110.0,
             'model_long': MODELS_LONG['TFT'], 'model_short': 'TFT'},
            {'code': '15002', 'date': date, 'pentad_in_year': 1,
             'pentad_in_month': '1', 'forecasted_discharge': 200.0,
             'model_long': MODELS_LONG['LR'], 'model_short': 'LR'},
            {'code': '15002', 'date': date, 'pentad_in_year': 1,
             'pentad_in_month': '1', 'forecasted_discharge': 210.0,
             'model_long': MODELS_LONG['TFT'], 'model_short': 'TFT'},
            {'code': '15003', 'date': date, 'pentad_in_year': 1,
             'pentad_in_month': '1', 'forecasted_discharge': 300.0,
             'model_long': MODELS_LONG['LR'], 'model_short': 'LR'},
        ])
        observed = pd.DataFrame({
            'code': ['15001', '15002', '15003'],
            'date': [date] * 3,
            'discharge_avg': [105.0, 205.0, 305.0],
            'delta': [5.0, 5.0, 5.0],
        })

        joint, _ = _make_ensemble(forecasts, skill, observed)
        em_rows = joint[joint['model_short'] == 'EM']
        em_stations = set(em_rows['code'].unique())
        assert em_stations == {'15001'}, (
            f"Only station 15001 should get EM, got {em_stations}"
        )
        # Verify EM discharge = mean(LR=100, TFT=110) = 105
        assert abs(
            em_rows.iloc[0]['forecasted_discharge'] - 105.0
        ) < 0.01
        # Total rows = 5 base + 1 EM = 6
        assert len(joint) == 6, f"Expected 6 rows, got {len(joint)}"

    def test_non_em_rows_preserved_after_outer_join(
        self, pentad_skill_csv, pentad_forecasts, pentad_observed,
        env_setup,
    ):
        """Non-EM rows in output match input exactly (outer join safe)."""
        skill_metrics = data_reader.read_skill_metrics('pentad')
        joint, _ = _make_ensemble(
            pentad_forecasts, skill_metrics, pentad_observed
        )

        non_em_out = joint[joint['model_short'] != 'EM'].sort_values(
            ['code', 'date', 'model_short']
        ).reset_index(drop=True)
        original_sorted = pentad_forecasts.sort_values(
            ['code', 'date', 'model_short']
        ).reset_index(drop=True)

        # Same number of non-EM rows
        assert len(non_em_out) == len(original_sorted), (
            f"Non-EM rows changed: {len(non_em_out)} vs {len(original_sorted)}"
        )
        # Discharge values match
        pd.testing.assert_series_equal(
            non_em_out['forecasted_discharge'].reset_index(drop=True),
            original_sorted['forecasted_discharge'].reset_index(drop=True),
            check_names=False,
        )

    def test_gap_fill_full_pipeline(self, env_setup):
        """Write combined CSV with gap, detect gap, verify detection."""
        tmp_path = env_setup
        rows = []
        # Station A: has LR, TFT, and EM
        for ms in ('LR', 'TFT', 'EM'):
            rows.append({
                'code': '15001', 'date': '2026-01-05',
                'pentad_in_year': 1, 'pentad_in_month': '1',
                'forecasted_discharge': 105.0 if ms == 'EM' else 100.0,
                'model_long': MODELS_LONG.get(ms, f'Ens. Mean with LR, TFT ({ms})'),
                'model_short': ms,
            })
        # Station B: has LR, TFT but NO EM
        for ms in ('LR', 'TFT'):
            rows.append({
                'code': '15002', 'date': '2026-01-05',
                'pentad_in_year': 1, 'pentad_in_month': '1',
                'forecasted_discharge': 200.0,
                'model_long': MODELS_LONG[ms], 'model_short': ms,
            })

        df = pd.DataFrame(rows)
        filepath = os.path.join(str(tmp_path), 'combined_pentad.csv')
        _write_csv(df, filepath)

        combined = gap_detector.read_combined_forecasts('pentad')
        gaps = gap_detector.detect_missing_ensembles(
            combined, lookback_days=10,
        )
        # Only station B should have a gap
        assert len(gaps) == 1
        assert str(gaps.iloc[0]['code']) == '15002'
        # Station A's data unchanged
        a_data = combined[combined['code'] == '15001']
        assert 'EM' in a_data['model_short'].values

    def test_ensemble_composition_string_preserved_in_csv_roundtrip(
        self, pentad_skill_csv, pentad_forecasts, pentad_observed,
        env_setup,
    ):
        """Save EM to CSV, read back, verify exact composition string."""
        skill_metrics = data_reader.read_skill_metrics('pentad')
        joint, _ = _make_ensemble(
            pentad_forecasts, skill_metrics, pentad_observed
        )
        fl.save_forecast_data_pentad(joint)

        csv_path = os.path.join(str(env_setup), 'combined_pentad.csv')
        saved = pd.read_csv(csv_path)
        em_saved = saved[saved['model_short'] == 'EM']
        assert not em_saved.empty
        comp = em_saved.iloc[0]['model_long']
        assert comp == 'Ens. Mean with LR, TFT (EM)', (
            f"Expected exact composition string, got {comp!r}"
        )


# ---------------------------------------------------------------------------
# TestDecadalOperationalPipeline
# ---------------------------------------------------------------------------
class TestDecadalOperationalPipeline:
    """Mirrors pentad operational tests but for decadal data.

    Verifies the full decadal pipeline: skill CSV read -> threshold filter
    -> ensemble create -> CSV + API write.
    """

    DECAD_DATES = pd.to_datetime(['2026-01-10', '2026-01-20'])
    DECAD_IN_YEAR = [1, 2]
    DECAD_IN_MONTH = ['1', '2']

    @pytest.fixture
    def decad_env_setup(self, tmp_path):
        """Set env vars for decadal mode, yield, restore."""
        overrides = {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_combined_forecast_pentad_file': 'combined_pentad.csv',
            'ieasyforecast_combined_forecast_decad_file': 'combined_decad.csv',
            'ieasyforecast_pentadal_skill_metrics_file': 'skill_pentad.csv',
            'ieasyforecast_decadal_skill_metrics_file': 'skill_decad.csv',
            'ieasyhydroforecast_efficiency_threshold': '0.6',
            'ieasyhydroforecast_accuracy_threshold': '0.8',
            'ieasyhydroforecast_nse_threshold': '0.8',
            'SAPPHIRE_API_ENABLED': 'false',
            'SAPPHIRE_CONSISTENCY_CHECK': 'false',
            'SAPPHIRE_TEST_ENV': 'True',
        }
        with patch.dict(os.environ, overrides):
            yield tmp_path

    @pytest.fixture
    def decad_skill_csv(self, decad_env_setup):
        """Write decadal skill metrics CSV.

        Station 15001: LR + TFT pass -> eligible for EM.
        Station 15002: Only LR passes -> no EM.
        """
        tmp_path = decad_env_setup
        rows = []
        for station in STATIONS:
            for decad in self.DECAD_IN_YEAR:
                # LR passes at both stations
                rows.append({
                    'decad_in_year': decad, 'code': station,
                    'model_long': MODELS_LONG['LR'], 'model_short': 'LR',
                    'sdivsigma': 0.3, 'nse': 0.95, 'delta': 5.0,
                    'accuracy': 0.95, 'mae': 2.0, 'n_pairs': 10,
                })
                # TFT: passes at 15001, fails at 15002
                tft_skilled = station == '15001'
                rows.append({
                    'decad_in_year': decad, 'code': station,
                    'model_long': MODELS_LONG['TFT'], 'model_short': 'TFT',
                    'sdivsigma': 0.4 if tft_skilled else 0.7,
                    'nse': 0.9 if tft_skilled else 0.7,
                    'delta': 5.0,
                    'accuracy': 0.88 if tft_skilled else 0.7,
                    'mae': 3.0 if tft_skilled else 6.0,
                    'n_pairs': 10,
                })

        df = pd.DataFrame(rows)
        filepath = os.path.join(str(tmp_path), 'skill_decad.csv')
        _write_csv(df, filepath)
        return df

    @pytest.fixture
    def decad_forecasts(self):
        """2 models x 2 dates x 2 stations = 8 decadal forecast rows."""
        rows = []
        discharges = {
            ('15001', 0): {'LR': 100.0, 'TFT': 110.0},
            ('15001', 1): {'LR': 120.0, 'TFT': 130.0},
            ('15002', 0): {'LR': 200.0, 'TFT': 210.0},
            ('15002', 1): {'LR': 220.0, 'TFT': 230.0},
        }
        for station in STATIONS:
            for i, (date, decad, dim) in enumerate(
                zip(
                    self.DECAD_DATES,
                    self.DECAD_IN_YEAR,
                    self.DECAD_IN_MONTH,
                )
            ):
                for ms in ('LR', 'TFT'):
                    rows.append({
                        'code': station,
                        'date': date,
                        'decad_in_year': decad,
                        'decad_in_month': dim,
                        'forecasted_discharge': discharges[
                            (station, i)
                        ][ms],
                        'model_long': MODELS_LONG[ms],
                        'model_short': ms,
                    })
        return pd.DataFrame(rows)

    @pytest.fixture
    def decad_observed(self):
        """Observed discharge for decadal dates."""
        rows = []
        obs_vals = {
            ('15001', 0): 105.0, ('15001', 1): 125.0,
            ('15002', 0): 205.0, ('15002', 1): 225.0,
        }
        for station in STATIONS:
            for i, date in enumerate(self.DECAD_DATES):
                rows.append({
                    'code': station, 'date': date,
                    'discharge_avg': obs_vals[(station, i)], 'delta': 5.0,
                })
        return pd.DataFrame(rows)

    def test_decadal_ensemble_created_and_written_to_csv(
        self, decad_skill_csv, decad_forecasts, decad_observed,
        decad_env_setup,
    ):
        """Full decadal pipeline: read skills -> ensemble -> save CSV.

        Station 15001: LR + TFT pass -> EM = mean(LR, TFT).
        Station 15002: Only LR passes -> no EM (single-model rejected).
        """
        skill_metrics = data_reader.read_skill_metrics('decad')
        joint, _ = ensemble_calculator.create_ensemble_forecasts(
            forecasts=decad_forecasts,
            skill_stats=skill_metrics,
            observed=decad_observed,
            period_col='decad_in_year',
            period_in_month_col='decad_in_month',
            get_period_in_month_func=tl.get_decad_in_month,
            calculate_all_metrics_func=fl.calculate_all_skill_metrics,
        )

        # EM only for station 15001 (2 dates)
        em_rows = joint[joint['model_short'] == 'EM']
        assert len(em_rows) == 2, f"Expected 2 EM rows, got {len(em_rows)}"
        assert set(em_rows['code'].unique()) == {'15001'}

        # Verify EM discharge: mean(LR, TFT)
        em_sorted = em_rows.sort_values('date').reset_index(drop=True)
        # Date 2026-01-10: mean(100, 110) = 105
        assert abs(
            em_sorted.iloc[0]['forecasted_discharge'] - 105.0
        ) < 0.01
        # Date 2026-01-20: mean(120, 130) = 125
        assert abs(
            em_sorted.iloc[1]['forecasted_discharge'] - 125.0
        ) < 0.01

        # Save to CSV
        fl.save_forecast_data_decade(joint)
        csv_path = os.path.join(
            str(decad_env_setup), 'combined_decad.csv'
        )
        assert os.path.exists(csv_path)
        saved = pd.read_csv(csv_path)
        assert 'EM' in saved['model_short'].values

    def test_decadal_api_records_correct(
        self, decad_skill_csv, decad_forecasts, decad_observed,
        decad_env_setup,
    ):
        """Decadal API records have correct fields and EM records."""
        skill_metrics = data_reader.read_skill_metrics('decad')
        joint, _ = ensemble_calculator.create_ensemble_forecasts(
            forecasts=decad_forecasts,
            skill_stats=skill_metrics,
            observed=decad_observed,
            period_col='decad_in_year',
            period_in_month_col='decad_in_month',
            get_period_in_month_func=tl.get_decad_in_month,
            calculate_all_metrics_func=fl.calculate_all_skill_metrics,
        )

        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_forecasts.return_value = len(joint)

        with patch.object(fl, 'SAPPHIRE_API_AVAILABLE', True), \
             patch.object(
                 fl, '_get_postprocessing_client',
                 return_value=mock_client,
             ), \
             patch.dict(os.environ, {'SAPPHIRE_API_ENABLED': 'true'}):
            fl.save_forecast_data_decade(joint)

        mock_client.write_forecasts.assert_called_once()
        records = mock_client.write_forecasts.call_args[0][0]

        # 2 stations x 2 dates x 2 models = 8, + 2 EM (station 15001)
        assert len(records) == 10, f"Expected 10 records, got {len(records)}"

        em_records = [r for r in records if r['model_type'] == 'EM']
        assert len(em_records) == 2
        for r in em_records:
            assert r['code'] == '15001'
            assert r.get('composition') is not None


# ---------------------------------------------------------------------------
# TestMaintenanceDataRouting
# ---------------------------------------------------------------------------
class TestMaintenanceDataRouting:
    """Validates the gap-fill path:
    combined CSV read -> gap detection -> ensemble fill -> CSV write.
    """

    def _build_combined_csv(
        self, tmp_path, include_em_dates=None, no_em_dates=None,
        station='15001',
    ):
        """Build a combined forecasts CSV with optional EM rows.

        Args:
            tmp_path: Directory for the CSV.
            include_em_dates: Date strings where EM rows are included.
            no_em_dates: Date strings where only model rows exist (no EM).
            station: Station code.

        Returns:
            The DataFrame that was written.
        """
        include_em_dates = include_em_dates or []
        no_em_dates = no_em_dates or []
        rows = []
        all_dates = sorted(set(include_em_dates + no_em_dates))

        for date in all_dates:
            pentad = 1
            pim = '1'
            for ms in ('LR', 'TFT'):
                rows.append({
                    'code': station, 'date': date,
                    'pentad_in_year': pentad, 'pentad_in_month': pim,
                    'forecasted_discharge': 100.0 if ms == 'LR' else 110.0,
                    'model_long': MODELS_LONG[ms], 'model_short': ms,
                })
            if date in include_em_dates:
                rows.append({
                    'code': station, 'date': date,
                    'pentad_in_year': pentad, 'pentad_in_month': pim,
                    'forecasted_discharge': 105.0,
                    'model_long': 'Ens. Mean with LR, TFT (EM)',
                    'model_short': 'EM',
                })

        df = pd.DataFrame(rows)
        filepath = os.path.join(str(tmp_path), 'combined_pentad.csv')
        _write_csv(df, filepath)
        return df

    def test_gap_detected_and_filled(self, env_setup):
        """Missing EM for one date is detected; gap has correct code."""
        tmp_path = env_setup
        em_date = '2026-01-10'
        gap_date = '2026-01-05'
        self._build_combined_csv(
            tmp_path,
            include_em_dates=[em_date],
            no_em_dates=[gap_date],
        )

        combined = gap_detector.read_combined_forecasts('pentad')
        assert not combined.empty

        gaps = gap_detector.detect_missing_ensembles(
            combined, lookback_days=10,
        )
        assert len(gaps) == 1
        assert str(gaps.iloc[0]['code']) == '15001'
        gap_date_str = pd.Timestamp(
            gaps.iloc[0]['date']
        ).strftime('%Y-%m-%d')
        assert gap_date_str == '2026-01-05'

    def test_no_gaps_returns_empty(self, env_setup):
        """All dates have EM -> no gaps detected."""
        self._build_combined_csv(
            env_setup,
            include_em_dates=['2026-01-05', '2026-01-10'],
        )

        combined = gap_detector.read_combined_forecasts('pentad')
        gaps = gap_detector.detect_missing_ensembles(
            combined, lookback_days=10,
        )
        assert gaps.empty

    def test_lookback_window_limits_scope(self, env_setup):
        """Old gaps (>lookback days) ignored, recent gaps detected."""
        self._build_combined_csv(
            env_setup,
            include_em_dates=[],
            no_em_dates=['2025-12-20', '2026-01-10'],
        )

        combined = gap_detector.read_combined_forecasts('pentad')
        gaps = gap_detector.detect_missing_ensembles(
            combined, lookback_days=7,
        )

        # max_date=2026-01-10, cutoff=2026-01-03
        # Only 2026-01-10 is within 7-day window
        gap_dates = gaps['date'].dt.strftime('%Y-%m-%d').tolist()
        assert '2026-01-10' in gap_dates
        assert '2025-12-20' not in gap_dates

    def test_gap_fill_preserves_existing_data(self, env_setup):
        """After gap detection, original non-EM rows are unchanged."""
        tmp_path = env_setup
        original = self._build_combined_csv(
            tmp_path,
            include_em_dates=['2026-01-10'],
            no_em_dates=['2026-01-05'],
        )

        combined = gap_detector.read_combined_forecasts('pentad')
        gaps = gap_detector.detect_missing_ensembles(
            combined, lookback_days=10,
        )
        assert not gaps.empty

        # Original non-EM rows are preserved in the read
        non_em_original = original[original['model_short'] != 'EM']
        non_em_read = combined[combined['model_short'] != 'EM']
        assert len(non_em_original) == len(non_em_read)

        # Discharge values match
        for ms in ('LR', 'TFT'):
            orig_vals = sorted(
                non_em_original[non_em_original['model_short'] == ms][
                    'forecasted_discharge'
                ].tolist()
            )
            read_vals = sorted(
                non_em_read[non_em_read['model_short'] == ms][
                    'forecasted_discharge'
                ].tolist()
            )
            assert orig_vals == read_vals


# ---------------------------------------------------------------------------
# TestMaintenanceFullGapFill
# ---------------------------------------------------------------------------
class TestMaintenanceFullGapFill:
    """End-to-end gap-fill: detect gap -> create ensemble -> verify EM rows.

    Unlike TestMaintenanceDataRouting which stops at gap detection, these
    tests exercise the full pipeline: detect -> read data -> ensemble -> save.
    """

    def test_gap_detected_ensemble_created_and_saved(self, env_setup):
        """Full pipeline: missing EM -> detect gap -> ensemble -> CSV saved.

        Combined CSV has 2 dates: Jan 5 has EM, Jan 10 is missing EM.
        After gap-fill, Jan 10 should have an EM row in the output.
        """
        tmp_path = env_setup

        # 1. Write combined CSV with gap (Jan 10 missing EM)
        rows = []
        for date_str, has_em in [('2026-01-05', True), ('2026-01-10', False)]:
            for ms in ('LR', 'TFT'):
                rows.append({
                    'code': '15001', 'date': date_str,
                    'pentad_in_year': 1, 'pentad_in_month': '1',
                    'forecasted_discharge': 100.0 if ms == 'LR' else 110.0,
                    'model_long': MODELS_LONG[ms], 'model_short': ms,
                })
            if has_em:
                rows.append({
                    'code': '15001', 'date': date_str,
                    'pentad_in_year': 1, 'pentad_in_month': '1',
                    'forecasted_discharge': 105.0,
                    'model_long': 'Ens. Mean with LR, TFT (EM)',
                    'model_short': 'EM',
                })
        combined_df = pd.DataFrame(rows)
        _write_csv(
            combined_df,
            os.path.join(str(tmp_path), 'combined_pentad.csv'),
        )

        # 2. Write skill CSV (both models pass)
        skill_rows = []
        for ms in ('LR', 'TFT'):
            skill_rows.append({
                'pentad_in_year': 1, 'code': '15001',
                'model_long': MODELS_LONG[ms], 'model_short': ms,
                'sdivsigma': 0.3, 'nse': 0.95, 'delta': 5.0,
                'accuracy': 0.95, 'mae': 2.0, 'n_pairs': 10,
            })
        skill_df = pd.DataFrame(skill_rows)
        _write_csv(
            skill_df,
            os.path.join(str(tmp_path), 'skill_pentad.csv'),
        )

        # 3. Detect gap
        combined = gap_detector.read_combined_forecasts('pentad')
        gaps = gap_detector.detect_missing_ensembles(combined, lookback_days=10)
        assert len(gaps) == 1
        gap_date_str = pd.Timestamp(
            gaps.iloc[0]['date']
        ).strftime('%Y-%m-%d')
        assert gap_date_str == '2026-01-10'

        # 4. Build gap data fresh (matching production flow where
        # modelled data is read separately, not from combined CSV)
        gap_data = pd.DataFrame([
            {
                'code': '15001', 'date': pd.Timestamp('2026-01-10'),
                'pentad_in_year': 1, 'pentad_in_month': '1',
                'forecasted_discharge': 100.0,
                'model_long': MODELS_LONG['LR'], 'model_short': 'LR',
            },
            {
                'code': '15001', 'date': pd.Timestamp('2026-01-10'),
                'pentad_in_year': 1, 'pentad_in_month': '1',
                'forecasted_discharge': 110.0,
                'model_long': MODELS_LONG['TFT'], 'model_short': 'TFT',
            },
        ])

        skill_metrics = data_reader.read_skill_metrics('pentad')
        observed = pd.DataFrame({
            'code': ['15001'],
            'date': pd.to_datetime(['2026-01-10']),
            'discharge_avg': [105.0],
            'delta': [5.0],
        })

        joint, _ = _make_ensemble(gap_data, skill_metrics, observed)

        # 5. Verify EM rows created for gap date
        em_rows = joint[joint['model_short'] == 'EM']
        assert len(em_rows) == 1, f"Expected 1 EM row, got {len(em_rows)}"
        assert abs(
            em_rows.iloc[0]['forecasted_discharge'] - 105.0
        ) < 0.01, "EM = mean(LR=100, TFT=110) = 105.0"

        # 6. Save to CSV and verify
        fl.save_forecast_data_pentad(joint)
        csv_path = os.path.join(str(tmp_path), 'combined_pentad.csv')
        saved = pd.read_csv(csv_path)
        saved_em = saved[saved['model_short'] == 'EM']
        assert len(saved_em) >= 1, "EM rows should be in saved CSV"


# ---------------------------------------------------------------------------
# TestSkillMetricsFallback
# ---------------------------------------------------------------------------
class TestSkillMetricsFallback:
    """Validates CSV-primary / API-fallback in data_reader."""

    def test_csv_used_when_available(self, pentad_skill_csv, env_setup):
        """CSV file exists -> CSV data returned, API never called."""
        with patch.object(
            data_reader, '_read_skill_metrics_api'
        ) as mock_api:
            df = data_reader.read_skill_metrics('pentad')
            mock_api.assert_not_called()

        assert not df.empty
        assert len(df) == len(pentad_skill_csv)

    def test_api_fallback_when_csv_missing(self, env_setup):
        """No CSV -> API client called, returns normalized columns."""
        # env_setup points to tmp_path where no skill CSV exists
        # (pentad_skill_csv fixture intentionally NOT used)
        api_response = pd.DataFrame({
            'horizon_in_year': [1, 1],
            'code': ['15001', '15001'],
            'model_type': ['LR', 'TFT'],
            'sdivsigma': [0.3, 0.4],
            'nse': [0.95, 0.9],
            'delta': [5.0, 5.0],
            'accuracy': [0.95, 0.88],
            'mae': [2.0, 3.0],
            'n_pairs': [10, 10],
        })

        mock_client_cls = MagicMock()
        mock_instance = mock_client_cls.return_value
        mock_instance.is_ready.return_value = True
        mock_instance.read_skill_metrics.return_value = api_response

        with patch.object(data_reader, 'SAPPHIRE_API_AVAILABLE', True), \
             patch.object(
                 data_reader, 'SapphirePostprocessingClient',
                 mock_client_cls, create=True,
             ), \
             patch.dict(os.environ, {'SAPPHIRE_API_ENABLED': 'true'}):
            df = data_reader.read_skill_metrics('pentad')

        assert not df.empty
        # Normalized from horizon_in_year -> pentad_in_year
        assert 'pentad_in_year' in df.columns
        # Normalized from model_type -> model_short
        assert 'model_short' in df.columns
        # Derived model_long
        assert 'model_long' in df.columns
        assert df.iloc[0]['model_long'] == 'Linear regression (LR)'


# ---------------------------------------------------------------------------
# TestApiFailureModes
# ---------------------------------------------------------------------------
class TestApiFailureModes:
    """Validates SAPPHIRE_API_FAILURE_MODE behavior during save."""

    def _save_with_api_error(self, env_setup, failure_mode):
        """Helper: save with an API client that raises.

        Returns:
            (raised, csv_exists): Whether an exception propagated,
            and whether the CSV was written.
        """
        data = pd.DataFrame({
            'code': ['15001'],
            'date': pd.to_datetime(['2026-01-05']),
            'pentad_in_year': [1],
            'pentad_in_month': ['1'],
            'forecasted_discharge': [100.0],
            'model_long': ['Linear regression (LR)'],
            'model_short': ['LR'],
        })

        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_forecasts.side_effect = RuntimeError("API down")

        raised = False
        with patch.object(fl, 'SAPPHIRE_API_AVAILABLE', True), \
             patch.object(
                 fl, '_get_postprocessing_client',
                 return_value=mock_client,
             ), \
             patch.dict(os.environ, {
                 'SAPPHIRE_API_ENABLED': 'true',
                 'SAPPHIRE_API_FAILURE_MODE': failure_mode,
             }):
            try:
                fl.save_forecast_data_pentad(data)
            except RuntimeError:
                raised = True

        csv_path = os.path.join(str(env_setup), 'combined_pentad.csv')
        return raised, os.path.exists(csv_path)

    def test_warn_mode_continues_after_api_error(self, env_setup):
        """mode=warn: no exception raised, CSV written with correct data."""
        raised, csv_exists = self._save_with_api_error(env_setup, 'warn')
        assert not raised
        assert csv_exists

        # Verify CSV content: correct columns and values
        csv_path = os.path.join(str(env_setup), 'combined_pentad.csv')
        saved = pd.read_csv(csv_path)
        assert len(saved) == 1
        assert str(saved.iloc[0]['code']) == '15001'
        assert saved.iloc[0]['model_short'] == 'LR'
        assert abs(saved.iloc[0]['forecasted_discharge'] - 100.0) < 0.01

    def test_fail_mode_raises_after_api_error(self, env_setup):
        """mode=fail: exception propagated to caller."""
        raised, csv_exists = self._save_with_api_error(env_setup, 'fail')
        assert raised
        # CSV is written before API call, so it should exist
        assert csv_exists

    def test_ignore_mode_silent(self, env_setup):
        """mode=ignore: no exception, execution continues silently."""
        raised, csv_exists = self._save_with_api_error(env_setup, 'ignore')
        assert not raised
        assert csv_exists


# ---------------------------------------------------------------------------
# TestSingleModelEnsembleBug
# ---------------------------------------------------------------------------
class TestSingleModelEnsembleBug:
    """End-to-end tests for the single-model ensemble filter bug fix.

    Before the fix, only single-LR ensembles were rejected. Single-TFT
    or single-TiDE ensembles could slip through.
    """

    def test_single_tft_rejected(self, pentad_observed, env_setup):
        """Only TFT passes thresholds -> no EM rows."""
        skill = pd.DataFrame({
            'pentad_in_year': [1],
            'code': ['15001'],
            'model_long': [MODELS_LONG['TFT']],
            'model_short': ['TFT'],
            'sdivsigma': [0.3], 'nse': [0.95], 'delta': [5.0],
            'accuracy': [0.95], 'mae': [2.0], 'n_pairs': [10],
        })
        forecasts = pd.DataFrame({
            'code': ['15001'],
            'date': pd.to_datetime(['2026-01-05']),
            'pentad_in_year': [1],
            'pentad_in_month': ['1'],
            'forecasted_discharge': [110.0],
            'model_long': [MODELS_LONG['TFT']],
            'model_short': ['TFT'],
        })
        joint, _ = _make_ensemble(forecasts, skill, pentad_observed)
        assert 'EM' not in joint['model_short'].values

    def test_single_tide_rejected(self, pentad_observed, env_setup):
        """Only TiDE passes thresholds -> no EM rows."""
        skill = pd.DataFrame({
            'pentad_in_year': [1],
            'code': ['15001'],
            'model_long': [MODELS_LONG['TiDE']],
            'model_short': ['TiDE'],
            'sdivsigma': [0.3], 'nse': [0.95], 'delta': [5.0],
            'accuracy': [0.95], 'mae': [2.0], 'n_pairs': [10],
        })
        forecasts = pd.DataFrame({
            'code': ['15001'],
            'date': pd.to_datetime(['2026-01-05']),
            'pentad_in_year': [1],
            'pentad_in_month': ['1'],
            'forecasted_discharge': [90.0],
            'model_long': [MODELS_LONG['TiDE']],
            'model_short': ['TiDE'],
        })
        joint, _ = _make_ensemble(forecasts, skill, pentad_observed)
        assert 'EM' not in joint['model_short'].values

    def test_two_ml_models_accepted(self, pentad_observed, env_setup):
        """TFT + TiDE pass (LR fails) -> EM with correct avg."""
        skill = pd.DataFrame({
            'pentad_in_year': [1, 1, 1],
            'code': ['15001'] * 3,
            'model_long': [
                MODELS_LONG['LR'], MODELS_LONG['TFT'], MODELS_LONG['TiDE'],
            ],
            'model_short': ['LR', 'TFT', 'TiDE'],
            'sdivsigma': [0.9, 0.3, 0.3],   # LR fails
            'nse': [0.5, 0.95, 0.95],        # LR fails
            'delta': [5.0, 5.0, 5.0],
            'accuracy': [0.6, 0.95, 0.95],   # LR fails
            'mae': [8.0, 2.0, 2.0],
            'n_pairs': [10, 10, 10],
        })
        forecasts = pd.DataFrame({
            'code': ['15001'] * 3,
            'date': pd.to_datetime(['2026-01-05'] * 3),
            'pentad_in_year': [1, 1, 1],
            'pentad_in_month': ['1', '1', '1'],
            'forecasted_discharge': [100.0, 110.0, 90.0],
            'model_long': [
                MODELS_LONG['LR'], MODELS_LONG['TFT'], MODELS_LONG['TiDE'],
            ],
            'model_short': ['LR', 'TFT', 'TiDE'],
        })
        joint, _ = _make_ensemble(forecasts, skill, pentad_observed)
        em_rows = joint[joint['model_short'] == 'EM']
        assert not em_rows.empty
        # EM = mean(TFT=110, TiDE=90) = 100.0
        assert abs(em_rows.iloc[0]['forecasted_discharge'] - 100.0) < 0.01
        comp = em_rows.iloc[0]['model_long']
        assert 'TFT' in comp and 'TiDE' in comp
        assert 'LR' not in comp


# ---------------------------------------------------------------------------
# TestEdgeCaseInputs
# ---------------------------------------------------------------------------
class TestEdgeCaseInputs:
    """Edge cases: empty inputs should not crash."""

    def test_empty_forecasts_returns_unchanged(self, env_setup):
        """Empty forecasts DF -> returns (empty, skill_stats), no crash."""
        empty_fc = pd.DataFrame(columns=[
            'code', 'date', 'pentad_in_year', 'pentad_in_month',
            'forecasted_discharge', 'model_long', 'model_short',
        ])
        skill = pd.DataFrame({
            'pentad_in_year': [1],
            'code': ['15001'],
            'model_long': [MODELS_LONG['LR']],
            'model_short': ['LR'],
            'sdivsigma': [0.3], 'nse': [0.95], 'delta': [5.0],
            'accuracy': [0.95], 'mae': [2.0], 'n_pairs': [10],
        })
        observed = pd.DataFrame({
            'code': ['15001'],
            'date': pd.to_datetime(['2026-01-05']),
            'discharge_avg': [105.0],
            'delta': [5.0],
        })
        joint, skill_out = _make_ensemble(empty_fc, skill, observed)
        assert joint.empty, "Empty input forecasts should produce empty output"
        assert skill_out is not None

    def test_empty_skill_stats_no_ensemble(self, env_setup):
        """Empty skill_stats -> no ensemble created."""
        empty_skill = pd.DataFrame(columns=[
            'pentad_in_year', 'code', 'model_long', 'model_short',
            'sdivsigma', 'nse', 'delta', 'accuracy', 'mae', 'n_pairs',
        ])
        forecasts = pd.DataFrame({
            'code': ['15001'],
            'date': pd.to_datetime(['2026-01-05']),
            'pentad_in_year': [1],
            'pentad_in_month': ['1'],
            'forecasted_discharge': [100.0],
            'model_long': [MODELS_LONG['LR']],
            'model_short': ['LR'],
        })
        observed = pd.DataFrame({
            'code': ['15001'],
            'date': pd.to_datetime(['2026-01-05']),
            'discharge_avg': [105.0],
            'delta': [5.0],
        })
        joint, skill_out = _make_ensemble(forecasts, empty_skill, observed)
        assert 'EM' not in joint['model_short'].values
        assert len(joint) == len(forecasts)

    def test_empty_observed_no_ensemble(self, env_setup):
        """Empty observed -> merge produces empty -> returns unchanged."""
        skill = pd.DataFrame({
            'pentad_in_year': [1, 1],
            'code': ['15001', '15001'],
            'model_long': [MODELS_LONG['LR'], MODELS_LONG['TFT']],
            'model_short': ['LR', 'TFT'],
            'sdivsigma': [0.3, 0.4], 'nse': [0.95, 0.9],
            'delta': [5.0, 5.0], 'accuracy': [0.95, 0.88],
            'mae': [2.0, 3.0], 'n_pairs': [10, 10],
        })
        forecasts = pd.DataFrame({
            'code': ['15001', '15001'],
            'date': pd.to_datetime(['2026-01-05', '2026-01-05']),
            'pentad_in_year': [1, 1],
            'pentad_in_month': ['1', '1'],
            'forecasted_discharge': [100.0, 110.0],
            'model_long': [MODELS_LONG['LR'], MODELS_LONG['TFT']],
            'model_short': ['LR', 'TFT'],
        })
        empty_obs = pd.DataFrame(columns=[
            'code', 'date', 'discharge_avg', 'delta',
        ])
        joint, skill_out = _make_ensemble(forecasts, skill, empty_obs)
        # With no observed data, ensemble skill can't be computed
        assert len(joint) == len(forecasts)


# ---------------------------------------------------------------------------
# TestYearAndMonthBoundaries
# ---------------------------------------------------------------------------
class TestYearAndMonthBoundaries:
    """Tests for date boundary handling in ensemble creation."""

    def _make_boundary_data(self, dates, pentads, pims, station='15001'):
        """Build forecasts, skill, observed for given dates."""
        frows = []
        srows = []
        orows = []
        for date, pentad, pim in zip(dates, pentads, pims):
            for ms, ml in [('LR', MODELS_LONG['LR']),
                           ('TFT', MODELS_LONG['TFT'])]:
                frows.append({
                    'code': station, 'date': date,
                    'pentad_in_year': pentad, 'pentad_in_month': pim,
                    'forecasted_discharge': 100.0 if ms == 'LR' else 110.0,
                    'model_long': ml, 'model_short': ms,
                })
            for ms, ml in [('LR', MODELS_LONG['LR']),
                           ('TFT', MODELS_LONG['TFT'])]:
                srows.append({
                    'pentad_in_year': pentad, 'code': station,
                    'model_long': ml, 'model_short': ms,
                    'sdivsigma': 0.3, 'nse': 0.95, 'delta': 5.0,
                    'accuracy': 0.95, 'mae': 2.0, 'n_pairs': 10,
                })
            orows.append({
                'code': station, 'date': date,
                'discharge_avg': 105.0, 'delta': 5.0,
            })
        return (
            pd.DataFrame(frows),
            pd.DataFrame(srows),
            pd.DataFrame(orows),
        )

    def test_year_boundary_ensemble_creation(self, env_setup):
        """Dec 31 (pentad 72) + Jan 5 (pentad 1) -> EM for both dates."""
        dates = pd.to_datetime(['2025-12-31', '2026-01-05'])
        pentads = [72, 1]
        pims = ['6', '1']
        forecasts, skill, observed = self._make_boundary_data(
            dates, pentads, pims,
        )
        joint, _ = _make_ensemble(forecasts, skill, observed)
        em_rows = joint[joint['model_short'] == 'EM']
        assert len(em_rows) == 2
        em_pentads = sorted(em_rows['pentad_in_year'].tolist())
        assert em_pentads == [1, 72]

    def test_year_boundary_gap_detection(self, env_setup):
        """Combined CSV spans Dec->Jan, one date missing EM."""
        tmp_path = env_setup
        # Build combined CSV with Dec 31 having EM but Jan 5 missing EM
        rows = []
        for date_str, pentad, pim, has_em in [
            ('2025-12-31', 72, '6', True),
            ('2026-01-05', 1, '1', False),
        ]:
            for ms in ('LR', 'TFT'):
                rows.append({
                    'code': '15001', 'date': date_str,
                    'pentad_in_year': pentad, 'pentad_in_month': pim,
                    'forecasted_discharge': 100.0 if ms == 'LR' else 110.0,
                    'model_long': MODELS_LONG[ms], 'model_short': ms,
                })
            if has_em:
                rows.append({
                    'code': '15001', 'date': date_str,
                    'pentad_in_year': pentad, 'pentad_in_month': pim,
                    'forecasted_discharge': 105.0,
                    'model_long': 'Ens. Mean with LR, TFT (EM)',
                    'model_short': 'EM',
                })
        df = pd.DataFrame(rows)
        filepath = os.path.join(str(tmp_path), 'combined_pentad.csv')
        _write_csv(df, filepath)

        combined = gap_detector.read_combined_forecasts('pentad')
        gaps = gap_detector.detect_missing_ensembles(
            combined, lookback_days=10,
        )
        assert len(gaps) == 1
        gap_date = gaps.iloc[0]['date']
        assert pd.Timestamp(gap_date).strftime('%Y-%m-%d') == '2026-01-05'

    def test_year_boundary_lookback_window(self, env_setup):
        """Dec 25 and Jan 5 both missing EM, 7-day lookback from max date."""
        tmp_path = env_setup
        rows = []
        for date_str, pentad, pim in [
            ('2025-12-25', 71, '5'),
            ('2026-01-05', 1, '1'),
        ]:
            for ms in ('LR', 'TFT'):
                rows.append({
                    'code': '15001', 'date': date_str,
                    'pentad_in_year': pentad, 'pentad_in_month': pim,
                    'forecasted_discharge': 100.0 if ms == 'LR' else 110.0,
                    'model_long': MODELS_LONG[ms], 'model_short': ms,
                })
        df = pd.DataFrame(rows)
        filepath = os.path.join(str(tmp_path), 'combined_pentad.csv')
        _write_csv(df, filepath)

        combined = gap_detector.read_combined_forecasts('pentad')
        gaps = gap_detector.detect_missing_ensembles(
            combined, lookback_days=7,
        )
        # max_date=2026-01-05, cutoff=2025-12-29
        # Dec 25 is outside 7-day window, only Jan 5 detected
        gap_dates = gaps['date'].dt.strftime('%Y-%m-%d').tolist()
        assert '2026-01-05' in gap_dates
        assert '2025-12-25' not in gap_dates

    def test_month_boundary_ensemble_creation(self, env_setup):
        """Jan 31 (pentad 6) + Feb 5 (pentad 7) -> EM for both."""
        dates = pd.to_datetime(['2026-01-31', '2026-02-05'])
        pentads = [6, 7]
        pims = ['6', '1']
        forecasts, skill, observed = self._make_boundary_data(
            dates, pentads, pims,
        )
        joint, _ = _make_ensemble(forecasts, skill, observed)
        em_rows = joint[joint['model_short'] == 'EM']
        assert len(em_rows) == 2
        em_pentads = sorted(em_rows['pentad_in_year'].tolist())
        assert em_pentads == [6, 7]


# ---------------------------------------------------------------------------
# TestQuantileFields
# ---------------------------------------------------------------------------
class TestQuantileFields:
    """Tests verifying quantile column handling in ensemble output."""

    def test_quantile_columns_preserved_in_original_rows(self, env_setup):
        """Quantile columns in forecasts input survive in non-EM output."""
        skill = pd.DataFrame({
            'pentad_in_year': [1, 1],
            'code': ['15001', '15001'],
            'model_long': [MODELS_LONG['LR'], MODELS_LONG['TFT']],
            'model_short': ['LR', 'TFT'],
            'sdivsigma': [0.3, 0.4], 'nse': [0.95, 0.9],
            'delta': [5.0, 5.0], 'accuracy': [0.95, 0.88],
            'mae': [2.0, 3.0], 'n_pairs': [10, 10],
        })
        forecasts = pd.DataFrame({
            'code': ['15001', '15001'],
            'date': pd.to_datetime(['2026-01-05', '2026-01-05']),
            'pentad_in_year': [1, 1],
            'pentad_in_month': ['1', '1'],
            'forecasted_discharge': [100.0, 110.0],
            'model_long': [MODELS_LONG['LR'], MODELS_LONG['TFT']],
            'model_short': ['LR', 'TFT'],
            'q05': [80.0, 90.0],
            'q25': [90.0, 100.0],
            'q75': [110.0, 120.0],
            'q95': [120.0, 130.0],
        })
        observed = pd.DataFrame({
            'code': ['15001'],
            'date': pd.to_datetime(['2026-01-05']),
            'discharge_avg': [105.0],
            'delta': [5.0],
        })
        joint, _ = _make_ensemble(forecasts, skill, observed)
        non_em = joint[joint['model_short'] != 'EM']
        for qcol in ('q05', 'q25', 'q75', 'q95'):
            assert qcol in non_em.columns
            assert non_em[qcol].notna().all()

        # Verify actual quantile values for LR and TFT
        lr_row = non_em[non_em['model_short'] == 'LR'].iloc[0]
        tft_row = non_em[non_em['model_short'] == 'TFT'].iloc[0]
        assert lr_row['q05'] == 80.0
        assert tft_row['q05'] == 90.0

    def test_ensemble_rows_lack_quantiles(self, env_setup):
        """EM rows have NaN/missing for quantile columns (current behavior)."""
        skill = pd.DataFrame({
            'pentad_in_year': [1, 1],
            'code': ['15001', '15001'],
            'model_long': [MODELS_LONG['LR'], MODELS_LONG['TFT']],
            'model_short': ['LR', 'TFT'],
            'sdivsigma': [0.3, 0.4], 'nse': [0.95, 0.9],
            'delta': [5.0, 5.0], 'accuracy': [0.95, 0.88],
            'mae': [2.0, 3.0], 'n_pairs': [10, 10],
        })
        forecasts = pd.DataFrame({
            'code': ['15001', '15001'],
            'date': pd.to_datetime(['2026-01-05', '2026-01-05']),
            'pentad_in_year': [1, 1],
            'pentad_in_month': ['1', '1'],
            'forecasted_discharge': [100.0, 110.0],
            'model_long': [MODELS_LONG['LR'], MODELS_LONG['TFT']],
            'model_short': ['LR', 'TFT'],
            'q05': [80.0, 90.0],
            'q95': [120.0, 130.0],
        })
        observed = pd.DataFrame({
            'code': ['15001'],
            'date': pd.to_datetime(['2026-01-05']),
            'discharge_avg': [105.0],
            'delta': [5.0],
        })
        joint, _ = _make_ensemble(forecasts, skill, observed)
        em_rows = joint[joint['model_short'] == 'EM']
        assert len(em_rows) == 1, f"Expected 1 EM row, got {len(em_rows)}"
        # EM discharge = mean(LR=100, TFT=110) = 105.0
        assert abs(
            em_rows.iloc[0]['forecasted_discharge'] - 105.0
        ) < 0.01
        # EM rows should have NaN for quantile columns
        for qcol in ('q05', 'q95'):
            if qcol in em_rows.columns:
                assert em_rows[qcol].isna().all()


# ---------------------------------------------------------------------------
# TestRecalculateWithRealisticData
# ---------------------------------------------------------------------------
class TestRecalculateWithRealisticData:
    """Test #8: Feed realistic data through calculate_skill_metrics_pentad().

    Uses real logic for skill metric calculation and ensemble creation.
    3 stations x 2 pentads x 2 models, with known observed values so
    we can verify metric correctness.
    """

    @pytest.fixture
    def recalc_env(self, tmp_path):
        """Env vars for recalculate integration test."""
        overrides = {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_combined_forecast_pentad_file': 'combined_pentad.csv',
            'ieasyforecast_pentadal_skill_metrics_file': 'skill_pentad.csv',
            'ieasyhydroforecast_efficiency_threshold': '0.6',
            'ieasyhydroforecast_accuracy_threshold': '0.8',
            'ieasyhydroforecast_nse_threshold': '0.8',
            'SAPPHIRE_API_ENABLED': 'false',
            'SAPPHIRE_CONSISTENCY_CHECK': 'false',
            'SAPPHIRE_TEST_ENV': 'True',
        }
        with patch.dict(os.environ, overrides):
            yield tmp_path

    @pytest.fixture
    def realistic_data(self):
        """3 stations x 2 pentads x 2 models, 5 dates per pentad.

        Station 15001: LR good, TFT good -> EM expected
        Station 15002: LR good, TFT bad  -> no EM (single-model)
        Station 15003: LR bad,  TFT bad  -> no EM
        """
        stations = ['15001', '15002', '15003']
        # 5 dates in pentad 1, 5 dates in pentad 2
        dates_p1 = pd.to_datetime([
            '2026-01-01', '2026-01-02', '2026-01-03',
            '2026-01-04', '2026-01-05',
        ])
        dates_p2 = pd.to_datetime([
            '2026-01-06', '2026-01-07', '2026-01-08',
            '2026-01-09', '2026-01-10',
        ])

        # Observed discharge: around 100 m3/s with some variation
        obs_base = {
            '15001': [98, 102, 100, 104, 96],
            '15002': [200, 210, 205, 195, 190],
            '15003': [50, 55, 52, 48, 45],
        }

        # LR forecasts: good for 15001/15002, bad for 15003
        lr_base = {
            '15001': [99, 101, 100, 103, 97],   # close to obs
            '15002': [201, 209, 204, 196, 191],  # close to obs
            '15003': [80, 85, 82, 78, 75],       # far from obs
        }

        # TFT forecasts: good for 15001, bad for 15002/15003
        tft_base = {
            '15001': [99, 103, 100, 105, 96],     # close to obs (NSE>0.8)
            '15002': [150, 160, 155, 145, 140],   # far from obs
            '15003': [80, 85, 82, 78, 75],        # far from obs
        }

        sim_rows = []
        obs_rows = []

        for pentad_idx, (dates, pentad_num) in enumerate(
            [(dates_p1, 1), (dates_p2, 2)]
        ):
            pim = str(tl.get_pentad(dates[0] + pd.Timedelta(days=1)))
            for i, date in enumerate(dates):
                for station in stations:
                    obs_val = obs_base[station][i]
                    # Observed: one row per (date, station) — the
                    # function only uses [code, date, discharge_avg,
                    # delta] from observed, but requires model_long
                    # and model_short columns to exist.
                    obs_rows.append({
                        'code': station, 'date': date,
                        'discharge_avg': float(obs_val),
                        'delta': 5.0,
                        'model_long': 'observed',
                        'model_short': 'obs',
                    })

                    # Simulated — LR
                    lr_val = lr_base[station][i]
                    sim_rows.append({
                        'code': station, 'date': date,
                        'pentad_in_year': pentad_num,
                        'pentad_in_month': pim,
                        'forecasted_discharge': float(lr_val),
                        'model_long': MODEL_LONG_NAMES['LR'],
                        'model_short': 'LR',
                    })

                    # Simulated — TFT
                    tft_val = tft_base[station][i]
                    sim_rows.append({
                        'code': station, 'date': date,
                        'pentad_in_year': pentad_num,
                        'pentad_in_month': pim,
                        'forecasted_discharge': float(tft_val),
                        'model_long': MODEL_LONG_NAMES['TFT'],
                        'model_short': 'TFT',
                    })

        observed = pd.DataFrame(obs_rows)
        simulated = pd.DataFrame(sim_rows)
        return observed, simulated

    def test_skill_stats_shape_and_groups(self, recalc_env, realistic_data):
        """Skill stats has correct groups: 3 stations x 2 pentads x 2 models.

        Plus EM rows for station 15001 (both LR and TFT pass).
        """
        observed, simulated = realistic_data

        skill_stats, joint, _ = fl.calculate_skill_metrics_pentad(
            observed, simulated
        )

        # Base: 3 stations x 2 pentads x 2 models = 12
        base_groups = skill_stats[skill_stats['model_short'] != 'EM']
        assert len(base_groups) == 12, (
            f"Expected 12 base skill rows, got {len(base_groups)}"
        )

        # Each row should have all 6 metrics
        for col in ['sdivsigma', 'nse', 'mae', 'n_pairs', 'delta',
                     'accuracy']:
            assert col in skill_stats.columns, f"Missing column: {col}"

        # n_pairs should be 5 for each group (5 dates per pentad)
        assert (base_groups['n_pairs'] == 5).all(), (
            "Each group should have 5 data points"
        )

    def test_highly_skilled_models_get_em(
        self, recalc_env, realistic_data,
    ):
        """Station 15001 has both LR and TFT with good forecasts.

        Skill metrics should pass thresholds and EM rows should appear.
        """
        observed, simulated = realistic_data

        skill_stats, joint, _ = fl.calculate_skill_metrics_pentad(
            observed, simulated
        )

        # Check 15001 LR skill — forecasts are close to observed
        lr_15001_p1 = skill_stats[
            (skill_stats['code'] == '15001') &
            (skill_stats['pentad_in_year'] == 1) &
            (skill_stats['model_short'] == 'LR')
        ]
        assert len(lr_15001_p1) == 1
        # LR errors are small (1-3 m3/s), MAE should be < 3
        assert lr_15001_p1.iloc[0]['mae'] < 3.0
        # NSE should be high for good forecasts
        assert lr_15001_p1.iloc[0]['nse'] > 0.8

        # EM rows should exist for 15001
        em_rows = skill_stats[
            (skill_stats['code'] == '15001') &
            (skill_stats['model_short'] == 'EM')
        ]
        assert len(em_rows) >= 1, (
            f"Station 15001 should have EM skill rows, got {len(em_rows)}"
        )

    def test_poorly_skilled_station_no_em(
        self, recalc_env, realistic_data,
    ):
        """Station 15003: both LR and TFT have bad forecasts.

        Neither model should pass thresholds, so no EM rows.
        """
        observed, simulated = realistic_data

        skill_stats, joint, _ = fl.calculate_skill_metrics_pentad(
            observed, simulated
        )

        # 15003 LR errors are large (~30 m3/s vs obs ~50)
        lr_15003 = skill_stats[
            (skill_stats['code'] == '15003') &
            (skill_stats['model_short'] == 'LR')
        ]
        assert len(lr_15003) == 2  # 2 pentads
        # MAE should be high
        assert (lr_15003['mae'] > 20).all(), (
            "Bad forecasts should have high MAE"
        )

        # No EM for 15003
        em_15003 = skill_stats[
            (skill_stats['code'] == '15003') &
            (skill_stats['model_short'] == 'EM')
        ]
        assert len(em_15003) == 0, (
            f"Station 15003 should have no EM rows, got {len(em_15003)}"
        )

    def test_joint_forecasts_contain_em_rows(
        self, recalc_env, realistic_data,
    ):
        """Joint forecasts include original + EM rows for qualifying stations.

        Original: 3 stations x 2 pentads x 5 dates x 2 models = 60 rows.
        Plus EM rows for station 15001 (10 dates x 1 EM).
        """
        observed, simulated = realistic_data

        skill_stats, joint, _ = fl.calculate_skill_metrics_pentad(
            observed, simulated
        )

        # Original rows preserved
        original_count = len(simulated)
        assert original_count == 60
        non_em = joint[joint['model_short'] != 'EM']
        assert len(non_em) == 60, (
            f"Original 60 rows should be preserved, got {len(non_em)}"
        )

        # EM rows added for 15001
        em_rows = joint[joint['model_short'] == 'EM']
        assert len(em_rows) > 0, "EM rows should be in joint forecasts"

        # EM discharge should be mean of LR and TFT
        for _, em in em_rows.iterrows():
            date = em['date']
            code = em['code']
            lr_val = simulated[
                (simulated['date'] == date) &
                (simulated['code'] == code) &
                (simulated['model_short'] == 'LR')
            ]['forecasted_discharge'].iloc[0]
            tft_val = simulated[
                (simulated['date'] == date) &
                (simulated['code'] == code) &
                (simulated['model_short'] == 'TFT')
            ]['forecasted_discharge'].iloc[0]
            expected_em = (lr_val + tft_val) / 2.0
            assert abs(em['forecasted_discharge'] - expected_em) < 0.01, (
                f"EM for {code} on {date}: expected {expected_em}, "
                f"got {em['forecasted_discharge']}"
            )

    def test_save_writes_csv_and_api(self, recalc_env, realistic_data):
        """Full pipeline: calculate → save skill metrics + forecasts to CSV.

        Verify CSV files created with correct content.
        """
        observed, simulated = realistic_data
        tmp_path = recalc_env

        skill_stats, joint, _ = fl.calculate_skill_metrics_pentad(
            observed, simulated
        )

        # Save skill metrics
        with patch.object(fl, 'SAPPHIRE_API_AVAILABLE', False):
            fl.save_pentadal_skill_metrics(skill_stats)

        # Verify skill metrics CSV
        skill_csv = os.path.join(str(tmp_path), 'skill_pentad.csv')
        assert os.path.exists(skill_csv), "Skill metrics CSV not created"
        saved_skill = pd.read_csv(skill_csv)

        # Should have all base groups + EM groups
        assert len(saved_skill) >= 12, (
            f"Skill CSV should have >= 12 rows, got {len(saved_skill)}"
        )
        # Sorted by pentad_in_year, code, model_short
        assert saved_skill.iloc[0]['pentad_in_year'] <= \
            saved_skill.iloc[-1]['pentad_in_year']

        # Required columns present
        for col in ['pentad_in_year', 'code', 'model_short', 'sdivsigma',
                     'nse', 'delta', 'accuracy', 'mae', 'n_pairs']:
            assert col in saved_skill.columns, (
                f"Missing column in skill CSV: {col}"
            )

        # Save combined forecasts
        # Need pentad_in_month for save_forecast_data_pentad
        joint_with_pim = joint.copy()
        if 'pentad_in_month' not in joint_with_pim.columns:
            joint_with_pim['pentad_in_month'] = (
                pd.to_datetime(joint_with_pim['date'])
                + pd.Timedelta(days=1)
            ).apply(tl.get_pentad)
        with patch.object(fl, 'SAPPHIRE_API_AVAILABLE', False):
            fl.save_forecast_data_pentad(joint_with_pim)

        # Verify combined forecasts CSV
        forecast_csv = os.path.join(str(tmp_path), 'combined_pentad.csv')
        assert os.path.exists(forecast_csv), "Forecast CSV not created"
        saved_fc = pd.read_csv(forecast_csv)
        # Should have original + EM rows
        assert len(saved_fc) >= 60, (
            f"Forecast CSV should have >= 60 rows, got {len(saved_fc)}"
        )
        # EM rows present in CSV
        assert 'EM' in saved_fc['model_short'].values, (
            "EM rows should be in saved forecast CSV"
        )


# ---------------------------------------------------------------------------
# TestSkillMetricSavePath
# ---------------------------------------------------------------------------
class TestSkillMetricSavePath:
    """Test #9: Verify save_pentadal_skill_metrics() writes CSV correctly
    and calls API with properly mapped records."""

    @pytest.fixture
    def save_env(self, tmp_path):
        """Set env vars for save path test."""
        overrides = {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_pentadal_skill_metrics_file': 'skill_pentad.csv',
            'SAPPHIRE_API_ENABLED': 'true',
            'SAPPHIRE_CONSISTENCY_CHECK': 'false',
            'SAPPHIRE_TEST_ENV': 'True',
        }
        with patch.dict(os.environ, overrides):
            yield tmp_path

    @pytest.fixture
    def known_skill_stats(self):
        """Known skill stats DataFrame for verifying save output."""
        return pd.DataFrame({
            'pentad_in_year': [1, 1, 1, 2, 2, 2],
            'code': ['15001', '15001', '15001',
                      '15002', '15002', '15002'],
            'model_short': ['LR', 'TFT', 'EM', 'LR', 'TFT', 'EM'],
            'model_long': [
                MODEL_LONG_NAMES['LR'],
                MODEL_LONG_NAMES['TFT'],
                'Ens. Mean with LR, TFT (EM)',
                MODEL_LONG_NAMES['LR'],
                MODEL_LONG_NAMES['TFT'],
                'Ens. Mean with LR, TFT (EM)',
            ],
            'sdivsigma': [0.3456, 0.4123, 0.2987,
                           0.5012, 0.3678, 0.3234],
            'nse': [0.9234, 0.8876, 0.9456,
                     0.8123, 0.9012, 0.8678],
            'delta': [5.0, 5.0, 5.0, 5.0, 5.0, 5.0],
            'accuracy': [0.92, 0.88, 0.95, 0.85, 0.91, 0.89],
            'mae': [2.1234, 3.4567, 1.8901,
                     4.5678, 2.3456, 3.0123],
            'n_pairs': [10, 10, 10, 8, 8, 8],
            'date': pd.to_datetime([
                '2026-01-05', '2026-01-05', '2026-01-05',
                '2026-01-10', '2026-01-10', '2026-01-10',
            ]),
        })

    def test_csv_columns_and_sort_order(
        self, save_env, known_skill_stats,
    ):
        """CSV has correct columns and is sorted by
        (pentad_in_year, code, model_short)."""
        with patch.object(fl, 'SAPPHIRE_API_AVAILABLE', False):
            fl.save_pentadal_skill_metrics(known_skill_stats)

        csv_path = os.path.join(str(save_env), 'skill_pentad.csv')
        saved = pd.read_csv(csv_path)

        # Required columns
        for col in ['pentad_in_year', 'code', 'model_short',
                     'sdivsigma', 'nse', 'delta', 'accuracy',
                     'mae', 'n_pairs']:
            assert col in saved.columns, f"Missing column: {col}"

        # Sort order: pentad_in_year ascending, then code, then model_short
        assert saved.iloc[0]['pentad_in_year'] == 1
        assert saved.iloc[-1]['pentad_in_year'] == 2

        # Within pentad 1, sorted by code then model_short
        p1 = saved[saved['pentad_in_year'] == 1]
        model_shorts = list(p1['model_short'])
        assert model_shorts == sorted(model_shorts), (
            f"Within pentad, model_short should be sorted: {model_shorts}"
        )

    def test_values_rounded_to_4_decimals(
        self, save_env, known_skill_stats,
    ):
        """Float values are rounded to 4 decimal places."""
        with patch.object(fl, 'SAPPHIRE_API_AVAILABLE', False):
            fl.save_pentadal_skill_metrics(known_skill_stats)

        csv_path = os.path.join(str(save_env), 'skill_pentad.csv')
        saved = pd.read_csv(csv_path, dtype={'code': str})

        # Check sdivsigma rounding — 0.3456 should stay 0.3456
        row = saved[
            (saved['code'] == '15001') &
            (saved['model_short'] == 'LR') &
            (saved['pentad_in_year'] == 1)
        ]
        assert len(row) == 1
        assert abs(row.iloc[0]['sdivsigma'] - 0.3456) < 1e-5

        # Check mae rounding — 2.1234 stays 2.1234
        assert abs(row.iloc[0]['mae'] - 2.1234) < 1e-5

    def test_code_cleaned_no_dot_zero(self, save_env):
        """Code values like '15001.0' are cleaned to '15001'."""
        data = pd.DataFrame({
            'pentad_in_year': [1],
            'code': [15001.0],  # float code
            'model_short': ['LR'],
            'model_long': [MODEL_LONG_NAMES['LR']],
            'sdivsigma': [0.3], 'nse': [0.9], 'delta': [5.0],
            'accuracy': [0.9], 'mae': [2.0], 'n_pairs': [10],
            'date': pd.to_datetime(['2026-01-05']),
        })
        with patch.object(fl, 'SAPPHIRE_API_AVAILABLE', False):
            fl.save_pentadal_skill_metrics(data)

        csv_path = os.path.join(str(save_env), 'skill_pentad.csv')
        saved = pd.read_csv(csv_path, dtype={'code': str})
        assert saved.iloc[0]['code'] == '15001', (
            f"Code should be '15001', got '{saved.iloc[0]['code']}'"
        )

    def test_api_write_called_with_correct_args(
        self, save_env, known_skill_stats,
    ):
        """When API is available, _write_skill_metrics_to_api is called
        with the DataFrame and 'pentad' horizon type."""
        with patch.object(fl, 'SAPPHIRE_API_AVAILABLE', True), \
             patch.object(
                 fl, '_write_skill_metrics_to_api'
             ) as mock_api_write:
            fl.save_pentadal_skill_metrics(known_skill_stats)

            mock_api_write.assert_called_once()
            call_args = mock_api_write.call_args
            # First positional arg is the DataFrame
            api_data = call_args[0][0]
            assert isinstance(api_data, pd.DataFrame)
            assert len(api_data) == 6
            # Second arg is horizon_type
            assert call_args[0][1] == 'pentad'

    def test_api_failure_does_not_prevent_csv(
        self, save_env, known_skill_stats,
    ):
        """API failure should not prevent CSV from being written."""
        with patch.object(fl, 'SAPPHIRE_API_AVAILABLE', True), \
             patch.object(
                 fl, '_write_skill_metrics_to_api',
                 side_effect=Exception("API connection refused"),
             ), \
             patch.object(fl, '_handle_api_write_error'):
            fl.save_pentadal_skill_metrics(known_skill_stats)

        # CSV should still exist
        csv_path = os.path.join(str(save_env), 'skill_pentad.csv')
        assert os.path.exists(csv_path), (
            "CSV should be written even when API fails"
        )
        saved = pd.read_csv(csv_path)
        assert len(saved) == 6

    def test_date_formatted_as_yyyy_mm_dd(
        self, save_env, known_skill_stats,
    ):
        """Date column is formatted as YYYY-MM-DD string in CSV."""
        with patch.object(fl, 'SAPPHIRE_API_AVAILABLE', False):
            fl.save_pentadal_skill_metrics(known_skill_stats)

        csv_path = os.path.join(str(save_env), 'skill_pentad.csv')
        saved = pd.read_csv(csv_path)
        # Date should be string in YYYY-MM-DD format
        assert saved.iloc[0]['date'] == '2026-01-05'


# ---------------------------------------------------------------------------
# TestRecalculateSkillMetricsIntegration
# ---------------------------------------------------------------------------
class TestRecalculateSkillMetricsIntegration:
    """Tests for the recalculate_skill_metrics entry point."""

    def test_pentad_mode_calls_correct_functions(self, env_setup):
        """PENTAD mode calls load_environment, read, calculate, save."""
        with patch('setup_library.load_environment') as mock_load, \
             patch(
                 'setup_library.read_observed_and_modelled_data_pentade',
                 return_value=(
                     pd.DataFrame(columns=[
                         'code', 'date', 'discharge_avg', 'delta',
                     ]),
                     pd.DataFrame(columns=[
                         'code', 'date', 'pentad_in_year',
                         'pentad_in_month', 'forecasted_discharge',
                         'model_long', 'model_short',
                     ]),
                 ),
             ) as mock_read, \
             patch(
                 'forecast_library.calculate_skill_metrics_pentad',
                 return_value=(
                     pd.DataFrame(), pd.DataFrame(), None,
                 ),
             ) as mock_calc, \
             patch(
                 'forecast_library.save_forecast_data_pentad',
                 return_value=None,
             ) as mock_save_fc, \
             patch(
                 'forecast_library.save_pentadal_skill_metrics',
                 return_value=None,
             ) as mock_save_sk, \
             patch.dict(os.environ, {
                 'SAPPHIRE_PREDICTION_MODE': 'PENTAD',
             }):
            # Import and call the function; it calls sys.exit(0) on success
            from postprocessing_forecasts.recalculate_skill_metrics import (
                recalculate_skill_metrics,
            )
            with pytest.raises(SystemExit) as exc_info:
                recalculate_skill_metrics()
            assert exc_info.value.code == 0

            mock_load.assert_called_once()
            mock_read.assert_called_once()
            mock_calc.assert_called_once()
            mock_save_fc.assert_called_once()
            mock_save_sk.assert_called_once()

    def test_save_error_exits_with_error_code(self, env_setup):
        """save returning error string -> exit(1)."""
        with patch('setup_library.load_environment'), \
             patch(
                 'setup_library.read_observed_and_modelled_data_pentade',
                 return_value=(
                     pd.DataFrame(columns=[
                         'code', 'date', 'discharge_avg', 'delta',
                     ]),
                     pd.DataFrame(columns=[
                         'code', 'date', 'pentad_in_year',
                         'pentad_in_month', 'forecasted_discharge',
                         'model_long', 'model_short',
                     ]),
                 ),
             ), \
             patch(
                 'forecast_library.calculate_skill_metrics_pentad',
                 return_value=(
                     pd.DataFrame(), pd.DataFrame(), None,
                 ),
             ), \
             patch(
                 'forecast_library.save_forecast_data_pentad',
                 return_value="DB connection failed",
             ), \
             patch(
                 'forecast_library.save_pentadal_skill_metrics',
                 return_value=None,
             ), \
             patch.dict(os.environ, {
                 'SAPPHIRE_PREDICTION_MODE': 'PENTAD',
             }):
            from postprocessing_forecasts.recalculate_skill_metrics import (
                recalculate_skill_metrics,
            )
            with pytest.raises(SystemExit) as exc_info:
                recalculate_skill_metrics()
            assert exc_info.value.code == 1
