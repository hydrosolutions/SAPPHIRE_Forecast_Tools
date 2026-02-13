"""Tests for src/ensemble_calculator.py — ensemble creation from skill metrics."""

import os
import sys
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast')
)
sys.path.insert(0, os.path.dirname(__file__))

from src.ensemble_calculator import (
    extract_first_parentheses_content,
    model_long_agg,
    model_short_agg,
    _is_multi_model_ensemble,
    filter_for_highly_skilled_forecasts,
    create_ensemble_forecasts,
)
import forecast_library as fl
import tag_library as tl

from test_constants import MODEL_LONG_NAMES


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def skill_stats_pentad():
    """Skill stats with two highly-skilled models for code 10001."""
    return pd.DataFrame({
        'pentad_in_year': [1, 1, 1],
        'code': ['10001', '10001', '10001'],
        'model_long': [
            MODEL_LONG_NAMES['LR'],
            MODEL_LONG_NAMES['TFT'],
            MODEL_LONG_NAMES['TiDE'],
        ],
        'model_short': ['LR', 'TFT', 'TiDE'],
        'sdivsigma': [0.3, 0.4, 0.9],  # TiDE fails sdivsigma threshold
        'nse': [0.95, 0.9, 0.5],         # TiDE fails nse threshold
        'delta': [5.0, 5.0, 5.0],
        'accuracy': [0.95, 0.88, 0.6],   # TiDE fails accuracy threshold
        'mae': [2.0, 3.0, 8.0],
        'n_pairs': [10, 10, 10],
    })


@pytest.fixture
def forecasts_pentad():
    """Forecast data for two models across multiple dates."""
    dates = pd.to_datetime(['2024-01-05', '2024-01-05',
                            '2024-01-10', '2024-01-10'])
    return pd.DataFrame({
        'code': ['10001'] * 4,
        'date': dates,
        'pentad_in_year': [1, 1, 2, 2],
        'pentad_in_month': ['1', '1', '2', '2'],  # string like tl.get_pentad
        'forecasted_discharge': [100.0, 110.0, 120.0, 130.0],
        'model_long': [
            MODEL_LONG_NAMES['LR'], MODEL_LONG_NAMES['TFT'],
            MODEL_LONG_NAMES['LR'], MODEL_LONG_NAMES['TFT'],
        ],
        'model_short': ['LR', 'TFT', 'LR', 'TFT'],
    })


@pytest.fixture
def observed_pentad():
    """Observed data matching the forecast dates."""
    dates = pd.to_datetime(['2024-01-05', '2024-01-10'])
    return pd.DataFrame({
        'code': ['10001', '10001'],
        'date': dates,
        'discharge_avg': [105.0, 125.0],
        'delta': [5.0, 5.0],
    })


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------

class TestHelpers:
    def test_extract_parentheses_content(self):
        result = extract_first_parentheses_content([
            'Linear regression (LR)',
            'Temporal Fusion Transformer (TFT)',
            'No parentheses',
        ])
        assert result == ['LR', 'TFT', '']

    def test_model_long_agg(self):
        series = pd.Series([
            'Linear regression (LR)',
            'Temporal Fusion Transformer (TFT)',
        ])
        result = model_long_agg(series)
        assert result == 'Ens. Mean with LR, TFT (EM)'

    def test_model_long_agg_sorted(self):
        """Models are sorted alphabetically in composition."""
        series = pd.Series([
            'Temporal Fusion Transformer (TFT)',
            'Linear regression (LR)',
        ])
        result = model_long_agg(series)
        assert 'LR, TFT' in result

    def test_model_short_agg(self):
        series = pd.Series(['LR', 'TFT'])
        assert model_short_agg(series) == 'EM'

    def test_is_multi_model_two_models(self):
        assert _is_multi_model_ensemble('Ens. Mean with LR, TFT (EM)') is True

    def test_is_multi_model_three_models(self):
        assert _is_multi_model_ensemble(
            'Ens. Mean with LR, TFT, TiDE (EM)'
        ) is True

    def test_is_multi_model_single(self):
        assert _is_multi_model_ensemble('Ens. Mean with TFT (EM)') is False

    def test_is_multi_model_empty(self):
        assert _is_multi_model_ensemble('Ens. Mean with  (EM)') is False

    def test_is_multi_model_no_match(self):
        """Random string with no 'with ... (EM)' pattern -> False."""
        assert _is_multi_model_ensemble('Some random string') is False

    def test_is_multi_model_empty_string(self):
        """Empty string -> False."""
        assert _is_multi_model_ensemble('') is False

    def test_is_multi_model_four_models(self):
        """Four models in composition -> True."""
        assert _is_multi_model_ensemble(
            'Ens. Mean with A, B, C, D (EM)'
        ) is True

    def test_model_long_agg_duplicate_models(self):
        """Duplicate model entries are uniquified via .unique()."""
        series = pd.Series([
            'Linear regression (LR)',
            'Linear regression (LR)',
        ])
        result = model_long_agg(series)
        assert result == 'Ens. Mean with LR (EM)'

    def test_model_long_agg_three_models(self):
        """Three models sorted alphabetically."""
        series = pd.Series([
            MODEL_LONG_NAMES['TiDE'],
            MODEL_LONG_NAMES['LR'],
            MODEL_LONG_NAMES['TFT'],
        ])
        result = model_long_agg(series)
        assert result == 'Ens. Mean with LR, TFT, TiDE (EM)'

    def test_model_long_agg_single_model(self):
        """Single model -> 'Ens. Mean with LR (EM)' (filtered later)."""
        series = pd.Series([MODEL_LONG_NAMES['LR']])
        result = model_long_agg(series)
        assert result == 'Ens. Mean with LR (EM)'

    def test_extract_no_parentheses(self):
        """Input with no parentheses -> empty string."""
        result = extract_first_parentheses_content(['ModelName'])
        assert result == ['']

    def test_extract_nested_parentheses(self):
        """Multiple parenthesised groups -> first match extracted."""
        result = extract_first_parentheses_content(
            ['Outer (Inner) more (Second)']
        )
        assert result == ['Inner']


# ---------------------------------------------------------------------------
# Filter tests
# ---------------------------------------------------------------------------

class TestFilterHighlySkilled:
    def test_default_thresholds(self, skill_stats_pentad):
        """With default thresholds, only LR and TFT pass."""
        with patch.dict(os.environ, {
            'ieasyhydroforecast_efficiency_threshold': '0.6',
            'ieasyhydroforecast_accuracy_threshold': '0.8',
            'ieasyhydroforecast_nse_threshold': '0.8',
        }):
            result = filter_for_highly_skilled_forecasts(skill_stats_pentad)
            assert len(result) == 2
            assert set(result['model_short']) == {'LR', 'TFT'}

    def test_custom_thresholds_all_pass(self, skill_stats_pentad):
        """With very loose thresholds, all models pass."""
        result = filter_for_highly_skilled_forecasts(
            skill_stats_pentad,
            threshold_sdivsigma=10.0,
            threshold_accuracy=0.0,
            threshold_nse=0.0,
        )
        assert len(result) == 3

    def test_disabled_threshold_via_false(self, skill_stats_pentad):
        """Threshold='False' disables that filter."""
        result = filter_for_highly_skilled_forecasts(
            skill_stats_pentad,
            threshold_sdivsigma='False',
            threshold_accuracy='False',
            threshold_nse='False',
        )
        assert len(result) == 3

    def test_strict_thresholds_none_pass(self, skill_stats_pentad):
        """With very strict thresholds, no models pass."""
        result = filter_for_highly_skilled_forecasts(
            skill_stats_pentad,
            threshold_sdivsigma=0.01,
            threshold_accuracy=0.99,
            threshold_nse=0.99,
        )
        assert len(result) == 0


# ---------------------------------------------------------------------------
# Ensemble creation tests
# ---------------------------------------------------------------------------

class TestCreateEnsembleForecasts:
    def _make_ensemble(
        self, forecasts, skill_stats, observed, period='pentad'
    ):
        """Helper to call create_ensemble_forecasts with pentad defaults."""
        if period == 'pentad':
            return create_ensemble_forecasts(
                forecasts=forecasts,
                skill_stats=skill_stats,
                observed=observed,
                period_col='pentad_in_year',
                period_in_month_col='pentad_in_month',
                get_period_in_month_func=tl.get_pentad,
                calculate_all_metrics_func=fl.calculate_all_skill_metrics,
            )
        else:
            return create_ensemble_forecasts(
                forecasts=forecasts,
                skill_stats=skill_stats,
                observed=observed,
                period_col='decad_in_year',
                period_in_month_col='decad_in_month',
                get_period_in_month_func=tl.get_decad_in_month,
                calculate_all_metrics_func=fl.calculate_all_skill_metrics,
            )

    def test_ensemble_created_for_qualified_models(
        self, forecasts_pentad, skill_stats_pentad, observed_pentad
    ):
        """Ensemble is created from LR+TFT (TiDE excluded by threshold)."""
        with patch.dict(os.environ, {
            'ieasyhydroforecast_efficiency_threshold': '0.6',
            'ieasyhydroforecast_accuracy_threshold': '0.8',
            'ieasyhydroforecast_nse_threshold': '0.8',
        }):
            joint, skill_out = self._make_ensemble(
                forecasts_pentad, skill_stats_pentad, observed_pentad
            )
            # Should contain original models plus EM rows
            assert 'EM' in joint['model_short'].values
            # Ensemble discharge should be mean of LR and TFT
            em_rows = joint[joint['model_short'] == 'EM']
            for _, row in em_rows.iterrows():
                date = row['date']
                lr_val = forecasts_pentad[
                    (forecasts_pentad['date'] == date) &
                    (forecasts_pentad['model_short'] == 'LR')
                ]['forecasted_discharge'].iloc[0]
                tft_val = forecasts_pentad[
                    (forecasts_pentad['date'] == date) &
                    (forecasts_pentad['model_short'] == 'TFT')
                ]['forecasted_discharge'].iloc[0]
                expected_mean = (lr_val + tft_val) / 2
                assert abs(row['forecasted_discharge'] - expected_mean) < 0.01

    def test_ensemble_skill_stats_appended(
        self, forecasts_pentad, skill_stats_pentad, observed_pentad
    ):
        """Skill stats for EM are appended to the output."""
        with patch.dict(os.environ, {
            'ieasyhydroforecast_efficiency_threshold': '0.6',
            'ieasyhydroforecast_accuracy_threshold': '0.8',
            'ieasyhydroforecast_nse_threshold': '0.8',
        }):
            _, skill_out = self._make_ensemble(
                forecasts_pentad, skill_stats_pentad, observed_pentad
            )
            assert 'EM' in skill_out['model_short'].values

    def test_ne_excluded_from_ensemble(
        self, forecasts_pentad, skill_stats_pentad, observed_pentad
    ):
        """NE (neural ensemble) is excluded from ensemble candidates."""
        # Add NE model to forecasts and skill_stats
        ne_forecast = pd.DataFrame({
            'code': ['10001'],
            'date': pd.to_datetime(['2024-01-05']),
            'pentad_in_year': [1],
            'pentad_in_month': ['1'],
            'forecasted_discharge': [999.0],
            'model_long': ['Neural Ensemble (NE)'],
            'model_short': ['NE'],
        })
        ne_skill = pd.DataFrame({
            'pentad_in_year': [1],
            'code': ['10001'],
            'model_long': ['Neural Ensemble (NE)'],
            'model_short': ['NE'],
            'sdivsigma': [0.1], 'nse': [0.99],
            'delta': [5.0], 'accuracy': [0.99],
            'mae': [0.5], 'n_pairs': [10],
        })
        forecasts_with_ne = pd.concat(
            [forecasts_pentad, ne_forecast], ignore_index=True
        )
        skills_with_ne = pd.concat(
            [skill_stats_pentad, ne_skill], ignore_index=True
        )

        with patch.dict(os.environ, {
            'ieasyhydroforecast_efficiency_threshold': '0.6',
            'ieasyhydroforecast_accuracy_threshold': '0.8',
            'ieasyhydroforecast_nse_threshold': '0.8',
        }):
            joint, _ = self._make_ensemble(
                forecasts_with_ne, skills_with_ne, observed_pentad
            )
            em_rows = joint[joint['model_short'] == 'EM']
            # Ensemble mean should NOT include 999.0 from NE
            for _, row in em_rows.iterrows():
                assert row['forecasted_discharge'] < 900.0

    def test_no_ensemble_when_no_qualified_models(self, observed_pentad):
        """No ensemble created when no models pass thresholds."""
        skill_stats = pd.DataFrame({
            'pentad_in_year': [1],
            'code': ['10001'],
            'model_long': ['Linear regression (LR)'],
            'model_short': ['LR'],
            'sdivsigma': [0.9], 'nse': [0.5],
            'delta': [5.0], 'accuracy': [0.6],
            'mae': [8.0], 'n_pairs': [10],
        })
        forecasts = pd.DataFrame({
            'code': ['10001'],
            'date': pd.to_datetime(['2024-01-05']),
            'pentad_in_year': [1],
            'pentad_in_month': ['1'],
            'forecasted_discharge': [100.0],
            'model_long': ['Linear regression (LR)'],
            'model_short': ['LR'],
        })
        with patch.dict(os.environ, {
            'ieasyhydroforecast_efficiency_threshold': '0.6',
            'ieasyhydroforecast_accuracy_threshold': '0.8',
            'ieasyhydroforecast_nse_threshold': '0.8',
        }):
            joint, skill_out = self._make_ensemble(
                forecasts, skill_stats, observed_pentad
            )
            assert 'EM' not in joint['model_short'].values

    def test_single_model_ensemble_discarded(self, observed_pentad):
        """Ensemble with only one model (LR only) is discarded."""
        skill_stats = pd.DataFrame({
            'pentad_in_year': [1],
            'code': ['10001'],
            'model_long': ['Linear regression (LR)'],
            'model_short': ['LR'],
            'sdivsigma': [0.3], 'nse': [0.95],
            'delta': [5.0], 'accuracy': [0.95],
            'mae': [2.0], 'n_pairs': [10],
        })
        forecasts = pd.DataFrame({
            'code': ['10001'],
            'date': pd.to_datetime(['2024-01-05']),
            'pentad_in_year': [1],
            'pentad_in_month': ['1'],
            'forecasted_discharge': [100.0],
            'model_long': ['Linear regression (LR)'],
            'model_short': ['LR'],
        })
        with patch.dict(os.environ, {
            'ieasyhydroforecast_efficiency_threshold': '0.6',
            'ieasyhydroforecast_accuracy_threshold': '0.8',
            'ieasyhydroforecast_nse_threshold': '0.8',
        }):
            joint, _ = self._make_ensemble(
                forecasts, skill_stats, observed_pentad
            )
            # Single-model "Ens. Mean with LR (EM)" should be discarded
            assert 'EM' not in joint['model_short'].values

    def test_decad_ensemble(self):
        """Ensemble works for decadal data too."""
        skill_stats = pd.DataFrame({
            'decad_in_year': [1, 1],
            'code': ['10001', '10001'],
            'model_long': [
                'Linear regression (LR)',
                'Temporal Fusion Transformer (TFT)',
            ],
            'model_short': ['LR', 'TFT'],
            'sdivsigma': [0.3, 0.4],
            'nse': [0.95, 0.9],
            'delta': [5.0, 5.0],
            'accuracy': [0.95, 0.88],
            'mae': [2.0, 3.0],
            'n_pairs': [10, 10],
        })
        dates = pd.to_datetime(['2024-01-10', '2024-01-10'])
        forecasts = pd.DataFrame({
            'code': ['10001', '10001'],
            'date': dates,
            'decad_in_year': [1, 1],
            'decad_in_month': ['1', '1'],  # string like tl.get_decad_in_month
            'forecasted_discharge': [100.0, 110.0],
            'model_long': [
                'Linear regression (LR)',
                'Temporal Fusion Transformer (TFT)',
            ],
            'model_short': ['LR', 'TFT'],
        })
        observed = pd.DataFrame({
            'code': ['10001'],
            'date': pd.to_datetime(['2024-01-10']),
            'discharge_avg': [105.0],
            'delta': [5.0],
        })
        with patch.dict(os.environ, {
            'ieasyhydroforecast_efficiency_threshold': '0.6',
            'ieasyhydroforecast_accuracy_threshold': '0.8',
            'ieasyhydroforecast_nse_threshold': '0.8',
        }):
            joint, _ = self._make_ensemble(
                forecasts, skill_stats, observed, period='decad'
            )
            assert 'EM' in joint['model_short'].values

    def test_composition_string_format(
        self, forecasts_pentad, skill_stats_pentad, observed_pentad
    ):
        """Composition string is 'Ens. Mean with LR, TFT (EM)'."""
        with patch.dict(os.environ, {
            'ieasyhydroforecast_efficiency_threshold': '0.6',
            'ieasyhydroforecast_accuracy_threshold': '0.8',
            'ieasyhydroforecast_nse_threshold': '0.8',
        }):
            joint, _ = self._make_ensemble(
                forecasts_pentad, skill_stats_pentad, observed_pentad
            )
            em_rows = joint[joint['model_short'] == 'EM']
            if not em_rows.empty:
                model_long = em_rows.iloc[0]['model_long']
                assert model_long.startswith('Ens. Mean with ')
                assert model_long.endswith(' (EM)')
                assert 'LR' in model_long
                assert 'TFT' in model_long

    def test_single_tft_ensemble_discarded(self, observed_pentad):
        """Ensemble with only TFT (single model) is discarded."""
        skill_stats = pd.DataFrame({
            'pentad_in_year': [1],
            'code': ['10001'],
            'model_long': ['Temporal Fusion Transformer (TFT)'],
            'model_short': ['TFT'],
            'sdivsigma': [0.3], 'nse': [0.95],
            'delta': [5.0], 'accuracy': [0.95],
            'mae': [2.0], 'n_pairs': [10],
        })
        forecasts = pd.DataFrame({
            'code': ['10001'],
            'date': pd.to_datetime(['2024-01-05']),
            'pentad_in_year': [1],
            'pentad_in_month': ['1'],
            'forecasted_discharge': [110.0],
            'model_long': ['Temporal Fusion Transformer (TFT)'],
            'model_short': ['TFT'],
        })
        with patch.dict(os.environ, {
            'ieasyhydroforecast_efficiency_threshold': '0.6',
            'ieasyhydroforecast_accuracy_threshold': '0.8',
            'ieasyhydroforecast_nse_threshold': '0.8',
        }):
            joint, _ = self._make_ensemble(
                forecasts, skill_stats, observed_pentad
            )
            assert 'EM' not in joint['model_short'].values

    def test_single_tide_ensemble_discarded(self, observed_pentad):
        """Ensemble with only TiDE (single model) is discarded."""
        skill_stats = pd.DataFrame({
            'pentad_in_year': [1],
            'code': ['10001'],
            'model_long': ['Time-series Dense Encoder (TiDE)'],
            'model_short': ['TiDE'],
            'sdivsigma': [0.3], 'nse': [0.95],
            'delta': [5.0], 'accuracy': [0.95],
            'mae': [2.0], 'n_pairs': [10],
        })
        forecasts = pd.DataFrame({
            'code': ['10001'],
            'date': pd.to_datetime(['2024-01-05']),
            'pentad_in_year': [1],
            'pentad_in_month': ['1'],
            'forecasted_discharge': [90.0],
            'model_long': ['Time-series Dense Encoder (TiDE)'],
            'model_short': ['TiDE'],
        })
        with patch.dict(os.environ, {
            'ieasyhydroforecast_efficiency_threshold': '0.6',
            'ieasyhydroforecast_accuracy_threshold': '0.8',
            'ieasyhydroforecast_nse_threshold': '0.8',
        }):
            joint, _ = self._make_ensemble(
                forecasts, skill_stats, observed_pentad
            )
            assert 'EM' not in joint['model_short'].values


# ---------------------------------------------------------------------------
# Model name consistency tests
# ---------------------------------------------------------------------------

class TestModelNameConsistency:
    """Verify that model name mappings in data_reader cover all core types."""

    def test_model_short_to_long_covers_core_types(self):
        """MODEL_SHORT_TO_LONG covers LR, TFT, TiDE, TSMixer, EM, NE, RRAM."""
        from src.data_reader import MODEL_SHORT_TO_LONG
        expected = {'LR', 'TFT', 'TiDE', 'TSMixer', 'EM', 'NE', 'RRAM'}
        assert expected.issubset(set(MODEL_SHORT_TO_LONG.keys()))

    def test_api_model_type_mapping_consistent(self):
        """API_MODEL_TYPE_TO_SHORT keys are a subset of MODEL_SHORT_TO_LONG."""
        from src.data_reader import (
            MODEL_SHORT_TO_LONG, API_MODEL_TYPE_TO_SHORT,
        )
        for api_key in API_MODEL_TYPE_TO_SHORT:
            short = API_MODEL_TYPE_TO_SHORT[api_key]
            assert short in MODEL_SHORT_TO_LONG, (
                f"API_MODEL_TYPE_TO_SHORT[{api_key!r}] = {short!r} "
                f"not in MODEL_SHORT_TO_LONG"
            )
