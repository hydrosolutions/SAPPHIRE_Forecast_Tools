"""Tests for src/skill_metrics.py — skill metric calculations.

Moved from iEasyHydroForecast/tests/test_forecast_library.py
(TestCalculateSkillMetricsPentad).
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast')
)

from src import skill_metrics


@pytest.fixture
def observed():
    """Sample observed data: 2 stations, 2 pentads, 2 years."""
    return pd.DataFrame({
        'code': ['123', '123', '123', '123',
                 '456', '456', '456', '456'],
        'date': pd.to_datetime([
            '2022-01-01', '2023-01-01', '2022-01-06', '2023-01-06',
            '2022-01-01', '2023-01-01', '2022-01-06', '2023-01-06',
        ]),
        'discharge_avg': [10.0, 12.0, 10.0, 12.0,
                          20.0, 22.0, 20.0, 22.0],
        'model_long': ['Observed (Obs)'] * 8,
        'model_short': ['Obs'] * 8,
        'delta': [1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 2.0],
    })


@pytest.fixture
def simulated():
    """Sample simulated data: 2 models (MA, MB)."""
    df = pd.DataFrame({
        'code': (['123'] * 4 + ['456'] * 4) * 2,
        'date': pd.to_datetime([
            '2022-01-01', '2023-01-01', '2022-01-06', '2023-01-06',
            '2022-01-01', '2023-01-01', '2022-01-06', '2023-01-06',
        ] * 2),
        'pentad_in_month': [1, 1, 2, 2, 1, 1, 2, 2] * 2,
        'pentad_in_year': [1, 1, 2, 2, 1, 1, 2, 2] * 2,
        'forecasted_discharge': [
            10.2, 10.3, 9.8, 11.9, 20.2, 22.3, 20.1, 21.7,
            10.1, 12.1, 10.05, 11.9, 20.1, 22.3, 19.9, 21.7,
        ],
        'model_long': ['Model A (MA)'] * 8 + ['Model B (MB)'] * 8,
        'model_short': ['MA'] * 8 + ['MB'] * 8,
    })
    df['pentad_in_month'] = df['pentad_in_month'].astype(str)
    df['pentad_in_year'] = df['pentad_in_year'].astype(str)
    return df


@pytest.fixture(autouse=True)
def _set_thresholds(monkeypatch):
    """Set ensemble threshold env vars."""
    monkeypatch.setenv('ieasyhydroforecast_efficiency_threshold', '0.6')
    monkeypatch.setenv('ieasyhydroforecast_accuracy_threshold', '0.8')
    monkeypatch.setenv('ieasyhydroforecast_nse_threshold', '0.8')


class TestCalculateSkillMetricsPentad:
    """Tests for calculate_skill_metrics_pentad."""

    def test_input_validation(self, observed, simulated):
        """Missing columns in observed or simulated raise ValueError."""
        bad_observed = observed.drop(columns=['delta'])
        with pytest.raises(ValueError):
            skill_metrics.calculate_skill_metrics_pentad(
                bad_observed, simulated
            )

        bad_simulated = simulated.drop(columns=['pentad_in_year'])
        with pytest.raises(ValueError):
            skill_metrics.calculate_skill_metrics_pentad(
                observed, bad_simulated
            )

    def test_date_filtering(self, observed, simulated):
        """Data filtered for dates after 2010."""
        combined_observed = pd.concat([observed, observed.copy()])
        skill_stats, joint_forecasts, _ = (
            skill_metrics.calculate_skill_metrics_pentad(
                combined_observed, simulated
            )
        )
        assert all(joint_forecasts['date'].dt.year >= 2010)

    def test_sdivsigma_calculation(self, observed, simulated):
        """sdivsigma values are finite and < 1 for good forecasts."""
        merged = pd.merge(
            simulated,
            observed[['code', 'date', 'discharge_avg', 'delta']],
            on=['code', 'date'],
        )
        output = (
            merged.groupby(
                ['pentad_in_year', 'code', 'model_long', 'model_short']
            )[merged.columns]
            .apply(
                skill_metrics.sdivsigma_nse,
                observed_col='discharge_avg',
                simulated_col='forecasted_discharge',
            )
            .reset_index()
        )
        assert all(output['nse'] < 1)

    def test_skill_metrics_columns_and_ranges(self, observed, simulated):
        """Skill stats have expected columns; values in valid ranges."""
        skill_stats, _, _ = skill_metrics.calculate_skill_metrics_pentad(
            observed, simulated
        )
        expected_columns = [
            'pentad_in_year', 'code', 'model_long', 'model_short',
            'sdivsigma', 'nse', 'mae', 'n_pairs', 'delta', 'accuracy',
        ]
        for col in expected_columns:
            assert col in skill_stats.columns, (
                f"Missing column: {col}"
            )
        assert all(skill_stats['accuracy'] >= 0)
        assert all(skill_stats['accuracy'] <= 1)
        assert all(skill_stats['sdivsigma'] >= 0)
        assert all(skill_stats['mae'] >= 0)

    def test_ensemble_creation(self, observed, simulated, monkeypatch):
        """Ensemble forecasts created as average of qualifying models."""
        # Relax thresholds so both models qualify
        monkeypatch.setenv('ieasyhydroforecast_efficiency_threshold', '2.0')
        monkeypatch.setenv('ieasyhydroforecast_accuracy_threshold', '0.0')
        monkeypatch.setenv('ieasyhydroforecast_nse_threshold', '-1.0')

        _, joint, _ = skill_metrics.calculate_skill_metrics_pentad(
            observed, simulated
        )

        assert any(joint['model_short'] == 'EM')

        em_rows = joint[joint['model_short'] == 'EM']
        for _, row in em_rows.iterrows():
            individual = joint[
                (joint['date'] == row['date'])
                & (joint['code'] == row['code'])
                & (joint['model_short'].isin(['MA', 'MB']))
            ]['forecasted_discharge']
            assert row['forecasted_discharge'] == pytest.approx(
                individual.mean(), abs=1e-5
            )

    def test_perfect_forecast(self, observed, simulated):
        """Perfect forecasts produce sdivsigma=0, nse=1, mae=0, acc=1."""
        perfect = simulated.copy()
        perfect['forecasted_discharge'] = np.tile(
            [10.0, 12.0, 10.0, 12.0, 20.0, 22.0, 20.0, 22.0], 2
        )
        skill_stats, _, _ = skill_metrics.calculate_skill_metrics_pentad(
            observed, perfect
        )
        for _, row in skill_stats.iterrows():
            assert row['sdivsigma'] == pytest.approx(0.0, abs=1e-5)
            assert row['nse'] == pytest.approx(1.0, abs=1e-5)
            assert row['mae'] == pytest.approx(0.0, abs=1e-5)
            assert row['accuracy'] == pytest.approx(1.0, abs=1e-5)

    def test_timing_stats_integration(self, observed, simulated):
        """Timing stats object is passed through and returned."""
        class MockTimingStats:
            def __init__(self):
                self.sections = []
            def start(self, section):
                self.sections.append(f"start_{section}")
            def end(self, section):
                self.sections.append(f"end_{section}")

        ts = MockTimingStats()
        _, _, returned = skill_metrics.calculate_skill_metrics_pentad(
            observed, simulated, ts
        )
        assert len(ts.sections) > 0
        assert returned is ts
