"""Unit tests for calculate_all_skill_metrics() in forecast_library.

Verifies all 6 metric values against hand-calculated results, edge cases
(single point, all-NaN, missing column, constant observations), and
branch logic (n<1, n<2).
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast')
)

import forecast_library as fl


class TestCalculateAllSkillMetricsHappyPath:
    """Verify all 6 metrics against hand-calculated results."""

    def test_five_data_points(self):
        """5 points: verify all 6 metrics against hand calculation.

        obs = [100, 110, 105, 115, 108]
        sim = [102, 108, 106, 112, 110]
        delta = [5, 5, 5, 5, 5]

        differences = [-2, 2, -1, 3, -2]
        abs_diff = [2, 2, 1, 3, 2]

        MAE = 10/5 = 2.0
        n_pairs = 5
        accuracy = 5/5 = 1.0 (all abs_diff <= 5)
        delta = 5.0

        obs_mean = 107.6
        denom_nse = sum((obs - 107.6)^2) = 125.2
        num_nse = sum(diff^2) = 22
        NSE = 1 - 22/125.2 = 0.82428...

        std(obs, ddof=1) = sqrt(125.2/4) = sqrt(31.3) = 5.59464...
        sdivsigma = sqrt(22/4) / std = sqrt(5.5) / 5.59464 = 0.41918...
        """
        data = pd.DataFrame({
            'obs': [100.0, 110.0, 105.0, 115.0, 108.0],
            'sim': [102.0, 108.0, 106.0, 112.0, 110.0],
            'delta': [5.0, 5.0, 5.0, 5.0, 5.0],
        })
        result = fl.calculate_all_skill_metrics(
            data, 'obs', 'sim', 'delta'
        )

        assert result['n_pairs'] == 5
        assert abs(result['mae'] - 2.0) < 1e-10
        assert result['accuracy'] == 1.0
        assert result['delta'] == 5.0
        assert abs(result['nse'] - 0.82428) < 1e-4
        assert abs(result['sdivsigma'] - 0.41918) < 1e-4

    def test_perfect_forecast(self):
        """obs == sim -> MAE=0, accuracy=1, NSE=1, sdivsigma=0."""
        data = pd.DataFrame({
            'obs': [100.0, 110.0, 120.0],
            'sim': [100.0, 110.0, 120.0],
            'delta': [5.0, 5.0, 5.0],
        })
        result = fl.calculate_all_skill_metrics(
            data, 'obs', 'sim', 'delta'
        )
        assert result['mae'] == 0.0
        assert result['accuracy'] == 1.0
        assert result['nse'] == 1.0
        assert result['sdivsigma'] == 0.0
        assert result['n_pairs'] == 3

    def test_partial_accuracy(self):
        """2 of 3 within delta -> accuracy = 2/3.

        obs = [100, 110, 120], sim = [102, 108, 130], delta = [5, 5, 5]
        abs_diff = [2, 2, 10]
        within delta: [True, True, False] -> accuracy = 2/3
        """
        data = pd.DataFrame({
            'obs': [100.0, 110.0, 120.0],
            'sim': [102.0, 108.0, 130.0],
            'delta': [5.0, 5.0, 5.0],
        })
        result = fl.calculate_all_skill_metrics(
            data, 'obs', 'sim', 'delta'
        )
        assert abs(result['accuracy'] - 2.0 / 3.0) < 1e-10
        assert result['n_pairs'] == 3
        # MAE = (2 + 2 + 10) / 3 = 14/3
        assert abs(result['mae'] - 14.0 / 3.0) < 1e-10


class TestCalculateAllSkillMetricsSinglePoint:
    """Single data point: MAE/accuracy calculated, sdivsigma/NSE = NaN."""

    def test_single_point_returns_mae_and_accuracy(self):
        """n=1: MAE and accuracy valid, sdivsigma and NSE are NaN."""
        data = pd.DataFrame({
            'obs': [100.0],
            'sim': [103.0],
            'delta': [5.0],
        })
        result = fl.calculate_all_skill_metrics(
            data, 'obs', 'sim', 'delta'
        )
        assert result['n_pairs'] == 1
        assert result['mae'] == 3.0
        assert result['accuracy'] == 1.0  # 3 <= 5
        assert result['delta'] == 5.0
        assert np.isnan(result['sdivsigma'])
        assert np.isnan(result['nse'])

    def test_single_point_outside_delta(self):
        """n=1, abs_diff > delta: accuracy = 0."""
        data = pd.DataFrame({
            'obs': [100.0],
            'sim': [110.0],
            'delta': [5.0],
        })
        result = fl.calculate_all_skill_metrics(
            data, 'obs', 'sim', 'delta'
        )
        assert result['n_pairs'] == 1
        assert result['mae'] == 10.0
        assert result['accuracy'] == 0.0


class TestCalculateAllSkillMetricsAllNaN:
    """All-NaN input returns nan_result with n_pairs=0."""

    def test_all_nan_observed(self):
        """All NaN in observed -> nan_result."""
        data = pd.DataFrame({
            'obs': [np.nan, np.nan],
            'sim': [100.0, 110.0],
            'delta': [5.0, 5.0],
        })
        result = fl.calculate_all_skill_metrics(
            data, 'obs', 'sim', 'delta'
        )
        assert result['n_pairs'] == 0
        assert np.isnan(result['mae'])
        assert np.isnan(result['nse'])
        assert np.isnan(result['sdivsigma'])
        assert np.isnan(result['accuracy'])
        assert np.isnan(result['delta'])

    def test_all_nan_simulated(self):
        """All NaN in simulated -> nan_result."""
        data = pd.DataFrame({
            'obs': [100.0, 110.0],
            'sim': [np.nan, np.nan],
            'delta': [5.0, 5.0],
        })
        result = fl.calculate_all_skill_metrics(
            data, 'obs', 'sim', 'delta'
        )
        assert result['n_pairs'] == 0
        assert np.isnan(result['mae'])

    def test_all_nan_delta(self):
        """All NaN in delta -> nan_result."""
        data = pd.DataFrame({
            'obs': [100.0, 110.0],
            'sim': [102.0, 108.0],
            'delta': [np.nan, np.nan],
        })
        result = fl.calculate_all_skill_metrics(
            data, 'obs', 'sim', 'delta'
        )
        assert result['n_pairs'] == 0
        assert np.isnan(result['mae'])

    def test_mixed_nan_filters_correctly(self):
        """Some NaN rows filtered, valid rows produce correct metrics.

        Row 0: obs=NaN (filtered)
        Row 1: obs=110, sim=108, delta=5 (kept)
        Row 2: obs=105, sim=106, delta=5 (kept)

        n=2, differences = [2, -1], abs_diff = [2, 1]
        MAE = 1.5, accuracy = 1.0 (both <= 5)
        """
        data = pd.DataFrame({
            'obs': [np.nan, 110.0, 105.0],
            'sim': [102.0, 108.0, 106.0],
            'delta': [5.0, 5.0, 5.0],
        })
        result = fl.calculate_all_skill_metrics(
            data, 'obs', 'sim', 'delta'
        )
        assert result['n_pairs'] == 2
        assert abs(result['mae'] - 1.5) < 1e-10
        assert result['accuracy'] == 1.0


class TestCalculateAllSkillMetricsMissingColumn:
    """Missing required column raises ValueError."""

    def test_missing_observed_col(self):
        """Missing observed column raises ValueError."""
        data = pd.DataFrame({
            'sim': [100.0],
            'delta': [5.0],
        })
        with pytest.raises(ValueError, match="missing required columns"):
            fl.calculate_all_skill_metrics(
                data, 'obs', 'sim', 'delta'
            )

    def test_missing_simulated_col(self):
        """Missing simulated column raises ValueError."""
        data = pd.DataFrame({
            'obs': [100.0],
            'delta': [5.0],
        })
        with pytest.raises(ValueError, match="missing required columns"):
            fl.calculate_all_skill_metrics(
                data, 'obs', 'sim', 'delta'
            )

    def test_missing_delta_col(self):
        """Missing delta column raises ValueError."""
        data = pd.DataFrame({
            'obs': [100.0],
            'sim': [102.0],
        })
        with pytest.raises(ValueError, match="missing required columns"):
            fl.calculate_all_skill_metrics(
                data, 'obs', 'sim', 'delta'
            )


class TestCalculateAllSkillMetricsConstantObservations:
    """Constant observed values -> denominator near zero -> NaN for
    sdivsigma and NSE."""

    def test_constant_observations(self):
        """obs all equal (100, 100, 100): std=0, NSE and sdivsigma -> NaN."""
        data = pd.DataFrame({
            'obs': [100.0, 100.0, 100.0],
            'sim': [102.0, 98.0, 101.0],
            'delta': [5.0, 5.0, 5.0],
        })
        result = fl.calculate_all_skill_metrics(
            data, 'obs', 'sim', 'delta'
        )
        assert result['n_pairs'] == 3
        # MAE = (2 + 2 + 1) / 3 = 5/3
        assert abs(result['mae'] - 5.0 / 3.0) < 1e-10
        assert result['accuracy'] == 1.0  # all within delta
        assert np.isnan(result['sdivsigma']), (
            "Constant observations -> std=0 -> sdivsigma should be NaN"
        )
        assert np.isnan(result['nse']), (
            "Constant observations -> denominator=0 -> NSE should be NaN"
        )

    def test_nearly_constant_observations(self):
        """obs nearly constant (100.0, 100.0001): denominator > 1e-10,
        so sdivsigma and NSE are calculated (not NaN)."""
        data = pd.DataFrame({
            'obs': [100.0, 100.0001],
            'sim': [100.0, 100.0001],
            'delta': [5.0, 5.0],
        })
        result = fl.calculate_all_skill_metrics(
            data, 'obs', 'sim', 'delta'
        )
        assert result['n_pairs'] == 2
        assert result['mae'] == 0.0
        # Perfect forecast -> NSE = 1.0, sdivsigma = 0.0
        assert result['nse'] == 1.0
        assert result['sdivsigma'] == 0.0


class TestCalculateAllSkillMetricsInfValues:
    """Inf values are filtered like NaN."""

    def test_inf_observed_filtered(self):
        """Inf in observed is filtered out."""
        data = pd.DataFrame({
            'obs': [np.inf, 110.0, 105.0],
            'sim': [102.0, 108.0, 106.0],
            'delta': [5.0, 5.0, 5.0],
        })
        result = fl.calculate_all_skill_metrics(
            data, 'obs', 'sim', 'delta'
        )
        assert result['n_pairs'] == 2

    def test_neg_inf_simulated_filtered(self):
        """Negative inf in simulated is filtered out."""
        data = pd.DataFrame({
            'obs': [100.0, 110.0],
            'sim': [-np.inf, 108.0],
            'delta': [5.0, 5.0],
        })
        result = fl.calculate_all_skill_metrics(
            data, 'obs', 'sim', 'delta'
        )
        assert result['n_pairs'] == 1
        assert result['mae'] == 2.0


class TestCalculateAllSkillMetricsReturnType:
    """Verify return type and index names."""

    def test_return_type_is_series(self):
        """Result is a pd.Series with correct index."""
        data = pd.DataFrame({
            'obs': [100.0, 110.0],
            'sim': [102.0, 108.0],
            'delta': [5.0, 5.0],
        })
        result = fl.calculate_all_skill_metrics(
            data, 'obs', 'sim', 'delta'
        )
        assert isinstance(result, pd.Series)
        expected_index = [
            'sdivsigma', 'nse', 'mae', 'n_pairs', 'delta', 'accuracy',
        ]
        assert list(result.index) == expected_index

    def test_empty_dataframe_returns_nan_result(self):
        """Empty DataFrame returns nan_result with n_pairs=0."""
        data = pd.DataFrame(columns=['obs', 'sim', 'delta'])
        result = fl.calculate_all_skill_metrics(
            data, 'obs', 'sim', 'delta'
        )
        assert result['n_pairs'] == 0
        assert np.isnan(result['mae'])
