"""Tests for CRPS (Continuous Ranked Probability Score) implementation.

Step 4 of Phase 4a: Monthly skill metrics.
TDD — tests written before implementation.
"""

import os
import sys

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.skill_metrics import calculate_crps

# Standard quantile levels used in SAPPHIRE long-term forecasts
QUANTILE_LEVELS = np.array([0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95])


class TestCrpsBasic:
    """Basic CRPS behavior tests."""

    def test_perfect_forecast_crps_zero(self):
        """When all quantiles equal the observation, CRPS = 0."""
        observed = np.array([100.0])
        # All quantiles predict exactly 100.0
        quantile_forecasts = np.array([[100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0]])
        result = calculate_crps(observed, quantile_forecasts, QUANTILE_LEVELS)
        assert result == pytest.approx(0.0, abs=1e-10)

    def test_crps_is_non_negative(self):
        """CRPS is always >= 0."""
        observed = np.array([100.0, 120.0, 80.0])
        quantile_forecasts = np.array(
            [
                [70, 80, 90, 100, 110, 120, 130],
                [90, 100, 110, 120, 130, 140, 150],
                [50, 60, 70, 80, 90, 100, 110],
            ],
            dtype=float,
        )
        result = calculate_crps(observed, quantile_forecasts, QUANTILE_LEVELS)
        assert result >= 0.0

    def test_worse_forecast_higher_crps(self):
        """A forecast further from observations has higher CRPS."""
        observed = np.array([100.0, 100.0, 100.0])

        # Good forecast: quantiles centered around 100
        good_q = np.array(
            [
                [80, 85, 92, 100, 108, 115, 120],
                [80, 85, 92, 100, 108, 115, 120],
                [80, 85, 92, 100, 108, 115, 120],
            ],
            dtype=float,
        )

        # Bad forecast: quantiles centered around 200 (biased)
        bad_q = np.array(
            [
                [180, 185, 192, 200, 208, 215, 220],
                [180, 185, 192, 200, 208, 215, 220],
                [180, 185, 192, 200, 208, 215, 220],
            ],
            dtype=float,
        )

        crps_good = calculate_crps(observed, good_q, QUANTILE_LEVELS)
        crps_bad = calculate_crps(observed, bad_q, QUANTILE_LEVELS)
        assert crps_bad > crps_good

    def test_wider_spread_higher_crps_when_centered(self):
        """With same median, wider quantile spread gives higher CRPS.

        A wider distribution is less informative even if centered correctly.
        """
        observed = np.array([100.0])

        # Narrow spread: quantiles close to 100
        narrow_q = np.array([[95, 96, 98, 100, 102, 104, 105]], dtype=float)

        # Wide spread: quantiles far from 100
        wide_q = np.array([[50, 60, 75, 100, 125, 140, 150]], dtype=float)

        crps_narrow = calculate_crps(observed, narrow_q, QUANTILE_LEVELS)
        crps_wide = calculate_crps(observed, wide_q, QUANTILE_LEVELS)
        assert crps_wide > crps_narrow

    def test_multiple_observations_averaged(self):
        """CRPS is averaged across all observation-forecast pairs."""
        # Two identical obs with identical quantiles -> same as single
        observed_single = np.array([100.0])
        q_single = np.array([[80, 85, 92, 100, 108, 115, 120]], dtype=float)

        observed_double = np.array([100.0, 100.0])
        q_double = np.array(
            [
                [80, 85, 92, 100, 108, 115, 120],
                [80, 85, 92, 100, 108, 115, 120],
            ],
            dtype=float,
        )

        crps_single = calculate_crps(observed_single, q_single, QUANTILE_LEVELS)
        crps_double = calculate_crps(observed_double, q_double, QUANTILE_LEVELS)
        assert crps_single == pytest.approx(crps_double, rel=1e-10)


class TestCrpsHandCalculated:
    """Hand-calculated CRPS values for verification.

    The quantile (pinball) loss for level tau and error u = y - q is:
        rho_tau(u) = u * (tau - 1{u < 0})
                   = u * tau        if u >= 0  (observation above quantile)
                   = u * (tau - 1)  if u < 0   (observation below quantile)

    CRPS is approximated via trapezoidal integration of pinball losses
    across quantile levels.
    """

    def test_single_observation_above_all_quantiles(self):
        """Observation above all quantile forecasts.

        observed = 200, all quantiles = 100
        For each level tau: u = 200 - 100 = 100 > 0
            rho_tau(100) = 100 * tau

        Pinball losses at each tau:
            tau=0.05: 100*0.05 = 5.0
            tau=0.10: 100*0.10 = 10.0
            tau=0.25: 100*0.25 = 25.0
            tau=0.50: 100*0.50 = 50.0
            tau=0.75: 100*0.75 = 75.0
            tau=0.90: 100*0.90 = 90.0
            tau=0.95: 100*0.95 = 95.0

        Trapezoidal integration over [0.05, 0.95]:
            = sum of trapezoids between consecutive tau levels
        """
        observed = np.array([200.0])
        quantile_forecasts = np.array([[100.0] * 7])

        result = calculate_crps(observed, quantile_forecasts, QUANTILE_LEVELS)

        # Calculate expected via trapezoidal integration
        taus = QUANTILE_LEVELS
        losses = 100.0 * taus  # all u > 0 so rho = u * tau
        expected = np.trapezoid(losses, taus)

        assert result == pytest.approx(expected, rel=1e-6)

    def test_single_observation_below_all_quantiles(self):
        """Observation below all quantile forecasts.

        observed = 0, all quantiles = 100
        For each level tau: u = 0 - 100 = -100 < 0
            rho_tau(-100) = -100 * (tau - 1) = 100 * (1 - tau)

        Pinball losses:
            tau=0.05: 100*0.95 = 95.0
            tau=0.10: 100*0.90 = 90.0
            tau=0.25: 100*0.75 = 75.0
            tau=0.50: 100*0.50 = 50.0
            tau=0.75: 100*0.25 = 25.0
            tau=0.90: 100*0.10 = 10.0
            tau=0.95: 100*0.05 = 5.0
        """
        observed = np.array([0.0])
        quantile_forecasts = np.array([[100.0] * 7])

        result = calculate_crps(observed, quantile_forecasts, QUANTILE_LEVELS)

        taus = QUANTILE_LEVELS
        losses = 100.0 * (1.0 - taus)  # all u < 0 so rho = |u| * (1 - tau)
        expected = np.trapezoid(losses, taus)

        assert result == pytest.approx(expected, rel=1e-6)

    def test_symmetric_quantiles_observation_at_median(self):
        """Symmetric quantile distribution with observation at median.

        observed = 100
        quantiles = [60, 70, 85, 100, 115, 130, 140]

        For each (tau, q):
            tau=0.05, q=60:  u=40, rho = 40*0.05 = 2.0
            tau=0.10, q=70:  u=30, rho = 30*0.10 = 3.0
            tau=0.25, q=85:  u=15, rho = 15*0.25 = 3.75
            tau=0.50, q=100: u=0,  rho = 0
            tau=0.75, q=115: u=-15, rho = -15*(0.75-1) = 15*0.25 = 3.75
            tau=0.90, q=130: u=-30, rho = -30*(0.90-1) = 30*0.10 = 3.0
            tau=0.95, q=140: u=-40, rho = -40*(0.95-1) = 40*0.05 = 2.0
        """
        observed = np.array([100.0])
        quantile_forecasts = np.array([[60.0, 70.0, 85.0, 100.0, 115.0, 130.0, 140.0]])
        result = calculate_crps(observed, quantile_forecasts, QUANTILE_LEVELS)

        losses = np.array([2.0, 3.0, 3.75, 0.0, 3.75, 3.0, 2.0])
        expected = np.trapezoid(losses, QUANTILE_LEVELS)

        assert result == pytest.approx(expected, rel=1e-6)


class TestCrpsEdgeCases:
    """Edge cases for CRPS."""

    def test_single_observation(self):
        """Works with a single observation."""
        observed = np.array([50.0])
        quantile_forecasts = np.array([[30.0, 35.0, 42.0, 50.0, 58.0, 65.0, 70.0]])
        result = calculate_crps(observed, quantile_forecasts, QUANTILE_LEVELS)
        assert isinstance(result, float)
        assert result >= 0.0

    def test_equal_quantiles_observation_matches(self):
        """All quantiles equal and match observation -> CRPS = 0."""
        observed = np.array([42.0, 42.0])
        quantile_forecasts = np.array(
            [
                [42.0] * 7,
                [42.0] * 7,
            ]
        )
        result = calculate_crps(observed, quantile_forecasts, QUANTILE_LEVELS)
        assert result == pytest.approx(0.0, abs=1e-10)

    def test_zero_observed_value(self):
        """Zero observation is valid (not missing)."""
        observed = np.array([0.0])
        quantile_forecasts = np.array([[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]])
        result = calculate_crps(observed, quantile_forecasts, QUANTILE_LEVELS)
        assert result == pytest.approx(0.0, abs=1e-10)

    def test_very_large_values(self):
        """Large discharge values don't cause overflow."""
        observed = np.array([15000.0])
        quantile_forecasts = np.array(
            [[12000, 13000, 14000, 15000, 16000, 17000, 18000]],
            dtype=float,
        )
        result = calculate_crps(observed, quantile_forecasts, QUANTILE_LEVELS)
        assert np.isfinite(result)
        assert result >= 0.0

    def test_very_small_positive_values(self):
        """Very small positive values work correctly."""
        observed = np.array([0.001])
        quantile_forecasts = np.array([[0.0005, 0.0006, 0.0008, 0.001, 0.0012, 0.0014, 0.0015]])
        result = calculate_crps(observed, quantile_forecasts, QUANTILE_LEVELS)
        assert np.isfinite(result)
        assert result >= 0.0

    def test_many_observations(self):
        """Works with many observation-forecast pairs."""
        n = 1000
        rng = np.random.RandomState(42)
        observed = rng.uniform(50, 200, size=n)
        quantile_forecasts = np.column_stack(
            [observed + rng.normal(0, 10, size=n) * (tau - 0.5) * 2 for tau in QUANTILE_LEVELS]
        )
        result = calculate_crps(observed, quantile_forecasts, QUANTILE_LEVELS)
        assert np.isfinite(result)
        assert result >= 0.0

    def test_two_quantile_levels(self):
        """Works with minimal quantile levels (K=2)."""
        levels = np.array([0.25, 0.75])
        observed = np.array([100.0])
        quantile_forecasts = np.array([[90.0, 110.0]])
        result = calculate_crps(observed, quantile_forecasts, levels)
        assert np.isfinite(result)
        assert result >= 0.0

    def test_observation_nan_excluded(self):
        """NaN observations are excluded from the mean.

        With 2 valid + 1 NaN observation, result equals the 2-valid mean.
        """
        # Two valid observations
        observed_valid = np.array([100.0, 100.0])
        q_valid = np.array(
            [
                [80, 85, 92, 100, 108, 115, 120],
                [80, 85, 92, 100, 108, 115, 120],
            ],
            dtype=float,
        )
        crps_valid = calculate_crps(observed_valid, q_valid, QUANTILE_LEVELS)

        # Same two + one NaN
        observed_with_nan = np.array([100.0, 100.0, np.nan])
        q_with_nan = np.array(
            [
                [80, 85, 92, 100, 108, 115, 120],
                [80, 85, 92, 100, 108, 115, 120],
                [80, 85, 92, 100, 108, 115, 120],  # ignored
            ],
            dtype=float,
        )
        crps_with_nan = calculate_crps(observed_with_nan, q_with_nan, QUANTILE_LEVELS)
        assert crps_valid == pytest.approx(crps_with_nan, rel=1e-10)

    def test_all_nan_observations_returns_nan(self):
        """All NaN observations -> return NaN."""
        observed = np.array([np.nan, np.nan])
        quantile_forecasts = np.array(
            [
                [80, 85, 92, 100, 108, 115, 120],
                [80, 85, 92, 100, 108, 115, 120],
            ],
            dtype=float,
        )
        result = calculate_crps(observed, quantile_forecasts, QUANTILE_LEVELS)
        assert np.isnan(result)

    def test_nan_in_quantile_forecasts_returns_nan(self):
        """NaN in quantile forecasts propagates to NaN CRPS.

        When some quantile values are NaN (e.g., model only produces q50
        but not q05-q95), the pinball loss is NaN for those quantiles,
        and trapezoidal integration propagates NaN to the final result.
        Callers (calculate_monthly_skill_metrics) rely on this behavior
        to detect incomplete quantile distributions.
        """
        observed = np.array([100.0])
        quantile_forecasts = np.array([[np.nan, np.nan, np.nan, 100.0, np.nan, np.nan, np.nan]])
        result = calculate_crps(observed, quantile_forecasts, QUANTILE_LEVELS)
        assert np.isnan(result)

    def test_negative_values(self):
        """CRPS works with negative observed and forecasted values.

        While discharge can't be negative, the function is a generic
        scoring rule and should handle any real-valued inputs.
        """
        observed = np.array([-10.0])
        quantile_forecasts = np.array([[-20.0, -15.0, -12.0, -10.0, -8.0, -5.0, 0.0]])
        result = calculate_crps(observed, quantile_forecasts, QUANTILE_LEVELS)
        assert np.isfinite(result)
        assert result >= 0.0
