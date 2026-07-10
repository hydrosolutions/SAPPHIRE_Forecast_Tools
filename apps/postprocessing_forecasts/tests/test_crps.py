"""Tests for CRPS (Continuous Ranked Probability Score) implementation.

Step 4 of Phase 4a: Monthly skill metrics.
TDD — tests written before implementation.

M4 (design decision D3, findings #5 + #6, postprocessing skill-correctness
campaign): calculate_crps now delegates to the canonical, textbook
crps_from_quantiles in iEasyHydroForecast.probabilistic_metrics (factor-2
trapezoidal integration + explicit flat-tail terms). The previous estimator
omitted the factor-2 term and the tail terms, so it returned roughly HALF
the correct CRPS — the hand-calculated expectations in
TestCrpsHandCalculated below are re-baselined accordingly (see each test's
docstring for the old vs. new value). It also only masked NaN
*observations*, not rows with NaN *quantiles*, so a single incomplete
quantile row poisoned the whole group's mean via plain np.mean — see
TestCrpsNaNQuantilePoisoning for the regression coverage of that fix (#6).
"""

import os
import sys

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

from probabilistic_metrics import crps_from_quantiles as shared_crps_from_quantiles
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
        """Observation above a DETERMINISTIC (point) quantile band.

        observed = 200, all quantiles = 100.

        Textbook property: for a point band, CRPS = |obs - q| exactly,
        independent of the specific quantile grid — because the flat-tail
        terms and the factor-2 middle term combine algebraically to cancel
        every level-dependent term. So expected = |200 - 100| = 100.0.

        RE-BASELINED (M4/D3/#5): the OLD estimator omitted the factor-2 term
        and the flat-tail terms and only integrated the middle trapezoid,
        giving trapz(100*tau, tau) = 45.0 — roughly HALF the correct value.
        """
        observed = np.array([200.0])
        quantile_forecasts = np.array([[100.0] * 7])

        result = calculate_crps(observed, quantile_forecasts, QUANTILE_LEVELS)

        assert result == pytest.approx(100.0, rel=1e-9)

    def test_single_observation_below_all_quantiles(self):
        """Observation below a DETERMINISTIC (point) quantile band.

        observed = 0, all quantiles = 100 -> expected = |0 - 100| = 100.0
        (same textbook point-band property as the test above).

        RE-BASELINED (M4/D3/#5): the OLD estimator gave
        trapz(100*(1-tau), tau) = 45.0 — roughly HALF the correct value.
        """
        observed = np.array([0.0])
        quantile_forecasts = np.array([[100.0] * 7])

        result = calculate_crps(observed, quantile_forecasts, QUANTILE_LEVELS)

        assert result == pytest.approx(100.0, rel=1e-9)

    def test_symmetric_quantiles_observation_at_median(self):
        """Symmetric quantile distribution with observation at median.

        observed = 100
        quantiles = [60, 70, 85, 100, 115, 130, 140]

        Per-node pinball losses (unchanged by the estimator fix — only the
        AGGREGATION changed):
            tau=0.05, q=60:  u=40, rho = 40*0.05 = 2.0
            tau=0.10, q=70:  u=30, rho = 30*0.10 = 3.0
            tau=0.25, q=85:  u=15, rho = 15*0.25 = 3.75
            tau=0.50, q=100: u=0,  rho = 0
            tau=0.75, q=115: u=-15, rho = -15*(0.75-1) = 15*0.25 = 3.75
            tau=0.90, q=130: u=-30, rho = -30*(0.90-1) = 30*0.10 = 3.0
            tau=0.95, q=140: u=-40, rho = -40*(0.95-1) = 40*0.05 = 2.0

        middle = trapz(losses, taus) = 2.2 (this is the OLD estimator's
        entire result — RE-BASELINED below, M4/D3/#5).

        Textbook estimator adds the flat tails and the factor 2:
            left_tail  = (obs - q_min) * tau_min^2 / 2
                       = (100-60) * 0.05^2 / 2 = 0.05
            right_tail = (q_max - obs) * (1-tau_max)^2 / 2
                       = (140-100) * 0.05^2 / 2 = 0.05
            CRPS = 2 * (0.05 + 2.2 + 0.05) = 2 * 2.3 = 4.6
        """
        observed = np.array([100.0])
        quantile_forecasts = np.array([[60.0, 70.0, 85.0, 100.0, 115.0, 130.0, 140.0]])
        result = calculate_crps(observed, quantile_forecasts, QUANTILE_LEVELS)

        assert result == pytest.approx(4.6, rel=1e-6)

    def test_deterministic_band_matches_textbook_abs_diff(self):
        """Locked M4 regression test (task spec 'the deterministic band'
        case): all quantiles == 100.0, observed == 130.0 -> CRPS == 30.0.

        The OLD (WRONG) postprocessing estimator returned ~13.5 for this
        input (trapz(30*tau, tau) over the 7-level SAPPHIRE grid, no
        factor-2, no tails) — exactly half the correct answer plus the
        missing tail contribution. This is the textbook sanity check that
        motivated milestone M4 (D3/#5).
        """
        observed = np.array([130.0])
        quantile_forecasts = np.array([[100.0] * 7])

        result = calculate_crps(observed, quantile_forecasts, QUANTILE_LEVELS)

        assert result == pytest.approx(30.0, abs=1e-9)

    def test_matches_shared_canonical_estimator(self):
        """calculate_crps must delegate to (produce identical values to) the
        canonical iEasyHydroForecast.probabilistic_metrics.crps_from_quantiles
        — the M4 'same value at both call sites' contract. forecast_skill_eval
        delegates to the same shared primitive (see
        forecast_skill_eval/tests/test_prob_metrics.py), so this indirectly
        proves both apps agree.
        """
        observed = np.array([100.0, 130.0, 80.0])
        quantile_forecasts = np.array(
            [
                [80, 85, 92, 100, 108, 115, 120],
                [100.0] * 7,
                [60, 65, 72, 80, 88, 95, 100],
            ],
            dtype=float,
        )

        result = calculate_crps(observed, quantile_forecasts, QUANTILE_LEVELS)
        expected = shared_crps_from_quantiles(observed, quantile_forecasts, QUANTILE_LEVELS)

        assert result == pytest.approx(expected, rel=1e-12)


class TestCrpsNaNQuantilePoisoning:
    """Regression tests for #6: a single row with NaN quantiles must not
    null the whole group's mean CRPS (the OLD estimator masked NaN
    *observations* but not NaN *quantile rows*, so np.mean(crps_per_obs)
    propagated a single bad row's NaN to the entire group)."""

    def test_one_all_nan_quantile_row_does_not_poison_group(self):
        # Row 0: perfectly valid, observed matches every quantile -> CRPS 0.
        # Row 1: every quantile is NaN -> unscoreable on its own, but must
        # not null row 0's contribution to the group mean.
        observed = np.array([100.0, 100.0])
        quantile_forecasts = np.array(
            [
                [100.0] * 7,
                [np.nan] * 7,
            ]
        )
        result = calculate_crps(observed, quantile_forecasts, QUANTILE_LEVELS)
        assert np.isfinite(result), (
            "A single fully-NaN quantile row must not poison the group's CRPS mean"
        )
        assert result == pytest.approx(0.0, abs=1e-9)

    def test_valid_row_scores_correctly_alongside_a_nan_quantile_row(self):
        observed = np.array([130.0, 100.0])
        quantile_forecasts = np.array(
            [
                [100.0] * 7,  # deterministic band, |130-100| = 30.0
                [np.nan] * 7,  # unscoreable
            ]
        )
        result = calculate_crps(observed, quantile_forecasts, QUANTILE_LEVELS)
        assert result == pytest.approx(30.0, abs=1e-9)


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
