"""Tests for the canonical CRPS estimator in probabilistic_metrics.py.

Milestone M4 (design decision D3, findings #5 + #6) of the postprocessing
skill-correctness campaign
(doc/plans/postprocessing_skill_correctness_design.md). Both
postprocessing_forecasts.skill_metrics.calculate_crps and
forecast_skill_eval.prob_metrics.crps_from_quantiles delegate to the
functions tested here, so a correctness fix or regression here affects both
apps identically.
"""

import math

import numpy as np
import pytest
from probabilistic_metrics import crps_from_quantiles, crps_single

QUANTILE_LEVELS = np.array([0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95])


class TestCrpsSingleDeterministicBand:
    """Deterministic (point) quantile band: CRPS must equal |obs - q|.

    This is the textbook property that the OLD postprocessing_forecasts
    estimator got wrong (it returned ~half this value because it omitted
    the factor-2 term and the flat-tail terms).
    """

    def test_observed_above_band_equals_abs_diff(self):
        result = crps_single(QUANTILE_LEVELS, [100.0] * 7, 130.0)
        assert result == pytest.approx(30.0, abs=1e-9)

    def test_observed_below_band_equals_abs_diff(self):
        result = crps_single(QUANTILE_LEVELS, [100.0] * 7, 70.0)
        assert result == pytest.approx(30.0, abs=1e-9)

    def test_observed_at_band_is_zero(self):
        result = crps_single(QUANTILE_LEVELS, [100.0] * 7, 100.0)
        assert result == pytest.approx(0.0, abs=1e-9)

    def test_property_independent_of_specific_levels(self):
        """|obs - q| holds for any monotonic level grid, not just the
        7-quantile SAPPHIRE grid."""
        result = crps_single([0.25, 0.75], [50.0, 50.0], 70.0)
        assert result == pytest.approx(20.0, abs=1e-9)


class TestCrpsSingleNaNHandling:
    def test_nan_observed_returns_nan(self):
        assert math.isnan(crps_single(QUANTILE_LEVELS, [100.0] * 7, float("nan")))

    def test_fewer_than_two_finite_nodes_returns_nan(self):
        # Only one finite quantile node (q50) survives isotonic repair.
        levels = [0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95]
        quantiles = [np.nan, np.nan, np.nan, 100.0, np.nan, np.nan, np.nan]
        assert math.isnan(crps_single(levels, quantiles, 100.0))

    def test_single_nan_node_among_many_is_dropped_not_poisoning(self):
        """A partially-NaN quantile row still scores using its remaining
        finite nodes (isotonic repair), rather than propagating NaN."""
        levels = [0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95]
        quantiles = [80.0, 85.0, 92.0, 100.0, np.nan, 115.0, 120.0]
        result = crps_single(levels, quantiles, 100.0)
        assert math.isfinite(result)
        assert result >= 0.0


class TestCrpsFromQuantilesBatchedMatchesSingle:
    """The (N,K) aggregator must reduce to crps_single for N=1, and to a
    plain mean of per-row crps_single scores for N>1 with no invalid rows —
    this is the "identical value at both call sites" contract (M4 test a)."""

    def test_single_row_matches_crps_single(self):
        observed = np.array([130.0])
        quantile_forecasts = np.array([[100.0] * 7])
        batched = crps_from_quantiles(observed, quantile_forecasts, QUANTILE_LEVELS)
        single = crps_single(QUANTILE_LEVELS, quantile_forecasts[0], observed[0])
        assert batched == pytest.approx(single, rel=1e-12)

    def test_deterministic_band_batched_equals_abs_diff(self):
        """Same textbook-expectation case as the single-pair test, but via
        the vectorised entry point used by postprocessing_forecasts."""
        observed = np.array([130.0])
        quantile_forecasts = np.array([[100.0] * 7])
        result = crps_from_quantiles(observed, quantile_forecasts, QUANTILE_LEVELS)
        assert result == pytest.approx(30.0, abs=1e-9)

    def test_multi_row_mean_of_identical_rows_equals_single_row(self):
        observed = np.array([130.0, 130.0, 130.0])
        quantile_forecasts = np.array([[100.0] * 7] * 3)
        result = crps_from_quantiles(observed, quantile_forecasts, QUANTILE_LEVELS)
        assert result == pytest.approx(30.0, abs=1e-9)


class TestCrpsFromQuantilesNaNQuantilePoisoning:
    """Regression tests for #6: a single bad row must not null the whole
    group's mean CRPS."""

    def test_one_all_nan_quantile_row_does_not_poison_group_mean(self):
        # Row 0: perfectly valid (score = 0.0, obs matches all quantiles).
        # Row 1: every quantile is NaN -> that row alone is unscoreable.
        observed = np.array([100.0, 100.0])
        quantile_forecasts = np.array(
            [
                [100.0] * 7,
                [np.nan] * 7,
            ]
        )
        result = crps_from_quantiles(observed, quantile_forecasts, QUANTILE_LEVELS)
        assert math.isfinite(result), (
            "A single fully-NaN quantile row must not null the whole group's CRPS"
        )
        assert result == pytest.approx(0.0, abs=1e-9)

    def test_valid_rows_score_correctly_alongside_a_bad_row(self):
        observed = np.array([130.0, 100.0])
        quantile_forecasts = np.array(
            [
                [100.0] * 7,  # deterministic band, |130-100| = 30.0
                [np.nan] * 7,  # unscoreable row
            ]
        )
        result = crps_from_quantiles(observed, quantile_forecasts, QUANTILE_LEVELS)
        assert result == pytest.approx(30.0, abs=1e-9)

    def test_nan_observation_still_excluded(self):
        observed = np.array([100.0, np.nan])
        quantile_forecasts = np.array(
            [
                [80, 85, 92, 100, 108, 115, 120],
                [80, 85, 92, 100, 108, 115, 120],
            ],
            dtype=float,
        )
        valid_only = crps_from_quantiles(np.array([100.0]), quantile_forecasts[:1], QUANTILE_LEVELS)
        with_nan_obs = crps_from_quantiles(observed, quantile_forecasts, QUANTILE_LEVELS)
        assert with_nan_obs == pytest.approx(valid_only, rel=1e-10)

    def test_all_rows_invalid_returns_nan(self):
        observed = np.array([np.nan, np.nan])
        quantile_forecasts = np.array(
            [
                [np.nan] * 7,
                [np.nan] * 7,
            ]
        )
        result = crps_from_quantiles(observed, quantile_forecasts, QUANTILE_LEVELS)
        assert math.isnan(result)
