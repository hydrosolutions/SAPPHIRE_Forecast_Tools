"""Unit tests for informational metrics: pbias, _kge, kge_lf, nse_log.

These metrics are diagnostic (not used for ensemble selection).
See Phase 4c plan for rationale.
"""

import os
import sys

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from src.skill_metrics import (
    METRIC_ORDER,
    METRIC_REGISTRY,
    THRESHOLD_METRICS,
    _kge,
    calculate_all_skill_metrics,
    kge_lf,
    nse_log,
    pbias,
)

# ===================================================================
# TestPbias
# ===================================================================


class TestPbias:
    """PBIAS: 100 * SUM(obs - sim) / SUM(obs)."""

    def test_perfect_forecast(self):
        """Perfect forecast => PBIAS = 0."""
        obs = np.array([10.0, 20.0, 30.0])
        sim = np.array([10.0, 20.0, 30.0])
        assert pbias(obs, sim) == pytest.approx(0.0)

    def test_underestimation(self):
        """sim < obs => PBIAS > 0 (positive = underestimation)."""
        obs = np.array([100.0, 100.0])
        sim = np.array([80.0, 80.0])
        result = pbias(obs, sim)
        assert result == pytest.approx(20.0)

    def test_overestimation(self):
        """sim > obs => PBIAS < 0 (negative = overestimation)."""
        obs = np.array([100.0, 100.0])
        sim = np.array([120.0, 120.0])
        result = pbias(obs, sim)
        assert result == pytest.approx(-20.0)

    def test_all_zero_obs(self):
        """All-zero obs => NaN (division by zero guard)."""
        obs = np.array([0.0, 0.0, 0.0])
        sim = np.array([1.0, 2.0, 3.0])
        assert np.isnan(pbias(obs, sim))

    def test_single_point(self):
        """<2 points => NaN."""
        obs = np.array([10.0])
        sim = np.array([12.0])
        assert np.isnan(pbias(obs, sim))

    def test_empty_array(self):
        """Empty arrays => NaN."""
        assert np.isnan(pbias(np.array([]), np.array([])))

    def test_exactly_2_points(self):
        """Boundary: exactly min_points=2 computes normally."""
        obs = np.array([100.0, 200.0])
        sim = np.array([90.0, 210.0])
        # PBIAS = 100 * (300 - 300) / 300 = 0
        assert pbias(obs, sim) == pytest.approx(0.0)

    def test_hand_calculated(self):
        """Hand-calculated: obs=[10,20,30], sim=[12,18,33].
        PBIAS = 100*(60-63)/60 = -5.0
        """
        obs = np.array([10.0, 20.0, 30.0])
        sim = np.array([12.0, 18.0, 33.0])
        assert pbias(obs, sim) == pytest.approx(-5.0)

    def test_negative_sim(self):
        """Negative sim values: formula still works (ML model edge case)."""
        obs = np.array([10.0, 20.0])
        sim = np.array([-5.0, 25.0])
        # PBIAS = 100 * (30 - 20) / 30 = 33.333...
        assert pbias(obs, sim) == pytest.approx(100.0 / 3.0)


# ===================================================================
# TestKge
# ===================================================================


class TestKge:
    """Internal _kge helper: 1 - sqrt((r-1)^2 + (a-1)^2 + (b-1)^2)."""

    def test_perfect_forecast(self):
        """Perfect forecast => KGE = 1.0."""
        obs = np.array([10.0, 20.0, 30.0, 40.0, 50.0])
        sim = np.array([10.0, 20.0, 30.0, 40.0, 50.0])
        assert _kge(obs, sim) == pytest.approx(1.0)

    def test_constant_obs(self):
        """Constant obs => std=0 => NaN."""
        obs = np.array([5.0, 5.0, 5.0, 5.0])
        sim = np.array([3.0, 4.0, 5.0, 6.0])
        assert np.isnan(_kge(obs, sim))

    def test_constant_sim(self):
        """Constant sim => correlation undefined => NaN."""
        obs = np.array([10.0, 20.0, 30.0, 40.0, 50.0])
        sim = np.array([30.0, 30.0, 30.0, 30.0, 30.0])
        assert np.isnan(_kge(obs, sim))

    def test_single_point(self):
        """<2 points => NaN."""
        obs = np.array([10.0])
        sim = np.array([10.0])
        assert np.isnan(_kge(obs, sim))

    def test_empty_array(self):
        """Empty arrays => NaN."""
        assert np.isnan(_kge(np.array([]), np.array([])))

    def test_exactly_2_points(self):
        """Boundary: exactly 2 points computes (r is +/-1 for 2 points)."""
        obs = np.array([10.0, 20.0])
        sim = np.array([12.0, 22.0])
        # r=1, alpha=std(sim)/std(obs)=1, beta=34/30
        # KGE = 1 - sqrt(0 + 0 + (34/30 - 1)^2) = 1 - 4/30
        result = _kge(obs, sim)
        assert result == pytest.approx(1.0 - 4.0 / 30.0)

    def test_known_value_doubled_variability(self):
        """Known decomposition: r=1, alpha=2, beta=1 => KGE = 0.0."""
        obs = np.array([10.0, 20.0, 30.0])
        # Double the variability: sim = 2*(obs - mean) + mean
        mean_obs = 20.0
        sim = 2.0 * (obs - mean_obs) + mean_obs
        result = _kge(obs, sim)
        # r=1, alpha=2, beta=1 => KGE = 1 - sqrt(0 + 1 + 0) = 0.0
        assert result == pytest.approx(0.0, abs=1e-10)

    def test_anti_correlated(self):
        """Reversed sim => r=-1, alpha=1, beta=1 => KGE = -1.0."""
        obs = np.array([10.0, 20.0, 30.0, 40.0, 50.0])
        sim = np.array([50.0, 40.0, 30.0, 20.0, 10.0])
        assert _kge(obs, sim) == pytest.approx(-1.0)


# ===================================================================
# TestKgeLf
# ===================================================================


class TestKgeLf:
    """KGElf: average of KGE(Q) and KGE(1/(Q+eps))."""

    def test_perfect_forecast(self):
        """Perfect forecast => KGElf = 1.0."""
        obs = np.arange(10.0, 25.0)  # 15 points
        sim = obs.copy()
        assert kge_lf(obs, sim) == pytest.approx(1.0)

    def test_fewer_than_10_points(self):
        """<10 points => NaN."""
        obs = np.array([10.0, 20.0, 30.0])
        sim = np.array([10.0, 20.0, 30.0])
        assert np.isnan(kge_lf(obs, sim))

    def test_exactly_9_points(self):
        """9 points => NaN (just below threshold)."""
        obs = np.arange(10.0, 19.0)
        sim = obs.copy()
        assert np.isnan(kge_lf(obs, sim))

    def test_exactly_10_points(self):
        """10 points => computes (at threshold boundary)."""
        obs = np.arange(10.0, 20.0)
        sim = obs.copy()
        assert kge_lf(obs, sim) == pytest.approx(1.0)

    def test_zero_mean_obs(self):
        """Zero mean obs => NaN."""
        obs = np.zeros(15)
        sim = np.ones(15)
        assert np.isnan(kge_lf(obs, sim))

    def test_constant_obs_returns_nan(self):
        """Constant obs => std=0 in _kge => NaN.

        This is distinct from the epsilon test: here we verify that
        constant obs causes NaN via the _kge std-check, not via epsilon.
        """
        obs = np.full(15, 10.0)
        sim = np.arange(3.0, 18.0)
        assert np.isnan(kge_lf(obs, sim))

    def test_negative_sim_returns_nan(self):
        """Negative sim values where sim + eps <= 0 => NaN.

        ML models can produce negative discharge predictions.
        """
        obs = np.arange(1.0, 16.0)  # 15 points, mean=8
        sim = obs.copy()
        sim[0] = -1.0  # eps = 8/100 = 0.08, so sim+eps = -0.92 < 0
        assert np.isnan(kge_lf(obs, sim))

    def test_hand_calculated(self):
        """12-point deterministic case with exact expected value.

        obs = [5,8,12,15,20,25,30,18,10,7,6,4]
        sim = [6,9,11,14,22,24,28,19,11,8,5,5]
        KGE_direct = 0.939509, KGE_inv = 0.846772
        KGElf = (0.939509 + 0.846772) / 2 = 0.893140
        """
        obs = np.array([5.0, 8.0, 12.0, 15.0, 20.0, 25.0, 30.0, 18.0, 10.0, 7.0, 6.0, 4.0])
        sim = np.array([6.0, 9.0, 11.0, 14.0, 22.0, 24.0, 28.0, 19.0, 11.0, 8.0, 5.0, 5.0])
        assert kge_lf(obs, sim) == pytest.approx(0.893140, abs=1e-4)


# ===================================================================
# TestNseLog
# ===================================================================


class TestNseLog:
    """NSE on log-transformed flows."""

    def test_perfect_forecast(self):
        """Perfect forecast => NSE_log = 1.0."""
        obs = np.array([10.0, 20.0, 30.0, 40.0, 50.0])
        sim = np.array([10.0, 20.0, 30.0, 40.0, 50.0])
        assert nse_log(obs, sim) == pytest.approx(1.0)

    def test_fewer_than_2_points(self):
        """<2 points => NaN."""
        obs = np.array([10.0])
        sim = np.array([10.0])
        assert np.isnan(nse_log(obs, sim))

    def test_empty_array(self):
        """Empty arrays => NaN."""
        assert np.isnan(nse_log(np.array([]), np.array([])))

    def test_exactly_2_points(self):
        """Boundary: exactly 2 points computes normally."""
        obs = np.array([10.0, 100.0])
        sim = np.array([10.0, 100.0])
        assert nse_log(obs, sim) == pytest.approx(1.0)

    def test_zero_mean_obs(self):
        """Zero mean obs => NaN."""
        obs = np.zeros(5)
        sim = np.ones(5)
        assert np.isnan(nse_log(obs, sim))

    def test_constant_log_obs(self):
        """Constant log(obs+eps) => denom=0 => NaN."""
        obs = np.full(5, 10.0)  # constant
        sim = np.array([8.0, 9.0, 10.0, 11.0, 12.0])
        assert np.isnan(nse_log(obs, sim))

    def test_negative_sim_returns_nan(self):
        """Negative sim where sim + eps <= 0 => NaN."""
        obs = np.array([10.0, 20.0, 30.0, 40.0, 50.0])
        sim = np.array([-5.0, 20.0, 30.0, 40.0, 50.0])
        # eps = 30/100 = 0.3, sim[0] + eps = -4.7 < 0
        assert np.isnan(nse_log(obs, sim))

    def test_hand_calculated(self):
        """Deterministic case with exact expected value.

        obs = [5, 10, 20, 40, 80], sim = [6, 9, 22, 38, 75]
        eps = 31/100 = 0.31
        NSE_log = 1 - 0.055724 / 4.618944 = 0.987936
        """
        obs = np.array([5.0, 10.0, 20.0, 40.0, 80.0])
        sim = np.array([6.0, 9.0, 22.0, 38.0, 75.0])
        assert nse_log(obs, sim) == pytest.approx(0.987936, abs=1e-4)

    def test_bad_forecast_produces_negative(self):
        """Very poor forecast => NSE_log << 0."""
        obs = np.array([5.0, 10.0, 20.0, 40.0, 80.0])
        sim = np.array([80.0, 40.0, 20.0, 10.0, 5.0])  # reversed
        result = nse_log(obs, sim)
        assert np.isfinite(result)
        assert result < 0.0


# ===================================================================
# TestNewMetricsInRegistry
# ===================================================================


class TestNewMetricsInRegistry:
    """Verify the 3 new metrics are registered correctly."""

    def test_pbias_in_registry(self):
        assert "pbias" in METRIC_REGISTRY
        assert METRIC_REGISTRY["pbias"]["higher_is_better"] is None
        assert METRIC_REGISTRY["pbias"]["env_var"] is None
        assert METRIC_REGISTRY["pbias"]["default_threshold"] is None

    def test_kgelf_in_registry(self):
        assert "kgelf" in METRIC_REGISTRY
        assert METRIC_REGISTRY["kgelf"]["higher_is_better"] is True
        assert METRIC_REGISTRY["kgelf"]["env_var"] is None
        assert METRIC_REGISTRY["kgelf"]["default_threshold"] is None

    def test_nse_log_in_registry(self):
        assert "nse_log" in METRIC_REGISTRY
        assert METRIC_REGISTRY["nse_log"]["higher_is_better"] is True
        assert METRIC_REGISTRY["nse_log"]["env_var"] is None
        assert METRIC_REGISTRY["nse_log"]["default_threshold"] is None

    def test_metric_order_length(self):
        assert len(METRIC_ORDER) == 9

    def test_new_metrics_not_in_threshold(self):
        """Informational metrics must not appear in THRESHOLD_METRICS."""
        assert "pbias" not in THRESHOLD_METRICS
        assert "kgelf" not in THRESHOLD_METRICS
        assert "nse_log" not in THRESHOLD_METRICS


# ===================================================================
# TestCalculateAllWithNewMetrics
# ===================================================================


class TestCalculateAllWithNewMetrics:
    """Verify new metrics flow through calculate_all_skill_metrics."""

    def test_5_point_happy_path(self):
        """5-point case: pbias+nse_log computed, kgelf=NaN (<10 points)."""
        data = pd.DataFrame(
            {
                "obs": [10.0, 20.0, 30.0, 40.0, 50.0],
                "sim": [12.0, 18.0, 32.0, 38.0, 52.0],
                "delta": [5.0] * 5,
            }
        )
        result = calculate_all_skill_metrics(data, "obs", "sim", "delta")
        assert np.isfinite(result["pbias"])
        assert np.isnan(result["kgelf"])  # <10 points
        assert np.isfinite(result["nse_log"])

    def test_15_point_all_computed(self):
        """15-point case: all 3 new metrics are finite."""
        rng = np.random.RandomState(99)
        obs = rng.uniform(5, 50, size=15)
        sim = obs * rng.uniform(0.9, 1.1, size=15)
        data = pd.DataFrame(
            {
                "obs": obs,
                "sim": sim,
                "delta": np.full(15, 5.0),
            }
        )
        result = calculate_all_skill_metrics(data, "obs", "sim", "delta")
        assert np.isfinite(result["pbias"])
        assert np.isfinite(result["kgelf"])
        assert np.isfinite(result["nse_log"])

    def test_n_lt_2_returns_nan_for_all_new(self):
        """n<2 early return has NaN for all 3 new metrics."""
        data = pd.DataFrame(
            {
                "obs": [10.0],
                "sim": [12.0],
                "delta": [5.0],
            }
        )
        result = calculate_all_skill_metrics(data, "obs", "sim", "delta")
        assert np.isnan(result["pbias"])
        assert np.isnan(result["kgelf"])
        assert np.isnan(result["nse_log"])

    def test_all_nan_input(self):
        """All-NaN obs/sim => NaN for all new metrics."""
        data = pd.DataFrame(
            {
                "obs": [np.nan, np.nan, np.nan],
                "sim": [np.nan, np.nan, np.nan],
                "delta": [5.0, 5.0, 5.0],
            }
        )
        result = calculate_all_skill_metrics(data, "obs", "sim", "delta")
        assert np.isnan(result["pbias"])
        assert np.isnan(result["kgelf"])
        assert np.isnan(result["nse_log"])

    def test_negative_sim_in_pipeline(self):
        """Negative sim flows through without crashing; kgelf/nse_log=NaN."""
        data = pd.DataFrame(
            {
                "obs": [10.0, 20.0, 30.0],
                "sim": [-5.0, 25.0, 28.0],
                "delta": [5.0, 5.0, 5.0],
            }
        )
        result = calculate_all_skill_metrics(data, "obs", "sim", "delta")
        # pbias still computes (formula works with negative sim)
        assert np.isfinite(result["pbias"])
        # nse_log: sim[0]+eps = -5+0.2 = -4.8 < 0 => NaN
        assert np.isnan(result["nse_log"])
