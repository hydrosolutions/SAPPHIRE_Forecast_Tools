"""Unit tests for Tier 2 daily skill metrics: FHV, FLV, GEV thresholds,
binary contingency, low-flow quantiles, and the combined calculator.

These metrics answer "how well does the model detect floods and droughts?"
See Phase 4d plan for rationale.
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.skill_metrics import (
    DAILY_METRIC_REGISTRY,
    binary_contingency,
    calculate_daily_skill_metrics,
    estimate_return_period_thresholds,
    fdc_fhv,
    fdc_flv,
    lowflow_quantiles,
)

# ===================================================================
# TestFdcFhv
# ===================================================================


class TestFdcFhv:
    """FDC High Volume bias (%), top 2% of sorted discharge."""

    def test_perfect_forecast(self):
        """Identical obs and sim => FHV = 0."""
        rng = np.random.default_rng(42)
        obs = rng.uniform(1, 100, size=200)
        assert fdc_fhv(obs, obs.copy()) == pytest.approx(0.0)

    def test_overestimate_high_flows(self):
        """sim has higher peaks => FHV > 0."""
        rng = np.random.default_rng(42)
        obs = rng.uniform(1, 100, size=200)
        sim = obs.copy()
        # Boost top values
        sim_sorted_idx = np.argsort(sim)[::-1]
        sim[sim_sorted_idx[:4]] *= 2.0
        result = fdc_fhv(obs, sim)
        assert result > 0

    def test_underestimate_high_flows(self):
        """sim has lower peaks => FHV < 0."""
        rng = np.random.default_rng(42)
        obs = rng.uniform(1, 100, size=200)
        sim = obs.copy()
        sim_sorted_idx = np.argsort(sim)[::-1]
        sim[sim_sorted_idx[:4]] *= 0.5
        result = fdc_fhv(obs, sim)
        assert result < 0

    def test_fewer_than_50_points(self):
        """<50 points => NaN."""
        obs = np.arange(1, 50, dtype=float)  # 49 points
        sim = obs.copy()
        assert np.isnan(fdc_fhv(obs, sim))

    def test_exactly_50_points(self):
        """Boundary: exactly 50 points => computes normally."""
        obs = np.arange(1, 51, dtype=float)  # 50 points
        sim = obs.copy()
        result = fdc_fhv(obs, sim)
        assert result == pytest.approx(0.0)

    def test_zero_obs_high(self):
        """All-zero obs top 2% => NaN."""
        obs = np.zeros(100)
        sim = np.ones(100)
        assert np.isnan(fdc_fhv(obs, sim))

    def test_hand_calculated(self):
        """Hand-calculated FHV for simple case.

        100 points, top 2% = top 2 values.
        obs sorted desc: [100, 99, 98, ..., 1]
        sim sorted desc: [200, 198, 196, ..., 2] (obs * 2)
        top 2 obs: [100, 99] => sum = 199
        top 2 sim: [200, 198] => sum = 398
        FHV = 100 * (398 - 199) / 199 = 100%
        """
        obs = np.arange(1, 101, dtype=float)
        sim = obs * 2.0
        result = fdc_fhv(obs, sim)
        assert result == pytest.approx(100.0)

    def test_empty_array(self):
        """Empty arrays => NaN."""
        assert np.isnan(fdc_fhv(np.array([]), np.array([])))


# ===================================================================
# TestFdcFlv
# ===================================================================


class TestFdcFlv:
    """FDC Low Volume bias (%), bottom 30% of log-FDC."""

    def test_perfect_forecast(self):
        """Identical obs and sim => FLV = 0."""
        rng = np.random.default_rng(42)
        obs = rng.uniform(1, 100, size=100)
        assert fdc_flv(obs, obs.copy()) == pytest.approx(0.0)

    def test_fewer_than_10_points(self):
        """<10 points => NaN."""
        obs = np.arange(1, 10, dtype=float)  # 9 points
        sim = obs.copy()
        assert np.isnan(fdc_flv(obs, sim))

    def test_zero_low_flows(self):
        """Low flows with zeros => NaN (log undefined)."""
        obs = np.arange(0, 100, dtype=float)  # includes 0
        sim = obs.copy() + 1
        assert np.isnan(fdc_flv(obs, sim))

    def test_negative_low_flows(self):
        """Negative low flows => NaN."""
        obs = np.arange(-5, 95, dtype=float)
        sim = obs.copy() + 10
        assert np.isnan(fdc_flv(obs, sim))

    def test_hand_calculated(self):
        """Hand-calculated FLV for simple case.

        10 points: obs = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        sim = obs * 2 = [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]
        Bottom 30% = 3 points (floor(10*0.3)=3)
        obs sorted desc: [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
        bottom 3: [3, 2, 1]
        sim sorted desc: [20, 18, 16, 14, 12, 10, 8, 6, 4, 2]
        bottom 3: [4, 2 is wrong... let me trace more carefully]

        Actually bottom 3 of descending = last 3 elements.
        obs desc: [10, 9, 8, 7, 6, 5, 4, 3, 2, 1] -> bottom 3: [3, 2, 1]
        sim desc: [20, 18, 16, 14, 12, 10, 8, 6, 4, 2] -> bottom 3: [6, 4, 2]
        sum(log(sim_low)) = log(6) + log(4) + log(2) = ln(48)
        sum(log(obs_low)) = log(3) + log(2) + log(1) = ln(6)
        FLV = 100 * (ln(48) - ln(6)) / ln(6) = 100 * ln(8) / ln(6)
            = 100 * 2.0794 / 1.7918 ≈ 116.05%
        """
        obs = np.arange(1, 11, dtype=float)
        sim = obs * 2.0
        result = fdc_flv(obs, sim)
        expected = 100.0 * np.log(48.0) / np.log(6.0) - 100.0
        assert result == pytest.approx(expected, rel=1e-4)

    def test_exactly_10_points(self):
        """Boundary: exactly 10 points => computes normally."""
        obs = np.arange(1, 11, dtype=float)
        result = fdc_flv(obs, obs.copy())
        assert result == pytest.approx(0.0)


# ===================================================================
# TestEstimateReturnPeriodThresholds
# ===================================================================


class TestEstimateReturnPeriodThresholds:
    """GEV fit to annual maxima -> return period thresholds."""

    def test_known_gumbel_case(self):
        """Gumbel distribution (shape=0) with known parameters.

        Generate data from Gumbel(loc=100, scale=20).
        2-yr return period = median ≈ loc - scale*ln(ln(2)) ≈ 107.3
        With enough data, fit should be close.
        """
        from scipy.stats import gumbel_r

        rng = np.random.default_rng(42)
        am = gumbel_r.rvs(loc=100, scale=20, size=50, random_state=rng)
        result = estimate_return_period_thresholds(am)
        assert 2 in result
        assert 5 in result
        # 2-yr threshold should be in reasonable range
        assert 80 < result[2] < 130
        # 5-yr threshold should be higher than 2-yr
        assert result[5] > result[2]

    def test_fewer_than_15_years(self):
        """<15 annual maxima => empty dict."""
        am = np.arange(1, 15, dtype=float)  # 14 values
        result = estimate_return_period_thresholds(am)
        assert result == {}

    def test_exactly_15_years(self):
        """Boundary: exactly 15 years => should attempt fit."""
        rng = np.random.default_rng(42)
        am = rng.uniform(50, 200, size=15)
        result = estimate_return_period_thresholds(am)
        # Should return thresholds (may or may not succeed)
        # With random data of 15 points, it should succeed
        assert 2 in result
        assert 5 in result

    def test_constant_maxima(self):
        """Constant values => empty dict (can't fit GEV)."""
        am = np.full(20, 100.0)
        result = estimate_return_period_thresholds(am)
        assert result == {}

    def test_nan_handling(self):
        """NaN values are removed before fitting."""
        rng = np.random.default_rng(42)
        am = rng.uniform(50, 200, size=20)
        am[0] = np.nan
        am[5] = np.nan
        result = estimate_return_period_thresholds(am)
        # 18 valid points >= 15 threshold
        assert 2 in result


# ===================================================================
# TestBinaryContingency
# ===================================================================


class TestBinaryContingency:
    """Binary contingency table + F1/precision/recall/CSI."""

    def test_perfect_detection(self):
        """All events detected, no false alarms."""
        obs = np.array([10, 20, 30, 40, 50], dtype=float)
        sim = np.array([10, 20, 30, 40, 50], dtype=float)
        result = binary_contingency(obs, sim, threshold=30.0)
        assert result["tp"] == 3
        assert result["fp"] == 0
        assert result["fn"] == 0
        assert result["tn"] == 2
        assert result["precision"] == pytest.approx(1.0)
        assert result["recall"] == pytest.approx(1.0)
        assert result["f1"] == pytest.approx(1.0)
        assert result["csi"] == pytest.approx(1.0)

    def test_no_observed_events(self):
        """No observed events => recall undefined (NaN)."""
        obs = np.array([1, 2, 3], dtype=float)
        sim = np.array([10, 20, 30], dtype=float)
        result = binary_contingency(obs, sim, threshold=50.0)
        assert result["tp"] == 0
        assert result["fn"] == 0
        assert np.isnan(result["recall"])
        assert np.isnan(result["f1"])

    def test_all_false_alarms(self):
        """Sim always predicts event, obs never has event."""
        obs = np.array([1, 2, 3, 4, 5], dtype=float)
        sim = np.array([100, 100, 100, 100, 100], dtype=float)
        result = binary_contingency(obs, sim, threshold=50.0)
        assert result["tp"] == 0
        assert result["fp"] == 5
        assert result["fn"] == 0
        assert result["tn"] == 0
        assert result["precision"] == pytest.approx(0.0)
        assert np.isnan(result["recall"])  # no observed events

    def test_all_misses(self):
        """Obs always has event, sim never predicts."""
        obs = np.array([100, 100, 100], dtype=float)
        sim = np.array([1, 1, 1], dtype=float)
        result = binary_contingency(obs, sim, threshold=50.0)
        assert result["tp"] == 0
        assert result["fp"] == 0
        assert result["fn"] == 3
        assert result["tn"] == 0
        assert np.isnan(result["precision"])  # tp + fp = 0
        assert result["recall"] == pytest.approx(0.0)
        assert result["csi"] == pytest.approx(0.0)

    def test_below_threshold_mode(self):
        """above=False: event = value <= threshold (low-flow)."""
        obs = np.array([1, 2, 3, 50, 60], dtype=float)
        sim = np.array([1, 2, 3, 50, 60], dtype=float)
        result = binary_contingency(obs, sim, threshold=10.0, above=False)
        assert result["tp"] == 3  # obs<=10 & sim<=10
        assert result["fp"] == 0
        assert result["fn"] == 0
        assert result["tn"] == 2
        assert result["f1"] == pytest.approx(1.0)

    def test_hand_calculated(self):
        """Hand-calculated contingency.

        threshold=5, above=True
        obs: [3, 7, 2, 8, 5] => events: [7, 8, 5] (>=5)
        sim: [4, 6, 3, 9, 4] => events: [6, 9] (>=5)
        TP (obs>=5 & sim>=5): indices 1,3 => 2
        FP (obs<5 & sim>=5): index 4(sim[4]=4<5.. no)
        Let me re-check:
        idx0: obs=3(<5), sim=4(<5) => TN
        idx1: obs=7(>=5), sim=6(>=5) => TP
        idx2: obs=2(<5), sim=3(<5) => TN
        idx3: obs=8(>=5), sim=9(>=5) => TP
        idx4: obs=5(>=5), sim=4(<5) => FN
        TP=2, FP=0, FN=1, TN=2
        precision=2/2=1.0, recall=2/3=0.667
        F1=2*1.0*0.667/(1.0+0.667)=0.8
        CSI=2/(2+0+1)=0.667
        """
        obs = np.array([3, 7, 2, 8, 5], dtype=float)
        sim = np.array([4, 6, 3, 9, 4], dtype=float)
        result = binary_contingency(obs, sim, threshold=5.0)
        assert result["tp"] == 2
        assert result["fp"] == 0
        assert result["fn"] == 1
        assert result["tn"] == 2
        assert result["precision"] == pytest.approx(1.0)
        assert result["recall"] == pytest.approx(2.0 / 3.0)
        assert result["f1"] == pytest.approx(0.8)
        assert result["csi"] == pytest.approx(2.0 / 3.0)

    def test_empty_arrays(self):
        """Empty arrays => all zeros, metrics NaN."""
        result = binary_contingency(np.array([]), np.array([]), threshold=5.0)
        assert result["tp"] == 0
        assert result["fp"] == 0
        assert np.isnan(result["f1"])


# ===================================================================
# TestLowflowQuantiles
# ===================================================================


class TestLowflowQuantiles:
    """Q90 (10th pctl) and Q95 (5th pctl) of daily flow."""

    def test_known_quantiles(self):
        """Known quantiles from uniform distribution.

        For uniform [1, 100] with 1000 points:
        10th percentile ≈ 10.9, 5th percentile ≈ 5.95
        """
        obs = np.linspace(1, 100, 1000)
        result = lowflow_quantiles(obs)
        assert "q90" in result
        assert "q95" in result
        assert result["q90"] == pytest.approx(np.percentile(obs, 10), rel=1e-4)
        assert result["q95"] == pytest.approx(np.percentile(obs, 5), rel=1e-4)

    def test_fewer_than_365(self):
        """<365 points => empty dict."""
        obs = np.arange(1, 365, dtype=float)  # 364 points
        result = lowflow_quantiles(obs)
        assert result == {}

    def test_exactly_365(self):
        """Boundary: exactly 365 points => computes."""
        obs = np.arange(1, 366, dtype=float)
        result = lowflow_quantiles(obs)
        assert "q90" in result
        assert "q95" in result
        assert result["q95"] < result["q90"]  # Q95 < Q90


# ===================================================================
# TestCalculateDailySkillMetrics
# ===================================================================


class TestCalculateDailySkillMetrics:
    """Combined daily skill metric calculator."""

    @pytest.fixture
    def daily_obs_df(self):
        """20 years of daily observations for 2 stations."""
        dates = pd.date_range("2000-01-01", "2019-12-31", freq="D")
        rng = np.random.default_rng(42)
        records = []
        for code in ["ST01", "ST02"]:
            base_flow = 50.0 if code == "ST01" else 30.0
            for d in dates:
                # Seasonal pattern + random
                seasonal = 20 * np.sin(2 * np.pi * d.timetuple().tm_yday / 365)
                val = max(base_flow + seasonal + rng.normal(0, 10), 0.5)
                records.append({"code": code, "date": d, "discharge_avg": val})
        return pd.DataFrame(records)

    @pytest.fixture
    def daily_sim_df(self, daily_obs_df):
        """Simulated forecasts: obs + small noise, 2 models."""
        rng = np.random.default_rng(99)
        records = []
        for model in ["TFT", "TiDE"]:
            for _, row in daily_obs_df.iterrows():
                noise = rng.normal(0, 5)
                val = max(row["discharge_avg"] + noise, 0.1)
                records.append(
                    {
                        "code": row["code"],
                        "date": row["date"],
                        "model_short": model,
                        "forecasted_discharge": val,
                    }
                )
        return pd.DataFrame(records)

    def test_happy_path_multi_model(self, daily_obs_df, daily_sim_df):
        """Multi-model, multi-station => FDC + threshold metrics."""
        fdc_df, threshold_df = calculate_daily_skill_metrics(daily_obs_df, daily_sim_df)

        # FDC metrics: 2 stations * 2 models = 4 rows
        assert len(fdc_df) == 4
        assert set(fdc_df.columns) >= {"code", "model_short", "fhv", "flv"}

        # Check that FHV/FLV are numeric (not all NaN)
        assert fdc_df["fhv"].notna().any()
        assert fdc_df["flv"].notna().any()

        # Threshold metrics: should have rows for each
        # (code, model, threshold_type) combination
        assert len(threshold_df) > 0
        assert set(threshold_df.columns) >= {
            "code",
            "model_short",
            "threshold_type",
            "threshold_value",
            "f1",
            "tp",
            "fp",
            "fn",
            "tn",
            "n_years",
        }

        # Check threshold types
        threshold_types = set(threshold_df["threshold_type"].unique())
        # Should have flood thresholds (GEV) and low-flow thresholds
        assert any("flood" in t for t in threshold_types)
        assert any("lowflow" in t for t in threshold_types)

    def test_empty_obs(self):
        """Empty observations => empty results."""
        obs = pd.DataFrame(columns=["code", "date", "discharge_avg"])
        sim = pd.DataFrame(columns=["code", "date", "model_short", "forecasted_discharge"])
        fdc_df, threshold_df = calculate_daily_skill_metrics(obs, sim)
        assert fdc_df.empty
        assert threshold_df.empty

    def test_no_overlap(self, daily_obs_df):
        """No overlapping dates => empty results."""
        sim = pd.DataFrame(
            {
                "code": ["ST01"],
                "date": [pd.Timestamp("2030-01-01")],
                "model_short": ["TFT"],
                "forecasted_discharge": [50.0],
            }
        )
        fdc_df, threshold_df = calculate_daily_skill_metrics(daily_obs_df, sim)
        assert fdc_df.empty
        assert threshold_df.empty

    def test_insufficient_gev_data(self):
        """<15 years of data => no GEV thresholds, no flood metrics."""
        # 2 years of data
        dates = pd.date_range("2020-01-01", "2021-12-31", freq="D")
        rng = np.random.default_rng(42)
        obs = pd.DataFrame(
            {
                "code": "ST01",
                "date": dates,
                "discharge_avg": rng.uniform(10, 100, len(dates)),
            }
        )
        sim = pd.DataFrame(
            {
                "code": "ST01",
                "date": dates,
                "model_short": "TFT",
                "forecasted_discharge": rng.uniform(10, 100, len(dates)),
            }
        )
        fdc_df, threshold_df = calculate_daily_skill_metrics(obs, sim)

        # FDC metrics should still work (enough daily pairs)
        assert len(fdc_df) == 1
        assert fdc_df.iloc[0]["fhv"] is not None

        # No flood thresholds (insufficient GEV data)
        flood_rows = threshold_df[threshold_df["threshold_type"].str.contains("flood")]
        assert flood_rows.empty

    def test_single_model(self, daily_obs_df):
        """Single model works correctly."""
        rng = np.random.default_rng(42)
        sim = daily_obs_df[["code", "date"]].copy()
        sim["model_short"] = "TFT"
        sim["forecasted_discharge"] = (
            daily_obs_df["discharge_avg"] + rng.normal(0, 3, len(daily_obs_df))
        ).clip(lower=0.1)

        fdc_df, threshold_df = calculate_daily_skill_metrics(daily_obs_df, sim)
        assert len(fdc_df) == 2  # 2 stations
        assert all(fdc_df["model_short"] == "TFT")

    def test_nan_forecasts_excluded(self, daily_obs_df):
        """Rows with NaN forecasted_discharge are excluded."""
        sim = daily_obs_df[["code", "date"]].copy()
        sim["model_short"] = "TFT"
        sim["forecasted_discharge"] = np.nan  # all NaN

        fdc_df, threshold_df = calculate_daily_skill_metrics(daily_obs_df, sim)
        assert fdc_df.empty
        assert threshold_df.empty


# ===================================================================
# TestMetricRegistry
# ===================================================================


class TestDailyMetricRegistry:
    """Verify Tier 2 daily metrics are registered correctly."""

    def test_fhv_registered(self):
        assert "fhv" in DAILY_METRIC_REGISTRY
        assert DAILY_METRIC_REGISTRY["fhv"]["min_points"] == 50
        assert DAILY_METRIC_REGISTRY["fhv"]["higher_is_better"] is None

    def test_flv_registered(self):
        assert "flv" in DAILY_METRIC_REGISTRY
        assert DAILY_METRIC_REGISTRY["flv"]["min_points"] == 10
        assert DAILY_METRIC_REGISTRY["flv"]["higher_is_better"] is None

    def test_fhv_flv_not_in_point_metric_registry(self):
        """FHV/FLV are daily metrics — not in the point metric registry."""
        from src.skill_metrics import METRIC_REGISTRY, THRESHOLD_METRICS

        assert "fhv" not in METRIC_REGISTRY
        assert "flv" not in METRIC_REGISTRY
        assert "fhv" not in THRESHOLD_METRICS
        assert "flv" not in THRESHOLD_METRICS
