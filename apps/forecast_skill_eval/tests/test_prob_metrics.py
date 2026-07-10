"""Unit tests for probabilistic scoring primitives in prob_metrics.py.

All fixtures use synthetic station codes 19999/29999 and invented quantile
vectors.  No real station codes or discharge values appear in this file.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest
from probabilistic_metrics import crps_from_quantiles as shared_crps_from_quantiles
from probabilistic_metrics import crps_single as shared_crps_single

from forecast_skill_eval.prob_metrics import (
    PROB_METRIC_COLUMNS,
    PROB_RELIABILITY_COLUMNS,
    _aggregate_brier,
    _crpss_paired,
    _rank_calibration_error,
    _score_pairs,
    brier_score,
    build_prob_reliability,
    compute_probabilistic_metrics,
    coverage_hit,
    crps_from_quantiles,
    crps_reference_from_samples,
    event_probability,
    interval_width,
    isotonic_band,
    rank_position,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pair(
    *,
    code: str = "19999",
    horizon: str = "pentad",
    period_key: int = 10,
    year: int = 2020,
    model: str = "TFT",
    obs: float = 50.0,
    norm: float = 60.0,
    q05: float | None = 30.0,
    q10: float | None = None,
    q25: float | None = 40.0,
    q50: float | None = 50.0,
    q75: float | None = 65.0,
    q90: float | None = None,
    q95: float | None = 80.0,
    grid_id: str = "short5",
    obs_class: str = "normal",
    fc_class: str = "normal",
    contingency: str = "TN",
    regime: str = "all",
    season: str = "all",
    basin: str = "all",
    norm_provenance: str = "official",
    lead: object = None,
) -> dict:
    return {
        "code": code,
        "horizon": horizon,
        "period_key": period_key,
        "year": year,
        "model": model,
        "observed_value": obs,
        "norm": norm,
        "fc_q05": q05,
        "fc_q10": q10,
        "fc_q25": q25,
        "fc_q50": q50,
        "fc_q75": q75,
        "fc_q90": q90,
        "fc_q95": q95,
        "fc_grid_id": grid_id,
        "obs_class": obs_class,
        "fc_class": fc_class,
        "contingency": contingency,
        "regime": regime,
        "season": season,
        "basin": basin,
        "norm_provenance": norm_provenance,
        "lead": lead,
        "forecast_value": q50 if q50 is not None else 0.0,
    }


def _pairs_df(*dicts: dict) -> pd.DataFrame:
    return pd.DataFrame(list(dicts))


# ===========================================================================
# isotonic_band
# ===========================================================================


class TestIsotonicBand:
    def test_already_sorted_and_nondecreasing(self):
        levels, qvals, repaired = isotonic_band([0.25, 0.75], [40.0, 60.0])
        assert levels == [0.25, 0.75]
        assert qvals == [40.0, 60.0]
        assert repaired is False

    def test_crossing_band_is_repaired(self):
        # q75 < q25 — must be repaired to q75 = q25
        levels, qvals, repaired = isotonic_band([0.25, 0.75], [60.0, 40.0])
        assert repaired is True
        assert qvals[0] <= qvals[1], "Band must be non-decreasing after repair"
        assert len(levels) == 2

    def test_unsorted_levels_are_sorted(self):
        levels, qvals, repaired = isotonic_band([0.75, 0.25], [60.0, 40.0])
        assert levels == [0.25, 0.75]
        assert qvals == [40.0, 60.0]
        assert repaired is False

    def test_nan_nodes_are_dropped(self):
        levels, qvals, repaired = isotonic_band(
            [0.05, 0.25, 0.75, 0.95], [float("nan"), 40.0, 60.0, float("nan")]
        )
        assert 0.05 not in levels
        assert 0.95 not in levels
        assert len(levels) == 2

    def test_empty_input(self):
        levels, qvals, repaired = isotonic_band([], [])
        assert levels == []
        assert qvals == []
        assert repaired is False

    def test_all_nan_returns_empty(self):
        levels, qvals, _ = isotonic_band([0.25, 0.75], [float("nan"), float("nan")])
        assert levels == []
        assert qvals == []

    def test_single_valid_node(self):
        levels, qvals, repaired = isotonic_band([0.50], [50.0])
        assert levels == [0.50]
        assert qvals == [50.0]
        assert repaired is False

    def test_three_crossings_all_repaired(self):
        # q05=100, q25=80, q75=60 — all crossed
        levels, qvals, repaired = isotonic_band([0.05, 0.25, 0.75], [100.0, 80.0, 60.0])
        assert repaired is True
        for i in range(len(qvals) - 1):
            assert qvals[i] <= qvals[i + 1]

    def test_was_repaired_counts_correctly(self):
        # Only the last node is crossed
        _, _, repaired = isotonic_band([0.25, 0.50, 0.75], [30.0, 50.0, 40.0])
        assert repaired is True


# ===========================================================================
# crps_from_quantiles — correctness-critical
# ===========================================================================


class TestCrpsFromQuantiles:
    """Tests for CRPS scoring with explicit tail treatment."""

    _LEVELS = [0.05, 0.25, 0.75, 0.95]
    _QVALS = [20.0, 40.0, 60.0, 80.0]

    def test_obs_at_band_centre_lower_than_off_centre(self):
        crps_centre = crps_from_quantiles(self._LEVELS, self._QVALS, 50.0)
        crps_off = crps_from_quantiles(self._LEVELS, self._QVALS, 90.0)
        assert math.isfinite(crps_centre)
        assert crps_off > crps_centre, "Off-centre obs should have higher CRPS than centred obs"

    def test_obs_beyond_band_penalised_vs_obs_at_edge(self):
        # obs at outer edge (q95 = 80) vs obs well beyond (120)
        crps_at_edge = crps_from_quantiles(self._LEVELS, self._QVALS, 80.0)
        crps_beyond = crps_from_quantiles(self._LEVELS, self._QVALS, 120.0)
        assert math.isfinite(crps_at_edge)
        assert crps_beyond > crps_at_edge, (
            "Obs beyond the band must score worse than obs at the edge "
            "(tail penalty proves overconfidence is NOT rewarded)"
        )

    def test_obs_below_band_penalised_vs_obs_at_lower_edge(self):
        crps_at_edge = crps_from_quantiles(self._LEVELS, self._QVALS, 20.0)
        crps_below = crps_from_quantiles(self._LEVELS, self._QVALS, -10.0)
        assert crps_below > crps_at_edge, (
            "Obs below the band must score worse than obs at the lower edge"
        )

    def test_narrow_overconfident_band_penalised_for_tail_miss(self):
        # Narrow band: q05=45, q95=55 (2σ ≈ 5 units)
        # Wide calibrated band: q05=20, q95=80
        # Obs = 100 (beyond both)
        narrow_levels = [0.05, 0.25, 0.75, 0.95]
        narrow_qvals = [45.0, 47.0, 53.0, 55.0]
        wide_levels = [0.05, 0.25, 0.75, 0.95]
        wide_qvals = [20.0, 40.0, 60.0, 80.0]
        obs = 100.0

        crps_narrow = crps_from_quantiles(narrow_levels, narrow_qvals, obs)
        crps_wide = crps_from_quantiles(wide_levels, wide_qvals, obs)
        assert crps_narrow > crps_wide, (
            "Narrow overconfident band must score worse than wider calibrated "
            "band when the observation falls in the tail"
        )

    def test_degenerate_point_band(self):
        # All nodes equal q=50: CRPS should equal |obs - 50|
        levels = [0.05, 0.25, 0.50, 0.75, 0.95]
        qvals = [50.0, 50.0, 50.0, 50.0, 50.0]
        obs = 70.0
        crps_val = crps_from_quantiles(levels, qvals, obs)
        # For a point mass at 50, CRPS = |70 - 50| = 20
        # The grid estimator should be very close (all tail corrections cancel)
        assert math.isfinite(crps_val)
        assert abs(crps_val - 20.0) < 1.0, f"Expected CRPS ≈ 20 for point band, got {crps_val}"

    def test_fewer_than_two_nodes_returns_nan(self):
        # Only one finite node
        assert math.isnan(crps_from_quantiles([0.50], [50.0], 60.0))

    def test_nan_observed_returns_nan(self):
        assert math.isnan(crps_from_quantiles(self._LEVELS, self._QVALS, float("nan")))

    def test_crps_nonnegative(self):
        for obs in [0.0, 30.0, 50.0, 80.0, 120.0]:
            val = crps_from_quantiles(self._LEVELS, self._QVALS, obs)
            assert val >= 0.0, f"CRPS must be non-negative, got {val} for obs={obs}"

    def test_deterministic_value_matches_hand_computation(self):
        # Simple 2-node band: levels=[0.25, 0.75], qvals=[40, 60], obs=50
        # Middle trapezoid: dtau=0.5,
        # pinball(0.25,40)=2.5, pinball(0.75,60)=2.5
        # middle = 0.5*(2.5+2.5)/2 = 1.25
        # Left tail [0, 0.25]: obs=50 >= q_min=40 → (50-40)*0.25²/2 = 10*0.03125 = 0.3125
        # Right tail [0.75, 1]: obs=50 <= q_max=60 → (60-50)*(1-0.75)²/2 = 10*0.03125 = 0.3125
        # CRPS = 2*(0.3125+1.25+0.3125) = 2*1.875 = 3.75
        crps_val = crps_from_quantiles([0.25, 0.75], [40.0, 60.0], 50.0)
        assert abs(crps_val - 3.75) < 1e-10, f"Expected 3.75, got {crps_val}"

    def test_tail_penalty_scales_with_distance(self):
        # Farther beyond the band → higher CRPS
        crps_near = crps_from_quantiles(self._LEVELS, self._QVALS, 85.0)
        crps_far = crps_from_quantiles(self._LEVELS, self._QVALS, 150.0)
        assert crps_far > crps_near


# ===========================================================================
# crps_from_quantiles — M4 (D3/#5+#6) canonical shared-estimator contract
# ===========================================================================


class TestCrpsFromQuantilesCanonicalDelegation:
    """crps_from_quantiles must delegate to (produce identical values to) the
    canonical iEasyHydroForecast.probabilistic_metrics estimator, so
    postprocessing_forecasts and forecast_skill_eval always agree (M4 'same
    value at both call sites' contract). See
    postprocessing_forecasts/tests/test_crps.py for the mirrored check on
    the other call site."""

    _QUANTILE_LEVELS = [0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95]

    def test_deterministic_band_matches_textbook_abs_diff(self):
        """Locked M4 regression test: all quantiles == 100.0, observed ==
        130.0 -> CRPS == 30.0. The OLD postprocessing_forecasts estimator
        returned ~13.5 for the equivalent input (missing factor-2 + tails);
        forecast_skill_eval's estimator already got this right."""
        result = crps_from_quantiles(self._QUANTILE_LEVELS, [100.0] * 7, 130.0)
        assert result == pytest.approx(30.0, abs=1e-9)

    def test_matches_shared_crps_single_directly(self):
        qvals = [20, 40, 60, 80, 100, 120, 140]
        result = crps_from_quantiles(self._QUANTILE_LEVELS, qvals, 95.0)
        expected = shared_crps_single(self._QUANTILE_LEVELS, qvals, 95.0)
        assert result == pytest.approx(expected, rel=1e-12)

    def test_matches_shared_batched_entry_point_for_one_row(self):
        """Same value whether scored via the per-pair entry point used here,
        or the (N,K) batched entry point used by postprocessing_forecasts —
        proves the two call-site shapes agree, not just the two apps."""
        observed = np.array([130.0])
        quantile_forecasts = np.array([[100.0] * 7])
        batched = shared_crps_from_quantiles(
            observed, quantile_forecasts, np.array(self._QUANTILE_LEVELS)
        )
        single = crps_from_quantiles(self._QUANTILE_LEVELS, [100.0] * 7, 130.0)
        assert single == pytest.approx(batched, rel=1e-12)


# ===========================================================================
# crps_reference_from_samples — estimator consistency
# ===========================================================================


class TestCrpsReferenceFromSamples:
    _LEVELS = [0.05, 0.25, 0.75, 0.95]

    def test_returns_finite_for_valid_input(self):
        sample = list(range(10, 110, 10))  # 10,20,...,100
        val = crps_reference_from_samples(sample, 55.0, self._LEVELS)
        assert math.isfinite(val)
        assert val >= 0.0

    def test_empty_sample_returns_nan(self):
        assert math.isnan(crps_reference_from_samples([], 50.0, self._LEVELS))

    def test_single_sample_returns_nan(self):
        assert math.isnan(crps_reference_from_samples([50.0], 50.0, self._LEVELS))

    def test_estimator_uses_identical_path_as_crps_from_quantiles(self):
        """If sample quantiles equal forecast quantiles → same CRPS value."""
        import numpy as np

        sample = [20.0, 40.0, 60.0, 80.0]  # uniform-ish
        qvals = list(np.quantile(sample, self._LEVELS))
        obs = 55.0

        ref_crps = crps_reference_from_samples(sample, obs, self._LEVELS)
        direct_crps = crps_from_quantiles(self._LEVELS, qvals, obs)
        assert abs(ref_crps - direct_crps) < 1e-10, (
            "crps_reference_from_samples must use the identical estimator "
            f"as crps_from_quantiles; got {ref_crps} vs {direct_crps}"
        )

    def test_nan_values_in_sample_ignored(self):
        clean = crps_reference_from_samples([10.0, 50.0, 90.0], 50.0, self._LEVELS)
        with_nans = crps_reference_from_samples(
            [10.0, float("nan"), 50.0, float("nan"), 90.0], 50.0, self._LEVELS
        )
        assert abs(clean - with_nans) < 1e-10


# ===========================================================================
# coverage_hit and interval_width
# ===========================================================================


class TestCoverageHit:
    def test_inside(self):
        assert coverage_hit(30.0, 70.0, 50.0) == 1.0

    def test_on_lower_boundary(self):
        assert coverage_hit(50.0, 70.0, 50.0) == 1.0

    def test_on_upper_boundary(self):
        assert coverage_hit(30.0, 50.0, 50.0) == 1.0

    def test_outside_below(self):
        assert coverage_hit(30.0, 70.0, 10.0) == 0.0

    def test_outside_above(self):
        assert coverage_hit(30.0, 70.0, 90.0) == 0.0

    def test_nan_lower_returns_nan(self):
        assert math.isnan(coverage_hit(float("nan"), 70.0, 50.0))

    def test_nan_upper_returns_nan(self):
        assert math.isnan(coverage_hit(30.0, float("nan"), 50.0))

    def test_nan_observed_returns_nan(self):
        assert math.isnan(coverage_hit(30.0, 70.0, float("nan")))


class TestIntervalWidth:
    def test_basic(self):
        assert interval_width(30.0, 70.0) == pytest.approx(40.0)

    def test_zero_width(self):
        assert interval_width(50.0, 50.0) == pytest.approx(0.0)

    def test_nan_lower_returns_nan(self):
        assert math.isnan(interval_width(float("nan"), 70.0))

    def test_nan_upper_returns_nan(self):
        assert math.isnan(interval_width(30.0, float("nan")))


# ===========================================================================
# rank_position
# ===========================================================================


class TestRankPosition:
    _LEVELS = [0.05, 0.25, 0.50, 0.75, 0.95]
    _QVALS = [20.0, 40.0, 50.0, 60.0, 80.0]

    def test_below_q05_returns_zero(self):
        assert rank_position(self._LEVELS, self._QVALS, 5.0) == pytest.approx(0.0)

    def test_above_q95_returns_one(self):
        assert rank_position(self._LEVELS, self._QVALS, 100.0) == pytest.approx(1.0)

    def test_at_q50_returns_approx_half(self):
        val = rank_position(self._LEVELS, self._QVALS, 50.0)
        assert abs(val - 0.50) < 0.05, f"Expected ≈0.50, got {val}"

    def test_single_node_returns_nan(self):
        assert math.isnan(rank_position([0.50], [50.0], 50.0))

    def test_nan_observed_returns_nan(self):
        assert math.isnan(rank_position(self._LEVELS, self._QVALS, float("nan")))

    def test_result_clamped_to_unit_interval(self):
        for obs in [-1e6, 1e6]:
            val = rank_position(self._LEVELS, self._QVALS, obs)
            assert 0.0 <= val <= 1.0


# ===========================================================================
# event_probability
# ===========================================================================


class TestEventProbability:
    _LEVELS = [0.05, 0.25, 0.75, 0.95]
    _QVALS = [20.0, 40.0, 60.0, 80.0]

    def test_below_and_above_sum_to_one(self):
        threshold = 50.0
        p_below = event_probability(self._LEVELS, self._QVALS, threshold, "below")
        p_above = event_probability(self._LEVELS, self._QVALS, threshold, "above")
        assert abs(p_below + p_above - 1.0) < 1e-10, (
            f"P_below + P_above must equal 1; got {p_below} + {p_above}"
        )

    def test_threshold_below_band_min_gives_zero_probability_below(self):
        p = event_probability(self._LEVELS, self._QVALS, 5.0, "below")
        assert p == pytest.approx(0.0)

    def test_threshold_above_band_max_gives_one_probability_below(self):
        p = event_probability(self._LEVELS, self._QVALS, 100.0, "below")
        assert p == pytest.approx(1.0)

    def test_interior_threshold_gives_interpolated_value(self):
        # q_25=40 is at level 0.25; threshold exactly at q25 → CDF ≈ 0.25
        p = event_probability(self._LEVELS, self._QVALS, 40.0, "below")
        assert abs(p - 0.25) < 0.05

    def test_single_node_returns_nan(self):
        assert math.isnan(event_probability([0.50], [50.0], 45.0, "below"))

    def test_nan_threshold_returns_nan(self):
        assert math.isnan(event_probability(self._LEVELS, self._QVALS, float("nan"), "below"))

    def test_probability_in_unit_interval(self):
        for t in [10.0, 40.0, 50.0, 60.0, 90.0]:
            p = event_probability(self._LEVELS, self._QVALS, t, "below")
            assert 0.0 <= p <= 1.0, f"p={p} for threshold={t} is outside [0,1]"


# ===========================================================================
# brier_score
# ===========================================================================


class TestBrierScore:
    def test_perfect_forecast_event_occurs(self):
        assert brier_score(1.0, True) == pytest.approx(0.0)

    def test_perfect_forecast_no_event(self):
        assert brier_score(0.0, False) == pytest.approx(0.0)

    def test_worst_case_event_occurs(self):
        assert brier_score(0.0, True) == pytest.approx(1.0)

    def test_worst_case_no_event(self):
        assert brier_score(1.0, False) == pytest.approx(1.0)

    def test_fifty_percent(self):
        assert brier_score(0.5, True) == pytest.approx(0.25)
        assert brier_score(0.5, False) == pytest.approx(0.25)

    def test_nan_prob_returns_nan(self):
        assert math.isnan(brier_score(float("nan"), True))


# ===========================================================================
# _score_pairs
# ===========================================================================


class TestScorePairs:
    def test_valid_band_produces_finite_scores(self):
        row = _make_pair(obs=50.0, q05=20.0, q25=40.0, q50=50.0, q75=65.0, q95=80.0)
        df = _score_pairs(_pairs_df(row))
        assert math.isfinite(df["crps"].iloc[0])
        assert math.isfinite(df["hit_90"].iloc[0])
        assert math.isfinite(df["width_outer"].iloc[0])

    def test_short_grid_no_q10_q90_gives_nan_hit_80(self):
        # q10 and q90 absent → hit_80 should be NaN
        row = _make_pair(q10=None, q90=None)
        df = _score_pairs(_pairs_df(row))
        assert math.isnan(df["hit_80"].iloc[0])
        assert math.isfinite(df["hit_50"].iloc[0])
        assert math.isfinite(df["hit_90"].iloc[0])

    def test_all_nan_band_gives_nan_scores(self):
        row = _make_pair(q05=None, q10=None, q25=None, q50=None, q75=None, q90=None, q95=None)
        df = _score_pairs(_pairs_df(row))
        assert math.isnan(df["crps"].iloc[0])
        assert math.isnan(df["rank"].iloc[0])
        assert math.isnan(df["hit_50"].iloc[0])

    def test_crossing_band_is_repaired_and_scored(self):
        # q25 > q75 — crossing; should be repaired and scored (not NaN)
        row = _make_pair(q25=65.0, q75=40.0, q05=20.0, q95=80.0)
        df = _score_pairs(_pairs_df(row))
        assert df["n_band_repaired"].iloc[0] == 1
        assert math.isfinite(df["crps"].iloc[0])

    def test_n_band_repaired_zero_for_valid_band(self):
        row = _make_pair()
        df = _score_pairs(_pairs_df(row))
        assert df["n_band_repaired"].iloc[0] == 0

    def test_custom_threshold_affects_below_norm_prob(self):
        # With threshold=0.50: norm*0.50=30; obs q05=20 → some prob
        # With threshold=0.90: norm*0.90=54; higher threshold → more probability
        row = _make_pair(
            obs=50.0,
            norm=60.0,
            q05=20.0,
            q25=40.0,
            q75=65.0,
            q95=80.0,
            q50=None,
            q10=None,
            q90=None,
        )
        df_lo = _score_pairs(_pairs_df(row), threshold=0.50)
        df_hi = _score_pairs(_pairs_df(row), threshold=0.90)
        p_lo = df_lo["below_norm_prob"].iloc[0]
        p_hi = df_hi["below_norm_prob"].iloc[0]
        assert p_hi >= p_lo, "Higher threshold → higher probability of below-norm event"

    def test_empty_frame_returns_typed_columns(self):
        df = _score_pairs(pd.DataFrame())
        assert "crps" in df.columns
        assert len(df) == 0

    def test_below_norm_prob_matches_event_at_threshold(self):
        # threshold=0.80, norm=100 → event threshold = 80 = q95
        # P(X < 80) ≈ 1.0 (obs at the outer boundary)
        row = _make_pair(
            obs=50.0,
            norm=100.0,
            q05=20.0,
            q25=40.0,
            q75=65.0,
            q95=80.0,
            q50=None,
            q10=None,
            q90=None,
        )
        df = _score_pairs(_pairs_df(row), threshold=0.80)
        p = df["below_norm_prob"].iloc[0]
        assert math.isfinite(p)
        assert 0.0 <= p <= 1.0

    def test_width_outer_norm_is_normalised_by_norm(self):
        row = _make_pair(obs=50.0, norm=40.0, q05=20.0, q95=80.0, q25=40.0, q75=65.0)
        df = _score_pairs(_pairs_df(row))
        width_outer = df["width_outer"].iloc[0]
        norm_val = 40.0
        expected_norm = width_outer / norm_val
        assert abs(df["width_outer_norm"].iloc[0] - expected_norm) < 1e-10

    def test_multiple_rows_scored_independently(self):
        row1 = _make_pair(obs=50.0)
        row2 = _make_pair(code="29999", obs=90.0, q05=20.0, q25=40.0, q75=65.0, q95=80.0)
        df = _score_pairs(_pairs_df(row1, row2))
        assert len(df) == 2
        # obs=90 is beyond q95=80 → should have right-tail penalty
        crps1 = df["crps"].iloc[0]
        crps2 = df["crps"].iloc[1]
        assert crps2 > crps1


# ===========================================================================
# compute_probabilistic_metrics — reducer
# ===========================================================================


def _make_scored_pairs() -> pd.DataFrame:
    """Build a minimal synthetic pairs DataFrame for reducer tests.

    Uses multiple years per (code, period_key) group so precompute_climatology_crps
    has enough observations (>=2) to produce a clim_ref entry.
    """
    rows = []
    for code in ("19999", "29999"):
        for pk in (1, 2, 3):
            for year in (2018, 2019, 2020):
                obs = 45.0 + pk * 2.0  # slightly different per period_key
                rows.append(
                    _make_pair(
                        code=code,
                        horizon="pentad",
                        period_key=pk,
                        year=year,
                        model="TFT",
                        obs=obs,
                        norm=60.0,
                        q05=20.0,
                        q25=40.0,
                        q50=50.0,
                        q75=65.0,
                        q95=80.0,
                        obs_class="normal",
                    )
                )
    return pd.DataFrame(rows)


class TestComputeProbabilisticMetrics:
    def test_returns_dataframe_with_correct_columns(self):
        pairs = _make_scored_pairs()
        result = compute_probabilistic_metrics(pairs, {}, {}, ("below_norm",))
        for col in PROB_METRIC_COLUMNS:
            assert col in result.columns, f"Missing column: {col}"

    def test_emits_distribution_and_below_norm_event_rows(self):
        pairs = _make_scored_pairs()
        result = compute_probabilistic_metrics(pairs, {}, {}, ("below_norm",))
        events = set(result["event"].unique())
        assert "distribution" in events
        assert "below_norm" in events

    def test_distribution_rows_have_nan_brier(self):
        pairs = _make_scored_pairs()
        result = compute_probabilistic_metrics(pairs, {}, {}, ("below_norm",))
        dist_rows = result[result["event"] == "distribution"]
        assert dist_rows["brier"].isna().all(), "distribution rows must have NaN brier"
        assert dist_rows["brier_ss"].isna().all()

    def test_below_norm_rows_have_nan_crps(self):
        pairs = _make_scored_pairs()
        result = compute_probabilistic_metrics(pairs, {}, {}, ("below_norm",))
        bn_rows = result[result["event"] == "below_norm"]
        assert bn_rows["crps"].isna().all(), "below_norm rows must have NaN crps"
        assert bn_rows["coverage_90"].isna().all()

    def test_pooled_and_per_station_rows_both_present(self):
        pairs = _make_scored_pairs()
        result = compute_probabilistic_metrics(pairs, {}, {}, ("below_norm",))
        codes = set(result["code"].unique())
        assert "POOLED" in codes
        assert "19999" in codes
        assert "29999" in codes

    def test_empty_pairs_returns_empty_frame(self):
        result = compute_probabilistic_metrics(pd.DataFrame(), {}, {}, ("below_norm",))
        assert result.empty
        for col in PROB_METRIC_COLUMNS:
            assert col in result.columns

    def test_crpss_positive_when_forecast_better_than_climatology(self):
        """Forecast CRPS < climatology CRPS → CRPSS > 0.

        Uses a wide, variable climatology so crps_clim > 0, and a narrow
        well-centred forecast band to ensure crps_fc << crps_clim.
        """
        from forecast_skill_eval.prob_baselines import precompute_climatology_crps

        # Wide climatology: obs uniformly spread [10, 90] over 10 years
        obs_vals = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]
        rows = []
        for yr, obs_v in enumerate(obs_vals, start=2010):
            # Narrow forecast centred on obs_v (near-perfect forecast)
            rows.append(
                _make_pair(
                    code="19999",
                    horizon="pentad",
                    period_key=5,
                    year=yr,
                    model="NARROW",
                    obs=obs_v,
                    norm=60.0,
                    q05=obs_v - 2.0,
                    q25=obs_v - 1.0,
                    q50=obs_v,
                    q75=obs_v + 1.0,
                    q95=obs_v + 2.0,
                    q10=None,
                    q90=None,
                    obs_class="normal",
                )
            )
        pairs = pd.DataFrame(rows)

        clim_ref = precompute_climatology_crps(pairs)
        assert len(clim_ref) > 0, "clim_ref must have at least one entry"

        result = compute_probabilistic_metrics(pairs, {}, clim_ref, ("below_norm",))
        dist_rows = result[result["event"] == "distribution"]
        pooled = dist_rows[dist_rows["code"] == "POOLED"]
        assert len(pooled) > 0

        crpss = pooled["crpss"].iloc[0]
        assert math.isfinite(crpss), f"CRPSS must be finite; got {crpss}"
        assert crpss > 0.0, (
            f"Near-perfect forecast vs wide climatology must have CRPSS > 0; got {crpss}"
        )

    def test_crpss_approximately_zero_when_forecast_equals_climatology(self):
        """When forecast distribution == climatology sample → CRPSS ≈ 0."""
        import numpy as np

        from forecast_skill_eval.prob_baselines import precompute_climatology_crps

        # Create pairs where q05..q95 ARE the climatology quantiles
        sample = [20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]
        levels = [0.05, 0.25, 0.50, 0.75, 0.95]
        qvals = list(np.quantile(sample, levels))

        rows = []
        for i, obs_val in enumerate(sample):
            rows.append(
                _make_pair(
                    code="19999",
                    horizon="pentad",
                    period_key=1,
                    year=2010 + i,
                    model="REF",
                    obs=obs_val,
                    norm=60.0,
                    q05=qvals[0],
                    q25=qvals[1],
                    q50=qvals[2],
                    q75=qvals[3],
                    q95=qvals[4],
                    q10=None,
                    q90=None,
                )
            )
        pairs = pd.DataFrame(rows)
        clim_ref = precompute_climatology_crps(pairs)
        result = compute_probabilistic_metrics(pairs, {}, clim_ref, ("below_norm",))
        dist_rows = result[result["event"] == "distribution"]
        pooled = dist_rows[dist_rows["code"] == "POOLED"]
        if len(pooled) > 0 and not math.isnan(pooled["crpss"].iloc[0]):
            crpss = pooled["crpss"].iloc[0]
            # When forecast = climatology, CRPSS should be near zero
            # (not exactly 0 due to finite sample but within ±0.2)
            assert abs(crpss) < 0.3, (
                f"CRPSS should be near 0 when forecast equals climatology; got {crpss}"
            )


# ===========================================================================
# build_prob_reliability
# ===========================================================================


class TestBuildProbReliability:
    def test_returns_correct_columns(self):
        pairs = _make_scored_pairs()
        result = build_prob_reliability(pairs)
        for col in PROB_RELIABILITY_COLUMNS:
            assert col in result.columns

    def test_observed_frequency_in_unit_interval(self):
        pairs = _make_scored_pairs()
        result = build_prob_reliability(pairs)
        assert (result["observed_frequency"].between(0.0, 1.0)).all()

    def test_perfectly_calibrated_sample_matches_nominal(self):
        """For a uniformly-spread obs matching the quantiles, coverage ≈ nominal."""
        import numpy as np

        # Build pairs where obs systematically spans [10, 90] uniformly
        # Band: q05=10, q25=30, q75=70, q95=90
        n = 20
        obs_vals = np.linspace(5.0, 95.0, n)
        rows = []
        for i, obs_v in enumerate(obs_vals):
            rows.append(
                _make_pair(
                    code="19999",
                    horizon="pentad",
                    period_key=1,
                    year=2000 + i,
                    model="CALIB",
                    obs=float(obs_v),
                    norm=60.0,
                    q05=10.0,
                    q25=30.0,
                    q75=70.0,
                    q95=90.0,
                    q10=None,
                    q90=None,
                    q50=50.0,
                )
            )
        pairs = pd.DataFrame(rows)
        result = build_prob_reliability(pairs)

        # For q95=90: ~18/20 obs <= 90 → observed_frequency ≈ 0.90
        pooled_q95 = result[(result["code"] == "POOLED") & (result["nominal_level"] == 0.95)]
        if len(pooled_q95) > 0:
            assert abs(pooled_q95["observed_frequency"].iloc[0] - 0.90) < 0.15, (
                "Calibrated q95 row should have observed_frequency ≈ 0.90"
            )

    def test_over_confident_band_deviates_from_nominal(self):
        """Narrow band that should contain 90% but contains less → deviation."""
        rows = [
            _make_pair(
                code="19999",
                horizon="pentad",
                period_key=1,
                year=2000 + i,
                model="NARROW",
                obs=float(obs_v),
                norm=60.0,
                q05=49.0,
                q25=49.5,
                q75=50.5,
                q95=51.0,
            )
            for i, obs_v in enumerate(range(10, 110, 10))
        ]
        pairs = pd.DataFrame(rows)
        result = build_prob_reliability(pairs)
        pooled_q95 = result[(result["code"] == "POOLED") & (result["nominal_level"] == 0.95)]
        if len(pooled_q95) > 0:
            freq = pooled_q95["observed_frequency"].iloc[0]
            # Most obs fall outside [49, 51] → coverage << 0.95
            assert freq <= 0.5, (
                f"Over-confident band should have low observed_frequency; got {freq}"
            )

    def test_empty_pairs_returns_empty(self):
        result = build_prob_reliability(pd.DataFrame())
        assert result.empty

    def test_level_absent_for_short_grid_not_emitted(self):
        """If fc_q10 is all NaN, the 0.10 level row should not be emitted."""
        rows = [
            _make_pair(
                code="19999",
                horizon="pentad",
                period_key=1,
                year=2000 + i,
                obs=50.0,
                q10=None,
                q90=None,  # absent for short grid
            )
            for i in range(5)
        ]
        pairs = pd.DataFrame(rows)
        result = build_prob_reliability(pairs)
        # q10 and q90 should not appear (all NaN → no valid pairs for that level)
        levels = set(result["nominal_level"].unique())
        assert 0.10 not in levels or result[result["nominal_level"] == 0.10].empty

    def test_n_column_counts_valid_pairs(self):
        n = 6
        rows = [_make_pair(code="19999", period_key=1, year=2000 + i, obs=50.0) for i in range(n)]
        pairs = pd.DataFrame(rows)
        result = build_prob_reliability(pairs)
        pooled = result[result["code"] == "POOLED"]
        if len(pooled) > 0:
            assert pooled["n"].max() == n


# ===========================================================================
# P1b: norm-factor Brier parity (below_norm_100)
# ===========================================================================


_NF_KEYS = [
    "horizon",
    "model",
    "regime",
    "season",
    "code",
    "basin",
    "norm_provenance",
]


class TestNormFactorBrier:
    def test_default_empty_norm_factor_events_is_frame_equal(self):
        """Default ``norm_factor_events=()`` must be byte-identical to today."""
        pairs = _make_scored_pairs()
        base = compute_probabilistic_metrics(pairs, {}, {}, ("below_norm",))
        same = compute_probabilistic_metrics(pairs, {}, {}, ("below_norm",), norm_factor_events=())
        pd.testing.assert_frame_equal(base, same)

    def test_norm_factor_events_not_passed_has_no_effect(self):
        """Even with below_norm_100 in events_filter, no rows appear unless the
        EventDef is passed via ``norm_factor_events``."""
        pairs = _make_scored_pairs()
        result = compute_probabilistic_metrics(pairs, {}, {}, ("below_norm", "below_norm_100"))
        assert "below_norm_100" not in set(result["event"].unique())

    def test_below_norm_100_adds_brier_only_rows_no_extra_distribution(self):
        from forecast_skill_eval.events import event_by_name

        pairs = _make_scored_pairs()
        ev = event_by_name("below_norm_100")

        without = compute_probabilistic_metrics(pairs, {}, {}, ("below_norm", "below_norm_100"))
        result = compute_probabilistic_metrics(
            pairs,
            {},
            {},
            ("below_norm", "below_norm_100"),
            norm_factor_events=(ev,),
        )

        events = set(result["event"].unique())
        assert "below_norm_100" in events
        assert "distribution" in events
        assert "below_norm" in events

        # No extra distribution rows are introduced by the norm-factor pass.
        n_dist_without = int((without["event"] == "distribution").sum())
        n_dist_result = int((result["event"] == "distribution").sum())
        assert n_dist_result == n_dist_without

        # below_norm_100 rows are Brier-only (NaN CRPS / coverage).
        bn100 = result[result["event"] == "below_norm_100"]
        assert bn100["crps"].isna().all()
        assert bn100["coverage_90"].isna().all()

    def test_below_norm_100_matches_below_norm_group_keys_and_n_pairs(self):
        from forecast_skill_eval.events import event_by_name

        pairs = _make_scored_pairs()
        ev = event_by_name("below_norm_100")
        result = compute_probabilistic_metrics(
            pairs,
            {},
            {},
            ("below_norm", "below_norm_100"),
            norm_factor_events=(ev,),
        )
        bn = result[result["event"] == "below_norm"].reset_index(drop=True)
        bn100 = result[result["event"] == "below_norm_100"].reset_index(drop=True)

        # The norm-factor pass reuses the exact same slice/group reducer over a
        # frame with identical group-key columns, so the Brier rows are emitted
        # in the same order with the same group keys and the same n_pairs.
        key_cols = [*_NF_KEYS, "lead", "n_pairs"]
        pd.testing.assert_frame_equal(
            bn[key_cols].reset_index(drop=True),
            bn100[key_cols].reset_index(drop=True),
        )

    def test_below_norm_only_rows_unchanged_by_norm_factor_pass(self):
        """distribution + below_norm rows must be byte-identical whether or not
        the below_norm_100 Brier pass runs."""
        from forecast_skill_eval.events import event_by_name

        pairs = _make_scored_pairs()
        ev = event_by_name("below_norm_100")

        base = compute_probabilistic_metrics(pairs, {}, {}, ("below_norm", "below_norm_100"))
        result = compute_probabilistic_metrics(
            pairs,
            {},
            {},
            ("below_norm", "below_norm_100"),
            norm_factor_events=(ev,),
        )

        def _nonfactor(df: pd.DataFrame) -> pd.DataFrame:
            return df[df["event"].isin(["distribution", "below_norm"])].reset_index(drop=True)

        pd.testing.assert_frame_equal(_nonfactor(base), _nonfactor(result))


# ===========================================================================
# CORE-4: rank_calibration_error — proper PIT divergence (0 = calibrated)
# ===========================================================================


class TestRankCalibrationError:
    def test_perfectly_uniform_ranks_gives_zero(self):
        # Exact Uniform PIT: ranks at the expected (i-0.5)/n quantiles → 0.
        n = 20
        ranks = pd.Series([(i - 0.5) / n for i in range(1, n + 1)])
        assert _rank_calibration_error(ranks) == pytest.approx(0.0, abs=1e-12)

    def test_old_definition_would_have_been_nonzero_for_calibrated(self):
        # Guard against regressing to mean|rank-0.5|, which was 0.25 here.
        n = 20
        ranks = pd.Series([(i - 0.5) / n for i in range(1, n + 1)])
        assert _rank_calibration_error(ranks) < 0.01

    def test_overdispersed_ranks_clumped_near_half_gives_positive(self):
        # Over-dispersion: all PIT mass at 0.5 must be penalised, not rewarded.
        ranks = pd.Series([0.5] * 20)
        assert _rank_calibration_error(ranks) > 0.1

    def test_underdispersed_ranks_at_extremes_gives_positive(self):
        ranks = pd.Series([0.0, 0.0, 0.0, 1.0, 1.0, 1.0])
        assert _rank_calibration_error(ranks) > 0.1

    def test_empty_returns_nan(self):
        assert math.isnan(_rank_calibration_error(pd.Series(dtype=float)))

    def test_nans_are_dropped(self):
        n = 10
        clean = pd.Series([(i - 0.5) / n for i in range(1, n + 1)])
        with_nan = pd.concat([clean, pd.Series([float("nan"), float("nan")])], ignore_index=True)
        assert _rank_calibration_error(with_nan) == pytest.approx(_rank_calibration_error(clean))


# ===========================================================================
# CORE-5: CRPSS uses the paired finite subset
# ===========================================================================


class TestCrpssPaired:
    def test_uses_intersection_of_finite_pairs(self):
        crps = pd.Series([1.0, 2.0, 3.0, 4.0])
        crps_ref = pd.Series([2.0, 4.0, float("nan"), 8.0])
        # Paired rows: 0, 1, 3 → fc_mean = 7/3, ref_mean = 14/3 → 1 - 0.5 = 0.5.
        assert _crpss_paired(crps, crps_ref) == pytest.approx(0.5)

    def test_all_reference_nan_returns_nan(self):
        crps = pd.Series([1.0, 2.0])
        crps_ref = pd.Series([float("nan"), float("nan")])
        assert math.isnan(_crpss_paired(crps, crps_ref))

    def test_paired_subset_differs_from_independent_means(self):
        # Independent means would give 1 - 5.5/2.0 = -1.75 (biased); the paired
        # subset (row 0 only) gives the correct 1 - 1.0/2.0 = 0.5.
        crps = pd.Series([1.0, 10.0])
        crps_ref = pd.Series([2.0, float("nan")])
        assert _crpss_paired(crps, crps_ref) == pytest.approx(0.5)

    def test_zero_reference_returns_nan(self):
        crps = pd.Series([1.0, 2.0])
        crps_ref = pd.Series([0.0, 0.0])
        assert math.isnan(_crpss_paired(crps, crps_ref))


# ===========================================================================
# CORE-6: Brier row n_pairs / base_rate match the band-valid Brier sample
# ===========================================================================


class TestAggregateBrierBandValid:
    @staticmethod
    def _brier_kwargs() -> dict:
        return {
            "horizon": "pentad",
            "model": "TFT",
            "regime": "all",
            "season": "all",
            "code": "POOLED",
            "basin": "all",
            "norm_provenance": "official",
            "lead": None,
        }

    @staticmethod
    def _frame() -> pd.DataFrame:
        # 2 band-valid pairs (finite below_norm_prob) + 2 band-less pairs (NaN).
        # All-obs base_rate = 3/4 = 0.75; band-valid base_rate = 1/2 = 0.50.
        return pd.DataFrame(
            [
                {"below_norm_prob": 0.2, "obs_class": "below", "fc_grid_id": "g"},
                {"below_norm_prob": 0.6, "obs_class": "normal", "fc_grid_id": "g"},
                {"below_norm_prob": float("nan"), "obs_class": "below", "fc_grid_id": "g"},
                {"below_norm_prob": float("nan"), "obs_class": "below", "fc_grid_id": "g"},
            ]
        )

    def test_n_pairs_is_band_valid_count(self):
        row = _aggregate_brier(self._frame(), **self._brier_kwargs())
        assert row["n_pairs"] == 2

    def test_brier_ss_uses_band_valid_base_rate(self):
        row = _aggregate_brier(self._frame(), **self._brier_kwargs())
        # brier_mean = mean[(0.2-1)^2, (0.6-0)^2] = mean[0.64, 0.36] = 0.5.
        assert row["brier"] == pytest.approx(0.5)
        # base_rate over the band-valid subset = 0.5 → brier_clim = 0.25 →
        # brier_ss = 1 - 0.5/0.25 = -1.0.  (All-obs base_rate 0.75 would give
        # brier_clim 0.1875 → brier_ss ≈ -1.667, which this asserts against.)
        assert row["brier_ss"] == pytest.approx(-1.0)

    def test_all_band_invalid_gives_zero_pairs_and_nan(self):
        frame = pd.DataFrame(
            [
                {"below_norm_prob": float("nan"), "obs_class": "below", "fc_grid_id": "g"},
                {"below_norm_prob": float("nan"), "obs_class": "normal", "fc_grid_id": "g"},
            ]
        )
        row = _aggregate_brier(frame, **self._brier_kwargs())
        assert row["n_pairs"] == 0
        assert math.isnan(row["brier"])
        assert math.isnan(row["brier_ss"])
