"""Unit tests for the reliability/rank table builder (build_prob_reliability).

Additional tests beyond those in test_prob_metrics.py, focused on structure,
calibration, Wilson CI, and the group-key alignment.

All fixtures use synthetic station codes 19999/29999.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from forecast_skill_eval.prob_metrics import (
    PROB_RELIABILITY_COLUMNS,
    build_prob_reliability,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_pairs(
    *,
    code: str = "19999",
    horizon: str = "pentad",
    period_key: int = 1,
    model: str = "TFT",
    obs_values: list[float],
    q05: float = 10.0,
    q25: float = 30.0,
    q50: float = 50.0,
    q75: float = 70.0,
    q95: float = 90.0,
    q10: float | None = None,
    q90: float | None = None,
    norm: float = 60.0,
) -> pd.DataFrame:
    rows = []
    for year, obs in enumerate(obs_values, start=2000):
        rows.append(
            {
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
                "fc_grid_id": "long7" if q10 is not None else "short5",
                "obs_class": "below" if obs < norm else "normal",
                "contingency": "TN",
                "regime": "all",
                "season": "all",
                "basin": "all",
                "norm_provenance": "official",
                "lead": None,
                "forecast_value": q50,
            }
        )
    return pd.DataFrame(rows)


# ===========================================================================
# Column and schema tests
# ===========================================================================


class TestReliabilitySchema:
    def test_all_required_columns_present(self):
        pairs = _build_pairs(obs_values=list(range(0, 100, 10)))
        result = build_prob_reliability(pairs)
        for col in PROB_RELIABILITY_COLUMNS:
            assert col in result.columns, f"Missing column: {col}"

    def test_no_extra_columns(self):
        """Output must contain exactly the PROB_RELIABILITY_COLUMNS, no more."""
        pairs = _build_pairs(obs_values=list(range(0, 100, 10)))
        result = build_prob_reliability(pairs)
        extra = set(result.columns) - set(PROB_RELIABILITY_COLUMNS)
        assert not extra, f"Unexpected extra columns: {extra}"

    def test_nominal_level_values_are_valid_quantile_levels(self):
        from forecast_skill_eval.prob_metrics import QUANTILE_LEVELS

        pairs = _build_pairs(
            obs_values=list(range(0, 100, 10)),
            q10=20.0,
            q90=80.0,
        )
        result = build_prob_reliability(pairs)
        valid = set(QUANTILE_LEVELS)
        for lvl in result["nominal_level"].unique():
            assert lvl in valid, f"Unexpected nominal_level: {lvl}"


# ===========================================================================
# Calibration tests
# ===========================================================================


class TestReliabilityCalibration:
    def test_calibrated_50pct_band_matches_nominal(self):
        """If 50% of obs <= q75 and 50% > q25, the 50% band is calibrated."""
        # 20 obs uniformly spaced [0, 100]; q25=25, q75=75
        # Fraction obs <= q25=25: obs in {0,10,20} → 3/20 → observed_freq(0.25) ≈ 0.15 (loose)
        # Fraction obs <= q75=75: obs in {0..70} → 8/20 = 0.40 → not exactly 0.75
        # Use a scenario with exactly calibrated outcomes:
        obs_vals = list(np.linspace(0, 100, 100))  # 100 uniform obs
        # q05=5, q95=95: ~90% of obs should fall within [5,95] → calibrated
        pairs = _build_pairs(
            obs_values=obs_vals,
            q05=5.0,
            q25=25.0,
            q50=50.0,
            q75=75.0,
            q95=95.0,
        )
        result = build_prob_reliability(pairs)
        pooled = result[result["code"] == "POOLED"]

        # Check q95 level: ~95% of obs are <= 95.0
        q95_rows = pooled[pooled["nominal_level"] == 0.95]
        if len(q95_rows) > 0:
            freq = q95_rows["observed_frequency"].iloc[0]
            # linspace(0,100,100) has 95 values <= 95 → 95/100 = 0.95
            assert abs(freq - 0.95) < 0.05, f"Expected ≈0.95, got {freq}"

        # Check q05 level: ~5% of obs are <= 5.0
        q05_rows = pooled[pooled["nominal_level"] == 0.05]
        if len(q05_rows) > 0:
            freq = q05_rows["observed_frequency"].iloc[0]
            assert abs(freq - 0.05) < 0.1, f"Expected ≈0.05, got {freq}"

    def test_overconfident_band_undercoverage(self):
        """Narrow band [49,51] should have << 90% of obs <= q95=51."""
        obs_vals = list(range(0, 100, 5))  # diverse obs far outside [49,51]
        pairs = _build_pairs(
            obs_values=obs_vals,
            q05=49.0,
            q25=49.5,
            q50=50.0,
            q75=50.5,
            q95=51.0,
        )
        result = build_prob_reliability(pairs)
        pooled = result[result["code"] == "POOLED"]
        q95_rows = pooled[pooled["nominal_level"] == 0.95]
        if len(q95_rows) > 0:
            freq = q95_rows["observed_frequency"].iloc[0]
            # Only obs <= 51: very few → undercoverage
            assert freq < 0.60, f"Overconfident band should show undercoverage; freq={freq}"

    def test_underconfident_band_overcoverage(self):
        """Very wide band [0, 1000] → obs always inside → coverage = 1.0."""
        obs_vals = list(range(10, 100, 10))
        pairs = _build_pairs(
            obs_values=obs_vals,
            q05=0.0,
            q25=1.0,
            q50=2.0,
            q75=3.0,
            q95=1000.0,
        )
        result = build_prob_reliability(pairs)
        pooled = result[result["code"] == "POOLED"]
        q95_rows = pooled[pooled["nominal_level"] == 0.95]
        if len(q95_rows) > 0:
            freq = q95_rows["observed_frequency"].iloc[0]
            assert freq == pytest.approx(1.0), f"Wide band should have coverage 1.0; got {freq}"


# ===========================================================================
# Group structure tests
# ===========================================================================


class TestReliabilityGroupStructure:
    def test_pooled_and_per_station_both_emitted(self):
        p1 = _build_pairs(code="19999", obs_values=list(range(10, 50)))
        p2 = _build_pairs(code="29999", obs_values=list(range(50, 90)))
        pairs = pd.concat([p1, p2], ignore_index=True)
        result = build_prob_reliability(pairs)
        codes = set(result["code"].unique())
        assert "POOLED" in codes
        assert "19999" in codes
        assert "29999" in codes

    def test_short_grid_q10_q90_absent_not_emitted(self):
        """Short grid (q10/q90 are None) must not emit rows for level 0.10/0.90."""
        pairs = _build_pairs(obs_values=list(range(10, 50)), q10=None, q90=None)
        result = build_prob_reliability(pairs)
        levels_present = set(result["nominal_level"].unique())
        # 0.10 and 0.90 should NOT be present when the columns are all-NaN
        assert 0.10 not in levels_present
        assert 0.90 not in levels_present

    def test_long_grid_q10_q90_present(self):
        """Long grid with q10/q90 populated → levels 0.10 and 0.90 emitted."""
        pairs = _build_pairs(
            obs_values=list(range(10, 50)),
            q10=15.0,
            q90=85.0,
        )
        result = build_prob_reliability(pairs)
        levels_present = set(result["nominal_level"].unique())
        assert 0.10 in levels_present
        assert 0.90 in levels_present

    def test_n_column_equals_non_nan_count(self):
        n = 15
        obs_vals = list(range(10, 10 + n))
        pairs = _build_pairs(obs_values=obs_vals)
        result = build_prob_reliability(pairs)
        pooled = result[result["code"] == "POOLED"]
        if len(pooled) > 0:
            # n should equal the number of non-NaN pairs for each level
            assert pooled["n"].max() == n

    def test_empty_input_returns_empty_frame_with_columns(self):
        result = build_prob_reliability(pd.DataFrame())
        assert result.empty
        for col in PROB_RELIABILITY_COLUMNS:
            assert col in result.columns

    def test_multiple_horizons_keyed_separately(self):
        p1 = _build_pairs(horizon="pentad", obs_values=list(range(10, 50)))
        p2 = _build_pairs(horizon="decade", obs_values=list(range(10, 50)))
        pairs = pd.concat([p1, p2], ignore_index=True)
        result = build_prob_reliability(pairs)
        horizons = set(result["horizon"].unique())
        assert "pentad" in horizons
        assert "decade" in horizons


# ===========================================================================
# Observed frequency bounds
# ===========================================================================


class TestObservedFrequencyBounds:
    def test_frequencies_always_in_zero_one(self):
        rng = np.random.default_rng(7)
        obs_vals = rng.uniform(0, 100, 50).tolist()
        pairs = _build_pairs(obs_values=obs_vals, q10=20.0, q90=80.0)
        result = build_prob_reliability(pairs)
        freq_col = result["observed_frequency"].dropna()
        assert (freq_col >= 0.0).all()
        assert (freq_col <= 1.0).all()

    def test_all_obs_below_q05_gives_frequency_one_for_all_levels(self):
        """All obs << q05 → hit = 1 for every level → freq = 1.0."""
        obs_vals = [1.0] * 10  # all obs far below q05=100
        pairs = _build_pairs(
            obs_values=obs_vals,
            q05=100.0,
            q25=200.0,
            q50=300.0,
            q75=400.0,
            q95=500.0,
        )
        result = build_prob_reliability(pairs)
        pooled = result[result["code"] == "POOLED"]
        # All obs (1.0) are <= every quantile (100..500) → freq = 1.0 for all
        freqs = pooled["observed_frequency"].to_numpy()
        assert np.allclose(freqs, 1.0), f"Expected all 1.0, got {freqs}"

    def test_all_obs_above_q95_gives_frequency_zero_for_all_levels(self):
        """All obs >> q95 → hit = 0 for every level → freq = 0.0."""
        obs_vals = [1000.0] * 10
        pairs = _build_pairs(
            obs_values=obs_vals,
            q05=1.0,
            q25=2.0,
            q50=3.0,
            q75=4.0,
            q95=5.0,
        )
        result = build_prob_reliability(pairs)
        pooled = result[result["code"] == "POOLED"]
        freqs = pooled["observed_frequency"].to_numpy()
        assert np.allclose(freqs, 0.0), f"Expected all 0.0, got {freqs}"
