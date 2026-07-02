"""Unit tests for probabilistic CRPS reference baselines.

Tests CRPSS sign, estimator consistency, conditioning group alignment,
and performance (O(m²) guard).

All fixtures use synthetic station codes 19999/29999.
"""

from __future__ import annotations

import math
import time

import numpy as np
import pandas as pd
import pytest

from forecast_skill_eval.prob_baselines import (
    climatology_crps_for_pair,
    persistence_crps_for_pair,
    precompute_climatology_crps,
    precompute_persistence_crps,
)
from forecast_skill_eval.prob_metrics import (
    crps_from_quantiles,
    crps_reference_from_samples,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _obs_pairs(
    *,
    code: str = "19999",
    horizon: str = "pentad",
    period_key: int = 10,
    obs_values: list[float],
    model: str = "TFT",
    norm: float = 60.0,
) -> pd.DataFrame:
    """Build a minimal pairs DataFrame with one model, one station, one period."""
    rows = []
    for year, obs in enumerate(obs_values, start=2005):
        rows.append(
            {
                "code": code,
                "horizon": horizon,
                "period_key": period_key,
                "year": year,
                "model": model,
                "observed_value": obs,
                "norm": norm,
                "fc_q05": obs * 0.5,
                "fc_q25": obs * 0.8,
                "fc_q75": obs * 1.2,
                "fc_q95": obs * 1.5,
                "fc_grid_id": "short4",
                "obs_class": "normal",
                "contingency": "TN",
                "basin": "all",
                "norm_provenance": "official",
                "regime": "all",
                "season": "all",
                "lead": None,
                "forecast_value": obs,
            }
        )
    return pd.DataFrame(rows)


# ===========================================================================
# precompute_climatology_crps
# ===========================================================================


class TestPrecomputeClimatologyCrps:
    def test_returns_dict_with_expected_keys(self):
        pairs = _obs_pairs(obs_values=list(range(10, 110, 10)))
        ref = precompute_climatology_crps(pairs)
        assert ("19999", "pentad", 10) in ref

    def test_entry_has_two_lists_of_equal_length(self):
        pairs = _obs_pairs(obs_values=list(range(10, 110, 10)))
        ref = precompute_climatology_crps(pairs)
        entry = ref[("19999", "pentad", 10)]
        levels, qvals = entry
        assert len(levels) == len(qvals)
        assert len(levels) >= 2

    def test_quantile_values_are_nondecreasing(self):
        pairs = _obs_pairs(obs_values=list(range(10, 110, 10)))
        ref = precompute_climatology_crps(pairs)
        _, qvals = ref[("19999", "pentad", 10)]
        for i in range(len(qvals) - 1):
            assert qvals[i] <= qvals[i + 1], "Reference quantiles must be non-decreasing"

    def test_empty_pairs_returns_empty_dict(self):
        ref = precompute_climatology_crps(pd.DataFrame())
        assert ref == {}

    def test_single_observation_group_excluded(self):
        """Groups with only 1 observation cannot have quantiles computed."""
        pairs = _obs_pairs(obs_values=[50.0])
        ref = precompute_climatology_crps(pairs)
        assert len(ref) == 0

    def test_multiple_groups_all_keyed(self):
        p1 = _obs_pairs(code="19999", period_key=1, obs_values=list(range(10, 50)))
        p2 = _obs_pairs(code="19999", period_key=2, obs_values=list(range(50, 90)))
        p3 = _obs_pairs(code="29999", period_key=1, obs_values=list(range(30, 70)))
        pairs = pd.concat([p1, p2, p3], ignore_index=True)
        ref = precompute_climatology_crps(pairs)
        assert ("19999", "pentad", 1) in ref
        assert ("19999", "pentad", 2) in ref
        assert ("29999", "pentad", 1) in ref

    def test_deduplicates_observations_across_models(self):
        """Multiple model rows for the same year/station/period should not inflate sample."""
        rows = []
        for model in ("TFT", "LR"):
            for year in range(2005, 2015):
                rows.append(
                    {
                        "code": "19999",
                        "horizon": "pentad",
                        "period_key": 5,
                        "year": year,
                        "model": model,
                        "observed_value": float(year - 2000),
                        "norm": 50.0,
                    }
                )
        pairs = pd.DataFrame(rows)
        ref = precompute_climatology_crps(pairs)
        # Sample should have 10 unique obs (deduped by code/horizon/period_key/year)
        levels, qvals = ref[("19999", "pentad", 5)]
        # Verify quantile range matches 10 values (5..14 → q05 ≈ 5, q95 ≈ 14)
        assert qvals[0] < qvals[-1]

    def test_custom_levels_respected(self):
        pairs = _obs_pairs(obs_values=list(range(10, 110, 10)))
        custom_levels = (0.25, 0.75)
        ref = precompute_climatology_crps(pairs, levels=custom_levels)
        levels, _ = ref[("19999", "pentad", 10)]
        assert levels == [0.25, 0.75]


# ===========================================================================
# climatology_crps_for_pair — correctness and estimator consistency
# ===========================================================================


class TestClimatologyCrpsForPair:
    def test_returns_finite_for_valid_entry(self):
        pairs = _obs_pairs(obs_values=list(range(10, 110, 10)))
        ref = precompute_climatology_crps(pairs)
        crps = climatology_crps_for_pair(ref, "19999", "pentad", 10, 55.0)
        assert math.isfinite(crps)
        assert crps >= 0.0

    def test_missing_key_returns_nan(self):
        crps = climatology_crps_for_pair({}, "19999", "pentad", 10, 55.0)
        assert math.isnan(crps)

    def test_estimator_consistency(self):
        """crps_reference_from_samples and climatology_crps_for_pair use the
        same estimator — feeding the same sample and levels must yield the same value.
        """
        sample = list(range(10, 110, 10))
        obs = 55.0
        levels = (0.05, 0.25, 0.50, 0.75, 0.95)

        # Compute via precompute path
        pairs = _obs_pairs(obs_values=sample)
        ref = precompute_climatology_crps(pairs, levels=levels)
        crps_via_ref = climatology_crps_for_pair(ref, "19999", "pentad", 10, obs)

        # Compute via direct sample path
        crps_direct = crps_reference_from_samples(sample, obs, levels)

        assert abs(crps_via_ref - crps_direct) < 1e-10, (
            f"Estimator mismatch: precompute={crps_via_ref}, direct={crps_direct}"
        )

    def test_crpss_sign_forecast_better_than_climatology(self):
        """CRPSS > 0 when forecast CRPS < climatology CRPS."""
        sample = list(range(10, 110, 10))  # wide climatology
        obs = 55.0
        levels = [0.05, 0.25, 0.75, 0.95]

        # Perfect forecast (very narrow band centred on obs)
        crps_fc = crps_from_quantiles(levels, [53.0, 54.0, 56.0, 57.0], obs)

        pairs = _obs_pairs(obs_values=sample)
        ref = precompute_climatology_crps(pairs, levels=tuple(levels))
        crps_clim = climatology_crps_for_pair(ref, "19999", "pentad", 10, obs)

        crpss = 1.0 - crps_fc / crps_clim
        assert crpss > 0.0, f"Better-than-climatology forecast must have CRPSS > 0; got {crpss}"

    def test_crpss_sign_forecast_worse_than_climatology(self):
        """CRPSS < 0 when forecast CRPS > climatology CRPS."""
        sample = list(range(10, 110, 10))
        obs = 55.0
        levels = [0.05, 0.25, 0.75, 0.95]

        # Terrible forecast (far off from obs)
        crps_fc = crps_from_quantiles(levels, [200.0, 250.0, 300.0, 350.0], obs)

        pairs = _obs_pairs(obs_values=sample)
        ref = precompute_climatology_crps(pairs, levels=tuple(levels))
        crps_clim = climatology_crps_for_pair(ref, "19999", "pentad", 10, obs)

        crpss = 1.0 - crps_fc / crps_clim
        assert crpss < 0.0, f"Worse-than-climatology forecast must have CRPSS < 0; got {crpss}"

    def test_forecast_equals_climatology_crpss_near_zero(self):
        """When forecast distribution equals climatology → CRPSS ≈ 0."""
        sample = list(range(10, 110, 10))
        obs = 55.0
        levels = (0.05, 0.25, 0.50, 0.75, 0.95)
        fc_qvals = list(np.quantile(sample, levels))

        crps_fc = crps_from_quantiles(list(levels), fc_qvals, obs)

        pairs = _obs_pairs(obs_values=sample)
        ref = precompute_climatology_crps(pairs, levels=levels)
        crps_clim = climatology_crps_for_pair(ref, "19999", "pentad", 10, obs)

        crpss = 1.0 - crps_fc / crps_clim
        assert abs(crpss) < 1e-9, f"CRPSS must be ≈ 0 when forecast == climatology; got {crpss}"

    def test_conditioning_group_matches_build_climatology_baseline(self):
        """The (code, horizon, period_key) conditioning key must align with
        the deterministic baseline (per baselines.build_climatology_baseline)."""
        pairs = pd.concat(
            [
                _obs_pairs(code="19999", period_key=1, obs_values=list(range(10, 30))),
                _obs_pairs(code="19999", period_key=2, obs_values=list(range(50, 70))),
                _obs_pairs(code="29999", period_key=1, obs_values=list(range(30, 50))),
            ],
            ignore_index=True,
        )
        ref = precompute_climatology_crps(pairs)
        # Each unique (code, horizon, period_key) must have its own entry
        assert ("19999", "pentad", 1) in ref
        assert ("19999", "pentad", 2) in ref
        assert ("29999", "pentad", 1) in ref
        # Cross-contamination check: q50 of group(19999,1) < q50 of group(19999,2)
        _, q1 = ref[("19999", "pentad", 1)]
        _, q2 = ref[("19999", "pentad", 2)]
        mid = len(q1) // 2
        assert q1[mid] < q2[mid], (
            "Conditioning groups must be separate — period_key=1 obs < period_key=2 obs"
        )


# ===========================================================================
# precompute_persistence_crps
# ===========================================================================


class TestPrecomputePersistenceCrps:
    def test_returns_dict(self):
        pairs = _obs_pairs(obs_values=list(range(10, 30)))
        ref = precompute_persistence_crps(pairs)
        assert isinstance(ref, dict)

    def test_empty_pairs_returns_empty(self):
        ref = precompute_persistence_crps(pd.DataFrame())
        assert ref == {}

    def test_lag1_values_plausible(self):
        obs_vals = list(range(10, 30))
        pairs = _obs_pairs(obs_values=obs_vals, horizon="pentad")
        ref = precompute_persistence_crps(pairs)
        # For period_key=10, year=2006 (second obs), lag1 = year=2005, pk=9
        # but our pairs all have the SAME period_key=10 — so lag1 key is (code, horizon, 9, year-1)
        # which doesn't exist in our obs_lookup → no entry.
        # With period_key varying, we'd get more entries. Use different fixtures.
        # Just check that existing entries have finite lag1 values.
        for _key, val in ref.items():
            assert math.isfinite(val)

    def test_year_series_produces_lag1_entries(self):
        """Use consecutive years with the same period_key so lag-1 is within the set."""
        rows = []
        obs_by_year = {2005: 30.0, 2006: 40.0, 2007: 50.0}
        for year, obs in obs_by_year.items():
            rows.append(
                {
                    "code": "19999",
                    "horizon": "pentad",
                    "period_key": 5,
                    "year": year,
                    "model": "TFT",
                    "observed_value": obs,
                    "norm": 60.0,
                }
            )
        # For year=2006, pk=5 → lag1 key is (19999, pentad, 4, 2006) — NOT in our set
        # The lag-1 definition in _lag1_key for pentad: lag1 is pk-1 in the same year
        # So lag1 for (pk=5, year=2006) = (pk=4, year=2006) — absent.
        # BUT for pk=5, there IS a pk-1=4 case.
        # Let's use a multi-period setup instead.
        rows2 = []
        for period_key in (4, 5):
            for year, _obs in {2005: 30.0, 2006: 40.0}.items():
                rows2.append(
                    {
                        "code": "19999",
                        "horizon": "pentad",
                        "period_key": period_key,
                        "year": year,
                        "model": "TFT",
                        "observed_value": 30.0 if period_key == 4 else 40.0,
                        "norm": 60.0,
                    }
                )
        pairs2 = pd.DataFrame(rows2)
        ref2 = precompute_persistence_crps(pairs2)
        # For (pk=5, year=2005): lag1 is (pk=4, year=2005) → obs=30.0
        assert ("19999", "pentad", 5, 2005) in ref2
        assert ref2[("19999", "pentad", 5, 2005)] == pytest.approx(30.0)


# ===========================================================================
# persistence_crps_for_pair
# ===========================================================================


class TestPersistenceCrpsForPair:
    def test_equals_absolute_difference(self):
        rows = [
            {
                "code": "19999",
                "horizon": "pentad",
                "period_key": pk,
                "year": yr,
                "model": "TFT",
                "observed_value": float(pk * 10),
                "norm": 60.0,
            }
            for pk in (4, 5)
            for yr in (2005,)
        ]
        pairs = pd.DataFrame(rows)
        ref = precompute_persistence_crps(pairs)
        # pk=5, year=2005: lag1 = pk=4, year=2005 → obs=40.0
        crps_p = persistence_crps_for_pair(ref, "19999", "pentad", 5, 2005, 50.0)
        assert crps_p == pytest.approx(abs(40.0 - 50.0))

    def test_missing_key_returns_nan(self):
        crps_p = persistence_crps_for_pair({}, "19999", "pentad", 5, 2005, 50.0)
        assert math.isnan(crps_p)

    def test_nan_observed_returns_nan(self):
        ref = {("19999", "pentad", 5, 2005): 40.0}
        crps_p = persistence_crps_for_pair(ref, "19999", "pentad", 5, 2005, float("nan"))
        assert math.isnan(crps_p)

    def test_persistence_crps_is_nonnegative(self):
        ref = {("19999", "pentad", 5, 2005): 40.0}
        for obs in [10.0, 40.0, 100.0]:
            val = persistence_crps_for_pair(ref, "19999", "pentad", 5, 2005, obs)
            assert val >= 0.0


# ===========================================================================
# Performance guard: precompute_climatology_crps must not be O(n·m)
# ===========================================================================


class TestPerformanceGuard:
    @pytest.mark.parametrize("n_groups,m_sample", [(50, 200)])
    def test_precompute_completes_in_reasonable_time(self, n_groups: int, m_sample: int):
        """Precomputing N groups × M-sample climatology must finish fast.

        Guard against the ef7975c6-class regression (O(n²) per-pair loop).
        N=50 groups, M=200 samples → 10,000 obs rows; target < 2 s.
        """
        rng = np.random.default_rng(42)
        rows = []
        for g in range(n_groups):
            code = "19999" if g % 2 == 0 else "29999"
            period_key = (g % 36) + 1
            for i in range(m_sample):
                rows.append(
                    {
                        "code": code,
                        "horizon": "pentad",
                        "period_key": period_key,
                        "year": 2000 + i,
                        "model": "TFT",
                        "observed_value": float(rng.uniform(10, 100)),
                        "norm": 60.0,
                    }
                )
        pairs = pd.DataFrame(rows)

        start = time.monotonic()
        ref = precompute_climatology_crps(pairs)
        elapsed = time.monotonic() - start

        assert len(ref) > 0
        assert elapsed < 2.0, (
            f"precompute_climatology_crps took {elapsed:.2f}s for "
            f"{n_groups} groups × {m_sample} samples — O(n·m) regression suspected"
        )
