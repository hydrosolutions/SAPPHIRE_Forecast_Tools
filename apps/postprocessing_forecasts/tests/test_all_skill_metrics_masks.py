"""Regression/contract tests for calculate_all_skill_metrics()'s per-metric
NaN masking (M3, finding #2 in doc/plans/postprocessing_skill_correctness_design.md).

Locks the fix that splits the single combined obs/sim/delta mask into:
  - an obs/sim-only mask used for point metrics (mae, sdivsigma, nse,
    pbias, kgelf, nse_log) and the stored n_pairs count
  - an independent obs/sim/delta mask used only for accuracy/delta

Before the fix, a single NaN in the delta column dropped an otherwise
valid obs/sim row from EVERY metric (including nse/mae/n_pairs), which
disagreed with the standalone sdivsigma_nse()/mae() helpers in the same
module and silently starved NSE/MAE of valid obs/sim pairs whenever
delta happened to be NaN for that row.
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

from src.skill_metrics import (
    calculate_all_skill_metrics,
    forecast_accuracy_hydromet,
    mae,
    sdivsigma_nse,
)


def _base_df(nan_delta_at: int | None = 2) -> pd.DataFrame:
    """10-row synthetic obs/sim/delta DataFrame.

    obs/sim are fully finite and varied across all 10 rows so that
    min-points-gated metrics (kgelf needs >= 10, sdivsigma/nse/nse_log
    need >= 2) are actually computed rather than short-circuited by a
    min-points guard.

    If `nan_delta_at` is given, that row's delta is set to NaN (the
    rest of delta stays finite and non-trivial/varied — not all equal
    to a single value). Pass None for an all-finite-delta DataFrame
    (used by the regression test, Test C).
    """
    df = pd.DataFrame(
        {
            "observed": [100.0, 110.0, 105.0, 115.0, 108.0, 120.0, 95.0, 130.0, 102.0, 111.0],
            "simulated": [102.0, 108.0, 106.0, 112.0, 110.0, 118.0, 97.0, 125.0, 101.0, 113.0],
            "delta": [5.0, 5.0, 3.0, 6.0, 4.0, 7.0, 5.0, 8.0, 5.0, 6.0],
        }
    )
    if nan_delta_at is not None:
        df.loc[nan_delta_at, "delta"] = np.nan
    return df


class TestObsSimOnlyMaskForPointMetrics:
    """Test A (finding #2 / M3).

    A NaN delta value must NOT drop an otherwise-valid obs/sim row from
    the point metrics (nse, sdivsigma, mae), and the stored n_pairs must
    reflect the obs/sim-only valid count, not the obs/sim/delta count.
    """

    def test_nan_delta_row_still_counts_for_nse_sdivsigma_mae(self):
        df = _base_df(nan_delta_at=2)

        result = calculate_all_skill_metrics(df, "observed", "simulated", "delta")

        # The standalone helpers only ever mask on obs/sim finiteness
        # (they don't even take a delta_col argument), so they
        # naturally include all 10 rows, including the NaN-delta row.
        standalone = sdivsigma_nse(df, "observed", "simulated")
        standalone_mae = mae(df, "observed", "simulated")

        assert result["nse"] == pytest.approx(standalone["nse"])
        assert result["sdivsigma"] == pytest.approx(standalone["sdivsigma"])
        assert result["mae"] == pytest.approx(standalone_mae["mae"])

        # n_pairs = obs/sim-valid count (all 10 rows), NOT the
        # obs/sim/delta-valid count (9 rows) that pre-fix code used.
        assert result["n_pairs"] == len(df)


class TestDeltaMaskStillExcludesNanDeltaRow:
    """Test B (finding #2 / M3).

    accuracy/delta must still be computed from the delta-valid subset
    only — splitting the masks must not leak the NaN-delta row into
    accuracy's denominator.
    """

    def test_accuracy_and_delta_match_delta_valid_subset(self):
        df = _base_df(nan_delta_at=2)

        result = calculate_all_skill_metrics(df, "observed", "simulated", "delta")

        # forecast_accuracy_hydromet() already does its own correct
        # obs/sim/delta masking over the full df, so comparing against
        # it directly proves accuracy/delta come from the delta-valid
        # subset (9 rows) only, not from all 10 obs/sim-valid rows.
        expected = forecast_accuracy_hydromet(df, "observed", "simulated", "delta")

        assert result["accuracy"] == pytest.approx(expected["accuracy"])
        assert result["delta"] == pytest.approx(expected["delta"])


class TestAllDeltaFiniteRegression:
    """Test C (finding #2 / M3) — numerical-equivalence regression guard.

    When delta is finite everywhere (the common real-world case — the
    monthly reader fills delta with 0.0, never NaN), the split-mask fix
    must be numerically UNCHANGED from today's single-combined-mask
    behavior. Rather than hardcoding today's output values (brittle to
    legitimate future changes elsewhere in the function), this asserts
    equality against the standalone helpers directly: with no NaN
    delta, the obs/sim-only mask and the obs/sim/delta mask select the
    exact same rows, so old and new code necessarily agree here. This
    doubles as both a correctness check and the "unchanged in the
    common case" regression guard required by M3.
    """

    def test_all_finite_delta_matches_standalone_helpers(self):
        df = _base_df(nan_delta_at=None)

        result = calculate_all_skill_metrics(df, "observed", "simulated", "delta")

        standalone = sdivsigma_nse(df, "observed", "simulated")
        standalone_mae = mae(df, "observed", "simulated")
        standalone_accuracy = forecast_accuracy_hydromet(df, "observed", "simulated", "delta")

        assert result["nse"] == pytest.approx(standalone["nse"])
        assert result["sdivsigma"] == pytest.approx(standalone["sdivsigma"])
        assert result["mae"] == pytest.approx(standalone_mae["mae"])
        assert result["accuracy"] == pytest.approx(standalone_accuracy["accuracy"])
        assert result["delta"] == pytest.approx(standalone_accuracy["delta"])
        assert result["n_pairs"] == len(df)

        # Sanity: with >= 10 varied points and no NaNs, kgelf/nse_log/
        # pbias should actually be computed, not NaN'd out by a
        # min-points guard — otherwise this test wouldn't exercise the
        # metrics it's meant to lock down.
        assert np.isfinite(result["kgelf"])
        assert np.isfinite(result["nse_log"])
        assert np.isfinite(result["pbias"])
