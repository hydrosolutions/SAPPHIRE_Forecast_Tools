"""Locked tests for the LONG-TERM Skilled-Mean NSE>0 relaxation.

Owner-locked goal
-----------------
The LONG-TERM (month/quarter/season) "Skilled Mean" ensemble should include
ALL long-term models with NSE > 0.  SHORT-TERM (pentad/decad) is UNCHANGED.
**EM MUST NOT CHANGE** — neither membership, skill, nor forecast output.

These tests are LOCKED: they assert the target (post-fix) behavior.  Where they
describe NEW behavior they MUST fail against current code and pass after the
fix.  Where they lock EXISTING behavior (EM byte-identity, short-term gate,
single-model discard) they must pass both before and after.

Mechanism under test
---------------------
- filter_for_highly_skilled_forecasts(skill_stats, **overrides) AND-filters over
  THRESHOLD_METRICS keyed exactly {sdivsigma, nse, accuracy}; the exact string
  "False" (and, after the fix, any parsed disable token) disables a gate.
- Identity: NSE = 1 - sdivsigma^2.  "NSE>0 only" for long-term requires
  nse=0.0 AND both sdivsigma and accuracy gates disabled.
- New config surface (to be added by the implementer):
    ieasyhydroforecast_nse_threshold_long_term        (default 0.0)
    ieasyhydroforecast_efficiency_threshold_long_term (default disabled)
    ieasyhydroforecast_accuracy_threshold_long_term   (default disabled)
  plus a shared robust parser and a single _long_term_threshold_overrides()
  helper returning the override dict for the Skilled-Mean calls.

Placeholder station code 19999 only.
"""

import os
import sys
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src import skill_metrics as sm_mod
from src.ensemble_calculator import create_monthly_ensemble_forecasts
from src.skill_metrics import filter_for_highly_skilled_forecasts

CODE = "19999"

# Short-term / EM DEFAULT gate (unchanged behavior).
DEFAULT_ENV = {
    "ieasyhydroforecast_efficiency_threshold": "0.6",
    "ieasyhydroforecast_nse_threshold": "0.8",
    "ieasyhydroforecast_accuracy_threshold": "0.8",
}

# Env with the long-term-only vars explicitly unset so defaults apply.
_LT_ENV_KEYS = (
    "ieasyhydroforecast_nse_threshold_long_term",
    "ieasyhydroforecast_efficiency_threshold_long_term",
    "ieasyhydroforecast_accuracy_threshold_long_term",
)


# ---------------------------------------------------------------------------
# Fixture builders (mirror test_monthly_ensemble_creation.py conventions)
# ---------------------------------------------------------------------------

_SKILL_COLS = [
    "month_in_year",
    "horizon_value",
    "code",
    "model_short",
    "sdivsigma",
    "nse",
    "delta",
    "accuracy",
    "mae",
    "n_pairs",
]

_FC_COLS = [
    "code",
    "year",
    "month",
    "month_in_year",
    "horizon_value",
    "model_short",
    "forecasted_discharge",
    "q05",
    "q10",
    "q25",
    "q50",
    "q75",
    "q90",
    "q95",
    "valid_from",
    "valid_to",
    "date",
    "flag",
]


def _skill(rows, cols=_SKILL_COLS):
    return pd.DataFrame(rows, columns=cols)


def _forecasts(rows, cols=_FC_COLS):
    return pd.DataFrame(rows, columns=cols)


def _fc_row(model, q50, *, hv=1, month=3, spread=20.0):
    """One monthly forecast row with a symmetric quantile fan around q50."""
    return (
        CODE,
        2025,
        month,
        month,
        hv,
        model,
        q50,
        q50 - spread,
        q50 - spread * 0.75,
        q50 - spread * 0.5,
        q50,
        q50 + spread * 0.5,
        q50 + spread * 0.75,
        q50 + spread,
        "2025-03-01",
        "2025-03-31",
        "2025-03-01",
        0,
    )


# ---------------------------------------------------------------------------
# 1. Config parsing helper (NEW — fails on current code: helpers absent)
# ---------------------------------------------------------------------------


class TestThresholdEnvParsing:
    """Robust parser for the long-term threshold env vars."""

    def _parser(self):
        parser = getattr(sm_mod, "_parse_threshold_env", None)
        if parser is None:
            pytest.fail(
                "skill_metrics._parse_threshold_env missing — the shared robust "
                "threshold parser has not been implemented yet."
            )
        return parser

    def test_numeric_string_returns_float(self):
        parse = self._parser()
        assert parse("0.0") == pytest.approx(0.0)
        assert parse("0.8") == pytest.approx(0.8)

    @pytest.mark.parametrize(
        "token", ["False", "false", "off", "none", "None", "disable", "DISABLE", ""]
    )
    def test_disable_tokens_disable_gate(self, token):
        """Disable tokens parse to a value that disables the gate in the filter.

        The filter treats str(threshold) == "False" as "skip this gate", so the
        parser must map every disable token to something whose str() is "False"
        (i.e. the bool/str sentinel False), never to a float.
        """
        parse = self._parser()
        result = parse(token)
        assert str(result) == "False", (
            f"disable token {token!r} must disable the gate (str(result) must be "
            f"'False'), got {result!r}"
        )

    def test_case_insensitive_false(self):
        parse = self._parser()
        assert str(parse("FALSE")) == "False"
        assert str(parse("fAlSe")) == "False"

    def test_invalid_value_raises_clear_error(self):
        """An invalid value raises a clear config error, not a bare ValueError
        from float('banana')."""
        parse = self._parser()
        with pytest.raises(Exception) as excinfo:
            parse("banana")
        msg = str(excinfo.value).lower()
        assert "banana" in msg or "threshold" in msg or "invalid" in msg, (
            "config error message should name the offending value or be about a "
            f"threshold; got: {excinfo.value!r}"
        )


class TestLongTermOverridesHelper:
    """_long_term_threshold_overrides() returns the NSE>0-only override dict."""

    def _helper(self):
        helper = getattr(sm_mod, "_long_term_threshold_overrides", None)
        if helper is None:
            pytest.fail(
                "skill_metrics._long_term_threshold_overrides missing — the LT "
                "override helper has not been implemented yet."
            )
        return helper

    def test_defaults_are_nse_zero_others_disabled(self):
        helper = self._helper()
        with patch.dict(os.environ, {}, clear=False):
            for k in _LT_ENV_KEYS:
                os.environ.pop(k, None)
            overrides = helper()
        assert set(overrides.keys()) == {"sdivsigma", "nse", "accuracy"}
        # M2: long-term NSE default is now a small positive epsilon (not exactly
        # 0.0) so that under the inclusive `>=` gate, nse == 0.0 still fails.
        assert 0.0 < float(overrides["nse"]) < 1e-6
        assert str(overrides["sdivsigma"]) == "False"
        assert str(overrides["accuracy"]) == "False"

    def test_env_can_override_lt_nse(self):
        helper = self._helper()
        env = {"ieasyhydroforecast_nse_threshold_long_term": "0.5"}
        with patch.dict(os.environ, env):
            overrides = helper()
        assert float(overrides["nse"]) == pytest.approx(0.5)

    def test_lt_overrides_broaden_filter_regardless_of_sdivsigma_accuracy(self):
        """Applying the LT overrides to the filter keeps NSE>0 models with
        arbitrary (failing) sdivsigma/accuracy and drops NSE<=0."""
        helper = self._helper()
        # nse independent of sdivsigma in the fixture — deliberately violates
        # the physical identity to prove the gate depends ONLY on nse.
        skill = _skill(
            [
                (3, 1, CODE, "M_neg", 5.0, -0.2, 5.0, 0.10, 3.0, 10),
                (3, 1, CODE, "M_low", 5.0, 0.10, 5.0, 0.10, 3.0, 10),
                (3, 1, CODE, "M_mid", 5.0, 0.64, 5.0, 0.10, 3.0, 10),
                (3, 1, CODE, "M_high", 5.0, 0.90, 5.0, 0.10, 3.0, 10),
                (3, 1, CODE, "M_zero", 5.0, 0.00, 5.0, 0.99, 1.0, 10),  # NSE==0 excluded
            ]
        )
        with patch.dict(os.environ, DEFAULT_ENV):
            for k in _LT_ENV_KEYS:
                os.environ.pop(k, None)
            overrides = helper()
            kept = filter_for_highly_skilled_forecasts(skill, **overrides)
        kept_models = set(kept["model_short"])
        assert kept_models == {"M_low", "M_mid", "M_high"}, (
            f"LT filter should keep exactly NSE>0 models, got {kept_models}"
        )


# ---------------------------------------------------------------------------
# 2. Short-term gate UNCHANGED (regression lock — passes before and after)
# ---------------------------------------------------------------------------


class TestShortTermGateUnchanged:
    """Pentad/decad still require NSE>0.8 AND sdivsigma<0.6 AND accuracy>0.8."""

    def _st_skill(self):
        return _skill(
            [
                # passes all three
                (3, 1, CODE, "PASS", 0.30, 0.95, 5.0, 0.90, 2.0, 10),
                # NSE ok but sdivsigma too high
                (3, 1, CODE, "BAD_SDIV", 0.90, 0.90, 5.0, 0.90, 2.0, 10),
                # sdivsigma ok, accuracy too low
                (3, 1, CODE, "BAD_ACC", 0.30, 0.90, 5.0, 0.10, 2.0, 10),
                # sdivsigma/accuracy ok, NSE too low (but >0)
                (3, 1, CODE, "BAD_NSE", 0.30, 0.50, 5.0, 0.90, 2.0, 10),
            ]
        )

    def test_default_gate_keeps_only_all_pass(self):
        with patch.dict(os.environ, DEFAULT_ENV):
            kept = filter_for_highly_skilled_forecasts(self._st_skill())
        assert set(kept["model_short"]) == {"PASS"}

    def test_nse_between_zero_and_point8_rejected_by_default(self):
        """A model with 0 < NSE < 0.8 is rejected by the DEFAULT (short-term)
        gate — this is the behavior long-term relaxes but short-term keeps."""
        with patch.dict(os.environ, DEFAULT_ENV):
            kept = filter_for_highly_skilled_forecasts(self._st_skill())
        assert "BAD_NSE" not in set(kept["model_short"])


# ---------------------------------------------------------------------------
# 3. Monthly forecast side: EM UNCHANGED while Skilled Mean broadens
# ---------------------------------------------------------------------------


def _mixed_pool():
    """Three models: two pass the DEFAULT gate (EM pool), one only passes
    the relaxed NSE>0 gate (extra Skilled-Mean member).

    - LR  : passes default (sdivsigma<0.6, nse>0.8, accuracy>0.8)
    - TFT : passes default
    - GBT : NSE=0.3 (>0) but accuracy 0.1 and sdivsigma 0.9 -> only in LT pool
    """
    skill = _skill(
        [
            (3, 1, CODE, "LR", 0.30, 0.95, 5.0, 0.90, 2.0, 10),
            (3, 1, CODE, "TFT", 0.40, 0.88, 5.0, 0.85, 4.0, 10),
            (3, 1, CODE, "GBT", 0.90, 0.30, 5.0, 0.10, 6.0, 10),
        ]
    )
    forecasts = _forecasts(
        [
            _fc_row("LR", 100.0),
            _fc_row("TFT", 120.0),
            _fc_row("GBT", 300.0),
        ]
    )
    return skill, forecasts


def _em_signature(result):
    """Byte-identical EM signature: key columns + composition + discharge +
    every quantile, as a sorted tuple of rounded tuples."""
    em = result[result["model_short"] == "EM"].copy()
    qcols = ["q05", "q10", "q25", "q50", "q75", "q90", "q95"]
    keep = ["code", "year", "month", "composition", "forecasted_discharge", *qcols]
    if "horizon_value" in em.columns:
        keep.insert(3, "horizon_value")
    em = em[keep].sort_values(keep).reset_index(drop=True)
    rows = []
    for _, r in em.iterrows():
        rows.append(
            tuple(
                round(float(r[c]), 9) if c in ("forecasted_discharge", *qcols) else r[c]
                for c in keep
            )
        )
    return tuple(rows)


class TestEMUnchangedForecastOutput:
    """The relaxed Skilled Mean must NOT perturb the EM forecast row."""

    def test_em_row_byte_identical_to_default(self):
        """EM output with the relaxation active is byte-identical to EM output
        under the default gate applied uniformly.

        Baseline: force the SAME gate everywhere by making the LT vars equal to
        the short-term defaults (so nothing is relaxed).  Then run with the LT
        relaxation defaults active.  The EM row must match exactly."""
        skill, forecasts = _mixed_pool()

        # Baseline: LT vars mirror the default gate -> no relaxation anywhere.
        baseline_env = dict(DEFAULT_ENV)
        baseline_env.update(
            {
                "ieasyhydroforecast_nse_threshold_long_term": "0.8",
                "ieasyhydroforecast_efficiency_threshold_long_term": "0.6",
                "ieasyhydroforecast_accuracy_threshold_long_term": "0.8",
            }
        )
        with patch.dict(os.environ, baseline_env):
            baseline = create_monthly_ensemble_forecasts(forecasts.copy(), skill.copy())

        # Relaxed: LT defaults (NSE>0 only) active.
        with patch.dict(os.environ, DEFAULT_ENV):
            for k in _LT_ENV_KEYS:
                os.environ.pop(k, None)
            relaxed = create_monthly_ensemble_forecasts(forecasts.copy(), skill.copy())

        assert _em_signature(relaxed) == _em_signature(baseline), (
            "EM forecast row changed when the Skilled-Mean relaxation was enabled "
            "— EM MUST be byte-identical."
        )

    def test_em_is_only_lr_tft_mean(self):
        """EM composition and discharge reflect ONLY the default-gate models,
        even though GBT now enters the Skilled-Mean pool."""
        skill, forecasts = _mixed_pool()
        with patch.dict(os.environ, DEFAULT_ENV):
            for k in _LT_ENV_KEYS:
                os.environ.pop(k, None)
            result = create_monthly_ensemble_forecasts(forecasts, skill)
        em = result[result["model_short"] == "EM"]
        assert len(em) == 1
        assert em.iloc[0]["composition"] == "LR, TFT"
        assert em.iloc[0]["forecasted_discharge"] == pytest.approx((100.0 + 120.0) / 2.0)

    def test_skilled_mean_admits_extra_lt_model(self):
        """NEW: Skilled Mean includes GBT (NSE>0) — its composition is the
        3-model set, distinct from EM's 2-model set."""
        skill, forecasts = _mixed_pool()
        with patch.dict(os.environ, DEFAULT_ENV):
            for k in _LT_ENV_KEYS:
                os.environ.pop(k, None)
            result = create_monthly_ensemble_forecasts(forecasts, skill)
        sm = result[result["model_short"] == "Skilled Mean"]
        assert len(sm) == 1, "Expected one Skilled Mean row"
        assert sm.iloc[0]["composition"] == "GBT, LR, TFT", (
            "Skilled Mean must admit the extra NSE>0 long-term model (GBT)"
        )


# ---------------------------------------------------------------------------
# 4. Lead-aware BOTH-SIDES and one-sided-absence regression
# ---------------------------------------------------------------------------


class TestLeadAwareSkilledMean:
    """Monthly Skilled Mean selection must be per-(code, month, horizon_value)
    when horizon_value is present on BOTH the forecast and skill sides; a
    one-sided presence must fall back to the 3-key legacy merge without
    mismatch."""

    def test_both_sides_lead_aware_pool_per_horizon(self):
        """Two leads (hv=1, hv=2) for the same month.  Each lead has its own
        qualifying pool; Skilled Mean must not blend across leads."""
        skill = _skill(
            [
                # hv=1: LR + GBT qualify (NSE>0)
                (3, 1, CODE, "LR", 0.30, 0.95, 5.0, 0.90, 2.0, 10),
                (3, 1, CODE, "GBT", 0.90, 0.30, 5.0, 0.10, 4.0, 10),
                # hv=2: only TFT has NSE>0; the other is NSE<=0 (excluded)
                (3, 2, CODE, "TFT", 0.40, 0.50, 5.0, 0.10, 3.0, 10),
                (3, 2, CODE, "GBT", 0.90, -0.10, 5.0, 0.10, 4.0, 10),
            ]
        )
        forecasts = _forecasts(
            [
                _fc_row("LR", 100.0, hv=1),
                _fc_row("GBT", 300.0, hv=1),
                _fc_row("TFT", 200.0, hv=2),
                _fc_row("GBT", 500.0, hv=2),
            ]
        )
        with patch.dict(os.environ, DEFAULT_ENV):
            for k in _LT_ENV_KEYS:
                os.environ.pop(k, None)
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        sm = result[result["model_short"] == "Skilled Mean"]
        # hv=1: two-model SM (LR, GBT) -> a row exists
        sm1 = sm[sm["horizon_value"] == 1]
        assert len(sm1) == 1, "hv=1 should produce one Skilled Mean row"
        assert sm1.iloc[0]["composition"] == "GBT, LR"
        # hv=2: only TFT qualifies -> single-model -> discarded, no blend into hv=1
        sm2 = sm[sm["horizon_value"] == 2]
        assert len(sm2) == 0, "hv=2 single-model Skilled Mean must be discarded"

    def test_one_sided_absence_falls_back_to_3key(self):
        """REGRESSION: forecasts carry horizon_value but skill rows do NOT.
        The merge must fall back to the 3-key legacy form (no horizon_value)
        and still produce a Skilled Mean, not silently drop everything to a
        4-key mismatch."""
        # Skill side has NO horizon_value column.
        skill = _skill(
            [
                (3, CODE, "LR", 0.30, 0.95, 5.0, 0.90, 2.0, 10),
                (3, CODE, "TFT", 0.40, 0.88, 5.0, 0.85, 4.0, 10),
            ],
            cols=[
                "month_in_year",
                "code",
                "model_short",
                "sdivsigma",
                "nse",
                "delta",
                "accuracy",
                "mae",
                "n_pairs",
            ],
        )
        forecasts = _forecasts(
            [
                _fc_row("LR", 100.0, hv=1),
                _fc_row("TFT", 120.0, hv=1),
            ]
        )
        with patch.dict(os.environ, DEFAULT_ENV):
            for k in _LT_ENV_KEYS:
                os.environ.pop(k, None)
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        sm = result[result["model_short"] == "Skilled Mean"]
        assert len(sm) == 1, (
            "One-sided horizon_value must fall back to 3-key merge and still "
            "produce a Skilled Mean row (no mismatch)."
        )
        assert sm.iloc[0]["composition"] == "LR, TFT"


# ---------------------------------------------------------------------------
# 5. Edge cases: single positive model, empty pool, NaN NSE
# ---------------------------------------------------------------------------


class TestSkilledMeanEdgeCases:
    def test_single_positive_model_no_skilled_mean_row(self):
        """Only one model has NSE>0 -> single-model -> Skilled Mean discarded."""
        skill = _skill(
            [
                (3, 1, CODE, "LR", 0.30, 0.95, 5.0, 0.90, 2.0, 10),
                (3, 1, CODE, "GBT", 0.90, -0.50, 5.0, 0.10, 6.0, 10),  # NSE<0
            ]
        )
        forecasts = _forecasts(
            [
                _fc_row("LR", 100.0),
                _fc_row("GBT", 300.0),
            ]
        )
        with patch.dict(os.environ, DEFAULT_ENV):
            for k in _LT_ENV_KEYS:
                os.environ.pop(k, None)
            result = create_monthly_ensemble_forecasts(forecasts, skill)
        sm = result[result["model_short"] == "Skilled Mean"]
        assert len(sm) == 0, "Single positive model must not yield a Skilled Mean row"

    def test_empty_pool_no_row_no_crash(self):
        """No model has NSE>0 -> no Skilled Mean row, no exception."""
        skill = _skill(
            [
                (3, 1, CODE, "LR", 0.90, -0.10, 5.0, 0.10, 2.0, 10),
                (3, 1, CODE, "GBT", 0.90, -0.50, 5.0, 0.10, 6.0, 10),
            ]
        )
        forecasts = _forecasts(
            [
                _fc_row("LR", 100.0),
                _fc_row("GBT", 300.0),
            ]
        )
        with patch.dict(os.environ, DEFAULT_ENV):
            for k in _LT_ENV_KEYS:
                os.environ.pop(k, None)
            result = create_monthly_ensemble_forecasts(forecasts, skill)
        assert result[result["model_short"] == "Skilled Mean"].empty

    def test_nan_nse_never_passes(self):
        """A model with NaN NSE is never admitted by the NSE>0 gate."""
        helper = getattr(sm_mod, "_long_term_threshold_overrides", None)
        if helper is None:
            pytest.fail(
                "skill_metrics._long_term_threshold_overrides missing — cannot "
                "verify NaN NSE handling under the LT gate."
            )
        skill = _skill(
            [
                (3, 1, CODE, "GOOD", 5.0, 0.30, 5.0, 0.10, 3.0, 10),
                (3, 1, CODE, "NANMODEL", 5.0, np.nan, 5.0, 0.10, 3.0, 10),
            ]
        )
        with patch.dict(os.environ, DEFAULT_ENV):
            for k in _LT_ENV_KEYS:
                os.environ.pop(k, None)
            kept = filter_for_highly_skilled_forecasts(skill, **helper())
        assert set(kept["model_short"]) == {"GOOD"}, "NaN NSE must not pass NSE>0"
