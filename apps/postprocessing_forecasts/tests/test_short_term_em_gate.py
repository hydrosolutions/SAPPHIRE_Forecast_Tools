"""Short-term EM gate (own env vars) and LR delta band (PP-072 P1). Synthetic data only."""

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

import tag_library as tl
from src.ensemble_calculator import create_ensemble_forecasts
from src.skill_metrics import (
    _long_term_min_pairs,
    _long_term_threshold_overrides,
    _short_term_min_pairs,
    _short_term_threshold_overrides,
)

CODE = "19999"
_ST_ENV = [
    "ieasyhydroforecast_nse_threshold_short_term",
    "ieasyhydroforecast_efficiency_threshold_short_term",
    "ieasyhydroforecast_accuracy_threshold_short_term",
    "ieasyhydroforecast_min_pairs_short_term",
]
_FALLBACK_ENV = [
    "ieasyhydroforecast_nse_threshold",
    "ieasyhydroforecast_efficiency_threshold",
    "ieasyhydroforecast_accuracy_threshold",
]
_LT_ENV = [
    "ieasyhydroforecast_nse_threshold_long_term",
    "ieasyhydroforecast_efficiency_threshold_long_term",
    "ieasyhydroforecast_accuracy_threshold_long_term",
    "ieasyhydroforecast_min_pairs_long_term",
]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in _ST_ENV + _LT_ENV + _FALLBACK_ENV:
        monkeypatch.delenv(var, raising=False)


def _em(forecasts, skill):
    return create_ensemble_forecasts(
        forecasts=forecasts,
        skill_stats=skill,
        period_col="pentad_in_year",
        period_in_month_col="pentad_in_month",
        get_period_in_month_func=tl.get_pentad,
    )


def _skill(models, nse=None, n_pairs=None, delta=None, sdivsigma=None, accuracy=None):
    n = len(models)
    return pd.DataFrame(
        {
            "pentad_in_year": [1] * n,
            "code": [CODE] * n,
            "model_short": models,
            "sdivsigma": sdivsigma or [999.0] * n,
            "nse": nse or [0.9] * n,
            "delta": delta or [2.0] * n,
            "accuracy": accuracy or [-99.0] * n,
            "mae": [5.0] * n,
            "n_pairs": n_pairs or [10] * n,
        }
    )


def _fc(models, points, **q):
    n = len(models)
    base = {
        "pentad_in_year": [1] * n,
        "pentad_in_month": [1] * n,
        "date": [pd.Timestamp("2025-01-05")] * n,
        "code": [CODE] * n,
        "model_short": models,
        "forecasted_discharge": points,
    }
    nan = [np.nan] * n
    for k in ("q05", "q25", "q75", "q95"):
        base[k] = q.get(k, nan)
    return pd.DataFrame(base)


def _lr_ml_forecasts():
    return _fc(
        ["LR", "TFT"],
        [10.0, 20.0],
        q05=[np.nan, 10.0],
        q25=[np.nan, 15.0],
        q75=[np.nan, 25.0],
        q95=[np.nan, 30.0],
    )


class TestLrDeltaBand:
    def test_hand_computed_example(self):
        """LR 10 delta 2 + TFT with four bands: bands per the plan's rule."""
        joint, _ = _em(_lr_ml_forecasts(), _skill(["LR", "TFT"], delta=[2.0, 2.0]))
        em = joint[joint["model_short"] == "EM"]
        assert len(em) == 1
        em0 = em.iloc[0]
        assert em0["forecasted_discharge"] == pytest.approx(15.0)
        assert em0["q05"] == pytest.approx(10.0)  # TFT only
        assert em0["q25"] == pytest.approx((8.0 + 15.0) / 2)  # LR 10-2, TFT 15
        assert em0["q75"] == pytest.approx((12.0 + 25.0) / 2)  # LR 10+2, TFT 25
        assert em0["q95"] == pytest.approx(30.0)  # TFT only
        assert em0["composition"] == "LR, TFT"

    def test_delta_taken_from_lr_row_not_other_models(self):
        """PP-072: a non-LR row with a very different delta, listed BEFORE LR, is ignored."""
        skill = _skill(["TFT", "LR"], delta=[50.0, 2.0])
        joint, _ = _em(_lr_ml_forecasts(), skill)
        em0 = joint[joint["model_short"] == "EM"].iloc[0]
        assert em0["q25"] == pytest.approx((8.0 + 15.0) / 2)  # LR 10-2, not 10-50
        assert em0["q75"] == pytest.approx((12.0 + 25.0) / 2)

    def test_latest_dated_lr_delta_wins(self):
        """Several LR rows for one (period, code): the latest `date` delta is used."""
        skill = _skill(["LR", "LR", "TFT"], delta=[9.0, 2.0, 2.0])
        skill["date"] = pd.to_datetime(["2024-01-05", "2025-01-05", "2025-01-05"])
        joint, _ = _em(_lr_ml_forecasts(), skill)
        assert joint[joint["model_short"] == "EM"].iloc[0]["q25"] == pytest.approx((8.0 + 15.0) / 2)
        # Row order must not matter: put the latest row first.
        skill2 = skill.iloc[[1, 0, 2]].reset_index(drop=True)
        joint2, _ = _em(_lr_ml_forecasts(), skill2)
        assert joint2[joint2["model_short"] == "EM"].iloc[0]["q25"] == pytest.approx(
            (8.0 + 15.0) / 2
        )

    def test_latest_lr_row_without_usable_delta_falls_back_to_earlier(self):
        """A non-finite delta on the latest LR row is skipped; the latest usable one is used."""
        skill = _skill(["LR", "LR", "TFT"], delta=[2.0, np.nan, 2.0])
        skill["date"] = pd.to_datetime(["2024-01-05", "2025-01-05", "2025-01-05"])
        joint, _ = _em(_lr_ml_forecasts(), skill)
        assert joint[joint["model_short"] == "EM"].iloc[0]["q25"] == pytest.approx((8.0 + 15.0) / 2)

    def test_lr_nan_delta_not_rescued_by_other_model(self):
        """LR with NaN delta is dropped even though TFT has a finite delta."""
        skill = _skill(["LR", "TFT"], delta=[np.nan, 2.0])
        joint, _ = _em(_lr_ml_forecasts(), skill)
        assert "EM" not in joint["model_short"].values

    def test_skill_without_date_column_uses_first_lr_delta(self):
        """Without a `date` column the first finite non-negative LR delta is used."""
        skill = _skill(["TFT", "LR", "LR"], delta=[50.0, 2.0, 9.0])
        assert "date" not in skill.columns
        joint, _ = _em(_lr_ml_forecasts(), skill)
        em0 = joint[joint["model_short"] == "EM"].iloc[0]
        assert em0["q25"] == pytest.approx((8.0 + 15.0) / 2)

    @pytest.mark.parametrize("bad", [np.nan, np.inf, -1.0])
    def test_lr_with_unusable_delta_dropped_and_no_em(self, bad):
        """LR with NaN/inf/negative delta is dropped; TFT alone -> no EM."""
        joint, _ = _em(_lr_ml_forecasts(), _skill(["LR", "TFT"], delta=[bad, bad]))
        assert "EM" not in joint["model_short"].values

    def test_lr_with_unusable_delta_dropped_ml_pair_still_forms_em(self):
        """With a third model, EM forms without LR in point or composition."""
        fc = _fc(
            ["LR", "TFT", "TiDE"],
            [100.0, 20.0, 30.0],
            q05=[np.nan, 10.0, 15.0],
            q25=[np.nan, 15.0, 20.0],
            q75=[np.nan, 25.0, 35.0],
            q95=[np.nan, 30.0, 40.0],
        )
        skill = _skill(["LR", "TFT", "TiDE"], delta=[np.nan] * 3)
        joint, _ = _em(fc, skill)
        em0 = joint[joint["model_short"] == "EM"].iloc[0]
        assert em0["composition"] == "TFT, TiDE"
        assert em0["forecasted_discharge"] == pytest.approx(25.0)

    def test_lr_dropped_without_delta_is_logged(self, caplog):
        """An LR row with no usable delta is logged once; valid delta logs nothing."""
        import logging

        with caplog.at_level(logging.INFO, logger="src.ensemble_calculator"):
            _em(_lr_ml_forecasts(), _skill(["LR", "TFT"], delta=[np.nan, np.nan]))
        msgs = [
            r.getMessage() for r in caplog.records if "without a usable delta" in r.getMessage()
        ]
        assert len(msgs) == 1
        assert "EM: LR dropped for 1 (code, pentad_in_year) group(s)" in msgs[0]
        assert CODE in msgs[0]

        caplog.clear()
        with caplog.at_level(logging.INFO, logger="src.ensemble_calculator"):
            _em(_lr_ml_forecasts(), _skill(["LR", "TFT"], delta=[2.0, 2.0]))
        assert not [r for r in caplog.records if "without a usable delta" in r.getMessage()]

    def test_missing_delta_column_drops_lr(self):
        skill = _skill(["LR", "TFT"]).drop(columns="delta")
        joint, _ = _em(_lr_ml_forecasts(), skill)
        assert "EM" not in joint["model_short"].values

    def test_lr_only_group_gives_no_em(self):
        fc = _fc(["LR"], [10.0])
        joint, _ = _em(fc, _skill(["LR"]))
        assert "EM" not in joint["model_short"].values


class TestNPairsGate:
    def test_model_below_default_k_excluded(self):
        joint, _ = _em(_lr_ml_forecasts(), _skill(["LR", "TFT"], n_pairs=[10, 3]))
        assert "EM" not in joint["model_short"].values  # TFT out -> LR alone

    def test_model_at_k_included(self):
        joint, _ = _em(_lr_ml_forecasts(), _skill(["LR", "TFT"], n_pairs=[10, 4]))
        assert "EM" in joint["model_short"].values

    def test_k_configurable_via_short_term_env(self, monkeypatch):
        monkeypatch.setenv("ieasyhydroforecast_min_pairs_short_term", "2")
        joint, _ = _em(_lr_ml_forecasts(), _skill(["LR", "TFT"], n_pairs=[10, 3]))
        assert "EM" in joint["model_short"].values
        monkeypatch.setenv("ieasyhydroforecast_min_pairs_short_term", "11")
        joint, _ = _em(_lr_ml_forecasts(), _skill(["LR", "TFT"], n_pairs=[10, 10]))
        assert "EM" not in joint["model_short"].values


class TestNseGate:
    def test_nse_zero_excluded_by_default(self):
        joint, _ = _em(_lr_ml_forecasts(), _skill(["LR", "TFT"], nse=[0.9, 0.0]))
        assert "EM" not in joint["model_short"].values

    def test_nse_configurable_via_short_term_env(self, monkeypatch):
        skill = _skill(["LR", "TFT"], nse=[0.9, 0.5])
        joint, _ = _em(_lr_ml_forecasts(), skill)
        assert "EM" in joint["model_short"].values
        monkeypatch.setenv("ieasyhydroforecast_nse_threshold_short_term", "0.8")
        joint, _ = _em(_lr_ml_forecasts(), skill)
        assert "EM" not in joint["model_short"].values

    def test_sdivsigma_and_accuracy_off_by_default_and_enableable(self, monkeypatch):
        skill = _skill(["LR", "TFT"], sdivsigma=[999.0, 999.0], accuracy=[-99.0, -99.0])
        joint, _ = _em(_lr_ml_forecasts(), skill)
        assert "EM" in joint["model_short"].values
        monkeypatch.setenv("ieasyhydroforecast_efficiency_threshold_short_term", "0.8")
        joint, _ = _em(_lr_ml_forecasts(), skill)
        assert "EM" not in joint["model_short"].values
        monkeypatch.delenv("ieasyhydroforecast_efficiency_threshold_short_term")
        monkeypatch.setenv("ieasyhydroforecast_accuracy_threshold_short_term", "0.8")
        joint, _ = _em(_lr_ml_forecasts(), skill)
        assert "EM" not in joint["model_short"].values


class TestEnvParsing:
    def test_defaults(self):
        ov = _short_term_threshold_overrides()
        assert ov["nse"] == pytest.approx(1e-9)
        assert ov["sdivsigma"] is False
        assert ov["accuracy"] is False
        assert _short_term_min_pairs() == 4

    @pytest.mark.parametrize("token", ["false", "off", "none"])
    def test_nse_cannot_be_disabled(self, monkeypatch, token):
        monkeypatch.setenv("ieasyhydroforecast_nse_threshold_short_term", token)
        with pytest.raises(ValueError, match="ieasyhydroforecast_nse_threshold_short_term"):
            _short_term_threshold_overrides()

    @pytest.mark.parametrize("var", _ST_ENV[:3])
    def test_bad_threshold_names_variable(self, monkeypatch, var):
        monkeypatch.setenv(var, "banana")
        with pytest.raises(ValueError, match=var):
            _short_term_threshold_overrides()

    @pytest.mark.parametrize("raw", ["banana", "0", "-3", "2.5"])
    def test_bad_min_pairs_names_variable(self, monkeypatch, raw):
        monkeypatch.setenv("ieasyhydroforecast_min_pairs_short_term", raw)
        with pytest.raises(ValueError, match="ieasyhydroforecast_min_pairs_short_term"):
            _short_term_min_pairs()


class TestUnsuffixedFallback:
    """Owner decision: `*_short_term` > un-suffixed deployed var > default."""

    def test_neither_set_gives_defaults(self):
        ov = _short_term_threshold_overrides()
        assert ov == {"nse": pytest.approx(1e-9), "sdivsigma": False, "accuracy": False}

    def test_only_unsuffixed_set_is_used(self, monkeypatch):
        monkeypatch.setenv("ieasyhydroforecast_nse_threshold", "0.0")
        monkeypatch.setenv("ieasyhydroforecast_efficiency_threshold", "0.4")
        monkeypatch.setenv("ieasyhydroforecast_accuracy_threshold", "0.8")
        ov = _short_term_threshold_overrides()
        assert ov == {"nse": 0.0, "sdivsigma": 0.4, "accuracy": 0.8}

    def test_unsuffixed_values_gate_em_through_create_ensemble_forecasts(self, monkeypatch):
        monkeypatch.setenv("ieasyhydroforecast_nse_threshold", "0.0")
        monkeypatch.setenv("ieasyhydroforecast_efficiency_threshold", "0.4")
        monkeypatch.setenv("ieasyhydroforecast_accuracy_threshold", "0.8")
        ok = _skill(["LR", "TFT"], sdivsigma=[0.3, 0.4], accuracy=[0.9, 0.8], nse=[0.9, 0.0])
        joint, _ = _em(_lr_ml_forecasts(), ok)
        assert "EM" in joint["model_short"].values  # all inclusive boundaries met
        bad_sd = _skill(["LR", "TFT"], sdivsigma=[0.3, 0.5], accuracy=[0.9, 0.9])
        joint, _ = _em(_lr_ml_forecasts(), bad_sd)
        assert "EM" not in joint["model_short"].values
        bad_acc = _skill(["LR", "TFT"], sdivsigma=[0.3, 0.3], accuracy=[0.9, 0.7])
        joint, _ = _em(_lr_ml_forecasts(), bad_acc)
        assert "EM" not in joint["model_short"].values

    def test_short_term_wins_per_variable_independently(self, monkeypatch):
        monkeypatch.setenv("ieasyhydroforecast_nse_threshold", "0.8")
        monkeypatch.setenv("ieasyhydroforecast_nse_threshold_short_term", "0.2")
        monkeypatch.setenv("ieasyhydroforecast_efficiency_threshold", "0.4")
        monkeypatch.setenv("ieasyhydroforecast_accuracy_threshold", "0.7")
        monkeypatch.setenv("ieasyhydroforecast_accuracy_threshold_short_term", "off")
        ov = _short_term_threshold_overrides()
        assert ov["nse"] == pytest.approx(0.2)  # from new var
        assert ov["sdivsigma"] == pytest.approx(0.4)  # from old var
        assert ov["accuracy"] is False  # new var set to a disable token wins

    def test_explicit_new_nse_disable_token_raises_naming_new_var(self, monkeypatch):
        monkeypatch.setenv("ieasyhydroforecast_nse_threshold", "0.5")
        monkeypatch.setenv("ieasyhydroforecast_nse_threshold_short_term", "False")
        with pytest.raises(ValueError, match="ieasyhydroforecast_nse_threshold_short_term"):
            _short_term_threshold_overrides()

    @pytest.mark.parametrize("token", ["False", "off", "none"])
    def test_unsuffixed_nse_disable_token_uses_default_and_warns(self, monkeypatch, caplog, token):
        monkeypatch.setenv("ieasyhydroforecast_nse_threshold", token)
        with caplog.at_level("WARNING"):
            ov = _short_term_threshold_overrides()
        assert ov["nse"] == pytest.approx(1e-9)
        assert any(
            "ieasyhydroforecast_nse_threshold" in r.getMessage() and r.levelname == "WARNING"
            for r in caplog.records
        )

    @pytest.mark.parametrize("var", _FALLBACK_ENV)
    def test_invalid_unsuffixed_value_names_unsuffixed_var(self, monkeypatch, var):
        monkeypatch.setenv(var, "banana")
        with pytest.raises(ValueError) as ei:
            _short_term_threshold_overrides()
        assert f"'{var}'" in str(ei.value)
        assert "_short_term" not in str(ei.value)

    def test_empty_new_var_treated_as_unset(self, monkeypatch):
        monkeypatch.setenv("ieasyhydroforecast_nse_threshold_short_term", "")
        monkeypatch.setenv("ieasyhydroforecast_efficiency_threshold_short_term", "")
        monkeypatch.setenv("ieasyhydroforecast_nse_threshold", "0.3")
        monkeypatch.setenv("ieasyhydroforecast_efficiency_threshold", "0.4")
        ov = _short_term_threshold_overrides()
        assert ov["nse"] == pytest.approx(0.3)
        assert ov["sdivsigma"] == pytest.approx(0.4)
        # empty new var with no fallback -> default, not a disable-token raise
        monkeypatch.delenv("ieasyhydroforecast_nse_threshold")
        assert _short_term_threshold_overrides()["nse"] == pytest.approx(1e-9)


class TestIndependenceFromLongTerm:
    def test_long_term_env_has_no_effect_on_short_term_em(self, monkeypatch):
        monkeypatch.setenv("ieasyhydroforecast_nse_threshold_long_term", "0.99")
        monkeypatch.setenv("ieasyhydroforecast_min_pairs_long_term", "99")
        monkeypatch.setenv("ieasyhydroforecast_efficiency_threshold_long_term", "0.01")
        joint, _ = _em(_lr_ml_forecasts(), _skill(["LR", "TFT"], nse=[0.5, 0.5], n_pairs=[5, 5]))
        assert "EM" in joint["model_short"].values

    def test_short_term_env_has_no_effect_on_long_term_gate(self, monkeypatch):
        before = (_long_term_threshold_overrides(), _long_term_min_pairs("MONTH"))
        monkeypatch.setenv("ieasyhydroforecast_nse_threshold_short_term", "0.99")
        monkeypatch.setenv("ieasyhydroforecast_min_pairs_short_term", "99")
        monkeypatch.setenv("ieasyhydroforecast_accuracy_threshold_short_term", "0.9")
        after = (_long_term_threshold_overrides(), _long_term_min_pairs("MONTH"))
        assert before == after
