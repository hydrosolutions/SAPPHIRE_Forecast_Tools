"""Locked tests for milestone M2 of the postprocessing skill-correctness campaign.

M2 scope (see `src/skill_metrics.py`):

1. The skill gate in `filter_for_highly_skilled_forecasts` becomes INCLUSIVE at
   the boundary (`>=` / `<=`) instead of exclusive (`>` / `<`).
2. The long-term NSE default moves from exactly ``0.0`` to a small positive
   epsilon so that, under the new inclusive ``>=`` gate, ``nse == 0.0`` still
   fails while any positive NSE still passes.
3. The long-term NSE gate can no longer be disabled via env var — attempting
   to disable it raises ``ValueError``. (sdivsigma/accuracy remain
   disable-able for long-term; untouched by this milestone.)
4. `_parse_threshold_env` now rejects non-finite floats (``nan``/``inf``/
   ``-inf``) with a clear ``ValueError`` instead of silently returning them.
5. The short-term direct env-var read in `filter_for_highly_skilled_forecasts`
   now routes through the same lenient `_parse_threshold_env` parser used by
   the long-term path, so a lowercase ``"false"`` or empty-string env value
   cleanly disables the gate instead of crashing on a bare ``float()`` call.

Placeholder station code 19999 only (never a real station code).
"""

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.skill_metrics import (
    _long_term_threshold_overrides,
    _parse_threshold_env,
    filter_for_highly_skilled_forecasts,
)

CODE = "19999"

# Short-term default env vars, matching METRIC_REGISTRY defaults exactly
# (nse=0.8, sdivsigma=0.6, accuracy=0.8).
_SHORT_TERM_ENV_KEYS = (
    "ieasyhydroforecast_efficiency_threshold",
    "ieasyhydroforecast_nse_threshold",
    "ieasyhydroforecast_accuracy_threshold",
)
SHORT_TERM_DEFAULT_ENV = {
    "ieasyhydroforecast_efficiency_threshold": "0.6",
    "ieasyhydroforecast_nse_threshold": "0.8",
    "ieasyhydroforecast_accuracy_threshold": "0.8",
}

# Long-term-only env vars; unsetting all three restores documented defaults
# (nse=epsilon, sdivsigma=disabled, accuracy=disabled).
_LONG_TERM_ENV_KEYS = (
    "ieasyhydroforecast_nse_threshold_long_term",
    "ieasyhydroforecast_efficiency_threshold_long_term",
    "ieasyhydroforecast_accuracy_threshold_long_term",
)


def _row(model_short, *, sdivsigma=0.3, nse=0.95, accuracy=0.95, n_pairs=10):
    """Minimal skill-stats row dict for the columns the gate reads."""
    return {
        "code": CODE,
        "model_short": model_short,
        "sdivsigma": sdivsigma,
        "nse": nse,
        "accuracy": accuracy,
        "n_pairs": n_pairs,
    }


# ---------------------------------------------------------------------------
# (a) Short-term: exact-boundary values on all three metrics are KEPT.
# ---------------------------------------------------------------------------


def test_short_term_exact_boundary_all_three_metrics_kept(monkeypatch):
    """nse=0.8, accuracy=0.80, sdivsigma=0.6 (all exactly at their short-term
    default thresholds) must be KEPT under the new inclusive gate."""
    for key, value in SHORT_TERM_DEFAULT_ENV.items():
        monkeypatch.setenv(key, value)

    df = pd.DataFrame([_row("LR", sdivsigma=0.6, nse=0.8, accuracy=0.80)])
    result = filter_for_highly_skilled_forecasts(df)

    assert len(result) == 1, "Model exactly at all three default thresholds must be kept"
    assert result.iloc[0]["model_short"] == "LR"


# ---------------------------------------------------------------------------
# (b) Long-term NSE epsilon: nse=0.0 excluded, nse=0.001 kept.
# ---------------------------------------------------------------------------


def test_long_term_nse_zero_excluded_small_positive_kept(monkeypatch):
    """Under long-term default overrides, nse=0.0 must still fail (epsilon
    default, not exactly 0.0) while nse=0.001 passes."""
    for key in _LONG_TERM_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    overrides = _long_term_threshold_overrides()

    df = pd.DataFrame(
        [
            _row("ZERO", nse=0.0, sdivsigma=5.0, accuracy=0.1),
            _row("SMALL_POSITIVE", nse=0.001, sdivsigma=5.0, accuracy=0.1),
        ]
    )
    result = filter_for_highly_skilled_forecasts(df, **overrides)
    kept = set(result["model_short"])

    assert "ZERO" not in kept, "nse=0.0 must still fail the long-term NSE gate"
    assert "SMALL_POSITIVE" in kept, "nse=0.001 must pass the long-term NSE gate"


# ---------------------------------------------------------------------------
# (c) Long-term NSE gate cannot be disabled.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("token", ["false", "False", "", "off"])
def test_long_term_nse_gate_cannot_be_disabled(monkeypatch, token):
    """Any disable token on the long-term NSE env var must raise ValueError —
    the long-term NSE gate may never be turned off, unlike sdivsigma/accuracy."""
    monkeypatch.setenv("ieasyhydroforecast_nse_threshold_long_term", token)
    with pytest.raises(ValueError):
        _long_term_threshold_overrides()


# ---------------------------------------------------------------------------
# (d) _parse_threshold_env rejects non-finite values.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("raw", ["nan", "inf", "-inf"])
def test_parse_threshold_env_rejects_non_finite(raw):
    with pytest.raises(ValueError):
        _parse_threshold_env(raw)


# ---------------------------------------------------------------------------
# (e) Short-term disable tokens ('false', '') cleanly disable the gate.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("disable_token", ["false", ""])
def test_short_term_sdivsigma_disable_token_disables_gate(monkeypatch, disable_token):
    """A lowercase 'false' or empty-string sdivsigma env value must cleanly
    disable that gate (no crash), while nse/accuracy stay strict at defaults."""
    # sdivsigma=0.9 fails the default (<=0.6) gate; nse/accuracy pass defaults.
    df = pd.DataFrame([_row("LR", sdivsigma=0.9, nse=0.95, accuracy=0.95)])

    # Baseline: sdivsigma env unset (module default 0.6) -> excluded.
    monkeypatch.delenv("ieasyhydroforecast_efficiency_threshold", raising=False)
    result_default = filter_for_highly_skilled_forecasts(df)
    assert len(result_default) == 0, "sdivsigma=0.9 must fail the default (0.6) gate"

    # Disabled: sdivsigma env set to a lenient disable token -> kept, no crash.
    monkeypatch.setenv("ieasyhydroforecast_efficiency_threshold", disable_token)
    result_disabled = filter_for_highly_skilled_forecasts(df)
    assert len(result_disabled) == 1, (
        f"sdivsigma disable token {disable_token!r} must disable the gate cleanly"
    )
    assert result_disabled.iloc[0]["model_short"] == "LR"


# ---------------------------------------------------------------------------
# (f) The overrides path is routed through the SAME parser as the env path,
#     so disable-token handling and the isfinite guard apply identically
#     regardless of where the threshold value came from.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad_override", [float("nan"), "nan", float("inf"), "inf"])
def test_override_non_finite_raises(monkeypatch, bad_override):
    """A non-finite threshold passed as an OVERRIDE (not via env) must raise
    ValueError, mirroring the env-path isfinite guard."""
    for key in _SHORT_TERM_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    df = pd.DataFrame([_row("LR")])
    with pytest.raises(ValueError):
        filter_for_highly_skilled_forecasts(df, sdivsigma=bad_override)


@pytest.mark.parametrize("disable_token", ["off", "none", "disable", ""])
def test_override_lenient_disable_token_disables_gate(monkeypatch, disable_token):
    """A lenient disable token passed as an OVERRIDE disables that gate,
    exactly like the env path (parity)."""
    for key in _SHORT_TERM_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    # sdivsigma=0.9 fails the default (<=0.6) gate; disabling it via an override
    # token keeps the model (nse/accuracy stay at inclusive defaults it meets).
    df = pd.DataFrame([_row("LR", sdivsigma=0.9, nse=0.95, accuracy=0.95)])
    result = filter_for_highly_skilled_forecasts(df, sdivsigma=disable_token)
    assert len(result) == 1, "override disable token must disable the sdivsigma gate"
    assert result.iloc[0]["model_short"] == "LR"


def test_bool_false_override_still_disables_gates(monkeypatch):
    """REGRESSION: the real long-term caller passes bool ``False`` for
    sdivsigma/accuracy overrides — these must still disable those gates."""
    for key in _SHORT_TERM_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    # Both metrics would fail a numeric gate; bool False must disable both so
    # the model survives on its NSE alone.
    df = pd.DataFrame([_row("LR", sdivsigma=0.9, nse=0.95, accuracy=0.05)])
    result = filter_for_highly_skilled_forecasts(df, sdivsigma=False, accuracy=False, nse=0.001)
    assert len(result) == 1, "bool False overrides must disable sdivsigma/accuracy"
    assert result.iloc[0]["model_short"] == "LR"
