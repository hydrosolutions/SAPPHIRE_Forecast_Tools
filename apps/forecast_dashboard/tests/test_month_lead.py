"""Unit tests for src.month_lead — the shared UI-layer monthly-lead accessor.

Mirrors the fallback + warn semantics of ``src.db._get_data_monthly``'s
``_safe_lead`` closure (trunk M1 P3), exposed as standalone, importable
helpers for the surviving UI fixes (plot captions, bulletin hydration,
header text) that need the same answer but cannot import a nested closure.

No real station codes / discharge values used (none needed here).
"""

import json

import pytest

from src.month_lead import month_lead_for_mode, primary_month_lead


def _write_config(tmp_path, name, modes):
    """Write per-mode config JSONs and point the resolver env at them.

    modes: dict {config_mode_name: operational_month_lead_time}.
    """
    config_dir = tmp_path / name
    config_dir.mkdir()
    for mode_name, lead in modes.items():
        (config_dir / f"{mode_name}.json").write_text(
            json.dumps({"operational_month_lead_time": lead})
        )
    return config_dir


def _set_env(monkeypatch, tmp_path, name, modes):
    _write_config(tmp_path, name, modes)
    monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_configuration", name)
    monkeypatch.setenv(
        "ieasyhydroforecast_ml_long_term_supported_modes", ",".join(modes.keys())
    )


class TestMonthLeadForMode:
    def test_resolves_configured_lead(self, monkeypatch, tmp_path):
        """Returns the configured lead when the mode is supported and resolvable."""
        _set_env(monkeypatch, tmp_path, "kghm", {"month_0": 0, "month_1": 1})

        assert month_lead_for_mode("month_0", default=99) == 0
        assert month_lead_for_mode("month_1", default=99) == 1

    def test_resolves_nonzero_configured_month0_lead(self, monkeypatch, tmp_path):
        """month_0 is not guaranteed to be lead 0 — a deployment may
        configure it with a non-zero operational lead, and
        month_lead_for_mode must return THAT resolved value verbatim.

        Discriminating mutation: an implementation that special-cases
        month_0 (e.g. ``if mode == "month_0": return 0`` before consulting
        the config) would pass every other case in this class — they all
        configure month_0's lead as 0 — but this test configures month_0
        with lead 2 and goes RED (returns 0 instead of 2) against that
        mutation.
        """
        _set_env(monkeypatch, tmp_path, "custom_hm", {"month_0": 2, "month_1": 3})

        assert month_lead_for_mode("month_0", default=99) == 2

    def test_falls_back_to_default_when_mode_unsupported(self, monkeypatch, tmp_path):
        """An unsupported mode (UnsupportedLongTermModeError, a
        LongTermHorizonResolverError subclass) degrades to the default rather
        than raising."""
        _set_env(monkeypatch, tmp_path, "tjhm", {"month_1": 0})

        assert month_lead_for_mode("month_0", default=7) == 7

    def test_falls_back_to_default_when_config_missing(self, monkeypatch):
        """No config env wired at all (FileNotFoundError / required-env error
        path) degrades to the default rather than raising."""
        monkeypatch.delenv("ieasyforecast_configuration_path", raising=False)
        monkeypatch.delenv("ieasyhydroforecast_ml_long_term_configuration", raising=False)
        monkeypatch.delenv("ieasyhydroforecast_ml_long_term_supported_modes", raising=False)

        assert month_lead_for_mode("month_1", default=3) == 3


class TestPrimaryMonthLead:
    def test_returns_resolved_lead_when_month_1_supported(self, monkeypatch, tmp_path):
        """kghm-shaped: month_1 is supported and resolves to lead 1."""
        _set_env(monkeypatch, tmp_path, "kghm", {"month_0": 0, "month_1": 1})

        assert primary_month_lead() == 1

    def test_returns_resolved_lead_for_tjhm_lead0(self, monkeypatch, tmp_path):
        """tjhm-shaped: month_1 is the flagship and resolves to lead 0."""
        _set_env(monkeypatch, tmp_path, "tjhm", {"month_1": 0, "month_2": 1})

        assert primary_month_lead() == 0

    def test_falls_back_to_one_when_month_1_not_supported(self, monkeypatch, tmp_path):
        """month_1 absent from supported modes: fall back to legacy lead 1
        without ever calling the resolver for an unsupported mode."""
        _set_env(monkeypatch, tmp_path, "tjhm", {"month_2": 1})

        assert primary_month_lead() == 1

    def test_falls_back_to_one_when_config_unresolvable(self, monkeypatch):
        """month_1 nominally supported but its config is missing entirely:
        graceful degrade to 1, not a raised exception."""
        monkeypatch.setenv(
            "ieasyhydroforecast_ml_long_term_supported_modes", "month_1"
        )
        monkeypatch.delenv("ieasyforecast_configuration_path", raising=False)
        monkeypatch.delenv("ieasyhydroforecast_ml_long_term_configuration", raising=False)

        assert primary_month_lead() == 1
