"""Tests for the SAPPHIRE_SKILL_LEAD_AWARE flag helper (M1 P0 scaffold).

TDD: pins the default-OFF contract and the truthy/falsey token parsing
before any behavior is gated on the flag (P1+).
"""

import pytest
import skill_lead_aware_flag as flag


def test_default_is_off_when_env_unset(monkeypatch):
    monkeypatch.delenv(flag.SKILL_LEAD_AWARE_ENV, raising=False)
    assert flag.skill_lead_aware_enabled() is False


@pytest.mark.parametrize("value", ["1", "true", "True", " TRUE ", "yes", "YES", "on", "On"])
def test_truthy_tokens_enable(monkeypatch, value):
    monkeypatch.setenv(flag.SKILL_LEAD_AWARE_ENV, value)
    assert flag.skill_lead_aware_enabled() is True


@pytest.mark.parametrize("value", ["0", "false", "False", " false ", "no", "NO", "off", "OFF", ""])
def test_falsey_tokens_disable(monkeypatch, value):
    monkeypatch.setenv(flag.SKILL_LEAD_AWARE_ENV, value)
    assert flag.skill_lead_aware_enabled() is False


def test_invalid_value_raises_value_error(monkeypatch):
    monkeypatch.setenv(flag.SKILL_LEAD_AWARE_ENV, "banana")
    with pytest.raises(ValueError, match="SAPPHIRE_SKILL_LEAD_AWARE"):
        flag.skill_lead_aware_enabled()
