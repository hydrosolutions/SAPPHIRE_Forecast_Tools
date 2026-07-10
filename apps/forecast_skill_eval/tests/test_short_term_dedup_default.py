"""Locked regression test for M5 (design D4/#7).

The default run must dedup short-term (day/pentad/decade) re-issues to one
pair per target, matching the operational side.  This closes the
eval-vs-operational divergence described in
``doc/plans/postprocessing_skill_correctness_design.md`` (M5).  The opt-out
(explicitly passing ``False``) must still work.
"""

from __future__ import annotations

from forecast_skill_eval import cli
from forecast_skill_eval.config import ForecastSkillEvalConfig


def test_short_term_dedup_one_per_target_defaults_true() -> None:
    """The default config must dedup short-term re-issues to one pair per target."""
    config = ForecastSkillEvalConfig()

    assert config.short_term_dedup_one_per_target is True


def test_short_term_dedup_one_per_target_opt_out_still_works() -> None:
    """A caller must still be able to explicitly disable the dedup gate."""
    config = ForecastSkillEvalConfig(short_term_dedup_one_per_target=False)

    assert config.short_term_dedup_one_per_target is False


def test_cli_default_run_dedups_one_per_target(monkeypatch) -> None:
    """A CLI run with no flag must resolve dedup ON (the operational default)."""
    monkeypatch.delenv("SAPPHIRE_SKILL_FORECAST_ONLY", raising=False)
    parser = cli._parser()
    args = parser.parse_args([])
    config = cli._config_from_args(args)

    assert config.short_term_dedup_one_per_target is True


def test_cli_opt_out_flag_disables_dedup(monkeypatch) -> None:
    """The explicit opt-out flag must turn short-term dedup OFF."""
    monkeypatch.delenv("SAPPHIRE_SKILL_FORECAST_ONLY", raising=False)
    parser = cli._parser()
    args = parser.parse_args(["--no-short-term-dedup-one-per-target"])
    config = cli._config_from_args(args)

    assert config.short_term_dedup_one_per_target is False


def test_cli_legacy_on_flag_still_works(monkeypatch) -> None:
    """The original --short-term-dedup-one-per-target flag must still turn it ON."""
    monkeypatch.delenv("SAPPHIRE_SKILL_FORECAST_ONLY", raising=False)
    parser = cli._parser()
    args = parser.parse_args(["--short-term-dedup-one-per-target"])
    config = cli._config_from_args(args)

    assert config.short_term_dedup_one_per_target is True


def test_cli_forecast_only_env_forces_dedup_even_with_opt_out(monkeypatch) -> None:
    """SAPPHIRE_SKILL_FORECAST_ONLY must force dedup ON despite the opt-out flag."""
    monkeypatch.setenv("SAPPHIRE_SKILL_FORECAST_ONLY", "1")
    parser = cli._parser()
    args = parser.parse_args(["--no-short-term-dedup-one-per-target"])
    config = cli._config_from_args(args)

    assert config.short_term_dedup_one_per_target is True
