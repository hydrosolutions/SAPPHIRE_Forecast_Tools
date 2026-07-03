from __future__ import annotations

from pathlib import Path

import pytest

from forecast_skill_eval.config import (
    DEFAULT_BASINS_BY_PREFIX,
    DEFAULT_ERROR_FLAGS,
    DEFAULT_HINDCAST_FLAGS,
    DEFAULT_NAN_EXCLUDE_FLAGS,
    DEFAULT_OPERATIONAL_FLAGS,
    DEFAULT_OPERATIONAL_ISSUE_DAYS,
    DEFAULT_PROVENANCE,
    ForecastSkillEvalConfig,
)


def test_config_defaults_are_captured_once() -> None:
    config = ForecastSkillEvalConfig()

    assert config.base_url == "http://localhost:8000"
    assert config.threshold == 0.80
    assert config.min_years == 10
    assert config.operational_start == "2024-01-01"
    assert config.operational_flags == DEFAULT_OPERATIONAL_FLAGS
    assert config.hindcast_flags == DEFAULT_HINDCAST_FLAGS
    assert config.nan_exclude_flags == DEFAULT_NAN_EXCLUDE_FLAGS
    assert config.error_flags == DEFAULT_ERROR_FLAGS
    assert config.horizons == ("day", "pentad", "decade", "month", "quarter", "season")
    assert config.provenance_by_horizon == DEFAULT_PROVENANCE
    assert config.provenance_by_horizon is not DEFAULT_PROVENANCE
    assert config.basin_by_prefix == DEFAULT_BASINS_BY_PREFIX
    assert config.basin_by_prefix is not DEFAULT_BASINS_BY_PREFIX


def test_config_overrides_are_normalized_and_copied(tmp_path: Path) -> None:
    provenance = {"decad": "custom-official", "month": "custom-month"}
    basins = {"15": "custom-chu", "99": "custom-other"}
    config = ForecastSkillEvalConfig(
        base_url="https://example.test",
        threshold=0.65,
        horizons=["decad", "month"],
        model_filter=["model-a"],
        station_filter=["19999"],
        start_date="2020-01-01",
        end_date="2024-12-31",
        output_dir=tmp_path,
        provenance_by_horizon=provenance,
        basin_by_prefix=basins,
        min_years=12,
        operational_start="2024-02-01",
        operational_flags=[10],
        hindcast_flags=[11, 14],
        nan_exclude_flags=[13],
        error_flags=[12],
    )
    provenance["month"] = "mutated"
    basins["15"] = "mutated"

    assert config.base_url == "https://example.test"
    assert config.threshold == 0.65
    assert config.horizons == ("decade", "month")
    assert config.model_filter == ("model-a",)
    assert config.station_filter == ("19999",)
    assert config.start_date == "2020-01-01"
    assert config.end_date == "2024-12-31"
    assert config.output_dir == tmp_path
    assert config.provenance_by_horizon["decade"] == "custom-official"
    assert config.provenance_by_horizon["month"] == "custom-month"
    assert config.basin_by_prefix["15"] == "custom-chu"
    assert config.basin_by_prefix["99"] == "custom-other"
    assert config.min_years == 12
    assert config.operational_start == "2024-02-01"
    assert config.operational_flags == (10,)
    assert config.hindcast_flags == (11, 14)
    assert config.nan_exclude_flags == (13,)
    assert config.error_flags == (12,)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"threshold": 0},
        {"threshold": 1.01},
        {"min_years": 0},
        {"horizons": ["hour"]},
        {"start_date": "2024-02-01", "end_date": "2024-01-01"},
        {"operational_start": "not-a-date"},
        {"hindcast_flags": []},
        {"nan_exclude_flags": ["not-an-int"]},
        {"operational_flags": [0], "hindcast_flags": [0, 4]},
        {"provenance_by_horizon": {"hour": "calculated"}},
        {"basin_by_prefix": {"": "empty-prefix"}},
        {"basin_by_prefix": {"15": ""}},
    ],
)
def test_invalid_config_inputs_are_rejected(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        ForecastSkillEvalConfig(**kwargs)


def test_short_term_gate_flags_default_off() -> None:
    """Both short-term correctness gates must default to False (byte-identical)."""
    config = ForecastSkillEvalConfig()

    assert config.short_term_issue_before_target is False
    assert config.short_term_dedup_one_per_target is False


def test_short_term_gate_flags_can_be_enabled() -> None:
    config = ForecastSkillEvalConfig(
        short_term_issue_before_target=True,
        short_term_dedup_one_per_target=True,
    )

    assert config.short_term_issue_before_target is True
    assert config.short_term_dedup_one_per_target is True


def test_lr_repair_flag_defaults_off() -> None:
    """The LR repair-on-read flag must default to False (byte-identical reads)."""
    config = ForecastSkillEvalConfig()

    assert config.short_term_lr_repair_issue_indexing is False


def test_lr_repair_flag_can_be_enabled() -> None:
    config = ForecastSkillEvalConfig(short_term_lr_repair_issue_indexing=True)

    assert config.short_term_lr_repair_issue_indexing is True


def test_config_events_filter_accepts_below_norm_100() -> None:
    """below_norm_100 (opt-in norm-factor event) must validate in events_filter."""
    config = ForecastSkillEvalConfig(events_filter=("below_norm", "below_norm_100"))
    assert set(config.events_filter) == {"below_norm", "below_norm_100"}


def test_default_events_unchanged_by_norm_factor_event() -> None:
    """DEFAULT_EVENTS must still be exactly the original five events."""
    from forecast_skill_eval.config import DEFAULT_EVENTS

    assert tuple(DEFAULT_EVENTS) == (
        "below_norm",
        "low_p10",
        "low_p5",
        "high_p90",
        "high_p95",
    )
    assert "below_norm_100" not in DEFAULT_EVENTS


def test_operational_issue_days_default_is_empty() -> None:
    config = ForecastSkillEvalConfig()
    assert config.operational_issue_days == ()
    assert DEFAULT_OPERATIONAL_ISSUE_DAYS == ()


def test_operational_issue_days_normalized_to_sorted_unique_ints() -> None:
    config = ForecastSkillEvalConfig(operational_issue_days=[25, 1, 10, 1])
    assert config.operational_issue_days == (1, 10, 25)


def test_operational_issue_days_empty_sequence_disables_filtering() -> None:
    config = ForecastSkillEvalConfig(operational_issue_days=[])
    assert config.operational_issue_days == ()


@pytest.mark.parametrize(
    "days",
    [
        [0],
        [32],
        [1, 0],
        [31, 32],
        ["1"],
        [1.5],
    ],
)
def test_operational_issue_days_rejects_invalid(days: list[object]) -> None:
    with pytest.raises(ValueError):
        ForecastSkillEvalConfig(operational_issue_days=days)


def test_regime_source_defaults_to_auto() -> None:
    """A config built without regime_source keeps the byte-identical default."""
    config = ForecastSkillEvalConfig()
    assert config.regime_source == "auto"


@pytest.mark.parametrize("value", ["auto", "flag", "date"])
def test_regime_source_accepts_valid_values(value: str) -> None:
    config = ForecastSkillEvalConfig(regime_source=value)
    assert config.regime_source == value


@pytest.mark.parametrize("value", ["", "issue_date", "FLAG", "dates"])
def test_regime_source_rejects_invalid(value: str) -> None:
    with pytest.raises(ValueError):
        ForecastSkillEvalConfig(regime_source=value)
