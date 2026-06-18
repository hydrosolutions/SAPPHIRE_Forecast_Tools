from __future__ import annotations

from pathlib import Path

import pytest

from forecast_skill_eval.config import DEFAULT_PROVENANCE, ForecastSkillEvalConfig


def test_config_defaults_are_captured_once() -> None:
    config = ForecastSkillEvalConfig()

    assert config.base_url == "http://localhost:8000"
    assert config.threshold == 0.80
    assert config.min_years == 10
    assert config.operational_start == "2024-01-01"
    assert config.horizons == ("day", "pentad", "decade", "month", "quarter", "season")
    assert config.provenance_by_horizon == DEFAULT_PROVENANCE
    assert config.provenance_by_horizon is not DEFAULT_PROVENANCE


def test_config_overrides_are_normalized_and_copied(tmp_path: Path) -> None:
    provenance = {"decad": "custom-official", "month": "custom-month"}
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
        min_years=12,
        operational_start="2024-02-01",
    )
    provenance["month"] = "mutated"

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
    assert config.min_years == 12
    assert config.operational_start == "2024-02-01"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"threshold": 0},
        {"threshold": 1.01},
        {"min_years": 0},
        {"horizons": ["hour"]},
        {"start_date": "2024-02-01", "end_date": "2024-01-01"},
        {"operational_start": "not-a-date"},
        {"provenance_by_horizon": {"hour": "calculated"}},
    ],
)
def test_invalid_config_inputs_are_rejected(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        ForecastSkillEvalConfig(**kwargs)
