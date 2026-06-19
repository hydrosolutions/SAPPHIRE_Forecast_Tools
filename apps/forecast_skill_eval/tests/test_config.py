from __future__ import annotations

from pathlib import Path

import pytest

from forecast_skill_eval.config import (
    DEFAULT_BASINS_BY_PREFIX,
    DEFAULT_ERROR_FLAGS,
    DEFAULT_HINDCAST_FLAGS,
    DEFAULT_NAN_EXCLUDE_FLAGS,
    DEFAULT_OPERATIONAL_FLAGS,
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
