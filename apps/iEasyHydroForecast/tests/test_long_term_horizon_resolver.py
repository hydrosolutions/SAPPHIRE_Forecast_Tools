import json

import long_term_horizon_resolver as resolver
import pytest


def _write_config(tmp_path, config_name: str, payload: dict) -> None:
    config_dir = tmp_path / "config" / "long_term_configs"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / f"{config_name}.json").write_text(json.dumps(payload))


def _set_long_term_env(monkeypatch, tmp_path, supported_modes: list[str]) -> None:
    monkeypatch.setenv(resolver.CONFIG_ROOT_ENV, str(tmp_path / "config"))
    monkeypatch.setenv(resolver.LONG_TERM_CONFIG_DIR_ENV, "long_term_configs")
    monkeypatch.setenv(resolver.SUPPORTED_MODES_ENV, ",".join(supported_modes))


@pytest.mark.parametrize("lead", [1, 0])
def test_quarter_horizon_value_reads_configured_lead(monkeypatch, tmp_path, lead):
    _set_long_term_env(monkeypatch, tmp_path, ["quarter"])
    _write_config(tmp_path, "quarter", {"operational_month_lead_time": lead})

    assert resolver.quarter_horizon_value() == lead


@pytest.mark.parametrize(
    ("issue_month", "config_name", "lead"),
    [
        (1, "seasonal_january", 3),
        (2, "seasonal_february", 2),
        (3, "seasonal_march", 1),
        (4, "seasonal_april", 0),
    ],
)
def test_seasonal_horizon_value_reads_issue_month_lead(
    monkeypatch, tmp_path, issue_month, config_name, lead
):
    supported_modes = [
        "seasonal_january",
        "seasonal_february",
        "seasonal_march",
        "seasonal_april",
    ]
    _set_long_term_env(monkeypatch, tmp_path, supported_modes)
    _write_config(tmp_path, config_name, {"operational_month_lead_time": lead})

    assert resolver.seasonal_horizon_value(issue_month) == lead


@pytest.mark.parametrize(
    ("issue_month", "config_name"),
    [
        (1, "seasonal_january"),
        (2, "seasonal_february"),
        (3, "seasonal_march"),
        (4, "seasonal_april"),
    ],
)
def test_seasonal_config_name_maps_issue_month_to_config(issue_month, config_name):
    assert resolver.seasonal_config_name(issue_month) == config_name


def test_seasonal_config_name_rejects_unmapped_issue_month():
    with pytest.raises(ValueError, match="Seasonal issue month"):
        resolver.seasonal_config_name(5)


@pytest.mark.parametrize("issue_month", [1, 2, 3])
def test_seasonal_horizon_value_rejects_unsupported_issue(monkeypatch, tmp_path, issue_month):
    _set_long_term_env(monkeypatch, tmp_path, ["seasonal_april"])
    _write_config(tmp_path, "seasonal_april", {"operational_month_lead_time": 0})

    with pytest.raises(resolver.UnsupportedLongTermModeError, match="not supported"):
        resolver.seasonal_horizon_value(issue_month)


def test_seasonal_horizon_value_allows_april_only_deployment(monkeypatch, tmp_path):
    _set_long_term_env(monkeypatch, tmp_path, ["seasonal_april"])
    _write_config(tmp_path, "seasonal_april", {"operational_month_lead_time": 0})

    assert resolver.seasonal_horizon_value(4) == 0


def test_quarter_horizon_value_raises_for_missing_config_file(monkeypatch, tmp_path):
    _set_long_term_env(monkeypatch, tmp_path, ["quarter"])

    with pytest.raises(FileNotFoundError, match="quarter"):
        resolver.quarter_horizon_value()


def test_quarter_horizon_value_raises_for_missing_lead_field(monkeypatch, tmp_path):
    _set_long_term_env(monkeypatch, tmp_path, ["quarter"])
    _write_config(tmp_path, "quarter", {"horizon_type": "quarter"})

    with pytest.raises(resolver.LongTermHorizonResolverError, match="operational_month_lead_time"):
        resolver.quarter_horizon_value()


def test_quarter_horizon_value_raises_for_invalid_json(monkeypatch, tmp_path):
    _set_long_term_env(monkeypatch, tmp_path, ["quarter"])
    config_dir = tmp_path / "config" / "long_term_configs"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "quarter.json").write_text("{not-json")

    with pytest.raises(resolver.LongTermHorizonResolverError, match="invalid JSON"):
        resolver.quarter_horizon_value()


def test_supported_long_term_modes_strips_empty_entries(monkeypatch, tmp_path):
    _set_long_term_env(monkeypatch, tmp_path, [])
    monkeypatch.setenv(
        resolver.SUPPORTED_MODES_ENV,
        " quarter, seasonal_april, , seasonal_january ",
    )

    assert resolver.supported_long_term_modes() == [
        "quarter",
        "seasonal_april",
        "seasonal_january",
    ]
