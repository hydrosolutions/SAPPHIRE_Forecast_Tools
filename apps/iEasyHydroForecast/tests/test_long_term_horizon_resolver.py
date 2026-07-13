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


# ---------------------------------------------------------------------------
# M1 P1: generic operational_schedule_for_mode / operational_schedules
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("mode", "lead", "issue_day"),
    [
        ("month_0", 0, 10),
        ("month_1", 1, 25),
        ("month_2", 2, 25),
        ("month_3", 3, 25),
        ("quarter", 1, 25),
        ("seasonal_april", 0, 25),
    ],
)
def test_operational_schedule_for_mode_reads_lead_and_issue_day(
    monkeypatch, tmp_path, mode, lead, issue_day
):
    """The generic accessor exposes BOTH fields for every mode shape

    (month_N, quarter, seasonal_*) -- not just the lead exposed by the
    older quarter_horizon_value/seasonal_horizon_value helpers.
    """
    _set_long_term_env(monkeypatch, tmp_path, [mode])
    _write_config(
        tmp_path,
        mode,
        {"operational_month_lead_time": lead, "operational_issue_day": issue_day},
    )

    schedule = resolver.operational_schedule_for_mode(mode)

    assert schedule == resolver.OperationalSchedule(mode=mode, lead_time=lead, issue_day=issue_day)


def test_operational_schedule_for_mode_raises_for_missing_issue_day(monkeypatch, tmp_path):
    """A mode config with a lead but no issue_day must raise clearly.

    quarter_horizon_value() would silently accept this config (it never
    reads operational_issue_day) -- the new generic accessor must not.
    """
    _set_long_term_env(monkeypatch, tmp_path, ["month_1"])
    _write_config(tmp_path, "month_1", {"operational_month_lead_time": 1})

    with pytest.raises(resolver.LongTermHorizonResolverError, match="operational_issue_day"):
        resolver.operational_schedule_for_mode("month_1")


def test_operational_schedule_for_mode_raises_for_missing_lead(monkeypatch, tmp_path):
    _set_long_term_env(monkeypatch, tmp_path, ["month_1"])
    _write_config(tmp_path, "month_1", {"operational_issue_day": 25})

    with pytest.raises(resolver.LongTermHorizonResolverError, match="operational_month_lead_time"):
        resolver.operational_schedule_for_mode("month_1")


def test_operational_schedule_for_mode_raises_for_non_integer_issue_day(monkeypatch, tmp_path):
    _set_long_term_env(monkeypatch, tmp_path, ["month_1"])
    _write_config(
        tmp_path,
        "month_1",
        {"operational_month_lead_time": 1, "operational_issue_day": "not-a-day"},
    )

    with pytest.raises(resolver.LongTermHorizonResolverError, match="operational_issue_day"):
        resolver.operational_schedule_for_mode("month_1")


def test_operational_schedule_for_mode_rejects_unsupported_mode(monkeypatch, tmp_path):
    _set_long_term_env(monkeypatch, tmp_path, ["month_1"])
    _write_config(
        tmp_path, "month_1", {"operational_month_lead_time": 1, "operational_issue_day": 25}
    )

    with pytest.raises(resolver.UnsupportedLongTermModeError, match="not supported"):
        resolver.operational_schedule_for_mode("month_2")


def test_operational_schedules_returns_all_supported_modes(monkeypatch, tmp_path):
    modes = ["month_0", "month_1", "quarter", "seasonal_april"]
    _set_long_term_env(monkeypatch, tmp_path, modes)
    configs = {
        "month_0": (0, 10),
        "month_1": (1, 25),
        "quarter": (1, 25),
        "seasonal_april": (0, 25),
    }
    for mode, (lead, issue_day) in configs.items():
        _write_config(
            tmp_path,
            mode,
            {"operational_month_lead_time": lead, "operational_issue_day": issue_day},
        )

    schedules = resolver.operational_schedules()

    assert set(schedules) == set(modes)
    for mode, (lead, issue_day) in configs.items():
        assert schedules[mode] == resolver.OperationalSchedule(
            mode=mode, lead_time=lead, issue_day=issue_day
        )


# ---------------------------------------------------------------------------
# M1 P3 review fix: operational_lead_for_mode (lead-only, no issue_day)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("lead", [0, 1, 2])
def test_operational_lead_for_mode_reads_lead_without_issue_day(monkeypatch, tmp_path, lead):
    """The lead-only accessor resolves from a config that has NO issue_day.

    operational_schedule_for_mode would raise on this config; the read-path
    accessor must not, since it never needs operational_issue_day.
    """
    _set_long_term_env(monkeypatch, tmp_path, ["month_1"])
    _write_config(tmp_path, "month_1", {"operational_month_lead_time": lead})

    assert resolver.operational_lead_for_mode("month_1") == lead


def test_operational_lead_for_mode_ignores_issue_day_when_present(monkeypatch, tmp_path):
    _set_long_term_env(monkeypatch, tmp_path, ["month_1"])
    _write_config(
        tmp_path, "month_1", {"operational_month_lead_time": 2, "operational_issue_day": 25}
    )

    assert resolver.operational_lead_for_mode("month_1") == 2


def test_operational_lead_for_mode_rejects_unsupported_mode(monkeypatch, tmp_path):
    _set_long_term_env(monkeypatch, tmp_path, ["month_1"])
    _write_config(tmp_path, "month_1", {"operational_month_lead_time": 1})

    with pytest.raises(resolver.UnsupportedLongTermModeError, match="not supported"):
        resolver.operational_lead_for_mode("month_2")


def test_operational_lead_for_mode_raises_for_missing_config_file(monkeypatch, tmp_path):
    _set_long_term_env(monkeypatch, tmp_path, ["month_2"])

    with pytest.raises(FileNotFoundError, match="month_2"):
        resolver.operational_lead_for_mode("month_2")


def test_operational_lead_for_mode_raises_for_missing_lead_field(monkeypatch, tmp_path):
    _set_long_term_env(monkeypatch, tmp_path, ["month_1"])
    _write_config(tmp_path, "month_1", {"operational_issue_day": 25})

    with pytest.raises(resolver.LongTermHorizonResolverError, match="operational_month_lead_time"):
        resolver.operational_lead_for_mode("month_1")


def test_operational_schedules_propagates_error_for_incomplete_mode(monkeypatch, tmp_path):
    """One incomplete mode config among several fails the whole enumeration,

    with an error identifying which mode is incomplete (not a silent
    partial result).
    """
    modes = ["month_0", "month_1"]
    _set_long_term_env(monkeypatch, tmp_path, modes)
    _write_config(
        tmp_path, "month_0", {"operational_month_lead_time": 0, "operational_issue_day": 10}
    )
    _write_config(tmp_path, "month_1", {"operational_month_lead_time": 1})  # missing issue_day

    with pytest.raises(resolver.LongTermHorizonResolverError, match="month_1"):
        resolver.operational_schedules()
