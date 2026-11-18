"""Latest-only LT recovery; all identifiers and forecast values are synthetic."""

import copy
import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest
import recover_latest_forecasts as recovery
from long_term_recovery_io import key_tuple


class Config:
    def __init__(
        self, models=("LR_Base", "GBT"), months=None, lead=0, day=10, horizon="month", fixed=None
    ):
        self.models, self.months, self.lead, self.day, self.horizon = (
            models,
            months or {},
            lead,
            day,
            horizon,
        )
        self.fixed = fixed
        self.dependencies = {}

    def get_models_to_run(self):
        return self.models

    def get_model_execution_order(self):
        return self.models

    def get_operational_issue_day(self):
        return self.day

    def get_forecast_months(self, model):
        return self.months.get(model, list(range(1, 13)))

    def get_operational_month_lead_time(self):
        return self.lead

    def get_horizon_type(self):
        return self.horizon

    def get_calendar_month_adjustment(self):
        return self.horizon == "month"

    def get_target_start_month(self):
        return self.fixed[0] if self.fixed else None

    def get_target_end_month(self):
        return self.fixed[1] if self.fixed else None

    def get_forecast_horizon_months(self):
        return 3

    def get_model_dependencies(self):
        return self.dependencies


class Client:
    def __init__(self, rows=()):
        self.rows = {key_tuple(row): copy.deepcopy(row) for row in rows}
        self.writes = []
        self.fail_model = None
        self.corrupt_model = None

    def read_long_term_forecasts(self, **query):
        if query["model"] == self.fail_model:
            raise RuntimeError("synthetic API outage")
        rows = [
            r
            for r in self.rows.values()
            if r["model_type"] == query["model"]
            and r["code"] == query["code"]
            and r["date"] == query["start_date"]
            and r["horizon_type"] == query["horizon_type"]
            and r["horizon_value"] == query["horizon_value"]
        ]
        return pd.DataFrame(copy.deepcopy(rows[query["skip"] : query["skip"] + query["limit"]]))

    def write_long_forecasts(self, rows):
        self.writes.extend(copy.deepcopy(rows))
        for row in rows:
            stored = copy.deepcopy(row)
            if stored["model_type"] == self.corrupt_model:
                stored["q"] += 1
            self.rows[key_tuple(row)] = stored
        return len(rows)


def key(config, model="GBT", issue="2026-11-10", code="90001"):
    return recovery.expected_key(config, model, dt.date.fromisoformat(issue), code)


def run(
    tmp_path, config, client, compute=None, reference="2026-11-18", modes=("month_0",), factory=None
):
    path = tmp_path / "fresh.json"
    calls = []

    def calculate(conf, model, issue, codes, writer):
        calls.append((model, issue, codes))
        records = [
            dict(key(conf, model, issue.isoformat(), code), flag=1, **{f"Q_{model}": 12.0})
            for code in codes
        ]
        return writer(
            forecast_df=pd.DataFrame(records),
            model_name=model,
            horizon_type=conf.get_horizon_type(),
            horizon_value=conf.lead,
        )

    result = recovery.run_latest(
        reference,
        modes,
        ["90001"],
        client,
        path,
        config_factory=factory or (lambda mode: config),
        compute=compute or calculate,
    )
    return result, json.loads(path.read_text()), calls


@pytest.mark.parametrize(
    "ref,day,months,expected",
    [
        ("2026-11-18", 25, [11, 10], "2026-10-25"),
        ("2026-01-05", 10, list(range(1, 13)), "2025-12-10"),
        ("2024-02-29", 31, [2], "2024-02-29"),
        ("2026-11-18", 25, [12], "2025-12-25"),
    ],
)
def test_latest_selector_never_selects_future(ref, day, months, expected):
    assert recovery.latest_issue(ref, day, months).isoformat() == expected


def test_latest_healthy_does_not_backfill_old_gap(tmp_path):
    config = Config(models=("GBT",))
    client = Client([dict(key(config), flag=0, q=10.0)])
    result, handoff, calls = run(tmp_path, config, client)
    assert not result["errors"] and not calls and not client.writes and handoff["scopes"] == []


def test_existing_recovered_row_retries_pp_without_compute(tmp_path):
    config = Config(models=("GBT",))
    client = Client([dict(key(config), flag=1, q=10.0)])
    for _ in range(2):
        result, handoff, calls = run(tmp_path, config, client)
        assert not result["errors"] and not calls
        assert handoff["scopes"][0]["triggers"] == [key(config)]
    assert not client.writes


def test_absent_and_failed_keys_recover_and_preserve_observation(tmp_path):
    config = Config()
    client = Client([dict(key(config), flag=2, q_obs=9.0)])
    result, handoff, calls = run(tmp_path, config, client)
    assert not result["errors"] and result["recovered"] == 2
    assert [c[0] for c in calls] == ["LR_Base", "GBT"]
    assert len(handoff["scopes"][0]["triggers"]) == 2
    assert client.rows[key_tuple(key(config))]["q_obs"] == 9.0
    assert all(r["flag"] == 1 for r in client.writes)


def test_independent_member_dates_and_outside_window(tmp_path):
    config = Config(months={"GBT": [10], "LR_Base": [9]})
    result, handoff, calls = run(tmp_path, config, Client())
    assert result["blocked"] == 1 and not result["errors"]
    assert calls[0][1] == dt.date(2026, 10, 10)
    assert [s["date"] for s in handoff["scopes"]] == ["2026-10-10"]


def test_quantile_only_healthy_is_never_overwritten(tmp_path):
    config = Config(models=("GBT",))
    client = Client([dict(key(config), flag=0, q75=19.0)])
    _, _, calls = run(tmp_path, config, client)
    assert not calls and not client.writes


def test_healthy_arrives_during_compute_is_protected(tmp_path):
    config = Config(models=("GBT",))
    client = Client()

    def calculate(conf, model, issue, codes, writer):
        client.rows[key_tuple(key(config))] = dict(key(config), flag=0, q=99.0)
        return writer(
            forecast_df=pd.DataFrame([dict(key(config), flag=1, Q_GBT=12.0)]),
            model_name=model,
            horizon_type="month",
            horizon_value=0,
        )

    result, handoff, _ = run(tmp_path, config, client, calculate)
    assert not result["errors"] and not client.writes and not handoff["scopes"]


def test_partial_delivery_finalizes_verified_only_and_restarts(tmp_path):
    config = Config()
    client = Client()
    client.corrupt_model = "GBT"
    result, handoff, _ = run(tmp_path, config, client)
    assert result["errors"] and result["recovered"] == 1
    assert {k["model_type"] for k in handoff["scopes"][0]["triggers"]} == {"LR_Base"}
    client.corrupt_model = None
    result, handoff, calls = run(tmp_path, config, client)
    assert (
        not calls and not result["errors"]
    )  # API current flag1 state is authoritative on restart.
    assert len(handoff["scopes"][0]["triggers"]) == 2


def test_later_member_read_failure_does_not_shrink_source_authorization(tmp_path):
    config = Config()
    client = Client([dict(key(config, "LR_Base"), flag=1, q=12.0)])
    client.fail_model = "GBT"
    result, handoff, _ = run(tmp_path, config, client)
    assert result["errors"]
    assert {k["model_type"] for k in handoff["scopes"][0]["sources"]} == {"LR_Base", "GBT"}


def test_missing_declared_mode_does_not_block_valid_mode(tmp_path):
    config = Config(models=("GBT",))

    def factory(mode):
        if mode == "missing":
            raise FileNotFoundError("synthetic missing configuration")
        return config

    result, handoff, calls = run(
        tmp_path, config, Client(), modes=("monthly", "missing", "month_0"), factory=factory
    )
    assert result["errors"] and result["recovered"] == 1 and calls
    assert handoff["scopes"]


def test_dependency_healthy_in_api_avoids_recomputation(tmp_path):
    config = Config()
    config.dependencies = {"GBT": ["LR_Base"]}
    client = Client([dict(key(config, "LR_Base"), flag=0, q=12.0)])
    result, _, calls = run(tmp_path, config, client)
    assert not result["errors"] and [c[0] for c in calls] == ["GBT"]


@pytest.mark.parametrize("dependency_state", ["absent", "unhealthy", "read_error", "second_bad"])
def test_dependency_failure_isolates_station_and_preserves_sources(
    tmp_path, monkeypatch, dependency_state
):
    config = Config()
    dependencies = ["LR_SM", "LR_SM_DT"] if dependency_state == "second_bad" else ["LR_SM"]
    config.dependencies = {"GBT": dependencies}
    bad_key = key(config, code="90001")
    good_key = key(config, code="90002")
    rows = [dict(bad_key, flag=2, q=None)]
    # A recovered independent member keeps A's complete source authorization visible.
    rows.extend(
        dict(key(config, "LR_Base", code=code), flag=1 if code == "90001" else 0, q=10.0)
        for code in ("90001", "90002")
    )
    rows.extend(dict(key(config, dep, code="90002"), flag=0, q=10.0) for dep in dependencies)
    if dependency_state == "unhealthy":
        rows.append(dict(key(config, "LR_SM"), flag=2, q=None))
    if dependency_state == "second_bad":
        rows.append(dict(key(config, "LR_SM"), flag=0, q=10.0))
    client = Client(rows)
    before = copy.deepcopy(client.rows)
    read = client.read_long_term_forecasts

    def read_dependency(**query):
        if (
            dependency_state == "read_error"
            and query["model"] == "LR_SM"
            and query["code"] == "90001"
        ):
            raise RuntimeError("synthetic dependency read failure")
        return read(**query)

    monkeypatch.setattr(client, "read_long_term_forecasts", read_dependency)
    calls = []

    def compute(conf, model, issue, codes, writer):
        calls.append((model, codes))
        return writer(
            forecast_df=pd.DataFrame([dict(good_key, flag=1, Q_GBT=12.0)]),
            model_name=model,
            horizon_type="month",
            horizon_value=0,
        )

    path = tmp_path / "handoff.json"
    result = recovery.run_latest(
        "2026-11-18",
        ["month_0"],
        ["90001", "90002"],
        client,
        path,
        config_factory=lambda mode: config,
        compute=compute,
    )
    assert calls == [("GBT", ["90002"])]
    assert result["recovered"] == 1 and len(result["errors"]) == 1
    assert int(bool(result["errors"])) == 1  # CLI nonzero-exit contract.
    assert client.rows[key_tuple(bad_key)] == before[key_tuple(bad_key)]
    assert [key_tuple(row) for row in client.writes] == [key_tuple(good_key)]
    assert client.rows[key_tuple(good_key)]["q"] == 12.0
    scopes = {scope["code"]: scope for scope in json.loads(path.read_text())["scopes"]}
    assert scopes["90002"]["triggers"] == [good_key]
    assert bad_key in scopes["90001"]["sources"]
    assert bad_key not in scopes["90001"]["triggers"]


def test_no_dependency_ready_station_never_computes(tmp_path):
    config = Config(models=("GBT",))
    config.dependencies = {"GBT": ["LR_Base"]}
    result, handoff, calls = run(tmp_path, config, Client())
    assert result["errors"] and not calls and not result["recovered"]
    assert handoff["scopes"] == []


def test_wrong_computed_interval_is_rejected_before_writes(tmp_path):
    config = Config(models=("GBT",))
    client = Client()

    def compute(conf, model, issue, codes, writer):
        return writer(
            forecast_df=pd.DataFrame(
                [dict(key(config), flag=1, Q_GBT=12.0, valid_to="2026-11-29")]
            ),
            model_name=model,
            horizon_type="month",
            horizon_value=0,
        )

    result, _, _ = run(tmp_path, config, client, compute)
    assert result["errors"] and not client.writes


def test_expected_dates_calendar_and_fixed_season():
    assert key(Config(lead=2), issue="2026-12-10")["valid_from"] == "2027-02-01"
    season = key(Config(horizon="season", fixed=(11, 2)), issue="2026-01-10")
    assert (season["valid_from"], season["valid_to"]) == ("2026-11-01", "2027-02-28")
    quarter = key(Config(horizon="quarter", lead=1), issue="2026-12-10")
    assert (quarter["valid_from"], quarter["valid_to"]) == ("2027-01-01", "2027-03-31")


def test_quarter_handoff_requires_independently_current_quarter(tmp_path):
    configs = {f"month_{lead}": Config(models=("GBT",), lead=lead, day=25) for lead in (1, 2, 3)}
    configs["quarter"] = Config(
        models=("LR_Base",), lead=1, day=25, horizon="quarter", months={"LR_Base": [3, 6, 9, 12]}
    )
    rows = [
        dict(key(conf, conf.models[0], "2026-09-25"), flag=1 if mode == "month_1" else 0, q=12.0)
        for mode, conf in configs.items()
    ]
    client = Client(rows)
    result, handoff, calls = run(
        tmp_path,
        configs["month_1"],
        client,
        reference="2026-10-18",
        modes=tuple(configs),
        factory=configs.__getitem__,
    )
    assert not result["errors"] and not calls
    quarters = [scope for scope in handoff["scopes"] if scope["horizon_type"] == "quarter"]
    assert len(quarters) == 1 and len(quarters[0]["sources"]) == 4
    result, handoff, _ = run(
        tmp_path, configs["month_1"], client, modes=tuple(configs), factory=configs.__getitem__
    )
    assert result["blocked"] == 1
    assert not any(scope["horizon_type"] == "quarter" for scope in handoff["scopes"])
    assert all(scope["date"] == "2026-10-25" for scope in handoff["scopes"])


def test_existing_single_model_default_writer_and_recovery_seam(monkeypatch):
    import run_forecast as runner

    class MinimalConfig(Config):
        all_paths = {"GBT": "/synthetic/model/GBT"}

        def get_model_specific_config(self, model_name):
            return {"general_config": {"model_type": "synthetic"}, "path_config": {}}

        def get_data_dependencies(self, model_name):
            return {"Discharge": 0}

        def get_output_path(self, model_name):
            return "/synthetic/output"

    monkeypatch.setattr(
        runner, "check_valid_forecast_issue_date", lambda **kwargs: pd.Timestamp("2026-11-10")
    )
    monkeypatch.setattr(runner, "post_process_lt_forecast", lambda **kwargs: kwargs["raw_forecast"])
    default, recovery_calls = [], []

    def ordinary(**kwargs):
        default.append(kwargs)
        return True

    def recovery_writer(**kwargs):
        recovery_calls.append(kwargs)
        return True

    monkeypatch.setattr(runner, "save_forecast", ordinary)
    arguments = dict(
        data_interface=None,
        forecast_configs=MinimalConfig(models=("GBT",)),
        model_name="GBT",
        temporal_data=pd.DataFrame(),
        static_data=pd.DataFrame(),
        offset_base=0,
        offset_discharge=1,
        station_codes=["90001"],
    )
    runner.run_single_model(**arguments)
    runner.run_single_model(**arguments, forecast_writer=recovery_writer)
    assert len(default) == len(recovery_calls) == 1
    assert default[0]["horizon_type"] == recovery_calls[0]["horizon_type"] == "month"
    assert default[0]["horizon_value"] == recovery_calls[0]["horizon_value"] == 0


def test_recovery_cli_starts_in_fresh_application_process():
    app_dir = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["SAPPHIRE_TEST_ENV"] = "True"
    env.pop("ieasyhydroforecast_env_file_path", None)
    result = subprocess.run(
        [sys.executable, "recover_latest_forecasts.py", "--help"],
        cwd=app_dir,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "--forecast-date" in result.stdout
    assert "--handoff" in result.stdout
