"""Stateful API/audit tests for bounded recovered DAY-to-period consumption."""

import datetime as dt
import json
from dataclasses import replace

import pandas as pd
import pytest
from src import recovered_periods as rp


@pytest.fixture
def recovery(tmp_path, monkeypatch, pentad_config):
    monkeypatch.setenv("SAPPHIRE_TEST_ENV", "false")
    monkeypatch.setenv("ML_FAILED_ISSUE_RECOVERY_ENABLED", "true")
    monkeypatch.setenv("SAPPHIRE_API_URL", "http://test.invalid")
    store, written, calls = {}, [], []
    issue = dt.date(2026, 11, 10)
    row = dict(
        date=pd.Timestamp(issue),
        code="101",
        model_short="TFT",
        pentad_in_year=63,
        pentad_in_month=3,
        forecasted_discharge=3.0,
        q05=1.0,
        q25=2.0,
        q75=4.0,
        q95=5.0,
    )

    def read(client, codes, model, horizon, start, end, forecast, **kwargs):
        calls.append((codes, model, horizon, start, end))
        key = (model.upper(), start.isoformat())
        value = store.get(key)
        if value:
            value = dict(value)
            value.setdefault("model_type", model)
            value.setdefault("horizon_type", horizon)
            value.setdefault("target", pd.Timestamp(start) + pd.Timedelta(days=1))
            value.setdefault("horizon_value", 3)
            value.setdefault("horizon_in_year", 63)
        return (
            rp.data_reader.RECOVERY_STATUS_COMPLETE
            if value
            else rp.data_reader.RECOVERY_STATUS_EMPTY,
            pd.DataFrame([value]) if value else pd.DataFrame(),
            "",
        )

    def write(frame, horizon):
        written.append(frame.copy())
        for _, item in frame.iterrows():
            value = item.to_dict()
            value.update(
                model_type=item.model_short,
                target=item.date + pd.Timedelta(days=1),
                horizon_type=horizon,
                horizon_value=3,
                horizon_in_year=63,
            )
            store[(item.model_short.upper(), item.date.date().isoformat())] = value
        return True

    def neural(frame):
        ne = frame.iloc[0].copy()
        ne["model_short"] = "NE"
        ne["composition"] = "TFT"
        return pd.concat([frame, pd.DataFrame([ne])], ignore_index=True)

    monkeypatch.setattr(rp.data_reader, "read_combined_period_forecasts_for_recovery", read)

    def day_read(client, codes, model, start, end, forecast, **kwargs):
        if model != "TFT":
            return rp.data_reader.RECOVERY_STATUS_EMPTY, pd.DataFrame(), ""
        daily = pd.DataFrame(
            [
                dict(row, target=pd.Timestamp(start) + pd.Timedelta(days=i), horizon_type="day")
                for i in range(1, 12)
            ]
        )
        return rp.data_reader.RECOVERY_STATUS_COMPLETE, daily, ""

    monkeypatch.setattr(rp.data_reader, "read_day_forecasts_for_recovery", day_read)
    monkeypatch.setattr(
        rp.data_reader,
        "read_lr_forecasts_for_recovery",
        lambda *a, **k: (rp.data_reader.RECOVERY_STATUS_EMPTY, pd.DataFrame(), ""),
    )
    monkeypatch.setattr(
        rp.data_reader,
        "read_individual_model_forecasts_for_dates",
        lambda horizon, dates, codes: (pd.DataFrame([row]), pd.DataFrame()),
    )
    real_skill_reader = rp.data_reader._read_skill_metrics_api
    monkeypatch.setattr(rp.data_reader, "_read_skill_metrics_api", lambda *a, **k: pd.DataFrame())
    monkeypatch.setattr(rp.api_writer, "_write_combined_forecast_to_api", write)
    config = replace(pentad_config, neural_ensemble_func=neural)

    def audit(date=issue, model="TFT"):
        state = dict(
            issue_date=date.isoformat(),
            model=model,
            code="101",
            status="verified",
            period_rebuild_pending=True,
            pending_horizons=["pentad", "decade"],
        )
        path = tmp_path / (rp._identity(state) + ".json")
        path.write_text(json.dumps(state))
        return path

    def run(**kwargs):
        return rp.consume_pending_recoveries(
            config, dt.date(2026, 11, 17), audit_dir=tmp_path, client=object(), **kwargs
        )

    return dict(
        real_skill_reader=real_skill_reader,
        audit=audit,
        run=run,
        written=written,
        store=store,
        calls=calls,
        row=row,
        config=config,
        directory=tmp_path,
    )


def test_absent_combined_date_is_created_verified_and_idempotent(recovery):
    r = recovery
    path = r["audit"]()
    assert r["run"]() == {"completed": 1, "errors": []}
    assert set(r["written"][0].model_short) == {"TFT", "NE"}
    assert json.loads(path.read_text())["pending_horizons"] == ["decade"]
    assert r["run"]() == {"completed": 0, "errors": []}
    assert len(r["written"]) == 1
    assert all(call[3] == call[4] == dt.date(2026, 11, 10) for call in r["calls"])


def test_preserves_existing_individual_values_and_rebuilds_ne(recovery):
    r = recovery
    r["audit"]()
    existing = dict(r["row"], forecasted_discharge=9.0)
    r["store"][("TFT", "2026-11-10")] = existing
    assert not r["run"]()["errors"]
    assert list(r["written"][0].model_short) == ["NE"]
    assert r["written"][0].iloc[0].forecasted_discharge == 9.0
    assert r["store"][("TFT", "2026-11-10")]["forecasted_discharge"] == 9.0


@pytest.mark.parametrize(
    "failure",
    ["false", "exception", "partial", "read", "wrong_date", "null", "day_error", "lr_error"],
)
def test_failed_or_partial_recovery_remains_pending(recovery, monkeypatch, failure):
    r = recovery
    path = r["audit"]()
    if failure == "false":
        monkeypatch.setattr(rp.api_writer, "_write_combined_forecast_to_api", lambda *a: False)
    elif failure == "exception":

        def fail(*a):
            raise RuntimeError("unavailable")

        monkeypatch.setattr(rp.api_writer, "_write_combined_forecast_to_api", fail)
    elif failure == "partial":
        monkeypatch.setattr(rp.api_writer, "_write_combined_forecast_to_api", lambda *a: True)
    elif failure == "read":
        monkeypatch.setattr(
            rp.data_reader,
            "read_combined_period_forecasts_for_recovery",
            lambda *a, **k: ("FAILED", None, "unavailable"),
        )
    elif failure in ("day_error", "lr_error"):
        function = (
            "read_day_forecasts_for_recovery"
            if failure == "day_error"
            else "read_lr_forecasts_for_recovery"
        )
        monkeypatch.setattr(
            rp.data_reader, function, lambda *a, **k: ("FAILED", None, "unavailable")
        )
    elif failure == "wrong_date":
        r["row"]["date"] = pd.Timestamp("2026-11-15")
    elif failure == "null":
        r["row"]["q05"] = None
    assert r["run"]()["errors"]
    assert "pentad" in json.loads(path.read_text())["pending_horizons"]
    if failure in ("read", "wrong_date", "null", "day_error", "lr_error"):
        assert not r["written"]


def test_nonboundary_decade_never_uses_different_issue(recovery, decad_config):
    r = recovery
    path = r["audit"](dt.date(2026, 11, 15))
    result = rp.consume_pending_recoveries(
        decad_config, dt.date(2026, 11, 17), audit_dir=r["directory"], client=object()
    )
    assert result == {"completed": 1, "errors": []}
    state = json.loads(path.read_text())
    assert state["pending_horizons"] == ["pentad"]
    assert state["period_rebuild_results"]["decade"] == "not_applicable_non_boundary"
    assert not r["calls"] and not r["written"]


@pytest.mark.parametrize(
    "setting", ["no_pending", "test_mode", "opt_out", "other_api", "other_code"]
)
def test_no_authorized_pending_means_no_api_work(recovery, monkeypatch, setting):
    r = recovery
    if setting != "no_pending":
        r["audit"]()
    if setting == "test_mode":
        monkeypatch.setenv("SAPPHIRE_TEST_ENV", "true")
    if setting == "opt_out":
        monkeypatch.setenv("ML_FAILED_ISSUE_RECOVERY_ENABLED", "false")
    if setting == "other_api":
        monkeypatch.setenv("SAPPHIRE_API_URL", "http://other.invalid")
    result = r["run"](codes=["102"] if setting == "other_code" else None)
    assert result == {"completed": 0, "errors": []}
    assert not r["calls"] and not r["written"]


def test_rebuilds_and_verifies_em_composition(recovery, monkeypatch):
    r = recovery
    r["audit"]()
    monkeypatch.setattr(
        rp.data_reader,
        "_read_skill_metrics_api",
        lambda *a, **k: pd.DataFrame([{"nse": 1, "model_short": "TFT"}]),
    )

    def ensemble(**kwargs):
        row = dict(r["row"], model_short="EM", composition="TFT")
        return pd.DataFrame([row]), pd.DataFrame()

    monkeypatch.setattr(rp.ensemble_calculator, "create_ensemble_forecasts", ensemble)
    assert not r["run"]()["errors"]
    assert set(r["written"][0].model_short) == {"TFT", "NE", "EM"}


def test_real_day_aggregation_and_ne_heal_entirely_empty_combined(recovery, monkeypatch):
    import setup_library as sl

    r = recovery
    r["audit"]()
    raw = pd.DataFrame(
        {
            "date": ["2026-11-10"] * 11,
            "target": pd.date_range("2026-11-11", periods=11),
            "code": ["101"] * 11,
            "horizon_type": ["day"] * 11,
            "forecasted_discharge": [3.0] * 5 + [999.0] * 6,
            "q05": [1.0] * 11,
            "q25": [2.0] * 11,
            "q75": [4.0] * 11,
            "q95": [5.0] * 11,
        }
    )

    def read_daily(client, codes, model, *args, **kwargs):
        if model == "TFT":
            return rp.data_reader.RECOVERY_STATUS_COMPLETE, raw, ""
        return rp.data_reader.RECOVERY_STATUS_EMPTY, pd.DataFrame(), ""

    monkeypatch.setattr(rp.data_reader, "read_day_forecasts_for_recovery", read_daily)
    config = replace(r["config"], neural_ensemble_func=sl.calculate_neural_ensemble_forecast)
    result = rp.consume_pending_recoveries(
        config, dt.date(2026, 11, 17), audit_dir=r["directory"], client=object()
    )
    assert result == {"completed": 1, "errors": []}
    assert set(r["written"][0].model_short) == {"TFT", "NE"}
    assert (r["written"][0].forecasted_discharge == 3.0).all()


def test_existing_ne_members_cannot_be_silently_dropped(recovery):
    r = recovery
    path = r["audit"]()
    r["store"][("NE", "2026-11-10")] = dict(r["row"], model_short="NE", composition="TFT, TiDE")
    assert r["run"]()["errors"]
    assert not r["written"]
    assert json.loads(path.read_text())["period_rebuild_pending"] is True


@pytest.mark.parametrize(
    "column,value",
    [
        ("target", "2026-11-16"),
        ("horizon_type", "day"),
        ("horizon_in_year", 64),
        ("horizon_value", 4),
    ],
)
def test_wrong_period_identity_is_rejected(recovery, column, value):
    r = recovery
    path = r["audit"]()
    r["store"][("TFT", "2026-11-10")] = dict(r["row"], **{column: value})
    assert r["run"]()["errors"]
    assert not r["written"]
    assert json.loads(path.read_text())["period_rebuild_pending"] is True


def test_api_tide_and_tsmixer_skill_names_join_real_ensemble(recovery, monkeypatch):
    import setup_library as sl

    r = recovery
    r["audit"](model="TIDE")
    r["audit"](model="TSMIXER")

    def days(client, codes, model, start, end, forecast, **kwargs):
        if model == "TFT":
            return rp.data_reader.RECOVERY_STATUS_EMPTY, pd.DataFrame(), ""
        daily = pd.DataFrame(
            [
                dict(
                    r["row"],
                    model_short=model,
                    forecasted_discharge=2.0 if model == "TIDE" else 4.0,
                    horizon_type="day",
                    target=pd.Timestamp(start) + pd.Timedelta(days=i),
                )
                for i in range(1, 12)
            ]
        )
        return rp.data_reader.RECOVERY_STATUS_COMPLETE, daily, ""

    class SkillClient:
        def __init__(self, **kwargs):
            pass

        def readiness_check(self):
            return True

        def read_skill_metrics(self, **kwargs):
            return pd.DataFrame(
                [
                    dict(
                        code="101",
                        model_type=model,
                        horizon_in_year=63,
                        nse=0.8,
                        n_pairs=100,
                        mae=1.0,
                        accuracy=0.9,
                        sdivsigma=0.2,
                        delta=1.0,
                    )
                    for model in ("TiDE", "TSMixer")
                ]
            )

    monkeypatch.setattr(rp.data_reader, "read_day_forecasts_for_recovery", days)
    monkeypatch.setattr(rp.data_reader, "SapphirePostprocessingClient", SkillClient)
    monkeypatch.setattr(rp.data_reader, "SAPPHIRE_API_AVAILABLE", True)
    monkeypatch.setattr(rp.data_reader, "_read_skill_metrics_api", r["real_skill_reader"])
    config = replace(r["config"], neural_ensemble_func=sl.calculate_neural_ensemble_forecast)
    result = rp.consume_pending_recoveries(
        config, dt.date(2026, 11, 17), audit_dir=r["directory"], client=object()
    )
    assert result == {"completed": 2, "errors": []}
    em = r["written"][0].query("model_short == 'EM'").iloc[0]
    assert em.forecasted_discharge == 3.0
    assert rp._members(em.composition) == {"TIDE", "TSMIXER"}


def test_three_blocked_old_dates_do_not_starve_new_date_after_restart(recovery, monkeypatch):
    import importlib

    r = recovery
    old = [dt.date(2026, 10, day) for day in (5, 10, 15)]
    paths = [r["audit"](date) for date in old]
    newer = r["audit"]()
    attempted = []

    def repair(config, issue, *args):
        attempted.append(issue)
        if issue in old:
            raise RuntimeError("permanently blocked source")

    monkeypatch.setattr(rp, "_repair", repair)
    first = r["run"]()
    assert len(first["errors"]) == 3 and first["completed"] == 0
    assert attempted == old
    assert all(
        json.loads(p.read_text())["period_rebuild_attempted_at"]["pentad"] > 0 for p in paths
    )
    assert "period_rebuild_attempted_at" not in json.loads(newer.read_text())
    # Reload discards module memory: fairness must come only from private audit files.
    importlib.reload(rp)
    monkeypatch.setattr(rp, "_repair", repair)
    attempted.clear()
    second = r["run"]()
    assert second["completed"] == 1
    assert dt.date(2026, 11, 10) in attempted and len(attempted) == 3
    assert json.loads(newer.read_text())["pending_horizons"] == ["decade"]
    assert all("pentad" in json.loads(p.read_text())["pending_horizons"] for p in paths)


def test_only_bounded_source_reads_are_used(recovery, monkeypatch):
    r = recovery
    r["audit"]()

    def broad_read(*args, **kwargs):
        raise AssertionError("Year-wide ordinary reader must never be used")

    monkeypatch.setattr(rp.data_reader, "read_individual_model_forecasts_for_dates", broad_read)
    original = rp.data_reader.read_day_forecasts_for_recovery
    bounds = []

    def bounded(client, codes, model, start, end, forecast, **kwargs):
        bounds.append((start, end, codes))
        assert kwargs["total_deadline"] > rp.time.monotonic()
        return original(client, codes, model, start, end, forecast, **kwargs)

    monkeypatch.setattr(rp.data_reader, "read_day_forecasts_for_recovery", bounded)
    assert not r["run"]()["errors"]
    assert bounds == [(dt.date(2026, 11, 10), dt.date(2026, 11, 10), ["101"])] * 3


def test_expired_budget_makes_no_source_calls(recovery, monkeypatch):
    r = recovery
    r["audit"]()

    def forbidden(*a, **k):
        raise AssertionError("Expired deadline must be checked before source read")

    monkeypatch.setattr(rp.data_reader, "read_combined_period_forecasts_for_recovery", forbidden)
    with pytest.raises(RuntimeError, match="deadline exceeded"):
        rp._repair(
            r["config"],
            dt.date(2026, 11, 10),
            "101",
            {"TFT"},
            dt.date(2026, 11, 17),
            object(),
            rp.time.monotonic() - 1,
        )
    assert not r["written"]


def test_budget_exhaustion_rotates_dates_and_stations_across_restart(recovery, monkeypatch):
    import importlib

    r = recovery
    old_date = dt.date(2026, 10, 5)
    old = r["audit"](old_date)
    state = json.loads(old.read_text())
    state["code"] = "102"
    second_station = r["directory"] / (rp._identity(state) + ".json")
    second_station.write_text(json.dumps(state))
    newer = r["audit"]()
    clock = {"now": 0.0}
    attempted = []

    def repair(config, issue, code, *args):
        attempted.append((issue, code))
        if issue == old_date:
            clock["now"] += 301.0
            raise RuntimeError("Source consumes entire recovery budget")

    monkeypatch.setattr(rp.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(rp.time, "time", lambda: 1000.0 + clock["now"])
    monkeypatch.setattr(rp, "_repair", repair)
    assert r["run"]()["completed"] == 0
    assert attempted == [(old_date, "101")]
    # Restart must not repeat the slow first station ahead of unattempted work.
    importlib.reload(rp)
    monkeypatch.setattr(rp, "_repair", repair)
    assert r["run"]()["completed"] == 0
    assert attempted == [(old_date, "101"), (old_date, "102")]
    importlib.reload(rp)
    monkeypatch.setattr(rp, "_repair", repair)
    assert r["run"]()["completed"] == 1
    assert attempted[2] == (dt.date(2026, 11, 10), "101")
    assert "pentad" not in json.loads(newer.read_text())["pending_horizons"]
    assert "pentad" in json.loads(old.read_text())["pending_horizons"]
    assert "pentad" in json.loads(second_station.read_text())["pending_horizons"]


def test_latest_filter_leaves_old_and_future_audits_untouched(recovery, monkeypatch):
    r = recovery
    old = r["audit"](dt.date(2026, 11, 10))
    future = r["audit"](dt.date(2026, 11, 20))
    current = r["audit"](dt.date(2026, 11, 15))
    before = {p: p.read_bytes() for p in (old, future)}
    calls = []
    monkeypatch.setattr(rp, "_repair", lambda config, issue, *a: calls.append(issue))
    assert r["run"](latest_only=True) == {"completed": 1, "errors": []}
    assert calls == [dt.date(2026, 11, 15)]
    assert all(p.read_bytes() == data for p, data in before.items())
    assert json.loads(current.read_text())["pending_horizons"] == ["decade"]


def test_latest_shared_issue_remains_current_for_decade(recovery, monkeypatch):
    r = recovery
    path = r["audit"](dt.date(2026, 11, 10))
    calls = []
    monkeypatch.setattr(rp, "_repair", lambda config, issue, *a: calls.append((config.name, issue)))
    assert r["run"](latest_only=True)["completed"] == 0
    result = rp.consume_pending_recoveries(
        replace(r["config"], name="decad"),
        dt.date(2026, 11, 17),
        latest_only=True,
        audit_dir=r["directory"],
        client=object(),
    )
    assert result == {"completed": 1, "errors": []}
    assert calls == [("decad", dt.date(2026, 11, 10))]
    assert json.loads(path.read_text())["pending_horizons"] == ["pentad"]


def test_latest_failure_keeps_pending_for_retry(recovery, monkeypatch):
    path = recovery["audit"](dt.date(2026, 11, 15))

    def fail(*args):
        raise RuntimeError("Synthetic API failure")

    monkeypatch.setattr(rp, "_repair", fail)
    result = recovery["run"](latest_only=True)
    assert result["completed"] == 0 and len(result["errors"]) == 1
    assert json.loads(path.read_text())["pending_horizons"] == ["pentad", "decade"]


def test_latest_cli_uses_one_reference_and_drains_second_horizon_after_failure(monkeypatch):
    import recover_latest_periods as cli

    loads, calls = [], []
    monkeypatch.setattr("sys.argv", ["recover_latest_periods.py", "--forecast-date", "2026-11-17"])
    monkeypatch.setattr(cli.sl, "load_environment", lambda: loads.append(True))
    monkeypatch.setenv("SAPPHIRE_PREDICTION_MODE", "BOTH")
    monkeypatch.setattr(cli, "_read_station_codes", lambda config: ["SYN-ONLY"])

    def consume(config, reference, **kwargs):
        calls.append((config.name, reference, kwargs))
        if config.name == "pentad":
            raise RuntimeError("Synthetic failure")
        return {"completed": 1, "errors": []}

    monkeypatch.setattr(cli, "consume_pending_recoveries", consume)
    assert cli.main() == 1
    assert loads == [True]
    assert calls == [
        (name, dt.date(2026, 11, 17), {"codes": ["SYN-ONLY"], "latest_only": True})
        for name in ("pentad", "decad")
    ]
