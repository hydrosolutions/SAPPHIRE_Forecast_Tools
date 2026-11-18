"""Offline guarded execution tests using synthetic identifiers only."""

import copy
import json
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest
import recover_failed_forecasts as recovery


@pytest.fixture
def setup(monkeypatch, tmp_path):
    monkeypatch.setenv("SAPPHIRE_TEST_ENV", "false")
    candidate = dict(
        issue_date="2020-01-10",
        model="TFT",
        code="101",
        n_targets=11,
        replace_keys=[("day", "101", "TFT", "2020-01-10", f"2020-01-{d}") for d in range(11, 22)],
        insert_keys=[],
    )
    frame = pd.DataFrame(
        dict(
            date=pd.date_range("2020-01-11", periods=11),
            forecast_date=pd.Timestamp("2020-01-10"),
            code="101",
            flag=4,
            **{f"Q{q}": float(q) for q in range(5, 100, 5)},
        )
    )
    rows = pd.DataFrame(
        dict(
            target=frame.date.dt.strftime("%Y-%m-%d"),
            date="2020-01-10",
            horizon_type="day",
            code="101",
            model_type="TFT",
            flag=4,
            q05=5.0,
            q25=25.0,
            forecasted_discharge=50.0,
            q75=75.0,
            q95=95.0,
        )
    )
    preview = dict(gate_blocked=None, candidates=[candidate], blocked=[], skipped=[])
    backend = Mock()
    backend.preview.side_effect = lambda *args: copy.deepcopy(preview)
    backend.compute.return_value = frame
    backend.write.return_value = True
    backend.read.return_value = rows
    return frame, rows, preview, backend, tmp_path


def run(setup, **kwargs):
    return recovery.run_recovery(
        ["2020-01-10"],
        ["TFT"],
        ["101"],
        execute=True,
        backend=setup[3],
        audit_dir=setup[4],
        **kwargs,
    )


def test_success(setup):
    result = run(setup)
    assert result["status"] == "complete"
    state = result["results"][0]
    assert state["status"] == "verified"
    assert state["pending_horizons"] == ["pentad", "decade"]
    with open(state["computed_artifact"]) as artifact:
        assert json.load(artifact)["data"]
    setup[3].write.assert_called_once()


def test_preview_never_computes(setup):
    result = recovery.run_recovery(["2020-01-10"], ["TFT"], ["101"], backend=setup[3])
    assert result["status"] == "preview"
    setup[3].compute.assert_not_called()
    setup[3].write.assert_not_called()


@pytest.mark.parametrize("change", ["healthy", "keys", "blocked"])
def test_fresh_eligibility(setup, change):
    fresh = copy.deepcopy(setup[2])
    if change == "healthy":
        fresh["candidates"] = []
    elif change == "keys":
        fresh["candidates"][0]["replace_keys"].pop()
    else:
        fresh["blocked"] = [dict(reason="read failed")]
    setup[3].preview.side_effect = [setup[2], fresh]
    result = run(setup)
    setup[3].write.assert_not_called()
    assert result["results"][0]["status"] == ("protected" if change == "healthy" else "error")


@pytest.mark.parametrize("change", ["missing", "nonfinite", "code", "issue", "duplicate", "flag"])
def test_compute_validation(setup, change):
    frame = setup[0].copy()
    if change == "missing":
        frame = frame.iloc[:-1]
    elif change == "nonfinite":
        frame.loc[0, "Q10"] = np.inf
    elif change == "code":
        frame.loc[0, "code"] = "102"
    elif change == "issue":
        frame.loc[0, "forecast_date"] = pd.Timestamp("2020-01-09")
    elif change == "duplicate":
        frame.loc[0, "date"] = frame.loc[1, "date"]
    else:
        frame.loc[0, "flag"] = 3
    setup[3].compute.return_value = frame
    assert run(setup)["status"] == "error"
    setup[3].write.assert_not_called()


@pytest.mark.parametrize("change", ["partial", "values", "flag", "identity", "falsewriter"])
def test_verified_readback_required(setup, change):
    rows = setup[1].copy()
    if change == "partial":
        rows = rows.iloc[:-1]
    elif change == "values":
        rows.loc[0, "q05"] = 0.0
    elif change == "flag":
        rows.loc[0, "flag"] = 3
    elif change == "identity":
        rows.loc[0, "code"] = "102"
    else:
        setup[3].write.return_value = False
    setup[3].read.return_value = rows
    result = run(setup)
    assert result["status"] == "error"
    assert not result["results"][0]["period_rebuild_pending"]


def test_cooldown(setup):
    setup[3].compute.return_value = None
    result = run(setup)
    assert result["status"] == "complete"
    assert result["counts"]["unrecoverable"] == 1
    assert not result["errors"]
    setup[3].compute.reset_mock()
    assert run(setup, cooldown_seconds=86400)["results"][0]["status"] == "cooldown"
    setup[3].compute.assert_not_called()


def test_timeout(setup):
    ticks = iter([0, 1, 2, 12])
    assert (
        run(setup, timeout_seconds=10, budget_seconds=20, clock=lambda: next(ticks))["status"]
        == "error"
    )
    assert setup[3].compute.call_args.args[1] == 10
    setup[3].write.assert_not_called()


def test_max_units(setup):
    run(setup, max_units=0)
    setup[3].compute.assert_not_called()


def test_testenv_and_optout(setup, monkeypatch):
    monkeypatch.setenv("SAPPHIRE_TEST_ENV", "true")
    assert run(setup)["status"] == "blocked"
    setup[3].preview.assert_not_called()
    assert recovery.run_maintenance_recovery(backend=setup[3])["status"] == "disabled"
    monkeypatch.setenv("SAPPHIRE_TEST_ENV", "false")
    monkeypatch.setenv("ML_FAILED_ISSUE_RECOVERY_ENABLED", "false")
    assert recovery.run_maintenance_recovery(backend=setup[3])["status"] == "disabled"


def test_blocked_not_empty(setup):
    setup[2]["gate_blocked"] = "API disabled"
    assert run(setup)["status"] == "blocked"
    setup[2]["gate_blocked"] = None
    setup[2]["candidates"] = []
    setup[2]["blocked"] = [dict(reason="API unavailable")]
    assert run(setup)["status"] == "error"


def test_restart_reconciles_successful_write_after_failed_readback(setup):
    setup[3].read.side_effect = RuntimeError("transient API read failure")
    assert run(setup)["status"] == "error"
    setup[3].write.reset_mock()
    setup[3].compute.reset_mock()
    setup[3].read.side_effect = None
    setup[3].read.return_value = setup[1]
    setup[2]["candidates"] = []  # The write persisted; healthy rows are now protected.
    result = run(setup)
    assert result["results"][0]["status"] == "verified"
    assert result["results"][0]["period_rebuild_pending"]
    setup[3].write.assert_not_called()
    setup[3].compute.assert_not_called()


def test_maintenance_caps_attempts_and_persists_cursor(setup, monkeypatch):
    from datetime import date

    monkeypatch.setenv("ML_FAILED_ISSUE_RECOVERY_AUDIT_DIR", str(setup[4]))
    monkeypatch.setenv("SAPPHIRE_MODEL_TO_USE", "TFT")
    setup[3].module.get_permitted_station_codes.return_value = {"101", "102"}
    monkeypatch.setattr(
        recovery,
        "configured_latest_units",
        lambda today: [
            ("2020-01-15", "101"),
            ("2020-01-15", "102"),
            ("2020-01-10", "101"),
            ("2020-01-10", "102"),
        ],
    )
    calls = []

    def execute(dates, models, codes, **kwargs):
        calls.append((dates, models, codes, kwargs))
        return dict(status="complete", results=[dict(status="error")], errors=[])

    monkeypatch.setattr(recovery, "run_recovery", execute)
    recovery.run_maintenance_recovery(date(2020, 1, 17), backend=setup[3])
    assert len(calls) == 3
    assert calls[0][0] == ["2020-01-15"]
    assert all(c[3]["timeout_seconds"] == 120 and c[3]["cooldown_seconds"] == 86400 for c in calls)
    cursor = json.loads((setup[4] / "scan-latest-TFT.json").read_text())
    assert cursor == {"next_issue_date": "2020-01-10", "next_code": "102", "next_model": "TFT"}
    calls.clear()
    recovery.run_maintenance_recovery(date(2020, 1, 18), backend=setup[3])
    assert calls[0][2] == ["102"]


def test_unit_lock_prevents_overlapping_writes(setup):
    candidate = setup[2]["candidates"][0]
    lock = setup[4] / (recovery.unit_id(candidate) + ".lock")
    with recovery.unit_lock(lock) as acquired:
        assert acquired
        result = run(setup)
    assert result["results"][0]["status"] == "locked"
    setup[3].compute.assert_not_called()
    setup[3].write.assert_not_called()


def test_restart_does_not_accept_different_healthy_values(setup):
    setup[3].read.side_effect = RuntimeError("temporary failure")
    assert run(setup)["status"] == "error"
    setup[3].read.side_effect = None
    setup[3].read.return_value = setup[1].assign(q05=100.0)
    setup[2]["candidates"] = []
    setup[3].write.reset_mock()
    result = run(setup)
    assert not any(r["status"] == "verified" for r in result["results"])
    state_path = next(p for p in setup[4].glob("*.json") if not p.name.endswith(".computed.json"))
    assert not json.loads(state_path.read_text())["period_rebuild_pending"]
    setup[3].write.assert_not_called()


@pytest.mark.parametrize(
    "reason",
    [
        "SAPPHIRE_API_ENABLED=false; recovery preview requires API access",
        "ieasyhydroforecast_run_ML_models is disabled; recovery preview blocked",
        "requested model(s) ['TFT'] not in configured allowlist ['TiDE']",
        "unsupported model(s): ['ARIMA']; supported: ['TFT']",
    ],
)
def test_maintenance_configuration_gates_preserve_disabled_pipeline(setup, monkeypatch, reason):
    from datetime import date

    monkeypatch.setenv("SAPPHIRE_MODEL_TO_USE", "TFT")
    setup[3].module.get_permitted_station_codes.return_value = {"101"}
    setup[2]["gate_blocked"] = reason
    result = recovery.run_maintenance_recovery(date(2020, 1, 17), backend=setup[3])
    assert result["status"] == "disabled"
    assert result["reason"] == reason
    setup[3].compute.assert_not_called()
    setup[3].write.assert_not_called()


def partial_write_setup(setup):
    """Simulate a per-row API writer interrupted after its first three records."""
    rows = setup[1].copy()
    fields = ["q05", "q25", "forecasted_discharge", "q75", "q95"]
    rows.loc[:, "flag"] = 3
    rows.loc[:, fields] = np.nan
    writes = []

    def writer(frame, model):
        writes.append(frame.copy())
        accepted = frame.iloc[:3] if len(writes) == 1 else frame
        for _, row in accepted.iterrows():
            target = row["date"].strftime("%Y-%m-%d")
            index = rows.index[rows.target == target][0]
            rows.loc[index, "flag"] = 4
            for api, column in zip(fields, ["Q5", "Q25", "Q50", "Q75", "Q95"], strict=True):
                rows.loc[index, api] = row[column]
        return len(writes) != 1

    setup[3].write.side_effect = writer
    setup[3].read.side_effect = lambda candidate: rows.copy()
    assert run(setup)["status"] == "error"
    setup[2]["candidates"] = []
    setup[2]["blocked"] = [
        dict(issue_date="2020-01-10", model="TFT", code="101", reason="mixed flags")
    ]
    return rows, writes


def test_owned_partial_write_restart_completes_only_remaining_keys(setup):
    rows, writes = partial_write_setup(setup)
    healthy_before = rows.iloc[:3].copy(deep=True)
    result = run(setup)
    assert result["status"] == "complete"
    assert not result["errors"]
    assert result["results"][0]["period_rebuild_pending"]
    assert len(writes) == 2
    assert len(writes[1]) == 8
    assert set(writes[0].iloc[:3].date).isdisjoint(set(writes[1].date))
    pd.testing.assert_frame_equal(rows.iloc[:3], healthy_before)
    assert (rows.flag == 4).all()
    assert setup[3].compute.call_count == 1


@pytest.mark.parametrize(
    "change", ["value", "flag0", "duplicate", "unknown", "nonnull_failed", "no_marker"]
)
def test_partial_restart_blocks_unowned_or_changed_rows(setup, change):
    rows, writes = partial_write_setup(setup)
    if change == "value":
        rows.loc[0, "q05"] += 1
    elif change == "flag0":
        rows.loc[0, "flag"] = 0
    elif change == "duplicate":
        rows.loc[1, "target"] = rows.loc[0, "target"]
    elif change == "unknown":
        rows.loc[0, "target"] = "2020-02-01"
    elif change == "nonnull_failed":
        rows.loc[4, "q05"] = 1.0
    else:
        audit = next(p for p in setup[4].glob("*.json") if not p.name.endswith(".computed.json"))
        state = json.loads(audit.read_text())
        del state["write_started"]
        recovery.atomic_json(audit, state)
    result = run(setup)
    assert result["status"] == "error"
    assert len(writes) == 1
    assert not any(r.get("period_rebuild_pending") for r in result["results"])


def test_partial_restart_fresh_read_blocks_newly_changed_healthy_row(setup):
    rows, writes = partial_write_setup(setup)
    reads = 0

    def changing_read(candidate):
        nonlocal reads
        reads += 1
        if reads == 2:
            rows.loc[0, "q05"] += 1
        return rows.copy()

    setup[3].read.side_effect = changing_read
    assert run(setup)["status"] == "error"
    assert len(writes) == 1


def test_partial_resume_obeys_attempt_cap_and_cooldown(setup):
    _, writes = partial_write_setup(setup)
    run(setup, max_units=0)
    assert len(writes) == 1
    result = run(setup, cooldown_seconds=86400)
    assert result["results"][0]["status"] == "cooldown"
    assert len(writes) == 1


@pytest.mark.parametrize(
    "reason",
    [
        "API not ready; cannot preview recovery",
        "API readiness check failed: connection refused",
        "sapphire-api-client not installed; cannot preview recovery",
        "station configuration unavailable; cannot determine permitted stations for recovery",
        "ieasyhydroforecast_available_ML_models not configured; cannot determine models",
    ],
)
def test_maintenance_outages_and_missing_configuration_remain_errors(setup, monkeypatch, reason):
    from datetime import date

    monkeypatch.setenv("SAPPHIRE_MODEL_TO_USE", "TFT")
    setup[3].module.get_permitted_station_codes.return_value = {"101"}
    setup[2]["gate_blocked"] = reason
    result = recovery.run_maintenance_recovery(date(2020, 1, 17), backend=setup[3])
    assert result["status"] == "blocked"
    assert result["reason"] == reason
    setup[3].write.assert_not_called()


def test_partial_restart_handles_missing_approved_keys_and_exact_float_artifact(setup):
    setup[0].loc[:, "Q5"] = 0.12345678912345678
    rows, writes = partial_write_setup(setup)
    setup[3].read.side_effect = (
        lambda candidate: rows.iloc[:-2].copy() if len(writes) == 1 else rows.copy()
    )
    result = run(setup)
    assert result["status"] == "complete"
    assert len(writes[1]) == 8
    assert (rows.q05 == 0.12345678912345678).all()
    assert result["results"][0]["period_rebuild_pending"]


@pytest.mark.parametrize("switch", ["SAPPHIRE_API_ENABLED", "ieasyhydroforecast_run_ML_models"])
def test_explicit_off_precedes_missing_sdk_gate(setup, monkeypatch, switch):
    monkeypatch.setenv(switch, "false")
    setup[2]["gate_blocked"] = "sapphire-api-client not installed; cannot preview recovery"
    result = recovery.run_maintenance_recovery(backend=setup[3])
    assert result["status"] == "disabled"
    setup[3].preview.assert_not_called()


@pytest.mark.parametrize(
    "reference,pentad,decade",
    [
        ("2020-01-17", "2020-01-15", "2020-01-10"),
        ("2020-01-20", "2020-01-20", "2020-01-20"),
        ("2020-03-01", "2020-02-29", "2020-02-29"),
        ("2021-01-01", "2020-12-31", "2020-12-31"),
        ("2021-03-01", "2021-02-28", "2021-02-28"),
    ],
)
def test_latest_boundaries_include_due_today_and_calendar_edges(reference, pentad, decade):
    from datetime import date

    assert recovery.latest_issue_dates(date.fromisoformat(reference)) == {
        "pentad": pentad,
        "decade": decade,
    }


def test_latest_units_respect_each_station_horizon_and_deduplicate(tmp_path, monkeypatch):
    from datetime import date

    monkeypatch.setenv("SAPPHIRE_PREDICTION_MODE", "BOTH")
    monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
    for env, filename, codes in [
        ("ieasyforecast_config_file_station_selection", "p.json", ["SYN-P", "SYN-B"]),
        ("ieasyforecast_config_file_station_selection_decad", "d.json", ["SYN-D", "SYN-B"]),
    ]:
        monkeypatch.setenv(env, filename)
        (tmp_path / filename).write_text(json.dumps({"stationsID": codes}))
    assert set(recovery.configured_latest_units(date(2020, 1, 17))) == {
        ("2020-01-15", "SYN-P"),
        ("2020-01-15", "SYN-B"),
        ("2020-01-10", "SYN-D"),
        ("2020-01-10", "SYN-B"),
    }
    assert len(recovery.configured_latest_units(date(2020, 1, 20))) == 3


def test_latest_healthy_never_resumes_old_failed_audit(setup, monkeypatch):
    from datetime import date

    run(setup)  # Durable old verified state, then simulate an interrupted old write.
    candidate = setup[2]["candidates"][0]
    path = setup[4] / (recovery.unit_id(candidate) + ".json")
    state = json.loads(path.read_text())
    state.update(status="error", write_started=True)
    recovery.atomic_json(path, state)
    before = path.read_bytes()
    monkeypatch.setenv("SAPPHIRE_MODEL_TO_USE", "TFT")
    monkeypatch.setattr(recovery, "configured_latest_units", lambda d: [("2020-01-15", "101")])
    setup[3].module.get_permitted_station_codes.return_value = {"101"}
    setup[2]["candidates"] = []
    setup[3].write.reset_mock()
    setup[3].compute.reset_mock()
    result = recovery.run_maintenance_recovery(
        date(2020, 1, 17), backend=setup[3], audit_dir=setup[4]
    )
    assert result["status"] == "empty"
    assert path.read_bytes() == before
    setup[3].write.assert_not_called()
    setup[3].compute.assert_not_called()


def test_absent_candidate_protects_new_healthy_rows_before_write(setup):
    candidate = setup[2]["candidates"][0]
    candidate["insert_keys"] = candidate.pop("replace_keys")
    candidate["replace_keys"] = []
    setup[3].preview.side_effect = [
        copy.deepcopy(setup[2]),
        dict(gate_blocked=None, candidates=[], blocked=[], skipped=[{"reason": "healthy"}]),
    ]
    result = run(setup)
    assert result["results"][0]["status"] == "protected"
    setup[3].write.assert_not_called()


def test_latest_compute_failure_is_visible_and_does_not_write(setup, monkeypatch):
    from datetime import date

    monkeypatch.setenv("SAPPHIRE_MODEL_TO_USE", "TFT")
    monkeypatch.setattr(recovery, "configured_latest_units", lambda d: [("2020-01-10", "101")])
    setup[3].module.get_permitted_station_codes.return_value = {"101"}
    setup[3].compute.return_value = None
    result = recovery.run_maintenance_recovery(
        date(2020, 1, 10), backend=setup[3], audit_dir=setup[4]
    )
    assert result["status"] == "error"
    assert result["counts"]["unrecoverable"] == 1
    setup[3].write.assert_not_called()


def test_absent_candidate_writes_and_verifies_all_eleven_targets(setup):
    candidate = setup[2]["candidates"][0]
    candidate["insert_keys"] = candidate["replace_keys"]
    candidate["replace_keys"] = []
    result = run(setup)
    assert result["counts"]["verified"] == 1
    assert len(setup[3].write.call_args.args[0]) == 11


def test_latest_absent_opt_in_does_not_change_reused_manual_backend(setup, monkeypatch):
    from datetime import date

    module = Mock()
    module.get_permitted_station_codes.return_value = {"101"}
    module.preview_recovery_candidates.return_value = {
        "gate_blocked": None,
        "candidates": [],
        "blocked": [],
        "skipped": [],
    }
    backend = recovery.Backend(module)
    monkeypatch.setattr(recovery, "configured_latest_units", lambda d: [("2020-01-10", "101")])
    recovery.run_maintenance_recovery(
        date(2020, 1, 10), models=["TFT"], execute=False, backend=backend
    )
    backend.preview(["2020-01-10"], ["TFT"], ["101"])
    assert module.preview_recovery_candidates.call_args.kwargs == {}
