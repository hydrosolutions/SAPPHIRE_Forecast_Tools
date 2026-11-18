"""Latest LT handoff tests with synthetic station identities and values only."""

import copy
import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest
from long_term_recovery_io import destination_hash, identity, key_tuple
from src.recovered_long_term import consume_handoff


class Client:
    def __init__(self, rows):
        self.rows = {key_tuple(r): copy.deepcopy(r) for r in rows}
        self.writes = []
        self.fail = False
        self.corrupt = False
        self.fail_model = None

    def read_long_term_forecasts(self, **query):
        if query["model"] == self.fail_model:
            raise RuntimeError("synthetic read failure")
        rows = [
            r
            for r in self.rows.values()
            if all(
                r[k] == query[q]
                for k, q in [
                    ("code", "code"),
                    ("model_type", "model"),
                    ("date", "start_date"),
                    ("horizon_type", "horizon_type"),
                    ("horizon_value", "horizon_value"),
                ]
            )
        ]
        return pd.DataFrame(copy.deepcopy(rows[query["skip"] : query["skip"] + query["limit"]]))

    def write_long_forecasts(self, rows):
        if self.fail:
            raise RuntimeError("synthetic write failure")
        self.writes.extend(copy.deepcopy(rows))
        for row in rows:
            stored = copy.deepcopy(row)
            if self.corrupt:
                stored["q"] += 1
            self.rows[key_tuple(row)] = stored
        return len(rows)


def row(
    model="GBT",
    flag=0,
    horizon="month",
    lead=0,
    issue="2026-11-10",
    start="2026-11-01",
    end="2026-11-30",
    q=10.0,
):
    model = {"LR_BASE": "LR_Base", "NAIVE_MEAN": "Naive Mean", "SKILLED_MEAN": "Skilled Mean"}.get(
        model, model
    )
    return dict(
        horizon_type=horizon,
        horizon_value=lead,
        code="90001",
        date=issue,
        model_type=model,
        valid_from=start,
        valid_to=end,
        flag=flag,
        q=q,
        q05=2.0,
        q10=3.0,
        q25=4.0,
        q50=q,
        q75=16.0,
        q90=17.0,
        q95=18.0,
    )


def handoff(tmp_path, rows, *, reference="2026-11-18", scope=None):
    base = identity(rows[0]) if scope is None else scope
    scope = {k: v for k, v in base.items() if k != "model_type"}
    scope.update(
        sources=[identity(r) for r in rows], triggers=[identity(r) for r in rows if r["flag"] == 1]
    )
    path = tmp_path / "handoff.json"
    path.write_text(
        json.dumps(
            dict(
                schema_version=1,
                destination_hash=destination_hash(),
                forecast_date=reference,
                scopes=[scope],
            )
        )
    )
    return path


def skills(client, scope):
    frame = pd.DataFrame(
        [
            dict(
                code="90001",
                model_short=model,
                horizon_value=scope["horizon_value"],
                month_in_year=11,
                quarter_in_year=4,
                season_in_year=scope["horizon_value"],
                n_pairs=100,
                nse=0.9,
                sdivsigma=0.5,
                accuracy=90,
                mae=1.0,
                delta=1.0,
            )
            for model in ["GBT", "LR_Base", "LR_SM"]
        ]
    )
    return frame


def consume(path, client, reference="2026-11-18"):
    return consume_handoff(path, reference, client, skills_reader=skills)


def test_rebuild_preserves_raw_rows_and_observation_metadata(tmp_path):
    raw = [row(flag=1), row("LR_BASE", q=14.0)]
    aggregate = dict(row("NAIVE_MEAN"), q_obs=7.0, composition="GBT,LR_BASE")
    client = Client([*raw, aggregate])
    before = {key_tuple(r): copy.deepcopy(r) for r in raw}
    result = consume(handoff(tmp_path, raw), client)
    assert not result["errors"] and result["written"] == 2
    assert {r["model_type"] for r in client.writes} == {"Naive Mean", "Skilled Mean"}
    assert all(r["flag"] == 1 and r["date"] == "2026-11-10" for r in client.writes)
    assert client.rows[key_tuple(aggregate)]["q_obs"] == 7.0
    assert all(client.rows[k] == v for k, v in before.items())
    again = consume(handoff(tmp_path, raw), client)
    assert not again["errors"] and again["written"] == 0


def test_pp_failure_retries_from_same_api_recovery_trigger(tmp_path):
    raw = [row(flag=1), row("LR_BASE", q=14.0)]
    client = Client(raw)
    client.fail = True
    path = handoff(tmp_path, raw)
    assert consume(path, client)["errors"]
    client.fail = False
    result = consume(handoff(tmp_path, raw, reference="2026-11-19"), client, "2026-11-19")
    assert not result["errors"] and result["written"] == 2


def test_flag_zero_only_cannot_authorize_rebuild(tmp_path):
    raw = [row(), row("LR_BASE")]
    client = Client(raw)
    assert consume(handoff(tmp_path, raw), client)["errors"]
    assert not client.writes


@pytest.mark.parametrize("change", ["destination", "reference", "window", "trigger"])
def test_wrong_authorization_is_rejected(tmp_path, change):
    raw = [row(flag=1), row("LR_BASE")]
    client = Client(raw)
    path = handoff(tmp_path, raw)
    doc = json.loads(path.read_text())
    if change == "destination":
        doc["destination_hash"] = "wrong"
    elif change == "reference":
        doc["forecast_date"] = "2026-11-17"
    elif change == "window":
        doc["scopes"][0]["date"] = "2026-09-10"
    else:
        client.rows[key_tuple(raw[0])]["flag"] = 0
    path.write_text(json.dumps(doc))
    if change in {"destination", "reference"}:
        with pytest.raises(ValueError):
            consume(path, client)
    else:
        assert consume(path, client)["errors"]
    assert not client.writes


def test_read_uncertainty_never_builds_partial_membership(tmp_path):
    raw = [row(flag=1), row("LR_BASE")]
    client = Client(raw)
    client.fail_model = "LR_Base"
    assert consume(handoff(tmp_path, raw), client)["errors"]
    assert not client.writes


def test_full_derived_readback_required(tmp_path):
    raw = [row(flag=1), row("LR_BASE")]
    client = Client(raw)
    client.corrupt = True
    assert consume(handoff(tmp_path, raw), client)["errors"]


def test_existing_membership_cannot_shrink(tmp_path):
    raw = [row(flag=1), row("LR_BASE")]
    old = dict(row("NAIVE_MEAN"), composition="GBT,LR_BASE,LR_SM")
    client = Client([*raw, old])
    result = consume(handoff(tmp_path, raw), client)
    assert result["errors"]
    assert client.rows[key_tuple(old)] == old


def quarter_rows():
    issue = "2026-09-25"
    native = row(
        "LR_BASE",
        horizon="quarter",
        lead=1,
        issue=issue,
        start="2026-10-01",
        end="2026-12-31",
        q=16.0,
    )
    monthly = [
        row(
            "GBT",
            flag=1 if lead == 1 else 0,
            lead=lead,
            issue=issue,
            start=start,
            end=end,
            q=10.0 + lead,
        )
        for lead, start, end in [
            (1, "2026-10-01", "2026-10-31"),
            (2, "2026-11-01", "2026-11-30"),
            (3, "2026-12-01", "2026-12-31"),
        ]
    ]
    return native, monthly


@pytest.mark.parametrize("flag", ["true", "false"])
def test_quarter_triplet_preserves_issue_and_healthy_native_collision(tmp_path, monkeypatch, flag):
    monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", flag)
    native, monthly = quarter_rows()
    collision = dict(native, model_type="GBT", q=99.0)
    client = Client([native, *monthly, collision])
    path = handoff(tmp_path, [native, *monthly], reference="2026-10-18", scope=identity(native))
    result = consume(path, client, "2026-10-18")
    assert not result["errors"]
    assert client.rows[key_tuple(collision)] == collision
    assert client.rows[key_tuple(native)] == native
    assert client.writes and all(
        r["date"] == "2026-09-25" and r["horizon_value"] == 1 for r in client.writes
    )
    assert all(r["model_type"] in {"Naive Mean", "Skilled Mean"} for r in client.writes)


def test_discarded_direct_quarter_trigger_does_not_authorize_monthly_rebuild(tmp_path):
    native, monthly = quarter_rows()
    for r in monthly:
        r["flag"] = 0
    ignored = dict(native, model_type="GBT", flag=1)
    raw = [native, ignored, *monthly]
    client = Client(raw)
    result = consume(
        handoff(tmp_path, raw, reference="2026-10-18", scope=identity(native)), client, "2026-10-18"
    )
    assert not result["errors"] and result["blocked"] == 1 and not client.writes


def test_native_lr_precedes_monthly_lr_fallback(tmp_path):
    native, monthly = quarter_rows()
    for r in monthly:
        r["model_type"] = "LR_Base"
    other = dict(native, model_type="LR_SM", q=20.0)
    raw = [native, other, *monthly]
    client = Client(raw)
    result = consume(
        handoff(tmp_path, raw, reference="2026-10-18", scope=identity(native)), client, "2026-10-18"
    )
    assert not result["errors"] and result["blocked"] == 1 and not client.writes


def test_consumer_import_needs_no_lt_model_packages():
    root = Path(__file__).resolve().parents[1]
    code = """import sys
class Block:
 def find_spec(self, fullname, path=None, target=None):
  if fullname.split('.')[0] in {'lt_forecasting','lt_recovery','run_forecast','config_forecast'}:
   raise RuntimeError('LT model import attempted')
sys.meta_path.insert(0,Block())
import src.recovered_long_term
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=root,
        env=dict(os.environ, SAPPHIRE_TEST_ENV="True"),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_recovered_member_excluded_from_skilled_pool_cannot_reflag_it(tmp_path):
    raw = [row(flag=1), row("LR_BASE"), row("LR_SM")]
    old = dict(row("SKILLED_MEAN"), composition="LR_Base,LR_SM")
    client = Client([*raw, old])

    def selected_skills(client, scope):
        frame = skills(client, scope)
        frame.loc[frame.model_short == "GBT", "nse"] = -1.0
        return frame

    result = consume_handoff(
        handoff(tmp_path, raw), "2026-11-18", client, skills_reader=selected_skills
    )
    assert not result["errors"]
    assert client.rows[key_tuple(old)] == old
    assert {r["model_type"] for r in client.writes} == {"Naive Mean"}


def test_season_uses_existing_helper_and_keeps_raw_records(tmp_path):
    raw = [
        row(
            "LR_BASE",
            flag=1,
            horizon="season",
            lead=0,
            issue="2026-04-25",
            start="2026-04-01",
            end="2026-09-30",
        ),
        row(
            "LR_SM",
            horizon="season",
            lead=0,
            issue="2026-04-25",
            start="2026-04-01",
            end="2026-09-30",
            q=14.0,
        ),
    ]
    client = Client(raw)
    result = consume(handoff(tmp_path, raw, reference="2026-05-18"), client, "2026-05-18")
    assert not result["errors"] and result["written"] == 2
    assert all(client.rows[key_tuple(r)] == r for r in raw)
    assert all(r["date"] == "2026-04-25" and r["valid_to"] == "2026-09-30" for r in client.writes)


def test_different_issues_with_same_target_never_mix(tmp_path):
    first = [row(flag=1), row("LR_BASE")]
    second = [dict(r, date="2026-11-15", q=100.0, q50=100.0) for r in first]
    path = handoff(tmp_path, first)
    document = json.loads(path.read_text())
    path = handoff(tmp_path, second)
    document["scopes"].extend(json.loads(path.read_text())["scopes"])
    path.write_text(json.dumps(document))
    client = Client(first + second)
    result = consume(path, client)
    assert not result["errors"]
    assert {r["q"] for r in client.writes if r["date"] == "2026-11-10"} == {10.0}
    assert {r["q"] for r in client.writes if r["date"] == "2026-11-15"} == {100.0}


@pytest.mark.parametrize("lead", [0, 2])
@pytest.mark.parametrize("flag", ["false", "true"])
def test_season_selects_its_lead_skill_pool(tmp_path, monkeypatch, lead, flag):
    monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", flag)
    raw = [
        row(
            "LR_BASE",
            flag=1,
            horizon="season",
            lead=lead,
            issue="2026-04-25",
            start="2026-04-01",
            end="2026-09-30",
        ),
        row(
            "LR_SM",
            horizon="season",
            lead=lead,
            issue="2026-04-25",
            start="2026-04-01",
            end="2026-09-30",
            q=14.0,
        ),
    ]
    # API wire periods encode seasonal leads, including when lead-aware is OFF.
    # The lead-1 decoy would produce a different weighted forecast.
    maes = {0: (1.0, 3.0), 1: (1.0, 9.0), 2: (3.0, 1.0)}
    wire_skills = pd.DataFrame(
        [
            dict(
                code="90001",
                model_type=model,
                horizon_in_year=period,
                horizon_value=period,
                n_pairs=100,
                nse=0.9,
                sdivsigma=0.5,
                accuracy=90,
                mae=mae,
                delta=1.0,
            )
            for period, values in maes.items()
            for model, mae in zip(("LR_Base", "LR_SM"), values, strict=True)
        ]
    )

    class SkillClient(Client):
        def read_skill_metrics(self, *, horizon, code, skip, limit):
            assert horizon == "season" and code == "90001"
            return wire_skills.iloc[skip : skip + limit].copy()

    client = SkillClient(raw)
    result = consume_handoff(handoff(tmp_path, raw, reference="2026-05-18"), "2026-05-18", client)
    assert not result["errors"] and result["written"] == 2
    outputs = {r["model_type"]: r for r in client.writes}
    expected = {0: 11.014778325123153, 2: 12.985221674876849}
    assert outputs["Skilled Mean"]["q"] == pytest.approx(expected[lead])
    assert outputs["Naive Mean"]["q"] == pytest.approx(12.0)
    assert all(r["horizon_value"] == lead for r in client.writes)
    assert all(client.rows[key_tuple(r)] == r for r in raw)


def test_season_normalization_uses_configured_cross_year_reference(monkeypatch):
    from src.recovered_long_term import _normalized

    monkeypatch.setenv("SAPPHIRE_SEASON_START_MONTH", "10")
    monkeypatch.setenv("SAPPHIRE_SEASON_END_MONTH", "3")
    raw = row(
        "LR_BASE",
        horizon="season",
        lead=2,
        issue="2026-12-25",
        start="2027-02-01",
        end="2027-03-31",
    )
    normalized = _normalized([raw], identity(raw))
    assert normalized["season_year"].tolist() == [2026]
    assert normalized["season_in_year"].tolist() == [2]
    assert normalized["date"].tolist() == [pd.Timestamp("2026-12-25")]
    assert normalized["valid_from"].tolist() == [pd.Timestamp("2027-02-01")]


def test_recovery_cli_starts_in_fresh_application_process():
    app_dir = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["SAPPHIRE_TEST_ENV"] = "True"
    env.pop("ieasyhydroforecast_env_file_path", None)
    result = subprocess.run(
        [sys.executable, "recover_latest_long_term.py", "--help"],
        cwd=app_dir,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "--forecast-date" in result.stdout
    assert "--handoff" in result.stdout


@pytest.mark.parametrize("horizon", ["month", "quarter", "season"])
@pytest.mark.parametrize("mixed_quantile", [False, True])
def test_nullable_api_quantiles_use_numeric_ensemble_inputs(tmp_path, horizon, mixed_quantile):
    from long_term_recovery_io import FORECAST_FIELDS
    from src import ensemble_calculator
    from src.recovered_long_term import _members, _skills

    raw = [row("LR_BASE", flag=1, horizon=horizon), row("LR_SM", q=14.0, horizon=horizon)]
    for record in raw:
        record.update({field: None for field in FORECAST_FIELDS if field not in {"q", "q50"}})
    if mixed_quantile:
        raw[1]["q10"] = 3.0
    client = Client(raw)
    path = handoff(tmp_path, raw)
    scope = json.loads(path.read_text())["scopes"][0]
    wire_skills = skills(client, scope).rename(columns={"model_short": "model_type"})
    wire_skills["horizon_in_year"] = {"month": 11, "quarter": 4, "season": 0}[horizon]
    wire_skills["crps"] = None
    wire_skills = wire_skills.drop(columns=["month_in_year", "quarter_in_year", "season_in_year"])
    client.read_skill_metrics = lambda **query: wire_skills.copy()
    before = copy.deepcopy(client.rows)
    frame, _, _ = _members(client, scope)
    helper = {
        "month": ensemble_calculator.create_monthly_ensemble_forecasts,
        "quarter": ensemble_calculator.create_quarterly_ensemble_forecasts,
        "season": ensemble_calculator.create_seasonal_ensemble_forecasts,
    }[horizon]
    # Exercise the real helper directly so a nullable-object arithmetic failure
    # retains its traceback rather than becoming a consumer summary error.
    actual = helper(frame, _skills(client, scope))
    expected_frame = frame.copy()
    expected_frame[list(FORECAST_FIELDS)] = expected_frame[list(FORECAST_FIELDS)].astype(float)
    expected = helper(expected_frame, _skills(client, scope))
    pd.testing.assert_frame_equal(actual, expected)
    result = consume_handoff(path, "2026-11-18", client)
    assert result == dict(written=2, unchanged=0, blocked=0, errors=[])
    for output in client.writes:
        assert output["q"] == pytest.approx(12.0)
        assert output["q05"] is None
        assert output["q10"] == (
            3.0
            if mixed_quantile and output["model_type"] == "Naive Mean" and horizon != "quarter"
            else None
        )
        assert output["flag"] == 1
        assert output["date"] == "2026-11-10"
        assert client.rows[key_tuple(output)] == output
    assert all(client.rows[key] == value for key, value in before.items())
    assert consume_handoff(path, "2026-11-18", client) == dict(
        written=0, unchanged=2, blocked=0, errors=[]
    )
