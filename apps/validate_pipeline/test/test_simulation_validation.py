"""Simulation checks use generated issue dates and leads, not wall time."""

import json
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import validate_pipeline as vp


@pytest.fixture
def configuration(tmp_path, monkeypatch):
    monkeypatch.setenv("ieasyhydroforecast_configuration_path", str(tmp_path))
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_configuration", "lt")
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", "month_0,month_2,quarter")
    (tmp_path / "lt").mkdir()

    def write(mode, lead=0, day=10, months=None, horizon="month"):
        model = tmp_path / "models" / mode / "family" / "synthetic"
        model.mkdir(parents=True, exist_ok=True)
        (model / "general_config.json").write_text(
            json.dumps({} if months is None else {"forecast_months": months})
        )
        (tmp_path / "lt" / f"{mode}.json").write_text(
            json.dumps(
                {
                    "operational_issue_day": day,
                    "operational_month_lead_time": lead,
                    "horizon_type": horizon,
                    "model_folder": f"models/{mode}",
                    "models_to_use": {"family": ["synthetic"]},
                }
            )
        )

    return write


def frame(issue="2024-01-10", horizon="month", lead=0):
    return pd.DataFrame(
        {"date": [issue], "horizon_type": [horizon], "horizon_value": [lead], "q": [1.0]}
    )


def test_multiple_modes_years_months(configuration):
    configuration("month_0")
    configuration("month_2", lead=2, day=25)
    configuration("quarter", lead=1, months=[1, 4, 7, 10], horizon="quarter")
    assert vp.simulation_expectations(
        ["month_0:2024:2", "month_0:2025:1", "month_2:2024:1", "quarter:2024:2"]
    ) == [
        (date(2024, 1, 10), "month", 0),
        (date(2024, 1, 10), "quarter", 1),
        (date(2024, 1, 25), "month", 2),
        (date(2024, 2, 10), "month", 0),
        (date(2025, 1, 10), "month", 0),
    ]


@pytest.mark.parametrize(
    "spec", ["bogus:2024:1", "month_0:2024:0", "month_0:2024:13", "bad", "month_0:abc:1"]
)
def test_invalid_specification(configuration, spec):
    configuration("month_0")
    with pytest.raises(ValueError):
        vp.simulation_expectations([spec])


def test_empty_schedule(configuration):
    configuration("quarter", months=[4], horizon="quarter")
    with pytest.raises(ValueError, match="no forecast outputs"):
        vp.simulation_expectations(["quarter:2024:1"])


def test_leap_day_no_clamping(configuration):
    configuration("month_0", day=29)
    assert (date(2024, 2, 29), "month", 0) in vp.simulation_expectations(["month_0:2024:2"])
    with pytest.raises(ValueError, match="day is out of range"):
        vp.simulation_expectations(["month_0:2025:2"])


@pytest.mark.parametrize("months", [[], [0], [13], "1,2", [True]])
def test_invalid_member_schedule(configuration, months):
    configuration("month_0", months=months)
    with pytest.raises(ValueError, match="forecast_months"):
        vp.simulation_expectations(["month_0:2024:1"])


@pytest.mark.parametrize(
    "response",
    [
        pd.DataFrame(),
        frame("2026-11-17"),
        frame(lead=1),
        frame(horizon="quarter"),
        pd.DataFrame({"q": [1.0]}),
    ],
)
def test_wrong_or_missing_scope(response):
    client = MagicMock()
    client.read_long_term_forecasts.return_value = response
    client.read_skill_metrics.return_value = pd.DataFrame({"value": [0.5]})
    result = vp.run_tier1_simulation(client, [(date(2024, 1, 10), "month", 0)])[0]
    assert result.status == "FAIL"
    client.read_long_term_forecasts.assert_called_once_with(
        horizon_type="month",
        horizon_value=0,
        start_date="2024-01-10",
        end_date="2024-01-10",
        limit=vp.READ_LIMIT,
    )


def test_api_failure():
    client = MagicMock()
    client.read_long_term_forecasts.side_effect = RuntimeError("unavailable")
    assert vp.run_tier1_simulation(client, [(date(2024, 1, 10), "month", 0)])[0].status == "FAIL"


def test_all_keeps_short_term_current():
    pre, post = MagicMock(), MagicMock()
    pre.readiness_check.return_value = post.readiness_check.return_value = True
    post.read_long_term_forecasts.return_value = frame()
    post.read_skill_metrics.return_value = pd.DataFrame()
    with (
        patch.object(vp, "SapphirePreprocessingClient", return_value=pre),
        patch.object(vp, "SapphirePostprocessingClient", return_value=post),
        patch.object(vp, "run_tier1_short_term", return_value=[]) as short,
        patch.object(vp, "run_tier2_long_term", return_value=[]) as correctness,
    ):
        vp.validate(
            "all", date(2026, 11, 17), ["pentad"], lt_expectations=[(date(2024, 1, 10), "month", 0)]
        )
    assert short.call_args.args[2] == date(2026, 11, 17)
    assert post.read_long_term_forecasts.call_args.kwargs["start_date"] == "2024-01-10"
    assert correctness.call_args.args[0][0].data.equals(frame())


def test_cli_invalid_scope_before_api(configuration):
    configuration("month_0")
    with patch.object(vp, "validate") as validate:
        assert vp.main(["--target", "all", "--lt-simulation", "month_0:2024:0"]) == 1
    validate.assert_not_called()


def test_cli_handoff_and_legacy(configuration):
    configuration("month_0")
    with (
        patch.object(vp, "SAPPHIRE_API_AVAILABLE", True),
        patch.object(vp, "validate", return_value=0) as validate,
    ):
        assert (
            vp.main(
                [
                    "--target",
                    "all",
                    "--forecast-date",
                    "2026-11-17",
                    "--lt-simulation",
                    "month_0:2024:1",
                ]
            )
            == 0
        )
        assert validate.call_args.kwargs["lt_expectations"] == [(date(2024, 1, 10), "month", 0)]
        assert validate.call_args.args[1] == date(2026, 11, 17)
        assert vp.main(["--target", "long-term"]) == 0
        assert "lt_expectations" not in validate.call_args.kwargs


def test_valid_scope_keeps_all_skill_horizons():
    client = MagicMock()
    client.read_long_term_forecasts.return_value = frame()
    client.read_skill_metrics.return_value = pd.DataFrame({"value": [0.5]})
    results = vp.run_tier1_simulation(client, [(date(2024, 1, 10), "month", 0)])
    assert all(result.status == "PASS" for result in results)
    assert [call.kwargs["horizon"] for call in client.read_skill_metrics.call_args_list] == [
        "month",
        "quarter",
        "season",
    ]


def test_quarter_simulation_still_runs_correctness_checks(capsys):
    pre, post = MagicMock(), MagicMock()
    pre.readiness_check.return_value = post.readiness_check.return_value = True
    wrong = frame(horizon="quarter", lead=1)
    wrong["q"] = -1.0
    post.read_long_term_forecasts.return_value = wrong
    post.read_skill_metrics.return_value = pd.DataFrame({"value": [0.5]})
    with (
        patch.object(vp, "SapphirePreprocessingClient", return_value=pre),
        patch.object(vp, "SapphirePostprocessingClient", return_value=post),
    ):
        result = vp.validate(
            "long-term",
            date(2026, 11, 17),
            ["month"],
            lt_expectations=[(date(2024, 1, 10), "quarter", 1)],
        )
    assert result == 1
    assert "negative values" in capsys.readouterr().out


def test_only_producer_configuration_root_is_required(configuration, monkeypatch):
    configuration("month_0")
    monkeypatch.delenv("ieasyforecast_configuration_path", raising=False)
    assert vp.simulation_expectations(["month_0:2024:1"]) == [(date(2024, 1, 10), "month", 0)]


def test_different_read_path_root_does_not_change_simulation_scope(
    configuration, monkeypatch, tmp_path
):
    configuration("month_0")
    alternate = tmp_path / "alternate"
    (alternate / "lt").mkdir(parents=True)
    (alternate / "lt" / "month_0.json").write_text(
        json.dumps(
            {
                "operational_issue_day": 25,
                "operational_month_lead_time": 9,
            }
        )
    )
    monkeypatch.setenv("ieasyforecast_configuration_path", str(alternate))
    assert vp.simulation_expectations(["month_0:2024:1"]) == [(date(2024, 1, 10), "month", 0)]


def test_schedule_numeric_strings_match_forecast_config_getters(configuration):
    configuration("month_2", day="25", lead="2")
    assert vp.simulation_expectations(["month_2:2024:1"]) == [(date(2024, 1, 25), "month", 2)]
