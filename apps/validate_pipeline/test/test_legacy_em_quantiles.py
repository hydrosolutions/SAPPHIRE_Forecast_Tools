"""Retired LT EM quantiles are audited without weakening current products."""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import validate_pipeline as vp


def forecast(model="EM", *, crossed=True, horizon="month"):
    return {
        "model_type": model,
        "horizon_type": horizon,
        "horizon_value": 0,
        "date": "2024-01-10",
        "q": 4.0,
        "q05": 1.0,
        "q10": 2.0,
        "q25": 3.0,
        "q50": 4.0,
        "q75": 8.0 if crossed else 5.0,
        "q90": 7.0,
        "q95": 9.0,
    }


def checks(rows):
    frame = pd.DataFrame(rows)
    before = frame.copy(deep=True)
    results = vp.run_tier2_long_term(
        [vp.CheckResult("Long-term forecasts (month)", "PASS", data=frame)]
    )
    pd.testing.assert_frame_equal(frame, before)
    return {r.name: r for r in results}


@pytest.mark.parametrize("alias", ["EM", "ENSEMBLE_MEAN", " em "])
@pytest.mark.parametrize("horizon", ["month", "quarter", "season"])
def test_legacy_crossing_warns_and_preserves_input(alias, horizon):
    result = checks([forecast(alias, horizon=horizon)])
    assert result["Quantile ordering"].status == "SKIP"
    warning = result["Legacy long-term EM quantile ordering"]
    assert warning.status == "WARN"
    assert "1 disordered quantile pairs" in warning.detail
    assert "historical data unchanged" in warning.detail


def test_mixed_healthy_current_and_crossed_legacy():
    result = checks([forecast(), forecast("Skilled Mean", crossed=False)])
    assert result["Quantile ordering"].status == "PASS"
    assert result["Legacy long-term EM quantile ordering"].status == "WARN"


@pytest.mark.parametrize("current", ["Skilled Mean", "Naive Mean", "MC_ALD", "unknown"])
def test_current_crossing_remains_failure_alongside_legacy(current):
    result = checks([forecast(), forecast(current)])
    assert result["Quantile ordering"].status == "FAIL"
    assert result["Quantile ordering"].detail == "1 disordered quantile pairs"
    assert result["Legacy long-term EM quantile ordering"].status == "WARN"


def test_unknown_model_metadata_remains_strict():
    row = forecast()
    del row["model_type"]
    result = checks([row])
    assert result["Quantile ordering"].status == "FAIL"
    assert "Legacy long-term EM quantile ordering" not in result


def test_other_legacy_errors_still_fail():
    row = forecast()
    row["q"] = -1.0
    result = checks([row])
    assert result["Discharge non-negative"].status == "FAIL"
    assert result["Legacy long-term EM quantile ordering"].status == "WARN"


def test_healthy_legacy_has_no_warning():
    result = checks([forecast(crossed=False), forecast("Skilled Mean", crossed=False)])
    assert result["Quantile ordering"].status == "PASS"
    assert "Legacy long-term EM quantile ordering" not in result


@pytest.mark.parametrize("horizon", ["pentad", "decade"])
def test_short_term_em_crossing_stays_strict(horizon):
    row = forecast(horizon=horizon)
    row["q05"] = 4.0
    result = vp.run_tier2(
        [vp.CheckResult(f"Forecasts (EM, {horizon})", "PASS", data=pd.DataFrame([row]))],
        horizon,
        forecast_date=date(2024, 1, 10),
    )
    assert next(r for r in result if r.name == "Quantile ordering").status == "FAIL"
    assert not any(r.name.startswith("Legacy") for r in result)


@pytest.mark.parametrize("simulation", [True, False])
def test_legacy_only_does_not_prove_active_forecast_presence(simulation):
    client = MagicMock()
    frame = pd.DataFrame([forecast()])
    client.read_long_term_forecasts.return_value = frame
    client.read_skill_metrics.return_value = pd.DataFrame({"nse": [0.5]})
    if simulation:
        results = vp.run_tier1_simulation(client, [(date(2024, 1, 10), "month", 0)])
    else:
        with (
            patch.object(vp, "quarter_horizon_value", return_value=1),
            patch.object(vp, "_resolved_seasonal_presence_horizon_value", return_value=(4, 0)),
        ):
            results = vp.run_tier1_long_term(client, date(2024, 1, 10))
    monthly = next(r for r in results if r.name.startswith("Long-term forecasts (month"))
    assert monthly.status == "FAIL"
    assert "only retired long-term EM" in monthly.detail
    assert monthly.data is frame


@pytest.mark.parametrize("only_legacy,expected_exit", [(False, 0), (True, 1)])
def test_simulation_exit_warns_for_legacy_but_requires_active_output(
    capsys, only_legacy, expected_exit
):
    pre, post = MagicMock(), MagicMock()
    pre.readiness_check.return_value = post.readiness_check.return_value = True
    rows = [forecast()]
    if not only_legacy:
        rows.append(forecast("Skilled Mean", crossed=False))
    post.read_long_term_forecasts.return_value = pd.DataFrame(rows)
    post.read_skill_metrics.return_value = pd.DataFrame({"nse": [0.5]})
    with (
        patch.object(vp, "SapphirePreprocessingClient", return_value=pre),
        patch.object(vp, "SapphirePostprocessingClient", return_value=post),
    ):
        result = vp.validate(
            "long-term",
            date(2026, 11, 18),
            ["month"],
            lt_expectations=[(date(2024, 1, 10), "month", 0)],
        )
    assert result == expected_exit
    assert "Legacy long-term EM quantile ordering" in capsys.readouterr().out
