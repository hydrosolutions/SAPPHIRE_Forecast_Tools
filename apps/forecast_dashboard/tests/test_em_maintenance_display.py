"""Tests for EM row preservation through get_forecasts_all and update_model_dict_date.

Contract:
  - An EM row stored with a finite point forecast (15.0) and four bands
    (Q5=18, Q25=19, Q75=21, Q95=22) passes through db.get_forecasts_all
    unchanged for both pentad and decade horizons.
  - db.get_data -> processing.update_model_dict_date includes "EM" in the
    returned model selector dict when the selected issue date matches the
    stored issue date.
  - Both contracts hold with no corresponding skill row for the EM model
    (get_data retains EM rows when no skill rows are present; the EM row
    remains in forecasts_all and appears in the model selector).

The stored EM values (e.g. q05=18/q25=19/q75=21/q95=22) are arbitrary display
payloads; they are not derived by the PP-072 rule.
These values are fixed in the synthetic
payloads; the tests prove the pipeline transmits them without modification,
not that any calculation produces them.
"""

from unittest.mock import MagicMock

import pandas as pd
import pytest
import requests
from src import db, processing

# ---------------------------------------------------------------------------
# Shared synthetic data
# ---------------------------------------------------------------------------

_ISSUE_DATE = "2026-02-28"
_STATION_CODE = "TEST001"
_STATION_LABEL = "Test River"
_STATION_LABEL_FULL = f"{_STATION_CODE} - {_STATION_LABEL}"

# horizon_in_year values: arbitrary but internally consistent for test purposes
_HORIZON_IN_YEAR = {"pentad": 13, "decade": 7}


def _all_stations():
    return pd.DataFrame({"code": [_STATION_CODE], "station_labels": [_STATION_LABEL]})


def _ml_record(horizon, model_type, model_description, q, q05, q25, q75, q95, rid):
    return {
        "id": rid,
        "horizon_type": horizon,
        "horizon_value": 1,
        "horizon_in_year": _HORIZON_IN_YEAR[horizon],
        "code": _STATION_CODE,
        "model_type": model_type,
        "model_type_description": model_description,
        "date": _ISSUE_DATE,
        "target": "2026-03-01",
        "forecasted_discharge": q,
        "q05": q05,
        "q25": q25,
        "q75": q75,
        "q95": q95,
        "flag": 0,
        "composition": None,
    }


def _lr_record(horizon):
    return {
        "id": 10,
        "horizon_type": horizon,
        "horizon_value": 1,
        "horizon_in_year": _HORIZON_IN_YEAR[horizon],
        "code": _STATION_CODE,
        "date": _ISSUE_DATE,
        "discharge_avg": 9.0,
        "predictor": 8.5,
        "slope": 1.1,
        "intercept": 0.5,
        "forecasted_discharge": 10.0,
        "q_mean": 9.3,
        "q_std_sigma": 1.5,
        "delta": 1.0,
        "rsquared": 0.85,
    }


def _fake_response(json_data):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


def _make_requests_mock(horizon):
    """Return a requests.get stand-in that dispatches by URL and params.

    - postprocessing/forecast/ with horizon=pentad or decade → EM + GBT records
    - postprocessing/forecast/ with horizon=day (get_ml_forecast) → []
    - postprocessing/lr-forecast/ → LR record
    - postprocessing/skill-metric/ → [] (no skill rows for EM)
    - preprocessing/* → [] (hydrograph, meteo, snow)
    """
    em = _ml_record(horizon, "EM", "Ensemble Mean (EM)", 15.0, 18.0, 19.0, 21.0, 22.0, rid=1)
    gbt = _ml_record(
        horizon, "GBT", "Gradient Boosted Trees (GBT)", 20.0, 18.0, 19.0, 21.0, 22.0, rid=2
    )
    lr = _lr_record(horizon)

    def _get(url, **kwargs):
        params = kwargs.get("params", {})
        if "/postprocessing/forecast/" in url:
            if params.get("horizon") == horizon:
                return _fake_response([em, gbt])
            return _fake_response([])
        if "/postprocessing/lr-forecast/" in url:
            return _fake_response([lr])
        # skill-metric and all preprocessing endpoints
        return _fake_response([])

    return _get


# ---------------------------------------------------------------------------
# Tests: get_forecasts_all preserves EM row unchanged
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("horizon", ["pentad", "decade"])
class TestEmRowPreservedByGetForecastsAll:
    """Real db.get_forecasts_all must pass the stored EM point and bands through."""

    def test_em_row_present(self, horizon, monkeypatch):
        monkeypatch.setattr(requests, "get", _make_requests_mock(horizon))
        result = db.get_forecasts_all(horizon, _STATION_CODE)
        em_rows = result[result["model_short"] == "EM"]
        assert len(em_rows) == 1, (
            f"[{horizon}] Expected exactly one EM row; got {len(em_rows)}. "
            f"model_short values: {list(result['model_short'].unique())}"
        )

    def test_em_finite_point_forecast_retained(self, horizon, monkeypatch):
        monkeypatch.setattr(requests, "get", _make_requests_mock(horizon))
        result = db.get_forecasts_all(horizon, _STATION_CODE)
        em_q = result.loc[result["model_short"] == "EM", "forecasted_discharge"].iloc[0]
        assert em_q == pytest.approx(15.0), (
            f"[{horizon}] EM point forecast mutated: expected 15.0, got {em_q}"
        )

    def test_em_q5_band_retained(self, horizon, monkeypatch):
        monkeypatch.setattr(requests, "get", _make_requests_mock(horizon))
        result = db.get_forecasts_all(horizon, _STATION_CODE)
        val = result.loc[result["model_short"] == "EM", "Q5"].iloc[0]
        assert val == pytest.approx(18.0), f"[{horizon}] Q5 mutated: {val}"

    def test_em_q25_band_retained(self, horizon, monkeypatch):
        monkeypatch.setattr(requests, "get", _make_requests_mock(horizon))
        result = db.get_forecasts_all(horizon, _STATION_CODE)
        val = result.loc[result["model_short"] == "EM", "Q25"].iloc[0]
        assert val == pytest.approx(19.0), f"[{horizon}] Q25 mutated: {val}"

    def test_em_q75_band_retained(self, horizon, monkeypatch):
        monkeypatch.setattr(requests, "get", _make_requests_mock(horizon))
        result = db.get_forecasts_all(horizon, _STATION_CODE)
        val = result.loc[result["model_short"] == "EM", "Q75"].iloc[0]
        assert val == pytest.approx(21.0), f"[{horizon}] Q75 mutated: {val}"

    def test_em_q95_band_retained(self, horizon, monkeypatch):
        monkeypatch.setattr(requests, "get", _make_requests_mock(horizon))
        result = db.get_forecasts_all(horizon, _STATION_CODE)
        val = result.loc[result["model_short"] == "EM", "Q95"].iloc[0]
        assert val == pytest.approx(22.0), f"[{horizon}] Q95 mutated: {val}"


# ---------------------------------------------------------------------------
# Tests: get_data + update_model_dict_date selector availability
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("horizon", ["pentad", "decade"])
class TestEmModelSelectorAvailability:
    """EM appears in the model selector dict returned by update_model_dict_date
    when the selected issue date matches; absent for a non-matching date.

    Exercises the full path: db.get_data (real) -> processing.update_model_dict_date
    (real), with no skill row stored for EM (get_data retains the EM row when no
    skill rows are present).
    """

    def test_em_in_model_dict_on_matching_issue_date(self, horizon, monkeypatch):
        monkeypatch.setattr(requests, "get", _make_requests_mock(horizon))
        data = db.get_data(horizon, _STATION_CODE, _all_stations())
        forecasts_all = data["forecasts_all"]
        model_dict = processing.update_model_dict_date(
            {}, forecasts_all, _STATION_LABEL_FULL, _ISSUE_DATE
        )
        assert "EM" in model_dict.values(), (
            f"[{horizon}] Expected 'EM' in model selector on matching date "
            f"{_ISSUE_DATE!r}; got {model_dict}"
        )

    def test_em_not_in_model_dict_on_nonmatching_date(self, horizon, monkeypatch):
        monkeypatch.setattr(requests, "get", _make_requests_mock(horizon))
        data = db.get_data(horizon, _STATION_CODE, _all_stations())
        forecasts_all = data["forecasts_all"]
        model_dict = processing.update_model_dict_date(
            {}, forecasts_all, _STATION_LABEL_FULL, "2025-01-01"
        )
        assert "EM" not in model_dict.values(), (
            f"[{horizon}] Expected no 'EM' for a date with no stored forecasts; got {model_dict}"
        )
