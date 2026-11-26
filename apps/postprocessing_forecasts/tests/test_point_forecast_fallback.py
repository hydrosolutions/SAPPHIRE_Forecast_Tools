"""Raw long-term points use q first without replacing quantiles."""

import datetime as dt
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from src import data_reader
from src.ensemble_calculator import create_monthly_ensemble_forecasts
from src.point_forecast import _resolve_forecasted_discharge, fill_forecasted_discharge
from test_monthly_ensemble_creation import THRESHOLD_ENV, _two_model_forecasts, _two_model_skill


@pytest.mark.parametrize(
    "raw, expected",
    [
        ({"q": [0, 12]}, [0, 12]),
        ({"q50": [0, 14]}, [0, 14]),
        ({"q": [0, None, 12], "q50": [99, 14, 98]}, [0, 14, 12]),
        ({"q": [12, 13], "q50": [98, 99], "forecasted_discharge": [44, None]}, [44, 13]),
    ],
)
def test_fill_points(raw, expected):
    frame = pd.DataFrame(raw)
    original = frame.copy(deep=True)
    result = fill_forecasted_discharge(frame)
    assert result.forecasted_discharge.tolist() == expected
    pd.testing.assert_frame_equal(frame, original)
    for col in ("q", "q50"):
        if col in frame:
            pd.testing.assert_series_equal(result[col], frame[col])


def test_no_raw_columns():
    frame = pd.DataFrame({"code": ["SYNTHETIC_A"], "forecasted_discharge": [np.nan]})
    assert _resolve_forecasted_discharge(frame) is None
    pd.testing.assert_frame_equal(fill_forecasted_discharge(frame), frame)


def test_historical_semantics():
    from src.skill_metrics import _resolve_forecasted_discharge as historical

    assert historical is _resolve_forecasted_discharge
    with pytest.raises(ValueError, match="could not convert string to float"):
        historical(pd.DataFrame({"q": ["bad"], "q50": [12]}))
    assert historical(pd.DataFrame({"q": [np.inf], "q50": [12]})).iloc[0] == np.inf


def test_latest_monthly_reader():
    raw = pd.DataFrame(
        {
            "code": ["SYNTHETIC_A"] * 3,
            "model_type": ["LR_Base", "GBT", "MC_ALD"],
            "valid_from": ["2025-03-01"] * 3,
            "valid_to": ["2025-03-31"] * 3,
            "q": [0, None, 30],
            "q50": [99, 20, 98],
            "forecasted_discharge": [None, None, 44],
        }
    )
    with (
        patch.object(data_reader, "_read_long_forecasts_api", return_value=raw),
        patch.object(data_reader, "skill_lead_aware_enabled", return_value=False),
    ):
        result = data_reader.read_latest_monthly_forecasts(["SYNTHETIC_A"], dt.date(2025, 3, 1))
    assert result.forecasted_discharge.tolist() == [0, 20, 44]
    assert result.q50.tolist() == [99, 20, 98]


@pytest.mark.parametrize("horizon_type", ["quarter", "season"])
def test_aggregated_reader(horizon_type):
    raw = pd.DataFrame(
        {
            "code": ["SYNTHETIC_A"] * 2,
            "model_type": ["LR", "TFT"],
            "valid_from": ["2025-04-01"] * 2,
            "valid_to": ["2025-06-30"] * 2,
            "q": [0, None],
            "q50": [99, 20],
            "horizon_value": [1, 1],
            "forecasted_discharge": [None, None],
        }
    )
    result = data_reader._normalize_combined_forecasts(raw, horizon_type)
    assert result.forecasted_discharge.tolist() == [0, 20]
    assert result.q50.tolist() == [99, 20]


@pytest.mark.parametrize("q50_null", [False, True])
def test_multimodel_ensemble_preserves_quantiles(q50_null):
    forecasts = _two_model_forecasts()
    forecasts["code"] = "SYNTHETIC_A"
    forecasts["q"] = [100, 200]
    if q50_null:
        forecasts["q50"] = np.nan
    forecasts["forecasted_discharge"] = np.nan
    skill = _two_model_skill()
    skill["code"] = "SYNTHETIC_A"
    quantiles = forecasts.filter(regex="^q[0-9]").copy()
    with patch.dict("os.environ", THRESHOLD_ENV):
        result = create_monthly_ensemble_forecasts(forecasts, skill)
    naive = result[result.model_short == "Naive Mean"]
    assert len(naive) == 1
    assert naive.forecasted_discharge.iloc[0] == 150
    if q50_null:
        assert pd.isna(naive.q50.iloc[0])
    else:
        assert naive.q50.iloc[0] == quantiles.q50.mean()
    raw = result[result.model_short.isin(["LR", "TFT"])]
    pd.testing.assert_frame_equal(
        raw[quantiles.columns].reset_index(drop=True), quantiles.reset_index(drop=True)
    )


@pytest.mark.parametrize("derived, expected", [([44, 45], [44, 45]), ([44, None], [44, 20])])
def test_populated_derived_ignores_malformed_raw(derived, expected):
    frame = pd.DataFrame({"forecasted_discharge": derived, "q": ["bad", 20], "q50": ["bad", 99]})
    assert fill_forecasted_discharge(frame).forecasted_discharge.tolist() == expected


@pytest.mark.parametrize("horizon_type", ["quarter", "season"])
def test_aggregated_reader_keeps_q_coercion(horizon_type):
    frame = pd.DataFrame(
        {
            "code": ["SYNTHETIC_A"],
            "model_type": ["LR"],
            "valid_from": ["2025-04-01"],
            "valid_to": ["2025-06-30"],
            "q": ["bad"],
            "q50": [20],
            "horizon_value": [1],
        }
    )
    result = data_reader._normalize_combined_forecasts(frame, horizon_type)
    assert result.forecasted_discharge.iloc[0] == 20
    assert result.q.iloc[0] == "bad"


def test_no_point_columns_does_not_invent_points():
    frame = pd.DataFrame({"code": ["SYNTHETIC_A"]})
    pd.testing.assert_frame_equal(fill_forecasted_discharge(frame), frame)


def test_monthly_combined_reader_preserves_derived_and_fills_missing():
    raw = pd.DataFrame(
        {
            "code": ["SYNTHETIC_A"] * 3,
            "model_type": ["LR_Base", "GBT", "MC_ALD"],
            "valid_from": ["2025-03-01"] * 3,
            "valid_to": ["2025-03-31"] * 3,
            "q": [0, None, 30],
            "q50": [99, 20, 98],
            "forecasted_discharge": [None, None, 44],
        }
    )
    result = data_reader._normalize_monthly_combined_forecasts(raw)
    assert result.forecasted_discharge.tolist() == [0, 20, 44]
    assert result.q50.tolist() == [99, 20, 98]


@pytest.mark.parametrize("horizon_type", ["quarter", "season"])
@pytest.mark.parametrize("existing, expected", [(np.nan, 60), (44, 82)])
@pytest.mark.parametrize("q50_null", [False, True])
def test_aggregated_multimodel_builder(horizon_type, existing, expected, q50_null):
    from src.ensemble_calculator import (
        create_quarterly_ensemble_forecasts,
        create_seasonal_ensemble_forecasts,
    )
    from test_quarterly_ensemble_creation import (
        _two_model_quarterly_fcst,
        _two_model_quarterly_skill,
        _two_model_seasonal_fcst,
        _two_model_seasonal_skill,
    )

    if horizon_type == "quarter":
        forecasts, skill = _two_model_quarterly_fcst(), _two_model_quarterly_skill()
        builder = create_quarterly_ensemble_forecasts
    else:
        forecasts, skill = _two_model_seasonal_fcst(), _two_model_seasonal_skill()
        builder = create_seasonal_ensemble_forecasts
    forecasts["code"] = "SYNTHETIC_A"
    skill["code"] = "SYNTHETIC_A"
    forecasts["q"] = [0, 120 if q50_null else np.nan]
    if q50_null:
        forecasts["q50"] = np.nan
    forecasts["forecasted_discharge"] = [existing, np.nan]
    forecasts["horizon_value"] = 1
    skill["horizon_value"] = 1
    forecasts["date"] = "2025-01-01"
    forecasts["valid_from"] = "2025-04-01"
    forecasts["valid_to"] = "2025-06-30"
    quantile_columns = forecasts.filter(regex="^q[0-9]").columns
    forecasts[quantile_columns] = forecasts[quantile_columns].astype(float)
    quantiles = forecasts[quantile_columns].copy()
    with patch.dict("os.environ", {**THRESHOLD_ENV, "SAPPHIRE_SKILL_LEAD_AWARE": "true"}):
        result = builder(forecasts, skill)
    naive = result[result.model_short == "Naive Mean"]
    assert len(naive) == 1
    row = naive.iloc[0]
    assert row.forecasted_discharge == expected
    assert row.composition == "LR_Base, LR_SM"
    assert row.horizon_value == 1
    assert row.date == "2025-01-01"
    assert row.valid_from == "2025-04-01"
    assert row.valid_to == "2025-06-30"
    if q50_null:
        assert pd.isna(row.q50)
    else:
        assert row.q50 == quantiles.q50.mean()
    raw = result[result.model_short.isin(["LR_Base", "LR_SM"])]
    pd.testing.assert_frame_equal(
        raw[quantiles.columns].reset_index(drop=True), quantiles.reset_index(drop=True)
    )
