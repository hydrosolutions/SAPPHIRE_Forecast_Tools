"""The EM reconstruction reader never substitutes DAY forecasts."""

import pandas as pd
import pytest
from src import data_reader


def setup_reader(monkeypatch, payload):
    calls = []

    class Client:
        def readiness_check(self):
            return True

        def read_short_term_forecasts(self, **kwargs):
            calls.append(kwargs)
            if isinstance(payload, Exception):
                raise payload
            return payload

        def read_lr_forecasts(self, **kwargs):
            return pd.DataFrame()

        def read_skill_metrics(self, **kwargs):
            if isinstance(payload, Exception):
                raise payload
            return payload

    monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")
    monkeypatch.setenv("ieasyhydroforecast_run_ML_models", "true")
    monkeypatch.setenv("ieasyhydroforecast_available_ML_models", "TFT")
    monkeypatch.setattr(data_reader, "SAPPHIRE_API_AVAILABLE", True)
    monkeypatch.setattr(data_reader, "SapphirePostprocessingClient", lambda **k: Client())
    return calls


@pytest.mark.parametrize("horizon,api_horizon", [("pentad", "pentad"), ("decad", "decade")])
def test_stored_period_reader_validates_exact_target_and_preserves_scope(
    monkeypatch, horizon, api_horizon
):
    payload = pd.DataFrame(
        {
            "code": ["19999"] * 4,
            "date": ["2020-12-31"] * 4,
            "target": ["2021-01-01", "2022-01-01", "2021-01-02", "2021-01-01"],
            "horizon_type": [api_horizon] * 3 + ["day"],
            "forecasted_discharge": [10, 999, 999, 999],
            "q05": [6] * 4,
            "q25": [8] * 4,
            "q75": [12] * 4,
            "q95": [14] * 4,
        }
    )
    calls = setup_reader(monkeypatch, payload)
    forecasts, _ = data_reader.read_individual_model_forecasts(
        horizon, codes=["19999"], stored_period_only=True
    )
    assert len(forecasts) == 1
    assert forecasts.iloc[0]["forecasted_discharge"] == 10
    assert calls == [
        {"horizon": api_horizon, "model": "TFT", "code": "19999", "skip": 0, "limit": 1000}
    ]


def test_stored_period_read_failure_is_not_an_absent_member(monkeypatch):
    setup_reader(monkeypatch, TimeoutError("synthetic timeout"))
    with pytest.raises(TimeoutError):
        data_reader.read_individual_model_forecasts("pentad", stored_period_only=True)


def test_strict_skill_read_empty_and_failed_are_distinct(monkeypatch):
    setup_reader(monkeypatch, pd.DataFrame())
    assert data_reader._read_skill_metrics_api("pentad", strict=True).empty
    setup_reader(monkeypatch, TimeoutError("synthetic timeout"))
    with pytest.raises(TimeoutError):
        data_reader._read_skill_metrics_api("pentad", strict=True)
    assert data_reader._read_skill_metrics_api("pentad") is None


def test_stored_period_duplicate_conflict_is_not_averaged(monkeypatch):
    payload = pd.DataFrame(
        {
            "code": ["19999", "19999"],
            "date": ["2020-12-31"] * 2,
            "target": ["2021-01-01"] * 2,
            "horizon_type": ["pentad"] * 2,
            "forecasted_discharge": [10, 20],
        }
    )
    setup_reader(monkeypatch, payload)
    with pytest.raises(ValueError, match="Conflicting stored ML"):
        data_reader.read_individual_model_forecasts("pentad", stored_period_only=True)
