"""EM replacement requires successful reads, including genuinely empty history."""

import importlib

import pandas as pd
import pytest
from conftest import DECAD, PENTAD
from src import api_writer, data_reader


def setup_clients(config, monkeypatch, tmp_path, missing=None, failure=None):
    horizon = api_writer.HORIZON_TYPE_TO_API[config.name]
    dates = [f"{year}-02-28" for year in (2021, 2022, 2023)] + ["2024-02-29"]
    runoff = pd.DataFrame(
        {
            "code": ["19999"] * 4,
            "date": dates,
            "discharge": [10.0, 20.0, 30.0, 40.0],
            "horizon_in_year": [13 if config.name == "pentad" else 7] * 4,
            "horizon_value": [1] * 4,
        }
    )
    lr = pd.DataFrame(
        {
            "code": ["19999"] * 4,
            "date": dates,
            "forecasted_discharge": [11.0, 21.0, 31.0, 41.0],
            "delta": [2.0] * 4,
        }
    )
    ml = lr.drop(columns="delta").assign(
        target=pd.to_datetime(dates) + pd.Timedelta(days=1),
        horizon_type=horizon,
        q05=[6.0, 16.0, 26.0, 36.0],
        q25=[8.0, 18.0, 28.0, 38.0],
        q75=[12.0, 22.0, 32.0, 42.0],
        q95=[14.0, 24.0, 34.0, 44.0],
    )

    def key(row):
        return tuple(
            row[k]
            for k in (
                "horizon_type",
                "code",
                "model_type",
                "date",
                "horizon_in_year",
                "horizon_value",
            )
        )

    old = {
        "code": "19999",
        "horizon_type": horizon,
        "model_type": "EM",
        "date": "2026-03-01",
        "horizon_in_year": 13 if config.name == "pentad" else 7,
        "horizon_value": 0,
        "n_pairs": 8,
        "nse": 0.9,
    }
    protected = [dict(old, model_type="TFT"), dict(old, date="2025-03-01"), dict(old, code="19998")]

    class Client:
        def __init__(self):
            self.skills = {key(r): r.copy() for r in [old, *protected]}
            self.forecast_writes = []
            self.skill_writes = []
            self.ml_reads = 0

        def readiness_check(self):
            return True

        def read_runoff(self, **kwargs):
            if failure == "observations_none":
                return None
            if failure == "observations":
                raise TimeoutError("synthetic observation timeout")
            return pd.DataFrame() if missing in ("observations", "both") else runoff.copy()

        def read_lr_forecasts(self, **kwargs):
            if failure == "lr_none":
                return None
            if failure == "lr":
                raise TimeoutError("synthetic LR timeout")
            return pd.DataFrame() if missing in ("forecasts", "both") else lr.copy()

        def read_short_term_forecasts(self, **kwargs):
            self.ml_reads += 1
            if failure == "ml_none":
                return None
            if failure == "ml" and self.ml_reads == 1:
                raise TimeoutError("synthetic first ML timeout")
            if missing in ("forecasts", "both") or kwargs["horizon"] == "day":
                return pd.DataFrame()
            return ml.copy()

        def read_skill_metrics(self, **kwargs):
            rows = [
                r.copy()
                for r in self.skills.values()
                if r["horizon_type"] == kwargs["horizon"]
                and ("code" not in kwargs or r["code"] == kwargs["code"])
            ]
            return pd.DataFrame(rows[kwargs["skip"] : kwargs["skip"] + kwargs["limit"]])

        def write_forecasts(self, records):
            self.forecast_writes.extend(records)
            return len(records)

        def write_skill_metrics(self, records):
            self.skill_writes.extend(records)
            for row in records:
                self.skills[key(row)] = {**self.skills.get(key(row), {}), **row}
            return len(records)

    client = Client()
    monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")
    monkeypatch.setenv("SAPPHIRE_SKILL_METRICS_START_YEAR", "2020")
    monkeypatch.setenv("SAPPHIRE_RECALC_STATION_CODE", "19999")
    monkeypatch.setenv("ieasyhydroforecast_run_ML_models", "true")
    monkeypatch.setenv("ieasyhydroforecast_available_ML_models", "TFT")
    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    monkeypatch.setenv(config.skill_csv_env, "skills.csv")
    monkeypatch.setenv(config.combined_csv_env, "forecasts.csv")
    monkeypatch.setattr(data_reader, "SAPPHIRE_API_AVAILABLE", True)
    monkeypatch.setattr(api_writer, "SAPPHIRE_API_AVAILABLE", True)
    monkeypatch.setattr(data_reader, "SapphirePreprocessingClient", lambda **kwargs: client)
    monkeypatch.setattr(data_reader, "SapphirePostprocessingClient", lambda **kwargs: client)
    monkeypatch.setattr(api_writer, "_get_postprocessing_client", lambda: client)
    monkeypatch.setattr(api_writer, "_check_write_codes", lambda *args: None)

    def forbid_csv(*args, **kwargs):
        pytest.fail("Strict recalculation must not fall back to CSV")

    monkeypatch.setattr(data_reader, "_read_short_term_observations_csv", forbid_csv)
    recalc = importlib.import_module("recalculate_skill_metrics")
    monkeypatch.setattr(recalc.pt, "log_most_recent_forecasts", lambda *args: None)
    return recalc, client, old, protected, key


@pytest.mark.parametrize("config", [PENTAD, DECAD], ids=lambda config: config.name)
@pytest.mark.parametrize(
    "failure", ["ml", "lr", "observations", "ml_none", "lr_none", "observations_none"]
)
def test_failed_scoring_read_aborts_before_any_em_replacement(
    config, failure, monkeypatch, tmp_path
):
    recalc, client, old, protected, key = setup_clients(
        config, monkeypatch, tmp_path, failure=failure
    )
    error_type, message = (
        (RuntimeError, "Incomplete") if failure.endswith("_none") else (TimeoutError, "synthetic")
    )
    with pytest.raises(error_type, match=message):
        recalc._run_short_term_recalc(config, 2026, [], None, codes=["19999"])
    assert client.forecast_writes == []
    assert client.skill_writes == []
    assert client.skills[key(old)] == old
    assert all(client.skills[key(row)] == row for row in protected)
    if failure == "ml":
        assert client.ml_reads == 1  # No second successful read can hide the failure.


@pytest.mark.parametrize("config", [PENTAD, DECAD], ids=lambda config: config.name)
@pytest.mark.parametrize("missing", ["observations", "forecasts", "both"])
def test_confirmed_empty_history_invalidates_only_scoped_current_em(
    config, missing, monkeypatch, tmp_path
):
    recalc, client, old, protected, key = setup_clients(
        config, monkeypatch, tmp_path, missing=missing
    )
    errors = []
    recalc._run_short_term_recalc(config, 2026, errors, None, codes=["19999"])
    assert errors == []
    assert client.forecast_writes == []
    assert len(client.skill_writes) == 1
    assert client.skills[key(old)]["n_pairs"] == 0
    assert client.skills[key(old)]["nse"] is None
    assert all(client.skills[key(row)] == row for row in protected)
    readback = data_reader._read_skill_metrics_api(config.name, codes=["19999"], strict=True)
    visible = data_reader._drop_tombstone_rows(readback)
    assert not ((visible["model_short"] == "EM") & (visible["date"] == "2026-03-01")).any()


@pytest.mark.parametrize("config", [PENTAD, DECAD], ids=lambda config: config.name)
def test_lead_aware_recalc_replaces_member_and_em_skills(config, monkeypatch, tmp_path):
    recalc, client, old, protected, key = setup_clients(config, monkeypatch, tmp_path)
    monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
    monkeypatch.setenv("ieasyhydroforecast_nse_threshold_short_term", "0.000000001")
    monkeypatch.setenv("ieasyhydroforecast_min_pairs_short_term", "4")
    monkeypatch.setenv("ieasyhydroforecast_efficiency_threshold_short_term", "off")
    monkeypatch.setenv("ieasyhydroforecast_accuracy_threshold_short_term", "off")
    models = {"LR", "TFT", "EM"}
    for model in models:
        row = dict(old, model_type=model, delta=99.0)
        client.skills[key(row)] = row

    errors = []
    recalc._run_short_term_recalc(config, 2026, errors, None, codes=["19999"])

    assert errors == []
    assert {row["model_type"] for row in client.skill_writes} == models
    assert len(client.skill_writes) == len(models)
    assert all(row["horizon_value"] == 0 for row in client.skill_writes)
    for model in models:
        row = client.skills[key(dict(old, model_type=model))]
        assert row["delta"] == pytest.approx(2.0)
        assert row["n_pairs"] == 4
    # The same API upsert key must preserve other stations and reporting years.
    assert all(client.skills[key(row)] == row for row in protected[1:])
    readback = data_reader._read_skill_metrics_api(config.name, codes=["19999"], strict=True)
    current = readback[pd.to_datetime(readback["date"]).dt.year == 2026]
    assert set(current["model_short"]) == models
    assert current["delta"].tolist() == [2.0] * len(models)
