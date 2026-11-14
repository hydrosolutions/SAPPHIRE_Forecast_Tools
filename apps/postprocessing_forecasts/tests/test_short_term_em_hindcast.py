"""PP-075: stored boundary EM history, pooled scoring, and replacement."""

import importlib

import numpy as np
import pandas as pd
import pytest
from conftest import DECAD, PENTAD
from src import api_writer, data_reader, skill_metrics


@pytest.fixture(autouse=True)
def settings(monkeypatch):
    monkeypatch.setenv("SAPPHIRE_SKILL_METRICS_START_YEAR", "2020")
    monkeypatch.setenv("ieasyhydroforecast_nse_threshold_short_term", "0.000000001")
    monkeypatch.setenv("ieasyhydroforecast_min_pairs_short_term", "4")
    monkeypatch.setenv("ieasyhydroforecast_efficiency_threshold_short_term", "off")
    monkeypatch.setenv("ieasyhydroforecast_accuracy_threshold_short_term", "off")


def history(config, n=4, month=1, day=1):
    dates = [
        pd.Timestamp(year=2020 + i, month=month, day=day) - pd.Timedelta(days=1) for i in range(n)
    ]
    per_month = 6 if config.name == "pentad" else 3
    period = (month - 1) * per_month + int(config.get_period_func(dates[0] + pd.Timedelta(days=1)))
    observed = pd.DataFrame(
        {
            "code": ["19999"] * n,
            "date": dates,
            "model_short": ["Obs"] * n,
            "discharge_avg": [10.0 + 10 * i for i in range(n)],
            "delta": [2.0] * n,
            config.period_col: [period] * n,
        }
    )
    rows = []
    for i, date in enumerate(dates):
        point = 10.0 + 10 * i
        for model in ["LR", "TFT"]:
            rows.append(
                {
                    "code": "19999",
                    "date": date,
                    "model_short": model,
                    config.period_col: period,
                    config.period_in_month_col: int(
                        config.get_period_func(date + pd.Timedelta(days=1))
                    ),
                    "forecasted_discharge": point + (0 if model == "LR" else 2),
                    "q05": point - 4,
                    "q25": point - 2,
                    "q75": point + 4,
                    "q95": point + 6,
                }
            )
    members = pd.DataFrame(rows)
    gates = pd.DataFrame(
        {
            "code": ["19999"] * 2,
            config.period_col: [period] * 2,
            "model_short": ["LR", "TFT"],
            "nse": [0.9] * 2,
            "n_pairs": [8] * 2,
            "delta": [2.0] * 2,
            "sdivsigma": [0.1] * 2,
            "accuracy": [1.0] * 2,
            "mae": [1.0] * 2,
        }
    )
    return observed, members, gates


@pytest.mark.parametrize("config", [PENTAD, DECAD], ids=lambda c: c.name)
@pytest.mark.parametrize("month,day", [(1, 1), (3, 1), (12, 21)])
def test_four_target_years_including_previous_december(config, month, day):
    observed, members, gates = history(config, month=month, day=day)
    em, stats = skill_metrics.calculate_short_term_em_hindcast(config, observed, members, gates)
    assert len(em) == 4
    assert stats.loc[0, "n_pairs"] == 4
    assert stats.loc[0, "mae"] == pytest.approx(1)
    assert stats.loc[0, "nse"] == pytest.approx(0.992)
    assert set(em["composition"]) == {"LR, TFT"}
    assert stats.loc[0, "composition"] == "LR, TFT"


@pytest.mark.parametrize("config", [PENTAD, DECAD], ids=lambda c: c.name)
def test_three_year_metrics_null_but_history_and_shared_delta_remain(config):
    observed, members, gates = history(config, n=3)
    em, stats = skill_metrics.calculate_short_term_em_hindcast(config, observed, members, gates)
    assert len(em) == 3
    assert stats.loc[0, "n_pairs"] == 3
    assert stats.loc[0, "delta"] == 2
    assert stats.loc[0, ["nse", "mae", "accuracy", "crps"]].isna().all()


def test_compositions_pool_and_duplicate_copies_do_not_inflate_years():
    observed, members, gates = history(PENTAD)
    members.loc[
        (members["model_short"] == "TFT") & (members["date"].dt.year >= 2021), "model_short"
    ] = "TiDE"
    gates = pd.concat([gates, gates.iloc[[1]].assign(model_short="TiDE")], ignore_index=True)
    em, stats = skill_metrics.calculate_short_term_em_hindcast(
        PENTAD,
        pd.concat([observed, observed.iloc[[0]]]),
        pd.concat([members, members.iloc[[0]]]),
        gates,
    )
    assert len(em) == 4
    assert len(stats) == 1
    assert stats.loc[0, "n_pairs"] == 4
    assert stats.loc[0, "composition"] == "LR, TFT, TiDE"


def test_nonboundary_and_mismatched_target_period_never_enter_em():
    observed, members, gates = history(PENTAD)
    bad = members.iloc[[1]].copy()
    bad["date"] += pd.Timedelta(days=2)
    members.loc[3, PENTAD.period_col] = 2
    em, stats = skill_metrics.calculate_short_term_em_hindcast(
        PENTAD,
        observed,
        pd.concat([members, bad]),
        gates,
    )
    assert len(em) == 3
    assert stats.loc[0, "n_pairs"] == 3


def test_missing_observation_does_not_count_and_undefined_metric_stays_null():
    observed, members, gates = history(PENTAD, n=5)
    observed.loc[0, "discharge_avg"] = np.inf
    observed.loc[1:, "discharge_avg"] = 10.0
    _, stats = skill_metrics.calculate_short_term_em_hindcast(PENTAD, observed, members, gates)
    assert stats.loc[0, "n_pairs"] == 4
    assert pd.isna(stats.loc[0, "nse"])
    assert np.isfinite(stats.loc[0, "mae"])


def test_conflicting_duplicate_member_is_not_averaged():
    observed, members, gates = history(PENTAD)
    conflicting = members.iloc[[0]].assign(forecasted_discharge=123.0)
    with pytest.raises(ValueError, match="Conflicting stored"):
        skill_metrics.calculate_short_term_em_hindcast(
            PENTAD, observed, pd.concat([members, conflicting]), gates
        )


def test_no_matching_observations_keeps_forecasts_without_skill():
    observed, members, gates = history(PENTAD)
    observed["code"] = "19998"
    em, stats = skill_metrics.calculate_short_term_em_hindcast(PENTAD, observed, members, gates)
    assert len(em) == 4
    assert stats.empty


@pytest.mark.parametrize("config", [PENTAD, DECAD], ids=lambda c: c.name)
@pytest.mark.parametrize("month", [1, 3])
@pytest.mark.parametrize("matching_observations", [False, True])
def test_recalc_writes_all_em_years_and_overwrites_stale_skills(
    config, month, matching_observations, monkeypatch, tmp_path
):
    recalc = importlib.import_module("recalculate_skill_metrics")
    observed, members, _ = history(config, month=month)
    # A valid earlier forecast is persisted too, but must not increase the
    # four target years selected by SAPPHIRE_SKILL_METRICS_START_YEAR=2020.
    earlier_observation = observed.iloc[[0]].copy()
    earlier_observation["date"] = pd.Timestamp(year=2019, month=month, day=1) - pd.Timedelta(days=1)
    earlier_members = members.iloc[:2].copy()
    earlier_members["date"] = earlier_observation.iloc[0]["date"]
    observed = pd.concat([earlier_observation, observed], ignore_index=True)
    members = pd.concat([earlier_members, members], ignore_index=True)
    if not matching_observations:
        observed["date"] += pd.DateOffset(years=10)
    monkeypatch.setenv("SAPPHIRE_RECALC_STATION_CODE", "19999")
    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    monkeypatch.setenv(config.skill_csv_env, "skills.csv")
    monkeypatch.setenv(config.combined_csv_env, "forecasts.csv")
    monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")
    monkeypatch.setattr(
        recalc.data_reader, "read_observed_and_modelled_data", lambda *a, **k: (observed, members)
    )
    monkeypatch.setattr(
        recalc.data_reader,
        "read_individual_model_forecasts",
        lambda *a, **k: (members, pd.DataFrame()),
    )
    monkeypatch.setattr(recalc.pt, "log_most_recent_forecasts", lambda *a: None)
    monkeypatch.setattr(api_writer, "SAPPHIRE_API_AVAILABLE", True)
    monkeypatch.setattr(data_reader, "SAPPHIRE_API_AVAILABLE", True)
    monkeypatch.setattr(api_writer, "_check_write_codes", lambda *a: None)
    api_horizon = api_writer.HORIZON_TYPE_TO_API[config.name]
    stale_date = "2026-01-06" if config.name == "pentad" else "2026-01-11"
    stale = {
        "code": "19999",
        "horizon_type": api_horizon,
        "model_type": "EM",
        "horizon_in_year": 2,
        "horizon_value": 0,
        "date": stale_date,
        "n_pairs": 2,
        "nse": -5.0,
    }
    protected = {**stale, "model_type": "TFT", "n_pairs": 9, "nse": 0.9}

    def skill_key(row):
        return tuple(
            row[field]
            for field in (
                "horizon_type",
                "code",
                "model_type",
                "date",
                "horizon_in_year",
                "horizon_value",
            )
        )

    class Client:
        def __init__(self):
            self.forecasts = {}
            self.skills = {skill_key(row): row.copy() for row in (stale, protected)}

        def readiness_check(self):
            return True

        def read_skill_metrics(self, **kwargs):
            rows = [
                row.copy()
                for row in self.skills.values()
                if row["horizon_type"] == kwargs["horizon"]
                and ("code" not in kwargs or row["code"] == kwargs["code"])
            ]
            return pd.DataFrame(rows[kwargs["skip"] : kwargs["skip"] + kwargs["limit"]])

        def write_forecasts(self, records):
            for row in records:
                key = tuple(
                    row[field] for field in ("horizon_type", "code", "model_type", "date", "target")
                )
                self.forecasts[key] = row.copy()
            return len(records)

        def write_skill_metrics(self, records):
            # Match the existing service's full-value upsert: None clears an
            # old metric rather than leaving the previous value behind.
            for row in records:
                key = skill_key(row)
                self.skills[key] = {**self.skills.get(key, {}), **row}
            return len(records)

    client = Client()
    monkeypatch.setattr(api_writer, "_get_postprocessing_client", lambda: client)
    monkeypatch.setattr(data_reader, "SapphirePostprocessingClient", lambda **k: client)
    errors = []
    recalc._run_short_term_recalc(config, 2026, errors, None, codes=["19999"])
    assert errors == []
    em_records = [r for r in client.forecasts.values() if r["model_type"] == "EM"]
    assert len(em_records) == (5 if matching_observations else 0)
    if matching_observations:
        assert {r["date"] for r in em_records} == set(members["date"].dt.strftime("%Y-%m-%d"))

    readback = data_reader._read_skill_metrics_api(config.name, codes=["19999"], strict=True)
    invalid = readback[(readback["model_short"] == "EM") & (readback[config.period_col] == 2)]
    assert len(invalid) == 1
    assert invalid.iloc[0]["n_pairs"] == 0
    assert pd.isna(invalid.iloc[0]["nse"])
    assert invalid.iloc[0]["date"] == stale_date
    assert client.skills[skill_key(protected)] == protected
    visible = data_reader._drop_tombstone_rows(readback)
    assert not ((visible["model_short"] == "EM") & (visible[config.period_col] == 2)).any()
    current_period = int(members.iloc[0][config.period_col])
    current = visible[visible[config.period_col] == current_period].set_index("model_short")
    if matching_observations:
        assert current.loc["LR", "n_pairs"] == 4
        assert current.loc["TFT", "n_pairs"] == 4
        assert current.loc["EM", "n_pairs"] == 4
        assert current.loc["EM", "nse"] == pytest.approx(0.992)
    else:
        assert current.empty
