"""The reported tolerance is independent of each model's paired history."""

import numpy as np
import pandas as pd
import pytest
from conftest import DECAD, PENTAD
from src.skill_metrics import calculate_skill_metrics


@pytest.mark.parametrize("config", [PENTAD, DECAD], ids=lambda c: c.name)
@pytest.mark.parametrize("with_period", [False, True])
def test_latest_observation_delta_shared_without_changing_accuracy(
    config, with_period, monkeypatch
):
    monkeypatch.setenv("SAPPHIRE_SKILL_METRICS_START_YEAR", "2010")
    observed = pd.DataFrame(
        {
            "code": ["19999"] * 4,
            "date": pd.to_datetime(["2019-12-31", "2020-12-31", "2021-12-31", "2022-12-31"]),
            "discharge_avg": [10.0, 20.0, 30.0, 40.0],
            "delta": [1.0, 3.0, 5.0, 7.0],
            "model_short": ["Obs"] * 4,
        }
    )
    if with_period:
        observed[config.period_col] = "1"
    simulated = pd.DataFrame(
        {
            "code": ["19999"] * 4,
            "date": observed["date"].iloc[[0, 1, 1, 2]].to_numpy(),
            config.period_col: ["1"] * 4,
            "forecasted_discharge": [12.0, 22.0, 22.0, 32.0],
            "model_short": ["LR", "LR", "TFT", "TFT"],
        }
    )
    stats, _, _ = calculate_skill_metrics(
        config, observed.sample(frac=1, random_state=4), simulated
    )
    stats = stats.set_index("model_short")
    assert stats["delta"].to_dict() == {"LR": 7.0, "TFT": 7.0}
    assert stats["accuracy"].to_dict() == {"LR": 0.5, "TFT": 1.0}
    assert stats["n_pairs"].to_dict() == {"LR": 2, "TFT": 2}


@pytest.mark.parametrize("config", [PENTAD, DECAD], ids=lambda c: c.name)
def test_missing_delta_is_not_borrowed_from_other_period(config, monkeypatch):
    monkeypatch.setenv("SAPPHIRE_SKILL_METRICS_START_YEAR", "2010")
    observed = pd.DataFrame(
        {
            "code": ["19999", "19999"],
            "date": pd.to_datetime(["2021-12-31", "2022-01-31"]),
            "discharge_avg": [10.0, 20.0],
            "delta": [np.nan, 3.0],
            "model_short": ["Obs", "Obs"],
        }
    )
    simulated = observed.iloc[:1][["code", "date"]].copy()
    simulated[config.period_col] = 1
    simulated["forecasted_discharge"] = 11.0
    simulated["model_short"] = "LR"
    stats, _, _ = calculate_skill_metrics(config, observed, simulated)
    assert pd.isna(stats.loc[0, "delta"])


@pytest.mark.parametrize("bad_delta", [np.nan, np.inf, -1.0])
def test_invalid_latest_delta_uses_latest_finite_nonnegative_source(bad_delta, monkeypatch):
    monkeypatch.setenv("SAPPHIRE_SKILL_METRICS_START_YEAR", "2010")
    observed = pd.DataFrame(
        {
            "code": ["19999"] * 3,
            "date": pd.to_datetime(["2019-12-31", "2020-12-31", "2021-12-31"]),
            "discharge_avg": [10.0, 20.0, 30.0],
            "delta": [1.0, 2.0, bad_delta],
            "model_short": ["Obs"] * 3,
        }
    )
    simulated = observed.iloc[:1][["code", "date"]].copy()
    simulated["pentad_in_year"] = 1
    simulated["forecasted_discharge"] = 11.0
    simulated["model_short"] = "LR"
    stats, _, _ = calculate_skill_metrics(PENTAD, observed, simulated)
    assert stats.loc[0, "delta"] == 2.0
