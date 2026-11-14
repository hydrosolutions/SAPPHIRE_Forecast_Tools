"""Integration tests: EM maintenance minimal chain (parametrized).

Verifies create_ensemble_forecasts -> save_forecast_data(write_csv=False,
require_api=True) -> _write_combined_forecast_to_api with a fake SDK client.

Corrected indices (period_in_year=2, in_month=2 for both horizons):
  SYNTH_A pentad: issue 2024-01-05, target 2024-01-06
  SYNTH_A decad : issue 2024-01-10, target 2024-01-11
  LR fd=10 (delta=5 -> q25=5, q75=15; no q05/q95), TFT fd=20/q05=10/q25=15/q75=25/q95=30
  => EM fd=15, q05/q95 = TFT's, q25=(5+15)/2=10, q75=(15+25)/2=20, composition="LR, TFT"

Un-suffixed env gates are STRICT (efficiency=0.7, accuracy=0.8, nse=0.9) but the
short-term EM gate (NSE>0, n_pairs>=4 via explicit *_short_term env vars) takes
precedence over them, so the lenient EM gate still applies.
"""

import os
import socket
import sys
from unittest.mock import patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

import tag_library as tl
from src import api_writer, file_writer
from src.ensemble_calculator import create_ensemble_forecasts
from src.file_writer import save_forecast_data
from src.horizon_config import ShortTermHorizonConfig

SYNTH_CODE = "SYNTH_A"
LR_FD, TFT_FD = 10.0, 20.0
TFT_Q05, TFT_Q25, TFT_Q75, TFT_Q95 = 10.0, 15.0, 25.0, 30.0
LR_DELTA = 5.0
EXPECTED_EM_Q25 = (LR_FD - LR_DELTA + TFT_Q25) / 2
EXPECTED_EM_Q75 = (LR_FD + LR_DELTA + TFT_Q75) / 2
EXPECTED_EM_FD = 15.0  # (10 + 20) / 2
EXPECTED_EM_COMPOSITION = "LR, TFT"

# STRICT un-suffixed vars are overridden by the explicit lenient `*_short_term`
# vars below (sdivsigma/accuracy off, NSE > 0).  The un-suffixed fallback itself
# is tested in test_short_term_em_gate.py.
_ENV_STRICT = {
    "ieasyhydroforecast_efficiency_threshold": "0.7",
    "ieasyhydroforecast_accuracy_threshold": "0.8",
    "ieasyhydroforecast_nse_threshold": "0.9",
    "ieasyhydroforecast_efficiency_threshold_short_term": "False",
    "ieasyhydroforecast_accuracy_threshold_short_term": "False",
    "ieasyhydroforecast_nse_threshold_short_term": "1e-9",
    "SAPPHIRE_API_ENABLED": "true",
    "SAPPHIRE_API_URL": "http://test-fake-url.internal:8000",
    "ieasyforecast_combined_forecast_pentad_file": "combined_forecasts_pentad.csv",
    "ieasyforecast_combined_forecast_decad_file": "combined_forecasts_decad.csv",
    "SAPPHIRE_CONSISTENCY_CHECK": "false",
}


class _FakePostprocessingClient:
    """Captures write_forecasts calls; never connects."""

    def __init__(self, base_url=None, **kwargs):
        self.records: list[dict] = []

    def readiness_check(self) -> bool:
        return True

    def write_forecasts(self, records: list[dict]) -> int:
        self.records.extend(records)
        return len(records)


@pytest.fixture
def _pentad_config():
    return ShortTermHorizonConfig(
        name="pentad",
        period_col="pentad_in_year",
        period_in_month_col="pentad_in_month",
        get_period_func=tl.get_pentad,
        combined_csv_env="ieasyforecast_combined_forecast_pentad_file",
        skill_csv_env="ieasyforecast_pentadal_skill_metrics_file",
        api_horizon_type="pentad",
        neural_ensemble_func=lambda x: x,
        station_selection_env="ieasyforecast_config_file_station_selection",
    )


@pytest.fixture
def _decad_config():
    return ShortTermHorizonConfig(
        name="decad",
        period_col="decad_in_year",
        period_in_month_col="decad_in_month",
        get_period_func=tl.get_decad_in_month,
        combined_csv_env="ieasyforecast_combined_forecast_decad_file",
        skill_csv_env="ieasyforecast_decadal_skill_metrics_file",
        api_horizon_type="decad",
        neural_ensemble_func=lambda x: x,
        station_selection_env="ieasyforecast_config_file_station_selection_decad",
    )


def _make_forecasts(period_col: str, period_in_month_col: str, issue_date: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "code": [SYNTH_CODE, SYNTH_CODE],
            "date": pd.to_datetime([issue_date, issue_date]),
            period_col: [2, 2],
            period_in_month_col: ["2", "2"],
            "forecasted_discharge": [LR_FD, TFT_FD],
            "q05": [float("nan"), TFT_Q05],
            "q25": [float("nan"), TFT_Q25],
            "q75": [float("nan"), TFT_Q75],
            "q95": [float("nan"), TFT_Q95],
            "model_short": ["LR", "TFT"],
        }
    )


def _make_skill(period_col: str) -> pd.DataFrame:
    # NSE=0.5 passes the short-term gate (NSE>0); sdivsigma=1.5 and accuracy=0.3 fail the
    # strict global gates, which the short-term EM gate does not apply.
    return pd.DataFrame(
        {
            period_col: [2, 2],
            "code": [SYNTH_CODE, SYNTH_CODE],
            "model_short": ["LR", "TFT"],
            "sdivsigma": [1.5, 1.5],
            "nse": [0.5, 0.5],
            "delta": [5.0, 5.0],
            "accuracy": [0.3, 0.3],
            "mae": [2.0, 3.0],
            "n_pairs": [10, 10],
        }
    )


@pytest.mark.parametrize(
    "issue_date,target_date,api_horizon,horizon_value,horizon_in_year,config_fixture",
    [
        ("2024-01-05", "2024-01-06", "pentad", 2, 2, "_pentad_config"),
        ("2024-01-10", "2024-01-11", "decade", 2, 2, "_decad_config"),
    ],
)
def test_em_maintenance_chain(
    request,
    monkeypatch,
    tmp_path,
    issue_date,
    target_date,
    api_horizon,
    horizon_value,
    horizon_in_year,
    config_fixture,
):
    """EM maintenance chain: NSE>0 qualifies despite STRICT global gates; correct wire records."""
    config = request.getfixturevalue(config_fixture)
    forecasts = _make_forecasts(config.period_col, config.period_in_month_col, issue_date)
    skill = _make_skill(config.period_col)

    # Hard network guard: appends before raising so swallowed exceptions show up in the list.
    network_calls: list = []

    def _no_network(*a, **kw):
        network_calls.append(a)
        raise AssertionError(f"Network access forbidden in tests: {a}")

    monkeypatch.setattr(socket.socket, "connect", _no_network)
    monkeypatch.setattr(socket, "create_connection", _no_network)

    # Fake SDK injected at singleton level (bypasses SapphirePostprocessingClient construction).
    fake_client = _FakePostprocessingClient()
    monkeypatch.setattr(api_writer, "SAPPHIRE_API_AVAILABLE", True)
    monkeypatch.setattr(api_writer, "_postprocessing_client", fake_client)

    # CSV guard: atomic_write_csv must never fire when write_csv=False.
    monkeypatch.setattr(
        file_writer,
        "atomic_write_csv",
        lambda *a, **kw: (_ for _ in ()).throw(
            AssertionError("atomic_write_csv must not be called when write_csv=False")
        ),
    )

    env = {**_ENV_STRICT, "ieasyforecast_intermediate_data_path": str(tmp_path)}
    with patch.dict(os.environ, env):
        joint, _ = create_ensemble_forecasts(
            forecasts=forecasts,
            skill_stats=skill,
            period_col=config.period_col,
            period_in_month_col=config.period_in_month_col,
            get_period_in_month_func=config.get_period_func,
        )
        save_forecast_data(config, joint, write_csv=False, require_api=True)

    # Network: any attempt that swallowed the exception would appear here.
    assert network_calls == [], f"Network was attempted: {network_calls}"
    # No CSV files written.
    assert list(tmp_path.rglob("*.csv")) == []

    # Exactly EM + TFT; no LR.
    model_types = {r["model_type"] for r in fake_client.records}
    assert model_types == {"EM", "TFT"}, f"Unexpected model types: {model_types}"

    em = next(r for r in fake_client.records if r["model_type"] == "EM")
    tft = next(r for r in fake_client.records if r["model_type"] == "TFT")

    # Exact EM wire record.
    assert em["code"] == SYNTH_CODE
    assert em["horizon_type"] == api_horizon
    assert em["date"] == issue_date
    assert em["target"] == target_date
    assert em["horizon_value"] == horizon_value
    assert em["horizon_in_year"] == horizon_in_year
    assert em["forecasted_discharge"] == pytest.approx(EXPECTED_EM_FD)
    assert em["q05"] == pytest.approx(TFT_Q05)
    assert em["q25"] == pytest.approx(EXPECTED_EM_Q25)
    assert em["q75"] == pytest.approx(EXPECTED_EM_Q75)
    assert em["q95"] == pytest.approx(TFT_Q95)
    assert em["composition"] == EXPECTED_EM_COMPOSITION
    # Bands straddle TFT's point (20), not EM's (15) — confirm no band-to-point clamp.
    assert em["q05"] != pytest.approx(em["forecasted_discharge"])

    # TFT peer: values unchanged.
    assert tft["forecasted_discharge"] == pytest.approx(TFT_FD)
    assert tft["q05"] == pytest.approx(TFT_Q05)
    assert tft["q95"] == pytest.approx(TFT_Q95)
