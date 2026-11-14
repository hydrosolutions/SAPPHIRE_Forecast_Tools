"""Shared fixtures for postprocessing_forecasts tests."""

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import forecast_library as fl
import tag_library as tl
from src import api_writer
from src.horizon_config import ShortTermHorizonConfig

# Module-level config constants for direct import in tests
PENTAD = ShortTermHorizonConfig(
    name="pentad",
    period_col="pentad_in_year",
    period_in_month_col="pentad_in_month",
    get_period_func=tl.get_pentad,
    combined_csv_env="ieasyforecast_combined_forecast_pentad_file",
    skill_csv_env="ieasyforecast_pentadal_skill_metrics_file",
    api_horizon_type="pentad",
    neural_ensemble_func=lambda x: x,  # placeholder for unit tests
    station_selection_env="ieasyforecast_config_file_station_selection",
)
DECAD = ShortTermHorizonConfig(
    name="decad",
    period_col="decad_in_year",
    period_in_month_col="decad_in_month",
    get_period_func=tl.get_decad_in_month,
    combined_csv_env="ieasyforecast_combined_forecast_decad_file",
    skill_csv_env="ieasyforecast_decadal_skill_metrics_file",
    api_horizon_type="decad",
    neural_ensemble_func=lambda x: x,  # placeholder for unit tests
    station_selection_env="ieasyforecast_config_file_station_selection_decad",
)


@pytest.fixture
def pentad_config():
    """ShortTermHorizonConfig for pentad horizon."""
    return PENTAD


@pytest.fixture
def decad_config():
    """ShortTermHorizonConfig for decad horizon."""
    return DECAD


@pytest.fixture(autouse=True)
def _reset_api_singletons():
    """Reset forecast_library and api_writer API client singletons between tests.

    Without this, a mock injected by one test leaks into subsequent tests
    because the singleton caches the first client instance it creates.
    """
    fl._reset_api_clients()
    api_writer._reset_api_client()
    yield
    fl._reset_api_clients()
    api_writer._reset_api_client()


@pytest.fixture
def em_recalc_api_boundaries(monkeypatch):
    """Supply period fixtures and an EM API sink to legacy CSV workflow tests.

    Scoring and EM calculation stay real. These tests predate the API-only EM
    path; its serialization and stateful readback have separate coverage.
    """
    from src import data_reader

    original_skill_read = data_reader._read_skill_metrics_api

    def read_periods(horizon, codes=None, **kwargs):
        _, members = data_reader.read_observed_and_modelled_data(horizon, codes=codes)
        return members.copy(), pd.DataFrame()

    def read_skills(horizon, codes=None, *, strict=False):
        if strict:
            return pd.DataFrame()
        return original_skill_read(horizon, codes)

    published = []

    def publish(data, horizon):
        published.append((horizon, data.copy()))
        return True

    monkeypatch.setattr(data_reader, "read_individual_model_forecasts", read_periods)
    monkeypatch.setattr(data_reader, "_read_skill_metrics_api", read_skills)
    monkeypatch.setattr(api_writer, "_write_combined_forecast_to_api", publish)
    return published
