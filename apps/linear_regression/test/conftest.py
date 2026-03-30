"""Shared fixtures for linear_regression tests."""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

import forecast_library as fl


@pytest.fixture(autouse=True)
def _reset_api_singletons():
    """Reset forecast_library API client singletons between tests.

    Without this, a mock injected by one test leaks into subsequent tests
    because the singleton caches the first client instance it creates.
    """
    fl._reset_api_clients()
    yield
    fl._reset_api_clients()


@pytest.fixture()
def disable_api_client(monkeypatch):
    """Force _get_postprocessing_client() to return None.

    Use this in CSV-path tests so that the API-first path in
    get_last_forecast_dates_per_gauge() is skipped and the CSV fallback
    is exercised.
    """
    monkeypatch.setattr(fl, "_get_postprocessing_client", lambda: None)
