"""Shared fixtures for postprocessing_forecasts tests."""

import os
import sys

import pytest

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast')
)
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..')
)

import forecast_library as fl
from src import api_writer


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
