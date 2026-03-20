"""Shared fixtures for iEasyHydroForecast tests."""

import os
import sys

import pytest

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..')
)

import forecast_library as fl


@pytest.fixture(autouse=True)
def _reset_api_singletons():
    """Reset forecast_library API client singletons between tests."""
    fl._reset_api_clients()
    yield
    fl._reset_api_clients()
