"""Shared fixtures for validate_pipeline tests."""

import os
import sys

import pytest

# Add validate_pipeline/ to path so validate_pipeline.py can be imported.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


_AMBIENT_ENV_VARS = (
    "ieasyhydroforecast_env_file_path",
    "PYTHON_DOTENV_DISABLED",
    "SAPPHIRE_API_ENABLED",
    "SAPPHIRE_API_URL",
    "SAPPHIRE_PREDICTION_MODE",
    "FRESHNESS_THRESHOLD_DAYS",
)


@pytest.fixture(autouse=True)
def _no_deployment_env_pointer(monkeypatch):
    """Remove ambient deployment env vars from the environment.

    Without this, a developer's shell with any of these variables exported
    (e.g. pointing at a deployment .env, or disabling dotenv/API for manual
    testing) leaks into every test and makes the suite's result depend on
    that shell's state. Tests that need one of them set it themselves with
    monkeypatch.setenv, which runs after this fixture and is restored
    automatically on teardown.
    """
    for var in _AMBIENT_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
