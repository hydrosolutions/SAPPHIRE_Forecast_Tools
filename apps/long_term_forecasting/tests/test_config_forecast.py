"""Tests for ForecastConfig (apps/long_term_forecasting/config_forecast.py).

Focused on MIG-005: env-var parsing for `ieasyhydroforecast_ml_long_term_supported_modes`
must strip whitespace per entry and drop empty entries, while preserving the
current fail-fast behavior when the env var is missing entirely.
"""

import os
import sys

import pytest

# Add parent directory to path for imports (matches sibling test files).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import setup_library as sl  # noqa: E402  (imported for monkeypatch target)
from config_forecast import ForecastConfig  # noqa: E402


@pytest.fixture
def isolated_config(monkeypatch, tmp_path):
    """Isolate ForecastConfig from real .env file loading.

    Patches sl.load_environment to a no-op and sets the minimum env vars
    that ForecastConfig.__init__ / _get_paths() needs, so tests can vary
    a single env var without dragging in a real .env file.

    ForecastConfig._get_paths() reads (in addition to the env var under test):
      - ieasyhydroforecast_configuration_path
      - ieasyhydroforecast_ml_long_term_configuration
      - ieasyforecast_intermediate_data_path
      - ieasyhydroforecast_ml_long_term_output_path

    The output path is then joined via os.path.join, so both intermediate and
    output env vars must be set to non-None string values.
    """
    monkeypatch.setattr(sl, "load_environment", lambda *a, **kw: None)
    # Minimum env vars _get_paths needs to construct without errors.
    monkeypatch.setenv("ieasyhydroforecast_configuration_path", str(tmp_path))
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_configuration", "long_term_configs")
    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_output_path", "lt_output")
    return monkeypatch


@pytest.mark.parametrize(
    "env_value, expected",
    [
        ("monthly", ["monthly"]),
        ("monthly,seasonal", ["monthly", "seasonal"]),
        ("monthly, seasonal", ["monthly", "seasonal"]),  # space after comma
        (" monthly , seasonal ", ["monthly", "seasonal"]),  # surrounding whitespace
        (",monthly,,seasonal,", ["monthly", "seasonal"]),  # empty entries dropped
        ("monthly\t,\nseasonal", ["monthly", "seasonal"]),  # tabs + newlines
    ],
)
def test_lt_supported_modes_normalization(isolated_config, env_value, expected):
    """MIG-005: env var values with whitespace and empty entries are normalized."""
    isolated_config.setenv("ieasyhydroforecast_ml_long_term_supported_modes", env_value)
    config = ForecastConfig()
    assert config.LT_supported_modes == expected


def test_lt_supported_modes_fail_fast_on_missing_env(isolated_config):
    """MIG-005: missing env var must still raise AttributeError (fail-fast preserved)."""
    isolated_config.delenv("ieasyhydroforecast_ml_long_term_supported_modes", raising=False)
    with pytest.raises(AttributeError):
        ForecastConfig()
