"""Shared fixtures for forecast_dashboard unit tests.

These fixtures support testing the dashboard's processing, db, and site
modules WITHOUT requiring a running Panel server or live API.
"""

import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# sys.path: make iEasyHydroForecast importable (follows postprocessing pattern)
# ---------------------------------------------------------------------------
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"),
)

# Make the dashboard src/ package importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ---------------------------------------------------------------------------
# Environment setup (autouse — runs for every test)
# ---------------------------------------------------------------------------
FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture(autouse=True)
def _dashboard_env(monkeypatch):
    """Set env vars expected by dashboard code."""
    monkeypatch.setenv("SAPPHIRE_TEST_ENV", "True")


# ---------------------------------------------------------------------------
# Identity gettext — no-op translation function
# ---------------------------------------------------------------------------
@pytest.fixture
def identity_gettext():
    """Return a no-op ``_()`` function for i18n tests."""
    return lambda x: x


# ---------------------------------------------------------------------------
# Sample DataFrames
# ---------------------------------------------------------------------------
@pytest.fixture
def sample_stations_df():
    """Small DataFrame with 3 test stations (all_stations format)."""
    return pd.DataFrame(
        {
            "code": ["99001", "99002", "99003"],
            "station_labels": ["Test River A", "Test River B", "Test River C"],
            "river_ru": ["Река A", "Река B", "Река C"],
            "punkt_ru": ["Пункт A", "Пункт B", "Пункт C"],
            "basin": ["Basin1", "Basin1", "Basin2"],
            "bulletin_order": [2, 1, 1],
        }
    )


@pytest.fixture
def sample_forecast_df():
    """Forecast DataFrame mimicking the union of ML + LR data."""
    dates = pd.to_datetime(["2026-03-01", "2026-03-01", "2026-03-01", "2026-03-01"])
    return pd.DataFrame(
        {
            "code": ["99001", "99001", "99001", "99001"],
            "date": dates,
            "Date": dates + pd.Timedelta(days=1),
            "year": [2026, 2026, 2026, 2026],
            "station_labels": [
                "99001 - Test River A",
                "99001 - Test River A",
                "99001 - Test River A",
                "99001 - Test River A",
            ],
            "pentad_in_year": [13, 13, 13, 13],
            "pentad_in_month": [3, 3, 3, 3],
            "model_short": ["TFT", "TiDE", "NE", "LR"],
            "model_long": [
                "Temporal Fusion Transformer (TFT)",
                "Temporal Improved Diverse Ensemble (TiDE)",
                "Neural Ensemble (NE)",
                "Linear regression (LR)",
            ],
            "forecasted_discharge": [10.5, 11.0, 10.75, 9.8],
            "Q5": [8.0, 8.5, 8.25, np.nan],
            "Q25": [9.0, 9.5, 9.25, np.nan],
            "Q75": [12.0, 12.5, 12.25, np.nan],
            "Q95": [13.0, 13.5, 13.25, np.nan],
            "E[Q]": [10.5, 11.0, 10.75, np.nan],
            "delta": [1.2, 1.3, 1.25, 0.9],
            "accuracy": [85.0, 82.0, 88.0, 78.0],
            "flag": [0, 0, 0, None],
        }
    )


@pytest.fixture
def sample_skill_df():
    """Skill metrics DataFrame."""
    return pd.DataFrame(
        {
            "code": ["99001", "99001", "99001"],
            "pentad_in_year": [13, 13, 13],
            "model_short": ["TFT", "TiDE", "NE"],
            "model_long": [
                "Temporal Fusion Transformer (TFT)",
                "Temporal Improved Diverse Ensemble (TiDE)",
                "Neural Ensemble (NE)",
            ],
            "sdivsigma": [0.55, 0.62, 0.48],
            "nse": [0.78, 0.72, 0.82],
            "mae": [1.1, 1.4, 0.9],
            "accuracy": [85.0, 82.0, 88.0],
            "n_pairs": [12, 12, 12],
        }
    )


# ---------------------------------------------------------------------------
# Mock API response factory
# ---------------------------------------------------------------------------
@pytest.fixture
def mock_api_response():
    """Factory fixture returning fake ``requests.Response`` objects."""

    def _make(json_data, status_code=200):
        resp = MagicMock()
        resp.status_code = status_code
        resp.json.return_value = json_data
        resp.raise_for_status.return_value = None
        return resp

    return _make


# ---------------------------------------------------------------------------
# Fixture-file loaders
# ---------------------------------------------------------------------------
@pytest.fixture
def forecast_response_json():
    """Load forecast fixture JSON (ML forecasts from postprocessing API)."""
    path = FIXTURES_DIR / "forecast_response.json"
    with open(path) as f:
        return json.load(f)


@pytest.fixture
def lr_forecast_response_json():
    """Load LR forecast fixture JSON."""
    path = FIXTURES_DIR / "lr_forecast_response.json"
    with open(path) as f:
        return json.load(f)


@pytest.fixture
def skill_metric_response_json():
    """Load skill-metric fixture JSON."""
    path = FIXTURES_DIR / "skill_metric_response.json"
    with open(path) as f:
        return json.load(f)
