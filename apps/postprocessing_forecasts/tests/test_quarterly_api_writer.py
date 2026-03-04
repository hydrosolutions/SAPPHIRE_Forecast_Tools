"""Tests for quarterly/seasonal extensions to api_writer.py.

Phase 4b Step 5.
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.api_writer import (
    HORIZON_TYPE_TO_API,
    _write_quarterly_ensemble_to_api,
    _write_seasonal_ensemble_to_api,
    _write_skill_metrics_to_api,
)

# ===================================================================
# Horizon mapping
# ===================================================================


class TestHorizonMapping:
    def test_quarter_in_mapping(self):
        assert "quarter" in HORIZON_TYPE_TO_API
        assert HORIZON_TYPE_TO_API["quarter"] == "quarter"

    def test_season_in_mapping(self):
        assert "season" in HORIZON_TYPE_TO_API
        assert HORIZON_TYPE_TO_API["season"] == "season"


# ===================================================================
# Skill metrics writer — quarter/season dispatch
# ===================================================================


class TestSkillMetricsQuarterSeason:
    @pytest.fixture(autouse=True)
    def _mock_api(self, monkeypatch):
        """Mock API client for all tests in this class."""
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")
        self.mock_client = MagicMock()
        self.mock_client.readiness_check.return_value = True
        self.mock_client.write_skill_metrics.return_value = 1

    def test_quarter_horizon_accepted(self):
        """_write_skill_metrics_to_api accepts horizon_type='quarter'."""
        data = pd.DataFrame(
            {
                "quarter_in_year": [1],
                "code": ["S1"],
                "model_short": ["LR"],
                "sdivsigma": [0.5],
                "nse": [0.9],
                "delta": [5.0],
                "accuracy": [0.85],
                "mae": [2.0],
                "n_pairs": [10],
            }
        )
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            result = _write_skill_metrics_to_api(data, "quarter", 2025)
        assert result is True
        # Verify the record has correct horizon_type
        records = self.mock_client.write_skill_metrics.call_args[0][0]
        assert records[0]["horizon_type"] == "quarter"

    def test_quarter_date_computation(self):
        """Quarter skill metric date should be first day of quarter."""
        data = pd.DataFrame(
            {
                "quarter_in_year": [2],
                "code": ["S1"],
                "model_short": ["LR"],
                "sdivsigma": [0.5],
                "nse": [0.9],
                "delta": [5.0],
                "accuracy": [0.85],
                "mae": [2.0],
                "n_pairs": [10],
            }
        )
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            _write_skill_metrics_to_api(data, "quarter", 2025)
        records = self.mock_client.write_skill_metrics.call_args[0][0]
        # Q2 → April 1
        assert records[0]["date"] == "2025-04-01"

    def test_season_horizon_accepted(self):
        """_write_skill_metrics_to_api accepts horizon_type='season'."""
        data = pd.DataFrame(
            {
                "season_in_year": [1],
                "code": ["S1"],
                "model_short": ["LR"],
                "sdivsigma": [0.5],
                "nse": [0.9],
                "delta": [5.0],
                "accuracy": [0.85],
                "mae": [2.0],
                "n_pairs": [10],
            }
        )
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            result = _write_skill_metrics_to_api(data, "season", 2025)
        assert result is True
        records = self.mock_client.write_skill_metrics.call_args[0][0]
        assert records[0]["horizon_type"] == "season"

    def test_season_date_uses_season_start(self, monkeypatch):
        """Season date should be first day of season start month."""
        monkeypatch.setenv("SAPPHIRE_SEASON_START_MONTH", "4")
        monkeypatch.setenv("SAPPHIRE_SEASON_END_MONTH", "9")
        data = pd.DataFrame(
            {
                "season_in_year": [1],
                "code": ["S1"],
                "model_short": ["LR"],
                "sdivsigma": [0.5],
                "nse": [0.9],
                "delta": [5.0],
                "accuracy": [0.85],
                "mae": [2.0],
                "n_pairs": [10],
            }
        )
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            _write_skill_metrics_to_api(data, "season", 2025)
        records = self.mock_client.write_skill_metrics.call_args[0][0]
        assert records[0]["date"] == "2025-04-01"


# ===================================================================
# Quarterly ensemble writer
# ===================================================================


class TestQuarterlyEnsembleWriter:
    @pytest.fixture(autouse=True)
    def _mock_api(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")
        self.mock_client = MagicMock()
        self.mock_client.readiness_check.return_value = True
        self.mock_client.write_long_forecasts.return_value = 2

    def test_writes_ensemble_rows(self):
        data = pd.DataFrame(
            {
                "code": ["S1", "S1"],
                "year": [2025, 2025],
                "quarter_in_year": [1, 1],
                "model_short": ["EM", "Naive Mean"],
                "forecasted_discharge": [100.0, 95.0],
                "q50": [100.0, 95.0],
            }
        )
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            result = _write_quarterly_ensemble_to_api(data)
        assert result is True
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert len(records) == 2
        assert records[0]["horizon_type"] == "quarter"

    def test_valid_from_valid_to(self):
        data = pd.DataFrame(
            {
                "code": ["S1"],
                "year": [2025],
                "quarter_in_year": [2],
                "model_short": ["EM"],
                "forecasted_discharge": [100.0],
            }
        )
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            _write_quarterly_ensemble_to_api(data)
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert records[0]["valid_from"] == "2025-04-01"
        assert records[0]["valid_to"] == "2025-06-30"
        assert records[0]["horizon_value"] == 2

    def test_empty_data_returns_false(self):
        result = _write_quarterly_ensemble_to_api(pd.DataFrame())
        assert result is False


# ===================================================================
# Seasonal ensemble writer
# ===================================================================


class TestSeasonalEnsembleWriter:
    @pytest.fixture(autouse=True)
    def _mock_api(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")
        monkeypatch.delenv("SAPPHIRE_SEASON_START_MONTH", raising=False)
        monkeypatch.delenv("SAPPHIRE_SEASON_END_MONTH", raising=False)
        self.mock_client = MagicMock()
        self.mock_client.readiness_check.return_value = True
        self.mock_client.write_long_forecasts.return_value = 1

    def test_writes_ensemble_rows(self):
        data = pd.DataFrame(
            {
                "code": ["S1"],
                "season_year": [2025],
                "season_in_year": [1],
                "model_short": ["EM"],
                "forecasted_discharge": [100.0],
            }
        )
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            result = _write_seasonal_ensemble_to_api(data)
        assert result is True
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert records[0]["horizon_type"] == "season"
        assert records[0]["horizon_value"] == 1

    def test_valid_from_valid_to_default_season(self):
        """Default season Apr-Sep → valid_from=Apr 1, valid_to=Sep 30."""
        data = pd.DataFrame(
            {
                "code": ["S1"],
                "season_year": [2025],
                "season_in_year": [1],
                "model_short": ["EM"],
                "forecasted_discharge": [100.0],
            }
        )
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            _write_seasonal_ensemble_to_api(data)
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert records[0]["valid_from"] == "2025-04-01"
        assert records[0]["valid_to"] == "2025-09-30"

    def test_cross_year_season(self, monkeypatch):
        """Oct-Mar season crosses year boundary."""
        monkeypatch.setenv("SAPPHIRE_SEASON_START_MONTH", "10")
        monkeypatch.setenv("SAPPHIRE_SEASON_END_MONTH", "3")
        data = pd.DataFrame(
            {
                "code": ["S1"],
                "season_year": [2024],
                "season_in_year": [1],
                "model_short": ["EM"],
                "forecasted_discharge": [100.0],
            }
        )
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            _write_seasonal_ensemble_to_api(data)
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert records[0]["valid_from"] == "2024-10-01"
        assert records[0]["valid_to"] == "2025-03-31"

    def test_empty_data_returns_false(self):
        result = _write_seasonal_ensemble_to_api(pd.DataFrame())
        assert result is False
