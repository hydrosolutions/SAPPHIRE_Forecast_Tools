"""Tests for quarterly/seasonal data reader functions.

Phase 4b Step 2.
"""

import os
import sys
from unittest.mock import patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src import data_reader

# ===================================================================
# read_skill_metrics() extended dispatch
# ===================================================================


class TestReadSkillMetricsDispatch:
    def test_quarter_accepted(self):
        """'quarter' is a valid horizon_type (no ValueError)."""
        with patch.object(data_reader, "read_quarterly_skill_metrics", return_value=pd.DataFrame()):
            result = data_reader.read_skill_metrics("quarter")
            assert isinstance(result, pd.DataFrame)

    def test_season_accepted(self):
        with patch.object(data_reader, "read_seasonal_skill_metrics", return_value=pd.DataFrame()):
            result = data_reader.read_skill_metrics("season")
            assert isinstance(result, pd.DataFrame)

    def test_invalid_horizon_type(self):
        with pytest.raises(ValueError, match="horizon_type must be one of"):
            data_reader.read_skill_metrics("weekly")


# ===================================================================
# Quarterly observations — delegation to monthly + aggregation
# ===================================================================


class TestReadQuarterlyObservations:
    def test_delegates_to_monthly(self):
        """read_quarterly_observations calls read_monthly_observations."""
        monthly = pd.DataFrame(
            {
                "code": ["S1"] * 6,
                "year": [2024] * 6,
                "month": [1, 2, 3, 4, 5, 6],
                "discharge_avg": [100, 110, 120, 50, 60, 70],
            }
        )
        with patch.object(data_reader, "read_monthly_observations", return_value=monthly):
            result = data_reader.read_quarterly_observations(["S1"], 2024, 2024)
        assert not result.empty
        assert "quarter_in_year" in result.columns
        assert "discharge_avg" in result.columns
        assert "delta" in result.columns

    def test_empty_monthly_returns_empty(self):
        with patch.object(data_reader, "read_monthly_observations", return_value=pd.DataFrame()):
            result = data_reader.read_quarterly_observations(["S1"], 2024, 2024)
        assert result.empty
        assert "quarter_in_year" in result.columns


# ===================================================================
# Seasonal observations
# ===================================================================


class TestReadSeasonalObservations:
    def test_delegates_to_monthly(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SEASON_START_MONTH", raising=False)
        monkeypatch.delenv("SAPPHIRE_SEASON_END_MONTH", raising=False)
        monthly = pd.DataFrame(
            {
                "code": ["S1"] * 6,
                "year": [2024] * 6,
                "month": [4, 5, 6, 7, 8, 9],
                "discharge_avg": [100, 110, 120, 130, 140, 150],
            }
        )
        with patch.object(data_reader, "read_monthly_observations", return_value=monthly):
            result = data_reader.read_seasonal_observations(["S1"], 2024, 2024)
        assert not result.empty
        assert "season_year" in result.columns
        assert "season_in_year" in result.columns


# ===================================================================
# Quarterly forecasts
# ===================================================================


class TestReadQuarterlyForecasts:
    def test_delegates_to_monthly(self):
        monthly = pd.DataFrame(
            {
                "code": ["S1"] * 3,
                "year": [2024] * 3,
                "month": [1, 2, 3],
                "model_short": ["M1"] * 3,
                "q05": [10, 20, 30],
                "q10": [15, 25, 35],
                "q25": [20, 30, 40],
                "q50": [30, 40, 50],
                "q75": [40, 50, 60],
                "q90": [50, 60, 70],
                "q95": [60, 70, 80],
                "forecasted_discharge": [30, 40, 50],
            }
        )
        with patch.object(data_reader, "read_monthly_forecasts", return_value=monthly):
            result = data_reader.read_quarterly_forecasts(["S1"], 2024, 2024)
        assert not result.empty
        assert "quarter_in_year" in result.columns
        assert "model_short" in result.columns

    def test_empty_monthly_returns_empty(self):
        with patch.object(data_reader, "read_monthly_forecasts", return_value=pd.DataFrame()):
            result = data_reader.read_quarterly_forecasts(["S1"], 2024, 2024)
        assert result.empty


# ===================================================================
# Seasonal forecasts
# ===================================================================


class TestReadSeasonalForecasts:
    def test_delegates_to_monthly(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SEASON_START_MONTH", raising=False)
        monkeypatch.delenv("SAPPHIRE_SEASON_END_MONTH", raising=False)
        monthly = pd.DataFrame(
            {
                "code": ["S1"] * 3,
                "year": [2024] * 3,
                "month": [4, 5, 6],
                "model_short": ["M1"] * 3,
                "q05": [10, 20, 30],
                "q10": [15, 25, 35],
                "q25": [20, 30, 40],
                "q50": [30, 40, 50],
                "q75": [40, 50, 60],
                "q90": [50, 60, 70],
                "q95": [60, 70, 80],
                "forecasted_discharge": [30, 40, 50],
            }
        )
        with patch.object(data_reader, "read_monthly_forecasts", return_value=monthly):
            result = data_reader.read_seasonal_forecasts(["S1"], 2024, 2024)
        assert not result.empty
        assert "season_year" in result.columns


# ===================================================================
# Latest quarterly/seasonal forecasts
# ===================================================================


class TestReadLatestQuarterlyForecasts:
    def test_returns_most_recent_quarter(self):
        """Filters to the most recent quarter after aggregation."""
        raw_api = pd.DataFrame(
            {
                "code": ["S1"] * 6,
                "valid_from": pd.to_datetime(
                    [
                        "2024-01-01",
                        "2024-02-01",
                        "2024-03-01",
                        "2024-04-01",
                        "2024-05-01",
                        "2024-06-01",
                    ]
                ),
                "valid_to": pd.to_datetime(
                    [
                        "2024-01-31",
                        "2024-02-29",
                        "2024-03-31",
                        "2024-04-30",
                        "2024-05-31",
                        "2024-06-30",
                    ]
                ),
                "model_type": ["M1"] * 6,
                "q50": [100, 110, 120, 50, 60, 70],
                "q05": [80, 90, 100, 30, 40, 50],
                "q10": [85, 95, 105, 35, 45, 55],
                "q25": [90, 100, 110, 40, 50, 60],
                "q75": [110, 120, 130, 60, 70, 80],
                "q90": [120, 130, 140, 70, 80, 90],
                "q95": [130, 140, 150, 80, 90, 100],
            }
        )
        with patch.object(data_reader, "_read_long_forecasts_api", return_value=raw_api):
            import datetime as dt

            result = data_reader.read_latest_quarterly_forecasts(
                ["S1"], forecast_date=dt.date(2024, 7, 1)
            )
        assert not result.empty
        # Should be Q2 (latest quarter with data)
        assert all(result["quarter_in_year"] == 2)

    def test_empty_api_returns_empty(self):
        with patch.object(data_reader, "_read_long_forecasts_api", return_value=None):
            result = data_reader.read_latest_quarterly_forecasts(["S1"])
        assert result.empty


class TestReadLatestSeasonalForecasts:
    def test_empty_api_returns_empty(self):
        with patch.object(data_reader, "_read_long_forecasts_api", return_value=None):
            result = data_reader.read_latest_seasonal_forecasts(["S1"])
        assert result.empty


# ===================================================================
# Combined forecasts from API
# ===================================================================


class TestReadQuarterlyCombinedForecasts:
    def test_returns_empty_when_api_unavailable(self):
        with patch.object(data_reader, "_read_long_combined_forecasts_api", return_value=None):
            result = data_reader.read_quarterly_combined_forecasts()
        assert result.empty

    def test_returns_data_when_api_available(self):
        mock_df = pd.DataFrame(
            {
                "code": ["S1"],
                "year": [2024],
                "quarter_in_year": [1],
                "model_short": ["EM"],
                "forecasted_discharge": [100.0],
            }
        )
        with patch.object(data_reader, "_read_long_combined_forecasts_api", return_value=mock_df):
            result = data_reader.read_quarterly_combined_forecasts()
        assert len(result) == 1


class TestReadSeasonalCombinedForecasts:
    def test_returns_empty_when_api_unavailable(self):
        with patch.object(data_reader, "_read_long_combined_forecasts_api", return_value=None):
            result = data_reader.read_seasonal_combined_forecasts()
        assert result.empty
