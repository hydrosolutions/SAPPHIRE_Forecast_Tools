"""Tests for src/aggregation.py — quarter/season definitions and aggregation.

Phase 4b Step 1.
"""

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.aggregation import (
    MONTH_TO_QUARTER,
    QUARTER_MIN_MONTHS,
    QUARTER_MONTHS,
    aggregate_monthly_fc_to_quarterly,
    aggregate_monthly_obs_to_quarterly,
    aggregate_monthly_obs_to_seasonal,
    get_season_months,
    get_season_year,
)

# ===================================================================
# Quarter constants
# ===================================================================


class TestQuarterConstants:
    def test_quarter_months_covers_all_months(self):
        all_months = sorted(m for ms in QUARTER_MONTHS.values() for m in ms)
        assert all_months == list(range(1, 13))

    def test_month_to_quarter_mapping(self):
        assert MONTH_TO_QUARTER[1] == 1
        assert MONTH_TO_QUARTER[4] == 2
        assert MONTH_TO_QUARTER[7] == 3
        assert MONTH_TO_QUARTER[12] == 4

    def test_quarter_min_months(self):
        assert QUARTER_MIN_MONTHS == 2


# ===================================================================
# Season helpers
# ===================================================================


class TestGetSeasonMonths:
    def test_default_season(self, monkeypatch):
        """Default season is Apr-Sep."""
        monkeypatch.delenv("SAPPHIRE_SEASON_START_MONTH", raising=False)
        monkeypatch.delenv("SAPPHIRE_SEASON_END_MONTH", raising=False)
        assert get_season_months() == [4, 5, 6, 7, 8, 9]

    def test_custom_season(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_SEASON_START_MONTH", "5")
        monkeypatch.setenv("SAPPHIRE_SEASON_END_MONTH", "8")
        assert get_season_months() == [5, 6, 7, 8]

    def test_cross_year_season(self, monkeypatch):
        """Oct-Mar wraps across year boundary."""
        monkeypatch.setenv("SAPPHIRE_SEASON_START_MONTH", "10")
        monkeypatch.setenv("SAPPHIRE_SEASON_END_MONTH", "3")
        assert get_season_months() == [10, 11, 12, 1, 2, 3]

    def test_single_month_season(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_SEASON_START_MONTH", "6")
        monkeypatch.setenv("SAPPHIRE_SEASON_END_MONTH", "6")
        assert get_season_months() == [6]


class TestGetSeasonYear:
    def test_non_crossing_season(self, monkeypatch):
        """Apr-Sep: all months belong to their calendar year."""
        monkeypatch.delenv("SAPPHIRE_SEASON_START_MONTH", raising=False)
        monkeypatch.delenv("SAPPHIRE_SEASON_END_MONTH", raising=False)
        assert get_season_year(2024, 4) == 2024
        assert get_season_year(2024, 9) == 2024

    def test_cross_year_season_first_half(self, monkeypatch):
        """Oct-Mar: Oct-Dec belong to that calendar year."""
        monkeypatch.setenv("SAPPHIRE_SEASON_START_MONTH", "10")
        monkeypatch.setenv("SAPPHIRE_SEASON_END_MONTH", "3")
        assert get_season_year(2024, 10) == 2024
        assert get_season_year(2024, 12) == 2024

    def test_cross_year_season_second_half(self, monkeypatch):
        """Oct-Mar: Jan-Mar of 2025 belong to season_year 2024."""
        monkeypatch.setenv("SAPPHIRE_SEASON_START_MONTH", "10")
        monkeypatch.setenv("SAPPHIRE_SEASON_END_MONTH", "3")
        assert get_season_year(2025, 1) == 2024
        assert get_season_year(2025, 3) == 2024


# ===================================================================
# Helper to build test DataFrames
# ===================================================================


def _make_monthly_obs(rows):
    """Build monthly obs DataFrame from (code, year, month, discharge_avg)."""
    return pd.DataFrame(rows, columns=["code", "year", "month", "discharge_avg"])


def _make_monthly_fc(rows, with_quantiles=True):
    """Build monthly forecast DataFrame.

    Each row: (code, year, month, model_short, q50) or with full quantiles.
    """
    if with_quantiles:
        cols = [
            "code",
            "year",
            "month",
            "model_short",
            "q05",
            "q10",
            "q25",
            "q50",
            "q75",
            "q90",
            "q95",
        ]
    else:
        cols = ["code", "year", "month", "model_short", "q50"]
    df = pd.DataFrame(rows, columns=cols)
    if "forecasted_discharge" not in df.columns and "q50" in df.columns:
        df["forecasted_discharge"] = df["q50"].astype(float)
    return df


# ===================================================================
# Quarterly observation aggregation
# ===================================================================


class TestAggregateMonthlyObsToQuarterly:
    def test_basic_aggregation(self):
        """3 months in Q1 → single quarterly row with mean discharge."""
        obs = _make_monthly_obs(
            [
                ("S1", 2024, 1, 100.0),
                ("S1", 2024, 2, 110.0),
                ("S1", 2024, 3, 120.0),
            ]
        )
        result = aggregate_monthly_obs_to_quarterly(obs)
        assert len(result) == 1
        assert result.iloc[0]["quarter_in_year"] == 1
        assert abs(result.iloc[0]["discharge_avg"] - 110.0) < 1e-6

    def test_coverage_filter(self):
        """Only 1 month in quarter → filtered out (need >=2)."""
        obs = _make_monthly_obs(
            [
                ("S1", 2024, 1, 100.0),
            ]
        )
        result = aggregate_monthly_obs_to_quarterly(obs)
        assert result.empty

    def test_two_months_passes(self):
        """2 months in quarter passes the filter."""
        obs = _make_monthly_obs(
            [
                ("S1", 2024, 4, 50.0),
                ("S1", 2024, 5, 60.0),
            ]
        )
        result = aggregate_monthly_obs_to_quarterly(obs)
        assert len(result) == 1
        assert result.iloc[0]["quarter_in_year"] == 2
        assert abs(result.iloc[0]["discharge_avg"] - 55.0) < 1e-6

    def test_delta_computation(self):
        """Delta = 0.674 * std across years for same quarter."""
        obs = _make_monthly_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2020, 2, 100.0),
                ("S1", 2020, 3, 100.0),
                ("S1", 2021, 1, 120.0),
                ("S1", 2021, 2, 120.0),
                ("S1", 2021, 3, 120.0),
            ]
        )
        result = aggregate_monthly_obs_to_quarterly(obs)
        assert len(result) == 2
        # mean per year: 100, 120 → std = ~14.14
        expected_delta = 0.674 * np.std([100.0, 120.0], ddof=1)
        assert abs(result.iloc[0]["delta"] - expected_delta) < 1e-4

    def test_multiple_stations(self):
        """Each station aggregated independently."""
        obs = _make_monthly_obs(
            [
                ("S1", 2024, 1, 100.0),
                ("S1", 2024, 2, 110.0),
                ("S2", 2024, 1, 200.0),
                ("S2", 2024, 2, 220.0),
            ]
        )
        result = aggregate_monthly_obs_to_quarterly(obs)
        assert len(result) == 2
        codes = set(result["code"])
        assert codes == {"S1", "S2"}

    def test_empty_input(self):
        result = aggregate_monthly_obs_to_quarterly(pd.DataFrame())
        assert result.empty
        assert "quarter_in_year" in result.columns

    def test_multiple_quarters(self):
        """Data in Q1 and Q3, each with enough months."""
        obs = _make_monthly_obs(
            [
                ("S1", 2024, 1, 100.0),
                ("S1", 2024, 2, 110.0),
                ("S1", 2024, 7, 50.0),
                ("S1", 2024, 8, 60.0),
                ("S1", 2024, 9, 55.0),
            ]
        )
        result = aggregate_monthly_obs_to_quarterly(obs)
        assert len(result) == 2
        quarters = set(result["quarter_in_year"])
        assert quarters == {1, 3}


# ===================================================================
# Seasonal observation aggregation
# ===================================================================


class TestAggregateMonthlyObsToSeasonal:
    def test_default_season(self, monkeypatch):
        """Default Apr-Sep season aggregation."""
        monkeypatch.delenv("SAPPHIRE_SEASON_START_MONTH", raising=False)
        monkeypatch.delenv("SAPPHIRE_SEASON_END_MONTH", raising=False)
        obs = _make_monthly_obs(
            [
                ("S1", 2024, 4, 100.0),
                ("S1", 2024, 5, 110.0),
                ("S1", 2024, 6, 120.0),
                ("S1", 2024, 7, 130.0),
                ("S1", 2024, 8, 140.0),
                ("S1", 2024, 9, 150.0),
            ]
        )
        result = aggregate_monthly_obs_to_seasonal(obs)
        assert len(result) == 1
        assert result.iloc[0]["season_year"] == 2024
        assert result.iloc[0]["season_in_year"] == 1
        expected_mean = np.mean([100, 110, 120, 130, 140, 150])
        assert abs(result.iloc[0]["discharge_avg"] - expected_mean) < 1e-6

    def test_cross_year_season(self, monkeypatch):
        """Oct-Mar season spans two calendar years."""
        monkeypatch.setenv("SAPPHIRE_SEASON_START_MONTH", "10")
        monkeypatch.setenv("SAPPHIRE_SEASON_END_MONTH", "3")
        obs = _make_monthly_obs(
            [
                ("S1", 2024, 10, 50.0),
                ("S1", 2024, 11, 40.0),
                ("S1", 2024, 12, 30.0),
                ("S1", 2025, 1, 25.0),
                ("S1", 2025, 2, 35.0),
                ("S1", 2025, 3, 45.0),
            ]
        )
        result = aggregate_monthly_obs_to_seasonal(obs)
        assert len(result) == 1
        assert result.iloc[0]["season_year"] == 2024
        expected_mean = np.mean([50, 40, 30, 25, 35, 45])
        assert abs(result.iloc[0]["discharge_avg"] - expected_mean) < 1e-6

    def test_coverage_filter(self, monkeypatch):
        """Less than 50% coverage → filtered out."""
        monkeypatch.delenv("SAPPHIRE_SEASON_START_MONTH", raising=False)
        monkeypatch.delenv("SAPPHIRE_SEASON_END_MONTH", raising=False)
        # Default 6-month season, need >= 3 months
        obs = _make_monthly_obs(
            [
                ("S1", 2024, 4, 100.0),
                ("S1", 2024, 5, 110.0),
            ]
        )
        result = aggregate_monthly_obs_to_seasonal(obs)
        assert result.empty

    def test_non_season_months_excluded(self, monkeypatch):
        """Months outside the season are ignored."""
        monkeypatch.delenv("SAPPHIRE_SEASON_START_MONTH", raising=False)
        monkeypatch.delenv("SAPPHIRE_SEASON_END_MONTH", raising=False)
        obs = _make_monthly_obs(
            [
                ("S1", 2024, 1, 999.0),  # not in season
                ("S1", 2024, 2, 999.0),  # not in season
                ("S1", 2024, 4, 100.0),
                ("S1", 2024, 5, 110.0),
                ("S1", 2024, 6, 120.0),
            ]
        )
        result = aggregate_monthly_obs_to_seasonal(obs)
        assert len(result) == 1
        # Only season months should be averaged
        expected_mean = np.mean([100, 110, 120])
        assert abs(result.iloc[0]["discharge_avg"] - expected_mean) < 1e-6

    def test_empty_input(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SEASON_START_MONTH", raising=False)
        monkeypatch.delenv("SAPPHIRE_SEASON_END_MONTH", raising=False)
        result = aggregate_monthly_obs_to_seasonal(pd.DataFrame())
        assert result.empty
        assert "season_year" in result.columns

    def test_delta_computation(self, monkeypatch):
        """Delta computed per code across season_years."""
        monkeypatch.delenv("SAPPHIRE_SEASON_START_MONTH", raising=False)
        monkeypatch.delenv("SAPPHIRE_SEASON_END_MONTH", raising=False)
        obs = _make_monthly_obs(
            [
                ("S1", 2020, 4, 100.0),
                ("S1", 2020, 5, 100.0),
                ("S1", 2020, 6, 100.0),
                ("S1", 2021, 4, 120.0),
                ("S1", 2021, 5, 120.0),
                ("S1", 2021, 6, 120.0),
            ]
        )
        result = aggregate_monthly_obs_to_seasonal(obs)
        assert len(result) == 2
        expected_delta = 0.674 * np.std([100.0, 120.0], ddof=1)
        assert abs(result.iloc[0]["delta"] - expected_delta) < 1e-4


# ===================================================================
# Quarterly forecast aggregation
# ===================================================================


class TestAggregateMonthlyFcToQuarterly:
    def test_basic_quantile_averaging(self):
        fc = _make_monthly_fc(
            [
                ("S1", 2024, 1, "M1", 10, 20, 30, 40, 50, 60, 70),
                ("S1", 2024, 2, "M1", 20, 30, 40, 50, 60, 70, 80),
                ("S1", 2024, 3, "M1", 30, 40, 50, 60, 70, 80, 90),
            ]
        )
        result = aggregate_monthly_fc_to_quarterly(fc)
        assert len(result) == 1
        assert abs(result.iloc[0]["q50"] - 50.0) < 1e-6
        assert abs(result.iloc[0]["q05"] - 20.0) < 1e-6

    def test_coverage_filter(self):
        """Only 1 month → filtered out."""
        fc = _make_monthly_fc(
            [
                ("S1", 2024, 1, "M1", 10, 20, 30, 40, 50, 60, 70),
            ]
        )
        result = aggregate_monthly_fc_to_quarterly(fc)
        assert result.empty

    def test_valid_from_valid_to(self):
        fc = _make_monthly_fc(
            [
                ("S1", 2024, 4, "M1", 10, 20, 30, 40, 50, 60, 70),
                ("S1", 2024, 5, "M1", 20, 30, 40, 50, 60, 70, 80),
            ]
        )
        result = aggregate_monthly_fc_to_quarterly(fc)
        assert result.iloc[0]["valid_from"] == "2024-04-01"
        assert result.iloc[0]["valid_to"] == "2024-06-30"

    def test_multiple_models(self):
        fc = _make_monthly_fc(
            [
                ("S1", 2024, 1, "M1", 10, 20, 30, 40, 50, 60, 70),
                ("S1", 2024, 2, "M1", 20, 30, 40, 50, 60, 70, 80),
                ("S1", 2024, 1, "M2", 15, 25, 35, 45, 55, 65, 75),
                ("S1", 2024, 2, "M2", 25, 35, 45, 55, 65, 75, 85),
            ]
        )
        result = aggregate_monthly_fc_to_quarterly(fc)
        assert len(result) == 2
        models = set(result["model_short"])
        assert models == {"M1", "M2"}

    def test_empty_input(self):
        result = aggregate_monthly_fc_to_quarterly(pd.DataFrame())
        assert result.empty

    def test_forecasted_discharge_from_q50(self):
        """forecasted_discharge synthesized from q50 if not present."""
        fc = _make_monthly_fc(
            [
                ("S1", 2024, 1, "M1", 10, 20, 30, 40, 50, 60, 70),
                ("S1", 2024, 2, "M1", 20, 30, 40, 50, 60, 70, 80),
            ]
        )
        # Remove forecasted_discharge to test synthesis
        fc = fc.drop(columns=["forecasted_discharge"])
        result = aggregate_monthly_fc_to_quarterly(fc)
        assert "forecasted_discharge" in result.columns
        assert abs(result.iloc[0]["forecasted_discharge"] - 45.0) < 1e-6
