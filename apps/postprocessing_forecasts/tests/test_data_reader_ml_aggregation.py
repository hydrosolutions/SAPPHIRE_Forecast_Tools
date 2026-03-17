"""Tests for period-aware ML daily target aggregation (PP-023).

Verifies that _normalize_ml_forecasts() filters daily targets to the
correct pentad/decade before averaging, rather than averaging all targets
indiscriminately.
"""

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.data_reader import _normalize_ml_forecasts


class TestPentadTargetFiltering:
    """Pentad: only targets within the pentad are averaged."""

    def test_pentad_filters_targets_to_period(self):
        """6 daily targets spanning two pentads — only 5 in-period kept."""
        # Boundary date Jan 5 → pentad covers Jan 6-10 (pentad 2)
        # 6 targets: Jan 6-11; Jan 11 is pentad 3 → filtered out
        raw = pd.DataFrame(
            {
                "code": ["10001"] * 6,
                "date": ["2024-01-05"] * 6,
                "target": [
                    "2024-01-06",
                    "2024-01-07",
                    "2024-01-08",
                    "2024-01-09",
                    "2024-01-10",  # in pentad 2
                    "2024-01-11",  # pentad 3 → should be filtered
                ],
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0, 600.0],
                "q05": [5.0, 10.0, 15.0, 20.0, 25.0, 300.0],
                "q95": [15.0, 30.0, 45.0, 60.0, 75.0, 900.0],
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        # Mean of 5 in-period values: (10+20+30+40+50)/5 = 30.0
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(30.0)
        # q05 mean: (5+10+15+20+25)/5 = 15.0
        assert result["q05"].iloc[0] == pytest.approx(15.0)
        # q95 mean: (15+30+45+60+75)/5 = 45.0
        assert result["q95"].iloc[0] == pytest.approx(45.0)

    def test_pentad6_february_3_day_period(self):
        """Worst case: pentad 6 of Feb (3 days: 26-28), 3 of 6 targets outside."""
        # Boundary date Feb 25 → pentad 6 covers Feb 26-28 (non-leap)
        raw = pd.DataFrame(
            {
                "code": ["10001"] * 6,
                "date": ["2025-02-25"] * 6,
                "target": [
                    "2025-02-26",
                    "2025-02-27",
                    "2025-02-28",  # in pentad 6
                    "2025-03-01",
                    "2025-03-02",
                    "2025-03-03",  # next pentad
                ],
                "forecasted_discharge": [3.1, 3.2, 3.0, 2.9, 2.8, 2.7],
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        # Mean of 3 in-period values: (3.1+3.2+3.0)/3 ≈ 3.1
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(3.1, abs=0.01)

    def test_all_targets_in_period_no_filtering(self):
        """When all targets are within the period, nothing is dropped."""
        # Boundary date Jan 10 → pentad 3 covers Jan 11-15
        raw = pd.DataFrame(
            {
                "code": ["10001"] * 5,
                "date": ["2024-01-10"] * 5,
                "target": [
                    "2024-01-11",
                    "2024-01-12",
                    "2024-01-13",
                    "2024-01-14",
                    "2024-01-15",
                ],
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0],
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(30.0)


class TestDecadTargetFiltering:
    """Decade: only targets within the decade are averaged."""

    def test_decad_filters_targets_to_period(self):
        """11 daily targets spanning two decades — only 10 in-period kept."""
        # Boundary date Jan 10 → decade 2 covers Jan 11-20
        # 11 targets: Jan 11-21; Jan 21 is decade 3 → filtered out
        raw = pd.DataFrame(
            {
                "code": ["10001"] * 11,
                "date": ["2024-01-10"] * 11,
                "target": [f"2024-01-{d}" for d in range(11, 22)],
                "forecasted_discharge": [
                    10.0,
                    20.0,
                    30.0,
                    40.0,
                    50.0,
                    60.0,
                    70.0,
                    80.0,
                    90.0,
                    100.0,  # 10 in decade 2
                    1100.0,  # Jan 21 = decade 3 → filtered
                ],
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "decad")
        assert len(result) == 1
        # Mean of 10 in-period: (10+20+...+100)/10 = 55.0
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(55.0)

    def test_decad3_february_8_day_period(self):
        """Decade 3 of Feb (8 days: 21-28), 3 of 11 targets outside."""
        # Boundary date Feb 20 → decade 3 covers Feb 21-28 (non-leap)
        targets = [f"2025-02-{d}" for d in range(21, 29)]  # 8 in-period
        targets += ["2025-03-01", "2025-03-02", "2025-03-03"]  # 3 outside
        discharges = [10.0] * 8 + [100.0] * 3
        raw = pd.DataFrame(
            {
                "code": ["10001"] * 11,
                "date": ["2025-02-20"] * 11,
                "target": targets,
                "forecasted_discharge": discharges,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "decad")
        assert len(result) == 1
        # Mean of 8 in-period values: all 10.0 → 10.0
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(10.0)


class TestBackwardCompatibility:
    """Legacy data without target column still works."""

    def test_no_target_column_skips_filtering(self):
        """When target column is absent, all rows are averaged (existing behavior)."""
        raw = pd.DataFrame(
            {
                "code": ["10001"] * 6,
                "date": ["2024-01-05"] * 6,
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        # All 6 averaged: (10+20+30+40+50+60)/6 = 35.0
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(35.0)

    def test_empty_after_filter(self):
        """When no targets fall in the period, return empty DataFrame."""
        # Boundary date Jan 5 → pentad 2 (Jan 6-10)
        # All targets in pentad 3 (Jan 11+)
        raw = pd.DataFrame(
            {
                "code": ["10001"] * 3,
                "date": ["2024-01-05"] * 3,
                "target": ["2024-01-11", "2024-01-12", "2024-01-13"],
                "forecasted_discharge": [10.0, 20.0, 30.0],
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty


class TestQuantileFiltering:
    """Quantile columns are filtered identically to forecasted_discharge."""

    def test_quantiles_filtered_same_as_discharge(self):
        """q05, q25, q75, q95 use only in-period targets."""
        raw = pd.DataFrame(
            {
                "code": ["10001"] * 6,
                "date": ["2024-01-05"] * 6,
                "target": [
                    "2024-01-06",
                    "2024-01-07",
                    "2024-01-08",
                    "2024-01-09",
                    "2024-01-10",  # in pentad 2
                    "2024-01-11",  # filtered out
                ],
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0, 999.0],
                "q05": [1.0, 2.0, 3.0, 4.0, 5.0, 999.0],
                "q25": [2.0, 4.0, 6.0, 8.0, 10.0, 999.0],
                "q75": [20.0, 40.0, 60.0, 80.0, 100.0, 999.0],
                "q95": [30.0, 60.0, 90.0, 120.0, 150.0, 999.0],
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result["q05"].iloc[0] == pytest.approx(3.0)  # mean(1,2,3,4,5)
        assert result["q25"].iloc[0] == pytest.approx(6.0)  # mean(2,4,6,8,10)
        assert result["q75"].iloc[0] == pytest.approx(60.0)  # mean(20,40,60,80,100)
        assert result["q95"].iloc[0] == pytest.approx(90.0)  # mean(30,60,90,120,150)


class TestMultipleCodes:
    """Filtering is per-row, not per-group."""

    def test_multiple_codes_filtered_independently(self):
        """Each code's targets are filtered by its own boundary date."""
        raw = pd.DataFrame(
            {
                "code": ["10001"] * 3 + ["10002"] * 3,
                "date": ["2024-01-05"] * 3 + ["2024-01-05"] * 3,
                "target": [
                    "2024-01-06",
                    "2024-01-07",
                    "2024-01-11",  # 10001: 2 in, 1 out
                    "2024-01-06",
                    "2024-01-07",
                    "2024-01-08",  # 10002: 3 in
                ],
                "forecasted_discharge": [10.0, 20.0, 999.0, 100.0, 200.0, 300.0],
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 2
        r1 = result[result["code"] == "10001"]
        r2 = result[result["code"] == "10002"]
        assert r1["forecasted_discharge"].iloc[0] == pytest.approx(15.0)  # (10+20)/2
        assert r2["forecasted_discharge"].iloc[0] == pytest.approx(200.0)  # (100+200+300)/3
