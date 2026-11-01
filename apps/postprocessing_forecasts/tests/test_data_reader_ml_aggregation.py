"""Tests for period-aware ML daily target aggregation (PP-023, PP-031).

Verifies that _normalize_ml_forecasts() filters daily targets to the
correct pentad/decade before averaging, rather than averaging all targets
indiscriminately.
"""

import datetime as dt
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import time
from unittest.mock import MagicMock

from src.data_reader import (
    RECOVERY_STATUS_COMPLETE,
    RECOVERY_STATUS_DISABLED,
    RECOVERY_STATUS_EMPTY,
    RECOVERY_STATUS_FAILED,
    SAPPHIRE_API_AVAILABLE,
    _apply_day_coverage_filter,
    _is_decad_boundary,
    _is_pentad_boundary,
    _normalize_ml_forecasts,
    read_combined_period_forecasts_for_recovery,
    read_day_forecasts_for_recovery,
    read_lr_forecasts_for_recovery,
)
from src.gap_detector import plan_period_refresh


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
                "q25": [2.0, 4.0, 6.0, 8.0, 10.0, 200.0],
                "q75": [8.0, 16.0, 24.0, 32.0, 40.0, 800.0],
                "q95": [15.0, 30.0, 45.0, 60.0, 75.0, 900.0],
                "horizon_type": ["day"] * 6,
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

    def test_mixed_day_and_period_rows_keep_expected_boundary_coverage(self):
        """PP-036: period rows with date+1 sentinel pass through with day fans."""
        day_targets = pd.DataFrame(
            {
                "code": ["19999"] * 5,
                "date": ["2024-01-10"] * 5,
                "target": [
                    "2024-01-11",
                    "2024-01-12",
                    "2024-01-13",
                    "2024-01-14",
                    "2024-01-15",
                ],
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0],
                "q05": [1.0] * 5,
                "q25": [2.0] * 5,
                "q75": [8.0] * 5,
                "q95": [9.0] * 5,
                "horizon_type": ["day"] * 5,
            }
        )
        period_row = pd.DataFrame(
            {
                "code": ["19999"],
                "date": ["2024-01-05"],
                "target": ["2024-01-06"],
                "forecasted_discharge": [101.0],
                "horizon_type": ["pentad"],
            }
        )
        non_boundary_row = pd.DataFrame(
            {
                "code": ["19999"],
                "date": ["2024-01-07"],
                "target": ["2024-01-08"],
                "forecasted_discharge": [777.0],
                "horizon_type": ["day"],
            }
        )
        out_of_period_row = pd.DataFrame(
            {
                "code": ["19999"],
                "date": ["2024-01-10"],
                "target": ["2024-01-16"],
                "forecasted_discharge": [888.0],
                "horizon_type": ["day"],
            }
        )
        raw = pd.concat(
            [period_row, day_targets, non_boundary_row, out_of_period_row],
            ignore_index=True,
        )

        result = _normalize_ml_forecasts(raw, "TFT", "pentad")

        assert set(result["code"]) == {"19999"}
        assert set(result["date"].dt.strftime("%Y-%m-%d")) == {
            "2024-01-05",
            "2024-01-10",
        }
        assert set(result["model_short"]) == {"TFT"}

        by_date = {
            row.date.strftime("%Y-%m-%d"): row.forecasted_discharge for row in result.itertuples()
        }
        assert by_date["2024-01-05"] == pytest.approx(101.0)
        assert by_date["2024-01-10"] == pytest.approx(30.0)

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
                "q05": [1.0] * 6,
                "q25": [2.0] * 6,
                "q75": [8.0] * 6,
                "q95": [9.0] * 6,
                "horizon_type": ["day"] * 6,
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
                "q05": [1.0] * 5,
                "q25": [2.0] * 5,
                "q75": [8.0] * 5,
                "q95": [9.0] * 5,
                "horizon_type": ["day"] * 5,
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
                "q05": [1.0] * 11,
                "q25": [2.0] * 11,
                "q75": [8.0] * 11,
                "q95": [9.0] * 11,
                "horizon_type": ["day"] * 11,
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
                "q05": [1.0] * 11,
                "q25": [2.0] * 11,
                "q75": [8.0] * 11,
                "q95": [9.0] * 11,
                "horizon_type": ["day"] * 11,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "decad")
        assert len(result) == 1
        # Mean of 8 in-period values: all 10.0 → 10.0
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(10.0)


class TestBackwardCompatibility:
    """Data without a horizon_type column is rejected (identity unestablishable)."""

    def test_no_target_column_skips_filtering(self):
        """No horizon_type column → identity unestablishable → rejected."""
        raw = pd.DataFrame(
            {
                "code": ["10001"] * 6,
                "date": ["2024-01-05"] * 6,
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    def test_empty_after_filter(self):
        """No horizon_type column → identity unestablishable → empty result."""
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
                "horizon_type": ["day"] * 6,
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
                "code": ["10001"] * 4 + ["10002"] * 3,
                "date": ["2024-01-05"] * 4 + ["2024-01-05"] * 3,
                "target": [
                    "2024-01-06",
                    "2024-01-07",
                    "2024-01-08",
                    "2024-01-11",  # 10001: 3 in, 1 out
                    "2024-01-06",
                    "2024-01-07",
                    "2024-01-08",  # 10002: 3 in
                ],
                "forecasted_discharge": [10.0, 20.0, 30.0, 999.0, 100.0, 200.0, 300.0],
                "q05": [1.0] * 7,
                "q25": [2.0] * 7,
                "q75": [8.0] * 7,
                "q95": [9.0] * 7,
                "horizon_type": ["day"] * 7,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 2
        r1 = result[result["code"] == "10001"]
        r2 = result[result["code"] == "10002"]
        assert r1["forecasted_discharge"].iloc[0] == pytest.approx(20.0)  # (10+20+30)/3
        assert r2["forecasted_discharge"].iloc[0] == pytest.approx(200.0)  # (100+200+300)/3


class TestBoundaryDayFiltering:
    """PP-031: Only boundary-day ML records should survive normalization."""

    def test_non_boundary_pentad_date_dropped(self):
        """ML record with date=Jan 4 (not a pentad boundary) is dropped."""
        raw = pd.DataFrame(
            {
                "code": ["10001"] * 6,
                "date": ["2024-01-04"] * 6,
                "target": [
                    "2024-01-05",
                    "2024-01-06",
                    "2024-01-07",
                    "2024-01-08",
                    "2024-01-09",
                    "2024-01-10",
                ],
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    def test_boundary_pentad_date_kept(self):
        """ML record with date=Jan 5 (pentad boundary) is kept and aggregated."""
        raw = pd.DataFrame(
            {
                "code": ["10001"] * 6,
                "date": ["2024-01-05"] * 6,
                "target": [
                    "2024-01-06",
                    "2024-01-07",
                    "2024-01-08",
                    "2024-01-09",
                    "2024-01-10",
                    "2024-01-11",
                ],
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
                "q05": [1.0] * 6,
                "q25": [2.0] * 6,
                "q75": [8.0] * 6,
                "q95": [9.0] * 6,
                "horizon_type": ["day"] * 6,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        # Targets Jan 6-10 are pentad 2 (matching date+1=Jan 6); Jan 11 is pentad 3 → dropped
        # Mean of 10, 20, 30, 40, 50 = 30.0
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(30.0)

    def test_eom_boundary_pentad_28day_month(self):
        """ML record with date=Feb 28 2025 (EOM non-leap, pentad boundary) is kept."""
        raw = pd.DataFrame(
            {
                "code": ["10001"] * 6,
                "date": ["2025-02-28"] * 6,
                "target": [
                    "2025-03-01",
                    "2025-03-02",
                    "2025-03-03",
                    "2025-03-04",
                    "2025-03-05",
                    "2025-03-06",
                ],
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
                "q05": [1.0] * 6,
                "q25": [2.0] * 6,
                "q75": [8.0] * 6,
                "q95": [9.0] * 6,
                "horizon_type": ["day"] * 6,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        # Targets Mar 1-5 are pentad 13 (matching date+1=Mar 1); Mar 6 is pentad 14 → dropped
        # Mean of 10, 20, 30, 40, 50 = 30.0
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(30.0)

    def test_eom_boundary_pentad_31day_month(self):
        """ML record with date=Mar 31 (EOM 31-day month, pentad boundary) is kept."""
        raw = pd.DataFrame(
            {
                "code": ["10001"] * 6,
                "date": ["2024-03-31"] * 6,
                "target": [
                    "2024-04-01",
                    "2024-04-02",
                    "2024-04-03",
                    "2024-04-04",
                    "2024-04-05",
                    "2024-04-06",
                ],
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
                "q05": [1.0] * 6,
                "q25": [2.0] * 6,
                "q75": [8.0] * 6,
                "q95": [9.0] * 6,
                "horizon_type": ["day"] * 6,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        # Targets Apr 1-5 are pentad 19; Apr 6 is pentad 20 → dropped
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(30.0)

    def test_leap_year_eom_feb29(self):
        """ML record with date=Feb 29 2024 (leap year EOM, pentad boundary) is kept."""
        raw = pd.DataFrame(
            {
                "code": ["10001"] * 5,
                "date": ["2024-02-29"] * 5,
                "target": [
                    "2024-03-01",
                    "2024-03-02",
                    "2024-03-03",
                    "2024-03-04",
                    "2024-03-05",
                ],
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0],
                "q05": [1.0] * 5,
                "q25": [2.0] * 5,
                "q75": [8.0] * 5,
                "q95": [9.0] * 5,
                "horizon_type": ["day"] * 5,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(30.0)

    def test_short_pentad_feb25_nonleap(self):
        """Feb 25 pentad: only 3 targets in period (Feb 26-28), not the usual 5."""
        raw = pd.DataFrame(
            {
                "code": ["10001"] * 6,
                "date": ["2025-02-25"] * 6,
                "target": [
                    "2025-02-26",
                    "2025-02-27",
                    "2025-02-28",
                    "2025-03-01",
                    "2025-03-02",
                    "2025-03-03",
                ],
                "forecasted_discharge": [10.0, 20.0, 30.0, 100.0, 200.0, 300.0],
                "q05": [1.0] * 6,
                "q25": [2.0] * 6,
                "q75": [8.0] * 6,
                "q95": [9.0] * 6,
                "horizon_type": ["day"] * 6,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        # Only Feb 26-28 in pentad 12; Mar 1+ is pentad 13 → dropped
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(20.0)

    def test_non_boundary_decad_date_dropped(self):
        """ML record with date=Jan 15 (pentad boundary but NOT decad boundary) is dropped for decad."""
        raw = pd.DataFrame(
            {
                "code": ["10001"] * 10,
                "date": ["2024-01-15"] * 10,
                "target": [f"2024-01-{d}" for d in range(16, 26)],
                "forecasted_discharge": [float(i) for i in range(10)],
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "decad")
        assert result.empty

    def test_boundary_decad_date_kept(self):
        """ML record with date=Jan 10 (decad boundary) is kept for decad."""
        raw = pd.DataFrame(
            {
                "code": ["10001"] * 11,
                "date": ["2024-01-10"] * 11,
                "target": [f"2024-01-{d}" for d in range(11, 22)],
                "forecasted_discharge": [float(i * 10) for i in range(11)],
                "q05": [1.0] * 11,
                "q25": [2.0] * 11,
                "q75": [8.0] * 11,
                "q95": [9.0] * 11,
                "horizon_type": ["day"] * 11,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "decad")
        assert len(result) == 1
        # Targets Jan 11-20 are decad 2 (10 days); Jan 21 is decad 3 → dropped
        # Mean of 0,10,20,30,40,50,60,70,80,90 = 45.0
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(45.0)

    def test_mixed_boundary_and_non_boundary(self):
        """Only boundary dates survive when input has both."""
        raw = pd.DataFrame(
            {
                "code": ["10001"] * 12,
                "date": (["2024-01-04"] * 6) + (["2024-01-05"] * 6),
                "target": (
                    [
                        "2024-01-05",
                        "2024-01-06",
                        "2024-01-07",
                        "2024-01-08",
                        "2024-01-09",
                        "2024-01-10",
                    ]
                    + [
                        "2024-01-06",
                        "2024-01-07",
                        "2024-01-08",
                        "2024-01-09",
                        "2024-01-10",
                        "2024-01-11",
                    ]
                ),
                "forecasted_discharge": ([100.0] * 6) + ([10.0, 20.0, 30.0, 40.0, 50.0, 60.0]),
                "q05": [1.0] * 12,
                "q25": [2.0] * 12,
                "q75": [8.0] * 12,
                "q95": [9.0] * 12,
                "horizon_type": ["day"] * 12,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        # Only Jan 5 boundary survives; Jan 4 non-boundary dropped
        assert pd.Timestamp(result["date"].iloc[0]) == pd.Timestamp("2024-01-05")
        # Mean of targets in pentad 2 from Jan 5 issue: 10,20,30,40,50 = 30.0
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(30.0)

    def test_dual_boundary_pentad_and_decad(self):
        """Date=Jan 10 is both pentad AND decad boundary — kept for both."""
        raw = pd.DataFrame(
            {
                "code": ["10001"] * 11,
                "date": ["2024-01-10"] * 11,
                "target": [f"2024-01-{d}" for d in range(11, 22)],
                "forecasted_discharge": [float(i * 10) for i in range(11)],
                "q05": [1.0] * 11,
                "q25": [2.0] * 11,
                "q75": [8.0] * 11,
                "q95": [9.0] * 11,
                "horizon_type": ["day"] * 11,
            }
        )
        # Pentad: targets Jan 11-15 kept (pentad 3), Jan 16+ dropped
        result_p = _normalize_ml_forecasts(raw.copy(), "TFT", "pentad")
        assert len(result_p) == 1
        # Mean of 0,10,20,30,40 = 20.0
        assert result_p["forecasted_discharge"].iloc[0] == pytest.approx(20.0)

        # Decad: targets Jan 11-20 kept (decad 2), Jan 21 dropped
        result_d = _normalize_ml_forecasts(raw.copy(), "TFT", "decad")
        assert len(result_d) == 1
        # Mean of 0,10,20,30,40,50,60,70,80,90 = 45.0
        assert result_d["forecasted_discharge"].iloc[0] == pytest.approx(45.0)

    def test_multiple_codes_boundary_filter_independent(self):
        """Boundary filter applies per-row, not per-code. Mixed dates across codes."""
        raw = pd.DataFrame(
            {
                "code": (["10001"] * 5) + (["10002"] * 5),
                "date": (["2024-01-04"] * 5) + (["2024-01-05"] * 5),
                "target": (
                    [f"2024-01-{d:02d}" for d in range(5, 10)]
                    + [f"2024-01-{d:02d}" for d in range(6, 11)]
                ),
                "forecasted_discharge": ([100.0] * 5) + ([10.0, 20.0, 30.0, 40.0, 50.0]),
                "q05": [1.0] * 10,
                "q25": [2.0] * 10,
                "q75": [8.0] * 10,
                "q95": [9.0] * 10,
                "horizon_type": ["day"] * 10,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        # Only code 10002 (Jan 5, boundary) survives
        assert len(result) == 1
        assert result["code"].iloc[0] == "10002"

    def test_all_non_boundary_returns_empty(self):
        """If ALL input dates are non-boundary, result is empty DataFrame."""
        raw = pd.DataFrame(
            {
                "code": ["10001"] * 5 + ["10001"] * 5,
                "date": ["2024-01-04"] * 5 + ["2024-01-06"] * 5,
                "target": (
                    [f"2024-01-{d:02d}" for d in range(5, 10)]
                    + [f"2024-01-{d:02d}" for d in range(7, 12)]
                ),
                "forecasted_discharge": [10.0] * 10,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    def test_non_boundary_no_target_column_returns_empty(self):
        """Non-boundary date with no target column → empty.

        Before PP-031, records without a target column were aggregated
        regardless of date. After the fix, the boundary filter runs
        unconditionally (before the target-presence check), so non-boundary
        dates are dropped even without a target column.
        """
        raw = pd.DataFrame(
            {
                "code": ["10001"] * 6,
                "date": ["2024-01-04"] * 6,
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty


class TestPentadBoundary:
    """PP-031: _is_pentad_boundary covers all edge cases."""

    @pytest.mark.parametrize(
        "day,expected",
        [
            (1, False),
            (4, False),
            (5, True),
            (6, False),
            (9, False),
            (10, True),
            (11, False),
            (14, False),
            (15, True),
            (16, False),
            (19, False),
            (20, True),
            (21, False),
            (24, False),
            (25, True),
            (26, False),
        ],
    )
    def test_regular_days(self, day, expected):
        assert _is_pentad_boundary(dt.date(2024, 1, day)) == expected

    @pytest.mark.parametrize(
        "month,last_day",
        [
            (1, 31),
            (2, 28),
            (3, 31),
            (4, 30),
            (6, 30),
            (12, 31),
        ],
    )
    def test_eom_is_boundary(self, month, last_day):
        assert _is_pentad_boundary(dt.date(2025, month, last_day)) is True

    def test_30day_month_non_eom_not_boundary(self):
        """Day 26-29 in a 30-day month are NOT boundaries (only 25 and 30 are)."""
        assert _is_pentad_boundary(dt.date(2025, 4, 26)) is False
        assert _is_pentad_boundary(dt.date(2025, 4, 29)) is False
        assert _is_pentad_boundary(dt.date(2025, 4, 30)) is True  # EOM

    def test_leap_year_feb29(self):
        assert _is_pentad_boundary(dt.date(2024, 2, 29)) is True  # EOM leap
        assert _is_pentad_boundary(dt.date(2024, 2, 28)) is False  # not EOM in leap year

    def test_works_with_pd_timestamp(self):
        assert _is_pentad_boundary(pd.Timestamp("2024-01-05")) is True
        assert _is_pentad_boundary(pd.Timestamp("2024-01-04")) is False


class TestDecadBoundary:
    """PP-031: _is_decad_boundary covers all edge cases."""

    @pytest.mark.parametrize(
        "day,expected",
        [
            (1, False),
            (5, False),
            (9, False),
            (10, True),
            (15, False),
            (19, False),
            (20, True),
            (21, False),
            (25, False),
        ],
    )
    def test_regular_days(self, day, expected):
        assert _is_decad_boundary(dt.date(2024, 1, day)) == expected

    @pytest.mark.parametrize(
        "month,last_day",
        [
            (1, 31),
            (2, 28),
            (4, 30),
            (2, 29),
        ],
    )
    def test_eom_is_boundary(self, month, last_day):
        year = 2024 if last_day == 29 else 2025
        assert _is_decad_boundary(dt.date(year, month, last_day)) is True

    def test_day25_not_decad_boundary(self):
        """Day 25 is pentad boundary but NOT decad boundary."""
        assert _is_decad_boundary(dt.date(2024, 1, 25)) is False


# ---------------------------------------------------------------------------
# MR-C: calendar coverage helper
# ---------------------------------------------------------------------------


class TestCalendarDaysInNextPeriod:
    """_calendar_days_in_next_period returns correct day counts for all
    pentad/decad boundary dates, covering all period lengths 3–11."""

    @pytest.mark.parametrize(
        "issue_date,horizon_type,expected",
        [
            # --- Pentad standard (5 days) ---
            (dt.date(2024, 1, 5), "pentad", 5),
            (dt.date(2024, 1, 10), "pentad", 5),
            (dt.date(2024, 1, 15), "pentad", 5),
            (dt.date(2024, 1, 20), "pentad", 5),
            # --- Pentad 6 days (25th of 31-day month) ---
            (dt.date(2024, 1, 25), "pentad", 6),
            (dt.date(2024, 12, 25), "pentad", 6),
            (dt.date(2024, 3, 25), "pentad", 6),
            # --- Pentad 3 days (Feb 25, non-leap) ---
            (dt.date(2025, 2, 25), "pentad", 3),
            # --- Pentad 4 days (Feb 25, leap year) ---
            (dt.date(2024, 2, 25), "pentad", 4),
            # --- Pentad EOM → 5 days in next month ---
            (dt.date(2025, 2, 28), "pentad", 5),
            (dt.date(2024, 2, 29), "pentad", 5),
            (dt.date(2024, 1, 31), "pentad", 5),
            (dt.date(2024, 12, 31), "pentad", 5),
            # --- Decad standard (10 days) ---
            (dt.date(2024, 1, 10), "decad", 10),
            (dt.date(2025, 2, 28), "decad", 10),
            (dt.date(2024, 2, 29), "decad", 10),
            (dt.date(2024, 12, 31), "decad", 10),
            # --- Decad 11 days (20th of 31-day month) ---
            (dt.date(2024, 1, 20), "decad", 11),
            (dt.date(2024, 12, 20), "decad", 11),
            (dt.date(2024, 3, 20), "decad", 11),
            # --- Decad 8 days (Feb 20, non-leap) ---
            (dt.date(2025, 2, 20), "decad", 8),
            # --- Decad 9 days (Feb 20, leap year) ---
            (dt.date(2024, 2, 20), "decad", 9),
        ],
    )
    def test_period_lengths(self, issue_date, horizon_type, expected):
        from src.data_reader import _calendar_days_in_next_period

        assert _calendar_days_in_next_period(issue_date, horizon_type) == expected

    def test_accepts_pd_timestamp(self):
        from src.data_reader import _calendar_days_in_next_period

        assert _calendar_days_in_next_period(pd.Timestamp("2025-02-25"), "pentad") == 3
        assert _calendar_days_in_next_period(pd.Timestamp("2024-02-20"), "decad") == 9


# ---------------------------------------------------------------------------
# MR-C: DAY-row coverage filter behaviour
# ---------------------------------------------------------------------------


class TestDayCoverageFilter:
    """_normalize_ml_forecasts applies >=50% calendar-coverage threshold to
    positively-identified DAY rows (horizon_type=='day').  Legacy period rows
    and data without a horizon_type column are unaffected."""

    @staticmethod
    def _day_frame(issue_date_str, targets, discharges, **extra_cols):
        n = len(targets)
        d = {
            "code": ["19999"] * n,
            "date": [issue_date_str] * n,
            "target": targets,
            "forecasted_discharge": discharges,
            "q05": [1.0] * n,
            "q25": [2.0] * n,
            "q75": [8.0] * n,
            "q95": [9.0] * n,
            "horizon_type": ["day"] * n,
        }
        d.update(extra_cols)
        return pd.DataFrame(d)

    # --- Period length 5 (standard pentad) ---

    def test_at_50pct_pentad5_passes(self):
        """3/5 finite targets at exact threshold ceil(2.5)=3 → passes."""
        raw = self._day_frame(
            "2024-01-05",
            ["2024-01-06", "2024-01-07", "2024-01-08", "2024-01-09", "2024-01-10"],
            [10.0, 20.0, 30.0, float("nan"), float("nan")],
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(20.0)

    def test_below_50pct_pentad5_excluded(self):
        """2/5 finite targets below threshold → excluded."""
        raw = self._day_frame(
            "2024-01-05",
            ["2024-01-06", "2024-01-07", "2024-01-08", "2024-01-09", "2024-01-10"],
            [10.0, 20.0, float("nan"), float("nan"), float("nan")],
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    # --- Period length 3 (Feb 25, non-leap) ---

    def test_at_50pct_pentad3_feb25_nonleap_passes(self):
        """2/3 finite targets at ceil(1.5)=2 → passes."""
        raw = self._day_frame(
            "2025-02-25",
            ["2025-02-26", "2025-02-27", "2025-02-28"],
            [10.0, 20.0, float("nan")],
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(15.0)

    def test_below_50pct_pentad3_feb25_nonleap_excluded(self):
        """1/3 finite targets below ceil(1.5)=2 → excluded."""
        raw = self._day_frame(
            "2025-02-25",
            ["2025-02-26", "2025-02-27", "2025-02-28"],
            [10.0, float("nan"), float("nan")],
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    # --- Period length 4 (Feb 25, leap year) ---

    def test_at_50pct_pentad4_feb25_leap_passes(self):
        """2/4 finite targets at ceil(2.0)=2 → passes."""
        raw = self._day_frame(
            "2024-02-25",
            ["2024-02-26", "2024-02-27", "2024-02-28", "2024-02-29"],
            [10.0, 20.0, float("nan"), float("nan")],
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1

    def test_below_50pct_pentad4_feb25_leap_excluded(self):
        """1/4 finite targets below ceil(2.0)=2 → excluded."""
        raw = self._day_frame(
            "2024-02-25",
            ["2024-02-26", "2024-02-27", "2024-02-28", "2024-02-29"],
            [10.0, float("nan"), float("nan"), float("nan")],
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    # --- Period length 6 (Jan 25, Jan 26–31) ---

    def test_at_50pct_pentad6_jan25_passes(self):
        """3/6 finite targets at ceil(3.0)=3 → passes."""
        raw = self._day_frame(
            "2024-01-25",
            [f"2024-01-{d}" for d in range(26, 32)],
            [10.0, 20.0, 30.0, float("nan"), float("nan"), float("nan")],
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1

    def test_below_50pct_pentad6_jan25_excluded(self):
        """2/6 finite targets below ceil(3.0)=3 → excluded."""
        raw = self._day_frame(
            "2024-01-25",
            [f"2024-01-{d}" for d in range(26, 32)],
            [10.0, 20.0, float("nan"), float("nan"), float("nan"), float("nan")],
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    # --- Period length 8 (Feb 20, non-leap decad) ---

    def test_at_50pct_decad8_feb20_nonleap_passes(self):
        """4/8 finite targets at ceil(4.0)=4 → passes."""
        targets = [f"2025-02-{d}" for d in range(21, 29)]
        discharges = [10.0, 20.0, 30.0, 40.0] + [float("nan")] * 4
        raw = self._day_frame("2025-02-20", targets, discharges)
        result = _normalize_ml_forecasts(raw, "TFT", "decad")
        assert len(result) == 1

    def test_below_50pct_decad8_feb20_nonleap_excluded(self):
        """3/8 finite targets below ceil(4.0)=4 → excluded."""
        targets = [f"2025-02-{d}" for d in range(21, 29)]
        discharges = [10.0, 20.0, 30.0] + [float("nan")] * 5
        raw = self._day_frame("2025-02-20", targets, discharges)
        result = _normalize_ml_forecasts(raw, "TFT", "decad")
        assert result.empty

    # --- Period length 9 (Feb 20, leap-year decad) ---

    def test_at_50pct_decad9_feb20_leap_passes(self):
        """5/9 finite targets at ceil(4.5)=5 → passes."""
        targets = [f"2024-02-{d}" for d in range(21, 30)]
        discharges = [10.0, 20.0, 30.0, 40.0, 50.0] + [float("nan")] * 4
        raw = self._day_frame("2024-02-20", targets, discharges)
        result = _normalize_ml_forecasts(raw, "TFT", "decad")
        assert len(result) == 1

    def test_below_50pct_decad9_feb20_leap_excluded(self):
        """4/9 finite targets below ceil(4.5)=5 → excluded."""
        targets = [f"2024-02-{d}" for d in range(21, 30)]
        discharges = [10.0, 20.0, 30.0, 40.0] + [float("nan")] * 5
        raw = self._day_frame("2024-02-20", targets, discharges)
        result = _normalize_ml_forecasts(raw, "TFT", "decad")
        assert result.empty

    # --- Period length 10 (standard decad) ---

    def test_at_50pct_decad10_standard_passes(self):
        """5/10 finite at ceil(5.0)=5 → passes."""
        targets = [f"2024-01-{d}" for d in range(11, 21)]
        discharges = [10.0] * 5 + [float("nan")] * 5
        raw = self._day_frame("2024-01-10", targets, discharges)
        result = _normalize_ml_forecasts(raw, "TFT", "decad")
        assert len(result) == 1

    def test_below_50pct_decad10_standard_excluded(self):
        """4/10 finite below ceil(5.0)=5 → excluded."""
        targets = [f"2024-01-{d}" for d in range(11, 21)]
        discharges = [10.0] * 4 + [float("nan")] * 6
        raw = self._day_frame("2024-01-10", targets, discharges)
        result = _normalize_ml_forecasts(raw, "TFT", "decad")
        assert result.empty

    # --- Period length 11 (Jan 20, decad) ---

    def test_at_50pct_decad11_jan20_passes(self):
        """6/11 finite targets at ceil(5.5)=6 → passes."""
        targets = [f"2024-01-{d}" for d in range(21, 32)]
        discharges = [10.0] * 6 + [float("nan")] * 5
        raw = self._day_frame("2024-01-20", targets, discharges)
        result = _normalize_ml_forecasts(raw, "TFT", "decad")
        assert len(result) == 1

    def test_below_50pct_decad11_jan20_excluded(self):
        """5/11 finite targets below ceil(5.5)=6 → excluded."""
        targets = [f"2024-01-{d}" for d in range(21, 32)]
        discharges = [10.0] * 5 + [float("nan")] * 6
        raw = self._day_frame("2024-01-20", targets, discharges)
        result = _normalize_ml_forecasts(raw, "TFT", "decad")
        assert result.empty

    # --- Shared mask: all present quantile columns must be finite ---

    def test_shared_mask_nan_q05_disqualifies_row(self):
        """Row with finite discharge but NaN q05 does not count toward coverage."""
        # 5-day pentad, threshold 3.
        # Rows 1-2: all 5 columns finite → pass shared mask.
        # Row 3: q05=NaN → fails shared mask even though discharge is finite.
        # Rows 4-5: NaN throughout → fail shared mask.
        # 2 pass → < ceil(2.5)=3 → excluded.
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 5,
                "date": ["2024-01-05"] * 5,
                "target": ["2024-01-06", "2024-01-07", "2024-01-08", "2024-01-09", "2024-01-10"],
                "forecasted_discharge": [10.0, 20.0, 30.0, float("nan"), float("nan")],
                "q05": [1.0, 2.0, float("nan"), float("nan"), float("nan")],
                "q25": [2.0, 4.0, 6.0, float("nan"), float("nan")],
                "q75": [8.0, 16.0, 24.0, float("nan"), float("nan")],
                "q95": [9.0, 18.0, 27.0, float("nan"), float("nan")],
                "horizon_type": ["day"] * 5,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    def test_shared_mask_all_quantiles_finite_counted(self):
        """All five required quantile columns finite → all rows count."""
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 5,
                "date": ["2024-01-05"] * 5,
                "target": ["2024-01-06", "2024-01-07", "2024-01-08", "2024-01-09", "2024-01-10"],
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0],
                "q05": [1.0, 2.0, 3.0, 4.0, 5.0],
                "q25": [2.0, 4.0, 6.0, 8.0, 10.0],
                "q75": [80.0, 160.0, 240.0, 320.0, 400.0],
                "q95": [100.0, 200.0, 300.0, 400.0, 500.0],
                "horizon_type": ["day"] * 5,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1

    def test_infinite_discharge_not_counted(self):
        """Rows with ±inf discharge do not count toward coverage."""
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 5,
                "date": ["2024-01-05"] * 5,
                "target": ["2024-01-06", "2024-01-07", "2024-01-08", "2024-01-09", "2024-01-10"],
                "forecasted_discharge": [10.0, 20.0, float("inf"), float("nan"), float("-inf")],
                "q05": [1.0, 2.0, float("inf"), float("nan"), float("-inf")],
                "q25": [2.0, 4.0, float("inf"), float("nan"), float("-inf")],
                "q75": [8.0, 16.0, float("inf"), float("nan"), float("-inf")],
                "q95": [9.0, 18.0, float("inf"), float("nan"), float("-inf")],
                "horizon_type": ["day"] * 5,
            }
        )
        # Only Jan 06 and 07 pass → 2 < ceil(2.5)=3 → excluded
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    # --- Wrong-year target ---

    def test_wrong_year_target_excluded(self):
        """Dec 31 issue: targets from same year are excluded (expected next year)."""
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 5,
                "date": ["2024-12-31"] * 5,
                "target": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0],
                "q05": [1.0] * 5,
                "q25": [2.0] * 5,
                "q75": [8.0] * 5,
                "q95": [9.0] * 5,
                "horizon_type": ["day"] * 5,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    def test_correct_year_dec31_passes(self):
        """Dec 31 issue: targets in next year (Jan 1–5) with full coverage pass."""
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 5,
                "date": ["2024-12-31"] * 5,
                "target": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"],
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0],
                "q05": [1.0] * 5,
                "q25": [2.0] * 5,
                "q75": [8.0] * 5,
                "q95": [9.0] * 5,
                "horizon_type": ["day"] * 5,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1

    # --- Duplicate rows ---

    def test_identical_duplicates_collapse_to_one(self):
        """Identical duplicate target rows collapse to one; still counted once."""
        targets_doubled = (
            ["2024-01-06"] * 2
            + ["2024-01-07"] * 2
            + ["2024-01-08"] * 2
            + ["2024-01-09"] * 2
            + ["2024-01-10"] * 2
        )
        discharges = [10.0, 10.0, 20.0, 20.0, 30.0, 30.0, 40.0, 40.0, 50.0, 50.0]
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 10,
                "date": ["2024-01-05"] * 10,
                "target": targets_doubled,
                "forecasted_discharge": discharges,
                "q05": [1.0] * 10,
                "q25": [2.0] * 10,
                "q75": [8.0] * 10,
                "q95": [9.0] * 10,
                "horizon_type": ["day"] * 10,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(30.0)

    def test_conflicting_duplicates_exclude_group(self):
        """Same target with different discharge values → entire (code,date) excluded."""
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 6,
                "date": ["2024-01-05"] * 6,
                "target": [
                    "2024-01-06",
                    "2024-01-06",  # conflicting payload
                    "2024-01-07",
                    "2024-01-08",
                    "2024-01-09",
                    "2024-01-10",
                ],
                "forecasted_discharge": [10.0, 999.0, 20.0, 30.0, 40.0, 50.0],
                "q05": [1.0] * 6,
                "q25": [2.0] * 6,
                "q75": [8.0] * 6,
                "q95": [9.0] * 6,
                "horizon_type": ["day"] * 6,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    def test_conflicting_in_one_code_keeps_other(self):
        """Conflict in one code's group leaves the other code unaffected."""
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 6 + ["20000"] * 5,
                "date": ["2024-01-05"] * 11,
                "target": (
                    [
                        "2024-01-06",
                        "2024-01-06",
                        "2024-01-07",
                        "2024-01-08",
                        "2024-01-09",
                        "2024-01-10",
                    ]
                    + ["2024-01-06", "2024-01-07", "2024-01-08", "2024-01-09", "2024-01-10"]
                ),
                "forecasted_discharge": [10.0, 999.0, 20.0, 30.0, 40.0, 50.0]
                + [10.0, 20.0, 30.0, 40.0, 50.0],
                "q05": [1.0] * 11,
                "q25": [2.0] * 11,
                "q75": [8.0] * 11,
                "q95": [9.0] * 11,
                "horizon_type": ["day"] * 11,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        assert result["code"].iloc[0] == "20000"

    # --- Mixed DAY and legacy period rows ---

    def test_legacy_period_rows_bypass_coverage_check(self):
        """Legacy period rows (horizon_type != 'day') pass without coverage."""
        # Single row with date+1 as target = period row sentinel → no coverage check
        raw = pd.DataFrame(
            {
                "code": ["19999"],
                "date": ["2024-01-05"],
                "target": ["2024-01-06"],
                "forecasted_discharge": [101.0],
                "horizon_type": ["pentad"],
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(101.0)

    def test_mixed_day_and_legacy_independent_checks(self):
        """DAY coverage check for Jan 10, legacy pass-through for Jan 5."""
        day_rows = pd.DataFrame(
            {
                "code": ["19999"] * 5,
                "date": ["2024-01-10"] * 5,
                "target": [
                    "2024-01-11",
                    "2024-01-12",
                    "2024-01-13",
                    "2024-01-14",
                    "2024-01-15",
                ],
                "forecasted_discharge": [10.0, 20.0, 30.0, float("nan"), float("nan")],
                "q05": [1.0, 2.0, 3.0, float("nan"), float("nan")],
                "q25": [2.0, 4.0, 6.0, float("nan"), float("nan")],
                "q75": [8.0, 16.0, 24.0, float("nan"), float("nan")],
                "q95": [9.0, 18.0, 27.0, float("nan"), float("nan")],
                "horizon_type": ["day"] * 5,
            }
        )
        legacy = pd.DataFrame(
            {
                "code": ["19999"],
                "date": ["2024-01-05"],
                "target": ["2024-01-06"],
                "forecasted_discharge": [101.0],
                "horizon_type": ["pentad"],
            }
        )
        raw = pd.concat([legacy, day_rows], ignore_index=True)
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 2
        by_date = {
            row.date.strftime("%Y-%m-%d"): row.forecasted_discharge for row in result.itertuples()
        }
        assert by_date["2024-01-05"] == pytest.approx(101.0)
        assert by_date["2024-01-10"] == pytest.approx(20.0)  # mean(10, 20, 30)

    # --- No-target malformed DAY ---

    def test_day_rows_without_target_column_excluded(self):
        """DAY rows without target column excluded (identity unverifiable)."""
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 5,
                "date": ["2024-01-05"] * 5,
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0],
                "horizon_type": ["day"] * 5,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    # --- Boundary/model behavior unchanged for non-DAY data ---

    def test_no_horizon_type_column_uses_existing_path(self):
        """No horizon_type column → identity unestablishable → rejected (empty)."""
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 6,
                "date": ["2024-01-05"] * 6,
                "target": [
                    "2024-01-06",
                    "2024-01-07",
                    "2024-01-08",
                    "2024-01-09",
                    "2024-01-10",
                    "2024-01-11",
                ],
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0, 600.0],
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    def test_model_mapping_preserved_for_day_rows(self):
        """Model name mapping (TIDE→TiDE, TSMIXER→TSMixer) preserved on DAY rows."""
        raw = self._day_frame(
            "2024-01-05",
            ["2024-01-06", "2024-01-07", "2024-01-08", "2024-01-09", "2024-01-10"],
            [10.0, 20.0, 30.0, 40.0, 50.0],
        )
        for api_model, expected_short in [("TIDE", "TiDE"), ("TSMIXER", "TSMixer")]:
            result = _normalize_ml_forecasts(raw.copy(), api_model, "pentad")
            assert result["model_short"].iloc[0] == expected_short


# ---------------------------------------------------------------------------
# Violation 1: _apply_day_coverage_filter must return ONLY finite rows
# ---------------------------------------------------------------------------


class TestCoverageFilterReturnsFiniteRowsOnly:
    """Passing group returns only the finite rows; non-finite rows are excluded
    from the returned set so the downstream mean is computed correctly."""

    def test_passing_group_with_inf_discharge_mean_is_finite(self):
        """4 finite + 1 inf discharge in a 5-day pentad (4>=3 → passes).
        Mean must be over 4 finite rows only, not inf."""
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 5,
                "date": ["2024-01-05"] * 5,
                "target": [
                    "2024-01-06",
                    "2024-01-07",
                    "2024-01-08",
                    "2024-01-09",
                    "2024-01-10",
                ],
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, float("inf")],
                "q05": [1.0, 2.0, 3.0, 4.0, float("inf")],
                "q25": [2.0, 4.0, 6.0, 8.0, float("inf")],
                "q75": [20.0, 40.0, 60.0, 80.0, float("inf")],
                "q95": [30.0, 60.0, 90.0, 120.0, float("inf")],
                "horizon_type": ["day"] * 5,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        # Mean of 4 finite rows: (10+20+30+40)/4 = 25.0
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(25.0)
        # q05 mean: (1+2+3+4)/4 = 2.5
        assert result["q05"].iloc[0] == pytest.approx(2.5)

    def test_passing_group_with_null_quantile_row_excluded_from_mean(self):
        """4 finite + 1 row with null q25 in a 5-day pentad (4>=3 → passes).
        Null-quantile row must not contribute to the mean."""
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 5,
                "date": ["2024-01-05"] * 5,
                "target": [
                    "2024-01-06",
                    "2024-01-07",
                    "2024-01-08",
                    "2024-01-09",
                    "2024-01-10",
                ],
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0],
                "q05": [1.0, 2.0, 3.0, 4.0, 5.0],
                # Row 5 has NaN q25 → fails shared mask
                "q25": [2.0, 4.0, 6.0, 8.0, float("nan")],
                "q75": [20.0, 40.0, 60.0, 80.0, 100.0],
                "q95": [30.0, 60.0, 90.0, 120.0, 150.0],
                "horizon_type": ["day"] * 5,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        # Only 4 rows are fully finite; mean = (10+20+30+40)/4 = 25.0
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(25.0)
        # q05 mean over 4 finite rows: (1+2+3+4)/4 = 2.5
        assert result["q05"].iloc[0] == pytest.approx(2.5)

    def test_passing_group_with_neg_inf_extreme_quantile_excluded(self):
        """Row with -inf q95 (invalid extreme quantile) excluded from mean."""
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 5,
                "date": ["2024-01-05"] * 5,
                "target": [
                    "2024-01-06",
                    "2024-01-07",
                    "2024-01-08",
                    "2024-01-09",
                    "2024-01-10",
                ],
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0],
                "q05": [1.0, 2.0, 3.0, 4.0, 5.0],
                "q25": [2.0, 4.0, 6.0, 8.0, 10.0],
                "q75": [20.0, 40.0, 60.0, 80.0, 100.0],
                # Row 5 has -inf q95 → fails shared mask
                "q95": [30.0, 60.0, 90.0, 120.0, float("-inf")],
                "horizon_type": ["day"] * 5,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        # Mean over 4 finite rows only: (10+20+30+40)/4 = 25.0
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(25.0)
        # q25 mean over 4 finite rows: (2+4+6+8)/4 = 5.0
        assert result["q25"].iloc[0] == pytest.approx(5.0)


# ---------------------------------------------------------------------------
# Violation 2: all five required columns must be present for DAY aggregation
# ---------------------------------------------------------------------------


class TestRequiredColumnsEnforcement:
    """DAY aggregation rejects (returns empty) if any of the five required
    columns is absent from the input DataFrame."""

    @staticmethod
    def _full_day_frame():
        return pd.DataFrame(
            {
                "code": ["19999"] * 5,
                "date": ["2024-01-05"] * 5,
                "target": [
                    "2024-01-06",
                    "2024-01-07",
                    "2024-01-08",
                    "2024-01-09",
                    "2024-01-10",
                ],
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0],
                "q05": [1.0, 2.0, 3.0, 4.0, 5.0],
                "q25": [2.0, 4.0, 6.0, 8.0, 10.0],
                "q75": [20.0, 40.0, 60.0, 80.0, 100.0],
                "q95": [30.0, 60.0, 90.0, 120.0, 150.0],
                "horizon_type": ["day"] * 5,
            }
        )

    def test_missing_forecasted_discharge_rejected(self):
        """Missing forecasted_discharge → DAY aggregation rejected."""
        raw = self._full_day_frame().drop(columns=["forecasted_discharge"])
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    def test_missing_q05_rejected(self):
        """Missing q05 → DAY aggregation rejected."""
        raw = self._full_day_frame().drop(columns=["q05"])
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    def test_missing_q25_rejected(self):
        """Missing q25 → DAY aggregation rejected."""
        raw = self._full_day_frame().drop(columns=["q25"])
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    def test_missing_q75_rejected(self):
        """Missing q75 → DAY aggregation rejected."""
        raw = self._full_day_frame().drop(columns=["q75"])
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    def test_missing_q95_rejected(self):
        """Missing q95 → DAY aggregation rejected."""
        raw = self._full_day_frame().drop(columns=["q95"])
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    def test_all_five_present_passes(self):
        """All five required columns present → aggregation succeeds."""
        raw = self._full_day_frame()
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(30.0)


# ---------------------------------------------------------------------------
# Violation 3: unknown/null horizon_type must not bypass as legacy period
# ---------------------------------------------------------------------------


class TestHorizonTypeProvenance:
    """Only rows with a positively-identified period horizon_type ('pentad' or
    'decad') bypass DAY coverage.  Null, NaN, and unrecognised values go
    through the coverage filter; insufficient coverage → excluded."""

    @staticmethod
    def _make_single_row(horizon_type_val, include_all_cols=True):
        d = {
            "code": ["19999"],
            "date": ["2024-01-05"],
            "target": ["2024-01-06"],
            "forecasted_discharge": [101.0],
            "horizon_type": [horizon_type_val],
        }
        if include_all_cols:
            d["q05"] = [1.0]
            d["q25"] = [2.0]
            d["q75"] = [8.0]
            d["q95"] = [9.0]
        return pd.DataFrame(d)

    def test_known_pentad_bypasses_coverage(self):
        """horizon_type='pentad' is a positively-identified legacy row → bypasses."""
        raw = self._make_single_row("pentad", include_all_cols=False)
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        # Single legacy row passes through without coverage check
        assert len(result) == 1
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(101.0)

    def test_known_decade_api_value_bypasses_coverage(self):
        """horizon_type='decade' (the API's actual value for decad) is positively
        identified as a legacy period row and bypasses coverage."""
        raw = pd.DataFrame(
            {
                "code": ["19999"],
                "date": ["2024-01-10"],
                "target": ["2024-01-11"],
                "forecasted_discharge": [202.0],
                "horizon_type": ["decade"],
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "decad")
        assert len(result) == 1
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(202.0)

    def test_null_horizon_type_not_legacy_bypass(self):
        """horizon_type=None does not bypass coverage; insufficient coverage → excluded."""
        raw = self._make_single_row(None)
        # Single DAY-fan row: n_unique_finite=1 < ceil(2.5)=3 → excluded
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    def test_nan_horizon_type_not_legacy_bypass(self):
        """horizon_type=NaN does not bypass coverage; insufficient coverage → excluded."""
        raw = self._make_single_row(float("nan"))
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    def test_unknown_string_horizon_type_not_legacy_bypass(self):
        """horizon_type='unknown' does not bypass coverage; excluded."""
        raw = self._make_single_row("unknown")
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    def test_absent_horizon_type_column_with_full_coverage_rejected(self):
        """Absent horizon_type column → identity unestablishable → rejected even with
        full coverage and all five required columns present."""
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 5,
                "date": ["2024-01-05"] * 5,
                "target": [
                    "2024-01-06",
                    "2024-01-07",
                    "2024-01-08",
                    "2024-01-09",
                    "2024-01-10",
                ],
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0],
                "q05": [1.0, 2.0, 3.0, 4.0, 5.0],
                "q25": [2.0, 4.0, 6.0, 8.0, 10.0],
                "q75": [20.0, 40.0, 60.0, 80.0, 100.0],
                "q95": [30.0, 60.0, 90.0, 120.0, 150.0],
                # no horizon_type column
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    def test_null_horizon_type_with_full_coverage_rejected(self):
        """Null horizon_type is unrecognised identity → rejected even with full coverage."""
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 5,
                "date": ["2024-01-05"] * 5,
                "target": [
                    "2024-01-06",
                    "2024-01-07",
                    "2024-01-08",
                    "2024-01-09",
                    "2024-01-10",
                ],
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0],
                "q05": [1.0, 2.0, 3.0, 4.0, 5.0],
                "q25": [2.0, 4.0, 6.0, 8.0, 10.0],
                "q75": [20.0, 40.0, 60.0, 80.0, 100.0],
                "q95": [30.0, 60.0, 90.0, 120.0, 150.0],
                "horizon_type": [None] * 5,
            }
        )
        # Coverage would pass, but identity is null → conservative rejection
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty


# ---------------------------------------------------------------------------
# Fix 1: semantic duplicate dedup — API metadata fields must not prevent
# deduplication of identical forecasts
# ---------------------------------------------------------------------------


class TestSemanticDuplicateDedup:
    """Fix 1: identical forecasts with differing API metadata (id, created_at,
    updated_at) must collapse to one row; genuine payload conflicts still
    exclude the entire (code, issue_date) group."""

    def test_api_id_duplicate_collapses_to_one_target(self):
        """Two rows identical on identity+payload but with different API id values
        are the same forecast — must deduplicate so the mean is over 5 unique
        target dates, not corrupted by a phantom sixth row."""
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 6,
                "date": ["2024-01-05"] * 6,
                # Jan 6 appears twice (different id); Jan 7-10 once each
                "target": [
                    "2024-01-06",
                    "2024-01-06",
                    "2024-01-07",
                    "2024-01-08",
                    "2024-01-09",
                    "2024-01-10",
                ],
                "forecasted_discharge": [10.0, 10.0, 20.0, 30.0, 40.0, 50.0],
                "q05": [1.0, 1.0, 2.0, 3.0, 4.0, 5.0],
                "q25": [2.0, 2.0, 4.0, 6.0, 8.0, 10.0],
                "q75": [8.0, 8.0, 16.0, 24.0, 32.0, 40.0],
                "q95": [9.0, 9.0, 18.0, 27.0, 36.0, 45.0],
                "horizon_type": ["day"] * 6,
                "id": [101, 201, 102, 103, 104, 105],  # different API ids
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        # 5 unique target dates after semantic dedup; mean = (10+20+30+40+50)/5 = 30.0
        assert len(result) == 1
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(30.0)

    def test_api_metadata_duplicates_threshold_unique_day_count(self):
        """Semantic dedup counts 3 unique target dates even when each appears twice
        with different API ids. 3/5 threshold = ceil(2.5)=3 → passes coverage."""
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 6,
                "date": ["2024-01-05"] * 6,
                # Jan 6, 7, 8 each appear twice with different ids
                "target": [
                    "2024-01-06",
                    "2024-01-06",
                    "2024-01-07",
                    "2024-01-07",
                    "2024-01-08",
                    "2024-01-08",
                ],
                "forecasted_discharge": [10.0, 10.0, 20.0, 20.0, 30.0, 30.0],
                "q05": [1.0] * 6,
                "q25": [2.0] * 6,
                "q75": [8.0] * 6,
                "q95": [9.0] * 6,
                "horizon_type": ["day"] * 6,
                "id": [101, 201, 102, 202, 103, 203],
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        # 3 unique target dates; mean = (10+20+30)/3 = 20.0
        assert len(result) == 1
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(20.0)

    def test_genuine_payload_conflict_still_excludes_group(self):
        """Same target with different discharge values (genuine conflict) excludes
        the entire (code, issue_date) group, even if API ids differ."""
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 6,
                "date": ["2024-01-05"] * 6,
                "target": [
                    "2024-01-06",
                    "2024-01-06",  # conflicting discharge
                    "2024-01-07",
                    "2024-01-08",
                    "2024-01-09",
                    "2024-01-10",
                ],
                "forecasted_discharge": [10.0, 999.0, 20.0, 30.0, 40.0, 50.0],
                "q05": [1.0] * 6,
                "q25": [2.0] * 6,
                "q75": [8.0] * 6,
                "q95": [9.0] * 6,
                "horizon_type": ["day"] * 6,
                "id": [101, 201, 102, 103, 104, 105],
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    def test_conflict_in_one_code_metadata_dup_in_other_both_handled(self):
        """Code 19999 has a genuine conflict → excluded.
        Code 20000 has metadata duplicates only → collapses correctly."""
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 6 + ["20000"] * 6,
                "date": ["2024-01-05"] * 12,
                "target": (
                    [
                        "2024-01-06",
                        "2024-01-06",
                        "2024-01-07",
                        "2024-01-08",
                        "2024-01-09",
                        "2024-01-10",
                    ]
                    + [
                        "2024-01-06",
                        "2024-01-06",
                        "2024-01-07",
                        "2024-01-08",
                        "2024-01-09",
                        "2024-01-10",
                    ]
                ),
                "forecasted_discharge": (
                    [10.0, 999.0, 20.0, 30.0, 40.0, 50.0]  # 19999: conflict on Jan 6
                    + [10.0, 10.0, 20.0, 30.0, 40.0, 50.0]  # 20000: metadata dup on Jan 6
                ),
                "q05": [1.0] * 12,
                "q25": [2.0] * 12,
                "q75": [8.0] * 12,
                "q95": [9.0] * 12,
                "horizon_type": ["day"] * 12,
                "id": [101, 201, 102, 103, 104, 105, 111, 211, 112, 113, 114, 115],
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        # Only code 20000 survives; mean = (10+20+30+40+50)/5 = 30.0
        assert len(result) == 1
        assert result["code"].iloc[0] == "20000"
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(30.0)


# ---------------------------------------------------------------------------
# Fix 2: calendar bounds enforced independently of TAG_LIBRARY_AVAILABLE
# ---------------------------------------------------------------------------


class TestCalendarBoundsWithoutTagLibrary:
    """Fix 2: DAY target calendar bounds are enforced in _apply_day_coverage_filter
    regardless of TAG_LIBRARY_AVAILABLE.  When TAG_LIBRARY_AVAILABLE=False the
    period filter in _normalize_ml_forecasts is skipped, but targets outside
    [issue+1, issue+period_length] must still be rejected."""

    def test_out_of_period_targets_rejected_without_tag_library(self, monkeypatch):
        """Jan 16-18 targets for Jan 5 issue are in the wrong pentad.
        Must be rejected even when TAG_LIBRARY_AVAILABLE=False."""
        import src.data_reader as dr

        monkeypatch.setattr(dr, "TAG_LIBRARY_AVAILABLE", False)
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 3,
                "date": ["2024-01-05"] * 3,
                # Jan 16-18 are same-year but outside the Jan 6-10 window
                "target": ["2024-01-16", "2024-01-17", "2024-01-18"],
                "forecasted_discharge": [10.0, 20.0, 30.0],
                "q05": [1.0] * 3,
                "q25": [2.0] * 3,
                "q75": [8.0] * 3,
                "q95": [9.0] * 3,
                "horizon_type": ["day"] * 3,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    def test_valid_targets_accepted_without_tag_library(self, monkeypatch):
        """Jan 6-8 targets for Jan 5 issue are within the Jan 6-10 window.
        3/5 unique finite dates meets ceil(2.5)=3 threshold → passes."""
        import src.data_reader as dr

        monkeypatch.setattr(dr, "TAG_LIBRARY_AVAILABLE", False)
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 3,
                "date": ["2024-01-05"] * 3,
                "target": ["2024-01-06", "2024-01-07", "2024-01-08"],
                "forecasted_discharge": [10.0, 20.0, 30.0],
                "q05": [1.0] * 3,
                "q25": [2.0] * 3,
                "q75": [8.0] * 3,
                "q95": [9.0] * 3,
                "horizon_type": ["day"] * 3,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(20.0)  # (10+20+30)/3

    def test_cross_year_bounds_without_tag_library(self, monkeypatch):
        """Dec 31 issue: Jan 1-5 (next year) are valid; Jan 6 is out of bounds.
        Valid mean = (10+20+30+40+50)/5 = 30.0."""
        import src.data_reader as dr

        monkeypatch.setattr(dr, "TAG_LIBRARY_AVAILABLE", False)
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 6,
                "date": ["2024-12-31"] * 6,
                "target": [
                    "2025-01-01",
                    "2025-01-02",
                    "2025-01-03",
                    "2025-01-04",
                    "2025-01-05",
                    "2025-01-06",
                ],
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0, 999.0],
                "q05": [1.0] * 6,
                "q25": [2.0] * 6,
                "q75": [8.0] * 6,
                "q95": [9.0] * 6,
                "horizon_type": ["day"] * 6,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(30.0)

    def test_month_end_short_pentad_bounds_without_tag_library(self, monkeypatch):
        """Feb 25 non-leap: 3-day period (Feb 26-28). Mar 1+ must be rejected."""
        import src.data_reader as dr

        monkeypatch.setattr(dr, "TAG_LIBRARY_AVAILABLE", False)
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 5,
                "date": ["2025-02-25"] * 5,
                "target": [
                    "2025-02-26",
                    "2025-02-27",
                    "2025-02-28",
                    "2025-03-01",
                    "2025-03-02",
                ],
                "forecasted_discharge": [10.0, 20.0, 30.0, 999.0, 999.0],
                "q05": [1.0] * 5,
                "q25": [2.0] * 5,
                "q75": [8.0] * 5,
                "q95": [9.0] * 5,
                "horizon_type": ["day"] * 5,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(20.0)  # (10+20+30)/3

    def test_leap_year_bounds_without_tag_library(self, monkeypatch):
        """Feb 25 leap year: 4-day period (Feb 26-29). Mar 1 must be rejected."""
        import src.data_reader as dr

        monkeypatch.setattr(dr, "TAG_LIBRARY_AVAILABLE", False)
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 5,
                "date": ["2024-02-25"] * 5,
                "target": [
                    "2024-02-26",
                    "2024-02-27",
                    "2024-02-28",
                    "2024-02-29",
                    "2024-03-01",
                ],
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 999.0],
                "q05": [1.0] * 5,
                "q25": [2.0] * 5,
                "q75": [8.0] * 5,
                "q95": [9.0] * 5,
                "horizon_type": ["day"] * 5,
            }
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(25.0)  # (10+20+30+40)/4


# ---------------------------------------------------------------------------
# Fix 3: semantic dedup includes horizon_value and horizon_in_year
# ---------------------------------------------------------------------------


class TestSemanticDedupFields:
    """horizon_value and horizon_in_year are semantic payload fields — rows that
    differ on these values are genuine conflicts, not duplicates."""

    @staticmethod
    def _day_frame(issue_date_str, targets, discharges, **extra):
        n = len(targets)
        d = {
            "code": ["19999"] * n,
            "date": [issue_date_str] * n,
            "target": targets,
            "forecasted_discharge": discharges,
            "q05": [1.0] * n,
            "q25": [2.0] * n,
            "q75": [8.0] * n,
            "q95": [9.0] * n,
            "horizon_type": ["day"] * n,
        }
        d.update(extra)
        return pd.DataFrame(d)

    def test_horizon_value_conflict_excludes_group(self):
        """Two Jan 6 rows with same numerics but different horizon_value → conflict → excluded."""
        targets = [
            "2024-01-06",
            "2024-01-06",
            "2024-01-07",
            "2024-01-08",
            "2024-01-09",
            "2024-01-10",
        ]
        discharges = [20.0] * 6
        raw = self._day_frame(
            "2024-01-05",
            targets,
            discharges,
            horizon_value=[1, 2, 1, 1, 1, 1],
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    def test_horizon_in_year_conflict_excludes_group(self):
        """Two Jan 6 rows with same numerics but different horizon_in_year → conflict → excluded."""
        targets = [
            "2024-01-06",
            "2024-01-06",
            "2024-01-07",
            "2024-01-08",
            "2024-01-09",
            "2024-01-10",
        ]
        discharges = [20.0] * 6
        raw = self._day_frame(
            "2024-01-05",
            targets,
            discharges,
            horizon_in_year=[2, 3, 2, 2, 2, 2],
        )
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty


# ---------------------------------------------------------------------------
# Recovery reader tests (P1): read_day_forecasts_for_recovery
# ---------------------------------------------------------------------------

_START = dt.date(2026, 10, 1)
_END = dt.date(2026, 10, 31)
_FD = dt.date(2026, 10, 30)


def _day_page(code="19999", n=3, issue="2026-10-05", model_type="TFT") -> pd.DataFrame:
    """Return a minimal valid DAY forecast page with all required identity columns.

    horizon_value and horizon_in_year are set to the day-of-year (DOY) of each
    target date, matching the api_writer contract.  2026 is not a leap year:
    Jan-Sep = 273 days; Oct 6 = DOY 279, Oct 7 = 280, etc.
    """
    rows = []
    for i in range(n):
        target_date = dt.date(2026, 10, 6 + i)
        doy = target_date.timetuple().tm_yday
        rows.append(
            {
                "id": 1000 + i,
                "code": code,
                "date": issue,
                "target": target_date.isoformat(),
                "model_type": model_type,
                "forecasted_discharge": float(100 + i),
                "q05": float(80 + i),
                "q25": float(90 + i),
                "q75": float(110 + i),
                "q95": float(120 + i),
                "flag": None,
                "horizon_type": "day",
                "horizon_value": doy,
                "horizon_in_year": doy,
            }
        )
    return pd.DataFrame(rows)


def _combined_period_page(
    code="19999",
    n=3,
    issue="2026-10-05",
    model_type="TFT",
    horizon_type="pentad",
) -> pd.DataFrame:
    """Return a minimal valid combined period forecast page.

    Each row uses a distinct boundary issue date; target = issue + 1 day.
    horizon_value = period-in-month(target), horizon_in_year = period-in-year(target).
    composition is included (ForecastBase required field, finding 5).

    Pentad issue dates cycle through boundary days within the [Oct1-Oct30] window
    (all ≤ forecast_date=Oct30): Oct5(→Oct6,p2/y56), Oct10(→Oct11,p3/y57),
    Oct15(→Oct16,p4/y58), Oct20(→Oct21,p5/y59), Oct25(→Oct26,p6/y60).

    Decade issue dates: Oct10(→Oct11,d2/y29), Oct20(→Oct21,d3/y30).
    """
    import datetime as _dt

    def _pim(d: _dt.date, ht: str) -> int:
        if ht == "pentad":
            if d.day <= 5:
                return 1
            elif d.day <= 10:
                return 2
            elif d.day <= 15:
                return 3
            elif d.day <= 20:
                return 4
            elif d.day <= 25:
                return 5
            return 6
        if d.day <= 10:
            return 1
        elif d.day <= 20:
            return 2
        return 3

    def _piy(d: _dt.date, ht: str) -> int:
        ppm = 6 if ht == "pentad" else 3
        return (d.month - 1) * ppm + _pim(d, ht)

    if horizon_type == "pentad":
        _issue_dates = [
            _dt.date(2026, 10, 5),
            _dt.date(2026, 10, 10),
            _dt.date(2026, 10, 15),
            _dt.date(2026, 10, 20),
            _dt.date(2026, 10, 25),
        ]
    elif horizon_type == "decade":
        _issue_dates = [_dt.date(2026, 10, 10), _dt.date(2026, 10, 20)]
    else:
        # fallback for unknown types — use pentad schedule
        _issue_dates = [_dt.date(2026, 10, 5 + i * 5) for i in range(5)]

    rows = []
    for i in range(n):
        iss = _issue_dates[i % len(_issue_dates)]
        tgt = iss + _dt.timedelta(days=1)
        rows.append(
            {
                "id": 2000 + i,
                "code": code,
                "date": iss.isoformat(),
                "target": tgt.isoformat(),
                "model_type": model_type,
                "forecasted_discharge": float(300 + i),
                "q05": float(270 + i),
                "q25": float(285 + i),
                "q75": float(315 + i),
                "q95": float(330 + i),
                "flag": None,
                "composition": model_type,
                "horizon_type": horizon_type,
                "horizon_value": _pim(tgt, horizon_type),
                "horizon_in_year": _piy(tgt, horizon_type),
            }
        )
    return pd.DataFrame(rows)


def _lr_page(code="19999", n=3, issue="2026-10-05", horizon_type="pentad") -> pd.DataFrame:
    """Minimal valid LR page matching actual LRForecastBase schema."""
    rows = []
    for i in range(n):
        rows.append(
            {
                "code": code,
                "date": f"2026-10-{5 + i:02d}",
                "horizon_type": horizon_type,
                "horizon_value": i + 1,
                "horizon_in_year": i + 1,
                "forecasted_discharge": float(200 + i),
            }
        )
    return pd.DataFrame(rows)


def _df_to_raw(df: pd.DataFrame | None) -> list:
    """Convert a DataFrame page to raw list-of-dicts (NaN → None).

    Mirrors what client._get returns: [] for empty/None, list-of-dicts otherwise.
    Readers now call client._get directly (finding 4 raw-shape adapter).
    """
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        return []
    return df.where(pd.notna(df), other=None).to_dict(orient="records")


def _fake_client(pages_by_code: dict | None = None, lr_pages: dict | None = None) -> MagicMock:
    """Build a fake client whose _get dispatches on endpoint, returning list-of-dicts.

    Readers call client._get directly (finding 4).  Each pages_by_code/lr_pages
    value is a list of DataFrames returned in order; exhausted → [] (genuine empty).
    """
    client = MagicMock()
    _call_counts: dict = {}

    def _read_get(endpoint, params=None):
        params = params or {}
        code = params.get("code", "__all__")
        if endpoint == "/lr-forecast/":
            page_list = (lr_pages or {}).get(code, [])
            key = ("lr", code)
        else:
            page_list = (pages_by_code or {}).get(code, [])
            key = ("st", code)
        idx = _call_counts.get(key, 0)
        _call_counts[key] = idx + 1
        if idx >= len(page_list):
            return []
        return _df_to_raw(page_list[idx])

    client._get.side_effect = _read_get
    return client


class TestReadDayForecastsForRecovery:
    def test_none_client_disabled(self):
        status, frame, _ = read_day_forecasts_for_recovery(
            None,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_DISABLED
        assert frame is None

    def test_api_disabled_env_no_calls(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "false")
        client = MagicMock()
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_DISABLED
        client._get.assert_not_called()

    def test_no_codes_confirmed_empty(self):
        client = MagicMock()
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            [],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_EMPTY
        client._get.assert_not_called()

    def test_genuine_empty_response_confirmed_empty(self):
        client = _fake_client(pages_by_code={"19999": [pd.DataFrame()]})
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_EMPTY
        assert frame is not None and frame.empty

    def test_single_page_returns_complete(self):
        page = _day_page(n=2)
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE
        assert len(frame) == 2
        assert reason == ""

    def test_later_page_exception_fails_entire_source(self):
        """After a successful first page, an exception on page 2 → FAILED."""
        page1 = _day_page(n=2)  # n=page_size=2 → full page triggers page 2
        client = MagicMock()
        call_count = {"n": 0}

        def _read(endpoint, params=None):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return _df_to_raw(page1)
            raise RuntimeError("network error on page 2")

        client._get.side_effect = _read
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            page_size=2,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None
        assert "network error" in reason
        assert call_count["n"] == 2  # page 2 was actually reached

    def test_zero_column_frame_fails(self):
        """Raw records missing required fields → schema boundary → FAILED."""
        # [{},{},{}] has 3 rows but no columns, so required keys are absent.
        # The reader fails at schema validation before constructing a DataFrame.
        client = MagicMock()
        client._get.return_value = [{}, {}, {}]
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None
        assert "missing required" in reason

    def test_missing_required_columns_fails(self):
        """Page without 'target' → missing required column → FAILED."""
        bad = pd.DataFrame(
            {"code": ["19999"], "date": ["2026-10-05"], "forecasted_discharge": [100.0]}
        )
        # missing 'target'
        client = _fake_client(pages_by_code={"19999": [bad]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "target" in reason

    def test_wrong_code_in_page_fails(self):
        """Page returning wrong code → identity mismatch → FAILED."""
        page = _day_page(code="99999")  # wrong code returned
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED

    def test_future_issue_date_in_page_fails(self):
        """Page with issue date after forecast_date → FAILED."""
        page = _day_page(issue="2026-11-05")  # after forecast_date 2026-10-30
        page["date"] = "2026-11-05"
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "future" in reason

    def test_repeated_page_detected(self):
        """Repeated skip → FAILED without infinite loop."""
        page = _day_page(n=2)  # n=page_size=2 → full page triggers page 2 request

        call_count = {"n": 0}

        def _read(endpoint, params=None):
            call_count["n"] += 1
            return _df_to_raw(page)  # always returns the same full page

        client = MagicMock()
        client._get.side_effect = _read
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            page_size=2,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "repeated page" in reason

    def test_max_pages_exceeded_fails(self):
        """Hitting max_pages cap → FAILED."""
        page = _day_page(n=2)  # full page at page_size=2 → triggers more pages

        client = MagicMock()
        client._get.return_value = _df_to_raw(page)
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            page_size=2,
            max_pages=1,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "max_pages" in reason

    def test_deadline_exceeded_fails(self):
        """Deadline exceeded before read → FAILED."""
        page = _day_page(n=2)
        client = _fake_client(pages_by_code={"19999": [page]})
        past_deadline = time.monotonic() - 1.0  # already expired
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            total_deadline=past_deadline,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "deadline" in reason

    def test_no_csv_fallback_ever(self, tmp_path, monkeypatch):
        """Even if a stale CSV exists, no CSV is touched (no read_csv call)."""
        csv_path = tmp_path / "combined_forecasts_pentad.csv"
        csv_path.write_text("code,date,model_type\n19999,2026-10-05,TFT\n")
        monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
        monkeypatch.setenv(
            "ieasyforecast_combined_forecast_pentad_file", "combined_forecasts_pentad.csv"
        )
        client = _fake_client(pages_by_code={"19999": []})
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        # Must return EMPTY (genuine API empty), never the CSV data
        assert status == RECOVERY_STATUS_EMPTY
        assert frame is not None and frame.empty


class TestReadLrForecastsForRecovery:
    def test_none_client_disabled(self):
        status, frame, _ = read_lr_forecasts_for_recovery(
            None,
            ["19999"],
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_DISABLED

    def test_invalid_horizon_type_fails(self):
        client = MagicMock()
        status, frame, reason = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "day",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "horizon_type" in reason

    def test_genuine_empty_confirmed_empty(self):
        client = _fake_client(lr_pages={"19999": [pd.DataFrame()]})
        status, frame, _ = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_EMPTY
        assert frame.empty

    def test_single_page_complete(self):
        page = _lr_page(n=2)
        client = _fake_client(lr_pages={"19999": [page]})
        status, frame, reason = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE
        assert len(frame) == 2

    def test_lr_source_independent_of_day_reader(self):
        """LR reader uses /lr-forecast/ endpoint, not /forecast/."""
        page = _lr_page(n=1)
        client = _fake_client(lr_pages={"19999": [page]})
        status, frame, _ = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE
        assert len(frame) == 1
        # All _get calls must target the LR endpoint, never the ST endpoint.
        call_endpoints = [c.args[0] for c in client._get.call_args_list]
        assert all("/lr-forecast/" in ep for ep in call_endpoints)
        assert not any(ep == "/forecast/" for ep in call_endpoints)

    def test_later_page_exception_fails_entire_lr_source(self):
        """After valid page 1, exception on page 2 → FAILED (page 2 actually reached)."""
        page1 = _lr_page(n=2)  # n=page_size=2 → full page triggers page 2 request
        client = MagicMock()
        call_count = {"n": 0}

        def _read(endpoint, params=None):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return _df_to_raw(page1)
            raise RuntimeError("lr page 2 error")

        client._get.side_effect = _read
        status, frame, reason = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "pentad",
            _START,
            _END,
            _FD,
            page_size=2,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None
        assert call_count["n"] == 2  # page 2 was actually reached


# ---------------------------------------------------------------------------
# Strict schema validation tests (_recovery_validate_page via public readers)
# ---------------------------------------------------------------------------


class TestRecoveryValidatePageStrict:
    """Exercise schema checks added in P1 strictreaders by feeding bad pages
    through read_day_forecasts_for_recovery (the validate call is internal)."""

    def test_wrong_model_type_in_page_fails(self):
        """Page with model_type != requested model → FAILED."""
        page = _day_page(model_type="GBT")  # requested TFT, got GBT
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_wrong_horizon_type_in_page_fails(self):
        """Page with horizon_type='pentad' when DAY expected → FAILED."""
        page = _day_page()
        page["horizon_type"] = "pentad"
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_nat_issue_date_fails(self):
        """Page where issue date is NaT → FAILED, not silently accepted."""
        page = _day_page()
        page["date"] = pd.NaT
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_malformed_numeric_string_fails(self):
        """forecasted_discharge containing non-numeric string → FAILED."""
        page = _day_page()
        page["forecasted_discharge"] = ["not_a_number", "100.0", "101.0"]
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_bool_in_numeric_col_fails(self):
        """Bool value in a numeric column → FAILED (bool is a subtype of int)."""
        page = _day_page()
        # Inject bool into object-dtype column
        page["forecasted_discharge"] = page["forecasted_discharge"].astype(object)
        page.at[0, "forecasted_discharge"] = True
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_inf_in_numeric_col_fails(self):
        """Infinite value in q05 column → FAILED."""
        page = _day_page()
        page.at[0, "q05"] = float("inf")
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_null_numeric_value_allowed(self):
        """NaN/null in a numeric column is allowed (known incomplete)."""
        page = _day_page(n=2)
        page.at[0, "forecasted_discharge"] = float("nan")
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE

    def test_missing_q_col_on_second_page_fails(self):
        """First page is valid; second page is missing q75 → entire source FAILED."""
        page1 = _day_page(n=2)  # n=page_size=2 → full page triggers page 2
        page2 = _day_page(n=1)
        page2 = page2.drop(columns=["q75"])
        client = MagicMock()
        call_count = {"n": 0}

        def _read(endpoint, params=None):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return _df_to_raw(page1)
            return _df_to_raw(page2)

        client._get.side_effect = _read
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            page_size=2,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None
        assert "q75" in reason


# ---------------------------------------------------------------------------
# Strict limit/deadline tests
# ---------------------------------------------------------------------------


class TestReadDayForecastsForRecoveryStrict:
    def test_bool_page_size_fails(self):
        """page_size=True (bool subclass of int) → FAILED."""
        client = MagicMock()
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            page_size=True,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "page_size" in reason

    def test_bool_max_pages_fails(self):
        """max_pages=False → FAILED."""
        client = MagicMock()
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            max_pages=False,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "max_pages" in reason

    def test_non_finite_deadline_fails(self):
        """total_deadline=inf → FAILED (must be finite)."""
        client = MagicMock()
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            total_deadline=float("inf"),
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "deadline" in reason

    def test_deadline_exceeded_after_successful_call_late_success(self):
        """Deadline fires AFTER a successful call → FAILED (not COMPLETE)."""
        page = _day_page(n=2)
        clock_calls = {"n": 0}

        def _clock():
            clock_calls["n"] += 1
            # Before: 0.0; after first API call: deadline + 1.0 → exceeded
            return 0.0 if clock_calls["n"] <= 2 else 999999.0

        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            total_deadline=1.0,
            _clock=_clock,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "deadline" in reason

    def test_deadline_exceeded_after_empty_final_page_late_empty(self):
        """Deadline fires after an empty final page → FAILED (not EMPTY)."""
        clock_calls = {"n": 0}

        def _clock():
            clock_calls["n"] += 1
            return 0.0 if clock_calls["n"] <= 2 else 999999.0

        client = _fake_client(pages_by_code={"19999": [pd.DataFrame()]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            total_deadline=1.0,
            _clock=_clock,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "deadline" in reason

    def test_repeated_page_new_ids_detected(self):
        """Same data with different IDs must still be detected as repeated."""
        base = _day_page(n=2)  # n=page_size=2 → full page triggers page 2
        page2 = base.copy()
        page2["id"] = base["id"] + 10000  # new IDs, same data

        call_count = {"n": 0}

        def _read(endpoint, params=None):
            call_count["n"] += 1
            return _df_to_raw(base) if call_count["n"] == 1 else _df_to_raw(page2)

        client = MagicMock()
        client._get.side_effect = _read
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            page_size=2,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "repeated page" in reason
        assert call_count["n"] == 2  # page 2 was actually requested

    def test_exact_duplicate_rows_collapsed_not_conflict(self):
        """Identical rows across two full pages collapse to unique rows → COMPLETE."""
        page1 = _day_page(n=2)  # n=page_size=2 → triggers page2
        # page2: exact copy of page1 row0 (same all writable fields)
        page2 = page1.iloc[[0]].copy()
        page2["id"] = page2["id"] + 5000  # different id, all other fields same

        call_count = {"n": 0}

        def _read(endpoint, params=None):
            call_count["n"] += 1
            return _df_to_raw(page1) if call_count["n"] == 1 else _df_to_raw(page2)

        client = MagicMock()
        client._get.side_effect = _read
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            page_size=2,
        )
        assert status == RECOVERY_STATUS_COMPLETE
        # page1 row0 and page2 row0 are identical writable-field duplicates → collapsed to 1
        assert len(frame) == 2
        assert call_count["n"] == 2  # page 2 was actually requested

    def test_full_key_conflict_across_pages_fails(self):
        """Same identity keys but different forecasted_discharge → FAILED."""
        page1 = _day_page(n=2)  # n=page_size=2 → triggers page2

        # page2 row0: same identity key as page1 row0 but different discharge
        page2 = _day_page(n=1)
        page2["target"] = [page1["target"].iloc[0]]
        page2["forecasted_discharge"] = [777.0]
        page2["id"] = [5001]

        call_count = {"n": 0}

        def _read(endpoint, params=None):
            call_count["n"] += 1
            return _df_to_raw(page1) if call_count["n"] == 1 else _df_to_raw(page2)

        client = MagicMock()
        client._get.side_effect = _read
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            page_size=2,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None


# ---------------------------------------------------------------------------
# LR strict tests
# ---------------------------------------------------------------------------


class TestReadLrForecastsForRecoveryStrict:
    def test_bool_page_size_fails(self):
        client = MagicMock()
        status, frame, reason = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "pentad",
            _START,
            _END,
            _FD,
            page_size=True,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "page_size" in reason

    def test_non_finite_deadline_fails(self):
        client = MagicMock()
        status, frame, reason = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "pentad",
            _START,
            _END,
            _FD,
            total_deadline=float("nan"),
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "deadline" in reason

    def test_forecast_date_before_start_date_fails(self):
        """forecast_date < start_date → FAILED (window cannot produce valid data)."""
        client = MagicMock()
        status, frame, reason = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "pentad",
            start_date=dt.date(2026, 10, 10),
            end_date=dt.date(2026, 10, 31),
            forecast_date=dt.date(2026, 10, 5),  # before start_date
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "forecast_date" in reason or "start_date" in reason

    def test_deadline_after_call_late_success(self):
        """Deadline fires after call returning data → FAILED."""
        page = _lr_page(n=2)
        clock_calls = {"n": 0}

        def _clock():
            clock_calls["n"] += 1
            return 0.0 if clock_calls["n"] <= 2 else 999999.0

        client = _fake_client(lr_pages={"19999": [page]})
        status, frame, reason = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "pentad",
            _START,
            _END,
            _FD,
            total_deadline=1.0,
            _clock=_clock,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "deadline" in reason

    def test_repeated_page_new_ids_detected(self):
        """LR repeated page with new IDs must be detected."""
        page = _lr_page(n=2)  # n=page_size=2 → triggers page 2 request
        page2 = page.copy()
        # LR pages may not have an "id" column — fingerprint on all columns
        call_count = {"n": 0}

        def _read(endpoint, params=None):
            call_count["n"] += 1
            return _df_to_raw(page) if call_count["n"] == 1 else _df_to_raw(page2)

        client = MagicMock()
        client._get.side_effect = _read
        status, frame, reason = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "pentad",
            _START,
            _END,
            _FD,
            page_size=2,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "repeated page" in reason
        assert call_count["n"] == 2


# ---------------------------------------------------------------------------
# Combined period reader tests
# ---------------------------------------------------------------------------


class TestReadCombinedPeriodForecastsForRecovery:
    def test_none_client_disabled(self):
        status, frame, _ = read_combined_period_forecasts_for_recovery(
            None,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_DISABLED
        assert frame is None

    def test_api_disabled_env_no_calls(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "false")
        client = MagicMock()
        status, frame, _ = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_DISABLED
        client._get.assert_not_called()

    def test_no_codes_confirmed_empty(self):
        client = MagicMock()
        status, frame, _ = read_combined_period_forecasts_for_recovery(
            client,
            [],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_EMPTY
        client._get.assert_not_called()

    def test_unsupported_horizon_type_fails(self):
        client = MagicMock()
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "day",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "horizon_type" in reason

    def test_genuine_empty_confirmed_empty(self):
        client = _fake_client(pages_by_code={"19999": [pd.DataFrame()]})
        status, frame, _ = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_EMPTY
        assert frame is not None and frame.empty

    def test_single_page_pentad_complete(self):
        page = _combined_period_page(n=2, horizon_type="pentad")
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE
        assert len(frame) == 2
        assert reason == ""

    def test_single_page_decad_complete(self):
        page = _combined_period_page(n=2, horizon_type="decade")
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "decad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE
        assert len(frame) == 2

    def test_wrong_horizon_type_in_page_fails(self):
        """Page returns 'day' instead of the requested 'pentad' → FAILED."""
        page = _combined_period_page(n=2, horizon_type="day")
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_wrong_model_in_page_fails(self):
        page = _combined_period_page(n=2, model_type="LR", horizon_type="pentad")
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED

    def test_bool_page_size_fails(self):
        client = MagicMock()
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
            page_size=True,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "page_size" in reason

    def test_non_finite_deadline_fails(self):
        client = MagicMock()
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
            total_deadline=float("inf"),
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "deadline" in reason

    def test_id_stripped_from_output(self):
        """The `id` column from the API must not appear in the returned frame."""
        page = _combined_period_page(n=2, horizon_type="pentad")
        assert "id" in page.columns
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE
        assert "id" not in frame.columns

    def test_later_page_exception_fails_entire_source(self):
        """After valid page 1, exception on page 2 → FAILED (page 2 reached)."""
        page1 = _combined_period_page(n=2, horizon_type="pentad")  # n=page_size=2
        call_count = {"n": 0}

        def _read(endpoint, params=None):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return _df_to_raw(page1)
            raise RuntimeError("network error on page 2")

        client = MagicMock()
        client._get.side_effect = _read
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
            page_size=2,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None
        assert call_count["n"] == 2  # page 2 was actually reached


# ---------------------------------------------------------------------------
# Normalizer pipeline integration: validated DAY frame passes through
# ---------------------------------------------------------------------------


class TestNormalizeDayFrameFromRecovery:
    """Routes a validated DAY frame through the public recovery reader and real normalizer."""

    def test_validated_day_frame_passes_through_normalize(self):
        """A well-formed DAY page from the public reader survives normalizer pipeline."""
        rows = []
        for i in range(5):
            # horizon_value/horizon_in_year must equal DOY of the target date.
            # 2026-10-06 = DOY 279; 2026-10-10 = DOY 283.
            doy = dt.date(2026, 10, 6 + i).timetuple().tm_yday
            rows.append(
                {
                    "id": 3000 + i,
                    "code": "19999",
                    "date": "2026-10-05",
                    "target": f"2026-10-{6 + i:02d}",
                    "model_type": "TFT",
                    "forecasted_discharge": float(150 + i * 5),
                    "q05": float(130 + i),
                    "q25": float(140 + i),
                    "q75": float(160 + i),
                    "q95": float(170 + i),
                    "flag": None,
                    "horizon_type": "day",
                    "horizon_value": doy,
                    "horizon_in_year": doy,
                }
            )
        page = pd.DataFrame(rows)
        client = _fake_client(pages_by_code={"19999": [page]})
        status, raw_frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE

        normalized = _normalize_ml_forecasts(raw_frame, "TFT", "pentad")
        assert not normalized.empty
        assert "forecasted_discharge" in normalized.columns

        filtered = _apply_day_coverage_filter(normalized, "TFT", "pentad")
        assert isinstance(filtered, pd.DataFrame)

    def test_validated_day_frame_missing_period_candidate_not_accepted(self):
        """A DAY page with only 2 targets → below coverage threshold → empty after filter."""
        rows = []
        for i in range(2):  # only 2 targets — below minimum coverage
            # horizon_value/horizon_in_year must equal DOY of the target date.
            # 2026-10-06 = DOY 279, 2026-10-07 = DOY 280.
            doy = dt.date(2026, 10, 6 + i).timetuple().tm_yday
            rows.append(
                {
                    "id": 4000 + i,
                    "code": "19999",
                    "date": "2026-10-05",
                    "target": f"2026-10-{6 + i:02d}",
                    "model_type": "TFT",
                    "forecasted_discharge": float(150 + i),
                    "q05": float(130 + i),
                    "q25": float(140 + i),
                    "q75": float(160 + i),
                    "q95": float(170 + i),
                    "flag": None,
                    "horizon_type": "day",
                    "horizon_value": doy,
                    "horizon_in_year": doy,
                }
            )
        page = pd.DataFrame(rows)
        client = _fake_client(pages_by_code={"19999": [page]})
        status, raw_frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE
        normalized = _normalize_ml_forecasts(raw_frame, "TFT", "pentad")
        filtered = _apply_day_coverage_filter(normalized, "TFT", "pentad")
        assert filtered.empty  # 2 targets is below coverage threshold → no pentad output


# ---------------------------------------------------------------------------
# A: _RECOVERY_DAY_IDENTITY_COLS excludes flag/horizon_value/horizon_in_year
# ---------------------------------------------------------------------------


class TestRecoveryIdentityColsA:
    """A: flag, horizon_value, horizon_in_year are VALUE fields, not DB identity.
    Same (code,date,target,model_type,horizon_type) with different flag/ints
    must be detected as a conflict across pages."""

    def test_same_key_different_flag_fails(self):
        """Two rows with same DB key but different flag across pages → conflict → FAILED."""
        page1 = _day_page(n=2)  # n=page_size=2 → triggers page2
        page1["flag"] = [0, 0]

        # page2 row0: same identity key as page1 row0, different flag
        page2 = _day_page(n=1)
        page2["target"] = [page1["target"].iloc[0]]
        page2["flag"] = [1]  # different flag → conflict

        call_count = {"n": 0}

        def _read(endpoint, params=None):
            call_count["n"] += 1
            return _df_to_raw(page1) if call_count["n"] == 1 else _df_to_raw(page2)

        client = MagicMock()
        client._get.side_effect = _read
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            page_size=2,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_same_key_different_horizon_value_fails(self):
        """Two rows with same (code,date,target,model_type,horizon_type) but
        different horizon_value across pages → conflict → FAILED."""
        page1 = _day_page(n=2)  # n=page_size=2 → triggers page2

        # page2 row0: same identity key as page1 row0, different horizon_value
        page2 = _day_page(n=1)
        page2["target"] = [page1["target"].iloc[0]]
        page2["horizon_value"] = [999]  # different value → conflict

        call_count = {"n": 0}

        def _read(endpoint, params=None):
            call_count["n"] += 1
            return _df_to_raw(page1) if call_count["n"] == 1 else _df_to_raw(page2)

        client = MagicMock()
        client._get.side_effect = _read
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            page_size=2,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_same_key_different_horizon_in_year_fails(self):
        """Same DB key but different horizon_in_year across pages → FAILED."""
        page1 = _day_page(n=2)  # n=page_size=2 → triggers page2

        page2 = _day_page(n=1)
        page2["target"] = [page1["target"].iloc[0]]
        page2["horizon_in_year"] = [999]  # different value → conflict

        call_count = {"n": 0}

        def _read(endpoint, params=None):
            call_count["n"] += 1
            return _df_to_raw(page1) if call_count["n"] == 1 else _df_to_raw(page2)

        client = MagicMock()
        client._get.side_effect = _read
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            page_size=2,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_identical_flag_and_ints_across_pages_collapses(self):
        """Exact duplicate rows (same DB key + same flag/ints) across pages collapse
        to unique rows → not a conflict → COMPLETE."""
        page1 = _day_page(n=2)  # n=page_size=2 → triggers page2
        page1["flag"] = [0, 0]
        # page2: exact copy of page1 row0 (all writable fields identical)
        page2 = page1.iloc[[0]].copy()
        page2["id"] = page2["id"] + 9000  # different id, same everything else

        call_count = {"n": 0}

        def _read(endpoint, params=None):
            call_count["n"] += 1
            return _df_to_raw(page1) if call_count["n"] == 1 else _df_to_raw(page2)

        client = MagicMock()
        client._get.side_effect = _read
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            page_size=2,
        )
        assert status == RECOVERY_STATUS_COMPLETE
        assert len(frame) == 2  # exact duplicate collapses; 2 unique rows remain
        assert call_count["n"] == 2  # page 2 was actually requested


# ---------------------------------------------------------------------------
# B: strict code/date validation in _recovery_validate_page
# ---------------------------------------------------------------------------


class TestRecoveryValidatePageB:
    """B: exact code match (no strip, no numeric coercion); canonical naive-midnight
    dates only (reject time component, reject timezone)."""

    def test_code_with_leading_space_fails(self):
        """Code with leading whitespace → exact match fails → FAILED."""
        page = _day_page()
        page["code"] = " 19999"  # leading space
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_code_with_trailing_space_fails(self):
        """Code with trailing whitespace → exact match fails → FAILED."""
        page = _day_page()
        page["code"] = "19999 "
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_numeric_string_code_fails(self):
        """Code returned as '19999.0' (numeric float string) → exact match fails."""
        page = _day_page()
        page["code"] = "19999.0"  # numeric coercion artifact
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_non_midnight_issue_date_fails(self):
        """Issue date with time component (non-midnight) → FAILED."""
        page = _day_page()
        # Inject a non-midnight timestamp as a string that pandas parses with time.
        page["date"] = pd.to_datetime("2026-10-05T12:30:00")
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_timezone_aware_issue_date_fails(self):
        """Issue date with timezone → FAILED."""
        page = _day_page()
        page["date"] = pd.to_datetime("2026-10-05").tz_localize("UTC")
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_plain_date_string_accepted(self):
        """Plain ISO date string (canonical midnight naive) → COMPLETE."""
        page = _day_page(n=1)
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE


# ---------------------------------------------------------------------------
# C: LR schema validation — horizon required, wrong horizon rejected
# ---------------------------------------------------------------------------


class TestRecoveryLRSchemaC:
    """C: LR recovery reader requires horizon_type/horizon_value/horizon_in_year;
    returned horizon_type must match the requested horizon."""

    def test_wrong_horizon_type_in_lr_page_fails(self):
        """LR page with horizon_type='month' when 'pentad' requested → FAILED."""
        page = _lr_page(horizon_type="month")
        client = _fake_client(lr_pages={"19999": [page]})
        status, frame, reason = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_correct_horizon_type_accepted(self):
        """LR page with horizon_type='pentad' → COMPLETE."""
        page = _lr_page(horizon_type="pentad")
        client = _fake_client(lr_pages={"19999": [page]})
        status, frame, reason = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE

    def test_decad_lr_page_requires_decade_horizon(self):
        """LR decad request: API horizon is 'decade'; page returning 'pentad' → FAILED."""
        page = _lr_page(horizon_type="pentad")  # wrong; should be 'decade'
        client = _fake_client(lr_pages={"19999": [page]})
        status, frame, reason = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "decad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_decad_lr_page_with_decade_passes(self):
        """LR decad: page returning 'decade' horizon_type → COMPLETE."""
        page = _lr_page(horizon_type="decade")
        client = _fake_client(lr_pages={"19999": [page]})
        status, frame, reason = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "decad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE

    def test_lr_missing_horizon_type_column_fails(self):
        """LR page missing horizon_type column → FAILED (required schema)."""
        page = _lr_page().drop(columns=["horizon_type"])
        client = _fake_client(lr_pages={"19999": [page]})
        status, frame, reason = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "horizon_type" in reason

    def test_lr_null_forecasted_discharge_allowed(self):
        """LR nullable forecasted_discharge = null → COMPLETE (known incomplete)."""
        page = _lr_page(n=2)
        page.at[0, "forecasted_discharge"] = float("nan")
        client = _fake_client(lr_pages={"19999": [page]})
        status, frame, reason = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE

    def test_lr_inf_forecasted_discharge_fails(self):
        """LR infinite forecasted_discharge → FAILED (must be finite real or null)."""
        page = _lr_page(n=2)
        page.at[0, "forecasted_discharge"] = float("inf")
        client = _fake_client(lr_pages={"19999": [page]})
        status, frame, reason = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None


# ---------------------------------------------------------------------------
# E: repeated-page detection covers non-adjacent full pages (A,B,A)
# ---------------------------------------------------------------------------


class TestRepeatedPageDetectionE:
    """E: repeated-page fingerprinting tracks ALL seen full pages, not just
    the immediately preceding one.  A,B,A sequence must be detected."""

    def test_non_adjacent_repeated_full_page_fails(self):
        """Pages A, B, A (non-adjacent): last A must be caught as repeated."""
        page_a = _day_page(n=2)  # targets Oct 06, Oct 07
        page_a["id"] = [1, 2]
        page_b = _day_page(n=2)
        page_b["id"] = [3, 4]
        page_b["target"] = ["2026-10-08", "2026-10-09"]  # different targets → different fp
        page_b["horizon_value"] = [281, 282]  # DOY for Oct 8, Oct 9 in 2026
        page_b["horizon_in_year"] = [281, 282]
        page_b["forecasted_discharge"] = [200.0, 201.0]
        page_a2 = page_a.copy()
        page_a2["id"] = [5, 6]  # new IDs, same payload as A → same fingerprint

        call_count = {"n": 0}

        def _read(endpoint, params=None):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return _df_to_raw(page_a)
            if call_count["n"] == 2:
                return _df_to_raw(page_b)
            return _df_to_raw(page_a2)  # repeated A with different IDs

        client = MagicMock()
        client._get.side_effect = _read
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            page_size=2,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "repeated page" in reason
        assert call_count["n"] == 3  # all three pages requested

    def test_oversized_page_fails(self):
        """API returns more rows than page_size → FAILED."""
        page = _day_page(n=10)  # 10 > page_size=5
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            page_size=5,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "oversized" in reason

    def test_adjacent_repeated_page_still_fails(self):
        """Original A,A pattern (adjacent) still detected."""
        page = _day_page(n=2)  # n=page_size=2 → full page triggers page 2
        page2 = page.copy()
        page2["id"] = page["id"] + 10000  # new IDs, same payload → same fingerprint

        call_count = {"n": 0}

        def _read(endpoint, params=None):
            call_count["n"] += 1
            return _df_to_raw(page) if call_count["n"] == 1 else _df_to_raw(page2)

        client = MagicMock()
        client._get.side_effect = _read
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            page_size=2,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "repeated page" in reason
        assert call_count["n"] == 2

    def test_combined_period_non_adjacent_repeated_fails(self):
        """Non-adjacent repeated full page in combined-period reader → FAILED."""
        page_a = _combined_period_page(n=2, horizon_type="pentad")  # targets Oct 06, Oct 11
        page_a["id"] = [10, 11]
        page_b = _combined_period_page(n=2, horizon_type="pentad")
        page_b["id"] = [20, 21]
        page_b["target"] = ["2026-10-16", "2026-10-21"]  # different targets → different fp
        page_b["date"] = ["2026-10-15", "2026-10-20"]  # issue_date+1 must equal target
        page_b["horizon_value"] = [4, 5]  # pim(Oct16,pentad)=4, pim(Oct21,pentad)=5
        page_b["horizon_in_year"] = [58, 59]  # (Oct=month9)*6+4=58, *6+5=59
        page_b["forecasted_discharge"] = [400.0, 401.0]
        page_a2 = page_a.copy()
        page_a2["id"] = [30, 31]  # new IDs, same payload as A → same fingerprint

        call_count = {"n": 0}

        def _read(endpoint, params=None):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return _df_to_raw(page_a)
            if call_count["n"] == 2:
                return _df_to_raw(page_b)
            return _df_to_raw(page_a2)

        client = MagicMock()
        client._get.side_effect = _read
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
            page_size=2,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "repeated page" in reason
        assert call_count["n"] == 3

    def test_lr_oversized_page_fails(self):
        """LR API returns more rows than page_size → FAILED."""
        page = _lr_page(n=10)
        client = _fake_client(lr_pages={"19999": [page]})
        status, frame, reason = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "pentad",
            _START,
            _END,
            _FD,
            page_size=5,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "oversized" in reason


# ---------------------------------------------------------------------------
# Finding 1: target range + period boundary + horizon integer validation
# ---------------------------------------------------------------------------


class TestRecoveryTargetAndHorizonIntValidation:
    """Finding 1: target range, period boundary, and horizon integer checks."""

    def test_day_target_before_range_fails(self):
        """DAY target before issue_date+1 → FAILED."""
        page = _day_page(n=1)
        page["target"] = "2026-10-04"  # before issue+1=Oct 06
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_day_target_after_range_fails(self):
        """DAY target after issue_date+11 → FAILED."""
        page = _day_page(n=1)
        page["target"] = "2026-10-17"  # after issue+11=Oct 16
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_day_target_on_issue_date_fails(self):
        """DAY target same as issue date (issue+0) → FAILED (min is issue+1)."""
        page = _day_page(n=1)
        page["target"] = "2026-10-05"  # same as issue date
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_pentad_target_non_boundary_day_fails(self):
        """Pentad target on Oct 10 (not in [1,6,11,16,21,26]) → FAILED."""
        page = _combined_period_page(n=1, horizon_type="pentad")
        page["target"] = "2026-10-10"  # 10 is not a valid pentad start
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_decade_target_non_boundary_day_fails(self):
        """Decade target on Oct 6 (not in [1,11,21]) → FAILED."""
        page = _combined_period_page(n=1, horizon_type="decade")
        page["target"] = "2026-10-06"  # 6 is not a valid decad boundary
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "decad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_horizon_in_year_null_fails(self):
        """horizon_in_year=None → FAILED (non-nullable integer)."""
        page = _day_page(n=1)
        page = page.astype(object)
        page.at[0, "horizon_in_year"] = None
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_horizon_in_year_fractional_fails(self):
        """horizon_in_year=1.5 (non-integer float) → FAILED."""
        page = _day_page(n=1)
        page = page.astype(object)
        page.at[0, "horizon_in_year"] = 1.5
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_horizon_value_bool_fails(self):
        """horizon_value=True (bool, subtype of int) → FAILED."""
        page = _day_page(n=1)
        page = page.astype(object)
        page.at[0, "horizon_value"] = True
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_combined_period_missing_composition_fails(self):
        """Combined period page missing composition field → FAILED (required writable)."""
        page = _combined_period_page(n=1, horizon_type="pentad")
        page = page.drop(columns=["composition"])
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "composition" in reason


# ---------------------------------------------------------------------------
# Finding 2: microseconds in issue date must be rejected
# ---------------------------------------------------------------------------


class TestRecoveryMicrosecondDateValidation:
    """Finding 2: microseconds and sub-second components in issue date are rejected."""

    def test_issue_date_with_microseconds_fails(self):
        """Issue date with microseconds → not canonical midnight → FAILED."""
        page = _day_page(n=1)
        page["date"] = pd.to_datetime("2026-10-05T00:00:00.000001")
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_issue_date_with_hours_fails(self):
        """Issue date at non-midnight hour → FAILED (already covered but guard holds)."""
        page = _day_page(n=1)
        page["date"] = pd.to_datetime("2026-10-05T01:00:00")
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None


# ---------------------------------------------------------------------------
# Finding 3: LR reader deduplication and conflict detection
# ---------------------------------------------------------------------------


class TestLRRecoveryDeduplication:
    """Finding 3: LR identity key (horizon_type, code, date); exact duplicates
    collapsed across pages; conflicting writable values fail the entire source."""

    def test_lr_exact_duplicate_rows_collapsed(self):
        """Exact duplicate LR rows across two pages → COMPLETE with deduped rows."""
        page1 = _lr_page(n=2)  # n=page_size=2 → triggers page2
        page2 = page1.iloc[[0]].copy()  # exact copy of row0

        call_count = {"n": 0}

        def _read(endpoint, params=None):
            call_count["n"] += 1
            return _df_to_raw(page1) if call_count["n"] == 1 else _df_to_raw(page2)

        client = MagicMock()
        client._get.side_effect = _read
        status, frame, reason = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "pentad",
            _START,
            _END,
            _FD,
            page_size=2,
        )
        assert status == RECOVERY_STATUS_COMPLETE
        assert len(frame) == 2  # page1 row0 and page2 row0 are duplicates → 2 unique rows
        assert call_count["n"] == 2

    def test_lr_conflicting_discharge_same_key_fails(self):
        """Two rows with same LR identity key (horizon_type, code, date) but different
        forecasted_discharge across pages → conflict → FAILED."""
        page1 = _lr_page(n=2)  # n=page_size=2 → triggers page2

        # page2 row: same (code, date, horizon_type) as page1 row0, different discharge
        page2 = _lr_page(n=1)
        page2["date"] = [page1["date"].iloc[0]]
        page2["horizon_type"] = [page1["horizon_type"].iloc[0]]
        page2["forecasted_discharge"] = [999.9]  # different value

        call_count = {"n": 0}

        def _read(endpoint, params=None):
            call_count["n"] += 1
            return _df_to_raw(page1) if call_count["n"] == 1 else _df_to_raw(page2)

        client = MagicMock()
        client._get.side_effect = _read
        status, frame, reason = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "pentad",
            _START,
            _END,
            _FD,
            page_size=2,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None


# ---------------------------------------------------------------------------
# Finding 4: raw JSON shape validation via _recovery_fetch_raw_page
# ---------------------------------------------------------------------------


class TestRecoveryRawShapeValidation:
    """Finding 4: null/false/{} raw API responses must be rejected before
    the client's lossy DataFrame conversion swallows them."""

    def test_null_raw_response_fails(self):
        """_get returns None → falsy non-list → FAILED."""
        client = MagicMock()
        client._get.return_value = None
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_false_raw_response_fails(self):
        """_get returns False → falsy non-list → FAILED."""
        client = MagicMock()
        client._get.return_value = False
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_empty_dict_raw_response_fails(self):
        """_get returns {} → non-list type → FAILED (not an empty response)."""
        client = MagicMock()
        client._get.return_value = {}
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_empty_list_raw_response_confirmed_empty(self):
        """_get returns [] → legitimate empty response → CONFIRMED_EMPTY."""
        client = MagicMock()
        client._get.return_value = []
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_EMPTY

    def test_list_with_nondict_element_fails(self):
        """_get returns list with non-dict element → shape error → FAILED."""
        client = MagicMock()
        client._get.return_value = ["not_a_dict"]
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_null_raw_lr_fails(self):
        """LR reader: _get returns None → FAILED (same adapter used)."""
        client = MagicMock()
        client._get.return_value = None
        status, frame, _ = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_null_raw_combined_period_fails(self):
        """Combined period reader: _get returns None → FAILED."""
        client = MagicMock()
        client._get.return_value = None
        status, frame, _ = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None


# ---------------------------------------------------------------------------
# Finding 6: model alias validation (TiDE/TSMixer accepted; TIDE/LR rejected)
# ---------------------------------------------------------------------------


class TestRecoveryModelAliasValidation:
    """Finding 6: explicit supported-model enum; TiDE/TSMixer accepted by name;
    wrong-case and unsupported models rejected before any API call."""

    def test_tide_model_accepted_for_day(self):
        """TiDE is a valid DAY ML model name → COMPLETE."""
        page = _day_page(n=1, model_type="TiDE")
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TiDE",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE

    def test_tsmixer_model_accepted_for_day(self):
        """TSMixer is a valid DAY ML model name → COMPLETE."""
        page = _day_page(n=1, model_type="TSMixer")
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TSMixer",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE

    def test_tide_alias_accepted_for_day(self):
        """TIDE is a known alias for TiDE → resolved to TiDE → COMPLETE.

        The request is sent with canonical name TiDE; the response page also
        carries model_type='TiDE'.
        """
        page = _day_page(n=1, model_type="TiDE")
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TIDE",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE
        assert frame is not None

    def test_tsmixer_alias_accepted_for_day(self):
        """TSMIXER is a known alias for TSMixer → resolved → COMPLETE."""
        page = _day_page(n=1, model_type="TSMixer")
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TSMIXER",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE
        assert frame is not None

    def test_lr_rejected_for_day_reader(self):
        """LR is not a valid DAY model → FAILED before API call."""
        client = MagicMock()
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "LR",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None
        client._get.assert_not_called()

    def test_linear_regression_rejected_for_day_reader(self):
        """LINEAR_REGRESSION is not a valid DAY model → FAILED before API call."""
        client = MagicMock()
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "LINEAR_REGRESSION",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None
        client._get.assert_not_called()

    def test_ne_model_accepted_for_period(self):
        """NE is a valid combined period model → COMPLETE."""
        page = _combined_period_page(n=1, model_type="NE", horizon_type="pentad")
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "NE",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE

    def test_neural_ensemble_alias_accepted_for_period(self):
        """NEURAL_ENSEMBLE is a known alias for NE → resolved → COMPLETE."""
        page = _combined_period_page(n=1, model_type="NE", horizon_type="pentad")
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "NEURAL_ENSEMBLE",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE
        assert frame is not None

    def test_em_model_accepted_for_period(self):
        """EM is a valid combined period model → COMPLETE."""
        page = _combined_period_page(n=1, model_type="EM", horizon_type="pentad")
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "EM",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE

    def test_ensemble_mean_alias_accepted_for_period(self):
        """ENSEMBLE_MEAN is a known alias for EM → resolved → COMPLETE."""
        page = _combined_period_page(n=1, model_type="EM", horizon_type="pentad")
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "ENSEMBLE_MEAN",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE
        assert frame is not None

    def test_unknown_model_rejected_for_period(self):
        """UNKNOWN model → not in _RECOVERY_PERIOD_ML_MODELS → FAILED before API call."""
        client = MagicMock()
        status, frame, _ = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "UNKNOWN",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None
        client._get.assert_not_called()

    def test_lt_rejected_for_period_reader(self):
        """LT is not a valid period model → FAILED before API call."""
        client = MagicMock()
        status, frame, _ = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "LT",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None
        client._get.assert_not_called()


# ---------------------------------------------------------------------------
# Finding 1: Period target must be exactly issue_date+1; horizon ints validated
# ---------------------------------------------------------------------------


class TestRecoveryPeriodTargetValidation:
    """Finding 1: target must equal issue+1; wrong period start rejected.
    Also validates horizon_value and horizon_in_year match calendar computation.
    """

    def test_period_target_exactly_issue_plus_one_accepted(self):
        """Oct5 issue, Oct6 target → target == issue+1 → COMPLETE."""
        page = _combined_period_page(n=1, model_type="TFT", horizon_type="pentad")
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE
        assert frame is not None

    def test_period_target_wrong_start_rejected(self):
        """Oct5 issue, Oct11 target → target != issue+1 → FAILED.

        Oct11 is a valid pentad-boundary day but is NOT issue+1 for Oct5.
        """

        page = _combined_period_page(n=1, model_type="TFT", horizon_type="pentad")
        bad_page = page.copy()
        bad_page["date"] = "2026-10-05"
        bad_page["target"] = "2026-10-11"
        bad_page["horizon_value"] = 3
        bad_page["horizon_in_year"] = 57
        client = _fake_client(pages_by_code={"19999": [bad_page]})
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert reason is not None and "issue_date+1" in reason

    def test_dec31_to_jan1_period_ints_are_one(self):
        """Dec31 issue → Jan1 target; period-in-month=1, period-in-year=1 for pentad.

        Jan1 is day <= 5 → pim=1; (1-1)*6+1=1 → piy=1.
        """

        row = {
            "id": 9001,
            "code": "19999",
            "date": "2025-12-31",
            "target": "2026-01-01",
            "model_type": "TFT",
            "forecasted_discharge": 200.0,
            "q05": 180.0,
            "q25": 190.0,
            "q75": 210.0,
            "q95": 220.0,
            "flag": None,
            "composition": "TFT",
            "horizon_type": "pentad",
            "horizon_value": 1,
            "horizon_in_year": 1,
        }
        page = pd.DataFrame([row])
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            dt.date(2025, 12, 1),
            dt.date(2026, 1, 31),
            dt.date(2026, 1, 31),
        )
        assert status == RECOVERY_STATUS_COMPLETE

    def test_wrong_horizon_value_rejected(self):
        """Oct5 issue, Oct6 target → pim should be 2 (Oct 1-5=p1, 6-10=p2).

        If horizon_value=1 (wrong for Oct6) the validator should reject the row.
        """
        page = _combined_period_page(n=1, model_type="TFT", horizon_type="pentad")
        bad_page = page.copy()
        bad_page["horizon_value"] = 1
        client = _fake_client(pages_by_code={"19999": [bad_page]})
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert reason is not None and "horizon_value" in reason

    def test_day_horizon_value_equals_doy(self):
        """DAY horizon_value and horizon_in_year must equal DOY of target.

        Oct6 2026 → DOY 279; fixture sets correct values → COMPLETE.
        """
        page = _day_page(n=1)
        assert page.iloc[0]["horizon_value"] == 279
        assert page.iloc[0]["horizon_in_year"] == 279
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE

    def test_day_wrong_horizon_value_rejected(self):
        """DAY horizon_value set to 1 (not Oct6 DOY=279) → validator rejects."""
        page = _day_page(n=1)
        bad_page = page.copy()
        bad_page["horizon_value"] = 1
        client = _fake_client(pages_by_code={"19999": [bad_page]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert reason is not None and "horizon_value" in reason


# ---------------------------------------------------------------------------
# Finding 2: Raw date validation; numeric string rejection
# ---------------------------------------------------------------------------


class TestRecoveryRawDateAndNumericValidation:
    """Finding 2: date values must be canonical; numeric strings in quantile
    columns are rejected."""

    def _make_day_page_with_date(self, date_val) -> pd.DataFrame:
        page = _day_page(n=1)
        page = page.copy()
        page["date"] = date_val
        return page

    def _make_day_page_with_target(self, target_val) -> pd.DataFrame:
        page = _day_page(n=1)
        page = page.copy()
        page["target"] = target_val
        return page

    def test_slash_date_rejected(self):
        """'2026/10/05' in date column → FAILED (non-canonical separator)."""
        bad_page = self._make_day_page_with_date("2026/10/05")
        client = _fake_client(pages_by_code={"19999": [bad_page]})
        status, _, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert reason is not None and "non-canonical" in reason

    def test_compact_date_rejected(self):
        """'20261005' (no separator) → FAILED."""
        bad_page = self._make_day_page_with_date("20261005")
        client = _fake_client(pages_by_code={"19999": [bad_page]})
        status, _, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert reason is not None and "non-canonical" in reason

    def test_time_bearing_string_rejected(self):
        """'2026-10-05T10:00:00' → FAILED (time component present)."""
        bad_page = self._make_day_page_with_date("2026-10-05T10:00:00")
        client = _fake_client(pages_by_code={"19999": [bad_page]})
        status, _, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert reason is not None and "non-canonical" in reason

    def test_numeric_date_int_rejected(self):
        """Integer 20261005 as date value → FAILED."""
        bad_page = self._make_day_page_with_date(20261005)
        client = _fake_client(pages_by_code={"19999": [bad_page]})
        status, _, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert reason is not None and "non-canonical" in reason

    def test_bool_in_date_column_rejected(self):
        """True in date column → FAILED (booleans rejected as dates)."""
        bad_page = self._make_day_page_with_date(True)
        client = _fake_client(pages_by_code={"19999": [bad_page]})
        status, _, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert reason is not None and "non-canonical" in reason

    def test_numeric_string_in_quantile_col_rejected(self):
        """'100.0' (numeric string) in q05 → FAILED."""
        page = _day_page(n=1)
        bad_page = page.copy()
        bad_page = bad_page.astype(object)
        bad_page.at[bad_page.index[0], "q05"] = "100.0"
        client = _fake_client(pages_by_code={"19999": [bad_page]})
        status, _, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert reason is not None and "q05" in reason

    def test_bool_in_quantile_col_rejected(self):
        """True in q25 column → FAILED (bool excluded from numeric)."""
        page = _day_page(n=1)
        bad_page = page.copy()
        bad_page = bad_page.astype(object)
        bad_page.at[bad_page.index[0], "q25"] = True
        client = _fake_client(pages_by_code={"19999": [bad_page]})
        status, _, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert reason is not None and "bool" in reason

    def test_valid_isoformat_date_string_accepted(self):
        """'2026-10-05' (exact YYYY-MM-DD) → accepted."""
        page = _day_page(n=1)
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE

    def test_period_numeric_string_in_quantile_col_rejected(self):
        """'300.0' (numeric string) in q05 of period page → FAILED."""
        page = _combined_period_page(n=1, model_type="TFT", horizon_type="pentad")
        bad_page = page.copy()
        bad_page = bad_page.astype(object)
        bad_page.at[bad_page.index[0], "q05"] = "300.0"
        client = _fake_client(pages_by_code={"19999": [bad_page]})
        status, _, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert reason is not None and "q05" in reason


# ---------------------------------------------------------------------------
# Finding 10: None deadline applies application-owned finite default
# ---------------------------------------------------------------------------


class TestRecoveryDeadlineDefault:
    """Finding 10: None total_deadline applies a finite 120-second default;
    enabled reads still terminate; non-finite deadline still fails."""

    def test_none_deadline_empty_response_returns_empty(self):
        """With None deadline, genuine empty API response → CONFIRMED_EMPTY (not deadline error)."""
        client = _fake_client(pages_by_code={"19999": [pd.DataFrame()]})
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            total_deadline=None,
        )
        assert status == RECOVERY_STATUS_EMPTY
        assert frame is not None

    def test_none_deadline_single_page_complete(self):
        """With None deadline, valid single page → COMPLETE (default is finite)."""
        page = _day_page(n=1)
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            total_deadline=None,
        )
        assert status == RECOVERY_STATUS_COMPLETE
        assert frame is not None
        assert len(frame) == 1

    def test_none_deadline_lr_returns_complete(self):
        """LR reader with None deadline → COMPLETE (default applied)."""
        page = _lr_page(n=1)
        client = _fake_client(lr_pages={"19999": [page]})
        status, frame, _ = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "pentad",
            _START,
            _END,
            _FD,
            total_deadline=None,
        )
        assert status == RECOVERY_STATUS_COMPLETE
        assert frame is not None

    def test_none_deadline_combined_period_returns_complete(self):
        """Combined period reader with None deadline → COMPLETE (default applied)."""
        page = _combined_period_page(n=1, horizon_type="pentad")
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
            total_deadline=None,
        )
        assert status == RECOVERY_STATUS_COMPLETE
        assert frame is not None

    def test_none_deadline_expired_clock_fails(self):
        """A clock that starts past epoch 0 with default=_clock()+120 expiry:
        if the default 120s were removed, an already-past deadline would not
        fire here — the test proves the default is applied."""
        page = _day_page(n=1)
        # Clock always returns a large value so deadline = large+120 is in
        # the future → COMPLETE.  Swap to clock()-120 to confirm expiry.
        call_n = {"n": 0}

        def _expired_clock():
            # First call (deadline setup): returns t=1000; subsequent calls
            # return t=1121 which is > 1000+120 → deadline fires.
            call_n["n"] += 1
            return 1000.0 if call_n["n"] == 1 else 1121.0

        client = MagicMock()
        client._get.return_value = _df_to_raw(page)
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            total_deadline=None,
            _clock=_expired_clock,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert "deadline" in reason


# ---------------------------------------------------------------------------
# New findings: malformed 6th element (finding 4), per-row required keys
# (finding 5), horizon inf/string (finding 1/new2), flag/composition types
# ---------------------------------------------------------------------------


class TestRecoveryMalformed6thElement:
    """Finding 4 (new): only the first 5 elements were checked; malformed
    later elements escaped the shape validation."""

    def test_day_malformed_6th_element_initial_page_fails(self):
        """5 valid dicts + None as 6th element on initial page → FAILED, no partial frame."""
        valid_page = _day_page(n=5)
        raw = _df_to_raw(valid_page)
        raw.append(None)  # 6th element is null
        client = MagicMock()
        client._get.return_value = raw
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None
        assert "index 5" in reason or "non-dict" in reason

    def test_combined_period_malformed_6th_element_initial_page_fails(self):
        """Period reader: 5 valid dicts + None as 6th → FAILED."""
        valid_page = _combined_period_page(n=5, horizon_type="pentad")
        raw = _df_to_raw(valid_page)
        raw.append(None)
        client = MagicMock()
        client._get.return_value = raw
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_malformed_6th_element_later_page_fails(self):
        """Page 1 valid (full), page 2 has 5 valid dicts + null 6th → FAILED."""
        page1 = _day_page(n=2)  # n=page_size=2 → triggers page 2
        valid_page2 = _day_page(n=5)
        raw_page2 = _df_to_raw(valid_page2)
        raw_page2.append(None)  # 6th element null

        call_n = {"n": 0}

        def _read(endpoint, params=None):
            call_n["n"] += 1
            return _df_to_raw(page1) if call_n["n"] == 1 else raw_page2

        client = MagicMock()
        client._get.side_effect = _read
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            page_size=2,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None

    def test_lr_malformed_6th_element_fails(self):
        """LR reader: 5 valid dicts + None as 6th element → FAILED."""
        valid_page = _lr_page(n=5)
        raw = _df_to_raw(valid_page)
        raw.append(None)
        client = MagicMock()
        client._get.return_value = raw
        status, frame, reason = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None


class TestRecoveryPerRowRequiredKeys:
    """Finding 5: one row missing a required key (e.g. q05) must fail even if
    another row supplies the column (union-of-columns would silently null it)."""

    def test_day_row_missing_q05_fails(self):
        """2-row page where row 1 has no 'q05' key → FAILED before DataFrame construction."""
        row0 = _df_to_raw(_day_page(n=1))[0]
        row1 = _df_to_raw(_day_page(n=1))[0]
        del row1["q05"]
        client = MagicMock()
        client._get.return_value = [row0, row1]
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None
        assert "q05" in reason

    def test_combined_period_row_missing_composition_fails(self):
        """Period page: row 1 missing 'composition' key → FAILED."""
        row0 = _df_to_raw(_combined_period_page(n=1, horizon_type="pentad"))[0]
        row1 = _df_to_raw(_combined_period_page(n=1, horizon_type="pentad"))[0]
        del row1["composition"]
        client = MagicMock()
        client._get.return_value = [row0, row1]
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None
        assert "composition" in reason

    def test_combined_period_row_missing_flag_fails(self):
        """Period page: row 1 missing 'flag' key → FAILED."""
        row0 = _df_to_raw(_combined_period_page(n=1, horizon_type="pentad"))[0]
        row1 = _df_to_raw(_combined_period_page(n=1, horizon_type="pentad"))[0]
        del row1["flag"]
        client = MagicMock()
        client._get.return_value = [row0, row1]
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None
        assert "flag" in reason

    def test_explicit_null_q05_accepted(self):
        """A row with q05=None (explicit null, key present) is accepted."""
        row = _df_to_raw(_day_page(n=1))[0]
        row["q05"] = None  # explicit null: key present, value null
        client = MagicMock()
        client._get.return_value = [row]
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE


class TestRecoveryHorizonIntStrictTypes:
    """Finding 1/new2: horizon_value/horizon_in_year must be plain int;
    inf raises during astype; numeric strings are silently accepted by
    pd.to_numeric and must be rejected."""

    @pytest.mark.parametrize(
        "bad_hv,label",
        [
            (float("inf"), "inf"),
            (float("-inf"), "-inf"),
            ("279", "numeric_string"),
            (1 << 64, "huge_int_out_of_int64"),
            (2**1000, "huge_int_2_pow_1000"),
        ],
    )
    def test_day_bad_horizon_value_fails(self, bad_hv, label):
        """Inf / huge int (including 2**1000) / numeric string → FAILED, not exception."""
        page = _day_page(n=1)
        bad_page = page.copy().astype(object)
        bad_page.at[bad_page.index[0], "horizon_value"] = bad_hv
        client = _fake_client(pages_by_code={"19999": [bad_page]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED, f"label={label!r}: expected FAILED"
        assert frame is None

    @pytest.mark.parametrize(
        "bad_flag,label",
        [
            (0.5, "fraction"),
            ("active", "string"),
            (1.9, "near_int_float"),
        ],
    )
    def test_flag_invalid_values_fail(self, bad_flag, label):
        """Flag fraction / string → FAILED (flag must be integer or null)."""
        page = _day_page(n=1)
        bad_page = page.copy().astype(object)
        bad_page.at[bad_page.index[0], "flag"] = bad_flag
        client = _fake_client(pages_by_code={"19999": [bad_page]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED, f"label={label!r}: expected FAILED"
        assert frame is None

    def test_flag_mixed_float_none_rejected(self):
        """Page with flag=[1.5, None] → FAILED (1.5 is not a valid int)."""
        page = _day_page(n=2)
        bad_page = page.copy().astype(object)
        bad_page.at[bad_page.index[0], "flag"] = 1.5
        bad_page.at[bad_page.index[1], "flag"] = None
        client = _fake_client(pages_by_code={"19999": [bad_page]})
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None
        assert "flag" in reason

    def test_composition_numeric_fails(self):
        """composition=42 (numeric, not string) → FAILED."""
        page = _combined_period_page(n=1, horizon_type="pentad")
        bad_page = page.copy().astype(object)
        bad_page.at[bad_page.index[0], "composition"] = 42
        client = _fake_client(pages_by_code={"19999": [bad_page]})
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None
        assert "composition" in reason

    def test_composition_null_accepted(self):
        """composition=None (explicit null) → accepted."""
        page = _combined_period_page(n=1, horizon_type="pentad")
        page = page.copy().astype(object)
        page.at[page.index[0], "composition"] = None
        client = _fake_client(pages_by_code={"19999": [page]})
        status, frame, _ = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE


class TestRecoveryPaginationSkipOffsets:
    """Finding 11: pagination must advance skip by page_size; tests must
    assert the actual skip values sent, not just call counts."""

    def test_day_pagination_skip_offsets(self):
        """Two full pages + empty terminates; skip offsets must be [0, 2, 4]."""
        page1 = _day_page(n=2)
        page2 = _day_page(n=2)
        # Alter page2 targets to avoid repeated-page detection
        page2 = page2.copy()
        page2["date"] = "2026-10-07"
        page2["target"] = ["2026-10-08", "2026-10-09"]
        page2["horizon_value"] = [281, 282]
        page2["horizon_in_year"] = [281, 282]
        page2["id"] = [2000, 2001]

        call_n = {"n": 0}
        seen_skips = []

        def _read(endpoint, params=None):
            params = params or {}
            seen_skips.append(params.get("skip"))
            call_n["n"] += 1
            if call_n["n"] == 1:
                return _df_to_raw(page1)
            if call_n["n"] == 2:
                return _df_to_raw(page2)
            return []

        client = MagicMock()
        client._get.side_effect = _read
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
            page_size=2,
        )
        assert status == RECOVERY_STATUS_COMPLETE
        assert seen_skips == [0, 2, 4]
        assert len(frame) == 4

    def test_lr_pagination_skip_offsets(self):
        """LR reader: two full pages + empty; skip must be [0, 2, 4]."""
        page1 = _lr_page(n=2)
        page2 = _lr_page(n=2)
        page2 = page2.copy()
        page2["date"] = ["2026-10-07", "2026-10-08"]
        page2["horizon_value"] = [3, 4]
        page2["horizon_in_year"] = [3, 4]

        call_n = {"n": 0}
        seen_skips = []

        def _read(endpoint, params=None):
            params = params or {}
            seen_skips.append(params.get("skip"))
            call_n["n"] += 1
            if call_n["n"] == 1:
                return _df_to_raw(page1)
            if call_n["n"] == 2:
                return _df_to_raw(page2)
            return []

        client = MagicMock()
        client._get.side_effect = _read
        status, frame, _ = read_lr_forecasts_for_recovery(
            client,
            ["19999"],
            "pentad",
            _START,
            _END,
            _FD,
            page_size=2,
        )
        assert status == RECOVERY_STATUS_COMPLETE
        assert seen_skips == [0, 2, 4]


class TestRecoveryNormalizerIntegrationExact:
    """Finding 11: normalization integration must assert exact aggregate values,
    not just DataFrame type."""

    def test_day_recovery_normalization_exact_aggregate(self):
        """read_day_forecasts_for_recovery + _normalize_ml_forecasts: exact mean."""
        # 5 valid targets for Oct 6 pentad (Oct 6-10), issue=Oct 5
        rows = []
        for i in range(5):
            target = dt.date(2026, 10, 6 + i)
            doy = target.timetuple().tm_yday
            rows.append(
                {
                    "code": "19999",
                    "date": "2026-10-05",
                    "target": target.isoformat(),
                    "model_type": "TFT",
                    "forecasted_discharge": float(10 * (i + 1)),  # 10,20,30,40,50
                    "q05": float(i + 1),  # 1,2,3,4,5
                    "q25": float(2 * (i + 1)),
                    "q75": float(8 * (i + 1)),
                    "q95": float(9 * (i + 1)),
                    "flag": None,
                    "horizon_type": "day",
                    "horizon_value": doy,
                    "horizon_in_year": doy,
                }
            )
        client = MagicMock()
        client._get.return_value = rows
        status, frame, _ = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE
        result = _normalize_ml_forecasts(frame, "TFT", "pentad")
        assert len(result) == 1
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(30.0)  # mean(10..50)
        assert result["q05"].iloc[0] == pytest.approx(3.0)  # mean(1..5)


class TestRecoveryFakeHTTPBoundary:
    """Finding 4: the real installed SapphirePostprocessingClient._get must
    preserve [] vs null/false/{} distinctions at the HTTP boundary.
    Uses a fake requests.Session to avoid any real network call."""

    @pytest.mark.skipif(not SAPPHIRE_API_AVAILABLE, reason="sapphire-api-client not installed")
    @pytest.mark.parametrize(
        "json_body,expected_status",
        [
            ([], RECOVERY_STATUS_EMPTY),  # genuine empty list
            (None, RECOVERY_STATUS_FAILED),  # null body → _get returns None
            (False, RECOVERY_STATUS_FAILED),  # false body
            ({}, RECOVERY_STATUS_FAILED),  # object body
        ],
    )
    def test_http_body_distinction(self, json_body, expected_status):
        """Real installed client with fake HTTP session: body=<json_body>
        must reach the reader as-is (not converted to empty DataFrame)."""
        import unittest.mock as _mock

        from sapphire_api_client.postprocessing import SapphirePostprocessingClient

        fake_response = _mock.MagicMock()
        fake_response.status_code = 200
        fake_response.json.return_value = json_body
        fake_session = _mock.MagicMock()
        fake_session.request.return_value = fake_response
        fake_session.max_redirects = 5
        fake_session.headers = {}

        client = SapphirePostprocessingClient(base_url="http://localhost:8000")
        client.session = fake_session

        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == expected_status, (
            f"json_body={json_body!r}: expected {expected_status!r}, got {status!r} ({reason})"
        )


class TestRecoveryFlagPrecision:
    """New finding: nullable flag ints > 2**53 must not be rounded by
    pd.DataFrame inference (float64 rounds 2**53+1 → 2**53).
    Contract: exact Python int preserved; None preserved; adjacent-int
    conflict detected in both orders."""

    _BIG = 2**53 + 1  # first int that float64 cannot represent exactly

    def _raw_day_with_flags(self, flags):
        """Two DAY rows from _day_page, flag column replaced."""
        page = _day_page(n=len(flags))
        raw = _df_to_raw(page)
        for rec, f in zip(raw, flags, strict=False):
            rec["flag"] = f
        return raw

    def _raw_period_with_flags(self, flags):
        """Combined period rows with flag column replaced."""
        page = _combined_period_page(n=len(flags), horizon_type="pentad")
        raw = _df_to_raw(page)
        for rec, f in zip(raw, flags, strict=False):
            rec["flag"] = f
        return raw

    def test_day_large_int_and_none_preserved(self):
        """DAY: flag=[2**53+1, None] → first row exact, second row None."""
        raw = self._raw_day_with_flags([self._BIG, None])
        client = MagicMock()
        client._get.return_value = raw
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE, reason
        flags = list(frame["flag"])
        assert flags[0] == self._BIG, f"expected {self._BIG}, got {flags[0]}"
        assert flags[1] is None

    def test_combined_large_int_and_none_preserved(self):
        """Combined: flag=[2**53+1, None] → first row exact, second row None."""
        raw = self._raw_period_with_flags([self._BIG, None])
        client = MagicMock()
        client._get.return_value = raw
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE, reason
        flags = list(frame["flag"])
        assert flags[0] == self._BIG, f"expected {self._BIG}, got {flags[0]}"
        assert flags[1] is None

    @pytest.mark.parametrize(
        "flag_a,flag_b",
        [
            (2**53, 2**53 + 1),
            (2**53 + 1, 2**53),
        ],
    )
    def test_day_adjacent_int_conflict_fails(self, flag_a, flag_b):
        """DAY: two rows same identity key but flags 2**53 vs 2**53+1 → FAILED."""
        page = _day_page(n=1)
        raw0 = _df_to_raw(page)
        raw1 = _df_to_raw(page)
        raw0[0]["flag"] = flag_a
        raw1[0]["flag"] = flag_b
        # Single page with two rows sharing same identity key
        client = MagicMock()
        client._get.return_value = raw0 + raw1
        status, frame, reason = read_day_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED, (
            f"flag_a={flag_a}, flag_b={flag_b}: expected FAILED, got {status!r} ({reason})"
        )

    @pytest.mark.parametrize(
        "flag_a,flag_b",
        [
            (2**53, 2**53 + 1),
            (2**53 + 1, 2**53),
        ],
    )
    def test_combined_adjacent_int_conflict_fails(self, flag_a, flag_b):
        """Combined: two rows same identity key but flags 2**53 vs 2**53+1 → FAILED."""
        page = _combined_period_page(n=1, horizon_type="pentad")
        raw0 = _df_to_raw(page)
        raw1 = _df_to_raw(page)
        raw0[0]["flag"] = flag_a
        raw1[0]["flag"] = flag_b
        client = MagicMock()
        client._get.return_value = raw0 + raw1
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _END,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED, (
            f"flag_a={flag_a}, flag_b={flag_b}: expected FAILED, got {status!r} ({reason})"
        )


# ---------------------------------------------------------------------------
# R1 regression: JSON null quantities survive reader → to_dict → planner
# ---------------------------------------------------------------------------

_KEY_OCT05 = ("pentad", "19999", "TFT", dt.date(2026, 10, 5), dt.date(2026, 10, 6))
_KEY_OCT10 = ("pentad", "19999", "TFT", dt.date(2026, 10, 10), dt.date(2026, 10, 11))
_ALL5_COLS = ("forecasted_discharge", "q05", "q25", "q75", "q95")


def _period_raw_with_nulls(null_rows: int, complete_rows: int) -> list:
    """Return a raw records list: first null_rows have all-null ALL5, rest complete."""
    base = _combined_period_page(n=null_rows + complete_rows, horizon_type="pentad")
    raw = _df_to_raw(base)
    for rec in raw[:null_rows]:
        for col in _ALL5_COLS:
            rec[col] = None
    return raw


class TestNullableQuantitiesToPlanner:
    """R1: JSON null in ALL5 quantity fields must survive
    read_combined_period_forecasts_for_recovery → to_dict → plan_period_refresh.

    Before the fix, pd.DataFrame(raw) converts JSON null to float NaN;
    NaN then fails _plan_existing_all5_valid (which accepts None but not
    non-finite floats), causing plan_error for any scope that contains a
    pre-existing row with null quantiles.
    """

    @pytest.mark.parametrize("null_col", list(_ALL5_COLS))
    def test_single_null_all5_field_no_plan_error(self, null_col):
        """Each individual ALL5 field null in an existing row → no plan_error."""
        base = _combined_period_page(n=2, horizon_type="pentad")
        raw = _df_to_raw(base)
        raw[0][null_col] = None

        client = MagicMock()
        client._get.return_value = raw
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _FD,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE, reason

        existing_payloads = frame.to_dict("records")
        assert existing_payloads[0][null_col] is None, (
            f"expected None after fix, got {existing_payloads[0][null_col]!r}"
        )
        plan = plan_period_refresh(
            existing_payloads=existing_payloads,
            candidate_payloads=[],
            authorized_keys=set(),
            explicit_refresh_keys=set(),
            start_date=_START,
            end_date=_FD,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is None, plan.plan_error

    def test_all_null_quantities_existing_row_no_plan_error(self):
        """One complete and one all-null-quantity existing row → no plan_error."""
        raw = _period_raw_with_nulls(null_rows=1, complete_rows=1)

        client = MagicMock()
        client._get.return_value = raw
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _FD,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE, reason

        existing_payloads = frame.to_dict("records")
        null_row = existing_payloads[0]
        for col in _ALL5_COLS:
            assert null_row[col] is None, f"expected None for {col!r}, got {null_row[col]!r}"
        plan = plan_period_refresh(
            existing_payloads=existing_payloads,
            candidate_payloads=[],
            authorized_keys=set(),
            explicit_refresh_keys=set(),
            start_date=_START,
            end_date=_FD,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is None, plan.plan_error

    def test_cross_page_null_then_complete_no_plan_error(self):
        """Page 1 has all-null quantities (fills page); page 2 has a complete row.
        None survives cross-page concat, drop_duplicates, and to_dict."""
        raw1 = _period_raw_with_nulls(null_rows=2, complete_rows=0)
        base3 = _combined_period_page(n=3, horizon_type="pentad")
        raw2 = _df_to_raw(base3.iloc[[2]].copy())  # Oct-15 issue row only

        call_count = {"n": 0}

        def _read(endpoint, params=None):
            call_count["n"] += 1
            return raw1 if call_count["n"] == 1 else raw2

        client = MagicMock()
        client._get.side_effect = _read
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _FD,
            _FD,
            page_size=2,
        )
        assert status == RECOVERY_STATUS_COMPLETE, reason
        assert len(frame) == 3

        existing_payloads = frame.to_dict("records")
        null_rows = [r for r in existing_payloads if r["forecasted_discharge"] is None]
        assert len(null_rows) == 2
        plan = plan_period_refresh(
            existing_payloads=existing_payloads,
            candidate_payloads=[],
            authorized_keys=set(),
            explicit_refresh_keys=set(),
            start_date=_START,
            end_date=_FD,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is None, plan.plan_error

    def test_existing_incomplete_protected_without_explicit_refresh(self):
        """Existing row has null quantities; candidate has complete quantities.
        Without explicit_refresh_keys the key lands in plan.protected, not plan_error."""
        raw = _period_raw_with_nulls(null_rows=1, complete_rows=0)
        client = MagicMock()
        client._get.return_value = raw
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _FD,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE, reason

        existing_payloads = frame.to_dict("records")
        # Candidate for the same key with complete quantities.
        candidate = dict(existing_payloads[0])
        for col in _ALL5_COLS:
            candidate[col] = 300.0  # complete, differs from None

        plan = plan_period_refresh(
            existing_payloads=existing_payloads,
            candidate_payloads=[candidate],
            authorized_keys={_KEY_OCT05},
            explicit_refresh_keys=set(),
            start_date=_START,
            end_date=_FD,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is None, plan.plan_error
        assert _KEY_OCT05 in plan.protected

    def test_absent_authorized_candidate_is_missing(self):
        """Authorized candidate key absent from existing DB rows → plan.missing."""
        raw = _period_raw_with_nulls(null_rows=1, complete_rows=0)
        client = MagicMock()
        client._get.return_value = raw
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _FD,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE, reason

        existing_payloads = frame.to_dict("records")  # only _KEY_OCT05
        # Candidate for _KEY_OCT10 (Oct 10 issue) — not in existing.
        base2 = _combined_period_page(n=2, horizon_type="pentad")
        raw2 = _df_to_raw(base2)
        cand = dict(raw2[1])  # row 1 = Oct 10 issue
        # Ensure complete ALL5.
        for col in _ALL5_COLS:
            cand[col] = 400.0

        plan = plan_period_refresh(
            existing_payloads=existing_payloads,
            candidate_payloads=[cand],
            authorized_keys={_KEY_OCT05, _KEY_OCT10},
            explicit_refresh_keys=set(),
            start_date=_START,
            end_date=_FD,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is None, plan.plan_error
        assert _KEY_OCT10 in plan.missing

    def test_healthy_key_is_identical(self):
        """Complete existing row and identical candidate → plan.identical; no plan_error."""
        base = _combined_period_page(n=1, horizon_type="pentad")
        raw = _df_to_raw(base)
        client = MagicMock()
        client._get.return_value = raw
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _FD,
            _FD,
        )
        assert status == RECOVERY_STATUS_COMPLETE, reason

        existing_payloads = frame.to_dict("records")
        # Use the same record as the candidate so types match exactly.
        candidate = dict(existing_payloads[0])

        plan = plan_period_refresh(
            existing_payloads=existing_payloads,
            candidate_payloads=[candidate],
            authorized_keys={_KEY_OCT05},
            explicit_refresh_keys=set(),
            start_date=_START,
            end_date=_FD,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is None, plan.plan_error
        assert _KEY_OCT05 in plan.identical

    def test_infinity_in_period_quantity_fails_read(self):
        """Infinity in a combined period quantity field → FAILED; not silently None."""
        base = _combined_period_page(n=1, horizon_type="pentad")
        raw = _df_to_raw(base)
        raw[0]["forecasted_discharge"] = float("inf")
        client = MagicMock()
        client._get.return_value = raw
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _FD,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None
        assert "infinite" in reason

    def test_period_row_missing_quantity_key_fails_read(self):
        """Raw record missing 'forecasted_discharge' entirely → FAILED (required key)."""
        row0 = _df_to_raw(_combined_period_page(n=1, horizon_type="pentad"))[0]
        row1 = _df_to_raw(_combined_period_page(n=1, horizon_type="pentad"))[0]
        del row1["forecasted_discharge"]
        client = MagicMock()
        client._get.return_value = [row0, row1]
        status, frame, reason = read_combined_period_forecasts_for_recovery(
            client,
            ["19999"],
            "TFT",
            "pentad",
            _START,
            _FD,
            _FD,
        )
        assert status == RECOVERY_STATUS_FAILED
        assert frame is None
        assert "forecasted_discharge" in reason
