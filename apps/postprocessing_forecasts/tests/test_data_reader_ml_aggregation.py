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

from src.data_reader import _is_decad_boundary, _is_pentad_boundary, _normalize_ml_forecasts


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
