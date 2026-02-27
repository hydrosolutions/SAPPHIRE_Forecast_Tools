"""
Tests for the 3 extracted functions in extend_era5_reanalysis.py:
- select_stable_operational_data
- extend_reanalysis_with_operational
- calculate_daily_norm
"""

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

from extend_era5_reanalysis import (
    calculate_daily_norm,
    extend_reanalysis_with_operational,
    select_stable_operational_data,
)

# =====================================================================
# select_stable_operational_data
# =====================================================================


class TestSelectStableOperationalData:
    """Tests filtering operational data to the stable window."""

    def test_default_195_day_threshold(self):
        """max_date=2024-07-01, threshold=2024-07-01 - 195d =
        2023-12-19. Dates before threshold kept (strict <)."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2023-01-01", "2023-12-18", "2023-12-19", "2024-07-01"]),
                "code": ["A"] * 4,
                "P": [1.0, 2.0, 3.0, 4.0],
            }
        )
        result = select_stable_operational_data(df)
        assert len(result) == 2
        expected_dates = {pd.Timestamp("2023-01-01"), pd.Timestamp("2023-12-18")}
        assert set(result["date"]) == expected_dates

    def test_custom_stability_days(self):
        """stability_days=30, max_date=2024-07-01 → threshold =
        2024-06-01. Only 2024-05-31 kept."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-05-31", "2024-06-01", "2024-07-01"]),
                "code": ["A"] * 3,
                "P": [1.0, 2.0, 3.0],
            }
        )
        result = select_stable_operational_data(df, stability_days=30)
        assert len(result) == 1
        assert result["date"].iloc[0] == pd.Timestamp("2024-05-31")

    def test_all_data_within_window_returns_empty(self):
        """All dates within unstable window → empty result, columns
        preserved. stability_days=5, max_date=2024-07-05, threshold=
        2024-06-30. Both dates >= threshold."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-06-30", "2024-07-05"]),
                "code": ["A", "A"],
                "P": [1.0, 2.0],
            }
        )
        result = select_stable_operational_data(df, stability_days=5)
        assert len(result) == 0
        assert list(result.columns) == ["date", "code", "P"]

    def test_preserves_all_columns_and_values(self):
        """Result has same columns with correct values for kept
        rows."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2023-01-01", "2024-07-01"]),
                "code": ["A", "A"],
                "P": [5.5, 99.0],
                "T": [12.3, 30.0],
            }
        )
        result = select_stable_operational_data(df)
        assert len(result) == 1
        row = result.iloc[0]
        assert row["P"] == 5.5
        assert row["T"] == 12.3
        assert row["code"] == "A"


# =====================================================================
# extend_reanalysis_with_operational
# =====================================================================


class TestExtendReanalysisWithOperational:
    """Tests for append + dedup + sort logic."""

    def test_appends_non_overlapping(self):
        """Reanalysis [Jan 1-3] + Operational [Jan 4-5] → 5 rows."""
        reanalysis = pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03"]),
                "code": ["A"] * 3,
                "P": [1.0, 2.0, 3.0],
            }
        )
        operational = pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-01-04", "2020-01-05"]),
                "code": ["A"] * 2,
                "P": [4.0, 5.0],
            }
        )
        result = extend_reanalysis_with_operational(reanalysis, operational)
        assert len(result) == 5
        assert list(result["P"]) == [1.0, 2.0, 3.0, 4.0, 5.0]

    def test_dedup_keeps_operational_on_overlap(self):
        """Overlap on 2020-01-01 code 'A': operational wins
        (keep='last')."""
        reanalysis = pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-01-01"]),
                "code": ["A"],
                "P": [1.0],
            }
        )
        operational = pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-01-01"]),
                "code": ["A"],
                "P": [99.0],
            }
        )
        result = extend_reanalysis_with_operational(reanalysis, operational)
        assert len(result) == 1
        assert result["P"].iloc[0] == 99.0

    def test_sorted_by_date_code(self):
        """Output is sorted by (date, code)."""
        reanalysis = pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-01-03", "2020-01-01"]),
                "code": ["B", "A"],
                "P": [3.0, 1.0],
            }
        )
        operational = pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-01-02"]),
                "code": ["A"],
                "P": [2.0],
            }
        )
        result = extend_reanalysis_with_operational(reanalysis, operational)
        expected = result.sort_values(["date", "code"]).reset_index(drop=True)
        pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)

    def test_multiple_codes_dedup_independent(self):
        """Code 'A' overlap → operational wins. Code 'B' no overlap
        → preserved."""
        reanalysis = pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-01-01", "2020-01-01"]),
                "code": ["A", "B"],
                "P": [1.0, 50.0],
            }
        )
        operational = pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-01-01"]),
                "code": ["A"],
                "P": [10.0],
            }
        )
        result = extend_reanalysis_with_operational(reanalysis, operational)
        assert len(result) == 2
        a_row = result[result["code"] == "A"]
        b_row = result[result["code"] == "B"]
        assert a_row["P"].iloc[0] == 10.0
        assert b_row["P"].iloc[0] == 50.0

    def test_empty_operational_returns_reanalysis(self):
        """Empty operational → result is identical to reanalysis."""
        reanalysis = pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-01-01", "2020-01-02"]),
                "code": ["A", "A"],
                "P": [1.0, 2.0],
            }
        )
        operational = pd.DataFrame(columns=["date", "code", "P"]).astype(
            {"date": "datetime64[ns]", "P": "float64"}
        )
        result = extend_reanalysis_with_operational(reanalysis, operational)
        pd.testing.assert_frame_equal(
            result.reset_index(drop=True),
            reanalysis.reset_index(drop=True),
        )


# =====================================================================
# calculate_daily_norm
# =====================================================================


class TestCalculateDailyNorm:
    """Tests for daily norm calculation with leap-year handling."""

    @pytest.fixture
    def three_year_reanalysis(self):
        """3 years (2021-2023) of daily data for code 'A'.
        P values = dayofyear + year_offset for easy manual
        verification.
        """
        frames = []
        for year in [2021, 2022, 2023]:
            dates = pd.date_range(f"{year}-01-01", f"{year}-12-31", freq="D")
            offset = (year - 2021) * 10  # 0, 10, 20
            df = pd.DataFrame(
                {
                    "date": dates,
                    "code": "A",
                    "P": [float(d.dayofyear + offset) for d in dates],
                }
            )
            frames.append(df)
        return pd.concat(frames, ignore_index=True)

    @pytest.fixture
    def leap_year_reanalysis(self):
        """4 years (2020-2023) including 2020 (leap year) for
        code 'A'. Creates day-366 entries only from 2020."""
        frames = []
        for year in [2020, 2021, 2022, 2023]:
            dates = pd.date_range(f"{year}-01-01", f"{year}-12-31", freq="D")
            df = pd.DataFrame(
                {
                    "date": dates,
                    "code": "A",
                    "P": [float(d.dayofyear) for d in dates],
                }
            )
            frames.append(df)
        return pd.concat(frames, ignore_index=True)

    @pytest.fixture
    def operational_current_year(self):
        """Operational data for Jan-Jun 2023, code 'A'."""
        dates = pd.date_range("2023-01-01", "2023-06-30", freq="D")
        return pd.DataFrame(
            {
                "date": dates,
                "code": "A",
                "P": [99.0] * len(dates),
            }
        )

    def test_norm_is_mean_across_years(self, three_year_reanalysis):
        """Dayofyear=1: values [1, 11, 21] → norm=11.0.
        Dayofyear=2: values [2, 12, 22] → norm=12.0."""
        operational = pd.DataFrame(columns=["date", "code", "P"]).astype(
            {"date": "datetime64[ns]", "P": "float64"}
        )
        result = calculate_daily_norm(three_year_reanalysis, operational, "P", 2023)
        day1 = result[result["date"] == pd.Timestamp("2023-01-01")]
        assert day1["P_norm"].iloc[0] == 11.0
        day2 = result[result["date"] == pd.Timestamp("2023-01-02")]
        assert day2["P_norm"].iloc[0] == 12.0

    def test_leap_year_produces_366_rows_per_code(self, leap_year_reanalysis):
        """current_year=2024 (leap), 1 code → 366 rows."""
        operational = pd.DataFrame(columns=["date", "code", "P"]).astype(
            {"date": "datetime64[ns]", "P": "float64"}
        )
        result = calculate_daily_norm(leap_year_reanalysis, operational, "P", 2024)
        assert len(result) == 366
        # Check date range
        assert result["date"].min() == pd.Timestamp("2024-01-01")
        assert result["date"].max() == pd.Timestamp("2024-12-31")

    def test_non_leap_year_produces_365_rows_per_code(self, leap_year_reanalysis):
        """current_year=2023 (non-leap), 1 code → 365 rows.
        No Feb 29."""
        operational = pd.DataFrame(columns=["date", "code", "P"]).astype(
            {"date": "datetime64[ns]", "P": "float64"}
        )
        result = calculate_daily_norm(leap_year_reanalysis, operational, "P", 2023)
        assert len(result) == 365
        # No Feb 29 — check that no date in March follows Feb 28
        # directly (i.e., there's no gap date)
        feb28 = result[result["date"] == pd.Timestamp("2023-02-28")]
        assert len(feb28) == 1
        mar1 = result[result["date"] == pd.Timestamp("2023-03-01")]
        assert len(mar1) == 1

    def test_non_leap_year_day365_present_day366_absent(self):
        """Bug fix validation: with 2 codes and leap year in
        historical data, non-leap current_year → day 365 present
        for BOTH codes, day 366 absent for BOTH.
        The old iloc[:-1] bug would leave day 366 for code A."""
        frames = []
        for year in [2020, 2021]:  # 2020 is leap
            for code in ["A", "B"]:
                dates = pd.date_range(f"{year}-01-01", f"{year}-12-31", freq="D")
                df = pd.DataFrame(
                    {
                        "date": dates,
                        "code": code,
                        "P": [float(d.dayofyear) for d in dates],
                    }
                )
                frames.append(df)
        reanalysis = pd.concat(frames, ignore_index=True)

        operational = pd.DataFrame(columns=["date", "code", "P"]).astype(
            {"date": "datetime64[ns]", "P": "float64"}
        )
        result = calculate_daily_norm(reanalysis, operational, "P", 2023)

        # 365 rows per code = 730 total
        assert len(result) == 730

        for code in ["A", "B"]:
            code_rows = result[result["code"] == code]
            assert len(code_rows) == 365
            # Dec 31 (day 365) present
            dec31 = code_rows[code_rows["date"] == pd.Timestamp("2023-12-31")]
            assert len(dec31) == 1
            # No Jan 1 next year (which day 366 would produce)
            jan1_next = code_rows[code_rows["date"] == pd.Timestamp("2024-01-01")]
            assert len(jan1_next) == 0

    def test_non_leap_year_no_historical_leaps(self):
        """Historical data 2019-2022 (no leaps for 2019, 2021, 2022;
        2020 is leap but we skip it), current_year=2023 → 365 rows,
        day 365 (Dec 31) IS present.
        The old iloc[:-1] bug would drop Dec 31."""
        frames = []
        for year in [2019, 2021, 2022]:  # No leap years
            dates = pd.date_range(f"{year}-01-01", f"{year}-12-31", freq="D")
            df = pd.DataFrame(
                {
                    "date": dates,
                    "code": "A",
                    "P": [float(d.dayofyear) for d in dates],
                }
            )
            frames.append(df)
        reanalysis = pd.concat(frames, ignore_index=True)

        operational = pd.DataFrame(columns=["date", "code", "P"]).astype(
            {"date": "datetime64[ns]", "P": "float64"}
        )
        result = calculate_daily_norm(reanalysis, operational, "P", 2023)

        assert len(result) == 365
        # Dec 31 present
        dec31 = result[result["date"] == pd.Timestamp("2023-12-31")]
        assert len(dec31) == 1

    def test_operational_values_merged_for_matching_dates(
        self, leap_year_reanalysis, operational_current_year
    ):
        """Operational has P=99.0 on 2023-03-15 → merged row has
        P=99.0 alongside computed P_norm."""
        result = calculate_daily_norm(
            leap_year_reanalysis,
            operational_current_year,
            "P",
            2023,
        )
        mar15 = result[result["date"] == pd.Timestamp("2023-03-15")]
        assert len(mar15) == 1
        assert mar15["P"].iloc[0] == 99.0
        assert pd.notna(mar15["P_norm"].iloc[0])

    def test_no_operational_match_produces_nan(
        self, leap_year_reanalysis, operational_current_year
    ):
        """Operational ends 2023-06-30 → rows for Jul onward have
        P=NaN, P_norm still valid."""
        result = calculate_daily_norm(
            leap_year_reanalysis,
            operational_current_year,
            "P",
            2023,
        )
        jul1 = result[result["date"] == pd.Timestamp("2023-07-01")]
        assert len(jul1) == 1
        assert pd.isna(jul1["P"].iloc[0])
        assert pd.notna(jul1["P_norm"].iloc[0])

    def test_norm_rounded_to_2_decimals(self):
        """dayofyear=1 values [1.111, 2.222, 3.333] → norm =
        round(2.222, 2) = 2.22."""
        frames = []
        for year, val in [(2021, 1.111), (2022, 2.222), (2023, 3.333)]:
            df = pd.DataFrame(
                {
                    "date": [pd.Timestamp(f"{year}-01-01")],
                    "code": ["A"],
                    "P": [val],
                }
            )
            frames.append(df)
        reanalysis = pd.concat(frames, ignore_index=True)

        operational = pd.DataFrame(columns=["date", "code", "P"]).astype(
            {"date": "datetime64[ns]", "P": "float64"}
        )
        result = calculate_daily_norm(reanalysis, operational, "P", 2023)
        day1 = result[result["date"] == pd.Timestamp("2023-01-01")]
        assert day1["P_norm"].iloc[0] == 2.22

    def test_multiple_codes_independent_norms(self):
        """Code 'A' dayofyear=1: [10, 20] → norm=15.0.
        Code 'B' dayofyear=1: [100, 200] → norm=150.0."""
        frames = []
        for year, a_val, b_val in [(2021, 10.0, 100.0), (2022, 20.0, 200.0)]:
            for code, val in [("A", a_val), ("B", b_val)]:
                df = pd.DataFrame(
                    {
                        "date": [pd.Timestamp(f"{year}-01-01")],
                        "code": [code],
                        "P": [val],
                    }
                )
                frames.append(df)
        reanalysis = pd.concat(frames, ignore_index=True)

        operational = pd.DataFrame(columns=["date", "code", "P"]).astype(
            {"date": "datetime64[ns]", "P": "float64"}
        )
        result = calculate_daily_norm(reanalysis, operational, "P", 2023)

        a_row = result[(result["code"] == "A") & (result["date"] == pd.Timestamp("2023-01-01"))]
        b_row = result[(result["code"] == "B") & (result["date"] == pd.Timestamp("2023-01-01"))]
        assert a_row["P_norm"].iloc[0] == 15.0
        assert b_row["P_norm"].iloc[0] == 150.0
