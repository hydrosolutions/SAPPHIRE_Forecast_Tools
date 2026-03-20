"""
Unit tests for pure forecast-day functions in linear_regression.py.

Tests get_forecast_days_for_month and get_next_forecast_day — pure functions
that compute forecast days from dates and prediction modes, with no I/O.
"""

import datetime as dt
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from linear_regression import get_forecast_days_for_month, get_next_forecast_day

# ============================================================================
# get_forecast_days_for_month
# ============================================================================


class TestGetForecastDaysForMonth:
    """Tests for get_forecast_days_for_month(year, month, prediction_mode)."""

    # --- PENTAD mode ---

    def test_pentad_31_day_month(self):
        """January (31 days) returns 5, 10, 15, 20, 25, 31."""
        result = get_forecast_days_for_month(2024, 1, "PENTAD")
        assert result == [5, 10, 15, 20, 25, 31]

    def test_pentad_30_day_month(self):
        """April (30 days) returns 5, 10, 15, 20, 25, 30."""
        result = get_forecast_days_for_month(2024, 4, "PENTAD")
        assert result == [5, 10, 15, 20, 25, 30]

    def test_pentad_feb_non_leap(self):
        """Feb non-leap (28 days) returns 5, 10, 15, 20, 25, 28."""
        result = get_forecast_days_for_month(2023, 2, "PENTAD")
        assert result == [5, 10, 15, 20, 25, 28]

    def test_pentad_feb_leap(self):
        """Feb leap (29 days) returns 5, 10, 15, 20, 25, 29."""
        result = get_forecast_days_for_month(2024, 2, "PENTAD")
        assert result == [5, 10, 15, 20, 25, 29]

    # --- DECAD mode ---

    def test_decad_31_day_month(self):
        """March (31 days) DECAD returns 10, 20, 31."""
        result = get_forecast_days_for_month(2024, 3, "DECAD")
        assert result == [10, 20, 31]

    def test_decad_30_day_month(self):
        """June (30 days) DECAD returns 10, 20, 30."""
        result = get_forecast_days_for_month(2024, 6, "DECAD")
        assert result == [10, 20, 30]

    def test_decad_feb_leap(self):
        """Feb leap DECAD returns 10, 20, 29."""
        result = get_forecast_days_for_month(2024, 2, "DECAD")
        assert result == [10, 20, 29]

    def test_decad_feb_non_leap(self):
        """Feb non-leap DECAD returns 10, 20, 28."""
        result = get_forecast_days_for_month(2023, 2, "DECAD")
        assert result == [10, 20, 28]

    # --- BOTH mode ---

    def test_both_equals_pentad(self):
        """BOTH mode returns same result as PENTAD."""
        for month in range(1, 13):
            both = get_forecast_days_for_month(2024, month, "BOTH")
            pentad = get_forecast_days_for_month(2024, month, "PENTAD")
            assert both == pentad, f"Mismatch for month {month}"

    def test_default_mode_is_both(self):
        """Omitting prediction_mode defaults to BOTH (same as PENTAD)."""
        default = get_forecast_days_for_month(2024, 7)
        explicit = get_forecast_days_for_month(2024, 7, "BOTH")
        assert default == explicit

    # --- Structural properties ---

    def test_pentad_always_six_days(self):
        """PENTAD always returns exactly 6 forecast days per month."""
        for month in range(1, 13):
            result = get_forecast_days_for_month(2024, month, "PENTAD")
            assert len(result) == 6, f"Month {month}: expected 6, got {len(result)}"

    def test_decad_always_three_days(self):
        """DECAD always returns exactly 3 forecast days per month."""
        for month in range(1, 13):
            result = get_forecast_days_for_month(2024, month, "DECAD")
            assert len(result) == 3, f"Month {month}: expected 3, got {len(result)}"

    def test_last_day_always_in_list(self):
        """The last day of the month is always a forecast day."""
        import calendar

        for month in range(1, 13):
            last_day = calendar.monthrange(2024, month)[1]
            for mode in ("PENTAD", "DECAD", "BOTH"):
                result = get_forecast_days_for_month(2024, month, mode)
                assert result[-1] == last_day, (
                    f"Month {month} {mode}: last day {last_day} not at end"
                )

    def test_output_is_sorted(self):
        """Output is always sorted ascending."""
        for month in range(1, 13):
            for mode in ("PENTAD", "DECAD", "BOTH"):
                result = get_forecast_days_for_month(2024, month, mode)
                assert result == sorted(result), f"Month {month} {mode}: not sorted"


# ============================================================================
# get_next_forecast_day
# ============================================================================


class TestGetNextForecastDay:
    """Tests for get_next_forecast_day(current_date, prediction_mode)."""

    # --- On a forecast day → returns same day ---

    def test_on_forecast_day_returns_same(self):
        """Jan 5 is a PENTAD day → returns Jan 5."""
        result = get_next_forecast_day(dt.date(2024, 1, 5), "PENTAD")
        assert result == dt.date(2024, 1, 5)

    def test_decad_on_forecast_day_returns_same(self):
        """Jan 10 is a DECAD day → returns Jan 10."""
        result = get_next_forecast_day(dt.date(2024, 1, 10), "DECAD")
        assert result == dt.date(2024, 1, 10)

    # --- Between forecast days → advances ---

    def test_between_days_advances(self):
        """Jan 6 (between 5 and 10) → Jan 10 in PENTAD mode."""
        result = get_next_forecast_day(dt.date(2024, 1, 6), "PENTAD")
        assert result == dt.date(2024, 1, 10)

    def test_day_1_pentad(self):
        """Day 1 → Day 5 in PENTAD mode."""
        result = get_next_forecast_day(dt.date(2024, 1, 1), "PENTAD")
        assert result == dt.date(2024, 1, 5)

    def test_day_1_decad(self):
        """Day 1 → Day 10 in DECAD mode."""
        result = get_next_forecast_day(dt.date(2024, 1, 1), "DECAD")
        assert result == dt.date(2024, 1, 10)

    def test_after_25th_advances_to_last_day(self):
        """Jan 26 → Jan 31 (last day) in PENTAD mode."""
        result = get_next_forecast_day(dt.date(2024, 1, 26), "PENTAD")
        assert result == dt.date(2024, 1, 31)

    def test_after_20th_decad_advances_to_last_day(self):
        """Jan 21 → Jan 31 (last day) in DECAD mode."""
        result = get_next_forecast_day(dt.date(2024, 1, 21), "DECAD")
        assert result == dt.date(2024, 1, 31)

    # --- Month and year boundaries ---

    def test_month_boundary_feb_pentad(self):
        """Feb 1 → Feb 5 in PENTAD mode."""
        result = get_next_forecast_day(dt.date(2024, 2, 1), "PENTAD")
        assert result == dt.date(2024, 2, 5)

    def test_year_boundary(self):
        """Jan 1 of new year → Jan 5."""
        result = get_next_forecast_day(dt.date(2025, 1, 1), "PENTAD")
        assert result == dt.date(2025, 1, 5)

    def test_dec_31_is_forecast_day(self):
        """Dec 31 is always the last day of the month → forecast day."""
        result = get_next_forecast_day(dt.date(2024, 12, 31), "PENTAD")
        assert result == dt.date(2024, 12, 31)

    # --- February edge cases ---

    def test_feb_28_non_leap_is_forecast_day(self):
        """Feb 28 in a non-leap year is the last day → forecast day."""
        result = get_next_forecast_day(dt.date(2023, 2, 28), "PENTAD")
        assert result == dt.date(2023, 2, 28)

    def test_feb_29_leap_is_forecast_day(self):
        """Feb 29 in a leap year is the last day → forecast day."""
        result = get_next_forecast_day(dt.date(2024, 2, 29), "PENTAD")
        assert result == dt.date(2024, 2, 29)

    def test_feb_26_leap_goes_to_29(self):
        """Feb 26 in leap year → Feb 29 (PENTAD: next after 25 is last day)."""
        result = get_next_forecast_day(dt.date(2024, 2, 26), "PENTAD")
        assert result == dt.date(2024, 2, 29)

    def test_feb_26_non_leap_goes_to_28(self):
        """Feb 26 in non-leap year → Feb 28."""
        result = get_next_forecast_day(dt.date(2023, 2, 26), "PENTAD")
        assert result == dt.date(2023, 2, 28)

    # --- DECAD between-day cases ---

    def test_decad_day_11_to_20(self):
        """DECAD: Day 11 → Day 20."""
        result = get_next_forecast_day(dt.date(2024, 3, 11), "DECAD")
        assert result == dt.date(2024, 3, 20)

    # --- Default mode ---

    def test_default_mode_matches_both(self):
        """Default (no mode arg) matches BOTH, which matches PENTAD."""
        default = get_next_forecast_day(dt.date(2024, 6, 3))
        explicit = get_next_forecast_day(dt.date(2024, 6, 3), "BOTH")
        assert default == explicit
