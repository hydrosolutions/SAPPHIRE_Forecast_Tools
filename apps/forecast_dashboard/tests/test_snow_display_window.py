"""Unit tests for snow_display_window (snow visualization display window).

Tests the calendar-year and hydrological-year window computation logic used
by the snow visualization panel.
"""

from datetime import date

import pandas as pd
import pytest
from src.snow_window import snow_display_window as _snow_display_window


# ===========================================================================
# Class 1: TestSnowDisplayWindowCalendarYear
# ===========================================================================


class TestSnowDisplayWindowCalendarYear:
    """Tests for the calendar year path (start_month=1, start_day=1)."""

    def test_jan1_returns_full_calendar_year(self):
        ref_date = date(2026, 6, 15)
        begin, end = _snow_display_window(1, 1, ref_date)
        assert begin == pd.Timestamp(2026, 1, 1)
        assert end == pd.Timestamp(2026, 12, 31)

    def test_jan1_on_dec31(self):
        ref_date = date(2026, 12, 31)
        begin, end = _snow_display_window(1, 1, ref_date)
        assert begin == pd.Timestamp(2026, 1, 1)
        assert end == pd.Timestamp(2026, 12, 31)

    def test_jan1_on_jan1(self):
        ref_date = date(2026, 1, 1)
        begin, end = _snow_display_window(1, 1, ref_date)
        assert begin == pd.Timestamp(2026, 1, 1)
        assert end == pd.Timestamp(2026, 12, 31)


# ===========================================================================
# Class 2: TestSnowDisplayWindowHydrologicalYear
# ===========================================================================


class TestSnowDisplayWindowHydrologicalYear:
    """Tests for the hydrological year path (non-Jan-1 start)."""

    def test_sep1_ref_in_autumn(self):
        # ref after start → window begins in ref's year
        begin, end = _snow_display_window(9, 1, date(2025, 10, 15))
        assert begin == pd.Timestamp(2025, 9, 1)
        assert end == pd.Timestamp(2026, 8, 31)

    def test_sep1_ref_in_spring(self):
        # ref before start → window began in previous year
        begin, end = _snow_display_window(9, 1, date(2026, 3, 15))
        assert begin == pd.Timestamp(2025, 9, 1)
        assert end == pd.Timestamp(2026, 8, 31)

    def test_sep1_ref_exactly_on_start(self):
        # ref == year_start → begin is current year (>= branch)
        begin, end = _snow_display_window(9, 1, date(2025, 9, 1))
        assert begin == pd.Timestamp(2025, 9, 1)
        assert end == pd.Timestamp(2026, 8, 31)

    def test_sep1_ref_day_before_start(self):
        # ref one day before start → window goes back one more year
        begin, end = _snow_display_window(9, 1, date(2025, 8, 31))
        assert begin == pd.Timestamp(2024, 9, 1)
        assert end == pd.Timestamp(2025, 8, 31)

    def test_sep1_ref_on_jan1(self):
        # Jan 1 is before Sep 1 in the same year → prior year window
        begin, end = _snow_display_window(9, 1, date(2026, 1, 1))
        assert begin == pd.Timestamp(2025, 9, 1)
        assert end == pd.Timestamp(2026, 8, 31)

    def test_sep1_ref_on_dec31(self):
        # Dec 31 is after Sep 1 in the same year → current year window
        begin, end = _snow_display_window(9, 1, date(2025, 12, 31))
        assert begin == pd.Timestamp(2025, 9, 1)
        assert end == pd.Timestamp(2026, 8, 31)

    def test_oct1_start(self):
        # Oct-1 start; ref in November → window runs Oct 2025 – Sep 2026
        begin, end = _snow_display_window(10, 1, date(2025, 11, 1))
        assert begin == pd.Timestamp(2025, 10, 1)
        assert end == pd.Timestamp(2026, 9, 30)

    def test_mar15_midmonth_start(self):
        # Mar-15 start; ref in April → window runs Mar 2026 – Mar 2027
        begin, end = _snow_display_window(3, 15, date(2026, 4, 1))
        assert begin == pd.Timestamp(2026, 3, 15)
        assert end == pd.Timestamp(2027, 3, 14)


# ===========================================================================
# Class 3: TestSnowDisplayWindowEndDateConsistency
# ===========================================================================


class TestSnowDisplayWindowEndDateConsistency:
    """Structural invariants that must hold for all non-calendar windows."""

    def test_window_is_always_less_than_366_days(self):
        # For each combo, window length must be 365 or 366 days
        cases = [
            (9, 1, date(2025, 10, 15)),   # normal year
            (9, 1, date(2026, 3, 15)),    # normal year, before start
            (10, 1, date(2025, 11, 1)),   # Oct start
            (3, 15, date(2026, 4, 1)),    # mid-month start
            (9, 1, date(2023, 10, 1)),    # window spans into leap year 2024
        ]
        for start_month, start_day, ref_date in cases:
            begin, end = _snow_display_window(start_month, start_day, ref_date)
            length = (end - begin).days + 1
            assert length in (365, 366), (
                f"start={start_month}/{start_day}, ref={ref_date}: "
                f"window length {length} is not 365 or 366"
            )

    def test_end_is_day_before_next_start(self):
        # For sep-1 start the end must always be Aug 31
        ref_dates = [
            date(2025, 9, 1),
            date(2025, 10, 15),
            date(2026, 3, 15),
            date(2026, 1, 1),
            date(2025, 12, 31),
            date(2025, 8, 31),
        ]
        for ref_date in ref_dates:
            _, end = _snow_display_window(9, 1, ref_date)
            assert end.month == 8, (
                f"ref={ref_date}: expected end month 8, got {end.month}"
            )
            assert end.day == 31, (
                f"ref={ref_date}: expected end day 31, got {end.day}"
            )
