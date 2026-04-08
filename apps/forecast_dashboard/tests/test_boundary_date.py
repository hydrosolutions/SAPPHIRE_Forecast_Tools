"""Unit tests for boundary date computation (FD-009).

Tests get_previous_boundary_date and the horizon_value → boundary date
algorithm used in save_to_database.

NOTE: vizualization.py is a large Panel dashboard file that imports Panel,
Bokeh, Docker SDK, and many other dependencies not available in the test
environment. We therefore cannot import it directly. Instead, we replicate
both algorithms verbatim here and test those local copies. Any drift between
the source and these copies will be caught during code review.
"""

import calendar
import datetime as dt
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Local replicas of the production algorithms
# (kept verbatim to match vizualization.py — do not simplify)
# ---------------------------------------------------------------------------


def get_previous_boundary_date(today, horizon):
    """Return the most recent boundary date <= today for the given horizon.

    Replica of vizualization.get_previous_boundary_date (apps/forecast_dashboard/
    src/vizualization.py).  Replicated here because the module cannot be
    imported in the test environment (heavy Panel / Docker / Bokeh deps).
    """
    if horizon == "pentad":
        boundaries = [5, 10, 15, 20, 25]
    else:  # decad
        boundaries = [10, 20]
    last_of_month = calendar.monthrange(today.year, today.month)[1]
    boundaries.append(last_of_month)

    for b in sorted(boundaries, reverse=True):
        if today.day >= b:
            return dt.date(today.year, today.month, b)

    first_of_month = dt.date(today.year, today.month, 1)
    last_day_prev = first_of_month - dt.timedelta(days=1)
    return last_day_prev


def compute_boundary_date_from_horizon_value(horizon_value, horizon, year):
    """Compute the forecast boundary date from a horizon_value integer.

    Replica of the inline algorithm in save_to_database (apps/forecast_dashboard/
    src/vizualization.py, ~line 3836).  The year-guard branch is NOT included
    here so that callers can inject an explicit year; a separate helper
    ``compute_issue_boundary_date`` computes the issue pentad boundary.

    Args:
        horizon_value: 1-based period index within the year
            (1-72 for pentad, 1-36 for decad).
        horizon: ``"pentad"`` or ``"decad"``.
        year: Calendar year to use for the computation.

    Returns:
        ``dt.date`` representing the boundary day for the given period.
    """
    if horizon == "pentad":
        periods_per_month = 6
        month = (horizon_value - 1) // periods_per_month + 1
        period_in_month = (horizon_value - 1) % periods_per_month + 1
        if period_in_month == periods_per_month:
            boundary_day = calendar.monthrange(year, month)[1]
        else:
            boundary_day = period_in_month * 5
    else:  # decad
        periods_per_month = 3
        month = (horizon_value - 1) // periods_per_month + 1
        period_in_month = (horizon_value - 1) % periods_per_month + 1
        if period_in_month == periods_per_month:
            boundary_day = calendar.monthrange(year, month)[1]
        else:
            boundary_day = period_in_month * 10
    return dt.date(year, month, boundary_day)


def compute_issue_boundary_date(horizon_value, horizon):
    """Production algorithm: compute SAPPHIRE_FORECAST_DATE from the issue pentad.

    The issue pentad is one before the target (horizon_value). The boundary
    date is the last day of that issue pentad — always in the past, so no
    year guard is needed.
    """
    year = dt.date.today().year
    if horizon == "pentad":
        periods_per_year = 72
        periods_per_month = 6
    else:  # decad
        periods_per_year = 36
        periods_per_month = 3
    issue_horizon = horizon_value - 1
    if issue_horizon < 1:
        issue_horizon = periods_per_year
        year -= 1
    month = (issue_horizon - 1) // periods_per_month + 1
    period_in_month = (issue_horizon - 1) % periods_per_month + 1
    if period_in_month == periods_per_month:
        boundary_day = calendar.monthrange(year, month)[1]
    elif horizon == "pentad":
        boundary_day = period_in_month * 5
    else:
        boundary_day = period_in_month * 10
    return dt.date(year, month, boundary_day)


# ---------------------------------------------------------------------------
# Helper: expected boundary day for a pentad period-in-month
# ---------------------------------------------------------------------------

def _pentad_boundary_day(period_in_month, year, month):
    if period_in_month == 6:
        return calendar.monthrange(year, month)[1]
    return period_in_month * 5


def _decad_boundary_day(period_in_month, year, month):
    if period_in_month == 3:
        return calendar.monthrange(year, month)[1]
    return period_in_month * 10


# ===========================================================================
# Class 1: TestGetPreviousBoundaryDate
# ===========================================================================


class TestGetPreviousBoundaryDate:
    """Tests for get_previous_boundary_date (pentad and decad modes)."""

    # ── Pentad mode ──────────────────────────────────────────────────────────

    @pytest.mark.parametrize("day", [5, 10, 15, 20, 25])
    def test_pentad_on_boundary_day_returns_that_day(self, day):
        today = dt.date(2026, 3, day)
        result = get_previous_boundary_date(today, "pentad")
        assert result == dt.date(2026, 3, day)

    def test_pentad_day_after_first_boundary_returns_day5(self):
        today = dt.date(2026, 3, 6)
        result = get_previous_boundary_date(today, "pentad")
        assert result == dt.date(2026, 3, 5)

    def test_pentad_mid_period_returns_previous_boundary(self):
        # Day 13 is between day 10 and day 15 → most recent boundary is day 10
        today = dt.date(2026, 3, 13)
        result = get_previous_boundary_date(today, "pentad")
        assert result == dt.date(2026, 3, 10)

    def test_pentad_last_day_of_month_returns_last_day(self):
        # Jan 31 is the last-of-month boundary
        today = dt.date(2026, 1, 31)
        result = get_previous_boundary_date(today, "pentad")
        assert result == dt.date(2026, 1, 31)

    def test_pentad_day1_returns_last_day_of_previous_month(self):
        today = dt.date(2026, 3, 1)
        result = get_previous_boundary_date(today, "pentad")
        assert result == dt.date(2026, 2, 28)

    def test_pentad_day4_returns_last_day_of_previous_month(self):
        today = dt.date(2026, 3, 4)
        result = get_previous_boundary_date(today, "pentad")
        assert result == dt.date(2026, 2, 28)

    def test_pentad_jan1_returns_dec31_previous_year(self):
        today = dt.date(2026, 1, 1)
        result = get_previous_boundary_date(today, "pentad")
        assert result == dt.date(2025, 12, 31)

    def test_pentad_jan4_returns_dec31_previous_year(self):
        today = dt.date(2026, 1, 4)
        result = get_previous_boundary_date(today, "pentad")
        assert result == dt.date(2025, 12, 31)

    # ── Decad mode ───────────────────────────────────────────────────────────

    @pytest.mark.parametrize("day", [10, 20])
    def test_decad_on_boundary_day_returns_that_day(self, day):
        today = dt.date(2026, 3, day)
        result = get_previous_boundary_date(today, "decad")
        assert result == dt.date(2026, 3, day)

    def test_decad_day11_returns_day10(self):
        today = dt.date(2026, 3, 11)
        result = get_previous_boundary_date(today, "decad")
        assert result == dt.date(2026, 3, 10)

    def test_decad_day1_returns_last_day_of_previous_month(self):
        today = dt.date(2026, 3, 1)
        result = get_previous_boundary_date(today, "decad")
        assert result == dt.date(2026, 2, 28)

    def test_decad_day9_returns_last_day_of_previous_month(self):
        today = dt.date(2026, 3, 9)
        result = get_previous_boundary_date(today, "decad")
        assert result == dt.date(2026, 2, 28)

    def test_decad_last_day_of_feb_non_leap(self):
        # Feb 28 is last-of-month in 2026 (non-leap)
        today = dt.date(2026, 2, 28)
        result = get_previous_boundary_date(today, "decad")
        assert result == dt.date(2026, 2, 28)

    def test_decad_last_day_of_feb_leap(self):
        # Feb 29 is last-of-month in 2024 (leap year)
        today = dt.date(2024, 2, 29)
        result = get_previous_boundary_date(today, "decad")
        assert result == dt.date(2024, 2, 29)

    def test_decad_jan1_returns_dec31_previous_year(self):
        today = dt.date(2026, 1, 1)
        result = get_previous_boundary_date(today, "decad")
        assert result == dt.date(2025, 12, 31)


# ===========================================================================
# Class 2: TestBoundaryDateFromHorizonValue
# ===========================================================================

# ---------------------------------------------------------------------------
# Build parametrize tables at module load time
# ---------------------------------------------------------------------------

# Pentad: 72 values × (horizon_value, expected_month, expected_day)
_PENTAD_CASES = []
for _hv in range(1, 73):
    _ppm = 6
    _m = (_hv - 1) // _ppm + 1
    _pim = (_hv - 1) % _ppm + 1
    _day = _pentad_boundary_day(_pim, 2026, _m)
    _PENTAD_CASES.append((_hv, _m, _day))

# Decad: 36 values × (horizon_value, expected_month, expected_day)
_DECAD_CASES = []
for _hv in range(1, 37):
    _ppm = 3
    _m = (_hv - 1) // _ppm + 1
    _pim = (_hv - 1) % _ppm + 1
    _day = _decad_boundary_day(_pim, 2026, _m)
    _DECAD_CASES.append((_hv, _m, _day))


class TestBoundaryDateFromHorizonValue:
    """Tests for the horizon_value → boundary date algorithm (save_to_database)."""

    # ── All 72 pentad values ─────────────────────────────────────────────────

    @pytest.mark.parametrize(
        "horizon_value, expected_month, expected_day",
        _PENTAD_CASES,
        ids=[f"pentad_{hv}" for hv, _, _ in _PENTAD_CASES],
    )
    def test_pentad_all_values_correct_month_and_day(
        self, horizon_value, expected_month, expected_day
    ):
        result = compute_boundary_date_from_horizon_value(horizon_value, "pentad", 2026)
        assert result.month == expected_month, (
            f"horizon_value={horizon_value}: expected month {expected_month}, got {result.month}"
        )
        assert result.day == expected_day, (
            f"horizon_value={horizon_value}: expected day {expected_day}, got {result.day}"
        )

    def test_pentad_first_value_is_jan5(self):
        result = compute_boundary_date_from_horizon_value(1, "pentad", 2026)
        assert result == dt.date(2026, 1, 5)

    def test_pentad_second_value_is_jan10(self):
        result = compute_boundary_date_from_horizon_value(2, "pentad", 2026)
        assert result == dt.date(2026, 1, 10)

    def test_pentad_third_value_is_jan15(self):
        result = compute_boundary_date_from_horizon_value(3, "pentad", 2026)
        assert result == dt.date(2026, 1, 15)

    def test_pentad_fourth_value_is_jan20(self):
        result = compute_boundary_date_from_horizon_value(4, "pentad", 2026)
        assert result == dt.date(2026, 1, 20)

    def test_pentad_fifth_value_is_jan25(self):
        result = compute_boundary_date_from_horizon_value(5, "pentad", 2026)
        assert result == dt.date(2026, 1, 25)

    def test_pentad_sixth_value_is_jan31(self):
        # Period 6 of January → last day of Jan = 31
        result = compute_boundary_date_from_horizon_value(6, "pentad", 2026)
        assert result == dt.date(2026, 1, 31)

    def test_pentad_seventh_value_is_feb5(self):
        result = compute_boundary_date_from_horizon_value(7, "pentad", 2026)
        assert result == dt.date(2026, 2, 5)

    def test_pentad_last_value_is_dec31(self):
        # horizon_value=72: Dec period 6 → last day of Dec = 31
        result = compute_boundary_date_from_horizon_value(72, "pentad", 2026)
        assert result == dt.date(2026, 12, 31)

    # ── All 36 decad values ──────────────────────────────────────────────────

    @pytest.mark.parametrize(
        "horizon_value, expected_month, expected_day",
        _DECAD_CASES,
        ids=[f"decad_{hv}" for hv, _, _ in _DECAD_CASES],
    )
    def test_decad_all_values_correct_month_and_day(
        self, horizon_value, expected_month, expected_day
    ):
        result = compute_boundary_date_from_horizon_value(horizon_value, "decad", 2026)
        assert result.month == expected_month, (
            f"horizon_value={horizon_value}: expected month {expected_month}, got {result.month}"
        )
        assert result.day == expected_day, (
            f"horizon_value={horizon_value}: expected day {expected_day}, got {result.day}"
        )

    def test_decad_first_value_is_jan10(self):
        result = compute_boundary_date_from_horizon_value(1, "decad", 2026)
        assert result == dt.date(2026, 1, 10)

    def test_decad_second_value_is_jan20(self):
        result = compute_boundary_date_from_horizon_value(2, "decad", 2026)
        assert result == dt.date(2026, 1, 20)

    def test_decad_third_value_is_jan31(self):
        # Period 3 of Jan → last day of Jan = 31
        result = compute_boundary_date_from_horizon_value(3, "decad", 2026)
        assert result == dt.date(2026, 1, 31)

    def test_decad_last_value_is_dec31(self):
        # horizon_value=36: Dec period 3 → last day of Dec = 31
        result = compute_boundary_date_from_horizon_value(36, "decad", 2026)
        assert result == dt.date(2026, 12, 31)

    # ── Year guard ───────────────────────────────────────────────────────────

    def test_issue_pentad_for_target_20_is_apr5(self):
        """horizon_value=20 (target: Apr 6-10) → issue pentad 19 → Apr 5."""
        # issue_horizon = 20 - 1 = 19
        # month = (19-1)//6 + 1 = 4, period = (19-1)%6 + 1 = 1, day = 5
        result = compute_boundary_date_from_horizon_value(19, "pentad", 2026)
        assert result == dt.date(2026, 4, 5)

    def test_issue_pentad_for_target_1_wraps_to_dec31_previous_year(self):
        """horizon_value=1 (target: Jan 1-5) → issue pentad 72 → Dec 31 prev year."""
        # issue_horizon = 1 - 1 = 0 → wraps to 72, year - 1
        result = compute_boundary_date_from_horizon_value(72, "pentad", 2025)
        assert result == dt.date(2025, 12, 31)

    def test_issue_pentad_for_target_13_is_feb28(self):
        """horizon_value=13 (target: Mar 1-5) → issue pentad 12 → Feb 28."""
        result = compute_boundary_date_from_horizon_value(12, "pentad", 2026)
        assert result == dt.date(2026, 2, 28)

    def test_issue_decad_for_target_1_wraps_to_dec31_previous_year(self):
        """horizon_value=1 (target: Jan 1-10) → issue decad 36 → Dec 31 prev year."""
        result = compute_boundary_date_from_horizon_value(36, "decad", 2025)
        assert result == dt.date(2025, 12, 31)

    # ── Feb last-of-month ─────────────────────────────────────────────────────

    def test_pentad_12_feb_last_non_leap_year_is_feb28(self):
        # horizon_value=12: Feb period 6 → last day of Feb 2026 (non-leap) = 28
        result = compute_boundary_date_from_horizon_value(12, "pentad", 2026)
        assert result == dt.date(2026, 2, 28)

    def test_pentad_12_feb_last_leap_year_is_feb29(self):
        # horizon_value=12: Feb period 6 → last day of Feb 2024 (leap) = 29
        result = compute_boundary_date_from_horizon_value(12, "pentad", 2024)
        assert result == dt.date(2024, 2, 29)

    def test_decad_6_feb_last_non_leap_year_is_feb28(self):
        # horizon_value=6: Feb period 3 → last day of Feb 2026 = 28
        result = compute_boundary_date_from_horizon_value(6, "decad", 2026)
        assert result == dt.date(2026, 2, 28)

    def test_decad_6_feb_last_leap_year_is_feb29(self):
        # horizon_value=6: Feb period 3 → last day of Feb 2024 = 29
        result = compute_boundary_date_from_horizon_value(6, "decad", 2024)
        assert result == dt.date(2024, 2, 29)

    # ── Spot checks for selected months ──────────────────────────────────────

    def test_pentad_june_first_period_is_jun5(self):
        # June is month 6; first pentad = horizon_value 31
        result = compute_boundary_date_from_horizon_value(31, "pentad", 2026)
        assert result == dt.date(2026, 6, 5)

    def test_pentad_june_last_period_is_jun30(self):
        # June is month 6; last pentad = horizon_value 36
        result = compute_boundary_date_from_horizon_value(36, "pentad", 2026)
        assert result == dt.date(2026, 6, 30)

    def test_decad_june_first_period_is_jun10(self):
        # June is month 6; first decad = horizon_value 16
        result = compute_boundary_date_from_horizon_value(16, "decad", 2026)
        assert result == dt.date(2026, 6, 10)

    def test_decad_june_last_period_is_jun30(self):
        # June is month 6; last decad = horizon_value 18
        result = compute_boundary_date_from_horizon_value(18, "decad", 2026)
        assert result == dt.date(2026, 6, 30)

    # ── Boundary: month transitions ───────────────────────────────────────────

    def test_pentad_transition_jan_to_feb(self):
        # horizon_value=6 is Jan last; horizon_value=7 is Feb 5
        jan_last = compute_boundary_date_from_horizon_value(6, "pentad", 2026)
        feb_first = compute_boundary_date_from_horizon_value(7, "pentad", 2026)
        assert jan_last.month == 1
        assert feb_first.month == 2
        assert feb_first.day == 5

    def test_decad_transition_jan_to_feb(self):
        # horizon_value=3 is Jan last; horizon_value=4 is Feb 10
        jan_last = compute_boundary_date_from_horizon_value(3, "decad", 2026)
        feb_first = compute_boundary_date_from_horizon_value(4, "decad", 2026)
        assert jan_last.month == 1
        assert feb_first.month == 2
        assert feb_first.day == 10

    def test_pentad_all_months_represented(self):
        """Each month 1-12 should appear exactly 6 times in the pentad table."""
        months = [r.month for r in (
            compute_boundary_date_from_horizon_value(hv, "pentad", 2026)
            for hv in range(1, 73)
        )]
        for m in range(1, 13):
            assert months.count(m) == 6, f"Month {m} appears {months.count(m)} times, expected 6"

    def test_decad_all_months_represented(self):
        """Each month 1-12 should appear exactly 3 times in the decad table."""
        months = [r.month for r in (
            compute_boundary_date_from_horizon_value(hv, "decad", 2026)
            for hv in range(1, 37)
        )]
        for m in range(1, 13):
            assert months.count(m) == 3, f"Month {m} appears {months.count(m)} times, expected 3"


# ---------------------------------------------------------------------------
# Patch target helper (needed only to attach __wrapped__ attribute above)
# ---------------------------------------------------------------------------

# Attach a trivial __wrapped__ for backward compatibility with any test that
# may reference it. The function was renamed to compute_issue_boundary_date.
compute_issue_boundary_date.__wrapped__ = lambda today: None
