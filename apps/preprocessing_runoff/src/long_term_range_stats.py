"""Pure calculator for long-term observation range statistics.

Configuration via environment variables (independent of skill-scoring settings):
    SAPPHIRE_RANGE_REFERENCE_START_YEAR  default 2000
    SAPPHIRE_RANGE_REFERENCE_END_YEAR    default 2025

Minimum eligible years: max(2, ceil(N_years / 2)).
For 2000-2025 (26 years): 13 accepted years required for valid std.
Below threshold: std=None, count=actual accepted count.
"""

from __future__ import annotations

import calendar
import datetime as dt
import math
import os
from dataclasses import dataclass, field
from typing import Any

_DEFAULT_START = 2000
_DEFAULT_END = 2025

# April-September: the only season-1 window published in stage 1.
SEASON_MONTHS = tuple(range(4, 10))
# Minimum accepted months for a seasonal year to be eligible.
SEASON_MIN_ACCEPTED_MONTHS = 3

QUARTER_MONTHS = {1: (1, 2, 3), 2: (4, 5, 6), 3: (7, 8, 9), 4: (10, 11, 12)}


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


_YEAR_MIN = dt.MINYEAR  # 1
_YEAR_MAX = dt.MAXYEAR  # 9999


def get_reference_start_year() -> int:
    raw = os.getenv("SAPPHIRE_RANGE_REFERENCE_START_YEAR")
    if raw is None:
        return _DEFAULT_START
    try:
        return int(raw.strip())
    except (ValueError, AttributeError) as exc:
        raise ValueError(
            f"SAPPHIRE_RANGE_REFERENCE_START_YEAR is set but malformed: {raw!r}"
        ) from exc


def get_reference_end_year() -> int:
    raw = os.getenv("SAPPHIRE_RANGE_REFERENCE_END_YEAR")
    if raw is None:
        return _DEFAULT_END
    try:
        return int(raw.strip())
    except (ValueError, AttributeError) as exc:
        raise ValueError(
            f"SAPPHIRE_RANGE_REFERENCE_END_YEAR is set but malformed: {raw!r}"
        ) from exc


def validate_reference_window(start: int, end: int, today_year: int) -> None:
    """Raise ValueError when years are out of calendar bounds, start > end,
    or end >= today_year (incomplete year)."""
    if start < _YEAR_MIN or start > _YEAR_MAX:
        raise ValueError(
            f"SAPPHIRE_RANGE_REFERENCE_START_YEAR ({start}) is outside the valid "
            f"range [{_YEAR_MIN}, {_YEAR_MAX}]"
        )
    if end < _YEAR_MIN or end > _YEAR_MAX:
        raise ValueError(
            f"SAPPHIRE_RANGE_REFERENCE_END_YEAR ({end}) is outside the valid "
            f"range [{_YEAR_MIN}, {_YEAR_MAX}]"
        )
    if start > end:
        raise ValueError(
            f"SAPPHIRE_RANGE_REFERENCE_START_YEAR ({start}) must be <= "
            f"SAPPHIRE_RANGE_REFERENCE_END_YEAR ({end})"
        )
    if end >= today_year:
        raise ValueError(
            f"SAPPHIRE_RANGE_REFERENCE_END_YEAR ({end}) must be strictly less than "
            f"today's year ({today_year}); the reference end year must be completed."
        )


def min_eligible_years(n_years: int) -> int:
    """Minimum accepted years required: max(2, ceil(n_years / 2))."""
    return max(2, math.ceil(n_years / 2))


def reference_years(start: int, end: int) -> list[int]:
    """All years in [start, end] inclusive."""
    return list(range(start, end + 1))


# ---------------------------------------------------------------------------
# Row helpers
# ---------------------------------------------------------------------------


def _iter_rows(rows: Any) -> list[dict]:
    """Convert various daily-row containers (list, DataFrame, None) to list[dict]."""
    if rows is None:
        return []
    # Duck-type pandas DataFrame
    try:
        return rows.to_dict("records")
    except AttributeError:
        pass
    if isinstance(rows, list):
        return rows
    try:
        return list(rows)
    except TypeError:
        return []


def _get_year_month(date_value: Any) -> tuple[int | None, int | None]:
    """Extract (year, month) from a date string, date, or datetime object."""
    if isinstance(date_value, (dt.date, dt.datetime)):
        return date_value.year, date_value.month
    s = str(date_value)[:10]
    try:
        d = dt.date.fromisoformat(s)
        return d.year, d.month
    except (ValueError, AttributeError):
        return None, None


# ---------------------------------------------------------------------------
# Core coverage computation
# ---------------------------------------------------------------------------


def _monthly_mean_50pct(rows: Any, year: int, month: int) -> float | None:
    """Monthly mean when distinct finite observation dates cover >= 50% of the month's days.

    Uses the postprocessing daily-coverage rule (50%) rather than the
    hydrograph-actuals 80% rule. Does not round the returned value.
    Duplicate rows for the same calendar day keep the first finite value only,
    so coverage is never inflated by repeated rows.
    """
    days_in_month = calendar.monthrange(year, month)[1]
    threshold = days_in_month / 2  # exactly 50%
    day_values: dict[int, float] = {}  # {calendar_day: first finite value}
    for row in _iter_rows(rows):
        if not isinstance(row, dict):
            continue
        y, m = _get_year_month(row.get("date"))
        if y != year or m != month:
            continue
        value = row.get("discharge")
        if value is None:
            continue
        try:
            fv = float(value)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(fv):
            continue
        date_val = row.get("date")
        if isinstance(date_val, (dt.date, dt.datetime)):
            day = date_val.day
        else:
            try:
                day = dt.date.fromisoformat(str(date_val)[:10]).day
            except (ValueError, AttributeError):
                continue
        if day not in day_values:
            day_values[day] = fv
    if len(day_values) < threshold:
        return None
    return sum(day_values.values()) / len(day_values)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PeriodStats:
    """Sample std (ddof=1) and accepted-year count for one station/period pair."""

    std: float | None  # None when below min_eligible_years threshold
    count: int  # always the accepted-year count, never inflated by dupes


@dataclass(frozen=True)
class StationRangeStats:
    """All period statistics for one station.

    monthly: {calendar_month_1_12: PeriodStats}
    quarterly: {quarter_1_4: PeriodStats}
    seasonal: PeriodStats for April-September (season 1)
    """

    monthly: dict[int, PeriodStats] = field(default_factory=dict)
    quarterly: dict[int, PeriodStats] = field(default_factory=dict)
    seasonal: PeriodStats | None = None


# ---------------------------------------------------------------------------
# Aggregation helpers
# ---------------------------------------------------------------------------


def _accepted_yearly_quarterly_means(
    monthly_means_by_year: dict[int, dict[int, float]],
    quarter: int,
) -> dict[int, float]:
    """Quarterly means: all 3 constituent months must be accepted (equal-weight)."""
    months = QUARTER_MONTHS[quarter]
    result: dict[int, float] = {}
    for year, month_means in monthly_means_by_year.items():
        if all(m in month_means for m in months):
            result[year] = sum(month_means[m] for m in months) / 3
    return result


def _accepted_yearly_seasonal_means(
    monthly_means_by_year: dict[int, dict[int, float]],
) -> dict[int, float]:
    """Apr-Sep seasonal means: >= SEASON_MIN_ACCEPTED_MONTHS of 6 required (equal-weight)."""
    result: dict[int, float] = {}
    for year, month_means in monthly_means_by_year.items():
        accepted = [m for m in SEASON_MONTHS if m in month_means]
        if len(accepted) >= SEASON_MIN_ACCEPTED_MONTHS:
            result[year] = sum(month_means[m] for m in accepted) / len(accepted)
    return result


# ---------------------------------------------------------------------------
# Period-stats calculator
# ---------------------------------------------------------------------------


def compute_period_stats(accepted_means: dict[int, float], min_years: int) -> PeriodStats:
    """Sample std (ddof=1) and count from {year: mean}.

    - count < 2 or count < min_years: std=None, count=actual count
    - all values identical: std=0.0 (genuine zero variability, not missing)
    - otherwise: std = sample std with ddof=1
    Duplicate year keys cannot occur (dict keys are unique), so count
    is always the number of distinct eligible years.
    """
    count = len(accepted_means)
    if count < 2 or count < min_years:
        return PeriodStats(std=None, count=count)
    values = list(accepted_means.values())
    mean = sum(values) / count
    variance = sum((v - mean) ** 2 for v in values) / (count - 1)
    return PeriodStats(std=math.sqrt(variance), count=count)


# ---------------------------------------------------------------------------
# Station-level calculator
# ---------------------------------------------------------------------------


def compute_station_range_stats(
    daily_by_year: dict[int, Any],
    ref_years: list[int],
    min_years: int,
) -> StationRangeStats:
    """Compute monthly/quarterly/seasonal range statistics for one station.

    Args:
        daily_by_year: {year: daily rows} where rows may be list[dict] or DataFrame.
                       Missing years are treated as having no observations.
        ref_years: ordered list of reference years from the configured window.
        min_years: minimum accepted years for valid std (from min_eligible_years).

    Returns:
        StationRangeStats with all 12 monthly, 4 quarterly, and 1 seasonal PeriodStats.
        Periods below the minimum-history threshold publish std=None and actual count.
        Only April-September seasonal statistics are published (stage-1 scope).
    """
    # Build {year: {month: mean}} for all 12 months across all reference years.
    monthly_means_by_year: dict[int, dict[int, float]] = {}
    for year in ref_years:
        rows = daily_by_year.get(year)
        monthly_means_by_year[year] = {}
        for month in range(1, 13):
            mean = _monthly_mean_50pct(rows, year, month)
            if mean is not None:
                monthly_means_by_year[year][month] = mean

    # Monthly stats: one PeriodStats per calendar month.
    monthly_stats: dict[int, PeriodStats] = {}
    for month in range(1, 13):
        accepted = {
            year: monthly_means_by_year[year][month]
            for year in ref_years
            if month in monthly_means_by_year.get(year, {})
        }
        monthly_stats[month] = compute_period_stats(accepted, min_years)

    # Quarterly stats: requires all 3 constituent months per year.
    quarterly_stats: dict[int, PeriodStats] = {}
    for quarter in range(1, 5):
        accepted_q = _accepted_yearly_quarterly_means(monthly_means_by_year, quarter)
        quarterly_stats[quarter] = compute_period_stats(accepted_q, min_years)

    # Seasonal stats: April-September, >= 3 of 6 months per year.
    accepted_s = _accepted_yearly_seasonal_means(monthly_means_by_year)
    seasonal_stats = compute_period_stats(accepted_s, min_years)

    return StationRangeStats(
        monthly=monthly_stats,
        quarterly=quarterly_stats,
        seasonal=seasonal_stats,
    )
