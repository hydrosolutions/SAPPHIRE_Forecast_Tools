from __future__ import annotations

import calendar
from datetime import date, datetime
from typing import Final

SHORT_TERM_HORIZONS: Final = ("day", "pentad", "decade")
LONG_TERM_HORIZONS: Final = ("month", "quarter", "season")
SUPPORTED_HORIZONS: Final = SHORT_TERM_HORIZONS + LONG_TERM_HORIZONS


def normalize_horizon(horizon: str) -> str:
    """Normalize local horizon aliases to API horizon literals."""
    normalized = horizon.strip().lower()
    if normalized == "decad":
        normalized = "decade"
    if normalized not in SUPPORTED_HORIZONS:
        raise ValueError(f"Unsupported horizon: {horizon}")
    return normalized


def short_term_join_key(horizon: str) -> str:
    """Return the join period key for short-term horizons."""
    normalized = normalize_horizon(horizon)
    if normalized not in SHORT_TERM_HORIZONS:
        raise ValueError(f"{normalized} is not a short-term horizon")
    return "horizon_in_year"


def join_key_for_horizon(horizon: str) -> str:
    """Return the normalized join key for a horizon."""
    normalized = normalize_horizon(horizon)
    if normalized in SHORT_TERM_HORIZONS:
        return short_term_join_key(normalized)
    return "calendar_period"


def long_term_calendar_period(
    horizon: str,
    valid_from: str | date | datetime,
    valid_to: str | date | datetime,
    season_start_month: int = 4,
    season_start_day: int = 1,
    season_end_month: int = 9,
    season_end_day: int = 30,
) -> tuple[int, bool]:
    """Map long-term validity dates to a calendar period key."""
    normalized = normalize_horizon(horizon)
    if normalized not in LONG_TERM_HORIZONS:
        raise ValueError(f"{normalized} is not a long-term horizon")

    start = _to_date(valid_from, "valid_from")
    end = _to_date(valid_to, "valid_to")
    if end < start:
        raise ValueError("valid_to must be on or after valid_from")

    if normalized == "month":
        return _month_period(start, end)
    if normalized == "quarter":
        return _quarter_period(start, end)
    return _season_period(
        start,
        end,
        season_start_month,
        season_start_day,
        season_end_month,
        season_end_day,
    )


def _month_period(start: date, end: date) -> tuple[int, bool]:
    last_day = calendar.monthrange(start.year, start.month)[1]
    aligned = (
        start.day == 1
        and end.year == start.year
        and end.month == start.month
        and end.day == last_day
    )
    return start.month, aligned


def _quarter_period(start: date, end: date) -> tuple[int, bool]:
    quarter = ((start.month - 1) // 3) + 1
    first_month = ((quarter - 1) * 3) + 1
    last_month = first_month + 2
    last_day = calendar.monthrange(start.year, last_month)[1]
    aligned = (
        start.month == first_month
        and start.day == 1
        and end.year == start.year
        and end.month == last_month
        and end.day == last_day
    )
    return quarter, aligned


def _season_period(
    start: date,
    end: date,
    season_start_month: int,
    season_start_day: int,
    season_end_month: int,
    season_end_day: int,
) -> tuple[int, bool]:
    season_start = date(start.year, season_start_month, season_start_day)
    season_end = date(start.year, season_end_month, season_end_day)
    return 1, start == season_start and end == season_end


def _to_date(value: str | date | datetime, field_name: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must use ISO date format YYYY-MM-DD") from exc
