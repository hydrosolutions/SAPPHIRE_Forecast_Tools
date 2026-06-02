"""Snow display-window helpers shared by dashboard data and plotting."""

from datetime import date, timedelta

import pandas as pd


def snow_display_window(
    start_month: int,
    start_day: int,
    ref_date: date,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Return (begin, end) Timestamps for the snow display window."""
    if start_month == 1 and start_day == 1:
        return (
            pd.Timestamp(ref_date.year, 1, 1),
            pd.Timestamp(ref_date.year, 12, 31),
        )
    year_start = date(ref_date.year, start_month, start_day)
    if ref_date >= year_start:
        begin = year_start
    else:
        begin = date(ref_date.year - 1, start_month, start_day)
    end = date(begin.year + 1, start_month, start_day) - timedelta(days=1)
    return pd.Timestamp(begin), pd.Timestamp(end)


def is_hydrological_year_display(month: int, day: int) -> bool:
    """True iff the configured display start is not Jan 1."""
    return (month, day) != (1, 1)
