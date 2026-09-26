"""Quarter and season definitions with monthly→quarterly/seasonal aggregation.

Single source of truth for how monthly data is aggregated to quarterly
and seasonal horizons.  Used by data_reader.py to build quarterly/seasonal
observations and forecasts from existing monthly records.

Design decisions:
- Fixed calendar quarters: Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec
- Season: configurable start/end month via environment variables,
  supports cross-year boundary (e.g. Oct-Mar)
- Delta = 0.674 * std (same convention as monthly)
"""

import logging
import os

import numpy as np
import pandas as pd
from skill_lead_aware_flag import skill_lead_aware_enabled
from src.postprocessing_tools import count_quantile_crossings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Quarter constants
# ---------------------------------------------------------------------------

QUARTER_MONTHS: dict[int, list[int]] = {
    1: [1, 2, 3],
    2: [4, 5, 6],
    3: [7, 8, 9],
    4: [10, 11, 12],
}

MONTH_TO_QUARTER: dict[int, int] = {m: q for q, ms in QUARTER_MONTHS.items() for m in ms}


def _parse_local_calendar_date(v) -> pd.Timestamp:
    """Return one raw value's LOCAL calendar date as a naive midnight Timestamp.

    Element-wise helper behind ``local_calendar_date``. Any tz offset is
    dropped via ``tz_localize(None)`` -- which keeps the LOCAL wall-clock
    date, unlike ``tz_convert`` which would shift the underlying instant
    to UTC first.

    The ``.as_unit("ns")`` cast happens INSIDE this try, not left to a
    later vectorized ``pd.to_datetime`` call: ``pd.Timestamp`` accepts
    dates outside the datetime64[ns] range (e.g. ``"9999-12-31"``,
    ``"0001-04-01"``, year 2500) by holding them at second resolution,
    but casting such a Timestamp to ns raises
    ``OutOfBoundsDatetime`` -- a ``ValueError`` subclass, so it is caught
    the same way an unparseable value is, and gives ``NaT`` instead of
    aborting the caller.

    Args:
        v: A single raw value (string, Timestamp, date, or null).

    Returns:
        A naive, midnight-normalized ``pd.Timestamp`` at ns resolution,
        or ``pd.NaT``.
    """
    try:
        ts = pd.Timestamp(v)
        if pd.isna(ts):
            return pd.NaT
        if ts.tzinfo is not None:
            ts = ts.tz_localize(None)
        return ts.normalize().as_unit("ns")
    except (ValueError, TypeError, OverflowError):
        return pd.NaT


def local_calendar_date(s: pd.Series) -> pd.Series:
    """Return a raw date-like column's LOCAL calendar date as naive datetime64.

    Parses each value with ``pd.Timestamp`` (not a bare
    ``pd.to_datetime(s, format="mixed", errors="coerce")``, which raises
    ``AttributeError`` on a subsequent ``.dt`` access when `s` mixes
    tz-aware and tz-naive strings across rows -- e.g. ``"2024-06-30"``
    next to ``"2024-09-30T00:00:00+06:00"`` -- because "mixed" then
    returns an object-dtype Series of Python objects rather than
    datetime64). A previous version of this helper instead sliced the
    string form to its first 10 characters and parsed with a fixed
    ``format="%Y-%m-%d"``; that changed which values parse in BOTH
    directions relative to ``format="mixed"`` (e.g. it silently accepted
    ``"2024-04-01garbage"`` and rejected ``"2024/04/01"``), so it is not
    used here.

    Each DISTINCT raw value is parsed only once (``pd.factorize``, which
    is NaN-safe: ``None``/``NaN``/``NaT`` all collapse to one sentinel
    and are excluded from the unique values), then broadcast back to
    every row -- at scale, most values repeat (the same handful of
    issue/target dates across many rows), and ``pd.Timestamp`` parsing
    per row dominates runtime otherwise.

    Args:
        s: Raw date-like column (strings, Timestamps, dates, or null).

    Returns:
        Series of naive datetime64[ns] (or NaT), same index as `s`.
        Empty input, and input that is entirely null regardless of its
        own dtype (e.g. an empty or all-NaT tz-aware column), both
        return naive datetime64[ns].
    """
    if len(s) == 0:
        return pd.Series(pd.array([], dtype="datetime64[ns]"), index=s.index)

    codes, uniques = pd.factorize(s, use_na_sentinel=True)
    parsed_uniques = pd.to_datetime(
        pd.Series([_parse_local_calendar_date(v) for v in uniques], dtype=object)
    ).to_numpy(dtype="datetime64[ns]")
    # `codes == -1` marks a null-like entry in `s`; such entries have no
    # corresponding row in `uniques`/`parsed_uniques`, so look them up
    # via one extra NaT appended past the end of the lookup array.
    lookup = np.append(parsed_uniques, np.datetime64("NaT", "ns"))
    values = lookup[np.where(codes == -1, len(parsed_uniques), codes)]
    return pd.Series(values, index=s.index)


def filter_calendar_quarter_windows(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Keep only rows whose window is an exact calendar-quarter window.

    A calendar quarter window has ``valid_from`` on the 1st of
    Jan/Apr/Jul/Oct and ``valid_to`` on the last day of the third month
    of that same quarter, of the SAME year (e.g. 2024-04-01..2024-06-30).
    A rolling window (different start day, different end month/year, or
    a mismatched span) is dropped -- never relabelled into a quarter.

    ``valid_from`` and ``valid_to`` are parsed via ``local_calendar_date``
    (their LOCAL calendar date, tz dropped) and normalized to midnight,
    because reader output mixes date-only strings, timestamps, and --
    across rows -- tz-aware and tz-naive strings; a bare
    ``format="mixed"`` parse can raise on the latter. The normalized
    ``valid_from`` is written back into the returned frame so that a
    subsequent plain ``pd.to_datetime(df["valid_from"])`` cannot raise on
    the mixed formats this helper already resolved. ``valid_to`` is left
    with the dtype it came in with.

    Column-presence rules:
    - Neither ``valid_from`` nor ``valid_to`` present: returned unchanged
      (0 dropped) -- there is nothing to validate.
    - Exactly one of the two columns present: every row is invalid (the
      window cannot be verified), so the result is empty and the dropped
      count is the full row count.
    - Both present: a row with either value null or unparseable is
      invalid and dropped.

    Args:
        df: Frame that may contain ``valid_from`` / ``valid_to`` columns.

    Returns:
        Tuple of (filtered frame, number of rows dropped).
    """
    has_valid_from = "valid_from" in df.columns
    has_valid_to = "valid_to" in df.columns

    if not has_valid_from and not has_valid_to:
        return df, 0

    df = df.copy()

    if not has_valid_from or not has_valid_to:
        # Only one of the two columns is present: no row's window can be
        # verified as a calendar quarter, so every row is invalid.
        dropped = len(df)
        if has_valid_from:
            df["valid_from"] = local_calendar_date(df["valid_from"]).dt.normalize()
        return df.iloc[0:0].copy(), dropped

    valid_from = local_calendar_date(df["valid_from"]).dt.normalize()
    valid_to = local_calendar_date(df["valid_to"]).dt.normalize()

    is_quarter_start = valid_from.dt.day.eq(1) & valid_from.dt.month.isin([1, 4, 7, 10])
    # Last day of the quarter's third month, same year: adding 3 months
    # then subtracting a day stays within the same year for all four
    # quarter-start months (including Oct -> Dec 31 of the same year).
    # datetime64[ns] tops out at 2262-04-11, so this arithmetic can
    # overflow for an in-range valid_from within ~3 months of that limit
    # (e.g. 2262-02-01) even though valid_from itself parsed fine.
    # Compute it only for rows whose valid_from year is <= 2261 (a
    # generous margin below the actual limit); any other row's window
    # cannot be verified this way and is simply not a calendar quarter.
    safe_for_offset = valid_from.dt.year <= 2261
    expected_valid_to = pd.Series(pd.NaT, index=valid_from.index, dtype="datetime64[ns]")
    if safe_for_offset.any():
        expected_valid_to.loc[safe_for_offset] = (
            valid_from.loc[safe_for_offset] + pd.DateOffset(months=3) - pd.Timedelta(days=1)
        )
    is_calendar_window = is_quarter_start & safe_for_offset & valid_to.eq(expected_valid_to)

    mask = valid_from.notna() & valid_to.notna() & is_calendar_window

    df["valid_from"] = valid_from
    kept = df[mask].copy()
    dropped = len(df) - len(kept)
    return kept, dropped


# Minimum months required per quarter (out of 3)
QUARTER_MIN_MONTHS = 2

# Minimum fraction of season months required
SEASON_MIN_COVERAGE = 0.5


# ---------------------------------------------------------------------------
# Season helpers
# ---------------------------------------------------------------------------


def get_season_months() -> list[int]:
    """Return the list of months that define the season.

    Reads SAPPHIRE_SEASON_START_MONTH (default 4) and
    SAPPHIRE_SEASON_END_MONTH (default 9) from env.

    Handles cross-year wrapping: if start > end, the season wraps
    (e.g. start=10, end=3 → [10, 11, 12, 1, 2, 3]).

    Returns:
        Ordered list of month numbers (1-12).
    """
    start = int(os.getenv("SAPPHIRE_SEASON_START_MONTH", "4"))
    end = int(os.getenv("SAPPHIRE_SEASON_END_MONTH", "9"))

    if start <= end:
        return list(range(start, end + 1))
    # Cross-year: e.g. 10→12, 1→3
    return list(range(start, 13)) + list(range(1, end + 1))


def get_season_year(year: int, month: int) -> int:
    """Return the year the season belongs to.

    For cross-year seasons (e.g. Oct-Mar), months in the second
    calendar year belong to the previous year's season.

    Args:
        year: Calendar year of the month.
        month: Month number (1-12).

    Returns:
        The season's reference year.
    """
    season_months = get_season_months()
    start_month = season_months[0]

    if start_month <= month:
        return year
    # month is in the "wrap" portion (e.g. Jan-Mar for Oct-Mar season)
    return year - 1


# ---------------------------------------------------------------------------
# Observation aggregation
# ---------------------------------------------------------------------------


def aggregate_monthly_obs_to_quarterly(
    monthly_obs: pd.DataFrame,
) -> pd.DataFrame:
    """Aggregate monthly observations to quarterly.

    Args:
        monthly_obs: DataFrame with columns [code, year, month,
            discharge_avg] (and optionally month_in_year, delta).

    Returns:
        DataFrame with columns [code, year, quarter_in_year,
        discharge_avg, delta].
    """
    if monthly_obs.empty:
        return pd.DataFrame(columns=["code", "year", "quarter_in_year", "discharge_avg", "delta"])

    df = monthly_obs.copy()
    df["quarter_in_year"] = df["month"].map(MONTH_TO_QUARTER)

    grouped = (
        df.groupby(["code", "year", "quarter_in_year"])
        .agg(
            discharge_avg=("discharge_avg", "mean"),
            n_months=("discharge_avg", "count"),
        )
        .reset_index()
    )

    # Require >= QUARTER_MIN_MONTHS months present
    grouped = grouped[grouped["n_months"] >= QUARTER_MIN_MONTHS].copy()
    grouped = grouped.drop(columns=["n_months"])

    if grouped.empty:
        return pd.DataFrame(columns=["code", "year", "quarter_in_year", "discharge_avg", "delta"])

    # Compute delta per (code, quarter_in_year): 0.674 * std across years
    delta_df = (
        grouped.groupby(["code", "quarter_in_year"])
        .agg(std_discharge=("discharge_avg", "std"))
        .reset_index()
    )
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)

    grouped = grouped.merge(
        delta_df[["code", "quarter_in_year", "delta"]],
        on=["code", "quarter_in_year"],
        how="left",
    )

    return grouped


def aggregate_monthly_obs_to_seasonal(
    monthly_obs: pd.DataFrame,
) -> pd.DataFrame:
    """Aggregate monthly observations to seasonal.

    Args:
        monthly_obs: DataFrame with columns [code, year, month,
            discharge_avg].

    Returns:
        DataFrame with columns [code, season_year, season_in_year,
        discharge_avg, delta].
    """
    if monthly_obs.empty:
        return pd.DataFrame(
            columns=["code", "season_year", "season_in_year", "discharge_avg", "delta"]
        )

    season_months = get_season_months()
    n_season_months = len(season_months)
    min_months = max(1, int(np.ceil(n_season_months * SEASON_MIN_COVERAGE)))

    df = monthly_obs.copy()
    # Filter to season months only
    df = df[df["month"].isin(season_months)].copy()
    if df.empty:
        return pd.DataFrame(
            columns=["code", "season_year", "season_in_year", "discharge_avg", "delta"]
        )

    df["season_year"] = df.apply(lambda r: get_season_year(int(r["year"]), int(r["month"])), axis=1)

    grouped = (
        df.groupby(["code", "season_year"])
        .agg(
            discharge_avg=("discharge_avg", "mean"),
            n_months=("discharge_avg", "count"),
        )
        .reset_index()
    )

    # Require >= min_months
    grouped = grouped[grouped["n_months"] >= min_months].copy()
    grouped = grouped.drop(columns=["n_months"])

    if grouped.empty:
        return pd.DataFrame(
            columns=["code", "season_year", "season_in_year", "discharge_avg", "delta"]
        )

    grouped["season_in_year"] = 1

    # Delta per code: 0.674 * std across season_years
    delta_df = grouped.groupby(["code"]).agg(std_discharge=("discharge_avg", "std")).reset_index()
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)

    grouped = grouped.merge(delta_df[["code", "delta"]], on=["code"], how="left")

    return grouped


# ---------------------------------------------------------------------------
# Forecast aggregation
# ---------------------------------------------------------------------------

# Quantile columns used in long-term forecasts
_FC_QUANTILE_COLS = ["q05", "q10", "q25", "q50", "q75", "q90", "q95"]


def aggregate_monthly_fc_to_quarterly(
    monthly_fc: pd.DataFrame,
) -> pd.DataFrame:
    """Aggregate monthly forecasts to quarterly.

    Args:
        monthly_fc: DataFrame with columns [code, year, month,
            model_short, q05-q95] and optionally [forecasted_discharge,
            valid_from, valid_to].

    Returns:
        DataFrame with columns [code, year, quarter_in_year,
        model_short, q05-q95, forecasted_discharge, valid_from,
        valid_to].
    """
    if monthly_fc.empty:
        return pd.DataFrame(
            columns=["code", "year", "quarter_in_year", "model_short"] + _FC_QUANTILE_COLS
        )

    df = monthly_fc.copy()
    df["quarter_in_year"] = df["month"].map(MONTH_TO_QUARTER)

    agg_dict: dict = {
        "n_months": ("month", "count"),
    }
    for qcol in _FC_QUANTILE_COLS:
        if qcol in df.columns:
            agg_dict[qcol] = (qcol, "mean")
    if "forecasted_discharge" in df.columns:
        agg_dict["forecasted_discharge"] = ("forecasted_discharge", "mean")
    if "q" in df.columns:
        agg_dict["q"] = ("q", "mean")

    group_cols = ["code", "year", "quarter_in_year", "model_short"]
    # Under SAPPHIRE_SKILL_LEAD_AWARE, keep distinct monthly leads
    # (horizon_value) as separate quarterly rows instead of averaging
    # them together. The QUARTER_MIN_MONTHS coverage filter below then
    # naturally applies PER LEAD, since n_months is counted within each
    # group. Flag OFF, or horizon_value absent, is unchanged.
    if skill_lead_aware_enabled() and "horizon_value" in df.columns:
        group_cols.append("horizon_value")

    grouped = df.groupby(group_cols).agg(**agg_dict).reset_index()
    count_quantile_crossings(grouped, _FC_QUANTILE_COLS, label="monthly→quarterly")

    # Require >= QUARTER_MIN_MONTHS
    grouped = grouped[grouped["n_months"] >= QUARTER_MIN_MONTHS].copy()
    grouped = grouped.drop(columns=["n_months"])

    if grouped.empty:
        return pd.DataFrame(
            columns=["code", "year", "quarter_in_year", "model_short"] + _FC_QUANTILE_COLS
        )

    # Synthesize valid_from/valid_to from quarter boundaries
    grouped["valid_from"] = grouped.apply(
        lambda r: f"{int(r['year'])}-{QUARTER_MONTHS[int(r['quarter_in_year'])][0]:02d}-01",
        axis=1,
    )
    grouped["valid_to"] = grouped.apply(
        lambda r: _quarter_end_date(int(r["year"]), int(r["quarter_in_year"])),
        axis=1,
    )

    # Under SAPPHIRE_SKILL_LEAD_AWARE, carry a representative issue `date`
    # = valid_from - horizon_value months. This is the issue date a
    # lead-`hv` forecast for the quarter's first month would carry, so a
    # lead-aware round-trip read derives EXACTLY `hv` from (date,
    # valid_from) -- rather than the writer fabricating date=valid_from
    # (lead 0) that contradicts the per-lead horizon_value. Deterministic
    # regardless of which constituent months are present (NOT min() of
    # constituent dates, which is off-by-one when the first quarter month
    # is absent). Flag OFF, or horizon_value absent: no `date` column
    # added (byte-identical to today). (FIX 6)
    if skill_lead_aware_enabled() and "horizon_value" in grouped.columns:
        grouped["date"] = grouped.apply(
            lambda r: (
                pd.Timestamp(r["valid_from"]) - pd.DateOffset(months=int(r["horizon_value"]))
            ).strftime("%Y-%m-%d"),
            axis=1,
        )

    # Ensure forecasted_discharge exists (q first, q50 fallback)
    if "forecasted_discharge" not in grouped.columns:
        if "q" in grouped.columns:
            grouped["forecasted_discharge"] = pd.to_numeric(grouped["q"], errors="coerce")
        elif "q50" in grouped.columns:
            grouped["forecasted_discharge"] = grouped["q50"].astype(float)

    return grouped


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

import calendar


def _quarter_end_date(year: int, quarter: int) -> str:
    """Last day of the quarter as YYYY-MM-DD string."""
    last_month = QUARTER_MONTHS[quarter][-1]
    last_day = calendar.monthrange(year, last_month)[1]
    return f"{year}-{last_month:02d}-{last_day:02d}"


def _season_start_date(season_year: int) -> str:
    """First day of the season as YYYY-MM-DD string."""
    season_months = get_season_months()
    start_month = season_months[0]
    return f"{season_year}-{start_month:02d}-01"


def _season_end_date(season_year: int) -> str:
    """Last day of the season as YYYY-MM-DD string."""
    season_months = get_season_months()
    end_month = season_months[-1]
    start_month = season_months[0]

    # Determine the calendar year of the end month
    if end_month >= start_month:
        end_year = season_year
    else:
        end_year = season_year + 1

    last_day = calendar.monthrange(end_year, end_month)[1]
    return f"{end_year}-{end_month:02d}-{last_day:02d}"
