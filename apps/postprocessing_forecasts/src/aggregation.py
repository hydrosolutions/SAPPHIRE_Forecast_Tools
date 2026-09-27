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
from collections import Counter

import numpy as np
import pandas as pd
from skill_lead_aware_flag import skill_lead_aware_enabled
from src.model_names import canonical_model_short_series
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


_LOCAL_CALENDAR_DATE_LOWER_BOUND = pd.Timestamp("1677-09-22")


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
    but casting such a Timestamp to ns raises ``OutOfBoundsDatetime``.

    A value just above the OTHER end of the range (``pd.Timestamp.min``,
    1677-09-21 00:12:43...) is rejected explicitly, BEFORE
    ``.normalize()``: normalizing such a value truncates its
    time-of-day DOWN to that day's midnight, which is earlier than the
    representable minimum, and pandas does not raise for this -- it
    silently wraps around to a bogus date near the UPPER limit instead
    (observed: 2262-04-11).

    The whole parse is wrapped in a broad ``except Exception`` -- not a
    fixed tuple of expected exception types -- because this function
    must NEVER raise for ANY input: ``pd.Timestamp(v)`` can invoke
    arbitrary methods on an arbitrary object `v` (e.g. its ``__str__``),
    which can raise anything.

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
        if ts < _LOCAL_CALENDAR_DATE_LOWER_BOUND:
            return pd.NaT
        return ts.normalize().as_unit("ns")
    except Exception:
        return pd.NaT


def _local_calendar_date_per_value(s: pd.Series) -> pd.Series:
    """Path (c): parse every value individually, with no de-duplication.

    The fallback every OTHER path in ``local_calendar_date`` reduces to
    when it cannot safely vectorize or dedup: unconditionally correct,
    just not fast.
    """
    parsed = pd.Series([_parse_local_calendar_date(v) for v in s], dtype=object, index=s.index)
    return pd.to_datetime(parsed)


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

    This function must return EXACTLY what applying
    ``_parse_local_calendar_date`` to every value individually would
    return, for every input, in any row order. An earlier version
    de-duplicated by a generic "type + string representation" key; each
    of three review rounds found a NEW way to break that key (a
    same-instant tz-aware value at two different UTC offsets; 0/False
    and 1/True/1.0, all ``==`` in Python but parsed differently;
    ``str()``/``repr()`` collisions between unrelated types; a
    ``__str__`` that raises). Rather than patch a fourth collision
    class, de-duplication here is restricted to the ONE case where it is
    correct BY CONSTRUCTION, not by enumeration:

    - (a) A ``datetime64`` column (naive or tz-aware, any unit) is
      handled fully vectorized: tz-aware is stripped to LOCAL wall time
      via ``.dt.tz_localize(None)`` (never ``tz_convert``, which would
      shift the instant). If the result is exactly ``datetime64[ns]``,
      ``.dt.normalize()`` plus the same lower-bound cutoff as
      ``_parse_local_calendar_date`` reproduces it exactly, with no
      Python-level loop. Any other unit (a non-ns cast could itself
      overflow for an extreme value) falls back to (c).
    - (b) Otherwise, only values that are EXACTLY ``str`` (``type(v) is
      str``, never a subclass or another type that merely looks like a
      date) are de-duplicated, keyed on the string itself. Two equal
      Python strings are, by definition, the same input to
      ``_parse_local_calendar_date`` -- a pure function of its argument
      -- so caching by the string value cannot collide with anything,
      for any other value of any other type. This also means a
      ``Categorical``/``StringDtype`` column is simply iterated (its
      actual per-row values, of whatever type they are), not special-cased.
    - (c) Every other value (``Timestamp``, ``datetime``, ``date``,
      ``np.datetime64``, a number, a bool, ``None``/``NaN``/``NA``/
      ``NaT``, or any other object -- including one that is unhashable,
      e.g. a list) is parsed individually, every time, with no key at
      all.

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

    dtype = s.dtype
    is_tz_aware = isinstance(dtype, pd.DatetimeTZDtype)
    is_naive_datetime = not is_tz_aware and getattr(dtype, "kind", None) == "M"

    if is_tz_aware or is_naive_datetime:
        # Path (a): datetime64 dtype, any unit, naive or tz-aware.
        working = s.dt.tz_localize(None) if is_tz_aware else s
        if working.dtype == "datetime64[ns]":
            # The lower-bound check must run on the ORIGINAL values,
            # BEFORE normalize(): normalize() on a value already close
            # to pd.Timestamp.min can itself silently wrap to a bogus
            # date near the upper limit (the same failure mode
            # _parse_local_calendar_date guards against), so checking
            # the NORMALIZED result here would be too late for exactly
            # that value.
            too_low = working < _LOCAL_CALENDAR_DATE_LOWER_BOUND
            normalized = working.dt.normalize()
            return normalized.where(~too_low, pd.NaT)
        # A non-ns unit: casting to ns to normalize it could itself
        # overflow for an extreme value, so parse per value instead.
        return _local_calendar_date_per_value(working)

    # Path (b)/(c): de-duplicate ONLY exact `str` values, keyed on the
    # string itself. `for v in s` yields the actual per-row values for
    # object, Categorical, and StringDtype columns alike.
    cache: dict[str, pd.Timestamp] = {}
    values = []
    for v in s:
        if type(v) is str:
            parsed = cache.get(v)
            if parsed is None:
                parsed = _parse_local_calendar_date(v)
                cache[v] = parsed
            values.append(parsed)
        else:
            values.append(_parse_local_calendar_date(v))
    return pd.to_datetime(pd.Series(values, dtype=object, index=s.index))


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

# Minimum DISTINCT calendar months required per quarter for observations
# (out of 3; PP-065 item 8). Used only by aggregate_monthly_obs_to_quarterly.
# Unlike QUARTER_MIN_MONTHS (forecasts), this counts distinct months, not
# non-null rows, so a repeated month never inflates coverage.
QUARTER_OBS_MIN_MONTHS = 3

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

    # Count DISTINCT calendar months, not non-null rows: first average per
    # (code, year, quarter, month) -- skipping NaN, as pandas mean() does by
    # default -- so a duplicated month collapses to ONE value before the
    # QUARTER_OBS_MIN_MONTHS coverage check. With the normal one-row-per-month
    # input, this monthly average is a no-op (mean of a single value is that
    # value), so discharge_avg and delta below are unchanged from before this
    # rewrite; only the coverage threshold changed.
    monthly_means = (
        df.groupby(["code", "year", "quarter_in_year", "month"])["discharge_avg"]
        .mean()
        .reset_index()
    )

    grouped = (
        monthly_means.groupby(["code", "year", "quarter_in_year"])
        .agg(
            discharge_avg=("discharge_avg", "mean"),
            n_months=("discharge_avg", "count"),
        )
        .reset_index()
    )

    # Require >= QUARTER_OBS_MIN_MONTHS distinct months present
    grouped = grouped[grouped["n_months"] >= QUARTER_OBS_MIN_MONTHS].copy()
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


# ---------------------------------------------------------------------------
# Quarter derived models (PP-065 P1a)
# ---------------------------------------------------------------------------

# Quarter-start calendar months: Jan, Apr, Jul, Oct.
_QUARTER_START_MONTHS = frozenset({1, 4, 7, 10})


def clamp_issue_day(year: int, month: int, issue_day: int) -> int:
    """Clamp a configured issue day to the length of the given month.

    Same rule as the producer (``apps/long_term_forecasting/lt_utils.py:170-172
    nearest_scheduled_issue_date``) and PP-064's ``data_reader.py:3097-3101``:
    a configured issue day past the end of a short month (e.g. 31 in June)
    is scheduled/matched on that month's last day instead.

    Args:
        year: Calendar year of the month.
        month: Month number (1-12).
        issue_day: Configured issue day (any positive integer).

    Returns:
        ``issue_day``, or the month's last day if ``issue_day`` exceeds it.
    """
    return min(issue_day, calendar.monthrange(year, month)[1])


def _add_months(year: int, month: int, delta: int) -> tuple[int, int]:
    """Year-aware month addition. Returns (year, month) for month + delta."""
    total = (month - 1) + delta
    return year + total // 12, total % 12 + 1


def derive_quarterly_from_monthly_same_issue(
    monthly_raw: pd.DataFrame,
    lead: int,
    issue_day: int,
    models: frozenset[str],
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Derive quarterly forecasts from same-issue monthly triplets.

    For each (code, model, issue date ``d``) whose issue month, offset by
    ``lead`` months (year-aware), lands on a calendar quarter's first month,
    this averages the model's own monthly point forecasts for leads
    ``lead``, ``lead + 1`` and ``lead + 2`` -- all issued on that same date
    ``d`` -- into one quarterly row. This is independent of
    ``SAPPHIRE_SKILL_LEAD_AWARE``: the output is identical under both flag
    states, since the flag only affects how OTHER code groups monthly rows,
    not this derivation.

    Args:
        monthly_raw: Raw monthly rows, after the CALLER has renamed
            ``model_type`` -> ``model_short`` and normalised ``code`` (this
            helper does neither). ``date`` and ``valid_from`` are string-like
            and may mix tz-aware and naive values. ``horizon_value``, ``q``
            and ``q50`` may be absent.
        lead: The configured quarter lead (the target quarter's first
            month's horizon_value).
        issue_day: The configured monthly issue day (unclamped; clamped
            internally per issue month via ``clamp_issue_day``).
        models: Canonical model names to derive (callers pass
            ``QUARTERLY_DERIVED_MODELS`` or ``QUARTER_NATIVE_RAW_MODELS``
            from ``src/model_names.py``).

    Returns:
        Tuple of (derived frame, dict of exclusion counts by reason). The
        frame has a fixed column set: ``code``, ``model_short``, ``year``,
        ``quarter_in_year``, ``date``, ``horizon_value``, ``valid_from``,
        ``valid_to``, ``forecasted_discharge``, ``q`` (only if the input
        had a ``q`` column), and every column of ``_FC_QUANTILE_COLS`` (all
        NaN). Rows are never copied from the input -- every output column
        is built fresh, so input-only columns (e.g. ``id``, ``flag``,
        ``composition``, ``q_obs``, ``model_type_description``,
        ``horizon_type``) never leak into the output. The counts dict is a
        ``Counter``: a missing key reads as 0.
    """
    has_q = "q" in monthly_raw.columns
    counts: Counter = Counter()

    def output_columns() -> list:
        cols = [
            "code",
            "model_short",
            "year",
            "quarter_in_year",
            "date",
            "horizon_value",
            "valid_from",
            "valid_to",
            "forecasted_discharge",
        ]
        if has_q:
            cols.append("q")
        cols += list(_FC_QUANTILE_COLS)
        return cols

    def empty_result() -> pd.DataFrame:
        return pd.DataFrame(columns=output_columns())

    def log_counts() -> None:
        for key, n in counts.items():
            level = (
                logging.WARNING
                if key in ("invalid_config", "ambiguous_duplicate")
                else logging.INFO
            )
            logger.log(
                level,
                "derive_quarterly_from_monthly_same_issue: %s=%d (lead=%s, issue_day=%s)",
                key,
                n,
                lead,
                issue_day,
            )

    # Invalid config: no exception, empty schema, ONE warning.
    if issue_day < 1 or lead < 0:
        counts["invalid_config"] += 1
        log_counts()
        return empty_result(), counts

    missing_required = [c for c in ("code", "model_short", "date") if c not in monthly_raw.columns]
    if missing_required:
        for c in missing_required:
            counts[f"missing_column:{c}"] += 1
        log_counts()
        return empty_result(), counts

    if "horizon_value" not in monthly_raw.columns:
        counts["missing_column:horizon_value"] += 1
        log_counts()
        return empty_result(), counts

    df = monthly_raw.copy()
    canon_model = canonical_model_short_series(df["model_short"])
    in_scope = canon_model.isin(models)
    df = df.loc[in_scope].copy()
    if df.empty:
        log_counts()
        return empty_result(), counts
    df["_canon_model"] = canon_model.loc[in_scope]

    # Parse the issue date via local_calendar_date (NOT
    # pd.to_datetime(format="mixed"), which raises on mixed tz-aware/naive
    # strings and would shift the local date under utc=True).
    df["_d"] = local_calendar_date(df["date"])
    bad_date = df["_d"].isna()
    n_bad_date = int(bad_date.sum())
    if n_bad_date:
        counts["bad_date"] = n_bad_date
    df = df.loc[~bad_date].copy()
    if df.empty:
        log_counts()
        return empty_result(), counts

    # Quarter-start scope filter: (d.month + lead), year-aware, must land on
    # a quarter-start month. Out of scope, routine -- never counted.
    target_years, target_months = [], []
    for ts in df["_d"]:
        ty, tm = _add_months(ts.year, ts.month, lead)
        target_years.append(ty)
        target_months.append(tm)
    df["_target_year"] = target_years
    df["_target_month"] = target_months
    df = df.loc[df["_target_month"].isin(_QUARTER_START_MONTHS)].copy()
    if df.empty:
        log_counts()
        return empty_result(), counts

    # Issue-day check, clamped to the issue month's length (counted).
    df["_expected_day"] = [clamp_issue_day(ts.year, ts.month, issue_day) for ts in df["_d"]]
    wrong_day = df["_d"].dt.day != df["_expected_day"]
    n_wrong_day = int(wrong_day.sum())
    if n_wrong_day:
        counts["wrong_issue_day"] = n_wrong_day
    df = df.loc[~wrong_day].copy()
    if df.empty:
        log_counts()
        return empty_result(), counts

    # horizon_value validity: finite and integer-valued only, cast to int
    # only AFTER the filter (real API frames carry hv as float64 with NaN).
    hv_numeric = pd.to_numeric(df["horizon_value"], errors="coerce")
    valid_hv = hv_numeric.notna() & np.isfinite(hv_numeric) & hv_numeric.eq(np.round(hv_numeric))
    n_bad_hv = int((~valid_hv).sum())
    if n_bad_hv:
        counts["bad_horizon_value"] = n_bad_hv
    df = df.loc[valid_hv].copy()
    if df.empty:
        log_counts()
        return empty_result(), counts
    df["_hv"] = hv_numeric.loc[valid_hv].round().astype(int)

    # hv outside {lead, lead+1, lead+2}: out of scope, routine -- not counted.
    leads_needed = (lead, lead + 1, lead + 2)
    df = df.loc[df["_hv"].isin(leads_needed)].copy()
    if df.empty:
        log_counts()
        return empty_result(), counts

    # Point value per row: q if present and finite, else q50. Both coerced
    # numeric; a row with neither finite becomes NaN (excluded downstream
    # as non_finite_value).
    q_val = (
        pd.to_numeric(df["q"], errors="coerce")
        if "q" in df.columns
        else pd.Series(np.nan, index=df.index)
    )
    q50_val = (
        pd.to_numeric(df["q50"], errors="coerce")
        if "q50" in df.columns
        else pd.Series(np.nan, index=df.index)
    )
    df["_point_value"] = q_val.where(np.isfinite(q_val), q50_val)

    # Exact duplicates (a repeated read, not an ambiguity): drop BEFORE the
    # uniqueness rule, keyed on `id` when present, else on the full tuple.
    if "id" in df.columns:
        df = df.drop_duplicates(subset=["id"]).copy()
    else:
        dedup_cols = ["code", "_canon_model", "_d", "_hv"]
        dedup_cols += [c for c in ("valid_from", "valid_to") if c in df.columns]
        df = df.drop_duplicates(subset=dedup_cols).copy()

    # Per-row target (year, month) for the uniqueness rule: d + hv months
    # (this row's own target month), NOT the triplet's quarter-start target.
    row_target_years, row_target_months = [], []
    for ts, hv in zip(df["_d"], df["_hv"], strict=True):
        ry, rm = _add_months(ts.year, ts.month, int(hv))
        row_target_years.append(ry)
        row_target_months.append(rm)
    df["_row_target_year"] = row_target_years
    df["_row_target_month"] = row_target_months

    has_valid_from_col = "valid_from" in df.columns
    if has_valid_from_col:
        vf = local_calendar_date(df["valid_from"])
        df["_vf_year"] = vf.dt.year
        df["_vf_month"] = vf.dt.month

    # Resolve each (code, canonical model, d, hv) group to at most one
    # winning row. A singleton wins regardless of its valid_from. In a
    # group of 2+, the unique row whose valid_from (year, month) equals the
    # target wins; zero or >= 2 matches marks the WHOLE triplet ambiguous.
    winners = []
    ambiguous_triplets = set()
    for key, group in df.groupby(["code", "_canon_model", "_d", "_hv"], sort=False):
        triplet_key = key[:3]
        if len(group) == 1:
            winners.append(group.iloc[0])
            continue
        if not has_valid_from_col:
            ambiguous_triplets.add(triplet_key)
            continue
        target_y = group["_row_target_year"].iloc[0]
        target_m = group["_row_target_month"].iloc[0]
        match_mask = (
            group["_vf_year"].notna()
            & group["_vf_year"].eq(target_y)
            & group["_vf_month"].eq(target_m)
        )
        matches = group.loc[match_mask]
        if len(matches) == 1:
            winners.append(matches.iloc[0])
        else:
            ambiguous_triplets.add(triplet_key)

    if ambiguous_triplets:
        counts["ambiguous_duplicate"] = len(ambiguous_triplets)

    if not winners:
        log_counts()
        return empty_result(), counts

    winners_df = pd.DataFrame(winners)
    winners_df["_triplet_key"] = list(
        zip(winners_df["code"], winners_df["_canon_model"], winners_df["_d"], strict=True)
    )
    winners_df = winners_df.loc[~winners_df["_triplet_key"].isin(ambiguous_triplets)].copy()

    rows_out = []
    for _triplet_key, group in winners_df.groupby("_triplet_key", sort=False):
        hv_present = set(group["_hv"])
        if hv_present != set(leads_needed):
            counts["missing_lead"] += 1
            continue

        point_values = {
            hv: group.loc[group["_hv"] == hv, "_point_value"].iloc[0] for hv in leads_needed
        }
        if not all(np.isfinite(v) for v in point_values.values()):
            counts["non_finite_value"] += 1
            continue

        lead_row = group.loc[group["_hv"] == lead].iloc[0]
        year = int(lead_row["_target_year"])
        quarter_in_year = MONTH_TO_QUARTER[int(lead_row["_target_month"])]
        forecasted_discharge = float(np.mean(list(point_values.values())))

        row = {
            "code": lead_row["code"],
            "model_short": lead_row["model_short"],
            "year": year,
            "quarter_in_year": quarter_in_year,
            "date": lead_row["_d"].strftime("%Y-%m-%d"),
            "horizon_value": lead,
            "valid_from": f"{year}-{QUARTER_MONTHS[quarter_in_year][0]:02d}-01",
            "valid_to": _quarter_end_date(year, quarter_in_year),
            "forecasted_discharge": forecasted_discharge,
        }
        if has_q:
            row["q"] = forecasted_discharge
        for qcol in _FC_QUANTILE_COLS:
            row[qcol] = np.nan
        rows_out.append(row)

    log_counts()
    if not rows_out:
        return empty_result(), counts
    return pd.DataFrame(rows_out, columns=output_columns()), counts
