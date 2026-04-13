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

    grouped = (
        df.groupby(["code", "year", "quarter_in_year", "model_short"]).agg(**agg_dict).reset_index()
    )
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
