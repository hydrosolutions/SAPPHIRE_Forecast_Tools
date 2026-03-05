"""Standalone API data fetcher for forecast dashboard dev tools.

Reimplements the clean read pattern from db.py without pulling in Panel,
iEasyHydroForecast, or any dashboard-specific imports.

Usage:
    from dev_code.fetch_data import fetch_forecasts, fetch_skill_metrics
    df = fetch_forecasts("http://localhost:8000/api", "15102", "pentad",
                         "2025-06-01", "2026-03-01")
"""

import contextlib

import numpy as np
import pandas as pd
import requests

DEFAULT_API_BASE = "http://localhost:8000/api"
DEFAULT_TIMEOUT = 30

# Expected gap between consecutive forecasts (pentad ≈ 5 days, decade ≈ 10)
_GAP_THRESHOLD = {"pentad": pd.Timedelta(days=7), "decade": pd.Timedelta(days=14)}


def insert_gap_nans(
    df: pd.DataFrame,
    date_col: str = "date",
    value_cols: list[str] | None = None,
    horizon: str = "pentad",
) -> pd.DataFrame:
    """Insert NaN rows where date gaps exceed the expected step size.

    This prevents matplotlib from drawing straight lines across missing
    periods.  The returned DataFrame has the same columns as the input;
    gap rows contain NaN for every numeric / value column.

    Args:
        df: Must be sorted by *date_col* within each group.
        date_col: Name of the datetime column.
        value_cols: Columns to set to NaN in gap rows.  If ``None``, all
            numeric columns are used.
        horizon: ``"pentad"`` or ``"decade"`` — controls the gap threshold.
    """
    if df.empty or len(df) < 2:
        return df

    threshold = _GAP_THRESHOLD.get(horizon, pd.Timedelta(days=7))
    dates = df[date_col]
    gaps = dates.diff() > threshold

    if not gaps.any():
        return df

    if value_cols is None:
        value_cols = df.select_dtypes(include="number").columns.tolist()

    gap_rows = []
    for idx in df.index[gaps]:
        row = {c: np.nan for c in df.columns}
        # Place the gap marker one day after the previous point
        prev_idx = df.index[df.index.get_loc(idx) - 1]
        row[date_col] = df.loc[prev_idx, date_col] + pd.Timedelta(days=1)
        gap_rows.append(row)

    if not gap_rows:
        return df

    gap_df = pd.DataFrame(gap_rows, columns=df.columns)
    # Match dtypes where possible; skip integer columns (can't hold NaN)
    for col in gap_df.columns:
        src_dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(src_dtype):
            continue
        with contextlib.suppress(ValueError, TypeError):
            gap_df[col] = gap_df[col].astype(src_dtype)
    return pd.concat([df, gap_df], ignore_index=True).sort_values(date_col)


def _read_data(
    api_base: str,
    service_type: str,
    data_type: str,
    params: dict,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Fetch data from the SAPPHIRE API and return a DataFrame."""
    url = f"{api_base}/{service_type}/{data_type}/"
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    df = pd.DataFrame(response.json())
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    if "target" in df.columns:
        df["target"] = pd.to_datetime(df["target"])
    return df


def fetch_forecasts(
    api_base: str,
    station: str,
    horizon: str,
    start_date: str,
    end_date: str,
    models: list[str] | None = None,
) -> pd.DataFrame:
    """Fetch ML forecasts from the postprocessing API.

    Args:
        api_base: API base URL (e.g. "http://localhost:8000/api")
        station: Station code
        horizon: "pentad" or "decade"
        start_date: ISO date string
        end_date: ISO date string
        models: Optional list of model_type to filter

    Returns:
        DataFrame with renamed columns (model_short, E[Q], Q5, etc.)
    """
    df = _read_data(
        api_base,
        "postprocessing",
        "forecast",
        {
            "horizon": horizon,
            "code": station,
            "start_date": start_date,
            "end_date": end_date,
            "limit": 10000,
        },
    )
    df.rename(
        columns={
            "date": "forecast_date",
            "target": "date",
            "model_type": "model_short",
            "model_type_description": "model_long",
            "q05": "Q5",
            "q25": "Q25",
            "q75": "Q75",
            "q95": "Q95",
            "forecasted_discharge": "E[Q]",
        },
        inplace=True,
    )
    df.drop(
        columns=["horizon_type", "horizon_value", "q50", "id"],
        inplace=True,
        errors="ignore",
    )
    if models:
        df = df[df["model_short"].isin(models)]
    return df


def fetch_lr_forecasts(
    api_base: str,
    station: str,
    horizon: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Fetch linear regression forecasts from the postprocessing API.

    The ``lr-forecast`` endpoint stores the boundary day as ``date``
    (the last day of the previous period).  This function computes a
    ``target`` column (boundary + 1 day = first day of forecast period)
    and renames columns so the result is compatible with the ML forecast
    DataFrame returned by :func:`fetch_forecasts`.
    """
    df = _read_data(
        api_base,
        "postprocessing",
        "lr-forecast",
        {
            "horizon": horizon,
            "code": station,
            "start_date": start_date,
            "end_date": end_date,
            "limit": 10000,
        },
    )
    if df.empty:
        return df

    # Compute target date (first day of forecast period)
    df["date"] = pd.to_datetime(df["date"])
    df["target"] = df["date"] + pd.Timedelta(days=1)

    # Rename to match the fetch_forecasts() schema
    df.rename(
        columns={
            "date": "forecast_date",
            "target": "date",
            "forecasted_discharge": "E[Q]",
        },
        inplace=True,
    )
    df["model_short"] = "LR"
    df["model_long"] = "Linear Regression"

    df.drop(
        columns=["horizon_type", "horizon_value", "horizon_in_year", "id"],
        inplace=True,
        errors="ignore",
    )
    return df


def fetch_skill_metrics(
    api_base: str,
    station: str,
    horizon: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Fetch skill metrics from the postprocessing API."""
    hin = "decad_in_year" if horizon == "decade" else "pentad_in_year"
    df = _read_data(
        api_base,
        "postprocessing",
        "skill-metric",
        {
            "horizon": horizon,
            "code": station,
            "start_date": start_date,
            "end_date": end_date,
            "limit": 10000,
        },
    )
    df.rename(
        columns={
            "horizon_in_year": hin,
            "model_type": "model_short",
            "model_type_description": "model_long",
        },
        inplace=True,
    )
    df.drop(
        columns=["horizon_type", "date", "id"],
        inplace=True,
        errors="ignore",
    )
    return df
