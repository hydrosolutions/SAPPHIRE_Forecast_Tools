"""Resolve long-term point forecasts without changing quantile columns."""

import pandas as pd


def _resolve_forecasted_discharge(df: pd.DataFrame) -> pd.Series | None:
    """Resolve the point forecast column from q or q50.

    Priority: q first (authoritative point forecast from the model),
    q50 as fallback (median quantile, rarely populated).

    Returns None if neither column is available.
    """
    has_q = "q" in df.columns
    has_q50 = "q50" in df.columns
    if has_q and has_q50:
        return df["q"].fillna(df["q50"]).astype(float)
    elif has_q:
        return df["q"].astype(float)
    elif has_q50:
        return df["q50"].astype(float)
    return None


def fill_forecasted_discharge(df: pd.DataFrame) -> pd.DataFrame:
    """Copy and fill missing derived points from q, with per-row q50 fallback."""
    result = df.copy()
    if "forecasted_discharge" in result.columns:
        missing = result["forecasted_discharge"].isna()
        if not missing.any():
            return result
        point = _resolve_forecasted_discharge(result.loc[missing])
        if point is not None:
            result.loc[missing, "forecasted_discharge"] = point
    else:
        point = _resolve_forecasted_discharge(result)
        if point is not None:
            result["forecasted_discharge"] = point
    return result
