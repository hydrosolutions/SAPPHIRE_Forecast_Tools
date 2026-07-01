"""Pure data-loading and filtering helpers for the skill-eval dashboard.

No Streamlit import — all functions are unit-testable with plain pandas.
"""

from __future__ import annotations

import math
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Column helpers
# ---------------------------------------------------------------------------

_METRIC_UNDEFINED_MAP: dict[str, str] = {
    "pod": "pod_undefined",
    "far": "far_undefined",
    "pofd": "pofd_undefined",
    "csi": "csi_undefined",
    "frequency_bias": "frequency_bias_undefined",
    "hss": "hss_undefined",
    "pss": "pss_undefined",
    "pod_ci_lower": "pod_ci_undefined",
    "pod_ci_upper": "pod_ci_undefined",
    "far_ci_lower": "far_ci_undefined",
    "far_ci_upper": "far_ci_undefined",
    "base_rate": "base_rate_undefined",
}


def _is_undefined(df: pd.DataFrame, metric: str) -> pd.Series:
    """Return a boolean Series: True where *metric* is undefined or NaN."""
    undef_col = _METRIC_UNDEFINED_MAP.get(metric)
    nan_mask = df[metric].isna()
    if undef_col is not None and undef_col in df.columns:
        return nan_mask | df[undef_col].astype(bool)
    return nan_mask


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_metrics(path: str | Path) -> pd.DataFrame:
    """Read *path* (CSV) and return a normalised DataFrame.

    If the ``event`` column is absent (pre-feature CSVs) it is synthesised as
    ``"below_norm"`` for every row so downstream code is uniform.

    Args:
        path: Absolute or relative path to the contingency_metrics.csv file.

    Returns:
        Normalised DataFrame with all expected columns present.
    """
    df = pd.read_csv(path)
    if "event" not in df.columns:
        df["event"] = "below_norm"

    # Ensure lead is stored as float so NaN comparisons work uniformly.
    df["lead"] = pd.to_numeric(df["lead"], errors="coerce")

    return df


def filter_metrics(
    df: pd.DataFrame,
    *,
    horizon: str,
    event: str,
    season: str,
    regime: str,
    norm_provenance: str,
    model: list[str] | str | None = None,
    lead: float | int | None = None,
) -> pd.DataFrame:
    """Apply equality filters and return the matching subset.

    Args:
        df: DataFrame produced by :func:`load_metrics`.
        horizon: Value to match in the ``horizon`` column.
        event: Value to match in the ``event`` column.
        season: Value to match in the ``season`` column.
        regime: Value to match in the ``regime`` column.
        norm_provenance: Value to match in the ``norm_provenance`` column.
        model: One model name (str), a list of model names, or None for all.
        lead: Integer lead to match, or None (matches NaN rows — short-term).

    Returns:
        Filtered DataFrame; empty if no rows match.
    """
    mask = (
        (df["horizon"] == horizon)
        & (df["event"] == event)
        & (df["season"] == season)
        & (df["regime"] == regime)
        & (df["norm_provenance"] == norm_provenance)
    )

    # Lead: None → match NaN rows (short-term); integer → exact match.
    if lead is None:
        mask &= df["lead"].isna()
    else:
        mask &= df["lead"] == float(lead)

    if model is not None:
        models = [model] if isinstance(model, str) else list(model)
        mask &= df["model"].isin(models)

    return df[mask].copy()


def per_station(df: pd.DataFrame) -> pd.DataFrame:
    """Return rows where ``code != "POOLED"`` (individual station rows).

    Args:
        df: Any filtered or unfiltered metrics DataFrame.

    Returns:
        Subset with only per-station rows.
    """
    return df[df["code"] != "POOLED"].copy()


def pooled_row(df: pd.DataFrame) -> pd.Series | None:
    """Return the single POOLED aggregate row, or None if absent.

    Args:
        df: Any filtered metrics DataFrame.

    Returns:
        A pandas Series for the ``code == "POOLED"`` row, or None.
    """
    pool = df[df["code"] == "POOLED"]
    if pool.empty:
        return None
    # Take the first row if multiple exist (e.g. multiple basins).
    return pool.iloc[0]


def rank_stations(
    df: pd.DataFrame,
    metric: str,
    *,
    ascending: bool = False,
) -> pd.DataFrame:
    """Sort per-station rows by *metric*, dropping undefined/NaN rows.

    Args:
        df: DataFrame that may include both per-station and POOLED rows.
        metric: Column name to rank by.
        ascending: Sort direction (False → highest metric first).

    Returns:
        Sorted DataFrame with undefined/NaN metric rows removed.
    """
    station_df = per_station(df).copy()
    undef_mask = _is_undefined(station_df, metric)
    ranked = station_df[~undef_mask].copy()
    return ranked.sort_values(metric, ascending=ascending)


def distinct_values(df: pd.DataFrame, column: str) -> list:
    """Return sorted unique non-null values in *column*.

    Args:
        df: Any metrics DataFrame.
        column: Column to inspect.

    Returns:
        Sorted list of unique values (suitable for populating filter widgets).
    """
    vals = df[column].dropna().unique().tolist()
    try:
        return sorted(vals)
    except TypeError:
        # Mixed types that cannot be compared — return as-is.
        return vals


def available_options(df: pd.DataFrame, column: str, selections: dict[str, object]) -> list:
    """Distinct sorted values of *column* in df filtered by upstream selections.

    For each ``(k, v)`` in *selections*: if ``k == column`` or ``v is None``
    the entry is skipped; otherwise rows where ``df[k] != v`` are dropped.
    This guarantees that every value returned for *column* corresponds to at
    least one real row given the choices already made — enabling cascading
    sidebar widgets where no combination can yield an empty table.

    Args:
        df: Full (unfiltered) metrics DataFrame produced by :func:`load_metrics`.
        column: The column whose valid options are to be returned.
        selections: Mapping of column name → currently selected value for every
            upstream filter widget.  ``None`` values are treated as "no
            constraint" and are skipped.

    Returns:
        Sorted list of unique non-null values of *column* after applying all
        applicable upstream selections.  Falls back to unsorted on
        ``TypeError`` (mixed types), matching :func:`distinct_values`.
    """
    mask = pd.Series(True, index=df.index)
    for k, v in selections.items():
        if k == column or v is None:
            continue
        mask &= df[k] == v
    vals = df.loc[mask, column].dropna().unique().tolist()
    try:
        return sorted(vals)
    except TypeError:
        return vals


def metric_display_value(value: float, undefined: bool | float) -> str:
    """Format a metric for display, returning 'n/a' when undefined.

    Args:
        value: Raw metric value.
        undefined: Boolean flag (or truthy float) from the ``*_undefined`` col.

    Returns:
        Formatted string or 'n/a'.
    """
    if undefined or (isinstance(value, float) and math.isnan(value)):
        return "n/a"
    return f"{value:.3f}"
