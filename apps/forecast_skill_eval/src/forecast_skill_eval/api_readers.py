from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Final, Literal

import numpy as np
import pandas as pd

from forecast_skill_eval.periods import (
    SHORT_TERM_HORIZONS,
    long_term_calendar_period,
    normalize_horizon,
)

try:
    from sapphire_api_client import SapphirePostprocessingClient, SapphirePreprocessingClient

    SAPPHIRE_API_AVAILABLE = True
except ImportError:
    SAPPHIRE_API_AVAILABLE = False
    SapphirePostprocessingClient = None
    SapphirePreprocessingClient = None

DEFAULT_PAGE_SIZE = 500
ForecastType = Literal["short", "long"]

QUANTILE_LEVELS: Final = (0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95)

# Per forecast_type: canonical level → source column name.
# NOTE: maps SHORT vs LONG columns; does NOT gate LR — LR also reads as "short"
# and is excluded by band-presence (Design Decision 5).
QUANTILE_SOURCE_MAP: Final[dict[ForecastType, dict[float, str]]] = {
    "short": {
        0.05: "q05",
        0.25: "q25",
        0.50: "forecasted_discharge",
        0.75: "q75",
        0.95: "q95",
    },
    "long": {
        0.05: "q05",
        0.10: "q10",
        0.25: "q25",
        0.50: "q50",
        0.75: "q75",
        0.90: "q90",
        0.95: "q95",
    },
}


@dataclass(frozen=True)
class ReaderResult:
    """Read result plus metadata surfaced by the reader."""

    data: pd.DataFrame
    dropped_sentinels: int = 0


def read_forecasts(
    client: Any,
    *,
    horizon: str,
    code: str | None,
    model: str | None,
    target: str | None,
    start_target: str | None,
    end_target: str | None,
    limit: int = DEFAULT_PAGE_SIZE,
) -> ReaderResult:
    """Read short-term forecasts using the non-deprecated API method."""
    normalized_horizon = normalize_horizon(horizon)
    data = _read_all_pages(
        lambda skip, page_limit: client.read_short_term_forecasts(
            horizon=normalized_horizon,
            code=code,
            model=model,
            target=target,
            start_target=start_target,
            end_target=end_target,
            skip=skip,
            limit=page_limit,
        ),
        limit=limit,
    )
    data, dropped_sentinels = _drop_short_term_sentinels(data, normalized_horizon)
    enriched = _add_point_values(data, forecast_type="short")
    enriched = _add_quantile_band(enriched, forecast_type="short")
    return ReaderResult(enriched, dropped_sentinels)


def read_lr_forecasts(
    client: Any,
    *,
    horizon: str,
    code: str | None,
    start_date: str | None,
    end_date: str | None,
    limit: int = DEFAULT_PAGE_SIZE,
) -> ReaderResult:
    """Read LR forecasts and normalize them for the short-term pair path.

    Verified mapping: horizon_in_year is target-indexed (override at
    linear_regression.py:925-933); date is the issue date; target := date+1
    recovers the target year only; period_key derives from horizon_in_year,
    never from date+1.
    """
    normalized_horizon = normalize_horizon(horizon)
    data = _read_all_pages(
        lambda skip, page_limit: client.read_lr_forecasts(
            horizon=normalized_horizon,
            code=code,
            start_date=start_date,
            end_date=end_date,
            skip=skip,
            limit=page_limit,
        ),
        limit=limit,
    )
    data = _normalize_lr_forecasts(data)
    data, dropped_sentinels = _drop_short_term_sentinels(data, normalized_horizon)
    enriched = _add_point_values(data, forecast_type="short")
    enriched = _add_quantile_band(enriched, forecast_type="short")
    return ReaderResult(enriched, dropped_sentinels)


def read_long_forecasts(
    client: Any,
    *,
    horizon: str,
    code: str | None,
    model: str | None,
    horizon_value: int | None,
    valid_from: str | None,
    valid_to: str | None,
    limit: int = DEFAULT_PAGE_SIZE,
) -> ReaderResult:
    """Read long-term forecasts using the non-deprecated API method."""
    normalized_horizon = normalize_horizon(horizon)
    data = _read_all_pages(
        lambda skip, page_limit: client.read_long_term_forecasts(
            horizon_type=normalized_horizon,
            code=code,
            model=model,
            horizon_value=horizon_value,
            valid_from=valid_from,
            valid_to=valid_to,
            skip=skip,
            limit=page_limit,
        ),
        limit=limit,
    )
    data = _add_long_calendar_periods(data, normalized_horizon)
    enriched = _add_point_values(data, forecast_type="long")
    enriched = _add_quantile_band(enriched, forecast_type="long")
    return ReaderResult(enriched)


def read_hydrograph_norms(
    client: Any,
    *,
    horizon: str,
    code: str | None,
    start_date: str | None,
    end_date: str | None,
    limit: int = DEFAULT_PAGE_SIZE,
) -> ReaderResult:
    """Read hydrograph norms using the preprocessing API client."""
    normalized_horizon = normalize_horizon(horizon)
    data = _read_all_pages(
        lambda skip, page_limit: client.read_hydrograph(
            horizon=normalized_horizon,
            code=code,
            start_date=start_date,
            end_date=end_date,
            skip=skip,
            limit=page_limit,
        ),
        limit=limit,
    )
    return ReaderResult(data)


def read_runoff_observed(
    client: Any,
    *,
    horizon: str,
    code: str | None,
    start_date: str | None,
    end_date: str | None,
    limit: int = DEFAULT_PAGE_SIZE,
) -> ReaderResult:
    """Read observed runoff using the preprocessing API client."""
    normalized_horizon = normalize_horizon(horizon)
    data = _read_all_pages(
        lambda skip, page_limit: client.read_runoff(
            horizon=normalized_horizon,
            code=code,
            start_date=start_date,
            end_date=end_date,
            skip=skip,
            limit=page_limit,
        ),
        limit=limit,
    )
    return ReaderResult(data)


def select_point_value(row: Mapping[str, Any], forecast_type: ForecastType) -> tuple[float, str]:
    """Select the deterministic point-value column for a forecast row."""
    if forecast_type == "short":
        candidates = ("forecasted_discharge",)
        missing_note = "No point value found in forecasted_discharge"
    elif forecast_type == "long":
        candidates = ("q", "q50", "q_loc")
        missing_note = "No point value found in q, q50, or q_loc"
    else:
        raise ValueError(f"Unsupported forecast_type: {forecast_type}")

    for column in candidates:
        value = row.get(column)
        if not pd.isna(value):
            return float(value), ""
    return float(np.nan), missing_note


def select_quantile_band(
    row: Mapping[str, Any], forecast_type: ForecastType
) -> tuple[dict[float, float], str, str]:
    """Return ({level: value} for finite source quantiles, note, grid_id).

    Missing/NaN nodes are dropped.  Rows with <2 finite nodes return
    ({}, 'no_quantile_band', '').  grid_id is 'long7' / 'short5' / '' when
    band-less.
    """
    column_map = QUANTILE_SOURCE_MAP.get(forecast_type, {})
    band: dict[float, float] = {}
    for level, col in column_map.items():
        val = row.get(col)
        if val is None:
            continue
        try:
            fval = float(val)
            if np.isfinite(fval):
                band[level] = fval
        except (TypeError, ValueError):
            pass
    if len(band) < 2:
        return {}, "no_quantile_band", ""
    grid_id = "long7" if forecast_type == "long" else "short5"
    return band, "", grid_id


def _read_all_pages(
    read_page: Callable[[int, int], pd.DataFrame],
    *,
    limit: int,
) -> pd.DataFrame:
    if limit < 1:
        raise ValueError("limit must be at least 1")

    pages: list[pd.DataFrame] = []
    skip = 0
    while True:
        page = _as_dataframe(read_page(skip, limit))
        if page.empty:
            break
        pages.append(page)
        if len(page) < limit:
            break
        skip += limit

    if not pages:
        return pd.DataFrame()
    return pd.concat(pages, ignore_index=True)


def _as_dataframe(value: Any) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value.copy()
    return pd.DataFrame(value)


def _drop_short_term_sentinels(data: pd.DataFrame, horizon: str) -> tuple[pd.DataFrame, int]:
    if data.empty or horizon not in SHORT_TERM_HORIZONS or "horizon_in_year" not in data.columns:
        return data.reset_index(drop=True), 0

    sentinel_mask = pd.to_numeric(data["horizon_in_year"], errors="coerce").eq(0)
    dropped_count = int(sentinel_mask.sum())
    filtered = data.loc[~sentinel_mask].reset_index(drop=True)
    return filtered, dropped_count


def _normalize_lr_forecasts(data: pd.DataFrame) -> pd.DataFrame:
    normalized = data.copy()
    normalized["model"] = "LR"
    if "date" in normalized.columns:
        issue_dates = pd.to_datetime(normalized["date"], errors="coerce")
        normalized["target"] = issue_dates + pd.Timedelta(days=1)
    else:
        normalized["target"] = pd.Series(pd.NaT, index=normalized.index, dtype="datetime64[ns]")
    return normalized


def _add_point_values(data: pd.DataFrame, forecast_type: ForecastType) -> pd.DataFrame:
    enriched = data.copy()
    if enriched.empty:
        enriched["point_value"] = pd.Series(dtype="float64")
        enriched["point_value_note"] = pd.Series(dtype="object")
        return enriched

    selections = [select_point_value(row, forecast_type) for row in enriched.to_dict("records")]
    enriched["point_value"] = [selection[0] for selection in selections]
    enriched["point_value_note"] = [selection[1] for selection in selections]
    return enriched


def _add_quantile_band(data: pd.DataFrame, forecast_type: ForecastType) -> pd.DataFrame:
    """Add 'quantiles' ({level:value}), 'quantiles_note', and 'fc_grid_id' columns.

    Empty frame → typed empty columns.  Additive: does not touch
    point_value / point_value_note / any existing column.
    """
    enriched = data.copy()
    if enriched.empty:
        enriched["quantiles"] = pd.Series(dtype="object")
        enriched["quantiles_note"] = pd.Series(dtype="object")
        enriched["fc_grid_id"] = pd.Series(dtype="object")
        return enriched
    selections = [select_quantile_band(row, forecast_type) for row in enriched.to_dict("records")]
    enriched["quantiles"] = [sel[0] for sel in selections]
    enriched["quantiles_note"] = [sel[1] for sel in selections]
    enriched["fc_grid_id"] = [sel[2] for sel in selections]
    return enriched


def _add_long_calendar_periods(data: pd.DataFrame, horizon: str) -> pd.DataFrame:
    enriched = data.copy()
    if enriched.empty:
        enriched["calendar_period"] = pd.Series(dtype="Int64")
        enriched["is_calendar_aligned"] = pd.Series(dtype="bool")
        return enriched
    if "valid_from" not in enriched.columns or "valid_to" not in enriched.columns:
        return enriched

    periods = [
        long_term_calendar_period(horizon, row["valid_from"], row["valid_to"])
        for row in enriched.to_dict("records")
    ]
    enriched["calendar_period"] = [period[0] for period in periods]
    enriched["is_calendar_aligned"] = [period[1] for period in periods]
    return enriched
