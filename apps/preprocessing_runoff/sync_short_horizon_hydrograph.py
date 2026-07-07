"""Pentad and decad short-horizon runoff hydrograph ingestion.

Builds one pentad (72/year) and one decad (36/year) hydrograph row per
station/period with the full envelope
(``mean``/``min``/``max``/``q05``/``q25``/``q75``/``q95``), ``norm``, and the
``current``/``previous`` actuals triad.

The envelope and ``norm`` are reproduced by the SAME climatology method as the
legacy ``forecast_library.write_pentad_hydrograph_data`` /
``write_decad_hydrograph_data`` writers (byte-identical), by reusing the real
legacy helpers (``add_pentad_issue_date`` / ``calculate_pentadaldischargeavg``
and their decad equivalents) on a continuous multi-year daily series. Only the
actuals (``current``/``previous``) are new: SDK-first (WDFA/WDDCA), with a
WDDA (daily-mean) fallback gated on >= 80% calendar-day coverage of the
period, and no finalized actual for a period that is still in progress.
"""

from __future__ import annotations

import argparse
import calendar
import datetime as dt
import logging
import math
import os
import sys
from collections.abc import Iterable
from typing import Any

import numpy as np
import pandas as pd

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_IEHF_DIR = os.path.join(_SCRIPT_DIR, "..", "iEasyHydroForecast")
if _IEHF_DIR not in sys.path:
    sys.path.insert(0, _IEHF_DIR)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

import forecast_library as fl
import setup_library as sl
import tag_library as tl
from ieasyhydro_sdk.sdk import IEasyHydroHFSDK

# Shared API-write plumbing, station resolution, and JSON-safety helper are
# mirrored/reused from the long-horizon builder rather than duplicated.
from sync_long_horizon_hydrograph import (
    _API_READ_WRITE_ERRORS,
    _get_preprocessing_client,
    _json_safe,
    resolve_sdk_station_codes,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

VALUE_FIELD = "discharge"

# How many years of daily history to read for the climatology envelope.
HISTORY_YEARS_BACK = 20

_HORIZON_CONFIG: dict[str, dict[str, Any]] = {
    "pentad": {
        "periods_per_year": 72,
        "periods_per_month": 6,
        "period_days": 5,
        "norm_period": "p",
        "sdk_variable": "WDFA",
        "get_in_year": tl.get_pentad_in_year,
        "add_issue_date": fl.add_pentad_issue_date,
        "calc_avg": fl.calculate_pentadaldischargeavg,
        "get_issue_date": fl.get_issue_date_from_pentad,
        "get_day_of_year": fl.get_day_of_year_from_pentad,
        "get_value_in_month": fl.get_pentad_from_pentad_in_year,
    },
    "decade": {
        "periods_per_year": 36,
        "periods_per_month": 3,
        "period_days": 10,
        "norm_period": "d",
        "sdk_variable": "WDDCA",
        "get_in_year": tl.get_decad_in_year,
        "add_issue_date": fl.add_decad_issue_date,
        "calc_avg": fl.calculate_decadaldischargeavg,
        "get_issue_date": fl.get_issue_date_from_decad,
        "get_day_of_year": fl.get_day_of_year_from_decad,
        "get_value_in_month": fl.get_decad_from_decad_in_year,
    },
}


class _ShortHorizonWriteResult(list):
    def __init__(self, records: Iterable[dict[str, Any]] = ()) -> None:
        super().__init__(records)
        self.attempted_station_codes: list[str] = []
        self.completed_station_codes: list[str] = []
        self.failed_station_codes: list[str] = []


# ---------------------------------------------------------------------------
# Pure builders (offline/deterministic - S27)
# ---------------------------------------------------------------------------


def _iter_daily_rows(daily_rows: Any) -> list[dict[str, Any]]:
    if daily_rows is None:
        return []
    if isinstance(daily_rows, pd.DataFrame):
        return daily_rows.to_dict("records")
    return list(daily_rows)


def _parsed_daily_frame(code: str, daily_by_year: dict[int, Any]) -> pd.DataFrame:
    """Parse every (year, records) entry into one continuous [date, discharge] frame.

    Raises on any record with an unparseable date so neither the WDDA
    fallback nor the envelope climatology can silently drop or misclassify a
    day (a dropped day would understate coverage or skew a period's mean).
    """
    rows = []
    for _year, recs in daily_by_year.items():
        for rec in _iter_daily_rows(recs):
            if not isinstance(rec, dict):
                raise TypeError(f"Malformed daily record for code {code}: {rec!r}")
            if "date" not in rec:
                raise KeyError(f"Daily record missing 'date' for code {code}: {rec!r}")
            parsed_date = pd.Timestamp(rec["date"])
            if pd.isna(parsed_date):
                raise ValueError(f"Unparseable date {rec['date']!r} for code {code}")
            rows.append({"date": parsed_date, "discharge": rec.get(VALUE_FIELD)})

    columns = ["date", "discharge"]
    if not rows:
        return pd.DataFrame(columns=columns)
    frame = pd.DataFrame(rows, columns=columns)
    return frame.sort_values("date").reset_index(drop=True)


def _continuous_daily_frame(daily_frame: pd.DataFrame) -> pd.DataFrame:
    """Reindex each present year to a full Jan-1..Dec-31 calendar range.

    Production daily runoff arrives as a (near-)continuous series; a sparse,
    gappy fixture must be densified the same way so the legacy issue-date
    machinery always has a row to test on the 5th/10th/15th/20th/25th/EOM,
    letting ``apply_calculation``'s own gap-limited interpolation (limit=3)
    decide whether a short gap spanning an issue date can be filled. Only
    used for the envelope; the WDDA actual fallback counts real reported
    days and must see the original, un-densified frame.
    """
    if daily_frame.empty:
        return daily_frame
    frames = []
    for year in sorted(daily_frame["date"].dt.year.unique()):
        year_frame = daily_frame[daily_frame["date"].dt.year == year].set_index("date")
        full_index = pd.date_range(dt.date(int(year), 1, 1), dt.date(int(year), 12, 31), freq="D")
        year_frame = year_frame.reindex(full_index)
        year_frame.index.name = "date"
        frames.append(year_frame.reset_index())
    return pd.concat(frames, ignore_index=True).sort_values("date").reset_index(drop=True)


def _clean_discharge_series(series: pd.Series) -> pd.Series:
    """Mirror ``forecast_library.apply_calculation``'s discharge cleaning exactly:
    negatives -> NaN, interpolate (linear, both directions, limit=3), then
    ``round_discharge_to_float`` on each daily value.
    """
    values = pd.to_numeric(series, errors="coerce")
    values = values.where(~(values < 0), np.nan)
    interpolated = values.interpolate(method="linear", limit_direction="both", limit=3)
    rounded = interpolated.apply(
        lambda v: v if pd.isna(v) else fl.round_discharge_to_float(float(v))
    )
    return rounded


def _envelope_by_period(
    daily_frame: pd.DataFrame, config: dict[str, Any], target_year: int
) -> dict[int, dict[str, float]]:
    """Reproduce the legacy pentad/decad envelope exactly.

    Mirrors ``write_pentad_hydrograph_data`` / ``write_decad_hydrograph_data``:
    clean the daily series the same way as ``apply_calculation``, derive the
    issue-date ``discharge_avg`` via the real legacy helpers, relabel each
    issue-date row to its calendar period via ``date + 1 day``, keep years
    other than ``target_year``, then
    ``groupby(period).agg(mean/min/max/q05/q25/q75/q95).round(3)``.
    """
    if daily_frame.empty:
        return {}

    work = _continuous_daily_frame(daily_frame)
    work["discharge"] = _clean_discharge_series(work["discharge"])
    work = config["add_issue_date"](work, "date")
    work = config["calc_avg"](work, "date", "discharge")
    work = work[work["issue_date"]].copy()
    if work.empty:
        return {}

    get_in_year = config["get_in_year"]
    work["period"] = (work["date"] + pd.Timedelta(days=1)).apply(lambda d: int(get_in_year(d)))
    historical = work[work["date"].dt.year != target_year]
    if historical.empty:
        return {}

    grouped = (
        historical.groupby("period")
        .agg(
            mean=("discharge_avg", "mean"),
            min=("discharge_avg", "min"),
            max=("discharge_avg", "max"),
            q05=("discharge_avg", lambda x: x.quantile(0.05)),
            q25=("discharge_avg", lambda x: x.quantile(0.25)),
            q75=("discharge_avg", lambda x: x.quantile(0.75)),
            q95=("discharge_avg", lambda x: x.quantile(0.95)),
        )
        .round(3)
    )
    return grouped.to_dict(orient="index")


def _envelope_value(envelope_row: dict[str, Any], key: str) -> float | None:
    if key not in envelope_row:
        return None
    try:
        value = float(envelope_row[key])
    except (TypeError, ValueError):
        return None
    return _json_safe(value)


def _period_calendar_bounds(
    period: int, config: dict[str, Any], year: int
) -> tuple[dt.date, dt.date]:
    periods_per_month = config["periods_per_month"]
    period_days = config["period_days"]
    month = (period - 1) // periods_per_month + 1
    value_in_month = config["get_value_in_month"](period)
    start_day = (value_in_month - 1) * period_days + 1
    last_day_of_month = calendar.monthrange(year, month)[1]
    if value_in_month == periods_per_month:
        end_day = last_day_of_month
    else:
        end_day = min(value_in_month * period_days, last_day_of_month)
    return dt.date(year, month, start_day), dt.date(year, month, end_day)


def _finite_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and math.isfinite(value):
        return float(value)
    return None


def _period_actual(
    daily_frame: pd.DataFrame,
    sdk_map: dict[int, Any] | None,
    source_year: int,
    period: int,
    bounds: tuple[dt.date, dt.date],
    is_closed: bool,
) -> float | None:
    """SDK-first actual for one period, WDDA fallback under the >= 80% rule.

    A non-numeric SDK value fails safe to the WDDA fallback (never surfaced
    as the stored actual). An in-progress period never finalizes, even when
    the SDK carries a value for it.
    """
    if not is_closed:
        return None

    finite_sdk_value = _finite_number((sdk_map or {}).get(period))
    if finite_sdk_value is not None:
        return fl.round_3sf(finite_sdk_value)

    if daily_frame.empty:
        return None

    start, end = bounds
    days_in_period = (end - start).days + 1
    in_range = daily_frame[
        (daily_frame["date"].dt.year == source_year)
        & (daily_frame["date"].dt.date >= start)
        & (daily_frame["date"].dt.date <= end)
    ]
    if in_range.empty:
        return None

    numeric = pd.to_numeric(in_range["discharge"], errors="coerce")
    finite_mask = numeric.notna()
    distinct_finite_days = in_range.loc[finite_mask, "date"].dt.date.nunique()
    if distinct_finite_days / days_in_period < 0.80:
        return None

    finite_numeric = numeric[finite_mask]
    if finite_numeric.empty:
        return None
    return fl.round_3sf(float(finite_numeric.mean()))


def period_actuals(
    code: str,
    horizon_type: str,
    *,
    daily_by_year: dict[int, Any],
    sdk_current: dict[int, Any] | None,
    sdk_previous: dict[int, Any] | None,
    target_year: int,
    today: dt.date,
) -> tuple[dict[int, float | None], dict[int, float | None]]:
    """Return (current_by_period, previous_by_period) 3sf actuals for every period_in_year.

    Covers every ``period_in_year`` (1..72 pentad / 1..36 decade) using the SAME
    SDK-first + 80% WDDA fallback + in-progress guard rules as
    ``build_pentad_records``/``build_decad_records``. ``current`` is keyed to
    ``target_year`` (an in-progress period -> None); ``previous`` is keyed to
    ``target_year - 1`` (always closed).
    """
    config = _HORIZON_CONFIG[horizon_type]
    periods_per_year = config["periods_per_year"]
    daily_frame = _parsed_daily_frame(code, daily_by_year)
    previous_year = target_year - 1

    current_by_period: dict[int, float | None] = {}
    previous_by_period: dict[int, float | None] = {}
    for period in range(1, periods_per_year + 1):
        current_bounds = _period_calendar_bounds(period, config, target_year)
        current_by_period[period] = _period_actual(
            daily_frame=daily_frame,
            sdk_map=sdk_current,
            source_year=target_year,
            period=period,
            bounds=current_bounds,
            is_closed=current_bounds[1] < today,
        )
        previous_bounds = _period_calendar_bounds(period, config, previous_year)
        previous_by_period[period] = _period_actual(
            daily_frame=daily_frame,
            sdk_map=sdk_previous,
            source_year=previous_year,
            period=period,
            bounds=previous_bounds,
            is_closed=True,
        )
    return current_by_period, previous_by_period


def _build_short_horizon_records(
    code: str,
    horizon_type: str,
    norms: Iterable[Any],
    daily_by_year: dict[int, Any],
    sdk_current: dict[int, Any] | None,
    sdk_previous: dict[int, Any] | None,
    target_year: int,
    today: dt.date,
) -> list[dict[str, Any]]:
    config = _HORIZON_CONFIG[horizon_type]
    periods_per_year = config["periods_per_year"]
    norm_values = list(norms)
    if len(norm_values) != periods_per_year:
        raise ValueError(
            f"build_{horizon_type}_records: expected {periods_per_year} norm values for "
            f"code {code}, got {len(norm_values)}"
        )

    daily_frame = _parsed_daily_frame(code, daily_by_year)
    envelope_by_period = _envelope_by_period(daily_frame, config, target_year)
    current_by_period, previous_by_period = period_actuals(
        code,
        horizon_type,
        daily_by_year=daily_by_year,
        sdk_current=sdk_current,
        sdk_previous=sdk_previous,
        target_year=target_year,
        today=today,
    )

    records = []
    for period in range(1, periods_per_year + 1):
        envelope_row = envelope_by_period.get(period, {})
        record = {
            "horizon_type": horizon_type,
            "code": str(code),
            "date": config["get_issue_date"](period, target_year),
            "horizon_value": config["get_value_in_month"](period),
            "horizon_in_year": period,
            "day_of_year": config["get_day_of_year"](period, target_year),
            "norm": _json_safe(norm_values[period - 1]),
            "current": _json_safe(current_by_period.get(period)),
            "previous": _json_safe(previous_by_period.get(period)),
        }
        for key in ("mean", "min", "max", "q05", "q25", "q75", "q95"):
            record[key] = _envelope_value(envelope_row, key)
        records.append(record)
    return records


def build_pentad_records(
    code: str,
    *,
    norms: Iterable[Any],
    daily_by_year: dict[int, Any],
    sdk_current: dict[int, Any] | None = None,
    sdk_previous: dict[int, Any] | None = None,
    target_year: int,
    today: dt.date,
) -> list[dict[str, Any]]:
    """Build 72 pentad hydrograph records (envelope + norm + SDK/WDDA actuals)."""
    return _build_short_horizon_records(
        code=code,
        horizon_type="pentad",
        norms=norms,
        daily_by_year=daily_by_year,
        sdk_current=sdk_current or {},
        sdk_previous=sdk_previous or {},
        target_year=target_year,
        today=today,
    )


def build_decad_records(
    code: str,
    *,
    norms: Iterable[Any],
    daily_by_year: dict[int, Any],
    sdk_current: dict[int, Any] | None = None,
    sdk_previous: dict[int, Any] | None = None,
    target_year: int,
    today: dt.date,
) -> list[dict[str, Any]]:
    """Build 36 decad hydrograph records (envelope + norm + SDK/WDDA actuals)."""
    return _build_short_horizon_records(
        code=code,
        horizon_type="decade",
        norms=norms,
        daily_by_year=daily_by_year,
        sdk_current=sdk_current or {},
        sdk_previous=sdk_previous or {},
        target_year=target_year,
        today=today,
    )


# ---------------------------------------------------------------------------
# SDK / API wrappers (station read + write)
# ---------------------------------------------------------------------------


def _read_daily_runoff(client: Any, code: str, year: int, limit: int = 10000) -> Any:
    return client.read_runoff(
        horizon="day",
        code=str(code),
        start_date=f"{year}-01-01",
        end_date=f"{year}-12-31",
        limit=limit,
    )


def _read_daily_by_year(
    client: Any, code: str, target_year: int, years_back: int = HISTORY_YEARS_BACK
) -> dict[int, list[dict[str, Any]]]:
    """Read daily runoff for the climatology window, tolerating per-year gaps."""
    daily_by_year: dict[int, list[dict[str, Any]]] = {}
    for year in range(target_year - years_back, target_year + 1):
        try:
            rows = _iter_daily_rows(_read_daily_runoff(client, code, year))
        except _API_READ_WRITE_ERRORS as exc:
            logger.debug(
                "write_station_short_horizon: no daily runoff for site %s year %d: %s: %s",
                code,
                year,
                type(exc).__name__,
                exc,
            )
            continue
        if rows:
            daily_by_year[year] = rows
    return daily_by_year


_SDK_PAGE_SIZE = 1000
_SDK_MAX_PAGES = 50


def _fetch_sdk_period_actuals(
    iehhf_sdk: Any, code: str, horizon_type: str, target_year: int
) -> tuple[dict[int, float], dict[int, float]]:
    """Fetch WDFA/WDDCA period actuals from the SDK and bucket by period.

    The SDK response is a paginated dict (``count``/``next``/``previous``/
    ``results``), where each result is one station with a nested ``data``
    list of variable series, each holding a ``values`` list of
    ``{"value", "timestamp_local"/"timestamp_utc", ...}`` points. All pages
    are fetched (bounded by ``_SDK_MAX_PAGES``) and every point belonging to
    the requested variable is decoded.

    # NOTE (unverified — confirm against live iEH HF in the M4 parity check): iEH HF
    # pre-flight notes say WDFA/WDDCA are stamped mid-period, which this +1-day shift
    # handles correctly (mid-period +1 stays within the same period). If a live
    # diagnostic shows a different stamping convention, revisit this mapping.

    Shift the stamped date +1 day and reclassify with ``tag_library`` to
    recover the described period, then split into ``sdk_current``
    (target_year) / ``sdk_previous`` (target_year - 1) by the shifted date's
    year.
    """
    config = _HORIZON_CONFIG[horizon_type]
    get_in_year = config["get_in_year"]
    sdk_variable = config["sdk_variable"]
    sdk_current: dict[int, float] = {}
    sdk_previous: dict[int, float] = {}

    start = dt.datetime(target_year - 1, 1, 1)
    end = dt.datetime(target_year, 12, 31, 23, 59, 59)
    base_filters = {
        "site_codes": [str(code)],
        "variable_names": [sdk_variable],
        "local_date_time__gte": start.isoformat(),
        "local_date_time__lte": end.isoformat(),
        "page_size": _SDK_PAGE_SIZE,
    }

    results: list[Any] = []
    try:
        page = 1
        filters = dict(base_filters)
        while True:
            response = iehhf_sdk.get_data_values_for_site(filters=filters)
            page_results = response.get("results", []) if isinstance(response, dict) else []
            if isinstance(page_results, list):
                results.extend(page_results)
            has_next = bool(response.get("next")) if isinstance(response, dict) else False
            if not has_next or not page_results:
                break
            page += 1
            if page > _SDK_MAX_PAGES:
                logger.warning(
                    "write_station_short_horizon: SDK actuals pagination for site %s (%s) "
                    "hit the %d-page cap; results may be truncated.",
                    code,
                    sdk_variable,
                    _SDK_MAX_PAGES,
                )
                break
            filters = dict(base_filters)
            filters["page"] = page
    except Exception as exc:
        logger.warning(
            "write_station_short_horizon: SDK actuals fetch failed for site %s (%s): %s: %s",
            code,
            sdk_variable,
            type(exc).__name__,
            exc,
        )
        return sdk_current, sdk_previous

    try:
        for result in results:
            if not isinstance(result, dict):
                continue
            for series in result.get("data", []) or []:
                if not isinstance(series, dict):
                    continue
                if series.get("variable_code") != sdk_variable:
                    continue
                for point in series.get("values", []) or []:
                    if not isinstance(point, dict):
                        continue
                    raw_date = point.get("timestamp_local") or point.get("timestamp_utc")
                    raw_value = point.get("value")
                    if raw_date is None or raw_value is None:
                        continue
                    try:
                        # Interpret the chosen timestamp as LOCAL WALL TIME: if it
                        # carries a timezone offset, drop the offset WITHOUT
                        # converting to UTC (tz_localize(None) keeps the wall-clock
                        # value; tz_convert(None) would shift it). A naive
                        # timestamp has no tzinfo and is used as-is, unchanged from
                        # prior behaviour.
                        stamped = pd.to_datetime(raw_date)
                        if stamped.tzinfo is not None:
                            stamped = stamped.tz_localize(None)
                    except (ValueError, TypeError):
                        continue
                    try:
                        finite_value = float(raw_value)
                    except (TypeError, ValueError):
                        continue
                    if not math.isfinite(finite_value):
                        continue
                    shifted = stamped + pd.Timedelta(days=1)
                    period = int(get_in_year(shifted))
                    if shifted.year == target_year:
                        sdk_current[period] = finite_value
                    elif shifted.year == target_year - 1:
                        sdk_previous[period] = finite_value
    except Exception as exc:
        # A malformed SDK response must never raise out of this fetch - fail
        # safe to whatever was already bucketed (possibly empty) so the WDDA
        # daily fallback in `period_actuals` takes over instead of crashing
        # the caller.
        logger.warning(
            "write_station_short_horizon: SDK actuals response malformed for site %s (%s): %s: %s",
            code,
            sdk_variable,
            type(exc).__name__,
            exc,
        )
        return sdk_current, sdk_previous

    return sdk_current, sdk_previous


def write_station_short_horizon(
    code: str,
    horizon_type: str,
    iehhf_sdk: Any,
    client: Any,
    target_year: int,
    today: dt.date,
) -> list[dict[str, Any]]:
    """Build and write pentad or decad hydrograph records for one station."""
    config = _HORIZON_CONFIG[horizon_type]
    logger.info("Building short-horizon %s hydrograph for station %s", horizon_type, code)
    try:
        norms = iehhf_sdk.get_norm_for_site(code, "discharge", norm_period=config["norm_period"])
    except Exception as exc:
        logger.warning(
            "write_station_short_horizon: SDK norm call failed for site %s (%s), skipping. "
            "Error: %s: %s",
            code,
            horizon_type,
            type(exc).__name__,
            exc,
        )
        return []

    if len(norms) != config["periods_per_year"]:
        logger.warning(
            "write_station_short_horizon: expected %d norm values for site %s (%s), got %d - "
            "skipping this site.",
            config["periods_per_year"],
            code,
            horizon_type,
            len(norms),
        )
        return []

    daily_by_year = _read_daily_by_year(client, code, target_year)
    sdk_current, sdk_previous = _fetch_sdk_period_actuals(
        iehhf_sdk, code, horizon_type, target_year
    )

    builder = build_pentad_records if horizon_type == "pentad" else build_decad_records
    records = builder(
        code=code,
        norms=norms,
        daily_by_year=daily_by_year,
        sdk_current=sdk_current,
        sdk_previous=sdk_previous,
        target_year=target_year,
        today=today,
    )
    client.write_hydrograph(records)
    logger.info("Wrote %d %s hydrograph records for station %s", len(records), horizon_type, code)
    return records


def write_short_horizon_hydrograph(
    codes: Iterable[str],
    iehhf_sdk: Any,
    client: Any,
    target_year: int,
    today: dt.date,
) -> list[dict[str, Any]]:
    """Build and write pentad + decad hydrograph records for all supplied stations."""
    all_records = _ShortHorizonWriteResult()
    for code in codes:
        code_str = str(code)
        all_records.attempted_station_codes.append(code_str)
        try:
            station_records: list[dict[str, Any]] = []
            for horizon_type in ("pentad", "decade"):
                station_records.extend(
                    write_station_short_horizon(
                        code=code_str,
                        horizon_type=horizon_type,
                        iehhf_sdk=iehhf_sdk,
                        client=client,
                        target_year=target_year,
                        today=today,
                    )
                )
            if not station_records:
                all_records.attempted_station_codes.pop()
                logger.info("No short-horizon hydrograph records produced for station %s", code_str)
                continue
            all_records.extend(station_records)
            all_records.completed_station_codes.append(code_str)
        except _API_READ_WRITE_ERRORS as exc:
            all_records.failed_station_codes.append(code_str)
            logger.warning(
                "Short-horizon hydrograph API read/write failed for station %s; preserving any "
                "records already written for this station and continuing. Error: %s: %s",
                code_str,
                type(exc).__name__,
                exc,
            )
            continue

    if all_records.failed_station_codes:
        logger.warning(
            "Short-horizon hydrograph API read/write failures for %d/%d attempted station(s): %s",
            len(all_records.failed_station_codes),
            len(all_records.attempted_station_codes),
            ", ".join(all_records.failed_station_codes),
        )
    return all_records


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sync_short_horizon_hydrograph.py",
        description=(
            "Fetch pentad/decad discharge norms and local daily runoff, plus SDK WDFA/WDDCA "
            "actuals, then write pentad and decad short-horizon hydrograph rows."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--target-year",
        type=int,
        default=None,
        metavar="YEAR",
        help="Year to stamp on pentad/decad rows. Defaults to the current calendar year.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Resolve stations and exit without writing hydrograph rows.",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    today = dt.date.today()
    target_year = args.target_year if args.target_year is not None else today.year

    try:
        sl.load_environment()
        sdk = IEasyHydroHFSDK()
        codes = resolve_sdk_station_codes(sdk)
        logger.info("Target year for short-horizon pentad/decad hydrograph rows: %d", target_year)

        if args.dry_run:
            print(f"DRY-RUN - sdk_only_codes: {codes}")
            print(f"DRY-RUN - target_year: {target_year}")
            sys.exit(0)

        if not codes:
            logger.error("No SDK sites remain after filtering - nothing to write.")
            sys.exit(2)

        client = _get_preprocessing_client()
        records = write_short_horizon_hydrograph(
            codes=codes,
            iehhf_sdk=sdk,
            client=client,
            target_year=target_year,
            today=today,
        )
        attempted_station_codes = getattr(records, "attempted_station_codes", [])
        completed_station_codes = getattr(records, "completed_station_codes", [])
        if len(completed_station_codes) == 0 and len(attempted_station_codes) > 0:
            logger.error(
                "All %d attempted station(s) had short-horizon hydrograph API read/write "
                "failures; exiting 2 after preserving any successful partial writes.",
                len(attempted_station_codes),
            )
            sys.exit(2)
        if not records:
            logger.error("No pentad/decad hydrograph records produced - nothing to write.")
            sys.exit(2)
        logger.info("Short-horizon hydrograph ingestion wrote %d records.", len(records))
        sys.exit(0)

    except RuntimeError as exc:
        logger.error("API error during short-horizon hydrograph ingestion: %s", exc)
        sys.exit(1)
    except SystemExit:
        raise
    except Exception as exc:
        logger.exception("Unexpected error during short-horizon hydrograph ingestion: %s", exc)
        sys.exit(3)


if __name__ == "__main__":
    main()
