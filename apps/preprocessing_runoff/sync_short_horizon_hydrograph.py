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
import numbers
import os
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
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
        # Sum of _ShortHorizonWriteStatus.API_FAILED across both horizons and all
        # attempted stations (C5). Additive attribute, populated by
        # write_short_horizon_hydrograph from the C3 per-(code, horizon) status
        # tally; existing callers that only use the list/attempted/completed/failed
        # surface are unaffected.
        self.api_failed_count: int = 0


class _NormClassification(Enum):
    VALID = "valid"
    NORM_ABSENT = "norm_absent"
    SDK_FAILED = "sdk_failed"


@dataclass(frozen=True)
class _ShortHorizonNormLookupResult:
    classification: _NormClassification
    norms: Any
    exception: Exception | None = None


class _ShortHorizonWriteStatus(Enum):
    """Terminal status for one ``(code, horizon)`` write attempt (C3)."""

    WRITTEN = "written"
    NORM_ABSENT = "norm_absent"
    SDK_FAILED = "sdk_failed"
    API_FAILED = "api_failed"


class _ShortHorizonNormReadError(Exception):
    """Raised when the C2 preservation read (``_read_existing_period_norms``)
    fails for any reason.

    The installed client can raise things outside ``_API_READ_WRITE_ERRORS``
    from a nominally successful response - e.g.
    ``requests.exceptions.JSONDecodeError`` decoding a 200 body, or a
    ``ValueError`` from DataFrame construction. A failed preservation read has
    exactly one safe response regardless of cause: do not write this horizon,
    because writing an all-``None`` norm would erase stored values via the
    API's field-by-field upsert. This narrow class lets the per-horizon
    boundary in ``write_short_horizon_hydrograph`` classify any such failure
    as ``API_FAILED`` without widening ``_API_READ_WRITE_ERRORS`` itself
    (see PREPQ-018).
    """


class _ShortHorizonDailyReadError(Exception):
    """Raised when ``_read_daily_by_year`` finds NO usable daily runoff at all
    for a station-horizon, across every year in the climatology window.

    Emptiness test: ``_read_daily_by_year`` only ever stores a year whose
    ``rows`` came back non-empty (a year that errors, per its existing
    per-year ``_API_READ_WRITE_ERRORS`` tolerance, or that legitimately
    returns zero rows, is simply omitted - never stored as an empty list).
    So the returned dict is empty if and only if every single year - whether
    by error or by a genuinely empty response - contributed nothing. A
    station with SOME years present and others missing (the common,
    legitimate multi-year-gap case) still returns a non-empty dict and never
    reaches this branch; per-year tolerance is unchanged.

    A wholly empty result would otherwise flow into the builder and produce a
    full batch of rows with every envelope field (mean/min/max/q05/q25/q75/
    q95) and current/previous None, which write_hydrograph would then write,
    clobbering any previously stored values via the API's field-by-field
    upsert. This narrow class lets the per-horizon boundary in
    ``write_short_horizon_hydrograph`` classify that outcome as API_FAILED
    for this ``(code, horizon)`` pair only, the same way a failed
    preservation read is classified, without writing anything for this
    horizon (see PREPQ-020).
    """


class _ShortHorizonHorizonRecords(list):
    """Records for one ``(code, horizon)`` attempt, tagged with its terminal
    ``_ShortHorizonWriteStatus`` (C3).

    A ``list`` subclass, not a new return type: every existing caller of
    ``write_station_short_horizon`` that treats the return value as a plain
    ``list[dict]`` (``len()``, iteration, indexing) is unaffected. Only
    ``write_short_horizon_hydrograph`` reads ``.status``.
    """

    def __init__(
        self,
        records: Iterable[dict[str, Any]] = (),
        status: _ShortHorizonWriteStatus | None = None,
    ) -> None:
        super().__init__(records)
        self.status = status


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
            "date": config["get_issue_date"](period, target_year).date().isoformat(),
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
    """Read daily runoff for the climatology window, tolerating per-year gaps.

    Per-year tolerance is unchanged: a single year's read failure (any
    ``_API_READ_WRITE_ERRORS`` member) is logged at DEBUG and skipped, same
    as before. Only after every year has been attempted do we check whether
    the result is usable at all - see ``_ShortHorizonDailyReadError`` for the
    exact emptiness test and why an all-years-failed/empty result must not
    silently flow into the builder.
    """
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
    if not daily_by_year:
        raise _ShortHorizonDailyReadError(
            f"no usable daily runoff for site {code} across {years_back + 1} years "
            f"(target_year={target_year}); refusing to build/write a full null batch"
        )
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


def _classify_short_horizon_norms(norms: Any, periods_per_year: int) -> _NormClassification:
    """Classify an SDK pentad/decad-norm return value.

    VALID only when ``norms`` is an ordered ``list``/``tuple`` of exactly
    ``periods_per_year`` finite real numbers (``bool`` explicitly rejected).
    Position carries meaning downstream (the builder indexes
    ``norm_values[period - 1]``), so a set or dict of the right size is
    NORM_ABSENT, not VALID - it could be silently mis-ordered or mis-keyed.
    The length is computed only after the list/tuple check, so a ``TypeError``
    on ``len()`` of an unsized response (e.g. ``None``) can no longer escape.
    """
    if not isinstance(norms, (list, tuple)):
        return _NormClassification.NORM_ABSENT
    if len(norms) != periods_per_year:
        return _NormClassification.NORM_ABSENT
    for value in norms:
        if isinstance(value, bool) or not isinstance(value, numbers.Real):
            return _NormClassification.NORM_ABSENT
        if not math.isfinite(float(value)):
            return _NormClassification.NORM_ABSENT
    return _NormClassification.VALID


def _lookup_short_horizon_norms(
    code: str, horizon_type: str, iehhf_sdk: Any
) -> _ShortHorizonNormLookupResult:
    """Fetch and classify the SDK pentad/decad norms, capturing any raised exception."""
    config = _HORIZON_CONFIG[horizon_type]
    try:
        norms = iehhf_sdk.get_norm_for_site(code, "discharge", norm_period=config["norm_period"])
    except Exception as exc:
        return _ShortHorizonNormLookupResult(
            classification=_NormClassification.SDK_FAILED,
            norms=None,
            exception=exc,
        )
    return _ShortHorizonNormLookupResult(
        classification=_classify_short_horizon_norms(norms, config["periods_per_year"]),
        norms=norms,
    )


def _read_existing_period_norms(
    client: Any, code: str, horizon_type: str, target_year: int
) -> list[Any]:
    """Read stored period-row norms for the target year into a periods_per_year list.

    Returns a ``periods_per_year``-length list keyed by ``horizon_in_year``
    (1..N); missing periods stay ``None``. Used to preserve any stored norm
    across a norm-absent/SDK-failed rerun (C2).

    The read window is NOT the calendar year - period 1 of ``target_year`` is
    stamped with the PRECEDING 31 December (``get_issue_date_from_pentad(1,
    2026)`` -> ``2025-12-31``). Both the request bounds and the per-row match
    are therefore derived from ``config["get_issue_date"]``, never from
    calendar-year boundaries, and each row is matched on the exact
    ``(date, horizon_in_year)`` pair for the target year - not on
    ``horizon_in_year`` alone - so a row from the wrong year cannot be
    mistaken for this year's period.

    Any exception from ``client.read_hydrograph`` - not only
    ``_API_READ_WRITE_ERRORS`` members, since the installed client can raise
    other things from a nominally successful response (e.g. a JSON-decode
    error or a ``ValueError``) - is caught here and re-raised as
    ``_ShortHorizonNormReadError``, so the per-horizon boundary in
    ``write_short_horizon_hydrograph`` can classify it as API_FAILED (C2a)
    rather than falling back to an all-``None`` write, which would erase
    every stored norm for this station-horizon via the API's field-by-field
    upsert.
    """
    config = _HORIZON_CONFIG[horizon_type]
    periods_per_year = config["periods_per_year"]
    get_issue_date = config["get_issue_date"]
    expected_dates = {
        period: get_issue_date(period, target_year).date().isoformat()
        for period in range(1, periods_per_year + 1)
    }

    try:
        existing = client.read_hydrograph(
            horizon=horizon_type,
            code=code,
            start_date=expected_dates[1],
            end_date=expected_dates[periods_per_year],
            limit=1000,
        )
    except Exception as exc:
        raise _ShortHorizonNormReadError(
            f"preservation read failed for site {code} ({horizon_type}): "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    norm_values: list[Any] = [None] * periods_per_year
    for row in _iter_daily_rows(existing):
        if not isinstance(row, dict):
            continue
        try:
            period = int(row.get("horizon_in_year"))
        except (TypeError, ValueError):
            continue
        if period not in expected_dates:
            continue
        if row.get("date") != expected_dates[period]:
            continue
        norm_values[period - 1] = row.get("norm")
    return norm_values


def write_station_short_horizon(
    code: str,
    horizon_type: str,
    iehhf_sdk: Any,
    client: Any,
    target_year: int,
    today: dt.date,
) -> list[dict[str, Any]]:
    """Build and write pentad or decad hydrograph records for one station.

    Row existence is decoupled from the iEH-HF pentad/decad norm (C1): when
    the norm is absent (any non-``periods_per_year``-finite-numbers return)
    OR the SDK call itself raises, the full 72 (pentad) / 36 (decad) rows are
    still built and written from local daily runoff, with any previously
    stored norm preserved via a read-merge (C2). A missing norm is not our
    failure, so that case logs at INFO; WARNING is reserved for the SDK-raise
    case, where absence cannot be distinguished from an outage.

    If the read-merge itself fails (C2a), this raises rather than falling
    back to an all-``None`` write, which would clobber every stored norm for
    this station-horizon; the caller is responsible for treating that as a
    failure and not writing this horizon. The same applies if the daily
    runoff read (``_read_daily_by_year``) comes back with no usable data at
    all across every climatology year: that also raises
    (``_ShortHorizonDailyReadError``) instead of building/writing a full
    batch with every envelope field and current/previous None, which would
    likewise clobber previously stored values.
    """
    logger.info("Building short-horizon %s hydrograph for station %s", horizon_type, code)

    norm_lookup = _lookup_short_horizon_norms(code, horizon_type, iehhf_sdk)
    norms = norm_lookup.norms
    if norm_lookup.classification is _NormClassification.SDK_FAILED:
        exc = norm_lookup.exception
        logger.warning(
            "write_station_short_horizon: SDK norm call failed for site %s (%s), continuing "
            "with a read-merge of any previously stored norm. Error: %s: %s",
            code,
            horizon_type,
            type(exc).__name__,
            exc,
        )
        norms = _read_existing_period_norms(client, code, horizon_type, target_year)
    elif norm_lookup.classification is _NormClassification.NORM_ABSENT:
        logger.info(
            "write_station_short_horizon: %s norm unavailable for site %s; continuing with a "
            "read-merge of any previously stored norm.",
            horizon_type,
            code,
        )
        norms = _read_existing_period_norms(client, code, horizon_type, target_year)

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
    if norm_lookup.classification is _NormClassification.SDK_FAILED:
        status = _ShortHorizonWriteStatus.SDK_FAILED
    elif norm_lookup.classification is _NormClassification.NORM_ABSENT:
        status = _ShortHorizonWriteStatus.NORM_ABSENT
    else:
        status = _ShortHorizonWriteStatus.WRITTEN
    return _ShortHorizonHorizonRecords(records, status=status)


def _short_horizon_degraded_line(
    horizon_type: str,
    counts: dict[_ShortHorizonWriteStatus, int],
    total_attempted: int,
) -> str | None:
    """Build the one-line degraded-norm note for ``horizon_type``, or ``None`` if clean.

    The reported count is ``norm_absent + sdk_failed`` (both are "no usable
    norm this run" outcomes); it excludes ``api_failed``, which is a
    read/write problem, not a norm-availability one.
    """
    n_unavailable = (
        counts[_ShortHorizonWriteStatus.NORM_ABSENT] + counts[_ShortHorizonWriteStatus.SDK_FAILED]
    )
    if n_unavailable == 0:
        return None
    return (
        f"{horizon_type} discharge norms unavailable for {n_unavailable}/{total_attempted} "
        "stations; observed runoff written; norm unavailable."
    )


def _log_short_horizon_run_summary(
    status_counts: dict[str, dict[_ShortHorizonWriteStatus, int]],
    total_attempted: int,
) -> None:
    """Log the counts-only ``SHORT-HORIZON RUN SUMMARY`` block (C3).

    The block itself is neutral and always logged at INFO. One additional
    line per horizon is logged only when that horizon has any
    ``norm_absent`` or ``sdk_failed`` pairs. Per the 2026-09-04 owner
    decision ("a missing norm is not our problem"), a norm-absent-only
    horizon logs at INFO; WARNING is reserved for a horizon with any
    ``sdk_failed`` count, where the lookup raised and absence cannot be
    distinguished from an outage. Neither case is labeled DEGRADED or ERROR
    (that wording predates the decision and is not a precedent here).
    """
    lines = ["SHORT-HORIZON RUN SUMMARY", f"total_attempted={total_attempted}"]
    for horizon_type in ("pentad", "decade"):
        counts = status_counts[horizon_type]
        lines.append(
            f"{horizon_type}_written={counts[_ShortHorizonWriteStatus.WRITTEN]} "
            f"{horizon_type}_norm_absent={counts[_ShortHorizonWriteStatus.NORM_ABSENT]} "
            f"{horizon_type}_sdk_failed={counts[_ShortHorizonWriteStatus.SDK_FAILED]} "
            f"{horizon_type}_api_failed={counts[_ShortHorizonWriteStatus.API_FAILED]}"
        )
    lines.append("END SHORT-HORIZON RUN SUMMARY")
    logger.info("\n".join(lines))

    for horizon_type in ("pentad", "decade"):
        counts = status_counts[horizon_type]
        line = _short_horizon_degraded_line(horizon_type, counts, total_attempted)
        if line is None:
            continue
        if counts[_ShortHorizonWriteStatus.SDK_FAILED] > 0:
            logger.warning(line)
        else:
            logger.info(line)


def write_short_horizon_hydrograph(
    codes: Iterable[str],
    iehhf_sdk: Any,
    client: Any,
    target_year: int,
    today: dt.date,
) -> list[dict[str, Any]]:
    """Build and write pentad + decad hydrograph records for all supplied stations.

    Each ``(code, horizon)`` pair is attempted, written, and classified
    independently (C3a): the exception boundary sits inside the horizon
    loop, so a pentad API read/write failure (e.g. a failed read-merge,
    C2a, or a total daily-runoff read failure across every climatology
    year - see ``_ShortHorizonDailyReadError``) records that pair as
    ``API_FAILED`` and skips only that horizon's write. The same station's
    decade horizon is still attempted and
    classified on its own terms, and the loop still continues to the next
    station. A station is counted as "completed" only when neither of its
    horizons hit ``API_FAILED``; otherwise it is "failed" (mirroring the
    pre-existing attempted/completed/failed station bookkeeping) even
    though it may still carry records from the horizon that did succeed.

    A ``SHORT-HORIZON RUN SUMMARY`` block, tallying one terminal
    ``_ShortHorizonWriteStatus`` per ``(code, horizon)`` pair, is logged
    before returning (C3).
    """
    all_records = _ShortHorizonWriteResult()
    status_counts: dict[str, dict[_ShortHorizonWriteStatus, int]] = {
        horizon_type: dict.fromkeys(_ShortHorizonWriteStatus, 0) for horizon_type in _HORIZON_CONFIG
    }
    for code in codes:
        code_str = str(code)
        all_records.attempted_station_codes.append(code_str)
        station_records: list[dict[str, Any]] = []
        station_had_api_failure = False
        for horizon_type in ("pentad", "decade"):
            try:
                horizon_records = write_station_short_horizon(
                    code=code_str,
                    horizon_type=horizon_type,
                    iehhf_sdk=iehhf_sdk,
                    client=client,
                    target_year=target_year,
                    today=today,
                )
            except (
                *_API_READ_WRITE_ERRORS,
                _ShortHorizonNormReadError,
                _ShortHorizonDailyReadError,
            ) as exc:
                status_counts[horizon_type][_ShortHorizonWriteStatus.API_FAILED] += 1
                station_had_api_failure = True
                logger.warning(
                    "Short-horizon %s hydrograph API read/write failed for station %s; this "
                    "horizon is not written, other horizons/stations continue. Error: %s: %s",
                    horizon_type,
                    code_str,
                    type(exc).__name__,
                    exc,
                )
                continue
            status_counts[horizon_type][horizon_records.status] += 1
            station_records.extend(horizon_records)

        if station_records:
            all_records.extend(station_records)
        else:
            logger.info("No short-horizon hydrograph records produced for station %s", code_str)

        if station_had_api_failure:
            all_records.failed_station_codes.append(code_str)
        else:
            all_records.completed_station_codes.append(code_str)

    if all_records.failed_station_codes:
        logger.warning(
            "Short-horizon hydrograph API read/write failures for %d/%d attempted station(s): %s",
            len(all_records.failed_station_codes),
            len(all_records.attempted_station_codes),
            ", ".join(all_records.failed_station_codes),
        )

    _log_short_horizon_run_summary(status_counts, len(all_records.attempted_station_codes))
    all_records.api_failed_count = sum(
        counts[_ShortHorizonWriteStatus.API_FAILED] for counts in status_counts.values()
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
        api_failed_count = getattr(records, "api_failed_count", 0)
        # C5: diagnose from the real per-(code, horizon) API_FAILED tally (C3),
        # not from attempted/completed list lengths. After C1, a norm-absent or
        # SDK-failed station still writes records and is counted "completed", so
        # completed == 0 no longer happens on a norm-only degradation - only an
        # actual API read/write failure clears it. Requiring api_failed_count > 0
        # keeps that inference honest instead of assuming it.
        if (
            api_failed_count > 0
            and len(completed_station_codes) == 0
            and len(attempted_station_codes) > 0
        ):
            logger.error(
                "%d short-horizon hydrograph API read/write failure(s) across %d attempted "
                "station(s), none completed; exiting 2 after preserving any successful partial "
                "writes.",
                api_failed_count,
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
