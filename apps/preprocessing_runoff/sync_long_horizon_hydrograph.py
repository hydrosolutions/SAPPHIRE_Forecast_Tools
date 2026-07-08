"""Monthly long-horizon runoff hydrograph ingestion.

Builds one monthly hydrograph row per station/month with the full
``(norm, previous, current)`` triad. Norms come from the iEH HF SDK; previous
and current values are monthly means aggregated from local daily SAPPHIRE
runoff rows.
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

import pandas as pd
import requests

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_IEHF_DIR = os.path.join(_SCRIPT_DIR, "..", "iEasyHydroForecast")
if _IEHF_DIR not in sys.path:
    sys.path.insert(0, _IEHF_DIR)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

import forecast_library as fl
import setup_library as sl
from ieasyhydro_sdk.sdk import IEasyHydroHFSDK
from setup_library import (
    _get_manual_site_codes,
    get_all_forecast_sites_from_HF_SDK,
)

try:
    from sapphire_api_client import SapphireAPIError, SapphirePreprocessingClient

    SAPPHIRE_API_AVAILABLE = True
except ImportError:
    SapphireAPIError = None
    SapphirePreprocessingClient = None
    SAPPHIRE_API_AVAILABLE = False

_API_READ_WRITE_ERRORS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)
if SapphireAPIError is not None:
    _API_READ_WRITE_ERRORS = (SapphireAPIError, *_API_READ_WRITE_ERRORS)


class _NormClassification(Enum):
    VALID = "valid"
    NORM_ABSENT = "norm_absent"
    SDK_FAILED = "sdk_failed"


class LongHorizonStationWriteStatus(Enum):
    WRITTEN = "written"
    NORM_ABSENT = "norm_absent"
    SDK_FAILED = "sdk_failed"
    API_FAILED = "api_failed"


@dataclass(frozen=True)
class LongHorizonStationWriteResult:
    status: LongHorizonStationWriteStatus
    records: list[dict[str, Any]]


@dataclass(frozen=True)
class LongHorizonRunSummary:
    status_counts: dict[LongHorizonStationWriteStatus, int]
    total_attempted: int


@dataclass(frozen=True)
class _MonthlyNormLookupResult:
    classification: _NormClassification
    norms: Any
    exception: Exception | None = None


class _LongHorizonWriteResult(list):
    def __init__(self, records: Iterable[dict[str, Any]] = ()) -> None:
        super().__init__(records)
        self.attempted_station_codes: list[str] = []
        self.completed_station_codes: list[str] = []
        self.failed_station_codes: list[str] = []
        self.station_statuses: list[tuple[str, LongHorizonStationWriteStatus]] = []


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

VALUE_FIELD = "discharge"
MONTHS = tuple(range(1, 13))
MID_MONTH_DOY = (15, 46, 74, 105, 135, 166, 196, 227, 258, 288, 319, 349)
QUARTER_MONTHS = {1: (1, 2, 3), 2: (4, 5, 6), 3: (7, 8, 9), 4: (10, 11, 12)}


def _json_safe(value: Any) -> Any:
    """NaN, +inf, -inf, and None all map to None. Finite values pass through."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, numbers.Real):
        coerced = float(value)
        if not math.isfinite(coerced):
            return None
        return coerced
    return value


def monthly_mean_threshold_80(values: Iterable[Any], year: int, month: int) -> float | None:
    """Return a monthly mean when finite values cover at least 80% of the month."""
    days_in_month = calendar.monthrange(year, month)[1]
    finite_values = [
        float(value)
        for value in values
        if value is not None and isinstance(value, (int, float)) and math.isfinite(value)
    ]
    if len(finite_values) / days_in_month < 0.80:
        logger.debug(
            "Monthly runoff cell below threshold: year=%s month=%s finite=%s days=%s",
            year,
            month,
            len(finite_values),
            days_in_month,
        )
        return None
    return sum(finite_values) / len(finite_values)


def _iter_daily_rows(daily_rows: Any) -> list[dict[str, Any]]:
    if daily_rows is None:
        return []
    if isinstance(daily_rows, pd.DataFrame):
        return daily_rows.to_dict("records")
    return list(daily_rows)


def _month_values(daily_rows: Any, year: int, month: int) -> list[Any]:
    values = []
    for row in _iter_daily_rows(daily_rows):
        if not isinstance(row, dict):
            continue
        date_value = row.get("date")
        if date_value is None:
            continue
        date_obj = pd.to_datetime(date_value, errors="coerce")
        if pd.isna(date_obj):
            continue
        if date_obj.year == year and date_obj.month == month:
            values.append(row.get(VALUE_FIELD))
    return values


def _read_daily_runoff(client: Any, code: str, year: int, limit: int = 10000) -> Any:
    return client.read_runoff(
        horizon="day",
        code=str(code),
        start_date=f"{year}-01-01",
        end_date=f"{year}-12-31",
        limit=limit,
    )


def _resolve_from_decadal(from_decadal: bool | None) -> bool:
    """Resolve the from-decadal switch, defaulting to TRUE when unset.

    ``SAPPHIRE_MONTHLY_FROM_DECADAL`` is parsed case-insensitively:
    "false"/"0" -> False, anything else (including unset, "true", "1") -> True.
    """
    if from_decadal is not None:
        return from_decadal
    raw = os.getenv("SAPPHIRE_MONTHLY_FROM_DECADAL")
    if raw is None:
        return True
    return raw.strip().lower() not in {"false", "0"}


def _month_from_decadal(
    month: int,
    decad_by_period: dict[int, float | None] | None,
) -> float | None:
    """Mean of a month's 3 (already 3sf-rounded) decadal actuals, all-or-nothing.

    The 3 decads of calendar month M have decad_in_year (M-1)*3+1, +2, +3. The
    round-of-already-rounded result is intentional (iEH-HF parity), not a bug.
    """
    if not decad_by_period:
        return None
    first_decad = (month - 1) * 3 + 1
    decad_values = [decad_by_period.get(first_decad + offset) for offset in range(3)]
    if any(value is None for value in decad_values):
        return None
    return fl.round_3sf(sum(float(value) for value in decad_values) / 3)


def build_monthly_records(
    code: str,
    norms: Iterable[Any],
    daily_current_year: Any,
    daily_previous_year: Any,
    target_year: int,
    today: dt.date,
    *,
    decad_current: dict[int, float | None] | None = None,
    decad_previous: dict[int, float | None] | None = None,
    from_decadal: bool | None = None,
) -> list[dict[str, Any]]:
    """Build 12 monthly hydrograph records for one station.

    Actuals (``current``/``previous``) follow the iEH-HF rounded-aggregation
    method: by default (``SAPPHIRE_MONTHLY_FROM_DECADAL`` unset/true), each
    month's actual is ``round_3sf`` of the mean of its 3 already-3sf-rounded
    decadal actuals (all-or-nothing on the 3 decads); when the flag is false,
    the actual falls back to ``round_3sf`` of the existing >=80%-coverage
    daily mean. ``norm``/date/day_of_year/horizon fields are unchanged.
    """
    norm_values = list(norms)
    use_decadal = _resolve_from_decadal(from_decadal)

    records = []
    previous_year = target_year - 1
    for month in MONTHS:
        if use_decadal:
            previous = _month_from_decadal(month, decad_previous)
            current = _month_from_decadal(month, decad_current)
        else:
            previous = fl.round_3sf(
                monthly_mean_threshold_80(
                    _month_values(daily_previous_year, previous_year, month),
                    previous_year,
                    month,
                )
            )
            current = fl.round_3sf(
                monthly_mean_threshold_80(
                    _month_values(daily_current_year, target_year, month),
                    target_year,
                    month,
                )
            )
        if target_year == today.year and month == today.month:
            current = None

        records.append(
            {
                "horizon_type": "month",
                "code": str(code),
                "date": f"{target_year}-{month:02d}-01",
                "day_of_year": MID_MONTH_DOY[month - 1],
                "horizon_value": month,
                "horizon_in_year": month,
                "norm": _json_safe(norm_values[month - 1]),
                "previous": _json_safe(previous),
                "current": _json_safe(current),
            }
        )
    return records


def _classify_monthly_norms(norms: Any) -> _NormClassification:
    """Classify an SDK monthly-norm return value.

    VALID only when ``norms`` is a list/tuple of exactly 12 finite real numbers
    (bool is explicitly rejected); any other successful shape is NORM_ABSENT.
    """
    if not isinstance(norms, (list, tuple)):
        return _NormClassification.NORM_ABSENT
    if len(norms) != 12:
        return _NormClassification.NORM_ABSENT
    for value in norms:
        if isinstance(value, bool) or not isinstance(value, numbers.Real):
            return _NormClassification.NORM_ABSENT
        if not math.isfinite(float(value)):
            return _NormClassification.NORM_ABSENT
    return _NormClassification.VALID


def _lookup_monthly_norms(code: str, iehhf_sdk: Any) -> _MonthlyNormLookupResult:
    """Fetch and classify the SDK monthly norms, capturing any raised exception."""
    try:
        norms = iehhf_sdk.get_norm_for_site(code, "discharge", norm_period="m")
    except Exception as exc:
        return _MonthlyNormLookupResult(
            classification=_NormClassification.SDK_FAILED,
            norms=None,
            exception=exc,
        )
    return _MonthlyNormLookupResult(
        classification=_classify_monthly_norms(norms),
        norms=norms,
    )


def _read_existing_month_norms(client: Any, code: str, target_year: int) -> list[Any]:
    """Read stored MONTH-row norms for the target year into a 12-element list.

    Returns ``[None] * 12`` keyed by ``horizon_in_year`` (1-12); missing months
    stay ``None``. Used to preserve any stored norm across a norm-absent rerun.
    """
    existing = client.read_hydrograph(
        horizon="month",
        code=code,
        start_date=f"{target_year}-01-01",
        end_date=f"{target_year}-12-31",
        limit=1000,
    )
    norm_values: list[Any] = [None] * 12
    for row in _iter_daily_rows(existing):
        if not isinstance(row, dict):
            continue
        try:
            month = int(row.get("horizon_in_year"))
        except (TypeError, ValueError):
            continue
        if month in MONTHS:
            norm_values[month - 1] = row.get("norm")
    return norm_values


def write_station_monthly_hydrograph(
    code: str,
    iehhf_sdk: Any,
    client: Any,
    target_year: int,
    today: dt.date,
) -> LongHorizonStationWriteResult:
    """Build and write monthly hydrograph records for one station.

    Row existence is decoupled from the iEH-HF monthly norm: when the norm is
    absent (any non-12-finite return), the 12 month rows are still written and
    any previously stored norm is preserved via a read-merge. Only an SDK
    exception skips the station.
    """
    logger.info("Building long-horizon monthly hydrograph for station %s", code)
    norm_lookup = _lookup_monthly_norms(code, iehhf_sdk)
    norm_classification = norm_lookup.classification
    if norm_classification is _NormClassification.SDK_FAILED:
        exc = norm_lookup.exception
        logger.debug(
            "write_station_monthly_hydrograph: SDK call failed for site %s, skipping. "
            "Error: %s: %s",
            code,
            type(exc).__name__,
            exc,
        )
        return LongHorizonStationWriteResult(
            status=LongHorizonStationWriteStatus.SDK_FAILED,
            records=[],
        )

    norms = norm_lookup.norms
    if norm_classification is _NormClassification.NORM_ABSENT:
        logger.debug(
            "write_station_monthly_hydrograph: monthly norms absent for site %s; "
            "preserving any existing month norms.",
            code,
        )
        norms = _read_existing_month_norms(client, code, target_year)

    daily_current_year = _read_daily_runoff(client, code, target_year)
    daily_previous_year = _read_daily_runoff(client, code, target_year - 1)

    # Local import: `sync_short_horizon_hydrograph` imports plumbing back out of
    # this module at module load time, so importing it here (call time, once
    # this module is already fully initialized) avoids a circular import.
    import sync_short_horizon_hydrograph as shh

    daily_by_year = {target_year: daily_current_year, target_year - 1: daily_previous_year}
    sdk_current, sdk_previous = shh._fetch_sdk_period_actuals(
        iehhf_sdk, code, "decade", target_year
    )
    decad_current, decad_previous = shh.period_actuals(
        code,
        "decade",
        daily_by_year=daily_by_year,
        sdk_current=sdk_current,
        sdk_previous=sdk_previous,
        target_year=target_year,
        today=today,
    )

    records = build_monthly_records(
        code=code,
        norms=norms,
        daily_current_year=daily_current_year,
        daily_previous_year=daily_previous_year,
        target_year=target_year,
        today=today,
        decad_current=decad_current,
        decad_previous=decad_previous,
        from_decadal=None,
    )
    client.write_hydrograph(records)
    logger.info("Wrote %d monthly hydrograph records for station %s", len(records), code)
    status = (
        LongHorizonStationWriteStatus.WRITTEN
        if norm_classification is _NormClassification.VALID
        else LongHorizonStationWriteStatus.NORM_ABSENT
    )
    return LongHorizonStationWriteResult(status=status, records=records)


def _seasonal_field_mean(monthly_records: list[dict[str, Any]], field: str) -> float | None:
    monthly_by_value = {record.get("horizon_value"): record for record in monthly_records}
    monthly_values = []
    for month in range(4, 10):
        record = monthly_by_value.get(month, {})
        value = record.get(field)
        if value is None or not isinstance(value, (int, float)) or not math.isfinite(value):
            monthly_values.append(None)
        else:
            monthly_values.append(float(value))

    if any(value is None for value in monthly_values):
        return None
    return sum(monthly_values) / 6


def build_seasonal_record(
    monthly_records: list[dict[str, Any]],
    code: str,
    target_year: int,
) -> dict[str, Any]:
    """Build the April-September seasonal hydrograph record for one station."""
    season_start = dt.date(target_year, 4, 1)
    return {
        "horizon_type": "season",
        "code": str(code),
        "date": season_start.isoformat(),
        "day_of_year": season_start.timetuple().tm_yday,
        "horizon_value": 1,
        "horizon_in_year": 1,
        "norm": _json_safe(_seasonal_field_mean(monthly_records, "norm")),
        "previous": _json_safe(fl.round_3sf(_seasonal_field_mean(monthly_records, "previous"))),
        "current": _json_safe(fl.round_3sf(_seasonal_field_mean(monthly_records, "current"))),
    }


def write_station_seasonal_hydrograph(
    code: str,
    monthly_records: list[dict[str, Any]],
    client: Any,
    target_year: int,
    today: dt.date,
) -> dict[str, Any]:
    """Build and write one seasonal hydrograph record for one station."""
    logger.info(
        "Building long-horizon seasonal hydrograph for station %s using today=%s",
        code,
        today.isoformat(),
    )
    record = build_seasonal_record(
        monthly_records=monthly_records,
        code=code,
        target_year=target_year,
    )
    client.write_hydrograph([record])
    logger.info("Wrote seasonal hydrograph record for station %s", code)
    return record


def _quarterly_field_mean(
    monthly_records: list[dict[str, Any]],
    quarter: int,
    field: str,
) -> float | None:
    monthly_by_value = {record.get("horizon_value"): record for record in monthly_records}
    monthly_values = []
    for month in QUARTER_MONTHS[quarter]:
        record = monthly_by_value.get(month, {})
        value = record.get(field)
        if value is None or not isinstance(value, (int, float)) or not math.isfinite(value):
            monthly_values.append(None)
        else:
            monthly_values.append(float(value))

    if any(value is None for value in monthly_values):
        return None
    return sum(monthly_values) / 3


def build_quarterly_records(
    monthly_records: list[dict[str, Any]],
    code: str,
    target_year: int,
) -> list[dict[str, Any]]:
    """Build quarterly records with leap-aware start-date DOY and all-or-nothing means."""
    records = []
    for quarter in range(1, 5):
        start_month = QUARTER_MONTHS[quarter][0]
        quarter_start = dt.date(target_year, start_month, 1)
        records.append(
            {
                "horizon_type": "quarter",
                "code": str(code),
                "date": quarter_start.isoformat(),
                "day_of_year": quarter_start.timetuple().tm_yday,
                "horizon_value": quarter,
                "horizon_in_year": quarter,
                "norm": _json_safe(_quarterly_field_mean(monthly_records, quarter, "norm")),
                "previous": _json_safe(
                    fl.round_3sf(_quarterly_field_mean(monthly_records, quarter, "previous"))
                ),
                "current": _json_safe(
                    fl.round_3sf(_quarterly_field_mean(monthly_records, quarter, "current"))
                ),
            }
        )
    return records


def write_station_quarterly_hydrograph(
    code: str,
    monthly_records: list[dict[str, Any]],
    client: Any,
    target_year: int,
    today: dt.date,
) -> list[dict[str, Any]]:
    """Build and write quarterly hydrograph records for one station."""
    logger.info(
        "Building long-horizon quarterly hydrograph for station %s using today=%s",
        code,
        today.isoformat(),
    )
    records = build_quarterly_records(
        monthly_records=monthly_records,
        code=code,
        target_year=target_year,
    )
    client.write_hydrograph(records)
    logger.info("Wrote %d quarterly hydrograph records for station %s", len(records), code)
    return records


def write_long_horizon_hydrograph(
    codes: Iterable[str],
    iehhf_sdk: Any,
    client: Any,
    target_year: int,
    today: dt.date,
) -> list[dict[str, Any]]:
    """Build and write monthly hydrograph records for all supplied stations."""
    all_records = _LongHorizonWriteResult()
    for code in codes:
        code_str = str(code)
        all_records.attempted_station_codes.append(code_str)
        try:
            monthly_result = write_station_monthly_hydrograph(
                code=code_str,
                iehhf_sdk=iehhf_sdk,
                client=client,
                target_year=target_year,
                today=today,
            )
            if monthly_result.status is LongHorizonStationWriteStatus.SDK_FAILED:
                all_records.station_statuses.append((code_str, monthly_result.status))
                all_records.attempted_station_codes.pop()
                logger.info(
                    "Skipping seasonal hydrograph for station %s without monthly records",
                    code,
                )
                continue
            monthly_records = monthly_result.records
            all_records.extend(monthly_records)
            all_records.append(
                write_station_seasonal_hydrograph(
                    code=code_str,
                    monthly_records=monthly_records,
                    client=client,
                    target_year=target_year,
                    today=today,
                )
            )
            all_records.extend(
                write_station_quarterly_hydrograph(
                    code=code_str,
                    monthly_records=monthly_records,
                    client=client,
                    target_year=target_year,
                    today=today,
                )
            )
            all_records.completed_station_codes.append(code_str)
            all_records.station_statuses.append((code_str, monthly_result.status))
        except _API_READ_WRITE_ERRORS as exc:
            all_records.failed_station_codes.append(code_str)
            all_records.station_statuses.append(
                (code_str, LongHorizonStationWriteStatus.API_FAILED)
            )
            logger.debug(
                "Long-horizon hydrograph API read/write failed for station %s; preserving "
                "any records already written for this station and continuing. Error: %s: %s",
                code_str,
                type(exc).__name__,
                exc,
            )
            continue

    if all_records.failed_station_codes:
        logger.warning(
            "Long-horizon hydrograph API read/write failures for %d/%d attempted station(s).",
            len(all_records.failed_station_codes),
            len(all_records.attempted_station_codes),
        )
    return all_records


def _summarize_long_horizon_station_statuses(
    records: Any,
) -> LongHorizonRunSummary:
    """Count long-horizon station write statuses from the writer result metadata."""
    status_counts = {status: 0 for status in LongHorizonStationWriteStatus}
    station_statuses = getattr(records, "station_statuses", [])
    for _code, status in station_statuses:
        status_counts[status] += 1
    return LongHorizonRunSummary(
        status_counts=status_counts,
        total_attempted=len(station_statuses),
    )


def _exit_code_for_long_horizon_summary(summary: LongHorizonRunSummary) -> int:
    """Return the terminal exit code for the station-status summary."""
    if summary.status_counts[LongHorizonStationWriteStatus.SDK_FAILED] >= 1:
        return 4
    if summary.status_counts[LongHorizonStationWriteStatus.API_FAILED] >= 1:
        return 5
    return 0


def _degraded_long_horizon_summary_line(summary: LongHorizonRunSummary) -> str | None:
    n_absent = summary.status_counts[LongHorizonStationWriteStatus.NORM_ABSENT]
    if n_absent == 0:
        return None
    n_total = summary.total_attempted
    return (
        f"DEGRADED: monthly discharge norms unavailable for {n_absent}/{n_total} stations; "
        "observed runoff written; norm and percent-of-norm unavailable."
    )


def _log_degraded_long_horizon_summary(summary: LongHorizonRunSummary) -> None:
    """Emit the counts-only degraded-success warning when monthly norms were absent."""
    degraded_line = _degraded_long_horizon_summary_line(summary)
    if degraded_line is not None:
        logger.warning("%s", degraded_line)


def _format_long_horizon_run_summary_artifact(summary: LongHorizonRunSummary) -> str:
    """Format the counts-only maintenance summary block captured by the yearly service log."""
    degraded_line = _degraded_long_horizon_summary_line(summary)
    lines = [
        "LONG-HORIZON RUN SUMMARY",
        f"total_attempted={summary.total_attempted}",
        f"written={summary.status_counts[LongHorizonStationWriteStatus.WRITTEN]}",
        f"norm_absent={summary.status_counts[LongHorizonStationWriteStatus.NORM_ABSENT]}",
        f"sdk_failed={summary.status_counts[LongHorizonStationWriteStatus.SDK_FAILED]}",
        f"api_failed={summary.status_counts[LongHorizonStationWriteStatus.API_FAILED]}",
    ]
    if degraded_line is not None:
        lines.append(degraded_line)
    lines.append("END LONG-HORIZON RUN SUMMARY")
    return "\n".join(lines)


def resolve_sdk_station_codes(sdk: Any) -> list[str]:
    """Resolve the SDK-only station set using the monthly-norms path."""
    _fc_sites, site_codes, _site_ids = get_all_forecast_sites_from_HF_SDK(sdk)
    if site_codes is None:
        site_codes = []
    manual_set = set(_get_manual_site_codes())
    sdk_only_codes = [str(code) for code in site_codes if code not in manual_set]
    logger.info("Resolved %d SDK-only station(s)", len(sdk_only_codes))
    return sdk_only_codes


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sync_long_horizon_hydrograph.py",
        description=(
            "Fetch monthly discharge norms and local daily runoff aggregates, "
            "then write monthly long-horizon hydrograph rows."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--target-year",
        type=int,
        default=None,
        metavar="YEAR",
        help="Year to stamp on monthly rows. Defaults to the current calendar year.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Resolve stations and exit without writing hydrograph rows.",
    )
    return parser


def _get_preprocessing_client() -> Any:
    if not SAPPHIRE_API_AVAILABLE or SapphirePreprocessingClient is None:
        raise RuntimeError("sapphire-api-client not installed")
    if os.getenv("SAPPHIRE_API_ENABLED", "true").lower() != "true":
        raise RuntimeError("SAPPHIRE API writing disabled via SAPPHIRE_API_ENABLED=false")
    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    client = SapphirePreprocessingClient(base_url=api_url)
    if not client.readiness_check():
        raise RuntimeError(f"SAPPHIRE API at {api_url} is not ready")
    return client


def main() -> None:
    """Entry point for long-horizon runoff hydrograph ingestion.

    Exit codes:
        0  Success, including degraded success with missing monthly norms.
        1  API setup/runtime error.
        2  No SDK sites/no records.
        3  Unexpected exception.
        4  >=1 SDK norm lookup failure.
        5  >=1 API read/write failure.
    """
    parser = _build_parser()
    args = parser.parse_args()
    today = dt.datetime.now().date()
    target_year = args.target_year if args.target_year is not None else today.year

    try:
        sl.load_environment()
        sdk = IEasyHydroHFSDK()
        codes = resolve_sdk_station_codes(sdk)
        logger.info("Target year for long-horizon monthly hydrograph rows: %d", target_year)

        if args.dry_run:
            print(f"DRY-RUN - sdk_only_codes: {codes}")
            print(f"DRY-RUN - target_year: {target_year}")
            sys.exit(0)

        if not codes:
            logger.error("No SDK sites remain after filtering - nothing to write.")
            sys.exit(2)

        client = _get_preprocessing_client()
        records = write_long_horizon_hydrograph(
            codes=codes,
            iehhf_sdk=sdk,
            client=client,
            target_year=target_year,
            today=today,
        )
        run_summary = _summarize_long_horizon_station_statuses(records)
        _log_degraded_long_horizon_summary(run_summary)
        print(_format_long_horizon_run_summary_artifact(run_summary))
        exit_code = _exit_code_for_long_horizon_summary(run_summary)
        if exit_code != 0:
            if exit_code == 4:
                logger.error(
                    "Long-horizon monthly hydrograph ingestion completed with %d SDK norm "
                    "lookup failure(s).",
                    run_summary.status_counts[LongHorizonStationWriteStatus.SDK_FAILED],
                )
            else:
                logger.error(
                    "Long-horizon monthly hydrograph ingestion completed with %d API "
                    "read/write failure(s).",
                    run_summary.status_counts[LongHorizonStationWriteStatus.API_FAILED],
                )
            sys.exit(exit_code)
        if not records:
            logger.error("No monthly hydrograph records produced - nothing to write.")
            sys.exit(2)
        logger.info("Long-horizon monthly hydrograph ingestion wrote %d records.", len(records))
        sys.exit(exit_code)

    except RuntimeError as exc:
        logger.error("API error during long-horizon monthly hydrograph ingestion: %s", exc)
        sys.exit(1)
    except SystemExit:
        raise
    except Exception as exc:
        logger.exception(
            "Unexpected error during long-horizon monthly hydrograph ingestion: %s",
            exc,
        )
        sys.exit(3)


if __name__ == "__main__":
    main()
