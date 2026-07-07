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
import os
import sys
from collections.abc import Iterable
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


class _LongHorizonWriteResult(list):
    def __init__(self, records: Iterable[dict[str, Any]] = ()) -> None:
        super().__init__(records)
        self.attempted_station_codes: list[str] = []
        self.completed_station_codes: list[str] = []
        self.failed_station_codes: list[str] = []


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
    if isinstance(value, (int, float)) and not math.isfinite(value):
        return None
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


def write_station_monthly_hydrograph(
    code: str,
    iehhf_sdk: Any,
    client: Any,
    target_year: int,
    today: dt.date,
) -> list[dict[str, Any]]:
    """Build and write monthly hydrograph records for one station."""
    logger.info("Building long-horizon monthly hydrograph for station %s", code)
    try:
        norms = iehhf_sdk.get_norm_for_site(code, "discharge", norm_period="m")
    except Exception as exc:
        logger.warning(
            "write_station_monthly_hydrograph: SDK call failed for site %s, skipping. "
            "Error: %s: %s",
            code,
            type(exc).__name__,
            exc,
        )
        return []

    if len(norms) != 12:
        logger.warning(
            "write_station_monthly_hydrograph: expected 12 norm values for site %s, "
            "got %d - skipping this site.",
            code,
            len(norms),
        )
        return []

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
    return records


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
            monthly_records = write_station_monthly_hydrograph(
                code=code_str,
                iehhf_sdk=iehhf_sdk,
                client=client,
                target_year=target_year,
                today=today,
            )
            if not monthly_records:
                all_records.attempted_station_codes.pop()
                logger.info(
                    "Skipping seasonal hydrograph for station %s without monthly records",
                    code,
                )
                continue
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
        except _API_READ_WRITE_ERRORS as exc:
            all_records.failed_station_codes.append(code_str)
            logger.warning(
                "Long-horizon hydrograph API read/write failed for station %s; preserving "
                "any records already written for this station and continuing. Error: %s: %s",
                code_str,
                type(exc).__name__,
                exc,
            )
            continue

    if all_records.failed_station_codes:
        logger.warning(
            "Long-horizon hydrograph API read/write failures for %d/%d attempted station(s): %s",
            len(all_records.failed_station_codes),
            len(all_records.attempted_station_codes),
            ", ".join(all_records.failed_station_codes),
        )
    return all_records


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
    parser = _build_parser()
    args = parser.parse_args()
    today = dt.date.today()
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
        attempted_station_codes = getattr(records, "attempted_station_codes", [])
        completed_station_codes = getattr(records, "completed_station_codes", [])
        if len(completed_station_codes) == 0 and len(attempted_station_codes) > 0:
            logger.error(
                "All %d attempted station(s) had long-horizon hydrograph API read/write "
                "failures; exiting 2 after preserving any successful partial writes.",
                len(attempted_station_codes),
            )
            sys.exit(2)
        if not records:
            logger.error("No monthly hydrograph records produced - nothing to write.")
            sys.exit(2)
        logger.info("Long-horizon monthly hydrograph ingestion wrote %d records.", len(records))
        sys.exit(0)

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
