"""Yearly snow norm recalculation.

Computes climatological daily norms from the SAPPHIRE preprocessing API
using ``dg_utils.calculate_snow_norms_from_api()``, and writes full-year
norm records back to the API.

Designed to run once a year (e.g., end of August via cron) after the
snow reanalysis files have been updated.

Preservation reads (target-year, prior-year, and statistics history)
guard against nulling a full year of stored values on write. If one
of those reads fails, this run raises ``dg_utils.SnowPreservationReadError``
and aborts rather than writing nulls (PREPG-020). There is no durable
API-side replay of what that run would have written — recovery is a
manual maintenance-mode re-run of this script once the underlying API
problem is resolved, not something a later scheduled run will pick up
on its own.

Usage (standalone)::

    uv run recalculate_snow_norms.py

Usage (from code / tests)::

    from recalculate_snow_norms import recalculate_norms
    recalculate_norms(
        snow_path="/path/to/snow",
        variables=["SWE", "HS"],
        hru_codes=["HRU_SNOW01"],
        year=2024,
    )
"""

import logging
import math
import os
import sys
from datetime import date as _date

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "iEasyHydroForecast"))

import dg_utils

logger = logging.getLogger(__name__)


def _snow_record_range(
    year: int, start_month: int, start_day: int
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Resolve the date range of snow norm/stat records to write (PREPG-022).

    The range is Jan-1-anchored on ``year`` and extends through the end
    of the hydrological display season that starts on
    ``start_month``/``start_day`` of that year, so it is always a
    strict superset of the plain calendar year.

    Args:
        year: Target year (the run's anchor year).
        start_month: Hydrological display-window start month (1-12).
        start_day: Hydrological display-window start day.

    Returns:
        ``(start, end)`` as inclusive ``pd.Timestamp`` bounds. When
        ``(start_month, start_day) == (1, 1)`` this is exactly
        ``({year}-01-01, {year}-12-31)`` — byte-identical to the
        previous calendar-year-only behavior. Otherwise ``end`` is one
        day before ``start_month``/``start_day`` of ``year + 1``.

    Note:
        ``start_month``/``start_day`` must form a calendar-valid
        month/day (e.g. not ``(2, 30)``); this function does not
        validate that itself. ``main()`` enforces validity via
        ``_date(2001, m, d)`` before calling in. A direct caller that
        skips that guard and passes an invalid pair -- notably
        ``(2, 29)`` -- reaches ``pd.Timestamp(year=year + 1, ...)``
        above and gets an uncaught ``ValueError`` from ``pd.Timestamp``,
        and only in years where ``year + 1`` is not a leap year, so
        the same call can succeed one year and raise the next.
    """
    start = pd.Timestamp(year=year, month=1, day=1)
    if (start_month, start_day) == (1, 1):
        end = pd.Timestamp(year=year, month=12, day=31)
    else:
        end = pd.Timestamp(year=year + 1, month=start_month, day=start_day) - pd.Timedelta(days=1)
    return start, end


def recalculate_norms(
    snow_path: str,
    variables: list[str],
    hru_codes: list[str],
    year: int,
    env_overrides: dict | None = None,
    display_start_month: int = 1,
    display_start_day: int = 1,
) -> bool:
    """Calculate snow norms from API historical data and write back.

    Args:
        snow_path: Deprecated — no longer used (CSV migration complete).
            Kept for backward compatibility with ``main()`` entry point.
        variables: Snow variable names (e.g., ``["SWE", "HS", "RoF"]``).
        hru_codes: Deprecated — station codes are now discovered from
            API data. Kept for backward compatibility.
        year: Target year to write norms for (all 365/366 days).
        env_overrides: Optional dict of env var overrides for testing
            (e.g., ``{"SAPPHIRE_API_ENABLED": "true"}``).
        display_start_month: Hydrological display-window start month
            (PREPG-022). Defaults to ``1`` — calendar-year behavior,
            byte-identical to before this argument existed.
        display_start_day: Hydrological display-window start day.
            Defaults to ``1``.

    Returns:
        True if norms were successfully written, False otherwise.

    Raises:
        dg_utils.SnowPreservationReadError: A preservation read for a
            code/type (target-year, prior-year, or statistics history)
            failed. Per PREPG-020, this aborts the run instead of
            writing with the affected fields nulled. No record for
            the failing code/type is written. Recovery is a manual
            maintenance-mode re-run once the read problem is fixed —
            there is no durable API replay, and this script does not
            retry.
    """
    # Apply env overrides (for testing)
    old_env = {}
    if env_overrides:
        for k, v in env_overrides.items():
            old_env[k] = os.environ.get(k)
            os.environ[k] = v

    try:
        return _recalculate_norms_impl(
            snow_path,
            variables,
            hru_codes,
            year,
            display_start_month,
            display_start_day,
        )
    finally:
        # Restore env
        if env_overrides:
            for k, v in old_env.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v


def _recalculate_norms_impl(
    snow_path: str,
    variables: list[str],
    hru_codes: list[str],
    year: int,
    display_start_month: int = 1,
    display_start_day: int = 1,
) -> bool:
    """Internal implementation of norm recalculation."""
    # 1. Check API availability and create client
    if not dg_utils.SAPPHIRE_API_AVAILABLE:
        logger.warning("sapphire-api-client not installed, cannot compute or write norms")
        return False

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
    if not api_enabled:
        logger.info("API disabled via SAPPHIRE_API_ENABLED=false")
        return False

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    client = dg_utils.SapphirePreprocessingClient(base_url=api_url)

    if not client.readiness_check():
        logger.warning("API at %s not ready, skipping norm recalculation", api_url)
        return False

    # 2. Compute norms and stats from API data
    logger.info(
        "Calculating snow norms from API for variables %s",
        variables,
    )
    norms_df = dg_utils.calculate_snow_norms_from_api(client, variables)

    if norms_df.empty:
        logger.warning("No snow norms computed — no historical data found in API")
        return False

    logger.info(
        "Computed %d norm entries across %d variables and %d codes",
        len(norms_df),
        norms_df["snow_type"].nunique(),
        norms_df["code"].nunique(),
    )

    logger.info(
        "Calculating snow climatology stats from API for variables %s",
        variables,
    )
    stats_df = dg_utils.calculate_snow_stats_from_api(client, variables, n_years_min=5)
    if not stats_df.empty:
        logger.info(
            "Computed %d stat entries across %d variables and %d codes",
            len(stats_df),
            stats_df["snow_type"].nunique(),
            stats_df["code"].nunique(),
        )
    else:
        logger.warning("No snow stats computed — proceeding with NaN stat fields")

    # 3. Build date range for the target year plus the remainder of the
    # hydrological display season that starts in it (PREPG-022). With
    # display_start_month/day == (1, 1) (the default) this is exactly
    # the plain calendar year, unchanged from before.
    range_start, range_end = _snow_record_range(year, display_start_month, display_start_day)
    date_range = pd.date_range(range_start, range_end, freq="D")
    logger.info(
        "Snow norm/stat record range: %s .. %s (%d days)",
        range_start.strftime("%Y-%m-%d"),
        range_end.strftime("%Y-%m-%d"),
        len(date_range),
    )

    # 4. For each variable+code, build records and write
    any_written = False

    for snow_type in norms_df["snow_type"].unique():
        type_norms = norms_df[norms_df["snow_type"] == snow_type]
        type_stats = (
            stats_df[stats_df["snow_type"] == snow_type] if not stats_df.empty else pd.DataFrame()
        )

        for code in type_norms["code"].unique():
            code_norms = type_norms[type_norms["code"] == code]
            # Build a dayofyear → norm lookup
            norm_lookup = dict(zip(code_norms["dayofyear"], code_norms["norm"], strict=False))

            # Build dayofyear → stats lookup for this code
            stat_lookup: dict[int, dict] = {}
            if not type_stats.empty:
                code_stats = type_stats[type_stats["code"] == code]
                for _, srow in code_stats.iterrows():
                    doy = int(srow["dayofyear"])
                    stat_lookup[doy] = {
                        "count": int(srow["count"]) if pd.notna(srow["count"]) else None,
                        "mean": float(srow["mean"]) if pd.notna(srow["mean"]) else None,
                        "std": float(srow["std"]) if pd.notna(srow["std"]) else None,
                        "min": float(srow["min"]) if pd.notna(srow["min"]) else None,
                        "max": float(srow["max"]) if pd.notna(srow["max"]) else None,
                        "q05": float(srow["q05"]) if pd.notna(srow["q05"]) else None,
                        "q25": float(srow["q25"]) if pd.notna(srow["q25"]) else None,
                        "q50": float(srow["q50"]) if pd.notna(srow["q50"]) else None,
                        "q75": float(srow["q75"]) if pd.notna(srow["q75"]) else None,
                        "q95": float(srow["q95"]) if pd.notna(srow["q95"]) else None,
                    }

            # Read existing target-range records from API to preserve
            # values. Bounds match date_range exactly (PREPG-022) — if
            # this read is ever narrower than the write range, every
            # date past its end gets written with value/current/bands
            # silently nulled.
            start_str = range_start.strftime("%Y-%m-%d")
            end_str = range_end.strftime("%Y-%m-%d")
            existing = {}
            try:
                api_df = client.read_snow(
                    snow_type=snow_type.upper(),
                    code=str(code),
                    start_date=start_str,
                    end_date=end_str,
                    limit=100000,
                )
                if not api_df.empty:
                    for _, row in api_df.iterrows():
                        d = pd.to_datetime(row["date"]).strftime("%Y-%m-%d")
                        existing[d] = row
            except Exception as e:
                # Target-year records back value/current/bands for this
                # code/type. A failed read must not be silently treated
                # as "nothing stored" — that would write null over a
                # full year of them. Abort this code/type (PREPG-020).
                raise dg_utils.SnowPreservationReadError(
                    f"Could not read existing target-year snow records for "
                    f"{snow_type}/{code} (year {year}): {e}. Refusing to write "
                    "records that would null the stored value/current/elevation "
                    "band fields."
                ) from e

            # Read prior-year records for calendar-date `previous` lookup.
            # The write range can now span two calendar years
            # (PREPG-022), so a single fixed `year - 1` read is no
            # longer enough — a date in `range_end.year` needs
            # `range_end.year - 1`'s data too. Cover the whole prior
            # span in one read, keyed per-date off `dt.year - 1` below.
            prior_start_str = f"{range_start.year - 1}-01-01"
            prior_end_str = f"{range_end.year - 1}-12-31"
            prior_existing: dict[str, object] = {}
            try:
                prior_df = client.read_snow(
                    snow_type=snow_type.upper(),
                    code=str(code),
                    start_date=prior_start_str,
                    end_date=prior_end_str,
                    limit=100000,
                )
                if not prior_df.empty:
                    for _, row in prior_df.iterrows():
                        d = pd.to_datetime(row["date"]).strftime("%Y-%m-%d")
                        prior_existing[d] = row
            except Exception as e:
                # A failed prior-year read must not be treated as "no
                # prior data" — that would null the stored 'previous'
                # field for every day of this code/type. Abort this
                # code/type instead (PREPG-020).
                raise dg_utils.SnowPreservationReadError(
                    f"Could not read prior-year snow records for {snow_type}/{code} "
                    f"({prior_start_str} .. {prior_end_str}): {e}. Refusing to write records that "
                    "would null the stored 'previous' field."
                ) from e

            # Build API records for each day of the year
            records = []
            for dt in date_range:
                date_str = dt.strftime("%Y-%m-%d")
                doy = dt.dayofyear

                norm_val = norm_lookup.get(doy)
                if norm_val is not None and pd.notna(norm_val):
                    norm_val = round(float(norm_val), 3)
                else:
                    norm_val = None

                # Preserve existing value/bands from API
                ex = existing.get(date_str)
                value = None
                band_values = {}
                if ex is not None:
                    v = ex.get("value")
                    if pd.notna(v):
                        value = float(v)
                    for i in range(1, 15):
                        bv = ex.get(f"value{i}")
                        if pd.notna(bv) if bv is not None else False:
                            band_values[f"value{i}"] = float(bv)

                # Compute `current` from the target-year row's own value
                current_val = value  # same as the preserved value field

                # Compute `previous` via calendar-date alignment to the
                # date's own prior year (dt.year - 1), not a single
                # fixed `year - 1` (PREPG-022: the write range can span
                # two calendar years, e.g. a 09-01 display window).
                try:
                    prior_date = _date(dt.year - 1, dt.month, dt.day)
                    prior_date_str = prior_date.strftime("%Y-%m-%d")
                    prior_row = prior_existing.get(prior_date_str)
                    if prior_row is not None:
                        pv = prior_row.get("value") if hasattr(prior_row, "get") else None
                        previous_val = float(pv) if pv is not None and pd.notna(pv) else None
                    else:
                        previous_val = None
                except ValueError:
                    # e.g. 2024-02-29 → 2023-02-29 does not exist
                    previous_val = None

                # Get climatology stats for this DOY
                stats = stat_lookup.get(doy, {})

                record = {
                    "snow_type": snow_type.upper(),
                    "code": str(code),
                    "date": date_str,
                    "value": _json_safe(value),
                    "norm": _json_safe(norm_val),
                    "count": _json_safe(stats.get("count")),
                    "mean": _json_safe(stats.get("mean")),
                    "std": _json_safe(stats.get("std")),
                    "min": _json_safe(stats.get("min")),
                    "max": _json_safe(stats.get("max")),
                    "q05": _json_safe(stats.get("q05")),
                    "q25": _json_safe(stats.get("q25")),
                    "q50": _json_safe(stats.get("q50")),
                    "q75": _json_safe(stats.get("q75")),
                    "q95": _json_safe(stats.get("q95")),
                    "previous": _json_safe(previous_val),
                    "current": _json_safe(current_val),
                }
                record.update({key: _json_safe(value) for key, value in band_values.items()})
                records.append(record)

            # Write to API — isolate per-station write failures
            if records:
                try:
                    count = client.write_snow(records)
                    logger.info(
                        "Wrote %d stat+norm records for %s/%s (year %d)",
                        count,
                        snow_type,
                        code,
                        year,
                    )
                    any_written = True
                except Exception as e:
                    logger.warning(
                        "Failed to write stat+norm records for %s/%s (year %d): %s — "
                        "skipping this station, continuing with others.",
                        snow_type,
                        code,
                        year,
                        e,
                    )

    return any_written


def _json_safe(value):
    """Return value if finite numeric; return None for NaN/inf/None."""
    if value is None:
        return None
    try:
        if not math.isfinite(float(value)):
            return None
    except (TypeError, ValueError):
        return None
    return value


def _parse_snow_vars(value: str | None) -> list[str]:
    """Parse and normalize the SNOW_VARS env var."""
    if not value:
        return []

    seen: set[str] = set()
    result: list[str] = []
    for token in value.split(","):
        norm = token.strip().upper()
        if norm and norm not in seen:
            seen.add(norm)
            result.append(norm)
    return result


def main():
    """Entry point for standalone execution."""
    import setup_library as sl

    sl.load_environment()

    # Read configuration from environment
    intermediate_path = os.getenv("ieasyforecast_intermediate_data_path", "")
    snow_output = os.getenv("ieasyhydroforecast_OUTPUT_PATH_SNOW", "snow")
    snow_path = os.path.join(intermediate_path, snow_output)

    variables_str = os.getenv("ieasyhydroforecast_SNOW_VARS", "SWE,HS")
    variables = _parse_snow_vars(variables_str)

    hru_str = os.getenv("ieasyhydroforecast_HRU_SNOW_DATA", "")
    hru_codes = [h.strip() for h in hru_str.split(",") if h.strip()]

    if not hru_codes:
        logger.error("No HRU codes configured for snow data")
        sys.exit(1)

    # Resolve the hydrological display-window start (PREPG-022): the
    # written record range extends from 1 January of the target year
    # through the end of the season that starts on this date. Same
    # tolerant "MM-DD" parsing and (1, 1) fallback as
    # apps/forecast_dashboard/dashboard/config.py (absent/invalid/
    # unparseable MM-DD, or 02-29, all fall back to (1, 1)).
    snow_display_start = os.getenv("ieasyhydroforecast_SNOW_DISPLAY_START_MMDD", "01-01")
    try:
        display_start_month, display_start_day = (
            int(snow_display_start[:2]),
            int(snow_display_start[3:]),
        )
        _date(2001, display_start_month, display_start_day)  # validate range, reject Feb 29
    except (ValueError, IndexError):
        display_start_month, display_start_day = 1, 1

    # Use current year by default, unless explicitly overridden for backfills
    from datetime import date as date_type

    year_env = os.getenv("ieasyhydroforecast_SNOW_RECALC_YEAR")
    if year_env:
        try:
            year = int(year_env)
        except ValueError as e:
            raise SystemExit(
                f"ieasyhydroforecast_SNOW_RECALC_YEAR must be an integer, got {year_env!r}"
            ) from e
    else:
        year = date_type.today().year

    logger.info("Starting yearly snow norm recalculation for year %d", year)
    logger.info("Snow path: %s", snow_path)
    logger.info("Variables: %s", variables)
    logger.info("HRU codes: %s", hru_codes)
    logger.info("Snow display window start: %02d-%02d", display_start_month, display_start_day)

    success = recalculate_norms(
        snow_path=snow_path,
        variables=variables,
        hru_codes=hru_codes,
        year=year,
        display_start_month=display_start_month,
        display_start_day=display_start_day,
    )

    if success:
        logger.info("Snow norm recalculation completed successfully")
        print("Snow norm recalculation completed successfully")
    else:
        logger.error("Snow norm recalculation failed or no data found")
        print("Snow norm recalculation failed or no data found")
        sys.exit(1)


if __name__ == "__main__":
    main()
