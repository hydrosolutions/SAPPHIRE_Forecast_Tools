"""Yearly snow norm recalculation.

Computes climatological daily norms from the SAPPHIRE preprocessing API
using ``dg_utils.calculate_snow_norms_from_api()``, and writes full-year
norm records back to the API.

Designed to run once a year (e.g., end of August via cron) after the
snow reanalysis files have been updated.

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


def recalculate_norms(
    snow_path: str,
    variables: list[str],
    hru_codes: list[str],
    year: int,
    env_overrides: dict | None = None,
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

    Returns:
        True if norms were successfully written, False otherwise.
    """
    # Apply env overrides (for testing)
    old_env = {}
    if env_overrides:
        for k, v in env_overrides.items():
            old_env[k] = os.environ.get(k)
            os.environ[k] = v

    try:
        return _recalculate_norms_impl(snow_path, variables, hru_codes, year)
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

    # 3. Build date range for the target year
    is_leap = dg_utils.is_leap_year(year)
    n_days = 366 if is_leap else 365
    date_range = pd.date_range(start=f"{year}-01-01", periods=n_days, freq="D")

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

            # Read existing target-year records from API to preserve values
            start_str = f"{year}-01-01"
            end_str = f"{year}-12-31"
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
                logger.warning(
                    "Could not read existing snow records for %s/%s: %s",
                    snow_type,
                    code,
                    e,
                )

            # Read prior-year records for calendar-date `previous` lookup
            prior_year = year - 1
            prior_start_str = f"{prior_year}-01-01"
            prior_end_str = f"{prior_year}-12-31"
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
                logger.warning(
                    "Could not read prior-year snow records for %s/%s (year %d): %s. "
                    "All 'previous' values will be NaN.",
                    snow_type,
                    code,
                    prior_year,
                    e,
                )

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

                # Compute `previous` via calendar-date alignment to year-1
                target_date = _date(year, dt.month, dt.day)
                try:
                    prior_date = _date(prior_year, target_date.month, target_date.day)
                    prior_date_str = prior_date.strftime("%Y-%m-%d")
                    prior_row = prior_existing.get(prior_date_str)
                    if prior_row is not None:
                        pv = prior_row.get("value") if hasattr(prior_row, "get") else None
                        previous_val = float(pv) if pv is not None and pd.notna(pv) else math.nan
                    else:
                        previous_val = math.nan
                except ValueError:
                    # e.g. 2024-02-29 → 2023-02-29 does not exist
                    previous_val = math.nan

                # Get climatology stats for this DOY
                stats = stat_lookup.get(doy, {})

                record = {
                    "snow_type": snow_type.upper(),
                    "code": str(code),
                    "date": date_str,
                    "value": value,
                    "norm": norm_val,
                    "count": stats.get("count"),
                    "mean": stats.get("mean"),
                    "std": stats.get("std"),
                    "min": stats.get("min"),
                    "max": stats.get("max"),
                    "q05": stats.get("q05"),
                    "q25": stats.get("q25"),
                    "q50": stats.get("q50"),
                    "q75": stats.get("q75"),
                    "q95": stats.get("q95"),
                    "previous": previous_val,
                    "current": current_val,
                }
                record.update(band_values)
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


def main():
    """Entry point for standalone execution."""
    import setup_library as sl

    sl.load_environment()

    # Read configuration from environment
    intermediate_path = os.getenv("ieasyforecast_intermediate_data_path", "")
    snow_output = os.getenv("ieasyhydroforecast_OUTPUT_PATH_SNOW", "snow")
    snow_path = os.path.join(intermediate_path, snow_output)

    variables_str = os.getenv("ieasyhydroforecast_SNOW_VARS", "SWE,HS")
    variables = [v.strip() for v in variables_str.split(",") if v.strip()]

    hru_str = os.getenv("ieasyhydroforecast_HRU_SNOW_DATA", "")
    hru_codes = [h.strip() for h in hru_str.split(",") if h.strip()]

    if not hru_codes:
        logger.error("No HRU codes configured for snow data")
        sys.exit(1)

    # Use current year by default
    from datetime import date as date_type

    year = date_type.today().year

    logger.info("Starting yearly snow norm recalculation for year %d", year)
    logger.info("Snow path: %s", snow_path)
    logger.info("Variables: %s", variables)
    logger.info("HRU codes: %s", hru_codes)

    success = recalculate_norms(
        snow_path=snow_path,
        variables=variables,
        hru_codes=hru_codes,
        year=year,
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
