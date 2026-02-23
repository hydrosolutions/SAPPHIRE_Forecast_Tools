"""Backfill new stations — detect gaps and fill meteo/snow history.

Workflow:
1. Read configured HRU codes from env vars
2. Query API coverage endpoint (/meteo/coverage, /snow/coverage)
3. Read station codes from existing CSVs
4. Detect gaps: new stations, stale stations, up-to-date stations
5. For new/stale meteo stations: read CSV or download via DG, write to API
6. For new/stale snow stations: read CSV or download via DG, write to API
7. Log summary

Usage:
    SAPPHIRE_SYNC_MODE=initial uv run backfill_new_stations.py
"""

import logging
import os
import sys
from datetime import date, timedelta

import pandas as pd
import requests

# Local libraries
script_dir = os.path.dirname(os.path.abspath(__file__))
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')
sys.path.append(forecast_dir)

import setup_library as sl  # noqa: E402

# SAPPHIRE API client
try:
    from sapphire_api_client import (
        SapphirePreprocessingClient,
        SapphireAPIError,
    )
    SAPPHIRE_API_AVAILABLE = True
except ImportError:
    SAPPHIRE_API_AVAILABLE = False
    SapphirePreprocessingClient = None
    SapphireAPIError = Exception

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Coverage helpers
# ------------------------------------------------------------------

def get_meteo_coverage(api_url: str) -> dict[tuple[str, str], date]:
    """Query /meteo/coverage and return {(meteo_type, code): max_date}.

    Falls back to empty dict if the endpoint is unavailable.
    """
    url = f"{api_url}/meteo/coverage"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return {
            (r["meteo_type"], r["code"]): date.fromisoformat(r["max_date"])
            for r in resp.json()
        }
    except Exception as exc:
        logger.warning("Could not query meteo coverage: %s", exc)
        return {}


def get_snow_coverage(api_url: str) -> dict[tuple[str, str], date]:
    """Query /snow/coverage and return {(snow_type, code): max_date}.

    Falls back to empty dict if the endpoint is unavailable.
    """
    url = f"{api_url}/snow/coverage"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return {
            (r["snow_type"], r["code"]): date.fromisoformat(r["max_date"])
            for r in resp.json()
        }
    except Exception as exc:
        logger.warning("Could not query snow coverage: %s", exc)
        return {}


# ------------------------------------------------------------------
# CSV station code extraction
# ------------------------------------------------------------------

def extract_meteo_codes_from_csv(csv_path: str) -> set[str]:
    """Read a reanalysis CSV and return unique station codes."""
    if not os.path.exists(csv_path):
        return set()
    try:
        df = pd.read_csv(csv_path, usecols=["code"])
        return set(df["code"].astype(str).unique())
    except Exception as exc:
        logger.warning("Error reading %s: %s", csv_path, exc)
        return set()


def extract_snow_codes_from_csv(csv_path: str) -> set[str]:
    """Read a snow CSV and return unique station codes."""
    if not os.path.exists(csv_path):
        return set()
    try:
        df = pd.read_csv(csv_path, usecols=["code"])
        return set(df["code"].astype(str).unique())
    except Exception as exc:
        logger.warning("Error reading %s: %s", csv_path, exc)
        return set()


# ------------------------------------------------------------------
# Backfill writers
# ------------------------------------------------------------------

def backfill_meteo_from_csv(
    csv_path: str,
    meteo_type: str,
    codes: set[str],
    client: "SapphirePreprocessingClient",
    max_date_by_code: dict[str, date | None],
) -> int:
    """Write missing meteo data from a reanalysis CSV to the API.

    Args:
        csv_path: Path to the reanalysis CSV.
        meteo_type: 'T' or 'P'.
        codes: Station codes to backfill.
        client: Preprocessing API client.
        max_date_by_code: {code: max_date_in_api} or None for new.

    Returns:
        Number of records written.
    """
    if not os.path.exists(csv_path):
        logger.warning("CSV not found: %s", csv_path)
        return 0

    df = pd.read_csv(csv_path)
    df["date"] = pd.to_datetime(df["date"])
    df["code"] = df["code"].astype(str)

    # Filter to requested codes
    df = df[df["code"].isin(codes)]
    if df.empty:
        return 0

    # For each code, only write data after the API's max_date
    frames = []
    for code in codes:
        code_df = df[df["code"] == code]
        existing_max = max_date_by_code.get(code)
        if existing_max is not None:
            code_df = code_df[
                code_df["date"] > pd.Timestamp(existing_max)
            ]
        frames.append(code_df)

    data_to_write = pd.concat(frames, ignore_index=True)
    if data_to_write.empty:
        return 0

    value_col = meteo_type  # 'T' or 'P'
    records = []
    for _, row in data_to_write.iterrows():
        d = row["date"]
        if pd.isna(d):
            continue
        records.append({
            "meteo_type": meteo_type.upper(),
            "code": str(row["code"]),
            "date": d.strftime("%Y-%m-%d"),
            "value": (
                float(row[value_col])
                if value_col in row and pd.notna(row.get(value_col))
                else None
            ),
            "norm": None,
            "day_of_year": d.dayofyear,
        })

    if not records:
        return 0

    count = client.write_meteo(records)
    logger.info(
        "Backfilled %d meteo records (%s) for codes %s",
        count, meteo_type, codes,
    )
    return count


def backfill_snow_from_csv(
    csv_path: str,
    snow_type: str,
    codes: set[str],
    client: "SapphirePreprocessingClient",
    max_date_by_code: dict[str, date | None],
) -> int:
    """Write missing snow data from a CSV to the API.

    Args:
        csv_path: Path to the snow CSV.
        snow_type: 'SWE', 'HS', or 'ROF'.
        codes: Station codes to backfill.
        client: Preprocessing API client.
        max_date_by_code: {code: max_date_in_api} or None for new.

    Returns:
        Number of records written.
    """
    if not os.path.exists(csv_path):
        logger.warning("CSV not found: %s", csv_path)
        return 0

    df = pd.read_csv(csv_path)
    df["date"] = pd.to_datetime(df["date"])
    df["code"] = df["code"].astype(str)

    df = df[df["code"].isin(codes)]
    if df.empty:
        return 0

    frames = []
    for code in codes:
        code_df = df[df["code"] == code]
        existing_max = max_date_by_code.get(code)
        if existing_max is not None:
            code_df = code_df[
                code_df["date"] > pd.Timestamp(existing_max)
            ]
        frames.append(code_df)

    data_to_write = pd.concat(frames, ignore_index=True)
    if data_to_write.empty:
        return 0

    # Detect elevation band columns
    value_columns = {}
    main_value_col = (
        snow_type if snow_type in data_to_write.columns else None
    )
    for col in data_to_write.columns:
        if col.startswith(f"{snow_type}_") and col != snow_type:
            try:
                band_num = int(col.split("_")[-1])
                value_columns[band_num] = col
            except ValueError:
                pass

    records = []
    for _, row in data_to_write.iterrows():
        d = row["date"]
        if pd.isna(d):
            continue
        record = {
            "snow_type": snow_type.upper(),
            "code": str(row["code"]),
            "date": d.strftime("%Y-%m-%d"),
            "value": (
                float(row[main_value_col])
                if main_value_col and pd.notna(row.get(main_value_col))
                else None
            ),
            "norm": (
                float(row["norm"])
                if "norm" in row and pd.notna(row.get("norm"))
                else None
            ),
        }
        for band_num, col_name in value_columns.items():
            if band_num <= 14:
                record[f"value{band_num}"] = (
                    float(row[col_name])
                    if pd.notna(row.get(col_name)) else None
                )
        records.append(record)

    if not records:
        return 0

    count = client.write_snow(records)
    logger.info(
        "Backfilled %d snow records (%s) for codes %s",
        count, snow_type, codes,
    )
    return count


# ------------------------------------------------------------------
# Gap detection
# ------------------------------------------------------------------

def detect_meteo_gaps(
    csv_codes: set[str],
    api_coverage: dict[tuple[str, str], date],
    meteo_type: str,
    staleness_days: int = 7,
) -> tuple[set[str], set[str]]:
    """Detect new and stale meteo stations.

    Args:
        csv_codes: Station codes found in CSVs.
        api_coverage: {(meteo_type, code): max_date} from API.
        meteo_type: 'T' or 'P'.
        staleness_days: Codes with max_date older than this are stale.

    Returns:
        (new_codes, stale_codes) — sets of station code strings.
    """
    today = date.today()
    cutoff = today - timedelta(days=staleness_days)

    new_codes = set()
    stale_codes = set()

    for code in csv_codes:
        key = (meteo_type, code)
        if key not in api_coverage:
            new_codes.add(code)
        elif api_coverage[key] < cutoff:
            stale_codes.add(code)

    return new_codes, stale_codes


def detect_snow_gaps(
    csv_codes: set[str],
    api_coverage: dict[tuple[str, str], date],
    snow_type: str,
    staleness_days: int = 7,
) -> tuple[set[str], set[str]]:
    """Detect new and stale snow stations.

    Returns:
        (new_codes, stale_codes) — sets of station code strings.
    """
    today = date.today()
    cutoff = today - timedelta(days=staleness_days)

    new_codes = set()
    stale_codes = set()

    for code in csv_codes:
        key = (snow_type.upper(), code)
        if key not in api_coverage:
            new_codes.add(code)
        elif api_coverage[key] < cutoff:
            stale_codes.add(code)

    return new_codes, stale_codes


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    """Run the backfill pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    logger.info("=" * 60)
    logger.info("backfill_new_stations.py started")
    logger.info("=" * 60)

    sl.load_environment()

    if not SAPPHIRE_API_AVAILABLE:
        logger.error(
            "sapphire-api-client not installed. Cannot run backfill."
        )
        sys.exit(1)

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    client = SapphirePreprocessingClient(base_url=api_url)

    if not client.readiness_check():
        logger.error("SAPPHIRE API at %s is not ready.", api_url)
        sys.exit(1)

    intermediate_data_path = os.getenv(
        "ieasyforecast_intermediate_data_path", ""
    )
    reanalysis_subdir = os.getenv(
        "ieasyhydroforecast_OUTPUT_PATH_REANALYSIS", ""
    )
    snow_subdir = os.getenv(
        "ieasyhydroforecast_OUTPUT_PATH_SNOW", ""
    )

    reanalysis_path = os.path.join(
        intermediate_data_path, reanalysis_subdir
    )
    snow_path = os.path.join(intermediate_data_path, snow_subdir)

    # ----------------------------------------------------------------
    # 1. Query API coverage
    # ----------------------------------------------------------------
    logger.info("Querying API coverage...")
    meteo_cov = get_meteo_coverage(api_url)
    snow_cov = get_snow_coverage(api_url)
    logger.info(
        "API coverage: %d meteo groups, %d snow groups",
        len(meteo_cov), len(snow_cov),
    )

    # ----------------------------------------------------------------
    # 2. Backfill METEO
    # ----------------------------------------------------------------
    hru_str = os.getenv("ieasyhydroforecast_HRU_CONTROL_MEMBER", "")
    meteo_hrus = [h.strip() for h in hru_str.split(",") if h.strip()]

    total_meteo = 0
    for hru in meteo_hrus:
        for meteo_type in ("P", "T"):
            csv_file = os.path.join(
                reanalysis_path, f"{hru}_{meteo_type}_reanalysis.csv"
            )
            csv_codes = extract_meteo_codes_from_csv(csv_file)
            if not csv_codes:
                logger.info(
                    "No codes found in %s, skipping", csv_file
                )
                continue

            new_codes, stale_codes = detect_meteo_gaps(
                csv_codes, meteo_cov, meteo_type
            )
            gap_codes = new_codes | stale_codes
            if not gap_codes:
                logger.info(
                    "Meteo %s HRU %s: all %d codes up to date",
                    meteo_type, hru, len(csv_codes),
                )
                continue

            logger.info(
                "Meteo %s HRU %s: %d new, %d stale out of %d codes",
                meteo_type, hru, len(new_codes), len(stale_codes),
                len(csv_codes),
            )

            max_date_by_code = {}
            for code in gap_codes:
                key = (meteo_type, code)
                max_date_by_code[code] = meteo_cov.get(key)

            try:
                n = backfill_meteo_from_csv(
                    csv_file, meteo_type, gap_codes,
                    client, max_date_by_code,
                )
                total_meteo += n
            except SapphireAPIError as exc:
                logger.error(
                    "Error backfilling meteo %s HRU %s: %s",
                    meteo_type, hru, exc,
                )

    # ----------------------------------------------------------------
    # 3. Backfill SNOW
    # ----------------------------------------------------------------
    snow_hru_str = os.getenv("ieasyhydroforecast_HRU_SNOW_DATA", "")
    snow_hrus = [h.strip() for h in snow_hru_str.split(",") if h.strip()]
    snow_vars_str = os.getenv("ieasyhydroforecast_SNOW_VARS", "")
    snow_vars = [v.strip() for v in snow_vars_str.split(",") if v.strip()]

    total_snow = 0
    for hru in snow_hrus:
        for snow_var in snow_vars:
            csv_file = os.path.join(
                snow_path, snow_var, f"{hru}_{snow_var}.csv"
            )
            csv_codes = extract_snow_codes_from_csv(csv_file)
            if not csv_codes:
                logger.info(
                    "No codes found in %s, skipping", csv_file
                )
                continue

            new_codes, stale_codes = detect_snow_gaps(
                csv_codes, snow_cov, snow_var,
            )
            gap_codes = new_codes | stale_codes
            if not gap_codes:
                logger.info(
                    "Snow %s HRU %s: all %d codes up to date",
                    snow_var, hru, len(csv_codes),
                )
                continue

            logger.info(
                "Snow %s HRU %s: %d new, %d stale out of %d codes",
                snow_var, hru, len(new_codes), len(stale_codes),
                len(csv_codes),
            )

            max_date_by_code = {}
            for code in gap_codes:
                key = (snow_var.upper(), code)
                max_date_by_code[code] = snow_cov.get(key)

            try:
                n = backfill_snow_from_csv(
                    csv_file, snow_var, gap_codes,
                    client, max_date_by_code,
                )
                total_snow += n
            except SapphireAPIError as exc:
                logger.error(
                    "Error backfilling snow %s HRU %s: %s",
                    snow_var, hru, exc,
                )

    # ----------------------------------------------------------------
    # 4. Summary
    # ----------------------------------------------------------------
    logger.info("=" * 60)
    logger.info(
        "Backfill complete: %d meteo records, %d snow records",
        total_meteo, total_snow,
    )
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
