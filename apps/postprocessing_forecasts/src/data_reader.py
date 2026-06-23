"""Read pre-calculated skill metrics, combined forecasts, and monthly
data from API or CSV (deprecated fallback).

Used by the operational and maintenance entry points to avoid
recalculating skill metrics from scratch, by the maintenance entry
point to read combined forecasts for gap detection, and by the yearly
recalculation entry point to read monthly observations and forecasts.
"""

import calendar
import datetime as dt
import logging
import os

import pandas as pd
from src.postprocessing_tools import count_quantile_crossings

from iEasyHydroForecast.long_term_horizon_resolver import quarter_horizon_value

logger = logging.getLogger(__name__)

try:
    from sapphire_api_client.postprocessing import (
        SapphirePostprocessingClient,
    )
    from sapphire_api_client.preprocessing import (
        SapphirePreprocessingClient,
    )

    SAPPHIRE_API_AVAILABLE = True
except ImportError:
    SAPPHIRE_API_AVAILABLE = False


# Ensemble model names to filter from raw API reads (these are derived,
# not raw model output).
_ENSEMBLE_MODELS = frozenset({"EM", "Skilled Mean", "Naive Mean"})

_SEASONAL_FC_COLS = [
    "code",
    "season_year",
    "season_in_year",
    "horizon_value",
    "date",
    "model_short",
    "q05",
    "q10",
    "q25",
    "q50",
    "q75",
    "q90",
    "q95",
    "forecasted_discharge",
    "valid_from",
    "valid_to",
]

_QUARTERLY_FC_COLS = [
    "code",
    "year",
    "quarter_in_year",
    "model_short",
    "q05",
    "q10",
    "q25",
    "q50",
    "q75",
    "q90",
    "q95",
    "forecasted_discharge",
    "valid_from",
    "valid_to",
]


def read_skill_metrics(
    horizon_type: str,
    codes: list[str] | None = None,
) -> pd.DataFrame:
    """Read pre-calculated skill metrics from API (primary) or CSV (fallback).

    Args:
        horizon_type: 'pentad', 'decad', 'month', 'quarter', or 'season'
        codes: Optional list of station codes to filter. When provided,
            only skill metrics for those codes are returned. When None,
            all codes are returned.

    Returns:
        DataFrame with columns: [pentad_in_year|decad_in_year|
        month_in_year|quarter_in_year|season_in_year, code,
        model_short, sdivsigma, nse, delta, accuracy, mae, n_pairs]

    Raises:
        ValueError: If horizon_type is invalid.
    """
    valid = ("pentad", "decad", "month", "quarter", "season")
    if horizon_type not in valid:
        raise ValueError(f"horizon_type must be one of {valid}, got: {horizon_type}")

    if horizon_type == "month":
        return read_monthly_skill_metrics(codes)
    if horizon_type == "quarter":
        return read_quarterly_skill_metrics(codes)
    if horizon_type == "season":
        return read_seasonal_skill_metrics(codes)

    # API-first: try the authoritative source
    df = _read_skill_metrics_api(horizon_type, codes)
    if df is not None and not df.empty:
        logger.info(
            "Read %d skill metric rows from API (%s)",
            len(df),
            horizon_type,
        )
        return df

    # CSV fallback (deprecated): only used when API is unavailable
    logger.info(
        "API skill metrics unavailable for %s, falling back to CSV",
        horizon_type,
    )
    df = _read_skill_metrics_csv(horizon_type, codes)
    if df is not None and not df.empty:
        logger.info(
            "Read %d skill metric rows from CSV (%s)",
            len(df),
            horizon_type,
        )
        return df

    logger.warning("No skill metrics available for %s", horizon_type)
    return pd.DataFrame()


def _read_skill_metrics_csv(
    horizon_type: str,
    codes: list[str] | None = None,
) -> pd.DataFrame | None:
    """Read skill metrics from CSV file.

    Returns None if the file doesn't exist or can't be read.
    """
    intermediate_path = os.getenv("ieasyforecast_intermediate_data_path", "")

    if horizon_type == "pentad":
        filename = os.getenv("ieasyforecast_pentadal_skill_metrics_file", "")
    else:
        filename = os.getenv("ieasyforecast_decadal_skill_metrics_file", "")

    if not intermediate_path or not filename:
        logger.debug("Skill metrics env vars not set for %s", horizon_type)
        return None

    filepath = os.path.join(intermediate_path, filename)
    if not os.path.exists(filepath):
        logger.debug("Skill metrics CSV not found: %s", filepath)
        return None

    try:
        df = pd.read_csv(filepath)
        # Ensure code is string
        if "code" in df.columns:
            df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)
        if codes is not None and not df.empty and "code" in df.columns:
            df = df[df["code"].astype(str).isin(codes)]
        return df
    except Exception as e:
        logger.error("Failed to read skill metrics CSV %s: %s", filepath, e)
        return None


def _read_skill_metrics_api(
    horizon_type: str,
    codes: list[str] | None = None,
) -> pd.DataFrame | None:
    """Read skill metrics from SAPPHIRE postprocessing API.

    Returns None if the API is unavailable or returns no data.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping API read")
        return None

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping API read")
        return None

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Postprocessing API not ready at %s", api_url)
            return None

        # Map internal horizon names to API horizon names
        # Internal uses 'decad', API expects 'decade'
        api_horizon = "decade" if horizon_type == "decad" else horizon_type

        batch_size = 1000
        if codes is not None:
            # Per-code loop: API supports code= but not batch code__in
            frames = []
            for code in codes:
                skip = 0
                while True:
                    df_batch = client.read_skill_metrics(
                        horizon=api_horizon,
                        code=code,
                        skip=skip,
                        limit=batch_size,
                    )
                    if df_batch is None or df_batch.empty:
                        break
                    frames.append(df_batch)
                    if len(df_batch) < batch_size:
                        break
                    skip += batch_size
            if not frames:
                return None
            df = pd.concat(frames, ignore_index=True)
        else:
            # Read all skill metrics for this horizon; paginate if needed
            all_records = []
            skip = 0
            while True:
                df_batch = client.read_skill_metrics(
                    horizon=api_horizon, skip=skip, limit=batch_size
                )
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

            if not all_records:
                return None

            df = pd.concat(all_records, ignore_index=True)

        return _normalize_api_skill_metrics(df, horizon_type)

    except Exception as e:
        logger.error("Failed to read skill metrics from API: %s", e)
        return None


def _normalize_api_skill_metrics(df: pd.DataFrame, horizon_type: str) -> pd.DataFrame:
    """Convert API column names to CSV-compatible column names.

    API returns: horizon_in_year, model_type, code, sdivsigma, nse,
                 delta, accuracy, mae, n_pairs, crps, pbias, kgelf,
                 nse_log
    CSV expects: pentad_in_year|decad_in_year, model_short,
                 code, sdivsigma, nse, delta, accuracy, mae, n_pairs,
                 pbias, kgelf, nse_log
    """
    period_col = "pentad_in_year" if horizon_type == "pentad" else "decad_in_year"

    # Rename API columns
    rename_map = {
        "horizon_in_year": period_col,
        "model_type": "model_short",
    }
    df = df.rename(columns=rename_map)

    # Ensure code is string
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)

    return df


# ===================================================================
# Monthly skill metrics
# ===================================================================


def read_monthly_skill_metrics(
    codes: list[str] | None = None,
) -> pd.DataFrame:
    """Read pre-calculated monthly skill metrics from API or CSV.

    Args:
        codes: Optional list of station codes to filter. When provided,
            only skill metrics for those codes are returned. When None,
            all codes are returned.

    Returns:
        DataFrame with columns: [month_in_year, code, model_short,
        sdivsigma, nse, delta, accuracy, mae, n_pairs]
    """
    # API-first: try the authoritative source
    df = _read_monthly_skill_metrics_api(codes)
    if df is not None and not df.empty:
        logger.info("Read %d monthly skill metric rows from API", len(df))
        return df

    # CSV fallback (deprecated)
    logger.info("API monthly skill metrics unavailable, falling back to CSV")
    df = _read_monthly_skill_metrics_csv(codes)
    if df is not None and not df.empty:
        logger.info("Read %d monthly skill metric rows from CSV", len(df))
        return df

    logger.warning("No monthly skill metrics available")
    return pd.DataFrame()


def _read_monthly_skill_metrics_csv(
    codes: list[str] | None = None,
) -> pd.DataFrame | None:
    """Read monthly skill metrics from CSV file.

    Args:
        codes: Optional list of station codes to filter. When provided,
            only skill metrics for those codes are returned. When None,
            all codes are returned.

    Returns None if the file doesn't exist or can't be read.
    """
    intermediate_path = os.getenv("ieasyforecast_intermediate_data_path", "")
    filename = os.getenv("ieasyforecast_monthly_skill_metrics_file", "")

    if not intermediate_path or not filename:
        logger.debug("Monthly skill metrics env vars not set")
        return None

    filepath = os.path.join(intermediate_path, filename)
    if not os.path.exists(filepath):
        logger.debug("Monthly skill metrics CSV not found: %s", filepath)
        return None

    try:
        df = pd.read_csv(filepath)
        if "code" in df.columns:
            df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)
        if codes is not None and not df.empty and "code" in df.columns:
            df = df[df["code"].astype(str).isin(codes)]
        return df
    except Exception as e:
        logger.error(
            "Failed to read monthly skill metrics CSV %s: %s",
            filepath,
            e,
        )
        return None


def _read_monthly_skill_metrics_api(
    codes: list[str] | None = None,
) -> pd.DataFrame | None:
    """Read monthly skill metrics from SAPPHIRE postprocessing API.

    Returns None if the API is unavailable or returns no data.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping API read")
        return None

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping API read")
        return None

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Postprocessing API not ready at %s", api_url)
            return None

        batch_size = 1000
        if codes is not None:
            # Per-code loop: API supports code= but not batch code__in
            frames = []
            for code in codes:
                skip = 0
                while True:
                    df_batch = client.read_skill_metrics(
                        horizon="month",
                        code=code,
                        skip=skip,
                        limit=batch_size,
                    )
                    if df_batch is None or df_batch.empty:
                        break
                    frames.append(df_batch)
                    if len(df_batch) < batch_size:
                        break
                    skip += batch_size
            if not frames:
                return None
            df = pd.concat(frames, ignore_index=True)
        else:
            all_records = []
            skip = 0
            while True:
                df_batch = client.read_skill_metrics(horizon="month", skip=skip, limit=batch_size)
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

            if not all_records:
                return None

            df = pd.concat(all_records, ignore_index=True)

        return _normalize_api_monthly_skill_metrics(df)

    except Exception as e:
        logger.error("Failed to read monthly skill metrics from API: %s", e)
        return None


def _normalize_api_monthly_skill_metrics(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Convert API column names to CSV-compatible names for monthly.

    API returns: horizon_in_year, model_type, code, sdivsigma, nse,
                 delta, accuracy, mae, n_pairs, crps, pbias, kgelf,
                 nse_log
    CSV expects: month_in_year, model_short, code, sdivsigma, nse,
                 delta, accuracy, mae, n_pairs, crps, pbias, kgelf,
                 nse_log
    """
    rename_map = {
        "horizon_in_year": "month_in_year",
        "model_type": "model_short",
    }
    df = df.rename(columns=rename_map)

    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)

    return df


# ===================================================================
# Short-term combined forecasts (pentad / decad)
# ===================================================================


def read_combined_forecasts(
    horizon_type: str,
    codes: list[str] | None = None,
) -> pd.DataFrame:
    """Read combined forecasts from API (primary) or CSV (fallback).

    Used by the maintenance entry point for gap detection and
    merge-back after filling missing ensembles.

    Args:
        horizon_type: 'pentad' or 'decad'.
        codes: Optional list of station codes to filter. When provided,
            only forecasts for those codes are returned. When None,
            all codes are returned.

    Returns:
        DataFrame with combined forecasts (all models + ensembles),
        or empty DataFrame if no data available.

    Raises:
        ValueError: If horizon_type is invalid.
    """
    if horizon_type not in ("pentad", "decad"):
        raise ValueError(f"horizon_type must be 'pentad' or 'decad', got: {horizon_type}")

    # API-first: try the authoritative source
    df = _read_combined_forecasts_api(horizon_type, codes)
    if df is not None and not df.empty:
        logger.info(
            "Read %d combined forecast rows from API (%s)",
            len(df),
            horizon_type,
        )
        return df

    # CSV fallback (deprecated)
    logger.info(
        "API combined forecasts unavailable for %s, falling back to CSV",
        horizon_type,
    )
    df = _read_combined_forecasts_csv(horizon_type, codes)
    if df is not None and not df.empty:
        logger.info(
            "Read %d combined forecast rows from CSV (%s)",
            len(df),
            horizon_type,
        )
        return df

    logger.warning("No combined forecasts available for %s", horizon_type)
    return pd.DataFrame()


def _read_combined_forecasts_api(
    horizon_type: str,
    codes: list[str] | None = None,
) -> pd.DataFrame | None:
    """Read combined forecasts from SAPPHIRE postprocessing API.

    Returns None if the API is unavailable or returns no data.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping API read")
        return None

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping API read")
        return None

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Postprocessing API not ready at %s", api_url)
            return None

        # Map internal horizon names to API horizon names
        api_horizon = "decade" if horizon_type == "decad" else horizon_type

        batch_size = 1000
        if codes is not None:
            # Per-code loop: API supports code= but not batch code__in
            frames = []
            for code in codes:
                skip = 0
                while True:
                    df_batch = client.read_short_term_forecasts(
                        horizon=api_horizon,
                        code=code,
                        skip=skip,
                        limit=batch_size,
                    )
                    if df_batch is None or df_batch.empty:
                        break
                    frames.append(df_batch)
                    if len(df_batch) < batch_size:
                        break
                    skip += batch_size
            if not frames:
                return None
            df = pd.concat(frames, ignore_index=True)
        else:
            all_records = []
            skip = 0
            while True:
                df_batch = client.read_short_term_forecasts(
                    horizon=api_horizon, skip=skip, limit=batch_size
                )
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

            if not all_records:
                return None

            df = pd.concat(all_records, ignore_index=True)

        return _normalize_api_combined_forecasts(df, horizon_type)

    except Exception as e:
        logger.error("Failed to read combined forecasts from API: %s", e)
        return None


def _normalize_api_combined_forecasts(df: pd.DataFrame, horizon_type: str) -> pd.DataFrame:
    """Convert API response columns to internal column names.

    API returns: id, horizon_type, code, model_type,
        model_type_description, date, target, flag,
        horizon_value, horizon_in_year, composition,
        q05, q25, q50, q75, q95, forecasted_discharge

    Internal expects: code, model_short, date, target, flag,
        pentad_in_year|decad_in_year, pentad_in_month|decad_in_month,
        composition, q05-q95, forecasted_discharge
    """
    df = df.copy()

    period_col = "pentad_in_year" if horizon_type == "pentad" else "decad_in_year"
    period_in_month_col = "pentad_in_month" if horizon_type == "pentad" else "decad_in_month"

    rename_map = {
        "model_type": "model_short",
        "horizon_in_year": period_col,
        "horizon_value": period_in_month_col,
    }
    df = df.rename(columns=rename_map)

    # Ensure date is datetime
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    # Ensure code is string without trailing .0
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)

    # Drop API-only columns not needed internally
    drop_cols = ["id", "horizon_type", "model_type_description"]
    df = df.drop(
        columns=[c for c in drop_cols if c in df.columns],
        errors="ignore",
    )

    return df


def _read_combined_forecasts_csv(
    horizon_type: str,
    codes: list[str] | None = None,
) -> pd.DataFrame | None:
    """Read combined forecasts from CSV file.

    Returns None if the file doesn't exist or can't be read.
    """
    intermediate_path = os.getenv("ieasyforecast_intermediate_data_path", "")

    if horizon_type == "pentad":
        filename = os.getenv("ieasyforecast_combined_forecast_pentad_file", "")
    else:
        filename = os.getenv("ieasyforecast_combined_forecast_decad_file", "")

    if not intermediate_path or not filename:
        logger.debug(
            "Combined forecast env vars not set for %s",
            horizon_type,
        )
        return None

    filepath = os.path.join(intermediate_path, filename)
    if not os.path.exists(filepath):
        logger.debug("Combined forecasts CSV not found: %s", filepath)
        return None

    try:
        df = pd.read_csv(filepath)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        if "code" in df.columns:
            df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)
        if codes is not None and not df.empty and "code" in df.columns:
            df = df[df["code"].astype(str).isin([str(c) for c in codes])]
        return df
    except Exception as e:
        logger.error(
            "Failed to read combined forecasts CSV %s: %s",
            filepath,
            e,
        )
        return None


# ===================================================================
# Daily observations and forecasts (for Tier 2 skill metrics)
# ===================================================================


def read_daily_observations(
    codes: list[str],
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Read daily runoff observations from preprocessing API.

    Thin wrapper around _read_daily_runoff_api() — no aggregation,
    returns raw daily data for Tier 2 skill metric calculations.

    Args:
        codes: Station codes to read.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        DataFrame with columns: [code, date, discharge_avg].
        Empty DataFrame if no data available.
    """
    empty = pd.DataFrame(columns=["code", "date", "discharge_avg"])

    try:
        daily = _read_daily_runoff_api(codes, start_year, end_year)
    except Exception as e:
        logger.error("Failed to read daily observations: %s", e)
        return empty

    if daily is None or daily.empty:
        logger.warning("No daily observation data available")
        return empty

    # Normalize columns
    df = daily.copy()
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)

    # Keep only needed columns
    cols = ["code", "date", "discharge_avg"]
    available = [c for c in cols if c in df.columns]
    return df[available]


def read_daily_forecasts(
    codes: list[str],
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Read ML forecasts with horizon_type='day' from postprocessing API.

    Deduplicates: keeps the latest forecast_date per
    (code, target date, model_short).

    Args:
        codes: Station codes to read.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        DataFrame with columns: [code, date, model_short,
        forecasted_discharge]. Empty DataFrame if no data.
    """
    empty = pd.DataFrame(columns=["code", "date", "model_short", "forecasted_discharge"])

    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping")
        return empty

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping")
        return empty

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Postprocessing API not ready at %s", api_url)
            return empty

        all_records = []
        start_date = f"{start_year}-01-01"
        end_date = f"{end_year}-12-31"

        for code in codes:
            skip = 0
            batch_size = 1000
            while True:
                df_batch = client.read_forecasts(
                    horizon="day",
                    code=code,
                    start_date=start_date,
                    end_date=end_date,
                    skip=skip,
                    limit=batch_size,
                )
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

        all_records = [df for df in all_records if not df.empty]
        if not all_records:
            return empty

        df = pd.concat(all_records, ignore_index=True)
        return _normalize_daily_forecasts(df)

    except Exception as e:
        logger.error("Failed to read daily forecasts from API: %s", e)
        return empty


def _normalize_daily_forecasts(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize API daily forecast response and deduplicate.

    Keeps latest forecast_date per (code, target, model).

    Returns DataFrame with: [code, date, model_short,
    forecasted_discharge].
    """
    df = df.copy()

    # Rename API columns
    if "model_type" in df.columns:
        df = df.rename(columns={"model_type": "model_short"})
    # API returns 'date' (issue date) and 'target' (target date).
    # Rename 'date' → 'forecast_date' first to avoid collision when
    # renaming 'target' → 'date'.
    if "target" in df.columns and "date" in df.columns:
        df = df.rename(columns={"date": "forecast_date", "target": "date"})
    elif "target" in df.columns:
        df = df.rename(columns={"target": "date"})

    # Ensure types
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    # Deduplicate: keep latest forecast_date per (code, date, model)
    if "forecast_date" in df.columns:
        df["forecast_date"] = pd.to_datetime(df["forecast_date"])
        df = df.sort_values("forecast_date", ascending=False)
        df = df.drop_duplicates(subset=["code", "date", "model_short"], keep="first")

    # Keep only needed columns
    cols = ["code", "date", "model_short", "forecasted_discharge"]
    available = [c for c in cols if c in df.columns]
    return df[available].reset_index(drop=True)


# ===================================================================
# Monthly observations (daily runoff → monthly mean)
# ===================================================================


def read_monthly_observations(
    codes: list[str],
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Aggregate daily runoff to monthly mean discharge.

    Reads daily runoff via preprocessing API. Requires >= 50%
    non-missing days per month.

    Args:
        codes: Station codes to read.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        DataFrame with columns: [code, year, month, month_in_year,
        discharge_avg, delta]. Empty DataFrame if no data available.
    """
    empty = pd.DataFrame(
        columns=["code", "year", "month", "month_in_year", "discharge_avg", "delta"]
    )

    try:
        daily = _read_daily_runoff_api(codes, start_year, end_year)
    except Exception as e:
        logger.error("Failed to read daily runoff: %s", e)
        return empty

    if daily is None or daily.empty:
        logger.warning("No daily runoff data available")
        return empty

    return _aggregate_daily_to_monthly(daily)


def _read_daily_runoff_api(
    codes: list[str],
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Read daily runoff from preprocessing API with pagination.

    Returns combined DataFrame or empty DataFrame if unavailable.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping")
        return pd.DataFrame()

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping")
        return pd.DataFrame()

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePreprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Preprocessing API not ready at %s", api_url)
            return pd.DataFrame()

        all_records = []
        start_date = f"{start_year}-01-01"
        end_date = f"{end_year}-12-31"

        for code in codes:
            skip = 0
            batch_size = 1000
            while True:
                df_batch = client.read_runoff(
                    horizon="day",
                    code=code,
                    start_date=start_date,
                    end_date=end_date,
                    skip=skip,
                    limit=batch_size,
                )
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

        all_records = [df.dropna(axis=1, how="all") for df in all_records if not df.empty]
        if not all_records:
            return pd.DataFrame()

        df = pd.concat(all_records, ignore_index=True)
        # API returns 'discharge'; internal convention is 'discharge_avg'
        if "discharge" in df.columns and "discharge_avg" not in df.columns:
            df = df.rename(columns={"discharge": "discharge_avg"})
        return df

    except Exception as e:
        logger.error("Failed to read daily runoff from API: %s", e)
        return pd.DataFrame()


def _aggregate_daily_to_monthly(daily: pd.DataFrame) -> pd.DataFrame:
    """Aggregate daily runoff to monthly means with 50% coverage filter.

    Args:
        daily: DataFrame with columns [code, date, discharge_avg].

    Returns:
        DataFrame with columns [code, year, month, month_in_year,
        discharge_avg, delta].
    """
    df = daily.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["days_in_month"] = df["date"].dt.days_in_month

    # Aggregate to monthly means per (code, year, month)
    monthly = (
        df.groupby(["code", "year", "month"])
        .agg(
            discharge_avg=("discharge_avg", "mean"),
            non_missing_days=("discharge_avg", "count"),
            days_in_month=("days_in_month", "first"),
        )
        .reset_index()
    )

    # Filter: require >= 50% non-missing days
    monthly = monthly[monthly["non_missing_days"] >= monthly["days_in_month"] * 0.5].copy()

    if monthly.empty:
        return pd.DataFrame(
            columns=["code", "year", "month", "month_in_year", "discharge_avg", "delta"]
        )

    monthly["month_in_year"] = monthly["month"]

    # Compute delta per (code, month_in_year): 0.674 * std across years
    delta_df = (
        monthly.groupby(["code", "month_in_year"])
        .agg(
            std_discharge=("discharge_avg", "std"),
        )
        .reset_index()
    )
    # Single year -> std is NaN -> delta = 0
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)

    monthly = monthly.merge(
        delta_df[["code", "month_in_year", "delta"]],
        on=["code", "month_in_year"],
        how="left",
    )

    # Drop intermediate columns
    monthly = monthly.drop(columns=["non_missing_days", "days_in_month"], errors="ignore")

    return monthly


# ===================================================================
# Monthly forecasts (from long_forecasts table)
# ===================================================================


def read_monthly_forecasts(
    codes: list[str],
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Read monthly long-term forecasts from postprocessing API.

    Args:
        codes: Station codes to read.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        DataFrame with columns: [code, year, month, model_short,
        q50, q05, q10, q25, q75, q90, q95, valid_from, valid_to,
        date, flag]. Empty DataFrame if no data available.
    """
    empty = pd.DataFrame()

    try:
        raw = _read_long_forecasts_api(codes, start_year, end_year)
    except Exception as e:
        logger.error("Failed to read monthly forecasts: %s", e)
        return empty

    if raw is None or raw.empty:
        logger.warning("No monthly forecast data available")
        return empty

    return _normalize_monthly_forecasts(raw)


def _read_long_forecasts_api(
    codes: list[str],
    start_year: int,
    end_year: int,
    horizon_type: str = "month",
    horizon_value: int | None = None,
) -> pd.DataFrame:
    """Read long-term forecasts from postprocessing API with pagination.

    Args:
        codes: List of station codes to query.
        start_year: First year of the date range (inclusive).
        end_year: Last year of the date range (inclusive).
        horizon_type: Horizon type filter passed to the API (e.g. ``"month"``
            or ``"season"``). Defaults to ``"month"`` to preserve existing
            behaviour for all current callers.
        horizon_value: Optional lead/horizon-value filter. When omitted, the
            request is unchanged.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping")
        return pd.DataFrame()

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping")
        return pd.DataFrame()

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Postprocessing API not ready at %s", api_url)
            return pd.DataFrame()

        all_records = []
        start_date = f"{start_year}-01-01"
        end_date = f"{end_year}-12-31"

        for code in codes:
            skip = 0
            batch_size = 1000
            while True:
                kwargs = {
                    "horizon_type": horizon_type,
                    "code": code,
                    "start_date": start_date,
                    "end_date": end_date,
                    "skip": skip,
                    "limit": batch_size,
                }
                if horizon_value is not None:
                    kwargs["horizon_value"] = horizon_value
                df_batch = client.read_long_term_forecasts(**kwargs)
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

        all_records = [df.dropna(axis=1, how="all") for df in all_records if not df.empty]
        if not all_records:
            return pd.DataFrame()

        return pd.concat(all_records, ignore_index=True)

    except Exception as e:
        logger.error("Failed to read long-term forecasts from API: %s", e)
        return pd.DataFrame()


def _normalize_monthly_forecasts(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize API response to expected column format.

    Extracts year and month from valid_from, renames model_type
    to model_short.
    """
    df = df.copy()

    # Extract year and month from valid_from
    df["valid_from"] = pd.to_datetime(df["valid_from"])
    df["year"] = df["valid_from"].dt.year
    df["month"] = df["valid_from"].dt.month

    # Rename model_type -> model_short
    if "model_type" in df.columns:
        df = df.rename(columns={"model_type": "model_short"})

    # Ensure code is string
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)

    return df


# ===================================================================
# Operational/maintenance monthly forecast readers
# ===================================================================


def read_latest_monthly_forecasts(
    codes: list[str],
    forecast_date: dt.date | None = None,
) -> pd.DataFrame:
    """Read the most recent month's long-term forecasts from API.

    Reads forecasts with issue dates in the last 60 days,
    then filters to the single most recent target (year, month).

    Args:
        codes: Station codes to read.
        forecast_date: Reference date for lookback window.
            Defaults to today if not provided.

    Returns:
        DataFrame with columns: code, year, month, month_in_year,
        model_short, forecasted_discharge (=q50), q05-q95,
        valid_from, valid_to, date, flag.
        Empty DataFrame if no data.
    """
    today = forecast_date if forecast_date is not None else dt.date.today()
    start_date = today - dt.timedelta(days=60)
    start_year = start_date.year
    end_year = today.year

    raw = _read_long_forecasts_api(codes, start_year, end_year)
    if raw is None or raw.empty:
        logger.warning("No recent monthly forecast data available")
        return pd.DataFrame()

    df = _normalize_monthly_forecasts(raw)
    if df.empty:
        return df

    # Add month_in_year
    if "month_in_year" not in df.columns and "month" in df.columns:
        df["month_in_year"] = df["month"]

    # Add forecasted_discharge from q50 if missing
    if "forecasted_discharge" not in df.columns and "q50" in df.columns:
        df["forecasted_discharge"] = df["q50"].astype(float)

    # Filter to the latest (year, month) based on valid_from
    vf = pd.to_datetime(df["valid_from"], errors="coerce")
    if vf.notna().any():
        latest_vf = vf.max()
        latest_year = latest_vf.year
        latest_month = latest_vf.month
    else:
        latest_year = int(df["year"].max())
        latest_month = int(df[df["year"] == latest_year]["month"].max())

    df = df[(df["year"] == latest_year) & (df["month"] == latest_month)].copy()

    logger.info(
        "Read %d latest monthly forecasts for %d-%02d",
        len(df),
        latest_year,
        latest_month,
    )
    return df


def read_monthly_combined_forecasts(
    codes: list[str] | None = None,
) -> pd.DataFrame:
    """Read monthly combined forecasts from API (primary) or CSV
    (fallback).

    Used by the maintenance entry point for gap detection and
    merge-back after filling missing ensembles.

    Args:
        codes: Optional list of station codes to filter. When provided,
            only forecasts for those codes are returned. When None,
            all codes are returned.

    Returns:
        DataFrame with combined forecasts (all models + ensembles),
        or empty DataFrame if no data available.
    """
    # API-first: try the authoritative source
    df = _read_monthly_combined_forecasts_api(codes)
    if df is not None and not df.empty:
        logger.info(
            "Read %d monthly combined forecast rows from API",
            len(df),
        )
        return df

    # CSV fallback (deprecated)
    logger.info("API monthly combined forecasts unavailable, falling back to CSV")
    df = _read_monthly_combined_forecasts_csv(codes)
    if df is not None and not df.empty:
        logger.info(
            "Read %d monthly combined forecast rows from CSV",
            len(df),
        )
        return df

    logger.warning("No monthly combined forecasts available")
    return pd.DataFrame()


def _read_monthly_combined_forecasts_api(
    codes: list[str] | None = None,
) -> pd.DataFrame | None:
    """Read monthly combined forecasts from SAPPHIRE postprocessing API.

    Returns None if the API is unavailable or returns no data.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping API read")
        return None

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping API read")
        return None

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Postprocessing API not ready at %s", api_url)
            return None

        batch_size = 1000
        if codes is not None:
            # Per-code loop: API supports code= but not batch code__in
            frames = []
            for code in codes:
                skip = 0
                while True:
                    df_batch = client.read_long_term_forecasts(
                        horizon_type="month",
                        code=code,
                        skip=skip,
                        limit=batch_size,
                    )
                    if df_batch is None or df_batch.empty:
                        break
                    frames.append(df_batch)
                    if len(df_batch) < batch_size:
                        break
                    skip += batch_size
            frames = [df.dropna(axis=1, how="all") for df in frames if not df.empty]
            if not frames:
                return None
            df = pd.concat(frames, ignore_index=True)
        else:
            all_records = []
            skip = 0
            while True:
                df_batch = client.read_long_term_forecasts(
                    horizon_type="month", skip=skip, limit=batch_size
                )
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

            all_records = [df.dropna(axis=1, how="all") for df in all_records if not df.empty]
            if not all_records:
                return None

            df = pd.concat(all_records, ignore_index=True)

        return _normalize_monthly_combined_forecasts(df)

    except Exception as e:
        logger.error(
            "Failed to read monthly combined forecasts from API: %s",
            e,
        )
        return None


def _normalize_monthly_combined_forecasts(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Normalize API monthly forecast response for gap detection.

    Delegates to _normalize_monthly_forecasts() for base
    normalization, then adds month_in_year and
    forecasted_discharge if absent.
    """
    df = _normalize_monthly_forecasts(df)

    # Add month_in_year (needed by gap detector)
    if "month_in_year" not in df.columns and "month" in df.columns:
        df["month_in_year"] = df["month"]

    # Add forecasted_discharge from q50 (needed for merge-back)
    if "forecasted_discharge" not in df.columns and "q50" in df.columns:
        df["forecasted_discharge"] = df["q50"].astype(float)

    # Drop API-only columns not needed internally
    drop_cols = ["id", "horizon_type", "horizon_value", "model_type_description"]
    df = df.drop(
        columns=[c for c in drop_cols if c in df.columns],
        errors="ignore",
    )

    return df


def _read_monthly_combined_forecasts_csv(
    codes: list[str] | None = None,
) -> pd.DataFrame | None:
    """Read monthly combined forecasts from CSV file.

    Returns None if the file doesn't exist or can't be read.
    """
    intermediate_path = os.getenv("ieasyforecast_intermediate_data_path", "")
    filename = os.getenv("ieasyforecast_monthly_combined_forecast_file", "")

    if not intermediate_path or not filename:
        logger.debug("Monthly combined forecast env vars not set")
        return None

    filepath = os.path.join(intermediate_path, filename)
    if not os.path.exists(filepath):
        logger.debug("Monthly combined forecasts CSV not found: %s", filepath)
        return None

    try:
        df = pd.read_csv(filepath)
        if "code" in df.columns:
            df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)
        if codes is not None and not df.empty and "code" in df.columns:
            df = df[df["code"].astype(str).isin([str(c) for c in codes])]
        return df
    except Exception as e:
        logger.error(
            "Failed to read monthly combined forecasts CSV %s: %s",
            filepath,
            e,
        )
        return None


# ===================================================================
# Short-term (pentad/decad) observations and individual forecasts
# ===================================================================

# tag_library is needed for period column computation
# (pentad_in_month, pentad_in_year, etc.)
try:
    import tag_library as tl

    TAG_LIBRARY_AVAILABLE = True
except ImportError:
    TAG_LIBRARY_AVAILABLE = False
    logger.warning("tag_library not available; short-term period columns cannot be computed")


def _is_pentad_boundary(d) -> bool:
    """Return True if *d* is a pentad issue day (5/10/15/20/25/last)."""
    last_day = calendar.monthrange(d.year, d.month)[1]
    return d.day in (5, 10, 15, 20, 25, last_day)


def _is_decad_boundary(d) -> bool:
    """Return True if *d* is a decad issue day (10/20/last)."""
    last_day = calendar.monthrange(d.year, d.month)[1]
    return d.day in (10, 20, last_day)


def _clean_code_column(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure code column is string without trailing .0."""
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)
    return df


# -------------------------------------------------------------------
# Private API reader functions
# -------------------------------------------------------------------


def _read_short_term_runoff_api(
    horizon_type: str,
    codes: list[str] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> pd.DataFrame | None:
    """Read pentad or decad runoff observations from preprocessing API.

    Args:
        horizon_type: 'pentad' or 'decad'.
        codes: Station codes to filter. None reads all.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        Raw DataFrame from API, or None if unavailable.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping API read")
        return None

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping API read")
        return None

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePreprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Preprocessing API not ready at %s", api_url)
            return None

        # Map internal horizon names to API horizon names
        api_horizon = "decade" if horizon_type == "decad" else horizon_type

        start_date = f"{start_year}-01-01" if start_year is not None else None
        end_date = f"{end_year}-12-31" if end_year is not None else None

        all_records = []
        batch_size = 1000

        if codes is not None:
            for code in codes:
                skip = 0
                kwargs = {"horizon": api_horizon, "code": code}
                if start_date:
                    kwargs["start_date"] = start_date
                if end_date:
                    kwargs["end_date"] = end_date
                while True:
                    df_batch = client.read_runoff(**kwargs, skip=skip, limit=batch_size)
                    if df_batch is None or df_batch.empty:
                        break
                    all_records.append(df_batch)
                    if len(df_batch) < batch_size:
                        break
                    skip += batch_size
        else:
            skip = 0
            kwargs = {"horizon": api_horizon}
            if start_date:
                kwargs["start_date"] = start_date
            if end_date:
                kwargs["end_date"] = end_date
            while True:
                df_batch = client.read_runoff(**kwargs, skip=skip, limit=batch_size)
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

        if not all_records:
            return None

        return pd.concat(all_records, ignore_index=True)

    except Exception as e:
        logger.error("Failed to read short-term runoff from API: %s", e)
        return None


def _read_lr_forecasts_pp_api(
    horizon_type: str,
    codes: list[str] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> pd.DataFrame | None:
    """Read LR forecasts from postprocessing API.

    Args:
        horizon_type: 'pentad' or 'decad'.
        codes: Station codes to filter. None reads all.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        Raw DataFrame from API, or None if unavailable.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping API read")
        return None

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping API read")
        return None

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Postprocessing API not ready at %s", api_url)
            return None

        api_horizon = "decade" if horizon_type == "decad" else horizon_type

        start_date = f"{start_year}-01-01" if start_year is not None else None
        end_date = f"{end_year}-12-31" if end_year is not None else None

        all_records = []
        batch_size = 1000

        if codes is not None:
            for code in codes:
                skip = 0
                kwargs = {"horizon": api_horizon, "code": code}
                if start_date:
                    kwargs["start_date"] = start_date
                if end_date:
                    kwargs["end_date"] = end_date
                while True:
                    df_batch = client.read_lr_forecasts(**kwargs, skip=skip, limit=batch_size)
                    if df_batch is None or df_batch.empty:
                        break
                    all_records.append(df_batch)
                    if len(df_batch) < batch_size:
                        break
                    skip += batch_size
        else:
            skip = 0
            kwargs = {"horizon": api_horizon}
            if start_date:
                kwargs["start_date"] = start_date
            if end_date:
                kwargs["end_date"] = end_date
            while True:
                df_batch = client.read_lr_forecasts(**kwargs, skip=skip, limit=batch_size)
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

        if not all_records:
            return None

        return pd.concat(all_records, ignore_index=True)

    except Exception as e:
        logger.error("Failed to read LR forecasts from API: %s", e)
        return None


def _read_ml_forecasts_pp_api(
    model: str,
    horizon_type: str,
    codes: list[str] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> pd.DataFrame | None:
    """Read ML forecasts from postprocessing API.

    Reads both horizon='day' (current pipeline writes daily targets) and
    horizon=horizon_type (migrated period archive), then keeps period rows
    only before each station/model's first DAY issue date.

    Args:
        model: Model short name (e.g. 'TFT', 'TiDE').
        horizon_type: 'pentad' or 'decad'.
        codes: Station codes to filter. None reads all.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        Raw DataFrame from API, or None if unavailable.
    """

    def _fetch_archive(try_horizon: str) -> pd.DataFrame | None:
        all_records = []
        batch_size = 1000

        if codes is not None:
            for code in codes:
                skip = 0
                kwargs = {
                    "horizon": try_horizon,
                    "model": model,
                    "code": code,
                }
                if start_date:
                    kwargs["start_date"] = start_date
                if end_date:
                    kwargs["end_date"] = end_date
                while True:
                    df_batch = client.read_short_term_forecasts(
                        **kwargs, skip=skip, limit=batch_size
                    )
                    if df_batch is None or df_batch.empty:
                        break
                    all_records.append(df_batch)
                    if len(df_batch) < batch_size:
                        break
                    skip += batch_size
        else:
            skip = 0
            kwargs = {
                "horizon": try_horizon,
                "model": model,
            }
            if start_date:
                kwargs["start_date"] = start_date
            if end_date:
                kwargs["end_date"] = end_date
            while True:
                df_batch = client.read_short_term_forecasts(**kwargs, skip=skip, limit=batch_size)
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

        if not all_records:
            return None

        return pd.concat(all_records, ignore_index=True)

    def _working_archive(df: pd.DataFrame) -> pd.DataFrame:
        work = _clean_code_column(df.copy())
        if "date" in work.columns:
            work["date"] = pd.to_datetime(work["date"])
        if "model_type" in work.columns:
            work["_pp036_model_type_key"] = work["model_type"].astype(str)
        else:
            work["_pp036_model_type_key"] = model
        return work

    def _merge_archives_by_day_cutover(
        day_df: pd.DataFrame | None,
        period_df: pd.DataFrame | None,
    ) -> pd.DataFrame | None:
        day_rows = 0 if day_df is None else len(day_df)
        period_rows = 0 if period_df is None else len(period_df)

        if (day_df is None or day_df.empty) and (period_df is None or period_df.empty):
            logger.debug(
                "Read ML forecasts for %s (%s): day_rows=0, period_rows=0, "
                "retained_period_rows=0, final_rows=0",
                model,
                horizon_type,
            )
            return None

        if day_df is None or day_df.empty:
            logger.debug(
                "Read ML forecasts for %s (%s): day_rows=0, period_rows=%d, "
                "retained_period_rows=%d, final_rows=%d",
                model,
                horizon_type,
                period_rows,
                period_rows,
                period_rows,
            )
            return period_df

        if period_df is None or period_df.empty:
            logger.debug(
                "Read ML forecasts for %s (%s): day_rows=%d, period_rows=0, "
                "retained_period_rows=0, final_rows=%d",
                model,
                horizon_type,
                day_rows,
                day_rows,
            )
            return day_df

        day_work = _working_archive(day_df)
        period_work = _working_archive(period_df)
        pair_cols = ["code", "_pp036_model_type_key"]

        first_day = day_work.groupby(pair_cols)["date"].min()
        first_period = period_work.groupby(pair_cols)["date"].min()

        for pair, first_day_date in first_day.items():
            if pair not in first_period:
                continue
            first_period_date = first_period[pair]
            if first_day_date < first_period_date:
                logger.warning(
                    "DAY ML archive for %s code=%s model_type=%s starts at %s "
                    "before period archive starts at %s",
                    model,
                    pair[0],
                    pair[1],
                    first_day_date.date(),
                    first_period_date.date(),
                )

        period_with_cutover = period_work.merge(
            first_day.rename("_pp036_first_day_date"),
            left_on=pair_cols,
            right_index=True,
            how="left",
        )
        retain_period = period_with_cutover["_pp036_first_day_date"].isna() | (
            period_with_cutover["date"] < period_with_cutover["_pp036_first_day_date"]
        )
        retained_period = period_df.loc[retain_period.to_numpy()].copy()
        final = pd.concat([retained_period, day_df], ignore_index=True)

        logger.debug(
            "Read ML forecasts for %s (%s): day_rows=%d, period_rows=%d, "
            "retained_period_rows=%d, final_rows=%d",
            model,
            horizon_type,
            day_rows,
            period_rows,
            len(retained_period),
            len(final),
        )
        return final

    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping API read")
        return None

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping API read")
        return None

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Postprocessing API not ready at %s", api_url)
            return None

        api_horizon = "decade" if horizon_type == "decad" else horizon_type

        start_date = f"{start_year}-01-01" if start_year is not None else None
        end_date = f"{end_year}-12-31" if end_year is not None else None

        day_records = _fetch_archive("day")
        period_records = _fetch_archive(api_horizon)
        return _merge_archives_by_day_cutover(day_records, period_records)

    except Exception as e:
        logger.error(
            "Failed to read ML forecasts for %s from API: %s",
            model,
            e,
        )
        return None


# -------------------------------------------------------------------
# Normalization functions
# -------------------------------------------------------------------


def _normalize_observed_runoff(df: pd.DataFrame, horizon_type: str) -> pd.DataFrame:
    """Normalize API runoff response to internal observed column format.

    Args:
        df: Raw DataFrame from preprocessing API.
        horizon_type: 'pentad' or 'decad'.

    Returns:
        DataFrame with columns: [code, date, discharge_avg,
        model_short, pentad_in_year, pentad_in_month] (or decad
        equivalents).
    """
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()

    period_col = "pentad_in_year" if horizon_type == "pentad" else "decad_in_year"
    period_in_month_col = "pentad_in_month" if horizon_type == "pentad" else "decad_in_month"

    # Rename API columns
    rename_map = {
        "discharge": "discharge_avg",
        "horizon_in_year": period_col,
        "horizon_value": period_in_month_col,
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Add model_short = "Obs"
    df["model_short"] = "Obs"

    # Clean code column
    df = _clean_code_column(df)

    # Parse dates
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    # Drop API-only columns
    drop_cols = ["id", "horizon_type", "model_type_description"]
    df = df.drop(
        columns=[c for c in drop_cols if c in df.columns],
        errors="ignore",
    )

    return df


def _normalize_lr_forecasts(
    df: pd.DataFrame, horizon_type: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normalize API LR forecast response and split forecasts + stats.

    Args:
        df: Raw DataFrame from postprocessing API.
        horizon_type: 'pentad' or 'decad'.

    Returns:
        Tuple of (forecasts_df, stats_df).
        - forecasts_df: [code, date, forecasted_discharge, predictor,
          slope, intercept, rsquared, model_short, pentad_in_month,
          pentad_in_year] (or decad equivalents)
        - stats_df: [date, code, q_mean, q_std_sigma, delta]
    """
    empty_fc = pd.DataFrame()
    empty_stats = pd.DataFrame(columns=["date", "code", "q_mean", "q_std_sigma", "delta"])

    if df is None or df.empty:
        return empty_fc, empty_stats

    df = df.copy()

    # Clean code column and parse dates
    df = _clean_code_column(df)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    # Rename model_type -> model_short, or set it explicitly.
    # The lr-forecast API endpoint does not return a model_type column,
    # so we must assign model_short = "LR" when it's absent.
    if "model_type" in df.columns:
        df = df.rename(columns={"model_type": "model_short"})
    if "model_short" not in df.columns:
        df["model_short"] = "LR"

    # Extract stats columns before dropping them from forecasts
    stats_cols = ["date", "code", "q_mean", "q_std_sigma", "delta"]
    stats_present = [c for c in stats_cols if c in df.columns]
    if len(stats_present) >= 3:  # At least date, code, and one stat
        stats = df[stats_present].drop_duplicates().copy()
    else:
        stats = empty_stats

    # Build forecasts: drop stats-only columns and discharge_avg
    drop_from_fc = [
        "q_mean",
        "q_std_sigma",
        "delta",
        "discharge_avg",
    ]
    forecasts = df.drop(
        columns=[c for c in drop_from_fc if c in df.columns],
        errors="ignore",
    )

    # Compute period columns using tag_library
    if TAG_LIBRARY_AVAILABLE and "date" in forecasts.columns:
        period_col = "pentad_in_year" if horizon_type == "pentad" else "decad_in_year"
        period_in_month_col = "pentad_in_month" if horizon_type == "pentad" else "decad_in_month"

        if horizon_type == "pentad":
            get_period = tl.get_pentad
            get_period_in_year = tl.get_pentad_in_year
        else:
            get_period = tl.get_decad_in_month
            get_period_in_year = tl.get_decad_in_year

        # +1 day offset: the forecast date is the last day of the
        # previous period, so +1 day gives the first day of the
        # forecasted period.
        offset_dates = forecasts["date"] + pd.Timedelta(days=1)
        forecasts[period_in_month_col] = offset_dates.apply(get_period)
        forecasts[period_col] = offset_dates.apply(get_period_in_year)

    # Deduplicate on [date, code], keep last
    if "date" in forecasts.columns and "code" in forecasts.columns:
        forecasts = forecasts.drop_duplicates(subset=["date", "code"], keep="last")

    # Drop API-only columns
    drop_cols = [
        "id",
        "horizon_type",
        "horizon_value",
        "horizon_in_year",
        "model_type_description",
    ]
    forecasts = forecasts.drop(
        columns=[c for c in drop_cols if c in forecasts.columns],
        errors="ignore",
    )

    return forecasts, stats


def _normalize_ml_forecasts(
    df: pd.DataFrame,
    model: str,
    horizon_type: str,
) -> pd.DataFrame:
    """Normalize API ML forecast response: aggregate daily->pentad/decad.

    Groups daily targets by (code, date) and computes:
    - mean for forecasted_discharge, q05, q25, q75, q95
    - max for flag
    - first for horizon_value, horizon_in_year

    Args:
        df: Raw DataFrame from postprocessing API.
        model: Model short name from API (e.g. 'TFT', 'TIDE').
        horizon_type: 'pentad' or 'decad'.

    Returns:
        DataFrame with aggregated forecasts and period columns.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()

    # Clean code column and parse dates
    df = _clean_code_column(df)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    # PP-031: Drop rows where date is not a boundary day for this horizon.
    if "date" in df.columns:
        if horizon_type == "pentad":
            boundary_mask = df["date"].apply(_is_pentad_boundary)
        else:
            boundary_mask = df["date"].apply(_is_decad_boundary)

        n_non_boundary = (~boundary_mask).sum()
        if n_non_boundary > 0:
            logger.info(
                "Dropped %d/%d rows on non-%s-boundary dates for %s",
                n_non_boundary,
                len(df),
                horizon_type,
                model,
            )
        df = df[boundary_mask].copy()

        if df.empty:
            return pd.DataFrame()

    # Filter daily targets to the forecast period boundary.
    # The forecast date is the last day of the previous period;
    # date+1 is the first day of the target period.
    if TAG_LIBRARY_AVAILABLE and "target" in df.columns and "date" in df.columns:
        df["target"] = pd.to_datetime(df["target"])

        if horizon_type == "pentad":
            period_func = tl.get_pentad_in_year
        else:
            period_func = tl.get_decad_in_year

        expected_period = (df["date"] + pd.Timedelta(days=1)).apply(period_func)
        target_period = df["target"].apply(period_func)

        in_period = target_period == expected_period
        n_dropped = (~in_period).sum()
        if n_dropped > 0:
            logger.info(
                "Filtered %d/%d daily targets outside %s boundary for %s",
                n_dropped,
                len(df),
                horizon_type,
                model,
            )
        df = df[in_period].copy()

        if df.empty:
            logger.warning(
                "No %s targets within period for model %s after filtering",
                horizon_type,
                model,
            )
            return pd.DataFrame()

    # Aggregate daily targets -> pentad/decad level
    numeric_cols = [
        "q05",
        "q25",
        "q75",
        "q95",
        "forecasted_discharge",
    ]
    agg_dict = {}
    for col in numeric_cols:
        if col in df.columns:
            agg_dict[col] = "mean"

    if "flag" in df.columns:
        agg_dict["flag"] = "max"

    for col in ["horizon_value", "horizon_in_year"]:
        if col in df.columns:
            agg_dict[col] = "first"

    if agg_dict and "code" in df.columns and "date" in df.columns:
        df = df.groupby(["code", "date"], as_index=False).agg(agg_dict)
        count_quantile_crossings(df, ["q05", "q25", "q75", "q95"], label="daily→pentad/decad")

    # Model name mapping: API stores uppercase, need display names
    model_name_map = {
        "TFT": "TFT",
        "TIDE": "TiDE",
        "TSMIXER": "TSMixer",
        "ARIMA": "ARIMA",
    }
    df["model_short"] = model_name_map.get(model.upper(), model)

    # Compute period columns using tag_library
    if TAG_LIBRARY_AVAILABLE and "date" in df.columns:
        period_col = "pentad_in_year" if horizon_type == "pentad" else "decad_in_year"
        period_in_month_col = "pentad_in_month" if horizon_type == "pentad" else "decad_in_month"

        if horizon_type == "pentad":
            get_period = tl.get_pentad
            get_period_in_year = tl.get_pentad_in_year
        else:
            get_period = tl.get_decad_in_month
            get_period_in_year = tl.get_decad_in_year

        offset_dates = df["date"] + pd.Timedelta(days=1)
        df[period_in_month_col] = offset_dates.apply(get_period)
        df[period_col] = offset_dates.apply(get_period_in_year)

    # Drop API-only columns
    drop_cols = [
        "id",
        "horizon_type",
        "horizon_value",
        "horizon_in_year",
        "model_type",
        "model_type_description",
    ]
    df = df.drop(
        columns=[c for c in drop_cols if c in df.columns],
        errors="ignore",
    )

    return df


# -------------------------------------------------------------------
# Public orchestrator functions
# -------------------------------------------------------------------


def read_short_term_observations(
    horizon_type: str,
    codes: list[str] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> pd.DataFrame:
    """Read pentad or decad runoff observations from API or CSV.

    Args:
        horizon_type: 'pentad' or 'decad'.
        codes: Station codes to filter. None reads all.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        DataFrame with columns: [code, date, discharge_avg,
        model_short, pentad_in_year, pentad_in_month] (or decad
        equivalents). Empty DataFrame if no data available.

    Raises:
        ValueError: If horizon_type is invalid.
    """
    if horizon_type not in ("pentad", "decad"):
        raise ValueError(f"horizon_type must be 'pentad' or 'decad', got: {horizon_type}")

    # API-first
    raw = _read_short_term_runoff_api(horizon_type, codes, start_year, end_year)
    if raw is not None and not raw.empty:
        df = _normalize_observed_runoff(raw, horizon_type)
        logger.info(
            "Read %d short-term observations from API (%s)",
            len(df),
            horizon_type,
        )
        return df

    # CSV fallback (deprecated)
    logger.info(
        "API short-term observations unavailable for %s, falling back to CSV",
        horizon_type,
    )
    df = _read_short_term_observations_csv(horizon_type)
    if df is not None and not df.empty:
        logger.info(
            "Read %d short-term observations from CSV (%s)",
            len(df),
            horizon_type,
        )
        return df

    logger.warning("No short-term observations available for %s", horizon_type)
    return pd.DataFrame()


def _read_short_term_observations_csv(
    horizon_type: str,
) -> pd.DataFrame | None:
    """Read pentad/decad observations from CSV (deprecated fallback).

    Returns None if the file doesn't exist or can't be read.
    """
    intermediate_path = os.getenv("ieasyforecast_intermediate_data_path", "")

    if horizon_type == "pentad":
        filename = os.getenv("ieasyforecast_pentadal_discharge_file", "")
    else:
        filename = os.getenv("ieasyforecast_decadal_discharge_file", "")

    if not intermediate_path or not filename:
        logger.debug("Discharge CSV env vars not set for %s", horizon_type)
        return None

    filepath = os.path.join(intermediate_path, filename)
    if not os.path.exists(filepath):
        logger.debug("Discharge CSV not found: %s", filepath)
        return None

    try:
        df = pd.read_csv(filepath)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        df = _clean_code_column(df)
        if "model_short" not in df.columns:
            df["model_short"] = "Obs"
        return df
    except Exception as e:
        logger.error("Failed to read discharge CSV %s: %s", filepath, e)
        return None


def read_individual_model_forecasts(
    horizon_type: str,
    codes: list[str] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read all individual model forecasts (LR + ML) for a horizon.

    Args:
        horizon_type: 'pentad' or 'decad'.
        codes: Station codes to filter. None reads all.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        Tuple of (forecasts_df, stats_df).
        - forecasts_df: Concatenation of all model forecasts.
        - stats_df: Statistics from LR (q_mean, q_std_sigma, delta).

    Raises:
        ValueError: If horizon_type is invalid.
    """
    if horizon_type not in ("pentad", "decad"):
        raise ValueError(f"horizon_type must be 'pentad' or 'decad', got: {horizon_type}")

    all_forecasts = []
    stats = pd.DataFrame(columns=["date", "code", "q_mean", "q_std_sigma", "delta"])

    # 1. Read LR forecasts
    lr_raw = _read_lr_forecasts_pp_api(horizon_type, codes, start_year, end_year)
    if lr_raw is not None and not lr_raw.empty:
        lr_fc, lr_stats = _normalize_lr_forecasts(lr_raw, horizon_type)
        if not lr_fc.empty:
            all_forecasts.append(lr_fc)
            logger.info(
                "Read %d LR forecast rows from API (%s)",
                len(lr_fc),
                horizon_type,
            )
        if not lr_stats.empty:
            stats = lr_stats
    else:
        logger.info("No LR forecasts from API for %s", horizon_type)

    # 2. Read ML models (env-gated)
    run_ml = os.getenv("ieasyhydroforecast_run_ML_models", "false").lower()
    if run_ml == "true":
        available_models_str = os.getenv("ieasyhydroforecast_available_ML_models", "")
        # Env var uses uppercase (TIDE, TSMIXER); API expects camelCase.
        _ml_name_map = {"TIDE": "TiDE", "TSMIXER": "TSMixer", "TFT": "TFT"}
        if available_models_str:
            available_models = [
                _ml_name_map.get(m.strip().upper(), m.strip())
                for m in available_models_str.split(",")
                if m.strip()
            ]
        else:
            available_models = []

        for model in available_models:
            ml_raw = _read_ml_forecasts_pp_api(model, horizon_type, codes, start_year, end_year)
            if ml_raw is not None and not ml_raw.empty:
                ml_fc = _normalize_ml_forecasts(ml_raw, model, horizon_type)
                if not ml_fc.empty:
                    all_forecasts.append(ml_fc)
                    logger.info(
                        "Read %d %s forecast rows from API (%s)",
                        len(ml_fc),
                        model,
                        horizon_type,
                    )
            else:
                logger.info(
                    "No %s forecasts from API for %s",
                    model,
                    horizon_type,
                )

    if all_forecasts:
        forecasts = pd.concat(all_forecasts, ignore_index=True)
    else:
        forecasts = pd.DataFrame()

    return forecasts, stats


def read_individual_model_forecasts_for_dates(
    horizon_type: str,
    dates: list,
    codes: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read LR + ML forecasts scoped to a specific set of dates.

    More efficient than ``read_individual_model_forecasts()`` when only a
    small number of gap or stale dates need to be filled. Calls the full
    reader with year bounds derived from ``dates``, then filters in-memory
    to exact dates.

    Args:
        horizon_type: 'pentad' or 'decad'.
        dates: Boundary dates to fetch data for (Timestamp, date, or str).
        codes: Station codes to filter. None reads all.

    Returns:
        Same tuple as ``read_individual_model_forecasts()``:
        (forecasts_df, stats_df).

    Raises:
        ValueError: If horizon_type is invalid.
    """
    empty_stats = pd.DataFrame(columns=["date", "code", "q_mean", "q_std_sigma", "delta"])
    if not dates:
        return pd.DataFrame(), empty_stats

    dates_ts = pd.to_datetime(list(dates))
    min_year = int(dates_ts.year.min())
    max_year = int(dates_ts.year.max())

    forecasts, stats = read_individual_model_forecasts(
        horizon_type,
        codes=codes,
        start_year=min_year,
        end_year=max_year,
    )

    if forecasts.empty:
        return forecasts, stats

    if not pd.api.types.is_datetime64_any_dtype(forecasts["date"]):
        forecasts = forecasts.copy()
        forecasts["date"] = pd.to_datetime(forecasts["date"])

    date_set = set(dates_ts)
    forecasts = forecasts[forecasts["date"].isin(date_set)].copy()

    logger.info(
        "read_individual_model_forecasts_for_dates (%s): %d dates requested, %d rows returned",
        horizon_type,
        len(date_set),
        len(forecasts),
    )
    return forecasts, stats


def read_observed_and_modelled_data(
    horizon_type: str,
    codes: list[str] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read observed and modelled data for pentad or decad horizon.

    API-first reader that replaces
    setup_library.read_observed_and_modelled_data_pentade() and
    setup_library.read_observed_and_modelled_data_decade().

    Does NOT include NE or virtual station calculations -- those must
    be called separately from the entry point via
    sl.calculate_virtual_stations_data() and
    sl.calculate_neural_ensemble_forecast() /
    sl.calculate_neural_ensemble_forecast_decade().

    Args:
        horizon_type: 'pentad' or 'decad'.
        codes: Station codes to filter. None reads all.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        Tuple of (observed_df, modelled_df).
        - observed_df includes stats (q_mean, q_std_sigma, delta)
          merged from LR.
        - modelled_df contains all individual model forecasts.

    Raises:
        ValueError: If horizon_type is invalid.
    """
    if horizon_type not in ("pentad", "decad"):
        raise ValueError(f"horizon_type must be 'pentad' or 'decad', got: {horizon_type}")

    # Read observations
    observed = read_short_term_observations(horizon_type, codes, start_year, end_year)

    # Read individual model forecasts
    forecasts, stats = read_individual_model_forecasts(horizon_type, codes, start_year, end_year)

    # Merge stats into observed
    if (
        not stats.empty
        and not observed.empty
        and "date" in observed.columns
        and "code" in observed.columns
    ):
        merge_cols = ["date", "code"]
        stats_to_merge = stats.copy()
        if "date" in stats_to_merge.columns:
            stats_to_merge["date"] = pd.to_datetime(stats_to_merge["date"])
        observed = pd.merge(
            observed,
            stats_to_merge,
            on=merge_cols,
            how="left",
        )

    return observed, forecasts


# ===================================================================
# Quarterly skill metrics, observations, and forecasts
# ===================================================================


def read_quarterly_skill_metrics(
    codes: list[str] | None = None,
) -> pd.DataFrame:
    """Read pre-calculated quarterly skill metrics from API.

    API-only (no CSV fallback for new horizons).

    Args:
        codes: Optional list of station codes to filter. When provided,
            only skill metrics for those codes are returned. When None,
            all codes are returned.

    Returns:
        DataFrame with columns: [quarter_in_year, code, model_short,
        sdivsigma, nse, delta, accuracy, mae, n_pairs, ...]
    """
    df = _read_horizon_skill_metrics_api("quarter", codes)
    if df is not None and not df.empty:
        logger.info("Read %d quarterly skill metric rows from API", len(df))
        return df
    logger.warning("No quarterly skill metrics available")
    return pd.DataFrame()


def read_seasonal_skill_metrics(
    codes: list[str] | None = None,
) -> pd.DataFrame:
    """Read pre-calculated seasonal skill metrics from API.

    API-only (no CSV fallback for new horizons).

    Args:
        codes: Optional list of station codes to filter. When provided,
            only skill metrics for those codes are returned. When None,
            all codes are returned.

    Returns:
        DataFrame with columns: [season_in_year, code, model_short,
        sdivsigma, nse, delta, accuracy, mae, n_pairs, ...]
    """
    df = _read_horizon_skill_metrics_api("season", codes)
    if df is not None and not df.empty:
        logger.info("Read %d seasonal skill metric rows from API", len(df))
        return df
    logger.warning("No seasonal skill metrics available")
    return pd.DataFrame()


def _read_horizon_skill_metrics_api(
    horizon_type: str,
    codes: list[str] | None = None,
) -> pd.DataFrame | None:
    """Read skill metrics from API for an arbitrary horizon type.

    Shared implementation for quarter/season (and potentially others).
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping API read")
        return None

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping API read")
        return None

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Postprocessing API not ready at %s", api_url)
            return None

        batch_size = 1000
        if codes is not None:
            # Per-code loop: API supports code= but not batch code__in
            frames = []
            for code in codes:
                skip = 0
                while True:
                    df_batch = client.read_skill_metrics(
                        horizon=horizon_type,
                        code=code,
                        skip=skip,
                        limit=batch_size,
                    )
                    if df_batch is None or df_batch.empty:
                        break
                    frames.append(df_batch)
                    if len(df_batch) < batch_size:
                        break
                    skip += batch_size
            if not frames:
                return None
            df = pd.concat(frames, ignore_index=True)
        else:
            all_records = []
            skip = 0
            while True:
                df_batch = client.read_skill_metrics(
                    horizon=horizon_type, skip=skip, limit=batch_size
                )
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

            if not all_records:
                return None

            df = pd.concat(all_records, ignore_index=True)

        return _normalize_horizon_skill_metrics(df, horizon_type)

    except Exception as e:
        logger.error(
            "Failed to read %s skill metrics from API: %s",
            horizon_type,
            e,
        )
        return None


def _normalize_horizon_skill_metrics(
    df: pd.DataFrame,
    horizon_type: str,
) -> pd.DataFrame:
    """Normalize API skill metrics response for quarter/season horizons.

    Maps horizon_in_year → quarter_in_year or season_in_year,
    model_type → model_short.
    """
    period_col_map = {
        "quarter": "quarter_in_year",
        "season": "season_in_year",
    }
    period_col = period_col_map.get(horizon_type, f"{horizon_type}_in_year")

    rename_map = {
        "horizon_in_year": period_col,
        "model_type": "model_short",
    }
    df = df.rename(columns=rename_map)

    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)

    return df


# -------------------------------------------------------------------
# Quarterly/seasonal observations — delegate to monthly + aggregate
# -------------------------------------------------------------------


def read_quarterly_observations(
    codes: list[str],
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Read quarterly observations by aggregating monthly observations.

    Args:
        codes: Station codes to read.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        DataFrame with columns: [code, year, quarter_in_year,
        discharge_avg, delta].
    """
    from src.aggregation import aggregate_monthly_obs_to_quarterly

    monthly = read_monthly_observations(codes, start_year, end_year)
    if monthly.empty:
        return pd.DataFrame(
            columns=[
                "code",
                "year",
                "quarter_in_year",
                "discharge_avg",
                "delta",
            ]
        )
    return aggregate_monthly_obs_to_quarterly(monthly)


def read_seasonal_observations(
    codes: list[str],
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Read seasonal observations by aggregating monthly observations.

    Args:
        codes: Station codes to read.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        DataFrame with columns: [code, season_year, season_in_year,
        discharge_avg, delta].
    """
    from src.aggregation import aggregate_monthly_obs_to_seasonal

    monthly = read_monthly_observations(codes, start_year, end_year)
    if monthly.empty:
        return pd.DataFrame(
            columns=[
                "code",
                "season_year",
                "season_in_year",
                "discharge_avg",
                "delta",
            ]
        )
    return aggregate_monthly_obs_to_seasonal(monthly)


# -------------------------------------------------------------------
# Quarterly/seasonal forecasts — delegate to monthly + aggregate
# -------------------------------------------------------------------


def read_quarterly_forecasts(
    codes: list[str],
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Read quarterly forecasts from aggregated monthly and direct API sources.

    Combines two sources:
    1. Monthly forecasts aggregated to quarterly via
       ``aggregate_monthly_fc_to_quarterly``.
    2. Direct quarterly forecasts read from the API
       (``horizon_type="quarter"``).

    When a model appears in both sources for the same quarter, the
    direct quarterly forecast takes precedence.  Ensemble models
    (EM, Skilled Mean, Naive Mean) are filtered out of the direct
    path — only raw model output is returned.

    Args:
        codes: Station codes to read.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        DataFrame with columns: [code, year, quarter_in_year,
        model_short, q05-q95, forecasted_discharge, valid_from,
        valid_to].
    """
    from src.aggregation import aggregate_monthly_fc_to_quarterly

    empty_cols = [
        "code",
        "year",
        "quarter_in_year",
        "model_short",
    ]

    # Source 1: aggregate monthly forecasts to quarterly
    monthly = read_monthly_forecasts(codes, start_year, end_year)
    if not monthly.empty:
        aggregated = aggregate_monthly_fc_to_quarterly(monthly)
    else:
        aggregated = pd.DataFrame()

    # Source 2: direct quarterly forecasts from API
    raw_q = _read_long_forecasts_api(
        codes,
        start_year,
        end_year,
        horizon_type="quarter",
        horizon_value=quarter_horizon_value(),
    )
    if raw_q is not None and not raw_q.empty:
        direct = _normalize_combined_forecasts(raw_q, "quarter")
        # Filter out ensemble models — only raw model output
        if "model_short" in direct.columns:
            direct = direct[~direct["model_short"].isin(_ENSEMBLE_MODELS)].copy()
    else:
        direct = pd.DataFrame()

    # Combine sources
    if aggregated.empty and direct.empty:
        return pd.DataFrame(columns=empty_cols)

    if aggregated.empty:
        combined = direct
    elif direct.empty:
        combined = aggregated
    else:
        # Concat: aggregated first, direct second.
        # drop_duplicates(keep="last") prefers direct.
        combined = pd.concat([aggregated, direct], ignore_index=True)
        dedup_cols = ["code", "year", "quarter_in_year", "model_short"]
        available = [c for c in dedup_cols if c in combined.columns]
        combined = combined.drop_duplicates(subset=available, keep="last")

    if combined.empty:
        return pd.DataFrame(columns=empty_cols)

    # Select canonical output columns
    combined = combined[[c for c in _QUARTERLY_FC_COLS if c in combined.columns]]

    # Normalize valid_from/valid_to to strings for consistency
    for col in ("valid_from", "valid_to"):
        if col in combined.columns:
            combined[col] = combined[col].astype(str)

    return combined


def read_seasonal_forecasts(
    codes: list[str],
    start_year: int,
    end_year: int,
    horizon_value: int | None = None,
) -> pd.DataFrame:
    """Read seasonal forecasts directly from the API.

    Reads forecasts stored with horizon_type="season" in the
    postprocessing API.  Ensemble models (EM, Skilled Mean,
    Naive Mean) are filtered out — only raw model output is returned.

    Args:
        codes: Station codes to read.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).
        horizon_value: Optional seasonal issue lead to read.

    Returns:
        DataFrame with columns: [code, season_year, season_in_year,
        horizon_value, date, model_short, q05-q95,
        forecasted_discharge, valid_from, valid_to].
    """
    empty = pd.DataFrame(
        columns=[
            "code",
            "season_year",
            "season_in_year",
            "model_short",
        ]
    )

    raw = _read_long_forecasts_api(
        codes,
        start_year,
        end_year,
        horizon_type="season",
        horizon_value=horizon_value,
    )
    if raw is None or raw.empty:
        logger.info("No seasonal forecast data from API for %d-%d", start_year, end_year)
        return empty

    df = _normalize_combined_forecasts(raw, "season")
    if df.empty:
        return empty

    # Filter out ensemble models — only raw model output
    if "model_short" in df.columns:
        df = df[~df["model_short"].isin(_ENSEMBLE_MODELS)].copy()

    if df.empty:
        return empty

    # Select canonical output columns
    df = df[[c for c in _SEASONAL_FC_COLS if c in df.columns]]
    df = _deduplicate_seasonal_forecasts(df)

    # Normalize valid_from/valid_to to strings for consistency
    for col in ("valid_from", "valid_to", "date"):
        if col in df.columns:
            df[col] = df[col].astype(str)

    return df


# -------------------------------------------------------------------
# Latest quarterly/seasonal forecasts (for operational entry point)
# -------------------------------------------------------------------


def read_latest_quarterly_forecasts(
    codes: list[str],
    forecast_date: dt.date | None = None,
) -> pd.DataFrame:
    """Read latest quarterly forecasts from aggregated monthly and direct API.

    Combines two sources:
    1. Monthly forecasts (120-day lookback) aggregated to quarterly.
    2. Direct quarterly forecasts from the API.

    When a model appears in both sources, the direct forecast wins.

    Args:
        codes: Station codes to read.
        forecast_date: Reference date for lookback window.

    Returns:
        DataFrame with quarterly forecasts for the most recent
        quarter. Empty DataFrame if no data.
    """
    from src.aggregation import (
        aggregate_monthly_fc_to_quarterly,
    )

    today = forecast_date if forecast_date is not None else dt.date.today()
    start_date = today - dt.timedelta(days=120)
    start_year = start_date.year
    end_year = today.year

    # Source 1: aggregate monthly forecasts to quarterly
    raw_m = _read_long_forecasts_api(codes, start_year, end_year)
    if raw_m is not None and not raw_m.empty:
        df_m = _normalize_monthly_forecasts(raw_m)
        if "forecasted_discharge" not in df_m.columns and "q50" in df_m.columns:
            df_m["forecasted_discharge"] = df_m["q50"].astype(float)
        aggregated = aggregate_monthly_fc_to_quarterly(df_m)
    else:
        aggregated = pd.DataFrame()

    # Source 2: direct quarterly forecasts from API
    raw_q = _read_long_forecasts_api(
        codes,
        start_year,
        end_year,
        horizon_type="quarter",
        horizon_value=quarter_horizon_value(),
    )
    if raw_q is not None and not raw_q.empty:
        direct = _normalize_combined_forecasts(raw_q, "quarter")
        if "model_short" in direct.columns:
            direct = direct[~direct["model_short"].isin(_ENSEMBLE_MODELS)].copy()
    else:
        direct = pd.DataFrame()

    # Combine sources
    if aggregated.empty and direct.empty:
        logger.warning("No quarterly forecast data available")
        return pd.DataFrame(columns=_QUARTERLY_FC_COLS)

    if aggregated.empty:
        combined = direct
    elif direct.empty:
        combined = aggregated
    else:
        combined = pd.concat([aggregated, direct], ignore_index=True)
        dedup_cols = ["code", "year", "quarter_in_year", "model_short"]
        available = [c for c in dedup_cols if c in combined.columns]
        combined = combined.drop_duplicates(subset=available, keep="last")

    if combined.empty:
        return pd.DataFrame(columns=_QUARTERLY_FC_COLS)

    # Select canonical output columns
    combined = combined[[c for c in _QUARTERLY_FC_COLS if c in combined.columns]]

    # Normalize valid_from/valid_to to strings
    for col in ("valid_from", "valid_to"):
        if col in combined.columns:
            combined[col] = combined[col].astype(str)

    # Filter to the most recent quarter
    max_year = int(combined["year"].max())
    max_q = int(combined[combined["year"] == max_year]["quarter_in_year"].max())
    combined = combined[
        (combined["year"] == max_year) & (combined["quarter_in_year"] == max_q)
    ].copy()

    logger.info(
        "Read %d latest quarterly forecasts for Q%d-%d",
        len(combined),
        max_q,
        max_year,
    )
    return combined


def read_latest_seasonal_forecasts(
    codes: list[str],
    forecast_date: dt.date | None = None,
    horizon_value: int | None = None,
) -> pd.DataFrame:
    """Read the most recent seasonal forecasts directly from the API.

    Uses a wide lookback (~200 days) to capture cross-year seasons.

    Args:
        codes: Station codes to read.
        forecast_date: Reference date for lookback window.
        horizon_value: Optional seasonal issue lead to read.

    Returns:
        DataFrame with seasonal forecasts for the most recent season.
        Empty DataFrame if no data.
    """
    today = forecast_date if forecast_date is not None else dt.date.today()
    start_date = today - dt.timedelta(days=200)
    start_year = start_date.year
    end_year = today.year

    raw = _read_long_forecasts_api(
        codes,
        start_year,
        end_year,
        horizon_type="season",
        horizon_value=horizon_value,
    )
    if raw is None or raw.empty:
        logger.warning("No recent seasonal forecast data from API")
        return pd.DataFrame(columns=_SEASONAL_FC_COLS)

    df = _normalize_combined_forecasts(raw, "season")
    if df.empty:
        return pd.DataFrame(columns=_SEASONAL_FC_COLS)

    # Filter out ensemble models — only raw model output
    if "model_short" in df.columns:
        df = df[~df["model_short"].isin(_ENSEMBLE_MODELS)].copy()

    if df.empty:
        return pd.DataFrame(columns=_SEASONAL_FC_COLS)

    # Select canonical output columns
    df = df[[c for c in _SEASONAL_FC_COLS if c in df.columns]]
    df = _deduplicate_seasonal_forecasts(df)

    # Normalize valid_from/valid_to to strings
    for col in ("valid_from", "valid_to", "date"):
        if col in df.columns:
            df[col] = df[col].astype(str)

    # Filter to the most recent season_year
    max_sy = int(df["season_year"].max())
    df = df[df["season_year"] == max_sy].copy()

    logger.info(
        "Read %d latest seasonal forecasts for season_year %d",
        len(df),
        max_sy,
    )
    return df


# -------------------------------------------------------------------
# Quarterly/seasonal combined forecasts (from API)
# -------------------------------------------------------------------


def read_quarterly_combined_forecasts(
    codes: list[str] | None = None,
) -> pd.DataFrame:
    """Read quarterly combined forecasts from API.

    API-only — no CSV fallback for new horizons.

    Args:
        codes: Optional list of station codes to filter. When provided,
            only forecasts for those codes are returned. When None,
            all codes are returned.

    Returns:
        DataFrame with combined quarterly forecasts, or empty DataFrame.
    """
    df = _read_long_combined_forecasts_api(
        "quarter",
        codes,
        horizon_value=quarter_horizon_value(),
    )
    if df is not None and not df.empty:
        logger.info("Read %d quarterly combined forecast rows from API", len(df))
        return df
    logger.warning("No quarterly combined forecasts available")
    return pd.DataFrame()


def read_seasonal_combined_forecasts(
    codes: list[str] | None = None,
    horizon_value: int | None = None,
) -> pd.DataFrame:
    """Read seasonal combined forecasts from API.

    API-only — no CSV fallback for new horizons.

    Args:
        codes: Optional list of station codes to filter. When provided,
            only forecasts for those codes are returned. When None,
            all codes are returned.
        horizon_value: Optional seasonal issue lead to read.

    Returns:
        DataFrame with combined seasonal forecasts, or empty DataFrame.
    """
    df = _read_long_combined_forecasts_api("season", codes, horizon_value=horizon_value)
    if df is not None and not df.empty:
        logger.info("Read %d seasonal combined forecast rows from API", len(df))
        return df
    logger.warning("No seasonal combined forecasts available")
    return pd.DataFrame()


def _read_long_combined_forecasts_api(
    horizon_type: str,
    codes: list[str] | None = None,
    horizon_value: int | None = None,
) -> pd.DataFrame | None:
    """Read long-term combined forecasts from API for a given horizon type.

    Shared implementation for quarter/season.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping API read")
        return None

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping API read")
        return None

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Postprocessing API not ready at %s", api_url)
            return None

        batch_size = 1000
        if codes is not None:
            # Per-code loop: API supports code= but not batch code__in
            frames = []
            for code in codes:
                skip = 0
                while True:
                    kwargs = {
                        "horizon_type": horizon_type,
                        "code": code,
                        "skip": skip,
                        "limit": batch_size,
                    }
                    if horizon_value is not None:
                        kwargs["horizon_value"] = horizon_value
                    df_batch = client.read_long_term_forecasts(**kwargs)
                    if df_batch is None or df_batch.empty:
                        break
                    frames.append(df_batch)
                    if len(df_batch) < batch_size:
                        break
                    skip += batch_size
            if not frames:
                return None
            df = pd.concat(frames, ignore_index=True)
        else:
            all_records = []
            skip = 0
            while True:
                kwargs = {
                    "horizon_type": horizon_type,
                    "skip": skip,
                    "limit": batch_size,
                }
                if horizon_value is not None:
                    kwargs["horizon_value"] = horizon_value
                df_batch = client.read_long_term_forecasts(**kwargs)
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

            if not all_records:
                return None

            df = pd.concat(all_records, ignore_index=True)

        return _normalize_combined_forecasts(df, horizon_type)

    except Exception as e:
        logger.error(
            "Failed to read %s combined forecasts from API: %s",
            horizon_type,
            e,
        )
        return None


def _normalize_combined_forecasts(
    df: pd.DataFrame,
    horizon_type: str,
) -> pd.DataFrame:
    """Normalize API combined forecast response for quarter/season.

    Extracts year/quarter/season from valid_from, renames model_type
    to model_short, adds derived columns.
    """
    from src.aggregation import MONTH_TO_QUARTER, get_season_year

    df = df.copy()

    # Parse valid_from for year extraction
    df["valid_from"] = pd.to_datetime(df["valid_from"])

    if "model_type" in df.columns:
        df = df.rename(columns={"model_type": "model_short"})

    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)

    if horizon_type == "quarter":
        df["year"] = df["valid_from"].dt.year
        month = df["valid_from"].dt.month
        df["quarter_in_year"] = month.map(MONTH_TO_QUARTER)
    elif horizon_type == "season":
        df["season_year"] = df.apply(
            lambda r: get_season_year(r["valid_from"].year, r["valid_from"].month),
            axis=1,
        )
        if "horizon_value" in df.columns:
            lead = pd.to_numeric(df["horizon_value"], errors="coerce")
            df["season_in_year"] = lead.astype("Int64") if lead.isna().any() else lead.astype(int)
        else:
            df["season_in_year"] = 1
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Add forecasted_discharge from q/q50
    if "forecasted_discharge" not in df.columns:
        if "q" in df.columns:
            df["forecasted_discharge"] = pd.to_numeric(df["q"], errors="coerce")
        elif "q50" in df.columns:
            df["forecasted_discharge"] = df["q50"].astype(float)

    # Drop API-only columns
    drop_cols = [
        "id",
        "horizon_type",
        "model_type_description",
    ]
    if horizon_type != "season":
        drop_cols.append("horizon_value")
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

    return df


def _deduplicate_seasonal_forecasts(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicate seasonal issue/model rows without folding leads."""
    if df.empty:
        return df

    dedup_cols = ["code", "season_year", "season_in_year", "date", "model_short"]
    available = [c for c in dedup_cols if c in df.columns]
    if len(available) < 4:
        return df
    return df.drop_duplicates(subset=available, keep="last")
