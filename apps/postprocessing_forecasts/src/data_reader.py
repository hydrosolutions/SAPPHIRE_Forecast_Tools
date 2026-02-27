"""Read pre-calculated skill metrics, combined forecasts, and monthly
data from API or CSV (deprecated fallback).

Used by the operational and maintenance entry points to avoid
recalculating skill metrics from scratch, by the maintenance entry
point to read combined forecasts for gap detection, and by the yearly
recalculation entry point to read monthly observations and forecasts.
"""

import datetime as dt
import os
import logging

import numpy as np
import pandas as pd

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


def read_skill_metrics(horizon_type: str) -> pd.DataFrame:
    """Read pre-calculated skill metrics from API (primary) or CSV (fallback).

    Args:
        horizon_type: 'pentad', 'decad', or 'month'

    Returns:
        DataFrame with columns: [pentad_in_year|decad_in_year, code,
        model_short, sdivsigma, nse, delta, accuracy, mae, n_pairs]

    Raises:
        ValueError: If horizon_type is invalid.
    """
    if horizon_type not in ("pentad", "decad", "month"):
        raise ValueError(
            f"horizon_type must be 'pentad', 'decad', or 'month', "
            f"got: {horizon_type}"
        )

    if horizon_type == "month":
        return read_monthly_skill_metrics()

    # API-first: try the authoritative source
    df = _read_skill_metrics_api(horizon_type)
    if df is not None and not df.empty:
        logger.info(
            "Read %d skill metric rows from API (%s)",
            len(df), horizon_type,
        )
        return df

    # CSV fallback (deprecated): only used when API is unavailable
    logger.info(
        "API skill metrics unavailable for %s, falling back to CSV",
        horizon_type,
    )
    df = _read_skill_metrics_csv(horizon_type)
    if df is not None and not df.empty:
        logger.info(
            "Read %d skill metric rows from CSV (%s)",
            len(df), horizon_type,
        )
        return df

    logger.warning("No skill metrics available for %s", horizon_type)
    return pd.DataFrame()


def _read_skill_metrics_csv(horizon_type: str) -> pd.DataFrame | None:
    """Read skill metrics from CSV file.

    Returns None if the file doesn't exist or can't be read.
    """
    intermediate_path = os.getenv("ieasyforecast_intermediate_data_path", "")

    if horizon_type == "pentad":
        filename = os.getenv(
            "ieasyforecast_pentadal_skill_metrics_file", ""
        )
    else:
        filename = os.getenv(
            "ieasyforecast_decadal_skill_metrics_file", ""
        )

    if not intermediate_path or not filename:
        logger.debug(
            "Skill metrics env vars not set for %s", horizon_type
        )
        return None

    filepath = os.path.join(intermediate_path, filename)
    if not os.path.exists(filepath):
        logger.debug("Skill metrics CSV not found: %s", filepath)
        return None

    try:
        df = pd.read_csv(filepath)
        # Ensure code is string
        if "code" in df.columns:
            df["code"] = df["code"].astype(str).str.replace(
                r"\.0$", "", regex=True
            )
        return df
    except Exception as e:
        logger.error("Failed to read skill metrics CSV %s: %s", filepath, e)
        return None


def _read_skill_metrics_api(horizon_type: str) -> pd.DataFrame | None:
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

        # Read all skill metrics for this horizon; paginate if needed
        all_records = []
        skip = 0
        batch_size = 1000
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


def _normalize_api_skill_metrics(
    df: pd.DataFrame, horizon_type: str
) -> pd.DataFrame:
    """Convert API column names to CSV-compatible column names.

    API returns: horizon_in_year, model_type, code, sdivsigma, nse,
                 delta, accuracy, mae, n_pairs, crps, pbias, kgelf,
                 nse_log
    CSV expects: pentad_in_year|decad_in_year, model_short,
                 code, sdivsigma, nse, delta, accuracy, mae, n_pairs,
                 pbias, kgelf, nse_log
    """
    period_col = (
        "pentad_in_year" if horizon_type == "pentad" else "decad_in_year"
    )

    # Rename API columns
    rename_map = {
        "horizon_in_year": period_col,
        "model_type": "model_short",
    }
    df = df.rename(columns=rename_map)

    # Ensure code is string
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.replace(
            r"\.0$", "", regex=True
        )

    return df


# ===================================================================
# Monthly skill metrics
# ===================================================================


def read_monthly_skill_metrics() -> pd.DataFrame:
    """Read pre-calculated monthly skill metrics from API or CSV.

    Returns:
        DataFrame with columns: [month_in_year, code, model_short,
        sdivsigma, nse, delta, accuracy, mae, n_pairs]
    """
    # API-first: try the authoritative source
    df = _read_monthly_skill_metrics_api()
    if df is not None and not df.empty:
        logger.info(
            "Read %d monthly skill metric rows from API", len(df)
        )
        return df

    # CSV fallback (deprecated)
    logger.info(
        "API monthly skill metrics unavailable, falling back to CSV"
    )
    df = _read_monthly_skill_metrics_csv()
    if df is not None and not df.empty:
        logger.info(
            "Read %d monthly skill metric rows from CSV", len(df)
        )
        return df

    logger.warning("No monthly skill metrics available")
    return pd.DataFrame()


def _read_monthly_skill_metrics_csv() -> pd.DataFrame | None:
    """Read monthly skill metrics from CSV file.

    Returns None if the file doesn't exist or can't be read.
    """
    intermediate_path = os.getenv(
        "ieasyforecast_intermediate_data_path", ""
    )
    filename = os.getenv(
        "ieasyforecast_monthly_skill_metrics_file", ""
    )

    if not intermediate_path or not filename:
        logger.debug(
            "Monthly skill metrics env vars not set"
        )
        return None

    filepath = os.path.join(intermediate_path, filename)
    if not os.path.exists(filepath):
        logger.debug(
            "Monthly skill metrics CSV not found: %s", filepath
        )
        return None

    try:
        df = pd.read_csv(filepath)
        if "code" in df.columns:
            df["code"] = df["code"].astype(str).str.replace(
                r"\.0$", "", regex=True
            )
        return df
    except Exception as e:
        logger.error(
            "Failed to read monthly skill metrics CSV %s: %s",
            filepath, e,
        )
        return None


def _read_monthly_skill_metrics_api() -> pd.DataFrame | None:
    """Read monthly skill metrics from SAPPHIRE postprocessing API.

    Returns None if the API is unavailable or returns no data.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug(
            "sapphire-api-client not installed, skipping API read"
        )
        return None

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping API read")
        return None

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning(
                "Postprocessing API not ready at %s", api_url
            )
            return None

        all_records = []
        skip = 0
        batch_size = 1000
        while True:
            df_batch = client.read_skill_metrics(
                horizon="month", skip=skip, limit=batch_size
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
        return _normalize_api_monthly_skill_metrics(df)

    except Exception as e:
        logger.error(
            "Failed to read monthly skill metrics from API: %s", e
        )
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
        df["code"] = df["code"].astype(str).str.replace(
            r"\.0$", "", regex=True
        )

    return df


# ===================================================================
# Short-term combined forecasts (pentad / decad)
# ===================================================================


def read_combined_forecasts(horizon_type: str) -> pd.DataFrame:
    """Read combined forecasts from API (primary) or CSV (fallback).

    Used by the maintenance entry point for gap detection and
    merge-back after filling missing ensembles.

    Args:
        horizon_type: 'pentad' or 'decad'.

    Returns:
        DataFrame with combined forecasts (all models + ensembles),
        or empty DataFrame if no data available.

    Raises:
        ValueError: If horizon_type is invalid.
    """
    if horizon_type not in ("pentad", "decad"):
        raise ValueError(
            f"horizon_type must be 'pentad' or 'decad', "
            f"got: {horizon_type}"
        )

    # API-first: try the authoritative source
    df = _read_combined_forecasts_api(horizon_type)
    if df is not None and not df.empty:
        logger.info(
            "Read %d combined forecast rows from API (%s)",
            len(df), horizon_type,
        )
        return df

    # CSV fallback (deprecated)
    logger.info(
        "API combined forecasts unavailable for %s, "
        "falling back to CSV",
        horizon_type,
    )
    df = _read_combined_forecasts_csv(horizon_type)
    if df is not None and not df.empty:
        logger.info(
            "Read %d combined forecast rows from CSV (%s)",
            len(df), horizon_type,
        )
        return df

    logger.warning(
        "No combined forecasts available for %s", horizon_type
    )
    return pd.DataFrame()


def _read_combined_forecasts_api(
    horizon_type: str,
) -> pd.DataFrame | None:
    """Read combined forecasts from SAPPHIRE postprocessing API.

    Returns None if the API is unavailable or returns no data.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug(
            "sapphire-api-client not installed, skipping API read"
        )
        return None

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping API read")
        return None

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning(
                "Postprocessing API not ready at %s", api_url
            )
            return None

        # Map internal horizon names to API horizon names
        api_horizon = (
            "decade" if horizon_type == "decad" else horizon_type
        )

        all_records = []
        skip = 0
        batch_size = 1000
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
        logger.error(
            "Failed to read combined forecasts from API: %s", e
        )
        return None


def _normalize_api_combined_forecasts(
    df: pd.DataFrame, horizon_type: str
) -> pd.DataFrame:
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

    period_col = (
        "pentad_in_year"
        if horizon_type == "pentad"
        else "decad_in_year"
    )
    period_in_month_col = (
        "pentad_in_month"
        if horizon_type == "pentad"
        else "decad_in_month"
    )

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
        df["code"] = df["code"].astype(str).str.replace(
            r"\.0$", "", regex=True
        )

    # Drop API-only columns not needed internally
    drop_cols = ["id", "horizon_type", "model_type_description"]
    df = df.drop(
        columns=[c for c in drop_cols if c in df.columns],
        errors="ignore",
    )

    return df


def _read_combined_forecasts_csv(
    horizon_type: str,
) -> pd.DataFrame | None:
    """Read combined forecasts from CSV file.

    Returns None if the file doesn't exist or can't be read.
    """
    intermediate_path = os.getenv(
        "ieasyforecast_intermediate_data_path", ""
    )

    if horizon_type == "pentad":
        filename = os.getenv(
            "ieasyforecast_combined_forecast_pentad_file", ""
        )
    else:
        filename = os.getenv(
            "ieasyforecast_combined_forecast_decad_file", ""
        )

    if not intermediate_path or not filename:
        logger.debug(
            "Combined forecast env vars not set for %s",
            horizon_type,
        )
        return None

    filepath = os.path.join(intermediate_path, filename)
    if not os.path.exists(filepath):
        logger.debug(
            "Combined forecasts CSV not found: %s", filepath
        )
        return None

    try:
        df = pd.read_csv(filepath)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        if "code" in df.columns:
            df["code"] = df["code"].astype(str).str.replace(
                r"\.0$", "", regex=True
            )
        return df
    except Exception as e:
        logger.error(
            "Failed to read combined forecasts CSV %s: %s",
            filepath, e,
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
    empty = pd.DataFrame(
        columns=["code", "date", "discharge_avg"]
    )

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
        df["code"] = df["code"].astype(str).str.replace(
            r"\.0$", "", regex=True
        )

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
    empty = pd.DataFrame(
        columns=["code", "date", "model_short",
                 "forecasted_discharge"]
    )

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
            logger.warning(
                "Postprocessing API not ready at %s", api_url
            )
            return empty

        all_records = []
        start_date = f"{start_year}-01-01"
        end_date = f"{end_year}-12-31"

        for code in codes:
            skip = 0
            batch_size = 1000
            while True:
                df_batch = client.read_forecasts(
                    horizon_type="day",
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

        if not all_records:
            return empty

        df = pd.concat(all_records, ignore_index=True)
        return _normalize_daily_forecasts(df)

    except Exception as e:
        logger.error(
            "Failed to read daily forecasts from API: %s", e
        )
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
    if "target" in df.columns:
        df = df.rename(columns={"target": "date"})

    # Ensure types
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.replace(
            r"\.0$", "", regex=True
        )
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    # Deduplicate: keep latest forecast_date per (code, date, model)
    date_col = "forecast_date" if "forecast_date" in df.columns else "date"
    if "forecast_date" in df.columns:
        df["forecast_date"] = pd.to_datetime(df["forecast_date"])
        df = df.sort_values("forecast_date", ascending=False)
        df = df.drop_duplicates(
            subset=["code", "date", "model_short"], keep="first"
        )

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
        columns=["code", "year", "month", "month_in_year",
                 "discharge_avg", "delta"]
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

        if not all_records:
            return pd.DataFrame()

        return pd.concat(all_records, ignore_index=True)

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
    monthly = df.groupby(["code", "year", "month"]).agg(
        discharge_avg=("discharge_avg", "mean"),
        non_missing_days=("discharge_avg", "count"),
        days_in_month=("days_in_month", "first"),
    ).reset_index()

    # Filter: require >= 50% non-missing days
    monthly = monthly[
        monthly["non_missing_days"] >= monthly["days_in_month"] * 0.5
    ].copy()

    if monthly.empty:
        return pd.DataFrame(
            columns=["code", "year", "month", "month_in_year",
                     "discharge_avg", "delta"]
        )

    monthly["month_in_year"] = monthly["month"]

    # Compute delta per (code, month_in_year): 0.674 * std across years
    delta_df = monthly.groupby(["code", "month_in_year"]).agg(
        std_discharge=("discharge_avg", "std"),
    ).reset_index()
    # Single year -> std is NaN -> delta = 0
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)

    monthly = monthly.merge(
        delta_df[["code", "month_in_year", "delta"]],
        on=["code", "month_in_year"],
        how="left",
    )

    # Drop intermediate columns
    monthly = monthly.drop(
        columns=["non_missing_days", "days_in_month"], errors="ignore"
    )

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
) -> pd.DataFrame:
    """Read long-term forecasts from postprocessing API with pagination."""
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
            logger.warning(
                "Postprocessing API not ready at %s", api_url
            )
            return pd.DataFrame()

        all_records = []
        start_date = f"{start_year}-01-01"
        end_date = f"{end_year}-12-31"

        for code in codes:
            skip = 0
            batch_size = 1000
            while True:
                df_batch = client.read_long_term_forecasts(
                    horizon_type="month",
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

        if not all_records:
            return pd.DataFrame()

        return pd.concat(all_records, ignore_index=True)

    except Exception as e:
        logger.error(
            "Failed to read long-term forecasts from API: %s", e
        )
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
        df["code"] = df["code"].astype(str).str.replace(
            r"\.0$", "", regex=True
        )

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
        latest_month = int(
            df[df["year"] == latest_year]["month"].max()
        )

    df = df[
        (df["year"] == latest_year) & (df["month"] == latest_month)
    ].copy()

    logger.info(
        "Read %d latest monthly forecasts for %d-%02d",
        len(df), latest_year, latest_month,
    )
    return df


def read_monthly_combined_forecasts() -> pd.DataFrame:
    """Read monthly combined forecasts from API (primary) or CSV
    (fallback).

    Used by the maintenance entry point for gap detection and
    merge-back after filling missing ensembles.

    Returns:
        DataFrame with combined forecasts (all models + ensembles),
        or empty DataFrame if no data available.
    """
    # API-first: try the authoritative source
    df = _read_monthly_combined_forecasts_api()
    if df is not None and not df.empty:
        logger.info(
            "Read %d monthly combined forecast rows from API",
            len(df),
        )
        return df

    # CSV fallback (deprecated)
    logger.info(
        "API monthly combined forecasts unavailable, "
        "falling back to CSV"
    )
    df = _read_monthly_combined_forecasts_csv()
    if df is not None and not df.empty:
        logger.info(
            "Read %d monthly combined forecast rows from CSV",
            len(df),
        )
        return df

    logger.warning("No monthly combined forecasts available")
    return pd.DataFrame()


def _read_monthly_combined_forecasts_api() -> pd.DataFrame | None:
    """Read monthly combined forecasts from SAPPHIRE postprocessing API.

    Returns None if the API is unavailable or returns no data.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug(
            "sapphire-api-client not installed, skipping API read"
        )
        return None

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping API read")
        return None

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning(
                "Postprocessing API not ready at %s", api_url
            )
            return None

        all_records = []
        skip = 0
        batch_size = 1000
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
    if (
        "forecasted_discharge" not in df.columns
        and "q50" in df.columns
    ):
        df["forecasted_discharge"] = df["q50"].astype(float)

    # Drop API-only columns not needed internally
    drop_cols = ["id", "horizon_type", "horizon_value",
                 "model_type_description"]
    df = df.drop(
        columns=[c for c in drop_cols if c in df.columns],
        errors="ignore",
    )

    return df


def _read_monthly_combined_forecasts_csv() -> pd.DataFrame | None:
    """Read monthly combined forecasts from CSV file.

    Returns None if the file doesn't exist or can't be read.
    """
    intermediate_path = os.getenv(
        "ieasyforecast_intermediate_data_path", ""
    )
    filename = os.getenv(
        "ieasyforecast_monthly_combined_forecast_file", ""
    )

    if not intermediate_path or not filename:
        logger.debug(
            "Monthly combined forecast env vars not set"
        )
        return None

    filepath = os.path.join(intermediate_path, filename)
    if not os.path.exists(filepath):
        logger.debug(
            "Monthly combined forecasts CSV not found: %s", filepath
        )
        return None

    try:
        df = pd.read_csv(filepath)
        if "code" in df.columns:
            df["code"] = df["code"].astype(str).str.replace(
                r"\.0$", "", regex=True
            )
        return df
    except Exception as e:
        logger.error(
            "Failed to read monthly combined forecasts CSV %s: %s",
            filepath, e,
        )
        return None
