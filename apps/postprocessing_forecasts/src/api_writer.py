"""Write postprocessing data (forecasts and skill metrics) to the SAPPHIRE API.

Extracted from forecast_library.py — these functions are exclusively
used by postprocessing_forecasts.

Follows the same singleton pattern as data_reader.py.
"""

import datetime as dt_module
import json
import logging
import os

import pandas as pd
import tag_library as tl
from long_term_horizon_resolver import quarter_horizon_value
from skill_lead_aware_flag import skill_lead_aware_enabled

logger = logging.getLogger(__name__)

# Map uppercased model_short to API model_type format.
# Short-term models (pentad/decad) and long-term models (monthly+).
MODEL_TYPE_MAP = {
    # Short-term forecasting models
    "LR": "LR",
    "TFT": "TFT",
    "TIDE": "TiDE",
    "TSMIXER": "TSMixer",
    "EM": "EM",
    "NE": "NE",
    "RRAM": "RRAM",
    # Long-term forecasting models
    "GBT": "GBT",
    "LR_BASE": "LR_Base",
    "LR_SM": "LR_SM",
    "LR_SM_DT": "LR_SM_DT",
    "LR_SM_ROF": "LR_SM_ROF",
    "MC_ALD": "MC_ALD",
    "SM_GBT": "SM_GBT",
    "SM_GBT_LR": "SM_GBT_LR",
    "SM_GBT_NORM": "SM_GBT_Norm",
    # Ensemble aggregates computed in postprocessing (not baselines):
    #   Naive Mean   = unweighted mean of all models
    #   Skilled Mean = skill-weighted (1/MAE) mean of the skilled models
    #   (EM = Ensemble Mean is listed above with the short-term models.)
    "NAIVE MEAN": "Naive Mean",
    "SKILLED MEAN": "Skilled Mean",
    # DB-form aliases (CSV-fallback read may yield these instead of value-form)
    "ENSEMBLE_MEAN": "EM",
    "NAIVE_MEAN": "Naive Mean",
    "SKILLED_MEAN": "Skilled Mean",
}

# Map internal horizon type names to API enum values.
# Internal code uses "decad"; the PostgreSQL enum uses "decade".
HORIZON_TYPE_TO_API = {
    "pentad": "pentad",
    "decad": "decade",
    "month": "month",
    "day": "day",
    "quarter": "quarter",
    "season": "season",
}

# ---------------------------------------------------------------------------
# API client availability
# ---------------------------------------------------------------------------
try:
    from sapphire_api_client.postprocessing import (
        SapphirePostprocessingClient,
    )

    SAPPHIRE_API_AVAILABLE = True
except ImportError:
    SAPPHIRE_API_AVAILABLE = False
    SapphirePostprocessingClient = None


# ---------------------------------------------------------------------------
# Singleton client
# ---------------------------------------------------------------------------
_postprocessing_client = None
_configured_codes: set[str] | None = None


def _load_configured_codes() -> set[str]:
    """Load station codes from config_station_selection.json.

    Returns empty set if config is unavailable (non-blocking).
    """
    global _configured_codes
    if _configured_codes is not None:
        return _configured_codes
    try:
        config_path = os.path.join(
            os.getenv("ieasyforecast_configuration_path", ""),
            os.getenv("ieasyforecast_config_file_station_selection", ""),
        )
        if not config_path.strip("/"):
            _configured_codes = set()
            return _configured_codes
        with open(config_path) as f:
            data = json.load(f)
        _configured_codes = {str(c) for c in data.get("stationsID", [])}
        decad_file = os.getenv("ieasyforecast_config_file_station_selection_decad", "")
        if decad_file:
            decad_path = os.path.join(
                os.getenv("ieasyforecast_configuration_path", ""),
                decad_file,
            )
            if os.path.exists(decad_path):
                with open(decad_path) as f:
                    decad_data = json.load(f)
                _configured_codes |= {str(c) for c in decad_data.get("stationsID", [])}
    except (FileNotFoundError, json.JSONDecodeError, TypeError):
        logger.debug("Could not load station selection config for write guard")
        _configured_codes = set()
    return _configured_codes


def _check_write_codes(batch_codes: set[str], context: str) -> None:
    """Warn if batch contains codes outside the configured station list.

    Non-blocking: logs warning only, never raises.
    """
    configured = _load_configured_codes()
    if not configured:
        return
    unexpected = batch_codes - configured
    if unexpected:
        logger.warning(
            "WRITE GUARD [%s]: batch contains %d code(s) not in station "
            "selection config: %s (configured: %d codes). This may indicate "
            "cross-org data leakage.",
            context,
            len(unexpected),
            sorted(unexpected)[:5],
            len(configured),
        )


def _get_postprocessing_client():
    """Return a cached SapphirePostprocessingClient (singleton).

    The client is created lazily on first call, using SAPPHIRE_API_URL.
    Returns None if sapphire-api-client is not installed.
    """
    global _postprocessing_client
    if _postprocessing_client is not None:
        return _postprocessing_client
    if not SAPPHIRE_API_AVAILABLE or SapphirePostprocessingClient is None:
        return None
    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    _postprocessing_client = SapphirePostprocessingClient(base_url=api_url)
    return _postprocessing_client


def _reset_api_client():
    """Reset cached API client (for testing)."""
    global _postprocessing_client, _configured_codes
    _postprocessing_client = None
    _configured_codes = None


# ---------------------------------------------------------------------------
# Write functions
# ---------------------------------------------------------------------------


def _write_combined_forecast_to_api(data: pd.DataFrame, horizon_type: str) -> bool:
    """
    Write combined forecasts (ML + ensemble models) to SAPPHIRE postprocessing
    API.  LR forecasts are excluded — they live exclusively in the
    ``lr_forecasts`` table, written by the linear-regression module.

    Args:
        data: DataFrame with forecast data. Expected columns:
            - code: station code
            - date: forecast date
            - pentad_in_month/decad: horizon value
            - pentad_in_year/decad_in_year: horizon in year
            - forecasted_discharge: the forecast value
            - model_short: model identifier (TFT, TIDE, TSMIXER, EM, NE, …)
            - composition (optional): for ensemble models, which models compose it
        horizon_type: "pentad" or "decad" (translated to API enum at boundary)

    Returns:
        bool: True if successful, False otherwise
    """
    # LR forecasts live exclusively in the lr_forecasts table (written by
    # the linear_regression module).  Exclude them here to avoid duplication.
    if "model_short" in data.columns:
        n_before = len(data)
        data = data[data["model_short"] != "LR"]
        n_dropped = n_before - len(data)
        if n_dropped > 0:
            logger.debug(
                "Excluded %d LR rows from combined forecast write (%s)",
                n_dropped,
                horizon_type,
            )
        if data.empty:
            logger.info("No non-LR forecast records to write (%s)", horizon_type)
            return False

    # Validate inputs before any I/O
    api_horizon_type = HORIZON_TYPE_TO_API.get(horizon_type)
    if api_horizon_type is None:
        raise ValueError(
            f"Invalid horizon_type: {horizon_type}. "
            f"Must be one of {list(HORIZON_TYPE_TO_API.keys())}."
        )

    if not SAPPHIRE_API_AVAILABLE:
        logger.warning("sapphire-api-client not installed, skipping combined forecast API write")
        return False

    # Check if API writing is enabled (default: enabled)
    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
    if not api_enabled:
        logger.info("SAPPHIRE API writing disabled via SAPPHIRE_API_ENABLED=false")
        return False

    # Get API URL from environment
    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    logger.debug(
        "D8 _write_combined_forecast_to_api: SAPPHIRE_API_URL=%s, horizon=%s, rows=%d",
        api_url,
        horizon_type,
        len(data),
    )

    client = _get_postprocessing_client()

    # Health check - non-blocking, skip if API unavailable
    ready = client.readiness_check()
    logger.debug("D9 API readiness check: %s (url=%s)", ready, api_url)
    if not ready:
        logger.warning(f"SAPPHIRE API at {api_url} is not ready, skipping combined forecast write")
        return False

    data = data.copy()

    # Determine column names based on horizon_type
    if horizon_type == "pentad":
        horizon_value_col = "pentad_in_month"
        horizon_in_year_col = "pentad_in_year"
    elif horizon_type == "decad":
        horizon_value_col = "decad_in_month"
        horizon_in_year_col = "decad_in_year"
    else:
        raise ValueError(f"Invalid horizon_type: {horizon_type}. Must be 'pentad' or 'decad'.")

    # Compute missing horizon values from dates before iterating.
    # Virtual station outer merges and ML API reads can produce rows
    # with valid dates but NaN pentad_in_month/pentad_in_year.
    repaired_count = 0
    if horizon_type == "pentad":
        get_period_func = tl.get_pentad
        get_period_in_year_func = tl.get_pentad_in_year
    else:  # decad
        get_period_func = tl.get_decad_in_month
        get_period_in_year_func = tl.get_decad_in_year

    if horizon_value_col in data.columns:
        missing_hv = data[horizon_value_col].isna()
    else:
        missing_hv = pd.Series(True, index=data.index)
        data[horizon_value_col] = None

    if horizon_in_year_col in data.columns:
        missing_hiy = data[horizon_in_year_col].isna()
    else:
        missing_hiy = pd.Series(True, index=data.index)
        data[horizon_in_year_col] = None

    need_repair = missing_hv | missing_hiy
    if need_repair.any():
        dates_for_repair = pd.to_datetime(data.loc[need_repair, "date"], errors="coerce")
        valid_dates = dates_for_repair.notna()
        if valid_dates.any():
            repair_idx = dates_for_repair[valid_dates].index
            first_day = dates_for_repair[valid_dates] + pd.Timedelta(days=1)
            # tl functions return strings; convert to int for float64 columns
            data.loc[repair_idx, horizon_value_col] = pd.to_numeric(
                first_day.apply(get_period_func), errors="coerce"
            )
            data.loc[repair_idx, horizon_in_year_col] = pd.to_numeric(
                first_day.apply(get_period_in_year_func), errors="coerce"
            )
            repaired_count = len(repair_idx)
            logger.info(
                f"Computed missing horizon values from dates for "
                f"{repaired_count} forecast records ({horizon_type})"
            )

    # Prepare records for API (vectorized)
    df_rec = data.copy()
    # Drop rows with missing horizon values
    n_before = len(df_rec)
    df_rec = df_rec.dropna(subset=[horizon_value_col, horizon_in_year_col])
    skipped_count = n_before - len(df_rec)

    if skipped_count > 0:
        # Identify which codes/dates were dropped so operators can investigate
        dropped = data[data[horizon_value_col].isna() | data[horizon_in_year_col].isna()]
        dropped_detail = dropped[["code", "date"]].drop_duplicates().head(10).to_dict("records")
        logger.warning(
            "Dropped %d forecast records with missing horizon values "
            "after repair attempt (%s). Sample codes/dates: %s",
            skipped_count,
            horizon_type,
            dropped_detail,
        )

    if df_rec.empty:
        records = []
    else:
        # Map model_short to API model_type
        df_rec = df_rec.copy()
        df_rec["model_type"] = (
            df_rec["model_short"]
            .astype(str)
            .str.upper()
            .map(MODEL_TYPE_MAP)
            .fillna(df_rec["model_short"].astype(str))
        )
        df_rec["date_str"] = pd.to_datetime(df_rec["date"]).dt.strftime("%Y-%m-%d")

        # Ensure composition column exists
        if "composition" not in df_rec.columns:
            df_rec["composition"] = None
        # Warn about ensemble rows missing composition
        is_ensemble = df_rec["model_short"].astype(str).str.upper().isin(["EM", "NE"])
        missing_comp = is_ensemble & (
            df_rec["composition"].isna() | (df_rec["composition"].astype(str).str.strip() == "")
        )
        if missing_comp.any():
            logger.warning(
                "%d ensemble forecast rows have no composition column; "
                "these will be written with composition=None",
                missing_comp.sum(),
            )

        # Target = first day of forecast period (day after the boundary)
        df_rec["target_str"] = (pd.to_datetime(df_rec["date"]) + pd.Timedelta(days=1)).dt.strftime(
            "%Y-%m-%d"
        )

        records_df = pd.DataFrame(
            {
                "horizon_type": api_horizon_type,
                "code": df_rec["code"].astype(str),
                "model_type": df_rec["model_type"],
                "date": df_rec["date_str"],
                "target": df_rec["target_str"],
                "horizon_value": df_rec[horizon_value_col].astype(float).astype(int),
                "horizon_in_year": df_rec[horizon_in_year_col].astype(float).astype(int),
                "composition": df_rec["composition"],
                "forecasted_discharge": df_rec["forecasted_discharge"].where(
                    df_rec["forecasted_discharge"].notna()
                ),
            }
        )

        # Short-term Forecast schema supports q05, q25, q75, q95 only
        # (NOT q10, q50, q90 — those exist only in the LongForecast table)
        _SHORT_TERM_QUANTILE_COLS = ("q05", "q25", "q75", "q95")
        for qcol in _SHORT_TERM_QUANTILE_COLS:
            if qcol in df_rec.columns:
                records_df[qcol] = df_rec[qcol]

        # Drop rows where forecasted_discharge is NaN — writing null
        # discharge creates phantom rows that mask real gaps.
        n_before_null_filter = len(records_df)
        records_df = records_df.dropna(subset=["forecasted_discharge"])
        n_nulls = n_before_null_filter - len(records_df)
        if n_nulls > 0:
            logger.warning(
                "Dropped %d null-discharge forecast records before API write (%s)",
                n_nulls,
                horizon_type,
            )

        # Deduplicate on the unique constraint columns to prevent
        # CardinalityViolation ("cannot affect row a second time").
        unique_cols = ["horizon_type", "code", "model_type", "date", "target"]
        n_before_dedup = len(records_df)
        records_df = records_df.drop_duplicates(subset=unique_cols, keep="last")
        n_dupes = n_before_dedup - len(records_df)
        if n_dupes > 0:
            logger.warning(
                "Dropped %d duplicate forecast records on %s (%s) before API write",
                n_dupes,
                unique_cols,
                horizon_type,
            )

        # Convert to records, replacing NaN/NaT with None
        records = [
            {k: (None if pd.isna(v) else v) for k, v in row_dict.items()}
            for row_dict in records_df.to_dict("records")
        ]

    # Write to API
    if records:
        _check_write_codes({str(r["code"]) for r in records}, "combined_forecast")
        logger.debug(f"Sample record being sent to API ({horizon_type}): {records[0]}")
        try:
            count = client.write_forecasts(records)
        except Exception as e:
            # Log server response body for diagnosis
            response_body = getattr(e, "response", None)
            if response_body:
                logger.error(f"API response body for {horizon_type} write: {response_body[:1000]}")
            raise
        logger.info(
            f"Successfully wrote {count} combined forecast records to SAPPHIRE API ({horizon_type})"
        )
        print(
            f"SAPPHIRE API: Successfully wrote {count} combined forecast records ({horizon_type})"
        )
        return True
    else:
        logger.info(f"No combined forecast records to write to API ({horizon_type})")
        return False


def _write_skill_metrics_to_api(data: pd.DataFrame, horizon_type: str, year: int) -> bool:
    """
    Write skill metrics to SAPPHIRE postprocessing API.

    Args:
        data: DataFrame with skill metrics. Expected columns:
            - code: station code
            - pentad_in_year/decad_in_year/month_in_year: horizon in year
            - model_short: model identifier (LR, TFT, TIDE, TSMIXER, EM, NE)
            - sdivsigma: s/sigma metric
            - nse: Nash-Sutcliffe Efficiency
            - delta: delta metric
            - accuracy: accuracy metric
            - mae: Mean Absolute Error
            - n_pairs: number of data pairs
            - composition (optional): for ensemble models, which models compose it
        horizon_type: "pentad", "decad", or "month"
            (translated to API enum at boundary)
        year: The target year for skill metrics dates. Each row gets a
            date corresponding to the first day of its period (pentad,
            decad, or month) in this year.

    Returns:
        bool: True if successful, False otherwise
    """
    # Validate inputs before any I/O
    api_horizon_type = HORIZON_TYPE_TO_API.get(horizon_type)
    if api_horizon_type is None:
        raise ValueError(
            f"Invalid horizon_type: {horizon_type}. "
            f"Must be one of {list(HORIZON_TYPE_TO_API.keys())}."
        )

    if not SAPPHIRE_API_AVAILABLE:
        logger.warning("sapphire-api-client not installed, skipping skill metrics API write")
        return False

    # Check if API writing is enabled (default: enabled)
    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
    if not api_enabled:
        logger.info("SAPPHIRE API writing disabled via SAPPHIRE_API_ENABLED=false")
        return False

    # Get API URL from environment
    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    client = _get_postprocessing_client()

    # Health check - non-blocking, skip if API unavailable
    if not client.readiness_check():
        logger.warning(f"SAPPHIRE API at {api_url} is not ready, skipping skill metrics write")
        return False

    data = data.copy()

    # Determine column names based on horizon_type
    if horizon_type == "pentad":
        horizon_in_year_col = "pentad_in_year"
    elif horizon_type == "decad":
        horizon_in_year_col = "decad_in_year"
    elif horizon_type == "month":
        horizon_in_year_col = "month_in_year"
    elif horizon_type == "day":
        horizon_in_year_col = "day_in_year"
    elif horizon_type == "quarter":
        horizon_in_year_col = "quarter_in_year"
    elif horizon_type == "season":
        horizon_in_year_col = "season_in_year"
    else:
        raise ValueError(
            f"Invalid horizon_type: {horizon_type}. "
            "Must be 'pentad', 'decad', 'month', 'day', "
            "'quarter', or 'season'."
        )

    # Prepare records for API (vectorized)
    df_rec = data.copy()
    n_before = len(df_rec)
    df_rec = df_rec.dropna(subset=[horizon_in_year_col])
    skipped_count = n_before - len(df_rec)

    if skipped_count > 0:
        dropped = data[data[horizon_in_year_col].isna()]
        dropped_detail = (
            dropped[["code", "model_short"]].drop_duplicates().head(10).to_dict("records")
        )
        logger.warning(
            "Dropped %d skill metric records with missing %s. Sample codes/models: %s",
            skipped_count,
            horizon_in_year_col,
            dropped_detail,
        )

    if df_rec.empty:
        records = []
    else:
        # Map model_short to API model_type
        df_rec["model_type"] = (
            df_rec["model_short"]
            .astype(str)
            .str.upper()
            .map(MODEL_TYPE_MAP)
            .fillna(df_rec["model_short"].astype(str))
        )

        # Extract composition from existing column
        if "composition" in df_rec.columns:
            df_rec["_composition"] = df_rec["composition"].where(
                df_rec["composition"].notna()
                & (df_rec["composition"].astype(str).str.strip() != "")
            )
        else:
            df_rec["_composition"] = None
        # Warn about ensemble rows missing composition
        is_ensemble = df_rec["model_short"].astype(str).str.upper().isin(["EM", "NE"])
        missing_comp = is_ensemble & df_rec["_composition"].isna()
        if missing_comp.any():
            logger.warning(
                "%d ensemble skill metric rows have no composition; "
                "these will be written with composition=None",
                missing_comp.sum(),
            )

        # Compute per-row date from the period index and target year
        if horizon_type == "pentad":
            df_rec["_date"] = (
                df_rec[horizon_in_year_col]
                .astype(int)
                .apply(lambda p: tl.get_date_for_pentad(p, year))
            )
        elif horizon_type == "decad":
            df_rec["_date"] = (
                df_rec[horizon_in_year_col]
                .astype(int)
                .apply(lambda d: tl.get_date_for_decad(d, year))
            )
        elif horizon_type == "day":
            df_rec["_date"] = (
                df_rec[horizon_in_year_col]
                .astype(int)
                .apply(
                    lambda doy: (
                        dt_module.date(year, 1, 1) + dt_module.timedelta(days=doy - 1)
                    ).strftime("%Y-%m-%d")
                )
            )
        elif horizon_type == "month":
            df_rec["_date"] = (
                df_rec[horizon_in_year_col]
                .astype(int)
                .apply(lambda m: dt_module.date(year, m, 1).strftime("%Y-%m-%d"))
            )
        elif horizon_type == "quarter":
            # Quarter 1→Jan, 2→Apr, 3→Jul, 4→Oct
            df_rec["_date"] = (
                df_rec[horizon_in_year_col]
                .astype(int)
                .apply(lambda q: dt_module.date(year, (q - 1) * 3 + 1, 1).strftime("%Y-%m-%d"))
            )
        else:  # season
            from src.aggregation import get_season_months

            season_start = get_season_months()[0]
            df_rec["_date"] = dt_module.date(year, season_start, 1).strftime("%Y-%m-%d")

        # --- Normalize horizon_value ---
        # Month skill DataFrames (Phase 2a) carry the actual forecast lead in
        # horizon_value.  Other horizons have no such column.  Always emit a
        # concrete int (sentinel 0 for non-month) so the crud upsert tuple
        # never contains NULL — a NULL in a tuple_ IN comparison evaluates to
        # NULL (never TRUE), causing pile-up duplicate inserts for every recalc
        # run across ALL horizons (cross-horizon NULL-tuple hazard).
        if "horizon_value" in df_rec.columns:
            df_rec["horizon_value"] = df_rec["horizon_value"].fillna(0).astype(int)
        else:
            df_rec["horizon_value"] = 0

        # --- Deduplicate on DB upsert key ---
        # The DB unique constraint is (horizon_type, code, model_type, date,
        # horizon_in_year, horizon_value).  composition is NOT part of the
        # constraint.  Monthly/quarterly/seasonal ensemble baselines (EM,
        # Naive Mean, Skilled Mean) produce multiple rows per key with
        # different composition values due to the CRPS merge fan-out.  Retain
        # the row with a non-None composition (the true ensemble record).
        # horizon_value is included so that distinct month leads (0, 1, 2, 3)
        # are NOT collapsed — each lead is a distinct upsert key.
        upsert_key = ["code", "model_type", "_date", horizon_in_year_col, "horizon_value"]
        n_before = len(df_rec)
        df_rec = df_rec.sort_values("_composition", na_position="first")
        df_rec = df_rec.drop_duplicates(subset=upsert_key, keep="last")
        n_dupes = n_before - len(df_rec)
        if n_dupes > 0:
            logger.warning(
                "Dropped %d duplicate skill metric records before API write (%s)",
                n_dupes,
                horizon_type,
            )

        # Build nullable float columns
        metric_cols = {}
        for col in (
            "sdivsigma",
            "nse",
            "delta",
            "accuracy",
            "mae",
            "crps",
            "pbias",
            "kgelf",
            "nse_log",
            "fhv",
            "flv",
        ):
            if col in df_rec.columns:
                metric_cols[col] = df_rec[col].where(df_rec[col].notna())
            else:
                metric_cols[col] = None
        n_pairs_col = (
            df_rec["n_pairs"].where(df_rec["n_pairs"].notna())
            if "n_pairs" in df_rec.columns
            else None
        )

        records_df = pd.DataFrame(
            {
                "horizon_type": api_horizon_type,
                "code": df_rec["code"].astype(str),
                "model_type": df_rec["model_type"],
                "date": df_rec["_date"],
                "horizon_in_year": df_rec[horizon_in_year_col].astype(int),
                "horizon_value": df_rec["horizon_value"],
                "composition": df_rec["_composition"],
                **metric_cols,
            }
        )
        if n_pairs_col is not None:
            records_df["n_pairs"] = n_pairs_col
        # Convert to records, replacing NaN/NaT with None
        records = [
            {k: (None if pd.isna(v) else v) for k, v in row_dict.items()}
            for row_dict in records_df.to_dict("records")
        ]
        # Ensure n_pairs is int where not None
        for r in records:
            if r.get("n_pairs") is not None:
                r["n_pairs"] = int(r["n_pairs"])

    # Write to API
    if records:
        _check_write_codes({str(r["code"]) for r in records}, "skill_metrics")
        count = client.write_skill_metrics(records)
        logger.info(
            f"Successfully wrote {count} skill metric records to SAPPHIRE API ({horizon_type})"
        )
        print(f"SAPPHIRE API: Successfully wrote {count} skill metric records ({horizon_type})")
        return True
    else:
        logger.info(f"No skill metric records to write to API ({horizon_type})")
        return False


def _write_threshold_skill_metrics_to_api(data: pd.DataFrame, year: int) -> bool:
    """Write threshold-based skill metrics to SAPPHIRE API.

    Writes F1/CSI/precision/recall for flood and low-flow thresholds
    to the ThresholdSkillMetric endpoint.

    Args:
        data: DataFrame with columns: code, model_short,
            threshold_type, threshold_value, f1, precision, recall,
            csi, tp, fp, fn, tn, n_years.
        year: Target year for the skill metric date.

    Returns:
        True if successful, False otherwise (never raises).
    """
    if data is None or data.empty:
        logger.info("No threshold skill metrics to write to API")
        return False

    if not SAPPHIRE_API_AVAILABLE:
        logger.warning(
            "sapphire-api-client not installed, skipping threshold skill metrics API write"
        )
        return False

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
    if not api_enabled:
        logger.info("SAPPHIRE API writing disabled via SAPPHIRE_API_ENABLED=false")
        return False

    client = _get_postprocessing_client()
    if client is None:
        return False

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    if not client.readiness_check():
        logger.warning(
            "SAPPHIRE API at %s is not ready, skipping threshold skill metrics write",
            api_url,
        )
        return False

    try:
        df = data.copy()

        # Map model_short to API model_type
        df["model_type"] = (
            df["model_short"]
            .astype(str)
            .str.upper()
            .map(MODEL_TYPE_MAP)
            .fillna(df["model_short"].astype(str))
        )

        # Build date from year (use Jan 1 as reference date)
        date_str = dt_module.date(year, 1, 1).strftime("%Y-%m-%d")

        records = []
        for _, row in df.iterrows():
            record = {
                "code": str(row["code"]),
                "model_type": row["model_type"],
                "horizon_type": "day",
                "threshold_type": str(row["threshold_type"]),
                "threshold_value": (
                    float(row["threshold_value"]) if pd.notna(row.get("threshold_value")) else None
                ),
                "date": date_str,
                "n_years": (int(row["n_years"]) if pd.notna(row.get("n_years")) else None),
            }
            # Add contingency metrics (nullable)
            for col in ("f1", "precision", "recall", "csi"):
                val = row.get(col)
                record[col] = float(val) if pd.notna(val) else None
            # Rename 'precision' to avoid SQL keyword conflict
            if "precision" in record:
                record["precision_score"] = record.pop("precision")
            for col in ("tp", "fp", "fn", "tn"):
                val = row.get(col)
                record[col] = int(val) if pd.notna(val) else None
            records.append(record)

        if not records:
            logger.info("No threshold skill metric records to write")
            return False

        _check_write_codes({str(r["code"]) for r in records}, "threshold_skill_metrics")
        logger.debug("Sample threshold skill metric record: %s", records[0])
        # Use write_threshold_skill_metrics if client supports it;
        # gracefully no-op if endpoint doesn't exist yet (Stage 2).
        try:
            count = client.write_threshold_skill_metrics(records)
            logger.info(
                "Successfully wrote %d threshold skill metric records to API",
                count,
            )
            print(f"SAPPHIRE API: Successfully wrote {count} threshold skill metric records")
            return True
        except AttributeError:
            logger.info(
                "Postprocessing client does not support "
                "write_threshold_skill_metrics yet (Stage 2 pending)"
            )
            return False
        except Exception as e:
            # If the endpoint returns 404 (not deployed yet), log and
            # continue — this is expected before Stage 2 is deployed.
            err_str = str(e)
            if "404" in err_str or "Not Found" in err_str:
                logger.info(
                    "ThresholdSkillMetric endpoint not deployed yet (Stage 2 pending): %s",
                    err_str,
                )
                return False
            raise

    except Exception as e:
        logger.error("Failed to write threshold skill metrics to API: %s", e)
        return False


def _write_monthly_ensemble_to_api(data: pd.DataFrame) -> bool:
    """Write monthly ensemble forecasts (EM, Naive Mean, Skilled Mean)
    to the SAPPHIRE long_forecasts table.

    Filters data to ensemble rows, builds LongForecast-compatible records,
    and upserts via client.write_long_forecasts().

    Args:
        data: DataFrame with monthly joint forecasts. Expects columns:
            code, year, month, forecasted_discharge, model_short,
            and optionally q05-q95, valid_from, valid_to, composition.

    Returns:
        True if successful, False otherwise (never raises).
    """
    import calendar

    ensemble_models = {"EM", "Naive Mean", "Skilled Mean"}

    if data is None or data.empty:
        logger.info("No monthly ensemble data to write to API")
        return False

    if not SAPPHIRE_API_AVAILABLE:
        logger.warning("sapphire-api-client not installed, skipping monthly ensemble API write")
        return False

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
    if not api_enabled:
        logger.info("SAPPHIRE API writing disabled via SAPPHIRE_API_ENABLED=false")
        return False

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = _get_postprocessing_client()

        if not client.readiness_check():
            logger.warning(
                f"SAPPHIRE API at {api_url} is not ready, skipping monthly ensemble write"
            )
            return False

        # Filter to ensemble rows only
        ens_mask = data["model_short"].isin(ensemble_models)
        ens_data = data[ens_mask].copy()
        if ens_data.empty:
            logger.info("No ensemble rows in monthly forecast data")
            return False

        records = []
        for _, row in ens_data.iterrows():
            year = int(row["year"])
            month = int(row["month"])
            code = str(row["code"]).replace(".0", "")

            # Synthesize valid_from/valid_to from year+month if missing
            if pd.notna(row.get("valid_from")):
                valid_from = str(row["valid_from"])[:10]
            else:
                valid_from = f"{year}-{month:02d}-01"

            if pd.notna(row.get("valid_to")):
                valid_to = str(row["valid_to"])[:10]
            else:
                last_day = calendar.monthrange(year, month)[1]
                valid_to = f"{year}-{month:02d}-{last_day:02d}"

            # Map model_short to API model_type
            model_upper = str(row["model_short"]).upper()
            model_type = MODEL_TYPE_MAP.get(model_upper, str(row["model_short"]))

            if "horizon_value" in row.index and pd.notna(row.get("horizon_value")):
                horizon_value = int(row["horizon_value"])
            else:
                # The calendar month is NEVER a valid horizon_value; fall
                # back to the 0 sentinel (legacy/no-lead) instead.
                logger.warning(
                    "horizon_value missing for monthly ensemble row "
                    "(code=%s, year=%s, month=%s, model=%s); using 0 "
                    "sentinel instead of the calendar month",
                    code,
                    year,
                    month,
                    row["model_short"],
                )
                horizon_value = 0

            record = {
                "horizon_type": "month",
                "horizon_value": horizon_value,
                "code": code,
                "date": (
                    str(row["date"])[:10]
                    if "date" in row.index and pd.notna(row.get("date"))
                    else valid_from
                ),
                "model_type": model_type,
                "valid_from": valid_from,
                "valid_to": valid_to,
                "flag": 0,
                "q": (
                    float(row["forecasted_discharge"])
                    if pd.notna(row.get("forecasted_discharge"))
                    else None
                ),
            }

            # Add quantile columns if present and not NaN
            for qcol in ("q05", "q10", "q25", "q50", "q75", "q90", "q95"):
                if qcol in row.index and pd.notna(row.get(qcol)):
                    record[qcol] = float(row[qcol])

            # Add composition if present
            comp = row.get("composition", "")
            if pd.notna(comp) and str(comp).strip():
                record["composition"] = str(comp)

            records.append(record)

        if not records:
            logger.info("No monthly ensemble records to write to API")
            return False

        _check_write_codes({str(r["code"]) for r in records}, "monthly_ensemble")
        logger.debug("Sample monthly ensemble record: %s", records[0])
        count = client.write_long_forecasts(records)
        logger.info(
            "Successfully wrote %d monthly ensemble forecast records to SAPPHIRE API",
            count,
        )
        print(f"SAPPHIRE API: Successfully wrote {count} monthly ensemble forecast records")
        return True

    except Exception as e:
        logger.error(
            "Failed to write monthly ensemble forecasts to API: %s",
            str(e),
        )
        return False


def _write_quarterly_ensemble_to_api(data: pd.DataFrame) -> bool:
    """Write quarterly forecasts to the SAPPHIRE long_forecasts table.

    Writes both individual model aggregates and ensemble rows.

    Args:
        data: DataFrame with quarterly joint forecasts. Expects columns:
            code, year, quarter_in_year, forecasted_discharge, model_short,
            and optionally q05-q95, valid_from, valid_to, composition.

    Returns:
        True if successful, False otherwise (never raises).
    """
    return _write_aggregated_forecasts_to_api(
        data,
        horizon_type="quarter",
        period_col="quarter_in_year",
        label="quarterly",
    )


def _write_seasonal_ensemble_to_api(data: pd.DataFrame) -> bool:
    """Write seasonal forecasts to the SAPPHIRE long_forecasts table.

    Writes both individual model aggregates and ensemble rows.

    Args:
        data: DataFrame with seasonal joint forecasts. Expects columns:
            code, season_year, season_in_year, forecasted_discharge,
            model_short, and optionally q05-q95, valid_from, valid_to,
            composition.

    Returns:
        True if successful, False otherwise (never raises).
    """
    return _write_aggregated_forecasts_to_api(
        data,
        horizon_type="season",
        period_col="season_in_year",
        label="seasonal",
    )


def _write_aggregated_forecasts_to_api(
    data: pd.DataFrame,
    horizon_type: str,
    period_col: str,
    label: str,
) -> bool:
    """Shared implementation for writing quarterly/seasonal forecasts.

    Writes all model rows (individual models and ensembles) to the API.
    """
    import calendar

    from src.aggregation import QUARTER_MONTHS, get_season_months

    if data is None or data.empty:
        logger.info("No %s forecast data to write to API", label)
        return False

    if not SAPPHIRE_API_AVAILABLE:
        logger.warning(
            "sapphire-api-client not installed, skipping %s API write",
            label,
        )
        return False

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
    if not api_enabled:
        logger.info("SAPPHIRE API writing disabled via SAPPHIRE_API_ENABLED=false")
        return False

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = _get_postprocessing_client()

        if not client.readiness_check():
            logger.warning(
                "SAPPHIRE API at %s is not ready, skipping %s write",
                api_url,
                label,
            )
            return False

        # --- NaN guard: drop rows with missing year/period before int() conversion ---
        # Determine which columns to check for NaN before int() conversion
        if horizon_type == "quarter":
            nan_check_cols = [c for c in ["year", "quarter_in_year"] if c in data.columns]
        else:  # season
            nan_check_cols = [
                c
                for c in [
                    "season_year" if "season_year" in data.columns else "year",
                    period_col,
                ]
                if c in data.columns
            ]

        if nan_check_cols:
            data_before_nan_drop = data  # keep reference for diagnostics
            data = data.dropna(subset=nan_check_cols)
            skipped_nan = len(data_before_nan_drop) - len(data)
            if skipped_nan > 0:
                nan_mask = data_before_nan_drop[nan_check_cols].isna().any(axis=1)
                dropped_detail = (
                    data_before_nan_drop[nan_mask][["code"]]
                    .drop_duplicates()
                    .head(10)
                    .to_dict("records")
                )
                logger.warning(
                    "Dropped %d %s forecast records with missing year/period values. "
                    "Sample codes: %s",
                    skipped_nan,
                    label,
                    dropped_detail,
                )

        if data.empty:
            logger.info("No %s forecast records to write to API after NaN removal", label)
            return False

        records = []
        for _, row in data.iterrows():
            code = str(row["code"]).replace(".0", "")

            # Map model_short to API model_type
            model_upper = str(row["model_short"]).upper()
            model_type = MODEL_TYPE_MAP.get(model_upper, str(row["model_short"]))

            # Compute valid_from / valid_to / horizon_value
            if horizon_type == "quarter":
                year = int(row["year"])
                quarter = int(row["quarter_in_year"])
                start_month = QUARTER_MONTHS[quarter][0]
                end_month = QUARTER_MONTHS[quarter][-1]
                valid_from = f"{year}-{start_month:02d}-01"
                last_day = calendar.monthrange(year, end_month)[1]
                valid_to = f"{year}-{end_month:02d}-{last_day:02d}"
                # Under SAPPHIRE_SKILL_LEAD_AWARE, prefer the row's own
                # per-lead horizon_value (set by select_operational_issuances
                # / carried through per-lead aggregation) over the single
                # deployment-configured lead. Flag OFF: unchanged.
                if skill_lead_aware_enabled() and pd.notna(row.get("horizon_value")):
                    horizon_value = int(row["horizon_value"])
                else:
                    horizon_value = quarter_horizon_value()
            else:  # season
                season_year = int(row.get("season_year", row.get("year")))
                season_months = get_season_months()
                start_m = season_months[0]
                end_m = season_months[-1]
                # Cross-year: start in season_year, end may be next year
                if start_m <= end_m:
                    valid_from = f"{season_year}-{start_m:02d}-01"
                    last_day = calendar.monthrange(season_year, end_m)[1]
                    valid_to = f"{season_year}-{end_m:02d}-{last_day:02d}"
                else:
                    valid_from = f"{season_year}-{start_m:02d}-01"
                    end_year = season_year + 1
                    last_day = calendar.monthrange(end_year, end_m)[1]
                    valid_to = f"{end_year}-{end_m:02d}-{last_day:02d}"
                horizon_value = int(row[period_col])

            # Use existing valid_from/valid_to if present
            if pd.notna(row.get("valid_from")):
                valid_from = str(row["valid_from"])[:10]
            if pd.notna(row.get("valid_to")):
                valid_to = str(row["valid_to"])[:10]

            record_date = valid_from
            if (
                horizon_type == "season"
                or (horizon_type == "quarter" and skill_lead_aware_enabled())
            ) and pd.notna(row.get("date")):
                record_date = str(row["date"])[:10]

            record = {
                "horizon_type": horizon_type,
                "horizon_value": horizon_value,
                "code": code,
                "date": record_date,
                "model_type": model_type,
                "valid_from": valid_from,
                "valid_to": valid_to,
                "flag": 0,
                "q": (
                    float(row["forecasted_discharge"])
                    if pd.notna(row.get("forecasted_discharge"))
                    else None
                ),
            }

            for qcol in ("q05", "q10", "q25", "q50", "q75", "q90", "q95"):
                if qcol in row.index and pd.notna(row.get(qcol)):
                    record[qcol] = float(row[qcol])

            comp = row.get("composition", "")
            if pd.notna(comp) and str(comp).strip():
                record["composition"] = str(comp)

            records.append(record)

        if not records:
            logger.info("No %s forecast records to write to API", label)
            return False

        _check_write_codes({str(r["code"]) for r in records}, "aggregated_forecasts")
        logger.debug("Sample %s forecast record: %s", label, records[0])
        count = client.write_long_forecasts(records)
        logger.info(
            "Successfully wrote %d %s forecast records to SAPPHIRE API",
            count,
            label,
        )
        print(f"SAPPHIRE API: Successfully wrote {count} {label} forecast records")
        return True

    except Exception as e:
        logger.error(
            "Failed to write %s forecasts to API: %s",
            label,
            str(e),
        )
        return False
