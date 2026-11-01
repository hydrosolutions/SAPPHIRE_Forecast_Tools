"""Write postprocessing data (forecasts and skill metrics) to the SAPPHIRE API.

Extracted from forecast_library.py — these functions are exclusively
used by postprocessing_forecasts.

Follows the same singleton pattern as data_reader.py.
"""

import contextlib
import datetime as dt_module
import json
import logging
import os
from enum import Enum
from typing import Any

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


class WriteOutcome(Enum):
    """Outcome of an `api_writer.py` skill-metrics write attempt.

    A single bool cannot distinguish "nothing was attempted" from "the
    attempt failed" from "the attempt correctly found nothing to send" —
    conflating them makes a benign, documented configuration state (writing
    disabled via SAPPHIRE_API_ENABLED=false) indistinguishable from a
    genuine operational failure (a missing required dependency). Only
    `FAILED` should ever cause a caller to treat the write as a failure;
    every other member is a non-failure outcome.
    """

    WROTE = "wrote"  # records were sent and accepted
    SKIPPED_BY_CONFIG = "skipped_by_config"  # SAPPHIRE_API_ENABLED=false (operator opt-out)
    SKIPPED_NO_RECORDS = "skipped_no_records"  # nothing to write after filtering
    SKIPPED_NOT_DEPLOYED = "skipped_not_deployed"  # threshold endpoint not yet deployed (Stage 2)
    FAILED = "failed"  # readiness-check failure, or the write call raised


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
# Operator configuration
# ---------------------------------------------------------------------------
def api_writing_enabled() -> bool:
    """Report whether the operator has SAPPHIRE API writing enabled.

    Mirrors the inline `SAPPHIRE_API_ENABLED` parse each writer function
    performs internally (default: enabled). That inline parse is left
    exactly as-is everywhere it already exists -- it is working code
    covered by existing tests. This helper is purely additive, for
    callers that need the answer *before* a writer runs at all, e.g.
    file_writer.py's pre-gate default outcome, which must not report a
    closed SAPPHIRE_API_AVAILABLE gate as FAILED when the operator has
    disabled API writing (a closed gate is then just a benign skip, not
    a genuine failure).
    """
    return os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"


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


def _write_skill_metrics_to_api(data: pd.DataFrame, horizon_type: str, year: int) -> WriteOutcome:
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
        WriteOutcome: WROTE if records were sent and accepted;
            SKIPPED_BY_CONFIG if SAPPHIRE_API_ENABLED=false;
            SKIPPED_NO_RECORDS if nothing remained to write after
            filtering; FAILED if the client is absent, or the
            readiness check failed. See `api_writer.WriteOutcome`.
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
        return WriteOutcome.FAILED

    # Check if API writing is enabled (default: enabled)
    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
    if not api_enabled:
        logger.info("SAPPHIRE API writing disabled via SAPPHIRE_API_ENABLED=false")
        return WriteOutcome.SKIPPED_BY_CONFIG

    # Get API URL from environment
    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    client = _get_postprocessing_client()

    # Health check - non-blocking, skip if API unavailable
    if not client.readiness_check():
        logger.warning(f"SAPPHIRE API at {api_url} is not ready, skipping skill metrics write")
        return WriteOutcome.FAILED

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
            if skill_lead_aware_enabled():
                # Lead-aware: a NaN horizon_value cannot be keyed to a real
                # lead. Coercing it to 0 would masquerade as a genuine lead-0
                # row and, via the upsert-key dedup below, could collapse or
                # overwrite a true lead-0 record. Instead EXCLUDE such rows,
                # emitting a WARNING naming the count so they are surfaced
                # (not silently dropped): they need migration. Mirrors the
                # NULL-lead WARN-exclude idiom in gap_detector
                # detect_missing_quarterly_ensembles (FIX 5, revised).
                df_rec["horizon_value"] = pd.to_numeric(df_rec["horizon_value"], errors="coerce")
                null_lead_count = int(df_rec["horizon_value"].isna().sum())
                if null_lead_count:
                    logger.warning(
                        "%d NULL-lead %s skill metric rows skipped from "
                        "lead-aware API write -- need migration",
                        null_lead_count,
                        horizon_type,
                    )
                    df_rec = df_rec[df_rec["horizon_value"].notna()]
                df_rec["horizon_value"] = df_rec["horizon_value"].astype(int)
            else:
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
        return WriteOutcome.WROTE
    else:
        logger.info(f"No skill metric records to write to API ({horizon_type})")
        return WriteOutcome.SKIPPED_NO_RECORDS


def _write_threshold_skill_metrics_to_api(data: pd.DataFrame, year: int) -> WriteOutcome:
    """Write threshold-based skill metrics to SAPPHIRE API.

    Writes F1/CSI/precision/recall for flood and low-flow thresholds
    to the ThresholdSkillMetric endpoint.

    Args:
        data: DataFrame with columns: code, model_short,
            threshold_type, threshold_value, f1, precision, recall,
            csi, tp, fp, fn, tn, n_years.
        year: Target year for the skill metric date.

    Returns:
        WriteOutcome: WROTE if records were sent and accepted;
            SKIPPED_BY_CONFIG if SAPPHIRE_API_ENABLED=false;
            SKIPPED_NO_RECORDS if there was nothing to write;
            SKIPPED_NOT_DEPLOYED if the endpoint isn't deployed yet
            (Stage 2 pending); FAILED if the client is absent, the
            readiness check failed, or the write call raised any other
            exception. Note: client construction and the readiness check
            itself (above) can still raise uncaught -- only the region
            from here down is exception-protected. See
            `api_writer.WriteOutcome`.
    """
    if data is None or data.empty:
        logger.info("No threshold skill metrics to write to API")
        return WriteOutcome.SKIPPED_NO_RECORDS

    if not SAPPHIRE_API_AVAILABLE:
        logger.warning(
            "sapphire-api-client not installed, skipping threshold skill metrics API write"
        )
        return WriteOutcome.FAILED

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
    if not api_enabled:
        logger.info("SAPPHIRE API writing disabled via SAPPHIRE_API_ENABLED=false")
        return WriteOutcome.SKIPPED_BY_CONFIG

    client = _get_postprocessing_client()
    if client is None:
        return WriteOutcome.SKIPPED_BY_CONFIG

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    if not client.readiness_check():
        logger.warning(
            "SAPPHIRE API at %s is not ready, skipping threshold skill metrics write",
            api_url,
        )
        return WriteOutcome.FAILED

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
            return WriteOutcome.SKIPPED_NO_RECORDS

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
            return WriteOutcome.WROTE
        except AttributeError:
            logger.info(
                "Postprocessing client does not support "
                "write_threshold_skill_metrics yet (Stage 2 pending)"
            )
            return WriteOutcome.SKIPPED_NOT_DEPLOYED
        except Exception as e:
            # If the endpoint returns 404 (not deployed yet), log and
            # continue — this is expected before Stage 2 is deployed.
            err_str = str(e)
            if "404" in err_str or "Not Found" in err_str:
                logger.info(
                    "ThresholdSkillMetric endpoint not deployed yet (Stage 2 pending): %s",
                    err_str,
                )
                return WriteOutcome.SKIPPED_NOT_DEPLOYED
            raise

    except Exception as e:
        logger.error("Failed to write threshold skill metrics to API: %s", e)
        return WriteOutcome.FAILED


# ---------------------------------------------------------------------------
# P3 recovery serializer — selection only, no transport
# ---------------------------------------------------------------------------

# Canonical plan model keys → wire API enum values (exact case from service schema).
# Only short-term ML/ensemble models are accepted; LR, LT and DAY are excluded.
_RECOVERY_WIRE_MODEL: dict[str, str] = {
    "TFT": "TFT",
    "TIDE": "TiDE",
    "TSMIXER": "TSMixer",
    "NE": "NE",
    "ENSEMBLE_MEAN": "EM",
}

# Internal plan horizon keys → wire API enum values.
_RECOVERY_WIRE_HORIZON: dict[str, str] = {
    "pentad": "pentad",
    "decad": "decade",
}

# The 14 writable fields that every output record must carry (exactly).
_RECOVERY_OUTPUT_FIELDS: tuple[str, ...] = (
    "horizon_type",
    "code",
    "model_type",
    "date",
    "target",
    "forecasted_discharge",
    "q05",
    "q25",
    "q75",
    "q95",
    "flag",
    "composition",
    "horizon_value",
    "horizon_in_year",
)

# Fields in a plan payload that are response-only and must be silently dropped.
_RECOVERY_RESPONSE_ONLY_FIELDS: frozenset[str] = frozenset({"id", "model_type_description"})


def _recovery_wire_serialize(payload: dict) -> dict:
    """Serialize one plan payload to a 14-field wire record.

    Returns a dict with exactly the 14 output fields, dates as ISO strings,
    model_type and horizon_type converted to wire enum values.

    Uses gap_detector._plan_key for canonical identity extraction (accepts
    TiDE/TSMixer/EM/ENSEMBLE_MEAN model aliases and ISO-8601 date strings in
    addition to date objects and midnight-naive datetime/Timestamp).  Uses
    gap_detector._plan_all5_finite and _plan_valid_meta_fields for ALL14
    presence, calendar, and metadata validation so this function stays in
    sync with the planner's own contracts without duplicating them.

    Raises ValueError for any validation failure.
    """
    import datetime as _dt

    try:
        from src.gap_detector import _plan_all5_finite as _gd_all5
        from src.gap_detector import _plan_key as _gd_plan_key
        from src.gap_detector import _plan_valid_meta_fields as _gd_meta
    except ImportError:
        from gap_detector import _plan_all5_finite as _gd_all5  # type: ignore[no-redef]
        from gap_detector import _plan_key as _gd_plan_key  # type: ignore[no-redef]
        from gap_detector import _plan_valid_meta_fields as _gd_meta  # type: ignore[no-redef]

    # Detect unknown extra writable fields (anything not in the 14 output
    # fields and not a known response-only field).
    _writable_fields = frozenset(
        {
            "horizon_type",
            "code",
            "model_type",
            "date",
            "target",
            "forecasted_discharge",
            "q05",
            "q25",
            "q75",
            "q95",
            "flag",
            "composition",
            "horizon_value",
            "horizon_in_year",
        }
    )
    for k in payload:
        if k not in _writable_fields and k not in _RECOVERY_RESPONSE_ONLY_FIELDS:
            raise ValueError(f"select_recovery_records: unknown extra field {k!r} in payload")

    # Use _plan_key for canonical identity extraction.  Accepts TiDE/TSMixer/EM
    # aliases plus ISO-8601 date strings and midnight datetime/Timestamp objects.
    # Returns None for any unsupported model/horizon, missing field, or malformed date.
    key = _gd_plan_key(payload)
    if key is None:
        raise ValueError(
            "select_recovery_records: payload has unparseable key — "
            "unsupported model_type or horizon_type, malformed/non-date date or target, "
            "or surrounding whitespace in code"
        )

    ht, code, canonical_model, issue_date, target_date = key

    wire_ht = _RECOVERY_WIRE_HORIZON.get(ht)
    if wire_ht is None:
        raise ValueError(
            f"select_recovery_records: unsupported horizon_type {ht!r}; "
            f"must be one of {list(_RECOVERY_WIRE_HORIZON)}"
        )

    wire_model = _RECOVERY_WIRE_MODEL.get(canonical_model)
    if wire_model is None:
        raise ValueError(
            f"select_recovery_records: unsupported model_type {canonical_model!r}; "
            f"must be one of {list(_RECOVERY_WIRE_MODEL)}"
        )

    # ALL5 quantities: must be present, finite, non-bool real numerics.
    if not _gd_all5(payload):
        raise ValueError(
            "select_recovery_records: ALL5 quantities (forecasted_discharge, q05, "
            "q25, q75, q95) must be present finite real numerics — None and bool "
            "are not accepted"
        )

    # flag: None or non-bool int (checked explicitly for a clear error message).
    flag = payload.get("flag")
    if flag is not None and (isinstance(flag, bool) or not isinstance(flag, int)):
        raise ValueError(
            f"select_recovery_records: flag must be None or int, "
            f"got {type(flag).__name__}: {flag!r}"
        )

    # composition: None or str (checked explicitly for a clear error message).
    composition = payload.get("composition")
    if composition is not None and (
        isinstance(composition, bool) or not isinstance(composition, str)
    ):
        raise ValueError(
            f"select_recovery_records: composition must be None or str, "
            f"got {type(composition).__name__}: {composition!r}"
        )

    # Full metadata validation: presence of all 14 writable fields, calendar boundary,
    # target == issue + 1 day, horizon_value/horizon_in_year match calendar position.
    if not _gd_meta(payload, key):
        if target_date != issue_date + _dt.timedelta(days=1):
            raise ValueError(
                f"select_recovery_records: target {target_date} must equal "
                f"issue {issue_date} + 1 day"
            )
        raise ValueError(
            "select_recovery_records: metadata validation failed — check that all "
            "14 writable fields are present and that horizon_value/horizon_in_year "
            "match the calendar position of the issue date"
        )

    return {
        "horizon_type": wire_ht,
        "code": code,
        "model_type": wire_model,
        "date": issue_date.isoformat(),
        "target": target_date.isoformat(),
        "forecasted_discharge": payload["forecasted_discharge"],
        "q05": payload["q05"],
        "q25": payload["q25"],
        "q75": payload["q75"],
        "q95": payload["q95"],
        "flag": flag,
        "composition": composition,
        "horizon_value": payload["horizon_value"],
        "horizon_in_year": payload["horizon_in_year"],
    }


def select_recovery_records(
    candidate_payloads: list[dict],
    plan: "Any",
    authorized_keys: set[tuple],
    *,
    start_date: "Any",
    end_date: "Any",
    forecast_date: "Any",
) -> list[dict]:
    """Select and serialize recovery records from a PeriodRefreshPlan.

    Pure function: no API calls, no writes, no side effects.

    Selects only plan.missing keys (looked up from candidate_payloads by
    canonical key) and plan.authorized_refresh payloads (already-merged by the
    planner). Aborts on plan.plan_error; never selects identical/protected/
    excluded/blocked keys; rejects keys outside authorized_keys; rejects any
    candidate whose key lands in more than one disposition set; rejects
    conflicting duplicate candidates (same 5-part key, differing values);
    collapses exact full-writable duplicates to one.

    Restricts to short-term ML/ensemble models (TFT/TIDE/TSMIXER/NE/ENSEMBLE_MEAN)
    and pentad/decad horizons only. Issue dates must be <= forecast_date and
    within [start_date, end_date].

    Returns a list of exactly-14-field serialized dicts with wire enum values
    (horizon_type decad→decade, model_type TIDE→TiDE/TSMIXER→TSMixer/
    ENSEMBLE_MEAN→EM) and ISO date strings. Returns [] when nothing is eligible.

    Args:
        candidate_payloads: Input-derived candidates (output of _build_candidate_payload).
        plan: A PeriodRefreshPlan from plan_period_refresh.
        authorized_keys: The same authorized_keys passed to plan_period_refresh.
        start_date: Inclusive lower bound for issue date.
        end_date: Inclusive upper bound for issue date.
        forecast_date: Captured run date; issue date must be <= this.

    Returns:
        list[dict]: Serialized wire records (each has exactly 14 fields).

    Raises:
        ValueError: On plan_error, unauthorized key, protected/identical/excluded/
            blocked selected key, missing candidate, conflicting duplicates,
            malformed fields, unsupported model/horizon, or unknown extra fields.
    """
    try:
        from src.gap_detector import _plan_all5_finite as _gd_all5
        from src.gap_detector import _plan_key
        from src.gap_detector import _plan_records_identical as _gd_identical
        from src.gap_detector import _plan_valid_meta_fields as _gd_meta
    except ImportError:
        from gap_detector import _plan_all5_finite as _gd_all5  # type: ignore[no-redef]
        from gap_detector import _plan_key  # type: ignore[no-redef]
        from gap_detector import _plan_records_identical as _gd_identical  # type: ignore[no-redef]
        from gap_detector import _plan_valid_meta_fields as _gd_meta  # type: ignore[no-redef]

    # Abort immediately on plan-level error.
    if plan.plan_error is not None:
        raise ValueError(
            f"select_recovery_records: plan has plan_error — aborting: {plan.plan_error}"
        )

    # Validate date bounds.
    if start_date > end_date:
        raise ValueError(f"select_recovery_records: start_date {start_date} > end_date {end_date}")
    if end_date > forecast_date:
        raise ValueError(
            f"select_recovery_records: end_date {end_date} > forecast_date {forecast_date}"
        )

    # Build a deduplicated index of candidate_payloads by canonical key.
    # Each candidate is validated (ALL5 + metadata) BEFORE dedup comparison so
    # that bool/int variants that compare equal numerically are still detected as
    # invalid before they can collapse into an apparently-valid duplicate.
    # Exact full-writable duplicates (per _plan_records_identical) collapse to one;
    # conflicting duplicates raise.
    candidate_by_key: dict[tuple, dict] = {}
    candidate_conflicted: set[tuple] = set()

    for p in candidate_payloads:
        key = _plan_key(p)
        if key is None:
            raise ValueError(
                "select_recovery_records: candidate_payloads contains a record "
                "with an unparseable key"
            )
        # Validate each candidate individually BEFORE dedup comparison.
        if not _gd_all5(p):
            raise ValueError(
                f"select_recovery_records: candidate for key {key!r} has invalid "
                "ALL5 fields (None, bool, or non-finite value)"
            )
        if not _gd_meta(p, key):
            raise ValueError(
                f"select_recovery_records: candidate for key {key!r} failed "
                "metadata validation (missing field, calendar boundary, or type error)"
            )
        # Full serialization of every candidate before dedup comparison to
        # reject unknown extra fields and flag/composition type errors
        # even if the candidate would later resolve as a duplicate.
        _recovery_wire_serialize(p)
        if key in candidate_conflicted:
            continue  # already known to be conflicted
        if key in candidate_by_key:
            if not _gd_identical(candidate_by_key[key], p):
                candidate_conflicted.add(key)
                del candidate_by_key[key]
                raise ValueError(
                    f"select_recovery_records: conflicting duplicate candidates for key {key!r}"
                )
            # Exact duplicate: keep the first, continue.
        else:
            candidate_by_key[key] = p

    result_records: list[dict] = []
    seen_keys: set[tuple] = set()

    # 1. Records for plan.missing: must look up candidate by key.
    for key in plan.missing:
        # Must be in authorized_keys.
        if key not in authorized_keys:
            raise ValueError(
                f"select_recovery_records: plan.missing key {key!r} is not in authorized_keys"
            )
        # Must not be in any blocking disposition set.
        if key in plan.identical:
            raise ValueError(
                f"select_recovery_records: plan.missing key {key!r} also in plan.identical"
            )
        if key in plan.protected:
            raise ValueError(
                f"select_recovery_records: plan.missing key {key!r} also in plan.protected"
            )
        if key in plan.excluded:
            raise ValueError(
                f"select_recovery_records: plan.missing key {key!r} also in plan.excluded"
            )
        if key in plan.blocked:
            raise ValueError(
                f"select_recovery_records: plan.missing key {key!r} also in plan.blocked"
            )
        # Must have a candidate.
        payload = candidate_by_key.get(key)
        if payload is None:
            raise ValueError(
                f"select_recovery_records: no candidate found for plan.missing key {key!r}"
            )
        # Issue date must be within [start_date, end_date].
        issue_date = key[3]  # (ht, code, model, issue_date, target_date)
        if not (start_date <= issue_date <= end_date):
            raise ValueError(
                f"select_recovery_records: issue date {issue_date} for key {key!r} "
                f"is outside [{start_date}, {end_date}]"
            )
        wire = _recovery_wire_serialize(payload)
        if key not in seen_keys:
            result_records.append(wire)
            seen_keys.add(key)

    # 2. Records from plan.authorized_refresh (already-merged payloads).
    # Track refresh-phase keys separately to detect repeated-refresh-key conflicts.
    refresh_seen: dict[tuple, dict] = {}

    for key, merged_payload in plan.authorized_refresh:
        # Must be in authorized_keys.
        if key not in authorized_keys:
            raise ValueError(
                f"select_recovery_records: authorized_refresh key {key!r} is not in authorized_keys"
            )
        # Must not be in blocking disposition sets.
        if key in plan.identical:
            raise ValueError(
                f"select_recovery_records: authorized_refresh key {key!r} also in plan.identical"
            )
        if key in plan.protected:
            raise ValueError(
                f"select_recovery_records: authorized_refresh key {key!r} also in plan.protected"
            )
        if key in plan.excluded:
            raise ValueError(
                f"select_recovery_records: authorized_refresh key {key!r} also in plan.excluded"
            )
        if key in plan.blocked:
            raise ValueError(
                f"select_recovery_records: authorized_refresh key {key!r} also in plan.blocked"
            )
        # Issue date must be within [start_date, end_date].
        issue_date = key[3]
        if not (start_date <= issue_date <= end_date):
            raise ValueError(
                f"select_recovery_records: issue date {issue_date} for authorized_refresh "
                f"key {key!r} is outside [{start_date}, {end_date}]"
            )
        # Refresh payload's canonical key must equal the associated plan key.
        payload_key = _plan_key(merged_payload)
        if payload_key != key:
            raise ValueError(
                f"select_recovery_records: authorized_refresh payload canonical key "
                f"{payload_key!r} does not match associated plan key {key!r}"
            )
        # Full serialization before dedup check to reject unknown extra fields
        # and flag/composition type errors even if payload is later a duplicate.
        wire = _recovery_wire_serialize(merged_payload)
        # Repeated refresh key: identical payload → collapse; differing payload → raise.
        if key in refresh_seen:
            if not _gd_identical(refresh_seen[key], merged_payload):
                raise ValueError(
                    f"select_recovery_records: authorized_refresh has repeated key "
                    f"{key!r} with differing payloads"
                )
            continue  # exact duplicate, skip
        # key already processed in plan.missing → must not silently drop.
        if key in seen_keys:
            raise ValueError(
                f"select_recovery_records: key {key!r} appears in both plan.missing "
                "and plan.authorized_refresh — these sets must be disjoint"
            )
        result_records.append(wire)
        seen_keys.add(key)
        refresh_seen[key] = merged_payload

    return result_records


def submit_recovery_batch(
    records: list[dict],
    *,
    authorized_keys: set[tuple],
    start_date: "Any",
    end_date: "Any",
    forecast_date: "Any",
    base_url: str,
    batch_limit: int,
    write_cutoff: float,
    deadline: float,
    evidence_path: str,
    timeout: int,
    _clock=None,
) -> dict:
    """Submit exactly one bounded batch of serialized recovery records.

    Pure transport seam: accepts only fully-validated serialized wire records
    (output of select_recovery_records). Creates an isolated client with
    max_retries=1 and no connection to the ordinary singleton. Writes evidence
    before any POST. Never resends on timeout/failure; returns transport_status
    and attempted flag separately.

    Returns a plain dict with keys:
      - attempted: bool — True if POST was entered (evidence exists)
      - transport_status: str — "acknowledged", "uncertain", or "not_attempted"
      - acknowledged_count: int or None
      - attempted_records: list[dict] — exact records submitted (or [])
      - evidence_path: str — path to evidence file (or None)
      - error: str or None — human-readable reason for non-success

    Args:
        records: Pre-validated serialized wire records (14 fields each).
        authorized_keys: Set of canonical 5-tuples; every record's key must be
            present. Re-validated here at the transport boundary.
        start_date: Inclusive issue-date lower bound (re-validated).
        end_date: Inclusive issue-date upper bound (re-validated).
        forecast_date: Run date; issue date must be <= this (re-validated).
        base_url: API base URL for the isolated client.
        batch_limit: Maximum number of records; reject (not split) if exceeded.
        write_cutoff: Monotonic timestamp; stop before POST if past.
        deadline: Monotonic timestamp; must be > write_cutoff.
        evidence_path: Caller-provided path for the evidence file (O_CREAT|O_EXCL).
        timeout: Request timeout in seconds for the isolated client (positive int).
        _clock: Monotonic clock callable (default: time.monotonic; injectable for tests).
    """
    import datetime as _dt
    import errno
    import json as _json
    import os
    import stat
    import time
    import uuid

    if _clock is None:
        _clock = time.monotonic

    try:
        from src.gap_detector import _plan_key as _gd_plan_key
    except ImportError:
        from gap_detector import _plan_key as _gd_plan_key  # type: ignore[no-redef]

    # --- Parameter validation ---
    if not isinstance(batch_limit, int) or isinstance(batch_limit, bool) or batch_limit <= 0:
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": f"batch_limit must be a positive int, got {batch_limit!r}",
        }
    if not isinstance(timeout, int) or isinstance(timeout, bool) or timeout <= 0:
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": f"timeout must be a positive int, got {timeout!r}",
        }
    if not isinstance(write_cutoff, (int, float)) or not _isfinite_real(write_cutoff):
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": "write_cutoff must be a finite real number",
        }
    if not isinstance(deadline, (int, float)) or not _isfinite_real(deadline):
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": "deadline must be a finite real number",
        }
    if write_cutoff >= deadline:
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": f"write_cutoff {write_cutoff} must be < deadline {deadline}",
        }
    if not isinstance(evidence_path, str) or not evidence_path:
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": "evidence_path must be a non-empty string",
        }

    # --- Type checks: records must be list[dict] ---
    if not isinstance(records, list) or not all(isinstance(r, dict) for r in records):
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": "records must be a list of dicts",
        }

    # --- Date type checks: bounds must be date not datetime, ordered ---
    for _bn, _bv in (
        ("start_date", start_date),
        ("end_date", end_date),
        ("forecast_date", forecast_date),
    ):
        if not isinstance(_bv, _dt.date) or isinstance(_bv, _dt.datetime):
            return {
                "attempted": False,
                "transport_status": "not_attempted",
                "acknowledged_count": None,
                "attempted_records": [],
                "evidence_path": None,
                "error": f"{_bn} must be a date object, not datetime",
            }
    if start_date > end_date or end_date > forecast_date:
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": "scope bounds must satisfy start_date <= end_date <= forecast_date",
        }

    # --- URL validation: http/https only, hostname required, no credentials/query/fragment ---
    from urllib.parse import urlsplit as _urlsplit

    try:
        _url_parts = _urlsplit(base_url)
        _url_hostname = _url_parts.hostname
        _url_scheme = _url_parts.scheme
    except ValueError:
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": "base_url could not be parsed as a URL",
        }
    if _url_scheme not in ("http", "https") or not _url_hostname:
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": "base_url must use http or https with a valid hostname",
        }
    if _url_parts.username or _url_parts.password:
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": "base_url must not contain credentials",
        }
    if _url_parts.query or _url_parts.fragment:
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": "base_url must not contain query string or fragment",
        }

    # --- Empty batch: no-op ---
    if not records:
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": 0,
            "attempted_records": [],
            "evidence_path": None,
            "error": None,
        }

    # --- Oversize batch: reject ---
    if len(records) > batch_limit:
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": (
                f"batch size {len(records)} exceeds batch_limit {batch_limit}; "
                "reject rather than auto-split"
            ),
        }

    # --- API-disabled / unavailable: zero client/network calls ---
    if not SAPPHIRE_API_AVAILABLE:
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": "sapphire-api-client not installed",
        }
    if not api_writing_enabled():
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": "SAPPHIRE_API_ENABLED=false",
        }

    # --- Re-validate all wire payloads at transport boundary ---
    try:
        _validated = [_recovery_wire_serialize(r) for r in records]
    except ValueError as e:
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": f"wire validation failed: {e}",
        }

    # --- Re-validate authorized_keys and date bounds ---
    for r in _validated:
        key = _gd_plan_key(r)
        if key is None:
            return {
                "attempted": False,
                "transport_status": "not_attempted",
                "acknowledged_count": None,
                "attempted_records": [],
                "evidence_path": None,
                "error": f"record has unparseable key after serialization: {r!r}",
            }
        if key not in authorized_keys:
            return {
                "attempted": False,
                "transport_status": "not_attempted",
                "acknowledged_count": None,
                "attempted_records": [],
                "evidence_path": None,
                "error": f"key {key!r} not in authorized_keys",
            }
        issue_date = key[3]
        if not (start_date <= issue_date <= end_date):
            return {
                "attempted": False,
                "transport_status": "not_attempted",
                "acknowledged_count": None,
                "attempted_records": [],
                "evidence_path": None,
                "error": (f"issue date {issue_date} outside [{start_date}, {end_date}]"),
            }
        if issue_date > forecast_date:
            return {
                "attempted": False,
                "transport_status": "not_attempted",
                "acknowledged_count": None,
                "attempted_records": [],
                "evidence_path": None,
                "error": f"issue date {issue_date} > forecast_date {forecast_date}",
            }

    # --- Strict duplicate check on canonical key ---
    seen_keys: set[tuple] = set()
    for r in _validated:
        key = _gd_plan_key(r)
        if key in seen_keys:
            return {
                "attempted": False,
                "transport_status": "not_attempted",
                "acknowledged_count": None,
                "attempted_records": [],
                "evidence_path": None,
                "error": f"duplicate canonical key in batch: {key!r}",
            }
        seen_keys.add(key)

    # --- Budget check: must have enough remaining time for timeout ---
    now = _clock()
    if now >= write_cutoff:
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": f"at or past write_cutoff ({write_cutoff}); clock={now}",
        }
    remaining = write_cutoff - now
    if remaining <= timeout:
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": (
                f"insufficient remaining time for request timeout: "
                f"{remaining:.2f}s remaining, timeout={timeout}s"
            ),
        }

    # --- Evidence file: O_CREAT|O_EXCL, mode 0600, private external dir ---
    # Check lexists on the ORIGINAL path before realpath: realpath resolves a
    # dangling symlink to its (non-existent) target, so lexists on the resolved
    # path would return False and silently miss the dangling symlink.
    if os.path.lexists(evidence_path):
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": f"evidence_path already exists or is symlink: {evidence_path!r}",
        }
    evidence_path = os.path.realpath(evidence_path)
    ev_parent = os.path.dirname(evidence_path)
    if not ev_parent:
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": "evidence_path must be under a non-empty parent directory",
        }
    if not os.path.isdir(ev_parent):
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": f"evidence parent directory does not exist: {ev_parent!r}",
        }
    _ev_parent_mode = os.stat(ev_parent).st_mode
    if _ev_parent_mode & 0o077:
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": f"evidence parent directory has group/other permission bits set: {ev_parent!r}",
        }

    # Reject evidence parent that is itself a symlink.
    if os.path.islink(ev_parent):
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": None,
            "error": f"evidence parent dir is a symlink: {ev_parent!r}",
        }
    # Reject if path is inside a Git checkout (presence of .git in ancestors)
    _check_dir = ev_parent
    while True:
        if os.path.exists(os.path.join(_check_dir, ".git")):
            return {
                "attempted": False,
                "transport_status": "not_attempted",
                "acknowledged_count": None,
                "attempted_records": [],
                "evidence_path": None,
                "error": (f"evidence_path is inside a Git checkout (found .git at {_check_dir!r})"),
            }
        parent = os.path.dirname(_check_dir)
        if parent == _check_dir:
            break
        _check_dir = parent

    attempt_id = str(uuid.uuid4())
    # Non-secret destination identity: base_url only (no auth/token)
    evidence_payload = {
        "attempt_id": attempt_id,
        "destination": base_url,
        "records": _validated,
        "canonical_keys": [list(k) for k in sorted(seen_keys, key=str)],
        "batch_limit": batch_limit,
        "n_records": len(_validated),
    }
    evidence_bytes = _json.dumps(evidence_payload, default=str).encode("utf-8")

    ev_created = False
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        fd = os.open(evidence_path, flags, stat.S_IRUSR | stat.S_IWUSR)
        try:
            ev_created = True
            offset = 0
            total = len(evidence_bytes)
            while offset < total:
                written = os.write(fd, evidence_bytes[offset:])
                if written <= 0:
                    raise OSError(errno.EIO, f"os.write returned {written}")
                offset += written
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError as e:
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": evidence_path if ev_created else None,
            "error": f"evidence file creation failed: {e}",
        }

    # --- Recheck cutoff after evidence write ---
    now = _clock()
    if now >= write_cutoff:
        return {
            "attempted": False,
            "transport_status": "not_attempted",
            "acknowledged_count": None,
            "attempted_records": [],
            "evidence_path": evidence_path,
            "error": f"past write_cutoff after evidence write; clock={now}",
        }

    # --- Isolated client: new instance, max_retries=1, session.max_redirects=0 ---
    transport_error = None
    transport_status = "uncertain"
    acknowledged_count = None
    client = None
    attempted = False

    try:
        client = SapphirePostprocessingClient(
            base_url=base_url,
            auth_token=None,
            max_retries=1,
            batch_size=batch_limit,
            timeout=timeout,
        )
        client.session.max_redirects = 0

        # Attach a response hook to validate shape before count
        _hook_error: list[str] = []

        def _validate_response_shape(response, *args, **kwargs):
            if not (200 <= response.status_code < 300):
                _hook_error.append(
                    f"non-2xx response status {response.status_code}; "
                    "cannot confirm acknowledgement"
                )
                return
            try:
                body = response.json()
                if not isinstance(body, list):
                    _hook_error.append(
                        f"malformed response: expected list, got {type(body).__name__}"
                    )
                elif not all(isinstance(item, dict) for item in body):
                    _hook_error.append("malformed response: list items are not all dicts")
            except Exception as parse_err:
                _hook_error.append(f"response JSON parse error: {parse_err}")

        client.session.hooks["response"].append(_validate_response_shape)

        # Recheck cutoff immediately before POST (client setup may consume budget)
        now = _clock()
        remaining = write_cutoff - now
        if now >= write_cutoff or remaining <= timeout:
            return {
                "attempted": False,
                "transport_status": "not_attempted",
                "acknowledged_count": None,
                "attempted_records": [],
                "evidence_path": evidence_path,
                "error": (
                    f"insufficient budget before POST; cutoff={write_cutoff}, "
                    f"clock={now}, remaining={remaining:.2f}, timeout={timeout}"
                ),
            }

        attempted = True
        count = client.write_short_term_forecasts(_validated)

        # Check hook-detected shape errors
        if _hook_error:
            transport_status = "uncertain"
            transport_error = _hook_error[0]
        elif not isinstance(count, int) or isinstance(count, bool):
            transport_status = "uncertain"
            transport_error = f"non-int/bool count returned: {count!r}"
        elif count != len(_validated):
            transport_status = "uncertain"
            transport_error = f"count mismatch: submitted {len(_validated)}, response count {count}"
        else:
            # Check clock: late completion is not timely success
            now = _clock()
            if now >= write_cutoff or now >= deadline:
                transport_status = "uncertain"
                transport_error = (
                    f"POST completed late (cutoff={write_cutoff}, deadline={deadline}); clock={now}"
                )
            else:
                transport_status = "acknowledged"
                acknowledged_count = count

    except Exception as exc:
        if not attempted:
            transport_status = "not_attempted"
        transport_error = str(exc)
    finally:
        if client is not None:
            with contextlib.suppress(Exception):
                client.session.close()

    return {
        "attempted": attempted,
        "transport_status": transport_status,
        "acknowledged_count": acknowledged_count,
        "attempted_records": _validated if attempted else [],
        "evidence_path": evidence_path,
        "error": transport_error,
    }


def reconcile_recovery_batches(
    attempted_batches: list[dict],
    *,
    authorized_keys: set[tuple],
    start_date: "Any",
    end_date: "Any",
    forecast_date: "Any",
    base_url: str,
    deadline: float,
    timeout: int,
    page_size: int = 500,
    max_pages: int = 200,
    _clock=None,
) -> dict:
    """Reconcile attempted recovery batches by strict full readback from the API.

    Accepts the caller-owned list of all prior submit_recovery_batch results,
    including acknowledged and uncertain ones. Reads back the actual state for
    every key that was attempted (attempted=True) and compares all 14 writable
    fields exactly. Does NOT modify transport statuses or retry anything.

    Returns a plain dict with keys:
      - verified_keys: list[tuple] — keys confirmed fully written (all 14 match)
      - unverified_keys: list[tuple] — keys not confirmed (missing, mismatch, conflict,
        or read failure; uncertain transport alone does not put a key here if readback verifies it)
      - reasons: dict[tuple, str] — per-key human-readable reason for unverified
      - reconciliation_status: str — "verified", "unverified", or "no_attempted_records"
      - original_transport_uncertain: list[tuple] — keys whose original transport was
        "uncertain" (retained separately even when readback verifies the key)

    Args:
        attempted_batches: All prior submit_recovery_batch return dicts (caller-owned).
        authorized_keys: Canonical 5-tuples that were authorized for this recovery run.
        start_date: Inclusive issue-date lower bound.
        end_date: Inclusive issue-date upper bound.
        forecast_date: Captured run date.
        base_url: API base URL for the isolated read client.
        deadline: Absolute monotonic deadline for all reads (finite positive).
        timeout: Per-request timeout in seconds.
        page_size: Records per API page (positive int).
        max_pages: Hard per-code page cap (positive int).
        _clock: Injectable monotonic clock (default: time.monotonic).
    """
    import datetime as _dt
    import time

    if _clock is None:
        _clock = time.monotonic

    try:
        from src.data_reader import (
            RECOVERY_STATUS_COMPLETE,
            RECOVERY_STATUS_EMPTY,
        )
        from src.data_reader import (
            read_combined_period_forecasts_for_recovery as _rb_read,
        )
    except ImportError:
        from data_reader import (
            RECOVERY_STATUS_COMPLETE,
            RECOVERY_STATUS_EMPTY,
        )
        from data_reader import (  # type: ignore[no-redef]
            read_combined_period_forecasts_for_recovery as _rb_read,
        )

    try:
        from src.gap_detector import _plan_key as _gd_plan_key
    except ImportError:
        from gap_detector import _plan_key as _gd_plan_key  # type: ignore[no-redef]

    import math as _math

    # --- Validate scalar parameters ---
    if (
        not isinstance(deadline, (int, float))
        or isinstance(deadline, bool)
        or not _math.isfinite(deadline)
    ):
        return {
            "verified_keys": [],
            "unverified_keys": [],
            "reasons": {},
            "reconciliation_status": "unverified",
            "original_transport_uncertain": [],
            "error": "deadline must be a finite real number",
        }
    if not isinstance(timeout, int) or isinstance(timeout, bool) or timeout <= 0:
        return {
            "verified_keys": [],
            "unverified_keys": [],
            "reasons": {},
            "reconciliation_status": "unverified",
            "original_transport_uncertain": [],
            "error": f"timeout must be a positive int, got {timeout!r}",
        }
    if not isinstance(page_size, int) or isinstance(page_size, bool) or page_size <= 0:
        return {
            "verified_keys": [],
            "unverified_keys": [],
            "reasons": {},
            "reconciliation_status": "unverified",
            "original_transport_uncertain": [],
            "error": f"page_size must be a positive int, got {page_size!r}",
        }
    if not isinstance(max_pages, int) or isinstance(max_pages, bool) or max_pages <= 0:
        return {
            "verified_keys": [],
            "unverified_keys": [],
            "reasons": {},
            "reconciliation_status": "unverified",
            "original_transport_uncertain": [],
            "error": f"max_pages must be a positive int, got {max_pages!r}",
        }
    for _bn, _bv in (
        ("start_date", start_date),
        ("end_date", end_date),
        ("forecast_date", forecast_date),
    ):
        if not isinstance(_bv, _dt.date) or isinstance(_bv, _dt.datetime):
            return {
                "verified_keys": [],
                "unverified_keys": [],
                "reasons": {},
                "reconciliation_status": "unverified",
                "original_transport_uncertain": [],
                "error": f"{_bn} must be a date object, not datetime",
            }
    if start_date > end_date or end_date > forecast_date:
        return {
            "verified_keys": [],
            "unverified_keys": [],
            "reasons": {},
            "reconciliation_status": "unverified",
            "original_transport_uncertain": [],
            "error": "scope bounds must satisfy start_date <= end_date <= forecast_date",
        }

    # --- Validate base_url (same rules as submit_recovery_batch) ---
    from urllib.parse import urlsplit as _urlsplit_rb

    try:
        _rb_url = _urlsplit_rb(base_url)
    except ValueError:
        return {
            "verified_keys": [],
            "unverified_keys": [],
            "reasons": {},
            "reconciliation_status": "unverified",
            "original_transport_uncertain": [],
            "error": "base_url could not be parsed as a URL",
        }
    if _rb_url.scheme not in ("http", "https") or not _rb_url.hostname:
        return {
            "verified_keys": [],
            "unverified_keys": [],
            "reasons": {},
            "reconciliation_status": "unverified",
            "original_transport_uncertain": [],
            "error": "base_url must use http or https with a valid hostname",
        }
    if _rb_url.username or _rb_url.password:
        return {
            "verified_keys": [],
            "unverified_keys": [],
            "reasons": {},
            "reconciliation_status": "unverified",
            "original_transport_uncertain": [],
            "error": "base_url must not contain credentials",
        }
    if _rb_url.query or _rb_url.fragment:
        return {
            "verified_keys": [],
            "unverified_keys": [],
            "reasons": {},
            "reconciliation_status": "unverified",
            "original_transport_uncertain": [],
            "error": "base_url must not contain query string or fragment",
        }

    # --- Validate attempted_batches structure and collect attempted records ---
    # Malformed batch/record/key → structured unverified return, never continue+skip.
    # False-attempted batches are ignored only when well-formed.
    if not isinstance(attempted_batches, list):
        return {
            "verified_keys": [],
            "unverified_keys": [],
            "reasons": {},
            "reconciliation_status": "unverified",
            "original_transport_uncertain": [],
            "error": "attempted_batches must be a list of dicts",
        }
    key_to_records: dict[tuple, list[dict]] = {}
    uncertain_transport_keys: set[tuple] = set()

    for batch in attempted_batches:
        if not isinstance(batch, dict):
            return {
                "verified_keys": [],
                "unverified_keys": [],
                "reasons": {},
                "reconciliation_status": "unverified",
                "original_transport_uncertain": [],
                "error": "attempted_batches contains a non-dict element",
            }
        raw_attempted = batch.get("attempted")
        if not isinstance(raw_attempted, bool):
            return {
                "verified_keys": [],
                "unverified_keys": [],
                "reasons": {},
                "reconciliation_status": "unverified",
                "original_transport_uncertain": [],
                "error": (
                    f"batch 'attempted' field must be bool, "
                    f"got {type(raw_attempted).__name__}: {raw_attempted!r}"
                ),
            }
        _VALID_STATUSES = ("acknowledged", "uncertain", "not_attempted")
        raw_status = batch.get("transport_status")
        if raw_status not in _VALID_STATUSES:
            return {
                "verified_keys": [],
                "unverified_keys": [],
                "reasons": {},
                "reconciliation_status": "unverified",
                "original_transport_uncertain": [],
                "error": (f"batch transport_status {raw_status!r} is not one of {_VALID_STATUSES}"),
            }
        recs_field = batch.get("attempted_records")
        if not raw_attempted:
            # attempted=False is only consistent with not_attempted + empty records.
            if raw_status != "not_attempted":
                return {
                    "verified_keys": [],
                    "unverified_keys": [],
                    "reasons": {},
                    "reconciliation_status": "unverified",
                    "original_transport_uncertain": [],
                    "error": (
                        f"batch attempted=False but transport_status={raw_status!r}; "
                        "expected 'not_attempted'"
                    ),
                }
            if not (isinstance(recs_field, list) and len(recs_field) == 0):
                return {
                    "verified_keys": [],
                    "unverified_keys": [],
                    "reasons": {},
                    "reconciliation_status": "unverified",
                    "original_transport_uncertain": [],
                    "error": (
                        "batch attempted=False but attempted_records is non-empty; "
                        "contradictory batch structure"
                    ),
                }
            continue
        # attempted=True: status must be acknowledged or uncertain.
        if raw_status not in ("acknowledged", "uncertain"):
            return {
                "verified_keys": [],
                "unverified_keys": [],
                "reasons": {},
                "reconciliation_status": "unverified",
                "original_transport_uncertain": [],
                "error": (
                    f"batch attempted=True but transport_status={raw_status!r}; "
                    "expected 'acknowledged' or 'uncertain'"
                ),
            }
        recs = recs_field
        if not isinstance(recs, list) or not recs or not all(isinstance(r, dict) for r in recs):
            return {
                "verified_keys": [],
                "unverified_keys": [],
                "reasons": {},
                "reconciliation_status": "unverified",
                "original_transport_uncertain": [],
                "error": "attempted=True batch must have a non-empty attempted_records list of dicts",
            }
        for rec in recs:
            # Full pre-flight: validates all 14 fields, types, calendar, extras.
            # Store the canonical serialized record (not the original) so that
            # alias/date-format variants (TiDE vs TIDE, date object vs ISO) compare
            # on identical full-14-field canonical values during conflict detection.
            try:
                canonical_rec = _recovery_wire_serialize(rec)
            except ValueError as _ve:
                return {
                    "verified_keys": [],
                    "unverified_keys": [],
                    "reasons": {},
                    "reconciliation_status": "unverified",
                    "original_transport_uncertain": [],
                    "error": f"attempted record failed validation: {_ve}",
                }
            key = _gd_plan_key(rec)
            if key is None:
                return {
                    "verified_keys": [],
                    "unverified_keys": [],
                    "reasons": {},
                    "reconciliation_status": "unverified",
                    "original_transport_uncertain": [],
                    "error": "attempted_batches contains a record with an unparseable key",
                }
            if key not in authorized_keys:
                return {
                    "verified_keys": [],
                    "unverified_keys": [key],
                    "reasons": {key: "attempted key not in authorized_keys"},
                    "reconciliation_status": "unverified",
                    "original_transport_uncertain": [],
                    "error": f"attempted batch contains key not in authorized_keys: {key!r}",
                }
            issue_date = key[3]
            if not (start_date <= issue_date <= end_date):
                return {
                    "verified_keys": [],
                    "unverified_keys": [key],
                    "reasons": {key: f"issue date {issue_date} outside [{start_date}, {end_date}]"},
                    "reconciliation_status": "unverified",
                    "original_transport_uncertain": [],
                    "error": (
                        f"attempted record issue date {issue_date} "
                        f"outside [{start_date}, {end_date}]"
                    ),
                }
            if issue_date > forecast_date:
                return {
                    "verified_keys": [],
                    "unverified_keys": [key],
                    "reasons": {key: f"issue date {issue_date} > forecast_date {forecast_date}"},
                    "reconciliation_status": "unverified",
                    "original_transport_uncertain": [],
                    "error": (
                        f"attempted record issue date {issue_date} > forecast_date {forecast_date}"
                    ),
                }
            if key not in key_to_records:
                key_to_records[key] = []
            key_to_records[key].append(canonical_rec)
        if batch.get("transport_status") == "uncertain":
            for rec in recs:
                key = _gd_plan_key(rec)
                if key is not None:
                    uncertain_transport_keys.add(key)

    # No attempted records across all batches → no-op, no reads
    if not key_to_records:
        return {
            "verified_keys": [],
            "unverified_keys": [],
            "reasons": {},
            "reconciliation_status": "no_attempted_records",
            "original_transport_uncertain": [],
            "error": None,
        }

    # Check API availability/enabled before constructing any client
    if not SAPPHIRE_API_AVAILABLE or not api_writing_enabled():
        all_keys = list(key_to_records.keys())
        return {
            "verified_keys": [],
            "unverified_keys": all_keys,
            "reasons": {k: "API unavailable or disabled; cannot read back" for k in all_keys},
            "reconciliation_status": "unverified",
            "original_transport_uncertain": [k for k in all_keys if k in uncertain_transport_keys],
            "error": None,
        }

    # Check deadline before constructing client
    now = _clock()
    if now >= deadline:
        all_keys = list(key_to_records.keys())
        return {
            "verified_keys": [],
            "unverified_keys": all_keys,
            "reasons": {k: "deadline exhausted before any reads" for k in all_keys},
            "reconciliation_status": "unverified",
            "original_transport_uncertain": [k for k in all_keys if k in uncertain_transport_keys],
            "error": None,
        }

    # --- Detect conflicting attempted records for the same key ---
    # Two records from different batches with the same key but differing values
    # → key is unverifiable (conflict)
    conflicted_keys: dict[tuple, str] = {}
    canonical_attempted: dict[tuple, dict] = {}

    for key, recs in key_to_records.items():
        if not recs:
            conflicted_keys[key] = "no records for key"
            continue
        first = recs[0]
        for other in recs[1:]:
            # Compare all 14 writable fields
            mismatch = None
            for field in _RECOVERY_OUTPUT_FIELDS:
                v1 = first.get(field)
                v2 = other.get(field)
                if v1 != v2:
                    mismatch = (
                        f"conflicting attempted records for key (field {field!r}: {v1!r} vs {v2!r})"
                    )
                    break
            if mismatch:
                conflicted_keys[key] = mismatch
                break
        if key not in conflicted_keys:
            canonical_attempted[key] = first

    # --- Group keys by (horizon_type, model_canonical) for scoped reads ---
    # Read scope per (ht, model): only attempted codes, min/max of attempted issue
    # dates (not the entire authorized window).
    from collections import defaultdict

    scope_keys: dict[tuple, set[str]] = defaultdict(set)  # (ht_internal, model_canonical) -> codes
    scope_date_range: dict[tuple, tuple] = {}  # (ht_internal, model_canonical) -> (min, max)
    for key in canonical_attempted:
        ht, code, model_canonical, issue_date, target_date = key
        scope_keys[(ht, model_canonical)].add(code)
        scope = (ht, model_canonical)
        if scope not in scope_date_range:
            scope_date_range[scope] = (issue_date, issue_date)
        else:
            lo, hi = scope_date_range[scope]
            scope_date_range[scope] = (min(lo, issue_date), max(hi, issue_date))

    verified_keys: list[tuple] = []
    unverified_keys: list[tuple] = []
    reasons: dict[tuple, str] = {}

    # Mark all conflicted keys as unverified immediately
    for key, reason in conflicted_keys.items():
        unverified_keys.append(key)
        reasons[key] = reason

    client = None
    try:
        # --- Build the isolated read client (inside guard so constructor exceptions are structured) ---
        client = SapphirePostprocessingClient(
            base_url=base_url,
            auth_token=None,
            max_retries=1,
            batch_size=page_size,
            timeout=timeout,
        )
        client.session.max_redirects = 0
        # Perform one read per (horizon_type, model_canonical) scope
        for (ht_internal, model_canonical), codes in scope_keys.items():
            # Check deadline before each read scope
            now = _clock()
            if now >= deadline:
                # All remaining keys in this scope → unverified
                for key in canonical_attempted:
                    if key[0] == ht_internal and key[2] == model_canonical and key not in reasons:
                        unverified_keys.append(key)
                        reasons[key] = "deadline exhausted before read"
                continue

            remaining_deadline = deadline - _clock()
            if remaining_deadline <= 0:
                for key in canonical_attempted:
                    if key[0] == ht_internal and key[2] == model_canonical and key not in reasons:
                        unverified_keys.append(key)
                        reasons[key] = "deadline exhausted before read"
                continue

            # Wire model name for the reader (reader accepts canonical internal names)
            # _RECOVERY_WIRE_MODEL maps canonical → wire; reader accepts wire model names
            wire_model = _RECOVERY_WIRE_MODEL.get(model_canonical)
            if wire_model is None:
                for key in canonical_attempted:
                    if key[0] == ht_internal and key[2] == model_canonical and key not in reasons:
                        unverified_keys.append(key)
                        reasons[key] = (
                            f"unsupported canonical model {model_canonical!r} for readback"
                        )
                continue

            scope_read_start, scope_read_end = scope_date_range.get(
                (ht_internal, model_canonical), (start_date, end_date)
            )
            status, frame, reason_str = _rb_read(
                client,
                list(codes),
                wire_model,
                ht_internal,
                scope_read_start,
                scope_read_end,
                forecast_date,
                total_deadline=deadline,
                page_size=page_size,
                max_pages=max_pages,
                _clock=_clock,
            )

            # Check clock after read
            now = _clock()

            if status not in (RECOVERY_STATUS_COMPLETE, RECOVERY_STATUS_EMPTY):
                # Failed/disabled read → all keys in this scope are unverified
                for key in canonical_attempted:
                    if key[0] == ht_internal and key[2] == model_canonical and key not in reasons:
                        unverified_keys.append(key)
                        reasons[key] = f"readback failed (status={status!r}): {reason_str}"
                continue

            # Build a lookup from (code, issue_date) → readback row for this scope
            readback_by_key: dict[tuple, dict] = {}
            readback_conflicts: set[tuple] = set()

            if frame is not None and not frame.empty:
                for _, rb_row in frame.iterrows():
                    rb_dict = dict(rb_row)
                    rb_key = _gd_plan_key(rb_dict)
                    if rb_key is None:
                        continue
                    if rb_key in readback_by_key:
                        # Duplicate in readback → conflict
                        readback_conflicts.add(rb_key)
                    else:
                        readback_by_key[rb_key] = rb_dict

            # Verify each key in this scope
            for key in list(canonical_attempted.keys()):
                if key[0] != ht_internal or key[2] != model_canonical:
                    continue
                if key in reasons:
                    continue

                if key in readback_conflicts:
                    unverified_keys.append(key)
                    reasons[key] = "duplicate conflicting rows in readback"
                    continue

                rb = readback_by_key.get(key)
                if rb is None:
                    # Key not found in readback — if read was CONFIRMED_EMPTY, key is missing
                    unverified_keys.append(key)
                    reasons[key] = "key not found in readback"
                    continue

                # Canonicalize both sides through the serializer so that bool/int
                # variants, absent-vs-None, model aliases, and date string formats
                # are normalized identically. ValueError from either side → unverified.
                expected = canonical_attempted[key]
                mismatch = None
                try:
                    exp_wire = _recovery_wire_serialize(expected)
                except ValueError as _ve:
                    unverified_keys.append(key)
                    reasons[key] = f"expected record failed re-serialization: {_ve}"
                    continue
                try:
                    rb_wire = _recovery_wire_serialize(rb)
                except ValueError as _ve:
                    unverified_keys.append(key)
                    reasons[key] = f"readback record failed serialization: {_ve}"
                    continue
                for field in _RECOVERY_OUTPUT_FIELDS:
                    exp_v = exp_wire.get(field)
                    rb_v = rb_wire.get(field)
                    if exp_v != rb_v:
                        mismatch = f"field {field!r} mismatch: expected {exp_v!r}, got {rb_v!r}"
                        break

                if mismatch:
                    unverified_keys.append(key)
                    reasons[key] = mismatch
                elif now >= deadline:
                    # Readback completed but deadline passed — do not certify as verified
                    unverified_keys.append(key)
                    reasons[key] = f"readback completed late (deadline={deadline}, clock={now})"
                else:
                    verified_keys.append(key)

    except Exception as _exc:
        # Only classify keys that have not yet been placed in either partition.
        # verified_keys have no reasons entry; without this guard they would be
        # moved to unverified on a later-scope exception, making the partitions
        # non-disjoint.
        _verified_set = set(verified_keys)
        for key in canonical_attempted:
            if key not in reasons and key not in _verified_set:
                unverified_keys.append(key)
                reasons[key] = f"client or read exception: {_exc}"
    finally:
        if client is not None:
            with contextlib.suppress(Exception):
                client.session.close()

    overall_status = "verified" if (verified_keys and not unverified_keys) else "unverified"
    if not verified_keys and not unverified_keys:
        overall_status = "no_attempted_records"

    return {
        "verified_keys": verified_keys,
        "unverified_keys": unverified_keys,
        "reasons": reasons,
        "reconciliation_status": overall_status,
        "original_transport_uncertain": [
            k for k in (verified_keys + unverified_keys) if k in uncertain_transport_keys
        ],
        "error": None,
    }


def _isfinite_real(x: object) -> bool:
    """Return True if x is a finite real number (not bool, not complex)."""
    import math

    if isinstance(x, bool):
        return False
    if isinstance(x, (int, float)):
        return math.isfinite(x)
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

    from src.aggregation import QUARTER_MONTHS, get_season_months, local_calendar_date

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
        dropped_calendar_rows = 0
        skipped_aggregated_rows = 0
        for _, row in data.iterrows():
            code = str(row["code"]).replace(".0", "")

            # Map model_short to API model_type
            model_upper = str(row["model_short"]).upper()

            # PP-065 item 3 (rev-3 PP-064 "B6"): quarter stops writing raw
            # LR_Base/LR_SM rows and EM/ENSEMBLE_MEAN rows. Fallback-derived
            # LR rows are native-shaped and would pass the native-row rule
            # forever once persisted; the stored EM row is re-dated on every
            # re-write under flag OFF, which would mint a new EM key each
            # time since date is part of the unique key. Season is
            # unaffected -- this branch only ever executes for quarter.
            if horizon_type == "quarter" and model_upper in {
                "LR_BASE",
                "LR_SM",
                "EM",
                "ENSEMBLE_MEAN",
            }:
                skipped_aggregated_rows += 1
                continue

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

            # Calendar-window guard (PP-064 Chunk A, quarter branch only):
            # a row whose own valid_from/valid_to disagree with the
            # synthesized calendar window for (year, quarter_in_year) --
            # a rolling window, a calendar valid_from paired with a null
            # valid_to, or a present-but-unparseable value -- is dropped
            # rather than written with the synthesized window. Both null
            # keeps today's synthesized-window behavior. Parsed via
            # local_calendar_date (not a str(...)[:10] prefix compare,
            # which accepted a garbage time-of-day like
            # "2024-06-30T99:00:00" and rejected a same-date value in a
            # different format like "2024/06/30" or "20240630" that the
            # reader's own calendar-window check already accepts).
            # Season is unaffected.
            if horizon_type == "quarter":
                # S2/T3: match the reader's conservative cutoffs at BOTH
                # ends -- filter_calendar_quarter_windows (aggregation.py)
                # treats ANY row with valid_from.year > 2261 as not a
                # calendar quarter regardless of month (upper,
                # datetime64[ns] tops out at ~2262-04-11), and
                # local_calendar_date itself rejects anything before
                # 1677-09-22 (lower, just past pd.Timestamp.min). Without
                # matching both here, the reader could reject a
                # synthesized window (e.g. year 1677 Q3, "1677-07-01") the
                # writer would have written -- out-of-range data is
                # nonsense; the only point is agreement between the two.
                out_of_range_target_year = year > 2261
                # U1: compare NUMERICALLY, not as unpadded strings --
                # "999-01-01" < "1677-09-22" is False lexicographically
                # (year 999 is misclassified as in-range, along with
                # every other year with fewer digits than 1677: 2-9,
                # 17-99, 170-999). (year, quarter start month) <
                # (1677, 10) is equivalent to year < 1677, or year ==
                # 1677 with a start month before October (quarters 1-3,
                # whose windows start Jan/Apr/Jul -- all before the
                # 1677-09-22 cutoff; only Q4's Oct 1 start clears it).
                out_of_range_low = year < 1677 or (year == 1677 and quarter < 4)
                out_of_range = out_of_range_target_year or out_of_range_low
                row_has_valid_from = pd.notna(row.get("valid_from"))
                row_has_valid_to = pd.notna(row.get("valid_to"))
                both_present = row_has_valid_from and row_has_valid_to
                if out_of_range:
                    mismatch = True
                elif both_present:
                    parsed_valid_from = local_calendar_date(pd.Series([row["valid_from"]])).iloc[0]
                    parsed_valid_to = local_calendar_date(pd.Series([row["valid_to"]])).iloc[0]
                    mismatch = (
                        pd.isna(parsed_valid_from)
                        or pd.isna(parsed_valid_to)
                        or parsed_valid_from != pd.Timestamp(valid_from)
                        or parsed_valid_to != pd.Timestamp(valid_to)
                    )
                else:
                    mismatch = True
                skip_row = out_of_range or ((row_has_valid_from or row_has_valid_to) and mismatch)
                if skip_row:
                    dropped_calendar_rows += 1
                    continue

            # Use existing valid_from/valid_to if present. Quarter rows
            # that reach this point already had their own values
            # verified equal to the synthesized calendar window above,
            # so they keep writing the synthesized ISO dates (not the
            # row's own, possibly differently-formatted, string) --
            # season is unaffected, unchanged from before.
            if horizon_type != "quarter":
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

        if dropped_calendar_rows:
            # WARNING, not INFO: unlike the reader filters (which drop
            # rolling windows on every run by design), a drop here means
            # an upstream invariant broke -- the readers already filter
            # non-calendar windows out, so this row should never have
            # reached the writer with one. INFO also never reaches the
            # logs from production entry points (setup_library caps the
            # root logger at WARNING on import -- INFRA-029).
            logger.warning(
                "Dropped %d %s forecast record(s) with a non-calendar quarter window",
                dropped_calendar_rows,
                label,
            )

        if skipped_aggregated_rows:
            # WARNING, matching the dropped_calendar_rows pattern directly
            # above: INFO never reaches the logs from production entry
            # points (setup_library caps the root logger at WARNING on
            # import -- INFRA-029), and this is a routine, per-call skip
            # count operators should be able to see (PP-065 item 3).
            logger.warning(
                "Skipped %d %s forecast record(s) for LR_BASE/LR_SM/EM "
                "(quarter no longer writes raw LR or EM rows)",
                skipped_aggregated_rows,
                label,
            )

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
