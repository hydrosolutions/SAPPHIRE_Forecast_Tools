# ----------------------------------------------------------------
# FILE: recalculate_nan_forecasts.py
# ----------------------------------------------------------------
#
# Description: This script checks if there are any nan values in the forecasts and then recalculates them.
# Operational NaN placeholders use flag == 1; recalc selects candidates with flag in [1, 2].
# This script checks if there are nan values in the forecasts and then recalculates them, by calling the hindcast script.
# The hindcast will return a file which is already flagged:
# - flag == 3 for nan values even after the hindcast
# - flag == 4 for valid values after the hindcast
# ----------------------------------------------------------------
# USAGE:
# SAPPHIRE_OPDEV_ENV=True SAPPHIRE_MODEL_TO_USE=TFT SAPPHIRE_PREDICTION_MODE=PENTAD python recalculate_nan_forecasts.py
# ----------------------------------------------------------------
import datetime
import json
import logging
import os
import subprocess
import sys
import time
import uuid
from logging.handlers import TimedRotatingFileHandler

import numpy as np
import pandas as pd

logging.getLogger("pytorch_lightning.utilities.rank_zero").setLevel(logging.WARNING)
logging.getLogger("pytorch_lightning.accelerators.cuda").setLevel(logging.WARNING)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
# Ensure the logs directory exists
logs_dir = "logs"
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)
file_handler = TimedRotatingFileHandler("logs/log", when="midnight", interval=1, backupCount=30)
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger = logging.getLogger("recalculate_nan_forecasts")
logger.setLevel(logging.DEBUG)
logger.handlers = []
logger.addHandler(file_handler)

import warnings

warnings.filterwarnings("ignore")

# SAPPHIRE API imports
from scr.utils_ml_forecast import (
    ML_MODEL_TYPE_MAP,
    SAPPHIRE_API_AVAILABLE,
    RecoveryReadError,
    SapphirePostprocessingClient,
    _read_ml_forecasts_from_api,
    _write_ml_forecast_to_api,
    canonical_ml_mode,
    get_permitted_station_codes,
    normalize_ml_csv_columns,
    read_unit_for_recovery_preview,
)

# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, "..", "iEasyHydroForecast")

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the setup_library module from the iEasyHydroForecast package
import setup_library as sl


def call_hindcast_script(
    min_missing_date: str,
    max_missing_date: str,
    MODEL_TO_USE: str,
    intermediate_data_path: str,
    codes_with_nan: list,
    PREDICTION_MODE: str,
) -> pd.DataFrame:
    # --------------------------------------------------------------------
    # CALL THE HINDCAST SCRIPT
    # --------------------------------------------------------------------
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    # Ensure the environment variable is set
    env = os.environ.copy()
    env["SAPPHIRE_MODEL_TO_USE"] = MODEL_TO_USE
    env["ieasyhydroforecast_START_DATE"] = min_missing_date
    env["ieasyhydroforecast_END_DATE"] = max_missing_date
    env["SAPPHIRE_HINDCAST_MODE"] = PREDICTION_MODE
    codes_hindcast = ",".join([str(code) for code in codes_with_nan])
    env["ieasyhydroforecast_NEW_STATIONS"] = codes_hindcast

    # Prepare the command
    command = [sys.executable, "hindcast_ML_models.py"]
    logger.info("Running hindcast command: %s", command)

    # Call the script with timeout guard
    _timeout_raw = os.getenv("SAPPHIRE_HINDCAST_TIMEOUT_SECONDS", "").strip()
    hindcast_timeout = int(_timeout_raw) if _timeout_raw else 14400
    logger.info("Hindcast timeout: %d seconds", hindcast_timeout)
    env["PYTHONUNBUFFERED"] = "1"
    try:
        result = subprocess.run(
            command,
            env=env,
            timeout=hindcast_timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"Hindcast subprocess timed out after {hindcast_timeout}s "
            f"for {MODEL_TO_USE} {PREDICTION_MODE}"
        ) from exc

    # Check if the script ran successfully
    if result.returncode == 0:
        logger.info("Hindcast ran successfully")
    else:
        logger.error(
            "Hindcast failed with return code %s",
            result.returncode,
        )
        raise RuntimeError(
            f"Hindcast subprocess failed with return code "
            f"{result.returncode} for {MODEL_TO_USE} {PREDICTION_MODE}"
        )

    # --------------------------------------------------------------------
    # GET THE HINDCAST
    # --------------------------------------------------------------------
    # Path to the output directory
    OUTPUT_PATH_DISCHARGE = os.getenv("ieasyhydroforecast_OUTPUT_PATH_DISCHARGE")

    PATH_FORECAST = os.path.join(intermediate_data_path, OUTPUT_PATH_DISCHARGE)

    PATH_HINDCAST = os.path.join(PATH_FORECAST, "hindcast", MODEL_TO_USE)

    file_name = f"{MODEL_TO_USE}_{canonical_ml_mode(MODEL_TO_USE, PREDICTION_MODE)}_hindcast_daily_{min_missing_date}_{max_missing_date}.csv"

    hindcast = pd.read_csv(os.path.join(PATH_HINDCAST, file_name))

    return hindcast


def recalculate_nan_forecasts():
    logger.info("--------------------------------------------------------------------")
    logger.info("Starting recalculate_nan_forecasts.py")
    print("--------------------------------------------------------------------")
    print("Starting recalculate_nan_forecasts.py")

    # --------------------------------------------------------------------
    # DEFINE WHICH MODEL TO USE
    # --------------------------------------------------------------------
    MODEL_TO_USE = os.getenv("SAPPHIRE_MODEL_TO_USE")
    logger.info("Model to use: %s", MODEL_TO_USE)
    print("Model to use:", MODEL_TO_USE)

    if MODEL_TO_USE not in ["TFT", "TIDE", "TSMIXER", "ARIMA"]:
        raise ValueError("Model not supported")

    # --------------------------------------------------------------------
    # Define whch prediction mode to use
    # --------------------------------------------------------------------
    PREDICTION_MODE = os.getenv("SAPPHIRE_PREDICTION_MODE")
    logger.debug("Prediction mode: %s", PREDICTION_MODE)
    if PREDICTION_MODE not in ["PENTAD", "DECAD"]:
        raise ValueError(
            f"Prediction mode {PREDICTION_MODE!r} is not supported.\nPlease choose one of the following prediction modes: PENTAD, DECAD"
        )

    # --------------------------------------------------------------------
    # INITIALIZE THE ENVIRONMENT
    # --------------------------------------------------------------------
    # Specify the path to the .env file
    sl.load_environment()

    # --------------------------------------------------------------------
    # GET THE LATEST FORECAST
    # --------------------------------------------------------------------
    intermediate_data_path = os.getenv("ieasyforecast_intermediate_data_path")

    # Path to the output directory
    OUTPUT_PATH_DISCHARGE = os.getenv("ieasyhydroforecast_OUTPUT_PATH_DISCHARGE")

    PATH_FORECAST = os.path.join(intermediate_data_path, OUTPUT_PATH_DISCHARGE)
    PATH_FORECAST = os.path.join(PATH_FORECAST, MODEL_TO_USE)

    # Get the current date
    current_date = datetime.datetime.now().date()
    current_date = current_date.strftime("%Y-%m-%d")

    if PREDICTION_MODE == "PENTAD":
        prefix = "pentad"
    else:
        prefix = "decad"

    forecast_path = os.path.join(PATH_FORECAST, prefix + "_" + MODEL_TO_USE + "_forecast.csv")

    # --- Read existing forecasts (API-primary, CSV fallback) ---
    from datetime import timedelta

    api_start = (datetime.date.today() - timedelta(days=730)).isoformat()
    permitted_codes = get_permitted_station_codes()

    if permitted_codes is not None and len(permitted_codes) > 0:
        # Per-code reads — each query ≤730 rows, fits in one page,
        # avoiding non-deterministic pagination (ML-007).
        frames = []
        for code in sorted(permitted_codes):
            df = _read_ml_forecasts_from_api(
                model_type=MODEL_TO_USE,
                horizon_type=prefix,
                start_date=api_start,
                code=code,
            )
            if not df.empty:
                frames.append(df)
        forecast = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    else:
        # Config unavailable — fall back to all-codes query (existing behavior)
        logger.warning(
            "recalculate_nan_forecasts: org config unavailable — falling back "
            "to all-codes read (non-deterministic pagination may affect results)"
        )
        forecast = _read_ml_forecasts_from_api(
            model_type=MODEL_TO_USE,
            horizon_type=prefix,
            start_date=api_start,
        )

    # CSV fallback — triggers if API returned empty for all codes
    if forecast.empty:
        logger.warning(
            "recalculate_nan_forecasts: API returned no %s %s forecasts — falling back to CSV",
            MODEL_TO_USE,
            prefix,
        )
        try:
            forecast = pd.read_csv(forecast_path)
            forecast = normalize_ml_csv_columns(forecast)
        except FileNotFoundError:
            logger.error("No forecast file found (API and CSV both empty)")
            return
        # CSV contains all orgs' data — re-apply org-filter
        if permitted_codes is not None and len(permitted_codes) > 0 and not forecast.empty:
            forecast = forecast[forecast["code"].astype(str).isin(permitted_codes)]

    # Second emptiness guard (preserved from original lines 211-218)
    if forecast.empty:
        logger.warning(
            "recalculate_nan_forecasts: Both API and CSV empty for %s %s. "
            "NaN recalculation skipped.",
            MODEL_TO_USE,
            prefix,
        )
        return

    unique_codes = forecast["code"].unique()

    codes_with_nan = []
    min_missing_dates = []
    max_missing_dates = []

    forecast["flag"] = pd.to_numeric(forecast["flag"], errors="coerce")
    try:
        # First attempt with default parsing
        forecast["date"] = pd.to_datetime(forecast["date"])
        forecast["forecast_date"] = pd.to_datetime(forecast["forecast_date"])
    except Exception:
        # Fallback to mixed format parsing if the first attempt fails
        try:
            forecast["date"] = pd.to_datetime(forecast["date"], format="mixed")
            forecast["forecast_date"] = pd.to_datetime(forecast["forecast_date"], format="mixed")
        except Exception as e:
            # Handle the case where both parsing methods fail
            logger.error("Error parsing date columns: %s", e)
            raise e

    # remove duplicates based on code and date and keep last
    forecast = forecast.drop_duplicates(subset=["code", "date", "forecast_date"], keep="last")

    for code in unique_codes:
        # select the forecast for the specific code
        forecast_code = forecast[forecast["code"] == code].copy()

        # check where the flag is equal to 1
        nan_values = forecast_code[forecast_code["flag"].isin([1, 2])]

        if nan_values.shape[0] > 0:
            min_missing_date = nan_values["forecast_date"].min()
            max_missing_date = nan_values["forecast_date"].max()

            min_missing_dates.append(min_missing_date)
            max_missing_dates.append(max_missing_date)
            codes_with_nan.append(code)

    if len(codes_with_nan) == 0:
        logger.debug("No forecasts to recalculate. Exiting recalculate_nan_forecasts.py\n")
        return

    # call the hindcast script
    max_date = max(max_missing_dates).strftime("%Y-%m-%d")
    # min date - 1 day
    min_date = min(min_missing_dates) - datetime.timedelta(days=1)
    min_date = min_date.strftime("%Y-%m-%d")

    logger.debug("Recalculating forecasts for codes %s", codes_with_nan)
    logger.debug("Min missing date: %s", min_date)
    logger.debug("Max missing date: %s", max_date)

    print("Recalculating forecasts for codes:", codes_with_nan)
    print("Min missing date:", min_date)
    print("Max missing date:", max_date)

    try:
        hindcast = call_hindcast_script(
            min_missing_date=min_date,
            max_missing_date=max_date,
            MODEL_TO_USE=MODEL_TO_USE,
            intermediate_data_path=intermediate_data_path,
            codes_with_nan=codes_with_nan,
            PREDICTION_MODE=PREDICTION_MODE,
        )
    except (FileNotFoundError, RuntimeError) as exc:
        logger.error(
            "Hindcast call failed for model=%s, mode=%s, dates=[%s..%s]: %s",
            MODEL_TO_USE,
            PREDICTION_MODE,
            min_date,
            max_date,
            exc,
        )
        return

    if hindcast.empty or "date" not in hindcast.columns:
        logger.warning("Hindcast returned empty — skipping")
        return

    print("Hindcast shape:", hindcast.shape)
    print("Hindcast columns:", hindcast.columns)
    print("Hindcast head:", hindcast.head())
    # --------------------------------------------------------------------
    # UPDATE THE FORECAST
    # Only replace the values with flag == 1
    hindcast["flag"] = pd.to_numeric(hindcast["flag"], errors="coerce")
    n_nan_flags = hindcast["flag"].isna().sum()
    if n_nan_flags > 0:
        logger.warning(
            "recalculate_nan_forecasts: %d hindcast rows have missing flag "
            "— assigning flag=3 (permanent failure)",
            n_nan_flags,
        )
        hindcast.loc[hindcast["flag"].isna(), "flag"] = 3
    hindcast["date"] = pd.to_datetime(hindcast["date"])
    hindcast["forecast_date"] = pd.to_datetime(hindcast["forecast_date"])
    hindcast["code"] = hindcast["code"].astype(str)
    # Normalize forecast codes to str so per-code filters match hindcast codes
    forecast["code"] = forecast["code"].astype(str)
    codes_with_nan = [str(c) for c in codes_with_nan]

    def update_forecast(forecast_code, hindcast_code):
        value_cols = [col for col in forecast_code.columns if "Q" in col]
        forecast_code = forecast_code.copy()
        hindcast_code = hindcast_code.copy()

        forecast_dates_flag1 = forecast_code[forecast_code["flag"].isin([1, 2])][
            "forecast_date"
        ].unique()

        # Track which rows originally had flag in [1, 2]
        original_flag12_mask = forecast_code["flag"].isin([1, 2])

        for forecast_date in forecast_dates_flag1:
            fc_mask = forecast_code["forecast_date"] == forecast_date
            hc_mask = hindcast_code["forecast_date"] == forecast_date

            if not hc_mask.any():
                continue

            fc_rows = forecast_code.loc[fc_mask].copy()
            hc_rows = hindcast_code.loc[hc_mask][["date"] + value_cols + ["flag"]].copy()

            # Align on target date — left join so only matching rows update
            merged = fc_rows[["date"]].merge(hc_rows, on="date", how="left", suffixes=("", "_hc"))
            merged = merged.set_index(fc_rows.index)

            for col in value_cols:
                hc_col = col + "_hc" if col + "_hc" in merged.columns else col
                valid = merged[hc_col].notna()
                forecast_code.loc[
                    fc_mask & valid.reindex(forecast_code.index, fill_value=False),
                    col,
                ] = merged.loc[valid, hc_col].values

            flag_col = "flag_hc" if "flag_hc" in merged.columns else "flag"
            valid_flag = merged[flag_col].notna()
            forecast_code.loc[
                fc_mask & valid_flag.reindex(forecast_code.index, fill_value=False),
                "flag",
            ] = merged.loc[valid_flag, flag_col].values

        # Rows that were flag=1/2 and got updated (flag changed)
        changed_mask = original_flag12_mask & ~forecast_code["flag"].isin([1, 2])
        applied_rows = forecast_code.loc[changed_mask]

        return forecast_code, applied_rows

    # Main loop — collect rows that were actually applied
    replaced_rows = []
    for code in codes_with_nan:
        forecast_code = forecast[forecast["code"] == code].copy()
        hindcast_code = hindcast[hindcast["code"] == code].copy()
        try:
            updated, applied = update_forecast(forecast_code, hindcast_code)
            forecast[forecast["code"] == code] = updated
            if not applied.empty:
                replaced_rows.append(applied)
        except Exception as e:
            logger.error(
                "recalculate_nan_forecasts: update_forecast failed for "
                "code=%s: %s — skipping code, NaN records preserved.",
                code,
                e,
            )
            # Do NOT re-raise; allow remaining codes to be processed
            # and the API write to execute

    # Save the updated forecast
    forecast["forecast_date"] = pd.to_datetime(forecast["forecast_date"])
    forecast = forecast.sort_values(by="forecast_date")

    # --- Write to API first (primary), then CSV (deprecated fallback) ---
    api_write_ok = False
    if SAPPHIRE_API_AVAILABLE and replaced_rows:
        try:
            api_data = pd.concat(replaced_rows, ignore_index=True)
            horizon_type = "pentad" if prefix == "pentad" else "decade"
            api_write_ok = _write_ml_forecast_to_api(api_data, horizon_type, MODEL_TO_USE)
            if api_write_ok:
                logger.info(
                    "Wrote %d recalculated forecasts to API (out of %d hindcast rows)",
                    len(api_data),
                    len(hindcast),
                )
            else:
                logger.warning(
                    "API write returned failure for %d forecasts (model=%s)",
                    len(api_data),
                    MODEL_TO_USE,
                )
        except Exception as e:
            logger.error("Failed to write recalculated forecasts to API: %s", e)

    # CSV write (deprecated fallback)
    csv_path = os.path.join(PATH_FORECAST, prefix + "_" + MODEL_TO_USE + "_forecast.csv")
    forecast = normalize_ml_csv_columns(forecast)
    forecast.to_csv(csv_path, index=False)

    if not api_write_ok:
        logger.warning(
            "API write unsuccessful; data persisted only in CSV: %s",
            csv_path,
        )

    logger.info("Nan Values are replaced. Exiting recalculate_nan_forecasts.py\n")
    logger.info("--------------------------------------------------------------------")
    print("Nan Values are replaced. Exiting recalculate_nan_forecasts.py\n")
    print("--------------------------------------------------------------------")


# ─── Recovery preview (P1a, preview-only) ────────────────────────────────────
#
# These functions are purely additive and read-only.  They do not change the
# existing recalculate_nan_forecasts() flow, flags 1/2 handling, CSV paths,
# or any writer.  No inference, no writes, no config mutation occurs here.
#
# Entry point: preview_recovery_candidates(issue_dates, models, station_codes)
# Unit of recovery: (issue_date, model, code) → 11 daily target rows.
# DAY uniqueness key (API): (horizon_type, code, model_type, date, target).
# ─────────────────────────────────────────────────────────────────────────────

_RECOVERY_SUPPORTED_MODELS: frozenset[str] = frozenset({"TFT", "TIDE", "TSMIXER"})

# The five quantile fields persisted by the API writer (q05/q25/q50/q75/q95).
# API column names as returned by read_short_term_forecasts (not renamed).
_PERSISTED_API_FIELDS: tuple[str, ...] = ("q05", "q25", "forecasted_discharge", "q75", "q95")


class RecoveryConfigError(Exception):
    """Raised when pre-read gate checks fail before any API read."""


def _expected_targets(issue_date_str: str) -> list[str]:
    """Return the 11 expected ISO target date strings for an issue date.

    Targets are issue_date + 1 day through issue_date + 11 days (the full
    DECAD fan per the approved single-generation contract).
    """
    issue = datetime.date.fromisoformat(issue_date_str)
    return [(issue + datetime.timedelta(days=i)).isoformat() for i in range(1, 12)]


def _validate_date_column(
    series: pd.Series,
    col_name: str,
) -> tuple:
    """Validate a date column against the strict raw-API date contract.

    Each value must be either:
    - A raw ``datetime.date`` (exact type — ``datetime.datetime`` and
      ``pd.Timestamp`` are sub-types and are rejected), or
    - A canonical ``"YYYY-MM-DD"`` string with an exact isoformat roundtrip
      (timestamps, timezone offsets, compact, non-canonical → blocked).

    Returns:
        ``(None, list_of_iso_strings)`` on success.
        ``(error_reason_str, None)`` on the first violation found.
    """
    result: list = []
    for v in series:
        if v is None:
            return f"null value in {col_name} column", None
        try:
            if pd.isna(v):
                return f"null or NaT value in {col_name} column", None
        except (TypeError, ValueError):
            pass
        # datetime.datetime and pd.Timestamp are datetime.date sub-types — must be
        # rejected BEFORE the datetime.date branch; midnight "00:00:00" timestamps
        # would otherwise silently pass the date check.
        if isinstance(v, (datetime.datetime, pd.Timestamp)):
            return (
                f"{col_name} column contains datetime.datetime or pd.Timestamp; "
                "only datetime.date (exact type) or YYYY-MM-DD string accepted",
                None,
            )
        if isinstance(v, datetime.date):
            result.append(v.isoformat())
            continue
        if isinstance(v, str):
            try:
                parsed = datetime.date.fromisoformat(v)
            except ValueError:
                return f"{col_name} column contains invalid date string {v!r}", None
            # Exact roundtrip rejects non-canonical forms: timestamps, timezone
            # offsets, compact YYYYMMDD, fractional seconds, etc.
            if parsed.isoformat() != v:
                return (
                    f"{col_name} column contains non-canonical date string {v!r}; "
                    f"expected {parsed.isoformat()!r}",
                    None,
                )
            result.append(v)
            continue
        return (
            f"{col_name} column contains unexpected type {type(v).__name__!r}; "
            "only datetime.date or YYYY-MM-DD string accepted",
            None,
        )
    return None, result


def _classify_unit_for_recovery(
    issue_date_str: str,
    api_model_type: str,
    code: str,
    rows: pd.DataFrame,
) -> dict:
    """Classify one (issue_date, model, code) unit for recovery eligibility.

    Args:
        issue_date_str: ISO date string for the issue date (= API "date" field).
        api_model_type: API-format model name ("TFT", "TiDE", "TSMixer").
        code: Station code string (str-normalised by caller).
        rows: Raw API rows for this unit as returned by read_unit_for_recovery_preview.
            May be empty (no rows for this unit).

    Returns:
        dict with keys:
            status: "candidate" | "blocked" | "skipped"
            reason: str
            replace_keys: list[tuple[str, str, str]] — (code, issue_date, target_date)
                keys to replace on execute; empty unless status=="candidate"
            insert_keys: list[tuple[str, str, str]] — keys to INSERT (absent tail);
                empty unless status=="candidate" with the approved six+five shape
            n_targets: int — total planned target count; 0 unless status=="candidate"
    """

    def _blocked(reason: str) -> dict:
        return {
            "status": "blocked",
            "reason": reason,
            "replace_keys": [],
            "insert_keys": [],
            "n_targets": 0,
        }

    def _skipped(reason: str) -> dict:
        return {
            "status": "skipped",
            "reason": reason,
            "replace_keys": [],
            "insert_keys": [],
            "n_targets": 0,
        }

    def _candidate(replace_keys: list, insert_keys: list) -> dict:
        return {
            "status": "candidate",
            "reason": "wholly failed flag-3 unit; all persisted fields null",
            "replace_keys": replace_keys,
            "insert_keys": insert_keys,
            "n_targets": len(replace_keys) + len(insert_keys),
        }

    # Step 1: No rows at all → not a flag-3 case
    if rows.empty:
        return _skipped("no rows present for this unit; absent records are not flag-3")

    # Step 2: Identity validation — required columns must be present
    _required_id = {"horizon_type", "code", "model_type", "date", "target", "flag"}
    missing_id = _required_id - set(rows.columns)
    if missing_id:
        return _blocked(
            f"malformed response: missing required identity columns {sorted(missing_id)}"
        )
    missing_q = [f for f in _PERSISTED_API_FIELDS if f not in rows.columns]
    if missing_q:
        return _blocked(f"malformed response: missing persisted quantile columns {missing_q}")

    # Validate per-row identity — returned code must be raw str exactly equal to the
    # requested string; numeric types (int, float, etc.) must BLOCK — do not coerce.
    _raw_code_col = rows["code"]
    _nonstr_codes = [type(v).__name__ for v in _raw_code_col if not isinstance(v, str)]
    if _nonstr_codes:
        from collections import Counter as _CCounter

        return _blocked(
            f"returned code column contains non-string type(s) {dict(_CCounter(_nonstr_codes))}; "
            "API ForecastResponse.code is str — numeric values indicate upstream type coercion"
        )
    if not _raw_code_col.eq(str(code)).all():
        unexpected = sorted(set(_raw_code_col) - {str(code)})
        return _blocked(f"returned rows contain unexpected station codes: {unexpected}")

    if not rows["horizon_type"].eq("day").all():
        return _blocked("returned rows contain non-day horizon_type")

    if not rows["model_type"].eq(api_model_type).all():
        _mt_col = rows["model_type"]
        # sorted() on a mixed-type set (null, int, str) raises TypeError — diagnose safely.
        _nonstr = [type(v).__name__ for v in _mt_col if not isinstance(v, str)]
        if _nonstr:
            from collections import Counter as _Counter

            return _blocked(
                f"model_type column contains non-string values {dict(_Counter(_nonstr))}; "
                f"expected typed string {api_model_type!r}"
            )
        unexpected_mt = sorted(set(_mt_col.astype(str)) - {api_model_type})
        return _blocked(f"returned rows contain unexpected model_type: {unexpected_mt}")

    _date_err, _date_strs = _validate_date_column(rows["date"], "date")
    if _date_err:
        return _blocked(_date_err)
    issue_dates_col = pd.Series(_date_strs, index=rows.index)
    if not issue_dates_col.eq(issue_date_str).all():
        return _blocked(f"returned rows have issue dates other than {issue_date_str}")

    # Step 3: Target date validation and uniqueness
    expected_targets = _expected_targets(issue_date_str)
    _tgt_err, _tgt_strs = _validate_date_column(rows["target"], "target")
    if _tgt_err:
        return _blocked(_tgt_err)
    target_strs = pd.Series(_tgt_strs, index=rows.index)

    if target_strs.duplicated().any():
        dup = sorted(target_strs[target_strs.duplicated(keep=False)].unique().tolist())
        return _blocked(f"duplicate target dates in returned rows: {dup}")

    extra_targets = sorted(set(target_strs) - set(expected_targets))
    if extra_targets:
        return _blocked(f"target dates outside expected 11-day window: {extra_targets}")

    # Step 4: Flag check — all must be flag=3 for this unit to be eligible.
    # Booleans silently coerce to 1/0 in pd.to_numeric — catch them first.
    if rows["flag"].apply(lambda x: isinstance(x, bool)).any():
        return _blocked("flag column contains boolean value(s); only integer flags 0-4 are valid")

    # Distinguish original null from conversion failure: a nonnull value that
    # fails numeric parse must BLOCK, not silently become null (NaN).
    _originally_null = rows["flag"].isna()

    try:
        _flags_numeric = pd.to_numeric(rows["flag"], errors="coerce")
    except Exception:
        return _blocked("malformed flag column in returned rows")

    _parse_failed = ~_originally_null & _flags_numeric.isna()
    if _parse_failed.any():
        return _blocked("flag column contains unparseable non-null value(s)")

    if _originally_null.any():
        return _blocked("flag column contains null value(s)")

    # Non-integer floats (e.g. 2.5, inf) must BLOCK, not be truncated.
    def _is_non_integer(x):
        return not (np.isfinite(x) and float(x) == float(int(x)))

    if _flags_numeric.apply(_is_non_integer).any():
        bad = sorted(_flags_numeric[_flags_numeric.apply(_is_non_integer)].unique().tolist())
        return _blocked(f"flag column contains non-integer value(s): {bad}")

    flags = _flags_numeric.astype(int)

    # Unknown flag values outside {0,1,2,3,4} preserve block.
    _VALID_FLAGS = frozenset({0, 1, 2, 3, 4})
    _unknown_flags = set(flags.unique().tolist()) - _VALID_FLAGS
    if _unknown_flags:
        return _blocked(f"flag column contains unknown flag value(s): {sorted(_unknown_flags)}")

    has_flag3 = flags.eq(3).any()
    all_flag3 = flags.eq(3).all()

    if not has_flag3:
        non3 = sorted(flags.unique().tolist())
        if all(f in (0, 1, 2, 4) for f in non3):
            return _skipped(f"no flag-3 rows; unit carries flags {non3}")
        return _blocked(f"no flag-3 rows; unexpected flags {non3}")

    if not all_flag3:
        non3_flags = sorted(flags[~flags.eq(3)].unique().tolist())
        return _blocked(
            f"non-flag-3 rows present alongside flag-3: {non3_flags}; cannot isolate unit"
        )

    # Step 5: Quantile value check — invalid non-null, non-finite blocks; any finite skips.
    # Capture original null mask before conversion: a non-null value that fails numeric
    # parse (string "garbage", string "NaN", etc.) must BLOCK, not silently become null.
    has_nonfinite = False
    has_finite = False
    for field in _PERSISTED_API_FIELDS:
        # Boolean values must BLOCK — pd.to_numeric coerces True/False → 0/1 which then
        # pass np.isfinite and produce a false "skipped" result rather than "blocked".
        if rows[field].apply(lambda x: isinstance(x, bool)).any():
            return _blocked(
                f"quantile field '{field}' contains boolean value(s); only numeric or null is valid"
            )
        _orig_null = rows[field].isna()
        col_numeric = pd.to_numeric(rows[field], errors="coerce")
        # Non-null original that fails numeric conversion → invalid payload (string, etc.)
        _conv_failed = ~_orig_null & col_numeric.isna()
        if _conv_failed.any():
            return _blocked(f"quantile field '{field}' contains non-numeric non-null value(s)")
        non_null = col_numeric[~_orig_null]
        if non_null.empty:
            continue  # all null for this field — OK
        finite_mask = np.isfinite(non_null.values)
        if not finite_mask.all():
            has_nonfinite = True
        else:
            has_finite = True

    if has_nonfinite:
        return _blocked("non-finite value in persisted quantile field(s)")
    if has_finite:
        return _skipped(
            "flag-3 rows have finite persisted values; "
            "original full quantile state unknown, not an anomaly"
        )

    # Step 6: Shape check — only wholly null flag-3 units reach here
    present_set = set(target_strs)
    n_present = len(rows)
    expected_first6 = set(expected_targets[:6])

    if present_set == set(expected_targets) and n_present == 11:
        # Complete 11 — all targets present, all null flag-3
        # Keys carry full API identity: (horizon_type, code, model_type, date, target)
        replace_keys = [
            ("day", str(code), api_model_type, issue_date_str, t) for t in expected_targets
        ]
        return _candidate(replace_keys, [])

    elif present_set == expected_first6 and n_present == 6:
        # Approved six+five shape: 6 present, 5 absent tail
        # Confirm the absent tail (issue+7..11) is not present with other flags
        # (already validated above that no out-of-window targets exist)
        replace_keys = [
            ("day", str(code), api_model_type, issue_date_str, t) for t in expected_targets[:6]
        ]
        insert_keys = [
            ("day", str(code), api_model_type, issue_date_str, t) for t in expected_targets[6:]
        ]
        return _candidate(replace_keys, insert_keys)

    else:
        # Unsupported shape
        missing = sorted(set(expected_targets) - present_set)
        n_missing = len(missing)
        head_missing = expected_targets[0] not in present_set
        shape_desc = (
            "missing head targets"
            if head_missing
            else f"interior gap or partial tail ({n_present} of 11 present)"
        )
        return _blocked(
            f"unsupported shape ({shape_desc}; present={n_present}, missing={n_missing})"
        )


def preview_recovery_candidates(
    issue_dates: list[str],
    models: list[str],
    station_codes: list[str],
    client_factory=None,
) -> dict:
    """Preview recovery eligibility for flag-3 wholly-failed units. Read-only.

    Checks all configuration gates before constructing any client or reading
    any data.  Returns a structured classification result.  Never writes,
    runs inference, or mutates any configuration.

    Args:
        issue_dates: List of ISO date strings to preview (e.g. ["2026-10-20"]).
        models: List of model names ("TFT", "TIDE", "TSMIXER").  Must be a
            non-empty subset of the three supported DL models.
        station_codes: List of station code strings.  Must be non-empty.
        client_factory: Optional callable() → SapphirePostprocessingClient.
            If None, constructs using SAPPHIRE_API_URL.  Injected in tests so
            no real API or model is contacted.

    Returns:
        {
            "gate_blocked": str | None,   # reason if blocked before any reads
            "candidates": [               # units eligible for retry
                {
                    "issue_date": str,
                    "model": str,         # uppercase internal name
                    "code": str,
                    "replace_keys": list[tuple[str, str, str]],
                    "insert_keys": list[tuple[str, str, str]],
                    "n_targets": int,
                }
            ],
            "blocked": [                  # units with read errors or unsafe shapes
                {"issue_date": str, "model": str, "code": str, "reason": str}
            ],
            "skipped": [                  # units with no flag-3 or protected rows
                {"issue_date": str, "model": str, "code": str, "reason": str}
            ],
            "n_units_checked": int,
        }
    """
    _empty: dict = {
        "gate_blocked": None,
        "candidates": [],
        "blocked": [],
        "skipped": [],
        "n_units_checked": 0,
    }

    # Input validation: non-empty selectors and ISO date parse before any API contact.
    # Invalid selector structure blocks rather than crashing or querying the DB.
    if not issue_dates:
        return {**_empty, "gate_blocked": "empty issue_dates list; no units to preview"}
    if not models:
        return {**_empty, "gate_blocked": "empty models list; no units to preview"}
    if not station_codes:
        return {**_empty, "gate_blocked": "empty station_codes list; no units to preview"}

    _normalized_dates: list[str] = []
    for _raw_d in issue_dates:
        _d = str(_raw_d).strip()
        try:
            _normalized_dates.append(datetime.date.fromisoformat(_d).isoformat())
        except ValueError:
            return {
                **_empty,
                "gate_blocked": f"invalid issue_date {_raw_d!r}: not a valid ISO date",
            }

    _normalized_models = [str(m).strip().upper() for m in models]
    _normalized_codes = [str(c).strip() for c in station_codes]

    # Gate 1: API must be enabled
    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
    if not api_enabled:
        return {
            **_empty,
            "gate_blocked": ("SAPPHIRE_API_ENABLED=false; recovery preview requires API access"),
        }

    # Gate 2: Client library must be installed
    if not SAPPHIRE_API_AVAILABLE:
        return {
            **_empty,
            "gate_blocked": "sapphire-api-client not installed; cannot preview recovery",
        }

    # Gate 3: ML models must be enabled — affirmative check matching project contract
    # (.lower() == "true"). Missing, empty, garbage, false, 0, no → gate blocked.
    ml_raw = os.getenv("ieasyhydroforecast_run_ML_models", "").strip()
    ml_enabled = ml_raw.lower() == "true"
    if not ml_enabled:
        return {
            **_empty,
            "gate_blocked": (
                "ieasyhydroforecast_run_ML_models is disabled; recovery preview blocked"
            ),
        }

    # Gate 4: Models must be a supported subset
    unsupported = [m for m in _normalized_models if m not in _RECOVERY_SUPPORTED_MODELS]
    if unsupported:
        return {
            **_empty,
            "gate_blocked": (
                f"unsupported model(s): {unsupported}; "
                f"supported: {sorted(_RECOVERY_SUPPORTED_MODELS)}"
            ),
        }

    # Gate 5: Configured ML model allowlist — explicit selectors cannot bypass config.
    _ml_models_cfg = os.getenv("ieasyhydroforecast_available_ML_models", "").strip()
    if not _ml_models_cfg:
        return {
            **_empty,
            "gate_blocked": (
                "ieasyhydroforecast_available_ML_models not configured; "
                "cannot determine which models are available for recovery"
            ),
        }
    _configured_models = frozenset(
        m.strip().upper() for m in _ml_models_cfg.split(",") if m.strip()
    )
    if not _configured_models:
        return {
            **_empty,
            "gate_blocked": (
                "ieasyhydroforecast_available_ML_models is empty after parsing; "
                "no models configured for recovery"
            ),
        }
    _not_configured = [m for m in _normalized_models if m not in _configured_models]
    if _not_configured:
        return {
            **_empty,
            "gate_blocked": (
                f"requested model(s) {_not_configured} not in configured allowlist "
                f"{sorted(_configured_models)}"
            ),
        }

    # Gate 6: Permitted station codes — config must be available and non-empty;
    #          requested codes must be a subset.  Explicit selectors cannot bypass.
    _permitted = get_permitted_station_codes()
    if _permitted is None:
        return {
            **_empty,
            "gate_blocked": (
                "station configuration unavailable; "
                "cannot determine permitted stations for recovery"
            ),
        }
    if not _permitted:
        return {
            **_empty,
            "gate_blocked": (
                "station configuration is empty; no permitted stations configured for recovery"
            ),
        }
    _not_permitted = [c for c in _normalized_codes if c not in _permitted]
    if _not_permitted:
        return {
            **_empty,
            "gate_blocked": (
                f"{len(_not_permitted)} requested station(s) not in permitted station configuration"
            ),
        }

    # Build client (factory injection for tests; direct construction for production)
    if client_factory is not None:
        client = client_factory()
    else:
        api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
        client = SapphirePostprocessingClient(base_url=api_url)

    # API readiness check
    try:
        ready = client.readiness_check()
    except Exception as exc:
        return {**_empty, "gate_blocked": f"API readiness check failed: {exc}"}
    if not ready:
        return {**_empty, "gate_blocked": "API not ready; cannot preview recovery"}

    candidates: list[dict] = []
    blocked: list[dict] = []
    skipped: list[dict] = []
    n_units = 0
    _seen_units: set = set()

    for issue_date in _normalized_dates:
        for model_upper in _normalized_models:
            api_model_type = ML_MODEL_TYPE_MAP.get(model_upper, model_upper)
            for code_str in _normalized_codes:
                _unit_key = (issue_date, model_upper, code_str)
                if _unit_key in _seen_units:
                    continue
                _seen_units.add(_unit_key)
                n_units += 1

                try:
                    rows = read_unit_for_recovery_preview(
                        client=client,
                        api_model_type=api_model_type,
                        issue_date=issue_date,
                        code=code_str,
                    )
                except RecoveryReadError as exc:
                    blocked.append(
                        {
                            "issue_date": issue_date,
                            "model": model_upper,
                            "code": code_str,
                            "reason": f"read error: {exc}",
                        }
                    )
                    continue

                try:
                    result = _classify_unit_for_recovery(
                        issue_date_str=issue_date,
                        api_model_type=api_model_type,
                        code=code_str,
                        rows=rows,
                    )
                except Exception as _clf_exc:
                    blocked.append(
                        {
                            "issue_date": issue_date,
                            "model": model_upper,
                            "code": code_str,
                            "reason": f"classification error ({type(_clf_exc).__name__})",
                        }
                    )
                    continue
                entry: dict = {
                    "issue_date": issue_date,
                    "model": model_upper,
                    "code": code_str,
                }
                if result["status"] == "candidate":
                    candidates.append(
                        {
                            **entry,
                            "replace_keys": result["replace_keys"],
                            "insert_keys": result["insert_keys"],
                            "n_targets": result["n_targets"],
                        }
                    )
                elif result["status"] == "blocked":
                    blocked.append({**entry, "reason": result["reason"]})
                else:
                    skipped.append({**entry, "reason": result["reason"]})

    return {
        "gate_blocked": None,
        "candidates": candidates,
        "blocked": blocked,
        "skipped": skipped,
        "n_units_checked": n_units,
    }


# ─── Recovery compute helper (P1b) ───────────────────────────────────────────
#
# Purely additive. Does not modify recalculate_nan_forecasts() or any existing
# function.  Launches hindcast_ML_models.py in compute-only mode for a single
# validated preview candidate and returns a validated DataFrame.  No API writes,
# no CSV writes occur here or in the child process.
#
# Entry point: compute_recovery_unit(candidate, timeout_seconds)
# ─────────────────────────────────────────────────────────────────────────────

_RECOVERY_SUPPORTED_COMPUTE_MODELS: frozenset[str] = frozenset({"TFT", "TIDE", "TSMIXER"})
_RECOVERY_19_Q_COLS: tuple[str, ...] = (
    "Q5",
    "Q10",
    "Q15",
    "Q20",
    "Q25",
    "Q30",
    "Q35",
    "Q40",
    "Q45",
    "Q50",
    "Q55",
    "Q60",
    "Q65",
    "Q70",
    "Q75",
    "Q80",
    "Q85",
    "Q90",
    "Q95",
)


def compute_recovery_unit(
    candidate: dict,
    timeout_seconds: float,
) -> "pd.DataFrame | None":
    """Compute one validated preview candidate via hindcast_ML_models in compute-only mode.

    Launches the hindcast script as a subprocess with an internal compute-only
    marker.  The child runs predict() once for the single source code at the
    given issue date and writes a strict JSON result.  This function validates
    the response and returns a DataFrame on success, or None on any failure.

    Args:
        candidate: A single dict from preview_recovery_candidates()["candidates"],
            with at minimum keys: issue_date, model, code.
        timeout_seconds: Budget in seconds for the child process.  Must be > 0.

    Returns:
        pd.DataFrame with columns [date, code, forecast_date, flag, Q5..Q95]
        on success; None if the compute failed for any reason.
    """
    if not (
        isinstance(timeout_seconds, (int, float))
        and np.isfinite(timeout_seconds)
        and timeout_seconds > 0
    ):
        logger.error(
            "compute_recovery_unit: timeout_seconds must be a finite positive number, got %r",
            timeout_seconds,
        )
        return None

    required_keys = {"issue_date", "model", "code"}
    if not required_keys.issubset(candidate.keys()):
        logger.error(
            "compute_recovery_unit: candidate missing keys %s",
            required_keys - set(candidate.keys()),
        )
        return None

    issue_date = str(candidate["issue_date"])
    model = str(candidate["model"]).upper()
    code = str(candidate["code"])

    if model not in _RECOVERY_SUPPORTED_COMPUTE_MODELS:
        logger.error("compute_recovery_unit: unsupported model %r (ARIMA blocked)", model)
        return None

    try:
        datetime.date.fromisoformat(issue_date)
    except ValueError:
        logger.error("compute_recovery_unit: invalid issue_date %r", issue_date)
        return None

    invocation_id = str(uuid.uuid4())
    deadline_utc = (
        datetime.datetime.utcnow() + datetime.timedelta(seconds=timeout_seconds)
    ).isoformat()

    request: dict = {
        "invocation_id": invocation_id,
        "model": model,
        "source_code": code,
        "issue_date": issue_date,
        "deadline_utc": deadline_utc,
    }

    import tempfile as _tmp

    with _tmp.TemporaryDirectory(prefix="sapphire_recovery_") as tmpdir:
        request_path = os.path.join(tmpdir, "request.json")
        response_path = os.path.join(tmpdir, "response.json")

        try:
            with open(request_path, "w") as fh:
                json.dump(request, fh, allow_nan=False)
        except Exception as e:
            logger.error("compute_recovery_unit: failed to write request JSON: %s", e)
            return None

        env = os.environ.copy()
        env["SAPPHIRE_MODEL_TO_USE"] = model
        env["SAPPHIRE_HINDCAST_MODE"] = "DECAD"
        env["ieasyhydroforecast_START_DATE"] = issue_date
        env["ieasyhydroforecast_END_DATE"] = issue_date
        env["ieasyhydroforecast_NEW_STATIONS"] = code
        env["_SAPPHIRE_RECOVERY_COMPUTE_REQUEST_PATH"] = request_path
        env["_SAPPHIRE_RECOVERY_COMPUTE_RESPONSE_PATH"] = response_path
        env["PYTHONUNBUFFERED"] = "1"

        hindcast_script = os.path.join(script_dir, "hindcast_ML_models.py")
        command = [sys.executable, hindcast_script]

        _mono_start = time.monotonic()
        try:
            result = subprocess.run(
                command,
                env=env,
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            logger.error(
                "compute_recovery_unit: subprocess timed out after %.1fs for %s/%s/%s",
                timeout_seconds,
                model,
                code,
                issue_date,
            )
            return None
        except Exception as e:
            logger.error("compute_recovery_unit: subprocess launch failed: %s", e)
            return None

        _mono_elapsed = time.monotonic() - _mono_start
        if _mono_elapsed >= timeout_seconds:
            logger.error(
                "compute_recovery_unit: monotonic elapsed %.2fs >= budget %.2fs; result discarded",
                _mono_elapsed,
                timeout_seconds,
            )
            return None

        if result.returncode != 0:
            logger.error(
                "compute_recovery_unit: child exited %d for %s/%s/%s",
                result.returncode,
                model,
                code,
                issue_date,
            )
            return None

        # Read and validate response JSON strictly.
        try:
            with open(response_path) as fh:
                response = json.load(fh)
        except FileNotFoundError:
            logger.error(
                "compute_recovery_unit: response file missing for %s/%s/%s", model, code, issue_date
            )
            return None
        except OSError as e:
            logger.error("compute_recovery_unit: cannot read response file: %s", e)
            return None
        except json.JSONDecodeError as e:
            logger.error("compute_recovery_unit: response JSON invalid: %s", e)
            return None
        if not isinstance(response, dict):
            logger.error(
                "compute_recovery_unit: response is not a JSON object (got %s)",
                type(response).__name__,
            )
            return None

        # Validate invocation binding — rejects stale/replayed/mismatched responses.
        if response.get("invocation_id") != invocation_id:
            logger.error(
                "compute_recovery_unit: invocation_id mismatch (expected %s, got %s)",
                invocation_id,
                response.get("invocation_id"),
            )
            return None
        if response.get("model") != model:
            logger.error("compute_recovery_unit: response model mismatch")
            return None
        if str(response.get("source_code", "")) != code:
            logger.error("compute_recovery_unit: response source_code mismatch")
            return None
        if response.get("issue_date") != issue_date:
            logger.error("compute_recovery_unit: response issue_date mismatch")
            return None
        if response.get("flag") != 4:
            logger.error(
                "compute_recovery_unit: response flag is %r, expected 4", response.get("flag")
            )
            return None

        rows = response.get("rows")
        if not isinstance(rows, list) or len(rows) != 11:
            logger.error(
                "compute_recovery_unit: expected 11 rows, got %s",
                len(rows) if isinstance(rows, list) else type(rows).__name__,
            )
            return None

        issue_dt = datetime.date.fromisoformat(issue_date)
        expected_targets = {
            (issue_dt + datetime.timedelta(days=i)).isoformat() for i in range(1, 12)
        }
        actual_targets: set = set()
        for row in rows:
            if not isinstance(row, dict):
                logger.error(
                    "compute_recovery_unit: row is not a dict (got %s)",
                    type(row).__name__,
                )
                return None
            d = row.get("date")
            if not isinstance(d, str):
                logger.error("compute_recovery_unit: row date is not a string: %r", d)
                return None
            actual_targets.add(d)
            for q in _RECOVERY_19_Q_COLS:
                v = row.get(q)
                if v is None:
                    logger.error("compute_recovery_unit: missing quantile %s in row", q)
                    return None
                if isinstance(v, bool):
                    logger.error(
                        "compute_recovery_unit: boolean value in %s; only numeric allowed", q
                    )
                    return None
                if not isinstance(v, (int, float)):
                    logger.error("compute_recovery_unit: non-numeric value in %s: %r", q, v)
                    return None
                try:
                    fv = float(v)
                except (TypeError, ValueError):
                    logger.error("compute_recovery_unit: cannot convert %s value to float", q)
                    return None
                if not np.isfinite(fv):
                    logger.error("compute_recovery_unit: non-finite value in %s", q)
                    return None

        if actual_targets != expected_targets:
            logger.error(
                "compute_recovery_unit: target date mismatch (expected %s, got %s)",
                sorted(expected_targets),
                sorted(actual_targets),
            )
            return None

        if len(actual_targets) != len(rows):
            logger.error("compute_recovery_unit: duplicate target dates in response")
            return None

        # Build result DataFrame — preserves full 19 quantiles for later writer.
        records = []
        for row in rows:
            rec: dict = {
                "date": pd.to_datetime(row["date"]),
                "code": code,
                "forecast_date": pd.to_datetime(issue_date),
                "flag": 4,
            }
            for q in _RECOVERY_19_Q_COLS:
                rec[q] = float(row[q])
            records.append(rec)

        return pd.DataFrame(records)


if __name__ == "__main__":
    recalculate_nan_forecasts()
