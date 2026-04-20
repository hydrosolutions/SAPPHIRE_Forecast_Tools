# ----------------------------------------------------------------
# FILE: recalculate_nan_forecasts.py
# ----------------------------------------------------------------
#
# Description: This script checks if there are any nan values in the forecasts and then recalculates them.
# Nan values from operational forecasts have flag == 0, while nan values from hindcasts have flag == 1.
# This script checks if there are nan values in the forecasts and then recalculates them, by calling the hindcast script.
# The hindcast will return a file which is already flagged:
# - flag == 3 for nan values even after the hindcast
# - flag == 4 for valid values after the hindcast
# ----------------------------------------------------------------
# USAGE:
# SAPPHIRE_OPDEV_ENV=True SAPPHIRE_MODEL_TO_USE=TFT SAPPHIRE_PREDICTION_MODE=PENTAD python recalculate_nan_forecasts.py
# ----------------------------------------------------------------
import datetime
import logging
import os
import subprocess
import sys
from logging.handlers import TimedRotatingFileHandler

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
    SAPPHIRE_API_AVAILABLE,
    _read_ml_forecasts_from_api,
    _write_ml_forecast_to_api,
    get_permitted_station_codes,
    normalize_ml_csv_columns,
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

    file_name = (
        f"{MODEL_TO_USE}_{PREDICTION_MODE}_hindcast_daily_{min_missing_date}_{max_missing_date}.csv"
    )

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
            "Prediction mode %s is not supported.\nPlease choose one of the following prediction modes: PENTAD, DECAD"
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


if __name__ == "__main__":
    recalculate_nan_forecasts()
