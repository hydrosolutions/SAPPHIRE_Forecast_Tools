# --------------------------------------------------------------------
# FORCING DATA PREPROCESSING
# --------------------------------------------------------------------
#        _
#      _( )_
#    _(     )_
#   (_________)
#     \  \  \
#       \  \
#         \  \
# --------------------------------------------------------------------
# DESCRIPTION:
# This script reads in the ERA5-Land data and ECMWF IFS weather forecast data
# from the Data-Gateaway and, if parameters for quantile mapping are available,
# performs qunatile mapping for the daily precipitation sum P and the daily
# average air temperature T with pre-defined parameters.
# The Formula for the Mapping is: y_fit = a * y_era_5^b
#
# If access to the data gateway is not available, the script will return an
# exit value of 1 and print a warning message. The subsequent modelling steps
# based on the machine learning and conceptual models will not be run. The
# linear regression models will still be run.
# --------------------------------------------------------------------
# --------------------------------------------------------------------
# INPUT:
# ERA5-Land and ECMWF IFS weather forecast data from the SAPPHIRE data gateaway
# --------------------------------------------------------------------
# Pre-defined Parameters for the Quantile Mapping
# COLUMNS for the Parameters: 'code', 'a', 'b', 'wet_day'
# Saved as HRU{HRU_CODE}_T_params.csv and HRU{HRU_CODE}_P_params.csv
# --------------------------------------------------------------------
# --------------------------------------------------------------------
# OUTPUT:
# Quantile Mapped Data
# P_control_member.csv and T_control_member.csv
# With columns: 'date', 'P/T', and code
# Saved as {HRU_CODE}_P_control_member.csv and {HRU_CODE}_T_control_member.csv
# ENSEMBLE MEMBERS
# columns are 'date', 'P/T', 'code', 'ensemble_member'
# Saved as: {HRU_CODE}_P_ensemble_forecast.csv and {HRU_CODE}_T_ensemble_forecast.csv
# --------------------------------------------------------------------
# TODO:
# - Include the Real Parameters for the Quantile Mapping
# - Include Nan Cases -> what happens when the data-gateaway raises an error etc?
# - Test if all codes in ieasyhydroforecast_HRU_ENSEMBLE are in ieasyhydroforecast_HRU_CONTROL_MEMBER and print a waring if not.

# Required Libraries
# Install libraries from iEasyHydroForecast/requirements.txt

# Useage
# cd to the directory where the script is located (apps/preprocessing_gateway)
# ieasyhydroforecast_env_file_path=/path/to/.env python Quantile_Mapping_OP.py

# Author: Sandro Hunziker


# --------------------------------------------------------------------
# Import Libraries
# --------------------------------------------------------------------
import errno
import json
import logging
import os
import shutil
import sys
import time
import traceback
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler

import dg_utils
import pandas as pd
import requests

# Note that the sapphire data gateway client is currently a private repository
# Access to the repository is required to install the package
# Further, access to the data gateway through an API key is required to use the
# client. The API key is stored in a .env file in the root directory of the project.
# The forecast tools can be used without access to the sapphire data gateay but
# the full power of the tools is only available with access to the data gateway.
# pip install git+https://github.com/hydrosolutions/sapphire-dg-client.git
import sapphire_dg_client

# SAPPHIRE API client for writing processed data to the API
# Optional - if not installed, API writing is skipped
try:
    from sapphire_api_client import SapphireAPIError, SapphirePreprocessingClient

    SAPPHIRE_API_AVAILABLE = True
except ImportError:
    SAPPHIRE_API_AVAILABLE = False
    SapphirePreprocessingClient = None
    SapphireAPIError = Exception

# Local libraries
# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, "..", "iEasyHydroForecast")
# print(script_dir)
# print(forecast_dir)

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the setup_library module from the iEasyHydroForecast package
import setup_library as sl

# Set up logging
# Configure the logging level and formatter
logging.basicConfig(level=logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# Create the logs directory if it doesn't exist
if not os.path.exists("logs"):
    os.makedirs("logs")

# Create a file handler to write logs to a file
# A new log file is created every <interval> day at <when>. It is kept for <backupCount> days.
file_handler = TimedRotatingFileHandler("logs/log", when="midnight", interval=1, backupCount=30)
file_handler.setFormatter(formatter)

# Create a stream handler to print logs to the console
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Get the root logger and add the handlers to it
logger = logging.getLogger()
logger.handlers = []
logger.addHandler(file_handler)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)


# --------------------------------------------------------------------
# TRANSPORT RETRY HELPER (PREPG-010)
# --------------------------------------------------------------------
# A transient reset/aborted connection on a single Data Gateway download
# should not fail the whole run. This is a small, hard-coded retry:
# exactly one retry (two attempts total), a short fixed pause before the
# retry, and the original exception re-raised unchanged if the retry is
# exhausted. Deliberately no env var, config surface, or CLI flag.
_RETRY_MAX_ATTEMPTS = 2
_RETRY_SLEEP_SECONDS = 2
# Indirection so tests can replace the sleep with a no-op instead of
# incurring a real delay (CLAUDE.md forbids sleep() in tests).
_retry_sleep = time.sleep

# requests.exceptions.SSLError and ProxyError both subclass
# ConnectionError and are therefore retried along with it -- deliberate,
# see PREPG-010 ("SSLError is retried"). ChunkedEncodingError is a
# sibling class (a reset that lands mid-body), not a subclass, so it is
# listed separately.
_RETRYABLE_TRANSPORT_ERRORS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
)


def _call_with_transport_retry(download_fn, context: str):
    """
    Call a Data Gateway download once, retrying once on a transport fault.

    Retries exactly once (two attempts total) when ``download_fn()``
    raises ``requests.exceptions.ConnectionError`` (which also covers
    ``SSLError`` and ``ProxyError``, both subclasses) or
    ``requests.exceptions.ChunkedEncodingError``. Any other exception
    (including ``ValueError``, used elsewhere for the today->yesterday
    fallback) is not retried and propagates immediately.

    Never logs the raw exception, endpoint, or URL: the Data Gateway can
    embed the API key in its error messages (PREPG-015). Only the
    attempt number, caller-supplied context, and exception class name
    are logged.

    Args:
        download_fn: Zero-argument callable performing a single Data
            Gateway download.
        context: Short, non-sensitive description used for logging
            (e.g. HRU code, model index, date). Must not include the
            URL or API key.

    Returns:
        Whatever ``download_fn()`` returns.

    Raises:
        Exception: The original exception raised by ``download_fn()``,
            unchanged, if it is not a retryable transport error or if
            the retry is exhausted.
    """
    for attempt in range(1, _RETRY_MAX_ATTEMPTS + 1):
        try:
            return download_fn()
        except _RETRYABLE_TRANSPORT_ERRORS as e:
            if attempt >= _RETRY_MAX_ATTEMPTS:
                raise
            logger.warning(
                "Transport fault on attempt %d/%d (%s): %s. Retrying.",
                attempt,
                _RETRY_MAX_ATTEMPTS,
                context,
                type(e).__name__,
            )
            _retry_sleep(_RETRY_SLEEP_SECONDS)


# The API-key redaction helper used below moved to dg_utils.redact_api_key
# (PREPG-015) -- shared across the module's Data Gateway call sites plus
# snow_data_operational.py, snow_data_renalysis.py, and
# get_era5_reanalysis_data.py.


def transform_data_file_ensemble_member(data_file: pd.DataFrame, HRU_CODE: str) -> pd.DataFrame:
    """
    Transforms the data file from the data gateaway in a more handy format.
    Inputs:
        data_file: pd.DataFrame with the data from the data gateaway. columns are the names resp. elevation bands
    Outputs:
        transformed_data: pd.DataFrame with the transformed data. Columns are 'date', 'Value (either P or T)', 'name' -> Later the HRU Code is added
    """
    data_file = data_file.copy()
    # rename the Station column to 'date'
    data_file.rename(columns={"Unnamed: 0": "date"}, inplace=True)

    # get the type of data
    value_type = data_file.iloc[0].values[1]

    # than we need to drop the first 4 rows of the era5 data
    data_file = data_file.iloc[4:]

    # now we need to convert the date column to a datetime object
    data_file["date"] = pd.to_datetime(data_file["date"], dayfirst=True)

    data_file = data_file.sort_values("date")

    transformed_data_file = pd.DataFrame()

    # unique names, here they are actually the names of the different HRU
    names = data_file.columns[1:]

    for name in names:
        # get the data for the code
        code_data = data_file[["date", name]].copy()
        # rename the columns
        code_data.rename(columns={name: value_type}, inplace=True)
        # Add the 'name' column
        code_data["code"] = HRU_CODE
        code_data["name"] = name
        # Convert 'Value' column to numeric, coercing errors
        code_data[value_type] = pd.to_numeric(code_data[value_type], errors="coerce").astype(float)
        transformed_data_file = pd.concat([transformed_data_file, code_data], axis=0)

    return transformed_data_file


# --------------------------------------------------------------------
# MERGE ENSEMBLE FORECAST
# --------------------------------------------------------------------
def merge_ensemble_forecast(files_downloaded: list) -> tuple[pd.DataFrame | None, set[str]]:
    """
    Merges the ensemble forecast files into one DataFrame.

    PREPG-023 C2: this used to `sys.exit(1)` the whole module when a
    variable was missing, which aborted every remaining HRU. It now
    reports the outcome to its caller instead, so the per-HRU loop in
    main() can record this HRU as failed and move on to the next one.

    Inputs:
        files_downloaded: list of strings with the paths to the files downloaded.
    Outputs:
        A tuple ``(merged_data, missing_variables)``:
          - ``merged_data``: the merged DataFrame, or ``None`` if any
            required variable ("P" and/or "T") is missing.
          - ``missing_variables``: a subset of ``{"P", "T"}`` naming
            which variables were missing. Empty when ``merged_data`` is
            not ``None``. An empty ``files_downloaded`` list is treated
            as both variables missing: ``{"P", "T"}``.
    """
    # Check if files_downloaded is empty
    if not files_downloaded:
        logger.error("No files downloaded for this HRU.")
        return None, {"P", "T"}

    # combine the data
    P_ensemble = pd.DataFrame()
    T_ensemble = pd.DataFrame()
    for file in files_downloaded:
        elements = file.split("_")
        # From the second last element, remove the first 3 characters ('HRU')
        HRU_CODE = elements[-2][3:]
        # HRU_CODE = elements[-2][-5:]
        variable = elements[-1].split(".")[0]
        ensemble_member = elements[-3][3:]
        # read the data file
        data_file = pd.read_csv(file)
        # transform the data file
        transformed_data_file = transform_data_file_ensemble_member(data_file, HRU_CODE)
        transformed_data_file["ensemble_member"] = int(ensemble_member)

        if variable == "tp":
            P_ensemble = pd.concat([P_ensemble, transformed_data_file], axis=0)
        elif variable == "2t":
            T_ensemble = pd.concat([T_ensemble, transformed_data_file], axis=0)
        else:
            logger.warning(f"Variable {variable} not recognized. Skipping file {file}.")
            continue

    # Test if P_ensemble and T_ensemble are empty
    missing: set[str] = set()
    if P_ensemble.empty:
        logger.error("No precipitation data found in the ensemble forecast files.")
        missing.add("P")
    if T_ensemble.empty:
        logger.error("No temperature data found in the ensemble forecast files.")
        missing.add("T")
    if missing:
        return None, missing

    # combine the P and T data, on code, than name, than ensemble_member than date
    P_ensemble = P_ensemble.sort_values(["code", "name", "ensemble_member", "date"])
    T_ensemble = T_ensemble.sort_values(["code", "name", "ensemble_member", "date"])

    combined_df = pd.merge(
        P_ensemble, T_ensemble, on=["code", "name", "ensemble_member", "date"], how="outer"
    )

    # clear the memory
    del P_ensemble
    del T_ensemble

    return combined_df, missing


def _write_meteo_to_api(
    data: pd.DataFrame,
    meteo_type: str,
    hru_code: str,
    mode: str | None = None,
) -> bool:
    """
    Write meteorological data to SAPPHIRE preprocessing API.

    Supports different sync modes:
    - operational (default): write yesterday's and today's data
      (2-day window guards against DG data lag)
    - maintenance: write the last 30 days of data
    - initial: write all data

    Args:
        data: DataFrame with meteo data. Expected columns:
            - date: date
            - P or T: precipitation or temperature value
            - code: station code
        meteo_type: Type of meteo data (T for temperature, P for precipitation)
        hru_code: HRU code for logging purposes
        mode: Sync mode override. If None, reads SAPPHIRE_SYNC_MODE
            env var, defaulting to 'operational'.

    Returns:
        True if successful, False otherwise
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.warning("sapphire-api-client not installed, skipping meteo API write")
        return False

    # Check if API writing is enabled (default: enabled)
    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
    if not api_enabled:
        logger.info("SAPPHIRE API writing disabled via SAPPHIRE_API_ENABLED=false")
        return False

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    client = SapphirePreprocessingClient(base_url=api_url)

    # Health check - non-blocking, skip if API unavailable
    if not client.readiness_check():
        logger.warning(f"SAPPHIRE API at {api_url} is not ready, skipping meteo write")
        return False

    if data.empty:
        logger.info(f"No meteo data to write to API ({meteo_type}) for HRU {hru_code}")
        return False

    # Ensure date column is datetime
    data = data.copy()
    data["date"] = pd.to_datetime(data["date"])

    # Filter data based on sync mode (parameter > env var > default)
    if mode is not None:
        sync_mode = mode.lower()
    else:
        sync_mode = os.getenv("SAPPHIRE_SYNC_MODE", "operational").lower()
    logger.info(
        "QM meteo API sync mode: %s (%s, HRU %s)",
        sync_mode,
        meteo_type,
        hru_code,
    )

    today = pd.Timestamp.today().normalize()
    yesterday = today - pd.Timedelta(days=1)
    if sync_mode == "operational":
        data_to_write = data[data["date"] >= yesterday]
    elif sync_mode == "maintenance":
        cutoff = today - pd.Timedelta(days=30)
        data_to_write = data[data["date"] >= cutoff]
    elif sync_mode == "initial":
        data_to_write = data
    else:
        logger.warning(
            "Unknown sync mode '%s', defaulting to operational",
            sync_mode,
        )
        data_to_write = data[data["date"] >= yesterday]

    logger.info(
        "%s mode: %d meteo records to write (%s, HRU %s)",
        sync_mode,
        len(data_to_write),
        meteo_type,
        hru_code,
    )

    if data_to_write.empty:
        logger.info(
            "No meteo data to write after %s filtering (%s, HRU %s)",
            sync_mode,
            meteo_type,
            hru_code,
        )
        return False

    # Determine column names for value
    value_col = meteo_type  # 'T' or 'P'

    # Prepare records for API
    records = []
    for _, row in data_to_write.iterrows():
        # Parse date
        date_obj = pd.to_datetime(row["date"]) if pd.notna(row.get("date")) else None
        if date_obj is None:
            logger.warning(f"Skipping meteo row with missing date: {row.to_dict()}")
            continue

        record = {
            "meteo_type": meteo_type.upper(),  # API expects uppercase
            "code": str(row["code"]),
            "date": date_obj.strftime("%Y-%m-%d"),
            "value": round(float(row[value_col]), 3)
            if value_col in row and pd.notna(row.get(value_col))
            else None,
            "norm": None,  # Control member data doesn't have norm values
            "day_of_year": date_obj.dayofyear,
        }
        records.append(record)

    # Write to API
    if records:
        count = client.write_meteo(records)
        logger.info(
            f"Successfully wrote {count} meteo records to SAPPHIRE API ({meteo_type}, HRU {hru_code})"
        )
        print(
            f"SAPPHIRE API: Successfully wrote {count} meteo records ({meteo_type}, HRU {hru_code})"
        )
        return True
    else:
        logger.info(f"No meteo records to write to API ({meteo_type}, HRU {hru_code})")
        return False


def _check_meteo_consistency(csv_data: pd.DataFrame, meteo_type: str, hru_code: str) -> bool:
    """
    Check consistency between CSV data and API data for meteo.

    Reads back from the API and compares with the CSV data that was
    written. Enabled via SAPPHIRE_CONSISTENCY_CHECK=true environment
    variable.

    Args:
        csv_data: DataFrame that was written to CSV
        meteo_type: Type of meteo data (T for temperature, P for
            precipitation)
        hru_code: HRU code for logging purposes

    Returns:
        True if consistent (or check disabled), False if
        inconsistencies found
    """
    tag = f"CONSISTENCY_CHECK [{meteo_type}, HRU {hru_code}]"

    consistency_check = os.getenv("SAPPHIRE_CONSISTENCY_CHECK", "false").lower() == "true"
    if not consistency_check:
        return True

    if not SAPPHIRE_API_AVAILABLE:
        logger.warning("%s: sapphire-api-client not installed, skipping", tag)
        return True

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    client = SapphirePreprocessingClient(base_url=api_url)

    # Get the date range from CSV data
    csv_data = csv_data.copy()
    csv_data["date"] = pd.to_datetime(csv_data["date"])

    # The write function filters to yesterday onward (includes forecast); match that window
    today = pd.Timestamp.today().normalize()
    yesterday = today - pd.Timedelta(days=1)
    csv_recent = csv_data[csv_data["date"] >= yesterday].copy()

    if csv_recent.empty:
        logger.warning(
            "%s: No CSV rows from %s onward, nothing to verify. CSV date range: %s to %s",
            tag,
            yesterday.date(),
            csv_data["date"].min().date(),
            csv_data["date"].max().date(),
        )
        return True

    codes = csv_recent["code"].unique()
    csv_val_col = meteo_type if meteo_type in csv_recent.columns else None
    logger.info(
        "%s: Verifying API data for dates %s to %s, codes=%s, api_url=%s",
        tag,
        yesterday.date(),
        csv_recent["date"].max().date(),
        list(codes),
        api_url,
    )
    if csv_val_col:
        for code in codes:
            csv_vals = csv_recent.loc[csv_recent["code"] == code, csv_val_col].tolist()
            logger.info("%s: CSV values for code=%s: %s", tag, code, csv_vals)

    # Read from API for each code
    all_api_data = []
    for code in codes:
        try:
            api_df = client.read_meteo(
                meteo_type=meteo_type.upper(),
                code=str(code),
                start_date=yesterday.strftime("%Y-%m-%d"),
                end_date=csv_recent["date"].max().strftime("%Y-%m-%d"),
                limit=1000,
            )
            if api_df.empty:
                logger.warning(
                    "%s: API returned 0 rows for code=%s, date=%s", tag, code, today.date()
                )
            else:
                val_col = "value" if "value" in api_df.columns else None
                if val_col:
                    vals = api_df[val_col].tolist()
                    logger.info(
                        "%s: API returned %d rows for code=%s, values=%s",
                        tag,
                        len(api_df),
                        code,
                        vals,
                    )
                else:
                    logger.info(
                        "%s: API returned %d rows for code=%s, columns=%s",
                        tag,
                        len(api_df),
                        code,
                        list(api_df.columns),
                    )
                all_api_data.append(api_df)
        except Exception as e:
            logger.error("%s: Error reading from API for code=%s: %s", tag, code, e)
            return False

    if not all_api_data:
        # Diagnostic: try reading without date filter to see if ANY
        # data exists for this meteo_type
        try:
            any_data = client.read_meteo(meteo_type=meteo_type.upper(), limit=5)
            if any_data.empty:
                logger.warning(
                    "%s: FAILED - No data returned from API. "
                    "Diagnostic: API has NO %s data at all. "
                    "The write may have succeeded (count was returned) "
                    "but data may not be persisted. "
                    "Check the preprocessing API logs.",
                    tag,
                    meteo_type.upper(),
                )
            else:
                dates_in_api = sorted(pd.to_datetime(any_data["date"]).dt.date.unique())
                codes_in_api = list(any_data["code"].unique())
                logger.warning(
                    "%s: FAILED - No data for %s to %s but API "
                    "has %s data for other dates: %s, codes: %s. "
                    "Possible date mismatch or write did not persist "
                    "recent records.",
                    tag,
                    yesterday.date(),
                    csv_recent["date"].max().date(),
                    meteo_type.upper(),
                    dates_in_api[:5],
                    codes_in_api[:5],
                )
        except Exception as e:
            logger.warning(
                "%s: FAILED - No data returned and diagnostic read also failed: %s", tag, e
            )
        return False

    # Merge and compare
    api_data = pd.concat(all_api_data, ignore_index=True)
    api_data["date"] = pd.to_datetime(api_data["date"])
    api_data["code"] = api_data["code"].astype(str)
    csv_recent["code"] = csv_recent["code"].astype(str)

    is_consistent = True

    # Compare row counts
    if len(api_data) != len(csv_recent):
        logger.warning(
            "%s: Row count mismatch - API: %d, CSV: %d", tag, len(api_data), len(csv_recent)
        )
        is_consistent = False

    # Merge on code and date
    merged = csv_recent.merge(
        api_data, on=["code", "date"], how="outer", suffixes=("_csv", "_api"), indicator=True
    )

    only_csv = merged[merged["_merge"] == "left_only"]
    only_api = merged[merged["_merge"] == "right_only"]

    if len(only_csv) > 0:
        logger.warning("%s: %d rows in CSV but not in API", tag, len(only_csv))
        is_consistent = False

    if len(only_api) > 0:
        logger.warning("%s: %d rows in API but not in CSV", tag, len(only_api))
        is_consistent = False

    # Compare value column
    both = merged[merged["_merge"] == "both"]
    if len(both) > 0:
        csv_val_col = meteo_type if meteo_type in csv_recent.columns else None
        if csv_val_col and "value" in api_data.columns:
            csv_values = both.get(f"{csv_val_col}_csv", both.get(csv_val_col))
            api_values = both.get("value_api", both.get("value"))

            if csv_values is not None and api_values is not None:
                csv_values = pd.to_numeric(csv_values, errors="coerce")
                api_values = pd.to_numeric(api_values, errors="coerce")
                diff = (csv_values - api_values).abs()
                mismatches = diff[diff > 0.01]

                if len(mismatches) > 0:
                    logger.warning(
                        "%s: %d value mismatches (max diff: %.4f)", tag, len(mismatches), diff.max()
                    )
                    is_consistent = False

    if is_consistent:
        logger.info("%s: PASSED - API matches CSV", tag)
        print(f"CONSISTENCY_CHECK: PASSED ({meteo_type}, HRU {hru_code})")
    else:
        logger.error("%s: FAILED - inconsistencies found", tag)
        print(f"CONSISTENCY_CHECK: FAILED ({meteo_type}, HRU {hru_code}) - see log for details")

    return is_consistent


# --------------------------------------------------------------------
# ENSEMBLE CONSUMPTION GATE (PREPG-023)
# --------------------------------------------------------------------
def _env_flag_is_true(raw_value: str | None) -> bool:
    """
    Parse an environment variable as a boolean flag, case-insensitively.

    Only the literal string "true" (in any case, with surrounding
    whitespace ignored) is truthy. Unset (None) and every other value are
    False. This is the single shared reader for the ensemble consumption
    gate's two inputs (ieasyhydroforecast_run_CM_models and
    ieasyhydroforecast_ensemble_forcing_required) so this module does not
    add another spelling of a boolean on top of the four incompatible ones
    already in the repo (INFRA-038).

    Args:
        raw_value: The raw environment variable value, or None if unset.

    Returns:
        True if raw_value case-insensitively equals "true", else False.
    """
    if raw_value is None:
        return False
    return raw_value.strip().lower() == "true"


def _parse_ensemble_hru_list(raw_value: str | None) -> list[str]:
    """
    Parse ieasyhydroforecast_HRU_ENSEMBLE into a list of HRU codes.

    Unset (None), an empty or whitespace-only string, and the literal
    sentinel "None" (matching the pre-existing convention for "no
    ensemble HRUs configured") all resolve to an empty list. Entries are
    split on commas, stripped of surrounding whitespace, empty entries
    are dropped, and duplicates are removed while preserving the first
    occurrence's order.

    Args:
        raw_value: The raw ieasyhydroforecast_HRU_ENSEMBLE value, or None.

    Returns:
        A list of unique, non-empty, stripped HRU codes (possibly empty).
    """
    if raw_value is None:
        return []
    stripped = raw_value.strip()
    if not stripped or stripped == "None":
        return []
    seen: set[str] = set()
    hrus: list[str] = []
    for entry in stripped.split(","):
        code = entry.strip()
        if not code or code in seen:
            continue
        seen.add(code)
        hrus.append(code)
    return hrus


# --------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------
def main():
    # --------------------------------------------------------------------
    # SETUP ENVIRONMENT
    # --------------------------------------------------------------------

    # Specify the path to the .env file
    # Loads the environment variables from the .env file
    sl.load_environment()

    # Test if an API key is available and exit the program if it isn't
    if not os.getenv("ieasyhydroforecast_API_KEY_GATEAWAY"):
        logger.warning(
            "No API key for the data gateway found. Exiting program.\nMachine learning or conceptual models will not be run."
        )
        sys.exit(1)
    else:
        API_KEY = os.getenv("ieasyhydroforecast_API_KEY_GATEAWAY")
    # output_path for control member and ensemble
    OUTPUT_PATH_CM = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyhydroforecast_OUTPUT_PATH_CM"),
    )
    # Test if the output path exists and create it if it doesn't
    if not os.path.exists(OUTPUT_PATH_CM):
        os.makedirs(OUTPUT_PATH_CM, exist_ok=True)

    # OUTPUT_PATH_ENS (ieasyhydroforecast_OUTPUT_PATH_ENS) is only created
    # lazily, once we know ensemble processing is actually required (see
    # the ensemble consumption gate ahead of the "ENSEMBLE MAPPING" section
    # below) — PREPG-023 C1.

    # output_path for the data from the data gateaway
    OUTPUT_PATH_DG = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyhydroforecast_OUTPUT_PATH_DG"),
    )
    # Test if the output path exists and create it if it doesn't
    if not os.path.exists(OUTPUT_PATH_DG):
        os.makedirs(OUTPUT_PATH_DG, exist_ok=True)

    # Remove all files from OUTPUT_PATH_DG if not in debug mode
    # print the logging level
    logger.debug(f"Logging level: {logger.level}")
    if logger.level > logging.DEBUG:  # Check if the logging level is higher than DEBUG
        try:
            for filename in os.listdir(OUTPUT_PATH_DG):
                file_path = os.path.join(OUTPUT_PATH_DG, filename)
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            logger.info(f"All files removed from {OUTPUT_PATH_DG} as not in debug mode.")
        except Exception as e:
            logger.error(f"Failed to remove files from {OUTPUT_PATH_DG}: {e}")

    logger.debug(f"OUTPUT_PATH_CM: {OUTPUT_PATH_CM}")
    logger.debug(f"OUTPUT_PATH_DG: {OUTPUT_PATH_DG}")
    logger.debug(f"Path OUTPUT_PATH_DG is valid: {os.path.exists(OUTPUT_PATH_DG)}")

    Q_MAP_PARAM_PATH = os.path.join(
        os.getenv("ieasyhydroforecast_models_and_scalers_path"),
        os.getenv("ieasyhydroforecast_Q_MAP_PARAM_PATH"),
    )
    # Test if the output path exists. Raise an error if it doesn't
    if not os.path.exists(Q_MAP_PARAM_PATH):
        logger.warning(
            f"Path {Q_MAP_PARAM_PATH} does not exist.\nParameters for quantile mapping of ERA5 and ECMWF ensemble forecast are not available.\nProducing weather data files that are not downscaled."
        )
        perform_qmapping = False
    else:
        perform_qmapping = True

    CONTROL_MEMBER_HRUS = os.getenv("ieasyhydroforecast_HRU_CONTROL_MEMBER")
    ENSEMBLE_HRUS_RAW = os.getenv("ieasyhydroforecast_HRU_ENSEMBLE")

    # Ensemble consumption gate (PREPG-023 C1): the ensemble block below is
    # only processed when the conceptual model (the known consumer) is
    # enabled, or a consumer declares the need explicitly via the new
    # ieasyhydroforecast_ensemble_forcing_required. Both are parsed
    # case-insensitively against "true", default off, through the same
    # helper — do not add another spelling of a boolean (see INFRA-038).
    run_cm_models_raw = os.getenv("ieasyhydroforecast_run_CM_models")
    ensemble_forcing_required_raw = os.getenv("ieasyhydroforecast_ensemble_forcing_required")
    run_cm_models = _env_flag_is_true(run_cm_models_raw)
    ensemble_forcing_required = _env_flag_is_true(ensemble_forcing_required_raw)
    ensembles_required = run_cm_models or ensemble_forcing_required

    logger.info("Meteo data configuration:")
    logger.info("  Q_MAP_PARAM_PATH: %s", Q_MAP_PARAM_PATH)
    logger.info("  Control member HRUs: %s", CONTROL_MEMBER_HRUS)
    logger.info("  Ensemble HRUs: %s", ENSEMBLE_HRUS_RAW)
    logger.info("  Quantile mapping: %s", perform_qmapping)

    # Initialize the client
    client = sapphire_dg_client.client.SapphireDGClient(api_key=API_KEY)
    # Get the codes for the HRU's
    control_member_hrus = [str(x) for x in CONTROL_MEMBER_HRUS.split(",")]
    # ieasyhydroforecast_HRU_ENSEMBLE is only parsed lazily, inside the
    # ensemble consumption gate ahead of the "ENSEMBLE MAPPING" section
    # below — parsing it here unconditionally is what used to raise
    # AttributeError on a deployment that never sets it.

    today = datetime.today().strftime("%Y-%m-%d")
    start_date = datetime.today() - timedelta(days=365)
    start_date = start_date.strftime("%Y-%m-%d")
    yesterday = datetime.today() - timedelta(days=1)
    yesterday = yesterday.strftime("%Y-%m-%d")

    logger.info("Date range: %s to %s (yesterday: %s)", start_date, today, yesterday)

    # Read configuration for mapping gateway station codes to hydromet station
    # codes, if file is available:
    # Path to the configuration file
    config_file = os.path.join(
        os.getenv("ieasyforecast_configuration_path"),
        os.getenv("ieasyhydroforecast_config_file_data_gateway_name_twins"),
    )
    logger.debug(f"Data gateway name mapping configuration file: {config_file}")
    # If the file is present, read the configuration
    if os.path.exists(config_file):
        # Read the configuration file
        with open(config_file) as f:
            config = json.load(f)
            # Get the mapping from the configuration
            mapping = config["gateway_name_twins"]
            logger.debug(f"Mapping from configuration: {mapping}")
    else:
        logger.debug("No configuration for mapping station codes found.")
        mapping = {}

    # --------------------------------------------------------------------
    # CONTROL MEMBER MAPPING
    # --------------------------------------------------------------------
    logger.info("=== Control member processing: %d HRUs ===", len(control_member_hrus))
    for cm_idx, c_m_hru in enumerate(control_member_hrus, 1):
        logger.info(
            "--- [%d/%d] Control member HRU %s ---", cm_idx, len(control_member_hrus), c_m_hru
        )
        print(f"Processing control member: HRU {c_m_hru}")
        # Initialize control_member_era5 to None
        control_member_era5 = None
        try:
            control_member_era5 = _call_with_transport_retry(
                lambda c_m_hru=c_m_hru: client.operational.get_control_spinup_and_forecast(
                    hru_code=c_m_hru, date=start_date, directory=OUTPUT_PATH_DG
                ),
                context=f"control member HRU {c_m_hru}",
            )

        except Exception as e:
            if "Operational data for HRU" in str(e):
                logger.error(f"Exiting the program due to error: {dg_utils.redact_api_key(str(e))}")
                sys.exit(1)
            else:
                # Narrowed 2026-08-20 (PREPG-010): previously this branch
                # had no else and no re-raise, so a non-matching
                # exception (e.g. an exhausted transport retry) was
                # silently discarded here and later misreported by the
                # "not available" check below as missing data rather
                # than a transport fault.
                # Log the exception CLASS only, not its message: the DG
                # client can embed the live API key in a ValueError's
                # text (sapphire_dg_client/client_base.py:55-60), and
                # this branch is reached by exactly the non-"Operational
                # data for HRU" messages that would otherwise write it
                # to the log. Safe to include the message text once
                # PREPG-015 lands a redaction helper.
                logger.error(
                    f"Control member download failed for HRU {c_m_hru} due to {type(e).__name__}"
                )
                sys.exit(1)

        # If control_member_era5 is empty, raise an error
        if not control_member_era5:
            logger.error(f"Control Member Data for HRU {c_m_hru} not available.")
            sys.exit(1)

        logger.debug(f"Control Member Data for HRU {c_m_hru} downloaded")
        logger.debug(f"for start_date: {start_date}")
        logger.debug(f"saved to directory: {OUTPUT_PATH_DG}")
        logger.debug(f"Control Member Data Path: {control_member_era5}")

        df_c_m = pd.read_csv(control_member_era5)

        # transform the data file
        transformed_data_file = dg_utils.transform_data_file_control_member(df_c_m)
        transformed_data_file["code"] = transformed_data_file["code"].astype(str)

        # get the parameters if available
        if perform_qmapping:
            logger.info("Performing Quantile Mapping for Control Member")
            P_params_hru = pd.read_csv(os.path.join(Q_MAP_PARAM_PATH, f"HRU{c_m_hru}_P_params.csv"))
            T_params_hru = pd.read_csv(os.path.join(Q_MAP_PARAM_PATH, f"HRU{c_m_hru}_T_params.csv"))

            # transform to string, as the other code is a string
            P_params_hru["code"] = P_params_hru["code"].astype(str)
            T_params_hru["code"] = T_params_hru["code"].astype(str)

            # perform the quantile mapping for the control member for the HRU's without Eleavtion bands
            P_data, T_data = dg_utils.do_quantile_mapping(
                transformed_data_file, P_params_hru, T_params_hru, ensemble=False
            )
            logger.info("Quantile Mapping for Control Member Done.")
        else:
            P_data = transformed_data_file[["date", "P", "code"]].copy()
            T_data = transformed_data_file[["date", "T", "code"]].copy()

        # check if there are nan values

        # TODO: check with Nikola what to do with Nan values, or what the expected amount of Nan values is
        if P_data.isnull().values.any():
            print(f"Nan values in P data for HRU {c_m_hru}")
            print("Take Last Observation")
            P_data = dg_utils.fill_gaps_grouped(P_data, "P", ["code"], "ffill")

        if T_data.isnull().values.any():
            print(f"Nan values in T data for HRU {c_m_hru}")
            print("Take Last Observation")
            T_data = dg_utils.fill_gaps_grouped(T_data, "T", ["code"], "interpolate")

        P_data.to_csv(os.path.join(OUTPUT_PATH_CM, f"{c_m_hru}_P_control_member.csv"), index=False)
        T_data.to_csv(os.path.join(OUTPUT_PATH_CM, f"{c_m_hru}_T_control_member.csv"), index=False)

        # Write meteo data to SAPPHIRE API (operational mode - latest date only)
        try:
            written = _write_meteo_to_api(P_data, "P", c_m_hru)
            # Run consistency check only if data was actually written
            if written:
                _check_meteo_consistency(P_data, "P", c_m_hru)
        except Exception as e:
            logger.error(f"Failed to write P data to SAPPHIRE API for HRU {c_m_hru}: {e}")
            # Don't fail the entire process - CSV was already written

        try:
            written = _write_meteo_to_api(T_data, "T", c_m_hru)
            # Run consistency check only if data was actually written
            if written:
                _check_meteo_consistency(T_data, "T", c_m_hru)
        except Exception as e:
            logger.error(f"Failed to write T data to SAPPHIRE API for HRU {c_m_hru}: {e}")
            # Don't fail the entire process - CSV was already written

        # clear memory
        del transformed_data_file

    # --------------------------------------------------------------------
    # ENSEMBLE  MAPPING
    # --------------------------------------------------------------------
    # ensemble_run_had_failures stays False when the gate is closed (C3:
    # a skipped ensemble stage is exit 0 -- there is no failure to report).
    # These three are initialized here (not just inside the `else` below)
    # so the truthful completion banner (PREPG-023 F1, ahead of the final
    # sys.exit) can report on the gate-closed state too.
    ensemble_run_had_failures = False
    hru_ensemble_forecast: list[str] = []
    ensemble_hrus_written: list[str] = []
    ensemble_hrus_failed: list[tuple[str, set[str]]] = []

    if not ensembles_required:
        logger.info(
            "Skipping ensemble processing: ieasyhydroforecast_run_CM_models=%r "
            "ieasyhydroforecast_ensemble_forcing_required=%r (both off).",
            run_cm_models_raw,
            ensemble_forcing_required_raw,
        )
    else:
        hru_ensemble_forecast = _parse_ensemble_hru_list(ENSEMBLE_HRUS_RAW)
        if not hru_ensemble_forecast:
            opened_by = [
                name
                for name, is_on in (
                    ("ieasyhydroforecast_run_CM_models", run_cm_models),
                    (
                        "ieasyhydroforecast_ensemble_forcing_required",
                        ensemble_forcing_required,
                    ),
                )
                if is_on
            ]
            logger.error(
                "Ensemble forcing is required (%s is on) but "
                "ieasyhydroforecast_HRU_ENSEMBLE names no HRUs (raw value=%r). "
                "Set ieasyhydroforecast_HRU_ENSEMBLE to at least one HRU code, "
                "or turn off %s.",
                " and ".join(opened_by),
                ENSEMBLE_HRUS_RAW,
                " and ".join(opened_by),
            )
            sys.exit(1)

        OUTPUT_PATH_ENS = os.path.join(
            os.getenv("ieasyforecast_intermediate_data_path"),
            os.getenv("ieasyhydroforecast_OUTPUT_PATH_ENS"),
        )
        # Test if the output path exists and create it if it doesn't
        if not os.path.exists(OUTPUT_PATH_ENS):
            os.makedirs(OUTPUT_PATH_ENS, exist_ok=True)
        logger.debug(f"OUTPUT_PATH_ENS: {OUTPUT_PATH_ENS}")

        # PREPG-023 C2: one bad HRU must not abort the module mid-flight.
        # The whole per-HRU unit -- download through write -- is wrapped
        # in a single try/except below so a failure at any stage is
        # recorded and the loop moves on to the next HRU, instead of the
        # previous `sys.exit(1)` calls that aborted every remaining HRU
        # (and left the already-durable control-member CSVs/API writes
        # reported as a total failure). The retry helper
        # (_call_with_transport_retry, PREPG-010) and the redaction
        # helper (dg_utils.redact_api_key, PREPG-015) are preserved
        # exactly on every error path. (ensemble_hrus_written and
        # ensemble_hrus_failed are initialized above, ahead of this
        # if/else, so they exist as empty lists for the gate-closed
        # completion banner too.)

        def _log_partial_ensemble_summary(attempted_idx: int, fault_hru: str) -> None:
            """PREPG-023 G3: a fatal fault re-raises from inside the
            loop below, which bypasses the normal after-loop summary
            (ensemble_run_had_failures / "ensemble_hrus_attempted=..."
            further down) entirely -- leaving no aggregate status at
            all, just a bare traceback. Call this immediately before
            each such `raise` so the counts accumulated so far
            (written/failed) survive alongside how far the loop got
            (attempted_idx) and which HRU triggered the abort. Do NOT
            call this for isolated (non-fatal) per-HRU failures -- those
            continue the loop and reach the real summary normally.
            """
            logger.error(
                "ensemble_hrus_attempted=%d written=%d failed=%d "
                "(partial -- ensemble loop aborted before completion by "
                "a fatal fault on HRU %s)",
                attempted_idx,
                len(ensemble_hrus_written),
                len(ensemble_hrus_failed),
                fault_hru,
            )

        for ens_idx, code_ens in enumerate(hru_ensemble_forecast, 1):
            logger.info(
                "--- [%d/%d] Ensemble HRU %s ---", ens_idx, len(hru_ensemble_forecast), code_ens
            )
            print(f"Processing HRU Ensemble: {code_ens} (gateway code)")
            print(f"Storing files downloaded to {OUTPUT_PATH_DG}")
            try:
                # download the ensemble forecast
                try:
                    files_downloaded = []
                    for model in range(1, 51):
                        files = _call_with_transport_retry(
                            lambda model=model,
                            code_ens=code_ens: client.ecmwf_ens.get_ensemble_forecast(
                                hru_code=code_ens,
                                date=today,
                                models=[str(model)],
                                directory=OUTPUT_PATH_DG,
                            ),
                            context=f"HRU {code_ens} model {model} date {today}",
                        )
                        files_downloaded.append(files)
                    # Unnest the list of lists
                    files_downloaded = [item for sublist in files_downloaded for item in sublist]
                    # May cause timeout errors from gateway server side. Better to download one by one.
                    # files_downloaded = client.ecmwf_ens.get_ensemble_forecast(
                    #    hru_code=code_ens,
                    #    date=today,
                    #    models=["pf"],
                    #    directory=OUTPUT_PATH_DG
                    # )
                except ValueError as e:
                    if "Couldn't find any files for the given HRU code, date and models!" in str(e):
                        print(f"No data for {today}, trying {yesterday}")
                        try:
                            files_downloaded = []
                            for model in range(1, 51):
                                files = _call_with_transport_retry(
                                    lambda model=model,
                                    code_ens=code_ens: client.ecmwf_ens.get_ensemble_forecast(
                                        hru_code=code_ens,
                                        date=yesterday,
                                        models=[str(model)],
                                        directory=OUTPUT_PATH_DG,
                                    ),
                                    context=f"HRU {code_ens} model {model} date {yesterday}",
                                )
                                files_downloaded.append(files)
                            # Unnest the list of lists
                            files_downloaded = [
                                item for sublist in files_downloaded for item in sublist
                            ]
                            # Attempt to download the ensemble forecast for yesterday
                            # files_downloaded = client.ecmwf_ens.get_ensemble_forecast(
                            #    hru_code=code_ens,
                            #    date=yesterday,
                            #    models=["pf"],
                            #    directory=OUTPUT_PATH_DG
                            # )
                        except ValueError as e2:
                            print(f"Error for date {yesterday}: {dg_utils.redact_api_key(str(e2))}")
                            print(dg_utils.redact_api_key(traceback.format_exc()))
                            # PREPG-023 C2: this used to sys.exit(1) here,
                            # aborting every remaining HRU. Re-raise so the
                            # per-HRU handler below records the failure and
                            # the loop continues with the next HRU.
                            raise
                    else:
                        # If it's a different error, record it and move on
                        # to the next HRU (PREPG-023 C2) instead of
                        # aborting the whole module.
                        print(
                            f"Unexpected error for date {today}: {dg_utils.redact_api_key(str(e))}"
                        )
                        print(dg_utils.redact_api_key(traceback.format_exc()))
                        raise

                # print(f"Files downloaded: {files_downloaded}")

                # A renaming of shapefiles sometimes is required in the data gateway.
                # The user can define name twins for the shapefiles in the data gateway
                # and in the hydromet in the configuration file:
                # ieasyhydroforecast_config_file_data_gateway_name_twins that is read at
                # before the loops.
                # Test if code_ens is in left column of the mapping
                code_ens_data_gateway = code_ens
                if code_ens in mapping:
                    logger.debug(f"Mapping found for {code_ens}")
                    # If it is, get the name from the right column
                    code_ens_data_gateway = code_ens
                    code_ens = mapping[code_ens]
                    logger.debug(f"Old code: {code_ens_data_gateway}, new code: {code_ens}")

                # merge the ensemble forecast
                combined_ensemble_forecast, missing_vars = merge_ensemble_forecast(files_downloaded)
                if combined_ensemble_forecast is None:
                    # PREPG-023 C2: a missing variable fails only this
                    # HRU -- record it and move to the next one instead
                    # of exiting the module.
                    ensemble_hrus_failed.append((code_ens, missing_vars))
                    continue

                # Replace code with the actual code from the mapping (if applicable)
                if code_ens_data_gateway in mapping:
                    combined_ensemble_forecast["code"] = code_ens

                combined_ensemble_forecast["code"] = combined_ensemble_forecast["code"].astype(str)

                # load the parameters
                if perform_qmapping:
                    P_params_hru = pd.read_csv(
                        os.path.join(Q_MAP_PARAM_PATH, f"HRU{c_m_hru}_P_params.csv")
                    )
                    T_params_hru = pd.read_csv(
                        os.path.join(Q_MAP_PARAM_PATH, f"HRU{c_m_hru}_T_params.csv")
                    )

                    P_params_hru["code"] = P_params_hru["code"].astype(str)
                    T_params_hru["code"] = T_params_hru["code"].astype(str)

                    # Perform the quantile mapping for the ensemble members
                    P_ensemble, T_ensemble = dg_utils.do_quantile_mapping(
                        combined_ensemble_forecast, P_params_hru, T_params_hru, ensemble=True
                    )
                else:
                    P_ensemble = combined_ensemble_forecast[
                        ["date", "P", "code", "ensemble_member"]
                    ].copy()
                    T_ensemble = combined_ensemble_forecast[
                        ["date", "T", "code", "ensemble_member"]
                    ].copy()

                # check if there are nan values
                if P_ensemble.isnull().values.any():
                    print(f"Nan values in P data (ensemble) for HRU {code_ens}")
                    print("Take Last Observation")
                    P_ensemble = dg_utils.fill_gaps_grouped(
                        P_ensemble, "P", ["ensemble_member"], "ffill"
                    )

                if T_ensemble.isnull().values.any():
                    print(f"Nan values in T data (ensemle) for HRU {code_ens}")
                    print("Take Last Observation")
                    T_ensemble = dg_utils.fill_gaps_grouped(
                        T_ensemble, "T", ["ensemble_member"], "interpolate"
                    )

                # save the data
                P_ensemble.to_csv(
                    os.path.join(OUTPUT_PATH_ENS, f"{code_ens}_P_ensemble_forecast.csv"),
                    index=False,
                )
                T_ensemble.to_csv(
                    os.path.join(OUTPUT_PATH_ENS, f"{code_ens}_T_ensemble_forecast.csv"),
                    index=False,
                )

                ensemble_hrus_written.append(code_ens)

            except MemoryError as e:
                # PREPG-023 F2(b): MemoryError is a process-wide fault,
                # not an HRU-local one -- continuing the loop under
                # memory exhaustion risks silently truncating further
                # HRU output. Log it (with traceback, for diagnosis) and
                # re-raise so the loop stops instead of isolating it.
                logger.error(
                    "Ensemble HRU %s failed (%s) -- not HRU-local, stopping the ensemble loop: %s",
                    code_ens,
                    type(e).__name__,
                    dg_utils.redact_api_key(str(e)),
                )
                logger.error(dg_utils.redact_api_key(traceback.format_exc()))
                # PREPG-023 G3: log the run-so-far summary before this
                # raise bypasses the after-loop summary below.
                _log_partial_ensemble_summary(ens_idx, code_ens)
                raise
            except OSError as e:
                # PREPG-023 G2: process-wide faults, same reasoning as
                # ENOSPC/EROFS below -- continuing to write more HRU
                # output after any of these is the same harm.
                #   ENOSPC  -- filesystem full
                #   EROFS   -- filesystem remounted read-only
                #   EDQUOT  -- the process's disk quota is exhausted
                #   ENOMEM  -- the kernel is out of memory (e.g. a failed
                #              write/mmap syscall under memory pressure)
                # Deliberately NOT included: EIO, EMFILE, EFBIG -- those
                # can plausibly be specific to this HRU's file/transfer
                # (a bad sector, this process's fd count, this file's
                # size) rather than process-wide, so they stay HRU-local
                # and fall through to the isolated branch below.
                if e.errno in (errno.ENOSPC, errno.EROFS, errno.EDQUOT, errno.ENOMEM):
                    # Everything else about OSError (e.g. a transient
                    # network OSError from the client) stays HRU-local
                    # and falls through to the isolated branch below.
                    logger.error(
                        "Ensemble HRU %s failed (%s, errno=%s) -- not "
                        "HRU-local, stopping the ensemble loop: %s",
                        code_ens,
                        type(e).__name__,
                        errno.errorcode.get(e.errno, e.errno),
                        dg_utils.redact_api_key(str(e)),
                    )
                    logger.error(dg_utils.redact_api_key(traceback.format_exc()))
                    # PREPG-023 G3: log the run-so-far summary before
                    # this raise bypasses the after-loop summary below.
                    _log_partial_ensemble_summary(ens_idx, code_ens)
                    raise
                logger.error(
                    "Ensemble HRU %s failed (%s): %s",
                    code_ens,
                    type(e).__name__,
                    dg_utils.redact_api_key(str(e)),
                )
                logger.error(dg_utils.redact_api_key(traceback.format_exc()))
                ensemble_hrus_failed.append((code_ens, set()))
            except Exception as e:
                # PREPG-023 C2: isolate this HRU's failure (download,
                # parse, quantile-map, or write) from the rest of the
                # loop. The download-path
                # branches above already print their own redacted
                # message before re-raising; this also catches any
                # exception raised further down in the per-HRU body
                # (e.g. an exhausted SSLError, or a parse/quantile-map
                # error) that was never printed anywhere else.
                #
                # PREPG-023 F2(a): the full traceback (redacted) is
                # logged at ERROR, not DEBUG -- a programming error
                # (TypeError, AttributeError, ...) must stay diagnosable
                # instead of being flattened into a one-line "this HRU
                # failed" message.
                logger.error(
                    "Ensemble HRU %s failed (%s): %s",
                    code_ens,
                    type(e).__name__,
                    dg_utils.redact_api_key(str(e)),
                )
                logger.error(dg_utils.redact_api_key(traceback.format_exc()))
                ensemble_hrus_failed.append((code_ens, set()))

        ensemble_run_had_failures = bool(ensemble_hrus_failed)
        logger.info(
            "ensemble_hrus_attempted=%d written=%d failed=%d",
            len(hru_ensemble_forecast),
            len(ensemble_hrus_written),
            len(ensemble_hrus_failed),
        )
        for failed_hru, failed_missing in ensemble_hrus_failed:
            if failed_missing:
                logger.error(
                    "Ensemble HRU %s failed: missing variables {%s}",
                    failed_hru,
                    ",".join(sorted(failed_missing)),
                )
            else:
                logger.error("Ensemble HRU %s failed during processing.", failed_hru)

    # PREPG-023 G1: this prefix used to claim "WEATHER DATA ... DONE"
    # and "ECMWF IFS FORECASTS WRITTEN" unconditionally. That was true
    # only for the control-member section above -- any control-member
    # failure has already sys.exit'd the module long before this point,
    # so reaching here does mean the control member is fully done -- but
    # it says nothing about the ensemble section (below, independently
    # gated and fallible). Scoping the prefix to "CONTROL MEMBER" keeps
    # it truthful in every ensemble state (skipped / written / partially
    # failed / all failed) without needing a state-specific prefix per
    # branch; the ensemble-specific truth is carried by ensemble_status,
    # appended to this same final line below.
    if perform_qmapping:
        completion_banner = (
            "PREPROCESSING OF CONTROL MEMBER WEATHER DATA FROM DATA GATWAY DONE. "
            "DOWNSCALING WITH QUANTILE MAPPING DONE FOR THE CONTROL MEMBER."
        )
    else:
        completion_banner = (
            "PREPROCESSING OF CONTROL MEMBER WEATHER DATA FROM DATA GATWAY DONE "
            "BUT NO DOWNSCALING DONE.\n"
            "ERA5-LAND and ECMWF IFS CONTROL MEMBER FORECASTS WRITTEN WITHOUT DOWNSCALING."
        )

    # PREPG-023 F1: append a truthful ensemble-forcing status, covering
    # all three states (skipped / written / failed), to the
    # control-member-scoped banner above -- so a reader of only the
    # final line gets the complete, truthful picture rather than the
    # control-member-only prefix alone. Log at ERROR (not INFO) when any
    # required ensemble HRU failed.
    if not ensembles_required:
        ensemble_status = "Ensemble forcing: SKIPPED (consumption gate closed)."
    elif ensemble_run_had_failures:
        ensemble_status = (
            f"Ensemble forcing: FAILED for {len(ensemble_hrus_failed)}/"
            f"{len(hru_ensemble_forecast)} HRU(s) (written={len(ensemble_hrus_written)})."
        )
    else:
        ensemble_status = (
            f"Ensemble forcing: written for {len(ensemble_hrus_written)}/"
            f"{len(hru_ensemble_forecast)} HRU(s)."
        )

    if ensemble_run_had_failures:
        logger.error("%s %s", completion_banner, ensemble_status)
    else:
        logger.info("%s %s", completion_banner, ensemble_status)

    # PREPG-023 C2: exit non-zero after the loop if any required
    # ensemble HRU failed. The control member's CSVs/API writes and any
    # ensemble HRUs that DID succeed have already been written above --
    # this only affects the module's final exit status.
    if ensemble_run_had_failures:
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
