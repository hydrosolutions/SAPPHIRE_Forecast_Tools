# USAGE: SAPPHIRE_OPDEV_ENV=True python snow_data_operational.py

# --------------------------------------------------------------------
# Import Libraries
# --------------------------------------------------------------------
import logging
import os
import sys
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler

# Custom Script for Data Gateway
import dg_utils
import pandas as pd

# Note that the sapphire data gateway client is currently a private repository
# Access to the repository is required to install the package
# Further, access to the data gateway through an API key is required to use the
# client. The API key is stored in a .env file in the root directory of the project.
# The forecast tools can be used without access to the sapphire data gateay but
# the full power of the tools is only available with access to the data gateway.
# pip install git+https://github.com/hydrosolutions/sapphire-dg-client.git
from sapphire_dg_client import snow_model

# SAPPHIRE API client for writing to the SAPPHIRE preprocessing API
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


def _check_snow_consistency(csv_data: pd.DataFrame, snow_type: str, hru_code: str) -> bool:
    """
    Check consistency between CSV data and API data for snow.

    Reads back from the API and compares with the CSV data that was
    written. Enabled via SAPPHIRE_CONSISTENCY_CHECK=true environment
    variable.

    Args:
        csv_data: DataFrame that was written to CSV
        snow_type: Type of snow data (SWE, HS, RoF)
        hru_code: HRU code for logging context

    Returns:
        True if consistent (or check disabled), False if
        inconsistencies found
    """
    tag = f"CONSISTENCY_CHECK [{snow_type}, HRU {hru_code}]"

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

    # The write function filters to yesterday onward (including forecast);
    # use the same window
    today = pd.Timestamp.today().normalize()
    yesterday = today - pd.Timedelta(days=1)
    csv_recent = csv_data[csv_data["date"] >= yesterday].copy()

    if csv_recent.empty:
        logger.warning(
            "%s: No CSV rows for %s to %s, nothing to verify. CSV date range: %s to %s",
            tag,
            yesterday.date(),
            today.date(),
            csv_data["date"].min().date(),
            csv_data["date"].max().date(),
        )
        return True

    codes = csv_recent["code"].unique()
    csv_val_col = snow_type if snow_type in csv_recent.columns else None
    logger.info(
        "%s: Verifying API data for dates %s to %s, codes=%s, api_url=%s",
        tag,
        yesterday.date(),
        today.date(),
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
            end_date = csv_recent["date"].max()
            api_df = client.read_snow(
                snow_type=snow_type.upper(),
                code=str(code),
                start_date=yesterday.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                limit=1000,
            )
            if api_df.empty:
                logger.warning(
                    "%s: API returned 0 rows for code=%s, date=%s", tag, code, today.date()
                )
            else:
                # Log actual values for cross-checking
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
        # data exists for this snow_type
        try:
            any_data = client.read_snow(snow_type=snow_type.upper(), limit=5)
            if any_data.empty:
                logger.warning(
                    "%s: FAILED - No data returned from API. "
                    "Diagnostic: API has NO %s data at all. "
                    "The write may have succeeded (count was returned) "
                    "but data may not be persisted. "
                    "Check the preprocessing API logs.",
                    tag,
                    snow_type.upper(),
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
                    today.date(),
                    snow_type.upper(),
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
        csv_val_col = snow_type if snow_type in csv_recent.columns else None
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
        print(f"CONSISTENCY_CHECK: PASSED ({snow_type}, HRU {hru_code})")
    else:
        logger.error("%s: FAILED - inconsistencies found", tag)
        print(f"CONSISTENCY_CHECK: FAILED ({snow_type}, HRU {hru_code}) - see log for details")

    return is_consistent


def get_snow_data_operational(client, hru, variable, date, dg_path, save_path):
    """
    Get snow data for a given HRU and variable from the Sapphire Data
    Gateway.

    The snow data is transformed into a file with format:
    |date|variable|code|name|

    Variables can be SWE, HS, RoF. The code is the unique basin
    identifier, and the name can be code_numhru for shapefiles with
    different elevation bands.

    This file is saved to save_path/variable/HRU_variable.csv. If the
    file already exists, new data is appended and duplicates are removed
    (keeping the latest value).
    """
    logger.info("Processing snow data: HRU %s, variable %s", hru, variable)
    print(f"Processing snow: HRU {hru}, {variable}")

    file_path = os.path.join(save_path, variable, f"{hru}_{variable}.csv")
    # Check if the file already exists
    if os.path.exists(file_path):
        try:
            old_dataframe = pd.read_csv(file_path)
            old_dataframe["date"] = pd.to_datetime(old_dataframe["date"])
            old_dataframe["code"] = old_dataframe["code"].astype(str)
            old_dataframe = old_dataframe.sort_values(by=["date", "code"])
            logger.info(
                "  Existing CSV: %d rows, dates %s to %s",
                len(old_dataframe),
                old_dataframe["date"].min().date(),
                old_dataframe["date"].max().date(),
            )
        except Exception as e:
            logger.error("Error reading file %s: %s", file_path, e)
            return False
    else:
        old_dataframe = pd.DataFrame()
        logger.info("  No existing CSV, starting fresh")

    # Fetch from Data Gateway
    try:
        outpath = client.get_operational(
            hru_code=hru, date=date, parameter=variable, directory=dg_path
        )
    except Exception as e:
        logger.error(
            "Error getting snow data from Data Gateway for HRU %s, %s: %s",
            hru,
            variable,
            dg_utils.redact_api_key(str(e)),
        )
        return False

    # Read the downloaded data
    try:
        df = pd.read_csv(outpath)
    except Exception as e:
        logger.error("Error reading downloaded file %s: %s", outpath, e)
        return False

    # Transform the data
    df_transformed = dg_utils.transform_snow_data(df, variable)
    df_transformed["date"] = pd.to_datetime(df_transformed["date"])
    logger.info(
        "  Data Gateway returned %d rows, dates %s to %s",
        len(df_transformed),
        df_transformed["date"].min().date(),
        df_transformed["date"].max().date(),
    )

    logger.debug("Head of transformed data:\n%s", df_transformed.head())

    # Sort the data by date and code
    df_transformed = df_transformed.sort_values(by=["date", "code"])

    df_combined = pd.concat([old_dataframe, df_transformed], ignore_index=True)
    # Remove duplicates and keep the last value
    df_combined = df_combined.drop_duplicates(subset=["date", "code"], keep="last")

    # Sort the data by date and code
    df_combined = df_combined.sort_values(by=["date", "code"])

    # Round data to 2 decimal places
    df_combined = df_combined.round(2)

    logger.info(
        "  Combined CSV: %d rows, dates %s to %s",
        len(df_combined),
        df_combined["date"].min().date(),
        df_combined["date"].max().date(),
    )

    # Save the data to the file
    try:
        df_combined.to_csv(file_path, index=False)
    except Exception as e:
        logger.error("Error saving file %s: %s", file_path, e)
        return False

    # Write to SAPPHIRE API (if enabled)
    try:
        written = dg_utils.write_snow_to_api(df_combined, variable, hru)
        # Run consistency check only if data was actually written
        if written:
            _check_snow_consistency(df_combined, variable, hru)
    except dg_utils.SnowPreservationReadError:
        # PREPG-020: a failed preservation read must abort this run,
        # not be logged and swallowed as a non-fatal API error. This
        # is currently redundant with SnowPreservationReadError not
        # deriving from SapphireAPIError (so the except below would
        # not catch it anyway) — it is here so that guarantee is
        # encoded in our own code, not left resting on a third-party
        # exception hierarchy that could change.
        raise
    except SapphireAPIError as e:
        logger.error("Error writing snow data to API (HRU %s, %s): %s", hru, variable, e)
        # Continue - CSV write succeeded, API failure is not fatal

    return True


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

    API_HOST = os.getenv("SAPPHIRE_DG_HOST")

    intermediate_data_path = os.getenv("ieasyforecast_intermediate_data_path")
    # output_path for the data from the data gateaway
    OUTPUT_PATH_DG = os.path.join(
        intermediate_data_path, os.getenv("ieasyhydroforecast_OUTPUT_PATH_DG")
    )
    # Test if the output path exists and create it if it doesn't
    if not os.path.exists(OUTPUT_PATH_DG):
        os.makedirs(OUTPUT_PATH_DG, exist_ok=True)

    snow_data_path = os.getenv("ieasyhydroforecast_OUTPUT_PATH_SNOW")
    # OUTPUT_PATH for snow data
    OUTPUT_PATH_SNOW = os.path.join(intermediate_data_path, snow_data_path)
    # Test if the output path exists and create it if it doesn't
    if not os.path.exists(OUTPUT_PATH_SNOW):
        os.makedirs(OUTPUT_PATH_SNOW, exist_ok=True)

    # Get the HRUs for the snow data
    SNOW_HRUS = os.getenv("ieasyhydroforecast_HRU_SNOW_DATA")
    SNOW_HRUS = [str(x) for x in SNOW_HRUS.split(",")]

    # Get the snow vars
    SNOW_VARS = os.getenv("ieasyhydroforecast_SNOW_VARS")
    SNOW_VARS = [str(x) for x in SNOW_VARS.split(",")]

    logger.info("Snow data configuration:")
    logger.info("  HRUs: %s", SNOW_HRUS)
    logger.info("  Variables: %s", SNOW_VARS)
    logger.info("  Output path (DG): %s", OUTPUT_PATH_DG)
    logger.info("  Output path (snow): %s", OUTPUT_PATH_SNOW)

    # Ensure variable directories exist
    for snow_var in SNOW_VARS:
        snow_var_dir = os.path.join(OUTPUT_PATH_SNOW, snow_var)
        if not os.path.exists(snow_var_dir):
            os.makedirs(snow_var_dir, exist_ok=True)
            logger.info("  Created directory: %s", snow_var_dir)

    client = snow_model.SapphireSnowModelClient(api_key=API_KEY, host=API_HOST)

    today = datetime.today().strftime("%Y-%m-%d")
    start_date = datetime.today() - timedelta(days=365)
    start_date = start_date.strftime("%Y-%m-%d")
    logger.info("Date range: %s to %s", start_date, today)

    total = len(SNOW_HRUS) * len(SNOW_VARS)
    count = 0
    # Iterate through the HRUs and get the snow data
    for hru in SNOW_HRUS:
        for snow_var in SNOW_VARS:
            count += 1
            logger.info("--- [%d/%d] HRU %s, %s ---", count, total, hru, snow_var)
            success = get_snow_data_operational(
                client=client,
                hru=hru,
                variable=snow_var,
                date=start_date,
                dg_path=OUTPUT_PATH_DG,
                save_path=OUTPUT_PATH_SNOW,
            )
            if not success:
                logger.error("Failed to get snow data for HRU %s, %s", hru, snow_var)

    logger.info("Snow data processing complete (%d tasks)", total)


if __name__ == "__main__":
    # Run the main function
    main()
