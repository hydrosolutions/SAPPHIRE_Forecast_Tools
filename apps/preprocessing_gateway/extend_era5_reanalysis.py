# Title: extend_era5_reanalysis
# Author: sandro hunziker
# Description: Reads The Operational Data and the ERA5 Reanalysis Data and
#   appends the last 6 month of operational data to the ERA5 Reanalysis Data
#
# In operational mode, this file runs after Quantile_Mapping_OP.py
#
# --------------------------------------------------------------------
# USAGE
# SAPPHIRE_OPDEV_ENV=True  python extend_era5_reanalysis.py
# --------------------------------------------------------------------



# Import necessary libraries
import os
import sys
import json
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging
from logging.handlers import TimedRotatingFileHandler


# Local libraries
# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')
#print(script_dir)
#print(forecast_dir)

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the setup_library module from the iEasyHydroForecast package
import setup_library as sl

# SAPPHIRE API client for writing to the SAPPHIRE preprocessing API
try:
    from sapphire_api_client import (
        SapphirePreprocessingClient,
        SapphireAPIError
    )
    SAPPHIRE_API_AVAILABLE = True
except ImportError:
    SAPPHIRE_API_AVAILABLE = False
    SapphirePreprocessingClient = None
    SapphireAPIError = Exception


# Set up logging
# Configure the logging level and formatter
logging.basicConfig(level=logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Create the logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Create a file handler to write logs to a file
# A new log file is created every <interval> day at <when>. It is kept for <backupCount> days.
file_handler = TimedRotatingFileHandler('logs/log', when='midnight', interval=1, backupCount=30)
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

def is_leap_year(year):
    if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
        return True
    else:
        return False


def _write_meteo_to_api(data: pd.DataFrame, meteo_type: str) -> bool:
    """
    Write dashboard meteorological data to SAPPHIRE preprocessing API.

    Writes all data passed to it (dashboard records with norm + value).
    The caller determines what data to include.

    Args:
        data: DataFrame with meteo data. Expected columns:
            - date: date
            - code: station code
            - T or P: temperature or precipitation value
            - T_norm or P_norm: optional norm value
            - dayofyear: optional day of year
        meteo_type: Type of meteo data (T for temperature, P for precipitation)

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
        logger.warning(
            f"SAPPHIRE API at {api_url} is not ready, "
            "skipping meteo write"
        )
        return False

    if data.empty:
        logger.info(f"No meteo data to write to API ({meteo_type})")
        return False

    # Ensure date column is datetime
    data = data.copy()
    data['date'] = pd.to_datetime(data['date'])

    # Write all data passed (caller determines what to include)
    data_to_write = data
    logger.info(f"Writing {len(data_to_write)} meteo records ({meteo_type})")

    # Determine column names for value and norm
    value_col = meteo_type  # 'T' or 'P'
    norm_col = f"{meteo_type}_norm"  # 'T_norm' or 'P_norm'

    # Prepare records for API
    records = []
    for _, row in data_to_write.iterrows():
        # Parse date
        date_obj = pd.to_datetime(row['date']) if pd.notna(row.get('date')) else None
        if date_obj is None:
            logger.warning(f"Skipping meteo row with missing date: {row.to_dict()}")
            continue

        record = {
            "meteo_type": meteo_type.upper(),  # API expects uppercase
            "code": str(row['code']),
            "date": date_obj.strftime('%Y-%m-%d'),
            "value": round(float(row[value_col]), 3) if value_col in row and pd.notna(row.get(value_col)) else None,
            "norm": round(float(row[norm_col]), 3) if norm_col in row and pd.notna(row.get(norm_col)) else None,
            "day_of_year": int(row['dayofyear']) if 'dayofyear' in row and pd.notna(row.get('dayofyear')) else date_obj.dayofyear,
        }
        records.append(record)

    # Write to API
    if records:
        count = client.write_meteo(records)
        logger.info(f"Successfully wrote {count} meteo records to SAPPHIRE API ({meteo_type})")
        print(f"SAPPHIRE API: Successfully wrote {count} meteo records ({meteo_type})")
        return True
    else:
        logger.info(f"No meteo records to write to API ({meteo_type})")
        return False


def _write_reanalysis_to_api(
    data: pd.DataFrame, meteo_type: str, mode: str | None = None,
) -> bool:
    """Write raw reanalysis data to SAPPHIRE preprocessing API.

    Unlike ``_write_meteo_to_api`` (which writes dashboard records with
    norms), this writes the raw extended reanalysis time series.

    Supports sync modes:
    - operational (default): no-op (reanalysis is not written daily)
    - maintenance: write last 365 days of reanalysis
    - initial: write all reanalysis data

    Args:
        data: Extended reanalysis DataFrame with columns
            [date, code, <meteo_type>].
        meteo_type: 'T' or 'P'.
        mode: Sync mode override. If None, reads SAPPHIRE_SYNC_MODE
            env var, defaulting to 'operational'.

    Returns:
        True if successful, False otherwise.
    """
    # Resolve sync mode: parameter > env var > default
    if mode is not None:
        sync_mode = mode.lower()
    else:
        sync_mode = os.getenv("SAPPHIRE_SYNC_MODE", "operational").lower()

    if sync_mode == "operational":
        logger.debug(
            "Reanalysis API write skipped in operational mode (%s)",
            meteo_type,
        )
        return False

    if not SAPPHIRE_API_AVAILABLE:
        logger.warning(
            "sapphire-api-client not installed, skipping reanalysis "
            "API write"
        )
        return False

    api_enabled = os.getenv(
        "SAPPHIRE_API_ENABLED", "true"
    ).lower() == "true"
    if not api_enabled:
        logger.info(
            "SAPPHIRE API writing disabled via SAPPHIRE_API_ENABLED=false"
        )
        return False

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    client = SapphirePreprocessingClient(base_url=api_url)

    if not client.readiness_check():
        logger.warning(
            "SAPPHIRE API at %s is not ready, skipping reanalysis write",
            api_url,
        )
        return False

    if data.empty:
        logger.info(
            "No reanalysis data to write to API (%s)", meteo_type
        )
        return False

    data = data.copy()
    data['date'] = pd.to_datetime(data['date'])

    # Filter by sync mode
    if sync_mode == "maintenance":
        cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=365)
        data_to_write = data[data['date'] >= cutoff]
    elif sync_mode == "initial":
        data_to_write = data
    else:
        logger.warning(
            "Unknown sync mode '%s', defaulting to operational "
            "(no reanalysis write)", sync_mode,
        )
        return False

    if data_to_write.empty:
        logger.info(
            "No reanalysis data to write after %s filtering (%s)",
            sync_mode, meteo_type,
        )
        return False

    logger.info(
        "%s mode: writing %d reanalysis records (%s)",
        sync_mode, len(data_to_write), meteo_type,
    )

    value_col = meteo_type  # 'T' or 'P'
    records = []
    for _, row in data_to_write.iterrows():
        date_obj = pd.to_datetime(row['date'])
        if pd.isna(date_obj):
            continue
        records.append({
            "meteo_type": meteo_type.upper(),
            "code": str(row['code']),
            "date": date_obj.strftime('%Y-%m-%d'),
            "value": (
                round(float(row[value_col]), 3)
                if value_col in row and pd.notna(row.get(value_col))
                else None
            ),
            "norm": None,
            "day_of_year": date_obj.dayofyear,
        })

    if not records:
        logger.info(
            "No reanalysis records to write to API (%s)", meteo_type
        )
        return False

    count = client.write_meteo(records)
    logger.info(
        "Successfully wrote %d reanalysis records to SAPPHIRE API "
        "(%s, %s mode)", count, meteo_type, sync_mode,
    )
    return True


def _check_meteo_consistency(csv_data: pd.DataFrame, meteo_type: str) -> bool:
    """
    Check consistency between CSV data and API data for meteo.

    Reads back from the API and compares with the CSV data that was written.
    Enabled via SAPPHIRE_CONSISTENCY_CHECK=true environment variable.

    Args:
        csv_data: DataFrame that was written to CSV
        meteo_type: Type of meteo data (T or P)

    Returns:
        True if consistent (or check disabled), False if inconsistencies found
    """
    # Check if consistency check is enabled
    consistency_check = os.getenv("SAPPHIRE_CONSISTENCY_CHECK", "false").lower() == "true"
    if not consistency_check:
        return True

    if not SAPPHIRE_API_AVAILABLE:
        logger.warning("sapphire-api-client not installed, skipping consistency check")
        return True

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    client = SapphirePreprocessingClient(base_url=api_url)

    logger.info(f"SAPPHIRE_CONSISTENCY_CHECK: Comparing API and CSV data for meteo ({meteo_type})...")

    # Track whether any inconsistencies are found
    is_consistent = True

    # Prepare CSV data
    csv_data = csv_data.copy()
    csv_data['date'] = pd.to_datetime(csv_data['date'])

    # Get unique codes and date range
    codes = csv_data['code'].unique()
    min_date = csv_data['date'].min()
    max_date = csv_data['date'].max()

    # Read from API for each code
    all_api_data = []
    for code in codes:
        try:
            api_df = client.read_meteo(
                meteo_type=meteo_type.upper(),
                code=str(code),
                start_date=min_date.strftime('%Y-%m-%d'),
                end_date=max_date.strftime('%Y-%m-%d'),
                limit=10000
            )
            if not api_df.empty:
                all_api_data.append(api_df)
        except Exception as e:
            logger.error(f"Error reading meteo data from API for code {code}: {e}")
            return False

    if not all_api_data:
        logger.warning(f"SAPPHIRE_CONSISTENCY_CHECK: No data returned from API for {meteo_type}")
        return False

    api_data = pd.concat(all_api_data, ignore_index=True)
    api_data['date'] = pd.to_datetime(api_data['date'])
    api_data['code'] = api_data['code'].astype(str)
    csv_data['code'] = csv_data['code'].astype(str)

    # Compare row counts
    if len(api_data) != len(csv_data):
        logger.warning(
            f"SAPPHIRE_CONSISTENCY_CHECK: Row count mismatch for {meteo_type} - "
            f"API: {len(api_data)}, CSV: {len(csv_data)}"
        )
        is_consistent = False

    # Compare values
    # The CSV has the value in a column named by meteo_type (e.g., 'T' or 'P')
    # The API returns it as 'value'
    value_col = meteo_type
    norm_col = f"{meteo_type}_norm"

    # Merge on code and date
    merged = csv_data.merge(
        api_data,
        on=['code', 'date'],
        how='outer',
        suffixes=('_csv', '_api'),
        indicator=True
    )

    # Check for rows only in one source
    only_csv = merged[merged['_merge'] == 'left_only']
    only_api = merged[merged['_merge'] == 'right_only']

    if len(only_csv) > 0:
        logger.warning(
            f"SAPPHIRE_CONSISTENCY_CHECK: {len(only_csv)} rows in CSV but not in API for {meteo_type}"
        )
        is_consistent = False

    if len(only_api) > 0:
        logger.warning(
            f"SAPPHIRE_CONSISTENCY_CHECK: {len(only_api)} rows in API but not in CSV for {meteo_type}"
        )
        is_consistent = False

    # Compare values for rows in both
    both = merged[merged['_merge'] == 'both']
    if len(both) > 0:
        # Compare value column
        if value_col in csv_data.columns and 'value' in api_data.columns:
            csv_val_col = f'{value_col}_csv' if f'{value_col}_csv' in both.columns else value_col
            api_val_col = 'value_api' if 'value_api' in both.columns else 'value'

            csv_values = pd.to_numeric(both[csv_val_col], errors='coerce')
            api_values = pd.to_numeric(both[api_val_col], errors='coerce')

            # Check for value mismatches (tolerance 0.01 for rounding)
            diff = (csv_values - api_values).abs()
            mismatches = diff[diff > 0.01]

            if len(mismatches) > 0:
                logger.warning(
                    f"SAPPHIRE_CONSISTENCY_CHECK: {len(mismatches)} value mismatches for {meteo_type} "
                    f"(max diff: {diff.max():.4f})"
                )
                is_consistent = False

        # Compare norm column
        if norm_col in csv_data.columns and 'norm' in api_data.columns:
            csv_norm_col = f'{norm_col}_csv' if f'{norm_col}_csv' in both.columns else norm_col
            api_norm_col = 'norm_api' if 'norm_api' in both.columns else 'norm'

            csv_norms = pd.to_numeric(both[csv_norm_col], errors='coerce')
            api_norms = pd.to_numeric(both[api_norm_col], errors='coerce')

            diff = (csv_norms - api_norms).abs()
            mismatches = diff[diff > 0.01]

            if len(mismatches) > 0:
                logger.warning(
                    f"SAPPHIRE_CONSISTENCY_CHECK: {len(mismatches)} norm mismatches for {meteo_type} "
                    f"(max diff: {diff.max():.4f})"
                )
                is_consistent = False

    if is_consistent:
        logger.info(f"SAPPHIRE_CONSISTENCY_CHECK: PASSED for {meteo_type} - API matches CSV")
    else:
        logger.error(f"SAPPHIRE_CONSISTENCY_CHECK: FAILED for {meteo_type} - inconsistencies found")

    return is_consistent


def select_stable_operational_data(
    operational_data: pd.DataFrame, stability_days: int = 195
) -> pd.DataFrame:
    """Filter operational data to the stable period.

    Returns rows with date strictly before (max_date - stability_days).

    Args:
        operational_data: DataFrame with a 'date' column (datetime).
        stability_days: Number of days before max date considered
            unstable. Default 195 (approx 6 months + 15 day forecast).

    Returns:
        Filtered DataFrame containing only stable rows.
    """
    max_date = operational_data['date'].max()
    threshold = max_date - timedelta(days=stability_days)
    return operational_data[operational_data['date'] < threshold].copy()


def extend_reanalysis_with_operational(
    reanalysis: pd.DataFrame, operational_stable: pd.DataFrame
) -> pd.DataFrame:
    """Append stable operational data to reanalysis, dedup, and sort.

    On date+code overlap the operational row wins (keep='last').

    Args:
        reanalysis: ERA5 reanalysis DataFrame with [date, code, ...].
        operational_stable: Stable operational DataFrame (same schema).

    Returns:
        Combined DataFrame sorted by (date, code).
    """
    combined = pd.concat(
        [reanalysis, operational_stable], ignore_index=True
    )
    combined = combined.drop_duplicates(
        subset=['date', 'code'], keep='last'
    )
    combined = combined.sort_values(by=['date', 'code'])
    return combined


def calculate_daily_norm(
    reanalysis: pd.DataFrame,
    operational: pd.DataFrame,
    value_col: str,
    current_year: int,
) -> pd.DataFrame:
    """Calculate daily climatological norm and merge with operational.

    Groups reanalysis by (code, dayofyear) to compute the mean,
    creates dates for the current year, and left-merges operational
    data so the dashboard can show both norm and current-year values.

    Args:
        reanalysis: Extended reanalysis DataFrame with [date, code,
            <value_col>].
        operational: Current-year operational DataFrame with [date,
            code, <value_col>].
        value_col: Name of the value column ('P' or 'T').
        current_year: Year to map dayofyear onto (determines leap
            year handling).

    Returns:
        Dashboard-ready DataFrame with columns:
        [code, <value_col>_norm, date, <value_col>] where the last
        column comes from the operational merge (NaN where no match).
    """
    reanalysis = reanalysis.copy()
    reanalysis['dayofyear'] = reanalysis['date'].dt.dayofyear

    daily_norm = (
        reanalysis
        .groupby(['code', 'dayofyear'])[value_col]
        .mean()
        .round(2)
        .reset_index()
    )

    # Remove day 366 for non-leap years. Use explicit filter instead
    # of iloc[:-1] which breaks with multiple codes or when no leap
    # years exist in the historical data.
    if not is_leap_year(current_year):
        daily_norm = daily_norm[daily_norm['dayofyear'] <= 365]

    daily_norm['date'] = (
        pd.Timestamp(str(current_year))
        + pd.to_timedelta(daily_norm['dayofyear'] - 1, unit='D')
    )

    daily_norm = daily_norm.drop(columns=['dayofyear'])
    daily_norm = daily_norm.rename(
        columns={value_col: f'{value_col}_norm'}
    )

    # Merge current-year operational data
    daily_norm = pd.merge(
        daily_norm, operational, on=['code', 'date'], how='left'
    )

    return daily_norm


def main():
    #--------------------------------------------------------------------
    # SETUP ENVIRONMENT
    #--------------------------------------------------------------------
    logger.info(f'||||||||||||||||||||||||||||||||||||||||')
    logger.info(f'----------------------------------------')
    logger.info(f'extend_era5_reanalysis.py started       ')
    logger.info(f'----------------------------------------')
    # Specify the path to the .env file
    # Loads the environment variables from the .env file
    sl.load_environment()

    #output_path for reanalysis
    OUTPUT_PATH_REANALYSIS = os.path.join(
        os.getenv('ieasyforecast_intermediate_data_path'),
        os.getenv('ieasyhydroforecast_OUTPUT_PATH_REANALYSIS'))
    # Test if the output path exists and create it if it doesn't
    if not os.path.exists(OUTPUT_PATH_REANALYSIS):
        os.makedirs(OUTPUT_PATH_REANALYSIS, exist_ok=True)

    #output_path for control member and ensemble
    OUTPUT_PATH_CM = os.path.join(
        os.getenv('ieasyforecast_intermediate_data_path'),
        os.getenv('ieasyhydroforecast_OUTPUT_PATH_CM'))
    # Test if the output path exists and create it if it doesn't
    if not os.path.exists(OUTPUT_PATH_CM):
        os.makedirs(OUTPUT_PATH_CM, exist_ok=True)

    logger.debug('OUTPUT_PATH_REANALYSIS: %s', OUTPUT_PATH_REANALYSIS)
    logger.debug('OUTPUT_PATH_CM: %s', OUTPUT_PATH_CM)

    #--------------------------------------------------------------------
    # READ IN THE FILES
    #--------------------------------------------------------------------
    CONTROL_MEMBER_HRUS = os.getenv('ieasyhydroforecast_HRU_CONTROL_MEMBER')
    control_member_hrus = [str(x) for x in CONTROL_MEMBER_HRUS.split(',')]


    for c_m in control_member_hrus:

        era5_reanalysis_P_file = os.path.join(OUTPUT_PATH_REANALYSIS, f'{c_m}_P_reanalysis.csv')
        era5_reanalysis_T_file = os.path.join(OUTPUT_PATH_REANALYSIS, f'{c_m}_T_reanalysis.csv')

        # Read in the ERA5 Reanalysis Data
        logger.debug('Reading in the ERA5 Reanalysis Data')
        era5_reanalysis_P = pd.read_csv(era5_reanalysis_P_file)
        era5_reanalysis_T = pd.read_csv(era5_reanalysis_T_file)

        # Read in the Operational Data
        operational_P_file = os.path.join(OUTPUT_PATH_CM, f'{c_m}_P_control_member.csv')
        operational_T_file = os.path.join(OUTPUT_PATH_CM, f'{c_m}_T_control_member.csv')

        logger.debug('Reading in the Operational Data')
        operational_P = pd.read_csv(operational_P_file)
        operational_T = pd.read_csv(operational_T_file)

        #--------------------------------------------------------------------
        operational_P['date'] = pd.to_datetime(operational_P['date'])
        operational_T['date'] = pd.to_datetime(operational_T['date'])

        era5_reanalysis_P['date'] = pd.to_datetime(era5_reanalysis_P['date'])
        era5_reanalysis_T['date'] = pd.to_datetime(era5_reanalysis_T['date'])

        #--------------------------------------------------------------------
        # FILTER TO STABLE OPERATIONAL DATA
        #--------------------------------------------------------------------
        operational_P_stable = select_stable_operational_data(operational_P)
        operational_T_stable = select_stable_operational_data(operational_T)

        #--------------------------------------------------------------------
        # APPEND THE STABLE OPERATIONAL DATA TO THE ERA5 REANALYSIS DATA
        #--------------------------------------------------------------------
        logger.debug('Appending the Operational Data to the ERA5 Reanalysis Data')
        era5_reanalysis_P = extend_reanalysis_with_operational(
            era5_reanalysis_P, operational_P_stable
        )
        era5_reanalysis_T = extend_reanalysis_with_operational(
            era5_reanalysis_T, operational_T_stable
        )

        #--------------------------------------------------------------------
        # CALCULATE DAILY NORM VALUES
        #--------------------------------------------------------------------
        logger.debug('Calculating Daily Norm Values for later display in dashboard')
        current_year = datetime.now().year
        daily_norm_P = calculate_daily_norm(
            era5_reanalysis_P, operational_P, 'P', current_year
        )
        daily_norm_T = calculate_daily_norm(
            era5_reanalysis_T, operational_T, 'T', current_year
        )

        #--------------------------------------------------------------------
        # SAVE THE APPENDED DATA
        #--------------------------------------------------------------------
        logger.debug('Saving the Appended Data')
        era5_reanalysis_P.to_csv(era5_reanalysis_P_file, index=False)
        era5_reanalysis_T.to_csv(era5_reanalysis_T_file, index=False)

        # Append '_dashboard' to the file name
        norm_P_file = era5_reanalysis_P_file.replace('.csv', '_dashboard.csv')
        norm_T_file = era5_reanalysis_T_file.replace('.csv', '_dashboard.csv')

        daily_norm_P.to_csv(norm_P_file, index=False)
        daily_norm_T.to_csv(norm_T_file, index=False)

        #--------------------------------------------------------------------
        # WRITE TO SAPPHIRE API
        #--------------------------------------------------------------------
        # Dashboard data (norms + current-year values) — always written
        try:
            written = _write_meteo_to_api(daily_norm_P, 'P')
            # Run consistency check only if data was actually written
            if written:
                _check_meteo_consistency(daily_norm_P, 'P')
        except SapphireAPIError as e:
            logger.error(f"Error writing precipitation data to API: {e}")
            # Continue - CSV write succeeded, API failure is not fatal

        try:
            written = _write_meteo_to_api(daily_norm_T, 'T')
            # Run consistency check only if data was actually written
            if written:
                _check_meteo_consistency(daily_norm_T, 'T')
        except SapphireAPIError as e:
            logger.error(f"Error writing temperature data to API: {e}")
            # Continue - CSV write succeeded, API failure is not fatal

        # Reanalysis data — only in maintenance/initial mode
        try:
            _write_reanalysis_to_api(era5_reanalysis_P, 'P')
        except SapphireAPIError as e:
            logger.error(
                "Error writing reanalysis P data to API: %s", e
            )

        try:
            _write_reanalysis_to_api(era5_reanalysis_T, 'T')
        except SapphireAPIError as e:
            logger.error(
                "Error writing reanalysis T data to API: %s", e
            )

    #--------------------------------------------------------------------
    # LOGGING
    #--------------------------------------------------------------------
    logger.info('Done')
    logger.info(f'----------------------------------------')

if __name__ == '__main__':
    main()
