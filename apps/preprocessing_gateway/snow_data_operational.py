#USAGE: SAPPHIRE_OPDEV_ENV=True python snow_data_operational.py

# --------------------------------------------------------------------
# Import Libraries
# --------------------------------------------------------------------
import os
import sys
import json
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging
from logging.handlers import TimedRotatingFileHandler
import traceback

# Custom Script for Data Gateway
import dg_utils

# Note that the sapphire data gateway client is currently a private repository
# Access to the repository is required to install the package
# Further, access to the data gateway through an API key is required to use the
# client. The API key is stored in a .env file in the root directory of the project.
# The forecast tools can be used without access to the sapphire data gateay but
# the full power of the tools is only available with access to the data gateway.
#pip install git+https://github.com/hydrosolutions/sapphire-dg-client.git
import sapphire_dg_client
from sapphire_dg_client import SapphireDGClient, snow_model

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


def _write_snow_to_api(data: pd.DataFrame, snow_type: str) -> bool:
    """
    Write snow data to SAPPHIRE preprocessing API.

    This function writes only the latest date's data (operational mode behavior).
    The snow_data_operational module fetches operational data which should be
    synced incrementally.

    Args:
        data: DataFrame with snow data. Expected columns:
            - date: date
            - code: station code
            - {snow_type}: value (e.g., SWE, HS, RoF)
            - {snow_type}_1, {snow_type}_2, ... : optional elevation band values
        snow_type: Type of snow data (SWE, HS, RoF)

    Returns:
        True if successful, False otherwise

    Raises:
        SapphireAPIError: If API write fails after retries
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.warning("sapphire-api-client not installed, skipping snow API write")
        return False

    # Check if API writing is enabled (default: enabled)
    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
    if not api_enabled:
        logger.info("SAPPHIRE API writing disabled via SAPPHIRE_API_ENABLED=false")
        return False

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    client = SapphirePreprocessingClient(base_url=api_url)

    # Health check first - fail fast if API unavailable
    if not client.readiness_check():
        raise SapphireAPIError(f"SAPPHIRE API at {api_url} is not ready")

    if data.empty:
        logger.info(f"No snow data to write to API ({snow_type})")
        return False

    # Ensure date column is datetime
    data = data.copy()
    data['date'] = pd.to_datetime(data['date'])

    # Operational mode: write only the latest date's data
    latest_date = data['date'].max()
    data_to_write = data[data['date'] == latest_date]
    logger.info(f"Writing {len(data_to_write)} snow records for date {latest_date}")

    if data_to_write.empty:
        logger.info(f"No snow data to write ({snow_type})")
        return False

    # Identify elevation band columns (e.g., SWE_1, SWE_2, ...)
    value_columns = {}
    main_value_col = snow_type if snow_type in data_to_write.columns else None
    for col in data_to_write.columns:
        if col.startswith(f"{snow_type}_") and col != snow_type:
            # Extract elevation band number
            try:
                band_num = int(col.split("_")[-1])
                value_columns[band_num] = col
            except ValueError:
                pass

    # Prepare records for API
    records = []
    for _, row in data_to_write.iterrows():
        # Parse date
        date_obj = pd.to_datetime(row['date']) if pd.notna(row.get('date')) else None
        if date_obj is None:
            logger.warning(f"Skipping snow row with missing date: {row.to_dict()}")
            continue

        record = {
            "snow_type": snow_type.upper(),  # API expects uppercase
            "code": str(row['code']),
            "date": date_obj.strftime('%Y-%m-%d'),
            "value": float(row[main_value_col]) if main_value_col and pd.notna(row.get(main_value_col)) else None,
            "norm": float(row['norm']) if 'norm' in row and pd.notna(row.get('norm')) else None,
        }

        # Add elevation band values (value1-value14)
        for band_num, col_name in value_columns.items():
            if band_num <= 14:  # API supports value1-value14
                record[f"value{band_num}"] = float(row[col_name]) if pd.notna(row.get(col_name)) else None

        records.append(record)

    # Write to API
    if records:
        count = client.write_snow(records)
        logger.info(f"Successfully wrote {count} snow records to SAPPHIRE API ({snow_type})")
        print(f"SAPPHIRE API: Successfully wrote {count} snow records ({snow_type})")
        return True
    else:
        logger.info(f"No snow records to write to API ({snow_type})")
        return False


def _check_snow_consistency(csv_data: pd.DataFrame, snow_type: str) -> bool:
    """
    Check consistency between CSV data and API data for snow.

    Reads back from the API and compares with the CSV data that was written.
    Enabled via SAPPHIRE_CONSISTENCY_CHECK=true environment variable.

    Args:
        csv_data: DataFrame that was written to CSV
        snow_type: Type of snow data (SWE, HS, RoF)

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

    logger.info(f"SAPPHIRE_CONSISTENCY_CHECK: Comparing API and CSV data for snow ({snow_type})...")

    # Track whether any inconsistencies are found
    is_consistent = True

    # Get the date range from CSV data
    csv_data = csv_data.copy()
    csv_data['date'] = pd.to_datetime(csv_data['date'])

    # For operational mode, we only wrote the latest date
    latest_date = csv_data['date'].max()
    csv_latest = csv_data[csv_data['date'] == latest_date].copy()

    # Get unique codes
    codes = csv_latest['code'].unique()

    # Read from API for each code
    all_api_data = []
    for code in codes:
        try:
            api_df = client.read_snow(
                snow_type=snow_type.upper(),
                code=str(code),
                start_date=latest_date.strftime('%Y-%m-%d'),
                end_date=latest_date.strftime('%Y-%m-%d'),
                limit=1000
            )
            if not api_df.empty:
                all_api_data.append(api_df)
        except Exception as e:
            logger.error(f"Error reading snow data from API for code {code}: {e}")
            return False

    if not all_api_data:
        logger.warning(f"SAPPHIRE_CONSISTENCY_CHECK: No data returned from API for {snow_type}")
        return False

    api_data = pd.concat(all_api_data, ignore_index=True)
    api_data['date'] = pd.to_datetime(api_data['date'])
    api_data['code'] = api_data['code'].astype(str)
    csv_latest['code'] = csv_latest['code'].astype(str)

    # Compare row counts
    if len(api_data) != len(csv_latest):
        logger.warning(
            f"SAPPHIRE_CONSISTENCY_CHECK: Row count mismatch for {snow_type} - "
            f"API: {len(api_data)}, CSV: {len(csv_latest)}"
        )
        is_consistent = False

    # Compare values
    # Merge on code and date
    merged = csv_latest.merge(
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
            f"SAPPHIRE_CONSISTENCY_CHECK: {len(only_csv)} rows in CSV but not in API for {snow_type}"
        )
        is_consistent = False

    if len(only_api) > 0:
        logger.warning(
            f"SAPPHIRE_CONSISTENCY_CHECK: {len(only_api)} rows in API but not in CSV for {snow_type}"
        )
        is_consistent = False

    # Compare value column
    both = merged[merged['_merge'] == 'both']
    if len(both) > 0:
        # The CSV has the value in a column named by snow_type (e.g., 'SWE')
        # The API returns it as 'value'
        csv_val_col = snow_type if snow_type in csv_latest.columns else None

        if csv_val_col and 'value' in api_data.columns:
            csv_values = both[f'{csv_val_col}_csv'] if f'{csv_val_col}_csv' in both.columns else both.get(csv_val_col)
            api_values = both['value_api'] if 'value_api' in both.columns else both.get('value')

            if csv_values is not None and api_values is not None:
                # Convert to numeric
                csv_values = pd.to_numeric(csv_values, errors='coerce')
                api_values = pd.to_numeric(api_values, errors='coerce')

                # Check for value mismatches (tolerance 0.01 for rounding)
                diff = (csv_values - api_values).abs()
                mismatches = diff[diff > 0.01]

                if len(mismatches) > 0:
                    logger.warning(
                        f"SAPPHIRE_CONSISTENCY_CHECK: {len(mismatches)} value mismatches for {snow_type} "
                        f"(max diff: {diff.max():.4f})"
                    )
                    is_consistent = False

    if is_consistent:
        logger.info(f"SAPPHIRE_CONSISTENCY_CHECK: PASSED for {snow_type} - API matches CSV")
    else:
        logger.error(f"SAPPHIRE_CONSISTENCY_CHECK: FAILED for {snow_type} - inconsistencies found")

    return is_consistent


def get_snow_data_operational(client, 
                  hru, 
                  variable,
                  date, 
                  dg_path, 
                  save_path):
    """
    Get the snow data for a given HRU and variable from the Sapphire Data Gateway.
    The snow data is then transformed in a file with following format:
    |date|variable|code|name|
    Variables can be SWE, HS, RoF
    The code is the unique basin identifier, and the name can be code_numhru,
    for shapefiles with different elevation bands.

    This file will then be saved in the path specified by save_path/variable/HRU_variable.csv
    There can / will already exist a file with the same name from previous forecast and past historic values
    Therefore, we need to check if the file already exist, if it exists we append the new data to the file
    and remove duplicates and we keep the latest value.
    """

    file_path = os.path.join(save_path, variable, f"{hru}_{variable}.csv")
    # Check if the file already exists
    if os.path.exists(file_path):
        try:
            old_dataframe = pd.read_csv(file_path)
            old_dataframe['date'] = pd.to_datetime(old_dataframe['date'])
            old_dataframe = old_dataframe.sort_values(by=['date', 'code'])
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            return False
    else:
        old_dataframe = pd.DataFrame()
    try:
        outpath = client.get_operational(
                hru_code=hru, date=date, parameter=variable, 
                directory = dg_path)
    except Exception as e:
        logger.error(f"Error getting snow data for {hru} {variable}: {e}")
        return False
    
    # Read the data from the file
    try:
        df = pd.read_csv(outpath)
    except Exception as e:
        logger.error(f"Error reading file {outpath}: {e}")
        return False
    
    # Transform the data
    df_transformed = dg_utils.transform_snow_data(df, variable)

    logger.debug(f"Head of transformed data:\n{df_transformed.head()}")

    # Sort the data by date and code
    df_transformed = df_transformed.sort_values(by=['date', 'code'])

    df_combined = pd.concat([old_dataframe, df_transformed], ignore_index=True)
    # Remove duplicates and keep the last value
    df_combined = df_combined.drop_duplicates(subset=['date', 'code'], keep='last')

    # Sort the data by date and code
    df_combined = df_combined.sort_values(by=['date', 'code'])

    #round data to 2 decimal places
    df_combined = df_combined.round(2)

    # Save the data to the file
    try:
        df_combined.to_csv(file_path, index=False)
    except Exception as e:
        logger.error(f"Error saving file {file_path}: {e}")
        return False

    # Write to SAPPHIRE API (if enabled)
    try:
        _write_snow_to_api(df_combined, variable)
        # Run consistency check if enabled
        _check_snow_consistency(df_combined, variable)
    except SapphireAPIError as e:
        logger.error(f"Error writing snow data to API: {e}")
        # Continue - CSV write succeeded, API failure is not fatal

    return True


def main():
    #--------------------------------------------------------------------
    # SETUP ENVIRONMENT
    #--------------------------------------------------------------------

    # Specify the path to the .env file
    # Loads the environment variables from the .env file
    sl.load_environment()

    # Test if an API key is available and exit the program if it isn't
    if not os.getenv('ieasyhydroforecast_API_KEY_GATEAWAY'):
        logger.warning("No API key for the data gateway found. Exiting program.\nMachine learning or conceptual models will not be run.")
        sys.exit(1)
    else:
        API_KEY = os.getenv('ieasyhydroforecast_API_KEY_GATEAWAY')

    API_HOST = os.getenv('SAPPHIRE_DG_HOST')

    intermediate_data_path = os.getenv('ieasyforecast_intermediate_data_path')
    #output_path for the data from the data gateaway
    OUTPUT_PATH_DG = os.path.join(
        intermediate_data_path,
        os.getenv('ieasyhydroforecast_OUTPUT_PATH_DG'))
    # Test if the output path exists and create it if it doesn't
    if not os.path.exists(OUTPUT_PATH_DG):
        os.makedirs(OUTPUT_PATH_DG, exist_ok=True)

    snow_data_path = os.getenv('ieasyhydroforecast_OUTPUT_PATH_SNOW')
    #OUTPUT_PATH for snow data
    OUTPUT_PATH_SNOW = os.path.join(
        intermediate_data_path,
        snow_data_path)
    # Test if the output path exists and create it if it doesn't
    if not os.path.exists(OUTPUT_PATH_SNOW):
        os.makedirs(OUTPUT_PATH_SNOW, exist_ok=True)
    
    # Get the HRUs for the snow data
    SNOW_HRUS = os.getenv('ieasyhydroforecast_HRU_SNOW_DATA')
    SNOW_HRUS = [str(x) for x in SNOW_HRUS.split(',')]

    # Get the snow vars
    SNOW_VARS = os.getenv('ieasyhydroforecast_SNOW_VARS')
    SNOW_VARS = [str(x) for x in SNOW_VARS.split(',')]

    logger.debug(f"Extracting snow data for HRUs: {SNOW_HRUS}")
    logger.debug(f"Extracting snow data for variables: {SNOW_VARS}")

    logger.debug(f"Output path of DG data: {OUTPUT_PATH_DG}")
    logger.debug(f"Output path of snow data: {OUTPUT_PATH_SNOW}")
    #iterate through the snow vars and check if a directory exists
    # if not, create it
    for snow_var in SNOW_VARS:
        snow_var_dir = os.path.join(OUTPUT_PATH_SNOW, snow_var)
        if not os.path.exists(snow_var_dir):
            os.makedirs(snow_var_dir, exist_ok=True)
            logger.debug(f"Creating directory for snow var: {snow_var_dir}")
        else:
            logger.debug(f"Directory for snow var already exists: {snow_var_dir}")

    client = snow_model.SapphireSnowModelClient(
        api_key=API_KEY,
        host = API_HOST)
    
    # today - 365 days
    today = datetime.today().strftime('%Y-%m-%d')
    start_date = datetime.today() - timedelta(days=365)
    start_date = start_date.strftime('%Y-%m-%d')
    logger.debug(f"Date for snow data from: {start_date}")

    # Iterate through the HRUs and get the snow data
    for hru in SNOW_HRUS:
        for snow_var in SNOW_VARS:
            logger.debug(f"Getting snow data for HRU: {hru} and variable: {snow_var}")
            # Get the snow data
            success = get_snow_data_operational(
                client=client,
                hru= hru, 
                variable= snow_var,
                date= start_date,
                dg_path= OUTPUT_PATH_DG,
                save_path= OUTPUT_PATH_SNOW)
            if not success:
                logger.error(f"Error getting snow data for HRU: {hru} and variable: {snow_var}")

    logger.debug('Finished getting snow data')

    
if __name__ == "__main__":
    # Run the main function
    main()