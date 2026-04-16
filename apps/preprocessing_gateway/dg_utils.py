# --------------------------------------------------------------------
# Import Libraries
# --------------------------------------------------------------------
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

import numpy as np
import pandas as pd

# SAPPHIRE API client for writing to the SAPPHIRE preprocessing API
try:
    from sapphire_api_client import SapphireAPIError, SapphirePreprocessingClient

    SAPPHIRE_API_AVAILABLE = True
except ImportError:
    SAPPHIRE_API_AVAILABLE = False
    SapphirePreprocessingClient = None
    SapphireAPIError = Exception

# Note that the sapphire data gateway client is currently a private repository
# Access to the repository is required to install the package
# Further, access to the data gateway through an API key is required to use the
# client. The API key is stored in a .env file in the root directory of the project.
# The forecast tools can be used without access to the sapphire data gateay but
# the full power of the tools is only available with access to the data gateway.
# pip install git+https://github.com/hydrosolutions/sapphire-dg-client.git

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
# Quantile Mapping
# --------------------------------------------------------------------
def ptf(x: np.array, a: float, b: float) -> np.array:
    return a * np.power(x, b)


def quantile_mapping_ptf(
    sce_data: np.array, a: float, b: float, wet_days: bool = True, wet_day_threshold: float = 0
) -> np.array:
    """
    Perform quantile mapping for precipitation or temperature data.
    FORMULA: y_fit = a * y_era^b
    Inputs:
        sce_data: numpy array of shape (n,) with the data to be transformed.
        a: float
        b: float
        wet_days: boolean, if True, the transformation is performed only for wet days.
        wet_day_threshold: float, the threshold to define wet days.
    Outputs:
        transformed_sce: numpy array of shape (n,) with the transformed data.
    """
    if wet_days:
        dry_days = sce_data <= wet_day_threshold
        # dry days to zero
        sce_data[dry_days] = 0
        transformed_sce = ptf(sce_data, a, b)

    else:
        transformed_sce = ptf(sce_data, a, b)

    # round to 3 decimals
    transformed_sce = np.round(transformed_sce, 2)

    return transformed_sce


def do_quantile_mapping(
    era5_data: pd.DataFrame, P_param: pd.DataFrame, T_param: pd.DataFrame, ensemble: bool
) -> pd.DataFrame:
    """
    Loop over all the stations and perform the quantile mapping for each station for the control member.
    Inputs:
        era5_data: pandas DataFrame with the ERA5 data.
        P_param: pandas DataFrame with the precipitation parameters.
        T_param: pandas DataFrame with the temperature parameters.
    Outputs:
        P_data: pandas DataFrame with the transformed precipitation data.
        T_data: pandas DataFrame with the transformed temperature data.
    """
    era5_data = era5_data.copy()
    # get the unique codes
    codes = era5_data["code"].unique()
    # iterate over the codes
    for code in codes:
        # get the data for the code
        code_data = era5_data[era5_data["code"] == code]

        # get the parameters for the code
        P_param_code = P_param[P_param["code"] == code]
        T_param_code = T_param[T_param["code"] == code]

        # get the parameters
        a_P = P_param_code["a"].values
        b_P = P_param_code["b"].values
        threshold_P = P_param_code["wet_day"].values
        # logger.debug(f"Code: {code[0]}, a_P: {a_P[0]}, b_P: {b_P[0]}, threshold_P: {threshold_P[0]}")
        # logger.debug(f"Types of a_P: {type(a_P[0])}, b_P: {type(b_P[0])}, threshold_P: {type(threshold_P[0])}")

        a_T = T_param_code["a"].values
        b_T = T_param_code["b"].values

        # transform the data
        code_data.loc[:, "P"] = quantile_mapping_ptf(
            code_data["P"].values, a_P, b_P, wet_days=True, wet_day_threshold=threshold_P
        )

        # for temperature we need to tranform it to Kelvin
        T_data = code_data["T"].values + 273.15
        T_fitted = quantile_mapping_ptf(T_data, a_T, b_T, wet_days=False, wet_day_threshold=0)
        code_data.loc[:, "T"] = T_fitted - 273.15

        era5_data.loc[era5_data["code"] == code, "P"] = code_data["P"]
        era5_data.loc[era5_data["code"] == code, "T"] = code_data["T"]

    if ensemble:
        P_data = era5_data[["date", "P", "code", "ensemble_member"]].copy()
        T_data = era5_data[["date", "T", "code", "ensemble_member"]].copy()
    else:
        P_data = era5_data[["date", "P", "code"]].copy()
        T_data = era5_data[["date", "T", "code"]].copy()

    return P_data, T_data


# --------------------------------------------------------------------
# TRANSFORM DATA FILE
# --------------------------------------------------------------------
def transform_data_file_control_member(data_file: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the data file from the data gateaway in a more handy format.
    Inputs:
        data_file: pd.DataFrame with the data from the data gateaway. columns Code XXXXX is T and columns Code XXXXX.1 is P
    Outputs:
        transformed_data: pd.DataFrame with the transformed data. Columns are 'date', 'P', 'T', 'code'
    """
    extension_mapper = {  # Temperature is without a . extension - so just the code
        ".1": "P",
        ".2": "SD",  # so far we ignore this column
    }

    data_file = data_file.copy()
    # rename the Station column to 'date'
    data_file.rename(columns={"Station": "date"}, inplace=True)

    # than we need to drop the first 7 rows of the era5 data
    data_file = data_file.iloc[7:]

    # now we need to convert the date column to a datetime object
    data_file["date"] = pd.to_datetime(data_file["date"], dayfirst=True)

    # sort by the date
    data_file = data_file.sort_values("date")

    transformed_data_file = pd.DataFrame()

    # unique codes
    codes = data_file.columns[1:]

    # if the ".1" is not in code
    codes = [code for code in codes if (code[-2:] not in extension_mapper and code != "Source")]

    # iterate over the codes
    for code in codes:
        # get the data for the code
        code_data = data_file[["date", code, code + ".1"]].copy()
        # rename the columns
        code_data.rename(columns={code: "T", code + ".1": "P"}, inplace=True)
        # Add the 'code' column
        code_data["code"] = code
        # Convert 'T' and 'P' columns to numeric, coercing errors
        code_data["T"] = pd.to_numeric(code_data["T"], errors="coerce").astype(float)
        code_data["P"] = pd.to_numeric(code_data["P"], errors="coerce").astype(float)
        transformed_data_file = pd.concat([transformed_data_file, code_data], axis=0)

    return transformed_data_file


# --------------------------------------------------------------------
# SNOW MODEL
# --------------------------------------------------------------------
def transform_snow_data(df, var_name):
    df = df.copy()
    # rename the first column to date
    columns = df.columns
    columns = list(columns)
    columns[0] = "date"
    df.columns = columns

    # this is hard coded
    df = df.iloc[4:]

    df["date"] = pd.to_datetime(df["date"], dayfirst=True)

    code_dict = {}

    for col in df.columns:
        if col != "date" and col != "Source":
            # Separate station code from elevation band suffix.
            # Convention: <code>_<band> where band is a small int
            # (1-14). Use rsplit to handle codes that contain
            # underscores (e.g., "KGZ_500_1" → code="KGZ_500",
            # band=1).
            parts = col.rsplit("_", 1)
            if len(parts) == 2:
                try:
                    band = int(parts[1])
                except ValueError:
                    band = None
                if band is not None and 1 <= band <= 14:
                    code = str(parts[0])
                    elevation_band = band
                    new_var_name = f"{var_name}_{elevation_band}"
                else:
                    # Suffix is not a valid elevation band;
                    # treat the whole column name as the code
                    code = str(col)
                    elevation_band = None
                    new_var_name = var_name
            else:
                code = str(col)
                elevation_band = None
                new_var_name = var_name

            dates = df["date"]
            values = df[col].astype(float)
            if code not in code_dict:
                code_dict[code] = {"date": dates, new_var_name: values}
            else:
                code_dict[code][new_var_name] = values

    # If the DG CSV has only elevation band columns (e.g., 15013_3,
    # 15013_6) but no base/mean column (e.g., bare 15013), compute the
    # base variable as the mean across all elevation bands for that code.
    for _code, data in code_dict.items():
        if var_name not in data:
            band_keys = [k for k in data if k.startswith(f"{var_name}_") and k != "date"]
            if band_keys:
                band_df = pd.DataFrame({k: data[k] for k in band_keys})
                data[var_name] = band_df.mean(axis=1)

    new_df = pd.DataFrame()
    for code, data in code_dict.items():
        code_df = pd.DataFrame(data)
        code_df["code"] = code
        new_df = pd.concat([new_df, code_df], ignore_index=True)

    return new_df


def is_leap_year(year: int) -> bool:
    """Check whether a given year is a leap year.

    Args:
        year: Four-digit year.

    Returns:
        True if *year* is a leap year, False otherwise.
    """
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)


def calculate_snow_norms(
    path: str,
    variables: list[str],
    hru_codes: list[str],
) -> pd.DataFrame:
    """Calculate climatological daily snow norms from historical CSVs.

    .. deprecated::
        Use ``calculate_snow_norms_from_api()`` instead. CSV-based
        computation is deprecated as part of the CSV-to-API migration.

    Reads CSV files at ``{path}/{variable}/{hru}_{variable}.csv``,
    groups by (code, dayofyear), and computes the mean value for each
    day of the year.

    Args:
        path: Root directory containing per-variable subdirectories.
        variables: Snow variable names (e.g., ``["SWE", "HS", "RoF"]``).
        hru_codes: HRU codes (e.g., ``["15013", "KGZ_500"]``).

    Returns:
        DataFrame with columns ``[snow_type, code, dayofyear, norm]``.
        Returns an empty DataFrame with those columns if no data is
        found.
    """
    result_frames = []

    for variable in variables:
        for hru in hru_codes:
            csv_path = os.path.join(path, variable, f"{hru}_{variable}.csv")
            if not os.path.exists(csv_path):
                logger.warning("Snow CSV not found, skipping: %s", csv_path)
                continue

            try:
                df = pd.read_csv(csv_path)
            except Exception as e:
                logger.error("Error reading snow CSV %s: %s", csv_path, e)
                continue

            if df.empty:
                logger.info("Empty CSV, skipping: %s", csv_path)
                continue

            if variable not in df.columns:
                logger.warning(
                    "Column '%s' not found in %s, skipping",
                    variable,
                    csv_path,
                )
                continue

            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.dropna(subset=["date"])

            if df.empty:
                continue

            df["dayofyear"] = df["date"].dt.dayofyear

            codes = df["code"].unique()
            for code in codes:
                code_df = df[df["code"] == code]
                norms = code_df.groupby("dayofyear")[variable].mean().reset_index()
                norms.columns = ["dayofyear", "norm"]
                norms["snow_type"] = variable
                norms["code"] = str(code)
                result_frames.append(norms[["snow_type", "code", "dayofyear", "norm"]])

    if result_frames:
        return pd.concat(result_frames, ignore_index=True)

    return pd.DataFrame(columns=["snow_type", "code", "dayofyear", "norm"])


def calculate_snow_norms_from_api(
    client,
    variables: list[str],
) -> pd.DataFrame:
    """Calculate climatological daily snow norms from API data.

    Reads all historical snow data from the preprocessing API for each
    variable (no code filter — discovers station codes from the response).
    Groups by ``(code, dayofyear)`` and computes the mean of the
    ``value`` column across all years.

    Args:
        client: SapphirePreprocessingClient instance.
        variables: Snow variable names (e.g., ``["SWE", "HS", "RoF"]``).

    Returns:
        DataFrame with columns ``[snow_type, code, dayofyear, norm]``.
        Returns an empty DataFrame with those columns if no data is
        found.
    """
    result_frames = []
    batch_size = 10000

    for variable in variables:
        # Paginate through all historical data for this variable
        pages = []
        skip = 0
        try:
            while True:
                page = client.read_snow(
                    snow_type=variable.upper(),
                    skip=skip,
                    limit=batch_size,
                )
                if page.empty:
                    break
                pages.append(page)
                if len(page) < batch_size:
                    break
                skip += batch_size
        except Exception as e:
            logger.warning(
                "Could not read snow data for %s: %s",
                variable,
                e,
            )
            continue

        if not pages:
            logger.info("No API data for %s, skipping", variable)
            continue

        df = pd.concat(pages, ignore_index=True)
        df = df.drop_duplicates(subset=["snow_type", "code", "date"])
        logger.info(
            "Fetched %d unique rows for %s in %d pages",
            len(df),
            variable,
            len(pages),
        )

        if "value" not in df.columns:
            logger.warning(
                "No 'value' column for %s, skipping",
                variable,
            )
            continue

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date", "value"])

        if df.empty:
            continue

        df["dayofyear"] = df["date"].dt.dayofyear

        # Compute norms per station code, per day of year
        for code in df["code"].unique():
            code_df = df[df["code"] == code]

            n_years = code_df["date"].dt.year.nunique()
            logger.info(
                "Computing norm for %s/%s from %d years of data",
                variable,
                code,
                n_years,
            )

            norms = code_df.groupby("dayofyear")["value"].mean().reset_index()
            norms.columns = ["dayofyear", "norm"]
            norms["snow_type"] = variable
            norms["code"] = str(code)
            result_frames.append(norms[["snow_type", "code", "dayofyear", "norm"]])

    if result_frames:
        return pd.concat(result_frames, ignore_index=True)

    return pd.DataFrame(columns=["snow_type", "code", "dayofyear", "norm"])


# --------------------------------------------------------------------
# Shared snow API write
# --------------------------------------------------------------------


def _read_existing_norms(
    client,
    snow_type: str,
    codes: list[str],
    start_date: str,
    end_date: str,
) -> dict:
    """Read existing norm values from the API to prevent overwrite.

    Returns a dict keyed by ``(code, date_str)`` → ``norm_value``.
    On any failure, returns an empty dict (writes proceed with None).
    """
    norms: dict = {}
    try:
        for code in codes:
            api_df = client.read_snow(
                snow_type=snow_type.upper(),
                code=str(code),
                start_date=start_date,
                end_date=end_date,
                limit=100000,
            )
            if api_df.empty:
                continue
            for _, row in api_df.iterrows():
                norm_val = row.get("norm")
                if pd.notna(norm_val):
                    d = pd.to_datetime(row["date"]).strftime("%Y-%m-%d")
                    norms[(str(row["code"]), d)] = float(norm_val)
    except Exception as e:
        logger.warning(
            "Could not read existing norms from API (%s): %s. "
            "Proceeding without norm preservation.",
            snow_type,
            e,
        )
        return {}
    return norms


def write_snow_to_api(
    data: pd.DataFrame,
    snow_type: str,
    hru_code: str,
    mode: str | None = None,
    reference_date=None,
) -> bool:
    """Write snow data to the SAPPHIRE preprocessing API.

    This is the shared implementation used by both operational and
    reanalysis snow pipelines. It preserves existing norm values in
    the API when the incoming data has no norm.

    Supports different sync modes:
    - operational (default): write yesterday+today (2-day window)
    - maintenance: write the last 30 days
    - initial: write all data

    Args:
        data: DataFrame with snow data. Expected columns:
            - date: date
            - code: station code
            - {snow_type}: value (e.g., SWE, HS, RoF)
            - {snow_type}_1 .. {snow_type}_14: optional elevation
              band values
            - norm: optional norm column
        snow_type: Type of snow data (SWE, HS, RoF).
        hru_code: HRU code for logging context.
        mode: Sync mode override. If None, reads SAPPHIRE_SYNC_MODE
            env var, defaulting to 'operational'.
        reference_date: Reference date for windowing. If None, uses
            ``pd.Timestamp.today()``. Reanalysis passes
            ``data['date'].max()`` so the maintenance window is
            relative to the data, not the wall clock.

    Returns:
        True if records were written, False otherwise.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.warning("sapphire-api-client not installed, skipping snow API write")
        return False

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
    if not api_enabled:
        logger.info("SAPPHIRE API writing disabled via SAPPHIRE_API_ENABLED=false")
        return False

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    client = SapphirePreprocessingClient(base_url=api_url)

    if not client.readiness_check():
        logger.warning(
            "SAPPHIRE API at %s is not ready, skipping snow write (HRU %s, %s)",
            api_url,
            hru_code,
            snow_type,
        )
        return False

    if data.empty:
        logger.info("No snow data to write to API (%s, HRU %s)", snow_type, hru_code)
        return False

    data = data.copy()
    data["date"] = pd.to_datetime(data["date"])

    # Determine reference point for date windowing
    if reference_date is not None:
        ref = pd.Timestamp(reference_date).normalize()
    else:
        ref = pd.Timestamp.today().normalize()

    # Filter data based on sync mode (parameter > env var > default)
    if mode is not None:
        sync_mode = mode.lower()
    else:
        sync_mode = os.getenv("SAPPHIRE_SYNC_MODE", "operational").lower()
    logger.info(
        "Snow API sync mode: %s (%s, HRU %s)",
        sync_mode,
        snow_type,
        hru_code,
    )

    yesterday = ref - pd.Timedelta(days=1)
    if sync_mode == "operational":
        # Include yesterday, today, and any forecast dates beyond today
        data_to_write = data[data["date"] >= yesterday]
    elif sync_mode == "maintenance":
        cutoff = ref - pd.Timedelta(days=30)
        data_to_write = data[data["date"] >= cutoff]
    elif sync_mode == "initial":
        data_to_write = data
    else:
        logger.warning(
            "Unknown sync mode '%s', defaulting to operational",
            sync_mode,
        )
        data_to_write = data[data["date"] >= yesterday]

    if data_to_write.empty:
        if sync_mode == "operational":
            date_range = f"{data['date'].min().date()} to {data['date'].max().date()}"
            logger.warning(
                "No snow data for %s to %s (%s, HRU %s). "
                "CSV date range: %s. Data gateway may not have "
                "returned recent data yet.",
                yesterday.date(),
                ref.date(),
                snow_type,
                hru_code,
                date_range,
            )
        else:
            logger.info(
                "No snow data to write after %s filtering (%s, HRU %s)",
                sync_mode,
                snow_type,
                hru_code,
            )
        return False

    codes = data_to_write["code"].unique()
    logger.info(
        "%s mode: writing %d snow records (HRU %s, %s, codes: %s)",
        sync_mode,
        len(data_to_write),
        hru_code,
        snow_type,
        list(codes),
    )

    # Read existing norms so we don't clobber them with None
    start_str = data_to_write["date"].min().strftime("%Y-%m-%d")
    end_str = data_to_write["date"].max().strftime("%Y-%m-%d")
    existing_norms = _read_existing_norms(
        client,
        snow_type,
        [str(c) for c in codes],
        start_str,
        end_str,
    )

    # Identify elevation band columns (e.g., SWE_1, SWE_2, ...)
    value_columns = {}
    main_value_col = snow_type if snow_type in data_to_write.columns else None
    for col in data_to_write.columns:
        if col.startswith(f"{snow_type}_") and col != snow_type:
            try:
                band_num = int(col.split("_")[-1])
                value_columns[band_num] = col
            except ValueError:
                pass

    # Prepare records for API
    records = []
    for _, row in data_to_write.iterrows():
        date_obj = pd.to_datetime(row["date"]) if pd.notna(row.get("date")) else None
        if date_obj is None:
            logger.warning("Skipping snow row with missing date: %s", row.to_dict())
            continue

        date_str = date_obj.strftime("%Y-%m-%d")
        code_str = str(row["code"])

        # Determine norm: prefer incoming, fall back to existing API
        local_norm = None
        if "norm" in row and pd.notna(row.get("norm")):
            local_norm = round(float(row["norm"]), 3)
        elif (code_str, date_str) in existing_norms:
            local_norm = round(existing_norms[(code_str, date_str)], 3)

        record = {
            "snow_type": snow_type.upper(),
            "code": code_str,
            "date": date_str,
            "value": (
                round(float(row[main_value_col]), 3)
                if main_value_col and pd.notna(row.get(main_value_col))
                else None
            ),
            "norm": local_norm,
        }

        # Add elevation band values (value1-value14)
        for band_num, col_name in value_columns.items():
            if band_num <= 14:
                record[f"value{band_num}"] = (
                    round(float(row[col_name]), 3) if pd.notna(row.get(col_name)) else None
                )

        records.append(record)

    # Write to API
    if records:
        count = client.write_snow(records)
        logger.info("SAPPHIRE API: Wrote %d snow records (%s, HRU %s)", count, snow_type, hru_code)
        print(
            f"SAPPHIRE API: Successfully wrote {count} snow records ({snow_type}, HRU {hru_code})"
        )
        return True
    else:
        logger.info("No snow records to write to API (%s, HRU %s)", snow_type, hru_code)
        return False
