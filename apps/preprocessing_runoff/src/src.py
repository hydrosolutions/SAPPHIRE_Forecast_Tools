import os
import sys
import io
import pandas as pd
import numpy as np
import datetime as dt
from datetime import timedelta
import time
import pytz
import json
import concurrent.futures
from typing import List, Tuple
from contextlib import contextmanager

# Profiling utilities for performance analysis
# Handle different import contexts (package vs direct module import)
import importlib.util
import os as _os
_profiling_path = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), 'profiling.py')
_spec = importlib.util.spec_from_file_location("profiling", _profiling_path)
_profiling_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_profiling_module)
ProfileTimer = _profiling_module.ProfileTimer
profile_section = _profiling_module.profile_section
log_profiling_report = _profiling_module.log_profiling_report
profiling_enabled = _profiling_module.profiling_enabled

# To avoid printing of warning
# pd.set_option('future.no_silent_downcasting', True)  # Comment out - not available in current pandas version

from ieasyhydro_sdk.filters import BasicDataValueFilters

import logging
logger = logging.getLogger(__name__)

# Set the logging level for iEasyHydro SDK specifically
logging.getLogger('ieasyhydro_sdk').setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# Also set any potential sub-loggers
for name in logging.root.manager.loggerDict:
    if name.startswith('ieasyhydro_sdk'):
        logging.getLogger(name).setLevel(logging.WARNING)


@contextmanager
def suppress_stdout():
    """Context manager to suppress stdout temporarily."""
    save_stdout = sys.stdout
    sys.stdout = io.StringIO()
    try:
        yield
    finally:
        sys.stdout = save_stdout

def standardize_date_column(df, date_col='date'):
    """
    Standardize a date column to normalized pandas Timestamps.
    
    This function converts the specified column to pandas Timestamp objects
    with time components set to midnight (00:00:00). Using normalized Timestamps
    ensures consistent behavior when:
    - Merging dataframes on date columns
    - Comparing dates
    - Calculating date ranges
    - Filtering by date
    
    Args:
        df (pd.DataFrame): The dataframe containing the date column
        date_col (str): The name of the date column to standardize
        
    Note: 
        - The function modifies the dataframe in place, converting the specified
          date column to pandas Timestamps with time set to midnight.
        - Timezone information is removed to ensure uniformity across different 
          time zones.

    Returns:
        pd.DataFrame: A copy of the dataframe with standardized date column
    """
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col]).dt.tz_localize(None).dt.normalize()
    return df

def get_local_time_range_for_daily_average_runoff_request(target_timezone, window_size=50):
    """
    Calculates UTC datetime objects for the data request that represent local times.
    
    According to the SDK documentation, we should use UTC timezone but the backend
    will interpret these as local times for filtering.

    Parameters:
    target_timezone (pytz.timezone): The target time zone (used only for calculating dates).
    window_size (int): The number of days to look back from the current date.
    Default is 50 days.

    Returns:
    tuple: A tuple containing the start and end times as UTC datetime objects
           that represent local times (evening 20:00 for daily averages).
    """
    if not target_timezone:
        return None, None

    # Get the current date in the target time zone
    now_local = dt.datetime.now(target_timezone)
    today = now_local.date()

    # Calculate the end time (today at 20:00 as UTC but interpreted as local time)
    end_time_utc = dt.datetime(today.year, today.month, today.day, 20, 0, tzinfo=dt.timezone.utc)

    # Calculate the start time (window_size days prior at 20:00 as UTC but interpreted as local time)
    start_date = today - timedelta(days=window_size)
    start_time_utc = dt.datetime(start_date.year, start_date.month, start_date.day, 20, 0, tzinfo=dt.timezone.utc)

    return start_time_utc, end_time_utc

def get_local_time_range_for_todays_morning_runoff_request(target_timezone):
    """
    Calculates UTC datetime objects for morning data request that represent local times.
    
    According to the SDK documentation, we should use UTC timezone but the backend
    will interpret these as local times for filtering. For morning data, we filter
    between 8:00 and the current hour.

    Parameters:
    target_timezone (pytz.timezone): The target time zone (used for calculating current time).

    Returns:
    tuple: A tuple containing the start and end times as UTC datetime objects
           that represent local times (8:00 morning start, current hour end).
    """
    if not target_timezone:
        return None, None

    # Get the current time in the target time zone
    now_local = dt.datetime.now(target_timezone)
    today = now_local.date()

    # Calculate the start time (today at 8:00 as UTC but interpreted as local time)
    start_time_utc = dt.datetime(today.year, today.month, today.day, 8, 0, tzinfo=dt.timezone.utc)

    # Calculate the end time (current hour as UTC but interpreted as local time)
    # Use current hour to get data up to now
    end_time_utc = dt.datetime(today.year, today.month, today.day, now_local.hour, now_local.minute, tzinfo=dt.timezone.utc)

    return start_time_utc, end_time_utc

def should_reprocess_input_files():
    """Check if any input files have been modified since the last run."""
    # Get the path to the daily_discharge directory
    daily_discharge_dir = os.getenv('ieasyforecast_daily_discharge_path')
    
    # Get the path to the output file
    intermediate_data_path = os.getenv('ieasyforecast_intermediate_data_path')
    output_file_path = os.path.join(
        intermediate_data_path,
        os.getenv("ieasyforecast_daily_discharge_file"))
    
    # Get last modification time of output file
    if not os.path.exists(output_file_path):
        return True  # Output file doesn't exist, need to process input files
        
    output_mod_time = os.path.getmtime(output_file_path)
    
    # Check if any input file was modified after the output file
    for file in os.listdir(daily_discharge_dir):
        full_path = os.path.join(daily_discharge_dir, file)
        if (os.path.isfile(full_path) and 
            (file.endswith('.xlsx') or file.endswith('.csv')) and
            not file.startswith('~')):
            if os.path.getmtime(full_path) > output_mod_time:
                logger.info(f"File {file} was modified since last processing")
                return True
                
    # No input files were modified after the output was generated
    return False

def filter_roughly_for_outliers(combined_data, group_by='Code',
                                filter_col='Q_m3s', date_col='date'):
    """
    Filters outliers in the filter_col column of the input DataFrame.

    This function groups the input DataFrame by the group_by column and the month derived from the date_col column,
    and applies a rolling window outlier detection method to the filter_col column of each group.
    Outliers are defined as values that are more than 1.5 times the IQR away from the Q1 and Q3.
    These outliers are replaced with NaN.

    The function further drops all rows with NaN in the group_by column.

    Parameters:
    combined_data (pd.DataFrame): The input DataFrame. Must contain 'Code' and 'Q_m3s' columns.
    group_by (str, optional): The column to group the data by. Default is 'Code'.
    filter_col (str, optional): The column to filter for outliers. Default is 'Q_m3s'.
    date_col (str, optional): The column representing dates. Default is 'date'.

    Returns:
    pd.DataFrame: The input DataFrame with outliers in the 'Q_m3s' column replaced with NaN.

    Raises:
    ValueError: If the group_by column is not found in the input DataFrame.
    """
    def filter_group(group, filter_col, date_col):

        # Only apply IQR filtering if the group has more than 10 rows
        if len(group) > 10:
            # Calculate Q1, Q3, and IQR
            Q1 = group[filter_col].quantile(0.25)
            Q3 = group[filter_col].quantile(0.75)
            IQR = Q3 - Q1

            # Calculate the upper and lower bounds for outliers
            upper_bound = Q3 + 6.5 * IQR
            lower_bound = Q1 - 2.0 * IQR

            # Set Q_m3s which exceeds lower and upper bounds to NaN
            group.loc[group[filter_col] > upper_bound, filter_col] = np.nan
            group.loc[group[filter_col] < lower_bound, filter_col] = np.nan

        # Set the date column as the index
        group[date_col] = pd.to_datetime(group[date_col])
        group.set_index(date_col, inplace=True)

        # Drop duplicates from the index
        group = group.loc[~group.index.duplicated(keep='first')]

        # Reindex the data frame to include all dates in the range
        all_dates = pd.date_range(start=group.index.min(), end=group.index.max(), freq='D')
        group = group.reindex(all_dates)
        group.index.name = date_col

        # Interpolate gaps of length of max 2 days linearly
        group[filter_col] = group[filter_col].interpolate(method='time', limit=2)

        # Infer object types to address the FutureWarning
        group = group.infer_objects(copy=False)

        # Filter out suspicious data characterized by changes of more than 300% from one time step to the next
        group['prev_value'] = group[filter_col].shift(1)
        group['change'] = (group[filter_col] - group['prev_value']).abs() / group['prev_value'].abs()
        group.loc[group['change'] > 3, filter_col] = np.nan
        group.drop(columns=['prev_value', 'change'], inplace=True)

        # Interpolate gaps of length of max 2 days linearly
        group[filter_col] = group[filter_col].interpolate(method='time', limit=2)

        # Reset the index
        group.reset_index(inplace=True)

        # Print statistics of how many values were set to NaN
        num_outliers = group[filter_col].isna().sum()
        num_total = group[filter_col].notna().sum() + num_outliers
        logger.info(f"filter_roughly_for_outliers:\n     from a total of {num_total}, {num_outliers} outliers set to NaN in group '{group[group_by].iloc[0]}'.")

        return group

    # Test if the group_by column is available
    if group_by not in combined_data.columns:
        raise ValueError(f"Column '{group_by}' not found in the DataFrame.")

    # Test if the filter_col column is available
    if filter_col not in combined_data.columns:
        raise ValueError(f"Column '{filter_col}' not found in the DataFrame.")

    # Drop rows with NaN in the group_by column
    # Use .copy() to avoid SettingWithCopyWarning when modifying the DataFrame later
    combined_data = combined_data.dropna(subset=[group_by]).copy()

    # Replace empty places in the filter_col column with NaN
    combined_data[filter_col] = combined_data[filter_col].replace('', np.nan)

    # Extract month from the date_col
    combined_data[date_col] = pd.to_datetime(combined_data[date_col])
    combined_data['month'] = combined_data[date_col].dt.month

    # To use individual months for filtering is too narrow a criteria. Combine
    # spring months (March, April, May) and autumn months (September, October,
    # November) to get a better estimate of the Q1, Q3, and similarly for the
    # other seasons.
    combined_data['month'] = combined_data['month'].replace({1: 'winter', 2: 'winter',
                                                             3: 'spring', 4: 'spring', 5: 'spring',
                                                             6: 'summer', 7: 'summer', 8: 'summer',
                                                             9: 'autumn', 10: 'autumn', 11: 'autumn',
                                                             12: 'winter'})

    # Ensure the DataFrame is properly structured
    combined_data = combined_data.reset_index(drop=True)

    # Apply the function to each group
    # Note: FutureWarning about include_groups is expected - the filter_group function
    # needs access to group_by column for logging. Full fix deferred to Phase 8.
    import warnings
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="DataFrameGroupBy.apply operated on the grouping columns")
        combined_data = combined_data.groupby([group_by, 'month'], as_index=False).apply(
            filter_group, filter_col, date_col)

    # Ungroup the DataFrame
    combined_data = combined_data.reset_index(drop=True)

    # Drop the temporary month column
    combined_data.drop(columns=['month'], inplace=True)

    # Drop rows with duplicate code and dates, keeping the last one
    combined_data = combined_data.drop_duplicates(subset=[group_by, date_col], keep='last')

    # Sort by code and date
    combined_data = combined_data.sort_values(by=[group_by, date_col])

    # Remove rows with NaN in the group_by column
    combined_data = combined_data.dropna(subset=[group_by])

    # Print the latest date available in combined_data
    if not combined_data.empty:
        latest_date = combined_data[date_col].max()
        logger.info(f"filter_roughly_for_outliers: Latest date available in combined_data: {latest_date}")

    return combined_data

def read_runoff_data_from_csv_files(filename, code_list, date_col='date', 
                                    discharge_col='discharge', name_col='name', 
                                    code_col='code'):
    """
    Read daily average river runoff data from a csv file.
    
    The function reads dates from the first column, station code from the second 
    column, river runoff in m3/s from the third column and the name of the river 
    from the third column. 

    Parameters
    ----------
    filename : str
        Path to the csv file containing the river runoff data.
    date_col : str, optional
        Name of the column containing the date data. Default is 'date'.
    discharge_col : str, optional
        Name of the column containing the river runoff data. Default is 'discharge'.
    name_col : str, optional
        Name of the column containing the river name. Default is 'name'.
    code_col : str, optional
        Name of the column containing the river code. Default is 'code'.
    code_list : list, required
        List of 5-digit codes to include in the output DataFrame

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the river runoff data.
    """

    # Test if code_list is None
    if code_list is None:
        logger.error("read_runoff_data_from_multiple_rivers_xlsx: No code list provided.")

    # Test if csv file is available
    try:
        df = pd.read_csv(filename, header=0, usecols=[0, 1, 2, 3],
                         names=[date_col, code_col, discharge_col, name_col])
        logger.debug(f"[DATA] read_runoff_data_from_csv_files: raw df.head():\n{df.head()}")
    except FileNotFoundError:
        raise FileNotFoundError(f"File '{filename}' not found.")
    except pd.errors.ParserError:
        raise ValueError(f"Error parsing file '{filename}'. Please check the file format.")
    except Exception as e:
        raise Exception(f"An error occurred while reading the file '{filename}': {e}")
    
    # Test if the file is empty
    if df.empty:
        logger.warning(f"File '{filename}' is empty. No data to read.")
        return pd.DataFrame()
    
    # Convert the date column to nomralized pandas Timestamps
    df[date_col] = pd.to_datetime(df[date_col], format='%Y-%m-%d').dt.normalize()

    # Convert discharge column to numeric format
    df[discharge_col] = pd.to_numeric(df[discharge_col], errors='coerce')

    # Make sure code_col is integer
    df[code_col] = df[code_col].astype(int)

    # Filter for codes in code_list
    if code_list is not None:
        df = df[df[code_col].astype(str).isin(code_list)]

    logger.debug(f"[DATA] read_runoff_data_from_csv_files: final df.head:\n{df.head()}")

    return df

def read_runoff_data_from_multiple_rivers_xlsx(filename, code_list, date_col='date',
                                               discharge_col='discharge',
                                               name_col='name', code_col='code'):
    """
    Read daily average river runoff data from an excel sheet.

    The function reads dates from the first column and river runoff data from
    the second column of the excel sheet. The river name is extracted from the
    first row of the sheet. The function reads data from all sheets in the excel
    file and combines them into a single DataFrame. The function replaces
    missing data with NaN.

    Note
    ----------
    This function assumes that the station code is an integer of 5 digits.


    Parameters
    ----------
    filename : str
        Path to the excel sheet containing the river runoff data.
    date_col : str, optional
        Name of the column containing the date data. Default is 'date'.
    discharge_col : str, optional
        Name of the column containing the river runoff data. Default is 'discharge'.
    name_col : str, optional
        Name of the column containing the river name. Default is 'name'.
    code_col : str, optional
        Name of the column containing the river code. Default is 'code'.
    code_list : list, required
        List of 5-digit codes to include in the output DataFrame

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the river runoff data.

    Raises
    ------
    FileNotFoundError
        If the excel file is not found.
    """
    # Test if code_list is None
    if code_list is None:
        logger.error("read_runoff_data_from_multiple_rivers_xlsx: No code list provided.")

    # Test if excel file is available
    try:
        xls = pd.ExcelFile(filename)
    except FileNotFoundError:
        raise FileNotFoundError(f"File '{filename}' not found.")

    # Extract all sheet names
    # Sheet names can be anything, they are only used to iterate through the
    # document.
    xls.sheet_names

    # load data from all sheets into a single dataframe
    df = pd.DataFrame()
    # the river name is in cell A1 of each sheet
    # the data starts in row 3 of each sheet where column A contains the time stamp and column B contains the river runoff data.
    # some of the daily time series data are missing and the corresponding cells contain '-'. There might be a type mismatch.
    # We want to have all data in a single dataframe df with the following columns: date, river runoff, river.
    for sheet_name in xls.sheet_names:
        df_sheet = pd.read_excel(xls, sheet_name, header=1, usecols=[0, 1], names=[date_col, discharge_col])
        logger.debug(f"Reading sheet {sheet_name} \n {df_sheet.head()}")
        # read cell A1 and extract the river name
        # Read the river name from cell A1
        river_name_df = pd.read_excel(xls, sheet_name, nrows=1, usecols="A", header=None)
        full_river_name = river_name_df.iloc[0, 0]
        logger.debug(f"full_river_name: %s", full_river_name)
        # Check if the first 5 characters are digits
        try:
            int(full_river_name[:5])
            is_numeric = True
        except (IndexError, ValueError):
            is_numeric = False

        if is_numeric:
            code = int(full_river_name[:5])
            if str(code) not in code_list:
                logger.debug(f"Code {code} not in code_list. Skipping data for river {full_river_name}.")
                continue
            river_name = full_river_name[5:].lstrip()
        else:
            code = 'NA'
            # Test if the river name is equal to 'date', 'Date' or 'Дата' in any of
            # the typical languages used in Central Asia or Switzerland. Print a
            # warning if it is.
            river_name = full_river_name
            if river_name.lower() in ['date', 'дата', 'datum', 'sana', 'сана',
                                  'senesi', 'sene', 'күні', 'күн']:
                logger.error(
                    f'The river name in file {filename}, sheet {sheet_name} was '
                    f'found to be {river_name}.\nPlease verify that a 5-digit code '
                    f'is present in cell A1 and rerun the preprocessing runoff module.')
                raise ValueError(
                    f'The river name in file {filename}, sheet {sheet_name} was '
                    f'found to be {river_name}.\nPlease verify that a 5-digit code '
                    f'is present in cell A1 and rerun the preprocessing runoff module.')

            logger.warning(f"No code could be read from file {filename} sheet {sheet_name}.\n"
                           f"Skipping data for river {full_river_name}.")
            continue

        logger.debug("Code read from header cell: %s", code)
        logger.debug("River name read from header cell: %s", river_name)

        df_sheet[name_col] = river_name
        df_sheet[code_col] = code
        df = pd.concat([df, df_sheet], axis=0)

    # Convert date column to normalized Timestamp format
    # This ensures that the time component is set to midnight (00:00:00) for 
    # consistency when merging dataframes on date columns, comparing dates, 
    # calculating date ranges, etc.
    df[date_col] = pd.to_datetime(df[date_col], format='%d.%m.%Y').dt.normalize()

    # Convert discharge column to numeric format
    df[discharge_col] = pd.to_numeric(df[discharge_col], errors='coerce')

    # Replace data in rows with missing values (indicated in excel with `-`), 
    # with NaN
    df[discharge_col] = df[discharge_col].replace('-', float('nan'))

    return df

def read_runoff_data_from_single_river_xlsx(filename, code_list, date_col='date',
                                            discharge_col='discharge',
                                            name_col='name', code_col='code'):
    """
    Read daily average river runoff data from an excel sheet.

    The function reads dates from the first column and river runoff data from
    the second column of the excel sheet. The river name is extracted from the
    first row of the sheet. The function reads data from all sheets in the excel
    file and combines them into a single DataFrame. The function replaces
    missing data with NaN.

    Parameters
    ----------
    filename : str
        Path to the excel sheet containing the river runoff data.
    date_col : str, optional
        Name of the column containing the date data. Default is 'date'.
    discharge_col : str, optional
        Name of the column containing the river runoff data. Default is 'discharge'.
    name_col : str, optional
        Name of the column containing the river name. Default is 'name'.
    code_col : str, optional
        Name of the column containing the river code. Default is 'code'.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the river runoff data.

    Raises
    ------
    FileNotFoundError
        If the excel file is not found.
    """
    # Test if code_list is None
    if code_list is None:
        logger.error("read_runoff_data_from_single_river_xlsx: No code list provided.")

    # Test if excel file is available
    try:
        xls = pd.ExcelFile(filename)
    except FileNotFoundError:
        raise FileNotFoundError(f"File '{filename}' not found.")

    # Extract the name of the file from the path filename
    filename = os.path.basename(filename)

    # Read the river name from the first 5 characters of the file name
    river_code = filename[:5]
    river_name = filename[6:-16]

    # Test if river_code is in code_list and skip the file if it is not
    if code_list is not None and river_code not in code_list:
        logger.debug(f"River code {river_code} not in code_list. Skipping file {filename}")
        return pd.DataFrame()

    # Extract all sheet names
    xls.sheet_names

    # load data from all sheets into a single dataframe
    df = pd.DataFrame()
    # the data starts in row 2 of each sheet where column A contains the time stamp and column B contains the river runoff data.
    # some of the daily time series data are missing and the corresponding cells contain '-'. There might be a type mismatch.
    # We want to have all data in a single dataframe df with the following columns: date, river runoff, river.
    for sheet_name in xls.sheet_names:
        df_sheet = pd.read_excel(xls, sheet_name, header=0, usecols=[0, 1], names=[date_col, discharge_col])
        #print(f"Reading sheet '{sheet_name}' \n '{df_sheet.head()}'")
        # read cell A1 and extract the river name
        df_sheet[name_col] = river_name
        df_sheet[code_col] = river_code
        df = pd.concat([df, df_sheet], axis=0)

    # Convert date column to normalized Timestamp format
    df[date_col] = pd.to_datetime(df[date_col], format='%d.%m.%Y').dt.normalize()

    # Convert discharge column to numeric format
    df[discharge_col] = pd.to_numeric(df[discharge_col], errors='coerce')

    # Replace data in rows with missing values with NaN
    df[discharge_col] = df[discharge_col].replace('-', float('nan'))

    # Make sure code_col is integer
    df[code_col] = df[code_col].astype(int)

    return df

def parallel_read_excel_files(file_paths: List[str],
                            read_function,
                            code_list,
                            date_col='date',
                            discharge_col='discharge',
                            name_col='name',
                            code_col='code') -> pd.DataFrame:
    """
    Reads multiple Excel files in parallel using ThreadPoolExecutor.

    Args:
        file_paths: List of Excel file paths to read
        read_function: Function to use for reading (either read_runoff_data_from_multiple_rivers_xlsx
                      or read_runoff_data_from_single_river_xlsx)
        date_col: Name of date column
        discharge_col: Name of discharge column
        name_col: Name of name column
        code_col: Name of code column
        code_list: List of hydropost codes to include

    Returns:
        Combined DataFrame from all Excel files
    """
    # Test if code_list is None
    if code_list is None:
        logger.warning("parallel_read_excel_files: No code list provided.")

    def read_file(file_path: str) -> Tuple[pd.DataFrame, str]:
        try:
            logger.info(f"Reading daily runoff from file {os.path.basename(file_path)}")
            df = read_function(
                filename=file_path,
                code_list=code_list,
                date_col=date_col,
                discharge_col=discharge_col,
                name_col=name_col,
                code_col=code_col,
            )
            return df, None
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
            return pd.DataFrame(), str(e)

    # Use ThreadPoolExecutor for parallel reading
    # Number of workers is min(32, os.cpu_count() + 4) by default
    results = []
    errors = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_file = {executor.submit(read_file, fp): fp for fp in file_paths}

        for future in concurrent.futures.as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                df, error = future.result()
                if error is None:
                    if not df.empty:
                        results.append(df)
                else:
                    errors.append((file_path, error))
            except Exception as e:
                logger.error(f"Exception occurred while processing {file_path}: {str(e)}")
                errors.append((file_path, str(e)))

    # Report any errors that occurred
    if errors:
        logger.warning(f"Encountered {len(errors)} errors while reading files:")
        for file_path, error in errors:
            logger.warning(f"  {os.path.basename(file_path)}: {error}")

    # Combine all DataFrames
    if not results:
        logger.warning("No data was successfully read from any Excel files")
        return pd.DataFrame()

    return pd.concat(results, ignore_index=True)

def read_all_runoff_data_from_csv(date_col='date', 
                                  discharge_col='discharge', 
                                  name_col='name', 
                                  code_col='code',
                                  code_list=None):
    """
    Reads daily river runoff data from all csv files in the daily_discharge
    directory.
    """
    # Test if code_list is none
    if code_list is None:
        logger.error("read_all_runoff_data_from_excel: No code list provided.")

    # Get the path to the daily_discharge directory
    daily_discharge_dir = os.getenv('ieasyforecast_daily_discharge_path')

    # Test if the directory is available
    if not os.path.exists(daily_discharge_dir):
        raise FileNotFoundError(f"Directory '{daily_discharge_dir}' not found.")

    # Get the list of csv files in the directory
    files = [
        os.path.join(daily_discharge_dir, f)
        for f in os.listdir(daily_discharge_dir)
        if os.path.isfile(os.path.join(daily_discharge_dir, f))
        and f.endswith('.csv')
    ]

    # Read the data from all files
    df = pd.DataFrame()
    for file in files:
        logger.info(f"Reading daily runoff from file {file}")
        if df.empty:
            df = read_runoff_data_from_csv_files(
                filename=file,
                code_list=code_list,
                date_col=date_col,
                discharge_col=discharge_col,
                name_col=name_col,
                code_col=code_col
            )
        else:
            df = pd.concat([df, read_runoff_data_from_csv_files(
                filename=file,
                code_list=code_list,
                date_col=date_col,
                discharge_col=discharge_col,
                name_col=name_col,
                code_col=code_col
            )], axis=0)

    # Test if the file is empty
    if df.empty:
        logger.warning(f"No data found in the daily discharge directory")
        return None
    
    return df

def read_all_runoff_data_from_excel(date_col='date',
                                  discharge_col='discharge',
                                  name_col='name',
                                  code_col='code',
                                  code_list=None):
    """
    Reads daily river runoff data from all excel sheets in the daily_discharge
    directory using parallel processing.
    """
    # Test if code_list is none
    if code_list is None:
        logger.error("read_all_runoff_data_from_excel: No code list provided.")

    # Get the path to the daily_discharge directory
    daily_discharge_dir = os.getenv('ieasyforecast_daily_discharge_path')

    # Test if the directory is available
    if not os.path.exists(daily_discharge_dir):
        raise FileNotFoundError(f"Directory '{daily_discharge_dir}' not found.")

    # Get lists of files for multiple rivers and single rivers
    files_multiple_rivers = [
        os.path.join(daily_discharge_dir, f)
        for f in os.listdir(daily_discharge_dir)
        if os.path.isfile(os.path.join(daily_discharge_dir, f))
        and f.endswith('.xlsx')
        and not f[0].isdigit()
        and not f.startswith('~')
    ]

    files_single_rivers = [
        os.path.join(daily_discharge_dir, f)
        for f in os.listdir(daily_discharge_dir)
        if os.path.isfile(os.path.join(daily_discharge_dir, f))
        and f.endswith('.xlsx')
        and f[0].isdigit()
        and not f.startswith('~')
    ]

    # Read multiple rivers files in parallel
    df_multiple = pd.DataFrame()
    if files_multiple_rivers:
        logger.info(f"Reading {len(files_multiple_rivers)} files with multiple rivers data")
        df_multiple = parallel_read_excel_files(
            files_multiple_rivers,
            read_runoff_data_from_multiple_rivers_xlsx,
            code_list=code_list,
            date_col=date_col,
            discharge_col=discharge_col,
            name_col=name_col,
            code_col=code_col,
        )
    else:
        logger.warning(f"No excel files with multiple rivers data found in '{daily_discharge_dir}'.")

    # Read single river files in parallel
    df_single = pd.DataFrame()
    if files_single_rivers:
        logger.info(f"Reading {len(files_single_rivers)} files with single river data")
        df_single = parallel_read_excel_files(
            files_single_rivers,
            read_runoff_data_from_single_river_xlsx,
            code_list=code_list,
            date_col=date_col,
            discharge_col=discharge_col,
            name_col=name_col,
            code_col=code_col,
        )
    else:
        logger.warning(f"No excel files with single river data found in '{daily_discharge_dir}'.")

    # Combine the results
    if df_multiple.empty and df_single.empty:
        logger.warning("No data found in the daily discharge directory")
        return None
    elif df_multiple.empty:
        return df_single
    elif df_single.empty:
        return df_multiple
    else:
        return pd.concat([df_multiple, df_single], ignore_index=True)

def original_read_all_runoff_data_from_excel(date_col='date', discharge_col='discharge', name_col='name', code_col='code'):
    """
    Reads daily river runoff data from all excel sheets in the daily_discharge
    directory.

    The names of the dataframe columns can be customized.

    Args:
        date_col (str, optional): The name of the column containing the date data.
            Default is 'date'.
        discharge_col (str, optional): The name of the column containing the discharge data.
            Default is 'discharge'.
        name_col (str, optional): The name of the column containing the river name.
            Default is 'name'.
        code_col (str, optional): The name of the column containing the river code.
            Default is 'code'.

    Returns:
        pandas.DataFrame: A DataFrame containing the daily river runoff data.

    Raises:
        None
    """
    # Get the path to the daily_discharge directory
    daily_discharge_dir = os.getenv('ieasyforecast_daily_discharge_path')

    # Test if the directory is available
    if not os.path.exists(daily_discharge_dir):
        raise FileNotFoundError(f"Directory '{daily_discharge_dir}' not found.")

    # Get the list of files in the directory
    # The names of excel files with daily runoff data from multiple rivers do
    # not start with a digit.
    files_multiple_rivers = [
        f for f in os.listdir(daily_discharge_dir)
        if os.path.isfile(os.path.join(daily_discharge_dir, f))
        and f.endswith('.xlsx') and not f[0].isdigit() and not f.startswith('~')
    ]
    # Initiate empty dataframe
    df = None

    if len(files_multiple_rivers) == 0:
        logger.warning(f"No excel files with multiple rivers data found in '{daily_discharge_dir}'.")
    else:
        # Read the data from all files
        for file in files_multiple_rivers:
            file_path = os.path.join(daily_discharge_dir, file)
            logger.info(f"Reading daily runoff from file {file}")
            if df is None:
                df = read_runoff_data_from_multiple_rivers_xlsx(
                            filename=file_path,
                            date_col=date_col,
                            discharge_col=discharge_col,
                            name_col=name_col,
                            code_col=code_col)
            else:
                df = pd.concat([df,
                        read_runoff_data_from_multiple_rivers_xlsx(
                            filename=file_path,
                            date_col=date_col,
                            discharge_col=discharge_col,
                            name_col=name_col,
                            code_col=code_col)],
                        axis=0)

    # Do the same for files with single rivers
    # Names of files with daily river runoff of individual rivers start with a
    # digit, indicating a unique code.
    df_single = None

    files_single_rivers = [
        f for f in os.listdir(daily_discharge_dir)
        if os.path.isfile(os.path.join(daily_discharge_dir, f))
        and f.endswith('.xlsx') and f[0].isdigit() and not f.startswith('~')
    ]
    if len(files_single_rivers) == 0:
        logger.warning(f"No excel files with single river data found in '{daily_discharge_dir}'.")
    else:
        # Read the data from all files
        for file in files_single_rivers:
            file_path = os.path.join(daily_discharge_dir, file)
            logger.info(f"Reading daily runoff from file {file}")
            if df_single is None:
                df_single = read_runoff_data_from_single_river_xlsx(
                            filename=file_path,
                            date_col=date_col,
                            discharge_col=discharge_col,
                            name_col=name_col,
                            code_col=code_col)
            else:
                df_single = pd.concat([df_single,
                        read_runoff_data_from_single_river_xlsx(
                            filename=file_path,
                            date_col=date_col,
                            discharge_col=discharge_col,
                            name_col=name_col,
                            code_col=code_col)],
                        axis=0)

    # Combine the data from multiple and single rivers
    if df is None and df_single is not None:
        df = df_single
    elif df is not None and df_single is not None:
        df = pd.concat([df, df_single], axis=0)
    else:
        # df is not None and df_single is None. Return df.
        logger.warning("No data found in the daily discharge directory")

    return df

def process_hydro_HF_data(data):
    """
    Processes the API response to extract hydro data into a Pandas DataFrame.

    Args:
        data (dict): The API response containing hydro data.

    Returns:
        pandas.DataFrame: A DataFrame containing the processed hydro data.

    Note:
        Records with null/None values are filtered out as they represent
        dates without actual measurements.
        
        IMPORTANT: This function should receive ALL API results aggregated
        from all pages. Sites are classified based on their station_type
        across all results:
        - Hydro-only: Sites that only appear with station_type='hydro'
        - Dual-type: Sites that appear with BOTH 'hydro' and 'meteo' types
        - Meteo-only: Sites that ONLY appear with station_type='meteo'
        
        Only hydro records are kept. Meteo-only sites are logged for awareness.
    """
    results = data.get('results', [])

    # Categorize results by station_type and collect site codes
    hydro_sites = set()
    meteo_sites = set()

    for site_data in results:
        st = site_data.get('station_type', 'unknown')
        code = site_data.get('station_code', '?')
        if st == 'hydro':
            hydro_sites.add(code)
        elif st == 'meteo':
            meteo_sites.add(code)

    # Calculate site categories
    dual_type_sites = hydro_sites & meteo_sites  # Sites with BOTH hydro and meteo
    hydro_only_sites = hydro_sites - meteo_sites  # Sites with ONLY hydro
    meteo_only_sites = meteo_sites - hydro_sites  # Sites with ONLY meteo (no discharge data)
    
    total_unique_sites = len(hydro_sites | meteo_sites)

    logger.debug(f"[DATA] process_hydro_HF_data: Total site-records: {len(results)}")
    logger.info(f"[DATA] Site classification across ALL pages: "
                f"{total_unique_sites} unique sites = "
                f"{len(hydro_only_sites)} hydro-only + "
                f"{len(dual_type_sites)} dual-type + "
                f"{len(meteo_only_sites)} meteo-only")
    
    # Log dual-type sites at debug level (for awareness that these sites have both)
    if dual_type_sites:
        logger.debug(f"[DATA] Dual-type sites (have both hydro & meteo data): {sorted(dual_type_sites)}")

    # Log meteo-only sites (these are sites that truly have NO discharge data)
    if meteo_only_sites:
        logger.info(f"[DATA] Meteo-only sites (no discharge data): {sorted(meteo_only_sites)}")

    hydro_data = [
        site_data for site_data in results
        if site_data.get('station_type') == 'hydro'
    ]

    processed_data = []
    skipped_null_count = 0

    for site in hydro_data:
        station_code = site['station_code']
        station_id = site['station_id']
        station_name = site['station_name']

        for data_item in site['data']:
            variable_code = data_item['variable_code']
            unit = data_item['unit']

            for value_item in data_item['values']:
                # Skip records with null values (no actual measurement)
                if value_item['value'] is None:
                    skipped_null_count += 1
                    continue

                processed_data.append({
                    'station_code': station_code,
                    'station_id': station_id,
                    'station_name': station_name,
                    'variable_code': variable_code,
                    'unit': unit,
                    'local_datetime': value_item['timestamp_local'],
                    'utc_datetime': value_item['timestamp_utc'],
                    'value': value_item['value'],
                    'value_code': value_item['value_code'],
                    'value_type': value_item['value_type']
                })

    if skipped_null_count > 0:
        logger.debug(f"[DATA] process_hydro_HF_data: Skipped {skipped_null_count} records with null values")

    logger.debug(f"[DATA] process_hydro_HF_data: Final processed records: {len(processed_data)} from {len(hydro_sites)} hydro sites")

    return pd.DataFrame(processed_data)


def log_data_retrieval_summary(
    variable_name: str,
    requested_sites: list,
    sites_with_data: set,
    total_records: int,
    date_range: tuple = None
):
    """
    Log a summary of data retrieval results with appropriate log levels.

    Parameters:
    ----------
    variable_name : str
        The variable being fetched (e.g., 'WDDA', 'WDD')
    requested_sites : list
        List of site codes that were requested
    sites_with_data : set
        Set of site codes that returned data
    total_records : int
        Total number of records retrieved
    date_range : tuple, optional
        (start_date, end_date) tuple for the query
    """
    missing_sites = set(requested_sites) - sites_with_data
    coverage_pct = (len(sites_with_data) / len(requested_sites) * 100) if requested_sites else 0
    missing_pct = 100 - coverage_pct

    # Build date range string if provided
    date_str = f" ({date_range[0]} to {date_range[1]})" if date_range else ""

    # Main summary line (INFO)
    logger.info(
        f"[API] Response {variable_name}{date_str}: "
        f"{total_records} records from {len(sites_with_data)}/{len(requested_sites)} sites ({coverage_pct:.1f}%)"
    )

    # Warning when >20% of sites return no data
    if missing_pct > 20:
        logger.warning(
            f"[API] High data loss for {variable_name}: {missing_pct:.1f}% of sites "
            f"({len(missing_sites)}/{len(requested_sites)}) returned no data"
        )

    # Log missing sites (WARNING level if any missing)
    if missing_sites:
        sorted_missing = sorted(missing_sites)
        logger.warning(f"[API] Sites without {variable_name} data: {sorted_missing}")


# Backwards compatibility alias
print_data_retrieval_summary = log_data_retrieval_summary


def fetch_hydro_HF_data_robust(sdk, filters, site_codes, batch_size=10, max_workers=5):
    """
    Robustly fetch data from iEasyHydro HF API with automatic batching and fallback.

    The API has complex limits based on combination of:
    - Number of site codes
    - Date range length
    - Page size

    This function handles these limits by:
    1. Trying bulk request first (all site codes at once)
    2. If that fails, batching site codes into smaller groups
    3. If batches still fail, falling back to individual requests

    Parameters:
    ----------
    sdk : IEasyHydroHFSDK
        The SDK HF client instance
    filters : dict
        Base filters (variable_names, date range, etc.) - without site_codes
    site_codes : list
        List of site code strings to fetch data for
    batch_size : int
        Number of site codes per batch (default 10, which is API's safe limit)
    max_workers : int
        Max parallel workers for individual fallback requests

    Returns:
    -------
    pandas.DataFrame
        Combined DataFrame with all fetched data
        
    Raises:
    -------
    RuntimeError: If API returns 422 or other error (DEBUG MODE - fail loudly)
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Validate inputs
    if not site_codes:
        logger.warning("fetch_hydro_HF_data_robust called with empty site_codes list!")
        return pd.DataFrame()

    if not isinstance(site_codes, list):
        logger.warning(f"site_codes is not a list, got: {type(site_codes)}")
        site_codes = list(site_codes) if site_codes else []

    # Check if site_codes contains strings
    non_string_codes = [c for c in site_codes if not isinstance(c, str)]
    if non_string_codes:
        logger.warning(f"site_codes contains non-string values: {non_string_codes[:5]}... Converting to strings.")
        site_codes = [str(c) for c in site_codes]

    # Extract request details for logging
    variable_names = filters.get('variable_names', ['unknown'])
    var_str = variable_names[0] if variable_names else 'unknown'
    date_start = filters.get('local_date_time__gte', 'N/A')
    date_end = filters.get('local_date_time__lte', 'N/A')

    logger.info(f"[API] Request: {len(site_codes)} sites, {var_str}, {date_start} to {date_end}")
    logger.debug(f"[API] Site codes: {site_codes[:10]}{'...' if len(site_codes) > 10 else ''}")

    # Remove page_size from filters - let API use default (10) which is most reliable
    safe_filters = {k: v for k, v in filters.items() if k != 'page_size'}

    all_dataframes = []

    def fetch_single_site(site_code):
        """Fetch data for a single site."""
        single_filters = safe_filters.copy()
        single_filters['site_codes'] = [site_code]
        try:
            return fetch_and_format_hydro_HF_data(sdk, single_filters)
        except Exception as e:
            logger.error(f"Failed to fetch data for site {site_code}: {e}")
            raise  # Re-raise for debugging

    def fetch_batch(batch_codes):
        """Fetch data for a batch of sites."""
        batch_filters = safe_filters.copy()
        batch_filters['site_codes'] = batch_codes  # Use site_codes (canonical filter parameter)
        return fetch_and_format_hydro_HF_data(sdk, batch_filters)

    # Strategy 1: Try bulk request with all site codes
    logger.debug(f"[API] Attempting bulk request for {len(site_codes)} sites...")
    bulk_filters = safe_filters.copy()
    bulk_filters['site_codes'] = site_codes

    response = sdk.get_data_values_for_site(filters=bulk_filters)

    # Log the response type and key info for debugging
    if isinstance(response, dict):
        if 'status_code' in response:
            status_code = response.get('status_code')
            error_text = response.get('text', 'No text')[:500]
            logger.error(f"[API] Bulk request failed with status {status_code}: {error_text}")
            raise RuntimeError(f"Bulk request failed with status {status_code}: {error_text}")
        else:
            total_count = response.get('count', 'N/A')
            results_count = len(response.get('results', []))
            logger.debug(f"[API] Bulk response: count={total_count}, first_page={results_count} records")
    else:
        logger.warning(f"[API] Unexpected response type: {type(response)}")

    if not (isinstance(response, dict) and 'status_code' in response):
        # Bulk request succeeded - use the standard fetch function
        logger.debug("[API] Bulk request succeeded, fetching all pages...")
        result_df = fetch_and_format_hydro_HF_data(sdk, bulk_filters)

        # Extract info for summary report
        variable_name = filters.get('variable_names', ['unknown'])[0] if filters.get('variable_names') else 'unknown'
        date_start = filters.get('local_date_time__gte', 'N/A')
        date_end = filters.get('local_date_time__lte', 'N/A')

        # Get sites with data
        if not result_df.empty and 'station_code' in result_df.columns:
            sites_with_data = set(result_df['station_code'].unique())
        else:
            sites_with_data = set()

        # Print summary report
        print_data_retrieval_summary(
            variable_name=variable_name,
            requested_sites=site_codes,
            sites_with_data=sites_with_data,
            total_records=len(result_df) if not result_df.empty else 0,
            date_range=(date_start, date_end)
        )

        return result_df

    # Strategy 2: Bulk failed - try batching (should not reach here in debug mode)
    logger.info(f"[API] Bulk failed, trying batched requests (batch_size={batch_size})...")

    batches = [site_codes[i:i + batch_size] for i in range(0, len(site_codes), batch_size)]
    batch_failed = False

    for i, batch in enumerate(batches):
        logger.debug(f"[API] Fetching batch {i+1}/{len(batches)} ({len(batch)} sites)...")
        try:
            batch_df = fetch_batch(batch)
            if not batch_df.empty:
                all_dataframes.append(batch_df)
                logger.debug(f"[API] Batch {i+1}: {len(batch_df)} records")
        except Exception as e:
            logger.error(f"[API] Batch {i+1} failed: {e}")
            raise  # Re-raise for debugging

    if not batch_failed and all_dataframes:
        result_df = pd.concat(all_dataframes, ignore_index=True) if all_dataframes else pd.DataFrame()
        logger.debug(f"[API] Batched requests succeeded: {len(result_df)} total records")

        # Print summary report
        variable_name = filters.get('variable_names', ['unknown'])[0] if filters.get('variable_names') else 'unknown'
        date_start = filters.get('local_date_time__gte', 'N/A')
        date_end = filters.get('local_date_time__lte', 'N/A')
        sites_with_data = set(result_df['station_code'].unique()) if not result_df.empty and 'station_code' in result_df.columns else set()
        print_data_retrieval_summary(
            variable_name=variable_name,
            requested_sites=site_codes,
            sites_with_data=sites_with_data,
            total_records=len(result_df) if not result_df.empty else 0,
            date_range=(date_start, date_end)
        )

        return result_df

    # Strategy 3: Batches failed - fall back to parallel individual requests
    logger.info(f"[API] Batches failed, falling back to individual requests ({max_workers} workers)...")
    all_dataframes = []  # Reset

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_single_site, code): code for code in site_codes}
        completed = 0
        for future in as_completed(futures):
            completed += 1
            if completed % 20 == 0:
                logger.info(f"[API] Progress: {completed}/{len(site_codes)} sites...")
            try:
                df = future.result()
                if not df.empty:
                    all_dataframes.append(df)
            except Exception as e:
                logger.error(f"[API] Error fetching {futures[future]}: {e}")
                raise  # Re-raise for debugging

    logger.debug(f"[API] Individual requests completed: data from {len(all_dataframes)} sites")

    if all_dataframes:
        result_df = pd.concat(all_dataframes, ignore_index=True)
    else:
        result_df = pd.DataFrame()

    # Print summary report
    variable_name = filters.get('variable_names', ['unknown'])[0] if filters.get('variable_names') else 'unknown'
    date_start = filters.get('local_date_time__gte', 'N/A')
    date_end = filters.get('local_date_time__lte', 'N/A')
    sites_with_data = set(result_df['station_code'].unique()) if not result_df.empty and 'station_code' in result_df.columns else set()
    print_data_retrieval_summary(
        variable_name=variable_name,
        requested_sites=site_codes,
        sites_with_data=sites_with_data,
        total_records=len(result_df) if not result_df.empty else 0,
        date_range=(date_start, date_end)
    )

    return result_df


def fetch_and_format_hydro_HF_data(sdk, initial_filters):
    """
    Fetch all pages of data from the API and format into a DataFrame.
    
    Uses parallel fetching for improved performance:
    1. Fetch page 1 to get total count
    2. Calculate total pages (page_size=10 is API hard limit)
    3. Fetch remaining pages in parallel
    4. Aggregate ALL results before processing (fixes meteo-only misclassification)
    
    Parameters:
    ----------
    sdk : IEasyHydroHFSDK
        The SDK HF client instance for making API requests
    initial_filters : dict
        The initial filters to use for the request
        
    Returns:
    -------
    pandas.DataFrame
        DataFrame with all hydro data in a long format with columns:
        station_code, timestamp_local, timestamp_utc, value
        
    Raises:
    -------
    RuntimeError: If API returns 422 or other error status codes
    
    Note:
    -----
    The API can return the same site with different station_type values across pages
    (e.g., site X as 'hydro' on page 1, and as 'meteo' on page 3). We aggregate ALL
    results before classification to ensure accurate meteo-only detection.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    PAGE_SIZE = 10  # API hard limit - cannot be changed
    
    # Copy filters and set page size
    filters = initial_filters.copy()
    filters['page_size'] = PAGE_SIZE
    
    all_raw_results = []  # Collect raw API results from all pages
    failed_pages = []
    
    def fetch_page_raw(page_num):
        """Fetch a single page and return raw results (not processed)."""
        page_filters = filters.copy()
        page_filters['page'] = page_num
        
        with ProfileTimer(f"sdk_api_call_page_{page_num}", log_immediately=False):
            response = sdk.get_data_values_for_site(filters=page_filters)
        
        # Check for error - raise exception instead of silently continuing
        if isinstance(response, dict) and 'status_code' in response:
            status_code = response.get('status_code')
            error_text = response.get('text', 'No error text')[:500]
            error_msg = f"Page {page_num} failed with status {status_code}: {error_text}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        # Return raw results list (not processed yet)
        if isinstance(response, dict) and 'results' in response:
            return response.get('results', [])
        return []
    
    # Step 1: Fetch first page to get total count
    logger.debug(f"[API] COUNT VALIDATION: Fetching page 1 to determine total count...")
    logger.debug(f"[API] COUNT VALIDATION: Filters: {filters}")
    filters['page'] = 1
    
    with ProfileTimer("sdk_api_call_page_1", log_immediately=True):
        first_response = sdk.get_data_values_for_site(filters=filters)
    
    # Check for error on first page - RAISE EXCEPTION
    if isinstance(first_response, dict) and 'status_code' in first_response:
        status_code = first_response.get('status_code')
        error_text = first_response.get('text', 'No error text')[:500]
        error_msg = f"First page request failed with status {status_code}: {error_text}"
        logger.error(f"[API] COUNT VALIDATION ERROR: {error_msg}")
        raise RuntimeError(error_msg)
    
    # Get total count and calculate pages
    total_count = first_response.get('count', 0) if isinstance(first_response, dict) else 0
    total_pages = (total_count + PAGE_SIZE - 1) // PAGE_SIZE
    
    logger.debug(f"[API] COUNT VALIDATION: API reports total_count={total_count}, total_pages={total_pages}")
    logger.info(f"[API] Total records: {total_count}, Total pages: {total_pages}")
    
    # Collect raw results from first page
    if isinstance(first_response, dict) and 'results' in first_response:
        page1_results = first_response.get('results', [])
        all_raw_results.extend(page1_results)
        logger.debug(f"[API] COUNT VALIDATION: Page 1: {len(page1_results)} site-records collected")
    
    # Step 2: Fetch remaining pages in parallel (collect raw results)
    if total_pages > 1:
        pages_to_fetch = list(range(2, total_pages + 1))
        logger.debug(f"[API] COUNT VALIDATION: Fetching pages 2-{total_pages} in parallel...")
        
        with ProfileTimer(f"parallel_fetch_pages_2_to_{total_pages}", log_immediately=True):
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(fetch_page_raw, p): p for p in pages_to_fetch}
                
                for future in as_completed(futures):
                    page_num = futures[future]
                    try:
                        page_results = future.result()
                        if page_results:
                            all_raw_results.extend(page_results)
                    except Exception as e:
                        failed_pages.append(page_num)
                        logger.error(f"Error fetching page {page_num}: {e}")
                        # Re-raise to stop execution
                        raise
        
        logger.debug(f"[API] COUNT VALIDATION: Parallel fetch complete: {len(all_raw_results)} total site-records collected")

    if failed_pages:
        logger.error(f"[API] COUNT VALIDATION ERROR: Failed pages: {failed_pages}")
        raise RuntimeError(f"Failed to fetch pages: {failed_pages}")
    
    # Step 3: Process ALL results together (fixes meteo-only misclassification)
    # This ensures a site is only classified as "meteo-only" if it has NO hydro
    # records across ALL pages, not just a single page
    if all_raw_results:
        combined_response = {'results': all_raw_results}
        combined_df = process_hydro_HF_data(combined_response)
        
        # Convert timestamps to datetime if they're strings
        for col in ['local_datetime', 'utc_datetime']:
            if col in combined_df.columns and combined_df[col].dtype == 'object':
                combined_df[col] = pd.to_datetime(combined_df[col])
        
        if combined_df.empty:
            return pd.DataFrame(columns=['station_code', 'local_datetime', 'utc_datetime', 'value'])
        
        # Drop columns that are not in the expected structure
        combined_df.drop(
            columns=['station_uuid', 'station_type', 'station_id', 
                     'variable_code', 'unit', 'value_type', 'value_code', 
                     'station_name'], 
            inplace=True, errors='ignore')
        
        # Final count validation
        unique_sites = combined_df['station_code'].nunique() if 'station_code' in combined_df.columns else 0
        logger.debug(f"[API] COUNT VALIDATION: Final DataFrame: {len(combined_df)} records from {unique_sites} unique sites")
        logger.info(f"[API] Total records fetched: {len(combined_df)} from {unique_sites} sites")
        return combined_df
    else:
        logger.debug(f"[API] COUNT VALIDATION: No results to process - returning empty DataFrame")
        return pd.DataFrame(columns=['station_code', 'local_datetime', 'utc_datetime', 'value'])

def get_todays_morning_discharge_from_iEH_HF_for_multiple_sites(
        ieh_hf_sdk, id_list, start_datetime=None, end_datetime=None,
        target_timezone=None,  
        date_col='date', discharge_col='discharge', code_col='code'):
    """
    Reads todays morning discharge data from the iEasyHydro database for multiple sites.
    
    Uses UTC datetime objects that represent local times (8:00 to current hour) for filtering.
    According to SDK documentation, the backend ignores timezone and searches for local times.

    Args:
        ieh_hf_sdk (object): An object that provides a method to get data values for a site from a database.
        id_list (list): A list of strings denoting site ids.
        start_datetime (datetime, optional): The start datetime as UTC (interpreted as local time). Defaults to None. 
        end_datetime (datetime, optional): The end datetime as UTC (interpreted as local time). Defaults to None.
        target_timezone (str, optional): The timezone used for calculating current time. Defaults to None.
        date_col (str, optional): The name of the column containing the date data. Default is 'date'.
        discharge_col (str, optional): The name of the column containing the discharge data. Default is 'discharge'.
        code_col (str, optional): The name of the column containing the site code. Default is 'code'.

    Returns:
        pandas.DataFrame: A DataFrame containing the morning discharge data. If multiple measurements
                         are found for the current day, they are averaged.

    Raises:
        ValueError: If the site code is not a string or an integer.
        ValueError: If the site name is not a string.
    """
    # Test is id_list is a list
    if not isinstance(id_list, list):
        raise ValueError("The sites_list must be a list of strings.")
    # Test if id_list is empty
    if not id_list:
        raise ValueError("The id_list must not be empty.")

    # Test if target_timezone is None
    if target_timezone is None:
        # Get the local timezone
        target_timezone = pytz.timezone(time.tzname[0])
        logger.debug(f"Target timezone is None. Using local timezone: {target_timezone}")
        
    # If start_datetime is None, get it from get_local_time_range_for_todays_morning_runoff_request
    if start_datetime is None or end_datetime is None:
        start_datetime, end_datetime = get_local_time_range_for_todays_morning_runoff_request(target_timezone)

    # Raise an error if the date format is not a datetime with timezone info
    if not isinstance(start_datetime, dt.datetime):
        raise ValueError("The start_datetime must be a datetime object with timezone info.")
    if not isinstance(end_datetime, dt.datetime):
        raise ValueError("The end_datetime must be a datetime object with timezone info.")

    logger.debug(f"Reading morning discharge data for all sites from {start_datetime} to {end_datetime} (UTC as local time).")

    # Build base filters (without site_codes - those are passed separately to robust function)
    filters = {
        "variable_names": ["WDD"],
        "local_date_time__gte": start_datetime.isoformat(),
        "local_date_time__lte": end_datetime.isoformat(),  # Use __lte to include end date
    }

    try:
        # Get data for all sites using robust fetching with automatic batching/fallback
        with ProfileTimer("fetch_WDD_morning_discharge", log_immediately=True):
            db_df = fetch_hydro_HF_data_robust(ieh_hf_sdk, filters, site_codes=id_list)
        if db_df.empty:
            logger.info("No morning data found for the given date range.")
            return pd.DataFrame(columns=[date_col, discharge_col, code_col])

        # Drop the local datetime column as we will be working with utc datetime
        db_df.drop(columns=['local_datetime'], inplace=True)

        # Rename the columns of df to match the columns of combined_data
        db_df.rename(
            columns={
                'utc_datetime': date_col,
                'station_code': code_col,
                'value': discharge_col},
            inplace=True
        )

        # If there are multiple measurements for the current day, take the average
        db_df[date_col] = pd.to_datetime(db_df[date_col]).dt.date
        db_df = db_df.groupby([code_col, date_col])[discharge_col].mean().reset_index()

    except Exception as e:
        logger.info(f"Error reading morning discharge data: {e}")
        logger.info(f"Returning empty data frame.")
        # Return an empty dataframe with columns 'date', 'discharge', 'code'
        db_df = pd.DataFrame(columns=[date_col, discharge_col, code_col])

    return db_df

def get_daily_average_discharge_from_iEH_HF_for_multiple_sites(
        ieh_hf_sdk, id_list, start_datetime=None, end_datetime=None,
        target_timezone=None,  
        date_col='date', discharge_col='discharge', code_col='code'):
    """
    Reads daily average discharge data from the iEasyHydro database for multiple sites.
    
    Uses UTC datetime objects that represent local times (20:00 evening) for filtering.
    According to SDK documentation, the backend ignores timezone and searches for local times.

    Args:
        ieh_hf_sdk (object): An object that provides a method to get data values for a site from a database.
        id_list (list): A list of strings denoting site ids.
        start_datetime (datetime, optional): The start datetime as UTC (interpreted as local time). Defaults to None. 
        end_datetime (datetime, optional): The end datetime as UTC (interpreted as local time). Defaults to None.
        target_timezone (str, optional): The timezone used for calculating current time. Defaults to None.
        date_col (str, optional): The name of the column containing the date data. Default is 'date'.
        discharge_col (str, optional): The name of the column containing the discharge data. Default is 'discharge'.
        code_col (str, optional): The name of the column containing the site code. Default is 'code'.

    Returns:
        pandas.DataFrame: A DataFrame containing the daily average discharge data.

    Raises:
        ValueError: If the site code is not a string or an integer.
        ValueError: If the site name is not a string.
    """
    # Test is id_list is a list
    if not isinstance(id_list, list):
        raise ValueError("The sites_list must be a list of strings.")
    # Test if id_list is empty
    if not id_list:
        raise ValueError("The id_list must not be empty.")

    # Test if target_timezone is None
    if target_timezone is None:
        # Get the local timezone
        target_timezone = pytz.timezone(time.tzname[0])
        logger.debug(f"Target timezone is None. Using local timezone: {target_timezone}")
        
    # If start_datetime is None, get it from get_local_time_range_for_daily_average_runoff_request
    if start_datetime is None or end_datetime is None:
        start_datetime, end_datetime = get_local_time_range_for_daily_average_runoff_request(target_timezone)

    # Raise an error if the date format is not a datetime with timezone info
    if not isinstance(start_datetime, dt.datetime):
        raise ValueError("The start_datetime must be a datetime object with timezone info.")
    if not isinstance(end_datetime, dt.datetime):
        raise ValueError("The end_datetime must be a datetime object with timezone info.")

    logger.debug(f"Reading daily average discharge data for all sites from {start_datetime} to {end_datetime} (UTC as local time).")

    # Build base filters (without site_codes - those are passed separately to robust function)
    filters = {
        "variable_names": ["WDDA"],
        "local_date_time__gte": start_datetime.isoformat(),
        "local_date_time__lte": end_datetime.isoformat(),  # Use __lte to include end date
    }

    # Debug: Log what we're about to fetch
    logger.debug(f"[API] get_daily_average_discharge_from_iEH_HF_for_multiple_sites:")
    logger.debug(f"[API]   id_list type: {type(id_list)}, length: {len(id_list) if id_list else 'None'}")
    logger.debug(f"[API]   id_list first 5: {id_list[:5] if id_list else 'None'}")
    logger.debug(f"[API]   date range: {start_datetime} to {end_datetime}")

    try:
        # Get data for all sites using robust fetching with automatic batching/fallback
        with ProfileTimer("fetch_WDDA_daily_average", log_immediately=True):
            db_df = fetch_hydro_HF_data_robust(ieh_hf_sdk, filters, site_codes=id_list)

        logger.debug(f"[API]   db_df shape: {db_df.shape if not db_df.empty else 'empty'}")
        if not db_df.empty:
            logger.debug(f"[API]   db_df columns: {db_df.columns.tolist()}")
            logger.debug(f"[API]   db_df head:\n{db_df.head()}")

        if db_df.empty:
            logger.info(f"No daily average data found for the given date range.")
            return pd.DataFrame(columns=[date_col, discharge_col, code_col])

        # Drop the local datetime column as we will be working with utc datetime
        db_df.drop(columns=['local_datetime'], inplace=True)

        # Rename the columns of df to match the columns of combined_data
        db_df.rename(
            columns={
                'utc_datetime': date_col,
                'station_code': code_col,
                'value': discharge_col},
            inplace=True
        )

    except Exception as e:
        logger.info(f"Error reading daily average discharge data: {e}")
        logger.info(f"Returning empty data frame.")
        # Return an empty dataframe with columns 'date', 'discharge', 'code'
        db_df = pd.DataFrame(columns=[date_col, discharge_col, code_col])

    return db_df


def get_daily_average_discharge_from_iEH_per_site(
        ieh_sdk, site, name, start_date, end_date=dt.date.today(),
        date_col='date', discharge_col='discharge', name_col='name', code_col='code'):
    """
    Reads daily average discharge data from the iEasyHydro database for a given site.

    The names of the dataframe columns can be customized.

    Args:
        ieh_sdk (object): An object that provides a method to get data values for a site from a database.
        site (str or int): The site code.
        name (str): The name of the site.
        start_date (datetime.date or str): The start date of the data to read.
        end_date (datetime.date or str, optional): The end date of the data to read. Defaults to dt.date.today().
        date_col (str, optional): The name of the column containing the date data. Default is 'date'.
        discharge_col (str, optional): The name of the column containing the discharge data. Default is 'discharge'.
        name_col (str, optional): The name of the column containing the site name. Default is 'name'.
        code_col (str, optional): The name of the column containing the site code. Default is 'code'.

    Returns:
        pandas.DataFrame: A DataFrame containing the daily average discharge data.

    Raises:
        ValueError: If the site code is not a string or an integer.
        ValueError: If the site name is not a string.
    """
    # Convert site to string if necessary
    if isinstance(site, int):
        site = str(site)
    # Throw an error if the site is not a string
    if not isinstance(site, str):
        raise ValueError("The site code must be a string or an integer.")

    # Convert start_date to dt.datetime
    if isinstance(start_date, str):
        start_date = dt.datetime.strptime(start_date, '%Y-%m-%d')
    if isinstance(start_date, dt.date):
        start_date = dt.datetime.combine(start_date, dt.datetime.min.time())

    # Convert end_date to dt.datetime
    if isinstance(end_date, str):
        end_date = dt.datetime.strptime(end_date, '%Y-%m-%d')
    if isinstance(end_date, dt.date):
        end_date = dt.datetime.combine(end_date, dt.datetime.min.time())

    # Test if name is a string
    if not isinstance(name, str):
        raise ValueError("The site name must be a string.")

    logger.debug(f"Reading daily average discharge data for site {site} from {start_date} to {end_date}.")

    filter = BasicDataValueFilters(
        local_date_time__gte=start_date,
        local_date_time__lt=end_date
    )

    try:
        # Get data for current site from the database
        db_raw = ieh_sdk.get_data_values_for_site(
            site,
            'discharge_daily_average',
            filters=filter
        )

        db_raw = db_raw['data_values']

        # Create a DataFrame
        db_df = pd.DataFrame(db_raw)

        # Rename the columns of df to match the columns of combined_data
        db_df = db_df.rename(columns={'local_date_time': date_col, 'data_value': discharge_col})

        # Drop the columns we don't need
        db_df.drop(columns=['utc_date_time'], inplace=True)

        # Convert the Date column to datetime
        db_df[date_col] = pd.to_datetime(db_df['date'], format='%Y-%m-%d %H:%M:%S').dt.date

        # Add the name and code columns
        db_df[name_col] = name
        db_df[code_col] = site

    except Exception as e:
        logger.debug(f"Error reading daily average discharge data for site {site}: {e}")
        logger.info(f"Skip reading daily average discharge data for site {site}")
        # Return an empty dataframe with columns 'date', 'discharge', 'name', 'code'
        db_df = pd.DataFrame(columns=[date_col, discharge_col, name_col, code_col])

    return db_df

def get_todays_morning_discharge_from_iEH_per_site(
        ieh_sdk, site, name,
        date_col='date', discharge_col='discharge', name_col='name', code_col='code'):
    """
    Reads river discharge data from the iEasyHydro database for a given site that
    was measured today.

    The names of the dataframe columns can be customized.

    Args:
        ieh_sdk (object): An object that provides a method to get data values for a site from a database.
        site (str or int): The site code.
        name (str): The name of the site.
        date_col (str, optional): The name of the column containing the date data. Default is 'date'.
        discharge_col (str, optional): The name of the column containing the discharge data. Default is 'discharge'.
        name_col (str, optional): The name of the column containing the site name. Default is 'name'.
        code_col (str, optional): The name of the column containing the site code. Default is 'code'.

    Returns:
        pandas.DataFrame: A DataFrame containing the river discharge data.

    Raises:
        ValueError: If the site code is not a string or an integer.
        ValueError: If the site name is not a string.
    """
    # Convert site to string if necessary
    if isinstance(site, int):
        site = str(site)
    # Throw an error if the site is not a string
    if not isinstance(site, str):
        raise ValueError("The site code must be a string or an integer.")

    # The morning measurement is taken at 8 o'clock. The datetime for this
    # measurement in the iEasyHydro database can vary by a few hours so we filter
    # for measurements between 00:00 and 12:00 today.
    today_startday = dt.datetime.combine(dt.date.today(), dt.datetime.min.time())
    today_noon = dt.datetime.combine(dt.date.today(), dt.time(12, 0))

    # Test if name is a string
    if not isinstance(name, str):
        raise ValueError("The site name must be a string.")

    logger.debug(f"Reading daily average discharge data for site {site}")

    filter = BasicDataValueFilters(
        local_date_time__gte=today_startday,
        local_date_time__lte=today_noon
    )

    try:
        # Get data for current site from the database
        db_raw = ieh_sdk.get_data_values_for_site(
            site,
            'discharge_daily',
            filters=filter
        )

        db_raw = db_raw['data_values']

        # Create a DataFrame
        db_df = pd.DataFrame(db_raw)

        # Rename the columns of df to match the columns of combined_data
        db_df = db_df.rename(columns={'local_date_time': date_col, 'data_value': discharge_col})

        # Drop the columns we don't need
        db_df.drop(columns=['utc_date_time'], inplace=True)

        # Convert the Date column to datetime
        db_df[date_col] = pd.to_datetime(db_df[date_col], format='%Y-%m-%d %H:%M:%S').dt.date

        # Add the name and code columns
        db_df[name_col] = name
        db_df[code_col] = site

    except Exception as e:
        logger.info(f"Skip reading morning measurement of discharge data for site {site}")
        # Return an empty dataframe with columns 'date', 'discharge', 'name', 'code'
        db_df = pd.DataFrame(columns=[date_col, discharge_col, name_col, code_col])

    return db_df

def add_hydroposts(combined_data, check_hydroposts):
    """
    Check if the virtual hydroposts are in the combined_data and add them if not.

    This function checks if the virtual hydroposts are in the combined_data and
    adds them if they are not. The virtual hydroposts are '15960' (Inflow to the
    Orto-Tokoy reservoir), '15954' (Inflow to the Kirov reservoir), and '16936'
    (Inflow to the Toktogul reservoir).

    Args:
    combined_data (pd.DataFrame): The input DataFrame. Must contain 'code' column.
    check_hydroposts (list): A list of the virtual hydroposts to check for.

    Returns:
    pd.DataFrame: The input DataFrame with the virtual hydroposts added.

    """
    # Make a copy to avoid modifying the original
    data_df = combined_data.copy()
    
    # First remove any timezone information, then normalize
    data_df['date'] = pd.to_datetime(data_df['date']).dt.tz_localize(None).dt.normalize()
    
    # Get the earliest date
    earliest_date = pd.to_datetime(combined_data['date'].min())

    # Check if the virtual hydroposts are in the combined_data
    for hydropost in check_hydroposts:
        if hydropost not in combined_data['code'].values:
            logger.debug(f"Virtual hydropost {hydropost} is not in the list of stations.")
            # Add the virtual hydropost to the combined_data
            new_row = pd.DataFrame({
                'code': [hydropost],
                'date': [earliest_date],
                'discharge': [np.nan],
                'name': [f'Virtual hydropost {hydropost}']
            })
            combined_data = pd.concat([combined_data, new_row], ignore_index=True)

    return combined_data

def calculate_virtual_stations_data(data_df: pd.DataFrame,
                                    code_col='code', discharge_col='discharge',
                                    date_col='date'):
    """

    """
    # Get configuration for virtual stations
    with open(os.path.join(os.getenv('ieasyforecast_configuration_path'),
                           os.getenv('ieasyforecast_virtual_stations')), 'r') as f:
        json_data = json.load(f)
        virtual_stations = json_data['virtualStations'].keys()
        instructions = json_data['virtualStations']

    # Add the virtual stations to the data if they are not already there
    data_df = add_hydroposts(data_df, virtual_stations)

    # Iterate over the station IDs
    for station in virtual_stations:
        # Get the instructions for the station
        instruction = instructions[station]
        #print(instruction)
        weigth_by_station = instruction['weightByStation']
        #print(weigth_by_station)

        # Currently, we only implement the combination function 'sum'. Throw an error if the function is not 'sum'
        if instruction['combinationFunction'] != 'sum':
            logger.error(f"Combination function for station {station} is not 'sum'.")
            logger.error(f"Please implement the combination function '{instruction['combinationFunction']}' for station {station}.")
            exit()

        # Get the data for the stations that contribute to the virtual station and multiply them with the weight
        for contributing_station, weight in weigth_by_station.items():
            # Make sure the contributing station is not equal to the virtual station
            if contributing_station == station:
                logger.error(f"Virtual station {station} cannot contribute to itself.")
                exit()

            #print(contributing_station, weight)
            # Get the data for the contributing station
            data_contributing_station = data_df[data_df[code_col] == contributing_station].copy()

            # Multiply the discharge data with the weight
            data_contributing_station[discharge_col] = data_contributing_station[discharge_col] * weight

            # Add the data to the virtual station if data_virtual_station exists
            if 'data_virtual_station' not in locals():
                data_virtual_station = data_contributing_station
                # Change code to the code of the virtual station
                data_virtual_station[code_col] = station
                # Change the name to the name of the virtual station
                data_virtual_station['name'] = f'Virtual hydropost {station}'
            else:
                # Merge the data for the contributing station with the data_virtual_station
                data_virtual_station = pd.merge(data_virtual_station, data_contributing_station, on=date_col, how='outer', suffixes=('', '_y'))
                # Add discharge_y to discharge and discard all _y columns
                data_virtual_station[discharge_col] = data_virtual_station[discharge_col] + data_virtual_station[discharge_col + '_y']
                data_virtual_station.drop(columns=[col for col in data_virtual_station.columns if '_y' in col], inplace=True)

            #print("data_virtual_station.tail(10)\n", data_virtual_station.tail(10))

        # Check if we already have data for the virtual station in the data_df dataframe and fill gaps with data_virtual_station
        if station in data_df[code_col].values:
            # Get the data for the virtual station
            data_station = data_df[data_df[code_col] == station].copy()

            # Get the latest date for which we have data in the data_df for the virtual station
            last_date_station = data_station[date_col].max()

            # Get the data for the date from the other stations
            data_virtual_station = data_virtual_station[data_virtual_station[date_col] >= last_date_station].copy()

            # Merge the data for the virtual station with the data_df
            data_df = pd.concat([data_df, data_virtual_station], ignore_index=True)

        # Delete data_virtual_station
        del data_virtual_station

    return data_df

def get_runoff_data(ieh_sdk=None, date_col='date', discharge_col='discharge', name_col='name', code_col='code'):
    """
    Reads runoff data from excel and, if possible, from iEasyHydro database.

    Note: This function will only try to read data from the iEasyHydro database
    which are already in the excel files.

    Args:
        ieh_sdk (object): An object that provides a method to get data values
            for a site from a database. None in case of no access to the database.
        date_col (str, optional): The name of the column containing the date data.
            Default is 'date'.
        discharge_col (str, optional): The name of the column containing the discharge data.
            Default is 'discharge'.
        name_col (str, optional): The name of the column containing the site name.
            Default is 'name'.
        code_col (str, optional): The name of the column containing the site code.
            Default is 'code'.
    """
    # Read data from excel files
    read_data = read_all_runoff_data_from_excel(
        date_col=date_col,
        discharge_col=discharge_col,
        name_col=name_col,
        code_col=code_col)

    # Initialize a flag for virtual stations
    virtual_stations_present = False
    # Get virtual station codes from json (if file exists) print a warning if file
    # does not exist.
    if os.getenv('ieasyforecast_virtual_stations') is None:
        logger.info(f"No calculation rules for virtual stations found.\n"
                    f"Environment variable ieasyforecast_virtual_stations is not set.")
    else:
        virtual_stations_config_file_path = os.path.join(
            os.getenv('ieasyforecast_configuration_path'),
            os.getenv('ieasyforecast_virtual_stations'))
        if not os.path.exists(virtual_stations_config_file_path):
            raise FileNotFoundError(
                f"File {virtual_stations_config_file_path} not found.\n",
                f"Filename for calculateion rules for virtual stations in environment\n"
                f"but file not found.\n"
                f"Please provide a configuraion file ieasyforecast_virtual_stations\n"
                f"or, if you don't have any virtual stations to predict, remove the\n"
                f"variable ieasyforecast_virtual_stations from your configuration file."
            )
        else:
            with open(virtual_stations_config_file_path, 'r') as f:
                virtual_stations = json.load(f)['virtualStations'].keys()
            virtual_stations_present = True

            read_data = add_hydroposts(read_data, virtual_stations)

    if ieh_sdk is None:
        # We do not have access to an iEasyHydro database
        logger.info("No data read from iEasyHydro Database.")

        return read_data

    else:
        # Get the last row for each code in runoff_data
        last_row = read_data.groupby(code_col).tail(1)
        #print("DEBUG: last_row: \n", last_row)

        # For each code in last_row, get the daily average discharge data from the
        # iEasyHydro database using the function get_daily_average_discharge_from_iEH_per_site
        for index, row in last_row.iterrows():
            with suppress_stdout(): 
                db_average_data = get_daily_average_discharge_from_iEH_per_site(
                    ieh_sdk, row[code_col], row[name_col], row[date_col],
                    date_col=date_col, discharge_col=discharge_col, name_col=name_col, code_col=code_col
                )
            with suppress_stdout():
                db_morning_data = get_todays_morning_discharge_from_iEH_per_site(
                    ieh_sdk, row[code_col], row[name_col],
                    date_col=date_col, discharge_col=discharge_col, name_col=name_col, code_col=code_col)
            # Append db_data to read_data if db_data is not empty
            if not db_average_data.empty:
                read_data = pd.concat([read_data, db_average_data], ignore_index=True)
            if not db_morning_data.empty:
                read_data = pd.concat([read_data, db_morning_data], ignore_index=True)

        # Drop rows where 'code' is "NA"
        # Use .copy() to avoid SettingWithCopyWarning when modifying columns
        read_data = read_data[read_data[code_col] != 'NA'].copy()

        # Cast the 'code' column to string
        read_data[code_col] = read_data[code_col].astype(str)

        #print(read_data[read_data['code'] == "16936"].tail(10))
        #print(read_data[read_data['code'] == "16059"].tail(10))
        # Calculate virtual hydropost data where necessary
        if virtual_stations_present:
            read_data = calculate_virtual_stations_data(read_data)
        #print(read_data[read_data['code'] == "16936"].tail(10))
        #print(read_data[read_data['code'] == "16059"].tail(10))

        # For sanity sake, we round the data to a mac of 3 decimal places
        read_data[discharge_col] = read_data[discharge_col].round(3)

        return read_data

def get_runoff_data_for_sites(ieh_sdk=None, date_col='date',
                              discharge_col='discharge', name_col='name',
                              code_col='code', site_list=None, code_list=None):
    """
    Reads runoff data from excel and, if possible, from iEasyHydro database.

    Note: This function will only try to read data from the iEasyHydro database
    which are already in the excel files.

    Args:
        ieh_sdk (object): An object that provides a method to get data values
            for a site from a database. None in case of no access to the database.
        date_col (str, optional): The name of the column containing the date data.
            Default is 'date'.
        discharge_col (str, optional): The name of the column containing the discharge data.
            Default is 'discharge'.
        name_col (str, optional): The name of the column containing the site name.
            Default is 'name'.
        code_col (str, optional): The name of the column containing the site code.
            Default is 'code'.
    """
    # Test if there have been any changes in the daily_discharge directory
    if should_reprocess_input_files(): 
        logger.info("Regime data has changed, reprocessing input files.")
    
        # Get organization from environment variable
        organization = os.getenv('ieasyhydroforecast_organization')

        if organization=='kghm': 
            # Read data from excel files
            read_data = read_all_runoff_data_from_excel(
                date_col=date_col,
                discharge_col=discharge_col,
                name_col=name_col,
                code_col=code_col,
                code_list=code_list)
        elif organization=='tjhm':
            # Read data from csv files
            read_data = read_all_runoff_data_from_csv(
                date_col=date_col,
                discharge_col=discharge_col,
                name_col=name_col,
                code_col=code_col,
                code_list=code_list)
        else:
            # Raise an error if the organization is not recognized
            raise ValueError(f"Organization '{organization}' not recognized. "
                             f"Please set the environment variable 'ieasyhydroforecast_organization' to 'kghm' or 'tjhm'.")
    else:
        logger.info("No changes in the daily_discharge directory, using previous data.")
        intermediate_data_path = os.getenv('ieasyforecast_intermediate_data_path')
        output_file_path = os.path.join(
            intermediate_data_path,
            os.getenv("ieasyforecast_daily_discharge_file"))
        try:
            read_data = pd.read_csv(output_file_path)
            read_data['date'] = pd.to_datetime(read_data['date']).dt.date
            read_data['code'] = read_data['code'].astype(str)
            read_data['discharge'] = read_data['discharge'].astype(float)
            # Get a dummy name column 
            read_data['name'] = read_data['code']
            # Check the latest date. If it is before today minus 51 days, go to 
            # the fallback and read in all regime data. 
            # This is to avoid shortening of the regime data in case operational 
            # data is not available. 
            if read_data['date'].max() < dt.date.today() - dt.timedelta(days=51):
                logger.info("Cached data is older than 50 days, reprocessing input files.")
                # Reprocess input files
                organization = os.getenv('ieasyhydroforecast_organization')
                if organization=='kghm': 
                    # Read data from excel files
                    read_data = read_all_runoff_data_from_excel(
                        date_col=date_col,
                        discharge_col=discharge_col,
                        name_col=name_col,
                        code_col=code_col,
                        code_list=code_list)
                elif organization=='tjhm':
                    # Read data from csv files
                    read_data = read_all_runoff_data_from_csv(
                        date_col=date_col,
                        discharge_col=discharge_col,
                        name_col=name_col,
                        code_col=code_col,
                        code_list=code_list)
                else:
                    # Raise an error if the organization is not recognized
                    raise ValueError(f"Organization '{organization}' not recognized. "
                                     f"Please set the environment variable 'ieasyhydroforecast_organization' to 'kghm' or 'tjhm'.")
            else: 
                logger.info("Cached data is newer than 50 days, using cached data.")
                # Discard the last 50 days of previous operational data and update 
                # these with the new data in the next code section. 
                read_data = read_data[read_data['date'] < dt.date.today() - dt.timedelta(days=50)]
        except Exception as e:
            logger.warning(f"Failed to read cached data: {e}, reprocessing input files")
            # Fall back to processing logic
            if organization=='kghm': 
                # Read data from excel files
                read_data = read_all_runoff_data_from_excel(
                    date_col=date_col,
                    discharge_col=discharge_col,
                    name_col=name_col,
                    code_col=code_col,
                    code_list=code_list)
            elif organization=='tjhm':
                # Read data from csv files
                read_data = read_all_runoff_data_from_csv(
                    date_col=date_col,
                    discharge_col=discharge_col,
                    name_col=name_col,
                    code_col=code_col,
                    code_list=code_list)
            else:
                # Raise an error if the organization is not recognized
                raise ValueError(f"Organization '{organization}' not recognized. "
                                 f"Please set the environment variable 'ieasyhydroforecast_organization' to 'kghm' or 'tjhm'.")

    # Initialize a flag for virtual stations
    virtual_stations_present = False
    # Get virtual station codes from json (if file exists) print a warning if file
    # does not exist.
    if os.getenv('ieasyforecast_virtual_stations') is None:
        logger.info(f"No calculation rules for virtual stations found.\n"
                    f"Environment variable ieasyforecast_virtual_stations is not set.")
    else:
        virtual_stations_config_file_path = os.path.join(
            os.getenv('ieasyforecast_configuration_path'),
            os.getenv('ieasyforecast_virtual_stations'))
        if not os.path.exists(virtual_stations_config_file_path):
            raise FileNotFoundError(
                f"File {virtual_stations_config_file_path} not found.\n",
                f"Filename for calculateion rules for virtual stations in environment\n"
                f"but file not found.\n"
                f"Please provide a configuraion file ieasyforecast_virtual_stations\n"
                f"or, if you don't have any virtual stations to predict, remove the\n"
                f"variable ieasyforecast_virtual_stations from your configuration file."
            )
        else:
            with open(virtual_stations_config_file_path, 'r') as f:
                virtual_stations = json.load(f)['virtualStations'].keys()
            virtual_stations_present = True

            read_data = add_hydroposts(read_data, virtual_stations)

    if ieh_sdk is None:
        # We do not have access to an iEasyHydro database
        logger.info("No data read from iEasyHydro Database.")

        return read_data

    else:
        # Get the last row for each code in runoff_data
        last_row = read_data.groupby(code_col).tail(1)
        #print("DEBUG: last_row: \n", last_row)

        # For each code in last_row, get the daily average discharge data from the
        # iEasyHydro database using the function get_daily_average_discharge_from_iEH_per_site
        for index, row in last_row.iterrows():
            db_average_data = get_daily_average_discharge_from_iEH_per_site(
                ieh_sdk=ieh_sdk, site=row[code_col], name=row[name_col], 
                start_date=row[date_col],
                date_col=date_col, discharge_col=discharge_col, name_col=name_col, code_col=code_col
            )
            db_morning_data = get_todays_morning_discharge_from_iEH_per_site(
                ieh_sdk, row[code_col], row[name_col],
                date_col=date_col, discharge_col=discharge_col, name_col=name_col, code_col=code_col)
            # Append db_data to read_data if db_data is not empty
            if not db_average_data.empty:
                read_data = pd.concat([read_data, db_average_data], ignore_index=True)
            if not db_morning_data.empty:
                read_data = pd.concat([read_data, db_morning_data], ignore_index=True)

        # Drop rows where 'code' is "NA"
        # Use .copy() to avoid SettingWithCopyWarning when modifying columns
        read_data = read_data[read_data[code_col] != 'NA'].copy()

        # Cast the 'code' column to string
        read_data[code_col] = read_data[code_col].astype(str)

        #print(read_data[read_data['code'] == "16936"].tail(10))
        #print(read_data[read_data['code'] == "16059"].tail(10))
        # Calculate virtual hydropost data where necessary
        if virtual_stations_present:
            read_data = calculate_virtual_stations_data(read_data)
        #print(read_data[read_data['code'] == "16936"].tail(10))
        #print(read_data[read_data['code'] == "16059"].tail(10))

        # For sanity sake, we round the data to a mac of 3 decimal places
        read_data[discharge_col] = read_data[discharge_col].round(3)

        return read_data


def _load_cached_data(date_col: str, discharge_col: str, name_col: str, 
                      code_col: str, code_list: list) -> pd.DataFrame:
    """
    Load cached discharge data from the intermediate output file.
    
    Falls back to reading from organization-specific input files if cached data
    is unavailable or empty.
    
    Args:
        date_col: Name of the date column.
        discharge_col: Name of the discharge column.
        name_col: Name of the name column.
        code_col: Name of the code column.
        code_list: List of site codes to read data for.
        
    Returns:
        pd.DataFrame: Cached discharge data.
    """
    intermediate_data_path = os.getenv('ieasyforecast_intermediate_data_path')
    output_file_path = os.path.join(
        intermediate_data_path,
        os.getenv("ieasyforecast_daily_discharge_file"))
    try:
        read_data = pd.read_csv(output_file_path)
        read_data[date_col] = pd.to_datetime(read_data[date_col]).dt.normalize()
        read_data[code_col] = read_data[code_col].astype(str)
        read_data[discharge_col] = read_data[discharge_col].astype(float)
        # Get a dummy name column 
        read_data[name_col] = read_data[code_col]
        
        # Check if data is valid
        if read_data.empty:
            logger.info("Cached data is empty, reprocessing input files.")
            organization = os.getenv('ieasyhydroforecast_organization')
            read_data = _read_runoff_data_by_organization(
                organization=organization,
                date_col=date_col,
                discharge_col=discharge_col,
                name_col=name_col,
                code_col=code_col,
                code_list=code_list
            )
        return read_data
    except Exception as e:
        logger.warning(f"Failed to read cached data: {e}, reprocessing input files")
        organization = os.getenv('ieasyhydroforecast_organization')
        return _read_runoff_data_by_organization(
            organization=organization,
            date_col=date_col,
            discharge_col=discharge_col,
            name_col=name_col,
            code_col=code_col,
            code_list=code_list
        )


def _merge_with_update(existing_data: pd.DataFrame, new_data: pd.DataFrame,
                       code_col: str, date_col: str, discharge_col: str) -> pd.DataFrame:
    """
    Merge new data into existing data, updating values where they differ.
    
    This is used in maintenance mode to update/correct historical values from
    the database while preserving data that hasn't changed.
    
    Args:
        existing_data: The existing cached data.
        new_data: New data from the database.
        code_col: Name of the site code column.
        date_col: Name of the date column.
        discharge_col: Name of the discharge column.
        
    Returns:
        pd.DataFrame: Merged data with updates applied.
    """
    if existing_data.empty:
        logger.debug(f"[MERGE] Existing data empty, returning {len(new_data)} new records")
        return new_data
    if new_data.empty:
        logger.debug(f"[MERGE] New data empty, returning {len(existing_data)} existing records")
        return existing_data

    logger.debug(f"[MERGE] Existing: {len(existing_data)}, New: {len(new_data)}")

    # Create a copy to avoid modifying the original
    result = existing_data.copy()

    # Set index for efficient lookup
    result_indexed = result.set_index([code_col, date_col])
    new_indexed = new_data.set_index([code_col, date_col])

    # Count overlapping keys (potential updates)
    overlapping_keys = result_indexed.index.intersection(new_indexed.index)
    logger.debug(f"[MERGE] Overlapping keys (updates): {len(overlapping_keys)}")

    # Update existing values with new values where they exist
    result_indexed.update(new_indexed)

    # Add any new rows that weren't in the existing data
    new_rows = new_indexed[~new_indexed.index.isin(result_indexed.index)]
    if not new_rows.empty:
        result_indexed = pd.concat([result_indexed, new_rows])

    # Reset index and return
    result = result_indexed.reset_index()

    logger.info(f"[MERGE] Complete: {len(existing_data)} existing + {len(new_rows)} new = {len(result)} total")

    return result


def _read_runoff_data_by_organization(organization, date_col, discharge_col, name_col, code_col, code_list):
    """Reads runoff data based on the organization."""
    if organization == 'kghm':
        read_data = read_all_runoff_data_from_excel(
            date_col=date_col,
            discharge_col=discharge_col,
            name_col=name_col,
            code_col=code_col,
            code_list=code_list)
    elif organization == 'tjhm':
        read_data = read_all_runoff_data_from_csv(
            date_col=date_col,
            discharge_col=discharge_col,
            name_col=name_col,
            code_col=code_col,
            code_list=code_list)
    elif organization == 'demo': 
        read_data = read_all_runoff_data_from_excel(
            date_col=date_col,
            discharge_col=discharge_col,
            name_col=name_col,
            code_col=code_col,
            code_list=code_list
        )
    else:
        raise ValueError(f"Organization '{organization}' not recognized. "
                         f"Please set the environment variable 'ieasyhydroforecast_organization' to 'kghm' or 'tjhm'.")
    return read_data


def get_latest_date_per_site(
    df: pd.DataFrame,
    date_col: str,
    code_col: str
) -> dict[str, pd.Timestamp]:
    """
    Extract the latest date per site from a DataFrame.

    Args:
        df: DataFrame containing the data.
        date_col: Name of the date column.
        code_col: Name of the site code column.

    Returns:
        dict: Mapping of site_code -> latest_date. Empty dict if df is empty.
    """
    if df is None or df.empty:
        return {}

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df[code_col] = df[code_col].astype(str)

    return df.groupby(code_col)[date_col].max().to_dict()


def calculate_fetch_ranges(
    code_list: list,
    coverage_per_site: dict[str, pd.Timestamp],
    default_lookback_days: int,
    end_date: pd.Timestamp = None
) -> list[tuple[pd.Timestamp, list[str]]]:
    """
    Group sites by their required fetch start date for efficient API calls.

    Sites are grouped by month of their coverage end date to minimize API calls
    while being efficient (not fetching redundant data).

    Args:
        code_list: List of all site codes to fetch.
        coverage_per_site: Dict of site_code -> coverage_end_date.
        default_lookback_days: Fallback lookback for sites with no coverage.
        end_date: End date for fetching (default: today).

    Returns:
        list of (start_date, [site_codes]) tuples, sorted by start_date.
    """
    if end_date is None:
        end_date = pd.Timestamp.now().normalize()

    # Calculate fetch start date for each site
    fetch_start_per_site = {}
    for site in code_list:
        site_str = str(site)
        if site_str in coverage_per_site:
            # Start from day after last coverage
            fetch_start = coverage_per_site[site_str] + pd.Timedelta(days=1)
            # Don't fetch if coverage is already up to date
            if fetch_start > end_date:
                fetch_start = end_date
        else:
            # No coverage data - use default lookback
            fetch_start = end_date - pd.Timedelta(days=default_lookback_days)
        fetch_start_per_site[site_str] = fetch_start

    # Group by month (YYYY-MM) to batch similar date ranges
    groups = {}
    for site, start_date in fetch_start_per_site.items():
        month_key = start_date.replace(day=1).strftime('%Y-%m')
        if month_key not in groups:
            groups[month_key] = {'start_date': start_date, 'sites': []}
        groups[month_key]['sites'].append(site)
        # Use the earliest start date in the group
        if start_date < groups[month_key]['start_date']:
            groups[month_key]['start_date'] = start_date

    # Convert to list of tuples, sorted by start date (oldest first)
    result = [(g['start_date'], g['sites']) for g in groups.values()]
    result.sort(key=lambda x: x[0])

    return result


def print_smart_lookback_summary(
    code_list: list,
    coverage_per_site: dict[str, pd.Timestamp],
    fetch_groups: list[tuple[pd.Timestamp, list[str]]]
):
    """Log a summary of the smart lookback analysis."""
    now = pd.Timestamp.now().normalize()

    logger.debug("[DATA] " + "=" * 70)
    logger.debug("[DATA] SMART LOOKBACK ANALYSIS")
    logger.debug("[DATA] " + "=" * 70)

    # Categorize sites by coverage
    recent = [s for s, d in coverage_per_site.items() if (now - d).days <= 7]
    older = [s for s, d in coverage_per_site.items() if (now - d).days > 7]
    no_coverage = [str(s) for s in code_list if str(s) not in coverage_per_site]

    logger.debug(f"[DATA] Total sites: {len(code_list)}")
    logger.debug(f"[DATA]   With recent data (<=7 days old): {len(recent)}")
    logger.debug(f"[DATA]   With older data (>7 days old):   {len(older)}")
    logger.debug(f"[DATA]   With no existing data:           {len(no_coverage)}")

    logger.debug(f"[DATA] Fetch plan ({len(fetch_groups)} batches):")
    logger.debug("[DATA] " + "-" * 70)
    for start_date, sites in fetch_groups:
        days = (now - start_date).days
        logger.debug(f"[DATA]   From {start_date.date()} ({days:>4} days to fetch): {len(sites):>3} sites")

    if older:
        logger.debug(f"[DATA] Sites with oldest coverage (top 5):")
        oldest = sorted([(s, coverage_per_site[s]) for s in older], key=lambda x: x[1])[:5]
        for site, date in oldest:
            logger.debug(f"[DATA]   {site}: last data {date.date()} ({(now - date).days} days ago)")

    logger.debug("[DATA] " + "=" * 70)

    # Also log at INFO level for summary
    logger.info(f"[DATA] Smart lookback: {len(code_list)} sites in {len(fetch_groups)} batches")


'''def get_runoff_data_for_sites_HF(ieh_hf_sdk=None, date_col='date', name_col='name',
                              discharge_col='discharge',
                              code_col='code', 
                              site_list=None, code_list=None, id_list=None, 
                              target_timezone=None):
    """
    Reads runoff data from excel and, if possible, from iEasyHydro database.

    Note: This function will only try to read data from the iEasyHydro database
    which are already in the excel files.

    Args:
        ieh_sdk (object): An object that provides a method to get data values
            for a site from a database. None in case of no access to the database.
        date_col (str, optional): The name of the column containing the date data.
            Default is 'date'.
        discharge_col (str, optional): The name of the column containing the discharge data.
            Default is 'discharge'.
        name_col (str, optional): The name of the column containing the site name.
            Default is 'name'.
        code_col (str, optional): The name of the column containing the site code.
            Default is 'code'.
        site_list (list, optional): A list of site codes to read data for. Default is None.
        code_list (list, optional): A list of site codes to read data for. Default is None.
        id_list (list): A list of site IDs to read data for. Default is None.
        target_timezone (str, optional): The timezone to convert the data to. Default is None.

    Details: 
    - Read data from excel files if available and necessary
      
    """
    # Test if id_list is None or empty. Return error if it is.
    if id_list is None or len(id_list) == 0:
        raise ValueError("id_list is None or empty. Please provide a list of site IDs to read data for.")

    # Test if there have been any changes in the daily_discharge directory
    if should_reprocess_input_files(): 
        logger.info("Regime data has changed, reprocessing input files.")
    
        # Get organization from environment variable
        organization = os.getenv('ieasyhydroforecast_organization')

        read_data = _read_runoff_data_by_organization(
            organization=organization,
            date_col=date_col,
            discharge_col=discharge_col,
            name_col=name_col,
            code_col=code_col,
            code_list=code_list
        )
    else:
        logger.info("No changes in the daily_discharge directory, using previous data.")
        intermediate_data_path = os.getenv('ieasyforecast_intermediate_data_path')
        output_file_path = os.path.join(
            intermediate_data_path,
            os.getenv("ieasyforecast_daily_discharge_file"))
        try:
            read_data = pd.read_csv(output_file_path)
            read_data['date'] = pd.to_datetime(read_data['date']).dt.date
            read_data['code'] = read_data['code'].astype(str)
            read_data['discharge'] = read_data['discharge'].astype(float)
            # Get a dummy name column 
            read_data['name'] = read_data['code']
            # Check the latest date. If it is before today minus 51 days, go to 
            # the fallback and read in all regime data. 
            # This is to avoid shortening of the regime data in case operational 
            # data is not available. 
            if read_data['date'].max() < dt.date.today() - dt.timedelta(days=51):
                logger.info("Cached data is older than 50 days, reprocessing input files.")
                # Reprocess input files
                organization = os.getenv('ieasyhydroforecast_organization')
                read_data = _read_runoff_data_by_organization(
                    organization=organization,
                    date_col=date_col,
                    discharge_col=discharge_col,
                    name_col=name_col,
                    code_col=code_col,
                    code_list=code_list
                )
            else: 
                logger.info("Cached data is newer than 50 days, using cached data.")
                # Discard the last 50 days of previous operational data and update 
                # these with the new data in the next code section. 
                read_data = read_data[read_data['date'] < dt.date.today() - dt.timedelta(days=50)]
        except Exception as e:
            logger.warning(f"Failed to read cached data: {e}, reprocessing input files")
            # Fall back to processing logic
            organization = os.getenv('ieasyhydroforecast_organization')
            read_data = _read_runoff_data_by_organization(
                organization=organization,
                date_col=date_col,
                discharge_col=discharge_col,
                name_col=name_col,
                code_col=code_col,
                code_list=code_list
            )

    # Initialize a flag for virtual stations
    virtual_stations_present = False
    # Get virtual station codes from json (if file exists) print a warning if file
    # does not exist.
    if os.getenv('ieasyforecast_virtual_stations') is None:
        logger.info(f"No calculation rules for virtual stations found.\n"
                    f"Environment variable ieasyforecast_virtual_stations is not set.")
    else:
        virtual_stations_config_file_path = os.path.join(
            os.getenv('ieasyforecast_configuration_path'),
            os.getenv('ieasyforecast_virtual_stations'))
        if not os.path.exists(virtual_stations_config_file_path):
            raise FileNotFoundError(
                f"File {virtual_stations_config_file_path} not found.\n",
                f"Filename for calculateion rules for virtual stations in environment\n"
                f"but file not found.\n"
                f"Please provide a configuraion file ieasyforecast_virtual_stations\n"
                f"or, if you don't have any virtual stations to predict, remove the\n"
                f"variable ieasyforecast_virtual_stations from your configuration file."
            )
        else:
            with open(virtual_stations_config_file_path, 'r') as f:
                virtual_stations = json.load(f)['virtualStations'].keys()
            virtual_stations_present = True

            read_data = add_hydroposts(read_data, virtual_stations)

    if ieh_hf_sdk in locals() and ieh_hf_sdk is None:
        # We do not have access to an iEasyHydro database
        logger.info("No data read from iEasyHydro Database.")

        return read_data

    else:
        # Update the last 50 days of the read_data with the data from the iEasyHydro HF database
        db_average_data = get_daily_average_discharge_from_iEH_HF_for_multiple_sites(
            ieh_hf_sdk=ieh_hf_sdk, id_list=id_list, 
            date_col=date_col, discharge_col=discharge_col, code_col=code_col
        )
        # We are dealing with daily data, so we convert date_col to date
        db_average_data[date_col] = pd.to_datetime(db_average_data[date_col]).dt.date
        #print("DEBUG: db_average_data: \n", db_average_data)
        db_morning_data = get_todays_morning_discharge_from_iEH_HF_for_multiple_sites(
            ieh_hf_sdk=ieh_hf_sdk, id_list=id_list,
            date_col=date_col, discharge_col=discharge_col, code_col=code_col
        )
        db_morning_data[date_col] = pd.to_datetime(db_morning_data[date_col]).dt.date
        #print("DEBUG: db_morning_data: \n", db_morning_data)
        # Append db_data to read_data if db_data is not empty
        if not db_average_data.empty:
            read_data = pd.concat([read_data, db_average_data], ignore_index=True)
        if not db_morning_data.empty:
            read_data = pd.concat([read_data, db_morning_data], ignore_index=True)

        # Drop rows where 'code' is "NA"
        # Use .copy() to avoid SettingWithCopyWarning when modifying columns
        read_data = read_data[read_data[code_col] != 'NA'].copy()

        # Cast the 'code' column to string
        read_data[code_col] = read_data[code_col].astype(str)

        #print(read_data[read_data['code'] == "16936"].tail(10))
        #print(read_data[read_data['code'] == "16059"].tail(10))
        # Calculate virtual hydropost data where necessary
        if virtual_stations_present:
            read_data = calculate_virtual_stations_data(read_data)
        #print(read_data[read_data['code'] == "16936"].tail(10))
        #print(read_data[read_data['code'] == "16059"].tail(10))

        # For sanity sake, we round the data to a mac of 3 decimal places
        read_data[discharge_col] = read_data[discharge_col].round(3)

        # Make sure the date column is in datetime format
        read_data[date_col] = pd.to_datetime(read_data[date_col]).dt.normalize()

        return read_data'''

def get_runoff_data_for_sites_HF(ieh_hf_sdk=None, date_col='date', name_col='name',
                              discharge_col='discharge',
                              code_col='code', 
                              site_list=None, code_list=None, id_list=None, 
                              target_timezone=None,
                              mode: str = 'operational'):
    """
    Reads runoff data from excel and, if possible, from iEasyHydro database.

    Supports two operating modes:
    - operational (default): Fast daily updates, fetch only yesterday's daily average
      and today's morning discharge. Skips Excel file checks.
    - maintenance: Full lookback window (configurable), checks for Excel file changes,
      fills gaps in data.

    Args:
        ieh_sdk (object): An object that provides a method to get data values
            for a site from a database. None in case of no access to the database.
        date_col (str, optional): The name of the column containing the date data.
            Default is 'date'.
        discharge_col (str, optional): The name of the column containing the discharge data.
            Default is 'discharge'.
        name_col (str, optional): The name of the column containing the site name.
            Default is 'name'.
        code_col (str, optional): The name of the column containing the site code.
            Default is 'code'.
        site_list (list, optional): A list of site codes to read data for. Default is None.
        code_list (list, optional): A list of site codes to read data for. Default is None.
        id_list (list): A list of site IDs to read data for. Default is None.
        target_timezone (str, optional): The timezone to convert the data to. Default is None.
        mode (str, optional): Operating mode - 'operational' or 'maintenance'. Default is 'operational'.
    """
    from .config import get_maintenance_lookback_days
    
    # Test if id_list is None or empty. Return error if it is.
    if id_list is None or len(id_list) == 0:
        raise ValueError("id_list is None or empty. Please provide a list of site IDs to read data for.")

    # Validate mode parameter
    mode = mode.lower()
    if mode not in ('operational', 'maintenance'):
        logger.warning(f"Unknown mode '{mode}', defaulting to 'operational'")
        mode = 'operational'
    
    logger.info(f"Running in {mode.upper()} mode")
    
    # Determine whether to reprocess input files based on mode
    if mode == 'maintenance':
        # In maintenance mode, ALWAYS reread input files to ensure data completeness.
        # This is necessary to fill gaps that may exist in the cached output file
        # (e.g., if rows were deleted or data was corrupted).
        # The DB fetch that follows will update/add any newer data on top of this baseline.
        logger.info("Maintenance mode: rereading input files to ensure data completeness.")

        # Log whether input files have also changed (informational only)
        if should_reprocess_input_files():
            logger.info("Note: Input files have also changed since last processing.")

        # Get organization from environment variable
        organization = os.getenv('ieasyhydroforecast_organization')

        read_data = _read_runoff_data_by_organization(
            organization=organization,
            date_col=date_col,
            discharge_col=discharge_col,
            name_col=name_col,
            code_col=code_col,
            code_list=code_list
        )
    else:
        # In operational mode, always use cached data (skip file checks)
        logger.info("Operational mode: using cached data, fetching only latest from database.")
        read_data = _load_cached_data(date_col, discharge_col, name_col, code_col, code_list)

    # Ensure date is a normalized timestamp object if read_data is not empty
    if not read_data.empty:
        read_data[date_col] = pd.to_datetime(read_data[date_col]).dt.normalize()

    # Initialize a flag for virtual stations
    virtual_stations_present = False
    # Get virtual station codes from json (if file exists)
    if os.getenv('ieasyforecast_virtual_stations') is not None:
        virtual_stations_config_file_path = os.path.join(
            os.getenv('ieasyforecast_configuration_path'),
            os.getenv('ieasyforecast_virtual_stations'))
        if os.path.exists(virtual_stations_config_file_path):
            with open(virtual_stations_config_file_path, 'r') as f:
                virtual_stations = json.load(f)['virtualStations'].keys()
            virtual_stations_present = True
            read_data = add_hydroposts(read_data, virtual_stations)
            read_data = standardize_date_column(read_data, date_col=date_col)
        else:
            logger.warning(f"Virtual stations configuration file {virtual_stations_config_file_path} not found.")

    if ieh_hf_sdk is None:
        # We do not have access to an iEasyHydro database
        logger.info("No data read from iEasyHydro Database.")
        return read_data

    else:
        # Make sure we have timezone-aware datetime objects for API calls
        if target_timezone is None:
            target_timezone = pytz.timezone(time.tzname[0])
            logger.debug(f"Target timezone is None. Using local timezone: {target_timezone}")
        
        # Set end datetime to current time
        end_datetime = pd.Timestamp.now().tz_localize(target_timezone)
        
        # Determine the date range based on mode
        if mode == 'operational':
            # Operational mode: fetch only yesterday's data for all sites
            start_date = pd.Timestamp.now().normalize() - pd.Timedelta(days=1)
            logger.info(f"Operational mode: fetching data from {start_date.date()} onwards.")

            # Convert start_date to timezone-aware datetime
            start_datetime = pd.Timestamp(start_date).replace(hour=0, minute=1)
            start_datetime = start_datetime.tz_localize(target_timezone)

            # Fetch data from iEH HF database
            logger.info(f"Fetching data from {start_datetime} to {end_datetime}")
            db_average_data = get_daily_average_discharge_from_iEH_HF_for_multiple_sites(
                ieh_hf_sdk=ieh_hf_sdk,
                id_list=code_list,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                target_timezone=target_timezone,
                date_col=date_col,
                discharge_col=discharge_col,
                code_col=code_col
            )
        else:
            # Maintenance mode: use smart lookback per site
            lookback_days = get_maintenance_lookback_days()

            # Extract coverage from already-loaded data
            coverage_per_site = get_latest_date_per_site(read_data, date_col, code_col)

            # Calculate efficient fetch ranges
            fetch_groups = calculate_fetch_ranges(
                code_list=code_list,
                coverage_per_site=coverage_per_site,
                default_lookback_days=lookback_days,
                end_date=pd.Timestamp.now().normalize()
            )

            # Print summary
            print_smart_lookback_summary(code_list, coverage_per_site, fetch_groups)

            # Fetch data in batches, grouped by start date
            all_batch_data = []
            for batch_start_date, batch_sites in fetch_groups:
                # Convert to timezone-aware datetime
                batch_start_datetime = pd.Timestamp(batch_start_date).replace(hour=0, minute=1)
                batch_start_datetime = batch_start_datetime.tz_localize(target_timezone)

                logger.info(f"Fetching {len(batch_sites)} sites from {batch_start_date.date()}")
                batch_data = get_daily_average_discharge_from_iEH_HF_for_multiple_sites(
                    ieh_hf_sdk=ieh_hf_sdk,
                    id_list=batch_sites,
                    start_datetime=batch_start_datetime,
                    end_datetime=end_datetime,
                    target_timezone=target_timezone,
                    date_col=date_col,
                    discharge_col=discharge_col,
                    code_col=code_col
                )
                if not batch_data.empty:
                    all_batch_data.append(batch_data)

            # Combine all batch results
            if all_batch_data:
                db_average_data = pd.concat(all_batch_data, ignore_index=True)
            else:
                db_average_data = pd.DataFrame()
        
        # We are dealing with daily data, so we convert date_col to date
        if not db_average_data.empty:
            db_average_data = standardize_date_column(db_average_data, date_col=date_col)
            logger.info(f"Retrieved {len(db_average_data)} new records from {db_average_data[date_col].min()} to {db_average_data[date_col].max()}")
            
            if mode == 'maintenance':
                # In maintenance mode, update existing values if they differ
                read_data = _merge_with_update(read_data, db_average_data, code_col, date_col, discharge_col)
            else:
                # In operational mode, simply append new data
                read_data = pd.concat([read_data, db_average_data], ignore_index=True)
            
        # Get today's morning data if necessary
        db_morning_data = get_todays_morning_discharge_from_iEH_HF_for_multiple_sites(
            ieh_hf_sdk=ieh_hf_sdk,
            id_list=code_list,  # Use code_list (string codes) - API requires site_codes, not site_ids
            target_timezone=target_timezone,
            date_col=date_col,
            discharge_col=discharge_col,
            code_col=code_col
        )
        
        if not db_morning_data.empty:
            db_morning_data = standardize_date_column(db_morning_data, date_col=date_col)
            logger.info(f"Retrieved {len(db_morning_data)} morning records for {db_morning_data[date_col].min()}")
            read_data = pd.concat([read_data, db_morning_data], ignore_index=True)

        # Drop rows where 'code' is "NA"
        # Use .copy() to avoid SettingWithCopyWarning when modifying columns
        read_data = read_data[read_data[code_col] != 'NA'].copy()

        # Cast the 'code' column to string
        read_data[code_col] = read_data[code_col].astype(str)

        # Calculate virtual hydropost data where necessary
        if virtual_stations_present:
            read_data = calculate_virtual_stations_data(read_data)

        # For sanity sake, we round the data to a mac of 3 decimal places
        read_data[discharge_col] = read_data[discharge_col].round(3)

        # Make sure the date column is in datetime format
        read_data[date_col] = pd.to_datetime(read_data[date_col]).dt.normalize()
        
        # Remove duplicate data (in case DB had overlapping data)
        read_data = read_data.drop_duplicates(subset=[code_col, date_col], keep='last')

        # Sort data by code and date
        read_data = read_data.sort_values([code_col, date_col])

        # Final output summary
        date_min = read_data[date_col].min().date()
        date_max = read_data[date_col].max().date()
        site_count = read_data[code_col].nunique()
        logger.info(
            f"[OUTPUT] Final: {len(read_data)} records, {site_count} sites, "
            f"{date_min} to {date_max}"
        )

        return read_data

def is_leap_year(year):
    if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
        return True
    else:
        return False

'''def from_daily_time_series_to_hydrograph(data_df: pd.DataFrame,
                                         date_col='date', discharge_col='discharge', code_col='code', name_col='name'):
    """
    Calculates daily runoff statistics and writes it to hydrograph format.

    Args:
    data_df (pd.DataFrame): The daily runoff data.
    date_col (str, optional): The name of the column containing the date data.
        Default is 'date'.
    discharge_col (str, optional): The name of the column containing the discharge data.
        Default is 'discharge'.
    code_col (str, optional): The name of the column containing the site code.
        Default is 'code'.

    Returns:
    pd.DataFrame: The hydrograph data.
    """
    # Ensure the date column is in datetime format
    data_df[date_col] = pd.to_datetime(data_df[date_col])

    # Ensure the code column is of string type
    data_df[code_col] = data_df[code_col].astype(str)

    # Ensure the discharge column is numeric
    data_df[discharge_col] = pd.to_numeric(data_df[discharge_col], errors='coerce')

    # Based on the date column, write the day of the year to a new column
    data_df['day_of_year'] = data_df[date_col].dt.dayofyear

    # Drop all rows where the date is the 29th of February
    #data_df = data_df[~((data_df[date_col].dt.month == 2) & (data_df[date_col].dt.day == 29))]

    # Adjust the day_of_year for the 29th of February for leap years
    #data_df.loc[(data_df[date_col].dt.month > 2) & (data_df[date_col].dt.is_leap_year), 'day_of_year'] -= 1

    # Get the current year data
    current_year = dt.date.today().year

    # If we are not in a leap year, drop the 29th of February and adjust the day_of_year
    if not is_leap_year(current_year):
        data_df = data_df[~((data_df[date_col].dt.month == 2) & (data_df[date_col].dt.day == 29))]
        data_df.loc[(data_df[date_col].dt.month > 2), 'day_of_year'] -= 1

    # Group the data by the code and day_of_year columns and calculate the min,
    # max, mean, 5th, 25th, 50th, 75th, and 95th percentiles of the discharge.
    hydrograph_data = data_df.groupby([code_col, 'day_of_year'])[discharge_col].describe(
        percentiles=[0.05, 0.25, 0.5, 0.75, 0.95])

    # Also get last year and current year data for the hydrograph
    current_year_data = data_df[data_df[date_col].dt.year == current_year]
    # Drop the date column
    current_year_data = current_year_data.drop(columns=[date_col, name_col])
    # Rename the discharge column to the current year
    current_year_data = current_year_data.rename(columns={discharge_col: f"{current_year}"})
    # last year
    last_year = current_year - 1
    last_year_data = data_df[data_df[date_col].dt.year == last_year]
    # Drop the date column
    last_year_data = last_year_data.drop(columns=[name_col])
    # Add 1 year to the date column, this leads to a bug in the hydrograph when
    # leap years are involved. Better to recalculate the date as further below.
    #last_year_data[date_col] = last_year_data[date_col] + pd.DateOffset(years=1)
    # Rename the discharge column to the last year
    last_year_data = last_year_data.rename(columns={discharge_col: f"{last_year}"})

    # Add current discharge and last year discharge to the hydrograph data by code and day_of_year
    hydrograph_data = hydrograph_data.merge(
        current_year_data.groupby([code_col, 'day_of_year'])[str(current_year)].mean().reset_index(),
        on=[code_col, 'day_of_year'], how='left', suffixes=('', '_current'))
    hydrograph_data = hydrograph_data.merge(
        last_year_data.groupby([code_col, 'day_of_year'])[str(last_year)].mean().reset_index(),
        on=[code_col, 'day_of_year'], how='left', suffixes=('', '_last_year'))
    #hydrograph_data = hydrograph_data.merge(
    #    last_year_data.groupby([code_col, 'day_of_year'])[date_col].first().reset_index(),
    #    on=[code_col, 'day_of_year'], how='left', suffixes=('', '_last_year'))
    # Create date based on day of year and the current year
    hydrograph_data['date'] = pd.Timestamp(str(current_year)) + pd.to_timedelta(hydrograph_data['day_of_year'] - 1, unit='D')
    # print head and tail of hydrograph_data for code == '15194'
    print(f"DEBUG: hydrograph_data[hydrograph_data['code'] == '15194'].head(10)\n{hydrograph_data[hydrograph_data['code'] == '15194'].head(10)}")
    print(f"DEBUG: hydrograph_data[hydrograph_data['code'] == '15194'].tail(10)\n{hydrograph_data[hydrograph_data['code'] == '15194'].tail(10)}")

    return hydrograph_data'''

def inspect_site_data(hydrograph_data, site_code='15189'):
    """Show data for specific site at beginning, around Feb 28, and end of year."""
    # Filter for the specific site
    site_data = hydrograph_data[hydrograph_data['code'] == site_code].sort_values('date')

    # First 5 days of the year
    logger.debug(f"[DATA] FIRST 5 DAYS:\n{site_data.head(5)}")

    # Feb 28 and surrounding days (10 days before and after)
    # Create a mask for February 28th
    feb_28_mask = (site_data['date'].dt.month == 2) & (site_data['date'].dt.day == 28)

    # Check if February 28th exists in the data
    if feb_28_mask.any():
        # Get the date of February 28th
        feb_28_date = site_data[feb_28_mask]['date'].iloc[0]

        # Find the dates 10 days before and 10 days after
        start_date = feb_28_date - pd.Timedelta(days=10)
        end_date = feb_28_date + pd.Timedelta(days=10)

        # Filter the data for the date range
        date_range_data = site_data[(site_data['date'] >= start_date) &
                                    (site_data['date'] <= end_date)]

        logger.debug(f"[DATA] DAYS AROUND FEB 28:\n{date_range_data}")
    else:
        logger.debug("[DATA] DAYS AROUND FEB 28: February 28th not found in the data")

    # Last 5 days of the year
    logger.debug(f"[DATA] LAST 5 DAYS:\n{site_data.tail(5)}")

def from_daily_time_series_to_hydrograph(data_df: pd.DataFrame,
                                        date_col='date', discharge_col='discharge', code_col='code', name_col='name'):
    """
    Calculates daily runoff statistics and writes it to hydrograph format.
    Properly handles leap years by creating a normalized day-of-year.
    """
    # Ensure the date column is in datetime format
    data_df[date_col] = pd.to_datetime(data_df[date_col])

    # Ensure the code column is of string type
    data_df[code_col] = data_df[code_col].astype(str)

    # Ensure the discharge column is numeric
    data_df[discharge_col] = pd.to_numeric(data_df[discharge_col], errors='coerce')

    # Create a copy of the dataframe to avoid modifying the original
    working_df = data_df.copy()
    
    # Create a normalized day-of-year column that works for both leap and non-leap years
    # First, add the regular day of year
    working_df['day_of_year'] = working_df[date_col].dt.dayofyear
    
    # Create a normalized day that maps Feb 29 to Feb 28 in leap years
    # and adjusts all subsequent days to maintain 365 days
    working_df['normalized_day'] = working_df['day_of_year']
    
    # For leap years, adjust days after Feb 29 (day 60)
    leap_year_mask = working_df[date_col].dt.is_leap_year
    after_feb29_mask = (leap_year_mask) & (working_df['day_of_year'] > 60)
    
    # Reduce normalized_day by 1 for days after Feb 29 in leap years
    working_df.loc[after_feb29_mask, 'normalized_day'] -= 1
    
    # Map Feb 29 to the same normalized_day as Feb 28 in leap years
    feb29_mask = (working_df[date_col].dt.month == 2) & (working_df[date_col].dt.day == 29)
    working_df.loc[feb29_mask, 'normalized_day'] = 59  # Feb 28's normalized day number
    
    # Get the current year
    current_year = dt.date.today().year
    
    # Calculate statistics based on the normalized day column
    hydrograph_data = working_df.groupby([code_col, 'normalized_day'])[discharge_col].describe(
        percentiles=[0.05, 0.25, 0.5, 0.75, 0.95])
    
    # Get current year's data
    current_year_data = working_df[working_df[date_col].dt.year == current_year]
    current_year_data = current_year_data.drop(columns=[date_col, name_col])
    current_year_data = current_year_data.rename(columns={discharge_col: f"{current_year}"})
    
    # Get last year's data
    last_year = current_year - 1
    last_year_data = working_df[working_df[date_col].dt.year == last_year]
    last_year_data = last_year_data.drop(columns=[name_col])
    last_year_data = last_year_data.rename(columns={discharge_col: f"{last_year}"})
    
    # Add current and last year's discharge to the hydrograph data
    hydrograph_data = hydrograph_data.merge(
        current_year_data.groupby([code_col, 'normalized_day'])[str(current_year)].mean().reset_index(),
        on=[code_col, 'normalized_day'], how='left', suffixes=('', '_current'))
    
    hydrograph_data = hydrograph_data.merge(
        last_year_data.groupby([code_col, 'normalized_day'])[str(last_year)].mean().reset_index(),
        on=[code_col, 'normalized_day'], how='left', suffixes=('', '_last_year'))
    
    # Create date based on normalized day and current year
    # But ensure we're creating valid dates (365 days for non-leap years, 366 for leap years)
    if is_leap_year(current_year):
        # Create regular mapping for days 1-59 (Jan 1 to Feb 28)
        days_before_leap = pd.DataFrame({
            'normalized_day': range(1, 60),  # Days 1-59
            'actual_day': range(1, 60)       # Days 1-59
        })
    
        # Create Feb 29 entry (maps from normalized day 59)
        leap_day = pd.DataFrame({
            'normalized_day': [59],  # Feb 28's normalized day
            'actual_day': [60]       # Feb 29's actual day
        })
    
        # Create mapping for days after Feb 29 with shifted actual_day
        days_after_leap = pd.DataFrame({
            'normalized_day': range(60, 366),  # Days 60-365
            'actual_day': range(61, 367)       # Days 61-366 (shifted by 1)
        })
        
        # Add extra entry for normalized_day 366 if it exists
        if 366 in hydrograph_data['normalized_day'].values:
            extra_day = pd.DataFrame({
                'normalized_day': [366],
                'actual_day': [367]
            })
            day_mapping = pd.concat([days_before_leap, leap_day, days_after_leap, extra_day])
        else:
            day_mapping = pd.concat([days_before_leap, leap_day, days_after_leap])
    else:
        # For non-leap years, normalized_day maps directly to actual_day
        max_normalized_day = int(hydrograph_data['normalized_day'].max())
        day_mapping = pd.DataFrame({
            'normalized_day': range(1, max_normalized_day + 1),
            'actual_day': range(1, max_normalized_day + 1)
        })

    # Print all_days for debugging
    logger.debug(f"day_mapping:\n{day_mapping}")
    
    # Merge the hydrograph data with the day mapping
    hydrograph_data = hydrograph_data.reset_index()
    hydrograph_data = hydrograph_data.merge(day_mapping, on='normalized_day', how='left')

    logger.debug(f"hydrograph_data after merge:\n{hydrograph_data.head(5)}")
    logger.debug(hydrograph_data.head(70).tail(20))
    logger.debug(hydrograph_data.tail(5))
    
    # Create date based on the actual day and current year
    hydrograph_data['date'] = pd.Timestamp(str(current_year)) + pd.to_timedelta(hydrograph_data['actual_day'] - 1, unit='D')
    
    # Set day_of_year to actual_day
    hydrograph_data['day_of_year'] = hydrograph_data['actual_day'].astype(int)
    
    # Drop the temporary columns
    hydrograph_data = hydrograph_data.drop(columns=['normalized_day', 'actual_day'])
    
    return hydrograph_data

def add_dangerous_discharge_from_sites(hydrograph_data: pd.DataFrame,
                                       code_col='code',
                                       site_list=None,
                                       site_code_list=None):
    """
    For each site, add the dangerous discharge value to the hydrograph data.
    """
    # Return error if any of the arguments is None
    if hydrograph_data is None or site_list is None or site_code_list is None:
        raise ValueError("hydrograph_data, site_list and site_code_list must be provided.")

    # Initialize a column in hydrograph data for dangerous discharge
    hydrograph_data['dangerous_discharge'] = np.nan

    # For each unique code, get the dangerous discharge value from the iEasyHydro database
    for site in site_list:
        logger.debug(f"[DATA] site: {site.code}: qdanger={site.qdanger}")
        try:
            dangerous_discharge = site.qdanger

            # Add the dangerous discharge value to the hydrograph_data
            hydrograph_data.loc[hydrograph_data[code_col] == site.code, 'dangerous_discharge'] = dangerous_discharge
        except Exception as e:
            logger.warning(f"Error while adding dangerous discharge for site {site.code}.")
            logger.warning(f"Error: {e}")
            continue

    return hydrograph_data

def add_dangerous_discharge(sdk, hydrograph_data: pd.DataFrame, code_col='code'):
    """
    For each unique code in hydrograph_data, add the dangerous discharge value.

    Args:
    sdk (object): An ieh_sdk object.
    hydrograph_data (pd.DataFrame): The hydrograph data.
    code_col (str, optional): The name of the column containing the site code.

    Returns:
    pd.DataFrame: The hydrograph data with the dangerous discharge values added.
    """

    # Get the unique codes in hydrograph_data
    unique_codes = hydrograph_data[code_col].unique()

    # Initialize a column in hydrograph data for dangerous discharge
    hydrograph_data['dangerous_discharge'] = np.nan

    # For each unique code, get the dangerous discharge value from the iEasyHydro database
    for code in unique_codes:
        try:
            dangerous_discharge = sdk.get_data_values_for_site(
                    code, 'dangerous_discharge')['data_values'][0]['data_value']

            # Add the dangerous discharge value to the hydrograph_data
            hydrograph_data.loc[hydrograph_data[code_col] == code, 'dangerous_discharge'] = dangerous_discharge
        except Exception:
            continue

    return hydrograph_data

def write_daily_time_series_data_to_csv(data: pd.DataFrame, column_list=["code", "date", "discharge"]):
    """
    Writes the data to a csv file for later reading by other forecast tools.

    Reads data from excel sheets and from the database (if access available).

    Args:
    data (pd.DataFrame): The data to be written to a csv file.
    column_list (list, optional): The list of columns to be written to the csv file.
        Default is ["code", "date", "discharge"].

    Returns:
    None upon success.

    Raises:
    Exception: If the data cannot be written to the csv file.
    """
    data = data.copy()

    # Get intermediate data path from environment variable
    intermediate_data_path = os.getenv("ieasyforecast_intermediate_data_path")
    if intermediate_data_path is None:
        raise ValueError("Environment variable ieasyforecast_intermediate_data_path is not set.")
    
    # Test if the intermediate data path exists. If not, create it.
    if not os.path.exists(intermediate_data_path):
        os.makedirs(intermediate_data_path)
        logger.info(f"[OUTPUT] Created directory {intermediate_data_path}.")

    # Get the path to the intermediate data folder from the environmental
    # variables and the name of the ieasyforecast_analysis_daily_file.
    # Concatenate them to the output file path.
    try:
       output_file_path = os.path.join(
            intermediate_data_path,
            os.getenv("ieasyforecast_daily_discharge_file"))
    except Exception as e:
        logger.error("[OUTPUT] Could not get the output file path.")
        logger.error(f"[OUTPUT] intermediate_data_path: {intermediate_data_path}")
        logger.error(f"[OUTPUT] ieasyforecast_daily_discharge_file: {os.getenv('ieasyforecast_daily_discharge_file')}")
        raise e

    # Test if the columns in column list are available in the dataframe
    for col in column_list:
        if col not in data.columns:
            raise ValueError(f"Column '{col}' not found in the DataFrame.")

    # Print head of data
    logger.debug(f'write_daily_time_series_data_to_csv: data.head(10)\n{data.head(10)}')

    # Round all values to 3 decimal places
    data = data.round(3)

    # Convert code to string without .0 suffixes from float-to-string conversion
    data['code'] = data['code'].astype(str).str.replace(r'\.0$', '', regex=True)
    
    # Ensure date is in %Y-%m-%d format
    if 'date' in data.columns:
        data['date'] = pd.to_datetime(data['date'], errors='coerce').dt.strftime('%Y-%m-%d')

    # Write the data to a csv file. Raise an error if this does not work.
    # Write data to CSV file
    logger.debug(f"[OUTPUT] Writing time series to {output_file_path}")
    try:
        ret = data.reset_index(drop=True)[column_list].to_csv(output_file_path, index=False)
        if ret is None:
            logger.info(f"[OUTPUT] Time series written: {output_file_path}")
            return ret
        else:
            logger.error(f"[OUTPUT] Failed to write time series: {output_file_path}")
    except Exception as e:
        logger.error(f"[OUTPUT] Failed to write time series: {output_file_path} - {e}")
        raise e

def write_daily_hydrograph_data_to_csv(data: pd.DataFrame, column_list=["code", "date", "discharge"]):
    """
    Writes the data to a csv file for later reading by other forecast tools.

    Reads data from excel sheets and from the database (if access available).

    Args:
    data (pd.DataFrame): The data to be written to a csv file.
    column_list (list, optional): The list of columns to be written to the csv file.
        Default is ["code", "date", "discharge"].

    Returns:
    None upon success.

    Raises:
    Exception: If the data cannot be written to the csv file.
    """

    # Get the path to the intermediate data folder from the environmental
    # variables and the name of the ieasyforecast_analysis_daily_file.
    # Concatenate them to the output file path.
    try:
       output_file_path = os.path.join(
            os.getenv("ieasyforecast_intermediate_data_path"),
            os.getenv("ieasyforecast_hydrograph_day_file"))
    except Exception as e:
        logger.error("[OUTPUT] Could not get the output file path.")
        logger.error(f"[OUTPUT] ieasyforecast_intermediate_data_path: {os.getenv('ieasyforecast_intermediate_data_path')}")
        logger.error(f"[OUTPUT] ieasyforecast_hydrograph_day_file: {os.getenv('ieasyforecast_hydrograph_day_file')}")
        raise e

    # Test if the columns in column list are available in the dataframe
    for col in column_list:
        if col not in data.columns:
            raise ValueError(f"Column '{col}' not found in the DataFrame.")

    # Round all values to 3 decimal places
    data = data.round(3)

    # Convert code to string without .0 suffixes from float-to-string conversion
    data['code'] = data['code'].astype(str).str.replace(r'\.0$', '', regex=True)
    
    # Ensure date is in %Y-%m-%d format
    if 'date' in data.columns:
        data['date'] = pd.to_datetime(data['date'], errors='coerce').dt.strftime('%Y-%m-%d')

    # Test if we have rows where count is 0. If so, drop these rows.
    data = data[data['count'] != 0]

    # Write data to CSV file
    logger.debug(f"[OUTPUT] Writing hydrograph to {output_file_path}")
    try:
        ret = data.reset_index(drop=True)[column_list].to_csv(output_file_path, index=False)
        if ret is None:
            logger.info(f"[OUTPUT] Hydrograph written: {output_file_path}")
            return ret
        else:
            logger.error(f"[OUTPUT] Failed to write hydrograph: {output_file_path}")
    except Exception as e:
        logger.error(f"[OUTPUT] Failed to write hydrograph: {output_file_path} - {e}")
        raise e


# =============================================================================
# Post-Write Validation Functions (Phase 3)
# =============================================================================

def validate_recent_data(
    output_df: pd.DataFrame,
    expected_sites: list,
    date_col: str = 'date',
    code_col: str = 'code',
    max_age_days: int = 3
) -> dict:
    """
    Validate that each site has data within the specified number of days.

    Forecasts require recent data (typically last 3 days). This function
    identifies sites missing recent data that may cause forecast failures.

    Args:
        output_df: DataFrame with discharge data (must have date and code columns)
        expected_sites: List of site codes that should have data
        date_col: Name of the date column
        code_col: Name of the site code column
        max_age_days: Maximum age of data in days (default: 3)

    Returns:
        dict with:
        - sites_with_recent_data: list of sites with data within max_age_days
        - sites_missing_recent_data: list of sites WITHOUT recent data
        - latest_date_per_site: dict of site_code -> latest date (as string)
        - reference_date: the date used as "today" for comparison
        - max_age_days: the threshold used
    """
    if output_df.empty:
        return {
            'sites_with_recent_data': [],
            'sites_missing_recent_data': list(expected_sites),
            'latest_date_per_site': {},
            'reference_date': pd.Timestamp.now().normalize().strftime('%Y-%m-%d'),
            'max_age_days': max_age_days
        }

    # Ensure date column is datetime
    df = output_df.copy()
    df[date_col] = pd.to_datetime(df[date_col])

    # Ensure code column is string for consistent comparison
    df[code_col] = df[code_col].astype(str)
    expected_sites_str = [str(s) for s in expected_sites]

    # Reference date (today at midnight)
    reference_date = pd.Timestamp.now().normalize()
    cutoff_date = reference_date - pd.Timedelta(days=max_age_days)

    # Get latest date per site
    latest_dates = df.groupby(code_col)[date_col].max().to_dict()

    sites_with_recent = []
    sites_missing_recent = []

    for site in expected_sites_str:
        if site in latest_dates:
            latest = latest_dates[site]
            if latest >= cutoff_date:
                sites_with_recent.append(site)
            else:
                sites_missing_recent.append(site)
        else:
            # Site not in data at all
            sites_missing_recent.append(site)

    # Convert dates to strings for JSON serialization
    latest_date_strings = {
        site: date.strftime('%Y-%m-%d') if pd.notna(date) else None
        for site, date in latest_dates.items()
    }

    return {
        'sites_with_recent_data': sites_with_recent,
        'sites_missing_recent_data': sites_missing_recent,
        'latest_date_per_site': latest_date_strings,
        'reference_date': reference_date.strftime('%Y-%m-%d'),
        'max_age_days': max_age_days
    }


def load_reliability_stats(stats_file: str) -> dict:
    """
    Load site reliability statistics from JSON file.

    Args:
        stats_file: Path to the reliability stats JSON file

    Returns:
        dict with site_code -> stats mapping, or empty dict if file doesn't exist
    """
    if not os.path.exists(stats_file):
        logger.debug(f"[DATA] Reliability stats file not found: {stats_file}")
        return {}

    try:
        with open(stats_file, 'r') as f:
            stats = json.load(f)
        logger.debug(f"[DATA] Loaded reliability stats for {len(stats)} sites")
        return stats
    except Exception as e:
        logger.warning(f"[DATA] Failed to load reliability stats: {e}")
        return {}


def save_reliability_stats(stats_file: str, stats: dict) -> bool:
    """
    Save site reliability statistics to JSON file.

    Args:
        stats_file: Path to the reliability stats JSON file
        stats: dict with site_code -> stats mapping

    Returns:
        True if saved successfully, False otherwise
    """
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(stats_file), exist_ok=True)

        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)
        logger.debug(f"[DATA] Saved reliability stats for {len(stats)} sites")
        return True
    except Exception as e:
        logger.error(f"[DATA] Failed to save reliability stats: {e}")
        return False


def update_reliability_stats(
    stats: dict,
    validation_result: dict,
    reference_date: str
) -> dict:
    """
    Update reliability statistics based on validation results.

    Args:
        stats: Current reliability stats dict (site_code -> stats)
        validation_result: Result from validate_recent_data()
        reference_date: Date of this validation run (YYYY-MM-DD)

    Returns:
        Updated stats dict
    """
    sites_with_data = set(validation_result['sites_with_recent_data'])
    sites_missing_data = set(validation_result['sites_missing_recent_data'])
    all_sites = sites_with_data | sites_missing_data

    for site in all_sites:
        if site not in stats:
            stats[site] = {
                'checks': 0,
                'recent_data_present': 0,
                'reliability_pct': 0.0,
                'last_gap_date': None
            }

        stats[site]['checks'] += 1

        if site in sites_with_data:
            stats[site]['recent_data_present'] += 1
        else:
            stats[site]['last_gap_date'] = reference_date

        # Recalculate reliability percentage
        checks = stats[site]['checks']
        present = stats[site]['recent_data_present']
        stats[site]['reliability_pct'] = round(100 * present / checks, 1) if checks > 0 else 0.0

    return stats


def log_validation_summary(
    validation_result: dict,
    stats: dict,
    reliability_threshold: float = 80.0
) -> None:
    """
    Log a summary of the validation results.

    Args:
        validation_result: Result from validate_recent_data()
        stats: Reliability stats dict
        reliability_threshold: Percentage below which sites are flagged (default: 80%)
    """
    sites_with = validation_result['sites_with_recent_data']
    sites_missing = validation_result['sites_missing_recent_data']
    latest_dates = validation_result['latest_date_per_site']
    max_age = validation_result['max_age_days']
    total = len(sites_with) + len(sites_missing)

    if total == 0:
        logger.warning("[DATA] No sites to validate")
        return

    pct_with_data = 100 * len(sites_with) / total

    logger.info(f"[DATA] === Recent Data Validation ===")
    logger.info(f"[DATA] Sites checked: {total}")
    logger.info(f"[DATA] Sites with data in last {max_age} days: {len(sites_with)} ({pct_with_data:.1f}%)")
    logger.info(f"[DATA] Sites missing recent data: {len(sites_missing)}")

    if sites_missing:
        logger.warning(f"[DATA] MISSING RECENT DATA (forecast may fail):")
        for site in sorted(sites_missing):
            latest = latest_dates.get(site, 'never')
            reliability = stats.get(site, {}).get('reliability_pct', 'N/A')
            if isinstance(reliability, float):
                reliability = f"{reliability:.0f}%"
            logger.warning(f"[DATA]   - {site}: Last data {latest} (reliability: {reliability})")

    # Find low reliability sites
    low_reliability = [
        (site, s['reliability_pct'])
        for site, s in stats.items()
        if s['checks'] >= 5 and s['reliability_pct'] < reliability_threshold
    ]

    if low_reliability:
        logger.warning(f"[DATA] LOW RELIABILITY SITES (< {reliability_threshold:.0f}% over recent checks):")
        for site, pct in sorted(low_reliability, key=lambda x: x[1]):
            logger.warning(f"[DATA]   - {site}: {pct:.0f}% reliability")


def run_post_write_validation(
    output_df: pd.DataFrame,
    expected_sites: list,
    stats_file: str,
    date_col: str = 'date',
    code_col: str = 'code',
    max_age_days: int = 3,
    reliability_threshold: float = 80.0
) -> dict:
    """
    Run full post-write validation: check recent data and update reliability stats.

    This is the main entry point for Phase 3 validation.

    Args:
        output_df: DataFrame with discharge data
        expected_sites: List of site codes that should have data
        stats_file: Path to reliability stats JSON file
        date_col: Name of the date column
        code_col: Name of the site code column
        max_age_days: Maximum age of data in days (default: 3)
        reliability_threshold: Percentage below which sites are flagged (default: 80%)

    Returns:
        Validation result dict (same as validate_recent_data output)
    """
    # Validate recent data
    validation_result = validate_recent_data(
        output_df, expected_sites, date_col, code_col, max_age_days
    )

    # Load existing stats
    stats = load_reliability_stats(stats_file)

    # Update stats
    stats = update_reliability_stats(
        stats, validation_result, validation_result['reference_date']
    )

    # Save updated stats
    save_reliability_stats(stats_file, stats)

    # Log summary
    log_validation_summary(validation_result, stats, reliability_threshold)

    return validation_result


# =============================================================================
# Spot-Check Validation Functions (Phase 4)
# =============================================================================

def get_spot_check_sites() -> list:
    """
    Get list of spot-check site codes from environment variable.

    Environment variable:
    - IEASYHYDRO_SPOTCHECK_SITES (comma-separated site codes)

    Returns:
        list of site codes (strings), or empty list if not configured
    """
    sites_str = os.getenv('IEASYHYDRO_SPOTCHECK_SITES')
    if sites_str:
        sites = [s.strip() for s in sites_str.split(',') if s.strip()]
        if sites:
            logger.debug(f"[DATA] Spot-check sites from IEASYHYDRO_SPOTCHECK_SITES: {sites}")
            return sites

    logger.debug("[DATA] No spot-check sites configured")
    return []


def spot_check_sites(
    sdk,
    site_codes: list,
    output_df: pd.DataFrame,
    date_col: str = 'date',
    code_col: str = 'code',
    value_col: str = 'discharge',
    target_timezone=None,
    variable_name: str = "WDDA"
) -> dict:
    """
    Verify data for spot-check sites by comparing output values against API.

    For each spot-check site:
    1. Fetch the latest available value from API for the specified variable
    2. Get the corresponding date's value from output_df
    3. Compare the values

    Args:
        sdk: iEasyHydro HF SDK instance
        site_codes: List of site codes to spot-check
        output_df: DataFrame with discharge data
        date_col: Name of the date column
        code_col: Name of the site code column
        value_col: Name of the value column
        target_timezone: Timezone for API queries (not used, kept for compatibility)
        variable_name: Variable to check - "WDDA" (daily average) or "WDD" (morning)

    Returns:
        dict with:
        - results: dict of site_code -> {output_date, output_value, api_date, api_value, match, error}
        - summary: {checked, matched, mismatched, errors}
        - variable: the variable name checked
    """
    if not site_codes:
        return {'results': {}, 'summary': {'checked': 0, 'matched': 0, 'mismatched': 0, 'errors': 0}, 'variable': variable_name}

    if output_df.empty:
        logger.warning("[DATA] Spot-check: output DataFrame is empty")
        return {'results': {}, 'summary': {'checked': 0, 'matched': 0, 'mismatched': 0, 'errors': 0}, 'variable': variable_name}

    # Ensure proper types
    df = output_df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df[code_col] = df[code_col].astype(str)

    results = {}
    matched = 0
    mismatched = 0
    errors = 0

    for site_code in site_codes:
        site_code_str = str(site_code)
        result = {
            'output_date': None,
            'output_value': None,
            'api_date': None,
            'api_value': None,
            'match': None,
            'error': None
        }

        # Fetch latest available data from API first (last 30 days to find most recent)
        try:
            end_date = dt.datetime.now()
            start_date = end_date - dt.timedelta(days=30)

            filters = {
                "site_codes": [site_code_str],  # Must be a list
                "variable_names": [variable_name],
                "local_date_time__gte": start_date.strftime('%Y-%m-%dT00:00:00'),
                "local_date_time__lte": end_date.strftime('%Y-%m-%dT23:59:59'),
                "page": 1,
                "page_size": 10,  # API limit
            }

            logger.debug(f"[DATA] Spot-check {site_code_str} ({variable_name}): Querying API with filters: {filters}")
            response = sdk.get_data_values_for_site(filters=filters)

            if isinstance(response, dict) and 'status_code' in response:
                # Log full response for debugging
                logger.warning(f"[DATA] Spot-check {site_code_str}: Full API error response: {response}")
                result['error'] = f"API error: {response.get('status_code')} - {response.get('detail', response.get('message', 'unknown'))}"
                errors += 1
            elif isinstance(response, dict) and 'results' in response:
                api_results = response.get('results', [])

                # Extract all data points and find the latest
                all_values = []
                for item in api_results:
                    if item.get('station_type') == 'hydro':
                        data_list = item.get('data', [])
                        for data_item in data_list:
                            values = data_item.get('values', [])
                            for v in values:
                                if v.get('value') is not None and v.get('timestamp_local'):
                                    try:
                                        # Parse the date from API
                                        date_str = v['timestamp_local']
                                        # Handle various date formats
                                        if 'T' in date_str:
                                            api_dt = dt.datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                                        else:
                                            api_dt = dt.datetime.strptime(date_str, '%Y-%m-%d')
                                        all_values.append({
                                            'date': api_dt.date(),
                                            'value': float(v['value'])
                                        })
                                    except (ValueError, KeyError) as e:
                                        logger.debug(f"[DATA] Spot-check {site_code_str}: Could not parse date: {e}")

                if not all_values:
                    result['error'] = f'No {variable_name} data from API in last 30 days'
                    errors += 1
                else:
                    # Find the latest value from API
                    latest_api = max(all_values, key=lambda x: x['date'])
                    result['api_date'] = latest_api['date'].strftime('%Y-%m-%d')
                    result['api_value'] = latest_api['value']

                    # Now get the corresponding date's value from output
                    site_data = df[df[code_col] == site_code_str]
                    if site_data.empty:
                        result['error'] = 'Site not in output data'
                        errors += 1
                    else:
                        # Find the output value for the API's latest date
                        api_date_ts = pd.Timestamp(latest_api['date'])
                        matching_output = site_data[site_data[date_col].dt.date == latest_api['date']]

                        if matching_output.empty:
                            result['error'] = f"No output data for API date {result['api_date']}"
                            errors += 1
                        else:
                            output_row = matching_output.iloc[0]
                            output_value = output_row[value_col]

                            if pd.isna(output_value):
                                result['error'] = f"Output value is null for {result['api_date']}"
                                errors += 1
                            else:
                                result['output_date'] = result['api_date']  # Same date
                                result['output_value'] = float(output_value)

                                # Compare values
                                tolerance = 0.01  # Allow small floating point differences
                                diff = abs(result['output_value'] - result['api_value'])
                                result['match'] = diff < tolerance
                                if result['match']:
                                    matched += 1
                                else:
                                    mismatched += 1
            else:
                result['error'] = 'Unexpected API response format'
                errors += 1

        except Exception as e:
            import traceback
            logger.error(f"[DATA] Spot-check {site_code_str}: Exception during API call: {e}")
            logger.debug(f"[DATA] Spot-check {site_code_str}: Stack trace:\n{traceback.format_exc()}")
            result['error'] = str(e)
            errors += 1

        results[site_code_str] = result

    return {
        'results': results,
        'summary': {
            'checked': len(site_codes),
            'matched': matched,
            'mismatched': mismatched,
            'errors': errors
        },
        'variable': variable_name
    }


def spot_check_sites_dual(
    sdk,
    site_codes: list,
    output_df: pd.DataFrame,
    date_col: str = 'date',
    code_col: str = 'code',
    value_col: str = 'discharge',
    target_timezone=None
) -> dict:
    """
    Run spot-check validation for both WDDA (daily average) and WDD (morning) data.

    This function checks both variable types separately to handle the case where
    morning data (WDD) creates a "today" entry before daily average (WDDA) is available.

    Args:
        sdk: iEasyHydro HF SDK instance
        site_codes: List of site codes to spot-check
        output_df: DataFrame with discharge data
        date_col: Name of the date column
        code_col: Name of the site code column
        value_col: Name of the value column
        target_timezone: Timezone for API queries

    Returns:
        dict with:
        - wdda: spot_check_sites result for daily average discharge
        - wdd: spot_check_sites result for morning discharge
        - combined_summary: aggregated summary across both checks
    """
    # Check WDDA (daily average discharge)
    wdda_result = spot_check_sites(
        sdk=sdk,
        site_codes=site_codes,
        output_df=output_df,
        date_col=date_col,
        code_col=code_col,
        value_col=value_col,
        target_timezone=target_timezone,
        variable_name="WDDA"
    )

    # Check WDD (morning discharge)
    wdd_result = spot_check_sites(
        sdk=sdk,
        site_codes=site_codes,
        output_df=output_df,
        date_col=date_col,
        code_col=code_col,
        value_col=value_col,
        target_timezone=target_timezone,
        variable_name="WDD"
    )

    # Combine summaries
    # A site is considered "matched" if it matches in at least one variable
    # A site is "mismatched" only if it has data but values differ
    # A site has "error" only if both variables have errors
    combined_matched = 0
    combined_mismatched = 0
    combined_errors = 0

    for site_code in site_codes:
        site_str = str(site_code)
        wdda_res = wdda_result['results'].get(site_str, {})
        wdd_res = wdd_result['results'].get(site_str, {})

        wdda_match = wdda_res.get('match')
        wdd_match = wdd_res.get('match')
        wdda_error = wdda_res.get('error')
        wdd_error = wdd_res.get('error')

        # Site passes if either WDDA or WDD matches
        if wdda_match is True or wdd_match is True:
            combined_matched += 1
        elif wdda_match is False or wdd_match is False:
            # Has data but value mismatch
            combined_mismatched += 1
        else:
            # Both have errors (no data available)
            combined_errors += 1

    return {
        'wdda': wdda_result,
        'wdd': wdd_result,
        'combined_summary': {
            'checked': len(site_codes),
            'matched': combined_matched,
            'mismatched': combined_mismatched,
            'errors': combined_errors
        }
    }


def log_spot_check_summary(spot_check_result: dict) -> None:
    """
    Log a summary of the spot-check validation results.

    Handles both single-variable results (from spot_check_sites) and
    dual-variable results (from spot_check_sites_dual).

    Args:
        spot_check_result: Result from spot_check_sites() or spot_check_sites_dual()
    """
    # Check if this is a dual result (has 'wdda' and 'wdd' keys)
    if 'wdda' in spot_check_result and 'wdd' in spot_check_result:
        _log_dual_spot_check_summary(spot_check_result)
    else:
        _log_single_spot_check_summary(spot_check_result)


def _log_single_spot_check_summary(spot_check_result: dict) -> None:
    """Log summary for a single-variable spot-check result."""
    results = spot_check_result.get('results', {})
    summary = spot_check_result.get('summary', {})
    variable = spot_check_result.get('variable', 'WDDA')

    if summary.get('checked', 0) == 0:
        logger.debug(f"[DATA] Spot-check ({variable}): No sites configured")
        return

    logger.info(f"[DATA] === Spot-Check Validation ({variable}) ===")
    logger.info(f"[DATA] Sites checked: {summary['checked']}")
    logger.info(f"[DATA] Matched: {summary['matched']}, Mismatched: {summary['mismatched']}, Errors: {summary['errors']}")

    # Report details for mismatches and errors
    for site, result in results.items():
        if result.get('error'):
            logger.warning(f"[DATA] Spot-check {site} ({variable}): ERROR - {result['error']}")
        elif result.get('match') is False:
            logger.warning(
                f"[DATA] Spot-check {site} ({variable}): MISMATCH - "
                f"output={result['output_value']} ({result['output_date']}), "
                f"api={result['api_value']} ({result.get('api_date', 'N/A')})"
            )
        else:
            logger.debug(
                f"[DATA] Spot-check {site} ({variable}): OK - value={result['output_value']} "
                f"(date: {result['output_date']})"
            )


def _log_dual_spot_check_summary(spot_check_result: dict) -> None:
    """Log summary for a dual-variable (WDDA + WDD) spot-check result."""
    wdda = spot_check_result.get('wdda', {})
    wdd = spot_check_result.get('wdd', {})
    combined = spot_check_result.get('combined_summary', {})

    if combined.get('checked', 0) == 0:
        logger.debug("[DATA] Spot-check: No sites configured")
        return

    logger.info("[DATA] === Spot-Check Validation ===")
    logger.info(f"[DATA] Sites checked: {combined['checked']}")
    logger.info(f"[DATA] Result: {combined['matched']} OK, {combined['mismatched']} value mismatch, {combined['errors']} no data")

    wdda_summary = wdda.get('summary', {})
    wdd_summary = wdd.get('summary', {})
    logger.debug(f"[DATA]   WDDA (daily avg): {wdda_summary.get('matched', 0)} OK, {wdda_summary.get('mismatched', 0)} mismatch, {wdda_summary.get('errors', 0)} no data")
    logger.debug(f"[DATA]   WDD (morning):    {wdd_summary.get('matched', 0)} OK, {wdd_summary.get('mismatched', 0)} mismatch, {wdd_summary.get('errors', 0)} no data")

    # Report per-site details
    wdda_results = wdda.get('results', {})
    wdd_results = wdd.get('results', {})
    all_sites = set(wdda_results.keys()) | set(wdd_results.keys())

    for site in sorted(all_sites):
        wdda_res = wdda_results.get(site, {})
        wdd_res = wdd_results.get(site, {})

        wdda_match = wdda_res.get('match')
        wdd_match = wdd_res.get('match')

        # Determine overall status for this site
        if wdda_match is True or wdd_match is True:
            # At least one matches - report success with details
            details = []
            if wdda_match is True:
                details.append(f"WDDA={wdda_res.get('output_value')} ({wdda_res.get('output_date')})")
            if wdd_match is True:
                details.append(f"WDD={wdd_res.get('output_value')} ({wdd_res.get('output_date')})")
            logger.debug(f"[DATA] Spot-check {site}: OK - {', '.join(details)}")
        elif wdda_match is False or wdd_match is False:
            # Value mismatch - this is a real problem
            if wdda_match is False:
                logger.warning(
                    f"[DATA] Spot-check {site} (WDDA): VALUE MISMATCH - "
                    f"output={wdda_res.get('output_value')}, api={wdda_res.get('api_value')} "
                    f"(date: {wdda_res.get('api_date')})"
                )
            if wdd_match is False:
                logger.warning(
                    f"[DATA] Spot-check {site} (WDD): VALUE MISMATCH - "
                    f"output={wdd_res.get('output_value')}, api={wdd_res.get('api_value')} "
                    f"(date: {wdd_res.get('api_date')})"
                )
        else:
            # Both have no data - report but this might be expected for some sites
            wdda_error = wdda_res.get('error', 'Unknown')
            wdd_error = wdd_res.get('error', 'Unknown')
            logger.warning(f"[DATA] Spot-check {site}: NO DATA available - WDDA: {wdda_error}")


def run_spot_check_validation(
    sdk,
    output_df: pd.DataFrame,
    target_timezone=None,
    date_col: str = 'date',
    code_col: str = 'code',
    value_col: str = 'discharge'
) -> dict:
    """
    Run spot-check validation if sites are configured.

    This is the main entry point for spot-check validation.
    Checks both WDDA (daily average) and WDD (morning) data separately
    to handle the case where morning data creates a "today" entry
    before the daily average is available.

    Args:
        sdk: iEasyHydro HF SDK instance
        output_df: DataFrame with discharge data
        target_timezone: Timezone for API queries
        date_col: Name of the date column
        code_col: Name of the site code column
        value_col: Name of the value column

    Returns:
        Dual spot-check result dict (from spot_check_sites_dual)
    """
    site_codes = get_spot_check_sites()

    if not site_codes:
        logger.info("[DATA] Spot-check validation skipped (no sites configured)")
        return {
            'wdda': {'results': {}, 'summary': {'checked': 0, 'matched': 0, 'mismatched': 0, 'errors': 0}, 'variable': 'WDDA'},
            'wdd': {'results': {}, 'summary': {'checked': 0, 'matched': 0, 'mismatched': 0, 'errors': 0}, 'variable': 'WDD'},
            'combined_summary': {'checked': 0, 'matched': 0, 'mismatched': 0, 'errors': 0}
        }

    logger.info(f"[DATA] Running spot-check validation for {len(site_codes)} sites...")

    result = spot_check_sites_dual(
        sdk=sdk,
        site_codes=site_codes,
        output_df=output_df,
        date_col=date_col,
        code_col=code_col,
        value_col=value_col,
        target_timezone=target_timezone
    )

    log_spot_check_summary(result)

    return result


# =============================================================================
# SITE CACHING (Phase 6)
# =============================================================================

def load_site_cache(cache_file: str, max_age_days: int = 7) -> dict | None:
    """
    Load cached site data from JSON file.

    Supports both cache versions:
    - Version 1: pentad/decad split (legacy)
    - Version 2: unified site list (current)

    Args:
        cache_file: Path to the cache file
        max_age_days: Maximum age in days before cache is considered stale

    Returns:
        dict with 'site_codes' and 'site_ids' if valid, None if missing or expired
    """
    import json

    if not os.path.exists(cache_file):
        logger.debug(f"[CONFIG] Site cache not found: {cache_file}")
        return None

    try:
        with open(cache_file, 'r') as f:
            cache = json.load(f)

        version = cache.get('cache_version', 0)

        # Check cache age
        cached_at = cache.get('cached_at')
        is_stale = False
        if cached_at:
            cache_time = dt.datetime.fromisoformat(cached_at)
            age_days = (dt.datetime.now(cache_time.tzinfo) - cache_time).days
            if age_days > max_age_days:
                logger.warning(f"[CONFIG] Site cache is {age_days} days old (max: {max_age_days}), cache is stale")
                is_stale = True
            else:
                logger.debug(f"[CONFIG] Site cache loaded, {age_days} days old")

        # Handle different versions
        if version == 2:
            # Version 2: unified format
            result = {
                'site_codes': cache.get('site_codes', []),
                'site_ids': cache.get('site_ids', []),
                'is_stale': is_stale
            }
        elif version == 1:
            # Version 1: pentad/decad split - convert to unified format
            pentad_codes = cache.get('pentad', {}).get('site_codes', [])
            decad_codes = cache.get('decad', {}).get('site_codes', [])
            pentad_ids = cache.get('pentad', {}).get('site_ids', [])
            decad_ids = cache.get('decad', {}).get('site_ids', [])

            # Combine and deduplicate
            seen = set()
            site_codes = []
            site_ids = []
            for code, site_id in zip(pentad_codes + decad_codes, pentad_ids + decad_ids):
                if code not in seen:
                    seen.add(code)
                    site_codes.append(code)
                    site_ids.append(site_id)

            result = {
                'site_codes': site_codes,
                'site_ids': site_ids,
                'is_stale': is_stale
            }
            logger.debug(f"[CONFIG] Converted v1 cache: {len(site_codes)} unique sites")
        else:
            logger.warning(f"[CONFIG] Unknown cache version {version}, ignoring cache")
            return None

        return result

    except Exception as e:
        logger.warning(f"[CONFIG] Failed to load site cache: {e}")
        return None


def save_site_cache(
    cache_file: str,
    site_codes: list,
    site_ids: list = None
) -> bool:
    """
    Save site codes to cache file with timestamp.

    Args:
        cache_file: Path to the cache file
        site_codes: List of all forecast site codes (already deduplicated)
        site_ids: Optional list of site IDs (same order as site_codes)

    Returns:
        True if saved successfully, False otherwise
    """
    import json

    cache = {
        "cached_at": dt.datetime.now().astimezone().isoformat(),
        "cache_version": 2,  # Version 2 uses unified site list
        "site_codes": site_codes,
        "site_ids": site_ids or [],
        "site_count": len(site_codes)
    }

    try:
        # Ensure directory exists
        cache_dir = os.path.dirname(cache_file)
        if cache_dir and not os.path.exists(cache_dir):
            os.makedirs(cache_dir, exist_ok=True)

        with open(cache_file, 'w') as f:
            json.dump(cache, f, indent=2)

        logger.info(f"[CONFIG] Site cache saved: {len(site_codes)} sites")
        return True

    except Exception as e:
        logger.warning(f"[CONFIG] Failed to save site cache: {e}")
        return False


def get_unique_site_codes(pentad_codes: list, decad_codes: list) -> list:
    """
    Combine and deduplicate site codes from pentad and decad lists.

    Args:
        pentad_codes: List of pentadal forecast site codes
        decad_codes: List of decadal forecast site codes

    Returns:
        List of unique site codes (preserving order, pentad first)
    """
    # Use dict.fromkeys to preserve order while removing duplicates
    combined = list(dict.fromkeys(pentad_codes + decad_codes))
    
    if len(pentad_codes) + len(decad_codes) != len(combined):
        duplicates = len(pentad_codes) + len(decad_codes) - len(combined)
        logger.debug(f"[CONFIG] Deduplicated site codes: {len(pentad_codes)} + {len(decad_codes)} -> {len(combined)} unique ({duplicates} duplicates removed)")
    
    return combined

