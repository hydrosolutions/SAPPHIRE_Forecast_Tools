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
from typing import List, Tuple, Optional
from dataclasses import dataclass
from contextlib import contextmanager

# To avoid printing of warning
# pd.set_option('future.no_silent_downcasting', True)  # Comment out - not available in current pandas version

from ieasyhydro_sdk.filters import BasicDataValueFilters

# SAPPHIRE API client for database writes
try:
    from sapphire_api_client import SapphirePreprocessingClient, SapphireAPIError
    SAPPHIRE_API_AVAILABLE = True
except ImportError:
    SAPPHIRE_API_AVAILABLE = False
    SapphirePreprocessingClient = None
    SapphireAPIError = Exception  # Fallback for type hints

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


@dataclass
class RunoffDataResult:
    """
    Result container for runoff data fetching operations.

    Attributes:
        full_data: Complete DataFrame with all historical + new data (for CSV output)
        new_data: DataFrame containing only newly fetched data (for API output)
    """
    full_data: pd.DataFrame
    new_data: pd.DataFrame


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
    combined_data = combined_data.dropna(subset=[group_by])

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
        print(f"read_runoff_data_from_csv_files: raw df.head(): \n{df.head()}")
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

    print(f"read_runoff_data_from_csv_files: final df.head: \n{df.head()}")

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
    """
    hydro_data = [site_data for site_data in data['results'] if site_data['station_type'] == 'hydro']
    processed_data = []
    for site in hydro_data:
        station_code = site['station_code']
        station_id = site['station_id']
        station_name = site['station_name']
        for data_item in site['data']:
            variable_code = data_item['variable_code']
            unit = data_item['unit']
            for value_item in data_item['values']:
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
    return pd.DataFrame(processed_data)

def fetch_and_format_hydro_HF_data(sdk, initial_filters):
    """
    Fetch all pages of data from the API and format each page into a DataFrame immediately.
    
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
        station_code, station_name, station_type, station_id, station_uuid, 
        variable_code, unit, timestamp_local, timestamp_utc, value, value_type, value_code
    """
    # Copy filters
    filters = initial_filters.copy()
    
    # Start with page 1
    page = 1
    all_data_frames = []
    
    while True:
        # Fetch data for the current page
        filters['page'] = page
        response = sdk.get_data_values_for_site(filters=filters)
        
        # Check if we got an error
        if isinstance(response, dict) and 'status_code' in response:
            print(f"Error: {response}")
            break
        
        # Extract the results and format immediately
        if isinstance(response, dict) and 'results' in response:
            # Extract the results
            results = response['results']
            
            # Format this page's data immediately
            if results:  # Only process if we have results
                records = []
                
                page_df = process_hydro_HF_data(response)

                # Convert timestamps to datetime if they're strings
                for col in ['local_datetime', 'utc_datetime']:
                    if col in page_df.columns and page_df[col].dtype == 'object':
                        page_df[col] = pd.to_datetime(page_df[col])
                
                # Add this page's data to our collection
                all_data_frames.append(page_df)
                
                logger.info(f"Processed page {page}: {len(page_df)} records")
                
        else:
            logger.warning(f"Expected 'results' key not found in response")
            logger.info(f"Response keys: {list(response.keys()) if isinstance(response, dict) else 'Not a dict'}")
        
        # Check if there are more pages
        if isinstance(response, dict) and response.get('next'):
            # Increment page number
            logger.info(f"Fetching page {page}...")
            page += 1
        else:
            # No more pages
            break
    
    # Combine all DataFrames
    if all_data_frames:
        combined_df = pd.concat(all_data_frames, ignore_index=True)
        # If the DataFrame is empty, return an empty DataFrame with the expected structure
        if combined_df.empty:
            return pd.DataFrame(columns=[
                'station_code', 'timestamp_local', 
                'timestamp_utc', 'value'
            ])
        # Drop columns that are not in the expected structure
        combined_df.drop(
            columns=['station_uuid', 'station_type', 'station_id', 
                     'variable_code', 'unit', 'value_type', 'value_code', 
                     'station_name'], 
            inplace=True, errors='ignore')
        logger.debug(f"Combined DataFrame: \n{combined_df}")
        return combined_df
    else:
        # Return empty DataFrame with the expected structure
        return pd.DataFrame(columns=[
            'station_code', 'timestamp_local', 
            'timestamp_utc', 'value'
        ])

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

    filters = {
        "site_ids": id_list,
        "variable_names": ["WDD"],
        "local_date_time__gte": start_datetime.isoformat(),
        "local_date_time__lte": end_datetime.isoformat(),
        "page": 1,
    }

    try:
        # Get data for all sites from the database
        db_df = fetch_and_format_hydro_HF_data(ieh_hf_sdk, filters)
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

    filters = {
        "site_ids": id_list,
        "variable_names": ["WDDA"],
        "local_date_time__gte": start_datetime.isoformat(),
        "local_date_time__lte": end_datetime.isoformat(),
        "page": 1,
    }

    try:
        # Get data for all sites from the database
        db_df = fetch_and_format_hydro_HF_data(ieh_hf_sdk, filters)
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
        read_data = read_data[read_data[code_col] != 'NA']

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
        read_data = read_data[read_data[code_col] != 'NA']

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
        read_data = read_data[read_data[code_col] != 'NA']

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
                              target_timezone=None) -> RunoffDataResult:
    """
    Reads runoff data from excel and, if possible, from iEasyHydro database.

    Note: This function will intelligently determine the date range to fetch from 
    the iEasyHydro database based on the latest date in the existing data.

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

    Returns:
        RunoffDataResult: Contains full_data (all historical + new) and new_data (only newly fetched).
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
        except Exception as e:
            logger.warning(f"Failed to read cached data: {e}, reprocessing input files")
            organization = os.getenv('ieasyhydroforecast_organization')
            read_data = _read_runoff_data_by_organization(
                organization=organization,
                date_col=date_col,
                discharge_col=discharge_col,
                name_col=name_col,
                code_col=code_col,
                code_list=code_list
            )

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
        # Return with empty new_data since no new data was fetched
        return RunoffDataResult(
            full_data=read_data,
            new_data=pd.DataFrame(columns=[code_col, date_col, discharge_col, name_col])
        )

    else:
        # Collect new data from iEasyHydro in a list
        new_data_chunks = []
        
        # Determine the latest date in the existing data to fetch only missing data
        if not read_data.empty:
            latest_date = pd.to_datetime(read_data[date_col]).max()
            start_date = latest_date + pd.Timedelta(days=1)
            logger.info(f"Latest date in existing data: {latest_date}. Fetching data from {start_date} onwards.")
            
            # Ensure start_date is not in the future
            today = pd.Timestamp.now().normalize()
            if start_date > today:
                logger.info(f"Start date {start_date.date()} is in the future. Setting to today.")
                start_date = pd.Timestamp(today)
        else:
            # For empty data, fetch data starting from January 1, 2024 or a reasonable time ago (e.g., 5 years)
            start_date = pd.Timestamp('2024-01-01')
            logger.info(f"No existing data. Fetching data from {start_date} onwards.")

        # Make sure we have timezone-aware datetime objects for API calls
        if target_timezone is None:
            target_timezone = pytz.timezone(time.tzname[0])
            logger.debug(f"Target timezone is None. Using local timezone: {target_timezone}")
            
        # Convert start_date to timezone-aware datetime
        start_datetime = pd.Timestamp(start_date).replace(hour=0, minute=1)
        start_datetime = start_datetime.tz_localize(target_timezone)
        
        # Set end datetime to current time
        end_datetime = pd.Timestamp.now().tz_localize(target_timezone)

        # Fetch data from iEH HF database for the calculated date range
        logger.info(f"Fetching data from {start_datetime} to {end_datetime}")
        db_average_data = get_daily_average_discharge_from_iEH_HF_for_multiple_sites(
            ieh_hf_sdk=ieh_hf_sdk, 
            id_list=id_list,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            target_timezone=target_timezone,
            date_col=date_col, 
            discharge_col=discharge_col, 
            code_col=code_col
        )
        
        # We are dealing with daily data, so we convert date_col to date
        if not db_average_data.empty:
            db_average_data = standardize_date_column(db_average_data, date_col=date_col)
            logger.info(f"Retrieved {len(db_average_data)} new records from {db_average_data[date_col].min()} to {db_average_data[date_col].max()}")
            new_data_chunks.append(db_average_data.copy())
            
            # Append db_data to read_data if db_data is not empty
            read_data = pd.concat([read_data, db_average_data], ignore_index=True)
            
        # Get today's morning data if necessary
        db_morning_data = get_todays_morning_discharge_from_iEH_HF_for_multiple_sites(
            ieh_hf_sdk=ieh_hf_sdk, 
            id_list=id_list,
            target_timezone=target_timezone,
            date_col=date_col, 
            discharge_col=discharge_col, 
            code_col=code_col
        )
        
        if not db_morning_data.empty:
            db_morning_data = standardize_date_column(db_morning_data, date_col=date_col)
            logger.info(f"Retrieved {len(db_morning_data)} morning records for {db_morning_data[date_col].min()}")
            new_data_chunks.append(db_morning_data.copy())
            read_data = pd.concat([read_data, db_morning_data], ignore_index=True)
        
        # Combine all new data chunks
        if new_data_chunks:
            new_data = pd.concat(new_data_chunks, ignore_index=True)
        else:
            new_data = pd.DataFrame(columns=[code_col, date_col, discharge_col, name_col])
        
        # Drop rows where 'code' is "NA" from both datasets
        read_data = read_data[read_data[code_col] != 'NA']
        if not new_data.empty:
            new_data = new_data[new_data[code_col] != 'NA']

        # Cast the 'code' column to string
        read_data[code_col] = read_data[code_col].astype(str)
        if not new_data.empty:
            new_data[code_col] = new_data[code_col].astype(str)

        # Calculate virtual hydropost data where necessary
        if virtual_stations_present:
            read_data = calculate_virtual_stations_data(read_data)
            # Note: virtual station calculations for new_data would need the full 
            # historical context, so we skip it here. The full_data already has them.

        # For sanity sake, we round the data to a max of 3 decimal places
        read_data[discharge_col] = read_data[discharge_col].round(3)
        if not new_data.empty:
            new_data[discharge_col] = new_data[discharge_col].round(3)

        # Make sure the date column is in datetime format
        read_data[date_col] = pd.to_datetime(read_data[date_col]).dt.normalize()
        if not new_data.empty:
            new_data[date_col] = pd.to_datetime(new_data[date_col]).dt.normalize()
        
        # Remove duplicate data (in case DB had overlapping data)
        read_data = read_data.drop_duplicates(subset=[code_col, date_col], keep='last')
        if not new_data.empty:
            new_data = new_data.drop_duplicates(subset=[code_col, date_col], keep='last')
        
        # Sort data by code and date
        read_data = read_data.sort_values([code_col, date_col])
        if not new_data.empty:
            new_data = new_data.sort_values([code_col, date_col])

        logger.info(f"Returning {len(read_data)} total records, {len(new_data)} new records for API")
        return RunoffDataResult(full_data=read_data, new_data=new_data)

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
    print("FIRST 5 DAYS:")
    print(site_data.head(5))
    print("\n")
    
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
        
        print("DAYS AROUND FEB 28:")
        print(date_range_data)
    else:
        print("DAYS AROUND FEB 28:")
        print("February 28th not found in the data")
    
    print("\n")
    
    # Last 5 days of the year
    print("LAST 5 DAYS:")
    print(site_data.tail(5))

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
        print(f"\n\n\n\nsite: {site.code}: {site.qdanger}")
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

def _write_runoff_to_api(data: pd.DataFrame) -> bool:
    """
    Write daily runoff data to SAPPHIRE API.

    Args:
        data: DataFrame with columns 'code', 'date', 'discharge'

    Returns:
        True if successful, False otherwise

    Raises:
        SapphireAPIError: If API write fails after retries
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.warning("sapphire-api-client not installed, skipping API write")
        return False

    # Get API URL from environment, default to localhost
    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    # Check if API writing is enabled (default: enabled)
    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
    if not api_enabled:
        logger.info("SAPPHIRE API writing disabled via SAPPHIRE_API_ENABLED=false")
        print("SAPPHIRE API writing disabled (SAPPHIRE_API_ENABLED=false)")
        return False

    print(f"DEBUG: Creating client with base_url={api_url}")
    client = SapphirePreprocessingClient(base_url=api_url)
    print(f"DEBUG: Client SERVICE_PREFIX={client.SERVICE_PREFIX}")
    print(f"DEBUG: Full URL for /runoff/: {client._get_full_url('/runoff/')}")

    # Health check first - fail fast if API unavailable
    if not client.readiness_check():
        raise SapphireAPIError(f"SAPPHIRE API at {api_url} is not ready")
    print("DEBUG: Readiness check passed")

    # Prepare records for API
    # Group by station code and prepare records
    records = []
    for _, row in data.iterrows():
        date_obj = pd.to_datetime(row['date'])
        record = {
            "horizon_type": "day",
            "code": str(row['code']),
            "date": date_obj.strftime('%Y-%m-%d'),
            "discharge": float(row['discharge']) if pd.notna(row['discharge']) else None,
            "predictor": None,  # Daily data doesn't have predictor
            "horizon_value": date_obj.day,
            "horizon_in_year": date_obj.dayofyear,
        }
        records.append(record)

    # Write to API
    print(f"DEBUG: Total records to write: {len(records)}")
    if records:
        print(f"DEBUG: First record: {records[0]}")
    count = client.write_runoff(records)
    logger.info(f"Successfully wrote {count} runoff records to SAPPHIRE API")
    print(f"SAPPHIRE API: Successfully wrote {count} runoff records")
    return True


def _write_runoff_to_csv(data: pd.DataFrame, output_file_path: str, column_list: list) -> None:
    """
    Write runoff data to CSV file.

    Args:
        data: DataFrame to write
        output_file_path: Path to output CSV file
        column_list: List of columns to include
    """
    ret = data.reset_index(drop=True)[column_list].to_csv(output_file_path, index=False)
    if ret is None:
        print(f"Time series data written to {output_file_path}.")
        logger.info(f"Time series data written to {output_file_path}.")
    else:
        raise IOError(f"Could not write the time series data to {output_file_path}.")


def _write_hydrograph_to_api(data: pd.DataFrame) -> bool:
    """
    Write daily hydrograph data to SAPPHIRE API.

    Args:
        data: DataFrame with hydrograph statistics. Expected columns:
            - code: station code
            - date: date
            - day_of_year: day of year (1-366)
            - count, mean, std, min, max: statistics
            - 5%, 25%, 50%, 75%, 95%: percentiles (will be renamed to q05, q25, etc.)
            - <current_year>: current year's discharge (e.g., "2026")
            - <previous_year>: previous year's discharge (e.g., "2025")

    Returns:
        True if successful, False otherwise

    Raises:
        SapphireAPIError: If API write fails after retries
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.warning("sapphire-api-client not installed, skipping API write")
        return False

    # Get API URL from environment, default to localhost
    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    # Check if API writing is enabled (default: enabled)
    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
    if not api_enabled:
        logger.info("SAPPHIRE API writing disabled via SAPPHIRE_API_ENABLED=false")
        print("SAPPHIRE API writing disabled (SAPPHIRE_API_ENABLED=false)")
        return False

    client = SapphirePreprocessingClient(base_url=api_url)

    # Health check first - fail fast if API unavailable
    if not client.readiness_check():
        raise SapphireAPIError(f"SAPPHIRE API at {api_url} is not ready")

    # Determine current and previous year columns
    current_year = dt.date.today().year
    previous_year = current_year - 1
    current_year_col = str(current_year)
    previous_year_col = str(previous_year)

    # Prepare records for API
    records = []
    for _, row in data.iterrows():
        date_obj = pd.to_datetime(row['date'])
        day_of_year = int(row['day_of_year']) if 'day_of_year' in row else date_obj.dayofyear

        record = {
            "horizon_type": "day",
            "code": str(row['code']),
            "date": date_obj.strftime('%Y-%m-%d'),
            "day_of_year": day_of_year,
            "horizon_value": date_obj.day,  # Day of month for daily data
            "horizon_in_year": day_of_year,
            # Statistics
            "count": int(row['count']) if pd.notna(row.get('count')) else None,
            "mean": float(row['mean']) if pd.notna(row.get('mean')) else None,
            "std": float(row['std']) if pd.notna(row.get('std')) else None,
            "min": float(row['min']) if pd.notna(row.get('min')) else None,
            "max": float(row['max']) if pd.notna(row.get('max')) else None,
            # Percentiles - map from DataFrame column names to API field names
            "q05": float(row['5%']) if pd.notna(row.get('5%')) else None,
            "q25": float(row['25%']) if pd.notna(row.get('25%')) else None,
            "q50": float(row['50%']) if pd.notna(row.get('50%')) else None,
            "q75": float(row['75%']) if pd.notna(row.get('75%')) else None,
            "q95": float(row['95%']) if pd.notna(row.get('95%')) else None,
            # Current and previous year values
            "current": float(row[current_year_col]) if current_year_col in row and pd.notna(row.get(current_year_col)) else None,
            "previous": float(row[previous_year_col]) if previous_year_col in row and pd.notna(row.get(previous_year_col)) else None,
            "norm": None,  # Not currently calculated
        }
        records.append(record)

    # Write to API
    print(f"DEBUG: Hydrograph records to write: {len(records)}")
    if records:
        print(f"DEBUG: First hydrograph record: {records[0]}")
    count = client.write_hydrograph(records)
    logger.info(f"Successfully wrote {count} hydrograph records to SAPPHIRE API")
    print(f"SAPPHIRE API: Successfully wrote {count} hydrograph records")
    return True


def write_daily_time_series_data_to_csv(
    data: pd.DataFrame, 
    column_list=["code", "date", "discharge"],
    api_data: Optional[pd.DataFrame] = None
):
    """
    Writes the data to a csv file and optionally to SAPPHIRE API.

    During the transition period, writes to both API and CSV for redundancy.
    API write is attempted first; if it fails, CSV is written as backup
    and an error is raised.

    Args:
        data (pd.DataFrame): The full data to be written to CSV.
        column_list (list, optional): The list of columns to be written to the csv file.
            Default is ["code", "date", "discharge"].
        api_data (pd.DataFrame, optional): Data to write to API. If None, writes all data 
            to API (legacy behavior). If empty DataFrame, skips API write. If provided with
            data, only writes that subset to the API. This allows sending only new/updated
            records to the API while writing the full dataset to CSV.

    Environment Variables:
        SAPPHIRE_API_URL: API base URL (default: http://localhost:8000)
        SAPPHIRE_API_ENABLED: Set to 'false' to disable API writes (default: true)

    Returns:
        None upon success.

    Raises:
        SapphireAPIError: If API write fails (after CSV backup is written)
        Exception: If CSV write fails
    """
    data = data.copy()

    # Get intermediate data path from environment variable
    intermediate_data_path = os.getenv("ieasyforecast_intermediate_data_path")
    if intermediate_data_path is None:
        raise ValueError("Environment variable ieasyforecast_intermediate_data_path is not set.")

    # Test if the intermediate data path exists. If not, create it.
    if not os.path.exists(intermediate_data_path):
        os.makedirs(intermediate_data_path)
        logger.info(f"Created directory {intermediate_data_path}.")
        print(f"Created directory {intermediate_data_path}.")

    # Get the path to the intermediate data folder from the environmental
    # variables and the name of the ieasyforecast_analysis_daily_file.
    # Concatenate them to the output file path.
    try:
       output_file_path = os.path.join(
            intermediate_data_path,
            os.getenv("ieasyforecast_daily_discharge_file"))
    except Exception as e:
        logger.error("Could not get the output file path.")
        print(intermediate_data_path)
        print(os.getenv("ieasyforecast_daily_discharge_file"))
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

    # === SAPPHIRE API Integration ===
    # Strategy: API first, CSV as backup
    # During transition: write both for redundancy
    api_error = None

    # Determine what data to send to API
    if api_data is None:
        # Legacy behavior: send all data (not recommended for large datasets)
        data_for_api = data
        logger.warning("api_data not provided, sending all data to API (legacy mode)")
        print(f"WARNING: api_data not provided, sending all {len(data)} records to API (legacy mode)")
    elif api_data.empty:
        # Explicitly empty: skip API write
        data_for_api = None
        logger.info("api_data is empty, skipping API write")
        print("API: No new data to sync (api_data is empty)")
    else:
        # Use the provided api_data
        data_for_api = api_data.copy()
        # Apply same formatting as full data
        data_for_api = data_for_api.round(3)
        data_for_api['code'] = data_for_api['code'].astype(str).str.replace(r'\.0$', '', regex=True)
        if 'date' in data_for_api.columns:
            data_for_api['date'] = pd.to_datetime(data_for_api['date'], errors='coerce').dt.strftime('%Y-%m-%d')
        logger.info(f"Sending {len(data_for_api)} new records to API (out of {len(data)} total)")
        print(f"API: Sending {len(data_for_api)} new records (out of {len(data)} total)")

    if data_for_api is not None and not data_for_api.empty:
        try:
            # Attempt API write
            _write_runoff_to_api(data_for_api)
            logger.info("API write successful")
        except SapphireAPIError as e:
            # API failed - log error, will write CSV as backup
            api_error = e
            logger.error(f"API write failed: {e}")
            print(f"WARNING: SAPPHIRE API write failed: {e}")
        except Exception as e:
            # Unexpected error - treat as API failure
            api_error = e
            logger.error(f"Unexpected error during API write: {e}")

    # === CSV Write ===
    # Always write CSV during transition period (for redundancy or as backup)
    print(f"DEBUG: Writing time series data to {output_file_path} with columns {column_list}")
    try:
        _write_runoff_to_csv(data, output_file_path, column_list)
    except Exception as e:
        print(f"Could not write the time series data to {output_file_path}.")
        logger.error(f"Could not write the time series data to {output_file_path}.")
        raise e

    # If API write failed, raise error after CSV backup is written
    if api_error is not None:
        raise SapphireAPIError(f"API write failed (CSV backup written): {api_error}")

def write_daily_hydrograph_data_to_csv(
    data: pd.DataFrame,
    column_list=["code", "date", "discharge"],
    api_data: Optional[pd.DataFrame] = None
):
    """
    Writes the hydrograph data to a CSV file and optionally to SAPPHIRE API.

    During the transition period, writes to both API and CSV for redundancy.
    API write is attempted first; if it fails, CSV is written as backup
    and an error is raised.

    Args:
        data (pd.DataFrame): The full hydrograph data to be written to CSV.
        column_list (list, optional): The list of columns to be written to the csv file.
            Default is ["code", "date", "discharge"].
        api_data (pd.DataFrame, optional): Data to write to API. If None, writes all data
            to API (legacy behavior). If empty DataFrame, skips API write. If provided with
            data, only writes that subset to the API. For daily operations, typically only
            today's row is sent to the API.

    Returns:
        None upon success.

    Raises:
        SapphireAPIError: If API write fails (after CSV backup is written)
        Exception: If the data cannot be written to the csv file.
    """
    data = data.copy()

    # Get the path to the intermediate data folder from the environmental
    # variables and the name of the ieasyforecast_analysis_daily_file.
    # Concatenate them to the output file path.
    try:
       output_file_path = os.path.join(
            os.getenv("ieasyforecast_intermediate_data_path"),
            os.getenv("ieasyforecast_hydrograph_day_file"))
    except Exception as e:
        logger.error("Could not get the output file path.")
        print(os.getenv("ieasyforecast_intermediate_data_path"))
        print(os.getenv("ieasyforecast_hydrograph_day_file"))
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

    # === SAPPHIRE API Integration ===
    api_error = None

    # Determine what data to send to API
    if api_data is None:
        # Legacy behavior: send all data (not recommended for large datasets)
        data_for_api = data
        logger.warning("api_data not provided, sending all hydrograph data to API (legacy mode)")
        print(f"WARNING: api_data not provided, sending all {len(data)} hydrograph records to API (legacy mode)")
    elif api_data.empty:
        # Explicitly empty: skip API write
        data_for_api = None
        logger.info("api_data is empty, skipping hydrograph API write")
        print("API: No new hydrograph data to sync (api_data is empty)")
    else:
        # Use the provided api_data
        data_for_api = api_data.copy()
        # Apply same formatting as full data
        data_for_api = data_for_api.round(3)
        data_for_api['code'] = data_for_api['code'].astype(str).str.replace(r'\.0$', '', regex=True)
        if 'date' in data_for_api.columns:
            data_for_api['date'] = pd.to_datetime(data_for_api['date'], errors='coerce').dt.strftime('%Y-%m-%d')
        # Filter rows where count is 0
        if 'count' in data_for_api.columns:
            data_for_api = data_for_api[data_for_api['count'] != 0]
        logger.info(f"Sending {len(data_for_api)} hydrograph records to API (out of {len(data)} total)")
        print(f"API: Sending {len(data_for_api)} hydrograph records (out of {len(data)} total)")

    if data_for_api is not None and not data_for_api.empty:
        try:
            # Attempt API write
            _write_hydrograph_to_api(data_for_api)
            logger.info("Hydrograph API write successful")
        except SapphireAPIError as e:
            # API failed - log error, will write CSV as backup
            api_error = e
            logger.error(f"Hydrograph API write failed: {e}")
            print(f"WARNING: SAPPHIRE hydrograph API write failed: {e}")
        except Exception as e:
            # Unexpected error - treat as API failure
            api_error = e
            logger.error(f"Unexpected error during hydrograph API write: {e}")

    # === CSV Write ===
    print(f"DEBUG: Trying to write hydrograph data to {output_file_path} with columns {column_list}")
    try:
        ret = data.reset_index(drop=True)[column_list].to_csv(output_file_path, index=False)
        if ret is None:
            print(f"Hydrograph data written to {output_file_path}.")
            logger.info(f"Hydrograph data written to {output_file_path}.")
        else:
            print(f"Could not write the hydrograph data to {output_file_path}.")
            logger.error(f"Could not write the hydrograph data to {output_file_path}.")
    except Exception as e:
        print(f"Could not write the hydrograph data to {output_file_path}.")
        logger.error(f"Could not write the hydrograph data to {output_file_path}.")
        raise e

    # If API write failed, raise error after CSV backup is written
    if api_error is not None:
        raise SapphireAPIError(f"Hydrograph API write failed (CSV backup written): {api_error}")


def verify_runoff_data_consistency(csv_path: Optional[str] = None) -> dict:
    """
    Verify consistency between runoff data in CSV file and SAPPHIRE API database.
    
    Compares the runoff_day.csv file with data in the preprocessing database.
    Only runs when SAPPHIRE_API_ENABLED is true.
    
    Args:
        csv_path: Path to CSV file. If None, uses environment variable.
        
    Returns:
        dict with verification results:
            - 'status': 'match', 'mismatch', 'error', or 'skipped'
            - 'csv_count': number of records in CSV
            - 'api_count': number of records in API for same date range
            - 'mismatches': list of mismatched records (if any)
            - 'message': human-readable summary
    """
    import requests
    
    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
    if not api_enabled:
        return {
            'status': 'skipped',
            'message': 'API disabled, skipping verification'
        }
    
    # Get CSV path
    if csv_path is None:
        intermediate_data_path = os.getenv("ieasyforecast_intermediate_data_path")
        csv_file = os.getenv("ieasyforecast_daily_discharge_file")
        if not intermediate_data_path or not csv_file:
            return {
                'status': 'error',
                'message': 'Environment variables not set for CSV path'
            }
        csv_path = os.path.join(intermediate_data_path, csv_file)
    
    # Read CSV
    try:
        csv_df = pd.read_csv(csv_path)
        csv_df['date'] = pd.to_datetime(csv_df['date']).dt.strftime('%Y-%m-%d')
        csv_df['code'] = csv_df['code'].astype(str)
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Failed to read CSV: {e}'
        }
    
    # Get date range from CSV
    dates = pd.to_datetime(csv_df['date'])
    start_date = dates.min().strftime('%Y-%m-%d')
    end_date = dates.max().strftime('%Y-%m-%d')
    codes = csv_df['code'].unique().tolist()
    
    # Query API for each station (to handle pagination properly)
    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    all_api_records = []
    
    try:
        for code in codes:
            # Query with high limit to get all records for this station
            params = {
                'horizon': 'day',
                'code': code,
                'start_date': start_date,
                'end_date': end_date,
                'limit': 100000  # High limit to get all records
            }
            response = requests.get(
                f"{api_url}/api/preprocessing/runoff/",
                params=params,
                timeout=60
            )
            if response.status_code == 200:
                all_api_records.extend(response.json())
            else:
                logger.warning(f"API query failed for code {code}: {response.status_code}")
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Failed to query API: {e}'
        }
    
    # Convert API response to DataFrame
    if not all_api_records:
        return {
            'status': 'mismatch',
            'csv_count': len(csv_df),
            'api_count': 0,
            'message': f'No data in API for date range {start_date} to {end_date}'
        }
    
    api_df = pd.DataFrame(all_api_records)
    api_df['date'] = pd.to_datetime(api_df['date']).dt.strftime('%Y-%m-%d')
    api_df['code'] = api_df['code'].astype(str)
    
    # Compare record counts
    csv_count = len(csv_df)
    api_count = len(api_df)
    
    # Create comparison keys (code + date)
    csv_df['key'] = csv_df['code'] + '_' + csv_df['date']
    api_df['key'] = api_df['code'] + '_' + api_df['date']
    
    # Find missing/extra records
    csv_keys = set(csv_df['key'])
    api_keys = set(api_df['key'])
    
    missing_in_api = csv_keys - api_keys
    extra_in_api = api_keys - csv_keys
    
    # For records in both, compare discharge values
    common_keys = csv_keys & api_keys
    mismatches = []
    
    for key in list(common_keys)[:100]:  # Limit detailed comparison to first 100
        csv_row = csv_df[csv_df['key'] == key].iloc[0]
        api_row = api_df[api_df['key'] == key].iloc[0]
        
        csv_discharge = csv_row['discharge'] if pd.notna(csv_row['discharge']) else None
        api_discharge = api_row['value'] if pd.notna(api_row['value']) else None
        
        # Compare with tolerance for floating point
        if csv_discharge is not None and api_discharge is not None:
            if abs(csv_discharge - api_discharge) > 0.001:
                mismatches.append({
                    'key': key,
                    'csv_discharge': csv_discharge,
                    'api_discharge': api_discharge
                })
        elif csv_discharge != api_discharge:  # One is None, other is not
            mismatches.append({
                'key': key,
                'csv_discharge': csv_discharge,
                'api_discharge': api_discharge
            })
    
    # Build result
    if not missing_in_api and not extra_in_api and not mismatches:
        status = 'match'
        message = f'Data consistent: {csv_count} CSV records match {api_count} API records'
    else:
        status = 'mismatch'
        parts = []
        if missing_in_api:
            parts.append(f'{len(missing_in_api)} records missing in API')
        if extra_in_api:
            parts.append(f'{len(extra_in_api)} extra records in API')
        if mismatches:
            parts.append(f'{len(mismatches)} value mismatches')
        message = f'Data inconsistent: {", ".join(parts)}'
    
    result = {
        'status': status,
        'csv_count': csv_count,
        'api_count': api_count,
        'missing_in_api': len(missing_in_api),
        'extra_in_api': len(extra_in_api),
        'value_mismatches': len(mismatches),
        'message': message
    }
    
    if mismatches:
        result['sample_mismatches'] = mismatches[:5]  # Include first 5 for debugging
    if missing_in_api:
        result['sample_missing'] = list(missing_in_api)[:5]
    
    return result


def verify_hydrograph_data_consistency(csv_path: Optional[str] = None) -> dict:
    """
    Verify consistency between hydrograph data in CSV file and SAPPHIRE API database.
    
    Compares the hydrograph_day.csv file with data in the preprocessing database.
    Only runs when SAPPHIRE_API_ENABLED is true.
    
    Args:
        csv_path: Path to CSV file. If None, uses environment variable.
        
    Returns:
        dict with verification results:
            - 'status': 'match', 'mismatch', 'error', or 'skipped'
            - 'csv_count': number of records in CSV
            - 'api_count': number of records in API
            - 'message': human-readable summary
    """
    import requests
    
    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
    if not api_enabled:
        return {
            'status': 'skipped',
            'message': 'API disabled, skipping verification'
        }
    
    # Get CSV path
    if csv_path is None:
        intermediate_data_path = os.getenv("ieasyforecast_intermediate_data_path")
        csv_file = os.getenv("ieasyforecast_hydrograph_day_file")
        if not intermediate_data_path or not csv_file:
            return {
                'status': 'error',
                'message': 'Environment variables not set for CSV path'
            }
        csv_path = os.path.join(intermediate_data_path, csv_file)
    
    # Read CSV
    try:
        csv_df = pd.read_csv(csv_path)
        csv_df['code'] = csv_df['code'].astype(str)
        # CSV has day_of_year column
        if 'day_of_year' not in csv_df.columns:
            return {
                'status': 'error',
                'message': 'CSV missing day_of_year column'
            }
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Failed to read CSV: {e}'
        }
    
    codes = csv_df['code'].unique().tolist()
    
    # Query API for each station
    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    all_api_records = []
    
    try:
        for code in codes:
            params = {
                'horizon': 'day',
                'code': code,
                'limit': 100000
            }
            response = requests.get(
                f"{api_url}/api/preprocessing/hydrograph/",
                params=params,
                timeout=60
            )
            if response.status_code == 200:
                all_api_records.extend(response.json())
            else:
                logger.warning(f"Hydrograph API query failed for code {code}: {response.status_code}")
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Failed to query API: {e}'
        }
    
    if not all_api_records:
        return {
            'status': 'mismatch',
            'csv_count': len(csv_df),
            'api_count': 0,
            'message': 'No hydrograph data in API'
        }
    
    api_df = pd.DataFrame(all_api_records)
    api_df['code'] = api_df['code'].astype(str)
    
    # Create comparison keys (code + day_of_year/horizon_in_year)
    csv_df['key'] = csv_df['code'] + '_' + csv_df['day_of_year'].astype(str)
    
    # API uses horizon_in_year for day_of_year
    if 'horizon_in_year' in api_df.columns:
        api_df['key'] = api_df['code'] + '_' + api_df['horizon_in_year'].astype(str)
    else:
        return {
            'status': 'error',
            'message': 'API response missing horizon_in_year column'
        }
    
    csv_count = len(csv_df)
    api_count = len(api_df)
    
    csv_keys = set(csv_df['key'])
    api_keys = set(api_df['key'])
    
    missing_in_api = csv_keys - api_keys
    extra_in_api = api_keys - csv_keys
    
    # Compare values for common keys (sample check)
    common_keys = csv_keys & api_keys
    mismatches = []
    
    # Column mapping: CSV -> API
    column_map = {
        '5%': 'q05', '25%': 'q25', '50%': 'q50', '75%': 'q75', '95%': 'q95',
        'norm': 'norm', 'count': 'count', 'std': 'std'
    }
    
    for key in list(common_keys)[:50]:  # Check first 50
        csv_row = csv_df[csv_df['key'] == key].iloc[0]
        api_row = api_df[api_df['key'] == key].iloc[0]
        
        for csv_col, api_col in column_map.items():
            if csv_col in csv_row and api_col in api_row:
                csv_val = csv_row[csv_col] if pd.notna(csv_row[csv_col]) else None
                api_val = api_row[api_col] if pd.notna(api_row[api_col]) else None
                
                if csv_val is not None and api_val is not None:
                    if abs(csv_val - api_val) > 0.001:
                        mismatches.append({
                            'key': key,
                            'column': csv_col,
                            'csv_value': csv_val,
                            'api_value': api_val
                        })
                        break  # One mismatch per key is enough
    
    if not missing_in_api and not extra_in_api and not mismatches:
        status = 'match'
        message = f'Hydrograph data consistent: {csv_count} CSV records, {api_count} API records'
    else:
        status = 'mismatch'
        parts = []
        if missing_in_api:
            parts.append(f'{len(missing_in_api)} records missing in API')
        if extra_in_api:
            parts.append(f'{len(extra_in_api)} extra records in API')
        if mismatches:
            parts.append(f'{len(mismatches)} value mismatches')
        message = f'Hydrograph data inconsistent: {", ".join(parts)}'
    
    result = {
        'status': status,
        'csv_count': csv_count,
        'api_count': api_count,
        'missing_in_api': len(missing_in_api),
        'extra_in_api': len(extra_in_api),
        'value_mismatches': len(mismatches),
        'message': message
    }
    
    if mismatches:
        result['sample_mismatches'] = mismatches[:5]
    if missing_in_api:
        result['sample_missing'] = list(missing_in_api)[:5]
    
    return result



