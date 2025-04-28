import os
import pandas as pd
import numpy as np
import datetime as dt
from datetime import timedelta
import time
import pytz
import json
import concurrent.futures
from typing import List, Tuple

# To avoid printing of warning
pd.set_option('future.no_silent_downcasting', True)

from ieasyhydro_sdk.filters import BasicDataValueFilters

import logging
logger = logging.getLogger(__name__)


def get_local_time_range_for_daily_average_runoff_request(target_timezone, window_size=50):
    """
    Calculates the local start and end times for the data request in the target time zone.

    Parameters:
    target_timezone (pytz.timezone): The target time zone for the data request.
    window_size (int): The number of days to look back from the current date.
    Default is 50 days.

    Returns:
    tuple: A tuple containing the start and end times in the target time zone.
    """
    if not target_timezone:
        return None, None

    # Get the current time in the target time zone
    now_local = dt.datetime.now(target_timezone)

    # Calculate the end time (today at 12:00 local time)
    end_time_local = now_local.replace(hour=12, minute=0, second=0, microsecond=0)

    # Calculate the start time (50 days prior at 00:01 local time)
    start_time_local = (now_local - timedelta(days=window_size)).replace(hour=0, minute=1, second=0, microsecond=0)

    return start_time_local, end_time_local

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
        # Calculate Q1, Q3, and IQR
        Q1 = group[filter_col].quantile(0.25)
        Q3 = group[filter_col].quantile(0.75)
        IQR = Q3 - Q1

        # Calculate the upper and lower bounds for outliers
        upper_bound = Q3 + 6.5 * IQR
        lower_bound = Q1 - 1.5 * IQR

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

        # Filter out suspicious data characterized by changes of more than 200% from one time step to the next
        group['prev_value'] = group[filter_col].shift(1)
        group['change'] = (group[filter_col] - group['prev_value']).abs() / group['prev_value'].abs()
        group.loc[group['change'] > 2, filter_col] = np.nan
        group.drop(columns=['prev_value', 'change'], inplace=True)

        # Interpolate gaps of length of max 2 days linearly
        group[filter_col] = group[filter_col].interpolate(method='time', limit=2)

        # Reset the index
        group.reset_index(inplace=True)

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
    
    # Convert the date column to datetime format
    df[date_col] = pd.to_datetime(df[date_col], format='%Y-%m-%d').dt.date

    # convert discharge column to numeric format
    df[discharge_col] = pd.to_numeric(df[discharge_col], errors='coerce')

    # make sure code_col is integer
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

    # convert date column to datetime format
    df[date_col] = pd.to_datetime(df[date_col], format='%d.%m.%Y').dt.date

    # convert discharge column to numeric format
    df[discharge_col] = pd.to_numeric(df[discharge_col], errors='coerce')

    # replace data in rows with missing values with NaN
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

    # convert date column to datetime format
    df[date_col] = pd.to_datetime(df[date_col], format='%d.%m.%Y').dt.date

    # convert discharge column to numeric format
    df[discharge_col] = pd.to_numeric(df[discharge_col], errors='coerce')

    # replace data in rows with missing values with NaN
    df[discharge_col] = df[discharge_col].replace('-', float('nan'))

    # make sure code_col is integer
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
                filename=file_path,
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
                
                print(f"Processed page {page}: {len(page_df)} records")
                
        else:
            print(f"Warning: Expected 'results' key not found in response")
            print(f"Response keys: {list(response.keys()) if isinstance(response, dict) else 'Not a dict'}")
        
        # Check if there are more pages
        if isinstance(response, dict) and response.get('next'):
            # Increment page number
            page += 1
            print(f"Fetching page {page}...")
        else:
            # No more pages
            break
    
    # Combine all DataFrames
    if all_data_frames:
        # Drop columns that are not in the expected structure
        all_data_frames.drop(
            columns=['station_uuid', 'station_type', 'station_id', 
                     'variable_code', 'unit', 'value_type', 'value_code', 
                     'station_name'], 
            inplace=True, errors='ignore')

        return pd.concat(all_data_frames, ignore_index=True)
    else:
        # Return empty DataFrame with the expected structure
        return pd.DataFrame(columns=[
            'station_code', 'timestamp_local', 
            'timestamp_utc', 'value'
        ])

def get_daily_average_discharge_from_iEH_HF_for_multiple_sites(
        ieh_hf_sdk, id_list, start_datetime=None, end_datetime=dt.date.today(),
        target_timezone=None,  
        date_col='date', discharge_col='discharge', code_col='code'):
    """
    Reads daily average discharge data from the iEasyHydro database for a given site.

    The names of the dataframe columns can be customized.

    Args:
        ieh_hf_sdk (object): An object that provides a method to get data values for a site from a database.
        id_list (list): A list of strings denoting site ids.
        start_datetime (datetime, optional): The start datetime of the data to read. Defaults to None. 
        end_datetime (datetime, optional): The end datetime of the data to read. Defaults to dt.datetime.today().
        target_timezone (str, optional): The timezone to convert the date to. Defaults to None.
        If None, local timezone is used.
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
        
    # If start_date is None, get it from get_local_time_range_for_daily_average_runoff_request
    if start_datetime is None:
        start_datetime, end_datetime = get_local_time_range_for_daily_average_runoff_request(target_timezone)

    # Raise an error if the date format is not a datetime with timezone info
    if not isinstance(start_datetime, dt.datetime):
        raise ValueError("The start_datetime must be a datetime object with timezone info.")
    if not isinstance(end_datetime, dt.datetime):
        raise ValueError("The end_datetime must be a datetime object with timezone info.")

    logger.debug(f"Reading daily average discharge data for all sites in sites_list from {start_datetime} to {end_datetime}.")

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
        # Drop the local datetime column as we will be working with utc datetime
        db_df.drop(columns=['local_datetime'], inplace=True)
        # Rename the columns of df to match the columns of combined_data
        db_df = db_df.rename(
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
    # Get the earliest date for which we have data in the combined_data
    earliest_date = combined_data['date'].min()

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
            db_average_data = get_daily_average_discharge_from_iEH_per_site(
                ieh_sdk, row[code_col], row[name_col], row[date_col],
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
    
def get_runoff_data_for_sites_HF(ieh_hf_sdk=None, date_col='date', name_col='name',
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
        exit()

        # For each code in last_row, get the daily average discharge data from the
        # iEasyHydro database using the function get_daily_average_discharge_from_iEH_per_site
        for index, row in last_row.iterrows():
            #db_average_data = get_daily_average_discharge_from_iEH_per_site(
            #    ieh_sdk=ieh_hf_sdk, site=row[code_col], name=row[name_col], 
            #    start_date=row[date_col],
            #    date_col=date_col, discharge_col=discharge_col, name_col=name_col, code_col=code_col
            #)
            db_morning_data = get_todays_morning_discharge_from_iEH_per_site(
                ieh_hf_sdk, row[code_col], row[name_col],
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
    print(f"DEBUG: day_mapping:\n{day_mapping}")
    
    # Merge the hydrograph data with the day mapping
    hydrograph_data = hydrograph_data.reset_index()
    hydrograph_data = hydrograph_data.merge(day_mapping, on='normalized_day', how='left')

    print(f"DEBUG: hydrograph_data after merge:\n{hydrograph_data.head(5)}")
    print(hydrograph_data.head(70).tail(20))
    print(hydrograph_data.tail(5))
    
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
    print(f'DEBUG: write_daily_time_series_data_to_csv: data.head(10)\n{data.head(10)}')

    # Round all values to 3 decimal places
    data = data.round(3)

    # Cast the 'code' column to int and then string
    data = data.astype({'code': 'Float64'}).astype({'code': 'Int64'})

    # Write the data to a csv file. Raise an error if this does not work.
    # If the data is written to the csv file, log a message that the data
    # has been written.
    try:
        ret = data.reset_index(drop=True)[column_list].to_csv(output_file_path, index=False)
        if ret is None:
            logger.info(f"Data written to {output_file_path}.")
            return ret
        else:
            logger.error(f"Could not write the data to {output_file_path}.")
    except Exception as e:
        logger.error(f"Could not write the data to {output_file_path}.")
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

    # Cast the 'code' column to int and then string
    data = data.astype({'code': 'Float64'}).astype({'code': 'Int64'})

    # Test if we have rows where count is 0. If so, drop these rows.
    data = data[data['count'] != 0]

    # Write the data to a csv file. Raise an error if this does not work.
    # If the data is written to the csv file, log a message that the data
    # has been written.
    try:
        ret = data.reset_index(drop=True)[column_list].to_csv(output_file_path, index=False)
        if ret is None:
            logger.info(f"Data written to {output_file_path}.")
            return ret
        else:
            logger.error(f"Could not write the data to {output_file_path}.")
    except Exception as e:
        logger.error(f"Could not write the data to {output_file_path}.")
        raise e



