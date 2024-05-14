import os
import pandas as pd
import numpy as np
import datetime as dt

from ieasyhydro_sdk.filters import BasicDataValueFilters

import logging
logger = logging.getLogger(__name__)


def filter_roughly_for_outliers(combined_data, group_by='Code', filter_col='Q_m3s', window_size=15):
    """
    Filters outliers in the cilter_col column of the input DataFrame.

    This function groups the input DataFrame by the group_by column and applies
    a rolling window outlier detection method to the filter_col column of each
    group. Outliers are defined as values that are more than 3 standard
    deviations away from the rolling mean. These outliers are replaced with NaN.

    The function further drops all rows with Nan in the group_by column.

    Parameters:
    combined_data (pd.DataFrame): The input DataFrame. Must contain 'Code' and 'Q_m3s' columns.
    group_by (str, optional): The column to group the data by. Default is 'Code'.
    filter_col (str, optional): The column to filter for outliers. Default is 'Q_m3s'.
    window_size (int, optional): The size of the rolling window used for outlier detection. Default is 15.

    Returns:
    pd.DataFrame: The input DataFrame with outliers in the 'Q_m3s' column replaced with NaN.

    Raises:
    ValueError: If the group_by column is not found in the input DataFrame.
    """
    # Preliminary filter for outliers
    def filter_group(group, filter_col, window_size):
        # Calculate the rolling mean and standard deviation
        # Will put NaNs in the first window_size-1 values and therefore not
        # filter outliers in the first window_size-1 values
        rolling_mean = group[filter_col].rolling(window_size).mean()
        rolling_std = group[filter_col].rolling(window_size).std()

        # Calculate the upper and lower bounds for outliers
        num_std = 3.5  # Number of standard deviations
        upper_bound = rolling_mean + num_std * rolling_std
        lower_bound = rolling_mean - num_std * rolling_std

        #print("DEBUG: upper_bound: ", upper_bound)
        #print("DEBUG: lower_bound: ", lower_bound)
        #print("DEBUG: "filter_col": ", group[filter_col])

        # Set Q_m3s which exceeds lower and upper bounds to nan
        group.loc[group[filter_col] > upper_bound, filter_col] = np.nan
        group.loc[group[filter_col] < lower_bound, filter_col] = np.nan

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

    # Apply the function to each group
    combined_data = combined_data.groupby(group_by).apply(
        filter_group, filter_col, window_size, include_groups=True)

    # Ungroup the DataFrame
    combined_data = combined_data.reset_index(drop=True)

    return combined_data

def read_runoff_data_from_multiple_rivers_xlsx(filename, date_col='date', discharge_col='discharge', name_col='name', code_col='code'):
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
    # Test if excel file is available
    try:
        xls = pd.ExcelFile(filename)
    except FileNotFoundError:
        raise FileNotFoundError(f"File '{filename}' not found.")

    # Extract all sheet names
    xls.sheet_names

    # load data from all sheets into a single dataframe
    df = pd.DataFrame()
    # the river name is in cell A1 of each sheet
    # the data starts in row 3 of each sheet where column A contains the time stamp and column B contains the river runoff data.
    # some of the daily time series data are missing and the corresponding cells contain '-'. There might be a type mismatch.
    # We want to have all data in a single dataframe df with the following columns: date, river runoff, river.
    for sheet_name in xls.sheet_names:
        df_sheet = pd.read_excel(xls, sheet_name, header=1, usecols=[0, 1], names=[date_col, discharge_col])
        #print(f"Reading sheet '{sheet_name}' \n '{df_sheet.head()}'")
        # read cell A1 and extract the river name
        # Read the river name from cell A1
        river_name_df = pd.read_excel(xls, sheet_name, nrows=1, usecols="A",header=None)
        full_river_name = river_name_df.iloc[0, 0]
        # Check if the first 5 characters are digits
        try:
            int(full_river_name[:5])
            is_numeric = True
        except (IndexError, ValueError):
            is_numeric = False

        if is_numeric:
            code = int(full_river_name[:5])
            river_name = full_river_name[5:].lstrip()
        else:
            code = 'NA'
            river_name = full_river_name

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

def read_runoff_data_from_single_river_xlsx(filename, date_col='date', discharge_col='discharge', name_col='name', code_col='code'):
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

    # Extract all sheet names
    xls.sheet_names

    # load data from all sheets into a single dataframe
    df = pd.DataFrame()
    # the river name is in cell A1 of each sheet
    # the data starts in row 3 of each sheet where column A contains the time stamp and column B contains the river runoff data.
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

    return df

def read_all_runoff_data_from_excel(date_col='date', discharge_col='discharge', name_col='name', code_col='code'):
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
    files_multiple_rivers = [
        f for f in os.listdir(daily_discharge_dir)
        if os.path.isfile(os.path.join(daily_discharge_dir, f))
        and f.endswith('.xlsx') and not f[0].isdigit()
    ]
    # Initiate empty dataframe
    df = None

    if len(files_multiple_rivers) == 0:
        logger.warning(f"No excel files with multiple rivers data found in '{daily_discharge_dir}'.")
    else:
        # Read the data from all files
        for file in files_multiple_rivers:
            file_path = os.path.join(daily_discharge_dir, file)
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
    df_single = None

    files_single_rivers = [
        f for f in os.listdir(daily_discharge_dir)
        if os.path.isfile(os.path.join(daily_discharge_dir, f))
        and f.endswith('.xlsx') and f[0].isdigit()
    ]
    if len(files_single_rivers) == 0:
        logger.warning(f"No excel files with single river data found in '{daily_discharge_dir}'.")
    else:
        # Read the data from all files
        for file in files_single_rivers:
            file_path = os.path.join(daily_discharge_dir, file)
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

def fill_gaps_in_reservoir_inflow_data(
        combined_data: pd.DataFrame,
        date_col: str,
        discharge_col: str,
        code_col: str):
    """
    Calculate discharge for virtual stations.

    This function calculates data for the virtual station '16936' (Inflow to the
    Toktogul reservoir) which is the sum of the data from the stations '16059',
    '16096', '16100', and '16093'.
    It further calculates the data for virtual station '15954' (Inflow to the
    Kirov reservoir) which is equal to the data from station '15102'.
    It also calculates the data for virtual station '15960' (Inflow to the
    Orto-Tokoy reservoir) which is equal to the the sum of station '15261' and
    4*station '15292'.

    Args:
    combined_data (pd.DataFrame): The input DataFrame. Must contain date_col, discharge_col, and code_col.
    date_col (str): The name of the column containing the date data.
    discharge_col (str): The name of the column containing the discharge data.
    code_col (str): The name of the column containing the station code.

    Returns:
    pd.DataFrame: The input DataFrame with the calculated data for the virtual stations.

    Raises:
    ValueError: If the required columns are not in the input DataFrame.
    """
    # Check if the required columns are in the DataFrame
    if date_col not in combined_data.columns:
        raise ValueError(f"Column '{date_col}' not found in the DataFrame.")
    if discharge_col not in combined_data.columns:
        raise ValueError(f"Column '{discharge_col}' not found in the DataFrame.")
    if code_col not in combined_data.columns:
        raise ValueError(f"Column '{code_col}' not found in the DataFrame.")

    # Calculate inflow for the Toktogul reservoir
    if '16936' in combined_data[code_col].values:
        logger.debug("preprocessing_runoff.src.fill_gaps_in_reservoir_inflow_data: Station 16936 is in the list of stations.")
        # Go through the the combined_data for '16936' and check for data gaps.
        # From the last available date in the combined_data for '16936', we
        # look for data in the combined_data for other stations, namely '16059',
        # '16096', '16100', and '16093' and add their values up to write to
        # station '16936'.

        # Get latest date for which we have data in '16936'
        last_date_16936 = combined_data[combined_data[code_col] == '16936'][date_col].max()

        # Get the data for the date from the other stations
        # Test if station 16093 (Torkent) is in the list of stations
        # (This station is under construction at the time of writing this code)
        if '16059' in combined_data[code_col].values:
            data_16059 = combined_data[(combined_data[code_col] == '16059') & (combined_data[date_col] >= last_date_16936)].copy()
        else:
            logger.error('Station 16059 is not in the list of stations for forecasting but it is required for forecasting 16936.\n -> Not able to calculate runoff for 16936.\n -> Please select station 16059 for forecasting.')
            exit()
        if '16096' in combined_data[code_col].values:
            data_16096 = combined_data[(combined_data[code_col] == '16096') & (combined_data[date_col] >= last_date_16936)].copy()
        else:
            logger.error('Station 16096 is not in the list of stations for forecasting but it is required for forecasting 16936.\n -> Not able to calculate runoff for 16936.\n -> Please select station 16096 for forecasting.')
            exit()
        if '16100' in combined_data[code_col].values:
            data_16100 = combined_data[(combined_data[code_col] == '16100') & (combined_data[date_col] >= last_date_16936)].copy()
        else:
            logger.error('Station 16100 is not in the list of stations for forecasting but it is required for forecasting 16936.\n -> Not able to calculate runoff for 16936.\n -> Please select station 16100 for forecasting.')
            exit()
        if '16093' in combined_data[code_col].values:
            data_16093 = combined_data[(combined_data[code_col] == '16093') & (combined_data[date_col] >= last_date_16936)].copy()
        else:
            # 16093 is currently not gauged. We calculate the discharge as 60% of 16096
            data_16093 = combined_data[(combined_data[code_col] == '16096') & (combined_data[date_col] >= last_date_16936)].copy()
            data_16093.loc[:, discharge_col] = 0.6 * data_16096.loc[:, discharge_col]

    else:
        logger.debug("preprocessing_runoff.src.fill_gaps_in_reservoir_inflow_data: Station 16936 is NOT in the list of stations.")
        # Get the data for the date from the other stations
        if '16059' in combined_data[code_col].values:
            data_16059 = combined_data[combined_data[code_col] == '16059'].copy()
        else:
            logger.error('Station 16059 is not in the list of stations for forecasting but it is required for forecasting 16936.\n -> Not able to calculate runoff for 16936.\n -> Please select station 16059 for forecasting.')
            exit()
        if '16096' in combined_data[code_col].values:
            data_16096 = combined_data[combined_data[code_col] == '16096'].copy()
        else:
            logger.error('Station 16096 is not in the list of stations for forecasting but it is required for forecasting 16936.\n -> Not able to calculate runoff for 16936.\n -> Please select station 16096 for forecasting.')
            exit()
        if '16100' in combined_data[code_col].values:
            data_16100 = combined_data[combined_data[code_col] == '16100'].copy()
        else:
            logger.error('Station 16100 is not in the list of stations for forecasting but it is required for forecasting 16936.\n -> Not able to calculate runoff for 16936.\n -> Please select station 16100 for forecasting.')
            exit()
        if '16093' in combined_data[code_col].values:
            data_16093 = combined_data[combined_data[code_col] == '16093'].copy()
        else:
            # 16093 is currently not gauged. We calculate the discharge as 60% of 16096
            data_16093 = combined_data[combined_data[code_col] == '16096'].copy()
            data_16093.loc[:, discharge_col] = 0.6 * data_16096.loc[:, discharge_col]

    # Merge data_15059, data_16096, data_16100, and data_16093 by date
    data_Tok_combined = pd.merge(data_16059, data_16096, on=date_col, how='left', suffixes=('_16059', '_16096'))
    data_16100.rename(columns={discharge_col: 'discharge_16100'}, inplace=True)
    data_Tok_combined = pd.merge(data_Tok_combined, data_16100, on=date_col, how='left')
    data_16093.rename(columns={discharge_col: 'discharge_16093'}, inplace=True)
    data_Tok_combined = pd.merge(data_Tok_combined, data_16093, on=date_col, how='left')

    # Sum up all the data and write to '16936'
    data_Tok_combined['predictor'] = data_Tok_combined['discharge_16059'] + \
        data_Tok_combined['discharge_16096'] + \
            data_Tok_combined['discharge_16100'] + \
                data_Tok_combined['discharge_16093']

    # Append the data_Tok_combined['predictor'] to combined_data for '16936'
    data_Tok_combined[code_col] = '16936'
    data_Tok_combined[discharge_col] = data_Tok_combined['predictor'].round(3)
    # Only keep the columns date_col, code_col, and discharge_col
    data_Tok_combined = data_Tok_combined[[date_col, code_col, discharge_col]]

    combined_data = pd.concat([combined_data, data_Tok_combined], ignore_index=True)

    # The same for the Kirov reservoir
    if '15960' in combined_data[code_col].values:
        logger.debug("preprocessing_runoff.src.fill_gaps_in_reservoir_inflow_data: Station 15960 is in the list of stations.")
        # Go through the the combined_data for '15960' and check for data gaps.

        # Get latest date for which we have data in '15960'
        last_date_15960 = combined_data[combined_data[code_col] == '15960'][date_col].max()

        # Get the data for the date from the other stations
        if '15261' in combined_data[code_col].values:
            data_15261 = combined_data[(combined_data[code_col] == '15261') & (combined_data[date_col] >= last_date_15960)].copy()
        else:
            logger.error('Station 15261 is not in the list of stations for forecasting but it is required for forecasting 15960.\n -> Not able to calculate runoff for 15960.\n -> Please select station 15261 for forecasting.')
            exit()
        if '15292' in combined_data[code_col].values:
            data_15292 = combined_data[(combined_data[code_col] == '15292') & (combined_data[date_col] >= last_date_15960)].copy()
        else:
            logger.error('Station 15292 is not in the list of stations for forecasting but it is required for forecasting 15960.\n -> Not able to calculate runoff for 15960.\n -> Please select station 15292 for forecasting.')
            exit()

    else:
        logger.debug("preprocessing_runoff.src.fill_gaps_in_reservoir_inflow_data: Station 15960 is NOT in the list of stations.")
        # Get the data for the date from the other stations
        if '15261' in combined_data[code_col].values:
            data_15261 = combined_data[combined_data[code_col] == '15261'].copy()
        else:
            logger.error('Station 15261 is not in the list of stations for forecasting but it is required for forecasting 15960.\n -> Not able to calculate runoff for 15960.\n -> Please select station 15261 for forecasting.')
            exit()
        if '15292' in combined_data[code_col].values:
            data_15292 = combined_data[combined_data[code_col] == '15292'].copy()
        else:
            logger.error('Station 15292 is not in the list of stations for forecasting but it is required for forecasting 15960.\n -> Not able to calculate runoff for 15960.\n -> Please select station 15292 for forecasting.')
            exit()

    # Merge by date
    data_Kir_combined = pd.merge(data_15261, data_15292, on=date_col, how='left', suffixes=('_15261', '_15292'))

    # Sum up all the data and write to '15960'
    data_Kir_combined['predictor'] = data_Kir_combined['discharge_15261'] + \
        data_Kir_combined['discharge_15292']*4

    # Append the data_Kir_combined['predictor'] to combined_data for '15960'
    data_Kir_combined[code_col] = '15960'
    data_Kir_combined[discharge_col] = data_Kir_combined['predictor'].round()
    # Only keep the columns date_col, code_col, and discharge_col
    data_Kir_combined = data_Kir_combined[[date_col, code_col, discharge_col]]

    combined_data = pd.concat([combined_data, data_Kir_combined], ignore_index=True)

    # The same for the Orto-Tokoy reservoir
    if '15954' in combined_data[code_col].values:
        logger.debug("preprocessing_runoff.src.fill_gaps_in_reservoir_inflow_data: Station 15954 is in the list of stations.")
        # Go through the the combined_data for '15954' and check for data gaps.

        # Get latest date for which we have data in '15954'
        last_date_15954 = combined_data[combined_data[code_col] == '15954'][date_col].max()

        # Get the data for the date from the other stations
        if '15102' in combined_data[code_col].values:
            data_15102 = combined_data[(combined_data[code_col] == '15102') & (combined_data[date_col] >= last_date_15954)].copy()
        else:
            logger.error('Station 15102 is not in the list of stations for forecasting but it is required for forecasting 15954.\n -> Not able to calculate runoff for 15954.\n -> Please select station 15102 for forecasting.')
            exit()

    else:
        logger.debug("preprocessing_runoff.src.fill_gaps_in_reservoir_inflow_data: Station 15954 is NOT in the list of stations.")
        if '15102' in combined_data[code_col].values:
            # Print the minimum and the maximum dates for station 15102
            data_15102 = combined_data[combined_data[code_col] == '15102'].copy()
        else:
            logger.error('Station 15102 is not in the list of stations for forecasting but it is required for forecasting 15954.\n -> Not able to calculate runoff for 15954.\n -> Please select station 15102 for forecasting.')
            exit()
        # Overwrite the code for the station
        data_15102[code_col] = '15954'

    combined_data = pd.concat([combined_data, data_15102], ignore_index=True)

    # Sort the data by date and code
    combined_data = combined_data.sort_values(by=[code_col, date_col])

    return combined_data

def get_runoff_data(ieh_sdk=None, date_col='date', discharge_col='discharge', name_col='name', code_col='code'):
    """
    Reads runoff data from excel and, if possible, from iEasyHydro database.

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
    read_data = read_all_runoff_data_from_excel(date_col=date_col, discharge_col=discharge_col, name_col=name_col, code_col=code_col)

    if ieh_sdk is None:
        # We do not have access to an iEasyHydro database
        return read_data

    else:
        # Get the last row for each code in runoff_data
        last_row = read_data.groupby(code_col).tail(1)

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

        # For sanity sake, we round the data to a mac of 3 decimal places
        read_data[discharge_col] = read_data[discharge_col].round(3)

        # Cast the 'code' column to string
        read_data[code_col] = read_data[code_col].astype(str)

        return read_data

def write_data_to_csv(data: pd.DataFrame, column_list=["code", "date", "discharge"]):
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
            os.getenv("ieasyforecast_daily_discharge_file"))
    except Exception as e:
        logger.error("Could not get the output file path.")
        print(os.getenv("ieasyforecast_intermediate_data_path"))
        print(os.getenv("ieasyforecast_daily_discharge_file"))
        raise e

    # Test if the columns in column list are available in the dataframe
    for col in column_list:
        if col not in data.columns:
            raise ValueError(f"Column '{col}' not found in the DataFrame.")

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

