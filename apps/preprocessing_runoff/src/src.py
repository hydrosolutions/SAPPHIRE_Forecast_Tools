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
    combined_data = combined_data.groupby(group_by).apply(filter_group, filter_col, window_size)

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
        FileNotFoundError: If the daily_discharge directory is not found or if
            there are no files with file ending .xlsx in the directory.
    """
    # Get the path to the daily_discharge directory
    daily_discharge_dir = os.getenv('ieasyforecast_daily_discharge_path')

    # Test if the directory is available
    if not os.path.exists(daily_discharge_dir):
        raise FileNotFoundError(f"Directory '{daily_discharge_dir}' not found.")

    # Get the list of files in the directory
    files = [
        f for f in os.listdir(daily_discharge_dir)
        if os.path.isfile(os.path.join(daily_discharge_dir, f))
        and f.endswith('.xlsx')
    ]
    if len(files) == 0:
        raise FileNotFoundError(f"No excel files found in '{daily_discharge_dir}'.")

    # Read the data from all files
    df = pd.DataFrame()
    for file in files:
        file_path = os.path.join(daily_discharge_dir, file)
        df = pd.concat([df,
                        read_runoff_data_from_multiple_rivers_xlsx(
                            filename=file_path,
                            date_col=date_col,
                            discharge_col=discharge_col,
                            name_col=name_col,
                            code_col=code_col)],
                        axis=0)

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

