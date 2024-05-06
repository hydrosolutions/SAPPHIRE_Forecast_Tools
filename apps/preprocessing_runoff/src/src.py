import os
import pandas as pd
import datetime as dt

from ieasyhydro_sdk.filters import BasicDataValueFilters

import logging
logger = logging.getLogger(__name__)


def read_runoff_data_from_multiple_rivers_xlsx(filename):
    """
    Read daily average river runoff data from an excel sheet.

    Parameters
    ----------
    filename : str
        Path to the excel sheet containing the river runoff data.

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
        df_sheet = pd.read_excel(xls, sheet_name, header=1, usecols=[0, 1], names=['date', 'discharge'])
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

        df_sheet['name'] = river_name
        df_sheet['code'] = code
        df = pd.concat([df, df_sheet], axis=0)

    # convert date column to datetime format
    df['date'] = pd.to_datetime(df['date'], format='%d.%m.%Y').dt.date

    # convert discharge column to numeric format
    df['discharge'] = pd.to_numeric(df['discharge'], errors='coerce')

    # replace data in rows with missing values with NaN
    df['discharge'] = df['discharge'].replace('-', float('nan'))

    return df

def read_all_runoff_data_from_excel():
    """
    Reads daily river runoff data from all excel sheets in the daily_discharge
    directory.

    Args:
        None

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
        df = pd.concat([df, read_runoff_data_from_multiple_rivers_xlsx(file_path)], axis=0)

    return df

def get_daily_average_discharge_from_iEH_per_site(
        ieh_sdk, site, name, start_date, end_date=dt.date.today()):
    """
    Reads daily average discharge data from the iEasyHydro database for a given site.

    Args:
        ieh_sdk (object): An object that provides a method to get data values for a site from a database.
        site (str or int): The site code.
        name (str): The name of the site.
        start_date (datetime.date or str): The start date of the data to read.
        end_date (datetime.date or str, optional): The end date of the data to read. Defaults to dt.date.today().

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
        db_df = db_df.rename(columns={'local_date_time': 'date', 'data_value': 'discharge'})

        # Drop the columns we don't need
        db_df.drop(columns=['utc_date_time'], inplace=True)

        # Convert the Date column to datetime
        db_df['date'] = pd.to_datetime(db_df['date'], format='%Y-%m-%d %H:%M:%S').dt.date

        # Add the name and code columns
        db_df['name'] = name
        db_df['code'] = site

    except Exception as e:
        logger.info(f"Skip reading daily average discharge data for site {site}")
        # Return an empty dataframe with columns 'date', 'discharge', 'name', 'code'
        db_df = pd.DataFrame(columns=['date', 'discharge', 'name', 'code'])

    return db_df

def get_todays_morning_discharge_from_iEH_per_site(
        ieh_sdk, site, name):
    """
    Reads river discharge data from the iEasyHydro database for a given site that
    was measured today.

    Args:
        ieh_sdk (object): An object that provides a method to get data values for a site from a database.
        site (str or int): The site code.
        name (str): The name of the site.

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
        db_df = db_df.rename(columns={'local_date_time': 'date', 'data_value': 'discharge'})

        # Drop the columns we don't need
        db_df.drop(columns=['utc_date_time'], inplace=True)

        # Convert the Date column to datetime
        db_df['date'] = pd.to_datetime(db_df['date'], format='%Y-%m-%d %H:%M:%S').dt.date

        # Add the name and code columns
        db_df['name'] = name
        db_df['code'] = site

    except Exception as e:
        logger.info(f"Skip reading morning measurement of discharge data for site {site}")
        # Return an empty dataframe with columns 'date', 'discharge', 'name', 'code'
        db_df = pd.DataFrame(columns=['date', 'discharge', 'name', 'code'])

    return db_df

def get_runoff_data(ieh_sdk=None):
    """
    Reads runoff data from excel and, if possible, from iEasyHydro database.

    Args:
        ieh_sdk (object): An object that provides a method to get data values
            for a site from a database. None in case of no access to the database.
    """
    # Read data from excel files
    read_data = read_all_runoff_data_from_excel()

    if ieh_sdk is None:
        # We do not have access to an iEasyHydro database
        return read_data

    else:
        # Get the last row for each code in runoff_data
        last_row = read_data.groupby('code').tail(1)

        # For each code in last_row, get the daily average discharge data from the
        # iEasyHydro database using the function get_daily_average_discharge_from_iEH_per_site
        for index, row in last_row.iterrows():
            db_average_data = get_daily_average_discharge_from_iEH_per_site(
                ieh_sdk, row['code'], row['name'], row['date']
            )
            db_morning_data = get_todays_morning_discharge_from_iEH_per_site(
                ieh_sdk, row['code'], row['name'])
            # Append db_data to read_data if db_data is not empty
            if not db_average_data.empty:
                read_data = pd.concat([read_data, db_average_data], ignore_index=True)
            if not db_morning_data.empty:
                read_data = pd.concat([read_data, db_morning_data], ignore_index=True)

        return read_data

def write_data_to_csv(data: pd.DataFrame):
    """
    Writes the data to a csv file for later reading by other forecast tools.

    Reads data from excel sheets and from the database (if access available).

    Args:
    data (pd.DataFrame): The data to be written to a csv file.

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

    # Write the data to a csv file. Raise an error if this does not work.
    # If the data is written to the csv file, log a message that the data
    # has been written.
    try:
        ret = data.reset_index(drop=True)[["code", "date", "discharge"]].to_csv(output_file_path, index=False)
        if ret is None:
            logger.info(f"Data written to {output_file_path}.")
            return ret
        else:
            logger.error(f"Could not write the data to {output_file_path}.")
    except Exception as e:
        logger.error(f"Could not write the data to {output_file_path}.")
        raise e

