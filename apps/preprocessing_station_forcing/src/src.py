import os
import pandas as pd
import numpy as np
import datetime as dt

from ieasyhydro_sdk.filters import BasicDataValueFilters

import logging
logger = logging.getLogger(__name__)


# --- Helper tools ---
# region helpers

def filter_roughly_for_outliers(combined_data, group_by=['code'],
                                filter_col='value', date_col='date'):
    """
    Filters outliers in the cilter_col column of the input DataFrame.

    This function groups the input DataFrame by the group_by column and filters
    values that are beyond 1.5 interquartile ranges of the quartiles. These
    outliers are replaced with NaN.

    The function further drops all rows with Nan in the group_by column.

    Parameters:
    combined_data (pd.DataFrame): The input DataFrame. Must contain 'Code' and 'Q_m3s' columns.
    group_by (list of str, optional): The column to group the data by. Default is 'Code'.
    filter_col (str, optional): The column to filter for outliers. Default is 'Q_m3s'.

    Returns:
    pd.DataFrame: The input DataFrame with outliers in the 'Q_m3s' column replaced with NaN.

    Raises:
    ValueError: If the group_by column is not found in the input DataFrame.
    """
    # Preliminary filter for outliers
    def filter_group(group, filter_col):
        # Calculate Q1, Q3, and IQR
        Q1 = group[filter_col].quantile(0.25)
        Q3 = group[filter_col].quantile(0.75)
        IQR = Q3 - Q1

        # Calculate the upper and lower bounds for outliers
        upper_bound = Q3 + 1.5 * IQR
        lower_bound = Q1 - 1.5 * IQR

        #print("DEBUG: upper_bound: ", upper_bound)
        #print("DEBUG: lower_bound: ", lower_bound)
        #print("DEBUG: "filter_col": ", group[filter_col])

        # Set Q_m3s which exceeds lower and upper bounds to nan
        group.loc[group[filter_col] > upper_bound, filter_col] = np.nan
        group.loc[group[filter_col] < lower_bound, filter_col] = np.nan

        # Set the date column as the index
        group[date_col] = pd.to_datetime(group[date_col])
        group.set_index(date_col, inplace=True)

        # Interpolate gaps of length of max 2 days linearly
        group[filter_col] = group[filter_col].interpolate(method='time', limit=2)

        # Reset the index
        group.reset_index(inplace=True)

        return group


    # Test if each entroy of the group_by list is available in the DataFrame
    for col in group_by:
        if col not in combined_data.columns:
            raise ValueError(f"Column '{col}' not found in the DataFrame.")

    # Test if the filter_col column is available
    if filter_col not in combined_data.columns:
        raise ValueError(f"Column '{filter_col}' not found in the DataFrame.")

    # Drop rows with NaN in the group_by columns
    combined_data = combined_data.dropna(subset=[group_by])

    # Replace empty places in the filter_col column with NaN
    combined_data.loc[:, filter_col] = combined_data.loc[:, filter_col].replace('', np.nan)

    # Apply the function to each group
    combined_data = combined_data.groupby(group_by).apply(
        filter_group, filter_col, include_groups=True)

    # Ungroup the DataFrame
    combined_data = combined_data.reset_index(drop=True)

    # Drop rows with duplicate code and dates, keeping the last one
    combined_data = combined_data.drop_duplicates(subset=[group_by, date_col], keep='last')

    return combined_data

# endregion


# --- Data processing tools ---
# region data_processing

def get_data_from_iEH_per_site(
        ieh_sdk, site, start_date=None, end_date=dt.date.today(),
        ieh_var_name='decade_precipitation',
        date_col='date', variable_col='variable', value_col='value',
        code_col='code', unit_col='unit'):
    """
    Reads decadal precipitation sums or average temperatures data from the iEasyHydro database for a given site.

    The names of the dataframe columns can be customized.

    Args:
        ieh_sdk (object): An object that provides a method to get data values for a site from a database.
        site (str or int): The site code.
        start_date (datetime.date or str, optional): The start date of the data to read. Default is None.
        end_date (datetime.date or str, optional): The end date of the data to read. Defaults to dt.date.today().
        ieh_var_name (str, optional): The name of the variable in the iEasyHydro
            database. Default is 'decade_precipitation'. Possible values are
            documented here: https://github.com/hydrosolutions/ieasyhydro-python-sdk under variable types.
        date_col (str, optional): The name of the column containing the date data. Default is 'date'.
        variable_col (str, optional): The name of the column containing the names of the variables. Default is 'variable'.
        value_col (str, optional): The name of the column containing the data. Default is 'value'.
        code_col (str, optional): The name of the column containing the site code. Default is 'code'.
        unit_col (str, optional): The name of the column containing the unit of the data. Default is 'unit'.

    Returns:
        pandas.DataFrame: A DataFrame containing the daily average discharge data or None if no data is found.

    Raises:
        ValueError: If the site code is not a string or an integer.
        ValueError: If the site name is not a string.

    Note:
    The function is especially tailored to read decadal and monthly precipitation
    and temperature data from the iEasyHydro database. I.e. decade_precipitation,
    monthly_preciptiation, decade_temperature, monthly_temperature. The function
    can be used to read other data types as well, but the function is not tested
    for other data types.
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

    logger.debug(f"Reading daily average discharge data for site {site} from {start_date} to {end_date}.")

    # Set the date filters
    if start_date is None:
        filter = BasicDataValueFilters(
            local_date_time__lte=end_date
        )
    else:
        filter = BasicDataValueFilters(
            local_date_time__gte=start_date,
            local_date_time__lte=end_date
        )


    try:
        # Get data for current site from the database
        db_raw = ieh_sdk.get_data_values_for_site(
            site,
            ieh_var_name,
            filters=filter
        )

        # --- Getting meta data ---
        # Extract the variable part of the response
        db_var = db_raw['variable']

        # Cast do dataframe
        db_var = pd.DataFrame([db_var])

        print("DEBUG: db_var: ", db_var)

        # --- Getting data values ---
        # Extract the data_values from the response
        db_raw = db_raw['data_values']

        # Create a DataFrame
        db_df = pd.DataFrame(db_raw)

        # Rename the columns of df to match the columns of combined_data
        db_df = db_df.rename(columns={'local_date_time': date_col, 'data_value': value_col})

        # Drop the columns we don't need
        db_df.drop(columns=['utc_date_time'], inplace=True)

        # Convert the Date column to datetime
        db_df[date_col] = pd.to_datetime(db_df['date'], format='%Y-%m-%d %H:%M:%S').dt.date

        # Add the name and code columns
        db_df[unit_col] = db_var['unit']
        db_df[variable_col] = db_var['variable_type']
        db_df[code_col] = site

    except Exception as e:
        logger.info(f"Skip reading daily average discharge data for site {site}")
        # Return an empty dataframe with required columns 'date', 'code', 'variable', 'value', 'unit'
        db_df = pd.DataFrame(columns=[date_col, code_col, variable_col, value_col, unit_col])

    return db_df

def get_all_decadal_weather_station_data_from_iEHDB(ieh_sdk, meteo_sites):
    """
    Read all available decadal precipitation sums and decadal temperature averages from the iEasyHydro database.
    """

    # Loop over all sites and get the data
    for site in meteo_sites['site_code']:
        site_code = site
        logger.info(f"Reading data for site {site_code}.")

        # Read the data
        site_data_p = get_data_from_iEH_per_site(
            ieh_sdk, site, ieh_var_name='decade_precipitation')

        site_data_t = get_data_from_iEH_per_site(
            ieh_sdk, site, ieh_var_name='decade_temperature')

        # Test if the data is empty
        if site_data_p.empty:
            logger.info(f"No P data found for site {site_code}.")
            if site_data_t.empty:
                logger.info(f"No T data found for site {site_code}.")
            continue

        # Append the data to the combined_data
        if 'combined_data' not in locals():
            combined_data = site_data_p
        else:
            combined_data = pd.concat([combined_data, site_data_p], ignore_index=True)

        if 'combined_data' not in locals():
            combined_data = site_data_t
        else:
            combined_data = pd.concat([combined_data, site_data_t], ignore_index=True)

    if 'combined_data' not in locals():
        logger.info("No data found for any site.")
        return None

    return combined_data

def get_weather_station_list(ieh_sdk):
    """
    Gets site ids for all weather stations available from iEasyHydro.

    Args:
        ieh_sdk (object): An object that provides a method to get data values for a site from a database.

    Returns:
        pandas.DataFrame: A DataFrame containing the site ids for all weather stations available from iEasyHydro.
    """

    # Get a list of all weather stations fro iEasyHydro
    meteo_sites = ieh_sdk.get_meteo_sites()
    meteo_sites = pd.DataFrame(meteo_sites)
    meteo_sites['site_code']

    return meteo_sites

def get_weather_station_data(ieh_sdk):

    meteo_sites = get_weather_station_list(ieh_sdk)

    # For now, only get data for the first 5 sites:
    meteo_sites = meteo_sites.head(5)

    meteo_data = get_all_decadal_weather_station_data_from_iEHDB(ieh_sdk, meteo_sites)

    return meteo_data

# endregion


