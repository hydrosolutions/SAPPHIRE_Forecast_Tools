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
    combined_data = combined_data.dropna(subset=group_by)

    # Replace empty places in the filter_col column with NaN
    combined_data.loc[:, filter_col] = combined_data.loc[:, filter_col].replace('', np.nan)

    # Apply the function to each group
    combined_data = combined_data.groupby(group_by).apply(
        filter_group, filter_col, include_groups=True)

    # Ungroup the DataFrame
    combined_data = combined_data.reset_index(drop=True)

    # Flatten the list to drop duplicates from
    nested_list = [group_by, date_col]
    flattened_list = [item for sublist in nested_list for item in (sublist if isinstance(sublist, list) else [sublist])]

    # Drop rows with duplicate code and dates, keeping the last one
    combined_data = combined_data.drop_duplicates(subset=flattened_list, keep='last')

    return combined_data

def aggregate_decadal_to_monthly(data_df, code_col='code', var_col='variable',
                                 value_col='value', date_col='date', unit_col='unit'):
    """
    Aggregates decadal data to monthly data.

    This function groups the input DataFrame by the group_by column and
    aggregates the values in the value_col column for each month, summing or
    averaging depending on the value of the var_col.

    Parameters:
    data_df (pd.DataFrame): The input DataFrame.
    code_col (str, optional): The column containing the site codes. Default is 'code'.
    var_col (str, optional): The column containing the variable names. Default is 'variable'.
    value_col (str, optional): The column containing the data values. Default is 'value'.
    date_col (str, optional): The column containing the date data. Default is 'date'.
    unit_col (str, optional): The column containing the unit of the data. Default is 'unit'.

    Returns:
    pd.DataFrame: The input DataFrame with the decadal data aggregated to monthly data.

    Raises:
    ValueError: If the code_col column is not found in the input DataFrame.
    ValueError: If the var_col column is not found in the input DataFrame.
    ValueError: If the value_col column is not found in the input DataFrame.
    ValueError: If the date_col column is not found in the input DataFrame.
    ValueError: If the unit_col column is not found in the input DataFrame.
    """
    # Test if the code_col column is available
    if code_col not in data_df.columns:
        raise ValueError(f"Column '{code_col}' not found in the DataFrame.")

    # Test if the var_col column is available
    if var_col not in data_df.columns:
        raise ValueError(f"Column '{var_col}' not found in the DataFrame.")

    # Test if the value_col column is available
    if value_col not in data_df.columns:
        raise ValueError(f"Column '{value_col}' not found in the DataFrame.")

    # Test if the date_col column is available
    if date_col not in data_df.columns:
        raise ValueError(f"Column '{date_col}' not found in the DataFrame.")

    # Test if the unit_col column is available
    if unit_col not in data_df.columns:
        raise ValueError(f"Column '{unit_col}' not found in the DataFrame.")

    # Convert the Date column to datetime
    data_df[date_col] = pd.to_datetime(data_df[date_col], format='%Y-%m-%d')

    # Create a new DataFrame to store the aggregated data
    monthly_data = pd.DataFrame()

    # Group the data by code, variable, and month
    grouped_data = data_df.groupby([code_col, var_col, data_df[date_col].dt.to_period('M')])

    # Loop over the groups and aggregate the data
    for group, data in grouped_data:
        # Get the code, variable, and date from the group
        code, variable, date = group

        # Get the unit of the data
        unit = data[unit_col].iloc[0]

        # Get the sum or average of the data values
        if 'precipitation' in variable.lower():
            value = data[value_col].sum()
        else:
            value = data[value_col].mean()

        # Append the aggregated data to the monthly_data DataFrame
        monthly_data_temp = pd.DataFrame({
                code_col: code,
                var_col: variable,
                date_col: date.to_timestamp(),
                value_col: value,
                unit_col: unit
            }, index=[0])
        if monthly_data.empty:
            monthly_data = monthly_data_temp
        else:
            monthly_data = pd.concat([monthly_data, monthly_data_temp], ignore_index=True)

    return monthly_data

def preprocess_station_meteo_data(data_df, code_col='code', var_col='variable',
                                  date_col='date', value_col='value', unit_col='unit'):
    """
    Pre-processing station weather data for further analysis.

    This function pre-processes the input DataFrame to calculate the percentage
    of the last year's data and the percentage of the norm data. The norm data
    is calculated from the decadal data if it is not present in the data_df
    data frame.

    Parameters:
    data_df (pd.DataFrame): The input DataFrame.
    code_col (str, optional): The column containing the site codes. Default is 'code'.
    var_col (str, optional): The column containing the variable names. Default is 'variable'.
    date_col (str, optional): The column containing the date data. Default is 'date'.
    value_col (str, optional): The column containing the data values. Default is 'value'.
    unit_col (str, optional): The column containing the unit of the data. Default is 'unit'.

    Returns:
    pd.DataFrame: The input DataFrame with the percentage of last year's data and the percentage of the norm data.

    Raises:
    ValueError: If the code_col column is not found in the input DataFrame.
    ValueError: If the var_col column is not found in the input DataFrame.
    ValueError: If the date_col column is not found in the input DataFrame.
    ValueError: If the value_col column is not found in the input DataFrame.
    ValueError: If the unit_col column is not found in the input DataFrame.

    """
    # Test if the code_col column is available
    if code_col not in data_df.columns:
        raise ValueError(f"Column '{code_col}' not found in the DataFrame.")

    # Test if the var_col column is available
    if var_col not in data_df.columns:
        raise ValueError(f"Column '{var_col}' not found in the DataFrame.")

    # Test if the date_col column is available
    if date_col not in data_df.columns:
        raise ValueError(f"Column '{date_col}' not found in the DataFrame.")

    # Test if the value_col column is available
    if value_col not in data_df.columns:
        raise ValueError(f"Column '{value_col}' not found in the DataFrame.")

    # Test if the unit_col column is available
    if unit_col not in data_df.columns:
        raise ValueError(f"Column '{unit_col}' not found in the DataFrame.")

    # Convert the Date column to datetime
    data_df[date_col] = pd.to_datetime(data_df[date_col], format='%Y-%m-%d')
    data_df['day'] = data_df[date_col].dt.day
    data_df['month'] = data_df[date_col].dt.month
    data_df['year'] = data_df[date_col].dt.year

    # Get the current year data and last year data for each site and variable
    # into a new data frame
    current_year_data = data_df[data_df['year'] == dt.date.today().year]
    last_year_data = data_df[data_df['year'] == (dt.date.today().year - 1)]

    # Add 1 year to last year's data so that we can merge it with the current year's data
    last_year_data.loc[:, 'date'] = last_year_data['date'] + pd.DateOffset(years=1)

    # If there is no string 'norm' in the strings in column var_col of data_df,
    # calculate the norm decadal precipitation or temperature as the mean of the
    # available data.
    if 'norm' not in data_df[var_col].unique():
        # Group by code, variable, day and month and calculate the mean of the values
        norm_data = data_df.drop(['unit', 'date', 'year'], axis=1).groupby([code_col, var_col, 'day', 'month']).mean().reset_index()
        # Create a date for the current year using day and month columns for the norm data
        norm_data['year'] = dt.date.today().year
        norm_data['date'] = pd.to_datetime(norm_data[['day', 'month', 'year']].astype(str).agg('-'.join, axis=1), format='%d-%m-%Y')

    # Select the columns we need
    last_year_data = last_year_data[[code_col, var_col, value_col, date_col]]
    norm_data = norm_data[[code_col, var_col, value_col, date_col]]

    # Merge the current year data and last year data
    merged_data = pd.merge(current_year_data, last_year_data, on=[code_col, var_col, date_col], suffixes=('_current', '_last'))
    merged_data = pd.merge(merged_data, norm_data, on=[code_col, var_col, date_col], suffixes=('', '_norm'))
    # Rename the column 'value', if it exists, with 'value_norm'
    if 'value' in merged_data.columns:
        merged_data.rename(columns={'value': 'value_norm'}, inplace=True)

    # Calculate the percentage of last year's data
    merged_data['diff_last_year'] = merged_data[value_col + '_current'] - merged_data[value_col + '_last']
    merged_data['percentage_last_year'] = (merged_data['diff_last_year'] / merged_data[value_col + '_last']) * 100
    merged_data['diff_norm'] = merged_data[value_col + '_current'] - merged_data['value_norm']
    merged_data['percentage_norm'] = (merged_data['diff_norm'] / merged_data['value_norm']) * 100
    # If the last year's data is 0, set the percentage to 100
    merged_data.loc[merged_data[value_col + '_last'] == 0, 'percentage_last_year'] = 100
    # If the norm data is 0, set the percentage to 100
    merged_data.loc[merged_data['value_norm'] == 0, 'percentage_norm'] = 100

    # Drop columns day, month, year
    merged_data.drop(['day', 'month', 'year'], axis=1, inplace=True)

    # melt the columns 'value_current', 'value_last', 'value_norm',
    # 'diff_last_year', 'diff_norm', 'percentage_last_year', 'percentage_norm'
    # to 'variable' and 'value'. Concatenate the existing variable names with the
    # column names.
    merged_data = pd.melt(
        merged_data,
        id_vars=[code_col, var_col, date_col, unit_col],
        value_vars=[value_col + '_current', value_col + '_last', 'value_norm',
                    'diff_last_year', 'diff_norm', 'percentage_last_year',
                    'percentage_norm'],
        var_name='type', value_name='value')

    # Adapt the unit, depending on the type of data
    merged_data.loc[merged_data['type'].str.contains('percentage'), 'unit'] = '%'

    # Round all values to 2 decimal places
    merged_data['value'] = merged_data['value'].round(2)

    return merged_data

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
        db_df[unit_col] = db_var['unit'][0]
        db_df[variable_col] = db_var['variable_type'][0]
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
        elif site_data_p.empty:
            continue
        else:
            combined_data = pd.concat([combined_data, site_data_p], ignore_index=True)

        if 'combined_data' not in locals():
            combined_data = site_data_t
        elif site_data_t.empty:
            continue
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

    meteo_data = get_all_decadal_weather_station_data_from_iEHDB(ieh_sdk, meteo_sites)

    return meteo_data

# endregion

def write_monthly_station_data_to_csv(data: pd.DataFrame):
    """
    Writes a data frame to a csv file.
    """

    filename = os.path.join(
        os.getenv('ieasyforecast_intermediate_data_path'),
        os.getenv('ieasyforecast_monthly_station_statistics_file')
    )

    # Write the data to a csv file, overwriting the file if it already exists
    ret = data.to_csv(filename, index=False)

    return ret
