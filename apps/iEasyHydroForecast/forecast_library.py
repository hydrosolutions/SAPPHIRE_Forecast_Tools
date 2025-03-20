import os
import json
import numpy as np
import pandas as pd
import datetime as dt
import math
import logging
import string
import re
import time
from contextlib import contextmanager

from ieasyhydro_sdk.filters import BasicDataValueFilters

from sklearn.linear_model import LinearRegression

import tag_library as tl

logger = logging.getLogger(__name__)

# === Functions ===
# --- Helper tools ---
# region tools

def get_last_day_of_month(date: dt.date) -> dt.date:
    """
    Get the last day of the month for a given date.

    Parameters:
        date (datetime.date): A date object representing the date to get the
        last day of the month for.

    Returns:
        datetime.date: A date object representing the last day of the month for
        the input date.

    Raises:
        TypeError: If the input date is not a datetime.date object.
        ValueError: If the input date is not a valid date.

    Examples:
        >>> get_last_day_of_month(dt.date(2022, 5, 15))
        datetime.date(2022, 5, 31)
    """
    try:
        # Check if the input date is a datetime.date object
        if not isinstance(date, dt.date):
            raise TypeError('Input date must be a datetime.date object')

        # Raise an error if date is a string
        if isinstance(date, str):
            raise TypeError('Input date must be a datetime.date object, not a string')

        # Get the first day of the next month
        first_day_of_next_month = dt.date(date.year, date.month, 1) + dt.timedelta(days=32)

        # Subtract one day to get the last day of the current month
        last_day_of_month = first_day_of_next_month - dt.timedelta(days=first_day_of_next_month.day)

        return last_day_of_month

    except (TypeError, ValueError) as e:
        # Raise an error if there is an error
        raise e
    except AttributeError as e:
        # Raise an error if the input date is not a valid date
        raise ValueError('Input date is not a valid datetime.date object') from e

def get_predictor_dates_deprecating(input_date: str, n: int):
    '''
    Returns a list of dates from input_date - n to input_date.

    Args:
        input_date (strftime): The starting date with format %YYYY-%MM-%DD.
        n (int): The number of days to go back.

    Returns:
        list: A list of datetime.date objects representing the dates from
            input_date - n to input_date - 1.

    Raises:
        TypeError: If input_date is not a datetime.date object.
        ValueError: If n is not a positive integer.

    Examples:
        >>> get_predictor_dates('2021-05-15', 3)
        [datetime.date(2021, 5, 14), datetime.date(2021, 5, 13), datetime.date(2021, 5, 12)]
    '''
    try:
        # Convert dates in dates_list to datetime.date objects
        input_date = dt.datetime.strptime(input_date, '%Y-%m-%d').date()

        if not isinstance(n, int) or n <= 0:
            raise ValueError('n must be a positive integer')

        date_list = []
        for i in range(1, n+1):
            date = input_date - dt.timedelta(days=i)
            date_list.append(date)

        return date_list

    except (TypeError, ValueError) as e:
        print(f'Error in get_predictor_dates: {e}')
        return None
    except AttributeError as e:
        print(f'Error in get_predictor_dates: {e}')
        return None

def get_predictor_datetimes(input_date: str, n: int):
    '''
    Returns a list of datetimes from input_date - n 00:00 to input_date 12:00.

    Args:
        input_date (strftime): The starting date with format %YYYY-%MM-%DD.
        n (int): The number of days to go back.

    Returns:
        list: A list of datetime objects representing the start dates & times from
            input_date - n at 00:00 to end dates & times input_date at 12:00 (local time of
            data in iEasyHydro DB).

    Raises:
        TypeError: If input_date is not a datetime.date object.
        ValueError: If n is not a positive integer.

    Examples:
        >>> get_predictor_dates('2021-05-15', 3)
        [datetime.date(2021, 5, 15, 12, 0), datetime.date(2021, 5, 13, 0, 0)]
    '''
    try:
        # Convert dates in dates_list to datetime.date objects
        input_date = dt.datetime.strptime(input_date, '%Y-%m-%d')
        #print("\n\nDEBUG get_predictor_datetimes: input_date=", input_date)

        if not isinstance(n, int) or n <= 0:
            raise ValueError('n must be a positive integer')

        end_datetime = dt.datetime(input_date.year, input_date.month, input_date.day, 12, 0)
        start_datetime = end_datetime - dt.timedelta(days=n, hours=12)
        date_list = [start_datetime, end_datetime]

        return date_list

    except (TypeError, ValueError) as e:
        print(f'Error in get_predictor_dates: {e}')
        return None
    except AttributeError as e:
        print(f'Error in get_predictor_dates: {e}')
        return None

def round_discharge_trad_bulletin(value: float) -> str:
    '''
    Round discharge to 3 decimals for values, analogue to
    round_discharge_to_float but convert output to str.

    Args:
        value (str): The discharge value to round.

    Returns:
        str: The rounded discharge value. An empty string is returned in case of
            a negative input value.
    '''
    if value < 0.0:
        return '0.0'
    # Test if the input value is close to zero, default tolerance is 1e-9
    elif math.isclose(value, 0.0, abs_tol=1e-4):
        return '0.0'
    elif value > 0.0 and value < 1.0:
        return "{:.2f}".format(value)
    elif value >= 1.0 and value < 10.0:
        return "{:.2f}".format(value)
    elif value >= 10.0 and value < 100.0:
        return "{:.1f}".format(value)
    else:
        return "{:.0f}".format(value)

def round_discharge_trad_bulletin_3numbers(value: float) -> str:
    '''
    Round discharge to 3 decimals for values, analogue to
    round_discharge_to_float but convert output to str.

    Args:
        value (str): The discharge value to round.

    Returns:
        str: The rounded discharge value. An empty string is returned in case of
            a negative input value.
    '''
    if value < 0.0:
        return '0.0'
    # Test if the input value is close to zero, default tolerance is 1e-9
    elif math.isclose(value, 0.0, abs_tol=1e-4):
        return '0.0'
    elif value > 0.0 and value < 0.001:
        return "{:.6f}".format(value)
    elif value >= 0.001 and value < 0.01:
        return "{:.5f}".format(value)
    elif value >= 0.01 and value < 0.1:
        return "{:.4f}".format(value)
    elif value >=0.1 and value < 1.0:
        return "{:.3f}".format(value)
    elif value >= 1.0 and value < 10.0:
        return "{:.2f}".format(value)
    elif value >= 10.0 and value < 100.0:
        return "{:.1f}".format(value)
    else:
        return "{:.0f}".format(value)

def round_discharge_to_float(value: float) -> float:
    '''
    Round discharge to 3 valid digits.

    Args:
        value (str): The discharge value to round.

    Returns:
        float: The rounded discharge value. An empty string is returned in case of
            a negative input value.

    Examples:
        >>> round_discharge_to_float(0.0)
        0.0
        >>> round_discharge_to_float(0.12345)
        '0.123'
        >>> round_discharge_to_float(0.012345)
        '0.0123'
        >>> round_discharge_to_float(0.0062315)
        '0.00623'
        >>> round_discharge_to_float(1.089)
        '1.09'
        >>> round_discharge_to_float(1.238)
        '1.24'
        >>> round_discharge_to_float(1.0123)
        '1.01'
        >>> round_discharge_to_float(10.123)
        '10.1'
        >>> round_discharge_to_float(100.123)
        '100'
        >>> round_discharge_to_float(1005.123)
        '1005'
    '''
    if not isinstance(value, float):
        raise TypeError('Input value must be a float')
    if isinstance(value, str):
        raise TypeError('Input value must be a float, not a string')

    # Return 0.0 if the input value is negative
    if value < 0.0:
        return 0.0
    # Test if the input value is close to zero, default tolerance is 1e-9
    elif math.isclose(value, 0.0, abs_tol=1e-4):
        return 0.0
    elif value > 0.0 and value < 0.001:
        return round(value, 6)
    elif value >= 0.001 and value < 0.01:
        return round(value, 5)
    elif value >= 0.01 and value < 0.1:
        return round(value, 4)
    elif value >=0.1 and value < 1.0:
        return round(value, 3)
    elif value >= 1.0 and value < 10.0:
        return round(value, 2)
    elif value >= 10.0 and value < 100.0:
        return round(value, 1)
    else:
        return round(value, 0)

def round_discharge(value: float) -> str:
    '''
    Round discharge to 0 decimals for values ge 100, to 1 decimal for values
    ge 10 and to 2 decimals for values ge 0.

    Args:
        value (str): The discharge value to round.

    Returns:
        str: The rounded discharge value. An empty string is returned in case of
            a negative input value.

    Examples:
        >>> round_discharge(0.0)
        '0'
        >>> round_discharge(0.123)
        '0.1'
        >>> round_discharge(0.0123)
        '0.01'
        >>> round_discharge(0.00623)
        '0.01'
        >>> round_discharge(1.0)
        '1'
        >>> round_discharge(1.23)
        '1.2'
        >>> round_discharge(1.0123)
        '1.01'
        >>> round_discharge(10.123)
        '10.1'
        >>> round_discharge(100.123)
        '100'
        >>> round_discharge(1000.123)
        '1000'
    '''
    try:
        if not isinstance(value, float):
            raise TypeError('Input value must be a float')
        if isinstance(value, str):
            raise TypeError('Input value must be a float, not a string')
        # Return an empty string if the input value is negative
        if value < 0.0:
            return " "
        # Test if the input value is close to zero
        elif math.isclose(value, 0.0):
            return "0"
        elif value > 0.0 and value < 10.0:
            return "{:.2f}".format(round(value, 2))
        elif value >= 10.0 and value < 100.0:
            return "{:.1f}".format(round(value, 1))
        else:
            return "{:.0f}".format(round(value, 0))
    except TypeError as e:
        print(f'Error in round_discharge: {e}')
        return None
    except Exception as e:
        print(f'Error in round_discharge: {e}')
        return None

def filter_discharge_data_for_code_and_date(
        df,
        filter_sites,
        filter_date,
        code_col='code',
        date_col='date')-> pd.DataFrame:
    """
    Filter the discharge data for the specified sites and dates.

    Args:
        df (pd.DataFrame): The input DataFrame containing the discharge data.
        filter_sites (list): The list of site codes to filter for.
        filter_date (datetime): The max date to filter for.
        code_col (str): The name of the column containing the site codes.
        date_col (str): The name of the column containing the dates.

    Returns:
        pd.DataFrame: The filtered DataFrame containing the discharge data.

    Raises:
        ValueError: If the input DataFrame does not contain the required columns.
    """
    # Test if the input data contains the required columns
    if not all(column in df.columns for column in [code_col, date_col]):
        raise ValueError(f'DataFrame is missing one or more required columns: {code_col, date_col}')

    # print type of code column in dataframe
    #print(f'Type of code column in the DataFrame: {type(df[code_col].iloc[0])}')
    #print(f"Type of filter_sites: {type(filter_sites[0])}")

    # Only keep rows where the site code is in the filter_sites list
    filtered_data = df[(df[code_col].isin(filter_sites))]

    # print the type of the date column in the DataFrame
    #print(f'Type of date column in the DataFrame: {type(filtered_data[date_col].iloc[0])}')
    #print(f'Type of filter_date: {type(filter_date)}')

    # Filter the data for dates smaller or equal the filter_dates
    filtered_data = filtered_data[(filtered_data[date_col] <= pd.to_datetime(filter_date))]

    return filtered_data

def add_pentad_issue_date(data_df, datetime_col):
    """
    Adds an 'issue_date' column to the DataFrame. The 'issue_date' column is True if the day in the date column
    identified by the string datetime_col is in 5, 10, 15, 20, 25, or the last day of the month. Otherwise, it's False.

    Parameters:
    data_df (DataFrame): The input DataFrame.
    datetime_col (str): The column identifier for the date column.

    Returns:
    DataFrame: The input DataFrame with the 'issue_date' column added.
    """
    # Check if the datetime_col is in the data_df columns
    if datetime_col not in data_df.columns:
        raise KeyError(f"The column {datetime_col} is not in the DataFrame.")
    # Ensure the datetime_col is of datetime type
    try:
        data_df[datetime_col] = pd.to_datetime(data_df[datetime_col], format = "%Y-%m-%d")
    except:
        raise TypeError(f"The column {datetime_col} cannot be converted to datetime type.")

    # Ensure the DataFrame is sorted by date
    data_df = data_df.sort_values(datetime_col)

    # Get the day of the month
    data_df['day'] = data_df[datetime_col].dt.day

    # Get the last day of each month
    #data_df['pdoffsetsMonthEnd'] = pd.offsets.MonthEnd(0)  # add one month end offset
    data_df['end_of_month'] = data_df[datetime_col] + pd.offsets.MonthEnd(0)  # add one month end offset
    data_df['is_end_of_month'] = data_df[datetime_col].dt.day == data_df['end_of_month'].dt.day

    # Set issue_date to True if the day is 5, 10, 15, 20, 25, or the last day of the month
    data_df['issue_date'] = data_df['day'].isin([5, 10, 15, 20, 25]) | data_df['is_end_of_month']

    # Drop the temporary columns
    data_df.drop(['day', 'end_of_month', 'is_end_of_month'], axis=1, inplace=True)

    return data_df

def add_decad_issue_date(data_df, datetime_col):
    """
    Adds an 'issue_date' column to the DataFrame. The 'issue_date' column is
    True if the day in the date column identified by the string datetime_col is
    in 10, 20, or the last day of the month. Otherwise, it's False.

    Parameters:
    data_df (DataFrame): The input DataFrame.
    datetime_col (str): The column identifier for the date column.

    Returns:
    DataFrame: The input DataFrame with the 'issue_date' column added.
    """
    # Check if the datetime_col is in the data_df columns
    if datetime_col not in data_df.columns:
        raise KeyError(f"The column {datetime_col} is not in the DataFrame.")
    # Ensure the datetime_col is of datetime type
    try:
        data_df[datetime_col] = pd.to_datetime(data_df[datetime_col], format = "%Y-%m-%d")
    except:
        raise TypeError(f"The column {datetime_col} cannot be converted to datetime type.")

    # Ensure the DataFrame is sorted by date
    data_df = data_df.sort_values(datetime_col)

    # Get the day of the month
    data_df['day'] = data_df[datetime_col].dt.day

    # Get the last day of each month
    #data_df['pdoffsetsMonthEnd'] = pd.offsets.MonthEnd(0)  # add one month end offset
    data_df['end_of_month'] = data_df[datetime_col] + pd.offsets.MonthEnd(0)  # add one month end offset
    data_df['is_end_of_month'] = data_df[datetime_col].dt.day == data_df['end_of_month'].dt.day

    # Set issue_date to True if the day is 10, 20, or the last day of the month
    data_df['issue_date'] = data_df['day'].isin([10, 20]) | data_df['is_end_of_month']

    # Drop the temporary columns
    data_df.drop(['day', 'end_of_month', 'is_end_of_month'], axis=1, inplace=True)

    return data_df

def calculate_3daydischargesum(data_df, datetime_col, discharge_col):
    """
    Calculate the 3-day discharge sum for each station in the input DataFrame.
    The 3-day discharge sum is the sum of the discharge values for the current
    day and the two previous days.

    Args:
    data_df (pandas.DataFrame):
        The input DataFrame containing the data for each station.
    datetime_col (str):
        The name of the column containing the datetime information.
    discharge_col (str):
        The name of the column containing the discharge information.

    Returns:
    pandas.DataFrame (pandas.DataFrame):
        The modified DataFrame with the 3-day discharge sum for each station in
        column 'discharge_sum'.
    """
    # Raise a Type error if the datetime_col is not of type datetime
    if data_df[datetime_col].dtype != 'datetime64[ns]':
        raise TypeError(f"The column {datetime_col} is not of type datetime.")

    # Ensure the DataFrame is indexed by datetime
    data_df.set_index(datetime_col, inplace=True)

    # Calculate the rolling sum of the discharge values over a 3-day window
    # We use todays value and the two previous days values.
    data_df['discharge_sum'] = data_df[discharge_col].rolling('3D').sum()

    # Set 'discharge_sum' to NaN for rows where 'issue_date' is False
    data_df.loc[~data_df['issue_date'], 'discharge_sum'] = np.nan

    # Reset the index
    data_df.reset_index(inplace=True)

    return data_df

def calculate_pentadaldischargeavg(data_df, datetime_col, discharge_col):
    """
    Calculate the 5-day discharge average for each station in the input DataFrame.

    Note that the last pentad has variable length; from the 26th to the last day
    of the month. The length of the last pentad can be 3, 4, 5, or 6 days.

    Args:
    data_df (pandas.DataFrame):
        The input DataFrame containing the data for each station.
    datetime_col (str):
        The name of the column containing the datetime information.
    discharge_col (str):
        The name of the column containing the discharge information.

    Returns:
    pandas.DataFrame:
        The modified DataFrame with the 5-day discharge average for each station in
        column 'discharge_avg'.
    """
    data_df = data_df.copy(deep=True)
    # Ensure the DataFrame is indexed by datetime
    data_df.set_index(pd.DatetimeIndex(data_df[datetime_col]), inplace=True)

    # Reverse the DataFrame
    data_df = data_df.iloc[::-1]

    # Shift the discharge column by 1 day
    data_df['temp'] = data_df[discharge_col].shift(1)

    # Calculate the rolling average of the discharge values over a n-day window
    data_df['discharge_avg3'] = data_df['temp'].rolling('3D', closed='right').mean()
    data_df['discharge_avg4'] = data_df['temp'].rolling('4D', closed='right').mean()
    data_df['discharge_avg5'] = data_df['temp'].rolling('5D', closed='right').mean()
    data_df['discharge_avg6'] = data_df['temp'].rolling('6D', closed='right').mean()

    # Drop the temporary column
    data_df.drop(columns='temp', inplace=True)

    # Reverse the DataFrame again
    data_df = data_df.iloc[::-1]

    # Reset the index
    data_df.reset_index(inplace=True, drop=True)

    # Check the dates in the datetime_col. Assign the correct discharge_avg
    # depending on the last day of the month. We have to check the last day of
    # the month because the last pentad can have variable length; from the 26th
    # to the last day of the month. The length of the last pentad can be 3, 4, 5,
    # or 6 days.
    # Per default, the discharge_avg is the 5-day discharge_avg
    data_df['discharge_avg'] = data_df['discharge_avg5']
    # If the day of the datetime_col is 25 we need to check if the last pentad
    # has 3, 4, 5, or 6 days.
    # If the day of the datetime_col is 25 and the last day of the month is 28
    # we assign the discharge_avg3 to the discharge_avg column. If the day of
    # the daytime column is 25 and the last day of the month is 29 we assign the
    # discharge_avg4 to the discharge_avg column and so forth.

    # Assign an endo of month column
    data_df['dom'] = data_df[datetime_col].dt.day
    data_df['end_of_month'] = (data_df[datetime_col] + pd.offsets.MonthEnd(0)).dt.day  # add one month end offset
    data_df.loc[(data_df['dom'] == 25) & (data_df['end_of_month'] == 28), 'discharge_avg'] = data_df['discharge_avg3']
    data_df.loc[(data_df['dom'] == 25) & (data_df['end_of_month'] == 29), 'discharge_avg'] = data_df['discharge_avg4']
    data_df.loc[(data_df['dom'] == 25) & (data_df['end_of_month'] == 31), 'discharge_avg'] = data_df['discharge_avg6']

    # Remove the temporary columns
    data_df.drop(columns=['discharge_avg3', 'discharge_avg4', 'discharge_avg5',
                          'discharge_avg6', 'dom', 'end_of_month'], inplace=True)

    # Set 'discharge_avg' to NaN for rows where 'issue_date' is False
    data_df.loc[~data_df['issue_date'], 'discharge_avg'] = np.nan

    return data_df

def calculate_decadaldischargeavg(data_df, datetime_col, discharge_col):
    """
    Calculate the 10-day discharge average for each station in the input DataFrame.

    Note that the last decad can have variable length from the 21st to the last
    day of the month. The length of the last decad can be 8, 9, 10, or 11 days.

    Args:
    data_df (pandas.DataFrame):
        The input DataFrame containing the data for each station.
    datetime_col (str):
        The name of the column containing the datetime information.
    discharge_col (str):
        The name of the column containing the discharge information.

    Returns:
    pandas.DataFrame:
        The modified DataFrame with the 5-day discharge average for each station in
        column 'discharge_avg'.
    """
    data_df = data_df.copy(deep=True)
    # Ensure the DataFrame is indexed by datetime
    data_df.set_index(pd.DatetimeIndex(data_df[datetime_col]), inplace=True)

    # Reverse the DataFrame
    data_df = data_df.iloc[::-1]

    # Shift the discharge column by 1 day
    data_df['temp'] = data_df[discharge_col].shift(1)

    # Calculate the rolling average of the discharge values over a n-day window
    data_df['discharge_avg8D'] = data_df['temp'].rolling('8D', closed='right').mean()
    data_df['discharge_avg9D'] = data_df['temp'].rolling('9D', closed='right').mean()
    data_df['discharge_avg10D'] = data_df['temp'].rolling('10D', closed='right').mean()
    data_df['discharge_avg11D'] = data_df['temp'].rolling('11D', closed='right').mean()

    # Per default, the discharge_avg is the 10-day discharge_avg
    data_df['discharge_avg'] = data_df['discharge_avg10D']
    data_df['dom'] = data_df[datetime_col].dt.day
    data_df['end_of_month'] = (data_df[datetime_col] + pd.offsets.MonthEnd(0)).dt.day  # add one month end offset

    data_df.reset_index(drop=True, inplace=True)
    data_df.loc[(data_df['dom'] == 20) & (data_df['end_of_month'] == 28), 'discharge_avg'] = data_df['discharge_avg8D']
    data_df.loc[(data_df['dom'] == 20) & (data_df['end_of_month'] == 29), 'discharge_avg'] = data_df['discharge_avg9D']
    data_df.loc[(data_df['dom'] == 20) & (data_df['end_of_month'] == 31), 'discharge_avg'] = data_df['discharge_avg11D']

    # Remove the temporary columns
    data_df.drop(columns=['discharge_avg8D', 'discharge_avg9D', 'discharge_avg10D',
                          'discharge_avg11D', 'dom', 'end_of_month'], inplace=True)

    # Drop the temporary column
    data_df.drop(columns='temp', inplace=True)

    # Reverse the DataFrame again
    data_df = data_df.iloc[::-1]

    # The predictor of the current date is the average of the previous
    # decade. If dom is 20, the predictor is the average of the second decade
    # which we find in the discharge_avg column at the 10th day of the
    # month. If dom is the last day of a month, the predictor is the average of
    # the third decade which we find in the discharge_avg column at the 20th
    # day of the month. If dom is 10, the predictor is the average of the first
    # decade which we find in the discharge_avg column at the last day of the
    # previous month.
    # Create a temporary DataFrame that drops the NaN values
    temp_df = data_df[data_df['issue_date'] != False].copy()

    # Shift the 'avg' column in the temporary DataFrame
    temp_df['avg_shifted'] = temp_df['discharge_avg'].shift(1)

    # Merge the shifted column back into the original DataFrame
    data_df = data_df.merge(temp_df[['avg_shifted']], left_index=True, right_index=True, how='left')

    # Rename the shifted column to 'pred'
    data_df.rename(columns={'avg_shifted': 'predictor'}, inplace=True)

    # Reset the index
    data_df.reset_index(inplace=True, drop=True)

    # Set 'discharge_avg' to NaN for rows where 'issue_date' is False
    data_df.loc[~data_df['issue_date'], 'discharge_avg'] = np.nan
    # Same for predictor
    data_df.loc[~data_df['issue_date'], 'predictor'] = np.nan

    return data_df

def generate_issue_and_forecast_dates(data_df_0: pd.DataFrame, datetime_col: str,
                                      station_col: str, discharge_col: str,
                                      forecast_flags):
    """
    Generate issue and forecast dates for each station in the input DataFrame
    and aggregate predictor and target data for pentadal and decadal forecasts.

    Arg:
    data_df (pandas.DataFrame):
        The input DataFrame containing the data for each station.
    datetime_col (str):
        The name of the column containing the datetime information.
    station_col (str)
        The name of the column containing the station information.
    discharge_col (str):
        The name of the column containing the discharge information.
    forecast_flags (config.ForecastFlags):
        Flags that identify the forecast horizons serviced on start_date.

    Returns:
    pandas.DataFrame
        The modified DataFrame with the issue and forecast dates for each station.
    """
    def apply_calculation(data_df, datetime_col, discharge_col):

        # Set negative values to nan
        data_df[discharge_col] = data_df[discharge_col].apply(lambda x: np.nan if x < 0 else x)

        # Fill in data gaps of up to 3 days by linear interpolation
        data_df[discharge_col] = data_df[discharge_col].interpolate(
            method='linear', limit_direction='both', limit=3)

        # Round data to 3 numbers according to the custom of operational hydrology
        # in Kyrgyzstan.
        data_df.loc[:, discharge_col] = data_df.loc[:, discharge_col].apply(round_discharge_to_float)

        data_df = add_pentad_issue_date(data_df, datetime_col)

        data_df = calculate_3daydischargesum(data_df, datetime_col, discharge_col)

        data_df = calculate_pentadaldischargeavg(data_df, datetime_col, discharge_col)

        return(data_df)

    def apply_calculation_decad(data_df, datetime_col, discharge_col):
        # The above functions are valid for pentadal forecasts for Kyg Hydromet.
        # The following functions are valid for decad forecasts for Kyg Hydromet.
        # Set negative values to nan
        data_df[discharge_col] = data_df[discharge_col].apply(lambda x: np.nan if x < 0 else x)

        # Fill in data gaps of up to 3 days by linear interpolation
        data_df[discharge_col] = data_df[discharge_col].interpolate(
            method='linear', limit_direction='both', limit=3)

        # Round data to 3 numbers according to the custom of operational hydrology
        # in Kyrgyzstan.
        data_df.loc[:, discharge_col] = data_df.loc[:, discharge_col].apply(round_discharge_to_float)

        # Identify the issue dates for the decadal forecasts
        data_df_decad = add_decad_issue_date(data_df, datetime_col)
        # Aggregate predictor and target data for the decadal forecasts
        data_df_decad = calculate_decadaldischargeavg(data_df_decad, datetime_col, discharge_col)

        return(data_df_decad)

    logger.info("input: generate_issue_and_forecast_dates")
    logger.info("data_df_0.head(): \n{}".format(data_df_0.head()))

    # Test if the input data contains the required columns
    if not all(column in data_df_0.columns for column in [datetime_col, station_col, discharge_col]):
        raise ValueError(f'DataFrame is missing one or more required columns: {datetime_col, station_col, discharge_col}')

    # Apply the calculation function to each group based on the 'station' column
    data_df_decad = data_df_0.copy(deep=True)
    modified_data = data_df_0.groupby(station_col)[data_df_0.columns].apply(
        apply_calculation,
        datetime_col = datetime_col,
        discharge_col = discharge_col)
    if forecast_flags.decad:
        modified_data_decad = data_df_decad.groupby(station_col)[data_df_decad.columns].apply(
            apply_calculation_decad,
            datetime_col = datetime_col,
            discharge_col = discharge_col)
    else:
        modified_data_decad = []

    # For each Date in modified_data, calculate the pentad of the month
    modified_data['pentad'] = modified_data[datetime_col].apply(tl.get_pentad)
    modified_data['pentad_in_year'] = modified_data[datetime_col].apply(tl.get_pentad_in_year)

    if forecast_flags.decad:
        # For each Date in modified_data_decad, calculate the decad of the month
        modified_data_decad['decad_in_month'] = modified_data_decad[datetime_col].apply(tl.get_decad_in_month)
        modified_data_decad['decad_in_year'] = modified_data_decad[datetime_col].apply(tl.get_decad_in_year)

    return modified_data, modified_data_decad

def save_discharge_avg(modified_data, fc_sites, group_id=None,
                       code_col='code', group_col=None, value_col=None):
    """
    Calculate the norm discharge for each site and write it to the site object.
    """
    # Test if all columns here are in the modified_data DataFrame
    if not all(column in modified_data.columns for column in [code_col, group_col, value_col]):
        raise ValueError(f'DataFrame is missing one or more required columns: {code_col, group_col, value_col}')

    # Group modified_data by Code and calculate the mean over discharge_avg
    # while ignoring NaN values
    norm_discharge = (
        modified_data.reset_index(drop=True).groupby([code_col, group_col], as_index=False)[value_col]
                      .apply(lambda x: x.mean(skipna=True))
    )
    min_discharge = (
        modified_data.reset_index(drop=True).groupby([code_col, group_col], as_index=False)[value_col]
                        .apply(lambda x: x.min(skipna=True))
    )
    max_discharge = (
        modified_data.reset_index(drop=True).groupby([code_col, group_col], as_index=False)[value_col]
                        .apply(lambda x: x.max(skipna=True))
    )

    # Now we need to write the discharge_avg for the current pentad to the site: Site
    for site in fc_sites:
        logger.debug(f'    calculating norm, min,max discharge for site {site.code} ...')
        Site.from_df_get_norm_discharge(
            site, group_id, norm_discharge, min_discharge, max_discharge,
            code_col=code_col, group_col=group_col, value_col=value_col)

    logger.debug(f'   {len(fc_sites)} Norm discharge calculated, namely:\n{[site1.qnorm for site1 in fc_sites]}')
    logger.debug("   ... done")

def save_discharge_avg_decad(modified_data, fc_sites, group_id=None,
                       code_col='code', group_col=None, value_col=None):
    """
    Calculate the norm discharge for each site and write it to the site object.
    """
    # Test if all columns here are in the modified_data DataFrame
    if not all(column in modified_data.columns for column in [code_col, group_col, value_col]):
        raise ValueError(f'DataFrame is missing one or more required columns: {code_col, group_col, value_col}')

    # Group modified_data by Code and calculate the mean over discharge_avg
    # while ignoring NaN values
    norm_discharge = (
        modified_data.reset_index(drop=True).groupby([code_col, group_col], as_index=False)[value_col]
                      .apply(lambda x: x.mean(skipna=True))
    )
    min_discharge = (
        modified_data.reset_index(drop=True).groupby([code_col, group_col], as_index=False)[value_col]
                        .apply(lambda x: x.min(skipna=True))
    )
    max_discharge = (
        modified_data.reset_index(drop=True).groupby([code_col, group_col], as_index=False)[value_col]
                        .apply(lambda x: x.max(skipna=True))
    )

    # Now we need to write the discharge_avg for the current pentad to the site: Site
    for site in fc_sites:
        logger.debug(f'    calculating norm, min,max discharge for site {site.code} ...')
        Site.from_df_get_norm_discharge_decad(
            site, group_id, norm_discharge, min_discharge, max_discharge,
            code_col=code_col, group_col=group_col, value_col=value_col)

    logger.debug(f'   {len(fc_sites)} Norm discharge calculated, namely:\n{[site1.qnorm for site1 in fc_sites]}')
    logger.debug("   ... done")

def get_predictor_dates(start_date, forecast_flags):
    """
    Gets datetimes for which to aggregate the predictors for the linear regression
    method.

    Details:
        For pentadal forecasts, the hydromet uses the sum of the last 2 days
        discharge plus the morning discharge of today.

    Arguments:
        start_date (datetime.date) Date on which the forecast is produced.
        forecast_flags (config.ForecastFlags) Flags that identify the forecast
            horizons serviced on start_date

    Return:
        list: A list of dates for which to aggregate the predictors for the
            linear regression method.
    """
    # Initialise the predictor_dates object
    predictor_dates = PredictorDates()
    # Get the dates to get the predictor from
    if forecast_flags.pentad:
        # For pentadal forecasts, the hydromet uses the sum of the last 2 days discharge.
        predictor_dates.pentad = get_predictor_datetimes(start_date.strftime('%Y-%m-%d'), 2)
        # if predictor_dates is None, raise an error
        if predictor_dates.pentad is None:
            raise ValueError("The predictor dates are not valid.")
    if forecast_flags.decad:
        # For decad forecasts, the hydromet uses the average runoff of the previous decade.
        #predictor_dates.decad = get_predictor_datetimes_for_decadal_forecasts(start_date)
        predictor_dates.decad = get_predictor_datetimes(start_date.strftime('%Y-%m-%d'), 9)
        # if predictor_dates is None, raise an error
        if predictor_dates.decad is None:
            raise ValueError("The predictor dates are not valid.")

    logger.debug(f"   Predictor dates for pentadal forecasts: {predictor_dates.pentad}")
    logger.debug(f"   Predictor dates for decad forecasts: {predictor_dates.decad}")

    return predictor_dates

def get_predictors(data_df, start_date, fc_sites,
                   code_col='code', date_col='date', predictor_col=None):

    logger.debug("Getting predictor for pentadal forecasting ...")
    # Iterate through sites in fc_sites and see if we can find the predictor
    # in result_df.
    # This is mostly relevant for the offline mode.
    for site in fc_sites:
        # Get the data for the site
        site_data = data_df[data_df[code_col] == site.code]
        if int(site.code) == 15292:
            logger.debug("DEBUG: forecasting:get_predictor: site_data1: \n%s",
                          site_data[[date_col, code_col, 'issue_date', predictor_col]].tail(10))
            logger.debug("DEBUG: forecasting:get_predictor: [start_date]: %s", [start_date])

        # Get the predictor from the data for today
        Site.from_df_get_predictor(site, site_data, [start_date],
                                   date_col=date_col, code_col=code_col,
                                     predictor_col=predictor_col)

    logger.debug(f'   {len(fc_sites)} Predictor discharge gotten from df, namely:\n'
                f'{[[site.code, site.predictor] for site in fc_sites]}')
    #print("DEBUG: forecasting:get_predictor: fc_sites: ", fc_sites)
    logger.debug("   ... done")

def get_pentadal_and_decadal_data(forecast_flags=None,
                                  fc_sites_pentad=None, fc_sites_decad=None,
                                  site_list_pentad=None, site_list_decad=None):
    """"
    Reads and pre-processes the data for the pentadal and decadal forecasts.

    This function was previously called at a later stage in the code.

    Legacy: The method was moved to a pre-processing step and now requires to be
    executed for pentadal and decadal forecasts in any case. Therefore, the
    forecast flags are set to true for both forecasts.

    Args:
    forecast_flags (config.ForecastFlags):
        Flags that identify the forecast horizons serviced on start_date.
    fc_sites_pentad (list):
        The list of Site objects for the pentadal forecasts.
    fc_sites_decad (list):
        The list of Site objects for the decadal forecasts.
    site_list_pentad (list):
        The list of site codes to filter for the pentadal forecasts.
    site_list_decad (list):
        The list of site codes to filter for the decadal forecasts.

    Returns:
    pandas.DataFrame, pandas.DataFrame
        The pre-processed pentadal and decadal data.
    """
    def filter_data(data, code_col, sites_list):
        # Filter the data
        filtered_data = data[data[code_col].isin(sites_list)]

        return filtered_data

    # Set the forecast flags to True for both forecasts
    # This is required as a remnant from the previous implementation.
    forecast_flags.pentad = True
    forecast_flags.decad = True

    # Read discharge data and filter for sites required to produce forecasts
    discharge_all = read_daily_discharge_data_from_csv()

    # Aggregate predictors and forecast variables for each issue date (date
    # on which a forecast is produced for the next pentad or decad). Note that
    # pentad of month and pentad of year are added based on the issue date.
    data_pentad, data_decad = generate_issue_and_forecast_dates(
        pd.DataFrame(discharge_all),
        datetime_col='date',
        station_col='code',
        discharge_col='discharge',
        forecast_flags=forecast_flags)

    # Print the first pentad and decad of the year 2023 and 2024
    print("DEBUG: forecasting:get_pentadal_and_decadal_data: data_pentad: \n",
            data_pentad[(data_pentad['date'] < '2023-01-02') & (data_pentad['code'] == '15102')].tail(10))
    print("DEBUG: forecasting:get_pentadal_and_decadal_data: data_pentad: \n",
            data_pentad[(data_pentad['date'] < '2024-01-02') & (data_pentad['code'] == '15102')].tail(10))

    # Only keep rows for sites in the site_lists
    data_pentad = filter_data(data_pentad, 'code', site_list_pentad)
    if forecast_flags.decad:
        data_decad = filter_data(data_decad, 'code', site_list_decad)

    return data_pentad, data_decad

def calculate_runoff_stats(data_df, value_col='discharge_avg'):
    """
    Calculates runoff statistics for each code and pentad or decad of the year
    that are required for the analysis step and the post-processing step. The
    statistics are the mean, standard deviation, and delta factor. Further we
    calculate min, max, and the 5th and 95th percentile as well as the 25th and
    75th percentile. We also put the current year and last calendar years data
    into the DataFrame.

    Args:
    data_df (pd.DataFrame): The input DataFrame containing the data for each station.
    value_col (str): The name of the column containing the discharge information.

    Returns:
    pd.DataFrame: The modified DataFrame with the runoff statistics for each station.

    Raises:
    ValueError: If the value column is not in the DataFrame.
    """
    # Test if the value column is in the data_df columns
    if value_col not in data_df.columns:
        raise ValueError(f'The column {value_col} is not in the DataFrame.')

    # Calculate the mean of the discharge values and write it to a new DataFrame.
    # The DataFrame is already grouped by code and pentad or decad. Keep the
    # grouping variables and calculate the mean of the discharge values.
    data_df_stats = data_df.groupby(['code', 'pentad_in_year']).agg({
    value_col: ['mean', 'min', 'max',
                lambda x: x.quantile(0.05),  # 5th percentile
                lambda x: x.quantile(0.25),  # 25th percentile
                lambda x: x.quantile(0.75),  # 75th percentile
                lambda x: x.quantile(0.95),  # 95th percentile
                # Add more aggregations here
                ]
    }).reset_index()

    # Get last years data from the latest date in the data_df minus 1 year
    last_year = data_df['date'].max() - pd.DateOffset(years=1)
    last_year = last_year.year
    data_df_stats[str(last_year)] = data_df[value_col].loc[data_df['date'].dt.year == last_year]

    # Get current year data from the latest date in the data_df
    current_year = data_df['date'].max().year
    data_df_stats[str(current_year)] = data_df[value_col].loc[data_df['date'].dt.year == current_year]

    print("data_df_stats:")
    print(data_df_stats.head(10))
    print(data_df_stats.tail(10))

    return data_df_stats

def split_name(name: str):
    """Splits a name string from ieasyhydro python sdk into 2 parts"""
    #print("DEBUG: forecasting:split_name: name: ", name)
    name_parts = name.split(' - ')
    #print("DEBUG: forecasting:split_name: first split ' - ' name_parts: ", name_parts)
    # Cound the number of parts to see if the name was split
    if len(name_parts) == 1:
        # If the name is not split by ' - ' then split by ' -'
        name_parts = name.split(' -')
        if len(name_parts) == 1:
            # Try '- '
            name_parts = name.split('- ')
            if len(name_parts) == 1:
                # Test how many '-' are in the name.
                # If there is only one '-' then split by '-'
                if name.count('-') == 1:
                    name_parts = name.split('-')
                    #print("DEBUG: forecasting:split_name: split '-' name_parts: ", name_parts)
                # If there are two '-' then we assume that we can split by the second '-'
                elif name.count('-') == 2:
                    name_parts = name.split('-')
                    # Merge the first two parts
                    name_parts[0] = name_parts[0] + '-' + name_parts[1]
                # If there are 3 '-' then we assume that we can split by the second '-'
                elif name.count('-') == 3:
                    name_parts = name.split('-')
                    # Merge the first two parts
                    name_parts[0] = name_parts[0] + '-' + name_parts[1]
                    # Merge tha last two parts
                    name_parts[1] = name_parts[2] + '-' + name_parts[3]
                # If none of the above applies, we'll not split at all
                else:
                    name_parts = [name, '']

    return name_parts

# endregion


# --- Forecasting ---
# region forecasting

def perform_linear_regression(
        data_df: pd.DataFrame, station_col: str, pentad_col: str, predictor_col: str,
        discharge_avg_col: str, forecast_pentad: int) -> pd.DataFrame:
    '''
    Perform a linear regression for each station & pentad in a DataFrame.

    Details:
    The linear regression is performed for the forecast pentad of the year
    (value between 1 and 72).

    Args:
        data_df (pd.DataFrame): The DataFrame containing the data to perform the
            linear regression on.
        station_col (str): The name of the column containing the station codes.
        pentad_col (str): The name of the column containing the pentad values.
            pentad is a place holder here. It can be pentad or decad.
        predictor_col (str): The name of the column containing the discharge
            predictor values.
        discharge_avg_col (str): The name of the column containing the discharge
            average values.
        forecast_pentad(int): The pentad of the year to perform the linear
            regression for. Must be a value between 1 and 72.

    Returns:
        pd.DataFrame: A DataFrame containing the columns of the input data frame
            plus the columns 'slope', 'intercept' and 'forecasted_discharge',
            as well as basic flow statistics like average pentadal discharge:
            q_mean, standard deviation of pentadal discharge: q_std_sigma, and
            the delta factor used to identifie the acceptable range for a forecast
            delta = 0.674 * sigma.
            The rows of the DataFrame are filtered to the forecast pentad.

    Examples:
        >>> data = {'station': ['A', 'A', 'B', 'B', 'C', 'C'],
                    'pentad': [1, 2, 1, 2, 1, 2],
                    'discharge_sum': [100, 200, 150, 250, 120, 180],
                    'discharge_avg': [10, 20, 15, 25, 12, 18]}
        >>> df = pd.DataFrame(data)
        >>> result = fl.perform_linear_regression(df, 'station', 'pentad', 'discharge_sum', 'discharge_avg', 2)
        >>> print(result)
            station  pentad  discharge_sum  discharge_avg  slope  intercept  forecasted_discharge
        1        A       2            200             20    0.0       200.0                  20.0
        3        B       2            250             25    0.0       250.0                  25.0
        5        C       2            180             18    0.0       180.0                  18.0

    '''
    # Test that all input types are as expected.
    if not isinstance(data_df, pd.DataFrame):
        raise TypeError('data_df must be a pandas.DataFrame object')

    # Validate DataFrame is not empty
    if data_df.empty:
        raise ValueError('Input DataFrame is empty')

    if not isinstance(station_col, str):
        raise TypeError('station_col must be a string')
    if not isinstance(pentad_col, str):
        raise TypeError('pentad_col must be a string')
    if not isinstance(predictor_col, str):
        raise TypeError('predictor_col must be a string')
    if not isinstance(discharge_avg_col, str):
        raise TypeError('discharge_avg_col must be a string')
    if not isinstance(forecast_pentad, int):
        raise TypeError('forecast_pentad must be an integer')
    if not all(column in data_df.columns for column in [station_col, pentad_col, predictor_col, discharge_avg_col]):
        raise ValueError(f'DataFrame is missing one or more required columns.\n   Required columns: station_col, pentad_col, predictor_col, discharge_avg_col\n   present columns {data_df.columns}')

    # Test that the required columns exist in the input DataFrame.
    required_columns = [station_col, pentad_col, predictor_col, discharge_avg_col]
    missing_columns = [col for col in required_columns if not hasattr(data_df, col)]
    if missing_columns:
        raise ValueError(f"DataFrame is missing one or more required columns: {missing_columns}")

    # Make sure pentad_col is of type int and values therein are between 1 and
    # 72.
    data_df[pentad_col] = data_df[pentad_col].astype(float)
    if not all(data_df[pentad_col].between(1, 72)):
        # Print the rows where the values are not between 1 and 72
        print(f"\n\n\nThe following rows have pentad not between 1 and 72: \n{data_df[~data_df[pentad_col].between(1, 71)]}")
        raise ValueError(f'Values in column {pentad_col} are not between 1 and 72')

    # Forecast pentad must be convertable to an int and it must be between 1 and 72
    if not 1 <= forecast_pentad <= 72:
        raise ValueError(f'forecast_pentad must be an integer between 1 and 72')

    # Filter for the forecast pentad
    data_dfp = data_df[data_df[pentad_col] == float(forecast_pentad)]

    # Initialize result DataFrame
    data_dfp = data_dfp.assign(
        slope=1.0,
        intercept=0.0,
        forecasted_discharge=-1.0,
        q_mean=0.0,
        q_std_sigma=0.0,
        delta=0.0,
        rsquared=0.0
    )

    # Create empty DataFrame with expected columns for fallback
    empty_result = pd.DataFrame(columns=[
        station_col, pentad_col, predictor_col, discharge_avg_col,
        'slope', 'intercept', 'forecasted_discharge', 'q_mean',
        'q_std_sigma', 'delta', 'rsquared'
    ])
    # Test if data_df is empty
    if data_dfp.empty:
        logger.warning(f'No data available for pentad {forecast_pentad}')
        logger.warning(f"  Returning default values for slope, intercept, forecasted_discharge, q_mean, q_std_sigma, delta, rsquared")
        logger.debug(f"Tail of data_df: \n{data_df.tail()}")
        # Return an empty data frame
        return empty_result

    # Loop over each station we have data for
    for station in data_dfp[station_col].unique():
        logger.info("Performing linear regression for station %s and pentad %s", station, forecast_pentad)
        # filter for station and pentad. If the DataFrame is empty,
        # raise an error.
        try:
            station_data = data_dfp[(data_dfp[station_col] == station)]
            # Test if station_data is empty
            if station_data.empty:
                logger.info(f'DataFrame is empty after filtering for station {station}')
                continue
        except ValueError as e:
            print(f'Error in perform_linear_regression when filtering for station data: {e}')

        #if int(station) == 15030:
        #    logger.debug("DEBUG: forecasting:perform_linear_regression: station_data: \n%s",
        #                  station_data[['date', pentad_col, station_col, predictor_col, discharge_avg_col]].tail(10))

        # Drop NaN values, i.e. keep only the time steps where both
        # discharge_sum and discharge_avg are not NaN. These correspond to the
        # time steps where we produce a forecast.
        station_data = station_data.dropna()
        if station_data.empty:
            logger.info("No data for station {station} in pentad {forecast_pentad}")
            continue

        # Check if there is a point selection file for the current pentad and month
        # in ieasyforecast_linreg_point_selection
        # Test if a variable ieasyforecast_linreg_point_selection is set. If not,
        # no need to check for a point selection file.
        logger.info("Checking for point selection file.")
        if os.getenv('ieasyforecast_linreg_point_selection') is None:
            logger.info("No point selection files available. Skipping point selection.")
        else:
            # Define the directory to save the data
            SAVE_DIRECTORY = os.path.join(
                os.getenv('ieasyforecast_configuration_path'),
                os.getenv('ieasyforecast_linreg_point_selection', 'linreg_point_selection')
            )
            # Define the file name
            logger.debug(f"forecast_pentad: {forecast_pentad}")
            #logger.debug(f"columns of station_data: {station_data.columns}")
            #logger.debug(f"station_data: {station_data}")
            forecast_date = tl.get_date_for_last_day_in_pentad(forecast_pentad)
            logger.debug(f"forecast_date: {forecast_date}")
            pentad_in_month = tl.get_pentad(forecast_date)
            title_month = tl.get_month_str_en(forecast_date)
            logger.debug(f"pentad_in_month: {pentad_in_month}")
            logger.debug(f"title_month: {title_month}")
            save_file_name = f"{station}_{pentad_in_month}_pentad_of_{title_month}.csv"
            save_file_path = os.path.join(SAVE_DIRECTORY, save_file_name)

            # Check if the file exists
            if os.path.exists(save_file_path):
                logger.info(f"Point selection file {save_file_path} exists. Reading the file.")
                # Read the file into a DataFrame
                point_selection = pd.read_csv(save_file_path)
                # Temporarily add column year to station_data
                station_data['year'] = station_data['date'].dt.year
                # Merge the column 'visible' from point selection into data_dfp
                station_data = station_data.merge(point_selection[['year', 'visible']], on='year', how='left')
                # Filter for rows where 'visible' is True
                station_data = station_data[station_data['visible'] == True]
                # Drop the 'visible' and 'year' columns
                station_data.drop(columns=['visible', 'year'], inplace=True)
                #logger.debug(f"station_data after point selection: {station_data}")

        #if int(station) == 15030:
        #    logger.debug("DEBUG: forecasting:perform_linear_regression: station_data: \n%s",
        #                  station_data[['date', pentad_col, station_col, predictor_col, discharge_avg_col]].tail(10))


        # Get the discharge_sum and discharge_avg columns
        discharge_sum = station_data[predictor_col].values.reshape(-1, 1)
        discharge_avg = station_data[discharge_avg_col].values.reshape(-1, 1)

        #if int(station) == 15030:
        #    logger.debug("DEBUG: forecasting:perform_linear_regression: discharge_sum: \n%s", discharge_sum)
        #    logger.debug("DEBUG: forecasting:perform_linear_regression: discharge_avg: \n%s", discharge_avg)

        # If we have more than 1 data point, perform the linear regression
        if len(discharge_sum) <= 2 or len(discharge_avg) <= 2:
            logger.info(f"Skipping linear regression for station {station} in pentad {forecast_pentad} due to insufficient data points.")
            slope = np.nan
            intercept = np.nan
            q_mean = np.nan
            q_std_sigma = np.nan
            delta = np.nan
            rsquared = np.nan

        else:
            # Perform the linear regression
            model = LinearRegression().fit(discharge_sum, discharge_avg)
            #if int(station) == 15030:
            #    logger.debug("model output: %s", model)
            #    logger.debug("model.coef_: %s", model.coef_)
            #    logger.debug("model.intercept_: %s", model.intercept_)

            # Calculate discharge statistics
            q_mean = np.mean(discharge_avg)
            q_std_sigma = np.std(discharge_avg)
            delta = 0.674 * q_std_sigma
            rsquared = model.score(discharge_sum, discharge_avg)

            #if int(station) == 15030:
            #    logger.debug(f'Station: {station}, pentad: {forecast_pentad}, q_mean: {q_mean}, q_std_sigma: {q_std_sigma}, delta: {delta}')

            # Get the slope and intercept
            slope = model.coef_[0][0]
            intercept = model.intercept_[0]

            # Print the slope and intercept
            logger.debug(f'Station: {station}, pentad: {forecast_pentad}, slope: {slope}, intercept: {intercept}')

        # Store the slope and intercept in the data_df
        data_dfp.loc[(data_dfp[station_col] == station), 'slope'] = slope
        data_dfp.loc[(data_dfp[station_col] == station), 'intercept'] = intercept
        data_dfp.loc[(data_dfp[station_col] == station), 'q_mean'] = q_mean
        data_dfp.loc[(data_dfp[station_col] == station), 'q_std_sigma'] = q_std_sigma
        data_dfp.loc[(data_dfp[station_col] == station), 'delta'] = delta
        data_dfp.loc[(data_dfp[station_col] == station), 'rsquared'] = rsquared

        # Test if station is of same type as data_dfp[station_col][0]
        if type(station) != type(data_dfp.loc[data_dfp.index[0], station_col]):
            raise ValueError(f"Station type {type(station)} does not match the type of data_dfp[station_col][0] {type(data_dfp[station_col][0])}")


        # Calculate the forecasted discharge for the current station and forecast_pentad
        data_dfp.loc[(data_dfp[station_col] == station), 'forecasted_discharge'] = \
            slope * data_dfp.loc[(data_dfp[station_col] == station), predictor_col] + intercept

        # print rows where code == 15292
        #if int(station) == 15030:
        #    #logger.debug("column names of data_dfp:\n%s", station_data.columns)
        #    logger.debug("DEBUG: forecasting:perform_linear_regression: data_dfp after linear regression: \n%s",
        #      data_dfp.loc[data_dfp[station_col] == station, ['date', station_col, pentad_col, predictor_col, discharge_avg_col, 'slope', 'intercept', 'forecasted_discharge']].tail(10))

    return data_dfp

def perform_forecast(fc_sites, group_id=None, result_df=None,
                     code_col='code', group_col='pentad_in_year'):
    # Perform forecast
    logger.debug("Performing pentad forecast ...")

    # Check if result_df is None or empty
    if result_df is None or result_df.empty:
        logger.warning("No regression results available for forecasting. Skipping forecast calculation.")
        return

    # For each site, calculate the forecasted discharge
    for site in fc_sites:
        Site.from_df_calculate_forecast(
            site, group_id=group_id, df=result_df,
            code_col=code_col, group_col=group_col)

    logger.info(f'   {len(fc_sites)} Forecasts calculated, namely:\n'
                f'{[[site.code, site.fc_qexp] for site in fc_sites]}')

    logger.debug("   ... done")

# endregion


# --- Post-processing ---
# region postprocessing

def sdivsigma_nse(data: pd.DataFrame, observed_col: str, simulated_col: str):
    """
    Calculate the forecast efficacy and the Nash-Sutcliffe Efficiency (NSE) for the observed and simulated data.

    NSE = 1 - s/sigma

    Args:
        data (pandas.DataFrame): The input data containing the observed and simulated data.
        observed_col (str): The name of the column containing the observed data.
        simulated_col (str): The name of the column containing the simulated data.

    Returns:
        pandas.Series: A pandas Series containing the forecast efficacy and the NSE value.

    Raises:
        ValueError: If the input data is missing one or more required columns.

    """
    # Test the input. Make sure that the DataFrame contains the required columns
    if not all(column in data.columns for column in [observed_col, simulated_col]):
        raise ValueError(f'DataFrame is missing one or more required columns: {observed_col, simulated_col}')

    #print("DEBUG: forecasting:sdivsigma_nse: data: \n", data)

    # Convert to numpy arrays for faster computation
    # Use float64 for better numerical stability
    observed = data[observed_col].to_numpy(dtype=np.float64)
    simulated = data[simulated_col].to_numpy(dtype=np.float64)

    # Check for empty data after dropping NaNs
    mask = ~(np.isnan(observed) | np.isnan(simulated))
    if not np.any(mask):
        return pd.Series([np.nan, np.nan], index=['sdivsigma', 'nse'])

    # Filter arrays using mask
    observed = observed[mask]
    simulated = simulated[mask]

    # Early return if not enough data points
    if len(observed) < 2:  # Need at least 2 points for std calculation
        logger.info(f"Not enough data points for sdivsigma_nse calculation.")
        print(f"Not enough data points for sdivsigma_nse calculation.")
        return pd.Series([np.nan, np.nan], index=['sdivsigma', 'nse'])

    # Calculate mean once for reuse
    observed_mean = np.mean(observed)

    # Count the number of data points
    n = len(observed)

    # Calculate denominators
    denominator_nse = np.sum((observed - observed_mean) ** 2)
    # sigma: Standard deviation of the observed data
    denominator_sdivsigma = np.std(observed, ddof=1)  # ddof=1 for sample std

    # Check for numerical stability
    if denominator_nse < 1e-10 or denominator_sdivsigma < 1e-10:
        logger.debug(f"Numerical stability issue in sdivsigma_nse:")
        logger.debug(f"denominator_nse: {denominator_nse}")
        logger.debug(f"denominator_sdivsigma: {denominator_sdivsigma}")
        return pd.Series([np.nan, np.nan], index=['sdivsigma', 'nse'])

    try:
        # Calculate differences once for reuse
        differences = observed - simulated

        # Calculate NSE
        numerator_nse = np.sum(differences ** 2)
        nse_value = 1 - (numerator_nse / denominator_nse)

        # Calculate sdivsigma
        # s: Average of squared differences between observed and simulated data
        numerator_sdivsigma = np.sqrt(np.sum(differences ** 2) / (n - 1))
        # s/sigma: Efficacy of the model
        sdivsigma = numerator_sdivsigma / denominator_sdivsigma

        # Sanity checks
        if not (-np.inf < nse_value < np.inf) or not (0 <= sdivsigma < np.inf):
            return pd.Series([np.nan, np.nan], index=['sdivsigma', 'nse'])

        return pd.Series([sdivsigma, nse_value], index=['sdivsigma', 'nse'])

    except (RuntimeWarning, FloatingPointError) as e:
        logger.debug(f"Numerical computation error in sdivsigma_nse: {str(e)}")
        return pd.Series([np.nan, np.nan], index=['sdivsigma', 'nse'])

def forecast_accuracy_hydromet(data: pd.DataFrame, observed_col: str, simulated_col: str, delta_col: str):
    """
    Calculate the forecast accuracy for the observed and simulated data.

    Args:
        data (pandas.DataFrame): The input data containing the observed and simulated data.
        observed_col (str): The name of the column containing the observed data.
        simulated_col (str): The name of the column containing the simulated data.

    Returns:
        pandas.Series: A pandas Series containing the forecast accuracy.

    Raises:
        ValueError: If the input data is missing one or more required columns.

    """
    # Test the input. Make sure that the DataFrame contains the required columns
    if not all(column in data.columns for column in [observed_col, simulated_col, delta_col]):
        raise ValueError(f'DataFrame is missing one or more required columns: {observed_col, simulated_col, delta_col}')

    # Convert to numpy arrays for faster computation
    observed = data[observed_col].to_numpy(dtype=np.float64)
    simulated = data[simulated_col].to_numpy(dtype=np.float64)
    delta_values = data[delta_col].to_numpy(dtype=np.float64)

    # Check for empty data after dropping NaNs
    mask = ~(np.isnan(observed) | np.isnan(simulated) | np.isnan(delta_values))
    if not np.any(mask):
        return pd.Series([np.nan, np.nan], index=['delta', 'accuracy'])

    # Also drop rows where observed, simulated or delta_valus is inf
    mask = mask & ~(np.isinf(observed) | np.isinf(simulated) | np.isinf(delta_values))
    if not np.any(mask):
        return pd.Series([np.nan, np.nan], index=['delta', 'accuracy'])

    # Filter arrays using mask
    observed = observed[mask]
    simulated = simulated[mask]
    delta_values = delta_values[mask]

    # Early return if not enough data points
    if len(observed) < 1:
        return pd.Series([np.nan, np.nan], index=['delta', 'accuracy'])

    try:
        # Calculate absolute differences once
        abs_diff = np.abs(observed - simulated)

        # Calculate accuracy using vectorized operations
        accuracy = np.mean(abs_diff <= delta_values)

        # Get the last delta value (they are all the same)
        delta = delta_values[-1]

        # Sanity checks
        if not (0 <= accuracy <= 1) or not (0 <= delta < np.inf):
            return pd.Series([np.nan, np.nan], index=['delta', 'accuracy'])

        return pd.Series([delta, accuracy], index=['delta', 'accuracy'])

    except (RuntimeWarning, FloatingPointError) as e:
        logger.debug(f"Numerical computation error in forecast_accuracy_hydromet: {str(e)}")
        return pd.Series([np.nan, np.nan], index=['delta', 'accuracy'])

def mae(data: pd.DataFrame, observed_col: str, simulated_col: str):
    """
    Calculate the mean average error between observed and simulated data

    Args:
        data (pandas.DataFrame): The input data containing the observed and simulated data.
        observed_col (str): The name of the column containing the observed data.
        simulated_col (str): The name of the column containing the simulated data.

    Returns:
        pandas.Series: A series containing:
            - mae: mean average error between observed and simulated data
            - n_pairs: number of valid observed-simulated pairs used in calculation

    Raises:
        ValueError: If the input data is missing one or more required columns.
    """
    # Test the input. Make sure that the DataFrame contains the required columns
    if not all(column in data.columns for column in [observed_col, simulated_col]):
        raise ValueError(f'DataFrame is missing one or more required columns: {observed_col, simulated_col}')

    # Convert to numpy arrays for faster computation
    observed = data[observed_col].to_numpy(dtype=np.float64)
    simulated = data[simulated_col].to_numpy(dtype=np.float64)

    # Check for empty data after dropping NaNs
    mask = ~(np.isnan(observed) | np.isnan(simulated))
    if not np.any(mask):
        return pd.Series([np.nan, 0], index=['mae', 'n_pairs'])

    # Filter arrays using mask
    observed = observed[mask]
    simulated = simulated[mask]

    # Early return if not enough data points
    if len(observed) < 1:
        return pd.Series([np.nan, 0], index=['mae', 'n_pairs'])

    try:
        # Calculate MAE using vectorized operations
        mae_value = np.mean(np.abs(observed - simulated))

        # Sanity check
        if not (0 <= mae_value < np.inf):  # MAE must be non-negative
            return pd.Series([np.nan, 0], index=['mae', 'n_pairs'])

        return pd.Series([mae_value, len(observed)], index=['mae', 'n_pairs'])

    except (RuntimeWarning, FloatingPointError) as e:
        logger.debug(f"Numerical computation error in mae: {str(e)}")
        return pd.Series([np.nan, 0], index=['mae', 'n_pairs'])

def calculate_forecast_skill_deprecating(data_df: pd.DataFrame, station_col: str,
                             pentad_col: str, observation_col: str,
                             simulation_col: str) -> pd.DataFrame:
    """
    Calculates the forecast skill for each group in the input data.

    Args:
        data (pandas.DataFrame): The input data containing the observation and
            simulation data.
        station_col (str): The name of the column containing the station
            identifier.
        pentad_col (str): The name of the column containing the pentad data.
        observation_col (str): The name of the column containing the observation
            data.
        simulation_col (str): The name of the column containing the simulation
            data.

    Returns:
        pandas.DataFrame: The modified input data with additional columns
            containing the forecast skill information, namely abolute error,
            observation_std0674 and flag.
    """

    # Test the input. Make sure that the DataFrame contains the required columns
    if not all(column in data_df.columns for column in [station_col, pentad_col, observation_col, simulation_col]):
        raise ValueError(f'DataFrame is missing one or more required columns: {station_col, pentad_col, observation_col, simulation_col}')

    # Initialize columns
    data_df.loc[:, 'absolute_error'] = 0.0
    data_df.loc[:, 'observation_std0674'] = 0.0  # delta
    data_df.loc[:, 'flag'] = 0.0
    data_df.loc[:, 'accuracy'] = 0.0  # percentage of good forecasts
    data_df.loc[:, 'observation_std'] = 0.0  # sigma
    data_df.loc[:, 'observation_std_sanity_check'] = 0.0  # sigma
    data_df.loc[:, 'forecast_std'] = 0.0  # s
    data_df.loc[:, 'sdivsigma'] = 0.0  # s / sigma, "effectiveness" of the model

    # Loop over each station and pentad
    for station in data_df[station_col].unique():
        for pentad in data_df[pentad_col].unique():
            # Get the data for the station and pentad
            station_data = data_df.loc[
                (data_df[station_col] == station) & (data_df[pentad_col] == pentad), :]

            # Drop NaN values
            station_data = station_data.dropna()

            # Calculate the absolute error between the simulation data and the observation data
            absolute_error_forecast = abs(station_data[simulation_col] - station_data[observation_col])
            absolute_error_observed = abs(station_data[observation_col] - station_data[observation_col].mean())
            observation_std = station_data[observation_col].std()
            # The unbiased sample standard deviation sigma is calculated as
            # sigma = sqrt(sum((x_i - x_mean)^2) / (n-1))
            observation_std_sanity_check = math.sqrt(station_data[observation_col].apply(lambda x: (x - station_data[observation_col].mean())**2).sum() / (len(station_data) - 1))
            # The measure s is calculated as
            # s = sqrt(sum((x_i - y_i)^2) / (n-2))
            forecast_std = math.sqrt(station_data.apply(lambda x: (x[observation_col] - x[simulation_col])**2, axis=1).sum() / (len(station_data) - 2))
            sdivsigma = forecast_std / observation_std

            # Note: .std() will yield NaN if there is only one value in the DataFrame
            # Test if the standard deviation is NaN and return 0.0 if it is
            if np.isnan(observation_std):
                observation_std = 0.0

            # Set the flag if the error is smaller than 0.674 times the standard deviation of the observation data
            flag = absolute_error_forecast <= 0.674 * observation_std

            # Calculate the accuracy of the forecast
            accuracy = flag.mean()

            # Delta is 0.674 times the standard deviation of the observation data
            # This is the measure for the allowable uncertainty of a good forecast
            observation_std0674 = 0.674 * observation_std

            # Store the slope and intercept in the data_df
            data_df.loc[
                (data_df[station_col] == station) & (data_df[pentad_col] == pentad),
                'absolute_error'] = absolute_error_forecast
            data_df.loc[
                (data_df[station_col] == station) & (data_df[pentad_col] == pentad),
                'observation_std0674'] = observation_std0674
            data_df.loc[
                (data_df[station_col] == station) & (data_df[pentad_col] == pentad),
                'flag'] = flag
            data_df.loc[
                (data_df[station_col] == station) & (data_df[pentad_col] == pentad),
                'observation_std'] = observation_std
            data_df.loc[
                (data_df[station_col] == station) & (data_df[pentad_col] == pentad),
                'observation_std_sanity_check'] = observation_std_sanity_check
            data_df.loc[
                (data_df[station_col] == station) & (data_df[pentad_col] == pentad),
                'forecast_std'] = forecast_std
            data_df.loc[
                (data_df[station_col] == station) & (data_df[pentad_col] == pentad),
                'sdivsigma'] = sdivsigma
            data_df.loc[
                (data_df[station_col] == station) & (data_df[pentad_col] == pentad),
                'accuracy'] = accuracy

    #print("DEBUG: fl.calculate_forecast_skill: data_df\n", data_df.head(20))
    #print(data_df.tail(20))

    return data_df

def calculate_skill_metrics_pentad(
        observed: pd.DataFrame, simulated: pd.DataFrame, timing_stats=None):
    """
    For each model and hydropost in the simulated DataFrame, calculates a number
    of skill metrics based on the observed DataFrame.

    Args:
        observed (pd.DataFrame): The DataFrame containing the observed data.
        simulated (pd.DataFrame): The DataFrame containing the simulated data.
        timing_stats (TimingStats, optional): Timing statistics collector

    Returns:
        pd.DataFrame: The DataFrame containing the skill metrics for each model
            and hydropost.
        pd.DataFrame: Combined forecasts and observations DataFrame
        timing_stats: Timing statistics collector
    """
    # Create a new timing_stats object if none was provided
    #create_new_timing_stats = False
    if timing_stats is None:
        # Import TimingStats class only if needed
        #from .. postprocessing_forecasts import TimingStats
        #timing_stats = TimingStats()
        #create_new_timing_stats = True

        @contextmanager
        def timer(stats, section):
            yield

    else:
        @contextmanager
        def timer(stats, section):
            stats.start(section)
            try:
                yield
            finally:
                stats.end(section)

    # Test the input. Make sure that the DataFrames contain the required columns
    if not all(column in observed.columns for column in ['code', 'date', 'discharge_avg', 'model_long', 'model_short', 'delta']):
        raise ValueError(f'Observed DataFrame is missing one or more required columns: {["code", "date", "discharge_avg", "model_long", "model_short", "delta"]}')
    if not all(column in simulated.columns for column in ['code', 'date', 'pentad_in_year', 'forecasted_discharge', 'model_long', 'model_short']):
        raise ValueError(f'Simulated DataFrame is missing one or more required columns: {["code", "date", "pentad_in_year", "forecasted_discharge", "model_long", "model_short"]}')

    # Local functions
    def test_for_tuples(df):
        # Identify tuples in each cell
        is_tuple = df.apply(lambda col: col.map(lambda x: isinstance(x, tuple)))
        # Check if there are any True values in is_tuple
        contains_tuples = is_tuple.any(axis=1).any()
        # Test if there are any tuples in the DataFrame
        if contains_tuples:
            logger.debug("There are tuples after the merge.")

            # Step 2: Filter rows that contain any tuples
            rows_with_tuples = df[is_tuple.any(axis=1)]

            # Print rows with tuples
            logger.debug(rows_with_tuples)
        else:
            logger.debug("No tuples found after the merge.")

    def extract_first_parentheses_content(string_list):
        pattern = r'\((.*?)\)'

        result = []
        for string in string_list:
            match = re.search(pattern, string)
            if match:
                result.append(match.group(1))
            else:
                result.append('')  # or None, or any other placeholder

        return result

    def model_long_agg(x):
        # Get unique models
        model_list = x.unique()
        # Only keep strings within brackets (), discard the rest of the string and the brackets
        short_model_list = extract_first_parentheses_content(model_list)
        # Concatenat the model names
        unique_models = ', '.join(sorted(short_model_list))
        return f'Ens. Mean with {unique_models} (EM)'

    def model_short_agg(x):
        return f'EM'

    def filter_for_highly_skilled_forecasts(skill_stats):
        # Get thresholds from environment
        threshold_sdivsigma = os.getenv('ieasyhydroforecast_efficiency_threshold', 0.6)
        threshold_accuracy = os.getenv('ieasyhydroforecast_accuracy_threshold', 0.8)
        threshold_nse = os.getenv('ieasyhydroforecast_nse_threshold', 0.8)

        # Test if threshold_sdivsigma is equal to False
        if threshold_sdivsigma != 'False':
            # Filter for rows where sdivsigma is smaller than the threshold
            skill_stats_ensemble = skill_stats[skill_stats['sdivsigma'] < float(threshold_sdivsigma)].copy()
        else:
            skill_stats_ensemble = skill_stats.copy()

        if threshold_accuracy != 'False':
            # Filter for rows where accuracy is larger than the threshold
            skill_stats_ensemble = skill_stats_ensemble[skill_stats_ensemble['accuracy'] > float(threshold_accuracy)].copy()
        else:
            skill_stats_ensemble = skill_stats_ensemble.copy()

        if threshold_nse != 'False':
            # Filter for rows where nse is larger than the threshold
            skill_stats_ensemble = skill_stats_ensemble[skill_stats_ensemble['nse'] > float(threshold_nse)].copy()
        else:
            skill_stats_ensemble = skill_stats_ensemble.copy()
        #print("DEBUG: skill_stats_ensemble\n", skill_stats_ensemble.head(20))

        return skill_stats_ensemble

    with timer(timing_stats, 'calculate_skill_metrics_pentad - Filter data'):
        # We calculate skill metrics only on forecasts after 2010
        # Filter observed and simulated DataFrames for dates after 2010
        observed = observed[observed['date'].dt.year >= 2010]
        simulated = simulated[simulated['date'].dt.year >= 2010]

    #print(f"DEBUG: simulated.columns\n{simulated.columns}")
    #print(f"DEBUG: simulated.head()\n{simulated.head(8)}")
    #logger.debug(f"DEBUG: simulated.tail()\n{simulated.tail(5)}")
    #print(f"DEBUG: observed.columns\n{observed.columns}")
    #print(f"DEBUG: observed.head()\n{observed.head(8)}")
    #logger.debug(f"DEBUG: observed.tail()\n{observed.tail(5)}")
    # Merge the observed and simulated DataFrames
    with timer(timing_stats, 'calculate_skill_metrics_pentad - Initially merge data'):
        skill_metrics_df = pd.merge(
            simulated,
            observed[['code', 'date', 'discharge_avg', 'delta']],
            on=['code', 'date'])
        #print(f"DEBUG: skill_metrics_df.columns\n{skill_metrics_df.columns}")
        #print(f"DEBUG: skill_metrics_df.head()\n{skill_metrics_df.head(8)}")
        #logger.debug(f"DEBUG: skill_metrics_df.tail()\n{skill_metrics_df.tail(5)}")
        test_for_tuples(skill_metrics_df)

    # Calculate the skill metrics for each group based on the 'pentad_in_year', 'code' and 'model' columns
    with timer(timing_stats, 'calculate_skill_metrics_pentad - Calculate sdivsigma_nse'):
        skill_stats = skill_metrics_df. \
            groupby(['pentad_in_year', 'code', 'model_long', 'model_short'])[skill_metrics_df.columns]. \
            apply(
                sdivsigma_nse,
                observed_col='discharge_avg',
                simulated_col='forecasted_discharge'). \
            reset_index()
        test_for_tuples(skill_stats)
        # Print dimensions of skill_metrics_df and skill_stats
        #print(f"\n\nDEBUG: skill_metrics_df.shape: {skill_metrics_df.shape}")
        #print(f"DEBUG: skill_stats.shape: {skill_stats.shape}\n\n")
        #print(f"DEBUG: nse: skill_stats.columns\n{skill_stats.columns}")
        #print(f"DEBUG: skill_stats.head()\n{skill_stats.head(8)}")

    with timer(timing_stats, 'calculate_skill_metrics_pentad - Calculate mae'):
        mae_stats = skill_metrics_df. \
            groupby(['pentad_in_year', 'code', 'model_long', 'model_short'])[skill_metrics_df.columns]. \
            apply(
                mae,
                observed_col='discharge_avg',
                simulated_col='forecasted_discharge'). \
            reset_index()
        #print("DEBUG: mae_stats\n", mae_stats.columns)
        test_for_tuples(mae_stats)

    with timer(timing_stats, 'calculate_skill_metrics_pentad - Calculate forecast_accuracy_hydromet'):
        accuracy_stats = skill_metrics_df. \
            groupby(['pentad_in_year', 'code', 'model_long', 'model_short'])[skill_metrics_df.columns]. \
            apply(
                forecast_accuracy_hydromet,
                observed_col='discharge_avg',
                simulated_col='forecasted_discharge',
                delta_col='delta').\
            reset_index()
        test_for_tuples(accuracy_stats)
        #print("DEBUG: accuracy_stats\n", accuracy_stats.columns)

    with timer(timing_stats, 'calculate_skill_metrics_pentad - merge all skill stats'):
        # Merge the skill metrics with the accuracy stats
        #print("DEBUG: skill_stats.columns\n", skill_stats.columns)
        #print("DEBUG: accuracy_stats.columns\n", accuracy_stats.columns)
        skill_stats = pd.merge(skill_stats, accuracy_stats, on=['pentad_in_year', 'code', 'model_long', 'model_short'])
        test_for_tuples(skill_stats)

        #print("DEBUG: skill_stats.columns\n", skill_stats.columns)
        #print("DEBUG: mae_stats.columns\n", mae_stats.columns)
        skill_stats = pd.merge(skill_stats, mae_stats, on=['pentad_in_year', 'code', 'model_long', 'model_short'])
        test_for_tuples(skill_stats)
        #print("DEBUG: skill_stats.columns\n", skill_stats.columns)
        #print("DEBUG: skill_stats.head()\n", skill_stats.head(20))

    with timer(timing_stats, 'calculate_skill_metrics_pentad - Calculate ensemble skill metrics for highly skilled forecasts'):
        skill_stats_ensemble = filter_for_highly_skilled_forecasts(skill_stats)
        #print("DEBUG: skill_stats_ensemble\n", skill_stats_ensemble.columns)
        #print("DEBUG: skill_stats_ensemble\n", skill_stats_ensemble.head(20))

        # Now we get the rows from the skill_metrics_df where pentad_in_year, code,
        # model_long and model_short are the same as in skill_stats_ensemble
        skill_metrics_df_ensemble = skill_metrics_df[
            skill_metrics_df['pentad_in_year'].isin(skill_stats_ensemble['pentad_in_year']) &
            skill_metrics_df['code'].isin(skill_stats_ensemble['code']) &
            skill_metrics_df['model_long'].isin(skill_stats_ensemble['model_long']) &
            skill_metrics_df['model_short'].isin(skill_stats_ensemble['model_short'])].copy()
        #print("DEBUG: skill_metrics_df_ensemble\n", skill_metrics_df_ensemble.columns)
        #print("DEBUG: skill_metrics_df_ensemble\n", skill_metrics_df_ensemble.head(20))

        # Drop columns with model_short == NE (neural ensemble)
        skill_metrics_df_ensemble = skill_metrics_df_ensemble[skill_metrics_df_ensemble['model_short'] != 'NE'].copy()
        #print("DEBUG: skill_metrics_df_ensemble\n", skill_metrics_df_ensemble.head(20))

        # Perform the aggregations and keep only the unique combinations
        skill_metrics_df_ensemble_avg = skill_metrics_df_ensemble.groupby(['date', 'code']).agg({
            'pentad_in_year': 'first',
            'forecasted_discharge': 'mean',
            'model_long': model_long_agg,
            'model_short': model_short_agg
        }).reset_index()
        #print("DEBUG: skill_metrics_df_ensemble_avg\n", skill_metrics_df_ensemble_avg.columns)
        #print("DEBUG: skill_metrics_df_ensemble_avg\n", skill_metrics_df_ensemble_avg.head(20))

        # Discard rows with model_long equal to 'Ensemble Mean with  (EM)' or equal to Ensemble Mean with LR (EM)
        skill_metrics_df_ensemble_avg = skill_metrics_df_ensemble_avg[
            (skill_metrics_df_ensemble_avg['model_long'] != 'Ens. Mean with  (EM)') &
            (skill_metrics_df_ensemble_avg['model_long'] != 'Ens. Mean with LR (EM)')].copy()
        #print("DEBUG: skill_metrics_df_ensemble_avg\n", skill_metrics_df_ensemble_avg.head(20))

        # Now recalculate the skill metrics for the ensemble
        ensemble_skill_metrics_df = pd.merge(
            skill_metrics_df_ensemble_avg,
            observed[['code', 'date', 'discharge_avg', 'delta']],
            on=['code', 'date'])
        #print("DEBUG: ensemble_skill_metrics_df\n", ensemble_skill_metrics_df.columns)
        #print("DEBUG: ensemble_skill_metrics_df\n", ensemble_skill_metrics_df.head(20))

        ensemble_skill_stats = ensemble_skill_metrics_df. \
            groupby(['pentad_in_year', 'code', 'model_long', 'model_short'])[ensemble_skill_metrics_df.columns]. \
            apply(
                sdivsigma_nse,
                observed_col='discharge_avg',
                simulated_col='forecasted_discharge'). \
            reset_index()
        #print("DEBUG: ensemble_skill_stats\n", ensemble_skill_stats.columns)
        #print("DEBUG: ensemble_skill_stats\n", ensemble_skill_stats.head(20))

        ensemble_mae_stats = ensemble_skill_metrics_df. \
            groupby(['pentad_in_year', 'code', 'model_long', 'model_short'])[ensemble_skill_metrics_df.columns]. \
            apply(
                mae,
                observed_col='discharge_avg',
                simulated_col='forecasted_discharge').\
            reset_index()

        ensemble_accuracy_stats = ensemble_skill_metrics_df. \
            groupby(['pentad_in_year', 'code', 'model_long', 'model_short'])[ensemble_skill_metrics_df.columns]. \
            apply(
                forecast_accuracy_hydromet,
                observed_col='discharge_avg',
                simulated_col='forecasted_discharge',
                delta_col='delta').\
            reset_index()

        ensemble_skill_stats = pd.merge(
            ensemble_skill_stats, ensemble_mae_stats, on=['pentad_in_year', 'code', 'model_long', 'model_short'])
        ensemble_skill_stats = pd.merge(
            ensemble_skill_stats, ensemble_accuracy_stats, on=['pentad_in_year', 'code', 'model_long', 'model_short'])

        # Append the ensemble skill metrics to the skill metrics
        skill_stats = pd.concat([skill_stats, ensemble_skill_stats], ignore_index=True)

        # Add ensemble mean forecasts to simulated dataframe
        #logger.debug(f"DEBUG: simulated.columns\n{simulated.columns}")
        #logger.debug(f"DEBUG: simulated.head()\n{simulated.head(5)}")
        #logger.debug(f"DEBUG: unique models in simulated: {simulated['model_long'].unique()}")
        #print(f"DEBUG: simulated.columns\n{ensemble_skill_metrics_df.columns}")
        #print("DEBUG: head of ensemble_skill_metrics_df: \n", ensemble_skill_metrics_df.head(5))
        #print("DEBUG: unique models in ensemble_skill_metrics_df: ", ensemble_skill_metrics_df['model_long'].unique())

        # Calculate pentad in month (add 1 day to date)
        ensemble_skill_metrics_df['pentad_in_month'] = (ensemble_skill_metrics_df['date']+dt.timedelta(days=1.0)).apply(tl.get_pentad)

        # Join the two dataframes
        joint_forecasts = pd.merge(
            simulated,
            ensemble_skill_metrics_df[['code', 'date', 'pentad_in_month', 'pentad_in_year', 'forecasted_discharge', 'model_long', 'model_short']],
            on=['code', 'date', 'pentad_in_month', 'pentad_in_year', 'model_long', 'model_short', 'forecasted_discharge'],
            how='outer')

        #print(f"DEBUG: joint_forecasts.columns\n{joint_forecasts.columns}")
        #print(f"DEBUG: joint_forecasts.head()\n{joint_forecasts.head(5)}")
        #print(f"DEBUG: unique models in joint_forecasts: {joint_forecasts['model_long'].unique()}")

        # Remove " from model_long values
        skill_stats['model_long'] = skill_stats['model_long'].str.replace('"', '')
        joint_forecasts['model_long'] = joint_forecasts['model_long'].str.replace('"', '')

    return skill_stats, joint_forecasts, timing_stats



def calculate_skill_metrics_decade(
        observed: pd.DataFrame, simulated: pd.DataFrame, timing_stats=None):
    """
    For each model and hydropost in the simulated DataFrame, calculates a number
    of skill metrics based on the observed DataFrame.

    Args:
        observed (pd.DataFrame): The DataFrame containing the observed data.
        simulated (pd.DataFrame): The DataFrame containing the simulated data.
        timing_stats (TimingStats, optional): Timing statistics collector

    Returns:
        pd.DataFrame: The DataFrame containing the skill metrics for each model
            and hydropost.
        pd.DataFrame: Combined forecasts and observations DataFrame
        timing_stats: Timing statistics collector
    """
    # Create a new timing_stats object if none was provided
    #create_new_timing_stats = False
    if timing_stats is None:
        # Import TimingStats class only if needed
        #from .. postprocessing_forecasts import TimingStats
        #timing_stats = TimingStats()
        #create_new_timing_stats = True

        @contextmanager
        def timer(stats, section):
            yield

    else:
        @contextmanager
        def timer(stats, section):
            stats.start(section)
            try:
                yield
            finally:
                stats.end(section)

    # Test the input. Make sure that the DataFrames contain the required columns
    if not all(column in observed.columns for column in ['code', 'date', 'discharge_avg', 'model_long', 'model_short', 'delta']):
        raise ValueError(f'Observed DataFrame is missing one or more required columns: {["code", "date", "discharge_avg", "model_long", "model_short", "delta"]}')
    if not all(column in simulated.columns for column in ['code', 'date', 'decad_in_year', 'forecasted_discharge', 'model_long', 'model_short']):
        raise ValueError(f'Simulated DataFrame is missing one or more required columns: {["code", "date", "decad_in_year", "forecasted_discharge", "model_long", "model_short"]}')

    # Local functions
    def test_for_tuples(df):
        # Identify tuples in each cell
        is_tuple = df.apply(lambda col: col.map(lambda x: isinstance(x, tuple)))
        # Check if there are any True values in is_tuple
        contains_tuples = is_tuple.any(axis=1).any()
        # Test if there are any tuples in the DataFrame
        if contains_tuples:
            logger.debug("There are tuples after the merge.")

            # Step 2: Filter rows that contain any tuples
            rows_with_tuples = df[is_tuple.any(axis=1)]

            # Print rows with tuples
            logger.debug(rows_with_tuples)
        else:
            logger.debug("No tuples found after the merge.")

    def extract_first_parentheses_content(string_list):
        pattern = r'\((.*?)\)'

        result = []
        for string in string_list:
            match = re.search(pattern, string)
            if match:
                result.append(match.group(1))
            else:
                result.append('')  # or None, or any other placeholder

        return result

    def model_long_agg(x):
        # Get unique models
        model_list = x.unique()
        # Only keep strings within brackets (), discard the rest of the string and the brackets
        short_model_list = extract_first_parentheses_content(model_list)
        # Concatenat the model names
        unique_models = ', '.join(sorted(short_model_list))
        return f'Ens. Mean with {unique_models} (EM)'

    def model_short_agg(x):
        return f'EM'

    def filter_for_highly_skilled_forecasts(skill_stats):
        # Get thresholds from environment
        threshold_sdivsigma = os.getenv('ieasyhydroforecast_efficiency_threshold', 0.6)
        threshold_accuracy = os.getenv('ieasyhydroforecast_accuracy_threshold', 0.8)
        threshold_nse = os.getenv('ieasyhydroforecast_nse_threshold', 0.8)

        # Test if threshold_sdivsigma is equal to False
        if threshold_sdivsigma != 'False':
            # Filter for rows where sdivsigma is smaller than the threshold
            skill_stats_ensemble = skill_stats[skill_stats['sdivsigma'] < float(threshold_sdivsigma)].copy()
        else:
            skill_stats_ensemble = skill_stats.copy()

        if threshold_accuracy != 'False':
            # Filter for rows where accuracy is larger than the threshold
            skill_stats_ensemble = skill_stats_ensemble[skill_stats_ensemble['accuracy'] > float(threshold_accuracy)].copy()
        else:
            skill_stats_ensemble = skill_stats_ensemble.copy()

        if threshold_nse != 'False':
            # Filter for rows where nse is larger than the threshold
            skill_stats_ensemble = skill_stats_ensemble[skill_stats_ensemble['nse'] > float(threshold_nse)].copy()
        else:
            skill_stats_ensemble = skill_stats_ensemble.copy()
        #print("DEBUG: skill_stats_ensemble\n", skill_stats_ensemble.head(20))

        return skill_stats_ensemble

    with timer(timing_stats, 'calculate_skill_metrics_decade - Filter data'):
        # We calculate skill metrics only on forecasts after 2010
        # Filter observed and simulated DataFrames for dates after 2010
        observed = observed[observed['date'].dt.year >= 2010]
        simulated = simulated[simulated['date'].dt.year >= 2010]

    #print(f"DEBUG: simulated.columns\n{simulated.columns}")
    #print(f"DEBUG: simulated.head()\n{simulated.head(8)}")
    #logger.debug(f"DEBUG: simulated.tail()\n{simulated.tail(5)}")
    #print(f"DEBUG: observed.columns\n{observed.columns}")
    #print(f"DEBUG: observed.head()\n{observed.head(8)}")
    #logger.debug(f"DEBUG: observed.tail()\n{observed.tail(5)}")
    # Merge the observed and simulated DataFrames
    with timer(timing_stats, 'calculate_skill_metrics_decade - Initially merge data'):
        skill_metrics_df = pd.merge(
            simulated,
            observed[['code', 'date', 'discharge_avg', 'delta']],
            on=['code', 'date'])
        #print(f"DEBUG: skill_metrics_df.columns\n{skill_metrics_df.columns}")
        #print(f"DEBUG: skill_metrics_df.head()\n{skill_metrics_df.head(8)}")
        #logger.debug(f"DEBUG: skill_metrics_df.tail()\n{skill_metrics_df.tail(5)}")
        test_for_tuples(skill_metrics_df)

    # Calculate the skill metrics for each group based on the 'pentad_in_year', 'code' and 'model' columns
    with timer(timing_stats, 'calculate_skill_metrics_decade - Calculate sdivsigma_nse'):
        skill_stats = skill_metrics_df. \
            groupby(['decad_in_year', 'code', 'model_long', 'model_short'])[skill_metrics_df.columns]. \
            apply(
                sdivsigma_nse,
                observed_col='discharge_avg',
                simulated_col='forecasted_discharge'). \
            reset_index()
        test_for_tuples(skill_stats)
        # Print dimensions of skill_metrics_df and skill_stats
        #print(f"\n\nDEBUG: skill_metrics_df.shape: {skill_metrics_df.shape}")
        #print(f"DEBUG: skill_stats.shape: {skill_stats.shape}\n\n")
        #print(f"DEBUG: nse: skill_stats.columns\n{skill_stats.columns}")
        #print(f"DEBUG: skill_stats.head()\n{skill_stats.head(8)}")

    with timer(timing_stats, 'calculate_skill_metrics_decad - Calculate mae'):
        mae_stats = skill_metrics_df. \
            groupby(['decad_in_year', 'code', 'model_long', 'model_short'])[skill_metrics_df.columns]. \
            apply(
                mae,
                observed_col='discharge_avg',
                simulated_col='forecasted_discharge'). \
            reset_index()
        #print("DEBUG: mae_stats\n", mae_stats.columns)
        test_for_tuples(mae_stats)

    with timer(timing_stats, 'calculate_skill_metrics_decad - Calculate forecast_accuracy_hydromet'):
        accuracy_stats = skill_metrics_df. \
            groupby(['decad_in_year', 'code', 'model_long', 'model_short'])[skill_metrics_df.columns]. \
            apply(
                forecast_accuracy_hydromet,
                observed_col='discharge_avg',
                simulated_col='forecasted_discharge',
                delta_col='delta').\
            reset_index()
        test_for_tuples(accuracy_stats)
        #print("DEBUG: accuracy_stats\n", accuracy_stats.columns)

    with timer(timing_stats, 'calculate_skill_metrics_decad - merge all skill stats'):
        # Merge the skill metrics with the accuracy stats
        #print("DEBUG: skill_stats.columns\n", skill_stats.columns)
        #print("DEBUG: accuracy_stats.columns\n", accuracy_stats.columns)
        skill_stats = pd.merge(skill_stats, accuracy_stats, on=['decad_in_year', 'code', 'model_long', 'model_short'])
        test_for_tuples(skill_stats)

        #print("DEBUG: skill_stats.columns\n", skill_stats.columns)
        #print("DEBUG: mae_stats.columns\n", mae_stats.columns)
        skill_stats = pd.merge(skill_stats, mae_stats, on=['decad_in_year', 'code', 'model_long', 'model_short'])
        test_for_tuples(skill_stats)
        #print("DEBUG: skill_stats.columns\n", skill_stats.columns)
        #print("DEBUG: skill_stats.head()\n", skill_stats.head(20))

    with timer(timing_stats, 'calculate_skill_metrics_decad - Calculate ensemble skill metrics for highly skilled forecasts'):
        skill_stats_ensemble = filter_for_highly_skilled_forecasts(skill_stats)
        #print("DEBUG: skill_stats_ensemble\n", skill_stats_ensemble.columns)
        #print("DEBUG: skill_stats_ensemble\n", skill_stats_ensemble.head(20))

        # Now we get the rows from the skill_metrics_df where decad_in_year, code,
        # model_long and model_short are the same as in skill_stats_ensemble
        skill_metrics_df_ensemble = skill_metrics_df[
            skill_metrics_df['decad_in_year'].isin(skill_stats_ensemble['decad_in_year']) &
            skill_metrics_df['code'].isin(skill_stats_ensemble['code']) &
            skill_metrics_df['model_long'].isin(skill_stats_ensemble['model_long']) &
            skill_metrics_df['model_short'].isin(skill_stats_ensemble['model_short'])].copy()
        #print("DEBUG: skill_metrics_df_ensemble\n", skill_metrics_df_ensemble.columns)
        #print("DEBUG: skill_metrics_df_ensemble\n", skill_metrics_df_ensemble.head(20))

        # Drop columns with model_short == NE (neural ensemble)
        skill_metrics_df_ensemble = skill_metrics_df_ensemble[skill_metrics_df_ensemble['model_short'] != 'NE'].copy()
        #print("DEBUG: skill_metrics_df_ensemble\n", skill_metrics_df_ensemble.head(20))

        # Perform the aggregations and keep only the unique combinations
        skill_metrics_df_ensemble_avg = skill_metrics_df_ensemble.groupby(['date', 'code']).agg({
            'decad_in_year': 'first',
            'forecasted_discharge': 'mean',
            'model_long': model_long_agg,
            'model_short': model_short_agg
        }).reset_index()
        #print("DEBUG: skill_metrics_df_ensemble_avg\n", skill_metrics_df_ensemble_avg.columns)
        #print("DEBUG: skill_metrics_df_ensemble_avg\n", skill_metrics_df_ensemble_avg.head(20))

        # Discard rows with model_long equal to 'Ensemble Mean with  (EM)' or equal to Ensemble Mean with LR (EM)
        skill_metrics_df_ensemble_avg = skill_metrics_df_ensemble_avg[
            (skill_metrics_df_ensemble_avg['model_long'] != 'Ens. Mean with  (EM)') &
            (skill_metrics_df_ensemble_avg['model_long'] != 'Ens. Mean with LR (EM)')].copy()
        #print("DEBUG: skill_metrics_df_ensemble_avg\n", skill_metrics_df_ensemble_avg.head(20))

        # Now recalculate the skill metrics for the ensemble
        ensemble_skill_metrics_df = pd.merge(
            skill_metrics_df_ensemble_avg,
            observed[['code', 'date', 'discharge_avg', 'delta']],
            on=['code', 'date'])
        #print("DEBUG: ensemble_skill_metrics_df\n", ensemble_skill_metrics_df.columns)
        #print("DEBUG: ensemble_skill_metrics_df\n", ensemble_skill_metrics_df.head(20))

        ensemble_skill_stats = ensemble_skill_metrics_df. \
            groupby(['decad_in_year', 'code', 'model_long', 'model_short'])[ensemble_skill_metrics_df.columns]. \
            apply(
                sdivsigma_nse,
                observed_col='discharge_avg',
                simulated_col='forecasted_discharge'). \
            reset_index()
        #print("DEBUG: ensemble_skill_stats\n", ensemble_skill_stats.columns)
        #print("DEBUG: ensemble_skill_stats\n", ensemble_skill_stats.head(20))

        ensemble_mae_stats = ensemble_skill_metrics_df. \
            groupby(['decad_in_year', 'code', 'model_long', 'model_short'])[ensemble_skill_metrics_df.columns]. \
            apply(
                mae,
                observed_col='discharge_avg',
                simulated_col='forecasted_discharge').\
            reset_index()

        ensemble_accuracy_stats = ensemble_skill_metrics_df. \
            groupby(['decad_in_year', 'code', 'model_long', 'model_short'])[ensemble_skill_metrics_df.columns]. \
            apply(
                forecast_accuracy_hydromet,
                observed_col='discharge_avg',
                simulated_col='forecasted_discharge',
                delta_col='delta').\
            reset_index()

        ensemble_skill_stats = pd.merge(
            ensemble_skill_stats, ensemble_mae_stats, on=['decad_in_year', 'code', 'model_long', 'model_short'])
        ensemble_skill_stats = pd.merge(
            ensemble_skill_stats, ensemble_accuracy_stats, on=['decad_in_year', 'code', 'model_long', 'model_short'])

        # Append the ensemble skill metrics to the skill metrics
        skill_stats = pd.concat([skill_stats, ensemble_skill_stats], ignore_index=True)

        # Add ensemble mean forecasts to simulated dataframe
        #logger.debug(f"DEBUG: simulated.columns\n{simulated.columns}")
        #logger.debug(f"DEBUG: simulated.head()\n{simulated.head(5)}")
        #logger.debug(f"DEBUG: unique models in simulated: {simulated['model_long'].unique()}")
        #print(f"DEBUG: simulated.columns\n{ensemble_skill_metrics_df.columns}")
        #print("DEBUG: head of ensemble_skill_metrics_df: \n", ensemble_skill_metrics_df.head(5))
        #print("DEBUG: unique models in ensemble_skill_metrics_df: ", ensemble_skill_metrics_df['model_long'].unique())

        # Calculate pentad in month (add 1 day to date)
        ensemble_skill_metrics_df['decad_in_month'] = (ensemble_skill_metrics_df['date']+dt.timedelta(days=1.0)).apply(tl.get_decad_in_month)

        # Join the two dataframes
        joint_forecasts = pd.merge(
            simulated,
            ensemble_skill_metrics_df[['code', 'date', 'decad_in_month', 'decad_in_year', 'forecasted_discharge', 'model_long', 'model_short']],
            on=['code', 'date', 'decad_in_month', 'decad_in_year', 'model_long', 'model_short', 'forecasted_discharge'],
            how='outer')

        #print(f"DEBUG: joint_forecasts.columns\n{joint_forecasts.columns}")
        #print(f"DEBUG: joint_forecasts.head()\n{joint_forecasts.head(5)}")
        #print(f"DEBUG: unique models in joint_forecasts: {joint_forecasts['model_long'].unique()}")

        # Remove " from model_long values
        skill_stats['model_long'] = skill_stats['model_long'].str.replace('"', '')
        joint_forecasts['model_long'] = joint_forecasts['model_long'].str.replace('"', '')

    return skill_stats, joint_forecasts, timing_stats


# endregion


# --- I/O ---
# region io

def load_all_station_data_from_JSON(file_path: str) -> pd.DataFrame:
    """
    Loads station data from a JSON file and returns a filtered DataFrame.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        pd.DataFrame: A DataFrame containing the discharge station data.

    Raises:
        ValueError: If the JSON file cannot be read.
    """
    try:
        with open(file_path) as f:
            config_all = json.load(f)

            # Check that the JSON object contains the expected keys and values
            if 'stations_available_for_forecast' not in config_all:
                raise ValueError('JSON file does not contain expected key "stations_available_for_forecast"')
            if not isinstance(config_all['stations_available_for_forecast'], dict):
                raise ValueError('Value of key "stations_available_for_forecast" is not a dictionary')

            # Check that each station has keys "name_ru", "river_ru", "punkt_ru",
            # "name_eng", "river_eng", "punkt_eng", "lat", "long", "code" and
            # "display_p"
            for key, value in config_all['stations_available_for_forecast'].items():
                if not isinstance(value, dict):
                    raise ValueError(f'Value of key "{key}" is not a dictionary')
                if 'name_ru' not in value:
                    raise ValueError(f'Station "{key}" does not have key "name_ru"')
                if 'lat' not in value:
                    raise ValueError(f'Station "{key}" does not have key "lat"')
                if 'long' not in value:
                    raise ValueError(f'Station "{key}" does not have key "long"')
                if 'code' not in value:
                    raise ValueError(f'Station "{key}" does not have key "code"')

            # Let's try another import of the json file.
            json_object = config_all['stations_available_for_forecast']

            # Create an empty DataFrame to store the station data
            df = pd.DataFrame()

            # Loop over the keys in the JSON object
            for key in json_object.keys():
                # Create a new DataFrame with the station data
                station_df = pd.DataFrame.from_dict(json_object[key], orient='index').T

                # Add a column to the DataFrame with the header string
                station_df['header'] = key

                # Append the station data to the main DataFrame
                df = pd.concat([df, station_df], ignore_index=True)

            # Filter for code starting with 1
            # Currently commented out to allow for the main code to update the
            # config all stations file.
            # df = df[df['code'].astype(str).str.startswith('1')]

            # Write a column 'site_code' which is 'code' transformed to str
            df['site_code'] = df['code'].astype(str)

            # Print the unique site_codes
            #print("DEBUG: fl.load_all_station_data_from_JSON: unique site_codes: %s", df['site_code'].unique())

            # Test if we have any code 15054 in the DataFrame
            #if df['site_code'].str.contains('15054').any():
            #    print("DEBUG: fl.load_all_station_data_from_JSON: code 15054 found in the DataFrame")
            return df

    except FileNotFoundError as e:
        raise FileNotFoundError('Could not read config file. Error message: {}'.format(e))
    except ValueError as e:
        raise ValueError('Could not read config file. Error message: {}'.format(e))

def load_selected_stations_from_json(file_path: str) -> list:
    """
    Load the selected stations from the JSON file.

    Args:
    file_path (str): The path to the JSON file.

    Returns:
    list: The list of selected stations.

    Raises:
    FileNotFoundError: If the JSON file cannot be read.
    ValueError: If the JSON file does not contain the expected keys and values.
    """
    try:
        with open(file_path) as f:
            config_all = json.load(f)

            # Check that the JSON object contains the expected keys and values
            if 'stationsID' not in config_all:
                raise ValueError('JSON file does not contain expected key "stationsID"')
            if not isinstance(config_all['stationsID'], list):
                raise ValueError('Value of key "stationsID" is not a list')

            return config_all['stationsID']

    except FileNotFoundError as e:
        raise FileNotFoundError('Could not read config file. Error message: {}'.format(e))
    except ValueError as e:
        raise ValueError('Could not read config file. Error message: {}'.format(e))

def read_daily_discharge_data_from_csv():
    """
    Read the discharge data from a csv file specified in the environment.

    Returns:
    --------
    pandas.DataFrame
        The discharge data with columns 'code', 'date', 'discharge' (in m3/s).

    Raises:
    -------
    EnvironmentError
        If the required environment variables are not set.
    FileNotFoundError
        If the specified file does not exist.
    ValueError
        If the DataFrame does not contain the required columns.
    pd.errors.ParserError
        If the specified file cannot be read as a CSV.
    """

    # Check if the required environment variables are set
    data_path = os.getenv("ieasyforecast_intermediate_data_path")
    discharge_file = os.getenv("ieasyforecast_daily_discharge_file")
    if data_path is None or discharge_file is None:
        raise EnvironmentError("The environment variables 'ieasyforecast_intermediate_data_path' and 'ieasyforecast_daily_discharge_file' must be set.")

    file_path = os.path.join(data_path, discharge_file)

    # Check if the specified file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The specified file {file_path} does not exist.")

    # Read the discharge data from the csv file
    try:
        discharge_data = pd.read_csv(file_path, sep=',')
    except pd.errors.ParserError:
        raise pd.errors.ParserError(f"The specified file {file_path} cannot be read as a CSV.")

    # Check if the DataFrame contains the required columns
    required_columns = ['code', 'date', 'discharge']
    if not all(column in discharge_data.columns for column in required_columns):
        raise ValueError(f"The DataFrame does not contain the required columns: {required_columns}")

    # Convert the 'date' column to datetime
    discharge_data['date'] = pd.to_datetime(discharge_data['date'])

    # Cast the 'code' column to string
    discharge_data['code'] = discharge_data['code'].astype(str)

    # Sort the DataFrame by 'code' and 'date'
    discharge_data = discharge_data.sort_values(by=['code', 'date'])
    logger.info("Daily discharge data read from %s", file_path)
    logger.info("Columns: %s", discharge_data.columns)
    logger.info("Head: %s", discharge_data.head())
    logger.info("Tail: %s", discharge_data.tail())

    return discharge_data

def write_linreg_pentad_forecast_data(data: pd.DataFrame):
    """
    Writes the data to a csv file for later reading into the forecast dashboard.
    Checks for duplicates by date and code, keeping only the most recent entry.

    Args:
    data (pd.DataFrame): The data to be written to a csv file.

    Returns:
    None
    """
    # Get the path to the output file
    try:
        output_file_path = os.path.join(
            os.getenv("ieasyforecast_intermediate_data_path"),
            os.getenv("ieasyforecast_analysis_pentad_file"))
    except Exception as e:
        logger.error("Could not get the output file path.")
        print(os.getenv("ieasyforecast_intermediate_data_path"))
        print(os.getenv("ieasyforecast_analysis_pentad_file"))
        raise e

    # Only proceed if data is not empty
    if data.empty:
        return

    # Reset index, dropping the old index
    data = data.reset_index(drop=True)

    # Filter to include only forecast data (where issue_date is True)
    data = data[data['issue_date'] == True]
    data = data.drop(columns=['issue_date', 'discharge'])

    # Round all values to 3 digits
    data = data.round(3)

    # For each code, extract the last row (most recent data in current batch)
    last_line = data.groupby('code').tail(1)

    # Get the max year of the last_line dates
    year = last_line['date'].dt.year.max()
    logger.debug(f'mode of year: {year}')

    # Standardize dates to the current year for consistency
    last_line.loc[last_line['date'].dt.year != year, 'predictor'] = np.nan
    last_line.loc[last_line['date'].dt.year != year, 'discharge_avg'] = np.nan
    last_line.loc[last_line['date'].dt.year != year, 'forecasted_discharge'] = np.nan

    # Iterate over last_line dates. Determine the most frequently occuring date.
    # If the other dates are shifted by 1 day, set the date to the most frequently
    # occuring date.
    for code in last_line['code'].unique():
        date_counts = last_line[last_line['code'] == code]['date'].value_counts()
        if len(date_counts) > 1:
            most_common_date = date_counts.idxmax()
            for date in date_counts.index:
                if date != most_common_date:
                    last_line.loc[(last_line['code'] == code) & (last_line['date'] == date), 'date'] = most_common_date


    # Handle existing file
    existing_data = None
    if os.path.exists(output_file_path):
        # Read existing data
        existing_data = pd.read_csv(output_file_path, parse_dates=['date'])

        # Combine with new data
        combined_data = pd.concat([existing_data, last_line], ignore_index=True)

        # Make sure 'code' column is treated as string (otherwise looking for duplicates will not work as expected)
        combined_data['code'] = combined_data['code'].astype(str)

        # Remove duplicates, keeping last occurrence (which has been added last)
        combined_data = combined_data.drop_duplicates(subset=['date', 'code'], keep='last')

        # Write back to file
        try:
            ret = combined_data.to_csv(output_file_path, index=False)
            logger.info(f"Data written to {output_file_path}. Removed duplicates keeping most recent entries.")
        except Exception as e:
            logger.error(f"Could not write the data to {output_file_path}.")
            raise e
    else:
        # Write the data to a new file
        try:
            ret = last_line.to_csv(output_file_path, index=False)
            if ret is None:
                logger.info(f"Data written to {output_file_path}.")
            else:
                logger.error(f"Could not write the data to {output_file_path}.")
        except Exception as e:
            logger.error(f"Could not write the data to {output_file_path}.")
            raise e

    return ret

def write_linreg_pentad_forecast_data_deprecating(data: pd.DataFrame):
    """
    Writes the data to a csv file for later reading into the forecast dashboard.

    Args:
    data (pd.DataFrame): The data to be written to a csv file.

    Returns:
    None
    """

    # Get the path to the intermediate data folder from the environmental
    # variables and the name of the ieasyforecast_analysis_pentad_file.
    # Concatenate them to the output file path.
    try:
       output_file_path = os.path.join(
            os.getenv("ieasyforecast_intermediate_data_path"),
            os.getenv("ieasyforecast_analysis_pentad_file"))
    except Exception as e:
        logger.error("Could not get the output file path.")
        print(os.getenv("ieasyforecast_intermediate_data_path"))
        print(os.getenv("ieasyforecast_analysis_pentad_file"))
        raise e

    #logger.debug(f'data.head: \n{data.head()}')
    #logger.debug(f'data.tail: \n{data.tail()}')

    # Only write results if data is not empty
    if data.empty:
        return

    # Reset index, dropping the old index
    data = data.reset_index(drop=True)

    # From data dataframe, drop all rows where column issue_date is False.
    # This is done to remove all rows that are not forecasts.
    data = data[data['issue_date'] == True]

    # Drop column 'issue_date' as it is not needed in the final output.
    data = data.drop(columns=['issue_date', 'discharge'])

    # Round all columns values to 3 digits
    data = data.round(3)

    # For each code, extract the last row
    last_line = data.groupby('code').tail(1)

    logger.debug(f'last_line before edits: \n{last_line}')

    # Get the max year of the last_line dates
    year = last_line['date'].dt.year.max()
    logger.debug(f'mode of year: {year}')
    print(f"\n\nmode of year: {year}\n\n")

    # If the year of one date of last_year is not equal to the majority year,
    # set the year of the date to the majority year, set predictor to NaN,
    # set discharge_avg to NaN, set forecasted_discharge to _nan.
    last_line.loc[last_line['date'].dt.year != year, 'predictor'] = np.nan
    last_line.loc[last_line['date'].dt.year != year, 'discharge_avg'] = np.nan
    last_line.loc[last_line['date'].dt.year != year, 'forecasted_discharge'] = np.nan

    # Iterate over last_line dates. Determine the most frequently occuring date.
    # If the other dates are shifted by 1 day, set the date to the most frequently
    # occuring date.
    for code in last_line['code'].unique():
        date_counts = last_line[last_line['code'] == code]['date'].value_counts()
        if len(date_counts) > 1:
            most_common_date = date_counts.idxmax()
            for date in date_counts.index:
                if date != most_common_date:
                    last_line.loc[(last_line['code'] == code) & (last_line['date'] == date), 'date'] = most_common_date

    # Test if all dates are valid dates
    #last_line.loc[last_line['date'].dt.year != year, 'date'] = pd.to_datetime(
    #    last_line.loc[last_line['date'].dt.year != year, 'date'].dt.strftime(f'{year}-%m-%d'))

    logger.debug(f'last_line after edits: \n{last_line}')

    # Test if the output file already exists
    if os.path.exists(output_file_path):
        # Append to the existing file
        with open(output_file_path, 'a') as f:
            ret = last_line.to_csv(f, index=False, header=False)
        if ret is None:
            logger.info(f"Data written to {output_file_path}.")
        else:
            logger.error(f"Could not write the data to {output_file_path}.")
    else:
        # Write the data to a csv file. Raise an error if this does not work.
        # If the data is written to the csv file, log a message that the data
        # has been written.
        try:
            ret = last_line.to_csv(output_file_path, index=False)
            if ret is None:
                logger.info(f"Data written to {output_file_path}.")
            else:
                logger.error(f"Could not write the data to {output_file_path}.")
        except Exception as e:
            logger.error(f"Could not write the data to {output_file_path}.")
            raise e

    return ret

def write_linreg_decad_forecast_data(data: pd.DataFrame):
    """
    Writes the data to a csv file for later reading into the forecast dashboard.
    Checks for duplicates by date and code, keeping only the most recent entry.

    Args:
    data (pd.DataFrame): The data to be written to a csv file.

    Returns:
    None
    """
    # Get the path to the output file
    try:
        output_file_path = os.path.join(
            os.getenv("ieasyforecast_intermediate_data_path"),
            os.getenv("ieasyforecast_analysis_decad_file"))
    except Exception as e:
        logger.error("Could not get the output file path.")
        print(os.getenv("ieasyforecast_intermediate_data_path"))
        print(os.getenv("ieasyforecast_analysis_decad_file"))
        raise e

    # Only proceed if data is not empty
    if data.empty:
        return

    # Reset index, dropping the old index
    data = data.reset_index(drop=True)

    # Filter to include only forecast data (where issue_date is True)
    data = data[data['issue_date'] == True]
    data = data.drop(columns=['issue_date', 'discharge'])

    # Round all values to 3 digits
    data = data.round(3)

    # For each code, extract the last row (most recent data in current batch)
    last_line = data.groupby('code').tail(1)

    # Get the max year of the last_line dates
    year = last_line['date'].dt.year.max()
    logger.debug(f'mode of year: {year}')

    # Standardize dates to the current year for consistency
    last_line.loc[last_line['date'].dt.year != year, 'predictor'] = np.nan
    last_line.loc[last_line['date'].dt.year != year, 'discharge_avg'] = np.nan
    last_line.loc[last_line['date'].dt.year != year, 'forecasted_discharge'] = np.nan

    # Iterate over last_line dates. Determine the most frequently occuring date.
    # If the other dates are shifted by 1 day, set the date to the most frequently
    # occuring date.
    for code in last_line['code'].unique():
        date_counts = last_line[last_line['code'] == code]['date'].value_counts()
        if len(date_counts) > 1:
            most_common_date = date_counts.idxmax()
            for date in date_counts.index:
                if date != most_common_date:
                    last_line.loc[(last_line['code'] == code) & (last_line['date'] == date), 'date'] = most_common_date

    # Handle existing file
    if os.path.exists(output_file_path):
        # Read existing data
        existing_data = pd.read_csv(output_file_path, parse_dates=['date'])

        # Combine with new data
        combined_data = pd.concat([existing_data, last_line])

        # Remove duplicates, keeping last occurrence (most recently added)
        combined_data = combined_data.drop_duplicates(subset=['date', 'code'], keep='last')

        # Sort by code and date for readability
        combined_data = combined_data.sort_values(['code', 'date'])

        # Write back to file
        try:
            ret = combined_data.to_csv(output_file_path, index=False)
            logger.info(f"Data written to {output_file_path}. Removed duplicates keeping most recent entries.")
        except Exception as e:
            logger.error(f"Could not write the data to {output_file_path}.")
            raise e
    else:
        # Write the data to a new file
        try:
            ret = last_line.to_csv(output_file_path, index=False)
            if ret is None:
                logger.info(f"Data written to {output_file_path}.")
            else:
                logger.error(f"Could not write the data to {output_file_path}.")
        except Exception as e:
            logger.error(f"Could not write the data to {output_file_path}.")
            raise e

    return ret

def write_linreg_decad_forecast_data_deprecating(data: pd.DataFrame):
    """
    Writes the data to a csv file for later reading into the forecast dashboard.

    Args:
    data (pd.DataFrame): The data to be written to a csv file.

    Returns:
    None
    """

    # Get the path to the intermediate data folder from the environmental
    # variables and the name of the ieasyforecast_analysis_decad_file.
    # Concatenate them to the output file path.
    try:
       output_file_path = os.path.join(
            os.getenv("ieasyforecast_intermediate_data_path"),
            os.getenv("ieasyforecast_analysis_decad_file"))
    except Exception as e:
        logger.error("Could not get the output file path.")
        print(os.getenv("ieasyforecast_intermediate_data_path"))
        print(os.getenv("ieasyforecast_analysis_decad_file"))
        raise e

    # Reset index, dropping the old index
    data = data.reset_index(drop=True)

    # From data dataframe, drop all rows where column issue_date is False.
    # This is done to remove all rows that are not forecasts.
    data = data[data['issue_date'] == True]

    # Drop column 'issue_date' as it is not needed in the final output.
    data = data.drop(columns=['issue_date', 'discharge'])

    # Round all columns values to 3 digits
    data = data.round(3)

    # Extract the last line of the DataFrame
    last_line = data.groupby('code').tail(1)

    # Get the year of max of the last_line dates
    year = last_line['date'].dt.year.max()
    logger.debug(f'mode of year: {year}')

    # If the year of one date of last_year is not equal to the majority year,
    # set the year of the date to the majority year, set predictor to NaN,
    # set discharge_avg to NaN, set forecasted_discharge to _nan.
    last_line.loc[last_line['date'].dt.year != year, 'predictor'] = np.nan
    last_line.loc[last_line['date'].dt.year != year, 'discharge_avg'] = np.nan
    last_line.loc[last_line['date'].dt.year != year, 'forecasted_discharge'] = np.nan

    # Iterate over last_line dates. Determine the most frequently occuring date.
    # If the other dates are shifted by 1 day, set the date to the most frequently
    # occuring date.
    for code in last_line['code'].unique():
        date_counts = last_line[last_line['code'] == code]['date'].value_counts()
        if len(date_counts) > 1:
            most_common_date = date_counts.idxmax()
            for date in date_counts.index:
                if date != most_common_date:
                    last_line.loc[(last_line['code'] == code) & (last_line['date'] == date), 'date'] = most_common_date

    # Test if the output file already exists
    if os.path.exists(output_file_path):
        # Append to the existing file
        with open(output_file_path, 'a') as f:
            ret = last_line.to_csv(f, index=False, header=False)
        if ret is None:
            logger.info(f"Data written to {output_file_path}.")
        else:
            logger.error(f"Could not write the data to {output_file_path}.")
    else:
        # Write the data to a csv file. Raise an error if this does not work.
        # If the data is written to the csv file, log a message that the data
        # has been written.
        try:
            ret = last_line.to_csv(output_file_path, index=False)
            if ret is None:
                logger.info(f"Data written to {output_file_path}.")
            else:
                logger.error(f"Could not write the data to {output_file_path}.")
        except Exception as e:
            logger.error(f"Could not write the data to {output_file_path}.")
            raise e

    return ret

def is_leap_year(year):
    if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
        return True
    else:
        return False

def write_pentad_hydrograph_data(data: pd.DataFrame, iehhf_sdk = None):
    """
    Calculates statistics of the pentadal hydrograph and saves it to a csv file.

    Args:
    data (pd.DataFrame): The data to be written to a csv file.
    iehhf_sdk (ieasyhydroforecast_sdk): The iEH HF SDK object. Required only if
        norms are to be read from iEH HF.

    Returns:
    None
    """

    # Only keep rows where issue_date is True
    data = data[data['issue_date'] == True].copy()

    # Drop the issue_date column
    data = data.drop(columns=['issue_date', 'discharge'])

    # If there is a column called discharge_sum, rename it to predictor
    if 'discharge_sum' in data.columns:
        data = data.rename(columns={'discharge_sum': 'predictor'})

    # These runoff statistics are now written to the date of the forecast
    # production. For the hydrograph output, we want the date to reflect the
    # pentad, the data is collected for. Therefore, we add 1 day to the 'date'
    # column and recalculate pentad and pentad_in_year.
    # Calculate pentad and pentad_in_year
    data.loc[:, 'pentad'] = (data['date'] + pd.Timedelta(days=1)).apply(tl.get_pentad)
    data.loc[:, 'pentad_in_year'] = (data['date'] + pd.Timedelta(days=1)).apply(tl.get_pentad_in_year)
    # Get year of the latest date in data
    current_year = data['date'].dt.year.max()

    logger.debug(f"Calculating pentadal runoff statistics with data from {data['date'].min()} to {data['date'].max()}")

    # If we are not in a leap year, drop the 29th of February and adjust the day_of_year
    data['day_of_year'] = data['date'].dt.dayofyear
    if not is_leap_year(current_year):
        data = data[~((data['date'].dt.month == 2) & (data['date'].dt.day == 29))]
        data.loc[(data['date'].dt.month > 2), 'day_of_year'] -= 1

    runoff_stats = data[data['date'].dt.year != current_year]. \
        reset_index(drop=True). \
        groupby(['code', 'pentad_in_year']). \
        agg(mean=pd.NamedAgg(column='discharge_avg', aggfunc='mean'),
            min=pd.NamedAgg(column='discharge_avg', aggfunc='min'),
            max=pd.NamedAgg(column='discharge_avg', aggfunc='max'),
            q05=pd.NamedAgg(column='discharge_avg', aggfunc=lambda x: x.quantile(0.05)),
            q25=pd.NamedAgg(column='discharge_avg', aggfunc=lambda x: x.quantile(0.25)),
            q75=pd.NamedAgg(column='discharge_avg', aggfunc=lambda x: x.quantile(0.75)),
            q95=pd.NamedAgg(column='discharge_avg', aggfunc=lambda x: x.quantile(0.95))). \
        reset_index(drop=False)
    # If the forecast tools are connected to iEH HF, we get the norm values from there.
    if os.getenv('ieasyhydroforecast_connect_to_iEH') == 'False':
        # Read the norm data from iEH HF
        # Test if iehhf_sdk is not None, throw an error if it is
        if iehhf_sdk is None:
            raise ValueError("ieasyhydroforecast_sdk object is required to read norms from iEH HF.")
        # Read the norms from iEH HF for each site
        all_pentadal_norms = pd.DataFrame({'pentad_in_year': range(1, 73)})
        # Cast pentad in year to string
        all_pentadal_norms['pentad_in_year'] = all_pentadal_norms['pentad_in_year'].astype(str)
        for code in runoff_stats['code'].unique():
            try:
                temp_norm = iehhf_sdk.get_norm_for_site(code, "discharge", norm_period="p")
            except Exception as e:
                logger.error(f"Could not get norm for site {code}.")
                temp_norm = []
            if len(temp_norm) == 72:
                all_pentadal_norms[code] = temp_norm
            else:
                all_pentadal_norms[code] = [None] * 72  # 72 pentads in a year
        # Melt to long format
        all_pentadal_norms = all_pentadal_norms.melt(id_vars=['pentad_in_year'], var_name='code', value_name='norm')
        # Merge with runoff_stats
        runoff_stats = pd.merge(runoff_stats, all_pentadal_norms, left_on=['pentad_in_year', 'code'], right_on=['pentad_in_year', 'code'], how='left')
    else:
        # Add a norm column to runoff_stats which is NaN
        runoff_stats['norm'] = np.nan

    # Get current and last years data for each station and pentad_in_year and
    # merge to runoff_stats
    last_year = data['date'].dt.year.max() - 1
    current_year = data['date'].dt.year.max()
    last_year_data = data[data['date'].dt.year == last_year]
    current_year_data = data[data['date'].dt.year == current_year]
    #last_year_data = last_year_data.drop(columns=['date'])
    # Add 1 year to date of last_year_data
    #last_year_data.loc[:, 'date'] = last_year_data.loc[:, 'date'] + pd.DateOffset(years=1)
    last_year_data.loc[:, 'date'] = pd.Timestamp(str(current_year)) + pd.to_timedelta(last_year_data['day_of_year'] - 1, unit='D')
    current_year_data = current_year_data.drop(columns=['date'])
    last_year_data = last_year_data.rename(columns={'discharge_avg': str(last_year)}).reset_index(drop=True)
    current_year_data = current_year_data.rename(columns={'discharge_avg': str(current_year)}).reset_index(drop=True)

    runoff_stats = pd.merge(runoff_stats, last_year_data, on=['code', 'pentad_in_year'], how='left')
    runoff_stats = pd.merge(runoff_stats, current_year_data[['code', 'pentad_in_year', str(current_year)]], on=['code', 'pentad_in_year'], how='left')

    # Drop the column predictor if it is in runoff_stats
    if 'predictor' in runoff_stats.columns:
        runoff_stats = runoff_stats.drop(columns=['predictor'])

    # Round all values to 3 decimal places
    runoff_stats = runoff_stats.round(3)

    # Sort the DataFrame by 'code' and 'pentad_in_year', using 'pentad_in_year'
    # as numerical values
    runoff_stats['pentad_in_year'] = runoff_stats['pentad_in_year'].astype(int)
    runoff_stats = runoff_stats.sort_values(by=['code', 'pentad_in_year'])

    # Get the path to the intermediate data folder from the environmental
    # variables and the name of the ieasyforecast_hydrograph_pentad_file.
    # Concatenate them to the output file path.
    try:
        output_file_path = os.path.join(
            os.getenv("ieasyforecast_intermediate_data_path"),
            os.getenv("ieasyforecast_hydrograph_pentad_file"))
    except Exception as e:
        logger.error("Could not get the output file path.")
        print(os.getenv("ieasyforecast_intermediate_data_path"))
        print(os.getenv("ieasyforecast_hydrograph_pentad_file"))
        raise e

    # Overwrite the file if it already exists
    if os.path.exists(output_file_path):
        os.remove(output_file_path)

    # Write the data to a csv file. Raise an error if this does not work.
    # If the data is written to the csv file, log a message that the data
    # has been written.
    try:
        ret = runoff_stats.to_csv(output_file_path, index=False)
        logger.info(f"Data written to {output_file_path}.")
    except Exception as e:
        logger.error(f"Could not write the data to {output_file_path}.")
        raise e

    return ret

def write_decad_hydrograph_data(data: pd.DataFrame, iehhf_sdk = None):
    """
    Calculates statistics of the decadal hydrograph and saves it to a csv file.

    Args:
    data (pd.DataFrame): The data to be written to a csv file.
    iehhf_sdk (ieasyhydroforecast_sdk): The iEH HF SDK object. Required only if
        norms are to be read from iEH HF.

    Returns:
    None
    """

    # Only keep rows where issue_date is True
    data = data[data['issue_date'] == True]

    # Drop the issue_date column
    data = data.drop(columns=['issue_date', 'discharge'])

    # If there is a column called discharge_sum, rename it to predictor
    if 'discharge_sum' in data.columns:
        data = data.rename(columns={'discharge_sum': 'predictor'})

    # These runoff statistics are now written to the date of the forecast
    # production. For the hydrograph output, we want the date to reflect the
    # decad, the data is collected for. Therefore, we add 1 day to the 'date'
    # column and recalculate decad and decad_in_year.
    # Add 1 day to the date column
    data.loc[:, 'date'] = data.loc[:, 'date'] + pd.DateOffset(days=1)
    # Calculate decad and decad_in_year
    data.loc[:, 'decad'] = data['date'].apply(tl.get_decad_in_month)
    data.loc[:, 'decad_in_year'] = data['date'].apply(tl.get_decad_in_year)

    # Calculate runoff statistics
    runoff_stats = data. \
        reset_index(drop=True). \
        groupby(['code', 'decad_in_year']). \
        agg(mean=pd.NamedAgg(column='discharge_avg', aggfunc='mean'),
            min=pd.NamedAgg(column='discharge_avg', aggfunc='min'),
            max=pd.NamedAgg(column='discharge_avg', aggfunc='max'),
            q05=pd.NamedAgg(column='discharge_avg', aggfunc=lambda x: x.quantile(0.05)),
            q25=pd.NamedAgg(column='discharge_avg', aggfunc=lambda x: x.quantile(0.25)),
            q75=pd.NamedAgg(column='discharge_avg', aggfunc=lambda x: x.quantile(0.75)),
            q95=pd.NamedAgg(column='discharge_avg', aggfunc=lambda x: x.quantile(0.95))). \
        reset_index(drop=False)
    # If the forecast tools are connected to iEH HF, we get the norm values from there.
    if os.getenv('ieasyhydroforecast_connect_to_iEH') == 'False':
        # Read the norm data from iEH HF
        # Test if iehhf_sdk is not None, throw an error if it is
        if iehhf_sdk is None:
            raise ValueError("ieasyhydroforecast_sdk object is required to read norms from iEH HF.")
        # Read the norms from iEH HF for each site
        all_pentadal_norms = pd.DataFrame({'decad_in_year': range(1, 37)})
        # Cast pentad in year to string
        all_pentadal_norms['decad_in_year'] = all_pentadal_norms['decad_in_year'].astype(str)
        for code in runoff_stats['code'].unique():
            try:
                temp_norm = iehhf_sdk.get_norm_for_site(code, "discharge")
            except Exception as e:
                logger.error(f"Could not get norm for site {code}.")
                temp_norm = []
            if len(temp_norm) == 36:
                #print(f"code {code} len(temp_norm): {len(temp_norm)}\ntemp_norm: {temp_norm}")
                all_pentadal_norms[code] = temp_norm
            else:
                all_pentadal_norms[code] = [None] * 36  # 36 decads in a year
        # Melt to long format
        all_pentadal_norms = all_pentadal_norms.melt(id_vars=['decad_in_year'], var_name='code', value_name='norm')
        # Merge with runoff_stats
        runoff_stats = pd.merge(runoff_stats, all_pentadal_norms, left_on=['decad_in_year', 'code'], right_on=['decad_in_year', 'code'], how='left')
    else:
        # Add a norm column to runoff_stats which is NaN
        runoff_stats['norm'] = np.nan

    # Get current and last years data for each station and pentad_in_year and
    # merge to runoff_stats
    last_year = data['date'].dt.year.max() - 1
    current_year = data['date'].dt.year.max()
    last_year_data = data[data['date'].dt.year == last_year]
    current_year_data = data[data['date'].dt.year == current_year]
    #last_year_data = last_year_data.drop(columns=['date'])
    # Add 1 year to date of last_year_data
    last_year_data.loc[:, 'date'] = last_year_data.loc[:, 'date'] + pd.DateOffset(years=1)
    current_year_data = current_year_data.drop(columns=['date'])
    last_year_data = last_year_data.rename(columns={'discharge_avg': str(last_year)}).reset_index(drop=True)
    current_year_data = current_year_data.rename(columns={'discharge_avg': str(current_year)}).reset_index(drop=True)

    runoff_stats = pd.merge(runoff_stats, last_year_data, on=['code', 'decad_in_year'], how='left')
    runoff_stats = pd.merge(runoff_stats, current_year_data[['code', 'decad_in_year', str(current_year)]], on=['code', 'decad_in_year'], how='left')

    # Drop the column predictor if it is in runoff_stats
    if 'predictor' in runoff_stats.columns:
        runoff_stats = runoff_stats.drop(columns=['predictor'])

    # Round all values to 3 decimal places
    runoff_stats = runoff_stats.round(3)

    # Sort the DataFrame by 'code' and 'decad_in_year', using 'decad_in_year'
    # as numerical values
    runoff_stats['decad_in_year'] = runoff_stats['decad_in_year'].astype(int)
    runoff_stats = runoff_stats.sort_values(by=['code', 'decad_in_year'])

    # Get the path to the intermediate data folder from the environmental
    # variables and the name of the ieasyforecast_hydrograph_decad_file.
    # Concatenate them to the output file path.
    try:
        output_file_path = os.path.join(
            os.getenv("ieasyforecast_intermediate_data_path"),
            os.getenv("ieasyforecast_hydrograph_decad_file"))
    except Exception as e:
        logger.error("Could not get the output file path.")
        print(os.getenv("ieasyforecast_intermediate_data_path"))
        print(os.getenv("ieasyforecast_hydrograph_decad_file"))
        raise e

    # Overwrite the file if it already exists
    if os.path.exists(output_file_path):
        os.remove(output_file_path)

    # Write the data to a csv file. Raise an error if this does not work.
    # If the data is written to the csv file, log a message that the data
    # has been written.
    try:
        ret = runoff_stats.to_csv(output_file_path, index=False)
        logger.info(f"Data written to {output_file_path}.")
    except Exception as e:
        logger.error(f"Could not write the data to {output_file_path}.")
        raise e

    return ret

def write_pentad_time_series_data(data: pd.DataFrame):
    """
    Writes data to csv file for later reading into the forecast dashboard.

    Args:
    data (pd.DataFrame): The data to be written to a csv file.

    Returns:
    None
    """
    # Drop the rows where the issue dates are False
    data = data[data['issue_date'] == True]

    # Drop the issue_date column
    data = data.drop(columns=['issue_date', 'discharge'])

    # If there is a column called discharge_sum, rename it to predictor
    if 'discharge_sum' in data.columns:
        data = data.rename(columns={'discharge_sum': 'predictor'})

    # Round data in the discharge_avg and predictor columns to 3 decimal places
    data['discharge_avg'] = data['discharge_avg'].round(3)
    data['predictor'] = data['predictor'].round(3)

    # These runoff statistics are now written to the date of the forecast
    # production. For the hydrograph output, we want the date to reflect the
    # pentad, the data is collected for. Therefore, we add 1 day to the 'date'
    # column and recalculate pentad and pentad_in_year.
    # Calculate pentad and pentad_in_year
    data.loc[:, 'pentad'] = (data['date'] + pd.Timedelta(days=1)).apply(tl.get_pentad)
    data.loc[:, 'pentad_in_year'] = (data['date'] + pd.Timedelta(days=1)).apply(tl.get_pentad_in_year)

    # Get the path to the intermediate data folder from the environmental
    # variables and the name of the ieasyforecast_hydrograph_pentad_file.
    # Concatenate them to the output file path.
    try:
         output_file_path = os.path.join(
                os.getenv("ieasyforecast_intermediate_data_path"),
                os.getenv("ieasyforecast_pentad_discharge_file"))
    except Exception as e:
        logger.error("Could not get the output file path.")
        print(os.getenv("ieasyforecast_intermediate_data_path"))
        print(os.getenv("ieasyforecast_pentad_discharge_file"))
        raise e

    # Overwrite the file if it already exists
    if os.path.exists(output_file_path):
        os.remove(output_file_path)

    # Write the data to a csv file. Raise an error if this does not work.
    # If the data is written to the csv file, log a message that the data
    # has been written.
    try:
        ret = data.to_csv(output_file_path, index=False)
        logger.info(f"Data written to {output_file_path}.")
    except Exception as e:
        logger.error(f"Could not write the data to {output_file_path}.")
        raise e

    return ret

def write_decad_time_series_data(data: pd.DataFrame):
    """
    Writes data to csv file for later reading into the forecast dashboard.

    Args:
    data (pd.DataFrame): The data to be written to a csv file.

    Returns:
    None
    """
    # Drop the rows where the issue dates are False
    data = data[data['issue_date'] == True]

    # Drop the issue_date column
    data = data.drop(columns=['issue_date', 'discharge'])

    # If there is a column called discharge_sum, rename it to predictor
    if 'discharge_sum' in data.columns:
        data = data.rename(columns={'discharge_sum': 'predictor'})

    # Round data in the discharge_avg and predictor columns to 3 decimal places
    data['discharge_avg'] = data['discharge_avg'].round(3)
    data['predictor'] = data['predictor'].round(3)

    # These runoff statistics are now written to the date of the forecast
    # production. For the hydrograph output, we want the date to reflect the
    # decad, the data is collected for. Therefore, we add 1 day to the 'date'
    # column and recalculate decad and decad_in_year.
    # Add 1 day to the date column
    # Calculate decad and decad_in_year
    data.loc[:, 'decad_in_month'] = (data['date'] + pd.Timedelta(days=1)).apply(tl.get_decad_in_month)
    data.loc[:, 'decad_in_year'] = (data['date'] + pd.Timedelta(days=1)).apply(tl.get_decad_in_year)

    # Get the path to the intermediate data folder from the environmental
    # variables and the name of the ieasyforecast_hydrograph_pentad_file.
    # Concatenate them to the output file path.
    try:
         output_file_path = os.path.join(
                os.getenv("ieasyforecast_intermediate_data_path"),
                os.getenv("ieasyforecast_decad_discharge_file"))
    except Exception as e:
        logger.error("Could not get the output file path.")
        print(os.getenv("ieasyforecast_intermediate_data_path"))
        print(os.getenv("ieasyforecast_decad_discharge_file"))
        raise e

    # Overwrite the file if it already exists
    if os.path.exists(output_file_path):
        os.remove(output_file_path)

    # Write the data to a csv file. Raise an error if this does not work.
    # If the data is written to the csv file, log a message that the data
    # has been written.
    try:
        ret = data.to_csv(output_file_path, index=False)
        logger.info(f"Data written to {output_file_path}.")
    except Exception as e:
        logger.error(f"Could not write the data to {output_file_path}.")
        raise e

    return ret

def save_pentadal_skill_metrics(data: pd.DataFrame):
    """
    Saves pentadal skill metrics to a csv file.

    Args:
    data (pd.DataFrame): The data to be written to a csv file.

    Returns:
    None

    """

    # Round all values to 4 decimal places
    data = data.round(4)

    # convert pentad_in_year to int
    data['pentad_in_year'] = data['pentad_in_year'].astype(int)

    # Sort in ascending order by 'pentad_in_year', 'code', and 'model_short'
    data = data.sort_values(by=['pentad_in_year', 'code', 'model_short'])

    filepath = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_pentadal_skill_metrics_file"))

    # Overwrite the file if it already exists
    if os.path.exists(filepath):
        os.remove(filepath)

    # Write the data to a csv file. Raise an error if this does not work.
    # If the data is written to the csv file, log a message that the data
    # has been written.
    try:
        ret = data.to_csv(filepath, index=False)
        logger.info(f"Data written to {filepath}.")
    except Exception as e:
        logger.error(f"Could not write the data to {filepath}.")
        raise e

    return ret

def save_decadal_skill_metrics(data: pd.DataFrame):
    """
    Saves decadal skill metrics to a csv file.

    Args:
    data (pd.DataFrame): The data to be written to a csv file.

    Returns:
    None

    """

    # Round all values to 4 decimal places
    data = data.round(4)

    # convert decad_in_year to int
    data['decad_in_year'] = data['decad_in_year'].astype(int)

    # Sort in ascending order by 'decad_in_year', 'code', and 'model_short'
    data = data.sort_values(by=['decad_in_year', 'code', 'model_short'])

    filepath = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_decadal_skill_metrics_file"))

    # Overwrite the file if it already exists
    if os.path.exists(filepath):
        os.remove(filepath)

    # Write the data to a csv file. Raise an error if this does not work.
    # If the data is written to the csv file, log a message that the data
    # has been written.
    try:
        ret = data.to_csv(filepath, index=False)
        logger.info(f"Data written to {filepath}.")
    except Exception as e:
        logger.error(f"Could not write the data to {filepath}.")
        raise e

    return ret

def get_latest_forecasts(simulated_df, horizon_column_name='pentad_in_year'):
    """
    Extract the latest forecasts for each unique combination of code, pentad_in_year, and model_short.
    
    Args:
        simulated_df (pd.DataFrame): DataFrame containing forecast data with columns 'code', 
                                    <horizon_column_name>, 'model_short', 'date', and forecast values
        horizon_column_name (str): Name of the column that represents the forecast horizon.
                                    Default is 'pentad_in_year'.
    
    Returns:
        pd.DataFrame: DataFrame containing only the most recent forecast for each unique 
                     combination of code, pentad_in_year, and model_short
    """
    if simulated_df.empty:
        return pd.DataFrame()
        
    # Ensure date is in datetime format
    if not pd.api.types.is_datetime64_any_dtype(simulated_df['date']):
        simulated_df = simulated_df.copy()
        simulated_df['date'] = pd.to_datetime(simulated_df['date'])

    # Method 1: Using groupby and idxmax (most efficient for large dataframes)
    # idx = simulated_df.groupby(['code', horizon_column_name, 'model_short'])['date'].idxmax()
    # latest_forecasts = simulated_df.loc[idx]
    
    # Alternatively, Method 2: Using drop_duplicates (easier to read)
    # Sort by date in descending order first
    sorted_df = simulated_df.sort_values('date', ascending=False)
    latest_forecasts = sorted_df.drop_duplicates(
        subset=['code', horizon_column_name, 'model_short'], keep='first')
    
    # Only keep lines where date is equal to the maximum date
    latest_date = simulated_df['date'].max()
    # Write year into column, derived from date column
    latest_forecasts['year'] = latest_forecasts['date'].dt.year
    latest_forecasts = latest_forecasts[latest_forecasts['year'] == latest_date.year]

    # Drop the 'year' column
    latest_forecasts = latest_forecasts.drop(columns=['year'])

    # Round numeric columns to 3 decimal places
    numeric_cols = latest_forecasts.select_dtypes(include=['float64', 'float32']).columns
    latest_forecasts[numeric_cols] = latest_forecasts[numeric_cols].round(3)
    
    return latest_forecasts

def save_forecast_data_pentad(simulated: pd.DataFrame):
    """
    Save observed pentadal runoff and simulated pentadal runoff for different models to csv.

    Args:
    observed (pd.DataFrame): The DataFrame containing the observed data.
    simulated (pd.DataFrame): The DataFrame containing the simulated data.

    Returns:
    None
    """
    filename = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_combined_forecast_pentad_file"))

    # Only keep relevant columns
    #simulated = simulated[['code', 'date', 'pentad_in_month', 'pentad_in_year', 'forecasted_discharge', 'model_long', 'model_short']]

    # Round all float values to 3 decimal places
    simulated = simulated.round(3)

    # Remove " from model_long values
    simulated['model_long'] = simulated['model_long'].str.replace('"', '')

    # write the data to csv
    ret = simulated.to_csv(filename, index=False)

    # Select forecast of the latest date for each code, pentad_in_year, and model_short
    simulated_latest = get_latest_forecasts(simulated, horizon_column_name='pentad_in_year')
    
    # Edit filename by appending '_latest' to the filename
    filename_latest = filename.replace('.csv', '_latest.csv')

    # Write the latest data to a csv file
    ret = simulated_latest.to_csv(filename_latest, index=False)

    return ret

def save_forecast_data_decade(simulated: pd.DataFrame):
    """
    Save observed decadal runoff and simulated decadal runoff for different models to csv.

    Args:
    observed (pd.DataFrame): The DataFrame containing the observed data.
    simulated (pd.DataFrame): The DataFrame containing the simulated data.

    Returns:
    None
    """
    filename = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_combined_forecast_decad_file"))

    # Only keep relevant columns
    #simulated = simulated[['code', 'date', 'decad_in_month', 'decad_in_year', 'forecasted_discharge', 'model_long', 'model_short']]

    # Round all float values to 3 decimal places
    simulated = simulated.round(3)

    # Remove " from model_long values
    simulated['model_long'] = simulated['model_long'].str.replace('"', '')

    # write the data to csv
    ret = simulated.to_csv(filename, index=False)

    # Select forecast of the latest date for each code, decad_in_year, and model_short
    simulated_latest = get_latest_forecasts(simulated, horizon_column_name='decad_in_year')

    # Edit filename by appending '_latest' to the filename
    filename_latest = filename.replace('.csv', '_latest.csv')

    # Write the latest data to a csv file
    ret = simulated_latest.to_csv(filename_latest, index=False)

    return ret

# endregion


# === Forecast classes ===
# region Class definitions
class Site:
    """
    Represents a site for which discharge forecasts are produced.

    Attributes:
        code (str): The site code.
        name (str): The site name (a combination of river_name and river_punkt).
        river_name (str): The name of the river that the site is located on.
        punkt_name (str): The name of the location within the river where the site is located.
        lat (float): The latitude of the site in WSG 84.
        lon (float): The longitude of the site in WSG 84.
        region (str): The region that the site is located in (typically oblast).
        basin (str): The basin that the site is located in.
        precictor (float): The predictor value for the site.
        fcp_qmin (str): The lower bound of the discharge forecasted for the next pentad.
        fcp_qmax (str): The upper bound of the discharge forecasted for the next pentad.
        fcp_qexp (str): The expected discharge forecasted for the next pentad.
        qnorm_pentad (str): The norm discharge for the site.
        qdanger (str): The threshold discharge for a dangerous flood.
        slope (float): The slope of the linear regression.
        intercept (float): The intercept of the linear regression.
        delta (float): 0.674 times the standard deviation of the observation data.

    Methods:
        __repr__(): Returns a string representation of the Site object.
        from_dataframe(df: pd.DataFrame) -> list: Creates a list of Site objects from a DataFrame.
        from_df_calculate_forecast(site, pentad: str, df): Calculate forecast from slope and intercept in the DataFrame.


    Required DataFrame columns:
        - site_code (str): The site code.
        - site_name (str): The site name (a combination of river_name and river_punkt).
        - river_ru (str): The name of the river that the site is located on.
        - punkt_ru (str): The name of the location within the river where the site is located.
        - latitude (float): The latitude of the site in WSG 84.
        - longitude (float): The longitude of the site in WSG 84.
        - region (str): The region that the site is located in (typically oblast).
        - basin (str): The basin that the site is located in.
    """
    def __init__(self, code: str, name="Name", name_nat="Name_nat",
                 river_name="River", river_name_nat="River_nat", punkt_name="Punkt",
                 punkt_name_nat="Punkt_nat", lat=0.0, lon=0.0,
                 region="Region", region_nat="Region_nat",
                 basin="Basin", basin_nat="Basin_nat",
                 predictor=-10000.0, fc_qmin=-10000.0,
                 fc_qmax=-10000.0, fc_qexp=-10000.0, qnorm=-10000.0,
                 qmin=-10000.0, qmax=-10000.0,
                 perc_norm=-10000.0, qdanger=-10000.0, slope=-10000.0,
                 intercept=-10000.0, rsquared=-10000.0,
                 delta=-10000.0, sdivsigma=-10000.0,
                 accuracy=-10000.0, histqmin=-10000.0, histqmax=-10000.0,
                 bulletin_order=0,
                 daily_forecast=False, pentadal_forecast=False, decadal_forecast=False,
                 monthly_forecast=False, seasonal_forecast=False,
                 site_type="default"):
        """
        Initializes a new Site object.

        Args:
            code (str): The site code.
            name (str): The site name (a combination of river_name and river_punkt).
            name_nat (str): The site name in national language.
            river_name (str): The name of the river that the site is located on.
            river_name_nat (str): The name of the river in national language.
            punkt_name (str): The name of the location within the river where the site is located.
            punkt_name_nat (str): The name of the location in national language.
            lat (float): The latitude of the site in WSG 84.
            lon (float): The longitude of the site in WSG 84.
            region (str): The region that the site is located in (typically oblast).
            region_nat (str): The region in national language.
            basin (str): The basin that the site is located in.
            basin_nat (str): The basin in national language.
            precictor (float): The predictor value for the site.
            fc_qmin (float): The lower bound of the discharge forecasted for the next pentad.
            fc_qmax (float): The upper bound of the discharge forecasted for the next pentad.
            fc_qexp (float): The expected discharge forecasted for the next pentad.
            qnorm (float): The norm discharge for the site.
            qmin (float): The minimum discharge for the site.
            qmax (float): The maximum discharge for the site.
            qdanger (str): The threshold discharge for a dangerous flood.
        """
        # Static attributes
        self.code = code
        self.name = name if name is not None else "Name"
        self.name_nat = name if name is not None else "Name_nat"
        self.river_name = river_name if river_name is not None else "River"
        self.river_name_nat = river_name_nat if river_name_nat is not None else "River_nat"
        self.punkt_name = punkt_name if punkt_name is not None else "Punkt"
        self.punkt_name_nat = punkt_name_nat if punkt_name_nat is not None else "Punkt_nat"
        self.lat = lat if lat is not None else 0.0
        self.lon = lon if lon is not None else 0.0
        self.region = region if region is not None else "Region"
        self.region_nat = region_nat if region_nat is not None else "Region_nat"
        self.basin = basin if basin is not None else "Basin"
        self.basin_nat = basin_nat if basin_nat is not None else "Basin_nat"
        self.qdanger = qdanger if qdanger is not None else -10000.0
        self.histqmin = histqmin if histqmin is not None else -10000.0
        self.histqmax = histqmax if histqmax is not None else -10000.0
        self.bulletin_order = bulletin_order if bulletin_order is not None else 0
        self.daily_forecast = daily_forecast if daily_forecast is not False else False
        self.pentadal_forecast = pentadal_forecast if pentadal_forecast is not False else False
        self.decadal_forecast = decadal_forecast if decadal_forecast is not False else False
        self.monthlly_forecast = monthly_forecast if monthly_forecast is not False else False
        self.seasonal_forecast = seasonal_forecast if seasonal_forecast is not False else False
        self.site_type = site_type if site_type is not None else "default"
        # Dynamic attributes
        self.predictor = predictor if predictor is not None else -10000.0
        self.fc_qmin = fc_qmin if fc_qmin is not None else -10000.0
        self.fc_qmax = fc_qmax if fc_qmax is not None else -10000.0
        self.fc_qexp = fc_qexp if fc_qexp is not None else -10000.0
        self.qnorm = qnorm if qnorm is not None else -10000.0
        self.qmin = qmin if qmin is not None else -10000.0
        self.qmax = qmax if qmax is not None else -10000.0
        self.perc_norm = perc_norm if perc_norm is not None else -10000.0
        self.slope = slope if slope is not None else -10000.0
        self.intercept = intercept if intercept is not None else -10000.0
        self.rsquared = rsquared if rsquared is not None else -10000.0
        self.delta = delta if delta is not None else -10000.0
        self.sdivsigma = sdivsigma if sdivsigma is not None else -10000.0
        self.accuracy = accuracy if accuracy is not None else -10000.0

    def __repr__(self):
        """
        Returns a string representation of the Site object.

        Returns:
            str: The site code and all other attributes row-by-row.
        """
        return (f"Site(\n"
            f"code={self.code},\n"
            f"name={self.name},\n"
            f"river_name={self.river_name},\n"
            f"punkt_name={self.punkt_name},\n"
            f"lat={self.lat},\n"
            f"lon={self.lon},\n"
            f"region={self.region},\n"
            f"basin={self.basin},\n"
            f"predictor={self.predictor},\n"
            f"fc_qmin={self.fc_qmin},\n"
            f"fc_qmax={self.fc_qmax},\n"
            f"fc_qexp={self.fc_qexp},\n"
            f"qnorm={self.qnorm},\n"
            f"qmin={self.qmin},\n"
            f"qmax={self.qmax},\n"
            f"perc_norm={self.perc_norm},\n"
            f"qdanger={self.qdanger},\n"
            f"slope={self.slope},\n"
            f"intercept={self.intercept},\n"
            f"delta={self.delta}\n"
            f"sdivsigma={self.sdivsigma}\n"
            f"accuracy={self.accuracy}\n"
            f")")

    @classmethod
    def from_df_calculate_forecast(cls, site, group_id: str, df: pd.DataFrame,
                                   code_col='code', group_col='pentad_in_year'):
        '''
        Calculate forecast from slope and intercept in the DataFrame.

        Args:
            site (Site): The site object to calculate forecast for.
            group_id (str): For which value to calculate the forecast for. For example the pentad of the year to calculate forecast for.
            df (pd.DataFrame): The DataFrame containing the slope and intercept data.
            code_col (str): The name of the column containing the site code.
            group_col (str): The name of the column containing the group identifier. Typcally pentad_in_year or decad_in_year.

        Returns:
            qpexpd (str): The expected discharge forecasted for the next pentad.
        '''
        try:
            # Test that df contains columns required
            if not all(column in df.columns for column in [code_col, group_col]):
                raise ValueError(f'DataFrame is missing one or more required columns: {code_col, group_col, "slope", "intercept"}')

            # Convert group_id to float
            group_id = float(group_id)

            # Get the slope and intercept for the site
            slope = df[(df[code_col] == site.code) & (df[group_col] == group_id)]['slope'].values[0]
            intercept = df[(df[code_col] == site.code) & (df[group_col] == group_id)]['intercept'].values[0]

            # Write slope and intercept to site
            site.slope = round(slope, 5)
            site.intercept = round(intercept, 5)

            # Calculate the expected discharge forecasted for the next pentad
            qpexpd = slope * float(site.predictor) + intercept

            # What happens if qpexpd is negative? We assign 0 discharge.
            if qpexpd < 0.0:
                qpexpd = 0.0

            # Write the expected discharge forecasted for the next pentad to self.fc_qexp
            site.fc_qexp = round_discharge(qpexpd)

            # Return the expected discharge forecasted for the next pentad
            return qpexpd
        except ValueError as e:
            print(e)
            return None
        except Exception:
            print(f'Note: No slope and intercept for site {site.code} in DataFrame. Returning None.')
            return None

    @classmethod
    def from_df_calculate_forecast_pentad(cls, site, pentad: str, df: pd.DataFrame):
        '''
        Calculate forecast from slope and intercept in the DataFrame.

        Args:
            site (Site): The site object to calculate forecast for.
            pentad (str): The pentad of the year to calculate forecast for.
            df (pd.DataFrame): The DataFrame containing the slope and intercept data.

        Returns:
            qpexpd (str): The expected discharge forecasted for the next pentad.
        '''
        try:
            # Test that df contains columns 'Code' and 'pentad'
            if not all(column in df.columns for column in ['Code', 'pentad_in_year', 'slope', 'intercept']):
                raise ValueError(f'DataFrame is missing one or more required columns: {"Code", "pentad_in_year", "slope", "intercept"}')

            # Convert pentad to float
            pentad = float(pentad)

            # Get the slope and intercept for the site
            slope = df[(df['Code'] == site.code) & (df['pentad_in_year'] == pentad)]['slope'].values[0]
            intercept = df[(df['Code'] == site.code) & (df['pentad_in_year'] == pentad)]['intercept'].values[0]

            # Write slope and intercept to site
            site.slope = round(slope, 5)
            site.intercept = round(intercept, 5)

            # Calculate the expected discharge forecasted for the next pentad
            qpexpd = slope * float(site.predictor) + intercept

            # What happens if qpexpd is negative? We assign 0 discharge.
            if qpexpd < 0.0:
                qpexpd = 0.0

            # Write the expected discharge forecasted for the next pentad to self.fc_qexp
            site.fc_qexp = round_discharge(qpexpd)

            # Return the expected discharge forecasted for the next pentad
            return qpexpd
        except ValueError as e:
            print(e)
            return None
        except Exception:
            print(f'Note: No slope and intercept for site {site.code} in DataFrame. Returning None.')
            return None

    @classmethod
    def from_df_calculate_forecast_decad(cls, site, decad: str, df: pd.DataFrame):
        '''
        Calculate forecast from slope and intercept in the DataFrame.

        Args:
            site (Site): The site object to calculate forecast for.
            decad (str): The decad of the year to calculate forecast for.
            df (pd.DataFrame): The DataFrame containing the slope and intercept data.

        Returns:
            qpexpd (str): The expected discharge forecasted for the next pentad.
        '''
        try:
            # Test that df contains columns 'Code' and 'pentad'
            if not all(column in df.columns for column in ['Code', 'decad_in_year', 'slope', 'intercept']):
                raise ValueError(f'DataFrame is missing one or more required columns: {"Code", "decad_in_year", "slope", "intercept"}')

            # Convert pentad to float
            pentad = float(decad)

            # Get the slope and intercept for the site
            slope = df[(df['Code'] == site.code) & (df['decad_in_year'] == pentad)]['slope'].values[0]
            intercept = df[(df['Code'] == site.code) & (df['decad_in_year'] == pentad)]['intercept'].values[0]

            # Write slope and intercept to site
            site.slope = round(slope, 5)
            site.intercept = round(intercept, 5)

            # Calculate the expected discharge forecasted for the next pentad
            qpexpd = slope * float(site.predictor) + intercept

            # What happens if qpexpd is negative? We assign 0 discharge.
            if qpexpd < 0.0:
                qpexpd = 0.0

            # Write the expected discharge forecasted for the next pentad to self.fc_qexp
            site.fc_qexp = round_discharge(qpexpd)

            # Return the expected discharge forecasted for the next pentad
            return qpexpd
        except ValueError as e:
            print(e)
            return None
        except Exception:
            print(f'Note: No slope and intercept for site {site.code} in DataFrame. Returning None.')
            return None

    @classmethod
    def calculate_percentages_norm(cls, site):
        '''
        From the norm discharge and the expected discharge, calculate the percentage of the norm discharge.

        Args:
            site (Site): The site object to calculate the percentage for.

        Returns:
            str: The percentage of the norm discharge.
        '''
        try:
            perc_norm = float(site.fc_qexp) / float(site.qnorm) * 100
            # print(f'perc_norm: {perc_norm}, site.fc_qexp: {site.fc_qexp}, site.qnorm: {site.qnorm}')

            if perc_norm < 0.0 or perc_norm > 500.0:
                site.perc_norm = " "
            elif perc_norm == 0.0:
                site.perc_norm = "0"
            else:
                site.perc_norm = str(round(perc_norm))
        except Exception as e:
            print(e)
            site.perc_norm = " "

    @classmethod
    def from_df_get_norm_discharge(cls, site, group_id: str, df: pd.DataFrame, df_min: pd.DataFrame, df_max: pd.DataFrame,
                                   code_col='code', group_col='pentad_in_year', value_col='discharge_avg'):
        '''
        Get norm discharge from DataFrame. I.e. for a given group_id, calculate
        the average over all values where group_col == group_id.

        Example for group_id = '1', the function calculates the average over all
        values where group_col == 1.

        Args:
            site (Site): The site object to get norm discharge for.
            group_id (str that can be converted to a number): The id to get norm discharge for. Typically the pentad of the year or the decad of the year
            df (pd.DataFrame): The DataFrame containing the norm discharge data.
            df_min (pd.DataFrame): The DataFrame containing the minimum discharge data.
            df_max (pd.DataFrame): The DataFrame containing the maximum discharge data.
            code_col (str): The column name of the site code. Default is 'code'.
            group_col (str that can be converted to a number): The column name of the pentad. Default is 'pentad_in_year' but can also be 'decad_in_year'.
            value_col (float): The column name of the discharge value. Default is 'discharge_avg'.

        Returns:
            str: The norm discharge value.

        Raises:
            ValueError: If the DataFrame is missing one or more required columns.
        '''
        try:
            # Test that df contains columns required
            if not all(column in df.columns for column in [code_col, group_col, value_col]):
                raise ValueError(f'DataFrame is missing one or more required columns: {code_col}, {group_col}, {value_col}')

            # Convert pentad to float
            group_id = float(group_id)

            # Also convert the column group_col to float
            df[group_col] = df[group_col].astype(float)
            df_min[group_col] = df_min[group_col].astype(float)
            df_max[group_col] = df_max[group_col].astype(float)

            # Get the norm discharge for the site
            qnorm = df[(df[code_col] == site.code) & (df[group_col] == group_id)][value_col].values[0]
            qmin = df_min[(df_min[code_col] == site.code) & (df_min[group_col] == group_id)][value_col].values[0]
            qmax = df_max[(df_max[code_col] == site.code) & (df_max[group_col] == group_id)][value_col].values[0]

            # Write the norm discharge value to self.qnorm as string
            site.qnorm = round_discharge(qnorm)
            site.qmin = round_discharge(qmin)
            site.qmax = round_discharge(qmax)

            # Return the norm discharge value
            return qnorm
        except Exception as e:
            print(f'Error {e}. Returning " ".')
            return " "

    @classmethod
    def from_df_get_norm_discharge_decad(cls, site, decad_in_year: str, df: pd.DataFrame,
                                         df_min: pd.DataFrame, df_max: pd.DataFrame):
        '''
        Get norm discharge from DataFrame.

        Args:
            site (Site): The site object to get norm discharge for.
            decad_in_year (str): The pentad of the year to get norm discharge for.
            df (pd.DataFrame): The DataFrame containing the norm discharge data.
            df_min (pd.DataFrame): The DataFrame containing the minimum discharge data.
            df_max (pd.DataFrame): The DataFrame containing the maximum discharge data.

        Returns:
            str: The norm discharge value.
        '''
        try:
            # Test that df contains columns 'Code' and 'pentad_in_year'
            if not all(column in df.columns for column in ['Code', 'decad_in_year']):
                raise ValueError(f'DataFrame is missing one or more required columns: {"Code", "decad_in_year"}')

            # Convert pentad to float
            decad_in_year = float(decad_in_year)

            # Also convert the column 'pentad_in_year' to float
            df['decad_in_year'] = df['decad_in_year'].astype(float)
            df_min['decad_in_year'] = df_min['decad_in_year'].astype(float)
            df_max['decad_in_year'] = df_max['decad_in_year'].astype(float)

            # Get the norm discharge for the site
            qnorm = df[(df['Code'] == site.code) & (df['decad_in_year'] == decad_in_year)]['discharge_avg'].values[0]
            qmin = df_min[(df_min['Code'] == site.code) & (df_min['decad_in_year'] == decad_in_year)]['discharge_avg'].values[0]
            qmax = df_max[(df_max['Code'] == site.code) & (df_max['decad_in_year'] == decad_in_year)]['discharge_avg'].values[0]

            # Write the norm discharge value to self.qnorm as string
            site.qnorm = round_discharge(qnorm)
            site.qmin = round_discharge(qmin)
            site.qmax = round_discharge(qmax)

            # Return the norm discharge value
            return qnorm
        except Exception as e:
            print(f'Error {e}. Returning " ".')
            return " "

    @classmethod
    def from_df_get_predictor(cls, site, df: pd.DataFrame, predictor_dates,
                                     date_col='date', code_col='code',
                                     predictor_col=None):
        '''
        Calculate predictor from df.

        Args:
            site (Site): The site object to get predictor for.
            df (pd.DataFrame): The DataFrame containing the predictor data.
            predictor_dates (list): dates for which to collect the predictor data.
            date_col (str): The column name of the date column in the DataFrame. Default is 'date'.
            code_col (str): The column name of the site code column in the DataFrame. Default is 'code'.
            predictor_col (str): The column name of the predictor column in the DataFrame. Default is None.

        Returns:
            float: The predictor for the current pentad.
        '''

        try:
            # Convert predictor_dates to date sting
            predictor_dates = predictor_dates[0].strftime('%Y-%m-%d')

            # Convert 'Date' column of df to datetime format
            df_copy = df.copy()
            df_copy.loc[:, date_col] = pd.to_datetime(df_copy[date_col]).dt.strftime('%Y-%m-%d')

            # Test that df contains columns code_col predictor_col and date_col
            if not all(column in df.columns for column in [code_col, predictor_col, date_col]):
                raise ValueError(f'DataFrame is missing one or more required columns: {code_col}, {predictor_col}, {date_col}')

            # Get the predictor for the site
            predictor = df_copy[(df_copy[code_col] == site.code) & (df_copy[date_col] == predictor_dates)][predictor_col].mean(skipna=True)

            # Note: Should the predictor be a negative number, we cannot make a
            # forecast. round_discharge will assign an empty string " ".

            # Write the predictor value to self.predictor
            site.predictor = round_discharge_to_float(predictor)

            # Return the predictor value
            return predictor
        except Exception as e:
            print(f'Error {e}. Returning None.')
            return None

    @classmethod
    def from_df_get_predictor_decad(cls, site, df: pd.DataFrame, predictor_dates):
        '''
        Calculate predictor from df.

        Args:
            site (Site): The site object to get predictor for.
            df (pd.DataFrame): The DataFrame containing the predictor data.
            predictor_dates (list): dates for which to collect the predictor data.

        Returns:
            float: The predictor for the current pentad.
        '''
        try:
            # Convert predictor_dates to datetime format
            predictor_dates = pd.to_datetime(predictor_dates)

            # Convert 'Date' column of df to datetime format
            df.loc[:, 'Date'] = pd.to_datetime(df['Date'])

            # Test that df contains columns 'Code' 'predictor' and 'Date'
            if not all(column in df.columns for column in ['Code', 'predictor', 'Date']):
                raise ValueError(f'DataFrame is missing one or more required columns: {"Code", "predictor", "Date"}')

            # Get the predictor for the site
            predictor = df[(df['Code'] == site.code) & (df['Date'].isin(predictor_dates))]['predictor'].mean(skipna=True)

            # Note: Should the predictor be a negative number, we cannot make a
            # forecast. round_discharge will assign an empty string " ".

            # Write the predictor value to self.predictor
            site.predictor = round_discharge_to_float(predictor)

            # Return the predictor value
            return predictor
        except Exception as e:
            print(f'Error {e}. Returning None.')
            return None

    @classmethod
    def from_df_get_qrange_discharge(cls, site, pentad: str, df: pd.DataFrame):
        '''
        Get qpmin & qpmax discharge from DataFrame.

        Args:
            site (Site): The site object to get norm discharge for.
            pentad (str): The pentad to get norm discharge for.
            df (pd.DataFrame): The DataFrame containing the norm discharge data.

        Returns:
            str: The lower and upper ranges for the discharge forecast.
        '''
        # Test that df contains columns 'Code' and 'pentad'
        if not all(column in df.columns for column in ['Code', 'pentad_in_year']):
            raise ValueError(f'DataFrame is missing one or more required columns: {"Code", "pentad_in_year"}')

            # Convert pentad to float
        pentad = float(pentad)

        # Convert 'pentad_in_year' column of df to float
        df['pentad_in_year'] = df['pentad_in_year'].astype(float)

        # Get the discharge ranges for the site
        delta = df[(df['Code'] == site.code) & (df['pentad_in_year'] == pentad)]['observation_std0674'].values[0]
        sdivsigma = df[(df['Code'] == site.code) & (df['pentad_in_year'] == pentad)]['sdivsigma'].values[0]
        accuracy = df[(df['Code'] == site.code) & (df['pentad_in_year'] == pentad)]['accuracy'].values[0]
        abserr = df[(df['Code'] == site.code) & (df['pentad_in_year'] == pentad)]['absolute_error'].values[0]

        qpmin = float(site.fc_qexp) - delta
        qpmax = float(site.fc_qexp) + delta

        site.delta = round(delta, 5)

        # Make sure none of the boundary values are negative.
        if qpmin < 0.0:
            qpmin = 0.0
            # qpmax really should never be negative, but just in case.
        if qpmax < 0.0:
            qpmax = 0.0

            # Test if both qpmin and qpmax are 0.0 then return " "
        if qpmin == 0.0 and qpmax == 0.0:
            site.fc_qmin = " "
            site.fc_qmax = " "

        else:
            # Write the lower and upper bound of the discharge forecast to
            # Site.fc_qmin and Site.fc_qmax.
            site.fc_qmin = round_discharge(qpmin)  # -> string
            site.fc_qmax = round_discharge(qpmax)  # -> string

        # Also assign sdivsigma and accuracy to site.
        site.sdivsigma = f'{sdivsigma}'
        site.accuracy = f'{accuracy}'
        site.abserr = f'{abserr}'
        #print(site.fc_qmin, site.fc_qmax)

        # Return the norm discharge value
        return qpmin, qpmax

    @classmethod
    def from_df_get_qrange_discharge_decad(cls, site, decad: str, df: pd.DataFrame):
        '''
        Get qpmin & qpmax discharge from DataFrame.

        Args:
            site (Site): The site object to get norm discharge for.
            decad (str): The decad to get norm discharge for.
            df (pd.DataFrame): The DataFrame containing the norm discharge data.

        Returns:
            str: The lower and upper ranges for the discharge forecast.
        '''
        # Test that df contains columns 'Code' and 'pentad'
        if not all(column in df.columns for column in ['Code', 'decad_in_year']):
            raise ValueError(f'DataFrame is missing one or more required columns: {"Code", "decad_in_year"}')

        # Convert decad to float
        decad = float(decad)

        # Convert 'decad_in_year' column of df to float
        df['decad_in_year'] = df['decad_in_year'].astype(float)

        # Get the discharge ranges for the site
        delta = df[(df['Code'] == site.code) & (df['decad_in_year'] == decad)]['observation_std0674'].values[0]
        sdivsigma = df[(df['Code'] == site.code) & (df['decad_in_year'] == decad)]['sdivsigma'].values[0]
        accuracy = df[(df['Code'] == site.code) & (df['decad_in_year'] == decad)]['accuracy'].values[0]
        abserr = df[(df['Code'] == site.code) & (df['decad_in_year'] == decad)]['absolute_error'].values[0]

        qpmin = float(site.fc_qexp) - delta
        qpmax = float(site.fc_qexp) + delta

        site.delta = round(delta, 5)

        # Make sure none of the boundary values are negative.
        if qpmin < 0.0:
            qpmin = 0.0
            # qpmax really should never be negative, but just in case.
        if qpmax < 0.0:
            qpmax = 0.0

            # Test if both qpmin and qpmax are 0.0 then return " "
        if qpmin == 0.0 and qpmax == 0.0:
            site.fc_qmin = " "
            site.fc_qmax = " "

        else:
            # Write the lower and upper bound of the discharge forecast to
            # Site.fc_qmin and Site.fc_qmax.
            site.fc_qmin = round_discharge_trad_bulletin(qpmin)  # -> string
            site.fc_qmax = round_discharge_trad_bulletin(qpmax)  # -> string

        # Also assign sdivsigma and accuracy to site
        site.sdivsigma = f'{sdivsigma}'
        site.accuracy = f'{accuracy}'
        site.abserr = f'{abserr}'

        #print(site.fc_qmin, site.fc_qmax)

        # Return the norm discharge value
        return qpmin, qpmax

    @classmethod
    def from_DB_get_dangerous_discharge(cls, sdk, site):
        '''
        Get dangerous discharge from DB.

        The DB connection hast to be established using:
        load_dotenv()
        ieh_sdk = IEasyHydroSDK()
        in the main code.

        Args:
            sdk (fl.SDK): The SDK connection object set up by calling
            site (Site): The site object to get dangerous discharge for.

        Returns:
            str: The dangerous discharge value.
        '''
        try:
            # Get the dangerous discharge for the site
            dangerous_discharge = sdk.get_data_values_for_site(
                site.code, 'dangerous_discharge')['data_values'][0]['data_value']

            # Write the dangerous discharge value to self.qdanger
            q = round_discharge(dangerous_discharge)

            site.qdanger = str(q)

            # Return the dangerous discharge value
            return dangerous_discharge
        except Exception:
            logger.debug(f'    Note: No dangerous discharge for site {site.code} in DB. Returning " ".')
            site.qdanger = " "
            return " "

    @classmethod
    def from_DB_get_predictor_sum(cls, sdk, site, dates, lagdays=20):
        '''
        Calculate predictor from data retrieved from the data base.

        The DB connection hast to be established using:
        load_dotenv()
        ieh_sdk = IEasyHydroSDK()
        in the main code.

        Details:
            The function retrieves sub-daily discharge data from the database
            and sums it up to get the predictor for the current pentad.
            The pentadal predictor for the pentad starting today + 1 day is
            calculated as the sum of the average daily discharge of today - 2
            days plus the average daily discharge of today - 1 day plus the
            morning discharge of today.

        Args:
            sdk (fl.SDK): The SDK connection object set up by calling
            site (Site): The site object to get dangerous discharge for.
            dates (list): A list of dates to get the predictor for.
            lagdays (int): The number of days to go back to retrieve data.

        Returns:
            float: The predictor for the current pentad.
        '''
        try:
            # Test that dates is a list of dates
            if not all(isinstance(date, dt.date) for date in dates):
                raise ValueError('Dates is not a list of dates')

            L = len(dates)

            # Define the date filter for the data request
            filters = BasicDataValueFilters(
                local_date_time__gte=min(dates),
                local_date_time__lte=max(dates)-dt.timedelta(days=1)
            )
            print(f'Reading data from site {site.code} with date range from {filters["local_date_time__gte"]} to {filters["local_date_time__lte"]}.')

            predictor_discharge = sdk.get_data_values_for_site(
                site.code,
                'discharge_daily_average',
                filters=filters)

            # Test if we have data or if some of the data is smaller than 0
            # Increase the number of lag days to go back for averaging if any of
            # the above is true
            if not predictor_discharge or any(d['data_value'] < 0 for d in predictor_discharge['data_values']):
                # go back up to 20 days to retrieve data
                counter = 0
                while (not predictor_discharge and counter < lagdays):
                    filters = BasicDataValueFilters(
                        local_date_time__gte=filters['local_date_time__gte'] - dt.timedelta(days=1),
                        local_date_time__lte=filters['local_date_time__lte']
                    )
                    predictor_discharge = sdk.get_data_values_for_site(
                        site.code,
                        'discharge_daily_average',
                        filters=filters)
                    counter += 1
                    print(f'Note: Not enough data retrieved from DB. New date range from {filters["local_date_time__gte"]} to {filters["local_date_time__lte"]}.')

                # Test if we have data now
                if not predictor_discharge:
                    print(f'No recent data for site {site.code} in DB. No forecast available.')
                    return None

            predictor_discharge = predictor_discharge['data_values']

            """ this overwrites existing predictor discharge. commented out for now.
            # Check if we have enough data
            if len(predictor_discharge) < L:
                counter = 0
                while (len(predictor_discharge) < L and counter < lagdays):
                    filters = BasicDataValueFilters(
                        local_date_time__gte=filters['local_date_time__gte'] - dt.timedelta(days=1),
                        local_date_time__lte=filters['local_date_time__lte']
                    )
                    predictor_discharge = sdk.get_data_values_for_site(
                        site.code,
                        'discharge_daily_average',
                        filters=filters)
                    counter += 1
                    print(f'Note: Not enough data retrieved from DB. New date range from {filters["local_date_time__gte"]} to {filters["local_date_time__lte"]}.')
                    predictor_discharge = predictor_discharge['data_values']
            """

            morning_filters = BasicDataValueFilters(
                local_date_time__gte=max(dates)-dt.timedelta(days=1),
                local_date_time__lte=max(dates))
            # Also get todays mornign discharge
            morning_discharge = sdk.get_data_values_for_site(
                site.code,
                'discharge_daily',
                filters=morning_filters)

            # Test if morning discharge is empty
            if not morning_discharge:
                print(f'No morning discharge data for site {site.code} in DB. Assuming yesterdays discharge.')
                # Get the row with the highest date from predictor discharge
                morning_discharge = predictor_discharge.tail(1)
            else:
                # Only keep the lastest row
                morning_discharge = pd.DataFrame(morning_discharge['data_values']).tail(1)

            # Make sure morning_discharge is a dataframe
            morning_discharge = pd.DataFrame(morning_discharge)

            # Create a DataFrame from the predictor_discharge list
            df = pd.DataFrame(predictor_discharge)

            # Convert values smaller than 0 to NaN
            df.loc[df['data_value'] < 0, 'data_value'] = np.nan

            # Add the morning discharge to the DataFrame
            df = pd.concat([df, morning_discharge])

            # Round the data_values to 3 digits. That is, if a value is
            # 0.123456789, it will be rounded to 0.123 and if a value is 123.456789
            # it will be rounded to 123.0
            df['data_value'] = df['data_value'].apply(round_discharge_to_float)

            print("\n\nDEBUG: Site: ", site.code)
            print("DEBUG: DB data for predictor discharge:\n", df)

            # If we still have missing data, we interpolate the existing data.
            # We take the average of the existing data to fill the gaps.
            if (len(df) < L):
                # Get the average of the existing data
                length = np.sum(~np.isnan(df['data_value']))
                q_avg = np.nansum(df['data_value']) / length

                df.loc[len(df)] = pd.DataFrame({
                    'data_value': q_avg,
                    'local_date_time': None,
                    'utc_date_time': None})

            # Sum the discharge over the past L days
            q = np.nansum(df['data_value'])

            if q < 0.0:
                q == np.nan
            site.predictor = q

            # Return the dangerous discharge value
            return q
        except Exception as e:
            print(f'Exception {e}')
            print(f'Note: No daily discharge data for site {site.code} in DB. Returning None.')
            return None

    @classmethod
    def from_DB_get_predictor_mean(cls, sdk, site, dates, lagdays=20):
        '''
        Calculate predictor from data retrieved from the data base.

        The DB connection hast to be established using:
        load_dotenv()
        ieh_sdk = IEasyHydroSDK()
        in the main code.

        Details:
            The function retrieves sub-daily discharge data from the database
            and sums it up to get the predictor for the current pentad.
            The pentadal predictor for the pentad starting today + 1 day is
            calculated as the sum of the average daily discharge of today - 2
            days plus the average daily discharge of today - 1 day plus the
            morning discharge of today.

        Args:
            sdk (fl.SDK): The SDK connection object set up by calling
            site (Site): The site object to get dangerous discharge for.
            dates (list): A list of dates to get the predictor for.
            lagdays (int): The number of days to go back to retrieve data.

        Returns:
            float: The predictor for the current pentad.
        '''
        try:
            # Test that dates is a list of dates
            if not all(isinstance(date, dt.date) for date in dates):
                raise ValueError('Dates is not a list of dates')

            L = len(dates)

            # Define the date filter for the data request
            filters = BasicDataValueFilters(
                local_date_time__gte=min(dates),
                local_date_time__lte=max(dates)-dt.timedelta(days=1)
            )
            print(f'Reading data from site {site.code} with date range from {filters["local_date_time__gte"]} to {filters["local_date_time__lte"]}.')

            predictor_discharge = sdk.get_data_values_for_site(
                site.code,
                'discharge_daily_average',
                filters=filters)

            # Test if we have data or if some of the data is smaller than 0
            # Increase the number of lag days to go back for averaging if any of
            # the above is true
            if not predictor_discharge or any(d['data_value'] < 0 for d in predictor_discharge['data_values']):
                # go back up to 20 days to retrieve data
                counter = 0
                while (not predictor_discharge and counter < lagdays):
                    filters = BasicDataValueFilters(
                        local_date_time__gte=filters['local_date_time__gte'] - dt.timedelta(days=1),
                        local_date_time__lte=filters['local_date_time__lte']
                    )
                    predictor_discharge = sdk.get_data_values_for_site(
                        site.code,
                        'discharge_daily_average',
                        filters=filters)
                    counter += 1
                    print(f'Note: Not enough data retrieved from DB. New date range from {filters["local_date_time__gte"]} to {filters["local_date_time__lte"]}.')

                # Test if we have data now
                if not predictor_discharge:
                    print(f'No recent data for site {site.code} in DB. No forecast available.')
                    return None

            predictor_discharge = predictor_discharge['data_values']

            """ this overwrites existing predictor discharge. commented out for now.
            # Check if we have enough data
            if len(predictor_discharge) < L:
                counter = 0
                while (len(predictor_discharge) < L and counter < lagdays):
                    filters = BasicDataValueFilters(
                        local_date_time__gte=filters['local_date_time__gte'] - dt.timedelta(days=1),
                        local_date_time__lte=filters['local_date_time__lte']
                    )
                    predictor_discharge = sdk.get_data_values_for_site(
                        site.code,
                        'discharge_daily_average',
                        filters=filters)
                    counter += 1
                    print(f'Note: Not enough data retrieved from DB. New date range from {filters["local_date_time__gte"]} to {filters["local_date_time__lte"]}.')
                    predictor_discharge = predictor_discharge['data_values']
            """

            morning_filters = BasicDataValueFilters(
                local_date_time__gte=max(dates)-dt.timedelta(days=1),
                local_date_time__lte=max(dates))
            # Also get todays mornign discharge
            morning_discharge = sdk.get_data_values_for_site(
                site.code,
                'discharge_daily',
                filters=morning_filters)

            # Test if morning discharge is empty
            if not morning_discharge:
                print(f'No morning discharge data for site {site.code} in DB. Assuming yesterdays discharge.')
                # Get the row with the highest date from predictor discharge
                morning_discharge = predictor_discharge.tail(1)
            else:
                # Only keep the lastest row
                morning_discharge = pd.DataFrame(morning_discharge['data_values']).tail(1)

            # Make sure morning_discharge is a dataframe
            morning_discharge = pd.DataFrame(morning_discharge)

            # Create a DataFrame from the predictor_discharge list
            df = pd.DataFrame(predictor_discharge)

            # Convert values smaller than 0 to NaN
            df.loc[df['data_value'] < 0, 'data_value'] = np.nan

            # Add the morning discharge to the DataFrame
            df = pd.concat([df, morning_discharge])

            # Round the data_values to 3 digits. That is, if a value is
            # 0.123456789, it will be rounded to 0.123 and if a value is 123.456789
            # it will be rounded to 123.0
            df['data_value'] = df['data_value'].apply(round_discharge_to_float)

            print("\n\nDEBUG: Site: ", site.code)
            print("DEBUG: DB data for predictor discharge:\n", df)

            # If we still have missing data, we interpolate the existing data.
            # We take the average of the existing data to fill the gaps.
            if (len(df) < L):
                # Get the average of the existing data
                length = np.sum(~np.isnan(df['data_value']))
                q_avg = np.nansum(df['data_value']) / length

                df.loc[len(df)] = pd.DataFrame({
                    'data_value': q_avg,
                    'local_date_time': None,
                    'utc_date_time': None})

            # Sum the discharge over the past L days
            q = np.nanmean(df['data_value'])

            if q < 0.0:
                q == np.nan
            site.predictor = q

            # Return the dangerous discharge value
            return q
        except Exception as e:
            print(f'Exception {e}')
            print(f'Note: No daily discharge data for site {site.code} in DB. Returning None.')
            return None

    @classmethod
    def from_DB_get_predictor_for_pentadal_forecast(cls, sdk, site, dates, lagdays=20):
        '''
        Calculate predictor from data retrieved from the data base.

        The DB connection hast to be established using:
        load_dotenv()
        ieh_sdk = IEasyHydroSDK()
        in the main code.

        Details:
            The function retrieves daily discharge data from the database
            and sums it up to get the predictor for the current pentad.
            The pentadal predictor for the pentad starting today + 1 day is
            calculated as the sum of the average daily discharge of today - 2
            days plus the average daily discharge of today - 1 day plus the
            morning discharge of today.

        Args:
            sdk (fl.SDK): The SDK connection object set up by calling
            site (Site): The site object to get dangerous discharge for.
            dates (list): A list of dates to get the predictor for.
            lagdays (int): The number of days to go back to retrieve data.

        Returns:
            float: The predictor for the current pentad.
        '''

        try:
            # Test that dates is a list of dates
            if not all(isinstance(date, dt.datetime) for date in dates):
                raise ValueError('Dates is not a list of dates')

            L = len(dates)

            # Define the date filter for the data request
            filters = BasicDataValueFilters(
                local_date_time__gte=min(dates),
                local_date_time__lte=max(dates)
            )
            print(f'Reading data from site {site.code} with date range from {filters["local_date_time__gte"]} to {filters["local_date_time__lte"]}.')

            predictor_discharge = sdk.get_data_values_for_site(
                site.code,
                'discharge_daily',
                filters=filters)

            # Test if we have data or if some of the data is smaller than 0
            # Increase the number of lag days to go back for averaging if any of
            # the above is true
            if not predictor_discharge or any(d['data_value'] < 0 for d in predictor_discharge['data_values']):
                # go back up to 20 days to retrieve data
                counter = 0
                while (not predictor_discharge and counter < lagdays):
                    filters = BasicDataValueFilters(
                        local_date_time__gte=filters['local_date_time__gte'] - dt.timedelta(days=1),
                        local_date_time__lte=filters['local_date_time__lte']
                    )
                    predictor_discharge = sdk.get_data_values_for_site(
                        site.code,
                        'discharge_daily',
                        filters=filters)
                    counter += 1
                    print(f'Note: Not enough data retrieved from DB. New date range from {filters["local_date_time__gte"]} to {filters["local_date_time__lte"]}.')

                # Test if we have data now
                if not predictor_discharge:
                    print(f'No recent data for site {site.code} in DB. No forecast available.')
                    return None

            predictor_discharge = predictor_discharge['data_values']
            # Check if we have enough data
            if len(predictor_discharge) < L:
                counter = 0
                while (len(predictor_discharge) < L and counter < lagdays):
                    filters = BasicDataValueFilters(
                        local_date_time__gte=filters['local_date_time__gte'] - dt.timedelta(days=1),
                        local_date_time__lte=filters['local_date_time__lte']
                    )
                    predictor_discharge = sdk.get_data_values_for_site(
                        site.code,
                        'discharge_daily',
                        filters=filters)
                    counter += 1
                    print(f'Note: Not enough data retrieved from DB. New date range from {filters["local_date_time__gte"]} to {filters["local_date_time__lte"]}.')
                    predictor_discharge = predictor_discharge['data_values']

            # Create a DataFrame from the predictor_discharge list
            df = pd.DataFrame(predictor_discharge)

            # Convert values smaller than 0 to NaN
            df.loc[df['data_value'] < 0, 'data_value'] = np.nan

            # If we still have missing data, we interpolate the existing data.
            # We take the average of the existing data to fill the gaps.
            if (len(df) < L):
                # Get the average of the existing data
                length = np.sum(~np.isnan(df['data_value']))
                q_avg = np.nansum(df['data_value']) / length

                df.loc[len(df)] = pd.DataFrame({
                    'data_value': q_avg,
                    'local_date_time': None,
                    'utc_date_time': None})

            #print("\n\nDEBUG: from_DB_get_predictor_for_pentadal_forecasts: df: ", df)
            # Aggregate the discharge data to daily values
            df['Date'] = pd.to_datetime(df['local_date_time']).dt.date
            df = df.groupby('Date').mean().reset_index()
            #print("\n\nDEBUG: from_DB_get_predictor_for_pentadal_forecasts: df: ", df)

            # Sum the discharge over the past L days
            q = np.nansum(df['data_value'])

            if q < 0.0:
                q == np.nan
            site.predictor = q

            # Return the dangerous discharge value
            return q
        except Exception:
            print(f'Note: No daily discharge data for site {site.code} in DB. Returning None.')
            return None

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> list:
        """
        Creates a list of Site objects from a DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame containing the site data.

        Returns:
            list: A list of Site objects.
        """
        try:
            # Check that the DataFrame contains the required columns
            required_columns = ['site_code', 'site_name', 'river_ru', 'punkt_ru', 'latitude', 'longitude', 'region', 'basin']
            if not all(column in df.columns for column in required_columns):
                raise ValueError(f'DataFrame is missing one or more required columns: {required_columns}')

            # Create a list of Site objects from the DataFrame
            sites = []
            for index, row in df.iterrows():
                site = cls(
                    code=row['site_code'],
                    name=row['site_name'],
                    river_name=row['river_ru'],
                    punkt_name=row['punkt_ru'],
                    lat=row['latitude'],
                    lon=row['longitude'],
                    region=row['region'],
                    basin=row['basin']
                )
                sites.append(site)
            return sites
        except Exception as e:
            print(f'Error creating Site objects from DataFrame: {e}')
            return []

    @classmethod
    def decad_forecast_sites_from_iEH_HF_SDK(cls, sites: list) -> list:
        """
        Creates a list of site objects with attributes read from the sites object.

        Args:
            sites (list): The object containing the site data.

        Returns:
            list: A list of Site objects.

        Note: The sites object is retrieved from iEH HF SDK with
            ieasyhydro_hf_sdk.get_discharge_sites()
        """
        try:
            # Convert the sites object to a DataFrame
            df = pd.DataFrame(sites)
            # Create a list of Site objects from the DataFrame
            sites = []
            for index, row in df.iterrows():
                row = pd.DataFrame(row).T

                # Test if the site has pentadal forecasts enabled and skip if not
                if row['enabled_forecasts'].values == None or \
                    (row['enabled_forecasts'].values[0]['decadal_forecast'] == False):
                    print(f'Skipping site {row["site_code"].values[0]} as decadal forecasts are not enabled.')
                    #print(f'enabled_forecasts: {row["enabled_forecasts"].values[0]}')
                    continue
                elif (row['enabled_forecasts'].values[0]['decadal_forecast'] == True):
                    # We need to create a pentadal forecast for the site as this is required to produce decadal forecasts as well.
                    print(f'Creating a virtual pentadal forecast for site {row["site_code"].values[0]} as decadal forecasts are enabled.')
                    name_parts = row['official_name'].values[0].split(' - ')
                    name_nat_parts = row['national_name'].values[0].split(' - ')
                    if len(name_parts) == 1:
                        name_parts = [row['official_name'].values[0], '']
                    if len(name_nat_parts) == 1:
                        name_nat_parts = [row['national_name'].values[0], '']

                    site = cls(
                        code=row['site_code'].values[0],
                        name=row['official_name'].values[0],
                        name_nat=row['national_name'].values[0],
                        river_name=name_parts[0],
                        river_name_nat=name_nat_parts[0],
                        punkt_name=name_parts[1],
                        punkt_name_nat=name_nat_parts[1],
                        lat=row['latitude'].values[0],
                        lon=row['longitude'].values[0],
                        region=row['region'].values[0]['official_name'],
                        region_nat=row['region'].values[0]['national_name'],
                        basin=row['basin'].values[0]['official_name'],
                        basin_nat=row['basin'].values[0]['national_name'],
                        qdanger=row['dangerous_discharge'].values[0],
                        histqmin=row['historical_discharge_minimum'].values[0],
                        histqmax=row['historical_discharge_maximum'].values[0],
                        bulletin_order=row['bulletin_order'].values[0],
                        daily_forecast=row['enabled_forecasts'].values[0]['daily_forecast'],
                        pentadal_forecast=row['enabled_forecasts'].values[0]['pentad_forecast'],
                        decadal_forecast=row['enabled_forecasts'].values[0]['decadal_forecast'],
                        monthly_forecast=row['enabled_forecasts'].values[0]['monthly_forecast'],
                        seasonal_forecast=row['enabled_forecasts'].values[0]['seasonal_forecast']
                    )
                    sites.append(site)
            # Get the basin and bulletin order for each site
            df = pd.DataFrame({
                'codes': [site.code for site in sites],
                'basins': [site.basin for site in sites],
                'bulletin_order': [site.bulletin_order for site in sites]
            })
            # Sort the sites_list according to the basin and bulletin order
            df = df.sort_values(by=['basins', 'bulletin_order'])
            print(f"Ordered sites: {df}")
            # Get the ordered list of codes
            ordered_codes = df['codes'].tolist()
            # Get site where site.code == ordered_codes[0]
            ordered_sites_list = []
            # Create a new list of sites in the order of the ordered_codes
            for code in ordered_codes:
                temp_site = next((site for site in sites if site.code == code), None)
                #print(f"temp_site: {temp_site}")
                # Test if temp_site is None
                if temp_site is None:
                    print(f"Site with code {code} not found.")
                    continue
                # Add the site to the ordered_sites_list
                # Test if ordered_sits_list is 'NoneType'
                if ordered_sites_list is None:
                    print(f"ordered_sites_list is NoneType")
                    ordered_sites_list = [temp_site]
                else: # ordered_sites_list is not 'NoneType'
                    ordered_sites_list.append(temp_site)
            print(f"Ordered sites: {[site.code for site in ordered_sites_list]}")
            return ordered_sites_list
        except Exception as e:
            print(f'Error creating Site objects from DataFrame: {e}')
            return []

    @classmethod
    def pentad_forecast_sites_from_iEH_HF_SDK(cls, sites: list) -> list:
        """
        Creates a list of site objects with attributes read from the sites object.

        Args:
            sites (list): The object containing the site data.

        Returns:
            list: A list of Site objects.

        Note: The sites object is retrieved from iEH HF SDK with
            ieasyhydro_hf_sdk.get_discharge_sites()
        """
        try:
            # Convert the sites object to a DataFrame
            df = pd.DataFrame(sites)
            # Create a list of Site objects from the DataFrame
            sites = []
            for index, row in df.iterrows():
                row = pd.DataFrame(row).T

                # Test if the site has pentadal forecasts enabled and skip if not
                if row['enabled_forecasts'].values == None or \
                    (row['enabled_forecasts'].values[0]['pentad_forecast'] == False and row['enabled_forecasts'].values[0]['decadal_forecast'] == False):
                    print(f'Skipping site {row["site_code"].values[0]} as neither pentadal nor decadal forecasts are not enabled.')
                    #print(f'enabled_forecasts: {row["enabled_forecasts"].values[0]}')
                    continue
                elif (row['enabled_forecasts'].values[0]['decadal_forecast'] == True and row['enabled_forecasts'].values[0]['pentad_forecast'] == False):
                    # We need to create a pentadal forecast for the site as this is required to produce decadal forecasts as well.
                    print(f'Creating a virtual pentadal forecast for site {row["site_code"].values[0]}, {row["official_name"].values[0]} as decadal forecasts are enabled.')
                    # We try to split the name of the site into river and punkt
                    # First try to separate by ' - '. If this fails, try to separate by '-'
                    name_parts = split_name(row['official_name'].values[0])
                    name_nat_parts = split_name(row['national_name'].values[0])
                    print(f"Name parts: {name_parts}, name_nat_parts: {name_nat_parts}")

                    site = site = cls(
                        code=row['site_code'].values[0],
                        name=row['official_name'].values[0],
                        name_nat=row['national_name'].values[0],
                        river_name=name_parts[0],
                        river_name_nat=name_nat_parts[0],
                        punkt_name=name_parts[1],
                        punkt_name_nat=name_nat_parts[1],
                        lat=row['latitude'].values[0],
                        lon=row['longitude'].values[0],
                        region=row['region'].values[0]['official_name'],
                        region_nat=row['region'].values[0]['national_name'],
                        basin=row['basin'].values[0]['official_name'],
                        basin_nat=row['basin'].values[0]['national_name'],
                        qdanger=row['dangerous_discharge'].values[0],
                        histqmin=row['historical_discharge_minimum'].values[0],
                        histqmax=row['historical_discharge_maximum'].values[0],
                        bulletin_order=row['bulletin_order'].values[0],
                        daily_forecast=row['enabled_forecasts'].values[0]['daily_forecast'],
                        pentadal_forecast=row['enabled_forecasts'].values[0]['pentad_forecast'],
                        decadal_forecast=row['enabled_forecasts'].values[0]['decadal_forecast'],
                        monthly_forecast=row['enabled_forecasts'].values[0]['monthly_forecast'],
                        seasonal_forecast=row['enabled_forecasts'].values[0]['seasonal_forecast'],
                        site_type=row['site_type'].values[0],
                    )
                    sites.append(site)
                elif (row['enabled_forecasts'].values[0]['pentad_forecast'] == True):
                    #print(f'Adding site {row["site_code"].values[0]}, {row["official_name"].values[0]} to the list of sites.')
                    # Try to split the names into river and punkt
                    name_parts = split_name(row['official_name'].values[0])
                    name_nat_parts = split_name(row['national_name'].values[0])
                    #print(f"Name parts: {name_parts}, name_nat_parts: {name_nat_parts}")

                    site = cls(
                        code=row['site_code'].values[0],
                        name=row['official_name'].values[0],
                        name_nat=row['national_name'].values[0],
                        river_name=name_parts[0],
                        river_name_nat=name_nat_parts[0],
                        punkt_name=name_parts[1],
                        punkt_name_nat=name_nat_parts[1],
                        lat=row['latitude'].values[0],
                        lon=row['longitude'].values[0],
                        region=row['region'].values[0]['official_name'],
                        region_nat=row['region'].values[0]['national_name'],
                        basin=row['basin'].values[0]['official_name'],
                        basin_nat=row['basin'].values[0]['national_name'],
                        qdanger=row['dangerous_discharge'].values[0],
                        histqmin=row['historical_discharge_minimum'].values[0],
                        histqmax=row['historical_discharge_maximum'].values[0],
                        bulletin_order=row['bulletin_order'].values[0],
                        daily_forecast=row['enabled_forecasts'].values[0]['daily_forecast'],
                        pentadal_forecast=row['enabled_forecasts'].values[0]['pentad_forecast'],
                        decadal_forecast=row['enabled_forecasts'].values[0]['decadal_forecast'],
                        monthly_forecast=row['enabled_forecasts'].values[0]['monthly_forecast'],
                        seasonal_forecast=row['enabled_forecasts'].values[0]['seasonal_forecast'],
                        site_type=row['site_type'].values[0]
                    )
                    sites.append(site)

            # Filter sites for manual stations only, otherwise we have duplicates in the list
            sites = [site for site in sites if site.site_type == 'manual']

            # Get the basin and bulletin order for each site
            df = pd.DataFrame({
                'codes': [site.code for site in sites],
                'basins': [site.basin for site in sites],
                'bulletin_order': [site.bulletin_order for site in sites]
            })
            # Sort the sites_list according to the basin and bulletin order
            df = df.sort_values(by=['basins', 'bulletin_order'])
            #print(f"Ordered sites: {df}")
            # Get the ordered list of codes
            ordered_codes = df['codes'].tolist()
            # Get site where site.code == ordered_codes[0]
            ordered_sites_list = []
            # Create a new list of sites in the order of the ordered_codes
            for code in ordered_codes:
                temp_site = next((site for site in sites if site.code == code), None)
                #print(f"temp_site: {temp_site}")
                # Test if temp_site is None
                if temp_site is None:
                    print(f"Site with code {code} not found.")
                    continue
                # Add the site to the ordered_sites_list
                # Test if ordered_sits_list is 'NoneType'
                if ordered_sites_list is None:
                    print(f"ordered_sites_list is NoneType")
                    ordered_sites_list = [temp_site]
                else: # ordered_sites_list is not 'NoneType'
                    ordered_sites_list.append(temp_site)
            #print(f"Ordered sites: {[site.code for site in ordered_sites_list]}")
            return ordered_sites_list
        except Exception as e:
            print(f'Error creating Site objects from DataFrame: {e}')
            return []

    @classmethod
    def virtual_decad_forecast_sites_from_iEH_HF_SDK(cls, sites: list) -> list:
        """
        Creates a list of site objects with attributes read from the sites object.

        Args:
            sites (list): The object containing the site data.

        Returns:
            list: A list of Site objects.

        Note: The sites object is retrieved from iEH HF SDK with
            ieasyhydro_hf_sdk.get_discharge_sites()
        """
        print(f'Creating virtual sites from iEH HF SDK.')
        try:
            # Convert the sites object to a DataFrame
            df = pd.DataFrame(sites)
            # Create a list of Site objects from the DataFrame
            sites = []
            for index, row in df.iterrows():
                row = pd.DataFrame(row).T

                # Test if the site has pentadal forecasts enabled and skip if not
                if row['enabled_forecasts'].values[0] == None or \
                    (row['enabled_forecasts'].values[0]['decadal_forecast'] == False):
                    print(f'Skipping site {row["site_code"].values[0]} as decadal forecasts are not enabled.')
                    #print(f'enabled_forecasts: {row["enabled_forecasts"].values[0]}')
                    continue
                elif (row['enabled_forecasts'].values[0]['decadal_forecast'] == True):
                    print(f"Creating a virtual pentadal forecast for site {row['site_code'].values[0]} as decadal forecasts are enabled.")
                    name_parts = row['official_name'].values[0].split(' - ')
                    name_nat_parts = row['national_name'].values[0].split(' - ')
                    if len(name_parts) == 1:
                        name_parts = [row['official_name'].values[0], '']
                    if len(name_nat_parts) == 1:
                        name_nat_parts = [row['national_name'].values[0], '']
                    site = cls(
                        code=row['site_code'].values[0],
                        name=row['official_name'].values[0],
                        name_nat=row['national_name'].values[0],
                        river_name=name_parts[0],
                        river_name_nat=name_nat_parts[0],
                        punkt_name=name_parts[1],
                        punkt_name_nat=name_nat_parts[1],
                        lat=row['latitude'].values[0],
                        lon=row['longitude'].values[0],
                        region=row['region'].values[0]['official_name'],
                        region_nat=row['region'].values[0]['national_name'],
                        basin=row['basin'].values[0]['official_name'],
                        basin_nat=row['basin'].values[0]['national_name'],
                        qdanger=row['dangerous_discharge'].values[0],
                        histqmin=row['historical_discharge_minimum'].values[0],
                        histqmax=row['historical_discharge_maximum'].values[0],
                        bulletin_order=row['bulletin_order'].values[0],  # Not yet implemented
                        daily_forecast=row['enabled_forecasts'].values[0]['daily_forecast'],
                        pentadal_forecast=row['enabled_forecasts'].values[0]['pentad_forecast'],
                        decadal_forecast=row['enabled_forecasts'].values[0]['decadal_forecast'],
                        monthly_forecast=row['enabled_forecasts'].values[0]['monthly_forecast'],
                        seasonal_forecast=row['enabled_forecasts'].values[0]['seasonal_forecast']
                    )
                    sites.append(site)

            # Get the basin and bulletin order for each site
            df = pd.DataFrame({
                'codes': [site.code for site in sites],
                'basins': [site.basin for site in sites],
                'bulletin_order': [site.bulletin_order for site in sites]
            })
            # Sort the sites_list according to the basin and bulletin order
            df = df.sort_values(by=['basins', 'bulletin_order'])
            print(f"Ordered sites: {df}")
            # Get the ordered list of codes
            ordered_codes = df['codes'].tolist()
            # Get site where site.code == ordered_codes[0]
            ordered_sites_list = []
            # Create a new list of sites in the order of the ordered_codes
            for code in ordered_codes:
                temp_site = next((site for site in sites if site.code == code), None)
                #print(f"temp_site: {temp_site}")
                # Test if temp_site is None
                if temp_site is None:
                    print(f"Site with code {code} not found.")
                    continue
                # Add the site to the ordered_sites_list
                # Test if ordered_sits_list is 'NoneType'
                if ordered_sites_list is None:
                    print(f"ordered_sites_list is NoneType")
                    ordered_sites_list = [temp_site]
                else: # ordered_sites_list is not 'NoneType'
                    ordered_sites_list.append(temp_site)
            print(f"Ordered sites: {[site.code for site in ordered_sites_list]}")
            return ordered_sites_list
        except Exception as e:
            print(f'Error creating Site objects from DataFrame: {e}')
            return []

    @classmethod
    def virtual_pentad_forecast_sites_from_iEH_HF_SDK(cls, sites: list) -> list:
        """
        Creates a list of site objects with attributes read from the sites object.

        Args:
            sites (list): The object containing the site data.

        Returns:
            list: A list of Site objects.

        Note: The sites object is retrieved from iEH HF SDK with
            ieasyhydro_hf_sdk.get_discharge_sites()
        """
        print(f'Creating virtual sites from iEH HF SDK.')
        try:
            # Convert the sites object to a DataFrame
            df = pd.DataFrame(sites)
            # Create a list of Site objects from the DataFrame
            sites = []
            for index, row in df.iterrows():
                row = pd.DataFrame(row).T

                # Test if the site has pentadal forecasts enabled and skip if not
                if row['enabled_forecasts'].values[0] == None or \
                    (row['enabled_forecasts'].values[0]['pentad_forecast'] == False and row['enabled_forecasts'].values[0]['decadal_forecast'] == False):
                    print(f'Skipping site {row["site_code"].values[0]} as neither pentadal nor decadal forecasts are not enabled.')
                    #print(f'enabled_forecasts: {row["enabled_forecasts"].values[0]}')
                    continue
                elif (row['enabled_forecasts'].values[0]['decadal_forecast'] == True and row['enabled_forecasts'].values[0]['pentad_forecast'] == False):
                    print(f"Creating a virtual pentadal forecast for site {row['site_code'].values[0]} as decadal forecasts are enabled.")
                    name_parts = row['official_name'].values[0].split(' - ')
                    name_nat_parts = row['national_name'].values[0].split(' - ')
                    if len(name_parts) == 1:
                        name_parts = [row['official_name'].values[0], '']
                    if len(name_nat_parts) == 1:
                        name_nat_parts = [row['national_name'].values[0], '']
                    site = cls(
                        code=row['site_code'].values[0],
                        name=row['official_name'].values[0],
                        name_nat=row['national_name'].values[0],
                        river_name=name_parts[0],
                        river_name_nat=name_nat_parts[0],
                        punkt_name=name_parts[1],
                        punkt_name_nat=name_nat_parts[1],
                        lat=row['latitude'].values[0],
                        lon=row['longitude'].values[0],
                        region=row['region'].values[0]['official_name'],
                        region_nat=row['region'].values[0]['national_name'],
                        basin=row['basin'].values[0]['official_name'],
                        basin_nat=row['basin'].values[0]['national_name'],
                        qdanger=row['dangerous_discharge'].values[0],
                        histqmin=row['historical_discharge_minimum'].values[0],
                        histqmax=row['historical_discharge_maximum'].values[0],
                        bulletin_order=row['bulletin_order'].values[0],  # Not yet implemented
                        daily_forecast=row['enabled_forecasts'].values[0]['daily_forecast'],
                        pentadal_forecast=row['enabled_forecasts'].values[0]['pentad_forecast'],
                        decadal_forecast=row['enabled_forecasts'].values[0]['decadal_forecast'],
                        monthly_forecast=row['enabled_forecasts'].values[0]['monthly_forecast'],
                        seasonal_forecast=row['enabled_forecasts'].values[0]['seasonal_forecast']
                    )
                    sites.append(site)
                elif (row['enabled_forecasts'].values[0]['pentad_forecast'] == True):
                    print(f'Adding site {row["site_code"].values[0]} to the list of sites.')
                    name_parts = row['official_name'].values[0].split(' - ')
                    name_nat_parts = row['national_name'].values[0].split(' - ')
                    if len(name_parts) == 1:
                        name_parts = [row['official_name'].values[0], '']
                    if len(name_nat_parts) == 1:
                        name_nat_parts = [row['national_name'].values[0], '']
                    site = cls(
                        code=row['site_code'].values[0],
                        name=row['official_name'].values[0],
                        name_nat=row['national_name'].values[0],
                        river_name=name_parts[0],
                        river_name_nat=name_nat_parts[0],
                        punkt_name=name_parts[1],
                        punkt_name_nat=name_nat_parts[1],
                        lat=row['latitude'].values[0],
                        lon=row['longitude'].values[0],
                        region=row['region'].values[0]['official_name'],
                        region_nat=row['region'].values[0]['national_name'],
                        basin=row['basin'].values[0]['official_name'],
                        basin_nat=row['basin'].values[0]['national_name'],
                        qdanger=row['dangerous_discharge'].values[0],
                        histqmin=row['historical_discharge_minimum'].values[0],
                        histqmax=row['historical_discharge_maximum'].values[0],
                        bulletin_order=row['bulletin_order'].values[0],
                        daily_forecast=row['enabled_forecasts'].values[0]['daily_forecast'],
                        pentadal_forecast=row['enabled_forecasts'].values[0]['pentad_forecast'],
                        decadal_forecast=row['enabled_forecasts'].values[0]['decadal_forecast'],
                        monthly_forecast=row['enabled_forecasts'].values[0]['monthly_forecast'],
                        seasonal_forecast=row['enabled_forecasts'].values[0]['seasonal_forecast']
                    )
                    sites.append(site)
            # Get the basin and bulletin order for each site
            df = pd.DataFrame({
                'codes': [site.code for site in sites],
                'basins': [site.basin for site in sites],
                'bulletin_order': [site.bulletin_order for site in sites]
            })
            # Sort the sites_list according to the basin and bulletin order
            df = df.sort_values(by=['basins', 'bulletin_order'])
            print(f"Ordered sites: {df}")
            # Get the ordered list of codes
            ordered_codes = df['codes'].tolist()
            # Get site where site.code == ordered_codes[0]
            ordered_sites_list = []
            # Create a new list of sites in the order of the ordered_codes
            for code in ordered_codes:
                temp_site = next((site for site in sites if site.code == code), None)
                #print(f"temp_site: {temp_site}")
                # Test if temp_site is None
                if temp_site is None:
                    print(f"Site with code {code} not found.")
                    continue
                # Add the site to the ordered_sites_list
                # Test if ordered_sits_list is 'NoneType'
                if ordered_sites_list is None:
                    print(f"ordered_sites_list is NoneType")
                    ordered_sites_list = [temp_site]
                else: # ordered_sites_list is not 'NoneType'
                    ordered_sites_list.append(temp_site)
            print(f"Ordered sites: {[site.code for site in ordered_sites_list]}")
            return ordered_sites_list
        except Exception as e:
            print(f'Error creating Site objects from DataFrame: {e}')
            return []

    @classmethod
    def change_basin(cls, site, basin):
        '''
        Change the basin of the site.

        Args:
            site (Site): The site object to change the basin for.
            basin (str): The new basin name.

        Returns:
            str: The new basin name.
        '''
        site.basin = basin
        return basin

class PredictorDates:
    """
    Store lists of predictor dates, depending on the forecast horizons which are
    active.
    """
    def __init__(self, pentad=[], decad=[], month=[], season=[]):
        self.pentad = pentad
        self.decad = decad
        self.month = month
        self.season = season

    def __repr__(self):
        return f"PredictorDates(pentad={self.pentad}, decad={self.decad}, month={self.month}, season={self.season})"

# endregion
