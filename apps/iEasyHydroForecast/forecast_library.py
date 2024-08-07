import os
import json
import numpy as np
import pandas as pd
import datetime as dt
import math
import logging

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
    temp_df = data_df[data_df['issue_date'] != False]

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
    Generate issue and forecast dates for each station in the input DataFrame.

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

    # Test if the input data contains the required columns
    if not all(column in data_df_0.columns for column in [datetime_col, station_col, discharge_col]):
        raise ValueError(f'DataFrame is missing one or more required columns: {datetime_col, station_col, discharge_col}')

    # Apply the calculation function to each group based on the 'station' column
    data_df_decad = data_df_0.copy(deep=True)
    modified_data = data_df_0.groupby(station_col).apply(apply_calculation, datetime_col = datetime_col, discharge_col = discharge_col)
    if forecast_flags.decad:
        modified_data_decad = data_df_decad.groupby(station_col).apply(apply_calculation_decad, datetime_col = datetime_col, discharge_col = discharge_col)
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
        logger.debug(f'    Calculating norm discharge for site {site.code} ...')
        Site.from_df_get_norm_discharge(
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

    This function was previously called at a later stage in the code. It was
    moved to a pre-processing step and now requires to be executed for pentadal
    and decadal forecasts in any case. Therefore, the forecast flags are ste to
    true for both forecasts.

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

    # Aggregate predictors and forecast variables
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
        raise ValueError(f'Values in column {pentad_col} are not between 1 and 72')

    # Filter for the forecast pentad
    data_dfp = data_df[data_df[pentad_col] == float(forecast_pentad)]

    # Test if data_df is empty
    if data_dfp.empty:
        raise ValueError(f'DataFrame is empty after filtering for pentad {forecast_pentad}')

    # Initialize the slope and intercept columns to 0 and 1
    data_dfp = data_dfp.assign(slope=1.0)
    data_dfp = data_dfp.assign(intercept=0.0)
    data_dfp = data_dfp.assign(forecasted_discharge=-1.0)
    data_dfp = data_dfp.assign(q_mean=0.0)
    data_dfp = data_dfp.assign(q_std_sigma=0.0)
    data_dfp = data_dfp.assign(delta=0.0)

    # Loop over each station we have data for
    for station in data_dfp[station_col].unique():
        # filter for station and pentad. If the DataFrame is empty,
        # raise an error.
        try:
            station_data = data_dfp[(data_dfp[station_col] == station)]
            # Test if station_data is empty
            if station_data.empty:
                raise ValueError(f'DataFrame is empty after filtering for station {station}')
        except ValueError as e:
            print(f'Error in perform_linear_regression when filtering for station data: {e}')

        # Drop NaN values, i.e. keep only the time steps where both
        # discharge_sum and discharge_avg are not NaN. These correspond to the
        # time steps where we produce a forecast.
        station_data = station_data.dropna()
        if station_data.empty:
            logger.info("No data for station {station} in pentad {forecast_pentad}")
            continue

        # Get the discharge_sum and discharge_avg columns
        discharge_sum = station_data[predictor_col].values.reshape(-1, 1)
        discharge_avg = station_data[discharge_avg_col].values.reshape(-1, 1)

        # Perform the linear regression
        model = LinearRegression().fit(discharge_sum, discharge_avg)
        if int(station) == 15292:
            logger.debug("model output: %s", model)
            logger.debug("model.coef_: %s", model.coef_)
            logger.debug("model.intercept_: %s", model.intercept_)

        # Calculate discharge statistics
        q_mean = np.mean(discharge_avg)
        q_std_sigma = np.std(discharge_avg)
        delta = 0.674 * q_std_sigma

        # Get the slope and intercept
        slope = model.coef_[0][0]
        intercept = model.intercept_[0]

        # Print the slope and intercept
        logger.debug(f'Station: {station}, pentad: {forecast_pentad}, slope: {slope}, intercept: {intercept}')

        # # Create a scatter plot with the regression line
        # fig = px.scatter(station_data, x=predictor_col, y=discharge_avg_col, color=station_col)
        # fig.add_trace(px.line(x=discharge_sum.flatten(), y=model.predict(discharge_sum).flatten()).data[0])
        # fig.show()

        # Store the slope and intercept in the data_df
        data_dfp.loc[(data_dfp[station_col] == station), 'slope'] = slope
        data_dfp.loc[(data_dfp[station_col] == station), 'intercept'] = intercept
        data_dfp.loc[(data_dfp[station_col] == station), 'q_mean'] = q_mean
        data_dfp.loc[(data_dfp[station_col] == station), 'q_std_sigma'] = q_std_sigma
        data_dfp.loc[(data_dfp[station_col] == station), 'delta'] = delta

        # Test if station is of same type as data_dfp[station_col][0]
        if type(station) != type(data_dfp.loc[data_dfp.index[0], station_col]):
            raise ValueError(f"Station type {type(station)} does not match the type of data_dfp[station_col][0] {type(data_dfp[station_col][0])}")


        # Calculate the forecasted discharge for the current station and forecast_pentad
        data_dfp.loc[(data_dfp[station_col] == station), 'forecasted_discharge'] = \
            slope * data_dfp.loc[(data_dfp[station_col] == station), predictor_col] + intercept

        # print rows where code == 15292
        if int(station) == 15292:
            #logger.debug("column names of data_dfp:\n%s", station_data.columns)
            logger.debug("DEBUG: forecasting:perform_linear_regression: data_dfp after linear regression: \n%s",
              data_dfp.loc[data_dfp[station_col] == station, ['date', station_col, pentad_col, predictor_col, discharge_avg_col, 'slope', 'intercept', 'forecasted_discharge']].tail(10))

    return data_dfp

def perform_forecast(fc_sites, group_id=None, result_df=None,
                     code_col='code', group_col='pentad_in_year'):
    # Perform forecast
    logger.debug("Performing pentad forecast ...")

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

    # Drop NaN values in columns with observed and simulated data
    data = data.dropna(subset=[observed_col, simulated_col])

    # Calculate the numerator and denominator of the NSE formula
    numerator = ((data[observed_col] - data[simulated_col])**2).sum()
    denominator = ((data[observed_col] - data[observed_col].mean())**2).sum()

    # Catch cases where the denomitar is 0
    if denominator == 0:
        print("Denominator is 0")
        print("data['date']", data['date'])
        print('data[observed_col]', data[observed_col])
        print('data[observed_col].mean()', data[observed_col].mean())
        print("numerator", numerator)
        print("denominator", denominator)
        return np.nan, np.nan

    # Calculate the efficacy of the model
    sdivsigma = numerator / denominator

    # Calculate the NSE value
    nse_value = 1 - (numerator / denominator)

    return pd.Series([sdivsigma, nse_value], index=['sdivsigma', 'nse'])

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

    # Drop NaN values
    data = data.dropna(subset=[observed_col, simulated_col])

    # Calculate the forecast accuracy
    accuracy = (abs(data[observed_col] - data[simulated_col]) <= data[delta_col]).mean()
    delta = data[delta_col].iloc[-1]

    return pd.Series([delta, accuracy], index=['delta', 'accuracy'])

def mae(data: pd.DataFrame, observed_col: str, simulated_col: str):
    """
    Calculate the mean average error between observed and simulated data

    Args:
        data (pandas.DataFrame): The input data containing the observed and simulated data.
        observed_col (str): The name of the column containing the observed data.
        simulated_col (str): The name of the column containing the simulated data.

    Returns:
        float: mean average error between observed and simulated data

    Raises:
        ValueError: If the input data is missing one or more required columns.

    """
    # Test the input. Make sure that the DataFrame contains the required columns
    if not all(column in data.columns for column in [observed_col, simulated_col]):
        raise ValueError(f'DataFrame is missing one or more required columns: {observed_col, simulated_col}')

    # Drop NaN values
    data = data.dropna(subset=[observed_col, simulated_col])

    # Calculate the mean average error between the observed and simulated data
    mae = abs(data[observed_col] - data[simulated_col]).mean()

    return pd.Series([mae], index=['mae'])

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

def calculate_skill_metrics_pentade(observed: pd.DataFrame, simulated: pd.DataFrame):
    """
    For each model and hydropost in the simulated DataFrame, calculates a number
    of skill metrics based on the observed DataFrame.

    Args:
        observed (pd.DataFrame): The DataFrame containing the observed data.
        simulated (pd.DataFrame): The DataFrame containing the simulated data.

    Returns:
        pd.DataFrame: The DataFrame containing the skill metrics for each model
            and hydropost.
    """
    # Test the input. Make sure that the DataFrames contain the required columns
    if not all(column in observed.columns for column in ['code', 'date', 'discharge_avg', 'model_long', 'model_short', 'delta']):
        raise ValueError(f'Observed DataFrame is missing one or more required columns: {["code", "date", "discharge_avg", "model_long", "model_short", "delta"]}')
    if not all(column in simulated.columns for column in ['code', 'date', 'pentad_in_year', 'forecasted_discharge', 'model_long', 'model_short']):
        raise ValueError(f'Simulated DataFrame is missing one or more required columns: {["code", "date", "pentad_in_year", "forecasted_discharge", "model_long", "model_short"]}')

    logger.debug("DEBUG: simulated.columns\n%s", simulated.columns)
    logger.debug("DEBUG: simulated.head()\n", simulated.head(5))
    logger.debug("DEBUG: simulated.tail()\n", simulated.tail(5))
    logger.info("DEBUG: observed.columns%s\n", observed.columns)
    logger.debug("DEBUG: observed.head()\n", observed.head(5))
    logger.debug("DEBUG: observed.tail()\n", observed.tail(5))
    # Merge the observed and simulated DataFrames
    skill_metrics_df = pd.merge(
        simulated,
        observed[['code', 'date', 'discharge_avg', 'delta']],
        on=['code', 'date'])
    logger.debug("DEBUG: skill_metrics_df.columns\n%s", skill_metrics_df.columns)
    logger.debug("DEBUG: skill_metrics_df.head()\n", skill_metrics_df.head(5))
    logger.debug("DEBUG: skill_metrics_df.tail()\n", skill_metrics_df.tail(5))

    # Identify tuples in each cell
    is_tuple = skill_metrics_df.applymap(lambda x: isinstance(x, tuple))
    #logger.info("DEBUG: is_tuple\n", is_tuple)
    # Check if there are any True values in is_tuple
    contains_tuples = is_tuple.any(axis=1).any()
    # Test if there are any tuples in the DataFrame
    if contains_tuples:
        logger.debug("There are tuples after the merge.")

        # Step 2: Filter rows that contain any tuples
        rows_with_tuples = skill_metrics_df[is_tuple.any(axis=1)]

        # Print rows with tuples
        logger.debug(rows_with_tuples)
    else:
        logger.debug("No tuples found after the merge.")

    # Calculate the skill metrics for each group based on the 'pentad_in_year', 'code' and 'model' columns
    skill_stats = skill_metrics_df. \
        groupby(['pentad_in_year', 'code', 'model_long', 'model_short']). \
        apply(
            sdivsigma_nse,
            observed_col='discharge_avg',
            simulated_col='forecasted_discharge'). \
        reset_index()
    # Identify tuples in each cell
    is_tuple = skill_stats.applymap(lambda x: isinstance(x, tuple))
    # Check if there are any True values in is_tuple
    contains_tuples = is_tuple.any(axis=1).any()
    # Test if there are any tuples in the DataFrame
    if contains_tuples:
        logger.debug("There are tuples in skill_stats.")

        # Step 2: Filter rows that contain any tuples
        rows_with_tuples = skill_stats[is_tuple.any(axis=1)]

        # logger.info rows with tuples
        logger.debug(rows_with_tuples)
    else:
        logger.debug("No tuples found in skill_stats.")

    mae_stats = skill_metrics_df. \
        groupby(['pentad_in_year', 'code', 'model_long', 'model_short']). \
        apply(
            mae,
            observed_col='discharge_avg',
            simulated_col='forecasted_discharge').\
        reset_index()
    # Identify tuples in each cell
    is_tuple = mae_stats.applymap(lambda x: isinstance(x, tuple))
    # Check if there are any True values in is_tuple
    contains_tuples = is_tuple.any(axis=1).any()
    # Test if there are any tuples in the DataFrame
    if contains_tuples:
        logger.debug("There are tuples in mae_stats.")

        # Step 2: Filter rows that contain any tuples
        rows_with_tuples = mae_stats[is_tuple.any(axis=1)]

        # Print rows with tuples
        logger.debug(rows_with_tuples)
    else:
        logger.debug("No tuples found in mae_stats.")

    accuracy_stats = skill_metrics_df. \
        groupby(['pentad_in_year', 'code', 'model_long', 'model_short']). \
        apply(
            forecast_accuracy_hydromet,
            observed_col='discharge_avg',
            simulated_col='forecasted_discharge',
            delta_col='delta').\
        reset_index()
    # Identify tuples in each cell
    is_tuple = accuracy_stats.applymap(lambda x: isinstance(x, tuple))
    # Check if there are any True values in is_tuple
    contains_tuples = is_tuple.any(axis=1).any()
    # Test if there are any tuples in the DataFrame
    if contains_tuples:
        logger.debug("There are tuples in accuracy_stats.")

        # Step 2: Filter rows that contain any tuples
        rows_with_tuples = accuracy_stats[is_tuple.any(axis=1)]

        # Print rows with tuples
        logger.debug(rows_with_tuples)
    else:
        logger.debug("No tuples found in accuracy_stats.")

    # Merge the skill metrics with the accuracy stats
    #print("DEBUG: skill_stats.columns\n", skill_stats.columns)
    #print("DEBUG: accuracy_stats.columns\n", accuracy_stats.columns)
    skill_stats = pd.merge(skill_stats, accuracy_stats, on=['pentad_in_year', 'code', 'model_long', 'model_short'])

    # Identify tuples in each cell
    is_tuple = skill_stats.applymap(lambda x: isinstance(x, tuple))
    # Check if there are any True values in is_tuple
    contains_tuples = is_tuple.any(axis=1).any()
    # Test if there are any tuples in the DataFrame
    if contains_tuples:
        logger.debug("There are tuples after the merge.")

        # Step 2: Filter rows that contain any tuples
        rows_with_tuples = skill_stats[is_tuple.any(axis=1)]

        # Print rows with tuples
        logger.debug(rows_with_tuples)
    else:
        logger.debug("No tuples found after the merge.")

    #print("DEBUG: skill_stats.columns\n", skill_stats.columns)
    #print("DEBUG: mae_stats.columns\n", mae_stats.columns)
    skill_stats = pd.merge(skill_stats, mae_stats, on=['pentad_in_year', 'code', 'model_long', 'model_short'])
    #print("DEBUG: skill_stats.columns\n", skill_stats.columns)

    # Identify tuples in each cell
    is_tuple = skill_stats.applymap(lambda x: isinstance(x, tuple))
    # Check if there are any True values in is_tuple
    contains_tuples = is_tuple.any(axis=1).any()
    # Test if there are any tuples in the DataFrame
    if contains_tuples:
        logger.debug("There are tuples after the merge.")

        # Step 2: Filter rows that contain any tuples
        rows_with_tuples = skill_stats[is_tuple.any(axis=1)]

        # Print rows with tuples
        logger.debug(rows_with_tuples)
    else:
        logger.debug("No tuples found after the merge.")


    return skill_stats


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

    return discharge_data

def write_linreg_pentad_forecast_data(data: pd.DataFrame):
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

    # Get the year of the majority of the last_line dates
    year = last_line['date'].dt.year.mode()[0]
    logger.debug(f'mode of year: {year}')

    # If the year of one date of last_year is not equal to the majority year,
    # set the year of the date to the majority year, set predictor to NaN,
    # set discharge_avg to NaN, set forecasted_discharge to _nan.
    last_line.loc[last_line['date'].dt.year != year, 'predictor'] = np.nan
    last_line.loc[last_line['date'].dt.year != year, 'discharge_avg'] = np.nan
    last_line.loc[last_line['date'].dt.year != year, 'forecasted_discharge'] = np.nan
    last_line.loc[last_line['date'].dt.year != year, 'date'] = pd.to_datetime(
        last_line.loc[last_line['date'].dt.year != year, 'date'].dt.strftime(f'{year}-%m-%d'))

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

    # Get the year of the majority of the last_line dates
    year = last_line['date'].dt.year.mode()[0]
    logger.debug(f'mode of year: {year}')

    # If the year of one date of last_year is not equal to the majority year,
    # set the year of the date to the majority year, set predictor to NaN,
    # set discharge_avg to NaN, set forecasted_discharge to _nan.
    last_line.loc[last_line['date'].dt.year != year, 'predictor'] = np.nan
    last_line.loc[last_line['date'].dt.year != year, 'discharge_avg'] = np.nan
    last_line.loc[last_line['date'].dt.year != year, 'forecasted_discharge'] = np.nan
    last_line.loc[last_line['date'].dt.year != year, 'date'] = pd.to_datetime(
        last_line.loc[last_line['date'].dt.year != year, 'date'].dt.strftime(f'{year}-%m-%d'))

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

def write_pentad_hydrograph_data(data: pd.DataFrame):
    """
    Calculates statistics of the pentadal hydrograph and saves it to a csv file.

    Args:
    data (pd.DataFrame): The data to be written to a csv file.

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
    # pentad, the data is collected for. Therefore, we add 1 day to the 'date'
    # column and recalculate pentad and pentad_in_year.
    # Add 1 day to the date column
    data.loc[:, 'date'] = data.loc[:, 'date'] + pd.DateOffset(days=1)
    # Calculate pentad and pentad_in_year
    data.loc[:, 'pentad'] = data['date'].apply(tl.get_pentad)
    data.loc[:, 'pentad_in_year'] = data['date'].apply(tl.get_pentad_in_year)
    # Get year of the latest date in data
    current_year = data['date'].dt.year.max()

    # Calculate runoff statistics
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

def write_decad_hydrograph_data(data: pd.DataFrame):
    """
    Calculates statistics of the decadal hydrograph and saves it to a csv file.

    Args:
    data (pd.DataFrame): The data to be written to a csv file.

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
    # Add 1 day to the date column
    data.loc[:, 'date'] = data.loc[:, 'date'] + pd.DateOffset(days=1)
    # Calculate pentad and pentad_in_year
    data.loc[:, 'pentad'] = data['date'].apply(tl.get_pentad)
    data.loc[:, 'pentad_in_year'] = data['date'].apply(tl.get_pentad_in_year)

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
    data.loc[:, 'date'] = data.loc[:, 'date'] + pd.DateOffset(days=1)
    # Calculate decad and decad_in_year
    data.loc[:, 'decad_in_month'] = data['date'].apply(tl.get_decad_in_month)
    data.loc[:, 'decad_in_year'] = data['date'].apply(tl.get_decad_in_year)

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
    simulated = simulated[['code', 'date', 'pentad_in_month', 'pentad_in_year', 'forecasted_discharge', 'model_long', 'model_short']]

    # write the data to csv
    ret = simulated.to_csv(filename, index=False)

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
    def __init__(self, code: str, name="Name", river_name="River",
                 punkt_name="Punkt", lat=0.0, lon=0.0, region="Region",
                 basin="Basin", predictor=-10000.0, fc_qmin=-10000.0,
                 fc_qmax=-10000.0, fc_qexp=-10000.0, qnorm=-10000.0,
                 qmin=-10000.0, qmax=-10000.0,
                 perc_norm=-10000.0, qdanger=-10000.0, slope=-10000.0,
                 intercept=-10000.0, delta=-10000.0, sdivsigma=-10000.0,
                 accuracy=-10000.0):
        """
        Initializes a new Site object.

        Args:
            code (str): The site code.
            name (str): The site name (a combination of river_name and river_punkt).
            river_name (str): The name of the river that the site is located on.
            punkt_name (str): The name of the location within the river where the site is located.
            lat (float): The latitude of the site in WSG 84.
            lon (float): The longitude of the site in WSG 84.
            region (str): The region that the site is located in (typically oblast).
            basin (str): The basin that the site is located in.
            precictor (float): The predictor value for the site.
            fc_qmin (float): The lower bound of the discharge forecasted for the next pentad.
            fc_qmax (float): The upper bound of the discharge forecasted for the next pentad.
            fc_qexp (float): The expected discharge forecasted for the next pentad.
            qnorm (float): The norm discharge for the site.
            qmin (float): The minimum discharge for the site.
            qmax (float): The maximum discharge for the site.
            qdanger (str): The threshold discharge for a dangerous flood.
        """
        self.code = str(code)
        self.name = str(name)
        self.river_name = str(river_name)
        self.punkt_name = str(punkt_name)
        self.lat = float(lat)
        self.lon = float(lon)
        self.region = str(region)
        self.basin = str(basin)
        self.predictor = predictor
        self.fc_qmin = fc_qmin
        self.fc_qmax = fc_qmax
        self.fc_qexp = fc_qexp
        self.qmin = qmin
        self.qmax = qmax
        self.qnorm = qnorm
        self.perc_norm = perc_norm
        self.qdanger = qdanger
        self.slope = slope
        self.intercept = intercept
        self.delta = delta
        self.sdivsigma = sdivsigma
        self.accuracy = accuracy

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
