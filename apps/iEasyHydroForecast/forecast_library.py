import json
import numpy as np
import pandas as pd
import datetime as dt
import math

from ieasyhydro_sdk.filters import BasicDataValueFilters

from sklearn.linear_model import LinearRegression

# === Functions ===
# region Function definitions


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


def get_predictor_dates(input_date: str, n: int):
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
        print(input_date)

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


def perform_linear_regression(
        data_df: pd.DataFrame, station_col: str, pentad_col: str, discharge_sum_col: str,
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
        discharge_sum_col (str): The name of the column containing the discharge
            sum values.
        discharge_avg_col (str): The name of the column containing the discharge
            average values.
        forecast_pentad(int): The pentad of the year to perform the linear
            regression for. Must be a value between 1 and 72.

    Returns:
        pd.DataFrame: A DataFrame containing the columns of the input data frame
            plus the columns 'slope', 'intercept' and 'forecasted_discharge'.
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
    if not isinstance(discharge_sum_col, str):
        raise TypeError('discharge_sum_col must be a string')
    if not isinstance(discharge_avg_col, str):
        raise TypeError('discharge_avg_col must be a string')
    if not isinstance(forecast_pentad, int):
        raise TypeError('forecast_pentad must be an integer')
    if not all(column in data_df.columns for column in [station_col, pentad_col, discharge_sum_col, discharge_avg_col]):
        raise ValueError('DataFrame is missing one or more required columns')

    # Test that the required columns exist in the input DataFrame.
    required_columns = [station_col, pentad_col, discharge_sum_col, discharge_avg_col]
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
    data_dfp = data_dfp.assign(slope=1)
    data_dfp = data_dfp.assign(intercept=0)
    data_dfp = data_dfp.assign(forecasted_discharge=-1)

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

        # Get the discharge_sum and discharge_avg columns
        discharge_sum = station_data[discharge_sum_col].values.reshape(-1, 1)
        discharge_avg = station_data[discharge_avg_col].values.reshape(-1, 1)

        # Perform the linear regression
        model = LinearRegression().fit(discharge_sum, discharge_avg)

        # Get the slope and intercept
        slope = model.coef_[0][0]
        intercept = model.intercept_[0]
        # forecasted_discharge = slope * discharge_sum + intercept

        # Print the slope and intercept
        print(f'Station: {station}, pentad: {forecast_pentad}, slope: {slope}, intercept: {intercept}')

        # # Create a scatter plot with the regression line
        # fig = px.scatter(station_data, x=discharge_sum_col, y=discharge_avg_col, color=station_col)
        # fig.add_trace(px.line(x=discharge_sum.flatten(), y=model.predict(discharge_sum).flatten()).data[0])
        # fig.show()

        # Store the slope and intercept in the data_df
        data_dfp.loc[(data_dfp[station_col] == station), 'slope'] = slope
        data_dfp.loc[(data_dfp[station_col] == station), 'intercept'] = intercept

        # Calculate the forecasted discharge for the current station and forecast_pentad
        data_dfp.loc[(data_dfp[station_col] == station), 'forecasted_discharge'] = \
            slope * data_dfp.loc[(data_dfp[station_col] == station), discharge_sum_col] + intercept

    return data_dfp


def calculate_forecast_skill(data_df: pd.DataFrame, station_col: str,
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

    # Initialize the slope and intercept columns to 0 and 1
    data_df.loc[:, 'absolute_error'] = 0.0
    data_df.loc[:, 'observation_std0674'] = 0.0
    data_df.loc[:, 'flag'] = 0.0

    # Loop over each station and pentad
    for station in data_df[station_col].unique():
        for pentad in data_df[pentad_col].unique():
            # Get the data for the station and pentad
            station_data = data_df.loc[
                (data_df[station_col] == station) & (data_df[pentad_col] == pentad), :]

            # Drop NaN values
            station_data = station_data.dropna()

            # Calculate the absolute error between the simulation data and the observation data
            absolute_error = abs(station_data[simulation_col] - station_data[observation_col])
            observation_std = station_data[observation_col].std()
            # Note: .std() will yield NaN if there is only one value in the DataFrame
            # Test if the standard deviation is NaN and return 0.0 if it is
            if np.isnan(observation_std):
                observation_std = 0.0

            # Set the flag if the error is smaller than 0.674 times the standard deviation of the observation data
            flag = absolute_error < 0.674 * observation_std

            # Delta is 0.674 times the standard deviation of the observation data
            # This is the measure for the allowable uncertainty of a good forecast
            observation_std0674 = 0.674 * observation_std

            # Store the slope and intercept in the data_df
            data_df.loc[
                (data_df[station_col] == station) & (data_df[pentad_col] == pentad),
                'absolute_error'] = absolute_error
            data_df.loc[
                (data_df[station_col] == station) & (data_df[pentad_col] == pentad),
                'observation_std0674'] = observation_std0674
            data_df.loc[
                (data_df[station_col] == station) & (data_df[pentad_col] == pentad),
                'flag'] = flag

    return data_df


def generate_issue_and_forecast_dates(data_df: pd.DataFrame, datetime_col: str,
                                      station_col: str, discharge_col: str):
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

    Returns:
    pandas.DataFrame
        The modified DataFrame with the issue and forecast dates for each station.
    """
    def apply_calculation(data_df, datetime_col, discharge_col):
        # Make sure data_df[datetime_col] is of datetime type
        data_df[datetime_col] = pd.to_datetime(data_df[datetime_col])

        # Set negative values to nan
        data_df[discharge_col] = data_df[discharge_col].apply(lambda x: np.nan if x < 0 else x)

        # Fill in data gaps of up to 3 days by linear interpolation
        data_df[discharge_col] = data_df[discharge_col].interpolate(
            method='linear', limit_direction='both', limit=3)

        # Define the start and end dates for the date range
        start_date = data_df[datetime_col].dt.date.min()
        end_date = data_df[datetime_col].dt.date.max()

        years = range(start_date.year, end_date.year+1)  # Specify the desired years
        months = range(1, 13)  # Specify the desired months
        days = [5, 10, 15, 20, 25]  # Specify the desired days

        # Create a list to store the issue dates
        issue_date_range_list = []

        # Iterate over the years, months, and days to construct the issue dates
        for year in years:
            for month in months:
                # Get the last day of the month
                last_day = pd.Timestamp(year, month, 1) + pd.offsets.MonthEnd()

                # Add the specific days and the last day of the month to the issue_date_range_list
                issue_date_range_list.extend([pd.Timestamp(year, month, day) for day in days + [last_day.day]])

        # Create a DataFrame for the issue dates
        issue_date_df = pd.DataFrame({'Date': issue_date_range_list, 'issue_date': True})
        issue_date_df['Date'] = issue_date_df['Date'].dt.date

        # Merge the issue_date_df with data_df
        data_df['Date'] = data_df[datetime_col].dt.date
        data_df = data_df.merge(issue_date_df, how='left', on='Date')
        # print(data_df.head(n=10))

        # Initialize data_df['discharge_sum'] and data_df['discharge_avg'] to NaN
        data_df['discharge_sum'] = np.nan
        data_df['discharge_avg'] = np.nan

        # Loop over each issue_date = True in data_df
        for index, row in data_df[data_df['issue_date'] == True].iterrows():
            # Get the station and the issue date
            # station = row[station_col]
            issue_date = row['Date']

            # Get the discharge values for the station and the issue date
            discharge_values = data_df[  # (data_df[station_col] == station) &
                (data_df['Date'] >= (issue_date - pd.DateOffset(days=3)).date()) &
                (data_df['Date'] < issue_date)][discharge_col]

            # Sum up the discharge values
            discharge_sum = discharge_values.sum()

            # Get the index of the issue date
            issue_date_index = data_df[data_df['Date'] == issue_date].index[0]

            # Store the discharge sum
            data_df.loc[issue_date_index, 'discharge_sum'] = discharge_sum

        # Loop over each issue_date = True in data_df
        for index, row in data_df[data_df['issue_date'] == True].iterrows():
            # Get the station and the forecast date
            # station = row[station_col]
            forecast_date = row['Date']

            # Get the discharge values for the station and the forecast date
            discharge_values = data_df[  # (data_df[station_col] == station) &
                (data_df['Date'] > forecast_date) &
                (data_df['Date'] <= (forecast_date + pd.DateOffset(days=5)).date())][discharge_col]

            # Calculate the average discharge
            discharge_avg = discharge_values.mean(skipna=True)

            # Get the index of the forecast date
            forecast_date_index = data_df[data_df['Date'] == forecast_date].index[0]

            # Store the discharge average
            data_df.loc[forecast_date_index, 'discharge_avg'] = discharge_avg

        return data_df

    # Test if the input data contains the required columns
    if not all(column in data_df.columns for column in [datetime_col, station_col, discharge_col]):
        raise ValueError(f'DataFrame is missing one or more required columns: {datetime_col, station_col, discharge_col}')

    # Apply the calculation function to each group based on the 'station' column
    modified_data = data_df.groupby(station_col).apply(
        apply_calculation, datetime_col=datetime_col, discharge_col=discharge_col)

    return modified_data


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
                 perc_norm=-10000.0, qdanger=-10000.0, slope=-10000.0,
                 intercept=-10000.0, delta=-10000.0):
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
        self.qnorm = qnorm
        self.perc_norm = perc_norm
        self.qdanger = qdanger
        self.slope = slope
        self.intercept = intercept
        self.delta = delta

    def __repr__(self):
        """
        Returns a string representation of the Site object.

        Returns:
            str: The site code.
        """
        return self.code

    @classmethod
    def from_df_calculate_forecast(cls, site, pentad: str, df: pd.DataFrame):
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
    def from_df_get_norm_discharge(cls, site, pentad: str, df: pd.DataFrame):
        '''
        Get norm discharge from DataFrame.

        Args:
            site (Site): The site object to get norm discharge for.
            pentad_in_year (str): The pentad of the year to get norm discharge for.
            df (pd.DataFrame): The DataFrame containing the norm discharge data.

        Returns:
            str: The norm discharge value.
        '''
        try:
            # Test that df contains columns 'Code' and 'pentad_in_year'
            if not all(column in df.columns for column in ['Code', 'pentad_in_year']):
                raise ValueError(f'DataFrame is missing one or more required columns: {"Code", "pentad_in_year"}')

            # Convert pentad to float
            pentad = float(pentad)

            # Also convert the column 'pentad_in_year' to float
            df['pentad_in_year'] = df['pentad_in_year'].astype(float)

            # Get the norm discharge for the site
            qnorm = df[(df['Code'] == site.code) & (df['pentad_in_year'] == pentad)]['discharge_avg'].values[0]

            # Write the norm discharge value to self.qnorm as string
            site.qnorm = round_discharge_trad_bulletin(qnorm)

            # Return the norm discharge value
            return qnorm
        except Exception as e:
            print(f'Error {e}. Returning " ".')
            return " "

    @classmethod
    def from_df_get_predictor(cls, site, df: pd.DataFrame, predictor_dates):
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

            # Test that df contains columns 'Code' 'discharge_sum' and 'Date'
            if not all(column in df.columns for column in ['Code', 'discharge_sum', 'Date']):
                raise ValueError(f'DataFrame is missing one or more required columns: {"Code", "discharge_sum", "Date"}')

            # Get the predictor for the site
            predictor = df[(df['Code'] == site.code) & (df['Date'].isin(predictor_dates))]['discharge_sum'].mean(skipna=True)

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

        print(site.fc_qmin, site.fc_qmax)

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
            print(f'    Note: No dangerous discharge for site {site.code} in DB. Returning " ".')
            site.qdanger = " "
            return " "

    @classmethod
    def from_DB_get_predictor(cls, sdk, site, dates, lagdays=20):
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

# endregion
