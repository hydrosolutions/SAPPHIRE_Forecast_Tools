import datetime as dt
import pandas as pd
from typing import Union

# --- Helper functions ---
# region helper functions

def get_pentad_first_day_of_year(date_str):
    """
    Returns the first day of the pentad of the year for a given date string.

    Args:
        date_str (str): A string representing a date in the format 'YYYY-MM-DD'.

    Returns:
        str: A string representing the first day of the pentad for the input
            date string, in the format 'D'.

        If the input date string is not a valid date, returns None.
    """
    try:
        # parse the input date string into a datetime object
        date = dt.datetime.strptime(date_str, '%Y-%m-%d').date()

    except ValueError:
        # return None if the input is not a valid date
        return None

    # Test if the date is using the Gregorian calendar
    if not is_gregorian_date(date_str):
        # return None if the input is not using the Gregorian calendar
        return None

    # calculate the pentad number
    pentad = (date.day - 1) // 5 + 1

    # make sure the pentad number is not larger than 6
    pentad = min(pentad, 6)

    # calculate the first day of the pentad
    first_day = 5 * (pentad - 1) + 1

    # Get the date of the first day of the pentad
    date_first_day = date.replace(day=first_day)

    # Get day of the year from the date
    day_of_year = date_first_day.timetuple().tm_yday

    # return the first day of the pentad as a string
    return str(day_of_year)

def is_gregorian_date(date: Union[str, dt.datetime]) -> bool:
    """
    Check if a date string is using the Gregorian calendar.

    Parameters:
        date (str or datetime.datetime): A string representing a date in the
        format 'YYYY-MM-DD' or a datetime.

    Returns:
        bool: True if the date is using the Gregorian calendar, False otherwise.

    Raises:
        ValueError: If the input date is not valid or is not using the Gregorian calendar.

    Examples:
        >>> is_gregorian_date('2022-05-15')
        True
        >>> is_gregorian_date('1581-12-31')
        False
        >>> is_gregorian_date('not a date')
        False
    """
    try:
        # Return None if the input is an integer
        if isinstance(date, int):
            raise ValueError('Input is an integer and not a date or datestring.')

        # Convert the input to a datetime object if it's a string
        if isinstance(date, str):
            try:
                date = dt.datetime.strptime(date, '%Y-%m-%d').date()
            except ValueError:
                raise ValueError('Input is not a valid date or datestring.')

        # check if the year is before 1582
        if date.year < 1582:
            # raise a ValueError if the year is before 1582
            raise ValueError('Date is not using the Gregorian calendar')
        if date.year > 2099:
            # raise a ValueError if the year is after 2099
            raise ValueError('Date is not using the Gregorian calendar')

        # return True if the date is using the Gregorian calendar
        return True

    except ValueError:
        return False

def add_pentad_in_year_column(df, date_col='Date'):
    """
    Add a 'pentad' column to a pandas DataFrame with a 'Date' column.

    Parameters:
        df (pandas.DataFrame): A pandas DataFrame with a 'date' column
            containing either datetime objects or datetime strings.
        date_col (str): The name of the column containing the dates. Defauls to 'Date'.

    Returns:
        pandas.DataFrame: The input DataFrame with a new 'pentad' column added.
            Pendads in a year can go from 1 for January 1 to 5 to 72 for
            December 26 to 31.

    Examples:
        >>> df = pd.DataFrame({'Date': ['2022-05-15', '2022-05-16']})
        >>> add_pentad_column(df)
               Date pentad
        0 2022-05-15     27
        1 2022-05-16     28
    """
    try:
        # Check if there is a date_col column in the DataFrame
        if date_col not in df.columns:
            # Return an error if there is no date_col column
            raise ValueError(f'DataFrame does not have a \'Date\' column called {date_col}')

        # Loop through each row in the 'Date' column and check if the date is valid
        for date in df[date_col]:
            if not is_gregorian_date(date):
                # Return an error if there is an invalid date
                raise ValueError('DataFrame contains invalid date(s)')

        # Convert the 'Date' column to a pandas Series of datetime objects
        date_series = pd.to_datetime(df[date_col], errors='coerce')

        # Get the day of the month for each date
        day_series = date_series.dt.day

        # Get the month of the year for each date
        month_series = date_series.dt.month

        # Calculate the pentad of the month using integer division
        pentad_series = ((day_series - 1) // 5 + 1).clip(upper=6)

        # If month is 1 then pentad is 1 to 6, if month is 2 then pentad is 7 to 12, etc.
        # Add the month number to the pentad number to get the pentad of the year
        pentad_in_year_series = (month_series - 1) * 6 + pentad_series

        # Add the 'pentad' column to the DataFrame
        df['pentad'] = pentad_in_year_series.astype(str)

        return df
    except ValueError as e:
        # Raise an error if the input is not a valid date
        raise ValueError('Invalid date') from e

def add_decad_in_year_column(df):
    """
    Add a 'decad_in_year' column to a pandas DataFrame with a 'Date' column.

    Parameters:
        df (pandas.DataFrame): A pandas DataFrame with a 'Date' column
            containing either datetime objects or datetime strings.

    Returns:
        pandas.DataFrame: The input DataFrame with a new 'decad_in_year' column added.
            Pendads in a year can go from 1 for January 1 to 10 to 36 for
            December 21 to 31.

    Examples:
        >>> df = pd.DataFrame({'Date': ['2022-05-15', '2022-05-16']})
        >>> add_decad_in_year_column(df)
               Date decad
        0 2022-05-15     14
        1 2022-05-26     15
    """
    try:
        # Check if there is a 'Date' column in the DataFrame
        if 'Date' not in df.columns:
            # Return an error if there is no 'Date' column
            raise ValueError('DataFrame does not have a \'Date\' column')

        # Loop through each row in the 'Date' column and check if the date is valid
        for date in df['Date']:
            if not is_gregorian_date(date):
                # Return an error if there is an invalid date
                raise ValueError('DataFrame contains invalid date(s)')

        # Convert the 'Date' column to a pandas Series of datetime objects
        date_series = pd.to_datetime(df['Date'], errors='coerce')

        # Get the day of the month for each date
        day_series = date_series.dt.day

        # Get the month of the year for each date
        month_series = date_series.dt.month

        # Calculate the decad of the year
        decad_series = ((day_series - 1) // 10 + 1).clip(upper=3)
        decad_in_year_series = (month_series - 1) * 3 + decad_series

        # Add the 'decad_in_year' column to the DataFrame
        df['decad_in_year'] = decad_in_year_series.astype(str)

        return df
    except ValueError as e:
        # Raise an error if the input is not a valid date
        raise ValueError('Invalid date') from e

# endregion

# --- Tag functions ---
# region tag get_value_fn function

def get_pentad(date):
    """
    Get the pentad of the month for a given date.

    Parameters:
        date (str or datetime.datetime): A string or datetime representing a
            date (string should be in the format 'YYYY-MM-DD').

    Returns:
        str: A string representing the pentad of the month, or None if the
            input is not a valid date or is not using the Gregorian calendar.

    Examples:
        >>> get_pentad('2022-05-15')
        '3'
        >>> get_pentad('not a date')
        None
        >>> get_pentad('1581-12-31')
        None
    """
    try:
        # Convert the input to a datetime object if it's a string
        if isinstance(date, str):
            date = dt.datetime.strptime(date, '%Y-%m-%d').date()

        # Test if the date is using the Gregorian calendar
        if not is_gregorian_date(date):
            # return None if the input is not using the Gregorian calendar
            return None

        # calculate the pentad number
        pentad = min((date.day - 1) // 5 + 1, 6)

        # return the pentad number as a string
        return str(pentad)

    except ValueError:
        # return None if the input is not a valid date
        return None

def get_decad_in_month(date):
    """
    Get the decad of the month for a given date.

    Parameters:
        date (str or datetime.datetime): A string or datetime representing a
            date (string should be in the format 'YYYY-MM-DD').

    Returns:
        str: A string representing the decad of the month, or None if the
            input is not a valid date or is not using the Gregorian calendar.

    Examples:
        >>> get_pentad('2022-05-10')
        '2'
        >>> get_pentad('not a date')
        None
        >>> get_pentad('1581-12-31')
        None
    """
    try:
        # Convert the input to a datetime object if it's a string
        if isinstance(date, str):
            date = dt.datetime.strptime(date, '%Y-%m-%d').date()

        # Test if the date is using the Gregorian calendar
        if not is_gregorian_date(date):
            # return None if the input is not using the Gregorian calendar
            return None

        # calculate the decad number
        decad = min((date.day - 1) // 10 + 1, 3)

        # return the decad number as a string
        return str(decad)

    except ValueError:
        # return None if the input is not a valid date
        return None

def get_predictor_decad(date):

    current_decad = get_decad_in_month(date)
    if current_decad == '1':
        predictor_decad = '3'
    elif current_decad == '2':
        predictor_decad = '1'
    else:
        predictor_decad = '2'

    return predictor_decad

def get_pentad_in_year(date):
    """
    Get the pentad of the year for a given date.

    Parameters:
        date (str or datetime.datetime): A string or datetime representing a
            date (string should be in the format 'YYYY-MM-DD').

    Returns:
        str: A string representing the pentad of the month, or None if the
            input is not a valid date or is not using the Gregorian calendar.

    Examples:
        >>> get_pentad_in_year('2022-05-15')
        '27'
        >>> get_pentad('not a date')
        None
        >>> get_pentad('1581-12-31')
        None
    """
    try:
        # Convert the input to a datetime object if it's a string
        if isinstance(date, str):
            date = dt.datetime.strptime(date, '%Y-%m-%d').date()

        # Test if the date is using the Gregorian calendar
        if not is_gregorian_date(date):
            # return None if the input is not using the Gregorian calendar
            return None

        # calculate the pentad number
        pentad = min((date.day - 1) // 5 + 1, 6)
        pentad_in_year = (date.month - 1) * 6 + pentad

        # return the pentad number as a string
        return str(pentad_in_year)

    except ValueError:
        # return None if the input is not a valid date
        return None

def get_decad_in_year(date):
    """
    Get the decad of the year for a given date.

    Parameters:
        date (str or datetime.datetime): A string or datetime representing a
            date (string should be in the format 'YYYY-MM-DD').

    Returns:
        str: A string representing the decad of the month, or None if the
            input is not a valid date or is not using the Gregorian calendar.

    Examples:
        >>> get_decad_in_year('2022-05-15')
        '14'
        >>> get_decad_in_year('2022-12-31')
        '36'
        >>> get_decad_in_year('not a date')
        None
        >>> get_decad_in_year('1581-12-31')
        None
    """
    try:
        # Convert the input to a datetime object if it's a string
        if isinstance(date, str):
            date = dt.datetime.strptime(date, '%Y-%m-%d').date()

        # Test if the date is using the Gregorian calendar
        if not is_gregorian_date(date):
            # return None if the input is not using the Gregorian calendar
            return None

        # calculate the decad number
        decad = min((date.day - 1) // 10 + 1, 3)
        decad_in_year = (date.month - 1) * 3 + decad

        # return the pentad number as a string
        return str(decad_in_year)

    except ValueError:
        # return None if the input is not a valid date
        return None

def get_basin_name_short_term_forecast(site):
    """
    Returns a string with the basin name as adjective plus basin.

    Args:
        site (fl.Site): A site object with attributes basin and code.

    Returns:
        str: A string representing the name of the basin as adjective plus basin.
    """
    string = site.basin
    #code = site.code

    # For short-term forecasts (pentad & decad), we want to return basin Naryn
    # for the tributaries to Toktogul inflow.
    # This concerns the following sites:
    #naryn_sites = ["16059", "16096", "16100", "16936"]
    # This special case is handeled earlier, when the sites are read.

    if string == "Чу":
        output = "Чуйский бассейн"
    elif string == "Талас":
        output = "Таласский бассейн"
    elif (string == "Иссык-Куль") or (string == "Иссык Куль"):
        output = "Иссык-Кульский бассейн"
    elif string == "Нарын":
        output = "Нарынский бассейн"
    elif (string == "Сырдарья") or (string == "Сыр-Дарья"):
        #if code in naryn_sites:
        #    output = "Нарынский бассейн"
        #else:
        output = "Сырдарьинский бассейн"
    elif string == "Кара-Дарья":
        output = "Кара-Дарьинский бассейн"
    else:
        # raise an error if the basin is not recognized
        raise ValueError('Basin not recognized')

    return output

def get_pentad_first_day(date_str):
    """
    Returns the first day of the pentad of the month for a given date string.

    Args:
        date_str (str): A string representing a date in the format 'YYYY-MM-DD'.

    Returns:
        str: A string representing the first day of the pentad for the input
            date string, in the format 'D'.

        If the input date string is not a valid date, returns None.
    """
    try:
        # parse the input date string into a datetime object
        date = dt.datetime.strptime(date_str, '%Y-%m-%d').date()

    except ValueError:
        # return None if the input is not a valid date
        return None

    # Test if the date is using the Gregorian calendar
    if not is_gregorian_date(date_str):
        # return None if the input is not using the Gregorian calendar
        return None

    # calculate the pentad number
    pentad = (date.day - 1) // 5 + 1

    # make sure the pentad number is not larger than 6
    pentad = min(pentad, 6)

    # calculate the first day of the pentad
    first_day = 5 * (pentad - 1) + 1

    # return the first day of the pentad as a string
    return str(first_day)

def get_pentad_last_day(date_str):
    """
    Returns the last day of the pentad of the month for a given date string.

    Args:
        date_str (str): A string representing a date in the format 'YYYY-MM-DD'.

    Returns:
        str: A string representing the last day of the pentad for the input
            date string, in the format 'D'.

        If the input date string is not a valid date, returns None.
    """
    try:
        # parse the input date string into a datetime object
        date = dt.datetime.strptime(date_str, '%Y-%m-%d').date()

    except ValueError:
        # return None if the input is not a valid date
        return None

    # Test if the date is using the Gregorian calendar
    if not is_gregorian_date(date_str):
        # return None if the input is not using the Gregorian calendar
        return None

    # The day 28 exists in every month. 4 days later, it's always next month
    next_month = date.replace(day=1) + dt.timedelta(days=31)
    # subtracting the number of the current day brings us back one month
    last_day_in_month = next_month - dt.timedelta(days=next_month.day)

    # calculate the pentad number
    pentad = (date.day - 1) // 5 + 1

    # calculate the last day of the pentad
    last_day = 5 * pentad

    # Make sure that last_day is not larger than the number of days in the month
    last_day = min(last_day, last_day_in_month.day)

    # return the first day of the pentad as a string
    return str(last_day)

def get_year(date_str):
    """
    Returns the year number for a given date string.

    Args:
        date_str (str): A string representing a date in the format 'YYYY-MM-DD'.

    Returns:
        str: A string representing the year number for the input date string, in
            the format 'YYYY'.

        If the input date string is not a valid date, returns None.
    """
    try:
        # parse the input date string into a datetime object
        date = dt.datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        # return None if the input is not a valid date
        return None

    # Test if the date is using the Gregorian calendar
    if not is_gregorian_date(date_str):
        # return None if the input is not using the Gregorian calendar
        return None

    # calculate the year number
    year = date.year

    # return the year number as a string
    return str(year)

def get_date_for_pentad(pentad_in_year, year=dt.datetime.now().year):
    """
    Get the date for a given pentad in the year.

    Parameters:
        pentad_in_year (int): The pentad number within the year (1-72).
        year (int): The year for which the date is needed. Defaults to the current year.

    Returns:
        str: A string representing the date for the input pentad in the format 'YYYY-MM-DD',
             or None if the input is not valid.
             
    Examples:
        >>> get_date_for_pentad(1)
        'YYYY-01-01'
        >>> get_date_for_pentad(72)
        'YYYY-12-26'
    """
    try:
        # Calculate the month and the pentad within the month
        month = (pentad_in_year - 1) // 6 + 1
        pentad_in_month = (pentad_in_year - 1) % 6 + 1

        # Calculate the first day of the pentad
        first_day_of_pentad = 5 * (pentad_in_month - 1) + 1
        
        # Ensure the calculated day is valid within the month
        days_in_month = (dt.date(year, month, 1) + dt.timedelta(days=31)).replace(day=1) - dt.timedelta(days=1)
        if first_day_of_pentad > days_in_month.day:
            first_day_of_pentad = days_in_month.day
        
        # Create the date
        date = dt.date(year, month, first_day_of_pentad)
        
        # Return the date as a string in 'YYYY-MM-DD' format
        return date.strftime('%Y-%m-%d')

    except Exception as e:
        # Return None if there's an error (invalid pentad, etc.)
        print(f"Error: {e}")
        return None

def get_month_str_latin(date_str):
    """
    Returns the name of the month for a given date string, in Latin number.

    Args:
        date_str (str): A string representing a date in the format 'YYYY-MM-DD'.

    Returns:
        str: A string representing the name of the month for the input date
            string, in Latin number.

        If the input date string is not a valid date, returns None.
    """
    try:
        # parse the input date string into a datetime object
        date = dt.datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        # return None if the input is not a valid date
        return None

    # Test if the date is using the Gregorian calendar
    if not is_gregorian_date(date_str):
        # return None if the input is not using the Gregorian calendar
        return None

    # calculate the month number
    month = date.month

    month_str = ''
    if month == 1:
        month_str = 'I'
    elif month == 2:
        month_str = 'II'
    elif month == 3:
        month_str = 'III'
    elif month == 4:
        month_str = 'IV'
    elif month == 5:
        month_str = 'V'
    elif month == 6:
        month_str = 'VI'
    elif month == 7:
        month_str = 'VII'
    elif month == 8:
        month_str = 'VIII'
    elif month == 9:
        month_str = 'IX'
    elif month == 10:
        month_str = 'X'
    elif month == 11:
        month_str = 'XI'
    elif month == 12:
        month_str = 'XII'
    else:
        return None

    # return the month name as a string
    return month_str

def get_predcitor_month_latin(date_str):

    current_month = get_month_str_latin(date_str)
    current_decad = get_decad_in_month(date_str)
    if current_decad == '1':
        if current_month == 'I':
            predictor_month = 'XII'
        if current_month == 'II':
            predictor_month = 'I'
        if current_month == 'III':
            predictor_month = 'II'
        if current_month == 'IV':
            predictor_month = 'III'
        if current_month == 'V':
            predictor_month = 'IV'
        if current_month == 'VI':
            predictor_month = 'V'
        if current_month == 'VII':
            predictor_month = 'VI'
        if current_month == 'VIII':
            predictor_month = 'VII'
        if current_month == 'IX':
            predictor_month = 'VIII'
        if current_month == 'X':
            predictor_month = 'IX'
        if current_month == 'XI':
            predictor_month = 'X'
        if current_month == 'XII':
            predictor_month = 'XI'
    else:
        predictor_month = current_month

    return predictor_month

def get_month_str_case1(date_str):
    """
    Returns the name of the month for a given date string, in Russian case 1
        (nominative).

    Args:
        date_str (str): A string representing a date in the format 'YYYY-MM-DD'.

    Returns:
        str: A string representing the name of the month for the input date
            string, in Russian.

        If the input date string is not a valid date, returns None.
    """

    try:
        # parse the input date string into a datetime object
        date = dt.datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        # return None if the input is not a valid date
        return None

    # Test if the date is using the Gregorian calendar
    if not is_gregorian_date(date_str):
        # return None if the input is not using the Gregorian calendar
        return None

    # calculate the month number
    month = date.month

    month_str = ''
    if month == 1:
        month_str = 'январь'
    elif month == 2:
        month_str = 'февраль'
    elif month == 3:
        month_str = 'март'
    elif month == 4:
        month_str = 'апрель'
    elif month == 5:
        month_str = 'май'
    elif month == 6:
        month_str = 'июнь'
    elif month == 7:
        month_str = 'июль'
    elif month == 8:
        month_str = 'август'
    elif month == 9:
        month_str = 'сентябрь'
    elif month == 10:
        month_str = 'октябрь'
    elif month == 11:
        month_str = 'ноябрь'
    elif month == 12:
        month_str = 'декабрь'
    else:
        return None

    # return the month name as a string
    return month_str

def get_month_str_case2(date_str):
    """
    Returns the name of the month for a given date string, in Russian, in the
    second case.

    Args:
        date_str (str): A string representing a date in the format 'YYYY-MM-DD'.

    Returns:
        str: A string representing the name of the month for the input date
            string, in Russian, in the second case.

        If the input date string is not a valid date, returns None.
    """
    try:
        # parse the input date string into a datetime object
        date = dt.datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        # return None if the input is not a valid date
        return None

    # Test if the date is using the Gregorian calendar
    if not is_gregorian_date(date_str):
        # return None if the input is not using the Gregorian calendar
        return None

    # calculate the month number
    month = date.month

    month_str = ''
    if month == 1:
        month_str = 'января'
    elif month == 2:
        month_str = 'февраля'
    elif month == 3:
        month_str = 'марта'
    elif month == 4:
        month_str = 'апреля'
    elif month == 5:
        month_str = 'мая'
    elif month == 6:
        month_str = 'июня'
    elif month == 7:
        month_str = 'июля'
    elif month == 8:
        month_str = 'августа'
    elif month == 9:
        month_str = 'сентября'
    elif month == 10:
        month_str = 'октября'
    elif month == 11:
        month_str = 'ноября'
    elif month == 12:
        month_str = 'декабря'
    else:
        return None

    # return the month name as a string
    return month_str

def get_month_str_case2_viz(_, date):
    """
    Returns the name of the month for a given date string, in Russian, in the
    second case.

    Args:
        _ (function): A function that returns the translated string for a given
            key.
        date (str or date): A string representing a date in the format 'YYYY-MM-DD'.

    Returns:
        str: A string representing the name of the month for the input date
            string, in Russian, in the second case.

        If the input date string is not a valid date, returns None.
    """
    # If date is a string, try to parse it to a date
    if isinstance(date, str):
        try:
            date = dt.datetime.strptime(date, '%Y-%m-%d').date()
        except ValueError:
            return None

    # If date is not a date, raise a value error
    if not isinstance(date, dt.date):
        raise ValueError('Input is not a valid date or datestring.')

    # Test if the date is using the Gregorian calendar
    if not is_gregorian_date(date):
        # return None if the input is not using the Gregorian calendar
        return None

    # calculate the month number
    month = date.month

    month_str = ''
    if month == 1:
        month_str = _('January')
    elif month == 2:
        month_str = _('February')
    elif month == 3:
        month_str = _('March')
    elif month == 4:
        month_str = _('April')
    elif month == 5:
        month_str = _('May')
    elif month == 6:
        month_str = _('June')
    elif month == 7:
        month_str = _('July')
    elif month == 8:
        month_str = _('August')
    elif month == 9:
        month_str = _('September')
    elif month == 10:
        month_str = _('October')
    elif month == 11:
        month_str = _('November')
    elif month == 12:
        month_str = _('December')
    else:
        return None

    # return the month name as a string
    return month_str

def get_month_str_abbrev_viz(_, date):
    """
    Returns the name of the month for a given date string, in Russian, in the
    second case.

    Args:
        _ (function): A function that returns the translated string for a given
            key.
        date (str or date): A string representing a date in the format 'YYYY-MM-DD'.

    Returns:
        str: A string representing the name of the month for the input date
            string, in Russian, in the second case.

        If the input date string is not a valid date, returns None.
    """
    # If date is a string, try to parse it to a date
    if isinstance(date, str):
        try:
            date = dt.datetime.strptime(date, '%Y-%m-%d').date()
        except ValueError:
            return None

    # If date is not a date, raise a value error
    if not isinstance(date, dt.date):
        raise ValueError('Input is not a valid date or datestring.')

    # Test if the date is using the Gregorian calendar
    if not is_gregorian_date(date):
        # return None if the input is not using the Gregorian calendar
        return None

    # calculate the month number
    month = date.month

    month_str = ''
    if month == 1:
        month_str = _('Jan')
    elif month == 2:
        month_str = _('Feb')
    elif month == 3:
        month_str = _('Mar')
    elif month == 4:
        month_str = _('Apr')
    elif month == 5:
        month_str = _('May')
    elif month == 6:
        month_str = _('Jun')
    elif month == 7:
        month_str = _('Jul')
    elif month == 8:
        month_str = _('Aug')
    elif month == 9:
        month_str = _('Sep')
    elif month == 10:
        month_str = _('Oct')
    elif month == 11:
        month_str = _('Nov')
    elif month == 12:
        month_str = _('Dec')
    else:
        return None

    # return the month name as a string
    return month_str

def get_river_name(site):
    '''
    Gets the name of the river for a given site.

    Args:
        site (str): A site of Site class

    Return:
        str: A string representing the name of the river for the input site.

        If the input site is not valid, returns None.
    '''
    return site.river_name

def get_site_name(site):
    '''
    Gets the name of the site for a given site.

    Args:
        site (str): A site of Site class

    Return:
        str: A string representing the name of the site for the input site.

        If the input site is not valid, returns None.
    '''
    return site.punkt_name

# endregion
