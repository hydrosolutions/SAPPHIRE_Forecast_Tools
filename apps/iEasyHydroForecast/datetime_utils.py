"""
Standardized datetime utilities for SAPPHIRE Forecast Tools

This module provides consistent datetime parsing, formatting, and conversion 
functions to ensure YYYY-MM-DD format is used throughout the project and 
to prevent server-side datetime format issues.

Author: SAPPHIRE Forecast Tools Team
Date: 2025-01-29
"""

import datetime as dt
import pandas as pd
import logging
from typing import Union, Optional, List

logger = logging.getLogger(__name__)

# Standard date format for the entire project
STANDARD_DATE_FORMAT = '%Y-%m-%d'
STANDARD_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

def to_standard_date_string(date_obj: Union[dt.date, dt.datetime, str, pd.Timestamp]) -> str:
    """
    Convert any date-like object to standard YYYY-MM-DD string format.
    
    Args:
        date_obj: Date object to convert (datetime.date, datetime.datetime, 
                 string, or pandas.Timestamp)
    
    Returns:
        str: Date in YYYY-MM-DD format
        
    Raises:
        ValueError: If the input cannot be converted to a date
        
    Examples:
        >>> to_standard_date_string(dt.date(2023, 1, 15))
        '2023-01-15'
        >>> to_standard_date_string('2023-01-15')
        '2023-01-15'
        >>> to_standard_date_string(pd.Timestamp('2023-01-15'))
        '2023-01-15'
    """
    try:
        if isinstance(date_obj, str):
            # Try to parse string first
            parsed_date = parse_date_string(date_obj)
            return parsed_date.strftime(STANDARD_DATE_FORMAT)
        elif isinstance(date_obj, dt.datetime):
            return date_obj.strftime(STANDARD_DATE_FORMAT)
        elif isinstance(date_obj, dt.date):
            return date_obj.strftime(STANDARD_DATE_FORMAT)
        elif isinstance(date_obj, pd.Timestamp):
            return date_obj.strftime(STANDARD_DATE_FORMAT)
        else:
            raise ValueError(f"Unsupported date type: {type(date_obj)}")
    except Exception as e:
        raise ValueError(f"Could not convert {date_obj} to standard date format: {e}")

def parse_date_string(date_str: str) -> dt.date:
    """
    Parse a date string using common formats, returning a datetime.date object.
    Prioritizes YYYY-MM-DD format but handles various input formats robustly.
    
    Args:
        date_str: Date string to parse
        
    Returns:
        datetime.date: Parsed date object
        
    Raises:
        ValueError: If the string cannot be parsed as a date
        
    Examples:
        >>> parse_date_string('2023-01-15')
        datetime.date(2023, 1, 15)
        >>> parse_date_string('15/01/2023')
        datetime.date(2023, 1, 15)
        >>> parse_date_string('2023-01-15 12:30:45')
        datetime.date(2023, 1, 15)
    """
    if not isinstance(date_str, str):
        raise ValueError(f"Expected string input, got {type(date_str)}")
    
    date_str = date_str.strip()
    
    # Prioritize standard format first
    date_formats = [
        STANDARD_DATE_FORMAT,     # 2023-01-15 (preferred)
        '%Y/%m/%d',              # 2023/01/15
        '%d-%m-%Y',              # 15-01-2023 (European)
        '%d/%m/%Y',              # 15/01/2023 (European)
        '%m-%d-%Y',              # 01-15-2023 (US)
        '%m/%d/%Y',              # 01/15/2023 (US)
        STANDARD_DATETIME_FORMAT, # 2023-01-15 12:30:45
        '%Y/%m/%d %H:%M:%S',     # 2023/01/15 12:30:45
        '%d-%m-%Y %H:%M:%S',     # 15-01-2023 12:30:45
        '%d/%m/%Y %H:%M:%S',     # 15/01/2023 12:30:45
    ]
    
    for date_format in date_formats:
        try:
            parsed_datetime = dt.datetime.strptime(date_str, date_format)
            return parsed_datetime.date()
        except ValueError:
            continue
    
    # If all formats fail, try pandas to_datetime as fallback
    try:
        parsed_pd = pd.to_datetime(date_str, errors='raise')
        return parsed_pd.date()
    except Exception:
        pass
    
    raise ValueError(f"Could not parse date string: '{date_str}'. Expected format: YYYY-MM-DD")

def parse_datetime_string(datetime_str: str) -> dt.datetime:
    """
    Parse a datetime string using common formats, returning a datetime.datetime object.
    
    Args:
        datetime_str: Datetime string to parse
        
    Returns:
        datetime.datetime: Parsed datetime object
        
    Raises:
        ValueError: If the string cannot be parsed as a datetime
    """
    if not isinstance(datetime_str, str):
        raise ValueError(f"Expected string input, got {type(datetime_str)}")
    
    datetime_str = datetime_str.strip()
    
    datetime_formats = [
        STANDARD_DATETIME_FORMAT, # 2023-01-15 12:30:45 (preferred)
        STANDARD_DATE_FORMAT,     # 2023-01-15 (assume 00:00:00)
        '%Y/%m/%d %H:%M:%S',     # 2023/01/15 12:30:45
        '%Y/%m/%d',              # 2023/01/15 (assume 00:00:00)
        '%d-%m-%Y %H:%M:%S',     # 15-01-2023 12:30:45
        '%d-%m-%Y',              # 15-01-2023 (assume 00:00:00)
        '%d/%m/%Y %H:%M:%S',     # 15/01/2023 12:30:45
        '%d/%m/%Y',              # 15/01/2023 (assume 00:00:00)
    ]
    
    for datetime_format in datetime_formats:
        try:
            return dt.datetime.strptime(datetime_str, datetime_format)
        except ValueError:
            continue
    
    # Fallback to pandas
    try:
        parsed_pd = pd.to_datetime(datetime_str, errors='raise')
        return parsed_pd.to_pydatetime()
    except Exception:
        pass
    
    raise ValueError(f"Could not parse datetime string: '{datetime_str}'. Expected format: YYYY-MM-DD HH:MM:SS")

def ensure_date_object(date_input: Union[dt.date, dt.datetime, str, pd.Timestamp]) -> dt.date:
    """
    Ensure the input is converted to a datetime.date object.
    
    Args:
        date_input: Any date-like input
        
    Returns:
        datetime.date: Date object
        
    Raises:
        ValueError: If the input cannot be converted to a date
    """
    try:
        if isinstance(date_input, dt.date) and not isinstance(date_input, dt.datetime):
            return date_input
        elif isinstance(date_input, dt.datetime):
            return date_input.date()
        elif isinstance(date_input, str):
            return parse_date_string(date_input)
        elif isinstance(date_input, pd.Timestamp):
            return date_input.date()
        else:
            raise ValueError(f"Unsupported date input type: {type(date_input)}")
    except Exception as e:
        raise ValueError(f"Could not convert {date_input} to date object: {e}")

def ensure_datetime_object(datetime_input: Union[dt.date, dt.datetime, str, pd.Timestamp]) -> dt.datetime:
    """
    Ensure the input is converted to a datetime.datetime object.
    
    Args:
        datetime_input: Any datetime-like input
        
    Returns:
        datetime.datetime: Datetime object
        
    Raises:
        ValueError: If the input cannot be converted to a datetime
    """
    try:
        if isinstance(datetime_input, dt.datetime):
            return datetime_input
        elif isinstance(datetime_input, dt.date):
            return dt.datetime.combine(datetime_input, dt.time())
        elif isinstance(datetime_input, str):
            return parse_datetime_string(datetime_input)
        elif isinstance(datetime_input, pd.Timestamp):
            return datetime_input.to_pydatetime()
        else:
            raise ValueError(f"Unsupported datetime input type: {type(datetime_input)}")
    except Exception as e:
        raise ValueError(f"Could not convert {datetime_input} to datetime object: {e}")

def safe_date_comparison(date1: Union[dt.date, dt.datetime, str, pd.Timestamp], 
                        date2: Union[dt.date, dt.datetime, str, pd.Timestamp],
                        comparison: str = 'le') -> bool:
    """
    Safely compare two date-like objects by converting them to standard format first.
    
    Args:
        date1: First date to compare
        date2: Second date to compare  
        comparison: Type of comparison ('le', 'ge', 'lt', 'gt', 'eq', 'ne')
        
    Returns:
        bool: Result of the comparison
        
    Raises:
        ValueError: If dates cannot be parsed or comparison type is invalid
    """
    try:
        d1 = ensure_date_object(date1)
        d2 = ensure_date_object(date2)
        
        if comparison == 'le':
            return d1 <= d2
        elif comparison == 'ge':
            return d1 >= d2
        elif comparison == 'lt':
            return d1 < d2
        elif comparison == 'gt':
            return d1 > d2
        elif comparison == 'eq':
            return d1 == d2
        elif comparison == 'ne':
            return d1 != d2
        else:
            raise ValueError(f"Invalid comparison type: {comparison}. Use 'le', 'ge', 'lt', 'gt', 'eq', or 'ne'")
    except Exception as e:
        raise ValueError(f"Could not compare dates {date1} and {date2}: {e}")

def parse_dates_robust_pandas(date_series: pd.Series, column_name: str = "date") -> pd.Series:
    """
    Robustly parse a pandas Series of dates, enforcing YYYY-MM-DD output format.
    This replaces the existing parse_dates_robust function with enhanced error handling.
    
    Args:
        date_series: Pandas Series containing date strings or objects
        column_name: Name of the column for logging purposes
        
    Returns:
        pd.Series: Series with parsed datetime objects
        
    Raises:
        ValueError: If no date format could be successfully parsed
    """
    # If already datetime, ensure consistent format
    if pd.api.types.is_datetime64_any_dtype(date_series):
        logger.debug(f"Date parsing for {column_name}: already datetime format, no conversion needed")
        return date_series
    
    # Try to parse using our standardized function
    try:
        # Convert each value using our robust parser
        parsed_values = []
        failed_indices = []
        
        for idx, value in date_series.items():
            try:
                if pd.isna(value):
                    parsed_values.append(pd.NaT)
                else:
                    parsed_date = parse_date_string(str(value))
                    parsed_values.append(pd.Timestamp(parsed_date))
            except Exception:
                parsed_values.append(pd.NaT)
                failed_indices.append(idx)
        
        result_series = pd.Series(parsed_values, index=date_series.index)
        
        # Calculate success rate
        success_rate = result_series.notna().sum() / len(result_series)
        
        if success_rate > 0.8:  # 80% success rate threshold
            if failed_indices:
                logger.warning(f"Date parsing for {column_name}: {len(failed_indices)} dates could not be parsed. Indices: {failed_indices[:10]}{'...' if len(failed_indices) > 10 else ''}")
            else:
                logger.debug(f"Date parsing for {column_name}: all dates parsed successfully")
            return result_series
        else:
            raise ValueError(f"Date parsing success rate too low: {success_rate:.2%}")
            
    except Exception as e:
        logger.error(f"Robust date parsing failed for {column_name}: {e}")
        # Fall back to pandas default
        try:
            fallback_result = pd.to_datetime(date_series, errors='coerce')
            success_rate = fallback_result.notna().sum() / len(fallback_result)
            if success_rate > 0.8:
                logger.warning(f"Date parsing for {column_name}: used pandas fallback with {success_rate:.2%} success rate")
                return fallback_result
        except Exception:
            pass
        
        raise ValueError(f"Could not parse dates in {column_name}. Please ensure dates are in YYYY-MM-DD format or other standard formats.")


def get_last_day_of_month_vectorized(date_series: pd.Series) -> pd.Series:
    """
    Vectorized function to get the last day of each month for a pandas Series of dates.
    This is much faster than applying a function row by row.
    
    Args:
        date_series: pandas Series containing datetime values
        
    Returns:
        pandas Series with the last day of each month
        
    Examples:
        >>> dates = pd.Series([pd.Timestamp('2023-01-15'), pd.Timestamp('2023-02-10')])
        >>> get_last_day_of_month_vectorized(dates)
        0   2023-01-31
        1   2023-02-28
        dtype: datetime64[ns]
    """
    try:
        # Ensure we have datetime64 type
        if not pd.api.types.is_datetime64_any_dtype(date_series):
            date_series = pd.to_datetime(date_series)
        
        # Use pandas built-in functionality for efficiency
        # Get the first day of next month, then subtract one day
        next_month_first = (date_series + pd.offsets.MonthBegin(1)).dt.normalize()
        last_day_current_month = next_month_first - pd.Timedelta(days=1)
        
        return last_day_current_month
        
    except Exception as e:
        logger.error(f"Error in vectorized last day of month calculation: {e}")
        raise ValueError(f"Could not calculate last day of month for series: {e}") from e


def get_today_string() -> str:
    """
    Get today's date as a standardized YYYY-MM-DD string.
    
    Returns:
        str: Today's date in YYYY-MM-DD format
    """
    return dt.date.today().strftime(STANDARD_DATE_FORMAT)

def get_yesterday_string() -> str:
    """
    Get yesterday's date as a standardized YYYY-MM-DD string.
    
    Returns:
        str: Yesterday's date in YYYY-MM-DD format
    """
    yesterday = dt.date.today() - dt.timedelta(days=1)
    return yesterday.strftime(STANDARD_DATE_FORMAT)