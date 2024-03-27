import os
import pandas as pd
import numpy as np
import pytest
from unittest.mock import patch, MagicMock
from backend.src import data_processing

import forecast_library as fl

def test_check_database_access_with_files_in_directory(tmpdir):
    # Set up the environment variable
    os.environ["ieasyforecast_daily_discharge_dir"] = str(tmpdir)

    # Create a file in the directory
    with open(os.path.join(os.environ["ieasyforecast_daily_discharge_dir"], "file.txt"), "w") as f:
        f.write("test")

    # Call the function and check the result
    # This will raise an exception because we don't have a real ieh_sdk object,
    # but the function should still return False because there are files in the directory
    with pytest.raises(Exception):
        assert data_processing.check_database_access(None) is False

    # Clean up the environment variable
    os.environ.pop("ieasyforecast_daily_discharge_dir")


def test_read_a_file_that_does_not_exist():
    file_path = 'backend/tests/test_files/12345_doesnotexist.xlsx'
    station = '12345'
    year = 2000
    # Check that call to read_discharge_from_excel_sheet throws ValueError
    with pytest.raises(ValueError):
        data_processing.read_discharge_from_excel_sheet(file_path, station, year)

def test_read_discharge_from_good_excel_sheet():
    # Call read_discharge_from_excel_sheet with test parameters
    file_path = 'backend/tests/test_files/12345_discharge_daily_good_file.xlsx'
    station = '12345'
    year = 2000
    data = data_processing.read_discharge_from_excel_sheet(file_path, station, year)

    # Check that the returned DataFrame has the expected columns and data
    assert 'Date' in data.columns
    assert 'Q_m3s' in data.columns
    assert 'Year' in data.columns
    assert 'Code' in data.columns
    assert data['Code'].unique() == [station]

    # First entry of Date, converted to format %Y-%m-%d, should be 2000-01-01
    assert data['Date'].iloc[0].strftime('%Y-%m-%d') == '2000-01-01'
    # Last entry of Date, converted to format %Y-%m-%d, should be 2000-01-18
    assert data['Date'].iloc[-1].strftime('%Y-%m-%d') == '2023-12-31'
    # First value of Q_m3s, rounded to 1 digit, should be 9.7
    assert data['Q_m3s'].iloc[0].round(1) == 9.7
    # Last value of Q_m3s, rounded to 1 digit, should be no value
    assert pd.isna(data['Q_m3s'].iloc[-1])

def test_read_discharge_from_another_good_excel_sheet():
    # Call read_discharge_from_excel_sheet with test parameters
    file_path = 'backend/tests/test_files/15678_test_data.xlsx'
    station = '15678'
    year = 2018
    data = data_processing.read_discharge_from_excel_sheet(file_path, station, year)

    # Check that the returned DataFrame has the expected columns and data
    assert 'Date' in data.columns
    assert 'Q_m3s' in data.columns
    assert 'Year' in data.columns
    assert 'Code' in data.columns
    assert data['Code'].unique() == [station]

    # First entry of Date, converted to format %Y-%m-%d, should be 2000-01-01
    assert data['Date'].iloc[0].strftime('%Y-%m-%d') == '2018-01-01'
    # Last entry of Date, converted to format %Y-%m-%d, should be 2000-01-18
    assert data['Date'].iloc[-1].strftime('%Y-%m-%d') == '2018-12-31'
    # First value of Q_m3s, rounded to 1 digit, should be 1.2
    assert data['Q_m3s'].iloc[0].round(1) == 1.2
    # Last value of Q_m3s, rounded to 1 digit, should be 1.2
    assert data['Q_m3s'].iloc[0].round(1) == 1.2


def test_read_discharge_from_slash_date_excel_sheet():
    # Call read_discharge_from_excel_sheet with test parameters
    file_path = 'backend/tests/test_files/12345_discharge_daily_slash_date_file.xlsx'
    station = '12345'
    year = 2000
    data = data_processing.read_discharge_from_excel_sheet(file_path, station, year)

    # Check that the returned DataFrame has the expected columns and data
    assert 'Date' in data.columns
    assert 'Q_m3s' in data.columns
    assert 'Year' in data.columns
    assert 'Code' in data.columns
    assert data['Code'].unique() == [station]

    # First entry of Date, converted to format %Y-%m-%d, should be 2000-01-01
    assert data['Date'].iloc[0].strftime('%Y-%m-%d') == '2000-01-01'
    # Last entry of Date, converted to format %Y-%m-%d, should be 2000-01-18
    assert data['Date'].iloc[-1].strftime('%Y-%m-%d') == '2000-01-18'
    # First value of Q_m3s, rounded to 1 digit, should be 9.7
    assert data['Q_m3s'].iloc[0].round(1) == 9.7
    # Last value of Q_m3s, rounded to 1 digit, should be 9.3
    assert data['Q_m3s'].iloc[-1].round(1) == 9.3

def test_read_discharge_from_excel_sheet_with_additional_header_row():
    # Call read_discharge_from_excel_sheet with test parameters
    file_path = 'backend/tests/test_files/12345_discharge_daily_additional_header.xlsx'
    station = '12345'
    year = 2000

    # Call to read_discharge_from_excel_sheet should throw ValueError
    with pytest.raises(ValueError):
        data_processing.read_discharge_from_excel_sheet(file_path, station, year)

def test_sheet_not_found_in_excel_file():
    # Call read_discharge_from_excel_sheet with test parameters
    file_path = 'backend/tests/test_files/12345_discharge_daily_sheet_not_found.xlsx'
    station = '12345'
    year = 2001

    # Call to read_discharge_from_excel_sheet should throw ValueError
    with pytest.raises(ValueError):
        data_processing.read_discharge_from_excel_sheet(file_path, station, year)


def test_get_daily_discharge_files_with_files_in_directory(tmpdir):
    # Set up the environment variable
    os.environ["ieasyforecast_daily_discharge_path"] = str(tmpdir)

    # Create a file in the directory
    with open(os.path.join(os.environ["ieasyforecast_daily_discharge_path"], "1_file.txt"), "w") as f:
        f.write("test")

    # Create a Site object with attribute code equal to 1.
    sites = [fl.Site(code='1')]

    expected_file_list = ["1_file.txt"]

    # With mock access to DB
    assert data_processing.get_daily_discharge_files(True, sites) == expected_file_list
    # Without mock access to DB
    assert data_processing.get_daily_discharge_files(False, sites) == expected_file_list

    # Create a site object with attribute code equal to 12345.
    sites = [fl.Site(code='12345')]

    # Get a list of excel files in the test_files directory
    # Setting the environment variable is required for the test to work
    test_directory = os.environ["ieasyforecast_daily_discharge_path"] = "backend/tests/test_files"
    expected_file_list = os.listdir(test_directory)

    # Filter for files with .xlsx extension
    expected_file_list = [f for f in expected_file_list if f.endswith(".xlsx")]

    # Filter for files that start with "1"
    expected_file_list = [f for f in expected_file_list if f.startswith("1")]

    # Test that get_daily_discharge throws a ValueError if there are duplicate
    # code strings in the file list
    with pytest.raises(ValueError):
        data_processing.get_daily_discharge_files(True, sites)

    # Clean up the environment variable
    os.environ.pop("ieasyforecast_daily_discharge_path")

def test_get_daily_discharge_files_without_files_in_directory(tmpdir):
    # Set up the environment variable
    os.environ["ieasyforecast_daily_discharge_path"] = str(tmpdir)

    # Create a site object with attribute code equal to 12345.
    sites = [fl.Site(code='12345')]

    # Call the function and check that it raises a FileNotFoundError
    with pytest.raises(FileNotFoundError):
        data_processing.get_daily_discharge_files(False, sites)

def test_get_daily_discharge_files_without_environment_variable():
    # Remove the environment variable
    if "ieasyforecast_daily_discharge_path" in os.environ:
        del os.environ["ieasyforecast_daily_discharge_path"]

    # Create a site object with attribute code equal to 12345.
    sites = [fl.Site(code='12345')]

    # Call the function and check that it raises an EnvironmentError
    with pytest.raises(EnvironmentError):
        data_processing.get_daily_discharge_files(True, sites)


def test_get_time_series_from_excel_with_valid_file():
    # Set up the environment variable
    os.environ["ieasyforecast_daily_discharge_path"] = "backend/tests/test_files"

    # Call the function with a valid file
    daily_discharge_files = pd.DataFrame(
        {"station": ["12345"],
         "file": [os.path.join(os.getenv("ieasyforecast_daily_discharge_path"),
                               "12345_discharge_daily_good_file.xlsx")]})
    result = data_processing.get_time_series_from_excel(daily_discharge_files)

    # Check that the result is a DataFrame with the expected columns
    assert isinstance(result, pd.DataFrame)

    # Check that the Q_m3s column is of type float
    assert pd.api.types.is_float_dtype(result["Q_m3s"])

    # Check that the Date column is of type datetime
    assert pd.api.types.is_datetime64_any_dtype(result["Date"])

    # Check that the Year column is of type int
    assert pd.api.types.is_integer_dtype(result["Year"])

    # First entry of Date, converted to format %Y-%m-%d, should be 2000-01-01
    assert result['Date'].iloc[0].strftime('%Y-%m-%d') == '2000-01-01'
    # Last entry of Date, converted to format %Y-%m-%d, should be 2000-01-18
    assert result['Date'].iloc[-1].strftime('%Y-%m-%d') == '2022-12-31'
    # First value of Q_m3s, rounded to 1 digit, should be 9.7
    assert result['Q_m3s'].iloc[0].round(1) == 9.7
    # Last value of Q_m3s, rounded to 1 digit, should be 9.5
    assert result['Q_m3s'].iloc[-1].round(1) == 9.5

    # Clean up the environment variable
    os.environ.pop("ieasyforecast_daily_discharge_path")

def test_get_time_series_from_excel_with_public_repo_data():
    # Set up the environment variable
    os.environ["ieasyforecast_daily_discharge_path"] = "../data/daily_runoff"

    # Call the function with a valid file
    daily_discharge_files = pd.DataFrame(
        {"station": ["12176", "12256"],
         "file": [os.path.join(os.getenv("ieasyforecast_daily_discharge_path"),
                               "12176_Sihl_example_river_runoff.xlsx"),
                  os.path.join(os.getenv("ieasyforecast_daily_discharge_path"),
                               "12256_Rosegbach_example_river_runoff.xlsx")]})
    result = data_processing.get_time_series_from_excel(daily_discharge_files)
    #print("DEBUG: test_read_data_from_public_repo_data: result: \n", result.head())
    #print(result.tail())


    # Check that the result is a DataFrame with the expected columns
    assert isinstance(result, pd.DataFrame)

    # Check that the Q_m3s column is of type float
    assert pd.api.types.is_float_dtype(result["Q_m3s"])

    # Check that the Date column is of type datetime
    assert pd.api.types.is_datetime64_any_dtype(result["Date"])

    # Check that the Year column is of type int
    assert pd.api.types.is_integer_dtype(result["Year"])

    # First entry of Date, converted to format %Y-%m-%d, should be 2000-01-01
    assert result['Date'].iloc[0].strftime('%Y-%m-%d') == '2000-01-01'
    # Last entry of Date, converted to format %Y-%m-%d, should be 2000-01-18
    assert result['Date'].iloc[-1].strftime('%Y-%m-%d') == '2023-04-30'
    # First value of Q_m3s, rounded to 1 digit, should be 5.19
    assert result['Q_m3s'].iloc[0].round(2) == 5.19
    # Last value of Q_m3s, rounded to 1 digit, should be 0.61
    assert result['Q_m3s'].iloc[-1].round(2) == 0.61
    # First value of Code should be 12176
    assert result['Code'].iloc[0] == '12176'
    # Last value of Code should be 12256
    assert result['Code'].iloc[-1] == '12256'

    # Clean up the environment variable
    os.environ.pop("ieasyforecast_daily_discharge_path")

def test_get_time_series_from_another_excel_with_valid_file():
    # Set up the environment variable
    os.environ["ieasyforecast_daily_discharge_path"] = "backend/tests/test_files"

    # Call the function with a valid file
    daily_discharge_files = pd.DataFrame(
        {"station": ["15678"],
         "file": [os.path.join(os.getenv("ieasyforecast_daily_discharge_path"),
                               "15678_test_data.xlsx")]})
    result = data_processing.get_time_series_from_excel(daily_discharge_files)

    # Check that the result is a DataFrame with the expected columns
    assert isinstance(result, pd.DataFrame)

    # Check that the Q_m3s column is of type float
    assert pd.api.types.is_float_dtype(result["Q_m3s"])

    # Check that the Date column is of type datetime
    assert pd.api.types.is_datetime64_any_dtype(result["Date"])

    # Check that the Year column is of type int
    assert pd.api.types.is_integer_dtype(result["Year"])

    # First entry of Date, converted to format %Y-%m-%d, should be 2000-01-01
    assert result['Date'].iloc[0].strftime('%Y-%m-%d') == '2017-01-01'
    # Last entry of Date, converted to format %Y-%m-%d, should be 2000-01-18
    assert result['Date'].iloc[-1].strftime('%Y-%m-%d') == '2018-12-31'
    # First value of Q_m3s, rounded to 1 digit, should be 1.96
    assert result['Q_m3s'].iloc[0].round(2) == 1.96
    # Last value of Q_m3s, rounded to 1 digit, should be 1.2
    assert result['Q_m3s'].iloc[-1].round(2) == 1.20

    # Clean up the environment variable
    os.environ.pop("ieasyforecast_daily_discharge_path")

def test_get_time_series_from_excel_with_invalid_file():
    # Set up the environment variable
    os.environ["ieasyforecast_daily_discharge_path"] = "backend/tests/test_files"

    # Call the function with an invalid file
    daily_discharge_files = pd.DataFrame(
        {"station": ["12345"],
         "file": [os.path.join(os.getenv("ieasyforecast_daily_discharge_path"),
                               "12345_discharge_daily_additional_header.xlsx")]})

    with pytest.raises(FileNotFoundError):
        data_processing.get_time_series_from_excel(daily_discharge_files)

    # Clean up the environment variable
    os.environ.pop("ieasyforecast_daily_discharge_path")


def test_get_station_data_from_another_excel_with_valid_file():
    # Set up the environment variable
    os.environ["ieasyforecast_daily_discharge_path"] = "backend/tests/test_files"

    # Get a mock ieasyhydro object
    ieh_sdk = MagicMock()

    # Define start_date
    start_date = pd.to_datetime("2018-05-05")

    # Create a site object with attribute code equal to 15678.
    sites = [fl.Site(code='15678')]

    # Call the function
    result = data_processing.get_station_data(ieh_sdk, False, start_date, sites)

    # Check that the result is a DataFrame with the expected columns
    assert isinstance(result, pd.DataFrame)

    # Check that the columns are as expected
    expected_columns = ['Date', 'Q_m3s', 'Year', 'Code', 'issue_date', 'discharge_sum',
       'discharge_avg', 'pentad', 'pentad_in_year']
    assert list(result.columns) == expected_columns

    # Check that the Q_m3s column is of type float
    assert pd.api.types.is_float_dtype(result["Q_m3s"])

    # Check that the Year column is of type int
    assert pd.api.types.is_integer_dtype(result["Year"])

    # First entry of Date, converted to format %Y-%m-%d, should be 2000-01-01
    assert result['Date'].iloc[0].strftime('%Y-%m-%d') == '2017-01-01'
    # Last entry of Date, converted to format %Y-%m-%d, should be 2000-01-18
    assert result['Date'].iloc[-1].strftime('%Y-%m-%d') == start_date.strftime('%Y-%m-%d')
    # First value of Q_m3s, rounded to 1 digit, should be 1.96
    assert result['Q_m3s'].iloc[0].round(2) == 1.96
    # Last value of Q_m3s, rounded to 1 digit, should be 1.9
    assert result['Q_m3s'].iloc[-1].round(2) == 1.25
    # Last value of issue_date should be True
    assert result['issue_date'].iloc[-1] == True
    # Last value of discharge_sum should be the sum of the discharge values of
    # time steps -4 to -2
    assert result['discharge_sum'].iloc[-1] == result['Q_m3s'].iloc[-4:-1].sum()
    # Last value of discharge_avg should be NaN
    assert pd.isna(result['discharge_avg'].iloc[-1])
    # Last value of pentad should be '1'
    assert result['pentad'].iloc[-1] == '1'
    # Last value of pentad_in_year should be '25'
    assert result['pentad_in_year'].iloc[-1] == '25'

    # Clean up the environment variable
    os.environ.pop("ieasyforecast_daily_discharge_path")


def test_add_pentad_issue_date():
    # Test with valid data
    data = {
        'Date': pd.date_range(start='1/1/2022', end='1/31/2022')
    }
    df = pd.DataFrame(data)
    result = data_processing.add_pentad_issue_date(df, datetime_col='Date')
    assert 'issue_date' in result.columns
    assert result['issue_date'].dtype == bool

    # Test if the issue_date column is True for days 5, 10, 15, 20, 25, and for
    # the last day of each month and False for all other days
    assert result['issue_date'].iloc[4] == True  # 5th day
    assert result['issue_date'].iloc[9] == True  # 10th day
    assert result['issue_date'].iloc[14] == True
    assert result['issue_date'].iloc[19] == True
    assert result['issue_date'].iloc[24] == True
    assert result['issue_date'].iloc[30] == True
    assert result['issue_date'].iloc[0] == False
    assert result['issue_date'].iloc[1] == False
    assert result['issue_date'].iloc[2] == False
    assert result['issue_date'].iloc[3] == False
    assert result['issue_date'].iloc[5] == False
    assert result['issue_date'].iloc[6] == False
    assert result['issue_date'].iloc[7] == False
    assert result['issue_date'].iloc[8] == False
    assert result['issue_date'].iloc[10] == False
    assert result['issue_date'].iloc[11] == False
    assert result['issue_date'].iloc[12] == False
    assert result['issue_date'].iloc[13] == False
    assert result['issue_date'].iloc[15] == False
    assert result['issue_date'].iloc[16] == False

    assert result['issue_date'].iloc[-3] == False
    assert result['issue_date'].iloc[-2] == False
    # The last value should be True
    assert result['issue_date'].iloc[-1] == True


    # Test with non-datetime datetime_col
    df['datetime_col'] = range(1, 32)
    with pytest.raises(TypeError):
        data_processing.add_pentad_issue_date(df, 'datetime_col')

    # Test with missing datetime_col
    with pytest.raises(KeyError):
        data_processing.add_pentad_issue_date(df, 'nonexistent_col')

    data = {
        'Date': pd.date_range(start='1/1/2022', end='1/31/2022')
    }
    df = pd.DataFrame(data)
    # Switch the locations of the first few dates
    df.loc[0, 'Date'] = pd.Timestamp('2022-01-05')
    df.loc[4, 'Date'] = pd.Timestamp('2022-01-01')
    result = data_processing.add_pentad_issue_date(df, datetime_col='Date')
    # The dates in the resulting data frame should be sorted
    assert result['issue_date'].iloc[0] == False
    assert result['issue_date'].iloc[1] == False
    assert result['issue_date'].iloc[2] == False
    assert result['issue_date'].iloc[3] == False
    assert result['issue_date'].iloc[4] == True  # 5th day
    assert result['issue_date'].iloc[5] == False


def test_calculate_3daydischargesum():
    # Test with valid data
    data = {
        'datetime_col': pd.date_range(start='1/1/2022', end='1/31/2022'),
        'discharge_col': np.random.rand(31),
        'issue_date': [True if i % 5 == 0 else False for i in range(31)]
    }
    df = pd.DataFrame(data)
    result = data_processing.calculate_3daydischargesum(df, 'datetime_col', 'discharge_col')
    assert 'discharge_sum' in result.columns
    assert result['discharge_sum'].dtype == float

    # Test with non-datetime datetime_col
    df2 = df.copy(deep=True)
    df2['datetime_col'] = range(1, 32)
    with pytest.raises(TypeError):
        data_processing.calculate_3daydischargesum(df2, 'datetime_col', 'discharge_col')

    # Test with missing datetime_col
    with pytest.raises(KeyError):
        data_processing.calculate_3daydischargesum(df, 'nonexistent_col', 'discharge_col')

    # Test with missing discharge_col
    with pytest.raises(KeyError):
        data_processing.calculate_3daydischargesum(df, 'datetime_col', 'nonexistent_col')

    # Test with reproducible data
    data = {
        'Dates': pd.date_range(start='1/1/2022', end='12/31/2022'),
        'Values': pd.date_range(start='1/1/2022', end='12/31/2022').day
    }
    df = pd.DataFrame(data)
    df = data_processing.add_pentad_issue_date(df, datetime_col='Dates')
    result = data_processing.calculate_3daydischargesum(df, 'Dates', 'Values')

def test_calculate_pentadaldischargeavg():
    # Test with reproducible data
    data = {
        'Dates': pd.date_range(start='1/1/2022', end='12/31/2022'),
        'Values': pd.date_range(start='1/1/2022', end='12/31/2022').day
    }
    df = pd.DataFrame(data)
    df = data_processing.add_pentad_issue_date(df, datetime_col='Dates')
    result0 = data_processing.calculate_3daydischargesum(df, 'Dates', 'Values')
    result = data_processing.calculate_pentadaldischargeavg(result0, 'Dates', 'Values')

    assert 'discharge_avg' in result.columns
    assert result['discharge_avg'].dtype == float
    # The first 4 values should be NaN
    assert pd.isna(result['discharge_avg'].iloc[0])
    assert pd.isna(result['discharge_avg'].iloc[1])
    assert pd.isna(result['discharge_avg'].iloc[2])
    assert pd.isna(result['discharge_avg'].iloc[3])
    # The first value that is not NaN should be 8.0
    assert result['discharge_avg'].iloc[4] == 8.0
    # Then we have another 4 NaN values
    assert pd.isna(result['discharge_avg'].iloc[5])
    assert pd.isna(result['discharge_avg'].iloc[6])
    assert pd.isna(result['discharge_avg'].iloc[7])
    assert pd.isna(result['discharge_avg'].iloc[8])
    # The next value should be 13.0
    assert result['discharge_avg'].iloc[9] == 13.0
    # The last value should be NaN
    assert pd.isna(result['discharge_avg'].iloc[-1])
    assert result['discharge_avg'].iloc[-7] == 28.0

def test_generate_issue_and_forecast_dates():
    # Calculate expected result:
    # Test with reproducible data
    data = {
        'Dates': pd.date_range(start='1/1/2022', end='12/31/2022'),
        'Values': pd.date_range(start='1/1/2022', end='12/31/2022').day,
        'Stations': ['12345' for i in range(365)]
    }
    df = pd.DataFrame(data)
    df = data_processing.add_pentad_issue_date(df, datetime_col='Dates')
    result0 = data_processing.calculate_3daydischargesum(df, 'Dates', 'Values')
    expected_result = data_processing.calculate_pentadaldischargeavg(result0, 'Dates', 'Values')

    # Call the function
    result = data_processing.generate_issue_and_forecast_dates(
        df, 'Dates', 'Stations', 'Values')

    # Check that the result is a DataFrame with the expected columns
    assert isinstance(result, pd.DataFrame)
    assert 'issue_date' in result.columns
    assert 'discharge_sum' in result.columns
    assert 'discharge_avg' in result.columns
    # Test if the result is the same as the expected result
    # Test if there are any NaNs in the Stations column
    assert result['Stations'].isna().sum() == 0
    assert expected_result['Stations'].isna().sum() == 0
    # Test if the datatypes are the same
    assert result['Stations'].dtype == expected_result['Stations'].dtype
    # Test each column separately. Only compare the values in the columns
    # because the indices may be different
    assert (result['Stations'].values == expected_result['Stations'].values).all()
    assert (result['issue_date'].values == expected_result['issue_date'].values).all()
    # Print discharge_sum from result and expected_result next to each other in a
    # DataFrame to visually inspect the values. Also add a column with the difference
    # between the two columns.
    temp = pd.DataFrame({'discharge_sum': result['discharge_sum'].values,
                         'expected_discharge_sum': expected_result['discharge_sum'].values,
                         'difference': result['discharge_sum'].values - expected_result['discharge_sum'].values})
    # Drop rows where all 3 columns have NaN
    temp = temp.dropna(how='all')
    # Drop rows where the difference is 0.0
    temp = temp[temp['difference'] != 0.0]
    #print("\n\nDEBUG: test_generate_issue_and_forecast_dates: result['discharge_sum'] vs expected_result['discharge_sum']: \n",
    #      temp)
    assert (result['discharge_sum'].dropna().values == expected_result['discharge_sum'].dropna().values).all()
    assert (result['discharge_avg'].dropna().values == expected_result['discharge_avg'].dropna().values).all()
