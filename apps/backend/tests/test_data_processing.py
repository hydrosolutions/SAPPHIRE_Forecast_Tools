import os
import pandas as pd
import numpy as np
import pytest
import datetime as dt
import shutil
from unittest.mock import patch, MagicMock
from backend.src import data_processing, config

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

    # Read actual data from excel
    # List of sheet names you want to read
    sheets = ['2000', '2001', '2002', '2003']
    # Read the data from the sheets
    dataframes = [pd.read_excel('../data/daily_runoff/12176_Sihl_example_river_runoff.xlsx', sheet_name=sheet) for sheet in sheets]
    # Concatenate the data
    sihl_data = pd.concat(dataframes)
    # Rename the columns to Date and discharge
    sihl_data.columns = ['Date', 'discharge']

    # Merge result for code 12176 with sihl_data by column Date
    data_comparison = pd.merge(result.loc[result['Code'] == '12176', ["Date", "Q_m3s"]], sihl_data, on='Date', how='inner')
    data_comparison['diff'] = data_comparison['Q_m3s'] - data_comparison['discharge']
    print(sum(data_comparison['diff']))
    print(data_comparison.head())

    assert sum(data_comparison['diff']) < 1e-6


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

    forecast_flags = config.ForecastFlags(pentad=True, decad=True)

    # Call the function
    result, result_decad = data_processing.get_station_data(
        ieh_sdk, False, start_date, sites, forecast_flags)

    #print("\n\nDEBUG: test_get_station_data_from_another_excel_with_valid_file: result_decad: \n", result_decad.head(40))

    # DECAD
    assert round(result_decad['discharge_avg'].iloc[19], 3) == round(1.653636, 3)
    assert result_decad['discharge_avg'].iloc[19] == result_decad['predictor'].iloc[30]

    # PENTAD
    # Check that the result is a DataFrame with the expected columns
    assert isinstance(result, pd.DataFrame)

    # Check that the columns are as expected
    expected_columns = ['Date', 'Q_m3s', 'Year', 'Code', 'issue_date', 'discharge_sum',
       'discharge_avg', 'pentad', 'pentad_in_year']
    assert list(result.columns) == expected_columns

    # Check that the Q_m3s column is of type float
    assert pd.api.types.is_float_dtype(result["Q_m3s"])

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

    # Read actual data from excel
    # List of sheet names you want to read
    sheets = ['2017', '2018']
    # Read the data from the sheets
    dataframes = [pd.read_excel('backend/tests/test_files/15678_test_data.xlsx', sheet_name=sheet) for sheet in sheets]
    # Concatenate the data
    test_data = pd.concat(dataframes)
    # Rename the columns to Date and discharge
    test_data.columns = ['Date', 'discharge']

    # Merge result for code 12176 with sihl_data by column Date
    data_comparison = pd.merge(result.loc[:, ["Date", "Q_m3s"]], test_data, on='Date', how='inner')
    data_comparison['diff'] = data_comparison['Q_m3s'] - data_comparison['discharge']
    print(sum(data_comparison['diff']))
    print(data_comparison.head())


    # Clean up the environment variable
    os.environ.pop("ieasyforecast_daily_discharge_path")

def test_get_station_data_from_public_repo_data():
    # Set up the environment variable
    os.environ["ieasyforecast_daily_discharge_path"] = "../data/daily_runoff"

    # Get a mock ieasyhydro object
    ieh_sdk = MagicMock()

    # Define start_date
    start_date = pd.to_datetime("2018-05-05")

    # Create a site object with attribute code equal to 12176.
    sites = [fl.Site(code='12176'), fl.Site(code='12256')]

    forecast_flags = config.ForecastFlags(pentad=True, decad=True)

    # Call the function
    result, result_decad = data_processing.get_station_data(ieh_sdk, False, start_date, sites,
                                              forecast_flags)

    # Check that the result is a DataFrame with the expected columns
    assert isinstance(result, pd.DataFrame)

    # Check that the columns are as expected
    expected_columns = ['Date', 'Q_m3s', 'Year', 'Code', 'issue_date', 'discharge_sum',
       'discharge_avg', 'pentad', 'pentad_in_year']
    assert list(result.columns) == expected_columns

    # Check that the Q_m3s column is of type float
    assert pd.api.types.is_float_dtype(result["Q_m3s"])

    # Compare data read with get_station_data with expected data read from excel.
    # List of sheet names you want to read
    sheets = ['2000', '2001', '2002', '2003']
    # Read the data from the sheets
    dataframes = [pd.read_excel('../data/daily_runoff/12176_Sihl_example_river_runoff.xlsx', sheet_name=sheet) for sheet in sheets]
    # Concatenate the data
    sihl_data = pd.concat(dataframes)
    # Rename the columns to Date and discharge
    sihl_data.columns = ['Date', 'discharge']

    #print(result_decad.head(40))
    #print(result_decad.head(80).tail(40))
    #print(sihl_data.head(5))

    # Merge result for code 12176 with sihl_data by column Date
    data_comparison = pd.merge(result.loc[result['Code'] == '12176', ["Date", "Q_m3s"]], sihl_data, on='Date', how='inner')
    data_comparison['diff'] = data_comparison['Q_m3s'] - data_comparison['discharge']
    #print(sum(data_comparison['diff']))
    #print(data_comparison.head())



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

def test_filter_roughly_for_outliers_no_outliers():
    # Create a DataFrame with no outliers
    df = pd.DataFrame({
        'Code': ['A', 'A', 'A', 'B', 'B', 'B'],
        'Q_m3s': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    })

    # Apply the function
    result = data_processing.filter_roughly_for_outliers(df)

    # Drop index
    result = result.reset_index(drop=True)

    # Check that the result is the same as the input
    pd.testing.assert_frame_equal(result, df)

def test_filter_roughly_for_outliers_with_outliers():
    # Create a DataFrame with an outlier
    df = pd.DataFrame({
        'Code': ['A', 'A', 'A', 'B', 'B', 'B',
                 'A', 'A', 'A', 'B', 'B', 'B',
                 'A', 'A', 'A', 'B', 'B', 'B',
                 'A', 'A', 'A', 'B', 'B', 'B',
                 'A', 'A', 'A', 'B', 'B', 'B',
                 'A', 'A', 'A', 'B', 'B', 'B',
                 'A', 'A', 'A', 'B', 'B', 'B',
                 'A', 'A', 'A', 'B', 'B', 'B',
                 'A', 'A', 'A', 'B', 'B', 'B',
                 'A', 'A', 'A', 'B', 'B', 'B',
                 'A', 'A', 'A', 'B', 'B', 'B'],
        'Q_m3s': [1.01, 2.01, 3.01, 4.0, 5.0, 6.0,
                  1.02, 2.02, 3.02, 4.0, 5.0, 6.0,
                  1.03, 2.03, 3.03, 4.0, 5.0, 6.0,
                  1.04, 2.04, 3.04, 4.0, 5.0, 6.0,
                  1.05, 2.05, 3.05, 4.0, 5.0, 6.0,
                  1.06, 2.06, 100.0, 4.0, 5.0, 6.0,
                  1.07, 2.07, 3.07, 4.0, 5.0, 6.0,
                  1.08, 2.08, 3.08, 4.0, 5.0, 6.0,
                  1.09, 2.09, 3.09, 4.0, 5.0, 6.0,
                  1.10, 2.10, 3.10, 4.0, 5.0, 6.0,
                  1.11, 2.11, 3.11, 4.0, 5.0, 6.0]
    })

    # Apply the function
    result = data_processing.filter_roughly_for_outliers(df, window_size=15)

    # Check that the outlier has been replaced with NaN
    # There should be exactly one NaN value in the DataFrame column Q_m3s
    #print(result[result['Q_m3s']==100.0])
    #print(result['Q_m3s'].isna().sum())
    assert result['Q_m3s'].isna().sum() == 1


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

    print("\n\nDEBUG: test_calculate_3daydischargesum: df: \n", df.head(40))

    result = data_processing.calculate_3daydischargesum(df, 'Dates', 'Values')

    print("\n\nDEBUG: test_calculate_3daydischargesum: result: \n", result.head(40))

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
    assert result['discharge_avg'].iloc[-7] == 28.5

def test_calculate_decadaldischargeavg():
    # Test with reproducible data
    data = {
        'Dates': pd.date_range(start='1/1/2022', end='12/31/2022'),
        'Values': pd.date_range(start='1/1/2022', end='12/31/2022').day
    }
    df = pd.DataFrame(data)
    df = data_processing.add_decad_issue_date(df, datetime_col='Dates')
    result = data_processing.calculate_decadaldischargeavg(df, 'Dates', 'Values')

    #print("\n\nDEBUG: test_calculate_decadaldischargeavg: result: \n", result.head(40))
    #print(result.tail())

    assert 'discharge_avg' in result.columns
    assert result['discharge_avg'].dtype == float
    # The first 4 values should be NaN
    assert pd.isna(result['discharge_avg'].iloc[0])
    assert pd.isna(result['discharge_avg'].iloc[1])
    assert pd.isna(result['discharge_avg'].iloc[2])
    assert pd.isna(result['discharge_avg'].iloc[3])
    # The first value that is not NaN should be 8.0
    assert result['discharge_avg'].iloc[9] == 15.5
    # Then we have another 4 NaN values
    assert pd.isna(result['discharge_avg'].iloc[5])
    assert pd.isna(result['discharge_avg'].iloc[6])
    assert pd.isna(result['discharge_avg'].iloc[7])
    assert pd.isna(result['discharge_avg'].iloc[8])
    # The next value should be 13.0
    assert result['discharge_avg'].iloc[19] == 26.0
    # The last value should be NaN
    assert pd.isna(result['discharge_avg'].iloc[-1])

    assert 'predictor' in result.columns
    assert result['predictor'].dtype == float
    # The first 4 values should be NaN
    assert pd.isna(result['predictor'].iloc[0])
    assert pd.isna(result['predictor'].iloc[1])
    assert pd.isna(result['predictor'].iloc[2])
    assert pd.isna(result['predictor'].iloc[9])
    assert pd.isna(result['predictor'].iloc[5])
    assert pd.isna(result['predictor'].iloc[6])
    assert pd.isna(result['predictor'].iloc[8])
    assert result['predictor'].iloc[19] == 15.5
    assert result['predictor'].iloc[30] == 26.0
    assert result['predictor'].iloc[-1] == 26.0

def test_generate_issue_and_forecast_dates():
    # Calculate expected result:
    # Test with reproducible data
    data = {
        'Dates': pd.date_range(start='1/1/2022', end='12/31/2022'),
        'Values': pd.date_range(start='1/1/2022', end='12/31/2022').day,
        'Stations': ['12345' for i in range(365)]
    }

    forecast_flags = config.ForecastFlags(pentad=True, decad=True)

    df = pd.DataFrame(data)
    # Make sure we have floats in the Values column
    df['Values'] = df['Values'].astype(float)
    df = data_processing.add_pentad_issue_date(df, datetime_col='Dates')
    result0 = data_processing.calculate_3daydischargesum(df, 'Dates', 'Values')
    expected_result = data_processing.calculate_pentadaldischargeavg(result0, 'Dates', 'Values')

    df_decad = data_processing.add_decad_issue_date(df, datetime_col='Dates')
    expected_result_decad = data_processing.calculate_decadaldischargeavg(df_decad, 'Dates', 'Values')

    # Call the function
    result, result_decad = data_processing.generate_issue_and_forecast_dates(
        df, 'Dates', 'Stations', 'Values', forecast_flags=forecast_flags)

    # DECAD
    assert isinstance(result_decad, pd.DataFrame)
    assert isinstance(result_decad, pd.DataFrame)
    assert 'issue_date' in result_decad.columns
    assert 'predictor' in result_decad.columns
    assert 'discharge_avg' in result_decad.columns

    temp = pd.DataFrame({'predictor': result_decad['predictor'].values,
                         'expected_predictor': expected_result_decad['predictor'].values,
                         'difference': result_decad['predictor'].values - expected_result_decad['predictor'].values})
    # Drop rows where all 3 columns have NaN
    temp = temp.dropna(how='all')
    # Drop rows where the difference is 0.0
    temp = temp[temp['difference'] != 0.0]
    print("\n\nDEBUG: test_generate_issue_and_forecast_dates: result['pred'] vs expected_result['pred']: \n",
          temp)
    assert (result_decad['predictor'].dropna().values == expected_result_decad['predictor'].dropna().values).all()
    assert (result_decad['discharge_avg'].dropna().values == expected_result_decad['discharge_avg'].dropna().values).all()

    # PENTAD
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

def test_generate_issue_and_forecast_dates_for_pentadal_forecasts_with_demo_data():

    # Set up the test environment
    # Temporary directory to store output
    tmpdir = "backend/tests/test_files/temp"
    # Clean up the folder in case it already exists (for example because a previous test failed)
    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)
    # Create the directory
    os.makedirs(tmpdir, exist_ok=True)
    # Load the environment
    config.load_environment()
    # Create the intermediate data directory
    os.makedirs(
        os.getenv("ieasyforecast_intermediate_data_path"),
        exist_ok=True)
    # Copy the input directories to tmpdir
    temp_ieasyforecast_configuration_path = os.path.join(tmpdir, "apps/config")
    temp_ieasyreports_templates_directory_path = os.path.join(tmpdir, "data/templates")
    temp_ieasyreports_report_output_path = os.path.join(tmpdir, "data/reports")
    temp_ieasyforecast_gis_directory_path = os.path.join(tmpdir, "data/GIS")
    temp_ieasyforecast_daily_discharge_path = os.path.join(tmpdir, "data/daily_runoff")
    temp_ieasyforecast_locale_dir = os.path.join(tmpdir, "apps/config/locale")
    temp_log_file = os.path.join(tmpdir, "backend.log")

    shutil.copytree("config", temp_ieasyforecast_configuration_path)
    shutil.copytree("../data", os.path.join(tmpdir, "data"))

    # Update the environment variables to point to the temporary directory
    os.environ["ieasyforecast_configuration_path"] = temp_ieasyforecast_configuration_path
    os.environ["ieasyreports_template_directory_path"] = temp_ieasyreports_templates_directory_path
    os.environ["ieasyreports_report_output_path"] = temp_ieasyreports_report_output_path
    os.environ["ieasyforecast_gis_directory_path"] = temp_ieasyforecast_gis_directory_path
    os.environ["ieasyforecast_daily_discharge_path"] = temp_ieasyforecast_daily_discharge_path
    os.environ["ieasyforecast_locale_dir"] = temp_ieasyforecast_locale_dir
    os.environ["log_file"] = temp_log_file

    # Read reference data from excel files
    # List of sheet names you want to read
    sheets = ['2000', '2001', '2002', '2003']
    # Read the data from the sheets
    dataframes = [pd.read_excel('../data/daily_runoff/12176_Sihl_example_river_runoff.xlsx', sheet_name=sheet) for sheet in sheets]
    # Concatenate the data
    sihl_data = pd.concat(dataframes)
    # Rename the columns to Date and discharge
    sihl_data.columns = ['Date', 'Q_excel']

    # Define start_date
    start_date = dt.datetime(2022, 5, 5)

    # Get a mock ieasyhydro object
    ieh_sdk = MagicMock()

    # Set forecast_flags
    forecast_flags = config.ForecastFlags(pentad=True)

    # Get bulletin date
    bulletin_date = config.get_bulletin_date(start_date)

    backend_has_access_to_db = False

    # - identify sites for which to produce forecasts
    #   reading sites from DB and config files
    db_sites = data_processing.get_db_sites(ieh_sdk, backend_has_access_to_db)
    #   writing sites information to as list of Site objects
    fc_sites = data_processing.get_fc_sites(ieh_sdk, backend_has_access_to_db, db_sites)

    # Step through the function to be tested step by step
    # Get a list of the Excel files to read data from
    daily_discharge_files = data_processing.get_daily_discharge_files(backend_has_access_to_db, site_list=fc_sites)

    # Create a dataframe with the station IDs and the file names. The station
    # IDs are in the first part of the file names, before the first underscore.
    # The filenames are the full path to the files.
    library = pd.DataFrame(
        {
            "station": [file.split('_')[0] for file in daily_discharge_files],
            "file": [os.path.join(os.getenv("ieasyforecast_daily_discharge_path"), file)
                     for file in daily_discharge_files]
        })
    print("\n", library)

    # Get the time series data from the Excel files
    combined_data = data_processing.get_time_series_from_excel(library)
    # Make sure we have data for all stations
    assert len(fc_sites) == len(combined_data['Code'].unique())
    # Rename column Q_m3s to Q_get_time_series_from_excel
    combined_data2 = combined_data.rename(columns={'Q_m3s': 'Q_gfEX'})
    # Filter combined_data for code 12176 and merge with sihl_data by column Date
    data_comparison = pd.merge(sihl_data,
                               combined_data2.loc[combined_data2['Code'] == '12176', ["Date", "Q_gfEX"]],
                               on='Date', how='inner')
    print("")
    # Assert that the difference between the two columns is less than 1e-6
    assert sum(data_comparison['Q_excel'] - data_comparison['Q_gfEX']) < 1e-6

    # Get data from DB
    db_data = data_processing.get_time_series_from_DB(ieh_sdk, library)
    # Combine the data from the DB and the Excel sheets. Check for duplicate
    # dates and keep the data from the DB.
    combined_data = pd.concat([combined_data, db_data]).drop_duplicates(subset=['Date', 'Code'], keep='first')
    # Make sure we still have data from all stations
    assert len(fc_sites) == len(combined_data['Code'].unique())
    # Make a full copy of the combined_data
    combined_data_copy = combined_data.copy(deep=True)
    # Rename column Q_m3s to Q_get_time_series_from_DB
    combined_data2 = combined_data.rename(columns={'Q_m3s': 'Q_gfDB'})
    # Filter combined_data for code 12176 and merge with sihl_data by column Date
    data_comparison = pd.merge(data_comparison,
                               combined_data2.loc[combined_data2['Code'] == '12176', ["Date", "Q_gfDB"]],
                               on='Date', how='inner')
    assert sum(data_comparison['Q_excel'] - data_comparison['Q_gfDB']) < 1e-6

    # Do the whole Toktogul routine
    if '16936' in combined_data['Code'].values:
        # Go through the the combined_data for '16936' and check for data gaps.
        # From the last available date in the combined_data for '16936', we
        # look for data in the combined_data for other stations, namely '16059',
        # '16096', '16100', and '16093' and add their values up to write to
        # station '16936'.

        # Get latest date for which we have data in '16936'
        last_date_16936 = combined_data[combined_data['Code'] == '16936']['Date'].max()

        # Get the data for the date from the other stations
        # Test if station 16093 (Torkent) is in the list of stations
        # (This station is under construction at the time of writing this code)
        if '16059' in combined_data['Code'].values:
            data_16059 = combined_data[(combined_data['Code'] == '16059') & (combined_data['Date'] >= last_date_16936)]
        else:
            exit()
        if '16096' in combined_data['Code'].values:
            data_16096 = combined_data[(combined_data['Code'] == '16096') & (combined_data['Date'] >= last_date_16936)]
        else:
            exit()
        if '16100' in combined_data['Code'].values:
            data_16100 = combined_data[(combined_data['Code'] == '16100') & (combined_data['Date'] >= last_date_16936)]
        else:
            exit()
        if '16093' in combined_data['Code'].values:
            data_16093 = combined_data[(combined_data['Code'] == '16093') & (combined_data['Date'] >= last_date_16936)]
        else:
            data_16093 = combined_data[(combined_data['Code'] == '16096') & (combined_data['Date'] >= last_date_16936)]
            data_16093.loc[:, 'Q_m3s'] = 0.6 * data_16093.loc[:,'Q_m3s']

        # Merge data_15059, data_16096, data_16100, and data_16093 by date
        data_Tok_combined = pd.merge(data_16059, data_16096, on='Date', how='left', suffixes=('_16059', '_16096'))
        data_16100.rename(columns={'Q_m3s': 'Q_m3s_16100'}, inplace=True)
        data_Tok_combined = pd.merge(data_Tok_combined, data_16100, on='Date', how='left')
        data_16093.rename(columns={'Q_m3s': 'Q_m3s_16093'}, inplace=True)
        data_Tok_combined = pd.merge(data_Tok_combined, data_16093, on='Date', how='left')

        # Sum up all the data and write to '16936'
        data_Tok_combined['sum'] = data_Tok_combined['Q_m3s_16059'] + \
            data_Tok_combined['Q_m3s_16096'] + \
                data_Tok_combined['Q_m3s_16100'] + \
                    data_Tok_combined['Q_m3s_16093']

        # Append the data_Tok_combined['sum'] to combined_data for '16936'
        data_Tok_combined['Code'] = '16936'
        data_Tok_combined['Year'] = data_Tok_combined['Year_16059']
        data_Tok_combined['Q_m3s'] = data_Tok_combined['sum']
        data_Tok_combined.drop(columns=['Year_16059', 'Code_16059', 'Year_16096',
                                        'Code_16096', 'Year_x', 'Code_x',
                                        'Year_y', 'Code_y',
                                        'Q_m3s_16059', 'Q_m3s_16096',
                                        'Q_m3s_16100', 'Q_m3s_16093', 'sum'],
                               inplace=True)

        combined_data = pd.concat([combined_data, data_Tok_combined], ignore_index=True)

    # Test that combined_data_copy is the same as combined_data
    assert combined_data_copy.equals(combined_data)
    # Make sure we still have data for all stations
    assert len(fc_sites) == len(combined_data['Code'].unique())

    # Filtering
    # Filter combined_data for dates before today (to simulate offline mode)
    combined_data = combined_data[combined_data['Date'] <= start_date]

    df_filtered = data_processing.filter_roughly_for_outliers(combined_data, window_size=15)
    # Make sure we have data from all stations
    assert len(fc_sites) == len(df_filtered['Code'].unique())

    # Copy df_filtered to df_filtered_copy
    df_filtered_copy = df_filtered.copy(deep=True)
    # Rename the column Q_m3s to Q_F
    df_filtered_copy.rename(columns={'Q_m3s': 'Q_F'}, inplace=True)
    # Merge df_filtered filtered for station 12176 with comparison data by date
    data_comparison = pd.merge(data_comparison,
                               df_filtered_copy.loc[df_filtered_copy['Code'] == '12176', ["Date", "Q_F"]],
                               on='Date', how='inner')
    # Print the rows where Q_F is NaN
    # There should now be some NaN values in the Q_F column. Test if there are NaNs
    # in the Q_F column
    assert data_comparison['Q_F'].isna().sum() > 0

    # Make sure the Date column is of type datetime
    df_filtered.loc[:, 'Date'] = pd.to_datetime(df_filtered.loc[:, 'Date'])
    # Cast to datetime64 type
    df_filtered.loc[:, 'Date'] = df_filtered.loc[:, 'Date'].values.astype('datetime64[D]')

    # Sort df_filter with ascending Code and Date
    df_filtered.sort_values(by=['Code', 'Date'], inplace=True)


    modified_data, modified_data_decad = data_processing.generate_issue_and_forecast_dates(
        pd.DataFrame(df_filtered), 'Date', 'Code', 'Q_m3s', forecast_flags=forecast_flags)
    # Make sure we still have data for all stations
    assert len(fc_sites) == len(modified_data['Code'].unique())
    # Add modified_data for Code == 12176 to data_comparison by Date
    data_comparison = pd.merge(data_comparison,
                               modified_data.loc[modified_data['Code'] == '12176', ["Date", "Q_m3s"]],
                               on='Date', how='inner')
    # The NaNs should now have been interpolated so we should not have any NaNs
    # in the Q_m3s column
    assert data_comparison['Q_m3s'].isna().sum() == 0
    # Print where columns Q_F and Q_m3s are not equal

    # Read discharge data from excel and iEasyHydro database
    modified_data2, modified_data2_decad = data_processing.get_station_data(
        ieh_sdk, backend_has_access_to_db, start_date, fc_sites, forecast_flags)

    # modified_data['Q_m3s'] should be equal to modified_data2['Q_m3s']
    # for all stations
    data_comparison_final = pd.merge(modified_data,
                               modified_data2,
                               on='Date', how='inner')
    # The difference between the two columns should be 0
    assert sum(data_comparison_final['Q_m3s_x'] - data_comparison_final['Q_m3s_y']) == 0

    # Delete tmpdir
    shutil.rmtree(tmpdir)

    # Clean up the environment variable
    os.environ.pop("IEASYHYDRO_HOST")
    os.environ.pop("IEASYHYDRO_USERNAME")
    os.environ.pop("IEASYHYDRO_PASSWORD")
    os.environ.pop("ieasyforecast_configuration_path")
    os.environ.pop("ieasyforecast_config_file_all_stations")
    os.environ.pop("ieasyforecast_config_file_station_selection")
    os.environ.pop("ieasyforecast_config_file_output")
    os.environ.pop("ieasyforecast_intermediate_data_path")
    os.environ.pop("ieasyforecast_hydrograph_day_file")
    os.environ.pop("ieasyforecast_hydrograph_pentad_file")
    os.environ.pop("ieasyforecast_results_file")
    os.environ.pop("ieasyreports_templates_directory_path")
    os.environ.pop("ieasyforecast_template_pentad_bulletin_file")
    os.environ.pop("ieasyforecast_template_pentad_sheet_file")
    os.environ.pop("ieasyreports_report_output_path")
    os.environ.pop("ieasyforecast_bulletin_file_name")
    os.environ.pop("ieasyforecast_sheet_file_name")
    os.environ.pop("ieasyforecast_gis_directory_path")
    os.environ.pop("ieasyforecast_country_borders_file_name")
    os.environ.pop("ieasyforecast_daily_discharge_path")
    os.environ.pop("ieasyforecast_locale_dir")
    os.environ.pop("log_file")
    os.environ.pop("log_level")
    os.environ.pop("ieasyforecast_restrict_stations_file")
    os.environ.pop("ieasyforecast_last_successful_run_file")





def test_get_predictor_datetimes_for_decadal_forecasts():
    # Test with start_date.day == 1
    start_date = dt.datetime(2022, 1, 31)
    assert data_processing.get_predictor_datetimes_for_decadal_forecasts(start_date) == [dt.datetime(2022, 1, 11), dt.datetime(2022, 1, 20)]

    # Test with start_date.day == 10
    start_date = dt.datetime(2022, 2, 10)
    assert data_processing.get_predictor_datetimes_for_decadal_forecasts(start_date) == [dt.datetime(2022, 1, 21), dt.datetime(2022, 1, 31)]

    # Test with start_date.day == 20
    start_date = dt.datetime(2022, 2, 20)
    assert data_processing.get_predictor_datetimes_for_decadal_forecasts(start_date) == [dt.datetime(2022, 2, 1), dt.datetime(2022, 2, 10)]

def test_get_predictor_datetimes_for_decadal_forecasts_invalid_date():
    # Test with invalid start_date.day
    start_date = dt.datetime(2022, 2, 15)
    with pytest.raises(ValueError):
        data_processing.get_predictor_datetimes_for_decadal_forecasts(start_date)

def test_get_predictor_datetimes_for_decadal_forecasts_invalid_type():
    # Test with invalid start_date type
    start_date = "2022-02-01"
    with pytest.raises(TypeError):
        data_processing.get_predictor_datetimes_for_decadal_forecasts(start_date)





