import os
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from backend.src import data_processing

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

    # Call the function and check the result
    assert data_processing.get_daily_discharge_files(True) == ["1_file.txt"]

    # Get a list of excel files in the test_files directory
    test_directory = os.environ["ieasyforecast_daily_discharge_path"] = "backend/tests/test_files"
    expected_file_list = os.listdir(test_directory)

    # Filter for files with .xlsx extension
    expected_file_list = [f for f in expected_file_list if f.endswith(".xlsx")]

    # Filter for files that start with "1"
    expected_file_list = [f for f in expected_file_list if f.startswith("1")]

    # Test that the file names returned are found also in the expected_file_list
    assert data_processing.get_daily_discharge_files(False) == expected_file_list

    # Clean up the environment variable
    os.environ.pop("ieasyforecast_daily_discharge_path")

def test_get_daily_discharge_files_without_files_in_directory(tmpdir):
    # Set up the environment variable
    os.environ["ieasyforecast_daily_discharge_path"] = str(tmpdir)

    # Call the function and check that it raises a FileNotFoundError
    with pytest.raises(FileNotFoundError):
        data_processing.get_daily_discharge_files(False)

def test_get_daily_discharge_files_without_environment_variable():
    # Remove the environment variable
    if "ieasyforecast_daily_discharge_path" in os.environ:
        del os.environ["ieasyforecast_daily_discharge_path"]

    # Call the function and check that it raises an EnvironmentError
    with pytest.raises(EnvironmentError):
        data_processing.get_daily_discharge_files(True)


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


