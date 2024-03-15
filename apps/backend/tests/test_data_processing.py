import os
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from backend.src import data_processing

# Run tests from the apps directory and use the following command:
# python -m pytest backend/tests/test_read_discharge_from_excel_sheet.py

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


