import os
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from ..src.data_processing import read_discharge_from_excel_sheet

@patch('os.path.exists')
@patch('pandas.read_excel')
def test_read_discharge_from_excel_sheet(mock_read_excel, mock_exists):
    # Mock os.path.exists to always return True
    mock_exists.return_value = True

    # Mock pd.read_excel to return a DataFrame with the expected columns and data types
    mock_df = pd.DataFrame({
        'Date': pd.date_range(start='2022-01-01', end='2022-12-31'),
        'Q_m3s': pd.Series([1.0]*365),
    })
    mock_read_excel.return_value = mock_df

    # Call read_discharge_from_excel_sheet with test parameters
    file_path = 'test.xlsx'
    station = 'test_station'
    year = 2022
    data = read_discharge_from_excel_sheet(file_path, station, year)

    # Check that the returned DataFrame has the expected columns and data
    assert 'Date' in data.columns
    assert 'Q_m3s' in data.columns
    assert 'Year' in data.columns
    assert 'Code' in data.columns
    assert data['Year'].unique() == [year]
    assert data['Code'].unique() == [station]

    # Check that pd.read_excel was called with the expected arguments
    mock_read_excel.assert_called_once_with(
        file_path, sheet_name=str(year), header=[0], skiprows=[1],
        names=['Date', 'Q_m3s'], parse_dates=['Date']
    )

    # Check that os.path.exists was called with the expected argument
    mock_exists.assert_called_once_with(file_path)