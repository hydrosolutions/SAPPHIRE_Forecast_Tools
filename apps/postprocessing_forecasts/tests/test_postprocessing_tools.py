import os
import sys
import pytest
import pandas as pd
import numpy as np
import datetime as dt
from unittest.mock import patch, MagicMock, mock_open

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the function to test
from src.postprocessing_tools import log_most_recent_forecasts_pentad

# Fixture to create sample test data
@pytest.fixture
def sample_data():
    """Create sample forecast data for testing."""
    sample_date = dt.datetime(2023, 5, 25)
    data = pd.DataFrame({
        'code': ['15102', '15124', '15136', '15102', '15124', '15136',
                 '15102', '15124', '15136', '15102', '15124', '15136'],
        'date': [sample_date] * 12,
        'pentad_in_month': [5] * 12,
        'pentad_in_year': [30] * 12,
        'forecasted_discharge': [125.4, 45.7, 67.8, 130.2, 47.2, 65.1,
                                 128.7, 46.9, 66.5, 129.1, 47.0, 66.3],
        'model_short': ['LR', 'LR', 'LR', 'TFT', 'TFT', 'TFT',
                        'TIDE', 'TIDE', 'TIDE', 'EM', 'EM', 'EM'],
        'model_long': ['Linear Regression (LR)', 'Linear Regression (LR)', 'Linear Regression (LR)',
                      'Temporal-Fusion Transformer (TFT)', 'Temporal-Fusion Transformer (TFT)', 'Temporal-Fusion Transformer (TFT)',
                      'Time-Series Dense Encoder (TiDE)', 'Time-Series Dense Encoder (TiDE)', 'Time-Series Dense Encoder (TiDE)',
                      'Ensemble Mean (EM)', 'Ensemble Mean (EM)', 'Ensemble Mean (EM)']
    })
    return data

@pytest.fixture
def expected_pivot_data(sample_data):
    """Create expected pivot table output based on sample data."""
    sample_date = dt.datetime(2023, 5, 25)
    return pd.DataFrame({
        'code': ['15102', '15124', '15136'],
        'date': [sample_date] * 3,
        'pentad_in_month': [5] * 3,
        'LR': [125.4, 45.7, 67.8],
        'TFT': [130.2, 47.2, 65.1],
        'TIDE': [128.7, 46.9, 66.5],
        'EM': [129.1, 47.0, 66.3]
    })

@patch('os.makedirs')
@patch('os.path.join')
@patch('os.getenv')
@patch('pandas.DataFrame.to_csv')
@patch('logging.Logger.info')
def test_log_most_recent_forecasts_pentad(mock_logger_info, mock_to_csv,
                                  mock_getenv, mock_path_join, mock_makedirs,
                                  sample_data, expected_pivot_data):
    """Test the log_most_recent_forecasts_pentad function with sample data."""
    # Configure mocks
    mock_getenv.return_value = '/tmp'
    mock_path_join.return_value = '/tmp/forecast_logs/recent_model_forecasts_20230525.csv'

    # Call the function
    result = log_most_recent_forecasts_pentad(sample_data)

    # Verify directory creation
    mock_makedirs.assert_called_once()

    # Verify data was written to CSV
    mock_to_csv.assert_called_once()

    # Verify logging happened
    assert mock_logger_info.called

    # Check the structure of the result
    assert isinstance(result, pd.DataFrame)

    # Check that the pivot operation was done correctly
    assert len(result) == len(expected_pivot_data)
    print(f'result.columns: {list(result.columns)}')
    print(f'expected_pivot_data.columns: {list(expected_pivot_data.columns)}')
    # The order of the columns is not guaranteed, so we need to sort the column names
    result_names = sorted(result.columns.tolist())
    expected_names = sorted(expected_pivot_data.columns.tolist())
    assert result_names == expected_names

    # Compare with expected values
    for idx, row in result.iterrows():
        expected_row = expected_pivot_data.iloc[idx]
        # Check code
        assert row['code'] == expected_row['code']
        # Check date
        assert row['date'] == expected_row['date']
        # Check model forecasts
        for model in ['LR', 'TFT', 'TIDE', 'EM']:
            assert row[model] == pytest.approx(expected_row[model])

@patch('os.makedirs')
@patch('os.getenv')
@patch('pandas.DataFrame.to_csv')
def test_log_most_recent_forecasts_pentad_empty_data(mock_to_csv, mock_getenv, mock_makedirs, sample_data):
    """Test the function with empty data."""
    # Create empty DataFrame with the right columns
    empty_data = pd.DataFrame(columns=sample_data.columns)

    # Configure mocks
    mock_getenv.return_value = '/tmp'

    # Call the function - it should handle empty data gracefully
    result = log_most_recent_forecasts_pentad(empty_data)

    # to_csv should not be called because there's no data to write
    mock_to_csv.assert_not_called()

    # The result should be an empty DataFrame
    assert result.empty

@patch('os.makedirs')
@patch('os.getenv')
@patch('pandas.DataFrame.to_csv')
def test_log_most_recent_forecasts_pentad_multiple_dates(mock_to_csv, mock_getenv, mock_makedirs, sample_data):
    """Test the function with multiple dates - it should only use the most recent date."""
    # Create data with multiple dates
    multi_date_data = sample_data.copy()
    # Add some older dates
    older_data = sample_data.copy()
    older_data['date'] = dt.datetime(2023, 5, 20)
    multi_date_data = pd.concat([multi_date_data, older_data])

    # Configure mocks
    mock_getenv.return_value = '/tmp'

    # Call the function
    result = log_most_recent_forecasts_pentad(multi_date_data)

    # Verify correct size of result (should only include most recent date)
    assert len(result) == 3  # 3 stations in the sample data

    # Check that all dates in result are the most recent date (May 25, 2023)
    sample_date = dt.datetime(2023, 5, 25)
    assert all(date == sample_date for date in result['date'])