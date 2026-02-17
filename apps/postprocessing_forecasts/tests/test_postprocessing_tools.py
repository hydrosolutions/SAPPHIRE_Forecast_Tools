import os
import sys
import pytest
import pandas as pd
import numpy as np
import datetime as dt
from unittest.mock import patch, MagicMock, mock_open

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the functions to test
from src.postprocessing_tools import (
    log_most_recent_forecasts_pentad,
    log_most_recent_forecasts_decade,
    log_most_recent_forecasts_monthly,
    forecast_target_date,
)

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
                        'TIDE', 'TIDE', 'TIDE', 'EM', 'EM', 'EM']
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


# ============================================================================
# Bug 3 Fix Tests: Unsafe .iloc[0] access
# These tests verify that the functions handle edge cases where filtering
# by date could return empty results (e.g., NaT dates)
# ============================================================================

@patch('os.makedirs')
@patch('os.getenv')
@patch('pandas.DataFrame.to_csv')
def test_log_most_recent_forecasts_pentad_nat_dates(mock_to_csv, mock_getenv, mock_makedirs):
    """Test that the function handles NaT (Not a Time) dates gracefully.

    This tests Bug 3: when a missing code has only NaT dates, the filter
    by date would return an empty DataFrame, and .iloc[0] would crash.
    The fix adds a check for empty filtered results before accessing .iloc[0].
    """
    # Create data where one station has valid dates, but a "missing" station
    # would have only NaT dates
    sample_date = dt.datetime(2023, 5, 25)

    # Main station with valid data
    valid_data = pd.DataFrame({
        'code': ['15102'] * 4,
        'date': [sample_date] * 4,
        'pentad_in_month': [5] * 4,
        'pentad_in_year': [30] * 4,
        'forecasted_discharge': [125.4, 130.2, 128.7, 129.1],
        'model_short': ['LR', 'TFT', 'TIDE', 'EM']
    })

    # "Missing" station with NaT date - this station won't appear in recent forecasts
    # because it doesn't have data for the most recent date
    nat_data = pd.DataFrame({
        'code': ['15999'] * 4,
        'date': [pd.NaT] * 4,  # NaT dates
        'pentad_in_month': [5] * 4,
        'pentad_in_year': [30] * 4,
        'forecasted_discharge': [100.0, 110.0, 105.0, 107.5],
        'model_short': ['LR', 'TFT', 'TIDE', 'EM']
    })

    combined_data = pd.concat([valid_data, nat_data], ignore_index=True)

    mock_getenv.return_value = '/tmp'

    # This should NOT raise IndexError with the fix
    result = log_most_recent_forecasts_pentad(combined_data)

    # The result should include the valid station with exact count
    assert len(result) == 1, (
        f"Only station 15102 has valid dates, expected 1 row, got {len(result)}"
    )
    assert result.iloc[0]['code'] == '15102'
    # Verify model discharge values for valid station
    assert result.iloc[0]['LR'] == pytest.approx(125.4)
    assert result.iloc[0]['TFT'] == pytest.approx(130.2)
    assert result.iloc[0]['EM'] == pytest.approx(129.1)


@patch('os.makedirs')
@patch('os.getenv')
@patch('pandas.DataFrame.to_csv')
def test_log_most_recent_forecasts_pentad_missing_code_no_matching_date(mock_to_csv, mock_getenv, mock_makedirs):
    """Test handling of missing codes where date filter returns no matches.

    This tests the scenario where a code exists in historical data but
    when we try to get its most recent pentad, the date filter returns empty.
    """
    sample_date = dt.datetime(2023, 5, 25)
    older_date = dt.datetime(2023, 5, 20)

    # Station 15102 has recent data
    recent_data = pd.DataFrame({
        'code': ['15102'] * 4,
        'date': [sample_date] * 4,
        'pentad_in_month': [5] * 4,
        'pentad_in_year': [30] * 4,
        'forecasted_discharge': [125.4, 130.2, 128.7, 129.1],
        'model_short': ['LR', 'TFT', 'TIDE', 'EM']
    })

    # Station 15999 only has older data - will be a "missing code" in recent forecasts
    old_data = pd.DataFrame({
        'code': ['15999'] * 4,
        'date': [older_date] * 4,
        'pentad_in_month': [4] * 4,
        'pentad_in_year': [29] * 4,
        'forecasted_discharge': [100.0, 110.0, 105.0, 107.5],
        'model_short': ['LR', 'TFT', 'TIDE', 'EM']
    })

    combined_data = pd.concat([recent_data, old_data], ignore_index=True)

    mock_getenv.return_value = '/tmp'

    # This should NOT raise IndexError
    result = log_most_recent_forecasts_pentad(combined_data)

    # Both stations present: 15102 with forecasts, 15999 as missing-code row
    assert len(result) == 2, (
        f"Expected 2 rows (1 active + 1 missing-code), got {len(result)}"
    )
    assert '15102' in result['code'].values
    assert '15999' in result['code'].values
    # Active station has real forecasts
    row_15102 = result[result['code'] == '15102'].iloc[0]
    assert row_15102['LR'] == pytest.approx(125.4)
    assert row_15102['TFT'] == pytest.approx(130.2)
    # Missing-code station has NaN placeholders for all models
    row_15999 = result[result['code'] == '15999'].iloc[0]
    assert pd.isna(row_15999['LR'])
    assert pd.isna(row_15999['TFT'])


# ============================================================================
# Decade Function Tests
# ============================================================================

@pytest.fixture
def sample_decade_data():
    """Create sample decade forecast data for testing."""
    sample_date = dt.datetime(2023, 5, 25)
    data = pd.DataFrame({
        'code': ['15102', '15124', '15136', '15102', '15124', '15136',
                 '15102', '15124', '15136', '15102', '15124', '15136'],
        'date': [sample_date] * 12,
        'decad_in_month': [3] * 12,
        'decad_in_year': [15] * 12,
        'forecasted_discharge': [125.4, 45.7, 67.8, 130.2, 47.2, 65.1,
                                 128.7, 46.9, 66.5, 129.1, 47.0, 66.3],
        'model_short': ['LR', 'LR', 'LR', 'TFT', 'TFT', 'TFT',
                        'TIDE', 'TIDE', 'TIDE', 'EM', 'EM', 'EM']
    })
    return data


@patch('os.makedirs')
@patch('os.getenv')
@patch('pandas.DataFrame.to_csv')
def test_log_most_recent_forecasts_decade(mock_to_csv, mock_getenv, mock_makedirs, sample_decade_data):
    """Test the log_most_recent_forecasts_decade function with sample data."""
    mock_getenv.return_value = '/tmp'

    result = log_most_recent_forecasts_decade(sample_decade_data)

    # Verify the result is a DataFrame with correct shape
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 3

    # Check that required columns exist
    assert 'code' in result.columns
    assert 'date' in result.columns
    assert 'decad_in_month' in result.columns

    # Verify model columns present and spot-check values
    for model in ['LR', 'TFT', 'TIDE', 'EM']:
        assert model in result.columns, f"Model column {model} missing"
    # Spot-check station 15102 discharge values
    row_15102 = result[result['code'] == '15102'].iloc[0]
    assert row_15102['LR'] == pytest.approx(125.4)
    assert row_15102['TFT'] == pytest.approx(130.2)
    assert row_15102['EM'] == pytest.approx(129.1)
    # All dates are the sample date
    sample_date = dt.datetime(2023, 5, 25)
    assert all(d == sample_date for d in result['date'])


@patch('os.makedirs')
@patch('os.getenv')
@patch('pandas.DataFrame.to_csv')
def test_log_most_recent_forecasts_decade_empty_data(mock_to_csv, mock_getenv, mock_makedirs, sample_decade_data):
    """Test the decade function with empty data."""
    empty_data = pd.DataFrame(columns=sample_decade_data.columns)

    mock_getenv.return_value = '/tmp'

    result = log_most_recent_forecasts_decade(empty_data)

    mock_to_csv.assert_not_called()
    assert result.empty


@patch('os.makedirs')
@patch('os.getenv')
@patch('pandas.DataFrame.to_csv')
def test_log_most_recent_forecasts_decade_nat_dates(mock_to_csv, mock_getenv, mock_makedirs):
    """Test that the decade function handles NaT dates gracefully.

    This tests Bug 3 for the decade version.
    """
    sample_date = dt.datetime(2023, 5, 25)

    # Valid station
    valid_data = pd.DataFrame({
        'code': ['15102'] * 4,
        'date': [sample_date] * 4,
        'decad_in_month': [3] * 4,
        'decad_in_year': [15] * 4,
        'forecasted_discharge': [125.4, 130.2, 128.7, 129.1],
        'model_short': ['LR', 'TFT', 'TIDE', 'EM']
    })

    # Station with NaT dates
    nat_data = pd.DataFrame({
        'code': ['15999'] * 4,
        'date': [pd.NaT] * 4,
        'decad_in_month': [3] * 4,
        'decad_in_year': [15] * 4,
        'forecasted_discharge': [100.0, 110.0, 105.0, 107.5],
        'model_short': ['LR', 'TFT', 'TIDE', 'EM']
    })

    combined_data = pd.concat([valid_data, nat_data], ignore_index=True)

    mock_getenv.return_value = '/tmp'

    # Should NOT raise IndexError
    result = log_most_recent_forecasts_decade(combined_data)

    # Exact count: only station 15102 has valid dates
    assert len(result) == 1, (
        f"Only station 15102 has valid dates, expected 1 row, got {len(result)}"
    )
    assert result.iloc[0]['code'] == '15102'
    # Verify model discharge values
    assert result.iloc[0]['LR'] == pytest.approx(125.4)
    assert result.iloc[0]['TFT'] == pytest.approx(130.2)
    assert result.iloc[0]['EM'] == pytest.approx(129.1)


# ===================================================================
# forecast_target_date tests (Commit 6)
# ===================================================================

class TestForecastTargetDate:
    """Tests for forecast_target_date helper.

    Convention: short-term forecast `date` = production date (last day
    of previous period); `date + 1` = first day of target period.
    Long-term forecasts use valid_from/valid_to instead.
    """

    def test_scalar_date(self):
        """Single date: Jan 5 -> Jan 6."""
        result = forecast_target_date(dt.date(2024, 1, 5))
        assert result == dt.date(2024, 1, 6)

    def test_series_dates(self):
        """pandas Series input produces +1 day output."""
        dates = pd.Series(pd.to_datetime(['2024-01-05', '2024-01-10']))
        result = forecast_target_date(dates)
        expected = pd.Series(pd.to_datetime(['2024-01-06', '2024-01-11']))
        pd.testing.assert_series_equal(result, expected)

    def test_year_boundary(self):
        """Dec 31 -> Jan 1 next year."""
        result = forecast_target_date(dt.date(2024, 12, 31))
        assert result == dt.date(2025, 1, 1)

    def test_leap_year_feb28(self):
        """Feb 28 2024 (leap year) -> Feb 29 2024."""
        result = forecast_target_date(dt.date(2024, 2, 28))
        assert result == dt.date(2024, 2, 29)

    def test_non_leap_year_feb28(self):
        """Feb 28 2023 (not leap year) -> Mar 1 2023."""
        result = forecast_target_date(dt.date(2023, 2, 28))
        assert result == dt.date(2023, 3, 1)


# ===================================================================
# Monthly logging tests
# ===================================================================

class TestLogMostRecentForecastsMonthly:
    """Tests for log_most_recent_forecasts_monthly()."""

    @pytest.fixture
    def sample_monthly_data(self):
        """Monthly joint forecast data with 2 stations, 2 models."""
        return pd.DataFrame({
            'code': ['15013', '15013', '15014', '15014',
                     '15013', '15013', '15014', '15014'],
            'year': [2023, 2023, 2023, 2023,
                     2024, 2024, 2024, 2024],
            'month_in_year': [6, 6, 6, 6, 6, 6, 6, 6],
            'forecasted_discharge': [100.0, 105.0, 200.0, 210.0,
                                     102.0, 107.0, 202.0, 212.0],
            'model_short': ['GBT', 'EM', 'GBT', 'EM',
                            'GBT', 'EM', 'GBT', 'EM'],
        })

    @patch('os.makedirs')
    @patch('os.getenv')
    @patch('pandas.DataFrame.to_csv')
    def test_happy_path(self, mock_to_csv, mock_getenv,
                        mock_makedirs, sample_monthly_data):
        """Returns pivoted DataFrame with correct shape."""
        mock_getenv.return_value = '/tmp'

        result = log_most_recent_forecasts_monthly(sample_monthly_data)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2  # 2 stations
        assert 'GBT' in result.columns
        assert 'EM' in result.columns
        mock_to_csv.assert_called_once()

    @patch('os.makedirs')
    @patch('os.getenv')
    @patch('pandas.DataFrame.to_csv')
    def test_uses_most_recent_year_month(
        self, mock_to_csv, mock_getenv, mock_makedirs,
        sample_monthly_data,
    ):
        """Only the most recent (year, month) combination is used."""
        mock_getenv.return_value = '/tmp'

        result = log_most_recent_forecasts_monthly(sample_monthly_data)

        # Most recent is year=2024, month=6
        assert all(result['year'] == 2024)
        assert all(result['month_in_year'] == 6)

    def test_empty_data(self):
        """Empty DataFrame returns empty DataFrame."""
        result = log_most_recent_forecasts_monthly(pd.DataFrame())
        assert result.empty

    def test_none_data(self):
        """None input returns empty DataFrame."""
        result = log_most_recent_forecasts_monthly(None)
        assert result.empty

    @patch('os.makedirs')
    @patch('os.getenv')
    @patch('pandas.DataFrame.to_csv')
    def test_single_model(self, mock_to_csv, mock_getenv,
                          mock_makedirs):
        """Single model still produces valid pivot."""
        mock_getenv.return_value = '/tmp'

        data = pd.DataFrame({
            'code': ['15013'],
            'year': [2024],
            'month_in_year': [3],
            'forecasted_discharge': [100.0],
            'model_short': ['GBT'],
        })
        result = log_most_recent_forecasts_monthly(data)

        assert len(result) == 1
        assert result.iloc[0]['GBT'] == pytest.approx(100.0)