import os
import sys
import pandas as pd
import datetime as dt
import pytest
from unittest.mock import Mock, patch, MagicMock

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import src


# =============================================================================
# Tests for SAPPHIRE API Integration
# =============================================================================

class TestWriteRunoffToApi:
    """Tests for the _write_runoff_to_api helper function."""

    def test_api_disabled_via_env_var(self):
        """When SAPPHIRE_API_ENABLED=false, API write should be skipped."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'
        try:
            data = pd.DataFrame({
                'code': ['12345', '12345'],
                'date': ['2024-01-01', '2024-01-02'],
                'discharge': [10.5, 11.2]
            })
            result = src._write_runoff_to_api(data)
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    def test_api_disabled_case_insensitive(self):
        """SAPPHIRE_API_ENABLED should be case-insensitive."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'FALSE'
        try:
            data = pd.DataFrame({
                'code': ['12345'],
                'date': ['2024-01-01'],
                'discharge': [10.5]
            })
            result = src._write_runoff_to_api(data)
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('preprocessing_runoff.src.src.SapphirePreprocessingClient')
    def test_api_not_ready_raises_error(self, mock_client_class):
        """When API health check fails, should raise SapphireAPIError."""
        # Skip if sapphire-api-client not installed
        if not src.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = False
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': ['12345'],
                'date': ['2024-01-01'],
                'discharge': [10.5]
            })

            with pytest.raises(src.SapphireAPIError, match="not ready"):
                src._write_runoff_to_api(data)
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('preprocessing_runoff.src.src.SapphirePreprocessingClient')
    def test_api_write_success(self, mock_client_class):
        """When API write succeeds, should return True."""
        if not src.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_runoff.return_value = 2
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': ['12345', '12345'],
                'date': ['2024-01-01', '2024-01-02'],
                'discharge': [10.5, 11.2]
            })

            result = src._write_runoff_to_api(data)

            assert result is True
            mock_client.write_runoff.assert_called_once()
            # Verify records were prepared correctly
            call_args = mock_client.write_runoff.call_args[0][0]
            assert len(call_args) == 2
            assert call_args[0]['horizon_type'] == 'day'
            assert call_args[0]['code'] == '12345'
            assert call_args[0]['date'] == '2024-01-01'
            assert call_args[0]['discharge'] == 10.5
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('preprocessing_runoff.src.src.SapphirePreprocessingClient')
    def test_api_record_preparation_with_nan(self, mock_client_class):
        """NaN discharge values should become None in API records."""
        if not src.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_runoff.return_value = 2
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'code': ['12345', '12345'],
                'date': ['2024-01-01', '2024-01-02'],
                'discharge': [10.5, float('nan')]
            })

            src._write_runoff_to_api(data)

            call_args = mock_client.write_runoff.call_args[0][0]
            assert call_args[0]['discharge'] == 10.5
            assert call_args[1]['discharge'] is None
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('preprocessing_runoff.src.src.SapphirePreprocessingClient')
    def test_api_record_date_fields(self, mock_client_class):
        """API records should have correct horizon_value and horizon_in_year."""
        if not src.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_runoff.return_value = 1
            mock_client_class.return_value = mock_client

            # Use a specific date where we know the day and day_of_year
            data = pd.DataFrame({
                'code': ['12345'],
                'date': ['2024-03-15'],  # Day 15, day of year 75 (leap year)
                'discharge': [10.5]
            })

            src._write_runoff_to_api(data)

            call_args = mock_client.write_runoff.call_args[0][0]
            assert call_args[0]['horizon_value'] == 15  # Day of month
            assert call_args[0]['horizon_in_year'] == 75  # Day of year (2024 is leap year)
            assert call_args[0]['predictor'] is None
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


class TestApiCsvConsistency:
    """Tests ensuring API and CSV processes receive consistent data."""

    @patch('preprocessing_runoff.src.src._write_runoff_to_api')
    def test_api_receives_same_transformed_data_as_csv(self, mock_api_write):
        """Verify API receives data with same transformations as CSV (rounding, code format, date format)."""
        mock_api_write.return_value = True

        os.environ['ieasyforecast_intermediate_data_path'] = 'preprocessing_runoff/test/test_files'
        os.environ['ieasyforecast_daily_discharge_file'] = 'test_consistency.csv'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'

        try:
            # Input data with values that need transformation
            runoff_data = pd.DataFrame({
                'date': ['2000-01-01', '2000-01-02'],
                'discharge': [2.33333, 2.44444],  # Will be rounded to 3 decimals
                'code': [12345.0, 12345.0],  # Will have .0 stripped
                'name': ['a', 'a']
            })

            src.write_daily_time_series_data_to_csv(runoff_data)

            # Check what data was passed to API
            api_call_data = mock_api_write.call_args[0][0]

            # Read CSV to compare
            csv_data = pd.read_csv('preprocessing_runoff/test/test_files/test_consistency.csv')

            # Verify discharge rounding is consistent
            assert api_call_data['discharge'].iloc[0] == 2.333  # Rounded to 3 decimals
            assert csv_data['discharge'].iloc[0] == 2.333

            # Verify code format is consistent (no .0)
            assert api_call_data['code'].iloc[0] == '12345'  # String, no .0
            assert str(csv_data['code'].iloc[0]) == '12345'

            # Verify date format is consistent
            assert api_call_data['date'].iloc[0] == '2000-01-01'
            assert csv_data['date'].iloc[0] == '2000-01-01'

        finally:
            os.environ.pop('ieasyforecast_intermediate_data_path', None)
            os.environ.pop('ieasyforecast_daily_discharge_file', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            if os.path.exists('preprocessing_runoff/test/test_files/test_consistency.csv'):
                os.remove('preprocessing_runoff/test/test_files/test_consistency.csv')


class TestWriteDailyTimeSeriesWithApi:
    """Tests for write_daily_time_series_data_to_csv with API integration."""

    @patch('preprocessing_runoff.src.src._write_runoff_to_api')
    def test_csv_written_when_api_disabled(self, mock_api_write):
        """CSV should still be written when API is disabled."""
        mock_api_write.return_value = False

        os.environ['ieasyforecast_intermediate_data_path'] = 'preprocessing_runoff/test/test_files'
        os.environ['ieasyforecast_daily_discharge_file'] = 'test_api_disabled.csv'
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'

        try:
            runoff_data = pd.DataFrame({
                'date': ['2000-01-01', '2000-01-02'],
                'discharge': [2.3, 2.4],
                'code': [1, 1]
            })

            # Should not raise any error
            src.write_daily_time_series_data_to_csv(runoff_data)

            # CSV should be written
            output_path = 'preprocessing_runoff/test/test_files/test_api_disabled.csv'
            assert os.path.exists(output_path)
            output = pd.read_csv(output_path)
            assert len(output) == 2
        finally:
            os.environ.pop('ieasyforecast_intermediate_data_path', None)
            os.environ.pop('ieasyforecast_daily_discharge_file', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            if os.path.exists('preprocessing_runoff/test/test_files/test_api_disabled.csv'):
                os.remove('preprocessing_runoff/test/test_files/test_api_disabled.csv')

    @patch('preprocessing_runoff.src.src._write_runoff_to_api')
    def test_api_error_writes_csv_then_raises(self, mock_api_write):
        """When API fails, CSV should be written as backup, then error raised."""
        mock_api_write.side_effect = src.SapphireAPIError("API connection failed")

        os.environ['ieasyforecast_intermediate_data_path'] = 'preprocessing_runoff/test/test_files'
        os.environ['ieasyforecast_daily_discharge_file'] = 'test_api_backup.csv'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'

        try:
            runoff_data = pd.DataFrame({
                'date': ['2000-01-01', '2000-01-02'],
                'discharge': [2.3, 2.4],
                'code': [1, 1]
            })

            # Should raise SapphireAPIError
            with pytest.raises(src.SapphireAPIError, match="API write failed"):
                src.write_daily_time_series_data_to_csv(runoff_data)

            # But CSV should still be written as backup
            output_path = 'preprocessing_runoff/test/test_files/test_api_backup.csv'
            assert os.path.exists(output_path)
            output = pd.read_csv(output_path)
            assert len(output) == 2
        finally:
            os.environ.pop('ieasyforecast_intermediate_data_path', None)
            os.environ.pop('ieasyforecast_daily_discharge_file', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            if os.path.exists('preprocessing_runoff/test/test_files/test_api_backup.csv'):
                os.remove('preprocessing_runoff/test/test_files/test_api_backup.csv')

    @patch('preprocessing_runoff.src.src._write_runoff_to_api')
    def test_api_success_also_writes_csv(self, mock_api_write):
        """During transition period, both API and CSV should be written."""
        mock_api_write.return_value = True

        os.environ['ieasyforecast_intermediate_data_path'] = 'preprocessing_runoff/test/test_files'
        os.environ['ieasyforecast_daily_discharge_file'] = 'test_api_success.csv'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'

        try:
            runoff_data = pd.DataFrame({
                'date': ['2000-01-01', '2000-01-02'],
                'discharge': [2.3, 2.4],
                'code': [1, 1]
            })

            # Should not raise any error
            src.write_daily_time_series_data_to_csv(runoff_data)

            # API should have been called
            mock_api_write.assert_called_once()

            # CSV should also be written (redundancy during transition)
            output_path = 'preprocessing_runoff/test/test_files/test_api_success.csv'
            assert os.path.exists(output_path)
            output = pd.read_csv(output_path)
            assert len(output) == 2
        finally:
            os.environ.pop('ieasyforecast_intermediate_data_path', None)
            os.environ.pop('ieasyforecast_daily_discharge_file', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            if os.path.exists('preprocessing_runoff/test/test_files/test_api_success.csv'):
                os.remove('preprocessing_runoff/test/test_files/test_api_success.csv')


# =============================================================================
# Original Tests (updated to disable API during testing)
# =============================================================================

def test_get_runoff_data_no_data_available():

    os.environ['ieasyforecast_daily_discharge_path'] = 'preprocessing_runoff/test/test_files/test_config'

    output = src.get_runoff_data()
    print("Output: ")
    print(output)

    os.environ.pop('ieasyforecast_daily_discharge_path')

def test_read_runoff_data_from_multiple_rivers_xlsx():
    filename = 'preprocessing_runoff/test/test_files/test_runoff_file.xlsx'
    expected_output = pd.DataFrame({
        'date': ['2000-01-01', '2000-01-02', '2000-01-03', '2000-01-04', '2000-01-05',
                 '2000-01-01', '2000-01-02', '2000-01-03', '2000-01-04', '2000-01-05'],
        'discharge': [2.3, 2.4, 2.5, 2.6, 2.7,
                      4.3, 4.4, 4.5, 4.6, 4.7],
        'name': ['s. n. wi - spec ch', 's. n. wi - spec ch',
                  's. n. wi - spec ch', 's. n. wi - spec ch',
                  's. n. wi - spec ch',
                  'other r. - hi', 'other r. - hi', 'other r. - hi',
                  'other r. - hi', 'other r. - hi'],
        'code': [17123, 17123, 17123, 17123, 17123,
                 17456, 17456, 17456, 17456, 17456]
    }).reset_index(drop=True)
    expected_output['date'] = pd.to_datetime(expected_output['date']).dt.normalize()

    output = src.read_runoff_data_from_multiple_rivers_xlsx(filename, code_list=['17123', '17456']).reset_index(drop=True)

    assert output.equals(expected_output)

def test_read_runoff_data_from_multiple_rivers_no_code():

    filename = 'preprocessing_runoff/test/test_files/files_with_errors/test_runoff_file_no_code.xlsx'

    expected_output = pd.DataFrame({
        'date': ['2000-01-01', '2000-01-02', '2000-01-03', '2000-01-04', '2000-01-05'],
        'discharge': [2.3, 2.4, 2.5, 2.6, 2.7],
        'name': ['s. n. wi - spec ch', 's. n. wi - spec ch',
                  's. n. wi - spec ch', 's. n. wi - spec ch',
                  's. n. wi - spec ch'],
        'code': [17123, 17123, 17123, 17123, 17123]
    }).reset_index(drop=True)
    expected_output['date'] = pd.to_datetime(expected_output['date']).dt.normalize()

    output = src.read_runoff_data_from_multiple_rivers_xlsx(filename, code_list=['17123']).reset_index(drop=True)

    # assert if all values in column discharge are NaN
    assert output.equals(expected_output)

def test_read_runoff_data_from_multiple_rivers_without_data_in_xls():

    filename = 'preprocessing_runoff/test/test_files/files_with_errors/test_runoff_file_no_data.xlsx'

    output = src.read_runoff_data_from_multiple_rivers_xlsx(filename, code_list=['17123']).reset_index(drop=True)

    # assert if all values in column discharge are NaN
    assert output['discharge'].isna().all()

def test_read_runoff_data_from_multiple_rivers_no_file():

    filename = 'preprocessing_runoff/test/test_files/files_with_errors/this_file_does_not_exist.xlsx'

    # Assert FileNotFoundError is raised
    with pytest.raises(FileNotFoundError):
        src.read_runoff_data_from_multiple_rivers_xlsx(filename, code_list=[123])

def test_read_runoff_data_from_multiple_rivers_no_station_header():

    filename = 'preprocessing_runoff/test/test_files/files_with_errors/test_runoff_file_no_station_header.xlsx'

    with pytest.raises(ValueError):
        src.read_runoff_data_from_multiple_rivers_xlsx(filename, code_list=[123])

def test_read_all_runoff_data_from_excel():
    expected_output = pd.DataFrame({
        'date': ['2000-01-01', '2000-01-02', '2000-01-03', '2000-01-04', '2000-01-05',
                 '2000-01-01', '2000-01-02', '2000-01-03', '2000-01-04', '2000-01-05',
                 '2000-01-01', '2000-01-02', '2000-01-03', '2000-01-04', '2000-01-05',
                 '2001-01-01', '2001-01-02', '2001-01-03', '2001-01-04', '2001-01-05'],
        'discharge': [2.3, 2.4, 2.5, 2.6, 2.7,
                      4.3, 4.4, 4.5, 4.6, 4.7,
                      2.3, 2.4, 2.5, 2.6, 2.7,
                      4.3, 4.4, 4.5, 4.6, 4.7],
        'name': ['s. n. wi - spec ch', 's. n. wi - spec ch',
                    's. n. wi - spec ch', 's. n. wi - spec ch',
                    's. n. wi - spec ch',
                    'other r. - hi', 'other r. - hi', 'other r. - hi',
                    'other r. - hi', 'other r. - hi',
                    '', '', '', '', '', '', '', '', '', ''],
        'code': [17123, 17123, 17123, 17123, 17123,
                 17456, 17456, 17456, 17456, 17456,
                 12345, 12345, 12345, 12345, 12345,
                 12345, 12345, 12345, 12345, 12345]
    }).reset_index(drop=True)
    expected_output['date'] = pd.to_datetime(expected_output['date']).dt.normalize()

    os.environ['ieasyforecast_daily_discharge_path'] = 'preprocessing_runoff/test/test_files'

    output = src.read_all_runoff_data_from_excel(code_list=['17123', '17456', '12345']).reset_index(drop=True)

    os.environ.pop('ieasyforecast_daily_discharge_path')

    assert output.equals(expected_output)


@patch('preprocessing_runoff.src.src._write_runoff_to_api')
def test_write_data_to_csv(mock_api_write):
    """Test CSV writing functionality (API disabled for this test)."""
    mock_api_write.return_value = False  # Simulate API disabled/unavailable

    runoff_data = pd.DataFrame({
        'date': ['2000-01-01', '2000-01-02', '2000-01-03', '2000-01-04', '2000-01-05'],
        'discharge': [2.3, 2.4, 2.5, 2.6, 2.7],
        'name': ['a', 'a', 'a', 'a', 'a'],
        'code': [1, 1, 1, 1, 1]})

    # Define environment variables
    os.environ['ieasyforecast_intermediate_data_path'] = 'preprocessing_runoff/test/test_files'
    os.environ['ieasyforecast_daily_discharge_file'] = 'test_runoff_file.csv'
    os.environ['SAPPHIRE_API_ENABLED'] = 'false'  # Disable API for this test

    try:
        # Write the output file
        src.write_daily_time_series_data_to_csv(runoff_data)

        # Read the output file
        output = pd.read_csv('preprocessing_runoff/test/test_files/test_runoff_file.csv')

        # The data in columns date, discharge and code should be the same
        assert output['date'].equals(runoff_data['date'])
        assert output['discharge'].equals(runoff_data['discharge'])
        assert output['code'].equals(runoff_data['code'])
    finally:
        # Clean up the environment variables
        os.environ.pop('ieasyforecast_intermediate_data_path', None)
        os.environ.pop('ieasyforecast_daily_discharge_file', None)
        os.environ.pop('SAPPHIRE_API_ENABLED', None)

        # Remove the output file
        if os.path.exists('preprocessing_runoff/test/test_files/test_runoff_file.csv'):
            os.remove('preprocessing_runoff/test/test_files/test_runoff_file.csv')


def test_filter_roughly_for_outliers_no_outliers():
    # Create a DataFrame with no outliers
    df = pd.DataFrame({
        'Date': ['2000-01-01', '2000-01-02', '2000-01-03', '2000-01-01', '2000-01-02', '2000-01-03'],
        'Code': ['A', 'A', 'A', 'B', 'B', 'B'],
        'Q_m3s': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    })
    # Convert df['Date'] to datetime
    df['Date'] = pd.to_datetime(df['Date'])

    # Apply the function
    result = src.filter_roughly_for_outliers(df, 'Code', 'Q_m3s', 'Date')

    # Drop index
    result = result.reset_index(drop=True)

    # Check that the result is the same as the input
    pd.testing.assert_frame_equal(result, df, check_like=True)

def test_filter_roughly_for_outliers_with_outliers():
    # Create a DataFrame with an outlier
    df = pd.DataFrame({
        'Date': ['2000-01-01', '2000-01-02', '2000-01-03', '2000-01-01', '2000-01-02', '2000-01-03',
                 '2000-01-04', '2000-01-05', '2000-01-06', '2000-01-04', '2000-01-05', '2000-01-06',
                 '2000-01-07', '2000-01-08', '2000-01-09', '2000-01-07', '2000-01-08', '2000-01-09',
                 '2000-01-10', '2000-01-11', '2000-01-12', '2000-01-10', '2000-01-11', '2000-01-12',
                 '2000-01-13', '2000-01-14', '2000-01-15', '2000-01-13', '2000-01-14', '2000-01-15',
                 '2000-01-16', '2000-01-17', '2000-01-18', '2000-01-16', '2000-01-17', '2000-01-18',
                 '2000-01-19', '2000-01-20', '2000-01-21', '2000-01-19', '2000-01-20', '2000-01-21',
                 '2000-01-22', '2000-01-23', '2000-01-24', '2000-01-22', '2000-01-23', '2000-01-24',
                 '2000-01-25', '2000-01-26', '2000-01-27', '2000-01-25', '2000-01-26', '2000-01-27',
                 '2000-01-28', '2000-01-29', '2000-01-30', '2000-01-28', '2000-01-29', '2000-01-30',
                 '2000-01-31', '2000-02-01', '2000-02-02', '2000-01-31', '2000-02-01', '2000-02-02'],
        'Category': ['A', 'A', 'A', 'B', 'B', 'B',
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
        'Values': [1.01, 2.01, 3.01, 4.0, 5.0, 6.0,
                  1.02, 2.02, 3.02, 4.0, 5.0, 6.0,
                  1.03, 2.03, 3.03, 4.0, 5.0, 6.0,
                  1.04, 2.04, 3.04, 4.0, 5.0, 6.0,
                  1.05, 2.05, 3.05, 4.0, 5.0, 6.0,
                  1.06, 2.06, 18.0, 4.0, 5.0, 6.0,
                  1.07, 2.07, 3.07, 4.0, 5.0, 6.0,
                  1.08, 2.08, 3.08, 4.0, 5.0, 6.0,
                  1.09, 2.09, 3.09, 4.0, 5.0, 6.0,
                  1.10, 2.10, 3.10, 4.0, 5.0, 6.0,
                  1.11, 2.11, 3.11, 4.0, 5.0, 6.0]
    })
    # Convert df['Date'] to datetime
    df['Date'] = pd.to_datetime(df['Date'])

    # Apply the function
    result = src.filter_roughly_for_outliers(df, 'Category', 'Values', 'Date')
    # Print value on January 18th for category A
    new = (result[(result['Category']=='A') & (result['Date']=='2000-01-18')])
    old = (df[(df['Category']=='A') & (df['Date']=='2000-01-18')])

    # Check that the outlier has been replaced with NaN
    # There should be exactly one NaN value in the DataFrame column Q_m3s
    #print(result[result['Values']==100.0])
    #print(result['Values'].isna().sum())
    assert result['Values'].isna().sum() == 0
    # Assert that the outlier has been replaced with the linear interpolation
    assert new['Values'].values[0] != old['Values'].values[0]


class TestFromDailyTimeSeriestoHydrograph:
    """Test class for the from_daily_time_series_to_hydrograph function."""
    
    def test_leap_year_handling(self):
        """Test proper handling of leap years in hydrograph generation."""
        # Create a DataFrame spanning multiple years, including a leap year
        dates = []
        values = []
        
        # Create test data with dates from 2019-2021 (2020 is a leap year)
        for year in [2019, 2020, 2021]:
            # Create full year of data
            year_dates = pd.date_range(start=f'{year}-01-01', end=f'{year}-12-31')
            dates.extend(year_dates)
            
            # Add some test values (just using day of year as the value)
            values.extend([date.dayofyear for date in year_dates])
        
        # Create DataFrame
        df = pd.DataFrame({
            'date': dates,
            'discharge': values,
            'code': '15194',
            'name': 'Test Site'
        })
        
        # Run the function
        result = src.from_daily_time_series_to_hydrograph(df)
        
        # Check for leap year handling
        # We should have day_of_year values 1-365 (no 366 even though 2020 is a leap year)
        assert set(result['day_of_year'].unique()) == set(range(1, 366))
    
        # The dates in the result should be in the current year
        current_year = dt.date.today().year
        assert all(d.year == current_year for d in result['date'])
    
    # Verify that date sequence is continuous (no gaps)
        sorted_result = result.sort_values('date')
        date_diffs = sorted_result['date'].diff().iloc[1:].dt.days
        assert date_diffs.max() == 1
        assert date_diffs.min() == 1
    
    def test_statistics_calculation(self):
        """Test that statistics are correctly calculated for historical data."""
        # Get current year for testing
        current_year = dt.date.today().year
        last_year = current_year - 1
        
        # Create 5 years of data for day 1-3 of January with known patterns
        dates = []
        values = []
        
        for year in range(current_year-4, current_year+1):
            for day in range(1, 4):
                dates.append(dt.datetime(year, 1, day))
                if year == current_year:
                    values.append(day * 10)  # Current year values: 10, 20, 30
                elif year == last_year:
                    values.append(day * 5)   # Last year values: 5, 10, 15
                else:
                    values.append(day)       # Earlier years values: 1, 2, 3
        
        # Create DataFrame
        df = pd.DataFrame({
            'date': dates,
            'discharge': values,
            'code': '15194',
            'name': 'Test Site'
        })
        
        # Run the function
        result = src.from_daily_time_series_to_hydrograph(df)
        
        # Check statistics for each day
        for day in range(1, 4):
            day_result = result[result['date'].dt.day == day].iloc[0]
            
            # Check count is correct (5 years of data)
            assert day_result['count'] == 5
            
            # Check mean (3 early years with value=day, last year with 5*day, current year with 10*day)
            expected_mean = (day*3 + day*5 + day*10) / 5
            assert abs(day_result['mean'] - expected_mean) < 0.0001
            
            # Check percentiles
            assert day_result['min'] == day  # Minimum is just the day value
            assert day_result['max'] == day * 10  # Maximum is current year value
            
            # Check current and previous year values
            assert day_result[str(current_year)] == day * 10
            assert day_result[str(last_year)] == day * 5
    
    def test_multiple_sites(self):
        """Test processing of multiple sites within the same dataset."""
        # Create test data for two sites
        dates = pd.date_range(start='2021-01-01', periods=10, freq='D')
        
        data = []
        for site_code in ['15194', '15212']:
            for date in dates:
                # Different pattern for each site
                if site_code == '15194':
                    value = date.day
                else:
                    value = date.day * 2
                
                data.append({
                    'date': date,
                    'discharge': value,
                    'code': site_code,
                    'name': f'Test Site {site_code}'
                })
        
        df = pd.DataFrame(data)
        
        # Run the function
        result = src.from_daily_time_series_to_hydrograph(df)
        
        # Verify each site is processed separately
        site_groups = result.groupby('code')
        assert len(site_groups) == 2
        
        # Check each site has the correct data
        site1_data = site_groups.get_group('15194')
        site2_data = site_groups.get_group('15212')
        
        # Both sites should have same number of days
        assert len(site1_data) == len(dates)
        assert len(site2_data) == len(dates)
        
        # Check that means reflect the different patterns
        for day in range(1, 11):
            site1_day = site1_data[site1_data['date'].dt.day == day]
            site2_day = site2_data[site2_data['date'].dt.day == day]
            
            if not site1_day.empty and not site2_day.empty:
                assert site1_day['mean'].iloc[0] == day
                assert site2_day['mean'].iloc[0] == day * 2


# =============================================================================
# Tests for Hydrograph API Integration
# =============================================================================

class TestWriteHydrographToApi:
    """Tests for the _write_hydrograph_to_api helper function."""

    def test_api_disabled_via_env_var(self):
        """When SAPPHIRE_API_ENABLED=false, hydrograph API write should be skipped."""
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'
        try:
            current_year = dt.date.today().year
            data = pd.DataFrame({
                'code': ['12345', '12345'],
                'date': ['2024-01-01', '2024-01-02'],
                'day_of_year': [1, 2],
                'count': [10, 10],
                'mean': [5.5, 6.5],
                'std': [1.0, 1.0],
                'min': [3.0, 4.0],
                'max': [8.0, 9.0],
                '5%': [3.5, 4.5],
                '25%': [4.5, 5.5],
                '50%': [5.5, 6.5],
                '75%': [6.5, 7.5],
                '95%': [7.5, 8.5],
                str(current_year): [10.0, 11.0],
                str(current_year - 1): [9.0, 10.0],
            })
            result = src._write_hydrograph_to_api(data)
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('preprocessing_runoff.src.src.SapphirePreprocessingClient')
    def test_hydrograph_api_write_success(self, mock_client_class):
        """When hydrograph API write succeeds, should return True."""
        if not src.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_hydrograph.return_value = 2
            mock_client_class.return_value = mock_client

            current_year = dt.date.today().year
            data = pd.DataFrame({
                'code': ['12345', '12345'],
                'date': ['2024-01-01', '2024-01-02'],
                'day_of_year': [1, 2],
                'count': [10, 10],
                'mean': [5.5, 6.5],
                'std': [1.0, 1.0],
                'min': [3.0, 4.0],
                'max': [8.0, 9.0],
                '5%': [3.5, 4.5],
                '25%': [4.5, 5.5],
                '50%': [5.5, 6.5],
                '75%': [6.5, 7.5],
                '95%': [7.5, 8.5],
                str(current_year): [10.0, 11.0],
                str(current_year - 1): [9.0, 10.0],
            })

            result = src._write_hydrograph_to_api(data)

            assert result is True
            mock_client.write_hydrograph.assert_called_once()
            # Verify records were prepared correctly
            call_args = mock_client.write_hydrograph.call_args[0][0]
            assert len(call_args) == 2
            assert call_args[0]['horizon_type'] == 'day'
            assert call_args[0]['code'] == '12345'
            # Verify percentile column mapping
            assert call_args[0]['q05'] == 3.5
            assert call_args[0]['q25'] == 4.5
            assert call_args[0]['q50'] == 5.5
            assert call_args[0]['q75'] == 6.5
            assert call_args[0]['q95'] == 7.5
            # Verify current/previous year mapping
            assert call_args[0]['current'] == 10.0
            assert call_args[0]['previous'] == 9.0
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('preprocessing_runoff.src.src.SapphirePreprocessingClient')
    def test_hydrograph_api_handles_nan_values(self, mock_client_class):
        """NaN values in hydrograph data should become None in API records."""
        if not src.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_hydrograph.return_value = 1
            mock_client_class.return_value = mock_client

            current_year = dt.date.today().year
            data = pd.DataFrame({
                'code': ['12345'],
                'date': ['2024-01-01'],
                'day_of_year': [1],
                'count': [10],
                'mean': [5.5],
                'std': [float('nan')],  # NaN value
                'min': [3.0],
                'max': [8.0],
                '5%': [float('nan')],  # NaN value
                '25%': [4.5],
                '50%': [5.5],
                '75%': [6.5],
                '95%': [7.5],
                str(current_year): [float('nan')],  # NaN value
                str(current_year - 1): [9.0],
            })

            src._write_hydrograph_to_api(data)

            call_args = mock_client.write_hydrograph.call_args[0][0]
            assert call_args[0]['std'] is None
            assert call_args[0]['q05'] is None
            assert call_args[0]['current'] is None
            assert call_args[0]['previous'] == 9.0  # Non-NaN should be preserved
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


class TestWriteDailyHydrographWithApi:
    """Tests for write_daily_hydrograph_data_to_csv with API integration."""

    @patch('preprocessing_runoff.src.src._write_hydrograph_to_api')
    def test_hydrograph_csv_written_when_api_disabled(self, mock_api_write):
        """CSV should still be written when API is disabled."""
        mock_api_write.return_value = False

        os.environ['ieasyforecast_intermediate_data_path'] = 'preprocessing_runoff/test/test_files'
        os.environ['ieasyforecast_hydrograph_day_file'] = 'test_hydrograph_api_disabled.csv'
        os.environ['SAPPHIRE_API_ENABLED'] = 'false'

        try:
            current_year = dt.date.today().year
            hydrograph_data = pd.DataFrame({
                'code': ['12345', '12345'],
                'date': ['2024-01-01', '2024-01-02'],
                'day_of_year': [1, 2],
                'count': [10, 10],
                'mean': [5.5, 6.5],
            })

            # Should not raise any error
            src.write_daily_hydrograph_data_to_csv(
                hydrograph_data,
                column_list=['code', 'date', 'day_of_year', 'count', 'mean']
            )

            # CSV should be written
            output_path = 'preprocessing_runoff/test/test_files/test_hydrograph_api_disabled.csv'
            assert os.path.exists(output_path)
            output = pd.read_csv(output_path)
            assert len(output) == 2
        finally:
            os.environ.pop('ieasyforecast_intermediate_data_path', None)
            os.environ.pop('ieasyforecast_hydrograph_day_file', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            if os.path.exists('preprocessing_runoff/test/test_files/test_hydrograph_api_disabled.csv'):
                os.remove('preprocessing_runoff/test/test_files/test_hydrograph_api_disabled.csv')

    @patch('preprocessing_runoff.src.src._write_hydrograph_to_api')
    def test_hydrograph_api_data_parameter_filters_api_write(self, mock_api_write):
        """When api_data is provided, only that subset should be sent to API."""
        mock_api_write.return_value = True

        os.environ['ieasyforecast_intermediate_data_path'] = 'preprocessing_runoff/test/test_files'
        os.environ['ieasyforecast_hydrograph_day_file'] = 'test_hydrograph_api_data.csv'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'

        try:
            # Full hydrograph data (365 days worth)
            full_data = pd.DataFrame({
                'code': ['12345'] * 10,
                'date': pd.date_range('2024-01-01', periods=10).strftime('%Y-%m-%d').tolist(),
                'day_of_year': list(range(1, 11)),
                'count': [10] * 10,
                'mean': [5.5] * 10,
            })

            # API data - only today's row
            api_data = pd.DataFrame({
                'code': ['12345'],
                'date': ['2024-01-05'],
                'day_of_year': [5],
                'count': [10],
                'mean': [5.5],
            })

            src.write_daily_hydrograph_data_to_csv(
                full_data,
                column_list=['code', 'date', 'day_of_year', 'count', 'mean'],
                api_data=api_data
            )

            # API should have been called with only 1 row
            mock_api_write.assert_called_once()
            api_call_data = mock_api_write.call_args[0][0]
            assert len(api_call_data) == 1

            # CSV should have all 10 rows
            output_path = 'preprocessing_runoff/test/test_files/test_hydrograph_api_data.csv'
            output = pd.read_csv(output_path)
            assert len(output) == 10
        finally:
            os.environ.pop('ieasyforecast_intermediate_data_path', None)
            os.environ.pop('ieasyforecast_hydrograph_day_file', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            if os.path.exists('preprocessing_runoff/test/test_files/test_hydrograph_api_data.csv'):
                os.remove('preprocessing_runoff/test/test_files/test_hydrograph_api_data.csv')

    @patch('preprocessing_runoff.src.src._write_hydrograph_to_api')
    def test_hydrograph_empty_api_data_skips_api_write(self, mock_api_write):
        """When api_data is empty, API write should be skipped."""
        mock_api_write.return_value = True

        os.environ['ieasyforecast_intermediate_data_path'] = 'preprocessing_runoff/test/test_files'
        os.environ['ieasyforecast_hydrograph_day_file'] = 'test_hydrograph_empty_api.csv'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'

        try:
            full_data = pd.DataFrame({
                'code': ['12345'],
                'date': ['2024-01-01'],
                'day_of_year': [1],
                'count': [10],
                'mean': [5.5],
            })

            # Empty api_data
            api_data = pd.DataFrame()

            src.write_daily_hydrograph_data_to_csv(
                full_data,
                column_list=['code', 'date', 'day_of_year', 'count', 'mean'],
                api_data=api_data
            )

            # API should NOT have been called
            mock_api_write.assert_not_called()

            # CSV should still be written
            output_path = 'preprocessing_runoff/test/test_files/test_hydrograph_empty_api.csv'
            assert os.path.exists(output_path)
        finally:
            os.environ.pop('ieasyforecast_intermediate_data_path', None)
            os.environ.pop('ieasyforecast_hydrograph_day_file', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            if os.path.exists('preprocessing_runoff/test/test_files/test_hydrograph_empty_api.csv'):
                os.remove('preprocessing_runoff/test/test_files/test_hydrograph_empty_api.csv')


# =============================================================================
# Tests for Incremental Sync (RunoffDataResult)
# =============================================================================

class TestRunoffDataResult:
    """Tests for RunoffDataResult dataclass and incremental sync."""

    def test_runoff_data_result_structure(self):
        """RunoffDataResult should have full_data and new_data attributes."""
        full_data = pd.DataFrame({'code': ['12345'], 'date': ['2024-01-01'], 'discharge': [10.0]})
        new_data = pd.DataFrame({'code': ['12345'], 'date': ['2024-01-02'], 'discharge': [11.0]})

        result = src.RunoffDataResult(full_data=full_data, new_data=new_data)

        assert hasattr(result, 'full_data')
        assert hasattr(result, 'new_data')
        assert len(result.full_data) == 1
        assert len(result.new_data) == 1

    def test_runoff_data_result_with_empty_new_data(self):
        """RunoffDataResult should handle empty new_data gracefully."""
        full_data = pd.DataFrame({'code': ['12345'], 'date': ['2024-01-01'], 'discharge': [10.0]})
        new_data = pd.DataFrame()

        result = src.RunoffDataResult(full_data=full_data, new_data=new_data)

        assert len(result.full_data) == 1
        assert result.new_data.empty


class TestIncrementalRunoffSync:
    """Tests for incremental runoff sync via api_data parameter."""

    @patch('preprocessing_runoff.src.src._write_runoff_to_api')
    def test_api_data_parameter_sends_only_subset(self, mock_api_write):
        """When api_data is provided, only that data should be sent to API."""
        mock_api_write.return_value = True

        os.environ['ieasyforecast_intermediate_data_path'] = 'preprocessing_runoff/test/test_files'
        os.environ['ieasyforecast_daily_discharge_file'] = 'test_incremental_sync.csv'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'

        try:
            # Full historical data
            full_data = pd.DataFrame({
                'date': pd.date_range('2024-01-01', periods=100).strftime('%Y-%m-%d').tolist(),
                'discharge': [10.0] * 100,
                'code': ['12345'] * 100,
            })

            # Only new data (last 2 records)
            new_data = pd.DataFrame({
                'date': ['2024-04-09', '2024-04-10'],
                'discharge': [15.0, 16.0],
                'code': ['12345', '12345'],
            })

            src.write_daily_time_series_data_to_csv(
                full_data,
                column_list=['code', 'date', 'discharge'],
                api_data=new_data
            )

            # API should receive only 2 records (new data)
            api_call_data = mock_api_write.call_args[0][0]
            assert len(api_call_data) == 2

            # CSV should have all 100 records
            output_path = 'preprocessing_runoff/test/test_files/test_incremental_sync.csv'
            output = pd.read_csv(output_path)
            assert len(output) == 100
        finally:
            os.environ.pop('ieasyforecast_intermediate_data_path', None)
            os.environ.pop('ieasyforecast_daily_discharge_file', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            if os.path.exists('preprocessing_runoff/test/test_files/test_incremental_sync.csv'):
                os.remove('preprocessing_runoff/test/test_files/test_incremental_sync.csv')

    @patch('preprocessing_runoff.src.src._write_runoff_to_api')
    def test_empty_api_data_skips_api_write(self, mock_api_write):
        """When api_data is empty DataFrame, API write should be skipped."""
        mock_api_write.return_value = True

        os.environ['ieasyforecast_intermediate_data_path'] = 'preprocessing_runoff/test/test_files'
        os.environ['ieasyforecast_daily_discharge_file'] = 'test_empty_api_data.csv'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'

        try:
            full_data = pd.DataFrame({
                'date': ['2024-01-01'],
                'discharge': [10.0],
                'code': ['12345'],
            })

            # Empty new data (data is already synced)
            api_data = pd.DataFrame()

            src.write_daily_time_series_data_to_csv(
                full_data,
                column_list=['code', 'date', 'discharge'],
                api_data=api_data
            )

            # API should NOT have been called
            mock_api_write.assert_not_called()

            # CSV should still be written
            output_path = 'preprocessing_runoff/test/test_files/test_empty_api_data.csv'
            assert os.path.exists(output_path)
        finally:
            os.environ.pop('ieasyforecast_intermediate_data_path', None)
            os.environ.pop('ieasyforecast_daily_discharge_file', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            if os.path.exists('preprocessing_runoff/test/test_files/test_empty_api_data.csv'):
                os.remove('preprocessing_runoff/test/test_files/test_empty_api_data.csv')


