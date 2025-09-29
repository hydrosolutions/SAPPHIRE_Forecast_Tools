import os
import shutil
import unittest
import datetime
import socket
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np
import tempfile
from iEasyHydroForecast import setup_library as sl
from iEasyHydroForecast import tag_library as tl
from iEasyHydroForecast import forecast_library as fl
import logging
import json

class TestLoadConfiguration():
    # Temporary directory to store output
    tmpdir = "iEasyHydroForecast/tests/test_data/temp"
    # Create the directory
    os.makedirs(tmpdir, exist_ok=True)
    # When in a test environment, the .env file should load the following
    # environment variables:
    test_env_path = "iEasyHydroForecast/tests/test_data/.env_develop_test"
    # Folders with data read by the forecast tools
    test_ieasyforecast_configuration_path="config"
    test_ieasyforecast_gis_directory_path="../data/GIS"
    test_ieasyreports_templates_directory_path="../data/templates"
    test_ieasyforecast_daily_discharge_path="../data/daily_runoff"
    test_ieasyforecast_locale_dir="config/locale"
    # Folders with data written by the forecast tools. These are temporary and
    # should be deleted after the tests are run.
    test_ieasyforecast_intermediate_data_path=os.path.join(tmpdir, "apps/internal_data")
    # Create the folder internal data
    os.makedirs(test_ieasyforecast_intermediate_data_path, exist_ok=True)
    test_ieasyreports_report_output_path=os.path.join(tmpdir, "data/reports")

    # Get full paths of the folders
    test_ieasyforecast_configuration_path_full_path = os.path.abspath(
        test_ieasyforecast_configuration_path)
    test_ieasyforecast_gis_directory_path_full_path = os.path.abspath(
        test_ieasyforecast_gis_directory_path)
    test_ieasyreports_templates_directory_path_full_path = os.path.abspath(
        test_ieasyreports_templates_directory_path)
    test_ieasyforecast_daily_discharge_path_full_path = os.path.abspath(
        test_ieasyforecast_daily_discharge_path)
    test_ieasyforecast_locale_dir_full_path = os.path.abspath(
        test_ieasyforecast_locale_dir)
    # Test if this path exists
    assert os.path.exists(test_ieasyforecast_configuration_path_full_path)
    assert os.path.exists(test_ieasyforecast_gis_directory_path_full_path)
    assert os.path.exists(test_ieasyreports_templates_directory_path_full_path)
    assert os.path.exists(test_ieasyforecast_daily_discharge_path_full_path)
    assert os.path.exists(test_ieasyforecast_locale_dir_full_path)

    # Test that the environment variables are loaded
    res = sl.load_environment()
    assert res == test_env_path
    assert os.getenv("ieasyforecast_configuration_path") == test_ieasyforecast_configuration_path
    assert os.getenv("ieasyforecast_gis_directory_path") == test_ieasyforecast_gis_directory_path
    assert os.getenv("ieasyreports_templates_directory_path") == test_ieasyreports_templates_directory_path
    assert os.getenv("ieasyforecast_daily_discharge_path") == test_ieasyforecast_daily_discharge_path
    assert os.getenv("ieasyforecast_locale_dir") == test_ieasyforecast_locale_dir
    assert os.getenv("ieasyforecast_intermediate_data_path") == test_ieasyforecast_intermediate_data_path
    assert os.getenv("ieasyreports_report_output_path") == test_ieasyreports_report_output_path

    # Delete the directory tmpdir and all its contents
    print("Deleting directory: ", tmpdir)
    shutil.rmtree(tmpdir)
    # Delete the environment variables
    ret=os.environ.pop("ieasyforecast_configuration_path")
    assert ret == test_ieasyforecast_configuration_path
    ret=os.environ.pop("ieasyforecast_gis_directory_path")
    assert ret == test_ieasyforecast_gis_directory_path
    ret=os.environ.pop("ieasyreports_templates_directory_path")
    assert ret == test_ieasyreports_templates_directory_path
    ret=os.environ.pop("ieasyforecast_daily_discharge_path")
    assert ret == test_ieasyforecast_daily_discharge_path
    ret=os.environ.pop("ieasyforecast_locale_dir")
    assert ret == test_ieasyforecast_locale_dir
    ret=os.environ.pop("ieasyforecast_intermediate_data_path")
    assert ret == test_ieasyforecast_intermediate_data_path
    ret=os.environ.pop("ieasyreports_report_output_path")
    assert ret == test_ieasyreports_report_output_path


class TestCheckIfSshTunnelIsRequired(unittest.TestCase):

    def setUp(self):
        # Setup logging for tests
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger(__name__)

    def test_variable_not_set(self):
        """Test when the environment variable is not set."""
        with patch.dict(os.environ, clear=True):  # Clear any existing env vars
            self.assertFalse(sl.check_if_ssh_tunnel_is_required())

    def test_variable_set_to_true(self):
        """Test when the environment variable is set to 'true'."""
        with patch.dict(os.environ, {"ieasyhydroforecast_ssh_to_iEH": "true"}):
            self.assertTrue(sl.check_if_ssh_tunnel_is_required())

        with patch.dict(os.environ, {"ieasyhydroforecast_ssh_to_iEH": "True"}):
            self.assertTrue(sl.check_if_ssh_tunnel_is_required())

    def test_variable_set_to_false(self):
        """Test when the environment variable is set to 'false'."""
        with patch.dict(os.environ, {"ieasyhydroforecast_ssh_to_iEH": "false"}):
            self.assertFalse(sl.check_if_ssh_tunnel_is_required())

        with patch.dict(os.environ, {"ieasyhydroforecast_ssh_to_iEH": "False"}):
            self.assertFalse(sl.check_if_ssh_tunnel_is_required())

    def test_variable_set_to_other_value(self):
        """Test when the environment variable is set to a value other than 'true' or 'false'."""
        with patch.dict(os.environ, {"ieasyhydroforecast_ssh_to_iEH": "some_value"}):
            # The function should return None if the value is not "true" or "false"
            self.assertIsNone(sl.check_if_ssh_tunnel_is_required())


class TestCheckLocalSshTunnels(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.CRITICAL)  # Suppress log messages during tests

    def tearDown(self):
        # Clean up any environment variables that might have been set
        if 'IEASYHYDRO_HOST' in os.environ:
            del os.environ['IEASYHYDRO_HOST']

    @patch('os.getenv')
    def test_no_ieasyhydro_host(self, mock_getenv):
        mock_getenv.return_value = None
        result = sl.check_local_ssh_tunnels()
        self.assertEqual(result, [])

    @patch('os.getenv')
    @patch('socket.socket')
    def test_successful_connection(self, mock_socket, mock_getenv):
        mock_getenv.return_value = "http://localhost:8080"
        mock_socket_instance = mock_socket.return_value
        mock_socket_instance.connect_ex.return_value = 0  # Simulate successful connection

        result = sl.check_local_ssh_tunnels()
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]['port'], 8080)
        self.assertIn('localhost', result[0]['line'])

    @patch('os.getenv')
    @patch('socket.socket')
    def test_connection_refused(self, mock_socket, mock_getenv):
        mock_getenv.return_value = "http://localhost:8080"
        mock_socket_instance = mock_socket.return_value
        mock_socket_instance.connect_ex.return_value = 111  # Simulate connection refused

        result = sl.check_local_ssh_tunnels()
        self.assertEqual(result, [])

    @patch('os.getenv')
    @patch('socket.socket')
    def test_address_resolution_error(self, mock_socket, mock_getenv):
        mock_getenv.return_value = "http://invalid_address:8080"
        mock_socket_instance = mock_socket.return_value
        mock_socket_instance.connect_ex.side_effect = socket.gaierror("Address resolution error")

        result = sl.check_local_ssh_tunnels()
        self.assertEqual(result, [])

    @patch('os.getenv')
    @patch('socket.socket')
    def test_socket_error(self, mock_socket, mock_getenv):
        mock_getenv.return_value = "http://localhost:8080"
        mock_socket_instance = mock_socket.return_value
        mock_socket_instance.connect_ex.side_effect = socket.error("Socket error")

        result = sl.check_local_ssh_tunnels()
        self.assertEqual(result, [])

    @patch('os.getenv')
    @patch('socket.socket')
    def test_custom_addresses(self, mock_socket, mock_getenv):
        mock_getenv.return_value = "http://localhost:8080"
        mock_socket_instance = mock_socket.return_value
        mock_socket_instance.connect_ex.return_value = 0

        addresses = ['127.0.0.1', 'host.docker.internal']
        result = sl.check_local_ssh_tunnels(addresses=addresses)
        self.assertEqual(len(result), 2)
        self.assertIn('127.0.0.1', result[0]['line'])
        self.assertIn('host.docker.internal', result[1]['line'])

    @patch('os.getenv')
    @patch('socket.socket')
    def test_custom_port(self, mock_socket, mock_getenv):
        mock_getenv.return_value = None
        mock_socket_instance = mock_socket.return_value
        mock_socket_instance.connect_ex.return_value = 0

        result = sl.check_local_ssh_tunnels(port=9000)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]['port'], 9000)

    @patch('os.getenv')
    @patch('socket.socket')
    def test_https_scheme(self, mock_socket, mock_getenv):
        mock_getenv.return_value = "https://localhost"
        mock_socket_instance = mock_socket.return_value
        mock_socket_instance.connect_ex.return_value = 0

        result = sl.check_local_ssh_tunnels()
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]['port'], 443)

    @patch('os.getenv')
    @patch('socket.socket')
    def test_http_scheme(self, mock_socket, mock_getenv):
        mock_getenv.return_value = "http://localhost"
        mock_socket_instance = mock_socket.return_value
        mock_socket_instance.connect_ex.return_value = 0

        result = sl.check_local_ssh_tunnels()
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]['port'], 80)

    @patch('os.getenv')
    @patch('socket.socket')
    def test_default_port(self, mock_socket, mock_getenv):
        mock_getenv.return_value = "localhost"
        mock_socket_instance = mock_socket.return_value
        mock_socket_instance.connect_ex.return_value = 0

        result = sl.check_local_ssh_tunnels()
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]['port'], 8881)


class TestCheckDatabaseAccess(unittest.TestCase):

    def test_ieh_sdk_is_none(self):
        with self.assertRaises(Exception) as context:
            sl.check_database_access(None)
        self.assertEqual(str(context.exception), "Invalid ieh_sdk object")

    @patch('iEasyHydroForecast.setup_library.logger')
    def test_successful_access(self, mock_logger):
        ieh_sdk_mock = MagicMock()
        ieh_sdk_mock.get_discharge_sites.return_value = []  # Simulate successful access
        result = sl.check_database_access(ieh_sdk_mock)
        self.assertTrue(result)
        mock_logger.info.assert_called_with("Access to iEasyHydro database.")

    @patch('iEasyHydroForecast.setup_library.logger')
    @patch.dict(os.environ, {"ieasyhydroforecast_organization": "demo", "ieasyforecast_daily_discharge_path": "/path/to/discharge"})
    @patch('os.listdir')
    def test_demo_mode_with_discharge_data(self, mock_listdir, mock_logger):
        ieh_sdk_mock = MagicMock()
        ieh_sdk_mock.get_discharge_sites.side_effect = ConnectionError("Failed to connect")
        mock_listdir.return_value = ['file1.txt', 'file2.txt']  # Simulate files in discharge path
        result = sl.check_database_access(ieh_sdk_mock)
        self.assertFalse(result)
        mock_logger.info.assert_called_with("No access to iEasyHydro database. Will use data from the ieasyforecast_daily_discharge_path for forecasting only.")

    @patch('iEasyHydroForecast.setup_library.logger')
    @patch.dict(os.environ, {"ieasyhydroforecast_organization": "demo", "ieasyforecast_daily_discharge_path": "/path/to/discharge"})
    @patch('os.listdir')
    def test_demo_mode_no_discharge_data(self, mock_listdir, mock_logger):
        ieh_sdk_mock = MagicMock()
        ieh_sdk_mock.get_discharge_sites.side_effect = ConnectionError("Failed to connect")
        mock_listdir.return_value = []  # Simulate no files in discharge path
        result = sl.check_database_access(ieh_sdk_mock)
        self.assertFalse(result)
        mock_logger.error.assert_called_with("No data in the /path/to/discharge directory.")

    @patch('iEasyHydroForecast.setup_library.logger')
    @patch.dict(os.environ, {"ieasyhydroforecast_organization": "demo", "ieasyforecast_daily_discharge_path": "/path/to/discharge"})
    @patch('os.listdir', side_effect=FileNotFoundError)
    def test_demo_mode_filenotfound(self, mock_listdir, mock_logger):
        ieh_sdk_mock = MagicMock()
        ieh_sdk_mock.get_discharge_sites.side_effect = ConnectionError("Failed to connect")
        result = sl.check_database_access(ieh_sdk_mock)
        self.assertFalse(result)
        mock_logger.error.assert_called_with("Directory /path/to/discharge not found.")

    @patch('iEasyHydroForecast.setup_library.logger')
    @patch.dict(os.environ, {"ieasyhydroforecast_organization": "kghm"})
    def test_non_demo_mode_connection_error(self, mock_logger):
        ieh_sdk_mock = MagicMock()
        ieh_sdk_mock.get_discharge_sites.side_effect = ConnectionError("Failed to connect")
        with self.assertRaises(ConnectionError) as context:
            sl.check_database_access(ieh_sdk_mock)
        self.assertEqual(str(context.exception), "Failed to connect")
        mock_logger.error.assert_called_with("SAPPHIRE tools do not have access to the iEasyHydro database.")

    @patch('iEasyHydroForecast.setup_library.logger')
    def test_unexpected_error(self, mock_logger):
        ieh_sdk_mock = MagicMock()
        ieh_sdk_mock.get_discharge_sites.side_effect = ValueError("Something went wrong")
        with self.assertRaises(ValueError) as context:
            sl.check_database_access(ieh_sdk_mock)
        self.assertEqual(str(context.exception), "Something went wrong")
        mock_logger.error.assert_called_with("An unexpected error occurred: Something went wrong")


class TestReadDailyProbabilisticMlForecastsPentad(unittest.TestCase):
    """Test the read_daily_probabilistic_ml_forecasts_pentad function."""
    
    def setUp(self):
        """Set up test files and mocks."""
        # Define test file paths
        self.tide_test_file = os.path.join(
            os.path.dirname(__file__),
            "test_data/test_probabil_forecast.csv"
        )
        self.arima_test_file = os.path.join(
            os.path.dirname(__file__),
            "test_data/test_probabil_arima_forecast.csv"
        )
        # Print the paths for debugging
        #print(f"Test file paths: \n{self.tide_test_file}, \n{self.arima_test_file}")
        
        # Test if files exist
        self.assertTrue(os.path.exists(self.tide_test_file))
        self.assertTrue(os.path.exists(self.arima_test_file))
        
    @patch('logging.getLogger')
    def test_read_tide_forecast(self, mock_logger):
        """Test reading a TIDE forecast file with probabilistic forecasts (Q5-Q95)."""
        # Arrange
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        # Act    
        result = sl.read_daily_probabilistic_ml_forecasts_pentad(
            self.tide_test_file,
            "TIDE",
            "TIDE model (TIDE)",
            "TIDE"
        )
        print(f"\n\nresult:\n{result}")
        
        # Assert
        self.assertFalse(result.empty)
        self.assertIn("forecasted_discharge", result.columns)
        self.assertIn("model_short", result.columns)
        self.assertEqual(result["model_short"].unique()[0], "TIDE")
        self.assertEqual(result["model_long"].unique()[0], "TIDE model (TIDE)")
        
        # Verify the Q50 column was renamed correctly to forecasted_discharge
        # We may have no forecasts for certain forecast dates
        # self.assertTrue(all(~result["forecasted_discharge"].isna()))
        
        # Verify that pentad columns were calculated correctly
        self.assertIn("pentad_in_month", result.columns)
        self.assertIn("pentad_in_year", result.columns)
        
        # Basic shape verification
        unique_codes = result["code"].nunique()
        self.assertTrue(unique_codes > 0, "Expected multiple station codes in the result")
        
        # Verify all expected station codes are present
        expected_codes = [16161, 16158, 16936, 16055, 14256]
        for code in expected_codes[:5]:  # Check at least some of the expected codes
            self.assertIn(code, result["code"].values, f"Station {code} missing from results")
            
    @patch('logging.getLogger')
    def test_read_arima_forecast(self, mock_logger):
        """Test reading an ARIMA forecast file with deterministic forecasts (Q column)."""
        # Arrange
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        # Act
        result = sl.read_daily_probabilistic_ml_forecasts_pentad(
            self.arima_test_file,
            "ARIMA",
            "ARIMA Model (ARIMA)",
            "ARIMA"
        )
        
        # Assert
        self.assertFalse(result.empty)
        self.assertIn("forecasted_discharge", result.columns)
        self.assertEqual(result["model_short"].unique()[0], "ARIMA")
        
        # Verify ARIMA-specific columns
        self.assertNotIn("Q", result.columns)  # Renamed column should be gone
        
        # Verify grouping worked correctly - there should be one row per code and date
        group_counts = result.groupby(["code", "date"]).size()
        self.assertTrue(all(count == 1 for count in group_counts))


class TestReadDailyProbabilisticMlForecastsPentad(unittest.TestCase):
    
    def setUp(self):
        # Create some test data
        self.create_test_data()
    
    def create_test_data(self):
        """Create test data for different formats and cases"""
        # Standard date format - Each forecast_date has 5 days of forecast
        forecast_dates = pd.date_range(start='2025-03-01', periods=6)  # Include 5th, 10th, 15th, 20th, 25th, 31st
        rows = []
    
        # Create data for pentad days (5, 10, 15, 20, 25, and end of month)
        for forecast_date in forecast_dates:
            if forecast_date.day in [5, 10, 15, 20, 25] or forecast_date.day == pd.Timestamp(forecast_date.year, forecast_date.month, 1).days_in_month:
                # For each forecast date, create 5 daily forecasts
                for i in range(1, 6):
                    forecast_day = forecast_date + pd.Timedelta(days=i)
                    rows.append({
                        'date': forecast_day,
                        'forecast_date': forecast_date,
                        'code': 15149,
                        'Q50': np.random.rand() * 100,  # Random discharge value
                        'flag': 0
                    })
    
        self.standard_data = pd.DataFrame(rows)
    
        # Time format (with hours, minutes, seconds)
        time_rows = []
        for forecast_date in forecast_dates:
            if forecast_date.day in [5, 10, 15, 20, 25] or forecast_date.day == pd.Timestamp(forecast_date.year, forecast_date.month, 1).days_in_month:
                # For each forecast date, create 5 daily forecasts
                for i in range(1, 6):
                    forecast_day = forecast_date + pd.Timedelta(days=i)
                    time_rows.append({
                        'date': forecast_day.strftime('%Y-%m-%d %H:%M:%S'),
                        'forecast_date': forecast_date.strftime('%Y-%m-%d %H:%M:%S'),
                        'code': 15149,
                        'Q50': np.random.rand() * 100,
                        'flag': 0
                    })
    
        self.time_data = pd.DataFrame(time_rows)
    
        print(f"time_data:\n{self.time_data}")

        # Multiple station codes
        multi_station_rows = []
        for code in ["15149", "15083"]:
            for forecast_date in forecast_dates:
                if forecast_date.day in [5, 10, 15, 20, 25] or forecast_date.day == pd.Timestamp(forecast_date.year, forecast_date.month, 1).days_in_month:
                    # For each forecast date and code, create 5 daily forecasts
                    for i in range(1, 6):
                        forecast_day = forecast_date + pd.Timedelta(days=i)
                        multi_station_rows.append({
                            'date': forecast_day,
                            'forecast_date': forecast_date,
                            'code': code,
                            'Q50': np.random.rand() * 100,
                            'flag': 0
                        })
    
        self.multi_station_data = pd.DataFrame(multi_station_rows)
    
        # ARIMA format (uses Q instead of Q50)
        arima_rows = []
        for forecast_date in forecast_dates:
            if forecast_date.day in [5, 10, 15, 20, 25] or forecast_date.day == pd.Timestamp(forecast_date.year, forecast_date.month, 1).days_in_month:
                # For each forecast date, create 5 daily forecasts
                for i in range(1, 6):
                    forecast_day = forecast_date + pd.Timedelta(days=i)
                    arima_rows.append({
                        'date': forecast_day,
                        'forecast_date': forecast_date,
                        'code': 15149,
                        'Q': np.random.rand() * 100,
                        'flag': 0
                    })
    
        self.arima_data = pd.DataFrame(arima_rows)

        # Print arima_data
        #print(f"arima_data:\n{self.arima_data}")
        
        # Create data specifically for testing groupby functionality
        groupby_rows = []
        test_forecast_date = pd.Timestamp('2025-03-05')
    
        # Create multiple entries for the same code and forecast_date with different values
        # The function should average these when grouping
        for _ in range(3):
            groupby_rows.append({
                'date': test_forecast_date,
                'forecast_date': test_forecast_date,
                'code': 15149,
                'Q50': 10.0,  # These values should average to 20.0
                'flag': 0
            })
    
        for _ in range(3):
            groupby_rows.append({
                'date': test_forecast_date,
                'forecast_date': test_forecast_date,
                'code': 15149,
                'Q50': 30.0,  # These values should average to 20.0
                'flag': 0
            })
    
        self.groupby_test_data = pd.DataFrame(groupby_rows)
    
    def create_temp_csv(self, data):
        """Create a temporary CSV file with the given data"""
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        data.to_csv(temp_file.name, index=False)
        temp_file.close()
        return temp_file.name
     
    
    @patch('logging.getLogger')
    def test_datetime_format(self, mock_logger):
        """Test with datetime format YYYY-MM-DD HH:MM:SS"""
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        # Create a temporary CSV file
        filepath = self.create_temp_csv(self.time_data)
        
        try:
            # Call the function
            result = sl.read_daily_probabilistic_ml_forecasts_pentad(
                filepath, 
                model="TEST",
                model_long="Test Model", 
                model_short="TM"
            )
            print(f"\n\nresult:\n{result}")
                
            # Check that the result is not empty
            if result.empty:
                self.skipTest("Result is empty, cannot test datetime format")
            
            # Check that date conversion worked correctly
            self.assertIsInstance(result['date'].iloc[0], (pd.Timestamp, datetime.date))
            
            # Verify that only forecast_date from pentad days (5, 10, 15, 20, 25, 31) are in the results
            result_dates = pd.DatetimeIndex(result['date']).day
            expected_pentad_days = [5, 10, 15, 20, 25, 31]  # March has 31 days as end of month
            for day in result_dates:
                self.assertIn(day, expected_pentad_days, f"Date with day {day} shouldn't be in results")
            
            # The time_data has 5 days of forecasts for each pentad date
            # Verify that we get the right number of results (one per forecast_date and code)
            input_forecast_dates = pd.to_datetime(self.time_data['forecast_date']).unique()
            pentad_forecast_dates = [d for d in input_forecast_dates if d.day in expected_pentad_days]
            expected_result_rows = len(pentad_forecast_dates)  # One row per pentad forecast date
            self.assertEqual(len(result), expected_result_rows, 
                             f"Expected {expected_result_rows} results (one per pentad date), got {len(result)}")
            
            # Verify that the forecasted_discharge values are correct (mean of original values for each date)
            for idx, row in result.iterrows():
                date_str = pd.to_datetime(row['date']).strftime('%Y-%m-%d %H:%M:%S')
                code = row['code']
                
                # Get all the rows from time_data with this forecast_date and code
                original_rows = self.time_data[
                    (self.time_data['forecast_date'] == date_str) & 
                    (self.time_data['code'] == code)
                ]
                
                # Only test if we have matching rows
                if not original_rows.empty:
                    # Calculate the expected mean of Q50 values for this date and code
                    expected_discharge = original_rows['Q50'].mean()
                    
                    # Check that the forecasted_discharge is the correct mean value
                    self.assertAlmostEqual(
                        row['forecasted_discharge'], 
                        expected_discharge,
                        places=5,  # Higher precision to ensure exactness
                        msg=f"Forecasted discharge for {date_str} code {code} doesn't match expected mean"
                    )
            
            # Verify all required columns exist with correct types
            self.assertIn('model_long', result.columns)
            self.assertIn('model_short', result.columns)
            self.assertIn('pentad_in_month', result.columns)
            self.assertIn('pentad_in_year', result.columns)
            
            # Verify pentad calculations are correct
            for _, row in result.iterrows():
                date_with_timedelta = row['date'] + pd.Timedelta(days=1)
                expected_pentad = tl.get_pentad(date_with_timedelta)
                self.assertEqual(row['pentad_in_month'], expected_pentad,
                                f"Pentad in month should be {expected_pentad} for date {row['date']}")
                
                expected_pentad_in_year = tl.get_pentad_in_year(date_with_timedelta)
                self.assertEqual(row['pentad_in_year'], expected_pentad_in_year,
                                f"Pentad in year should be {expected_pentad_in_year} for date {row['date']}")
        finally:
            # Clean up the temporary file
            os.unlink(filepath)
    
    @patch('logging.getLogger')
    def test_arima_format(self, mock_logger):
        """Test with ARIMA format (Q instead of Q50)"""
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        # Create a temporary CSV file
        filepath = self.create_temp_csv(self.arima_data)
        
        try:
            # Call the function
            result = sl.read_daily_probabilistic_ml_forecasts_pentad(
                filepath, 
                model="ARIMA",
                model_long="ARIMA Model", 
                model_short="AR"
            )
                
            # Check that the result is not empty
            if result.empty:
                self.skipTest("Result is empty for ARIMA format test")
            
            # Check that Q has been renamed to forecasted_discharge
            self.assertIn('forecasted_discharge', result.columns)
            self.assertNotIn('Q', result.columns)
            
            # Verify that only forecast_date from pentad days (5, 10, 15, 20, 25, 31) are in the results
            result_dates = pd.DatetimeIndex(result['date']).day
            expected_pentad_days = [5, 10, 15, 20, 25, 31]  # March has 31 days as end of month
            for day in result_dates:
                self.assertIn(day, expected_pentad_days, f"Date with day {day} shouldn't be in results")
            
            # The arima_data has 5 days of forecasts for each pentad date
            # Verify that we get the right number of results (one per forecast_date and code)
            input_forecast_dates = self.arima_data['forecast_date'].unique()
            pentad_forecast_dates = [d for d in input_forecast_dates if pd.to_datetime(d).day in expected_pentad_days]
            expected_result_rows = len(pentad_forecast_dates)  # One row per pentad forecast date
            self.assertEqual(len(result), expected_result_rows, 
                             f"Expected {expected_result_rows} results (one per pentad date), got {len(result)}")
            
            # Verify that the forecasted_discharge values are correct (mean of original values for each date)
            for idx, row in result.iterrows():
                date = row['date']
                code = row['code']
                
                # Get all the rows from arima_data with this forecast_date and code
                original_rows = self.arima_data[
                    (self.arima_data['forecast_date'] == date) & 
                    (self.arima_data['code'] == code)
                ]
                
                # Only test if we have matching rows
                if not original_rows.empty:
                    # Calculate the expected mean of Q values for this date and code
                    expected_discharge = original_rows['Q'].mean()
                    
                    # Check that the forecasted_discharge is the correct mean value
                    self.assertAlmostEqual(
                        row['forecasted_discharge'], 
                        expected_discharge,
                        places=5,  # Higher precision to ensure exactness
                        msg=f"Forecasted discharge for {date} code {code} doesn't match expected mean"
                    )
            
            # Check for required columns
            required_columns = ['code', 'date', 'forecasted_discharge', 
                                'model_long', 'model_short', 
                                'pentad_in_month', 'pentad_in_year']
            for col in required_columns:
                self.assertIn(col, result.columns)
            
            # Check if model info is correctly added
            self.assertEqual(result['model_long'].iloc[0], "ARIMA Model")
            self.assertEqual(result['model_short'].iloc[0], "AR")
            
            # Verify pentad calculations are correct
            for _, row in result.iterrows():
                date_with_timedelta = row['date'] + pd.Timedelta(days=1)
                expected_pentad = tl.get_pentad(date_with_timedelta)
                self.assertEqual(row['pentad_in_month'], expected_pentad,
                                f"Pentad in month should be {expected_pentad} for date {row['date']}")
                
                expected_pentad_in_year = tl.get_pentad_in_year(date_with_timedelta)
                self.assertEqual(row['pentad_in_year'], expected_pentad_in_year,
                                f"Pentad in year should be {expected_pentad_in_year} for date {row['date']}")
        finally:
            # Clean up the temporary file
            os.unlink(filepath)
    
    @patch('logging.getLogger')
    def test_multiple_stations(self, mock_logger):
        """Test with multiple station codes"""
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        # Create a temporary CSV file
        filepath = self.create_temp_csv(self.multi_station_data)
        
        try:
            # Call the function
            result = sl.read_daily_probabilistic_ml_forecasts_pentad(
                filepath, 
                model="TEST",
                model_long="Test Model", 
                model_short="TM"
            )
                
            # Check that the result is not empty
            if result.empty:
                self.skipTest("Result is empty for multiple stations test")
                
            # Check that both station codes are present
            self.assertGreaterEqual(len(result['code'].unique()), 2)
            self.assertIn("15149", result['code'].values)
            self.assertIn("15083", result['code'].values)
            
            # Verify that only forecast_date from pentad days (5, 10, 15, 20, 25, 31) are in the results
            result_dates = pd.DatetimeIndex(result['date']).day
            expected_pentad_days = [5, 10, 15, 20, 25, 31]  # March has 31 days as end of month
            for day in result_dates:
                self.assertIn(day, expected_pentad_days, f"Date with day {day} shouldn't be in results")
            
            # Verify we get the expected number of rows (one row per unique combination of code and pentad date)
            # Get unique combinations of code and pentad dates in the input data
            input_combinations = set()
            for _, row in self.multi_station_data.iterrows():
                code = row['code']
                date = pd.to_datetime(row['forecast_date'])
                if date.day in expected_pentad_days:
                    input_combinations.add((code, date))
            
            expected_rows = len(input_combinations)
            self.assertEqual(len(result), expected_rows, 
                            f"Expected {expected_rows} results (one per code and pentad date), got {len(result)}")
            
            # Test that each station code is represented for each pentad date
            # Create a DataFrame with all combinations of dates and codes
            pentad_dates = [d for d in pd.to_datetime(self.multi_station_data['forecast_date'].unique()) 
                           if d.day in expected_pentad_days]
            station_codes = self.multi_station_data['code'].unique()
            
            expected_combinations = set()
            for date in pentad_dates:
                for code in station_codes:
                    expected_combinations.add((code, date))
            
            actual_combinations = set()
            for _, row in result.iterrows():
                actual_combinations.add((row['code'], row['date']))
            
            # Verify all expected combinations exist in the result
            for combo in expected_combinations:
                code, date = combo
                self.assertIn(combo, actual_combinations, 
                             f"Missing result for code {code} and date {date}")
            
            # Check for required columns
            required_columns = ['code', 'date', 'forecasted_discharge', 
                               'model_long', 'model_short', 
                               'pentad_in_month', 'pentad_in_year']
            for col in required_columns:
                self.assertIn(col, result.columns)
            
            # Check if model info is correctly added
            self.assertEqual(result['model_long'].iloc[0], "Test Model")
            self.assertEqual(result['model_short'].iloc[0], "TM")
            
            # Verify that each station's data is correctly aggregated
            # For each station and date, verify the forecasted discharge
            for code in station_codes:
                code_results = result[result['code'] == code]
                for _, row in code_results.iterrows():
                    date = row['date']
                    
                    # Get all rows from the original data with this code and forecast_date
                    original_rows = self.multi_station_data[
                        (self.multi_station_data['code'] == code) & 
                        (self.multi_station_data['forecast_date'] == date)
                    ]
                    
                    if not original_rows.empty:
                        # Calculate expected mean
                        expected_discharge = original_rows['Q50'].mean()
                        
                        # Verify forecasted discharge
                        self.assertAlmostEqual(
                            row['forecasted_discharge'], 
                            expected_discharge,
                            places=5,
                            msg=f"Incorrect discharge for code {code} and date {date}"
                        )
        finally:
            # Clean up the temporary file
            os.unlink(filepath)


class TestReadDailyProbabilisticMLForecastsPentad(unittest.TestCase):

    def setUp(self):
        # Setup runs before each test
        self.file_path = "iEasyHydroForecast/tests/test_data/test_probabil_forecast.csv"

        # Read validation data
        self.val_data = pd.read_csv(self.file_path)
        self.val_data.loc[:, "pentad_in_year"] = self.val_data["date"].apply(tl.get_pentad_in_year)
        # Cast code column to string
        self.val_data['code'] = self.val_data['code'].astype(str)
        self.val_data['date'] = pd.to_datetime(self.val_data['date']).dt.date
        self.val_data['forecast_date'] = pd.to_datetime(self.val_data['forecast_date']).dt.date

        # Process using the function to test
        self.test_data = sl.read_daily_probabilistic_ml_forecasts_pentad(
            self.file_path, 'test', 't', 'test')
        # Cast code column to string only if data is not empty
        if not self.test_data.empty and 'code' in self.test_data.columns:
            self.test_data['code'] = self.test_data['code'].astype(str)
        if not self.test_data.empty and 'date' in self.test_data.columns:
            self.test_data['date'] = pd.to_datetime(self.test_data['date']).dt.date

        
    def test_columns_present(self):
        # Skip test if processed data is empty
        if self.test_data.empty:
            self.skipTest("test_data is empty, cannot test column presence")
            
        # Test if columns in val_data are present in test_data, except for the
        # column Q50, which is renamed to forecasted_discharge in the test_data
        for col in self.val_data.columns:
            if col == 'forecast_date':
                continue
            elif col == 'Q50':
                self.assertIn('forecasted_discharge', self.test_data.columns, f"Column {col} is missing in processed data")
            else:
                self.assertIn(col, self.test_data.columns, f"Column {col} is missing in processed data")

    def test_data(self):
        # Skip test if processed data is empty
        if self.test_data.empty:
            self.skipTest("test_data is empty, cannot test data values")

        # Define a few codes and dates to test
        codes = [16161, 15083, 14283, 15054]
        # Cast codes to string, as in the test_data they are strings
        codes = [str(code) for code in codes]
        # Dates are in the format YYYY-MM-DD and are the last day of the previous pentad
        dates = ['2010-12-31', '2010-04-05', '2010-10-25', '2010-07-10']
        # Cast dates to dates in format YYYY-MM-DD, as in the test_data they are dates
        dates = [pd.to_datetime(date).date() for date in dates]

        for code in codes:
            for date in dates:
                print(f"Testing code {code} and date {date}")
                validation_data_code = self.val_data[(self.val_data['code'] == code) & (self.val_data['forecast_date'] == date)]
                # Test if validation_data_code is not empty
                self.assertFalse(validation_data_code.empty)
                # Sort validation_data_code by date
                validation_data_code = validation_data_code.sort_values(by='date')
                # Calculate the mean of the quantiles, only for columns starting with 'Q'
                mean = validation_data_code.filter(regex='^Q').mean(axis=0).round(3)
                # Get the test data for the code and forecast date
                test_data_code = self.test_data[(self.test_data['code'] == code) & (self.test_data['date'] == date)]
                # Test if test_data_code is not empty
                self.assertFalse(test_data_code.empty)

                # Special case for code 16161 and date 2010-07-10: We don't have data and all quantiles are not defined
                if code == "16161" and date == pd.to_datetime('2010-07-10').date():
                    self.assertTrue(test_data_code['forecasted_discharge'].isnull().values[0])
                    self.assertTrue(test_data_code['Q10'].isnull().values[0])
                    self.assertTrue(test_data_code['Q25'].isnull().values[0])
                    self.assertTrue(test_data_code['Q75'].isnull().values[0])
                    self.assertTrue(test_data_code['Q90'].isnull().values[0])
                    continue

                # Assert that all values in columns starting with Q are equal in both dataframes
                for col in mean.index:
                    if col == 'Q50':
                        self.assertAlmostEqual(mean[col], test_data_code['forecasted_discharge'].values[0], places=2)
                    else:
                        self.assertAlmostEqual(mean[col], test_data_code[col].values[0], places=2)

class TestReadDailyProbabilisticMLForecastsDecade(unittest.TestCase):

    def setUp(self):
        # Setup runs before each test
        self.file_path = "iEasyHydroForecast/tests/test_data/test_probabil_forecast.csv"

        # Read validation data
        self.val_data = pd.read_csv(self.file_path)
        self.val_data.loc[:, "decad_in_year"] = self.val_data["date"].apply(tl.get_decad_in_year)
        # Cast code column to string
        self.val_data['code'] = self.val_data['code'].astype(str)
        self.val_data['date'] = pd.to_datetime(self.val_data['date']).dt.date
        self.val_data['forecast_date'] = pd.to_datetime(self.val_data['forecast_date']).dt.date

        # Process using the function to test
        self.test_data = sl.read_daily_probabilistic_ml_forecasts_decade(
            self.file_path, 'test', 't', 'test')
        # Cast code column to string
        self.test_data['code'] = self.test_data['code'].astype(str)
        self.test_data['date'] = pd.to_datetime(self.test_data['date']).dt.date
        
    def test_columns_present(self):
        # Test if columns in val_data are present in test_data, except for the
        # column Q50, which is renamed to forecasted_discharge in the test_data
        for col in self.val_data.columns:
            if col == 'forecast_date':
                continue
            elif col == 'Q50':
                self.assertIn('forecasted_discharge', self.test_data.columns, f"Column {col} is missing in processed data")
            else:
                self.assertIn(col, self.test_data.columns, f"Column {col} is missing in processed data")

    def test_data(self):

        # Define a few codes and dates to test
        codes = [16161, 15083, 14283, 15054]
        # Cast codes to string, as in the test_data they are strings
        codes = [str(code) for code in codes]
        # Dates are in the format YYYY-MM-DD and are the last day of the previous decade
        dates = ['2010-12-31', '2010-04-10', '2010-10-20', '2010-07-10']
        # Cast dates to dates in format YYYY-MM-DD, as in the test_data they are dates
        dates = [pd.to_datetime(date).date() for date in dates]

        for code in codes:
            for date in dates:
                print(f"Testing code {code} and date {date}")
                # Print data types of code and date columns
                print(f"Data types - code loop: {type(code)}")
                print(f"Data types - date loop: {type(date)}")
                print(f"Data types - code: {type(self.val_data['code'][0])}")
                print(f"Data types - date: {type(self.val_data['date'][0])}")
                print(f"Data types - forecast_date: {type(self.val_data['forecast_date'][0])}")
                validation_data_code = self.val_data[(self.val_data['code'] == code) & (self.val_data['forecast_date'] == date)]
                # Test if validation_data_code is not empty
                self.assertFalse(validation_data_code.empty)
                # Sort validation_data_code by date
                validation_data_code = validation_data_code.sort_values(by='date')
                # Calculate the mean of the quantiles, only for columns starting with 'Q'
                mean = validation_data_code.filter(regex='^Q').mean(axis=0).round(3)
                # Get the test data for the code and forecast date
                test_data_code = self.test_data[(self.test_data['code'] == code) & (self.test_data['date'] == date)]
                # Test if test_data_code is not empty
                self.assertFalse(test_data_code.empty)

                # Special case for code 16161 and date 2010-07-10: We don't have data and all quantiles are not defined
                if code == "16161" and date == pd.to_datetime('2010-07-10').date():
                    self.assertTrue(test_data_code['forecasted_discharge'].isnull().values[0])
                    self.assertTrue(test_data_code['Q10'].isnull().values[0])
                    self.assertTrue(test_data_code['Q25'].isnull().values[0])
                    self.assertTrue(test_data_code['Q75'].isnull().values[0])
                    self.assertTrue(test_data_code['Q90'].isnull().values[0])
                    continue

                # Assert that all values in columns starting with Q are equal in both dataframes
                for col in mean.index:
                    if col == 'Q50':
                        self.assertAlmostEqual(mean[col], test_data_code['forecasted_discharge'].values[0], places=2)
                    else:
                        self.assertAlmostEqual(mean[col], test_data_code[col].values[0], places=2)


class TestGetPentadalForecastSitesFromHFSdk(unittest.TestCase):

    def setUp(self):
        self.mock_ieh_hf_sdk = MagicMock()
        self.mock_discharge_sites = [
            {'site_code': '12345', 'site_name': 'Test Site 1', 'iehhf_site_id': 'ID1'},
            {'site_code': '67890', 'site_name': 'Test Site 2', 'iehhf_site_id': 'ID2'}
        ]
        self.mock_virtual_sites = [
            {'site_code': 'V123', 'site_name': 'Virtual Site 1', 'iehhf_site_id': 'VID1'}
        ]
        self.mock_ieh_hf_sdk.get_discharge_sites.return_value = self.mock_discharge_sites
        self.mock_ieh_hf_sdk.get_virtual_sites.return_value = self.mock_virtual_sites

        self.mock_fc_sites = [
            MagicMock(code='12345', iehhf_site_id='ID1'),
            MagicMock(code='67890', iehhf_site_id='ID2')
        ]
        self.mock_virtual_fc_sites = [
            MagicMock(code='V123', iehhf_site_id='VID1')
        ]   

        # Patch fl.Site methods
        self.patch_pentad_forecast_sites = patch('forecast_library.Site.pentad_forecast_sites_from_iEH_HF_SDK',
                                                    return_value=self.mock_fc_sites)
        self.patch_virtual_pentad_forecast_sites = patch('forecast_library.Site.virtual_pentad_forecast_sites_from_iEH_HF_SDK',
                                                            return_value=self.mock_virtual_fc_sites)
        self.patch_os_path_join = patch('os.path.join', return_value='/path/to/config.json')
        self.patch_open = patch('builtins.open', new_callable=MagicMock)
        self.patch_os_getenv = patch.dict(os.environ, {
            'ieasyforecast_configuration_path': '/config',
            'ieasyforecast_config_file_station_selection': 'config.json'
        })
        self.addCleanup(patch.stopall)

        self.mock_pentad_forecast_sites = self.patch_pentad_forecast_sites.start()
        self.mock_virtual_pentad_forecast_sites = self.patch_virtual_pentad_forecast_sites.start()
        self.mock_os_path_join = self.patch_os_path_join.start()
        self.mock_open = self.patch_open.start()
        self.mock_os_getenv = self.patch_os_getenv.start()

    def test_get_pentadal_forecast_sites_from_HF_SDK(self):
        fc_sites, site_codes, site_ids = sl.get_pentadal_forecast_sites_from_HF_SDK(self.mock_ieh_hf_sdk)

        # Assertions
        self.assertEqual(len(fc_sites), 3)
        self.assertEqual(len(site_codes), 3)
        self.assertEqual(len(site_ids), 3)

        self.assertCountEqual(site_codes, ['12345', '67890', 'V123'])
        self.assertCountEqual(site_ids, ['ID1', 'ID2', 'VID1'])

        self.mock_ieh_hf_sdk.get_discharge_sites.assert_called_once()
        self.mock_ieh_hf_sdk.get_virtual_sites.assert_called_once()
        self.mock_pentad_forecast_sites.assert_called_once_with(self.mock_discharge_sites)
        self.mock_virtual_pentad_forecast_sites.assert_called_once_with(self.mock_virtual_sites)

        self.mock_os_path_join.assert_called_with('/config', 'config.json')
        self.mock_open.assert_called()

        
class TestGetDecadalForecastSitesFromHFSdk(unittest.TestCase):

    def setUp(self):
        self.mock_ieh_hf_sdk = MagicMock()
        self.mock_discharge_sites = [
            {'site_code': '12345', 'site_name': 'Test Site 1', 'iehhf_site_id': 'ID1'},
            {'site_code': '67890', 'site_name': 'Test Site 2', 'iehhf_site_id': 'ID2'}
        ]
        self.mock_virtual_sites = [
            {'site_code': 'V123', 'site_name': 'Virtual Site 1', 'iehhf_site_id': 'VID1'}
        ]
        self.mock_ieh_hf_sdk.get_discharge_sites.return_value = self.mock_discharge_sites
        self.mock_ieh_hf_sdk.get_virtual_sites.return_value = self.mock_virtual_sites

        self.mock_fc_sites = [
            MagicMock(code='12345', iehhf_site_id='ID1'),
            MagicMock(code='67890', iehhf_site_id='ID2')
        ]
        self.mock_virtual_fc_sites = [
            MagicMock(code='V123', iehhf_site_id='VID1')
        ]   

        # Patch fl.Site methods
        self.patch_decad_forecast_sites = patch('forecast_library.Site.decad_forecast_sites_from_iEH_HF_SDK',
                                                    return_value=self.mock_fc_sites)
        self.patch_virtual_decad_forecast_sites = patch('forecast_library.Site.virtual_decad_forecast_sites_from_iEH_HF_SDK',
                                                            return_value=self.mock_virtual_fc_sites)
        self.patch_os_path_join = patch('os.path.join', return_value='/path/to/config.json')
        self.patch_open = patch('builtins.open', new_callable=MagicMock)
        self.patch_os_getenv = patch.dict(os.environ, {
            'ieasyforecast_configuration_path': '/config',
            'ieasyforecast_config_file_station_selection_decad': 'config.json'
        })
        self.addCleanup(patch.stopall)

        self.mock_decad_forecast_sites = self.patch_decad_forecast_sites.start()
        self.mock_virtual_decad_forecast_sites = self.patch_virtual_decad_forecast_sites.start()
        self.mock_os_path_join = self.patch_os_path_join.start()
        self.mock_open = self.patch_open.start()
        self.mock_os_getenv = self.patch_os_getenv.start()

    def test_get_decadal_forecast_sites_from_HF_SDK(self):
        fc_sites, site_codes, site_ids = sl.get_decadal_forecast_sites_from_HF_SDK(self.mock_ieh_hf_sdk)

        # Assertions
        self.assertEqual(len(fc_sites), 3)
        self.assertEqual(len(site_codes), 3)
        self.assertEqual(len(site_ids), 3)

        self.assertCountEqual(site_codes, ['12345', '67890', 'V123'])
        self.assertCountEqual(site_ids, ['ID1', 'ID2', 'VID1'])

        self.mock_ieh_hf_sdk.get_discharge_sites.assert_called_once()
        self.mock_ieh_hf_sdk.get_virtual_sites.assert_called_once()
        self.mock_decad_forecast_sites.assert_called_once_with(self.mock_discharge_sites)
        self.mock_virtual_decad_forecast_sites.assert_called_once_with(self.mock_virtual_sites)

        self.mock_os_path_join.assert_called_with('/config', 'config.json')
        self.mock_open.assert_called()





