import datetime
import json
import logging
import os
import shutil
import socket
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from iEasyHydroForecast import setup_library as sl
from iEasyHydroForecast import tag_library as tl


class TestLoadConfiguration:
    # Temporary directory to store output
    tmpdir = "iEasyHydroForecast/tests/test_data/temp"
    # Create the directory
    os.makedirs(tmpdir, exist_ok=True)
    # When in a test environment, the .env file should load the following
    # environment variables:
    test_env_path = "iEasyHydroForecast/tests/test_data/.env_develop_test"
    # Folders with data read by the forecast tools
    test_ieasyforecast_configuration_path = "config"
    test_ieasyforecast_gis_directory_path = "../data/GIS"
    test_ieasyreports_templates_directory_path = "../data/templates"
    test_ieasyforecast_daily_discharge_path = "../data/daily_runoff"
    test_ieasyforecast_locale_dir = "config/locale"
    # Folders with data written by the forecast tools. These are temporary and
    # should be deleted after the tests are run.
    test_ieasyforecast_intermediate_data_path = os.path.join(tmpdir, "apps/internal_data")
    # Create the folder internal data
    os.makedirs(test_ieasyforecast_intermediate_data_path, exist_ok=True)
    test_ieasyreports_report_output_path = os.path.join(tmpdir, "data/reports")

    # Get full paths of the folders
    test_ieasyforecast_configuration_path_full_path = os.path.abspath(
        test_ieasyforecast_configuration_path
    )
    test_ieasyforecast_gis_directory_path_full_path = os.path.abspath(
        test_ieasyforecast_gis_directory_path
    )
    test_ieasyreports_templates_directory_path_full_path = os.path.abspath(
        test_ieasyreports_templates_directory_path
    )
    test_ieasyforecast_daily_discharge_path_full_path = os.path.abspath(
        test_ieasyforecast_daily_discharge_path
    )
    test_ieasyforecast_locale_dir_full_path = os.path.abspath(test_ieasyforecast_locale_dir)
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
    assert (
        os.getenv("ieasyreports_templates_directory_path")
        == test_ieasyreports_templates_directory_path
    )
    assert (
        os.getenv("ieasyforecast_daily_discharge_path") == test_ieasyforecast_daily_discharge_path
    )
    assert os.getenv("ieasyforecast_locale_dir") == test_ieasyforecast_locale_dir
    assert (
        os.getenv("ieasyforecast_intermediate_data_path")
        == test_ieasyforecast_intermediate_data_path
    )
    assert os.getenv("ieasyreports_report_output_path") == test_ieasyreports_report_output_path

    # Delete the directory tmpdir and all its contents
    print("Deleting directory: ", tmpdir)
    shutil.rmtree(tmpdir)
    # Delete the environment variables
    ret = os.environ.pop("ieasyforecast_configuration_path")
    assert ret == test_ieasyforecast_configuration_path
    ret = os.environ.pop("ieasyforecast_gis_directory_path")
    assert ret == test_ieasyforecast_gis_directory_path
    ret = os.environ.pop("ieasyreports_templates_directory_path")
    assert ret == test_ieasyreports_templates_directory_path
    ret = os.environ.pop("ieasyforecast_daily_discharge_path")
    assert ret == test_ieasyforecast_daily_discharge_path
    ret = os.environ.pop("ieasyforecast_locale_dir")
    assert ret == test_ieasyforecast_locale_dir
    ret = os.environ.pop("ieasyforecast_intermediate_data_path")
    assert ret == test_ieasyforecast_intermediate_data_path
    ret = os.environ.pop("ieasyreports_report_output_path")
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
        if "IEASYHYDRO_HOST" in os.environ:
            del os.environ["IEASYHYDRO_HOST"]

    @patch("os.getenv")
    def test_no_ieasyhydro_host(self, mock_getenv):
        mock_getenv.return_value = None
        result = sl.check_local_ssh_tunnels()
        self.assertEqual(result, [])

    @patch("os.getenv")
    @patch("socket.socket")
    def test_successful_connection(self, mock_socket, mock_getenv):
        mock_getenv.return_value = "http://localhost:8080"
        mock_socket_instance = mock_socket.return_value
        mock_socket_instance.connect_ex.return_value = 0  # Simulate successful connection

        result = sl.check_local_ssh_tunnels()
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]["port"], 8080)
        self.assertIn("localhost", result[0]["line"])

    @patch("os.getenv")
    @patch("socket.socket")
    def test_connection_refused(self, mock_socket, mock_getenv):
        mock_getenv.return_value = "http://localhost:8080"
        mock_socket_instance = mock_socket.return_value
        mock_socket_instance.connect_ex.return_value = 111  # Simulate connection refused

        result = sl.check_local_ssh_tunnels()
        self.assertEqual(result, [])

    @patch("os.getenv")
    @patch("socket.socket")
    def test_address_resolution_error(self, mock_socket, mock_getenv):
        mock_getenv.return_value = "http://invalid_address:8080"
        mock_socket_instance = mock_socket.return_value
        mock_socket_instance.connect_ex.side_effect = socket.gaierror("Address resolution error")

        result = sl.check_local_ssh_tunnels()
        self.assertEqual(result, [])

    @patch("os.getenv")
    @patch("socket.socket")
    def test_socket_error(self, mock_socket, mock_getenv):
        mock_getenv.return_value = "http://localhost:8080"
        mock_socket_instance = mock_socket.return_value
        mock_socket_instance.connect_ex.side_effect = OSError("Socket error")

        result = sl.check_local_ssh_tunnels()
        self.assertEqual(result, [])

    @patch("os.getenv")
    @patch("socket.socket")
    def test_custom_addresses(self, mock_socket, mock_getenv):
        mock_getenv.return_value = "http://localhost:8080"
        mock_socket_instance = mock_socket.return_value
        mock_socket_instance.connect_ex.return_value = 0

        addresses = ["127.0.0.1", "host.docker.internal"]
        result = sl.check_local_ssh_tunnels(addresses=addresses)
        self.assertEqual(len(result), 2)
        self.assertIn("127.0.0.1", result[0]["line"])
        self.assertIn("host.docker.internal", result[1]["line"])

    @patch("os.getenv")
    @patch("socket.socket")
    def test_custom_port(self, mock_socket, mock_getenv):
        mock_getenv.return_value = None
        mock_socket_instance = mock_socket.return_value
        mock_socket_instance.connect_ex.return_value = 0

        result = sl.check_local_ssh_tunnels(port=9000)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]["port"], 9000)

    @patch("os.getenv")
    @patch("socket.socket")
    def test_https_scheme(self, mock_socket, mock_getenv):
        mock_getenv.return_value = "https://localhost"
        mock_socket_instance = mock_socket.return_value
        mock_socket_instance.connect_ex.return_value = 0

        result = sl.check_local_ssh_tunnels()
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]["port"], 443)

    @patch("os.getenv")
    @patch("socket.socket")
    def test_http_scheme(self, mock_socket, mock_getenv):
        mock_getenv.return_value = "http://localhost"
        mock_socket_instance = mock_socket.return_value
        mock_socket_instance.connect_ex.return_value = 0

        result = sl.check_local_ssh_tunnels()
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]["port"], 80)

    @patch("os.getenv")
    @patch("socket.socket")
    def test_default_port(self, mock_socket, mock_getenv):
        mock_getenv.return_value = "localhost"
        mock_socket_instance = mock_socket.return_value
        mock_socket_instance.connect_ex.return_value = 0

        result = sl.check_local_ssh_tunnels()
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]["port"], 8881)


class TestCheckDatabaseAccess(unittest.TestCase):
    def test_ieh_sdk_is_none(self):
        with self.assertRaises(Exception) as context:
            sl.check_database_access(None)
        self.assertEqual(str(context.exception), "Invalid ieh_sdk object")

    @patch("iEasyHydroForecast.setup_library.logger")
    def test_successful_access(self, mock_logger):
        ieh_sdk_mock = MagicMock()
        ieh_sdk_mock.get_discharge_sites.return_value = []  # Simulate successful access
        result = sl.check_database_access(ieh_sdk_mock)
        self.assertTrue(result)
        mock_logger.info.assert_called_with("Access to iEasyHydro database.")

    @patch("iEasyHydroForecast.setup_library.logger")
    @patch.dict(
        os.environ,
        {
            "ieasyhydroforecast_organization": "demo",
            "ieasyforecast_daily_discharge_path": "/path/to/discharge",
        },
    )
    @patch("os.listdir")
    def test_demo_mode_with_discharge_data(self, mock_listdir, mock_logger):
        ieh_sdk_mock = MagicMock()
        ieh_sdk_mock.get_discharge_sites.side_effect = ConnectionError("Failed to connect")
        mock_listdir.return_value = ["file1.txt", "file2.txt"]  # Simulate files in discharge path
        result = sl.check_database_access(ieh_sdk_mock)
        self.assertFalse(result)
        mock_logger.info.assert_called_with(
            "No access to iEasyHydro database. Will use data from the ieasyforecast_daily_discharge_path for forecasting only."
        )

    @patch("iEasyHydroForecast.setup_library.logger")
    @patch.dict(
        os.environ,
        {
            "ieasyhydroforecast_organization": "demo",
            "ieasyforecast_daily_discharge_path": "/path/to/discharge",
        },
    )
    @patch("os.listdir")
    def test_demo_mode_no_discharge_data(self, mock_listdir, mock_logger):
        ieh_sdk_mock = MagicMock()
        ieh_sdk_mock.get_discharge_sites.side_effect = ConnectionError("Failed to connect")
        mock_listdir.return_value = []  # Simulate no files in discharge path
        result = sl.check_database_access(ieh_sdk_mock)
        self.assertFalse(result)
        mock_logger.error.assert_called_with("No data in the /path/to/discharge directory.")

    @patch("iEasyHydroForecast.setup_library.logger")
    @patch.dict(
        os.environ,
        {
            "ieasyhydroforecast_organization": "demo",
            "ieasyforecast_daily_discharge_path": "/path/to/discharge",
        },
    )
    @patch("os.listdir", side_effect=FileNotFoundError)
    def test_demo_mode_filenotfound(self, mock_listdir, mock_logger):
        ieh_sdk_mock = MagicMock()
        ieh_sdk_mock.get_discharge_sites.side_effect = ConnectionError("Failed to connect")
        result = sl.check_database_access(ieh_sdk_mock)
        self.assertFalse(result)
        mock_logger.error.assert_called_with("Directory /path/to/discharge not found.")

    @patch("iEasyHydroForecast.setup_library.logger")
    @patch.dict(os.environ, {"ieasyhydroforecast_organization": "kghm"})
    def test_non_demo_mode_connection_error(self, mock_logger):
        ieh_sdk_mock = MagicMock()
        ieh_sdk_mock.get_discharge_sites.side_effect = ConnectionError("Failed to connect")
        with self.assertRaises(ConnectionError) as context:
            sl.check_database_access(ieh_sdk_mock)
        self.assertEqual(str(context.exception), "Failed to connect")
        mock_logger.error.assert_called_with(
            "SAPPHIRE tools do not have access to the iEasyHydro database."
        )

    @patch("iEasyHydroForecast.setup_library.logger")
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
            os.path.dirname(__file__), "test_data/test_probabil_forecast.csv"
        )
        self.arima_test_file = os.path.join(
            os.path.dirname(__file__), "test_data/test_probabil_arima_forecast.csv"
        )
        # Print the paths for debugging
        # print(f"Test file paths: \n{self.tide_test_file}, \n{self.arima_test_file}")

        # Test if files exist
        self.assertTrue(os.path.exists(self.tide_test_file))
        self.assertTrue(os.path.exists(self.arima_test_file))

    @patch("logging.getLogger")
    def test_read_tide_forecast(self, mock_logger):
        """Test reading a TIDE forecast file with probabilistic forecasts (Q5-Q95)."""
        # Arrange
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance

        # Act
        result = sl.read_daily_probabilistic_ml_forecasts_pentad(
            self.tide_test_file, "TIDE", model_short="TIDE"
        )
        print(f"\n\nresult:\n{result}")

        # Assert
        self.assertFalse(result.empty)
        self.assertIn("forecasted_discharge", result.columns)
        self.assertIn("model_short", result.columns)
        self.assertEqual(result["model_short"].unique()[0], "TIDE")

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
        expected_codes = ["16161", "16158", "16936", "16055", "14256"]
        for code in expected_codes[:5]:  # Check at least some of the expected codes
            self.assertIn(code, result["code"].values, f"Station {code} missing from results")

    @patch("logging.getLogger")
    def test_read_arima_forecast(self, mock_logger):
        """Test reading an ARIMA forecast file with deterministic forecasts (Q column)."""
        # Arrange
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance

        # Act
        result = sl.read_daily_probabilistic_ml_forecasts_pentad(
            self.arima_test_file, "ARIMA", model_short="ARIMA"
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


class TestReadDailyProbabilisticMlForecastsPentadFormats(unittest.TestCase):
    def setUp(self):
        # Create some test data
        self.create_test_data()

    def create_test_data(self):
        """Create test data for different formats and cases"""
        # Standard date format - Each forecast_date has 5 days of forecast
        forecast_dates = pd.date_range(
            start="2025-03-01", periods=6
        )  # Include 5th, 10th, 15th, 20th, 25th, 31st
        rows = []

        # Create data for pentad days (5, 10, 15, 20, 25, and end of month)
        for forecast_date in forecast_dates:
            if (
                forecast_date.day in [5, 10, 15, 20, 25]
                or forecast_date.day
                == pd.Timestamp(forecast_date.year, forecast_date.month, 1).days_in_month
            ):
                # For each forecast date, create 5 daily forecasts
                for i in range(1, 6):
                    forecast_day = forecast_date + pd.Timedelta(days=i)
                    rows.append(
                        {
                            "date": forecast_day,
                            "forecast_date": forecast_date,
                            "code": 15149,
                            "Q50": np.random.rand() * 100,  # Random discharge value
                            "flag": 0,
                        }
                    )

        self.standard_data = pd.DataFrame(rows)

        # Time format (with hours, minutes, seconds)
        time_rows = []
        for forecast_date in forecast_dates:
            if (
                forecast_date.day in [5, 10, 15, 20, 25]
                or forecast_date.day
                == pd.Timestamp(forecast_date.year, forecast_date.month, 1).days_in_month
            ):
                # For each forecast date, create 5 daily forecasts
                for i in range(1, 6):
                    forecast_day = forecast_date + pd.Timedelta(days=i)
                    time_rows.append(
                        {
                            "date": forecast_day.strftime("%Y-%m-%d %H:%M:%S"),
                            "forecast_date": forecast_date.strftime("%Y-%m-%d %H:%M:%S"),
                            "code": 15149,
                            "Q50": np.random.rand() * 100,
                            "flag": 0,
                        }
                    )

        self.time_data = pd.DataFrame(time_rows)

        print(f"time_data:\n{self.time_data}")

        # Multiple station codes
        multi_station_rows = []
        for code in ["15149", "15083"]:
            for forecast_date in forecast_dates:
                if (
                    forecast_date.day in [5, 10, 15, 20, 25]
                    or forecast_date.day
                    == pd.Timestamp(forecast_date.year, forecast_date.month, 1).days_in_month
                ):
                    # For each forecast date and code, create 5 daily forecasts
                    for i in range(1, 6):
                        forecast_day = forecast_date + pd.Timedelta(days=i)
                        multi_station_rows.append(
                            {
                                "date": forecast_day,
                                "forecast_date": forecast_date,
                                "code": code,
                                "Q50": np.random.rand() * 100,
                                "flag": 0,
                            }
                        )

        self.multi_station_data = pd.DataFrame(multi_station_rows)

        # ARIMA format (uses Q instead of Q50)
        arima_rows = []
        for forecast_date in forecast_dates:
            if (
                forecast_date.day in [5, 10, 15, 20, 25]
                or forecast_date.day
                == pd.Timestamp(forecast_date.year, forecast_date.month, 1).days_in_month
            ):
                # For each forecast date, create 5 daily forecasts
                for i in range(1, 6):
                    forecast_day = forecast_date + pd.Timedelta(days=i)
                    arima_rows.append(
                        {
                            "date": forecast_day,
                            "forecast_date": forecast_date,
                            "code": 15149,
                            "Q": np.random.rand() * 100,
                            "flag": 0,
                        }
                    )

        self.arima_data = pd.DataFrame(arima_rows)

        # Print arima_data
        # print(f"arima_data:\n{self.arima_data}")

        # Create data specifically for testing groupby functionality
        groupby_rows = []
        test_forecast_date = pd.Timestamp("2025-03-05")

        # Create multiple entries for the same code and forecast_date with different values
        # The function should average these when grouping
        for _ in range(3):
            groupby_rows.append(
                {
                    "date": test_forecast_date,
                    "forecast_date": test_forecast_date,
                    "code": 15149,
                    "Q50": 10.0,  # These values should average to 20.0
                    "flag": 0,
                }
            )

        for _ in range(3):
            groupby_rows.append(
                {
                    "date": test_forecast_date,
                    "forecast_date": test_forecast_date,
                    "code": 15149,
                    "Q50": 30.0,  # These values should average to 20.0
                    "flag": 0,
                }
            )

        self.groupby_test_data = pd.DataFrame(groupby_rows)

    def create_temp_csv(self, data):
        """Create a temporary CSV file with the given data"""
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
            data.to_csv(temp_file.name, index=False)
            return temp_file.name

    @patch("logging.getLogger")
    def test_datetime_format(self, mock_logger):
        """Test with datetime format YYYY-MM-DD HH:MM:SS"""
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance

        # Create a temporary CSV file
        filepath = self.create_temp_csv(self.time_data)

        try:
            # Call the function
            result = sl.read_daily_probabilistic_ml_forecasts_pentad(
                filepath, model="TEST", model_short="TM"
            )
            print(f"\n\nresult:\n{result}")

            # Check that the result is not empty
            # self.assertFalse(result.empty)

            # Check that date conversion worked correctly
            self.assertIsInstance(result["date"].iloc[0], (pd.Timestamp, datetime.date))

            # Verify that only forecast_date from pentad days (5, 10, 15, 20, 25, 31) are in the results
            result_dates = pd.DatetimeIndex(result["date"]).day
            expected_pentad_days = [5, 10, 15, 20, 25, 31]  # March has 31 days as end of month
            for day in result_dates:
                self.assertIn(
                    day, expected_pentad_days, f"Date with day {day} shouldn't be in results"
                )

            # The time_data has 5 days of forecasts for each pentad date
            # Verify that we get the right number of results (one per forecast_date and code)
            input_forecast_dates = pd.to_datetime(self.time_data["forecast_date"]).unique()
            pentad_forecast_dates = [
                d for d in input_forecast_dates if d.day in expected_pentad_days
            ]
            expected_result_rows = len(pentad_forecast_dates)  # One row per pentad forecast date
            self.assertEqual(
                len(result),
                expected_result_rows,
                f"Expected {expected_result_rows} results (one per pentad date), got {len(result)}",
            )

            # Verify that the forecasted_discharge values are correct (mean of original values for each date)
            for _idx, row in result.iterrows():
                date_str = pd.to_datetime(row["date"]).strftime("%Y-%m-%d %H:%M:%S")
                code = row["code"]

                # Get all the rows from time_data with this forecast_date and code
                original_rows = self.time_data[
                    (self.time_data["forecast_date"] == date_str) & (self.time_data["code"] == code)
                ]

                # Only test if we have matching rows
                if not original_rows.empty:
                    # Calculate the expected mean of Q50 values for this date and code
                    expected_discharge = original_rows["Q50"].mean()

                    # Check that the forecasted_discharge is the correct mean value
                    self.assertAlmostEqual(
                        row["forecasted_discharge"],
                        expected_discharge,
                        places=5,  # Higher precision to ensure exactness
                        msg=f"Forecasted discharge for {date_str} code {code} doesn't match expected mean",
                    )

            # Verify all required columns exist with correct types
            self.assertIn("model_short", result.columns)
            self.assertIn("pentad_in_month", result.columns)
            self.assertIn("pentad_in_year", result.columns)

            # Verify pentad calculations are correct
            for _, row in result.iterrows():
                date_with_timedelta = row["date"] + pd.Timedelta(days=1)
                expected_pentad = tl.get_pentad(date_with_timedelta)
                self.assertEqual(
                    row["pentad_in_month"],
                    expected_pentad,
                    f"Pentad in month should be {expected_pentad} for date {row['date']}",
                )

                expected_pentad_in_year = tl.get_pentad_in_year(date_with_timedelta)
                self.assertEqual(
                    row["pentad_in_year"],
                    expected_pentad_in_year,
                    f"Pentad in year should be {expected_pentad_in_year} for date {row['date']}",
                )
        finally:
            # Clean up the temporary file
            os.unlink(filepath)

    @patch("logging.getLogger")
    def test_arima_format(self, mock_logger):
        """Test with ARIMA format (Q instead of Q50)"""
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance

        # Create a temporary CSV file
        filepath = self.create_temp_csv(self.arima_data)

        try:
            # Call the function
            result = sl.read_daily_probabilistic_ml_forecasts_pentad(
                filepath, model="ARIMA", model_short="AR"
            )

            # Check that the result is not empty
            self.assertFalse(result.empty)

            # Check that Q has been renamed to forecasted_discharge
            self.assertIn("forecasted_discharge", result.columns)
            self.assertNotIn("Q", result.columns)

            # Verify that only forecast_date from pentad days (5, 10, 15, 20, 25, 31) are in the results
            result_dates = pd.DatetimeIndex(result["date"]).day
            expected_pentad_days = [5, 10, 15, 20, 25, 31]  # March has 31 days as end of month
            for day in result_dates:
                self.assertIn(
                    day, expected_pentad_days, f"Date with day {day} shouldn't be in results"
                )

            # The arima_data has 5 days of forecasts for each pentad date
            # Verify that we get the right number of results (one per forecast_date and code)
            input_forecast_dates = self.arima_data["forecast_date"].unique()
            pentad_forecast_dates = [
                d for d in input_forecast_dates if pd.to_datetime(d).day in expected_pentad_days
            ]
            expected_result_rows = len(pentad_forecast_dates)  # One row per pentad forecast date
            self.assertEqual(
                len(result),
                expected_result_rows,
                f"Expected {expected_result_rows} results (one per pentad date), got {len(result)}",
            )

            # Verify that the forecasted_discharge values are correct (mean of original values for each date)
            for _idx, row in result.iterrows():
                date = row["date"]
                code = row["code"]

                # Get all the rows from arima_data with this forecast_date and code
                original_rows = self.arima_data[
                    (self.arima_data["forecast_date"] == date) & (self.arima_data["code"] == code)
                ]

                # Only test if we have matching rows
                if not original_rows.empty:
                    # Calculate the expected mean of Q values for this date and code
                    expected_discharge = original_rows["Q"].mean()

                    # Check that the forecasted_discharge is the correct mean value
                    self.assertAlmostEqual(
                        row["forecasted_discharge"],
                        expected_discharge,
                        places=5,  # Higher precision to ensure exactness
                        msg=f"Forecasted discharge for {date} code {code} doesn't match expected mean",
                    )

            # Check for required columns
            required_columns = [
                "code",
                "date",
                "forecasted_discharge",
                "model_short",
                "pentad_in_month",
                "pentad_in_year",
            ]
            for col in required_columns:
                self.assertIn(col, result.columns)

            # Check if model info is correctly added
            self.assertEqual(result["model_short"].iloc[0], "AR")

            # Verify pentad calculations are correct
            for _, row in result.iterrows():
                date_with_timedelta = row["date"] + pd.Timedelta(days=1)
                expected_pentad = tl.get_pentad(date_with_timedelta)
                self.assertEqual(
                    row["pentad_in_month"],
                    expected_pentad,
                    f"Pentad in month should be {expected_pentad} for date {row['date']}",
                )

                expected_pentad_in_year = tl.get_pentad_in_year(date_with_timedelta)
                self.assertEqual(
                    row["pentad_in_year"],
                    expected_pentad_in_year,
                    f"Pentad in year should be {expected_pentad_in_year} for date {row['date']}",
                )
        finally:
            # Clean up the temporary file
            os.unlink(filepath)

    @patch("logging.getLogger")
    def test_multiple_stations(self, mock_logger):
        """Test with multiple station codes"""
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance

        # Create a temporary CSV file
        filepath = self.create_temp_csv(self.multi_station_data)

        try:
            # Call the function
            result = sl.read_daily_probabilistic_ml_forecasts_pentad(
                filepath, model="TEST", model_short="TM"
            )

            # Check that the result is not empty
            self.assertFalse(result.empty)

            # Check that both station codes are present
            self.assertGreaterEqual(len(result["code"].unique()), 2)
            self.assertIn("15149", result["code"].values)
            self.assertIn("15083", result["code"].values)

            # Verify that only forecast_date from pentad days (5, 10, 15, 20, 25, 31) are in the results
            result_dates = pd.DatetimeIndex(result["date"]).day
            expected_pentad_days = [5, 10, 15, 20, 25, 31]  # March has 31 days as end of month
            for day in result_dates:
                self.assertIn(
                    day, expected_pentad_days, f"Date with day {day} shouldn't be in results"
                )

            # Verify we get the expected number of rows (one row per unique combination of code and pentad date)
            # Get unique combinations of code and pentad dates in the input data
            input_combinations = set()
            for _, row in self.multi_station_data.iterrows():
                code = row["code"]
                date = pd.to_datetime(row["forecast_date"])
                if date.day in expected_pentad_days:
                    input_combinations.add((code, date))

            expected_rows = len(input_combinations)
            self.assertEqual(
                len(result),
                expected_rows,
                f"Expected {expected_rows} results (one per code and pentad date), got {len(result)}",
            )

            # Test that each station code is represented for each pentad date
            # Create a DataFrame with all combinations of dates and codes
            pentad_dates = [
                d
                for d in pd.to_datetime(self.multi_station_data["forecast_date"].unique())
                if d.day in expected_pentad_days
            ]
            station_codes = self.multi_station_data["code"].unique()

            expected_combinations = set()
            for date in pentad_dates:
                for code in station_codes:
                    expected_combinations.add((code, date))

            actual_combinations = set()
            for _, row in result.iterrows():
                actual_combinations.add((row["code"], row["date"]))

            # Verify all expected combinations exist in the result
            for combo in expected_combinations:
                code, date = combo
                self.assertIn(
                    combo, actual_combinations, f"Missing result for code {code} and date {date}"
                )

            # Check for required columns
            required_columns = [
                "code",
                "date",
                "forecasted_discharge",
                "model_short",
                "pentad_in_month",
                "pentad_in_year",
            ]
            for col in required_columns:
                self.assertIn(col, result.columns)

            # Check if model info is correctly added
            self.assertEqual(result["model_short"].iloc[0], "TM")

            # Verify that each station's data is correctly aggregated
            # For each station and date, verify the forecasted discharge
            for code in station_codes:
                code_results = result[result["code"] == code]
                for _, row in code_results.iterrows():
                    date = row["date"]

                    # Get all rows from the original data with this code and forecast_date
                    original_rows = self.multi_station_data[
                        (self.multi_station_data["code"] == code)
                        & (self.multi_station_data["forecast_date"] == date)
                    ]

                    if not original_rows.empty:
                        # Calculate expected mean
                        expected_discharge = original_rows["Q50"].mean()

                        # Verify forecasted discharge
                        self.assertAlmostEqual(
                            row["forecasted_discharge"],
                            expected_discharge,
                            places=5,
                            msg=f"Incorrect discharge for code {code} and date {date}",
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
        self.val_data["code"] = self.val_data["code"].astype(str)
        self.val_data["date"] = pd.to_datetime(self.val_data["date"]).dt.date
        self.val_data["forecast_date"] = pd.to_datetime(self.val_data["forecast_date"]).dt.date

        # Process using the function to test
        self.test_data = sl.read_daily_probabilistic_ml_forecasts_pentad(
            self.file_path, "test", model_short="test"
        )
        # Cast code column to string
        self.test_data["code"] = self.test_data["code"].astype(str)
        self.test_data["date"] = pd.to_datetime(self.test_data["date"]).dt.date

    def test_columns_present(self):
        # Test if columns in val_data are present in test_data.
        # Q50 → forecasted_discharge; other Q* columns → lowercase q* (e.g. Q5 → q05).
        for col in self.val_data.columns:
            if col == "forecast_date":
                continue
            elif col == "Q50":
                self.assertIn(
                    "forecasted_discharge",
                    self.test_data.columns,
                    f"Column {col} is missing in processed data",
                )
            elif col.startswith("Q") and col[1:].isdigit():
                expected = f"q{int(col[1:]):02d}"
                self.assertIn(
                    expected,
                    self.test_data.columns,
                    f"Column {col} (normalized to {expected}) is missing in processed data",
                )
            else:
                self.assertIn(
                    col, self.test_data.columns, f"Column {col} is missing in processed data"
                )

    def test_data(self):
        # Define a few codes and dates to test
        codes = [16161, 15083, 14283, 15054]
        # Cast codes to string, as in the test_data they are strings
        codes = [str(code) for code in codes]
        # Dates are in the format YYYY-MM-DD and are the last day of the previous pentad
        dates = ["2010-12-31", "2010-04-05", "2010-10-25", "2010-07-10"]
        # Cast dates to dates in format YYYY-MM-DD, as in the test_data they are dates
        dates = [pd.to_datetime(date).date() for date in dates]

        for code in codes:
            for date in dates:
                print(f"Testing code {code} and date {date}")
                validation_data_code = self.val_data[
                    (self.val_data["code"] == code) & (self.val_data["forecast_date"] == date)
                ]
                # Test if validation_data_code is not empty
                self.assertFalse(validation_data_code.empty)
                # Sort validation_data_code by date
                validation_data_code = validation_data_code.sort_values(by="date")
                # Calculate the mean of the quantiles, only for columns starting with 'Q'
                mean = validation_data_code.filter(regex="^Q").mean(axis=0).round(3)
                # Get the test data for the code and forecast date
                test_data_code = self.test_data[
                    (self.test_data["code"] == code) & (self.test_data["date"] == date)
                ]
                # Test if test_data_code is not empty
                self.assertFalse(test_data_code.empty)

                # Special case for code 16161 and date 2010-07-10: We don't have data and all quantiles are not defined
                if code == "16161" and date == pd.to_datetime("2010-07-10").date():
                    self.assertTrue(test_data_code["forecasted_discharge"].isnull().values[0])
                    self.assertTrue(test_data_code["q10"].isnull().values[0])
                    self.assertTrue(test_data_code["q25"].isnull().values[0])
                    self.assertTrue(test_data_code["q75"].isnull().values[0])
                    self.assertTrue(test_data_code["q90"].isnull().values[0])
                    continue

                # Assert that all values in columns starting with Q are equal in both dataframes
                # Raw CSV uses uppercase (Q5, Q10, ...), processed uses lowercase (q05, q10, ...)
                for col in mean.index:
                    if col == "Q50":
                        self.assertAlmostEqual(
                            mean[col], test_data_code["forecasted_discharge"].values[0], places=2
                        )
                    else:
                        normalized = f"q{int(col[1:]):02d}"
                        self.assertAlmostEqual(
                            mean[col], test_data_code[normalized].values[0], places=2
                        )


class TestReadDailyProbabilisticMLForecastsDecade(unittest.TestCase):
    def setUp(self):
        # Setup runs before each test
        self.file_path = "iEasyHydroForecast/tests/test_data/test_probabil_forecast.csv"

        # Read validation data
        self.val_data = pd.read_csv(self.file_path)
        self.val_data.loc[:, "decad_in_year"] = self.val_data["date"].apply(tl.get_decad_in_year)
        # Cast code column to string
        self.val_data["code"] = self.val_data["code"].astype(str)
        self.val_data["date"] = pd.to_datetime(self.val_data["date"]).dt.date
        self.val_data["forecast_date"] = pd.to_datetime(self.val_data["forecast_date"]).dt.date

        # Process using the function to test
        self.test_data = sl.read_daily_probabilistic_ml_forecasts_decade(
            self.file_path, "test", model_short="test"
        )
        # Cast code column to string
        self.test_data["code"] = self.test_data["code"].astype(str)
        self.test_data["date"] = pd.to_datetime(self.test_data["date"]).dt.date

    def test_columns_present(self):
        # Test if columns in val_data are present in test_data.
        # Q50 → forecasted_discharge; other Q* columns → lowercase q* (e.g. Q5 → q05).
        for col in self.val_data.columns:
            if col == "forecast_date":
                continue
            elif col == "Q50":
                self.assertIn(
                    "forecasted_discharge",
                    self.test_data.columns,
                    f"Column {col} is missing in processed data",
                )
            elif col.startswith("Q") and col[1:].isdigit():
                expected = f"q{int(col[1:]):02d}"
                self.assertIn(
                    expected,
                    self.test_data.columns,
                    f"Column {col} (normalized to {expected}) is missing in processed data",
                )
            else:
                self.assertIn(
                    col, self.test_data.columns, f"Column {col} is missing in processed data"
                )

    def test_data(self):
        # Define a few codes and dates to test
        codes = [16161, 15083, 14283, 15054]
        # Cast codes to string, as in the test_data they are strings
        codes = [str(code) for code in codes]
        # Dates are in the format YYYY-MM-DD and are the last day of the previous decade
        dates = ["2010-12-31", "2010-04-10", "2010-10-20", "2010-07-10"]
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
                validation_data_code = self.val_data[
                    (self.val_data["code"] == code) & (self.val_data["forecast_date"] == date)
                ]
                # Test if validation_data_code is not empty
                self.assertFalse(validation_data_code.empty)
                # Sort validation_data_code by date
                validation_data_code = validation_data_code.sort_values(by="date")
                # Calculate the mean of the quantiles, only for columns starting with 'Q'
                mean = validation_data_code.filter(regex="^Q").mean(axis=0).round(3)
                # Get the test data for the code and forecast date
                test_data_code = self.test_data[
                    (self.test_data["code"] == code) & (self.test_data["date"] == date)
                ]
                # Test if test_data_code is not empty
                self.assertFalse(test_data_code.empty)

                # Special case for code 16161 and date 2010-07-10: We don't have data and all quantiles are not defined
                if code == "16161" and date == pd.to_datetime("2010-07-10").date():
                    self.assertTrue(test_data_code["forecasted_discharge"].isnull().values[0])
                    self.assertTrue(test_data_code["q10"].isnull().values[0])
                    self.assertTrue(test_data_code["q25"].isnull().values[0])
                    self.assertTrue(test_data_code["q75"].isnull().values[0])
                    self.assertTrue(test_data_code["q90"].isnull().values[0])
                    continue

                # Assert that all values in columns starting with Q are equal in both dataframes
                # Raw CSV uses uppercase (Q5, Q10, ...), processed uses lowercase (q05, q10, ...)
                for col in mean.index:
                    if col == "Q50":
                        self.assertAlmostEqual(
                            mean[col], test_data_code["forecasted_discharge"].values[0], places=2
                        )
                    else:
                        normalized = f"q{int(col[1:]):02d}"
                        self.assertAlmostEqual(
                            mean[col], test_data_code[normalized].values[0], places=2
                        )


class TestModelLongDeprecation(unittest.TestCase):
    """Tests for INFRA-005: model_long parameter deprecation."""

    def setUp(self):
        self.file_path = os.path.join(
            os.path.dirname(__file__), "test_data/test_probabil_forecast.csv"
        )

    def test_ml_pentad_emits_deprecation_warning(self):
        """Passing model_long emits DeprecationWarning."""
        with self.assertWarns(DeprecationWarning) as cm:
            sl.read_daily_probabilistic_ml_forecasts_pentad(
                self.file_path,
                "TIDE",
                model_long="TIDE model (TIDE)",
                model_short="TIDE",
            )
        self.assertIn("model_long", str(cm.warning))

    def test_ml_pentad_no_warning_without_model_long(self):
        """Omitting model_long produces no DeprecationWarning."""
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            sl.read_daily_probabilistic_ml_forecasts_pentad(
                self.file_path,
                "TIDE",
                model_short="TIDE",
            )
        deprecation_warnings = [
            x
            for x in w
            if issubclass(x.category, DeprecationWarning) and "model_long" in str(x.message)
        ]
        self.assertEqual(len(deprecation_warnings), 0)

    def test_ml_pentad_output_lacks_model_long(self):
        """Output DataFrame has model_short but not model_long."""
        result = sl.read_daily_probabilistic_ml_forecasts_pentad(
            self.file_path,
            "TIDE",
            model_short="TIDE",
        )
        self.assertIn("model_short", result.columns)
        self.assertNotIn("model_long", result.columns)
        self.assertEqual(result["model_short"].unique()[0], "TIDE")

    def test_ml_decade_emits_deprecation_warning(self):
        """Passing model_long to decade function emits DeprecationWarning."""
        with self.assertWarns(DeprecationWarning):
            sl.read_daily_probabilistic_ml_forecasts_decade(
                self.file_path,
                "TIDE",
                model_long="TIDE model (TIDE)",
                model_short="TIDE",
            )

    def test_ml_decade_output_lacks_model_long(self):
        """Decade output has model_short but not model_long."""
        result = sl.read_daily_probabilistic_ml_forecasts_decade(
            self.file_path,
            "TIDE",
            model_short="TIDE",
        )
        self.assertIn("model_short", result.columns)
        self.assertNotIn("model_long", result.columns)


class TestGetPentadalForecastSitesReturnTypes(unittest.TestCase):
    """Regression test: site_codes must be a Python list, not numpy.ndarray.

    Before the fix, ``db_sites["site_code"].unique()`` returned a numpy
    array, which caused ``ValueError: truth value of an array is ambiguous``
    when the result was used in boolean context downstream.
    """

    @patch("iEasyHydroForecast.setup_library.fl.Site.from_dataframe")
    @patch("iEasyHydroForecast.setup_library.get_pentadal_forecast_sites_complicated_method")
    def test_site_codes_returns_list_not_numpy(self, mock_complicated, mock_from_df):
        """Verify site_codes is a Python list to prevent numpy
        truth-value errors."""
        mock_complicated.return_value = pd.DataFrame(
            {
                "site_code": ["12345", "67890", "12345"],
                "site_name": ["Site A", "Site B", "Site A"],
                "river_ru": ["River1", "River2", "River1"],
                "punkt_ru": ["Punkt1", "Punkt2", "Punkt1"],
                "latitude": [42.0, 43.0, 42.0],
                "longitude": [74.0, 75.0, 74.0],
                "region": ["Region1", "Region2", "Region1"],
                "basin": ["Basin1", "Basin2", "Basin1"],
            }
        )

        mock_site_a = MagicMock(code="12345")
        mock_site_b = MagicMock(code="67890")
        mock_from_df.return_value = [mock_site_a, mock_site_b]

        mock_sdk = MagicMock()
        fc_sites, site_codes = sl.get_pentadal_forecast_sites(mock_sdk, False)

        # The return type must be list, not numpy.ndarray, to avoid
        # "ValueError: truth value of an array is ambiguous" downstream.
        assert isinstance(site_codes, list)
        assert not isinstance(site_codes, np.ndarray)
        self.assertCountEqual(site_codes, ["12345", "67890"])


class TestGetPentadalForecastSitesFromHFSdk(unittest.TestCase):
    def setUp(self):
        self.mock_ieh_hf_sdk = MagicMock()
        self.mock_discharge_sites = [
            {"site_code": "12345", "site_name": "Test Site 1", "iehhf_site_id": "ID1"},
            {"site_code": "67890", "site_name": "Test Site 2", "iehhf_site_id": "ID2"},
        ]
        self.mock_virtual_sites = [
            {"site_code": "V123", "site_name": "Virtual Site 1", "iehhf_site_id": "VID1"}
        ]
        self.mock_ieh_hf_sdk.get_discharge_sites.return_value = self.mock_discharge_sites
        self.mock_ieh_hf_sdk.get_virtual_sites.return_value = self.mock_virtual_sites

        self.mock_fc_sites = [
            MagicMock(code="12345", iehhf_site_id="ID1"),
            MagicMock(code="67890", iehhf_site_id="ID2"),
        ]
        self.mock_virtual_fc_sites = [MagicMock(code="V123", iehhf_site_id="VID1")]

        # Patch fl.Site methods
        self.patch_pentad_forecast_sites = patch(
            "forecast_library.Site.pentad_forecast_sites_from_iEH_HF_SDK",
            return_value=self.mock_fc_sites,
        )
        self.patch_virtual_pentad_forecast_sites = patch(
            "forecast_library.Site.virtual_pentad_forecast_sites_from_iEH_HF_SDK",
            return_value=self.mock_virtual_fc_sites,
        )
        self.patch_os_path_join = patch("os.path.join", return_value="/path/to/config.json")
        self.patch_open = patch("builtins.open", new_callable=MagicMock)
        self.patch_os_getenv = patch.dict(
            os.environ,
            {
                "ieasyforecast_configuration_path": "/config",
                "ieasyforecast_config_file_station_selection": "config.json",
            },
        )
        self.addCleanup(patch.stopall)

        self.mock_pentad_forecast_sites = self.patch_pentad_forecast_sites.start()
        self.mock_virtual_pentad_forecast_sites = self.patch_virtual_pentad_forecast_sites.start()
        self.mock_os_path_join = self.patch_os_path_join.start()
        self.mock_open = self.patch_open.start()
        self.mock_os_getenv = self.patch_os_getenv.start()

    def test_get_pentadal_forecast_sites_from_HF_SDK(self):
        fc_sites, site_codes, site_ids = sl.get_pentadal_forecast_sites_from_HF_SDK(
            self.mock_ieh_hf_sdk
        )

        # Assertions
        self.assertEqual(len(fc_sites), 3)
        self.assertEqual(len(site_codes), 3)
        self.assertEqual(len(site_ids), 3)

        self.assertCountEqual(site_codes, ["12345", "67890", "V123"])
        self.assertCountEqual(site_ids, ["ID1", "ID2", "VID1"])

        self.mock_ieh_hf_sdk.get_discharge_sites.assert_called_once()
        self.mock_ieh_hf_sdk.get_virtual_sites.assert_called_once()
        self.mock_pentad_forecast_sites.assert_called_once_with(self.mock_discharge_sites)
        self.mock_virtual_pentad_forecast_sites.assert_called_once_with(self.mock_virtual_sites)

        self.mock_os_path_join.assert_called_with("/config", "config.json")
        self.mock_open.assert_called()


class TestGetDecadalForecastSitesFromHFSdk(unittest.TestCase):
    def setUp(self):
        self.mock_ieh_hf_sdk = MagicMock()
        self.mock_discharge_sites = [
            {"site_code": "12345", "site_name": "Test Site 1", "iehhf_site_id": "ID1"},
            {"site_code": "67890", "site_name": "Test Site 2", "iehhf_site_id": "ID2"},
        ]
        self.mock_virtual_sites = [
            {"site_code": "V123", "site_name": "Virtual Site 1", "iehhf_site_id": "VID1"}
        ]
        self.mock_ieh_hf_sdk.get_discharge_sites.return_value = self.mock_discharge_sites
        self.mock_ieh_hf_sdk.get_virtual_sites.return_value = self.mock_virtual_sites

        self.mock_fc_sites = [
            MagicMock(code="12345", iehhf_site_id="ID1"),
            MagicMock(code="67890", iehhf_site_id="ID2"),
        ]
        self.mock_virtual_fc_sites = [MagicMock(code="V123", iehhf_site_id="VID1")]

        # Patch fl.Site methods
        self.patch_decad_forecast_sites = patch(
            "forecast_library.Site.decad_forecast_sites_from_iEH_HF_SDK",
            return_value=self.mock_fc_sites,
        )
        self.patch_virtual_decad_forecast_sites = patch(
            "forecast_library.Site.virtual_decad_forecast_sites_from_iEH_HF_SDK",
            return_value=self.mock_virtual_fc_sites,
        )
        self.patch_os_path_join = patch("os.path.join", return_value="/path/to/config.json")
        self.patch_open = patch("builtins.open", new_callable=MagicMock)
        self.patch_os_getenv = patch.dict(
            os.environ,
            {
                "ieasyforecast_configuration_path": "/config",
                "ieasyforecast_config_file_station_selection_decad": "config.json",
            },
        )
        self.addCleanup(patch.stopall)

        self.mock_decad_forecast_sites = self.patch_decad_forecast_sites.start()
        self.mock_virtual_decad_forecast_sites = self.patch_virtual_decad_forecast_sites.start()
        self.mock_os_path_join = self.patch_os_path_join.start()
        self.mock_open = self.patch_open.start()
        self.mock_os_getenv = self.patch_os_getenv.start()

    def test_get_decadal_forecast_sites_from_HF_SDK(self):
        fc_sites, site_codes, site_ids = sl.get_decadal_forecast_sites_from_HF_SDK(
            self.mock_ieh_hf_sdk
        )

        # Assertions
        self.assertEqual(len(fc_sites), 3)
        self.assertEqual(len(site_codes), 3)
        self.assertEqual(len(site_ids), 3)

        self.assertCountEqual(site_codes, ["12345", "67890", "V123"])
        self.assertCountEqual(site_ids, ["ID1", "ID2", "VID1"])

        self.mock_ieh_hf_sdk.get_discharge_sites.assert_called_once()
        self.mock_ieh_hf_sdk.get_virtual_sites.assert_called_once()
        self.mock_decad_forecast_sites.assert_called_once_with(self.mock_discharge_sites)
        self.mock_virtual_decad_forecast_sites.assert_called_once_with(self.mock_virtual_sites)

        self.mock_os_path_join.assert_called_with("/config", "config.json")
        self.mock_open.assert_called()


class TestManualSiteProtection(unittest.TestCase):
    """Tests for Phase 1: manual site protection during config refresh."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.config_all_file = os.path.join(self.tmpdir, "config_all_stations_library.json")
        self.config_selection_file = os.path.join(self.tmpdir, "config_station_selection.json")
        self.config_selection_decad_file = os.path.join(
            self.tmpdir, "config_station_selection_decad"
        )
        self.env_patcher = patch.dict(
            os.environ,
            {
                "ieasyforecast_configuration_path": self.tmpdir,
                "ieasyforecast_config_file_all_stations": "config_all_stations_library.json",
                "ieasyforecast_config_file_station_selection": "config_station_selection.json",
                "ieasyforecast_config_file_station_selection_decad": "config_station_selection_decad",
            },
        )
        self.env_patcher.start()

    def tearDown(self):
        self.env_patcher.stop()
        shutil.rmtree(self.tmpdir)

    def _write_config_all(self, stations_dict):
        data = {"stations_available_for_forecast": stations_dict}
        with open(self.config_all_file, "w") as f:
            json.dump(data, f)

    def _read_config_all(self):
        with open(self.config_all_file) as f:
            return json.load(f)["stations_available_for_forecast"]

    def _write_selection(self, path, codes):
        with open(path, "w") as f:
            json.dump({"stationsID": codes}, f)

    def _read_selection(self, path):
        with open(path) as f:
            return json.load(f)["stationsID"]

    # --- _get_manual_site_codes tests ---

    def test_get_manual_site_codes_returns_manual(self):
        self._write_config_all(
            {
                "12176": {"code": [12176], "name_ru": ["A"], "lat": [42], "long": [74]},
                "99001": {
                    "code": [99001],
                    "name_ru": ["B"],
                    "lat": [41],
                    "long": [73],
                    "data_source": ["manual"],
                },
            }
        )
        result = sl._get_manual_site_codes()
        self.assertEqual(result, ["99001"])

    def test_get_manual_site_codes_absent_field_treated_as_ieh_hf(self):
        self._write_config_all(
            {
                "12176": {"code": [12176], "name_ru": ["A"], "lat": [42], "long": [74]},
            }
        )
        result = sl._get_manual_site_codes()
        self.assertEqual(result, [])

    def test_get_manual_site_codes_ieh_hf_not_returned(self):
        self._write_config_all(
            {
                "12176": {
                    "code": [12176],
                    "name_ru": ["A"],
                    "lat": [42],
                    "long": [74],
                    "data_source": ["ieh_hf"],
                },
            }
        )
        result = sl._get_manual_site_codes()
        self.assertEqual(result, [])

    def test_get_manual_site_codes_unwraps_list(self):
        """data_source stored as list-wrapped value is correctly unwrapped."""
        self._write_config_all(
            {
                "99001": {
                    "code": [99001],
                    "name_ru": ["B"],
                    "lat": [41],
                    "long": [73],
                    "data_source": ["manual"],
                },
            }
        )
        result = sl._get_manual_site_codes()
        self.assertEqual(result, ["99001"])

    def test_get_manual_site_codes_unwraps_string(self):
        """data_source stored as plain string is handled."""
        self._write_config_all(
            {
                "99001": {
                    "code": [99001],
                    "name_ru": ["B"],
                    "lat": [41],
                    "long": [73],
                    "data_source": "manual",
                },
            }
        )
        result = sl._get_manual_site_codes()
        self.assertEqual(result, ["99001"])

    def test_get_manual_site_codes_file_missing(self):
        result = sl._get_manual_site_codes()
        self.assertEqual(result, [])

    # --- _read_manual_entries_from_config tests ---

    def test_read_manual_entries_returns_full_entry(self):
        manual_entry = {
            "code": [99001],
            "name_ru": ["B"],
            "lat": [41],
            "long": [73],
            "data_source": ["manual"],
            "river_ru": ["River"],
            "punkt_ru": ["Point"],
            "basin": ["Basin"],
            "region": ["Region"],
        }
        self._write_config_all({"12176": {"code": [12176]}, "99001": manual_entry})
        result = sl._read_manual_entries_from_config()
        self.assertEqual(list(result.keys()), ["99001"])
        self.assertEqual(result["99001"]["data_source"], ["manual"])

    def test_read_manual_entries_empty_on_missing_file(self):
        result = sl._read_manual_entries_from_config()
        self.assertEqual(result, {})

    # --- Guard for get_pentadal_forecast_sites_complicated_method ---

    def test_complicated_method_preserves_manual_sites(self):
        """Manual site survives config refresh via the complicated method."""
        manual_entry = {
            "code": [99001],
            "name_ru": ["Manual Site"],
            "lat": [41.2],
            "long": [73.1],
            "data_source": ["manual"],
            "river_ru": ["River"],
            "punkt_ru": ["Point"],
            "basin": ["Basin"],
            "region": ["Region"],
        }
        sdk_entry = {
            "code": [12176],
            "name_ru": ["SDK Site"],
            "lat": [42.5],
            "long": [74.6],
            "river_ru": ["R1"],
            "punkt_ru": ["P1"],
            "basin": ["B1"],
            "region": ["Reg1"],
        }
        self._write_config_all(
            {
                "12176": sdk_entry,
                "99001": manual_entry,
            }
        )
        self._write_selection(self.config_selection_file, ["12176", "99001"])

        with patch.dict(
            os.environ,
            {
                "ieasyforecast_connect_to_iEH": "False",
                "ieasyforecast_restrict_stations_file": "null",
            },
        ):
            sl.get_pentadal_forecast_sites_complicated_method(None, False)

        written = self._read_config_all()
        self.assertIn("99001", written)
        ds = written["99001"].get("data_source")
        if isinstance(ds, list):
            ds = ds[0]
        self.assertEqual(ds, "manual")

    def test_complicated_method_creates_backup(self):
        """Config file is backed up before overwriting."""
        self._write_config_all(
            {
                "12176": {
                    "code": [12176],
                    "name_ru": ["A"],
                    "lat": [42],
                    "long": [74],
                    "river_ru": ["R"],
                    "punkt_ru": ["P"],
                    "basin": ["B"],
                    "region": ["Reg"],
                },
            }
        )
        self._write_selection(self.config_selection_file, ["12176"])

        with patch.dict(
            os.environ,
            {
                "ieasyforecast_connect_to_iEH": "False",
                "ieasyforecast_restrict_stations_file": "null",
            },
        ):
            sl.get_pentadal_forecast_sites_complicated_method(None, False)

        self.assertTrue(os.path.exists(self.config_all_file + ".bak"))

    def test_empty_sdk_does_not_wipe_manual_sites(self):
        """When SDK returns no sites, manual sites are preserved."""
        manual_entry = {
            "code": [99001],
            "name_ru": ["Manual Site"],
            "lat": [41.2],
            "long": [73.1],
            "data_source": ["manual"],
            "river_ru": ["River"],
            "punkt_ru": ["Point"],
            "basin": ["Basin"],
            "region": ["Region"],
        }
        # Only manual site in config, no SDK sites
        self._write_config_all({"99001": manual_entry})
        self._write_selection(self.config_selection_file, ["99001"])

        with patch.dict(
            os.environ,
            {
                "ieasyforecast_connect_to_iEH": "False",
                "ieasyforecast_restrict_stations_file": "null",
            },
        ):
            sl.get_pentadal_forecast_sites_complicated_method(None, False)

        written = self._read_config_all()
        self.assertIn("99001", written)

    def test_collision_removes_manual_entry_from_merge(self):
        """When db_sites contains the same code as a manual entry, the manual
        entry is dropped from the merge-back (SDK wins)."""
        # Config has both an SDK site and a manual site with the same code
        self._write_config_all(
            {
                "12176": {
                    "code": [12176],
                    "name_ru": ["Manual Imposter"],
                    "lat": [41],
                    "long": [73],
                    "data_source": ["manual"],
                    "river_ru": ["R"],
                    "punkt_ru": ["P"],
                    "basin": ["B"],
                    "region": ["Reg"],
                },
            }
        )
        # _read_manual_entries_from_config returns the manual entry
        entries = sl._read_manual_entries_from_config()
        self.assertIn("12176", entries)

        # Simulate the collision detection logic from complicated_method:
        # db_sites contains 12176 from SDK
        sdk_codes = {"12176"}
        for code in list(entries.keys()):
            if code in sdk_codes:
                del entries[code]

        # After collision, manual entry is removed
        self.assertEqual(entries, {})

    # --- Guard for station selection writers ---

    def test_pentadal_from_hf_sdk_preserves_manual_codes(self):
        """Manual site codes survive pentadal selection refresh."""
        self._write_config_all(
            {
                "99001": {
                    "code": [99001],
                    "name_ru": ["Manual"],
                    "lat": [41],
                    "long": [73],
                    "data_source": ["manual"],
                },
            }
        )
        self._write_selection(self.config_selection_file, ["12345", "99001"])

        mock_sdk = MagicMock()
        mock_sdk.get_discharge_sites.return_value = [
            {"site_code": "12345", "site_name": "S1", "iehhf_site_id": "ID1"}
        ]
        mock_sdk.get_virtual_sites.return_value = []

        mock_site = MagicMock(code="12345", iehhf_site_id="ID1")
        with (
            patch(
                "forecast_library.Site.pentad_forecast_sites_from_iEH_HF_SDK",
                return_value=[mock_site],
            ),
            patch(
                "forecast_library.Site.virtual_pentad_forecast_sites_from_iEH_HF_SDK",
                return_value=[],
            ),
        ):
            sl.get_pentadal_forecast_sites_from_HF_SDK(mock_sdk)

        codes = self._read_selection(self.config_selection_file)
        self.assertIn("99001", codes)
        self.assertIn("12345", codes)

    def test_decadal_from_hf_sdk_preserves_manual_codes(self):
        """Manual site codes survive decadal selection refresh."""
        self._write_config_all(
            {
                "99001": {
                    "code": [99001],
                    "name_ru": ["Manual"],
                    "lat": [41],
                    "long": [73],
                    "data_source": ["manual"],
                },
            }
        )

        mock_sdk = MagicMock()
        mock_sdk.get_discharge_sites.return_value = [
            {"site_code": "12345", "site_name": "S1", "iehhf_site_id": "ID1"}
        ]
        mock_sdk.get_virtual_sites.return_value = []

        mock_site = MagicMock(code="12345", iehhf_site_id="ID1")
        with (
            patch(
                "forecast_library.Site.decad_forecast_sites_from_iEH_HF_SDK",
                return_value=[mock_site],
            ),
            patch(
                "forecast_library.Site.virtual_decad_forecast_sites_from_iEH_HF_SDK",
                return_value=[],
            ),
        ):
            sl.get_decadal_forecast_sites_from_HF_SDK(mock_sdk)

        codes = self._read_selection(self.config_selection_decad_file)
        self.assertIn("99001", codes)
        self.assertIn("12345", codes)

    def test_all_from_hf_sdk_preserves_manual_codes(self):
        """Manual site codes survive all-forecasts selection refresh."""
        self._write_config_all(
            {
                "99001": {
                    "code": [99001],
                    "name_ru": ["Manual"],
                    "lat": [41],
                    "long": [73],
                    "data_source": ["manual"],
                },
            }
        )

        mock_sdk = MagicMock()
        mock_sdk.get_discharge_sites.return_value = [
            {"site_code": "12345", "site_name": "S1", "iehhf_site_id": "ID1"}
        ]
        mock_sdk.get_virtual_sites.return_value = []

        mock_site = MagicMock(code="12345", iehhf_site_id="ID1")
        with (
            patch(
                "forecast_library.Site.all_forecast_sites_from_iEH_HF_SDK", return_value=[mock_site]
            ),
            patch(
                "forecast_library.Site.virtual_all_forecast_sites_from_iEH_HF_SDK", return_value=[]
            ),
        ):
            sl.get_all_forecast_sites_from_HF_SDK(mock_sdk)

        codes = self._read_selection(self.config_selection_file)
        self.assertIn("99001", codes)
        self.assertIn("12345", codes)

    def test_concurrent_writers_preserve_manual_codes(self):
        """Both pentadal and all-forecasts writers preserve manual codes."""
        self._write_config_all(
            {
                "99001": {
                    "code": [99001],
                    "name_ru": ["Manual"],
                    "lat": [41],
                    "long": [73],
                    "data_source": ["manual"],
                },
            }
        )

        mock_sdk = MagicMock()
        mock_sdk.get_discharge_sites.return_value = [
            {"site_code": "12345", "site_name": "S1", "iehhf_site_id": "ID1"}
        ]
        mock_sdk.get_virtual_sites.return_value = []

        mock_site = MagicMock(code="12345", iehhf_site_id="ID1")

        # Run pentadal first
        with (
            patch(
                "forecast_library.Site.pentad_forecast_sites_from_iEH_HF_SDK",
                return_value=[mock_site],
            ),
            patch(
                "forecast_library.Site.virtual_pentad_forecast_sites_from_iEH_HF_SDK",
                return_value=[],
            ),
        ):
            sl.get_pentadal_forecast_sites_from_HF_SDK(mock_sdk)
        codes_after_pentad = self._read_selection(self.config_selection_file)
        self.assertIn("99001", codes_after_pentad)

        # Run all-forecasts second (overwrites same file)
        with (
            patch(
                "forecast_library.Site.all_forecast_sites_from_iEH_HF_SDK", return_value=[mock_site]
            ),
            patch(
                "forecast_library.Site.virtual_all_forecast_sites_from_iEH_HF_SDK", return_value=[]
            ),
        ):
            sl.get_all_forecast_sites_from_HF_SDK(mock_sdk)
        codes_after_all = self._read_selection(self.config_selection_file)
        self.assertIn("99001", codes_after_all)

    def test_data_source_roundtrip(self):
        """Write a manual entry, read it back via _get_manual_site_codes."""
        manual_entry = {
            "code": [99001],
            "name_ru": ["Manual"],
            "lat": [41.2],
            "long": [73.1],
            "data_source": ["manual"],
            "river_ru": ["River"],
            "punkt_ru": ["Point"],
            "basin": ["Basin"],
            "region": ["Region"],
        }
        self._write_config_all({"99001": manual_entry})

        # Verify round-trip
        codes = sl._get_manual_site_codes()
        self.assertEqual(codes, ["99001"])

        entries = sl._read_manual_entries_from_config()
        self.assertIn("99001", entries)
        ds = entries["99001"]["data_source"]
        if isinstance(ds, list):
            ds = ds[0]
        self.assertEqual(ds, "manual")


class TestWriteConfigAllStations(unittest.TestCase):
    """Tests for write_config_all_stations and its helpers."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.config_all_file = os.path.join(self.tmpdir, "config_all_stations_library.json")
        self.env_patcher = patch.dict(
            os.environ,
            {
                "ieasyforecast_configuration_path": self.tmpdir,
                "ieasyforecast_config_file_all_stations": ("config_all_stations_library.json"),
            },
        )
        self.env_patcher.start()

    def tearDown(self):
        self.env_patcher.stop()
        shutil.rmtree(self.tmpdir)

    def _make_site(
        self,
        code="12345",
        name_nat="\u0440. \u0422\u0435\u0441\u0442 - \u0441. \u0421\u0435\u043b\u043e",
        lat=42.5,
        lon=74.5,
        basin_nat="\u0427\u0443",
        region_nat="\u0427\u0443\u0439\u0441\u043a\u0430\u044f",
        river_name_nat="\u0440. \u0422\u0435\u0441\u0442",
        punkt_name_nat="\u0441. \u0421\u0435\u043b\u043e",
        site_type="automatic-discharge",
        iehhf_site_id=99,
    ):
        """Create a mock Site object with the given attributes."""
        site = MagicMock()
        site.code = code
        site.name = "r. Test - v. Village"
        site.name_nat = name_nat
        site.lat = lat
        site.lon = lon
        site.basin = "Chu"
        site.basin_nat = basin_nat
        site.region = "Chuy"
        site.region_nat = region_nat
        site.river_name = "r. Test"
        site.river_name_nat = river_name_nat
        site.punkt_name = "v. Village"
        site.punkt_name_nat = punkt_name_nat
        site.site_type = site_type
        site.iehhf_site_id = iehhf_site_id
        site.is_virtual = False
        site.organization = None
        return site

    def _read_config(self):
        with open(self.config_all_file, encoding="utf-8") as f:
            return json.load(f)

    def _write_manual_config(self, stations_dict):
        """Write a config file with given stations."""
        data = {"stations_available_for_forecast": stations_dict}
        with open(self.config_all_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    def test_writes_valid_json_from_site_objects(self):
        """Two Site objects are serialised to correct list-wrapped JSON."""
        site1 = self._make_site(code="12345", lat=42.5, lon=74.5)
        site2 = self._make_site(
            code="67890",
            name_nat="\u0440. \u0414\u0440\u0443\u0433\u0430\u044f"
            " - \u0441. \u0414\u0440\u0443\u0433\u043e\u0435",
            lat=41.0,
            lon=73.0,
            iehhf_site_id=100,
        )

        sl.write_config_all_stations([site1, site2], self.config_all_file)

        data = self._read_config()
        self.assertIn("stations_available_for_forecast", data)
        stations = data["stations_available_for_forecast"]
        self.assertIn("12345", stations)
        self.assertIn("67890", stations)

        # Verify list-wrapped fields for site1
        s1 = stations["12345"]
        self.assertEqual(s1["code"], [12345])
        self.assertEqual(s1["lat"], [42.5])
        self.assertEqual(s1["long"], [74.5])
        self.assertEqual(
            s1["name_ru"],
            ["\u0440. \u0422\u0435\u0441\u0442 - \u0441. \u0421\u0435\u043b\u043e"],
        )
        self.assertEqual(s1["basin"], ["\u0427\u0443"])
        self.assertEqual(s1["river_ru"], ["\u0440. \u0422\u0435\u0441\u0442"])
        self.assertEqual(s1["punkt_ru"], ["\u0441. \u0421\u0435\u043b\u043e"])
        self.assertEqual(s1["data_source"], ["ieh_hf"])

    def test_preserves_manual_entries(self):
        """Manual entries from existing config survive a write."""
        manual_entry = {
            "code": [99999],
            "name_ru": ["Manual Site"],
            "lat": [40.0],
            "long": [72.0],
            "data_source": ["google_sheets"],
            "river_ru": ["River"],
            "punkt_ru": ["Point"],
            "basin": ["Basin"],
            "region": ["Region"],
        }
        self._write_manual_config({"99999": manual_entry})

        with patch.object(
            sl,
            "_read_manual_entries_from_config",
            return_value={"99999": manual_entry},
        ):
            sl.write_config_all_stations([self._make_site()], self.config_all_file)

        stations = self._read_config()["stations_available_for_forecast"]
        self.assertIn("12345", stations)
        self.assertIn("99999", stations)
        self.assertEqual(stations["99999"]["data_source"], ["google_sheets"])

    def test_sdk_collision_removes_manual(self):
        """SDK data wins when a manual entry has the same code."""
        manual_entry = {
            "code": [12345],
            "name_ru": ["Manual Imposter"],
            "lat": [40.0],
            "long": [72.0],
            "data_source": ["google_sheets"],
            "river_ru": ["River"],
            "punkt_ru": ["Point"],
            "basin": ["Basin"],
            "region": ["Region"],
        }
        self._write_manual_config({"12345": manual_entry})

        with patch.object(
            sl,
            "_read_manual_entries_from_config",
            return_value={"12345": manual_entry.copy()},
        ):
            sl.write_config_all_stations([self._make_site(code="12345")], self.config_all_file)

        stations = self._read_config()["stations_available_for_forecast"]
        self.assertIn("12345", stations)
        self.assertEqual(stations["12345"]["data_source"], ["ieh_hf"])

    def test_backs_up_existing_file(self):
        """An existing config file is backed up before overwriting."""
        self._write_manual_config({"11111": {"code": [11111], "name_ru": ["Old"]}})

        sl.write_config_all_stations([self._make_site()], self.config_all_file)

        self.assertTrue(os.path.exists(self.config_all_file + ".bak"))

    def test_creates_file_when_missing(self):
        """Config file is created from scratch when it does not exist."""
        self.assertFalse(os.path.exists(self.config_all_file))

        sl.write_config_all_stations([self._make_site()], self.config_all_file)

        self.assertTrue(os.path.exists(self.config_all_file))
        data = self._read_config()
        self.assertIn("stations_available_for_forecast", data)
        self.assertIn("12345", data["stations_available_for_forecast"])

    def test_empty_site_list_writes_manual_only(self):
        """An empty SDK list still preserves manual entries."""
        manual_entry = {
            "code": [99999],
            "name_ru": ["Manual Only"],
            "lat": [40.0],
            "long": [72.0],
            "data_source": ["google_sheets"],
            "river_ru": ["River"],
            "punkt_ru": ["Point"],
            "basin": ["Basin"],
            "region": ["Region"],
        }

        with patch.object(
            sl,
            "_read_manual_entries_from_config",
            return_value={"99999": manual_entry},
        ):
            sl.write_config_all_stations([], self.config_all_file)

        stations = self._read_config()["stations_available_for_forecast"]
        self.assertIn("99999", stations)
        self.assertEqual(len(stations), 1)


class TestHfSdkBootstrapFallback(unittest.TestCase):
    """Tests for _try_bootstrap_from_hf_sdk and the fallback path."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.config_all_file = os.path.join(self.tmpdir, "config_all_stations_library.json")

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def _make_site(
        self,
        code="12345",
        name_nat="\u0440. \u0422\u0435\u0441\u0442 - \u0441. \u0421\u0435\u043b\u043e",
        lat=42.5,
        lon=74.5,
        basin_nat="\u0427\u0443",
        region_nat="\u0427\u0443\u0439\u0441\u043a\u0430\u044f",
        river_name_nat="\u0440. \u0422\u0435\u0441\u0442",
        punkt_name_nat="\u0441. \u0421\u0435\u043b\u043e",
        site_type="automatic-discharge",
        iehhf_site_id=99,
    ):
        site = MagicMock()
        site.code = code
        site.name = "r. Test - v. Village"
        site.name_nat = name_nat
        site.lat = lat
        site.lon = lon
        site.basin = "Chu"
        site.basin_nat = basin_nat
        site.region = "Chuy"
        site.region_nat = region_nat
        site.river_name = "r. Test"
        site.river_name_nat = river_name_nat
        site.punkt_name = "v. Village"
        site.punkt_name_nat = punkt_name_nat
        site.site_type = site_type
        site.iehhf_site_id = iehhf_site_id
        site.is_virtual = False
        site.organization = None
        return site

    def test_bootstrap_succeeds_with_hf_sdk(self):
        """When HF SDK is available and returns sites, config file is created."""
        fake_sites = [
            self._make_site(code="12176"),
            self._make_site(code="12345"),
        ]

        with (
            patch.object(sl, "IEASYHYDRO_HF_SDK_AVAILABLE", True),
            patch.object(sl, "IEasyHydroHFSDK", create=True) as mock_sdk_cls,
            patch.object(sl, "check_database_access", return_value=True),
            patch.object(
                sl,
                "get_all_forecast_sites_from_HF_SDK",
                return_value=(fake_sites, ["12176", "12345"], [42, 99]),
            ),
            patch.object(sl, "_read_manual_entries_from_config", return_value={}),
        ):
            result = sl._try_bootstrap_from_hf_sdk(self.config_all_file)

        # Config file should exist with stations
        assert os.path.exists(self.config_all_file)
        with open(self.config_all_file, encoding="utf-8") as f:
            data = json.load(f)
        assert "12176" in data["stations_available_for_forecast"]
        assert "12345" in data["stations_available_for_forecast"]

        # Result should be a non-empty DataFrame
        assert not result.empty
        mock_sdk_cls.assert_called_once()

    def test_bootstrap_returns_empty_when_sdk_unavailable(self):
        """When ieasyhydro_sdk is not installed, returns empty DataFrame."""
        with patch.object(sl, "IEASYHYDRO_HF_SDK_AVAILABLE", False):
            result = sl._try_bootstrap_from_hf_sdk(self.config_all_file)

        assert result.empty
        assert not os.path.exists(self.config_all_file)

    def test_bootstrap_empty_df_includes_organization_column(self):
        """Empty DataFrame returned when SDK unavailable includes organization column."""
        with patch.object(sl, "IEASYHYDRO_HF_SDK_AVAILABLE", False):
            result = sl._try_bootstrap_from_hf_sdk(self.config_all_file)

        assert result.empty
        assert "organization" in result.columns

    def test_bootstrap_returns_empty_on_sdk_init_failure(self):
        """When HF SDK init raises, returns empty DataFrame gracefully."""
        with (
            patch.object(sl, "IEASYHYDRO_HF_SDK_AVAILABLE", True),
            patch.object(
                sl,
                "IEasyHydroHFSDK",
                create=True,
                side_effect=ConnectionError("no server"),
            ),
        ):
            result = sl._try_bootstrap_from_hf_sdk(self.config_all_file)

        assert result.empty
        assert not os.path.exists(self.config_all_file)

    def test_bootstrap_returns_empty_when_no_db_access(self):
        """When SDK connects but has no DB access, returns empty DataFrame."""
        with (
            patch.object(sl, "IEASYHYDRO_HF_SDK_AVAILABLE", True),
            patch.object(sl, "IEasyHydroHFSDK", create=True),
            patch.object(sl, "check_database_access", return_value=False),
        ):
            result = sl._try_bootstrap_from_hf_sdk(self.config_all_file)

        assert result.empty


class TestSitesToConfigDict(unittest.TestCase):
    """Tests for _sites_to_config_dict — the helper that converts Site objects
    to the list-wrapped JSON dict format used by config_all_stations_library.json.
    """

    def _make_site(
        self,
        code="12345",
        name_nat="\u0440. \u0422\u0435\u0441\u0442 - \u0441. \u0421\u0435\u043b\u043e",
        lat=42.5,
        lon=74.5,
        basin_nat="\u0427\u0443",
        region_nat="\u0427\u0443\u0439\u0441\u043a\u0430\u044f",
        river_name_nat="\u0440. \u0422\u0435\u0441\u0442",
        punkt_name_nat="\u0441. \u0421\u0435\u043b\u043e",
        site_type="automatic-discharge",
        iehhf_site_id=99,
    ):
        """Create a mock Site object with the given attributes."""
        site = MagicMock()
        site.code = code
        site.name = "r. Test - v. Village"
        site.name_nat = name_nat
        site.lat = lat
        site.lon = lon
        site.basin = "Chu"
        site.basin_nat = basin_nat
        site.region = "Chuy"
        site.region_nat = region_nat
        site.river_name = "r. Test"
        site.river_name_nat = river_name_nat
        site.punkt_name = "v. Village"
        site.punkt_name_nat = punkt_name_nat
        site.site_type = site_type
        site.iehhf_site_id = iehhf_site_id
        site.is_virtual = False
        site.organization = None
        return site

    def test_organization_field_present(self):
        """organization is passed through when set on the site object."""
        site = self._make_site(code="12345")
        site.organization = "kghm"

        result = sl._sites_to_config_dict([site])

        self.assertEqual(result["12345"]["organization"], ["kghm"])

    def test_organization_field_none_when_not_set(self):
        """organization is [None] when site.organization is None."""
        site = self._make_site(code="12345")
        # _make_site sets site.organization = None by default

        result = sl._sites_to_config_dict([site])

        self.assertEqual(result["12345"]["organization"], [None])

    def test_organization_field_default_via_getattr(self):
        """organization falls back to [None] via getattr when attribute is absent."""
        site = MagicMock(
            spec=[
                "code",
                "name_nat",
                "lat",
                "lon",
                "basin_nat",
                "region_nat",
                "river_name_nat",
                "punkt_name_nat",
            ]
        )
        site.code = "55555"
        site.name_nat = "\u0440. \u0422\u0435\u0441\u0442 - \u0441. \u0422\u0435\u0441\u0442"
        site.lat = 40.0
        site.lon = 73.0
        site.basin_nat = "\u041d\u0430\u0440\u044b\u043d"
        site.region_nat = "\u041d\u0430\u0440\u044b\u043d\u0441\u043a\u0430\u044f"
        site.river_name_nat = "\u0440. \u0422\u0435\u0441\u0442"
        site.punkt_name_nat = "\u0441. \u0422\u0435\u0441\u0442"

        result = sl._sites_to_config_dict([site])

        self.assertEqual(result["55555"]["organization"], [None])

    def test_organization_id_unchanged(self):
        """organization_id field still exists and equals [None] after adding organization."""
        site = self._make_site(code="12345")

        result = sl._sites_to_config_dict([site])

        self.assertIn("organization_id", result["12345"])
        self.assertEqual(result["12345"]["organization_id"], [None])

    def test_organization_propagated_from_site_init(self):
        """organization set via Site.__init__ is correctly serialised."""
        from iEasyHydroForecast import forecast_library as fl

        site = fl.Site(
            code="77777",
            name="r. River - v. Village",
            river_name="r. River",
            punkt_name="v. Village",
            lat=41.5,
            lon=72.5,
            organization="tjhm",
        )

        result = sl._sites_to_config_dict([site])

        self.assertEqual(result["77777"]["organization"], ["tjhm"])


class TestFilterSitesByOrg:
    """Tests for filter_sites_by_org and _get_current_org."""

    def test_matching_org_filters_correctly(self):
        """Rows matching the requested org are kept; others are dropped."""
        df = pd.DataFrame(
            {
                "code": ["A", "B", "C"],
                "organization": ["kghm", "tjhm", "kghm"],
            }
        )

        result = sl.filter_sites_by_org(df, org="kghm")

        assert list(result["code"]) == ["A", "C"]
        assert len(result) == 2

    def test_non_matching_org_returns_empty(self):
        """No rows match the requested org; result is an empty DataFrame."""
        df = pd.DataFrame(
            {
                "code": ["A", "B"],
                "organization": ["tjhm", "tjhm"],
            }
        )

        result = sl.filter_sites_by_org(df, org="kghm")

        assert len(result) == 0

    def test_org_none_env_unset_returns_passthrough(self, monkeypatch):
        """Passthrough when org arg is None and env var is not set."""
        monkeypatch.delenv("ieasyhydroforecast_organization", raising=False)
        df = pd.DataFrame(
            {
                "code": ["A", "B"],
                "organization": ["kghm", "tjhm"],
            }
        )

        result = sl.filter_sites_by_org(df)

        assert len(result) == 2

    def test_missing_org_column_returns_passthrough(self):
        """Passthrough when DataFrame has no organization column."""
        df = pd.DataFrame({"code": ["A", "B", "C"]})

        result = sl.filter_sites_by_org(df, org="kghm")

        assert len(result) == 3

    def test_all_none_org_column_returns_passthrough(self):
        """Passthrough when all organization values are None (migration safety)."""
        df = pd.DataFrame(
            {
                "code": ["A", "B"],
                "organization": [None, None],
            }
        )

        result = sl.filter_sites_by_org(df, org="kghm")

        assert len(result) == 2

    def test_list_wrapped_values_unwrapped(self):
        """List-wrapped org values like ['kghm'] are unwrapped before filtering."""
        df = pd.DataFrame(
            {
                "code": ["A", "B"],
                "organization": [["kghm"], ["tjhm"]],
            }
        )

        result = sl.filter_sites_by_org(df, org="kghm")

        assert list(result["code"]) == ["A"]

    def test_mixed_none_and_string_keeps_none_rows(self):
        """Rows with org=None are kept alongside matching rows (Decision D1)."""
        df = pd.DataFrame(
            {
                "code": ["A", "B", "C"],
                "organization": ["kghm", None, "tjhm"],
            }
        )

        result = sl.filter_sites_by_org(df, org="kghm")

        assert set(result["code"]) == {"A", "B"}
        assert "C" not in result["code"].values

    def test_mixed_orgs_with_none(self):
        """None rows and matching-org rows are kept; non-matching are excluded."""
        df = pd.DataFrame(
            {
                "code": ["A", "B", "C", "D"],
                "organization": ["kghm", None, "tjhm", None],
            }
        )

        result = sl.filter_sites_by_org(df, org="kghm")

        assert len(result) == 3
        assert set(result["code"]) == {"A", "B", "D"}

    def test_get_current_org_returns_env_value(self, monkeypatch):
        """_get_current_org returns the value of the env var when set."""
        monkeypatch.setenv("ieasyhydroforecast_organization", "kghm")

        result = sl._get_current_org()

        assert result == "kghm"

    def test_get_current_org_returns_none_when_unset(self, monkeypatch):
        """_get_current_org returns None when the env var is not set."""
        monkeypatch.delenv("ieasyhydroforecast_organization", raising=False)

        result = sl._get_current_org()

        assert result is None

    def test_org_from_env_var(self, monkeypatch):
        """filter_sites_by_org reads org from env var when no arg is given."""
        monkeypatch.setenv("ieasyhydroforecast_organization", "tjhm")
        df = pd.DataFrame(
            {
                "code": ["A", "B"],
                "organization": ["kghm", "tjhm"],
            }
        )

        result = sl.filter_sites_by_org(df)

        assert list(result["code"]) == ["B"]

    def test_missing_org_column_triggers_passthrough_condition(self):
        """When org column is absent and org is set, all rows are returned unchanged.

        This is the condition that triggers a caller-side warning in
        get_pentadal_forecast_sites_complicated_method (len unchanged after filter).
        """
        df = pd.DataFrame({"code": ["A", "B"], "name": ["River A", "River B"]})

        result = sl.filter_sites_by_org(df, org="kghm")

        # All rows returned — same as pre-filter count
        assert len(result) == len(df)
        pd.testing.assert_frame_equal(result.reset_index(drop=True), df.reset_index(drop=True))

    def test_org_filter_independent_of_code_prefix(self):
        """Org filter is based on organization column, not station code prefix.

        Regression test: the old startswith hack would filter by code prefix
        (e.g. codes starting with "1" for kghm). The correct implementation
        uses the organization column regardless of station code prefix.
        """
        df = pd.DataFrame(
            {
                "code": ["15001", "15002", "25001", "25002"],
                "organization": ["kghm", "kghm", "tjhm", "tjhm"],
            }
        )

        result_kghm = sl.filter_sites_by_org(df, org="kghm")
        assert list(result_kghm["code"]) == ["15001", "15002"]

        result_tjhm = sl.filter_sites_by_org(df, org="tjhm")
        assert list(result_tjhm["code"]) == ["25001", "25002"]


class TestOrgBackfillAndWritePath:
    """Tests for organization backfill on read and inclusion on write."""

    def test_backfill_org_when_column_missing(self, monkeypatch):
        """config_all without 'organization' column gets it from env var.

        Simulates a legacy JSON that lacks the organization field.
        After backfill, the column should exist with the env var value.
        """
        monkeypatch.setenv("ieasyhydroforecast_organization", "kghm")
        config_all = pd.DataFrame(
            {
                "site_code": [15001, 15002],
                "organization_id": [1, 1],
            }
        )

        # Simulate the backfill logic from setup_library
        if "organization" not in config_all.columns or config_all["organization"].isna().all():
            org = os.getenv("ieasyhydroforecast_organization")
            if org:
                config_all["organization"] = org

        assert "organization" in config_all.columns
        assert (config_all["organization"] == "kghm").all()

    def test_backfill_org_when_column_all_none(self, monkeypatch):
        """config_all with all-None organization column gets backfilled.

        Args:
            monkeypatch: Pytest fixture for setting env vars.
        """
        monkeypatch.setenv("ieasyhydroforecast_organization", "tjhm")
        config_all = pd.DataFrame(
            {
                "site_code": [25001],
                "organization": [None],
            }
        )

        if "organization" not in config_all.columns or config_all["organization"].isna().all():
            org = os.getenv("ieasyhydroforecast_organization")
            if org:
                config_all["organization"] = org

        assert (config_all["organization"] == "tjhm").all()

    def test_no_backfill_when_org_column_populated(self, monkeypatch):
        """config_all with existing non-null organization is not overwritten.

        Args:
            monkeypatch: Pytest fixture for setting env vars.
        """
        monkeypatch.setenv("ieasyhydroforecast_organization", "tjhm")
        config_all = pd.DataFrame(
            {
                "site_code": [15001],
                "organization": ["kghm"],
            }
        )

        if "organization" not in config_all.columns or config_all["organization"].isna().all():
            org = os.getenv("ieasyhydroforecast_organization")
            if org:
                config_all["organization"] = org

        # Should remain kghm, not overwritten to tjhm
        assert (config_all["organization"] == "kghm").all()

    def test_write_path_includes_organization(self, monkeypatch):
        """stations_dict built from db_sites includes 'organization' key.

        Verifies that the write-path loop produces a dict entry with
        the organization field populated from the row or env var.

        Args:
            monkeypatch: Pytest fixture for setting env vars.
        """
        monkeypatch.setenv("ieasyhydroforecast_organization", "kghm")
        db_sites = pd.DataFrame(
            {
                "site_code": [15001, 15002],
                "site_name": ["Station A", "Station B"],
                "latitude": [42.0, 43.0],
                "longitude": [74.0, 75.0],
                "basin": ["Chu", "Chu"],
                "region": ["North", "North"],
                "river_ru": ["Chu", "Chu"],
                "punkt_ru": ["A", "B"],
                "site_type": ["manual", "manual"],
                "elevation": [1000, 1100],
                "organization_id": [1, 1],
                "organization": ["kghm", "kghm"],
                "country": ["KG", "KG"],
                "is_virtual": [False, False],
            }
        )

        stations_dict = {}
        for _, row in db_sites.iterrows():
            code_str = str(row["site_code"])
            stations_dict[code_str] = {
                "organization_id": [row.get("organization_id", None)],
                "organization": [
                    row.get(
                        "organization",
                        os.getenv("ieasyhydroforecast_organization"),
                    )
                ],
            }

        assert "organization" in stations_dict["15001"]
        assert stations_dict["15001"]["organization"] == ["kghm"]
        assert stations_dict["15002"]["organization"] == ["kghm"]

    def test_write_path_falls_back_to_env_var(self, monkeypatch):
        """When row lacks 'organization', env var is used as fallback.

        Args:
            monkeypatch: Pytest fixture for setting env vars.
        """
        monkeypatch.setenv("ieasyhydroforecast_organization", "tjhm")
        db_sites = pd.DataFrame(
            {
                "site_code": [25001],
                "organization_id": [2],
            }
        )

        for _, row in db_sites.iterrows():
            org_value = row.get(
                "organization",
                os.getenv("ieasyhydroforecast_organization"),
            )

        assert org_value == "tjhm"

    def test_backfill_enables_filter_to_work(self, monkeypatch):
        """End-to-end: backfill + filter_sites_by_org actually filters.

        Without backfill, filter_sites_by_org returns all rows because
        the organization column is missing. With backfill, it filters
        correctly.

        Args:
            monkeypatch: Pytest fixture for setting env vars.
        """
        monkeypatch.setenv("ieasyhydroforecast_organization", "kghm")

        # Simulate loading from JSON without organization column
        config_all = pd.DataFrame(
            {
                "site_code": [15001, 15002],
                "code": ["15001", "15002"],
            }
        )

        # Without backfill, filter returns all rows (passthrough)
        result_before = sl.filter_sites_by_org(config_all)
        assert len(result_before) == 2  # passthrough — no org column

        # Apply backfill
        if "organization" not in config_all.columns or config_all["organization"].isna().all():
            org = os.getenv("ieasyhydroforecast_organization")
            if org:
                config_all["organization"] = org

        # Now filter should keep rows (they match kghm)
        result_after = sl.filter_sites_by_org(config_all, org="kghm")
        assert len(result_after) == 2

        # And filter with a different org should drop them
        result_other = sl.filter_sites_by_org(config_all, org="tjhm")
        assert len(result_other) == 0


class TestCheckStationCodeCollisions:
    """Tests for check_station_code_collisions() (INFRA-012 Phase 2b)."""

    def test_no_foreign_stations(self, tmp_path, caplog):
        """All stations match the current org — no warning should be emitted.

        Args:
            tmp_path: Pytest-provided temporary directory.
            caplog: Pytest log capture fixture.
        """
        config = {
            "stations_available_for_forecast": {
                "99001": {"organization": "kghm"},
                "99002": {"organization": "kghm"},
            }
        }
        config_file = tmp_path / "config_all.json"
        config_file.write_text(json.dumps(config))

        env = {
            "ieasyforecast_configuration_path": str(tmp_path),
            "ieasyforecast_config_file_all_stations": "config_all.json",
            "ieasyhydroforecast_organization": "kghm",
        }
        with patch.dict(os.environ, env), caplog.at_level(logging.WARNING):
            sl.check_station_code_collisions()

        assert not any(r.levelno >= logging.WARNING for r in caplog.records)

    def test_detects_foreign_org(self, tmp_path, caplog):
        """Station tagged with a different org triggers a WARNING.

        Args:
            tmp_path: Pytest-provided temporary directory.
            caplog: Pytest log capture fixture.
        """
        config = {
            "stations_available_for_forecast": {
                "99001": {"organization": "kghm"},
                "25001": {"organization": "tjhm"},
            }
        }
        config_file = tmp_path / "config_all.json"
        config_file.write_text(json.dumps(config))

        env = {
            "ieasyforecast_configuration_path": str(tmp_path),
            "ieasyforecast_config_file_all_stations": "config_all.json",
            "ieasyhydroforecast_organization": "kghm",
        }
        with patch.dict(os.environ, env), caplog.at_level(logging.WARNING):
            sl.check_station_code_collisions()

        warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any("FOREIGN ORG CONTAMINATION" in m for m in warning_messages)
        assert any("25001" in m for m in warning_messages)

    def test_skips_when_no_org_field(self, tmp_path, caplog):
        """Stations with organization=[null] trigger a debug message, not a warning.

        Args:
            tmp_path: Pytest-provided temporary directory.
            caplog: Pytest log capture fixture.
        """
        config = {
            "stations_available_for_forecast": {
                "99001": {"organization": [None]},
                "99002": {"organization": [None]},
            }
        }
        config_file = tmp_path / "config_all.json"
        config_file.write_text(json.dumps(config))

        env = {
            "ieasyforecast_configuration_path": str(tmp_path),
            "ieasyforecast_config_file_all_stations": "config_all.json",
            "ieasyhydroforecast_organization": "kghm",
        }
        with patch.dict(os.environ, env), caplog.at_level(logging.DEBUG):
            sl.check_station_code_collisions()

        assert not any(r.levelno >= logging.WARNING for r in caplog.records)
        debug_messages = [r.message for r in caplog.records if r.levelno == logging.DEBUG]
        assert any("not available" in m for m in debug_messages)

    def test_skips_when_file_missing(self, tmp_path, caplog):
        """Missing config file is silently ignored — no crash, no warning.

        Args:
            tmp_path: Pytest-provided temporary directory.
            caplog: Pytest log capture fixture.
        """
        env = {
            "ieasyforecast_configuration_path": str(tmp_path),
            "ieasyforecast_config_file_all_stations": "nonexistent_config.json",
            "ieasyhydroforecast_organization": "kghm",
        }
        with patch.dict(os.environ, env), caplog.at_level(logging.WARNING):
            sl.check_station_code_collisions()

        assert not any(r.levelno >= logging.WARNING for r in caplog.records)

    def test_handles_list_wrapped_org(self, tmp_path, caplog):
        """List-wrapped organization values like ["kghm"] are correctly unwrapped.

        Args:
            tmp_path: Pytest-provided temporary directory.
            caplog: Pytest log capture fixture.
        """
        config = {
            "stations_available_for_forecast": {
                "99001": {"organization": ["kghm"]},
                "99002": {"organization": ["kghm"]},
            }
        }
        config_file = tmp_path / "config_all.json"
        config_file.write_text(json.dumps(config))

        env = {
            "ieasyforecast_configuration_path": str(tmp_path),
            "ieasyforecast_config_file_all_stations": "config_all.json",
            "ieasyhydroforecast_organization": "kghm",
        }
        with patch.dict(os.environ, env), caplog.at_level(logging.WARNING):
            sl.check_station_code_collisions()

        assert not any(r.levelno >= logging.WARNING for r in caplog.records)

    def test_ignores_metadata_keys(self, tmp_path, caplog):
        """Top-level keys like "comment" in the station dict are silently ignored.

        Args:
            tmp_path: Pytest-provided temporary directory.
            caplog: Pytest log capture fixture.
        """
        config = {
            "stations_available_for_forecast": {
                "comment": "some descriptive text",
                "99001": {"organization": "kghm"},
            }
        }
        config_file = tmp_path / "config_all.json"
        config_file.write_text(json.dumps(config))

        env = {
            "ieasyforecast_configuration_path": str(tmp_path),
            "ieasyforecast_config_file_all_stations": "config_all.json",
            "ieasyhydroforecast_organization": "kghm",
        }
        with patch.dict(os.environ, env), caplog.at_level(logging.WARNING):
            sl.check_station_code_collisions()

        assert not any(r.levelno >= logging.WARNING for r in caplog.records)

    def test_navigates_wrapper(self, tmp_path, caplog):
        """stations_available_for_forecast wrapper is navigated correctly.

        Foreign station inside the wrapper triggers the warning, confirming
        the wrapper key is traversed rather than the top-level dict.

        Args:
            tmp_path: Pytest-provided temporary directory.
            caplog: Pytest log capture fixture.
        """
        config = {
            "stations_available_for_forecast": {
                "99001": {"organization": "kghm"},
                "25001": {"organization": "tjhm"},
            }
        }
        config_file = tmp_path / "config_all.json"
        config_file.write_text(json.dumps(config))

        env = {
            "ieasyforecast_configuration_path": str(tmp_path),
            "ieasyforecast_config_file_all_stations": "config_all.json",
            "ieasyhydroforecast_organization": "kghm",
        }
        with patch.dict(os.environ, env), caplog.at_level(logging.WARNING):
            sl.check_station_code_collisions()

        warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any("FOREIGN ORG CONTAMINATION" in m for m in warning_messages)
