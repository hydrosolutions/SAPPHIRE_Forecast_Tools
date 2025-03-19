import os
import shutil
import unittest
import pandas as pd
from iEasyHydroForecast import setup_library as sl
from iEasyHydroForecast import tag_library as tl

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


class TestReadDailyProbabilisticMLForecastsPentad(unittest.TestCase):

    def setUp(self):
        # Setup runs before each test
        self.file_path = "iEasyHydroForecast/tests/test_data/test_probabil_forecast.csv"

        # Read validation data
        self.val_data = pd.read_csv(self.file_path)
        self.val_data.loc[:, "pentad_in_year"] = self.val_data["date"].apply(tl.get_pentad_in_year)

        # Process using the function to test
        self.test_data = sl.read_daily_probabilistic_ml_forecasts_pentad(
            self.file_path, 'test', 't', 'test')

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
        # Dates are in the format YYYY-MM-DD and are the last day of the previous pentad
        dates = ['2010-12-31', '2010-04-05', '2010-10-25', '2010-07-10']

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
                if code == 16161 and date == '2010-07-10':
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

        # Process using the function to test
        self.test_data = sl.read_daily_probabilistic_ml_forecasts_decade(
            self.file_path, 'test', 't', 'test')

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
        # Dates are in the format YYYY-MM-DD and are the last day of the previous decade
        dates = ['2010-12-31', '2010-04-10', '2010-10-20', '2010-07-10']

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
                if code == 16161 and date == '2010-07-10':
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

