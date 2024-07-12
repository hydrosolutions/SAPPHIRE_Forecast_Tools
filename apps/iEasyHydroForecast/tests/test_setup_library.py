import os
import shutil
from iEasyHydroForecast import setup_library as sl

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