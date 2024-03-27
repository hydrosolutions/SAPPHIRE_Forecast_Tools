import os
import datetime as dt
import pytest
import json
import shutil
from backend.src import config

def test_store_last_successful_run_date(tmpdir):
    # Set up the environment variables
    os.environ["ieasyforecast_intermediate_data_path"] = str(tmpdir)
    os.environ["ieasyforecast_last_successful_run_file"] = "last_run.txt"

    try:
        # Call the function with a specific date
        date = dt.date(2022, 1, 1)
        config.store_last_successful_run_date(date)

        # Check that the function wrote the correct date to the file
        last_run_file = os.path.join(
            os.getenv("ieasyforecast_intermediate_data_path"),
            os.getenv("ieasyforecast_last_successful_run_file")
        )
        with open(last_run_file, "r") as f1:
            assert f1.read() == "2022-01-01"
    finally:
        # Remove the environment variables
        os.environ.pop("ieasyforecast_intermediate_data_path")
        os.environ.pop("ieasyforecast_last_successful_run_file")

def test_store_wrong_date(tmpdir):
    # Set up the environment variables
    os.environ["ieasyforecast_intermediate_data_path"] = str(tmpdir)
    os.environ["ieasyforecast_last_successful_run_file"] = "last_run.txt"

    try:
        # Check that the function wrote the correct date to the file
        last_run_file = os.path.join(
            os.getenv("ieasyforecast_intermediate_data_path"),
            os.getenv("ieasyforecast_last_successful_run_file")
        )

        # Should throw ValueError
        with pytest.raises(ValueError):
            with open(last_run_file, "w") as f1:
                config.store_last_successful_run_date("2022-13-33")

    finally:
        # Remove the environment variables
        os.environ.pop("ieasyforecast_intermediate_data_path")
        os.environ.pop("ieasyforecast_last_successful_run_file")

def test_load_dotenv():
    # Temporary directory to store output
    tmpdir = "backend/tests/test_files/temp"
    # Create the directory
    os.makedirs(tmpdir, exist_ok=True)
    # When in a test environment, the .env file should load the following
    # environment variables:
    test_env_path = "backend/tests/test_files/.env_develop_test"
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
    res = config.load_environment()
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

def test_get_bulletin_date_with_valid_date():
    # Call the function with a specific date
    start_date = dt.datetime(2022, 1, 1)
    bulletin_date = config.get_bulletin_date(start_date)

    # Check that the function returned the correct date
    assert bulletin_date == "2022-01-02"

def test_get_bulletin_date_with_invalid_date():
    # Check that the function raises a TypeError
    with pytest.raises(TypeError):
        config.get_bulletin_date("2022-13-01")
    with pytest.raises(TypeError):
        config.get_bulletin_date(20221301)
    with pytest.raises(TypeError):
        config.get_bulletin_date("2022-12-34")


def test_excel_output_with_write_excel_true(tmpdir):
    # Set up the environment variables and configuration file
    os.environ["ieasyforecast_configuration_path"] = str(tmpdir)
    os.environ["ieasyforecast_config_file_output"] = "config.json"
    config_output_file = os.path.join(
        os.getenv("ieasyforecast_configuration_path"),
        os.getenv("ieasyforecast_config_file_output"),
    )
    with open(config_output_file, "w") as json_file:
        json.dump({"write_excel": True}, json_file)

    # Call the function and check the result
    assert config.excel_output() is True

    # Clean up the environment
    os.environ.pop("ieasyforecast_configuration_path")
    os.environ.pop("ieasyforecast_config_file_output")

def test_excel_output_with_write_excel_false(tmpdir):
    # Set up the environment variables and configuration file
    os.environ["ieasyforecast_configuration_path"] = str(tmpdir)
    os.environ["ieasyforecast_config_file_output"] = "config.json"
    config_output_file = os.path.join(
        os.getenv("ieasyforecast_configuration_path"),
        os.getenv("ieasyforecast_config_file_output"),
    )
    with open(config_output_file, "w") as json_file:
        json.dump({"write_excel": False}, json_file)

    # Call the function and check the result
    assert config.excel_output() is False

    # Clean up the environment
    os.environ.pop("ieasyforecast_configuration_path")
    os.environ.pop("ieasyforecast_config_file_output")

def test_excel_output_with_invalid_write_excel_value(tmpdir):
    # Set up the environment variables and configuration file
    os.environ["ieasyforecast_configuration_path"] = str(tmpdir)
    os.environ["ieasyforecast_config_file_output"] = "config.json"
    config_output_file = os.path.join(
        os.getenv("ieasyforecast_configuration_path"),
        os.getenv("ieasyforecast_config_file_output"),
    )
    with open(config_output_file, "w") as json_file:
        json.dump({"write_excel": "invalid"}, json_file)

    # Call the function and check that it raises a ValueError
    with pytest.raises(ValueError):
        config.excel_output()

    # Clean up the environment
    os.environ.pop("ieasyforecast_configuration_path")
    os.environ.pop("ieasyforecast_config_file_output")

def test_excel_output_with_missing_write_excel_key(tmpdir):
    # Set up the environment variables and configuration file
    os.environ["ieasyforecast_configuration_path"] = str(tmpdir)
    os.environ["ieasyforecast_config_file_output"] = "config.json"
    config_output_file = os.path.join(
        os.getenv("ieasyforecast_configuration_path"),
        os.getenv("ieasyforecast_config_file_output"),
    )
    with open(config_output_file, "w") as json_file:
        json.dump({}, json_file)

    # Call the function and check that it raises a KeyError
    with pytest.raises(KeyError):
        config.excel_output()

    # Clean up the environment
    os.environ.pop("ieasyforecast_configuration_path")
    os.environ.pop("ieasyforecast_config_file_output")