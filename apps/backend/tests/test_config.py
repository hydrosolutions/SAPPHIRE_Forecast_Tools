import os
import datetime as dt
import pytest
import json
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