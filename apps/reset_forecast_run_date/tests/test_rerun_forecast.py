import pytest
import datetime
import os
from reset_forecast_run_date.rerun_forecast import load_environment, parse_last_successful_run_date, calculate_new_forecast_date, write_date

def test_parse_last_successful_run_date():
    last_run_file = "reset_forecast_run_date/tests/test_files/test_last_successful_run.txt"
    # Test with an invalid date in the file
    with open(last_run_file, "w") as file:
        file.write("invalid_date")
    with pytest.raises(ValueError):
        parse_last_successful_run_date(last_run_file)

    # Test with a valid date in the file
    with open(last_run_file, "w") as file:
        file.write("2022-01-01")
    assert parse_last_successful_run_date(last_run_file) == datetime.date(2022, 1, 1)

    # Test with a non-existent file
    assert parse_last_successful_run_date("non_existent_file") == datetime.date.today() - datetime.timedelta(days=1)

def test_load_environment():
        # No tests here
        pass

def test_calculate_new_forecast_date():
    # Test with a date where the day is less than 5
    last_successful_run_date = datetime.date(2022, 2, 4)
    expected_rerun_forecast_date = datetime.date(2022, 1, 30)
    assert calculate_new_forecast_date(last_successful_run_date) == expected_rerun_forecast_date

    # Test with a date where the day is greater than or equal to 5
    last_successful_run_date = datetime.date(2022, 2, 6)
    expected_rerun_forecast_date = datetime.date(2022, 2, 4)
    assert calculate_new_forecast_date(last_successful_run_date) == expected_rerun_forecast_date

    last_successful_run_date = datetime.date(2022, 2, 13)
    expected_rerun_forecast_date = datetime.date(2022, 2, 9)
    assert calculate_new_forecast_date(last_successful_run_date) == expected_rerun_forecast_date

    # Test with a date where the day is one of the forecast days
    last_successful_run_date = datetime.date(2022, 2, 27)
    expected_rerun_forecast_date = datetime.date(2022, 2, 24)
    assert calculate_new_forecast_date(last_successful_run_date) == expected_rerun_forecast_date

    last_successful_run_date = datetime.date(2022, 1, 31)
    expected_rerun_forecast_date = datetime.date(2022, 1, 30)
    assert calculate_new_forecast_date(last_successful_run_date) == expected_rerun_forecast_date

def test_write_date():
        # Test with valid parameters
    date = datetime.date.today()
    file_path = ("reset_forecast_run_date/tests/test_files/test_file.txt")
    write_date(date, file_path)
    with open(file_path, "r") as file:
        assert file.read() == date.strftime("%Y-%m-%d")

    # Test with invalid date
    with pytest.raises(TypeError):
        write_date("invalid_date", file_path)

    # Test with invalid file_path
    with pytest.raises(TypeError):
        write_date(date, 123)

    # Test with unwriteable file_path
    unwriteable_file_path = "/unwriteable_file_path.txt"
    with pytest.raises(IOError):
        write_date(date, unwriteable_file_path)

    # Clean up the test file
    os.remove(file_path)

