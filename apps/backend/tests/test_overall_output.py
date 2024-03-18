import os
import pandas as pd
import shutil
import datetime as dt
import subprocess
import time
from backend.src import config

# Tests if the backend produces the expected output
def test_overall_output():
    # Set up the test environment
    # Temporary directory to store output
    tmpdir = "backend/tests/test_files/temp"
    # Create the directory
    os.makedirs(tmpdir, exist_ok=True)
    # Load the environment
    config.load_environment()
    # Create the intermediate data directory
    os.makedirs(
        os.getenv("ieasyforecast_intermediate_data_path"),
        exist_ok=True)
    # Copy the input directories to tmpdir
    temp_ieasyforecast_configuration_path = os.path.join(tmpdir, "apps/config")
    temp_ieasyreports_templates_directory_path = os.path.join(tmpdir, "data/templates")
    temp_ieasyreports_report_output_path = os.path.join(tmpdir, "data/reports")
    temp_ieasyforecast_gis_directory_path = os.path.join(tmpdir, "data/GIS")
    temp_ieasyforecast_daily_discharge_path = os.path.join(tmpdir, "data/daily_runoff")
    temp_ieasyforecast_locale_dir = os.path.join(tmpdir, "apps/config/locale")
    temp_log_file = os.path.join(tmpdir, "backend.log")

    shutil.copytree("config", temp_ieasyforecast_configuration_path)
    shutil.copytree("../data", os.path.join(tmpdir, "data"))

    # Update the environment variables to point to the temporary directory
    os.environ["ieasyforecast_configuration_path"] = temp_ieasyforecast_configuration_path
    os.environ["ieasyreports_template_directory_path"] = temp_ieasyreports_templates_directory_path
    os.environ["ieasyreports_report_output_path"] = temp_ieasyreports_report_output_path
    os.environ["ieasyforecast_gis_directory_path"] = temp_ieasyforecast_gis_directory_path
    os.environ["ieasyforecast_daily_discharge_path"] = temp_ieasyforecast_daily_discharge_path
    os.environ["ieasyforecast_locale_dir"] = temp_ieasyforecast_locale_dir
    os.environ["log_file"] = temp_log_file

    # Write the date of May 1, 2022 to the file last_successful_run.txt in intermediate data.
    # The input date must be in the format "YYYY-MM-DD"
    config.store_last_successful_run_date(dt.date(2022, 5, 1))

    # Get the filepath of the last_successful_run.txt file
    last_run_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_last_successful_run_file")
    )

    # Run the forecast as a subprocess
    process = subprocess.Popen(["python", "backend/run_offline_mode.py"])

    # Wait for up to 10 seconds for the file to change
    try:
        # Monitor the output file
        last_mod_time = os.path.getmtime(last_run_file)
        change_count = 0
        print("TEST : Last modification time: ", last_mod_time, " Change count: ", change_count)

        # Wait for the file to change 3 times (this will run the forecast for
        # the second pentad of May, 2022)
        while change_count < 4:
            time.sleep(0.3)
            new_mod_time = os.path.getmtime(last_run_file)
            print("TEST : New modification time: ", new_mod_time, "Change count: ", change_count)
            if new_mod_time != last_mod_time:
                last_mod_time = new_mod_time
                change_count += 1
                print("TEST :     Change count: ", change_count)

    finally:
        # Terminate the subprocess
        process.terminate()

    assert change_count == 4, "The output file did not change 4 times"

    # Test that we now have a file forecasts_pentad.csv in internal data
    forecast_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_results_file")
    )
    assert os.path.exists(forecast_file), "The forecast file was not created"

    # Test if the file is the same as the expected file
    # Read the expected file
    expected_file = "backend/tests/test_files/test_one_step_forecasts_pentad.csv"
    expected_df = pd.read_csv(expected_file)
    # Read the forecast file
    forecast_df = pd.read_csv(forecast_file)
    # Compare the two files
    assert expected_df.equals(forecast_df), "The forecast file is not as expected"

    # Also check the two hydrograph files
    # Test that we now have a file hydrograph_pentad.csv in internal data
    hydrograph_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_hydrograph_pentad_file")
    )
    assert os.path.exists(hydrograph_file), "The hydrograph file was not created"
    expected_hydrograph_file = "backend/tests/test_files/test_one_step_hydrograph_pentad.csv"
    assert os.path.exists(expected_hydrograph_file), "The expected hydrograph file does not exist"
    # Read the expected file
    expected_hydrograph_df = pd.read_csv(expected_hydrograph_file)
    # Read the forecast file
    hydrograph_df = pd.read_csv(hydrograph_file)
    # Compare the two files
    assert expected_hydrograph_df.equals(hydrograph_df), "The hydrograph file is not as expected"

    # And the same for the hydrograph_day.csv file
    # Test that we now have a file hydrograph_day.csv in internal data
    hydrograph_day_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_hydrograph_day_file")
    )
    assert os.path.exists(hydrograph_day_file), "The hydrograph_day file was not created"
    expected_hydrograph_day_file = "backend/tests/test_files/test_one_step_hydrograph_day.csv"
    assert os.path.exists(expected_hydrograph_day_file), "The expected hydrograph_day file does not exist"
    # Read the expected file
    expected_hydrograph_day_df = pd.read_csv(expected_hydrograph_day_file)
    # Read the forecast file
    hydrograph_day_df = pd.read_csv(hydrograph_day_file)
    # Compare the two files
    assert expected_hydrograph_day_df.equals(hydrograph_day_df), "The hydrograph_day file is not as expected"

    # Test if a bulletin file is generated
    bulletin_file = os.path.join(
        os.getenv("ieasyreports_report_output_path"),
        "bulletins/pentad/2022/05_Май/2022_05_Май_2_pentadal_forecast_bulletin.xlsx"
    )
    assert os.path.exists(bulletin_file), "The bulletin file was not created"

    # Test if the file is the same as the expected file
    expected_bulletin_file = "backend/tests/test_files/test_one_step_2022_05_Май_2_pentadal_forecast_bulletin.xlsx"
    assert os.path.exists(expected_bulletin_file), "The expected bulletin file does not exist"
    # Read the expected file
    expected_bulletin_df = pd.read_excel(expected_bulletin_file)
    # Read the forecast file
    bulletin_df = pd.read_excel(bulletin_file)
    # Compare the two files
    assert expected_bulletin_df.equals(bulletin_df), "The bulletin file is not as expected"

    # Same for the two traditional bulletins
    # Test if a bulletin file is generated
    bulletin_file_0 = os.path.join(
        os.getenv("ieasyreports_report_output_path"),
        "forecast_sheets/pentad/2022/05_Май/12176/2022_05_Май_2-12176-pentadal_forecast_bulletin.xlsx"
    )
    assert os.path.exists(bulletin_file_0), "The bulletin file was not created"
    bulletin_file_1 = os.path.join(
        os.getenv("ieasyreports_report_output_path"),
        "forecast_sheets/pentad/2022/05_Май/12256/2022_05_Май_2-12256-pentadal_forecast_bulletin.xlsx"
    )
    assert os.path.exists(bulletin_file_1), "The bulletin file was not created"

    # Test if the file is the same as the expected file
    expected_bulletin_file_0 = "backend/tests/test_files/test_one_step_2022_05_Май_2-12176-pentadal_forecast_bulletin.xlsx"
    assert os.path.exists(expected_bulletin_file_0), "The expected bulletin file does not exist"
    # Read the expected file
    expected_bulletin_df_0 = pd.read_excel(expected_bulletin_file_0)
    # Read the forecast file
    bulletin_df_0 = pd.read_excel(bulletin_file_0)
    # Compare the two files
    assert expected_bulletin_df_0.equals(bulletin_df_0), "The bulletin file is not as expected"

    # Test if the file is the same as the expected file
    expected_bulletin_file_1 = "backend/tests/test_files/test_one_step_2022_05_Май_2-12256-pentadal_forecast_bulletin.xlsx"
    assert os.path.exists(expected_bulletin_file_1), "The expected bulletin file does not exist"
    # Read the expected file
    expected_bulletin_df_1 = pd.read_excel(expected_bulletin_file_1)
    # Read the forecast file
    bulletin_df_1 = pd.read_excel(bulletin_file_1)
    # Compare the two files
    assert expected_bulletin_df_1.equals(bulletin_df_1), "The bulletin file is not as expected"

    # Delete tmpdir
    shutil.rmtree(tmpdir)

