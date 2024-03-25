import os
import pandas as pd
import numpy as np
import shutil
import datetime as dt
import subprocess
from openpyxl import load_workbook
import time
from backend.src import config, data_processing, forecasting
from ieasyhydro_sdk.sdk import IEasyHydroSDK

# Tests if the backend produces the expected output
def test_overall_output_step_by_step():
    # Set up the test environment
    # Temporary directory to store output
    tmpdir = "backend/tests/test_files/temp"
    # Clean up the folder in case it already exists (for example because a previous test failed)
    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)
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

    # Define start_date
    start_date = dt.datetime(2022, 5, 5)

    # Set forecast_flags
    forecast_flags = config.ForecastFlags(pentad=True)

    # Get bulletin date
    bulletin_date = config.get_bulletin_date(start_date)
    assert bulletin_date == "2022-05-06", "The bulletin date is not as expected"

    ieh_sdk = IEasyHydroSDK()  # ieasyhydro
    backend_has_access_to_db = data_processing.check_database_access(ieh_sdk)
    assert backend_has_access_to_db == False, "The backend unexpectedly does have access to the database"

    # - identify sites for which to produce forecasts
    #   reading sites from DB and config files
    db_sites = data_processing.get_db_sites(ieh_sdk, backend_has_access_to_db)
    #   writing sites information to as list of Site objects
    fc_sites = data_processing.get_fc_sites(ieh_sdk, backend_has_access_to_db, db_sites)
    fc_sites2 = data_processing.get_fc_sites(ieh_sdk, backend_has_access_to_db, db_sites)
    assert len(fc_sites) == 2, "The number of sites is not as expected"
    assert fc_sites[0].code == "12176", "The first site code is not as expected"
    assert fc_sites[1].code == "12256", "The second site code is not as expected"
    # The predictors should be -10000.0 for both sites as no predictor should be
    # assigned at this point
    assert fc_sites[0].predictor == -10000.0, "The first predictor is not as expected"
    assert fc_sites[1].predictor == -10000.0, "The second predictor is not as expected"

    # - identify dates for which to aggregate predictor data
    predictor_dates = data_processing.get_predictor_dates(start_date, forecast_flags)
    assert predictor_dates.pentad == [dt.date(2022, 5, 4), dt.date(2022, 5, 3), dt.date(2022, 5, 2)], "The predictor date is not as expected"

    # Read discharge data from excel and iEasyHydro database
    modified_data = data_processing.get_station_data(ieh_sdk, backend_has_access_to_db, start_date, fc_sites)
    # Test that the first date in the dataframe is 2000-01-01
    assert modified_data['Date'][0].strftime('%Y-%m-%d') == dt.datetime(2000, 1, 1).strftime('%Y-%m-%d'), "The first date in the dataframe is not as expected"
    # The last date should be 2022-05-05
    assert modified_data['Date'].iloc[-1].strftime('%Y-%m-%d') == dt.datetime(2022, 5, 5).strftime('%Y-%m-%d'), "The last date in the dataframe is not as expected"
    # The first code should be 12176
    assert modified_data['Code'][0] == '12176', "The first code in the dataframe is not as expected"
    # The last code should be 12256
    assert modified_data['Code'].iloc[-1] == '12256', "The last code in the dataframe is not as expected"
    # The last value in discharge sum should be 2.43
    expected_predictor = 2.43
    assert round(modified_data['discharge_sum'].iloc[-1], 2) == expected_predictor, "The last value in discharge sum is not as expected"

    forecast_pentad_of_year = data_processing.get_forecast_pentad_of_year(bulletin_date)
    data_processing.save_discharge_avg(modified_data, fc_sites, forecast_pentad_of_year)

    # Get the predictor into the site object
    forecasting.get_predictor(modified_data, start_date, fc_sites, ieh_sdk, backend_has_access_to_db, predictor_dates.pentad)
    # The first predictor should be nan
    assert pd.isna(fc_sites[0].predictor), "The first predictor is not as expected"
    # The second predictor should be expected_predictor
    assert fc_sites[1].predictor == expected_predictor, "The second predictor is not as expected"

    # Test that the perdictors are -10000.0 for both sites
    assert fc_sites2[0].predictor == -10000.0, "The first predictor is not as expected"
    assert fc_sites2[1].predictor == -10000.0, "The second predictor is not as expected"

    result_df = forecasting.perform_linear_regression(modified_data, forecast_pentad_of_year)

    # Get the predictors based on result_df. There should not be a difference to
    # the predictors gained from modified_data.
    forecasting.get_predictor(result_df, start_date, fc_sites2, ieh_sdk, backend_has_access_to_db, predictor_dates.pentad)
    # Both predictors produced using results_df should be nan
    assert pd.isna(fc_sites2[0].predictor), "The first predictor is not as expected"
    assert pd.isna(fc_sites2[1].predictor), "The second predictor is not as expected"

    # Clean up the environment
    shutil.rmtree(tmpdir)
    os.environ.pop("IEASYHYDRO_HOST")
    os.environ.pop("IEASYHYDRO_USERNAME")
    os.environ.pop("IEASYHYDRO_PASSWORD")
    os.environ.pop("ieasyforecast_configuration_path")
    os.environ.pop("ieasyforecast_config_file_all_stations")
    os.environ.pop("ieasyforecast_config_file_station_selection")
    os.environ.pop("ieasyforecast_config_file_output")
    os.environ.pop("ieasyforecast_intermediate_data_path")
    os.environ.pop("ieasyforecast_hydrograph_day_file")
    os.environ.pop("ieasyforecast_hydrograph_pentad_file")
    os.environ.pop("ieasyforecast_results_file")
    os.environ.pop("ieasyreports_templates_directory_path")
    os.environ.pop("ieasyforecast_template_pentad_bulletin_file")
    os.environ.pop("ieasyforecast_template_pentad_sheet_file")
    os.environ.pop("ieasyreports_report_output_path")
    os.environ.pop("ieasyforecast_bulletin_file_name")
    os.environ.pop("ieasyforecast_sheet_file_name")
    os.environ.pop("ieasyforecast_gis_directory_path")
    os.environ.pop("ieasyforecast_country_borders_file_name")
    os.environ.pop("ieasyforecast_daily_discharge_path")
    os.environ.pop("ieasyforecast_locale_dir")
    os.environ.pop("log_file")
    os.environ.pop("log_level")
    os.environ.pop("ieasyforecast_restrict_stations_file")
    os.environ.pop("ieasyforecast_last_successful_run_file")


def test_overall_output():
    # Set up the test environment
    # Temporary directory to store output
    tmpdir = "backend/tests/test_files/temp"
    # Clean up the folder in case it already exists (for example because a previous test failed)
    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)
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
    # Print the differences between the two files. Drop rows where both files
    # are equal.
    temp = (expected_hydrograph_df[expected_hydrograph_df != hydrograph_df])
    temp = temp.dropna(how='all')
    temp2 = (hydrograph_df[hydrograph_df != expected_hydrograph_df])
    temp2 = temp2.dropna(how='all')
    #print(temp)
    #print(temp2)
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
    temp = (expected_hydrograph_day_df[expected_hydrograph_day_df != hydrograph_day_df])
    temp = temp.dropna(how='all')
    temp2 = (hydrograph_day_df[hydrograph_day_df != expected_hydrograph_day_df])
    temp2 = temp2.dropna(how='all')
    #print(temp)
    #print(temp2)
    assert expected_hydrograph_day_df.equals(hydrograph_day_df), "The hydrograph_day file is not as expected"

    # Test if a bulletin file is generated
    bulletin_file = os.path.join(
        os.getenv("ieasyreports_report_output_path"),
        "bulletins/pentad/2022/05_Май/2022_05_Май_2_pentadal_forecast_bulletin.xlsx"
    )
    assert os.path.exists(bulletin_file), "The bulletin file was not created"

    # Print the current working directory
    print(os.getcwd())
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

    # Now test if the content of the files is consistent.
    # The average pentadal discharge in hydrograph_pentad.csv (read to
    # hydrograph_df) should be consistent with the data in the traditional excel
    # sheets (read to bulletin_df_0 and bulletin_df_1).

    # Replace all "," with "."
    hydrograph_df = hydrograph_df.replace(",", ".", regex=True)
    # Convert all remaining columns to floats
    hydrograph_df = hydrograph_df.astype(float)
    # Convert the columns 'Code' and 'pentad' to integer
    hydrograph_df['Code'] = hydrograph_df['Code'].astype(int)
    hydrograph_df['pentad'] = hydrograph_df['pentad'].astype(int)
    # Derive month and pentad of month from the pentad of the year.
    hydrograph_df['month'] = hydrograph_df['pentad'].apply(lambda x: (x - 1) // 6 + 1).astype(int)
    hydrograph_df['pentad_of_month'] = hydrograph_df['pentad'].apply(lambda x: (x - 1) % 6 + 1).astype(int)

    # from bulletin_df_0 get rows 2 to 6 and columns 0 to 2 into a new data
    # frame. The names of the columns are 'year', 'av', 'predictor'
    bulletin_df_0 = bulletin_df_0.iloc[2:7, 0:3]
    bulletin_df_0.columns = ['year', 'av', 'predictor']
    # Replace all "," with "." in the columns 'av' and 'predictor'
    bulletin_df_0['av'] = bulletin_df_0['av'].replace(",", ".", regex=True)
    bulletin_df_0['predictor'] = bulletin_df_0['predictor'].replace(",", ".", regex=True)
    # Convert column 'year' to integer and columns 'av' and 'predictor' to float
    bulletin_df_0['year'] = bulletin_df_0['year'].astype(int)
    bulletin_df_0['av'] = bulletin_df_0['av'].astype(float)
    bulletin_df_0['predictor'] = bulletin_df_0['predictor'].astype(float)
    # Add columns 'month' and 'pentad_of_month' to bulletin_df_0
    bulletin_df_0['Code'] = 12176
    bulletin_df_0['month'] = 5
    bulletin_df_0['pentad_of_month'] = 2

    # Do the same for bulletin_df_1
    bulletin_df_1 = bulletin_df_1.iloc[2:7, 0:3]
    bulletin_df_1.columns = ['year', 'av', 'predictor']
    bulletin_df_1['av'] = bulletin_df_1['av'].replace(",", ".", regex=True)
    bulletin_df_1['predictor'] = bulletin_df_1['predictor'].replace(",", ".", regex=True)
    bulletin_df_1['year'] = bulletin_df_1['year'].astype(int)
    bulletin_df_1['av'] = bulletin_df_1['av'].astype(float)
    bulletin_df_1['predictor'] = bulletin_df_1['predictor'].astype(float)
    bulletin_df_1['Code'] = 12256
    bulletin_df_1['month'] = 5
    bulletin_df_1['pentad_of_month'] = 2

    # Concatenate bulletin_df_0 and bulletin_df_1
    bulletin_df = pd.concat([bulletin_df_0, bulletin_df_1])
    # Reformat hydrograph_df, columns 2000 to 2022, into long format
    hydrograph_df = hydrograph_df.melt(id_vars=['Code', 'pentad', 'month', 'pentad_of_month'], var_name='year', value_name='av_hydrograph')
    # Convert the column 'year' to integer
    hydrograph_df['year'] = hydrograph_df['year'].astype(int)

    # Merge hydrograph_df and bulletin_df
    comparison = hydrograph_df.merge(bulletin_df, on=['Code', 'year', 'month', 'pentad_of_month'])

    print_comparison = comparison[['Code', 'year', 'month', 'pentad_of_month', 'av_hydrograph', 'av']]
    # Round the columns 'av_hydrograph' and 'av' to 2 decimal places
    print_comparison.loc[:, 'av_hydrograph'] = round(print_comparison.loc[:, 'av_hydrograph'], 2)
    print_comparison.loc[:, 'av'] = round(print_comparison.loc[:, 'av'], 2)

    # Test if av_hydrograph, rounded to 2 decimal places, is equal to av,
    # rounded to 2 decimal places
    assert (print_comparison['av_hydrograph'].dropna() == print_comparison['av'].dropna()).all(), "The hydrograph data is not consistent with the traditional bulletins"
    # Test if NaNs are at the same location in both columns
    assert print_comparison['av_hydrograph'].isna().equals(print_comparison['av'].isna()), "The hydrograph data is not consistent with the traditional bulletins"

    # We also want to test if the predictors in the traditional bulletins are
    # consistent with the predictors in the forecast file (expected_df) file.

    # Get the columns date, code, and predictor from the expected_df
    expected_df = expected_df[['date', 'code', 'predictor']]
    # Get the rows for year == 2022, month == 5 and pentad_of_month == 2 from
    # comparison and get the columns 'year', 'month', 'pentad_of_month', 'Code',
    # 'predictor' into a new data frame.
    comparison_predictor = comparison[(comparison['year'] == 2022) & (comparison['month'] == 5) & (comparison['pentad_of_month'] == 2)]
    comparison_predictor = comparison_predictor[['year', 'month', 'pentad_of_month', 'Code', 'predictor']]

    # Assert that for each code, the predictor value is the same in both data frames.
    # For station 12176, the predictor should be NaN and for station 12256, the
    # predictor should be a float.
    print(forecast_df)
    assert (np.isnan(comparison_predictor['predictor'].iloc[0]) == np.isnan(forecast_df.loc[forecast_df['code'] == 12176]['predictor'].iloc[0])), "The predictor for code 12176 is not as expected"
    assert (comparison_predictor['predictor'].iloc[1] == forecast_df.loc[forecast_df['code'] == 12256]['predictor'].iloc[0]), "The predictor for code 12256 is not as expected"

    # Let's now read in the 4th sheet from each of the traditional bulletins and
    # compare slope, intercept and forecasted value with forecast_df
    # This can not be implemented as apparently excel documents which are written
    # by openpyxl do not actually execute the formulas in the cells unless the excel
    # document is opened and saved in Excel. This is a known issue with openpyxl.
    # We will therefore skip this test.

    # Manual comparison shows that the values in the traditional bulletins are
    # not consistent with the values in the forecast files.


    # Delete tmpdir
    shutil.rmtree(tmpdir)

    os.environ.pop("IEASYHYDRO_HOST")
    os.environ.pop("IEASYHYDRO_USERNAME")
    os.environ.pop("IEASYHYDRO_PASSWORD")
    os.environ.pop("ieasyforecast_configuration_path")
    os.environ.pop("ieasyforecast_config_file_all_stations")
    os.environ.pop("ieasyforecast_config_file_station_selection")
    os.environ.pop("ieasyforecast_config_file_output")
    os.environ.pop("ieasyforecast_intermediate_data_path")
    os.environ.pop("ieasyforecast_hydrograph_day_file")
    os.environ.pop("ieasyforecast_hydrograph_pentad_file")
    os.environ.pop("ieasyforecast_results_file")
    os.environ.pop("ieasyreports_templates_directory_path")
    os.environ.pop("ieasyforecast_template_pentad_bulletin_file")
    os.environ.pop("ieasyforecast_template_pentad_sheet_file")
    os.environ.pop("ieasyreports_report_output_path")
    os.environ.pop("ieasyforecast_bulletin_file_name")
    os.environ.pop("ieasyforecast_sheet_file_name")
    os.environ.pop("ieasyforecast_gis_directory_path")
    os.environ.pop("ieasyforecast_country_borders_file_name")
    os.environ.pop("ieasyforecast_daily_discharge_path")
    os.environ.pop("ieasyforecast_locale_dir")
    os.environ.pop("log_file")
    os.environ.pop("log_level")
    os.environ.pop("ieasyforecast_restrict_stations_file")
    os.environ.pop("ieasyforecast_last_successful_run_file")

