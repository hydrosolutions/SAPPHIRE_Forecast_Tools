import os
import pytest
import datetime as dt
import pandas as pd
import time
from unittest.mock import patch
from forecast_dashboard.src import dashboard_methods as dm




def test_load_configuration_sapphire_test_env():
    def clean_up_env_vars():
        # Clean up all environment variables set in the test
        os.environ.pop("SAPPHIRE_TEST_ENV")
        os.environ.pop("ieasyforecast_daily_discharge_path")
        os.environ.pop("ieasyforecast_results_file")
        os.environ.pop("ieasyforecast_last_successful_run_file")
        os.environ.pop("ieasyforecast_template_pentad_bulletin_file")
        os.environ.pop("ieasyforecast_template_pentad_sheet_file")
        os.environ.pop("ieasyforecast_bulletin_file_name")
        os.environ.pop("ieasyforecast_sheet_file_name")
        os.environ.pop("ieasyforecast_gis_directory_path")
        os.environ.pop("ieasyforecast_country_borders_file_name")
        os.environ.pop("ieasyforecast_locale_dir")
        os.environ.pop("ieasyforecast_locale")
        os.environ.pop("log_file")
        os.environ.pop("log_level")
        os.environ.pop("ieasyforecast_restrict_stations_file")
        os.environ.pop("ieasyforecast_configuration_path")
        os.environ.pop("ieasyforecast_config_file_all_stations")
        os.environ.pop("ieasyforecast_config_file_station_selection")
        os.environ.pop("ieasyforecast_config_file_output")
        os.environ.pop("ieasyforecast_intermediate_data_path")
        os.environ.pop("ieasyreports_templates_directory_path")
        os.environ.pop("ieasyreports_report_output_path")
        os.environ.pop("ieasyforecast_intermediate_data_path")
        os.environ.pop("ieasyforecast_hydrograph_day_file")
        os.environ.pop("ieasyforecast_hydrograph_pentad_file")
        os.environ.pop("ieasyforecast_results_file")

    with patch.dict(os.environ, {"SAPPHIRE_TEST_ENV": "True"}):
        assert dm.load_configuration() == 'None'

    # Read a environment variable
    expected_var = '../data/daily_runoff'
    with patch.dict(os.environ, {"SAPPHIRE_TEST_ENV": "True"}):
        dm.load_configuration()
        assert os.getenv("ieasyforecast_daily_discharge_path") == expected_var
        print(os.getenv("ieasyforecast_daily_discharge_path"))

    #clean_up_env_vars()

def test_read_hydrograph_day_file():
    # Set environment variables
    os.environ["ieasyforecast_intermediate_data_path"] = "backend/tests/test_files"
    os.environ["ieasyforecast_hydrograph_day_file"] = "test_one_step_hydrograph_day.csv"

    #ieasyforecast_hydrograph_day_file=hydrograph_day.csv
    #ieasyforecast_hydrograph_pentad_file=hydrograph_pentad.csv
    #ieasyforecast_results_file=forecasts_pentad.csv
    today = dt.datetime.strptime('2021-01-01', '%Y-%m-%d')
    read_data = dm.read_hydrograph_day_file(today)

    # Read the expected data from the path concatenated with the file name
    expected_data = pd.read_csv("backend/tests/test_files/test_one_step_hydrograph_day.csv")

    # If we are not in a leap year, we remove the leap year data
    if today.year % 4 != 0:
        expected_data = expected_data[expected_data["day_of_year"] != 366]

    # Compare the data
    # Do we have the same column names
    assert read_data.columns.equals(expected_data.columns)
    # Do we have the same number of rows
    assert len(read_data) == len(expected_data)
    # Do we have the same data, ignoring NaN values
    assert read_data["Code"].equals(expected_data["Code"].astype(str))
    assert read_data["day_of_year"].equals(expected_data["day_of_year"].astype(int))
    assert read_data["2000"].equals(expected_data["2000"])
    assert read_data["2001"].equals(expected_data["2001"])
    assert read_data["2002"].equals(expected_data["2002"])
    assert read_data["2003"].equals(expected_data["2003"])
    assert read_data["2004"].equals(expected_data["2004"])
    assert read_data["2022"].equals(expected_data["2022"])

    # Clean up the environment variables
    os.environ.pop("ieasyforecast_intermediate_data_path")
    os.environ.pop("ieasyforecast_hydrograph_day_file")

def test_preprocess_hydrograph_day_data():
    # Set environment variables
    os.environ["ieasyforecast_intermediate_data_path"] = "backend/tests/test_files"
    os.environ["ieasyforecast_hydrograph_day_file"] = "test_one_step_hydrograph_day.csv"
    os.environ["ieasyforecast_configuration_path"] = "config"
    os.environ["ieasyforecast_config_file_all_stations"] = "config_all_stations_library.json"
    today = dt.datetime.strptime('2023-04-01', '%Y-%m-%d')
    hydrograph_day_all = dm.read_hydrograph_day_file(today)
    station_list = hydrograph_day_all['Code'].unique().tolist()
    station_list, all_stations, station_df = dm.read_stations_from_file(station_list)
    hydrograph_day_all = dm.add_labels_to_hydrograph_day_all(hydrograph_day_all, all_stations)

    # Test if preprocess_hydrograph_day_data throws an error if the station is not
    # unique in the station list
    with pytest.raises(ValueError):
        dm.preprocess_hydrograph_day_data(hydrograph_day_all, today)

    # Filter for stations that are in the station list
    hydrograph_day_12176 = hydrograph_day_all[hydrograph_day_all['Code'] == '12176']
    hydrograph_day_12256 = hydrograph_day_all[hydrograph_day_all['Code'] == '12256']

    # Drop index of the dataframes
    hydrograph_day_12176.reset_index(drop=True, inplace=True)
    hydrograph_day_12256.reset_index(drop=True, inplace=True)

    preprocessed_data_12176 = dm.preprocess_hydrograph_day_data(hydrograph_day_12176, today)
    preprocessed_data_12256 = dm.preprocess_hydrograph_day_data(hydrograph_day_12256, today)

    assert preprocessed_data_12176['day_of_year'].equals(hydrograph_day_12176['day_of_year'])
    assert min(preprocessed_data_12176["Min"]) == min(hydrograph_day_12176[['2000', '2001', '2002', '2003', '2004', '2022']].min())
    assert max(preprocessed_data_12176["Max"]) == max(hydrograph_day_12176[['2000', '2001', '2002', '2003', '2004', '2022']].max())

    assert preprocessed_data_12256['day_of_year'].equals(hydrograph_day_12256['day_of_year'])
    assert min(preprocessed_data_12256["Min"]) == min(hydrograph_day_12256[['2000', '2001', '2002', '2003', '2004', '2022']].min())
    assert max(preprocessed_data_12256["Max"]) == max(hydrograph_day_12256[['2000', '2001', '2002', '2003', '2004', '2022']].max())

    # Clean up the environment variables
    os.environ.pop("ieasyforecast_intermediate_data_path")
    os.environ.pop("ieasyforecast_hydrograph_day_file")
    os.environ.pop("ieasyforecast_configuration_path")
    os.environ.pop("ieasyforecast_config_file_all_stations")


def test_preprocess_hydrograph_pentad_data():
    # Set environment variables
    os.environ["ieasyforecast_intermediate_data_path"] = "backend/tests/test_files"
    os.environ["ieasyforecast_hydrograph_pentad_file"] = "test_one_step_hydrograph_pentad.csv"
    os.environ["ieasyforecast_configuration_path"] = "config"
    os.environ["ieasyforecast_config_file_all_stations"] = "config_all_stations_library.json"
    today = dt.datetime.strptime('2023-04-01', '%Y-%m-%d')
    hydrograph_pentad_all = dm.read_hydrograph_pentad_file()
    station_list = hydrograph_pentad_all['Code'].unique().tolist()
    station_list, all_stations, station_df = dm.read_stations_from_file(station_list)
    hydrograph_pentad_all = dm.add_labels_to_hydrograph_pentad_all(hydrograph_pentad_all, all_stations)

    print("DEBUG test_preprocess_hydrograph_pentad_data: \n", hydrograph_pentad_all)

    # Test if preprocess_hydrograph_day_data throws an error if the station is not
    # unique in the station list
    with pytest.raises(ValueError):
        dm.preprocess_hydrograph_day_data(hydrograph_pentad_all, today)

    # Filter for stations that are in the station list
    hydrograph_pentad_12176 = hydrograph_pentad_all[hydrograph_pentad_all['Code'] == '12176']
    hydrograph_pentad_12256 = hydrograph_pentad_all[hydrograph_pentad_all['Code'] == '12256']

    # Drop index of the dataframes
    hydrograph_pentad_12176.reset_index(drop=True, inplace=True)
    hydrograph_pentad_12256.reset_index(drop=True, inplace=True)

    preprocessed_data_12176 = dm.preprocess_hydrograph_pentad_data(hydrograph_pentad_12176, today)
    preprocessed_data_12256 = dm.preprocess_hydrograph_pentad_data(hydrograph_pentad_12256, today)

    assert preprocessed_data_12176['pentad'].equals(hydrograph_pentad_12176['pentad'])
    assert min(preprocessed_data_12176["Min"]) == min(hydrograph_pentad_12176[['2000', '2001', '2002', '2003', '2022']].min())
    assert max(preprocessed_data_12176["Max"]) == max(hydrograph_pentad_12176[['2000', '2001', '2002', '2003', '2022']].max())

    assert preprocessed_data_12256['pentad'].equals(hydrograph_pentad_12256['pentad'])
    assert min(preprocessed_data_12256["Min"]) == min(hydrograph_pentad_12256[['2000', '2001', '2002', '2003', '2022']].min())
    assert max(preprocessed_data_12256["Max"]) == max(hydrograph_pentad_12256[['2000', '2001', '2002', '2003', '2022']].max())

    # Clean up the environment variables
    os.environ.pop("ieasyforecast_intermediate_data_path")
    os.environ.pop("ieasyforecast_hydrograph_pentad_file")
    os.environ.pop("ieasyforecast_configuration_path")
    os.environ.pop("ieasyforecast_config_file_all_stations")








