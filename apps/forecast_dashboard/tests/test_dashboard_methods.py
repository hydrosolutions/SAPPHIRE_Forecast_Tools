import os
import pytest
import datetime as dt
import pandas as pd
import time
from unittest.mock import patch
from forecast_dashboard.src import processing as dm

"""
def test_preprocess_hydrograph_day_data():
    # Set environment variables
    os.environ["ieasyforecast_intermediate_data_path"] = "backend/tests/test_files"
    os.environ["ieasyforecast_hydrograph_day_file"] = "test_one_step_hydrograph_day.csv"
    os.environ["ieasyforecast_configuration_path"] = "config"
    os.environ["ieasyforecast_config_file_all_stations"] = "config_all_stations_library.json"
    today = dt.datetime.strptime('2023-04-01', '%Y-%m-%d')
    hydrograph_day_all = dm.read_hydrograph_day_file()
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
"""







