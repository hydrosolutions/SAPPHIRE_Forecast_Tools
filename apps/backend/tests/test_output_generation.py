import os
import pandas as pd
import pytest
import datetime as dt
from unittest import mock
from backend.src import output_generation, data_processing

import forecast_library as fl

def test_validate_hydrograph_data_with_dummy_data():
    # Test with valid data
    data = {
        'Date': ['2022-01-01', '2022-01-02'],
        'Year': [2022, 2022],
        'Code': ['code1', 'code2'],
        'Q_m3s': [1.0, 2.0],
        'discharge_avg': [1.5, 2.5],
        'pentad': [1, 2]
    }
    df = pd.DataFrame(data)
    df['Date'] = pd.to_datetime(df['Date'])
    result = output_generation.validate_hydrograph_data(df)
    assert isinstance(result, pd.DataFrame)
    assert 'day_of_year' in result.columns

    # Test with missing column
    del df['Code']
    with pytest.raises(ValueError):
        output_generation.validate_hydrograph_data(df)

    # Test with wrong data type
    with pytest.raises(TypeError):
        output_generation.validate_hydrograph_data("not a dataframe")

def test_reformat_hydrograph_data():
    # Test with valid data
    data = {
        'Code': ['code1', 'code2', 'code1', 'code2'],
        'Year': [2022, 2022, 2023, 2023],
        'day_of_year': [1, 2, 1, 2],
        'Q_m3s': [1.0, 2.0, 4.0, 5.0],
        'pentad': [1, 2, 1, 2],
        'discharge_avg': [1.5, 2.5, 3.5, 4.5]
    }
    df = pd.DataFrame(data)
    print("\n\nDEBUG: test_reformat_hydrograph_data: df: \n", df)
    hydrograph_pentad, hydrograph_day = output_generation.reformat_hydrograph_data(df)
    print("\n\nDEBUG: test_reformat_hydrograph_data: hydrograph_pentad: \n", hydrograph_pentad)
    print(hydrograph_day)
    assert isinstance(hydrograph_pentad, pd.DataFrame)
    assert isinstance(hydrograph_day, pd.DataFrame)
    assert 'Code' in hydrograph_pentad.index.names
    assert 'pentad' in hydrograph_pentad.index.names
    assert 'Code' in hydrograph_day.index.names
    assert 'day_of_year' in hydrograph_day.index.names

    assert hydrograph_pentad.iloc[0, 0] == 1.5
    assert hydrograph_pentad.iloc[1, 1] == 4.5
    assert hydrograph_day.iloc[0, 0] == 1.0
    assert hydrograph_day.iloc[1, 1] == 5.0

    # Test with missing column
    del df['Code']
    with pytest.raises(KeyError):
        output_generation.reformat_hydrograph_data(df)


def test_write_hydrograph_with_valid_data():
    # Set up the environment variable
    os.environ["ieasyforecast_daily_discharge_path"] = "../data/daily_runoff"

    # Set the forecast flags
    # forecast_flags = config.ForecastFlags(pentad=True)

    # Define a start date
    start_date = dt.datetime(2022, 5, 5)

    # Define a list of forecast sites
    site_list = [fl.Site(code='12176'), fl.Site(code='12256')]

    # Mock ieh_sdk & access to DB
    ieh_sdk = mock.MagicMock()
    backend_has_access_to_db = False

    # Read data from files
    modified_data = data_processing.get_station_data(
        ieh_sdk, backend_has_access_to_db, start_date, site_list)
    print("\n\nDEBUG: test_write_hydrograph_with_valid_data: modified_data: \n",
          modified_data[['Date', 'Q_m3s', 'issue_date', 'discharge_sum', 'discharge_avg']].head())
    print(modified_data[['Date', 'Q_m3s', 'issue_date', 'discharge_sum', 'discharge_avg']].tail())

    # Test validate_hydrograph_data with modified_data
    hydrograph_data = output_generation.validate_hydrograph_data(modified_data)

    # Test if first and last values of Date, Q_m3s, Year and Code are the same in
    # modified_data and hydrograph_data
    assert modified_data['Date'].iloc[0] == hydrograph_data['Date'].iloc[0]
    assert modified_data['Date'].iloc[-1] == hydrograph_data['Date'].iloc[-1]
    assert modified_data['Q_m3s'].iloc[0] == hydrograph_data['Q_m3s'].iloc[0]
    assert modified_data['Q_m3s'].iloc[-1] == hydrograph_data['Q_m3s'].iloc[-1]
    assert modified_data['Year'].iloc[0] == hydrograph_data['Year'].iloc[0]
    assert modified_data['Year'].iloc[-1] == hydrograph_data['Year'].iloc[-1]
    assert modified_data['Code'].iloc[0] == hydrograph_data['Code'].iloc[0]
    assert modified_data['Code'].iloc[-1] == hydrograph_data['Code'].iloc[-1]

    print("\n\nDEBUG: test_write_hydrograph_with_valid_data: hydrograph_data: \n", hydrograph_data.head())
    print(hydrograph_data.tail())

    # Test reformat_hydrograph_data with hydrograph_data
    hydrograph_pentad, hydrograph_day = output_generation.reformat_hydrograph_data(hydrograph_data)

    print("\n\nDEBUG: test_write_hydrograph_with_valid_data: hydrograph_pentad: \n", hydrograph_pentad.head())
    print(hydrograph_pentad.tail())
    print("\n\nDEBUG: test_write_hydrograph_with_valid_data: hydrograph_day: \n", hydrograph_day.head())
    print(hydrograph_day.tail())