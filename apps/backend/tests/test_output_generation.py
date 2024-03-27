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
    #print("\n\nDEBUG: test_write_hydrograph_with_valid_data: modified_data: \n",
    #      modified_data[['Date', 'Q_m3s', 'issue_date', 'discharge_sum', 'discharge_avg']].head())
    #print(modified_data[['Date', 'Q_m3s', 'issue_date', 'discharge_sum', 'discharge_avg']].tail())

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

    test_hydrograph_2000 = hydrograph_data[hydrograph_data['Year']=='2000']
    test_hydrograph_2003 = hydrograph_data[hydrograph_data['Year']=='2003']

    # Test reformat_hydrograph_data with hydrograph_data
    hydrograph_pentad, hydrograph_day = output_generation.reformat_hydrograph_data(hydrograph_data)
    #print("\n\nDEBUG: test_write_hydrograph_with_valid_data: test_hydrograph_2003: \n", test_hydrograph_2003.head(36))
    #print(test_hydrograph_2003.tail(36))

    #print("\n\nDEBUG: test_write_hydrograph_with_valid_data: hydrograph_pentad: \n", hydrograph_pentad.reset_index().head(10))
    #print(hydrograph_pentad.reset_index().tail(10))
    #print("\n\nDEBUG: test_write_hydrograph_with_valid_data: hydrograph_day: \n", hydrograph_day.head())
    #print(hydrograph_day.tail())

    # The first value in hydrograph_pentad['2000'] should be the same as the
    # fifth value in hydrograph_data['disharge_avg'].
    # The second value in hydrograph_pentad['2000'] should be the same as the
    # tenth value in hydrograph_data['disharge_avg'] and so on.
    assert hydrograph_pentad['2000'].iloc[0] == test_hydrograph_2000['discharge_avg'].iloc[4]  # pentad 1
    assert hydrograph_pentad['2000'].iloc[1] == test_hydrograph_2000['discharge_avg'].iloc[9]  # pentad 2
    assert hydrograph_pentad['2000'].iloc[2] == test_hydrograph_2000['discharge_avg'].iloc[14]  # pentad 3
    assert hydrograph_pentad['2000'].iloc[3] == test_hydrograph_2000['discharge_avg'].iloc[19]
    assert hydrograph_pentad['2000'].iloc[4] == test_hydrograph_2000['discharge_avg'].iloc[24]
    assert hydrograph_pentad['2000'].iloc[5] == test_hydrograph_2000['discharge_avg'].iloc[30]
    assert hydrograph_pentad['2000'].iloc[6] == test_hydrograph_2000['discharge_avg'].iloc[35]

    assert hydrograph_pentad['2003'].iloc[0] == test_hydrograph_2003['discharge_avg'].iloc[4]  # pentad 1
    assert hydrograph_pentad['2003'].iloc[1] == test_hydrograph_2003['discharge_avg'].iloc[9]
    assert hydrograph_pentad['2003'].iloc[2] == test_hydrograph_2003['discharge_avg'].iloc[14]
    assert hydrograph_pentad['2003'].iloc[3] == test_hydrograph_2003['discharge_avg'].iloc[19]
    assert hydrograph_pentad['2003'].iloc[4] == test_hydrograph_2003['discharge_avg'].iloc[24]
    assert hydrograph_pentad['2003'].iloc[5] == test_hydrograph_2003['discharge_avg'].iloc[30]
    assert hydrograph_pentad['2003'].iloc[6] == test_hydrograph_2003['discharge_avg'].iloc[35]
    # The same from the back end. The sixth-to-last value in
    # hydrograph_pentad['2000'] should be the same as the the last value in
    # hydrograph_data['disharge_avg'].
    assert test_hydrograph_2003['discharge_avg'].iloc[-1] == hydrograph_pentad['2003'].iloc[-1]
    assert test_hydrograph_2003['discharge_avg'].iloc[-7] == hydrograph_pentad['2003'].iloc[-2]
    assert test_hydrograph_2003['discharge_avg'].iloc[-12] == hydrograph_pentad['2003'].iloc[-3]
    assert test_hydrograph_2003['discharge_avg'].iloc[-17] == hydrograph_pentad['2003'].iloc[-4]
    assert test_hydrograph_2003['discharge_avg'].iloc[-22] == hydrograph_pentad['2003'].iloc[-5]
    assert test_hydrograph_2003['discharge_avg'].iloc[-27] == hydrograph_pentad['2003'].iloc[-6]

    # Set the environment variables
    # Set environment variables
    os.environ["ieasyforecast_intermediate_data_path"] = "backend/tests/test_files/tmp"
    os.environ["ieasyforecast_hydrograph_pentad_file"] = "test_pentad.csv"
    os.environ["ieasyforecast_hydrograph_day_file"] = "test_day.csv"

    # Create the tmp directory
    os.makedirs(os.getenv("ieasyforecast_intermediate_data_path"), exist_ok=True)

    # Write the hydrograph data to csv files
    output_generation.save_hydrograph_data_to_csv(hydrograph_pentad, hydrograph_day)

    # Test if the files were created
    assert os.path.exists(
        os.path.join(os.getenv("ieasyforecast_intermediate_data_path"),
                     os.getenv("ieasyforecast_hydrograph_pentad_file")))
    assert os.path.exists(
        os.path.join(os.getenv("ieasyforecast_intermediate_data_path"),
                        os.getenv("ieasyforecast_hydrograph_day_file")))

    # Read in the files and check the content
    saved_pentad = pd.read_csv(
        os.path.join(os.getenv("ieasyforecast_intermediate_data_path"),
                        os.getenv("ieasyforecast_hydrograph_pentad_file")))
    saved_day = pd.read_csv(
        os.path.join(os.getenv("ieasyforecast_intermediate_data_path"),
                        os.getenv("ieasyforecast_hydrograph_day_file")))

    #print("\n\nDEBUG: test_write_hydrograph_with_valid_data: saved_pentad: \n", saved_pentad.head(10))
    #print(saved_pentad.tail(10))
    #print("\n\nDEBUG: test_write_hydrograph_with_valid_data: saved_day: \n", saved_day.head(10))
    #print(saved_day.tail(10))

    # Check the content of the files column by column
    assert (saved_pentad['Code'].values.astype(str) == hydrograph_pentad.reset_index()['Code'].values.astype(str)).all()
    assert (saved_pentad['pentad'].values.astype(str) == hydrograph_pentad.reset_index()['pentad'].values.astype(str)).all()

    # Test if the difference between the values in the '2000' column of
    # saved_pentad and hydrograph_pentad is less than 1e-6
    assert (saved_pentad['2000'].values.astype(float) - hydrograph_pentad['2000'].values.astype(float) < 1e-6).all()
    assert (saved_pentad['2001'].values.astype(float) - hydrograph_pentad['2001'].values.astype(float) < 1e-6).all()
    assert (saved_pentad['2002'].values.astype(float) - hydrograph_pentad['2002'].values.astype(float) < 1e-6).all()
    assert (saved_pentad['2003'].values.astype(float) - hydrograph_pentad['2003'].values.astype(float) < 1e-6).all()
    assert (saved_pentad['2022'].dropna().values.astype(float) - hydrograph_pentad['2022'].dropna().values.astype(float) < 1e-6).all()

    # Do the same for the day data
    assert (saved_day['Code'].values.astype(str) == hydrograph_day.reset_index()['Code'].values.astype(str)).all()
    assert (saved_day['day_of_year'].values.astype(str) == hydrograph_day.reset_index()['day_of_year'].values.astype(str)).all()
    assert (saved_day['2000'].dropna().values.astype(float) - hydrograph_day['2000'].dropna().values.astype(float) < 1e-6).all()
    assert (saved_day['2001'].dropna().values.astype(float) - hydrograph_day['2001'].dropna().values.astype(float) < 1e-6).all()
    assert (saved_day['2002'].dropna().values.astype(float) - hydrograph_day['2002'].dropna().values.astype(float) < 1e-6).all()
    assert (saved_day['2003'].dropna().values.astype(float) - hydrograph_day['2003'].dropna().values.astype(float) < 1e-6).all()
    assert (saved_day['2022'].dropna().values.astype(float) - hydrograph_day['2022'].dropna().values.astype(float) < 1e-6).all()

    # Do Also write data with write_hydrograph_data and check if the files are
    # created as expected.
    output_generation.write_hydrograph_data(modified_data)

    # Test if the files were created
    assert os.path.exists(
        os.path.join(os.getenv("ieasyforecast_intermediate_data_path"),
                     os.getenv("ieasyforecast_hydrograph_pentad_file")))
    assert os.path.exists(
        os.path.join(os.getenv("ieasyforecast_intermediate_data_path"),
                        os.getenv("ieasyforecast_hydrograph_day_file")))

    # Read in the files and check the content
    saved_pentad = pd.read_csv(
        os.path.join(os.getenv("ieasyforecast_intermediate_data_path"),
                        os.getenv("ieasyforecast_hydrograph_pentad_file")))
    saved_day = pd.read_csv(
        os.path.join(os.getenv("ieasyforecast_intermediate_data_path"),
                        os.getenv("ieasyforecast_hydrograph_day_file")))

    #print("\n\nDEBUG: test_write_hydrograph_with_valid_data: saved_pentad: \n", saved_pentad.head(10))
    #print(saved_pentad.tail(10))
    #print("\n\nDEBUG: test_write_hydrograph_with_valid_data: saved_day: \n", saved_day.head(10))
    #print(saved_day.tail(10))

    # Check the content of the files column by column
    assert (saved_pentad['Code'].values.astype(str) == hydrograph_pentad.reset_index()['Code'].values.astype(str)).all()
    assert (saved_pentad['pentad'].values.astype(str) == hydrograph_pentad.reset_index()['pentad'].values.astype(str)).all()

    # Test if the difference between the values in the '2000' column of
    # saved_pentad and hydrograph_pentad is less than 1e-6
    assert (saved_pentad['2000'].values.astype(float) - hydrograph_pentad['2000'].values.astype(float) < 1e-6).all()
    assert (saved_pentad['2001'].values.astype(float) - hydrograph_pentad['2001'].values.astype(float) < 1e-6).all()
    assert (saved_pentad['2002'].values.astype(float) - hydrograph_pentad['2002'].values.astype(float) < 1e-6).all()
    assert (saved_pentad['2003'].values.astype(float) - hydrograph_pentad['2003'].values.astype(float) < 1e-6).all()
    assert (saved_pentad['2022'].dropna().values.astype(float) - hydrograph_pentad['2022'].dropna().values.astype(float) < 1e-6).all()

    # Do the same for the day data
    assert (saved_day['Code'].values.astype(str) == hydrograph_day.reset_index()['Code'].values.astype(str)).all()
    assert (saved_day['day_of_year'].values.astype(str) == hydrograph_day.reset_index()['day_of_year'].values.astype(str)).all()
    assert (saved_day['2000'].dropna().values.astype(float) - hydrograph_day['2000'].dropna().values.astype(float) < 1e-6).all()
    assert (saved_day['2001'].dropna().values.astype(float) - hydrograph_day['2001'].dropna().values.astype(float) < 1e-6).all()
    assert (saved_day['2002'].dropna().values.astype(float) - hydrograph_day['2002'].dropna().values.astype(float) < 1e-6).all()
    assert (saved_day['2003'].dropna().values.astype(float) - hydrograph_day['2003'].dropna().values.astype(float) < 1e-6).all()
    assert (saved_day['2022'].dropna().values.astype(float) - hydrograph_day['2022'].dropna().values.astype(float) < 1e-6).all()


    # Delete the tmp directory and all files in it
    for file in os.listdir(os.getenv("ieasyforecast_intermediate_data_path")):
        os.remove(os.path.join(os.getenv("ieasyforecast_intermediate_data_path"), file))
    os.rmdir(os.getenv("ieasyforecast_intermediate_data_path"))

    # Clean up the environment
    os.environ.pop("ieasyforecast_intermediate_data_path")
    os.environ.pop("ieasyforecast_hydrograph_pentad_file")
    os.environ.pop("ieasyforecast_hydrograph_day_file")


def test_save_hydrograph_data_to_csv():
    # Create test data
    hydrograph_pentad = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]}, index=['a', 'b', 'c'])
    hydrograph_day = pd.DataFrame({'C': [7, 8, 9], 'D': [10, 11, 12]}, index=['a', 'b', 'c'])

    # Give some name to the index
    hydrograph_pentad.index.name = "Code"
    hydrograph_day.index.name = "Code"

    # Set environment variables
    os.environ["ieasyforecast_intermediate_data_path"] = "backend/tests/test_files/tmp"
    os.environ["ieasyforecast_hydrograph_pentad_file"] = "test_pentad.csv"
    os.environ["ieasyforecast_hydrograph_day_file"] = "test_day.csv"

    # Create the tmp directory
    os.makedirs(os.getenv("ieasyforecast_intermediate_data_path"), exist_ok=True)

    # Call the function
    output_generation.save_hydrograph_data_to_csv(hydrograph_pentad, hydrograph_day)

    # Check that the files were created
    assert os.path.exists(os.path.join(os.getenv("ieasyforecast_intermediate_data_path"), os.getenv("ieasyforecast_hydrograph_pentad_file")))
    assert os.path.exists(os.path.join(os.getenv("ieasyforecast_intermediate_data_path"), os.getenv("ieasyforecast_hydrograph_day_file")))

    # Check the content of the files
    saved_pentad = pd.read_csv(
        os.path.join(os.getenv("ieasyforecast_intermediate_data_path"),
                     os.getenv("ieasyforecast_hydrograph_pentad_file")),
                     index_col="Code")
    saved_day = pd.read_csv(
        os.path.join(os.getenv("ieasyforecast_intermediate_data_path"),
                     os.getenv("ieasyforecast_hydrograph_day_file")),
                     index_col="Code")

    pd.testing.assert_frame_equal(saved_pentad, hydrograph_pentad)
    pd.testing.assert_frame_equal(saved_day, hydrograph_day)

    # Delete the tmp directory and all files in it
    for file in os.listdir(os.getenv("ieasyforecast_intermediate_data_path")):
        os.remove(os.path.join(os.getenv("ieasyforecast_intermediate_data_path"), file))
    os.rmdir(os.getenv("ieasyforecast_intermediate_data_path"))

    # Clean up environment
    os.environ.pop("ieasyforecast_intermediate_data_path")
    os.environ.pop("ieasyforecast_hydrograph_pentad_file")
    os.environ.pop("ieasyforecast_hydrograph_day_file")