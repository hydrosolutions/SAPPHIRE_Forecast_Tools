import os
import pandas as pd
import sys
from unittest import mock
import numpy as np
import datetime as dt
from backend.src import forecasting, data_processing, config

import forecast_library as fl

def test_get_predictor_with_df_from_public_demo_data():
    # Set up the environment variable
    os.environ["ieasyforecast_daily_discharge_path"] = "../data/daily_runoff"

    # Set the forecast flags
    forecast_flags = config.ForecastFlags(pentad=True, decad=True)

    # Define a start date
    start_date = dt.datetime(2022, 5, 5)

    # Define a list of forecast sites
    site_list = [fl.Site(code='12176'), fl.Site(code='12256')]

    # Mock ieh_sdk & access to DB
    ieh_sdk = mock.MagicMock()
    backend_has_access_to_db = False

    # Read data from files
    modified_data, modified_data_decad = data_processing.get_station_data(
        ieh_sdk, backend_has_access_to_db, start_date, site_list, forecast_flags)
    #print("\n\nDEBUG: test_get_predictor_with_df_from_public_demo_data: modified_data: \n", modified_data.head())
    #print(modified_data.tail())

    # Get predictor dates
    predictor_dates = data_processing.get_predictor_dates(start_date, forecast_flags)
    #print("\n\nDEBUG: test_get_predictor_with_df_from_public_demo_data: predictor_dates: \n", predictor_dates)

    # Get predictor
    forecasting.get_predictor(modified_data, start_date, site_list, ieh_sdk, False, predictor_dates.pentad)
    print("\n\nDEBUG: test_get_predictor_with_df_from_public_demo_data: site.predictors: \n", site_list[0].predictor, site_list[1].predictor)

    # Predictor of site 12176 should be nan
    assert np.isnan(site_list[0].predictor)
    # Predictor of site 12256 should be 2.43
    assert round(site_list[1].predictor, 2) == 2.43

    # Clean up the environment variable
    os.environ.pop("ieasyforecast_daily_discharge_path")


def test_get_predictor_with_df_from_test_file():
    # Set up the environment variable
    os.environ["ieasyforecast_daily_discharge_path"] = "backend/tests/test_files"

    # Set the forecast flags
    forecast_flags = config.ForecastFlags(pentad=True, decad=True)

    # Define a start date
    start_date = dt.datetime(2018, 5, 5)

    # Define a list of forecast sites
    site_list = [fl.Site(code='15678')]

    # Mock ieh_sdk & access to DB
    ieh_sdk = mock.MagicMock()
    backend_has_access_to_db = False

    # Read data from files
    modified_data, modified_data_decad = data_processing.get_station_data(
        ieh_sdk, backend_has_access_to_db, start_date, site_list, forecast_flags)

    # Get predictor dates
    predictor_dates = data_processing.get_predictor_dates(start_date, forecast_flags)

    # Get predictor
    forecasting.get_predictor(modified_data, start_date, site_list, ieh_sdk, False, predictor_dates.pentad)

    # The predictor should have value 4.0
    assert round(site_list[0].predictor, 1) == 4.0

    # Clean up the environment variable
    os.environ.pop("ieasyforecast_daily_discharge_path")


def test_get_predictor_with_predefined_df():

    # Define a start date
    start_date = dt.datetime(2018, 5, 5)

    # Define a list of forecast sites
    site_list = [fl.Site(code='15678')]

    # Mock ieh_sdk
    ieh_sdk = mock.MagicMock()

    # Define a data frame with data
    df = pd.DataFrame({
        'Date': ["2018-04-16", "2018-04-17", "2018-04-18", "2018-04-19",
                 "2018-04-20", "2018-04-21", "2018-04-22", "2018-04-23",
                 "2018-04-24", "2018-04-25", "2018-04-26", "2018-04-27",
                 "2018-04-28", "2018-04-29", "2018-04-30", "2018-05-01",
                 "2018-05-02", "2018-05-03", "2018-05-04", "2018-05-05"],
        'Q_m3s': [1.96, 1.88, 1.96, 1.96, 1.96, 1.88, 1.96, 1.96, 1.96, 1.88,
                  1.96, 1.96, 1.96, 1.88, 1.80, 1.35, 1.30, 1.35, 1.35, 1.25],
        'Year': [2018, 2018, 2018, 2018, 2018, 2018, 2018, 2018, 2018, 2018,
                 2018, 2018, 2018, 2018, 2018, 2018, 2018, 2018, 2018, 2018],
        'Code': ['15678','15678','15678','15678','15678','15678','15678','15678','15678','15678',
                 '15678','15678','15678','15678','15678','15678','15678','15678','15678','15678'],
        'issue_date': [np.nan, np.nan, np.nan, np.nan, True, np.nan, np.nan, np.nan, np.nan, True,
                       np.nan, np.nan, np.nan, np.nan, True, np.nan, np.nan, np.nan, np.nan, True],
        'discharge_sum': [np.nan, np.nan, np.nan, np.nan, 5.8, np.nan, np.nan, np.nan, np.nan, 5.88,
                          np.nan, np.nan, np.nan, np.nan, 5.8, np.nan, np.nan, np.nan, np.nan, 4.0],
        'discharge_avg': [np.nan, np.nan, np.nan, np.nan, 1.784, np.nan, np.nan, np.nan, np.nan, 1.784,
                          np.nan, np.nan, np.nan, np.nan, 1.784, np.nan, np.nan, np.nan, np.nan, np.nan],
        'pentad': ['4', '4', '4', '4', '4', '5', '5', '5', '5', '5',
                   '6', '6', '6', '6', '6', '1', '1', '1', '1', '1'],
        'pentad_in_year': ['22', '22', '22', '22', '22', '23', '23', '23', '23', '23',
                           '24', '24', '24', '24', '24', '25', '25', '25', '25', '25']
    })

    # Group df by code
    df = df.groupby('Code').apply(lambda x: x.sort_values('Date')).reset_index(drop=True)

    # Define a list of predictor dates
    predictor_dates = [dt.date(2018, 5, 1), dt.date(2018, 5, 2), dt.date(2018, 5, 3)]

    forecasting.get_predictor(df, start_date, site_list, ieh_sdk, False, predictor_dates)

    # The predictor should have value 4.0
    assert round(site_list[0].predictor, 1) == 4.0