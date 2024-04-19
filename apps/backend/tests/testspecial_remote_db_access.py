from ieasyhydro_sdk.sdk import IEasyHydroSDK
from dotenv import load_dotenv

from ieasyhydro_sdk.filters import BasicDataValueFilters
import datetime as dt

import pandas as pd
import sys
import os

# Get the absolute path of the directory containing the current script
cwd = os.getcwd()

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(
    cwd, '..', 'iEasyHydroForecast')

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the modules from the forecast library
import tag_library as tl
import forecast_library as fl

env_file_path = "../config/.env_develop_kghm"
load_dotenv(env_file_path)
print("DEBUG: IEASYHYDRO_HOST: ", os.getenv("IEASYHYDRO_HOST"))

# Load sdk configuration from .env
ieh_sdk = IEasyHydroSDK()

predictor_dates = [dt.datetime(2024, 4, 3, 0, 0, 0), dt.datetime(2024, 4, 5, 12, 0, 0)]

# Define date filter
filters = BasicDataValueFilters(
    local_date_time__gte=predictor_dates[0],
    local_date_time__lt=predictor_dates[1]
)

site = '15194'

# Get data
qdata = ieh_sdk.get_data_values_for_site(
    [site],
    'discharge_daily_average',
    filters=filters
)
qdata = pd.DataFrame(qdata['data_values'])
print("get_data_values_for_site:\n", qdata)
#print(type(qdata))

tdata = ieh_sdk.get_data_values_for_site(
    [site],
    'discharge_daily',
    filters=filters,
)
tdata = pd.DataFrame(tdata['data_values'])
print(tdata)

# Get the first row from tdata in the wide format
row = pd.DataFrame(tdata.iloc[-1]).transpose()
print(row)

# add the row to qdata
qdata = pd.concat([qdata, row])
print(qdata)

sites = fl.Site(code=site)
print(sites)
fl.Site.from_DB_get_predictor_sum(sdk=ieh_sdk, site=sites, dates=predictor_dates, lagdays=20)
print(sites.predictor)
print(sum(qdata['data_value']))

sdata = ieh_sdk.get_discharge_sites()
print("get_discharge_sites:\n", pd.DataFrame(sdata))

# print unique values in the 'basin' column
print("Unique values in the 'basin' column:\n", pd.DataFrame(sdata)['basin'].unique())


