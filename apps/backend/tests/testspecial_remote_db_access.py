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

# Define date filter
filters = BasicDataValueFilters(
    local_date_time__gt=dt.datetime(2020, 4, 10),
    local_date_time__lt=dt.datetime(2020, 4, 30)
)

# Get data
qdata = ieh_sdk.get_data_values_for_site(
    '15194',
    'discharge_daily_average',
    filters=filters
)
qdata = pd.DataFrame(qdata['data_values'])
print("get_data_values_for_site:\n", qdata)

sdata = ieh_sdk.get_discharge_sites()
print("get_discharge_sites:\n", sdata)


