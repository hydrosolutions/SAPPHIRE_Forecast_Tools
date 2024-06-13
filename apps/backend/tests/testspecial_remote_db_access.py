from ieasyhydro_sdk.sdk import IEasyHydroSDK
from dotenv import load_dotenv

from ieasyhydro_sdk.filters import BasicDataValueFilters
import datetime as dt

import pandas as pd
import sys
import os

# Get the absolute path of the directory containing the current script
cwd = os.getcwd()
print(cwd)

env_file_path = "../../../../sensitive_data_forecast_tools/config/.env_develop_kghm"
load_dotenv(env_file_path)
print("DEBUG: IEASYHYDRO_HOST: ", os.getenv("IEASYHYDRO_HOST"))

# Load sdk configuration from .env
ieh_sdk = IEasyHydroSDK()

predictor_dates = [dt.datetime(2024, 6, 3, 0, 0, 0), dt.datetime.today()]

# Define date filter
filters = BasicDataValueFilters(
    local_date_time__gte=predictor_dates[0],
    local_date_time__lt=predictor_dates[1]
)

site = '15102'

# Get data
qdata = ieh_sdk.get_data_values_for_site(
    [site],
    'discharge_daily_average',
    filters=filters
)
qdata = pd.DataFrame(qdata['data_values'])
print("get_data_values_for_site:\n", qdata)

tdata = ieh_sdk.get_data_values_for_site(
    [site],
    'discharge_daily',
    filters=filters,
)
tdata = pd.DataFrame(tdata['data_values'])
#print(tdata)

# Get the first row from tdata in the wide format
row = pd.DataFrame(tdata.iloc[-1]).transpose()
#print(row)

# add the row to qdata
qdata = pd.concat([qdata, row])
print("Discharge data read from qdata:\n", qdata)



