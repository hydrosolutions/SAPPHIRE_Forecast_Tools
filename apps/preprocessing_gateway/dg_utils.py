# --------------------------------------------------------------------
# Import Libraries
# --------------------------------------------------------------------
import os
import sys
import json
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging
from logging.handlers import TimedRotatingFileHandler
import traceback

# Note that the sapphire data gateway client is currently a private repository
# Access to the repository is required to install the package
# Further, access to the data gateway through an API key is required to use the
# client. The API key is stored in a .env file in the root directory of the project.
# The forecast tools can be used without access to the sapphire data gateay but
# the full power of the tools is only available with access to the data gateway.
#pip install git+https://github.com/hydrosolutions/sapphire-dg-client.git
import sapphire_dg_client

# Local libraries
# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')
#print(script_dir)
#print(forecast_dir)

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the setup_library module from the iEasyHydroForecast package
import setup_library as sl


# Set up logging
# Configure the logging level and formatter
logging.basicConfig(level=logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Create the logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Create a file handler to write logs to a file
# A new log file is created every <interval> day at <when>. It is kept for <backupCount> days.
file_handler = TimedRotatingFileHandler('logs/log', when='midnight', interval=1, backupCount=30)
file_handler.setFormatter(formatter)

# Create a stream handler to print logs to the console
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Get the root logger and add the handlers to it
logger = logging.getLogger()
logger.handlers = []
logger.addHandler(file_handler)
logger.addHandler(console_handler)


# --------------------------------------------------------------------
# Quantile Mapping
# --------------------------------------------------------------------
def ptf(x: np.array,  a: float, b:float ) -> np.array:
    return a * np.power(x, b)

def quantile_mapping_ptf(sce_data:np.array, a: float, b: float, wet_days: bool = True, wet_day_threshold: float = 0) -> np.array:
    """
    Perform quantile mapping for precipitation or temperature data.
    FORMULA: y_fit = a * y_era^b
    Inputs:
        sce_data: numpy array of shape (n,) with the data to be transformed.
        a: float
        b: float
        wet_days: boolean, if True, the transformation is performed only for wet days.
        wet_day_threshold: float, the threshold to define wet days.
    Outputs:
        transformed_sce: numpy array of shape (n,) with the transformed data.
    """
    if wet_days:
        dry_days = sce_data <= wet_day_threshold
        # dry days to zero
        sce_data[dry_days] = 0
        transformed_sce = ptf(sce_data, a, b)

    else:
        transformed_sce = ptf(sce_data, a, b)

    #round to 3 decimals
    transformed_sce = np.round(transformed_sce, 2)

    return transformed_sce

def do_quantile_mapping(era5_data: pd.DataFrame, P_param: pd.DataFrame, T_param: pd.DataFrame, ensemble: bool) -> pd.DataFrame:
    """
    Loop over all the stations and perform the quantile mapping for each station for the control member.
    Inputs:
        era5_data: pandas DataFrame with the ERA5 data.
        P_param: pandas DataFrame with the precipitation parameters.
        T_param: pandas DataFrame with the temperature parameters.
    Outputs:
        P_data: pandas DataFrame with the transformed precipitation data.
        T_data: pandas DataFrame with the transformed temperature data.
    """
    era5_data = era5_data.copy()
    #get the unique codes
    codes = era5_data['code'].unique()
    #iterate over the codes
    for code in codes:
        #get the data for the code
        code_data = era5_data[era5_data['code'] == code]

        #get the parameters for the code
        P_param_code = P_param[P_param['code'] == code]
        T_param_code = T_param[T_param['code'] == code]

        #get the parameters
        a_P = P_param_code['a'].values
        b_P = P_param_code['b'].values
        threshold_P = P_param_code['wet_day'].values
        #logger.debug(f"Code: {code[0]}, a_P: {a_P[0]}, b_P: {b_P[0]}, threshold_P: {threshold_P[0]}")
        #logger.debug(f"Types of a_P: {type(a_P[0])}, b_P: {type(b_P[0])}, threshold_P: {type(threshold_P[0])}")

        a_T = T_param_code['a'].values
        b_T = T_param_code['b'].values

        #transform the data
        code_data.loc[:,'P'] = quantile_mapping_ptf(code_data['P'].values, a_P, b_P, wet_days=True, wet_day_threshold=threshold_P)

        #for temperature we need to tranform it to Kelvin
        T_data = code_data['T'].values + 273.15
        T_fitted = quantile_mapping_ptf(T_data, a_T, b_T, wet_days=False, wet_day_threshold=0)
        code_data.loc[:,'T'] = T_fitted - 273.15

        era5_data.loc[era5_data['code'] == code, 'P'] = code_data['P']
        era5_data.loc[era5_data['code'] == code, 'T'] = code_data['T']

    if ensemble:
        P_data = era5_data[['date', 'P', 'code', 'ensemble_member']].copy()
        T_data = era5_data[['date', 'T', 'code', 'ensemble_member']].copy()
    else:
        P_data = era5_data[['date', 'P', 'code']].copy()
        T_data = era5_data[['date', 'T', 'code']].copy()

    return P_data, T_data


# --------------------------------------------------------------------
# TRANSFORM DATA FILE
# --------------------------------------------------------------------
def transform_data_file_control_member(data_file:pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the data file from the data gateaway in a more handy format.
    Inputs:
        data_file: pd.DataFrame with the data from the data gateaway. columns Code XXXXX is T and columns Code XXXXX.1 is P
    Outputs:
        transformed_data: pd.DataFrame with the transformed data. Columns are 'date', 'P', 'T', 'code'
    """
    extension_mapper = { # Temperature is without a . extension - so just the code
        '.1': 'P',
        '.2' : 'SD', # so far we ignore this column
    }

    data_file = data_file.copy()
    # rename the Station column to 'date'
    data_file.rename(columns={'Station': 'date'}, inplace=True)

    #than we need to drop the first 7 rows of the era5 data
    data_file = data_file.iloc[7:]

    # now we need to convert the date column to a datetime object
    data_file['date'] = pd.to_datetime(data_file['date'], dayfirst=True)

    #sort by the date
    data_file = data_file.sort_values('date')


    transformed_data_file = pd.DataFrame()

    #unique codes
    codes = data_file.columns[1:]

    # if the ".1" is not in code
    codes = [code for code in codes if (code[-2:] not in extension_mapper.keys() and code != 'Source')]

    #iterate over the codes
    for code in codes:
        # get the data for the code
        code_data = data_file[['date', code, code + '.1']].copy()
        # rename the columns
        code_data.rename(columns={code: 'T', code + '.1': 'P'}, inplace=True)
        # Add the 'code' column
        code_data['code'] = code
        # Convert 'T' and 'P' columns to numeric, coercing errors
        code_data['T'] = pd.to_numeric(code_data['T'], errors='coerce').astype(float)
        code_data['P'] = pd.to_numeric(code_data['P'], errors='coerce').astype(float)
        transformed_data_file = pd.concat([transformed_data_file, code_data], axis = 0)

    return transformed_data_file




# --------------------------------------------------------------------
# SNOW MODEL
# --------------------------------------------------------------------
def transform_snow_data(df, var_name):
    df = df.copy()
    #rename the first column to date
    columns = df.columns
    columns = list(columns)
    columns[0] = 'date'
    df.columns = columns

    # this is hard coded
    df = df.iloc[4:]

    df['date'] = pd.to_datetime(df['date'], dayfirst=True)

    code_dict = {}

    for col in df.columns:
        if col != 'date' and col != 'Source':
            #split by "_"
            new_col = col.split("_")
            if len(new_col) > 1:
                code = int(new_col[0])
                elevation_band = int(new_col[1])
                new_var_name = f"{var_name}_{elevation_band}"
            else:
                code = int(new_col[0])
                elevation_band = None
                new_var_name = var_name

            dates = df['date']
            values = df[col].astype(float)
            if code not in code_dict:
                code_dict[code] = {'date': dates, new_var_name: values}
            else:
                code_dict[code][new_var_name] = values

    new_df = pd.DataFrame()
    for code, data in code_dict.items():
        code_df = pd.DataFrame(data)
        code_df['code'] = code
        new_df = pd.concat([new_df, code_df], ignore_index=True)

    return new_df




