# --------------------------------------------------------------------
# FORCING DATA PREPROCESSING
# --------------------------------------------------------------------
#        _
#      _( )_
#    _(     )_
#   (_________)
#     \  \  \
#       \  \
#         \  \
# --------------------------------------------------------------------
# DESCRIPTION:
# This script reads in the ERA5-Land data and ECMWF IFS weather forecast data
# from the Data-Gateaway and, if parameters for quantile mapping are available,
# performs qunatile mapping for the daily precipitation sum P and the daily
# average air temperature T with pre-defined parameters.
# The Formula for the Mapping is: y_fit = a * y_era_5^b
#
# If access to the data gateway is not available, the script will return an
# exit value of 1 and print a warning message. The subsequent modelling steps
# based on the machine learning and conceptual models will not be run. The
# linear regression models will still be run.
# --------------------------------------------------------------------
# --------------------------------------------------------------------
# INPUT:
# ERA5-Land and ECMWF IFS weather forecast data from the SAPPHIRE data gateaway
# --------------------------------------------------------------------
# Pre-defined Parameters for the Quantile Mapping
# COLUMNS for the Parameters: 'code', 'a', 'b', 'wet_day'
# Saved as HRU{HRU_CODE}_T_params.csv and HRU{HRU_CODE}_P_params.csv
# --------------------------------------------------------------------
# --------------------------------------------------------------------
# OUTPUT:
# Quantile Mapped Data
# P_control_member.csv and T_control_member.csv
# With columns: 'date', 'P/T', and code
# Saved as {HRU_CODE}_P_control_member.csv and {HRU_CODE}_T_control_member.csv
# ENSEMBLE MEMBERS
# columns are 'date', 'P/T', 'code', 'ensemble_member'
# Saved as: {HRU_CODE}_P_ensemble_forecast.csv and {HRU_CODE}_T_ensemble_forecast.csv
# --------------------------------------------------------------------
# TODO:
# - Include the Real Parameters for the Quantile Mapping
# - Include Nan Cases -> what happens when the data-gateaway raises an error etc?
# - Test if all codes in ieasyhydroforecast_HRU_ENSEMBLE are in ieasyhydroforecast_HRU_CONTROL_MEMBER and print a waring if not.

# Required Libraries
# Install libraries from iEasyHydroForecast/requirements.txt

# Useage
# cd to the directory where the script is located (apps/preprocessing_gateway)
# python Quantile_Mapping_OP.py



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
    codes = [code for code in codes if (code[-2:] != '.1' and code != 'Source')]

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

def transform_data_file_ensemble_member(data_file: pd.DataFrame, HRU_CODE: str) -> pd.DataFrame:
    """
    Transforms the data file from the data gateaway in a more handy format.
    Inputs:
        data_file: pd.DataFrame with the data from the data gateaway. columns are the names resp. elevation bands
    Outputs:
        transformed_data: pd.DataFrame with the transformed data. Columns are 'date', 'Value (either P or T)', 'name' -> Later the HRU Code is added
    """
    data_file = data_file.copy()
    # rename the Station column to 'date'
    data_file.rename(columns={'Unnamed: 0': 'date'}, inplace=True)

    #get the type of data
    value_type = data_file.iloc[0].values[1]

    #than we need to drop the first 4 rows of the era5 data
    data_file = data_file.iloc[4:]

    # now we need to convert the date column to a datetime object
    data_file['date'] = pd.to_datetime(data_file['date'], dayfirst=True)

    data_file = data_file.sort_values('date')

    transformed_data_file = pd.DataFrame()

    #unique names, here they are actually the names of the different HRU
    names = data_file.columns[1:]

    for name in names:
        # get the data for the code
        code_data = data_file[['date', name]].copy()
        # rename the columns
        code_data.rename(columns={name: value_type}, inplace=True)
        # Add the 'name' column
        code_data['code'] = HRU_CODE
        code_data['name'] = name
        # Convert 'Value' column to numeric, coercing errors
        code_data[value_type] = pd.to_numeric(code_data[value_type], errors='coerce').astype(float)
        transformed_data_file = pd.concat([transformed_data_file, code_data], axis = 0)

    return transformed_data_file


# --------------------------------------------------------------------
# MERGE ENSEMBLE FORECAST
# --------------------------------------------------------------------
def merge_ensemble_forecast(files_downloaded: list) -> pd.DataFrame:
    """
    Merges the ensemble forecast files into one DataFrame.
    Inputs:
        files_downloaded: list of strings with the paths to the files downloaded.
    Outputs:
        merged_data: pd.DataFrame with the merged data.
    """
    #combine the data
    P_ensemble = pd.DataFrame()
    T_ensemble = pd.DataFrame()
    for file in files_downloaded:
        elements = file.split("_")
        HRU_CODE = elements[-2][-5:]
        variable = elements[-1].split(".")[0]
        ensemble_member = elements[-3][3:]
        #read the data file
        data_file = pd.read_csv(file)
        #transform the data file
        transformed_data_file = transform_data_file_ensemble_member(data_file, HRU_CODE)
        transformed_data_file['ensemble_member'] = int(ensemble_member)


        if variable == "tp":
            P_ensemble = pd.concat([P_ensemble, transformed_data_file], axis = 0)
        else:
            T_ensemble = pd.concat([T_ensemble, transformed_data_file], axis = 0)


    #combine the P and T data, on code, than name, than ensemble_member than date
    P_ensemble = P_ensemble.sort_values(['code', 'name', 'ensemble_member', 'date'])
    T_ensemble = T_ensemble.sort_values(['code','name', 'ensemble_member', 'date'])

    combined_df = pd.merge(P_ensemble, T_ensemble, on=['code', 'name','ensemble_member', 'date'], how='outer')

    #clear the memory
    del P_ensemble
    del T_ensemble

    return combined_df


# --------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------
def main():
    #--------------------------------------------------------------------
    # SETUP ENVIRONMENT
    #--------------------------------------------------------------------

    # Specify the path to the .env file
    # Loads the environment variables from the .env file
    sl.load_environment()

    # Test if an API key is available and exit the program if it isn't
    if not os.getenv('ieasyhydroforecast_API_KEY_GATEAWAY'):
        logger.warning("No API key for the data gateway found. Exiting program.\nMachine learning or conceptual models will not be run.")
        sys.exit(1)
    else:
        API_KEY = os.getenv('ieasyhydroforecast_API_KEY_GATEAWAY')
    #output_path for control member and ensemble
    OUTPUT_PATH_CM = os.path.join(
        os.getenv('ieasyforecast_intermediate_data_path'),
        os.getenv('ieasyhydroforecast_OUTPUT_PATH_CM'))
    # Test if the output path exists and create it if it doesn't
    if not os.path.exists(OUTPUT_PATH_CM):
        os.makedirs(OUTPUT_PATH_CM, exist_ok=True)

    OUTPUT_PATH_ENS = os.path.join(
        os.getenv('ieasyforecast_intermediate_data_path'),
        os.getenv('ieasyhydroforecast_OUTPUT_PATH_ENS'))
    # Test if the output path exists and create it if it doesn't
    if not os.path.exists(OUTPUT_PATH_ENS):
        os.makedirs(OUTPUT_PATH_ENS, exist_ok=True)

    #output_path for the data from the data gateaway
    OUTPUT_PATH_DG = os.path.join(
        os.getenv('ieasyforecast_intermediate_data_path'),
        os.getenv('ieasyhydroforecast_OUTPUT_PATH_DG'))
    # Test if the output path exists and create it if it doesn't
    if not os.path.exists(OUTPUT_PATH_DG):
        os.makedirs(OUTPUT_PATH_DG, exist_ok=True)

    logger.debug(f"OUTPUT_PATH_CM: {OUTPUT_PATH_CM}")
    logger.debug(f"OUTPUT_PATH_ENS: {OUTPUT_PATH_ENS}")
    logger.debug(f"OUTPUT_PATH_DG: {OUTPUT_PATH_DG}")
    logger.debug(f"Path OUTPUT_PATH_DG is valid: {os.path.exists(OUTPUT_PATH_DG)}")

    Q_MAP_PARAM_PATH = os.path.join(
        os.getenv('ieasyhydroforecast_models_and_scalers_path'),
        os.getenv('ieasyhydroforecast_Q_MAP_PARAM_PATH'))
    # Test if the output path exists. Raise an error if it doesn't
    if not os.path.exists(Q_MAP_PARAM_PATH):
        logger.warning(f"Path {Q_MAP_PARAM_PATH} does not exist.\nParameters for quantile mapping of ERA5 and ECMWF ensemble forecast are not available.\nProducing weather data files that are not downscaled.")
        perform_qmapping=False
    else:
        perform_qmapping=True

    CONTROL_MEMBER_HRUS = os.getenv('ieasyhydroforecast_HRU_CONTROL_MEMBER')
    ENSEMBLE_HRUS = os.getenv('ieasyhydroforecast_HRU_ENSEMBLE')

    logger.debug(f"Q_MAP_PARAM_PATH: {Q_MAP_PARAM_PATH}")
    logger.debug(f"CONTROL_MEMBER_HRUS: {CONTROL_MEMBER_HRUS}")
    logger.debug(f"ENSEMBLE_HRUS: {ENSEMBLE_HRUS}")




    #initialize the client
    client = sapphire_dg_client.client.SapphireDGClient(api_key= API_KEY)
    #get the codes for the HRU's
    control_member_hrus = [str(x) for x in CONTROL_MEMBER_HRUS.split(',')]

    hru_ensemble_forecast = [str(x) for x in ENSEMBLE_HRUS.split(',')]



    today = datetime.today().strftime('%Y-%m-%d')
    start_date = datetime.today() - timedelta(days=365)
    start_date = start_date.strftime('%Y-%m-%d')
    yesterday = datetime.today() - timedelta(days=1)
    yesterday = yesterday.strftime('%Y-%m-%d')

    logger.debug(f"Today: {today}, start_date: {start_date}, yesterday: {yesterday}")


    # Read configuration for mapping gateway station codes to hydromet station
    # codes, if file is available:
    # Path to the configuration file
    config_file = os.path.join(
        os.getenv('ieasyforecast_configuration_path'),
        os.getenv('ieasyhydroforecast_config_file_data_gateway_name_twins'))
    logger.debug(f"Data gateway name mapping configuration file: {config_file}")
    # If the file is present, read the configuration
    if os.path.exists(config_file):
        # Read the configuration file
        with open(config_file) as f:
            config = json.load(f)
            # Get the mapping from the configuration
            mapping = config['gateway_name_twins']
            logger.debug(f"Mapping from configuration: {mapping}")
    else:
        logger.debug("No configuration for mapping station codes found.")
        mapping = {}

    #--------------------------------------------------------------------
    # CONTROL MEMBER MAPPING
    #--------------------------------------------------------------------
    logger.debug("Current working directory: %s", os.getcwd())
    for c_m_hru in control_member_hrus:
        #download the control member data
        control_member_era5 = client.operational.get_control_spinup_and_forecast(
            hru_code=c_m_hru,
            date=start_date,
            directory=OUTPUT_PATH_DG
            )
        logger.debug(f"Control Member Data for HRU {c_m_hru} downloaded")
        logger.debug(f"for start_date: {start_date}")
        logger.debug(f"saved to directory: {OUTPUT_PATH_DG}")
        logger.debug(f"Control Member Data Path: {control_member_era5}")

        df_c_m = pd.read_csv(control_member_era5)
        #transform the data file
        transformed_data_file = transform_data_file_control_member(df_c_m)
        transformed_data_file['code'] = transformed_data_file['code'].astype(str)

        #get the parameters if available
        if perform_qmapping:
            P_params_hru = pd.read_csv(os.path.join(Q_MAP_PARAM_PATH, f'HRU{c_m_hru}_P_params.csv'))
            T_params_hru = pd.read_csv(os.path.join(Q_MAP_PARAM_PATH, f'HRU{c_m_hru}_T_params.csv'))

            #transform to string, as the other code is a string
            P_params_hru['code'] = P_params_hru['code'].astype(str)
            T_params_hru['code'] = T_params_hru['code'].astype(str)

            #perform the quantile mapping for the control member for the HRU's without Eleavtion bands
            P_data, T_data = do_quantile_mapping(transformed_data_file, P_params_hru, T_params_hru, ensemble=False)
        else:
            P_data = transformed_data_file[['date', 'P', 'code']].copy()
            T_data = transformed_data_file[['date', 'T', 'code']].copy()

        #check if there are nan values
        if P_data.isnull().values.any():
            print(f"Nan values in P data for HRU {c_m_hru}")
            print("Take Last Observation")
            P_data = P_data.ffill()

        if T_data.isnull().values.any():
            print(f"Nan values in T data for HRU {c_m_hru}")
            print("Take Last Observation")
            T_data = T_data.ffill()

        P_data.to_csv(os.path.join( OUTPUT_PATH_CM, f"{c_m_hru}_P_control_member.csv"), index=False)
        T_data.to_csv(os.path.join( OUTPUT_PATH_CM, f"{c_m_hru}_T_control_member.csv"), index=False)

    #clear memory
    del transformed_data_file

    #--------------------------------------------------------------------
    # ENSEMBLE  MAPPING
    #--------------------------------------------------------------------
    for code_ens in hru_ensemble_forecast:

        print(f"Processing HRU Ensemble: {code_ens} (gateway code)")
        if ENSEMBLE_HRUS == 'None':
            break
        #download the ensemble forecast
        try:
            files_downloaded = client.ecmwf_ens.get_ensemble_forecast(
            hru_code=code_ens,
            date=today,
            models=["pf"],
            directory=OUTPUT_PATH_DG
            )
        except ValueError as e:
            if "Couldn't find any files for the given HRU code, date and models!" in str(e):
                print(f"No data for {today}, trying {yesterday}")
                try:
                    # Attempt to download the ensemble forecast for yesterday
                    files_downloaded = client.ecmwf_ens.get_ensemble_forecast(
                        hru_code=code_ens,
                        date=yesterday,
                        models=["pf"],
                        directory=OUTPUT_PATH_DG
                    )
                except ValueError as e2:
                    print(f"Error on {yesterday}: {e2}")
                    # Handle the second error or re-raise it
                    raise
            else:
                # If it's a different error, re-raise it.
                # The exit value will be 1 (failure) in this case.
                raise

        # A renaming of shapefiles sometimes is required in the data gateway.
        # The user can define name twins for the shapefiles in the data gateway
        # and in the hydromet in the configuration file:
        # ieasyhydroforecast_config_file_data_gateway_name_twins that is read at
        # before the loops.
        # Test if code_ens is in left column of the mapping
        code_ens_data_gateway = code_ens
        if code_ens in mapping.keys():
            logger.debug(f"Mapping found for {code_ens}")
            # If it is, get the name from the right column
            code_ens_data_gateway = code_ens
            code_ens = mapping[code_ens]
            logger.debug(f"Old code: {code_ens_data_gateway}, new code: {code_ens}")

        #merge the ensemble forecast
        combined_ensemble_forecast = merge_ensemble_forecast(files_downloaded)
        # Replace code with the actual code from the mapping (if applicable)
        if code_ens_data_gateway in mapping.keys():
            combined_ensemble_forecast['code'] = code_ens

        combined_ensemble_forecast['code'] = combined_ensemble_forecast['code'].astype(str)

        #load the parameters
        if perform_qmapping:
            P_params_hru = pd.read_csv(os.path.join(Q_MAP_PARAM_PATH, f'HRU{c_m_hru}_P_params.csv'))
            T_params_hru = pd.read_csv(os.path.join(Q_MAP_PARAM_PATH, f'HRU{c_m_hru}_T_params.csv'))

            P_params_hru['code'] = P_params_hru['code'].astype(str)
            T_params_hru['code'] = T_params_hru['code'].astype(str)

            #Perform the quantile mapping for the ensemble members
            P_ensemble, T_ensemble = do_quantile_mapping(combined_ensemble_forecast, P_params_hru, T_params_hru, ensemble=True)
        else:
            P_ensemble = combined_ensemble_forecast[['date', 'P', 'code', 'ensemble_member']].copy()
            T_ensemble = combined_ensemble_forecast[['date', 'T', 'code', 'ensemble_member']].copy()

        #check if there are nan values
        if P_ensemble.isnull().values.any():
            print(f"Nan values in P data (ensemble) for HRU {code_ens}")
            print("Take Last Observation")
            P_ensemble = P_ensemble.ffill()

        if T_ensemble.isnull().values.any():
            print(f"Nan values in T data (ensemle) for HRU {code_ens}")
            print("Take Last Observation")
            T_ensemble = T_ensemble.fffll()

        #save the data
        P_ensemble.to_csv(os.path.join(OUTPUT_PATH_ENS, f"{code_ens}_P_ensemble_forecast.csv"), index=False)
        T_ensemble.to_csv(os.path.join(OUTPUT_PATH_ENS,   f"{code_ens}_T_ensemble_forecast.csv"), index=False)

    if perform_qmapping:
        logger.info("PREPROCESSING OF WEATHER DATA FROM DATA GATWAY DONE. DOWNSCALING WITH QUANTILE MAPPING DONE.")
    else:
        logger.info("PREPROCESSING OF WEATHER DATA FROM DATA GATWAY DONE BUT NO DOWNSCALING DONE.\nERA5-LAND and ECMWF IFS FORECASTS WRITTEN WITHOUT DOWNSCALING.")

    sys.exit(0)

if __name__ == '__main__':
    main()

