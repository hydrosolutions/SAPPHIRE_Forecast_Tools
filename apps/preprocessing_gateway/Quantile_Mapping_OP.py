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
# ieasyhydroforecast_env_file_path=/path/to/.env python Quantile_Mapping_OP.py

# Author: Sandro Hunziker



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

import dg_utils

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
logging.basicConfig(level=logging.INFO)
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
    # Check if files_downloaded is empty
    if not files_downloaded:
        logger.error("No files downloaded. Exiting program.")
        sys.exit(1)

    #combine the data
    P_ensemble = pd.DataFrame()
    T_ensemble = pd.DataFrame()
    for file in files_downloaded:
        elements = file.split("_")
        # From the second last element, remove the first 3 characters ('HRU')
        HRU_CODE = elements[-2][3:]
        #HRU_CODE = elements[-2][-5:]
        variable = elements[-1].split(".")[0]
        ensemble_member = elements[-3][3:]
        #read the data file
        data_file = pd.read_csv(file)
        #transform the data file
        transformed_data_file = transform_data_file_ensemble_member(data_file, HRU_CODE)
        transformed_data_file['ensemble_member'] = int(ensemble_member)

        if variable == "tp":
            P_ensemble = pd.concat([P_ensemble, transformed_data_file], axis = 0)
        elif variable == "2t":
            T_ensemble = pd.concat([T_ensemble, transformed_data_file], axis = 0)
        else:
            logger.warning(f"Variable {variable} not recognized. Skipping file {file}.")
            continue

    # Test if P_ensemble and T_ensemble are empty
    if P_ensemble.empty:
        logger.error("No precipitation data found in the ensemble forecast files.")
        sys.exit(1)
    if T_ensemble.empty:
        logger.error("No temperature data found in the ensemble forecast files.")
        sys.exit(1)

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
    logger.debug(f"Iterating over the control member HRUs: {control_member_hrus}")
    for c_m_hru in control_member_hrus:
        #download the control member data
        logger.info(f"Processing control member for HRU: {c_m_hru}")
        # Initialize control_member_era5 to None
        control_member_era5 = None
        try:
            control_member_era5 = client.operational.get_control_spinup_and_forecast(
                hru_code=c_m_hru,
                date=start_date,
                directory=OUTPUT_PATH_DG
                )

        except Exception as e:
            if "Operational data for HRU" in str(e):
                logger.error(f"Exiting the program due to error: {e}")
                sys.exit(1)


        # If control_member_era5 is empty, raise an error
        if not control_member_era5:
            logger.error(f"Control Member Data for HRU {c_m_hru} not available.")
            sys.exit(1)

        logger.debug(f"Control Member Data for HRU {c_m_hru} downloaded")
        logger.debug(f"for start_date: {start_date}")
        logger.debug(f"saved to directory: {OUTPUT_PATH_DG}")
        logger.debug(f"Control Member Data Path: {control_member_era5}")

        df_c_m = pd.read_csv(control_member_era5)

        #transform the data file
        transformed_data_file = dg_utils.transform_data_file_control_member(df_c_m)
        transformed_data_file['code'] = transformed_data_file['code'].astype(str)

        #get the parameters if available
        if perform_qmapping:
            logger.info("Performing Quantile Mapping for Control Member")
            P_params_hru = pd.read_csv(os.path.join(Q_MAP_PARAM_PATH, f'HRU{c_m_hru}_P_params.csv'))
            T_params_hru = pd.read_csv(os.path.join(Q_MAP_PARAM_PATH, f'HRU{c_m_hru}_T_params.csv'))

            #transform to string, as the other code is a string
            P_params_hru['code'] = P_params_hru['code'].astype(str)
            T_params_hru['code'] = T_params_hru['code'].astype(str)

            #perform the quantile mapping for the control member for the HRU's without Eleavtion bands
            P_data, T_data = dg_utils.do_quantile_mapping(transformed_data_file, P_params_hru, T_params_hru, ensemble=False)
            logger.info("Quantile Mapping for Control Member Done.")
        else:
            P_data = transformed_data_file[['date', 'P', 'code']].copy()
            T_data = transformed_data_file[['date', 'T', 'code']].copy()

        #check if there are nan values

        #TODO: check with Nikola what to do with Nan values, or what the expected amount of Nan values is
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
        print(f"Storing files downloaded to {OUTPUT_PATH_DG}")
        if ENSEMBLE_HRUS == 'None':
            break
        #download the ensemble forecast
        try:
            files_downloaded = []
            for model in range(1, 51):
                files = client.ecmwf_ens.get_ensemble_forecast(
                    hru_code=code_ens,
                    date=today,
                    models=[str(model)],
                    directory=OUTPUT_PATH_DG
                )
                files_downloaded.append(files)
            # Unnest the list of lists
            files_downloaded = [item for sublist in files_downloaded for item in sublist]
            # May cause timeout errors from gateway server side. Better to download one by one.
            #files_downloaded = client.ecmwf_ens.get_ensemble_forecast(
            #    hru_code=code_ens,
            #    date=today,
            #    models=["pf"],
            #    directory=OUTPUT_PATH_DG
            #)
        except ValueError as e:
            if "Couldn't find any files for the given HRU code, date and models!" in str(e):
                print(f"No data for {today}, trying {yesterday}")
                try:
                    files_downloaded = []
                    for model in range(1, 51):
                        files = client.ecmwf_ens.get_ensemble_forecast(
                            hru_code=code_ens,
                            date=yesterday,
                            models=[str(model)],
                            directory=OUTPUT_PATH_DG
                        )
                        files_downloaded.append(files)
                    # Unnest the list of lists
                    files_downloaded = [item for sublist in files_downloaded for item in sublist]
                    # Attempt to download the ensemble forecast for yesterday
                    #files_downloaded = client.ecmwf_ens.get_ensemble_forecast(
                    #    hru_code=code_ens,
                    #    date=yesterday,
                    #    models=["pf"],
                    #    directory=OUTPUT_PATH_DG
                    #)
                except ValueError as e2:
                    print(f"Error for date {yesterday}: {e2}")
                    print(traceback.format_exc())
                    # Handle the second error or re-raise it
                    sys.exit(1)
            else:
                # If it's a different error, re-raise it.
                # The exit value will be 1 (failure) in this case.
                print(f"Unexpected error for date {today}: {e}")
                print(traceback.format_exc())
                sys.exit(1)

        #print(f"Files downloaded: {files_downloaded}")

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
            P_ensemble, T_ensemble = dg_utils.do_quantile_mapping(combined_ensemble_forecast, P_params_hru, T_params_hru, ensemble=True)
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
            T_ensemble = T_ensemble.ffill()

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

