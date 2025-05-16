# filepath: test_preprocessing_data.py
# 
# This script is designed to test the data retrieval process from the iEasyHydro SDK or HF SDK.
# It fetches data from the iEasyHydro API, compares it with expected data, and logs the results.
#
# How to run:
# 
import os
import sys
import logging
import pandas as pd  # For comparing DataFrames
from dotenv import load_dotenv
import datetime as dt

# Add the forecast directory to the Python path
script_dir = os.path.dirname(os.path.abspath(__file__))
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')
sys.path.append(forecast_dir)
import setup_library as sl

# Add the src directory to the Python path
src_dir = os.path.join(script_dir, '..', 'src')
sys.path.append(src_dir)
import src as src_module

from ieasyhydro_sdk.sdk import IEasyHydroSDK, IEasyHydroHFSDK


# Configure logging (optional, but good practice)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
# Add a file handler to log to a file
log_file_path = os.path.join(script_dir, 'dataprep_test.log')
file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def get_ieh_sdk():
    """
    Checks if an SSH tunnel is required and if it is running. Then checks if
    access to the iEasyHydro database is available. If so, it returns the
    iEasyHydro SDK object. If not, it returns None.

    Returns:
        IEasyHydroSDK or IEasyHydroHFSDK: The appropriate SDK object.
        bool: True if the backend has access to the database, False otherwise.
    """
    # Add debug logging to verify which organization we're using
    org = os.getenv('ieasyhydroforecast_organization')
    logger.info(f"Getting SDK for organization: {org}")
    
    # Check environment to see if an ssh tunnel is required
    # If so, check if the tunnel is running
    logger.info("Checking if SSH tunnel is required...")
    needs_ssh_tunnel = sl.check_if_ssh_tunnel_is_required()
    if needs_ssh_tunnel:
        logger.info("SSH tunnel is required.")
        # Check if the SSH tunnel is running
        logger.info("Checking if SSH tunnel is running...")
        ssh_tunnel_running = sl.check_local_ssh_tunnels()
        if not ssh_tunnel_running:
            logger.error("SSH tunnel is not running. Please start the SSH tunnel.")
            return None, False
    # Check if we do have access to the database
    if os.getenv('ieasyhydroforecast_connect_to_iEH') == 'True':
        logger.info("Connecting to iEasyHydro SDK")
        ieh_sdk = IEasyHydroSDK()
        has_access_to_db = sl.check_database_access(ieh_sdk)
        if not has_access_to_db:
            ieh_sdk = None
        return ieh_sdk, has_access_to_db
    else:
        if os.getenv('ieasyhydroforecast_organization') == 'demo' or os.getenv('ieasyhydroforecast_organization') == 'tjhm': 
            # Connection to the database is optional for demo organization
            try: 
                ieh_hf_sdk = IEasyHydroHFSDK()
                has_access_to_db = sl.check_database_access(ieh_hf_sdk)
                if not has_access_to_db:
                    ieh_hf_sdk = None
                return ieh_hf_sdk, has_access_to_db
            except Exception as e:
                logger.warning(f"Error while accessing iEasyHydro HF SDK: {e}")
                logger.warning("Continuing without database access.")
                ieh_hf_sdk = None
                has_access_to_db = False
                return ieh_hf_sdk, has_access_to_db
        else:
            logger.info("Connecting to iEasyHydro HF SDK")
            try:
                ieh_hf_sdk = IEasyHydroHFSDK()
                has_access_to_db = sl.check_database_access(ieh_hf_sdk)
                if not has_access_to_db:
                    ieh_hf_sdk = None
                return ieh_hf_sdk, has_access_to_db
            except Exception as e:
                logger.error(f"Error while accessing iEasyHydro HF SDK: {e}")
                raise e
    
def compare_dataframes(df1, df2):
    """
    Compares two pandas DataFrames and returns True if they are equal, False otherwise.
    """
    try:
        pd.testing.assert_frame_equal(df1, df2, check_dtype=False)  # Ignore dtype differences
        return True
    except AssertionError as e:
        logger.error(f"DataFrames are not equal:\n{e}")
        return False

def fetch_expected_data(site_codes, connect_to_ieh):
     """
     Fetches expected data from a "known good" source (e.g., a CSV file, a database, or a simplified API call).
     This is a placeholder; you'll need to implement the actual data fetching logic.
     """
     # Example: Load expected data from a CSV file
     # expected_data = pd.read_csv("path/to/expected_data.csv")
     # return expected_data
     # Replace with your actual implementation
     logger.warning("fetch_expected_data function is not fully implemented. Returning None.")
     return None

def get_expected_site_codes(): 
    if os.getenv('ieasyhydroforecast_organization') == 'kghm': 
        expected_site_codes = ['15194', '16936']
    elif os.getenv('ieasyhydroforecast_organization') == 'tjhm':
        expected_site_codes = ['17050', '17338']
    elif os.getenv('ieasyhydroforecast_organization') == 'demo':
        expected_site_codes = ['12176', '12256']
    return expected_site_codes

def compare_lists(list1, list2):
    """
    Check if list2 is a subset of list1.
    """
    # check if list 1 is empty
    if hasattr(list1, 'size'):  # Check if it's a NumPy array
        if list1.size == 0:
            logger.error("List1 is empty")
            return False
    elif not list1:  # For regular Python lists
        logger.error("List1 is empty")
        return False
    # Check if all elements in list2 are present in list1
    return all(item in list1 for item in list2)

def test_data_retrieval(org, connect_to_ieh, expected_success):
    """
    Tests the data retrieval process from iEasyHydro SDK or HF SDK.
    """
    try:
        if connect_to_ieh:
            logger.info("Testing data retrieval from iEasyHydro SDK...")
            try: 
                ieh_sdk, has_access_to_db = get_ieh_sdk()  
                success = has_access_to_db and ieh_sdk is not None
            except Exception as e:
                logger.error(f"Error while accessing iEasyHydro SDK: {e}")
                return False
            if success != expected_success:
                logger.error(f"Expected iEH access to be {expected_success}, but got {success}")
                return False
            
            if success:
                fc_sites_pentad, site_codes_pentad = sl.get_pentadal_forecast_sites(ieh_sdk, backend_has_access_to_db=has_access_to_db)
            
                # Expected site_codes_pentad
                expected_site_codes_pentad = get_expected_site_codes()

                # Compare the site codes
                if not compare_lists(site_codes_pentad, expected_site_codes_pentad):
                    logger.error(f"Expected sites not found in the site list")
                    logger.error(f"Site codes found: {site_codes_pentad}")
                    logger.error(f"Expected site codes: {expected_site_codes_pentad}")
                    return False
                else: 
                    logger.info(f"Expected sites found in the site list")

                #runoff_data = src_module.get_runoff_data_for_sites(
                #     ieh_sdk,
                #     date_col='date',
                #     discharge_col='discharge',
                #     name_col='name',
                #     code_col='code',
                #     site_list=fc_sites_pentad,
                #     code_list=site_codes_pentad,
                # )
                #expected_data = fetch_expected_data(site_codes_pentad, connect_to_ieh)
            else: 
                logger.info("Skipping site code comparison because iEH access has failed as expected.")

        else:
            logger.info("Testing data retrieval from iEasyHydro HF SDK...")
            try: 
                ieh_hf_sdk, has_access_to_db = get_ieh_sdk()  
                success = has_access_to_db and ieh_hf_sdk is not None
            except Exception as e:
                logger.error(f"Error while accessing iEasyHydro HF SDK: {e}")
                return False
            if success != expected_success:
                logger.error(f"Expected iEH HF access to be {expected_success}, but got {success}")
                return False

            if success:
                fc_sites_pentad, site_codes_pentad, site_ids_pentad = sl.get_pentadal_forecast_sites_from_HF_SDK(ieh_hf_sdk)
            
                # Expected site_codes_pentad
                expected_site_codes_pentad = get_expected_site_codes()

                # Compare the site codes
                if not compare_lists(site_codes_pentad, expected_site_codes_pentad):
                    logger.error(f"Expected sites not found in the site list")
                    logger.error(f"Site codes found: {site_codes_pentad}")
                    logger.error(f"Expected site codes: {expected_site_codes_pentad}")
                    return False
                else: 
                    logger.info(f"Expected sites found in the site list")

                #runoff_data = src_module.get_runoff_data_for_sites_HF(
                #     ieh_hf_sdk,
                #     date_col='date',
                #     discharge_col='discharge',
                #     code_col='code',
                #     site_list=fc_sites_pentad,
                #     code_list=site_codes_pentad,
                #     id_list=site_ids_pentad,
                #     target_timezone=sl.get_local_timezone_from_env(),
                # )
                #expected_data = fetch_expected_data(site_codes_pentad, connect_to_ieh)
            else: 
                logger.info("Skipping site code comparison because iEH HF access has failed as expected.")


        # TODO: Add code to fetch actual data from ieasyhydro.org or hf.ieasyhydro.org (using API or other means)
        # actual_data = ...  # DataFrame with actual data from your preprocessing_runoff module

        # Compare the DataFrames
        #if compare_dataframes(runoff_data, expected_data):
        #    logger.info("Data retrieval test passed!")
        #    return True
        #else:
        #    logger.error("Data retrieval test failed!")
        #    return False

        return True  # Test passed if no assertions failed

    except Exception as e:
        logger.exception(f"An error occurred during the test: {e}")
        return False

def test_validation_of_env_variables(org, expected_success):
    """
    Tests the validation of environment variables.
    """
    try:
        # Validate environment variables
        validation = sl.validate_environment_variables()
        if validation is not None:
            logger.error("Environment variables validation failed.")
            return False

        # Check if the organization is set correctly
        if os.getenv('ieasyhydroforecast_organization') != org:
            logger.error(f"Expected organization: {org}, but got: {os.getenv('ieasyhydroforecast_organization')}")
            return False

        # Check if the connection to iEH is set correctly
        if os.getenv('ieasyhydroforecast_connect_to_iEH') == 'True' and expected_success:
            logger.info("Environment variables validation passed.")
            return True
        else:
            logger.info("Environment variables validation passed, but connection to iEH is not expected.")
            return True
        
    except EnvironmentError as e:
        # For tjhm with iEH=True, this error is expected
        if not expected_success:
            logger.info(f"Expected validation failure: {e}")
            return True  # Return true because this failure was expected
        else:
            logger.error(f"Unexpected validation error: {e}")
            return False

    except Exception as e:
        logger.error(f"An error occurred during environment variable validation: {e}")
        return False

if __name__ == "__main__":
    
    # Configure the test environment
    # Add boolean values to indicate if we expect the test to be successful or not
    test_organizations = {
        'kghm': {'ieh': True, 'iehhf': True},
        'tjhm': {'ieh': False, 'iehhf': True},  # Only HF SDK is available
        'demo': {'ieh': False, 'iehhf': False}
    }
    # EDIT PATHS TO ENV FILES
    # These paths should point to the .env files for each organization
    test_env_paths = {
        'kghm': os.path.expanduser('~/Documents/GitHub/sensitive_data_forecast_tools/config/.env_develop_kghm'), 
        'tjhm': os.path.expanduser('~/Documents/GitHub/taj_data_forecast_tools/config/.env_develop_tjhm'),
        'demo': os.path.expanduser('../../config/.env')
    }

    overall_test_result = True  # Initialize overall test result
    
    for org, expected_success in test_organizations.items():
        logger.info("\n")
        logger.info("="*60)
        logger.info(f"Testing data retrieval for organization: {org}")
        logger.info(f"environment file path: {test_env_paths[org]}")
        
        # Load environment variables
        original_ieasyhydroforecast_env_file_path = os.getenv('ieasyhydroforecast_env_file_path')
        os.environ['ieasyhydroforecast_env_file_path'] = test_env_paths[org]
        sl.load_environment()

        # Store the original value of ieasyforecast_configuration_path
        original_ieasyhydroforecast_organization = os.getenv('ieasyhydroforecast_organization')
        original_ieasyforecast_configuration_path = os.getenv('ieasyforecast_configuration_path')
        original_ieasyforecast_daily_discharge_path = os.getenv('ieasyforecast_daily_discharge_path')
        original_ieasyhydroforecast_connect_to_iEH = os.getenv('ieasyhydroforecast_connect_to_iEH')

        # Need to temporarily change paths in the environment variables. 
        if original_ieasyhydroforecast_organization:
            os.environ['ieasyhydroforecast_organization'] = org
        if original_ieasyforecast_configuration_path:
            os.environ['ieasyforecast_configuration_path'] = os.path.join(script_dir, '..', original_ieasyforecast_configuration_path)
        if original_ieasyforecast_daily_discharge_path:
            os.environ['ieasyforecast_daily_discharge_path'] = os.path.join(script_dir, '..', original_ieasyforecast_daily_discharge_path)

        # Run the test for iEH
        logger.info(f"== Test connecting to iEH ==")
        os.environ['ieasyhydroforecast_connect_to_iEH'] = 'True'
        os.environ['ieasyhydroforecast_ssh_to_iEH'] = 'True'
        validation = test_validation_of_env_variables(org, expected_success['ieh'])
        if not validation:
            logger.error("Environment variables validation failed.")
            overall_test_result = False
            continue
        test_result_ieh = test_data_retrieval(org, True, expected_success['ieh'])
        if not test_result_ieh:
            logger.error(f"iEH test failed for organization: {org}")
            overall_test_result = False

        # Run the test for iEH HF
        logger.info(f"== Test connecting to iEH HF ==")
        os.environ['ieasyhydroforecast_connect_to_iEH'] = 'False'
        os.environ['ieasyhydroforecast_ssh_to_iEH'] = 'False'
        validation = test_validation_of_env_variables(org, expected_success['iehhf'])
        if not validation:
            logger.error("Environment variables validation failed.")
            overall_test_result = False
            continue
        test_result_iehhf = test_data_retrieval(org, False, expected_success['iehhf'])
        if not test_result_iehhf:
            logger.error(f"iEH HF test failed for organization: {org}")
            overall_test_result = False

        # Clear the environment variables
        os.environ.clear()

    if overall_test_result:
        logger.info("All tests passed!")
        sys.exit(0)
    else:
        logger.error("Some tests failed!")
        sys.exit(1)