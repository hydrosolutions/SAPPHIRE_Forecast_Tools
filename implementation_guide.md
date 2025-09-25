# Step-by-Step Implementation Guide: Preprocessing Runoff Maintenance/Forecast Mode

## Overview
This guide implements a maintenance/forecast mode separation for the preprocessing_runoff module to optimize performance. The current system processes 50+ days of data on every run, taking several minutes. The new system will:
- **Maintenance mode**: Process historical data (today-50 to today-3) overnight
- **Forecast mode**: Process only recent data (today-2 to today) during daily runs

## Prerequisites
- Existing SAPPHIRE forecast tools codebase
- Understanding of Docker, Luigi pipeline, and Python
- Access to the project's .env configuration files

## Step 1: Create Mode-Specific Functions in src.py

### Task: Add new time range functions to support maintenance and forecast modes

**File to modify**: `apps/preprocessing_runoff/src/src.py`

**Action**: Add these new functions after the existing `get_local_time_range_for_daily_average_runoff_request` function:

```python
def get_local_time_range_for_maintenance_runoff_request(target_timezone, maintenance_window_size=47):
    """
    Calculates UTC datetime objects for maintenance mode data request (today-50 to today-3).
    
    Parameters:
    target_timezone (pytz.timezone): The target time zone
    maintenance_window_size (int): Days to process during maintenance (default 47 = today-50 to today-3)
    
    Returns:
    tuple: Start and end times as UTC datetime objects for maintenance processing
    """
    if not target_timezone:
        return None, None

    now_local = dt.datetime.now(target_timezone)
    today = now_local.date()

    # End time: 3 days ago at 20:00
    end_date = today - timedelta(days=3)
    end_time_utc = dt.datetime(end_date.year, end_date.month, end_date.day, 20, 0, tzinfo=dt.timezone.utc)

    # Start time: 50 days ago at 20:00  
    start_date = today - timedelta(days=50)
    start_time_utc = dt.datetime(start_date.year, start_date.month, start_date.day, 20, 0, tzinfo=dt.timezone.utc)

    return start_time_utc, end_time_utc

def get_local_time_range_for_forecast_runoff_request(target_timezone):
    """
    Calculates UTC datetime objects for forecast mode data request (today-2 to today).
    
    Parameters:
    target_timezone (pytz.timezone): The target time zone
    
    Returns:
    tuple: Start and end times as UTC datetime objects for forecast processing
    """
    if not target_timezone:
        return None, None

    now_local = dt.datetime.now(target_timezone)
    today = now_local.date()

    # End time: today at 20:00
    end_time_utc = dt.datetime(today.year, today.month, today.day, 20, 0, tzinfo=dt.timezone.utc)

    # Start time: 2 days ago at 20:00
    start_date = today - timedelta(days=2)
    start_time_utc = dt.datetime(start_date.year, start_date.month, start_date.day, 20, 0, tzinfo=dt.timezone.utc)

    return start_time_utc, end_time_utc
```

**Action**: Add functions for saving and loading maintenance data:

```python
def save_maintenance_data(data, filename_prefix="maintenance_runoff"):
    """
    Save maintenance-processed data to intermediate files.
    
    Parameters:
    data (pd.DataFrame): The processed maintenance data
    filename_prefix (str): Prefix for the saved files
    
    Returns:
    str: Path to saved file or None if error
    """
    try:
        import setup_library as sl
        
        # Get the intermediate data path
        intermediate_path = sl.get_absolute_path(os.getenv('ieasyforecast_intermediate_data_path'))
        maintenance_dir = os.path.join(intermediate_path, 'maintenance_data')
        
        # Create directory if it doesn't exist
        os.makedirs(maintenance_dir, exist_ok=True)
        
        # Create filename with current date
        today = dt.datetime.now().strftime('%Y%m%d')
        filepath = os.path.join(maintenance_dir, f"{filename_prefix}_{today}.csv")
        
        # Save data
        data.to_csv(filepath, index=False)
        logger.info(f"Maintenance data saved to: {filepath}")
        return filepath
        
    except Exception as e:
        logger.error(f"Error saving maintenance data: {e}")
        return None

def load_maintenance_data(filename_prefix="maintenance_runoff", max_age_days=1):
    """
    Load the most recent maintenance data if available and not too old.
    
    Parameters:
    filename_prefix (str): Prefix of the saved files to look for
    max_age_days (int): Maximum age of maintenance data to consider valid
    
    Returns:
    pd.DataFrame or None: Loaded maintenance data or None if not available/too old
    """
    try:
        import setup_library as sl
        
        # Get the intermediate data path
        intermediate_path = sl.get_absolute_path(os.getenv('ieasyforecast_intermediate_data_path'))
        maintenance_dir = os.path.join(intermediate_path, 'maintenance_data')
        
        if not os.path.exists(maintenance_dir):
            logger.info("No maintenance data directory found")
            return None
        
        # Look for maintenance files
        pattern = os.path.join(maintenance_dir, f"{filename_prefix}_*.csv")
        files = glob.glob(pattern)
        
        if not files:
            logger.info("No maintenance data files found")
            return None
        
        # Get the most recent file
        latest_file = max(files, key=os.path.getctime)
        
        # Check if file is recent enough
        file_age = dt.datetime.now() - dt.datetime.fromtimestamp(os.path.getctime(latest_file))
        if file_age.days > max_age_days:
            logger.warning(f"Maintenance data is {file_age.days} days old, too old to use")
            return None
        
        # Load and return data
        data = pd.read_csv(latest_file)
        data = standardize_date_column(data, 'date')
        logger.info(f"Loaded maintenance data from: {latest_file}")
        return data
        
    except Exception as e:
        logger.error(f"Error loading maintenance data: {e}")
        return None
```

## Step 2: Create Maintenance Script

### Task: Create preprocessing_runoff_maintenance.py for overnight processing

**File to create**: `apps/preprocessing_runoff/preprocessing_runoff_maintenance.py`

**Content**:
```python
#!/usr/bin/env python3
"""
Maintenance mode preprocessing for SAPPHIRE forecast tools.
Processes historical runoff data (today-50 to today-3 days) and saves results for fast daily updates.

Usage:
ieasyhydroforecast_env_file_path=<path/to/.env> python preprocessing_runoff_maintenance.py
"""

import os
import sys
import time
import logging
from logging.handlers import TimedRotatingFileHandler

# Add paths for local imports
script_dir = os.path.dirname(os.path.abspath(__file__))
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')
sys.path.append(forecast_dir)

# Local imports
from src import src
import setup_library as sl
from ieasyhydro_sdk.sdk import IEasyHydroSDK, IEasyHydroHFSDK

# Configure logging
logging.basicConfig(level=logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

if not os.path.exists('logs'):
    os.makedirs('logs')

file_handler = TimedRotatingFileHandler('logs/maintenance_log', when='midnight', interval=1, backupCount=30)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.INFO)

logger = logging.getLogger()
logger.handlers = []
logger.addHandler(file_handler)
logger.addHandler(console_handler)
logger.info = print

def get_ieh_sdk():
    """Get appropriate SDK object based on configuration."""
    # [Copy the exact same function from preprocessing_runoff.py]
    needs_ssh_tunnel = sl.check_if_ssh_tunnel_is_required()
    if needs_ssh_tunnel:
        ssh_tunnel_running = sl.check_local_ssh_tunnels()
        if not ssh_tunnel_running:
            logger.error("SSH tunnel is not running. Please start the SSH tunnel.")
            sys.exit(1)
    
    if os.getenv('ieasyhydroforecast_connect_to_iEH') == 'True':
        logger.info("Connecting to iEasyHydro SDK")
        ieh_sdk = IEasyHydroSDK()
        has_access_to_db = sl.check_database_access(ieh_sdk)
        if not has_access_to_db:
            ieh_sdk = None
        return ieh_sdk, has_access_to_db
    else:
        if os.getenv('ieasyhydroforecast_organization') == 'demo': 
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
                sys.exit(1)

def main():
    """Main maintenance processing function."""
    logger.info("Starting MAINTENANCE mode preprocessing of runoff data")
    overall_start_time = time.time()
    
    # Load environment
    sl.load_environment()
    target_time_zone = sl.get_local_timezone_from_env()
    logger.info(f"Target time zone: {target_time_zone}")
    
    # Get forecast sites
    start_time = time.time()
    if os.getenv('ieasyhydroforecast_connect_to_iEH') == 'True':
        logger.info("Reading forecast sites from iEasyHydro SDK")
        ieh_sdk, has_access_to_db = get_ieh_sdk()
        fc_sites_pentad, site_codes_pentad = sl.get_pentadal_forecast_sites(ieh_sdk, backend_has_access_to_db=has_access_to_db)
        fc_sites_decad, site_codes_decad = sl.get_decadal_forecast_sites_from_pentadal_sites(fc_sites_pentad, site_codes_pentad)
        logger.info("... done reading forecast sites from iEasyHydro SDK")
    else:
        logger.info("Reading forecast sites from iEasyHydro HF SDK")
        ieh_hf_sdk, has_access_to_db = get_ieh_sdk()
        fc_sites_pentad, site_codes_pentad, site_ids_pentad = sl.get_pentadal_forecast_sites_from_HF_SDK(ieh_hf_sdk)
        fc_sites_decad, site_codes_decad, site_ids_decad = sl.get_decadal_forecast_sites_from_HF_SDK(ieh_hf_sdk)
        logger.info("... done reading forecast sites from iEasyHydro HF SDK")
    
    end_time = time.time()
    time_get_forecast_sites = end_time - start_time
    
    # Process PENTAD data (maintenance mode)
    logger.info("Processing PENTAD sites data in MAINTENANCE mode (today-50 to today-3)")
    start_time = time.time()
    
    # Get runoff data using maintenance time range
    if os.getenv('ieasyhydroforecast_connect_to_iEH') == 'True':
        runoff_data_pentad = src.get_runoff_data_for_sites(
            ieh_sdk, 
            site_list=fc_sites_pentad,
            site_code_list=site_codes_pentad,
            use_maintenance_range=True  # New parameter to use maintenance range
        )
    else:
        runoff_data_pentad = src.get_runoff_data_for_sites_HF(
            ieh_hf_sdk,
            site_list=fc_sites_pentad,
            site_code_list=site_codes_pentad,
            site_id_list=site_ids_pentad,
            use_maintenance_range=True  # New parameter to use maintenance range
        )
    
    end_time = time.time()
    time_get_runoff_data = end_time - start_time
    
    # Filter for outliers
    start_time = time.time()
    filtered_data_pentad = src.filter_roughly_for_outliers(
        runoff_data_pentad,
        site_list=fc_sites_pentad,
        site_code_list=site_codes_pentad
    )
    end_time = time.time()
    time_filter_outliers = end_time - start_time
    
    # Process DECAD data (maintenance mode)
    logger.info("Processing DECAD sites data in MAINTENANCE mode")
    start_time = time.time()
    
    if os.getenv('ieasyhydroforecast_connect_to_iEH') == 'True':
        runoff_data_decad = src.get_runoff_data_for_sites(
            ieh_sdk,
            site_list=fc_sites_decad,
            site_code_list=site_codes_decad,
            use_maintenance_range=True
        )
    else:
        runoff_data_decad = src.get_runoff_data_for_sites_HF(
            ieh_hf_sdk,
            site_list=fc_sites_decad,
            site_code_list=site_codes_decad,
            site_id_list=site_ids_decad,
            use_maintenance_range=True
        )
    
    filtered_data_decad = src.filter_roughly_for_outliers(
        runoff_data_decad,
        site_list=fc_sites_decad,
        site_code_list=site_codes_decad
    )
    end_time = time.time()
    time_process_decad = end_time - start_time
    
    # Combine and save maintenance data
    start_time = time.time()
    all_maintenance_data = pd.concat([filtered_data_pentad, filtered_data_decad], ignore_index=True)
    
    # Save maintenance data
    saved_path = src.save_maintenance_data(all_maintenance_data, "daily_runoff_maintenance")
    if saved_path:
        logger.info(f"Maintenance data saved successfully to: {saved_path}")
    else:
        logger.error("Failed to save maintenance data")
        sys.exit(1)
    
    end_time = time.time()
    time_save_data = end_time - start_time
    
    # Log timing information
    overall_end_time = time.time()
    logger.info(f"MAINTENANCE mode completed successfully")
    logger.info(f"Overall time: {overall_end_time - overall_start_time:.2f} seconds")
    logger.info(f"Time to get forecast sites: {time_get_forecast_sites:.2f} seconds")
    logger.info(f"Time to get runoff data: {time_get_runoff_data:.2f} seconds")
    logger.info(f"Time to filter outliers: {time_filter_outliers:.2f} seconds")
    logger.info(f"Time to process decad: {time_process_decad:.2f} seconds")
    logger.info(f"Time to save data: {time_save_data:.2f} seconds")

if __name__ == "__main__":
    main()
```

## Step 3: Create Forecast Script

### Task: Create preprocessing_runoff_forecast.py for fast daily updates

**File to create**: `apps/preprocessing_runoff/preprocessing_runoff_forecast.py`

**Content**:
```python
#!/usr/bin/env python3
"""
Forecast mode preprocessing for SAPPHIRE forecast tools.
Processes only recent runoff data (today-2 to today) and combines with maintenance data.

Usage:
ieasyhydroforecast_env_file_path=<path/to/.env> python preprocessing_runoff_forecast.py
"""

import os
import sys
import time
import logging
import pandas as pd
from logging.handlers import TimedRotatingFileHandler

# Add paths for local imports
script_dir = os.path.dirname(os.path.abspath(__file__))
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')
sys.path.append(forecast_dir)

# Local imports
from src import src
import setup_library as sl
from ieasyhydro_sdk.sdk import IEasyHydroSDK, IEasyHydroHFSDK

# Configure logging (same as maintenance script)
logging.basicConfig(level=logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

if not os.path.exists('logs'):
    os.makedirs('logs')

file_handler = TimedRotatingFileHandler('logs/forecast_log', when='midnight', interval=1, backupCount=30)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.INFO)

logger = logging.getLogger()
logger.handlers = []
logger.addHandler(file_handler)
logger.addHandler(console_handler)
logger.info = print

def get_ieh_sdk():
    """Get appropriate SDK object based on configuration."""
    # [Same implementation as maintenance script]
    # ... (copy from maintenance script)

def main():
    """Main forecast processing function."""
    logger.info("Starting FORECAST mode preprocessing of runoff data")
    overall_start_time = time.time()
    
    # Load environment
    sl.load_environment()
    target_time_zone = sl.get_local_timezone_from_env()
    logger.info(f"Target time zone: {target_time_zone}")
    
    # Try to load maintenance data first
    start_time = time.time()
    maintenance_data = src.load_maintenance_data("daily_runoff_maintenance")
    if maintenance_data is None:
        logger.warning("No valid maintenance data found, falling back to full processing")
        # Fall back to original full processing
        from preprocessing_runoff import main as original_main
        return original_main()
    
    logger.info(f"Loaded maintenance data with {len(maintenance_data)} records")
    end_time = time.time()
    time_load_maintenance = end_time - start_time
    
    # Get forecast sites
    start_time = time.time()
    if os.getenv('ieasyhydroforecast_connect_to_iEH') == 'True':
        ieh_sdk, has_access_to_db = get_ieh_sdk()
        fc_sites_pentad, site_codes_pentad = sl.get_pentadal_forecast_sites(ieh_sdk, backend_has_access_to_db=has_access_to_db)
        fc_sites_decad, site_codes_decad = sl.get_decadal_forecast_sites_from_pentadal_sites(fc_sites_pentad, site_codes_pentad)
    else:
        ieh_hf_sdk, has_access_to_db = get_ieh_sdk()
        fc_sites_pentad, site_codes_pentad, site_ids_pentad = sl.get_pentadal_forecast_sites_from_HF_SDK(ieh_hf_sdk)
        fc_sites_decad, site_codes_decad, site_ids_decad = sl.get_decadal_forecast_sites_from_HF_SDK(ieh_hf_sdk)
    
    end_time = time.time()
    time_get_forecast_sites = end_time - start_time
    
    # Process only recent data (today-2 to today)
    logger.info("Processing recent data in FORECAST mode (today-2 to today)")
    start_time = time.time()
    
    # Get recent runoff data
    if os.getenv('ieasyhydroforecast_connect_to_iEH') == 'True':
        recent_data_pentad = src.get_runoff_data_for_sites(
            ieh_sdk,
            site_list=fc_sites_pentad,
            site_code_list=site_codes_pentad,
            use_forecast_range=True  # New parameter for forecast range
        )
        recent_data_decad = src.get_runoff_data_for_sites(
            ieh_sdk,
            site_list=fc_sites_decad,
            site_code_list=site_codes_decad,
            use_forecast_range=True
        )
    else:
        recent_data_pentad = src.get_runoff_data_for_sites_HF(
            ieh_hf_sdk,
            site_list=fc_sites_pentad,
            site_code_list=site_codes_pentad,
            site_id_list=site_ids_pentad,
            use_forecast_range=True
        )
        recent_data_decad = src.get_runoff_data_for_sites_HF(
            ieh_hf_sdk,
            site_list=fc_sites_decad,
            site_code_list=site_codes_decad,
            site_id_list=site_ids_decad,
            use_forecast_range=True
        )
    
    # Combine recent data
    recent_data = pd.concat([recent_data_pentad, recent_data_decad], ignore_index=True)
    
    # Filter recent data for outliers
    all_sites = fc_sites_pentad + fc_sites_decad
    all_codes = site_codes_pentad + site_codes_decad
    filtered_recent_data = src.filter_roughly_for_outliers(
        recent_data,
        site_list=all_sites,
        site_code_list=all_codes
    )
    
    end_time = time.time()
    time_get_recent_data = end_time - start_time
    
    # Combine maintenance and recent data
    start_time = time.time()
    
    # Remove any overlap between maintenance and recent data (keep recent)
    maintenance_cutoff = pd.Timestamp.now().date() - pd.Timedelta(days=2)
    maintenance_data_cleaned = maintenance_data[maintenance_data['date'] < maintenance_cutoff]
    
    # Combine datasets
    combined_data = pd.concat([maintenance_data_cleaned, filtered_recent_data], ignore_index=True)
    combined_data = combined_data.drop_duplicates(subset=['code', 'date'], keep='last')
    combined_data = combined_data.sort_values(['code', 'date']).reset_index(drop=True)
    
    logger.info(f"Combined data: {len(maintenance_data_cleaned)} maintenance + {len(filtered_recent_data)} recent = {len(combined_data)} total records")
    
    end_time = time.time()
    time_combine_data = end_time - start_time
    
    # Create hydrograph data
    start_time = time.time()
    hydrograph = src.from_daily_time_series_to_hydrograph(
        combined_data,
        site_list=all_sites,
        site_code_list=all_codes
    )
    end_time = time.time()
    time_create_hydrograph = end_time - start_time
    
    # Save final outputs
    start_time = time.time()
    ret1 = src.write_daily_time_series_data_to_csv(
        data=combined_data,
        column_list=['code', 'date', 'discharge']
    )
    
    ret2 = src.write_daily_hydrograph_data_to_csv(
        data=hydrograph,
        column_list=hydrograph.columns.tolist()
    )
    
    if ret1 is None and ret2 is None:
        logger.info("All data written successfully.")
        success = True
    else:
        logger.error("Failed to write output data.")
        success = False
    
    end_time = time.time()
    time_write_data = end_time - start_time
    
    # Log timing information
    overall_end_time = time.time()
    logger.info(f"FORECAST mode completed successfully")
    logger.info(f"Overall time: {overall_end_time - overall_start_time:.2f} seconds")
    logger.info(f"Time to load maintenance data: {time_load_maintenance:.2f} seconds")
    logger.info(f"Time to get forecast sites: {time_get_forecast_sites:.2f} seconds")
    logger.info(f"Time to get recent data: {time_get_recent_data:.2f} seconds")
    logger.info(f"Time to combine data: {time_combine_data:.2f} seconds")
    logger.info(f"Time to create hydrograph: {time_create_hydrograph:.2f} seconds")
    logger.info(f"Time to write data: {time_write_data:.2f} seconds")
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
```

## Step 4: Modify Main preprocessing_runoff.py Script

### Task: Update the main script to check RUN_MODE and delegate to appropriate mode

**File to modify**: `apps/preprocessing_runoff/preprocessing_runoff.py`

**Action**: Replace the `main()` function with:

```python
def main():
    """
    Pre-processing of discharge data for the SAPPHIRE forecast tools.
    
    Checks RUN_MODE environment variable to determine processing mode:
    - 'maintenance': Process historical data (today-50 to today-3) 
    - 'forecast' or default: Process recent data (today-2 to today) with maintenance data
    """
    
    # Check RUN_MODE environment variable
    run_mode = os.getenv('RUN_MODE', 'forecast').lower()
    
    if run_mode == 'maintenance':
        logger.info("Running in MAINTENANCE mode")
        # Import and run maintenance script
        try:
            from preprocessing_runoff_maintenance import main as maintenance_main
            return maintenance_main()
        except ImportError as e:
            logger.error(f"Could not import maintenance module: {e}")
            sys.exit(1)
    else:
        logger.info("Running in FORECAST mode")
        # Import and run forecast script
        try:
            from preprocessing_runoff_forecast import main as forecast_main
            return forecast_main()
        except ImportError as e:
            logger.error(f"Could not import forecast module: {e}")
            # Fall back to original implementation
            logger.warning("Falling back to original full processing")
            return original_main()

def original_main():
    """
    Original preprocessing implementation (kept as fallback).
    """
    # [Move the current main() function content here as backup]
    # This preserves the existing functionality in case of issues
    
    # ... (copy all existing main() function content here)
```

## Step 5: Update src.py Functions for Mode Support

### Task: Modify existing functions to support maintenance and forecast ranges

**File to modify**: `apps/preprocessing_runoff/src/src.py`

**Action**: Update the `get_runoff_data_for_sites` and `get_runoff_data_for_sites_HF` functions to support new parameters:

Find the function definitions and add these parameters:

```python
def get_runoff_data_for_sites(ieh_sdk=None, date_col='date',
                             discharge_col='discharge', name_col='name', code_col='code',
                             site_list=None, site_code_list=None,
                             use_maintenance_range=False, use_forecast_range=False):
    """
    [existing docstring...]
    
    Additional Parameters:
    use_maintenance_range (bool): If True, use maintenance time range (today-50 to today-3)
    use_forecast_range (bool): If True, use forecast time range (today-2 to today)
    """
    
    # Get target timezone
    target_time_zone = get_local_timezone_from_env()
    
    # Determine time range based on mode
    if use_maintenance_range:
        start_time_utc, end_time_utc = get_local_time_range_for_maintenance_runoff_request(target_time_zone)
        logger.info("Using MAINTENANCE time range for data request")
    elif use_forecast_range:
        start_time_utc, end_time_utc = get_local_time_range_for_forecast_runoff_request(target_time_zone)
        logger.info("Using FORECAST time range for data request")
    else:
        # Default behavior (original)
        start_time_utc, end_time_utc = get_local_time_range_for_daily_average_runoff_request(target_time_zone)
        logger.info("Using DEFAULT time range for data request")
    
    # [Rest of the function remains the same, using start_time_utc and end_time_utc]
    # ... (existing implementation)
```

**Action**: Apply the same changes to `get_runoff_data_for_sites_HF`:

```python
def get_runoff_data_for_sites_HF(ieh_hf_sdk=None, date_col='date', name_col='name',
                                code_col='code', discharge_col='discharge',
                                site_list=None, site_code_list=None, site_id_list=None,
                                use_maintenance_range=False, use_forecast_range=False):
    """
    [existing docstring...]
    
    Additional Parameters:
    use_maintenance_range (bool): If True, use maintenance time range (today-50 to today-3)
    use_forecast_range (bool): If True, use forecast time range (today-2 to today)
    """
    
    # [Same time range logic as above]
    target_time_zone = get_local_timezone_from_env()
    
    if use_maintenance_range:
        start_time_utc, end_time_utc = get_local_time_range_for_maintenance_runoff_request(target_time_zone)
        logger.info("Using MAINTENANCE time range for HF data request")
    elif use_forecast_range:
        start_time_utc, end_time_utc = get_local_time_range_for_forecast_runoff_request(target_time_zone)
        logger.info("Using FORECAST time range for HF data request")
    else:
        start_time_utc, end_time_utc = get_local_time_range_for_daily_average_runoff_request(target_time_zone)
        logger.info("Using DEFAULT time range for HF data request")
    
    # [Rest of the function remains the same]
    # ... (existing implementation)
```

## Step 6: Update Pipeline Docker Configuration

### Task: Modify PreprocessingRunoff class to support RUN_MODE

**File to modify**: `apps/pipeline/pipeline_docker.py`

**Action**: Find the `PreprocessingRunoff` class and update it:

```python
class PreprocessingRunoff(DockerTaskBase):
    # Add parameter for run mode
    run_mode = luigi.Parameter(default="forecast")
    
    # Update logging output path to include mode
    @property
    def docker_logs_file_path(self):
        return f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_preprunoff_{self.run_mode}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    def output(self):
        # Different output files for different modes
        if self.run_mode == "maintenance":
            return luigi.LocalTarget(f'/app/log_preprunoff_maintenance.txt')
        else:
            return luigi.LocalTarget(f'/app/log_preprunoff_forecast.txt')

    def run(self):
        # Set up volumes (same as before)
        volumes = setup_docker_volumes(env, [
            'ieasyforecast_configuration_path',
            'ieasyforecast_intermediate_data_path',
            'ieasyforecast_daily_discharge_path'
        ])
        
        # Define environment variables - ADD RUN_MODE
        environment = [
            f'ieasyhydroforecast_env_file_path={env_file_path}',
            f'RUN_MODE={self.run_mode}',  # Add this line
        ] 
        
        # Execute with retries using the base class method
        status, details = self.execute_with_retries(
            lambda attempt: self.run_docker_container(
                image_name='sapphire-preprunoff',
                container_name=f'preprunoff-{self.run_mode}',  # Different container names
                volumes=volumes,
                environment=environment,
                attempt_number=attempt
            )
        )
        
        # Write marker file only if successful
        if status == "Success":
            today = datetime.date.today()
            marker_file = get_marker_filepath(f'preprocessing_runoff_{self.run_mode}', date=today)
            print(f"Writing success marker file to: {marker_file}")
            with open(marker_file, 'w') as f:
                f.write(f"PreprocessingRunoff ({self.run_mode}) completed successfully at {datetime.datetime.now()}")
            
            if os.path.exists(marker_file):
                print(f"✅ Marker file created successfully at {marker_file}")
            else:
                print(f"❌ Failed to create marker file at {marker_file}")
```

**Action**: Add new workflow classes:

```python
class PreprocessingRunoffMaintenance(PreprocessingRunoff):
    """Maintenance mode preprocessing - runs overnight"""
    run_mode = luigi.Parameter(default="maintenance")

class PreprocessingRunoffForecast(PreprocessingRunoff):  
    """Forecast mode preprocessing - runs during daily operations"""
    run_mode = luigi.Parameter(default="forecast")

class RunPreprocessingRunoffMaintenanceWorkflow(luigi.Task):
    """Workflow for running maintenance preprocessing"""
    
    def requires(self):
        return PreprocessingRunoffMaintenance()
    
    def run(self):
        # Mark workflow as complete
        with self.output().open('w') as f:
            f.write(f"Maintenance preprocessing workflow completed at {datetime.datetime.now()}")
    
    def output(self):
        today = datetime.date.today()
        return luigi.LocalTarget(get_marker_filepath('preprocessing_runoff_maintenance_workflow', date=today))
```

## Step 7: Create Maintenance Script in bin/

### Task: Create daily_preprocessing_maintenance.sh script

**File to create**: `bin/daily_preprocessing_maintenance.sh`

**Content**: Base this on `daily_ml_maintenance.sh` but adapted for preprocessing_runoff:

```bash
#!/bin/bash
# Runs maintenance preprocessing for runoff data overnight

# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"

# Print the banner
print_banner

# Read the configuration from the .env file
read_configuration $1

# Validate required environment variables
if [ -z "$ieasyhydroforecast_data_root_dir" ] || [ -z "$ieasyhydroforecast_env_file_path" ]; then
  echo "Error: Required environment variables are not set. Please check your .env file."
  exit 1
fi

# Create log directory if it doesn't exist
LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/preprocessing_maintenance"
mkdir -p ${LOG_DIR}

# Print log directory path
echo "Log directory: ${LOG_DIR}"

# Set main log file path with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
log_file="${LOG_DIR}/run_${TIMESTAMP}.log"

# Function to log messages to both console and log file
log_message() {
  echo "[$(date +"%Y-%m-%d %H:%M:%S")] $1" | tee -a "$log_file"
}

log_message "Starting preprocessing runoff maintenance run"

# Create a temporary docker-compose file
COMPOSE_FILE="preprocessing-maintenance-compose.yml"
echo "services:" > $COMPOSE_FILE

# Add the service configuration
cat >> $COMPOSE_FILE << EOF
  preprocessing-runoff-maintenance:
    image: sapphire-preprunoff:latest
    environment:
      - PYTHONPATH=/app
      - ieasyhydroforecast_data_root_dir=${ieasyhydroforecast_data_root_dir}
      - ieasyhydroforecast_env_file_path=${ieasyhydroforecast_env_file_path}
      - RUN_MODE=maintenance
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ${ieasyhydroforecast_data_ref_dir}/config:${ieasyhydroforecast_container_data_ref_dir}/config
      - ${ieasyhydroforecast_data_ref_dir}/daily_runoff:${ieasyhydroforecast_container_data_ref_dir}/daily_runoff
      - ${ieasyhydroforecast_data_ref_dir}/intermediate_data:${ieasyhydroforecast_container_data_ref_dir}/intermediate_data
    deploy:
      resources:
        limits:
          memory: 4g
          cpus: '1.0'
    networks:
      - preprocessing-network

networks:
  preprocessing-network:
    driver: bridge
EOF

log_message "Generated docker-compose file for preprocessing maintenance"

# Verify Docker is running
if ! docker info > /dev/null 2>&1; then
  log_message "ERROR: Docker is not running. Please start Docker and try again."
  exit 1
fi

# Check if the Docker image exists
IMAGE_ID="sapphire-preprunoff:latest"
if ! docker image inspect $IMAGE_ID > /dev/null 2>&1; then
  log_message "ERROR: Docker image $IMAGE_ID not found. Please build the image first."
  exit 1
fi

log_message "Starting preprocessing maintenance container..."

# Start the service
docker_output=$(docker compose -f $COMPOSE_FILE up --abort-on-container-exit 2>&1)
docker_exit_code=$?

# Log the Docker output
log_message "Docker compose output:"
echo "$docker_output" | tee -a "$log_file"

# Check if the docker compose command succeeded
if [ $docker_exit_code -ne 0 ]; then
    log_message "ERROR: Preprocessing maintenance failed. Exit code: $docker_exit_code"
    exit 1
fi

log_message "Preprocessing maintenance completed successfully"

# Clean up
log_message "Cleaning up docker resources"
docker compose -f $COMPOSE_FILE down
rm $COMPOSE_FILE

log_message "Preprocessing maintenance run completed successfully"
```

## Step 8: Update Existing Scripts

### Task: Modify run_preprocessing_runoff.sh to use forecast mode

**File to modify**: `bin/run_preprocessing_runoff.sh`

**Action**: Find the docker compose command and add RUN_MODE environment variable:

```bash
# Find this section in the file:
docker compose -f bin/docker-compose-luigi.yml run \
    -v $(pwd)/temp_luigi.cfg:/app/luigi.cfg \
    -e PYTHONPATH="/home/appuser/.local/lib/python3.11/site-packages:${PYTHONPATH}" \
    -e RUN_MODE=forecast \
    --user root \
    --rm \
    preprocessing-runoff
```

## Step 9: Update Docker Configuration

### Task: Ensure Docker image supports new scripts

**File to check/modify**: `apps/preprocessing_runoff/Dockerfile`

**Action**: Verify that the Dockerfile copies all necessary files:

```dockerfile
# Make sure these lines are present (or add them):
COPY preprocessing_runoff.py ./
COPY preprocessing_runoff_maintenance.py ./
COPY preprocessing_runoff_forecast.py ./
COPY src/ ./src/
COPY requirements.txt .
```

## Step 10: Testing and Validation

### Task: Create test scripts to validate the implementation

**File to create**: `apps/preprocessing_runoff/test/test_mode_separation.py`

**Content**:
```python
import os
import sys
import pytest
import tempfile
import pandas as pd
from unittest.mock import patch, MagicMock

# Add path for local imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src import src

class TestModeSeparation:
    """Test the maintenance/forecast mode separation."""
    
    def test_maintenance_time_range(self):
        """Test that maintenance mode uses correct time range."""
        import pytz
        timezone = pytz.timezone('UTC')
        
        start, end = src.get_local_time_range_for_maintenance_runoff_request(timezone)
        
        # Should be 50 days ago to 3 days ago
        assert start is not None
        assert end is not None
        assert (end - start).days == 47  # 50-3 = 47 days
    
    def test_forecast_time_range(self):
        """Test that forecast mode uses correct time range."""
        import pytz
        timezone = pytz.timezone('UTC')
        
        start, end = src.get_local_time_range_for_forecast_runoff_request(timezone)
        
        # Should be 2 days ago to today
        assert start is not None
        assert end is not None
        assert (end - start).days == 2
    
    def test_save_and_load_maintenance_data(self):
        """Test saving and loading maintenance data."""
        # Create test data
        test_data = pd.DataFrame({
            'date': pd.date_range('2025-01-01', periods=10),
            'code': [12345] * 10,
            'discharge': range(10)
        })
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock the environment path
            with patch.dict(os.environ, {'ieasyforecast_intermediate_data_path': temp_dir}):
                # Save data
                saved_path = src.save_maintenance_data(test_data, "test_maintenance")
                assert saved_path is not None
                assert os.path.exists(saved_path)
                
                # Load data
                loaded_data = src.load_maintenance_data("test_maintenance")
                assert loaded_data is not None
                assert len(loaded_data) == len(test_data)
                
    @patch.dict(os.environ, {'RUN_MODE': 'maintenance'})
    def test_main_script_maintenance_mode(self):
        """Test that main script correctly detects maintenance mode."""
        with patch('preprocessing_runoff_maintenance.main') as mock_maintenance:
            mock_maintenance.return_value = 0
            
            # Import after setting environment
            from preprocessing_runoff import main
            result = main()
            
            mock_maintenance.assert_called_once()
            assert result == 0
    
    @patch.dict(os.environ, {'RUN_MODE': 'forecast'})  
    def test_main_script_forecast_mode(self):
        """Test that main script correctly detects forecast mode."""
        with patch('preprocessing_runoff_forecast.main') as mock_forecast:
            mock_forecast.return_value = 0
            
            from preprocessing_runoff import main
            result = main()
            
            mock_forecast.assert_called_once()
            assert result == 0

if __name__ == '__main__':
    pytest.main([__file__])
```

## Step 11: Documentation and Deployment

### Task: Update documentation and deployment scripts

**Files to update**:
1. `apps/preprocessing_runoff/README.md` - Document the new modes
2. `doc/workflows.md` - Update workflow documentation
3. Add cron job examples for scheduling maintenance runs

**README.md addition**:
```markdown
## Maintenance vs Forecast Modes

The preprocessing_runoff module now supports two operation modes:

### Maintenance Mode (RUN_MODE=maintenance)
- Runs overnight during maintenance windows
- Processes historical data from today-50 to today-3 days
- Saves intermediate results for fast daily updates
- Usage: `RUN_MODE=maintenance ieasyhydroforecast_env_file_path=<path> python preprocessing_runoff.py`

### Forecast Mode (RUN_MODE=forecast, default)
- Runs during daily forecast operations
- Processes only recent data from today-2 to today
- Combines with maintenance data for complete time series
- Much faster execution for daily operations
- Usage: `RUN_MODE=forecast ieasyhydroforecast_env_file_path=<path> python preprocessing_runoff.py`

### Performance Benefits
- Forecast mode reduces processing time from ~5-10 minutes to ~30-60 seconds
- Maintenance mode ensures data is always ready for fast daily operations
- Graceful fallback to full processing if maintenance data is unavailable
```

## Implementation Order

Execute these steps in the following order:

1. **Step 1**: Add mode-specific functions to `src.py`
2. **Step 5**: Update existing functions in `src.py` for mode support  
3. **Step 2**: Create `preprocessing_runoff_maintenance.py`
4. **Step 3**: Create `preprocessing_runoff_forecast.py`
5. **Step 4**: Modify main `preprocessing_runoff.py`
6. **Step 9**: Update Dockerfile if needed
7. **Step 6**: Update pipeline Docker configuration
8. **Step 7**: Create maintenance script in bin/
9. **Step 8**: Update existing run scripts
10. **Step 10**: Create and run tests
11. **Step 11**: Update documentation

## Testing Strategy

After implementation:
1. Test maintenance mode manually: `RUN_MODE=maintenance python preprocessing_runoff.py`
2. Test forecast mode manually: `RUN_MODE=forecast python preprocessing_runoff.py`  
3. Verify data persistence and loading
4. Test fallback behavior when maintenance data is missing
5. Compare processing times before and after implementation
6. Run full pipeline tests to ensure integration works

This step-by-step guide provides a comprehensive implementation plan that a coding-specialized LLM can follow to implement the maintenance/forecast mode separation for the preprocessing_runoff module.