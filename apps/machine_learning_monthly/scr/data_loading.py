import os
import pandas as pd
from typing import Dict, Any, Tuple
# Ensure the logs directory exists
import datetime

# Shared logging
import logging
from log_config import setup_logging
setup_logging()  

logger = logging.getLogger(__name__)  # Use __name__ to get module-specific logger

# Custom Libraries
from monthly_base_config import PATH_CONFIG, GENERAL_CONFIG, MODEL_CONFIG, FEATURE_CONFIG



def load_data(path_discharge, 
              path_to_P_operational, 
              path_to_T_operational,
              path_to_P_reanalysis, 
              path_to_T_reanalysis,
              path_static_features, 
              path_snow_data) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load the data from the given paths.
    """
    discharge = pd.read_csv(path_discharge, parse_dates=['date'])
    discharge['date'] = pd.to_datetime(discharge['date'])
    # Get the list of unique codes
    unique_codes = discharge['code'].unique()
    
    # Find the min and max dates from the discharge data
    min_date = discharge['date'].min()
    # Use today's date as the max date to ensure completeness through present
    max_date = pd.Timestamp.today().normalize()
    
    # Create a continuous date range
    date_range = pd.date_range(start=min_date, end=max_date, freq='D')
    
    # Create a complete dataframe with all dates for all codes
    all_dates = pd.DataFrame([(date, code) for code in unique_codes for date in date_range],
                            columns=['date', 'code'])
    
    # Merge with the original discharge data to fill in missing dates
    discharge = pd.merge(all_dates, discharge, on=['date', 'code'], how='left')


    P_data_operational = pd.read_csv(path_to_P_operational, parse_dates=['date'])
    P_data_operational['date'] = pd.to_datetime(P_data_operational['date'])
    T_data_operational = pd.read_csv(path_to_T_operational, parse_dates=['date'])
    T_data_operational['date'] = pd.to_datetime(T_data_operational['date'])

    forcing_operational = pd.merge(P_data_operational, T_data_operational, on=['date', 'code'], how='left')


    P_data_reanalysis = pd.read_csv(path_to_P_reanalysis, parse_dates=['date'])
    P_data_reanalysis['date'] = pd.to_datetime(P_data_reanalysis['date'])
    T_data_reanalysis = pd.read_csv(path_to_T_reanalysis, parse_dates=['date'])
    T_data_reanalysis['date'] = pd.to_datetime(T_data_reanalysis['date'])
    forcing_reanalysis = pd.merge(P_data_reanalysis, T_data_reanalysis, on=['date', 'code'], how='left')

    forcing_df = pd.concat([forcing_reanalysis, forcing_operational], axis=0)
    forcing_df = forcing_df[['date', 'code', 'P', 'T']]
    forcing_df = forcing_df.sort_values(by=['date', 'code'])
    #drop duplicates on code and date
    forcing_df = forcing_df.drop_duplicates(subset=['date', 'code'])

    logger.debug(f"Head of forcing_df: {forcing_df.head()}")

    hydro_df = pd.merge(discharge, forcing_df, on=['date', 'code'], how='left')

    HRU_SWE = GENERAL_CONFIG['HRU_SWE']
    swe_df = None
    if HRU_SWE:
        try:
            swe_path = os.path.join(path_snow_data, 'SWE', f"{HRU_SWE}_SWE.csv")
            swe_df = pd.read_csv(swe_path, parse_dates=['date'])
            swe_df['date'] = pd.to_datetime(swe_df['date'])
            hydro_df = pd.merge(hydro_df, swe_df, on=['date', 'code'], how='left')
        except Exception as e:
            logger.error(f"Error loading SWE data: {e}")
            
    HRU_HS = GENERAL_CONFIG['HRU_HS']
    hs_df = None
    if HRU_HS:
        try:
            hs_path = os.path.join(path_snow_data, 'HS', f"{HRU_HS}_HS.csv")
            hs_df = pd.read_csv(hs_path, parse_dates=['date'])
            hs_df['date'] = pd.to_datetime(hs_df['date'])
            hydro_df = pd.merge(hydro_df, hs_df, on=['date', 'code'], how='left')
        except Exception as e:
            logger.error(f"Error loading HS data: {e}")

    HRU_RoF = GENERAL_CONFIG['HRU_RoF']
    rof_df = None
    if HRU_RoF:
        try:
            rof_path = os.path.join(path_snow_data, 'RoF', f"{HRU_RoF}_RoF.csv")
            rof_df = pd.read_csv(rof_path, parse_dates=['date'])
            rof_df['date'] = pd.to_datetime(rof_df['date'])
            hydro_df = pd.merge(hydro_df, rof_df, on=['date', 'code'], how='left')
        except Exception as e:
            logger.error(f"Error loading RoF data: {e}")

    # Load static features
    static_features = pd.read_csv(path_static_features)
    if 'CODE' in static_features.columns:
        static_features.rename(columns={'CODE': 'code'}, inplace=True)
    
    hydro_df['code'] = hydro_df['code'].astype(int)
    static_features['code'] = static_features['code'].astype(int)

    return hydro_df, static_features