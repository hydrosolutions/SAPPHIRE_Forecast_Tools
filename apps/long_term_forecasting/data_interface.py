##################################################
# Data Interface for retrieving and managing datasets
##################################################


import os
import sys
import glob
import pandas as pd
import numpy as np
import json
from typing import List, Dict, Any, Optional, Tuple

from __init__ import logger, initialize_today, get_today


# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the setup_library module from the iEasyHydroForecast package
import setup_library as sl

class DataInterfaceDB:
    def __init__(self):
        raise NotImplementedError("Database loading not implemented yet.")

class DataInterface:
    def __init__(self):
        
        sl.load_environment()

        self._get_paths()

    def _get_paths(self):
        """
        Retrieve necessary file paths.
        """
        # Access the environment variables
        intermediate_data_path = os.getenv('ieasyforecast_intermediate_data_path')
        MODELS_AND_SCALERS_PATH = os.getenv('ieasyhydroforecast_models_and_scalers_path')
        PATH_TO_STATIC_FEATURES = os.getenv('ieasyhydroforecast_ml_long_term_path_to_static')

        # Static Features
        self.PATH_TO_STATIC_FEATURES = os.path.join(MODELS_AND_SCALERS_PATH, PATH_TO_STATIC_FEATURES)

        # Read in the ERA5 Reanalysis Data
        PATH_ERA5_REANALYSIS = os.getenv('ieasyhydroforecast_OUTPUT_PATH_REANALYSIS')
        self.PATH_ERA5_REANALYSIS = os.path.join(intermediate_data_path, PATH_ERA5_REANALYSIS)

        # Read in The Operational Forcing Data
        PATH_OPERATIONAL_CONTROL_MEMBER = os.getenv('ieasyhydroforecast_OUTPUT_PATH_CM')
        self.PATH_OPERATIONAL_CONTROL_MEMBER = os.path.join(intermediate_data_path, PATH_OPERATIONAL_CONTROL_MEMBER)
    
        PATH_TO_PAST_DISCHARGE = os.getenv('ieasyforecast_daily_discharge_file')
        self.PATH_TO_PAST_DISCHARGE = os.path.join(intermediate_data_path, PATH_TO_PAST_DISCHARGE)

        PATH_SNOW_DATA = os.getenv('ieasyhydroforecast_OUTPUT_PATH_SNOW')
        self.PATH_SNOW_DATA = os.path.join(intermediate_data_path, PATH_SNOW_DATA)


    def _prepare_static_data(self) -> pd.DataFrame:
        """
        Prepare static features dataset.

        Returns:
            pd.DataFrame: The static features dataset.
        """
        static_features = pd.read_csv(self.PATH_TO_STATIC_FEATURES)
        if "CODE" in static_features.columns:
            static_features.rename(columns={"CODE": "code"}, inplace=True)

        return static_features
    

    def _load_forcing_data(self,
                          HRU: str):
        """
        Load in the forcing file:
        Names:
            path_hindcast/00003_P_reanalysis.csv
            path_hindcast/00003_T_reanalysis.csv
            path_operational/00003_P_control_member.csv
            path_operational/00003_T_control_member.csv

        where 00003 is the HRU number

        contains columns: date, code, (P or T)
        combines the data frames - merge hindcast on date and code (ensure same format)
        then merge operational on date and code (ensure same format)
        then combine the two data frames (concat)
        """

        path_hindcast = self.PATH_ERA5_REANALYSIS
        path_operational = self.PATH_OPERATIONAL_CONTROL_MEMBER

        # Load hindcast data
        hindcast_files = {
            "P": os.path.join(path_hindcast, f"{HRU}_P_reanalysis.csv"),
            "T": os.path.join(path_hindcast, f"{HRU}_T_reanalysis.csv"),
        }

        hindcast_dfs = {}
        for var, path in hindcast_files.items():
            df = pd.read_csv(path)
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
            df["code"] = df["code"].astype(int)
            hindcast_dfs[var] = df

        # Merge hindcast data on date and code
        hindcast_merged = pd.merge(
            hindcast_dfs["P"], hindcast_dfs["T"], on=["date", "code"], how="inner"
        )

        # Load operational data
        operational_files = {
            "P": os.path.join(path_operational, f"{HRU}_P_control_member.csv"),
            "T": os.path.join(path_operational, f"{HRU}_T_control_member.csv"),
        }

        operational_dfs = {}
        for var, path in operational_files.items():
            df = pd.read_csv(path)
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
            df["code"] = df["code"].astype(int)
            operational_dfs[var] = df

        # Merge operational data on date and code
        operational_merged = pd.merge(
            operational_dfs["P"], operational_dfs["T"], on=["date", "code"], how="inner"
        )

        # Combine hindcast and operational data
        combined_df = pd.concat([hindcast_merged, operational_merged], ignore_index=True)

        # Drop duplicates based on date and code
        combined_df = combined_df.drop_duplicates(subset=["date", "code"], keep='last').reset_index(drop=True)

        # drop columns if "day_of_year" in columns
        cols_to_drop = [col for col in combined_df.columns if "dayofyear" in col]
        combined_df.drop(columns=cols_to_drop, inplace=True)

        return combined_df


    def get_base_data(self,
                      forcing_HRU: str,
                      start_date: Optional[str]=None) -> dict[str, Any]:
        """
        Retrieve the base dataset for long-term forecasting.

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: The base temporal dataset and the static dataset.
        """

        today = get_today()

        discharge = pd.read_csv(self.PATH_TO_PAST_DISCHARGE, parse_dates=['date'])
        # filter discharge to only include dates before today
        discharge = discharge[discharge["date"] <= today].reset_index(drop=True)
        
        max_date_discharge = discharge["date"].max()
        discharge["code"] = discharge["code"].astype(int)

        forcing_data = self._load_forcing_data(HRU=forcing_HRU)

        # Use outer merge to ensure all dates from both datasets are included
        temporal_data  = pd.merge(discharge, forcing_data, on=["date", "code"], how="outer")

        if start_date is not None:
            start_date_pd = pd.to_datetime(start_date, format="%Y-%m-%d")
            temporal_data = temporal_data[temporal_data["date"] >= start_date_pd].reset_index(drop=True)

        static_data = self._prepare_static_data()

        # Calculate Time Offsets
        # This is usefull to check if we can run the forecast or if we are missing data
        max_date_temporal = temporal_data["date"].max()
        offset_base = (today - max_date_temporal).days # we expect max_date_temporal to be in the future as we have forecasting data
        offset_discharge = (today - max_date_discharge).days # we expect max_date_discharge to be in the past as we do not have todays discharge yet

        temporal_data = self._clean_data(temporal_data)


        return {"temporal_data": temporal_data, "static_data": static_data, 
                "offset_date_base": offset_base, "offset_date_discharge": offset_discharge}
    
    def extend_base_data_with_snow(self,
                                  base_data: pd.DataFrame,
                                  HRUs_snow: List[str],
                                  snow_variables: List[str]) -> dict[str, Any]:
        
        assert len(HRUs_snow) == len(snow_variables), "Length of HRUs_snow must match length of snow_variables"

        temporal_data = base_data.copy()
        today = get_today()

        if len(HRUs_snow) > 0:
            for HRU, variable in zip(HRUs_snow, snow_variables):
                snow_data, max_date = self.load_snow_data(
                    HRU=HRU,
                    variable=variable
                    )
                
                offset_snow = (today - max_date).days

                # remove duplicates based on date and code
                snow_data = snow_data.drop_duplicates(subset=["date", "code"])

                temporal_data = pd.merge(temporal_data, snow_data, on=["date", "code"], how="left")
        else:
            offset_snow = None

        return {"temporal_data": temporal_data, "offset_date_snow": offset_snow}

    def load_snow_data(self, 
                       HRU : str,
                       variable: str) -> Tuple[pd.DataFrame, pd.Timestamp]:
        """
        Load the snow data from a csv file from the data-gateway
        """
        available_snow_vars = os.getenv('ieasyhydroforecast_SNOW_VARS').split(',')
        available_snow_hrus = os.getenv('ieasyhydroforecast_HRU_SNOW_DATA').split(',')

        assert variable in available_snow_vars, f"Variable {variable} not in available snow variables: {available_snow_vars}"
        assert HRU in available_snow_hrus, f"HRU {HRU} not in available snow HRUs: {available_snow_hrus}"

        # add snow variable to file name
        snow_path = os.path.join(self.PATH_SNOW_DATA, variable)
        file_path = os.path.join(snow_path, f"{HRU}_{variable}.csv")
        df = pd.read_csv(file_path)
        
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        df["code"] = df["code"].astype(int)

        if "RoF" in df.columns:
            # If the column is named "RoF", rename it to "ROF"
            df.rename(columns={"RoF": "ROF"}, inplace=True)

        max_date = df["date"].max()

        return df, max_date


    def _clean_data(self, 
                   data: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the dataset.

        Args:
            data (pd.DataFrame): The dataset to clean.

        Returns:
            pd.DataFrame: The cleaned dataset.
        """
        
        # Step one - sort by date and code
        data = data.sort_values(by=["code", "date"]).reset_index(drop=True)

        # Step two drop duplicates
        data = data.drop_duplicates(subset=["code", "date"], keep='last').reset_index(drop=True)

        # Step three - ensure time series is continuous - reindex
        # We expect that the data has EMCWF IFS Forecasts up to 15 days ahead
        emcwf_forecast_days_ahead = int(os.getenv('ieasyhydroforecast_ECMWF_IFS_lead_time'))
        today = get_today()
        end_date = today + pd.Timedelta(days=emcwf_forecast_days_ahead)
        data = data[data["date"] <= end_date].reset_index(drop=True)
        
        max_date_data = data["date"].max()
        if end_date > max_date_data:
            logger.warning(f"Data ends at {max_date_data}, but with EMCWF forecasts we expect data up to {end_date}. This indicates some missing data.")
        
        full_end_date = max(end_date, max_date_data)
                
        all_codes = data["code"].unique()
        full_date_range = pd.date_range(start=data["date"].min(), end=full_end_date, freq='D')

        full_index = pd.MultiIndex.from_product([all_codes, full_date_range], names=["code", "date"])
        data = data.set_index(["code", "date"]).reindex(full_index).reset_index()

        return data
    

class BasePredictorDataInterface:
    def __init__(self):
        logger.info("Initialized BasePredictorDataInterface")
        
    def get_base_predictor_data_csv(self, 
                                    model_name: str,
                                    data_path: str) -> Tuple[pd.DataFrame, List[str]]:
        """
        Retrieve the base predictor dataset.

        Returns:
            pd.DataFrame: The base predictor dataset.
        """
        base_data = pd.read_csv(data_path)
        
        base_data["date"] = pd.to_datetime(base_data["date"], format="%Y-%m-%d")
        base_data["code"] = base_data["code"].astype(int)

        # get valid Q columns
        Q_cols = [col for col in base_data.columns if "Q_" in col]
        Q_cols = [col for col in Q_cols if col not in ["Q_obs", "Q_OBS"]]

        base_data = base_data[["date", "code"] + Q_cols]

        if len(Q_cols) == 1:
            include_ensemble = True
        else:
            include_ensemble = False

        base_models_cols = []

        for col in Q_cols:
            sub_model = col.replace("Q_", "")
            member_name = sub_model

            if sub_model == model_name:
                if not include_ensemble:
                    continue
                base_models_cols.append(member_name)

            else:
                sub_sub_model = sub_model.split("_")[-1]
                member_name = f"{model_name}_{sub_sub_model}"
                base_models_cols.append(member_name)

            base_data.rename(columns={col: member_name}, inplace=True)

        return base_data, base_models_cols

        
    def get_base_predictor_data_database(self) -> pd.DataFrame:
        """
        Retrieve the base predictor dataset from the database.

        Returns:
            pd.DataFrame: The base predictor dataset.
        """
        raise NotImplementedError("Database loading not implemented yet.")

    def load_all_dependencies_csv(self,
                            all_dependencies_models: List[str],
                            all_dependencies_paths: List[str],
                              ) -> Tuple[pd.DataFrame, List[str]]:
        """
        Loads all dependencies data from the provided paths.
        """

        all_predictions = None
        all_model_cols = []
        
        for model_name, model_path in zip(all_dependencies_models, all_dependencies_paths):

            base_data, base_models_cols = self.get_base_predictor_data_csv(
                model_name=model_name,
                data_path=model_path
            )

            if all_predictions is None:
                all_predictions = base_data
            else:
                all_predictions = pd.merge(
                    all_predictions,
                    base_data,
                    on=["date", "code"],
                    how="inner"
                )
            
            all_model_cols.extend(base_models_cols)
            
        return all_predictions, all_model_cols

    def load_all_dependencies_database(self,
                            all_dependencies_models: List[str]
                              ) -> Tuple[pd.DataFrame, List[str]]:
        """
        Loads all dependencies data from the database.
        """
        raise NotImplementedError("Database loading not implemented yet.")
    
