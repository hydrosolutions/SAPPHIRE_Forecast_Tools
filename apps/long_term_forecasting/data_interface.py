##################################################
# Data Interface for retrieving and managing datasets
##################################################


import os
import sys
from time import time
from typing import Any

import numpy as np
import pandas as pd
from __init__ import get_today, logger

# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, "..", "iEasyHydroForecast")

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

import setup_library as sl


def _api_client(client_type):
    # Import lazily so CSV-only installations do not require the API client.
    from sapphire_api_client import SapphirePreprocessingClient, SapphirePostprocessingClient

    cls = SapphirePreprocessingClient if client_type == "preprocessing" else SapphirePostprocessingClient
    url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    logger.info(f"Long-term {client_type} reads use configured API: {url}")
    return cls(base_url=url)


def _read_api_pages(read, station_codes=None, **filters):
    """Read all pages from one configured service; failures propagate without fallback."""
    frames = []
    codes = list(dict.fromkeys(str(c) for c in station_codes)) if station_codes else [None]
    for code in codes:
        skip = 0
        while True:
            page = read(**filters, code=code, skip=skip, limit=1000)
            if page.empty:
                break
            frames.append(page)
            skip += len(page)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


class DataInterfaceDB:
    """Database-backed inputs accessed exclusively through the configured regional API."""

    def __init__(self, station_codes=None):
        sl.load_environment()
        self.client = _api_client("preprocessing")
        self.station_codes = station_codes
        self._get_paths()

    def _read_input(self, read, code=None, **filters):
        if code is not None and self.station_codes and str(code) not in {str(c) for c in self.station_codes}:
            return pd.DataFrame()
        codes = [str(code)] if code is not None else self.station_codes
        return _read_api_pages(read, codes, **filters)

    @staticmethod
    def _normalize_input(df, mapping):
        df = df.rename(columns=mapping).reindex(columns=["date", "code", *mapping.values()]).copy()
        df["date"] = pd.to_datetime(df["date"])
        df["code"] = df["code"].astype(int)
        for col in mapping.values():
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.sort_values(["code", "date"]).reset_index(drop=True)

    def _get_paths(self):
        """Retrieve necessary file paths for static features (still CSV-based)."""
        MODELS_AND_SCALERS_PATH = os.getenv("ieasyhydroforecast_models_and_scalers_path")
        PATH_TO_STATIC_FEATURES = os.getenv("ieasyhydroforecast_ml_long_term_path_to_static")
        self.PATH_TO_STATIC_FEATURES = os.path.join(
            MODELS_AND_SCALERS_PATH, PATH_TO_STATIC_FEATURES
        )

    def get_meteo_data(self, meteo_type, code=None, start_date=None, end_date=None):
        df = self._read_input(self.client.read_meteo, code=code, meteo_type=meteo_type,
                              start_date=start_date, end_date=end_date)
        return self._normalize_input(df, {"value": meteo_type})

    def get_rain(self, station: str | None = None) -> pd.DataFrame:
        """Get precipitation data."""
        df = self.get_meteo_data(meteo_type="P", code=station)
        df.rename(columns={"P": "Precipitation"}, inplace=True)
        return df

    def get_temperature(self, station: str | None = None) -> pd.DataFrame:
        """Get temperature data."""
        return self.get_meteo_data(meteo_type="T", code=station)

    # ─────────────────────────────────────────────────────────────
    # RUNOFF / DISCHARGE DATA
    # ─────────────────────────────────────────────────────────────
    def get_runoff_data(self, code=None, start_date=None, end_date=None):
        df = self._read_input(self.client.read_runoff, code=code, horizon="day",
                              start_date=start_date, end_date=end_date)
        return self._normalize_input(df, {"discharge": "discharge"})

    def get_snow_data(self, variable, code=None, start_date=None, end_date=None):
        variable = variable.upper()
        df = self._read_input(self.client.read_snow, code=code, snow_type=variable,
                              start_date=start_date, end_date=end_date)
        mapping = {"value": variable, **{f"value{i}": f"{variable}_{i}" for i in range(1, 15)}}
        return self._normalize_input(df, mapping)

    def load_snow_data(self, HRU: str, variable: str) -> tuple[pd.DataFrame, pd.Timestamp]:
        """Load snow data for a specific HRU and variable.

        Note: HRU is used for validation only (to match CSV-based interface).
        The database stores snow data by station codes, not HRU codes.
        The configured station selection is applied to API reads.
        """
        available_snow_vars = os.getenv("ieasyhydroforecast_SNOW_VARS", "").split(",")
        available_snow_hrus = os.getenv("ieasyhydroforecast_HRU_SNOW_DATA", "").split(",")

        assert variable in available_snow_vars, f"Variable {variable} not in {available_snow_vars}"
        assert HRU in available_snow_hrus, f"HRU {HRU} not in {available_snow_hrus}"

        # Load snow data for the configured station selection
        # HRU is just used for validation, similar to CSV version where HRU is part of filename
        df = self.get_snow_data(variable=variable)

        # Normalize column names to uppercase , we expect columns to be SWE, SWE_1, SWE_2 etc, ensure swe_1 is renamed to SWE_1 etc
        var_upper = variable.upper()

        for col in df.columns:
            # split col by "_" and check if first part matches variable (case-insensitive)
            splitted_col = col.split("_")
            if len(splitted_col) == 1:
                if splitted_col[0].lower() == variable.lower():
                    df.rename(columns={col: var_upper}, inplace=True)
            else:
                if splitted_col[0].lower() == variable.lower():
                    new_col_name = var_upper + "_" + "_".join(splitted_col[1:])
                    df.rename(columns={col: new_col_name}, inplace=True)

        has_elevation_bands = (
            os.getenv(f"ieasyhydroforecast_{HRU}_has_elevation_bands", "False").lower() == "true"
        )
        if not has_elevation_bands:
            print(f"HRU {HRU} does not have elevation bands, keeping only main variable column.")
            columns_expected = ["date", "code", var_upper]
            df = df[columns_expected]

        max_date = df["date"].max()

        return df, max_date

    # ─────────────────────────────────────────────────────────────
    # FORCING DATA (Combined P & T - Reanalysis + Operational)
    # ─────────────────────────────────────────────────────────────
    def _load_forcing_data(self, HRU: str) -> pd.DataFrame:
        """
        Load combined forcing data (P and T) from the database.
        Replaces the CSV-based hindcast + operational merge.
        """
        # Get precipitation
        df_p = self.get_meteo_data(meteo_type="P")
        # ensure P is named "P" capital
        if "P" not in df_p.columns:
            p_col = [col for col in df_p.columns if col.lower() == "p"]
            if len(p_col) == 1:
                df_p.rename(columns={p_col[0]: "P"}, inplace=True)
            else:
                raise ValueError("Precipitation column not found or ambiguous in meteo data.")

        # Get temperature
        df_t = self.get_meteo_data(meteo_type="T")
        # ensure T is named "T" capital
        if "T" not in df_t.columns:
            t_col = [col for col in df_t.columns if col.lower() == "t"]
            if len(t_col) == 1:
                df_t.rename(columns={t_col[0]: "T"}, inplace=True)
            else:
                raise ValueError("Temperature column not found or ambiguous in meteo data.")

        # Merge P and T on date and code
        combined_df = pd.merge(df_p, df_t, on=["date", "code"], how="outer")
        combined_df = combined_df.sort_values(by=["code", "date"]).reset_index(drop=True)
        combined_df = combined_df.drop_duplicates(subset=["date", "code"], keep="last")

        return combined_df

    # ─────────────────────────────────────────────────────────────
    # STATIC FEATURES (Still CSV-based, could be migrated later)
    # ─────────────────────────────────────────────────────────────
    def _prepare_static_data(self) -> pd.DataFrame:
        """Prepare static features dataset."""
        static_features = pd.read_csv(self.PATH_TO_STATIC_FEATURES)
        if "CODE" in static_features.columns:
            static_features.rename(columns={"CODE": "code"}, inplace=True)
        if (
            self.station_codes is not None
            and len(self.station_codes) > 0
            and "code" in static_features.columns
        ):
            static_features = static_features[
                static_features["code"].astype(str).isin(self.station_codes)
            ].reset_index(drop=True)
        return static_features

    # ─────────────────────────────────────────────────────────────
    # BASE DATA ASSEMBLY
    # ─────────────────────────────────────────────────────────────
    def get_base_data(self, forcing_HRU: str, start_date: str | None = None) -> dict[str, Any]:
        """
        Retrieve the base dataset for long-term forecasting.

        Returns:
            Dict with temporal_data, static_data, and offset information
        """
        today = get_today()

        # Load discharge from database
        discharge = self.get_runoff_data(end_date=today.strftime("%Y-%m-%d"))
        # max date where discharge is not nan
        max_date_discharge = discharge[discharge["discharge"].notna()]["date"].max()

        # Load forcing data from database
        forcing_data = self._load_forcing_data(HRU=forcing_HRU)

        # Merge discharge and forcing
        temporal_data = pd.merge(discharge, forcing_data, on=["date", "code"], how="outer")

        if start_date is not None:
            start_date_pd = pd.to_datetime(start_date, format="%Y-%m-%d")
            temporal_data = temporal_data[temporal_data["date"] >= start_date_pd].reset_index(
                drop=True
            )

        static_data = self._prepare_static_data()

        temporal_data = self._clean_data(temporal_data)

        # Calculate time offsets
        max_date_temporal = temporal_data["date"].max()
        offset_base = (today - max_date_temporal).days
        offset_discharge = (today - max_date_discharge).days

        return {
            "temporal_data": temporal_data,
            "static_data": static_data,
            "offset_date_base": offset_base,
            "offset_date_discharge": offset_discharge,
        }

    def extend_base_data_with_snow(
        self, base_data: pd.DataFrame, HRUs_snow: list[str], snow_variables: list[str]
    ) -> dict[str, Any]:
        """Extend base data with snow variables."""
        assert len(HRUs_snow) == len(snow_variables), "Length mismatch"

        temporal_data = base_data.copy()
        today = get_today()
        offset_snow = None

        for HRU, variable in zip(HRUs_snow, snow_variables, strict=True):
            snow_data, max_date = self.load_snow_data(HRU=HRU, variable=variable)
            offset_snow = (today - max_date).days
            snow_data = snow_data.drop_duplicates(subset=["date", "code"])
            temporal_data = pd.merge(temporal_data, snow_data, on=["date", "code"], how="left")

        return {"temporal_data": temporal_data, "offset_date_snow": offset_snow}

    def _clean_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Clean and ensure continuous time series."""
        data = data.sort_values(by=["code", "date"]).reset_index(drop=True)
        data = data.drop_duplicates(subset=["code", "date"], keep="last").reset_index(drop=True)

        emcwf_forecast_days = int(os.getenv("ieasyhydroforecast_ECMWF_IFS_lead_time", 15))
        today = get_today()
        end_date = today + pd.Timedelta(days=emcwf_forecast_days)
        data = data[data["date"] <= end_date].reset_index(drop=True)

        max_date_data = data["date"].max()
        if end_date > max_date_data:
            logger.warning(f"Data ends at {max_date_data}, expected up to {end_date}")

        full_end_date = max(end_date, max_date_data)
        all_codes = data["code"].unique()
        full_date_range = pd.date_range(start=data["date"].min(), end=full_end_date, freq="D")

        full_index = pd.MultiIndex.from_product(
            [all_codes, full_date_range], names=["code", "date"]
        )
        data = data.set_index(["code", "date"]).reindex(full_index).reset_index()

        return data


class DataInterface:
    def __init__(self):
        sl.load_environment()

        self._get_paths()

    def _get_paths(self):
        """
        Retrieve necessary file paths.
        """
        # Access the environment variables
        intermediate_data_path = os.getenv("ieasyforecast_intermediate_data_path")
        MODELS_AND_SCALERS_PATH = os.getenv("ieasyhydroforecast_models_and_scalers_path")
        PATH_TO_STATIC_FEATURES = os.getenv("ieasyhydroforecast_ml_long_term_path_to_static")

        # Static Features
        self.PATH_TO_STATIC_FEATURES = os.path.join(
            MODELS_AND_SCALERS_PATH, PATH_TO_STATIC_FEATURES
        )

        # Read in the ERA5 Reanalysis Data
        PATH_ERA5_REANALYSIS = os.getenv("ieasyhydroforecast_OUTPUT_PATH_REANALYSIS")
        self.PATH_ERA5_REANALYSIS = os.path.join(intermediate_data_path, PATH_ERA5_REANALYSIS)

        # Read in The Operational Forcing Data
        PATH_OPERATIONAL_CONTROL_MEMBER = os.getenv("ieasyhydroforecast_OUTPUT_PATH_CM")
        self.PATH_OPERATIONAL_CONTROL_MEMBER = os.path.join(
            intermediate_data_path, PATH_OPERATIONAL_CONTROL_MEMBER
        )

        PATH_TO_PAST_DISCHARGE = os.getenv("ieasyforecast_daily_discharge_file")
        self.PATH_TO_PAST_DISCHARGE = os.path.join(intermediate_data_path, PATH_TO_PAST_DISCHARGE)

        PATH_SNOW_DATA = os.getenv("ieasyhydroforecast_OUTPUT_PATH_SNOW")
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

    def _load_forcing_data_db(self, HRU: str):
        raise NotImplementedError(
            "This method is not implemented in DataInterface. Use DataInterfaceDB instead."
        )

    def _load_forcing_data(self, HRU: str):
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
        combined_df = combined_df.drop_duplicates(subset=["date", "code"], keep="last").reset_index(
            drop=True
        )

        # drop columns if "day_of_year" in columns
        cols_to_drop = [col for col in combined_df.columns if "dayofyear" in col]
        combined_df.drop(columns=cols_to_drop, inplace=True)

        return combined_df

    def get_base_data(self, forcing_HRU: str, start_date: str | None = None) -> dict[str, Any]:
        """
        Retrieve the base dataset for long-term forecasting.

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: The base temporal dataset and the static dataset.
        """

        today = get_today()

        discharge = pd.read_csv(self.PATH_TO_PAST_DISCHARGE, parse_dates=["date"])
        # filter discharge to only include dates before today
        discharge = discharge[discharge["date"] <= today].reset_index(drop=True)

        max_date_discharge = discharge["date"].max()
        discharge["code"] = discharge["code"].astype(int)

        forcing_data = self._load_forcing_data(HRU=forcing_HRU)

        # Use outer merge to ensure all dates from both datasets are included
        temporal_data = pd.merge(discharge, forcing_data, on=["date", "code"], how="outer")

        if start_date is not None:
            start_date_pd = pd.to_datetime(start_date, format="%Y-%m-%d")
            temporal_data = temporal_data[temporal_data["date"] >= start_date_pd].reset_index(
                drop=True
            )

        static_data = self._prepare_static_data()

        # Calculate Time Offsets
        # This is usefull to check if we can run the forecast or if we are missing data
        max_date_temporal = temporal_data["date"].max()
        offset_base = (
            today - max_date_temporal
        ).days  # we expect max_date_temporal to be in the future as we have forecasting data
        offset_discharge = (
            today - max_date_discharge
        ).days  # we expect max_date_discharge to be in the past as we do not have todays discharge yet

        temporal_data = self._clean_data(temporal_data)

        return {
            "temporal_data": temporal_data,
            "static_data": static_data,
            "offset_date_base": offset_base,
            "offset_date_discharge": offset_discharge,
        }

    def extend_base_data_with_snow(
        self, base_data: pd.DataFrame, HRUs_snow: list[str], snow_variables: list[str]
    ) -> dict[str, Any]:
        assert len(HRUs_snow) == len(snow_variables), (
            "Length of HRUs_snow must match length of snow_variables"
        )

        temporal_data = base_data.copy()
        today = get_today()

        if len(HRUs_snow) > 0:
            for HRU, variable in zip(HRUs_snow, snow_variables, strict=True):
                snow_data, max_date = self.load_snow_data(HRU=HRU, variable=variable)

                offset_snow = (today - max_date).days

                # remove duplicates based on date and code
                snow_data = snow_data.drop_duplicates(subset=["date", "code"])

                temporal_data = pd.merge(temporal_data, snow_data, on=["date", "code"], how="left")
        else:
            offset_snow = None

        return {"temporal_data": temporal_data, "offset_date_snow": offset_snow}

    def load_snow_data(self, HRU: str, variable: str) -> tuple[pd.DataFrame, pd.Timestamp]:
        """
        Load the snow data from a csv file from the data-gateway
        """
        available_snow_vars = os.getenv("ieasyhydroforecast_SNOW_VARS").split(",")
        available_snow_hrus = os.getenv("ieasyhydroforecast_HRU_SNOW_DATA").split(",")

        assert variable in available_snow_vars, (
            f"Variable {variable} not in available snow variables: {available_snow_vars}"
        )
        assert HRU in available_snow_hrus, (
            f"HRU {HRU} not in available snow HRUs: {available_snow_hrus}"
        )

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

    def _clean_data(self, data: pd.DataFrame) -> pd.DataFrame:
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
        data = data.drop_duplicates(subset=["code", "date"], keep="last").reset_index(drop=True)

        # Step three - ensure time series is continuous - reindex
        # We expect that the data has EMCWF IFS Forecasts up to 15 days ahead
        emcwf_forecast_days_ahead = int(os.getenv("ieasyhydroforecast_ECMWF_IFS_lead_time"))
        today = get_today()
        end_date = today + pd.Timedelta(days=emcwf_forecast_days_ahead)
        data = data[data["date"] <= end_date].reset_index(drop=True)

        max_date_data = data["date"].max()
        if end_date > max_date_data:
            logger.warning(
                f"Data ends at {max_date_data}, but with EMCWF forecasts we expect data up to {end_date}. This indicates some missing data."
            )

        full_end_date = max(end_date, max_date_data)

        all_codes = data["code"].unique()
        full_date_range = pd.date_range(start=data["date"].min(), end=full_end_date, freq="D")

        full_index = pd.MultiIndex.from_product(
            [all_codes, full_date_range], names=["code", "date"]
        )
        data = data.set_index(["code", "date"]).reindex(full_index).reset_index()

        return data


class BasePredictorDataInterface:
    def __init__(self, station_codes=None):
        logger.info("Initialized BasePredictorDataInterface")
        sl.load_environment()
        self.station_codes = station_codes  # list[str] | None

        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = _api_client("postprocessing")
        return self._client

    def get_base_predictor_data_csv(
        self, model_name: str, data_path: str
    ) -> tuple[pd.DataFrame, list[str]]:
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

        # filter by today ; no future data
        today = get_today()
        base_data = base_data[base_data["date"] <= today].reset_index(drop=True)

        return base_data, base_models_cols

    def get_base_predictor_data_database(
        self, model_name: str, horizon_type: str = "month", horizon_value: int = 1
    ) -> tuple[pd.DataFrame, list[str]]:
        """
        Retrieve the base predictor dataset from the postprocessing API.

        Fetches from the /long-forecast/ endpoint and returns data in the same
        format as get_base_predictor_data_csv for seamless integration.

        Column Mapping (API -> DataFrame):
            q -> {model_name}
            q_xgb -> {model_name}_xgb
            q_lgbm -> {model_name}_lgbm
            q_catboost -> {model_name}_catboost
            q_loc -> {model_name}_loc

        Note: Quantile columns (q05-q95) are excluded to match CSV loading
        behavior where filter "Q_" in col excludes quantiles.

        Args:
            model_name: Name of the model (e.g., "SM_GBT", "LR_Base")
            horizon_type: Horizon type (default: "month")
            horizon_value: Lead time value (default: 1)

        Returns:
            Tuple[pd.DataFrame, List[str]]: DataFrame with predictions and
                list of model column names
        """
        today = get_today()

        df = _read_api_pages(
            self.client.read_long_term_forecasts,
            self.station_codes,
            model=model_name,
            horizon_type=horizon_type,
            horizon_value=horizon_value,
            end_date=today.strftime("%Y-%m-%d"),
        )

        if df.empty:
            logger.warning(
                f"No data found for model {model_name} "
                f"(horizon_type={horizon_type}, horizon_value={horizon_value})"
            )
            return df, []

        # Convert types
        df["date"] = pd.to_datetime(df["date"])
        df["code"] = df["code"].astype(int)

        # Determine which model columns have data and build column mapping
        # Maps DB column -> DataFrame column name
        potential_columns = {
            "q": model_name,
            "q_xgb": f"{model_name}_xgb",
            "q_lgbm": f"{model_name}_lgbm",
            "q_catboost": f"{model_name}_catboost",
            "q_loc": f"{model_name}_loc",
        }

        model_cols = []
        rename_mapping = {}

        for db_col, df_col in potential_columns.items():
            if db_col in df.columns and df[db_col].notna().any():
                rename_mapping[db_col] = df_col
                model_cols.append(df_col)

        # Rename columns
        df = df.rename(columns=rename_mapping)

        # Keep only date, code, and model columns
        cols_to_keep = ["date", "code"] + model_cols
        df = df[cols_to_keep]

        # Drop rows where all model columns are NaN
        if model_cols:
            df = df.dropna(subset=model_cols, how="all").reset_index(drop=True)

        return df, model_cols

    def load_all_dependencies_csv(
        self,
        all_dependencies_models: list[str],
        all_dependencies_paths: list[str],
    ) -> tuple[pd.DataFrame, list[str]]:
        """
        Loads all dependencies data from the provided paths.
        """

        all_predictions = None
        all_model_cols = []

        for model_name, model_path in zip(
            all_dependencies_models, all_dependencies_paths, strict=True
        ):
            base_data, base_models_cols = self.get_base_predictor_data_csv(
                model_name=model_name, data_path=model_path
            )

            if all_predictions is None:
                all_predictions = base_data
            else:
                all_predictions = pd.merge(
                    all_predictions, base_data, on=["date", "code"], how="inner"
                )

            all_model_cols.extend(base_models_cols)

        return all_predictions, all_model_cols

    def load_all_dependencies_database(
        self,
        all_dependencies_models: list[str],
        horizon_type: str = "month",
        horizon_value: int = 1,
    ) -> tuple[pd.DataFrame, list[str]]:
        """
        Loads all dependencies data from the database.

        Iterates through all dependency models, loads each from the database,
        and merges them using INNER JOIN on date and code (same as CSV version).

        Args:
            all_dependencies_models: List of model names to load as dependencies
            horizon_type: Horizon type for all models (default: "month")
            horizon_value: Lead time value for all models (default: 1)

        Returns:
            Tuple[pd.DataFrame, List[str]]: Merged DataFrame with all dependency
                predictions and list of all model column names
        """
        all_predictions = None
        all_model_cols = []

        for model_name in all_dependencies_models:
            base_data, base_model_cols = self.get_base_predictor_data_database(
                model_name=model_name, horizon_type=horizon_type, horizon_value=horizon_value
            )

            if base_data.empty:
                logger.warning(f"No database data found for dependency model {model_name}")
                continue

            if all_predictions is None:
                all_predictions = base_data
            else:
                all_predictions = pd.merge(
                    all_predictions, base_data, on=["date", "code"], how="inner"
                )

            all_model_cols.extend(base_model_cols)

        if all_predictions is None:
            logger.warning("No dependency data loaded from database")
            all_predictions = pd.DataFrame()

        return all_predictions, all_model_cols


def convert_na_to_nan(df):
    """Convert pd.NA to np.nan and revert to numpy dtypes."""
    result = df.copy()
    for col in result.columns:
        mask = result[col].isna()
        result[col] = result[col].astype(object)
        result.loc[mask, col] = np.nan
    return result.infer_objects()


# ─────────────────────────────────────────────────────────────────
# TESTING
# ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    di = DataInterfaceDB()
    tody = get_today()
    logger.info(f"Today's date: {tody}")
    # print("=" * 60)
    # print("Testing DataInterfaceDB")
    # print("=" * 60)

    # Test meteo data
    print("\n1. Testing get_meteo_data (Precipitation)...")
    t0 = time()
    rain = di.get_meteo_data(meteo_type="P")
    print(f"   Loaded {len(rain)} rows in {time() - t0:.3f}s")
    print(rain.head())

    # Test temperature
    print("\n2. Testing get_meteo_data (Temperature)...")
    t0 = time()
    temp = di.get_meteo_data(meteo_type="T")
    print(f"   Loaded {len(temp)} rows in {time() - t0:.3f}s")
    print(temp.head())

    # Test runoff
    print("\n3. Testing get_runoff_data...")
    t0 = time()
    runoff = di.get_runoff_data()
    print(f"   Loaded {len(runoff)} rows in {time() - t0:.3f}s")
    print(runoff.head())

    # Test forcing data
    print("\n4. Testing _load_forcing_data...")
    t0 = time()
    forcing = di._load_forcing_data(HRU="00003")
    print(f"   Loaded {len(forcing)} rows in {time() - t0:.3f}s")
    print(forcing.head())

    # Test full base data
    print("\n5. Testing get_base_data...")
    t0 = time()
    base_data = di.get_base_data(forcing_HRU="00003")
    print(f"   Loaded in {time() - t0:.3f}s")
    print(f"   Temporal shape: {base_data['temporal_data'].shape}")
    print(f"   Static shape: {base_data['static_data'].shape}")
    print(f"   Offset base: {base_data['offset_date_base']}")
    print(f"   Offset discharge: {base_data['offset_date_discharge']}")

    # Test extending with snow data
    print("\n6. Testing extend_base_data_with_snow...")
    t0 = time()
    extended_data = di.extend_base_data_with_snow(
        base_data=base_data["temporal_data"], HRUs_snow=["00003"], snow_variables=["RoF"]
    )

    print(f"   Loaded in {time() - t0:.3f}s")
    print(f"   Extended temporal shape: {extended_data['temporal_data'].shape}")
    print(" Head of extended data:")
    print(extended_data["temporal_data"].head())

    print("\n7. Testing extend_base_data_with_snow and elevation bands...")
    t0 = time()
    extended_data = di.extend_base_data_with_snow(
        base_data=base_data["temporal_data"], HRUs_snow=["KGZ500m"], snow_variables=["RoF"]
    )

    print(f"   Loaded in {time() - t0:.3f}s")
    print(f"   Extended temporal shape: {extended_data['temporal_data'].shape}")
    print(" Head of extended data:")
    print(extended_data["temporal_data"].head())

    # Just test different snow variable
    print("\n8. Testing extend_base_data_with_snow with different variable...")
    t0 = time()
    swe = di.get_snow_data(
        variable="RoF",
    )
    print(f"   Loaded in {time() - t0:.3f}s")
    print(f"   SWE shape: {swe.shape}")
    print(swe.head())
    print("Swe columns: ", swe.columns)

    # ─────────────────────────────────────────────────────────────────
    # Testing BasePredictorDataInterface (Database Loading)
    # ─────────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Testing BasePredictorDataInterface (Database)")
    print("=" * 60)

    bpi = BasePredictorDataInterface()

    # Test loading LR_Base from database
    print("\n8. Testing get_base_predictor_data_database (LR_Base)...")
    t0 = time()
    try:
        lr_data, lr_cols = bpi.get_base_predictor_data_database(
            model_name="LR_Base", horizon_type="month", horizon_value=1
        )
        print(f"   Loaded in {time() - t0:.3f}s")
        print(f"   Shape: {lr_data.shape}")
        print(f"   Columns: {lr_cols}")
        if not lr_data.empty:
            print(f"   Date range: {lr_data['date'].min()} to {lr_data['date'].max()}")
            print(f"   Unique codes: {lr_data['code'].nunique()}")
            print(lr_data.head())
        else:
            print("   No data found for LR_Base")
    except Exception as e:
        print(f"   Error: {e}")

    # Test loading MC_ALD from database
    print("\n9. Testing get_base_predictor_data_database (MC_ALD)...")
    t0 = time()
    try:
        mc_data, mc_cols = bpi.get_base_predictor_data_database(
            model_name="MC_ALD", horizon_type="month", horizon_value=1
        )
        print(f"   Loaded in {time() - t0:.3f}s")
        print(f"   Shape: {mc_data.shape}")
        print(f"   Columns: {mc_cols}")
        if not mc_data.empty:
            print(f"   Date range: {mc_data['date'].min()} to {mc_data['date'].max()}")
            print(f"   Unique codes: {mc_data['code'].nunique()}")
            print(mc_data.head())
        else:
            print("   No data found for MC_ALD")
    except Exception as e:
        print(f"   Error: {e}")

    # Test loading all dependencies from database
    print("\n10. Testing load_all_dependencies_database (LR_Base + MC_ALD)...")
    t0 = time()
    try:
        all_data, all_cols = bpi.load_all_dependencies_database(
            all_dependencies_models=["LR_Base", "MC_ALD"], horizon_type="month", horizon_value=1
        )
        print(f"   Loaded in {time() - t0:.3f}s")
        print(f"   Shape: {all_data.shape}")
        print(f"   All columns: {all_cols}")
        if not all_data.empty:
            print(f"   Date range: {all_data['date'].min()} to {all_data['date'].max()}")
            print(f"   Unique codes: {all_data['code'].nunique()}")
            print(all_data.head())
        else:
            print("   No merged data found")
    except Exception as e:
        print(f"   Error: {e}")
