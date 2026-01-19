"""
Client for SAPPHIRE Preprocessing API.

Handles runoff, hydrograph, meteorological, and snow data.
"""

import logging
from datetime import date
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from sapphire_api_client.client import SapphireAPIClient

logger = logging.getLogger(__name__)


class SapphirePreprocessingClient(SapphireAPIClient):
    """
    Client for the SAPPHIRE Preprocessing API.

    Provides methods for reading and writing:
    - Runoff data (daily time series)
    - Hydrograph data (statistical summaries)
    - Meteorological data (temperature, precipitation)
    - Snow data (height, SWE, runoff)

    Example:
        >>> client = SapphirePreprocessingClient()
        >>> df = client.read_runoff(horizon="day", code="12345")
        >>> records = client.prepare_runoff_records(df, "day", "12345")
        >>> client.write_runoff(records)
    """

    # Service prefix for API gateway routing
    SERVICE_PREFIX = "/api/preprocessing"

    # ==================== RUNOFF ====================

    def read_runoff(
        self,
        horizon: Optional[str] = None,
        code: Optional[str] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> pd.DataFrame:
        """
        Read runoff data from the API.

        Args:
            horizon: Horizon type filter (day, pentad, decade, month, season, year)
            code: Station code filter
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)
            skip: Number of records to skip (pagination)
            limit: Maximum records to return

        Returns:
            DataFrame with runoff data
        """
        params: Dict[str, Any] = {"skip": skip, "limit": limit}
        if horizon:
            params["horizon"] = horizon
        if code:
            params["code"] = code
        if start_date:
            params["start_date"] = str(start_date)
        if end_date:
            params["end_date"] = str(end_date)

        records = self._get("/runoff/", params=params)
        return pd.DataFrame(records) if records else pd.DataFrame()

    def write_runoff(self, records: List[Dict[str, Any]]) -> int:
        """
        Write runoff records to the API.

        Args:
            records: List of runoff records (use prepare_runoff_records to create)

        Returns:
            Number of records successfully written

        Raises:
            SapphireAPIError: If write fails
        """
        return self._post_batched("/runoff/", records)

    @staticmethod
    def prepare_runoff_records(
        df: pd.DataFrame,
        horizon_type: str,
        code: str,
        date_col: str = "date",
        discharge_col: str = "discharge",
        predictor_col: Optional[str] = "predictor",
        horizon_value_col: str = "horizon_value",
        horizon_in_year_col: str = "horizon_in_year",
    ) -> List[Dict[str, Any]]:
        """
        Prepare runoff records from a DataFrame.

        Args:
            df: Source DataFrame
            horizon_type: Horizon type (day, pentad, decade, month, season, year)
            code: Station code
            date_col: Name of date column
            discharge_col: Name of discharge column
            predictor_col: Name of predictor column (optional)
            horizon_value_col: Name of horizon value column
            horizon_in_year_col: Name of horizon in year column

        Returns:
            List of records ready for API
        """
        records = []
        for _, row in df.iterrows():
            record: Dict[str, Any] = {
                "horizon_type": horizon_type,
                "code": code,
                "date": str(row[date_col]),
                "discharge": row.get(discharge_col) if pd.notna(row.get(discharge_col)) else None,
                "horizon_value": int(row[horizon_value_col]),
                "horizon_in_year": int(row[horizon_in_year_col]),
            }
            if predictor_col and predictor_col in df.columns:
                val = row.get(predictor_col)
                record["predictor"] = val if pd.notna(val) else None
            records.append(record)
        return records

    # ==================== HYDROGRAPH ====================

    def read_hydrograph(
        self,
        horizon: Optional[str] = None,
        code: Optional[str] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> pd.DataFrame:
        """
        Read hydrograph data from the API.

        Args:
            horizon: Horizon type filter
            code: Station code filter
            start_date: Start date filter
            end_date: End date filter
            skip: Pagination offset
            limit: Maximum records

        Returns:
            DataFrame with hydrograph data
        """
        params: Dict[str, Any] = {"skip": skip, "limit": limit}
        if horizon:
            params["horizon"] = horizon
        if code:
            params["code"] = code
        if start_date:
            params["start_date"] = str(start_date)
        if end_date:
            params["end_date"] = str(end_date)

        records = self._get("/hydrograph/", params=params)
        return pd.DataFrame(records) if records else pd.DataFrame()

    def write_hydrograph(self, records: List[Dict[str, Any]]) -> int:
        """
        Write hydrograph records to the API.

        Args:
            records: List of hydrograph records

        Returns:
            Number of records written
        """
        return self._post_batched("/hydrograph/", records)

    @staticmethod
    def prepare_hydrograph_records(
        df: pd.DataFrame,
        horizon_type: str,
        code: str,
        date_col: str = "date",
        day_of_year_col: str = "day_of_year",
        horizon_value_col: str = "horizon_value",
        horizon_in_year_col: str = "horizon_in_year",
    ) -> List[Dict[str, Any]]:
        """
        Prepare hydrograph records from a DataFrame.

        Expects columns for statistical measures: count, mean, std, min, max,
        q05, q25, q50, q75, q95, norm, previous, current.

        Args:
            df: Source DataFrame
            horizon_type: Horizon type
            code: Station code
            date_col: Name of date column
            day_of_year_col: Name of day of year column
            horizon_value_col: Name of horizon value column
            horizon_in_year_col: Name of horizon in year column

        Returns:
            List of records ready for API
        """
        stat_cols = [
            "count", "mean", "std", "min", "max",
            "q05", "q25", "q50", "q75", "q95",
            "norm", "previous", "current"
        ]

        records = []
        for _, row in df.iterrows():
            record: Dict[str, Any] = {
                "horizon_type": horizon_type,
                "code": code,
                "date": str(row[date_col]),
                "day_of_year": int(row[day_of_year_col]),
                "horizon_value": int(row[horizon_value_col]),
                "horizon_in_year": int(row[horizon_in_year_col]),
            }
            # Add statistical columns
            for col in stat_cols:
                if col in df.columns:
                    val = row.get(col)
                    record[col] = val if pd.notna(val) else None
            records.append(record)
        return records

    # ==================== METEO ====================

    def read_meteo(
        self,
        meteo_type: Optional[str] = None,
        code: Optional[str] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> pd.DataFrame:
        """
        Read meteorological data from the API.

        Args:
            meteo_type: Type filter (T for temperature, P for precipitation)
            code: Station code filter
            start_date: Start date filter
            end_date: End date filter
            skip: Pagination offset
            limit: Maximum records

        Returns:
            DataFrame with meteo data
        """
        params: Dict[str, Any] = {"skip": skip, "limit": limit}
        if meteo_type:
            params["meteo_type"] = meteo_type
        if code:
            params["code"] = code
        if start_date:
            params["start_date"] = str(start_date)
        if end_date:
            params["end_date"] = str(end_date)

        records = self._get("/meteo/", params=params)
        return pd.DataFrame(records) if records else pd.DataFrame()

    def write_meteo(self, records: List[Dict[str, Any]]) -> int:
        """
        Write meteorological records to the API.

        Args:
            records: List of meteo records

        Returns:
            Number of records written
        """
        return self._post_batched("/meteo/", records)

    @staticmethod
    def prepare_meteo_records(
        df: pd.DataFrame,
        meteo_type: str,
        code: str,
        date_col: str = "date",
        value_col: str = "value",
        norm_col: Optional[str] = "norm",
        day_of_year_col: str = "day_of_year",
    ) -> List[Dict[str, Any]]:
        """
        Prepare meteorological records from a DataFrame.

        Args:
            df: Source DataFrame
            meteo_type: Type (T for temperature, P for precipitation)
            code: Station code
            date_col: Name of date column
            value_col: Name of value column
            norm_col: Name of norm column (optional)
            day_of_year_col: Name of day of year column

        Returns:
            List of records ready for API
        """
        records = []
        for _, row in df.iterrows():
            record: Dict[str, Any] = {
                "meteo_type": meteo_type,
                "code": code,
                "date": str(row[date_col]),
                "day_of_year": int(row[day_of_year_col]),
            }
            val = row.get(value_col)
            record["value"] = val if pd.notna(val) else None

            if norm_col and norm_col in df.columns:
                norm_val = row.get(norm_col)
                record["norm"] = norm_val if pd.notna(norm_val) else None

            records.append(record)
        return records

    # ==================== SNOW ====================

    def read_snow(
        self,
        snow_type: Optional[str] = None,
        code: Optional[str] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> pd.DataFrame:
        """
        Read snow data from the API.

        Args:
            snow_type: Type filter (HS, ROF, SWE)
            code: Station code filter
            start_date: Start date filter
            end_date: End date filter
            skip: Pagination offset
            limit: Maximum records

        Returns:
            DataFrame with snow data
        """
        params: Dict[str, Any] = {"skip": skip, "limit": limit}
        if snow_type:
            params["snow_type"] = snow_type
        if code:
            params["code"] = code
        if start_date:
            params["start_date"] = str(start_date)
        if end_date:
            params["end_date"] = str(end_date)

        records = self._get("/snow/", params=params)
        return pd.DataFrame(records) if records else pd.DataFrame()

    def write_snow(self, records: List[Dict[str, Any]]) -> int:
        """
        Write snow records to the API.

        Args:
            records: List of snow records

        Returns:
            Number of records written
        """
        return self._post_batched("/snow/", records)

    @staticmethod
    def prepare_snow_records(
        df: pd.DataFrame,
        snow_type: str,
        code: str,
        date_col: str = "date",
        value_col: str = "value",
        norm_col: Optional[str] = "norm",
    ) -> List[Dict[str, Any]]:
        """
        Prepare snow records from a DataFrame.

        The DataFrame may contain value1-value14 columns for zone-specific values.

        Args:
            df: Source DataFrame
            snow_type: Type (HS, ROF, SWE)
            code: Station code
            date_col: Name of date column
            value_col: Name of main value column
            norm_col: Name of norm column (optional)

        Returns:
            List of records ready for API
        """
        zone_cols = [f"value{i}" for i in range(1, 15)]

        records = []
        for _, row in df.iterrows():
            record: Dict[str, Any] = {
                "snow_type": snow_type,
                "code": code,
                "date": str(row[date_col]),
            }
            # Main value
            val = row.get(value_col)
            record["value"] = val if pd.notna(val) else None

            # Norm
            if norm_col and norm_col in df.columns:
                norm_val = row.get(norm_col)
                record["norm"] = norm_val if pd.notna(norm_val) else None

            # Zone values
            for col in zone_cols:
                if col in df.columns:
                    zone_val = row.get(col)
                    record[col] = zone_val if pd.notna(zone_val) else None

            records.append(record)
        return records
