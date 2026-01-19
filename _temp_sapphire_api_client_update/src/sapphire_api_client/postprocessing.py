"""
Client for SAPPHIRE Postprocessing API.

Handles forecasts, linear regression forecasts, and skill metrics.
"""

import logging
from datetime import date
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from sapphire_api_client.client import SapphireAPIClient

logger = logging.getLogger(__name__)


class SapphirePostprocessingClient(SapphireAPIClient):
    """
    Client for the SAPPHIRE Postprocessing API.

    Provides methods for reading and writing:
    - Forecasts
    - Linear regression forecasts
    - Skill metrics

    Note: The postprocessing API endpoints may differ from preprocessing.
    Update this client as the postprocessing service is developed.

    Example:
        >>> client = SapphirePostprocessingClient()
        >>> df = client.read_forecasts(horizon="pentad", code="12345")
    """

    # Service prefix for API gateway routing
    SERVICE_PREFIX = "/api/postprocessing"

    # ==================== FORECASTS ====================

    def read_forecasts(
        self,
        horizon: Optional[str] = None,
        code: Optional[str] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> pd.DataFrame:
        """
        Read forecast data from the API.

        Args:
            horizon: Horizon type filter
            code: Station code filter
            start_date: Start date filter
            end_date: End date filter
            skip: Pagination offset
            limit: Maximum records

        Returns:
            DataFrame with forecast data
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

        records = self._get("/forecasts/", params=params)
        return pd.DataFrame(records) if records else pd.DataFrame()

    def write_forecasts(self, records: List[Dict[str, Any]]) -> int:
        """
        Write forecast records to the API.

        Args:
            records: List of forecast records

        Returns:
            Number of records written
        """
        return self._post_batched("/forecasts/", records)

    @staticmethod
    def prepare_forecast_records(
        df: pd.DataFrame,
        horizon_type: str,
        code: str,
        date_col: str = "date",
        forecast_col: str = "forecast",
        lower_col: Optional[str] = "lower",
        upper_col: Optional[str] = "upper",
    ) -> List[Dict[str, Any]]:
        """
        Prepare forecast records from a DataFrame.

        Args:
            df: Source DataFrame
            horizon_type: Horizon type
            code: Station code
            date_col: Name of date column
            forecast_col: Name of forecast value column
            lower_col: Name of lower bound column (optional)
            upper_col: Name of upper bound column (optional)

        Returns:
            List of records ready for API
        """
        records = []
        for _, row in df.iterrows():
            record: Dict[str, Any] = {
                "horizon_type": horizon_type,
                "code": code,
                "date": str(row[date_col]),
            }
            # Forecast value
            val = row.get(forecast_col)
            record["forecast"] = val if pd.notna(val) else None

            # Confidence bounds
            if lower_col and lower_col in df.columns:
                lower_val = row.get(lower_col)
                record["lower"] = lower_val if pd.notna(lower_val) else None

            if upper_col and upper_col in df.columns:
                upper_val = row.get(upper_col)
                record["upper"] = upper_val if pd.notna(upper_val) else None

            records.append(record)
        return records

    # ==================== LR FORECASTS ====================

    def read_lr_forecasts(
        self,
        horizon: Optional[str] = None,
        code: Optional[str] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> pd.DataFrame:
        """
        Read linear regression forecast data from the API.

        Args:
            horizon: Horizon type filter
            code: Station code filter
            start_date: Start date filter
            end_date: End date filter
            skip: Pagination offset
            limit: Maximum records

        Returns:
            DataFrame with LR forecast data
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

        records = self._get("/lr-forecasts/", params=params)
        return pd.DataFrame(records) if records else pd.DataFrame()

    def write_lr_forecasts(self, records: List[Dict[str, Any]]) -> int:
        """
        Write linear regression forecast records to the API.

        Args:
            records: List of LR forecast records

        Returns:
            Number of records written
        """
        return self._post_batched("/lr-forecasts/", records)

    # ==================== SKILL METRICS ====================

    def read_skill_metrics(
        self,
        horizon: Optional[str] = None,
        code: Optional[str] = None,
        model: Optional[str] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> pd.DataFrame:
        """
        Read skill metrics from the API.

        Args:
            horizon: Horizon type filter
            code: Station code filter
            model: Model name filter
            skip: Pagination offset
            limit: Maximum records

        Returns:
            DataFrame with skill metrics
        """
        params: Dict[str, Any] = {"skip": skip, "limit": limit}
        if horizon:
            params["horizon"] = horizon
        if code:
            params["code"] = code
        if model:
            params["model"] = model

        records = self._get("/skill-metrics/", params=params)
        return pd.DataFrame(records) if records else pd.DataFrame()

    def write_skill_metrics(self, records: List[Dict[str, Any]]) -> int:
        """
        Write skill metric records to the API.

        Args:
            records: List of skill metric records

        Returns:
            Number of records written
        """
        return self._post_batched("/skill-metrics/", records)

    @staticmethod
    def prepare_skill_metric_records(
        df: pd.DataFrame,
        horizon_type: str,
        code: str,
        model: str,
    ) -> List[Dict[str, Any]]:
        """
        Prepare skill metric records from a DataFrame.

        Expects columns for metrics like: mae, rmse, nse, kge, bias, etc.

        Args:
            df: Source DataFrame
            horizon_type: Horizon type
            code: Station code
            model: Model name

        Returns:
            List of records ready for API
        """
        metric_cols = ["mae", "rmse", "nse", "kge", "bias", "r2", "pbias"]

        records = []
        for _, row in df.iterrows():
            record: Dict[str, Any] = {
                "horizon_type": horizon_type,
                "code": code,
                "model": model,
            }
            # Add metric columns
            for col in metric_cols:
                if col in df.columns:
                    val = row.get(col)
                    record[col] = val if pd.notna(val) else None
            records.append(record)
        return records
