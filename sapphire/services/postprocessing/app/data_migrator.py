"""
Data Migration Script for Forecast and Skill Metrics Data
Migrates CSV files (day, pentad, decade horizons) to PostgreSQL database
with performance statistics and batch processing
"""

import os
import json
import pandas as pd
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import time
import argparse
from dataclasses import dataclass
from abc import ABC, abstractmethod

from app.logger import logger
from app.config import settings


# Long-term forecast configuration
# Base paths (relative to intermediate_data or config root)
LT_PREDICTIONS_PATH = "long_term_predictions"  # {intermediate_data}/long_term_predictions/{mode}/{model}/
LT_CONFIG_PATH = "long_term_configs"  # {config_path}/long_term_configs/{mode}.json (note: plural)


@dataclass
class MigrationStats:
    """Statistics for data migration"""
    horizon_type: str
    total_records: int
    successful: int
    failed: int
    duration_seconds: float
    records_per_second: float
    batch_count: int

    def __str__(self):
        return (
            f"\n{'=' * 70}\n"
            f"Migration Statistics - {self.horizon_type.upper()}\n"
            f"{'=' * 70}\n"
            f"Total Records:        {self.total_records:,}\n"
            f"Successful:           {self.successful:,}\n"
            f"Failed:               {self.failed:,}\n"
            f"Success Rate:         {(self.successful / self.total_records * 100):.2f}%\n"
            f"Duration:             {self.duration_seconds:.2f} seconds\n"
            f"Throughput:           {self.records_per_second:.2f} records/sec\n"
            f"Batch Count:          {self.batch_count}\n"
            f"{'=' * 70}\n"
        )


class DataMigrator(ABC):
    """Abstract base class for data migration"""
    def __init__(self, api_base_url: str = "http://localhost:8000", batch_size: int = 1000, horizons: dict = None, sub_url: str = ""):
        self.api_base_url = api_base_url
        self.batch_size = batch_size
        self.horizons = horizons
        self.sub_url = sub_url
        self.session = requests.Session()

    @abstractmethod
    def prepare_pentad_data(self, df: pd.DataFrame) -> List[Dict]:
        """Prepare pentad (5-day) data for API"""
        pass

    @abstractmethod
    def prepare_decade_data(self, df: pd.DataFrame) -> List[Dict]:
        """Prepare decade (10-day) data for API"""
        pass

    def send_batch(self, batch: List[Dict], sub_url: str) -> Tuple[bool, int]:
        """Send a batch of records to the API"""
        try:
            payload = {"data": batch}
            response = self.session.post(
                f"{self.api_base_url}/{sub_url}/",
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            return True, len(batch)
        except requests.exceptions.RequestException as e:
            logger.error(f"Batch upload failed: {str(e)}")
            return False, 0

    def migrate_csv(self, csv_path: Path, horizon_type: str) -> MigrationStats:
        """Migrate a single CSV file

        Args:
            csv_path: Path to CSV file
            horizon_type: 'day', 'pentad', or 'decade'
        """
        print(f"Starting migration for {csv_path.name} ({horizon_type})")
        start_time = time.time()

        # Read CSV
        df = pd.read_csv(csv_path)
        total_records = len(df)
        print(f"Loaded {total_records:,} records from CSV")

        # Prepare data based on horizon type
        if horizon_type == "pentad":
            records = self.prepare_pentad_data(df)
        elif horizon_type == "decade":
            records = self.prepare_decade_data(df)
        else:
            raise ValueError(f"Unknown horizon type: {horizon_type}")

        # Send in batches
        successful = 0
        failed = 0
        batch_count = 0

        for i in range(0, len(records), self.batch_size):
            batch = records[i:i + self.batch_size]
            batch_count += 1

            success, count = self.send_batch(batch, self.sub_url)
            if success:
                successful += count
                print(f"Batch {batch_count}: {count} records uploaded successfully")
            else:
                failed += len(batch)
                print(f"Batch {batch_count}: Failed to upload {len(batch)} records")

        # Calculate statistics
        duration = time.time() - start_time
        rps = successful / duration if duration > 0 else 0

        stats = MigrationStats(
            horizon_type=horizon_type,
            total_records=total_records,
            successful=successful,
            failed=failed,
            duration_seconds=duration,
            records_per_second=rps,
            batch_count=batch_count
        )

        print(str(stats))
        return stats

    def migrate_all_horizons(self, csv_folder: str) -> List[MigrationStats]:
        """Migrate all horizon types (pentad, decade) for this data type"""
        all_stats = []

        for horizon_type, filename in self.horizons.items():
            csv_path = Path(csv_folder) / filename

            if not csv_path.exists():
                logger.warning(f"File not found: {csv_path}")
                continue

            try:
                stats = self.migrate_csv(csv_path, horizon_type)
                all_stats.append(stats)
            except Exception as e:
                logger.error(f"Migration failed for {horizon_type}: {str(e)}", exc_info=True)

        return all_stats


class CombinedForecastDataMigrator(DataMigrator):
    """Handles migration of forecast data from CSV to API"""

    def prepare_pentad_data(self, df: pd.DataFrame) -> List[Dict]:
        """Prepare pentad (5-day) runoff data for API"""
        records = []
        skip = 0
        for _, row in df.iterrows():
            if not pd.notna(row['pentad_in_year']):
                skip += 1
                continue
            if row['model_short'] != 'LR':
                record = {
                    "horizon_type": "pentad",
                    "code": str(row['code']),
                    "model_type": str(row['model_short']),
                    "date": row['date'],
                    "target": None,
                    "flag": int(row['flag']) if pd.notna(row['flag']) else None,
                    "horizon_value": int(row['pentad_in_month']),
                    "horizon_in_year": int(row['pentad_in_year']),
                    "q05": float(row['Q5']) if pd.notna(row['Q5']) else None,
                    "q25": float(row['Q25']) if pd.notna(row['Q25']) else None,
                    "q50": None,
                    "q75": float(row['Q75']) if pd.notna(row['Q75']) else None,
                    "q95": float(row['Q95']) if pd.notna(row['Q95']) else None,
                    "forecasted_discharge": float(row['forecasted_discharge']) if pd.notna(row['forecasted_discharge']) else None
                }
                records.append(record)
            if skip > 0:
                logger.debug(f"Skipped {skip} records due to missing pentad_in_year")
        return records

    def prepare_decade_data(self, df: pd.DataFrame) -> List[Dict]:
        """Prepare decade (10-day) runoff data for API"""
        records = []
        skip = 0
        for _, row in df.iterrows():
            if not pd.notna(row['decad_in_year']):
                skip += 1
                continue
            if row['model_short'] != 'LR':
                record = {
                    "horizon_type": "decade",
                    "code": str(row['code']),
                    "model_type": str(row['model_short']),
                    "date": row['date'],
                    "target": None,
                    "flag": int(row['flag']) if pd.notna(row['flag']) else None,
                    "horizon_value": int(row['decad']),
                    "horizon_in_year": int(row['decad_in_year']),
                    "q05": float(row['Q5']) if pd.notna(row['Q5']) else None,
                    "q25": float(row['Q25']) if pd.notna(row['Q25']) else None,
                    "q50": None,
                    "q75": float(row['Q75']) if pd.notna(row['Q75']) else None,
                    "q95": float(row['Q95']) if pd.notna(row['Q95']) else None,
                    "forecasted_discharge": float(row['forecasted_discharge']) if pd.notna(row['forecasted_discharge']) else None
                }
                records.append(record)
            if skip > 0:
                logger.debug(f"Skipped {skip} records due to missing decad_in_year")
        return records


class LRForecastDataMigrator(DataMigrator):
    """Handles migration of lr-forecast data from CSV to API"""

    def prepare_pentad_data(self, df: pd.DataFrame) -> List[Dict]:
        """Prepare pentad (5-day) runoff data for API"""
        records = []
        for _, row in df.iterrows():
            record = {
                    "horizon_type": "pentad",
                    "code": str(row['code']),
                    "date": row['date'],
                    "horizon_value": int(row['pentad_in_month']),
                    "horizon_in_year": int(row['pentad_in_year']),
                    "discharge_avg": float(row['discharge_avg']) if pd.notna(row['discharge_avg']) else None,
                    "predictor": float(row['predictor']) if pd.notna(row['predictor']) else None,
                    "slope": float(row['slope']) if pd.notna(row['slope']) else None,
                    "intercept": float(row['intercept']) if pd.notna(row['intercept']) else None,
                    "forecasted_discharge": float(row['forecasted_discharge']) if pd.notna(row['forecasted_discharge']) else None,
                    "q_mean": float(row['q_mean']) if pd.notna(row['q_mean']) else None,
                    "q_std_sigma": float(row['q_std_sigma']) if pd.notna(row['q_std_sigma']) else None,
                    "delta": float(row['delta']) if pd.notna(row['delta']) else None,
                    "rsquared": float(row['rsquared']) if pd.notna(row['rsquared']) else None
                }
            records.append(record)
        return records

    def prepare_decade_data(self, df: pd.DataFrame) -> List[Dict]:
        """Prepare decade (10-day) runoff data for API"""
        records = []
        for _, row in df.iterrows():
            record = {
                    "horizon_type": "decade",
                    "code": str(row['code']),
                    "date": row['date'],
                    "horizon_value": int(row['decad_in_month']),
                    "horizon_in_year": int(row['decad_in_year']),
                    "discharge_avg": float(row['discharge_avg']) if pd.notna(row['discharge_avg']) else None,
                    "predictor": float(row['predictor']) if pd.notna(row['predictor']) else None,
                    "slope": float(row['slope']) if pd.notna(row['slope']) else None,
                    "intercept": float(row['intercept']) if pd.notna(row['intercept']) else None,
                    "forecasted_discharge": float(row['forecasted_discharge']) if pd.notna(row['forecasted_discharge']) else None,
                    "q_mean": float(row['q_mean']) if pd.notna(row['q_mean']) else None,
                    "q_std_sigma": float(row['q_std_sigma']) if pd.notna(row['q_std_sigma']) else None,
                    "delta": float(row['delta']) if pd.notna(row['delta']) else None,
                    "rsquared": float(row['rsquared']) if pd.notna(row['rsquared']) else None
                }
            records.append(record)
        return records


class ForecastDataMigrator(DataMigrator):
    """Handles migration of prediction data from CSV to API"""

    def prepare_pentad_data(self, df: pd.DataFrame) -> List[Dict]:
        records = []
        for _, row in df.iterrows():
            record = {
                "horizon_type": "pentad",
                "code": str(row['code']),
                "model_type": self.model_type,
                "date": row['forecast_date'],
                "target": row['date'],
                "flag": int(row['flag']) if pd.notna(row['flag']) else None,
                "horizon_value": 0,
                "horizon_in_year": 0,
                "q05": float(row['Q5']) if pd.notna(row['Q5']) else None,
                "q25": float(row['Q25']) if pd.notna(row['Q25']) else None,
                "q50": float(row['Q50']) if pd.notna(row['Q50']) else None,
                "q75": float(row['Q75']) if pd.notna(row['Q75']) else None,
                "q95": float(row['Q95']) if pd.notna(row['Q95']) else None,
                "forecasted_discharge": None
            }
            records.append(record)
        return records

    def prepare_decade_data(self, df: pd.DataFrame) -> List[Dict]:
        records = []
        for _, row in df.iterrows():
            record = {
                "horizon_type": "decade",
                "code": str(row['code']),
                "model_type": self.model_type,
                "date": row['forecast_date'],
                "target": row['date'],
                "flag": int(row['flag']) if pd.notna(row['flag']) else None,
                "horizon_value": 0,
                "horizon_in_year": 0,
                "q05": float(row['Q5']) if pd.notna(row['Q5']) else None,
                "q25": float(row['Q25']) if pd.notna(row['Q25']) else None,
                "q50": float(row['Q50']) if pd.notna(row['Q50']) else None,
                "q75": float(row['Q75']) if pd.notna(row['Q75']) else None,
                "q95": float(row['Q95']) if pd.notna(row['Q95']) else None,
                "forecasted_discharge": None
            }
            records.append(record)
        return records

    def migrate_all_horizons(self, csv_folder: str) -> List[MigrationStats]:
        """Migrate all horizon types (pentad, decade) for this data type"""
        all_stats = []

        for horizon_type, model_filename in self.horizons.items():

            for model_type, filename in model_filename.items():

                self.model_type = model_type

                csv_path = Path(csv_folder) / filename

                if not csv_path.exists():
                    logger.warning(f"File not found: {csv_path}")
                    continue

                try:
                    stats = self.migrate_csv(csv_path, horizon_type)
                    all_stats.append(stats)
                except Exception as e:
                    logger.error(f"Migration failed for {horizon_type}: {str(e)}", exc_info=True)

        return all_stats


class SkillMetricDataMigrator(DataMigrator):
    """Handles migration of skill-metric data from CSV to API"""

    def prepare_pentad_data(self, df: pd.DataFrame) -> List[Dict]:
        """Prepare pentad (5-day) runoff data for API"""
        pentad_to_date = {
            1: "2025-12-31",
            2: "2026-01-05",
            3: "2026-01-10",
            4: "2026-01-15",
            5: "2026-01-20",
            6: "2026-01-25",
            7: "2026-01-31",
            8: "2026-02-05",
            9: "2026-02-10",
            10: "2026-02-15",
            11: "2026-02-20",
            12: "2026-02-25",
            13: "2026-02-28",
            14: "2026-03-05",
            15: "2026-03-10",
            16: "2026-03-15",
            17: "2026-03-20",
            18: "2026-03-25",
            19: "2026-03-31",
            20: "2026-04-05",
            21: "2026-04-10",
            22: "2026-04-15",
            23: "2026-04-20",
            24: "2026-04-25",
            25: "2026-04-30",
            26: "2026-05-05",
            27: "2026-05-10",
            28: "2026-05-15",
            29: "2026-05-20",
            30: "2026-05-25",
            31: "2026-05-31",
            32: "2026-06-05",
            33: "2026-06-10",
            34: "2026-06-15",
            35: "2026-06-20",
            36: "2026-06-25",
            37: "2026-06-30",
            38: "2026-07-05",
            39: "2026-07-10",
            40: "2026-07-15",
            41: "2026-07-20",
            42: "2026-07-25",
            43: "2026-07-31",
            44: "2026-08-05",
            45: "2026-08-10",
            46: "2026-08-15",
            47: "2026-08-20",
            48: "2026-08-25",
            49: "2026-08-31",
            50: "2026-09-05",
            51: "2026-09-10",
            52: "2026-09-15",
            53: "2026-09-20",
            54: "2026-09-25",
            55: "2026-09-30",
            56: "2026-10-05",
            57: "2026-10-10",
            58: "2026-10-15",
            59: "2026-10-20",
            60: "2026-10-25",
            61: "2026-10-31",
            62: "2026-11-05",
            63: "2026-11-10",
            64: "2026-11-15",
            65: "2026-11-20",
            66: "2026-11-25",
            67: "2026-11-30",
            68: "2026-12-05",
            69: "2026-12-10",
            70: "2026-12-15",
            71: "2026-12-20",
            72: "2026-12-25",
        }
        records = []
        for _, row in df.iterrows():
            horizon = int(row['pentad_in_year'])
            record = {
                "horizon_type": "pentad",
                "code": str(row['code']),
                "model_type": str(row['model_short']),
                "date": pentad_to_date[horizon],
                "horizon_in_year": horizon,
                "sdivsigma": float(row['sdivsigma']) if pd.notna(row['sdivsigma']) else None,
                "nse": float(row['nse']) if pd.notna(row['nse']) else None,
                "delta": float(row['delta']) if pd.notna(row['delta']) else None,
                "accuracy": float(row['accuracy']) if pd.notna(row['accuracy']) else None,
                "mae": float(row['mae']) if pd.notna(row['mae']) else None,
                "n_pairs": float(row['n_pairs']) if pd.notna(row['n_pairs']) else None
            }
            records.append(record)

        # Deduplicate by unique key (keeping first occurrence)
        seen = set()
        unique_records = []
        for record in records:
            key = (record['horizon_type'], record['code'], record['model_type'],
                   record['date'], record['horizon_in_year'])
            if key not in seen:
                seen.add(key)
                unique_records.append(record)

        if len(unique_records) < len(records):
            logger.info(f"Deduplicated {len(records) - len(unique_records)} duplicate pentad skill metrics")

        return unique_records

    def prepare_decade_data(self, df: pd.DataFrame) -> List[Dict]:
        """Prepare decade (10-day) runoff data for API"""
        decade_to_date = {
            1: "2025-12-31",
            2: "2026-01-10",
            3: "2026-01-20",
            4: "2026-01-31",
            5: "2026-02-10",
            6: "2026-02-20",
            7: "2026-02-28",
            8: "2026-03-10",
            9: "2026-03-20",
            10: "2026-03-31",
            11: "2026-04-10",
            12: "2026-04-20",
            13: "2026-04-30",
            14: "2026-05-10",
            15: "2026-05-20",
            16: "2026-05-31",
            17: "2026-06-10",
            18: "2026-06-20",
            19: "2026-06-30",
            20: "2026-07-10",
            21: "2026-07-20",
            22: "2026-07-31",
            23: "2026-08-10",
            24: "2026-08-20",
            25: "2026-08-31",
            26: "2026-09-10",
            27: "2026-09-20",
            28: "2026-09-30",
            29: "2026-10-10",
            30: "2026-10-20",
            31: "2026-10-31",
            32: "2026-11-10",
            33: "2026-11-20",
            34: "2026-11-30",
            35: "2026-12-10",
            36: "2026-12-20",
        }
        records = []
        for _, row in df.iterrows():
            horizon = int(row['decad_in_year'])
            record = {
                "horizon_type": "decade",
                "code": str(row['code']),
                "model_type": str(row['model_short']),
                "date": decade_to_date[horizon],
                "horizon_in_year": horizon,
                "sdivsigma": float(row['sdivsigma']) if pd.notna(row['sdivsigma']) else None,
                "nse": float(row['nse']) if pd.notna(row['nse']) else None,
                "delta": float(row['delta']) if pd.notna(row['delta']) else None,
                "accuracy": float(row['accuracy']) if pd.notna(row['accuracy']) else None,
                "mae": float(row['mae']) if pd.notna(row['mae']) else None,
                "n_pairs": float(row['n_pairs']) if pd.notna(row['n_pairs']) else None
            }
            records.append(record)

        # Deduplicate by unique key (keeping first occurrence)
        seen = set()
        unique_records = []
        for record in records:
            key = (record['horizon_type'], record['code'], record['model_type'],
                   record['date'], record['horizon_in_year'])
            if key not in seen:
                seen.add(key)
                unique_records.append(record)

        if len(unique_records) < len(records):
            logger.info(f"Deduplicated {len(records) - len(unique_records)} duplicate decade skill metrics")

        return unique_records


class LongForecastDataMigrator(DataMigrator):
    """Handles migration of long-term forecast data from CSV to API

    File structure:
        Configs: {config_path}/long_term_config/{forecast_mode}.json
        Data: {intermediate_data}/long_term_predictions/{forecast_mode}/{model}/{model}_hindcast.csv

    Config JSON structure:
        {
            "models_to_use": {"family1": ["model1", "model2"], ...},
            "operational_month_lead_time": 1
        }
    """

    def __init__(
        self,
        api_base_url: str = "http://localhost:8000",
        batch_size: int = 1000,
        horizons: dict = None,
        sub_url: str = "long-forecast",
        horizon_type: str = "month",
        horizon_value: int = 1,
        forecast_mode: str = None
    ):
        super().__init__(api_base_url, batch_size, horizons, sub_url)
        self.horizon_type = horizon_type
        self.horizon_value = horizon_value
        self.forecast_mode = forecast_mode

    @staticmethod
    def load_forecast_mode_config(config_path: str, forecast_mode: str) -> dict:
        """Load configuration for a specific forecast mode

        Args:
            config_path: Base config path (e.g., /config)
            forecast_mode: Mode name (e.g., month_1)

        Returns:
            Dict with 'models' list and 'horizon_value' int
        """
        config_file = Path(config_path) / LT_CONFIG_PATH / f"{forecast_mode}.json"

        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_file}")

        with open(config_file, 'r') as f:
            config = json.load(f)

        # Extract all models from models_to_use dict
        models = []
        models_to_use = config.get("models_to_use", {})
        for family, model_list in models_to_use.items():
            models.extend(model_list)

        horizon_value = config["operational_month_lead_time"]

        return {
            "models": models,
            "horizon_value": horizon_value,
            "raw_config": config
        }

    @staticmethod
    def discover_forecast_modes(config_path: str) -> List[str]:
        """Discover available forecast modes from config directory

        Args:
            config_path: Base config path (e.g., /config)

        Returns:
            List of forecast mode names (e.g., ['month_0', 'month_1', 'month_2'])
        """
        config_dir = Path(config_path) / LT_CONFIG_PATH

        if not config_dir.exists():
            logger.warning(f"Config directory not found: {config_dir}")
            return []

        modes = []
        for config_file in config_dir.glob("*.json"):
            mode_name = config_file.stem  # filename without extension
            modes.append(mode_name)

        return sorted(modes)

    @staticmethod
    def build_horizons_dict(
        data_path: str,
        forecast_mode: str,
        models: List[str]
    ) -> dict:
        """Build horizons dict with file paths for each model

        Args:
            data_path: Base data path (e.g., /intermediate_data)
            forecast_mode: Mode name (e.g., month_1)
            models: List of model names

        Returns:
            Dict like {"month": {"LR_SM": "/path/to/LR_SM_hindcast.csv", ...}}
        """
        return {
            "month": {
                model: str(
                    Path(data_path) / LT_PREDICTIONS_PATH / forecast_mode / model / f"{model}_hindcast.csv"
                )
                for model in models
            }
        }

    def prepare_pentad_data(self, df: pd.DataFrame) -> List[Dict]:
        """Not used for long-term forecasts"""
        raise NotImplementedError("Long-term forecasts use prepare_month_data")

    def prepare_decade_data(self, df: pd.DataFrame) -> List[Dict]:
        """Not used for long-term forecasts"""
        raise NotImplementedError("Long-term forecasts use prepare_month_data")

    def prepare_month_data(self, df: pd.DataFrame, model_name: str) -> List[Dict]:
        """Prepare monthly long-term forecast data for API

        Args:
            df: DataFrame with hindcast data
            model_name: Name of the model (e.g., "LR_Base", "GBT")

        Returns:
            List of dictionaries ready for API submission
        """
        records = []
        q_model_col = f"Q_{model_name}"

        # Identify quantile columns if present
        quantile_mapping = {
            "Q5": "q05", "Q10": "q10", "Q25": "q25", "Q50": "q50",
            "Q75": "q75", "Q90": "q90", "Q95": "q95"
        }

        # Identify ensemble columns if present
        ensemble_suffixes = {
            "_xgb": "q_xgb",
            "_lgbm": "q_lgbm",
            "_catboost": "q_catboost",
            "_loc" : "q_loc"
        }

        for _, row in df.iterrows():
            # Skip rows with missing required dates
            required_cols = ['date', 'valid_from', 'valid_to']
            if not all(pd.notna(row.get(c)) for c in required_cols):
                continue

            record = {
                "horizon_type": self.horizon_type,
                "horizon_value": self.horizon_value,
                "code": str(int(row['code'])) if pd.notna(row.get('code')) else None,
                "date": pd.to_datetime(row['date']).strftime('%Y-%m-%d'),
                "model_type": model_name,
                "valid_from": pd.to_datetime(row['valid_from']).strftime('%Y-%m-%d'),
                "valid_to": pd.to_datetime(row['valid_to']).strftime('%Y-%m-%d'),
                "flag": int(row['flag']) if pd.notna(row.get('flag')) else None,
            }

            # Skip rows with invalid code
            if record["code"] is None:
                continue

            # Main model output column (Q_{model_name})
            if q_model_col in df.columns and pd.notna(row.get(q_model_col)):
                record["q"] = float(row[q_model_col])


            # Quantile columns (Q5, Q10, ..., Q95)
            for csv_col, api_col in quantile_mapping.items():
                if csv_col in df.columns and pd.notna(row.get(csv_col)):
                    record[api_col] = float(row[csv_col])

            # Ensemble components (GBT models)
            for suffix, api_col in ensemble_suffixes.items():
                csv_col = f"Q_{model_name}{suffix}"
                if csv_col in df.columns and pd.notna(row.get(csv_col)):
                    record[api_col] = float(row[csv_col])

            # Q_loc for uncertainty models
            if "Q_loc" in df.columns and pd.notna(row.get("Q_loc")):
                record["q_loc"] = float(row["Q_loc"])

            records.append(record)

        return records

    def migrate_long_forecast_csv(
        self, csv_path: Path, model_name: str
    ) -> MigrationStats:
        """Migrate a single long-term forecast CSV file

        Args:
            csv_path: Path to the CSV file
            model_name: Name of the model for column mapping

        Returns:
            MigrationStats with migration statistics
        """
        print(f"Starting migration for {csv_path.name} (model: {model_name})")
        start_time = time.time()

        logger.info(f"Reading long-term forecast CSV: {csv_path} for model {model_name}")
        # Read CSV
        df = pd.read_csv(csv_path)
        total_records = len(df)
        print(f"Loaded {total_records:,} records from CSV")

        # Prepare data
        records = self.prepare_month_data(df, model_name)
        print(f"Prepared {len(records):,} records for API")

        # Send in batches
        successful = 0
        failed = 0
        batch_count = 0

        print(f"Now uploading in batches of {self.batch_size} records...")

        for i in range(0, len(records), self.batch_size):
            batch = records[i:i + self.batch_size]
            batch_count += 1

            success, count = self.send_batch(batch, self.sub_url)
            if success:
                successful += count
                print(f"Batch {batch_count}: {count} records uploaded successfully")
            else:
                failed += len(batch)
                print(f"Batch {batch_count}: Failed to upload {len(batch)} records")

        # Calculate statistics
        duration = time.time() - start_time
        rps = successful / duration if duration > 0 else 0

        stats = MigrationStats(
            horizon_type=f"{self.horizon_type}_{model_name}",
            total_records=len(records),
            successful=successful,
            failed=failed,
            duration_seconds=duration,
            records_per_second=rps,
            batch_count=batch_count
        )

        print(str(stats))
        return stats

    def migrate_all_horizons(self, csv_folder: str) -> List[MigrationStats]:
        """Migrate long-term forecasts for all specified models

        Args:
            csv_folder: Base folder for CSV files (may be overridden by full paths)

        Returns:
            List of MigrationStats for each model migrated
        """
        all_stats = []

        # horizons format: {"month": {"LR_Base": "/path/to/LR_Base_hindcast.csv", ...}}
        for horizon_type, model_files in self.horizons.items():
            for model_name, filepath in model_files.items():
                # If filepath is absolute, use it directly; otherwise, join with csv_folder
                if os.path.isabs(filepath):
                    csv_path = Path(filepath)
                else:
                    csv_path = Path(csv_folder) / filepath

                if not csv_path.exists():
                    logger.warning(f"File not found: {csv_path}")
                    continue

                try:
                    stats = self.migrate_long_forecast_csv(csv_path, model_name)
                    all_stats.append(stats)
                except Exception as e:
                    logger.error(
                        f"Migration failed for {model_name}: {str(e)}",
                        exc_info=True
                    )

        return all_stats


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Migrate runoff, hydrograph and meteo data from CSV to database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Migrate only combined forecast data without linear regression
  python data_migrator.py --type combinedforecast

  # Migrate only lr forecast data
  python data_migrator.py --type lrforecast

  # Migrate only forecast (prediction) data
  python data_migrator.py --type forecast

  # Migrate only skill metrics data
  python data_migrator.py --type skillmetric

  # Migrate only long-term forecast data
  python data_migrator.py --type longforecast

  # Migrate long-term forecasts for specific modes
  python data_migrator.py --type longforecast --modes monthly

  # Migrate long-term forecasts for specific models
  python data_migrator.py --type longforecast --modes monthly --model-filter LR_Base,GBT

  # Migrate all forecast and skill metrics data
  python data_migrator.py --type all

  # Use custom batch size
  python data_migrator.py --type all --batch-size 2000
        """
    )

    parser.add_argument(
        '--type',
        choices=[
            'combinedforecast', 'lrforecast', 'forecast',
            'skillmetric', 'longforecast', 'all'
        ],
        default='all',
        help='Type of data to migrate (default: all)'
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=None,
        help='Batch size for migration (default: from settings)'
    )

    parser.add_argument(
        '--modes',
        type=str,
        default=None,
        help='Comma-separated forecast modes for longforecast (e.g., monthly,seasonal). '
             'If not specified, uses all supported modes from config.'
    )

    parser.add_argument(
        '--model-filter',
        type=str,
        default=None,
        help='Filter to specific models within a mode (e.g., LR_Base,GBT). '
             'If not specified, migrates all models in the config.'
    )

    return parser.parse_args()


def main():
    """Main migration execution"""
    # Parse command line arguments
    args = parse_arguments()

    # Configuration - use command line args if provided, otherwise use settings
    API_URL = settings.api_base_url
    BATCH_SIZE = args.batch_size if args.batch_size else settings.batch_size  # Adjust based on your API performance
    CSV_FOLDER = settings.csv_folder

    print("=" * 70)
    print("DATA MIGRATION SCRIPT")
    print("=" * 70)
    print(f"Migration Type: {args.type.upper()}")
    print(f"API URL:        {API_URL}")
    print(f"Batch Size:     {BATCH_SIZE}")
    print(f"CSV Folder:     {CSV_FOLDER}")
    print("=" * 70)

    # Track overall statistics
    all_stats = []
    overall_start = time.time()

    # Determine which migrators to run
    migrators_to_run = []

    if args.type in ['combinedforecast', 'all']:
        horizons = {
            "pentad": "combined_forecasts_pentad_latest.csv",
            "decade": "combined_forecasts_decad_latest.csv"
        }
        migrators_to_run.append(CombinedForecastDataMigrator(API_URL, BATCH_SIZE, horizons, sub_url="forecast"))

    if args.type in ['lrforecast', 'all']:
        horizons = {
            "pentad": "forecast_pentad_linreg_latest.csv",
            "decade": "forecast_decad_linreg_latest.csv"
        }
        migrators_to_run.append(LRForecastDataMigrator(API_URL, BATCH_SIZE, horizons, sub_url="lr-forecast"))

    if args.type in ['forecast', 'all']:
        horizons = {
            "pentad": {
                "TFT": "predictions/TFT/pentad_TFT_forecast_latest.csv",
                "TiDE": "predictions/TIDE/pentad_TIDE_forecast_latest.csv",
                "TSMixer": "predictions/TSMIXER/pentad_TSMIXER_forecast_latest.csv"
            },
            "decade": {
                "TFT": "predictions/TFT/decad_TFT_forecast_latest.csv",
                "TiDE": "predictions/TIDE/decad_TIDE_forecast_latest.csv",
                "TSMixer": "predictions/TSMIXER/decad_TSMIXER_forecast_latest.csv"
            }
        }
        migrators_to_run.append(ForecastDataMigrator(API_URL, BATCH_SIZE, horizons, sub_url="forecast"))

    if args.type in ['skillmetric', 'all']:
        horizons = {
            "pentad": "skill_metrics_pentad.csv",
            "decade": "skill_metrics_decad.csv"
        }
        migrators_to_run.append(SkillMetricDataMigrator(API_URL, BATCH_SIZE, horizons, sub_url="skill-metric"))

    if args.type in ['longforecast', 'all']:
        # Long-term forecast migration using hardcoded paths
        # Structure: {csv_folder}/long_term_predictions/{mode}/{model}/{model}_hindcast.csv
        # Config: {config_folder}/long_term_config/{mode}.json
        CONFIG_FOLDER = settings.config_folder

        # Discover or use specified forecast modes
        if args.modes:
            forecast_modes = [m.strip() for m in args.modes.split(',')]
        else:
            # Auto-discover available modes from config directory
            forecast_modes = LongForecastDataMigrator.discover_forecast_modes(CONFIG_FOLDER)

        if not forecast_modes:
            logger.warning(
                f"No forecast modes found. Check config directory: "
                f"{CONFIG_FOLDER}/{LT_CONFIG_PATH}/"
            )

        for forecast_mode in forecast_modes:
            try:
                # Load config for this mode
                config_data = LongForecastDataMigrator.load_forecast_mode_config(
                    CONFIG_FOLDER, forecast_mode
                )

                models = config_data["models"]
                horizon_value = config_data["horizon_value"]

                # Filter by --model-filter argument if provided
                if args.model_filter:
                    filter_models = [m.strip() for m in args.model_filter.split(',')]
                    models = [m for m in models if m in filter_models]

                if not models:
                    logger.warning(f"No models to migrate for {forecast_mode}")
                    continue

                # Build horizons dict with paths
                horizons = LongForecastDataMigrator.build_horizons_dict(
                    CSV_FOLDER, forecast_mode, models
                )

                migrators_to_run.append(
                    LongForecastDataMigrator(
                        api_base_url=API_URL,
                        batch_size=BATCH_SIZE,
                        horizons=horizons,
                        sub_url="long-forecast",
                        horizon_type="month",
                        horizon_value=horizon_value,
                        forecast_mode=forecast_mode
                    )
                )

                logger.info(
                    f"Configured migration for {forecast_mode}: "
                    f"{len(models)} models, horizon_value={horizon_value}"
                )

            except FileNotFoundError as e:
                logger.error(f"Config not found for {forecast_mode}: {e}")
            except Exception as e:
                logger.error(f"Failed to configure {forecast_mode}: {e}")

    # Run migrations
    for migrator in migrators_to_run:
        stats = migrator.migrate_all_horizons(CSV_FOLDER)
        all_stats.extend(stats)

    # Print summary
    overall_duration = time.time() - overall_start
    total_records = sum(s.total_records for s in all_stats)
    total_successful = sum(s.successful for s in all_stats)
    total_failed = sum(s.failed for s in all_stats)

    print("\n" + "=" * 70)
    print("OVERALL MIGRATION SUMMARY")
    print("=" * 70)
    print(f"Migration Type:           {args.type.upper()}")
    print(f"Total Records Processed:  {total_records:,}")
    print(f"Total Successful:         {total_successful:,}")
    print(f"Total Failed:             {total_failed:,}")
    if total_records > 0:
        print(f"Success Rate:             {(total_successful / total_records * 100):.2f}%")
    else:
        print("Success Rate:             N/A")
    print(f"Total Duration:           {overall_duration:.2f} seconds ({overall_duration / 60:.2f} minutes)")
    if overall_duration > 0:
        print(f"Overall Throughput:       {total_successful / overall_duration:.2f} records/sec")
    else:
        print("Overall Throughput:       N/A")
    print("=" * 70)

    # Individual statistics
    if all_stats:
        print("\nDetailed Statistics:")
        for stats in all_stats:
            print(stats)
    else:
        logger.warning("No data was migrated. Check if CSV files exist in the specified folder.")


if __name__ == "__main__":
    main()
