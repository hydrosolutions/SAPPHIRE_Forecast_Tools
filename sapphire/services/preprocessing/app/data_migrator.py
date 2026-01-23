"""
Data Migration Script for Runoff, Hydrograph and Meteo Data
Migrates CSV files (day, pentad, decade horizons) to PostgreSQL database
with performance statistics and batch processing
"""

import pandas as pd
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
import time
import argparse
from dataclasses import dataclass
from abc import ABC, abstractmethod

from app.logger import logger
from app.config import settings


@dataclass
class MigrationStats:
    """Statistics for data migration"""
    data_type: str  # runoff, hydrograph, meteo
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
            f"Migration Statistics - {self.data_type.upper()} ({self.horizon_type.upper()})\n"
            f"{'=' * 70}\n"
            f"Total Records:        {self.total_records:,}\n"
            f"Successful:           {self.successful:,}\n"
            f"Failed:               {self.failed:,}\n"
            f"Success Rate:         {(self.successful/self.total_records*100):.2f}%\n"
            f"Duration:             {self.duration_seconds:.2f} seconds\n"
            f"Throughput:           {self.records_per_second:.2f} records/sec\n"
            f"Batch Count:          {self.batch_count}\n"
            f"{'=' * 70}\n"
        )


class DataMigrator(ABC):
    """Abstract base class for data migration"""

    def __init__(self, api_base_url: str = "http://localhost:8000", batch_size: int = 1000):
        self.api_base_url = api_base_url
        self.batch_size = batch_size
        self.session = requests.Session()
        self.data_type = self.get_data_type()

    @abstractmethod
    def get_data_type(self) -> str:
        """Return the data type name ('runoff', 'hydrograph', 'meteo')"""
        pass

    @abstractmethod
    def prepare_day_data(self, df: pd.DataFrame) -> List[Dict]:
        """Prepare daily data for API"""
        pass

    @abstractmethod
    def prepare_pentad_data(self, df: pd.DataFrame) -> List[Dict]:
        """Prepare pentad (5-day) data for API"""
        pass

    @abstractmethod
    def prepare_decade_data(self, df: pd.DataFrame) -> List[Dict]:
        """Prepare decade (10-day) data for API"""
        pass

    def send_batch(self, batch: List[Dict]) -> Tuple[bool, int]:
        """Send a batch of records to the API"""
        try:
            payload = {"data": batch}
            response = self.session.post(
                f"{self.api_base_url}/{self.data_type}/",
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
        print(f"Starting migration for {csv_path.name} ({self.data_type} - {horizon_type})")
        start_time = time.time()

        # Read CSV
        df = pd.read_csv(csv_path)
        total_records = len(df)
        print(f"Loaded {total_records:,} records from CSV")

        # Prepare data based on horizon type
        if horizon_type == "day":
            records = self.prepare_day_data(df)
        elif horizon_type == "pentad":
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

            success, count = self.send_batch(batch)
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
            data_type=self.data_type,
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
        """Migrate all horizon types (day, pentad, decade) for this data type"""
        all_stats = []

        horizons = {
            "day": f"{self.data_type}_day.csv",
            "pentad": f"{self.data_type}_pentad.csv",
            "decade": f"{self.data_type}_decad.csv"
        }

        for horizon_type, filename in horizons.items():
            csv_path = Path(csv_folder) / filename

            if not csv_path.exists():
                logger.warning(f"File not found: {csv_path}")
                continue

            try:
                stats = self.migrate_csv(csv_path, horizon_type)
                all_stats.append(stats)
            except Exception as e:
                logger.error(f"Migration failed for {self.data_type} {horizon_type}: {str(e)}", exc_info=True)

        return all_stats


class RunoffDataMigrator(DataMigrator):
    """Handles migration of runoff data from CSV to API"""

    def get_data_type(self) -> str:
        return "runoff"

    def prepare_day_data(self, df: pd.DataFrame) -> List[Dict]:
        """Prepare daily runoff data for API"""
        records = []
        for _, row in df.iterrows():
            date_obj = pd.to_datetime(row['date'])
            record = {
                "horizon_type": "day",
                "code": str(row['code']),
                "date": row['date'],
                "discharge": float(row['discharge']) if pd.notna(row['discharge']) else None,
                "predictor": None,  # Daily data doesn't have predictor
                "horizon_value": date_obj.day,
                "horizon_in_year": date_obj.dayofyear
            }
            records.append(record)
        return records

    def prepare_pentad_data(self, df: pd.DataFrame) -> List[Dict]:
        """Prepare pentad (5-day) runoff data for API"""
        records = []
        for _, row in df.iterrows():
            record = {
                "horizon_type": "pentad",
                "code": str(row['code']),
                "date": row['date'],
                "discharge": float(row['discharge_avg']) if pd.notna(row['discharge_avg']) else None,
                "predictor": float(row['predictor']) if pd.notna(row['predictor']) else None,
                "horizon_value": int(row['pentad']),
                "horizon_in_year": int(row['pentad_in_year'])
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
                "discharge": float(row['discharge_avg']) if pd.notna(row['discharge_avg']) else None,
                "predictor": float(row['predictor']) if pd.notna(row['predictor']) else None,
                "horizon_value": int(row['decad_in_year']),
                "horizon_in_year": int(row['decad_in_year'])
            }
            records.append(record)
        return records


class HydrographDataMigrator(DataMigrator):
    """Handles migration of hydrograph data from CSV to API"""

    def get_data_type(self) -> str:
        return "hydrograph"

    def prepare_day_data(self, df: pd.DataFrame) -> List[Dict]:
        """Prepare daily hydrograph data for API"""
        records = []
        for _, row in df.iterrows():
            date_obj = pd.to_datetime(row['date'])
            record = {
                "horizon_type": "day",
                "code": str(row['code']),
                "date": row['date'],
                "horizon_value": date_obj.day,
                "horizon_in_year": int(row['day_of_year']),
                "day_of_year": int(row['day_of_year']),
                "count": int(row['count']) if pd.notna(row['count']) else None,
                "mean": float(row['mean']) if pd.notna(row['mean']) else None,
                "std": float(row['std']) if pd.notna(row['std']) else None,
                "min": float(row['min']) if pd.notna(row['min']) else None,
                "max": float(row['max']) if pd.notna(row['max']) else None,
                "q05": float(row['5%']) if pd.notna(row['5%']) else None,
                "q25": float(row['25%']) if pd.notna(row['25%']) else None,
                "q50": float(row['50%']) if pd.notna(row['50%']) else None,
                "q75": float(row['75%']) if pd.notna(row['75%']) else None,
                "q95": float(row['95%']) if pd.notna(row['95%']) else None,
                "norm": None,  # Not in sample data
                "previous": float(row['2024']) if pd.notna(row.get('2024')) else None,
                "current": float(row['2025']) if pd.notna(row.get('2025')) else None
            }
            records.append(record)
        return records

    def prepare_pentad_data(self, df: pd.DataFrame) -> List[Dict]:
        """Prepare pentad (5-day) hydrograph data for API"""
        records = []
        for _, row in df.iterrows():
            if not pd.notna(row['pentad']):
                continue
            record = {
                "horizon_type": "pentad",
                "code": str(row['code']),
                "date": row['date'],
                "horizon_value": int(row['pentad']),
                "horizon_in_year": int(row['pentad_in_year']),
                "day_of_year": int(row['day_of_year']),
                "count": None,  # Not in sample data
                "mean": float(row['mean']) if pd.notna(row['mean']) else None,
                "std": None,  # Not in sample data
                "min": float(row['min']) if pd.notna(row['min']) else None,
                "max": float(row['max']) if pd.notna(row['max']) else None,
                "q05": float(row['q05']) if pd.notna(row['q05']) else None,
                "q25": float(row['q25']) if pd.notna(row['q25']) else None,
                "q50": None,  # Not in sample data
                "q75": float(row['q75']) if pd.notna(row['q75']) else None,
                "q95": float(row['q95']) if pd.notna(row['q95']) else None,
                "norm": float(row['norm']) if pd.notna(row.get('norm')) else None,
                "previous": float(row['2024']) if pd.notna(row.get('2024')) else None,
                "current": float(row['2025']) if pd.notna(row.get('2025')) else None
            }
            records.append(record)
        return records

    def prepare_decade_data(self, df: pd.DataFrame) -> List[Dict]:
        """Prepare decade (10-day) hydrograph data for API"""
        records = []
        for _, row in df.iterrows():
            date_obj = pd.to_datetime(row['date'])
            decade_value = ((date_obj.day - 1) // 10) + 1
            if not pd.notna(row['day_of_year']):
                continue
            record = {
                "horizon_type": "decade",
                "code": str(row['code']),
                "date": row['date'],
                "horizon_value": int(row['decad_in_month']) if pd.notna(row['decad_in_month']) else decade_value,
                "horizon_in_year": int(row['decad_in_year']),
                "day_of_year": int(row['day_of_year']),
                "count": None,  # Not in sample data
                "mean": float(row['mean']) if pd.notna(row['mean']) else None,
                "std": None,  # Not in sample data
                "min": float(row['min']) if pd.notna(row['min']) else None,
                "max": float(row['max']) if pd.notna(row['max']) else None,
                "q05": float(row['q05']) if pd.notna(row['q05']) else None,
                "q25": float(row['q25']) if pd.notna(row['q25']) else None,
                "q50": None,  # Not in sample data
                "q75": float(row['q75']) if pd.notna(row['q75']) else None,
                "q95": float(row['q95']) if pd.notna(row['q95']) else None,
                "norm": float(row['norm']) if pd.notna(row.get('norm')) else None,
                "previous": float(row['2024']) if pd.notna(row.get('2024')) else None,
                "current": float(row['2025']) if pd.notna(row.get('2025')) else None
            }
            records.append(record)
        return records


class MeteoDataMigrator(DataMigrator):
    """Handles migration of meteorological (temperature and precipitation) data from CSV to API"""

    def __init__(self, api_base_url: str = "http://localhost:8000", batch_size: int = 1000, meteo_type: str = "T"):
        """
        Args:
            api_base_url: Base URL of the API
            batch_size: Number of records per batch
            meteo_type: 'T' for temperature or 'P' for precipitation
        """
        self.meteo_type = meteo_type  # Must be set before calling super().__init__
        super().__init__(api_base_url, batch_size)

    def get_data_type(self) -> str:
        return "meteo"

    def load_and_merge_data(self, reanalysis_path: Path, dashboard_path: Path) -> pd.DataFrame:
        """Load reanalysis data and merge with norm values from dashboard

        Args:
            reanalysis_path: Path to *_T_reanalysis.csv or *_P_reanalysis.csv
            dashboard_path: Path to *_T_reanalysis_dashboard.csv or *_P_reanalysis_dashboard.csv

        Returns:
            Merged DataFrame with reanalysis values and norm values
        """
        # Load reanalysis data (main data source)
        print(f"Loading reanalysis data from: {reanalysis_path.name}")
        df_reanalysis = pd.read_csv(reanalysis_path)
        print(f"Loaded {len(df_reanalysis):,} records from reanalysis file")

        # Try to load dashboard data (for norm values)
        if dashboard_path.exists():
            print(f"Loading norm data from: {dashboard_path.name}")
            df_dashboard = pd.read_csv(dashboard_path)
            print(f"Loaded {len(df_dashboard):,} records from dashboard file")

            # Merge on code and date to get norm values
            norm_column = f"{self.meteo_type}_norm"  # 'T_norm' or 'P_norm'

            # Select only needed columns from dashboard
            df_norm = df_dashboard[['code', 'date', norm_column]].copy()
            df_norm.rename(columns={norm_column: 'norm'}, inplace=True)

            # Merge reanalysis with norm values
            df_merged = pd.merge(
                df_reanalysis,
                df_norm,
                on=['code', 'date'],
                how='outer'  # create rows when missing in either DF
            )
            print(f"Merged data: {len(df_merged):,} records")
            print(f"Records with norm values: {df_merged['norm'].notna().sum():,}")

            return df_merged
        else:
            print(f"Dashboard file not found: {dashboard_path}")
            print("Proceeding without norm values")
            df_reanalysis['norm'] = None
            return df_reanalysis

    def prepare_day_data(self, df: pd.DataFrame) -> List[Dict]:
        """Prepare daily meteorological data for API

        Expected columns in reanalysis file:
        - date: Date
        - T or P: Temperature or Precipitation value
        - code: Station code
        - dayofyear: Day of year
        - norm: Norm value (merged from dashboard file)
        """
        records = []
        value_column = self.meteo_type  # 'T' or 'P'

        for _, row in df.iterrows():
            date_obj = pd.to_datetime(row['date'])
            record = {
                "meteo_type": self.meteo_type,
                "code": str(row['code']),
                "date": row['date'],
                "value": round(float(row[value_column]), 2) if pd.notna(row[value_column]) else None,
                "norm": float(row['norm']) if pd.notna(row.get('norm')) else None,
                "day_of_year": date_obj.dayofyear
            }
            records.append(record)
        return records

    def prepare_pentad_data(self, df: pd.DataFrame) -> List[Dict]:
        """Meteorological data typically doesn't have pentad aggregation"""
        raise NotImplementedError("Meteo data doesn't support pentad horizon")

    def prepare_decade_data(self, df: pd.DataFrame) -> List[Dict]:
        """Meteorological data typically doesn't have decade aggregation"""
        raise NotImplementedError("Meteo data doesn't support decade horizon")

    def migrate_csv(self, csv_path: Path, horizon_type: str) -> MigrationStats:
        """Migrate a single CSV file (override to handle dashboard merge)

        Args:
            csv_path: Path to reanalysis CSV file
            horizon_type: 'day' (only day is supported for meteo)
        """
        print(f"Starting migration for {csv_path.name} ({horizon_type})")
        start_time = time.time()

        # Construct dashboard file path
        # e.g., 00003_T_reanalysis.csv -> 00003_T_reanalysis_dashboard.csv
        dashboard_path = csv_path.parent / csv_path.name.replace('.csv', '_dashboard.csv')

        # Load and merge data from reanalysis and dashboard
        df = self.load_and_merge_data(csv_path, dashboard_path)
        total_records = len(df)
        print(f"Total records to migrate: {total_records:,}")

        # Prepare data based on horizon type
        if horizon_type == "day":
            records = self.prepare_day_data(df)
        elif horizon_type == "pentad":
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

            success, count = self.send_batch(batch)
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
            data_type=self.data_type,
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
        """Migrate only daily meteorological data from reanalysis files

        Looks for files matching pattern: *_T_reanalysis.csv or *_P_reanalysis.csv
        And merges with: *_T_reanalysis_dashboard.csv or *_P_reanalysis_dashboard.csv
        """
        all_stats = []

        filename = f"hindcast_forcing/00003_{self.meteo_type}_reanalysis.csv"
        csv_path = Path(csv_folder) / filename

        if not csv_path.exists():
            print(f"No meteo reanalysis files found for type '{self.meteo_type}' in {csv_folder}")
        else:
            print(f"Processing: {csv_path.name}")
            try:
                stats = self.migrate_csv(csv_path, "day")
                all_stats.append(stats)
            except Exception as e:
                print(f"Migration failed for {csv_path.name}: {str(e)}")

        return all_stats


class SnowDataMigrator(DataMigrator):
    """Handles migration of snow data from CSV to API"""

    def __init__(self, api_base_url: str = "http://localhost:8000", batch_size: int = 1000, snow_type: str = "HS"):
        """
        Args:
            api_base_url: Base URL of the API
            batch_size: Number of records per batch
            snow_type: 'HS', 'ROF', or 'SWE'
        """
        self.snow_type = snow_type  # Must be set before calling super().__init__
        super().__init__(api_base_url, batch_size)

    def get_data_type(self) -> str:
        return "snow"

    def prepare_day_data(self, df: pd.DataFrame) -> List[Dict]:
        """Prepare daily snow data for API"""
        records = []
        for _, row in df.iterrows():
            record = {
                "snow_type": self.snow_type.upper(),
                "code": str(row['code']),
                "date": row['date'],
                "value": float(row['value']) if pd.notna(row['value']) else None,
                "value1": float(row['value1']) if pd.notna(row['value1']) else None,
                "value2": float(row['value2']) if pd.notna(row['value2']) else None,
                "value3": float(row['value3']) if pd.notna(row['value3']) else None,
                "value4": float(row['value4']) if pd.notna(row['value4']) else None,
                "value5": float(row['value5']) if pd.notna(row['value5']) else None,
                "value6": float(row['value6']) if pd.notna(row['value6']) else None,
                "value7": float(row['value7']) if pd.notna(row['value7']) else None,
                "value8": float(row['value8']) if pd.notna(row['value8']) else None,
                "value9": float(row['value9']) if pd.notna(row['value9']) else None,
                "value10": float(row['value10']) if pd.notna(row['value10']) else None,
                "value11": float(row['value11']) if pd.notna(row['value11']) else None,
                "value12": float(row['value12']) if pd.notna(row['value12']) else None,
                "value13": float(row['value13']) if pd.notna(row['value13']) else None,
                "value14": float(row['value14']) if pd.notna(row['value14']) else None,
            }
            records.append(record)
        return records

    def prepare_pentad_data(self, df: pd.DataFrame) -> List[Dict]:
        """Snow data typically doesn't have pentad aggregation"""
        raise NotImplementedError("Snow data doesn't support pentad horizon")

    def prepare_decade_data(self, df: pd.DataFrame) -> List[Dict]:
        """Snow data typically doesn't have decade aggregation"""
        raise NotImplementedError("Snow data doesn't support decade horizon")

    def load_and_merge_data(self, main_path: Path, secondary_path: Path) -> pd.DataFrame:
        """Load value data and merge with value1..value14 from secondary file

        Args:
            main_path: Path to 00003_HS.csv, 00003_ROF.csv, or 00003_SWE.csv
            secondary_path: Path to KGZ500m_HS.csv, KGZ500m_ROF.csv, or KGZ500m_SWE.csv

        Returns:
            Merged DataFrame with main value and secondary values
        """
        # Load main value data
        print(f"Loading main value data from: {main_path.name}")
        df_main = pd.read_csv(main_path)
        df_main.rename(columns={self.snow_type: 'value'}, inplace=True)
        # print("df_main", df_main)
        print(f"Loaded {len(df_main):,} records from main value file")

        # Try to load secondary value data
        if secondary_path.exists():
            print(f"Loading secondary values from: {secondary_path.name}")
            df_secondary = pd.read_csv(secondary_path)
            # print("df_secondary", df_secondary)
            print(f"Loaded {len(df_secondary):,} records from secondary file")

            rename_map = {f"{self.snow_type}_{i}": f"value{i}" for i in range(1, 15)}
            # print("rename_map:", rename_map)

            df_secondary.rename(columns=rename_map, inplace=True)
            # print("df_secondary after rename:", df_secondary)

            # Merge main with secondary values
            df_merged = pd.merge(
                df_main,
                df_secondary,
                on=['code', 'date'],
                how='outer'  # create rows when missing in either DF
            )
            print(f"Merged data: {len(df_merged):,} records")
            # print("df_merged:", df_merged)

            return df_merged
        else:
            print(f"Secondary file not found: {secondary_path}")
            for i in range(1, 15):
                df_main[f'value{i}'] = None
            return df_main

    def migrate_csv(self, main_path: Path, horizon_type: str) -> MigrationStats:
        """Migrate CSV files

        Args:
            main_path: Path to main CSV file
            horizon_type: 'day' (only day is supported for snow)
        """
        print(f"Starting migration for {main_path.name} ({horizon_type})")
        start_time = time.time()

        # Construct secondary file path
        # e.g., 00003_HS.csv -> KGZ500m_HS.csv
        secondary_path = main_path.parent / main_path.name.replace('00003', 'KGZ500m')

        # Load and merge data from reanalysis and dashboard
        df = self.load_and_merge_data(main_path, secondary_path)
        # print("df", df)
        total_records = len(df)
        print(f"Total records to migrate: {total_records:,}")

        # Prepare data based on horizon type
        if horizon_type == "day":
            records = self.prepare_day_data(df)
        elif horizon_type == "pentad":
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

            success, count = self.send_batch(batch)
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
            data_type=self.data_type,
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
        """Migrate only daily snow data from reanalysis files

        Looks for files matching pattern: 00003_HS.csv or 00003_ROF.csv or 00003_SWE.csv
        And merges with: KGZ500m_HS.csv or KGZ500m_ROF.csv or KGZ500m_SWE.csv
        """
        all_stats = []

        filename = f"snow_data/{self.snow_type}/00003_{self.snow_type}.csv"
        csv_path = Path(csv_folder) / filename

        if not csv_path.exists():
            print(f"No snow files found for type '{self.snow_type}' in {csv_folder}")
        else:
            print(f"Processing: {csv_path.name}")
            try:
                stats = self.migrate_csv(csv_path, "day")
                all_stats.append(stats)
            except Exception as e:
                print(f"Migration failed for {csv_path.name}: {str(e)}")

        return all_stats


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Migrate runoff, hydrograph and meteo data from CSV to database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Migrate only runoff data
  python data_migrator.py --type runoff

  # Migrate only hydrograph data
  python data_migrator.py --type hydrograph
  
  # Migrate only meteo data
  python data_migrator.py --type meteo

  # Migrate only snow data
  python data_migrator.py --type snow

  # Migrate all runoff, hydrograph, meteo, and snow data
  python data_migrator.py --type all

  # Use custom batch size
  python data_migrator.py --type all --batch-size 2000
        """
    )

    parser.add_argument(
        '--type',
        choices=['runoff', 'hydrograph', 'meteo', 'snow', 'all'],
        default='all',
        help='Type of data to migrate (default: all)'
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=None,
        help='Batch size for migration (default: from settings)'
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

    if args.type in ['runoff', 'all']:
        migrators_to_run.append(('runoff', RunoffDataMigrator(API_URL, BATCH_SIZE)))

    if args.type in ['hydrograph', 'all']:
        migrators_to_run.append(('hydrograph', HydrographDataMigrator(API_URL, BATCH_SIZE)))

    if args.type in ['meteo', 'all']:
        # Migrate both temperature and precipitation
        migrators_to_run.append(('meteo_temperature', MeteoDataMigrator(API_URL, BATCH_SIZE, meteo_type='T')))
        migrators_to_run.append(('meteo_precipitation', MeteoDataMigrator(API_URL, BATCH_SIZE, meteo_type='P')))

    if args.type in ['snow', 'all']:
        migrators_to_run.append(('snow_hs', SnowDataMigrator(API_URL, BATCH_SIZE, snow_type='HS')))
        migrators_to_run.append(('snow_rof', SnowDataMigrator(API_URL, BATCH_SIZE, snow_type='RoF')))
        migrators_to_run.append(('snow_swe', SnowDataMigrator(API_URL, BATCH_SIZE, snow_type='SWE')))

    # Run migrations
    for data_type, migrator in migrators_to_run:
        print(f"\n{'=' * 70}")
        print(f"Starting {data_type.upper()} migration")
        print(f"{'=' * 70}")

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
    print(f"Total Duration:           {overall_duration:.2f} seconds ({overall_duration/60:.2f} minutes)")
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
