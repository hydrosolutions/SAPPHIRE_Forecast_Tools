"""
Data Migration Script for Runoff Data
Migrates CSV files (day, pentad, decade horizons) to PostgreSQL database
with performance statistics and batch processing
"""

import pandas as pd
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
import time
from dataclasses import dataclass
from app.logger import logger
from app.config import settings


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
            f"\n{'=' * 60}\n"
            f"Migration Statistics - {self.horizon_type.upper()}\n"
            f"{'=' * 60}\n"
            f"Total Records:        {self.total_records:,}\n"
            f"Successful:           {self.successful:,}\n"
            f"Failed:               {self.failed:,}\n"
            f"Duration:             {self.duration_seconds:.2f} seconds\n"
            f"Throughput:           {self.records_per_second:.2f} records/sec\n"
            f"Batch Count:          {self.batch_count}\n"
            f"{'=' * 60}\n"
        )


class RunoffDataMigrator:
    """Handles migration of runoff data from CSV to API"""

    def __init__(self, api_base_url: str = "http://localhost:8000", batch_size: int = 1000):
        self.api_base_url = api_base_url
        self.batch_size = batch_size
        self.session = requests.Session()

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

    def send_batch(self, batch: List[Dict]) -> Tuple[bool, int]:
        """Send a batch of records to the API"""
        try:
            payload = {"data": batch}
            response = self.session.post(
                f"{self.api_base_url}/runoff/",
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            return True, len(batch)
        except requests.exceptions.RequestException as e:
            logger.error(f"Batch upload failed: {str(e)}")
            return False, 0

    def migrate_csv(self, csv_path: Path, horizon_type: str) -> MigrationStats:
        """Migrate a single CSV file"""
        logger.info(f"Starting migration for {csv_path.name} ({horizon_type})")
        start_time = time.time()

        # Read CSV
        df = pd.read_csv(csv_path)
        total_records = len(df)
        logger.info(f"Loaded {total_records:,} records from CSV")

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
                logger.info(f"Batch {batch_count}: {count} records uploaded successfully")
            else:
                failed += len(batch)
                logger.error(f"Batch {batch_count}: Failed to upload {len(batch)} records")

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

        logger.info(str(stats))
        return stats


def main():
    """Main migration execution"""
    # Configuration
    API_URL = settings.api_base_url
    BATCH_SIZE = settings.batch_size  # Adjust based on your API performance
    CSV_FOLDER = settings.csv_folder
    print("API_URL:", API_URL)
    print("BATCH_SIZE:", BATCH_SIZE)
    print("CSV_FOLDER:", CSV_FOLDER)

    # CSV file paths (update these to your actual paths)
    csv_files = {
        "day": Path(CSV_FOLDER + "runoff_day.csv"),
        "pentad": Path(CSV_FOLDER + "runoff_pentad.csv"),
        "decade": Path(CSV_FOLDER + "runoff_decad.csv")
    }

    # Initialize migrator
    migrator = RunoffDataMigrator(api_base_url=API_URL, batch_size=BATCH_SIZE)

    # Track overall statistics
    all_stats = []
    overall_start = time.time()

    # Migrate each file
    for horizon_type, csv_path in csv_files.items():
        if not csv_path.exists():
            logger.warning(f"File not found: {csv_path}")
            continue

        try:
            stats = migrator.migrate_csv(csv_path, horizon_type)
            all_stats.append(stats)
        except Exception as e:
            logger.error(f"Migration failed for {horizon_type}: {str(e)}")

    # Print summary
    overall_duration = time.time() - overall_start
    total_records = sum(s.total_records for s in all_stats)
    total_successful = sum(s.successful for s in all_stats)
    total_failed = sum(s.failed for s in all_stats)

    print("\n" + "=" * 60)
    print("OVERALL MIGRATION SUMMARY")
    print("=" * 60)
    print(f"Total Records Processed:  {total_records:,}")
    print(f"Total Successful:         {total_successful:,}")
    print(f"Total Failed:             {total_failed:,}")
    if total_records > 0:
        print(f"Success Rate:             {(total_successful / total_records * 100):.2f}%")
    else:
        print("Success Rate:             N/A")
    print(f"Total Duration:           {overall_duration:.2f} seconds")
    if overall_duration > 0:
        print(f"Overall Throughput:       {total_successful / overall_duration:.2f} records/sec")
    else:
        print("Overall Throughput:       N/A")
    print("=" * 60)

    # Individual statistics
    for stats in all_stats:
        print(stats)


if __name__ == "__main__":
    main()
