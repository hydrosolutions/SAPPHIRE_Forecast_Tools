"""
Data Migration Script for Forecast and Skill Metrics Data
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
        return records

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
        return records


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

  # Migrate all forecast and skill metrics data
  python data_migrator.py --type all

  # Use custom batch size
  python data_migrator.py --type all --batch-size 2000
        """
    )

    parser.add_argument(
        '--type',
        choices=['combinedforecast', 'lrforecast', 'forecast', 'skillmetric', 'all'],
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
