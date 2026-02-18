# postprocessing_operational_long_term.py
# Fast path for monthly (long-term) ensemble forecasts.
# Reads pre-calculated skill metrics + latest month's forecasts,
# creates EM / Skilled Mean / Naive Mean ensembles, writes to CSV + API.
#
# Usage:
#   ieasyhydroforecast_env_file_path=/path/to/.env \
#   python postprocessing_operational_long_term.py

import os
import sys
import json
import datetime as dt
import logging
from logging.handlers import TimedRotatingFileHandler

import pandas as pd

# Local libraries
script_dir = os.path.dirname(os.path.abspath(__file__))
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')
sys.path.append(forecast_dir)

import setup_library as sl

from src import postprocessing_tools as pt
from src.postprocessing_tools import TimingStats, timer
from src import data_reader
from src import ensemble_calculator
from src import file_writer

# region Logging
logging.basicConfig(level=logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

if not os.path.exists('logs'):
    os.makedirs('logs')

file_handler = TimedRotatingFileHandler(
    'logs/log_operational_long_term',
    when='midnight', interval=1, backupCount=30,
)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.handlers = []
logger.addHandler(file_handler)
logger.addHandler(console_handler)
# endregion

timing_stats = TimingStats()


def _read_station_codes():
    """Read station codes from the station selection config file."""
    config_path = os.path.join(
        os.getenv("ieasyforecast_configuration_path", ""),
        os.getenv("ieasyforecast_config_file_station_selection", ""),
    )
    with open(config_path, "r") as f:
        config = json.load(f)
    codes = [str(c) for c in config.get("stationsID", [])]
    logger.info("Read %d station codes", len(codes))
    return codes


def postprocessing_operational_long_term():
    global timing_stats

    logger.info(
        "\n\n====== Post-processing forecasts "
        "(OPERATIONAL LONG-TERM / MONTHLY) ======"
    )
    logger.debug(f"Script started at {dt.datetime.now()}.")

    errors = []

    with timer(timing_stats, 'total execution'):

        with timer(timing_stats, 'setup'):
            logger.info(
                "\n\n------ Setting up --------------------------------"
            )
            sl.load_environment()
            codes = _read_station_codes()
            forecast_date = dt.date.today()
            logger.info("Forecast date: %s", forecast_date)

        # 1. Read pre-calculated monthly skill metrics
        with timer(timing_stats, 'reading monthly skill metrics'):
            logger.info(
                "\n\n------ Reading pre-calculated monthly skill "
                "metrics -----"
            )
            skill_stats = data_reader.read_skill_metrics('month')

        if skill_stats.empty:
            logger.warning(
                "No monthly skill metrics available. "
                "Run recalculate_skill_metrics.py or maintenance "
                "first. Exiting."
            )
            sys.exit(0)

        # 2. Read latest month's forecasts from API
        with timer(timing_stats, 'reading latest monthly forecasts'):
            logger.info(
                "\n\n------ Reading latest monthly forecasts ----------"
            )
            forecasts = data_reader.read_latest_monthly_forecasts(
                codes, forecast_date=forecast_date,
            )

        if forecasts.empty:
            logger.warning(
                "No recent monthly forecasts available. Exiting."
            )
            sys.exit(0)

        # 3. Create ensemble forecasts
        with timer(timing_stats, 'creating monthly ensembles'):
            logger.info(
                "\n\n------ Creating monthly ensemble forecasts -------"
            )
            joint = ensemble_calculator.create_monthly_ensemble_forecasts(
                forecasts, skill_stats,
            )

        # 3.5. Merge into existing combined forecasts
        with timer(timing_stats, 'merging with existing data'):
            existing = data_reader.read_monthly_combined_forecasts()
            if not existing.empty:
                joint = pd.concat(
                    [existing, joint], ignore_index=True,
                )
                dedup_cols = [
                    'year', 'month', 'code', 'model_short',
                ]
                available_dedup = [
                    c for c in dedup_cols if c in joint.columns
                ]
                joint = joint.drop_duplicates(
                    subset=available_dedup, keep='last',
                )

        # 4. Save results
        with timer(timing_stats, 'saving monthly results'):
            logger.info(
                "\n\n------ Saving monthly results --------------------"
            )
            ret = file_writer.save_monthly_forecast_data(joint)
            if ret is None:
                logger.info(
                    "Monthly forecast results saved successfully."
                )
            else:
                logger.error(
                    f"Error saving monthly forecast results: {ret}"
                )
                errors.append(f"Monthly forecast save failed: {ret}")

        pt.log_most_recent_forecasts_monthly(joint)

    # Print timing summary
    summary, total = timing_stats.summary()
    logger.info("\n\n")
    logger.info("Timing summary for postprocessing_operational_long_term:")
    logger.info("Total execution time: {:.2f} seconds".format(total))
    logger.info("Breakdown by section:")
    for entry in summary:
        logger.info(f"{entry['section']}:")
        logger.info(
            f"  Total time: {entry['total_time']:.2f} seconds "
            f"({entry['percentage']:.1f}%)"
        )
        logger.info(
            f"  Average time per call: {entry['avg_time']:.2f} seconds"
        )
        logger.info(f"  Number of calls: {entry['calls']}")

    if errors:
        logger.error(f"Script finished with {len(errors)} error(s):")
        for error in errors:
            logger.error(f"  - {error}")
        sys.exit(1)
    else:
        logger.info(
            f"Script finished successfully at {dt.datetime.now()}."
        )
        sys.exit(0)


if __name__ == "__main__":
    postprocessing_operational_long_term()
