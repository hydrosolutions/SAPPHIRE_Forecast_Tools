# postprocessing_operational_long_term.py
# Fast path for monthly (long-term) ensemble forecasts.
# Reads pre-calculated skill metrics + latest month's forecasts,
# creates EM / Skilled Mean / Naive Mean ensembles, writes to CSV + API.
#
# Usage:
#   ieasyhydroforecast_env_file_path=/path/to/.env \
#   python postprocessing_operational_long_term.py

import datetime as dt
import json
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

import pandas as pd

# Local libraries
script_dir = os.path.dirname(os.path.abspath(__file__))
forecast_dir = os.path.join(script_dir, "..", "iEasyHydroForecast")
sys.path.append(forecast_dir)

import setup_library as sl
from src import data_reader, ensemble_calculator, file_writer
from src import postprocessing_tools as pt
from src.postprocessing_tools import TimingStats, timer

# region Logging
logging.basicConfig(level=logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

if not os.path.exists("logs"):
    os.makedirs("logs")

file_handler = TimedRotatingFileHandler(
    "logs/log_operational_long_term",
    when="midnight",
    interval=1,
    backupCount=30,
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
    with open(config_path) as f:
        config = json.load(f)
    codes = [str(c) for c in config.get("stationsID", [])]
    logger.info("Read %d station codes", len(codes))
    return codes


def postprocessing_operational_long_term():
    global timing_stats

    logger.info("\n\n====== Post-processing forecasts (OPERATIONAL LONG-TERM / MONTHLY) ======")
    logger.debug(f"Script started at {dt.datetime.now()}.")

    errors = []

    with timer(timing_stats, "total execution"):
        with timer(timing_stats, "setup"):
            logger.info("\n\n------ Setting up --------------------------------")
            sl.load_environment()
            codes = _read_station_codes()
            forecast_date = dt.date.today()
            logger.info("Forecast date: %s", forecast_date)

        # 1. Read pre-calculated monthly skill metrics
        with timer(timing_stats, "reading monthly skill metrics"):
            logger.info("\n\n------ Reading pre-calculated monthly skill metrics -----")
            skill_stats = data_reader.read_skill_metrics("month", codes=codes)

        if skill_stats.empty:
            logger.warning(
                "No monthly skill metrics available. "
                "Run recalculate_skill_metrics.py or maintenance "
                "first. Exiting."
            )
            sys.exit(0)

        # 2. Read latest month's forecasts from API
        with timer(timing_stats, "reading latest monthly forecasts"):
            logger.info("\n\n------ Reading latest monthly forecasts ----------")
            forecasts = data_reader.read_latest_monthly_forecasts(
                codes,
                forecast_date=forecast_date,
            )

        if forecasts.empty:
            logger.warning("No recent monthly forecasts available. Exiting.")
            sys.exit(0)

        # 3. Create ensemble forecasts
        with timer(timing_stats, "creating monthly ensembles"):
            logger.info("\n\n------ Creating monthly ensemble forecasts -------")
            joint = ensemble_calculator.create_monthly_ensemble_forecasts(
                forecasts,
                skill_stats,
            )

        # 3.5. Merge into existing combined forecasts
        with timer(timing_stats, "merging with existing data"):
            existing = data_reader.read_monthly_combined_forecasts(codes=codes)
            if not existing.empty:
                joint = pd.concat(
                    [existing, joint],
                    ignore_index=True,
                )
                dedup_cols = [
                    "year",
                    "month",
                    "code",
                    "model_short",
                ]
                available_dedup = [c for c in dedup_cols if c in joint.columns]
                joint = joint.drop_duplicates(
                    subset=available_dedup,
                    keep="last",
                )

        # 4. Save results
        with timer(timing_stats, "saving monthly results"):
            logger.info("\n\n------ Saving monthly results --------------------")
            ret = file_writer.save_monthly_forecast_data(joint)
            if ret is None:
                logger.info("Monthly forecast results saved successfully.")
            else:
                logger.error(f"Error saving monthly forecast results: {ret}")
                errors.append(f"Monthly forecast save failed: {ret}")

        pt.log_most_recent_forecasts_monthly(joint)

        # ----- QUARTERLY ENSEMBLES -----
        with timer(timing_stats, "quarterly processing"):
            logger.info("\n\n------ Quarterly ensemble processing -------------")
            quarterly_skill = data_reader.read_skill_metrics("quarter", codes=codes)
            if not quarterly_skill.empty:
                quarterly_fc = data_reader.read_latest_quarterly_forecasts(
                    codes,
                    forecast_date=forecast_date,
                )
                if not quarterly_fc.empty:
                    quarterly_joint = ensemble_calculator.create_quarterly_ensemble_forecasts(
                        quarterly_fc,
                        quarterly_skill,
                    )
                    existing_q = data_reader.read_quarterly_combined_forecasts(codes=codes)
                    if not existing_q.empty:
                        quarterly_joint = pd.concat(
                            [existing_q, quarterly_joint],
                            ignore_index=True,
                        )
                        quarterly_joint = quarterly_joint.drop_duplicates(
                            subset=[
                                "year",
                                "quarter_in_year",
                                "code",
                                "model_short",
                            ],
                            keep="last",
                        )
                    file_writer.save_quarterly_forecast_data(quarterly_joint)
                    logger.info("Quarterly ensembles saved.")
                else:
                    logger.info("No recent quarterly forecasts. Skipping quarterly ensembles.")
            else:
                logger.info("No quarterly skill metrics. Skipping quarterly ensembles.")

        # ----- SEASONAL ENSEMBLES -----
        with timer(timing_stats, "seasonal processing"):
            logger.info("\n\n------ Seasonal ensemble processing --------------")
            seasonal_skill = data_reader.read_skill_metrics("season", codes=codes)
            if not seasonal_skill.empty:
                seasonal_fc = data_reader.read_latest_seasonal_forecasts(
                    codes,
                    forecast_date=forecast_date,
                )
                if not seasonal_fc.empty:
                    seasonal_joint = ensemble_calculator.create_seasonal_ensemble_forecasts(
                        seasonal_fc,
                        seasonal_skill,
                    )
                    existing_s = data_reader.read_seasonal_combined_forecasts(codes=codes)
                    if not existing_s.empty:
                        seasonal_joint = pd.concat(
                            [existing_s, seasonal_joint],
                            ignore_index=True,
                        )
                        dedup = ["season_year", "code", "model_short"]
                        available = [c for c in dedup if c in seasonal_joint.columns]
                        seasonal_joint = seasonal_joint.drop_duplicates(
                            subset=available,
                            keep="last",
                        )
                    file_writer.save_seasonal_forecast_data(seasonal_joint)
                    logger.info("Seasonal ensembles saved.")
                else:
                    logger.info("No recent seasonal forecasts. Skipping seasonal ensembles.")
            else:
                logger.info("No seasonal skill metrics. Skipping seasonal ensembles.")

    # Print timing summary
    summary, total = timing_stats.summary()
    logger.info("\n\n")
    logger.info("Timing summary for postprocessing_operational_long_term:")
    logger.info(f"Total execution time: {total:.2f} seconds")
    logger.info("Breakdown by section:")
    for entry in summary:
        logger.info(f"{entry['section']}:")
        logger.info(f"  Total time: {entry['total_time']:.2f} seconds ({entry['percentage']:.1f}%)")
        logger.info(f"  Average time per call: {entry['avg_time']:.2f} seconds")
        logger.info(f"  Number of calls: {entry['calls']}")

    if errors:
        logger.error(f"Script finished with {len(errors)} error(s):")
        for error in errors:
            logger.error(f"  - {error}")
        sys.exit(1)
    else:
        logger.info(f"Script finished successfully at {dt.datetime.now()}.")
        sys.exit(0)


if __name__ == "__main__":
    postprocessing_operational_long_term()
