# recalculate_skill_metrics.py
# Yearly (or on-demand) entry point: reads ALL historical data,
# recalculates ALL skill metrics, and saves everything.
# This is the slow path — identical to the legacy postprocessing_forecasts.py
# behavior but without the deprecation warning overhead.
#
# Usage:
#   ieasyhydroforecast_env_file_path=/path/to/.env \
#   SAPPHIRE_PREDICTION_MODE=BOTH python recalculate_skill_metrics.py

import os
import sys
import datetime as dt
import logging
from logging.handlers import TimedRotatingFileHandler

# Local libraries
script_dir = os.path.dirname(os.path.abspath(__file__))
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')
sys.path.append(forecast_dir)

import setup_library as sl
import forecast_library as fl

from src import postprocessing_tools as pt
from src.postprocessing_tools import TimingStats, timer

# region Logging
logging.basicConfig(level=logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

if not os.path.exists('logs'):
    os.makedirs('logs')

file_handler = TimedRotatingFileHandler(
    'logs/log_recalc', when='midnight', interval=1, backupCount=30
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


def recalculate_skill_metrics():
    global timing_stats

    logger.info(
        "\n\n====== Recalculating ALL skill metrics ========================="
    )
    logger.debug(f"Script started at {dt.datetime.now()}.")

    errors = []

    with timer(timing_stats, 'total execution'):

        with timer(timing_stats, 'setup'):
            logger.info(
                "\n\n------ Setting up --------------------------------"
            )
            sl.load_environment()

        prediction_mode = os.getenv('SAPPHIRE_PREDICTION_MODE', 'BOTH')
        if prediction_mode not in ['PENTAD', 'DECAD', 'BOTH']:
            logger.error(
                f"Invalid SAPPHIRE_PREDICTION_MODE: {prediction_mode}. "
                f"Expected 'PENTAD', 'DECAD', or 'BOTH'."
            )
            sys.exit(1)
        logger.info(
            f"Running skill metrics recalculation for mode: "
            f"{prediction_mode}"
        )

        if prediction_mode in ['PENTAD', 'BOTH']:
            with timer(timing_stats, 'reading pentadal data'):
                logger.info(
                    "\n\n------ Reading pentadal observed and modelled "
                    "data -------"
                )
                observed, modelled = (
                    sl.read_observed_and_modelled_data_pentade()
                )

            with timer(timing_stats, 'calculating skill metrics pentads'):
                logger.info(
                    "\n\n------ Calculating skill metrics pentads --------"
                )
                original_timing_stats = timing_stats
                skill_metrics, modelled, returned_timing_stats = (
                    fl.calculate_skill_metrics_pentad(
                        observed, modelled, timing_stats
                    )
                )
                if returned_timing_stats is not None:
                    timing_stats = returned_timing_stats
                else:
                    timing_stats = original_timing_stats

            with timer(timing_stats, 'saving pentad results'):
                logger.info(
                    "\n\n------ Saving pentad results --------------------"
                )
                ret = fl.save_forecast_data_pentad(modelled)
                if ret is None:
                    logger.info(
                        "Pentadal forecast results saved successfully."
                    )
                else:
                    logger.error(
                        f"Error saving pentadal forecast results: {ret}"
                    )
                    errors.append(f"Pentad forecast save failed: {ret}")

                ret = fl.save_pentadal_skill_metrics(skill_metrics)
                if ret is None:
                    logger.info(
                        "Pentadal skill metrics saved successfully."
                    )
                else:
                    logger.error(
                        f"Error saving pentadal skill metrics: {ret}"
                    )
                    errors.append(
                        f"Pentad skill metrics save failed: {ret}"
                    )

            pt.log_most_recent_forecasts_pentad(modelled)

        if prediction_mode in ['DECAD', 'BOTH']:
            with timer(timing_stats, 'reading decadal data'):
                logger.info(
                    "\n\n------ Reading decadal observed and modelled "
                    "data -------"
                )
                observed_decade, modelled_decade = (
                    sl.read_observed_and_modelled_data_decade()
                )

            with timer(timing_stats, 'calculating skill metrics decads'):
                logger.info(
                    "\n\n------ Calculating skill metrics decads ---------"
                )
                original_timing_stats = timing_stats
                skill_metrics_decade, modelled_decade, returned_timing_stats = (
                    fl.calculate_skill_metrics_decade(
                        observed_decade, modelled_decade, timing_stats
                    )
                )
                if returned_timing_stats is not None:
                    timing_stats = returned_timing_stats
                else:
                    timing_stats = original_timing_stats

            with timer(timing_stats, 'saving decade results'):
                logger.info(
                    "\n\n------ Saving decade results --------------------"
                )
                ret = fl.save_forecast_data_decade(modelled_decade)
                if ret is None:
                    logger.info(
                        "Decadal forecast results saved successfully."
                    )
                else:
                    logger.error(
                        f"Error saving decadal forecast results: {ret}"
                    )
                    errors.append(f"Decade forecast save failed: {ret}")

                ret = fl.save_decadal_skill_metrics(skill_metrics_decade)
                if ret is None:
                    logger.info(
                        "Decadal skill metrics saved successfully."
                    )
                else:
                    logger.error(
                        f"Error saving decadal skill metrics: {ret}"
                    )
                    errors.append(
                        f"Decade skill metrics save failed: {ret}"
                    )

            pt.log_most_recent_forecasts_decade(modelled_decade)

    # Print timing summary
    summary, total = timing_stats.summary()
    logger.info("\n\n")
    logger.info("Timing summary for recalculate_skill_metrics:")
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
    recalculate_skill_metrics()
