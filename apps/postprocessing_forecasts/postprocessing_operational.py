# postprocessing_operational.py
# Daily operational entry point: reads pre-calculated skill metrics,
# creates ensemble forecasts, and saves results.
# Does NOT recalculate skill metrics (fast path).
#
# Usage:
#   ieasyhydroforecast_env_file_path=/path/to/.env \
#   SAPPHIRE_PREDICTION_MODE=PENTAD python postprocessing_operational.py

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
import tag_library as tl

from src import postprocessing_tools as pt
from src.postprocessing_tools import TimingStats, timer
from src import data_reader
from src import ensemble_calculator
from src import skill_metrics
from src import file_writer

# region Logging
logging.basicConfig(level=logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

if not os.path.exists('logs'):
    os.makedirs('logs')

file_handler = TimedRotatingFileHandler(
    'logs/log_operational', when='midnight', interval=1, backupCount=30
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


def postprocessing_operational():
    global timing_stats

    logger.info(
        "\n\n====== Post-processing forecasts (OPERATIONAL) ================"
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
        valid_modes = ['PENTAD', 'DECAD', 'BOTH', 'MONTHLY', 'ALL']
        if prediction_mode not in valid_modes:
            logger.error(
                f"Invalid SAPPHIRE_PREDICTION_MODE: {prediction_mode}. "
                f"Expected one of {valid_modes}."
            )
            sys.exit(1)
        logger.info(
            f"Running operational postprocessing for mode: {prediction_mode}"
        )

        if prediction_mode in ['PENTAD', 'BOTH', 'ALL']:
            with timer(timing_stats, 'reading pentadal data'):
                logger.info(
                    "\n\n------ Reading pentadal observed and modelled "
                    "data -------"
                )
                observed, modelled = (
                    sl.read_observed_and_modelled_data_pentade()
                )

            with timer(timing_stats, 'reading pentadal skill metrics'):
                logger.info(
                    "\n\n------ Reading pre-calculated pentadal skill "
                    "metrics ----"
                )
                skill_stats_pentad = data_reader.read_skill_metrics('pentad')

            if skill_stats_pentad.empty:
                logger.warning(
                    "No pentadal skill metrics available. "
                    "Skipping ensemble creation. "
                    "Run recalculate_skill_metrics.py first."
                )
            else:
                with timer(timing_stats, 'creating pentadal ensembles'):
                    logger.info(
                        "\n\n------ Creating pentadal ensemble forecasts ----"
                    )
                    modelled, skill_stats_pentad = (
                        ensemble_calculator.create_ensemble_forecasts(
                            forecasts=modelled,
                            skill_stats=skill_stats_pentad,
                            observed=observed,
                            period_col='pentad_in_year',
                            period_in_month_col='pentad_in_month',
                            get_period_in_month_func=tl.get_pentad,
                            calculate_all_metrics_func=(
                                skill_metrics.calculate_all_skill_metrics
                            ),
                        )
                    )

            with timer(timing_stats, 'saving pentad results'):
                logger.info(
                    "\n\n------ Saving pentad results --------------------"
                )
                ret = file_writer.save_forecast_data_pentad(modelled)
                if ret is None:
                    logger.info(
                        "Pentadal forecast results saved successfully."
                    )
                else:
                    logger.error(
                        f"Error saving pentadal forecast results: {ret}"
                    )
                    errors.append(f"Pentad forecast save failed: {ret}")

            pt.log_most_recent_forecasts_pentad(modelled)

        if prediction_mode in ['DECAD', 'BOTH', 'ALL']:
            with timer(timing_stats, 'reading decadal data'):
                logger.info(
                    "\n\n------ Reading decadal observed and modelled "
                    "data -------"
                )
                observed_decade, modelled_decade = (
                    sl.read_observed_and_modelled_data_decade()
                )

            with timer(timing_stats, 'reading decadal skill metrics'):
                logger.info(
                    "\n\n------ Reading pre-calculated decadal skill "
                    "metrics ----"
                )
                skill_metrics_decade = data_reader.read_skill_metrics('decad')

            if skill_metrics_decade.empty:
                logger.warning(
                    "No decadal skill metrics available. "
                    "Skipping ensemble creation. "
                    "Run recalculate_skill_metrics.py first."
                )
            else:
                with timer(timing_stats, 'creating decadal ensembles'):
                    logger.info(
                        "\n\n------ Creating decadal ensemble forecasts -----"
                    )
                    modelled_decade, skill_metrics_decade = (
                        ensemble_calculator.create_ensemble_forecasts(
                            forecasts=modelled_decade,
                            skill_stats=skill_metrics_decade,
                            observed=observed_decade,
                            period_col='decad_in_year',
                            period_in_month_col='decad_in_month',
                            get_period_in_month_func=tl.get_decad_in_month,
                            calculate_all_metrics_func=(
                                skill_metrics.calculate_all_skill_metrics
                            ),
                        )
                    )

            with timer(timing_stats, 'saving decade results'):
                logger.info(
                    "\n\n------ Saving decade results --------------------"
                )
                ret = file_writer.save_forecast_data_decade(modelled_decade)
                if ret is None:
                    logger.info(
                        "Decadal forecast results saved successfully."
                    )
                else:
                    logger.error(
                        f"Error saving decadal forecast results: {ret}"
                    )
                    errors.append(f"Decade forecast save failed: {ret}")

            pt.log_most_recent_forecasts_decade(modelled_decade)

        if prediction_mode in ['MONTHLY', 'ALL']:
            with timer(timing_stats, 'reading monthly skill metrics'):
                logger.info(
                    "\n\n------ Reading pre-calculated monthly skill "
                    "metrics -----"
                )
                skill_metrics_monthly = (
                    data_reader.read_monthly_skill_metrics()
                )

            if skill_metrics_monthly.empty:
                logger.warning(
                    "No monthly skill metrics available. "
                    "Run recalculate_skill_metrics.py first."
                )
            else:
                logger.info(
                    "Monthly skill metrics available: %d rows",
                    len(skill_metrics_monthly),
                )

    # Print timing summary
    summary, total = timing_stats.summary()
    logger.info("\n\n")
    logger.info("Timing summary for postprocessing_operational:")
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
    postprocessing_operational()
