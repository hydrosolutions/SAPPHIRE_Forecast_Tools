# postprocessing_operational.py
# Daily operational entry point: reads pre-calculated skill metrics,
# creates ensemble forecasts, and saves results.
# Does NOT recalculate skill metrics (fast path).
#
# Usage:
#   ieasyhydroforecast_env_file_path=/path/to/.env \
#   SAPPHIRE_PREDICTION_MODE=PENTAD python postprocessing_operational.py

import calendar
import datetime as dt
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

# Local libraries
script_dir = os.path.dirname(os.path.abspath(__file__))
forecast_dir = os.path.join(script_dir, "..", "iEasyHydroForecast")
sys.path.append(forecast_dir)

import setup_library as sl
import tag_library as tl
from src import data_reader, ensemble_calculator, file_writer
from src import postprocessing_tools as pt
from src.postprocessing_tools import TimingStats, timer

# region Logging
logging.basicConfig(level=logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

if not os.path.exists("logs"):
    os.makedirs("logs")

file_handler = TimedRotatingFileHandler(
    "logs/log_operational", when="midnight", interval=1, backupCount=30
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


def is_pentad_boundary(d: dt.date) -> bool:
    """Return True if *d* is a pentad boundary (5/10/15/20/25/last)."""
    last_day = calendar.monthrange(d.year, d.month)[1]
    return d.day in (5, 10, 15, 20, 25, last_day)


def is_decad_boundary(d: dt.date) -> bool:
    """Return True if *d* is a decad boundary (10/20/last)."""
    last_day = calendar.monthrange(d.year, d.month)[1]
    return d.day in (10, 20, last_day)


def postprocessing_operational():
    global timing_stats

    logger.info("\n\n====== Post-processing forecasts (OPERATIONAL) ================")
    logger.debug(f"Script started at {dt.datetime.now()}.")

    errors = []

    with timer(timing_stats, "total execution"):
        with timer(timing_stats, "setup"):
            logger.info("\n\n------ Setting up --------------------------------")
            sl.load_environment()

        prediction_mode = os.getenv("SAPPHIRE_PREDICTION_MODE", "") or "BOTH"
        valid_modes = ["PENTAD", "DECAD", "BOTH", "MONTHLY", "ALL"]
        if prediction_mode not in valid_modes:
            logger.error(
                f"Invalid SAPPHIRE_PREDICTION_MODE: {prediction_mode}. "
                f"Expected one of {valid_modes}."
            )
            sys.exit(1)
        logger.info(f"Running operational postprocessing for mode: {prediction_mode}")

        today = dt.date.today()

        if prediction_mode in ["PENTAD", "BOTH", "ALL"]:
            if not is_pentad_boundary(today):
                logger.info(
                    "Skipping pentad postprocessing: %s is not a pentad "
                    "boundary day (boundaries: 5/10/15/20/25/last)",
                    today,
                )
            else:
                with timer(timing_stats, "reading pentadal data"):
                    logger.info("\n\n------ Reading pentadal observed and modelled data -------")
                    _, modelled = data_reader.read_observed_and_modelled_data("pentad")
                    modelled = sl.calculate_virtual_stations_data(modelled)
                    modelled = sl.calculate_neural_ensemble_forecast(modelled)

                with timer(timing_stats, "reading pentadal skill metrics"):
                    logger.info("\n\n------ Reading pre-calculated pentadal skill metrics ----")
                    skill_stats_pentad = data_reader.read_skill_metrics("pentad")

                if skill_stats_pentad.empty:
                    logger.warning(
                        "No pentadal skill metrics available. "
                        "Skipping ensemble creation. "
                        "Run recalculate_skill_metrics.py first."
                    )
                else:
                    with timer(timing_stats, "creating pentadal ensembles"):
                        logger.info("\n\n------ Creating pentadal ensemble forecasts ----")
                        modelled, skill_stats_pentad = (
                            ensemble_calculator.create_ensemble_forecasts(
                                forecasts=modelled,
                                skill_stats=skill_stats_pentad,
                                period_col="pentad_in_year",
                                period_in_month_col="pentad_in_month",
                                get_period_in_month_func=tl.get_pentad,
                            )
                        )

                with timer(timing_stats, "saving pentad results"):
                    logger.info("\n\n------ Saving pentad results ----------------")
                    ret = file_writer.save_forecast_data_pentad(modelled)
                    if ret is None:
                        logger.info("Pentadal forecast results saved successfully.")
                    else:
                        logger.error(f"Error saving pentadal forecast results: {ret}")
                        errors.append(f"Pentad forecast save failed: {ret}")

                pt.log_most_recent_forecasts_pentad(modelled)

        if prediction_mode in ["DECAD", "BOTH", "ALL"]:
            if not is_decad_boundary(today):
                logger.info(
                    "Skipping decad postprocessing: %s is not a decad "
                    "boundary day (boundaries: 10/20/last)",
                    today,
                )
            else:
                with timer(timing_stats, "reading decadal data"):
                    logger.info("\n\n------ Reading decadal observed and modelled data -------")
                    _, modelled_decade = data_reader.read_observed_and_modelled_data("decad")
                    modelled_decade = sl.calculate_virtual_stations_data(modelled_decade)
                    modelled_decade = sl.calculate_neural_ensemble_forecast_decade(modelled_decade)

                with timer(timing_stats, "reading decadal skill metrics"):
                    logger.info("\n\n------ Reading pre-calculated decadal skill metrics ----")
                    skill_metrics_decade = data_reader.read_skill_metrics("decad")

                if skill_metrics_decade.empty:
                    logger.warning(
                        "No decadal skill metrics available. "
                        "Skipping ensemble creation. "
                        "Run recalculate_skill_metrics.py first."
                    )
                else:
                    with timer(timing_stats, "creating decadal ensembles"):
                        logger.info("\n\n------ Creating decadal ensemble forecasts -----")
                        modelled_decade, skill_metrics_decade = (
                            ensemble_calculator.create_ensemble_forecasts(
                                forecasts=modelled_decade,
                                skill_stats=skill_metrics_decade,
                                period_col="decad_in_year",
                                period_in_month_col="decad_in_month",
                                get_period_in_month_func=(tl.get_decad_in_month),
                            )
                        )

                with timer(timing_stats, "saving decade results"):
                    logger.info("\n\n------ Saving decade results ----------------")
                    ret = file_writer.save_forecast_data_decade(modelled_decade)
                    if ret is None:
                        logger.info("Decadal forecast results saved successfully.")
                    else:
                        logger.error(f"Error saving decadal forecast results: {ret}")
                        errors.append(f"Decade forecast save failed: {ret}")

                pt.log_most_recent_forecasts_decade(modelled_decade)

        if prediction_mode in ["MONTHLY", "ALL"]:
            logger.info(
                "Monthly postprocessing is handled by "
                "postprocessing_operational_long_term.py. "
                "Skipping monthly in operational mode."
            )

    # Print timing summary
    summary, total = timing_stats.summary()
    logger.info("\n\n")
    logger.info("Timing summary for postprocessing_operational:")
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
    postprocessing_operational()
