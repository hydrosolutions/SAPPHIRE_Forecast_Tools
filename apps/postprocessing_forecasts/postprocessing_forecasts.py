# postprocessing_forecasts.py
# Reads in forecast results, calculates forecast skill metrics, and saves the results.
# Usage:
#   ieasyhydroforecast_env_file_path=/path/to/.env SAPPHIRE_PREDICTION_MODE=PENTAD python postprocessing_forecasts.py
# Accepts SAPPHIRE PREDICTION MODE to be PENTAD, DECADE, or BOTH.


# region Libraries
import datetime as dt
import logging
import os
import sys
import warnings
from logging.handlers import TimedRotatingFileHandler

# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, "..", "iEasyHydroForecast")

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the setup_library module from the iEasyHydroForecast package
import setup_library as sl
import tag_library as tl
from src import file_writer, skill_metrics
from src.horizon_config import ShortTermHorizonConfig
from src.postprocessing_tools import TimingStats, timer

# endregion

# region Logging
# Configure the logging level and formatter
logging.basicConfig(level=logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# Create the logs directory if it doesn't exist
if not os.path.exists("logs"):
    os.makedirs("logs")

# Create a file handler to write logs to a file
file_handler = TimedRotatingFileHandler("logs/log", when="midnight", interval=1, backupCount=30)
file_handler.setFormatter(formatter)

# Create a stream handler to print logs to the console
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Get the root logger and add the handlers to it
logger = logging.getLogger()
logger.handlers = []
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# endregion

# Initialize the timing stats object
timing_stats = TimingStats()

PENTAD = ShortTermHorizonConfig(
    name="pentad",
    period_col="pentad_in_year",
    period_in_month_col="pentad_in_month",
    get_period_func=tl.get_pentad,
    combined_csv_env="ieasyforecast_combined_forecast_pentad_file",
    skill_csv_env="ieasyforecast_pentadal_skill_metrics_file",
    api_horizon_type="pentad",
    neural_ensemble_func=sl.calculate_neural_ensemble_forecast,
    station_selection_env="ieasyforecast_config_file_station_selection",
)
DECAD_CONFIG = ShortTermHorizonConfig(
    name="decad",
    period_col="decad_in_year",
    period_in_month_col="decad_in_month",
    get_period_func=tl.get_decad_in_month,
    combined_csv_env="ieasyforecast_combined_forecast_decad_file",
    skill_csv_env="ieasyforecast_decadal_skill_metrics_file",
    api_horizon_type="decad",
    neural_ensemble_func=sl.calculate_neural_ensemble_forecast_decade,
    station_selection_env="ieasyforecast_config_file_station_selection_decad",
)


def postprocessing_forecasts():
    global timing_stats

    warnings.warn(
        "postprocessing_forecasts.py is deprecated. Use "
        "postprocessing_operational.py (daily), "
        "postprocessing_maintenance.py (gap-fill), or "
        "recalculate_skill_metrics.py (yearly).",
        DeprecationWarning,
        stacklevel=2,
    )

    logger.info("\n\n====== Post-processing forecasts =================")
    logger.debug(f"Script started at {dt.datetime.now()}.")

    # Accumulate errors from all save operations (fixes return value masking bug)
    errors = []

    with timer(timing_stats, "total execution"):
        with timer(timing_stats, "setup"):
            logger.info("\n\n------ Setting up --------------------------------")
            # Configuration
            sl.load_environment()

        # Get environment variable to determine which forecast horizon we process
        prediction_mode = os.getenv("SAPPHIRE_PREDICTION_MODE", "") or "BOTH"

        if prediction_mode not in ["PENTAD", "DECAD", "BOTH"]:
            logger.error(
                f"Invalid SAPPHIRE_PREDICTION_MODE: {prediction_mode}. "
                f"Expected 'PENTAD', 'DECAD', or 'BOTH'."
            )
            sys.exit(1)
        logger.info(f"Running postprocessing for prediction mode: {prediction_mode}")

        if prediction_mode in ["PENTAD", "BOTH"]:
            with timer(timing_stats, "reading pentadal data"):
                logger.info("\n\n------ Reading pentadal observed and modelled data -------")
                # Data processing
                observed, modelled = sl.read_observed_and_modelled_data_pentade()

            with timer(timing_stats, "calculating skill metrics pentads"):
                logger.info("\n\n------ Calculating skill metrics pentads -----------------")
                # Store the original timing_stats in case the function returns None
                original_timing_stats = timing_stats

                # Calculate forecast skill metrics, adds ensemble forecast to modelled
                skill_metrics_result, modelled, returned_timing_stats = (
                    skill_metrics.calculate_skill_metrics(PENTAD, observed, modelled, timing_stats)
                )

                # Use returned timing_stats only if it's not None
                if returned_timing_stats is not None:
                    timing_stats = returned_timing_stats
                else:
                    timing_stats = original_timing_stats

            with timer(timing_stats, "saving pentad results"):
                logger.info("\n\n------ Saving pentad results ----------------------")
                # Save the observed and modelled data to CSV files
                ret = file_writer.save_forecast_data(PENTAD, modelled)
                if ret is None:
                    logger.info("Pentadal forecast results for all models saved successfully.")
                else:
                    logger.error(f"Error saving the pentadal forecast results: {ret}")
                    errors.append(f"Pentad forecast save failed: {ret}")

                # Save the skill metrics to a CSV file
                ret = file_writer.save_skill_metrics(PENTAD, skill_metrics_result)
                if ret is None:
                    logger.info("Pentadal skill metrics saved successfully.")
                else:
                    logger.error(f"Error saving the pentadal skill metrics: {ret}")
                    errors.append(f"Pentad skill metrics save failed: {ret}")

        if prediction_mode in ["DECAD", "BOTH"]:
            with timer(timing_stats, "reading decadal data"):
                logger.info("\n\n------ Reading decadal observed and modelled data -------")
                # Data processing
                observed_decade, modelled_decade = sl.read_observed_and_modelled_data_decade()

            with timer(timing_stats, "calculating skill metrics decads"):
                logger.info("\n\n------ Calculating skill metrics decads -----------------")
                # Store the original timing_stats in case the function returns None
                original_timing_stats = timing_stats

                # Calculate forecast skill metrics, adds ensemble forecast to modelled
                skill_metrics_decade, modelled_decade, returned_timing_stats = (
                    skill_metrics.calculate_skill_metrics(
                        DECAD_CONFIG, observed_decade, modelled_decade, timing_stats
                    )
                )

                # Use returned timing_stats only if it's not None
                if returned_timing_stats is not None:
                    timing_stats = returned_timing_stats
                else:
                    timing_stats = original_timing_stats

            with timer(timing_stats, "saving decade results"):
                logger.info("\n\n------ Saving decade results ----------------------")
                # Save the observed and modelled data to CSV files
                ret = file_writer.save_forecast_data(DECAD_CONFIG, modelled_decade)
                if ret is None:
                    logger.info("Decadal forecast results for all models saved successfully.")
                else:
                    logger.error(f"Error saving the decadal forecast results: {ret}")
                    errors.append(f"Decade forecast save failed: {ret}")

                # Save the skill metrics to a CSV file
                ret = file_writer.save_skill_metrics(DECAD_CONFIG, skill_metrics_decade)
                if ret is None:
                    logger.info("Decadal skill metrics saved successfully.")
                else:
                    logger.error(f"Error saving the decadal skill metrics: {ret}")
                    errors.append(f"Decade skill metrics save failed: {ret}")

    # Print timing summary
    summary, total = timing_stats.summary()
    logger.info("\n\n")
    logger.info("Timing summary for postprocessin_forecasts:")
    logger.info(f"Total execution time: {total:.2f} seconds")
    logger.info("Breakdown by section:")
    for entry in summary:
        logger.info(f"{entry['section']}:")
        logger.info(f"  Total time: {entry['total_time']:.2f} seconds ({entry['percentage']:.1f}%)")
        logger.info(f"  Average time per call: {entry['avg_time']:.2f} seconds")
        logger.info(f"  Number of calls: {entry['calls']}")

    # Check if any save operations failed
    if errors:
        logger.error(f"Script finished with {len(errors)} error(s):")
        for error in errors:
            logger.error(f"  - {error}")
        sys.exit(1)
    else:
        logger.info(f"Script finished successfully at {dt.datetime.now()}.")
        sys.exit(0)


if __name__ == "__main__":
    # Post-process the forecasts
    postprocessing_forecasts()
