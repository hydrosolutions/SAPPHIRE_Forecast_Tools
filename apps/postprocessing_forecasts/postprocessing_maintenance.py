# postprocessing_maintenance.py
# Nightly gap-fill entry point: detects missing ensemble forecasts
# within a lookback window, creates them from pre-calculated skill metrics,
# and saves the results.
#
# Usage:
#   ieasyhydroforecast_env_file_path=/path/to/.env \
#   SAPPHIRE_PREDICTION_MODE=BOTH \
#   POSTPROCESSING_GAPFILL_WINDOW_DAYS=7 \
#   python postprocessing_maintenance.py

import datetime as dt
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
import tag_library as tl
from src import data_reader, ensemble_calculator, file_writer, gap_detector
from src import postprocessing_tools as pt
from src.horizon_config import ShortTermHorizonConfig
from src.postprocessing_tools import TimingStats, timer

# region Logging
logging.basicConfig(level=logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

if not os.path.exists("logs"):
    os.makedirs("logs")

file_handler = TimedRotatingFileHandler(
    "logs/log_maintenance", when="midnight", interval=1, backupCount=30
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

PENTAD = ShortTermHorizonConfig(
    name="pentad",
    period_col="pentad_in_year",
    period_in_month_col="pentad_in_month",
    get_period_func=tl.get_pentad,
    combined_csv_env="ieasyforecast_combined_forecast_pentad_file",
    skill_csv_env="ieasyforecast_pentadal_skill_metrics_file",
    api_horizon_type="pentad",
    neural_ensemble_func=sl.calculate_neural_ensemble_forecast,
)
DECAD = ShortTermHorizonConfig(
    name="decad",
    period_col="decad_in_year",
    period_in_month_col="decad_in_month",
    get_period_func=tl.get_decad_in_month,
    combined_csv_env="ieasyforecast_combined_forecast_decad_file",
    skill_csv_env="ieasyforecast_decadal_skill_metrics_file",
    api_horizon_type="decad",
    neural_ensemble_func=sl.calculate_neural_ensemble_forecast_decade,
)


def postprocessing_maintenance():
    global timing_stats

    logger.info("\n\n====== Post-processing forecasts (MAINTENANCE / GAP-FILL) =====")
    logger.debug(f"Script started at {dt.datetime.now()}.")

    errors = []
    lookback = int(os.getenv("POSTPROCESSING_GAPFILL_WINDOW_DAYS", "7"))
    logger.info(f"Gap-fill lookback window: {lookback} days")

    with timer(timing_stats, "total execution"):
        with timer(timing_stats, "setup"):
            logger.info("\n\n------ Setting up --------------------------------")
            sl.load_environment()

        prediction_mode = os.getenv("SAPPHIRE_PREDICTION_MODE", "") or "BOTH"
        if prediction_mode not in ["PENTAD", "DECAD", "BOTH"]:
            logger.error(
                f"Invalid SAPPHIRE_PREDICTION_MODE: {prediction_mode}. "
                f"Expected 'PENTAD', 'DECAD', or 'BOTH'."
            )
            sys.exit(1)
        logger.info(f"Running maintenance postprocessing for mode: {prediction_mode}")

        if prediction_mode in ["PENTAD", "BOTH"]:
            _fill_gaps_for_horizon(PENTAD, lookback, errors)

        if prediction_mode in ["DECAD", "BOTH"]:
            _fill_gaps_for_horizon(DECAD, lookback, errors)

    # Print timing summary
    summary, total = timing_stats.summary()
    logger.info("\n\n")
    logger.info("Timing summary for postprocessing_maintenance:")
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


def _fill_gaps_for_horizon(config, lookback, errors):
    """Detect and fill ensemble gaps for one horizon type."""
    global timing_stats

    label = config.name.upper()

    with timer(timing_stats, f"reading {label} combined forecasts"):
        logger.info(f"\n\n------ Reading {label} combined forecasts for gap detection ----")
        combined = data_reader.read_combined_forecasts(config.name)

    if combined.empty:
        logger.info(f"No {label} combined forecasts found. Skipping gap detection.")
        return

    with timer(timing_stats, f"detecting {label} gaps"):
        gaps = gap_detector.detect_missing_ensembles(
            combined,
            lookback,
            ensemble_models={"EM", "NE"},
        )

    if gaps.empty:
        logger.info(f"No {label} ensemble gaps found. Nothing to fill.")
        return

    # NE gaps are created by the operational pipeline and cannot be
    # filled by maintenance — warn and keep only EM gaps.
    ne_gaps = gaps[gaps["model_short"] == "NE"]
    if not ne_gaps.empty:
        logger.warning(
            "Found %d NE gaps (created by operational pipeline, not fillable by maintenance): %s",
            len(ne_gaps),
            ne_gaps[["date", "code"]].drop_duplicates().to_dict("records")[:10],
        )
    gaps = gaps[gaps["model_short"] == "EM"].reset_index(drop=True)

    if gaps.empty:
        logger.info(f"No fillable {label} EM gaps after filtering. Nothing to fill.")
        return

    logger.info(f"Found {len(gaps)} {label} (date, code) pairs needing gap-fill")

    with timer(timing_stats, f"reading {label} data for gap-fill"):
        logger.info(f"\n\n------ Reading {label} observed and modelled data ----")
        _, modelled = data_reader.read_observed_and_modelled_data(config.name)
        modelled = sl.calculate_virtual_stations_data(modelled)
        modelled = config.neural_ensemble_func(modelled)

    # Filter modelled to gap dates only
    gap_dates = set(gaps["date"].unique())
    gap_codes = set(gaps["code"].unique())
    modelled_filtered = modelled[
        modelled["date"].isin(gap_dates) & modelled["code"].isin(gap_codes)
    ].copy()

    if modelled_filtered.empty:
        logger.warning(f"No {label} forecast data available for gap dates. Cannot fill gaps.")
        return

    with timer(timing_stats, f"reading {label} skill metrics"):
        skill_stats = data_reader.read_skill_metrics(config.name)

    if skill_stats.empty:
        logger.warning(f"No {label} skill metrics available. Cannot create ensembles.")
        return

    with timer(timing_stats, f"creating {label} gap-fill ensembles"):
        joint, _ = ensemble_calculator.create_ensemble_forecasts(
            forecasts=modelled_filtered,
            skill_stats=skill_stats,
            period_col=config.period_col,
            period_in_month_col=config.period_in_month_col,
            get_period_in_month_func=config.get_period_func,
        )

    # Extract only the new EM rows from the gap-fill output
    new_em_rows = joint[joint["model_short"] == "EM"].copy()

    if new_em_rows.empty:
        logger.info(f"No new {label} ensemble rows created. Nothing to save.")
        return

    # Merge new EM rows into existing combined data to preserve history.
    # Without this merge, save would overwrite the combined CSV with
    # only the gap-fill rows, losing all non-gap historical data.
    merged = pd.concat([combined, new_em_rows], ignore_index=True)
    # Deduplicate on (date, code, model_short) in case of overlap
    merged = merged.drop_duplicates(subset=["date", "code", "model_short"], keep="last")

    logger.info(
        f"Merged {len(new_em_rows)} new EM rows into "
        f"{len(combined)} existing rows -> {len(merged)} total"
    )

    with timer(timing_stats, f"saving {label} gap-fill results"):
        ret = file_writer.save_forecast_data(config, merged)
        if ret is None:
            logger.info(f"{label} gap-fill forecast results saved successfully.")
        else:
            logger.error(f"Error saving {label} gap-fill forecast results: {ret}")
            errors.append(f"{label} gap-fill save failed: {ret}")

    pt.log_most_recent_forecasts(config, merged)

    # Audit trail
    logger.info(f"AUDIT: Filled {len(gaps)} {label} ensemble gaps (lookback={lookback} days)")
    for _, gap_row in gaps.iterrows():
        logger.info(f"  Filled: date={gap_row['date']}, code={gap_row['code']}")


if __name__ == "__main__":
    postprocessing_maintenance()
