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
import tag_library as tl

from src import postprocessing_tools as pt
from src.postprocessing_tools import TimingStats, timer
from src import data_reader
from src import ensemble_calculator
from src import gap_detector

# region Logging
logging.basicConfig(level=logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

if not os.path.exists('logs'):
    os.makedirs('logs')

file_handler = TimedRotatingFileHandler(
    'logs/log_maintenance', when='midnight', interval=1, backupCount=30
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


def postprocessing_maintenance():
    global timing_stats

    logger.info(
        "\n\n====== Post-processing forecasts (MAINTENANCE / GAP-FILL) ====="
    )
    logger.debug(f"Script started at {dt.datetime.now()}.")

    errors = []
    lookback = int(
        os.getenv('POSTPROCESSING_GAPFILL_WINDOW_DAYS', '7')
    )
    logger.info(f"Gap-fill lookback window: {lookback} days")

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
            f"Running maintenance postprocessing for mode: "
            f"{prediction_mode}"
        )

        if prediction_mode in ['PENTAD', 'BOTH']:
            _fill_gaps_for_horizon(
                horizon_type='pentad',
                read_data_func=sl.read_observed_and_modelled_data_pentade,
                save_func=fl.save_forecast_data_pentad,
                log_func=pt.log_most_recent_forecasts_pentad,
                period_col='pentad_in_year',
                period_in_month_col='pentad_in_month',
                get_period_in_month_func=tl.get_pentad,
                lookback=lookback,
                errors=errors,
            )

        if prediction_mode in ['DECAD', 'BOTH']:
            _fill_gaps_for_horizon(
                horizon_type='decad',
                read_data_func=sl.read_observed_and_modelled_data_decade,
                save_func=fl.save_forecast_data_decade,
                log_func=pt.log_most_recent_forecasts_decade,
                period_col='decad_in_year',
                period_in_month_col='decad_in_month',
                get_period_in_month_func=tl.get_decad_in_month,
                lookback=lookback,
                errors=errors,
            )

    # Print timing summary
    summary, total = timing_stats.summary()
    logger.info("\n\n")
    logger.info("Timing summary for postprocessing_maintenance:")
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


def _fill_gaps_for_horizon(
    horizon_type,
    read_data_func,
    save_func,
    log_func,
    period_col,
    period_in_month_col,
    get_period_in_month_func,
    lookback,
    errors,
):
    """Detect and fill ensemble gaps for one horizon type."""
    global timing_stats

    label = horizon_type.upper()

    with timer(timing_stats, f'reading {label} combined forecasts'):
        logger.info(
            f"\n\n------ Reading {label} combined forecasts for gap "
            f"detection ----"
        )
        combined = gap_detector.read_combined_forecasts(horizon_type)

    if combined.empty:
        logger.info(
            f"No {label} combined forecasts found. Skipping gap detection."
        )
        return

    with timer(timing_stats, f'detecting {label} gaps'):
        gaps = gap_detector.detect_missing_ensembles(combined, lookback)

    if gaps.empty:
        logger.info(f"No {label} ensemble gaps found. Nothing to fill.")
        return

    logger.info(
        f"Found {len(gaps)} {label} (date, code) pairs needing gap-fill"
    )

    with timer(timing_stats, f'reading {label} data for gap-fill'):
        logger.info(
            f"\n\n------ Reading {label} observed and modelled data ----"
        )
        observed, modelled = read_data_func()

    # Filter modelled to gap dates only
    gap_dates = set(gaps['date'].unique())
    gap_codes = set(gaps['code'].unique())
    modelled_filtered = modelled[
        modelled['date'].isin(gap_dates) &
        modelled['code'].isin(gap_codes)
    ].copy()

    if modelled_filtered.empty:
        logger.warning(
            f"No {label} forecast data available for gap dates. "
            f"Cannot fill gaps."
        )
        return

    with timer(timing_stats, f'reading {label} skill metrics'):
        skill_metrics = data_reader.read_skill_metrics(horizon_type)

    if skill_metrics.empty:
        logger.warning(
            f"No {label} skill metrics available. Cannot create ensembles."
        )
        return

    with timer(timing_stats, f'creating {label} gap-fill ensembles'):
        joint, _ = ensemble_calculator.create_ensemble_forecasts(
            forecasts=modelled_filtered,
            skill_stats=skill_metrics,
            observed=observed,
            period_col=period_col,
            period_in_month_col=period_in_month_col,
            get_period_in_month_func=get_period_in_month_func,
            sdivsigma_nse_func=fl.sdivsigma_nse,
            mae_func=fl.mae,
            forecast_accuracy_hydromet_func=fl.forecast_accuracy_hydromet,
        )

    with timer(timing_stats, f'saving {label} gap-fill results'):
        ret = save_func(joint)
        if ret is None:
            logger.info(
                f"{label} gap-fill forecast results saved successfully."
            )
        else:
            logger.error(
                f"Error saving {label} gap-fill forecast results: {ret}"
            )
            errors.append(f"{label} gap-fill save failed: {ret}")

    log_func(joint)

    # Audit trail
    logger.info(
        f"AUDIT: Filled {len(gaps)} {label} ensemble gaps "
        f"(lookback={lookback} days)"
    )
    for _, gap_row in gaps.iterrows():
        logger.info(
            f"  Filled: date={gap_row['date']}, code={gap_row['code']}"
        )


if __name__ == "__main__":
    postprocessing_maintenance()
