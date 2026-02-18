# postprocessing_maintenance_long_term.py
# Gap-fill entry point for monthly (long-term) ensemble forecasts.
# Detects missing ensemble rows in recent months, creates them from
# pre-calculated skill metrics. Does NOT recalculate skill metrics
# (that remains in recalculate_skill_metrics.py).
#
# Usage:
#   ieasyhydroforecast_env_file_path=/path/to/.env \
#   POSTPROCESSING_GAPFILL_WINDOW_MONTHS=3 \
#   python postprocessing_maintenance_long_term.py

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
from src import gap_detector
from src import file_writer

# region Logging
logging.basicConfig(level=logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

if not os.path.exists('logs'):
    os.makedirs('logs')

file_handler = TimedRotatingFileHandler(
    'logs/log_maintenance_long_term',
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


def postprocessing_maintenance_long_term():
    global timing_stats

    logger.info(
        "\n\n====== Post-processing forecasts "
        "(MAINTENANCE / GAP-FILL LONG-TERM) ====="
    )
    logger.debug(f"Script started at {dt.datetime.now()}.")

    errors = []
    lookback = int(
        os.getenv('POSTPROCESSING_GAPFILL_WINDOW_MONTHS', '3')
    )
    logger.info(f"Monthly gap-fill lookback window: {lookback} months")

    with timer(timing_stats, 'total execution'):

        with timer(timing_stats, 'setup'):
            logger.info(
                "\n\n------ Setting up --------------------------------"
            )
            sl.load_environment()
            codes = _read_station_codes()

        # 1. Read monthly combined forecasts for gap detection
        with timer(timing_stats, 'reading monthly combined forecasts'):
            logger.info(
                "\n\n------ Reading monthly combined forecasts ---------"
            )
            combined = data_reader.read_monthly_combined_forecasts()

        if combined.empty:
            logger.info(
                "No monthly combined forecasts found. "
                "Skipping gap detection."
            )
            _print_timing()
            sys.exit(0)

        # 2. Detect missing ensemble rows
        with timer(timing_stats, 'detecting monthly gaps'):
            gaps = gap_detector.detect_missing_monthly_ensembles(
                combined, lookback,
                ensemble_models={'EM', 'Skilled Mean', 'Naive Mean'},
            )

        if gaps.empty:
            logger.info(
                "No monthly ensemble gaps found. Nothing to fill."
            )
            _print_timing()
            sys.exit(0)

        logger.info(
            "Found %d (year, month, code, model_short) gaps needing "
            "gap-fill",
            len(gaps),
        )

        # 3. Read skill metrics
        with timer(timing_stats, 'reading monthly skill metrics'):
            logger.info(
                "\n\n------ Reading pre-calculated monthly skill "
                "metrics -----"
            )
            skill_stats = data_reader.read_skill_metrics('month')

        if skill_stats.empty:
            logger.warning(
                "No monthly skill metrics available. "
                "Cannot create ensembles."
            )
            _print_timing()
            sys.exit(0)

        # 4. Read forecasts for gap periods from API
        gap_years = gaps['year'].unique()
        start_year = int(gap_years.min())
        end_year = int(gap_years.max())

        with timer(timing_stats, 'reading monthly forecasts for gaps'):
            logger.info(
                "\n\n------ Reading monthly forecasts for gap-fill ----"
            )
            all_forecasts = data_reader.read_monthly_forecasts(
                codes, start_year, end_year,
            )

        if all_forecasts.empty:
            logger.warning(
                "No monthly forecast data available for gap years. "
                "Cannot fill gaps."
            )
            _print_timing()
            sys.exit(0)

        # Filter to gap (year, month, code) tuples (deduplicated,
        # since gaps may have multiple model_short per triple)
        gap_set = set(
            gaps[['year', 'month', 'code']].drop_duplicates()
            .itertuples(index=False, name=None)
        )
        # Ensure year/month are numeric for comparison
        all_forecasts['year'] = pd.to_numeric(
            all_forecasts['year'], errors='coerce'
        ).astype('Int64')
        all_forecasts['month'] = pd.to_numeric(
            all_forecasts['month'], errors='coerce'
        ).astype('Int64')

        filtered = all_forecasts[
            all_forecasts.apply(
                lambda r: (r['year'], r['month'], str(r['code']))
                in gap_set,
                axis=1,
            )
        ].copy()

        if filtered.empty:
            logger.warning(
                "No forecast data matches gap tuples. "
                "Cannot fill gaps."
            )
            _print_timing()
            sys.exit(0)

        # Ensure month_in_year and forecasted_discharge exist
        if 'month_in_year' not in filtered.columns:
            if 'month' in filtered.columns:
                filtered['month_in_year'] = filtered['month']
        if 'forecasted_discharge' not in filtered.columns:
            if 'q50' in filtered.columns:
                filtered['forecasted_discharge'] = (
                    filtered['q50'].astype(float)
                )

        # 5. Create ensemble forecasts for gap periods
        with timer(timing_stats, 'creating monthly gap-fill ensembles'):
            logger.info(
                "\n\n------ Creating monthly ensemble forecasts "
                "for gaps ---"
            )
            joint = ensemble_calculator.create_monthly_ensemble_forecasts(
                filtered, skill_stats,
            )

        # Extract only ensemble rows that match actual gap tuples.
        # create_monthly_ensemble_forecasts creates all 3 types for
        # every period, but we only want those that were actually
        # missing (per the gap detector).
        gap_keys = set(
            gaps[['year', 'month', 'code', 'model_short']]
            .itertuples(index=False, name=None)
        )
        ensemble_models = {'EM', 'Skilled Mean', 'Naive Mean'}
        new_ensemble = joint[
            joint['model_short'].isin(ensemble_models)
        ].copy()
        new_ensemble = new_ensemble[
            new_ensemble.apply(
                lambda r: (
                    r['year'], r['month'], str(r['code']),
                    r['model_short'],
                ) in gap_keys,
                axis=1,
            )
        ]

        if new_ensemble.empty:
            logger.info(
                "No new monthly ensemble rows created. "
                "Nothing to save."
            )
            _print_timing()
            sys.exit(0)

        # 6. Merge into existing combined forecasts
        merged = pd.concat(
            [combined, new_ensemble], ignore_index=True,
        )
        # Deduplicate on (year, month, code, model_short)
        dedup_cols = ['year', 'month', 'code', 'model_short']
        available_dedup = [c for c in dedup_cols if c in merged.columns]
        merged = merged.drop_duplicates(
            subset=available_dedup, keep='last',
        )

        logger.info(
            "Merged %d new ensemble rows into %d existing rows "
            "-> %d total",
            len(new_ensemble), len(combined), len(merged),
        )

        # 7. Save
        with timer(timing_stats, 'saving monthly gap-fill results'):
            logger.info(
                "\n\n------ Saving monthly gap-fill results -----------"
            )
            ret = file_writer.save_monthly_forecast_data(merged)
            if ret is None:
                logger.info(
                    "Monthly gap-fill results saved successfully."
                )
            else:
                logger.error(
                    f"Error saving monthly gap-fill results: {ret}"
                )
                errors.append(
                    f"Monthly gap-fill save failed: {ret}"
                )

        pt.log_most_recent_forecasts_monthly(merged)

        # Audit trail — deduplicate to (year, month, code) level
        unique_gaps = gaps[
            ['year', 'month', 'code']
        ].drop_duplicates()
        logger.info(
            "AUDIT: Filled %d monthly ensemble gaps "
            "(%d unique periods, lookback=%d months)",
            len(gaps), len(unique_gaps), lookback,
        )
        for _, gap_row in unique_gaps.iterrows():
            logger.info(
                "  Filled: year=%d, month=%d, code=%s",
                gap_row['year'], gap_row['month'], gap_row['code'],
            )

    _print_timing()

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


def _print_timing():
    """Print timing summary."""
    summary, total = timing_stats.summary()
    logger.info("\n\n")
    logger.info(
        "Timing summary for postprocessing_maintenance_long_term:"
    )
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


if __name__ == "__main__":
    postprocessing_maintenance_long_term()
