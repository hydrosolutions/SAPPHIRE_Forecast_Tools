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
import json
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
from src.horizon_config import ShortTermHorizonConfig
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
DECAD = ShortTermHorizonConfig(
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


def _read_station_codes(config):
    """Read station codes from the horizon's station selection config file.

    Args:
        config: ShortTermHorizonConfig with station_selection_env field.

    Returns:
        List of station code strings.
    """
    config_path = os.path.join(
        os.getenv("ieasyforecast_configuration_path", ""),
        os.getenv(config.station_selection_env, ""),
    )
    with open(config_path) as f:
        station_config = json.load(f)
    codes = [str(c) for c in station_config.get("stationsID", [])]
    logger.info("Read %d station codes for %s", len(codes), config.name)
    return codes


def _run_short_term_postprocessing(
    config,
    today,
    errors,
    timing_stats_,
    start_year=None,
    end_year=None,
    dry_run=False,
    write_csv=True,
    require_api=False,
):
    """Read data, create ensembles, save results for one horizon type.

    Args:
        config: ShortTermHorizonConfig for the horizon (PENTAD or DECAD).
        today: Anchor date for this run.
        errors: List accumulating error messages (mutated in place).
        timing_stats_: TimingStats instance for section timing.
        start_year: First calendar year of data to read. Defaults to
            ``today.year`` (operational behavior).
        end_year: Last calendar year of data to read. Defaults to
            ``today.year`` (operational behavior).
        dry_run: When True, skip the save entirely and only log the coverage
            that WOULD have been written. Used by the backfill CLI.
        write_csv: Forwarded to ``file_writer.save_forecast_data``; when False
            the save performs the API write only (no CSV files).
        require_api: Forwarded to ``file_writer.save_forecast_data``; when True
            a non-performed or failed API write raises instead of being
            swallowed. Used by the backfill CLI so a run cannot report success
            without writing.
    """
    label = config.name.upper()

    sy = start_year if start_year is not None else today.year
    ey = end_year if end_year is not None else today.year

    codes = _read_station_codes(config)
    if not codes:
        logger.warning(
            "No station codes for %s — station selection file may be empty",
            config.name,
        )

    with timer(timing_stats_, f"reading {config.name} data"):
        logger.info(f"\n\n------ Reading {config.name} observed and modelled data -------")
        _, modelled = data_reader.read_observed_and_modelled_data(
            config.name,
            codes=codes,
            start_year=sy,
            end_year=ey,
        )
        if modelled.empty:
            logger.warning(
                "No %s modelled data available. Skipping virtual stations.",
                config.name,
            )
        else:
            modelled = sl.calculate_virtual_stations_data(modelled)
            modelled = config.neural_ensemble_func(modelled)

    with timer(timing_stats_, f"reading {config.name} skill metrics"):
        logger.info(f"\n\n------ Reading pre-calculated {config.name} skill metrics ----")
        skill_stats = data_reader.read_skill_metrics(config.name, codes=codes)

    if skill_stats.empty:
        logger.warning(
            "No %s skill metrics available. "
            "Skipping ensemble creation. "
            "Run recalculate_skill_metrics.py first.",
            config.name,
        )
    elif modelled.empty:
        logger.warning(
            "No %s modelled data available. Skipping ensemble creation.",
            config.name,
        )
    else:
        with timer(timing_stats_, f"creating {config.name} ensembles"):
            logger.info(f"\n\n------ Creating {config.name} ensemble forecasts ----")
            modelled, skill_stats = ensemble_calculator.create_ensemble_forecasts(
                forecasts=modelled,
                skill_stats=skill_stats,
                period_col=config.period_col,
                period_in_month_col=config.period_in_month_col,
                get_period_in_month_func=config.get_period_func,
            )

    if dry_run:
        _period_col = config.period_col
        n_rows = 0 if modelled is None else len(modelled)
        coverage = "n/a"
        if modelled is not None and not modelled.empty:
            n_periods = (
                modelled[_period_col].nunique() if _period_col in modelled.columns else "n/a"
            )
            n_models = (
                modelled["model_short"].nunique() if "model_short" in modelled.columns else "n/a"
            )
            coverage = f"{n_periods} distinct {_period_col} x {n_models} model(s)"
        logger.info(
            "%s DRY-RUN: would write %d row(s) (%s); save skipped.",
            label,
            n_rows,
            coverage,
        )
    else:
        with timer(timing_stats_, f"saving {config.name} results"):
            logger.info(f"\n\n------ Saving {config.name} results ----------------")
            ret = file_writer.save_forecast_data(
                config, modelled, write_csv=write_csv, require_api=require_api
            )
            if ret is None:
                logger.info(f"{label} forecast results saved successfully.")
            else:
                logger.error(f"Error saving {label} forecast results: {ret}")
                errors.append(f"{label} forecast save failed: {ret}")

    if not dry_run:
        pt.log_most_recent_forecasts(config, modelled)


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

        _env_date = os.getenv("SAPPHIRE_FORECAST_DATE", "").strip()
        if _env_date:
            try:
                today = dt.datetime.strptime(_env_date, "%Y-%m-%d").date()
                logger.info("Using explicit forecast date from SAPPHIRE_FORECAST_DATE: %s", today)
            except ValueError:
                logger.error(
                    "Invalid SAPPHIRE_FORECAST_DATE=%r — expected YYYY-MM-DD. Falling back to today.",
                    _env_date,
                )
                today = dt.date.today()
        else:
            today = dt.date.today()

        if prediction_mode in ["PENTAD", "BOTH", "ALL"]:
            if not is_pentad_boundary(today):
                logger.info(
                    "Skipping pentad postprocessing: %s is not a pentad "
                    "boundary day (boundaries: 5/10/15/20/25/last)",
                    today,
                )
            else:
                _run_short_term_postprocessing(PENTAD, today, errors, timing_stats)

        if prediction_mode in ["DECAD", "BOTH", "ALL"]:
            if not is_decad_boundary(today):
                logger.info(
                    "Skipping decad postprocessing: %s is not a decad "
                    "boundary day (boundaries: 10/20/last)",
                    today,
                )
            else:
                _run_short_term_postprocessing(DECAD, today, errors, timing_stats)

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
