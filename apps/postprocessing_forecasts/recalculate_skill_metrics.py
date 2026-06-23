# recalculate_skill_metrics.py
# Yearly (or on-demand) entry point: reads ALL historical data,
# recalculates ALL skill metrics, and saves everything.
# This is the slow path — identical to the legacy postprocessing_forecasts.py
# behavior but without the deprecation warning overhead.
#
# Usage:
#   ieasyhydroforecast_env_file_path=/path/to/.env \
#   SAPPHIRE_PREDICTION_MODE=BOTH python recalculate_skill_metrics.py
#
# Prediction modes:
#   PENTAD  — pentadal skill metrics only
#   DECAD   — decadal skill metrics only
#   BOTH    — pentad + decad (backward compatible)
#   MONTHLY   — monthly skill metrics only (long-term forecasts)
#   QUARTERLY — quarterly skill metrics (aggregated from monthly)
#   SEASONAL  — seasonal skill metrics (aggregated from monthly)
#   ALL       — pentad + decad + monthly + quarterly + seasonal + daily

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
import tag_library as tl
from long_term_horizon_resolver import (
    seasonal_config_name,
    seasonal_horizon_value,
    supported_long_term_modes,
)
from src import data_reader, file_writer, skill_metrics
from src import postprocessing_tools as pt
from src.horizon_config import ShortTermHorizonConfig
from src.postprocessing_tools import TimingStats, timer

# region Logging
logging.basicConfig(level=logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

if not os.path.exists("logs"):
    os.makedirs("logs")

file_handler = TimedRotatingFileHandler(
    "logs/log_recalc", when="midnight", interval=1, backupCount=30
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

VALID_MODES = [
    "PENTAD",
    "DECAD",
    "BOTH",
    "MONTHLY",
    "DAILY",
    "QUARTERLY",
    "SEASONAL",
    "ALL",
]


def _supported_seasonal_issue_leads() -> list[int]:
    """Return unique supported seasonal issue leads for this deployment."""
    modes = set(supported_long_term_modes())
    leads = []
    for issue_month in (1, 2, 3, 4):
        if seasonal_config_name(issue_month) in modes:
            lead = seasonal_horizon_value(issue_month)
            if lead not in leads:
                leads.append(lead)
    return leads


def _read_station_codes(config):
    """Read station codes from the horizon's station selection config file.

    Args:
        config: ShortTermHorizonConfig with station_selection_env field.

    Returns:
        list[str]: Station codes for the given horizon.
    """
    override = os.getenv("SAPPHIRE_RECALC_STATION_CODE", "").strip()
    if override:
        logger.info("Scoped recalculation for station %s (%s)", override, config.name)
        return [override]

    config_path = os.path.join(
        os.getenv("ieasyforecast_configuration_path", ""),
        os.getenv(config.station_selection_env, ""),
    )
    with open(config_path) as f:
        station_config = json.load(f)
    codes = [str(c) for c in station_config.get("stationsID", [])]
    logger.info("Read %d station codes for %s", len(codes), config.name)
    return codes


def _run_short_term_recalc(config, skill_metrics_year, errors, timing_stats_, codes=None):
    """Read data, recalculate skill metrics, save results for one horizon."""
    label = config.name.upper()

    with timer(timing_stats_, f"reading {config.name} data"):
        logger.info(f"\n\n------ Reading {config.name} observed and modelled data -------")
        observed, modelled = data_reader.read_observed_and_modelled_data(
            config.name,
            codes=codes,
        )
        if observed.empty and modelled.empty:
            logger.warning(
                "No observed or modelled data for %s codes=%s — skipping skill recalc",
                config.name,
                codes,
            )
            return timing_stats_
        if observed.empty:
            logger.warning("No observed data for %s codes=%s — skipping", config.name, codes)
            return timing_stats_
        if modelled.empty:
            logger.warning("No modelled data for %s codes=%s — skipping", config.name, codes)
            return timing_stats_

        # Skip virtual station computation for scoped recalcs — the
        # scoped DataFrame lacks the other contributing stations, so
        # calculate_virtual_stations_data would produce incorrect partial sums.
        if not os.getenv("SAPPHIRE_RECALC_STATION_CODE", "").strip():
            modelled = sl.calculate_virtual_stations_data(modelled)
        modelled = config.neural_ensemble_func(modelled)

    with timer(timing_stats_, f"calculating skill metrics {config.name}"):
        logger.info(f"\n\n------ Calculating skill metrics {config.name} --------")
        skill_metrics_result, modelled, returned_timing_stats = (
            skill_metrics.calculate_skill_metrics(
                config,
                observed,
                modelled,
                timing_stats_,
                exclude_models=["EM"],  # PP-030: skip EM re-derivation at boundaries
            )
        )

    with timer(
        returned_timing_stats if returned_timing_stats is not None else timing_stats_,
        f"saving {config.name} results",
    ):
        logger.info(f"\n\n------ Saving {config.name} results --------------------")
        ret = file_writer.save_forecast_data(config, modelled)
        if ret is None:
            logger.info(f"{label} forecast results saved successfully.")
        else:
            logger.error(f"Error saving {label} forecast results: {ret}")
            errors.append(f"{label} forecast save failed: {ret}")

        ret = file_writer.save_skill_metrics(config, skill_metrics_result, year=skill_metrics_year)
        if ret is None:
            logger.info(f"{label} skill metrics saved successfully.")
        else:
            logger.error(f"Error saving {label} skill metrics: {ret}")
            errors.append(f"{label} skill metrics save failed: {ret}")

    pt.log_most_recent_forecasts(config, modelled)

    return returned_timing_stats if returned_timing_stats is not None else timing_stats_


def recalculate_skill_metrics():
    global timing_stats

    logger.info("\n\n====== Recalculating ALL skill metrics =========================")
    logger.debug(f"Script started at {dt.datetime.now()}.")

    errors = []

    with timer(timing_stats, "total execution"):
        with timer(timing_stats, "setup"):
            logger.info("\n\n------ Setting up --------------------------------")
            sl.load_environment()

        prediction_mode = os.getenv("SAPPHIRE_PREDICTION_MODE", "") or "BOTH"
        if prediction_mode not in VALID_MODES:
            logger.error(
                f"Invalid SAPPHIRE_PREDICTION_MODE: {prediction_mode}. "
                f"Expected one of {VALID_MODES}."
            )
            sys.exit(1)
        logger.info(f"Running skill metrics recalculation for mode: {prediction_mode}")

        skill_metrics_year = int(os.getenv("SAPPHIRE_SKILL_METRICS_YEAR", dt.date.today().year))
        logger.info(f"Skill metrics target year: {skill_metrics_year}")

        if prediction_mode in ["PENTAD", "BOTH", "ALL"]:
            codes = _read_station_codes(PENTAD)
            if not codes:
                logger.warning("No station codes for pentad — selection file may be empty")
            timing_stats = _run_short_term_recalc(
                PENTAD,
                skill_metrics_year,
                errors,
                timing_stats,
                codes=codes,
            )

        if prediction_mode in ["DECAD", "BOTH", "ALL"]:
            codes = _read_station_codes(DECAD)
            if not codes:
                logger.warning("No station codes for decad — selection file may be empty")
            timing_stats = _run_short_term_recalc(
                DECAD,
                skill_metrics_year,
                errors,
                timing_stats,
                codes=codes,
            )

        if prediction_mode in ["MONTHLY", "ALL"]:
            current_year = dt.date.today().year
            start_year = int(
                os.getenv(
                    "SAPPHIRE_SKILL_METRICS_START_YEAR",
                    os.getenv("SAPPHIRE_RECALC_START_YEAR", current_year - 20),
                )
            )
            end_year = int(os.getenv("SAPPHIRE_RECALC_END_YEAR", current_year))
            codes = _read_station_codes(PENTAD)

            with timer(timing_stats, "reading monthly data"):
                logger.info("\n\n------ Reading monthly observed and forecast data -------")
                monthly_obs = data_reader.read_monthly_observations(codes, start_year, end_year)
                monthly_fc = data_reader.read_monthly_forecasts(codes, start_year, end_year)

            with timer(timing_stats, "calculating skill metrics monthly"):
                logger.info("\n\n------ Calculating skill metrics monthly --------")
                original_timing_stats = timing_stats
                monthly_skill, monthly_joint, returned_timing_stats = (
                    skill_metrics.calculate_monthly_skill_metrics(
                        monthly_obs, monthly_fc, timing_stats
                    )
                )
                if returned_timing_stats is not None:
                    timing_stats = returned_timing_stats
                else:
                    timing_stats = original_timing_stats

            with timer(timing_stats, "saving monthly results"):
                logger.info("\n\n------ Saving monthly results -------------------")
                ret = file_writer.save_monthly_forecast_data(monthly_joint)
                if ret is None:
                    logger.info("Monthly forecast data saved successfully.")
                else:
                    logger.error(f"Error saving monthly forecast data: {ret}")
                    errors.append(f"Monthly forecast data save failed: {ret}")

                ret = file_writer.save_monthly_skill_metrics(monthly_skill, year=skill_metrics_year)
                if ret is None:
                    logger.info("Monthly skill metrics saved successfully.")
                else:
                    logger.error(f"Error saving monthly skill metrics: {ret}")
                    errors.append(f"Monthly skill metrics save failed: {ret}")

            pt.log_most_recent_forecasts_monthly(monthly_joint)

        if prediction_mode in ["QUARTERLY", "ALL"]:
            current_year = dt.date.today().year
            start_year = int(
                os.getenv(
                    "SAPPHIRE_SKILL_METRICS_START_YEAR",
                    os.getenv("SAPPHIRE_RECALC_START_YEAR", current_year - 20),
                )
            )
            end_year = int(os.getenv("SAPPHIRE_RECALC_END_YEAR", current_year))
            codes = _read_station_codes(PENTAD)

            with timer(timing_stats, "reading quarterly data"):
                logger.info("\n\n------ Reading quarterly data (aggregated from monthly) -------")
                quarterly_obs = data_reader.read_quarterly_observations(codes, start_year, end_year)
                quarterly_fc = data_reader.read_quarterly_forecasts(codes, start_year, end_year)

            with timer(timing_stats, "calculating skill metrics quarterly"):
                logger.info("\n\n------ Calculating skill metrics quarterly ------")
                original_timing_stats = timing_stats
                quarterly_skill, quarterly_joint, returned_timing_stats = (
                    skill_metrics.calculate_quarterly_skill_metrics(
                        quarterly_obs, quarterly_fc, timing_stats
                    )
                )
                if returned_timing_stats is not None:
                    timing_stats = returned_timing_stats
                else:
                    timing_stats = original_timing_stats

            with timer(timing_stats, "saving quarterly results"):
                logger.info("\n\n------ Saving quarterly results -----------------")
                ret = file_writer.save_quarterly_forecast_data(quarterly_joint)
                if ret is None:
                    logger.info("Quarterly forecast data saved successfully.")
                else:
                    logger.error(f"Error saving quarterly forecast data: {ret}")
                    errors.append(f"Quarterly forecast data save failed: {ret}")

                ret = file_writer.save_quarterly_skill_metrics(
                    quarterly_skill, year=skill_metrics_year
                )
                if ret is None:
                    logger.info("Quarterly skill metrics saved successfully.")
                else:
                    logger.error(f"Error saving quarterly skill metrics: {ret}")
                    errors.append(f"Quarterly skill metrics save failed: {ret}")

        if prediction_mode in ["SEASONAL", "ALL"]:
            current_year = dt.date.today().year
            start_year = int(
                os.getenv(
                    "SAPPHIRE_SKILL_METRICS_START_YEAR",
                    os.getenv("SAPPHIRE_RECALC_START_YEAR", current_year - 20),
                )
            )
            end_year = int(os.getenv("SAPPHIRE_RECALC_END_YEAR", current_year))
            codes = _read_station_codes(PENTAD)

            with timer(timing_stats, "reading seasonal data"):
                logger.info("\n\n------ Reading seasonal data (aggregated from monthly) -------")
                seasonal_obs = data_reader.read_seasonal_observations(codes, start_year, end_year)
                seasonal_frames = []
                for issue_lead in _supported_seasonal_issue_leads():
                    seasonal_fc_for_lead = data_reader.read_seasonal_forecasts(
                        codes,
                        start_year,
                        end_year,
                        horizon_value=issue_lead,
                    )
                    if not seasonal_fc_for_lead.empty:
                        seasonal_frames.append(seasonal_fc_for_lead)
                seasonal_fc = (
                    pd.concat(seasonal_frames, ignore_index=True)
                    if seasonal_frames
                    else pd.DataFrame()
                )

            with timer(timing_stats, "calculating skill metrics seasonal"):
                logger.info("\n\n------ Calculating skill metrics seasonal ------")
                original_timing_stats = timing_stats
                seasonal_skill, seasonal_joint, returned_timing_stats = (
                    skill_metrics.calculate_seasonal_skill_metrics(
                        seasonal_obs, seasonal_fc, timing_stats
                    )
                )
                if returned_timing_stats is not None:
                    timing_stats = returned_timing_stats
                else:
                    timing_stats = original_timing_stats

            with timer(timing_stats, "saving seasonal results"):
                logger.info("\n\n------ Saving seasonal results -----------------")
                ret = file_writer.save_seasonal_forecast_data(seasonal_joint)
                if ret is None:
                    logger.info("Seasonal forecast data saved successfully.")
                else:
                    logger.error(f"Error saving seasonal forecast data: {ret}")
                    errors.append(f"Seasonal forecast data save failed: {ret}")

                ret = file_writer.save_seasonal_skill_metrics(
                    seasonal_skill, year=skill_metrics_year
                )
                if ret is None:
                    logger.info("Seasonal skill metrics saved successfully.")
                else:
                    logger.error(f"Error saving seasonal skill metrics: {ret}")
                    errors.append(f"Seasonal skill metrics save failed: {ret}")

        if prediction_mode in ["DAILY", "ALL"]:
            current_year = dt.date.today().year
            start_year = int(
                os.getenv(
                    "SAPPHIRE_SKILL_METRICS_START_YEAR",
                    os.getenv("SAPPHIRE_RECALC_START_YEAR", current_year - 20),
                )
            )
            end_year = int(os.getenv("SAPPHIRE_RECALC_END_YEAR", current_year))
            codes = _read_station_codes(PENTAD)

            with timer(timing_stats, "reading daily data"):
                logger.info("\n\n------ Reading daily observed and forecast data -------")
                daily_obs = data_reader.read_daily_observations(codes, start_year, end_year)
                daily_fc = data_reader.read_daily_forecasts(codes, start_year, end_year)

            with timer(timing_stats, "calculating daily skill metrics"):
                logger.info("\n\n------ Calculating daily skill metrics ---------")
                try:
                    fdc_metrics, threshold_metrics = skill_metrics.calculate_daily_skill_metrics(
                        daily_obs, daily_fc
                    )
                    logger.info(
                        "Daily metrics: %d FDC rows, %d threshold rows",
                        len(fdc_metrics),
                        len(threshold_metrics),
                    )
                except Exception as e:
                    logger.error("Failed to calculate daily skill metrics: %s", e)
                    errors.append(f"Daily skill metrics calculation failed: {e}")
                    fdc_metrics = None
                    threshold_metrics = None

            with timer(timing_stats, "saving daily results"):
                logger.info("\n\n------ Saving daily results --------------------")
                try:
                    file_writer.save_daily_skill_metrics(
                        fdc_metrics,
                        threshold_metrics,
                        year=skill_metrics_year,
                    )
                    logger.info("Daily skill metrics saved successfully.")
                except Exception as e:
                    logger.error("Error saving daily skill metrics: %s", e)
                    errors.append(f"Daily skill metrics save failed: {e}")

    # Print timing summary
    summary, total = timing_stats.summary()
    logger.info("\n\n")
    logger.info("Timing summary for recalculate_skill_metrics:")
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
    recalculate_skill_metrics()
