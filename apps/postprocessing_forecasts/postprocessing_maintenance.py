# postprocessing_maintenance.py
# Nightly gap-fill entry point: detects missing ensemble forecasts
# within a lookback window, creates them from pre-calculated skill metrics,
# and saves the results.
#
# Usage:
#   ieasyhydroforecast_env_file_path=/path/to/.env \
#   SAPPHIRE_PREDICTION_MODE=BOTH \
#   POSTPROCESSING_GAPFILL_MAX_MONTHS=13 \
#   python postprocessing_maintenance.py

import datetime as dt
import logging
import os
import sys
import warnings
from logging.handlers import TimedRotatingFileHandler

import pandas as pd

# Local libraries
script_dir = os.path.dirname(os.path.abspath(__file__))
forecast_dir = os.path.join(script_dir, "..", "iEasyHydroForecast")
sys.path.append(forecast_dir)

import setup_library as sl
import tag_library as tl
from src import api_writer, data_reader, ensemble_calculator, file_writer, gap_detector
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

    # Deprecation: warn if the old env var is still set
    if os.getenv("POSTPROCESSING_GAPFILL_WINDOW_DAYS"):
        logger.warning(
            "POSTPROCESSING_GAPFILL_WINDOW_DAYS is deprecated. "
            "Use POSTPROCESSING_GAPFILL_MAX_MONTHS instead."
        )

    max_lookback_months = int(os.getenv("POSTPROCESSING_GAPFILL_MAX_MONTHS", "13"))
    logger.info(f"Gap-fill lookback window: {max_lookback_months} months")

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
            _fill_gaps_for_horizon(PENTAD, max_lookback_months, errors)

        if prediction_mode in ["DECAD", "BOTH"]:
            _fill_gaps_for_horizon(DECAD, max_lookback_months, errors)

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


def _fill_gaps_for_horizon(config, max_lookback_months, errors):
    """Detect and fill ensemble gaps for one horizon type.

    New flow (PP-021):
    1. Read combined forecasts (cheap).
    2. Detect EM gaps using combined only — no expensive modelled read.
    3. Detect stale individual-model / NE records (q05 IS NULL).
    4. Detect stale EM records (q05 IS NULL for EM rows).
    5. Early exit if nothing to do.
    6. Read modelled data scoped to affected dates only.
    7. Create NE + EM rows with quantiles.
    8. Merge and save.
    """
    global timing_stats

    label = config.name.upper()

    # Step 1: read what we already have (cheap)
    with timer(timing_stats, f"reading {label} combined forecasts"):
        logger.info(f"\n\n------ Reading {label} combined forecasts for gap detection ----")
        combined = data_reader.read_combined_forecasts(config.name)

    if combined.empty:
        logger.info(f"No {label} combined data found. Skipping gap detection.")
        return

    # Step 2–4: detect gaps and stale records (all cheap, in-memory)
    with timer(timing_stats, f"detecting {label} gaps"):
        # EM + NE gaps (missing rows)
        all_gaps = gap_detector.detect_missing_ensembles(
            combined,
            max_lookback_months=max_lookback_months,
            ensemble_models={"EM", "NE"},
            horizon_type=config.name,
        )
        ne_gaps = all_gaps[all_gaps["model_short"] == "NE"]
        if not ne_gaps.empty:
            logger.info(
                "Found %d NE gaps within lookback window. "
                "NE rows will be re-created from individual-model data.",
                len(ne_gaps),
            )
        em_gaps = all_gaps[all_gaps["model_short"] == "EM"].reset_index(drop=True)

        # Stale individual-model / NE rows (have discharge, no quantiles)
        stale = gap_detector.detect_stale_quantiles(
            combined,
            max_lookback_months=max_lookback_months,
            horizon_type=config.name,
        )

        # Stale EM rows (have discharge, no quantiles)
        stale_em = pd.DataFrame(columns=["date", "code"])
        if "q05" in combined.columns:
            stale_em = (
                combined[
                    (combined["model_short"] == "EM")
                    & combined["forecasted_discharge"].notna()
                    & combined["q05"].isna()
                ][["date", "code"]]
                .drop_duplicates()
                .reset_index(drop=True)
            )

    # Step 5: early exit if nothing to do
    em_gap_dates = set(pd.to_datetime(em_gaps["date"]).unique()) if not em_gaps.empty else set()
    stale_dates = set(pd.to_datetime(stale["date"]).unique()) if not stale.empty else set()
    stale_em_dates = set(pd.to_datetime(stale_em["date"]).unique()) if not stale_em.empty else set()
    ne_gap_dates = set(pd.to_datetime(ne_gaps["date"]).unique()) if not ne_gaps.empty else set()
    all_affected = em_gap_dates | stale_dates | stale_em_dates | ne_gap_dates

    logger.info(
        "%s: %d EM gaps, %d NE gaps, %d stale individual/NE records, "
        "%d stale EM records → %d total affected dates",
        label,
        len(em_gaps),
        len(ne_gaps),
        len(stale),
        len(stale_em),
        len(all_affected),
    )

    if not all_affected:
        logger.info(f"No {label} gaps or stale records found. Nothing to do.")
        return

    # Compute gap codes BEFORE data read so we can scope the API query
    gap_codes: set[str] = set()
    for df_check in [em_gaps, ne_gaps, stale, stale_em]:
        if not df_check.empty and "code" in df_check.columns:
            gap_codes.update(df_check["code"].unique())

    # Step 6: read modelled data scoped to affected dates and codes
    affected_dates = sorted(all_affected)
    with timer(timing_stats, f"reading {label} data for gap-fill"):
        logger.info(
            "\n\n------ Reading %s modelled data for %d affected date(s) ----",
            label,
            len(affected_dates),
        )
        modelled, _ = data_reader.read_individual_model_forecasts_for_dates(
            config.name,
            affected_dates,
            codes=list(gap_codes) if gap_codes else None,
        )
        modelled = sl.calculate_virtual_stations_data(modelled)
        modelled = config.neural_ensemble_func(modelled)

    if modelled.empty:
        logger.warning(f"No {label} modelled data for affected dates. Cannot fill gaps.")
        return

    modelled_filtered = modelled[
        modelled["date"].isin(all_affected) & modelled["code"].isin(gap_codes)
    ].copy()

    if modelled_filtered.empty:
        logger.warning(f"No {label} forecast data for affected dates/codes. Cannot fill gaps.")
        return

    with timer(timing_stats, f"reading {label} skill metrics"):
        skill_stats = data_reader.read_skill_metrics(config.name)

    # Step 7: build the set of refreshed rows to merge back into combined.
    # Only include rows that are actually stale/gap-fill — not all of
    # modelled_filtered, which would duplicate non-stale individual rows.
    refresh_parts: list[pd.DataFrame] = []
    stale_keys: set[tuple] = set()

    # 7a: Refreshed stale individual/NE rows (from freshly-read modelled data)
    if not stale.empty:
        stale_keys = set(
            zip(
                pd.to_datetime(stale["date"]).dt.normalize(),
                stale["code"],
                stale["model_short"],
                strict=True,
            )
        )
        stale_mask = modelled_filtered.apply(
            lambda r: (
                pd.Timestamp(r["date"]).normalize(),
                r["code"],
                r["model_short"],
            )
            in stale_keys,
            axis=1,
        )
        refreshed_stale = modelled_filtered[stale_mask]
        if not refreshed_stale.empty:
            refresh_parts.append(refreshed_stale)

    # 7b: New NE rows for NE gaps (created by neural_ensemble_func above).
    # Skip NE rows already captured as stale in 7a to avoid double-counting.
    if not ne_gaps.empty:
        ne_gap_keys = set(
            zip(
                pd.to_datetime(ne_gaps["date"]).dt.normalize(),
                ne_gaps["code"],
                strict=True,
            )
        )
        ne_mask = modelled_filtered.apply(
            lambda r: r["model_short"] == "NE"
            and (pd.Timestamp(r["date"]).normalize(), r["code"]) in ne_gap_keys
            and (
                pd.Timestamp(r["date"]).normalize(),
                r["code"],
                "NE",
            )
            not in stale_keys,
            axis=1,
        )
        new_ne = modelled_filtered[ne_mask]
        if not new_ne.empty:
            refresh_parts.append(new_ne)

    # 7c: EM rows (gap-fill + stale EM refresh) — requires skill metrics
    if skill_stats.empty:
        logger.warning(
            f"No {label} skill metrics available. "
            "Refreshing individual/NE rows but skipping EM creation."
        )
    else:
        with timer(timing_stats, f"creating {label} gap-fill ensembles"):
            ensemble_out, _ = ensemble_calculator.create_ensemble_forecasts(
                forecasts=modelled_filtered,
                skill_stats=skill_stats,
                period_col=config.period_col,
                period_in_month_col=config.period_in_month_col,
                get_period_in_month_func=config.get_period_func,
            )
        new_em = ensemble_out[ensemble_out["model_short"] == "EM"]
        if not new_em.empty:
            refresh_parts.append(new_em)

    if not refresh_parts:
        logger.info(f"No new {label} rows created from gap-fill data. Nothing to save.")
        return

    joint = pd.concat(refresh_parts, ignore_index=True)

    # Step 8: merge refreshed rows into combined.
    # Drop all-NA columns from joint to avoid introducing empty columns
    # into the merge. Suppress the remaining FutureWarning from pandas
    # >= 2.1 about concat dtype inference with all-NA entries — the
    # current behavior is correct for our use case.
    joint = joint.dropna(axis=1, how="all")

    # Write refreshed rows directly to API (bypasses get_latest_forecasts
    # filter so that historical gap-fills reach the database).
    if not joint.empty:
        try:
            ok = api_writer._write_combined_forecast_to_api(joint, config.name)
            if ok:
                logger.info(
                    "%s: submitted %d refreshed rows to API",
                    label,
                    len(joint),
                )
            else:
                logger.warning(
                    "%s: direct API write returned False (API may be unavailable or data filtered)",
                    label,
                )
        except Exception:
            logger.exception(
                "%s: direct API write of refreshed rows failed",
                label,
            )
            errors.append(f"{label} direct API write failed")

    # concat puts combined first, joint last → keep="last" replaces stale entries
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="The behavior of DataFrame concatenation with empty or all-NA",
            category=FutureWarning,
        )
        merged = pd.concat([combined, joint], ignore_index=True)
    merged = merged.drop_duplicates(
        subset=["date", "code", config.period_col, "model_short"], keep="last"
    )

    n_em = (joint["model_short"] == "EM").sum()
    n_ne = (joint["model_short"] == "NE").sum()
    n_individual = len(joint) - n_em - n_ne

    logger.info(
        "Merged %d refreshed rows (%d EM, %d NE, %d individual) into %d existing → %d total",
        len(joint),
        n_em,
        n_ne,
        n_individual,
        len(combined),
        len(merged),
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
    logger.info(
        "AUDIT: %s — filled %d EM gaps, refreshed %d NE, %d individual rows; "
        "%d stale detected (%d NE/individual, %d EM); lookback=%d months",
        label,
        len(em_gaps),
        n_ne,
        n_individual,
        len(stale) + len(stale_em),
        len(stale),
        len(stale_em),
        max_lookback_months,
    )
    for _, gap_row in em_gaps.iterrows():
        logger.info("  Filled EM: date=%s, code=%s", gap_row["date"], gap_row["code"])


if __name__ == "__main__":
    postprocessing_maintenance()
