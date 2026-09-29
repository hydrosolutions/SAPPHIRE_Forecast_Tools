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
from long_term_horizon_resolver import (
    seasonal_config_name,
    seasonal_horizon_value,
    supported_long_term_modes,
)
from skill_lead_aware_flag import skill_lead_aware_enabled
from src import data_reader, ensemble_calculator, file_writer, gap_detector
from src import postprocessing_tools as pt
from src.model_names import (
    QUARTER_NATIVE_RAW_MODELS,
    QUARTERLY_DERIVED_MODELS,
    canonical_model_short_series,
)
from src.postprocessing_tools import TimingStats, timer

# region Logging
logging.basicConfig(level=logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

if not os.path.exists("logs"):
    os.makedirs("logs")

file_handler = TimedRotatingFileHandler(
    "logs/log_maintenance_long_term",
    when="midnight",
    interval=1,
    backupCount=30,
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


def _read_station_codes():
    """Read station codes from the station selection config file."""
    config_path = os.path.join(
        os.getenv("ieasyforecast_configuration_path", ""),
        os.getenv("ieasyforecast_config_file_station_selection", ""),
    )
    with open(config_path) as f:
        config = json.load(f)
    codes = [str(c) for c in config.get("stationsID", [])]
    logger.info("Read %d station codes", len(codes))
    return codes


def _run_monthly_gap_fill(
    codes: list[str],
    lookback: int,
    errors: list[str],
) -> tuple[bool, pd.DataFrame]:
    """Run the monthly ensemble gap-fill pipeline.

    Reads monthly combined forecasts, detects missing ensemble rows,
    creates them from pre-calculated skill metrics, merges them into
    the existing combined forecasts, and saves the result.

    Args:
        codes: Station codes to process.
        lookback: Monthly gap-fill lookback window, in months.
        errors: Mutable list of error messages accumulated by the
            caller; a save failure appends to it in place.

    Returns:
        A tuple ``(completed, gaps)``. ``completed`` is True only if
        the pipeline ran through to its final save step; False if it
        stopped early because there was nothing to do (empty combined
        forecasts, no gaps, no skill metrics, no forecast data for the
        gap years, no forecast rows matching the gap tuples, or no new
        ensemble rows created). ``gaps`` is the detected monthly gaps
        DataFrame once step 2 has run (used by the caller's
        audit-trail logging when ``completed`` is True), and an empty
        DataFrame if step 2 never ran.
    """
    # 1. Read monthly combined forecasts for gap detection
    with timer(timing_stats, "reading monthly combined forecasts"):
        logger.info("\n\n------ Reading monthly combined forecasts ---------")
        combined = data_reader.read_monthly_combined_forecasts(codes=codes)

    if combined.empty:
        logger.info("No monthly combined forecasts found. Skipping gap detection.")
        return False, pd.DataFrame()

    # 2. Detect missing ensemble rows
    with timer(timing_stats, "detecting monthly gaps"):
        gaps = gap_detector.detect_missing_monthly_ensembles(
            combined,
            lookback,
            ensemble_models={"EM", "Skilled Mean", "Naive Mean"},
        )

    if gaps.empty:
        logger.info("No monthly ensemble gaps found. Nothing to fill.")
        return False, gaps

    logger.info(
        "Found %d (year, month, code, model_short) gaps needing gap-fill",
        len(gaps),
    )

    # 3. Read skill metrics
    with timer(timing_stats, "reading monthly skill metrics"):
        logger.info("\n\n------ Reading pre-calculated monthly skill metrics -----")
        skill_stats = data_reader.read_skill_metrics("month", codes=codes)

    if skill_stats.empty:
        logger.warning("No monthly skill metrics available. Cannot create ensembles.")
        return False, gaps

    # 4. Read forecasts for gap periods from API
    gap_years = gaps["year"].unique()
    start_year = int(gap_years.min())
    end_year = int(gap_years.max())

    with timer(timing_stats, "reading monthly forecasts for gaps"):
        logger.info("\n\n------ Reading monthly forecasts for gap-fill ----")
        all_forecasts = data_reader.read_monthly_forecasts(
            codes,
            start_year,
            end_year,
        )

    if all_forecasts.empty:
        logger.warning("No monthly forecast data available for gap years. Cannot fill gaps.")
        return False, gaps

    # Filter to gap (year, month, code) tuples (deduplicated,
    # since gaps may have multiple model_short per triple)
    gap_set = set(
        gaps[["year", "month", "code"]].drop_duplicates().itertuples(index=False, name=None)
    )
    # Ensure year/month are numeric for comparison
    all_forecasts["year"] = pd.to_numeric(all_forecasts["year"], errors="coerce").astype("Int64")
    all_forecasts["month"] = pd.to_numeric(all_forecasts["month"], errors="coerce").astype("Int64")

    filtered = all_forecasts[
        all_forecasts.apply(
            lambda r: (r["year"], r["month"], str(r["code"])) in gap_set,
            axis=1,
        )
    ].copy()

    if filtered.empty:
        logger.warning("No forecast data matches gap tuples. Cannot fill gaps.")
        return False, gaps

    # Ensure month_in_year and forecasted_discharge exist
    if "month_in_year" not in filtered.columns and "month" in filtered.columns:
        filtered["month_in_year"] = filtered["month"]
    if "forecasted_discharge" not in filtered.columns and "q50" in filtered.columns:
        filtered["forecasted_discharge"] = filtered["q50"].astype(float)

    # 5. Create ensemble forecasts for gap periods
    with timer(timing_stats, "creating monthly gap-fill ensembles"):
        logger.info("\n\n------ Creating monthly ensemble forecasts for gaps ---")
        joint = ensemble_calculator.create_monthly_ensemble_forecasts(
            filtered,
            skill_stats,
        )

    # Extract only ensemble rows that match actual gap tuples.
    # create_monthly_ensemble_forecasts creates all 3 types for
    # every period, but we only want those that were actually
    # missing (per the gap detector).
    ensemble_models = {"EM", "Skilled Mean", "Naive Mean"}
    new_ensemble = joint[joint["model_short"].isin(ensemble_models)].copy()
    # Under SAPPHIRE_SKILL_LEAD_AWARE, key the gap filter per-lead --
    # mirroring the quarterly block's q_gap_keys -- so a regenerated
    # ensemble row for a lead that was NEVER a gap does not survive the
    # filter (and, via keep="last" in the merge dedup, silently overwrite
    # an existing non-gap ensemble at that lead). The gap detector emits
    # per-lead gaps under the flag and create_monthly_ensemble_forecasts
    # stamps each ensemble row with its own horizon_value, so restricting
    # to the actual missing (year, month, code, model, lead) keys also
    # guarantees each gap-filled row keeps the lead it was detected
    # missing at. Flag OFF: unchanged (period-only key, no lead).
    if (
        skill_lead_aware_enabled()
        and "horizon_value" in gaps.columns
        and "horizon_value" in new_ensemble.columns
    ):
        gap_key_cols = ["year", "month", "code", "model_short", "horizon_value"]
        gap_keys = set(
            gaps[gap_key_cols]
            .assign(code=lambda d: d["code"].astype(str))
            .itertuples(index=False, name=None)
        )
        new_ensemble = new_ensemble[
            new_ensemble.apply(
                lambda r, _keys=gap_keys, _cols=gap_key_cols: (
                    tuple(str(r[c]) if c == "code" else r[c] for c in _cols) in _keys
                ),
                axis=1,
            )
        ]
    else:
        gap_keys = set(
            gaps[["year", "month", "code", "model_short"]].itertuples(index=False, name=None)
        )
        new_ensemble = new_ensemble[
            new_ensemble.apply(
                lambda r: (
                    (
                        r["year"],
                        r["month"],
                        str(r["code"]),
                        r["model_short"],
                    )
                    in gap_keys
                ),
                axis=1,
            )
        ]

    if new_ensemble.empty:
        logger.info("No new monthly ensemble rows created. Nothing to save.")
        return False, gaps

    # 6. Merge into existing combined forecasts
    merged = pd.concat(
        [combined, new_ensemble],
        ignore_index=True,
    )
    # Deduplicate on (year, month, code, model_short, horizon_value)
    dedup_cols = ["year", "month", "code", "model_short", "horizon_value"]
    available_dedup = [c for c in dedup_cols if c in merged.columns]
    merged = merged.drop_duplicates(
        subset=available_dedup,
        keep="last",
    )

    logger.info(
        "Merged %d new ensemble rows into %d existing rows -> %d total",
        len(new_ensemble),
        len(combined),
        len(merged),
    )

    # 7. Save
    with timer(timing_stats, "saving monthly gap-fill results"):
        logger.info("\n\n------ Saving monthly gap-fill results -----------")
        ret = file_writer.save_monthly_forecast_data(merged)
        if ret is None:
            logger.info("Monthly gap-fill results saved successfully.")
        else:
            logger.error(f"Error saving monthly gap-fill results: {ret}")
            errors.append(f"Monthly gap-fill save failed: {ret}")

    pt.log_most_recent_forecasts_monthly(merged)

    return True, gaps


# PP-065 P1b Finding 1 fix (out-of-loop review): every raw quarter
# model can contribute to a Naive Mean, not only the two native LR
# ones -- ensemble_calculator's actual "two or more non-null raw
# contributors" gate has no LR-specific restriction. Reuse the
# existing model-set constants rather than hand-rolling a new one.
_QUARTER_RAW_MODELS = QUARTER_NATIVE_RAW_MODELS | QUARTERLY_DERIVED_MODELS


def _filter_quarterly_gap_universe(universe: pd.DataFrame) -> pd.DataFrame:
    """Drop quarterly gap-universe keys with fewer than two raw models.

    Quarter no longer produces an EM row (PP-065 P1b item 3), so gap
    detection keys on Naive Mean instead. A key needs at least two
    distinct raw quarter models present somewhere in the universe to
    ever form a Naive Mean ensemble; a single-model key would
    otherwise surface as a perpetual, unfillable gap.

    "Raw quarter models" is every model in ``_QUARTER_RAW_MODELS``
    (PP-065 P1b Finding 1 fix): the two native LR models
    (``QUARTER_NATIVE_RAW_MODELS``) plus the seven models re-enabled
    for quarter as same-issue monthly derivations
    (``QUARTERLY_DERIVED_MODELS``) -- any two of these nine can form a
    Naive Mean, not only the two LR ones. Model names are compared via
    ``canonical_model_short_series`` because ``model_short`` values
    flowing through this function are in their API display-case form
    (e.g. "LR_Base", "SM_GBT_Norm"), not the canonical upper-snake form
    the constants are defined in.

    Under ``SAPPHIRE_SKILL_LEAD_AWARE`` (PP-065 P1b Finding 2 fix), the
    key additionally includes ``horizon_value``: Naive Mean formation
    happens per (code, year, quarter, horizon_value) under the flag --
    mirroring the gap-key-matching logic further down in the caller --
    so two single-model rows at DIFFERENT leads must never be counted
    together as "2 distinct models present" for one key.

    Args:
        universe: Concatenated quarterly combined-forecast rows and
            raw per-model quarterly forecast rows (see caller).

    Returns:
        The subset of ``universe`` whose key has at least two distinct
        raw models present. Empty on empty input, or on input missing
        a required column.
    """
    key_cols = ["year", "quarter_in_year", "code"]
    if skill_lead_aware_enabled() and "horizon_value" in universe.columns:
        key_cols.append("horizon_value")
    required = {*key_cols, "model_short"}
    if universe.empty or not required.issubset(universe.columns):
        return universe.iloc[0:0]

    df = universe.copy()
    df["code"] = df["code"].astype(str)
    canon_model = canonical_model_short_series(df["model_short"])
    raw_rows = df[canon_model.isin(_QUARTER_RAW_MODELS)].copy()
    if raw_rows.empty:
        return df.iloc[0:0]
    raw_rows["_pp065_canon_model"] = canon_model.loc[raw_rows.index]

    distinct_counts = raw_rows.groupby(key_cols)["_pp065_canon_model"].nunique()
    eligible_keys = set(distinct_counts[distinct_counts >= 2].index)
    if not eligible_keys:
        return df.iloc[0:0]

    mask = df.apply(
        lambda r, _keys=eligible_keys, _cols=key_cols: (tuple(r[c] for c in _cols) in _keys),
        axis=1,
    )
    return df[mask].copy()


def postprocessing_maintenance_long_term():
    global timing_stats

    logger.info("\n\n====== Post-processing forecasts (MAINTENANCE / GAP-FILL LONG-TERM) =====")
    logger.debug(f"Script started at {dt.datetime.now()}.")

    errors = []
    lookback = int(os.getenv("POSTPROCESSING_GAPFILL_WINDOW_MONTHS", "3"))
    logger.info(f"Monthly gap-fill lookback window: {lookback} months")

    # The Forecast Date Rule: capture once at the entry point, pass as
    # a parameter to anything that needs it (the quarterly gap-universe
    # read below).
    forecast_date = dt.date.today()

    with timer(timing_stats, "total execution"):
        with timer(timing_stats, "setup"):
            logger.info("\n\n------ Setting up --------------------------------")
            sl.load_environment()
            codes = _read_station_codes()

        monthly_completed, gaps = _run_monthly_gap_fill(codes, lookback, errors)

        # ----- QUARTERLY GAP-FILL -----
        with timer(timing_stats, "quarterly gap-fill"):
            logger.info("\n\n------ Quarterly gap-fill -------------------------")
            lookback_q = int(os.getenv("POSTPROCESSING_GAPFILL_WINDOW_QUARTERS", "2"))
            q_combined = data_reader.read_quarterly_combined_forecasts(codes=codes)
            # Gap universe (PP-065 P1b item 4): quarter no longer produces
            # EM (see _filter_quarterly_gap_universe), so gap detection
            # below keys on Naive Mean instead of EM. Concatenate the raw
            # per-model rows from read_quarterly_forecasts (forecast_date's
            # year, +/-1 -- the +1 covers a December-issued Q1) with
            # q_combined, then drop keys with fewer than two distinct raw
            # models before ever calling the gap detector: a single-model
            # key can never form a Naive Mean and would otherwise surface
            # as a perpetual, unfillable gap.
            q_universe_year = forecast_date.year
            q_universe_raw = data_reader.read_quarterly_forecasts(
                codes,
                q_universe_year - 1,
                q_universe_year + 1,
            )
            q_universe = _filter_quarterly_gap_universe(
                pd.concat([q_combined, q_universe_raw], ignore_index=True)
            )
            if not q_universe.empty:
                q_gaps = gap_detector.detect_missing_quarterly_ensembles(
                    q_universe,
                    lookback_q,
                    ensemble_models={"Naive Mean"},
                )
                if not q_gaps.empty:
                    q_skill = data_reader.read_skill_metrics("quarter", codes=codes)
                    if not q_skill.empty:
                        q_years = q_gaps["year"].unique()
                        q_fc = data_reader.read_quarterly_forecasts(
                            codes,
                            int(q_years.min()),
                            int(q_years.max()),
                        )
                        if not q_fc.empty:
                            q_joint = ensemble_calculator.create_quarterly_ensemble_forecasts(
                                q_fc,
                                q_skill,
                            )
                            q_ens_models = {
                                "EM",
                                "Skilled Mean",
                                "Naive Mean",
                            }
                            q_new = q_joint[q_joint["model_short"].isin(q_ens_models)].copy()
                            # Under SAPPHIRE_SKILL_LEAD_AWARE, restrict the
                            # freshly-generated ensemble rows to the ACTUAL
                            # missing gap keys before merge -- mirroring the
                            # seasonal block's s_gap_keys filter -- so a
                            # non-gap (year, quarter, code[, lead]) ensemble
                            # row already present in q_combined is not
                            # silently overwritten (keep="last") by a
                            # regenerated row for a lead that was never a
                            # gap. model_short is deliberately left OUT of
                            # this key (unlike the monthly/seasonal
                            # equivalents): gap detection above only checks
                            # Naive Mean (quarter no longer produces EM), so
                            # a Naive Mean gap must admit BOTH freshly
                            # formed ensembles -- Naive Mean and Skilled
                            # Mean -- for that (code, year, quarter[, lead])
                            # key, not just the one model the gap detector
                            # reported. Flag OFF: unchanged (no q_new
                            # filtering).
                            if skill_lead_aware_enabled() and not q_new.empty:
                                q_key_cols = [
                                    "year",
                                    "quarter_in_year",
                                    "code",
                                ]
                                if (
                                    "horizon_value" in q_gaps.columns
                                    and "horizon_value" in q_new.columns
                                ):
                                    q_key_cols.append("horizon_value")
                                q_gap_keys = set(
                                    q_gaps[q_key_cols]
                                    .assign(code=lambda d: d["code"].astype(str))
                                    .itertuples(index=False, name=None)
                                )
                                q_new = q_new[
                                    q_new.apply(
                                        lambda r, _keys=q_gap_keys, _cols=q_key_cols: (
                                            tuple(str(r[c]) if c == "code" else r[c] for c in _cols)
                                            in _keys
                                        ),
                                        axis=1,
                                    )
                                ]
                            # PP-065 P1b Finding 4 fix (out-of-loop review):
                            # model_short is intentionally OUT of the key
                            # above so a Naive-Mean-only gap admits a
                            # freshly regenerated Skilled Mean too (see
                            # comment above). But
                            # create_quarterly_ensemble_forecasts recomputes
                            # BOTH ensembles together whenever it has enough
                            # data, regardless of whether Skilled Mean was
                            # actually the thing missing -- so a fresh
                            # Skilled Mean row for a key that ALREADY has a
                            # correct, persisted Skilled Mean in q_combined
                            # would otherwise pass the filter above and
                            # silently WIN the keep="last" dedup below,
                            # overwriting a value that was never a gap
                            # (possibly with a different value, e.g. if
                            # skill membership shifted since it was last
                            # computed). Drop such a row: only let a fresh
                            # Skilled Mean through when there is no
                            # pre-existing Skilled Mean at that key to
                            # overwrite, or when Skilled Mean was ITSELF
                            # reported missing at that key (gap detection
                            # currently only checks Naive Mean -- see the
                            # ensemble_models={"Naive Mean"} call above --
                            # so the "itself reported missing" branch is a
                            # no-op today, but keeps this correct if that
                            # ever changes).
                            if not q_new.empty and (q_new["model_short"] == "Skilled Mean").any():
                                # PP-065 P1b Finding B fix (fix round 2,
                                # confirm-fixes review): the key used to
                                # decide "does an existing Skilled Mean
                                # already cover this key" must not
                                # silently collapse to (year, quarter,
                                # code) -- ignoring lead entirely --
                                # just because q_combined happens to lack
                                # a `horizon_value` column (e.g. it holds
                                # only a legacy Skilled Mean row written
                                # before this reader/writer contract, or
                                # before the flag was ever turned on for
                                # this org). Requiring the column on BOTH
                                # frames (the round-1 condition) let a
                                # legacy no-lead row match ANY lead,
                                # incorrectly suppressing a genuinely new,
                                # correctly-gapped Skilled Mean for a
                                # DIFFERENT lead than the legacy row's
                                # (unknown) one.
                                #
                                # Fix: key on `horizon_value` whenever the
                                # flag is on and q_new (the freshly
                                # generated candidates) carries it --
                                # regardless of whether q_combined has the
                                # column. `_sm_keys` below already treats
                                # a frame missing a key column as
                                # contributing no keys at all (its
                                # `issubset` guard), so when q_combined
                                # lacks `horizon_value` entirely,
                                # `existing_sm_keys` comes back empty and
                                # no row is treated as "already covered"
                                # -- the safer default (option (a) in the
                                # review: an existing row with an unknown
                                # lead is never a match for a specific-
                                # lead new row, since ambiguous coverage
                                # should not block a real, detected gap).
                                # A present-but-individually-null
                                # `horizon_value` in q_combined already
                                # behaves this way without any extra
                                # code: the tuple comparison below never
                                # matches NaN against a real lead value,
                                # so it was never part of this defect.
                                # The original finding-4 exact-match
                                # protection (a genuinely non-gapped
                                # Skilled Mean at the SAME known lead)
                                # is unchanged: when both frames carry a
                                # real, equal `horizon_value`, the key
                                # still matches and the row is still
                                # dropped.
                                sm_key_cols = ["year", "quarter_in_year", "code"]
                                if skill_lead_aware_enabled() and "horizon_value" in q_new.columns:
                                    sm_key_cols.append("horizon_value")

                                def _sm_keys(frame, _cols=sm_key_cols):
                                    if frame.empty or not {"model_short", *_cols}.issubset(
                                        frame.columns
                                    ):
                                        return set()
                                    sm_rows = frame[frame["model_short"] == "Skilled Mean"]
                                    if sm_rows.empty:
                                        return set()
                                    return set(
                                        sm_rows[_cols]
                                        .assign(code=lambda d: d["code"].astype(str))
                                        .itertuples(index=False, name=None)
                                    )

                                existing_sm_keys = _sm_keys(q_combined)
                                gapped_sm_keys = _sm_keys(q_gaps)

                                def _sm_row_key(r, _cols=sm_key_cols):
                                    return tuple(str(r[c]) if c == "code" else r[c] for c in _cols)

                                overwrite_mask = q_new.apply(
                                    lambda r: (
                                        r["model_short"] == "Skilled Mean"
                                        and _sm_row_key(r) in existing_sm_keys
                                        and _sm_row_key(r) not in gapped_sm_keys
                                    ),
                                    axis=1,
                                )
                                if overwrite_mask.any():
                                    q_new = q_new[~overwrite_mask].copy()
                            q_merged = pd.concat(
                                [q_combined, q_new],
                                ignore_index=True,
                            )
                            q_dedup_subset = [
                                "year",
                                "quarter_in_year",
                                "code",
                                "model_short",
                            ]
                            # Under SAPPHIRE_SKILL_LEAD_AWARE, keep distinct
                            # horizon_value leads as separate rows instead of
                            # collapsing them. Flag OFF: unchanged.
                            if skill_lead_aware_enabled() and "horizon_value" in q_merged.columns:
                                q_dedup_subset.append("horizon_value")
                            q_merged = q_merged.drop_duplicates(
                                subset=q_dedup_subset,
                                keep="last",
                            )
                            file_writer.save_quarterly_forecast_data(q_merged)
                            logger.info(
                                "Quarterly gap-fill: %d gaps filled.",
                                len(q_gaps),
                            )
                        else:
                            logger.info("No quarterly forecasts for gap years.")
                    else:
                        logger.info("No quarterly skill metrics for gap-fill.")
                else:
                    logger.info("No quarterly gaps found.")
            else:
                logger.info(
                    "No quarterly gap universe (no data, or no key has both "
                    "raw quarter models). Skipping quarterly gap-fill."
                )

        # The monthly block did not complete (one of its six early-exit
        # conditions in _run_monthly_gap_fill fired). Quarterly gap-fill
        # still ran above; seasonal gap-fill and the audit-trail section
        # below (which reports on the monthly `gaps`) are reached only
        # when the monthly block actually completed its fill, so exit
        # here rather than falling through to them.
        if not monthly_completed:
            _print_timing()
            sys.exit(0)

        # ----- SEASONAL GAP-FILL -----
        with timer(timing_stats, "seasonal gap-fill"):
            logger.info("\n\n------ Seasonal gap-fill --------------------------")
            lookback_s = int(os.getenv("POSTPROCESSING_GAPFILL_WINDOW_SEASONS", "1"))
            seasonal_issue_leads = _supported_seasonal_issue_leads()
            s_combined_frames = []
            for issue_lead in seasonal_issue_leads:
                combined_for_lead = data_reader.read_seasonal_combined_forecasts(
                    codes=codes,
                    horizon_value=issue_lead,
                )
                if not combined_for_lead.empty:
                    s_combined_frames.append(combined_for_lead)
            s_combined = (
                pd.concat(s_combined_frames, ignore_index=True)
                if s_combined_frames
                else pd.DataFrame()
            )
            if not s_combined.empty:
                s_gaps = gap_detector.detect_missing_seasonal_ensembles(
                    s_combined,
                    lookback_s,
                    ensemble_models={"EM", "Skilled Mean", "Naive Mean"},
                )
                if not s_gaps.empty:
                    s_skill = data_reader.read_skill_metrics("season", codes=codes)
                    if not s_skill.empty:
                        s_fc_frames = []
                        for issue_lead, lead_gaps in s_gaps.groupby("season_in_year"):
                            s_years = lead_gaps["season_year"].unique()
                            fc_for_lead = data_reader.read_seasonal_forecasts(
                                codes,
                                int(s_years.min()),
                                int(s_years.max()),
                                horizon_value=int(issue_lead),
                            )
                            if fc_for_lead.empty:
                                continue

                            lead_gap_set = set(
                                lead_gaps[["season_year", "season_in_year", "code"]]
                                .drop_duplicates()
                                .itertuples(index=False, name=None)
                            )
                            fc_for_lead = fc_for_lead[
                                fc_for_lead.apply(
                                    lambda r, _gap_set=lead_gap_set: (
                                        (
                                            r["season_year"],
                                            r["season_in_year"],
                                            str(r["code"]),
                                        )
                                        in _gap_set
                                    ),
                                    axis=1,
                                )
                            ].copy()
                            if not fc_for_lead.empty:
                                s_fc_frames.append(fc_for_lead)
                        s_fc = (
                            pd.concat(s_fc_frames, ignore_index=True)
                            if s_fc_frames
                            else pd.DataFrame()
                        )
                        if not s_fc.empty:
                            s_joint = ensemble_calculator.create_seasonal_ensemble_forecasts(
                                s_fc,
                                s_skill,
                            )
                            s_ens_models = {
                                "EM",
                                "Skilled Mean",
                                "Naive Mean",
                            }
                            s_new = s_joint[s_joint["model_short"].isin(s_ens_models)].copy()
                            s_gap_keys = set(
                                s_gaps[
                                    [
                                        "season_year",
                                        "season_in_year",
                                        "code",
                                        "model_short",
                                    ]
                                ].itertuples(index=False, name=None)
                            )
                            s_new = s_new[
                                s_new.apply(
                                    lambda r: (
                                        (
                                            r["season_year"],
                                            r["season_in_year"],
                                            str(r["code"]),
                                            r["model_short"],
                                        )
                                        in s_gap_keys
                                    ),
                                    axis=1,
                                )
                            ]
                            s_merged = pd.concat(
                                [s_combined, s_new],
                                ignore_index=True,
                            )
                            s_dedup = [
                                "season_year",
                                "season_in_year",
                                "code",
                                "model_short",
                            ]
                            s_merged = s_merged.drop_duplicates(
                                subset=s_dedup,
                                keep="last",
                            )
                            file_writer.save_seasonal_forecast_data(s_merged)
                            logger.info(
                                "Seasonal gap-fill: %d gaps filled.",
                                len(s_gaps),
                            )
                        else:
                            logger.info("No seasonal forecasts for gap years.")
                    else:
                        logger.info("No seasonal skill metrics for gap-fill.")
                else:
                    logger.info("No seasonal gaps found.")
            else:
                logger.info("No seasonal combined data. Skipping seasonal gap-fill.")

        # Audit trail — deduplicate to (year, month, code) level
        unique_gaps = gaps[["year", "month", "code"]].drop_duplicates()
        logger.info(
            "AUDIT: Filled %d monthly ensemble gaps (%d unique periods, lookback=%d months)",
            len(gaps),
            len(unique_gaps),
            lookback,
        )
        for _, gap_row in unique_gaps.iterrows():
            logger.info(
                "  Filled: year=%d, month=%d, code=%s",
                gap_row["year"],
                gap_row["month"],
                gap_row["code"],
            )

    _print_timing()

    if errors:
        logger.error(f"Script finished with {len(errors)} error(s):")
        for error in errors:
            logger.error(f"  - {error}")
        sys.exit(1)
    else:
        logger.info(f"Script finished successfully at {dt.datetime.now()}.")
        sys.exit(0)


def _print_timing():
    """Print timing summary."""
    summary, total = timing_stats.summary()
    logger.info("\n\n")
    logger.info("Timing summary for postprocessing_maintenance_long_term:")
    logger.info(f"Total execution time: {total:.2f} seconds")
    logger.info("Breakdown by section:")
    for entry in summary:
        logger.info(f"{entry['section']}:")
        logger.info(f"  Total time: {entry['total_time']:.2f} seconds ({entry['percentage']:.1f}%)")
        logger.info(f"  Average time per call: {entry['avg_time']:.2f} seconds")
        logger.info(f"  Number of calls: {entry['calls']}")


if __name__ == "__main__":
    postprocessing_maintenance_long_term()
