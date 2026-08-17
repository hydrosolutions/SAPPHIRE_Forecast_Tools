"""PP-045 backfill CLI for stranded short-term PENTAD/DECADE period forecasts.

Per-model PENTAD/DECADE period rows are written on boundary days by the
operational path, which is the only path that writes them *on a schedule*. If an
operational boundary day was missed, routine maintenance cannot heal it. This CLI
re-aggregates a date range through the EXISTING operational aggregation + save
path so the stranded rows are recomputed and written.

What maintenance actually does (and does not)
---------------------------------------------
``postprocessing_maintenance.py`` does NOT recalculate skill metrics -- it *reads*
them; ``recalculate_skill_metrics.py`` recalculates. And it DOES write period
forecasts: its ``refresh_parts`` build emits refreshed stale individual/NE rows
(block 7a), NE gap rows (7b) and EM rows (7c, "requires skill metrics"), then
writes them straight to the API ("bypasses get_latest_forecasts").

Its actual limitation is narrower, and is the reason this CLI exists: maintenance
can only act on dates it can *discover*, and it builds that universe solely from
existing ``combined`` rows. It returns early when combined is empty, and its
``gap_detector.detect_missing_ensembles`` call omits the ``modelled_forecasts``
argument that would widen the universe. A boundary date with ZERO combined rows is
therefore invisible to it -- and even if it were made discoverable, its write set
never emits fresh per-model rows for a newly-discovered date.

Note also that ``recalculate_skill_metrics.py`` re-saves period rows as a side
effect of a skill recalculation, so this CLI is not the only non-operational
writer of them.

What must be true for a date to be recoverable
-----------------------------------------------
This CLI re-runs the ordinary aggregation; it cannot invent inputs. For a given
issue date to produce a row, TWO independent stages must both pass.

1. The merged archive must yield a row for that issue date that survives the
   boundary-day drop AND the in-period ``target`` filter (a daily target counts
   only if ``get_pentad_in_year(target) == get_pentad_in_year(date + 1 day)``).
   The row may come either from the DAY archive or from a retained pre-cutover
   period-archive row -- ``_merge_archives_by_day_cutover`` keeps period rows for
   dates before each (code, model)'s first DAY issue date. A DAY row is therefore
   NOT strictly required, but a retained period row is not exempt from these
   filters either.
2. SEPARATELY, a surviving row reaches the API only if ``forecasted_discharge``
   is non-null -- ``api_writer`` drops null-discharge records before the write.

If a date yields nothing, the gap is upstream and this CLI cannot close it.
``machine_learning/fill_ml_gaps.py`` and ``recalculate_nan_forecasts.py`` are the
usual upstream tools, with one important caveat: ``fill_ml_gaps.py`` detects only
gaps BETWEEN consecutive existing dates, so it cannot see an empty archive, a
leading gap, or a trailing gap. A stale period with no forecasts on either side
may be invisible to it.

Whole-year granularity, ONE YEAR AT A TIME
------------------------------------------
The run iterates one calendar year per call to ``_run_short_term_postprocessing``
(``start_year == end_year``). This is deliberate, not an optimization:
``file_writer.get_latest_forecasts`` dedups on a YEARLESS key
(``[code, period_in_year, model_short]``), so feeding it more than one year at
once would collapse the same period across years into a single row and drop
older years. Processing one year at a time keeps each year's period rows
distinct.

Only the YEARS of --start-date/--end-date matter
-------------------------------------------------
The day and month components are used ONLY for validation. The loop is
``range(start.year, end.year + 1)`` and every selected year is reprocessed IN
FULL, for every configured station. ``--start-date 2026-07-25 --end-date
2026-08-10`` therefore does exactly the same work as ``--start-date 2026-01-01
--end-date 2026-12-31``: it rewrites all of 2026. Sub-year bounds do NOT narrow
the write set, and there is no option that does. Size the blast radius from the
YEARS you pass and from ``--horizon``, never from the dates.

Issue-date vs target (Dec 31 -> Jan 1)
--------------------------------------
``--start-date`` / ``--end-date`` are ISSUE dates, and the loop iterates ISSUE
years (``range(start.year, end.year + 1)``). Short-term forecasts are issued on a
boundary day and target the NEXT period (``forecast_target_date`` maps an issue
date to target = issue + 1 day), so a period whose target STARTS Jan 1 of year Y
is produced by the Dec 31 (year Y-1) ISSUE date and is therefore healed by the
year ``Y-1`` iteration, NOT the year ``Y`` iteration. Consequently, to heal a
target period that begins Jan 1 of year Y, the requested range must include its
issue date — i.e. it must extend back into the PRIOR calendar year (Y-1).

Idempotence
-----------
Individual per-model period rows re-aggregate deterministically and are upserted,
so re-running is safe for them. Ensemble (EM / Skilled Mean) rows are NOT
byte-for-byte idempotent across re-runs because they depend on which member
models and skill metrics are present at run time; a backfill re-run may shift
ensemble values if the underlying inputs changed. This is expected.

API-only by default
-------------------
``--write-csv`` is OFF by default so the backfill writes ONLY to the SAPPHIRE API
and never clobbers the operational combined CSVs (which the daily operational run
owns and which reflect the latest operational state). Pass ``--write-csv`` to also
rewrite the CSV files.

Failure handling
----------------
The run sets ``SAPPHIRE_API_FAILURE_MODE=fail`` so that API-write errors, which
are otherwise swallowed by ``forecast_library._handle_api_write_error``, re-raise
and surface as a non-zero exit code. A failure in one (year, horizon) is recorded
but does not abort the whole run — remaining years/horizons are still attempted so
one bad year is reported without hiding the rest.

Usage:
    ieasyhydroforecast_env_file_path=/path/to/.env \\
        python backfill_period_forecasts.py \\
        --start-date 2024-01-01 --end-date 2026-07-10 --horizon both
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_IEHF_DIR = os.path.join(_SCRIPT_DIR, "..", "iEasyHydroForecast")
if _IEHF_DIR not in sys.path:
    sys.path.insert(0, _IEHF_DIR)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

import setup_library as sl
from postprocessing_operational import (
    DECAD,
    PENTAD,
    _run_short_term_postprocessing,
)
from src.postprocessing_tools import TimingStats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="backfill_period_forecasts.py",
        description=(
            "Backfill stranded short-term PENTAD/DECADE per-model period forecasts "
            "over a date range by re-aggregating through the operational save path, "
            "one calendar year at a time (API-only by default)."
        ),
    )
    parser.add_argument(
        "--start-date",
        required=True,
        metavar="YYYY-MM-DD",
        help=(
            "First ISSUE date of the range to backfill. ONLY ITS YEAR IS USED: "
            "every selected year is reprocessed in full for every configured "
            "station, so a narrow date range does NOT narrow the work. A target "
            "period starting Jan 1 of year Y is healed by its Dec 31 (year Y-1) "
            "issue date, so include the prior calendar year to heal it."
        ),
    )
    parser.add_argument(
        "--end-date",
        required=True,
        metavar="YYYY-MM-DD",
        help=(
            "Last ISSUE date of the range to backfill. ONLY ITS YEAR IS USED -- "
            "the loop iterates whole issue years and reprocesses each in full."
        ),
    )
    parser.add_argument(
        "--horizon",
        required=True,
        choices=["pentad", "decad", "both"],
        help="Which short-term horizon(s) to backfill.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Log the coverage that would be written without saving anything.",
    )
    parser.add_argument(
        "--write-csv",
        action="store_true",
        default=False,
        help=(
            "Also rewrite the combined forecast CSV files. Off by default so the "
            "backfill is API-only and never clobbers the operational CSVs."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the period-forecast backfill CLI.

    Returns an int exit code (0 on success, non-zero on any error). Never calls
    ``sys.exit`` so callers (and tests) can read the return value directly.
    """
    if argv is None:
        argv = sys.argv[1:]

    parser = _build_parser()
    args = parser.parse_args(argv)

    # --- Validate dates BEFORE any environment load / processing so bad input
    # returns non-zero without ever invoking the aggregation path. ---
    try:
        start = dt.datetime.strptime(args.start_date, "%Y-%m-%d").date()
    except ValueError:
        logger.error("Invalid --start-date=%r; expected YYYY-MM-DD.", args.start_date)
        return 2
    try:
        end = dt.datetime.strptime(args.end_date, "%Y-%m-%d").date()
    except ValueError:
        logger.error("Invalid --end-date=%r; expected YYYY-MM-DD.", args.end_date)
        return 2
    if end < start:
        logger.error(
            "--end-date (%s) is before --start-date (%s); nothing to do.",
            end,
            start,
        )
        return 2

    sl.load_environment()

    # Make swallowed API-write exceptions surface so a failed backfill write
    # cannot be mistaken for success. Restored in the finally below so the
    # override does not leak beyond this run (relevant only in-process, e.g.
    # tests; a real CLI run exits afterward).
    _prev_failure_mode = os.environ.get("SAPPHIRE_API_FAILURE_MODE")
    os.environ["SAPPHIRE_API_FAILURE_MODE"] = "fail"
    logger.info("Set SAPPHIRE_API_FAILURE_MODE=fail so API-write errors surface.")

    try:
        if args.horizon == "pentad":
            configs = [PENTAD]
        elif args.horizon == "decad":
            configs = [DECAD]
        else:
            configs = [PENTAD, DECAD]

        logger.info(
            "Backfilling period forecasts: %s -> %s, horizon=%s, dry_run=%s, write_csv=%s",
            start,
            end,
            args.horizon,
            args.dry_run,
            args.write_csv,
        )

        errors: list[str] = []
        for year in range(start.year, end.year + 1):
            for config in configs:
                label = config.name.upper()
                anchor = dt.date(year, 1, 1)
                try:
                    _run_short_term_postprocessing(
                        config,
                        anchor,
                        errors,
                        TimingStats(),
                        start_year=year,
                        end_year=year,
                        dry_run=args.dry_run,
                        write_csv=args.write_csv,
                        require_api=True,
                    )
                    logger.info("Backfill %s year=%d: ok", label, year)
                except Exception as exc:
                    msg = f"{label} year={year} backfill failed: {exc}"
                    logger.exception(msg)
                    errors.append(msg)
    finally:
        if _prev_failure_mode is None:
            os.environ.pop("SAPPHIRE_API_FAILURE_MODE", None)
        else:
            os.environ["SAPPHIRE_API_FAILURE_MODE"] = _prev_failure_mode

    if errors:
        logger.error("Backfill finished with %d error(s):", len(errors))
        for err in errors:
            logger.error("  - %s", err)
        return 1

    logger.info("Backfill finished successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
