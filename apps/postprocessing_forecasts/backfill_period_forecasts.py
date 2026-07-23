"""PP-045 backfill CLI for stranded short-term PENTAD/DECADE period forecasts.

Per-model PENTAD/DECADE period rows are created only by the operational path on
boundary days. If an operational boundary day was missed, those rows are never
written and routine maintenance cannot heal them (maintenance recalculates skill
metrics, not period forecasts). This CLI re-aggregates a date range through the
EXISTING operational aggregation + save path so the stranded rows are recomputed
and written.

Whole-year granularity, ONE YEAR AT A TIME
------------------------------------------
The run iterates one calendar year per call to ``_run_short_term_postprocessing``
(``start_year == end_year``). This is deliberate, not an optimization:
``file_writer.get_latest_forecasts`` dedups on a YEARLESS key
(``[code, period_in_year, model_short]``), so feeding it more than one year at
once would collapse the same period across years into a single row and drop
older years. Processing one year at a time keeps each year's period rows
distinct.

Issue-date vs target (Dec 31 -> Jan 1)
--------------------------------------
Short-term forecasts are issued on a boundary day and target the NEXT period
(``forecast_target_date`` maps an issue date to target = issue + 1 day). A period
that starts Jan 1 of year Y is therefore issued on Dec 31 of year Y-1. The reader
is asked for calendar year Y and the aggregation anchors the period in year Y, so
the year-at-a-time loop keeps period rows anchored in the correct year.

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
        help="First date of the range to backfill (inclusive).",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        metavar="YYYY-MM-DD",
        help="Last date of the range to backfill (inclusive).",
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
