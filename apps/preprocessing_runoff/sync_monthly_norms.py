# DEPRECATED (2026-06-02). Use sync_long_horizon_hydrograph.py
# instead. This script wrote norm-only monthly hydrograph rows
# (`previous` and `current` were always None). The replacement
# writes the full triad (norm + previous + current) for monthly
# rows and additionally writes seasonal April-September rows.
# Operator wrapper: bin/yearly_runoff_hydrograph_aggregation.sh.
# This script may be deleted in a follow-up cleanup phase.

"""Yearly monthly discharge norm ingestion from iEH HF SDK.

Fetches monthly discharge norms for every forecast-enabled site from the
iEasyHydro High Frequency (HF) SDK and writes them to the SAPPHIRE
hydrographs table with ``horizon_type='month'`` (12 rows per site, one per
calendar month of the current year).

Manual (Google-Sheets-backed) sites are filtered out before the SDK call;
they do not contribute to the monthly norm table and would silently return
no data if passed in.

Designed to run once a year (e.g., ``0 3 1 1 *`` — 1 January 03:00 UTC).

Usage::

    # Dry-run: print resolved site list and exit without writing
    uv run sync_monthly_norms.py --dry-run

    # Normal run (year resolved from today)
    uv run sync_monthly_norms.py

    # Explicitly specify the target year
    uv run sync_monthly_norms.py --current-year 2025

Exit codes:
    0  Partial or full success — at least one SDK site's norm was written.
       Also used by --dry-run and --help (no write attempted).
    1  RuntimeError from the library (API disabled / unavailable / readiness
       check failed). The monthly path has no CSV fallback; the caller
       (Luigi, cron, ``set -e`` shell) sees a nonzero exit and retries.
    2  Full failure — the library returned False because zero SDK sites
       produced valid monthly norms (SDK returned wrong-length or empty
       data for every site).
    3  Unexpected exception not covered by the above (generic catch-all).
"""

import argparse
import logging
import os
import sys

# ---------------------------------------------------------------------------
# Path setup — ensure iEasyHydroForecast is importable from this directory.
# ---------------------------------------------------------------------------
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_IEHF_DIR = os.path.join(_SCRIPT_DIR, "..", "iEasyHydroForecast")
if _IEHF_DIR not in sys.path:
    sys.path.insert(0, _IEHF_DIR)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

# ---------------------------------------------------------------------------
# Imports from shared libraries (after path setup)
# ---------------------------------------------------------------------------
import setup_library as sl
from forecast_library import write_month_hydrograph_data
from ieasyhydro_sdk.sdk import IEasyHydroHFSDK
from setup_library import (
    _get_manual_site_codes,
    get_all_forecast_sites_from_HF_SDK,
)

# ---------------------------------------------------------------------------
# Logging — simple stream handler; the Luigi runner captures stdout/stderr.
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sync_monthly_norms.py",
        description=(
            "Fetch monthly discharge norms for all forecast-enabled SDK sites "
            "from the iEH HF API and write them to the SAPPHIRE hydrographs "
            "table (horizon_type='month').\n\n"
            "Exit codes: 0=success/dry-run, 1=API RuntimeError, "
            "2=zero SDK sites succeeded, 3=unexpected exception."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--current-year",
        type=int,
        default=None,
        metavar="YEAR",
        help=(
            "Year to stamp on the date column (e.g. 2025 → dates "
            "2025-01-01 .. 2025-12-01). Defaults to the current calendar year."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help=(
            "Resolve the site list and print it, then exit 0 without "
            "calling the library or the SDK norm endpoint. "
            "Useful to validate configuration."
        ),
    )
    return parser


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Entry point for the yearly monthly norm ingestion task.

    Initialises the iEH HF SDK, loads all forecast-enabled site codes,
    filters out manual (Google-Sheets-backed) sites, and delegates to
    ``forecast_library.write_month_hydrograph_data``.

    Args:
        None — reads from ``sys.argv`` and the deployment ``.env`` file.

    Returns:
        None — exits via ``sys.exit`` with an appropriate code.

    Exit codes:
        0  Success (or --dry-run / --help).
        1  ``RuntimeError`` from ``write_month_hydrograph_data`` (API
           disabled / unavailable / readiness check failed).
        2  Zero SDK sites produced valid monthly norms.
        3  Unexpected exception.
    """
    parser = _build_parser()
    args = parser.parse_args()

    try:
        # ------------------------------------------------------------------
        # 1. Load deployment environment (.env file)
        # ------------------------------------------------------------------
        sl.load_environment()

        # ------------------------------------------------------------------
        # 2. Initialise the iEH HF SDK
        # ------------------------------------------------------------------
        sdk = IEasyHydroHFSDK()
        logger.info("iEasyHydro HF SDK initialised.")

        # ------------------------------------------------------------------
        # 3. Load all forecast-enabled sites (includes manual sites appended
        #    by setup_library at lines 1567-1571)
        # ------------------------------------------------------------------
        _fc_sites, site_codes, _site_ids = get_all_forecast_sites_from_HF_SDK(sdk)
        if site_codes is None:
            site_codes = []
        logger.info("Total forecast-enabled sites (before filtering): %d", len(site_codes))

        # ------------------------------------------------------------------
        # 4. Filter out manual sites — they are not available via the iEH HF
        #    SDK norm endpoint and would produce empty/wrong-length responses.
        # ------------------------------------------------------------------
        manual_codes = _get_manual_site_codes()
        manual_set = set(manual_codes)

        sdk_only_codes = []
        for code in site_codes:
            if code in manual_set:
                logger.info(
                    "Skipping manual site %s — not available via iEH HF SDK, "
                    "daily/monthly data comes from alternate source",
                    code,
                )
            else:
                sdk_only_codes.append(code)

        logger.info(
            "SDK-only sites after filtering %d manual site(s): %d site(s) → %s",
            len(manual_codes),
            len(sdk_only_codes),
            sdk_only_codes,
        )

        # ------------------------------------------------------------------
        # 5. Resolve current_year (for logging; the library handles None too)
        # ------------------------------------------------------------------
        from datetime import date as _date

        resolved_year = args.current_year if args.current_year is not None else _date.today().year
        logger.info("Target year for monthly norm rows: %d", resolved_year)

        # ------------------------------------------------------------------
        # 6. --dry-run: print and exit without writing
        # ------------------------------------------------------------------
        if args.dry_run:
            print(f"DRY-RUN — sdk_only_codes: {sdk_only_codes}")
            print(f"DRY-RUN — current_year: {resolved_year}")
            logger.info("Dry-run complete — no data written.")
            sys.exit(0)

        # ------------------------------------------------------------------
        # 7. Guard: no SDK sites → exit 2
        # ------------------------------------------------------------------
        if not sdk_only_codes:
            logger.error(
                "No SDK sites remain after filtering — nothing to write. "
                "Check that at least one forecast-enabled site is not manual."
            )
            sys.exit(2)

        # ------------------------------------------------------------------
        # 8. Call the library function
        # ------------------------------------------------------------------
        logger.info(
            "Calling write_month_hydrograph_data for %d SDK site(s).",
            len(sdk_only_codes),
        )
        result = write_month_hydrograph_data(sdk_only_codes, sdk, current_year=args.current_year)

        if not result:
            logger.error(
                "write_month_hydrograph_data returned False — "
                "zero SDK sites produced valid monthly norms."
            )
            sys.exit(2)

        logger.info("Monthly norm ingestion completed successfully.")
        sys.exit(0)

    except RuntimeError as exc:
        logger.error("API error during monthly norm ingestion: %s", exc)
        sys.exit(1)

    except SystemExit:
        # Re-raise SystemExit (from sys.exit calls above or argparse --help)
        raise

    except Exception as exc:
        logger.exception("Unexpected error during monthly norm ingestion: %s", exc)
        sys.exit(3)


if __name__ == "__main__":
    main()
