"""Query which long-term forecast modes are active for a given date.

Reads all mode JSON configs and reports which modes should run today,
based on operational_issue_day and per-model forecast_months restrictions.

Usage:
    ieasyhydroforecast_env_file_path=... python lt_schedule_query.py [--today YYYY-MM-DD]

Outputs JSON to stdout:
    {
      "active_modes": ["month_0"],
      "skipped_modes": {"month_1": "13 days from issue date 2026-03-25", ...},
      "skill_metric_types": ["MONTHLY"]
    }

All logging goes to stderr so stdout is clean JSON for shell parsing.
"""

import argparse
import json
import logging
import os
import sys

import pandas as pd

# Add parent directory for iEasyHydroForecast imports
script_dir = os.path.dirname(os.path.abspath(__file__))
forecast_dir = os.path.join(script_dir, "..", "iEasyHydroForecast")
sys.path.insert(0, forecast_dir)
sys.path.insert(0, script_dir)

import setup_library as sl
from config_forecast import ForecastConfig
from lt_utils import nearest_scheduled_issue_date

# Log to stderr only
logger = logging.getLogger("lt_schedule_query")
handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

HORIZON_TYPE_TO_SKILL = {
    "month": "MONTHLY",
    "quarter": "QUARTERLY",
    "season": "SEASONAL",
}

# Temoporarily relaxed to 10 days to allow more modes to be active for testing and calibration.
# Must be changed back to 5 days for operational use to avoid running modes too far from their issue day.
ISSUE_DAY_TOLERANCE = 10

# Modes used only for calibration / retraining, not operational scheduling.
# Keep them in ieasyhydroforecast_ml_long_term_supported_modes so the
# maintenance pipeline can reference them, but skip in query_schedule().
NON_OPERATIONAL_MODES = {"monthly"}


def day_distance(today_dom: int, issue_day: int) -> int:
    """Compute distance between day-of-month values with wrap-around."""
    diff = abs(today_dom - issue_day)
    wrap = 30 - diff  # approximate month wrap
    return min(diff, wrap)


def query_schedule(today: pd.Timestamp) -> dict:
    """Determine which long-term modes are active for the given date.

    Args:
        today: The date to check against mode configs.

    Returns:
        Dict with active_modes, skipped_modes, and skill_metric_types.
    """
    sl.load_environment()

    config = ForecastConfig()
    supported_modes = config.LT_supported_modes

    today_dom = today.day
    today_month = today.month

    active_modes = []
    skipped_modes = {}
    skill_types = set()

    for mode in supported_modes:
        if mode in NON_OPERATIONAL_MODES:
            skipped_modes[mode] = "non-operational (calibration/retraining only)"
            continue

        try:
            config.load_forecast_config(forecast_mode=mode)
        except Exception as e:
            logger.warning("Failed to load config for mode %s: %s", mode, e)
            skipped_modes[mode] = f"config load error: {e}"
            continue

        issue_day = config.get_operational_issue_day()
        dist = day_distance(today_dom, issue_day)

        if dist > ISSUE_DAY_TOLERANCE:
            skipped_modes[mode] = f"{dist} days from issue day {issue_day}"
            continue

        # Day check passed — now check per-model forecast_months
        # A mode is active if at least one model is scheduled this month
        models = config.get_models_to_run()
        any_model_scheduled = False
        for model_name in models:
            forecast_months = config.get_forecast_months(model_name=model_name)
            if not forecast_months or forecast_months == list(range(1, 13)):
                # No restriction or all months — scheduled
                any_model_scheduled = True
                break
            # Use nearest_scheduled_issue_date to check if this month qualifies
            nearest = nearest_scheduled_issue_date(today, issue_day, forecast_months)
            if abs((today - nearest).days) <= ISSUE_DAY_TOLERANCE:
                any_model_scheduled = True
                break

        if not any_model_scheduled:
            skipped_modes[mode] = f"no models scheduled in month {today_month}"
            continue

        active_modes.append(mode)
        horizon_type = config.get_horizon_type()
        skill_type = HORIZON_TYPE_TO_SKILL.get(horizon_type)
        if skill_type:
            skill_types.add(skill_type)

    return {
        "active_modes": active_modes,
        "skipped_modes": skipped_modes,
        "skill_metric_types": sorted(skill_types),
    }


def main():
    parser = argparse.ArgumentParser(description="Query active long-term forecast modes for a date")
    parser.add_argument(
        "--today",
        type=str,
        default=None,
        help="Override date in YYYY-MM-DD format (default: today)",
    )
    args = parser.parse_args()

    if args.today:
        today = pd.Timestamp(args.today)
    else:
        today = pd.Timestamp.now().normalize()

    logger.info("Checking schedule for %s", today.date())

    result = query_schedule(today)

    logger.info("Active modes: %s", result["active_modes"])
    for mode, reason in result["skipped_modes"].items():
        logger.info("Skipped %s: %s", mode, reason)
    logger.info("Skill metric types: %s", result["skill_metric_types"])

    # Clean JSON to stdout for shell parsing
    print(json.dumps(result))


if __name__ == "__main__":
    main()
