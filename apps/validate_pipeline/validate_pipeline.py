"""Validate SAPPHIRE pipeline data in the API after a pipeline run.

Queries the preprocessing and postprocessing APIs to verify that
expected data was written for the current forecast date. Reports
results in a table matching the run_locally.sh log style.

Usage:
    python validate_pipeline.py --target short-term
    python validate_pipeline.py --target long-term --forecast-date 2026-02-23
    python validate_pipeline.py --target daily --horizon pentad
    python validate_pipeline.py --module linear_regression

The --module flag validates just one module's data (Tier 1 + Tier 2).
When used, --target is auto-inferred from the module if not provided,
and Tier 3 cross-module checks are skipped.

Exit codes:
    0 — all checks passed (or skipped/warned)
    1 — at least one check FAILed
"""

import argparse
import calendar
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Callable, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# API client availability
# ---------------------------------------------------------------------------
try:
    from sapphire_api_client.preprocessing import (
        SapphirePreprocessingClient,
    )
    from sapphire_api_client.postprocessing import (
        SapphirePostprocessingClient,
    )

    SAPPHIRE_API_AVAILABLE = True
except ImportError:
    SapphirePreprocessingClient = None  # type: ignore[assignment,misc]
    SapphirePostprocessingClient = None  # type: ignore[assignment,misc]
    SAPPHIRE_API_AVAILABLE = False


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Map SAPPHIRE_PREDICTION_MODE values to API horizon strings.
MODE_TO_HORIZONS: Dict[str, List[str]] = {
    "PENTAD": ["pentad"],
    "DECAD": ["decade"],
    "BOTH": ["pentad", "decade"],
}

# Short-term forecast models to check.
SHORT_TERM_MODELS = ["LR", "TFT", "TiDE", "TSMixer", "EM", "NE"]

# Quantile columns in ML forecasts.
QUANTILE_COLS = ["q05", "q25", "q50", "q75", "q95"]

# High limit for API reads so we get enough records for validation.
READ_LIMIT = 5000

# Map single-module names to their default validation target.
MODULE_DEFAULT_TARGET: Dict[str, str] = {
    "preprocessing_runoff": "short-term",
    "preprocessing_gateway": "short-term",
    "linear_regression": "short-term",
    "machine_learning": "short-term",
    "postprocessing_forecasts": "short-term",
    "long_term_forecasting": "long-term",
}

# Modules that only produce data on forecast days (not daily).
FORECAST_DAY_MODULES = {
    "linear_regression", "machine_learning", "postprocessing_forecasts",
}


def is_pentad_forecast_day(d: date) -> bool:
    """Return True if *d* is a pentad forecast day (5/10/15/20/25/last)."""
    last_day = calendar.monthrange(d.year, d.month)[1]
    return d.day in (5, 10, 15, 20, 25, last_day)


def is_decad_forecast_day(d: date) -> bool:
    """Return True if *d* is a decad forecast day (10/20/last)."""
    last_day = calendar.monthrange(d.year, d.month)[1]
    return d.day in (10, 20, last_day)


def most_recent_pentad_boundary(d: date) -> date:
    """Return the most recent pentad boundary date <= *d*."""
    last_day = calendar.monthrange(d.year, d.month)[1]
    boundaries = [5, 10, 15, 20, 25, last_day]
    for b in reversed(boundaries):
        if b <= d.day:
            return date(d.year, d.month, b)
    # Current day is before day 5: wrap to previous month's last day
    prev_month_last = d.replace(day=1) - timedelta(days=1)
    return prev_month_last


def most_recent_decad_boundary(d: date) -> date:
    """Return the most recent decad boundary date <= *d*."""
    last_day = calendar.monthrange(d.year, d.month)[1]
    boundaries = [10, 20, last_day]
    for b in reversed(boundaries):
        if b <= d.day:
            return date(d.year, d.month, b)
    # Current day is before day 10: wrap to previous month's last day
    prev_month_last = d.replace(day=1) - timedelta(days=1)
    return prev_month_last


# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------


@dataclass
class CheckResult:
    """Result of a single validation check."""

    name: str
    status: str  # PASS, FAIL, WARN, SKIP
    detail: str = ""
    record_count: int = 0
    module: str = ""  # Source pipeline module, e.g. "preprocessing_runoff"
    data: Optional[pd.DataFrame] = field(default=None, repr=False)


def _status_tag(status: str) -> str:
    """Return a coloured status tag for terminal output."""
    tags = {
        "PASS": "\033[0;32m[OK]  \033[0m",
        "FAIL": "\033[0;31m[FAIL]\033[0m",
        "WARN": "\033[1;33m[WARN]\033[0m",
        "SKIP": "\033[0;34m[SKIP]\033[0m",
    }
    return tags.get(status, f"[{status}]")


# ---------------------------------------------------------------------------
# Tier 1 — Data Presence
# ---------------------------------------------------------------------------


def check_presence(
    client,
    read_method: str,
    check_name: str,
    *,
    module: str = "",
    warn_if_empty: bool = False,
    **query_kwargs,
) -> CheckResult:
    """Query an API read method and check for non-empty results.

    Args:
        client: API client instance.
        read_method: Name of the read method on the client.
        check_name: Human-readable name for reporting.
        module: Source pipeline module that produces this data.
        warn_if_empty: If True, empty results produce WARN instead of FAIL.
        **query_kwargs: Passed to the read method.

    Returns:
        CheckResult with the fetched DataFrame stored in .data.
    """
    try:
        method = getattr(client, read_method)
        df = method(**query_kwargs, limit=READ_LIMIT)
    except Exception as exc:
        return CheckResult(
            name=check_name,
            status="FAIL",
            detail=f"API error: {exc}",
            module=module,
        )

    if df is None or df.empty:
        status = "WARN" if warn_if_empty else "FAIL"
        detail = "no records" + (
            " (may not be configured)" if warn_if_empty else ""
        )
        return CheckResult(
            name=check_name,
            status=status,
            detail=detail,
            module=module,
            data=df,
        )

    return CheckResult(
        name=check_name,
        status="PASS",
        detail=f"{len(df)} records",
        record_count=len(df),
        module=module,
        data=df,
    )


def run_tier1_short_term(
    pre_client,
    post_client,
    forecast_date: date,
    horizon: str,
) -> List[CheckResult]:
    """Run Tier 1 presence checks for a short-term horizon.

    Note: preprocessing_runoff only writes horizon_type="day" for both
    runoff and hydrograph. There are no pentad/decade horizon records
    in the preprocessing API, so we only check the "day" horizon.
    """
    results: List[CheckResult] = []
    fd = str(forecast_date)

    # Compute the most recent boundary date for forecast queries.
    # Forecasts are only issued on boundary days, so we query from
    # the boundary to today to find data even on non-boundary days.
    if horizon == "pentad":
        boundary = most_recent_pentad_boundary(forecast_date)
    elif horizon == "decade":
        boundary = most_recent_decad_boundary(forecast_date)
    else:
        boundary = forecast_date
    bd = str(boundary)

    # Preprocessing checks — runoff and hydrograph are day-horizon only
    # (always query today's date since preprocessing runs daily)
    results.append(
        check_presence(
            pre_client, "read_runoff", "Runoff (day)",
            module="preprocessing_runoff",
            horizon="day", start_date=fd, end_date=fd,
        )
    )
    results.append(
        check_presence(
            pre_client, "read_hydrograph", "Hydrograph (day)",
            module="preprocessing_runoff",
            horizon="day", start_date=fd, end_date=fd,
        )
    )
    results.append(
        check_presence(
            pre_client, "read_meteo", "Meteo (T)",
            module="preprocessing_gateway",
            meteo_type="T", start_date=fd, end_date=fd,
        )
    )
    results.append(
        check_presence(
            pre_client, "read_meteo", "Meteo (P)",
            module="preprocessing_gateway",
            meteo_type="P", start_date=fd, end_date=fd,
        )
    )
    results.append(
        check_presence(
            pre_client, "read_snow", "Snow (SWE)",
            module="preprocessing_gateway",
            snow_type="SWE", start_date=fd, end_date=fd,
            warn_if_empty=True,
        )
    )

    # Postprocessing checks — short-term forecasts per model
    # Query from boundary date to today to find forecasts issued
    # on the most recent boundary day.
    model_modules = {
        "LR": "linear_regression",
        "TFT": "machine_learning",
        "TiDE": "machine_learning",
        "TSMixer": "machine_learning",
        "EM": "postprocessing_forecasts",
        "NE": "postprocessing_forecasts",
    }
    for model in SHORT_TERM_MODELS:
        results.append(
            check_presence(
                post_client, "read_short_term_forecasts",
                f"Forecasts ({model}, {horizon})",
                module=model_modules.get(model, ""),
                horizon=horizon, model=model,
                start_date=bd, end_date=fd,
            )
        )

    # LR details — query from boundary to today
    results.append(
        check_presence(
            post_client, "read_lr_forecasts",
            f"LR details ({horizon})",
            module="linear_regression",
            horizon=horizon, start_date=bd, end_date=fd,
        )
    )

    # Skill metrics (not date-filtered — covers all historical periods)
    results.append(
        check_presence(
            post_client, "read_skill_metrics",
            f"Skill metrics ({horizon})",
            module="postprocessing_forecasts",
            horizon=horizon,
        )
    )

    return results


def run_tier1_long_term(
    post_client,
    forecast_date: date,
) -> List[CheckResult]:
    """Run Tier 1 presence checks for long-term forecasts."""
    results: List[CheckResult] = []
    fd = str(forecast_date)

    results.append(
        check_presence(
            post_client, "read_long_term_forecasts",
            "Long-term forecasts (month)",
            module="long_term_forecasting",
            horizon_type="month", start_date=fd, end_date=fd,
        )
    )
    results.append(
        check_presence(
            post_client, "read_skill_metrics",
            "Monthly skill metrics",
            module="postprocessing_forecasts",
            horizon="month",
        )
    )

    return results


# ---------------------------------------------------------------------------
# Tier 2 — Data Correctness
# ---------------------------------------------------------------------------


def check_discharge_non_negative(results: List[CheckResult]) -> CheckResult:
    """Verify that discharge / forecasted_discharge values are >= 0."""
    bad_count = 0
    total = 0
    sources: List[str] = []

    for r in results:
        if r.data is None or r.data.empty:
            continue
        for col in ("discharge", "forecasted_discharge"):
            if col in r.data.columns:
                series = pd.to_numeric(r.data[col], errors="coerce")
                negatives = series.dropna() < 0
                n_bad = int(negatives.sum())
                bad_count += n_bad
                total += int(negatives.count())
                if n_bad > 0:
                    neg_vals = series.dropna()[negatives].tolist()
                    sources.append(
                        f"{r.name}.{col}: {n_bad} neg "
                        f"(vals: {neg_vals[:5]})"
                    )

    if total == 0:
        return CheckResult(
            name="Discharge non-negative",
            status="SKIP",
            detail="no discharge data to check",
        )
    if bad_count > 0:
        detail = f"{bad_count} records with negative values"
        if sources:
            detail += " — " + "; ".join(sources)
        return CheckResult(
            name="Discharge non-negative",
            status="FAIL",
            detail=detail,
        )
    return CheckResult(
        name="Discharge non-negative",
        status="PASS",
        detail=f"all {total} values valid",
    )


def check_no_nan_in_forecasts(results: List[CheckResult]) -> CheckResult:
    """Check that forecasted_discharge is not NaN in forecast records."""
    nan_count = 0
    total = 0

    for r in results:
        if r.data is None or r.data.empty:
            continue
        if "forecasted_discharge" not in r.data.columns:
            continue
        series = r.data["forecasted_discharge"]
        nan_count += int(series.isna().sum())
        total += len(series)

    if total == 0:
        return CheckResult(
            name="No NaN in forecasts",
            status="SKIP",
            detail="no forecast data to check",
        )
    if nan_count > 0:
        return CheckResult(
            name="No NaN in forecasts",
            status="WARN",
            detail=f"{nan_count}/{total} forecasts are NaN",
        )
    return CheckResult(
        name="No NaN in forecasts",
        status="PASS",
        detail=f"all {total} values present",
    )


def check_quantile_ordering(results: List[CheckResult]) -> CheckResult:
    """Verify q05 <= q25 <= q50 <= q75 <= q95 row-wise."""
    bad_count = 0
    total = 0

    for r in results:
        if r.data is None or r.data.empty:
            continue
        cols_present = [c for c in QUANTILE_COLS if c in r.data.columns]
        if len(cols_present) < 2:
            continue

        df_q = r.data[cols_present].apply(
            pd.to_numeric, errors="coerce"
        )
        # Drop rows where all quantiles are NaN
        df_q = df_q.dropna(how="all")
        total += len(df_q)

        for i in range(len(cols_present) - 1):
            left = df_q[cols_present[i]]
            right = df_q[cols_present[i + 1]]
            # Only compare where both are non-NaN
            mask = left.notna() & right.notna()
            bad_count += int((left[mask] > right[mask]).sum())

    if total == 0:
        return CheckResult(
            name="Quantile ordering",
            status="SKIP",
            detail="no quantile data to check",
        )
    if bad_count > 0:
        return CheckResult(
            name="Quantile ordering",
            status="FAIL",
            detail=f"{bad_count} disordered quantile pairs",
        )
    return CheckResult(
        name="Quantile ordering",
        status="PASS",
        detail="all valid",
    )


def check_expected_models(
    results: List[CheckResult],
    horizon: str,
) -> CheckResult:
    """Check that all expected short-term models are present.

    Models whose Tier 1 check was SKIP'd (e.g. not a forecast day for
    that horizon) are excluded from the expected set.
    """
    found_models = set()
    skipped_models = set()
    for r in results:
        if r.status == "SKIP" and r.name.startswith("Forecasts ("):
            # Extract model name from "Forecasts (MODEL, horizon)"
            model = r.name.split("(")[1].split(",")[0]
            skipped_models.add(model)
            continue
        if r.data is None or r.data.empty:
            continue
        if "model_type" in r.data.columns:
            found_models.update(r.data["model_type"].unique())
        elif "model_short" in r.data.columns:
            found_models.update(r.data["model_short"].unique())

    expected = set(SHORT_TERM_MODELS) - skipped_models
    missing = expected - found_models
    if not missing:
        detail = f"found {len(found_models)} models"
        if skipped_models:
            detail += f" ({len(skipped_models)} skipped)"
        return CheckResult(
            name=f"All models present ({horizon})",
            status="PASS",
            detail=detail,
        )
    return CheckResult(
        name=f"All models present ({horizon})",
        status="FAIL",
        detail=f"missing: {', '.join(sorted(missing))}",
    )


def check_skill_metric_ranges(results: List[CheckResult]) -> CheckResult:
    """Verify skill metric values are within reasonable ranges.

    NSE > 1.0 and accuracy outside [0, 100] are hard FAILs.
    n_pairs <= 0 is a WARN — new stations legitimately have 0 pairs
    until enough historical data accumulates.
    """
    fail_issues: List[str] = []
    warn_issues: List[str] = []
    checked = 0

    for r in results:
        if r.data is None or r.data.empty:
            continue
        if "nse" not in r.data.columns and "accuracy" not in r.data.columns:
            continue
        checked += len(r.data)

        if "nse" in r.data.columns:
            nse = pd.to_numeric(r.data["nse"], errors="coerce").dropna()
            bad_nse = (nse > 1.0).sum()
            if bad_nse > 0:
                fail_issues.append(f"{bad_nse} records with NSE > 1.0")

        if "accuracy" in r.data.columns:
            acc = pd.to_numeric(
                r.data["accuracy"], errors="coerce"
            ).dropna()
            bad_lo = (acc < 0).sum()
            bad_hi = (acc > 100).sum()
            if bad_lo + bad_hi > 0:
                fail_issues.append(
                    f"{bad_lo + bad_hi} records with accuracy outside [0, 100]"
                )

        if "n_pairs" in r.data.columns:
            np_ = pd.to_numeric(
                r.data["n_pairs"], errors="coerce"
            ).dropna()
            bad_np = (np_ <= 0).sum()
            if bad_np > 0:
                warn_issues.append(
                    f"{bad_np} records with n_pairs <= 0 "
                    "(new stations may lack historical data)"
                )

    if checked == 0:
        return CheckResult(
            name="Skill metric ranges",
            status="SKIP",
            detail="no skill metrics to check",
            module="postprocessing_forecasts",
        )
    if fail_issues:
        all_issues = fail_issues + warn_issues
        return CheckResult(
            name="Skill metric ranges",
            status="FAIL",
            detail="; ".join(all_issues),
            module="postprocessing_forecasts",
        )
    if warn_issues:
        return CheckResult(
            name="Skill metric ranges",
            status="WARN",
            detail="; ".join(warn_issues),
            module="postprocessing_forecasts",
        )
    return CheckResult(
        name="Skill metric ranges",
        status="PASS",
        detail=f"all {checked} records valid",
        module="postprocessing_forecasts",
    )


def run_tier2(
    tier1_results: List[CheckResult],
    horizon: str,
    module_filter: Optional[str] = None,
) -> List[CheckResult]:
    """Run Tier 2 correctness checks reusing Tier 1 DataFrames.

    Args:
        tier1_results: Tier 1 check results (possibly filtered by module).
        horizon: API horizon string (e.g. "pentad").
        module_filter: If set, skip cross-module checks like "all models
            present" — same rationale as Tier 3.
    """
    results: List[CheckResult] = []

    results.append(check_discharge_non_negative(tier1_results))
    results.append(check_no_nan_in_forecasts(tier1_results))
    results.append(check_quantile_ordering(tier1_results))

    # "All models present" is a cross-module check — skip when
    # validating a single module (same rationale as Tier 3).
    if not module_filter:
        forecast_results = [
            r for r in tier1_results if r.name.startswith("Forecasts (")
        ]
        results.append(check_expected_models(forecast_results, horizon))

    # Skill metric checks
    skill_results = [
        r for r in tier1_results if "skill" in r.name.lower()
    ]
    results.append(check_skill_metric_ranges(skill_results))

    return results


# ---------------------------------------------------------------------------
# Tier 3 — Cross-module Consistency
# ---------------------------------------------------------------------------


def check_station_codes_match(
    tier1_results: List[CheckResult],
) -> CheckResult:
    """Verify forecast station codes are a subset of runoff station codes."""
    runoff_codes = set()
    forecast_codes = set()

    for r in tier1_results:
        if r.data is None or r.data.empty:
            continue
        if "code" not in r.data.columns:
            continue

        if r.name.startswith("Runoff"):
            runoff_codes.update(r.data["code"].astype(str).unique())
        elif r.name.startswith("Forecasts"):
            forecast_codes.update(r.data["code"].astype(str).unique())

    if not runoff_codes or not forecast_codes:
        return CheckResult(
            name="Station codes match",
            status="SKIP",
            detail="insufficient data for comparison",
        )

    extra = forecast_codes - runoff_codes
    if extra:
        return CheckResult(
            name="Station codes match",
            status="WARN",
            detail=(
                f"{len(extra)} forecast codes not in runoff: "
                f"{', '.join(sorted(extra)[:5])}"
            ),
        )
    return CheckResult(
        name="Station codes match",
        status="PASS",
        detail=f"{len(forecast_codes)} codes verified",
    )


def check_dates_consistent(
    tier1_results: List[CheckResult],
) -> CheckResult:
    """Check all models have forecasts for the same (code, date) tuples."""
    model_tuples: Dict[str, set] = {}

    for r in tier1_results:
        if not r.name.startswith("Forecasts ("):
            continue
        if r.data is None or r.data.empty:
            continue
        if "code" not in r.data.columns or "date" not in r.data.columns:
            continue

        model_name = r.name
        tuples = set(
            zip(
                r.data["code"].astype(str),
                r.data["date"].astype(str),
            )
        )
        model_tuples[model_name] = tuples

    if len(model_tuples) < 2:
        return CheckResult(
            name="Dates consistent across models",
            status="SKIP",
            detail="need at least 2 models with data",
        )

    all_tuples = set.union(*model_tuples.values())
    issues = []
    for name, tuples in model_tuples.items():
        missing = all_tuples - tuples
        if missing:
            issues.append(f"{name}: missing {len(missing)} tuples")

    if issues:
        return CheckResult(
            name="Dates consistent across models",
            status="WARN",
            detail="; ".join(issues[:3]),
        )
    return CheckResult(
        name="Dates consistent across models",
        status="PASS",
        detail=f"{len(all_tuples)} (code, date) tuples consistent",
    )


def run_tier3(tier1_results: List[CheckResult]) -> List[CheckResult]:
    """Run Tier 3 cross-module consistency checks."""
    return [
        check_station_codes_match(tier1_results),
        check_dates_consistent(tier1_results),
    ]


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def print_results(
    section: str,
    results: List[CheckResult],
) -> None:
    """Print results in run_locally.sh log style."""
    print(f"\n--- {section} ---")
    for r in results:
        tag = _status_tag(r.status)
        detail = f": {r.detail}" if r.detail else ""
        module = f" [{r.module}]" if r.module else ""
        print(f"{tag} {r.name}{detail}{module}")


def print_summary(all_results: List[CheckResult]) -> int:
    """Print summary counts and return exit code."""
    counts = {"PASS": 0, "FAIL": 0, "WARN": 0, "SKIP": 0}
    for r in all_results:
        counts[r.status] = counts.get(r.status, 0) + 1

    print(
        f"\nVALIDATION SUMMARY: "
        f"{counts['PASS']} passed, "
        f"{counts['FAIL']} failed, "
        f"{counts['WARN']} warned, "
        f"{counts['SKIP']} skipped"
    )

    return 1 if counts["FAIL"] > 0 else 0


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------


def resolve_horizons(horizon_arg: Optional[str]) -> List[str]:
    """Resolve horizon argument into a list of API horizon strings.

    Falls back to SAPPHIRE_PREDICTION_MODE env var, then defaults to
    ["pentad"].
    """
    if horizon_arg:
        return [horizon_arg]

    mode = os.getenv("SAPPHIRE_PREDICTION_MODE", "").upper()
    return MODE_TO_HORIZONS.get(mode, ["pentad"])


def _apply_non_forecast_day_skip(
    results: List[CheckResult],
    forecast_date: date,
    horizon: str,
) -> None:
    """Downgrade FAIL → SKIP for forecast-day modules on non-forecast days.

    Modifies *results* in place. Only affects checks that returned
    FAIL with 0 records from modules that only produce data on
    specific forecast days (not daily).

    Args:
        results: Tier 1 check results to potentially modify.
        forecast_date: The date being validated.
        horizon: "pentad", "decade", or "long-term".
    """
    is_forecast_day = {
        "pentad": is_pentad_forecast_day(forecast_date),
        "decade": is_decad_forecast_day(forecast_date),
        # Long-term forecasts run on specific dates per month;
        # we cannot predict the schedule, so always treat as
        # potentially a non-forecast day when data is absent.
        "long-term": True,
    }
    # If today IS a forecast day for this horizon, nothing to change.
    if is_forecast_day.get(horizon, True):
        return

    for r in results:
        if (
            r.status == "FAIL"
            and r.record_count == 0
            and r.module in FORECAST_DAY_MODULES
        ):
            r.status = "SKIP"
            r.detail = f"not a {horizon} forecast day"


def validate(
    target: str,
    forecast_date: date,
    horizons: List[str],
    module_filter: Optional[str] = None,
) -> int:
    """Run all validation tiers and return exit code.

    Args:
        target: Pipeline target ("short-term", "long-term", "daily", "all").
        forecast_date: Date to validate data for.
        horizons: List of API horizon strings (e.g. ["pentad"]).
        module_filter: If set, only show results from this module and
            skip Tier 3 cross-module checks.
    """
    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    # --- Create clients ---
    pre_client = SapphirePreprocessingClient(base_url=api_url)
    post_client = SapphirePostprocessingClient(base_url=api_url)

    # --- Readiness checks ---
    all_results: List[CheckResult] = []

    pre_ready = False
    try:
        pre_ready = pre_client.readiness_check()
    except Exception:
        pass
    if not pre_ready:
        all_results.append(
            CheckResult(
                name="Preprocessing API",
                status="FAIL",
                detail=f"not ready at {api_url}",
            )
        )

    post_ready = False
    try:
        post_ready = post_client.readiness_check()
    except Exception:
        pass
    if not post_ready:
        all_results.append(
            CheckResult(
                name="Postprocessing API",
                status="FAIL",
                detail=f"not ready at {api_url}",
            )
        )

    # --- Tier 1: Data Presence ---
    tier1_results: List[CheckResult] = []

    if target in ("short-term", "daily", "all"):
        for horizon in horizons:
            if pre_ready and post_ready:
                t1 = run_tier1_short_term(
                    pre_client, post_client, forecast_date, horizon,
                )
            elif post_ready:
                # Can still check postprocessing
                t1 = run_tier1_short_term(
                    None, post_client, forecast_date, horizon,
                )
            else:
                t1 = []
            # Filter to single module if requested
            if module_filter:
                t1 = [r for r in t1 if r.module == module_filter]
            # On non-forecast days, downgrade FAIL→SKIP for modules
            # that only produce data on forecast days.
            _apply_non_forecast_day_skip(t1, forecast_date, horizon)
            print_results(f"Tier 1: Data Presence ({horizon})", t1)
            tier1_results.extend(t1)

    if target in ("long-term", "all"):
        if post_ready:
            t1_lt = run_tier1_long_term(post_client, forecast_date)
        else:
            t1_lt = []
        if module_filter:
            t1_lt = [r for r in t1_lt if r.module == module_filter]
        # Long-term forecasts only run on specific dates per month;
        # treat empty postprocessing results as SKIP if we have no data.
        _apply_non_forecast_day_skip(
            t1_lt, forecast_date, "long-term",
        )
        print_results("Tier 1: Data Presence (long-term)", t1_lt)
        tier1_results.extend(t1_lt)

    all_results.extend(tier1_results)

    # --- Tier 2: Data Correctness ---
    if tier1_results:
        for horizon in horizons:
            horizon_results = [
                r for r in tier1_results
                if horizon in r.name or "day" in r.name.lower()
                or "Meteo" in r.name or "Snow" in r.name
                or "skill" in r.name.lower()
            ]
            t2 = run_tier2(horizon_results, horizon, module_filter)
            print_results("Tier 2: Data Correctness", t2)
            all_results.extend(t2)

    # --- Tier 3: Cross-module Consistency ---
    # Skip when validating a single module (cross-module checks don't apply)
    if tier1_results and not module_filter:
        t3 = run_tier3(tier1_results)
        print_results("Tier 3: Cross-module Consistency", t3)
        all_results.extend(t3)

    return print_summary(all_results)


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point.

    Returns:
        Exit code: 0 if no failures, 1 if any FAIL.
    """
    parser = argparse.ArgumentParser(
        description="Validate SAPPHIRE pipeline data in the API.",
    )
    parser.add_argument(
        "--target",
        choices=["short-term", "long-term", "daily", "all"],
        default=None,
        help="Pipeline target to validate (default: short-term).",
    )
    parser.add_argument(
        "--forecast-date",
        type=date.fromisoformat,
        default=None,
        help="Forecast date in ISO format (default: today).",
    )
    parser.add_argument(
        "--horizon",
        choices=["pentad", "decade"],
        default=None,
        help="Override horizon (default: from SAPPHIRE_PREDICTION_MODE).",
    )
    parser.add_argument(
        "--module",
        choices=sorted(MODULE_DEFAULT_TARGET.keys()),
        default=None,
        help=(
            "Validate only this module's data (Tier 1 + Tier 2). "
            "Auto-infers --target if not provided."
        ),
    )

    args = parser.parse_args(argv)

    # If --module given without --target, auto-infer from the mapping
    target = args.target
    if target is None:
        if args.module:
            target = MODULE_DEFAULT_TARGET[args.module]
        else:
            target = "short-term"

    forecast_date = args.forecast_date or date.today()
    horizons = resolve_horizons(args.horizon)

    # --- Early-exit checks ---
    if not SAPPHIRE_API_AVAILABLE:
        print("[SKIP] sapphire_api_client not installed — skipping validation")
        return 0

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        print("[SKIP] SAPPHIRE_API_ENABLED=false — skipping validation")
        return 0

    if args.module:
        print(
            f"Validating {args.module} data for "
            f"{forecast_date} (horizons: {', '.join(horizons)})"
        )
    else:
        print(
            f"Validating {target} pipeline data for "
            f"{forecast_date} (horizons: {', '.join(horizons)})"
        )

    return validate(target, forecast_date, horizons, module_filter=args.module)


if __name__ == "__main__":
    sys.exit(main())
