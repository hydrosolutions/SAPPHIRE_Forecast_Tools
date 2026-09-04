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
import contextlib
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

try:
    from iEasyHydroForecast.long_term_horizon_resolver import (
        LongTermHorizonResolverError,
        quarter_horizon_value,
        seasonal_config_name,
        seasonal_horizon_value,
        supported_long_term_modes,
    )
except ModuleNotFoundError:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from iEasyHydroForecast.long_term_horizon_resolver import (
        LongTermHorizonResolverError,
        quarter_horizon_value,
        seasonal_config_name,
        seasonal_horizon_value,
        supported_long_term_modes,
    )

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# API client availability
# ---------------------------------------------------------------------------
try:
    from sapphire_api_client.postprocessing import (
        SapphirePostprocessingClient,
    )
    from sapphire_api_client.preprocessing import (
        SapphirePreprocessingClient,
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
MODE_TO_HORIZONS: dict[str, list[str]] = {
    "PENTAD": ["pentad"],
    "DECAD": ["decade"],
    "BOTH": ["pentad", "decade"],
}

# Short-term forecast models to check.
SHORT_TERM_MODELS = ["LR", "TFT", "TiDE", "TSMixer", "EM", "NE"]

# Quantile columns in ML forecasts.
# q50 is stored as forecasted_discharge in short-term Forecast records.
QUANTILE_COLS = ["q05", "q25", "q75", "q95"]

# Quantile columns in long-term (monthly) forecasts.
LT_QUANTILE_COLS = ["q05", "q10", "q25", "q50", "q75", "q90", "q95"]

# Discharge column name in long-term forecasts.
LT_DISCHARGE_COL = "q"

SEASONAL_ISSUE_MONTHS = (1, 2, 3, 4)

# Default threshold (days) for data freshness checks.
_FRESHNESS_THRESHOLD_DEFAULT = 3

# High limit for API reads so we get enough records for validation.
READ_LIMIT = 5000

# Map single-module names to their default validation target.
MODULE_DEFAULT_TARGET: dict[str, str] = {
    "preprocessing_runoff": "short-term",
    "preprocessing_gateway": "short-term",
    "linear_regression": "short-term",
    "machine_learning": "short-term",
    "postprocessing_forecasts": "short-term",
    "long_term_forecasting": "long-term",
}

# Modules that only produce data on forecast days (not daily).
FORECAST_DAY_MODULES = {
    "linear_regression",
    "machine_learning",
    "postprocessing_forecasts",
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
    data: pd.DataFrame | None = field(default=None, repr=False)
    max_date: str | None = None
    counts: dict = field(default_factory=dict)
    # Marks a configuration failure ("the requested validation could not be
    # performed"), as opposed to an ordinary data finding. Critical rows
    # bypass --module filtering (F1) and force a non-zero exit even under
    # --phase pre (see validate()).
    critical: bool = False


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
# JSON serialisation
# ---------------------------------------------------------------------------


def results_to_json(
    results: list[CheckResult],
    metadata: dict | None = None,
) -> dict:
    """Serialise check results to a JSON-compatible dict.

    Args:
        results: List of check results.
        metadata: Optional metadata (forecast_date, target) stored
            under the ``_meta`` key.

    Returns:
        Dict keyed by check name with status, detail, record_count,
        module, max_date, and counts fields.
    """
    payload: dict = {}
    if metadata:
        payload["_meta"] = metadata
    for r in results:
        payload[r.name] = {
            "status": r.status,
            "detail": r.detail,
            "record_count": r.record_count,
            "module": r.module,
            "max_date": r.max_date,
            "counts": r.counts,
        }
    return payload


# ---------------------------------------------------------------------------
# Baseline / delta mode
# ---------------------------------------------------------------------------


def write_baseline(
    results: list[CheckResult],
    forecast_date: date,
    target: str,
    path: str,
) -> None:
    """Write a baseline JSON snapshot for later delta comparison.

    Args:
        results: Check results to serialise.
        forecast_date: The forecast date for the current run.
        target: Pipeline target (e.g. "short-term").
        path: File path to write the baseline JSON to.
    """
    metadata = {
        "forecast_date": forecast_date.isoformat(),
        "target": target,
    }
    payload = results_to_json(results, metadata=metadata)
    Path(path).write_text(json.dumps(payload, indent=2))


def load_and_validate_baseline(
    path: str,
    forecast_date: date,
    target: str,
) -> dict:
    """Load baseline JSON and validate metadata matches current run.

    Args:
        path: File path to the baseline JSON.
        forecast_date: The forecast date for the current run.
        target: Pipeline target (e.g. "short-term").

    Returns:
        The loaded baseline dict.

    Raises:
        FileNotFoundError: If baseline file does not exist.
        ValueError: If forecast_date or target do not match.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Baseline file not found: {path}")
    baseline = json.loads(p.read_text())
    meta = baseline.get("_meta", {})
    if meta.get("forecast_date") != forecast_date.isoformat():
        raise ValueError(
            f"Baseline forecast_date {meta.get('forecast_date')} "
            f"!= current {forecast_date.isoformat()}"
        )
    if meta.get("target") != target:
        raise ValueError(f"Baseline target {meta.get('target')!r} != current {target!r}")
    return baseline


def compute_deltas(current_json: dict, baseline_json: dict) -> list[str]:
    """Compare record counts between current and baseline runs.

    Args:
        current_json: Current run results as JSON-compatible dict.
        baseline_json: Baseline run results as JSON-compatible dict.

    Returns:
        List of delta report lines. WARN for decreases, INFO for
        increases, nothing for unchanged.
    """
    lines: list[str] = []
    for key in current_json:
        if key == "_meta":
            continue
        cur_count = current_json[key].get("record_count", 0)
        if key not in baseline_json:
            lines.append(f"DELTA INFO: {key} not in baseline (new check)")
            continue
        base_count = baseline_json[key].get("record_count", 0)
        delta = cur_count - base_count
        if delta < 0:
            lines.append(f"DELTA WARN: {key} count decreased from {base_count} to {cur_count}")
        elif delta > 0:
            lines.append(f"DELTA INFO: {key} count increased from {base_count} to {cur_count}")
    return lines


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
        detail = "no records" + (" (may not be configured)" if warn_if_empty else "")
        return CheckResult(
            name=check_name,
            status=status,
            detail=detail,
            module=module,
            data=df,
        )

    result = CheckResult(
        name=check_name,
        status="PASS",
        detail=f"{len(df)} records",
        record_count=len(df),
        module=module,
        data=df,
    )
    if "date" in df.columns and not df.empty:
        raw = pd.to_datetime(df["date"], errors="coerce").max()
        result.max_date = raw.date().isoformat() if pd.notna(raw) else None
    return result


def run_tier1_short_term(
    pre_client,
    post_client,
    forecast_date: date,
    horizon: str,
) -> list[CheckResult]:
    """Run Tier 1 presence checks for a short-term horizon.

    Note: preprocessing_runoff only writes horizon_type="day" for both
    runoff and hydrograph. There are no pentad/decade horizon records
    in the preprocessing API, so we only check the "day" horizon.
    """
    results: list[CheckResult] = []
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
            pre_client,
            "read_runoff",
            "Runoff (day)",
            module="preprocessing_runoff",
            horizon="day",
            start_date=fd,
            end_date=fd,
        )
    )
    results.append(
        check_presence(
            pre_client,
            "read_hydrograph",
            "Hydrograph (day)",
            module="preprocessing_runoff",
            horizon="day",
            start_date=fd,
            end_date=fd,
        )
    )
    results.append(
        check_presence(
            pre_client,
            "read_meteo",
            "Meteo (T)",
            module="preprocessing_gateway",
            meteo_type="T",
            start_date=fd,
            end_date=fd,
        )
    )
    results.append(
        check_presence(
            pre_client,
            "read_meteo",
            "Meteo (P)",
            module="preprocessing_gateway",
            meteo_type="P",
            start_date=fd,
            end_date=fd,
        )
    )
    results.append(
        check_presence(
            pre_client,
            "read_snow",
            "Snow (SWE)",
            module="preprocessing_gateway",
            snow_type="SWE",
            start_date=fd,
            end_date=fd,
            warn_if_empty=True,
        )
    )

    # Postprocessing checks — short-term forecasts per model
    # Query from boundary date to today to find forecasts issued
    # on the most recent boundary day.
    model_modules = {
        "LR": "postprocessing_forecasts",
        "TFT": "postprocessing_forecasts",
        "TiDE": "postprocessing_forecasts",
        "TSMixer": "postprocessing_forecasts",
        "EM": "postprocessing_forecasts",
        "NE": "postprocessing_forecasts",
    }
    for model in SHORT_TERM_MODELS:
        results.append(
            check_presence(
                post_client,
                "read_short_term_forecasts",
                f"Forecasts ({model}, {horizon})",
                module=model_modules.get(model, ""),
                horizon=horizon,
                model=model,
                start_date=bd,
                end_date=fd,
            )
        )

    # LR details — query from boundary to today
    results.append(
        check_presence(
            post_client,
            "read_lr_forecasts",
            f"LR details ({horizon})",
            module="linear_regression",
            horizon=horizon,
            start_date=bd,
            end_date=fd,
        )
    )

    # Skill metrics (not date-filtered — covers all historical periods)
    results.append(
        check_presence(
            post_client,
            "read_skill_metrics",
            f"Skill metrics ({horizon})",
            module="postprocessing_forecasts",
            horizon=horizon,
        )
    )

    return results


def run_tier1_long_term(
    post_client,
    forecast_date: date,
) -> list[CheckResult]:
    """Run Tier 1 presence checks for long-term forecasts."""
    results: list[CheckResult] = []
    fd = str(forecast_date)

    results.append(
        check_presence(
            post_client,
            "read_long_term_forecasts",
            "Long-term forecasts (month)",
            module="long_term_forecasting",
            horizon_type="month",
            start_date=fd,
            end_date=fd,
        )
    )
    results.append(
        check_presence(
            post_client,
            "read_skill_metrics",
            "Monthly skill metrics",
            module="postprocessing_forecasts",
            horizon="month",
        )
    )
    # The quarter and seasonal horizon resolutions are guarded INDEPENDENTLY:
    # each is the piece that can fail due to configuration (missing env var,
    # missing mode config file, invalid JSON, unsupported/unconfigured
    # seasonal mode, ...), and a failure in one must not suppress the checks
    # for the other. The check_presence() calls below can independently
    # raise ValueError from pandas on a malformed API response, which is an
    # unrelated data problem and must not be mislabelled as a horizon
    # configuration failure, so they run outside these try blocks.
    try:
        quarter_hv = quarter_horizon_value()
    except (LongTermHorizonResolverError, OSError, ValueError) as exc:
        # This is not a per-module data finding — it means "the requested
        # validation could not be performed" — so it is marked
        # critical=True: it must survive --module filtering and force a
        # non-zero exit (F1).
        results.append(
            CheckResult(
                name="Long-term horizon configuration (quarter)",
                status="FAIL",
                detail=f"{type(exc).__name__}: {exc}",
                module="long_term_forecasting",
                critical=True,
            )
        )
    else:
        results.append(
            check_presence(
                post_client,
                "read_long_term_forecasts",
                f"Long-term forecasts (quarter hv{quarter_hv})",
                module="postprocessing_forecasts",
                horizon_type="quarter",
                horizon_value=quarter_hv,
                start_date=fd,
                end_date=fd,
            )
        )
        results.append(
            check_presence(
                post_client,
                "read_skill_metrics",
                "Quarterly skill metrics",
                module="postprocessing_forecasts",
                horizon="quarter",
            )
        )

    try:
        season_issue_month, season_hv = _resolved_seasonal_presence_horizon_value(forecast_date)
    except (LongTermHorizonResolverError, OSError, ValueError) as exc:
        results.append(
            CheckResult(
                name="Long-term horizon configuration (seasonal)",
                status="FAIL",
                detail=f"{type(exc).__name__}: {exc}",
                module="long_term_forecasting",
                critical=True,
            )
        )
    else:
        results.append(
            check_presence(
                post_client,
                "read_long_term_forecasts",
                f"Long-term forecasts (season issue {season_issue_month} hv{season_hv})",
                module="postprocessing_forecasts",
                horizon_type="season",
                horizon_value=season_hv,
                start_date=fd,
                end_date=fd,
            )
        )
        results.append(
            check_presence(
                post_client,
                "read_skill_metrics",
                "Seasonal skill metrics",
                module="postprocessing_forecasts",
                horizon="season",
            )
        )

    return results


def _resolved_seasonal_presence_horizon_value(forecast_date: date) -> tuple[int, int]:
    """Return the configured seasonal issue month and deployment lead for a run."""
    modes = set(supported_long_term_modes())
    supported = [
        issue_month
        for issue_month in SEASONAL_ISSUE_MONTHS
        if seasonal_config_name(issue_month) in modes
    ]
    if not supported:
        raise ValueError("No seasonal long-term modes are supported by this deployment.")

    eligible = [issue_month for issue_month in supported if issue_month <= forecast_date.month]
    issue_month = max(eligible or supported)
    return issue_month, seasonal_horizon_value(issue_month)


# ---------------------------------------------------------------------------
# Tier 2 — Data Correctness
# ---------------------------------------------------------------------------


def check_discharge_non_negative(
    results: list[CheckResult],
    discharge_cols: tuple[str, ...] = ("discharge", "forecasted_discharge"),
) -> CheckResult:
    """Verify that discharge / forecasted_discharge values are >= 0.

    Args:
        results: Tier 1 check results containing DataFrames.
        discharge_cols: Column names to check for negative values.

    Returns:
        CheckResult with PASS, FAIL, or SKIP status.
    """
    bad_count = 0
    total = 0
    sources: list[str] = []

    for r in results:
        if r.data is None or r.data.empty:
            continue
        for col in discharge_cols:
            if col in r.data.columns:
                series = pd.to_numeric(r.data[col], errors="coerce")
                negatives = series.dropna() < 0
                n_bad = int(negatives.sum())
                bad_count += n_bad
                total += int(negatives.count())
                if n_bad > 0:
                    neg_vals = series.dropna()[negatives].tolist()
                    sources.append(f"{r.name}.{col}: {n_bad} neg (vals: {neg_vals[:5]})")

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


def check_no_nan_in_forecasts(results: list[CheckResult]) -> CheckResult:
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


def check_quantile_ordering(
    results: list[CheckResult],
    quantile_cols: list[str] | None = None,
) -> CheckResult:
    """Verify quantile columns are non-decreasing row-wise.

    Args:
        results: Tier 1 check results containing DataFrames.
        quantile_cols: Ordered list of quantile column names to check.
            Defaults to QUANTILE_COLS (short-term: q05, q25, q75, q95).

    Returns:
        CheckResult with PASS, FAIL, or SKIP status.
    """
    cols = quantile_cols if quantile_cols is not None else QUANTILE_COLS
    bad_count = 0
    total = 0

    for r in results:
        if r.data is None or r.data.empty:
            continue
        cols_present = [c for c in cols if c in r.data.columns]
        if len(cols_present) < 2:
            continue

        df_q = r.data[cols_present].apply(pd.to_numeric, errors="coerce")
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
    results: list[CheckResult],
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


def check_skill_metric_ranges(results: list[CheckResult]) -> CheckResult:
    """Verify skill metric values are within reasonable ranges.

    NSE > 1.0 and accuracy outside [0, 100] are hard FAILs.
    n_pairs <= 0 is a WARN — new stations legitimately have 0 pairs
    until enough historical data accumulates.
    """
    fail_issues: list[str] = []
    warn_issues: list[str] = []
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
            acc = pd.to_numeric(r.data["accuracy"], errors="coerce").dropna()
            bad_lo = (acc < 0).sum()
            bad_hi = (acc > 100).sum()
            if bad_lo + bad_hi > 0:
                fail_issues.append(f"{bad_lo + bad_hi} records with accuracy outside [0, 100]")

        if "n_pairs" in r.data.columns:
            np_ = pd.to_numeric(r.data["n_pairs"], errors="coerce").dropna()
            bad_np = (np_ <= 0).sum()
            if bad_np > 0:
                warn_issues.append(
                    f"{bad_np} records with n_pairs <= 0 (new stations may lack historical data)"
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


def check_ml_flag_distribution(
    tier1_results: list[CheckResult],
) -> CheckResult:
    """Check ML forecast flag distribution for stuck-flag detection.

    Counts records per flag value across all ML forecast results.
    Emits WARN if all records carry the same flag value, which may
    indicate a stuck flag assignment bug.

    Args:
        tier1_results: Tier 1 check results to inspect.

    Returns:
        CheckResult with PASS, WARN, or SKIP status.
    """
    ml_models = {"TFT", "TiDE", "TSMixer"}
    flag_counts: dict[str, int] = {}
    total = 0

    for r in tier1_results:
        # Only check ML model forecast results
        if not any(m in r.name for m in ml_models):
            continue
        if r.data is None or r.data.empty:
            continue
        if "flag" not in r.data.columns:
            continue
        for flag_val in r.data["flag"].dropna():
            key = str(flag_val)
            flag_counts[key] = flag_counts.get(key, 0) + 1
            total += 1

    if total == 0:
        return CheckResult(
            name="ML flag distribution",
            status="SKIP",
            detail="no ML forecast data with flag column",
        )

    result = CheckResult(
        name="ML flag distribution",
        status="PASS",
        detail=f"{total} records, {len(flag_counts)} distinct flag value(s)",
        record_count=total,
        counts=flag_counts,
    )
    if len(flag_counts) == 1:
        only_flag = next(iter(flag_counts))
        result.status = "WARN"
        result.detail = f"all {total} records have flag={only_flag!r} — possible stuck flag"
    return result


def check_snow_operational_values(
    tier1_results: list[CheckResult],
) -> CheckResult:
    """Detect whether snow records contain only year-2000 norm dates.

    Snow records may be climatological norms (year-2000 dates) or
    operational records (current-year dates). If all dates fall in
    year 2000, this likely indicates the PREPG-003 symptom where the
    operational write window was missed.

    Args:
        tier1_results: Tier 1 check results to inspect.

    Returns:
        CheckResult with PASS, WARN, or SKIP status.
    """
    snow_results = [r for r in tier1_results if "Snow" in r.name]
    all_dates: list[date] = []

    for r in snow_results:
        if r.data is None or r.data.empty:
            continue
        if "date" not in r.data.columns:
            continue
        parsed = pd.to_datetime(r.data["date"], errors="coerce").dropna()
        all_dates.extend(d.date() for d in parsed)

    if not all_dates:
        return CheckResult(
            name="Snow operational values",
            status="SKIP",
            detail="no snow data to check",
        )

    years = {d.year for d in all_dates}
    if years == {2000}:
        return CheckResult(
            name="Snow operational values",
            status="WARN",
            detail=(
                "all snow dates are year-2000 norms — "
                "operational update may have been missed (PREPG-003)"
            ),
            record_count=len(all_dates),
        )
    return CheckResult(
        name="Snow operational values",
        status="PASS",
        detail=f"{len(all_dates)} records, years: {sorted(years)}",
        record_count=len(all_dates),
    )


def check_em_ne_parity(
    tier1_results: list[CheckResult],
    horizon: str,
) -> CheckResult:
    """Check EM and NE record counts match per horizon.

    A mismatch between EM and NE record counts for the same horizon
    indicates an incomplete ensemble and triggers WARN.

    Args:
        tier1_results: Tier 1 check results to inspect.
        horizon: API horizon string (e.g. "pentad").

    Returns:
        CheckResult with PASS, WARN, or SKIP status.
    """
    em_count = 0
    ne_count = 0

    for r in tier1_results:
        if r.data is None or r.data.empty:
            continue
        if f"Forecasts (EM, {horizon})" == r.name:
            em_count = r.record_count
        elif f"Forecasts (NE, {horizon})" == r.name:
            ne_count = r.record_count

    if em_count == 0 and ne_count == 0:
        return CheckResult(
            name=f"EM/NE parity ({horizon})",
            status="SKIP",
            detail="no EM or NE records to compare",
        )

    if em_count != ne_count:
        return CheckResult(
            name=f"EM/NE parity ({horizon})",
            status="WARN",
            detail=(f"EM={em_count} records, NE={ne_count} records — ensemble may be incomplete"),
            counts={"EM": em_count, "NE": ne_count},
        )
    return CheckResult(
        name=f"EM/NE parity ({horizon})",
        status="PASS",
        detail=f"EM and NE both have {em_count} records",
        record_count=em_count,
        counts={"EM": em_count, "NE": ne_count},
    )


def check_data_freshness(
    tier1_results: list[CheckResult],
    forecast_date: date,
) -> CheckResult:
    """Check data freshness against forecast_date.

    For each dataset in tier1_results that has a max_date, compute the
    lag (forecast_date - max_date). Emit WARN if any dataset's max_date
    is more than FRESHNESS_THRESHOLD_DAYS older than forecast_date.

    The threshold defaults to _FRESHNESS_THRESHOLD_DEFAULT and can be
    overridden with the FRESHNESS_THRESHOLD_DAYS environment variable.

    Args:
        tier1_results: Tier 1 check results with max_date populated.
        forecast_date: The forecast date for the current run.

    Returns:
        CheckResult with PASS, WARN, or SKIP status.
    """
    threshold = int(os.environ.get("FRESHNESS_THRESHOLD_DAYS", str(_FRESHNESS_THRESHOLD_DEFAULT)))
    stale: list[str] = []
    checked = 0

    for r in tier1_results:
        if r.max_date is None:
            continue
        try:
            max_dt = date.fromisoformat(r.max_date)
        except ValueError:
            continue
        lag = (forecast_date - max_dt).days
        checked += 1
        if lag > threshold:
            stale.append(f"{r.name}: max_date={r.max_date} (lag={lag}d)")

    if checked == 0:
        return CheckResult(
            name="Data freshness",
            status="SKIP",
            detail="no max_date information available",
        )
    if stale:
        return CheckResult(
            name="Data freshness",
            status="WARN",
            detail=(f"{len(stale)} dataset(s) stale (>{threshold}d): " + "; ".join(stale)),
        )
    return CheckResult(
        name="Data freshness",
        status="PASS",
        detail=f"all {checked} datasets fresh (threshold={threshold}d)",
        record_count=checked,
    )


def run_tier2_long_term(
    tier1_results: list[CheckResult],
) -> list[CheckResult]:
    """Run Tier 2 correctness checks for long-term (monthly) forecasts.

    Applies quantile ordering and discharge non-negative checks using
    long-term column names, plus skill metric range checks.

    Args:
        tier1_results: Tier 1 long-term check results.

    Returns:
        List of CheckResult from long-term correctness checks.
    """
    results: list[CheckResult] = []

    # Long-term uses a wider quantile set
    results.append(check_quantile_ordering(tier1_results, quantile_cols=LT_QUANTILE_COLS))
    # Long-term discharge column is "q"
    results.append(
        check_discharge_non_negative(
            tier1_results,
            discharge_cols=(LT_DISCHARGE_COL, "forecasted_discharge"),
        )
    )
    skill_results = [r for r in tier1_results if "skill" in r.name.lower()]
    results.append(check_skill_metric_ranges(skill_results))

    return results


def run_tier2(
    tier1_results: list[CheckResult],
    horizon: str,
    module_filter: str | None = None,
    forecast_date: date | None = None,
) -> list[CheckResult]:
    """Run Tier 2 correctness checks reusing Tier 1 DataFrames.

    Args:
        tier1_results: Tier 1 check results (possibly filtered by module).
        horizon: API horizon string (e.g. "pentad").
        module_filter: If set, skip cross-module checks like "all models
            present" — same rationale as Tier 3.
        forecast_date: Forecast date for freshness checks.
    """
    results: list[CheckResult] = []

    results.append(check_discharge_non_negative(tier1_results))
    results.append(check_no_nan_in_forecasts(tier1_results))
    results.append(check_quantile_ordering(tier1_results))

    # "All models present" is a cross-module check — skip when
    # validating a single module (same rationale as Tier 3) or when
    # the horizon is not short-term (long-term has different models).
    if not module_filter and horizon in ("pentad", "decade"):
        forecast_results = [r for r in tier1_results if r.name.startswith("Forecasts (")]
        results.append(check_expected_models(forecast_results, horizon))

    # Skill metric checks
    skill_results = [r for r in tier1_results if "skill" in r.name.lower()]
    results.append(check_skill_metric_ranges(skill_results))

    # New checks: ML flag distribution, snow operational dates, EM/NE parity
    results.append(check_ml_flag_distribution(tier1_results))
    results.append(check_snow_operational_values(tier1_results))
    results.append(check_em_ne_parity(tier1_results, horizon))

    # Data freshness check (requires forecast_date)
    if forecast_date is not None:
        results.append(check_data_freshness(tier1_results, forecast_date))

    return results


# ---------------------------------------------------------------------------
# Tier 3 — Cross-module Consistency
# ---------------------------------------------------------------------------


def check_station_codes_match(
    tier1_results: list[CheckResult],
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
            detail=(f"{len(extra)} forecast codes not in runoff: {', '.join(sorted(extra)[:5])}"),
        )
    return CheckResult(
        name="Station codes match",
        status="PASS",
        detail=f"{len(forecast_codes)} codes verified",
    )


def check_dates_consistent(
    tier1_results: list[CheckResult],
) -> CheckResult:
    """Check all models have forecasts for the same (code, date) tuples."""
    model_tuples: dict[str, set] = {}

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
                strict=False,
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


def run_tier3(tier1_results: list[CheckResult]) -> list[CheckResult]:
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
    results: list[CheckResult],
) -> None:
    """Print results in run_locally.sh log style."""
    print(f"\n--- {section} ---")
    for r in results:
        tag = _status_tag(r.status)
        detail = f": {r.detail}" if r.detail else ""
        module = f" [{r.module}]" if r.module else ""
        print(f"{tag} {r.name}{detail}{module}")


def print_summary(all_results: list[CheckResult]) -> int:
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


def resolve_horizons(horizon_arg: str | None, target: str = "short-term") -> list[str]:
    """Resolve horizon argument into a list of API horizon strings.

    For long-term targets, defaults to ["month"].
    For short-term, falls back to SAPPHIRE_PREDICTION_MODE env var,
    then defaults to ["pentad"].
    """
    if horizon_arg:
        return [horizon_arg]

    if target == "long-term":
        return ["month"]

    mode = os.getenv("SAPPHIRE_PREDICTION_MODE", "").upper()
    return MODE_TO_HORIZONS.get(mode, ["pentad"])


def _apply_non_forecast_day_skip(
    results: list[CheckResult],
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
            not r.critical
            and r.status == "FAIL"
            and r.record_count == 0
            and r.module in FORECAST_DAY_MODULES
        ):
            r.status = "SKIP"
            r.detail = f"not a {horizon} forecast day"


def validate(
    target: str,
    forecast_date: date,
    horizons: list[str],
    module_filter: str | None = None,
    output_json_path: str | None = None,
    phase: str | None = None,
    baseline_path: str | None = None,
) -> int:
    """Run all validation tiers and return exit code.

    Args:
        target: Pipeline target ("short-term", "long-term", "daily", "all").
        forecast_date: Date to validate data for.
        horizons: List of API horizon strings (e.g. ["pentad"]).
        module_filter: If set, only show results from this module and
            skip Tier 3 cross-module checks.
        output_json_path: If set, write JSON results to this file path.
        phase: If "pre", write baseline JSON and exit 0. If "post",
            load baseline and compute deltas. None means normal mode.
        baseline_path: File path for the baseline JSON (used with phase).
    """
    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    # --- Create clients ---
    pre_client = SapphirePreprocessingClient(base_url=api_url)
    post_client = SapphirePostprocessingClient(base_url=api_url)

    # --- Readiness checks ---
    all_results: list[CheckResult] = []

    pre_ready = False
    with contextlib.suppress(Exception):
        pre_ready = pre_client.readiness_check()
    if not pre_ready:
        all_results.append(
            CheckResult(
                name="Preprocessing API",
                status="FAIL",
                detail=f"not ready at {api_url}",
            )
        )

    post_ready = False
    with contextlib.suppress(Exception):
        post_ready = post_client.readiness_check()
    if not post_ready:
        all_results.append(
            CheckResult(
                name="Postprocessing API",
                status="FAIL",
                detail=f"not ready at {api_url}",
            )
        )

    # --- Tier 1: Data Presence ---
    tier1_results: list[CheckResult] = []

    if target in ("short-term", "daily", "all"):
        for horizon in horizons:
            if pre_ready and post_ready:
                t1 = run_tier1_short_term(
                    pre_client,
                    post_client,
                    forecast_date,
                    horizon,
                )
            elif post_ready:
                # Can still check postprocessing
                t1 = run_tier1_short_term(
                    None,
                    post_client,
                    forecast_date,
                    horizon,
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
            # Critical rows (F1) always survive module filtering — a
            # configuration failure means the requested validation could
            # not be performed at all, which is not scoped to one module's
            # data and must never be silently dropped.
            t1_lt = [r for r in t1_lt if r.module == module_filter or r.critical]
        # Long-term forecasts only run on specific dates per month;
        # treat empty postprocessing results as SKIP if we have no data.
        _apply_non_forecast_day_skip(
            t1_lt,
            forecast_date,
            "long-term",
        )
        print_results("Tier 1: Data Presence (long-term)", t1_lt)
        tier1_results.extend(t1_lt)

    all_results.extend(tier1_results)

    # --- Tier 2: Data Correctness ---
    if tier1_results:
        # Split LT results from short-term results for separate routing
        lt_results = [r for r in tier1_results if "month" in r.name.lower()]
        if lt_results:
            t2_lt = run_tier2_long_term(lt_results)
            print_results("Tier 2: Data Correctness (long-term)", t2_lt)
            all_results.extend(t2_lt)

        for horizon in horizons:
            horizon_results = [
                r
                for r in tier1_results
                if (
                    horizon in r.name
                    or "day" in r.name.lower()
                    or "Meteo" in r.name
                    or "Snow" in r.name
                    or "skill" in r.name.lower()
                )
                and "month" not in r.name.lower()
            ]
            t2 = run_tier2(horizon_results, horizon, module_filter, forecast_date)
            print_results("Tier 2: Data Correctness", t2)
            all_results.extend(t2)

    # --- Tier 3: Cross-module Consistency ---
    # Skip when validating a single module (cross-module checks don't apply)
    if tier1_results and not module_filter:
        t3 = run_tier3(tier1_results)
        print_results("Tier 3: Cross-module Consistency", t3)
        all_results.extend(t3)

    exit_code = print_summary(all_results)

    # --- JSON output ---
    if output_json_path:
        metadata = {
            "forecast_date": forecast_date.isoformat(),
            "target": target,
        }
        payload = results_to_json(all_results, metadata=metadata)
        Path(output_json_path).write_text(json.dumps(payload, indent=2))

    # --- Phase mode ---
    if phase == "pre" and baseline_path:
        # --phase pre otherwise returns 0 unconditionally, even over
        # ordinary FAIL rows — that pre-existing behaviour is intentionally
        # left unchanged here (tracked as a separate issue). A *critical*
        # row (F1: the requested validation could not be performed at all,
        # e.g. the long-term horizon config could not be resolved) means
        # this run produced an incomplete snapshot, so the baseline must be
        # left untouched rather than overwritten with it — a later
        # --phase post run would otherwise silently compare against a
        # corrupted baseline.
        if any(r.critical for r in all_results):
            print(
                f"[FAIL] baseline at {baseline_path} left unchanged — "
                "validation could not be performed (see critical row above)"
            )
            return 1
        write_baseline(all_results, forecast_date, target, baseline_path)
        return 0

    if phase == "post" and baseline_path:
        try:
            baseline = load_and_validate_baseline(baseline_path, forecast_date, target)
        except (FileNotFoundError, ValueError) as exc:
            print(f"[FAIL] Baseline error: {exc}")
            return 1
        current_json = results_to_json(all_results)
        delta_lines = compute_deltas(current_json, baseline)
        if delta_lines:
            print("\n--- Delta Report ---")
            for line in delta_lines:
                print(line)

    return exit_code


def _load_deployment_env() -> bool:
    """Load the deployment .env file pointed to by ieasyhydroforecast_env_file_path.

    Variables already present in the process environment (e.g. exported by
    run_locally.sh, or set directly in a container) always win — the file is
    loaded with ``override=False``. This keeps a single, consistent source
    of truth for every env var the validator reads downstream.

    Returns:
        True if the environment is ready and validation should proceed.
        False if a [FAIL] line was already printed and main() should
        return 1 without running any checks.
    """
    env_file_path = os.environ.get("ieasyhydroforecast_env_file_path", "")
    if not env_file_path.strip():
        # No pointer set — the container / already-exported case. Accept
        # the process environment as-is.
        return True

    path = Path(env_file_path)
    if not path.is_file() or not os.access(path, os.R_OK):
        # A relative pointer resolves against the *caller's* cwd, which can
        # differ from where this process actually runs (e.g. run_locally.sh
        # validates from the repo root but launches this script from
        # apps/postprocessing_forecasts). Report our own cwd so a mismatch
        # here is diagnosable from the log.
        print(
            f"[FAIL] ieasyhydroforecast_env_file_path={env_file_path} "
            f"does not exist or is not a readable file (cwd={os.getcwd()})"
        )
        return False

    try:
        from dotenv import load_dotenv
    except ImportError:
        # Under run_locally.sh this script is executed by whichever venv
        # invoked it (e.g. apps/postprocessing_forecasts/.venv), not
        # apps/validate_pipeline/.venv, so name the venv actually running
        # this process rather than assuming it is validate_pipeline's own.
        print(
            "[FAIL] python-dotenv is not installed in the interpreter running "
            f"this check ({sys.prefix}) — install it there, and in "
            "apps/validate_pipeline (e.g. `uv sync --all-extras` in both)"
        )
        return False

    try:
        # NOTE: `loaded=True` only means the file parsed and contributed at
        # least one binding — it is not proof the *right* variables (e.g.
        # SAPPHIRE_API_URL) are among them. A file with unrelated bindings
        # still reports success and the validator silently falls back to
        # its defaults for anything it doesn't contain.
        loaded = load_dotenv(path, override=False)
    except Exception as exc:
        # Diagnostics-only boundary at the very start of the process: any
        # failure here (e.g. UnicodeDecodeError from non-UTF-8 bytes) must
        # become a [FAIL] line, never a traceback, per this module's
        # documented exit contract.
        print(
            f"[FAIL] failed to load ieasyhydroforecast_env_file_path={env_file_path}: "
            f"{type(exc).__name__}: {exc}"
        )
        return False

    if not loaded:
        # The operator explicitly named this file — silently proceeding on
        # an empty/comment-only file (or one python-dotenv otherwise
        # declined to load, e.g. PYTHON_DOTENV_DISABLED) risks validating
        # the wrong deployment while reporting success.
        print(
            f"[FAIL] ieasyhydroforecast_env_file_path={env_file_path} loaded no "
            "variables (file may be empty/comment-only, or dotenv loading is "
            "disabled) — refusing to proceed with a possibly-wrong deployment config"
        )
        return False

    return True


def main(argv: list[str] | None = None) -> int:
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
        choices=["pentad", "decade", "month"],
        default=None,
        help="Override horizon (default: from SAPPHIRE_PREDICTION_MODE or target).",
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
    parser.add_argument(
        "--output-json",
        metavar="PATH",
        default=None,
        help="Write all check results as JSON to this file path.",
    )
    parser.add_argument(
        "--phase",
        choices=["pre", "post"],
        default=None,
        help=(
            "Phase mode: 'pre' saves a baseline JSON before the run; "
            "'post' loads baseline and reports count deltas after the run. "
            "Requires --baseline."
        ),
    )
    parser.add_argument(
        "--baseline",
        metavar="PATH",
        default=None,
        help="File path for the baseline JSON (required when --phase is set).",
    )

    args = parser.parse_args(argv)

    # --baseline is required when --phase is set
    if args.phase and not args.baseline:
        parser.error("--baseline PATH is required when --phase is set")

    if not _load_deployment_env():
        return 1

    # If --module given without --target, auto-infer from the mapping
    target = args.target
    if target is None:
        if args.module:
            target = MODULE_DEFAULT_TARGET[args.module]
        else:
            target = "short-term"

    forecast_date = args.forecast_date or date.today()
    horizons = resolve_horizons(args.horizon, target)

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
            f"Validating {args.module} data for {forecast_date} (horizons: {', '.join(horizons)})"
        )
    else:
        print(
            f"Validating {target} pipeline data for "
            f"{forecast_date} (horizons: {', '.join(horizons)})"
        )

    return validate(
        target,
        forecast_date,
        horizons,
        module_filter=args.module,
        output_json_path=args.output_json,
        phase=args.phase,
        baseline_path=args.baseline,
    )


if __name__ == "__main__":
    sys.exit(main())
