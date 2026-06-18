from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Final, Literal

import pandas as pd

ALL_REGIME: Final = "all"
OPERATIONAL_REGIME: Final = "operational"
HINDCAST_REGIME: Final = "hindcast"
DEFAULT_OPERATIONAL_START: Final = "2024-01-01"
FORECAST_ERROR_FLAG_REASON: Final = "forecast_error_flag"

_MEANINGFUL_FLAG_MIN_SHARE: Final = 0.05
_MEANINGFUL_FLAG_MIN_COUNT: Final = 1000

RegimeSource = Literal["flag", "date"]


@dataclass(frozen=True)
class RegimePolicy:
    """Per-horizon regime policy chosen from the available forecast rows."""

    source: RegimeSource
    operational_start: date
    reason: str
    flag_counts: dict[int, int]


@dataclass(frozen=True)
class RegimeDecision:
    """Regime assignment or exclusion reason for one forecast row."""

    regime: str | None
    exclude_reason: str | None = None


def choose_regime_policy(
    forecasts: pd.DataFrame,
    *,
    operational_start: str | date = DEFAULT_OPERATIONAL_START,
) -> RegimePolicy:
    """Choose flag- or date-based regime assignment for one horizon."""
    start = parse_operational_start(operational_start)
    counts = flag_counts(forecasts)
    operational_count = counts.get(0, 0)
    hindcast_count = counts.get(1, 0)

    if _flags_meaningfully_separate(operational_count, hindcast_count):
        reason = (
            f"flag values 0 and 1 both present at scale (0={operational_count}, 1={hindcast_count})"
        )
        return RegimePolicy(
            source="flag",
            operational_start=start,
            reason=reason,
            flag_counts=counts,
        )

    if counts:
        reason = (
            "flag values do not meaningfully separate operational and hindcast "
            f"(0={operational_count}, 1={hindcast_count}); using issue date"
        )
    else:
        reason = "flag column absent or empty; using issue date"
    return RegimePolicy(
        source="date",
        operational_start=start,
        reason=reason,
        flag_counts=counts,
    )


def derive_regime(
    row: Mapping[str, object],
    *,
    issue_date: object,
    policy: RegimePolicy,
) -> RegimeDecision:
    """Assign one forecast row to an evaluation regime."""
    flag = _flag_value(row.get("flag"))
    if flag == 2:
        return RegimeDecision(regime=None, exclude_reason=FORECAST_ERROR_FLAG_REASON)

    if policy.source == "flag":
        if flag == 0:
            return RegimeDecision(regime=OPERATIONAL_REGIME)
        if flag == 1:
            return RegimeDecision(regime=HINDCAST_REGIME)
        return RegimeDecision(regime=None, exclude_reason="forecast_unknown_flag")

    parsed_issue_date = _date_or_none(issue_date)
    if parsed_issue_date is None:
        return RegimeDecision(regime=None, exclude_reason="forecast_regime_unavailable")
    if parsed_issue_date >= policy.operational_start:
        return RegimeDecision(regime=OPERATIONAL_REGIME)
    return RegimeDecision(regime=HINDCAST_REGIME)


def flag_counts(forecasts: pd.DataFrame) -> dict[int, int]:
    """Return integer flag counts, ignoring missing and non-integer values."""
    if forecasts.empty or "flag" not in forecasts.columns:
        return {}

    counts: dict[int, int] = {}
    for value in forecasts["flag"]:
        flag = _flag_value(value)
        if flag is None:
            continue
        counts[flag] = counts.get(flag, 0) + 1
    return dict(sorted(counts.items()))


def parse_operational_start(value: str | date) -> date:
    """Parse and validate the operational-regime start date."""
    parsed = _date_or_none(value)
    if parsed is None:
        raise ValueError("operational_start must use ISO date format YYYY-MM-DD")
    return parsed


def _flags_meaningfully_separate(operational_count: int, hindcast_count: int) -> bool:
    if operational_count <= 0 or hindcast_count <= 0:
        return False
    minority = min(operational_count, hindcast_count)
    total = operational_count + hindcast_count
    minority_share = minority / total
    return minority_share >= _MEANINGFUL_FLAG_MIN_SHARE or minority >= _MEANINGFUL_FLAG_MIN_COUNT


def _flag_value(value: object) -> int | None:
    if value is None or pd.isna(value):
        return None
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return None
    float_value = float(numeric)
    if not math.isfinite(float_value) or not float_value.is_integer():
        return None
    return int(float_value)


def _date_or_none(value: object) -> date | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()
