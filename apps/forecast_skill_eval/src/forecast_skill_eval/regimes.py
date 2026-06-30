from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Final, Literal

import pandas as pd

ALL_REGIME: Final = "all"
OPERATIONAL_REGIME: Final = "operational"
HINDCAST_REGIME: Final = "hindcast"
DEFAULT_OPERATIONAL_START: Final = "2024-01-01"
DEFAULT_OPERATIONAL_FLAGS: Final = (0,)
DEFAULT_HINDCAST_FLAGS: Final = (1, 4)
DEFAULT_NAN_EXCLUDE_FLAGS: Final = (3,)
DEFAULT_ERROR_FLAGS: Final = (2,)
FORECAST_ACTUAL_NAN_FLAG_REASON: Final = "forecast_actual_nan_flag"
FORECAST_ERROR_FLAG_REASON: Final = "forecast_error_flag"

_MEANINGFUL_FLAG_MIN_SHARE: Final = 0.05
_MEANINGFUL_FLAG_MIN_COUNT: Final = 1000

RegimeSource = Literal["flag", "date"]


@dataclass(frozen=True)
class RegimeFlagSets:
    """Forecast flag taxonomy used for regime assignment.

    Attributes:
        operational_flags: Flags that identify operational forecasts.
        hindcast_flags: Flags that identify hindcast or backfilled forecasts.
        nan_exclude_flags: Flags that identify rows excluded due to actual NaN values.
        error_flags: Flags that identify forecast error rows.
    """

    operational_flags: Sequence[object] = DEFAULT_OPERATIONAL_FLAGS
    hindcast_flags: Sequence[object] = DEFAULT_HINDCAST_FLAGS
    nan_exclude_flags: Sequence[object] = DEFAULT_NAN_EXCLUDE_FLAGS
    error_flags: Sequence[object] = DEFAULT_ERROR_FLAGS

    def __post_init__(self) -> None:
        normalized = {
            "operational_flags": _normalize_flag_values(
                self.operational_flags,
                "operational_flags",
            ),
            "hindcast_flags": _normalize_flag_values(self.hindcast_flags, "hindcast_flags"),
            "nan_exclude_flags": _normalize_flag_values(
                self.nan_exclude_flags,
                "nan_exclude_flags",
            ),
            "error_flags": _normalize_flag_values(self.error_flags, "error_flags"),
        }
        _validate_disjoint_flag_sets(normalized)
        for name, values in normalized.items():
            object.__setattr__(self, name, values)


class OperationalStart(str):
    """ISO operational start date carrying regime flag metadata."""

    flag_sets: RegimeFlagSets

    def __new__(
        cls,
        value: str | date,
        flag_sets: RegimeFlagSets | None = None,
    ) -> OperationalStart:
        """Create an operational start string with attached flag sets.

        Args:
            value: ISO operational-regime start date.
            flag_sets: Resolved flag taxonomy to carry through legacy call sites.

        Returns:
            String-like operational start value with ``flag_sets`` metadata.
        """
        if isinstance(value, datetime):
            text = value.date().isoformat()
        elif isinstance(value, date):
            text = value.isoformat()
        else:
            text = str(value)
        instance = str.__new__(cls, text)
        instance.flag_sets = flag_sets or RegimeFlagSets()
        return instance


@dataclass(frozen=True)
class RegimePolicy:
    """Per-horizon regime policy chosen from the available forecast rows."""

    source: RegimeSource
    operational_start: date
    reason: str
    flag_counts: dict[int, int]
    flag_sets: RegimeFlagSets = field(default_factory=RegimeFlagSets)


@dataclass(frozen=True)
class RegimeDecision:
    """Regime assignment or exclusion reason for one forecast row."""

    regime: str | None
    exclude_reason: str | None = None


def choose_regime_policy(
    forecasts: pd.DataFrame,
    *,
    operational_start: str | date = DEFAULT_OPERATIONAL_START,
    flag_sets: RegimeFlagSets | None = None,
    operational_flags: Sequence[object] | None = None,
    hindcast_flags: Sequence[object] | None = None,
    nan_exclude_flags: Sequence[object] | None = None,
    error_flags: Sequence[object] | None = None,
) -> RegimePolicy:
    """Choose flag- or date-based regime assignment for one horizon.

    Args:
        forecasts: Forecast rows for a single horizon.
        operational_start: ISO date or date object for the operational boundary.
        flag_sets: Optional pre-resolved flag taxonomy.
        operational_flags: Optional operational flag override.
        hindcast_flags: Optional hindcast/backfilled flag override.
        nan_exclude_flags: Optional actual-NaN exclusion flag override.
        error_flags: Optional forecast-error flag override.

    Returns:
        Regime policy with the chosen source, boundary, counts, and taxonomy.
    """
    start = parse_operational_start(operational_start)
    resolved_flags = _resolve_flag_sets(
        operational_start=operational_start,
        flag_sets=flag_sets,
        operational_flags=operational_flags,
        hindcast_flags=hindcast_flags,
        nan_exclude_flags=nan_exclude_flags,
        error_flags=error_flags,
    )
    counts = flag_counts(forecasts)
    informative_counts = _selected_flag_counts(
        counts,
        (*resolved_flags.hindcast_flags, *resolved_flags.nan_exclude_flags),
    )
    informative_count = sum(informative_counts.values())

    if _flag_presence_is_meaningful(informative_count, sum(counts.values())):
        reason = (
            "hindcast/nan flag values present at scale "
            f"({_format_flag_counts(informative_counts)}); using flags"
        )
        return RegimePolicy(
            source="flag",
            operational_start=start,
            reason=reason,
            flag_counts=counts,
            flag_sets=resolved_flags,
        )

    if counts:
        reason = (
            "hindcast/nan flag values absent or below scale "
            f"({_format_flag_counts(informative_counts)}); using issue date"
        )
    else:
        reason = "flag column absent or empty; using issue date"
    return RegimePolicy(
        source="date",
        operational_start=start,
        reason=reason,
        flag_counts=counts,
        flag_sets=resolved_flags,
    )


def derive_regime(
    row: Mapping[str, object],
    *,
    issue_date: object,
    policy: RegimePolicy,
) -> RegimeDecision:
    """Assign one forecast row to an evaluation regime.

    Args:
        row: Forecast row.
        issue_date: Forecast issue date used for date fallback.
        policy: Per-horizon regime policy.

    Returns:
        Regime assignment or exclusion reason for the row.
    """
    flag = _flag_value(row.get("flag"))
    flag_sets = policy.flag_sets
    if flag in flag_sets.error_flags:
        return RegimeDecision(regime=None, exclude_reason=FORECAST_ERROR_FLAG_REASON)

    if policy.source == "flag":
        if flag in flag_sets.operational_flags:
            return RegimeDecision(regime=OPERATIONAL_REGIME)
        if flag in flag_sets.hindcast_flags:
            return RegimeDecision(regime=HINDCAST_REGIME)
        if flag in flag_sets.nan_exclude_flags:
            return RegimeDecision(regime=None, exclude_reason=FORECAST_ACTUAL_NAN_FLAG_REASON)
        return _derive_date_regime(issue_date, policy.operational_start)

    return _derive_date_regime(issue_date, policy.operational_start)


def _derive_date_regime(issue_date: object, operational_start: date) -> RegimeDecision:
    parsed_issue_date = _date_or_none(issue_date)
    if parsed_issue_date is None:
        return RegimeDecision(regime=None, exclude_reason="forecast_regime_unavailable")
    if parsed_issue_date >= operational_start:
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


def _resolve_flag_sets(
    *,
    operational_start: str | date,
    flag_sets: RegimeFlagSets | None,
    operational_flags: Sequence[object] | None,
    hindcast_flags: Sequence[object] | None,
    nan_exclude_flags: Sequence[object] | None,
    error_flags: Sequence[object] | None,
) -> RegimeFlagSets:
    resolved = flag_sets or getattr(operational_start, "flag_sets", None) or RegimeFlagSets()
    if (
        operational_flags is None
        and hindcast_flags is None
        and nan_exclude_flags is None
        and error_flags is None
    ):
        return resolved
    return RegimeFlagSets(
        operational_flags=(
            resolved.operational_flags if operational_flags is None else operational_flags
        ),
        hindcast_flags=resolved.hindcast_flags if hindcast_flags is None else hindcast_flags,
        nan_exclude_flags=(
            resolved.nan_exclude_flags if nan_exclude_flags is None else nan_exclude_flags
        ),
        error_flags=resolved.error_flags if error_flags is None else error_flags,
    )


def _flag_presence_is_meaningful(count: int, total: int) -> bool:
    if count <= 0 or total <= 0:
        return False
    share = count / total
    return share >= _MEANINGFUL_FLAG_MIN_SHARE or count >= _MEANINGFUL_FLAG_MIN_COUNT


def _selected_flag_counts(counts: Mapping[int, int], flags: Sequence[int]) -> dict[int, int]:
    selected = set(flags)
    return {flag: counts[flag] for flag in sorted(selected) if flag in counts}


def _format_flag_counts(counts: Mapping[int, int]) -> str:
    if not counts:
        return "none"
    return ", ".join(f"{flag}={count}" for flag, count in sorted(counts.items()))


def _normalize_flag_values(values: Sequence[object], field_name: str) -> tuple[int, ...]:
    if not values:
        raise ValueError(f"{field_name} must not be empty")

    normalized: set[int] = set()
    for value in values:
        flag = _config_flag_value(value, field_name)
        normalized.add(flag)
    return tuple(sorted(normalized))


def _config_flag_value(value: object, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} values must be integer flags")
    if isinstance(value, int):
        flag = value
    elif isinstance(value, str):
        try:
            flag = int(value.strip())
        except ValueError as exc:
            raise ValueError(f"{field_name} values must be integer flags") from exc
    else:
        raise ValueError(f"{field_name} values must be integer flags")
    if flag < 0:
        raise ValueError(f"{field_name} values must be non-negative integer flags")
    return flag


def _validate_disjoint_flag_sets(flag_sets: Mapping[str, tuple[int, ...]]) -> None:
    owners: dict[int, str] = {}
    for name, flags in flag_sets.items():
        for flag in flags:
            owner = owners.get(flag)
            if owner is not None and owner != name:
                raise ValueError(f"flag {flag} is configured in both {owner} and {name}")
            owners[flag] = name


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
