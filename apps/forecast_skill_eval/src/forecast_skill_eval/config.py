from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Final

from forecast_skill_eval.events import ALL_EVENT_NAMES, VALID_EVENTS
from forecast_skill_eval.periods import normalize_horizon
from forecast_skill_eval.regimes import (
    DEFAULT_ERROR_FLAGS,
    DEFAULT_HINDCAST_FLAGS,
    DEFAULT_NAN_EXCLUDE_FLAGS,
    DEFAULT_OPERATIONAL_FLAGS,
    DEFAULT_OPERATIONAL_START,
    OperationalStart,
    RegimeFlagSets,
    parse_operational_start,
)

DEFAULT_BASE_URL: Final = "http://localhost:8000"
DEFAULT_HORIZONS: Final = ("day", "pentad", "decade", "month", "quarter", "season")
DEFAULT_PROVENANCE: Final = {
    "decade": "official",
    "month": "official",
    "quarter": "aggregated_from_monthly",
    "season": "aggregated_from_monthly",
    "pentad": "calculated",
    "day": "calculated",
}
DEFAULT_BASINS_BY_PREFIX: Final = {
    "15": "chu_kyrgyz",
    "16": "syr_darya",
    "17": "amu_darya",
}
# Empty tuple = alignment-only filtering (the recommended default).
# `is_calendar_aligned` already separates the operational calendar-month product
# from the erroneous rolling-31-day product, so no additional day-of-month guard
# is needed by default.  A non-empty set opts into exact day-of-month filtering
# for the "month" horizon; this is NOT org-general (Tajik/Uzbek issue-day
# configurations differ and are unverified) and is intended as a future
# config-driven per-org/per-lead tolerance filter only.
DEFAULT_OPERATIONAL_ISSUE_DAYS: Final = ()
# Default event set: all five binary events.  Override via --events CLI flag or
# the events_filter config field to restrict output to a subset.
DEFAULT_EVENTS: Final[tuple[str, ...]] = ALL_EVENT_NAMES


@dataclass(frozen=True)
class ForecastSkillEvalConfig:
    """Typed configuration captured at app startup."""

    base_url: str = DEFAULT_BASE_URL
    threshold: float = 0.80
    horizons: Sequence[str] = DEFAULT_HORIZONS
    model_filter: Sequence[str] | None = None
    station_filter: Sequence[str] | None = None
    start_date: str | None = None
    end_date: str | None = None
    output_dir: Path = Path("artifacts")
    provenance_by_horizon: Mapping[str, str] = field(
        default_factory=lambda: DEFAULT_PROVENANCE.copy()
    )
    basin_by_prefix: Mapping[str, str] = field(
        default_factory=lambda: DEFAULT_BASINS_BY_PREFIX.copy()
    )
    min_years: int = 10
    operational_start: str = DEFAULT_OPERATIONAL_START
    operational_flags: Sequence[int] = DEFAULT_OPERATIONAL_FLAGS
    hindcast_flags: Sequence[int] = DEFAULT_HINDCAST_FLAGS
    nan_exclude_flags: Sequence[int] = DEFAULT_NAN_EXCLUDE_FLAGS
    error_flags: Sequence[int] = DEFAULT_ERROR_FLAGS
    operational_issue_days: Sequence[int] = DEFAULT_OPERATIONAL_ISSUE_DAYS
    events_filter: Sequence[str] = DEFAULT_EVENTS
    season_filter: str = "all"
    regime_source: str = "auto"
    # Short-term (day/pentad/decade) forecast-pairing correctness gates.
    #   * short_term_issue_before_target: drop short-term forecasts issued on or
    #     after their target period start (observation leakage / mislabelled rows).
    #     Defaults False so the default pairing behaviour is byte-identical.
    #   * short_term_dedup_one_per_target: keep only the latest genuine pre-period
    #     issue per (code, period_key, year, model) for short-term horizons.
    #     Defaults True (D4/#7) so the default eval run matches the operational
    #     one-pair-per-target convention; pass False explicitly to opt out.
    short_term_issue_before_target: bool = False
    short_term_dedup_one_per_target: bool = True
    #   * short_term_lr_repair_issue_indexing: correct historical issue-indexed LR
    #     pentad/decade forecasts to target-indexed at read time.  Default False so
    #     the default read behaviour is byte-identical.
    short_term_lr_repair_issue_indexing: bool = False
    # Long-term (quarter/season) lead-handling correctness gate.  Default False so
    # the default long-term pairing behaviour is byte-identical.
    #   * long_term_derive_lead: for quarter/season forecasts, derive the true
    #     forecast lead (months from issue date to target-period start) instead of
    #     using the overloaded stored ``horizon_value`` (quarter-of-year / constant
    #     1).  Under the flag, quarter/season pairs are also deduped to one forecast
    #     per (code, period_key, year, model) keeping the smallest derived lead, and
    #     the quarter ``lead`` output is set to the target quarter (period_key) so
    #     contingency stratifies per target quarter (Q1–Q4).  Month is unchanged.
    long_term_derive_lead: bool = False

    def __post_init__(self) -> None:
        if not self.base_url:
            raise ValueError("base_url must not be empty")
        if not 0 < self.threshold <= 1:
            raise ValueError("threshold must be in the range (0, 1]")
        if self.min_years < 1:
            raise ValueError("min_years must be at least 1")

        normalized_horizons = tuple(normalize_horizon(horizon) for horizon in self.horizons)
        if not normalized_horizons:
            raise ValueError("horizons must not be empty")

        object.__setattr__(self, "horizons", normalized_horizons)
        object.__setattr__(self, "model_filter", _freeze_optional_strings(self.model_filter))
        object.__setattr__(self, "station_filter", _freeze_optional_strings(self.station_filter))
        object.__setattr__(self, "output_dir", Path(self.output_dir))
        object.__setattr__(
            self,
            "provenance_by_horizon",
            _normalize_provenance(self.provenance_by_horizon),
        )
        object.__setattr__(
            self,
            "basin_by_prefix",
            _normalize_basin_by_prefix(self.basin_by_prefix),
        )
        _validate_date_range(self.start_date, self.end_date)
        parse_operational_start(self.operational_start)
        flag_sets = RegimeFlagSets(
            operational_flags=self.operational_flags,
            hindcast_flags=self.hindcast_flags,
            nan_exclude_flags=self.nan_exclude_flags,
            error_flags=self.error_flags,
        )
        object.__setattr__(
            self,
            "operational_start",
            OperationalStart(self.operational_start, flag_sets),
        )
        object.__setattr__(self, "operational_flags", flag_sets.operational_flags)
        object.__setattr__(self, "hindcast_flags", flag_sets.hindcast_flags)
        object.__setattr__(self, "nan_exclude_flags", flag_sets.nan_exclude_flags)
        object.__setattr__(self, "error_flags", flag_sets.error_flags)
        object.__setattr__(
            self,
            "operational_issue_days",
            _normalize_operational_issue_days(self.operational_issue_days),
        )
        _validate_events_filter(self.events_filter)
        object.__setattr__(
            self,
            "events_filter",
            tuple(self.events_filter),
        )
        _validate_season_filter(self.season_filter)
        _validate_regime_source(self.regime_source)


def _freeze_optional_strings(values: Sequence[str] | None) -> tuple[str, ...] | None:
    if values is None:
        return None
    return tuple(values)


def _normalize_provenance(overrides: Mapping[str, str]) -> dict[str, str]:
    provenance = DEFAULT_PROVENANCE.copy()
    for horizon, source in overrides.items():
        normalized_horizon = normalize_horizon(horizon)
        if not source:
            raise ValueError("provenance values must not be empty")
        provenance[normalized_horizon] = source
    return provenance


def _normalize_basin_by_prefix(mapping: Mapping[str, str]) -> dict[str, str]:
    basin_by_prefix: dict[str, str] = {}
    for prefix, basin in mapping.items():
        if not isinstance(prefix, str) or not prefix:
            raise ValueError("basin prefixes must be non-empty strings")
        if not isinstance(basin, str) or not basin:
            raise ValueError("basin labels must be non-empty strings")
        basin_by_prefix[prefix] = basin
    return basin_by_prefix


def _validate_date_range(start_date: str | None, end_date: str | None) -> None:
    parsed_start = _parse_date(start_date, "start_date")
    parsed_end = _parse_date(end_date, "end_date")
    if parsed_start is not None and parsed_end is not None and parsed_start > parsed_end:
        raise ValueError("start_date must be on or before end_date")


def _parse_date(value: str | None, field_name: str) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must use ISO date format YYYY-MM-DD") from exc


def _normalize_operational_issue_days(days: Sequence[object]) -> tuple[int, ...]:
    if not days:
        return ()
    normalized: list[int] = []
    for day in days:
        if not isinstance(day, int):
            raise ValueError(f"operational_issue_days values must be integers, got {day!r}")
        if not 1 <= day <= 31:
            raise ValueError(f"operational_issue_days values must be in 1..31, got {day}")
        normalized.append(day)
    return tuple(sorted(set(normalized)))


_VALID_SEASON_FILTERS: Final = ("all", "irrigation", "non_irrigation")


def _validate_season_filter(value: str) -> None:
    if value not in _VALID_SEASON_FILTERS:
        raise ValueError(f"season_filter must be one of {_VALID_SEASON_FILTERS!r}, got {value!r}")


_VALID_REGIME_SOURCES: Final = ("auto", "flag", "date")


def _validate_regime_source(value: str) -> None:
    if value not in _VALID_REGIME_SOURCES:
        raise ValueError(f"regime_source must be one of {_VALID_REGIME_SOURCES!r}, got {value!r}")


def _validate_events_filter(events: Sequence[str]) -> None:
    """Validate the events_filter sequence.

    Raises:
        ValueError: If the sequence is empty or contains unrecognised event names.
    """
    if not events:
        raise ValueError("events_filter must not be empty; provide at least one event name")
    unknown = sorted(str(e) for e in events if str(e) not in VALID_EVENTS)
    if unknown:
        raise ValueError(
            f"events_filter contains unknown event names: {unknown}. "
            f"Valid events: {sorted(VALID_EVENTS)}"
        )
