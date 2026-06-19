from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Final

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
    min_years: int = 10
    operational_start: str = DEFAULT_OPERATIONAL_START
    operational_flags: Sequence[int] = DEFAULT_OPERATIONAL_FLAGS
    hindcast_flags: Sequence[int] = DEFAULT_HINDCAST_FLAGS
    nan_exclude_flags: Sequence[int] = DEFAULT_NAN_EXCLUDE_FLAGS
    error_flags: Sequence[int] = DEFAULT_ERROR_FLAGS

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
