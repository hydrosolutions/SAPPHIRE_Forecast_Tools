from __future__ import annotations

import calendar
import math
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd

from forecast_skill_eval.api_readers import DEFAULT_PAGE_SIZE, ReaderResult, read_runoff_observed
from forecast_skill_eval.config import ForecastSkillEvalConfig
from forecast_skill_eval.periods import LONG_TERM_HORIZONS, SHORT_TERM_HORIZONS, normalize_horizon

ObservedTruthKey = tuple[str, int, int]
RunoffReader = Callable[..., ReaderResult | pd.DataFrame]

OBSERVED_MISSING = "observed_missing"
OBSERVED_INCOMPLETE_MONTH = "observed_incomplete_month"
OBSERVED_INCOMPLETE_QUARTER = "observed_incomplete_quarter"
OBSERVED_INCOMPLETE_SEASON = "observed_incomplete_season"


@dataclass(frozen=True)
class ObservedTruthConfig:
    """Observed truth provider options not covered by app config."""

    season_start_month: int = 4
    season_start_day: int = 1
    season_end_month: int = 9
    season_end_day: int = 30

    def __post_init__(self) -> None:
        for name, value in (
            ("season_start_month", self.season_start_month),
            ("season_end_month", self.season_end_month),
        ):
            if not 1 <= value <= 12:
                raise ValueError(f"{name} must be in the range 1..12")
        for name, value in (
            ("season_start_day", self.season_start_day),
            ("season_end_day", self.season_end_day),
        ):
            if not 1 <= value <= 31:
                raise ValueError(f"{name} must be in the range 1..31")
        if self.season_end_month < self.season_start_month:
            raise ValueError("season_end_month must be on or after season_start_month")

    @property
    def season_months(self) -> tuple[int, ...]:
        """Return the configured season months."""
        return tuple(range(self.season_start_month, self.season_end_month + 1))


@dataclass(frozen=True)
class ObservedTruthLedgerEntry:
    """Exclusion reason for an observed truth row or period."""

    reason: str
    code: str | None = None
    period_key: int | None = None
    year: int | None = None


@dataclass(frozen=True)
class ObservedTruthResult:
    """Observed values and non-fatal exclusions for one horizon."""

    values: dict[ObservedTruthKey, float]
    ledger: tuple[ObservedTruthLedgerEntry, ...] = ()


@dataclass
class ObservedTruthProvider:
    """Load observed period-mean discharge keyed for forecast skill scoring."""

    config: ForecastSkillEvalConfig
    client: Any
    runoff_reader: RunoffReader = read_runoff_observed
    observed_config: ObservedTruthConfig = field(default_factory=ObservedTruthConfig)
    limit: int = DEFAULT_PAGE_SIZE

    def observed_for(self, horizon: str) -> ObservedTruthResult:
        """Return observed truth keyed by ``(code, period_key, year)``."""
        normalized_horizon = normalize_horizon(horizon)
        if normalized_horizon in SHORT_TERM_HORIZONS:
            return self._short_term_observed(normalized_horizon)
        if normalized_horizon in LONG_TERM_HORIZONS:
            return self._long_term_observed(normalized_horizon)
        raise ValueError(f"Unsupported horizon: {horizon}")

    def _short_term_observed(self, horizon: str) -> ObservedTruthResult:
        data = self._read_runoff(horizon)
        values: dict[ObservedTruthKey, float] = {}
        ledger: list[ObservedTruthLedgerEntry] = []

        for row in data.to_dict("records"):
            code = _code_or_none(row.get("code"))
            period_key = _int_or_none(row.get("horizon_in_year"))
            year = _year_or_none(row)
            observed_value = _finite_float_or_none(row.get("discharge"))
            if (
                code is None
                or period_key is None
                or period_key == 0
                or year is None
                or observed_value is None
            ):
                ledger.append(
                    ObservedTruthLedgerEntry(
                        OBSERVED_MISSING,
                        code=code,
                        period_key=period_key,
                        year=year,
                    )
                )
                continue

            values[(code, period_key, year)] = observed_value

        return ObservedTruthResult(values, tuple(ledger))

    def _long_term_observed(self, horizon: str) -> ObservedTruthResult:
        daily = _valid_daily_runoff(self._read_runoff("day"))
        if daily.empty:
            return ObservedTruthResult({}, ())

        month_means = _complete_month_means(daily)
        if horizon == "month":
            return self._month_result(daily, month_means)
        if horizon == "quarter":
            return self._quarter_result(daily, month_means)
        return self._season_result(daily, month_means)

    def _month_result(
        self,
        daily: pd.DataFrame,
        month_means: pd.DataFrame,
    ) -> ObservedTruthResult:
        values = {
            (row["code"], int(row["month"]), int(row["year"])): float(row["observed_value"])
            for row in month_means.to_dict("records")
        }
        ledger = [
            ObservedTruthLedgerEntry(
                OBSERVED_INCOMPLETE_MONTH,
                code=row["code"],
                period_key=int(row["month"]),
                year=int(row["year"]),
            )
            for row in _incomplete_months(daily, month_means).to_dict("records")
        ]
        return ObservedTruthResult(values, tuple(ledger))

    def _quarter_result(
        self,
        daily: pd.DataFrame,
        month_means: pd.DataFrame,
    ) -> ObservedTruthResult:
        complete_quarters = _complete_quarter_means(month_means)
        values = {
            (row["code"], int(row["quarter"]), int(row["year"])): float(row["observed_value"])
            for row in complete_quarters.to_dict("records")
        }
        ledger = [
            ObservedTruthLedgerEntry(
                OBSERVED_INCOMPLETE_QUARTER,
                code=row["code"],
                period_key=int(row["quarter"]),
                year=int(row["year"]),
            )
            for row in _incomplete_quarters(daily, month_means).to_dict("records")
        ]
        return ObservedTruthResult(values, tuple(ledger))

    def _season_result(
        self,
        daily: pd.DataFrame,
        month_means: pd.DataFrame,
    ) -> ObservedTruthResult:
        complete_seasons = _complete_season_means(month_means, self.observed_config.season_months)
        values = {
            (row["code"], 1, int(row["year"])): float(row["observed_value"])
            for row in complete_seasons.to_dict("records")
        }
        ledger = [
            ObservedTruthLedgerEntry(
                OBSERVED_INCOMPLETE_SEASON,
                code=row["code"],
                period_key=1,
                year=int(row["year"]),
            )
            for row in _incomplete_seasons(
                daily,
                month_means,
                self.observed_config.season_months,
            ).to_dict("records")
        ]
        return ObservedTruthResult(values, tuple(ledger))

    def _read_runoff(self, horizon: str) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for code in self.config.station_filter or (None,):
            result = self.runoff_reader(
                self.client,
                horizon=horizon,
                code=code,
                start_date=self.config.start_date,
                end_date=self.config.end_date,
                limit=self.limit,
            )
            frame = result.data if isinstance(result, ReaderResult) else result
            frames.append(frame.copy())

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)


def _valid_daily_runoff(data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        return _empty_daily_frame()

    rows: list[dict[str, object]] = []
    for row in data.to_dict("records"):
        code = _code_or_none(row.get("code"))
        row_date = _date_or_none(row.get("date"))
        discharge = _finite_float_or_none(row.get("discharge"))
        if code is None or row_date is None or discharge is None:
            continue
        rows.append({"code": code, "date": row_date, "discharge": discharge})

    if not rows:
        return _empty_daily_frame()

    daily = pd.DataFrame(rows)
    daily = daily.groupby(["code", "date"], as_index=False)["discharge"].mean()
    daily["year"] = daily["date"].map(lambda value: value.year)
    daily["month"] = daily["date"].map(lambda value: value.month)
    daily["quarter"] = daily["month"].map(lambda value: ((value - 1) // 3) + 1)
    return daily


def _complete_month_means(daily: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return _empty_month_frame()

    grouped = (
        daily.groupby(["code", "year", "month"], as_index=False)
        .agg(observed_value=("discharge", "mean"), observed_days=("date", "nunique"))
        .assign(days_in_month=lambda data: data.apply(_days_in_month, axis=1))
    )
    complete = grouped.loc[grouped["observed_days"] >= grouped["days_in_month"] / 2].copy()
    if complete.empty:
        return _empty_month_frame()
    return complete[["code", "year", "month", "observed_value"]].reset_index(drop=True)


def _incomplete_months(daily: pd.DataFrame, month_means: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return _empty_month_key_frame()

    candidates = daily[["code", "year", "month"]].drop_duplicates()
    complete = month_means[["code", "year", "month"]].drop_duplicates()
    incomplete = candidates.merge(
        complete,
        on=["code", "year", "month"],
        how="left",
        indicator=True,
    )
    incomplete = incomplete.loc[incomplete["_merge"].eq("left_only")]
    return incomplete[["code", "year", "month"]].sort_values(["code", "year", "month"])


def _complete_quarter_means(month_means: pd.DataFrame) -> pd.DataFrame:
    if month_means.empty:
        return _empty_quarter_frame()

    with_quarter = month_means.assign(
        quarter=lambda data: ((data["month"].astype(int) - 1) // 3) + 1
    )
    grouped = with_quarter.groupby(["code", "year", "quarter"], as_index=False).agg(
        observed_value=("observed_value", "mean"),
        complete_months=("month", "nunique"),
    )
    complete = grouped.loc[grouped["complete_months"].ge(2)].copy()
    if complete.empty:
        return _empty_quarter_frame()
    return complete[["code", "year", "quarter", "observed_value"]].reset_index(drop=True)


def _incomplete_quarters(daily: pd.DataFrame, month_means: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return _empty_quarter_key_frame()

    candidates = daily[["code", "year", "quarter"]].drop_duplicates()
    complete = _complete_quarter_means(month_means)
    complete_keys = complete[["code", "year", "quarter"]].drop_duplicates()
    incomplete = candidates.merge(
        complete_keys,
        on=["code", "year", "quarter"],
        how="left",
        indicator=True,
    )
    incomplete = incomplete.loc[incomplete["_merge"].eq("left_only")]
    return incomplete[["code", "year", "quarter"]].sort_values(["code", "year", "quarter"])


def _complete_season_means(
    month_means: pd.DataFrame,
    season_months: tuple[int, ...],
) -> pd.DataFrame:
    if month_means.empty:
        return _empty_season_frame()

    season = month_means.loc[month_means["month"].isin(season_months)].copy()
    if season.empty:
        return _empty_season_frame()

    grouped = season.groupby(["code", "year"], as_index=False).agg(
        observed_value=("observed_value", "mean"),
        complete_months=("month", "nunique"),
    )
    min_months = max(1, math.ceil(0.5 * len(season_months)))
    complete = grouped.loc[grouped["complete_months"].ge(min_months)].copy()
    if complete.empty:
        return _empty_season_frame()
    return complete[["code", "year", "observed_value"]].reset_index(drop=True)


def _incomplete_seasons(
    daily: pd.DataFrame,
    month_means: pd.DataFrame,
    season_months: tuple[int, ...],
) -> pd.DataFrame:
    if daily.empty:
        return _empty_season_key_frame()

    season_daily = daily.loc[daily["month"].isin(season_months)]
    if season_daily.empty:
        return _empty_season_key_frame()

    candidates = season_daily[["code", "year"]].drop_duplicates()
    complete = _complete_season_means(month_means, season_months)
    complete_keys = complete[["code", "year"]].drop_duplicates()
    incomplete = candidates.merge(
        complete_keys,
        on=["code", "year"],
        how="left",
        indicator=True,
    )
    incomplete = incomplete.loc[incomplete["_merge"].eq("left_only")]
    return incomplete[["code", "year"]].sort_values(["code", "year"])


def _days_in_month(row: pd.Series) -> int:
    return calendar.monthrange(int(row["year"]), int(row["month"]))[1]


def _code_or_none(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    code = str(value)
    return code if code else None


def _int_or_none(value: object) -> int | None:
    if value is None or pd.isna(value):
        return None
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric) or not np.isfinite(float(numeric)):
        return None
    return int(numeric)


def _finite_float_or_none(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric) or not np.isfinite(float(numeric)):
        return None
    return float(numeric)


def _year_or_none(row: dict[str, object]) -> int | None:
    year = _int_or_none(row.get("year"))
    if year is not None:
        return year
    row_date = _date_or_none(row.get("date"))
    if row_date is not None:
        return row_date.year
    return None


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


def _empty_daily_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["code", "date", "discharge", "year", "month", "quarter"])


def _empty_month_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["code", "year", "month", "observed_value"])


def _empty_month_key_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["code", "year", "month"])


def _empty_quarter_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["code", "year", "quarter", "observed_value"])


def _empty_quarter_key_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["code", "year", "quarter"])


def _empty_season_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["code", "year", "observed_value"])


def _empty_season_key_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["code", "year"])
