from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from forecast_skill_eval.api_readers import (
    DEFAULT_PAGE_SIZE,
    ReaderResult,
    read_hydrograph_norms,
    read_runoff_observed,
)
from forecast_skill_eval.config import ForecastSkillEvalConfig
from forecast_skill_eval.periods import LONG_TERM_HORIZONS, SHORT_TERM_HORIZONS, normalize_horizon

Reader = Callable[..., Any]

NORM_DUPLICATE_CONFLICT = "norm_duplicate_conflict"
NORM_UNAVAILABLE_LONG_TERM = "norm_unavailable_long_term"
NORM_UNAVAILABLE_LT_MIN_YEARS = "norm_unavailable_lt_min_years"


@dataclass(frozen=True)
class NormResolution:
    """Resolved norm value or exclusion reason for one forecast score row."""

    norm: float | None
    provenance: str | None
    excluded: bool
    reason: str | None


@dataclass(frozen=True)
class NormResolver:
    """Resolve forecast skill norms from stored hydrographs or short-term observations."""

    config: ForecastSkillEvalConfig
    client: Any
    hydrograph_reader: Reader = read_hydrograph_norms
    observed_reader: Reader = read_runoff_observed
    limit: int = DEFAULT_PAGE_SIZE

    def resolve(
        self,
        horizon: str,
        code: str,
        period_key: int,
        scored_year: int,
    ) -> NormResolution:
        """Resolve a norm or a ledger-ready exclusion reason."""
        normalized_horizon = normalize_horizon(horizon)
        normalized_period = _coerce_int(period_key, "period_key")
        normalized_year = _coerce_int(scored_year, "scored_year")

        stored_resolution = self._resolve_stored_norm(
            normalized_horizon,
            code,
            normalized_period,
        )
        if stored_resolution is not None:
            return stored_resolution

        if normalized_horizon in SHORT_TERM_HORIZONS:
            return self._resolve_short_term_calculated(
                normalized_horizon,
                code,
                normalized_period,
                normalized_year,
            )

        return _excluded(NORM_UNAVAILABLE_LONG_TERM)

    def _resolve_stored_norm(
        self,
        horizon: str,
        code: str,
        period_key: int,
    ) -> NormResolution | None:
        rows = _matching_period_rows(
            self._read_hydrograph_norms(horizon, code),
            horizon,
            code,
            period_key,
        )
        if rows.empty:
            return None

        provenance = self.config.provenance_by_horizon[horizon]
        usable_rows = _usable_stored_rows(rows, provenance, self.config.min_years)
        if usable_rows.empty:
            return None

        values = usable_rows["_norm_value"].drop_duplicates()
        if len(values) > 1:
            return _excluded(NORM_DUPLICATE_CONFLICT)

        return NormResolution(
            norm=float(values.iloc[0]),
            provenance=provenance,
            excluded=False,
            reason=None,
        )

    def _resolve_short_term_calculated(
        self,
        horizon: str,
        code: str,
        period_key: int,
        scored_year: int,
    ) -> NormResolution:
        period_means = _observed_period_year_means(
            self._read_observed_runoff(horizon, code),
            horizon,
            code,
            period_key,
        )
        other_years = period_means.loc[period_means["_year"].ne(scored_year)]
        if other_years["_year"].nunique() < self.config.min_years:
            return _excluded(NORM_UNAVAILABLE_LT_MIN_YEARS)

        return NormResolution(
            norm=float(other_years["_discharge"].mean()),
            provenance="calculated",
            excluded=False,
            reason=None,
        )

    def _read_hydrograph_norms(self, horizon: str, code: str) -> pd.DataFrame:
        result = self.hydrograph_reader(
            self.client,
            horizon=horizon,
            code=code,
            start_date=self.config.start_date,
            end_date=self.config.end_date,
            limit=self.limit,
        )
        return _reader_dataframe(result)

    def _read_observed_runoff(self, horizon: str, code: str) -> pd.DataFrame:
        result = self.observed_reader(
            self.client,
            horizon=horizon,
            code=code,
            start_date=self.config.start_date,
            end_date=self.config.end_date,
            limit=self.limit,
        )
        return _reader_dataframe(result)


def _excluded(reason: str) -> NormResolution:
    return NormResolution(norm=None, provenance=None, excluded=True, reason=reason)


def _reader_dataframe(result: Any) -> pd.DataFrame:
    if isinstance(result, ReaderResult):
        return result.data.copy()
    if isinstance(result, pd.DataFrame):
        return result.copy()
    return pd.DataFrame(result)


def _matching_period_rows(
    data: pd.DataFrame,
    horizon: str,
    code: str,
    period_key: int,
) -> pd.DataFrame:
    if data.empty:
        return data.copy()

    rows = _filter_horizon_and_code(data, horizon, code)
    if rows.empty:
        return rows

    period_values = _period_values(rows, horizon)
    if period_values is None:
        return rows.iloc[0:0].copy()

    numeric_periods = pd.to_numeric(period_values, errors="coerce")
    return rows.loc[numeric_periods.eq(period_key)].copy()


def _filter_horizon_and_code(data: pd.DataFrame, horizon: str, code: str) -> pd.DataFrame:
    rows = data.copy()
    for column in ("horizon", "horizon_type"):
        if column in rows.columns:
            rows = rows.loc[rows[column].map(_horizon_label).eq(horizon)]

    if "code" in rows.columns:
        rows = rows.loc[rows["code"].astype(str).eq(code)]

    return rows.copy()


def _horizon_label(value: object) -> str:
    text = str(value).split(".")[-1].strip().lower()
    if text == "decad":
        return "decade"
    return text


def _period_values(data: pd.DataFrame, horizon: str) -> pd.Series | None:
    if "horizon_in_year" in data.columns:
        return data["horizon_in_year"]

    if horizon in SHORT_TERM_HORIZONS:
        return None
    if horizon not in LONG_TERM_HORIZONS:
        return None
    if "date" in data.columns:
        return _calendar_period_from_date(data["date"], horizon)
    return None


def _calendar_period_from_date(values: pd.Series, horizon: str) -> pd.Series:
    dates = pd.to_datetime(values, errors="coerce")
    if horizon == "month":
        return dates.dt.month
    if horizon == "quarter":
        return ((dates.dt.month - 1) // 3) + 1
    return pd.Series(np.where(dates.notna(), 1, np.nan), index=values.index)


def _usable_stored_rows(
    rows: pd.DataFrame,
    provenance: str,
    min_years: int,
) -> pd.DataFrame:
    norm_values = _numeric_column(rows, "norm")
    finite_mask = pd.Series(np.isfinite(norm_values.to_numpy()), index=rows.index)
    usable_mask = finite_mask & norm_values.gt(0)

    if provenance == "calculated":
        count_values = _numeric_column(rows, "count")
        usable_mask &= count_values.notna() & count_values.ge(min_years)

    usable_rows = rows.loc[usable_mask].copy()
    usable_rows["_norm_value"] = norm_values.loc[usable_mask].astype(float)
    return usable_rows


def _observed_period_year_means(
    data: pd.DataFrame,
    horizon: str,
    code: str,
    period_key: int,
) -> pd.DataFrame:
    rows = _matching_period_rows(data, horizon, code, period_key)
    if rows.empty or "discharge" not in rows.columns:
        return _empty_observed_means()

    years = _year_values(rows)
    period_values = pd.to_numeric(_period_values(rows, horizon), errors="coerce")
    discharge = _numeric_column(rows, "discharge")
    code_values = _code_values(rows, code)

    finite_mask = (
        years.notna()
        & period_values.notna()
        & pd.Series(np.isfinite(discharge.to_numpy()), index=rows.index)
    )
    prepared = pd.DataFrame(
        {
            "_code": code_values.loc[finite_mask].astype(str),
            "_period": period_values.loc[finite_mask].astype(int),
            "_year": years.loc[finite_mask].astype(int),
            "_discharge": discharge.loc[finite_mask].astype(float),
        }
    )
    if prepared.empty:
        return _empty_observed_means()

    return prepared.groupby(
        ["_code", "_period", "_year"],
        as_index=False,
    )["_discharge"].mean()


def _year_values(data: pd.DataFrame) -> pd.Series:
    if "year" in data.columns:
        return pd.to_numeric(data["year"], errors="coerce")
    if "date" in data.columns:
        return pd.to_datetime(data["date"], errors="coerce").dt.year
    return pd.Series(np.nan, index=data.index)


def _code_values(data: pd.DataFrame, code: str) -> pd.Series:
    if "code" in data.columns:
        return data["code"]
    return pd.Series(code, index=data.index)


def _numeric_column(data: pd.DataFrame, column: str) -> pd.Series:
    if column not in data.columns:
        return pd.Series(np.nan, index=data.index, dtype="float64")
    return pd.to_numeric(data[column], errors="coerce").astype("float64")


def _empty_observed_means() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "_code": pd.Series(dtype="object"),
            "_period": pd.Series(dtype="int64"),
            "_year": pd.Series(dtype="int64"),
            "_discharge": pd.Series(dtype="float64"),
        }
    )


def _coerce_int(value: int, field_name: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer") from exc
