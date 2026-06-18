from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd

from forecast_skill_eval.api_readers import (
    DEFAULT_PAGE_SIZE,
    ReaderResult,
    read_forecasts,
    read_hydrograph_norms,
    read_long_forecasts,
    read_runoff_observed,
)
from forecast_skill_eval.classifier import ClassLabel, classify, contingency
from forecast_skill_eval.config import ForecastSkillEvalConfig
from forecast_skill_eval.ledger import ExclusionLedger
from forecast_skill_eval.norms import NormResolver
from forecast_skill_eval.observed_truth import ObservedTruthProvider
from forecast_skill_eval.periods import LONG_TERM_HORIZONS, SHORT_TERM_HORIZONS, normalize_horizon
from forecast_skill_eval.regimes import RegimePolicy, choose_regime_policy, derive_regime

PAIR_COLUMNS = (
    "horizon",
    "code",
    "period_key",
    "year",
    "model",
    "regime",
    "lead",
    "issue_date",
    "forecast_value",
    "observed_value",
    "norm",
    "norm_provenance",
    "fc_class",
    "obs_class",
    "contingency",
)

Reader = Callable[..., ReaderResult | pd.DataFrame]


@dataclass(frozen=True)
class _ForecastInstance:
    code: str | None
    period_key: int | None
    year: int | None
    model: str | None
    lead: int | None
    issue_date: object | None
    forecast_value: float | None


def build_pairs(
    config: ForecastSkillEvalConfig,
    client: Any,
    horizon: str,
) -> tuple[pd.DataFrame, ExclusionLedger]:
    """Join forecasts, observed truth, and norms into classified contingency pairs."""
    normalized_horizon = normalize_horizon(horizon)
    threshold = float(config.threshold)
    start_date = config.start_date
    end_date = config.end_date
    code_filters = tuple(config.station_filter or (None,))
    model_filters = tuple(config.model_filter or (None,))

    ledger = ExclusionLedger()
    hydrograph_reader = _memoized_reader(read_hydrograph_norms)
    runoff_reader = _memoized_reader(read_runoff_observed)

    observed_result = ObservedTruthProvider(
        config=config,
        client=client,
        runoff_reader=runoff_reader,
    ).observed_for(normalized_horizon)
    ledger.merge(observed_result.ledger, stage="observed")

    norm_resolver = NormResolver(
        config=config,
        client=client,
        hydrograph_reader=hydrograph_reader,
        observed_reader=runoff_reader,
    )
    forecasts = _read_forecasts(
        client=client,
        horizon=normalized_horizon,
        code_filters=code_filters,
        model_filters=model_filters,
        start_date=start_date,
        end_date=end_date,
        ledger=ledger,
    )
    regime_policy = choose_regime_policy(
        forecasts,
        operational_start=config.operational_start,
    )

    rows: list[dict[str, object]] = []
    for forecast in forecasts.to_dict("records"):
        instance = _forecast_instance(forecast, normalized_horizon, ledger)
        if instance is None:
            continue
        regime_decision = derive_regime(
            forecast,
            issue_date=instance.issue_date,
            policy=regime_policy,
        )
        if regime_decision.exclude_reason is not None:
            ledger.add(
                stage="pair",
                reason=regime_decision.exclude_reason,
                code=instance.code,
                period_key=instance.period_key,
                year=instance.year,
            )
            continue
        if instance.code is None or instance.period_key is None or instance.year is None:
            ledger.add(
                stage="pair",
                reason="forecast_missing_key",
                code=instance.code,
                period_key=instance.period_key,
                year=instance.year,
            )
            continue
        if instance.forecast_value is None:
            ledger.add(
                stage="pair",
                reason="forecast_missing",
                code=instance.code,
                period_key=instance.period_key,
                year=instance.year,
            )
            continue

        observed_key = (instance.code, instance.period_key, instance.year)
        observed_value = observed_result.values.get(observed_key)
        if observed_value is None:
            ledger.add(
                stage="pair",
                reason="observed_unmatched",
                code=instance.code,
                period_key=instance.period_key,
                year=instance.year,
            )
            continue

        resolution = norm_resolver.resolve(
            normalized_horizon,
            instance.code,
            instance.period_key,
            scored_year=instance.year,
        )
        if resolution.excluded:
            ledger.add(
                stage="norm",
                reason=resolution.reason or "norm_excluded",
                code=instance.code,
                period_key=instance.period_key,
                year=instance.year,
            )
            continue

        fc_class = classify(instance.forecast_value, threshold, resolution.norm)
        obs_class = classify(observed_value, threshold, resolution.norm)
        if fc_class is None or obs_class is None or resolution.norm is None:
            ledger.add(
                stage="pair",
                reason="unclassifiable",
                code=instance.code,
                period_key=instance.period_key,
                year=instance.year,
            )
            continue

        rows.append(
            _pair_row(
                horizon=normalized_horizon,
                instance=instance,
                regime=str(regime_decision.regime),
                observed_value=observed_value,
                norm=resolution.norm,
                norm_provenance=resolution.provenance,
                fc_class=fc_class,
                obs_class=obs_class,
            )
        )

    pairs = pd.DataFrame(rows, columns=PAIR_COLUMNS)
    _attach_regime_attrs(pairs, regime_policy)
    return pairs, ledger


def _read_forecasts(
    *,
    client: Any,
    horizon: str,
    code_filters: tuple[str | None, ...],
    model_filters: tuple[str | None, ...],
    start_date: str | None,
    end_date: str | None,
    ledger: ExclusionLedger,
) -> pd.DataFrame:
    if horizon in SHORT_TERM_HORIZONS:
        return _read_short_forecasts(
            client,
            horizon,
            code_filters,
            model_filters,
            start_date,
            end_date,
            ledger,
        )
    if horizon in LONG_TERM_HORIZONS:
        return _read_long_forecasts(
            client,
            horizon,
            code_filters,
            model_filters,
            start_date,
            end_date,
        )
    raise ValueError(f"Unsupported horizon: {horizon}")


def _read_short_forecasts(
    client: Any,
    horizon: str,
    code_filters: tuple[str | None, ...],
    model_filters: tuple[str | None, ...],
    start_date: str | None,
    end_date: str | None,
    ledger: ExclusionLedger,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for code in code_filters:
        for model in model_filters:
            result = read_forecasts(
                client,
                horizon=horizon,
                code=code,
                model=model,
                target=None,
                start_target=start_date,
                end_target=end_date,
            )
            frames.append(_result_frame(result))
            for _index in range(result.dropped_sentinels):
                ledger.add(stage="pair", reason="forecast_sentinel")
    return _concat_frames(frames)


def _read_long_forecasts(
    client: Any,
    horizon: str,
    code_filters: tuple[str | None, ...],
    model_filters: tuple[str | None, ...],
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for code in code_filters:
        for model in model_filters:
            result = read_long_forecasts(
                client,
                horizon=horizon,
                code=code,
                model=model,
                horizon_value=None,
                valid_from=start_date,
                valid_to=end_date,
            )
            frames.append(_result_frame(result))
    return _concat_frames(frames)


def _forecast_instance(
    row: dict[str, object],
    horizon: str,
    ledger: ExclusionLedger,
) -> _ForecastInstance | None:
    if horizon in SHORT_TERM_HORIZONS:
        return _short_instance(row)
    return _long_instance(row, ledger)


def _short_instance(row: dict[str, object]) -> _ForecastInstance:
    return _ForecastInstance(
        code=_string_or_none(row.get("code")),
        period_key=_int_or_none(row.get("horizon_in_year")),
        year=_year_or_none(row.get("target")),
        model=_string_or_none(row.get("model_type")) or _string_or_none(row.get("model")),
        lead=None,
        issue_date=_plain_value(row.get("date")),
        forecast_value=_finite_float_or_none(row.get("point_value")),
    )


def _long_instance(
    row: dict[str, object],
    ledger: ExclusionLedger,
) -> _ForecastInstance | None:
    instance = _ForecastInstance(
        code=_string_or_none(row.get("code")),
        period_key=_int_or_none(row.get("calendar_period")),
        year=_year_or_none(row.get("valid_from")),
        model=_string_or_none(row.get("model_type")) or _string_or_none(row.get("model")),
        lead=_int_or_none(row.get("horizon_value")),
        issue_date=_plain_value(row.get("date")),
        forecast_value=_finite_float_or_none(row.get("point_value")),
    )
    if _bool_or_none(row.get("is_calendar_aligned")) is not True:
        ledger.add(
            stage="pair",
            reason="forecast_rolling_window",
            code=instance.code,
            period_key=instance.period_key,
            year=instance.year,
        )
        return None
    return instance


def _pair_row(
    *,
    horizon: str,
    instance: _ForecastInstance,
    regime: str,
    observed_value: float,
    norm: float,
    norm_provenance: str | None,
    fc_class: ClassLabel,
    obs_class: ClassLabel,
) -> dict[str, object]:
    return {
        "horizon": horizon,
        "code": instance.code,
        "period_key": instance.period_key,
        "year": instance.year,
        "model": instance.model,
        "regime": regime,
        "lead": instance.lead,
        "issue_date": instance.issue_date,
        "forecast_value": instance.forecast_value,
        "observed_value": observed_value,
        "norm": norm,
        "norm_provenance": norm_provenance,
        "fc_class": fc_class,
        "obs_class": obs_class,
        "contingency": contingency(fc_class, obs_class),
    }


def _attach_regime_attrs(frame: pd.DataFrame, policy: RegimePolicy) -> None:
    frame.attrs["regime_source"] = policy.source
    frame.attrs["regime_reason"] = policy.reason
    frame.attrs["flag_counts"] = dict(policy.flag_counts)


def _memoized_reader(reader: Reader) -> Reader:
    cache: dict[tuple[str, str | None], ReaderResult] = {}

    def memoized(
        client: Any,
        *,
        horizon: str,
        code: str | None,
        start_date: str | None,
        end_date: str | None,
        limit: int = DEFAULT_PAGE_SIZE,
    ) -> ReaderResult:
        key = (normalize_horizon(horizon), None if code is None else str(code))
        if key not in cache:
            cache[key] = _as_reader_result(
                reader(
                    client,
                    horizon=horizon,
                    code=code,
                    start_date=start_date,
                    end_date=end_date,
                    limit=limit,
                )
            )
        cached = cache[key]
        return ReaderResult(cached.data.copy(), cached.dropped_sentinels)

    return memoized


def _as_reader_result(result: ReaderResult | pd.DataFrame) -> ReaderResult:
    if isinstance(result, ReaderResult):
        return ReaderResult(result.data.copy(), result.dropped_sentinels)
    return ReaderResult(pd.DataFrame(result).copy())


def _result_frame(result: ReaderResult | pd.DataFrame) -> pd.DataFrame:
    if isinstance(result, ReaderResult):
        return result.data.copy()
    return pd.DataFrame(result).copy()


def _concat_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame()
    return pd.concat(non_empty, ignore_index=True)


def _string_or_none(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value)
    return text if text else None


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


def _year_or_none(value: object) -> int | None:
    parsed = _date_or_none(value)
    if parsed is None:
        return None
    return parsed.year


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


def _plain_value(value: object) -> object | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _bool_or_none(value: object) -> bool | None:
    if value is None or pd.isna(value):
        return None
    return bool(value)
