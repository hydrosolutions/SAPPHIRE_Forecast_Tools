from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from typing import Any, Final

import numpy as np
import pandas as pd

from forecast_skill_eval.api_readers import (
    DEFAULT_PAGE_SIZE,
    ReaderResult,
    read_forecasts,
    read_hydrograph_norms,
    read_long_forecasts,
    read_lr_forecasts,
    read_runoff_observed,
)
from forecast_skill_eval.classifier import ClassLabel, classify, contingency
from forecast_skill_eval.config import ForecastSkillEvalConfig
from forecast_skill_eval.ledger import ExclusionLedger
from forecast_skill_eval.norms import NormResolver
from forecast_skill_eval.observed_truth import ObservedTruthProvider
from forecast_skill_eval.periods import LONG_TERM_HORIZONS, SHORT_TERM_HORIZONS, normalize_horizon
from forecast_skill_eval.regimes import RegimePolicy, choose_regime_policy, derive_regime

ISSUE_DAY_FILTER_HORIZONS: tuple[str, ...] = ("month",)

# Long-term horizons whose stored ``horizon_value`` is NOT the forecast lead
# (quarter = quarter-of-year, season = constant 1).  When
# ``long_term_derive_lead`` is on, the true lead is derived from the issue and
# target-period-start dates for these horizons.  Month is excluded because its
# stored ``horizon_value`` already equals the lead.
LONG_TERM_DERIVE_LEAD_HORIZONS: tuple[str, ...] = ("quarter", "season")

# Months that belong to the irrigation season (April through September).
_IRRIGATION_MONTHS: Final = frozenset({4, 5, 6, 7, 8, 9})

PAIR_COLUMNS = (
    "horizon",
    "code",
    "basin",
    "period_key",
    "year",
    "model",
    "regime",
    "season",
    "lead",
    "issue_date",
    "forecast_value",
    "observed_value",
    "norm",
    "norm_provenance",
    "fc_class",
    "obs_class",
    "contingency",
    "fc_q05",
    "fc_q10",
    "fc_q25",
    "fc_q50",
    "fc_q75",
    "fc_q90",
    "fc_q95",
    "fc_grid_id",
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
    quantiles: Mapping[float, float] | None = None
    grid_id: str = ""


def build_pairs(
    config: ForecastSkillEvalConfig,
    client: Any,
    horizon: str,
) -> tuple[pd.DataFrame, ExclusionLedger]:
    """Join forecasts, observed truth, and norms into classified contingency pairs."""
    normalized_horizon = normalize_horizon(horizon)
    threshold = float(config.threshold)
    basin_by_prefix = config.basin_by_prefix
    start_date = config.start_date
    end_date = config.end_date
    code_filters = tuple(config.station_filter or (None,))
    model_filters = tuple(config.model_filter or (None,))

    operational_issue_days = config.operational_issue_days

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
        repair_lr=config.short_term_lr_repair_issue_indexing,
    )
    regime_policy = choose_regime_policy(
        forecasts,
        operational_start=config.operational_start,
        regime_source=config.regime_source,
    )

    rows: list[dict[str, object]] = []
    for forecast in forecasts.to_dict("records"):
        instance = _forecast_instance(
            forecast,
            normalized_horizon,
            ledger,
            operational_issue_days,
            derive_lead=config.long_term_derive_lead,
        )
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
        if config.short_term_issue_before_target and normalized_horizon in SHORT_TERM_HORIZONS:
            # A genuine short-term forecast is issued STRICTLY BEFORE its target
            # period starts.  Rows whose issue date lands on/after the period
            # start observed part of their own target (leakage / mislabelled).
            issue_date_date = _date_or_none(instance.issue_date)
            start = _target_period_start(normalized_horizon, instance.period_key, instance.year)
            # Unparseable issue dates are left in place (do NOT drop).
            if issue_date_date is not None and start is not None and issue_date_date >= start:
                ledger.add(
                    stage="pair",
                    reason="forecast_issue_in_target_period",
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
                basin_by_prefix=basin_by_prefix,
                fc_class=fc_class,
                obs_class=obs_class,
            )
        )

    if config.short_term_dedup_one_per_target and normalized_horizon in SHORT_TERM_HORIZONS:
        rows = _dedup_short_term_latest_issue(rows)

    if config.long_term_derive_lead and normalized_horizon in LONG_TERM_DERIVE_LEAD_HORIZONS:
        rows = _dedup_long_term(rows, normalized_horizon)
        if normalized_horizon == "quarter":
            # Stratify quarter output by the target quarter (period_key) so the
            # existing per-lead contingency machinery emits one row per Q1–Q4.
            # The derived lead has already been used for dedup selection above;
            # the "lead" column now carries the target quarter for this horizon.
            for row in rows:
                row["lead"] = row.get("period_key")

    pairs = pd.DataFrame(rows, columns=PAIR_COLUMNS)
    _attach_regime_attrs(pairs, regime_policy)
    return pairs, ledger


def _dedup_short_term_latest_issue(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Collapse re-issues to one row per ``(code, period_key, year, model)``.

    Keeps the row with the LATEST ``issue_date`` for each target/model group,
    which — combined with the issue-before-target filter — is the operational
    decision-time forecast (the latest genuine pre-period issue).  Rows whose
    ``issue_date`` cannot be parsed are treated as the earliest and only survive
    when no dated re-issue exists for the same group.  Deterministic: groups are
    resolved in ascending-issue-date order with original position as tie-break.
    """

    def _sort_key(item: tuple[int, dict[str, object]]) -> tuple[bool, date, int]:
        index, row = item
        parsed = _date_or_none(row.get("issue_date"))
        return (parsed is not None, parsed or date.min, index)

    latest: dict[tuple[object, object, object, object], dict[str, object]] = {}
    for _index, row in sorted(enumerate(rows), key=_sort_key):
        key = (row.get("code"), row.get("period_key"), row.get("year"), row.get("model"))
        latest[key] = row
    return list(latest.values())


def _dedup_long_term(
    rows: list[dict[str, object]],
    horizon: str,
) -> list[dict[str, object]]:
    """Collapse long-term re-issues, horizon-aware, keyed on the target period.

    The dedup key is target-scoped ``(code, period_key, year, model)``, extended
    with the derived ``lead`` for horizons whose ``period_key`` does NOT already
    distinguish the lead:

    * ``quarter`` — ``period_key`` is Q1–Q4, which already separates the four
      targets; the two genuine leads {0, 1} of a single target quarter are the
      re-issue/two-source multiplicity we want to collapse.  Key excludes the
      lead and the SMALLEST derived lead wins (the operational-headline issuance,
      issued closest to the target).  Ties on lead break by the LATEST
      ``issue_date`` then original position.
    * ``season`` — there is one Apr–Sep season per year, so ``period_key`` is the
      constant 1 and does NOT separate leads.  The lead is added to the key so the
      genuine leads 0–3 are all RETAINED (like month); only true re-issues within
      the same lead are collapsed, keeping the LATEST ``issue_date`` (ties → index).

    Deterministic in all cases: original position is the final tie-break.  Mirrors
    :func:`_dedup_short_term_latest_issue` but scoped to the long-term target.
    """
    include_lead_in_key = horizon == "season"

    def _sort_key(item: tuple[int, dict[str, object]]) -> tuple[int, bool, date, int]:
        index, row = item
        parsed = _date_or_none(row.get("issue_date"))
        issue_rank = (parsed is not None, parsed or date.min, index)
        if include_lead_in_key:
            # Leads are separated by the key, so lead is not a selection axis;
            # within a lead, the LATEST issue date wins.
            return (0, *issue_rank)
        lead = _int_or_none(row.get("lead"))
        # Underivable-lead rows are dropped upstream; guard defensively so an
        # unexpected missing lead sorts last (never chosen over a real lead).
        lead_rank = lead if lead is not None else 10**9
        # Ascending sort with last-wins dict assignment: negate the lead so the
        # SMALLEST lead sorts last, then prefer the latest issue date, then index.
        return (-lead_rank, *issue_rank)

    winners: dict[tuple[object, ...], dict[str, object]] = {}
    for _index, row in sorted(enumerate(rows), key=_sort_key):
        key: tuple[object, ...] = (
            row.get("code"),
            row.get("period_key"),
            row.get("year"),
            row.get("model"),
        )
        if include_lead_in_key:
            key = (*key, row.get("lead"))
        winners[key] = row
    return list(winners.values())


def _read_forecasts(
    *,
    client: Any,
    horizon: str,
    code_filters: tuple[str | None, ...],
    model_filters: tuple[str | None, ...],
    start_date: str | None,
    end_date: str | None,
    ledger: ExclusionLedger,
    repair_lr: bool = False,
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
            repair_lr=repair_lr,
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
    repair_lr: bool = False,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    include_lr = _include_lr_forecasts(model_filters)
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
        if include_lr:
            result = read_lr_forecasts(
                client,
                horizon=horizon,
                code=code,
                start_date=start_date,
                end_date=end_date,
                repair_issue_indexing=repair_lr,
            )
            frames.append(_result_frame(result))
            for _index in range(result.dropped_sentinels):
                ledger.add(stage="pair", reason="forecast_sentinel")
    return _concat_frames(frames)


def _include_lr_forecasts(model_filters: tuple[str | None, ...]) -> bool:
    return any(model is None or model == "LR" for model in model_filters)


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
    operational_issue_days: tuple[int, ...] = (),
    *,
    derive_lead: bool = False,
) -> _ForecastInstance | None:
    if horizon in SHORT_TERM_HORIZONS:
        return _short_instance(row)
    return _long_instance(row, ledger, horizon, operational_issue_days, derive_lead=derive_lead)


def _short_instance(row: dict[str, object]) -> _ForecastInstance:
    return _ForecastInstance(
        code=_string_or_none(row.get("code")),
        period_key=_int_or_none(row.get("horizon_in_year")),
        year=_year_or_none(row.get("target")),
        model=_string_or_none(row.get("model_type")) or _string_or_none(row.get("model")),
        lead=None,
        issue_date=_plain_value(row.get("date")),
        forecast_value=_finite_float_or_none(row.get("point_value")),
        quantiles=row.get("quantiles"),
        grid_id=str(row.get("fc_grid_id") or ""),
    )


def _long_instance(
    row: dict[str, object],
    ledger: ExclusionLedger,
    horizon: str = "",
    operational_issue_days: tuple[int, ...] = (),
    *,
    derive_lead: bool = False,
) -> _ForecastInstance | None:
    instance = _ForecastInstance(
        code=_string_or_none(row.get("code")),
        period_key=_int_or_none(row.get("calendar_period")),
        year=_year_or_none(row.get("valid_from")),
        model=_string_or_none(row.get("model_type")) or _string_or_none(row.get("model")),
        lead=_int_or_none(row.get("horizon_value")),
        issue_date=_plain_value(row.get("date")),
        forecast_value=_finite_float_or_none(row.get("point_value")),
        quantiles=row.get("quantiles"),
        grid_id=str(row.get("fc_grid_id") or ""),
    )
    if horizon in ISSUE_DAY_FILTER_HORIZONS and operational_issue_days:
        issue_day = _issue_day_or_none(row.get("date"))
        if issue_day is None or issue_day not in operational_issue_days:
            ledger.add(
                stage="pair",
                reason="forecast_non_operational_issue_day",
                code=instance.code,
                period_key=instance.period_key,
                year=instance.year,
            )
            return None
    if _bool_or_none(row.get("is_calendar_aligned")) is not True:
        ledger.add(
            stage="pair",
            reason="forecast_rolling_window",
            code=instance.code,
            period_key=instance.period_key,
            year=instance.year,
        )
        return None
    if derive_lead and horizon in LONG_TERM_DERIVE_LEAD_HORIZONS:
        derived_lead = _derive_long_lead(
            horizon,
            instance.period_key,
            row.get("date"),
            instance.year,
        )
        if derived_lead is None:
            # No issue date (or otherwise underivable): drop so aggregated-only
            # rows with no lead do not pool with genuine per-lead forecasts.
            ledger.add(
                stage="pair",
                reason="long_forecast_lead_underivable",
                code=instance.code,
                period_key=instance.period_key,
                year=instance.year,
            )
            return None
        instance = replace(instance, lead=derived_lead)
    return instance


def _derive_long_lead(
    horizon: str,
    period_key: int | None,
    issue_date: object,
    year: int | None,
) -> int | None:
    """Derive the forecast lead in months from issue date to target-period start.

    ``lead = (valid_from.year - date.year) * 12 + (valid_from.month - date.month)``
    where ``date`` is the issue date and ``valid_from`` is the target period start.
    The target-period-start month is derived from the horizon and period key
    (quarter Q→month {1:1, 2:4, 3:7, 4:10}; season→April) via :func:`_target_month`,
    and ``valid_from.year`` is the scored ``year`` (year of ``valid_from``).

    Returns ``None`` when the lead cannot be derived (missing issue date, year, or
    an unmappable period key).
    """
    if year is None:
        return None
    target_month = _target_month(horizon, period_key, year) if period_key is not None else None
    if target_month is None:
        return None
    issue = _date_or_none(issue_date)
    if issue is None:
        return None
    return (year - issue.year) * 12 + (target_month - issue.month)


def _pair_row(
    *,
    horizon: str,
    instance: _ForecastInstance,
    regime: str,
    observed_value: float,
    norm: float,
    norm_provenance: str | None,
    basin_by_prefix: Mapping[str, str],
    fc_class: ClassLabel,
    obs_class: ClassLabel,
) -> dict[str, object]:
    q: dict[float, float] = instance.quantiles if isinstance(instance.quantiles, dict) else {}
    return {
        "horizon": horizon,
        "code": instance.code,
        "basin": basin_for_code(instance.code, basin_by_prefix),
        "period_key": instance.period_key,
        "year": instance.year,
        "model": instance.model,
        "regime": regime,
        "season": _season_label(horizon, instance.period_key, instance.year),
        "lead": instance.lead,
        "issue_date": instance.issue_date,
        "forecast_value": instance.forecast_value,
        "observed_value": observed_value,
        "norm": norm,
        "norm_provenance": norm_provenance,
        "fc_class": fc_class,
        "obs_class": obs_class,
        "contingency": contingency(fc_class, obs_class),
        "fc_q05": q.get(0.05, np.nan),
        "fc_q10": q.get(0.10, np.nan),
        "fc_q25": q.get(0.25, np.nan),
        "fc_q50": q.get(0.50, np.nan),
        "fc_q75": q.get(0.75, np.nan),
        "fc_q90": q.get(0.90, np.nan),
        "fc_q95": q.get(0.95, np.nan),
        "fc_grid_id": instance.grid_id,
    }


def basin_for_code(code: object, mapping: Mapping[str, str]) -> str:
    """Return the configured aggregate basin label for a station code."""
    if not isinstance(code, str) or len(code) < 2:
        return "other"
    return mapping.get(code[:2], "other")


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


def _issue_day_or_none(value: object) -> int | None:
    parsed = _date_or_none(value)
    if parsed is None:
        return None
    return parsed.day


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


def _season_label(horizon: str, period_key: int | None, year: int | None) -> str:
    """Return 'irrigation' (Apr–Sep) or 'non_irrigation' for the target period.

    Derives the target calendar month from the horizon type and period key.
    Returns 'non_irrigation' for any horizon/period_key that cannot be resolved.
    """
    if period_key is None or year is None:
        return "non_irrigation"
    month = _target_month(horizon, int(period_key), int(year))
    if month is None:
        return "non_irrigation"
    return "irrigation" if month in _IRRIGATION_MONTHS else "non_irrigation"


def _target_month(horizon: str, period_key: int, year: int) -> int | None:
    """Map horizon + period_key + year to the calendar month (1–12) of the target.

    Returns None when the mapping cannot be determined.
    """
    if horizon == "month":
        return period_key if 1 <= period_key <= 12 else None
    if horizon == "quarter":
        # Q1=Jan–Mar(1), Q2=Apr–Jun(4), Q3=Jul–Sep(7), Q4=Oct–Dec(10)
        return {1: 1, 2: 4, 3: 7, 4: 10}.get(period_key)
    if horizon == "season":
        # The Apr–Sep season is always in the irrigation window.
        return 4
    if horizon == "day":
        # period_key is the day-of-year index (1-based).
        try:
            target = date(year, 1, 1) + timedelta(days=period_key - 1)
            return target.month
        except (ValueError, OverflowError):
            return None
    if horizon == "pentad":
        # SAPPHIRE uses 6 pentads per calendar month (72 per year).
        month = (period_key - 1) // 6 + 1
        return month if 1 <= month <= 12 else None
    if horizon == "decade":
        # SAPPHIRE uses 3 decades per calendar month (36 per year).
        month = (period_key - 1) // 3 + 1
        return month if 1 <= month <= 12 else None
    return None


def _target_period_start(horizon: str, period_key: int, year: int) -> date | None:
    """Return the first calendar day of a short-term target period.

    Mirrors the calendar logic in :func:`_target_month`.  Long-term horizons and
    any invalid period key return ``None`` (the issue-before-target guard is not
    applicable to them).

    Args:
        horizon: Normalized horizon literal (day/pentad/decade for a real result).
        period_key: The in-year period index (``horizon_in_year``).
        year: The target calendar year.

    Returns:
        The target period's first calendar day, or ``None`` when it cannot be
        determined (invalid period key, out-of-range date, or long-term horizon).
    """
    if horizon == "day":
        # period_key is the day-of-year index (1-based).
        try:
            return date(year, 1, 1) + timedelta(days=period_key - 1)
        except (ValueError, OverflowError):
            return None
    if horizon == "pentad":
        # 6 pentads per calendar month (72 per year).
        month = (period_key - 1) // 6 + 1
        if not 1 <= month <= 12:
            return None
        day = [1, 6, 11, 16, 21, 26][(period_key - 1) % 6]
        try:
            return date(year, month, day)
        except (ValueError, OverflowError):
            return None
    if horizon == "decade":
        # 3 decades per calendar month (36 per year).
        month = (period_key - 1) // 3 + 1
        if not 1 <= month <= 12:
            return None
        day = [1, 11, 21][(period_key - 1) % 3]
        try:
            return date(year, month, day)
        except (ValueError, OverflowError):
            return None
    return None
