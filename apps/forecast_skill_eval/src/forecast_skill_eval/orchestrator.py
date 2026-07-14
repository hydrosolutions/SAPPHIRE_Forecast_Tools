from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from forecast_skill_eval.baselines import (
    build_climatology_baseline,
    build_operational_proxy_baseline,
    build_persistence_baseline,
)
from forecast_skill_eval.config import ForecastSkillEvalConfig
from forecast_skill_eval.contingency import OUTPUT_COLUMNS, POOLED_CODE, count_contingencies
from forecast_skill_eval.continuous_metrics import (
    CONTINUOUS_METRIC_COLUMNS,
    MIN_PAIRS_FOR_VARIANCE_METRICS,
    SEASONAL_VOLUME_COLUMNS,
    SEASONAL_VOLUME_SUMMARY_COLUMNS,
    compute_continuous_metrics,
    compute_seasonal_volume,
)
from forecast_skill_eval.economic_value import (
    ECONOMIC_VALUE_COLUMNS,
    ECONOMIC_VALUE_SUMMARY_COLUMNS,
    compute_economic_value,
)
from forecast_skill_eval.events import (
    _NORM_FACTOR_EVENTS,
    _RP_EVENTS,
    ALL_EVENTS,
    compute_percentile_thresholds,
    compute_return_levels,
    event_by_name,
    reclassify_pairs_for_event,
    reclassify_pairs_for_rp_event,
)
from forecast_skill_eval.ledger import ExclusionLedger
from forecast_skill_eval.metrics import METRIC_COLUMNS, add_metrics
from forecast_skill_eval.pairs import PAIR_COLUMNS, build_pairs
from forecast_skill_eval.prob_baselines import (
    precompute_climatology_crps,
    precompute_persistence_crps,
)
from forecast_skill_eval.prob_metrics import (
    PROB_METRIC_COLUMNS,
    PROB_RELIABILITY_COLUMNS,
    build_prob_reliability,
    compute_probabilistic_metrics,
)


@dataclass(frozen=True)
class HorizonCoverage:
    """Coverage and skip status for one configured horizon."""

    horizon: str
    n_pairs: int
    skipped: bool = False
    skip_reason: str = ""
    regime_source: str = ""
    regime_reason: str = ""


@dataclass(frozen=True)
class ResultsBundle:
    """P6 analysis outputs passed to artifact persistence."""

    pairs: pd.DataFrame
    contingency_metrics: pd.DataFrame
    baselines: pd.DataFrame
    exclusion_ledger: ExclusionLedger
    horizon_summary: tuple[HorizonCoverage, ...]
    # NEW -- defaulted so existing constructors keep working (SAPPHIRE_SKILL_PROB):
    prob_metrics: pd.DataFrame = field(
        default_factory=lambda: pd.DataFrame(columns=PROB_METRIC_COLUMNS)
    )
    prob_reliability: pd.DataFrame = field(
        default_factory=lambda: pd.DataFrame(columns=PROB_RELIABILITY_COLUMNS)
    )
    # NEW -- Phase-4 value metrics, defaulted so existing constructors keep
    # working (gated by SAPPHIRE_SKILL_VALUE, mirroring SAPPHIRE_SKILL_PROB):
    continuous_metrics: pd.DataFrame = field(
        default_factory=lambda: pd.DataFrame(columns=CONTINUOUS_METRIC_COLUMNS)
    )
    seasonal_volume: pd.DataFrame = field(
        default_factory=lambda: pd.DataFrame(columns=SEASONAL_VOLUME_COLUMNS)
    )
    seasonal_volume_summary: pd.DataFrame = field(
        default_factory=lambda: pd.DataFrame(columns=SEASONAL_VOLUME_SUMMARY_COLUMNS)
    )
    economic_value: pd.DataFrame = field(
        default_factory=lambda: pd.DataFrame(columns=ECONOMIC_VALUE_COLUMNS)
    )
    economic_value_summary: pd.DataFrame = field(
        default_factory=lambda: pd.DataFrame(columns=ECONOMIC_VALUE_SUMMARY_COLUMNS)
    )


def run(config: ForecastSkillEvalConfig, client: Any, run_id: str) -> ResultsBundle:
    """Run the full forecast-skill analysis across configured horizons.

    Args:
        config: Resolved forecast-skill evaluation configuration.
        client: Sapphire-like client exposing the P1 reader methods.
        run_id: Caller-provided run identifier. It is accepted for the public P6
            orchestration signature; artifact naming is handled by ``artifacts.py``.

    Returns:
        A result bundle containing all pairs, contingency metrics, baselines,
        merged exclusions, and per-horizon coverage notes.
    """
    _ = run_id
    pair_frames: list[pd.DataFrame] = []
    merged_ledger = ExclusionLedger()
    coverage: list[HorizonCoverage] = []

    for horizon in config.horizons:
        try:
            pairs, ledger = build_pairs(config, client, horizon)
        except Exception as exc:
            merged_ledger.add(stage="horizon", reason="horizon_error")
            coverage.append(
                HorizonCoverage(
                    horizon=horizon,
                    n_pairs=0,
                    skipped=True,
                    skip_reason=f"{type(exc).__name__}: {exc}",
                )
            )
            continue

        merged_ledger.merge(ledger)
        n_pairs = len(pairs)
        if n_pairs == 0:
            coverage.append(
                HorizonCoverage(
                    horizon=horizon,
                    n_pairs=0,
                    skipped=True,
                    skip_reason="empty pairs",
                    regime_source=str(pairs.attrs.get("regime_source", "")),
                    regime_reason=str(pairs.attrs.get("regime_reason", "")),
                )
            )
            continue

        pair_frames.append(pairs)
        coverage.append(
            HorizonCoverage(
                horizon=horizon,
                n_pairs=n_pairs,
                regime_source=str(pairs.attrs.get("regime_source", "")),
                regime_reason=str(pairs.attrs.get("regime_reason", "")),
            )
        )

    all_pairs = _concat_pairs(pair_frames)
    thresholds = compute_percentile_thresholds(all_pairs, config.min_years)

    # Return-period (GEV) events are expensive and opt-in: only fit the GEV
    # distributions when a caller actually requested an rp* event.
    requested_rp_events = tuple(event for event in _RP_EVENTS if event.name in config.events_filter)
    return_levels = (
        compute_return_levels(
            all_pairs,
            return_periods=tuple(event.return_period for event in requested_rp_events),
            min_years=config.min_years,
        )
        if requested_rp_events
        else {}
    )

    contingency = _compute_event_contingencies(
        all_pairs, thresholds, config.events_filter, return_levels
    )
    baselines = _concat_baselines(
        [
            build_climatology_baseline(all_pairs),
            build_operational_proxy_baseline(all_pairs),
            build_persistence_baseline(all_pairs, threshold=float(config.threshold)),
        ]
    )

    # Additive 1.0 × norm baseline set (opt-in, tagged event="below_norm_100").
    # Built from pairs reclassified at value < 1.0 × norm; the existing 0.80
    # rows (tagged event="below_norm") are unchanged and ordered first.
    if "below_norm_100" in config.events_filter:
        event_100 = event_by_name("below_norm_100")
        pairs_100 = reclassify_pairs_for_event(all_pairs, event_100, thresholds)
        if not pairs_100.empty:
            baselines = _concat_baselines(
                [
                    baselines,
                    build_climatology_baseline(pairs_100, event=event_100.name),
                    build_operational_proxy_baseline(pairs_100, event=event_100.name),
                    build_persistence_baseline(
                        pairs_100, threshold=float(event_100.factor), event=event_100.name
                    ),
                ]
            )

    if os.environ.get("SAPPHIRE_SKILL_PROB", "").lower() in {"1", "true"}:
        clim_ref = precompute_climatology_crps(all_pairs)
        persist_ref = precompute_persistence_crps(all_pairs)
        prob_metrics = compute_probabilistic_metrics(
            all_pairs,
            thresholds,
            clim_ref,
            config.events_filter,
            threshold=float(config.threshold),
            persist_ref=persist_ref,
            norm_factor_events=tuple(
                event for event in _NORM_FACTOR_EVENTS if event.name in config.events_filter
            ),
        )
        prob_reliability = build_prob_reliability(all_pairs)
        for code, _horizon in _bandless_groups(all_pairs):
            merged_ledger.add(
                stage="probabilistic",
                reason="no_quantile_band",
                code=code,
            )
    else:
        prob_metrics = pd.DataFrame(columns=PROB_METRIC_COLUMNS)
        prob_reliability = pd.DataFrame(columns=PROB_RELIABILITY_COLUMNS)

    if os.environ.get("SAPPHIRE_SKILL_VALUE", "").lower() in {"1", "true"}:
        continuous_metrics = compute_continuous_metrics(all_pairs)
        seasonal_volume, seasonal_volume_summary = compute_seasonal_volume(
            all_pairs, ledger=merged_ledger
        )
        economic_value, economic_value_summary = compute_economic_value(contingency)
        # Additive 1.0 × norm REV (opt-in).  The contingency frame already holds
        # below_norm_100 rows (from _compute_event_contingencies), so this is a
        # second read of the same frame filtered to that event.  0.80 rows first.
        if "below_norm_100" in config.events_filter:
            long_100, summary_100 = compute_economic_value(contingency, event="below_norm_100")
            if not long_100.empty:
                economic_value = pd.concat([economic_value, long_100], ignore_index=True)
            if not summary_100.empty:
                economic_value_summary = pd.concat(
                    [economic_value_summary, summary_100], ignore_index=True
                )
        for code in _starved_value_groups(continuous_metrics):
            merged_ledger.add(stage="value", reason="min_pairs_gate", code=code)
    else:
        continuous_metrics = pd.DataFrame(columns=CONTINUOUS_METRIC_COLUMNS)
        seasonal_volume = pd.DataFrame(columns=SEASONAL_VOLUME_COLUMNS)
        seasonal_volume_summary = pd.DataFrame(columns=SEASONAL_VOLUME_SUMMARY_COLUMNS)
        economic_value = pd.DataFrame(columns=ECONOMIC_VALUE_COLUMNS)
        economic_value_summary = pd.DataFrame(columns=ECONOMIC_VALUE_SUMMARY_COLUMNS)

    return ResultsBundle(
        pairs=all_pairs,
        contingency_metrics=contingency,
        baselines=baselines,
        exclusion_ledger=merged_ledger,
        horizon_summary=tuple(coverage),
        prob_metrics=prob_metrics,
        prob_reliability=prob_reliability,
        continuous_metrics=continuous_metrics,
        seasonal_volume=seasonal_volume,
        seasonal_volume_summary=seasonal_volume_summary,
        economic_value=economic_value,
        economic_value_summary=economic_value_summary,
    )


def _compute_event_contingencies(
    pairs: pd.DataFrame,
    thresholds: dict[tuple[str, str, int], dict[float, float]],
    events_filter: tuple[str, ...],
    return_levels: dict[tuple[str, str, int], dict[float, float]] | None = None,
) -> pd.DataFrame:
    """Compute contingency metrics for each requested event and tag with event name.

    Runs :func:`count_contingencies` independently for each event in
    *events_filter*, reclassifying pairs as needed, then concatenates the results
    with an ``event`` column added.

    The ``below_norm`` event uses the existing classification embedded in the
    pairs DataFrame; percentile events recompute ``fc_class``/``obs_class`` from
    the empirical thresholds.  Percentile events for which no thresholds are
    available (stations with fewer years than ``min_years``) produce no rows
    (those rows are silently dropped by :func:`reclassify_pairs_for_event`).
    Return-period events (rp5/rp10/rp30/rp100) recompute ``fc_class``/
    ``obs_class`` from the GEV return levels in *return_levels*; groups without
    an available return level (station/period did not meet the ``min_years``
    gate, or the GEV fit failed) likewise produce no rows.

    Args:
        pairs: All-horizons pair DataFrame.
        thresholds: Per-``(code, horizon, period_key)`` percentile thresholds.
        events_filter: Ordered sequence of event names to include in the output.
        return_levels: Per-``(code, horizon, period_key)`` GEV return-level
            mappings as returned by :func:`events.compute_return_levels`, used
            only for the return-period events (rp5/rp10/rp30/rp100). Defaults to
            an empty mapping, which yields no rp* rows -- callers that never
            requested an rp* event may omit this argument entirely.

    Returns:
        Contingency metrics DataFrame with an ``event`` column.  Columns follow
        ``OUTPUT_COLUMNS + METRIC_COLUMNS + ("event",)``.  An empty DataFrame
        with the same schema is returned when no events produce rows.
    """
    return_levels = return_levels or {}
    events_set = frozenset(events_filter)
    frames: list[pd.DataFrame] = []

    for event in (*ALL_EVENTS, *_NORM_FACTOR_EVENTS):
        if event.name not in events_set:
            continue
        event_pairs = reclassify_pairs_for_event(pairs, event, thresholds)
        if event_pairs.empty:
            continue
        ct = add_metrics(count_contingencies(event_pairs))
        ct["event"] = event.name
        frames.append(ct)

    for event in _RP_EVENTS:
        if event.name not in events_set:
            continue
        event_pairs = reclassify_pairs_for_rp_event(pairs, event, return_levels)
        if event_pairs.empty:
            continue
        ct = add_metrics(count_contingencies(event_pairs))
        ct["event"] = event.name
        frames.append(ct)

    if not frames:
        empty_cols = list(OUTPUT_COLUMNS) + list(METRIC_COLUMNS) + ["event"]
        return pd.DataFrame(columns=empty_cols)

    return pd.concat(frames, ignore_index=True)


def _concat_pairs(frames: list[pd.DataFrame]) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=PAIR_COLUMNS)
    return pd.concat(non_empty, ignore_index=True)


def _concat_baselines(frames: list[pd.DataFrame]) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if non_empty:
        return pd.concat(non_empty, ignore_index=True)
    return frames[0].copy()


def _starved_value_groups(continuous_metrics: pd.DataFrame) -> list[str]:
    """Return unique per-station codes whose continuous groups are variance-starved.

    A group is variance-starved when its pair count is below
    ``MIN_PAIRS_FOR_VARIANCE_METRICS`` (so ``kge*``/``nse`` are suppressed to
    ``NaN``).  One ledger entry per unique station ``code`` is logged; the POOLED
    aggregate row is excluded to keep the ledger concise.

    Args:
        continuous_metrics: Reduced continuous-metrics frame.

    Returns:
        Sorted list of station codes with at least one starved group.
    """
    if continuous_metrics.empty or "n_pairs" not in continuous_metrics.columns:
        return []
    n_pairs = pd.to_numeric(continuous_metrics["n_pairs"], errors="coerce")
    starved = continuous_metrics[n_pairs < MIN_PAIRS_FOR_VARIANCE_METRICS]
    if starved.empty:
        return []
    codes = starved.loc[starved["code"].ne(POOLED_CODE), "code"].dropna().unique()
    return sorted(str(code) for code in codes)


def _bandless_groups(pairs: pd.DataFrame) -> list[tuple[str, str]]:
    """Return unique (code, horizon) tuples where no quantile band is available.

    A pair is band-less when ``fc_grid_id`` is absent, empty, or NaN.  One
    ledger entry per unique (code, horizon) combination is logged — not one
    per pair — to keep the ledger concise.

    Args:
        pairs: All-horizons pair DataFrame.

    Returns:
        List of ``(code, horizon)`` tuples with no quantile band.
    """
    if pairs.empty or "fc_grid_id" not in pairs.columns:
        return []
    bandless_mask = pairs["fc_grid_id"].eq("") | pairs["fc_grid_id"].isna()
    bandless = pairs.loc[bandless_mask]
    if bandless.empty:
        return []
    groups = bandless.groupby(["code", "horizon"], sort=True).groups.keys()
    return list(groups)
