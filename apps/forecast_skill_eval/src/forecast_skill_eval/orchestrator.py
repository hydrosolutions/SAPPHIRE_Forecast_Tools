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
from forecast_skill_eval.contingency import OUTPUT_COLUMNS, count_contingencies
from forecast_skill_eval.events import (
    ALL_EVENTS,
    compute_percentile_thresholds,
    reclassify_pairs_for_event,
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
    contingency = _compute_event_contingencies(all_pairs, thresholds, config.events_filter)
    baselines = _concat_baselines(
        [
            build_climatology_baseline(all_pairs),
            build_operational_proxy_baseline(all_pairs),
            build_persistence_baseline(all_pairs, threshold=float(config.threshold)),
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

    return ResultsBundle(
        pairs=all_pairs,
        contingency_metrics=contingency,
        baselines=baselines,
        exclusion_ledger=merged_ledger,
        horizon_summary=tuple(coverage),
        prob_metrics=prob_metrics,
        prob_reliability=prob_reliability,
    )


def _compute_event_contingencies(
    pairs: pd.DataFrame,
    thresholds: dict[tuple[str, str, int], dict[float, float]],
    events_filter: tuple[str, ...],
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

    Args:
        pairs: All-horizons pair DataFrame.
        thresholds: Per-``(code, horizon, period_key)`` percentile thresholds.
        events_filter: Ordered sequence of event names to include in the output.

    Returns:
        Contingency metrics DataFrame with an ``event`` column.  Columns follow
        ``OUTPUT_COLUMNS + METRIC_COLUMNS + ("event",)``.  An empty DataFrame
        with the same schema is returned when no events produce rows.
    """
    events_set = frozenset(events_filter)
    frames: list[pd.DataFrame] = []

    for event in ALL_EVENTS:
        if event.name not in events_set:
            continue
        event_pairs = reclassify_pairs_for_event(pairs, event, thresholds)
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
