from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from forecast_skill_eval.baselines import (
    build_climatology_baseline,
    build_operational_proxy_baseline,
)
from forecast_skill_eval.config import ForecastSkillEvalConfig
from forecast_skill_eval.contingency import count_contingencies
from forecast_skill_eval.ledger import ExclusionLedger
from forecast_skill_eval.metrics import add_metrics
from forecast_skill_eval.pairs import PAIR_COLUMNS, build_pairs


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
    contingency = add_metrics(count_contingencies(all_pairs))
    baselines = _concat_baselines(
        [
            build_climatology_baseline(all_pairs),
            build_operational_proxy_baseline(all_pairs),
        ]
    )
    return ResultsBundle(
        pairs=all_pairs,
        contingency_metrics=contingency,
        baselines=baselines,
        exclusion_ledger=merged_ledger,
        horizon_summary=tuple(coverage),
    )


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
