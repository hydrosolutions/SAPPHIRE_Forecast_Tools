"""Relative Economic Value (REV) / cost-loss — Richardson (2000) / Wilks (2011).

Phase-4, Part B of the forecast-skill evaluator.  This module scores the
*decision value* of the below-norm forecast for a user whose cost-loss ratio is
``alpha = C / L`` (cost of protective action over loss avoided).  It consumes
the already-computed contingency-count frame (``event == "below_norm"``) so the
grouping and keys are guaranteed consistent with the binary contingency layer —
no re-slicing of pairs is performed here.

Locked formula (verified in the Phase-4 plan)::

    s = base_rate = (TP + FN) / N        (sample event base rate)
    H = pod       = TP / (TP + FN)        (hit rate)
    F = pofd      = FP / (FP + TN)        (false-alarm RATE, NOT FAR)

    V(alpha) = (min(a, s) - F*a*(1-s) + H*s*(1-a) - s) / (min(a, s) - s*a)
    v_max    = H - F                      (analytic peak, NOT max over the grid)
    alpha*   = s                          (Peirce skill score identity)

Edge detection uses the ``base_rate_undefined`` / ``pod_undefined`` /
``pofd_undefined`` boolean columns already produced by
:func:`forecast_skill_eval.metrics.metrics_from_counts`, NOT float equality on
``s``.  Any undefined flag, ``N == 0``, or ``n_pairs < min_pairs`` yields a row
that is still emitted (with counts recorded) but whose ``value``/``v_max`` are
``NaN``.  Values are never clamped: a genuinely skill-negative table produces a
negative ``V(alpha)``.

Self-contained: no orchestrator import, no DB access, no side effects.  Feature
gating (``SAPPHIRE_SKILL_VALUE``) happens at the orchestrator boundary — this
module is flag-agnostic.
"""

from __future__ import annotations

import math
from typing import Final

import numpy as np
import pandas as pd

try:  # pragma: no cover - continuous_metrics lands in a parallel Phase-4 step
    from forecast_skill_eval.continuous_metrics import (
        MIN_PAIRS_FOR_VARIANCE_METRICS,
    )
except ImportError:  # pragma: no cover - fallback keeps Part B importable alone
    MIN_PAIRS_FOR_VARIANCE_METRICS = 10

# 99 interior grid points in (0, 1).  alpha_star = s is appended per group so
# the analytic peak is always sampled even when s < 0.01 or s > 0.99.  Endpoints
# are excluded because V is degenerate at alpha in {0, 1}.
REV_ALPHA_GRID: Final[np.ndarray] = np.round(np.arange(0.01, 1.00, 0.01), 2)

_GROUP_KEYS: Final[tuple[str, ...]] = (
    "horizon",
    "model",
    "regime",
    "season",
    "code",
    "basin",
    "norm_provenance",
    "lead",
)

ECONOMIC_VALUE_COLUMNS: Final[tuple[str, ...]] = (
    *_GROUP_KEYS,
    "event",
    "n_pairs",
    "base_rate_s",
    "hit_rate_H",
    "pofd_F",
    "alpha",
    "value",
)

ECONOMIC_VALUE_SUMMARY_COLUMNS: Final[tuple[str, ...]] = (
    *_GROUP_KEYS,
    "event",
    "n_pairs",
    "base_rate_s",
    "hit_rate_H",
    "pofd_F",
    "v_max",
    "alpha_star",
)


def rev_curve(
    s: float,
    H: float,
    F: float,
    alphas: np.ndarray,
) -> tuple[np.ndarray, float, float]:
    """Richardson (2000) / Wilks (2011) relative-economic-value curve.

    Args:
        s: Sample event base rate ``(TP + FN) / N``.
        H: Hit rate (probability of detection) ``TP / (TP + FN)``.
        F: False-alarm rate (POFD) ``FP / (FP + TN)`` — NOT the false-alarm
            ratio ``FP / (TP + FP)``.
        alphas: Array of cost-loss ratios in ``(0, 1)`` at which to evaluate
            ``V(alpha)``.

    Returns:
        A tuple ``(values, v_max, alpha_star)`` where ``values`` is an array
        aligned to ``alphas``, ``v_max = H - F`` is the analytic maximum (never
        the discrete grid max), and ``alpha_star = s`` is the peak location.

        Any non-finite ``s``/``H``/``F`` input yields an all-``NaN`` ``values``
        array, ``v_max = NaN`` and ``alpha_star = s`` (or ``NaN`` when ``s`` is
        itself non-finite).  Degenerate denominators (``min(a, s) - s*a == 0``)
        yield ``NaN`` at those alphas.  Values are NOT clamped.
    """
    alphas = np.asarray(alphas, dtype=float)

    if not (math.isfinite(s) and math.isfinite(H) and math.isfinite(F)):
        peak = s if math.isfinite(s) else math.nan
        return np.full(alphas.shape, math.nan), math.nan, peak

    min_as = np.minimum(alphas, s)
    numerator = min_as - F * alphas * (1.0 - s) + H * s * (1.0 - alphas) - s
    denominator = min_as - s * alphas

    with np.errstate(divide="ignore", invalid="ignore"):
        values = np.where(denominator == 0.0, math.nan, numerator / denominator)

    return values, H - F, s


def _alpha_grid(s: float) -> np.ndarray:
    """Return the REV alpha grid with ``alpha_star = s`` inserted when finite.

    ``s`` is only appended when it is a finite interior point ``0 < s < 1`` so
    that ``V(alpha_star)`` is guaranteed to be sampled and equal to ``v_max``.
    """
    if math.isfinite(s) and 0.0 < s < 1.0:
        return np.union1d(REV_ALPHA_GRID, np.array([s], dtype=float))
    return REV_ALPHA_GRID.copy()


def compute_economic_value(
    contingency_metrics: pd.DataFrame,
    *,
    event: str = "below_norm",
    min_pairs: int = MIN_PAIRS_FOR_VARIANCE_METRICS,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compute the long ``V(alpha)`` frame and the wide REV summary frame.

    Consumes the contingency-count frame directly (each row is already one
    contingency group), filtered to ``event``.  Reads ``base_rate``/``pod``/
    ``pofd`` as ``s``/``H``/``F`` and the ``*_undefined`` flags for edge
    detection.  A group whose base rate/POD/POFD is undefined, whose ``N == 0``,
    or whose ``n_pairs < min_pairs`` still emits rows (counts recorded) but with
    ``value``/``v_max`` set to ``NaN`` — nothing is ever dropped.

    Args:
        contingency_metrics: Frame with ``OUTPUT_COLUMNS + METRIC_COLUMNS +
            ("event",)`` semantics (see :mod:`forecast_skill_eval.contingency`
            and :mod:`forecast_skill_eval.metrics`).
        event: Event name to score.  Defaults to ``"below_norm"`` — the
            operational allocation decision.
        min_pairs: Minimum group pair count below which REV is suppressed to
            ``NaN``.  Defaults to ``MIN_PAIRS_FOR_VARIANCE_METRICS``.

    Returns:
        ``(economic_value, economic_value_summary)`` — the long per-``(group,
        alpha)`` frame with ``ECONOMIC_VALUE_COLUMNS`` and the wide per-group
        frame with ``ECONOMIC_VALUE_SUMMARY_COLUMNS``.  An empty input (or no
        rows matching ``event``) yields two empty frames with the correct
        columns.
    """
    empty_long = pd.DataFrame(columns=list(ECONOMIC_VALUE_COLUMNS))
    empty_summary = pd.DataFrame(columns=list(ECONOMIC_VALUE_SUMMARY_COLUMNS))

    if contingency_metrics is None or contingency_metrics.empty:
        return empty_long, empty_summary
    if "event" not in contingency_metrics.columns:
        return empty_long, empty_summary

    filtered = contingency_metrics[contingency_metrics["event"] == event]
    if filtered.empty:
        return empty_long, empty_summary

    long_rows: list[dict[str, object]] = []
    summary_rows: list[dict[str, object]] = []

    for record in filtered.to_dict("records"):
        s = _as_float(record.get("base_rate"))
        hit = _as_float(record.get("pod"))
        pofd = _as_float(record.get("pofd"))
        n_pairs = _as_int(record.get("n_pairs"))

        undefined = (
            bool(record.get("base_rate_undefined"))
            or bool(record.get("pod_undefined"))
            or bool(record.get("pofd_undefined"))
        )
        gated = undefined or n_pairs == 0 or n_pairs < min_pairs

        alphas = _alpha_grid(s)
        if gated:
            values = np.full(alphas.shape, math.nan)
            v_max = math.nan
            alpha_star = math.nan
        else:
            values, v_max, alpha_star = rev_curve(s, hit, pofd, alphas)

        keys = {key: record.get(key) for key in _GROUP_KEYS}
        base_fields = {
            **keys,
            "event": event,
            "n_pairs": n_pairs,
            "base_rate_s": s,
            "hit_rate_H": hit,
            "pofd_F": pofd,
        }

        for alpha, value in zip(alphas, values, strict=True):
            long_rows.append({**base_fields, "alpha": float(alpha), "value": float(value)})

        summary_rows.append({**base_fields, "v_max": v_max, "alpha_star": alpha_star})

    economic_value = pd.DataFrame(long_rows).loc[:, list(ECONOMIC_VALUE_COLUMNS)]
    economic_value_summary = pd.DataFrame(summary_rows).loc[:, list(ECONOMIC_VALUE_SUMMARY_COLUMNS)]
    return (
        economic_value.reset_index(drop=True),
        economic_value_summary.reset_index(drop=True),
    )


def compute_economic_value_summary(
    contingency_metrics: pd.DataFrame,
    *,
    event: str = "below_norm",
    min_pairs: int = MIN_PAIRS_FOR_VARIANCE_METRICS,
) -> pd.DataFrame:
    """Return only the wide REV summary frame (``v_max``/``alpha_star`` per group).

    Thin convenience wrapper over :func:`compute_economic_value`.
    """
    _, summary = compute_economic_value(contingency_metrics, event=event, min_pairs=min_pairs)
    return summary


def _as_float(value: object) -> float:
    if value is None:
        return math.nan
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return math.nan


def _as_int(value: object) -> int:
    if value is None:
        return 0
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0
    if math.isnan(result):
        return 0
    return int(result)
