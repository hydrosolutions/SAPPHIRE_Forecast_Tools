"""Canonical CRPS (Continuous Ranked Probability Score) estimator.

Shared by ``postprocessing_forecasts`` and ``forecast_skill_eval`` so both apps
compute IDENTICAL CRPS values from quantile forecasts. Prior to this module,
each app had its own implementation and they disagreed by roughly a factor of
2 (postprocessing_forecasts omitted the factor-2 term and the flat-tail
terms). See design decision D3 and milestone M4 in
``doc/plans/postprocessing_skill_correctness_design.md``.

CRPS estimator (textbook):
    CRPS = 2 * integral_0^1 pinball_loss(tau, obs) dtau

Approximated by:
    - trapezoidal integration between quantile grid nodes;
    - explicit flat-tail terms on [0, tau_min] and [tau_max, 1], treating the
      quantile function as flat (extrapolated) beyond the observed grid. This
      penalises an observation that falls outside the forecast band instead
      of rewarding an artificially narrow ("overconfident") band.

For a degenerate/point quantile band (every node equal to the same value q),
this estimator reduces exactly to CRPS = |obs - q|, independent of the
specific quantile levels used — a useful sanity check.

NaN handling (fixes issue #6 — "NaN-quantile poisoning"):
    - A single (quantile band, observation) pair is scored via
      :func:`crps_single`. Non-finite quantile *nodes* are dropped before
      scoring (isotonic-repair keeps the remaining finite nodes and enforces
      a non-decreasing quantile function); if fewer than 2 finite nodes
      remain, or the observation itself is non-finite, the pair scores NaN.
    - The batched aggregator :func:`crps_from_quantiles` (N observations x K
      quantile levels -> scalar mean) scores each row independently via
      :func:`crps_single` and reduces with a NaN-aware mean. A single bad row
      (NaN observation, or a quantile row with too few finite levels) is
      excluded from the mean instead of nulling the entire group's CRPS, as
      the previous postprocessing_forecasts implementation did.
"""

from __future__ import annotations

import math
from collections.abc import Sequence

import numpy as np


def _isotonic_repair(
    levels: Sequence[float], quantiles: Sequence[float]
) -> tuple[list[float], list[float]]:
    """Drop non-finite nodes, sort by level, enforce a non-decreasing quantile
    function via cumulative max.

    This mirrors ``forecast_skill_eval.prob_metrics.isotonic_band``'s node
    repair algorithm. It is intentionally duplicated (not imported) so this
    shared library — which ``postprocessing_forecasts`` also depends on — has
    no reverse dependency on ``forecast_skill_eval``.

    Returns:
        Tuple of (repaired_levels, repaired_quantiles), both sorted by level.
        Empty lists when there are no finite node pairs.
    """
    node_pairs: list[tuple[float, float]] = []
    for lv, qv in zip(levels, quantiles, strict=False):
        try:
            lv_f = float(lv)
            qv_f = float(qv)
        except (TypeError, ValueError):
            continue
        if math.isfinite(lv_f) and math.isfinite(qv_f):
            node_pairs.append((lv_f, qv_f))

    if not node_pairs:
        return [], []

    node_pairs.sort(key=lambda x: x[0])
    sorted_levels = [p[0] for p in node_pairs]
    sorted_quantiles = [p[1] for p in node_pairs]

    repaired: list[float] = [sorted_quantiles[0]]
    for qv in sorted_quantiles[1:]:
        running_max = repaired[-1]
        repaired.append(qv if qv >= running_max else running_max)

    return sorted_levels, repaired


def crps_single(
    levels: Sequence[float],
    quantiles: Sequence[float],
    observed: float,
) -> float:
    """Score one (quantile band, observation) pair with the textbook CRPS
    estimator. See the module docstring for the formula.

    Args:
        levels: Quantile probability levels (will be isotonic-repaired).
        quantiles: Corresponding quantile values.
        observed: The scalar observation to score.

    Returns:
        Approximate CRPS >= 0, or ``math.nan`` when fewer than 2 finite nodes
        remain after isotonic repair or when ``observed`` is non-finite.
    """
    lev, qval = _isotonic_repair(levels, quantiles)
    if len(lev) < 2:
        return math.nan
    try:
        obs = float(observed)
    except (TypeError, ValueError):
        return math.nan
    if not math.isfinite(obs):
        return math.nan

    def _pinball(tau: float, q: float) -> float:
        if obs >= q:
            return tau * (obs - q)
        return (1.0 - tau) * (q - obs)

    # --- Middle: trapezoidal integration between adjacent nodes ---
    middle = 0.0
    for i in range(len(lev) - 1):
        dtau = lev[i + 1] - lev[i]
        middle += dtau * (_pinball(lev[i], qval[i]) + _pinball(lev[i + 1], qval[i + 1])) / 2.0

    # --- Left tail: [0, tau_min] flat at q_min ---
    tau_min = lev[0]
    q_min = qval[0]
    if obs >= q_min:
        # integral_0^tau_min tau * (obs - q_min) dtau = (obs - q_min) * tau_min^2 / 2
        left_tail = (obs - q_min) * tau_min**2 / 2.0
    else:
        # integral_0^tau_min (1-tau) * (q_min - obs) dtau
        #     = (q_min - obs) * (tau_min - tau_min^2/2)
        left_tail = (q_min - obs) * (tau_min - tau_min**2 / 2.0)

    # --- Right tail: [tau_max, 1] flat at q_max ---
    tau_max = lev[-1]
    q_max = qval[-1]
    if obs <= q_max:
        # integral_tau_max^1 (1-tau) * (q_max - obs) dtau = (q_max - obs) * (1-tau_max)^2/2
        right_tail = (q_max - obs) * (1.0 - tau_max) ** 2 / 2.0
    else:
        # integral_tau_max^1 tau * (obs - q_max) dtau = (obs - q_max) * (1-tau_max^2)/2
        right_tail = (obs - q_max) * (1.0 - tau_max**2) / 2.0

    return 2.0 * (left_tail + middle + right_tail)


def crps_from_quantiles(
    observed: Sequence[float] | np.ndarray,
    quantile_forecasts: Sequence[Sequence[float]] | np.ndarray,
    quantile_levels: Sequence[float] | np.ndarray,
) -> float:
    """Mean textbook CRPS across (N, K) observation/quantile-forecast pairs.

    This is the canonical, vectorised entry point — it is what
    ``postprocessing_forecasts.skill_metrics.calculate_crps`` delegates to.
    Internally each row is scored via :func:`crps_single`, so this function
    and any direct caller of :func:`crps_single` always agree.

    Args:
        observed: shape (N,) — observed values.
        quantile_forecasts: shape (N, K) — forecasted quantiles.
        quantile_levels: shape (K,) — e.g. [0.05, 0.10, ..., 0.95].

    Returns:
        NaN-aware mean CRPS across valid observation rows (lower is better).
        A row is invalid (excluded from the mean) when its observation is
        non-finite, or when fewer than 2 finite quantile nodes remain after
        isotonic repair (#6 — a single bad row no longer poisons the whole
        group's mean). Returns NaN only when every row is invalid.
    """
    observed_arr = np.asarray(observed, dtype=np.float64)
    quantile_forecasts_arr = np.asarray(quantile_forecasts, dtype=np.float64)
    quantile_levels_arr = np.asarray(quantile_levels, dtype=np.float64)

    n = observed_arr.shape[0]
    per_obs = np.full(n, np.nan, dtype=np.float64)
    for i in range(n):
        per_obs[i] = crps_single(quantile_levels_arr, quantile_forecasts_arr[i], observed_arr[i])

    if not np.any(np.isfinite(per_obs)):
        return float("nan")
    return float(np.nanmean(per_obs))
