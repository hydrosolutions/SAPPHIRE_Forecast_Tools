"""Probabilistic CRPS reference distributions for CRPSS computation.

Provides climatology and persistence CRPS reference values using the IDENTICAL
grid+tail estimator as :func:`prob_metrics.crps_from_quantiles` so that
CRPSS = 1 − CRPS_fc / CRPS_ref is unbiased by estimator mismatch
(Design Decision 2).

Performance (Design Decision 9):
    The climatology sample per conditioning group (code, horizon, period_key)
    is assembled ONCE.  For each group, np.quantile is computed once at the
    target levels, producing a fixed (levels, quantile_values) tuple.  Per-pair
    CRPS_clim then calls crps_from_quantiles(precomputed_levels, precomputed_qvals,
    obs) which is O(n_grid) — not O(m²).  A performance-category test guards
    against re-introducing an O(n·m) or O(m²) loop.

Conditioning (Design Decision alignment with baselines.build_climatology_baseline):
    The climatology group key is (code, horizon, period_key) — identical to
    the grouping used in build_climatology_baseline for the deterministic
    baseline.  This ensures the CRPSS denominator shares the same conditioning
    set as the "always-normal" skill score for cross-metric consistency.

Persistence:
    Persistence reference CRPS = |lag1_obs − obs|  (degenerate zero-spread
    point forecast).  This is the theoretically exact CRPS of a point mass at
    lag1_obs — no grid estimator is required.  Documented as a degenerate case.
"""

from __future__ import annotations

import math
from typing import Final

import numpy as np
import pandas as pd

from forecast_skill_eval.baselines import _build_obs_lookup, _lag1_key
from forecast_skill_eval.prob_metrics import QUANTILE_LEVELS, crps_from_quantiles

# Default quantile levels used when the caller does not specify levels.
_DEFAULT_LEVELS: Final[tuple[float, ...]] = QUANTILE_LEVELS

# Conditioning key type: (code, horizon, period_key).
_ClimKey = tuple[str, str, int]

# Precomputed entry: (sorted_levels, quantile_values_at_those_levels).
_ClimEntry = tuple[list[float], list[float]]

# Persistence key: (code, horizon, period_key, year).
_PersistKey = tuple[str, str, int, int]


def precompute_climatology_crps(
    pairs: pd.DataFrame,
    levels: tuple[float, ...] | None = None,
) -> dict[_ClimKey, _ClimEntry]:
    """Precompute climatology reference quantiles per conditioning group.

    Groups pairs by (code, horizon, period_key) — the same conditioning set
    as baselines.build_climatology_baseline — and computes np.quantile of the
    observed_value sample at ``levels`` once per group (O(m log m) per group).

    Per-pair CRPS_clim is then computed inside compute_probabilistic_metrics by
    calling crps_from_quantiles with the precomputed (levels, quantiles) tuple
    and the pair's specific observed value.  This avoids O(n·m) recomputation.

    Args:
        pairs: Pair DataFrame containing ``code``, ``horizon``,
            ``period_key``, ``year``, and ``observed_value`` columns.
        levels: Quantile probability levels to use.  Defaults to
            ``QUANTILE_LEVELS`` (all 7 canonical levels).  Pass a subset
            (e.g. ``(0.05, 0.25, 0.75, 0.95)``) to match a short-term grid.

    Returns:
        Mapping from ``(code, horizon, period_key)`` to
        ``(levels_list, quantile_values_list)``.  Groups with fewer than 2
        distinct finite observations are excluded (no entry).
    """
    if levels is None:
        levels = _DEFAULT_LEVELS

    finite_levels = [float(lv) for lv in levels if math.isfinite(float(lv))]
    if len(finite_levels) < 2:
        return {}

    required = {"code", "horizon", "period_key", "year", "observed_value"}
    if pairs.empty or not required.issubset(pairs.columns):
        return {}

    # De-duplicate: one observed value per (code, horizon, period_key, year).
    obs = (
        pairs[list(required)]
        .drop_duplicates(subset=["code", "horizon", "period_key", "year"])
        .dropna(subset=["observed_value"])
        .copy()
    )
    if obs.empty:
        return {}

    result: dict[_ClimKey, _ClimEntry] = {}
    for (code, horizon, period_key), group in obs.groupby(
        ["code", "horizon", "period_key"], sort=True
    ):
        values = group["observed_value"].to_numpy(dtype=float)
        values = values[np.isfinite(values)]
        if len(values) < 2:
            continue
        q_values = np.quantile(values, finite_levels).tolist()
        result[(str(code), str(horizon), int(period_key))] = (finite_levels, q_values)

    return result


def precompute_persistence_crps(
    pairs: pd.DataFrame,
) -> dict[_PersistKey, float]:
    """Precompute the lag-1 observed value per (code, horizon, period_key, year).

    Persistence reference CRPS = |lag1_obs − obs| (degenerate zero-spread
    point forecast).  This function returns the lag-1 observed value; the
    absolute difference is computed at scoring time in _attach_reference_crps.

    Reuses :func:`baselines._build_obs_lookup` and :func:`baselines._lag1_key`
    for consistent lag-1 definition (same as the deterministic persistence
    baseline).

    Args:
        pairs: Pair DataFrame containing the columns required by
            _build_obs_lookup.

    Returns:
        Mapping from ``(code, horizon, period_key, year)`` to the lag-1
        observed value (float).  Pairs where the lag-1 observation is absent
        are excluded (no entry).
    """
    if pairs.empty:
        return {}

    obs_lookup = _build_obs_lookup(pairs)
    result: dict[_PersistKey, float] = {}

    required = {"code", "horizon", "period_key", "year"}
    if not required.issubset(pairs.columns):
        return {}

    seen: set[_PersistKey] = set()
    for row in pairs.to_dict("records"):
        code = str(row.get("code", ""))
        horizon = str(row.get("horizon", ""))
        try:
            period_key = int(row["period_key"])
            year = int(row["year"])
        except (TypeError, ValueError, KeyError):
            continue

        current_key: _PersistKey = (code, horizon, period_key, year)
        if current_key in seen:
            continue
        seen.add(current_key)

        lag1_key = _lag1_key(horizon, code, period_key, year)
        if lag1_key is None:
            continue

        lag1_obs = obs_lookup.get(lag1_key)
        if lag1_obs is None:
            continue

        result[current_key] = lag1_obs

    return result


def climatology_crps_for_pair(
    clim_ref: dict[_ClimKey, _ClimEntry],
    code: str,
    horizon: str,
    period_key: int,
    observed: float,
) -> float:
    """Compute CRPS_clim for a single pair by looking up the precomputed entry.

    Convenience function for testing.  Production code uses _attach_reference_crps
    inside compute_probabilistic_metrics for batch processing.

    Args:
        clim_ref: Output of precompute_climatology_crps.
        code: Station code.
        horizon: Horizon string.
        period_key: Calendar period key (int).
        observed: The observation to score.

    Returns:
        CRPS of the climatology reference distribution against ``observed``,
        or ``math.nan`` when the conditioning group is absent.
    """
    entry = clim_ref.get((code, horizon, period_key))
    if entry is None:
        return math.nan
    clim_levels, clim_qvals = entry
    return crps_from_quantiles(clim_levels, clim_qvals, observed)


def persistence_crps_for_pair(
    persist_ref: dict[_PersistKey, float],
    code: str,
    horizon: str,
    period_key: int,
    year: int,
    observed: float,
) -> float:
    """Compute CRPS_persist = |lag1_obs − observed| for a single pair.

    Persistence is a degenerate point-mass distribution; its CRPS equals the
    mean absolute error of the lag-1 observed value as a deterministic forecast.

    Args:
        persist_ref: Output of precompute_persistence_crps.
        code: Station code.
        horizon: Horizon string.
        period_key: Calendar period key.
        year: Forecast year.
        observed: The observation to score.

    Returns:
        |lag1_obs − observed|, or ``math.nan`` when lag-1 data is absent.
    """
    lag1_obs = persist_ref.get((code, horizon, period_key, year))
    if lag1_obs is None or not math.isfinite(observed):
        return math.nan
    return abs(lag1_obs - observed)
