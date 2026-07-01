"""Phase-2C: symmetric percentile-based event detection.

Defines the five binary events used in forecast skill evaluation:

- ``below_norm``  — value < 0.80 × norm (the original irrigation decision rule)
- ``low_p10``     — value < 10th empirical percentile (low-flow detection)
- ``low_p5``      — value < 5th empirical percentile (severe low-flow)
- ``high_p90``    — value > 90th empirical percentile (high-flow / flood)
- ``high_p95``    — value > 95th empirical percentile (severe high-flow)

Percentiles are computed empirically per ``(code, horizon, period_key)`` from
the observed values already embedded in the pairs DataFrame (leave-all-in;
same ``min_years`` gate as the norm calculation).

Phase-2D adds four return-period (EVT) events:

- ``rp5``   — observed / forecast exceeds the 5-year GEV return level
- ``rp10``  — observed / forecast exceeds the 10-year GEV return level
- ``rp30``  — observed / forecast exceeds the 30-year GEV return level
- ``rp100`` — observed / forecast exceeds the 100-year GEV return level

Return-period events are **opt-in** (not in the default event set) because
they require a GEV fit over annual maxima, are expensive, and their positive
class is rare by construction.  Enable them via ``--events rp5 rp10 ...`` or
the ``events_filter`` config field.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final, Literal

import numpy as np
import pandas as pd
from scipy import stats

from forecast_skill_eval.classifier import contingency as _contingency

# ---------------------------------------------------------------------------
# Public API constants
# ---------------------------------------------------------------------------

ALL_EVENT_NAMES: Final[tuple[str, ...]] = (
    "below_norm",
    "low_p10",
    "low_p5",
    "high_p90",
    "high_p95",
)

_RP_EVENT_NAMES: Final[tuple[str, ...]] = ("rp5", "rp10", "rp30", "rp100")

VALID_EVENTS: Final[frozenset[str]] = frozenset((*ALL_EVENT_NAMES, *_RP_EVENT_NAMES))

# Percentile levels required for the four percentile events.
_REQUIRED_PERCENTILES: Final[tuple[float, ...]] = (5.0, 10.0, 90.0, 95.0)


# ---------------------------------------------------------------------------
# EventDef
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EventDef:
    """Definition of a binary event for contingency evaluation.

    Attributes:
        name: Event identifier used in output.
        direction: ``"below"`` for low-flow events, ``"above"`` for high-flow
            events.  The ``"below"`` class is always the positive class (event
            occurred) in the contingency machinery regardless of direction.
        percentile: Percentile level (0–100), or ``None`` for norm-based or
            return-period events.
        return_period: Return period in years for EVT events, or ``None`` for
            percentile / norm-based events.  When set, the event is a
            return-period event and ``percentile`` must be ``None``.
    """

    name: str
    direction: Literal["below", "above"]
    percentile: float | None
    return_period: float | None = field(default=None)


ALL_EVENTS: Final[tuple[EventDef, ...]] = (
    EventDef("below_norm", "below", None),
    EventDef("low_p10", "below", 10.0),
    EventDef("low_p5", "below", 5.0),
    EventDef("high_p90", "above", 90.0),
    EventDef("high_p95", "above", 95.0),
)

_RP_EVENTS: Final[tuple[EventDef, ...]] = (
    EventDef("rp5", "above", None, return_period=5.0),
    EventDef("rp10", "above", None, return_period=10.0),
    EventDef("rp30", "above", None, return_period=30.0),
    EventDef("rp100", "above", None, return_period=100.0),
)

_EVENT_BY_NAME: Final[dict[str, EventDef]] = {e.name: e for e in (*ALL_EVENTS, *_RP_EVENTS)}


# ---------------------------------------------------------------------------
# Lookup helper
# ---------------------------------------------------------------------------


def event_by_name(name: str) -> EventDef:
    """Return the :class:`EventDef` matching *name*.

    Args:
        name: Event identifier.

    Returns:
        The matching EventDef.

    Raises:
        ValueError: If *name* is not a recognised event identifier.
    """
    if name not in _EVENT_BY_NAME:
        raise ValueError(f"Unknown event: {name!r}. Valid events: {sorted(VALID_EVENTS)}")
    return _EVENT_BY_NAME[name]


# ---------------------------------------------------------------------------
# Percentile threshold computation
# ---------------------------------------------------------------------------


def compute_percentile_thresholds(
    pairs: pd.DataFrame,
    min_years: int,
    percentiles: tuple[float, ...] = _REQUIRED_PERCENTILES,
) -> dict[tuple[str, str, int], dict[float, float]]:
    """Compute empirical percentile thresholds per ``(code, horizon, period_key)``.

    Thresholds are derived from the observed values already embedded in the
    pairs DataFrame.  The computation uses a de-duplicated view of
    ``(code, horizon, period_key, year)`` so that rows from multiple models
    for the same station/period/year contribute only once to the empirical
    distribution.

    Groups with fewer than *min_years* distinct years are excluded (same gate
    as the norm ``min_years`` guard).

    Args:
        pairs: Pair DataFrame containing at minimum ``code``, ``horizon``,
            ``period_key``, ``year``, and ``observed_value`` columns.
        min_years: Minimum number of distinct years required.  Groups with
            fewer distinct years are omitted from the returned mapping.
        percentiles: Percentile levels (0–100) to compute.  Defaults to the
            four levels used by the standard percentile events.

    Returns:
        Mapping from ``(code, horizon, period_key)`` to a dict of
        ``{percentile_level: threshold_value}``.  Absent entries mean the
        station/period did not meet the *min_years* gate.
    """
    required = {"code", "horizon", "period_key", "year", "observed_value"}
    if pairs.empty or not required.issubset(pairs.columns):
        return {}

    obs = (
        pairs[list(required)]
        .drop_duplicates(subset=["code", "horizon", "period_key", "year"])
        .dropna(subset=["observed_value"])
        .copy()
    )
    if obs.empty:
        return {}

    result: dict[tuple[str, str, int], dict[float, float]] = {}
    for (code, horizon, period_key), group in obs.groupby(["code", "horizon", "period_key"]):
        if group["year"].nunique() < min_years:
            continue
        values = group["observed_value"].to_numpy(dtype=float)
        thresholds: dict[float, float] = {}
        for p in percentiles:
            thresholds[p] = float(np.percentile(values, p))
        result[(str(code), str(horizon), int(period_key))] = thresholds

    return result


# ---------------------------------------------------------------------------
# GEV return-level computation
# ---------------------------------------------------------------------------


def compute_return_levels(
    pairs: pd.DataFrame,
    return_periods: tuple[float, ...],
    min_years: int,
) -> dict[tuple[str, str], dict[float, float]]:
    """Compute GEV return levels per ``(code, horizon)`` from annual maxima.

    For each station and horizon the function takes the **annual maxima** of
    ``observed_value`` (de-duplicated across models and period keys so no
    observation is counted more than once per year), fits a Generalised
    Extreme Value (GEV) distribution via maximum-likelihood estimation, and
    computes the return level for each requested return period as the
    ``1 - 1/T`` quantile.

    **Feasibility note**: a return period ``T`` exceeds the record length
    ``n_years`` whenever ``T > n_years`` — this constitutes extrapolation
    beyond the observed range.  Return levels are still computed (GEV theory
    allows it), but callers and reports *must* caveat results where
    ``T > n_years``.  With ~26 years of archive, 5-yr and 10-yr return levels
    are within the observed range; 30-yr is marginal; 100-yr is extrapolation
    and should be treated as illustrative only.

    Groups with fewer than *min_years* distinct years are skipped.  A
    degenerate (constant) observed series or a failed GEV fit yields no
    return-level entry for that group — the function never raises.

    Args:
        pairs: Pair DataFrame containing at minimum ``code``, ``horizon``,
            ``period_key``, ``year``, and ``observed_value`` columns.
        return_periods: Return periods in years (e.g. ``(5.0, 10.0, 30.0,
            100.0)``).
        min_years: Minimum number of distinct years with annual maxima
            required.  Groups with fewer years are omitted.

    Returns:
        Mapping from ``(code, horizon)`` to a dict of
        ``{return_period: return_level}``.  Absent entries mean the station
        did not meet the *min_years* gate or the GEV fit failed.
    """
    required = {"code", "horizon", "period_key", "year", "observed_value"}
    if pairs.empty or not required.issubset(pairs.columns):
        return {}

    # De-duplicate: one observed value per (code, horizon, period_key, year)
    # across models so multiple model rows for the same observation don't
    # inflate the annual-max distribution.
    obs = (
        pairs[["code", "horizon", "period_key", "year", "observed_value"]]
        .drop_duplicates(subset=["code", "horizon", "period_key", "year"])
        .dropna(subset=["observed_value"])
    )
    if obs.empty:
        return {}

    # Aggregate to annual maxima per (code, horizon, year)
    annual_max = obs.groupby(["code", "horizon", "year"])["observed_value"].max().reset_index()

    result: dict[tuple[str, str], dict[float, float]] = {}

    for (code, horizon), group in annual_max.groupby(["code", "horizon"]):
        n_years = group["year"].nunique()
        if n_years < min_years:
            continue

        maxima = group["observed_value"].to_numpy(dtype=float)
        maxima = maxima[np.isfinite(maxima)]
        if len(maxima) < min_years:
            continue

        # Skip degenerate (constant) series — GEV fit is undefined
        if float(np.ptp(maxima)) == 0.0:
            continue

        try:
            gev_params = stats.genextreme.fit(maxima)
        except Exception:
            continue

        levels: dict[float, float] = {}
        for T in return_periods:
            try:
                level = float(stats.genextreme.ppf(1.0 - 1.0 / T, *gev_params))
                if np.isfinite(level):
                    levels[T] = level
            except Exception:
                pass

        if levels:
            result[(str(code), str(horizon))] = levels

    return result


# ---------------------------------------------------------------------------
# Pair reclassification — percentile events
# ---------------------------------------------------------------------------


def reclassify_pairs_for_event(
    pairs: pd.DataFrame,
    event: EventDef,
    thresholds: dict[tuple[str, str, int], dict[float, float]],
) -> pd.DataFrame:
    """Return a copy of *pairs* reclassified for the given percentile event.

    For the ``below_norm`` event (``event.percentile is None``) the pairs are
    returned unchanged — their ``fc_class``, ``obs_class``, and ``contingency``
    columns already reflect the norm-based classification.

    For percentile events the function recomputes ``fc_class``, ``obs_class``,
    and ``contingency`` based on the event's direction and percentile threshold.
    ``"below"`` is used as the positive class (= event occurred) regardless of
    direction:

    - Low-flow events (``direction="below"``): positive when value *<* threshold.
    - High-flow events (``direction="above"``): positive when value *>* threshold.

    Rows where the threshold is unavailable (station/period did not meet the
    ``min_years`` gate) are silently dropped.

    Args:
        pairs: Original pair DataFrame with ``fc_class``, ``obs_class``,
            ``contingency``, ``forecast_value``, and ``observed_value`` columns.
        event: The event definition.  Must be a percentile or norm-based event
            (``event.return_period`` must be ``None``).  For return-period
            events use :func:`reclassify_pairs_for_rp_event` instead.
        thresholds: Per-``(code, horizon, period_key)`` percentile thresholds as
            returned by :func:`compute_percentile_thresholds`.

    Returns:
        Reclassified DataFrame with updated ``fc_class``, ``obs_class``, and
        ``contingency`` columns.  Column order is preserved.  May be empty if
        no rows have thresholds.
    """
    if pairs.empty:
        return pairs.copy()

    if event.percentile is None and event.return_period is None:
        # below_norm: return pairs unchanged — fc_class / obs_class already set.
        return pairs.copy()

    rows: list[dict[str, object]] = []
    for row in pairs.to_dict("records"):
        code = str(row.get("code", ""))
        horizon = str(row.get("horizon", ""))
        try:
            period_key = int(row["period_key"])
        except (TypeError, ValueError, KeyError):
            continue

        key = (code, horizon, period_key)
        period_thresholds = thresholds.get(key)
        if period_thresholds is None:
            continue

        threshold_value = period_thresholds.get(event.percentile)
        if threshold_value is None:
            continue

        fc_val = row.get("forecast_value")
        obs_val = row.get("observed_value")
        if fc_val is None or obs_val is None:
            continue

        try:
            fc_f = float(fc_val)
            obs_f = float(obs_val)
        except (TypeError, ValueError):
            continue

        if event.direction == "below":
            fc_class: str = "below" if fc_f < threshold_value else "normal"
            obs_class: str = "below" if obs_f < threshold_value else "normal"
        else:
            # direction == "above": positive class = value exceeds threshold
            fc_class = "below" if fc_f > threshold_value else "normal"
            obs_class = "below" if obs_f > threshold_value else "normal"

        new_row = dict(row)
        new_row["fc_class"] = fc_class
        new_row["obs_class"] = obs_class
        new_row["contingency"] = _contingency(fc_class, obs_class)
        rows.append(new_row)

    if not rows:
        return pd.DataFrame(columns=list(pairs.columns))

    return pd.DataFrame(rows, columns=list(pairs.columns))


# ---------------------------------------------------------------------------
# Pair reclassification — return-period events
# ---------------------------------------------------------------------------


def reclassify_pairs_for_rp_event(
    pairs: pd.DataFrame,
    event: EventDef,
    return_levels: dict[tuple[str, str], dict[float, float]],
) -> pd.DataFrame:
    """Return a copy of *pairs* reclassified for the given return-period event.

    Recomputes ``fc_class``, ``obs_class``, and ``contingency`` based on
    whether each value exceeds the GEV return level for its ``(code, horizon)``
    group.  ``"below"`` is used as the positive class (= event occurred):

    - Positive (event): value *>* return level → ``fc_class = "below"``.
    - Negative (no event): value *≤* return level → ``fc_class = "normal"``.

    Rows where no return level is available (station did not meet the
    ``min_years`` gate or GEV fit failed) are silently dropped.

    Args:
        pairs: Original pair DataFrame with ``fc_class``, ``obs_class``,
            ``contingency``, ``forecast_value``, and ``observed_value`` columns.
        event: The event definition.  Must be a return-period event
            (``event.return_period`` must not be ``None``).
        return_levels: Per-``(code, horizon)`` return-level mappings as
            returned by :func:`compute_return_levels`.

    Returns:
        Reclassified DataFrame with updated ``fc_class``, ``obs_class``, and
        ``contingency`` columns.  Column order is preserved.  May be empty if
        no rows have an available return level.

    Raises:
        ValueError: If *event* is not a return-period event.
    """
    if pairs.empty:
        return pairs.copy()

    if event.return_period is None:
        raise ValueError(
            f"Event {event.name!r} is not a return-period event (return_period is None). "
            "Use reclassify_pairs_for_event for percentile / norm-based events."
        )

    rows: list[dict[str, object]] = []
    for row in pairs.to_dict("records"):
        code = str(row.get("code", ""))
        horizon = str(row.get("horizon", ""))

        key = (code, horizon)
        group_levels = return_levels.get(key)
        if group_levels is None:
            continue

        level = group_levels.get(event.return_period)
        if level is None:
            continue

        fc_val = row.get("forecast_value")
        obs_val = row.get("observed_value")
        if fc_val is None or obs_val is None:
            continue

        try:
            fc_f = float(fc_val)
            obs_f = float(obs_val)
        except (TypeError, ValueError):
            continue

        # direction "above": positive class = value exceeds the return level
        fc_class: str = "below" if fc_f > level else "normal"
        obs_class: str = "below" if obs_f > level else "normal"

        new_row = dict(row)
        new_row["fc_class"] = fc_class
        new_row["obs_class"] = obs_class
        new_row["contingency"] = _contingency(fc_class, obs_class)
        rows.append(new_row)

    if not rows:
        return pd.DataFrame(columns=list(pairs.columns))

    return pd.DataFrame(rows, columns=list(pairs.columns))
