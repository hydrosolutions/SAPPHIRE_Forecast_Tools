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
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Literal

import numpy as np
import pandas as pd

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
VALID_EVENTS: Final[frozenset[str]] = frozenset(ALL_EVENT_NAMES)

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
        percentile: Percentile level (0–100), or ``None`` for the norm-based
            ``below_norm`` event.
    """

    name: str
    direction: Literal["below", "above"]
    percentile: float | None


ALL_EVENTS: Final[tuple[EventDef, ...]] = (
    EventDef("below_norm", "below", None),
    EventDef("low_p10", "below", 10.0),
    EventDef("low_p5", "below", 5.0),
    EventDef("high_p90", "above", 90.0),
    EventDef("high_p95", "above", 95.0),
)

_EVENT_BY_NAME: Final[dict[str, EventDef]] = {e.name: e for e in ALL_EVENTS}


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
# Pair reclassification
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
        event: The event definition.
        thresholds: Per-``(code, horizon, period_key)`` percentile thresholds as
            returned by :func:`compute_percentile_thresholds`.

    Returns:
        Reclassified DataFrame with updated ``fc_class``, ``obs_class``, and
        ``contingency`` columns.  Column order is preserved.  May be empty if
        no rows have thresholds.
    """
    if pairs.empty:
        return pairs.copy()

    if event.percentile is None:
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
