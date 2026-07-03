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

A norm-factor event is also available (opt-in, not in the default set):

- ``below_norm_100`` — value < 1.0 × norm (plain below-norm), reclassified from
  the ``norm`` column in the same run as the 0.80 × norm ``below_norm`` event.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final, Literal

import numpy as np
import pandas as pd
from scipy import stats

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

# Norm-factor events are opt-in (not in ALL_EVENT_NAMES / DEFAULT_EVENTS).  They
# reclassify from the ``norm`` column at ``value < factor * norm`` and are only
# computed when the caller lists them explicitly in ``--events``.
_NORM_FACTOR_EVENT_NAMES: Final[tuple[str, ...]] = ("below_norm_100",)

VALID_EVENTS: Final[frozenset[str]] = frozenset(
    (*ALL_EVENT_NAMES, *_RP_EVENT_NAMES, *_NORM_FACTOR_EVENT_NAMES)
)

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
        factor: Norm multiplier for norm-factor events (e.g. ``1.0`` for plain
            below-norm), or ``None`` for percentile / return-period events and
            the global-threshold ``below_norm`` event.  When set, the event is
            reclassified from the ``norm`` column at ``value < factor * norm``.
    """

    name: str
    direction: Literal["below", "above"]
    percentile: float | None
    return_period: float | None = field(default=None)
    factor: float | None = field(default=None)


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

_NORM_FACTOR_EVENTS: Final[tuple[EventDef, ...]] = (
    EventDef("below_norm_100", "below", None, factor=1.0),
)

_EVENT_BY_NAME: Final[dict[str, EventDef]] = {
    e.name: e for e in (*ALL_EVENTS, *_RP_EVENTS, *_NORM_FACTOR_EVENTS)
}


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
) -> dict[tuple[str, str, int], dict[float, float]]:
    """Compute GEV return levels per ``(code, horizon, period_key)``.

    For each station, horizon, and calendar period the function takes the
    per-period realizations of ``observed_value`` (de-duplicated across models
    so no observation is counted more than once per year), fits a Generalised
    Extreme Value (GEV) distribution via maximum-likelihood estimation, and
    computes the return level for each requested return period as the
    ``1 - 1/T`` quantile.

    **Interpretation note**: low return periods (T = 5, 10) approximate the
    ``(1 − 1/T)``-th empirical percentile — so rp10 ≈ 90th percentile of the
    per-period distribution — and are well-supported when ``n_years ≥ T``.
    High return periods (T = 30, 100) are parametric extrapolations beyond the
    observed range whenever ``T > n_years`` and should be treated as
    illustrative only.

    Groups with fewer than *min_years* distinct years are skipped.  A
    degenerate (constant) observed series or a failed GEV fit yields no
    return-level entry for that group — the function never raises.

    Args:
        pairs: Pair DataFrame containing at minimum ``code``, ``horizon``,
            ``period_key``, ``year``, and ``observed_value`` columns.
        return_periods: Return periods in years (e.g. ``(5.0, 10.0, 30.0,
            100.0)``).
        min_years: Minimum number of distinct years with observations required.
            Groups with fewer years are omitted.

    Returns:
        Mapping from ``(code, horizon, period_key)`` to a dict of
        ``{return_period: return_level}``.  Absent entries mean the
        station/period did not meet the *min_years* gate or the GEV fit
        failed.
    """
    required = {"code", "horizon", "period_key", "year", "observed_value"}
    if pairs.empty or not required.issubset(pairs.columns):
        return {}

    # De-duplicate: one observed value per (code, horizon, period_key, year)
    # across models so multiple model rows for the same observation don't
    # inflate the per-period distribution.
    obs = (
        pairs[["code", "horizon", "period_key", "year", "observed_value"]]
        .drop_duplicates(subset=["code", "horizon", "period_key", "year"])
        .dropna(subset=["observed_value"])
    )
    if obs.empty:
        return {}

    result: dict[tuple[str, str, int], dict[float, float]] = {}

    for (code, horizon, period_key), group in obs.groupby(["code", "horizon", "period_key"]):
        n_years = group["year"].nunique()
        if n_years < min_years:
            continue

        values = group["observed_value"].to_numpy(dtype=float)
        values = values[np.isfinite(values)]
        if len(values) < min_years:
            continue

        # Skip degenerate (constant) series — GEV fit is undefined
        if float(np.ptp(values)) == 0.0:
            continue

        try:
            gev_params = stats.genextreme.fit(values)
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
            result[(str(code), str(horizon), int(period_key))] = levels

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

    For norm-factor events (``event.factor is not None``, e.g. ``below_norm_100``
    at ``factor=1.0``) the pairs are reclassified directly from the ``norm``
    column: a value is "below" iff ``value < factor * norm`` (strict ``<``).
    Rows whose ``norm`` is non-finite or ``<= 0`` are dropped (matching
    :func:`classifier.classify` returning ``None``).

    For the ``below_norm`` event (``event.percentile is None`` and
    ``event.factor is None``) the pairs are returned unchanged — their
    ``fc_class``, ``obs_class``, and ``contingency`` columns already reflect the
    norm-based classification at the global ``config.threshold``.

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

    if event.factor is not None:
        # Norm-factor event (e.g. below_norm_100): reclassify from the norm
        # column at value < factor * norm.  Mirrors classifier.classify: strict
        # <, and rows with non-finite norm or norm <= 0 are dropped.
        return _reclassify_pairs_from_norm(pairs, float(event.factor))

    if event.percentile is None and event.return_period is None:
        # below_norm: return pairs unchanged — fc_class / obs_class already set.
        return pairs.copy()

    orig_cols = list(pairs.columns)

    # Build a small lookup DataFrame: one row per (code, horizon, period_key) that
    # has a threshold for this event's percentile level.
    thresh_rows: list[tuple[str, str, int, float]] = []
    for (c, h, pk), pd_thresholds in thresholds.items():
        tv = pd_thresholds.get(event.percentile)
        if tv is not None:
            thresh_rows.append((str(c), str(h), int(pk), float(tv)))

    if not thresh_rows:
        return pd.DataFrame(columns=orig_cols)

    thresh_df = pd.DataFrame(thresh_rows, columns=["_code", "_horizon", "_pk_int", "_threshold"])

    # Cast period_key to int; coerce failures → NaN → drop (mirrors the try/except
    # in the row-wise loop that catches TypeError, ValueError, KeyError).
    df = pairs.copy()
    pk_numeric = pd.to_numeric(df["period_key"], errors="coerce")
    valid_pk = pk_numeric.notna()
    if not valid_pk.all():
        df = df[valid_pk].copy()
        pk_numeric = pk_numeric[valid_pk]
    if df.empty:
        return pd.DataFrame(columns=orig_cols)

    df["_pk_int"] = pk_numeric.astype("int64")
    df["_code"] = df["code"].astype(str)
    df["_horizon"] = df["horizon"].astype(str)

    # Inner join: drops rows whose (code, horizon, period_key) has no threshold.
    df = df.merge(thresh_df, on=["_code", "_horizon", "_pk_int"], how="inner")
    if df.empty:
        return pd.DataFrame(columns=orig_cols)

    # Cast forecast_value / observed_value to float.
    # NaN floats (np.nan) are KEPT — the original loop calls float(nan) which
    # succeeds, and nan comparisons return False (→ "normal").  Only drop rows
    # that are Python None (TypeError) or a non-castable non-float in an object
    # column (ValueError), mirroring the row-wise None-check + try/except.
    fc_vals = pd.to_numeric(df["forecast_value"], errors="coerce")
    obs_vals = pd.to_numeric(df["observed_value"], errors="coerce")

    if not pd.api.types.is_numeric_dtype(df["forecast_value"]) or not pd.api.types.is_numeric_dtype(
        df["observed_value"]
    ):
        # At least one object-dtype column present: drop Python None and
        # non-castable strings.  Float NaN stored as object is valid → keep.
        def _non_castable(col: pd.Series) -> pd.Series:
            if pd.api.types.is_numeric_dtype(col):
                return pd.Series(False, index=col.index)
            return col.apply(
                lambda x: x is None
                or (not isinstance(x, (int, float)) and pd.isna(pd.to_numeric(x, errors="coerce")))
            )

        drop = _non_castable(df["forecast_value"]) | _non_castable(df["observed_value"])
        if drop.any():
            df = df[~drop].copy()
            fc_vals = fc_vals[~drop]
            obs_vals = obs_vals[~drop]
        if df.empty:
            return pd.DataFrame(columns=orig_cols)

    threshold_arr = df["_threshold"].to_numpy(dtype=float)
    fc_arr = fc_vals.to_numpy(dtype=float)
    obs_arr = obs_vals.to_numpy(dtype=float)

    # Classify — strict < / > so equality at threshold → "normal".
    # NaN comparisons return False → "normal", matching the original.
    if event.direction == "below":
        fc_class = np.where(fc_arr < threshold_arr, "below", "normal")
        obs_class = np.where(obs_arr < threshold_arr, "below", "normal")
    else:
        # direction == "above": positive class = value exceeds threshold
        fc_class = np.where(fc_arr > threshold_arr, "below", "normal")
        obs_class = np.where(obs_arr > threshold_arr, "below", "normal")

    # Vectorized contingency — mirrors classifier.contingency exactly.
    fc_b = fc_class == "below"
    obs_b = obs_class == "below"
    contingency_arr = np.where(
        fc_b & obs_b,
        "TP",
        np.where(fc_b & ~obs_b, "FP", np.where(~fc_b & obs_b, "FN", "TN")),
    )

    df = df.copy()
    df["fc_class"] = fc_class
    df["obs_class"] = obs_class
    df["contingency"] = contingency_arr

    return df[orig_cols].reset_index(drop=True)


def _reclassify_pairs_from_norm(pairs: pd.DataFrame, factor: float) -> pd.DataFrame:
    """Reclassify *pairs* from the ``norm`` column at ``value < factor * norm``.

    Mirrors :func:`classifier.classify` semantics for a norm-factor event:

    - A value is "below" iff ``value < factor * norm`` (strict ``<``; equality
      → "normal").
    - Rows whose ``norm`` is non-finite or ``<= 0`` are dropped (the classifier
      returns ``None`` for these).
    - NaN ``forecast_value`` / ``observed_value`` are kept and classify as
      "normal" (NaN comparisons return ``False``), matching the percentile
      branch's NaN-keep behaviour.

    Original column order and ``reset_index(drop=True)`` are preserved, exactly
    like the percentile branch.

    Args:
        pairs: Pair DataFrame with ``norm``, ``forecast_value``, and
            ``observed_value`` columns.
        factor: Norm multiplier (e.g. ``1.0`` for plain below-norm).

    Returns:
        Reclassified DataFrame with updated ``fc_class``, ``obs_class``, and
        ``contingency`` columns.  May be empty if no rows have a usable norm.
    """
    orig_cols = list(pairs.columns)

    if pairs.empty:
        return pairs.copy()

    df = pairs.copy()

    norm_arr = pd.to_numeric(df["norm"], errors="coerce").to_numpy(dtype=float)
    # Drop rows with non-finite norm or norm <= 0 (classifier.classify → None).
    keep = np.isfinite(norm_arr) & (norm_arr > 0.0)
    if not keep.all():
        df = df[keep].copy()
        norm_arr = norm_arr[keep]
    if df.empty:
        return pd.DataFrame(columns=orig_cols)

    threshold_arr = factor * norm_arr
    fc_arr = pd.to_numeric(df["forecast_value"], errors="coerce").to_numpy(dtype=float)
    obs_arr = pd.to_numeric(df["observed_value"], errors="coerce").to_numpy(dtype=float)

    # Classify — strict < so equality at threshold → "normal".
    # NaN comparisons return False → "normal", matching classifier.classify.
    fc_class = np.where(fc_arr < threshold_arr, "below", "normal")
    obs_class = np.where(obs_arr < threshold_arr, "below", "normal")

    # Vectorized contingency — mirrors classifier.contingency exactly.
    fc_b = fc_class == "below"
    obs_b = obs_class == "below"
    contingency_arr = np.where(
        fc_b & obs_b,
        "TP",
        np.where(fc_b & ~obs_b, "FP", np.where(~fc_b & obs_b, "FN", "TN")),
    )

    df["fc_class"] = fc_class
    df["obs_class"] = obs_class
    df["contingency"] = contingency_arr

    return df[orig_cols].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Pair reclassification — return-period events
# ---------------------------------------------------------------------------


def reclassify_pairs_for_rp_event(
    pairs: pd.DataFrame,
    event: EventDef,
    return_levels: dict[tuple[str, str, int], dict[float, float]],
) -> pd.DataFrame:
    """Return a copy of *pairs* reclassified for the given return-period event.

    Recomputes ``fc_class``, ``obs_class``, and ``contingency`` based on
    whether each value exceeds the GEV return level for its
    ``(code, horizon, period_key)`` group.  ``"below"`` is used as the
    positive class (= event occurred):

    - Positive (event): value *>* return level → ``fc_class = "below"``.
    - Negative (no event): value *≤* return level → ``fc_class = "normal"``.

    Rows where no return level is available (station/period did not meet the
    ``min_years`` gate or GEV fit failed) are silently dropped.

    Args:
        pairs: Original pair DataFrame with ``fc_class``, ``obs_class``,
            ``contingency``, ``forecast_value``, ``observed_value``, and
            ``period_key`` columns.
        event: The event definition.  Must be a return-period event
            (``event.return_period`` must not be ``None``).
        return_levels: Per-``(code, horizon, period_key)`` return-level
            mappings as returned by :func:`compute_return_levels`.

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

    orig_cols = list(pairs.columns)

    # Build a small lookup DataFrame: one row per (code, horizon, period_key) that
    # has a return level for this event's return_period.
    level_rows: list[tuple[str, str, int, float]] = []
    for (c, h, pk), group_levels in return_levels.items():
        lv = group_levels.get(event.return_period)
        if lv is not None:
            level_rows.append((str(c), str(h), int(pk), float(lv)))

    if not level_rows:
        return pd.DataFrame(columns=orig_cols)

    level_df = pd.DataFrame(level_rows, columns=["_code", "_horizon", "_pk_int", "_level"])

    # Cast period_key to int; coerce failures → NaN → drop.
    df = pairs.copy()
    pk_numeric = pd.to_numeric(df["period_key"], errors="coerce")
    valid_pk = pk_numeric.notna()
    if not valid_pk.all():
        df = df[valid_pk].copy()
        pk_numeric = pk_numeric[valid_pk]
    if df.empty:
        return pd.DataFrame(columns=orig_cols)

    df["_pk_int"] = pk_numeric.astype("int64")
    df["_code"] = df["code"].astype(str)
    df["_horizon"] = df["horizon"].astype(str)

    # Inner join: drops rows whose (code, horizon, period_key) has no return level.
    df = df.merge(level_df, on=["_code", "_horizon", "_pk_int"], how="inner")
    if df.empty:
        return pd.DataFrame(columns=orig_cols)

    # Cast forecast_value / observed_value to float; same NaN-keep semantics
    # as reclassify_pairs_for_event — NaN floats pass through as "normal".
    fc_vals = pd.to_numeric(df["forecast_value"], errors="coerce")
    obs_vals = pd.to_numeric(df["observed_value"], errors="coerce")

    if not pd.api.types.is_numeric_dtype(df["forecast_value"]) or not pd.api.types.is_numeric_dtype(
        df["observed_value"]
    ):

        def _non_castable_rp(col: pd.Series) -> pd.Series:
            if pd.api.types.is_numeric_dtype(col):
                return pd.Series(False, index=col.index)
            return col.apply(
                lambda x: x is None
                or (not isinstance(x, (int, float)) and pd.isna(pd.to_numeric(x, errors="coerce")))
            )

        drop = _non_castable_rp(df["forecast_value"]) | _non_castable_rp(df["observed_value"])
        if drop.any():
            df = df[~drop].copy()
            fc_vals = fc_vals[~drop]
            obs_vals = obs_vals[~drop]
        if df.empty:
            return pd.DataFrame(columns=orig_cols)

    level_arr = df["_level"].to_numpy(dtype=float)
    fc_arr = fc_vals.to_numpy(dtype=float)
    obs_arr = obs_vals.to_numpy(dtype=float)

    # direction "above": positive class = value exceeds the return level (strict >).
    fc_class = np.where(fc_arr > level_arr, "below", "normal")
    obs_class = np.where(obs_arr > level_arr, "below", "normal")

    # Vectorized contingency.
    fc_b = fc_class == "below"
    obs_b = obs_class == "below"
    contingency_arr = np.where(
        fc_b & obs_b,
        "TP",
        np.where(fc_b & ~obs_b, "FP", np.where(~fc_b & obs_b, "FN", "TN")),
    )

    df = df.copy()
    df["fc_class"] = fc_class
    df["obs_class"] = obs_class
    df["contingency"] = contingency_arr

    return df[orig_cols].reset_index(drop=True)
