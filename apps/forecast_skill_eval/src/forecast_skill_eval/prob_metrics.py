"""Probabilistic forecast verification — pure per-pair scorers and reducers.

P2 of the Phase-3 probabilistic verification plan.  This module is self-contained:
no orchestrator import, no DB access, no side effects.  All functions that accept
a quantile band ({level: value}) apply isotonic repair before scoring so quantile
crossings are counted but never silently dropped.

CRPS estimator (Design Decision 2):
    CRPS ≈ 2·∫₀¹ pinball_loss(τ) dτ  via trapezoidal weights on the grid nodes,
    PLUS explicit flat-tail terms on [0, τ_min] and [τ_max, 1] so that an
    observation outside the band is penalised.  The IDENTICAL estimator is used
    for the climatology and persistence reference CRPS (via
    crps_reference_from_samples), making CRPSS unbiased by estimator mismatch.

Cross-grid comparability (Design Decision 3):
    Raw crps is never ranked across fc_grid_id values.  The dashboard/report
    restrict CRPSS ranking to a single fc_grid_id.
"""

from __future__ import annotations

import math
from collections.abc import Iterator, Sequence
from typing import Final, Literal

import numpy as np
import pandas as pd

from forecast_skill_eval.contingency import (
    ALL_BASIN,
    ALL_PROVENANCE,
    ALL_SEASON,
    POOLED_CODE,
)
from forecast_skill_eval.metrics import _wilson_interval
from forecast_skill_eval.periods import LONG_TERM_HORIZONS
from forecast_skill_eval.regimes import ALL_REGIME

# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

QUANTILE_LEVELS: Final[tuple[float, ...]] = (0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95)

# Canonical pairs DataFrame column for each canonical level.
_LEVEL_TO_COL: Final[dict[float, str]] = {
    0.05: "fc_q05",
    0.10: "fc_q10",
    0.25: "fc_q25",
    0.50: "fc_q50",
    0.75: "fc_q75",
    0.90: "fc_q90",
    0.95: "fc_q95",
}

PROB_METRIC_COLUMNS: Final[tuple[str, ...]] = (
    "horizon",
    "model",
    "regime",
    "season",
    "code",
    "basin",
    "norm_provenance",
    "lead",
    "event",
    "fc_grid_id",
    "n_pairs",
    "crps",
    "crps_clim",
    "crpss",
    "crps_persist",
    "crpss_persist",
    "coverage_50",
    "coverage_80",
    "coverage_90",
    "coverage_ci_lower",
    "coverage_ci_upper",
    "reliability_50",
    "reliability_80",
    "reliability_90",
    "nominal_50",
    "nominal_80",
    "nominal_90",
    "sharpness_iqr",
    "sharpness_width",
    "sharpness_width_norm",
    "rank_mean",
    "rank_var",
    "rank_calibration_error",
    "brier",
    "brier_ss",
)

PROB_RELIABILITY_COLUMNS: Final[tuple[str, ...]] = (
    "horizon",
    "model",
    "regime",
    "season",
    "code",
    "basin",
    "norm_provenance",
    "lead",
    "fc_grid_id",
    "nominal_level",
    "observed_frequency",
    "n",
)

# Group keys mirroring count_contingencies (8-key structure).
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


# ---------------------------------------------------------------------------
# Primitive scorers
# ---------------------------------------------------------------------------


def isotonic_band(
    levels: Sequence[float],
    quantiles: Sequence[float],
) -> tuple[list[float], list[float], bool]:
    """Drop NaN nodes, sort by level, enforce non-decreasing via cumulative max.

    Args:
        levels: Quantile probability levels (e.g. 0.05, 0.25, ...).
        quantiles: Corresponding quantile values.

    Returns:
        Tuple of (repaired_levels, repaired_quantiles, was_repaired).
        ``was_repaired`` is True when at least one quantile was clipped upward.
        Both output lists are empty when there are no finite node pairs.
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
        return [], [], False

    node_pairs.sort(key=lambda x: x[0])
    sorted_levels = [p[0] for p in node_pairs]
    sorted_quantiles = [p[1] for p in node_pairs]

    was_repaired = False
    repaired: list[float] = [sorted_quantiles[0]]
    for qv in sorted_quantiles[1:]:
        running_max = repaired[-1]
        if qv < running_max:
            was_repaired = True
            repaired.append(running_max)
        else:
            repaired.append(qv)

    return sorted_levels, repaired, was_repaired


def crps_from_quantiles(
    levels: Sequence[float],
    quantiles: Sequence[float],
    observed: float,
) -> float:
    """Approximate CRPS via trapezoidal integration plus explicit flat-tail terms.

    CRPS = 2·∫₀¹ pinball_loss(τ, obs) dτ

    The integral is split into three parts:
    - Left tail [0, τ_min]: flat quantile function q(τ) = q_min.
    - Middle [τ_min, τ_max]: trapezoidal integration over the grid nodes.
    - Right tail [τ_max, 1]: flat quantile function q(τ) = q_max.

    The flat-tail treatment ensures that an observation beyond the band receives
    a positive penalty (Design Decision 2 — narrow overconfident bands are NOT
    rewarded).  The IDENTICAL estimator is used for the climatology reference
    via :func:`crps_reference_from_samples`, so CRPSS is estimator-consistent.

    Args:
        levels: Quantile probability levels (will be isotonic-repaired).
        quantiles: Corresponding quantile values.
        observed: The scalar observation to score.

    Returns:
        Approximate CRPS ≥ 0, or ``math.nan`` when fewer than 2 finite nodes
        remain after isotonic repair or when ``observed`` is non-finite.
    """
    lev, qval, _ = isotonic_band(levels, quantiles)
    if len(lev) < 2:
        return math.nan
    if not math.isfinite(observed):
        return math.nan

    obs = observed

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
        # ∫₀^τ_min τ·(obs - q_min) dτ = (obs - q_min)·τ_min²/2
        left_tail = (obs - q_min) * tau_min**2 / 2.0
    else:
        # ∫₀^τ_min (1-τ)·(q_min - obs) dτ = (q_min - obs)·(τ_min - τ_min²/2)
        left_tail = (q_min - obs) * (tau_min - tau_min**2 / 2.0)

    # --- Right tail: [tau_max, 1] flat at q_max ---
    tau_max = lev[-1]
    q_max = qval[-1]
    if obs <= q_max:
        # ∫_τ_max^1 (1-τ)·(q_max - obs) dτ = (q_max - obs)·(1 - τ_max)²/2
        right_tail = (q_max - obs) * (1.0 - tau_max) ** 2 / 2.0
    else:
        # ∫_τ_max^1 τ·(obs - q_max) dτ = (obs - q_max)·(1 - τ_max²)/2
        right_tail = (obs - q_max) * (1.0 - tau_max**2) / 2.0

    return 2.0 * (left_tail + middle + right_tail)


def crps_reference_from_samples(
    sample: Sequence[float],
    observed: float,
    levels: Sequence[float],
) -> float:
    """CRPS of an empirical reference distribution using the IDENTICAL estimator.

    Samples the reference distribution's quantiles at ``levels`` and feeds them
    to :func:`crps_from_quantiles`, so CRPSS = 1 − CRPS/CRPS_ref is unbiased
    by estimator mismatch (Design Decision 2).

    Args:
        sample: Reference distribution samples (e.g. climatology observations).
        observed: The scalar observation to score.
        levels: Quantile probability levels to use — must match the forecast
            grid so the estimator is consistent.

    Returns:
        Reference CRPS ≥ 0, or ``math.nan`` when the sample is empty or
        has fewer than 2 finite values, or when ``observed`` is non-finite.
    """
    finite_sample = [float(v) for v in sample if math.isfinite(float(v))]
    if len(finite_sample) < 2:
        return math.nan
    if not math.isfinite(observed):
        return math.nan

    finite_levels = [float(lv) for lv in levels if math.isfinite(float(lv))]
    if len(finite_levels) < 2:
        return math.nan

    ref_quantiles = np.quantile(finite_sample, finite_levels).tolist()
    return crps_from_quantiles(finite_levels, ref_quantiles, observed)


def coverage_hit(lower: float, upper: float, observed: float) -> float:
    """Return 1.0 if lower ≤ observed ≤ upper, else 0.0.

    Args:
        lower: Lower bound of the prediction interval.
        upper: Upper bound of the prediction interval.
        observed: The observation to check.

    Returns:
        1.0 (hit) or 0.0 (miss), or ``math.nan`` when any bound is non-finite.
    """
    if not math.isfinite(lower) or not math.isfinite(upper):
        return math.nan
    if not math.isfinite(observed):
        return math.nan
    return 1.0 if lower <= observed <= upper else 0.0


def interval_width(lower: float, upper: float) -> float:
    """Return upper − lower.

    Args:
        lower: Lower bound.
        upper: Upper bound.

    Returns:
        Width ≥ 0, or ``math.nan`` when any bound is non-finite.
    """
    if not math.isfinite(lower) or not math.isfinite(upper):
        return math.nan
    return upper - lower


def rank_position(
    levels: Sequence[float],
    quantiles: Sequence[float],
    observed: float,
) -> float:
    """Predictive-CDF value at ``observed`` via linear interpolation, clamped to [0, 1].

    Feeds the coarse reliability/rank table (5–7 nodes only; NOT a fine PIT
    histogram).

    Args:
        levels: Quantile probability levels.
        quantiles: Corresponding quantile values (isotonic repair applied).
        observed: The observation.

    Returns:
        Value in [0, 1] representing the CDF of the predictive distribution at
        ``observed``, or ``math.nan`` when fewer than 2 finite nodes exist.
    """
    lev, qval, _ = isotonic_band(levels, quantiles)
    if len(lev) < 2:
        return math.nan
    if not math.isfinite(observed):
        return math.nan

    if observed <= qval[0]:
        return 0.0
    if observed >= qval[-1]:
        return 1.0

    for i in range(len(qval) - 1):
        q_lo, q_hi = qval[i], qval[i + 1]
        if q_lo <= observed <= q_hi:
            tau_lo, tau_hi = lev[i], lev[i + 1]
            if q_hi == q_lo:
                return (tau_lo + tau_hi) / 2.0
            return tau_lo + (tau_hi - tau_lo) * (observed - q_lo) / (q_hi - q_lo)

    return 1.0


def event_probability(
    levels: Sequence[float],
    quantiles: Sequence[float],
    threshold: float,
    direction: Literal["below", "above"],
) -> float:
    """P(X < threshold) or P(X > threshold) via CDF interpolation on the grid.

    Intended for interior thresholds (below_norm).  Values at or beyond the
    band edges saturate at 0/1 (Design Decision 1 — tail events are flag-only).

    Args:
        levels: Quantile probability levels.
        quantiles: Corresponding quantile values.
        threshold: The event threshold.
        direction: ``"below"`` → P(X < threshold); ``"above"`` → P(X > threshold).

    Returns:
        Probability in [0, 1], or ``math.nan`` when fewer than 2 finite nodes
        exist or when ``threshold`` is non-finite.
    """
    lev, qval, _ = isotonic_band(levels, quantiles)
    if len(lev) < 2:
        return math.nan
    if not math.isfinite(threshold):
        return math.nan

    # CDF(threshold) = P(X ≤ threshold) under the piecewise-linear model.
    if threshold <= qval[0]:
        cdf = 0.0
    elif threshold >= qval[-1]:
        cdf = 1.0
    else:
        cdf = 0.0
        for i in range(len(qval) - 1):
            q_lo, q_hi = qval[i], qval[i + 1]
            if q_lo <= threshold <= q_hi:
                tau_lo, tau_hi = lev[i], lev[i + 1]
                if q_hi == q_lo:
                    cdf = (tau_lo + tau_hi) / 2.0
                else:
                    cdf = tau_lo + (tau_hi - tau_lo) * (threshold - q_lo) / (q_hi - q_lo)
                break

    return cdf if direction == "below" else 1.0 - cdf


def brier_score(forecast_prob: float, observed_event: bool) -> float:
    """Brier score: (forecast_prob − 𝟙[event])².

    Args:
        forecast_prob: Forecast probability in [0, 1].
        observed_event: Whether the event occurred.

    Returns:
        Brier score ≥ 0, or ``math.nan`` when ``forecast_prob`` is non-finite.
    """
    if not math.isfinite(forecast_prob):
        return math.nan
    indicator = 1.0 if observed_event else 0.0
    return (forecast_prob - indicator) ** 2


# ---------------------------------------------------------------------------
# Per-pair scorer
# ---------------------------------------------------------------------------


def _score_pairs(
    pairs: pd.DataFrame,
    threshold: float = 0.80,
) -> pd.DataFrame:
    """Add per-pair probabilistic score columns to a pairs DataFrame.

    Extracts the quantile band from fc_q05…fc_q95 columns (NaN = absent node),
    applies isotonic repair, then scores each row.  Rows with fewer than 2
    finite quantile nodes receive NaN for all score columns.

    Args:
        pairs: Pairs DataFrame containing fc_q* columns, ``observed_value``,
            and ``norm``.  The existing columns are not modified.
        threshold: Below-norm threshold fraction (config.threshold, default
            0.80 — mirrors classifier.classify; NOT hardcoded).

    Returns:
        Copy of ``pairs`` with additional columns: ``crps``, ``rank``,
        ``hit_50``, ``hit_80``, ``hit_90``, ``width_iqr``, ``width_outer``,
        ``width_outer_norm``, ``below_norm_prob``, ``n_band_repaired``.
    """
    if pairs.empty:
        scored = pairs.copy()
        for col in (
            "crps",
            "rank",
            "hit_50",
            "hit_80",
            "hit_90",
            "width_iqr",
            "width_outer",
            "width_outer_norm",
            "below_norm_prob",
            "n_band_repaired",
        ):
            scored[col] = pd.Series(dtype="float64")
        return scored

    rows = pairs.to_dict("records")
    out_rows: list[dict] = []

    for row in rows:
        scored_row = dict(row)

        # --- Extract quantile band from fc_q* columns ---
        band_levels: list[float] = []
        band_qvals: list[float] = []
        for lvl, col in _LEVEL_TO_COL.items():
            if col not in row:
                continue
            raw = row[col]
            if raw is None:
                continue
            try:
                v = float(raw)
            except (TypeError, ValueError):
                continue
            if math.isfinite(v):
                band_levels.append(lvl)
                band_qvals.append(v)

        repaired_levels, repaired_qvals, was_repaired = isotonic_band(band_levels, band_qvals)
        scored_row["n_band_repaired"] = 1 if was_repaired else 0

        # --- Observed value and norm ---
        obs_raw = row.get("observed_value")
        norm_raw = row.get("norm")

        try:
            obs_f: float | None = float(obs_raw) if obs_raw is not None else None
            if obs_f is not None and not math.isfinite(obs_f):
                obs_f = None
        except (TypeError, ValueError):
            obs_f = None

        try:
            norm_f: float | None = float(norm_raw) if norm_raw is not None else None
            if norm_f is not None and (not math.isfinite(norm_f) or norm_f <= 0):
                norm_f = None
        except (TypeError, ValueError):
            norm_f = None

        if len(repaired_levels) < 2 or obs_f is None:
            # No valid band or no observation — all NaN
            for col in (
                "crps",
                "rank",
                "hit_50",
                "hit_80",
                "hit_90",
                "width_iqr",
                "width_outer",
                "width_outer_norm",
                "below_norm_prob",
            ):
                scored_row[col] = math.nan
            out_rows.append(scored_row)
            continue

        # --- CRPS ---
        scored_row["crps"] = crps_from_quantiles(repaired_levels, repaired_qvals, obs_f)

        # --- Rank / PIT ---
        scored_row["rank"] = rank_position(repaired_levels, repaired_qvals, obs_f)

        # --- Coverage at 50% (q25/q75), 80% (q10/q90), 90% (q05/q95) ---
        q_vals = _extract_band_map(repaired_levels, repaired_qvals)
        q25 = q_vals.get(0.25)
        q75 = q_vals.get(0.75)
        q10 = q_vals.get(0.10)
        q90 = q_vals.get(0.90)
        q05 = q_vals.get(0.05)
        q95 = q_vals.get(0.95)

        scored_row["hit_50"] = (
            coverage_hit(q25, q75, obs_f) if q25 is not None and q75 is not None else math.nan
        )
        scored_row["hit_80"] = (
            coverage_hit(q10, q90, obs_f) if q10 is not None and q90 is not None else math.nan
        )
        scored_row["hit_90"] = (
            coverage_hit(q05, q95, obs_f) if q05 is not None and q95 is not None else math.nan
        )

        # --- Sharpness ---
        scored_row["width_iqr"] = (
            interval_width(q25, q75) if q25 is not None and q75 is not None else math.nan
        )
        scored_row["width_outer"] = (
            interval_width(q05, q95) if q05 is not None and q95 is not None else math.nan
        )
        scored_row["width_outer_norm"] = (
            scored_row["width_outer"] / norm_f
            if norm_f is not None and math.isfinite(scored_row["width_outer"])
            else math.nan
        )

        # --- Below-norm event probability ---
        if norm_f is not None:
            threshold_val = threshold * norm_f
            scored_row["below_norm_prob"] = event_probability(
                repaired_levels, repaired_qvals, threshold_val, "below"
            )
        else:
            scored_row["below_norm_prob"] = math.nan

        out_rows.append(scored_row)

    return pd.DataFrame(out_rows)


def _extract_band_map(levels: list[float], qvals: list[float]) -> dict[float, float]:
    """Return {level: quantile_value} for a repaired band."""
    return {lv: qv for lv, qv in zip(levels, qvals, strict=False)}


# ---------------------------------------------------------------------------
# Reducer — compute_probabilistic_metrics
# ---------------------------------------------------------------------------


def compute_probabilistic_metrics(
    pairs: pd.DataFrame,
    thresholds: dict,
    clim_ref: dict,
    events_filter: tuple[str, ...],
    *,
    threshold: float = 0.80,
    persist_ref: dict | None = None,
) -> pd.DataFrame:
    """Score pairs and aggregate across the same 8-key slice structure as
    count_contingencies (POOLED + per-code; per-lead for long-term).

    Emits two event rows per group:
    - ``event="distribution"``: carries CRPS/CRPSS/coverage/sharpness/rank.
      Brier columns are NaN.
    - ``event="below_norm"``: carries Brier/Brier-skill.  CRPS/coverage/rank
      columns are NaN.  Only emitted when "below_norm" is in events_filter.

    Args:
        pairs: Pairs DataFrame (must include fc_q* columns, observed_value, norm,
            and the 8 group-key columns).
        thresholds: Unused directly here (reserved for future percentile-event
            Brier); pass ``{}`` if not needed.
        clim_ref: Precomputed climatology reference quantiles per conditioning
            group.  Produced by :func:`prob_baselines.precompute_climatology_crps`.
            Maps (code, horizon, period_key) → (levels, quantile_values).
        events_filter: Subset of event names to include (at minimum "below_norm"
            for Brier rows to be emitted).
        threshold: Below-norm threshold fraction (mirrors classifier.classify).
        persist_ref: Optional persistence reference.  Maps
            (code, horizon, period_key, year) → lag1_observed_value.

    Returns:
        DataFrame with ``PROB_METRIC_COLUMNS``.
    """
    if pairs.empty:
        return pd.DataFrame(columns=PROB_METRIC_COLUMNS)

    scored = _score_pairs(pairs, threshold=threshold)

    # Attach crps_clim and crps_persist per row.
    scored = _attach_reference_crps(scored, clim_ref, persist_ref)

    frames: list[pd.DataFrame] = []
    working = _ensure_group_columns(scored)

    for basin, basin_frame in _basin_slices(working):
        for provenance, prov_frame in _provenance_slices(basin_frame):
            for regime, regime_frame in _regime_slices(prov_frame):
                for season, season_frame in _season_slices(regime_frame):
                    frames.extend(
                        _metric_scopes(
                            season_frame,
                            basin,
                            provenance,
                            regime,
                            season,
                            events_filter=events_filter,
                        )
                    )

    if not frames:
        return pd.DataFrame(columns=PROB_METRIC_COLUMNS)

    result = pd.concat(frames, ignore_index=True)
    # Ensure all columns present
    for col in PROB_METRIC_COLUMNS:
        if col not in result.columns:
            result[col] = math.nan
    return result.loc[:, list(PROB_METRIC_COLUMNS)].reset_index(drop=True)


def _attach_reference_crps(
    scored: pd.DataFrame,
    clim_ref: dict,
    persist_ref: dict | None,
) -> pd.DataFrame:
    """Add crps_clim and crps_persist columns to the scored pairs DataFrame."""
    clim_vals: list[float] = []
    persist_vals: list[float] = []

    for row in scored.to_dict("records"):
        code = str(row.get("code", ""))
        horizon = str(row.get("horizon", ""))
        obs_raw = row.get("observed_value")

        try:
            period_key = int(row["period_key"])
        except (TypeError, ValueError, KeyError):
            period_key = -1

        try:
            obs_f = float(obs_raw) if obs_raw is not None else math.nan
            if not math.isfinite(obs_f):
                obs_f = math.nan
        except (TypeError, ValueError):
            obs_f = math.nan

        # Climatology reference CRPS
        clim_key = (code, horizon, period_key)
        clim_entry = clim_ref.get(clim_key)
        if clim_entry is not None and not math.isnan(obs_f):
            clim_levels, clim_qvals = clim_entry
            crps_clim = crps_from_quantiles(clim_levels, clim_qvals, obs_f)
        else:
            crps_clim = math.nan
        clim_vals.append(crps_clim)

        # Persistence reference CRPS = |lag1_obs - obs| (degenerate zero-spread)
        if persist_ref is not None:
            try:
                year = int(row["year"])
            except (TypeError, ValueError, KeyError):
                year = -1
            persist_key = (code, horizon, period_key, year)
            lag1_obs = persist_ref.get(persist_key)
            if lag1_obs is not None and not math.isnan(obs_f):
                crps_persist = abs(lag1_obs - obs_f)
            else:
                crps_persist = math.nan
        else:
            crps_persist = math.nan
        persist_vals.append(crps_persist)

    out = scored.copy()
    out["crps_clim"] = clim_vals
    out["crps_persist"] = persist_vals
    return out


def _ensure_group_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Add missing group-key columns with sensible defaults."""
    out = frame.copy()
    if "season" not in out.columns:
        out["season"] = ALL_SEASON
    if "basin" not in out.columns:
        out["basin"] = ALL_BASIN
    if "norm_provenance" not in out.columns:
        out["norm_provenance"] = ALL_PROVENANCE
    if "lead" not in out.columns:
        out["lead"] = None
    return out


def _metric_scopes(
    frame: pd.DataFrame,
    basin: str,
    provenance: str,
    regime: str,
    season: str,
    events_filter: tuple[str, ...],
) -> list[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    for horizon, h_frame in frame.groupby("horizon", dropna=False, sort=True):
        is_long = str(horizon) in LONG_TERM_HORIZONS
        for pooled in (False, True):
            group_cols = ["horizon", "model"]
            if not pooled:
                group_cols.append("code")
            if is_long:
                group_cols = [*group_cols, "lead"]

            for keys, g_frame in h_frame.groupby(group_cols, dropna=False, sort=True):
                if not isinstance(keys, tuple):
                    keys = (keys,)
                key_dict = dict(zip(group_cols, keys, strict=False))

                code_val = POOLED_CODE if pooled else str(key_dict.get("code", ""))

                dist_row = _aggregate_distribution(
                    g_frame,
                    horizon=str(horizon),
                    model=str(key_dict.get("model", "")),
                    regime=regime,
                    season=season,
                    code=code_val,
                    basin=basin,
                    norm_provenance=provenance,
                    lead=key_dict.get("lead") if is_long else None,
                )
                frames.append(pd.DataFrame([dist_row]))

                if "below_norm" in events_filter:
                    brier_row = _aggregate_brier(
                        g_frame,
                        horizon=str(horizon),
                        model=str(key_dict.get("model", "")),
                        regime=regime,
                        season=season,
                        code=code_val,
                        basin=basin,
                        norm_provenance=provenance,
                        lead=key_dict.get("lead") if is_long else None,
                    )
                    frames.append(pd.DataFrame([brier_row]))

    return frames


def _nan_mean(series: pd.Series) -> float:
    vals = series.dropna()
    return float(vals.mean()) if len(vals) > 0 else math.nan


def _nan_count(series: pd.Series) -> int:
    return int(series.notna().sum())


def _crpss(crps_fc: float, crps_ref: float) -> float:
    if not math.isfinite(crps_fc) or not math.isfinite(crps_ref) or crps_ref == 0.0:
        return math.nan
    return 1.0 - crps_fc / crps_ref


def _aggregate_distribution(
    frame: pd.DataFrame,
    *,
    horizon: str,
    model: str,
    regime: str,
    season: str,
    code: str,
    basin: str,
    norm_provenance: str,
    lead: object,
) -> dict:
    n = len(frame)
    grid_id = str(frame["fc_grid_id"].iloc[0]) if "fc_grid_id" in frame.columns else ""

    crps_mean = _nan_mean(frame["crps"])
    crps_clim_mean = _nan_mean(frame["crps_clim"])
    crps_persist_mean = _nan_mean(frame["crps_persist"])

    # Coverage at 50%, 80%, 90%
    cov_50 = _nan_mean(frame["hit_50"]) if "hit_50" in frame.columns else math.nan
    cov_80 = _nan_mean(frame["hit_80"]) if "hit_80" in frame.columns else math.nan
    cov_90 = _nan_mean(frame["hit_90"]) if "hit_90" in frame.columns else math.nan
    n_cov_90 = _nan_count(frame["hit_90"]) if "hit_90" in frame.columns else 0

    # Wilson CI on the widest available coverage (90% preferred)
    ci_lower, ci_upper = math.nan, math.nan
    if n_cov_90 > 0 and math.isfinite(cov_90):
        ci_lower, ci_upper, _ = _wilson_interval(cov_90 * n_cov_90, n_cov_90)

    # Reliability = |coverage − nominal|
    rel_50 = abs(cov_50 - 0.50) if math.isfinite(cov_50) else math.nan
    rel_80 = abs(cov_80 - 0.80) if math.isfinite(cov_80) else math.nan
    rel_90 = abs(cov_90 - 0.90) if math.isfinite(cov_90) else math.nan

    # Rank stats
    rank_vals = frame["rank"].dropna() if "rank" in frame.columns else pd.Series(dtype=float)
    rank_mean = float(rank_vals.mean()) if len(rank_vals) > 0 else math.nan
    rank_var = float(rank_vals.var()) if len(rank_vals) > 1 else math.nan
    # Calibration error: mean |rank - uniform_mean| (uniform mean = 0.5)
    rank_cal_err = float((rank_vals - 0.5).abs().mean()) if len(rank_vals) > 0 else math.nan

    return {
        "horizon": horizon,
        "model": model,
        "regime": regime,
        "season": season,
        "code": code,
        "basin": basin,
        "norm_provenance": norm_provenance,
        "lead": lead,
        "event": "distribution",
        "fc_grid_id": grid_id,
        "n_pairs": n,
        "crps": crps_mean,
        "crps_clim": crps_clim_mean,
        "crpss": _crpss(crps_mean, crps_clim_mean),
        "crps_persist": crps_persist_mean,
        "crpss_persist": _crpss(crps_mean, crps_persist_mean),
        "coverage_50": cov_50,
        "coverage_80": cov_80,
        "coverage_90": cov_90,
        "coverage_ci_lower": ci_lower,
        "coverage_ci_upper": ci_upper,
        "reliability_50": rel_50,
        "reliability_80": rel_80,
        "reliability_90": rel_90,
        "nominal_50": 0.50,
        "nominal_80": 0.80,
        "nominal_90": 0.90,
        "sharpness_iqr": (
            _nan_mean(frame["width_iqr"]) if "width_iqr" in frame.columns else math.nan
        ),
        "sharpness_width": (
            _nan_mean(frame["width_outer"]) if "width_outer" in frame.columns else math.nan
        ),
        "sharpness_width_norm": (
            _nan_mean(frame["width_outer_norm"])
            if "width_outer_norm" in frame.columns
            else math.nan
        ),
        "rank_mean": rank_mean,
        "rank_var": rank_var,
        "rank_calibration_error": rank_cal_err,
        # NaN for Brier columns in distribution rows
        "brier": math.nan,
        "brier_ss": math.nan,
    }


def _aggregate_brier(
    frame: pd.DataFrame,
    *,
    horizon: str,
    model: str,
    regime: str,
    season: str,
    code: str,
    basin: str,
    norm_provenance: str,
    lead: object,
) -> dict:
    """Aggregate below_norm Brier scores for a group."""
    n = len(frame)
    grid_id = str(frame["fc_grid_id"].iloc[0]) if "fc_grid_id" in frame.columns else ""

    # Compute Brier score per pair: need below_norm_prob and observed event
    brier_vals: list[float] = []

    for row in frame.to_dict("records"):
        fc_prob = row.get("below_norm_prob", math.nan)
        obs_class = row.get("obs_class")
        # obs_class == "below" means the event occurred
        if obs_class is None:
            continue
        event_occurred = str(obs_class) == "below"
        bs = brier_score(float(fc_prob) if fc_prob is not None else math.nan, event_occurred)
        if math.isfinite(bs):
            brier_vals.append(bs)

        # Climatology Brier reference = base_rate * (1 - base_rate)
        # Using mean observed event rate as clim forecast probability
        # (constant forecast = historical base rate)

    brier_mean = float(np.mean(brier_vals)) if brier_vals else math.nan

    # Climatology Brier reference (base rate × (1 - base_rate))
    if "obs_class" in frame.columns:
        obs_classes = frame["obs_class"].dropna()
        base_rate = float((obs_classes == "below").mean()) if len(obs_classes) > 0 else math.nan
        brier_clim = base_rate * (1.0 - base_rate) if math.isfinite(base_rate) else math.nan
    else:
        brier_clim = math.nan

    brier_ss = (
        1.0 - brier_mean / brier_clim
        if math.isfinite(brier_mean) and math.isfinite(brier_clim) and brier_clim > 0
        else math.nan
    )

    dist_nan = math.nan
    return {
        "horizon": horizon,
        "model": model,
        "regime": regime,
        "season": season,
        "code": code,
        "basin": basin,
        "norm_provenance": norm_provenance,
        "lead": lead,
        "event": "below_norm",
        "fc_grid_id": grid_id,
        "n_pairs": n,
        # NaN for distribution columns in below_norm rows
        "crps": dist_nan,
        "crps_clim": dist_nan,
        "crpss": dist_nan,
        "crps_persist": dist_nan,
        "crpss_persist": dist_nan,
        "coverage_50": dist_nan,
        "coverage_80": dist_nan,
        "coverage_90": dist_nan,
        "coverage_ci_lower": dist_nan,
        "coverage_ci_upper": dist_nan,
        "reliability_50": dist_nan,
        "reliability_80": dist_nan,
        "reliability_90": dist_nan,
        "nominal_50": dist_nan,
        "nominal_80": dist_nan,
        "nominal_90": dist_nan,
        "sharpness_iqr": dist_nan,
        "sharpness_width": dist_nan,
        "sharpness_width_norm": dist_nan,
        "rank_mean": dist_nan,
        "rank_var": dist_nan,
        "rank_calibration_error": dist_nan,
        "brier": brier_mean,
        "brier_ss": brier_ss,
    }


# ---------------------------------------------------------------------------
# Reliability builder
# ---------------------------------------------------------------------------


def build_prob_reliability(pairs: pd.DataFrame) -> pd.DataFrame:
    """Build a long reliability table: group × nominal_level.

    For each nominal level τ in QUANTILE_LEVELS and each group (mirroring the
    count_contingencies 8-key structure), emits the empirical frequency of
    ``observed <= fc_q_{τ}`` and the count of valid pairs.

    A calibrated forecast should show ``observed_frequency ≈ nominal_level``.
    This is a coarse-resolution table (5–7 nodes only — NOT a fine PIT
    histogram).

    Args:
        pairs: Scored pairs DataFrame containing fc_q* columns,
            ``observed_value``, and the 8 group-key columns.

    Returns:
        Long DataFrame with ``PROB_RELIABILITY_COLUMNS``.
    """
    if pairs.empty:
        return pd.DataFrame(columns=PROB_RELIABILITY_COLUMNS)

    working = _ensure_group_columns(pairs)
    frames: list[pd.DataFrame] = []

    for basin, basin_frame in _basin_slices(working):
        for provenance, prov_frame in _provenance_slices(basin_frame):
            for regime, regime_frame in _regime_slices(prov_frame):
                for season, season_frame in _season_slices(regime_frame):
                    frames.extend(
                        _reliability_scopes(season_frame, basin, provenance, regime, season)
                    )

    if not frames:
        return pd.DataFrame(columns=PROB_RELIABILITY_COLUMNS)

    result = pd.concat(frames, ignore_index=True)
    return result.loc[:, list(PROB_RELIABILITY_COLUMNS)].reset_index(drop=True)


def _reliability_scopes(
    frame: pd.DataFrame,
    basin: str,
    provenance: str,
    regime: str,
    season: str,
) -> list[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    for horizon, h_frame in frame.groupby("horizon", dropna=False, sort=True):
        is_long = str(horizon) in LONG_TERM_HORIZONS
        for pooled in (False, True):
            group_cols = ["horizon", "model"]
            if not pooled:
                group_cols.append("code")
            if is_long:
                group_cols = [*group_cols, "lead"]

            for keys, g_frame in h_frame.groupby(group_cols, dropna=False, sort=True):
                if not isinstance(keys, tuple):
                    keys = (keys,)
                key_dict = dict(zip(group_cols, keys, strict=False))
                code_val = POOLED_CODE if pooled else str(key_dict.get("code", ""))
                grid_id = (
                    str(g_frame["fc_grid_id"].iloc[0]) if "fc_grid_id" in g_frame.columns else ""
                )

                obs_series = (
                    g_frame["observed_value"].astype(float)
                    if "observed_value" in g_frame.columns
                    else pd.Series(dtype=float)
                )

                for lvl in QUANTILE_LEVELS:
                    col = _LEVEL_TO_COL.get(lvl)
                    if col is None or col not in g_frame.columns:
                        continue
                    q_series = g_frame[col].astype(float)
                    valid_mask = q_series.notna() & obs_series.notna()
                    n_valid = int(valid_mask.sum())
                    if n_valid == 0:
                        continue
                    hits = (obs_series[valid_mask] <= q_series[valid_mask]).astype(float)
                    obs_freq = float(hits.mean())

                    frames.append(
                        pd.DataFrame(
                            [
                                {
                                    "horizon": str(horizon),
                                    "model": str(key_dict.get("model", "")),
                                    "regime": regime,
                                    "season": season,
                                    "code": code_val,
                                    "basin": basin,
                                    "norm_provenance": provenance,
                                    "lead": key_dict.get("lead") if is_long else None,
                                    "fc_grid_id": grid_id,
                                    "nominal_level": lvl,
                                    "observed_frequency": obs_freq,
                                    "n": n_valid,
                                }
                            ]
                        )
                    )

    return frames


# ---------------------------------------------------------------------------
# Slice helpers (mirrors contingency.py — first-cut replication per plan)
# ---------------------------------------------------------------------------


def _basin_slices(frame: pd.DataFrame) -> Iterator[tuple[str, pd.DataFrame]]:
    yield ALL_BASIN, frame
    basins = sorted(str(v) for v in frame["basin"].dropna().unique())
    for basin in basins:
        yield basin, frame[frame["basin"] == basin]


def _provenance_slices(frame: pd.DataFrame) -> Iterator[tuple[str, pd.DataFrame]]:
    yield ALL_PROVENANCE, frame
    provenances = sorted(str(v) for v in frame["norm_provenance"].dropna().unique())
    for prov in provenances:
        yield prov, frame[frame["norm_provenance"] == prov]


def _regime_slices(frame: pd.DataFrame) -> Iterator[tuple[str, pd.DataFrame]]:
    yield ALL_REGIME, frame
    regimes = sorted(str(v) for v in frame["regime"].dropna().unique() if str(v) != ALL_REGIME)
    for regime in regimes:
        yield regime, frame[frame["regime"] == regime]


def _season_slices(frame: pd.DataFrame) -> Iterator[tuple[str, pd.DataFrame]]:
    yield ALL_SEASON, frame
    seasons = sorted(str(v) for v in frame["season"].dropna().unique() if str(v) != ALL_SEASON)
    for season in seasons:
        yield season, frame[frame["season"] == season]
