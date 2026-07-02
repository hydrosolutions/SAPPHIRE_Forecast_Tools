"""Continuous/volume accuracy metrics — pure per-group reducers (Phase-4, Part A).

Self-contained: no orchestrator import, no DB access, no side effects.  The
reducers import the ``prob_metrics`` slice/scope helpers so the emitted group
keys are byte-identical to the probabilistic layer (no fan-out drift).  This
module is flag-agnostic — the ``SAPPHIRE_SKILL_VALUE`` gate lives at the
orchestrator boundary.

Metric definitions (see the Phase-4 plan §2.1):
    bias   = mean(fc - obs)
    mae    = mean(|fc - obs|)
    rve    = (sum(fc) - sum(obs)) / sum(obs)          (== kge_beta - 1)
    kge    = 1 - sqrt((r-1)^2 + (alpha-1)^2 + (beta-1)^2)   (KGE-2009)
             r = corrcoef(fc, obs), alpha = std_fc/std_obs, beta = mean_fc/mean_obs
    nse    = 1 - sum((fc-obs)^2) / sum((obs-mean_obs)^2)

Seasonal Apr-Sep volume (the allocation headline) is a day-weighted TRUE volume
in cubic metres: forecast_value/observed_value are period-MEAN discharges in
m^3/s, so V = sum_p (mean_flow_p * days_p * 86400).
"""

from __future__ import annotations

import calendar
import math
import warnings
from typing import Final

import numpy as np
import pandas as pd

from forecast_skill_eval.contingency import POOLED_CODE
from forecast_skill_eval.ledger import ExclusionLedger
from forecast_skill_eval.periods import LONG_TERM_HORIZONS
from forecast_skill_eval.prob_metrics import (
    _basin_slices,
    _ensure_group_columns,
    _provenance_slices,
    _regime_slices,
    _season_slices,
)

# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

# Minimum per-group pair count below which variance-sensitive metrics
# (kge*, nse) are suppressed to NaN.  Distinct from config.min_years (a *years*
# threshold for percentiles — wrong semantics here).  Reused by Part B (REV).
MIN_PAIRS_FOR_VARIANCE_METRICS: Final[int] = 10

# Horizons whose targets tile the Apr-Sep irrigation season.  ``day`` is
# excluded (short archive, no allocation value); ``quarter``/``season`` are
# excluded (``season`` already IS the Apr-Sep aggregate; ``quarter`` maps to a
# single month) — summing them would double-count.
_SEASONAL_VOLUME_HORIZONS: Final[tuple[str, ...]] = ("pentad", "decade", "month")

# Expected number of target sub-periods across months 4-9 (Apr-Sep) per horizon.
_EXPECTED_PERIODS: Final[dict[str, int]] = {"pentad": 36, "decade": 18, "month": 6}

_SECONDS_PER_DAY: Final[int] = 86_400

_IRRIGATION_SEASON: Final[str] = "irrigation"

CONTINUOUS_METRIC_COLUMNS: Final[tuple[str, ...]] = (
    "horizon",
    "model",
    "regime",
    "season",
    "code",
    "basin",
    "norm_provenance",
    "lead",
    "n_pairs",
    "bias",
    "mae",
    "rve",
    "kge",
    "kge_r",
    "kge_alpha",
    "kge_beta",
    "nse",
)

SEASONAL_VOLUME_COLUMNS: Final[tuple[str, ...]] = (
    "horizon",
    "model",
    "regime",
    "code",
    "basin",
    "norm_provenance",
    "lead",
    "year",
    "n_periods",
    "expected_periods",
    "season_complete",
    "season_volume_m3_fc",
    "season_volume_m3_obs",
    "seasonal_volume_error",
    "mean_flow_fc",
    "mean_flow_obs",
)

SEASONAL_VOLUME_SUMMARY_COLUMNS: Final[tuple[str, ...]] = (
    "horizon",
    "model",
    "regime",
    "code",
    "basin",
    "norm_provenance",
    "lead",
    "n_years",
    "seasonal_volume_error_mean",
    "seasonal_volume_error_median",
)


# ---------------------------------------------------------------------------
# Primitives (pure array reducers)
# ---------------------------------------------------------------------------


def bias(fc: np.ndarray, obs: np.ndarray) -> float:
    """Signed mean error ``mean(fc - obs)``.

    Args:
        fc: Forecast values.
        obs: Observed values, aligned element-wise with ``fc``.

    Returns:
        Mean signed error, or ``math.nan`` when there are no pairs.
    """
    fc_arr = np.asarray(fc, dtype=float)
    obs_arr = np.asarray(obs, dtype=float)
    if fc_arr.size == 0:
        return math.nan
    return float(np.mean(fc_arr - obs_arr))


def mae(fc: np.ndarray, obs: np.ndarray) -> float:
    """Mean absolute error ``mean(|fc - obs|)``.

    Args:
        fc: Forecast values.
        obs: Observed values, aligned element-wise with ``fc``.

    Returns:
        Mean absolute error, or ``math.nan`` when there are no pairs.
    """
    fc_arr = np.asarray(fc, dtype=float)
    obs_arr = np.asarray(obs, dtype=float)
    if fc_arr.size == 0:
        return math.nan
    return float(np.mean(np.abs(fc_arr - obs_arr)))


def relative_volume_error(fc: np.ndarray, obs: np.ndarray) -> float:
    """Relative volume error ``(sum(fc) - sum(obs)) / sum(obs)`` as a fraction.

    NB: numerically identical to ``kge_beta - 1`` over the same sample — this is
    documented, not a bug.

    Args:
        fc: Forecast values.
        obs: Observed values, aligned element-wise with ``fc``.

    Returns:
        Relative volume error, or ``math.nan`` when there are no pairs or when
        ``sum(obs) == 0``.
    """
    fc_arr = np.asarray(fc, dtype=float)
    obs_arr = np.asarray(obs, dtype=float)
    if fc_arr.size == 0:
        return math.nan
    sum_obs = float(np.sum(obs_arr))
    if sum_obs == 0.0:
        return math.nan
    return (float(np.sum(fc_arr)) - sum_obs) / sum_obs


def kge_2009(
    fc: np.ndarray,
    obs: np.ndarray,
) -> tuple[float, float, float, float]:
    """Kling-Gupta Efficiency (2009 formulation) with ``std(ddof=0)``.

    Returns ``(kge, r, alpha, beta)`` where ``r`` is the Pearson correlation,
    ``alpha = sigma_fc / sigma_obs`` (the KGE-2009 variability ratio, NOT the
    Kling-2012 CV ratio) and ``beta = mu_fc / mu_obs``.

    The ratio ``alpha`` is ``ddof``-invariant (numerator and denominator share
    the same ``n``); ``ddof=0`` is pinned as a parity convention with
    hydroeval/spotpy, not as load-bearing behaviour.

    Args:
        fc: Forecast values.
        obs: Observed values, aligned element-wise with ``fc``.

    Returns:
        Tuple ``(kge, r, alpha, beta)``.  An all-``NaN`` tuple is returned when
        ``n < 2``, ``sigma_obs == 0``, ``sigma_fc == 0`` (constant forecast —
        Pearson ``r`` is genuinely undefined), or ``mu_obs == 0``.
    """
    fc_arr = np.asarray(fc, dtype=float)
    obs_arr = np.asarray(obs, dtype=float)
    nan_tuple = (math.nan, math.nan, math.nan, math.nan)
    if fc_arr.size < 2:
        return nan_tuple

    mu_fc = float(fc_arr.mean())
    mu_obs = float(obs_arr.mean())
    sigma_fc = float(fc_arr.std(ddof=0))
    sigma_obs = float(obs_arr.std(ddof=0))

    if sigma_obs == 0.0 or sigma_fc == 0.0 or mu_obs == 0.0:
        return nan_tuple

    # Suppress the constant-series divide-by-zero RuntimeWarning; the guards
    # above already cover the degenerate cases, so this is belt-and-braces.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        r = float(np.corrcoef(fc_arr, obs_arr)[0, 1])

    if not math.isfinite(r):
        return nan_tuple

    alpha = sigma_fc / sigma_obs
    beta = mu_fc / mu_obs
    kge = 1.0 - math.sqrt((r - 1.0) ** 2 + (alpha - 1.0) ** 2 + (beta - 1.0) ** 2)
    return (kge, r, alpha, beta)


def nse(fc: np.ndarray, obs: np.ndarray) -> float:
    """Nash-Sutcliffe efficiency ``1 - SS_res / SS_tot``.

    Args:
        fc: Forecast values.
        obs: Observed values, aligned element-wise with ``fc``.

    Returns:
        NSE, or ``math.nan`` when there are no pairs or when the observed
        variance ``sum((obs - mean_obs)^2) == 0``.
    """
    fc_arr = np.asarray(fc, dtype=float)
    obs_arr = np.asarray(obs, dtype=float)
    if fc_arr.size == 0:
        return math.nan
    denom = float(np.sum((obs_arr - obs_arr.mean()) ** 2))
    if denom == 0.0:
        return math.nan
    num = float(np.sum((fc_arr - obs_arr) ** 2))
    return 1.0 - num / denom


def days_in_period(horizon: str, period_key: int, year: int) -> int | None:
    """Length in days of a horizon sub-period, from ``calendar.monthrange``.

    Month lengths are NEVER hardcoded — Feb leap/non-leap and 30/31-day months
    are all derived.  Sub-period semantics mirror ``pairs._target_month``:

        pentad: 6 sub-periods per month (72/year).  Sub-periods 1-5 are 5 days
            each; sub-period 6 spans days 26..end -> ``monthrange - 25``.
        decade: 3 sub-periods per month (36/year).  Sub-periods 1-2 are 10 days
            each; sub-period 3 spans days 21..end -> ``monthrange - 20``.
        month:  ``monthrange`` of the month.
        else:   ``None`` (day/quarter/season are gated out upstream).

    Args:
        horizon: Normalized horizon literal.
        period_key: The in-year period index for the horizon.
        year: The target calendar year (for leap-year resolution).

    Returns:
        Number of days, or ``None`` for unsupported horizons or out-of-range
        period keys.
    """
    if horizon == "month":
        if not 1 <= period_key <= 12:
            return None
        return calendar.monthrange(year, period_key)[1]

    if horizon == "pentad":
        month = (period_key - 1) // 6 + 1
        sub = (period_key - 1) % 6 + 1
        if not 1 <= month <= 12 or period_key < 1:
            return None
        if sub <= 5:
            return 5
        return calendar.monthrange(year, month)[1] - 25

    if horizon == "decade":
        month = (period_key - 1) // 3 + 1
        sub = (period_key - 1) % 3 + 1
        if not 1 <= month <= 12 or period_key < 1:
            return None
        if sub <= 2:
            return 10
        return calendar.monthrange(year, month)[1] - 20

    return None


# ---------------------------------------------------------------------------
# Continuous-metrics reducer
# ---------------------------------------------------------------------------


def _finite_pair_arrays(frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """Return aligned finite (fc, obs) arrays from a group frame."""
    fc = pd.to_numeric(frame.get("forecast_value"), errors="coerce")
    obs = pd.to_numeric(frame.get("observed_value"), errors="coerce")
    mask = np.isfinite(fc.to_numpy(dtype=float)) & np.isfinite(obs.to_numpy(dtype=float))
    return fc.to_numpy(dtype=float)[mask], obs.to_numpy(dtype=float)[mask]


def _aggregate_continuous(
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
    """Reduce one group frame to a single continuous-metrics record.

    ``bias``/``mae``/``rve`` are emitted for ``n_pairs >= 1``; the
    variance-sensitive ``kge*``/``nse`` are suppressed to ``NaN`` below
    ``MIN_PAIRS_FOR_VARIANCE_METRICS`` (the primitive alone would let ``n == 2``
    through with ``r == ±1``, which is meaningless).
    """
    fc, obs = _finite_pair_arrays(frame)
    n = int(fc.size)

    bias_v = bias(fc, obs)
    mae_v = mae(fc, obs)
    rve_v = relative_volume_error(fc, obs)

    if n >= MIN_PAIRS_FOR_VARIANCE_METRICS:
        kge_v, kge_r_v, kge_alpha_v, kge_beta_v = kge_2009(fc, obs)
        nse_v = nse(fc, obs)
    else:
        kge_v = kge_r_v = kge_alpha_v = kge_beta_v = nse_v = math.nan

    return {
        "horizon": horizon,
        "model": model,
        "regime": regime,
        "season": season,
        "code": code,
        "basin": basin,
        "norm_provenance": norm_provenance,
        "lead": lead,
        "n_pairs": n,
        "bias": bias_v,
        "mae": mae_v,
        "rve": rve_v,
        "kge": kge_v,
        "kge_r": kge_r_v,
        "kge_alpha": kge_alpha_v,
        "kge_beta": kge_beta_v,
        "nse": nse_v,
    }


def _continuous_scopes(
    frame: pd.DataFrame,
    basin: str,
    provenance: str,
    regime: str,
    season: str,
) -> list[pd.DataFrame]:
    """Fan a strata slice out over horizon x {per-code, POOLED} x model (x lead).

    Mirrors ``prob_metrics._metric_scopes`` group-key logic exactly so the
    emitted keys stay byte-identical to the probabilistic layer.
    """
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

                row = _aggregate_continuous(
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
                frames.append(pd.DataFrame([row]))

    return frames


def compute_continuous_metrics(pairs: pd.DataFrame) -> pd.DataFrame:
    """Compute per-group continuous/volume-accuracy metrics.

    Aggregates over the same 8-key POOLED + per-code slice structure as
    ``count_contingencies`` / ``compute_probabilistic_metrics`` (per-lead only
    for long-term horizons).  Pure group reductions over ``forecast_value`` /
    ``observed_value`` — no per-pair scoring prelude.

    Args:
        pairs: Pairs DataFrame (must include ``forecast_value``,
            ``observed_value`` and the group-key columns).

    Returns:
        DataFrame with ``CONTINUOUS_METRIC_COLUMNS``.  Empty input yields an
        empty frame with those columns.
    """
    if pairs.empty:
        return pd.DataFrame(columns=CONTINUOUS_METRIC_COLUMNS)

    working = _ensure_group_columns(pairs)
    frames: list[pd.DataFrame] = []

    for basin, basin_frame in _basin_slices(working):
        for provenance, prov_frame in _provenance_slices(basin_frame):
            for regime, regime_frame in _regime_slices(prov_frame):
                for season, season_frame in _season_slices(regime_frame):
                    frames.extend(
                        _continuous_scopes(season_frame, basin, provenance, regime, season)
                    )

    if not frames:
        return pd.DataFrame(columns=CONTINUOUS_METRIC_COLUMNS)

    result = pd.concat(frames, ignore_index=True)
    for col in CONTINUOUS_METRIC_COLUMNS:
        if col not in result.columns:
            result[col] = math.nan
    return result.loc[:, list(CONTINUOUS_METRIC_COLUMNS)].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Seasonal Apr-Sep volume reducer
# ---------------------------------------------------------------------------

_SEASONAL_GROUP_COLS: Final[list[str]] = [
    "horizon",
    "model",
    "regime",
    "code",
    "basin",
    "norm_provenance",
    "lead",
    "year",
]


def _dedupe_target_periods(
    frame: pd.DataFrame,
    *,
    ledger: ExclusionLedger | None,
    code: object,
    year: object,
) -> pd.DataFrame:
    """Collapse re-issued forecasts to one pair per target ``period_key``.

    The day-weighted sum silently double-counts if a group carries two pairs
    with the same target ``period_key`` (re-issued forecasts, or multiple issue
    dates collapsing to one long-term target).  Keep the latest issue date when
    ``issue_date`` is present; log a ledger entry when duplicates are found.
    """
    if not frame["period_key"].duplicated().any():
        return frame

    if ledger is not None:
        year_arg: int | None
        if year is None or (isinstance(year, float) and math.isnan(year)):
            year_arg = None
        else:
            year_arg = int(year)
        ledger.add(
            stage="value",
            reason="duplicate_target_period",
            code=None if code is None else str(code),
            year=year_arg,
        )

    ordered = frame
    if "issue_date" in frame.columns:
        ordered = frame.sort_values("issue_date", ascending=False, kind="stable")
    return ordered.drop_duplicates(subset="period_key", keep="first")


def _seasonal_volume_row(
    frame: pd.DataFrame,
    key_dict: dict,
    *,
    ledger: ExclusionLedger | None,
) -> dict | None:
    """Reduce one (group, year) frame to a seasonal-volume record."""
    horizon = str(key_dict.get("horizon", ""))
    year_raw = key_dict.get("year")
    try:
        year = int(year_raw)
    except (TypeError, ValueError):
        return None

    deduped = _dedupe_target_periods(
        frame,
        ledger=ledger,
        code=key_dict.get("code"),
        year=year_raw,
    )

    vol_fc = 0.0
    vol_obs = 0.0
    flows_fc: list[float] = []
    flows_obs: list[float] = []
    n_periods = 0

    for record in deduped.to_dict("records"):
        period_key_raw = record.get("period_key")
        try:
            period_key = int(period_key_raw)
        except (TypeError, ValueError):
            continue
        days = days_in_period(horizon, period_key, year)
        if days is None:
            continue
        try:
            fc_val = float(record.get("forecast_value"))
            obs_val = float(record.get("observed_value"))
        except (TypeError, ValueError):
            continue
        if not (math.isfinite(fc_val) and math.isfinite(obs_val)):
            continue
        weight = days * _SECONDS_PER_DAY
        vol_fc += fc_val * weight
        vol_obs += obs_val * weight
        flows_fc.append(fc_val)
        flows_obs.append(obs_val)
        n_periods += 1

    expected = _EXPECTED_PERIODS.get(horizon)
    seasonal_error = (vol_fc - vol_obs) / vol_obs if vol_obs != 0.0 else math.nan
    mean_flow_fc = float(np.mean(flows_fc)) if flows_fc else math.nan
    mean_flow_obs = float(np.mean(flows_obs)) if flows_obs else math.nan

    return {
        "horizon": horizon,
        "model": str(key_dict.get("model", "")),
        "regime": str(key_dict.get("regime", "")),
        "code": key_dict.get("code"),
        "basin": key_dict.get("basin"),
        "norm_provenance": key_dict.get("norm_provenance"),
        "lead": key_dict.get("lead"),
        "year": year,
        "n_periods": n_periods,
        "expected_periods": expected,
        "season_complete": bool(expected is not None and n_periods == expected),
        "season_volume_m3_fc": vol_fc if n_periods > 0 else math.nan,
        "season_volume_m3_obs": vol_obs if n_periods > 0 else math.nan,
        "seasonal_volume_error": seasonal_error,
        "mean_flow_fc": mean_flow_fc,
        "mean_flow_obs": mean_flow_obs,
    }


def compute_seasonal_volume(
    pairs: pd.DataFrame,
    *,
    ledger: ExclusionLedger | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compute the day-weighted Apr-Sep volume error per (group, year) + rollup.

    Restricted to ``{pentad, decade, month}`` targets in the ``irrigation``
    season (D9).  Within each (group, year) the target ``period_key`` is deduped
    (D10) so re-issued forecasts do not double-count; duplicates are logged to
    ``ledger`` with ``stage="value", reason="duplicate_target_period"``.

    Args:
        pairs: Pairs DataFrame with ``horizon``, ``season``, ``period_key``,
            ``year``, ``forecast_value``, ``observed_value`` and group-key
            columns.
        ledger: Optional exclusion ledger for duplicate-period logging.

    Returns:
        Tuple ``(seasonal_volume, seasonal_volume_summary)`` with
        ``SEASONAL_VOLUME_COLUMNS`` and ``SEASONAL_VOLUME_SUMMARY_COLUMNS``.
        Empty input yields two empty frames with those columns.
    """
    empty_detail = pd.DataFrame(columns=SEASONAL_VOLUME_COLUMNS)
    empty_summary = pd.DataFrame(columns=SEASONAL_VOLUME_SUMMARY_COLUMNS)
    if pairs.empty:
        return empty_detail, empty_summary

    working = _ensure_group_columns(pairs)
    mask = working["horizon"].astype(str).isin(_SEASONAL_VOLUME_HORIZONS)
    mask &= working["season"].astype(str) == _IRRIGATION_SEASON
    sub = working[mask]
    if sub.empty:
        return empty_detail, empty_summary

    rows: list[dict] = []
    for keys, g_frame in sub.groupby(_SEASONAL_GROUP_COLS, dropna=False, sort=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        key_dict = dict(zip(_SEASONAL_GROUP_COLS, keys, strict=False))
        row = _seasonal_volume_row(g_frame, key_dict, ledger=ledger)
        if row is not None:
            rows.append(row)

    if not rows:
        return empty_detail, empty_summary

    detail = pd.DataFrame(rows).loc[:, list(SEASONAL_VOLUME_COLUMNS)].reset_index(drop=True)
    summary = _seasonal_volume_summary(detail)
    return detail, summary


def _seasonal_volume_summary(detail: pd.DataFrame) -> pd.DataFrame:
    """Roll the per-year seasonal-volume detail up across years."""
    if detail.empty:
        return pd.DataFrame(columns=SEASONAL_VOLUME_SUMMARY_COLUMNS)

    group_cols = [
        "horizon",
        "model",
        "regime",
        "code",
        "basin",
        "norm_provenance",
        "lead",
    ]
    rows: list[dict] = []
    for keys, g_frame in detail.groupby(group_cols, dropna=False, sort=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        key_dict = dict(zip(group_cols, keys, strict=False))
        errors = pd.to_numeric(g_frame["seasonal_volume_error"], errors="coerce").dropna()
        rows.append(
            {
                "horizon": key_dict.get("horizon"),
                "model": key_dict.get("model"),
                "regime": key_dict.get("regime"),
                "code": key_dict.get("code"),
                "basin": key_dict.get("basin"),
                "norm_provenance": key_dict.get("norm_provenance"),
                "lead": key_dict.get("lead"),
                "n_years": int(len(g_frame)),
                "seasonal_volume_error_mean": (
                    float(errors.mean()) if len(errors) > 0 else math.nan
                ),
                "seasonal_volume_error_median": (
                    float(errors.median()) if len(errors) > 0 else math.nan
                ),
            }
        )

    return pd.DataFrame(rows).loc[:, list(SEASONAL_VOLUME_SUMMARY_COLUMNS)].reset_index(drop=True)
