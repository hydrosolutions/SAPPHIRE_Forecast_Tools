from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Final

import pandas as pd

COUNT_LABELS: Final = ("TP", "FP", "FN", "TN")
METRIC_COLUMNS: Final = (
    "base_rate",
    "base_rate_undefined",
    "pod",
    "pod_undefined",
    "far",
    "far_undefined",
    "pofd",
    "pofd_undefined",
    "csi",
    "csi_undefined",
    "frequency_bias",
    "frequency_bias_undefined",
    "hss",
    "hss_undefined",
    "pss",
    "pss_undefined",
    "pod_ci_lower",
    "pod_ci_upper",
    "pod_ci_undefined",
    "far_ci_lower",
    "far_ci_upper",
    "far_ci_undefined",
)
WILSON_Z_95: Final = 1.959963984540054


def metrics_from_counts(row: Mapping[str, object]) -> dict[str, float | bool]:
    """Compute low-flow contingency skill metrics from one TP/FP/FN/TN row.

    Args:
        row: Mapping containing TP, FP, FN, and TN counts.

    Returns:
        Metric values plus explicit ``<metric>_undefined`` flags. Undefined
        denominator cases are represented as ``NaN`` values and ``True`` flags.
    """
    tp = _count(row, "TP")
    fp = _count(row, "FP")
    fn = _count(row, "FN")
    tn = _count(row, "TN")
    n = tp + fp + fn + tn

    metrics: dict[str, float | bool] = {}
    _add_ratio(metrics, "base_rate", tp + fn, n)
    _add_ratio(metrics, "pod", tp, tp + fn)
    _add_ratio(metrics, "far", fp, tp + fp)
    _add_ratio(metrics, "pofd", fp, fp + tn)
    _add_ratio(metrics, "csi", tp, tp + fp + fn)
    _add_ratio(metrics, "frequency_bias", tp + fp, tp + fn)
    _add_hss(metrics, tp=tp, fp=fp, fn=fn, tn=tn)
    _add_pss(metrics, tp=tp, fp=fp, fn=fn, tn=tn)
    _add_wilson(metrics, "pod", successes=tp, total=tp + fn)
    _add_wilson(metrics, "far", successes=fp, total=tp + fp)
    return metrics


def add_metrics(counts: pd.DataFrame) -> pd.DataFrame:
    """Append contingency metrics to every count row.

    Args:
        counts: Tidy count table containing TP, FP, FN, and TN columns.

    Returns:
        A copy of ``counts`` with metric columns appended.
    """
    result = counts.reset_index(drop=True).copy()
    result = result.drop(columns=[column for column in METRIC_COLUMNS if column in result])

    if result.empty:
        for column in METRIC_COLUMNS:
            result[column] = pd.Series(dtype="object")
        return result

    metrics = pd.DataFrame(
        [metrics_from_counts(row) for row in result.to_dict("records")],
        columns=METRIC_COLUMNS,
    )
    return pd.concat([result, metrics], axis=1)


def _add_ratio(
    metrics: dict[str, float | bool],
    name: str,
    numerator: float,
    denominator: float,
) -> None:
    undefined = denominator == 0
    metrics[name] = math.nan if undefined else numerator / denominator
    metrics[f"{name}_undefined"] = undefined


def _add_hss(
    metrics: dict[str, float | bool],
    *,
    tp: float,
    fp: float,
    fn: float,
    tn: float,
) -> None:
    # HSS is undefined ONLY when its denominator vanishes.  A slice with zero
    # observed events (TP+FN == 0) but FP>0/TN>0 has a finite denominator and a
    # genuine skill of 0.0 (the standard Heidke value), so it must not be NaN.
    denominator = (tp + fn) * (fn + tn) + (tp + fp) * (fp + tn)
    undefined = denominator == 0
    numerator = 2 * ((tp * tn) - (fp * fn))
    metrics["hss"] = math.nan if undefined else numerator / denominator
    metrics["hss_undefined"] = undefined


def _add_pss(
    metrics: dict[str, float | bool],
    *,
    tp: float,
    fp: float,
    fn: float,
    tn: float,
) -> None:
    observed_positives = tp + fn
    observed_negatives = fp + tn
    undefined = observed_positives == 0 or observed_negatives == 0
    metrics["pss"] = (
        math.nan if undefined else (tp / observed_positives) - (fp / observed_negatives)
    )
    metrics["pss_undefined"] = undefined


def _add_wilson(
    metrics: dict[str, float | bool],
    name: str,
    *,
    successes: float,
    total: float,
) -> None:
    lower, upper, undefined = _wilson_interval(successes, total)
    metrics[f"{name}_ci_lower"] = lower
    metrics[f"{name}_ci_upper"] = upper
    metrics[f"{name}_ci_undefined"] = undefined


def _wilson_interval(successes: float, total: float) -> tuple[float, float, bool]:
    if total == 0:
        return math.nan, math.nan, True

    proportion = successes / total
    z_squared = WILSON_Z_95 * WILSON_Z_95
    denominator = 1 + z_squared / total
    center = (proportion + z_squared / (2 * total)) / denominator
    half_width = (
        WILSON_Z_95
        * math.sqrt((proportion * (1 - proportion) + z_squared / (4 * total)) / total)
        / denominator
    )
    return max(0.0, center - half_width), min(1.0, center + half_width), False


def _count(row: Mapping[str, object], label: str) -> float:
    value = row.get(label, 0)
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
