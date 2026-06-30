from __future__ import annotations

from typing import Final

import pandas as pd

from forecast_skill_eval.contingency import OUTPUT_COLUMNS, count_contingencies
from forecast_skill_eval.metrics import METRIC_COLUMNS, add_metrics
from forecast_skill_eval.periods import LONG_TERM_HORIZONS

CLIMATOLOGY_MODEL: Final = "climatology"
CLIMATOLOGY_BASELINE: Final = "climatology"
OPERATIONAL_BASELINE: Final = "operational_proxy"
SHORT_TERM_PROXY_MODEL: Final = "LR"
LONG_TERM_PROXY_MODEL: Final = "LR_Base"
BASELINE_EXTRA_COLUMNS: Final = (
    "baseline",
    "comparison_model",
    "is_proxy",
    "n_matched",
)
BASELINE_COLUMNS: Final = (*OUTPUT_COLUMNS, *METRIC_COLUMNS, *BASELINE_EXTRA_COLUMNS)

_MATCH_COLUMNS: Final = ("horizon", "code", "period_key", "year", "model", "regime", "lead")


def build_climatology_baseline(pairs: pd.DataFrame) -> pd.DataFrame:
    """Build always-normal climatology rows on each model's available samples.

    Args:
        pairs: P4 pair DataFrame.

    Returns:
        Tidy count and metric rows for an always-normal reference forecast.
    """
    _require_columns(pairs, ("contingency", "norm_provenance"))
    frames: list[pd.DataFrame] = []
    for model in _model_names(pairs):
        model_pairs = pairs[pairs["model"].astype(str) == model].copy()
        model_pairs["model"] = CLIMATOLOGY_MODEL
        model_pairs["contingency"] = model_pairs["contingency"].map(_always_normal_cell)
        frames.append(
            _baseline_table(
                model_pairs,
                baseline=CLIMATOLOGY_BASELINE,
                comparison_model=model,
                is_proxy=False,
            )
        )
    return _concat_baselines(frames)


def build_operational_proxy_baseline(
    pairs: pd.DataFrame,
    *,
    short_term_proxy: str = SHORT_TERM_PROXY_MODEL,
    long_term_proxy: str = LONG_TERM_PROXY_MODEL,
) -> pd.DataFrame:
    """Build matched-sample model and LR/LR_Base operational proxy rows.

    Args:
        pairs: P4 pair DataFrame.
        short_term_proxy: Proxy model name used for short-term horizons.
        long_term_proxy: Proxy model name used for long-term horizons.

    Returns:
        Tidy rows for each non-proxy model and its matched proxy row. Matching
        uses ``(code, period_key, year)`` plus ``lead`` for long-term horizons.
    """
    _require_columns(pairs, _MATCH_COLUMNS)
    frames: list[pd.DataFrame] = []
    for horizon, horizon_pairs in pairs.groupby("horizon", dropna=False, sort=True):
        proxy_model = _proxy_for_horizon(str(horizon), short_term_proxy, long_term_proxy)
        proxy_pairs = horizon_pairs[horizon_pairs["model"].astype(str) == proxy_model]
        if proxy_pairs.empty:
            continue

        key_columns = _key_columns(str(horizon))
        for model in _model_names(horizon_pairs):
            if model == proxy_model:
                continue

            model_pairs = horizon_pairs[horizon_pairs["model"].astype(str) == model]
            matched_keys = _matched_keys(model_pairs, proxy_pairs, key_columns)
            if matched_keys.empty:
                continue

            matched_model = _filter_to_keys(model_pairs, matched_keys, key_columns)
            matched_proxy = _filter_to_keys(proxy_pairs, matched_keys, key_columns)
            frames.append(
                _baseline_table(
                    matched_model,
                    baseline=OPERATIONAL_BASELINE,
                    comparison_model=model,
                    is_proxy=False,
                )
            )
            frames.append(
                _baseline_table(
                    matched_proxy,
                    baseline=OPERATIONAL_BASELINE,
                    comparison_model=model,
                    is_proxy=True,
                )
            )
    return _concat_baselines(frames)


def _baseline_table(
    pairs: pd.DataFrame,
    *,
    baseline: str,
    comparison_model: str,
    is_proxy: bool,
) -> pd.DataFrame:
    counts = count_contingencies(pairs)
    if counts.empty:
        return _empty_baseline_frame()

    table = add_metrics(counts)
    table["baseline"] = baseline
    table["comparison_model"] = comparison_model
    table["is_proxy"] = pd.Series([bool(is_proxy)] * len(table), dtype="object")
    table["n_matched"] = table["n_pairs"].astype("int64")
    return table.loc[:, BASELINE_COLUMNS]


def _always_normal_cell(contingency: object) -> str:
    if contingency in ("TP", "FN"):
        return "FN"
    if contingency in ("FP", "TN"):
        return "TN"
    raise ValueError(f"Unsupported contingency label: {contingency!r}")


def _proxy_for_horizon(
    horizon: str,
    short_term_proxy: str,
    long_term_proxy: str,
) -> str:
    if horizon in LONG_TERM_HORIZONS:
        return long_term_proxy
    return short_term_proxy


def _key_columns(horizon: str) -> list[str]:
    columns = ["code", "period_key", "year", "regime"]
    if horizon in LONG_TERM_HORIZONS:
        columns.append("lead")
    return columns


def _matched_keys(
    model_pairs: pd.DataFrame,
    proxy_pairs: pd.DataFrame,
    key_columns: list[str],
) -> pd.DataFrame:
    left = model_pairs.loc[:, key_columns].drop_duplicates()
    right = proxy_pairs.loc[:, key_columns].drop_duplicates()
    return left.merge(right, on=key_columns, how="inner")


def _filter_to_keys(
    pairs: pd.DataFrame,
    keys: pd.DataFrame,
    key_columns: list[str],
) -> pd.DataFrame:
    tagged_keys = keys.assign(_matched_key=True)
    matched = pairs.merge(tagged_keys, on=key_columns, how="inner")
    return matched.drop(columns=["_matched_key"])


def _model_names(frame: pd.DataFrame) -> list[str]:
    return sorted(str(value) for value in frame["model"].dropna().unique())


def _concat_baselines(frames: list[pd.DataFrame]) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return _empty_baseline_frame()
    return pd.concat(non_empty, ignore_index=True)


def _empty_baseline_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=BASELINE_COLUMNS)


def _require_columns(frame: pd.DataFrame, columns: tuple[str, ...]) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"Missing required pair columns: {missing}")
