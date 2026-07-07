from __future__ import annotations

import calendar
from typing import Final

import pandas as pd

from forecast_skill_eval.classifier import classify
from forecast_skill_eval.classifier import contingency as classify_contingency
from forecast_skill_eval.contingency import OUTPUT_COLUMNS, count_contingencies
from forecast_skill_eval.metrics import METRIC_COLUMNS, add_metrics
from forecast_skill_eval.periods import LONG_TERM_HORIZONS

CLIMATOLOGY_MODEL: Final = "climatology"
CLIMATOLOGY_BASELINE: Final = "climatology"
OPERATIONAL_BASELINE: Final = "operational_proxy"
PERSISTENCE_BASELINE: Final = "persistence"
SHORT_TERM_PROXY_MODEL: Final = "LR"
LONG_TERM_PROXY_MODEL: Final = "LR_Base"
BASELINE_EXTRA_COLUMNS: Final = (
    "baseline",
    "comparison_model",
    "is_proxy",
    "n_matched",
    "event",
)
BASELINE_COLUMNS: Final = (*OUTPUT_COLUMNS, *METRIC_COLUMNS, *BASELINE_EXTRA_COLUMNS)

_MATCH_COLUMNS: Final = ("horizon", "code", "period_key", "year", "model", "regime", "lead")


def build_climatology_baseline(
    pairs: pd.DataFrame,
    *,
    event: str = "below_norm",
) -> pd.DataFrame:
    """Build always-normal climatology rows on each model's available samples.

    Args:
        pairs: P4 pair DataFrame.
        event: Event label tagged onto the emitted rows.  Defaults to
            ``"below_norm"`` (the 0.80 × norm classification embedded in the
            pairs).  Pass ``"below_norm_100"`` when building from pairs
            reclassified at 1.0 × norm.

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
                event=event,
            )
        )
    return _concat_baselines(frames)


def build_operational_proxy_baseline(
    pairs: pd.DataFrame,
    *,
    short_term_proxy: str = SHORT_TERM_PROXY_MODEL,
    long_term_proxy: str = LONG_TERM_PROXY_MODEL,
    event: str = "below_norm",
) -> pd.DataFrame:
    """Build matched-sample model and LR/LR_Base operational proxy rows.

    Args:
        pairs: P4 pair DataFrame.
        short_term_proxy: Proxy model name used for short-term horizons.
        long_term_proxy: Proxy model name used for long-term horizons.
        event: Event label tagged onto the emitted rows.  Defaults to
            ``"below_norm"`` (the 0.80 × norm classification embedded in the
            pairs).  Pass ``"below_norm_100"`` when building from pairs
            reclassified at 1.0 × norm.

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
                    event=event,
                )
            )
            frames.append(
                _baseline_table(
                    matched_proxy,
                    baseline=OPERATIONAL_BASELINE,
                    comparison_model=model,
                    is_proxy=True,
                    event=event,
                )
            )
    return _concat_baselines(frames)


def build_persistence_baseline(
    pairs: pd.DataFrame,
    *,
    threshold: float = 0.80,
    event: str = "below_norm",
) -> pd.DataFrame:
    """Build lag-1 persistence baseline rows on each model's available samples.

    Definition: the forecast equals the last measured flow — the observed runoff
    of the most recent completed period immediately before the target period.
    The persistence forecast is classified against the same ``threshold × norm``
    boundary used for every other pair in the evaluation.

    Pairs where the lag-1 observed value is not present in the pairs DataFrame
    are silently excluded (typically the first period in the available record for
    each station and the first period of each new year when prior-year data are
    absent).  This exclusion is conservative and transparent: callers can inspect
    ``n_matched`` to see how many pairs contributed.

    Args:
        pairs: P4 pair DataFrame including ``observed_value``, ``norm``, and
            ``obs_class`` columns.
        threshold: Below-norm threshold fraction (default 0.80, matching the
            operational ``config.threshold``).
        event: Event label tagged onto the emitted rows.  Defaults to
            ``"below_norm"``.  Pass ``"below_norm_100"`` together with
            ``threshold=1.0`` when building the 1.0 × norm persistence set.

    Returns:
        Tidy count and metric rows tagged ``baseline="persistence"``.  Returns
        an empty DataFrame (same schema as ``BASELINE_COLUMNS``) if no lag-1
        values are available.
    """
    required = (
        "contingency",
        "norm_provenance",
        "observed_value",
        "norm",
        "obs_class",
        "horizon",
        "period_key",
        "year",
        "code",
    )
    _require_columns(pairs, required)

    if pairs.empty:
        return _empty_baseline_frame()

    obs_lookup = _build_obs_lookup(pairs)
    frames: list[pd.DataFrame] = []

    for model in _model_names(pairs):
        model_pairs = pairs[pairs["model"].astype(str) == model].copy()
        persistence_rows: list[dict[str, object]] = []

        for row in model_pairs.to_dict("records"):
            lag1_key = _lag1_key(
                str(row.get("horizon", "")),
                str(row.get("code", "")),
                row.get("period_key"),
                row.get("year"),
            )
            if lag1_key is None:
                continue
            lag1_value = obs_lookup.get(lag1_key)
            if lag1_value is None:
                continue

            # Classify the lag-1 observed value against the current period's norm.
            fc_class = classify(lag1_value, threshold, row.get("norm"))
            obs_class = row.get("obs_class")
            if fc_class is None or obs_class not in ("below", "normal"):
                continue

            new_row = dict(row)
            new_row["forecast_value"] = lag1_value
            new_row["fc_class"] = fc_class
            new_row["contingency"] = classify_contingency(fc_class, obs_class)
            persistence_rows.append(new_row)

        if not persistence_rows:
            continue

        model_persistence = pd.DataFrame(persistence_rows, columns=list(model_pairs.columns))
        model_persistence["model"] = PERSISTENCE_BASELINE

        frames.append(
            _baseline_table(
                model_persistence,
                baseline=PERSISTENCE_BASELINE,
                comparison_model=model,
                is_proxy=False,
                event=event,
            )
        )

    return _concat_baselines(frames)


def _build_obs_lookup(
    pairs: pd.DataFrame,
) -> dict[tuple[str, str, int, int], float]:
    """Return ``{(code, horizon, period_key, year): observed_value}`` from pairs.

    Where a (code, horizon, period_key, year) tuple appears in multiple model
    rows, the first non-null observed value is used (they are identical across
    models for the same station–period–year).
    """
    result: dict[tuple[str, str, int, int], float] = {}
    for row in pairs.to_dict("records"):
        code = str(row.get("code", ""))
        horizon = str(row.get("horizon", ""))
        period_key = row.get("period_key")
        year = row.get("year")
        obs_val = row.get("observed_value")
        if not code or not horizon or period_key is None or year is None or obs_val is None:
            continue
        try:
            key = (code, horizon, int(period_key), int(year))
        except (TypeError, ValueError):
            continue
        if key not in result:
            result[key] = float(obs_val)
    return result


def _lag1_key(
    horizon: str,
    code: str,
    period_key: object,
    year: object,
) -> tuple[str, str, int, int] | None:
    """Return the observed-truth key for the period immediately before the given one.

    Returns None when the input values cannot be interpreted as integers or when
    the horizon is not recognised.
    """
    if not code or not horizon or period_key is None or year is None:
        return None
    try:
        pk = int(period_key)
        yr = int(year)
    except (TypeError, ValueError):
        return None

    if horizon == "season":
        # Only one season per year → lag-1 is the prior year's season.
        return (code, horizon, 1, yr - 1)

    if horizon == "month":
        if pk == 1:
            return (code, horizon, 12, yr - 1)
        return (code, horizon, pk - 1, yr)

    if horizon == "quarter":
        if pk == 1:
            return (code, horizon, 4, yr - 1)
        return (code, horizon, pk - 1, yr)

    if horizon == "day":
        if pk == 1:
            # Last day of the prior year (366 for a leap year, 365 otherwise).
            prev_days = 366 if calendar.isleap(yr - 1) else 365
            return (code, horizon, prev_days, yr - 1)
        return (code, horizon, pk - 1, yr)

    if horizon == "pentad":
        if pk == 1:
            return (code, horizon, 72, yr - 1)
        return (code, horizon, pk - 1, yr)

    if horizon == "decade":
        if pk == 1:
            return (code, horizon, 36, yr - 1)
        return (code, horizon, pk - 1, yr)

    return None


def _baseline_table(
    pairs: pd.DataFrame,
    *,
    baseline: str,
    comparison_model: str,
    is_proxy: bool,
    event: str = "below_norm",
) -> pd.DataFrame:
    counts = count_contingencies(pairs)
    if counts.empty:
        return _empty_baseline_frame()

    table = add_metrics(counts)
    table["baseline"] = baseline
    table["comparison_model"] = comparison_model
    table["is_proxy"] = pd.Series([bool(is_proxy)] * len(table), dtype="object")
    table["n_matched"] = table["n_pairs"].astype("int64")
    table["event"] = event
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
    matched = matched.drop(columns=["_matched_key"])
    # The operational_proxy baseline is a PAIRED comparison: the model row and
    # the proxy row must be scored on the same sample. Matching on unique keys
    # (``_matched_keys``) equalises *which* keys appear on each side, but a side
    # can still carry several pairs per key (re-issued forecasts). Left as-is,
    # the two emitted rows would have unequal n_pairs. Collapse each side to one
    # representative pair per matched key so both sides contribute exactly one
    # pair per key group and their n_pairs are equal.
    return _collapse_to_one_per_key(matched, key_columns)


def _collapse_to_one_per_key(
    matched: pd.DataFrame,
    key_columns: list[str],
) -> pd.DataFrame:
    """Keep exactly one representative pair per ``key_columns`` group.

    The representative is the row with the latest parseable ``issue_date`` in
    the group. Missing or unparseable ``issue_date`` values sort first (as
    ``NaT``), so any real date wins; pure ties (or an absent ``issue_date``
    column) fall back to the frame's existing stable order. This keeps
    single-pair-per-key groups byte-identical to the pre-collapse frame.
    """
    if matched.empty:
        return matched

    working = matched.reset_index(drop=True)
    # Preserve the original row order so we can restore it after collapsing and
    # break issue_date ties deterministically.
    working["_stable_order"] = range(len(working))
    if "issue_date" in working.columns:
        working["_issue_order"] = pd.to_datetime(working["issue_date"], errors="coerce")
    else:
        working["_issue_order"] = pd.NaT

    # Sort so the latest issue_date sits last within each key group (NaT first),
    # then keep the last row per group.
    ranked = working.sort_values(
        by=["_issue_order", "_stable_order"],
        kind="stable",
        na_position="first",
    )
    kept = ranked.drop_duplicates(subset=key_columns, keep="last")
    # Restore the original row order so a no-multiplicity frame is unchanged.
    kept = kept.sort_values("_stable_order", kind="stable")
    return kept.drop(columns=["_issue_order", "_stable_order"]).reset_index(drop=True)


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
