from __future__ import annotations

from collections.abc import Iterator
from typing import Final

import pandas as pd

from forecast_skill_eval.periods import LONG_TERM_HORIZONS
from forecast_skill_eval.regimes import ALL_REGIME

CONTINGENCY_LABELS: Final = ("TP", "FP", "FN", "TN")
COUNT_COLUMNS: Final = (*CONTINGENCY_LABELS, "n_pairs")
OUTPUT_COLUMNS: Final = (
    "horizon",
    "model",
    "regime",
    "season",
    "code",
    "basin",
    "norm_provenance",
    "lead",
    *COUNT_COLUMNS,
)
POOLED_CODE: Final = "POOLED"
ALL_PROVENANCE: Final = "all"
ALL_BASIN: Final = "all"
ALL_SEASON: Final = "all"

_REQUIRED_COLUMNS: Final = (
    "horizon",
    "code",
    "model",
    "regime",
    "lead",
    "basin",
    "norm_provenance",
    "contingency",
)


def count_contingencies(pairs: pd.DataFrame) -> pd.DataFrame:
    """Aggregate classified pair rows into station and pooled contingency tables.

    Args:
        pairs: P4 pair DataFrame with one valid forecast/observed pair per row.

    Returns:
        Tidy rows containing station and pooled counts. Each scope is emitted once
        for each basin/provenance value and once with ``basin="all"`` and
        ``norm_provenance="all"``. Long-term horizons also include per-lead rows.

    Raises:
        ValueError: If required columns are missing or contingency labels are invalid.
    """
    _require_columns(pairs)
    if pairs.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    working = pairs.copy()
    working["basin"] = working["basin"].map(_basin_label)
    working["norm_provenance"] = working["norm_provenance"].map(_provenance_label)
    working["regime"] = working["regime"].map(_regime_label)
    # ``season`` is optional — pairs built before Phase-2A lack this column.
    # Treat all such rows as season="all" so no season stratification is emitted.
    if "season" not in working.columns:
        working["season"] = ALL_SEASON
    _validate_contingencies(working)

    frames: list[pd.DataFrame] = []
    for basin, basin_frame in _basin_slices(working):
        for provenance, provenance_frame in _provenance_slices(basin_frame):
            for regime, regime_frame in _regime_slices(provenance_frame):
                for season, season_frame in _season_slices(regime_frame):
                    frames.extend(_count_scopes(season_frame, basin, provenance, regime, season))

    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    result = pd.concat(frames, ignore_index=True)
    result = result.loc[:, OUTPUT_COLUMNS]
    return result.sort_values(
        ["horizon", "model", "regime", "season", "code", "basin", "norm_provenance", "lead"],
        kind="stable",
        na_position="first",
    ).reset_index(drop=True)


def _count_scopes(
    frame: pd.DataFrame,
    basin: str,
    provenance: str,
    regime: str,
    season: str,
) -> list[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    for horizon, horizon_frame in frame.groupby("horizon", dropna=False, sort=True):
        for pooled in (False, True):
            group_columns = ["horizon", "model"]
            if not pooled:
                group_columns.append("code")

            if str(horizon) in LONG_TERM_HORIZONS:
                # Long-term: emit per-lead rows only.  NaN lead is its own group
                # (dropna=False inside _count_frame preserves it).
                frames.append(
                    _count_frame(
                        horizon_frame,
                        [*group_columns, "lead"],
                        basin,
                        provenance,
                        regime,
                        season,
                        pooled,
                    )
                )
            else:
                # Short-term: single lead-agnostic row (lead column is always NaN).
                frames.append(
                    _count_frame(
                        horizon_frame, group_columns, basin, provenance, regime, season, pooled
                    )
                )
    return frames


def _count_frame(
    frame: pd.DataFrame,
    group_columns: list[str],
    basin: str,
    provenance: str,
    regime: str,
    season: str,
    pooled: bool,
) -> pd.DataFrame:
    grouped = frame.groupby([*group_columns, "contingency"], dropna=False).size()
    wide = grouped.unstack("contingency", fill_value=0).reset_index()

    for label in CONTINGENCY_LABELS:
        if label not in wide:
            wide[label] = 0
        wide[label] = wide[label].astype("int64")

    if pooled:
        wide["code"] = POOLED_CODE
    if "lead" not in group_columns:
        wide["lead"] = None

    wide["basin"] = basin
    wide["norm_provenance"] = provenance
    wide["regime"] = regime
    wide["season"] = season
    wide["n_pairs"] = wide.loc[:, list(CONTINGENCY_LABELS)].sum(axis=1).astype("int64")
    return wide


def _basin_slices(frame: pd.DataFrame) -> Iterator[tuple[str, pd.DataFrame]]:
    yield ALL_BASIN, frame
    basins = sorted(str(value) for value in frame["basin"].dropna().unique())
    for basin in basins:
        yield basin, frame[frame["basin"] == basin]


def _provenance_slices(frame: pd.DataFrame) -> Iterator[tuple[str, pd.DataFrame]]:
    yield ALL_PROVENANCE, frame
    provenances = sorted(str(value) for value in frame["norm_provenance"].dropna().unique())
    for provenance in provenances:
        yield provenance, frame[frame["norm_provenance"] == provenance]


def _regime_slices(frame: pd.DataFrame) -> Iterator[tuple[str, pd.DataFrame]]:
    yield ALL_REGIME, frame
    regimes = sorted(
        str(value) for value in frame["regime"].dropna().unique() if str(value) != ALL_REGIME
    )
    for regime in regimes:
        yield regime, frame[frame["regime"] == regime]


def _season_slices(frame: pd.DataFrame) -> Iterator[tuple[str, pd.DataFrame]]:
    yield ALL_SEASON, frame
    seasons = sorted(
        str(value)
        for value in frame["season"].dropna().unique()
        if str(value) != ALL_SEASON
    )
    for season in seasons:
        yield season, frame[frame["season"] == season]


def _provenance_label(value: object) -> str:
    if value is None or pd.isna(value):
        return "unknown"
    text = str(value)
    return text if text else "unknown"


def _regime_label(value: object) -> str:
    if value is None or pd.isna(value):
        return "unknown"
    text = str(value)
    return text if text else "unknown"


def _basin_label(value: object) -> str:
    if value is None or pd.isna(value):
        return "other"
    text = str(value)
    return text if text else "other"


def _require_columns(frame: pd.DataFrame) -> None:
    missing = [column for column in _REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"Missing required pair columns: {missing}")


def _validate_contingencies(frame: pd.DataFrame) -> None:
    labels = {str(value) for value in frame["contingency"].dropna().unique()}
    unsupported = sorted(labels.difference(CONTINGENCY_LABELS))
    if unsupported:
        raise ValueError(f"Unsupported contingency labels: {unsupported}")
