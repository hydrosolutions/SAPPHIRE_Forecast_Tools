"""Stale-aggregate tombstone builder for long-term skill invalidation.

After a skill recalculation, keys that the recalc no longer emits (because the
min-n gate dropped them, or an aggregate was discarded) must be overwritten with
a tombstone row — n_pairs=0, all metric columns NULL — so that stale rows in the
database are overwritten rather than surviving indefinitely.

This module is intentionally pure (no I/O) for unit-testability.
"""

from __future__ import annotations

import logging

import pandas as pd
from src.model_names import canonical_model_short_series

logger = logging.getLogger(__name__)

# Metric columns that tombstone rows must carry (set to None/NaN).
_METRIC_COLS = ("sdivsigma", "nse", "delta", "accuracy", "mae")


def build_stale_tombstones(
    existing: pd.DataFrame | None,
    emitted: pd.DataFrame,
    period_col: str,
) -> pd.DataFrame:
    """Return tombstone rows for skill keys present in *existing* but not in *emitted*.

    A tombstone row has n_pairs=0 and all metric columns set to NaN/None.
    It is passed to the same save_*_skill_metrics() writer as the real
    emitted rows, so the upsert overwrites any stale DB row.

    Args:
        existing: DataFrame of currently stored skill rows, as returned by
            data_reader.read_skill_metrics().  Key columns expected:
            [period_col, "code", "horizon_value", "model_short"].
            May be None or empty — both are treated as "no existing data".
        emitted: DataFrame of newly computed skill rows (post-min-n gate).
            Same key columns required.  Metric columns present in this
            frame are used to determine which extra columns the tombstone
            rows should carry (all set to NaN).
        period_col: Name of the period column, one of
            "month_in_year", "quarter_in_year", "season_in_year".

    Returns:
        DataFrame of tombstone rows.  Columns match those of *emitted*
        (same set, same dtypes for key columns, NaN/None for metric cols
        and composition).  Empty DataFrame if nothing is stale.
    """
    key_cols = ["code", period_col, "horizon_value", "model_short"]

    # Edge case: existing is None or empty → nothing is stale.
    if existing is None or existing.empty:
        return pd.DataFrame(columns=emitted.columns)

    # Validate that key columns exist in existing; bail out gracefully if not.
    missing_in_existing = [c for c in key_cols if c not in existing.columns]
    if missing_in_existing:
        logger.warning(
            "build_stale_tombstones: existing is missing key columns %s — "
            "returning empty tombstone set",
            missing_in_existing,
        )
        return pd.DataFrame(columns=emitted.columns)

    # Normalize model_short on existing to canonical form for the diff.
    existing_canon = existing[key_cols].copy()
    existing_canon["_model_canon"] = canonical_model_short_series(existing_canon["model_short"])

    if emitted.empty:
        # All existing keys become tombstones.
        stale_existing = existing_canon
    else:
        # For quarter/season the aggregated emitted frame lacks horizon_value
        # (those frames group only by [period_col, code, model_short]).  The
        # writer stores sentinel horizon_value = 0 for all non-month horizons,
        # so the read side returns horizon_value = 0 in *existing*.  Inject the
        # same sentinel into emitted so both sides of the anti-join share the
        # same key value, instead of bailing.
        if "horizon_value" not in emitted.columns:
            emitted = emitted.copy()
            emitted["horizon_value"] = 0
            logger.debug(
                "build_stale_tombstones: emitted lacks horizon_value — "
                "injecting sentinel 0 for period_col=%s",
                period_col,
            )

        # Validate remaining key columns in emitted; bail out only if a
        # non-horizon_value key column is absent.
        missing_in_emitted = [c for c in key_cols if c not in emitted.columns]
        if missing_in_emitted:
            logger.warning(
                "build_stale_tombstones: emitted is missing key columns %s — "
                "returning empty tombstone set",
                missing_in_emitted,
            )
            return pd.DataFrame(columns=emitted.columns)

        # Normalize model_short on emitted to canonical form.
        emitted_canon = emitted[key_cols].copy()
        emitted_canon["_model_canon"] = canonical_model_short_series(emitted_canon["model_short"])

        # Anti-join: find existing rows whose canonical key is absent from emitted.
        join_cols = ["code", period_col, "horizon_value", "_model_canon"]
        emitted_keys = emitted_canon[join_cols].drop_duplicates()
        emitted_keys = emitted_keys.copy()
        emitted_keys["_in_emitted"] = True

        merged = existing_canon.merge(emitted_keys, on=join_cols, how="left")
        stale_mask = merged["_in_emitted"].isna()
        stale_existing = existing_canon[stale_mask.values]

    if stale_existing.empty:
        return pd.DataFrame(columns=emitted.columns)

    # Build tombstone rows from the stale keys' original values.
    tombstones = stale_existing[key_cols].copy().reset_index(drop=True)
    tombstones["n_pairs"] = 0

    # Null out all metric columns that appear in emitted.
    null_cols = list(_METRIC_COLS) + (["composition"] if "composition" in emitted.columns else [])
    for col in null_cols:
        tombstones[col] = None

    # Fill any remaining columns emitted has that tombstones don't yet have.
    for col in emitted.columns:
        if col not in tombstones.columns:
            tombstones[col] = None

    # Reorder to match emitted column order exactly.
    tombstones = tombstones.reindex(columns=emitted.columns)

    # Ensure horizon_value is integer (sentinel 0 for non-month if NaN).
    if "horizon_value" in tombstones.columns:
        tombstones["horizon_value"] = tombstones["horizon_value"].fillna(0).astype(int)

    logger.info(
        "build_stale_tombstones: %d tombstone row(s) for period_col=%s",
        len(tombstones),
        period_col,
    )

    return tombstones
