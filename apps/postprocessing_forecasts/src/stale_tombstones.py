"""Stale-aggregate tombstone builder for long-term skill invalidation.

After a skill recalculation, keys that the recalc no longer emits (because the
min-n gate dropped them, or an aggregate was discarded) must be overwritten with
a tombstone row — n_pairs=0, all metric columns NULL — so that stale rows in the
database are overwritten rather than surviving indefinitely.

This module is intentionally pure (no I/O) for unit-testability.

SAPPHIRE_SKILL_LEAD_AWARE (flag-ON) behavior
---------------------------------------------
Flag-OFF, this module is byte-identical to its pre-M1 behavior, including the
legacy sentinel-``horizon_value=0`` injection for quarter/season ``emitted``
frames that lack a ``horizon_value`` column (those frames historically pooled
skill across leads; the DB read side and writer both stamp sentinel 0 for
those rows).

Flag-ON, quarter/season ``emitted`` frames normally carry a REAL per-lead
``horizon_value`` (see M1 P2, ``_calculate_aggregated_skill_metrics``). The
old sentinel-injection fallback becomes unsafe in that world: if it fired
unconditionally, real per-lead ``existing`` keys (e.g. hv=1, hv=2, hv=3 from
a prior run) would be diffed against an injected sentinel 0 that matches none
of them, misidentifying every legitimate per-lead row as stale. Under the
flag, that fallback is therefore disabled — if ``emitted`` is non-empty but
still lacks ``horizon_value``, this function logs a warning and returns an
empty tombstone set for that run (refuse to guess) rather than mass-
tombstoning good data.

Transitional tombstone design decision: no dedicated "transitional" code path
is added for legacy sentinel-0 rows that pre-date lead-aware skill. Once the
sentinel-injection fallback is disabled under the flag, the ORDINARY
anti-join (both sides keyed on real ``horizon_value``) already does the right
thing:
  - If a group's real per-lead leads are all non-zero, the legacy sentinel-0
    row simply matches no emitted key and is tombstoned with
    ``horizon_value=0`` by the normal anti-join — exactly the desired
    transitional tombstone.
  - If a real lead-0 row IS legitimately emitted for that group, it shares
    the same key as the legacy sentinel row, so the ordinary upsert write
    (outside this function) overwrites the old pooled value in place. That
    key isn't "stale", it's "updated" — no tombstone is needed or possible.
Both are covered by the pre-existing key-diff logic; only the incorrect
blanket sentinel-injection needed to be guarded (see below).

Under the flag, rows with a NULL/non-numeric ``horizon_value`` in either
``existing`` or ``emitted`` are coerced via ``pd.to_numeric(errors="coerce")``
and dropped before the diff — never silently included in a tombstone (which
would write a nonsensical ``horizon_value=NaN`` key) or silently matched via
NaN-equals-NaN join semantics. A WARNING naming the dropped count is logged,
mirroring ``skill_metrics.py::_exclude_null_lead_rows``'s "legacy NULL-lead"
phrasing so grep-based log auditing stays consistent.
"""

from __future__ import annotations

import logging

import pandas as pd
from skill_lead_aware_flag import skill_lead_aware_enabled
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

    Flag behavior (SAPPHIRE_SKILL_LEAD_AWARE): see the module docstring for
    the full flag-ON design (NULL-lead exclusion, disabled sentinel
    fallback, "transitional tombstone" rationale). Flag-OFF is unaffected.
    """
    lead_aware = skill_lead_aware_enabled()
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

    if lead_aware:
        # Under the flag, existing rows with a NULL/non-numeric horizon_value
        # (legacy/partially-migrated rows) must never be silently included in
        # a tombstone (horizon_value=NaN key) or silently NaN-matched in the
        # join below. Exclude them with a loud warning naming the count,
        # mirroring skill_metrics.py::_exclude_null_lead_rows.
        existing_canon["horizon_value"] = pd.to_numeric(
            existing_canon["horizon_value"], errors="coerce"
        )
        _valid_existing = existing_canon["horizon_value"].notna()
        _n_dropped_existing = int((~_valid_existing).sum())
        if _n_dropped_existing:
            logger.warning(
                "%d legacy NULL-lead %s existing rows skipped from "
                "stale-tombstone diff — need migration",
                _n_dropped_existing,
                period_col,
            )
        existing_canon = existing_canon[_valid_existing].copy()

    existing_canon["_model_canon"] = canonical_model_short_series(existing_canon["model_short"])

    if emitted.empty:
        # All existing keys become tombstones.
        stale_existing = existing_canon
    else:
        # For quarter/season the aggregated emitted frame historically lacked
        # horizon_value (those frames grouped only by
        # [period_col, code, model_short]).  The writer stores sentinel
        # horizon_value = 0 for all non-month horizons, so the read side
        # returns horizon_value = 0 in *existing*.  Flag-OFF, inject the same
        # sentinel into emitted so both sides of the anti-join share the same
        # key value, instead of bailing — this is the pre-existing, still-
        # needed legacy path and must remain byte-identical.
        #
        # Flag-ON, quarter/season emitted frames normally DO carry a real
        # per-lead horizon_value (M1 P2). If emitted is non-empty but still
        # lacks the column here, that is an anomaly, not the expected
        # aggregated-pooling case: injecting sentinel 0 would diff it against
        # existing rows that may hold real per-lead keys (hv=1, hv=2, ...),
        # misidentifying every one of them as stale and mass-tombstoning
        # good data. So under the flag we refuse to guess and skip tombstone
        # generation for this run instead. No dedicated "transitional
        # tombstone" branch is added elsewhere: once this fallback no longer
        # fires, the ordinary anti-join below (keyed on real horizon_value on
        # both sides) already retires legacy sentinel-0 rows by itself — see
        # the module docstring for the full reasoning.
        if "horizon_value" not in emitted.columns:
            if lead_aware:
                logger.warning(
                    "build_stale_tombstones: SAPPHIRE_SKILL_LEAD_AWARE is ON "
                    "but emitted lacks horizon_value for period_col=%s — "
                    "refusing to inject the legacy sentinel-0 fallback "
                    "(would misidentify real per-lead existing rows as "
                    "stale); skipping tombstone generation for this run",
                    period_col,
                )
                return pd.DataFrame(columns=emitted.columns)
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

        if lead_aware:
            # Defense in depth: skill_metrics.py should already exclude
            # legacy NULL-lead rows upstream, but don't assume — apply the
            # same coercion/exclusion here before the join.
            emitted_canon["horizon_value"] = pd.to_numeric(
                emitted_canon["horizon_value"], errors="coerce"
            )
            _valid_emitted = emitted_canon["horizon_value"].notna()
            _n_dropped_emitted = int((~_valid_emitted).sum())
            if _n_dropped_emitted:
                logger.warning(
                    "%d legacy NULL-lead %s emitted rows skipped from "
                    "stale-tombstone diff — need migration",
                    _n_dropped_emitted,
                    period_col,
                )
            emitted_canon = emitted_canon[_valid_emitted].copy()

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
    #
    # Flag-ON caveat: `emitted` can be EMPTY (or otherwise lack a
    # horizon_value column) — e.g. the empty_stats frame returned by
    # _calculate_aggregated_skill_metrics when the input forecasts frame is
    # empty/lacks horizon_value flows in here as `emitted`. Reindexing to bare
    # `emitted.columns` in that case would STRIP the real per-lead
    # horizon_value that `tombstones` already carries from key_cols (sourced
    # from `existing`). The writer would then default the missing column to
    # sentinel 0, collapsing distinct per-lead stale rows (hv=1/2/3) onto a
    # single hv=0 row — so the real per-lead DB rows would never be
    # invalidated. Under the flag, therefore, ensure horizon_value survives
    # the reindex by keeping it in the output column order (inserted right
    # after period_col when present, else appended). Flag-OFF stays
    # byte-identical: reindex to bare emitted.columns and let the writer supply
    # the legacy sentinel 0 for the pooled quarter/season path.
    if lead_aware:
        out_cols = list(emitted.columns)
        if "horizon_value" not in out_cols:
            if period_col in out_cols:
                out_cols.insert(out_cols.index(period_col) + 1, "horizon_value")
            else:
                out_cols.append("horizon_value")
    else:
        out_cols = list(emitted.columns)
    tombstones = tombstones.reindex(columns=out_cols)

    # Ensure horizon_value is integer (sentinel 0 for non-month if NaN).
    if "horizon_value" in tombstones.columns:
        tombstones["horizon_value"] = tombstones["horizon_value"].fillna(0).astype(int)

    logger.info(
        "build_stale_tombstones: %d tombstone row(s) for period_col=%s",
        len(tombstones),
        period_col,
    )

    return tombstones
