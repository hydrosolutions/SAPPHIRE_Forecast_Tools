"""P3 end-to-end consolidation tests: floor → tombstone → suppression.

These tests compose the P1 (stale-key tombstones) and P2 (min-n floor)
phases together for the MONTHLY path and verify that:

1. A raw model with n_pairs = K-1 is floored out of emitted skill.
2. That dropped key (which persists in the DB as "existing") becomes a
   tombstone row produced by build_stale_tombstones.
3. _drop_tombstone_rows suppresses the tombstone — it can never be seen
   by a downstream consumer or pass a filter_for_highly_skilled_forecasts
   gate.

A deferred-state marker (Test 2) documents that forecast-side
``long_forecasts`` tombstones are NOT yet handled (P1b deferred) and
points to the relevant issue plan.

Placeholder station code: 19999 throughout (never a real code).
"""

from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.data_reader import _drop_tombstone_rows
from src.skill_metrics import (
    calculate_monthly_skill_metrics,
    filter_for_highly_skilled_forecasts,
)
from src.stale_tombstones import build_stale_tombstones

STATION = "19999"
K_MONTH = 4  # production default (MONTH)
QUANTILE_COLS = ["q05", "q10", "q25", "q50", "q75", "q90", "q95"]


# ---------------------------------------------------------------------------
# Shared fixture builders (mirrors helpers in test_lt_min_pairs_gate.py)
# ---------------------------------------------------------------------------


def _q_row(q50: float) -> list[float]:
    """Expand a single q50 into 7 monotone quantile values."""
    q = float(q50)
    return [q * 0.70, q * 0.75, q * 0.85, q, q * 1.15, q * 1.25, q * 1.30]


def _make_monthly_obs(rows: list[tuple]) -> pd.DataFrame:
    """Build a monthly observations DataFrame.

    Args:
        rows: list of (code, year, month, discharge_avg) tuples.
    """
    df = pd.DataFrame(rows, columns=["code", "year", "month", "discharge_avg"])
    df["month_in_year"] = df["month"]
    delta_df = (
        df.groupby(["code", "month_in_year"])
        .agg(std_discharge=("discharge_avg", "std"))
        .reset_index()
    )
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)
    return df.merge(
        delta_df[["code", "month_in_year", "delta"]],
        on=["code", "month_in_year"],
    )


def _make_monthly_fcst(rows: list[tuple]) -> pd.DataFrame:
    """Build a monthly forecasts DataFrame.

    Args:
        rows: list of (code, year, month, model_short, q50) tuples.
    """
    records = []
    for code, year, month, model, q50 in rows:
        records.append([code, year, month, model] + _q_row(q50))
    return pd.DataFrame(records, columns=["code", "year", "month", "model_short"] + QUANTILE_COLS)


def _make_existing_skill_row(
    *,
    model_short: str,
    month_in_year: int = 3,
    horizon_value: int = 0,
    n_pairs: int = 6,
    nse: float = 0.5,
) -> dict:
    """Build a skill row as data_reader.read_monthly_skill_metrics would return."""
    return {
        "code": STATION,
        "month_in_year": month_in_year,
        "horizon_value": horizon_value,
        "model_short": model_short,
        "n_pairs": n_pairs,
        "sdivsigma": 0.7,
        "nse": nse,
        "delta": 0.2,
        "accuracy": 0.8,
        "mae": 10.0,
    }


# ---------------------------------------------------------------------------
# Test 1 — End-to-end: floor → tombstone → suppression → unselectable
# ---------------------------------------------------------------------------


class TestEndToEndFloorTombstoneSuppression:
    """End-to-end composition of P1 + P2 for the MONTHLY path.

    Scenario: one model (LR_Base) has only K-1 years of data and therefore
    n_pairs = K-1 = 3.  A second model (LR_SM) has K years and n_pairs = K = 4.

    Step 1 (P2 floor): calculate_monthly_skill_metrics emits the LR_SM row
    but NOT the LR_Base row.

    Step 2 (P1 tombstone): an "existing" frame (simulating what the DB held
    before the recalc) contains the LR_Base row.  build_stale_tombstones
    produces a tombstone for it (n_pairs=0, NULL metrics).

    Step 3 (P1 read suppression): _drop_tombstone_rows removes the tombstone
    so no downstream consumer can see or select it.
    """

    TARGET_MONTH = 3

    def _build_scenario(self):
        """Return (obs, fcst) where LR_Base has K-1 years, LR_SM has K years."""
        # LR_Base: K-1=3 years → n_pairs = 3 < K=4, will be floored out
        # LR_SM:   K=4   years → n_pairs = 4 >= K=4, will be kept
        obs_rows = [
            (STATION, 2010 + i, self.TARGET_MONTH, 100.0 + i * 5)
            for i in range(K_MONTH)  # 4 obs years (covers both models' range)
        ]
        fcst_rows = (
            # LR_Base has only the first K-1 years
            [
                (STATION, 2010 + i, self.TARGET_MONTH, "LR_Base", 102.0 + i * 5)
                for i in range(K_MONTH - 1)
            ]
            # LR_SM has all K years
            + [
                (STATION, 2010 + i, self.TARGET_MONTH, "LR_SM", 98.0 + i * 5)
                for i in range(K_MONTH)
            ]
        )
        return _make_monthly_obs(obs_rows), _make_monthly_fcst(fcst_rows)

    # ------------------------------------------------------------------
    # Step 1: floor drops LR_Base, keeps LR_SM
    # ------------------------------------------------------------------

    def test_step1_floor_drops_low_n_model(self):
        """LR_Base (n_pairs=K-1=3) must NOT appear in emitted skill output."""
        obs, fcst = self._build_scenario()
        emitted, _, _ = calculate_monthly_skill_metrics(obs, fcst)

        lr_base_rows = emitted[emitted["model_short"] == "LR_Base"]
        assert lr_base_rows.empty, (
            f"LR_Base (n_pairs=K-1={K_MONTH - 1}) must be floored out of emitted skill; "
            f"found {len(lr_base_rows)} row(s)"
        )

    def test_step1_floor_keeps_sufficient_n_model(self):
        """LR_SM (n_pairs=K=4) must appear in emitted skill output."""
        obs, fcst = self._build_scenario()
        emitted, _, _ = calculate_monthly_skill_metrics(obs, fcst)

        lr_sm_rows = emitted[emitted["model_short"] == "LR_SM"]
        assert not lr_sm_rows.empty, f"LR_SM (n_pairs=K={K_MONTH}) must be present in emitted skill"
        assert (lr_sm_rows["n_pairs"] >= K_MONTH).all(), "All LR_SM rows must have n_pairs >= K"

    def test_step1_no_emitted_row_below_k(self):
        """After the floor, no row in the emitted frame has n_pairs < K."""
        obs, fcst = self._build_scenario()
        emitted, _, _ = calculate_monthly_skill_metrics(obs, fcst)

        bad_rows = emitted[emitted["n_pairs"].fillna(0) < K_MONTH]
        assert bad_rows.empty, (
            f"Emitted rows with n_pairs < {K_MONTH} found: "
            f"{bad_rows[['model_short', 'n_pairs']].to_dict('records')}"
        )

    # ------------------------------------------------------------------
    # Step 2: the floored key becomes a tombstone
    # ------------------------------------------------------------------

    def test_step2_floored_key_becomes_tombstone(self):
        """The LR_Base key (now absent from emitted) must produce a tombstone."""
        obs, fcst = self._build_scenario()
        emitted, _, _ = calculate_monthly_skill_metrics(obs, fcst)

        # Simulate what the DB held before this recalc: both models present.
        existing = pd.DataFrame(
            [
                _make_existing_skill_row(
                    model_short="LR_Base",
                    month_in_year=self.TARGET_MONTH,
                    n_pairs=5,  # it had valid data in a previous recalc
                ),
                _make_existing_skill_row(
                    model_short="LR_SM",
                    month_in_year=self.TARGET_MONTH,
                    n_pairs=4,
                ),
            ]
        )

        tombstones = build_stale_tombstones(existing, emitted, "month_in_year")

        # LR_Base is stale → exactly one tombstone for it
        lr_base_tombstones = tombstones[tombstones["model_short"] == "LR_Base"]
        assert len(lr_base_tombstones) == 1, (
            f"Expected exactly 1 tombstone for LR_Base; got {len(lr_base_tombstones)}"
        )
        row = lr_base_tombstones.iloc[0]
        assert row["n_pairs"] == 0, "Tombstone must have n_pairs == 0"
        assert pd.isna(row["nse"]), "Tombstone must have NULL nse"
        assert pd.isna(row["sdivsigma"]), "Tombstone must have NULL sdivsigma"
        assert pd.isna(row["mae"]), "Tombstone must have NULL mae"

    def test_step2_active_key_not_tombstoned(self):
        """LR_SM (present in emitted) must NOT be tombstoned."""
        obs, fcst = self._build_scenario()
        emitted, _, _ = calculate_monthly_skill_metrics(obs, fcst)

        existing = pd.DataFrame(
            [
                _make_existing_skill_row(
                    model_short="LR_Base",
                    month_in_year=self.TARGET_MONTH,
                    n_pairs=5,
                ),
                _make_existing_skill_row(
                    model_short="LR_SM",
                    month_in_year=self.TARGET_MONTH,
                    n_pairs=4,
                ),
            ]
        )

        tombstones = build_stale_tombstones(existing, emitted, "month_in_year")

        lr_sm_tombstones = tombstones[tombstones["model_short"] == "LR_SM"]
        assert lr_sm_tombstones.empty, (
            "LR_SM (still active in emitted) must NOT produce a tombstone"
        )

    # ------------------------------------------------------------------
    # Step 3: read suppression — tombstone cannot be seen or selected
    # ------------------------------------------------------------------

    def test_step3_drop_tombstone_rows_removes_tombstone(self):
        """_drop_tombstone_rows must remove tombstone rows from a combined frame."""
        obs, fcst = self._build_scenario()
        emitted, _, _ = calculate_monthly_skill_metrics(obs, fcst)

        existing = pd.DataFrame(
            [
                _make_existing_skill_row(
                    model_short="LR_Base",
                    month_in_year=self.TARGET_MONTH,
                    n_pairs=5,
                ),
                _make_existing_skill_row(
                    model_short="LR_SM",
                    month_in_year=self.TARGET_MONTH,
                    n_pairs=4,
                ),
            ]
        )

        tombstones = build_stale_tombstones(existing, emitted, "month_in_year")

        # Simulate the combined frame a reader would receive from the DB after
        # the upsert: emitted rows + tombstone rows.
        combined = pd.concat([emitted, tombstones], ignore_index=True)

        # The tombstone must be present in combined before suppression.
        combined_tombstones = combined[combined["n_pairs"].fillna(1) == 0]
        assert not combined_tombstones.empty, (
            "Combined frame must contain the tombstone before suppression (pre-condition)"
        )

        # After suppression, tombstone must be gone.
        suppressed = _drop_tombstone_rows(combined)
        remaining_tombstones = suppressed[suppressed["n_pairs"].fillna(1) == 0]
        assert remaining_tombstones.empty, (
            "_drop_tombstone_rows must remove all tombstone rows (n_pairs=0)"
        )

    def test_step3_tombstone_not_selectable_by_skill_gate(self):
        """A tombstone row must not pass filter_for_highly_skilled_forecasts.

        Even without explicit suppression via _drop_tombstone_rows, a tombstone
        has n_pairs=0 which falls below any positive K floor.  With min_pairs=K
        the tombstone is excluded by the gate.
        """
        # Build a minimal frame containing one live row and one tombstone row.
        live_row = {
            "code": STATION,
            "month_in_year": self.TARGET_MONTH,
            "horizon_value": 0,
            "model_short": "LR_SM",
            "n_pairs": K_MONTH,
            "nse": 0.9,
            "sdivsigma": 0.4,
            "accuracy": 0.9,
            "mae": 5.0,
        }
        tombstone_row = {
            "code": STATION,
            "month_in_year": self.TARGET_MONTH,
            "horizon_value": 0,
            "model_short": "LR_Base",
            "n_pairs": 0,  # tombstone sentinel
            "nse": np.nan,
            "sdivsigma": np.nan,
            "accuracy": np.nan,
            "mae": np.nan,
        }
        frame = pd.DataFrame([live_row, tombstone_row])

        # Apply the gate with the production K floor.
        # Disable metric thresholds so only n_pairs matters for the tombstone.
        result = filter_for_highly_skilled_forecasts(
            frame,
            min_pairs=K_MONTH,
            nse=0.0,
            sdivsigma=False,
            accuracy=False,
        )

        assert "LR_Base" not in result["model_short"].values, (
            "Tombstone row (n_pairs=0) must not pass the skill gate with min_pairs=K"
        )
        assert "LR_SM" in result["model_short"].values, (
            "Live row (n_pairs=K) must pass the skill gate"
        )

    # ------------------------------------------------------------------
    # Full pipeline composition: all three steps chained
    # ------------------------------------------------------------------

    def test_full_pipeline_composition(self):
        """Chain floor → tombstone → suppression in a single test.

        Asserts the end-state: after suppression the combined frame contains
        only rows with n_pairs >= K; LR_Base (floored out) is absent.
        """
        obs, fcst = self._build_scenario()
        emitted, _, _ = calculate_monthly_skill_metrics(obs, fcst)

        # Existing DB frame (both models previously stored).
        existing = pd.DataFrame(
            [
                _make_existing_skill_row(
                    model_short="LR_Base",
                    month_in_year=self.TARGET_MONTH,
                    n_pairs=5,
                ),
                _make_existing_skill_row(
                    model_short="LR_SM",
                    month_in_year=self.TARGET_MONTH,
                    n_pairs=4,
                ),
            ]
        )

        # Step 2: tombstone generation.
        tombstones = build_stale_tombstones(existing, emitted, "month_in_year")

        # Step 3: simulate DB → concat → suppression.
        combined = pd.concat([emitted, tombstones], ignore_index=True)
        after_suppression = _drop_tombstone_rows(combined)

        # No n_pairs == 0 rows survive.
        assert (after_suppression["n_pairs"].fillna(0) > 0).all(), (
            "All rows after suppression must have n_pairs > 0"
        )

        # LR_Base is completely gone (floored, tombstoned, then suppressed).
        assert "LR_Base" not in after_suppression["model_short"].values, (
            "LR_Base must be absent from the suppressed frame "
            "(floored at K-1, then tombstoned and suppressed)"
        )

        # LR_SM is present and healthy.
        lr_sm = after_suppression[after_suppression["model_short"] == "LR_SM"]
        assert not lr_sm.empty, "LR_SM must survive all three steps"
        assert (lr_sm["n_pairs"] >= K_MONTH).all(), "LR_SM must have n_pairs >= K after suppression"


# ---------------------------------------------------------------------------
# Test 2 — P1b deferred-state marker
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    reason=(
        "P1b deferred: long_forecasts forecast-side tombstones not yet implemented"
        " — see doc/plans/working/skill_min_n_and_stale_aggregate_plan.md §P1b"
        " and mid_prio_gi_draft_pp_longforecasts_stale_invalidation.md"
    ),
    strict=False,
)
def test_p1b_deferred_long_forecasts_forecast_side_tombstones():
    """P1b DEFERRED: stale aggregate rows in long_forecasts are NOT invalidated.

    The write-side (src/api_writer.py _write_monthly_ensemble_to_api) does NOT
    emit tombstone rows for EM/SM/NM forecasts that were previously written but
    would no longer be emitted under the current skill/floor regime.

    This means stale ensemble FORECAST rows (not skill-metric rows) can persist
    in the long_forecasts table even after a recalculation that would exclude
    the underlying models.

    This test asserts the DESIRED future behavior: after a recalc where LR_Base
    is floored out of the monthly EM, the old EM forecast rows written when
    LR_Base was a member should be invalidated (overwritten with n_pairs=0 or
    deleted). Currently they are not.

    When P1b is implemented, update this test to assert the correct behavior
    and remove the @xfail marker.

    References:
        - doc/plans/working/skill_min_n_and_stale_aggregate_plan.md §P1b
        - doc/plans/issues/mid_prio_gi_draft_pp_longforecasts_stale_invalidation.md
          (to be created when P1b is scoped)
    """
    # The assertion below documents DESIRED (post-P1b) behavior.
    # It intentionally fails to flag this as "not yet done."
    #
    # Desired: _write_monthly_ensemble_to_api should emit a tombstone record
    # (or trigger a deletion) for stale EM rows when the EM composition
    # changes due to the min-n floor.  That mechanism does not exist yet.
    from src.api_writer import _write_monthly_ensemble_to_api  # noqa: PLC0415

    # Confirm the writer exists and is importable (module-level smoke check).
    assert callable(_write_monthly_ensemble_to_api), (
        "_write_monthly_ensemble_to_api must be importable (P1b precondition)"
    )

    # The DESIRED post-P1b invariant (will pass only after implementation):
    # calling _write_monthly_ensemble_to_api with a frame that lacks a formerly-
    # present EM model composition should trigger stale-row invalidation in
    # long_forecasts.  For now, raise AssertionError to mark this as failing
    # so the xfail decorator is exercised.
    raise AssertionError(
        "P1b not yet implemented: stale long_forecasts forecast rows are NOT "
        "invalidated when EM composition changes. Implement P1b and update this "
        "test. See skill_min_n_and_stale_aggregate_plan.md §P1b."
    )
