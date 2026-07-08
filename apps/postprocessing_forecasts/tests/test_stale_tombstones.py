"""Unit tests for src.stale_tombstones.build_stale_tombstones.

All station codes use the placeholder value "19999" (never real codes).
"""

from __future__ import annotations

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

from src.stale_tombstones import build_stale_tombstones

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_skill_row(
    code: str = "19999",
    month_in_year: int = 3,
    horizon_value: int = 0,
    model_short: str = "LR_BASE",
    n_pairs: int = 6,
    nse: float = 0.5,
) -> dict:
    return {
        "code": code,
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


def _df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 1. Stale key → tombstone with n_pairs=0 and NULL metrics
# ---------------------------------------------------------------------------


class TestStaleTombstones:
    def test_stale_key_becomes_tombstone(self):
        """A key in existing but not in emitted produces a tombstone."""
        existing = _df([_make_skill_row(month_in_year=3, model_short="LR_BASE")])
        emitted = _df([_make_skill_row(month_in_year=4, model_short="LR_BASE")])  # different period

        result = build_stale_tombstones(existing, emitted, "month_in_year")

        assert len(result) == 1
        row = result.iloc[0]
        assert row["n_pairs"] == 0
        assert pd.isna(row["nse"])
        assert pd.isna(row["sdivsigma"])
        assert pd.isna(row["delta"])
        assert pd.isna(row["accuracy"])
        assert pd.isna(row["mae"])
        # Key columns preserved
        assert row["code"] == "19999"
        assert row["month_in_year"] == 3
        assert row["model_short"] == "LR_BASE"

    def test_emitted_key_not_tombstoned(self):
        """A key present in both existing and emitted must NOT appear in tombstones."""
        row = _make_skill_row(month_in_year=3, model_short="LR_BASE")
        existing = _df([row])
        emitted = _df([row])

        result = build_stale_tombstones(existing, emitted, "month_in_year")

        assert result.empty

    def test_mixed_keys(self):
        """Only the key not in emitted becomes a tombstone; shared key is untouched."""
        existing = _df(
            [
                _make_skill_row(month_in_year=3, model_short="LR_BASE"),
                _make_skill_row(month_in_year=4, model_short="LR_BASE"),
            ]
        )
        emitted = _df([_make_skill_row(month_in_year=4, model_short="LR_BASE")])

        result = build_stale_tombstones(existing, emitted, "month_in_year")

        assert len(result) == 1
        assert result.iloc[0]["month_in_year"] == 3
        assert result.iloc[0]["n_pairs"] == 0

    # ---------------------------------------------------------------------------
    # 3. Canonical match: ENSEMBLE_MEAN vs EM → same key → NOT tombstoned
    # ---------------------------------------------------------------------------

    def test_canonical_match_no_tombstone(self):
        """existing has model_short='ENSEMBLE_MEAN', emitted has 'EM' → same canonical → not tombstoned."""
        existing = _df([_make_skill_row(month_in_year=3, model_short="ENSEMBLE_MEAN")])
        emitted = _df([_make_skill_row(month_in_year=3, model_short="EM")])

        result = build_stale_tombstones(existing, emitted, "month_in_year")

        assert result.empty, (
            "ENSEMBLE_MEAN and EM canonicalize to the same key and must not produce a tombstone"
        )

    def test_canonical_match_reverse(self):
        """existing has 'EM', emitted has 'ENSEMBLE_MEAN' → same canonical → not tombstoned."""
        existing = _df([_make_skill_row(month_in_year=3, model_short="EM")])
        emitted = _df([_make_skill_row(month_in_year=3, model_short="ENSEMBLE_MEAN")])

        result = build_stale_tombstones(existing, emitted, "month_in_year")

        assert result.empty

    # ---------------------------------------------------------------------------
    # 4. Empty existing → empty result
    # ---------------------------------------------------------------------------

    def test_empty_existing_returns_empty(self):
        """Empty existing DataFrame → empty tombstone output."""
        existing = pd.DataFrame(
            columns=["code", "month_in_year", "horizon_value", "model_short", "n_pairs"]
        )
        emitted = _df([_make_skill_row()])

        result = build_stale_tombstones(existing, emitted, "month_in_year")

        assert result.empty

    # ---------------------------------------------------------------------------
    # 5. None existing → empty result
    # ---------------------------------------------------------------------------

    def test_none_existing_returns_empty(self):
        """None existing → empty tombstone output (graceful handling)."""
        emitted = _df([_make_skill_row()])

        result = build_stale_tombstones(None, emitted, "month_in_year")

        assert result.empty

    # ---------------------------------------------------------------------------
    # 6. Empty emitted → all existing keys become tombstones
    # ---------------------------------------------------------------------------

    def test_empty_emitted_all_tombstoned(self):
        """Empty emitted → every existing key becomes a tombstone."""
        existing = _df(
            [
                _make_skill_row(month_in_year=1, model_short="LR_BASE"),
                _make_skill_row(month_in_year=2, model_short="LR_SM"),
            ]
        )
        emitted = pd.DataFrame(columns=list(existing.columns))

        result = build_stale_tombstones(existing, emitted, "month_in_year")

        assert len(result) == 2
        assert (result["n_pairs"] == 0).all()
        assert result["nse"].isna().all()

    # ---------------------------------------------------------------------------
    # 7. Idempotency: already-tombstoned key stays a tombstone
    # ---------------------------------------------------------------------------

    def test_idempotency_already_tombstone(self):
        """If existing already has a tombstone (n_pairs=0) for a key not in emitted,
        the output is also n_pairs=0 — idempotent."""
        existing_tombstone = {
            "code": "19999",
            "month_in_year": 3,
            "horizon_value": 0,
            "model_short": "LR_BASE",
            "n_pairs": 0,
            "sdivsigma": None,
            "nse": None,
            "delta": None,
            "accuracy": None,
            "mae": None,
        }
        existing = _df([existing_tombstone])
        emitted = _df([_make_skill_row(month_in_year=4, model_short="LR_BASE")])

        result = build_stale_tombstones(existing, emitted, "month_in_year")

        assert len(result) == 1
        assert result.iloc[0]["n_pairs"] == 0
        assert pd.isna(result.iloc[0]["nse"])

    # ---------------------------------------------------------------------------
    # 8. Composition column: tombstone rows carry composition=None
    # ---------------------------------------------------------------------------

    def test_composition_column_in_tombstone(self):
        """When emitted has a 'composition' column, tombstone rows carry composition=None."""
        existing_row = _make_skill_row(month_in_year=3, model_short="ENSEMBLE_MEAN")
        existing = _df([existing_row])

        # emitted has composition column but a different key
        emitted_row = _make_skill_row(month_in_year=4, model_short="LR_BASE")
        emitted_row["composition"] = "LR_BASE,LR_SM"
        emitted = _df([emitted_row])

        result = build_stale_tombstones(existing, emitted, "month_in_year")

        assert len(result) == 1
        assert "composition" in result.columns
        assert result.iloc[0]["composition"] is None

    def test_no_composition_column_when_emitted_lacks_it(self):
        """When emitted has no 'composition' column, tombstones also lack it."""
        existing = _df([_make_skill_row(month_in_year=3)])
        emitted = _df([_make_skill_row(month_in_year=4)])

        result = build_stale_tombstones(existing, emitted, "month_in_year")

        assert "composition" not in result.columns

    # ---------------------------------------------------------------------------
    # 9. Wiring integration: concat produces combined frame with tombstone
    # ---------------------------------------------------------------------------

    def test_concat_produces_combined_frame(self):
        """Simulates the recalc wiring: tombstones concatenated with emitted
        yield a combined frame containing the tombstone row with n_pairs=0."""
        # existing has an extra stale key
        existing = _df(
            [
                _make_skill_row(month_in_year=3, model_short="LR_BASE"),  # stale
                _make_skill_row(month_in_year=4, model_short="LR_BASE"),  # still emitted
            ]
        )
        monthly_skill = _df([_make_skill_row(month_in_year=4, model_short="LR_BASE")])

        tombstones = build_stale_tombstones(existing, monthly_skill, "month_in_year")
        combined = pd.concat([monthly_skill, tombstones], ignore_index=True)

        # Combined frame has 2 rows: 1 real + 1 tombstone
        assert len(combined) == 2

        # Tombstone row is the one with month_in_year=3
        tombstone_rows = combined[combined["month_in_year"] == 3]
        assert len(tombstone_rows) == 1
        assert tombstone_rows.iloc[0]["n_pairs"] == 0
        assert pd.isna(tombstone_rows.iloc[0]["nse"])

        # Real row is untouched
        real_rows = combined[combined["month_in_year"] == 4]
        assert len(real_rows) == 1
        assert real_rows.iloc[0]["n_pairs"] == 6

    # ---------------------------------------------------------------------------
    # Quarter/season: period_col variants work correctly
    # ---------------------------------------------------------------------------

    def test_quarter_period_col(self):
        """build_stale_tombstones works with quarter_in_year period_col."""
        existing = _df(
            [
                {
                    "code": "19999",
                    "quarter_in_year": 2,
                    "horizon_value": 0,
                    "model_short": "LR_BASE",
                    "n_pairs": 5,
                    "sdivsigma": 0.6,
                    "nse": 0.4,
                    "delta": 0.1,
                    "accuracy": 0.75,
                    "mae": 8.0,
                }
            ]
        )
        emitted = _df(
            [
                {
                    "code": "19999",
                    "quarter_in_year": 3,  # different quarter → existing key is stale
                    "horizon_value": 0,
                    "model_short": "LR_BASE",
                    "n_pairs": 5,
                    "sdivsigma": 0.6,
                    "nse": 0.4,
                    "delta": 0.1,
                    "accuracy": 0.75,
                    "mae": 8.0,
                }
            ]
        )

        result = build_stale_tombstones(existing, emitted, "quarter_in_year")

        assert len(result) == 1
        assert result.iloc[0]["quarter_in_year"] == 2
        assert result.iloc[0]["n_pairs"] == 0

    def test_season_period_col(self):
        """build_stale_tombstones works with season_in_year period_col."""
        existing = _df(
            [
                {
                    "code": "19999",
                    "season_in_year": 1,
                    "horizon_value": 0,
                    "model_short": "LR_SM",
                    "n_pairs": 7,
                    "sdivsigma": 0.5,
                    "nse": 0.3,
                    "delta": 0.15,
                    "accuracy": 0.7,
                    "mae": 12.0,
                }
            ]
        )
        emitted = pd.DataFrame(
            columns=[
                "code",
                "season_in_year",
                "horizon_value",
                "model_short",
                "n_pairs",
                "sdivsigma",
                "nse",
                "delta",
                "accuracy",
                "mae",
            ]
        )

        result = build_stale_tombstones(existing, emitted, "season_in_year")

        assert len(result) == 1
        assert result.iloc[0]["season_in_year"] == 1
        assert result.iloc[0]["n_pairs"] == 0

    # ---------------------------------------------------------------------------
    # horizon_value sentinel: ensure 0 is used correctly
    # ---------------------------------------------------------------------------

    def test_horizon_value_in_key(self):
        """horizon_value is part of the key: same period/model but different horizon_value
        are separate keys."""
        existing = _df(
            [
                _make_skill_row(month_in_year=3, horizon_value=0, model_short="LR_BASE"),
                _make_skill_row(month_in_year=3, horizon_value=1, model_short="LR_BASE"),
            ]
        )
        # emitted only has horizon_value=1
        emitted = _df([_make_skill_row(month_in_year=3, horizon_value=1, model_short="LR_BASE")])

        result = build_stale_tombstones(existing, emitted, "month_in_year")

        assert len(result) == 1
        assert result.iloc[0]["horizon_value"] == 0  # the stale one
        assert result.iloc[0]["n_pairs"] == 0

    # ---------------------------------------------------------------------------
    # Sentinel horizon_value = 0 injection for quarter/season aggregated frames
    # ---------------------------------------------------------------------------

    def test_quarter_missing_horizon_value_stale_key_tombstoned(self):
        """Quarter emitted frame that LACKS horizon_value column: a stale existing
        key (quarter_in_year=2) NOT in emitted is tombstoned with horizon_value=0.
        The key present in emitted (quarter_in_year=3) is NOT tombstoned.

        This mirrors the real recalc path where the aggregated quarter/season
        skill frame groups by [period_col, code, model_short] only, and the DB
        read side returns horizon_value=0 (sentinel written by api_writer).
        """
        # existing — as returned by read_skill_metrics('quarter'); horizon_value=0
        existing = _df(
            [
                {
                    "code": "19999",
                    "quarter_in_year": 2,
                    "horizon_value": 0,
                    "model_short": "LR_BASE",
                    "n_pairs": 5,
                    "sdivsigma": 0.6,
                    "nse": 0.4,
                    "delta": 0.1,
                    "accuracy": 0.75,
                    "mae": 8.0,
                },
                {
                    "code": "19999",
                    "quarter_in_year": 3,
                    "horizon_value": 0,
                    "model_short": "LR_BASE",
                    "n_pairs": 6,
                    "sdivsigma": 0.7,
                    "nse": 0.5,
                    "delta": 0.2,
                    "accuracy": 0.8,
                    "mae": 7.0,
                },
            ]
        )

        # emitted — aggregated frame; NO horizon_value column
        emitted = _df(
            [
                {
                    "code": "19999",
                    "quarter_in_year": 3,  # only quarter 3 present; quarter 2 is stale
                    "model_short": "LR_BASE",
                    "n_pairs": 6,
                    "sdivsigma": 0.7,
                    "nse": 0.5,
                    "delta": 0.2,
                    "accuracy": 0.8,
                    "mae": 7.0,
                }
            ]
        )
        assert "horizon_value" not in emitted.columns  # pre-condition: column absent

        result = build_stale_tombstones(existing, emitted, "quarter_in_year")

        # Stale key (quarter 2) must appear as a tombstone with horizon_value=0
        assert len(result) == 1
        row = result.iloc[0]
        assert row["quarter_in_year"] == 2
        assert row["horizon_value"] == 0
        assert row["n_pairs"] == 0
        assert pd.isna(row["nse"])
        assert pd.isna(row["sdivsigma"])
        assert pd.isna(row["mae"])
        # Non-stale key (quarter 3) must NOT appear
        assert not (result["quarter_in_year"] == 3).any()

    def test_season_missing_horizon_value_stale_key_tombstoned(self):
        """Season emitted frame that LACKS horizon_value column: stale key is
        tombstoned with horizon_value=0; present key is not tombstoned.
        Uses period_col='season_in_year'."""
        existing = _df(
            [
                {
                    "code": "19999",
                    "season_in_year": 1,
                    "horizon_value": 0,
                    "model_short": "LR_SM",
                    "n_pairs": 4,
                    "sdivsigma": 0.5,
                    "nse": 0.3,
                    "delta": 0.15,
                    "accuracy": 0.7,
                    "mae": 12.0,
                },
                {
                    "code": "19999",
                    "season_in_year": 2,
                    "horizon_value": 0,
                    "model_short": "LR_SM",
                    "n_pairs": 5,
                    "sdivsigma": 0.6,
                    "nse": 0.4,
                    "delta": 0.2,
                    "accuracy": 0.75,
                    "mae": 10.0,
                },
            ]
        )

        # emitted lacks horizon_value; only season 2 present → season 1 is stale
        emitted = _df(
            [
                {
                    "code": "19999",
                    "season_in_year": 2,
                    "model_short": "LR_SM",
                    "n_pairs": 5,
                    "sdivsigma": 0.6,
                    "nse": 0.4,
                    "delta": 0.2,
                    "accuracy": 0.75,
                    "mae": 10.0,
                }
            ]
        )
        assert "horizon_value" not in emitted.columns

        result = build_stale_tombstones(existing, emitted, "season_in_year")

        assert len(result) == 1
        row = result.iloc[0]
        assert row["season_in_year"] == 1
        assert row["horizon_value"] == 0
        assert row["n_pairs"] == 0
        assert pd.isna(row["nse"])
        assert not (result["season_in_year"] == 2).any()

    def test_month_path_unchanged_with_real_horizon_values(self):
        """MONTH path: emitted carries real horizon_value (leads 0, 1).
        - A key at hv=1 present in emitted must NOT be tombstoned.
        - A stale key at hv=2 absent from emitted MUST be tombstoned.
        Confirms the sentinel-injection logic does not affect month behaviour."""
        existing = _df(
            [
                _make_skill_row(month_in_year=3, horizon_value=1, model_short="LR_BASE"),
                _make_skill_row(month_in_year=3, horizon_value=2, model_short="LR_BASE"),
            ]
        )
        # emitted has horizon_value column with lead 1 only
        emitted = _df([_make_skill_row(month_in_year=3, horizon_value=1, model_short="LR_BASE")])
        assert "horizon_value" in emitted.columns  # pre-condition

        result = build_stale_tombstones(existing, emitted, "month_in_year")

        # Only horizon_value=2 is stale
        assert len(result) == 1
        assert result.iloc[0]["horizon_value"] == 2
        assert result.iloc[0]["n_pairs"] == 0
        # horizon_value=1 (in emitted) must NOT appear in tombstones
        assert not (result["horizon_value"] == 1).any()

    def test_still_bails_when_emitted_missing_code(self):
        """emitted missing 'code' (a truly required key column) → returns empty
        tombstone set and does NOT raise.  Only horizon_value gets sentinel-0
        treatment; other key columns always cause bail-out."""
        existing = _df([_make_skill_row(month_in_year=3, model_short="LR_BASE")])
        # emitted is missing 'code' (and has horizon_value to isolate the test)
        emitted = _df(
            [
                {
                    "month_in_year": 3,
                    "horizon_value": 0,
                    "model_short": "LR_BASE",
                    "n_pairs": 5,
                    "nse": 0.4,
                }
            ]
        )
        assert "code" not in emitted.columns

        result = build_stale_tombstones(existing, emitted, "month_in_year")

        assert result.empty

    def test_still_bails_when_emitted_missing_model_short(self):
        """emitted missing 'model_short' → bail-out (not sentinel-patched)."""
        existing = _df([_make_skill_row(month_in_year=3, model_short="LR_BASE")])
        emitted = _df(
            [
                {
                    "code": "19999",
                    "month_in_year": 3,
                    "horizon_value": 0,
                    "n_pairs": 5,
                    "nse": 0.4,
                }
            ]
        )
        assert "model_short" not in emitted.columns

        result = build_stale_tombstones(existing, emitted, "month_in_year")

        assert result.empty

    def test_still_bails_when_emitted_missing_period_col(self):
        """emitted missing the period column → bail-out."""
        existing = _df([_make_skill_row(month_in_year=3, model_short="LR_BASE")])
        emitted = _df(
            [
                {
                    "code": "19999",
                    "horizon_value": 0,
                    "model_short": "LR_BASE",
                    "n_pairs": 5,
                    "nse": 0.4,
                }
            ]
        )
        # month_in_year is absent

        result = build_stale_tombstones(existing, emitted, "month_in_year")

        assert result.empty
