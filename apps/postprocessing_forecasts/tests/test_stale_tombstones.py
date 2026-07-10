"""Unit tests for src.stale_tombstones.build_stale_tombstones.

All station codes use the placeholder value "19999" (never real codes).
"""

from __future__ import annotations

import logging
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

from src.skill_metrics import calculate_quarterly_skill_metrics
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


# ---------------------------------------------------------------------------
# P2b: flag-aware tombstone reconciliation (SAPPHIRE_SKILL_LEAD_AWARE)
#
# Under the flag, quarter/season `emitted` frames normally carry a REAL
# per-lead `horizon_value` (P2). The legacy sentinel-0 injection fallback
# (see the flag-OFF tests above) becomes a landmine in that world: if it
# fired unconditionally, a genuinely per-lead `existing` frame (keys like
# hv=1, hv=2, hv=3 from a prior successful run) would be diffed against an
# injected sentinel 0 that matches none of them — silently tombstoning every
# legitimate per-lead row. These tests lock the flag-ON guard that prevents
# that, plus the NULL-lead exclusion and the "transitional tombstone" design
# decision for legacy sentinel-0 rows.
# ---------------------------------------------------------------------------


@pytest.fixture
def lead_aware_on(monkeypatch):
    monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "1")


def _quantile_row(q50, spread=20.0):
    return {
        "q05": q50 - spread,
        "q10": q50 - spread * 0.75,
        "q25": q50 - spread * 0.35,
        "q50": q50,
        "q75": q50 + spread * 0.35,
        "q90": q50 + spread * 0.75,
        "q95": q50 + spread,
    }


def _e2e_quarterly_obs(years, obs, code="19999", quarter_in_year=1):
    rows = [(code, y, quarter_in_year, o) for y, o in zip(years, obs, strict=True)]
    df = pd.DataFrame(rows, columns=["code", "year", "quarter_in_year", "discharge_avg"])
    delta_df = (
        df.groupby(["code", "quarter_in_year"])
        .agg(std_discharge=("discharge_avg", "std"))
        .reset_index()
    )
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)
    return df.merge(delta_df[["code", "quarter_in_year", "delta"]], on=["code", "quarter_in_year"])


def _e2e_quarterly_fcst(
    years, obs, horizon_value, model_short="LR_BASE", offset=-2.0, code="19999", quarter_in_year=1
):
    rows = []
    for y, o in zip(years, obs, strict=True):
        row = {
            "code": code,
            "year": y,
            "quarter_in_year": quarter_in_year,
            "horizon_value": horizon_value,
            "model_short": model_short,
        }
        row.update(_quantile_row(o + offset))
        rows.append(row)
    return pd.DataFrame(rows)


class TestFlagOnHv0Hv1Coexist:
    """Existing has real per-lead rows at hv=0 AND hv=1 for the same
    (code, period, model); emitted has neither this run (both leads
    dropped). Both must survive as DISTINCT tombstone rows, not collapse
    into a single sentinel-0 row."""

    def test_quarter(self, lead_aware_on):
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
                    "quarter_in_year": 2,
                    "horizon_value": 1,
                    "model_short": "LR_BASE",
                    "n_pairs": 5,
                    "sdivsigma": 0.65,
                    "nse": 0.45,
                    "delta": 0.12,
                    "accuracy": 0.78,
                    "mae": 7.5,
                },
            ]
        )
        # emitted has a real horizon_value column, but no rows for this
        # (code, quarter, model) at all — both leads dropped this run.
        emitted = pd.DataFrame(
            columns=[
                "code",
                "quarter_in_year",
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

        result = build_stale_tombstones(existing, emitted, "quarter_in_year")

        assert len(result) == 2
        assert set(result["horizon_value"]) == {0, 1}
        assert (result["n_pairs"] == 0).all()

    def test_season(self, lead_aware_on):
        existing = _df(
            [
                {
                    "code": "19999",
                    "season_in_year": 1,
                    "horizon_value": 0,
                    "model_short": "LR_SM",
                    "n_pairs": 5,
                    "sdivsigma": 0.5,
                    "nse": 0.3,
                    "delta": 0.15,
                    "accuracy": 0.7,
                    "mae": 12.0,
                },
                {
                    "code": "19999",
                    "season_in_year": 1,
                    "horizon_value": 1,
                    "model_short": "LR_SM",
                    "n_pairs": 5,
                    "sdivsigma": 0.55,
                    "nse": 0.35,
                    "delta": 0.16,
                    "accuracy": 0.72,
                    "mae": 11.0,
                },
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

        assert len(result) == 2
        assert set(result["horizon_value"]) == {0, 1}
        assert (result["n_pairs"] == 0).all()


class TestFlagOnPerLeadMinNDropTombstonesCorrectLead:
    """End-to-end: real calculate_quarterly_skill_metrics floors out the
    thin lead (#411 min-n gate), and build_stale_tombstones must tombstone
    that dropped lead's REAL horizon_value — not the sentinel 0."""

    def test_quarter(self, lead_aware_on, monkeypatch):
        monkeypatch.setenv("ieasyhydroforecast_min_pairs_long_term_quarter", "5")
        years_lead1 = [2020, 2021, 2022, 2023, 2024]  # 5 pairs -> kept
        years_lead2 = [2020, 2021, 2022]  # 3 pairs -> dropped by the floor
        obs_5 = [100.0, 110.0, 120.0, 130.0, 140.0]

        obs = _e2e_quarterly_obs(years_lead1, obs_5)
        fcst = pd.concat(
            [
                _e2e_quarterly_fcst(years_lead1, obs_5, horizon_value=1),
                _e2e_quarterly_fcst(years_lead2, obs_5[:3], horizon_value=2),
            ],
            ignore_index=True,
        )

        skill_stats, _, _ = calculate_quarterly_skill_metrics(obs, fcst)

        base_rows = skill_stats[skill_stats["model_short"] == "LR_BASE"]
        # Precondition: the floor dropped lead 2, kept lead 1.
        assert set(base_rows["horizon_value"]) == {1}

        # existing had BOTH leads from a prior successful run.
        existing = _df(
            [
                {
                    "code": "19999",
                    "quarter_in_year": 1,
                    "horizon_value": 1,
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
                    "quarter_in_year": 1,
                    "horizon_value": 2,
                    "model_short": "LR_BASE",
                    "n_pairs": 3,
                    "sdivsigma": 0.5,
                    "nse": 0.3,
                    "delta": 0.15,
                    "accuracy": 0.7,
                    "mae": 9.0,
                },
            ]
        )

        result = build_stale_tombstones(existing, skill_stats, "quarter_in_year")
        base_result = result[result["model_short"] == "LR_BASE"]

        assert set(base_result["horizon_value"]) == {2}
        assert not (base_result["horizon_value"] == 1).any()


class TestFlagOnLegacySentinelZeroRowInvalidated:
    """Locks the 'transitional tombstone' design decision (P2b point 4d):

    existing has ONLY a legacy pooled/sentinel row at horizon_value=0 for
    (code, period, model). emitted (flag ON) now carries real per-lead rows
    at horizon_value=1 and 2 for that same group (no hv=0 emitted this run).

    No special-cased "transitional" branch is needed: the ordinary anti-join
    already retires the legacy sentinel-0 row, because it simply never
    matches any real-lead key on the emitted side. This test proves that
    the ordinary anti-join alone is sufficient once the incorrect blanket
    sentinel-injection fallback (which would have collapsed everything onto
    key hv=0) is disabled under the flag.
    """

    def test_quarter(self, lead_aware_on):
        existing = _df(
            [
                {
                    "code": "19999",
                    "quarter_in_year": 1,
                    "horizon_value": 0,
                    "model_short": "LR_BASE",
                    "n_pairs": 8,
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
                    "quarter_in_year": 1,
                    "horizon_value": 1,
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
                    "quarter_in_year": 1,
                    "horizon_value": 2,
                    "model_short": "LR_BASE",
                    "n_pairs": 5,
                    "sdivsigma": 0.6,
                    "nse": 0.4,
                    "delta": 0.1,
                    "accuracy": 0.75,
                    "mae": 8.0,
                },
            ]
        )

        result = build_stale_tombstones(existing, emitted, "quarter_in_year")

        assert len(result) == 1
        assert result.iloc[0]["horizon_value"] == 0
        assert result.iloc[0]["n_pairs"] == 0
        assert not (result["horizon_value"] == 1).any()
        assert not (result["horizon_value"] == 2).any()


class TestFlagOnEmittedMissingHorizonValueSkipsWithoutMassTombstoning:
    """emitted lacks horizon_value entirely (anomaly), while existing has
    real per-lead rows. Under the flag we must refuse to guess and return
    empty rather than mass-tombstoning good data via a sentinel-0 mismatch."""

    def test_quarter(self, lead_aware_on, caplog):
        existing = _df(
            [
                {
                    "code": "19999",
                    "quarter_in_year": 1,
                    "horizon_value": 1,
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
                    "quarter_in_year": 1,
                    "horizon_value": 2,
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
                    "quarter_in_year": 1,
                    "horizon_value": 3,
                    "model_short": "LR_BASE",
                    "n_pairs": 5,
                    "sdivsigma": 0.6,
                    "nse": 0.4,
                    "delta": 0.1,
                    "accuracy": 0.75,
                    "mae": 8.0,
                },
            ]
        )
        # Contrived anomaly: non-empty emitted with no horizon_value column.
        emitted = pd.DataFrame(
            [
                {
                    "code": "19999",
                    "quarter_in_year": 1,
                    "model_short": "LR_BASE",
                    "n_pairs": 15,
                    "sdivsigma": 0.6,
                    "nse": 0.4,
                    "delta": 0.1,
                    "accuracy": 0.75,
                    "mae": 8.0,
                }
            ]
        )
        assert "horizon_value" not in emitted.columns

        with caplog.at_level(logging.WARNING):
            result = build_stale_tombstones(existing, emitted, "quarter_in_year")

        assert result.empty
        assert any(record.levelno == logging.WARNING for record in caplog.records), (
            "expected a WARNING log when flag-ON emitted lacks horizon_value"
        )


class TestFlagOnNullLeadExistingRowExcludedWithWarning:
    """existing has one row with horizon_value=NaN plus one normal
    real-lead row (hv=1), both stale relative to emitted. The NULL-lead
    row must never silently produce a tombstone keyed on NaN; it must be
    excluded with a WARNING naming the count (mirroring the 'legacy
    NULL-lead' phrasing used by skill_metrics.py::_exclude_null_lead_rows).
    The real hv=1 row's stale-ness must still be tombstoned correctly."""

    def test_quarter(self, lead_aware_on, caplog):
        existing = _df(
            [
                {
                    "code": "19999",
                    "quarter_in_year": 1,
                    "horizon_value": float("nan"),
                    "model_short": "LR_BASE",
                    "n_pairs": 6,
                    "sdivsigma": 0.6,
                    "nse": 0.4,
                    "delta": 0.1,
                    "accuracy": 0.75,
                    "mae": 8.0,
                },
                {
                    "code": "19999",
                    "quarter_in_year": 1,
                    "horizon_value": 1,
                    "model_short": "LR_BASE",
                    "n_pairs": 5,
                    "sdivsigma": 0.6,
                    "nse": 0.4,
                    "delta": 0.1,
                    "accuracy": 0.75,
                    "mae": 8.0,
                },
            ]
        )
        # emitted empty -> both existing rows are candidates; the NaN-lead
        # row must be filtered before it can become a tombstone.
        emitted = pd.DataFrame(
            columns=[
                "code",
                "quarter_in_year",
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

        with caplog.at_level(logging.WARNING):
            result = build_stale_tombstones(existing, emitted, "quarter_in_year")

        warning_messages = [record.getMessage() for record in caplog.records]
        assert any("legacy NULL-lead" in msg for msg in warning_messages), (
            f"expected a 'legacy NULL-lead' warning, got: {warning_messages}"
        )

        # The real hv=1 row must still be tombstoned...
        assert len(result) == 1
        assert result.iloc[0]["horizon_value"] == 1
        assert result.iloc[0]["n_pairs"] == 0
        # ...and no row with a NaN/None horizon_value was emitted.
        assert result["horizon_value"].notna().all()


class TestFlagOffSentinelInjectionUnchanged:
    """Explicit regression: with the flag OFF (unset), the legacy sentinel-0
    injection for quarter/season is completely unchanged. Adapted from
    test_quarter_missing_horizon_value_stale_key_tombstoned as a
    belt-and-suspenders check that the P2b flag-ON changes did not regress
    the flag-OFF path."""

    def test_quarter(self):
        assert os.environ.get("SAPPHIRE_SKILL_LEAD_AWARE") in (None, "", "0", "false", "off", "no")

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
        emitted = _df(
            [
                {
                    "code": "19999",
                    "quarter_in_year": 3,
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
        assert "horizon_value" not in emitted.columns

        result = build_stale_tombstones(existing, emitted, "quarter_in_year")

        assert len(result) == 1
        row = result.iloc[0]
        assert row["quarter_in_year"] == 2
        assert row["horizon_value"] == 0
        assert row["n_pairs"] == 0
        assert not (result["quarter_in_year"] == 3).any()


# ---------------------------------------------------------------------------
# P2b bug fix: EMPTY (or horizon_value-less) `emitted` must not strip the real
# per-lead horizon_value sourced from `existing`.
#
# The empty_stats frame returned by _calculate_aggregated_skill_metrics when
# the input forecasts frame is empty/lacks horizon_value has columns like
# [period_col, "code", "model_short"] + metrics + n_pairs — with NO
# horizon_value column. That frame flows into build_stale_tombstones as
# `emitted`. Pre-fix, the final `reindex(columns=emitted.columns)` DROPPED the
# horizon_value column that `tombstones` already carried from key_cols, so the
# writer defaulted the missing column to sentinel 0 — collapsing distinct
# per-lead stale rows (hv=1/2/3) onto a single hv=0 row, and the real per-lead
# DB rows were never invalidated. These tests lock the flag-ON fix that keeps
# each real per-lead horizon_value distinct, and confirm flag-OFF is untouched.
# ---------------------------------------------------------------------------


def _empty_hvless_emitted(period_col: str) -> pd.DataFrame:
    """Empty skill frame with NO horizon_value column, mimicking the real
    empty_stats shape from _calculate_aggregated_skill_metrics when the input
    forecasts frame is empty / lacks horizon_value."""
    return pd.DataFrame(
        columns=[
            period_col,
            "code",
            "model_short",
            "sdivsigma",
            "nse",
            "delta",
            "accuracy",
            "mae",
            "n_pairs",
        ]
    )


class TestFlagOnEmittedEmptyPreservesRealLead:
    """Flag ON, quarter: emitted is EMPTY and lacks horizon_value. The real
    per-lead horizon_value carried by existing must survive into the
    tombstones (not be dropped and defaulted to sentinel 0 downstream)."""

    def test_single_lead(self, lead_aware_on):
        existing = _df(
            [
                {
                    "code": "19999",
                    "quarter_in_year": 1,
                    "horizon_value": 1,
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
        emitted = _empty_hvless_emitted("quarter_in_year")
        assert "horizon_value" not in emitted.columns  # pre-condition

        result = build_stale_tombstones(existing, emitted, "quarter_in_year")

        assert len(result) == 1
        row = result.iloc[0]
        assert row["horizon_value"] == 1  # the REAL lead, NOT sentinel 0
        assert row["n_pairs"] == 0

    def test_quarter_multi_lead(self, lead_aware_on):
        existing = _df(
            [
                {
                    "code": "19999",
                    "quarter_in_year": 1,
                    "horizon_value": hv,
                    "model_short": "LR_BASE",
                    "n_pairs": 5,
                    "sdivsigma": 0.6,
                    "nse": 0.4,
                    "delta": 0.1,
                    "accuracy": 0.75,
                    "mae": 8.0,
                }
                for hv in (1, 2, 3)
            ]
        )
        emitted = _empty_hvless_emitted("quarter_in_year")
        assert "horizon_value" not in emitted.columns

        result = build_stale_tombstones(existing, emitted, "quarter_in_year")

        # Each lead tombstoned distinctly, NOT collapsed to a single hv=0 row.
        assert len(result) == 3
        assert set(result["horizon_value"]) == {1, 2, 3}
        assert (result["n_pairs"] == 0).all()


class TestFlagOnEmittedEmptyPreservesRealLeadSeason:
    """Flag ON, season: same as the quarter case but with
    period_col='season_in_year'."""

    def test_single_lead(self, lead_aware_on):
        existing = _df(
            [
                {
                    "code": "19999",
                    "season_in_year": 1,
                    "horizon_value": 2,
                    "model_short": "LR_SM",
                    "n_pairs": 5,
                    "sdivsigma": 0.5,
                    "nse": 0.3,
                    "delta": 0.15,
                    "accuracy": 0.7,
                    "mae": 12.0,
                }
            ]
        )
        emitted = _empty_hvless_emitted("season_in_year")
        assert "horizon_value" not in emitted.columns

        result = build_stale_tombstones(existing, emitted, "season_in_year")

        assert len(result) == 1
        row = result.iloc[0]
        assert row["horizon_value"] == 2  # the REAL lead, NOT sentinel 0
        assert row["n_pairs"] == 0

    def test_multi_lead(self, lead_aware_on):
        existing = _df(
            [
                {
                    "code": "19999",
                    "season_in_year": 1,
                    "horizon_value": hv,
                    "model_short": "LR_SM",
                    "n_pairs": 5,
                    "sdivsigma": 0.5,
                    "nse": 0.3,
                    "delta": 0.15,
                    "accuracy": 0.7,
                    "mae": 12.0,
                }
                for hv in (1, 2, 3)
            ]
        )
        emitted = _empty_hvless_emitted("season_in_year")
        assert "horizon_value" not in emitted.columns

        result = build_stale_tombstones(existing, emitted, "season_in_year")

        assert len(result) == 3
        assert set(result["horizon_value"]) == {1, 2, 3}
        assert (result["n_pairs"] == 0).all()


class TestFlagOffEmittedEmptyUnchanged:
    """Flag OFF, quarter: emitted is the SAME empty hv-less frame. Behavior
    must be byte-identical to the pre-fix flag-OFF path: the reindex is
    unchanged, so the output columns exactly equal emitted.columns and NO
    horizon_value column is added (the writer supplies the legacy sentinel 0
    downstream). This proves the flag-OFF sentinel-0 path is untouched."""

    def test_quarter(self):
        assert os.environ.get("SAPPHIRE_SKILL_LEAD_AWARE") in (None, "", "0", "false", "off", "no")

        existing = _df(
            [
                {
                    "code": "19999",
                    "quarter_in_year": 2,
                    "horizon_value": 0,  # legacy sentinel
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
        emitted = _empty_hvless_emitted("quarter_in_year")

        result = build_stale_tombstones(existing, emitted, "quarter_in_year")

        # Byte-identical to pre-fix flag-OFF: columns exactly == emitted.columns,
        # horizon_value NOT added here (writer defaults it to sentinel 0).
        assert list(result.columns) == list(emitted.columns)
        assert "horizon_value" not in result.columns
        assert len(result) == 1
        assert result.iloc[0]["n_pairs"] == 0
