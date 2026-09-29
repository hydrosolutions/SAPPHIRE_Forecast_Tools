"""PP-065 P1c: quarter-only ensemble/skill behavior not covered elsewhere.

Covers the plan's Tests list items not already exercised (post-fix) by the
existing quarter test files:
- Direct unit tests of the shared helpers (`_skip_em_for_quarter`,
  `_quarter_group_cols_without_composition`, `_quarter_composition_labels`)
  that both `ensemble_calculator.py` and `skill_metrics.py` import.
- Quantile nulling is per column (an LR-only Naive Mean with q50 null
  keeps q05-q95; adding a derived member nulls all of its own quantile
  columns without breaking the other members' per-column average).
- Recalc path, composition-free: 12 target years with compositions 5/2/5
  (each subset alone below K=10) -> one persisted row per ensemble,
  n_pairs=12; 9 years -> the group is suppressed.
- Flag OFF, skill hv=0, forecast hv=1 -> Naive Mean and Skilled Mean still
  form (the hv key must be gated on the flag, not on column presence).
- Tombstone: a stored quarter EM skill row is correctly identified as
  stale and tombstoned by the recalc (build_stale_tombstones), since
  quarter no longer emits EM at all.

Placeholder station code "19999" throughout (never a real code).
"""

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.ensemble_calculator import (
    _skip_em_for_quarter,
    create_quarterly_ensemble_forecasts,
)
from src.skill_metrics import (
    _quarter_composition_labels,
    _quarter_group_cols_without_composition,
    calculate_quarterly_skill_metrics,
)
from src.stale_tombstones import build_stale_tombstones

STATION = "19999"
QUANTILE_COLS = ["q05", "q10", "q25", "q50", "q75", "q90", "q95"]


def _q_row(q50: float) -> list:
    q = float(q50)
    return [q * 0.70, q * 0.75, q * 0.85, q, q * 1.15, q * 1.25, q * 1.30]


def _make_quarterly_obs(rows):
    """Rows: (code, year, quarter_in_year, discharge_avg)."""
    df = pd.DataFrame(rows, columns=["code", "year", "quarter_in_year", "discharge_avg"])
    delta_df = (
        df.groupby(["code", "quarter_in_year"])
        .agg(std_discharge=("discharge_avg", "std"))
        .reset_index()
    )
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)
    return df.merge(delta_df[["code", "quarter_in_year", "delta"]], on=["code", "quarter_in_year"])


def _make_quarterly_fcst(rows):
    """Rows: (code, year, quarter_in_year, model_short, q50)."""
    records = []
    for code, year, qiy, model, q50 in rows:
        records.append([code, year, qiy, model] + _q_row(q50))
    return pd.DataFrame(
        records, columns=["code", "year", "quarter_in_year", "model_short"] + QUANTILE_COLS
    )


# ===========================================================================
# 1. Shared helper unit tests
# ===========================================================================


class TestSkipEmForQuarterHelper:
    def test_true_for_quarter(self):
        assert _skip_em_for_quarter("quarter_in_year") is True

    def test_false_for_season(self):
        assert _skip_em_for_quarter("season_in_year") is False


class TestQuarterGroupColsWithoutCompositionHelper:
    def test_quarter_drops_composition(self):
        cols = _quarter_group_cols_without_composition(
            ["quarter_in_year", "code", "model_short"], "quarter_in_year"
        )
        assert cols == ["quarter_in_year", "code", "model_short"]
        assert "composition" not in cols

    def test_season_keeps_composition(self):
        cols = _quarter_group_cols_without_composition(
            ["season_in_year", "code", "model_short"], "season_in_year"
        )
        assert cols == ["season_in_year", "code", "model_short", "composition"]


class TestQuarterCompositionLabelsHelper:
    def test_union_is_sorted_deduplicated_and_non_blank(self):
        df = pd.DataFrame(
            {
                "quarter_in_year": [1, 1, 1],
                "code": [STATION] * 3,
                "composition": ["LR_Base, LR_SM", "LR_Base, LR_SM, GBT", "LR_Base, TFT"],
            }
        )
        labels = _quarter_composition_labels(df, ["quarter_in_year", "code"])
        assert len(labels) == 1
        comp = labels.iloc[0]["composition"]
        assert comp, "composition label must never be blank"
        assert comp == "GBT, LR_Base, LR_SM, TFT"

    def test_per_group_isolation(self):
        """Two distinct groups get their own independent union label."""
        df = pd.DataFrame(
            {
                "quarter_in_year": [1, 1, 2, 2],
                "code": [STATION] * 4,
                "composition": ["LR_Base, LR_SM", "LR_Base, LR_SM", "GBT, TFT", "GBT, TFT"],
            }
        )
        labels = _quarter_composition_labels(df, ["quarter_in_year", "code"]).set_index(
            "quarter_in_year"
        )
        assert labels.loc[1, "composition"] == "LR_Base, LR_SM"
        assert labels.loc[2, "composition"] == "GBT, TFT"


# ===========================================================================
# 2. Quantile nulling is per column, not per row
# ===========================================================================


class TestQuantileNullingPerColumn:
    """Naive Mean's per-column .agg("mean") must not cascade a null q50
    into other quantile columns, and adding a member with entirely-null
    quantiles must not zero out columns the other members still supply."""

    _SKILL_COLS = [
        "quarter_in_year",
        "code",
        "model_short",
        "sdivsigma",
        "nse",
        "delta",
        "accuracy",
        "mae",
        "n_pairs",
    ]

    def _dummy_skill(self):
        # Any non-empty skill_stats avoids the early-empty-skill guard;
        # Naive Mean itself is not skill-gated.
        return pd.DataFrame(
            [(1, STATION, "LR_Base", 0.3, 0.9, 5.0, 0.9, 2.0, 10)], columns=self._SKILL_COLS
        )

    def test_lr_only_naive_mean_null_q50_keeps_other_quantiles(self):
        fcst = pd.DataFrame(
            [
                {
                    "code": STATION,
                    "year": 2025,
                    "quarter_in_year": 1,
                    "model_short": "LR_Base",
                    "forecasted_discharge": 100.0,
                    "q05": 80.0,
                    "q10": 85.0,
                    "q25": 90.0,
                    "q50": np.nan,
                    "q75": 110.0,
                    "q90": 115.0,
                    "q95": 120.0,
                },
                {
                    "code": STATION,
                    "year": 2025,
                    "quarter_in_year": 1,
                    "model_short": "LR_SM",
                    "forecasted_discharge": 120.0,
                    "q05": 100.0,
                    "q10": 105.0,
                    "q25": 110.0,
                    "q50": np.nan,
                    "q75": 130.0,
                    "q90": 135.0,
                    "q95": 140.0,
                },
            ]
        )
        result = create_quarterly_ensemble_forecasts(fcst, self._dummy_skill())
        nm = result[result["model_short"] == "Naive Mean"]
        assert len(nm) == 1
        row = nm.iloc[0]
        assert pd.isna(row["q50"]), "q50 null for both members must stay null"
        for qcol in ("q05", "q10", "q25", "q75", "q90", "q95"):
            assert pd.notna(row[qcol]), f"{qcol} must not be nulled by the unrelated q50 NaN"
        assert row["q05"] == _mean(80.0, 100.0)
        assert row["q95"] == _mean(120.0, 140.0)

    def test_adding_all_null_derived_member_does_not_null_other_columns(self):
        """A 3rd member with ALL quantile columns null (a point-only/derived
        model) must not drag the other members' valid per-column values to
        null — the per-column mean must skip only that member's NaNs."""
        fcst = pd.DataFrame(
            [
                {
                    "code": STATION,
                    "year": 2025,
                    "quarter_in_year": 1,
                    "model_short": "LR_Base",
                    "forecasted_discharge": 100.0,
                    "q05": 80.0,
                    "q10": 85.0,
                    "q25": 90.0,
                    "q50": 100.0,
                    "q75": 110.0,
                    "q90": 115.0,
                    "q95": 120.0,
                },
                {
                    "code": STATION,
                    "year": 2025,
                    "quarter_in_year": 1,
                    "model_short": "LR_SM",
                    "forecasted_discharge": 120.0,
                    "q05": 100.0,
                    "q10": 105.0,
                    "q25": 110.0,
                    "q50": 120.0,
                    "q75": 130.0,
                    "q90": 135.0,
                    "q95": 140.0,
                },
                {
                    "code": STATION,
                    "year": 2025,
                    "quarter_in_year": 1,
                    "model_short": "GBT",
                    "forecasted_discharge": 110.0,
                    "q05": np.nan,
                    "q10": np.nan,
                    "q25": np.nan,
                    "q50": np.nan,
                    "q75": np.nan,
                    "q90": np.nan,
                    "q95": np.nan,
                },
            ]
        )
        result = create_quarterly_ensemble_forecasts(fcst, self._dummy_skill())
        nm = result[result["model_short"] == "Naive Mean"]
        assert len(nm) == 1
        row = nm.iloc[0]
        # q50 now has ONE valid contributor's worth of NaN removed by
        # skipna — LR_Base/LR_SM both have real q50 values, so it must be
        # their mean, unaffected by GBT's fully-null row.
        assert row["q50"] == _mean(100.0, 120.0)
        for qcol in ("q05", "q10", "q25", "q75", "q90", "q95"):
            assert pd.notna(row[qcol])
        assert row["q05"] == _mean(80.0, 100.0)
        assert row["q95"] == _mean(120.0, 140.0)
        # Composition includes all 3 raw models even though GBT contributed
        # no quantile information.
        assert "GBT" in str(row["composition"])


def _mean(*values: float) -> float:
    """Small helper: exact mean of the given values (no pytest.approx
    needed since these are clean floats)."""
    return sum(values) / len(values)


# ===========================================================================
# 3. Recalc path, composition-free grouping
# ===========================================================================


class TestCompositionFreeGroupingRecalc:
    """A (code, quarter[, hv]) key whose contributing models change across
    years must be scored as ONE combined skill row (PP-065 P1c decision 4),
    not as one row per distinct per-year composition."""

    _MODELS_BY_ERA = {
        # 5 years: A + B only.
        "era1": (range(2000, 2005), ("LR_Base", "LR_SM")),
        # 2 years: A + B + a transient third model.
        "era2": (range(2005, 2007), ("LR_Base", "LR_SM", "GBT")),
        # 5 years: A + B + a different transient third model.
        "era3": (range(2007, 2012), ("LR_Base", "LR_SM", "TFT")),
    }

    def _build(self, eras: dict):
        obs_rows = []
        fcst_rows = []
        year_counter = 2000
        for _era, (years, models) in eras.items():
            for y in years:
                obs = 100.0 + (y - 2000) * 2.0
                obs_rows.append((STATION, y, 1, obs))
                for model in models:
                    # Small, consistent offset keeps NSE > 0 for every model.
                    offset = -1.0 if model == "LR_Base" else (1.0 if model == "LR_SM" else 0.5)
                    fcst_rows.append((STATION, y, 1, model, obs + offset))
                year_counter = y
        del year_counter
        obs_df = _make_quarterly_obs(obs_rows)
        fcst_df = _make_quarterly_fcst(fcst_rows)
        return obs_df, fcst_df

    def test_5_2_5_years_combine_into_one_row_with_n_pairs_12(self):
        obs, fcst = self._build(self._MODELS_BY_ERA)
        skill_stats, _, _ = calculate_quarterly_skill_metrics(obs, fcst)

        naive_rows = skill_stats[skill_stats["model_short"] == "Naive Mean"]
        assert len(naive_rows) == 1, (
            "Composition-free grouping must combine all 12 years into ONE "
            f"Naive Mean row regardless of per-year composition; got "
            f"{len(naive_rows)} rows: {naive_rows[['n_pairs', 'composition']].to_dict('records')}"
        )
        assert int(naive_rows.iloc[0]["n_pairs"]) == 12
        comp = naive_rows.iloc[0]["composition"]
        assert comp, "composition label must be a non-null stable union"
        for model in ("LR_Base", "LR_SM", "GBT", "TFT"):
            assert model in comp

        sm_rows = skill_stats[skill_stats["model_short"] == "Skilled Mean"]
        assert len(sm_rows) == 1, (
            f"Expected exactly one combined Skilled Mean row, got {len(sm_rows)}"
        )
        assert int(sm_rows.iloc[0]["n_pairs"]) == 12

        # No erroneous tombstone concern here — this is the "emitted" side;
        # tombstoning against a stale EXISTING row is covered separately
        # below (TestTombstoneQuarterEM).
        em_rows = skill_stats[skill_stats["model_short"] == "EM"]
        assert em_rows.empty

    def test_9_years_is_suppressed_below_k(self):
        """The same shape with only 9 total years (4/2/3) stays below
        K=10 and the group must be suppressed entirely."""
        eras = {
            "era1": (range(2000, 2004), ("LR_Base", "LR_SM")),  # 4 years
            "era2": (range(2004, 2006), ("LR_Base", "LR_SM", "GBT")),  # 2 years
            "era3": (range(2006, 2009), ("LR_Base", "LR_SM", "TFT")),  # 3 years
        }
        obs, fcst = self._build(eras)
        skill_stats, _, _ = calculate_quarterly_skill_metrics(obs, fcst)

        naive_rows = skill_stats[skill_stats["model_short"] == "Naive Mean"]
        sm_rows = skill_stats[skill_stats["model_short"] == "Skilled Mean"]
        assert naive_rows.empty, (
            f"9 years (< K=10) must be suppressed, got {len(naive_rows)} Naive Mean rows"
        )
        assert sm_rows.empty, f"9 years (< K=10) must be suppressed, got {len(sm_rows)} SM rows"


# ===========================================================================
# 4. Flag OFF: hv key gated on the FLAG, never on column presence
# ===========================================================================


class TestFlagOffHvGatedOnFlagNotColumnPresence:
    def test_flag_off_hv_mismatch_still_forms_naive_and_skilled_mean(self, monkeypatch):
        """skill sits at horizon_value=0 (flag-off sentinel) while the
        forecast frame carries horizon_value=1 (e.g. a stray/legacy value).
        If ensemble grouping followed column presence instead of the flag,
        this mismatch would starve Skilled Mean's membership merge. Both
        Naive Mean and Skilled Mean must still form because flag-OFF's
        3-key (period, code, model) grouping ignores horizon_value
        entirely."""
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        skill = pd.DataFrame(
            {
                "quarter_in_year": [1, 1],
                "horizon_value": [0, 0],
                "code": [STATION, STATION],
                "model_short": ["LR_Base", "LR_SM"],
                "sdivsigma": [0.3, 0.4],
                "nse": [0.9, 0.85],
                "delta": [5.0, 5.0],
                "accuracy": [0.9, 0.85],
                "mae": [2.0, 3.0],
                "n_pairs": [10, 10],
            }
        )
        fcst = pd.DataFrame(
            {
                "code": [STATION, STATION],
                "year": [2025, 2025],
                "quarter_in_year": [1, 1],
                "horizon_value": [1, 1],
                "model_short": ["LR_Base", "LR_SM"],
                "forecasted_discharge": [100.0, 120.0],
                "q05": [80.0, 90.0],
                "q10": [85.0, 95.0],
                "q25": [90.0, 100.0],
                "q50": [100.0, 120.0],
                "q75": [110.0, 130.0],
                "q90": [115.0, 135.0],
                "q95": [120.0, 140.0],
            }
        )
        result = create_quarterly_ensemble_forecasts(fcst, skill)
        models = set(result["model_short"].unique())
        assert "Naive Mean" in models, (
            "Naive Mean must not be starved by a flag-off hv column mismatch"
        )
        assert "Skilled Mean" in models, (
            "Skilled Mean's membership merge must ignore horizon_value under flag OFF, "
            "not silently fail because forecast hv=1 != skill hv=0"
        )
        # Quarter never forms EM (PP-065 P1c decision 3) — independent of this scenario.
        assert "EM" not in models


# ===========================================================================
# 5. Tombstone: a stored quarter EM row must be identified as stale
# ===========================================================================


class TestTombstoneQuarterEM:
    def test_stored_quarter_em_row_gets_tombstoned_by_recalc(self):
        """Simulates a quarter EM skill row written before this change.
        Since the recalc no longer emits EM for quarter at all, that
        existing row must be correctly identified as stale and tombstoned
        — not silently left stale forever."""
        n = 10  # == K_QUARTER, so the still-emitted rows survive the floor
        obs_rows = [(STATION, 2010 + i, 1, 100.0 + i * 2.0) for i in range(n)]
        fcst_rows = [(STATION, 2010 + i, 1, "LR_Base", 101.0 + i * 2.0) for i in range(n)] + [
            (STATION, 2010 + i, 1, "LR_SM", 99.0 + i * 2.0) for i in range(n)
        ]
        obs = _make_quarterly_obs(obs_rows)
        fcst = _make_quarterly_fcst(fcst_rows)

        emitted, _, _ = calculate_quarterly_skill_metrics(obs, fcst)
        # Sanity: this recalc really does not emit an EM row for quarter.
        assert emitted[emitted["model_short"] == "EM"].empty

        existing = pd.DataFrame(
            [
                {
                    # A quarter EM row stored before this change shipped.
                    "code": STATION,
                    "quarter_in_year": 1,
                    "horizon_value": 0,
                    "model_short": "EM",
                    "n_pairs": 10,
                    "sdivsigma": 0.4,
                    "nse": 0.6,
                    "delta": 5.0,
                    "accuracy": 0.85,
                    "mae": 3.0,
                },
                {
                    # A raw-model row that the recalc STILL emits — must
                    # NOT be tombstoned.
                    "code": STATION,
                    "quarter_in_year": 1,
                    "horizon_value": 0,
                    "model_short": "LR_Base",
                    "n_pairs": 10,
                    "sdivsigma": 0.3,
                    "nse": 0.9,
                    "delta": 5.0,
                    "accuracy": 0.9,
                    "mae": 1.0,
                },
            ]
        )

        tombstones = build_stale_tombstones(existing, emitted, "quarter_in_year")

        assert len(tombstones) == 1, (
            f"Expected exactly one tombstone (the stale EM row), got "
            f"{len(tombstones)}: {tombstones[['model_short']].to_dict('records') if not tombstones.empty else []}"
        )
        row = tombstones.iloc[0]
        assert row["model_short"] == "EM"
        assert row["n_pairs"] == 0
        assert pd.isna(row["nse"])
        assert pd.isna(row["mae"])
