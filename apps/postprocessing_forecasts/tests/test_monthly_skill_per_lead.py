"""Tests for per-lead (horizon_value) grouping in calculate_monthly_skill_metrics.

PP-038: month skill stratified by forecast lead so each
(month_in_year, code, model_short) no longer pools leads 0-3.

Tests are written before implementation (TDD).
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.skill_metrics import calculate_monthly_skill_metrics

# Use station code following convention (no real codes)
STATION = "19999"


# ===========================================================================
# Shared helpers
# ===========================================================================


def _make_obs(rows):
    """Create observations DataFrame from (code, year, month, discharge_avg).

    Automatically computes month_in_year and delta (0.674 * std).
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
        how="left",
    )


def _make_fcst_lead(rows):
    """Create forecasts DataFrame WITH horizon_value.

    Each row: (code, year, month, model_short, horizon_value,
               q05, q10, q25, q50, q75, q90, q95)
    """
    return pd.DataFrame(
        rows,
        columns=[
            "code",
            "year",
            "month",
            "model_short",
            "horizon_value",
            "q05",
            "q10",
            "q25",
            "q50",
            "q75",
            "q90",
            "q95",
        ],
    )


def _make_fcst_no_lead(rows):
    """Create forecasts DataFrame WITHOUT horizon_value (legacy / no-lead format)."""
    return pd.DataFrame(
        rows,
        columns=[
            "code",
            "year",
            "month",
            "model_short",
            "q05",
            "q10",
            "q25",
            "q50",
            "q75",
            "q90",
            "q95",
        ],
    )


# ===========================================================================
# Per-lead model rows
# ===========================================================================


class TestPerLeadModelRows:
    """calculate_monthly_skill_metrics groups by horizon_value for model rows."""

    @pytest.fixture
    def obs_3yr(self):
        """Three years of observations for station STATION, month 1."""
        return _make_obs(
            [
                (STATION, 2020, 1, 100.0),
                (STATION, 2021, 1, 110.0),
                (STATION, 2022, 1, 105.0),
            ]
        )

    @pytest.fixture
    def fcst_two_leads(self):
        """Single model M1 with leads 0 and 1, 3 years.

        lead=0: q50=[102, 108, 104], obs=[100, 110, 105] -> MAE=(2+2+1)/3 = 5/3
        lead=1: q50=[ 88,  94,  90], obs=[100, 110, 105] -> MAE=(12+16+15)/3 = 43/3
        """
        return _make_fcst_lead(
            [
                # lead 0 — close to obs
                (STATION, 2020, 1, "M1", 0, 80, 85, 92, 102, 112, 118, 125),
                (STATION, 2021, 1, "M1", 0, 88, 93, 100, 108, 116, 123, 130),
                (STATION, 2022, 1, "M1", 0, 84, 89, 97, 104, 113, 120, 128),
                # lead 1 — further from obs
                (STATION, 2020, 1, "M1", 1, 72, 77, 83, 88, 94, 100, 106),
                (STATION, 2021, 1, "M1", 1, 79, 84, 89, 94, 100, 106, 112),
                (STATION, 2022, 1, "M1", 1, 75, 80, 85, 90, 97, 103, 109),
            ]
        )

    def test_output_has_horizon_value_column(self, obs_3yr, fcst_two_leads):
        """skill_stats must carry a horizon_value column."""
        stats, _, _ = calculate_monthly_skill_metrics(obs_3yr, fcst_two_leads)
        assert "horizon_value" in stats.columns, "horizon_value column missing from skill_stats"

    def test_two_leads_produce_two_rows_per_model(self, obs_3yr, fcst_two_leads):
        """Two distinct leads produce two rows for the same (month, code, model)."""
        stats, _, _ = calculate_monthly_skill_metrics(obs_3yr, fcst_two_leads)
        m1_rows = stats[stats["model_short"] == "M1"]
        assert len(m1_rows) == 2, f"Expected 2 rows (one per lead), got {len(m1_rows)}"
        assert set(m1_rows["horizon_value"].unique()) == {0, 1}

    def test_per_lead_metrics_differ(self, obs_3yr, fcst_two_leads):
        """Lead-0 and lead-1 have distinct MAE (not pooled)."""
        stats, _, _ = calculate_monthly_skill_metrics(obs_3yr, fcst_two_leads)
        m1 = stats[stats["model_short"] == "M1"]
        mae0 = m1[m1["horizon_value"] == 0]["mae"].iloc[0]
        mae1 = m1[m1["horizon_value"] == 1]["mae"].iloc[0]
        assert mae0 != mae1, "Lead-0 and lead-1 MAE must differ when forecasts differ"
        # Lead-0 is closer to obs -> lower MAE
        assert mae0 < mae1

    def test_per_lead_n_pairs_correct(self, obs_3yr, fcst_two_leads):
        """Each lead accumulates only its own pairs (3 years -> n_pairs=3)."""
        stats, _, _ = calculate_monthly_skill_metrics(obs_3yr, fcst_two_leads)
        m1 = stats[stats["model_short"] == "M1"]
        for _, row in m1.iterrows():
            assert row["n_pairs"] == 3, (
                f"Expected n_pairs=3 for lead={row['horizon_value']}, got {row['n_pairs']}"
            )

    def test_lead0_mae_value_correct(self, obs_3yr, fcst_two_leads):
        """Lead-0 MAE = (|100-102|+|110-108|+|105-104|)/3 = 5/3."""
        stats, _, _ = calculate_monthly_skill_metrics(obs_3yr, fcst_two_leads)
        m1 = stats[stats["model_short"] == "M1"]
        row0 = m1[m1["horizon_value"] == 0].iloc[0]
        expected_mae = (abs(100.0 - 102.0) + abs(110.0 - 108.0) + abs(105.0 - 104.0)) / 3.0
        assert row0["mae"] == pytest.approx(expected_mae, rel=1e-5)


# ===========================================================================
# Backward compatibility — no horizon_value in input
# ===========================================================================


class TestNoHorizonValueBackwardCompat:
    """Inputs without horizon_value still work; output defaults to horizon_value=0."""

    def test_no_horizon_value_col_works(self):
        """Legacy input (no horizon_value) does not raise."""
        obs = _make_obs(
            [
                (STATION, 2020, 1, 100.0),
                (STATION, 2021, 1, 110.0),
            ]
        )
        fcst = _make_fcst_no_lead(
            [
                (STATION, 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                (STATION, 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
            ]
        )
        stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        assert not stats.empty

    def test_no_horizon_value_defaults_to_zero(self):
        """Output has horizon_value=0 when input has no horizon_value column."""
        obs = _make_obs(
            [
                (STATION, 2020, 1, 100.0),
                (STATION, 2021, 1, 110.0),
            ]
        )
        fcst = _make_fcst_no_lead(
            [
                (STATION, 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                (STATION, 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
            ]
        )
        stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        m1 = stats[stats["model_short"] == "M1"]
        assert len(m1) == 1
        assert "horizon_value" in m1.columns
        assert int(m1.iloc[0]["horizon_value"]) == 0

    def test_single_lead_regression_metrics(self):
        """Single-lead (horizon_value=0) produces same metrics as the old grouping.

        Reference values match test_monthly_skill_metrics.py TestMonthlyMetricsBasic:
          obs=[100,110], q50=[102,108] -> MAE=2.0, NSE=0.84, sdivsigma=0.4
        """
        obs = _make_obs(
            [
                (STATION, 2020, 1, 100.0),
                (STATION, 2021, 1, 110.0),
            ]
        )
        fcst = _make_fcst_lead(
            [
                (STATION, 2020, 1, "M1", 0, 80, 85, 92, 102, 112, 118, 125),
                (STATION, 2021, 1, "M1", 0, 88, 93, 100, 108, 116, 123, 130),
            ]
        )
        stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        m1 = stats[(stats["model_short"] == "M1") & (stats["horizon_value"] == 0)]
        assert len(m1) == 1
        row = m1.iloc[0]
        assert row["mae"] == pytest.approx(2.0, rel=1e-6)
        assert row["n_pairs"] == 2
        assert row["sdivsigma"] == pytest.approx(0.4, rel=1e-6)
        assert row["nse"] == pytest.approx(0.84, rel=1e-6)

    def test_nan_horizon_value_defaults_to_zero(self):
        """Rows with NaN horizon_value are coerced to 0, not dropped."""
        obs = _make_obs(
            [
                (STATION, 2020, 1, 100.0),
                (STATION, 2021, 1, 110.0),
            ]
        )
        fcst = pd.DataFrame(
            {
                "code": [STATION, STATION],
                "year": [2020, 2021],
                "month": [1, 1],
                "model_short": ["M1", "M1"],
                "horizon_value": [np.nan, np.nan],
                "q05": [80, 88],
                "q10": [85, 93],
                "q25": [92, 100],
                "q50": [102, 108],
                "q75": [112, 116],
                "q90": [118, 123],
                "q95": [125, 130],
            }
        )
        stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        m1 = stats[stats["model_short"] == "M1"]
        assert not m1.empty, "NaN horizon_value rows should not be dropped"
        assert int(m1.iloc[0]["horizon_value"]) == 0


# ===========================================================================
# EM per-lead
# ===========================================================================


class TestEMPerLead:
    """EM rows are computed per lead, not pooled across leads."""

    @pytest.fixture
    def obs_2yr(self):
        return _make_obs(
            [
                (STATION, 2020, 1, 100.0),
                (STATION, 2021, 1, 110.0),
            ]
        )

    @pytest.fixture
    def two_model_two_lead_fcst(self):
        """Two models (M1, M2), two leads (0, 1), 2 years.

        Both leads use forecasts close to obs so both pass skill thresholds
        (NSE > 0.8, sdivsigma < 0.6, accuracy > 0.8).

        obs=[100, 110], obs_mean=105, sigma=7.071, delta=4.766
        NSE threshold: sum_sq_errors < 50 * (1-0.8) = 10

        Lead-0: M1 q50=[102,108], M2 q50=[101,109]
          EM q50 = [101.5, 108.5], MAE = 1.5
        Lead-1: M1 q50=[101,109], M2 q50=[100,111]
          EM q50 = [100.5, 110.0], MAE = (0.5+0)/2 = 0.25
        """
        return _make_fcst_lead(
            [
                # M1, lead 0 — close to obs
                (STATION, 2020, 1, "M1", 0, 80, 85, 92, 102, 112, 118, 125),
                (STATION, 2021, 1, "M1", 0, 88, 93, 100, 108, 116, 123, 130),
                # M2, lead 0 — close to obs
                (STATION, 2020, 1, "M2", 0, 82, 87, 94, 101, 108, 114, 120),
                (STATION, 2021, 1, "M2", 0, 90, 95, 102, 109, 117, 124, 131),
                # M1, lead 1 — also close to obs (NSE = 1 - 2/50 = 0.96)
                (STATION, 2020, 1, "M1", 1, 79, 84, 91, 101, 111, 117, 124),
                (STATION, 2021, 1, "M1", 1, 87, 92, 99, 109, 117, 124, 131),
                # M2, lead 1 — also close to obs (NSE = 1 - 1/50 = 0.98)
                (STATION, 2020, 1, "M2", 1, 78, 83, 90, 100, 108, 114, 120),
                (STATION, 2021, 1, "M2", 1, 89, 94, 101, 111, 119, 126, 133),
            ]
        )

    def test_em_has_one_row_per_lead(self, obs_2yr, two_model_two_lead_fcst):
        """EM produces one row per lead present in input, not one pooled row."""
        stats, _, _ = calculate_monthly_skill_metrics(obs_2yr, two_model_two_lead_fcst)
        em_rows = stats[stats["model_short"] == "EM"]
        assert len(em_rows) == 2, f"Expected 2 EM rows (one per lead), got {len(em_rows)}"
        assert set(em_rows["horizon_value"].unique()) == {0, 1}

    def test_em_lead0_mae_from_lead0_models_only(self, obs_2yr, two_model_two_lead_fcst):
        """EM lead-0 averages only lead-0 model forecasts.

        M1 q50=[102,108], M2 q50=[101,109] -> EM q50=[101.5,108.5]
        obs=[100,110] -> MAE = 1.5
        """
        stats, _, _ = calculate_monthly_skill_metrics(obs_2yr, two_model_two_lead_fcst)
        em_lead0 = stats[(stats["model_short"] == "EM") & (stats["horizon_value"] == 0)]
        assert len(em_lead0) == 1
        assert em_lead0.iloc[0]["mae"] == pytest.approx(1.5, rel=1e-6)

    def test_em_lead0_differs_from_lead1(self, obs_2yr, two_model_two_lead_fcst):
        """EM has different MAE per lead (leads not mixed).

        Lead-0 EM: MAE=1.5, Lead-1 EM: MAE=0.25 (different values confirm no pooling).
        """
        stats, _, _ = calculate_monthly_skill_metrics(obs_2yr, two_model_two_lead_fcst)
        em = stats[stats["model_short"] == "EM"]
        assert len(em) == 2, "Both leads must produce EM for this comparison"
        mae0 = em[em["horizon_value"] == 0]["mae"].iloc[0]
        mae1 = em[em["horizon_value"] == 1]["mae"].iloc[0]
        assert mae0 != mae1, "EM MAE must differ across leads (leads not pooled)"

    def test_em_joint_forecasts_carries_horizon_value(self, obs_2yr, two_model_two_lead_fcst):
        """EM rows appended to joint_forecasts carry horizon_value for each lead."""
        _, joint, _ = calculate_monthly_skill_metrics(obs_2yr, two_model_two_lead_fcst)
        em_joint = joint[joint["model_short"] == "EM"]
        assert not em_joint.empty
        if "horizon_value" in em_joint.columns:
            # Both leads should be present in joint_forecasts
            leads = set(em_joint["horizon_value"].dropna().astype(int).unique())
            assert 0 in leads
            assert 1 in leads


# ===========================================================================
# Naive Mean per-lead
# ===========================================================================


class TestNaiveMeanPerLead:
    """Naive Mean rows are computed per lead, not pooled across leads."""

    @pytest.fixture
    def obs_3yr(self):
        return _make_obs(
            [
                (STATION, 2020, 1, 100.0),
                (STATION, 2021, 1, 110.0),
                (STATION, 2022, 1, 105.0),
            ]
        )

    @pytest.fixture
    def two_model_two_lead_fcst(self):
        """Two models, two leads, 3 years.

        Lead-0 q50: M1=[102,108,104], M2=[104,112,108]
          Naive Mean lead-0 q50: [103, 110, 106]
          obs=[100,110,105] -> MAE = (3+0+1)/3 = 4/3

        Lead-1 q50: M1=[88,94,90], M2=[86,92,88]
          Naive Mean lead-1 q50: [87, 93, 89]
          obs=[100,110,105] -> MAE = (13+17+16)/3 = 46/3
        """
        return _make_fcst_lead(
            [
                # M1, lead 0
                (STATION, 2020, 1, "M1", 0, 80, 85, 92, 102, 112, 118, 125),
                (STATION, 2021, 1, "M1", 0, 88, 93, 100, 108, 116, 123, 130),
                (STATION, 2022, 1, "M1", 0, 84, 89, 97, 104, 113, 120, 128),
                # M1, lead 1
                (STATION, 2020, 1, "M1", 1, 72, 77, 83, 88, 94, 100, 106),
                (STATION, 2021, 1, "M1", 1, 79, 84, 89, 94, 100, 106, 112),
                (STATION, 2022, 1, "M1", 1, 75, 80, 85, 90, 97, 103, 109),
                # M2, lead 0
                (STATION, 2020, 1, "M2", 0, 82, 87, 94, 104, 114, 120, 127),
                (STATION, 2021, 1, "M2", 0, 90, 95, 102, 112, 120, 127, 134),
                (STATION, 2022, 1, "M2", 0, 86, 91, 99, 108, 116, 122, 130),
                # M2, lead 1
                (STATION, 2020, 1, "M2", 1, 70, 75, 81, 86, 92, 98, 104),
                (STATION, 2021, 1, "M2", 1, 77, 82, 87, 92, 98, 104, 110),
                (STATION, 2022, 1, "M2", 1, 73, 78, 83, 88, 95, 101, 107),
            ]
        )

    def test_naive_mean_has_one_row_per_lead(self, obs_3yr, two_model_two_lead_fcst):
        """Naive Mean produces one row per lead, not one pooled row."""
        stats, _, _ = calculate_monthly_skill_metrics(obs_3yr, two_model_two_lead_fcst)
        naive_rows = stats[stats["model_short"] == "Naive Mean"]
        assert len(naive_rows) == 2, (
            f"Expected 2 Naive Mean rows (one per lead), got {len(naive_rows)}"
        )
        assert set(naive_rows["horizon_value"].unique()) == {0, 1}

    def test_naive_mean_lead0_mae_correct(self, obs_3yr, two_model_two_lead_fcst):
        """Naive Mean lead-0 uses only lead-0 forecasts.

        Naive Mean lead-0 q50 = [103, 110, 106], obs=[100, 110, 105]
        MAE = (3+0+1)/3 = 4/3
        """
        stats, _, _ = calculate_monthly_skill_metrics(obs_3yr, two_model_two_lead_fcst)
        naive_lead0 = stats[(stats["model_short"] == "Naive Mean") & (stats["horizon_value"] == 0)]
        assert len(naive_lead0) == 1
        expected_mae = (abs(100.0 - 103.0) + abs(110.0 - 110.0) + abs(105.0 - 106.0)) / 3.0
        assert naive_lead0.iloc[0]["mae"] == pytest.approx(expected_mae, rel=1e-4)

    def test_naive_mean_lead0_differs_from_lead1(self, obs_3yr, two_model_two_lead_fcst):
        """Naive Mean MAE differs per lead (leads not mixed)."""
        stats, _, _ = calculate_monthly_skill_metrics(obs_3yr, two_model_two_lead_fcst)
        naive = stats[stats["model_short"] == "Naive Mean"]
        mae0 = naive[naive["horizon_value"] == 0]["mae"].iloc[0]
        mae1 = naive[naive["horizon_value"] == 1]["mae"].iloc[0]
        assert mae0 < mae1, "Lead-0 Naive Mean should have lower MAE (closer to obs)"


# ===========================================================================
# Skilled Mean per-lead
# ===========================================================================


class TestSkilledMeanPerLead:
    """Skilled Mean rows are computed per lead."""

    @pytest.fixture
    def obs_2yr(self):
        return _make_obs(
            [
                (STATION, 2020, 1, 100.0),
                (STATION, 2021, 1, 110.0),
            ]
        )

    @pytest.fixture
    def two_model_two_lead_fcst_skilled(self):
        """Two skilled models (both close to obs), two leads.

        Both M1 and M2 pass default thresholds on lead-0.
        Lead-1 models are further away (may or may not pass thresholds).
        """
        return _make_fcst_lead(
            [
                # M1, lead 0
                (STATION, 2020, 1, "M1", 0, 80, 85, 92, 102, 112, 118, 125),
                (STATION, 2021, 1, "M1", 0, 88, 93, 100, 108, 116, 123, 130),
                # M2, lead 0
                (STATION, 2020, 1, "M2", 0, 82, 87, 94, 101, 108, 114, 120),
                (STATION, 2021, 1, "M2", 0, 90, 95, 102, 109, 117, 124, 131),
                # M1, lead 1
                (STATION, 2020, 1, "M1", 1, 80, 85, 92, 102, 112, 118, 125),
                (STATION, 2021, 1, "M1", 1, 88, 93, 100, 108, 116, 123, 130),
                # M2, lead 1
                (STATION, 2020, 1, "M2", 1, 82, 87, 94, 101, 108, 114, 120),
                (STATION, 2021, 1, "M2", 1, 90, 95, 102, 109, 117, 124, 131),
            ]
        )

    def test_skilled_mean_per_lead_present(self, obs_2yr, two_model_two_lead_fcst_skilled):
        """Skilled Mean carries horizon_value when produced."""
        stats, _, _ = calculate_monthly_skill_metrics(obs_2yr, two_model_two_lead_fcst_skilled)
        sm_rows = stats[stats["model_short"] == "Skilled Mean"]
        if not sm_rows.empty:
            assert "horizon_value" in sm_rows.columns
            # Each Skilled Mean row must have a valid horizon_value
            assert sm_rows["horizon_value"].notna().all()

    def test_skilled_mean_per_lead_count(self, obs_2yr, two_model_two_lead_fcst_skilled):
        """Skilled Mean has at most one row per lead (not pooled)."""
        stats, _, _ = calculate_monthly_skill_metrics(obs_2yr, two_model_two_lead_fcst_skilled)
        sm_rows = stats[stats["model_short"] == "Skilled Mean"]
        if not sm_rows.empty:
            # At most one Skilled Mean row per (month_in_year, horizon_value, code)
            key_cols = [
                c for c in ["month_in_year", "horizon_value", "code"] if c in sm_rows.columns
            ]
            dupes = sm_rows.duplicated(subset=key_cols)
            assert not dupes.any(), (
                "Skilled Mean must not have duplicate (month_in_year, horizon_value, code) rows"
            )


# ===========================================================================
# n_pairs floor preserved per lead
# ===========================================================================


class TestNPairsFloorPerLead:
    """n_pairs >= 2 constraint applies per (lead, month, code, model)."""

    def test_single_year_per_lead_dropped_by_floor(self):
        """A per-lead group with only 1 year (n_pairs=1) is dropped by the
        n_pairs >= 2 floor applied at the end of the function.

        n_pairs=1 < 2, so the M1 lead-0 row is removed entirely.
        """
        obs = _make_obs(
            [
                (STATION, 2020, 1, 100.0),
            ]
        )
        fcst = _make_fcst_lead(
            [
                (STATION, 2020, 1, "M1", 0, 80, 85, 92, 102, 112, 118, 125),
            ]
        )
        stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        m1 = stats[stats["model_short"] == "M1"]
        # n_pairs=1 < 2 floor drops the thin per-lead row entirely
        assert m1.empty, "M1 lead-0 row with n_pairs=1 must be dropped by the floor"

    def test_thin_lead_dropped_qualifying_lead_kept(self):
        """The n_pairs >= 2 floor drops thin per-lead rows but keeps qualifying leads.

        lead-0 has 2 years (n_pairs=2, SURVIVES); lead-1 has 1 year
        (n_pairs=1, DROPPED by the floor).
        """
        obs = _make_obs(
            [
                (STATION, 2020, 1, 100.0),
                (STATION, 2021, 1, 110.0),
            ]
        )
        fcst = _make_fcst_lead(
            [
                (STATION, 2020, 1, "M1", 0, 80, 85, 92, 102, 112, 118, 125),
                (STATION, 2021, 1, "M1", 0, 88, 93, 100, 108, 116, 123, 130),
                # lead 1: single year -> n_pairs=1 -> dropped by floor
                (STATION, 2020, 1, "M1", 1, 72, 77, 83, 88, 94, 100, 106),
            ]
        )
        stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        m1 = stats[stats["model_short"] == "M1"]
        # Lead-0: n_pairs=2 survives, sdivsigma computable
        row0 = m1[m1["horizon_value"] == 0]
        assert len(row0) == 1
        assert row0.iloc[0]["n_pairs"] == 2
        assert not np.isnan(row0.iloc[0]["sdivsigma"])

        # Lead-1: n_pairs=1 dropped by the floor -> absent
        row1 = m1[m1["horizon_value"] == 1]
        assert row1.empty, "Lead-1 thin row (n_pairs=1) must be dropped by the floor"


# ===========================================================================
# Stale aggregate row (calendar-month horizon_value) must not be scored
# ===========================================================================


class TestStaleAggregateRowNotScored:
    """A stale aggregate row at horizon_value == month must not be scored.

    A previously-written aggregate (e.g. Naive Mean) can land at
    ``horizon_value = target month`` via the buggy calendar-month
    fallback. It must be excluded from the raw-model-skill groupby and the
    aggregate regenerated from the base models at their own (0) lead.
    """

    def test_stale_naive_mean_at_month_not_scored(self):
        """Stale Naive Mean at hv=8 yields no skill row at hv=8.

        Two base models live at horizon_value 0 (so the ensemble path
        activates); one stale ``Naive Mean`` row is injected at
        horizon_value 8 (the calendar month, the bug signature). No skill
        row may exist at horizon_value 8, and the regenerated aggregate
        rows must follow the base-model convention (0).
        """
        stale_month = 8
        obs = _make_obs(
            [
                (STATION, 2021, stale_month, 100.0),
                (STATION, 2022, stale_month, 110.0),
                (STATION, 2023, stale_month, 120.0),
            ]
        )
        fcst = _make_fcst_lead(
            [
                # Two base models at the no-lead sentinel horizon_value = 0.
                (STATION, 2021, stale_month, "LR_Base", 0, 83, 88, 95, 103, 111, 118, 123),
                (STATION, 2021, stale_month, "LR_SM", 0, 81, 86, 93, 101, 109, 116, 121),
                (STATION, 2022, stale_month, "LR_Base", 0, 89, 94, 101, 109, 117, 124, 129),
                (STATION, 2022, stale_month, "LR_SM", 0, 87, 92, 99, 107, 115, 122, 127),
                (STATION, 2023, stale_month, "LR_Base", 0, 100, 105, 112, 122, 130, 137, 142),
                (STATION, 2023, stale_month, "LR_SM", 0, 98, 103, 110, 120, 128, 135, 140),
                # STALE aggregate row at the calendar-month horizon_value = 8.
                (
                    STATION,
                    2021,
                    stale_month,
                    "Naive Mean",
                    stale_month,
                    82,
                    87,
                    94,
                    102,
                    110,
                    117,
                    122,
                ),
                (
                    STATION,
                    2022,
                    stale_month,
                    "Naive Mean",
                    stale_month,
                    88,
                    93,
                    100,
                    108,
                    116,
                    123,
                    128,
                ),
                (
                    STATION,
                    2023,
                    stale_month,
                    "Naive Mean",
                    stale_month,
                    99,
                    104,
                    111,
                    121,
                    129,
                    136,
                    141,
                ),
            ]
        )

        stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)

        # (1) No skill row may exist at the stale calendar-month lead.
        at_month = stats[stats["horizon_value"] == stale_month]
        assert at_month.empty, (
            "stale aggregate row at horizon_value == month must not be "
            f"scored; found stray rows:\n{at_month.to_string()}"
        )

        # (2) Regenerated aggregate skill uses the base-model lead (0).
        aggregates = stats[stats["model_short"].isin({"EM", "Naive Mean", "Skilled Mean"})]
        assert not aggregates.empty, "expected regenerated aggregate skill rows"
        assert (aggregates["horizon_value"] == 0).all(), (
            "aggregate skill must follow the base-model horizon_value "
            f"convention (0); got {sorted(aggregates['horizon_value'].unique())}"
        )
