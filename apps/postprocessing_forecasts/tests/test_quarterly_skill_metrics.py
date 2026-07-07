"""Tests for quarterly and seasonal skill metric calculation.

Phase 4b Step 3.
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.skill_metrics import (
    calculate_quarterly_skill_metrics,
    calculate_seasonal_skill_metrics,
    filter_for_highly_skilled_forecasts,
)

EXPECTED_METRIC_COLS = {
    "code",
    "model_short",
    "sdivsigma",
    "nse",
    "delta",
    "accuracy",
    "mae",
    "n_pairs",
    "crps",
    "pbias",
    "kgelf",
    "nse_log",
}


# ===================================================================
# Helper functions
# ===================================================================


def _make_quarterly_obs(rows):
    """(code, year, quarter_in_year, discharge_avg) → DataFrame with delta."""
    df = pd.DataFrame(rows, columns=["code", "year", "quarter_in_year", "discharge_avg"])
    delta_df = (
        df.groupby(["code", "quarter_in_year"])
        .agg(std_discharge=("discharge_avg", "std"))
        .reset_index()
    )
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)
    df = df.merge(
        delta_df[["code", "quarter_in_year", "delta"]],
        on=["code", "quarter_in_year"],
        how="left",
    )
    return df


def _make_quarterly_fcst(rows):
    """(code, year, quarter_in_year, model_short, q05..q95)."""
    return pd.DataFrame(
        rows,
        columns=[
            "code",
            "year",
            "quarter_in_year",
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


def _make_seasonal_obs(rows):
    """(code, season_year, discharge_avg) → DataFrame with delta, season_in_year."""
    df = pd.DataFrame(rows, columns=["code", "season_year", "discharge_avg"])
    df["season_in_year"] = 1
    delta_df = df.groupby(["code"]).agg(std_discharge=("discharge_avg", "std")).reset_index()
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)
    df = df.merge(delta_df[["code", "delta"]], on=["code"], how="left")
    return df


def _make_seasonal_fcst(rows):
    """(code, season_year, model_short, q05..q95)."""
    df = pd.DataFrame(
        rows,
        columns=[
            "code",
            "season_year",
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
    df["season_in_year"] = 1
    return df


def _make_seasonal_fcst_dated(rows):
    """(code, season_year, date, model_short, q05..q95) with season_in_year=1.

    Use when several issue dates share a (season_year, lead) so the
    per-date ensemble behaviour can be exercised.
    """
    df = pd.DataFrame(
        rows,
        columns=[
            "code",
            "season_year",
            "date",
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
    df["season_in_year"] = 1
    return df


# ===================================================================
# Quarterly skill metrics
# ===================================================================


class TestQuarterlyMetricsBasic:
    @pytest.fixture
    def basic_data(self):
        obs = _make_quarterly_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
                ("S1", 2022, 1, 100.0),
                ("S1", 2023, 1, 110.0),
                ("S1", 2024, 1, 100.0),
                ("S1", 2020, 2, 80.0),
                ("S1", 2021, 2, 85.0),
                ("S1", 2022, 2, 80.0),
                ("S1", 2023, 2, 85.0),
                ("S1", 2024, 2, 80.0),
            ]
        )
        fcst = _make_quarterly_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2022, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2023, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2024, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2020, 2, "M1", 65, 68, 75, 82, 89, 94, 98),
                ("S1", 2021, 2, "M1", 67, 70, 76, 83, 90, 95, 100),
                ("S1", 2022, 2, "M1", 65, 68, 75, 82, 89, 94, 98),
                ("S1", 2023, 2, "M1", 67, 70, 76, 83, 90, 95, 100),
                ("S1", 2024, 2, "M1", 65, 68, 75, 82, 89, 94, 98),
            ]
        )
        return obs, fcst

    def test_returns_tuple_of_three(self, basic_data):
        obs, fcst = basic_data
        result = calculate_quarterly_skill_metrics(obs, fcst)
        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_skill_stats_has_quarter_in_year(self, basic_data):
        obs, fcst = basic_data
        skill_stats, _, _ = calculate_quarterly_skill_metrics(obs, fcst)
        assert "quarter_in_year" in skill_stats.columns

    def test_skill_stats_columns(self, basic_data):
        obs, fcst = basic_data
        skill_stats, _, _ = calculate_quarterly_skill_metrics(obs, fcst)
        expected = EXPECTED_METRIC_COLS | {"quarter_in_year"}
        assert expected.issubset(set(skill_stats.columns))

    def test_one_row_per_group(self, basic_data):
        obs, fcst = basic_data
        skill_stats, _, _ = calculate_quarterly_skill_metrics(obs, fcst)
        model_rows = skill_stats[
            ~skill_stats["model_short"].isin({"Naive Mean", "EM", "Skilled Mean"})
        ]
        # 1 model * 1 station * 2 quarters = 2 rows
        assert len(model_rows) == 2

    def test_n_pairs(self, basic_data):
        obs, fcst = basic_data
        skill_stats, _, _ = calculate_quarterly_skill_metrics(obs, fcst)
        model_rows = skill_stats[skill_stats["model_short"] == "M1"]
        assert all(model_rows["n_pairs"] == 5)

    def test_crps_computed(self, basic_data):
        obs, fcst = basic_data
        skill_stats, _, _ = calculate_quarterly_skill_metrics(obs, fcst)
        model_rows = skill_stats[skill_stats["model_short"] == "M1"]
        assert all(model_rows["crps"].notna())


class TestQuarterlyMetricsEnsembles:
    @pytest.fixture
    def multi_model_data(self):
        obs = _make_quarterly_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
                ("S1", 2022, 1, 105.0),
                ("S1", 2023, 1, 110.0),
                ("S1", 2024, 1, 100.0),
            ]
        )
        # Two models with good accuracy (close to observed)
        fcst = _make_quarterly_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2022, 1, "M1", 85, 90, 97, 107, 115, 120, 127),
                ("S1", 2023, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2024, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2020, 1, "M2", 82, 87, 94, 101, 111, 117, 124),
                ("S1", 2021, 1, "M2", 90, 95, 102, 109, 117, 124, 131),
                ("S1", 2022, 1, "M2", 87, 92, 99, 106, 114, 119, 126),
                ("S1", 2023, 1, "M2", 90, 95, 102, 109, 117, 124, 131),
                ("S1", 2024, 1, "M2", 82, 87, 94, 101, 111, 117, 124),
            ]
        )
        return obs, fcst

    def test_naive_mean_created(self, multi_model_data):
        """Multi-model data should produce Naive Mean."""
        obs, fcst = multi_model_data
        skill_stats, joint, _ = calculate_quarterly_skill_metrics(obs, fcst)
        naive_rows = skill_stats[skill_stats["model_short"] == "Naive Mean"]
        assert not naive_rows.empty

    def test_naive_mean_composition(self, multi_model_data):
        obs, fcst = multi_model_data
        skill_stats, _, _ = calculate_quarterly_skill_metrics(obs, fcst)
        naive_rows = skill_stats[skill_stats["model_short"] == "Naive Mean"]
        if not naive_rows.empty:
            comp = naive_rows.iloc[0].get("composition", "")
            assert "M1" in str(comp)
            assert "M2" in str(comp)

    def test_joint_forecasts_contain_naive_mean(self, multi_model_data):
        obs, fcst = multi_model_data
        _, joint, _ = calculate_quarterly_skill_metrics(obs, fcst)
        if not joint.empty and "model_short" in joint.columns:
            models = joint["model_short"].unique()
            assert "Naive Mean" in models

    def test_em_recalc_uses_lr_mean_when_lr_skills_fail_thresholds(self, monkeypatch):
        for key, value in {
            "ieasyhydroforecast_efficiency_threshold": "0.6",
            "ieasyhydroforecast_nse_threshold": "0.8",
            "ieasyhydroforecast_accuracy_threshold": "0.8",
        }.items():
            monkeypatch.setenv(key, value)

        obs = _make_quarterly_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
                ("S1", 2022, 1, 120.0),
                ("S1", 2023, 1, 130.0),
                ("S1", 2024, 1, 140.0),
            ]
        )
        fcst = _make_quarterly_fcst(
            [
                ("S1", 2020, 1, "LR_Base", -20, -15, -5, 0, 5, 15, 20),
                ("S1", 2021, 1, "LR_Base", -20, -15, -5, 0, 5, 15, 20),
                ("S1", 2022, 1, "LR_Base", -20, -15, -5, 0, 5, 15, 20),
                ("S1", 2023, 1, "LR_Base", -20, -15, -5, 0, 5, 15, 20),
                ("S1", 2024, 1, "LR_Base", -20, -15, -5, 0, 5, 15, 20),
                ("S1", 2020, 1, "LR_SM", 180, 185, 195, 200, 205, 215, 220),
                ("S1", 2021, 1, "LR_SM", 180, 185, 195, 200, 205, 215, 220),
                ("S1", 2022, 1, "LR_SM", 180, 185, 195, 200, 205, 215, 220),
                ("S1", 2023, 1, "LR_SM", 180, 185, 195, 200, 205, 215, 220),
                ("S1", 2024, 1, "LR_SM", 180, 185, 195, 200, 205, 215, 220),
            ]
        )

        skill_stats, joint, _ = calculate_quarterly_skill_metrics(obs, fcst)
        raw_skill = skill_stats[skill_stats["model_short"].isin({"LR_Base", "LR_SM"})]
        filtered_raw = filter_for_highly_skilled_forecasts(raw_skill)
        em_joint = joint[joint["model_short"] == "EM"].sort_values("year")
        em_skill = skill_stats[skill_stats["model_short"] == "EM"]

        assert filtered_raw.empty
        assert len(em_joint) == 5
        assert np.allclose(em_joint["forecasted_discharge"], [100.0] * 5)
        assert np.allclose(em_joint["q05"], [80.0] * 5)
        assert np.allclose(em_joint["q50"], [100.0] * 5)
        assert np.allclose(em_joint["q95"], [120.0] * 5)
        assert set(em_joint["composition"]) == {"LR_Base, LR_SM"}
        # EM rows must keep their period key so the write-side NaN guard
        # (api_writer drops rows with null year/quarter_in_year) persists them.
        assert "quarter_in_year" in em_joint.columns
        assert em_joint["quarter_in_year"].notna().all()
        assert set(em_joint["quarter_in_year"].astype(int)) == {1}
        assert not em_skill.empty
        assert int(em_skill.iloc[0]["n_pairs"]) == 5
        assert pd.notna(em_skill.iloc[0]["crps"])

    def test_em_recalc_accepts_db_form_lr_model_names(self, monkeypatch):
        for key, value in {
            "ieasyhydroforecast_efficiency_threshold": "0.6",
            "ieasyhydroforecast_nse_threshold": "0.8",
            "ieasyhydroforecast_accuracy_threshold": "0.8",
        }.items():
            monkeypatch.setenv(key, value)

        obs = _make_quarterly_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
                ("S1", 2022, 1, 120.0),
                ("S1", 2023, 1, 130.0),
                ("S1", 2024, 1, 140.0),
            ]
        )
        fcst = _make_quarterly_fcst(
            [
                ("S1", 2020, 1, "LR_BASE", -20, -15, -5, 0, 5, 15, 20),
                ("S1", 2021, 1, "LR_BASE", -20, -15, -5, 0, 5, 15, 20),
                ("S1", 2022, 1, "LR_BASE", -20, -15, -5, 0, 5, 15, 20),
                ("S1", 2023, 1, "LR_BASE", -20, -15, -5, 0, 5, 15, 20),
                ("S1", 2024, 1, "LR_BASE", -20, -15, -5, 0, 5, 15, 20),
                ("S1", 2020, 1, "LR_SM", 180, 185, 195, 200, 205, 215, 220),
                ("S1", 2021, 1, "LR_SM", 180, 185, 195, 200, 205, 215, 220),
                ("S1", 2022, 1, "LR_SM", 180, 185, 195, 200, 205, 215, 220),
                ("S1", 2023, 1, "LR_SM", 180, 185, 195, 200, 205, 215, 220),
                ("S1", 2024, 1, "LR_SM", 180, 185, 195, 200, 205, 215, 220),
            ]
        )

        skill_stats, joint, _ = calculate_quarterly_skill_metrics(obs, fcst)
        em_joint = joint[joint["model_short"] == "EM"].sort_values("year")
        em_skill = skill_stats[skill_stats["model_short"] == "EM"]

        assert len(em_joint) == 5
        assert np.allclose(em_joint["forecasted_discharge"], [100.0] * 5)
        assert np.allclose(em_joint["q50"], [100.0] * 5)
        assert set(em_joint["composition"]) == {"LR_BASE, LR_SM"}
        assert not em_skill.empty

    def test_preexisting_baseline_rows_replaced_not_duplicated(self):
        """Stored EM/Naive/Skilled rows in the input are dropped, not doubled.

        The recalc regenerates the aggregated ensembles, so a source that
        already contains them must not yield two rows for the same key
        (which would violate the long_forecasts unique constraint on write).
        """
        obs = _make_quarterly_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        fcst = _make_quarterly_fcst(
            [
                ("S1", 2020, 1, "LR_Base", 80, 85, 95, 100, 105, 115, 120),
                ("S1", 2020, 1, "LR_SM", 120, 125, 135, 140, 145, 155, 160),
                ("S1", 2021, 1, "LR_Base", 80, 85, 95, 100, 105, 115, 120),
                ("S1", 2021, 1, "LR_SM", 120, 125, 135, 140, 145, 155, 160),
                # Pre-existing stored ensemble row with a stale value.
                ("S1", 2020, 1, "Naive Mean", 900, 905, 915, 999, 925, 935, 940),
            ]
        )

        _, joint, _ = calculate_quarterly_skill_metrics(obs, fcst)

        key = ["code", "year", "quarter_in_year", "model_short"]
        assert not joint.duplicated(key).any()
        nm = joint[joint["model_short"] == "Naive Mean"]
        # One recomputed Naive Mean per (year, quarter), the stale one gone.
        assert len(nm) == 2
        assert 999.0 not in set(nm["q50"])
        assert np.allclose(sorted(nm["q50"]), [120.0, 120.0])


class TestQuarterlyMetricsEdgeCases:
    def test_empty_observations(self):
        obs = pd.DataFrame(columns=["code", "year", "quarter_in_year", "discharge_avg", "delta"])
        fcst = _make_quarterly_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
            ]
        )
        skill_stats, joint, ts = calculate_quarterly_skill_metrics(obs, fcst)
        assert skill_stats.empty or len(skill_stats) == 0
        assert ts is None

    def test_empty_forecasts(self):
        obs = _make_quarterly_obs([("S1", 2020, 1, 100.0)])
        fcst = pd.DataFrame(
            columns=[
                "code",
                "year",
                "quarter_in_year",
                "model_short",
                "q05",
                "q10",
                "q25",
                "q50",
                "q75",
                "q90",
                "q95",
            ]
        )
        skill_stats, joint, ts = calculate_quarterly_skill_metrics(obs, fcst)
        assert skill_stats.empty or len(skill_stats) == 0

    def test_no_overlap(self):
        """No matching (code, year, quarter) between obs and fcst."""
        obs = _make_quarterly_obs([("S1", 2020, 1, 100.0)])
        fcst = _make_quarterly_fcst(
            [
                ("S2", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
            ]
        )
        skill_stats, _, _ = calculate_quarterly_skill_metrics(obs, fcst)
        model_rows = skill_stats[
            ~skill_stats["model_short"].isin({"Naive Mean", "EM", "Skilled Mean"})
        ]
        assert model_rows.empty


# ===================================================================
# Seasonal skill metrics
# ===================================================================


class TestSeasonalMetricsBasic:
    @pytest.fixture
    def basic_data(self):
        obs = _make_seasonal_obs(
            [
                ("S1", 2020, 100.0),
                ("S1", 2021, 110.0),
                ("S1", 2022, 100.0),
                ("S1", 2023, 110.0),
                ("S1", 2024, 100.0),
            ]
        )
        fcst = _make_seasonal_fcst(
            [
                ("S1", 2020, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2022, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2023, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2024, "M1", 80, 85, 92, 102, 112, 118, 125),
            ]
        )
        return obs, fcst

    def test_returns_tuple_of_three(self, basic_data):
        obs, fcst = basic_data
        result = calculate_seasonal_skill_metrics(obs, fcst)
        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_skill_stats_has_season_in_year(self, basic_data):
        obs, fcst = basic_data
        skill_stats, _, _ = calculate_seasonal_skill_metrics(obs, fcst)
        assert "season_in_year" in skill_stats.columns

    def test_skill_stats_columns(self, basic_data):
        obs, fcst = basic_data
        skill_stats, _, _ = calculate_seasonal_skill_metrics(obs, fcst)
        expected = EXPECTED_METRIC_COLS | {"season_in_year"}
        assert expected.issubset(set(skill_stats.columns))

    def test_n_pairs(self, basic_data):
        obs, fcst = basic_data
        skill_stats, _, _ = calculate_seasonal_skill_metrics(obs, fcst)
        model_rows = skill_stats[skill_stats["model_short"] == "M1"]
        assert all(model_rows["n_pairs"] == 5)


class TestSeasonalMetricsEnsembles:
    @pytest.fixture
    def multi_model_data(self):
        obs = _make_seasonal_obs(
            [
                ("S1", 2020, 100.0),
                ("S1", 2021, 110.0),
                ("S1", 2022, 105.0),
                ("S1", 2023, 110.0),
                ("S1", 2024, 100.0),
            ]
        )
        fcst = _make_seasonal_fcst(
            [
                ("S1", 2020, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2022, "M1", 85, 90, 97, 107, 115, 120, 127),
                ("S1", 2023, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2024, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2020, "M2", 82, 87, 94, 101, 111, 117, 124),
                ("S1", 2021, "M2", 90, 95, 102, 109, 117, 124, 131),
                ("S1", 2022, "M2", 87, 92, 99, 106, 114, 119, 126),
                ("S1", 2023, "M2", 90, 95, 102, 109, 117, 124, 131),
                ("S1", 2024, "M2", 82, 87, 94, 101, 111, 117, 124),
            ]
        )
        return obs, fcst

    def test_naive_mean_created(self, multi_model_data):
        obs, fcst = multi_model_data
        skill_stats, _, _ = calculate_seasonal_skill_metrics(obs, fcst)
        naive_rows = skill_stats[skill_stats["model_short"] == "Naive Mean"]
        assert not naive_rows.empty

    def test_em_recalc_uses_lr_mean_when_lr_skills_fail_thresholds(self, monkeypatch):
        for key, value in {
            "ieasyhydroforecast_efficiency_threshold": "0.6",
            "ieasyhydroforecast_nse_threshold": "0.8",
            "ieasyhydroforecast_accuracy_threshold": "0.8",
        }.items():
            monkeypatch.setenv(key, value)

        obs = _make_seasonal_obs(
            [
                ("S1", 2020, 100.0),
                ("S1", 2021, 110.0),
                ("S1", 2022, 120.0),
                ("S1", 2023, 130.0),
                ("S1", 2024, 140.0),
            ]
        )
        fcst = _make_seasonal_fcst(
            [
                ("S1", 2020, "LR_Base", -20, -15, -5, 0, 5, 15, 20),
                ("S1", 2021, "LR_Base", -20, -15, -5, 0, 5, 15, 20),
                ("S1", 2022, "LR_Base", -20, -15, -5, 0, 5, 15, 20),
                ("S1", 2023, "LR_Base", -20, -15, -5, 0, 5, 15, 20),
                ("S1", 2024, "LR_Base", -20, -15, -5, 0, 5, 15, 20),
                ("S1", 2020, "LR_SM", 180, 185, 195, 200, 205, 215, 220),
                ("S1", 2021, "LR_SM", 180, 185, 195, 200, 205, 215, 220),
                ("S1", 2022, "LR_SM", 180, 185, 195, 200, 205, 215, 220),
                ("S1", 2023, "LR_SM", 180, 185, 195, 200, 205, 215, 220),
                ("S1", 2024, "LR_SM", 180, 185, 195, 200, 205, 215, 220),
            ]
        )

        skill_stats, joint, _ = calculate_seasonal_skill_metrics(obs, fcst)
        raw_skill = skill_stats[skill_stats["model_short"].isin({"LR_Base", "LR_SM"})]
        filtered_raw = filter_for_highly_skilled_forecasts(raw_skill)
        em_joint = joint[joint["model_short"] == "EM"].sort_values("season_year")
        em_skill = skill_stats[skill_stats["model_short"] == "EM"]

        assert filtered_raw.empty
        assert len(em_joint) == 5
        assert np.allclose(em_joint["forecasted_discharge"], [100.0] * 5)
        assert np.allclose(em_joint["q05"], [80.0] * 5)
        assert np.allclose(em_joint["q50"], [100.0] * 5)
        assert np.allclose(em_joint["q95"], [120.0] * 5)
        assert set(em_joint["composition"]) == {"LR_Base, LR_SM"}
        # EM rows must keep their season keys so the write-side NaN guard
        # (api_writer drops rows with null season_year/season_in_year)
        # persists them.
        assert {"season_year", "season_in_year"} <= set(em_joint.columns)
        assert em_joint["season_year"].notna().all()
        assert em_joint["season_in_year"].notna().all()
        assert not em_skill.empty
        assert int(em_skill.iloc[0]["n_pairs"]) == 5
        assert pd.notna(em_skill.iloc[0]["crps"])

    def test_em_recalc_accepts_db_form_lr_model_names(self, monkeypatch):
        for key, value in {
            "ieasyhydroforecast_efficiency_threshold": "0.6",
            "ieasyhydroforecast_nse_threshold": "0.8",
            "ieasyhydroforecast_accuracy_threshold": "0.8",
        }.items():
            monkeypatch.setenv(key, value)

        obs = _make_seasonal_obs(
            [
                ("S1", 2020, 100.0),
                ("S1", 2021, 110.0),
                ("S1", 2022, 120.0),
                ("S1", 2023, 130.0),
                ("S1", 2024, 140.0),
            ]
        )
        fcst = _make_seasonal_fcst(
            [
                ("S1", 2020, "LR_BASE", -20, -15, -5, 0, 5, 15, 20),
                ("S1", 2021, "LR_BASE", -20, -15, -5, 0, 5, 15, 20),
                ("S1", 2022, "LR_BASE", -20, -15, -5, 0, 5, 15, 20),
                ("S1", 2023, "LR_BASE", -20, -15, -5, 0, 5, 15, 20),
                ("S1", 2024, "LR_BASE", -20, -15, -5, 0, 5, 15, 20),
                ("S1", 2020, "LR_SM", 180, 185, 195, 200, 205, 215, 220),
                ("S1", 2021, "LR_SM", 180, 185, 195, 200, 205, 215, 220),
                ("S1", 2022, "LR_SM", 180, 185, 195, 200, 205, 215, 220),
                ("S1", 2023, "LR_SM", 180, 185, 195, 200, 205, 215, 220),
                ("S1", 2024, "LR_SM", 180, 185, 195, 200, 205, 215, 220),
            ]
        )

        skill_stats, joint, _ = calculate_seasonal_skill_metrics(obs, fcst)
        em_joint = joint[joint["model_short"] == "EM"].sort_values("season_year")
        em_skill = skill_stats[skill_stats["model_short"] == "EM"]

        assert len(em_joint) == 5
        assert np.allclose(em_joint["forecasted_discharge"], [100.0] * 5)
        assert np.allclose(em_joint["q50"], [100.0] * 5)
        assert set(em_joint["composition"]) == {"LR_BASE, LR_SM"}
        assert not em_skill.empty

    def test_em_per_issue_date_not_collapsed(self):
        """Each seasonal issue date yields its own clean 2-model EM.

        When a (season_year, lead) is (re-)issued on several dates, the EM
        must be computed per issue date — mean(LR_Base, LR_SM) for that
        date — rather than averaged across dates into one blended row.
        """
        obs = _make_seasonal_obs(
            [
                ("S1", 2020, 100.0),
                ("S1", 2021, 110.0),
            ]
        )
        # Two issue dates per season_year, same lead. Per-date EM q50 is
        # 150 (=(100+200)/2) for date d1 and 130 (=(120+140)/2) for date d2.
        # A date-collapsing EM would instead produce a single 140 per year.
        fcst = _make_seasonal_fcst_dated(
            [
                ("S1", 2020, "2020-03-25", "LR_Base", 80, 85, 95, 100, 105, 115, 120),
                ("S1", 2020, "2020-03-25", "LR_SM", 180, 185, 195, 200, 205, 215, 220),
                ("S1", 2020, "2020-04-01", "LR_Base", 100, 105, 115, 120, 125, 135, 140),
                ("S1", 2020, "2020-04-01", "LR_SM", 120, 125, 135, 140, 145, 155, 160),
                ("S1", 2021, "2021-03-25", "LR_Base", 80, 85, 95, 100, 105, 115, 120),
                ("S1", 2021, "2021-03-25", "LR_SM", 180, 185, 195, 200, 205, 215, 220),
                ("S1", 2021, "2021-04-01", "LR_Base", 100, 105, 115, 120, 125, 135, 140),
                ("S1", 2021, "2021-04-01", "LR_SM", 120, 125, 135, 140, 145, 155, 160),
            ]
        )

        _, joint, _ = calculate_seasonal_skill_metrics(obs, fcst)
        em = joint[joint["model_short"] == "EM"].copy()

        # One EM row per issue date (2 years x 2 dates), not one per year.
        assert len(em) == 4
        assert "date" in em.columns
        assert em["date"].notna().all()
        # Each EM is the clean 2-model mean for its own date.
        by_date = em.groupby("date")["q50"].first().to_dict()
        assert by_date["2020-03-25"] == pytest.approx(150.0)
        assert by_date["2020-04-01"] == pytest.approx(130.0)
        assert by_date["2021-03-25"] == pytest.approx(150.0)
        assert by_date["2021-04-01"] == pytest.approx(130.0)
        # forecasted_discharge (point) tracks the same per-date mean.
        fd = em.groupby("date")["forecasted_discharge"].first().to_dict()
        assert fd["2020-03-25"] == pytest.approx(150.0)
        assert fd["2020-04-01"] == pytest.approx(130.0)
        assert set(em["composition"]) == {"LR_Base, LR_SM"}


class TestQFallbackQuarterly:
    """Quarterly skill metrics when q50 column is absent."""

    def test_q50_column_absent_produces_metrics(self):
        """When q50 is absent and q is present, quarterly metrics compute."""
        obs = _make_quarterly_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
                ("S1", 2022, 1, 100.0),
                ("S1", 2023, 1, 110.0),
                ("S1", 2024, 1, 100.0),
            ]
        )
        # No q50 column — only q
        fcst = pd.DataFrame(
            {
                "code": ["S1"] * 5,
                "year": [2020, 2021, 2022, 2023, 2024],
                "quarter_in_year": [1] * 5,
                "model_short": ["GBT"] * 5,
                "q": [95.0, 105.0, 95.0, 105.0, 95.0],
                "forecasted_discharge": [95.0, 105.0, 95.0, 105.0, 95.0],
                "q05": [np.nan] * 5,
                "q10": [np.nan] * 5,
                "q25": [np.nan] * 5,
                "q75": [np.nan] * 5,
                "q90": [np.nan] * 5,
                "q95": [np.nan] * 5,
            }
        )
        stats, _, _ = calculate_quarterly_skill_metrics(obs, fcst)
        gbt_stats = stats[stats["model_short"] == "GBT"]
        assert not gbt_stats.empty, "GBT should have quarterly skill metrics"
        assert gbt_stats.iloc[0]["n_pairs"] > 0, "n_pairs should be > 0"

    def test_q50_absent_no_forecasted_discharge_resolves_from_q(self):
        """When q50 and forecasted_discharge are absent, q is used as fallback."""
        obs = _make_quarterly_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
                ("S1", 2022, 1, 100.0),
                ("S1", 2023, 1, 110.0),
                ("S1", 2024, 1, 100.0),
            ]
        )
        # No q50, no forecasted_discharge — only q
        fcst = pd.DataFrame(
            {
                "code": ["S1"] * 5,
                "year": [2020, 2021, 2022, 2023, 2024],
                "quarter_in_year": [1] * 5,
                "model_short": ["GBT"] * 5,
                "q": [95.0, 105.0, 95.0, 105.0, 95.0],
                "q05": [np.nan] * 5,
                "q10": [np.nan] * 5,
                "q25": [np.nan] * 5,
                "q75": [np.nan] * 5,
                "q90": [np.nan] * 5,
                "q95": [np.nan] * 5,
            }
        )
        stats, _, _ = calculate_quarterly_skill_metrics(obs, fcst)
        gbt_stats = stats[stats["model_short"] == "GBT"]
        assert not gbt_stats.empty, "GBT should have quarterly skill metrics"
        assert gbt_stats.iloc[0]["n_pairs"] > 0, "n_pairs should be > 0"
        assert pd.notna(gbt_stats.iloc[0]["mae"]), "MAE should be computed"

    def test_q50_absent_no_q_no_forecasted_discharge_returns_empty(self):
        """When no q, q50, or forecasted_discharge exist, return empty gracefully."""
        obs = _make_quarterly_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        fcst = pd.DataFrame(
            {
                "code": ["S1", "S1"],
                "year": [2020, 2021],
                "quarter_in_year": [1, 1],
                "model_short": ["GBT", "GBT"],
            }
        )
        stats, _, _ = calculate_quarterly_skill_metrics(obs, fcst)
        # Should return empty stats, not crash
        gbt_stats = stats[stats["model_short"] == "GBT"]
        assert gbt_stats.empty or gbt_stats.iloc[0]["n_pairs"] == 0
