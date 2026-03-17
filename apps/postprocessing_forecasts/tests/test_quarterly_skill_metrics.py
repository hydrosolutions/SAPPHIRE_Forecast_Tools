"""Tests for quarterly and seasonal skill metric calculation.

Phase 4b Step 3.
"""

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.skill_metrics import (
    calculate_quarterly_skill_metrics,
    calculate_seasonal_skill_metrics,
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
                ("S1", 2020, 2, 80.0),
                ("S1", 2021, 2, 85.0),
            ]
        )
        fcst = _make_quarterly_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2020, 2, "M1", 65, 68, 75, 82, 89, 94, 98),
                ("S1", 2021, 2, "M1", 67, 70, 76, 83, 90, 95, 100),
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
        assert all(model_rows["n_pairs"] == 2)

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
            ]
        )
        # Two models with good accuracy (close to observed)
        fcst = _make_quarterly_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2022, 1, "M1", 85, 90, 97, 107, 115, 120, 127),
                ("S1", 2020, 1, "M2", 82, 87, 94, 101, 111, 117, 124),
                ("S1", 2021, 1, "M2", 90, 95, 102, 109, 117, 124, 131),
                ("S1", 2022, 1, "M2", 87, 92, 99, 106, 114, 119, 126),
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
            ]
        )
        fcst = _make_seasonal_fcst(
            [
                ("S1", 2020, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, "M1", 88, 93, 100, 108, 116, 123, 130),
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
        assert all(model_rows["n_pairs"] == 2)


class TestSeasonalMetricsEnsembles:
    @pytest.fixture
    def multi_model_data(self):
        obs = _make_seasonal_obs(
            [
                ("S1", 2020, 100.0),
                ("S1", 2021, 110.0),
                ("S1", 2022, 105.0),
            ]
        )
        fcst = _make_seasonal_fcst(
            [
                ("S1", 2020, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2022, "M1", 85, 90, 97, 107, 115, 120, 127),
                ("S1", 2020, "M2", 82, 87, 94, 101, 111, 117, 124),
                ("S1", 2021, "M2", 90, 95, 102, 109, 117, 124, 131),
                ("S1", 2022, "M2", 87, 92, 99, 106, 114, 119, 126),
            ]
        )
        return obs, fcst

    def test_naive_mean_created(self, multi_model_data):
        obs, fcst = multi_model_data
        skill_stats, _, _ = calculate_seasonal_skill_metrics(obs, fcst)
        naive_rows = skill_stats[skill_stats["model_short"] == "Naive Mean"]
        assert not naive_rows.empty
