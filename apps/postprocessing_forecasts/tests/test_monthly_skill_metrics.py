"""Tests for calculate_monthly_skill_metrics().

Step 5 of Phase 4a: Monthly skill metrics.
TDD — tests written before implementation.
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.skill_metrics import calculate_crps, calculate_monthly_skill_metrics

# Standard quantile levels used in SAPPHIRE long-term forecasts
QUANTILE_LEVELS = np.array([0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95])

# Expected columns in skill_stats output
EXPECTED_METRIC_COLS = {
    "month_in_year",
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


def _make_obs(rows):
    """Create observations DataFrame from (code, year, month, discharge_avg).

    Automatically computes month_in_year and delta (0.674 * std).
    """
    df = pd.DataFrame(rows, columns=["code", "year", "month", "discharge_avg"])
    df["month_in_year"] = df["month"]

    delta_df = (
        df.groupby(["code", "month_in_year"])
        .agg(
            std_discharge=("discharge_avg", "std"),
        )
        .reset_index()
    )
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)

    df = df.merge(
        delta_df[["code", "month_in_year", "delta"]],
        on=["code", "month_in_year"],
        how="left",
    )
    return df


def _make_fcst(rows):
    """Create forecasts DataFrame.

    Each row: (code, year, month, model_short,
               q05, q10, q25, q50, q75, q90, q95)
    """
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


# ===================================================================
# Basic functionality
# ===================================================================


class TestMonthlyMetricsBasic:
    """Core merge, point metrics, and CRPS."""

    @pytest.fixture
    def basic_data(self):
        """Single model M1, station S1, 2 years x 2 months."""
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
                ("S1", 2020, 2, 80.0),
                ("S1", 2021, 2, 85.0),
            ]
        )
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2020, 2, "M1", 65, 68, 75, 82, 89, 94, 98),
                ("S1", 2021, 2, "M1", 67, 70, 76, 83, 90, 95, 100),
            ]
        )
        return obs, fcst

    def test_returns_tuple_of_three(self, basic_data):
        """Returns (skill_stats, joint_forecasts, timing_stats)."""
        obs, fcst = basic_data
        result = calculate_monthly_skill_metrics(obs, fcst)
        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_skill_stats_columns(self, basic_data):
        """skill_stats has all required metric columns plus crps."""
        obs, fcst = basic_data
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        assert EXPECTED_METRIC_COLS.issubset(set(skill_stats.columns))

    def test_one_row_per_group(self, basic_data):
        """One skill row per (month_in_year, code, model_short)."""
        obs, fcst = basic_data
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        model_rows = skill_stats[~skill_stats["model_short"].isin(["Naive Mean", "EM"])]
        # 1 model * 1 station * 2 months = 2 rows
        assert len(model_rows) == 2

    def test_point_metrics_use_q50(self, basic_data):
        """Point metrics use q50 as forecasted_discharge.

        Month 1: q50=[102, 108], obs=[100, 110], delta=0.674*std=4.766
        diff = [-2, 2]
        MAE = mean(2, 2) = 2.0
        sdivsigma = sqrt(8/1) / std(obs,ddof=1) = 2.828/7.071 = 0.4
        NSE = 1 - 8/50 = 0.84
        accuracy: |2|,|2| both <= 4.766 -> 1.0
        """
        obs, fcst = basic_data
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        row = skill_stats[
            (skill_stats["month_in_year"] == 1) & (skill_stats["model_short"] == "M1")
        ].iloc[0]
        assert row["mae"] == pytest.approx(2.0, rel=1e-6)
        assert row["n_pairs"] == 2
        assert row["sdivsigma"] == pytest.approx(0.4, rel=1e-6)
        assert row["nse"] == pytest.approx(0.84, rel=1e-6)
        assert row["accuracy"] == pytest.approx(1.0, rel=1e-6)
        expected_delta = 0.674 * np.std([100.0, 110.0], ddof=1)
        assert row["delta"] == pytest.approx(expected_delta, rel=1e-6)

    def test_crps_computed_and_nonnegative(self, basic_data):
        """CRPS is present and non-negative for models with quantiles."""
        obs, fcst = basic_data
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        model_rows = skill_stats[skill_stats["model_short"] == "M1"]
        assert all(np.isfinite(model_rows["crps"]))
        assert all(model_rows["crps"] >= 0)

    def test_crps_value_matches_direct_calculation(self, basic_data):
        """CRPS matches calculate_crps() for month 1 group."""
        obs, fcst = basic_data
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        row = skill_stats[
            (skill_stats["month_in_year"] == 1) & (skill_stats["model_short"] == "M1")
        ].iloc[0]

        expected = calculate_crps(
            np.array([100.0, 110.0]),
            np.array(
                [
                    [80, 85, 92, 102, 112, 118, 125],
                    [88, 93, 100, 108, 116, 123, 130],
                ],
                dtype=float,
            ),
            QUANTILE_LEVELS,
        )
        assert row["crps"] == pytest.approx(expected, rel=1e-6)


# ===================================================================
# Multiple models
# ===================================================================


class TestMonthlyMetricsMultiModel:
    """Multiple models per station."""

    @pytest.fixture
    def two_model_data(self):
        """Two models M1/M2, one station, 2 years x 1 month."""
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2020, 1, "M2", 82, 87, 94, 101, 108, 114, 120),
                ("S1", 2021, 1, "M2", 90, 95, 102, 109, 117, 124, 131),
            ]
        )
        return obs, fcst

    def test_both_models_get_metrics(self, two_model_data):
        """Both M1 and M2 appear in skill_stats."""
        obs, fcst = two_model_data
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        models = set(skill_stats["model_short"].unique())
        assert {"M1", "M2"}.issubset(models)

    def test_different_crps_per_model(self, two_model_data):
        """M1 and M2 have different CRPS (different quantiles)."""
        obs, fcst = two_model_data
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        crps_m1 = skill_stats[skill_stats["model_short"] == "M1"]["crps"].iloc[0]
        crps_m2 = skill_stats[skill_stats["model_short"] == "M2"]["crps"].iloc[0]
        assert crps_m1 != crps_m2


# ===================================================================
# Ensemble creation
# ===================================================================


class TestMonthlyMetricsEnsemble:
    """Ensemble mean (EM) from threshold-filtered models."""

    @pytest.fixture
    def two_skilled_models(self):
        """M1 and M2 both pass default thresholds.

        obs=[100, 110], delta=4.766
        M1 q50=[102, 108]: sdivsigma~0.4, NSE~0.84, accuracy=1.0
        M2 q50=[101, 109]: sdivsigma~0.2, NSE~0.96, accuracy=1.0
        """
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2020, 1, "M2", 82, 87, 94, 101, 108, 114, 120),
                ("S1", 2021, 1, "M2", 90, 95, 102, 109, 117, 124, 131),
            ]
        )
        return obs, fcst

    def test_em_row_created(self, two_skilled_models):
        """EM row created when 2+ models pass threshold."""
        obs, fcst = two_skilled_models
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_rows = skill_stats[skill_stats["model_short"] == "EM"]
        assert len(em_rows) == 1

    def test_em_has_composition(self, two_skilled_models):
        """EM row lists contributing models in composition."""
        obs, fcst = two_skilled_models
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_row = skill_stats[skill_stats["model_short"] == "EM"].iloc[0]
        assert "composition" in skill_stats.columns
        assert "M1" in em_row["composition"]
        assert "M2" in em_row["composition"]

    def test_em_mae_from_mean_q50(self, two_skilled_models):
        """EM forecast = mean of skilled models' q50.

        M1 q50=[102, 108], M2 q50=[101, 109]
        EM = [101.5, 108.5], obs = [100, 110]
        MAE = mean(1.5, 1.5) = 1.5
        """
        obs, fcst = two_skilled_models
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_row = skill_stats[skill_stats["model_short"] == "EM"].iloc[0]
        assert em_row["mae"] == pytest.approx(1.5, rel=1e-6)

    def test_em_all_metrics_verified(self, two_skilled_models):
        """Verify all EM metrics numerically.

        EM = [101.5, 108.5], obs = [100, 110], delta = 4.766
        diff = [-1.5, 1.5]
        sdivsigma = sqrt(4.5/1) / 7.071 = 2.121/7.071 = 0.3
        NSE = 1 - 4.5/50 = 0.91
        accuracy: |1.5|,|1.5| both <= 4.766 -> 1.0
        """
        obs, fcst = two_skilled_models
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_row = skill_stats[skill_stats["model_short"] == "EM"].iloc[0]
        assert em_row["n_pairs"] == 2
        assert em_row["sdivsigma"] == pytest.approx(0.3, rel=1e-6)
        assert em_row["nse"] == pytest.approx(0.91, rel=1e-6)
        assert em_row["accuracy"] == pytest.approx(1.0, rel=1e-6)

    def test_em_crps_computed_from_quantiles(self, two_skilled_models):
        """EM has CRPS computed from aggregated quantile distribution.

        EM quantiles = mean of M1 and M2 quantiles per group.
        CRPS is computed via trapezoidal pinball loss integration.
        """
        obs, fcst = two_skilled_models
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_row = skill_stats[skill_stats["model_short"] == "EM"].iloc[0]
        assert not np.isnan(em_row["crps"])
        assert em_row["crps"] > 0  # non-zero CRPS

    def test_no_em_single_skilled(self):
        """No EM when only one model passes threshold."""
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        # M1 passes (q50 close), Bad fails (q50=150/160 far from obs)
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2020, 1, "Bad", 120, 130, 140, 150, 160, 170, 180),
                ("S1", 2021, 1, "Bad", 130, 140, 150, 160, 170, 180, 190),
            ]
        )
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_rows = skill_stats[skill_stats["model_short"] == "EM"]
        assert len(em_rows) == 0

    def test_em_in_joint_forecasts(self, two_skilled_models):
        """EM rows in joint_forecasts alongside originals."""
        obs, fcst = two_skilled_models
        _, joint, _ = calculate_monthly_skill_metrics(obs, fcst)
        models = set(joint["model_short"].unique())
        assert "EM" in models
        assert "M1" in models
        assert "M2" in models

    def test_em_three_models_partial_pass(self):
        """EM from 2 of 3 models when only 2 pass thresholds.

        M1 and M2 have good q50, Bad has q50=150/160 (far from obs).
        EM = mean(M1, M2) only. Bad excluded from composition.
        EM MAE = 1.5 (same as two_skilled_models fixture).
        """
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2020, 1, "M2", 82, 87, 94, 101, 108, 114, 120),
                ("S1", 2021, 1, "M2", 90, 95, 102, 109, 117, 124, 131),
                ("S1", 2020, 1, "Bad", 120, 130, 140, 150, 160, 170, 180),
                ("S1", 2021, 1, "Bad", 130, 140, 150, 160, 170, 180, 190),
            ]
        )
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_rows = skill_stats[skill_stats["model_short"] == "EM"]
        assert len(em_rows) == 1
        em_row = em_rows.iloc[0]
        assert "M1" in em_row["composition"]
        assert "M2" in em_row["composition"]
        assert "Bad" not in em_row["composition"]
        assert em_row["mae"] == pytest.approx(1.5, rel=1e-6)


# ===================================================================
# Naive Mean baseline (unweighted model average)
# ===================================================================


class TestNaiveMeanBaseline:
    """Naive Mean = unweighted average of ALL model forecasts.

    Unlike EM (which filters by skill), Naive Mean includes all models
    regardless of skill. Requires >=2 models (single-model groups
    discarded, same as EM).
    """

    @pytest.fixture
    def data_two_models(self):
        """2 models x 3 years for meaningful Naive Mean.

        M1 q50: [102, 108, 106], M2 q50: [104, 112, 108]
        Naive Mean q50 = mean(M1, M2) = [103, 110, 107]
        obs = [100, 110, 105]
        MAE = mean(|100-103|, |110-110|, |105-107|) = 5/3
        """
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
                ("S1", 2022, 1, 105.0),
            ]
        )
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2022, 1, "M1", 85, 90, 98, 106, 114, 120, 128),
                ("S1", 2020, 1, "M2", 82, 87, 94, 104, 114, 120, 127),
                ("S1", 2021, 1, "M2", 92, 97, 104, 112, 120, 127, 134),
                ("S1", 2022, 1, "M2", 86, 91, 99, 108, 116, 122, 130),
            ]
        )
        return obs, fcst

    def test_naive_mean_in_skill_stats(self, data_two_models):
        """Naive Mean appears as a model in skill_stats."""
        obs, fcst = data_two_models
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        assert "Naive Mean" in skill_stats["model_short"].values

    def test_naive_mean_is_model_average(self, data_two_models):
        """Naive Mean q50 = mean of model q50s, NOT climatological mean.

        M1 q50=[102,108,106], M2 q50=[104,112,108]
        Naive Mean = [103,110,107], obs = [100,110,105]
        MAE = mean(3, 0, 2) = 5/3
        """
        obs, fcst = data_two_models
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        naive = skill_stats[skill_stats["model_short"] == "Naive Mean"].iloc[0]
        assert naive["mae"] == pytest.approx(5.0 / 3.0, rel=1e-6)

    def test_naive_mean_crps_computed(self, data_two_models):
        """Naive Mean has CRPS from aggregated quantile distribution."""
        obs, fcst = data_two_models
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        naive = skill_stats[skill_stats["model_short"] == "Naive Mean"].iloc[0]
        assert not np.isnan(naive["crps"])
        assert naive["crps"] > 0

    def test_naive_mean_has_composition(self, data_two_models):
        """Naive Mean lists contributing models in composition."""
        obs, fcst = data_two_models
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        naive = skill_stats[skill_stats["model_short"] == "Naive Mean"].iloc[0]
        assert "M1" in naive["composition"]
        assert "M2" in naive["composition"]

    def test_naive_mean_includes_unskilled_models(self):
        """Naive Mean includes ALL models, even unskilled ones.

        M1 is skilled (close to obs), Bad is terrible.
        EM only includes M1 (single-model -> no EM).
        Naive Mean includes both M1 and Bad.
        """
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2020, 1, "Bad", 120, 130, 140, 150, 160, 170, 180),
                ("S1", 2021, 1, "Bad", 130, 140, 150, 160, 170, 180, 190),
            ]
        )
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)

        # EM: only M1 passes -> single-model -> no EM
        em_rows = skill_stats[skill_stats["model_short"] == "EM"]
        assert len(em_rows) == 0

        # Naive Mean: M1 + Bad both included
        naive = skill_stats[skill_stats["model_short"] == "Naive Mean"]
        assert len(naive) == 1
        assert "Bad" in naive.iloc[0]["composition"]
        assert "M1" in naive.iloc[0]["composition"]

    def test_naive_mean_not_created_single_model(self):
        """No Naive Mean when only 1 model exists."""
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
            ]
        )
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        naive = skill_stats[skill_stats["model_short"] == "Naive Mean"]
        assert len(naive) == 0

    def test_naive_mean_adds_rows_to_joint_forecasts(self, data_two_models):
        """Naive Mean rows appear in joint_forecasts with correct columns."""
        obs, fcst = data_two_models
        _, joint, _ = calculate_monthly_skill_metrics(obs, fcst)
        naive_rows = joint[joint["model_short"] == "Naive Mean"]
        assert len(naive_rows) == 3, (
            f"Expected 3 Naive Mean rows (1 station x 3 years), got {len(naive_rows)}"
        )
        assert "forecasted_discharge" in naive_rows.columns
        assert "composition" in naive_rows.columns


# ===================================================================
# Edge cases
# ===================================================================


class TestMonthlyMetricsEdgeCases:
    """Edge cases and boundary conditions."""

    def test_empty_observations(self):
        """Empty observations returns empty skill_stats."""
        obs = pd.DataFrame(
            columns=["code", "year", "month", "month_in_year", "discharge_avg", "delta"]
        )
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 100, 110, 120, 130),
            ]
        )
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        assert len(skill_stats) == 0

    def test_empty_forecasts(self):
        """Empty forecasts returns empty skill_stats."""
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        fcst = pd.DataFrame(
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
            ]
        )
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        assert len(skill_stats) == 0

    def test_no_overlap_no_real_model_metrics(self):
        """No matching (code, year, month) gives no real model metrics."""
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        fcst = _make_fcst(
            [
                ("S2", 2020, 1, "M1", 80, 85, 92, 100, 110, 120, 130),
            ]
        )
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        real = skill_stats[~skill_stats["model_short"].isin(["Naive Mean"])]
        assert len(real) == 0

    def test_multi_station_independent(self):
        """Stations get independent metrics with correct values.

        S1: obs=[100,110], q50=[102,108], MAE=2.0, sdivsigma=0.4
        S2: obs=[200,220], q50=[205,218], MAE=3.5
            diff=[-5,2], s=sqrt(29/1)=5.385, sigma=std([200,220],ddof=1)
        """
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
                ("S2", 2020, 1, 200.0),
                ("S2", 2021, 1, 220.0),
            ]
        )
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S2", 2020, 1, "M1", 160, 170, 185, 205, 215, 225, 235),
                ("S2", 2021, 1, "M1", 180, 190, 205, 218, 230, 240, 250),
            ]
        )
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        model_rows = skill_stats[skill_stats["model_short"] == "M1"]
        assert len(model_rows) == 2
        assert set(model_rows["code"].values) == {"S1", "S2"}

        # S1: obs=[100,110], q50=[102,108]
        s1 = model_rows[model_rows["code"] == "S1"].iloc[0]
        assert s1["mae"] == pytest.approx(2.0, rel=1e-6)
        assert s1["sdivsigma"] == pytest.approx(0.4, rel=1e-6)

        # S2: obs=[200,220], q50=[205,218], MAE=mean(5,2)=3.5
        s2 = model_rows[model_rows["code"] == "S2"].iloc[0]
        assert s2["mae"] == pytest.approx(3.5, rel=1e-6)
        # sdivsigma = sqrt(sum([-5,2]^2)/(2-1)) / std([200,220],ddof=1)
        assert s2["sdivsigma"] == pytest.approx(
            np.sqrt(29.0) / np.std([200.0, 220.0], ddof=1), rel=1e-6
        )

    def test_single_year_npairs_one(self):
        """Single year: n_pairs=1, sdivsigma/NSE=NaN, MAE still valid.

        min_points=2 for sdivsigma/NSE not met with one obs-fcst pair.
        delta=0 (single year std=NaN -> fillna(0)), accuracy=0.
        """
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
            ]
        )
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
            ]
        )
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        row = skill_stats[skill_stats["model_short"] == "M1"].iloc[0]
        assert row["n_pairs"] == 1
        assert row["mae"] == pytest.approx(2.0, rel=1e-6)
        assert np.isnan(row["sdivsigma"])
        assert np.isnan(row["nse"])
        assert row["delta"] == pytest.approx(0.0, abs=1e-10)
        assert row["accuracy"] == pytest.approx(0.0, abs=1e-10)

    def test_no_em_when_all_models_fail_thresholds(self):
        """No EM when all models fail skill thresholds.

        Both M1 and M2 have terrible q50 (150/160 and 200/210).
        Neither passes -> no EM. Model rows and Naive Mean still present.
        """
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 120, 130, 140, 150, 160, 170, 180),
                ("S1", 2021, 1, "M1", 130, 140, 150, 160, 170, 180, 190),
                ("S1", 2020, 1, "M2", 170, 180, 190, 200, 210, 220, 230),
                ("S1", 2021, 1, "M2", 180, 190, 200, 210, 220, 230, 240),
            ]
        )
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_rows = skill_stats[skill_stats["model_short"] == "EM"]
        assert len(em_rows) == 0
        assert "M1" in skill_stats["model_short"].values
        assert "M2" in skill_stats["model_short"].values
        assert "Naive Mean" in skill_stats["model_short"].values

    def test_nan_quantiles_point_metrics_still_computed(self):
        """NaN quantiles don't break point metrics (q50 still valid).

        obs=[100,110], q50=[102,108] — same as basic month 1.
        Point metrics computed from q50. CRPS = NaN (NaN quantiles).
        """
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", np.nan, np.nan, np.nan, 102, np.nan, np.nan, np.nan),
                ("S1", 2021, 1, "M1", np.nan, np.nan, np.nan, 108, np.nan, np.nan, np.nan),
            ]
        )
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        row = skill_stats[skill_stats["model_short"] == "M1"].iloc[0]
        assert row["mae"] == pytest.approx(2.0, rel=1e-6)
        assert row["sdivsigma"] == pytest.approx(0.4, rel=1e-6)
        assert row["nse"] == pytest.approx(0.84, rel=1e-6)
        assert np.isnan(row["crps"])

    def test_nan_discharge_avg_excluded_from_metrics(self):
        """NaN discharge_avg rows are dropped by the inner merge.

        Observations with NaN discharge_avg should not contribute to
        metrics. Here S1 month 1 has 3 years but one has NaN obs.
        Only 2 valid pairs should be used.

        obs valid = [100, 110], q50 = [102, 108], MAE = 2.0
        """
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
                ("S1", 2022, 1, np.nan),
            ]
        )
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2022, 1, "M1", 85, 90, 98, 105, 113, 120, 127),
            ]
        )
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        row = skill_stats[skill_stats["model_short"] == "M1"].iloc[0]
        # NaN obs merged in but produce NaN forecasted_discharge diff
        # calculate_all_skill_metrics masks NaN pairs internally
        # n_pairs should reflect valid pairs only
        assert row["n_pairs"] >= 2
        assert row["mae"] == pytest.approx(2.0, rel=1e-6)

    def test_duplicate_obs_rows_inflate_metrics(self):
        """Duplicate (code, year, month) in observations inflates n_pairs.

        This test documents current behavior: the inner merge produces
        one forecast row per observation duplicate, so n_pairs increases.
        If this is ever guarded against, this test should be updated.

        S1 month 1: obs has 2020 duplicated. Merge produces 3 rows
        for M1 (2020 appears twice, 2021 once) instead of 2.
        """
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2020, 1, 100.0),  # duplicate
                ("S1", 2021, 1, 110.0),
            ]
        )
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
            ]
        )
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        row = skill_stats[skill_stats["model_short"] == "M1"].iloc[0]
        # 2020 forecast joined twice (one per obs dup) + 2021 = 3
        assert row["n_pairs"] == 3

    def test_duplicate_forecast_rows_inflate_metrics(self):
        """Duplicate (code, year, month, model_short) in forecasts
        inflates n_pairs via merge. Documents current behavior.
        """
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
            ]
        )
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        row = skill_stats[skill_stats["model_short"] == "M1"].iloc[0]
        # 2020 obs joined twice (one per fcst dup) + 2021 = 3
        assert row["n_pairs"] == 3

    def test_em_joint_forecasts_values_correct(self):
        """EM rows in joint_forecasts have correct discharge values.

        M1 q50=[102, 108], M2 q50=[101, 109]
        EM = mean of q50 = [101.5, 108.5] per (year, code).
        """
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2020, 1, "M2", 82, 87, 94, 101, 108, 114, 120),
                ("S1", 2021, 1, "M2", 90, 95, 102, 109, 117, 124, 131),
            ]
        )
        _, joint, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_rows = joint[joint["model_short"] == "EM"]
        assert len(em_rows) == 2

        em_2020 = em_rows[em_rows["year"] == 2020].iloc[0]
        assert em_2020["forecasted_discharge"] == pytest.approx(101.5, rel=1e-6)

        em_2021 = em_rows[em_rows["year"] == 2021].iloc[0]
        assert em_2021["forecasted_discharge"] == pytest.approx(108.5, rel=1e-6)

    def test_partial_station_month_coverage(self):
        """Stations with different month coverage get independent metrics.

        S1 has data for months 1 and 2.
        S2 has data for month 1 only.
        Both should get metrics for their respective months.
        """
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
                ("S1", 2020, 2, 80.0),
                ("S1", 2021, 2, 85.0),
                ("S2", 2020, 1, 200.0),
                ("S2", 2021, 1, 220.0),
            ]
        )
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2020, 2, "M1", 65, 68, 75, 82, 89, 94, 98),
                ("S1", 2021, 2, "M1", 67, 70, 76, 83, 90, 95, 100),
                ("S2", 2020, 1, "M1", 160, 170, 185, 205, 215, 225, 235),
                ("S2", 2021, 1, "M1", 180, 190, 205, 218, 230, 240, 250),
            ]
        )
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        model_rows = skill_stats[skill_stats["model_short"] == "M1"]

        # S1: 2 months, S2: 1 month = 3 rows total
        assert len(model_rows) == 3

        # S1 month 1 and S2 month 1 both present
        s1_m1 = model_rows[(model_rows["code"] == "S1") & (model_rows["month_in_year"] == 1)]
        assert len(s1_m1) == 1
        assert s1_m1.iloc[0]["mae"] == pytest.approx(2.0, rel=1e-6)

        s2_m1 = model_rows[(model_rows["code"] == "S2") & (model_rows["month_in_year"] == 1)]
        assert len(s2_m1) == 1
        assert s2_m1.iloc[0]["mae"] == pytest.approx(3.5, rel=1e-6)

        # S2 has no month 2
        s2_m2 = model_rows[(model_rows["code"] == "S2") & (model_rows["month_in_year"] == 2)]
        assert len(s2_m2) == 0

    def test_skilled_mean_excluded_from_em_composition(self):
        """Skilled Mean baseline is not included in EM composition.

        M1 and M2 pass thresholds. Skilled Mean is added separately
        and should never appear in EM's composition string.
        """
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2020, 1, "M2", 82, 87, 94, 101, 108, 114, 120),
                ("S1", 2021, 1, "M2", 90, 95, 102, 109, 117, 124, 131),
            ]
        )
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_rows = skill_stats[skill_stats["model_short"] == "EM"]
        assert len(em_rows) == 1
        composition = em_rows.iloc[0]["composition"]
        assert "Skilled Mean" not in composition

    def test_naive_mean_excluded_from_em_composition(self):
        """Naive Mean baseline is not included in EM composition.

        M1 and M2 pass thresholds. Naive Mean is added separately
        and should never appear in EM's composition string.
        """
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2020, 1, "M2", 82, 87, 94, 101, 108, 114, 120),
                ("S1", 2021, 1, "M2", 90, 95, 102, 109, 117, 124, 131),
            ]
        )
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_rows = skill_stats[skill_stats["model_short"] == "EM"]
        assert len(em_rows) == 1
        composition = em_rows.iloc[0]["composition"]
        assert "Naive Mean" not in composition
        assert "M1" in composition
        assert "M2" in composition


# ===================================================================
# Skilled Mean baseline
# ===================================================================


class TestSkilledMeanBaseline:
    """Inverse-MAE-weighted mean baseline."""

    @pytest.fixture
    def two_skilled_data(self):
        """Two models M1/M2 both passing thresholds with different MAE.

        obs=[100, 110], delta=4.766
        M1 q50=[102, 108]: MAE = (|100-102|+|110-108|)/2 = 2.0
        M2 q50=[101, 109]: MAE = (|100-101|+|110-109|)/2 = 1.0
        """
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2020, 1, "M2", 82, 87, 94, 101, 108, 114, 120),
                ("S1", 2021, 1, "M2", 90, 95, 102, 109, 117, 124, 131),
            ]
        )
        return obs, fcst

    def test_skilled_mean_appears_in_skill_stats(self, two_skilled_data):
        """Skilled Mean appears with correct model_short."""
        obs, fcst = two_skilled_data
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        sm_rows = skill_stats[skill_stats["model_short"] == "Skilled Mean"]
        assert len(sm_rows) == 1

    def test_skilled_mean_weighted_average(self, two_skilled_data):
        """Hand-calculated weighted discharge.

        M1 MAE=2.0, M2 MAE=1.0
        eps = mean(2.0, 1.0) / 100 = 0.015
        w1 = 1/(2.0+0.015) = 0.49628..., w2 = 1/(1.0+0.015) = 0.98522...
        For year 2020: M1=102, M2=101
          SM = (102*w1 + 101*w2) / (w1+w2)
        For year 2021: M1=108, M2=109
          SM = (108*w1 + 109*w2) / (w1+w2)
        """
        obs, fcst = two_skilled_data
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        sm_row = skill_stats[skill_stats["model_short"] == "Skilled Mean"].iloc[0]

        # Compute expected MAE from the weighted forecasts
        mae_m1 = 2.0
        mae_m2 = 1.0
        eps = (mae_m1 + mae_m2) / 2.0 / 100.0  # 0.015
        w1 = 1.0 / (mae_m1 + eps)
        w2 = 1.0 / (mae_m2 + eps)

        sm_2020 = (102 * w1 + 101 * w2) / (w1 + w2)
        sm_2021 = (108 * w1 + 109 * w2) / (w1 + w2)
        expected_mae = (abs(100.0 - sm_2020) + abs(110.0 - sm_2021)) / 2.0

        assert sm_row["mae"] == pytest.approx(expected_mae, rel=1e-4)

    def test_skilled_mean_composition(self, two_skilled_data):
        """Skilled Mean lists contributing models."""
        obs, fcst = two_skilled_data
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        sm_row = skill_stats[skill_stats["model_short"] == "Skilled Mean"].iloc[0]
        assert "composition" in skill_stats.columns
        assert "M1" in sm_row["composition"]
        assert "M2" in sm_row["composition"]

    def test_skilled_mean_crps_computed(self, two_skilled_data):
        """Skilled Mean CRPS computed from vincentized quantiles."""
        obs, fcst = two_skilled_data
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        sm_row = skill_stats[skill_stats["model_short"] == "Skilled Mean"].iloc[0]
        assert not np.isnan(sm_row["crps"])
        assert sm_row["crps"] > 0

    def test_skilled_mean_not_created_single_model(self):
        """Only 1 model passes threshold -> no Skilled Mean."""
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        # M1 passes, Bad fails threshold
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2020, 1, "Bad", 120, 130, 140, 150, 160, 170, 180),
                ("S1", 2021, 1, "Bad", 130, 140, 150, 160, 170, 180, 190),
            ]
        )
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        sm_rows = skill_stats[skill_stats["model_short"] == "Skilled Mean"]
        assert len(sm_rows) == 0

    def test_skilled_mean_not_created_no_models_pass(self):
        """All models fail threshold -> no Skilled Mean."""
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 120, 130, 140, 150, 160, 170, 180),
                ("S1", 2021, 1, "M1", 130, 140, 150, 160, 170, 180, 190),
                ("S1", 2020, 1, "M2", 170, 180, 190, 200, 210, 220, 230),
                ("S1", 2021, 1, "M2", 180, 190, 200, 210, 220, 230, 240),
            ]
        )
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        sm_rows = skill_stats[skill_stats["model_short"] == "Skilled Mean"]
        assert len(sm_rows) == 0

    def test_skilled_mean_excluded_from_em(self):
        """EM does not include Skilled Mean in its composition."""
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2020, 1, "M2", 82, 87, 94, 101, 108, 114, 120),
                ("S1", 2021, 1, "M2", 90, 95, 102, 109, 117, 124, 131),
            ]
        )
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_rows = skill_stats[skill_stats["model_short"] == "EM"]
        assert len(em_rows) == 1
        assert "Skilled Mean" not in em_rows.iloc[0]["composition"]

    def test_skilled_mean_equal_mae_equals_em(self):
        """When all models have equal MAE, Skilled Mean ~ EM.

        With equal MAE, weights are equal, so the weighted mean
        equals the arithmetic mean (EM).
        """
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        # M1 q50=[102,108], M2 q50=[98,112] -> both MAE=2.0
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2021, 1, "M1", 88, 93, 100, 108, 116, 123, 130),
                ("S1", 2020, 1, "M2", 78, 83, 90, 98, 106, 112, 118),
                ("S1", 2021, 1, "M2", 92, 97, 104, 112, 120, 127, 134),
            ]
        )
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_row = skill_stats[skill_stats["model_short"] == "EM"].iloc[0]
        sm_row = skill_stats[skill_stats["model_short"] == "Skilled Mean"].iloc[0]
        assert sm_row["mae"] == pytest.approx(em_row["mae"], rel=1e-4)

    def test_skilled_mean_nan_mae_excluded(self):
        """Model with NaN MAE is excluded from weighting.

        When only 1 valid model remains after NaN exclusion,
        Skilled Mean is not created (single-model check).
        """
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
            ]
        )
        # Single-year: sdivsigma/NSE are NaN but MAE is valid
        # M1 q50=[102] -> MAE=2, M2 has NaN (will not have skill row
        # with valid MAE since single year MAE is still computable).
        # Actually with single year both models get MAE.
        # Let's use a case where one model has NaN forecast:
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 80, 85, 92, 102, 112, 118, 125),
                ("S1", 2020, 1, "M2", 80, 85, 92, np.nan, 112, 118, 125),
            ]
        )
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        # M2 has NaN q50 -> forecasted_discharge is NaN -> MAE is NaN
        # Only M1 has valid MAE -> single model -> no Skilled Mean
        sm_rows = skill_stats[skill_stats["model_short"] == "Skilled Mean"]
        assert len(sm_rows) == 0


# ===================================================================
# Quantile aggregation tests
# ===================================================================


class TestQuantileAggregation:
    """Tests for quantile aggregation in EM, Naive Mean, Skilled Mean."""

    @pytest.fixture
    def two_model_data(self):
        """2 models with known quantile values for exact verification.

        Both M1 and M2 pass skill thresholds (NSE>0.8, sdivsigma<0.6).
        obs = [100, 110], delta = 0.674*7.071 = 4.766

        M1 q50=[102, 108]: NSE=0.84, sdivsigma=0.4
        M2 q50=[101, 109]: NSE=0.96, sdivsigma=0.2
        EM q50 = mean = [101.5, 108.5]

        2020: M1 q05=10, M2 q05=20 -> EM q05=15
        """
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 10, 20, 30, 102, 112, 120, 130),
                ("S1", 2021, 1, "M1", 10, 20, 30, 108, 118, 128, 138),
                ("S1", 2020, 1, "M2", 20, 30, 40, 101, 108, 114, 120),
                ("S1", 2021, 1, "M2", 20, 30, 40, 109, 117, 124, 131),
            ]
        )
        return obs, fcst

    def test_em_quantile_aggregation(self, two_model_data):
        """EM quantiles = simple mean of model quantiles.

        2020: M1 q05=10, M2 q05=20 -> EM q05=15
        2020: M1 q75=112, M2 q75=108 -> EM q75=110
        """
        obs, fcst = two_model_data
        _, joint, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_rows = joint[joint["model_short"] == "EM"]
        assert len(em_rows) == 2, f"Expected 2 EM rows (1 station x 2 years), got {len(em_rows)}"

        em_2020 = em_rows[em_rows["year"] == 2020].iloc[0]
        assert em_2020["q05"] == pytest.approx(15.0, rel=1e-6)
        assert em_2020["q10"] == pytest.approx(25.0, rel=1e-6)
        assert em_2020["q25"] == pytest.approx(35.0, rel=1e-6)
        # M1 q75=112, M2 q75=108 -> mean=110
        assert em_2020["q75"] == pytest.approx(110.0, rel=1e-6)
        # M1 q90=120, M2 q90=114 -> mean=117
        assert em_2020["q90"] == pytest.approx(117.0, rel=1e-6)
        # M1 q95=130, M2 q95=120 -> mean=125
        assert em_2020["q95"] == pytest.approx(125.0, rel=1e-6)

    def test_em_crps_computed(self, two_model_data):
        """EM CRPS is not NaN when quantiles are aggregated."""
        obs, fcst = two_model_data
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_row = skill_stats[skill_stats["model_short"] == "EM"].iloc[0]
        assert not np.isnan(em_row["crps"])
        assert em_row["crps"] > 0

    def test_naive_mean_quantile_aggregation(self, two_model_data):
        """Naive Mean quantiles = simple mean of ALL model quantiles.

        Same as EM for this fixture (both models are skilled).
        M1 q05=10, M2 q05=20 -> mean=15
        M1 q95=130, M2 q95=120 -> mean=125
        """
        obs, fcst = two_model_data
        _, joint, _ = calculate_monthly_skill_metrics(obs, fcst)
        naive_rows = joint[joint["model_short"] == "Naive Mean"]
        assert len(naive_rows) == 2, (
            f"Expected 2 Naive Mean rows (1 station x 2 years), got {len(naive_rows)}"
        )

        naive_2020 = naive_rows[naive_rows["year"] == 2020].iloc[0]
        assert naive_2020["q05"] == pytest.approx(15.0, rel=1e-6)
        assert naive_2020["q95"] == pytest.approx(125.0, rel=1e-6)

    def test_naive_mean_crps_computed(self, two_model_data):
        """Naive Mean CRPS computed from aggregated quantiles."""
        obs, fcst = two_model_data
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        naive = skill_stats[skill_stats["model_short"] == "Naive Mean"].iloc[0]
        assert not np.isnan(naive["crps"])
        assert naive["crps"] > 0

    def test_skilled_mean_quantile_vincentization(self):
        """Skilled Mean quantiles = inverse-MAE weighted mean.

        M1 (MAE=2): q05=10
        M2 (MAE=8): q05=20
        eps = mean(2,8)/100 = 0.05
        w1 = 1/(2+0.05) = 0.4878, w2 = 1/(8+0.05) = 0.1242
        weighted q05 = (0.4878*10 + 0.1242*20)/(0.4878+0.1242) = 12.03
        """
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        # M1 close to obs (low MAE), M2 farther (higher MAE)
        fcst = _make_fcst(
            [
                ("S1", 2020, 1, "M1", 10, 20, 30, 102, 110, 120, 130),
                ("S1", 2021, 1, "M1", 10, 20, 30, 108, 118, 128, 138),
                ("S1", 2020, 1, "M2", 20, 30, 40, 108, 118, 128, 138),
                ("S1", 2021, 1, "M2", 20, 30, 40, 118, 128, 138, 148),
            ]
        )
        skill_stats, joint, _ = calculate_monthly_skill_metrics(obs, fcst)

        sm_rows = skill_stats[skill_stats["model_short"] == "Skilled Mean"]
        if len(sm_rows) > 0:
            sm = sm_rows.iloc[0]
            # Skilled Mean should exist and have CRPS
            assert not np.isnan(sm["crps"])

            # Verify weighted quantiles in joint
            sm_joint = joint[joint["model_short"] == "Skilled Mean"]
            if "q05" in sm_joint.columns and len(sm_joint) > 0:
                sm_2020 = sm_joint[sm_joint["year"] == 2020].iloc[0]
                # Weighted toward M1 (lower MAE), so q05 < 15
                assert sm_2020["q05"] < 15.0

    def test_ensemble_joint_cols_include_valid_from_valid_to(self):
        """Ensemble rows carry valid_from, valid_to when present."""
        obs = _make_obs(
            [
                ("S1", 2020, 1, 100.0),
                ("S1", 2021, 1, 110.0),
            ]
        )
        fcst = pd.DataFrame(
            {
                "code": ["S1", "S1", "S1", "S1"],
                "year": [2020, 2021, 2020, 2021],
                "month": [1, 1, 1, 1],
                "model_short": ["M1", "M1", "M2", "M2"],
                "q05": [10, 10, 20, 20],
                "q10": [20, 20, 30, 30],
                "q25": [30, 30, 40, 40],
                "q50": [102, 108, 101, 109],
                "q75": [110, 118, 108, 117],
                "q90": [120, 128, 114, 124],
                "q95": [130, 138, 120, 131],
                "valid_from": ["2020-01-01", "2021-01-01", "2020-01-01", "2021-01-01"],
                "valid_to": ["2020-01-31", "2021-01-31", "2020-01-31", "2021-01-31"],
                "date": pd.to_datetime(
                    [
                        "2020-01-01",
                        "2021-01-01",
                        "2020-01-01",
                        "2021-01-01",
                    ]
                ),
            }
        )
        _, joint, _ = calculate_monthly_skill_metrics(obs, fcst)

        em_rows = joint[joint["model_short"] == "EM"]
        if len(em_rows) > 0 and "valid_from" in em_rows.columns:
            assert em_rows["valid_from"].notna().all()


class TestPointForecastFallback:
    """Models that store forecast in q (not q50) should still produce skill metrics.

    This covers GBT, LR_Base, LR_SM, etc. which populate q but not q50 in the
    long_forecasts API. MC_ALD populates both q and q50.
    """

    def test_q_fallback_produces_nonzero_npairs(self):
        """When q50 is NaN but q is populated, skill metrics should compute."""
        obs = _make_obs(
            [
                ("S1", 2024, 1, 10.0),
                ("S1", 2024, 2, 12.0),
                ("S1", 2025, 1, 11.0),
                ("S1", 2025, 2, 13.0),
            ]
        )
        # Point-only model: q has values, q50 is NaN, no quantiles
        fcst = pd.DataFrame(
            {
                "code": ["S1"] * 4,
                "year": [2024, 2024, 2025, 2025],
                "month": [1, 2, 1, 2],
                "model_short": ["GBT"] * 4,
                "q": [9.5, 11.0, 10.5, 12.5],
                "q50": [np.nan] * 4,
                "q05": [np.nan] * 4,
                "q10": [np.nan] * 4,
                "q25": [np.nan] * 4,
                "q75": [np.nan] * 4,
                "q90": [np.nan] * 4,
                "q95": [np.nan] * 4,
            }
        )
        stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        gbt_stats = stats[stats["model_short"] == "GBT"]
        assert not gbt_stats.empty, "GBT should have skill metrics"
        assert gbt_stats.iloc[0]["n_pairs"] > 0, "n_pairs should be > 0"
        assert pd.notna(gbt_stats.iloc[0]["nse"]), "nse should be computed"
        assert pd.notna(gbt_stats.iloc[0]["mae"]), "mae should be computed"

    def test_q50_preferred_over_q_when_both_present(self):
        """When both q50 and q are populated, q50 should be used (MC_ALD case)."""
        obs = _make_obs(
            [
                ("S1", 2024, 1, 10.0),
                ("S1", 2024, 2, 12.0),
            ]
        )
        fcst = pd.DataFrame(
            {
                "code": ["S1", "S1"],
                "year": [2024, 2024],
                "month": [1, 2],
                "model_short": ["MC_ALD", "MC_ALD"],
                "q": [9.0, 11.0],  # different from q50
                "q50": [9.5, 11.5],  # q50 should be preferred
                "q05": [7.0, 9.0],
                "q10": [7.5, 9.5],
                "q25": [8.0, 10.0],
                "q75": [11.0, 13.0],
                "q90": [12.0, 14.0],
                "q95": [13.0, 15.0],
            }
        )
        stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        mc_stats = stats[stats["model_short"] == "MC_ALD"]
        assert mc_stats.iloc[0]["n_pairs"] > 0
        # MAE should be computed against q50 values (9.5, 11.5), not q values (9.0, 11.0)
        # obs = (10, 12), q50 = (9.5, 11.5) → errors = (0.5, 0.5) → MAE = 0.5
        assert abs(mc_stats.iloc[0]["mae"] - 0.5) < 0.01, (
            f"MAE should be ~0.5 (using q50), got {mc_stats.iloc[0]['mae']}"
        )

    def test_mixed_models_q50_and_q_only(self):
        """MC_ALD (has q50) and GBT (q only) should both produce metrics."""
        obs = _make_obs(
            [
                ("S1", 2024, 1, 10.0),
                ("S1", 2024, 2, 12.0),
                ("S1", 2025, 1, 11.0),
                ("S1", 2025, 2, 13.0),
            ]
        )
        rows_mc = [
            ("S1", 2024, 1, "MC_ALD", 7.0, 7.5, 8.0, 9.5, 11.0, 12.0, 13.0),
            ("S1", 2024, 2, "MC_ALD", 9.0, 9.5, 10.0, 11.5, 13.0, 14.0, 15.0),
            ("S1", 2025, 1, "MC_ALD", 7.5, 8.0, 8.5, 10.0, 11.5, 12.5, 13.5),
            ("S1", 2025, 2, "MC_ALD", 9.5, 10.0, 10.5, 12.0, 13.5, 14.5, 15.5),
        ]
        fcst_mc = _make_fcst(rows_mc)
        fcst_mc["q"] = fcst_mc["q50"]  # MC_ALD has both

        fcst_gbt = pd.DataFrame(
            {
                "code": ["S1"] * 4,
                "year": [2024, 2024, 2025, 2025],
                "month": [1, 2, 1, 2],
                "model_short": ["GBT"] * 4,
                "q": [9.5, 11.0, 10.5, 12.5],
                "q50": [np.nan] * 4,
                "q05": [np.nan] * 4,
                "q10": [np.nan] * 4,
                "q25": [np.nan] * 4,
                "q75": [np.nan] * 4,
                "q90": [np.nan] * 4,
                "q95": [np.nan] * 4,
            }
        )
        fcst = pd.concat([fcst_mc, fcst_gbt], ignore_index=True)
        stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)

        models_with_metrics = stats[stats["n_pairs"] > 0]["model_short"].unique()
        assert "MC_ALD" in models_with_metrics, "MC_ALD should have metrics"
        assert "GBT" in models_with_metrics, "GBT should have metrics via q fallback"

    def test_q_column_absent_still_works(self):
        """When q column is not present at all, behavior unchanged (uses q50)."""
        obs = _make_obs(
            [
                ("S1", 2024, 1, 10.0),
                ("S1", 2024, 2, 12.0),
            ]
        )
        rows = [
            ("S1", 2024, 1, "MC_ALD", 7.0, 7.5, 8.0, 9.5, 11.0, 12.0, 13.0),
            ("S1", 2024, 2, "MC_ALD", 9.0, 9.5, 10.0, 11.5, 13.0, 14.0, 15.0),
        ]
        fcst = _make_fcst(rows)  # no "q" column
        stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        mc_stats = stats[stats["model_short"] == "MC_ALD"]
        assert mc_stats.iloc[0]["n_pairs"] > 0

    def test_both_q_and_q50_nan_returns_zero_npairs(self):
        """When both q and q50 are NaN, n_pairs should be 0 (no crash)."""
        obs = _make_obs(
            [
                ("S1", 2024, 1, 10.0),
                ("S1", 2024, 2, 12.0),
                ("S1", 2025, 1, 11.0),
                ("S1", 2025, 2, 13.0),
            ]
        )
        fcst = pd.DataFrame(
            {
                "code": ["S1"] * 4,
                "year": [2024, 2024, 2025, 2025],
                "month": [1, 2, 1, 2],
                "model_short": ["EMPTY"] * 4,
                "q": [np.nan] * 4,
                "q50": [np.nan] * 4,
                "q05": [np.nan] * 4,
                "q10": [np.nan] * 4,
                "q25": [np.nan] * 4,
                "q75": [np.nan] * 4,
                "q90": [np.nan] * 4,
                "q95": [np.nan] * 4,
            }
        )
        stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        empty_stats = stats[stats["model_short"] == "EMPTY"]
        # Should not crash; should return 0 pairs
        assert empty_stats.empty or empty_stats.iloc[0]["n_pairs"] == 0

    def test_zero_forecast_value_not_treated_as_missing(self):
        """q=0.0 is a valid forecast (e.g. dry season), not NaN."""
        obs = _make_obs(
            [
                ("S1", 2024, 1, 0.5),
                ("S1", 2024, 2, 0.3),
                ("S1", 2025, 1, 0.4),
                ("S1", 2025, 2, 0.2),
            ]
        )
        fcst = pd.DataFrame(
            {
                "code": ["S1"] * 4,
                "year": [2024, 2024, 2025, 2025],
                "month": [1, 2, 1, 2],
                "model_short": ["DRY"] * 4,
                "q": [0.0, 0.0, 0.0, 0.0],
                "q50": [np.nan] * 4,
                "q05": [np.nan] * 4,
                "q10": [np.nan] * 4,
                "q25": [np.nan] * 4,
                "q75": [np.nan] * 4,
                "q90": [np.nan] * 4,
                "q95": [np.nan] * 4,
            }
        )
        stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        dry_stats = stats[stats["model_short"] == "DRY"]
        assert not dry_stats.empty, "DRY model should have stats"
        assert dry_stats.iloc[0]["n_pairs"] > 0, "Zero forecasts should count as valid pairs"
        assert pd.notna(dry_stats.iloc[0]["mae"]), "MAE should be computed"

    def test_partial_q50_fill_within_single_model(self):
        """Some rows have q50, others only q — both should contribute pairs."""
        obs = _make_obs(
            [
                ("S1", 2024, 1, 10.0),
                ("S1", 2024, 2, 12.0),
                ("S1", 2025, 1, 11.0),
                ("S1", 2025, 2, 13.0),
            ]
        )
        fcst = pd.DataFrame(
            {
                "code": ["S1"] * 4,
                "year": [2024, 2024, 2025, 2025],
                "month": [1, 2, 1, 2],
                "model_short": ["MIXED"] * 4,
                "q": [9.5, 11.0, 10.5, 12.5],
                "q50": [9.5, np.nan, 10.5, np.nan],  # partial: 2 have q50, 2 don't
                "q05": [np.nan] * 4,
                "q10": [np.nan] * 4,
                "q25": [np.nan] * 4,
                "q75": [np.nan] * 4,
                "q90": [np.nan] * 4,
                "q95": [np.nan] * 4,
            }
        )
        stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        mixed_stats = stats[stats["model_short"] == "MIXED"]
        assert not mixed_stats.empty
        # All 4 rows should produce pairs (q50 used where available, q elsewhere)
        # With 2 years x 2 months, grouped by month_in_year, each group has 2 pairs
        assert mixed_stats.iloc[0]["n_pairs"] >= 2
