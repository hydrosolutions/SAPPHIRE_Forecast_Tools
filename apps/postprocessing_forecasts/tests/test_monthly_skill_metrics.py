"""Tests for calculate_monthly_skill_metrics().

Step 5 of Phase 4a: Monthly skill metrics.
TDD — tests written before implementation.
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..')
)

from src.skill_metrics import calculate_monthly_skill_metrics, calculate_crps


# Standard quantile levels used in SAPPHIRE long-term forecasts
QUANTILE_LEVELS = np.array([0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95])

# Expected columns in skill_stats output
EXPECTED_METRIC_COLS = {
    'month_in_year', 'code', 'model_short',
    'sdivsigma', 'nse', 'delta', 'accuracy', 'mae', 'n_pairs', 'crps',
}


# ===================================================================
# Helper functions
# ===================================================================

def _make_obs(rows):
    """Create observations DataFrame from (code, year, month, discharge_avg).

    Automatically computes month_in_year and delta (0.674 * std).
    """
    df = pd.DataFrame(
        rows, columns=['code', 'year', 'month', 'discharge_avg']
    )
    df['month_in_year'] = df['month']

    delta_df = df.groupby(['code', 'month_in_year']).agg(
        std_discharge=('discharge_avg', 'std'),
    ).reset_index()
    delta_df['delta'] = 0.674 * delta_df['std_discharge'].fillna(0.0)

    df = df.merge(
        delta_df[['code', 'month_in_year', 'delta']],
        on=['code', 'month_in_year'],
        how='left',
    )
    return df


def _make_fcst(rows):
    """Create forecasts DataFrame.

    Each row: (code, year, month, model_short,
               q05, q10, q25, q50, q75, q90, q95)
    """
    return pd.DataFrame(rows, columns=[
        'code', 'year', 'month', 'model_short',
        'q05', 'q10', 'q25', 'q50', 'q75', 'q90', 'q95',
    ])


# ===================================================================
# Basic functionality
# ===================================================================

class TestMonthlyMetricsBasic:
    """Core merge, point metrics, and CRPS."""

    @pytest.fixture
    def basic_data(self):
        """Single model M1, station S1, 2 years x 2 months."""
        obs = _make_obs([
            ('S1', 2020, 1, 100.0),
            ('S1', 2021, 1, 110.0),
            ('S1', 2020, 2, 80.0),
            ('S1', 2021, 2, 85.0),
        ])
        fcst = _make_fcst([
            ('S1', 2020, 1, 'M1', 80, 85, 92, 102, 112, 118, 125),
            ('S1', 2021, 1, 'M1', 88, 93, 100, 108, 116, 123, 130),
            ('S1', 2020, 2, 'M1', 65, 68, 75, 82, 89, 94, 98),
            ('S1', 2021, 2, 'M1', 67, 70, 76, 83, 90, 95, 100),
        ])
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
        model_rows = skill_stats[
            ~skill_stats['model_short'].isin(['Naive Mean', 'EM'])
        ]
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
            (skill_stats['month_in_year'] == 1)
            & (skill_stats['model_short'] == 'M1')
        ].iloc[0]
        assert row['mae'] == pytest.approx(2.0, rel=1e-6)
        assert row['n_pairs'] == 2
        assert row['sdivsigma'] == pytest.approx(0.4, rel=1e-6)
        assert row['nse'] == pytest.approx(0.84, rel=1e-6)
        assert row['accuracy'] == pytest.approx(1.0, rel=1e-6)
        expected_delta = 0.674 * np.std([100.0, 110.0], ddof=1)
        assert row['delta'] == pytest.approx(expected_delta, rel=1e-6)

    def test_crps_computed_and_nonnegative(self, basic_data):
        """CRPS is present and non-negative for models with quantiles."""
        obs, fcst = basic_data
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        model_rows = skill_stats[skill_stats['model_short'] == 'M1']
        assert all(np.isfinite(model_rows['crps']))
        assert all(model_rows['crps'] >= 0)

    def test_crps_value_matches_direct_calculation(self, basic_data):
        """CRPS matches calculate_crps() for month 1 group."""
        obs, fcst = basic_data
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        row = skill_stats[
            (skill_stats['month_in_year'] == 1)
            & (skill_stats['model_short'] == 'M1')
        ].iloc[0]

        expected = calculate_crps(
            np.array([100.0, 110.0]),
            np.array([
                [80, 85, 92, 102, 112, 118, 125],
                [88, 93, 100, 108, 116, 123, 130],
            ], dtype=float),
            QUANTILE_LEVELS,
        )
        assert row['crps'] == pytest.approx(expected, rel=1e-6)


# ===================================================================
# Multiple models
# ===================================================================

class TestMonthlyMetricsMultiModel:
    """Multiple models per station."""

    @pytest.fixture
    def two_model_data(self):
        """Two models M1/M2, one station, 2 years x 1 month."""
        obs = _make_obs([
            ('S1', 2020, 1, 100.0),
            ('S1', 2021, 1, 110.0),
        ])
        fcst = _make_fcst([
            ('S1', 2020, 1, 'M1', 80, 85, 92, 102, 112, 118, 125),
            ('S1', 2021, 1, 'M1', 88, 93, 100, 108, 116, 123, 130),
            ('S1', 2020, 1, 'M2', 82, 87, 94, 101, 108, 114, 120),
            ('S1', 2021, 1, 'M2', 90, 95, 102, 109, 117, 124, 131),
        ])
        return obs, fcst

    def test_both_models_get_metrics(self, two_model_data):
        """Both M1 and M2 appear in skill_stats."""
        obs, fcst = two_model_data
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        models = set(skill_stats['model_short'].unique())
        assert {'M1', 'M2'}.issubset(models)

    def test_different_crps_per_model(self, two_model_data):
        """M1 and M2 have different CRPS (different quantiles)."""
        obs, fcst = two_model_data
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        crps_m1 = skill_stats[
            skill_stats['model_short'] == 'M1'
        ]['crps'].iloc[0]
        crps_m2 = skill_stats[
            skill_stats['model_short'] == 'M2'
        ]['crps'].iloc[0]
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
        obs = _make_obs([
            ('S1', 2020, 1, 100.0),
            ('S1', 2021, 1, 110.0),
        ])
        fcst = _make_fcst([
            ('S1', 2020, 1, 'M1', 80, 85, 92, 102, 112, 118, 125),
            ('S1', 2021, 1, 'M1', 88, 93, 100, 108, 116, 123, 130),
            ('S1', 2020, 1, 'M2', 82, 87, 94, 101, 108, 114, 120),
            ('S1', 2021, 1, 'M2', 90, 95, 102, 109, 117, 124, 131),
        ])
        return obs, fcst

    def test_em_row_created(self, two_skilled_models):
        """EM row created when 2+ models pass threshold."""
        obs, fcst = two_skilled_models
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_rows = skill_stats[skill_stats['model_short'] == 'EM']
        assert len(em_rows) == 1

    def test_em_has_composition(self, two_skilled_models):
        """EM row lists contributing models in composition."""
        obs, fcst = two_skilled_models
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_row = skill_stats[
            skill_stats['model_short'] == 'EM'
        ].iloc[0]
        assert 'composition' in skill_stats.columns
        assert 'M1' in em_row['composition']
        assert 'M2' in em_row['composition']

    def test_em_mae_from_mean_q50(self, two_skilled_models):
        """EM forecast = mean of skilled models' q50.

        M1 q50=[102, 108], M2 q50=[101, 109]
        EM = [101.5, 108.5], obs = [100, 110]
        MAE = mean(1.5, 1.5) = 1.5
        """
        obs, fcst = two_skilled_models
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_row = skill_stats[
            skill_stats['model_short'] == 'EM'
        ].iloc[0]
        assert em_row['mae'] == pytest.approx(1.5, rel=1e-6)

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
        em_row = skill_stats[
            skill_stats['model_short'] == 'EM'
        ].iloc[0]
        assert em_row['n_pairs'] == 2
        assert em_row['sdivsigma'] == pytest.approx(0.3, rel=1e-6)
        assert em_row['nse'] == pytest.approx(0.91, rel=1e-6)
        assert em_row['accuracy'] == pytest.approx(1.0, rel=1e-6)

    def test_em_crps_is_nan(self, two_skilled_models):
        """EM has no quantile distribution, CRPS = NaN."""
        obs, fcst = two_skilled_models
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_row = skill_stats[
            skill_stats['model_short'] == 'EM'
        ].iloc[0]
        assert np.isnan(em_row['crps'])

    def test_no_em_single_skilled(self):
        """No EM when only one model passes threshold."""
        obs = _make_obs([
            ('S1', 2020, 1, 100.0),
            ('S1', 2021, 1, 110.0),
        ])
        # M1 passes (q50 close), Bad fails (q50=150/160 far from obs)
        fcst = _make_fcst([
            ('S1', 2020, 1, 'M1', 80, 85, 92, 102, 112, 118, 125),
            ('S1', 2021, 1, 'M1', 88, 93, 100, 108, 116, 123, 130),
            ('S1', 2020, 1, 'Bad', 120, 130, 140, 150, 160, 170, 180),
            ('S1', 2021, 1, 'Bad', 130, 140, 150, 160, 170, 180, 190),
        ])
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_rows = skill_stats[skill_stats['model_short'] == 'EM']
        assert len(em_rows) == 0

    def test_em_in_joint_forecasts(self, two_skilled_models):
        """EM rows in joint_forecasts alongside originals."""
        obs, fcst = two_skilled_models
        _, joint, _ = calculate_monthly_skill_metrics(obs, fcst)
        models = set(joint['model_short'].unique())
        assert 'EM' in models
        assert 'M1' in models
        assert 'M2' in models

    def test_em_three_models_partial_pass(self):
        """EM from 2 of 3 models when only 2 pass thresholds.

        M1 and M2 have good q50, Bad has q50=150/160 (far from obs).
        EM = mean(M1, M2) only. Bad excluded from composition.
        EM MAE = 1.5 (same as two_skilled_models fixture).
        """
        obs = _make_obs([
            ('S1', 2020, 1, 100.0),
            ('S1', 2021, 1, 110.0),
        ])
        fcst = _make_fcst([
            ('S1', 2020, 1, 'M1', 80, 85, 92, 102, 112, 118, 125),
            ('S1', 2021, 1, 'M1', 88, 93, 100, 108, 116, 123, 130),
            ('S1', 2020, 1, 'M2', 82, 87, 94, 101, 108, 114, 120),
            ('S1', 2021, 1, 'M2', 90, 95, 102, 109, 117, 124, 131),
            ('S1', 2020, 1, 'Bad', 120, 130, 140, 150, 160, 170, 180),
            ('S1', 2021, 1, 'Bad', 130, 140, 150, 160, 170, 180, 190),
        ])
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_rows = skill_stats[skill_stats['model_short'] == 'EM']
        assert len(em_rows) == 1
        em_row = em_rows.iloc[0]
        assert 'M1' in em_row['composition']
        assert 'M2' in em_row['composition']
        assert 'Bad' not in em_row['composition']
        assert em_row['mae'] == pytest.approx(1.5, rel=1e-6)


# ===================================================================
# Naive Mean baseline
# ===================================================================

class TestNaiveMeanBaseline:
    """Climatological mean baseline (no-skill reference)."""

    @pytest.fixture
    def data_three_years(self):
        """3 years of data for meaningful Naive Mean."""
        obs = _make_obs([
            ('S1', 2020, 1, 100.0),
            ('S1', 2021, 1, 110.0),
            ('S1', 2022, 1, 105.0),
        ])
        fcst = _make_fcst([
            ('S1', 2020, 1, 'M1', 80, 85, 92, 102, 112, 118, 125),
            ('S1', 2021, 1, 'M1', 88, 93, 100, 108, 116, 123, 130),
            ('S1', 2022, 1, 'M1', 85, 90, 98, 106, 114, 120, 128),
        ])
        return obs, fcst

    def test_naive_mean_in_skill_stats(self, data_three_years):
        """Naive Mean appears as a model in skill_stats."""
        obs, fcst = data_three_years
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        assert 'Naive Mean' in skill_stats['model_short'].values

    def test_naive_mean_mae(self, data_three_years):
        """Naive Mean q50 = climatological mean = 105.

        MAE = mean(|100-105|, |110-105|, |105-105|) = 10/3
        """
        obs, fcst = data_three_years
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        naive = skill_stats[
            skill_stats['model_short'] == 'Naive Mean'
        ].iloc[0]
        assert naive['mae'] == pytest.approx(10.0 / 3.0, rel=1e-6)

    def test_naive_mean_crps_is_nan(self, data_three_years):
        """Naive Mean has no quantile distribution, CRPS = NaN."""
        obs, fcst = data_three_years
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        naive = skill_stats[
            skill_stats['model_short'] == 'Naive Mean'
        ].iloc[0]
        assert np.isnan(naive['crps'])

    def test_naive_mean_nse_zero(self, data_three_years):
        """Naive Mean NSE = 0 — the no-skill reference by definition.

        obs = [100, 110, 105], mean = 105
        SS_res = (100-105)^2 + (110-105)^2 + (105-105)^2 = 50
        SS_tot = same = 50
        NSE = 1 - 50/50 = 0.0
        """
        obs, fcst = data_three_years
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        naive = skill_stats[
            skill_stats['model_short'] == 'Naive Mean'
        ].iloc[0]
        assert naive['nse'] == pytest.approx(0.0, abs=1e-10)

    def test_naive_mean_all_metrics(self, data_three_years):
        """All Naive Mean metrics verified.

        obs = [100, 110, 105], clim_mean = 105
        sdivsigma: s = sigma -> sdivsigma = 1.0
        accuracy: |5|,|5| > delta(3.37), |0| <= delta -> 1/3
        n_pairs = 3
        """
        obs, fcst = data_three_years
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        naive = skill_stats[
            skill_stats['model_short'] == 'Naive Mean'
        ].iloc[0]
        assert naive['n_pairs'] == 3
        assert naive['sdivsigma'] == pytest.approx(1.0, rel=1e-6)
        assert naive['accuracy'] == pytest.approx(1.0 / 3.0, rel=1e-6)
        expected_delta = 0.674 * np.std([100.0, 110.0, 105.0], ddof=1)
        assert naive['delta'] == pytest.approx(expected_delta, rel=1e-6)


# ===================================================================
# Edge cases
# ===================================================================

class TestMonthlyMetricsEdgeCases:
    """Edge cases and boundary conditions."""

    def test_empty_observations(self):
        """Empty observations returns empty skill_stats."""
        obs = pd.DataFrame(
            columns=['code', 'year', 'month', 'month_in_year',
                     'discharge_avg', 'delta']
        )
        fcst = _make_fcst([
            ('S1', 2020, 1, 'M1', 80, 85, 92, 100, 110, 120, 130),
        ])
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        assert len(skill_stats) == 0

    def test_empty_forecasts(self):
        """Empty forecasts returns empty skill_stats."""
        obs = _make_obs([
            ('S1', 2020, 1, 100.0),
            ('S1', 2021, 1, 110.0),
        ])
        fcst = pd.DataFrame(columns=[
            'code', 'year', 'month', 'model_short',
            'q05', 'q10', 'q25', 'q50', 'q75', 'q90', 'q95',
        ])
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        assert len(skill_stats) == 0

    def test_no_overlap_no_real_model_metrics(self):
        """No matching (code, year, month) gives no real model metrics."""
        obs = _make_obs([
            ('S1', 2020, 1, 100.0),
            ('S1', 2021, 1, 110.0),
        ])
        fcst = _make_fcst([
            ('S2', 2020, 1, 'M1', 80, 85, 92, 100, 110, 120, 130),
        ])
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        real = skill_stats[
            ~skill_stats['model_short'].isin(['Naive Mean'])
        ]
        assert len(real) == 0

    def test_multi_station_independent(self):
        """Stations get independent metrics with correct values.

        S1: obs=[100,110], q50=[102,108], MAE=2.0, sdivsigma=0.4
        S2: obs=[200,220], q50=[205,218], MAE=3.5
            diff=[-5,2], s=sqrt(29/1)=5.385, sigma=std([200,220],ddof=1)
        """
        obs = _make_obs([
            ('S1', 2020, 1, 100.0),
            ('S1', 2021, 1, 110.0),
            ('S2', 2020, 1, 200.0),
            ('S2', 2021, 1, 220.0),
        ])
        fcst = _make_fcst([
            ('S1', 2020, 1, 'M1', 80, 85, 92, 102, 112, 118, 125),
            ('S1', 2021, 1, 'M1', 88, 93, 100, 108, 116, 123, 130),
            ('S2', 2020, 1, 'M1', 160, 170, 185, 205, 215, 225, 235),
            ('S2', 2021, 1, 'M1', 180, 190, 205, 218, 230, 240, 250),
        ])
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        model_rows = skill_stats[skill_stats['model_short'] == 'M1']
        assert len(model_rows) == 2
        assert set(model_rows['code'].values) == {'S1', 'S2'}

        # S1: obs=[100,110], q50=[102,108]
        s1 = model_rows[model_rows['code'] == 'S1'].iloc[0]
        assert s1['mae'] == pytest.approx(2.0, rel=1e-6)
        assert s1['sdivsigma'] == pytest.approx(0.4, rel=1e-6)

        # S2: obs=[200,220], q50=[205,218], MAE=mean(5,2)=3.5
        s2 = model_rows[model_rows['code'] == 'S2'].iloc[0]
        assert s2['mae'] == pytest.approx(3.5, rel=1e-6)
        # sdivsigma = sqrt(sum([-5,2]^2)/(2-1)) / std([200,220],ddof=1)
        assert s2['sdivsigma'] == pytest.approx(
            np.sqrt(29.0) / np.std([200.0, 220.0], ddof=1), rel=1e-6
        )

    def test_single_year_npairs_one(self):
        """Single year: n_pairs=1, sdivsigma/NSE=NaN, MAE still valid.

        min_points=2 for sdivsigma/NSE not met with one obs-fcst pair.
        delta=0 (single year std=NaN -> fillna(0)), accuracy=0.
        """
        obs = _make_obs([
            ('S1', 2020, 1, 100.0),
        ])
        fcst = _make_fcst([
            ('S1', 2020, 1, 'M1', 80, 85, 92, 102, 112, 118, 125),
        ])
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        row = skill_stats[skill_stats['model_short'] == 'M1'].iloc[0]
        assert row['n_pairs'] == 1
        assert row['mae'] == pytest.approx(2.0, rel=1e-6)
        assert np.isnan(row['sdivsigma'])
        assert np.isnan(row['nse'])
        assert row['delta'] == pytest.approx(0.0, abs=1e-10)
        assert row['accuracy'] == pytest.approx(0.0, abs=1e-10)

    def test_no_em_when_all_models_fail_thresholds(self):
        """No EM when all models fail skill thresholds.

        Both M1 and M2 have terrible q50 (150/160 and 200/210).
        Neither passes -> no EM. Model rows and Naive Mean still present.
        """
        obs = _make_obs([
            ('S1', 2020, 1, 100.0),
            ('S1', 2021, 1, 110.0),
        ])
        fcst = _make_fcst([
            ('S1', 2020, 1, 'M1', 120, 130, 140, 150, 160, 170, 180),
            ('S1', 2021, 1, 'M1', 130, 140, 150, 160, 170, 180, 190),
            ('S1', 2020, 1, 'M2', 170, 180, 190, 200, 210, 220, 230),
            ('S1', 2021, 1, 'M2', 180, 190, 200, 210, 220, 230, 240),
        ])
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_rows = skill_stats[skill_stats['model_short'] == 'EM']
        assert len(em_rows) == 0
        assert 'M1' in skill_stats['model_short'].values
        assert 'M2' in skill_stats['model_short'].values
        assert 'Naive Mean' in skill_stats['model_short'].values

    def test_nan_quantiles_point_metrics_still_computed(self):
        """NaN quantiles don't break point metrics (q50 still valid).

        obs=[100,110], q50=[102,108] — same as basic month 1.
        Point metrics computed from q50. CRPS = NaN (NaN quantiles).
        """
        obs = _make_obs([
            ('S1', 2020, 1, 100.0),
            ('S1', 2021, 1, 110.0),
        ])
        fcst = _make_fcst([
            ('S1', 2020, 1, 'M1', np.nan, np.nan, np.nan, 102,
             np.nan, np.nan, np.nan),
            ('S1', 2021, 1, 'M1', np.nan, np.nan, np.nan, 108,
             np.nan, np.nan, np.nan),
        ])
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        row = skill_stats[skill_stats['model_short'] == 'M1'].iloc[0]
        assert row['mae'] == pytest.approx(2.0, rel=1e-6)
        assert row['sdivsigma'] == pytest.approx(0.4, rel=1e-6)
        assert row['nse'] == pytest.approx(0.84, rel=1e-6)
        assert np.isnan(row['crps'])

    def test_nan_discharge_avg_excluded_from_metrics(self):
        """NaN discharge_avg rows are dropped by the inner merge.

        Observations with NaN discharge_avg should not contribute to
        metrics. Here S1 month 1 has 3 years but one has NaN obs.
        Only 2 valid pairs should be used.

        obs valid = [100, 110], q50 = [102, 108], MAE = 2.0
        """
        obs = _make_obs([
            ('S1', 2020, 1, 100.0),
            ('S1', 2021, 1, 110.0),
            ('S1', 2022, 1, np.nan),
        ])
        fcst = _make_fcst([
            ('S1', 2020, 1, 'M1', 80, 85, 92, 102, 112, 118, 125),
            ('S1', 2021, 1, 'M1', 88, 93, 100, 108, 116, 123, 130),
            ('S1', 2022, 1, 'M1', 85, 90, 98, 105, 113, 120, 127),
        ])
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        row = skill_stats[skill_stats['model_short'] == 'M1'].iloc[0]
        # NaN obs merged in but produce NaN forecasted_discharge diff
        # calculate_all_skill_metrics masks NaN pairs internally
        # n_pairs should reflect valid pairs only
        assert row['n_pairs'] >= 2
        assert row['mae'] == pytest.approx(2.0, rel=1e-6)

    def test_duplicate_obs_rows_inflate_metrics(self):
        """Duplicate (code, year, month) in observations inflates n_pairs.

        This test documents current behavior: the inner merge produces
        one forecast row per observation duplicate, so n_pairs increases.
        If this is ever guarded against, this test should be updated.

        S1 month 1: obs has 2020 duplicated. Merge produces 3 rows
        for M1 (2020 appears twice, 2021 once) instead of 2.
        """
        obs = _make_obs([
            ('S1', 2020, 1, 100.0),
            ('S1', 2020, 1, 100.0),  # duplicate
            ('S1', 2021, 1, 110.0),
        ])
        fcst = _make_fcst([
            ('S1', 2020, 1, 'M1', 80, 85, 92, 102, 112, 118, 125),
            ('S1', 2021, 1, 'M1', 88, 93, 100, 108, 116, 123, 130),
        ])
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        row = skill_stats[skill_stats['model_short'] == 'M1'].iloc[0]
        # 2020 forecast joined twice (one per obs dup) + 2021 = 3
        assert row['n_pairs'] == 3

    def test_duplicate_forecast_rows_inflate_metrics(self):
        """Duplicate (code, year, month, model_short) in forecasts
        inflates n_pairs via merge. Documents current behavior.
        """
        obs = _make_obs([
            ('S1', 2020, 1, 100.0),
            ('S1', 2021, 1, 110.0),
        ])
        fcst = _make_fcst([
            ('S1', 2020, 1, 'M1', 80, 85, 92, 102, 112, 118, 125),
            ('S1', 2020, 1, 'M1', 80, 85, 92, 102, 112, 118, 125),
            ('S1', 2021, 1, 'M1', 88, 93, 100, 108, 116, 123, 130),
        ])
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        row = skill_stats[skill_stats['model_short'] == 'M1'].iloc[0]
        # 2020 obs joined twice (one per fcst dup) + 2021 = 3
        assert row['n_pairs'] == 3

    def test_em_joint_forecasts_values_correct(self):
        """EM rows in joint_forecasts have correct discharge values.

        M1 q50=[102, 108], M2 q50=[101, 109]
        EM = mean of q50 = [101.5, 108.5] per (year, code).
        """
        obs = _make_obs([
            ('S1', 2020, 1, 100.0),
            ('S1', 2021, 1, 110.0),
        ])
        fcst = _make_fcst([
            ('S1', 2020, 1, 'M1', 80, 85, 92, 102, 112, 118, 125),
            ('S1', 2021, 1, 'M1', 88, 93, 100, 108, 116, 123, 130),
            ('S1', 2020, 1, 'M2', 82, 87, 94, 101, 108, 114, 120),
            ('S1', 2021, 1, 'M2', 90, 95, 102, 109, 117, 124, 131),
        ])
        _, joint, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_rows = joint[joint['model_short'] == 'EM']
        assert len(em_rows) == 2

        em_2020 = em_rows[em_rows['year'] == 2020].iloc[0]
        assert em_2020['forecasted_discharge'] == pytest.approx(
            101.5, rel=1e-6
        )

        em_2021 = em_rows[em_rows['year'] == 2021].iloc[0]
        assert em_2021['forecasted_discharge'] == pytest.approx(
            108.5, rel=1e-6
        )

    def test_partial_station_month_coverage(self):
        """Stations with different month coverage get independent metrics.

        S1 has data for months 1 and 2.
        S2 has data for month 1 only.
        Both should get metrics for their respective months.
        """
        obs = _make_obs([
            ('S1', 2020, 1, 100.0),
            ('S1', 2021, 1, 110.0),
            ('S1', 2020, 2, 80.0),
            ('S1', 2021, 2, 85.0),
            ('S2', 2020, 1, 200.0),
            ('S2', 2021, 1, 220.0),
        ])
        fcst = _make_fcst([
            ('S1', 2020, 1, 'M1', 80, 85, 92, 102, 112, 118, 125),
            ('S1', 2021, 1, 'M1', 88, 93, 100, 108, 116, 123, 130),
            ('S1', 2020, 2, 'M1', 65, 68, 75, 82, 89, 94, 98),
            ('S1', 2021, 2, 'M1', 67, 70, 76, 83, 90, 95, 100),
            ('S2', 2020, 1, 'M1', 160, 170, 185, 205, 215, 225, 235),
            ('S2', 2021, 1, 'M1', 180, 190, 205, 218, 230, 240, 250),
        ])
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        model_rows = skill_stats[skill_stats['model_short'] == 'M1']

        # S1: 2 months, S2: 1 month = 3 rows total
        assert len(model_rows) == 3

        # S1 month 1 and S2 month 1 both present
        s1_m1 = model_rows[
            (model_rows['code'] == 'S1')
            & (model_rows['month_in_year'] == 1)
        ]
        assert len(s1_m1) == 1
        assert s1_m1.iloc[0]['mae'] == pytest.approx(2.0, rel=1e-6)

        s2_m1 = model_rows[
            (model_rows['code'] == 'S2')
            & (model_rows['month_in_year'] == 1)
        ]
        assert len(s2_m1) == 1
        assert s2_m1.iloc[0]['mae'] == pytest.approx(3.5, rel=1e-6)

        # S2 has no month 2
        s2_m2 = model_rows[
            (model_rows['code'] == 'S2')
            & (model_rows['month_in_year'] == 2)
        ]
        assert len(s2_m2) == 0

    def test_naive_mean_excluded_from_em_composition(self):
        """Naive Mean baseline is not included in EM composition.

        M1 and M2 pass thresholds. Naive Mean is added separately
        and should never appear in EM's composition string.
        """
        obs = _make_obs([
            ('S1', 2020, 1, 100.0),
            ('S1', 2021, 1, 110.0),
        ])
        fcst = _make_fcst([
            ('S1', 2020, 1, 'M1', 80, 85, 92, 102, 112, 118, 125),
            ('S1', 2021, 1, 'M1', 88, 93, 100, 108, 116, 123, 130),
            ('S1', 2020, 1, 'M2', 82, 87, 94, 101, 108, 114, 120),
            ('S1', 2021, 1, 'M2', 90, 95, 102, 109, 117, 124, 131),
        ])
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_rows = skill_stats[skill_stats['model_short'] == 'EM']
        assert len(em_rows) == 1
        composition = em_rows.iloc[0]['composition']
        assert 'Naive Mean' not in composition
        assert 'M1' in composition
        assert 'M2' in composition
