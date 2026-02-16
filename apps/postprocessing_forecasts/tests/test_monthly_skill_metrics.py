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

        Month 1: q50=[102, 108], obs=[100, 110]
        MAE = mean(|100-102|, |110-108|) = 2.0
        """
        obs, fcst = basic_data
        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        row = skill_stats[
            (skill_stats['month_in_year'] == 1)
            & (skill_stats['model_short'] == 'M1')
        ].iloc[0]
        assert row['mae'] == pytest.approx(2.0, rel=1e-6)
        assert row['n_pairs'] == 2

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
        """Stations get independent metrics."""
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

    def test_nan_quantiles_point_metrics_still_computed(self):
        """NaN quantiles don't break point metrics (q50 still valid)."""
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
