"""Edge case and boundary condition tests for postprocessing_forecasts.

Covers: empty/single-row data, NaN handling, discharge value boundaries,
date boundaries, duplicate handling, threshold behavior, period column
coercion, and code normalization.

Reference: preprocessing_runoff/test/test_edge_cases.py
"""

import os
import sys
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast')
)
sys.path.insert(0, os.path.dirname(__file__))

from src.ensemble_calculator import (
    filter_for_highly_skilled_forecasts,
    create_ensemble_forecasts,
    _is_multi_model_ensemble,
)
from src.gap_detector import detect_missing_ensembles
import forecast_library as fl
import tag_library as tl

from test_constants import MODEL_LONG_NAMES, DEFAULT_THRESHOLDS, DEFAULT_DELTA


def _make_ensemble_pentad(forecasts, skill_stats, observed):
    """Call create_ensemble_forecasts with pentad defaults."""
    return create_ensemble_forecasts(
        forecasts=forecasts,
        skill_stats=skill_stats,
        observed=observed,
        period_col='pentad_in_year',
        period_in_month_col='pentad_in_month',
        get_period_in_month_func=tl.get_pentad,
        calculate_all_metrics_func=fl.calculate_all_skill_metrics,
    )


def _make_skill_row(code, pentad, model_short, sdivsigma=0.3,
                     nse=0.95, accuracy=0.95, delta=5.0,
                     mae=2.0, n_pairs=10):
    """Build a single skill stats row."""
    return {
        'pentad_in_year': pentad,
        'code': code,
        'model_long': MODEL_LONG_NAMES[model_short],
        'model_short': model_short,
        'sdivsigma': sdivsigma,
        'nse': nse,
        'delta': delta,
        'accuracy': accuracy,
        'mae': mae,
        'n_pairs': n_pairs,
    }


def _make_forecast_row(code, date, pentad, pim, model_short, discharge):
    """Build a single forecast row."""
    return {
        'code': code,
        'date': pd.Timestamp(date),
        'pentad_in_year': pentad,
        'pentad_in_month': pim,
        'forecasted_discharge': discharge,
        'model_long': MODEL_LONG_NAMES[model_short],
        'model_short': model_short,
    }


# ---------------------------------------------------------------------------
# TestEmptyAndSingleRowData
# ---------------------------------------------------------------------------
class TestEmptyAndSingleRowData:
    """Empty and single-row inputs should not crash and should produce
    correct schema and row counts."""

    def test_filter_empty_skill_stats(self):
        """Empty skill stats returns empty DF with columns preserved."""
        empty = pd.DataFrame(columns=[
            'pentad_in_year', 'code', 'model_long', 'model_short',
            'sdivsigma', 'nse', 'delta', 'accuracy', 'mae', 'n_pairs',
        ])
        result = filter_for_highly_skilled_forecasts(
            empty, threshold_sdivsigma=0.6,
            threshold_accuracy=0.8, threshold_nse=0.8,
        )
        assert len(result) == 0
        assert 'sdivsigma' in result.columns
        assert 'accuracy' in result.columns
        assert 'nse' in result.columns

    def test_filter_single_passing_row(self):
        """Single row passing all thresholds is returned."""
        df = pd.DataFrame([_make_skill_row('10001', 1, 'LR')])
        result = filter_for_highly_skilled_forecasts(
            df, threshold_sdivsigma=0.6,
            threshold_accuracy=0.8, threshold_nse=0.8,
        )
        assert len(result) == 1
        assert result.iloc[0]['model_short'] == 'LR'

    def test_ensemble_single_model_rejected(self):
        """Single model (LR only) passing threshold -> no EM created."""
        skill = pd.DataFrame([_make_skill_row('10001', 1, 'LR')])
        forecasts = pd.DataFrame([
            _make_forecast_row('10001', '2024-01-05', 1, '1', 'LR', 100.0),
        ])
        observed = pd.DataFrame({
            'code': ['10001'],
            'date': pd.to_datetime(['2024-01-05']),
            'discharge_avg': [105.0],
            'delta': [5.0],
        })
        with patch.dict(os.environ, DEFAULT_THRESHOLDS):
            joint, _ = _make_ensemble_pentad(forecasts, skill, observed)
        assert 'EM' not in joint['model_short'].values, (
            "Single-model ensemble should be discarded"
        )

    def test_gap_detection_single_row(self):
        """Single forecast row (LR, no EM) produces 1 gap."""
        df = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-05']),
            'code': ['10001'],
            'model_short': ['LR'],
        })
        result = detect_missing_ensembles(df, lookback_days=7)
        assert len(result) == 1

    def test_gap_detection_empty_input(self):
        """Empty input returns empty DataFrame with correct schema."""
        df = pd.DataFrame(columns=['date', 'code', 'model_short'])
        result = detect_missing_ensembles(df, lookback_days=7)
        assert result.empty
        assert list(result.columns) == ['date', 'code']


# ---------------------------------------------------------------------------
# TestNaNHandling
# ---------------------------------------------------------------------------
class TestNaNHandling:
    """NaN values in skill metrics and discharge should be handled
    correctly (excluded from filtering and averaging)."""

    def test_filter_nan_sdivsigma_excluded(self):
        """NaN sdivsigma fails the < threshold comparison (NaN < 0.6 is
        False), so all rows are excluded."""
        df = pd.DataFrame([
            _make_skill_row('10001', 1, 'LR', sdivsigma=np.nan),
            _make_skill_row('10001', 1, 'TFT', sdivsigma=np.nan),
            _make_skill_row('10001', 1, 'TiDE', sdivsigma=np.nan),
        ])
        result = filter_for_highly_skilled_forecasts(
            df, threshold_sdivsigma=0.6,
            threshold_accuracy=0.0, threshold_nse=0.0,
        )
        assert len(result) == 0, (
            "NaN sdivsigma should fail the < 0.6 comparison"
        )

    def test_filter_mixed_nan_sdivsigma(self):
        """Only the non-NaN row passes."""
        df = pd.DataFrame([
            _make_skill_row('10001', 1, 'LR', sdivsigma=np.nan),
            _make_skill_row('10001', 1, 'TFT', sdivsigma=0.3),
        ])
        result = filter_for_highly_skilled_forecasts(
            df, threshold_sdivsigma=0.6,
            threshold_accuracy=0.0, threshold_nse=0.0,
        )
        assert len(result) == 1
        assert result.iloc[0]['sdivsigma'] == 0.3

    def test_filter_nan_nse(self):
        """NaN nse fails the > threshold comparison."""
        df = pd.DataFrame([
            _make_skill_row('10001', 1, 'LR', nse=np.nan),
            _make_skill_row('10001', 1, 'TFT', nse=0.95),
        ])
        result = filter_for_highly_skilled_forecasts(
            df, threshold_sdivsigma='False',
            threshold_accuracy='False', threshold_nse=0.8,
        )
        assert len(result) == 1
        assert result.iloc[0]['nse'] == 0.95

    def test_ensemble_all_nan_discharge(self):
        """When all qualifying models have NaN discharge, no EM is created
        (NaN rows are dropped before mean computation)."""
        skill = pd.DataFrame([
            _make_skill_row('10001', 1, 'LR'),
            _make_skill_row('10001', 1, 'TFT'),
        ])
        forecasts = pd.DataFrame([
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'LR', np.nan
            ),
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'TFT', np.nan
            ),
        ])
        observed = pd.DataFrame({
            'code': ['10001'],
            'date': pd.to_datetime(['2024-01-05']),
            'discharge_avg': [105.0],
            'delta': [5.0],
        })
        with patch.dict(os.environ, DEFAULT_THRESHOLDS):
            joint, _ = _make_ensemble_pentad(forecasts, skill, observed)
        assert 'EM' not in joint['model_short'].values, (
            "All-NaN discharge should produce no ensemble"
        )

    def test_all_nan_discharge_preserves_original_rows(self):
        """All NaN discharge: no EM created AND original rows returned
        unchanged (count + values preserved)."""
        skill = pd.DataFrame([
            _make_skill_row('10001', 1, 'LR'),
            _make_skill_row('10001', 1, 'TFT'),
        ])
        forecasts = pd.DataFrame([
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'LR', np.nan
            ),
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'TFT', np.nan
            ),
        ])
        observed = pd.DataFrame({
            'code': ['10001'],
            'date': pd.to_datetime(['2024-01-05']),
            'discharge_avg': [105.0],
            'delta': [5.0],
        })
        with patch.dict(os.environ, DEFAULT_THRESHOLDS):
            joint, skill_out = _make_ensemble_pentad(
                forecasts, skill, observed,
            )
        assert 'EM' not in joint['model_short'].values
        assert len(joint) == 2, (
            f"Original 2 rows must be preserved, got {len(joint)}"
        )
        assert set(joint['model_short'].values) == {'LR', 'TFT'}
        # Discharge values preserved (both NaN)
        assert joint['forecasted_discharge'].isna().all(), (
            "Original NaN discharges must not be altered"
        )
        # Skill stats returned unchanged (no EM appended)
        assert 'EM' not in skill_out['model_short'].values

    def test_multi_station_all_nan_one_valid_other(self):
        """Station A: all NaN discharge -> no EM.
        Station B: valid discharge -> EM created.

        Operational scenario: one station's sensor fails while others work.
        """
        skill = pd.DataFrame([
            _make_skill_row('10001', 1, 'LR'),
            _make_skill_row('10001', 1, 'TFT'),
            _make_skill_row('10002', 1, 'LR'),
            _make_skill_row('10002', 1, 'TFT'),
        ])
        forecasts = pd.DataFrame([
            # Station 10001: all NaN (sensor failure)
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'LR', np.nan
            ),
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'TFT', np.nan
            ),
            # Station 10002: valid data
            _make_forecast_row(
                '10002', '2024-01-05', 1, '1', 'LR', 100.0
            ),
            _make_forecast_row(
                '10002', '2024-01-05', 1, '1', 'TFT', 110.0
            ),
        ])
        observed = pd.DataFrame({
            'code': ['10001', '10002'],
            'date': pd.to_datetime(['2024-01-05', '2024-01-05']),
            'discharge_avg': [105.0, 105.0],
            'delta': [5.0, 5.0],
        })
        with patch.dict(os.environ, DEFAULT_THRESHOLDS):
            joint, _ = _make_ensemble_pentad(
                forecasts, skill, observed,
            )
        em_rows = joint[joint['model_short'] == 'EM']
        assert len(em_rows) == 1, (
            f"Expected 1 EM row (station 10002 only), got {len(em_rows)}"
        )
        assert em_rows.iloc[0]['code'] == '10002'
        assert abs(em_rows.iloc[0]['forecasted_discharge'] - 105.0) < 0.01, (
            "EM = mean(100, 110) = 105.0"
        )
        # Station 10001 original rows preserved
        s1_rows = joint[joint['code'] == '10001']
        assert len(s1_rows) == 2
        assert set(s1_rows['model_short'].values) == {'LR', 'TFT'}
        assert s1_rows['forecasted_discharge'].isna().all()

    def test_ensemble_mixed_nan_discharge(self):
        """LR=100, TFT=NaN, TiDE=90 all pass skill -> EM = mean(100,90) = 95."""
        skill = pd.DataFrame([
            _make_skill_row('10001', 1, 'LR'),
            _make_skill_row('10001', 1, 'TFT'),
            _make_skill_row('10001', 1, 'TiDE'),
        ])
        forecasts = pd.DataFrame([
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'LR', 100.0
            ),
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'TFT', np.nan
            ),
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'TiDE', 90.0
            ),
        ])
        observed = pd.DataFrame({
            'code': ['10001'],
            'date': pd.to_datetime(['2024-01-05']),
            'discharge_avg': [95.0],
            'delta': [5.0],
        })
        with patch.dict(os.environ, DEFAULT_THRESHOLDS):
            joint, _ = _make_ensemble_pentad(forecasts, skill, observed)
        em_rows = joint[joint['model_short'] == 'EM']
        assert len(em_rows) == 1, (
            f"Expected 1 EM row, got {len(em_rows)}"
        )
        em_discharge = em_rows.iloc[0]['forecasted_discharge']
        assert abs(em_discharge - 95.0) < 0.01, (
            f"EM should be mean(100, 90) = 95.0, got {em_discharge}"
        )


# ---------------------------------------------------------------------------
# TestDischargeValueBoundaries
# ---------------------------------------------------------------------------
class TestDischargeValueBoundaries:
    """Boundary values for forecasted discharge: zero, near-zero, large."""

    @pytest.fixture
    def _two_model_skill(self):
        """Two models (LR, TFT) both passing thresholds for code 10001."""
        return pd.DataFrame([
            _make_skill_row('10001', 1, 'LR'),
            _make_skill_row('10001', 1, 'TFT'),
        ])

    @pytest.fixture
    def _observed(self):
        return pd.DataFrame({
            'code': ['10001'],
            'date': pd.to_datetime(['2024-01-05']),
            'discharge_avg': [0.0],
            'delta': [5.0],
        })

    def _run_ensemble(self, lr_val, tft_val, skill, observed):
        forecasts = pd.DataFrame([
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'LR', lr_val
            ),
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'TFT', tft_val
            ),
        ])
        with patch.dict(os.environ, DEFAULT_THRESHOLDS):
            joint, _ = _make_ensemble_pentad(forecasts, skill, observed)
        return joint[joint['model_short'] == 'EM']

    def test_zero_discharge_ensemble(self, _two_model_skill, _observed):
        """LR=0.0, TFT=0.0 -> EM = 0.0."""
        em = self._run_ensemble(0.0, 0.0, _two_model_skill, _observed)
        assert len(em) == 1
        assert em.iloc[0]['forecasted_discharge'] == 0.0

    def test_near_zero_discharge(self, _two_model_skill, _observed):
        """LR=0.001, TFT=0.002 -> EM = 0.0015."""
        em = self._run_ensemble(0.001, 0.002, _two_model_skill, _observed)
        assert len(em) == 1
        assert abs(em.iloc[0]['forecasted_discharge'] - 0.0015) < 1e-6

    def test_very_large_discharge(self, _two_model_skill, _observed):
        """LR=10000.0, TFT=12000.0 -> EM = 11000.0."""
        em = self._run_ensemble(
            10000.0, 12000.0, _two_model_skill, _observed
        )
        assert len(em) == 1
        assert abs(
            em.iloc[0]['forecasted_discharge'] - 11000.0
        ) < 0.01

    def test_negative_discharge_ensemble(self, _two_model_skill, _observed):
        """Negative discharge: LR=-5.0, TFT=10.0 -> EM = 2.5.

        Negative discharge is physically meaningless but should not crash.
        Models may produce negative values during low-flow extrapolation.
        """
        em = self._run_ensemble(-5.0, 10.0, _two_model_skill, _observed)
        assert len(em) == 1
        assert abs(em.iloc[0]['forecasted_discharge'] - 2.5) < 0.01

    def test_identical_discharge_all_models(self):
        """LR=TFT=TiDE=105.0 (3 models pass) -> EM = 105.0."""
        skill = pd.DataFrame([
            _make_skill_row('10001', 1, 'LR'),
            _make_skill_row('10001', 1, 'TFT'),
            _make_skill_row('10001', 1, 'TiDE'),
        ])
        forecasts = pd.DataFrame([
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'LR', 105.0
            ),
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'TFT', 105.0
            ),
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'TiDE', 105.0
            ),
        ])
        observed = pd.DataFrame({
            'code': ['10001'],
            'date': pd.to_datetime(['2024-01-05']),
            'discharge_avg': [105.0],
            'delta': [5.0],
        })
        with patch.dict(os.environ, DEFAULT_THRESHOLDS):
            joint, _ = _make_ensemble_pentad(forecasts, skill, observed)
        em = joint[joint['model_short'] == 'EM']
        assert len(em) == 1
        assert em.iloc[0]['forecasted_discharge'] == 105.0


# ---------------------------------------------------------------------------
# TestDateBoundaries
# ---------------------------------------------------------------------------
class TestDateBoundaries:
    """Date boundaries: year transitions, leap years, month boundaries."""

    def _make_data_for_dates(self, dates, pentads, pims, station='10001'):
        """Build forecasts, skill, observed for given dates."""
        frows, srows, orows = [], [], []
        for date, pentad, pim in zip(dates, pentads, pims):
            for ms in ('LR', 'TFT'):
                frows.append(_make_forecast_row(
                    station, date, pentad, pim, ms,
                    100.0 if ms == 'LR' else 110.0,
                ))
                srows.append(_make_skill_row(station, pentad, ms))
            orows.append({
                'code': station, 'date': pd.Timestamp(date),
                'discharge_avg': 105.0, 'delta': 5.0,
            })
        return (
            pd.DataFrame(frows),
            pd.DataFrame(srows),
            pd.DataFrame(orows),
        )

    def test_year_boundary_dec31_jan1(self):
        """Forecasts for 2025-12-31 and 2026-01-05 both get EM rows."""
        dates = ['2025-12-31', '2026-01-05']
        pentads = [72, 1]
        pims = ['6', '1']
        forecasts, skill, observed = self._make_data_for_dates(
            dates, pentads, pims,
        )
        with patch.dict(os.environ, DEFAULT_THRESHOLDS):
            joint, _ = _make_ensemble_pentad(forecasts, skill, observed)
        em = joint[joint['model_short'] == 'EM']
        assert len(em) == 2, f"Expected 2 EM rows, got {len(em)}"
        em_dates = sorted(em['date'].dt.strftime('%Y-%m-%d').tolist())
        assert em_dates == ['2025-12-31', '2026-01-05']

    def test_leap_year_feb29(self):
        """Forecast for 2024-02-29 creates EM with date preserved."""
        dates = ['2024-02-29', '2024-03-05']
        pentads = [12, 13]
        pims = ['6', '1']
        forecasts, skill, observed = self._make_data_for_dates(
            dates, pentads, pims,
        )
        with patch.dict(os.environ, DEFAULT_THRESHOLDS):
            joint, _ = _make_ensemble_pentad(forecasts, skill, observed)
        em = joint[joint['model_short'] == 'EM']
        assert len(em) == 2
        em_dates = em['date'].tolist()
        assert pd.Timestamp('2024-02-29') in em_dates

    def test_gap_detection_across_year_boundary(self):
        """Dec 31 has EM, Jan 5 doesn't -> gap on Jan 5."""
        df = pd.DataFrame({
            'date': pd.to_datetime([
                '2025-12-31', '2025-12-31', '2025-12-31',
                '2026-01-05', '2026-01-05',
            ]),
            'code': ['10001'] * 5,
            'model_short': ['LR', 'TFT', 'EM', 'LR', 'TFT'],
        })
        gaps = detect_missing_ensembles(df, lookback_days=10)
        assert len(gaps) == 1
        assert gaps.iloc[0]['date'] == pd.Timestamp('2026-01-05')

    def test_month_boundary_pentads(self):
        """Jan 31 (pentad 6) + Feb 5 (pentad 7) -> correct pentad numbers."""
        dates = ['2026-01-31', '2026-02-05']
        pentads = [6, 7]
        pims = ['6', '1']
        forecasts, skill, observed = self._make_data_for_dates(
            dates, pentads, pims,
        )
        with patch.dict(os.environ, DEFAULT_THRESHOLDS):
            joint, _ = _make_ensemble_pentad(forecasts, skill, observed)
        em = joint[joint['model_short'] == 'EM']
        assert len(em) == 2
        em_pentads = sorted(em['pentad_in_year'].tolist())
        assert em_pentads == [6, 7]


# ---------------------------------------------------------------------------
# TestDuplicateHandling
# ---------------------------------------------------------------------------
class TestDuplicateHandling:
    """Duplicate rows should not crash and should be handled gracefully."""

    def test_duplicate_forecast_rows(self):
        """Same (date, code, model) twice: groupby mean includes both."""
        skill = pd.DataFrame([
            _make_skill_row('10001', 1, 'LR'),
            _make_skill_row('10001', 1, 'TFT'),
        ])
        forecasts = pd.DataFrame([
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'LR', 100.0
            ),
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'LR', 120.0
            ),
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'TFT', 110.0
            ),
        ])
        observed = pd.DataFrame({
            'code': ['10001'],
            'date': pd.to_datetime(['2024-01-05']),
            'discharge_avg': [105.0],
            'delta': [5.0],
        })
        with patch.dict(os.environ, DEFAULT_THRESHOLDS):
            joint, _ = _make_ensemble_pentad(forecasts, skill, observed)
        em = joint[joint['model_short'] == 'EM']
        # Should not crash; EM should be created
        assert len(em) == 1, "Duplicates should not prevent EM creation"

    def test_duplicate_skill_stats_rows(self):
        """Duplicate (pentad, code, model) in skill_stats: drop_duplicates
        in merge prevents double-counting."""
        skill = pd.DataFrame([
            _make_skill_row('10001', 1, 'LR'),
            _make_skill_row('10001', 1, 'LR'),  # duplicate
            _make_skill_row('10001', 1, 'TFT'),
        ])
        forecasts = pd.DataFrame([
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'LR', 100.0
            ),
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'TFT', 110.0
            ),
        ])
        observed = pd.DataFrame({
            'code': ['10001'],
            'date': pd.to_datetime(['2024-01-05']),
            'discharge_avg': [105.0],
            'delta': [5.0],
        })
        with patch.dict(os.environ, DEFAULT_THRESHOLDS):
            joint, _ = _make_ensemble_pentad(forecasts, skill, observed)
        em = joint[joint['model_short'] == 'EM']
        assert len(em) == 1, "Duplicate skill rows should not prevent EM"
        # EM should be mean of LR and TFT, not double-counted LR
        assert abs(
            em.iloc[0]['forecasted_discharge'] - 105.0
        ) < 0.01, "EM = mean(100, 110) = 105"

    def test_duplicate_in_gap_detection(self):
        """Duplicate (date, code) rows: each unique pair counted once."""
        df = pd.DataFrame({
            'date': pd.to_datetime([
                '2024-01-05', '2024-01-05', '2024-01-05',
            ]),
            'code': ['10001', '10001', '10001'],
            'model_short': ['LR', 'LR', 'TFT'],
        })
        gaps = detect_missing_ensembles(df, lookback_days=7)
        # Only 1 unique (date, code) pair, and it has no EM
        assert len(gaps) == 1


# ---------------------------------------------------------------------------
# TestThresholdBehavior
# ---------------------------------------------------------------------------
class TestThresholdBehavior:
    """Threshold string parsing, case sensitivity, and boundary behavior."""

    @pytest.fixture
    def _three_model_skill(self):
        """Three models with sdivsigma=0.9 (would normally fail)."""
        return pd.DataFrame([
            _make_skill_row('10001', 1, 'LR', sdivsigma=0.9),
            _make_skill_row('10001', 1, 'TFT', sdivsigma=0.9),
            _make_skill_row('10001', 1, 'TiDE', sdivsigma=0.9),
        ])

    def test_threshold_disabled_with_string_False(self, _three_model_skill):
        """threshold_sdivsigma='False' disables the sdivsigma filter."""
        result = filter_for_highly_skilled_forecasts(
            _three_model_skill,
            threshold_sdivsigma='False',
            threshold_accuracy=0.0,
            threshold_nse=0.0,
        )
        assert len(result) == 3, "All should pass when filter disabled"

    def test_threshold_case_sensitive(self, _three_model_skill):
        """threshold_sdivsigma='false' (lowercase) raises ValueError.
        Only 'False' (capital F) disables the filter. This documents
        the case-sensitivity behavior: str('false') != 'False' so it
        tries float('false') which is invalid."""
        with pytest.raises(ValueError, match="could not convert"):
            filter_for_highly_skilled_forecasts(
                _three_model_skill,
                threshold_sdivsigma='false',
                threshold_accuracy=0.0,
                threshold_nse=0.0,
            )

    def test_exact_boundary_excluded(self):
        """sdivsigma exactly at threshold (0.6) is excluded (< not <=)."""
        df = pd.DataFrame([
            _make_skill_row('10001', 1, 'LR', sdivsigma=0.6),
        ])
        result = filter_for_highly_skilled_forecasts(
            df, threshold_sdivsigma=0.6,
            threshold_accuracy=0.0, threshold_nse=0.0,
        )
        assert len(result) == 0, (
            "sdivsigma=0.6 should be excluded (< 0.6 is False)"
        )

    def test_all_thresholds_must_pass(self):
        """Model passes sdivsigma and nse but fails accuracy -> excluded."""
        df = pd.DataFrame([
            _make_skill_row(
                '10001', 1, 'LR',
                sdivsigma=0.3,   # passes (< 0.6)
                nse=0.95,        # passes (> 0.8)
                accuracy=0.7,    # FAILS (not > 0.8)
            ),
        ])
        result = filter_for_highly_skilled_forecasts(
            df, threshold_sdivsigma=0.6,
            threshold_accuracy=0.8, threshold_nse=0.8,
        )
        assert len(result) == 0, (
            "Failing any single threshold should exclude the model"
        )

    def test_custom_relaxed_thresholds_include_all(self):
        """Relaxed thresholds (sdivsigma=1.0, accuracy=0.5, nse=0.4)
        include TiDE that normally fails default thresholds."""
        df = pd.DataFrame([
            _make_skill_row('10001', 1, 'LR', sdivsigma=0.3, nse=0.95,
                            accuracy=0.95),
            _make_skill_row('10001', 1, 'TFT', sdivsigma=0.4, nse=0.9,
                            accuracy=0.88),
            _make_skill_row('10001', 1, 'TiDE', sdivsigma=0.9, nse=0.5,
                            accuracy=0.6),
        ])
        result = filter_for_highly_skilled_forecasts(
            df, threshold_sdivsigma=1.0,
            threshold_accuracy=0.5, threshold_nse=0.4,
        )
        assert len(result) == 3, (
            "Relaxed thresholds should include all 3 models"
        )
        assert set(result['model_short']) == {'LR', 'TFT', 'TiDE'}

    def test_custom_strict_sdivsigma_only(self):
        """Very strict sdivsigma (0.2) with relaxed others: only LR passes
        (sdivsigma=0.15), TFT (0.4) excluded."""
        df = pd.DataFrame([
            _make_skill_row('10001', 1, 'LR', sdivsigma=0.15, nse=0.85,
                            accuracy=0.82),
            _make_skill_row('10001', 1, 'TFT', sdivsigma=0.4, nse=0.9,
                            accuracy=0.88),
        ])
        result = filter_for_highly_skilled_forecasts(
            df, threshold_sdivsigma=0.2,
            threshold_accuracy=0.0, threshold_nse=0.0,
        )
        assert len(result) == 1
        assert result.iloc[0]['model_short'] == 'LR'

    def test_custom_thresholds_via_env_vars(self):
        """Thresholds set via environment variables (not parameters)."""
        df = pd.DataFrame([
            _make_skill_row('10001', 1, 'LR', sdivsigma=0.3, nse=0.95,
                            accuracy=0.95),
            _make_skill_row('10001', 1, 'TFT', sdivsigma=0.4, nse=0.9,
                            accuracy=0.88),
            _make_skill_row('10001', 1, 'TiDE', sdivsigma=0.9, nse=0.5,
                            accuracy=0.6),
        ])
        custom_env = {
            'ieasyhydroforecast_efficiency_threshold': '1.0',
            'ieasyhydroforecast_accuracy_threshold': '0.5',
            'ieasyhydroforecast_nse_threshold': '0.4',
        }
        with patch.dict(os.environ, custom_env):
            result = filter_for_highly_skilled_forecasts(df)
        assert len(result) == 3, (
            "Relaxed env-var thresholds should include all models"
        )

    def test_custom_thresholds_change_ensemble_composition(self):
        """Relaxed thresholds include TiDE -> EM = mean(LR, TFT, TiDE)
        instead of default EM = mean(LR, TFT)."""
        skill = pd.DataFrame([
            _make_skill_row('10001', 1, 'LR', sdivsigma=0.3, nse=0.95,
                            accuracy=0.95),
            _make_skill_row('10001', 1, 'TFT', sdivsigma=0.4, nse=0.9,
                            accuracy=0.88),
            _make_skill_row('10001', 1, 'TiDE', sdivsigma=0.9, nse=0.5,
                            accuracy=0.6),
        ])
        forecasts = pd.DataFrame([
            _make_forecast_row('10001', '2024-01-05', 1, '1', 'LR', 100.0),
            _make_forecast_row('10001', '2024-01-05', 1, '1', 'TFT', 110.0),
            _make_forecast_row('10001', '2024-01-05', 1, '1', 'TiDE', 90.0),
        ])
        observed = pd.DataFrame({
            'code': ['10001'],
            'date': pd.to_datetime(['2024-01-05']),
            'discharge_avg': [100.0],
            'delta': [5.0],
        })
        relaxed_env = {
            'ieasyhydroforecast_efficiency_threshold': '1.0',
            'ieasyhydroforecast_accuracy_threshold': '0.5',
            'ieasyhydroforecast_nse_threshold': '0.4',
        }
        with patch.dict(os.environ, relaxed_env):
            joint, skill_out = _make_ensemble_pentad(
                forecasts, skill, observed,
            )
        em_rows = joint[joint['model_short'] == 'EM']
        assert len(em_rows) == 1
        # EM = mean(100, 110, 90) = 100.0 (3 models, not 2)
        assert abs(em_rows.iloc[0]['forecasted_discharge'] - 100.0) < 0.01, (
            "With relaxed thresholds, EM should include TiDE: "
            f"mean(100, 110, 90) = 100.0, got "
            f"{em_rows.iloc[0]['forecasted_discharge']}"
        )
        # Composition string should include all 3 models
        comp = em_rows.iloc[0]['model_long']
        assert 'LR' in comp and 'TFT' in comp and 'TiDE' in comp, (
            f"Composition should include all 3 models, got: {comp}"
        )

    def test_mixed_threshold_disabling(self):
        """Disable sdivsigma but keep strict accuracy+nse: only models
        with good accuracy and nse pass regardless of sdivsigma."""
        df = pd.DataFrame([
            _make_skill_row('10001', 1, 'LR', sdivsigma=0.9, nse=0.95,
                            accuracy=0.95),   # bad sdivsigma, good others
            _make_skill_row('10001', 1, 'TFT', sdivsigma=0.8, nse=0.9,
                            accuracy=0.88),   # bad sdivsigma, good others
            _make_skill_row('10001', 1, 'TiDE', sdivsigma=0.1, nse=0.5,
                            accuracy=0.6),    # good sdivsigma, bad others
        ])
        result = filter_for_highly_skilled_forecasts(
            df, threshold_sdivsigma='False',
            threshold_accuracy=0.8, threshold_nse=0.8,
        )
        assert len(result) == 2, (
            "sdivsigma disabled: LR+TFT pass on accuracy+nse, TiDE fails"
        )
        assert set(result['model_short']) == {'LR', 'TFT'}


# ---------------------------------------------------------------------------
# TestPeriodColCoercion
# ---------------------------------------------------------------------------
class TestPeriodColCoercion:
    """Period column type coercion: string<->int merges, non-numeric values."""

    def test_string_period_values_coerced(self):
        """forecasts pentad_in_year='1' (str), skill pentad_in_year=1 (int)
        -> merge succeeds via pd.to_numeric coercion, EM created."""
        skill = pd.DataFrame([
            _make_skill_row('10001', 1, 'LR'),
            _make_skill_row('10001', 1, 'TFT'),
        ])
        forecasts = pd.DataFrame([
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'LR', 100.0
            ),
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'TFT', 110.0
            ),
        ])
        # Force pentad_in_year to string in forecasts
        forecasts['pentad_in_year'] = forecasts[
            'pentad_in_year'
        ].astype(str)

        observed = pd.DataFrame({
            'code': ['10001'],
            'date': pd.to_datetime(['2024-01-05']),
            'discharge_avg': [105.0],
            'delta': [5.0],
        })
        with patch.dict(os.environ, DEFAULT_THRESHOLDS):
            joint, _ = _make_ensemble_pentad(forecasts, skill, observed)
        assert 'EM' in joint['model_short'].values, (
            "String period values should be coerced to numeric for merge"
        )

    def test_non_numeric_period_becomes_nan(self):
        """pentad_in_year='abc' -> pd.to_numeric coerces to NaN,
        no merge match, no EM."""
        skill = pd.DataFrame([
            _make_skill_row('10001', 1, 'LR'),
            _make_skill_row('10001', 1, 'TFT'),
        ])
        forecasts = pd.DataFrame([
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'LR', 100.0
            ),
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'TFT', 110.0
            ),
        ])
        # Set non-numeric period
        forecasts['pentad_in_year'] = 'abc'

        observed = pd.DataFrame({
            'code': ['10001'],
            'date': pd.to_datetime(['2024-01-05']),
            'discharge_avg': [105.0],
            'delta': [5.0],
        })
        with patch.dict(os.environ, DEFAULT_THRESHOLDS):
            joint, _ = _make_ensemble_pentad(forecasts, skill, observed)
        assert 'EM' not in joint['model_short'].values, (
            "Non-numeric period should produce NaN -> no merge match"
        )

    def test_code_type_mismatch_resolved(self):
        """forecasts code='10001' (str), skill code=10001 (int)
        -> astype(str) resolves, EM created."""
        skill = pd.DataFrame([
            _make_skill_row(10001, 1, 'LR'),  # int code
            _make_skill_row(10001, 1, 'TFT'),
        ])
        forecasts = pd.DataFrame([
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'LR', 100.0
            ),
            _make_forecast_row(
                '10001', '2024-01-05', 1, '1', 'TFT', 110.0
            ),
        ])
        observed = pd.DataFrame({
            'code': ['10001'],
            'date': pd.to_datetime(['2024-01-05']),
            'discharge_avg': [105.0],
            'delta': [5.0],
        })
        with patch.dict(os.environ, DEFAULT_THRESHOLDS):
            joint, _ = _make_ensemble_pentad(forecasts, skill, observed)
        assert 'EM' in joint['model_short'].values, (
            "Code type mismatch (int vs str) should be resolved by astype"
        )


# ---------------------------------------------------------------------------
# TestCodeNormalization
# ---------------------------------------------------------------------------
class TestCodeNormalization:
    """Code normalization: float .0 stripping, roundtrip consistency."""

    def test_numeric_code_dot_zero_stripped(self, tmp_path):
        """code=10001.0 (float in CSV) becomes '10001' after reading."""
        from src.gap_detector import read_combined_forecasts

        csv_file = tmp_path / "combined_pentad.csv"
        pd.DataFrame({
            'date': ['2024-01-05'],
            'code': [10001.0],  # float code
            'model_short': ['LR'],
            'forecasted_discharge': [100.0],
        }).to_csv(csv_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_combined_forecast_pentad_file':
                'combined_pentad.csv',
        }):
            result = read_combined_forecasts('pentad')
        assert result.iloc[0]['code'] == '10001', (
            "Float code 10001.0 should be normalized to '10001'"
        )

    def test_code_roundtrip_consistency(self, tmp_path):
        """Write code='10001' to CSV, read back, verify match."""
        from src.gap_detector import read_combined_forecasts

        csv_file = tmp_path / "combined_pentad.csv"
        pd.DataFrame({
            'date': ['2024-01-05'],
            'code': ['10001'],
            'model_short': ['LR'],
            'forecasted_discharge': [100.0],
        }).to_csv(csv_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_combined_forecast_pentad_file':
                'combined_pentad.csv',
        }):
            result = read_combined_forecasts('pentad')
        assert result.iloc[0]['code'] == '10001'


# ---------------------------------------------------------------------------
# TestDeltaEdgeCases
# ---------------------------------------------------------------------------
class TestDeltaEdgeCases:
    """Edge cases for delta values in skill metrics and ensemble creation.

    Delta represents the measurement uncertainty tolerance band (m^3/s).
    accuracy = fraction of forecasts within observed +/- delta.
    """

    def test_nan_delta_excluded_from_skill_metrics(self):
        """NaN delta: calculate_all_skill_metrics filters row via NaN mask.

        Row 0: delta=NaN -> filtered
        Row 1-2: valid -> n_pairs=2, metrics computed from those only.
        """
        data = pd.DataFrame({
            'obs': [100.0, 110.0, 105.0],
            'sim': [102.0, 108.0, 106.0],
            'delta': [np.nan, 5.0, 5.0],
        })
        result = fl.calculate_all_skill_metrics(
            data, 'obs', 'sim', 'delta'
        )
        assert result['n_pairs'] == 2, (
            "NaN delta row should be filtered, leaving 2 valid pairs"
        )
        # MAE from valid rows: mean(|110-108|, |105-106|) = mean(2, 1) = 1.5
        assert abs(result['mae'] - 1.5) < 1e-10
        # Both within delta=5: accuracy = 1.0
        assert result['accuracy'] == 1.0

    def test_zero_delta_strict_matching(self):
        """delta=0: only exact matches count as within tolerance.

        obs = [100, 110], sim = [100, 108]
        abs_diff = [0, 2]
        abs_diff <= 0 -> [True, False] -> accuracy = 0.5
        """
        data = pd.DataFrame({
            'obs': [100.0, 110.0],
            'sim': [100.0, 108.0],
            'delta': [0.0, 0.0],
        })
        result = fl.calculate_all_skill_metrics(
            data, 'obs', 'sim', 'delta'
        )
        assert result['n_pairs'] == 2
        assert result['accuracy'] == 0.5, (
            "delta=0: only exact match (row 0) counts, accuracy = 0.5"
        )
        assert result['delta'] == 0.0

    def test_zero_delta_all_exact(self):
        """delta=0 with perfect forecasts: accuracy = 1.0."""
        data = pd.DataFrame({
            'obs': [100.0, 110.0],
            'sim': [100.0, 110.0],
            'delta': [0.0, 0.0],
        })
        result = fl.calculate_all_skill_metrics(
            data, 'obs', 'sim', 'delta'
        )
        assert result['accuracy'] == 1.0
        assert result['mae'] == 0.0

    def test_negative_delta_produces_nan(self):
        """Negative delta: fails validation, accuracy and delta -> NaN.

        delta < 0 is physically meaningless. The function validates
        0 <= delta < inf; negative delta fails this check, so both
        accuracy and delta are set to NaN. Does not crash.
        """
        data = pd.DataFrame({
            'obs': [100.0, 110.0],
            'sim': [100.0, 110.0],  # perfect forecast
            'delta': [-5.0, -5.0],
        })
        result = fl.calculate_all_skill_metrics(
            data, 'obs', 'sim', 'delta'
        )
        assert result['n_pairs'] == 2
        assert np.isnan(result['delta']), (
            "Negative delta fails 0 <= delta validation -> NaN"
        )
        assert np.isnan(result['accuracy']), (
            "When delta is invalid, accuracy is also set to NaN"
        )
        # MAE is independent of delta and should still be computed
        assert result['mae'] == 0.0

    def test_varying_delta_per_row(self):
        """Different delta values per row (realistic: delta depends on flow).

        obs = [100, 500], sim = [108, 490], delta = [5, 20]
        abs_diff = [8, 10]
        within delta: [8<=5 -> False, 10<=20 -> True] -> accuracy = 0.5
        """
        data = pd.DataFrame({
            'obs': [100.0, 500.0],
            'sim': [108.0, 490.0],
            'delta': [5.0, 20.0],
        })
        result = fl.calculate_all_skill_metrics(
            data, 'obs', 'sim', 'delta'
        )
        assert result['n_pairs'] == 2
        assert result['accuracy'] == 0.5
        # delta is the last row's value
        assert result['delta'] == 20.0
        # MAE = mean(8, 10) = 9.0
        assert result['mae'] == 9.0


# ---------------------------------------------------------------------------
# TestNaTDatesInGapDetector
# ---------------------------------------------------------------------------
class TestNaTDatesInGapDetector:
    """NaT dates in gap detection should not crash."""

    def test_nat_dates_dropped_gracefully(self):
        """Combined forecasts with NaT dates: NaT rows ignored."""
        df = pd.DataFrame({
            'date': pd.to_datetime(
                ['2024-01-05', pd.NaT, '2024-01-10']
            ),
            'code': ['10001', '10001', '10001'],
            'model_short': ['LR', 'TFT', 'EM'],
        })
        # Should not crash even though one date is NaT
        gaps = detect_missing_ensembles(df, lookback_days=7)
        # Only 2024-01-05 has models but no EM for that date
        # 2024-01-10 has EM, NaT row is malformed
        assert isinstance(gaps, pd.DataFrame)

    def test_all_nat_dates_returns_empty(self):
        """All NaT dates -> empty gap result."""
        df = pd.DataFrame({
            'date': pd.to_datetime([pd.NaT, pd.NaT]),
            'code': ['10001', '10001'],
            'model_short': ['LR', 'TFT'],
        })
        gaps = detect_missing_ensembles(df, lookback_days=7)
        assert gaps.empty


# ---------------------------------------------------------------------------
# TestMissingRequiredColumns
# ---------------------------------------------------------------------------
class TestMissingRequiredColumns:
    """Missing required columns should not crash with KeyError."""

    def test_gap_detector_missing_model_short_raises(self):
        """gap_detector with missing model_short column raises KeyError.

        Documents current behavior: detect_missing_ensembles requires
        model_short column and raises KeyError if missing.
        """
        df = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-05']),
            'code': ['10001'],
            # model_short missing
        })
        with pytest.raises(KeyError):
            detect_missing_ensembles(df, lookback_days=7)
