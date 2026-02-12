"""Tests for src/gap_detector.py — find missing ensemble forecasts."""

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.gap_detector import (
    detect_missing_ensembles,
    read_combined_forecasts,
)


# ---------------------------------------------------------------------------
# detect_missing_ensembles tests
# ---------------------------------------------------------------------------

class TestDetectMissingEnsembles:
    def test_no_gaps(self):
        """All (date, code) pairs have EM — no gaps detected."""
        df = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-05'] * 3),
            'code': ['10001'] * 3,
            'model_short': ['LR', 'TFT', 'EM'],
        })
        result = detect_missing_ensembles(df, lookback_days=7)
        assert result.empty

    def test_missing_em(self):
        """One (date, code) pair has LR+TFT but no EM."""
        df = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-05'] * 2),
            'code': ['10001'] * 2,
            'model_short': ['LR', 'TFT'],
        })
        result = detect_missing_ensembles(df, lookback_days=7)
        assert len(result) == 1
        assert result.iloc[0]['code'] == '10001'

    def test_lookback_window(self):
        """Only dates within lookback window are checked."""
        dates = pd.to_datetime(['2024-01-01', '2024-01-01',
                                '2024-01-10', '2024-01-10'])
        df = pd.DataFrame({
            'date': dates,
            'code': ['10001'] * 4,
            'model_short': ['LR', 'TFT', 'LR', 'TFT'],
        })
        # lookback=3 from max date 2024-01-10 => cutoff 2024-01-07
        # Only 2024-01-10 is in window
        result = detect_missing_ensembles(df, lookback_days=3)
        assert len(result) == 1
        assert result.iloc[0]['date'] == pd.Timestamp('2024-01-10')

    def test_multi_code_gaps(self):
        """Gaps detected independently per code."""
        df = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-05'] * 5),
            'code': ['10001', '10001', '10001', '10002', '10002'],
            'model_short': ['LR', 'TFT', 'EM', 'LR', 'TFT'],
        })
        result = detect_missing_ensembles(df, lookback_days=7)
        assert len(result) == 1
        assert result.iloc[0]['code'] == '10002'

    def test_empty_input(self):
        """Empty input returns empty DataFrame."""
        df = pd.DataFrame(columns=['date', 'code', 'model_short'])
        result = detect_missing_ensembles(df, lookback_days=7)
        assert result.empty
        assert list(result.columns) == ['date', 'code']

    def test_string_dates_converted(self):
        """String dates are properly converted to datetime."""
        df = pd.DataFrame({
            'date': ['2024-01-05', '2024-01-05'],
            'code': ['10001', '10001'],
            'model_short': ['LR', 'TFT'],
        })
        result = detect_missing_ensembles(df, lookback_days=7)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# read_combined_forecasts tests
# ---------------------------------------------------------------------------

class TestReadCombinedForecasts:
    def test_invalid_horizon_type_raises(self):
        with pytest.raises(ValueError, match="'pentad' or 'decad'"):
            read_combined_forecasts('weekly')

    def test_reads_pentad_csv(self, tmp_path):
        csv_file = tmp_path / "combined_pentad.csv"
        pd.DataFrame({
            'date': ['2024-01-05'],
            'code': [10001],
            'model_short': ['LR'],
            'forecasted_discharge': [100.0],
        }).to_csv(csv_file, index=False)

        with pytest.MonkeyPatch.context() as mp:
            mp.setenv(
                'ieasyforecast_intermediate_data_path', str(tmp_path)
            )
            mp.setenv(
                'ieasyforecast_combined_forecast_pentad_file',
                'combined_pentad.csv',
            )
            result = read_combined_forecasts('pentad')
            assert len(result) == 1
            assert result['code'].iloc[0] == '10001'
            assert pd.api.types.is_datetime64_any_dtype(result['date'])

    def test_missing_file_returns_empty(self, tmp_path):
        with pytest.MonkeyPatch.context() as mp:
            mp.setenv(
                'ieasyforecast_intermediate_data_path', str(tmp_path)
            )
            mp.setenv(
                'ieasyforecast_combined_forecast_pentad_file',
                'nonexistent.csv',
            )
            result = read_combined_forecasts('pentad')
            assert result.empty
