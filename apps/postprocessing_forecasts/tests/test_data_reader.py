"""Tests for src/data_reader.py — reading pre-calculated skill metrics."""

import os
import sys
import tempfile
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

# Ensure the postprocessing_forecasts package is importable
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..')
)

from src.data_reader import (
    read_skill_metrics,
    _read_skill_metrics_csv,
    _normalize_api_skill_metrics,
    MODEL_SHORT_TO_LONG,
)


class TestReadSkillMetricsCsv:
    """Tests for CSV-based skill metrics reading."""

    def test_reads_pentad_csv(self, tmp_path):
        """CSV with correct columns is read and returned."""
        csv_file = tmp_path / "pentad_skill.csv"
        df = pd.DataFrame({
            'pentad_in_year': [1, 2],
            'code': ['10001', '10002'],
            'model_long': ['Linear regression (LR)', 'Linear regression (LR)'],
            'model_short': ['LR', 'LR'],
            'sdivsigma': [0.3, 0.4],
            'nse': [0.9, 0.85],
            'delta': [5.0, 6.0],
            'accuracy': [0.95, 0.88],
            'mae': [2.1, 3.2],
            'n_pairs': [10, 12],
        })
        df.to_csv(csv_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_pentadal_skill_metrics_file': 'pentad_skill.csv',
        }):
            result = _read_skill_metrics_csv('pentad')
            assert result is not None
            assert len(result) == 2
            assert result['code'].dtype == object  # string
            assert result.iloc[0]['sdivsigma'] == 0.3
            assert result.iloc[0]['code'] == '10001'
            assert result.iloc[0]['pentad_in_year'] == 1

    def test_reads_decad_csv(self, tmp_path):
        """Decadal CSV is read using the correct env var."""
        csv_file = tmp_path / "decad_skill.csv"
        df = pd.DataFrame({
            'decad_in_year': [1],
            'code': [10001],  # numeric code, should become string
            'model_short': ['LR'],
            'sdivsigma': [0.3],
            'nse': [0.9],
        })
        df.to_csv(csv_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_decadal_skill_metrics_file': 'decad_skill.csv',
        }):
            result = _read_skill_metrics_csv('decad')
            assert result is not None
            assert result['code'].iloc[0] == '10001'
            assert result.iloc[0]['sdivsigma'] == 0.3
            assert result.iloc[0]['decad_in_year'] == 1

    def test_missing_env_vars_returns_none(self):
        """Returns None when env vars are not set."""
        with patch.dict(os.environ, {}, clear=True):
            # Clear relevant vars
            for key in ['ieasyforecast_intermediate_data_path',
                        'ieasyforecast_pentadal_skill_metrics_file']:
                os.environ.pop(key, None)
            result = _read_skill_metrics_csv('pentad')
            assert result is None

    def test_missing_file_returns_none(self, tmp_path):
        """Returns None when the CSV file doesn't exist."""
        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_pentadal_skill_metrics_file': 'nonexistent.csv',
        }):
            result = _read_skill_metrics_csv('pentad')
            assert result is None

    def test_empty_csv_returns_empty_df(self, tmp_path):
        """CSV with headers but no rows returns empty DataFrame."""
        csv_file = tmp_path / "empty_skill.csv"
        pd.DataFrame(columns=[
            'pentad_in_year', 'code', 'model_short', 'sdivsigma'
        ]).to_csv(csv_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_pentadal_skill_metrics_file': 'empty_skill.csv',
        }):
            result = _read_skill_metrics_csv('pentad')
            assert result is not None
            assert result.empty

    def test_corrupt_csv_returns_none(self, tmp_path):
        """Truly empty (no headers) CSV returns None gracefully."""
        csv_file = tmp_path / "corrupt_skill.csv"
        csv_file.write_text("")

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_pentadal_skill_metrics_file': 'corrupt_skill.csv',
        }):
            result = _read_skill_metrics_csv('pentad')
            assert result is None


class TestNormalizeApiSkillMetrics:
    """Tests for API -> CSV column normalization."""

    def test_renames_api_columns_pentad(self):
        """API column horizon_in_year -> pentad_in_year."""
        df = pd.DataFrame({
            'horizon_in_year': [1, 2],
            'model_type': ['LR', 'TFT'],
            'code': ['10001', '10001'],
            'sdivsigma': [0.3, 0.4],
        })
        result = _normalize_api_skill_metrics(df, 'pentad')
        assert 'pentad_in_year' in result.columns
        assert 'horizon_in_year' not in result.columns
        assert 'model_short' in result.columns
        assert 'model_long' in result.columns

    def test_renames_api_columns_decad(self):
        """API column horizon_in_year -> decad_in_year."""
        df = pd.DataFrame({
            'horizon_in_year': [1],
            'model_type': ['LR'],
            'code': ['10001'],
        })
        result = _normalize_api_skill_metrics(df, 'decad')
        assert 'decad_in_year' in result.columns

    def test_reconstructs_model_long(self):
        """model_long is reconstructed from model_short via mapping."""
        df = pd.DataFrame({
            'horizon_in_year': [1, 2, 3],
            'model_type': ['LR', 'TFT', 'EM'],
            'code': ['10001'] * 3,
        })
        result = _normalize_api_skill_metrics(df, 'pentad')
        assert result.loc[0, 'model_long'] == 'Linear regression (LR)'
        assert result.loc[1, 'model_long'] == 'Temporal Fusion Transformer (TFT)'
        assert result.loc[2, 'model_long'] == 'Ensemble Mean (EM)'

    def test_unknown_model_gets_fallback(self):
        """Unknown model types get 'Unknown (<type>)' as model_long."""
        df = pd.DataFrame({
            'horizon_in_year': [1],
            'model_type': ['NEWMODEL'],
            'code': ['10001'],
        })
        result = _normalize_api_skill_metrics(df, 'pentad')
        assert result.loc[0, 'model_long'] == 'Unknown (NEWMODEL)'


class TestReadSkillMetricsIntegration:
    """Integration tests for the main read_skill_metrics function."""

    def test_invalid_horizon_type_raises(self):
        """Invalid horizon_type raises ValueError."""
        with pytest.raises(ValueError, match="'pentad' or 'decad'"):
            read_skill_metrics('weekly')

    def test_csv_preferred_over_api(self, tmp_path):
        """CSV is used when available; API is not called."""
        csv_file = tmp_path / "pentad_skill.csv"
        pd.DataFrame({
            'pentad_in_year': [1],
            'code': ['10001'],
            'model_short': ['LR'],
            'sdivsigma': [0.3],
        }).to_csv(csv_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_pentadal_skill_metrics_file': 'pentad_skill.csv',
        }):
            with patch(
                'src.data_reader._read_skill_metrics_api'
            ) as mock_api:
                result = read_skill_metrics('pentad')
                mock_api.assert_not_called()
                assert len(result) == 1

    def test_falls_back_to_api_when_csv_empty(self, tmp_path):
        """When CSV is empty, tries API fallback."""
        csv_file = tmp_path / "empty.csv"
        pd.DataFrame().to_csv(csv_file, index=False)

        api_df = pd.DataFrame({
            'pentad_in_year': [1],
            'code': ['10001'],
            'model_short': ['LR'],
            'model_long': ['Linear regression (LR)'],
            'sdivsigma': [0.3],
        })

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_pentadal_skill_metrics_file': 'empty.csv',
        }):
            with patch(
                'src.data_reader._read_skill_metrics_api',
                return_value=api_df,
            ):
                result = read_skill_metrics('pentad')
                assert len(result) == 1

    def test_returns_empty_when_both_fail(self):
        """Returns empty DataFrame when CSV and API both return nothing."""
        with patch(
            'src.data_reader._read_skill_metrics_csv', return_value=None
        ):
            with patch(
                'src.data_reader._read_skill_metrics_api', return_value=None
            ):
                result = read_skill_metrics('pentad')
                assert isinstance(result, pd.DataFrame)
                assert result.empty

    def test_corrupted_csv_falls_back_to_api(self, tmp_path):
        """CSV exists but contains garbled/binary content -> falls back to API.

        Operational scenario: disk corruption or partial write during crash.
        """
        csv_file = tmp_path / "pentad_skill.csv"
        csv_file.write_bytes(b'\x00\x01\x02\xff\xfe garbled content')

        api_df = pd.DataFrame({
            'pentad_in_year': [1, 2],
            'code': ['10001', '10002'],
            'model_short': ['LR', 'TFT'],
            'model_long': [
                'Linear regression (LR)',
                'Temporal Fusion Transformer (TFT)',
            ],
            'sdivsigma': [0.3, 0.4],
            'nse': [0.9, 0.85],
            'delta': [5.0, 6.0],
            'accuracy': [0.95, 0.88],
            'mae': [2.1, 3.2],
            'n_pairs': [10, 12],
        })

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_pentadal_skill_metrics_file': 'pentad_skill.csv',
        }):
            with patch(
                'src.data_reader._read_skill_metrics_api',
                return_value=api_df,
            ) as mock_api:
                result = read_skill_metrics('pentad')
                # CSV read fails -> API fallback called
                mock_api.assert_called_once()
                assert len(result) == 2
                assert result.iloc[0]['code'] == '10001'
                assert result.iloc[0]['sdivsigma'] == 0.3

    def test_truncated_csv_with_partial_rows_falls_back(self, tmp_path):
        """CSV with headers + truncated row (no newline) -> exception -> API.

        Operational scenario: process killed mid-write.
        """
        csv_file = tmp_path / "pentad_skill.csv"
        # Write a valid header but a truncated data row
        csv_file.write_text(
            "pentad_in_year,code,model_short,sdivsigma\n"
            "1,10001,LR,0.3\n"
        )

        # This CSV is actually valid (1 row), so CSV read succeeds
        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_pentadal_skill_metrics_file': 'pentad_skill.csv',
        }):
            with patch(
                'src.data_reader._read_skill_metrics_api'
            ) as mock_api:
                result = read_skill_metrics('pentad')
                # CSV was valid so API should NOT be called
                mock_api.assert_not_called()
                assert len(result) == 1
                assert result.iloc[0]['code'] == '10001'


class TestDataReaderMissingColumns:
    """Tests for data_reader handling of CSVs with missing/extra columns (#14).

    Operational scenarios: CSV was written by an older version of the code
    (missing new columns) or by a newer version (extra columns).
    """

    def test_csv_missing_code_column_still_readable(self, tmp_path):
        """CSV without 'code' column is read but code cleanup is skipped.

        When a CSV has no 'code' column, _read_skill_metrics_csv should
        still return the DataFrame (with whatever columns exist).
        """
        csv_file = tmp_path / "pentad_skill.csv"
        pd.DataFrame({
            'pentad_in_year': [1, 2],
            'model_short': ['LR', 'TFT'],
            'sdivsigma': [0.3, 0.4],
        }).to_csv(csv_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_pentadal_skill_metrics_file': 'pentad_skill.csv',
        }):
            result = _read_skill_metrics_csv('pentad')
            assert result is not None
            assert len(result) == 2
            assert 'code' not in result.columns
            assert result.iloc[0]['sdivsigma'] == 0.3

    def test_csv_with_extra_columns_preserved(self, tmp_path):
        """CSV with extra columns beyond expected set is read fully.

        Operational scenario: newer code adds columns; older reader
        should not break.
        """
        csv_file = tmp_path / "pentad_skill.csv"
        pd.DataFrame({
            'pentad_in_year': [1],
            'code': ['10001'],
            'model_short': ['LR'],
            'sdivsigma': [0.3],
            'nse': [0.9],
            'extra_metric': [42.0],  # not in the expected schema
        }).to_csv(csv_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_pentadal_skill_metrics_file': 'pentad_skill.csv',
        }):
            result = _read_skill_metrics_csv('pentad')
            assert result is not None
            assert 'extra_metric' in result.columns
            assert result.iloc[0]['extra_metric'] == 42.0

    def test_csv_numeric_code_cleaned(self, tmp_path):
        """Code column with float codes (15001.0) is cleaned to '15001'."""
        csv_file = tmp_path / "pentad_skill.csv"
        pd.DataFrame({
            'pentad_in_year': [1],
            'code': [15001.0],  # float from CSV round-trip
            'model_short': ['LR'],
            'sdivsigma': [0.3],
        }).to_csv(csv_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_pentadal_skill_metrics_file': 'pentad_skill.csv',
        }):
            result = _read_skill_metrics_csv('pentad')
            assert result is not None
            assert result.iloc[0]['code'] == '15001'

    def test_normalize_api_missing_model_type_graceful(self):
        """API response missing model_type → no model_short/model_long columns.

        Documents current behavior: _normalize_api_skill_metrics uses
        df.rename() which silently skips missing columns. If model_type
        is absent, model_short won't exist and model_long won't be derived.
        """
        df = pd.DataFrame({
            'horizon_in_year': [1],
            'code': ['10001'],
            # 'model_type' is missing
            'sdivsigma': [0.3],
        })
        result = _normalize_api_skill_metrics(df, 'pentad')
        # horizon_in_year → pentad_in_year rename still works
        assert 'pentad_in_year' in result.columns
        # model_short and model_long not derived
        assert 'model_short' not in result.columns
        assert 'model_long' not in result.columns
        # Other columns preserved
        assert result.iloc[0]['sdivsigma'] == 0.3

    def test_normalize_api_missing_horizon_graceful(self):
        """API response missing horizon_in_year → no period column created.

        Documents current behavior: rename silently skips, so the period
        column (pentad_in_year) won't exist in output.
        """
        df = pd.DataFrame({
            'model_type': ['LR'],
            'code': ['10001'],
            # 'horizon_in_year' is missing
            'sdivsigma': [0.3],
        })
        result = _normalize_api_skill_metrics(df, 'pentad')
        # pentad_in_year not created (source column was missing)
        assert 'pentad_in_year' not in result.columns
        # model_type → model_short rename still works
        assert 'model_short' in result.columns
        assert result.iloc[0]['model_short'] == 'LR'
        # model_long derived from model_short
        assert result.iloc[0]['model_long'] == 'Linear regression (LR)'
