"""Tests for src/file_writer.py — atomic CSV writes and save orchestration.

Moved from iEasyHydroForecast/tests/test_forecast_library.py (TestAtomicWriteCSV).
"""

import os
import sys
import shutil
import tempfile

import numpy as np
import pandas as pd
import pytest
from unittest.mock import patch
from pandas.testing import assert_frame_equal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast')
)

from src import file_writer
from src import api_writer


class TestAtomicWriteCSV:
    """Tests for atomic_write_csv that prevents data loss."""

    @pytest.fixture(autouse=True)
    def setup_dirs(self, tmp_path):
        self.test_dir = str(tmp_path)
        self.test_file = os.path.join(self.test_dir, "test_output.csv")
        self.test_data = pd.DataFrame({
            'code': ['15102', '15124', '15136'],
            'value': [100.0, 200.0, 300.0],
            'date': ['2023-05-25', '2023-05-25', '2023-05-25']
        })

    def test_atomic_write_success(self):
        """atomic_write_csv writes data to a new file."""
        file_writer.atomic_write_csv(
            self.test_data, self.test_file, index=False
        )
        assert os.path.exists(self.test_file)
        result = pd.read_csv(self.test_file, dtype={'code': str})
        assert_frame_equal(result, self.test_data, check_dtype=False)

    def test_atomic_write_overwrites_existing(self):
        """atomic_write_csv overwrites existing files correctly."""
        initial_data = pd.DataFrame({'code': ['OLD'], 'value': [999]})
        initial_data.to_csv(self.test_file, index=False)

        file_writer.atomic_write_csv(
            self.test_data, self.test_file, index=False
        )
        result = pd.read_csv(self.test_file, dtype={'code': str})
        assert_frame_equal(result, self.test_data, check_dtype=False)

    def test_atomic_write_preserves_original_on_failure(self):
        """Original file preserved if write fails (core atomic property)."""
        initial_data = pd.DataFrame({'code': ['ORIGINAL'], 'value': [123]})
        initial_data.to_csv(self.test_file, index=False)

        def failing_to_csv(*args, **kwargs):
            raise IOError("Simulated write failure")

        with patch.object(pd.DataFrame, 'to_csv', failing_to_csv):
            with pytest.raises(IOError):
                file_writer.atomic_write_csv(
                    self.test_data, self.test_file, index=False
                )

        assert os.path.exists(self.test_file)
        result = pd.read_csv(self.test_file)
        assert_frame_equal(result, initial_data, check_dtype=False)

    def test_atomic_write_creates_directory(self):
        """atomic_write_csv creates parent directories if needed."""
        nested_path = os.path.join(
            self.test_dir, "subdir1", "subdir2", "output.csv"
        )
        file_writer.atomic_write_csv(
            self.test_data, nested_path, index=False
        )
        assert os.path.exists(nested_path)
        result = pd.read_csv(nested_path, dtype={'code': str})
        assert_frame_equal(result, self.test_data, check_dtype=False)

    def test_atomic_write_with_kwargs(self):
        """atomic_write_csv passes kwargs to to_csv correctly."""
        file_writer.atomic_write_csv(
            self.test_data, self.test_file, index=False, sep=';'
        )
        with open(self.test_file, 'r') as f:
            content = f.read()
            assert ';' in content
            assert '15102' in content
            assert '100.0' in content

    def test_atomic_write_no_temp_files_remain(self):
        """No temporary files remain after successful write."""
        file_writer.atomic_write_csv(
            self.test_data, self.test_file, index=False
        )
        files = os.listdir(self.test_dir)
        assert len(files) == 1
        assert files[0] == "test_output.csv"


class TestSaveMonthlySkillMetrics:
    """Tests for save_monthly_skill_metrics().

    Follows the same pattern as save_pentadal/decadal_skill_metrics:
    round, clean codes, convert horizon to int, sort, atomic CSV, API write.
    """

    @pytest.fixture(autouse=True)
    def save_env(self, tmp_path):
        """Set env vars so save_monthly_skill_metrics can resolve paths."""
        overrides = {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_monthly_skill_metrics_file': 'skill_monthly.csv',
            'SAPPHIRE_API_ENABLED': 'true',
            'SAPPHIRE_CONSISTENCY_CHECK': 'false',
            'SAPPHIRE_TEST_ENV': 'True',
        }
        with patch.dict(os.environ, overrides):
            self.tmp_path = tmp_path
            yield tmp_path

    @pytest.fixture
    def monthly_skill_data(self):
        """Representative monthly skill metrics DataFrame."""
        return pd.DataFrame({
            'month_in_year': [6, 6, 6, 3, 3, 3],
            'code': ['15013', '15013', '15013',
                     '15014', '15014', '15014'],
            'model_short': ['GBT', 'LR_Base', 'EM',
                            'GBT', 'LR_Base', 'EM'],
            'composition': [
                '', '', 'GBT, LR_Base',
                '', '', 'GBT, LR_Base',
            ],
            'sdivsigma': [0.34567, 0.41234, 0.29876,
                          0.50123, 0.36789, 0.32345],
            'nse': [0.92345, 0.88765, 0.94567,
                    0.81234, 0.90123, 0.86789],
            'delta': [12.5, 12.5, 12.5, 8.3, 8.3, 8.3],
            'accuracy': [0.92, 0.88, 0.95, 0.85, 0.91, 0.89],
            'mae': [2.12345, 3.45678, 1.89012,
                    4.56789, 2.34567, 3.01234],
            'n_pairs': [10, 10, 10, 8, 8, 8],
            'crps': [15.234, 18.567, np.nan,
                     12.345, 14.678, np.nan],
        })

    def test_csv_written_to_correct_path(self, monthly_skill_data):
        """CSV file is created at the env-var-configured path."""
        with patch.object(api_writer, 'SAPPHIRE_API_AVAILABLE', False):
            file_writer.save_monthly_skill_metrics(monthly_skill_data)

        csv_path = os.path.join(str(self.tmp_path), 'skill_monthly.csv')
        assert os.path.exists(csv_path)

    def test_csv_columns_present(self, monthly_skill_data):
        """CSV has all required columns including crps."""
        with patch.object(api_writer, 'SAPPHIRE_API_AVAILABLE', False):
            file_writer.save_monthly_skill_metrics(monthly_skill_data)

        csv_path = os.path.join(str(self.tmp_path), 'skill_monthly.csv')
        saved = pd.read_csv(csv_path)

        for col in ['month_in_year', 'code', 'model_short',
                     'sdivsigma', 'nse', 'delta', 'accuracy',
                     'mae', 'n_pairs', 'crps']:
            assert col in saved.columns, f"Missing column: {col}"

    def test_csv_sorted_by_month_code_model(self, monthly_skill_data):
        """CSV is sorted by (month_in_year, code, model_short)."""
        with patch.object(api_writer, 'SAPPHIRE_API_AVAILABLE', False):
            file_writer.save_monthly_skill_metrics(monthly_skill_data)

        csv_path = os.path.join(str(self.tmp_path), 'skill_monthly.csv')
        saved = pd.read_csv(csv_path)

        # month_in_year should be ascending: 3 before 6
        assert saved.iloc[0]['month_in_year'] == 3
        assert saved.iloc[-1]['month_in_year'] == 6

        # Within each month, model_short should be sorted
        m3 = saved[saved['month_in_year'] == 3]
        model_shorts = list(m3['model_short'])
        assert model_shorts == sorted(model_shorts)

    def test_values_rounded_to_4_decimals(self, monthly_skill_data):
        """Float values are rounded to 4 decimal places."""
        with patch.object(api_writer, 'SAPPHIRE_API_AVAILABLE', False):
            file_writer.save_monthly_skill_metrics(monthly_skill_data)

        csv_path = os.path.join(str(self.tmp_path), 'skill_monthly.csv')
        saved = pd.read_csv(csv_path, dtype={'code': str})

        row = saved[
            (saved['code'] == '15013') &
            (saved['model_short'] == 'GBT') &
            (saved['month_in_year'] == 6)
        ]
        assert len(row) == 1
        # 0.34567 -> 0.3457
        assert abs(row.iloc[0]['sdivsigma'] - 0.3457) < 1e-5
        # 2.12345 -> 2.1234 (banker's rounding: 4 is even, rounds down)
        assert abs(row.iloc[0]['mae'] - 2.1234) < 1e-5

    def test_code_cleaned_no_dot_zero(self):
        """Code values like '15013.0' are cleaned to '15013'."""
        data = pd.DataFrame({
            'month_in_year': [1],
            'code': [15013.0],  # float code
            'model_short': ['GBT'],
            'sdivsigma': [0.3], 'nse': [0.9], 'delta': [5.0],
            'accuracy': [0.9], 'mae': [2.0], 'n_pairs': [10],
            'crps': [12.0],
        })
        with patch.object(api_writer, 'SAPPHIRE_API_AVAILABLE', False):
            file_writer.save_monthly_skill_metrics(data)

        csv_path = os.path.join(str(self.tmp_path), 'skill_monthly.csv')
        saved = pd.read_csv(csv_path, dtype={'code': str})
        assert saved.iloc[0]['code'] == '15013'

    def test_month_in_year_is_int(self, monthly_skill_data):
        """month_in_year column is written as integer."""
        with patch.object(api_writer, 'SAPPHIRE_API_AVAILABLE', False):
            file_writer.save_monthly_skill_metrics(monthly_skill_data)

        csv_path = os.path.join(str(self.tmp_path), 'skill_monthly.csv')
        saved = pd.read_csv(csv_path)
        assert saved['month_in_year'].dtype in (np.int64, np.int32, int)

    @patch('src.api_writer._write_skill_metrics_to_api')
    def test_api_write_called_with_month_horizon(
        self, mock_api_write, monthly_skill_data
    ):
        """API writer is called with horizon_type='month'."""
        with patch.object(api_writer, 'SAPPHIRE_API_AVAILABLE', True):
            mock_api_write.return_value = True
            file_writer.save_monthly_skill_metrics(monthly_skill_data)

        mock_api_write.assert_called_once()
        call_args = mock_api_write.call_args
        assert call_args[0][1] == "month"  # second positional arg
        # First arg is the data DataFrame
        assert len(call_args[0][0]) == 6

    def test_api_not_called_when_unavailable(self, monthly_skill_data):
        """API writer is not called when sapphire-api-client unavailable."""
        with patch.object(api_writer, 'SAPPHIRE_API_AVAILABLE', False), \
             patch('src.api_writer._write_skill_metrics_to_api') as mock_api:
            file_writer.save_monthly_skill_metrics(monthly_skill_data)
            mock_api.assert_not_called()

    def test_composition_column_preserved_in_csv(self):
        """Composition column survives CSV round-trip."""
        data = pd.DataFrame({
            'month_in_year': [6, 6],
            'code': ['15013', '15013'],
            'model_short': ['GBT', 'EM'],
            'composition': ['', 'GBT, LR_Base'],
            'sdivsigma': [0.3, 0.2],
            'nse': [0.9, 0.95],
            'delta': [12.5, 12.5],
            'accuracy': [0.9, 0.95],
            'mae': [2.0, 1.5],
            'n_pairs': [10, 10],
            'crps': [15.0, np.nan],
        })
        with patch.object(api_writer, 'SAPPHIRE_API_AVAILABLE', False):
            file_writer.save_monthly_skill_metrics(data)

        csv_path = os.path.join(str(self.tmp_path), 'skill_monthly.csv')
        saved = pd.read_csv(csv_path, dtype={'code': str})
        assert 'composition' in saved.columns
        em_row = saved[saved['model_short'] == 'EM'].iloc[0]
        assert em_row['composition'] == 'GBT, LR_Base'

    def test_empty_dataframe_handled(self):
        """Empty DataFrame doesn't crash save_monthly_skill_metrics.

        An empty DataFrame with correct columns should produce a valid
        (empty) CSV file without raising on .astype(int).
        """
        data = pd.DataFrame({
            'month_in_year': pd.Series([], dtype=float),
            'code': pd.Series([], dtype=str),
            'model_short': pd.Series([], dtype=str),
            'sdivsigma': pd.Series([], dtype=float),
            'nse': pd.Series([], dtype=float),
            'delta': pd.Series([], dtype=float),
            'accuracy': pd.Series([], dtype=float),
            'mae': pd.Series([], dtype=float),
            'n_pairs': pd.Series([], dtype=float),
            'crps': pd.Series([], dtype=float),
        })
        with patch.object(api_writer, 'SAPPHIRE_API_AVAILABLE', False):
            file_writer.save_monthly_skill_metrics(data)

        csv_path = os.path.join(str(self.tmp_path), 'skill_monthly.csv')
        assert os.path.exists(csv_path)
        saved = pd.read_csv(csv_path)
        assert len(saved) == 0

    def test_crps_nan_written_to_csv(self, monthly_skill_data):
        """NaN CRPS values (from EM/Naive Mean) are written to CSV.

        EM and Naive Mean have no quantile distribution so CRPS is NaN.
        The CSV should contain these NaN values without dropping rows.
        """
        with patch.object(api_writer, 'SAPPHIRE_API_AVAILABLE', False):
            file_writer.save_monthly_skill_metrics(monthly_skill_data)

        csv_path = os.path.join(str(self.tmp_path), 'skill_monthly.csv')
        saved = pd.read_csv(csv_path, dtype={'code': str})
        em_rows = saved[saved['model_short'] == 'EM']
        assert len(em_rows) == 2
        assert all(pd.isna(em_rows['crps']))
