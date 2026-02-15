"""Tests for src/file_writer.py — atomic CSV writes and save orchestration.

Moved from iEasyHydroForecast/tests/test_forecast_library.py (TestAtomicWriteCSV).
"""

import os
import sys
import shutil
import tempfile

import pandas as pd
import pytest
from unittest.mock import patch
from pandas.testing import assert_frame_equal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast')
)

from src import file_writer


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
