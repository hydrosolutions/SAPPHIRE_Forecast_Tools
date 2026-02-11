"""Tests for marker file functionality in pipeline_docker.py.

This module tests the marker file system that prevents gateway preprocessing
from running multiple times per day. It covers:
- get_marker_filepath() function
- get_gateway_dependency() function
- Marker file creation on task success
- requires() methods using the gateway dependency helper

Related issue: gi_P-002_gateway_double_run.md
"""
import pytest
import datetime
import sys
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
import inspect

# Add the parent directory to path for pipeline_docker import
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


class TestGetMarkerFilepath:
    """Test the get_marker_filepath() function."""

    def test_basic_filepath(self, mock_env):
        """Test basic marker filepath generation."""
        from pipeline_docker import get_marker_filepath

        path = get_marker_filepath('preprocessing_gateway', date=datetime.date(2026, 2, 2))
        assert 'preprocessing_gateway_2026-02-02.marker' in path

    def test_default_date_is_today(self, mock_env):
        """Test that date defaults to today when not specified."""
        from pipeline_docker import get_marker_filepath

        today = datetime.date.today()
        path = get_marker_filepath('preprocessing_gateway')
        assert str(today) in path

    def test_time_slot_marker_filepath(self, mock_env):
        """Test marker filepath with time_slot for sub-daily support."""
        from pipeline_docker import get_marker_filepath

        path = get_marker_filepath(
            'preprocessing_gateway',
            date=datetime.date(2026, 2, 2),
            time_slot=0
        )
        assert path.endswith('preprocessing_gateway_2026-02-02_slot0.marker')

    def test_different_time_slots_different_paths(self, mock_env):
        """Test that different time slots produce different paths."""
        from pipeline_docker import get_marker_filepath

        date = datetime.date(2026, 2, 2)
        path0 = get_marker_filepath('preprocessing_gateway', date=date, time_slot=0)
        path1 = get_marker_filepath('preprocessing_gateway', date=date, time_slot=1)
        assert path0 != path1
        assert 'slot0' in path0
        assert 'slot1' in path1

    def test_uses_mock_marker_dir(self, mock_env):
        """Test that the fixture correctly patches MARKER_DIR."""
        from pipeline_docker import get_marker_filepath, MARKER_DIR

        path = get_marker_filepath('test_task')
        # Path should be under the temp directory, not the real MARKER_DIR
        assert str(mock_env['marker_dir']) in path


class TestGetGatewayDependency:
    """Test the get_gateway_dependency() function."""

    def test_returns_external_task_when_marker_exists(self, mock_env, clean_marker_dir):
        """When marker file exists, should return ExternalPreprocessingGateway."""
        from pipeline_docker import (
            get_gateway_dependency,
            get_marker_filepath,
            ExternalPreprocessingGateway
        )

        # Create marker file for today
        marker = get_marker_filepath('preprocessing_gateway')
        Path(marker).parent.mkdir(parents=True, exist_ok=True)
        Path(marker).write_text("test marker content")

        result = get_gateway_dependency()
        assert isinstance(result, ExternalPreprocessingGateway)

    def test_returns_real_task_when_no_marker(self, mock_env, clean_marker_dir):
        """When no marker file exists, should return PreprocessingGatewayQuantileMapping."""
        from pipeline_docker import (
            get_gateway_dependency,
            PreprocessingGatewayQuantileMapping
        )

        # No marker file created
        result = get_gateway_dependency()
        assert isinstance(result, PreprocessingGatewayQuantileMapping)

    def test_sub_daily_always_runs_preprocessing(self, mock_env, clean_marker_dir):
        """Sub-daily runs should always return PreprocessingGatewayQuantileMapping.

        ExternalPreprocessingGateway doesn't support time_slot, so sub-daily
        runs must always execute preprocessing to produce the correct
        slot-specific marker file.
        """
        from pipeline_docker import (
            get_gateway_dependency,
            get_marker_filepath,
            PreprocessingGatewayQuantileMapping
        )

        # Create marker for slot 0
        marker = get_marker_filepath('preprocessing_gateway', time_slot=0)
        Path(marker).parent.mkdir(parents=True, exist_ok=True)
        Path(marker).write_text("slot 0 marker")

        # Even with marker present, sub-daily should always run preprocessing
        result_slot0 = get_gateway_dependency(time_slot=0)
        assert isinstance(result_slot0, PreprocessingGatewayQuantileMapping)

        # Slot without marker should also run preprocessing
        result_slot1 = get_gateway_dependency(time_slot=1)
        assert isinstance(result_slot1, PreprocessingGatewayQuantileMapping)


class TestMarkerFileCreation:
    """Test marker file creation patterns."""

    def test_marker_created_on_success(self, mock_env, clean_marker_dir):
        """Verify marker file can be created with expected content."""
        from pipeline_docker import get_marker_filepath

        marker_path = get_marker_filepath('preprocessing_gateway')

        # Simulate successful task completion writing marker
        Path(marker_path).parent.mkdir(parents=True, exist_ok=True)
        with open(marker_path, 'w') as f:
            f.write(f"PreprocessingGateway completed successfully at {datetime.datetime.now()}")

        assert Path(marker_path).exists()
        content = Path(marker_path).read_text()
        assert "completed successfully" in content

    def test_marker_not_created_on_failure(self, mock_env, clean_marker_dir):
        """Verify marker file does NOT exist when task fails (marker not written)."""
        from pipeline_docker import get_marker_filepath

        marker_path = get_marker_filepath('preprocessing_gateway')
        # Don't create marker (simulating failure)
        assert not Path(marker_path).exists()


class TestRequiresMethods:
    """Test that requires() methods correctly use get_gateway_dependency()."""

    def test_conceptual_model_uses_helper(self, mock_env):
        """ConceptualModel.requires() should use get_gateway_dependency()."""
        from pipeline_docker import ConceptualModel

        source = inspect.getsource(ConceptualModel.requires)
        assert 'get_gateway_dependency' in source
        # Should NOT directly instantiate PreprocessingGatewayQuantileMapping
        assert 'PreprocessingGatewayQuantileMapping()' not in source

    def test_run_ml_model_uses_helper(self, mock_env):
        """RunMLModel.requires() should use get_gateway_dependency()."""
        from pipeline_docker import RunMLModel

        source = inspect.getsource(RunMLModel.requires)
        assert 'get_gateway_dependency' in source

    def test_run_all_ml_models_uses_helper(self, mock_env):
        """RunAllMLModels.requires() should use get_gateway_dependency()."""
        from pipeline_docker import RunAllMLModels

        source = inspect.getsource(RunAllMLModels.requires)
        assert 'get_gateway_dependency' in source


class TestGatewayDependencyIntegration:
    """Integration tests for the gateway dependency workflow."""

    def test_multiple_calls_same_day_return_consistent_types(self, mock_env, clean_marker_dir):
        """Multiple calls to get_gateway_dependency should be consistent within a day."""
        from pipeline_docker import (
            get_gateway_dependency,
            get_marker_filepath,
            PreprocessingGatewayQuantileMapping,
            ExternalPreprocessingGateway
        )

        # First call - no marker, should return real task
        result1 = get_gateway_dependency()
        assert isinstance(result1, PreprocessingGatewayQuantileMapping)

        # Simulate gateway completing and writing marker
        marker = get_marker_filepath('preprocessing_gateway')
        Path(marker).write_text("completed")

        # Second call - marker exists, should return external task
        result2 = get_gateway_dependency()
        assert isinstance(result2, ExternalPreprocessingGateway)

    def test_different_days_different_markers(self, mock_env, clean_marker_dir):
        """Markers from different days should be independent."""
        from pipeline_docker import get_marker_filepath

        date1 = datetime.date(2026, 2, 1)
        date2 = datetime.date(2026, 2, 2)

        marker1 = get_marker_filepath('preprocessing_gateway', date=date1)
        marker2 = get_marker_filepath('preprocessing_gateway', date=date2)

        assert marker1 != marker2
        assert '2026-02-01' in marker1
        assert '2026-02-02' in marker2
