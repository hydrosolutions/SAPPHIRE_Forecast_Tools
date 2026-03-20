"""Tests for long-term operational task classes in pipeline_docker.py.

Covers:
- RunLongTermForecast: per-mode forecast task
- LongTermPostProcessing: postprocessing after all modes
- RunLongTermWorkflow: top-level orchestrator
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))


class TestRunLongTermForecast:
    """Test RunLongTermForecast task."""

    def test_requires_preprocessing(self, mock_env):
        """RunLongTermForecast requires PreprocessingRunoff + gateway."""
        from pipeline_docker import PreprocessingRunoff, RunLongTermForecast

        task = RunLongTermForecast(forecast_mode="month_0")
        deps = task.requires()
        assert isinstance(deps, list)
        assert len(deps) == 2
        assert isinstance(deps[0], PreprocessingRunoff)

    def test_output_includes_mode(self, mock_env):
        """Output path includes the forecast_mode."""
        from pipeline_docker import RunLongTermForecast

        task = RunLongTermForecast(forecast_mode="month_0")
        assert "month_0" in task.output().path

    def test_different_modes_different_outputs(self, mock_env):
        """Different modes produce different output paths."""
        from pipeline_docker import RunLongTermForecast

        t1 = RunLongTermForecast(forecast_mode="month_0")
        t2 = RunLongTermForecast(forecast_mode="quarter")
        assert t1.output().path != t2.output().path

    def test_resource_declaration(self, mock_env):
        """RunLongTermForecast declares lt_memory resource."""
        from pipeline_docker import RunLongTermForecast

        task = RunLongTermForecast(forecast_mode="month_0")
        assert hasattr(task, "resources")
        assert task.resources == {"lt_memory": 1}

    def test_docker_logs_file_path_includes_mode(self, mock_env):
        """Docker logs file path includes the forecast mode."""
        from pipeline_docker import RunLongTermForecast

        task = RunLongTermForecast(forecast_mode="month_1")
        assert "month_1" in task.docker_logs_file_path


class TestLongTermPostProcessing:
    """Test LongTermPostProcessing task."""

    def test_requires_all_active_modes(self, mock_env):
        """Requires RunLongTermForecast for each active mode."""
        from pipeline_docker import LongTermPostProcessing, RunLongTermForecast

        task = LongTermPostProcessing(active_modes="month_0,quarter")
        deps = task.requires()
        assert len(deps) == 2
        assert all(isinstance(d, RunLongTermForecast) for d in deps)

    def test_parses_comma_separated_modes(self, mock_env):
        """Correctly parses comma-separated active_modes parameter."""
        from pipeline_docker import LongTermPostProcessing

        task = LongTermPostProcessing(active_modes="month_0, month_1, quarter")
        deps = task.requires()
        assert len(deps) == 3
        modes = {d.forecast_mode for d in deps}
        assert modes == {"month_0", "month_1", "quarter"}

    def test_single_mode(self, mock_env):
        """Works with a single mode."""
        from pipeline_docker import LongTermPostProcessing, RunLongTermForecast

        task = LongTermPostProcessing(active_modes="month_0")
        deps = task.requires()
        assert len(deps) == 1
        assert isinstance(deps[0], RunLongTermForecast)
        assert deps[0].forecast_mode == "month_0"

    def test_output_path(self, mock_env):
        """Output is /app/log_lt_postprocessing.txt."""
        from pipeline_docker import LongTermPostProcessing

        task = LongTermPostProcessing(active_modes="month_0")
        assert task.output().path == "/app/log_lt_postprocessing.txt"

    def test_default_skill_metric_types(self, mock_env):
        """Default skill_metric_types is MONTHLY."""
        from pipeline_docker import LongTermPostProcessing

        task = LongTermPostProcessing(active_modes="month_0")
        assert task.skill_metric_types == "MONTHLY"


class TestRunLongTermWorkflow:
    """Test RunLongTermWorkflow orchestrator."""

    def test_requires_forecasts_and_postproc(self, mock_env):
        """Without notifications, requires forecast tasks + postprocessing + cleanup."""
        from pipeline_docker import (
            LongTermPostProcessing,
            RunLongTermForecast,
            RunLongTermWorkflow,
        )

        task = RunLongTermWorkflow(active_modes="month_0,quarter", send_notifications=False)
        deps = task.requires()
        assert isinstance(deps, list)

        forecast_tasks = [d for d in deps if isinstance(d, RunLongTermForecast)]
        assert len(forecast_tasks) == 2

        postproc_tasks = [d for d in deps if isinstance(d, LongTermPostProcessing)]
        assert len(postproc_tasks) == 1

    def test_includes_cleanup_tasks(self, mock_env):
        """Includes LogFileCleanup and DeleteOldMarkerFiles."""
        from pipeline_docker import (
            RunLongTermWorkflow,
        )

        task = RunLongTermWorkflow(active_modes="month_0", send_notifications=False)
        deps = task.requires()
        class_names = [type(d).__name__ for d in deps]
        assert "LogFileCleanup" in class_names
        assert "DeleteOldMarkerFiles" in class_names

    def test_with_notifications(self, mock_env):
        """With notifications, wraps in SendPipelineCompletionNotification."""
        from pipeline_docker import (
            RunLongTermWorkflow,
            SendPipelineCompletionNotification,
        )

        task = RunLongTermWorkflow(active_modes="month_0", send_notifications=True)
        dep = task.requires()
        assert isinstance(dep, SendPipelineCompletionNotification)

    def test_output_path(self, mock_env):
        """Output is /app/log_long_term_workflow_complete.txt."""
        from pipeline_docker import RunLongTermWorkflow

        task = RunLongTermWorkflow(active_modes="month_0")
        assert task.output().path == "/app/log_long_term_workflow_complete.txt"

    def test_task_count_single_mode(self, mock_env):
        """Single mode: 1 forecast + 1 postproc + 2 cleanup = 4 tasks."""
        from pipeline_docker import RunLongTermWorkflow

        task = RunLongTermWorkflow(active_modes="month_0", send_notifications=False)
        deps = task.requires()
        assert len(deps) == 4

    def test_task_count_multiple_modes(self, mock_env):
        """Two modes: 2 forecasts + 1 postproc + 2 cleanup = 5 tasks."""
        from pipeline_docker import RunLongTermWorkflow

        task = RunLongTermWorkflow(active_modes="month_0,quarter", send_notifications=False)
        deps = task.requires()
        assert len(deps) == 5

    def test_passes_skill_metric_types_to_postproc(self, mock_env):
        """skill_metric_types is forwarded to LongTermPostProcessing."""
        from pipeline_docker import (
            LongTermPostProcessing,
            RunLongTermWorkflow,
        )

        task = RunLongTermWorkflow(
            active_modes="month_0",
            skill_metric_types="QUARTERLY",
            send_notifications=False,
        )
        deps = task.requires()
        postproc = [d for d in deps if isinstance(d, LongTermPostProcessing)]
        assert postproc[0].skill_metric_types == "QUARTERLY"
