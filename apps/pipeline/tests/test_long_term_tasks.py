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

    def test_output_path(self, mock_env):
        """Output is /app/log_long_term_workflow_complete.txt."""
        from pipeline_docker import RunLongTermWorkflow

        task = RunLongTermWorkflow(active_modes="month_0")
        assert task.output().path == "/app/log_long_term_workflow_complete.txt"

    def test_requires_schedule_query_when_no_override(self, mock_env):
        """Default active_modes triggers LTScheduleQuery dependency."""
        from pipeline_docker import LTScheduleQuery, RunLongTermWorkflow

        task = RunLongTermWorkflow(send_notifications=False)
        dep = task.requires()
        assert isinstance(dep, LTScheduleQuery)

    def test_requires_empty_when_override(self, mock_env):
        """Explicit active_modes skips schedule query."""
        from pipeline_docker import RunLongTermWorkflow

        task = RunLongTermWorkflow(active_modes="month_0", send_notifications=False)
        deps = task.requires()
        assert deps == []

    def test_parse_override_modes_truthy_but_empty(self, mock_env):
        """Truthy-but-empty active_modes (e.g. ',', ' ') parse to empty list."""
        from pipeline_docker import LTScheduleQuery, RunLongTermWorkflow

        task_comma = RunLongTermWorkflow(active_modes=",", send_notifications=False)
        assert task_comma._parse_override_modes() == []
        assert isinstance(task_comma.requires(), LTScheduleQuery)

        task_space = RunLongTermWorkflow(active_modes=" ", send_notifications=False)
        assert task_space._parse_override_modes() == []
        assert isinstance(task_space.requires(), LTScheduleQuery)

    def test_run_with_override_yields_tasks(self, mock_env):
        """Override path: run() yields forecast + postproc + cleanup tasks."""
        from pipeline_docker import (
            LongTermPostProcessing,
            RunLongTermForecast,
            RunLongTermWorkflow,
        )

        task = RunLongTermWorkflow(active_modes="month_0,quarter", send_notifications=False)
        gen = task.run()
        yielded = next(gen)

        # yielded should be a list of tasks
        assert isinstance(yielded, list)

        forecast_tasks = [t for t in yielded if isinstance(t, RunLongTermForecast)]
        assert len(forecast_tasks) == 2
        modes = {t.forecast_mode for t in forecast_tasks}
        assert modes == {"month_0", "quarter"}

        postproc_tasks = [t for t in yielded if isinstance(t, LongTermPostProcessing)]
        assert len(postproc_tasks) == 1
        assert postproc_tasks[0].active_modes == "month_0,quarter"

        class_names = [type(t).__name__ for t in yielded]
        assert "LogFileCleanup" in class_names
        assert "DeleteOldMarkerFiles" in class_names

    def test_run_with_notifications_yields_notification(self, mock_env):
        """Notification path: run() yields base_tasks then notification."""
        from pipeline_docker import (
            RunLongTermWorkflow,
            SendPipelineCompletionNotification,
        )

        task = RunLongTermWorkflow(active_modes="month_0", send_notifications=True)
        gen = task.run()
        first = next(gen)
        assert isinstance(first, list)  # base_tasks yielded first
        second = next(gen)
        assert isinstance(second, SendPipelineCompletionNotification)

    def test_run_passes_skill_metric_types(self, mock_env):
        """skill_metric_types is forwarded to LongTermPostProcessing via run()."""
        from pipeline_docker import LongTermPostProcessing, RunLongTermWorkflow

        task = RunLongTermWorkflow(
            active_modes="month_0",
            skill_metric_types="QUARTERLY",
            send_notifications=False,
        )
        gen = task.run()
        yielded = next(gen)
        postproc = [t for t in yielded if isinstance(t, LongTermPostProcessing)]
        assert postproc[0].skill_metric_types == "QUARTERLY"
