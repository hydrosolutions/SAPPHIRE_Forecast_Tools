"""Tests for the dated long-term recovery path on RunLongTermForecast.

Contracts under test:

- an issue date turns RunLongTermForecast into a recovery task: command
  override, no preprocessing dependency, marker keyed on mode AND date,
  max_retries pinned to 1;
- the undated operational path is byte-for-byte unchanged (REGRESSION);
- RunPeriodicMaintenanceWorkflow routes task_type 'lt_recovery' to the dated
  task and refuses incomplete arguments;
- the wrapper script returns the Compose status for lt_recovery only.
"""

import os
import re
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


def _capture_run(task, monkeypatch):
    """Run task.run() with docker stubbed out, returning the container kwargs."""
    captured = {}

    def fake_run_docker_container(**kwargs):
        captured.update(kwargs)
        return "container_id", 0, "logs"

    def fake_execute(func):
        func(1)
        return "Success", {}

    monkeypatch.setattr(task, "run_docker_container", fake_run_docker_container)
    monkeypatch.setattr(task, "execute_with_retries", fake_execute)
    task.run()
    return captured


class TestOperationalPathUnchanged:
    """REGRESSION: an undated RunLongTermForecast must behave as before."""

    def test_requires_preprocessing(self, mock_env):
        from pipeline_docker import PreprocessingRunoff, RunLongTermForecast

        deps = RunLongTermForecast(forecast_mode="month_0").requires()
        assert len(deps) == 2
        assert isinstance(deps[0], PreprocessingRunoff)

    def test_output_path_has_no_date(self, mock_env):
        from pipeline_docker import RunLongTermForecast

        task = RunLongTermForecast(forecast_mode="month_0")
        assert task.output().path == "/app/log_lt_forecast_month_0.txt"

    def test_no_command_override(self, mock_env, monkeypatch):
        from pipeline_docker import RunLongTermForecast

        task = RunLongTermForecast(forecast_mode="month_0")
        captured = _capture_run(task, monkeypatch)
        assert captured["command"] is None
        assert captured["container_name"] == "lt_forecast_month_0_1"
        assert captured["mem_limit"] == "12g"
        assert captured["memswap_limit"] == "16g"
        assert captured["network"] == "sapphire_sapphire-network"

    def test_max_retries_not_pinned(self, mock_env):
        from pipeline_docker import RunLongTermForecast

        task = RunLongTermForecast(forecast_mode="month_0")
        # Whatever the timeout manager says, the recovery pin must not apply.
        assert task.max_retries != 1 or task.max_retries is None

    def test_resources_unchanged(self, mock_env):
        from pipeline_docker import RunLongTermForecast

        assert RunLongTermForecast(forecast_mode="month_0").resources == {"lt_memory": 1}


class TestDatedRecoveryTask:
    def test_no_preprocessing_dependency(self, mock_env):
        from pipeline_docker import RunLongTermForecast

        task = RunLongTermForecast(forecast_mode="month_0", issue_date="2026-08-01")
        assert task.requires() == []

    def test_marker_includes_the_date(self, mock_env):
        from pipeline_docker import RunLongTermForecast

        task = RunLongTermForecast(forecast_mode="month_0", issue_date="2026-08-01")
        assert task.output().path == "/app/log_lt_forecast_month_0_2026-08-01.txt"

    def test_marker_distinguishes_two_dates_for_one_mode(self, mock_env):
        from pipeline_docker import RunLongTermForecast

        july = RunLongTermForecast(forecast_mode="month_0", issue_date="2026-07-01")
        august = RunLongTermForecast(forecast_mode="month_0", issue_date="2026-08-01")
        assert july.output().path != august.output().path

    def test_marker_differs_from_operational_marker(self, mock_env):
        from pipeline_docker import RunLongTermForecast

        operational = RunLongTermForecast(forecast_mode="month_0")
        recovery = RunLongTermForecast(forecast_mode="month_0", issue_date="2026-08-01")
        assert operational.output().path != recovery.output().path

    def test_max_retries_pinned_to_one(self, mock_env):
        from pipeline_docker import RunLongTermForecast

        task = RunLongTermForecast(forecast_mode="month_0", issue_date="2026-08-01")
        assert task.max_retries == 1

    def test_command_override(self, mock_env, monkeypatch):
        from pipeline_docker import RunLongTermForecast

        task = RunLongTermForecast(forecast_mode="month_0", issue_date="2026-08-01")
        captured = _capture_run(task, monkeypatch)
        assert captured["command"] == [
            "uv",
            "run",
            "run_forecast.py",
            "--today",
            "2026-08-01",
            "--recover",
        ]

    def test_keeps_api_network_and_memory_limits(self, mock_env, monkeypatch):
        """The guard and read-back run in the child, so it needs the API network."""
        from pipeline_docker import RunLongTermForecast

        task = RunLongTermForecast(forecast_mode="month_0", issue_date="2026-08-01")
        captured = _capture_run(task, monkeypatch)
        assert captured["network"] == "sapphire_sapphire-network"
        assert captured["mem_limit"] == "12g"
        assert captured["memswap_limit"] == "16g"
        assert "SAPPHIRE_API_URL=http://api-gateway:8000" in captured["environment"]
        assert "lt_forecast_mode=month_0" in captured["environment"]

    def test_container_name_includes_mode_and_date(self, mock_env, monkeypatch):
        from pipeline_docker import RunLongTermForecast

        task = RunLongTermForecast(forecast_mode="month_0", issue_date="2026-08-01")
        captured = _capture_run(task, monkeypatch)
        assert captured["container_name"] == "lt_recovery_month_0_2026-08-01_1"

    def test_docker_log_path_includes_the_date(self, mock_env):
        from pipeline_docker import RunLongTermForecast

        task = RunLongTermForecast(forecast_mode="month_0", issue_date="2026-08-01")
        assert "month_0_2026-08-01" in task.docker_logs_file_path


class TestPeriodicMaintenanceRouting:
    def test_lt_recovery_requires_dated_task(self, mock_env):
        from pipeline_docker import RunLongTermForecast, RunPeriodicMaintenanceWorkflow

        task = RunPeriodicMaintenanceWorkflow(
            task_type="lt_recovery",
            lt_recovery_mode="month_0",
            lt_recovery_issue_date="2026-08-01",
        )
        dep = task.requires()
        assert isinstance(dep, RunLongTermForecast)
        assert dep.forecast_mode == "month_0"
        assert dep.issue_date == "2026-08-01"

    @pytest.mark.parametrize(
        ("mode", "issue_date"),
        [("", "2026-08-01"), ("month_0", ""), ("", ""), ("  ", " ")],
    )
    def test_incomplete_arguments_raise(self, mock_env, mode, issue_date):
        from pipeline_docker import RunPeriodicMaintenanceWorkflow

        task = RunPeriodicMaintenanceWorkflow(
            task_type="lt_recovery",
            lt_recovery_mode=mode,
            lt_recovery_issue_date=issue_date,
        )
        with pytest.raises(ValueError, match="lt_recovery"):
            task.requires()

    def test_output_keyed_on_mode_and_date(self, mock_env):
        from pipeline_docker import RunPeriodicMaintenanceWorkflow

        july = RunPeriodicMaintenanceWorkflow(
            task_type="lt_recovery",
            lt_recovery_mode="month_0",
            lt_recovery_issue_date="2026-07-01",
        )
        august = RunPeriodicMaintenanceWorkflow(
            task_type="lt_recovery",
            lt_recovery_mode="month_0",
            lt_recovery_issue_date="2026-08-01",
        )
        assert july.output().path != august.output().path
        assert "2026-08-01" in august.output().path

    @pytest.mark.parametrize("task_type", ["long_term", "skill_recalc", "snow_norms"])
    def test_existing_task_types_unchanged(self, mock_env, task_type):
        """REGRESSION: existing task types keep their dependency and marker."""
        from pipeline_docker import RunPeriodicMaintenanceWorkflow

        task = RunPeriodicMaintenanceWorkflow(task_type=task_type)
        assert task.requires() is not None
        assert task.output().path == f"/app/log_periodic_maintenance_{task_type}_complete.txt"

    def test_unknown_task_type_still_raises(self, mock_env):
        from pipeline_docker import RunPeriodicMaintenanceWorkflow

        task = RunPeriodicMaintenanceWorkflow(task_type="monthly_norms")
        with pytest.raises(ValueError, match="Unknown task_type"):
            task.requires()


class TestLuigiCommandLineContract:
    """The Compose command always passes the two new args, empty when unused."""

    @staticmethod
    def _build(argv):
        import luigi.cmdline_parser as cmdline_parser
        import pipeline_docker  # noqa: F401  (registers the tasks)

        with cmdline_parser.CmdlineParser.global_instance(argv) as parser:
            return parser.get_task_obj()

    def test_empty_recovery_args_are_accepted(self, mock_env):
        """REGRESSION: existing task types still parse when the args are empty."""
        task = self._build(
            [
                "--module",
                "pipeline_docker",
                "RunPeriodicMaintenanceWorkflow",
                "--task-type",
                "long_term",
                "--lt-recovery-mode",
                "",
                "--lt-recovery-issue-date",
                "",
            ]
        )
        assert task.task_type == "long_term"
        assert task.lt_recovery_mode == ""
        assert task.output().path == "/app/log_periodic_maintenance_long_term_complete.txt"

    def test_recovery_args_reach_the_dated_task(self, mock_env):
        from pipeline_docker import RunLongTermForecast

        task = self._build(
            [
                "--module",
                "pipeline_docker",
                "RunPeriodicMaintenanceWorkflow",
                "--task-type",
                "lt_recovery",
                "--lt-recovery-mode",
                "month_0",
                "--lt-recovery-issue-date",
                "2026-08-01",
            ]
        )
        dep = task.requires()
        assert isinstance(dep, RunLongTermForecast)
        assert dep.issue_date == "2026-08-01"


class TestOperatorEntryPoint:
    """Structural checks on the wrapper and the Compose service."""

    @staticmethod
    def _wrapper():
        path = os.path.join(_REPO_ROOT, "bin", "run_periodic_maintenance.sh")
        with open(path) as handle:
            return handle.read()

    @staticmethod
    def _compose():
        path = os.path.join(_REPO_ROOT, "bin", "docker-compose-luigi.yml")
        with open(path) as handle:
            return handle.read()

    def test_wrapper_captures_compose_status(self):
        assert "COMPOSE_STATUS=$?" in self._wrapper()

    def test_wrapper_returns_status_for_lt_recovery_only(self):
        text = self._wrapper()
        # The only `exit "$COMPOSE_STATUS"` must sit inside the lt_recovery branch.
        branch = re.search(r'if \[ "\$TASK_TYPE" = "lt_recovery" \];.*?\nfi\n', text, re.DOTALL)
        assert branch is not None
        assert 'exit "$COMPOSE_STATUS"' in text
        assert text.count('exit "$COMPOSE_STATUS"') == 1
        # ... and that occurrence is inside a lt_recovery guard.
        before = text[: text.index('exit "$COMPOSE_STATUS"')]
        assert before.rstrip().endswith("fi") or 'TASK_TYPE" = "lt_recovery"' in before

    def test_wrapper_validates_recovery_arguments(self):
        text = self._wrapper()
        assert "LT_RECOVERY_MODE" in text
        assert "LT_RECOVERY_ISSUE_DATE" in text
        assert "[0-9]{4}-[0-9]{2}-[0-9]{2}" in text

    def test_wrapper_sets_non_zero_luigi_retcodes_for_recovery_only(self):
        """Luigi's default retcodes are all 0; the status would be meaningless."""
        text = self._wrapper()
        assert "[retcode]" in text
        assert "task_failed = 1" in text
        retcode_block_start = text.index("[retcode]")
        preceding = text[:retcode_block_start]
        assert 'if [ "$TASK_TYPE" = "lt_recovery" ]' in preceding

    def test_compose_forwards_recovery_arguments(self):
        text = self._compose()
        assert "'--lt-recovery-mode', '${MAINTENANCE_LT_MODE:-}'" in text
        assert "'--lt-recovery-issue-date', '${MAINTENANCE_LT_ISSUE_DATE:-}'" in text

    def test_compose_still_forwards_task_type(self):
        assert "'--task-type', '${MAINTENANCE_TASK_TYPE:-long_term}'" in self._compose()
