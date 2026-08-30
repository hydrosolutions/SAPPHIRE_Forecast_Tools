"""Tests for the dated long-term recovery path on RunLongTermForecast.

Contracts under test:

- an issue date turns RunLongTermForecast into a recovery task: command
  override, no preprocessing dependency, marker keyed on mode AND date,
  max_retries pinned to 1;
- the undated operational path is byte-for-byte unchanged (REGRESSION);
- RunPeriodicMaintenanceWorkflow routes task_type 'lt_recovery' to the dated
  task and refuses incomplete arguments;
- the wrapper script returns the Compose status for lt_recovery only, and the
  [retcode] block it writes actually reaches Luigi (proved by running Luigi).
"""

import os
import subprocess
import sys
import textwrap

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

    @pytest.mark.parametrize(
        ("task_type", "expected_class"),
        [
            ("long_term", "LongTermPostProcessingMaintenance"),
            ("skill_recalc", "YearlySkillRecalculation"),
            ("snow_norms", "YearlySnowNormRecalculation"),
        ],
    )
    def test_existing_task_types_unchanged(self, mock_env, task_type, expected_class):
        """REGRESSION: each existing task type keeps its exact dependency."""
        import pipeline_docker

        task = pipeline_docker.RunPeriodicMaintenanceWorkflow(task_type=task_type)
        dep = task.requires()
        assert isinstance(dep, getattr(pipeline_docker, expected_class))
        assert not isinstance(dep, pipeline_docker.RunLongTermForecast)
        assert task.output().path == f"/app/log_periodic_maintenance_{task_type}_complete.txt"

    @pytest.mark.parametrize("task_type", ["long_term", "skill_recalc", "snow_norms"])
    def test_recovery_arguments_are_ignored_by_other_task_types(self, mock_env, task_type):
        """REGRESSION: the always-present Compose args must not alter routing.

        Compose passes --lt-recovery-mode / --lt-recovery-issue-date on every
        invocation. Even if an operator's shell leaks real values into them,
        a non-recovery task_type must behave exactly as before.
        """
        import pipeline_docker

        plain = pipeline_docker.RunPeriodicMaintenanceWorkflow(task_type=task_type)
        polluted = pipeline_docker.RunPeriodicMaintenanceWorkflow(
            task_type=task_type,
            lt_recovery_mode="month_0",
            lt_recovery_issue_date="2026-08-01",
        )
        assert type(polluted.requires()) is type(plain.requires())
        assert polluted.output().path == plain.output().path

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


class TestLuigiRetcodeReachesTheProcessExit:
    """The wrapper's exit status is only meaningful if Luigi returns non-zero.

    Luigi's task-failure return codes default to 0 (luigi/retcodes.py), so the
    wrapper appends a [retcode] block to the config it mounts. That block is
    useless unless Luigi actually reads the file: Luigi resolves the bare
    'luigi.cfg' entry of its search path against the process CWD, and the
    Compose service sets working_dir: /app/apps/pipeline while the wrapper
    mounts its file at /app/luigi.cfg.

    These tests reproduce that layout on disk and RUN Luigi on a task that
    fails, asserting on the real process exit status. A text-grep over the
    wrapper cannot catch a config file that is never read.
    """

    @staticmethod
    def _layout(tmp_path, wrapper_retcode_block):
        """Mirror the container: image config at the CWD, mount one level up."""
        workdir = tmp_path / "app" / "apps" / "pipeline"
        workdir.mkdir(parents=True)

        # The image's own config, at the CWD Luigi resolves 'luigi.cfg' against.
        # No [retcode] section, exactly like apps/pipeline/luigi.cfg.
        (workdir / "luigi.cfg").write_text(
            "[core]\nautoload_range: false\n\n"
            "[resources]\nlt_memory = 1\n\n"
            "[worker]\ncheck_complete_on_run = true\n"
        )

        # What run_periodic_maintenance.sh mounts at /app/luigi.cfg.
        (tmp_path / "app" / "luigi.cfg").write_text(
            "[core]\nscheduler_host = luigi-daemon\n\n"
            "[worker]\ncheck_complete_on_run = true\n" + wrapper_retcode_block
        )

        (workdir / "failing_mod.py").write_text(
            textwrap.dedent("""
                import luigi

                class DeliberatelyFailingTask(luigi.Task):
                    def output(self):
                        return luigi.LocalTarget("/nonexistent/never-created")

                    def run(self):
                        raise RuntimeError("deliberate failure")
            """)
        )
        return workdir

    @staticmethod
    def _run_luigi(workdir, extra_env):
        """Run Luigi on the failing task; assert it really ran, return the code."""
        env = dict(os.environ)
        env["PYTHONPATH"] = str(workdir)
        env.pop("LUIGI_CONFIG_PATH", None)
        env.update(extra_env)
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "luigi",
                "--local-scheduler",
                "--module",
                "failing_mod",
                "DeliberatelyFailingTask",
            ],
            cwd=str(workdir),
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        combined = result.stdout + result.stderr
        # Guard against a vacuous pass: the exit code only means something if
        # Luigi actually scheduled the task and saw it fail.
        assert "DeliberatelyFailingTask" in combined, combined
        assert "deliberate failure" in combined, combined
        return result.returncode

    RETCODE_BLOCK = (
        "\n[retcode]\nunhandled_exception = 4\nmissing_data = 5\n"
        "task_failed = 1\nalready_running = 6\nscheduling_error = 7\nnot_run = 8\n"
    )

    def test_mounted_config_alone_is_not_read(self, tmp_path):
        """Negative control: without LUIGI_CONFIG_PATH a failed task exits 0.

        This is the defect the fix exists for. If this ever starts returning
        non-zero, Luigi's config resolution changed and the fix can be revisited.
        """
        workdir = self._layout(tmp_path, self.RETCODE_BLOCK)
        assert self._run_luigi(workdir, {}) == 0

    def test_luigi_config_path_makes_a_failed_task_exit_non_zero(self, tmp_path):
        """The fix: LUIGI_CONFIG_PATH layers the [retcode] block on top."""
        workdir = self._layout(tmp_path, self.RETCODE_BLOCK)
        mounted = str(tmp_path / "app" / "luigi.cfg")
        assert self._run_luigi(workdir, {"LUIGI_CONFIG_PATH": mounted}) == 1

    def test_layering_keeps_the_image_config(self, tmp_path):
        """add_config_path APPENDS, so the image's own settings still apply.

        The mounted file has no [resources] section. If LUIGI_CONFIG_PATH
        replaced the search path instead of extending it, the resource
        declaration would be lost and the run would behave differently.
        """
        workdir = self._layout(tmp_path, self.RETCODE_BLOCK)
        probe = workdir / "probe.py"
        probe.write_text(
            textwrap.dedent("""
                import luigi.configuration
                cfg = luigi.configuration.get_config()
                # From the image config at the CWD:
                print("lt_memory=" + cfg.get("resources", "lt_memory"))
                # From the mounted config:
                print("task_failed=" + cfg.get("retcode", "task_failed"))
            """)
        )
        env = dict(os.environ)
        env["LUIGI_CONFIG_PATH"] = str(tmp_path / "app" / "luigi.cfg")
        result = subprocess.run(
            [sys.executable, str(probe)],
            cwd=str(workdir),
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, result.stderr
        assert "lt_memory=1" in result.stdout
        assert "task_failed=1" in result.stdout

    def test_wrapper_sets_luigi_config_path_for_recovery_only(self):
        path = os.path.join(_REPO_ROOT, "bin", "run_periodic_maintenance.sh")
        with open(path) as handle:
            text = handle.read()
        assert "LUIGI_CONFIG_PATH=/app/luigi.cfg" in text
        index = text.index("LUIGI_CONFIG_PATH=/app/luigi.cfg")
        preceding = text[:index]
        assert 'if [ "$TASK_TYPE" = "lt_recovery" ]' in preceding
        # ... and it is only ever passed through the recovery-scoped array.
        assert 'RECOVERY_DOCKER_ARGS[@]+"${RECOVERY_DOCKER_ARGS[@]}"' in text


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

    def test_only_one_exit_of_the_compose_status_exists(self):
        """Belt and braces for the executable tests below."""
        assert self._wrapper().count('exit "$COMPOSE_STATUS"') == 1

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


class TestWrapperExitStatus:
    """Run bin/run_periodic_maintenance.sh for real, with docker stubbed out.

    The wrapper's whole point for lt_recovery is that its exit status reaches
    the operator, so it is tested by executing it and reading the status --
    not by grepping for an `exit` line.
    """

    @staticmethod
    def _stub_env(tmp_path, compose_exit):
        """Build a PATH with fake `docker` and `curl`, plus a minimal env file."""
        stub_dir = tmp_path / "stubs"
        stub_dir.mkdir()
        log = tmp_path / "docker_calls.log"

        docker = stub_dir / "docker"
        docker.write_text(
            "#!/bin/bash\n"
            f'printf "%s\\n" "$*" >> "{log}"\n'
            'if [ "$1" = "compose" ]; then\n'
            '  for arg in "$@"; do\n'
            '    if [ "$arg" = "run" ]; then\n'
            f"      exit {compose_exit}\n"
            "    fi\n"
            "  done\n"
            "fi\n"
            "exit 0\n"
        )
        docker.chmod(0o755)

        curl = stub_dir / "curl"
        curl.write_text("#!/bin/bash\nexit 0\n")
        curl.chmod(0o755)

        # read_configuration() derives the deployment from the LAST FOUR
        # CHARACTERS of the env file path and exits 1 on anything else, so the
        # filename suffix here is load-bearing.
        env_file = tmp_path / "data" / "config" / ".env_test_kghm"
        env_file.parent.mkdir(parents=True)
        env_file.write_text("ieasyhydroforecast_organization=demo\n")

        env = dict(os.environ)
        env["PATH"] = f"{stub_dir}{os.pathsep}{env.get('PATH', '')}"
        env.pop("ieasyhydroforecast_ssh_to_iEH", None)
        env.pop("ieasyhydroforecast_env_file_path", None)
        return env, env_file, log

    def _run(self, tmp_path, args, compose_exit=0):
        env, env_file, log = self._stub_env(tmp_path, compose_exit)
        script = os.path.join(_REPO_ROOT, "bin", "run_periodic_maintenance.sh")
        result = subprocess.run(
            ["bash", script, args[0], str(env_file), *args[1:]],
            cwd=str(tmp_path),
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        calls = log.read_text() if log.exists() else ""
        cfg = tmp_path / "temp_luigi.cfg"
        return result, calls, (cfg.read_text() if cfg.exists() else "")

    def test_recovery_propagates_a_compose_failure(self, tmp_path):
        result, calls, _ = self._run(
            tmp_path, ["lt_recovery", "month_0", "2026-08-01"], compose_exit=7
        )
        assert result.returncode == 7, result.stdout + result.stderr
        assert "run" in calls

    def test_recovery_returns_zero_on_success(self, tmp_path):
        result, _, _ = self._run(tmp_path, ["lt_recovery", "month_0", "2026-08-01"], compose_exit=0)
        assert result.returncode == 0, result.stdout + result.stderr

    @pytest.mark.parametrize("task_type", ["long_term", "skill_recalc", "snow_norms"])
    def test_existing_task_types_still_swallow_the_status(self, tmp_path, task_type):
        """REGRESSION: cron lines for the other task types must not start failing."""
        result, _, _ = self._run(tmp_path, [task_type], compose_exit=7)
        assert result.returncode == 0, result.stdout + result.stderr

    def test_recovery_passes_luigi_config_path(self, tmp_path):
        _, calls, cfg = self._run(tmp_path, ["lt_recovery", "month_0", "2026-08-01"])
        run_line = [line for line in calls.splitlines() if " run " in f" {line} "]
        assert run_line, calls
        assert "LUIGI_CONFIG_PATH=/app/luigi.cfg" in run_line[-1]
        assert "MAINTENANCE_LT_MODE=month_0" in run_line[-1]
        assert "MAINTENANCE_LT_ISSUE_DATE=2026-08-01" in run_line[-1]
        assert "[retcode]" in cfg
        assert "task_failed = 1" in cfg

    def test_other_task_types_get_no_config_path_and_no_retcode(self, tmp_path):
        """REGRESSION: neither the env var nor the [retcode] block leaks out."""
        _, calls, cfg = self._run(tmp_path, ["long_term"])
        assert "LUIGI_CONFIG_PATH" not in calls
        assert "[retcode]" not in cfg

    @pytest.mark.parametrize(
        "args",
        [
            ["lt_recovery"],
            ["lt_recovery", "month_0"],
            ["lt_recovery", "month_0", "01.08.2026"],
            ["lt_recovery", "month_0", "2026-8-1"],
        ],
    )
    def test_bad_recovery_arguments_exit_before_docker(self, tmp_path, args):
        result, calls, _ = self._run(tmp_path, args)
        assert result.returncode == 1, result.stdout + result.stderr
        assert "run" not in calls

    def test_missing_task_type_still_errors(self, tmp_path):
        """REGRESSION: the pre-existing empty-task_type guard is unchanged."""
        env, _, log = self._stub_env(tmp_path, 0)
        script = os.path.join(_REPO_ROOT, "bin", "run_periodic_maintenance.sh")
        result = subprocess.run(
            ["bash", script],
            cwd=str(tmp_path),
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 1
        assert not log.exists()
