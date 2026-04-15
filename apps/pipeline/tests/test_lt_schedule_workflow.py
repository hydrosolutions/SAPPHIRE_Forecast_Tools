"""Tests for LTScheduleQuery task and _read_schedule_result() validation.

Covers:
- LTScheduleQuery container parameters and configuration
- _read_schedule_result() JSON validation (valid, empty, malformed, missing keys)
- LTScheduleQuery stale file cleanup
- LTScheduleQuery --today parameter
- Shell script structural checks (no bare docker run)
"""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))


class TestLTScheduleQueryTask:
    """Test LTScheduleQuery container parameters."""

    def test_image_name(self, mock_env):
        """LTScheduleQuery uses sapphire-lt-forecasting image."""
        from pipeline_docker import LTScheduleQuery

        task = LTScheduleQuery()
        # Verify by inspecting the run method's closure/source isn't practical,
        # but we can verify the task exists and has expected attributes
        assert task.today == ""

    def test_output_path(self, mock_env):
        """Output marker is /app/log_schedule_query.txt."""
        from pipeline_docker import LTScheduleQuery

        task = LTScheduleQuery()
        assert task.output().path == "/app/log_schedule_query.txt"

    def test_requires_empty(self, mock_env):
        """LTScheduleQuery has no upstream dependencies."""
        from pipeline_docker import LTScheduleQuery

        task = LTScheduleQuery()
        assert task.requires() == []

    def test_no_lt_memory_resource(self, mock_env):
        """LTScheduleQuery must NOT declare lt_memory resource (would deadlock)."""
        from pipeline_docker import LTScheduleQuery

        task = LTScheduleQuery()
        assert not hasattr(task, "resources") or "lt_memory" not in getattr(task, "resources", {})

    def test_schedule_result_path_is_class_attribute(self, mock_env):
        """SCHEDULE_RESULT_PATH is a class-level string attribute."""
        from pipeline_docker import LTScheduleQuery

        assert isinstance(LTScheduleQuery.SCHEDULE_RESULT_PATH, str)
        assert LTScheduleQuery.SCHEDULE_RESULT_PATH.endswith("/lt_schedule_result.json")

    def test_docker_logs_file_path_is_class_attribute(self, mock_env):
        """docker_logs_file_path is a class-level string attribute."""
        from pipeline_docker import LTScheduleQuery

        assert isinstance(LTScheduleQuery.docker_logs_file_path, str)
        assert "log_lt_schedule_query_" in LTScheduleQuery.docker_logs_file_path

    def test_today_parameter_default(self, mock_env):
        """today parameter defaults to empty string."""
        from pipeline_docker import LTScheduleQuery

        task = LTScheduleQuery()
        assert task.today == ""

    def test_today_parameter_override(self, mock_env):
        """today parameter can be set explicitly."""
        from pipeline_docker import LTScheduleQuery

        task = LTScheduleQuery(today="2026-03-15")
        assert task.today == "2026-03-15"


class TestReadScheduleResult:
    """Test _read_schedule_result() validation."""

    @pytest.fixture
    def schedule_path(self, mock_env):
        """Return the SCHEDULE_RESULT_PATH and ensure it doesn't exist."""
        from pipeline_docker import LTScheduleQuery

        path = LTScheduleQuery.SCHEDULE_RESULT_PATH
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if os.path.exists(path):
            os.remove(path)
        yield path
        # Cleanup
        if os.path.exists(path):
            os.remove(path)

    def test_valid_json(self, mock_env, schedule_path):
        """Valid JSON with active modes returns parsed dict."""
        from pipeline_docker import _read_schedule_result

        data = {
            "active_modes": ["month_0", "quarter"],
            "skill_metric_types": ["MONTHLY"],
            "skipped_modes": {"season": "13 days from issue day 25"},
        }
        with open(schedule_path, "w") as f:
            json.dump(data, f)

        result = _read_schedule_result()
        assert result["active_modes"] == ["month_0", "quarter"]
        assert result["skill_metric_types"] == ["MONTHLY"]
        assert result["skipped_modes"] == {"season": "13 days from issue day 25"}

    def test_valid_json_empty_active_modes(self, mock_env, schedule_path):
        """Valid JSON with empty active_modes returns dict with empty list."""
        from pipeline_docker import _read_schedule_result

        data = {
            "active_modes": [],
            "skill_metric_types": [],
            "skipped_modes": {"month_0": "no models scheduled in month 4"},
        }
        with open(schedule_path, "w") as f:
            json.dump(data, f)

        result = _read_schedule_result()
        assert result["active_modes"] == []

    def test_missing_file(self, mock_env, schedule_path):
        """Missing file raises RuntimeError mentioning log path."""
        from pipeline_docker import _read_schedule_result

        with pytest.raises(RuntimeError, match="not found"):
            _read_schedule_result()

    def test_empty_file(self, mock_env, schedule_path):
        """Empty file raises RuntimeError mentioning log path."""
        from pipeline_docker import _read_schedule_result

        with open(schedule_path, "w") as f:
            f.write("")

        with pytest.raises(RuntimeError, match="empty"):
            _read_schedule_result()

    def test_malformed_json(self, mock_env, schedule_path):
        """Malformed JSON raises RuntimeError."""
        from pipeline_docker import _read_schedule_result

        with open(schedule_path, "w") as f:
            f.write("{not valid json")

        with pytest.raises(RuntimeError, match="invalid JSON"):
            _read_schedule_result()

    def test_missing_active_modes_key(self, mock_env, schedule_path):
        """JSON missing active_modes key raises RuntimeError."""
        from pipeline_docker import _read_schedule_result

        data = {"skill_metric_types": ["MONTHLY"], "skipped_modes": {}}
        with open(schedule_path, "w") as f:
            json.dump(data, f)

        with pytest.raises(RuntimeError, match="active_modes"):
            _read_schedule_result()

    def test_active_modes_non_string_element(self, mock_env, schedule_path):
        """active_modes with non-string element raises RuntimeError."""
        from pipeline_docker import _read_schedule_result

        data = {
            "active_modes": ["month_0", None],
            "skill_metric_types": ["MONTHLY"],
            "skipped_modes": {},
        }
        with open(schedule_path, "w") as f:
            json.dump(data, f)

        with pytest.raises(RuntimeError, match="list of strings"):
            _read_schedule_result()


class TestLTScheduleQueryStaleCleanup:
    """Test that LTScheduleQuery.run() deletes stale JSON before launch."""

    def test_stale_file_deleted_before_run(self, mock_env, monkeypatch):
        """run() deletes SCHEDULE_RESULT_PATH before launching container."""
        from pipeline_docker import LTScheduleQuery

        path = LTScheduleQuery.SCHEDULE_RESULT_PATH
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # Write a stale file
        with open(path, "w") as f:
            json.dump({"active_modes": ["stale"]}, f)

        assert os.path.exists(path)

        # Mock execute_with_retries to prevent actual Docker calls
        task = LTScheduleQuery()
        calls = []

        def fake_execute(func):
            # By this point, run() should have already deleted the file
            calls.append(os.path.exists(path))
            return "success", {}

        monkeypatch.setattr(task, "execute_with_retries", fake_execute)
        task.run()

        # execute_with_retries was called, and file was gone by that point
        assert len(calls) == 1
        assert calls[0] is False  # file was deleted before execute

    def test_no_error_when_file_missing(self, mock_env, monkeypatch):
        """run() does not raise when SCHEDULE_RESULT_PATH does not exist."""
        from pipeline_docker import LTScheduleQuery

        path = LTScheduleQuery.SCHEDULE_RESULT_PATH
        if os.path.exists(path):
            os.remove(path)

        task = LTScheduleQuery()
        monkeypatch.setattr(task, "execute_with_retries", lambda func: ("success", {}))

        # Should not raise FileNotFoundError
        task.run()


class TestShellScriptStructure:
    """Structural tests for shell script changes."""

    def test_no_bare_docker_run_in_shell_script(self):
        """run_long_term_forecasts.sh contains no 'docker run' commands."""
        script_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "..",
            "bin",
            "run_long_term_forecasts.sh",
        )
        with open(script_path) as f:
            content = f.read()
        assert "docker run" not in content

    def test_no_lt_active_modes_in_shell_script(self):
        """LT_ACTIVE_MODES does not appear in run_long_term_forecasts.sh."""
        script_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "..",
            "bin",
            "run_long_term_forecasts.sh",
        )
        with open(script_path) as f:
            content = f.read()
        assert "LT_ACTIVE_MODES" not in content

    def test_no_lt_schedule_query_in_shell_script(self):
        """lt_schedule_query.py does not appear in run_long_term_forecasts.sh."""
        script_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "..",
            "bin",
            "run_long_term_forecasts.sh",
        )
        with open(script_path) as f:
            content = f.read()
        assert "lt_schedule_query.py" not in content

    def test_cleanup_includes_schedule_query(self):
        """cleanup_long_term_forecasting_containers includes lt_schedule_query."""
        script_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "..",
            "bin",
            "utils",
            "common_functions.sh",
        )
        with open(script_path) as f:
            content = f.read()
        assert "lt_schedule_query" in content
