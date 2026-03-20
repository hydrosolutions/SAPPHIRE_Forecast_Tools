"""Tests for DockerTaskBase in apps/pipeline/pipeline_docker.py.

Covers:
- __init__ reads timeout params from TimeoutManager
- __init__ with explicit param overrides
- run_docker_container() success, timeout, and error paths
- execute_with_retries() success, retry, exhaustion, timeout
- send_failure_notification() delegation
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))


class TestDockerTaskBaseInit:
    """Test DockerTaskBase.__init__ parameter loading."""

    def test_reads_timeout_params_from_manager(self, mock_env):
        """DockerTaskBase reads defaults from TimeoutManager."""
        from pipeline_docker import PreprocessingRunoff

        task = PreprocessingRunoff()
        # Should have positive timeout values from the manager
        assert task.timeout_seconds > 0
        assert task.max_retries > 0
        assert task.retry_delay >= 0

    def test_explicit_params_override_manager(self, mock_env):
        """Explicit timeout_seconds overrides TimeoutManager default."""
        from pipeline_docker import LinearRegression

        task = LinearRegression(
            prediction_mode="PENTAD",
            timeout_seconds=120,
            max_retries=7,
            retry_delay=15,
        )
        assert task.timeout_seconds == 120
        assert task.max_retries == 7
        assert task.retry_delay == 15


class TestRunDockerContainer:
    """Test DockerTaskBase.run_docker_container()."""

    def test_success_path(self, mock_env, mock_docker_client):
        """Successful container run returns (id, 0, logs)."""
        from pipeline_docker import PreprocessingRunoff

        client, container = mock_docker_client
        task = PreprocessingRunoff()

        with (
            patch("docker.from_env", return_value=client),
            patch.object(task, "run_with_timeout", return_value=None),
            patch(
                "apps.pipeline.src.pipeline_utils.there_is_a_newer_image_on_docker_hub",
                return_value=False,
            ),
        ):
            cid, exit_status, logs = task.run_docker_container(
                image_name="sapphire-preprunoff",
                container_name="test",
                volumes={},
                environment=[],
                attempt_number=1,
            )

        assert cid == "test_container_123"
        assert exit_status == 0
        assert "container output logs" in logs

    def test_timeout_path(self, mock_env, mock_docker_client):
        """Custom TimeoutError is now caught by 'except TimeoutError:' (exit 124).

        pipeline_utils.TimeoutError extends builtins.TimeoutError, so the
        inner handler catches it, stops the container, and returns exit 124.
        """
        from pipeline_docker import PreprocessingRunoff

        from apps.pipeline.src.pipeline_utils import (
            TimeoutError as PuTimeoutError,
        )

        client, container = mock_docker_client
        task = PreprocessingRunoff()

        def raise_timeout(*args, **kwargs):
            raise PuTimeoutError("timed out")

        with (
            patch("docker.from_env", return_value=client),
            patch.object(task, "run_with_timeout", side_effect=raise_timeout),
            patch(
                "apps.pipeline.src.pipeline_utils.there_is_a_newer_image_on_docker_hub",
                return_value=False,
            ),
        ):
            cid, exit_status, logs = task.run_docker_container(
                image_name="sapphire-preprunoff",
                container_name="test",
                volumes={},
                environment=[],
                attempt_number=1,
            )

        assert cid == "test_container_123"
        assert exit_status == 124
        assert "container output logs" in logs

    def test_error_path(self, mock_env, mock_docker_client):
        """Exception during container startup returns (None, 1, error_msg)."""
        from pipeline_docker import PreprocessingRunoff

        client, container = mock_docker_client
        client.containers.run.side_effect = Exception("Docker daemon unavailable")
        task = PreprocessingRunoff()

        with (
            patch("docker.from_env", return_value=client),
            patch(
                "apps.pipeline.src.pipeline_utils.there_is_a_newer_image_on_docker_hub",
                return_value=False,
            ),
        ):
            cid, exit_status, logs = task.run_docker_container(
                image_name="sapphire-preprunoff",
                container_name="test",
                volumes={},
                environment=[],
                attempt_number=1,
            )

        assert cid is None
        assert exit_status == 1
        assert "Docker daemon unavailable" in logs


class TestExecuteWithRetries:
    """Test DockerTaskBase.execute_with_retries()."""

    def test_success_on_first_attempt(self, mock_env, tmp_path):
        """Success on first attempt writes output marker."""
        from pipeline_docker import PreprocessingRunoff

        task = PreprocessingRunoff()
        log_path = str(tmp_path / "test_log.txt")
        task.docker_logs_file_path = log_path
        os.makedirs(os.path.dirname(log_path), exist_ok=True)

        mock_output = MagicMock()
        mock_output.open.return_value.__enter__ = MagicMock()
        mock_output.open.return_value.__exit__ = MagicMock(return_value=False)

        def success_func(attempt):
            return ("cid_123", 0, "success logs")

        with patch.object(task, "output", return_value=mock_output):
            status, details = task.execute_with_retries(success_func)

        assert status == "Success"
        assert "attempt 1" in details

    def test_retry_then_success(self, mock_env, tmp_path):
        """Fail on attempt 1, succeed on attempt 2."""
        from pipeline_docker import PreprocessingRunoff

        task = PreprocessingRunoff()
        task.max_retries = 3
        task.retry_delay = 0  # No delay in tests
        log_path = str(tmp_path / "test_log.txt")
        task.docker_logs_file_path = log_path

        mock_output = MagicMock()
        mock_output.open.return_value.__enter__ = MagicMock()
        mock_output.open.return_value.__exit__ = MagicMock(return_value=False)

        call_count = 0

        def fail_then_succeed(attempt):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return ("cid_123", 1, "error logs")
            return ("cid_456", 0, "success logs")

        with patch.object(task, "output", return_value=mock_output), patch("time.sleep"):
            status, details = task.execute_with_retries(fail_then_succeed)

        assert status == "Success"
        assert call_count == 2

    def test_all_retries_exhausted(self, mock_env, tmp_path):
        """All retries fail → RuntimeError raised."""
        from pipeline_docker import PreprocessingRunoff

        task = PreprocessingRunoff()
        task.max_retries = 2
        task.retry_delay = 0
        log_path = str(tmp_path / "test_log.txt")
        task.docker_logs_file_path = log_path

        def always_fail(attempt):
            return ("cid_123", 1, "error logs")

        with (
            patch.object(task, "send_failure_notification", return_value=True),
            patch("time.sleep"),
        ):
            with pytest.raises(RuntimeError, match="failed after 2 attempts"):
                task.execute_with_retries(always_fail)

    def test_timeout_stops_retrying(self, mock_env, tmp_path):
        """Exit code 124 (timeout) → no retry, status is Timeout."""
        from pipeline_docker import PreprocessingRunoff

        task = PreprocessingRunoff()
        task.max_retries = 3
        log_path = str(tmp_path / "test_log.txt")
        task.docker_logs_file_path = log_path

        call_count = 0

        def timeout_func(attempt):
            nonlocal call_count
            call_count += 1
            return ("cid_123", 124, "timeout logs")

        status, details = task.execute_with_retries(timeout_func)
        assert status == "Timeout"
        assert call_count == 1  # No retry after timeout


class TestSendFailureNotification:
    """Test DockerTaskBase.send_failure_notification()."""

    def test_delegates_to_notification_manager(self, mock_env, tmp_path):
        """send_failure_notification calls NotificationManager."""
        from pipeline_docker import PreprocessingRunoff

        task = PreprocessingRunoff()
        log_path = str(tmp_path / "test_log.txt")
        task.docker_logs_file_path = log_path
        # Create the log file so it gets attached
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, "w") as f:
            f.write("some logs")

        with patch(
            "apps.pipeline.src.notification_manager.NotificationManager.send_failure_notification",
            return_value=True,
        ) as mock_notify:
            task.send_failure_notification("Test error", "extra logs")

        mock_notify.assert_called_once()
        call_kwargs = mock_notify.call_args
        assert call_kwargs[1]["task_name"] == "PreprocessingRunoff"
        assert "Test error" in call_kwargs[1]["error_details"]
