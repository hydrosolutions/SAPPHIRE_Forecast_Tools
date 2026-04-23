"""Unit tests for ``bin/utils/run_skill_metrics_recalc.sh``.

These tests drive the helper via ``subprocess.run`` with a stubbed ``docker``
binary prepended to ``PATH``. The stub captures every ``docker`` invocation's
argv so we can assert the helper plumbs ``SAPPHIRE_PREDICTION_MODE`` and
``--name`` correctly into the container run, ignores ambient env, and
propagates the container's exit code.

Covers Phase P3a of
``doc/plans/issues/high_prio_gi_draft_pipeline_longterm_skill_recalc_cadence.md``.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

# Repo root: apps/pipeline/tests/ -> apps/pipeline -> apps -> repo root
REPO_ROOT = Path(__file__).resolve().parents[3]
HELPER_PATH = REPO_ROOT / "bin" / "utils" / "run_skill_metrics_recalc.sh"


# ---------------------------------------------------------------------------
# Stub docker fixture
# ---------------------------------------------------------------------------


STUB_DOCKER_SCRIPT = r"""#!/bin/bash
# Stub docker: records every invocation's argv to $STUB_ARGS_FILE and emulates
# minimal behavior for subcommands used by run_skill_metrics_recalc.sh.
#
# For ``docker inspect`` with ``--format={{.State.ExitCode}}`` the stub echoes
# ``$STUB_INSPECT_OUTPUT`` (default 0) so the helper's CONTAINER_EXIT_CODE
# becomes controllable from the test.
#
# For ``docker run`` the stub exits with ``$STUB_RUN_EXIT_CODE`` (default 0),
# which feeds into the ``tee`` pipe the helper uses. Combined with
# $STUB_INSPECT_OUTPUT, the test can exercise the success path and the
# non-zero exit-code propagation path.

# Record all argv, NUL-separated per invocation, with a human-readable marker
# between invocations so assertions can search with plain ``in`` checks.
{
    printf 'CALL:'
    for arg in "$@"; do
        printf ' [%s]' "$arg"
    done
    printf '\n'
} >> "${STUB_ARGS_FILE}"

case "$1" in
    info|image|pull|ps|rm)
        exit 0
        ;;
    inspect)
        # The helper queries the container exit code via ``--format=...``. Any
        # other ``inspect`` call (none today) also gets the same echo, which
        # is harmless.
        echo "${STUB_INSPECT_OUTPUT:-0}"
        exit 0
        ;;
    run)
        exit "${STUB_RUN_EXIT_CODE:-0}"
        ;;
    *)
        exit 0
        ;;
esac
"""


def _make_stub_docker(tmp_path: Path) -> tuple[Path, Path]:
    """Create a stub docker executable and the args-capture file.

    Returns (stub_bin_dir, args_file).
    """
    bin_dir = tmp_path / "stub_bin"
    bin_dir.mkdir()
    docker_path = bin_dir / "docker"
    docker_path.write_text(STUB_DOCKER_SCRIPT)
    docker_path.chmod(0o755)
    args_file = tmp_path / "docker_args.log"
    args_file.touch()
    return bin_dir, args_file


def _base_env(tmp_path: Path, stub_bin: Path, args_file: Path, **overrides: str) -> dict[str, str]:
    """Minimal env for the helper to run against a stub docker."""
    env = {
        "PATH": f"{stub_bin}:{os.environ.get('PATH', '')}",
        "HOME": str(tmp_path),
        "STUB_ARGS_FILE": str(args_file),
        # Helper-required vars — arbitrary placeholder values are fine because
        # the stub docker never reads them.
        "ieasyhydroforecast_data_root_dir": str(tmp_path / "data"),
        "ieasyhydroforecast_env_file_path": str(tmp_path / ".env"),
        "ieasyhydroforecast_data_ref_dir": str(tmp_path / "ref"),
        "ieasyhydroforecast_container_data_ref_dir": "/app/ref",
        "ieasyhydroforecast_backend_docker_image_tag": "testtag",
        # Non-macOS value so the override branch stays quiet and predictable
        # (we do not assert the macOS branch here).
        "IEASYHYDROHF_HOST": "remotehost:1234",
    }
    env.update(overrides)
    return env


def _run_helper(
    env: dict[str, str],
    log_dir: Path,
    args: list[str],
) -> subprocess.CompletedProcess[str]:
    """Invoke the helper function once with the given positional args."""
    log_dir.mkdir(parents=True, exist_ok=True)
    bash_cmd = f'source "{HELPER_PATH}" && run_skill_metrics_recalc_once "$@"'
    return subprocess.run(
        ["bash", "-c", bash_cmd, "_helper_test", *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


def _docker_run_line(args_file: Path) -> str:
    """Return the CALL line that corresponds to ``docker run``.

    The helper makes multiple ``docker`` calls; only the run-line contains
    ``--name`` and the env vars we care about.
    """
    text = args_file.read_text()
    for line in text.splitlines():
        # The stub records each call as ``CALL: [run] [--name] [container]...``
        if line.startswith("CALL: [run]"):
            return line
    raise AssertionError(f"No ``docker run`` invocation captured in stub log. Full log:\n{text}")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestModePlumbing:
    """Mode arg must be passed through as SAPPHIRE_PREDICTION_MODE and --name."""

    def test_mode_and_container_name_in_docker_run_args(self, tmp_path):
        stub_bin, args_file = _make_stub_docker(tmp_path)
        env = _base_env(tmp_path, stub_bin, args_file)
        log_dir = tmp_path / "lg"

        result = _run_helper(
            env,
            log_dir,
            ["MONTHLY", str(log_dir), "20260422_100000", "test-container"],
        )

        assert result.returncode == 0, (
            f"Helper exited non-zero.\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        run_line = _docker_run_line(args_file)
        assert "[-e] [SAPPHIRE_PREDICTION_MODE=MONTHLY]" in run_line, run_line
        assert "[--name] [test-container]" in run_line, run_line


class TestNoAmbientEnvFallback:
    """Helper must not let ambient SAPPHIRE_PREDICTION_MODE win over $1."""

    def test_ambient_mode_is_ignored(self, tmp_path):
        stub_bin, args_file = _make_stub_docker(tmp_path)
        # Ambient env deliberately set to a different mode to prove it loses.
        env = _base_env(tmp_path, stub_bin, args_file, SAPPHIRE_PREDICTION_MODE="DECAD")
        log_dir = tmp_path / "lg"

        result = _run_helper(
            env,
            log_dir,
            ["MONTHLY", str(log_dir), "20260422_100000", "test-container"],
        )

        assert result.returncode == 0, (
            f"Helper exited non-zero.\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        run_line = _docker_run_line(args_file)
        assert "[-e] [SAPPHIRE_PREDICTION_MODE=MONTHLY]" in run_line, run_line
        assert "SAPPHIRE_PREDICTION_MODE=DECAD" not in run_line, run_line


class TestEmptyModeRejected:
    """Empty first arg must produce exit 2 and no docker run."""

    def test_empty_mode_returns_2(self, tmp_path):
        stub_bin, args_file = _make_stub_docker(tmp_path)
        env = _base_env(tmp_path, stub_bin, args_file)
        log_dir = tmp_path / "lg"

        result = _run_helper(
            env,
            log_dir,
            ["", str(log_dir), "20260422_100000", "test-container"],
        )

        assert result.returncode == 2, (
            f"Expected exit 2 for empty mode, got {result.returncode}.\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        # Sanity: helper bailed before issuing ``docker run``.
        assert "CALL: [run]" not in args_file.read_text()


class TestExitCodePropagation:
    """Helper must return the container's exit code (from docker inspect)."""

    @pytest.mark.parametrize("exit_code", ["0", "42"])
    def test_container_exit_code_is_propagated(self, tmp_path, exit_code):
        stub_bin, args_file = _make_stub_docker(tmp_path)
        env = _base_env(
            tmp_path,
            stub_bin,
            args_file,
            # Make docker run exit with the same code so the tee pipeline
            # reports consistently, and force docker inspect to echo it back.
            STUB_RUN_EXIT_CODE=exit_code,
            STUB_INSPECT_OUTPUT=exit_code,
        )
        log_dir = tmp_path / "lg"

        result = _run_helper(
            env,
            log_dir,
            ["MONTHLY", str(log_dir), "20260422_100000", "test-container"],
        )

        assert result.returncode == int(exit_code), (
            f"Expected helper exit {exit_code}, got {result.returncode}.\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )


class TestHelperHasNoTraps:
    """The helper must not install any traps of its own (trap ownership rule)."""

    def test_no_trap_lines_in_helper(self):
        # grep -c counts matching lines; we expect 0.
        result = subprocess.run(
            ["grep", "-c", "^trap ", str(HELPER_PATH)],
            capture_output=True,
            text=True,
        )
        # grep -c returns 1 (no matches) when count is 0; that's fine. We want
        # the printed count to be "0".
        count = int(result.stdout.strip() or "0")
        assert count == 0, (
            f"Helper {HELPER_PATH} must not declare any ``trap`` commands; found {count}."
        )
