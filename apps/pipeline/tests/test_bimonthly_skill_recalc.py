"""Integration tests for ``bin/bimonthly_long_term_skill_metrics_recalculation.sh``.

These tests copy the wrapper into a temp directory with a fake helper and a
fake ``common_functions.sh`` alongside it, then invoke the wrapper in a
subprocess. The fake helper records each invocation (mode + container name)
to a record file and returns a mode-specific exit code controlled by env vars
``RC_MONTHLY``, ``RC_QUARTERLY``, ``RC_SEASONAL``.

We also stub ``docker`` on PATH with a minimal success-only version so the
``docker info`` / ``docker image inspect`` probes inside the wrapper pass
without touching the real Docker daemon.

Covers Phase P3b of
``doc/plans/issues/high_prio_gi_draft_pipeline_longterm_skill_recalc_cadence.md``.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
BIMONTHLY_SCRIPT = REPO_ROOT / "bin" / "bimonthly_long_term_skill_metrics_recalculation.sh"


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


FAKE_HELPER = r"""#!/bin/bash
# Fake run_skill_metrics_recalc.sh: records each invocation to $RECORD_FILE
# and returns a mode-specific exit code from $RC_<MODE> (default 0).
run_skill_metrics_recalc_once() {
    local mode="$1"
    local log_dir="$2"
    local timestamp="$3"
    local container_name="$4"
    echo "${mode}|${container_name}" >> "${RECORD_FILE}"
    local var_name="RC_${mode}"
    return "${!var_name:-0}"
}
"""

FAKE_COMMON_FUNCTIONS = r"""#!/bin/bash
# Fake common_functions.sh: no-op shims for every function the wrapper calls.
# The wrapper defines its own log_message, so we do not override it here.
print_banner() { :; }
read_configuration() { :; }
establish_ssh_tunnel() { :; }
cleanup() { :; }
"""

STUB_DOCKER_SCRIPT = r"""#!/bin/bash
# Minimal stub docker: ``docker info`` and ``docker image inspect`` succeed
# so the wrapper skips the docker-pull branch. No other docker calls happen
# because the real helper is replaced by a fake that records and returns.
case "$1" in
    info)
        exit 0
        ;;
    image)
        # ``docker image inspect <image>``
        exit 0
        ;;
    *)
        exit 0
        ;;
esac
"""


def _install_harness(tmp_path: Path) -> tuple[Path, Path, dict[str, str]]:
    """Copy wrapper + fakes + stub docker into ``tmp_path``.

    Returns (wrapper_path, record_file_path, base_env).
    """
    # Layout mirrors the real repo: <td>/bimonthly.sh with sibling <td>/utils/
    # so the wrapper's ``source "$(dirname "$0")/utils/..."`` resolves here.
    wrapper_path = tmp_path / "bimonthly.sh"
    shutil.copy(BIMONTHLY_SCRIPT, wrapper_path)
    wrapper_path.chmod(0o755)

    utils_dir = tmp_path / "utils"
    utils_dir.mkdir()
    (utils_dir / "common_functions.sh").write_text(FAKE_COMMON_FUNCTIONS)
    (utils_dir / "run_skill_metrics_recalc.sh").write_text(FAKE_HELPER)

    # Stub docker on a sandboxed PATH so docker info / image inspect succeed.
    stub_bin = tmp_path / "stub_bin"
    stub_bin.mkdir()
    docker_path = stub_bin / "docker"
    docker_path.write_text(STUB_DOCKER_SCRIPT)
    docker_path.chmod(0o755)

    # Required paths and files for the wrapper's validation to pass.
    data_root = tmp_path / "data"
    (data_root / "logs").mkdir(parents=True)
    env_file = tmp_path / ".env"
    env_file.touch()
    ref_dir = tmp_path / "ref"
    ref_dir.mkdir()
    container_ref_dir = tmp_path / "cref"
    container_ref_dir.mkdir()

    record_file = tmp_path / "record.txt"
    record_file.touch()

    base_env = {
        "PATH": f"{stub_bin}:{os.environ.get('PATH', '')}",
        "HOME": str(tmp_path),
        "RECORD_FILE": str(record_file),
        # Wrapper-required vars.
        "ieasyhydroforecast_data_root_dir": str(data_root),
        "ieasyhydroforecast_env_file_path": str(env_file),
        "ieasyhydroforecast_data_ref_dir": str(ref_dir),
        "ieasyhydroforecast_container_data_ref_dir": str(container_ref_dir),
        "ieasyhydroforecast_backend_docker_image_tag": "test",
    }
    return wrapper_path, record_file, base_env


def _run_wrapper(
    wrapper_path: Path, env: dict[str, str], env_file_arg: str
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(wrapper_path), env_file_arg],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


def _parse_record(record_file: Path) -> list[tuple[str, str]]:
    lines = [ln for ln in record_file.read_text().splitlines() if ln.strip()]
    return [tuple(ln.split("|", 1)) for ln in lines]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestBimonthlyWrapper:
    """End-to-end wrapper tests with a fake helper and stub docker."""

    def test_happy_path_all_modes_succeed(self, tmp_path):
        wrapper, record, env = _install_harness(tmp_path)

        result = _run_wrapper(wrapper, env, env["ieasyhydroforecast_env_file_path"])

        assert result.returncode == 0, (
            f"Expected exit 0 on happy path, got {result.returncode}.\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )

        calls = _parse_record(record)
        assert calls == [
            ("MONTHLY", "postprc-skill-recalc-MONTHLY"),
            ("QUARTERLY", "postprc-skill-recalc-QUARTERLY"),
            ("SEASONAL", "postprc-skill-recalc-SEASONAL"),
        ], calls

        assert "[SUMMARY] Completed 3/3 modes, 0 failures" in result.stdout, result.stdout

    def test_quarterly_failure_logged_and_continues(self, tmp_path):
        wrapper, record, env = _install_harness(tmp_path)
        env["RC_QUARTERLY"] = "5"

        result = _run_wrapper(wrapper, env, env["ieasyhydroforecast_env_file_path"])

        assert result.returncode == 1, (
            f"Expected exit 1 on single-mode failure, got {result.returncode}.\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )

        calls = _parse_record(record)
        modes = [c[0] for c in calls]
        assert modes == ["MONTHLY", "QUARTERLY", "SEASONAL"], modes

        # The summary line reports the failed mode list.
        summary_lines = [ln for ln in result.stdout.splitlines() if "[SUMMARY]" in ln]
        assert len(summary_lines) == 1, summary_lines
        summary = summary_lines[0]
        assert "1 failure" in summary, summary
        assert "QUARTERLY" in summary, summary
        # The successful modes must not appear in the failure list. The summary
        # format is ``... failure(s): QUARTERLY`` so we check for the list.
        failure_list = summary.split(":", 2)[-1]
        assert "MONTHLY" not in failure_list, summary
        assert "SEASONAL" not in failure_list, summary

    def test_monthly_failure_logged_and_continues(self, tmp_path):
        wrapper, record, env = _install_harness(tmp_path)
        env["RC_MONTHLY"] = "7"

        result = _run_wrapper(wrapper, env, env["ieasyhydroforecast_env_file_path"])

        assert result.returncode == 1
        calls = _parse_record(record)
        modes = [c[0] for c in calls]
        assert modes == ["MONTHLY", "QUARTERLY", "SEASONAL"], modes

        summary_lines = [ln for ln in result.stdout.splitlines() if "[SUMMARY]" in ln]
        assert len(summary_lines) == 1, summary_lines
        summary = summary_lines[0]
        assert "MONTHLY" in summary.split(":", 2)[-1], summary

    def test_all_three_fail(self, tmp_path):
        wrapper, record, env = _install_harness(tmp_path)
        env["RC_MONTHLY"] = "1"
        env["RC_QUARTERLY"] = "2"
        env["RC_SEASONAL"] = "3"

        result = _run_wrapper(wrapper, env, env["ieasyhydroforecast_env_file_path"])

        assert result.returncode == 1
        calls = _parse_record(record)
        modes = [c[0] for c in calls]
        assert modes == ["MONTHLY", "QUARTERLY", "SEASONAL"], modes

        summary_lines = [ln for ln in result.stdout.splitlines() if "[SUMMARY]" in ln]
        assert len(summary_lines) == 1, summary_lines
        summary = summary_lines[0]
        failure_list = summary.split(":", 2)[-1]
        for mode in ("MONTHLY", "QUARTERLY", "SEASONAL"):
            assert mode in failure_list, (mode, summary)
        assert "3 failure" in summary, summary

    @pytest.mark.parametrize(
        "failing_env",
        [
            {},
            {"RC_MONTHLY": "1"},
            {"RC_QUARTERLY": "2"},
            {"RC_SEASONAL": "3"},
            {"RC_MONTHLY": "1", "RC_QUARTERLY": "2", "RC_SEASONAL": "3"},
        ],
    )
    def test_container_name_per_mode(self, tmp_path, failing_env):
        """Container name column must be ``postprc-skill-recalc-<MODE>``."""
        wrapper, record, env = _install_harness(tmp_path)
        env.update(failing_env)

        _run_wrapper(wrapper, env, env["ieasyhydroforecast_env_file_path"])

        calls = _parse_record(record)
        assert len(calls) == 3, calls
        for mode, container in calls:
            assert container == f"postprc-skill-recalc-{mode}", (mode, container)

    @pytest.mark.parametrize(
        "failing_env",
        [
            {"RC_MONTHLY": "1"},
            {"RC_QUARTERLY": "2"},
            {"RC_SEASONAL": "3"},
            {"RC_MONTHLY": "1", "RC_QUARTERLY": "2", "RC_SEASONAL": "3"},
        ],
    )
    def test_log_rotation_runs_even_after_failure(self, tmp_path, failing_env):
        """``find ... -mtime +15 -delete`` must still execute on failure paths.

        The plan requires ``exit 1`` to happen AFTER log rotation, not before.
        """
        wrapper, record, env = _install_harness(tmp_path)
        env.update(failing_env)

        result = _run_wrapper(wrapper, env, env["ieasyhydroforecast_env_file_path"])

        assert result.returncode == 1
        assert "Removing log files older than 15 days" in result.stdout, result.stdout
