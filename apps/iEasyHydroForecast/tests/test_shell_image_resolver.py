"""Subprocess-driven tests for the bash umh_check_arch_platform seam.

Covers Finding 5 from the Tajik runbook walkthrough (2026-06-08): the
mabesa/sapphire-prepgateway image tags are amd64-only, so Apple Silicon
operators must set DOCKER_DEFAULT_PLATFORM=linux/amd64 before invoking any
wrapper. The wrappers inherit an arm64 detection check by calling
umh_resolve_image, which in turn calls umh_check_arch_platform.

These tests source bin/utils/update_migration_helpers.sh in a child bash
process and assert on stderr. The architecture string is injected via the
UMH_ARCH_OVERRIDE env var seam (intentional test hook in the helper) so the
test does NOT stub `uname` on PATH.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

# Repo root: apps/iEasyHydroForecast/tests/test_*.py -> parents[3]
_REPO_ROOT = Path(__file__).resolve().parents[3]
_HELPER = _REPO_ROOT / "bin" / "utils" / "update_migration_helpers.sh"

_ARM64_WARNING_SUBSTRING = "arm64/Apple Silicon host detected"


def _run_arch_check(
    *,
    arch_override: str,
    docker_default_platform: str | None,
) -> subprocess.CompletedProcess[str]:
    """Source the helper in a child bash and run umh_check_arch_platform.

    Args:
        arch_override: value to inject as UMH_ARCH_OVERRIDE.
        docker_default_platform: value for DOCKER_DEFAULT_PLATFORM, or None
            to leave it unset in the child environment.

    Returns:
        The completed process (stderr captured for assertion).
    """
    # Build a minimal child env. Start from PATH only so we don't inherit a
    # stale DOCKER_DEFAULT_PLATFORM from the test runner's environment.
    env: dict[str, str] = {
        "PATH": "/usr/bin:/bin:/usr/local/bin",
        "UMH_ARCH_OVERRIDE": arch_override,
    }
    if docker_default_platform is not None:
        env["DOCKER_DEFAULT_PLATFORM"] = docker_default_platform

    script = f'set -eo pipefail; source "{_HELPER}"; umh_check_arch_platform'
    return subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )


def test_arm64_host_without_platform_emits_warning():
    """Case A: arm64 host + no DOCKER_DEFAULT_PLATFORM -> warning on stderr."""
    result = _run_arch_check(arch_override="aarch64", docker_default_platform=None)
    assert result.returncode == 0, (
        f"helper sourcing failed: stderr={result.stderr!r} stdout={result.stdout!r}"
    )
    assert _ARM64_WARNING_SUBSTRING in result.stderr, (
        f"expected arm64 warning on stderr, got: {result.stderr!r}"
    )
    # The warning must direct operators to the specific env var fix.
    assert "DOCKER_DEFAULT_PLATFORM=linux/amd64" in result.stderr


def test_arm64_host_with_platform_set_is_silent():
    """Case B: arm64 host + DOCKER_DEFAULT_PLATFORM=linux/amd64 -> no warning."""
    result = _run_arch_check(
        arch_override="aarch64",
        docker_default_platform="linux/amd64",
    )
    assert result.returncode == 0, (
        f"helper sourcing failed: stderr={result.stderr!r} stdout={result.stdout!r}"
    )
    assert _ARM64_WARNING_SUBSTRING not in result.stderr, (
        f"arm64 warning must be suppressed when DOCKER_DEFAULT_PLATFORM is "
        f"set; got stderr={result.stderr!r}"
    )


def test_amd64_host_never_warns():
    """Case C: x86_64 host (no DOCKER_DEFAULT_PLATFORM) -> no warning."""
    result = _run_arch_check(arch_override="x86_64", docker_default_platform=None)
    assert result.returncode == 0, (
        f"helper sourcing failed: stderr={result.stderr!r} stdout={result.stdout!r}"
    )
    assert _ARM64_WARNING_SUBSTRING not in result.stderr, (
        f"amd64 hosts must not see the arm64 warning; got stderr={result.stderr!r}"
    )


def test_arm64_alias_also_warns():
    """`uname -m` returns 'arm64' on macOS and 'aarch64' on Linux ARM. Both fire."""
    result = _run_arch_check(arch_override="arm64", docker_default_platform=None)
    assert result.returncode == 0
    assert _ARM64_WARNING_SUBSTRING in result.stderr, (
        f"both arm64 and aarch64 must trigger the warning; got: {result.stderr!r}"
    )


if __name__ == "__main__":
    # Manual smoke run helper: `python test_shell_image_resolver.py`.
    sys.exit(
        subprocess.call(
            ["pytest", "-v", __file__],
        )
    )
