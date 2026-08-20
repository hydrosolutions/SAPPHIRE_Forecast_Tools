"""Regression test for the central untested production contract raised by
post-implementation review of INFRA-032 (FIX 8): that an invalid
ieasyhydroforecast_url_pentad / _decad value stops a dashboard launcher
BEFORE it takes a running stack down, and does not merely fail to make it
worse.

Before this test, deleting all three `validate_dashboard_origins` call sites
(bin/restart_sapphire_stack.sh, bin/daily_update_sapphire_frontend.sh,
bin/deploy_sapphire_forecast_tools.sh) would leave every other test in this
suite green - test_validate_dashboard_origins.py only drives the function
directly, never through a launcher, and test_websocket_origin_config.py
never calls it at all.

This test drives all three launchers whose validate_dashboard_origins call
sites the FIX 8 report identified: bin/restart_sapphire_stack.sh,
bin/daily_update_sapphire_frontend.sh, and
bin/deploy_sapphire_forecast_tools.sh. The third one also calls
clean_out_docker_space(), which shells out to bin/utils/clean_docker.sh
--execute (docker ps / docker images piped through grep/awk, then docker
builder prune / docker system prune) and, further down, sources
bin/utils/pull_docker_images.sh (a sequence of `docker pull` calls) - both
scripts only ever invoke `docker` and standard POSIX text tools (grep, awk),
so the same stub `docker` on PATH plus the ordinary system grep/awk (already
on PATH via /usr/bin:/bin:/usr/local/bin) is sufficient to drive it end to
end without a second, separate stub. No SSH tunnel stub is needed either:
establish_ssh_tunnel() returns immediately unless
ieasyhydroforecast_ssh_to_iEH=true, which the test env file never sets.

A stub `docker` executable is placed first on PATH. It records every
invocation (one line per call, the arguments space-joined) to a file named
by $DOCKER_RECORD_FILE, and always exits 0, so any failure the script hits
is attributable to validate_dashboard_origins - not to a missing `docker`.

For each launcher, two cases are run:
  - INVALID origin value: the launcher must exit non-zero, and the stub
    docker must have recorded NO invocation at all (a fortiori, no
    "compose ... down" - validate_dashboard_origins runs before the first
    docker call in both scripts, so the stronger "no invocation at all"
    assertion holds and is checked explicitly).
  - VALID origin value: the launcher must exit 0, and the stub docker MUST
    have recorded a "compose ... down" invocation - proving the test is not
    vacuous (a broken stub, a missing PATH entry, or an unrelated early
    failure would make the invalid case pass for the wrong reason without
    this half).

Placeholders only (10.0.0.1, host.example, example.org) - no real IP
addresses, internal hostnames or credentials.
"""

from __future__ import annotations

import stat
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

# Repo root: apps/iEasyHydroForecast/tests/test_*.py -> parents[3]
_REPO_ROOT = Path(__file__).resolve().parents[3]
_RESTART_SCRIPT = _REPO_ROOT / "bin" / "restart_sapphire_stack.sh"
_DAILY_UPDATE_SCRIPT = _REPO_ROOT / "bin" / "daily_update_sapphire_frontend.sh"
_DEPLOY_SCRIPT = _REPO_ROOT / "bin" / "deploy_sapphire_forecast_tools.sh"

_VALID_PENTAD = "host.example:5006"
_VALID_DECAD = "host.example:5007"
_INVALID_PENTAD = "*"  # rejected: wildcard

_STUB_DOCKER_SCRIPT = textwrap.dedent("""\
    #!/usr/bin/env bash
    printf '%s\\n' "$*" >> "$DOCKER_RECORD_FILE"
    exit 0
    """)


def _write_env_file(directory: Path, pentad_value: str) -> Path:
    """Write an env file ending in 'kghm' (read_configuration keys on the
    last four characters of the path) that sets a full, valid base URL plus
    the pentad/decad origins under test."""
    env_path = directory / "deployment_kghm"
    env_path.write_text(
        textwrap.dedent(f"""\
            ieasyhydroforecast_url=example.org
            ieasyhydroforecast_url_pentad={pentad_value}
            ieasyhydroforecast_url_decad={_VALID_DECAD}
            """)
    )
    return env_path


def _make_stub_docker(bin_dir: Path) -> None:
    stub = bin_dir / "docker"
    stub.write_text(_STUB_DOCKER_SCRIPT)
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)


def _run_launcher(
    script_path: Path, pentad_value: str
) -> tuple[list[str], subprocess.CompletedProcess]:
    """Run `script_path <env_file>` with a stub docker first on PATH and a
    minimal environment. Returns (recorded docker invocations, result)."""
    with tempfile.TemporaryDirectory(prefix="sapphire_test_") as tmpdir:
        tmp = Path(tmpdir)
        stub_dir = tmp / "stubbin"
        stub_dir.mkdir()
        _make_stub_docker(stub_dir)
        record_file = tmp / "docker_record.txt"
        env_file = _write_env_file(tmp, pentad_value)

        env = {
            "PATH": f"{stub_dir}:/usr/bin:/bin:/usr/local/bin",
            "DOCKER_RECORD_FILE": str(record_file),
        }
        result = subprocess.run(
            ["bash", str(script_path), str(env_file)],
            capture_output=True,
            text=True,
            env=env,
            cwd=str(_REPO_ROOT),
            check=False,
            timeout=60,
        )
        recorded = record_file.read_text().splitlines() if record_file.exists() else []
        return recorded, result


def _has_compose_down(recorded: list[str]) -> bool:
    return any(
        line.split() and line.split()[0] == "compose" and "down" in line.split()
        for line in recorded
    )


# ---------------------------------------------------------------------------
# bin/restart_sapphire_stack.sh
# ---------------------------------------------------------------------------


def test_restart_stack_invalid_origin_aborts_before_any_docker_call():
    recorded, result = _run_launcher(_RESTART_SCRIPT, _INVALID_PENTAD)
    assert result.returncode != 0, (
        f"restart_sapphire_stack.sh must exit non-zero on an invalid origin; "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert not _has_compose_down(recorded), (
        f"restart_sapphire_stack.sh must not reach any 'compose ... down' "
        f"invocation when the origin value is invalid; recorded={recorded!r}"
    )
    # Stronger check: validate_dashboard_origins runs before the FIRST
    # docker call in this script, so nothing should be recorded at all.
    assert recorded == [], (
        f"expected no docker invocation whatsoever before the abort; recorded={recorded!r}"
    )


def test_restart_stack_valid_origin_proceeds_to_docker_compose_down():
    """Proves the invalid-case test above is not vacuous: with a valid
    value, the same script DOES reach 'compose ... down'."""
    recorded, result = _run_launcher(_RESTART_SCRIPT, _VALID_PENTAD)
    assert result.returncode == 0, (
        f"restart_sapphire_stack.sh must succeed with a valid origin (docker "
        f"is fully stubbed to exit 0); stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert _has_compose_down(recorded), (
        f"expected at least one 'compose ... down' invocation once "
        f"validation passes; recorded={recorded!r}"
    )


# ---------------------------------------------------------------------------
# bin/daily_update_sapphire_frontend.sh
# ---------------------------------------------------------------------------


def test_daily_update_invalid_origin_aborts_before_any_docker_call():
    recorded, result = _run_launcher(_DAILY_UPDATE_SCRIPT, _INVALID_PENTAD)
    assert result.returncode != 0, (
        f"daily_update_sapphire_frontend.sh must exit non-zero on an invalid "
        f"origin; stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert not _has_compose_down(recorded), (
        f"daily_update_sapphire_frontend.sh must not reach any "
        f"'compose ... down' invocation when the origin value is invalid; "
        f"recorded={recorded!r}"
    )
    assert recorded == [], (
        f"expected no docker invocation whatsoever before the abort; recorded={recorded!r}"
    )


def test_daily_update_valid_origin_proceeds_to_docker_compose_down():
    """Proves the invalid-case test above is not vacuous: with a valid
    value, the same script DOES reach 'compose ... down'."""
    recorded, result = _run_launcher(_DAILY_UPDATE_SCRIPT, _VALID_PENTAD)
    assert result.returncode == 0, (
        f"daily_update_sapphire_frontend.sh must succeed with a valid origin "
        f"(docker is fully stubbed to exit 0); stdout={result.stdout!r} "
        f"stderr={result.stderr!r}"
    )
    assert _has_compose_down(recorded), (
        f"expected at least one 'compose ... down' invocation once "
        f"validation passes; recorded={recorded!r}"
    )


# ---------------------------------------------------------------------------
# bin/deploy_sapphire_forecast_tools.sh
#
# This launcher does more than the other two before it ever touches
# `docker`: read_configuration, then `validate_dashboard_origins || exit 1`,
# and only THEN clean_out_docker_space() - whose first statement is
# `docker compose -f bin/docker-compose-dashboards.yml down`, i.e. the same
# "compose ... down" shape _has_compose_down looks for, so the existing
# helper needs no changes. Everything downstream of that (clean_docker.sh's
# ps/images/prune calls, pull_docker_images.sh's `docker pull` calls, the
# backgrounded `docker compose ... up -d` calls for luigi/pipeline/
# dashboards) only ever calls `docker`, so the single stub above drives the
# whole script to completion.
# ---------------------------------------------------------------------------


def test_deploy_invalid_origin_aborts_before_any_docker_call():
    recorded, result = _run_launcher(_DEPLOY_SCRIPT, _INVALID_PENTAD)
    assert result.returncode != 0, (
        f"deploy_sapphire_forecast_tools.sh must exit non-zero on an invalid "
        f"origin; stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert not _has_compose_down(recorded), (
        f"deploy_sapphire_forecast_tools.sh must not reach any "
        f"'compose ... down' invocation when the origin value is invalid; "
        f"recorded={recorded!r}"
    )
    # Stronger check: validate_dashboard_origins runs before
    # clean_out_docker_space(), whose FIRST statement is the first docker
    # call in this script, so nothing should be recorded at all.
    assert recorded == [], (
        f"expected no docker invocation whatsoever before the abort; recorded={recorded!r}"
    )


def test_deploy_valid_origin_proceeds_to_docker_compose_down():
    """Proves the invalid-case test above is not vacuous: with a valid
    value, the same script DOES reach 'compose ... down' (inside
    clean_out_docker_space(), the first thing it does)."""
    recorded, result = _run_launcher(_DEPLOY_SCRIPT, _VALID_PENTAD)
    assert result.returncode == 0, (
        f"deploy_sapphire_forecast_tools.sh must succeed with a valid origin "
        f"(docker is fully stubbed to exit 0); stdout={result.stdout!r} "
        f"stderr={result.stderr!r}"
    )
    assert _has_compose_down(recorded), (
        f"expected at least one 'compose ... down' invocation once "
        f"validation passes; recorded={recorded!r}"
    )


if __name__ == "__main__":
    # Manual smoke run: `python test_launcher_validation_order.py`
    sys.exit(
        subprocess.call(
            ["pytest", "-v", __file__],
        )
    )
