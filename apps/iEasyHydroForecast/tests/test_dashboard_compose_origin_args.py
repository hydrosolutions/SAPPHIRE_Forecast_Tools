"""Integration-boundary test for INFRA-032: verify that the real Compose
`command:` strings turn a configured ieasyhydroforecast_url_pentad /
ieasyhydroforecast_url_decad value into the right `panel serve` argv.

This test reads the literal `command:` block for:
  - the pentad service ("dashboard") in sapphire/docker-compose.yml
  - the decad service ("decaddashboard") in bin/docker-compose-dashboards.yml

It does NOT use PyYAML: PyYAML availability in this venv is not guaranteed,
and the plan explicitly allows targeted text parsing as a fallback, so this
test uses targeted text parsing directly for both files (avoiding the extra
dependency and any YAML-quirk mismatch). Report: PyYAML was not used.

The `command:` value is a YAML folded block scalar (`>`). This test folds it
the same way YAML does (join non-blank lines with a single space), then
undoes Compose's `$$` -> `$` escaping (Compose only escapes literal `$$`;
this snippet contains no bare `$VAR`/`${VAR}` that Compose would otherwise
interpolate at parse time), then uses `shlex.split` to recover the actual
`["bash", "-c", "<script>"]` argv Docker would run.

CRITICAL: that argv is executed DIRECTLY (`subprocess.run(argv, ...)`) - NOT
wrapped in another `bash -c`. Wrapping it again would expand the script's
`$( )` command substitution one shell-level too early, making the test
meaningless.

A stub `uv` executable, placed first on PATH, records the arguments it
receives so we can assert on the recovered `panel serve` invocation.
"""

from __future__ import annotations

import re
import shlex
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

# Repo root: apps/iEasyHydroForecast/tests/test_*.py -> parents[3]
_REPO_ROOT = Path(__file__).resolve().parents[3]
_PENTAD_COMPOSE_FILE = _REPO_ROOT / "sapphire" / "docker-compose.yml"
_DECAD_COMPOSE_FILE = _REPO_ROOT / "bin" / "docker-compose-dashboards.yml"

_PENTAD_SERVICE_KEY = "dashboard"
_DECAD_SERVICE_KEY = "decaddashboard"

_STUB_UV_SCRIPT = '#!/usr/bin/env bash\nprintf \'%s\\n\' "$@" > "$UV_RECORD_FILE"\n'


def _extract_service_command(compose_text: str, service_key: str) -> str:
    """Extract and YAML-fold a `command: >` block scalar for one top-level
    service in a Compose file, via targeted text parsing (no PyYAML).

    Locates the service by its exact top-level key line ("  <service_key>:"),
    bounds the block by the next top-level (2-space-indented) key, finds the
    `command: >` line inside that span, and folds the subsequent
    more-indented lines into one string (space-joined), matching YAML's
    folded-scalar behaviour for a block with no blank lines.
    """
    lines = compose_text.splitlines()
    start = None
    for i, line in enumerate(lines):
        if line == f"  {service_key}:":
            start = i
            break
    assert start is not None, f"service {service_key!r} not found"

    end = len(lines)
    for i in range(start + 1, len(lines)):
        if re.match(r"^  \S", lines[i]):
            end = i
            break
    block_lines = lines[start:end]

    cmd_start = None
    cmd_indent = None
    for i, line in enumerate(block_lines):
        m = re.match(r"^(\s+)command:\s*>\s*$", line)
        if m:
            cmd_start = i
            cmd_indent = len(m.group(1))
            break
    assert cmd_start is not None, f"no 'command: >' block found for {service_key!r}"

    content_lines = []
    for line in block_lines[cmd_start + 1 :]:
        if line.strip() == "":
            content_lines.append("")
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent <= cmd_indent:
            break
        content_lines.append(line.strip())

    assert content_lines, f"empty command block for {service_key!r}"
    return " ".join(content_lines)


def _recover_bash_c_argv(command_string: str) -> list[str]:
    """Undo Compose's `$$` -> `$` escaping, then shlex.split to recover the
    real ["bash", "-c", "<script>"] argv."""
    undone = command_string.replace("$$", "$")
    argv = shlex.split(undone)
    assert len(argv) == 3, f"expected ['bash', '-c', script]; got {argv!r}"
    assert argv[0] == "bash"
    assert argv[1] == "-c"
    return argv


def _make_stub_uv(bin_dir: Path) -> None:
    stub = bin_dir / "uv"
    stub.write_text(_STUB_UV_SCRIPT)
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)


def _run_recovered_argv(
    argv: list[str], extra_env: dict[str, str]
) -> tuple[list[str], subprocess.CompletedProcess]:
    """Run the recovered ["bash", "-c", script] argv directly (no extra
    wrapping bash -c) with a stub `uv` first on PATH, and return the
    recorded uv invocation args plus the CompletedProcess."""
    with tempfile.TemporaryDirectory(prefix="sapphire_test_") as tmpdir:
        tmp = Path(tmpdir)
        stub_dir = tmp / "stubbin"
        stub_dir.mkdir()
        _make_stub_uv(stub_dir)
        record_file = tmp / "uv_record.txt"

        env = {
            "PATH": f"{stub_dir}:/usr/bin:/bin",
            "UV_RECORD_FILE": str(record_file),
            "ieasyhydroforecast_container_data_ref_dir": str(tmp / "refdir"),
        }
        env.update(extra_env)

        result = subprocess.run(argv, capture_output=True, text=True, env=env, check=False)
        recorded = record_file.read_text().splitlines() if record_file.exists() else []
        return recorded, result


# ---------------------------------------------------------------------------
# Test 11
# ---------------------------------------------------------------------------


def test_pentad_two_entry_list_yields_two_allow_websocket_origin_args():
    compose_text = _PENTAD_COMPOSE_FILE.read_text()
    command_string = _extract_service_command(compose_text, _PENTAD_SERVICE_KEY)
    argv = _recover_bash_c_argv(command_string)

    recorded, result = _run_recovered_argv(
        argv,
        extra_env={
            "ieasyhydroforecast_url_pentad": "10.0.0.1:5006,host.example:5006",
            # Decoy on the OTHER variable, to prove the pentad service does
            # not read it.
            "ieasyhydroforecast_url_decad": "decoy-decad.example:9999",
        },
    )

    assert result.returncode == 0, (
        f"stub uv invocation failed: stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    origin_args = [a for a in recorded if a.startswith("--allow-websocket-origin=")]
    assert origin_args == [
        "--allow-websocket-origin=10.0.0.1:5006",
        "--allow-websocket-origin=host.example:5006",
    ], recorded

    # Correct port for pentad.
    assert "--port=5006" in recorded, recorded

    # Must not have picked up the decad decoy value anywhere.
    assert not any("decoy-decad" in a for a in recorded), recorded


def test_decad_service_reads_decad_variable_and_serves_port_5007():
    compose_text = _DECAD_COMPOSE_FILE.read_text()
    command_string = _extract_service_command(compose_text, _DECAD_SERVICE_KEY)
    argv = _recover_bash_c_argv(command_string)

    recorded, result = _run_recovered_argv(
        argv,
        extra_env={
            "ieasyhydroforecast_url_decad": "10.0.0.1:5007,host.example:5007",
            # Decoy on the OTHER variable, to prove the decad service does
            # not read it.
            "ieasyhydroforecast_url_pentad": "decoy-pentad.example:9999",
        },
    )

    assert result.returncode == 0, (
        f"stub uv invocation failed: stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    origin_args = [a for a in recorded if a.startswith("--allow-websocket-origin=")]
    assert origin_args == [
        "--allow-websocket-origin=10.0.0.1:5007",
        "--allow-websocket-origin=host.example:5007",
    ], recorded

    # Correct port for decad.
    assert "--port=5007" in recorded, recorded

    # Must not have picked up the pentad decoy value anywhere.
    assert not any("decoy-pentad" in a for a in recorded), recorded


def test_pentad_service_reads_pentad_variable_not_decad():
    """Explicit confirmation that the pentad service's ORIGINS_ARGS comes
    from ieasyhydroforecast_url_pentad specifically, not from
    ieasyhydroforecast_url_decad.

    Leaving pentad unset while decad is set to a distinct value must NOT
    produce an origin arg carrying the decad value. (Leaving it unset does
    still produce one empty-valued "--allow-websocket-origin=" arg - that is
    a pre-existing property of the sed pipeline: `sed 's/^/prefix/'` prefixes
    even an empty line. This IS reachable via the real call path: the
    documented direct `docker compose up` starts (doc/deployment.md:718-720,
    and the first-deploy checklist's decadal command) bypass both
    read_configuration's ":=" fallback and validate_dashboard_origins()
    entirely - see the "Validation coverage" section of the INFRA-032 plan
    file. Only the three scripted dashboard launchers get that protection.
    An operator who runs the manual decad `up` command with the variable
    unset in the env file WILL produce this empty-valued arg.)
    """
    compose_text = _PENTAD_COMPOSE_FILE.read_text()
    command_string = _extract_service_command(compose_text, _PENTAD_SERVICE_KEY)
    argv = _recover_bash_c_argv(command_string)

    recorded, result = _run_recovered_argv(
        argv,
        extra_env={
            "ieasyhydroforecast_url_decad": "host.example:5007",
        },
    )

    assert result.returncode == 0, (
        f"stub uv invocation failed: stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    origin_args = [a for a in recorded if a.startswith("--allow-websocket-origin=")]
    # Pre-existing sed-pipeline property: an unset/empty pentad value still
    # yields one empty-valued origin arg (not zero args, and NOT the decad
    # value) - see docstring above.
    assert origin_args == ["--allow-websocket-origin="], recorded
    assert "--port=5006" in recorded, recorded


if __name__ == "__main__":
    # Manual smoke run: `python test_dashboard_compose_origin_args.py`
    sys.exit(
        subprocess.call(
            ["pytest", "-v", __file__],
        )
    )
