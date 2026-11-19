"""Exercise the local runner's OpenMP environment with real Python children."""

import json
import os
import shlex
import subprocess
import sys

import pytest
from conftest import RUN_LOCALLY_SH


def _run_child(tree, platform, module, script, ambient, extra_env=(), exit_code=0):
    module_dir = tree.script_dir / module
    python = module_dir / ".venv/bin/python"
    python.unlink()
    python.symlink_to(sys.executable)
    probe = module_dir / script
    probe.parent.mkdir(parents=True, exist_ok=True)
    result_file = tree.script_dir / "child.json"
    probe.write_text(
        "import json, os, sys\n"
        f"with open({str(result_file)!r}, 'w') as output:\n"
        "    json.dump({'limit': os.environ.get('OMP_THREAD_LIMIT'),\n"
        "               'args': sys.argv[1:], 'extra': os.environ.get('PROBE_EXTRA'),\n"
        "               'cwd': os.getcwd()}, output)\n"
        f"sys.exit({exit_code})\n"
    )
    env = os.environ.copy()
    env.pop("OMP_THREAD_LIMIT", None)
    if ambient is not None:
        env["OMP_THREAD_LIMIT"] = ambient
    args = ["--flag", "two words", "", "literal*$value"]
    command = f"""
source {shlex.quote(str(RUN_LOCALLY_SH))}
SCRIPT_DIR={shlex.quote(str(tree.script_dir))}
LOG_FILE={shlex.quote(str(tree.script_dir / "run.log"))}
uname() {{ printf '%s\\n' {shlex.quote(platform)}; }}
rc=0
run_in_venv {shlex.join([module, script, *extra_env, "--", *args])} || rc=$?
printf 'PARENT_LIMIT=<%s>\\n' "${{OMP_THREAD_LIMIT-unset}}"
exit "$rc"
"""
    result = subprocess.run(
        ["bash", "-c", command], env=env, capture_output=True, text=True, timeout=30
    )
    assert result.returncode == exit_code, result.stdout + result.stderr
    assert f"PARENT_LIMIT=<{ambient if ambient is not None else 'unset'}>" in result.stdout
    child = json.loads(result_file.read_text())
    assert child["args"] == args
    assert child["cwd"] == str(module_dir)
    return child


@pytest.mark.parametrize("platform", ["Darwin", "Linux"])
@pytest.mark.parametrize("module", ["long_term_forecasting", "machine_learning"])
@pytest.mark.parametrize("ambient", [None, "1", "8"])
def test_cap_is_scoped_to_mac_long_term_children(synth_tree, platform, module, ambient):
    child = _run_child(synth_tree, platform, module, "probe.py", ambient)
    expected = "1" if platform == "Darwin" and module == "long_term_forecasting" else ambient
    assert child["limit"] == expected


@pytest.mark.parametrize(
    "script",
    [
        "run_forecast.py",
        "dev_code/simulate_forecasts.py",
        "lt_schedule_query.py",
        "calibrate_and_hindcast.py",
        "recover_latest_forecasts.py",
    ],
)
@pytest.mark.parametrize("exit_code", [0, 7])
def test_all_long_term_scripts_override_explicit_limit_and_forward_failures(
    synth_tree, script, exit_code
):
    child = _run_child(
        synth_tree,
        "Darwin",
        "long_term_forecasting",
        script,
        "8",
        extra_env=("OMP_THREAD_LIMIT=16", "PROBE_EXTRA=two words"),
        exit_code=exit_code,
    )
    assert child["limit"] == "1"
    assert child["extra"] == "two words"


@pytest.mark.parametrize(
    "platform,module", [("Linux", "long_term_forecasting"), ("Darwin", "machine_learning")]
)
def test_other_children_preserve_explicit_environment(synth_tree, platform, module):
    child = _run_child(
        synth_tree, platform, module, "probe.py", "8", extra_env=("OMP_THREAD_LIMIT=16",)
    )
    assert child["limit"] == "16"
