"""Tests for INFRA-047: the five canonical scheduled cron wrappers must
propagate their Luigi task's real outcome instead of always exiting 0.

Scope (all under bin/):
    run_preprocessing_gateway.sh   (03:00, kghm + tjhm)
    run_pentadal_forecasts.sh      (04:00, all three)
    run_decadal_forecasts.sh       (05:00, all three)
    run_long_term_forecasts.sh     (06:00 on configured issue days, kghm + tjhm)
    run_daily_maintenance.sh       (19:00, all three -- special: it runs the
                                     frontend updater AFTER the Luigi submission
                                     and must keep doing so even when Luigi fails)

Do NOT confuse this with bin/run_periodic_maintenance.sh or
bin/yearly_runoff_hydrograph_aggregation.sh -- both already fixed by
INFRA-023 (PR #494) and out of scope here.

Two layers are tested, mirroring the reasoning INFRA-023 established:

1. Shell-level propagation (docker stubbed): a failing/succeeding Compose
   exit code must reach the wrapper's own process exit code. This proves
   shell plumbing only -- it does NOT prove Luigi's own retcodes would ever
   produce that failing exit code in the first place, because stubbing
   Docker removes Luigi from the path entirely. Harness style follows
   apps/preprocessing_runoff/test/test_yearly_runoff_hydrograph_aggregation_wrapper.py
   and test_lt_dated_recovery.py's TestWrapperExitStatus.

2. The Luigi [retcode] layer, proven with Luigi actually running. Rather
   than duplicating a real-Luigi execution five times, this reuses
   test_lt_dated_recovery.TestLuigiRetcodeReachesTheProcessExit's harness
   ONCE, against the retcode block one of these wrappers (pentadal) actually
   generates. TestRetcodeBlockIsConsistentAcrossWrappers proves the other
   four wrappers write a byte-identical block, which is what extends the
   single real-Luigi proof to all five.
"""

import os
import stat
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_BIN = os.path.join(_REPO_ROOT, "bin")

# Reuse the real-Luigi harness rather than re-implementing it -- see the
# module docstring, part 2.
import test_lt_dated_recovery as _lt_recovery  # noqa: E402

RETCODE_BLOCK_MARKER = (
    "[retcode]\n"
    "unhandled_exception = 4\n"
    "missing_data = 5\n"
    "task_failed = 1\n"
    "already_running = 6\n"
    "scheduling_error = 7\n"
    "not_run = 8"
)

WRAPPERS = {
    "gateway": "run_preprocessing_gateway.sh",
    "pentadal": "run_pentadal_forecasts.sh",
    "decadal": "run_decadal_forecasts.sh",
    "long_term": "run_long_term_forecasts.sh",
    "daily_maintenance": "run_daily_maintenance.sh",
}


def _write_docker_stub(stub_dir, log_path, compose_exit):
    """Fake `docker`: any `compose ... run ...` call exits `compose_exit`;
    everything else (ps/up/inspect/config/pull/stop/rm) exits 0. Mirrors
    TestWrapperExitStatus._stub_env in test_lt_dated_recovery.py.
    """
    docker = stub_dir / "docker"
    docker.write_text(
        "#!/bin/bash\n"
        f'printf "%s\\n" "$*" >> "{log_path}"\n'
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


def _write_curl_stub(stub_dir):
    curl = stub_dir / "curl"
    curl.write_text("#!/bin/bash\nexit 0\n")
    curl.chmod(0o755)


def _make_env_file(tmp_path):
    """read_configuration() derives the deployment from the LAST FOUR
    CHARACTERS of the env file path and exits 1 on anything else (see
    bin/utils/common_functions.sh:118-138), so the filename suffix here is
    load-bearing. "kghm" is a deployment that installs all five wrappers.
    """
    config_dir = tmp_path / "data" / "config"
    config_dir.mkdir(parents=True)
    env_file = config_dir / ".env_test_kghm"
    env_file.write_text("ieasyhydroforecast_organization=demo\n")
    return env_file


def _frontend_stub(marker_path):
    """Fake bin/daily_update_sapphire_frontend.sh: records that it ran (so
    tests can assert on that independent of its exit code) and exits with
    $FAKE_FRONTEND_EXIT_CODE (default 0).
    """
    return (
        "#!/bin/bash\n"
        f'echo "frontend stub ran with arg: $1" >> "{marker_path}"\n'
        'exit "${FAKE_FRONTEND_EXIT_CODE:-0}"\n'
    )


def _run_wrapper(
    tmp_path,
    script_name,
    compose_exit=0,
    extra_stub_files=None,
    frontend_exit=None,
):
    """Execute one of the five wrapper scripts with docker/curl stubbed.

    extra_stub_files: {relative_path: content} written under tmp_path
    BEFORE the wrapper runs (cwd=tmp_path) -- for scripts that invoke a
    helper via a CWD-relative path rather than through PATH, e.g.
    pull_docker_images.sh (gateway, sourced by common_functions.sh's
    pull_docker_images()) or daily_update_sapphire_frontend.sh (daily
    maintenance, invoked directly at bin/run_daily_maintenance.sh:82).
    """
    stub_dir = tmp_path / "stubs"
    stub_dir.mkdir()
    log = tmp_path / "docker_calls.log"
    _write_docker_stub(stub_dir, log, compose_exit)
    _write_curl_stub(stub_dir)

    env_file = _make_env_file(tmp_path)

    for rel_path, content in (extra_stub_files or {}).items():
        target = tmp_path / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
        target.chmod(target.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

    env = dict(os.environ)
    env["PATH"] = f"{stub_dir}{os.pathsep}{env.get('PATH', '')}"
    env.pop("ieasyhydroforecast_ssh_to_iEH", None)
    env.pop("ieasyhydroforecast_env_file_path", None)
    if frontend_exit is not None:
        env["FAKE_FRONTEND_EXIT_CODE"] = str(frontend_exit)

    script = os.path.join(_BIN, script_name)
    result = subprocess.run(
        ["bash", script, str(env_file)],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    calls = log.read_text() if log.exists() else ""
    cfg = tmp_path / "temp_luigi.cfg"
    return result, calls, (cfg.read_text() if cfg.exists() else "")


def _extra_files_for(key, tmp_path):
    """Per-wrapper extra CWD-relative stub scripts it needs to run clean."""
    if key == "gateway":
        # pull_docker_images() in common_functions.sh SOURCES this script
        # (not a subprocess call), so it must not `exit` -- that would exit
        # the whole wrapper process. A no-op body is enough.
        return {"bin/utils/pull_docker_images.sh": "#!/bin/bash\n:\n"}
    if key == "daily_maintenance":
        marker = tmp_path / "frontend_ran.log"
        return {"bin/daily_update_sapphire_frontend.sh": _frontend_stub(marker)}
    return {}


class TestSimpleWrappersPropagateComposeStatus:
    """gateway/pentadal/decadal/long_term: no work happens after the
    Compose call, so the wrapper's exit code must equal COMPOSE_STATUS
    exactly -- same contract as run_periodic_maintenance.sh.
    """

    @pytest.mark.parametrize("key", ["gateway", "pentadal", "decadal", "long_term"])
    def test_success_exits_zero(self, tmp_path, key):
        result, calls, _ = _run_wrapper(
            tmp_path,
            WRAPPERS[key],
            compose_exit=0,
            extra_stub_files=_extra_files_for(key, tmp_path),
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert "run" in calls

    @pytest.mark.parametrize("key", ["gateway", "pentadal", "decadal", "long_term"])
    def test_luigi_task_failure_exits_nonzero(self, tmp_path, key):
        result, calls, _ = _run_wrapper(
            tmp_path,
            WRAPPERS[key],
            compose_exit=7,
            extra_stub_files=_extra_files_for(key, tmp_path),
        )
        assert result.returncode == 7, result.stdout + result.stderr
        assert "run" in calls


class TestDailyMaintenanceStickyAggregate:
    """run_daily_maintenance.sh must NOT exit early on the Luigi status: the
    frontend updater at bin/run_daily_maintenance.sh:82 currently runs
    unconditionally after the Compose call and must keep doing so. The
    wrapper exits 0 only when BOTH steps succeed.
    """

    def test_luigi_failure_still_runs_frontend_updater_and_exits_nonzero(self, tmp_path):
        """The test that protects production work: an early-exit "fix"
        would silently stop the frontend update on every deployment
        whenever the Luigi step fails.
        """
        marker = tmp_path / "frontend_ran.log"
        result, calls, _ = _run_wrapper(
            tmp_path,
            WRAPPERS["daily_maintenance"],
            compose_exit=7,
            extra_stub_files={"bin/daily_update_sapphire_frontend.sh": _frontend_stub(marker)},
            frontend_exit=0,
        )
        assert "run" in calls
        assert marker.exists(), "frontend updater did not run after a Luigi failure"
        assert "frontend stub ran" in marker.read_text()
        assert result.returncode != 0, result.stdout + result.stderr

    def test_luigi_success_frontend_failure_exits_nonzero(self, tmp_path):
        """The aggregate includes the frontend updater's own status: a
        failed updater must make the wrapper non-zero even though Luigi
        succeeded (daily_update_sapphire_frontend.sh:55 can genuinely
        exit 1 during validation).
        """
        marker = tmp_path / "frontend_ran.log"
        result, calls, _ = _run_wrapper(
            tmp_path,
            WRAPPERS["daily_maintenance"],
            compose_exit=0,
            extra_stub_files={"bin/daily_update_sapphire_frontend.sh": _frontend_stub(marker)},
            frontend_exit=5,
        )
        assert "run" in calls
        assert marker.exists()
        assert result.returncode != 0, result.stdout + result.stderr

    def test_both_succeed_exits_zero(self, tmp_path):
        marker = tmp_path / "frontend_ran.log"
        result, calls, _ = _run_wrapper(
            tmp_path,
            WRAPPERS["daily_maintenance"],
            compose_exit=0,
            extra_stub_files={"bin/daily_update_sapphire_frontend.sh": _frontend_stub(marker)},
            frontend_exit=0,
        )
        assert "run" in calls
        assert marker.exists()
        assert result.returncode == 0, result.stdout + result.stderr


class TestRetcodeBlockIsConsistentAcrossWrappers:
    """All five wrappers must write the SAME [retcode] block and forward
    LUIGI_CONFIG_PATH the same way -- unconditionally, not gated behind any
    per-script branch. This is what lets the single real-Luigi proof below
    (run against pentadal's generated config) stand in for all five.
    """

    @pytest.mark.parametrize("key", list(WRAPPERS))
    def test_wrapper_source_has_retcode_block_and_config_path(self, key):
        path = os.path.join(_BIN, WRAPPERS[key])
        with open(path) as fh:
            text = fh.read()
        assert RETCODE_BLOCK_MARKER in text, text
        assert "LUIGI_RETCODE_DOCKER_ARGS=(-e LUIGI_CONFIG_PATH=/app/luigi.cfg)" in text
        assert 'LUIGI_RETCODE_DOCKER_ARGS[@]+"${LUIGI_RETCODE_DOCKER_ARGS[@]}"' in text
        assert "COMPOSE_STATUS=$?" in text


class TestLuigiRetcodeActuallyRunsForTheseWrappers:
    """Prove the [retcode] block these wrappers generate really makes a
    failed Luigi task exit non-zero -- run ONCE (not duplicated per
    wrapper), reusing test_lt_dated_recovery.TestLuigiRetcodeReachesTheProcessExit's
    real-Luigi harness. A text-grep over the wrapper (as in the class
    above) cannot catch a config file that Luigi never actually reads --
    only running Luigi for real can.
    """

    def test_pentadal_generated_retcode_block_makes_luigi_fail_nonzero(self, tmp_path):
        wrapper_dir = tmp_path / "wrapper_run"
        wrapper_dir.mkdir()
        _, _, cfg = _run_wrapper(wrapper_dir, WRAPPERS["pentadal"], compose_exit=0)
        assert "[retcode]" in cfg
        assert "task_failed = 1" in cfg
        retcode_block = cfg[cfg.index("[retcode]") - 1 :]  # keep the leading blank line

        luigi_dir = tmp_path / "luigi_run"
        luigi_dir.mkdir()
        harness = _lt_recovery.TestLuigiRetcodeReachesTheProcessExit()
        workdir = harness._layout(luigi_dir, retcode_block)
        mounted = str(luigi_dir / "app" / "luigi.cfg")
        assert harness._run_luigi(workdir, {"LUIGI_CONFIG_PATH": mounted}) == 1
