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


def _publication_stub(marker_path):
    """Fake bin/yearly_runoff_hydrograph_aggregation.sh for daily-wrapper
    tests: records all arguments (so tests can assert on argument forwarding,
    call count, and flag presence) and exits with
    $FAKE_PUBLICATION_EXIT_CODE (default 0).

    The first positional arg ($1) is the env file path forwarded by the
    daily wrapper; $2 should be --reuse-existing-tunnel.
    """
    return (
        "#!/bin/bash\n"
        f'echo "publication stub args: $*" >> "{marker_path}"\n'
        'exit "${FAKE_PUBLICATION_EXIT_CODE:-0}"\n'
    )


def _run_wrapper(
    tmp_path,
    script_name,
    compose_exit=0,
    extra_stub_files=None,
    frontend_exit=None,
    publication_exit=None,
):
    """Execute one of the five wrapper scripts with docker/curl stubbed.

    extra_stub_files: {relative_path: content} written under tmp_path
    BEFORE the wrapper runs (cwd=tmp_path) -- for scripts that invoke a
    helper via a CWD-relative path rather than through PATH, e.g.
    pull_docker_images.sh (gateway, sourced by common_functions.sh's
    pull_docker_images()) or daily_update_sapphire_frontend.sh (daily
    maintenance, invoked directly).

    For daily_maintenance scripts a publication stub is auto-injected at
    bin/yearly_runoff_hydrograph_aggregation.sh (the new child invocation)
    unless the caller already supplies one in extra_stub_files.  The stub
    exits with $FAKE_PUBLICATION_EXIT_CODE (controlled by publication_exit).
    """
    stub_dir = tmp_path / "stubs"
    stub_dir.mkdir()
    log = tmp_path / "docker_calls.log"
    _write_docker_stub(stub_dir, log, compose_exit)
    _write_curl_stub(stub_dir)

    env_file = _make_env_file(tmp_path)

    # Auto-inject publication stub for daily-maintenance so the new child
    # invocation resolves to a controllable stub rather than the real script.
    all_extra = dict(extra_stub_files or {})
    if "daily_maintenance" in script_name:
        pub_key = "bin/yearly_runoff_hydrograph_aggregation.sh"
        if pub_key not in all_extra:
            pub_marker = tmp_path / "publication_ran.log"
            all_extra[pub_key] = _publication_stub(pub_marker)

    for rel_path, content in all_extra.items():
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
    if publication_exit is not None:
        env["FAKE_PUBLICATION_EXIT_CODE"] = str(publication_exit)

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
        frontend_marker = tmp_path / "frontend_ran.log"
        pub_marker = tmp_path / "publication_ran.log"
        return {
            "bin/daily_update_sapphire_frontend.sh": _frontend_stub(frontend_marker),
            "bin/yearly_runoff_hydrograph_aggregation.sh": _publication_stub(pub_marker),
        }
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


def _run_daily_with_shared_log(tmp_path, compose_exit=0, frontend_exit=0, publication_exit=0):
    """Like _run_daily but all three stages append to a single shared event log.

    Each stub records one line when actually invoked:
      docker compose run  → "event:luigi"
      yearly_runoff...sh  → "event:publication"
      daily_update...sh   → "event:frontend"

    The event file ordering is therefore the real subprocess invocation order,
    not echo-text positions in the wrapper's stdout.
    """
    shared_log = tmp_path / "events.log"
    stub_dir = tmp_path / "stubs_ordered"
    stub_dir.mkdir()
    docker_log = tmp_path / "docker_calls_ordered.log"

    # Docker stub: also appends to shared_log on compose run so the Luigi
    # invocation is visible in the same sequence as publication/frontend.
    docker = stub_dir / "docker"
    docker.write_text(
        "#!/bin/bash\n"
        f'printf "%s\\n" "$*" >> "{docker_log}"\n'
        'if [ "$1" = "compose" ]; then\n'
        '  for arg in "$@"; do\n'
        '    if [ "$arg" = "run" ]; then\n'
        f'      printf "event:luigi\\n" >> "{shared_log}"\n'
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

    env_file = _make_env_file(tmp_path)

    extra = {
        "bin/yearly_runoff_hydrograph_aggregation.sh": (
            "#!/bin/bash\n"
            f'printf "event:publication\\n" >> "{shared_log}"\n'
            'exit "${FAKE_PUBLICATION_EXIT_CODE:-0}"\n'
        ),
        "bin/daily_update_sapphire_frontend.sh": (
            "#!/bin/bash\n"
            f'printf "event:frontend\\n" >> "{shared_log}"\n'
            'exit "${FAKE_FRONTEND_EXIT_CODE:-0}"\n'
        ),
    }
    for rel_path, content in extra.items():
        target = tmp_path / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
        target.chmod(target.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

    env = dict(os.environ)
    env["PATH"] = f"{stub_dir}{os.pathsep}{env.get('PATH', '')}"
    env.pop("ieasyhydroforecast_ssh_to_iEH", None)
    env.pop("ieasyhydroforecast_env_file_path", None)
    if frontend_exit != 0:
        env["FAKE_FRONTEND_EXIT_CODE"] = str(frontend_exit)
    if publication_exit != 0:
        env["FAKE_PUBLICATION_EXIT_CODE"] = str(publication_exit)

    script = os.path.join(_BIN, WRAPPERS["daily_maintenance"])
    result = subprocess.run(
        ["bash", script, str(env_file)],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    return result, shared_log


def _run_daily_spaced_env(tmp_path, publication_exit=0):
    """Runs daily_maintenance with an env-file arg that contains a space.

    The wrapper calls ``read_configuration $1`` (unquoted), so bash word-splits
    the arg.  A 'decoy' file is placed at the first token of that split; it
    ends in 'kghm' and contains valid env content so read_configuration
    succeeds.  This stubs read_configuration only for this space-path scenario
    without touching common_functions or the wrapper.

    The publication child is called via ``"$1"`` (properly quoted in the
    wrapper), so it receives the full original spaced path as a single
    argument.  The stub records argc on its own line, then each arg on its own
    line (not $* which word-splits), letting the test assert exact path
    identity.
    """
    # Decoy file: first token of the space-split path, ends in "kghm".
    decoy = tmp_path / "my_kghm"
    decoy.write_text("ieasyhydroforecast_organization=demo\n")

    # The full arg the wrapper receives as $1 — contains a space.
    spaced_arg = str(decoy) + " extra"

    stub_dir = tmp_path / "stubs_spaced"
    stub_dir.mkdir()
    docker_log = tmp_path / "docker_calls_spaced.log"
    _write_docker_stub(stub_dir, docker_log, compose_exit=0)
    _write_curl_stub(stub_dir)

    pub_marker = tmp_path / "pub_spaced.log"
    frontend_marker = tmp_path / "frontend_spaced.log"

    extra = {
        # Records argc on the first line, then one arg per line via "$@".
        "bin/yearly_runoff_hydrograph_aggregation.sh": (
            "#!/bin/bash\n"
            f'printf "%s\\n" "$#" >> "{pub_marker}"\n'
            f'printf "%s\\n" "$@" >> "{pub_marker}"\n'
            'exit "${FAKE_PUBLICATION_EXIT_CODE:-0}"\n'
        ),
        "bin/daily_update_sapphire_frontend.sh": (
            "#!/bin/bash\n"
            f'printf "frontend ran\\n" >> "{frontend_marker}"\n'
            "exit 0\n"
        ),
    }
    for rel_path, content in extra.items():
        target = tmp_path / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
        target.chmod(target.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

    env = dict(os.environ)
    env["PATH"] = f"{stub_dir}{os.pathsep}{env.get('PATH', '')}"
    env.pop("ieasyhydroforecast_ssh_to_iEH", None)
    env.pop("ieasyhydroforecast_env_file_path", None)
    if publication_exit != 0:
        env["FAKE_PUBLICATION_EXIT_CODE"] = str(publication_exit)

    script = os.path.join(_BIN, WRAPPERS["daily_maintenance"])
    result = subprocess.run(
        ["bash", script, spaced_arg],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    return result, spaced_arg, pub_marker


class TestDailyMaintenancePublicationStep:
    """run_daily_maintenance.sh must call the yearly publication wrapper once,
    after Luigi and before the frontend, forwarding the env-file argument and
    the --reuse-existing-tunnel flag.  All nonzero publication exits (including
    2/4/6, which the local runner tolerates) must fail the server aggregate.
    """

    def _run_daily(self, tmp_path, compose_exit=0, frontend_exit=0, publication_exit=0):
        """Convenience wrapper: runs daily_maintenance with explicit stages."""
        frontend_marker = tmp_path / "frontend_ran.log"
        pub_marker = tmp_path / "publication_ran.log"
        result, calls, _ = _run_wrapper(
            tmp_path,
            WRAPPERS["daily_maintenance"],
            compose_exit=compose_exit,
            extra_stub_files={
                "bin/daily_update_sapphire_frontend.sh": _frontend_stub(frontend_marker),
                "bin/yearly_runoff_hydrograph_aggregation.sh": _publication_stub(pub_marker),
            },
            frontend_exit=frontend_exit,
            publication_exit=publication_exit,
        )
        return result, frontend_marker, pub_marker

    def test_all_three_succeed_exits_zero(self, tmp_path):
        result, frontend_marker, pub_marker = self._run_daily(tmp_path)
        assert result.returncode == 0, result.stdout + result.stderr
        assert frontend_marker.exists(), "frontend did not run"
        assert pub_marker.exists(), "publication did not run"

    def test_publication_runs_before_frontend(self, tmp_path):
        """Ordering: Luigi → publication → frontend.  Each stub appends to a
        shared event log when actually invoked; the line order proves real
        subprocess invocation order, not echo-text positions in wrapper stdout.
        Each stage must appear exactly once.
        """
        result, shared_log = _run_daily_with_shared_log(tmp_path)
        assert result.returncode == 0, result.stdout + result.stderr
        assert shared_log.exists(), "no events were logged; stubs may not have run"
        events = [line for line in shared_log.read_text().splitlines() if line.strip()]
        assert events == ["event:luigi", "event:publication", "event:frontend"], (
            f"unexpected event order or count: {events}"
        )

    def test_publication_called_exactly_once(self, tmp_path):
        """Publication must not be called multiple times per invocation."""
        _, _, pub_marker = self._run_daily(tmp_path)
        lines = [line for line in pub_marker.read_text().splitlines() if line.strip()]
        assert len(lines) == 1, f"expected 1 publication call, got {len(lines)}: {lines}"

    def test_env_arg_forwarded_to_publication_with_reuse_tunnel_flag(self, tmp_path):
        """The daily wrapper passes "$1" (quoted) and "--reuse-existing-tunnel"
        to the publication child.  The env-file arg contains a space to verify
        quoting: if the wrapper used unquoted $1, the child would receive only
        the first token.  The stub records argc on the first line then one arg
        per line via "$@" (not $* which word-splits), so exact path identity
        can be asserted.
        """
        result, spaced_arg, pub_marker = _run_daily_spaced_env(tmp_path)
        assert pub_marker.exists(), (
            f"publication stub did not run; stderr: {result.stderr}"
        )
        lines = [line for line in pub_marker.read_text().splitlines() if line.strip()]
        assert lines, "publication marker is empty"
        argc = int(lines[0])
        args = lines[1:]
        assert argc == 2, (
            f"expected argc=2 (env-path + flag), got {argc}: {args}"
        )
        assert args[0] == spaced_arg, (
            f"env path not forwarded intact; expected {spaced_arg!r}, got {args[0]!r}"
        )
        assert args[1] == "--reuse-existing-tunnel", (
            f"--reuse-existing-tunnel not second arg; got: {args}"
        )

    def test_frontend_runs_after_publication_failure(self, tmp_path):
        """Publication failure must not stop the frontend update."""
        result, frontend_marker, pub_marker = self._run_daily(
            tmp_path, publication_exit=1
        )
        assert pub_marker.exists(), "publication stub did not run"
        assert frontend_marker.exists(), "frontend did not run after publication failure"
        assert result.returncode != 0

    def test_frontend_runs_after_luigi_and_publication_failure(self, tmp_path):
        """Both upstream failures must not prevent the frontend from running."""
        result, frontend_marker, _ = self._run_daily(
            tmp_path, compose_exit=1, publication_exit=1
        )
        assert frontend_marker.exists(), "frontend did not run after upstream failures"
        assert result.returncode != 0

    @pytest.mark.parametrize("pub_exit", [2, 4, 5, 6])
    def test_publication_nonzero_exits_fail_aggregate(self, tmp_path, pub_exit):
        """Publication exits 2/4/6 (which the LOCAL runner tolerates) must still
        fail the SERVER wrapper's aggregate.  The server wrapper does not apply
        any per-code tolerance.
        """
        result, _, _ = self._run_daily(
            tmp_path, compose_exit=0, frontend_exit=0, publication_exit=pub_exit
        )
        assert result.returncode != 0, (
            f"expected nonzero overall for publication exit {pub_exit}; "
            f"got 0: {result.stdout}"
        )

    def test_luigi_failure_does_not_prevent_publication(self, tmp_path):
        """Publication runs even when Luigi failed: it uses stored observations
        and doesn't assert upstream data were refreshed.
        """
        result, _, pub_marker = self._run_daily(
            tmp_path, compose_exit=1, publication_exit=0, frontend_exit=0
        )
        assert pub_marker.exists(), "publication did not run after Luigi failure"
        assert result.returncode != 0  # Luigi failure propagates to aggregate


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
