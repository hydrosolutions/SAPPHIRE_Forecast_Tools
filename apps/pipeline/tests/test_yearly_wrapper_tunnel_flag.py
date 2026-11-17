"""Tests for the --reuse-existing-tunnel flag added to
bin/yearly_runoff_hydrograph_aggregation.sh (PP-075 W2).

The flag lets a parent wrapper (e.g. bin/run_daily_maintenance.sh) own the
tunnel lifecycle: child must neither establish a new tunnel nor install the
cleanup trap.  Standalone (no flag) behaviour must remain unchanged.

Four observable axes are tested:

1. Tunnel establishment: establish_ssh_tunnel() sources
   $ieasyhydroforecast_data_ref_dir/bin/.ssh/open_ssh_tunnel.sh when
   ieasyhydroforecast_ssh_to_iEH=true.  A marker-writing stub at that path
   lets us assert whether the tunnel was attempted.

2. Cleanup trap: cleanup() always prints "| Cleaning up".  When
   trap cleanup EXIT is installed (standalone mode), that text appears in
   the wrapper's stdout at process exit.  When the flag is passed (reuse
   mode), the trap is NOT installed and the text must be absent.

3. Inherited-PID safety: cleanup() calls kill on
   ieasyhydroforecast_ssh_tunnel_pid when set.  _run() always clears that
   variable from the subprocess environment so a real PID inherited from
   the test runner's os.environ can never be killed.  Dedicated PID tests
   supply a synthetic PID and intercept kill via BASH_ENV so no real
   process is signalled; a marker file records every PID targeted.

4. Docker argv forwarding: the docker stub logs its argv to DOCKER_ARGV_LOG
   when set, so tests can assert the actual command line passed to docker
   run rather than relying on log text alone.

All tests drive the real wrapper script end-to-end with stubbed docker.
No real Docker daemon, SSH service or iEH HF server is required.
"""

import os
import pathlib
import stat
import subprocess

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
_WRAPPER = _REPO_ROOT / "bin" / "yearly_runoff_hydrograph_aggregation.sh"

# Minimal docker stub: enough for the wrapper's docker info / image inspect /
# ps / run / inspect / rm calls to succeed.  When DOCKER_ARGV_LOG is set in
# the environment, the 'run' arm appends the full argv (one token per line)
# to that file so tests can inspect exactly what was forwarded to docker run.
_DOCKER_STUB = """\
#!/usr/bin/env bash
case "$1" in
  info) exit 0 ;;
  image) exit 0 ;;
  ps) echo ""; exit 0 ;;
  pull) exit 0 ;;
  run)
    if [ -n "${DOCKER_ARGV_LOG:-}" ]; then
      printf '%s\\n' "$@" >> "${DOCKER_ARGV_LOG}"
    fi
    echo "fake docker run"
    exit "${FAKE_DOCKER_EXIT_CODE:-0}"
    ;;
  inspect)
    echo "${FAKE_DOCKER_EXIT_CODE:-0}"
    exit 0
    ;;
  rm) exit 0 ;;
  *) exit 0 ;;
esac
"""

# BASH_ENV snippet that overrides the kill builtin with a shell function.
# The function records each targeted PID in KILL_MARKER_PATH (one per line)
# and returns 0 without signalling anything.  Injected as BASH_ENV so the
# override is in effect before any script body runs (including sourced files).
# Shell functions take precedence over builtins in bash, so no enable -n is
# needed.
_KILL_INTERCEPT_BASH_ENV = """\
kill() {
    if [ -n "${KILL_MARKER_PATH:-}" ]; then
        printf '%s\\n' "$@" >> "${KILL_MARKER_PATH}"
    fi
    return 0
}
"""


def _make_env_file(tmp_path: pathlib.Path) -> pathlib.Path:
    """Minimal env file at a path read_configuration() accepts."""
    config_dir = tmp_path / "root" / "data_ref" / "config"
    config_dir.mkdir(parents=True)
    env_file = config_dir / ".env_kghm"
    env_file.write_text("")
    return env_file


def _make_tunnel_stub(tmp_path: pathlib.Path, marker: pathlib.Path) -> None:
    """Create a fake open_ssh_tunnel.sh at the path establish_ssh_tunnel()
    sources.  It writes a single line to `marker` so tests can assert
    whether it was executed.
    """
    # ieasyhydroforecast_data_ref_dir is derived as dirname(dirname(env_file))
    # = tmp_path/root/data_ref; the tunnel script lives two levels below.
    ssh_dir = tmp_path / "root" / "data_ref" / "bin" / ".ssh"
    ssh_dir.mkdir(parents=True)
    tunnel_script = ssh_dir / "open_ssh_tunnel.sh"
    tunnel_script.write_text(f'echo "tunnel opened" > "{marker}"\n')
    tunnel_script.chmod(tunnel_script.stat().st_mode | stat.S_IEXEC)


def _run(
    tmp_path: pathlib.Path,
    extra_args: list[str] | None = None,
    with_ssh: bool = False,
    docker_exit: int = 0,
    extra_env: dict | None = None,
) -> subprocess.CompletedProcess:
    """Run the yearly wrapper with a stubbed docker.

    extra_args: positional args after the env file path, e.g.
        ["--reuse-existing-tunnel"] or ["--target-year", "2023"].
    with_ssh: if True, set ieasyhydroforecast_ssh_to_iEH=true in the env so
        establish_ssh_tunnel() actually tries to source the tunnel script.
    extra_env: additional env vars merged into the subprocess environment
        after the standard sanitisation below.
    """
    env_file = _make_env_file(tmp_path)

    bin_dir = tmp_path / "fakebin"
    bin_dir.mkdir()
    stub = bin_dir / "docker"
    stub.write_text(_DOCKER_STUB)
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    env["FAKE_DOCKER_EXIT_CODE"] = str(docker_exit)
    env.pop("ieasyhydroforecast_env_file_path", None)
    env.pop("IEASYHYDROHF_HOST", None)
    # Clear any real tunnel PID that may be present in the test runner's
    # environment — if inherited, cleanup() would kill it.
    env.pop("ieasyhydroforecast_ssh_tunnel_pid", None)
    if with_ssh:
        env["ieasyhydroforecast_ssh_to_iEH"] = "true"
    else:
        env.pop("ieasyhydroforecast_ssh_to_iEH", None)
    if extra_env:
        env.update(extra_env)

    cmd = ["bash", str(_WRAPPER), str(env_file)] + (extra_args or [])
    return subprocess.run(
        cmd,
        cwd=str(_REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


class TestReuseExistingTunnelFlag:
    """--reuse-existing-tunnel suppresses tunnel setup and cleanup trap."""

    def test_without_flag_and_no_ssh_no_tunnel_attempted(self, tmp_path):
        """Baseline: without the flag and without ieasyhydroforecast_ssh_to_iEH,
        establish_ssh_tunnel() short-circuits early and no tunnel is started.
        The wrapper must still succeed.
        """
        result = _run(tmp_path)
        assert result.returncode == 0, result.stdout + result.stderr

    def test_with_flag_wrapper_succeeds(self, tmp_path):
        """--reuse-existing-tunnel must not cause the wrapper to fail."""
        result = _run(tmp_path, extra_args=["--reuse-existing-tunnel"])
        assert result.returncode == 0, result.stdout + result.stderr

    def test_without_flag_tunnel_established_when_ssh_enabled(self, tmp_path):
        """Without the flag and with ieasyhydroforecast_ssh_to_iEH=true,
        establish_ssh_tunnel() sources the tunnel script.
        """
        marker = tmp_path / "tunnel_opened.marker"
        _make_tunnel_stub(tmp_path, marker)
        result = _run(tmp_path, with_ssh=True)
        assert result.returncode == 0, result.stdout + result.stderr
        assert marker.exists(), (
            "expected tunnel stub to be sourced (marker not written); "
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )

    def test_with_flag_tunnel_not_established_even_when_ssh_enabled(self, tmp_path):
        """With --reuse-existing-tunnel, establish_ssh_tunnel() must NOT be
        called, even when ieasyhydroforecast_ssh_to_iEH=true.
        """
        marker = tmp_path / "tunnel_opened.marker"
        _make_tunnel_stub(tmp_path, marker)
        result = _run(tmp_path, extra_args=["--reuse-existing-tunnel"], with_ssh=True)
        assert result.returncode == 0, result.stdout + result.stderr
        assert not marker.exists(), (
            "tunnel stub was sourced despite --reuse-existing-tunnel; "
            "establish_ssh_tunnel() must be suppressed in reuse mode"
        )

    def test_without_flag_cleanup_trap_installed(self, tmp_path):
        """Without --reuse-existing-tunnel, 'trap cleanup EXIT' is installed.
        cleanup() always prints '| Cleaning up', which must appear in stdout.
        """
        result = _run(tmp_path)
        assert "Cleaning up" in result.stdout, (
            "cleanup() output not found; trap may not have been installed"
        )

    def test_with_flag_cleanup_trap_not_installed(self, tmp_path):
        """With --reuse-existing-tunnel, 'trap cleanup EXIT' must NOT be
        installed; the parent wrapper owns cleanup.  cleanup() prints
        '| Cleaning up' — its absence proves the trap was not registered.
        """
        result = _run(tmp_path, extra_args=["--reuse-existing-tunnel"])
        assert "Cleaning up" not in result.stdout, (
            "cleanup() output found; trap should NOT be installed in reuse mode"
        )

    def test_with_flag_on_writer_failure_cleanup_not_called(self, tmp_path):
        """Even when the Docker container fails, --reuse-existing-tunnel must
        not trigger cleanup (the parent owns tunnel teardown).
        """
        result = _run(
            tmp_path, extra_args=["--reuse-existing-tunnel"], docker_exit=1
        )
        assert result.returncode != 0
        assert "Cleaning up" not in result.stdout, (
            "cleanup() should not run in reuse mode even on writer failure"
        )


class TestInheritedTunnelPid:
    """cleanup() calls kill on ieasyhydroforecast_ssh_tunnel_pid when set.

    Standalone mode must signal the inherited PID on exit; reuse mode must
    not.  kill is intercepted via BASH_ENV so no real process is targeted.
    The synthetic PID 19999 is chosen to avoid colliding with real processes
    even if the intercept were somehow bypassed.
    """

    _SYNTHETIC_PID = "19999"

    def _bash_env_path(self, tmp_path: pathlib.Path) -> pathlib.Path:
        f = tmp_path / "bash_env_kill_intercept.sh"
        f.write_text(_KILL_INTERCEPT_BASH_ENV)
        return f

    def test_standalone_cleanup_kills_inherited_pid(self, tmp_path):
        """Standalone mode: cleanup() must signal the inherited tunnel PID.

        kill is intercepted; the marker file records each PID targeted.
        """
        kill_marker = tmp_path / "kill_marker.txt"
        result = _run(
            tmp_path,
            extra_env={
                "ieasyhydroforecast_ssh_tunnel_pid": self._SYNTHETIC_PID,
                "BASH_ENV": str(self._bash_env_path(tmp_path)),
                "KILL_MARKER_PATH": str(kill_marker),
            },
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert "Cleaning up" in result.stdout, (
            "cleanup() was not called; trap may not have been installed"
        )
        assert kill_marker.exists(), (
            "kill was never called; cleanup() may not have signalled the PID"
        )
        killed_pids = kill_marker.read_text().split()
        assert self._SYNTHETIC_PID in killed_pids, (
            f"synthetic PID {self._SYNTHETIC_PID!r} not in kill calls: {killed_pids}"
        )

    def test_reuse_does_not_kill_inherited_pid(self, tmp_path):
        """Reuse mode: cleanup() must NOT be called, so the PID is not signalled.

        kill is intercepted; the marker file must remain absent.
        """
        kill_marker = tmp_path / "kill_marker.txt"
        result = _run(
            tmp_path,
            extra_args=["--reuse-existing-tunnel"],
            extra_env={
                "ieasyhydroforecast_ssh_tunnel_pid": self._SYNTHETIC_PID,
                "BASH_ENV": str(self._bash_env_path(tmp_path)),
                "KILL_MARKER_PATH": str(kill_marker),
            },
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert "Cleaning up" not in result.stdout, (
            "cleanup() ran in reuse mode; trap must NOT be installed"
        )
        assert not kill_marker.exists(), (
            "kill was called in reuse mode (marker exists); "
            "cleanup() must not signal PID in reuse mode"
        )


class TestDockerArgvForwarding:
    """docker run receives the correct argv — verified from the stub's argv
    log, not from wrapper log-text that could diverge from the actual call.
    """

    def test_target_year_forwarded_to_docker_argv(self, tmp_path):
        """--target-year 2022 must appear in the actual docker run argument list."""
        argv_log = tmp_path / "docker_argv.log"
        result = _run(
            tmp_path,
            extra_args=["--target-year", "2022"],
            extra_env={"DOCKER_ARGV_LOG": str(argv_log)},
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert argv_log.exists(), "docker run was not called"
        argv_tokens = argv_log.read_text().split()
        assert "--target-year" in argv_tokens, (
            f"--target-year not in docker run argv: {argv_tokens}"
        )
        assert "2022" in argv_tokens, (
            f"'2022' not in docker run argv: {argv_tokens}"
        )

    def test_no_target_year_not_in_docker_argv(self, tmp_path):
        """Without --target-year, docker run must not receive --target-year."""
        argv_log = tmp_path / "docker_argv.log"
        result = _run(
            tmp_path,
            extra_env={"DOCKER_ARGV_LOG": str(argv_log)},
        )
        assert result.returncode == 0, result.stdout + result.stderr
        if argv_log.exists():
            argv_tokens = argv_log.read_text().split()
            assert "--target-year" not in argv_tokens, (
                f"--target-year found in docker run argv without being requested: {argv_tokens}"
            )


class TestFlagParsing:
    """Flag-parsing contracts: --target-year still works alongside the new
    flag; unknown args still error; --help still prints usage.
    """

    def test_target_year_works_with_reuse_tunnel(self, tmp_path):
        """--target-year and --reuse-existing-tunnel can be combined."""
        result = _run(
            tmp_path,
            extra_args=["--target-year", "2022", "--reuse-existing-tunnel"],
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert "2022" in result.stdout, "target year not reflected in log output"

    def test_reuse_tunnel_before_target_year(self, tmp_path):
        """Flag order should not matter."""
        result = _run(
            tmp_path,
            extra_args=["--reuse-existing-tunnel", "--target-year", "2021"],
        )
        assert result.returncode == 0, result.stdout + result.stderr

    def test_unknown_arg_exits_nonzero(self, tmp_path):
        result = _run(tmp_path, extra_args=["--no-such-flag"])
        assert result.returncode != 0
        assert "unknown argument" in result.stderr.lower() or "unknown argument" in result.stdout.lower()

    def test_help_flag_exits_zero(self, tmp_path):
        """--help must still work (unchanged standalone path)."""
        bin_dir = tmp_path / "fakebin2"
        bin_dir.mkdir()
        result = subprocess.run(
            ["bash", str(_WRAPPER), "--help"],
            cwd=str(_REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0
        assert "reuse-existing-tunnel" in result.stdout
