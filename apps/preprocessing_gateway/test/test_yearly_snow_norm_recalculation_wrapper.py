"""Tests for bin/yearly_snow_norm_recalculation.sh's exit-code contract.

PREPG-020 requires that a preservation-read failure inside
recalculate_snow_norms.py aborts the run with a non-zero process exit
(dg_utils.SnowPreservationReadError, uncaught, crashes the script).
For that to actually reach a cron job's monitoring, the *shell*
wrapper that launches the container must forward the container's
exit code — otherwise the abort is invisible outside the container
log (this is the same failure shape as INFRA-023, see
doc/prod/update_deployment_checklist.md).

Prior to this fix, the wrapper computed ``CONTAINER_EXIT_CODE`` but
never used it, so the wrapper itself always exited 0 regardless of
what happened inside the container. The fix copies the last line of
the sibling script, ``bin/yearly_runoff_hydrograph_aggregation.sh``:
``exit "$CONTAINER_EXIT_CODE"``.

A second, independent bug (PREPG-020 review finding 4) was in how
``CONTAINER_EXIT_CODE`` itself gets computed: ``docker run ... | tee
"$SERVICE_LOG"`` means the immediately-following ``$?`` is *tee's*
exit status, not ``docker run``'s — so the ``EXIT_CODE`` used as a
fallback (when ``docker inspect`` also fails) was silently always 0.
The fix captures ``${PIPESTATUS[0]}`` right after the pipeline
instead. ``test_wrapper_exits_nonzero_when_container_fails_and_inspect_also_fails``
below is the test that would have caught this — it is the only test
here where ``docker inspect`` does not unconditionally succeed.

These tests drive the real wrapper script end to end with a stub
``docker`` executable placed first on PATH, so no real Docker daemon
or image is needed. The stub answers every subcommand the wrapper
calls (``info``, ``image inspect``, ``ps``, ``run``, ``inspect``,
``rm``). ``run`` exits with ``$FAKE_DOCKER_EXIT_CODE`` (default 0).
``inspect`` normally echoes that same value and exits 0 (so
``CONTAINER_EXIT_CODE`` is resolved from ``docker inspect``, as in
production); setting ``$FAKE_INSPECT_FAILS=1`` makes ``inspect``
itself fail instead, forcing the wrapper onto its ``$EXIT_CODE``
fallback path.
"""

import os
import pathlib
import stat
import subprocess


def _find_repo_root() -> pathlib.Path:
    """Locate the repo root from any CWD by finding the wrapper script."""
    here = pathlib.Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "bin" / "yearly_snow_norm_recalculation.sh"
        if candidate.is_file():
            return parent
    raise FileNotFoundError(
        f"Could not locate bin/yearly_snow_norm_recalculation.sh from {here} or any of its parents."
    )


REPO_ROOT = _find_repo_root()
WRAPPER_SCRIPT = REPO_ROOT / "bin" / "yearly_snow_norm_recalculation.sh"

_DOCKER_STUB = """#!/usr/bin/env bash
# Stub `docker` for testing bin/yearly_snow_norm_recalculation.sh
# without a real Docker daemon. `run` reports the exit code from
# $FAKE_DOCKER_EXIT_CODE (default 0). `inspect` normally echoes that
# same value and succeeds (mirroring production: the wrapper reads
# the container's real State.ExitCode from `docker inspect`); setting
# $FAKE_INSPECT_FAILS=1 makes `inspect` itself fail instead, so the
# wrapper must fall back to the exit code it captured from the
# pipeline directly. Everything else the wrapper calls just succeeds.
case "$1" in
  info) exit 0 ;;
  image) exit 0 ;;
  ps) echo ""; exit 0 ;;
  pull) exit 0 ;;
  run)
    echo "fake docker run output"
    exit "${FAKE_DOCKER_EXIT_CODE:-0}"
    ;;
  inspect)
    if [ "${FAKE_INSPECT_FAILS:-0}" = "1" ]; then
      exit 1
    fi
    echo "${FAKE_DOCKER_EXIT_CODE:-0}"
    exit 0
    ;;
  rm) exit 0 ;;
  *) exit 0 ;;
esac
"""


def _make_env_file(tmp_path: pathlib.Path) -> pathlib.Path:
    """Build a minimal env file at a path read_configuration() accepts.

    read_configuration() (bin/utils/common_functions.sh) requires the
    env file path to end in one of the four-character org codes
    ("kghm", "tjhm", "uzhm") and derives
    ieasyhydroforecast_data_root_dir / _data_ref_dir two directories
    above it, so the file must sit at <root>/<any>/config/.env_kghm.
    """
    config_dir = tmp_path / "root" / "data_ref" / "config"
    config_dir.mkdir(parents=True)
    env_file = config_dir / ".env_kghm"
    env_file.write_text("")
    return env_file


def _run_wrapper(
    tmp_path: pathlib.Path, fake_exit_code: int, inspect_fails: bool = False
) -> subprocess.CompletedProcess:
    env_file = _make_env_file(tmp_path)

    bin_dir = tmp_path / "fakebin"
    bin_dir.mkdir()
    stub = bin_dir / "docker"
    stub.write_text(_DOCKER_STUB)
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    env["FAKE_DOCKER_EXIT_CODE"] = str(fake_exit_code)
    if inspect_fails:
        env["FAKE_INSPECT_FAILS"] = "1"
    # Explicitly not set: ieasyhydroforecast_ssh_to_iEH, so
    # establish_ssh_tunnel() short-circuits without touching a real
    # SSH tunnel script.

    return subprocess.run(
        ["bash", str(WRAPPER_SCRIPT), str(env_file)],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


class TestYearlySnowNormRecalculationWrapperExitCode:
    """bin/yearly_snow_norm_recalculation.sh must exit with the
    container's status, not always exit 0."""

    def test_wrapper_exits_zero_when_container_succeeds(self, tmp_path):
        result = _run_wrapper(tmp_path, fake_exit_code=0)
        assert result.returncode == 0, result.stdout + result.stderr

    def test_wrapper_exits_nonzero_when_container_fails(self, tmp_path):
        """A failing stub (simulating a SnowPreservationReadError abort
        inside the container) must make the wrapper itself fail — this
        is what lets a cron job's exit-code monitoring see the abort.
        """
        result = _run_wrapper(tmp_path, fake_exit_code=1)
        assert result.returncode == 1, result.stdout + result.stderr

    def test_wrapper_forwards_the_exact_container_exit_code(self, tmp_path):
        """The wrapper forwards the container's own exit code value,
        not just a generic non-zero status."""
        result = _run_wrapper(tmp_path, fake_exit_code=17)
        assert result.returncode == 17, result.stdout + result.stderr

    def test_wrapper_exits_nonzero_when_container_fails_and_inspect_also_fails(self, tmp_path):
        """PREPG-020 review finding 4: docker run's exit status is
        piped through `tee`, so the immediately-following `$?` is
        tee's status (effectively always 0), not docker run's. If
        `docker inspect` (the normal source of the real exit code)
        also fails, the wrapper falls back to that `$?` — which, before
        the ${PIPESTATUS[0]} fix, silently produced a fallback of 0
        even though the container genuinely failed. Every other test
        in this module lets `docker inspect` succeed, which would not
        catch this: it must specifically fail here to exercise the
        fallback path at all.
        """
        result = _run_wrapper(tmp_path, fake_exit_code=9, inspect_fails=True)
        assert result.returncode == 9, result.stdout + result.stderr
