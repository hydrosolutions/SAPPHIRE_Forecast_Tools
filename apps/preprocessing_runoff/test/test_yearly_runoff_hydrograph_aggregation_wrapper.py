"""Tests for bin/yearly_runoff_hydrograph_aggregation.sh's exit-code contract.

INFRA-023: this wrapper computed ``CONTAINER_EXIT_CODE`` and did
``exit "$CONTAINER_EXIT_CODE"`` (the correct final line, and the
pattern the sibling ``bin/yearly_snow_norm_recalculation.sh`` wrapper
was told to copy — see that script's own fix and
apps/preprocessing_gateway/test/test_yearly_snow_norm_recalculation_wrapper.py),
but the *value it exits with* could name the wrong cause.

Note this script's semantics differ from the sibling's, and an earlier
revision of INFRA-023 got it wrong. This wrapper sets
``set -euo pipefail`` (:57) and the later ``set +e`` disables *errexit
only*, so pipefail stays active: a bare ``$?`` after
``docker run ... | tee "$SERVICE_LOG"`` is NOT tee's status, it is the
rightmost non-zero status in the pipeline. Docker's failure therefore
does propagate, and the wrapper does NOT exit 0 on a Docker failure.

What a bare ``$?`` gets wrong is the case where ``tee`` *also* fails —
disk full, or permissions on ``$SERVICE_LOG``. Pipefail then reports
tee's incidental status instead of docker's, so an operator reading the
exit code sees the wrong cause. ``${PIPESTATUS[0]}`` names docker run's
own status unconditionally.

(``yearly_snow_norm_recalculation.sh`` has no pipefail, so there a bare
``$?`` really is tee's status and the exit-0 reading does hold. Do not
carry that reasoning across.)

``test_wrapper_uses_dockers_own_exit_code_not_tees`` below is the test
that pins the real defect: it is the only one where ``tee`` itself
fails, which is the sole case where the old and new code diverge.

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
        candidate = parent / "bin" / "yearly_runoff_hydrograph_aggregation.sh"
        if candidate.is_file():
            return parent
    raise FileNotFoundError(
        f"Could not locate bin/yearly_runoff_hydrograph_aggregation.sh from {here} "
        "or any of its parents."
    )


REPO_ROOT = _find_repo_root()
WRAPPER_SCRIPT = REPO_ROOT / "bin" / "yearly_runoff_hydrograph_aggregation.sh"

_DOCKER_STUB = """#!/usr/bin/env bash
# Stub `docker` for testing bin/yearly_runoff_hydrograph_aggregation.sh
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

# Stub `tee` that fails on the *unnamed* (non `-a`) invocation only -- that
# is the one call in the wrapper that captures `docker run`'s output
# (`2>&1 | tee "$SERVICE_LOG"`); the `log_message()` helper's own
# `tee -a "$log_file"` calls are passed straight through to the real `tee`
# so the rest of the script's logging keeps working. This reproduces a
# real failure mode independent of Docker (e.g. the log volume filling up
# or a permissions problem) to prove the wrapper reads *docker run's own*
# exit code via ${PIPESTATUS[0]}, not whatever `tee` happens to return.
_TEE_ALSO_FAILS_STUB = """#!/usr/bin/env bash
if [ "$1" = "-a" ]; then
    exec /usr/bin/tee "$@"
fi
cat
exit 2
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
    tmp_path: pathlib.Path,
    fake_exit_code: int,
    inspect_fails: bool = False,
    tee_also_fails: bool = False,
) -> subprocess.CompletedProcess:
    env_file = _make_env_file(tmp_path)

    bin_dir = tmp_path / "fakebin"
    bin_dir.mkdir()
    stub = bin_dir / "docker"
    stub.write_text(_DOCKER_STUB)
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

    if tee_also_fails:
        tee_stub = bin_dir / "tee"
        tee_stub.write_text(_TEE_ALSO_FAILS_STUB)
        tee_stub.chmod(tee_stub.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    env["FAKE_DOCKER_EXIT_CODE"] = str(fake_exit_code)
    if inspect_fails:
        env["FAKE_INSPECT_FAILS"] = "1"
    # Explicitly not set: ieasyhydroforecast_ssh_to_iEH, so
    # establish_ssh_tunnel() short-circuits without touching a real
    # SSH tunnel script. Also drop anything the parent shell may have
    # exported for these so a previous read_configuration call in this
    # same shell can't leak in.
    env.pop("ieasyhydroforecast_ssh_to_iEH", None)
    env.pop("ieasyhydroforecast_env_file_path", None)
    env.pop("IEASYHYDROHF_HOST", None)

    return subprocess.run(
        ["bash", str(WRAPPER_SCRIPT), str(env_file)],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


class TestYearlyRunoffHydrographAggregationWrapperExitCode:
    """bin/yearly_runoff_hydrograph_aggregation.sh must exit with the
    container's status, not always exit 0."""

    def test_wrapper_exits_zero_when_container_succeeds(self, tmp_path):
        result = _run_wrapper(tmp_path, fake_exit_code=0)
        assert result.returncode == 0, result.stdout + result.stderr

    def test_wrapper_exits_nonzero_when_container_fails(self, tmp_path):
        """A failing stub (simulating the writer erroring inside the
        container) must make the wrapper itself fail — this is what
        lets a cron job's exit-code monitoring see the failure.
        """
        result = _run_wrapper(tmp_path, fake_exit_code=1)
        assert result.returncode == 1, result.stdout + result.stderr

    def test_wrapper_forwards_the_exact_container_exit_code(self, tmp_path):
        """The wrapper forwards the container's own exit code value,
        not just a generic non-zero status. 4/5 are the writer's own
        SDK_FAILED / write-failure codes (see the module docstring in
        bin/yearly_runoff_hydrograph_aggregation.sh)."""
        result = _run_wrapper(tmp_path, fake_exit_code=4)
        assert result.returncode == 4, result.stdout + result.stderr

    def test_wrapper_exits_nonzero_when_container_fails_and_inspect_also_fails(self, tmp_path):
        """INFRA-023: docker run's exit status is piped through `tee`,
        so the immediately-following `$?` used to be tee's status
        (effectively always 0), not docker run's. If `docker inspect`
        (the normal source of the real exit code) also fails, the
        wrapper falls back to that `$?` — which, before the
        ${PIPESTATUS[0]} fix, silently produced a fallback of 0 even
        though the container genuinely failed. Every other test in
        this module lets `docker inspect` succeed, which would not
        catch this: it must specifically fail here to exercise the
        fallback path at all.
        """
        result = _run_wrapper(tmp_path, fake_exit_code=9, inspect_fails=True)
        assert result.returncode == 9, result.stdout + result.stderr

    def test_wrapper_uses_dockers_own_exit_code_not_tees(self, tmp_path):
        """The script keeps `set -euo pipefail`, so a bare `$?` after the
        pipe is usually already pipefail-corrected to docker run's status
        -- UNLESS `tee` *itself* also exits non-zero (e.g. it could not
        write $SERVICE_LOG), in which case pipefail reports the *rightmost*
        non-zero command, which is tee, masking docker run's real exit
        code. Combined with a failing `docker inspect` (forcing the
        fallback path), the pre-fix `EXIT_CODE=$?` would report tee's
        incidental error code (2 here) instead of the container's actual
        exit code (9). ${PIPESTATUS[0]} is captured before anything else
        can touch it, so it is immune to what tee itself returns.
        """
        result = _run_wrapper(tmp_path, fake_exit_code=9, inspect_fails=True, tee_also_fails=True)
        assert result.returncode == 9, result.stdout + result.stderr
