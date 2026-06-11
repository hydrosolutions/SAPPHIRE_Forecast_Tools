"""Subprocess-driven regression tests for the set -u safety of the
yearly_runoff_hydrograph_aggregation.sh wrapper.

Background (2026-06-11): bin/yearly_runoff_hydrograph_aggregation.sh enables
set -euo pipefail at the top, then calls read_configuration() from
bin/utils/common_functions.sh. That helper is not set-u-safe (it dereferences
ieasyhydroforecast_env_file_path before it is set, and sources the operator .env
with set -a, so values containing an unescaped $ trigger parameter expansion
→ unbound variable under -u). The fix (Option A) wraps the call in the wrapper
only with set +u / set -u.

These tests drive child bash processes with a minimal env that does NOT inherit
the test runner's environment, mirroring the cron / fresh-shell scenario where
the bug manifests.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

import pytest

# Repo root: apps/iEasyHydroForecast/tests/test_*.py -> parents[3]
_REPO_ROOT = Path(__file__).resolve().parents[3]
_WRAPPER = _REPO_ROOT / "bin" / "yearly_runoff_hydrograph_aggregation.sh"
_COMMON_FUNCTIONS = _REPO_ROOT / "bin" / "utils" / "common_functions.sh"

# Minimal synthetic .env content that encodes the contract.
# The DUMMY_SECRET line intentionally contains an unescaped $ — this is what
# triggers the unbound-variable crash under set -u when the helper sources the
# file without quoting the value.
_ENV_CONTENT = textwrap.dedent("""\
    ieasyhydroforecast_data_root_dir=/tmp/dummy_sapphire_test
    ieasyhydroforecast_data_ref_dir=/tmp/dummy_sapphire_test/ref
    ieasyhydroforecast_env_file_path=/tmp/dummy_sapphire_test/.env
    ieasyhydroforecast_container_data_ref_dir=/app/ref
    DUMMY_SECRET=ab$cdMASKED
    DUMMY_STATION_CODE=19999
    DUMMY_FLAG=true
""")


_ENV_FILENAME = "sapphire_test.env"


def _write_env_file(directory: Path) -> Path:
    """Write the synthetic .env into the given directory and return its path."""
    env_path = directory / _ENV_FILENAME
    with env_path.open("w") as f:
        f.write(_ENV_CONTENT)
    return env_path


def _minimal_env() -> dict[str, str]:
    """Return a minimal child environment with only PATH.

    Intentionally does NOT export ieasyhydroforecast_env_file_path so the
    child shell starts without any of the test runner's env — this mirrors
    a fresh cron shell.
    """
    return {"PATH": "/usr/bin:/bin:/usr/local/bin"}


def test_wrapper_survives_set_u_with_dollar_in_env():
    """Wrapper must not abort with 'unbound variable' when sourcing config.

    The wrapper is expected to fail later (no Docker, no SSH tunnel) — that is
    acceptable. The contract is that the config-reading section completes
    without a set -u error.
    """
    with tempfile.TemporaryDirectory(prefix="sapphire_test_") as tmpdir:
        env_file = _write_env_file(Path(tmpdir))
        result = subprocess.run(
            ["bash", str(_WRAPPER), str(env_file)],
            capture_output=True,
            text=True,
            env=_minimal_env(),
            check=False,
        )
        combined = result.stdout + result.stderr
        assert "unbound variable" not in combined, (
            "wrapper aborted with 'unbound variable' during config sourcing; "
            f"stderr={result.stderr!r}"
        )
        # Belt-and-suspenders: also check for the specific helper line marker.
        assert not ("common_functions.sh: line" in combined and "unbound variable" in combined), (
            f"common_functions.sh raised an unbound variable error; stderr={result.stderr!r}"
        )


@pytest.mark.xfail(
    reason=(
        "documents helper-level set-u unsafety; wrapper guards instead "
        "— Option A scope per fix_yearly_runoff_wrapper_set_u"
    ),
    strict=False,
)
def test_read_configuration_survives_set_u_directly():
    """Unit test: read_configuration() itself is NOT set-u-safe (known gap).

    Sources common_functions.sh under set -euo pipefail with
    ieasyhydroforecast_env_file_path unset, then calls read_configuration.
    Expected to fail with 'unbound variable' — that is the documented
    helper-level unsafety that the wrapper guard works around (Option A).

    Marked xfail so that a future Option-B fix to the helper turns this
    green without breaking anything else.
    """
    with tempfile.TemporaryDirectory(prefix="sapphire_test_") as tmpdir:
        env_file = _write_env_file(Path(tmpdir))
        script = textwrap.dedent(f"""\
            set -euo pipefail
            unset ieasyhydroforecast_env_file_path 2>/dev/null || true
            source "{_COMMON_FUNCTIONS}"
            read_configuration "{env_file}"
        """)
        result = subprocess.run(
            ["bash", "-c", script],
            capture_output=True,
            text=True,
            env=_minimal_env(),
            check=False,
        )
        combined = result.stdout + result.stderr
        # This assertion is expected to FAIL (hence xfail): the helper-level
        # call raises 'unbound variable'.
        assert "unbound variable" not in combined, (
            f"helper raised 'unbound variable' as expected (xfail); stderr={result.stderr!r}"
        )


def test_wrapper_dollar_in_env_does_not_emit_unbound_variable():
    """Precise signature test: the exact crash string must not appear in stderr.

    Pre-fix, the wrapper emits:
        common_functions.sh: line NNN: ieasyhydroforecast_env_file_path: unbound variable

    Post-fix, that line must be absent.
    """
    with tempfile.TemporaryDirectory(prefix="sapphire_test_") as tmpdir:
        env_file = _write_env_file(Path(tmpdir))
        result = subprocess.run(
            ["bash", str(_WRAPPER), str(env_file)],
            capture_output=True,
            text=True,
            env=_minimal_env(),
            check=False,
        )
        assert "ieasyhydroforecast_env_file_path: unbound variable" not in result.stderr, (
            "exact crash signature found in stderr; fix not applied or not effective; "
            f"stderr={result.stderr!r}"
        )


if __name__ == "__main__":
    # Manual smoke run: `python test_read_configuration_set_u.py`
    sys.exit(
        subprocess.call(
            ["pytest", "-v", __file__],
        )
    )
