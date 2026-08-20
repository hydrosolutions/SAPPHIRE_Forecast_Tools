"""Subprocess-driven regression tests for the dashboard WebSocket origin
derivation in read_configuration() (bin/utils/common_functions.sh).

Background (INFRA-032, 2026-08-20): a deployment's env file now wins when it
sets ieasyhydroforecast_url_pentad / ieasyhydroforecast_url_decad; the values
are otherwise derived from ieasyhydroforecast_url, keyed on the last four
characters of the env-file path ("kghm" / "tjhm" / "uzhm"). Immediately
before sourcing the env file, read_configuration unsets both variables so a
second call in the same shell cannot inherit an earlier deployment's values
(see doc/prod/long_term_deploy_runbook.md, which instructs operators to
`source` this file and call read_configuration directly in their current
shell).

These tests drive child bash processes with a minimal env that does NOT
inherit the test runner's environment, following the precedent in
test_read_configuration_set_u.py. Placeholders only (10.0.0.1, example.org,
host.example) - no real IP addresses, internal hostnames or credentials.
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
_COMMON_FUNCTIONS = _REPO_ROOT / "bin" / "utils" / "common_functions.sh"

# Marker delimiters so the assertions on stdout are immune to the many other
# "| ..." lines read_configuration() prints.
_PENTAD_MARKER = "TEST_PENTAD="
_DECAD_MARKER = "TEST_DECAD="

_BASE_URL = "example.org"

_ORG_ENDINGS = {
    "kghm": ("kyg.fc." + _BASE_URL, "demo.fc.decade." + _BASE_URL),
    "tjhm": ("taj.fc." + _BASE_URL, "taj.fc.decade." + _BASE_URL),
    "uzhm": ("uzb.fc." + _BASE_URL, "uzb.fc.decade." + _BASE_URL),
}


def _minimal_env() -> dict[str, str]:
    """Return a minimal child environment with only PATH.

    Mirrors a fresh cron/operator shell: no inherited test-runner env.
    """
    return {"PATH": "/usr/bin:/bin:/usr/local/bin"}


def _write_env_file(directory: Path, suffix: str, content: str) -> Path:
    """Write an env file whose path ends in `suffix` (no extension).

    read_configuration keys on the LAST FOUR CHARACTERS of the full env file
    path, so the file must be named to end exactly in "kghm" / "tjhm" /
    "uzhm" with nothing after it (no ".env").
    """
    assert len(suffix) == 4
    env_path = directory / f"deployment_{suffix}"
    env_path.write_text(content)
    return env_path


def _run_script(script: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        env=_minimal_env() if env is None else env,
        check=False,
    )


def _read_configuration_snippet(env_file: Path, marker_prefix: str = "") -> str:
    """A snippet that sources common_functions.sh, calls read_configuration,
    and echoes the resulting pentad/decad values behind stable markers."""
    return textwrap.dedent(f"""\
        source "{_COMMON_FUNCTIONS}"
        {marker_prefix}
        read_configuration "{env_file}"
        echo "{_PENTAD_MARKER}${{ieasyhydroforecast_url_pentad}}"
        echo "{_DECAD_MARKER}${{ieasyhydroforecast_url_decad}}"
    """)


def _extract_marker(stdout: str, marker: str) -> str:
    for line in stdout.splitlines():
        if line.startswith(marker):
            return line[len(marker) :]
    raise AssertionError(f"marker {marker!r} not found in stdout:\n{stdout}")


# ---------------------------------------------------------------------------
# Test 1: unset -> derived, for all three org endings
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("suffix", sorted(_ORG_ENDINGS))
def test_unset_derives_expected_origins(suffix: str):
    expected_pentad, expected_decad = _ORG_ENDINGS[suffix]
    with tempfile.TemporaryDirectory(prefix="sapphire_test_") as tmpdir:
        env_file = _write_env_file(Path(tmpdir), suffix, f"ieasyhydroforecast_url={_BASE_URL}\n")
        result = _run_script(_read_configuration_snippet(env_file))
        assert result.returncode == 0, result.stderr
        pentad = _extract_marker(result.stdout, _PENTAD_MARKER)
        decad = _extract_marker(result.stdout, _DECAD_MARKER)
        assert pentad == expected_pentad
        assert decad == expected_decad


def test_kghm_decad_prefix_intentionally_asymmetric_with_pentad():
    """kghm decad uses 'demo.fc.decade.', NOT 'kyg.fc.decade.' - confirmed
    intentional by the owner. This is the regression guard against someone
    "fixing" it to match taj/uzb.

    The exact strings this checks for kghm duplicate
    test_unset_derives_expected_origins (test 1), which already pins all
    three org endings. What that parametrized test does NOT assert is the
    cross-org STRUCTURAL property that would break if kghm's decad prefix
    were ever "fixed" to match pentad, or if tjhm/uzhm's shared-stem
    derivation were ever refactored apart: pentad and decad prefixes DIFFER
    entirely for kghm, but share a common stem ("<org>.fc.") for tjhm and
    uzhm. That is the property this test earns its place by asserting.
    """
    with tempfile.TemporaryDirectory(prefix="sapphire_test_") as tmpdir:
        tmp = Path(tmpdir)
        prefixes: dict[str, tuple[str, str]] = {}
        for suffix in sorted(_ORG_ENDINGS):
            env_file = _write_env_file(tmp, suffix, f"ieasyhydroforecast_url={_BASE_URL}\n")
            result = _run_script(_read_configuration_snippet(env_file))
            assert result.returncode == 0, result.stderr
            pentad = _extract_marker(result.stdout, _PENTAD_MARKER)
            decad = _extract_marker(result.stdout, _DECAD_MARKER)
            assert pentad.endswith(_BASE_URL)
            assert decad.endswith(_BASE_URL)
            prefixes[suffix] = (
                pentad[: -len(_BASE_URL)],
                decad[: -len(_BASE_URL)],
            )

        kghm_pentad_prefix, kghm_decad_prefix = prefixes["kghm"]
        assert kghm_pentad_prefix == "kyg.fc."
        assert kghm_decad_prefix == "demo.fc.decade."
        assert not kghm_decad_prefix.startswith(kghm_pentad_prefix), (
            "kghm pentad and decad prefixes must NOT share a stem - "
            f"pentad={kghm_pentad_prefix!r} decad={kghm_decad_prefix!r}"
        )

        for suffix in ("tjhm", "uzhm"):
            pentad_prefix, decad_prefix = prefixes[suffix]
            assert decad_prefix.startswith(pentad_prefix), (
                f"{suffix} pentad and decad prefixes must share a stem "
                f"(decad prefix must start with pentad prefix) - "
                f"pentad={pentad_prefix!r} decad={decad_prefix!r}"
            )


# ---------------------------------------------------------------------------
# Test 2: env file sets the origins -> value survives verbatim, no derived
# hostname appears
# ---------------------------------------------------------------------------


def test_env_file_value_wins_verbatim():
    custom_pentad = "10.0.0.1:5006"
    custom_decad = "10.0.0.1:5007"
    with tempfile.TemporaryDirectory(prefix="sapphire_test_") as tmpdir:
        env_file = _write_env_file(
            Path(tmpdir),
            "kghm",
            textwrap.dedent(f"""\
                ieasyhydroforecast_url={_BASE_URL}
                ieasyhydroforecast_url_pentad={custom_pentad}
                ieasyhydroforecast_url_decad={custom_decad}
            """),
        )
        result = _run_script(_read_configuration_snippet(env_file))
        assert result.returncode == 0, result.stderr
        pentad = _extract_marker(result.stdout, _PENTAD_MARKER)
        decad = _extract_marker(result.stdout, _DECAD_MARKER)
        assert pentad == custom_pentad
        assert decad == custom_decad
        # The derived public hostname must not appear anywhere.
        assert "kyg.fc." not in result.stdout
        assert "demo.fc.decade." not in result.stdout


# ---------------------------------------------------------------------------
# Test 3: empty value in env file -> falls back to derived
# ---------------------------------------------------------------------------


def test_empty_env_file_value_falls_back_to_derived():
    with tempfile.TemporaryDirectory(prefix="sapphire_test_") as tmpdir:
        env_file = _write_env_file(
            Path(tmpdir),
            "kghm",
            textwrap.dedent(f"""\
                ieasyhydroforecast_url={_BASE_URL}
                ieasyhydroforecast_url_pentad=
            """),
        )
        result = _run_script(_read_configuration_snippet(env_file))
        assert result.returncode == 0, result.stderr
        pentad = _extract_marker(result.stdout, _PENTAD_MARKER)
        decad = _extract_marker(result.stdout, _DECAD_MARKER)
        assert pentad == f"kyg.fc.{_BASE_URL}", (
            "an env-file line 'ieasyhydroforecast_url_pentad=' (empty) must "
            "fall back to the derived value, not produce an empty origin "
            f"(would crash Bokeh); got {pentad!r}"
        )
        assert decad == f"demo.fc.decade.{_BASE_URL}"


# ---------------------------------------------------------------------------
# Test 4: pentad and decad are independent
# ---------------------------------------------------------------------------


def test_setting_pentad_only_does_not_alter_decad():
    custom_pentad = "10.0.0.1:5006"
    with tempfile.TemporaryDirectory(prefix="sapphire_test_") as tmpdir:
        env_file = _write_env_file(
            Path(tmpdir),
            "kghm",
            textwrap.dedent(f"""\
                ieasyhydroforecast_url={_BASE_URL}
                ieasyhydroforecast_url_pentad={custom_pentad}
            """),
        )
        result = _run_script(_read_configuration_snippet(env_file))
        assert result.returncode == 0, result.stderr
        pentad = _extract_marker(result.stdout, _PENTAD_MARKER)
        decad = _extract_marker(result.stdout, _DECAD_MARKER)
        assert pentad == custom_pentad
        assert decad == f"demo.fc.decade.{_BASE_URL}"


def test_setting_decad_only_does_not_alter_pentad():
    custom_decad = "10.0.0.1:5007"
    with tempfile.TemporaryDirectory(prefix="sapphire_test_") as tmpdir:
        env_file = _write_env_file(
            Path(tmpdir),
            "kghm",
            textwrap.dedent(f"""\
                ieasyhydroforecast_url={_BASE_URL}
                ieasyhydroforecast_url_decad={custom_decad}
            """),
        )
        result = _run_script(_read_configuration_snippet(env_file))
        assert result.returncode == 0, result.stderr
        pentad = _extract_marker(result.stdout, _PENTAD_MARKER)
        decad = _extract_marker(result.stdout, _DECAD_MARKER)
        assert pentad == f"kyg.fc.{_BASE_URL}"
        assert decad == custom_decad


# ---------------------------------------------------------------------------
# Test 5: provenance - two read_configuration calls in ONE shell must not
# leak the first env file's origins into the second call
# ---------------------------------------------------------------------------


def test_second_call_in_same_shell_does_not_inherit_first_call_origins():
    """The key regression guard for the §1 `unset`.

    First call uses an env file that SETS explicit origins. Second call, in
    the SAME shell, uses an env file that does NOT set them. The second
    call's exported values must be the DERIVED values for its own org
    ending, not leftovers from the first file.
    """
    first_pentad = "10.0.0.1:5006"
    first_decad = "10.0.0.1:5007"
    with tempfile.TemporaryDirectory(prefix="sapphire_test_") as tmpdir:
        tmp = Path(tmpdir)
        first_env_file = _write_env_file(
            tmp,
            "kghm",
            textwrap.dedent(f"""\
                ieasyhydroforecast_url={_BASE_URL}
                ieasyhydroforecast_url_pentad={first_pentad}
                ieasyhydroforecast_url_decad={first_decad}
            """),
        )
        # Different org ending (tjhm) for the second file so the derived
        # values are unambiguously distinct from the first file's values.
        # Both files live in the same tmp dir under different filenames
        # (deployment_kghm, deployment_tjhm), which is fine.
        second_env_file = _write_env_file(
            tmp,
            "tjhm",
            f"ieasyhydroforecast_url={_BASE_URL}\n",
        )
        script = textwrap.dedent(f"""\
            source "{_COMMON_FUNCTIONS}"
            read_configuration "{first_env_file}" > /dev/null
            read_configuration "{second_env_file}"
            echo "{_PENTAD_MARKER}${{ieasyhydroforecast_url_pentad}}"
            echo "{_DECAD_MARKER}${{ieasyhydroforecast_url_decad}}"
        """)
        result = _run_script(script)
        assert result.returncode == 0, result.stderr
        pentad = _extract_marker(result.stdout, _PENTAD_MARKER)
        decad = _extract_marker(result.stdout, _DECAD_MARKER)
        assert pentad == f"taj.fc.{_BASE_URL}", (
            "second read_configuration call must yield the DERIVED value "
            f"for its own org ending, not the first file's value; got {pentad!r}"
        )
        assert decad == f"taj.fc.decade.{_BASE_URL}"
        assert pentad != first_pentad
        assert decad != first_decad


# ---------------------------------------------------------------------------
# Test 6: a parent-shell export does not leak in
# ---------------------------------------------------------------------------


def test_parent_shell_export_does_not_leak_in():
    leaked_pentad = "10.0.0.1:5006"
    with tempfile.TemporaryDirectory(prefix="sapphire_test_") as tmpdir:
        env_file = _write_env_file(Path(tmpdir), "kghm", f"ieasyhydroforecast_url={_BASE_URL}\n")
        script = textwrap.dedent(f"""\
            export ieasyhydroforecast_url_pentad={leaked_pentad}
            source "{_COMMON_FUNCTIONS}"
            read_configuration "{env_file}"
            echo "{_PENTAD_MARKER}${{ieasyhydroforecast_url_pentad}}"
            echo "{_DECAD_MARKER}${{ieasyhydroforecast_url_decad}}"
        """)
        result = _run_script(script)
        assert result.returncode == 0, result.stderr
        pentad = _extract_marker(result.stdout, _PENTAD_MARKER)
        decad = _extract_marker(result.stdout, _DECAD_MARKER)
        assert pentad == f"kyg.fc.{_BASE_URL}", (
            "a value exported in the parent shell before sourcing "
            "common_functions.sh must not leak into read_configuration's "
            f"result when the env file omits it; got {pentad!r}"
        )
        assert decad == f"demo.fc.decade.{_BASE_URL}"


# ---------------------------------------------------------------------------
# Test 7: a strict caller survives (set -euo pipefail + the established
# set +u / set -u wrapper pattern)
# ---------------------------------------------------------------------------


def test_strict_caller_survives_with_set_u_wrapper():
    """Mirrors the established pattern in
    bin/yearly_runoff_hydrograph_aggregation.sh: the caller runs under
    `set -euo pipefail`, sources common_functions.sh, then wraps the
    read_configuration call in `set +u` / `set -u`."""
    with tempfile.TemporaryDirectory(prefix="sapphire_test_") as tmpdir:
        env_file = _write_env_file(Path(tmpdir), "kghm", f"ieasyhydroforecast_url={_BASE_URL}\n")
        script = textwrap.dedent(f"""\
            set -euo pipefail
            source "{_COMMON_FUNCTIONS}"
            set +u
            read_configuration "{env_file}"
            set -u
            echo "{_PENTAD_MARKER}${{ieasyhydroforecast_url_pentad}}"
            echo "{_DECAD_MARKER}${{ieasyhydroforecast_url_decad}}"
        """)
        result = _run_script(script)
        combined = result.stdout + result.stderr
        assert "unbound variable" not in combined, (
            f"strict caller aborted with 'unbound variable'; stderr={result.stderr!r}"
        )
        assert result.returncode == 0, result.stderr
        pentad = _extract_marker(result.stdout, _PENTAD_MARKER)
        decad = _extract_marker(result.stdout, _DECAD_MARKER)
        assert pentad == f"kyg.fc.{_BASE_URL}"
        assert decad == f"demo.fc.decade.{_BASE_URL}"


if __name__ == "__main__":
    # Manual smoke run: `python test_websocket_origin_config.py`
    sys.exit(
        subprocess.call(
            ["pytest", "-v", __file__],
        )
    )
