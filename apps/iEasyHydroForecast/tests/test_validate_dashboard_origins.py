"""Subprocess-driven regression tests for validate_dashboard_origins()
(bin/utils/common_functions.sh), added under INFRA-032.

validate_dashboard_origins() is a SEPARATE function from read_configuration,
called only by the three dashboard launcher scripts (restart_sapphire_stack.sh,
daily_update_sapphire_frontend.sh, deploy_sapphire_forecast_tools.sh) - never
by read_configuration itself. It validates the form of
ieasyhydroforecast_url_pentad / ieasyhydroforecast_url_decad
(comma-separated HOST[:PORT] entries, no scheme, no wildcard) and exits 1 with
a message naming the offending variable and entry on failure.

These tests drive child bash processes with a minimal env that does NOT
inherit the test runner's environment, following the precedent in
test_read_configuration_set_u.py. Placeholders only (10.0.0.1, example.org,
host.example) - no real IP addresses, internal hostnames or credentials.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

import pytest

# Repo root: apps/iEasyHydroForecast/tests/test_*.py -> parents[3]
_REPO_ROOT = Path(__file__).resolve().parents[3]
_COMMON_FUNCTIONS = _REPO_ROOT / "bin" / "utils" / "common_functions.sh"

_VALID_DECAD = "host.example:5007"


def _minimal_env() -> dict[str, str]:
    return {"PATH": "/usr/bin:/bin:/usr/local/bin"}


def _run_script(script: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        env=_minimal_env(),
        check=False,
    )


def _bash_single_quote(value: str) -> str:
    """Wrap `value` in bash single quotes, escaping any embedded single
    quote. Deliberately NOT Python's repr(): repr() would escape an
    embedded literal newline to the two characters backslash-n, which is
    exactly wrong for the multiline-bypass regression tests below - a bash
    single-quoted string preserves an embedded literal newline as-is, which
    is what is needed to reproduce a real multiline env-file value."""
    return "'" + value.replace("'", "'\\''") + "'"


def _validate_snippet(pentad_value: str, decad_value: str = _VALID_DECAD) -> str:
    # FIX 6 (INFRA-032 review round 2): capture $? IMMEDIATELY after calling
    # validate_dashboard_origins, before anything else runs. The previous
    # form - `validate_dashboard_origins || exit 1` followed by a separate
    # `echo "VALIDATE_EXIT_MARKER=$?"` line - was tautological: by the time
    # the echo runs, either the `|| exit 1` already fired (process gone, echo
    # never reached) or the compound statement `A || B` succeeded, whose own
    # exit status is always 0 regardless of what code path A took. So
    # "VALIDATE_EXIT_MARKER=0" could never appear as anything but 0 on any
    # run that reached the echo at all - the assertion
    # `"VALIDATE_EXIT_MARKER=0" not in result.stdout` in the rejection tests
    # was passing only because the echo line was simply never reached on
    # failure, not because the marker captured a real non-zero status.
    # Capturing $? right after the call, then using THAT value to decide
    # whether to exit, makes the marker meaningful on both paths while
    # preserving the same overall process exit-code contract the `|| exit 1`
    # call sites rely on.
    return textwrap.dedent(f"""\
        source "{_COMMON_FUNCTIONS}"
        export ieasyhydroforecast_url_pentad={_bash_single_quote(pentad_value)}
        export ieasyhydroforecast_url_decad={_bash_single_quote(decad_value)}
        validate_dashboard_origins
        VALIDATE_EXIT_MARKER=$?
        echo "VALIDATE_EXIT_MARKER=$VALIDATE_EXIT_MARKER"
        exit "$VALIDATE_EXIT_MARKER"
    """)


_RESULT_PENTAD_MARKER = "RESULT_PENTAD="
_RESULT_DECAD_MARKER = "RESULT_DECAD="


def _validate_and_report_snippet(pentad_value: str, decad_value: str = _VALID_DECAD) -> str:
    """Like _validate_snippet, but also echoes the pentad/decad values as
    they stand AFTER validate_dashboard_origins returns - used to check the
    lowercasing behaviour (FIX 2). See _validate_snippet's docstring for why
    $? is captured immediately after the call rather than after `|| exit 1`
    (FIX 6)."""
    return textwrap.dedent(f"""\
        source "{_COMMON_FUNCTIONS}"
        export ieasyhydroforecast_url_pentad={_bash_single_quote(pentad_value)}
        export ieasyhydroforecast_url_decad={_bash_single_quote(decad_value)}
        validate_dashboard_origins
        VALIDATE_EXIT_MARKER=$?
        echo "VALIDATE_EXIT_MARKER=$VALIDATE_EXIT_MARKER"
        echo "{_RESULT_PENTAD_MARKER}$ieasyhydroforecast_url_pentad"
        echo "{_RESULT_DECAD_MARKER}$ieasyhydroforecast_url_decad"
        exit "$VALIDATE_EXIT_MARKER"
    """)


def _extract_marker(stdout: str, marker: str) -> str:
    for line in stdout.splitlines():
        if line.startswith(marker):
            return line[len(marker) :]
    raise AssertionError(f"marker {marker!r} not found in stdout:\n{stdout}")


# ---------------------------------------------------------------------------
# Test 8: accepts valid forms
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "host.example",
        "host.example:5006",
        "10.0.0.1:5006",
        "10.0.0.1:5006,host.example:5006",
        "host_name:5006",
        "host.example:1",
        "host.example:65535",
        "10.0.0.1",
    ],
    ids=[
        "bare-host",
        "host-with-port",
        "ip-with-port",
        "multi-entry-list",
        "underscore-hostname",
        "port-lower-boundary",
        "port-upper-boundary",
        # FIX B (regression): a bare IPv4 literal (no port) must stay
        # accepted - it is all digits AND dots, which distinguishes it from
        # the bare-port-typo case ("5006") rejected below.
        "bare-ipv4-no-port",
    ],
)
def test_accepts_valid_origin_forms(value: str):
    result = _run_script(_validate_snippet(value))
    assert result.returncode == 0, (
        f"expected acceptance of {value!r}; stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert "VALIDATE_EXIT_MARKER=0" in result.stdout


# ---------------------------------------------------------------------------
# Test 9: rejects malformed forms, each with a message naming the variable
# and the offending entry, and a non-zero exit
# ---------------------------------------------------------------------------


# (bad value, substring expected in the error message identifying the
# offending content - either the specific entry, or, for the two structural
# categories (whitespace-only value; leading/trailing/doubled comma), the
# category-appropriate substring per the implementation's own messages).
_REJECTED_CASES = [
    ("*", "*", "wildcard"),
    ("*:5006", "*:5006", "wildcard-with-port"),
    ("a, b", " b", "whitespace-after-comma"),
    (" ", "empty or whitespace-only", "whitespace-only-value"),
    ("a,,b", "a,,b", "doubled-comma"),
    ("a,", "a,", "trailing-comma"),
    (",a", ",a", "leading-comma"),
    ("https://a", "https://a", "scheme"),
    (":5006", ":5006", "bare-port-no-host"),
    ("a:notaport", "a:notaport", "non-numeric-port"),
    ("a:b:c", "a:b:c", "too-many-colons"),
    ("fe80::1:5006", "fe80::1:5006", "ipv6-literal"),
    ("host.example:0", "0", "port-zero"),
    ("host.example:65536", "65536", "port-too-large"),
    # FIX B (regression): bash cannot parse a 20+ digit integer, so
    # `[ "$port" -lt 1 ] || [ "$port" -gt 65535 ]` used to evaluate BOTH
    # comparisons false (each emitting "integer expression expected") and
    # silently ACCEPT the value - verified before the fix (RC=0). The
    # port's length/form must now be rejected before any arithmetic
    # comparison is attempted.
    ("host.example:12345678901234567890", "12345678901234567890", "port-20-digits"),
    (
        "host.example:123456789012345678901234567890",
        "123456789012345678901234567890",
        "port-30-digits",
    ),
    # BLOCKING FIX B (regression): a host component that is ALL DIGITS with
    # no dot is a bare PORT typed where a HOST[:PORT] pair was expected
    # (e.g. "ieasyhydroforecast_url_pentad=5006", a plausible operator typo
    # for "HOST:5006"). With no colon, the whole entry is the "host", and
    # the pre-fix regex accepted it as a bare hostname, producing the
    # Bokeh allow-list entry "5006:80" - which no browser Origin header can
    # ever match (verified against the pinned Bokeh). An IPv4 literal such
    # as "10.0.0.1:5006" or "10.0.0.1" (see the accepted-forms test) must
    # keep working - it is digits AND dots, not digits alone.
    ("5006", "5006", "bare-port-typo-no-host"),
]


@pytest.mark.parametrize(
    "bad_value,expected_substring,_case_id",
    _REJECTED_CASES,
    ids=[c[2] for c in _REJECTED_CASES],
)
def test_rejects_malformed_origin_forms(bad_value: str, expected_substring: str, _case_id: str):
    result = _run_script(_validate_snippet(bad_value))
    combined = result.stdout + result.stderr

    assert result.returncode != 0, (
        f"expected rejection of {bad_value!r}; stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert "VALIDATE_EXIT_MARKER=0" not in result.stdout, (
        f"validate_dashboard_origins must not return 0 for {bad_value!r}"
    )
    # Message must name the offending variable.
    assert "ieasyhydroforecast_url_pentad" in combined, (
        f"error message does not name the variable for {bad_value!r}; combined={combined!r}"
    )
    # Message must name the offending entry (or, for the two structural
    # comma/whitespace categories, the category-identifying substring).
    assert expected_substring in combined, (
        f"error message does not mention {expected_substring!r} for input "
        f"{bad_value!r}; combined={combined!r}"
    )


def test_rejects_when_decad_is_the_malformed_variable():
    """The rejection path also fires when the malformed value is on decad,
    not just pentad, and the message names ieasyhydroforecast_url_decad."""
    result = _run_script(_validate_snippet(pentad_value="host.example:5006", decad_value="*"))
    combined = result.stdout + result.stderr
    assert result.returncode != 0
    assert "ieasyhydroforecast_url_decad" in combined
    assert "*" in combined


# ---------------------------------------------------------------------------
# FIX 1 (security): multiline validator bypass is closed.
#
# `IFS=',' read -ra entries <<< "$value"` (the pre-fix implementation) only
# ever reads the FIRST LINE of a multiline value, so
#     ieasyhydroforecast_url_pentad=$'host.example:5006\n,*'
# used to pass validation on line 1 while smuggling a wildcard through on
# line 2, all the way to Bokeh's --allow-websocket-origin argv (verified by
# executing the exact resulting argv before this fix). Any value containing
# an embedded newline or carriage return must now be rejected outright,
# before splitting.
# ---------------------------------------------------------------------------


def test_rejects_multiline_value_with_wildcard_smuggled_on_second_line():
    """The exact reproduction of the reported bypass: a syntactically valid
    first line, with the actual attack payload on a second line reached
    only by a line-based read. Must be rejected as a whole, not accepted
    because line 1 looks fine."""
    bypass_value = "host.example:5006\n,*"
    result = _run_script(_validate_snippet(pentad_value=bypass_value))
    combined = result.stdout + result.stderr
    assert result.returncode != 0, (
        f"multiline value with a smuggled wildcard on line 2 must be rejected; "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert "VALIDATE_EXIT_MARKER=0" not in result.stdout
    assert "ieasyhydroforecast_url_pentad" in combined
    assert "newline" in combined or "carriage return" in combined


def test_rejects_multiline_value_even_when_second_line_is_itself_valid():
    """A multiline value must be rejected as a whole even when every
    individual line, read in isolation, would itself be a valid entry -
    proving the rejection is unconditional on the presence of a newline,
    not just a heuristic that happens to catch the wildcard case above."""
    multiline_value = "host.example:5006\nhost2.example:5007"
    result = _run_script(_validate_snippet(pentad_value=multiline_value))
    combined = result.stdout + result.stderr
    assert result.returncode != 0, (
        f"a multiline value must be rejected even when line 2 is otherwise "
        f"valid on its own; stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert "VALIDATE_EXIT_MARKER=0" not in result.stdout
    assert "ieasyhydroforecast_url_pentad" in combined
    assert "newline" in combined or "carriage return" in combined


def test_rejects_carriage_return_embedded_value():
    """The same guard must catch a bare carriage return, not just '\\n'."""
    cr_value = "host.example:5006\r,*"
    result = _run_script(_validate_snippet(pentad_value=cr_value))
    assert result.returncode != 0
    assert "VALIDATE_EXIT_MARKER=0" not in result.stdout


# ---------------------------------------------------------------------------
# FIX 2 (owner decision): both variables are lowercased after validation
# succeeds, and re-exported - Bokeh lowercases the browser's Origin header
# but compares it verbatim against the allow-list entries, so an uppercase
# entry silently matches nothing.
# ---------------------------------------------------------------------------


def test_uppercase_input_is_accepted_and_exported_value_is_lowercase():
    result = _run_script(
        _validate_and_report_snippet(
            pentad_value="HOST.EXAMPLE:5006", decad_value="HOST.EXAMPLE:5007"
        )
    )
    assert result.returncode == 0, (
        f"uppercase input must still be accepted; stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    pentad = _extract_marker(result.stdout, _RESULT_PENTAD_MARKER)
    decad = _extract_marker(result.stdout, _RESULT_DECAD_MARKER)
    assert pentad == "host.example:5006", pentad
    assert decad == "host.example:5007", decad


def test_mixed_case_input_is_accepted_and_exported_value_is_lowercase():
    result = _run_script(
        _validate_and_report_snippet(
            pentad_value="Host.Example:5006,OTHER.Example:5006",
            decad_value="Host.Example:5007",
        )
    )
    assert result.returncode == 0, (
        f"mixed-case input must still be accepted; stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    pentad = _extract_marker(result.stdout, _RESULT_PENTAD_MARKER)
    decad = _extract_marker(result.stdout, _RESULT_DECAD_MARKER)
    assert pentad == "host.example:5006,other.example:5006", pentad
    assert decad == "host.example:5007", decad


# ---------------------------------------------------------------------------
# FIX C (regression): a leading-zero port is normalised away, in the same
# pass that lowercases. Bokeh compares the port as a STRING against the
# browser's Origin header, so "host.example:05006" would otherwise never
# match a real Origin of "host.example:5006" - verified against the pinned
# Bokeh. A port of "0" must stay rejected by the range check (FIX B path),
# not fall through this stripping into an empty string.
# ---------------------------------------------------------------------------


def test_leading_zero_port_is_accepted_and_normalised():
    result = _run_script(
        _validate_and_report_snippet(pentad_value="host.example:05006", decad_value=_VALID_DECAD)
    )
    assert result.returncode == 0, (
        f"a leading-zero port must still be accepted; stdout={result.stdout!r} "
        f"stderr={result.stderr!r}"
    )
    pentad = _extract_marker(result.stdout, _RESULT_PENTAD_MARKER)
    assert pentad == "host.example:5006", pentad


def test_multiple_leading_zeros_in_port_are_all_stripped():
    # "00501" is 5 raw digits - within FIX B's `^[0-9]{1,5}$` shape check -
    # with two leading zeros, so this also confirms stripping is not
    # limited to a single leading zero.
    result = _run_script(
        _validate_and_report_snippet(pentad_value="host.example:00501", decad_value=_VALID_DECAD)
    )
    assert result.returncode == 0, (
        f"a port with several leading zeros must still be accepted; "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    pentad = _extract_marker(result.stdout, _RESULT_PENTAD_MARKER)
    assert pentad == "host.example:501", pentad


def test_all_zero_port_still_rejected_not_emptied():
    """A port of "0" (or "00") is out of range (1-65535) and must still be
    rejected by the FIX B range check - the leading-zero stripping added for
    FIX C must not run first and turn it into an empty-port entry that
    slips past validation."""
    result = _run_script(_validate_snippet(pentad_value="host.example:00"))
    assert result.returncode != 0, (
        f"an all-zero port must be rejected, not silently emptied; "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert "VALIDATE_EXIT_MARKER=0" not in result.stdout


# ---------------------------------------------------------------------------
# Test 10: read_configuration ALONE does not abort on an invalid value
# ---------------------------------------------------------------------------


def test_read_configuration_alone_does_not_abort_on_invalid_value():
    """validate_dashboard_origins is confined to the dashboard launchers and
    is never called from read_configuration - so a malformed value in an env
    file must not take down a backfill script or an operator's shell that
    only calls read_configuration."""
    with tempfile.TemporaryDirectory(prefix="sapphire_test_") as tmpdir:
        env_file = Path(tmpdir) / "deployment_kghm"
        env_file.write_text(
            textwrap.dedent("""\
                ieasyhydroforecast_url=example.org
                ieasyhydroforecast_url_pentad=*
            """)
        )
        script = textwrap.dedent(f"""\
            source "{_COMMON_FUNCTIONS}"
            read_configuration "{env_file}"
            echo "RC_EXIT_MARKER=$?"
        """)
        result = _run_script(script)
        assert result.returncode == 0, (
            f"read_configuration alone must not abort on an invalid origin "
            f"value; stdout={result.stdout!r} stderr={result.stderr!r}"
        )
        assert "RC_EXIT_MARKER=0" in result.stdout


# ---------------------------------------------------------------------------
# FIX A (regression): lowercasing must not fail open when `tr` is
# unavailable. `printf '%s' "$value" | tr ...` yields EMPTY if `tr` cannot
# be exec'd (cron strips PATH; a minimal/coreutils-less environment) -
# verified before the fix: both origin values were silently emptied and the
# function still returned 0, only crashing later at Bokeh with
# "ValueError: Empty host value".
#
# This is an ENVIRONMENT test, not an input test - no value fuzzing can
# reach this fault, only a PATH lacking `tr` can, so it is written as its
# own dedicated environment setup rather than a case in _REJECTED_CASES.
# ---------------------------------------------------------------------------

# The minimal set of tools common_functions.sh / bash itself needs, other
# than `tr` - deliberately excluding `tr` from this list.
_TR_INDEPENDENT_TOOLS = [
    "bash",
    "sh",
    "cat",
    "printf",
    "sed",
    "awk",
    "grep",
    "dirname",
    "basename",
    "date",
    "expr",
    "readlink",
]


def _build_path_without_tr() -> str:
    """Build (and return the path to) a directory containing symlinks to
    bash and the other plain tools needed to source common_functions.sh and
    call validate_dashboard_origins, deliberately WITHOUT `tr` - so a PATH
    built from only this directory reproduces an environment where `tr` is
    missing, regardless of what is installed on the host running the test
    suite."""
    bin_dir = Path(tempfile.mkdtemp(prefix="sapphire_test_no_tr_"))
    found_bash = False
    for tool in _TR_INDEPENDENT_TOOLS:
        src = shutil.which(tool)
        if src is None:
            continue
        dst = bin_dir / tool
        if not dst.exists():
            dst.symlink_to(src)
        if tool == "bash":
            found_bash = True
    assert found_bash, "bash could not be located on the test runner's PATH"
    assert shutil.which("tr", path=str(bin_dir)) is None, (
        "test setup bug: 'tr' must not be resolvable on the constructed no-tr PATH"
    )
    return str(bin_dir)


def test_lowercasing_failure_when_tr_unavailable_returns_nonzero_and_does_not_blank_values():
    bin_dir = _build_path_without_tr()
    pentad_input = "host.example:5006"
    script = textwrap.dedent(f"""\
        source "{_COMMON_FUNCTIONS}"
        export ieasyhydroforecast_url_pentad={_bash_single_quote(pentad_input)}
        export ieasyhydroforecast_url_decad={_bash_single_quote(_VALID_DECAD)}
        validate_dashboard_origins
        echo "VALIDATE_EXIT_MARKER=$?"
        echo "{_RESULT_PENTAD_MARKER}$ieasyhydroforecast_url_pentad"
        echo "{_RESULT_DECAD_MARKER}$ieasyhydroforecast_url_decad"
    """)
    result = subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        env={"PATH": bin_dir},
        check=False,
    )
    combined = result.stdout + result.stderr
    assert "VALIDATE_EXIT_MARKER=0" not in result.stdout, (
        f"validate_dashboard_origins must not return 0 when 'tr' is unavailable "
        f"(it must fail loudly, not silently export an empty origin); "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    pentad = _extract_marker(result.stdout, _RESULT_PENTAD_MARKER)
    decad = _extract_marker(result.stdout, _RESULT_DECAD_MARKER)
    assert pentad == pentad_input, (
        f"pentad value must be left UNCHANGED (not silently blanked) when the "
        f"lowercase transform fails; got {pentad!r}; combined={combined!r}"
    )
    assert decad == _VALID_DECAD, (
        f"decad value must be left UNCHANGED (not silently blanked) when the "
        f"lowercase transform fails; got {decad!r}; combined={combined!r}"
    )
    # The error message should name the likely cause, per the fix's spec.
    assert "tr" in combined, f"error message should mention 'tr'; combined={combined!r}"


# ---------------------------------------------------------------------------
# BLOCKING FIX A (3rd review round): the normalisation block used to check
# only whether `tr`'s output was EMPTY, never whether `tr` itself reported
# failure, and never re-validated what the transform actually produced. Two
# distinct fault classes below, both reproduced with a `tr` STUB placed
# first on PATH (not "no tr at all", unlike the FIX A test above):
#
#   1. `tr` exits non-zero but still echoes its (unmodified) input - the
#      pre-fix code only checked for an EMPTY result, so a non-empty but
#      un-transformed value sailed through with the function still
#      returning 0.
#   2. `tr` exits 0 (success) but emits a DIFFERENT, structurally invalid
#      value - no exit-status check could ever catch this; only re-running
#      the structural validator against the post-transform value can. This
#      is the "ASSERT THE POSTCONDITION" defence and must be shown to fire
#      on its own, independent of fault class 1.
# ---------------------------------------------------------------------------


def _build_path_with_tr_stub(tr_script_body: str) -> str:
    """Like _build_path_without_tr, but PATH resolves `tr` to a custom stub
    script (given as a shell script body, shebang included) instead of
    omitting it - reproduces "tr is present but misbehaves", as opposed to
    "tr is missing"."""
    bin_dir = Path(tempfile.mkdtemp(prefix="sapphire_test_tr_stub_"))
    found_bash = False
    for tool in _TR_INDEPENDENT_TOOLS:
        src = shutil.which(tool)
        if src is None:
            continue
        dst = bin_dir / tool
        if not dst.exists():
            dst.symlink_to(src)
        if tool == "bash":
            found_bash = True
    assert found_bash, "bash could not be located on the test runner's PATH"
    tr_path = bin_dir / "tr"
    tr_path.write_text(tr_script_body)
    tr_path.chmod(0o755)
    return str(bin_dir)


def test_tr_exits_nonzero_while_echoing_input_causes_nonzero_return():
    """A `tr` that FAILS (non-zero exit) but still emits its input
    unmodified must not be treated as success just because the output is
    non-empty - the pre-fix code checked emptiness only, never the exit
    status, so this exact stub used to return 0 with the un-lowercased
    value silently exported."""
    stub = "#!/bin/sh\ncat\nexit 9\n"
    bin_dir = _build_path_with_tr_stub(stub)
    pentad_input = "HOST.EXAMPLE:5006"
    script = textwrap.dedent(f"""\
        source "{_COMMON_FUNCTIONS}"
        export ieasyhydroforecast_url_pentad={_bash_single_quote(pentad_input)}
        export ieasyhydroforecast_url_decad={_bash_single_quote(_VALID_DECAD)}
        validate_dashboard_origins
        echo "VALIDATE_EXIT_MARKER=$?"
        echo "{_RESULT_PENTAD_MARKER}$ieasyhydroforecast_url_pentad"
    """)
    result = subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        env={"PATH": bin_dir},
        check=False,
    )
    combined = result.stdout + result.stderr
    assert "VALIDATE_EXIT_MARKER=0" not in result.stdout, (
        f"a failing 'tr' that echoes its input unchanged must not be treated as "
        f"success; stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert "tr" in combined, f"error message should mention 'tr'; combined={combined!r}"


def test_tr_succeeds_but_emits_invalid_value_causes_nonzero_return_via_postcheck():
    """A `tr` that exits 0 but emits a DIFFERENT, structurally invalid value
    (here, a value that normalises down to a doubled/empty-entry comma list)
    must be caught by the post-transform re-validation - no exit-status
    check can catch this, since `tr` itself reports success."""
    stub = "#!/bin/sh\ncat >/dev/null\nprintf '%s' ',,,'\nexit 0\n"
    bin_dir = _build_path_with_tr_stub(stub)
    pentad_input = "host.example:5006"
    script = textwrap.dedent(f"""\
        source "{_COMMON_FUNCTIONS}"
        export ieasyhydroforecast_url_pentad={_bash_single_quote(pentad_input)}
        export ieasyhydroforecast_url_decad={_bash_single_quote(_VALID_DECAD)}
        validate_dashboard_origins
        echo "VALIDATE_EXIT_MARKER=$?"
    """)
    result = subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        env={"PATH": bin_dir},
        check=False,
    )
    combined = result.stdout + result.stderr
    assert "VALIDATE_EXIT_MARKER=0" not in result.stdout, (
        f"a 'tr' that succeeds but emits an invalid value must be caught by "
        f"post-transform re-validation, not treated as success; "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert "normalisation" in combined or "invalid" in combined, (
        f"error message should indicate the post-transform value was invalid; combined={combined!r}"
    )


def test_tr_stub_emitting_oversized_port_causes_nonzero_return_via_postcheck():
    """Same postcondition-recheck path as above, but the corrupted transform
    output is a well-formed-looking comma list with an oversized (20-digit)
    port instead of a comma-structure fault - confirms the re-validation
    covers the per-entry/port checks, not just the comma-structure ones."""
    stub = "#!/bin/sh\ncat >/dev/null\nprintf '%s' 'host.example:12345678901234567890'\nexit 0\n"
    bin_dir = _build_path_with_tr_stub(stub)
    pentad_input = "host.example:5006"
    script = textwrap.dedent(f"""\
        source "{_COMMON_FUNCTIONS}"
        export ieasyhydroforecast_url_pentad={_bash_single_quote(pentad_input)}
        export ieasyhydroforecast_url_decad={_bash_single_quote(_VALID_DECAD)}
        validate_dashboard_origins
        echo "VALIDATE_EXIT_MARKER=$?"
    """)
    result = subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        env={"PATH": bin_dir},
        check=False,
    )
    assert "VALIDATE_EXIT_MARKER=0" not in result.stdout, (
        f"a 'tr' stub emitting an oversized-port value must be rejected by "
        f"post-transform re-validation; stdout={result.stdout!r} stderr={result.stderr!r}"
    )


# ---------------------------------------------------------------------------
# FIX 2 (review round 2): the postcondition must assert CANONICAL form (no
# uppercase remaining), not merely well-formedness. A `tr` that exits 0
# without actually lowercasing (a true no-op/identity stub - distinct from
# the FIX-A-round "tr exits non-zero" stub above) is reproduced here with a
# fresh stub. Two tests: one where PENTAD is the corrupted variable and one
# where DECAD is - deliberately separate, because both existing corrupt-`tr`
# tests above happen to corrupt pentad, so a postcondition guard that was
# accidentally only wired up for the pentad iteration of the loop would
# stay green on every test in this file except this DECAD-specific one.
# ---------------------------------------------------------------------------


def _noop_tr_stub_path() -> str:
    """A `tr` stub that exits 0 and echoes its input completely unchanged -
    the identity function. Distinct from the FIX-A-round stub
    ("cat; exit 9"), which fails the exit-status check; this one passes
    every check except the canonical-form (FIX 2) postcheck."""
    stub = "#!/bin/sh\ncat\nexit 0\n"
    return _build_path_with_tr_stub(stub)


def test_tr_noop_stub_on_pentad_causes_nonzero_return_via_canonical_postcheck():
    bin_dir = _noop_tr_stub_path()
    pentad_input = "HOST.EXAMPLE:5006"
    script = textwrap.dedent(f"""\
        source "{_COMMON_FUNCTIONS}"
        export ieasyhydroforecast_url_pentad={_bash_single_quote(pentad_input)}
        export ieasyhydroforecast_url_decad={_bash_single_quote(_VALID_DECAD)}
        validate_dashboard_origins
        echo "VALIDATE_EXIT_MARKER=$?"
    """)
    result = subprocess.run(
        ["bash", "-c", script], capture_output=True, text=True, env={"PATH": bin_dir}, check=False
    )
    combined = result.stdout + result.stderr
    assert "VALIDATE_EXIT_MARKER=0" not in result.stdout, (
        f"a no-op 'tr' (exits 0, does not lowercase) must be caught by the "
        f"canonical-form postcheck on PENTAD; stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert "ieasyhydroforecast_url_pentad" in combined
    assert "uppercase" in combined


def test_tr_noop_stub_on_decad_causes_nonzero_return_via_canonical_postcheck():
    """Same as above, but PENTAD is already lowercase (so it needs no real
    transformation and would pass even under a broken tr) while DECAD is
    the one the no-op tr fails to lowercase - isolates the DECAD iteration
    of the loop."""
    bin_dir = _noop_tr_stub_path()
    decad_input = "HOST.EXAMPLE:5007"
    script = textwrap.dedent(f"""\
        source "{_COMMON_FUNCTIONS}"
        export ieasyhydroforecast_url_pentad={_bash_single_quote("host.example:5006")}
        export ieasyhydroforecast_url_decad={_bash_single_quote(decad_input)}
        validate_dashboard_origins
        echo "VALIDATE_EXIT_MARKER=$?"
    """)
    result = subprocess.run(
        ["bash", "-c", script], capture_output=True, text=True, env={"PATH": bin_dir}, check=False
    )
    combined = result.stdout + result.stderr
    assert "VALIDATE_EXIT_MARKER=0" not in result.stdout, (
        f"a no-op 'tr' (exits 0, does not lowercase) must be caught by the "
        f"canonical-form postcheck on DECAD specifically, even though PENTAD "
        f"needed no change; stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert "ieasyhydroforecast_url_decad" in combined
    assert "uppercase" in combined


# ---------------------------------------------------------------------------
# FIX 4 (review round 2): reject a non-scalar (array/nameref) variable
# before ${!var_name} is ever read - reachable because env files are
# sourced as shell code.
# ---------------------------------------------------------------------------


def test_array_valued_pentad_var_is_rejected():
    script = textwrap.dedent(f"""\
        source "{_COMMON_FUNCTIONS}"
        declare -a ieasyhydroforecast_url_pentad=("host.example:5006" "other.example:5006")
        export ieasyhydroforecast_url_decad={_bash_single_quote(_VALID_DECAD)}
        validate_dashboard_origins
        VALIDATE_EXIT_MARKER=$?
        echo "VALIDATE_EXIT_MARKER=$VALIDATE_EXIT_MARKER"
        exit "$VALIDATE_EXIT_MARKER"
    """)
    result = _run_script(script)
    combined = result.stdout + result.stderr
    assert result.returncode != 0, (
        f"an array-valued ieasyhydroforecast_url_pentad must be rejected, not silently "
        f"validated/exported via element zero; stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert "VALIDATE_EXIT_MARKER=0" not in result.stdout
    assert "ieasyhydroforecast_url_pentad" in combined
    assert "array" in combined or "nameref" in combined or "scalar" in combined


# ---------------------------------------------------------------------------
# FIX 5 (review round 2): reject a host that is empty or ends in a bare
# trailing dot - reachable when ieasyhydroforecast_url is unset and the
# kghm derivation ("kyg.fc.$ieasyhydroforecast_url") concatenates a prefix
# with nothing after it.
# ---------------------------------------------------------------------------


def test_derived_origin_with_trailing_dot_and_no_host_is_rejected():
    result = _run_script(_validate_snippet(pentad_value="kyg.fc."))
    combined = result.stdout + result.stderr
    assert result.returncode != 0, (
        f"a host ending in a bare trailing dot with nothing after it must be rejected; "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert "VALIDATE_EXIT_MARKER=0" not in result.stdout
    assert "trailing dot" in combined


def test_legal_fqdn_trailing_dot_is_also_rejected_not_just_bare_prefix():
    """DECISION (documented next to the check in common_functions.sh): a
    single trailing dot on a real FQDN ("host.example.") is legal, absolute
    DNS syntax - but it is rejected here too, deliberately, because the
    property that matters for this allow-list is not DNS validity, it is
    whether the entry can ever equal a real browser Origin header, and
    browsers never send a root-zone trailing dot in Origin. This test
    proves the rejection is that deliberate policy, not a heuristic that
    happens to catch only the obviously-truncated "kyg.fc." case above."""
    result = _run_script(_validate_snippet(pentad_value="host.example."))
    combined = result.stdout + result.stderr
    assert result.returncode != 0, (
        f"a legal-DNS trailing-dot FQDN must still be rejected - it can never match a "
        f"real browser Origin header; stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert "VALIDATE_EXIT_MARKER=0" not in result.stdout
    assert "trailing dot" in combined


# ---------------------------------------------------------------------------
# FIX 7 (review round 2): regression tests for the post-assignment CONTENT
# check - per the mutation matrix, the ONLY guard that catches a read-only
# assignment target on bash 3.2 (where `printf -v` returns exit status 0
# even though the assignment was refused) and a `declare -u` target on
# bash 4.3+ (where `printf -v` "succeeds" but the shell's own attribute
# forces the value straight back to uppercase). Both cases need the
# assigned value to actually CHANGE under normalisation (i.e. start
# uppercase) - if the value were already canonical, a blocked/overridden
# write would coincidentally still match, and the guard would not be
# exercised.
#
# Run against every distinct bash interpreter this repo must support, since
# the read-only case's exit-status behaviour is bash-version-dependent
# (verified: exit 0 on macOS system bash 3.2, exit 1 on GNU bash 5.2) and
# `declare -u` does not exist before bash 4.3.
# ---------------------------------------------------------------------------


def _find_bash_executables() -> dict[str, str]:
    """Locate every distinct bash interpreter available on this machine,
    deduplicated by resolved path. Used to confirm bash-version-dependent
    behaviour (the printf -v-on-readonly exit status; declare -u) the same
    way on every bash this repo must run under, not just whichever `bash`
    happens to be first on the test runner's PATH."""
    candidate_paths = [
        shutil.which("bash"),
        "/bin/bash",
        "/opt/local/bin/bash",
        "/opt/homebrew/bin/bash",
        "/usr/local/bin/bash",
        "/usr/bin/bash",
    ]
    found: dict[str, str] = {}
    seen_realpaths: set[str] = set()
    for path in candidate_paths:
        if not path or not Path(path).exists():
            continue
        try:
            real = str(Path(path).resolve())
        except OSError:
            continue
        if real in seen_realpaths:
            continue
        seen_realpaths.add(real)
        found[real] = path
    return found


def _bash_major_version(bash_path: str) -> int:
    result = subprocess.run(
        [bash_path, "-c", 'printf "%s" "${BASH_VERSINFO[0]}"'],
        capture_output=True,
        text=True,
        check=False,
    )
    return int(result.stdout.strip() or "0")


_BASH_EXECUTABLES = _find_bash_executables()


@pytest.mark.parametrize(
    "bash_path", list(_BASH_EXECUTABLES.values()), ids=list(_BASH_EXECUTABLES.keys())
)
def test_readonly_pentad_var_causes_nonzero_return_via_content_check(bash_path: str):
    """FIX 7: declares ieasyhydroforecast_url_pentad readonly AFTER giving
    it an uppercase value that normalisation must change, then calls
    validate_dashboard_origins. The `printf -v` assignment is refused; on
    macOS bash 3.2 that refusal is reported via exit status 0 (verified),
    so only the post-assignment CONTENT check
    (`"${!var_name}" != "$normalized"`) can catch it."""
    pentad_input = "HOST.EXAMPLE:5006"
    script = textwrap.dedent(f"""\
        source "{_COMMON_FUNCTIONS}"
        export ieasyhydroforecast_url_pentad={_bash_single_quote(pentad_input)}
        export ieasyhydroforecast_url_decad={_bash_single_quote(_VALID_DECAD)}
        readonly ieasyhydroforecast_url_pentad
        validate_dashboard_origins
        echo "VALIDATE_EXIT_MARKER=$?"
    """)
    result = subprocess.run(
        [bash_path, "-c", script], capture_output=True, text=True, env=_minimal_env(), check=False
    )
    combined = result.stdout + result.stderr
    assert "VALIDATE_EXIT_MARKER=0" not in result.stdout, (
        f"a read-only pentad variable that needs lowercasing must be rejected on "
        f"{bash_path}; stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert "ieasyhydroforecast_url_pentad" in combined


@pytest.mark.parametrize(
    "bash_path", list(_BASH_EXECUTABLES.values()), ids=list(_BASH_EXECUTABLES.keys())
)
def test_declare_u_pentad_var_causes_nonzero_return_via_content_check(bash_path: str):
    """FIX 7: `declare -u` (bash 4.3+) forces the variable back to
    UPPERCASE on every assignment, including `printf -v`'s own write of the
    freshly-lowercased value - so `printf -v` reports success (exit 0) but
    the variable's actual content is wrong. Only the post-assignment
    CONTENT check can catch this; the exit-status check cannot, since
    printf -v genuinely did succeed by bash's own accounting."""
    if _bash_major_version(bash_path) < 4:
        pytest.skip(f"{bash_path} is bash < 4 - 'declare -u' was added in bash 4.3")
    script = textwrap.dedent(f"""\
        source "{_COMMON_FUNCTIONS}"
        declare -u ieasyhydroforecast_url_pentad="host.example:5006"
        export ieasyhydroforecast_url_decad={_bash_single_quote(_VALID_DECAD)}
        export ieasyhydroforecast_url_pentad
        validate_dashboard_origins
        echo "VALIDATE_EXIT_MARKER=$?"
    """)
    result = subprocess.run(
        [bash_path, "-c", script], capture_output=True, text=True, env=_minimal_env(), check=False
    )
    combined = result.stdout + result.stderr
    assert "VALIDATE_EXIT_MARKER=0" not in result.stdout, (
        f"a 'declare -u' pentad variable must be rejected on {bash_path} - printf -v "
        f"reports success but the shell's own uppercase attribute overwrites the "
        f"freshly-lowercased value; stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert "ieasyhydroforecast_url_pentad" in combined


if __name__ == "__main__":
    # Manual smoke run: `python test_validate_dashboard_origins.py`
    sys.exit(
        subprocess.call(
            ["pytest", "-v", __file__],
        )
    )
