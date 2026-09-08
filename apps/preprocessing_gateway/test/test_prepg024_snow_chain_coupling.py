"""
Tests for PREPG-024: a `Quantile_Mapping_OP.py` failure must no longer
suppress `snow_data_operational.py`.

See doc/plans/issues/archive/mid_prio_gi_draft_prepg_qm_failure_suppresses_snow.md.

Before this fix, `apps/preprocessing_gateway/Dockerfile`'s CMD chained all
three scripts with `&&`:

    uv run Quantile_Mapping_OP.py && uv run extend_era5_reanalysis.py \
        && uv run snow_data_operational.py

Snow has no data dependency on the meteo branch (quantile mapping + ERA5
extension) -- it reads a different Data Gateway endpoint and writes a
different API sink -- so a QM failure silently withheld five days of snow
data on the kghm server even though snow itself was healthy. The fix keeps
`QM && extend` chained (extend genuinely depends on QM's control-member
CSVs) but runs snow unconditionally and sequentially afterwards (never
concurrently -- QM deletes every file in the shared OUTPUT_PATH_DG that
snow downloads into), and reports non-zero if either branch failed.

These tests do NOT re-run the real Quantile_Mapping_OP.py /
extend_era5_reanalysis.py / snow_data_operational.py scripts (that is
already covered by test_integration_preprocessing_gateway.py and
test_snow_data_operational_exit_status.py). Instead they extract the
literal CMD string from the Dockerfile and execute it for real via `sh -c`,
with `uv` replaced by a fake executable on PATH that logs which script it
was asked to run and exits with a scripted per-script code. This exercises
the actual shell orchestration logic shipped to production -- a test that
only asserted on the CMD string's *text* could not tell a working
implementation from a subtly broken one (e.g. wrong `&&`/`;` placement).

apps/pipeline/tests/test_prepg024_snow_chain_coupling.py covers the same
four scenarios against apps/run_locally.sh's `run_preprocessing_gateway`
function via that module's existing synthetic-venv test harness -- the two
files together satisfy "cover both implementations" without either one
alone being able to hide a defect in the other site.

Run::

    cd apps
    SAPPHIRE_TEST_ENV=True pytest preprocessing_gateway/test/test_prepg024_snow_chain_coupling.py -v
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

GATEWAY_DIR = Path(__file__).resolve().parents[1]
DOCKERFILE = GATEWAY_DIR / "Dockerfile"

# The fake `uv` stub: `uv run <script>` looks up an exit code from an env
# var keyed to the script name and appends one line to CALL_LOG before
# exiting with it. `run` is swallowed as $1 (ignored) so the stub matches
# real `uv run <script>.py` invocations.
_FAKE_UV_TEMPLATE = """#!/bin/sh
# $1 = "run", $2 = script name
script="$2"
case "$script" in
    Quantile_Mapping_OP.py) rc="${QM_RC:-0}" ;;
    extend_era5_reanalysis.py) rc="${EXTEND_RC:-0}" ;;
    snow_data_operational.py) rc="${SNOW_RC:-0}" ;;
    *) rc=0 ;;
esac
echo "CALL $script" >> "@CALL_LOG@"
exit "$rc"
"""


def _extract_cmd_string() -> str:
    """Pull the literal shell command out of the Dockerfile's CMD line.

    The Dockerfile's CMD is JSON-array `sh -c` syntax on a single line:
        CMD ["sh", "-c", "<command>"]
    Parsed with `json.loads` on the bracketed portion so any escaping in
    the command string (e.g. the embedded `\\"..."` around the FAILED
    stage(s) message) is decoded exactly as Docker itself would decode it,
    rather than approximated with a regex.
    """
    import json

    text = DOCKERFILE.read_text()
    match = re.search(r"^CMD\s+(\[.*\])\s*$", text, flags=re.MULTILINE)
    assert match, f"No single-line CMD [...] array found in {DOCKERFILE}"
    argv = json.loads(match.group(1))
    assert argv[:2] == ["sh", "-c"], f'Expected CMD to be ["sh", "-c", ...], got {argv[:2]}'
    assert len(argv) == 3, f"Expected exactly 3 CMD elements, got {len(argv)}: {argv}"
    return argv[2]


@pytest.fixture
def fake_uv(tmp_path: Path):
    """Put a fake `uv` executable at the front of PATH, plus a call log."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    call_log = tmp_path / "calls.log"
    call_log.write_text("")

    uv_path = bin_dir / "uv"
    uv_path.write_text(_FAKE_UV_TEMPLATE.replace("@CALL_LOG@", str(call_log)))
    uv_path.chmod(0o755)

    return bin_dir, call_log


def _run_cmd(
    fake_uv,
    *,
    qm_rc: int = 0,
    extend_rc: int = 0,
    snow_rc: int = 0,
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    """Execute the Dockerfile's real CMD string under a fake `uv`.

    Returns (completed process, ordered list of scripts `uv run` was
    actually invoked with).
    """
    import os

    bin_dir, call_log = fake_uv
    cmd = _extract_cmd_string()

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env.get('PATH', '')}"
    env["QM_RC"] = str(qm_rc)
    env["EXTEND_RC"] = str(extend_rc)
    env["SNOW_RC"] = str(snow_rc)

    result = subprocess.run(
        ["sh", "-c", cmd],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    calls = [
        line.split(" ", 1)[1]
        for line in call_log.read_text().splitlines()
        if line.startswith("CALL ")
    ]
    return result, calls


class TestDockerfileCmdSnowChainDecoupling:
    """The Dockerfile CMD run through a real `sh -c`, `uv` stubbed out."""

    def test_all_succeed_exits_zero(self, fake_uv):
        result, calls = _run_cmd(fake_uv)
        assert result.returncode == 0, result.stderr
        assert calls == [
            "Quantile_Mapping_OP.py",
            "extend_era5_reanalysis.py",
            "snow_data_operational.py",
        ]

    def test_qm_failure_still_runs_snow_and_exits_nonzero(self, fake_uv):
        """A QM failure must not prevent snow from running, and the
        overall status must stay non-zero (QM's failure not masked by
        snow's success)."""
        result, calls = _run_cmd(fake_uv, qm_rc=1)
        assert result.returncode != 0
        # extend must NOT run -- it depends on files only a successful QM
        # writes.
        assert calls == ["Quantile_Mapping_OP.py", "snow_data_operational.py"]

    def test_extend_failure_still_runs_snow_and_exits_nonzero(self, fake_uv):
        result, calls = _run_cmd(fake_uv, extend_rc=1)
        assert result.returncode != 0
        assert calls == [
            "Quantile_Mapping_OP.py",
            "extend_era5_reanalysis.py",
            "snow_data_operational.py",
        ]

    def test_snow_failure_alone_exits_nonzero(self, fake_uv):
        """Meteo branch healthy, snow fails -- must still be a loud
        failure, not masked by the meteo branch's success."""
        result, calls = _run_cmd(fake_uv, snow_rc=1)
        assert result.returncode != 0
        assert calls == [
            "Quantile_Mapping_OP.py",
            "extend_era5_reanalysis.py",
            "snow_data_operational.py",
        ]

    def test_snow_runs_exactly_once_after_meteo_branch(self, fake_uv):
        """Regardless of QM/extend outcome, snow is invoked exactly once,
        and always as the last call -- proving it runs sequentially after
        the meteo branch rather than concurrently with it."""
        bin_dir, call_log = fake_uv
        for qm_rc, extend_rc in [(0, 0), (1, 0), (0, 1)]:
            call_log.write_text("")  # reset between scenarios
            _, calls = _run_cmd(fake_uv, qm_rc=qm_rc, extend_rc=extend_rc)
            assert calls.count("snow_data_operational.py") == 1
            assert calls[-1] == "snow_data_operational.py"


class TestDockerfileCmdStructure:
    """Structural checks independent of executing the command, so a
    divergence between the Dockerfile CMD and run_locally.sh's
    run_preprocessing_gateway() shows up even if one of them were changed
    to something unexecutable."""

    def test_cmd_keeps_qm_and_extend_chained_with_and_and(self):
        """QM -> extend must stay a real `&&` dependency, not decoupled
        the way snow was -- extend reads control-member CSVs only a
        successful QM writes."""
        cmd = _extract_cmd_string()
        assert "Quantile_Mapping_OP.py && uv run extend_era5_reanalysis.py" in cmd, (
            "QM and extend_era5_reanalysis must stay chained with && "
            f"(extend depends on QM's output); got: {cmd!r}"
        )

    def test_cmd_does_not_background_or_parallelize_snow(self):
        """No `&` backgrounding anywhere -- snow must run sequentially,
        never concurrently with the meteo branch (shared OUTPUT_PATH_DG,
        deleted by QM)."""
        cmd = _extract_cmd_string()
        # A bare `&` (job-control backgrounding) is the concern here, not
        # `&&` (chaining) or `>&2`/`2>&1`-style fd redirection, both of
        # which legitimately contain the character. Strip those first,
        # then confirm no bare `&` remains.
        stripped = cmd.replace("&&", "").replace(">&", "")
        assert "&" not in stripped, f"CMD must not background any command with a bare '&': {cmd!r}"

    def test_cmd_runs_snow_script(self):
        cmd = _extract_cmd_string()
        assert "uv run snow_data_operational.py" in cmd
