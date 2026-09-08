"""
Tests for PREPG-024: a `Quantile_Mapping_OP.py` failure must no longer
suppress `snow_data_operational.py` in `apps/run_locally.sh`.

See doc/plans/issues/archive/mid_prio_gi_draft_prepg_qm_failure_suppresses_snow.md.

Before this fix, `run_preprocessing_gateway()` ran the three
preprocessing_gateway scripts in a `for` loop that `break`s on the first
non-zero exit:

    for script in Quantile_Mapping_OP.py extend_era5_reanalysis.py \\
            snow_data_operational.py; do
        run_in_venv preprocessing_gateway "$script" || { rc=$?; break; }
    done

mirroring the Dockerfile's `&&` chain -- so a QM failure meant
`extend_era5_reanalysis.py` AND `snow_data_operational.py` never ran, even
though snow has no data dependency on either meteo script. The fix keeps
`QM -> extend` sequential-with-early-stop (extend genuinely depends on
QM's control-member CSVs) but runs snow unconditionally afterwards
(sequentially, never concurrently -- QM deletes every file in the shared
OUTPUT_PATH_DG that snow downloads into), and returns non-zero if either
branch failed.

Reuses the synthetic-venv test harness in conftest.py (`synth_tree`,
`SynthTree`, `run_main` -- fake `.venv/bin/python` stubs that log each
invocation and exit with a scripted code) rather than reimplementing it,
so these tests drive the REAL, unmodified `run_locally.sh` exactly the way
test_run_locally_orchestration.py's own tests do (that file originated the
harness and still documents its full rationale: sourcing under the
BASH_SOURCE guard, calling `main()` directly, why it lives under
apps/pipeline/tests/ instead of a standalone harness). The harness lives
in conftest.py -- not imported from test_run_locally_orchestration.py --
because `synth_tree` is a pytest fixture, and pytest auto-discovers
fixtures from conftest.py by name with no import needed; a cross-module
`from test_run_locally_orchestration import synth_tree` plus
`def test_x(synth_tree)` is flagged by ruff (F811) as redefining an
unused import.

apps/preprocessing_gateway/test/test_prepg024_snow_chain_coupling.py
covers the same four scenarios against the Dockerfile's CMD string
directly (via a real `sh -c` with `uv` stubbed out) -- the two files
together satisfy "cover both implementations": a defect reintroduced into
only one site fails only that site's tests, proving the two are not
accidentally validated by a single shared code path.

Run::

    cd apps
    SAPPHIRE_TEST_ENV=True pytest pipeline/tests/test_prepg024_snow_chain_coupling.py -v
"""

from __future__ import annotations

from conftest import SynthTree, run_main

# Decision snippets inserted into the fake preprocessing_gateway
# `.venv/bin/python` stub (see test_run_locally_orchestration._write_stub):
# raw bash executed after the call is logged, before the stub's default
# `exit 0`. Each branches on `$script` to fail exactly one of the three
# preprocessing_gateway scripts.
_FAIL_QM = 'if [ "$script" = "Quantile_Mapping_OP.py" ]; then exit 1; fi'
_FAIL_EXTEND = 'if [ "$script" = "extend_era5_reanalysis.py" ]; then exit 1; fi'
_FAIL_SNOW = 'if [ "$script" = "snow_data_operational.py" ]; then exit 1; fi'


def _gateway_calls(tree: SynthTree) -> list[str]:
    """Return just the script names run_in_venv invoked for
    preprocessing_gateway, in call order."""
    scripts = []
    for line in tree.calls():
        if "module=preprocessing_gateway " not in line:
            continue
        # Line shape: "CALL module=preprocessing_gateway script=<x>.py args=... mode=..."
        after_script = line.split("script=", 1)[1]
        scripts.append(after_script.split(" args=", 1)[0])
    return scripts


class TestPreprocessingGatewaySnowChainDecoupling:
    """Drives run_locally.sh's real `main preprocessing_gateway` dispatch,
    which calls `run_preprocessing_gateway()`, under four scripted
    failure scenarios."""

    def test_all_succeed_exits_zero(self, synth_tree: SynthTree):
        result = run_main(synth_tree, "preprocessing_gateway")
        assert result.returncode == 0, result.stdout + result.stderr
        assert _gateway_calls(synth_tree) == [
            "Quantile_Mapping_OP.py",
            "extend_era5_reanalysis.py",
            "snow_data_operational.py",
        ]

    def test_qm_failure_still_runs_snow_and_exits_nonzero(self, synth_tree: SynthTree):
        """A QM failure must not prevent snow from running, and the
        overall run_locally.sh exit status must stay non-zero -- QM's
        failure must not be masked by snow's success."""
        synth_tree.override("preprocessing_gateway", _FAIL_QM)

        result = run_main(synth_tree, "preprocessing_gateway")

        assert result.returncode != 0, result.stdout + result.stderr
        # extend must NOT run -- it depends on files only a successful QM
        # writes.
        assert _gateway_calls(synth_tree) == [
            "Quantile_Mapping_OP.py",
            "snow_data_operational.py",
        ]
        assert "preprocessing_gateway failed" in result.stdout

    def test_extend_failure_still_runs_snow_and_exits_nonzero(self, synth_tree: SynthTree):
        synth_tree.override("preprocessing_gateway", _FAIL_EXTEND)

        result = run_main(synth_tree, "preprocessing_gateway")

        assert result.returncode != 0, result.stdout + result.stderr
        assert _gateway_calls(synth_tree) == [
            "Quantile_Mapping_OP.py",
            "extend_era5_reanalysis.py",
            "snow_data_operational.py",
        ]

    def test_snow_failure_alone_exits_nonzero(self, synth_tree: SynthTree):
        """Meteo branch healthy, snow fails -- must still be a loud
        failure, not masked by the meteo branch's success."""
        synth_tree.override("preprocessing_gateway", _FAIL_SNOW)

        result = run_main(synth_tree, "preprocessing_gateway")

        assert result.returncode != 0, result.stdout + result.stderr
        assert _gateway_calls(synth_tree) == [
            "Quantile_Mapping_OP.py",
            "extend_era5_reanalysis.py",
            "snow_data_operational.py",
        ]

    def test_snow_runs_exactly_once_after_meteo_branch(self, synth_tree: SynthTree):
        """Regardless of QM/extend outcome, snow is invoked exactly once,
        and always as the last preprocessing_gateway call -- proving it
        runs sequentially after the meteo branch, never concurrently with
        it."""
        for decision in ["", _FAIL_QM, _FAIL_EXTEND]:
            synth_tree.call_log.write_text("")  # reset between scenarios
            synth_tree.override("preprocessing_gateway", decision)

            run_main(synth_tree, "preprocessing_gateway")

            calls = _gateway_calls(synth_tree)
            assert calls.count("snow_data_operational.py") == 1
            assert calls[-1] == "snow_data_operational.py"
