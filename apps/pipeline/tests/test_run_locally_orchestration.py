"""Regression tests for apps/run_locally.sh's orchestration/dispatch logic.

Context: three behaviour changes were made to run_locally.sh in earlier
INFRA-032 phases, and this file locks all three in end-to-end:

  A. A ``--continue-on-error`` hint, emitted once from ``main()`` after
     dispatch, when a fail-fast target aborted early with the flag unset.
  B. Mode resolution/validation for the bare ``machine_learning``
     single-module target (``resolve_ml_bare_target_modes``), which has no
     outer mode loop to resolve SAPPHIRE_PREDICTION_MODE for it the way the
     daily/maintenance pipelines do.
  C. A downgrade of ``sync_long_horizon_hydrograph.py`` exit code 4 (SDK
     norm lookup failure) from aborting ``preprocessing_runoff`` maintenance
     to a separate FAIL row that lets the rest of the run continue.

``apps/test_run_tests.sh`` is the local precedent for driving a bash script
from tests with synthetic fixtures (fake ``.venv/bin/<exe>`` stubs that exit
with a chosen code), but it is a standalone bash harness that is NOT wired
into ``run_tests.sh``'s MODULES list, so tests placed there never run in the
project's mandated gate. This file lives under ``apps/pipeline/tests/``
instead specifically so it does.

``run_locally.sh`` ends with a ``[[ "${BASH_SOURCE[0]}" == "${0}" ]]``
guard, so sourcing it defines functions/arrays without invoking ``main``.
Each test sources the real, unmodified script, overrides only *data*
globals (SCRIPT_DIR, LOG_DIR, LOG_FILE, and the
ML_MODELS/ML_SCRIPTS/ML_MAINTENANCE_SCRIPTS arrays, purely to keep runtime
down) and calls ``main`` with a real target. Every module's fake
``.venv/bin/python`` is a small bash stub that logs its invocation (module,
script, args, SAPPHIRE_PREDICTION_MODE) to a shared call log and exits with
a scripted code, so assertions are made only on process exit code and
stdout/stderr text -- never on run_locally.sh's internal shell variables.

NOTE on scope: this file does not test whether an exit-4 downgrade reflects
a *single* station's norm-lookup failure vs. *all* stations failing --
that distinction is not observable at the shell level, since
sync_long_horizon_hydrograph.py always exits one process code regardless of
how many stations failed underneath it. That is already covered at the
Python level by
apps/preprocessing_runoff/test/test_sync_long_horizon_hydrograph.py.
"""

from __future__ import annotations

import os
import subprocess
import textwrap
from dataclasses import dataclass
from pathlib import Path

import pytest

APPS_DIR = Path(__file__).resolve().parents[2]
RUN_LOCALLY_SH = APPS_DIR / "run_locally.sh"
REAL_LOG_DIR = APPS_DIR / "logs"

# Every module run_locally.sh's main() dispatch can reach across the targets
# exercised below (daily / maintenance / all / initialize / yearly / bare
# machine_learning). Stubbed unconditionally so validate_env's venv check
# always passes, whichever target a given test drives.
MODULES = [
    "preprocessing_runoff",
    "preprocessing_gateway",
    "linear_regression",
    "machine_learning",
    "postprocessing_forecasts",
    "long_term_forecasting",
]

# query_lt_schedule() calls long_term_forecasting's lt_schedule_query.py and
# parses its last stdout line as JSON with the real system python3 (not the
# fake venv). Every stub answers "nothing active" so the long-term phase of
# `daily` / `long-term-operational` stays inert in every test here -- none
# of them are testing long-term scheduling.
LT_SCHEDULE_EMPTY_JSON = '{"active_modes": [], "skill_metric_types": []}'

# Env vars that could leak in from the developer's shell and silently change
# which branch run_locally.sh takes. Popped before every subprocess;
# individual tests re-add only what they need via `extra_env`.
_ISOLATE_ENV_VARS = [
    "ieasyhydroforecast_organization",
    "SAPPHIRE_PREDICTION_MODE",
    "ML_MODE",
    "CONTINUE_ON_ERROR",
    "DRY_RUN",
    "LT_FORECAST_TODAY",
    "LT_OPERATIONAL_ISSUE_DAYS",
    "LT_OPERATIONAL_MODES",
    "LT_SIMULATE_YEARS",
    "LT_SIMULATE_NUM_MONTHS",
    "LT_SIMULATE_MODES",
    "LT_ACTIVE_WINDOW",
    "LT_ACTIVE_MODES",
    "LT_SKILL_METRIC_TYPES",
    "RUNOFF_LONG_HORIZON_TARGET_YEAR",
    "POSTPROCESSING_GAPFILL_WINDOW_MONTHS",
    "SAPPHIRE_CONSISTENCY_CHECK",
    "ieasyhydroforecast_START_DATE",
    "lt_forecast_mode",
]

# A fake `python` stub: logs its invocation to a call log, then lets a
# test-supplied bash snippet decide the exit code (falling back to 0).
# `@MODULE@` / `@CALL_LOG@` / `@LT_STUB@` / `@DECISION@` are substituted
# with plain str.replace() (not str.format()/f-string) because the template
# is full of literal bash `${...}` expansions that would otherwise have to
# be brace-escaped.
_STUB_TEMPLATE = """#!/usr/bin/env bash
script="$1"
shift || true
printf 'CALL module=@MODULE@ script=%s args=%s mode=%s\\n' \\
    "$script" "$*" "${SAPPHIRE_PREDICTION_MODE:-}" >> "@CALL_LOG@"
@LT_STUB@
@DECISION@
exit 0
"""


def _write_stub(module_dir: Path, module: str, call_log: Path, decision: str = "") -> None:
    """(Re)write module_dir/.venv/bin/python as a fake python executable.

    Every invocation appends one line to `call_log`:
        CALL module=<module> script=<script> args=<args> mode=<SAPPHIRE_PREDICTION_MODE>

    `decision` is raw bash inserted before the default `exit 0`, letting a
    test branch on `$script` / `$SAPPHIRE_PREDICTION_MODE` to script the
    exit code for a specific invocation. Matches run_in_venv's call
    convention: the fake python is invoked as `python <script> [args...]`,
    run from the module's own directory, with SAPPHIRE_PREDICTION_MODE (and
    any extra KEY=VALUE pairs) set in its environment.

    Args:
        module_dir: Synthetic SCRIPT_DIR/<module> directory.
        module: Module name, used only in the logged call-log line.
        call_log: Shared file every stub appends one line to per call.
        decision: Raw bash executed after logging, before the default
            `exit 0` -- typically an `if`/`case` on `$script` that calls
            `exit N` for a specific script name.
    """
    bin_dir = module_dir / ".venv" / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    python_path = bin_dir / "python"

    lt_schedule_stub = ""
    if module == "long_term_forecasting":
        lt_schedule_stub = (
            'if [ "$script" = "lt_schedule_query.py" ]; then\n'
            f"    echo '{LT_SCHEDULE_EMPTY_JSON}'\n"
            "    exit 0\n"
            "fi"
        )

    content = (
        _STUB_TEMPLATE.replace("@MODULE@", module)
        .replace("@CALL_LOG@", str(call_log))
        .replace("@LT_STUB@", lt_schedule_stub)
        .replace("@DECISION@", decision)
    )
    python_path.write_text(content)
    python_path.chmod(0o755)


@dataclass
class SynthTree:
    """A synthetic SCRIPT_DIR tree of fake module venvs plus support files."""

    script_dir: Path
    env_file: Path
    call_log: Path
    log_dir: Path

    def override(self, module: str, decision: str) -> None:
        """Rewrite one module's stub with custom exit-code decision logic."""
        _write_stub(self.script_dir / module, module, self.call_log, decision)

    def calls(self) -> list[str]:
        """Return the call log as a list of lines, in invocation order."""
        if not self.call_log.exists():
            return []
        return [line for line in self.call_log.read_text().splitlines() if line]


@pytest.fixture
def synth_tree(tmp_path: Path) -> SynthTree:
    """Build a synthetic SCRIPT_DIR with default (always-succeed) stubs.

    Org is fixed to a value that is neither "demo" nor "uzhm" so
    should_skip_module() never skips a module -- every dispatch path
    exercised below stays reachable.
    """
    script_dir = tmp_path / "synth_apps"
    script_dir.mkdir()
    call_log = tmp_path / "calls.log"
    call_log.write_text("")
    for module in MODULES:
        _write_stub(script_dir / module, module, call_log)

    env_file = tmp_path / "test.env"
    env_file.write_text(
        "ieasyhydroforecast_organization=testorg\nieasyhydroforecast_START_DATE=2020-01-01\n"
    )

    return SynthTree(
        script_dir=script_dir,
        env_file=env_file,
        call_log=call_log,
        log_dir=tmp_path / "logs",
    )


def run_main(
    tree: SynthTree,
    target: str,
    *,
    continue_on_error: bool = False,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Source run_locally.sh, override data-only globals, and call main().

    Drives the real, unmodified main() (arg parsing, validate_env, dispatch,
    the continue-on-error hint, print_summary, exit code) exactly as
    apps/test_run_tests.sh drives run_tests.sh's main() -- by sourcing the
    script (which the BASH_SOURCE guard keeps from auto-running) and then
    calling main() directly with real arguments.

    ML_MODELS / ML_SCRIPTS / ML_MAINTENANCE_SCRIPTS are overridden to a
    single entry each purely to keep runtime down -- they are plain
    configuration data iterated by run_machine_learning /
    run_maintenance_machine_learning, not logic under test.
    """
    log_file = tree.log_dir / "run.log"
    flag = "--continue-on-error " if continue_on_error else ""

    script = textwrap.dedent(f"""
        source "{RUN_LOCALLY_SH}"
        SCRIPT_DIR="{tree.script_dir}"
        LOG_DIR="{tree.log_dir}"
        LOG_FILE="{log_file}"
        ML_MODELS=(TFT)
        ML_SCRIPTS=(recalculate_nan_forecasts.py)
        ML_MAINTENANCE_SCRIPTS=(recalculate_nan_forecasts.py)
        main {flag}{target}
        """)

    env = os.environ.copy()
    for var in _ISOLATE_ENV_VARS:
        env.pop(var, None)
    env["ieasyhydroforecast_env_file_path"] = str(tree.env_file)
    if extra_env:
        env.update(extra_env)

    return subprocess.run(
        ["bash", "-c", script],
        cwd=str(APPS_DIR),
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )


def _mode_of(call_line: str) -> str:
    """Extract the trailing `mode=<value>` field from a call-log line."""
    return call_line.rsplit("mode=", 1)[-1]


@pytest.fixture(autouse=True)
def protect_real_apps_logs():
    """Fail loudly if a test writes into the live apps/logs/ directory.

    apps/logs/ is the operator's real log directory -- every test here must
    override LOG_DIR/LOG_FILE after sourcing, exactly as instructed. This
    fixture is the verification: it snapshots apps/logs/ before and after
    every single test and fails if anything changed.
    """
    before = set(REAL_LOG_DIR.iterdir()) if REAL_LOG_DIR.exists() else set()
    yield
    after = set(REAL_LOG_DIR.iterdir()) if REAL_LOG_DIR.exists() else set()
    assert after == before, (
        f"A test wrote into the live apps/logs/ directory: new/changed entries = {after - before}"
    )


# ---------------------------------------------------------------------------
# Group A -- the --continue-on-error hint
# ---------------------------------------------------------------------------


class TestContinueOnErrorHint:
    """The hint is emitted once from main(), after dispatch, before
    print_summary, gated on exit_code != 0 && CONTINUE_ON_ERROR == false &&
    IS_FAIL_FAST_TARGET == true.
    """

    def test_fail_fast_target_without_flag_emits_hint_once(self, synth_tree):
        synth_tree.override(
            "preprocessing_runoff",
            'if [ "$script" = "preprocessing_runoff.py" ]; then exit 1; fi',
        )
        result = run_main(synth_tree, "daily")
        out = result.stdout + result.stderr

        assert result.returncode != 0
        assert out.count("Pipeline stopped early because") == 1
        assert "bash apps/run_locally.sh --continue-on-error daily" in out

    def test_continue_on_error_suppresses_hint_but_still_exits_nonzero(self, synth_tree):
        synth_tree.override(
            "preprocessing_runoff",
            'if [ "$script" = "preprocessing_runoff.py" ]; then exit 1; fi',
        )
        result = run_main(synth_tree, "daily", continue_on_error=True)
        out = result.stdout + result.stderr

        assert "Pipeline stopped early because" not in out
        # An earlier draft of this issue wrongly claimed --continue-on-error
        # makes a failing run exit 0 -- it must not.
        assert result.returncode != 0
        # And it must be because later modules genuinely ran, not because
        # the run silently stopped without recording anything.
        assert any("module=machine_learning" in ln for ln in synth_tree.calls())

    def test_single_module_target_failure_emits_no_hint(self, synth_tree):
        synth_tree.override(
            "preprocessing_runoff",
            'if [ "$script" = "preprocessing_runoff.py" ]; then exit 1; fi',
        )
        result = run_main(synth_tree, "preprocessing_runoff")

        assert result.returncode != 0
        assert "Pipeline stopped early because" not in (result.stdout + result.stderr)

    def test_all_target_failure_emits_no_hint(self, synth_tree):
        synth_tree.override(
            "preprocessing_runoff",
            'if [ "$script" = "preprocessing_runoff.py" ]; then exit 1; fi',
        )
        result = run_main(synth_tree, "all")

        assert result.returncode != 0
        assert "Pipeline stopped early because" not in (result.stdout + result.stderr)
        # run_all always runs the long-term phase after short-term,
        # regardless of a short-term failure -- a non-zero return here
        # really doesn't mean anything was skipped.
        assert any("module=long_term_forecasting" in ln for ln in synth_tree.calls())

    def test_yearly_target_failure_emits_no_hint(self, synth_tree):
        synth_tree.override(
            "postprocessing_forecasts",
            'if [ "$script" = "recalculate_skill_metrics.py" ]; then exit 1; fi',
        )
        result = run_main(synth_tree, "yearly")

        assert result.returncode != 0
        assert "Pipeline stopped early because" not in (result.stdout + result.stderr)
        calls = synth_tree.calls()
        assert any("script=recalculate_snow_norms.py" in ln for ln in calls)
        assert any("script=recalculate_skill_metrics.py" in ln for ln in calls)


# ---------------------------------------------------------------------------
# Group B -- bare `machine_learning` target mode resolution
# ---------------------------------------------------------------------------


class TestMachineLearningBareTargetModes:
    """resolve_ml_bare_target_modes validates SAPPHIRE_PREDICTION_MODE and
    ML_MODE for the bare `machine_learning` single-module target, which has
    no outer mode loop to resolve SAPPHIRE_PREDICTION_MODE the way the
    daily/maintenance pipelines do.
    """

    def test_default_mode_resolves_and_invokes(self, synth_tree):
        result = run_main(synth_tree, "machine_learning")

        assert result.returncode == 0, result.stdout + result.stderr
        calls = [ln for ln in synth_tree.calls() if "module=machine_learning" in ln]
        assert calls, "machine_learning stub was never invoked"
        # Default ML_MODE is DECAD (see run_locally.sh's
        # ML_MODE="${ML_MODE:-DECAD}"), and SAPPHIRE_PREDICTION_MODE was
        # unset, so exactly DECAD should have been resolved and forwarded.
        assert [_mode_of(ln) for ln in calls] == ["DECAD"]

    def test_ml_mode_both_runs_pentad_then_decad_in_order(self, synth_tree):
        result = run_main(synth_tree, "machine_learning", extra_env={"ML_MODE": "BOTH"})

        assert result.returncode == 0, result.stdout + result.stderr
        modes = [_mode_of(ln) for ln in synth_tree.calls() if "module=machine_learning" in ln]
        assert modes == ["PENTAD", "DECAD"]

    def test_inconsistent_prediction_mode_and_ml_mode_errors_and_never_invokes(self, synth_tree):
        result = run_main(
            synth_tree,
            "machine_learning",
            extra_env={"SAPPHIRE_PREDICTION_MODE": "PENTAD", "ML_MODE": "DECAD"},
        )
        out = result.stdout + result.stderr

        assert result.returncode != 0
        assert "SAPPHIRE_PREDICTION_MODE=PENTAD" in out
        assert "ML_MODE=DECAD" in out
        assert not any("module=machine_learning" in ln for ln in synth_tree.calls())

    def test_prediction_mode_both_with_ml_mode_decad_runs_decad_only(self, synth_tree):
        result = run_main(
            synth_tree,
            "machine_learning",
            extra_env={"SAPPHIRE_PREDICTION_MODE": "BOTH", "ML_MODE": "DECAD"},
        )

        assert result.returncode == 0, result.stdout + result.stderr
        modes = [_mode_of(ln) for ln in synth_tree.calls() if "module=machine_learning" in ln]
        assert modes == ["DECAD"]

    def test_invalid_ml_mode_errors_and_never_invokes(self, synth_tree):
        result = run_main(synth_tree, "machine_learning", extra_env={"ML_MODE": "JUNK"})
        out = result.stdout + result.stderr

        assert result.returncode != 0
        assert "ML_MODE" in out
        assert "JUNK" in out
        assert not any("module=machine_learning" in ln for ln in synth_tree.calls())

    def test_both_mode_first_failure_stops_second_mode(self, synth_tree):
        synth_tree.override(
            "machine_learning",
            (
                'if [ "$script" = "recalculate_nan_forecasts.py" ] '
                '&& [ "$SAPPHIRE_PREDICTION_MODE" = "PENTAD" ]; then exit 1; fi'
            ),
        )
        result = run_main(synth_tree, "machine_learning", extra_env={"ML_MODE": "BOTH"})

        assert result.returncode != 0
        modes = [_mode_of(ln) for ln in synth_tree.calls() if "module=machine_learning" in ln]
        assert modes == ["PENTAD"]


# ---------------------------------------------------------------------------
# Group C -- the long-horizon sync exit-4 downgrade
# ---------------------------------------------------------------------------

# (lt_rc, expect_nonzero_exit, expect_maintenance_status, expect_extra_fail_row)
_LT_RC_CASES = [
    (0, False, "PASS", False),
    (2, False, "PASS", False),
    (4, True, "PASS", True),
    (1, True, "FAIL", False),
    (3, True, "FAIL", False),
    (5, True, "FAIL", False),
]


def _override_sync_exit_code(tree: SynthTree, lt_rc: int) -> None:
    """Make preprocessing_runoff.py always pass and only
    sync_long_horizon_hydrograph.py exit with `lt_rc`.
    """
    tree.override(
        "preprocessing_runoff",
        textwrap.dedent(f"""\
            if [ "$script" = "preprocessing_runoff.py" ]; then
                exit 0
            fi
            if [ "$script" = "sync_long_horizon_hydrograph.py" ]; then
                exit {lt_rc}
            fi
            """),
    )


class TestLongHorizonSyncExitCodeHandling:
    """run_maintenance_preprocessing_runoff's handling of
    sync_long_horizon_hydrograph.py's exit code (run via
    `run_in_venv preprocessing_runoff ...`).
    """

    @pytest.mark.parametrize(
        ("lt_rc", "expect_nonzero_exit", "expect_maintenance_status", "expect_extra_fail_row"),
        _LT_RC_CASES,
        ids=[f"lt_rc={case[0]}" for case in _LT_RC_CASES],
    )
    def test_exit_code_table(
        self,
        synth_tree,
        lt_rc,
        expect_nonzero_exit,
        expect_maintenance_status,
        expect_extra_fail_row,
    ):
        _override_sync_exit_code(synth_tree, lt_rc)
        result = run_main(synth_tree, "maintenance:preprocessing_runoff")
        out = result.stdout + result.stderr

        if expect_nonzero_exit:
            assert result.returncode != 0
        else:
            assert result.returncode == 0, out

        assert f"preprocessing_runoff (maintenance): {expect_maintenance_status}" in out

        extra_row = "preprocessing_runoff (long-horizon sync): FAIL"
        if expect_extra_fail_row:
            assert extra_row in out
        else:
            assert extra_row not in out

    def test_daily_continues_past_downgraded_failure_and_runs_ml(self, synth_tree):
        _override_sync_exit_code(synth_tree, 4)
        result = run_main(synth_tree, "daily")

        assert result.returncode != 0
        calls = synth_tree.calls()
        assert any(
            "module=preprocessing_gateway" in ln and "script=extend_era5_reanalysis.py" in ln
            for ln in calls
        ), "maintenance:preprocessing_gateway step never ran -- Phase 2 did not continue"
        assert any("module=machine_learning" in ln for ln in calls), (
            "machine_learning never ran after the downgraded exit-4 failure"
        )

    def test_maintenance_continues_past_downgraded_failure(self, synth_tree):
        _override_sync_exit_code(synth_tree, 4)
        result = run_main(synth_tree, "maintenance")

        assert result.returncode != 0
        calls = synth_tree.calls()
        assert any(
            "module=preprocessing_gateway" in ln and "script=extend_era5_reanalysis.py" in ln
            for ln in calls
        ), "maintenance:preprocessing_gateway step never ran -- pipeline did not continue"

    def test_initialize_continues_past_downgraded_failure(self, synth_tree):
        _override_sync_exit_code(synth_tree, 4)
        result = run_main(synth_tree, "initialize")

        assert result.returncode != 0
        calls = synth_tree.calls()
        assert any("script=initial_api_sync.py" in ln for ln in calls), (
            "Step 2 (initial API sync) never ran -- Step 1 did not continue"
        )

    def test_bare_maintenance_target_continues_past_downgraded_failure(self, synth_tree):
        _override_sync_exit_code(synth_tree, 4)
        result = run_main(synth_tree, "maintenance:preprocessing_runoff")
        out = result.stdout + result.stderr

        assert result.returncode != 0
        assert "preprocessing_runoff (maintenance): PASS" in out
        assert "preprocessing_runoff (long-horizon sync): FAIL" in out

    def test_fatal_code_five_still_aborts_daily_and_ml_never_runs(self, synth_tree):
        _override_sync_exit_code(synth_tree, 5)
        result = run_main(synth_tree, "daily")

        assert result.returncode != 0
        calls = synth_tree.calls()
        assert not any("module=machine_learning" in ln for ln in calls), (
            "machine_learning ran despite a fatal (exit 5) maintenance failure"
        )
