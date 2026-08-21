"""Regression tests for apps/run_locally.sh's orchestration/dispatch logic.

Context: three behaviour changes were made to run_locally.sh in earlier
INFRA-037 phases, and this file locks all three in end-to-end:

  A. A ``--continue-on-error`` hint, emitted once from ``main()`` after
     dispatch, gated on the ``PIPELINE_ABORTED`` fact recorded by the
     CONTINUE_ON_ERROR guard idiom itself at its abort site -- not on an
     inference from the target name. An earlier version of this gate used a
     hardcoded per-target ``IS_FAIL_FAST_TARGET`` list, which was wrong both
     ways: it excluded ``all``/``yearly`` even though a guarded step inside
     them can genuinely skip later work, and it included ``maintenance``
     even though a failure in its *last* guarded step skips nothing, making
     the old hint text ("the remaining modules did not run") false.
  B. Mode resolution/validation for the bare ``machine_learning``
     single-module target (``resolve_ml_bare_target_modes``), which has no
     outer mode loop to resolve SAPPHIRE_PREDICTION_MODE for it the way the
     daily/maintenance pipelines do. Validation must run under the mode(s)
     ML actually attempted, not the pre-loop original mode.
  C. A downgrade of ``sync_long_horizon_hydrograph.py`` exit code 4 (SDK
     norm lookup failure) from aborting ``preprocessing_runoff`` maintenance
     to a separate FAIL row that lets the rest of the run continue. For the
     remaining fatal exit codes (1, 3, 5), CURRENT_MODULE_LOG must stay
     pointed at the long-horizon log when the module-level FAIL is
     recorded, so MODULE ERROR DETAILS shows the sub-step output that
     explains the failure rather than the maintenance log's unrelated
     successful primary-step output.

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


# The `NC` (no-color) ANSI reset code run_locally.sh's log() appends after
# every line it prints -- see run_locally.sh's NC='\033[0m'. The hint line
# is colorized (WARN -> YELLOW), so the raw captured text ends in this
# escape sequence; it must be stripped before the trailing token (the
# target) is usable as a real shell argument.
_ANSI_RESET_SUFFIX = "\x1b[0m"


def _extract_hint_command(out: str) -> str:
    """Pull the literal copy-pasteable command out of the emitted hint text.

    The hint line (see emit_continue_on_error_hint) reads:
        ... run: <ieasyhydroforecast_env_file_path=... >bash apps/run_locally.sh --continue-on-error <target>
    Returns everything after "run: ", with the trailing ANSI reset code (if
    any) and surrounding whitespace stripped.
    """
    marker = "stopping at the first, run: "
    for line in out.splitlines():
        if marker in line:
            command = line.split(marker, 1)[1]
            if command.endswith(_ANSI_RESET_SUFFIX):
                command = command[: -len(_ANSI_RESET_SUFFIX)]
            return command.strip()
    raise AssertionError(f"continue-on-error hint command not found in output:\n{out}")


def _real_apps_logs_snapshot() -> dict[str, tuple[int, int]]:
    """Map each entry name in apps/logs/ to (size, mtime_ns).

    A directory-entry-name-only snapshot (the previous version of this
    fixture) misses a test that appends to an EXISTING file -- e.g. a
    module-level TimedRotatingFileHandler bound to the real apps/logs/log
    that a test writes into: the file was already there before the test,
    so its name alone doesn't change. (size, mtime_ns) per entry catches
    that without the cost of hashing file contents -- apps/logs/log can be
    multiple MB, and this fixture runs on every test in this file.
    """
    if not REAL_LOG_DIR.exists():
        return {}
    snapshot = {}
    for entry in REAL_LOG_DIR.iterdir():
        try:
            stat = entry.stat()
        except OSError:
            # Entry vanished between iterdir() and stat() -- treat as
            # "no stable info", which still can't spuriously match a
            # differently-sized/timestamped post-test entry.
            continue
        snapshot[entry.name] = (stat.st_size, stat.st_mtime_ns)
    return snapshot


@pytest.fixture(autouse=True)
def protect_real_apps_logs():
    """Fail loudly if a test writes into the live apps/logs/ directory.

    apps/logs/ is the operator's real log directory -- every test here must
    override LOG_DIR/LOG_FILE after sourcing, exactly as instructed. This
    fixture is the verification: it snapshots apps/logs/ before and after
    every single test and fails if anything changed -- including a test
    that only APPENDS to an existing file there, which a name-only
    directory-entry comparison would miss (see _real_apps_logs_snapshot).
    """
    before = _real_apps_logs_snapshot()
    yield
    after = _real_apps_logs_snapshot()
    assert after == before, (
        "A test wrote into the live apps/logs/ directory: "
        f"added/removed entries = {after.keys() ^ before.keys()}, "
        "changed entries (size, mtime_ns) = "
        f"{ {name: (before.get(name), after.get(name)) for name in after.keys() & before.keys() if after[name] != before[name]} }"
    )


# ---------------------------------------------------------------------------
# Group A -- the --continue-on-error hint
# ---------------------------------------------------------------------------


class TestContinueOnErrorHint:
    """The hint is emitted once from main(), after dispatch, before
    print_summary, gated on PIPELINE_ABORTED == true -- a fact recorded by
    the CONTINUE_ON_ERROR guard idiom itself at the abort site (not an
    inference from the target name). PIPELINE_ABORTED can only become true
    when CONTINUE_ON_ERROR is false, so the gate needs no extra condition.
    """

    HINT_TEXT = "stopped at its first failing module"

    def test_fail_fast_target_without_flag_emits_hint_once(self, synth_tree):
        synth_tree.override(
            "preprocessing_runoff",
            'if [ "$script" = "preprocessing_runoff.py" ]; then exit 1; fi',
        )
        result = run_main(synth_tree, "daily")
        out = result.stdout + result.stderr

        assert result.returncode != 0
        assert out.count(self.HINT_TEXT) == 1
        assert "bash apps/run_locally.sh --continue-on-error daily" in out

    def test_continue_on_error_suppresses_hint_but_still_exits_nonzero(self, synth_tree):
        synth_tree.override(
            "preprocessing_runoff",
            'if [ "$script" = "preprocessing_runoff.py" ]; then exit 1; fi',
        )
        result = run_main(synth_tree, "daily", continue_on_error=True)
        out = result.stdout + result.stderr

        assert self.HINT_TEXT not in out
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
        # A single-module target's dispatch arm assigns exit_code directly
        # (`run_X || exit_code=$?`), never going through the
        # CONTINUE_ON_ERROR guard idiom, so PIPELINE_ABORTED stays false.
        assert self.HINT_TEXT not in (result.stdout + result.stderr)

    def test_all_target_short_term_failure_emits_hint(self, synth_tree):
        """Defect (a): `all` was excluded from the old hardcoded fail-fast
        list, but when preprocessing_runoff fails, run_short_term_pipeline's
        own guard idiom aborts it -- genuinely skipping gateway/forecasting/
        postprocessing/validation for the short-term phase -- before run_all
        goes on to run long-term regardless. PIPELINE_ABORTED is set at that
        abort site regardless of which top-level target called in, so the
        hint now fires even though `all` itself never touches the guard
        idiom directly.
        """
        synth_tree.override(
            "preprocessing_runoff",
            'if [ "$script" = "preprocessing_runoff.py" ]; then exit 1; fi',
        )
        result = run_main(synth_tree, "all")

        assert result.returncode != 0
        assert self.HINT_TEXT in (result.stdout + result.stderr)
        # run_all always runs the long-term phase after short-term,
        # regardless of a short-term failure -- that part of the pipeline
        # was NOT skipped, even though the short-term phase genuinely was.
        assert any("module=long_term_forecasting" in ln for ln in synth_tree.calls())
        # But the short-term modules after preprocessing_runoff really were
        # skipped.
        assert not any("module=linear_regression" in ln for ln in synth_tree.calls())

    def test_yearly_snow_norms_failure_emits_hint_and_skips_skill_metrics(self, synth_tree):
        """Defect (b): `yearly` was excluded from the old hardcoded fail-fast
        list, but run_yearly_pipeline has two guarded steps -- if snow norms
        fails, skill metrics is genuinely skipped, and the operator got no
        hint under the old inference-based gate.
        """
        synth_tree.override(
            "preprocessing_gateway",
            'if [ "$script" = "recalculate_snow_norms.py" ]; then exit 1; fi',
        )
        result = run_main(synth_tree, "yearly")
        out = result.stdout + result.stderr

        assert result.returncode != 0
        assert self.HINT_TEXT in out
        calls = synth_tree.calls()
        assert any("script=recalculate_snow_norms.py" in ln for ln in calls)
        assert not any("script=recalculate_skill_metrics.py" in ln for ln in calls), (
            "skill metrics ran despite snow norms aborting the yearly pipeline"
        )

    def test_yearly_last_step_failure_emits_hint_without_claiming_skipped_modules(self, synth_tree):
        """Defect (c): when the FINAL guarded step of a pipeline fails,
        nothing remains to skip, so the hint text must not assert that
        remaining modules did not run -- that would be knowingly false.
        """
        synth_tree.override(
            "postprocessing_forecasts",
            'if [ "$script" = "recalculate_skill_metrics.py" ]; then exit 1; fi',
        )
        result = run_main(synth_tree, "yearly")
        out = result.stdout + result.stderr

        assert result.returncode != 0
        # Both yearly steps ran -- skill metrics (the last one) is what
        # failed, nothing was left to skip.
        calls = synth_tree.calls()
        assert any("script=recalculate_snow_norms.py" in ln for ln in calls)
        assert any("script=recalculate_skill_metrics.py" in ln for ln in calls)
        # The hint still fires (a real abort happened)...
        assert self.HINT_TEXT in out
        # ...but must not claim modules remained unrun. Neither hint line
        # may assert that anything "remained" or was left to run "anyway"
        # -- the offer to use --continue-on-error must stand on its own,
        # without implying there is necessarily unrun work waiting behind
        # the failure.
        assert "did not run" not in out
        assert "remaining modules" not in out
        assert "modules anyway" not in out

    def test_hint_command_is_actually_runnable_as_printed(self, synth_tree):
        """Round-4 review finding 3: 'The test merely matches the string and
        never executes the suggested command.' validate_env REQUIRES
        ieasyhydroforecast_env_file_path to be set to an existing file, so a
        hint that prints a bare `bash apps/run_locally.sh --continue-on-error
        <target>` is not actually copy-pasteable -- an operator who runs it
        verbatim gets a validation failure, not a continued run.

        This test extracts the literal command the hint prints, then
        executes it for real (through the same source-and-override harness
        every other test in this file uses) WITHOUT pre-seeding
        ieasyhydroforecast_env_file_path in the subprocess environment --
        proving the fix works: the command carries its own working env-file
        assignment and gets past validate_env, then genuinely proceeds past
        the module that failed on the first run.
        """
        synth_tree.override(
            "preprocessing_runoff",
            'if [ "$script" = "preprocessing_runoff.py" ]; then exit 1; fi',
        )
        first = run_main(synth_tree, "daily")
        out = first.stdout + first.stderr
        assert first.returncode != 0

        command = _extract_hint_command(out)
        assert command.startswith("ieasyhydroforecast_env_file_path="), (
            f"hint command does not interpolate the env file path: {command!r}"
        )

        env_assignment, sep, rest = command.partition(" bash ")
        assert sep, f"expected ' bash ' after the env assignment in: {command!r}"
        env_key, _, env_value = env_assignment.partition("=")
        assert env_key == "ieasyhydroforecast_env_file_path"
        assert env_value == str(synth_tree.env_file), (
            "hint command's env file path does not match the path this run "
            f"actually used: {env_value!r} vs {synth_tree.env_file}"
        )

        script_name, _, main_args = rest.partition(" ")
        assert script_name == "apps/run_locally.sh"
        main_args = main_args.strip()
        assert main_args == "--continue-on-error daily"

        # Re-run the extracted command for real. Source the unmodified
        # script and override only the synthetic data globals -- exactly
        # what run_main() does -- but this time do NOT pre-set
        # ieasyhydroforecast_env_file_path in the subprocess environment.
        # The command line itself, exactly as the hint printed it, must
        # supply it.
        rerun_log_file = synth_tree.log_dir / "hint_rerun.log"
        script = textwrap.dedent(f"""
            source "{RUN_LOCALLY_SH}"
            SCRIPT_DIR="{synth_tree.script_dir}"
            LOG_DIR="{synth_tree.log_dir}"
            LOG_FILE="{rerun_log_file}"
            ML_MODELS=(TFT)
            ML_SCRIPTS=(recalculate_nan_forecasts.py)
            ML_MAINTENANCE_SCRIPTS=(recalculate_nan_forecasts.py)
            {env_key}={env_value} main {main_args}
            """)

        env = os.environ.copy()
        for var in _ISOLATE_ENV_VARS:
            env.pop(var, None)
        env.pop("ieasyhydroforecast_env_file_path", None)

        rerun = subprocess.run(
            ["bash", "-c", script],
            cwd=str(APPS_DIR),
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        rerun_out = rerun.stdout + rerun.stderr

        # It must NOT die in validate_env for a missing/unset env file --
        # that was the entire bug: the old hint printed a command with no
        # env var at all, so validate_env failed before --continue-on-error
        # ever had a chance to matter.
        assert "ieasyhydroforecast_env_file_path is not set" not in rerun_out, rerun_out
        assert "Env file not found" not in rerun_out, rerun_out

        # And it must genuinely proceed past preprocessing_runoff (the
        # module that failed on the first run) into later Phase-3 modules
        # -- proving --continue-on-error took effect, not merely that
        # validation passed.
        calls = synth_tree.calls()
        assert any("module=machine_learning" in ln for ln in calls), (
            "re-running the hint's own printed command did not get past "
            f"the failing module. Output:\n{rerun_out}"
        )


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

    @staticmethod
    def _validation_calls(tree: SynthTree) -> list[str]:
        """Calls to validate_pipeline.py (run via the postprocessing_forecasts
        stub, per run_module_validation's run_in_venv invocation).
        """
        return [
            ln
            for ln in tree.calls()
            if "module=postprocessing_forecasts" in ln and "validate_pipeline.py" in ln
        ]

    def test_validation_uses_the_mode_ml_actually_ran_under(self, synth_tree):
        """Regression for the bug where main()'s machine_learning) case
        restored SAPPHIRE_PREDICTION_MODE to original_mode BEFORE calling
        run_module_validation -- so with SAPPHIRE_PREDICTION_MODE unset,
        validation ran under an empty mode (which validate_pipeline.py then
        defaults to PENTAD) even though ML itself ran under DECAD (the
        default ML_MODE). Validation must check the same mode ML produced.
        """
        result = run_main(synth_tree, "machine_learning")

        assert result.returncode == 0, result.stdout + result.stderr
        ml_modes = [_mode_of(ln) for ln in synth_tree.calls() if "module=machine_learning" in ln]
        validation_modes = [_mode_of(ln) for ln in self._validation_calls(synth_tree)]

        assert ml_modes, "machine_learning stub was never invoked"
        assert validation_modes, "validate_pipeline.py was never invoked"
        assert validation_modes == ml_modes

    def test_partial_both_mode_run_validates_only_attempted_modes(self, synth_tree):
        """When ML_MODE=BOTH and the first mode (PENTAD) fails, the loop
        breaks and DECAD never runs. Validation must cover only the mode(s)
        actually attempted (PENTAD) -- validating a mode that never ran
        would produce a false validation failure for work that was never
        supposed to happen.
        """
        synth_tree.override(
            "machine_learning",
            (
                'if [ "$script" = "recalculate_nan_forecasts.py" ] '
                '&& [ "$SAPPHIRE_PREDICTION_MODE" = "PENTAD" ]; then exit 1; fi'
            ),
        )
        result = run_main(synth_tree, "machine_learning", extra_env={"ML_MODE": "BOTH"})

        assert result.returncode != 0
        validation_modes = [_mode_of(ln) for ln in self._validation_calls(synth_tree)]
        assert validation_modes == ["PENTAD"]

    def test_both_mode_validation_failure_gets_its_own_log_and_label(self, synth_tree):
        """INFRA-037 defect 1 regression: run_module_validation used to
        derive both its log path and its summary-row label from `$module`
        alone, so calling it once per mode (PENTAD, then DECAD) made the
        second call truncate the first call's log file and reuse the exact
        same row label. With PENTAD failing and DECAD passing, the operator
        got a FAIL row for PENTAD whose "error details" tail showed DECAD's
        successful output, with no way to tell the two rows apart.

        Both modes actually run here (unlike the partial-run test above),
        so validate_pipeline.py is invoked twice under the same module,
        each echoing a distinctive marker and exiting with a different
        code.
        """
        synth_tree.override(
            "postprocessing_forecasts",
            textwrap.dedent(
                """\
                case "$script" in
                    */validate_pipeline.py)
                        if [ "$SAPPHIRE_PREDICTION_MODE" = "PENTAD" ]; then
                            echo "PENTAD_VALIDATION_MARKER"
                            exit 1
                        fi
                        if [ "$SAPPHIRE_PREDICTION_MODE" = "DECAD" ]; then
                            echo "DECAD_VALIDATION_MARKER"
                            exit 0
                        fi
                        ;;
                esac
                """
            ),
        )
        result = run_main(synth_tree, "machine_learning", extra_env={"ML_MODE": "BOTH"})
        out = result.stdout + result.stderr

        assert result.returncode != 0
        # Both modes ran and were validated.
        validation_modes = [_mode_of(ln) for ln in self._validation_calls(synth_tree)]
        assert validation_modes == ["PENTAD", "DECAD"]

        # Each mode gets its own, distinguishable summary row -- not two
        # identically-labelled "api_validation (machine_learning)" rows.
        assert "api_validation (machine_learning PENTAD): FAIL" in out
        assert "api_validation (machine_learning DECAD): PASS" in out

        # The failing row's own error-details tail must show PENTAD's own
        # output, not DECAD's (which would mean DECAD's later call
        # truncated/overwrote PENTAD's log file).
        assert "VALIDATION ERROR DETAILS" in out
        details = out.split("VALIDATION ERROR DETAILS", 1)[1]
        assert "PENTAD_VALIDATION_MARKER" in details
        assert "DECAD_VALIDATION_MARKER" not in details


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
        """This is the central claim of the whole branch: a downgraded
        exit-4 failure must let Phase 3 (run_daily_pipeline's "ML + linear
        regression + postprocessing" loop) actually execute, not just its
        first module. Assert on all three Phase-3 consumers -- a test that
        only checked machine_learning could pass while linear_regression
        and postprocessing_forecasts were silently skipped.
        """
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
        assert any("module=linear_regression" in ln for ln in calls), (
            "linear_regression never ran after the downgraded exit-4 failure -- "
            "Phase 3 did not fully continue"
        )
        assert any("module=postprocessing_forecasts" in ln for ln in calls), (
            "postprocessing_forecasts never ran after the downgraded exit-4 failure -- "
            "Phase 3 did not fully continue"
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
        """Mirror image of test_daily_continues_past_downgraded_failure_and_
        runs_ml: a fatal (exit 5) sub-step failure aborts Phase 2 before
        Phase 3 is ever reached, so NONE of Phase 3's three consumers --
        machine_learning, linear_regression, postprocessing_forecasts --
        may run. A test that only checked machine_learning could pass while
        a regression let linear_regression or postprocessing_forecasts run
        anyway.
        """
        _override_sync_exit_code(synth_tree, 5)
        result = run_main(synth_tree, "daily")

        assert result.returncode != 0
        calls = synth_tree.calls()
        assert not any("module=machine_learning" in ln for ln in calls), (
            "machine_learning ran despite a fatal (exit 5) maintenance failure"
        )
        assert not any("module=linear_regression" in ln for ln in calls), (
            "linear_regression ran despite a fatal (exit 5) maintenance failure -- "
            "Phase 3 should never have been reached"
        )
        assert not any("module=postprocessing_forecasts" in ln for ln in calls), (
            "postprocessing_forecasts ran despite a fatal (exit 5) maintenance failure -- "
            "Phase 3 should never have been reached"
        )

    def test_fatal_failure_records_long_horizon_log_not_maintenance_log(self, synth_tree):
        """Regression: CURRENT_MODULE_LOG used to be restored to the
        maintenance log UNCONDITIONALLY after the lt_rc branches, including
        for fatal sub-step exits (1, 3, 5). record_result was then called
        with the log pointing at the maintenance log, which for this
        scenario holds only the SUCCESSFUL primary maintenance output --
        the sub-step's own output (the thing that actually explains the
        failure) was in the long-horizon log and got left out of MODULE
        ERROR DETAILS.

        For a fatal exit, CURRENT_MODULE_LOG must still be pointed at the
        long-horizon log when record_result runs, so the "MODULE ERROR
        DETAILS" tail shows the sub-step's own output, not the unrelated
        successful primary-step output. The stub's CALL-log line (used
        elsewhere in this file for `synth_tree.calls()`) is written direct
        to a file, not to stdout, so it never reaches CURRENT_MODULE_LOG --
        this test instead makes the sync sub-step echo a distinctive
        marker to its own stdout, which run_in_venv tees into whichever
        file CURRENT_MODULE_LOG points to at that moment.
        """
        synth_tree.override(
            "preprocessing_runoff",
            textwrap.dedent("""\
                if [ "$script" = "preprocessing_runoff.py" ]; then
                    exit 0
                fi
                if [ "$script" = "sync_long_horizon_hydrograph.py" ]; then
                    echo "SYNC_SUBSTEP_DISTINCTIVE_FAILURE_OUTPUT"
                    exit 5
                fi
                """),
        )
        result = run_main(synth_tree, "maintenance:preprocessing_runoff")
        out = result.stdout + result.stderr

        assert result.returncode != 0
        assert "preprocessing_runoff (maintenance): FAIL" in out
        assert "MODULE ERROR DETAILS" in out

        # Isolate the tailed error-detail block so this doesn't just match
        # the marker appearing earlier in stdout when run_in_venv streamed
        # the sub-step's live output to the console.
        details = out.split("MODULE ERROR DETAILS", 1)[1]
        assert "SYNC_SUBSTEP_DISTINCTIVE_FAILURE_OUTPUT" in details, (
            "MODULE ERROR DETAILS should tail the long-horizon log (the "
            "sub-step that actually failed), not the maintenance log -- "
            "got '(no output captured)' or the wrong log's tail instead"
        )
