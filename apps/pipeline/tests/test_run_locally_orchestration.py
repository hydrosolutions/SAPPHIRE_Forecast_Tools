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

    A symlink at ``script_dir/run_locally.sh`` -> the real, unmodified
    ``RUN_LOCALLY_SH`` is included so the continue-on-error hint's printed
    command (which interpolates ``${SCRIPT_DIR}/run_locally.sh``, and
    SCRIPT_DIR is overridden to this synthetic ``script_dir`` for every test
    in this file) is directly executable as a standalone ``bash '<path>'
    ...`` invocation: running it computes SCRIPT_DIR from its own
    (symlinked) invocation path via ``dirname``/``pwd``, which resolves back
    to this same synthetic tree -- reaching these same module stubs -- with
    no sourcing or override needed. See
    TestContinueOnErrorHint.test_hint_command_is_actually_runnable_as_printed.
    """
    script_dir = tmp_path / "synth_apps"
    script_dir.mkdir()
    (script_dir / "run_locally.sh").symlink_to(RUN_LOCALLY_SH)
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
    dry_run: bool = False,
    extra_env: dict[str, str] | None = None,
    cwd: Path | None = None,
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

    `cwd` defaults to APPS_DIR (matching every existing caller). A test that
    needs to prove behaviour of a RELATIVE ieasyhydroforecast_env_file_path
    (both validate_env's own resolution and emit_continue_on_error_hint's
    cwd-independence fix) passes a different `cwd` explicitly.

    `dry_run` passes ``--dry-run``, so validate_env still runs (it precedes
    the dry-run exit) but no module is ever dispatched -- used by
    TestUnvalidatedModeDomains to prove Block 1/2 fire before dispatch, not
    merely before something a passing dispatch would also have blocked.
    """
    log_file = tree.log_dir / "run.log"
    flag = "--continue-on-error " if continue_on_error else ""
    flag += "--dry-run " if dry_run else ""

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
        cwd=str(cwd) if cwd is not None else str(APPS_DIR),
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
        ... run: <assignments... >bash '<SCRIPT_DIR>/run_locally.sh' --continue-on-error '<target>'
    where every interpolated value (env file path, SAPPHIRE_PREDICTION_MODE,
    ML_MODE if set, the script path, the target) is single-quoted by
    shell_quote. Returns everything after "run: ", with the trailing ANSI
    reset code (if any) and surrounding whitespace stripped.
    """
    marker = "stopping at the first, run: "
    for line in out.splitlines():
        if marker in line:
            command = line.split(marker, 1)[1]
            if command.endswith(_ANSI_RESET_SUFFIX):
                command = command[: -len(_ANSI_RESET_SUFFIX)]
            return command.strip()
    raise AssertionError(f"continue-on-error hint command not found in output:\n{out}")


def _extract_env_path_arg(command: str) -> str:
    """Pull the single-quoted value of ieasyhydroforecast_env_file_path=...

    out of an extracted hint `command` string. The env path is always the
    first token (see emit_continue_on_error_hint's prefix construction),
    and none of the paths built in the tests that use this helper contain a
    real single-quote character, so a plain substring search between the
    opening and the next closing quote is exact -- no need to reproduce
    shell_quote's `'\''`-escaping logic here.
    """
    prefix = "ieasyhydroforecast_env_file_path='"
    assert command.startswith(prefix), (
        f"hint command does not start with the env path assignment: {command!r}"
    )
    rest = command[len(prefix) :]
    end = rest.index("'")
    return rest[:end]


def _run_hint_command_verbatim(command: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    """Execute an extracted hint command line EXACTLY as printed.

    Round-5 review finding 1: the previous version of this test claimed to
    execute the printed command but actually parsed the string and
    reconstructed a `main <args>` function call after re-sourcing the
    script with its own overrides -- it never ran the literal text an
    operator would paste, which is exactly why the unquoted-interpolation
    defect survived review. This runs it for real, through a shell, from a
    directory that is NOT the repo root, and does not pre-seed any of the
    variables the command itself interpolates (ieasyhydroforecast_env_file_
    path, SAPPHIRE_PREDICTION_MODE, ML_MODE) -- proving the command is both
    safe (no injected content can run) and self-sufficient (it supplies
    everything validate_env needs on its own).

    `command`'s script path is `${SCRIPT_DIR}/run_locally.sh` as captured
    at hint-emission time -- for every test in this file that is the
    synth_tree fixture's `script_dir/run_locally.sh` symlink (SCRIPT_DIR is
    always overridden to that path before dispatch), so running it reaches
    the same synthetic module stubs the first run used, with no sourcing or
    override needed here.
    """
    env = os.environ.copy()
    for var in _ISOLATE_ENV_VARS:
        env.pop(var, None)
    env.pop("ieasyhydroforecast_env_file_path", None)

    return subprocess.run(
        ["bash", "-c", command],
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )


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
        # Round-5 review finding 1b: the script path must be the absolute
        # SCRIPT_DIR-derived path (cwd-independent), not a literal
        # "apps/run_locally.sh" relative fragment -- and single-quoted
        # (finding 1a).
        assert f"bash '{synth_tree.script_dir}/run_locally.sh' --continue-on-error 'daily'" in out

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
        never executes the suggested command.' Round-5 finding 1 caught a
        REAL defect that finding-3's own fix let through: the previous
        version of this test extracted the printed command, then
        RECONSTRUCTED a `main <args>` call by re-sourcing the script with
        its own overrides -- it never executed the literal text an operator
        would paste, so unquoted interpolation (a space or `;` in the env
        path corrupting the command, or worse, executing) was invisible to
        it.

        This version extracts the literal command and runs it EXACTLY as
        printed through `bash -c` (via _run_hint_command_verbatim), from a
        working directory that is NOT the repo root (proving finding 1b:
        cwd-independence), without pre-seeding ieasyhydroforecast_env_file_
        path/SAPPHIRE_PREDICTION_MODE/ML_MODE in the child environment --
        proving the command is self-sufficient: it supplies everything
        validate_env needs on its own, then genuinely proceeds past the
        module that failed on the first run.
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
        assert f"'{synth_tree.env_file}'" in command, (
            "hint command's env file path does not match the path this run "
            f"actually used: {command!r} vs {synth_tree.env_file}"
        )
        assert f"'{synth_tree.script_dir}/run_locally.sh'" in command, (
            f"hint command's script path is not the expected absolute path: {command!r}"
        )
        assert command.endswith("--continue-on-error 'daily'")

        # Run a directory that is NOT the repo root -- the whole point of
        # finding 1b is that the printed command must not silently depend
        # on the operator's cwd.
        rerun = _run_hint_command_verbatim(command, cwd=synth_tree.script_dir.parent)
        rerun_out = rerun.stdout + rerun.stderr

        # It must NOT die in validate_env for a missing/unset env file --
        # that was the entire round-4 bug: the old hint printed a command
        # with no env var at all, so validate_env failed before
        # --continue-on-error ever had a chance to matter.
        assert "ieasyhydroforecast_env_file_path is not set" not in rerun_out, rerun_out
        assert "Env file not found" not in rerun_out, rerun_out
        assert "No such file or directory" not in rerun_out, rerun_out

        # And it must genuinely proceed past preprocessing_runoff (the
        # module that failed on the first run) into later Phase-3 modules
        # -- proving --continue-on-error took effect, not merely that
        # validation passed.
        calls = synth_tree.calls()
        assert any("module=machine_learning" in ln for ln in calls), (
            "re-running the hint's own printed command did not get past "
            f"the failing module. Output:\n{rerun_out}"
        )

    def test_hint_command_survives_space_and_metacharacter_in_env_path(self, synth_tree):
        """Round-5 review finding 1a: unquoted interpolation of
        ieasyhydroforecast_env_file_path is a real hazard, not a
        theoretical one -- a space breaks the printed command (exit 127),
        and `;`/backticks/$(...) in the path would EXECUTE that content
        when an operator pastes the printed line into a shell, since the
        hint is printed specifically to be pasted.

        Builds the synthetic env file at a path containing a space, a `;`,
        AND a `$(...)` substring all at once, then proves the printed
        command still executes correctly (reaches machine_learning) and
        that the injected `touch` never ran. The injected command uses a
        bare (path-free) filename -- a directory NAME cannot itself contain
        "/", and every rerun in this test uses the same cwd
        (synth_tree.script_dir.parent), so the marker's location is
        unambiguous regardless of where in the weird name it fires from.
        """
        weird_dir = synth_tree.script_dir.parent / "op env; $(touch INJECTED_BY_UNQUOTED_HINT)"
        weird_dir.mkdir()
        weird_env_file = weird_dir / "test.env"
        weird_env_file.write_text(synth_tree.env_file.read_text())
        marker = synth_tree.script_dir.parent / "INJECTED_BY_UNQUOTED_HINT"

        synth_tree.override(
            "preprocessing_runoff",
            'if [ "$script" = "preprocessing_runoff.py" ]; then exit 1; fi',
        )
        first = run_main(
            synth_tree,
            "daily",
            extra_env={"ieasyhydroforecast_env_file_path": str(weird_env_file)},
        )
        out = first.stdout + first.stderr
        assert first.returncode != 0
        assert not marker.exists(), (
            "the injected touch ran while emitting/logging the hint itself, "
            "before the command was ever re-executed"
        )

        command = _extract_hint_command(out)
        assert f"'{weird_env_file}'" in command, (
            f"env path with space/metacharacters not safely quoted in: {command!r}"
        )

        rerun = _run_hint_command_verbatim(command, cwd=synth_tree.script_dir.parent)
        rerun_out = rerun.stdout + rerun.stderr

        assert not marker.exists(), (
            "shell metacharacter/command-substitution in the env path "
            f"executed when the printed hint command was run verbatim. Output:\n{rerun_out}"
        )
        assert "Env file not found" not in rerun_out, rerun_out
        calls = synth_tree.calls()
        assert any("module=machine_learning" in ln for ln in calls), (
            "re-running the hint command with a space/metacharacter env "
            f"path did not get past the failing module. Output:\n{rerun_out}"
        )

    def test_hint_command_survives_backslash_x27_escape_in_env_path(self, synth_tree):
        """Round-6 review finding 1: shell_quote is correct in isolation, but
        emit_continue_on_error_hint emits its line via `log WARN`, and
        log() rendered with `echo -e`, which reinterprets backslash escape
        sequences IN THE MESSAGE -- undoing shell_quote's protection. If the
        env path's text contains the literal 4-character sequence `\\x27`,
        echo -e's hex-escape interpretation turns it into a real apostrophe
        BEFORE the line is even printed, closing shell_quote's single-quote
        wrapping early. Everything the attacker placed after that point in
        the path text then becomes live, unquoted shell text once pasted.

        Reproduced directly: pre-fix, pasting the printed line for this
        exact path runs `touch <marker>`. This test must FAIL against the
        pre-fix code (verified before applying the fix) and pass after.
        """
        marker_name = "INJECTED_BY_BACKSLASH_X27_ESCAPE"
        # Directory name literally contains `\x27` followed by a
        # semicolon/command/comment -- echo -e's hex-escape interpretation
        # of \x27 turns it into a real single quote, closing shell_quote's
        # quoting early and exposing the rest as live shell text (the `;`
        # separates commands, `touch <marker>` runs, `;#` comments out the
        # remainder of the line).
        weird_dir = synth_tree.script_dir.parent / ("op" + "\\x27;touch " + marker_name + ";#")
        weird_dir.mkdir()
        weird_env_file = weird_dir / "test.env"
        weird_env_file.write_text(synth_tree.env_file.read_text())
        marker = synth_tree.script_dir.parent / marker_name

        synth_tree.override(
            "preprocessing_runoff",
            'if [ "$script" = "preprocessing_runoff.py" ]; then exit 1; fi',
        )
        first = run_main(
            synth_tree,
            "daily",
            extra_env={"ieasyhydroforecast_env_file_path": str(weird_env_file)},
        )
        out = first.stdout + first.stderr
        assert first.returncode != 0
        assert not marker.exists(), (
            "the injected touch ran while emitting/logging the hint itself, "
            "before the command was ever re-executed"
        )

        command = _extract_hint_command(out)
        env_path_in_command = _extract_env_path_arg(command)
        assert env_path_in_command == str(weird_env_file), (
            "the \\x27 escape sequence in the env path was reinterpreted "
            f"while rendering the log line, corrupting the printed path: {command!r}"
        )
        assert command.endswith("--continue-on-error 'daily'"), (
            f"hint command was truncated or corrupted: {command!r}"
        )

        rerun = _run_hint_command_verbatim(command, cwd=synth_tree.script_dir.parent)
        rerun_out = rerun.stdout + rerun.stderr

        assert not marker.exists(), (
            "the \\x27 escape sequence in the env path closed shell_quote's "
            f"quoting early, executing injected content when the printed hint "
            f"command was run verbatim. Output:\n{rerun_out}"
        )
        assert "Env file not found" not in rerun_out, rerun_out
        calls = synth_tree.calls()
        assert any("module=machine_learning" in ln for ln in calls), (
            "re-running the hint command with a \\x27 escape sequence in the "
            f"env path did not get past the failing module. Output:\n{rerun_out}"
        )

    def test_hint_command_survives_backslash_c_escape_in_env_path(self, synth_tree):
        """Round-6 review finding 1 (companion case): `\\c` is echo -e's
        'stop output here, no trailing newline' escape. If the env path
        contains it, pre-fix log() truncates the ENTIRE hint line -- not
        just the path -- dropping the `--continue-on-error '<target>'` tail
        (and even the newline, so the next log() call's text gets glued
        onto the same line). The operator would paste a syntactically
        broken/incomplete command rather than something merely wrong.

        This test must FAIL against the pre-fix code (verified before
        applying the fix) and pass after.
        """
        weird_dir = synth_tree.script_dir.parent / "op\\ctruncation_test"
        weird_dir.mkdir()
        weird_env_file = weird_dir / "test.env"
        weird_env_file.write_text(synth_tree.env_file.read_text())

        synth_tree.override(
            "preprocessing_runoff",
            'if [ "$script" = "preprocessing_runoff.py" ]; then exit 1; fi',
        )
        first = run_main(
            synth_tree,
            "daily",
            extra_env={"ieasyhydroforecast_env_file_path": str(weird_env_file)},
        )
        out = first.stdout + first.stderr
        assert first.returncode != 0

        command = _extract_hint_command(out)
        env_path_in_command = _extract_env_path_arg(command)
        assert env_path_in_command == str(weird_env_file), (
            f"the \\c escape sequence in the env path corrupted the rendered log line: {command!r}"
        )
        assert command.endswith("--continue-on-error 'daily'"), (
            f"hint command was truncated by a \\c escape sequence: {command!r}"
        )

        rerun = _run_hint_command_verbatim(command, cwd=synth_tree.script_dir.parent)
        rerun_out = rerun.stdout + rerun.stderr
        assert "Env file not found" not in rerun_out, rerun_out
        calls = synth_tree.calls()
        assert any("module=machine_learning" in ln for ln in calls), (
            "re-running the hint command with a \\c escape sequence in the "
            f"env path did not get past the failing module. Output:\n{rerun_out}"
        )

    def test_hint_command_resolves_relative_env_path_to_absolute(self, synth_tree, tmp_path):
        """Round-6 review 'ALSO': validate_env accepts a RELATIVE
        ieasyhydroforecast_env_file_path (it only checks `-f`, which
        resolves fine against the run's own cwd) but the hint is printed
        specifically to be pasted into a shell session later -- possibly
        from a different cwd, the same class of defect already fixed for
        the script path via SCRIPT_DIR (round-5 finding 1b).
        emit_continue_on_error_hint must resolve a relative env path to
        absolute before interpolating it. This does not change what
        validate_env itself accepts.
        """
        operator_cwd = tmp_path / "operator_cwd"
        (operator_cwd / "envs").mkdir(parents=True)
        relative_env_file = operator_cwd / "envs" / "test.env"
        relative_env_file.write_text(synth_tree.env_file.read_text())
        relative_env_path = "envs/test.env"

        synth_tree.override(
            "preprocessing_runoff",
            'if [ "$script" = "preprocessing_runoff.py" ]; then exit 1; fi',
        )
        first = run_main(
            synth_tree,
            "daily",
            extra_env={"ieasyhydroforecast_env_file_path": relative_env_path},
            cwd=operator_cwd,
        )
        out = first.stdout + first.stderr
        assert first.returncode != 0
        assert "Env file not found" not in out, (
            f"validate_env rejected the relative env path from its own cwd: {out}"
        )

        command = _extract_hint_command(out)
        env_path_in_command = _extract_env_path_arg(command)
        assert env_path_in_command.startswith("/"), (
            "hint command still interpolates the RELATIVE env path verbatim "
            f"instead of resolving it to absolute: {command!r}"
        )
        assert env_path_in_command != relative_env_path

        # Run the printed command from a cwd that is NOT the one the
        # original run used -- proving the absolute path the hint printed
        # survives the cwd change, unlike the relative input that produced it.
        rerun = _run_hint_command_verbatim(command, cwd=synth_tree.script_dir.parent)
        rerun_out = rerun.stdout + rerun.stderr
        assert "ieasyhydroforecast_env_file_path is not set" not in rerun_out, rerun_out
        assert "Env file not found" not in rerun_out, rerun_out
        calls = synth_tree.calls()
        assert any("module=machine_learning" in ln for ln in calls), (
            "re-running the hint command (built from a relative env path) "
            f"from a different cwd did not get past the failing module. "
            f"Output:\n{rerun_out}"
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

        Round-5 review finding 2: the module-name-only assertions below
        would ALSO pass if Phase 3 were skipped entirely and only Phase 4
        (maintenance, ~line 1507) ran -- run_daily_pipeline's Phase 4 calls
        the exact same module names (machine_learning, linear_regression,
        postprocessing_forecasts) via run_maintenance_machine_learning /
        run_maintenance_linear_regression / run_maintenance_postprocessing_
        forecasts, and this harness's stub logs only the module name, not
        which function invoked it. So the module-name assertions are kept
        (still true, still worth checking) but are no longer load-bearing
        on their own -- the two assertions below key on what actually
        differs between the phases:
          - postprocessing_forecasts: Phase 3 runs postprocessing_
            operational.py; Phase 4 runs postprocessing_maintenance.py.
            Different script name.
          - linear_regression: Phase 3 (run_linear_regression) invokes it
            with no extra args; Phase 4 (run_maintenance_linear_regression)
            passes `-- --hindcast`, which run_in_venv turns into a real
            `--hindcast` script argument. The stub logs args=<script args>,
            so Phase 3's call line has an empty args field and Phase 4's
            has "args=--hindcast" -- different, and observable.
        Neither module ever appears with these Phase-3 signatures unless
        Phase 3 itself actually ran.
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
        # Phase-3-specific discriminators (see docstring): these can only be
        # true if the OPERATIONAL calls actually ran, not just Phase 4's
        # maintenance calls under the same module names.
        assert any(
            "module=postprocessing_forecasts" in ln and "script=postprocessing_operational.py" in ln
            for ln in calls
        ), (
            "Phase 3's operational postprocessing (postprocessing_operational.py) never ran -- "
            "only maintenance's postprocessing_maintenance.py appeared, meaning Phase 3 "
            "was skipped and only Phase 4 executed"
        )
        assert any("module=linear_regression" in ln and "args= mode=" in ln for ln in calls), (
            "Phase 3's operational linear_regression call (no --hindcast argument) never "
            "ran -- only the maintenance call (args=--hindcast) appeared, meaning Phase 3 "
            "was skipped and only Phase 4 executed"
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

    def test_exit_four_error_points_to_run_summary_and_its_log(self, synth_tree):
        """Peer review (INFRA-037): exit 4 covers both "a few of many
        stations failed" (benign) and "every station failed" (total
        outage), and the original ERROR line here was identical for both,
        with no counts and no pointer. The real counts already live in the
        sub-step's own LONG-HORIZON RUN SUMMARY print (see
        sync_long_horizon_hydrograph.py, which always survives
        print_error_details' tail -- it prints last, right before the
        sub-step's own final `sys.exit(4)` logger.error line, and nothing
        the sub-step emits afterward can push it out of the tail window).
        This test does not (and cannot, at the shell level -- see the
        module docstring's NOTE on scope) fabricate that block itself,
        since the stub here is a bash exit-code substitute, not the real
        Python script. It only pins that run_locally.sh's own ERROR line
        now names the block and points at where it landed, instead of
        silently repeating the same undifferentiated message.
        """
        _override_sync_exit_code(synth_tree, 4)
        result = run_main(synth_tree, "maintenance:preprocessing_runoff")
        out = result.stdout + result.stderr

        assert result.returncode != 0
        assert "Long-horizon hydrograph sync had SDK norm lookup failure(s)" in out
        assert "LONG-HORIZON RUN SUMMARY" in out, (
            "the exit-4 ERROR line should name the LONG-HORIZON RUN SUMMARY "
            "block so the operator knows where the counts live"
        )
        assert "preprocessing_runoff_long_horizon.log" in out, (
            "the exit-4 ERROR line should point at the sub-step's own log "
            "file, not just repeat the generic message"
        )
        assert "preprocessing_runoff (long-horizon sync)" in out, (
            "the pointer should name the summary row the sub-step's own log is recorded on"
        )


# ---------------------------------------------------------------------------
# Group D -- run_module_validation's non-ML single-module call sites
# ---------------------------------------------------------------------------
#
# run_module_validation's `run_in_venv ... || rc=$?` fix (see its docstring)
# exists because every call site in main()'s dispatch invokes it as a bare
# statement under `set -euo pipefail`. Before the fix, a validate_pipeline.py
# failure at ANY of these call sites -- not just machine_learning's -- would
# have tripped `set -e` and killed run_locally.sh outright, before
# record_validation ever ran, defeating "don't abort pipeline mid-run;
# failures surface in summary". Call sites found in main()'s single-module
# case branches (apps/run_locally.sh):
#   preprocessing_runoff)       run_module_validation "preprocessing_runoff"
#   preprocessing_gateway)      run_module_validation "preprocessing_gateway"
#   linear_regression)          run_module_validation "linear_regression"
#   machine_learning)           run_module_validation "machine_learning" "$mode"  (already covered above)
#   postprocessing_forecasts)   run_module_validation "postprocessing_forecasts"
#   long_term_forecasting)      run_module_validation "long_term_forecasting"
# Two non-ML targets are covered below: preprocessing_runoff and
# linear_regression.


def _fail_validation_for_module(tree: SynthTree, module: str) -> None:
    """Make validate_pipeline.py (run via run_module_validation's
    postprocessing_forecasts stub, regardless of which module is being
    validated) exit 1 only when invoked with `--module <module>`, and
    succeed for every other invocation (e.g. the module's own script,
    or validation of a different module).
    """
    tree.override(
        "postprocessing_forecasts",
        textwrap.dedent(f"""\
            case "$script" in
                */validate_pipeline.py)
                    if [ "$2" = "{module}" ]; then
                        exit 1
                    fi
                    ;;
            esac
            """),
    )


class TestRunModuleValidationNonMLCallSites:
    """Pin the "don't abort mid-run" contract for run_module_validation's
    non-ML single-module call sites, mirroring the ML coverage in
    TestMachineLearningBareTargetModes above (its
    test_validation_uses_the_mode_ml_actually_ran_under and friends).
    """

    def test_preprocessing_runoff_validation_failure_does_not_abort(self, synth_tree):
        _fail_validation_for_module(synth_tree, "preprocessing_runoff")
        result = run_main(synth_tree, "preprocessing_runoff")
        out = result.stdout + result.stderr

        # (a) did not die before recording -- the module's own PASS row and
        # the full summary (including its validation section) both printed,
        # which could not happen if set -e had killed the script inside
        # run_module_validation before record_validation ran.
        assert "preprocessing_runoff: PASS" in out, (
            "the module's own run should have succeeded and been recorded "
            "-- its absence means the script died before reaching here"
        )
        assert "PIPELINE SUMMARY" in out
        assert "API VALIDATION SUMMARY" in out
        # (b) recorded a validation FAIL row.
        assert "api_validation (preprocessing_runoff): FAIL" in out
        # (c) printed the summary -- VALIDATION ERROR DETAILS is only
        # reached if print_summary ran to completion past record_validation.
        assert "VALIDATION ERROR DETAILS" in out
        # (d) exits non-zero.
        assert result.returncode != 0

    def test_linear_regression_validation_failure_does_not_abort(self, synth_tree):
        _fail_validation_for_module(synth_tree, "linear_regression")
        result = run_main(synth_tree, "linear_regression")
        out = result.stdout + result.stderr

        assert "linear_regression: PASS" in out, (
            "the module's own run should have succeeded and been recorded "
            "-- its absence means the script died before reaching here"
        )
        assert "PIPELINE SUMMARY" in out
        assert "API VALIDATION SUMMARY" in out
        assert "api_validation (linear_regression): FAIL" in out
        assert "VALIDATION ERROR DETAILS" in out
        assert result.returncode != 0


# ---------------------------------------------------------------------------
# Group E -- INFRA-039: unvalidated SAPPHIRE_PREDICTION_MODE / ML_MODE
# ---------------------------------------------------------------------------
#
# validate_env used to log SAPPHIRE_PREDICTION_MODE but never check its
# domain, and never checked ML_MODE at all. Both variables have consumers
# that accept an out-of-domain value SILENTLY: linear_regression.py
# disables both horizons and exits 0 on a mode it doesn't recognise, and
# should_skip_ml_for_mode does a plain string compare against ML_MODE, so
# an invalid ML_MODE filters every mode with only an INFO line. Two new,
# deliberately narrow case blocks in validate_env close that for the
# targets where it is otherwise silent -- see
# doc/plans/issues/mid_prio_gi_draft_infra_run_locally_unvalidated_modes.md.
#
# Both blocks are additive: they use validate_env's existing `errors`
# counter (never `exit` directly), so a run with two bad variables still
# reports both, and neither block touches resolve_ml_bare_target_modes,
# should_skip_ml_for_mode, or any run_* pipeline function.


class TestPredictionModeDomainBlock1:
    """Block 1: SAPPHIRE_PREDICTION_MODE, domain PENTAD|DECAD|BOTH.

    Scoped to the targets that dispatch linear_regression or
    machine_learning -- the two consumers Failure A in the issue documents
    as silent on an out-of-domain value. `daily` is deliberately excluded
    (it overwrites the variable itself before dispatch); see
    TestModeDomainRegressionGuards for the targets that must NOT be
    touched by this block.
    """

    BLOCK_1_TARGETS = [
        "short-term",
        "all",
        "maintenance",
        "linear_regression",
        "maintenance:linear_regression",
        "maintenance:machine_learning",
    ]

    @pytest.mark.parametrize("target", BLOCK_1_TARGETS)
    @pytest.mark.parametrize("bad_mode", ["ALL", "PENTAAD"])
    def test_out_of_domain_mode_rejected_before_any_module_runs(self, synth_tree, target, bad_mode):
        """`ALL` is a real mode elsewhere (Failure A's dangerous case, not a
        typo) and `PENTAAD` is a plain typo -- both must be rejected the
        same way for every target that dispatches LR or ML.
        """
        result = run_main(synth_tree, target, extra_env={"SAPPHIRE_PREDICTION_MODE": bad_mode})
        out = result.stdout + result.stderr

        assert result.returncode != 0, out
        assert "SAPPHIRE_PREDICTION_MODE" in out, out
        assert bad_mode in out, out
        assert not synth_tree.calls(), (
            f"target {target!r} invoked a module despite an out-of-domain "
            f"SAPPHIRE_PREDICTION_MODE={bad_mode!r}: {synth_tree.calls()}"
        )


class TestBlock1MaintenanceMlOrgGate:
    """F1 fix: out-of-loop review found `maintenance:machine_learning` was
    the one Block 1 target that dispatches ONLY machine_learning, not
    linear_regression -- so for demo/uzhm (which skip machine_learning
    entirely, DEMO_SKIP_MODULES/UZHM_SKIP_MODULES) that target already
    no-ops today, and an ungated Block 1 newly rejected a currently-working
    invocation. The fix gates only this one Block 1 arm on
    `! should_skip_module machine_learning`; every other Block 1 target
    stays ungated because it still dispatches linear_regression, which is
    not org-skippable.
    """

    @pytest.mark.parametrize("org", ["demo", "uzhm"])
    def test_out_of_domain_mode_on_maintenance_ml_still_succeeds_for_orgs_that_skip_ml(
        self, synth_tree, org
    ):
        """The F1 guard itself: before the fix, this exited 1 for demo and
        uzhm even though `maintenance:machine_learning` does nothing for
        those orgs regardless of SAPPHIRE_PREDICTION_MODE.
        """
        result = run_main(
            synth_tree,
            "maintenance:machine_learning",
            extra_env={"SAPPHIRE_PREDICTION_MODE": "ALL", "ieasyhydroforecast_organization": org},
        )
        out = result.stdout + result.stderr

        assert result.returncode == 0, out
        assert "is not valid" not in out, out
        assert not synth_tree.calls(), (
            f"org={org!r} should skip machine_learning entirely: {synth_tree.calls()}"
        )

    def test_out_of_domain_mode_on_maintenance_ml_still_rejected_for_a_normal_org(self, synth_tree):
        """Complement of the guard above: the gate must not have swallowed
        the whole Block 1 arm for `maintenance:machine_learning` -- an org
        that does NOT skip machine_learning (the default test org) must
        still be rejected. Without this test, a fix that gated the entire
        Block 1 case (not just this one target) on
        `! should_skip_module machine_learning` would also pass.
        """
        result = run_main(
            synth_tree,
            "maintenance:machine_learning",
            extra_env={"SAPPHIRE_PREDICTION_MODE": "ALL"},
        )
        out = result.stdout + result.stderr

        assert result.returncode != 0, out
        assert "SAPPHIRE_PREDICTION_MODE" in out, out
        assert not synth_tree.calls()

    @pytest.mark.parametrize("org", ["demo", "uzhm"])
    @pytest.mark.parametrize(
        "target",
        [
            "short-term",
            "all",
            "maintenance",
            "linear_regression",
            "maintenance:linear_regression",
        ],
    )
    def test_out_of_domain_mode_on_lr_bearing_targets_still_rejected_for_ml_skipping_orgs(
        self, synth_tree, target, org
    ):
        """The other half of the complement: all five LR-bearing Block 1
        targets must stay REJECTED for demo/uzhm too, since they all still
        dispatch linear_regression regardless of the org's ML-skip list.
        This is what rules out the wrong fix of gating the whole Block 1
        case on `! should_skip_module machine_learning` -- that would have
        made demo/uzhm silently pass here as well. Covers all five targets
        in Block 1's ungated arm (run_locally.sh's
        `short-term|all|maintenance|linear_regression|maintenance:linear_regression`
        case), not just three of them -- otherwise a later change that
        gated `all` or `maintenance` on the ML org skip would make demo/uzhm
        silently accept SAPPHIRE_PREDICTION_MODE=ALL there too, and every
        test would still pass.
        """
        result = run_main(
            synth_tree,
            target,
            extra_env={"SAPPHIRE_PREDICTION_MODE": "ALL", "ieasyhydroforecast_organization": org},
        )
        out = result.stdout + result.stderr

        assert result.returncode != 0, out
        assert "SAPPHIRE_PREDICTION_MODE" in out, out
        assert not synth_tree.calls()


class TestMlModeDomainBlock2:
    """Block 2: ML_MODE, domain PENTAD|DECAD|BOTH.

    Scoped to targets that dispatch machine_learning through the outer
    mode loops (`daily` included here, unlike Block 1 -- it is vulnerable
    to Failure B, not Failure A). Excludes linear_regression-only targets
    and recalculate_skill_metrics, which never dispatch ML.
    """

    BLOCK_2_TARGETS = [
        "daily",
        "short-term",
        "all",
        "maintenance",
        "maintenance:machine_learning",
    ]

    @pytest.mark.parametrize("target", BLOCK_2_TARGETS)
    def test_invalid_ml_mode_rejected_before_any_module_runs(self, synth_tree, target):
        result = run_main(synth_tree, target, extra_env={"ML_MODE": "DEACD"})
        out = result.stdout + result.stderr

        assert result.returncode != 0, out
        assert "ML_MODE" in out, out
        assert "DEACD" in out, out
        assert not synth_tree.calls(), (
            f"target {target!r} invoked a module despite ML_MODE=DEACD: {synth_tree.calls()}"
        )


class TestModeDomainValidationUnderDryRun:
    """Acceptance criterion 4: both blocks fire under --dry-run, since
    validate_env runs before the dry-run exit (main():~2139-2143). Tested
    directly rather than only asserted in prose -- a regression that moved
    validate_env after the dry-run check would pass every other test in
    this file (none of them use --dry-run) while silently making --dry-run
    useless for catching these two defects.
    """

    def test_block_1_fires_under_dry_run(self, synth_tree):
        result = run_main(
            synth_tree,
            "short-term",
            dry_run=True,
            extra_env={"SAPPHIRE_PREDICTION_MODE": "ALL"},
        )
        out = result.stdout + result.stderr

        assert result.returncode != 0, out
        assert "SAPPHIRE_PREDICTION_MODE" in out, out
        assert "ALL" in out, out
        assert not synth_tree.calls()

    def test_block_2_fires_under_dry_run(self, synth_tree):
        result = run_main(synth_tree, "short-term", dry_run=True, extra_env={"ML_MODE": "DEACD"})
        out = result.stdout + result.stderr

        assert result.returncode != 0, out
        assert "ML_MODE" in out, out
        assert "DEACD" in out, out
        assert not synth_tree.calls()

    def test_dry_run_still_passes_for_a_valid_mode(self, synth_tree):
        """Sanity baseline for the two failing-dry-run tests above: a
        --dry-run with in-domain values reaches the "Dry run complete"
        message and exits 0, so the failures asserted above are actually
        caused by the bad values, not by --dry-run itself always failing.
        """
        result = run_main(
            synth_tree,
            "short-term",
            dry_run=True,
            extra_env={"SAPPHIRE_PREDICTION_MODE": "PENTAD", "ML_MODE": "DECAD"},
        )
        out = result.stdout + result.stderr

        assert result.returncode == 0, out
        assert "Dry run complete" in out
        assert not synth_tree.calls()


class TestModeDomainRegressionGuards:
    """Acceptance criterion 3 -- the load-bearing half of this change.

    Each test here maps to a live usage or a stated exclusion in the issue
    and must keep succeeding after Block 1 / Block 2 are added. A first
    draft of this plan used a single global whitelist for
    SAPPHIRE_PREDICTION_MODE and would have failed the recalculate_skill_
    metrics tests below (VALID_MODES there includes ALL/MONTHLY/SEASONAL).
    """

    @pytest.mark.parametrize("mode", ["ALL", "MONTHLY", "SEASONAL"])
    def test_recalculate_skill_metrics_keeps_accepting_its_full_domain(self, synth_tree, mode):
        """recalculate_skill_metrics.py's own VALID_MODES accepts eight
        modes (PENTAD, DECAD, BOTH, MONTHLY, ALL, DAILY, QUARTERLY,
        SEASONAL) and already exits 1 on its own; Block 1 deliberately
        excludes this target so as not to duplicate (and inevitably drift
        from) that list in bash.
        """
        result = run_main(
            synth_tree,
            "recalculate_skill_metrics",
            extra_env={"SAPPHIRE_PREDICTION_MODE": mode},
        )
        out = result.stdout + result.stderr

        assert result.returncode == 0, out
        assert any("module=postprocessing_forecasts" in ln for ln in synth_tree.calls())

    def test_unrecognised_mode_on_daily_still_runs(self, synth_tree):
        """`daily` overwrites SAPPHIRE_PREDICTION_MODE itself in Phases 3-4
        before any module runs under it, so a stale/invalid value the
        operator happened to have exported must not newly break `daily`.
        """
        result = run_main(synth_tree, "daily", extra_env={"SAPPHIRE_PREDICTION_MODE": "PENTAAD"})
        out = result.stdout + result.stderr

        assert result.returncode == 0, out
        assert any("module=preprocessing_runoff" in ln for ln in synth_tree.calls())
        assert any("module=machine_learning" in ln for ln in synth_tree.calls())

    def test_unrecognised_mode_on_initialize_still_runs(self, synth_tree):
        """F2 (documented, not fixed): `initialize` dispatches
        linear_regression (run_initialize_deployment) but is absent from
        Block 1. This is harmless, not an oversight to close here --
        run_initialize_deployment forces SAPPHIRE_PREDICTION_MODE=$mode
        with PENTAD then DECAD on every linear_regression/skill-metrics
        call it makes, so an operator's stale/invalid value never reaches
        those consumers. Same rationale as the `daily` exclusion. Adding
        `initialize` to Block 1 would newly reject a currently-harmless
        invocation, so it stays out.
        """
        result = run_main(
            synth_tree, "initialize", extra_env={"SAPPHIRE_PREDICTION_MODE": "PENTAAD"}
        )
        out = result.stdout + result.stderr

        assert result.returncode == 0, out
        assert "is not valid" not in out, out
        assert any("module=linear_regression" in ln for ln in synth_tree.calls())

    def test_unrecognised_mode_on_long_term_still_runs(self, synth_tree):
        """`long-term` doesn't depend on SAPPHIRE_PREDICTION_MODE at all
        (see validate_env's untouched WARN case), so neither block applies.
        """
        result = run_main(
            synth_tree, "long-term", extra_env={"SAPPHIRE_PREDICTION_MODE": "PENTAAD"}
        )
        out = result.stdout + result.stderr

        assert result.returncode == 0, out
        assert any("module=long_term_forecasting" in ln for ln in synth_tree.calls())

    @pytest.mark.parametrize(
        "target",
        [
            "long-term",
            "recalculate_skill_metrics",
            "maintenance:linear_regression",
            "linear_regression",
        ],
    )
    def test_invalid_ml_mode_does_not_block_targets_that_never_dispatch_ml(
        self, synth_tree, target
    ):
        """None of these four targets are in Block 2's list -- long-term
        and recalculate_skill_metrics never touch machine_learning at all,
        and the two linear_regression targets dispatch LR only.
        """
        result = run_main(synth_tree, target, extra_env={"ML_MODE": "DEACD"})
        out = result.stdout + result.stderr

        assert result.returncode == 0, out
        assert "is not valid" not in out, out
        assert synth_tree.calls(), f"target {target!r} invoked no module at all"

    @pytest.mark.parametrize("org", ["demo", "uzhm"])
    def test_invalid_ml_mode_on_daily_does_not_block_orgs_that_skip_ml(self, synth_tree, org):
        """Block 2 is gated on `! should_skip_module machine_learning`
        specifically so demo/uzhm orgs -- which skip machine_learning
        entirely (DEMO_SKIP_MODULES / UZHM_SKIP_MODULES) -- are unaffected
        by an ML_MODE value that is irrelevant to them today. An ungated
        Block 2 would newly reject a `daily` run that works in production
        for both orgs.
        """
        result = run_main(
            synth_tree,
            "daily",
            extra_env={"ML_MODE": "DEACD", "ieasyhydroforecast_organization": org},
        )
        out = result.stdout + result.stderr

        assert result.returncode == 0, out
        assert "is not valid" not in out, out
        assert not any("module=machine_learning" in ln for ln in synth_tree.calls()), (
            f"machine_learning ran for org={org!r}, which should skip it entirely"
        )
        assert any("module=preprocessing_runoff" in ln for ln in synth_tree.calls())

    def test_unset_prediction_mode_on_short_term_still_warns_and_defaults(self, synth_tree):
        """The pre-existing unset-mode WARN/OK case in validate_env (the
        block above the two new ones) must keep firing for its full
        current target list -- Block 1's narrower list must not have
        replaced it.
        """
        result = run_main(synth_tree, "short-term")
        out = result.stdout + result.stderr

        assert result.returncode == 0, out
        assert "SAPPHIRE_PREDICTION_MODE not set (will default to PENTAD)" in out
        assert synth_tree.calls()

    def test_unset_prediction_mode_on_recalculate_skill_metrics_still_warns(self, synth_tree):
        """recalculate_skill_metrics is excluded from Block 1 (its consumer
        validates its own, larger domain) but must still get the existing
        unset-mode WARN -- the narrowing must not have dropped it from
        that older, separate case block.
        """
        result = run_main(synth_tree, "recalculate_skill_metrics")
        out = result.stdout + result.stderr

        assert result.returncode == 0, out
        assert "SAPPHIRE_PREDICTION_MODE not set (will default to PENTAD)" in out

    def test_unset_prediction_mode_on_bare_linear_regression_still_runs(self, synth_tree):
        """Bare `linear_regression` has no outer mode loop, so with
        SAPPHIRE_PREDICTION_MODE unset it must still run and forward an
        empty mode to the module. This uses a stub, so it only proves that
        run_locally.sh does not invent a value and forwards the mode
        unchanged (empty) -- it does NOT exercise linear_regression.py's
        own default resolution. The module-side fallback that turns an
        empty mode into BOTH is `prediction_mode = os.getenv(
        "SAPPHIRE_PREDICTION_MODE", "") or "BOTH"` at linear_regression.py:634;
        that line, not this test, is what keeps the default BOTH rather than
        PENTAD or an invalid value.
        """
        result = run_main(synth_tree, "linear_regression")
        out = result.stdout + result.stderr

        assert result.returncode == 0, out
        calls = [ln for ln in synth_tree.calls() if "module=linear_regression" in ln]
        assert calls, "linear_regression stub was never invoked"
        assert [_mode_of(ln) for ln in calls] == [""]
