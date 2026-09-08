"""Pytest fixtures for pipeline tests.

This module provides common fixtures for testing pipeline_docker.py functionality,
particularly marker file operations and gateway dependency resolution.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Add the repository root to the path for pipeline_docker import.
# pipeline_docker.py uses imports like "from apps.pipeline.src import ..."
# which requires the repository root (parent of apps/) to be in sys.path.
# Also add the pipeline directory itself for direct module imports.
_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_pipeline_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _repo_root)
sys.path.insert(0, _pipeline_dir)

# Set up minimal environment variables required by pipeline_docker before importing.
# pipeline_docker.py evaluates environment variables at module import time,
# so these must be set before the import happens.
# These are test-specific values that won't affect production.
_test_tmp = "/tmp/pipeline_tests"
os.makedirs(_test_tmp, exist_ok=True)
os.makedirs(f"{_test_tmp}/marker_files", exist_ok=True)

os.environ.setdefault("ieasyhydroforecast_env_file_path", "")
os.environ.setdefault("ieasyhydroforecast_backend_docker_image_tag", "py312")
os.environ.setdefault("ieasyhydroforecast_organization", "demo")
os.environ.setdefault("SAPPHIRE_DG_HOST", "http://localhost:8000")
os.environ.setdefault("ieasyhydroforecast_run_ML_models", "False")
os.environ.setdefault("ieasyhydroforecast_run_CM_models", "False")
os.environ.setdefault("ieasyforecast_intermediate_data_path", _test_tmp)
os.environ.setdefault("ieasyforecast_configuration_path", _test_tmp)
os.environ.setdefault("ieasyhydroforecast_OUTPUT_PATH_DG", "gateway_output")
os.environ.setdefault("ieasyhydroforecast_available_ML_models", "TFT,TIDE")


@pytest.fixture
def temp_marker_dir(tmp_path):
    """Create a temporary directory for marker files.

    Args:
        tmp_path: pytest's built-in tmp_path fixture

    Returns:
        Path to the temporary marker directory
    """
    marker_dir = tmp_path / "marker_files"
    marker_dir.mkdir()
    return marker_dir


@pytest.fixture
def mock_env(temp_marker_dir, monkeypatch):
    """Mock environment with temp marker directory.

    This fixture patches the MARKER_DIR constant in pipeline_docker
    to use a temporary directory, allowing tests to run in isolation
    without affecting real marker files.

    Args:
        temp_marker_dir: Temporary directory for marker files
        monkeypatch: pytest's monkeypatch fixture

    Returns:
        Dictionary with marker_dir path for test reference
    """
    # Import the module to patch (using relative import from sys.path)
    import pipeline_docker

    # Store the original value
    original_marker_dir = pipeline_docker.MARKER_DIR

    # Patch the MARKER_DIR constant
    monkeypatch.setattr(pipeline_docker, "MARKER_DIR", str(temp_marker_dir))

    yield {"marker_dir": temp_marker_dir, "original_marker_dir": original_marker_dir}

    # monkeypatch automatically restores the original value


@pytest.fixture
def clean_marker_dir(mock_env):
    """Ensure marker directory is clean before each test.

    Args:
        mock_env: The mock environment fixture

    Returns:
        Path to the clean marker directory
    """
    marker_dir = mock_env["marker_dir"]
    # Clean any existing marker files
    for f in marker_dir.glob("*.marker"):
        f.unlink()
    return marker_dir


@pytest.fixture
def tmp_timeout_config(tmp_path, monkeypatch):
    """Create a temporary timeout config YAML and point the env var at it.

    Returns:
        Path to the temporary config file.
    """
    import yaml

    config = {
        "environments": {
            "demo_ch": {"base_timeout": 600},
            "kghm_local": {"base_timeout": 1200},
            "kghm_aws": {"base_timeout": 900},
            "tjhm_local": {"base_timeout": 1200},
            "tjhm_aws": {"base_timeout": 900},
        },
        "tasks": {
            "TestTask": {
                "relative_complexity": 2.0,
                "max_retries": 5,
                "retry_delay": 10,
            },
            "OverrideTask": {
                "relative_complexity": 1.0,
                "kghm_local_override": 3600,
            },
        },
    }
    config_path = tmp_path / "timeout_config.yaml"
    config_path.write_text(yaml.dump(config))
    monkeypatch.setenv("IEASYHYDROFORECAST_TIMEOUT_CONFIG_PATH", str(config_path))
    return config_path


@pytest.fixture
def reset_timeout_singleton():
    """Reset the TimeoutManager singleton between tests."""
    from apps.pipeline.src import timeout_manager as tm

    original = tm._timeout_manager
    tm._timeout_manager = None
    yield
    tm._timeout_manager = original


@pytest.fixture
def mock_docker_client():
    """Return a MagicMock for docker.from_env() with configurable container.

    Usage:
        client, container = mock_docker_client
        container.wait.return_value = {'StatusCode': 0}
    """
    container = MagicMock()
    container.id = "test_container_123"
    container.wait.return_value = {"StatusCode": 0}
    container.logs.return_value = b"container output logs"
    container.remove.return_value = None
    container.stop.return_value = None

    client = MagicMock()
    client.containers.run.return_value = container
    client.images.pull.return_value = None

    return client, container


@pytest.fixture
def mock_smtp(monkeypatch):
    """Patch smtplib.SMTP for notification tests.

    Returns:
        The MagicMock SMTP instance.
    """
    smtp_instance = MagicMock()
    smtp_class = MagicMock(return_value=smtp_instance)
    monkeypatch.setattr("smtplib.SMTP", smtp_class)

    # Set required SMTP env vars
    monkeypatch.setenv("SAPPHIRE_PIPELINE_SMTP_SERVER", "smtp.test.com")
    monkeypatch.setenv("SAPPHIRE_PIPELINE_SMTP_PORT", "587")
    monkeypatch.setenv("SAPPHIRE_PIPELINE_SMTP_USERNAME", "testuser")
    monkeypatch.setenv("SAPPHIRE_PIPELINE_SMTP_PASSWORD", "testpass")
    monkeypatch.setenv("SAPPHIRE_PIPELINE_SENDER_EMAIL", "test@test.com")

    return smtp_instance


# ---------------------------------------------------------------------------
# Shared run_locally.sh synthetic-venv harness (PREPG-024 move out of
# test_run_locally_orchestration.py into conftest.py: `synth_tree` is a
# pytest fixture, and ruff's F811 flags the cross-module
# `from test_run_locally_orchestration import synth_tree` + `def test_x(synth_tree)`
# pattern as a redefinition of an unused import. Fixtures belong in
# conftest.py, where pytest auto-discovers them by name with no import
# needed. `SynthTree`, `run_main`, and their supporting constants move
# alongside `synth_tree` because the fixture and `run_main` both depend on
# them. See test_run_locally_orchestration.py's module docstring for the
# full harness rationale -- unchanged by this move.)
# ---------------------------------------------------------------------------

APPS_DIR = Path(__file__).resolve().parents[2]
RUN_LOCALLY_SH = APPS_DIR / "run_locally.sh"

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
    "LT_RECOVERY_DATE",
]

# A fake `python` stub: logs its invocation to a call log, then lets a
# test-supplied bash snippet decide the exit code (falling back to 0).
# `@MODULE@` / `@CALL_LOG@` / `@LT_STUB@` / `@DECISION@` are substituted
# with plain str.replace() (not str.format()/f-string) because the template
# is full of literal bash `${...}` expansions that would otherwise have to
# be brace-escaped.
#
# `lt_forecast_mode` is logged as its own field, placed BEFORE the trailing
# `mode=%s` (SAPPHIRE_PREDICTION_MODE) field deliberately: the substring
# "mode=" also occurs inside the literal text "lt_forecast_mode=", and
# test_run_locally_orchestration.py's `_mode_of()` helper extracts
# SAPPHIRE_PREDICTION_MODE via `call_line.rsplit("mode=", 1)[-1]` -- the
# LAST occurrence of that substring in the line. Putting lt_forecast_mode
# after mode= would make its value shadow _mode_of()'s result for every
# existing caller. Keeping mode=%s last preserves that helper unchanged;
# LTF-010's own tests parse the lt_forecast_mode= field by name instead.
_STUB_TEMPLATE = """#!/usr/bin/env bash
script="$1"
shift || true
printf 'CALL module=@MODULE@ script=%s args=%s lt_forecast_mode=%s mode=%s\\n' \\
    "$script" "$*" "${lt_forecast_mode:-}" "${SAPPHIRE_PREDICTION_MODE:-}" >> "@CALL_LOG@"
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
    ml_models: list[str] | None = None,
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

    `ml_models`, when given, replaces the single-entry ML_MODELS default
    with the given list (e.g. ["TFT", "TIDE", "TSMIXER"]) -- ML-021's
    per-model exit-5 continuation tests need more than one model to prove
    "the remaining models still ran". A stub's `decision` snippet can
    branch on `$SAPPHIRE_MODEL_TO_USE` (set by run_in_venv's extra_env,
    same as `$SAPPHIRE_PREDICTION_MODE`) to script a specific model's exit
    code. Names are interpolated directly into the sourced bash array, so
    callers must only pass simple identifiers (as every existing ML model
    name is) -- never untrusted input.

    `cwd` defaults to APPS_DIR (matching every existing caller). A test that
    needs to prove behaviour of a RELATIVE ieasyhydroforecast_env_file_path
    (both validate_env's own resolution and emit_continue_on_error_hint's
    cwd-independence fix) passes a different `cwd` explicitly.

    `dry_run` passes ``--dry-run``, so validate_env still runs (it precedes
    the dry-run exit) but no module is ever dispatched -- used by
    TestModeDomainValidationUnderDryRun to prove Block 1/2 fire before
    dispatch, not merely before something a passing dispatch would also
    have blocked.
    """
    log_file = tree.log_dir / "run.log"
    flag = "--continue-on-error " if continue_on_error else ""
    flag += "--dry-run " if dry_run else ""

    ml_models_literal = " ".join(ml_models) if ml_models else "TFT"

    script = textwrap.dedent(f"""
        source "{RUN_LOCALLY_SH}"
        SCRIPT_DIR="{tree.script_dir}"
        LOG_DIR="{tree.log_dir}"
        LOG_FILE="{log_file}"
        ML_MODELS=({ml_models_literal})
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
