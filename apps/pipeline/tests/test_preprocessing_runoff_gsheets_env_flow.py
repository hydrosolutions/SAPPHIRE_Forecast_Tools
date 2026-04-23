"""Integration tests: GOOGLE_SHEETS_* env vars flow through docker-compose-luigi.yml.

These tests verify (a) the four GOOGLE_SHEETS_* env vars reach the
preprocessing-runoff service when set on the host, (b) the pipeline is a
clean no-op when they are unset (opt-in safety invariant), and (c) the
reader is a clean no-op when env vars are partially set.

Tests use `docker compose config` which renders YAML without starting any
container — no Docker daemon is required beyond the CLI binary.
Tests 1 and 2 are skipped gracefully when `docker` is not on PATH.
Test 3's direct-call assertion runs without Docker.

sys.path note: `apps/preprocessing_runoff/src` is not on the default
pipeline-test path (conftest.py adds repo root and pipeline dir only).
We add it explicitly here so the import of `google_sheets_reader` works
without modifying conftest.py or any production file.
"""

import os
import shutil
import subprocess
import sys

import pandas as pd
import pytest
import yaml

# ---------------------------------------------------------------------------
# sys.path fix for cross-module import
# ---------------------------------------------------------------------------
# conftest.py adds the repo root and apps/pipeline/ to sys.path.
# That is sufficient for `pipeline_docker` imports but NOT for
# `apps.preprocessing_runoff.src.google_sheets_reader`, which lives in a
# sibling module tree. We add the repo root (already present via conftest)
# and the src directory directly so the import works in isolation too.
_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_gsheets_src = os.path.join(_repo_root, "apps", "preprocessing_runoff", "src")
if _gsheets_src not in sys.path:
    sys.path.insert(0, _gsheets_src)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

# ---------------------------------------------------------------------------
# Docker availability guard
# ---------------------------------------------------------------------------
pytest_docker_skip = pytest.mark.skipif(
    shutil.which("docker") is None,
    reason="docker not installed",
)

# Path to the compose file — relative to repo root, must be absolute for
# subprocess calls that may run from any cwd.
_COMPOSE_FILE = os.path.join(_repo_root, "bin", "docker-compose-luigi.yml")

_SERVICE = "preprocessing-runoff"

_GSHEETS_VARS = [
    "GOOGLE_SHEETS_ENABLED",
    "GOOGLE_SHEETS_DISCHARGE_ID",
    "GOOGLE_SHEETS_CREDENTIALS_PATH",
    "GOOGLE_SHEETS_SITE_CODES",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _base_env(tmp_path: "os.PathLike") -> dict:
    """Return a dict of env vars required by docker-compose-luigi.yml.

    Without these, `docker compose config` prints "variable is not set"
    warnings and exits non-zero. The values are throwaway — they only need
    to satisfy the compose interpolation engine; no actual directories or
    Docker images are started.

    Args:
        tmp_path: pytest tmp_path fixture value (unique per test).

    Returns:
        Dictionary of environment variable names to dummy values.
    """
    base = str(tmp_path)
    env = os.environ.copy()
    env.update(
        {
            "ieasyhydroforecast_data_root_dir": base,
            "ieasyhydroforecast_env_file_path": os.path.join(base, "env"),
            "ieasyhydroforecast_backend_docker_image_tag": "test",
            "ieasyhydroforecast_data_ref_dir": base,
            "ieasyhydroforecast_container_data_ref_dir": "/container/ref",
            "DOCKER_GID": "988",
            # MAINTENANCE_TASK_TYPE has a :-long_term default; set it anyway
            # to silence any warning on older compose versions.
            "MAINTENANCE_TASK_TYPE": "long_term",
        }
    )
    # Ensure all four GOOGLE_SHEETS vars are not inherited from the calling shell
    for var in _GSHEETS_VARS:
        env.pop(var, None)
    return env


def _run_compose_config(env: dict) -> dict:
    """Invoke `docker compose config <service>` and parse the YAML output.

    Args:
        env: Full environment dict to pass to the subprocess.

    Returns:
        Parsed YAML dict from compose config stdout.

    Raises:
        AssertionError: If the subprocess exits non-zero.
    """
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            _COMPOSE_FILE,
            "config",
            _SERVICE,
        ],
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, (
        f"`docker compose config` failed (rc={result.returncode}).\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    return yaml.safe_load(result.stdout)


def _get_service_env(config: dict) -> dict:
    """Extract the environment mapping for the preprocessing-runoff service.

    `docker compose config` renders `environment:` as a dict of
    KEY -> VALUE (not a list). This helper normalises the output so tests
    can assert with plain dict look-ups regardless of compose version.

    Args:
        config: Parsed compose config dict from yaml.safe_load.

    Returns:
        Dict mapping env var name to its rendered value (as string or None).
    """
    service = config["services"][_SERVICE]
    env_block = service.get("environment", {})

    if isinstance(env_block, dict):
        # Standard compose v2 / v5 output: already a dict
        return {k: ("" if v is None else str(v)) for k, v in env_block.items()}

    # Fallback: older compose may emit a list of "KEY=VALUE" strings
    result = {}
    for entry in env_block:
        if "=" in entry:
            k, _, v = entry.partition("=")
            result[k] = v
        else:
            result[entry] = ""
    return result


# ---------------------------------------------------------------------------
# Test 1 — All four vars set
# ---------------------------------------------------------------------------


@pytest_docker_skip
def test_all_gsheets_vars_propagate_to_service(tmp_path):
    """All four GOOGLE_SHEETS_* vars appear with exact values in rendered config.

    When all four vars are exported before `docker compose config`, the
    rendered preprocessing-runoff environment block must contain each var
    with the exact value supplied — proving the ${VAR:-} wiring in the
    compose file works end-to-end.
    """
    expected = {
        "GOOGLE_SHEETS_ENABLED": "True",
        "GOOGLE_SHEETS_DISCHARGE_ID": "fake_spreadsheet_id_abc123",
        "GOOGLE_SHEETS_CREDENTIALS_PATH": "/fake/path/to/creds.json",
        "GOOGLE_SHEETS_SITE_CODES": "19999,20000",
    }

    env = _base_env(tmp_path)
    env.update(expected)

    config = _run_compose_config(env)
    service_env = _get_service_env(config)

    for var, value in expected.items():
        assert var in service_env, (
            f"Expected '{var}' in preprocessing-runoff environment, "
            f"but it was missing. Rendered env keys: {sorted(service_env)}"
        )
        assert service_env[var] == value, (
            f"'{var}' rendered as '{service_env[var]}', expected '{value}'."
        )


# ---------------------------------------------------------------------------
# Test 2 — No vars set (opt-in safety invariant)
# ---------------------------------------------------------------------------


@pytest_docker_skip
def test_no_gsheets_vars_renders_as_empty_strings(tmp_path):
    """When GOOGLE_SHEETS_* vars are unset, they render as empty strings.

    This is the opt-in safety invariant: organizations that do not set
    any Google Sheets env vars must see empty strings in the container
    environment (NOT literal '${VAR}' placeholders, NOT missing entries).
    The reader's top-of-function guard then returns an empty DataFrame
    without touching gspread.
    """
    env = _base_env(tmp_path)
    # All four vars already removed in _base_env; explicit for clarity.
    for var in _GSHEETS_VARS:
        env.pop(var, None)

    config = _run_compose_config(env)
    service_env = _get_service_env(config)

    for var in _GSHEETS_VARS:
        assert var in service_env, (
            f"'{var}' missing from preprocessing-runoff environment block entirely. "
            f"Expected it to be present with an empty string value."
        )
        rendered = service_env[var]
        assert rendered == "", (
            f"'{var}' rendered as '{rendered}' (expected empty string). "
            f"The compose file must use the '${{VAR:-}}' default pattern."
        )
        # Confirm it is NOT a literal placeholder — that would indicate a
        # missing ${VAR:-} default in the compose file.
        assert "${" not in rendered, f"'{var}' contains an unresolved placeholder: '{rendered}'."


# ---------------------------------------------------------------------------
# Test 3 — Partial config: ENABLED only; direct reader call returns empty
# ---------------------------------------------------------------------------


def test_partial_config_enabled_only(tmp_path):
    """Partial env (ENABLED=True, others unset) renders correctly + reader is safe.

    This test has two parts:
    1. Compose rendering (skipped when Docker unavailable): ENABLED renders
       as 'True', the other three vars render as empty strings.
    2. Direct reader call (always runs): calling read_discharge_from_google_sheet
       with empty args must return an empty DataFrame without raising, proving
       the defensive arg guard in the reader works correctly.
    """
    # --- Part A: compose rendering (needs Docker) ---
    if shutil.which("docker") is not None:
        env = _base_env(tmp_path)
        env["GOOGLE_SHEETS_ENABLED"] = "True"
        # Others remain unset (already popped in _base_env)

        config = _run_compose_config(env)
        service_env = _get_service_env(config)

        assert service_env.get("GOOGLE_SHEETS_ENABLED") == "True", (
            f"Expected GOOGLE_SHEETS_ENABLED='True', "
            f"got '{service_env.get('GOOGLE_SHEETS_ENABLED')}'."
        )
        for var in (
            "GOOGLE_SHEETS_DISCHARGE_ID",
            "GOOGLE_SHEETS_CREDENTIALS_PATH",
            "GOOGLE_SHEETS_SITE_CODES",
        ):
            rendered = service_env.get(var, None)
            assert rendered == "", f"Expected '{var}' to be empty string, got '{rendered}'."

    # --- Part B: direct reader call (no Docker needed) ---
    # Import here (not at top level) so collection succeeds even if the
    # preprocessing_runoff package is not importable during test discovery.
    from google_sheets_reader import read_discharge_from_google_sheet

    result = read_discharge_from_google_sheet("", [], "")

    assert isinstance(result, pd.DataFrame), f"Expected pd.DataFrame, got {type(result)}."
    assert result.empty, f"Expected empty DataFrame, got {len(result)} rows."
    # Column contract must be preserved even for empty return
    for col in ("code", "date", "discharge"):
        assert col in result.columns, (
            f"Column '{col}' missing from empty DataFrame returned by reader."
        )
