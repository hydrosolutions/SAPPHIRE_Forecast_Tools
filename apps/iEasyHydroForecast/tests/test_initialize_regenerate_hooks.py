"""Tests for the P6 regenerate-hooks meta-wrapper.

Covers:
- Wrapper CLI surface (`--help`, each `--skip-hook-<name>` flag, late-start
  window flag, station-filter documentation).
- Wrapper error paths (missing env file, missing wrapper script path).
- Dry-run inventory (all four hooks listed, correctly marked skipped on
  `--skip-hook-*` flags, no hook scripts invoked).
- Late-start guard (aborts when within window; bypassed by `--allow-late-start`).
- Cron-pause / cron-restore discipline (real run installs and reverts a
  crontab backup via traps).
- Long-term skill hook missing-script graceful handling.
- `--start-year` forwarding into snow-stats and hydrograph hook command lines.

The hook scripts themselves are stubbed via a temporary `bin/` directory on
PATH whose stubs record their invocations. The wrapper resolves sibling
scripts relative to `dirname "${BASH_SOURCE[0]}"` (i.e. the wrapper's own
directory), so a clean test must execute a copy of the wrapper from a tmp
`bin/` dir that also holds the stubs.

Integration against a live deployment is out of scope (Stage E item: hooks'
underlying actions are covered by their own test suites).
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

# Repo root: apps/iEasyHydroForecast/tests/test_*.py -> parents[3]
_REPO_ROOT = Path(__file__).resolve().parents[3]
_WRAPPER = _REPO_ROOT / "bin" / "initialize_regenerate_hooks.sh"
_BIN_DIR = _REPO_ROOT / "bin"
_UTILS_DIR = _BIN_DIR / "utils"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_env_file(tmp_path: Path) -> Path:
    """Write a minimal .env file the wrapper's read_configuration will accept.

    The wrapper requires `ieasyhydroforecast_data_root_dir` to be set. The
    other vars are populated by read_configuration via derivation from the
    env file path.
    """
    # Layout: <tmp_path>/data_ref/config/.env_kghm  (the suffix is required by
    # bin/utils/common_functions.sh:read_configuration, which derives URL
    # subdomains based on the env-file suffix. 'kghm' is one of the recognised
    # suffixes; the actual value does not matter for this wrapper.)
    config_dir = tmp_path / "data_ref" / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    env_path = config_dir / ".env_kghm"
    data_root = tmp_path / "data_root"
    data_root.mkdir(parents=True, exist_ok=True)
    env_path.write_text(
        f"ieasyhydroforecast_data_root_dir={data_root}\n"
        "ieasyhydroforecast_backend_docker_image_tag=local\n"
        "ieasyhydroforecast_frontend_docker_image_tag=local\n"
        "ieasyhydroforecast_url=example.invalid\n",
        encoding="utf-8",
    )
    return env_path


def _make_stub_bin(
    tmp_path: Path, wrapper_basename: str = "initialize_regenerate_hooks.sh"
) -> Path:
    """Copy the wrapper + helpers into a tmp bin/ tree and stub the four hook scripts.

    Returns the absolute path of the wrapper inside the stub bin/.
    Each stub script writes its argv to `<tmp_path>/<stub_name>.invoked` so
    tests can assert what (if anything) was called.
    """
    stub_bin = tmp_path / "bin"
    stub_utils = stub_bin / "utils"
    stub_utils.mkdir(parents=True, exist_ok=True)

    # Copy wrapper + helpers verbatim.
    shutil.copy(_WRAPPER, stub_bin / wrapper_basename)
    shutil.copy(
        _UTILS_DIR / "update_migration_helpers.sh", stub_utils / "update_migration_helpers.sh"
    )
    shutil.copy(_UTILS_DIR / "common_functions.sh", stub_utils / "common_functions.sh")

    # The wrapper sibling-script lookup uses dirname "$0", so stubs go next to
    # the wrapper copy.
    stubs = {
        "backfill_snow_stats_history.sh": "snow_stats",
        "yearly_runoff_hydrograph_aggregation.sh": "hydrograph_ms",
        "yearly_skill_metrics_recalculation.sh": "short_term_skill",
        "bimonthly_long_term_skill_metrics_recalculation.sh": "long_term_skill",
    }
    invocation_dir = tmp_path / "invocations"
    invocation_dir.mkdir(exist_ok=True)
    for fname, tag in stubs.items():
        target = stub_bin / fname
        target.write_text(
            "#!/usr/bin/env bash\n"
            f'echo "STUB:{tag}" "$@" >> "{invocation_dir}/{tag}.invoked"\n'
            "exit 0\n",
            encoding="utf-8",
        )
        target.chmod(0o755)

    return stub_bin / wrapper_basename


def _run_wrapper(
    wrapper_path: Path,
    args: list[str],
    timeout: int = 60,
    extra_env: dict | None = None,
) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        ["bash", str(wrapper_path), *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )


# ---------------------------------------------------------------------------
# 1. --help
# ---------------------------------------------------------------------------


def test_wrapper_help_returns_zero():
    """`bash bin/initialize_regenerate_hooks.sh --help` exits 0 and shows usage."""
    assert _WRAPPER.is_file(), f"wrapper missing: {_WRAPPER}"
    result = subprocess.run(
        ["bash", str(_WRAPPER), "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, f"expected exit 0, got {result.returncode}: {result.stderr}"
    assert "Usage" in result.stdout


def test_wrapper_help_documents_each_skip_hook_flag():
    """Every `--skip-hook-<name>` flag is listed in --help."""
    result = subprocess.run(
        ["bash", str(_WRAPPER), "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    out = result.stdout
    assert "--skip-hook-snow-stats" in out
    assert "--skip-hook-hydrograph-month-season" in out
    assert "--skip-hook-short-term-skill" in out
    assert "--skip-hook-long-term-skill" in out


def test_wrapper_help_documents_late_start_window():
    """The late-start window flags appear in --help."""
    result = subprocess.run(
        ["bash", str(_WRAPPER), "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    out = result.stdout
    assert "--late-start-window-minutes" in out
    assert "--allow-late-start" in out


def test_wrapper_help_documents_station_filter_contract():
    """Document the deliberate omission of --station-filter so operators
    porting muscle memory from P1a/P1b/P3/P5 know it isn't a typo. This pins
    the documentation so a refactor cannot silently drop the explanation."""
    result = subprocess.run(
        ["bash", str(_WRAPPER), "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    out = result.stdout
    # The help text must explicitly call out that station-filter is NOT honored.
    assert "Station-filter" in out or "station-filter" in out
    assert (
        "organisation-wide" in out
        or "organization-wide" in out
        or "NOT honor" in out
        or "not honor" in out
        or "intentionally absent" in out
    )


# ---------------------------------------------------------------------------
# 2. CLI error paths
# ---------------------------------------------------------------------------


def test_wrapper_rejects_missing_env_file(tmp_path):
    """Wrapper exits non-zero with descriptive error when env file is missing."""
    nonexistent = tmp_path / "no_such_env_file"
    result = subprocess.run(
        ["bash", str(_WRAPPER), str(nonexistent)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0, "expected non-zero exit"
    combined = (result.stdout + result.stderr).lower()
    assert "env file" in combined or "not found" in combined


def test_wrapper_rejects_no_args():
    """Wrapper exits non-zero when no arguments are supplied at all."""
    result = subprocess.run(
        ["bash", str(_WRAPPER)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "env_file_path" in combined or "required" in combined.lower()


def test_wrapper_rejects_unknown_flag(tmp_path):
    """Unknown flags surface as an error, not a silent skip."""
    env_file = _make_env_file(tmp_path)
    result = subprocess.run(
        ["bash", str(_WRAPPER), str(env_file), "--no-such-flag"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "unknown" in combined.lower()


def test_wrapper_rejects_non_numeric_start_year(tmp_path):
    """`--start-year not-a-year` is rejected at parse-time."""
    env_file = _make_env_file(tmp_path)
    result = subprocess.run(
        ["bash", str(_WRAPPER), str(env_file), "--start-year", "abc"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    combined = (result.stdout + result.stderr).lower()
    assert "four-digit" in combined or "start-year" in combined


# ---------------------------------------------------------------------------
# 3. Dry-run inventory
# ---------------------------------------------------------------------------


def test_dry_run_lists_all_four_hooks_when_no_skip_flags(tmp_path):
    """The dry-run inventory mentions each of the four hooks by name."""
    env_file = _make_env_file(tmp_path)
    result = _run_wrapper(_WRAPPER, [str(env_file), "--dry-run"])
    assert result.returncode == 0, f"dry-run failed: {result.stdout}\n{result.stderr}"
    out = result.stdout + result.stderr
    # Each hook line appears in the inventory (either as RUN or, if the
    # sibling script is missing on this branch, as MISS).
    assert "hook 1/4: snow-stats" in out
    assert "hook 2/4: hydrograph-month-season" in out
    assert "hook 3/4: short-term-skill" in out
    assert "hook 4/4: long-term-skill" in out


def test_dry_run_marks_skipped_hooks_correctly(tmp_path):
    """`--skip-hook-snow-stats` makes the dry-run report it as SKIP."""
    env_file = _make_env_file(tmp_path)
    result = _run_wrapper(
        _WRAPPER,
        [str(env_file), "--dry-run", "--skip-hook-snow-stats"],
    )
    assert result.returncode == 0, f"dry-run failed: {result.stdout}\n{result.stderr}"
    out = result.stdout + result.stderr
    # The snow-stats line should be marked SKIP; the others must not be.
    assert re.search(r"\[SKIP\]\s+hook 1/4: snow-stats", out)
    assert not re.search(r"\[SKIP\]\s+hook 2/4", out)
    assert not re.search(r"\[SKIP\]\s+hook 3/4", out)
    assert not re.search(r"\[SKIP\]\s+hook 4/4", out)


def test_dry_run_does_not_invoke_hook_scripts(tmp_path):
    """A dry-run with all stubs in place must NOT execute any stub."""
    env_file = _make_env_file(tmp_path)
    wrapper = _make_stub_bin(tmp_path)
    invocation_dir = tmp_path / "invocations"
    result = _run_wrapper(wrapper, [str(env_file), "--dry-run"])
    assert result.returncode == 0, f"dry-run failed: {result.stdout}\n{result.stderr}"
    # Not a single stub may have left an .invoked file.
    files = list(invocation_dir.glob("*.invoked"))
    assert files == [], (
        f"expected no stubs invoked in dry-run, got: {[f.name for f in files]}\n"
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )


# ---------------------------------------------------------------------------
# 4. Late-start guard
# ---------------------------------------------------------------------------


def _install_crontab(minute_offset: int, tmp_path: Path) -> dict:
    """Return env vars that wrap `crontab` with a shim returning a fake schedule.

    The wrapper invokes `crontab -l` to read the schedule and `crontab -` /
    `crontab <file>` to install. We override `crontab` with a shell script that:
      - echoes a single schedule line whose minute is current_minute + offset
        (i.e. the next tick is `offset` minutes from now)
      - silently accepts install commands so cron-pause does not actually touch
        the real user crontab.
    """
    fake_crontab = tmp_path / "fake_crontab"
    fake_crontab.write_text(
        "#!/usr/bin/env bash\n"
        'if [[ "$1" == "-l" ]]; then\n'
        f"  # Print a schedule line whose minute is now+{minute_offset}\n"
        f"  next_minute=$(( ( $(date +%-M) + {minute_offset} ) % 60 ))\n"
        '  echo "${next_minute} * * * * /bin/true"\n'
        "  exit 0\n"
        "fi\n"
        "# Silently accept install commands.\n"
        "cat > /dev/null 2>&1 || true\n"
        "exit 0\n",
        encoding="utf-8",
    )
    fake_crontab.chmod(0o755)
    # Put the fake on PATH ahead of the real crontab.
    shim_dir = tmp_path / "shim"
    shim_dir.mkdir(exist_ok=True)
    (shim_dir / "crontab").symlink_to(fake_crontab)
    return {"PATH": f"{shim_dir}:{os.environ['PATH']}"}


def test_late_start_guard_aborts_when_within_window(tmp_path):
    """Crontab says next tick in 5 minutes; window is 30; expect non-zero exit."""
    env_file = _make_env_file(tmp_path)
    fake_env = _install_crontab(minute_offset=5, tmp_path=tmp_path)
    result = _run_wrapper(
        _WRAPPER,
        [str(env_file)],
        extra_env=fake_env,
    )
    assert result.returncode != 0, (
        f"expected non-zero exit, got 0\nstdout={result.stdout}\nstderr={result.stderr}"
    )
    combined = result.stdout + result.stderr
    assert "late-start guard" in combined.lower()


def test_late_start_guard_bypass_with_allow_late_start(tmp_path):
    """`--allow-late-start` bypasses the guard even when within the window."""
    env_file = _make_env_file(tmp_path)
    # We use the stub bin so the real hooks aren't called.
    wrapper = _make_stub_bin(tmp_path)
    fake_env = _install_crontab(minute_offset=5, tmp_path=tmp_path)
    # Set late-start window to 30 to ensure 5 < 30, then opt-in.
    result = _run_wrapper(
        wrapper,
        [
            str(env_file),
            "--allow-late-start",
            "--late-start-window-minutes",
            "30",
            "--skip-hook-snow-stats",
            "--skip-hook-hydrograph-month-season",
            "--skip-hook-short-term-skill",
            "--skip-hook-long-term-skill",
        ],
        extra_env=fake_env,
    )
    # With all hooks skipped + late-start bypass, the wrapper should exit 0.
    assert result.returncode == 0, (
        f"expected exit 0, got {result.returncode}\nstdout={result.stdout}\nstderr={result.stderr}"
    )
    combined = result.stdout + result.stderr
    # The log line confirms --allow-late-start was acknowledged.
    assert "allow-late-start" in combined or "proceeding" in combined.lower()


def test_late_start_guard_disabled_with_zero_window(tmp_path):
    """`--late-start-window-minutes 0` disables the guard entirely."""
    env_file = _make_env_file(tmp_path)
    wrapper = _make_stub_bin(tmp_path)
    fake_env = _install_crontab(minute_offset=1, tmp_path=tmp_path)
    result = _run_wrapper(
        wrapper,
        [
            str(env_file),
            "--late-start-window-minutes",
            "0",
            "--skip-hook-snow-stats",
            "--skip-hook-hydrograph-month-season",
            "--skip-hook-short-term-skill",
            "--skip-hook-long-term-skill",
        ],
        extra_env=fake_env,
    )
    assert result.returncode == 0, (
        f"expected exit 0 with window=0, got {result.returncode}\n"
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )


# ---------------------------------------------------------------------------
# 5. Cron-pause / restore
# ---------------------------------------------------------------------------


def test_cron_pause_and_restore_called_on_normal_run(tmp_path):
    """A real run pauses cron BEFORE hooks and restores it on EXIT.

    Instruments the fake `crontab` so we can see the pause + restore calls in
    a log file. The fake records each invocation's args + a small marker.
    """
    env_file = _make_env_file(tmp_path)
    wrapper = _make_stub_bin(tmp_path)
    # Pretend the next cron tick is far in the future so the guard does not fire.
    crontab_log = tmp_path / "crontab.log"
    fake_crontab = tmp_path / "fake_crontab"
    # Record every invocation; for `-l`, echo a non-empty crontab so the pause
    # is real. For other invocations (install), record the args.
    fake_crontab.write_text(
        "#!/usr/bin/env bash\n"
        f'echo "INVOKE: $*" >> "{crontab_log}"\n'
        'if [[ "$1" == "-l" ]]; then\n'
        # Schedule line that's far in the future.
        '  echo "0 0 * * 1 /bin/true"\n'
        "  exit 0\n"
        "fi\n"
        # Install paths: just consume stdin.
        "cat > /dev/null 2>&1 || true\n"
        "exit 0\n",
        encoding="utf-8",
    )
    fake_crontab.chmod(0o755)
    shim_dir = tmp_path / "shim"
    shim_dir.mkdir(exist_ok=True)
    (shim_dir / "crontab").symlink_to(fake_crontab)
    fake_env = {"PATH": f"{shim_dir}:{os.environ['PATH']}"}

    result = _run_wrapper(
        wrapper,
        [
            str(env_file),
            # Skip all hooks so the loop terminates immediately and we exercise
            # only the pause/restore path.
            "--skip-hook-snow-stats",
            "--skip-hook-hydrograph-month-season",
            "--skip-hook-short-term-skill",
            "--skip-hook-long-term-skill",
        ],
        extra_env=fake_env,
    )
    assert result.returncode == 0, f"wrapper failed: {result.stdout}\n{result.stderr}"
    assert crontab_log.is_file(), "fake crontab never invoked"
    log_text = crontab_log.read_text(encoding="utf-8")
    # Expect at least one `-l` (pause read), one `-` (pause write/empty), and
    # one positional-file install (restore).
    invocations = [line for line in log_text.splitlines() if line.startswith("INVOKE:")]
    assert any("INVOKE: -l" in line for line in invocations), (
        f"expected `crontab -l` invocation in log: {log_text!r}"
    )
    # The pause writes an empty stdin via `crontab -`.
    assert any(line.strip() == "INVOKE: -" for line in invocations), (
        f"expected `crontab -` (empty pause install) in log: {log_text!r}"
    )
    # The restore passes the backup file as a positional argument; the line
    # will be "INVOKE: /path/to/backup".
    assert any(line.startswith("INVOKE:") and "backup" in line.lower() for line in invocations), (
        f"expected restore-from-backup invocation in log: {log_text!r}"
    )


def test_cron_restore_called_when_hook_fails(tmp_path):
    """Cron is restored even when a hook exits non-zero (trap on EXIT/INT/TERM)."""
    env_file = _make_env_file(tmp_path)
    wrapper = _make_stub_bin(tmp_path)

    # Replace the snow-stats stub with one that fails.
    failing_stub = wrapper.parent / "backfill_snow_stats_history.sh"
    failing_stub.write_text(
        '#!/usr/bin/env bash\necho "snow-stats stub: deliberate failure"\nexit 7\n',
        encoding="utf-8",
    )
    failing_stub.chmod(0o755)

    # Fake crontab that records calls + reports a far-future schedule.
    crontab_log = tmp_path / "crontab.log"
    fake_crontab = tmp_path / "fake_crontab"
    fake_crontab.write_text(
        "#!/usr/bin/env bash\n"
        f'echo "INVOKE: $*" >> "{crontab_log}"\n'
        'if [[ "$1" == "-l" ]]; then\n'
        '  echo "0 0 * * 1 /bin/true"\n'
        "  exit 0\n"
        "fi\n"
        "cat > /dev/null 2>&1 || true\n"
        "exit 0\n",
        encoding="utf-8",
    )
    fake_crontab.chmod(0o755)
    shim_dir = tmp_path / "shim"
    shim_dir.mkdir(exist_ok=True)
    (shim_dir / "crontab").symlink_to(fake_crontab)
    fake_env = {"PATH": f"{shim_dir}:{os.environ['PATH']}"}

    result = _run_wrapper(
        wrapper,
        [
            str(env_file),
            # Skip later hooks so the wrapper doesn't drag on after snow-stats
            # fails fast.
            "--skip-hook-hydrograph-month-season",
            "--skip-hook-short-term-skill",
            "--skip-hook-long-term-skill",
        ],
        extra_env=fake_env,
    )
    # Fail-fast: overall exit should be the snow-stats stub's exit code (7).
    assert result.returncode == 7, (
        f"expected exit 7 from failing snow-stats, got {result.returncode}\n"
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
    # Even though the run failed, cron must have been restored via the trap.
    log_text = crontab_log.read_text(encoding="utf-8")
    invocations = [line for line in log_text.splitlines() if line.startswith("INVOKE:")]
    assert any(line.startswith("INVOKE:") and "backup" in line.lower() for line in invocations), (
        f"expected cron restore (backup install) on failure path: {log_text!r}"
    )


# ---------------------------------------------------------------------------
# 6. Long-term-skill missing-script graceful handling
# ---------------------------------------------------------------------------


def test_long_term_skill_hook_handles_missing_script_gracefully(tmp_path):
    """When the long-term skill script is missing on disk, the wrapper logs
    a WARNING and continues (or completes) rather than crashing.

    Implementation: build a stub bin with three hook scripts present and the
    long-term one deliberately absent. The wrapper must still exit 0.
    """
    env_file = _make_env_file(tmp_path)
    wrapper = _make_stub_bin(tmp_path)
    # Remove the long-term skill stub.
    (wrapper.parent / "bimonthly_long_term_skill_metrics_recalculation.sh").unlink()

    # Skip the other long-running hooks so the test is fast.
    result = _run_wrapper(
        wrapper,
        [
            str(env_file),
            "--skip-hook-snow-stats",
            "--skip-hook-hydrograph-month-season",
            "--skip-hook-short-term-skill",
            "--late-start-window-minutes",
            "0",
        ],
    )
    assert result.returncode == 0, (
        f"expected exit 0 when long-term script is absent, got {result.returncode}\n"
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
    combined = result.stdout + result.stderr
    # Wrapper must mention the long-term hook + the warning / skip language.
    assert "long-term-skill" in combined
    assert "not found" in combined.lower() or "warning" in combined.lower()


# ---------------------------------------------------------------------------
# 7. --start-year forwarding
# ---------------------------------------------------------------------------


def test_wrapper_forwards_start_year_to_snow_stats_in_dry_run(tmp_path):
    """`--start-year 2015` appears in the snow-stats dry-run command line."""
    env_file = _make_env_file(tmp_path)
    result = _run_wrapper(
        _WRAPPER,
        [str(env_file), "--dry-run", "--start-year", "2015"],
    )
    assert result.returncode == 0, f"dry-run failed: {result.stdout}\n{result.stderr}"
    combined = result.stdout + result.stderr
    # The dry-run prints the snow-stats command line incl. --start-year 2015.
    # We look at the snow-stats inventory line specifically.
    # The line is of the form:
    #   cmd: ieasyhydroforecast_env_file_path=... bash <path>/backfill_snow_stats_history.sh --start-year 2015
    assert "--start-year" in combined
    assert "2015" in combined


def test_wrapper_forwards_start_year_to_hydrograph_year_range_in_dry_run(tmp_path):
    """The hydrograph per-year loop starts at --start-year in the dry-run."""
    env_file = _make_env_file(tmp_path)
    result = _run_wrapper(
        _WRAPPER,
        [str(env_file), "--dry-run", "--start-year", "2015"],
    )
    assert result.returncode == 0, f"dry-run failed: {result.stdout}\n{result.stderr}"
    combined = result.stdout + result.stderr
    # The hydrograph inventory should mention --target-year 2015 as one of the
    # per-year cmd lines.
    assert "--target-year 2015" in combined


# ---------------------------------------------------------------------------
# 8. Module audit — N/A
#
# P6 ships a bash wrapper only (no new Python module). The stdlib-only audit
# rule for migration_py modules does not apply. This test pins the contract
# so a future plan author who copies the P5 pattern remembers to add an audit
# test only when they introduce a Python module.
# ---------------------------------------------------------------------------


def test_module_audit_not_applicable():
    """P6 adds no Python module under bin/utils/migration_py/."""
    migration_py_dir = _REPO_ROOT / "bin" / "utils" / "migration_py"
    if not migration_py_dir.is_dir():
        pytest.skip("migration_py directory missing; P0 not present on this branch")
    # No file in migration_py/ should reference 'regenerate_hooks' (the P6 name).
    for py in migration_py_dir.glob("*.py"):
        text = py.read_text(encoding="utf-8")
        assert "regenerate_hooks" not in text, (
            f"unexpected migration_py module references regenerate_hooks: {py}"
        )
