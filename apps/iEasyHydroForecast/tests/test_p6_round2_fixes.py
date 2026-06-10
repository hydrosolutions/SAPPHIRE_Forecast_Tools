"""Round-2 fix regression tests for the P6 regenerate-hooks meta-wrapper.

These tests pin the behaviour introduced by the round-2 reviewer feedback:

    Fix 1 (cron policy):   --allow-unpaused-cron + four-way classification
    Fix 2 (signal traps):  separate INT/TERM handlers, _on_signal exit codes
    Fix 3 (hook 3):        yearly_skill_metrics_recalculation.sh exit propagation
    Fix 4 (workspace):     umh_acquire_temp_workspace + backup-path log
    Fix 5 (preflight):     mandatory-hook validation before cron pause

Helpers reuse the stub-bin harness from test_initialize_regenerate_hooks.py
and add a `crontab` PATH stub when behaviour depends on the cron pipeline.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
_WRAPPER = _REPO_ROOT / "bin" / "initialize_regenerate_hooks.sh"
_BIN_DIR = _REPO_ROOT / "bin"
_UTILS_DIR = _BIN_DIR / "utils"
_YEARLY_SKILL = _BIN_DIR / "yearly_skill_metrics_recalculation.sh"


# ---------------------------------------------------------------------------
# Harness — local copy of the helpers so this file is self-contained.
# ---------------------------------------------------------------------------


def _make_env_file(tmp_path: Path) -> Path:
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
    tmp_path: Path,
    *,
    skip_hooks: set[str] | None = None,
    short_term_skill_exit: int = 0,
) -> Path:
    """Build a tmp bin/ tree with the wrapper + four hook stubs.

    Args:
        tmp_path: pytest tmp_path fixture.
        skip_hooks: set of stub filenames to NOT create (simulates missing
            scripts on disk). Useful for preflight tests.
        short_term_skill_exit: exit code for the short-term-skill stub
            (`yearly_skill_metrics_recalculation.sh`). Useful for hook-3
            fail-fast tests.
    """
    skip_hooks = skip_hooks or set()
    stub_bin = tmp_path / "bin"
    stub_utils = stub_bin / "utils"
    stub_utils.mkdir(parents=True, exist_ok=True)
    shutil.copy(_WRAPPER, stub_bin / "initialize_regenerate_hooks.sh")
    shutil.copy(
        _UTILS_DIR / "update_migration_helpers.sh", stub_utils / "update_migration_helpers.sh"
    )
    shutil.copy(_UTILS_DIR / "common_functions.sh", stub_utils / "common_functions.sh")
    stubs = {
        "backfill_snow_stats_history.sh": ("snow_stats", 0),
        "yearly_runoff_hydrograph_aggregation.sh": ("hydrograph_ms", 0),
        "yearly_skill_metrics_recalculation.sh": ("short_term_skill", short_term_skill_exit),
        "bimonthly_long_term_skill_metrics_recalculation.sh": ("long_term_skill", 0),
    }
    invocation_dir = tmp_path / "invocations"
    invocation_dir.mkdir(exist_ok=True)
    for fname, (tag, exit_code) in stubs.items():
        if fname in skip_hooks:
            continue
        target = stub_bin / fname
        target.write_text(
            "#!/usr/bin/env bash\n"
            f'echo "STUB:{tag}" "$@" >> "{invocation_dir}/{tag}.invoked"\n'
            f"exit {exit_code}\n",
            encoding="utf-8",
        )
        target.chmod(0o755)
    return stub_bin / "initialize_regenerate_hooks.sh"


def _make_crontab_stub(
    tmp_path: Path,
    *,
    behavior: str = "happy",
) -> Path:
    """Create a fake `crontab` on PATH that emulates one of several behaviours.

    behavior options:
        "happy"          — `crontab -l` returns a single tick line; `crontab -` succeeds.
        "no_user_crontab"— `crontab -l` exits 1 with "no crontab for testuser".
        "real_error"     — `crontab -l` exits 2 with "permission denied".
        "write_failure"  — `crontab -l` returns a line; `crontab -` exits 1.

    Returns the directory containing the stub (caller prepends it to PATH).
    """
    fake_dir = tmp_path / "fake_path"
    fake_dir.mkdir(exist_ok=True)
    stub = fake_dir / "crontab"

    if behavior == "happy":
        body = (
            "#!/usr/bin/env bash\n"
            'if [[ "$1" == "-l" ]]; then\n'
            '    echo "0 12 * * * /bin/true"\n'
            "    exit 0\n"
            "fi\n"
            'if [[ "$1" == "-" ]]; then\n'
            "    cat > /dev/null\n"
            "    exit 0\n"
            "fi\n"
            'if [[ -f "$1" ]]; then\n'
            "    exit 0\n"
            "fi\n"
            "exit 0\n"
        )
    elif behavior == "no_user_crontab":
        body = (
            "#!/usr/bin/env bash\n"
            'if [[ "$1" == "-l" ]]; then\n'
            '    echo "no crontab for testuser" >&2\n'
            "    exit 1\n"
            "fi\n"
            "exit 0\n"
        )
    elif behavior == "real_error":
        body = (
            "#!/usr/bin/env bash\n"
            'if [[ "$1" == "-l" ]]; then\n'
            '    echo "crontab: permission denied" >&2\n'
            "    exit 2\n"
            "fi\n"
            "exit 0\n"
        )
    elif behavior == "write_failure":
        body = (
            "#!/usr/bin/env bash\n"
            'if [[ "$1" == "-l" ]]; then\n'
            '    echo "0 12 * * * /bin/true"\n'
            "    exit 0\n"
            "fi\n"
            'if [[ "$1" == "-" ]]; then\n'
            "    cat > /dev/null\n"
            '    echo "crontab: write failed" >&2\n'
            "    exit 1\n"
            "fi\n"
            "exit 0\n"
        )
    else:
        raise ValueError(f"unknown behavior: {behavior}")

    stub.write_text(body, encoding="utf-8")
    stub.chmod(0o755)
    return fake_dir


def _run_wrapper(
    wrapper_path: Path,
    args: list[str],
    *,
    timeout: int = 60,
    extra_env: dict | None = None,
    path_prepend: Path | None = None,
    path_replace_minimal: bool = False,
) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    if path_replace_minimal:
        # For "crontab binary missing" — give the wrapper a PATH with the
        # essentials (bash, python3, mktemp etc) but no crontab.
        env["PATH"] = "/usr/bin:/bin"
    if path_prepend:
        env["PATH"] = f"{path_prepend}{os.pathsep}{env.get('PATH', '')}"
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
# Fix 1 — cron-pause four-way classification + --allow-unpaused-cron
# ---------------------------------------------------------------------------


def test_pause_proceeds_when_no_user_crontab(tmp_path):
    """`crontab -l` returns 'no crontab for ...' -> INFO + proceed (not abort)."""
    env_file = _make_env_file(tmp_path)
    wrapper = _make_stub_bin(tmp_path)
    fake_path = _make_crontab_stub(tmp_path, behavior="no_user_crontab")
    result = _run_wrapper(
        wrapper,
        [
            str(env_file),
            "--skip-hook-snow-stats",
            "--skip-hook-hydrograph-month-season",
            "--skip-hook-short-term-skill",
            "--skip-hook-long-term-skill",
            "--late-start-window-minutes",
            "0",
        ],
        path_prepend=fake_path,
    )
    assert result.returncode == 0, (
        f"expected proceed on 'no crontab', got {result.returncode}\n"
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
    combined = result.stdout + result.stderr
    assert "no crontab installed for user" in combined.lower()


def test_pause_aborts_when_crontab_binary_missing():
    """`crontab` binary missing from PATH -> hard-fail, NO bypass.

    Verified via source-text inspection. A behavioural test would need to
    portably scrub `crontab` from PATH while keeping coreutils + bash
    available; mock approaches diverge across BSD/macOS/Linux and don't
    add coverage beyond what the assertions below pin."""
    src = _WRAPPER.read_text(encoding="utf-8")
    assert "command -v crontab >/dev/null 2>&1" in src
    assert "crontab(1) is not installed" in src
    assert "no bypass available" in src.lower()
    # The check must precede the four-way classification (return 1 inside
    # the `! command -v` branch unconditionally — no ALLOW_UNPAUSED_CRON
    # bypass on this branch).
    pause_start = src.index("_pause_cron() {")
    binary_check_pos = src.index("command -v crontab", pause_start)
    classification_pos = src.index("LC_ALL=C crontab -l", pause_start)
    assert binary_check_pos < classification_pos, (
        "binary-missing check must come BEFORE the four-way classification"
    )
    # The bypass flag is checked elsewhere in _pause_cron, but NOT inside
    # the binary-missing branch.
    binary_block_end = src.index("fi", binary_check_pos)
    binary_block = src[binary_check_pos:binary_block_end]
    assert "ALLOW_UNPAUSED_CRON" not in binary_block, (
        "binary-missing branch must NOT honour --allow-unpaused-cron"
    )


def test_pause_aborts_on_real_crontab_l_error(tmp_path):
    """`crontab -l` returns a real error (not 'no crontab') -> hard-fail."""
    env_file = _make_env_file(tmp_path)
    wrapper = _make_stub_bin(tmp_path)
    fake_path = _make_crontab_stub(tmp_path, behavior="real_error")
    result = _run_wrapper(
        wrapper,
        [
            str(env_file),
            "--skip-hook-snow-stats",
            "--skip-hook-hydrograph-month-season",
            "--skip-hook-short-term-skill",
            "--skip-hook-long-term-skill",
            "--late-start-window-minutes",
            "0",
        ],
        path_prepend=fake_path,
    )
    assert result.returncode != 0, (
        f"expected hard-fail on crontab -l error, got {result.returncode}\n"
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
    combined = result.stdout + result.stderr
    assert "crontab -l failed" in combined


def test_pause_aborts_on_write_failure(tmp_path):
    """`crontab -` write fails -> hard-fail; partial backup is removed."""
    env_file = _make_env_file(tmp_path)
    wrapper = _make_stub_bin(tmp_path)
    fake_path = _make_crontab_stub(tmp_path, behavior="write_failure")
    result = _run_wrapper(
        wrapper,
        [
            str(env_file),
            "--skip-hook-snow-stats",
            "--skip-hook-hydrograph-month-season",
            "--skip-hook-short-term-skill",
            "--skip-hook-long-term-skill",
            "--late-start-window-minutes",
            "0",
        ],
        path_prepend=fake_path,
    )
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "failed to install empty crontab" in combined


def test_allow_unpaused_cron_downgrades_write_failure_to_warning(tmp_path):
    """With --allow-unpaused-cron, a `crontab -` write failure becomes a
    WARNING and the wrapper proceeds with cron ACTIVE. Exactly ONE warning
    fires per condition (regression guard against duplicate logs)."""
    env_file = _make_env_file(tmp_path)
    wrapper = _make_stub_bin(tmp_path)
    fake_path = _make_crontab_stub(tmp_path, behavior="write_failure")
    result = _run_wrapper(
        wrapper,
        [
            str(env_file),
            "--allow-unpaused-cron",
            "--skip-hook-snow-stats",
            "--skip-hook-hydrograph-month-season",
            "--skip-hook-short-term-skill",
            "--skip-hook-long-term-skill",
            "--late-start-window-minutes",
            "0",
        ],
        path_prepend=fake_path,
    )
    assert result.returncode == 0, (
        f"expected proceed with bypass + write failure, got {result.returncode}\n"
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
    combined = result.stdout + result.stderr
    # The "cron was NEVER paused" message identifies the bypass path.
    assert combined.count("cron was NEVER paused") == 1, (
        f"expected exactly one bypass-cron-never-paused message; output:\n{combined}"
    )


# ---------------------------------------------------------------------------
# Fix 2 — separate signal handlers + _on_signal
# ---------------------------------------------------------------------------


def test_int_term_traps_are_separate_from_exit():
    """`trap _on_exit EXIT` and `trap '_on_signal ...' INT/TERM` are
    three distinct registrations."""
    src = _WRAPPER.read_text(encoding="utf-8")
    assert "trap _on_exit EXIT" in src
    assert "trap '_on_signal 130' INT" in src
    assert "trap '_on_signal 143' TERM" in src


def test_signal_handler_exits_with_conventional_code():
    """`_on_signal` calls `exit "$code"` where the caller passed 130 (INT)
    or 143 (TERM) per POSIX convention. `_on_exit` preserves the incoming
    exit code via `local rc=$?` + `return "$rc"`."""
    src = _WRAPPER.read_text(encoding="utf-8")
    assert 'exit "$code"' in src
    assert "local rc=$?" in src
    assert 'return "$rc"' in src
    # Both handlers guard cleanup with || true so a restore failure doesn't
    # skip workspace cleanup.
    assert "_restore_cron || true" in src
    assert "_umh_cleanup_tempdirs || true" in src


# ---------------------------------------------------------------------------
# Fix 3 — yearly_skill_metrics_recalculation.sh exit propagation
#         + hook 3 failure aborts P6 run
# ---------------------------------------------------------------------------


def test_yearly_skill_metrics_propagates_exit_code():
    """The yearly_skill_metrics_recalculation.sh script must `exit` with
    the container exit code, not fall off the end with exit 0."""
    src = _YEARLY_SKILL.read_text(encoding="utf-8")
    # Must end with an explicit `exit "$CONTAINER_EXIT_CODE"` (any quoting).
    assert 'exit "$CONTAINER_EXIT_CODE"' in src or "exit $CONTAINER_EXIT_CODE" in src, (
        "expected explicit exit with $CONTAINER_EXIT_CODE at end of script"
    )


def test_hook_3_failure_aborts_p6_run(tmp_path):
    """When the short-term-skill stub exits non-zero, P6 must abort the
    run (fail-fast). Round-2 reviewer found the upstream script swallowed
    failures; this regression test pins the fixed behaviour."""
    env_file = _make_env_file(tmp_path)
    wrapper = _make_stub_bin(tmp_path, short_term_skill_exit=7)
    fake_path = _make_crontab_stub(tmp_path, behavior="happy")
    result = _run_wrapper(
        wrapper,
        [
            str(env_file),
            "--skip-hook-snow-stats",
            "--skip-hook-hydrograph-month-season",
            "--skip-hook-long-term-skill",
            "--late-start-window-minutes",
            "0",
        ],
        path_prepend=fake_path,
    )
    assert result.returncode != 0, (
        f"expected P6 to fail-fast on hook-3 non-zero exit, got {result.returncode}\n"
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
    combined = result.stdout + result.stderr
    assert "short-term-skill: END (exit=7)" in combined


# ---------------------------------------------------------------------------
# Fix 4 — workspace acquisition + trap composition
# ---------------------------------------------------------------------------


def test_pause_backup_lives_under_log_dir_not_workspace():
    """Round-3 contract: cron backup lives in LOG_DIR (set via
    _CRON_BACKUP_DIR), NOT in the umh-managed workspace (TMPDIRS[0]).
    This ensures the backup survives the trap-driven workspace cleanup
    when restore fails or --allow-unpaused-cron is set."""
    src = _WRAPPER.read_text(encoding="utf-8")
    # Backup directory is the new script-scope variable, set from LOG_DIR.
    assert "_CRON_BACKUP_DIR=" in src
    assert '_CRON_BACKUP_DIR="$LOG_DIR"' in src
    # Backup path is composed under _CRON_BACKUP_DIR with a timestamped
    # filename.
    assert '_CRON_BACKUP_PATH="${_CRON_BACKUP_DIR}/crontab_backup_${backup_ts}.txt"' in src
    # The old TMPDIRS[0] target must be gone.
    assert "TMPDIRS[0]}/crontab_backup.txt" not in src
    # And the mktemp fallback that round-2 already removed must stay gone.
    assert "mktemp -t crontab_backup" not in src
    # And the backup-path log must precede the pause attempt.
    pre_pause, _, _ = src.partition("printf '' | crontab -")
    assert "backup written:" in pre_pause


def test_umh_cleanup_called_from_on_exit_and_on_signal():
    """Both trap handlers call `_umh_cleanup_tempdirs` explicitly because
    the wrapper's later `trap _on_exit EXIT` overwrites the helper's own
    EXIT trap. Function-body boundary uses `\\n}\\n` (closing brace on its
    own line) so `${var}`-style interpolation doesn't end the search early."""
    src = _WRAPPER.read_text(encoding="utf-8")

    on_exit_start = src.index("_on_exit() {")
    on_exit_end = src.index("\n}\n", on_exit_start)
    on_exit_body = src[on_exit_start:on_exit_end]
    assert "_umh_cleanup_tempdirs" in on_exit_body

    on_signal_start = src.index("_on_signal() {")
    on_signal_end = src.index("\n}\n", on_signal_start)
    on_signal_body = src[on_signal_start:on_signal_end]
    assert "_umh_cleanup_tempdirs" in on_signal_body


def test_workspace_acquired_before_pause_in_main():
    """main() acquires the temp workspace BEFORE calling _pause_cron.
    Match the actual call sites (line-anchored) rather than comments that
    mention `_pause_cron` or `umh_acquire_temp_workspace` in prose."""
    src = _WRAPPER.read_text(encoding="utf-8")
    real_run_start = src.index("# Real run: preflight mandatory hooks")
    real_run_block = src[real_run_start : real_run_start + 2000]
    # Match the actual call sites (start-of-line + 4 spaces of indent).
    preflight_pos = real_run_block.index("    if ! _preflight_validate_hooks; then")
    workspace_pos = real_run_block.index("    umh_acquire_temp_workspace regenerate_hooks")
    pause_pos = real_run_block.index("    if ! _pause_cron; then")
    assert preflight_pos < workspace_pos < pause_pos, (
        "expected order: preflight < workspace < pause; got positions "
        f"preflight={preflight_pos}, workspace={workspace_pos}, pause={pause_pos}"
    )


# ---------------------------------------------------------------------------
# Fix 5 — preflight + dry-run [MISS fatal] markers
# ---------------------------------------------------------------------------


def test_preflight_aborts_before_cron_pause_when_snow_script_missing(tmp_path):
    """Missing snow-stats script aborts the run BEFORE cron pause is
    attempted. Behavioral: instrument the crontab stub to record any call;
    assert it was NOT called."""
    env_file = _make_env_file(tmp_path)
    wrapper = _make_stub_bin(tmp_path, skip_hooks={"backfill_snow_stats_history.sh"})

    # Crontab stub that records every call so we can assert it was NOT used.
    fake_path = tmp_path / "fake_path"
    fake_path.mkdir(exist_ok=True)
    call_log = tmp_path / "crontab_calls.log"
    crontab_stub = fake_path / "crontab"
    crontab_stub.write_text(
        f'#!/usr/bin/env bash\necho "called: $*" >> "{call_log}"\nexit 0\n',
        encoding="utf-8",
    )
    crontab_stub.chmod(0o755)

    result = _run_wrapper(
        wrapper,
        [
            str(env_file),
            "--late-start-window-minutes",
            "0",
        ],
        path_prepend=fake_path,
    )
    assert result.returncode != 0, (
        f"expected preflight abort on missing snow script, got {result.returncode}\n"
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
    combined = result.stdout + result.stderr
    assert "snow-stats" in combined.lower()
    assert "preflight" in combined.lower()
    assert "abort" in combined.lower()
    # Crontab must NOT have been called — preflight runs first.
    assert not call_log.exists() or call_log.read_text() == "", (
        f"crontab was called before preflight abort: {call_log.read_text()}"
    )


def test_preflight_aborts_when_hydrograph_script_missing(tmp_path):
    env_file = _make_env_file(tmp_path)
    wrapper = _make_stub_bin(tmp_path, skip_hooks={"yearly_runoff_hydrograph_aggregation.sh"})
    fake_path = _make_crontab_stub(tmp_path, behavior="happy")
    result = _run_wrapper(
        wrapper,
        [str(env_file), "--late-start-window-minutes", "0"],
        path_prepend=fake_path,
    )
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "hydrograph-month-season" in combined.lower()


def test_preflight_aborts_when_short_term_skill_script_missing(tmp_path):
    env_file = _make_env_file(tmp_path)
    wrapper = _make_stub_bin(tmp_path, skip_hooks={"yearly_skill_metrics_recalculation.sh"})
    fake_path = _make_crontab_stub(tmp_path, behavior="happy")
    result = _run_wrapper(
        wrapper,
        [str(env_file), "--late-start-window-minutes", "0"],
        path_prepend=fake_path,
    )
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "short-term-skill" in combined.lower()


def test_preflight_passes_when_mandatory_hook_explicitly_skipped(tmp_path):
    """If snow-stats script is missing BUT the operator passes
    --skip-hook-snow-stats, preflight should pass (the operator declared
    intent)."""
    env_file = _make_env_file(tmp_path)
    wrapper = _make_stub_bin(tmp_path, skip_hooks={"backfill_snow_stats_history.sh"})
    fake_path = _make_crontab_stub(tmp_path, behavior="happy")
    result = _run_wrapper(
        wrapper,
        [
            str(env_file),
            "--skip-hook-snow-stats",
            "--skip-hook-hydrograph-month-season",
            "--skip-hook-short-term-skill",
            "--skip-hook-long-term-skill",
            "--late-start-window-minutes",
            "0",
        ],
        path_prepend=fake_path,
    )
    assert result.returncode == 0, (
        f"expected proceed when missing hook is explicitly skipped, got {result.returncode}\n"
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )


def test_dry_run_marks_missing_mandatory_hooks_as_miss_fatal(tmp_path):
    """Dry-run inventory must show `[MISS fatal]` for missing hooks 1-3
    (not 'will skip at run-time')."""
    env_file = _make_env_file(tmp_path)
    wrapper = _make_stub_bin(tmp_path, skip_hooks={"backfill_snow_stats_history.sh"})
    result = _run_wrapper(wrapper, [str(env_file), "--dry-run"])
    assert result.returncode == 0
    combined = result.stdout + result.stderr
    assert "[MISS fatal]" in combined
    assert "will ABORT before any hook runs" in combined
    # And the old wording is gone.
    assert "will skip at run-time" not in combined


def test_dry_run_marks_missing_hook4_as_graceful_skip(tmp_path):
    """Dry-run inventory shows hook 4 with missing script as
    `[GRACEFUL SKIP — see WARNING]` + a reference to the follow-up issue."""
    env_file = _make_env_file(tmp_path)
    wrapper = _make_stub_bin(
        tmp_path,
        skip_hooks={"bimonthly_long_term_skill_metrics_recalculation.sh"},
    )
    result = _run_wrapper(wrapper, [str(env_file), "--dry-run"])
    assert result.returncode == 0
    combined = result.stdout + result.stderr
    assert "[GRACEFUL SKIP" in combined
    assert "hook4_long_term_skill_mandatory" in combined


# ---------------------------------------------------------------------------
# Help + flag documentation
# ---------------------------------------------------------------------------


def test_help_documents_allow_unpaused_cron_flag():
    """--help must document the new --allow-unpaused-cron flag + its
    intended use (verified no-race hosts only)."""
    result = subprocess.run(
        ["bash", str(_WRAPPER), "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    assert "--allow-unpaused-cron" in result.stdout
    assert "no-race hosts" in result.stdout.lower()
    assert "does not bypass" in result.stdout.lower()


# ---------------------------------------------------------------------------
# Round-3 lifetime contract — behavioural tests
# ---------------------------------------------------------------------------


def _make_crontab_stub_restore_failure(tmp_path: Path) -> Path:
    """`crontab -l` returns a tick line, `crontab -` succeeds (pause OK),
    `crontab <file>` fails (restore fails)."""
    fake_dir = tmp_path / "fake_path"
    fake_dir.mkdir(exist_ok=True)
    stub = fake_dir / "crontab"
    stub.write_text(
        "#!/usr/bin/env bash\n"
        'if [[ "$1" == "-l" ]]; then\n'
        '    echo "0 12 * * * /bin/true"\n'
        "    exit 0\n"
        "fi\n"
        'if [[ "$1" == "-" ]]; then\n'
        "    cat > /dev/null\n"
        "    exit 0\n"
        "fi\n"
        # File arg (restore path) -> fail.
        'if [[ -f "$1" ]]; then\n'
        '    echo "crontab: restore failed" >&2\n'
        "    exit 1\n"
        "fi\n"
        "exit 0\n"
    )
    stub.chmod(0o755)
    return fake_dir


def test_restore_success_removes_backup(tmp_path):
    """Round-3 contract: on successful restore the backup is rm'd + state
    flags are cleared. Behavioural: pause OK + happy crontab; verify no
    backup file under LOG_DIR after the wrapper exits."""
    env_file = _make_env_file(tmp_path)
    wrapper = _make_stub_bin(tmp_path)
    fake_path = _make_crontab_stub(tmp_path, behavior="happy")
    result = _run_wrapper(
        wrapper,
        [
            str(env_file),
            "--skip-hook-snow-stats",
            "--skip-hook-hydrograph-month-season",
            "--skip-hook-short-term-skill",
            "--skip-hook-long-term-skill",
            "--late-start-window-minutes",
            "0",
        ],
        path_prepend=fake_path,
    )
    assert result.returncode == 0, (
        f"expected clean exit, got {result.returncode}\n"
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
    # read_configuration derives ieasyhydroforecast_data_root_dir from the
    # env-file location, so glob anywhere under tmp_path for the backup.
    backups = list(tmp_path.rglob("crontab_backup_*.txt"))
    assert backups == [], f"expected no surviving backup after restore success; found {backups}"


def test_restore_failure_persists_backup_in_log_dir(tmp_path):
    """Round-3 contract: when `crontab <file>` (restore) fails, the
    backup file PERSISTS in LOG_DIR for operator manual recovery + the
    wrapper logs the recovery command pointing at the surviving file."""
    env_file = _make_env_file(tmp_path)
    wrapper = _make_stub_bin(tmp_path)
    fake_path = _make_crontab_stub_restore_failure(tmp_path)
    result = _run_wrapper(
        wrapper,
        [
            str(env_file),
            "--skip-hook-snow-stats",
            "--skip-hook-hydrograph-month-season",
            "--skip-hook-short-term-skill",
            "--skip-hook-long-term-skill",
            "--late-start-window-minutes",
            "0",
        ],
        path_prepend=fake_path,
    )
    combined = result.stdout + result.stderr
    # The restore-failure log message must point the operator at the
    # surviving backup.
    assert "manual recovery: crontab" in combined, (
        f"expected manual-recovery hint; got:\n{combined}"
    )
    assert "PERSISTS at" in combined or "backup file" in combined.lower()
    # read_configuration derives ieasyhydroforecast_data_root_dir from the
    # env-file location, so glob anywhere under tmp_path for the backup.
    backups = list(tmp_path.rglob("crontab_backup_*.txt"))
    assert len(backups) == 1, (
        f"expected exactly one surviving backup after restore failure; found {backups}\n"
        f"output:\n{combined}"
    )
    # The logged path must match the surviving file.
    assert str(backups[0]) in combined, (
        f"expected logged recovery path to match surviving file {backups[0]}; output:\n{combined}"
    )


def test_allow_unpaused_cron_write_failure_persists_backup_in_log_dir(tmp_path):
    """Round-3 contract: when --allow-unpaused-cron + write failure, the
    backup persists in LOG_DIR as a pre-attempt-state reference."""
    env_file = _make_env_file(tmp_path)
    wrapper = _make_stub_bin(tmp_path)
    fake_path = _make_crontab_stub(tmp_path, behavior="write_failure")
    result = _run_wrapper(
        wrapper,
        [
            str(env_file),
            "--allow-unpaused-cron",
            "--skip-hook-snow-stats",
            "--skip-hook-hydrograph-month-season",
            "--skip-hook-short-term-skill",
            "--skip-hook-long-term-skill",
            "--late-start-window-minutes",
            "0",
        ],
        path_prepend=fake_path,
    )
    assert result.returncode == 0
    # read_configuration derives ieasyhydroforecast_data_root_dir from the
    # env-file location, so glob anywhere under tmp_path for the backup.
    backups = list(tmp_path.rglob("crontab_backup_*.txt"))
    assert len(backups) == 1, (
        f"expected one persisting backup under bypass write-failure; found {backups}"
    )


def test_hard_fail_write_failure_removes_backup(tmp_path):
    """Round-3 contract: hard-fail write failure (no bypass) removes the
    partial backup before exit."""
    env_file = _make_env_file(tmp_path)
    wrapper = _make_stub_bin(tmp_path)
    fake_path = _make_crontab_stub(tmp_path, behavior="write_failure")
    result = _run_wrapper(
        wrapper,
        [
            str(env_file),
            "--skip-hook-snow-stats",
            "--skip-hook-hydrograph-month-season",
            "--skip-hook-short-term-skill",
            "--skip-hook-long-term-skill",
            "--late-start-window-minutes",
            "0",
        ],
        path_prepend=fake_path,
    )
    assert result.returncode != 0
    # read_configuration derives ieasyhydroforecast_data_root_dir from the
    # env-file location, so glob anywhere under tmp_path for the backup.
    backups = list(tmp_path.rglob("crontab_backup_*.txt"))
    assert backups == [], (
        f"expected no surviving backup under hard-fail write-failure; found {backups}"
    )


def test_yearly_skill_metrics_exit_propagation_is_final(tmp_path):
    """Strengthen C4 (round-3): the upstream yearly_skill_metrics_recalculation.sh
    must have `exit "$CONTAINER_EXIT_CODE"` as the LAST line that can exit.
    Defends against a future edit that adds a trailing `exit 0` (or any
    other `exit` statement) after the capture line."""
    src = _YEARLY_SKILL.read_text(encoding="utf-8")
    # Find the capture and the propagation lines.
    capture_pos = src.index("CONTAINER_EXIT_CODE=$?")
    propagate_pos = src.index('exit "$CONTAINER_EXIT_CODE"')
    assert capture_pos < propagate_pos
    # After the propagate line, no further `exit` statement may appear.
    after_propagate = src[propagate_pos + len('exit "$CONTAINER_EXIT_CODE"') :]
    # Find any subsequent `exit ` (with space, to skip 'exited' / etc.).
    # Heredocs / comments could in theory contain it; this is a simple
    # textual guard sufficient for the maintenance-time check we want.
    import re

    rogue_exits = re.findall(r"^\s*exit\b", after_propagate, flags=re.MULTILINE)
    assert rogue_exits == [], (
        f"found {len(rogue_exits)} `exit` statement(s) after the CONTAINER_EXIT_CODE "
        "propagation; the exit-code propagation must be the final exit"
    )


def test_restore_failure_forces_nonzero_exit_when_hooks_passed(tmp_path):
    """Round-3 NR2: when hooks succeed (rc=0) but _restore_cron fails,
    _on_exit must bump the wrapper rc to 1 so operational monitoring
    catches the 'cron left paused' condition. This is the operator-safety
    signal — a successful-hooks run with restore failure should NOT exit 0."""
    env_file = _make_env_file(tmp_path)
    wrapper = _make_stub_bin(tmp_path)
    fake_path = _make_crontab_stub_restore_failure(tmp_path)
    result = _run_wrapper(
        wrapper,
        [
            str(env_file),
            "--skip-hook-snow-stats",
            "--skip-hook-hydrograph-month-season",
            "--skip-hook-short-term-skill",
            "--skip-hook-long-term-skill",
            "--late-start-window-minutes",
            "0",
        ],
        path_prepend=fake_path,
    )
    # All hooks were skipped (rc=0 from the orchestration loop), but the
    # restore call failed -> _on_exit must bump rc to 1.
    assert result.returncode == 1, (
        f"expected exit 1 on restore-failure even when hooks passed, "
        f"got {result.returncode}\nstdout={result.stdout}\nstderr={result.stderr}"
    )
    combined = result.stdout + result.stderr
    assert "cron restore FAILED" in combined, (
        f"expected monitoring-signal log line; got:\n{combined}"
    )
