# Fix silent timeout failure in execute_with_retries

**Priority**: High
**Module**: pipeline
**Status**: Review

## Problem

The maintenance pipeline reports all 6 `MLMaintenance` tasks as "ran
successfully" but then `PostProcessingMaintenance` fails with:

```
RuntimeError: Unfulfilled dependencies at run time:
  MLMaintenance_None_TFT_PENTAD_... (/.../maintenance_ml_TFT_PENTAD_2026-04-15.marker),
  ... (all 6 markers missing)
```

Luigi execution summary shows "6 ran successfully" for MLMaintenance tasks,
yet their marker files do not exist.

## Root Cause

`execute_with_retries()` has a timeout exit path that silently swallows
failure. When a Docker container times out (exit_status == 124):

1. `execute_with_retries()` sets `final_status = "Timeout"` and `break`s
   out of the retry loop — **no marker file is written, no exception raised**
2. Returns `("Timeout", "Task timed out...")` to the caller
3. `MLMaintenance.run()` assigns the return value but **never checks it** —
   falls through, returns `None`
4. Luigi worker: `check_complete_on_run` defaults to `False`, so Luigi marks
   the task DONE purely because `run()` didn't raise — **without checking
   `output().exists()`**
5. Execution summary shows "ran successfully" (= `run()` returned, not
   that output exists)
6. When downstream `PostProcessingMaintenance` starts, its pre-run check
   calls `os.path.exists()` on each dependency marker → all 6 missing →
   `RuntimeError: Unfulfilled dependencies at run time`

### Additional finding: dead code

Lines 402-406 in `execute_with_retries()` contain a `send_failure_notification`
call and a second `break` that are unreachable — they appear after the `break`
at line 400. This was likely intended to notify on timeout but was placed
after the loop exit.

### Broader impact: 18 of 20 callers affected

Only 2 of 20 callers of `execute_with_retries` check its return value
(`PreprocessingGatewayQuantileMapping` and `ConceptualModel`). The remaining
18 — including all maintenance tasks, operational forecast tasks, and initial
setup tasks — silently discard the timeout status. The fix (raising on timeout)
benefits the entire pipeline, not just `MLMaintenance`.

### Why operational pipelines don't hit this

Pentadal/decadal use `--workers 1` (sequential execution, same process).
Even if a timeout occurred, the `check_unfulfilled_deps` pre-run check at
`worker.py:184` would catch it inline before dispatching the downstream task.
With `--workers 6`, the scheduler dispatches the downstream task to a
separate subprocess that discovers the missing markers independently.

However, the primary fix is making the timeout raise an exception — not
adjusting worker count or disabling the pre-run check.

## Fix

### `apps/pipeline/pipeline_docker.py` — `execute_with_retries()`

In the timeout branch (exit_status == 124), raise a `RuntimeError` instead
of returning silently. Move the `send_failure_notification` call before the
raise so timeout failures are properly reported:

**Current code (lines 397-407):**
```python
            if exit_status == 124:
                final_status = "Timeout"
                details = f"Task timed out after {self.timeout_seconds} seconds"
                break

                # DEAD CODE — unreachable
                self.send_failure_notification(...)
                break
```

**Fixed code:**
```python
            if exit_status == 124:
                final_status = "Timeout"
                details = f"Task timed out after {self.timeout_seconds} seconds"
                self.send_failure_notification(
                    f"Task timed out after {self.timeout_seconds}s "
                    f"on attempt {attempts}/{self.max_retries}",
                    logs,
                )
                raise RuntimeError(
                    f"Task timed out after {self.timeout_seconds} seconds "
                    f"(attempt {attempts}/{self.max_retries})"
                )
```

This ensures:
- Luigi sees the exception and marks the task FAILED (not DONE)
- A failure notification email is sent (the current dead code's intent)
- The execution summary correctly shows the task as failed
- Downstream tasks are never dispatched for tasks that timed out

### `apps/pipeline/luigi.cfg` and `temp_luigi.cfg` — safety net

`apps/pipeline/luigi.cfg` is **shadowed at runtime**: every run script generates
a `temp_luigi.cfg` and mounts it over `/app/luigi.cfg` inside the container,
so edits to the repo file alone have no effect on deployed runs.

The `[worker]` section must therefore be appended to the generated
`temp_luigi.cfg` in all 7 run scripts:

- `bin/run_daily_maintenance.sh`
- `bin/run_periodic_maintenance.sh`
- `bin/run_pentadal_forecasts.sh`
- `bin/run_decadal_forecasts.sh`
- `bin/run_long_term_forecasts.sh`
- `bin/run_preprocessing_gateway.sh`
- `bin/run_preprocessing_runoff.sh`

In 6 of the 7 scripts, replace the existing `cat > temp_luigi.cfg` heredoc with:

```bash
cat > temp_luigi.cfg <<EOF
[core]
scheduler_host = ${LUIGI_SCHEDULER_HOST}
scheduler_port = ${LUIGI_SCHEDULER_PORT}

[worker]
check_complete_on_run = true
EOF
```

`bin/run_preprocessing_runoff.sh` uses `echo` lines and `default_scheduler_url`
instead of a heredoc. Append the `[worker]` section after the existing lines:

```bash
echo "[core]" > temp_luigi.cfg
echo "default_scheduler_url = $LUIGI_SCHEDULER_URL" >> temp_luigi.cfg
echo "" >> temp_luigi.cfg
echo "[worker]" >> temp_luigi.cfg
echo "check_complete_on_run = true" >> temp_luigi.cfg
```

Also add `check_complete_on_run = true` to `apps/pipeline/luigi.cfg` so that
local and test runs — where the `temp_luigi.cfg` override does not apply —
also benefit from the safety net:

```ini
[worker]
check_complete_on_run = true
```

This catches any future scenario where `run()` returns without raising but
the marker wasn't written.

### `apps/pipeline/tests/test_docker_task_base.py` — update timeout test

`test_timeout_stops_retrying` (lines 218–236) currently asserts a return value:

```python
status, details = task.execute_with_retries(timeout_func)
assert status == "Timeout"
assert call_count == 1
```

After the fix, `execute_with_retries` raises instead of returning. Update the
test to:

```python
def test_timeout_stops_retrying(self, mock_env, tmp_path):
    """Exit code 124 (timeout) → no retry, raises RuntimeError."""
    from pipeline_docker import PreprocessingRunoff

    task = PreprocessingRunoff()
    task.max_retries = 3
    log_path = str(tmp_path / "test_log.txt")
    task.docker_logs_file_path = log_path

    call_count = 0

    def timeout_func(attempt):
        nonlocal call_count
        call_count += 1
        return ("cid_123", 124, "timeout logs")

    with patch.object(task, "send_failure_notification"):
        with pytest.raises(RuntimeError, match="timed out"):
            task.execute_with_retries(timeout_func)

    assert call_count == 1  # No retry after timeout
```

This also patches `send_failure_notification` to avoid requiring email
infrastructure in tests.

Add a second test to verify the notification is actually called with correct
arguments (the whole point of fixing the dead code):

```python
def test_timeout_sends_failure_notification(self, mock_env, tmp_path):
    """Exit code 124 (timeout) → send_failure_notification is called once
    with a message containing the timeout seconds and attempt info."""
    from pipeline_docker import PreprocessingRunoff

    task = PreprocessingRunoff()
    task.max_retries = 3
    task.timeout_seconds = 900
    log_path = str(tmp_path / "test_log.txt")
    task.docker_logs_file_path = log_path

    def timeout_func(attempt):
        return ("cid_123", 124, "timeout logs")

    with patch.object(task, "send_failure_notification") as mock_notify:
        with pytest.raises(RuntimeError, match="timed out"):
            task.execute_with_retries(timeout_func)

    mock_notify.assert_called_once()
    call_args = mock_notify.call_args
    message = call_args[0][0]
    logs_arg = call_args[0][1]
    assert "900" in message
    assert "1" in message  # attempt 1
    assert "3" in message  # max_retries
    assert logs_arg == "timeout logs"
```

## Verification

1. Update `test_timeout_stops_retrying` (see above), then run
   `SAPPHIRE_TEST_ENV=True bash apps/run_tests.sh pipeline` — all tests pass
2. Deploy to server, rebuild pipeline image
3. Run `bin/run_daily_maintenance.sh`:
   - If ML tasks complete within timeout: markers written, PostProcessing runs
   - If ML tasks timeout: they are marked FAILED (not DONE), clear error in
     summary, failure notification sent
4. Check that execution summary no longer shows timed-out tasks as
   "ran successfully"

## Risk Assessment

**Low risk**: The timeout path was already broken (dead code, silent failure).
Raising an exception restores the intended behavior — a timed-out task should
be treated as failed, not successful.

`check_complete_on_run = true` adds a safety net: if `run()` returns
normally but the marker wasn't written, Luigi raises `TaskException("Task
finished running, but complete() is still returning false")` instead of
silently marking DONE. This is strictly safer than the default `false`.

**`finally` block (pre-existing)**: `log_task_timing` (line 424-432) does
file I/O inside `finally`. If the log write fails (disk full, permissions),
the `OSError` replaces the in-flight `RuntimeError` — the timeout error is
silently lost. This affects the existing max-retries raise too, not just this
fix. Optional hardening: wrap `log_task_timing` in `try/except OSError` inside
the `finally` block. Not a blocker for this fix.

**Script changes**: The `temp_luigi.cfg` modification touches 7 shell scripts.
Each change is identical (appending `[worker]\ncheck_complete_on_run = true`)
and the setting is strictly additive — it only triggers when `run()` returns
normally but the output marker is missing, which should never happen in correct
operation.

## Open Questions

### Timeout values

The timeout values come from `TimeoutManager` (default 900s). If ML
maintenance tasks legitimately need more than 900s, the timeout should be
increased in the timeout config YAML rather than disabled. Check server logs
to confirm whether the ML containers are actually timing out.

### Should timeouts be retried?

Currently the timeout branch (exit_status == 124) exits the retry loop
immediately — no retry. Non-timeout failures (exit_status != 0, != 124) get
`max_retries` attempts with `retry_delay` between each. This asymmetry may
be intentional (a container that times out at 900s will likely time out again)
or may be a second bug (transient resource contention could cause a one-off
timeout that succeeds on retry).

This fix intentionally preserves the existing no-retry-on-timeout behavior
to keep the change minimal and focused on the silent-failure bug. If retries
are desired later, the timeout branch should mirror the non-timeout retry
pattern: continue the loop, sleep `retry_delay`, and only raise after all
attempts are exhausted.
