# Fix silent timeout failure in execute_with_retries

**Priority**: High
**Module**: pipeline
**Status**: Draft

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

### `apps/pipeline/luigi.cfg` — safety net

Add `check_complete_on_run = true` so Luigi always verifies `output().exists()`
after `run()` returns, even for non-timeout cases:

```ini
[worker]
check_complete_on_run = true
```

This catches any future scenario where `run()` returns without raising but
the marker wasn't written.

## Verification

1. `SAPPHIRE_TEST_ENV=True bash apps/run_tests.sh pipeline` — all tests pass
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

## Open Question

The timeout values come from `TimeoutManager` (default 900s). If ML
maintenance tasks legitimately need more than 900s, the timeout should be
increased in the timeout config YAML rather than disabled. Check server logs
to confirm whether the ML containers are actually timing out.
