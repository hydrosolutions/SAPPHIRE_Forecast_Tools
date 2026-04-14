# ML-014: Hindcast subprocess has no timeout — maintenance containers hang indefinitely

**Status**: Ready
**Module**: machine_learning, pipeline
**Priority**: High
**Labels**: `bug`, `timeout`, `maintenance`
**Branch**: `fix_ml_hindcast_subprocess_path` (build on top of commit `b9270dc`)

---

## Summary

The three ML maintenance scripts that call `hindcast_ML_models.py` via `subprocess.run()` have no `timeout=` parameter. If the hindcast hangs (e.g. PyTorch inference stalls, API call blocks), the subprocess blocks indefinitely, the container never exits, and the Luigi timeout mechanism cannot kill it reliably.

Observed 2026-04-13: container `maintenance-ml-TSMIXER-PENTAD` ran for 11 hours before exiting. Luigi logs showed `timeout_seconds=None` (display artifact — actual runtime value is 900s from `TimeoutManager` fallback, but the subprocess itself has no internal timeout guard).

## Context

Three files have the same `call_hindcast_script()` pattern:

| File | Subprocess call | Caller catches RuntimeError? |
|------|----------------|------------------------------|
| `recalculate_nan_forecasts.py` (line 93) | `subprocess.run(command, capture_output=True, text=True, env=env)` | Yes (line 307) — catches `(FileNotFoundError, RuntimeError)` |
| `fill_ml_gaps.py` (line 92) | Same | Yes (line 307) — catches `(FileNotFoundError, RuntimeError)`, but only the new timeout RuntimeError can reach here since the function does not raise on non-zero returncode |
| `add_new_station.py` (line 79) | Same | **No** — pre-existing issue |

Branch `fix_ml_hindcast_subprocess_path` (commit `b9270dc`) removed the `IN_DOCKER` branching from all three files. The replacement snippet below only touches the `result = subprocess.run(...)` line, not the command construction.

In all three files, `MODEL_TO_USE` and `PREDICTION_MODE` are **function parameters** (not globals), confirmed in scope at the subprocess call site.

### Why not Popen + read loop?

A `Popen` + `for line in proc.stdout` + `proc.wait(timeout=...)` pattern was considered and rejected during review: the read loop blocks until the process closes stdout (i.e., exits), making `proc.wait(timeout=...)` a no-op — the timeout never fires. `subprocess.run(timeout=...)` internally uses `communicate()` which correctly enforces the timeout.

---

## Implementation Plan

### Phase 1 — Add timeout to subprocess calls

**Goal:** Add `timeout=` parameter to `subprocess.run()` and output logging in all three `call_hindcast_script()` functions.

**Files:**
- `apps/machine_learning/recalculate_nan_forecasts.py` — `call_hindcast_script()`, the `result = subprocess.run(...)` line (line 93)
- `apps/machine_learning/fill_ml_gaps.py` — same (line 92)
- `apps/machine_learning/add_new_station.py` — same (line 79)

**Depends on:** Nothing

**Agents:** 1 (all three files have identical change pattern)

**Allowed changes:** Only the `call_hindcast_script()` function in each file. Do NOT change function signatures, data flow, CSV reading logic, caller error handling, or the existing `print()` calls in the returncode block.

In each file, replace the `result = subprocess.run(...)` line with the timeout-guarded version below. **The existing `if result.returncode == 0:` block that follows must be preserved as-is underneath.** The new output-logging block is inserted between the subprocess call and the returncode check.

Replace:
```python
result = subprocess.run(command, capture_output=True, text=True, env=env)
```

With:
```python
_timeout_raw = os.getenv("SAPPHIRE_HINDCAST_TIMEOUT_SECONDS", "").strip()
hindcast_timeout = int(_timeout_raw) if _timeout_raw else 14400
logger.info("Hindcast timeout: %d seconds", hindcast_timeout)
env["PYTHONUNBUFFERED"] = "1"
try:
    result = subprocess.run(
        command, capture_output=True, text=True, env=env,
        timeout=hindcast_timeout,
    )
except subprocess.TimeoutExpired:
    raise RuntimeError(
        f"Hindcast subprocess timed out after {hindcast_timeout}s "
        f"for {MODEL_TO_USE} {PREDICTION_MODE}"
    )
```

Then insert the following output logging **before** the existing `if result.returncode == 0:` block (do NOT replace or move that block):
```python
if result.stdout:
    for line in result.stdout.splitlines():
        logger.info("[hindcast] %s", line)
if result.stderr:
    for line in result.stderr.splitlines():
        logger.warning("[hindcast stderr] %s", line)
```

The resulting code structure in each file must be:
1. Timeout parse + `subprocess.run(..., timeout=...)` with `TimeoutExpired` guard
2. Output logging (new)
3. Existing `if result.returncode == 0:` / `else:` block (unchanged — including existing `print()` calls)
4. Existing CSV read + return (unchanged)

**Note on per-file differences:** `recalculate_nan_forecasts.py` raises `RuntimeError` on non-zero returncode (line ~107). `fill_ml_gaps.py` and `add_new_station.py` only `print()` the error and fall through to CSV read — this is a pre-existing gap, not addressed by this issue. Do NOT add error handling for non-zero returncode — that is out of scope. Do NOT add try/except around the call sites in `add_new_station.py` — an uncaught timeout RuntimeError crashing the script is **intended behavior** (better than hanging indefinitely).

**Key design decisions:**
- Keeps `subprocess.run()` (no Popen refactor) — minimal change, `timeout=` works correctly via internal `communicate()` which calls `process.kill()` on timeout
- Keeps `capture_output=True` — preserves separate `result.stderr` in all existing failure log messages
- Adds `PYTHONUNBUFFERED=1` to env — ensures child process flushes output before timeout kill
- Default timeout 14400s (4 hours) — accommodates 55 stations × months of hindcast data on CPU
- Configurable via `SAPPHIRE_HINDCAST_TIMEOUT_SECONDS` env var — ops teams can override without code change
- Env var parse handles empty string gracefully (`int("")` would crash; the `strip() + conditional` pattern avoids this)

**Acceptance criteria:**
- `subprocess.run()` has `timeout=hindcast_timeout` in all three files
- `TimeoutExpired` → `RuntimeError` guard in all three files
- `PYTHONUNBUFFERED=1` added to env in all three files
- Output logging block inserted before existing `if result.returncode` block
- Existing `if result.returncode` blocks and `print()` calls unchanged
- No changes to function signatures, imports (beyond what's already present), or caller code

### Phase 2 — Write tests

**Goal:** Add tests for timeout behavior, env var parsing, and output logging in `call_hindcast_script()`.

**Files:**
- `apps/machine_learning/test/test_recalculate_nan_api_write.py` — add new test class `TestCallHindcastScriptTimeout` adjacent to the existing `TestCallHindcastScriptRaisesOnFailure` (line ~437). Existing tests already cover non-zero returncode and successful CSV read; new tests cover timeout-specific behavior only.

**Depends on:** Phase 1

**Agents:** 1

**Existing tests to be aware of (do NOT duplicate):**
- `TestCallHindcastScriptRaisesOnFailure.test_raises_runtime_error_on_nonzero_returncode` (line ~450)
- `TestCallHindcastScriptRaisesOnFailure.test_success_reads_csv` (line ~480)

**Note:** No `conftest.py` exists in `apps/machine_learning/test/` — fixtures are managed inline per test file.

**Test cases to implement:**
- Subprocess timeout fires correctly (mock `subprocess.run` to raise `TimeoutExpired`, verify `RuntimeError` raised)
- Env var parsing: empty string → 14400, valid int → parsed, missing → 14400
- Hindcast stdout/stderr appear in logger output (use `caplog` or mock logger)

**Acceptance criteria:**
- All new tests pass with `SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning`
- Zero unexpected skips
- No duplication with existing `TestCallHindcastScriptRaisesOnFailure` tests

### Phase 3 — Verify

**Goal:** Run full test suite, verify zero failures and zero unexpected skips.

**Depends on:** Phase 2

**Agents:** 0 (orchestrator runs tests directly)

### Dropped: Pipeline env var passthrough (original Phase 2)

**Reason:** `TimeoutManager` defaults to 900s for `MLMaintenance` (no `timeout_config.yaml` in repo, no `MLMaintenance` entry). Passing `SAPPHIRE_HINDCAST_TIMEOUT_SECONDS=900` into the container would **override** the script's 14400s default — a regression that kills hindcasts after 15 minutes instead of 4 hours. The 14400s default in the scripts is sensible. The env var exists for operators who explicitly need a different value. This phase can be revisited after `timeout_config.yaml` is properly configured with an `MLMaintenance` entry (e.g., `kghm_aws_override: 14400`).

### Dependency Graph

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P2"], "parallel_agents": 0 }
  }
}
```

---

## Risks Reviewed

| Risk | Severity | Mitigation |
|------|----------|------------|
| `subprocess.TimeoutExpired` propagates as `RuntimeError` — callers must catch it | Low | `recalculate_nan_forecasts.py` and `fill_ml_gaps.py` already catch `RuntimeError` at call sites. `add_new_station.py` does not — timeout will crash the script. This is **intended**: crashing is better than hanging indefinitely, and mirrors what would happen with `FileNotFoundError` today. |
| Empty string env var (`SAPPHIRE_HINDCAST_TIMEOUT_SECONDS=""`) | Low | Handled by `strip() + conditional` parse pattern — falls back to 14400. |
| `PYTHONUNBUFFERED=1` changes child buffering behavior | Negligible | Only affects when output is flushed, not what output is produced. No data flow impact. |
| 14400s default may be too short for very large station sets | Low | Configurable via env var. Operators on large deployments can increase. |
| Double stderr logging in `recalculate_nan_forecasts.py` on failure | Negligible | Cosmetic — stderr logged once as `[hindcast stderr]` (new), once in `logger.error(... Stderr: ...)` (existing). Not a correctness issue. |

---

## Testing

### Test Cases

New tests in `apps/machine_learning/test/test_recalculate_nan_api_write.py` (class `TestCallHindcastScriptTimeout`, adjacent to existing `TestCallHindcastScriptRaisesOnFailure`):

- [ ] Subprocess timeout fires correctly (mock `subprocess.run` to raise `TimeoutExpired`, verify `RuntimeError` raised with correct message)
- [ ] Env var parsing: empty string → 14400, valid int → parsed, missing → 14400
- [ ] Hindcast stdout/stderr logged via `logger.info`/`logger.warning` (verify with `caplog` or mock logger)

Already covered by existing tests (do NOT duplicate):
- Successful hindcast (returncode 0) → `test_success_reads_csv`
- Failed hindcast (non-zero exit) raises `RuntimeError` → `test_raises_runtime_error_on_nonzero_returncode`

### Testing Commands

```bash
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning
```

---

## Acceptance Criteria

- [ ] All three `call_hindcast_script()` functions have `timeout=` on `subprocess.run()`
- [ ] Timeout value is configurable via `SAPPHIRE_HINDCAST_TIMEOUT_SECONDS` env var
- [ ] Empty/missing env var falls back to 14400s without crashing
- [ ] Hindcast output (stdout/stderr) is logged after subprocess completion
- [ ] Existing `if result.returncode` blocks and `print()` calls preserved unchanged in all three files
- [ ] No changes to function signatures, caller code, or CSV reading logic
- [ ] Unit tests cover timeout, env var parsing, output logging, and returncode behavior
- [ ] Existing successful hindcast data flow is unaffected
- [ ] All tests pass with zero unexpected skips

---

## Code Review Findings (2026-04-14)

### RESOLVED — Incorporated into plan above

#### C1: Branch prerequisite not merged — stale line numbers → FIXED

Line numbers corrected to current `maxat_sapphire_2` code. Context section now notes `IN_DOCKER` branches are still present and explains why the replacement snippet is safe regardless.

#### C2: Timeout race condition → N/A (Phase 2 dropped)

Would have applied if `self.timeout_seconds` were passed into the container. No longer relevant since Phase 2 was dropped.

#### C3: Phase 2 makes things actively worse → FIXED (Phase 2 dropped)

`TimeoutManager` defaults to 900s for `MLMaintenance`. Passing that into the container would override the script's 14400s default — a regression. Phase 2 dropped entirely. Can be revisited after `timeout_config.yaml` has a proper `MLMaintenance` entry.

#### M1: Behavioral inconsistency → FIXED (explicit scope constraint added)

Plan now states: "Do NOT add error handling for non-zero returncode — that is out of scope."

#### M2: Uncaught RuntimeError in add_new_station.py → FIXED (documented as intended)

Plan now explicitly states crash-on-timeout is intended behavior and adds constraint: "Do NOT add try/except around the call sites in `add_new_station.py`."

### MINOR — No action required

#### m1: Double stderr logging in `recalculate_nan_forecasts.py`

On failure, stderr will be logged twice: once by the new `logger.warning("[hindcast stderr]")` block, and once by the existing `logger.error("... Stderr: %s", result.stderr)` at line ~107. Cosmetic only — correctly noted in the risk table.

#### m2: All prerequisites confirmed present

- `import subprocess` — present in all three files ✓
- `import os` — present in all three files ✓
- `logger` — defined in all three files ✓
- `MODEL_TO_USE` / `PREDICTION_MODE` — function parameters in all three files, in scope at `subprocess.run()` call ✓
- `env = os.environ.copy()` — present in all three files, `env["PYTHONUNBUFFERED"] = "1"` will work ✓

---

## Related Issues

- **ML-002**: Hindcast subprocess root cause (failure vectors inside `hindcast_ML_models.py`) — complementary, separate scope
- **Branch `fix_ml_hindcast_subprocess_path`** (commit `b9270dc`): Docker path fix — prerequisite, already complete

## Files Modified

| File | Change |
|------|--------|
| `apps/machine_learning/recalculate_nan_forecasts.py` | Add timeout + log output in `call_hindcast_script()` |
| `apps/machine_learning/fill_ml_gaps.py` | Same |
| `apps/machine_learning/add_new_station.py` | Same |
| `apps/machine_learning/test/test_recalculate_nan_api_write.py` | Add `TestCallHindcastScriptTimeout` class for timeout, env var parsing, output logging |
