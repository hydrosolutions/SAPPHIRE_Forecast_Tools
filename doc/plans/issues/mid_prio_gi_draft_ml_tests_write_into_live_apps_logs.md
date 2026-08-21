# ML-023: Four `machine_learning` test files write into the live operator log at import time

**Status**: Draft (2026-08-21)
**Module**: `apps/machine_learning/test/` (`test_fill_ml_gaps.py`, `test_write_forecast.py`,
`test_recalculate_nan_api_write.py`, `test_fill_ml_gaps_null_loop.py`)
**Priority**: Medium — no data loss, but it corrupts the operator's evidence trail for which days
actually ran, and gets worse every time `run_tests.sh` runs.
**Labels**: `machine_learning`, `test-hygiene`, `logs`
**Found**: 2026-08-21, filed as a follow-up while implementing INFRA-037. **Pre-existing** — not
introduced by INFRA-037; INFRA-037's own new test file (`test_mode_error_messages.py`) is clean
under adversarial import orderings (see § Known-good fix pattern below).
**Related**: **INFRA-037**'s own test file solves this problem for the same three modules within
one file — this issue is about the four sibling files that don't. `apps/logs/` rotation is
per-run, not per-day (30 backups = months of history, and the filename set is what an operator
reads to know which days actually ran — a fact surfaced while investigating **PP-045**, not part
of that issue's own scope), which is why this matters operationally, not just as test hygiene.

---

## Defect

`fill_ml_gaps.py`, `make_forecast.py`, and `recalculate_nan_forecasts.py` each set up
module-level logging **at import time** using a path relative to the process's current working
directory:

```python
# fill_ml_gaps.py:23,32 / recalculate_nan_forecasts.py:20,31 / make_forecast.py:79,88
from logging.handlers import TimedRotatingFileHandler
...
logs_dir = "logs"
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)
file_handler = TimedRotatingFileHandler("logs/log", when="midnight", interval=1, backupCount=30)
```

The mandated test runner, `run_tests.sh`, invokes pytest with `cd apps` first, so `"logs/log"`
resolves to `apps/logs/log` — the **live operator log directory** that `run_locally.sh` writes to
and that operators read to see which days actually ran (per the `apps/logs` rotation-is-per-run
convention).

Four test files import one or more of these modules with **no guard at all** around the import:

| File | Unguarded import |
|---|---|
| `test_fill_ml_gaps.py` | `import fill_ml_gaps` (`:46`) |
| `test_fill_ml_gaps_null_loop.py` | `import fill_ml_gaps` (`:47`) |
| `test_write_forecast.py` | `import make_forecast` (`:65`) |
| `test_recalculate_nan_api_write.py` | `import recalculate_nan_forecasts` (`:39`) |

Each of these imports triggers the module-level `TimedRotatingFileHandler("logs/log", ...)`
construction with the *real* `apps/` as cwd, binding the handler's `.baseFilename` to the live
`apps/logs/log`. Any subsequent logging through that module's `logger` — including logging that
happens as a side effect of running the file's own tests — appends to the real operator log.

**Empirically confirmed** per file: the md5 of `apps/logs/log` changes after running each file
alone (`SAPPHIRE_TEST_ENV=True pytest test/<file> -q` from `apps/`), confirming each one on its
own reaches and exercises the live-bound handler, not just imports it inertly.

## Why this matters

`apps/logs/` rotation is per-**run**, not per-day, with 30 backups — the set of files present in
that directory is used as operational evidence of which days the pipeline actually ran. Test noise appended to `apps/logs/log` pollutes that evidence trail: an
operator diagnosing "did `daily` run yesterday" by inspecting `apps/logs/` cannot distinguish a
real pipeline entry from a test run's leakage. This gets worse on every `run_tests.sh` invocation,
not just once.

## Known-good fix pattern (already landed, not yet applied to these four files)

`apps/machine_learning/test/test_mode_error_messages.py` (added by INFRA-037) imports all three of
the same modules (`fill_ml_gaps`, `make_forecast`, `recalculate_nan_forecasts`) and is clean under
adversarial import orderings. It uses two layers, not one:

1. **A cwd redirect around the import**: `os.chdir()` into a `tempfile.TemporaryDirectory()`,
   import the three modules inside the `with` block, then `os.chdir()` back in a `finally`. This
   makes `"logs/log"` resolve inside the throwaway tempdir instead of the repo working tree — but
   **only for whichever import runs first** (see § Order-dependence trap below).
2. **An autouse fixture that detaches the live handler regardless of import order**
   (`_detach_live_apps_logs_handlers`): for every test in the file, it walks each module's
   `logger.handlers`, finds any handler whose resolved `baseFilename` equals the real
   `apps/logs/log`, removes it for the duration of the test, and reattaches it afterward. This is
   the layer that actually matters under the real test runner, per § Order-dependence trap.

A shared `conftest.py` in `apps/machine_learning/test/` does not currently exist (verified: no
`conftest.py` in that directory) — one could apply the second layer (the autouse
handler-detach fixture) to every file in the directory at once, rather than requiring each of the
four files to reimplement it individually.

## The order-dependence trap (cost two failed attempts on INFRA-037)

A cwd redirect **alone** only protects the module that **this file** imports **first**. Under
pytest's default alphabetical collection order in this directory, `test_fill_ml_gaps.py` and
`test_fill_ml_gaps_null_loop.py` both import `fill_ml_gaps` — unguarded — *before*
`test_mode_error_messages.py` is even collected. By the time `test_mode_error_messages.py`'s own
cwd-redirected `import fill_ml_gaps` runs, Python's module cache already holds `fill_ml_gaps` bound
to the real `apps/logs/log` from the earlier, unguarded import — the redirect has no effect on an
already-cached module. `test_mode_error_messages.py`'s own docstring documents this explicitly for
`fill_ml_gaps` and is the reason its autouse fixture (layer 2 above), not the cwd redirect (layer
1), is the actual guard under the mandated runner. A fix to these four files that adds only a cwd
redirect, without the handler-detach fixture (or an equivalent), will look correct when the file is
run alone and still leak when run as part of the full suite in whichever collection order
`fill_ml_gaps`/`make_forecast`/`recalculate_nan_forecasts` first get imported unguarded.

## Verification pattern worth copying (not the naive version)

`apps/pipeline/tests/test_run_locally_orchestration.py` has an autouse `protect_real_apps_logs`
fixture that snapshots `apps/logs/` before and after every test in that file and fails if anything
changed. Its snapshot maps each directory entry to `(size, mtime_ns)`, not just the entry's name.
An earlier, **name-only** version of that fixture (comparing only which filenames exist) silently
failed to detect a test that **appends** to an *already-existing* file — exactly the failure mode
here, since `apps/logs/log` already exists before any test runs, so a test that only appends to it
changes no filename. Whoever implements the fix for this issue should adapt the `(size, mtime_ns)`
version of that fixture, not redo the naive name-only one.

## Scope boundaries

- This issue is scoped to the four files listed above. `test_mode_error_messages.py` is not in
  scope — it already implements the fix.
- Not proposing here whether the fix belongs in a shared `conftest.py`, in each file individually,
  or as a change to the three production modules' logging setup (e.g., resolving `"logs/log"`
  against an absolute path derived from the module's own file location instead of cwd) — that is a
  design choice for whoever implements it.
- Does not cover any other `apps/machine_learning/test/*.py` file not listed above; not verified
  whether other files in that directory import these three modules through some other path.

## Acceptance criteria

- [ ] Running each of the four files alone, and running the full `apps/machine_learning/test/`
      suite together (in default alphabetical collection order), leaves `apps/logs/log` byte-for-byte
      unchanged (size and mtime).
- [ ] The fix is verified under adversarial import ordering, not just "passes when run alone" — per
      § Order-dependence trap, a cwd-redirect-only fix is known to look correct in isolation and
      still leak under the real runner's collection order.
- [ ] A regression test (per-file or shared) fails loudly if the live-log guard is ever removed —
      mirroring `test_mode_error_messages.py`'s own
      `test_cwd_redirect_keeps_module_logging_out_of_real_apps_logs` and
      `_detach_live_apps_logs_handlers` guards, or the `(size, mtime_ns)` snapshot pattern from
      `apps/pipeline/tests/test_run_locally_orchestration.py`.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning` — zero failures, zero
      unexpected skips.
