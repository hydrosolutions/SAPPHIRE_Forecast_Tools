# PREPQ-016: `preprocessing_runoff.py`: three of seven `sys.exit(1)` sites are unreachable dead code

**Status**: Draft (2026-08-20)
**Module**: `apps/preprocessing_runoff/preprocessing_runoff.py`, `apps/preprocessing_runoff/src/src.py`
**Priority**: Low — the guarded failure mode is never lost (a write failure still surfaces, just
as an uncaught traceback instead of a graded exit code); no data-loss or silent-success risk.
**Labels**: `preprocessing_runoff`, `dead-code`, `exit-code-hygiene`
**Found**: 2026-08-20, filed as a follow-up while implementing INFRA-037, which separately
documents these `sys.exit(1)` sites as part of its diagnosis of `run_locally.sh`'s fail-fast
behavior.
**Related**: **INFRA-037** (the row in `module_issues.md` currently characterizes `:653` as the
sole dead site and `:523`/`:536` as "reachable" signaling a CSV-only failure; this draft goes
further after direct verification of the two `to_csv` helpers — see "Note on INFRA-037's framing"
below).

---

## Verified against source (2026-08-20, this branch)

`preprocessing_runoff.py` has seven `sys.exit(1)` call sites (`:120`, `:161`, `:449`, `:468`,
`:523`, `:536`, `:653`). This issue is scoped to three of them: `:523`, `:536`, `:653`.

### Sites 1 and 2 — the `to_csv` guards (`:516-523`, `:529-536`)

```python
ret = src.write_daily_time_series_data_to_csv(
    data=filtered_data, column_list=["code", "date", "discharge"], mode=mode
)
if ret is None:
    logger.info("Daily time series data written successfully.")
else:
    logger.error("Failed to write daily time series data.")
    sys.exit(1)                                                    # :523
...
ret = src.write_daily_hydrograph_data_to_csv(
    data=hydrograph, column_list=hydrograph.columns.tolist(), mode=mode
)
if ret is None:
    logger.info("Daily hydrograph data written successfully.")
else:
    logger.error("Failed to write daily hydrograph data.")
    sys.exit(1)                                                    # :536
```

Both helpers (`src/src.py:4587-4675` and `:4678-4758`) end their write step identically:

```python
try:
    ret = data.reset_index(drop=True)[column_list].to_csv(output_file_path, index=False)
    if ret is None:
        logger.info(f"[OUTPUT] ... written: {output_file_path}")
        return ret
    else:
        logger.error(f"[OUTPUT] Failed to write ...: {output_file_path}")
except Exception as e:
    logger.error(f"[OUTPUT] Failed to write ...: {output_file_path} - {e}")
    raise e
```

`DataFrame.to_csv(path, index=False)` — called with a filesystem path and no buffer — **always
returns `None`** per pandas' own contract (it returns a string only when `path_or_buf` is `None`,
which never happens here). So on the non-exception path, `ret` inside the helper can only be
`None`; the `else` branch (`logger.error(...)`) is itself dead in normal operation, and even if it
somehow ran, it **falls off the end of the function with no `return`**, which means the helper
returns `None` (Python's implicit `None`) regardless. The only way either helper returns anything
other than `None` would require `to_csv` to violate its own documented return contract, which
these functions do not defend against.

The single non-`None` behavior a write failure actually produces is the `except Exception as e:
... raise e` branch — a re-raised exception, not a returned non-`None` value. That propagates
straight out of the `ret = src.write_daily_..._to_csv(...)` call in `preprocessing_runoff.py`
(there is no `try/except` around either call site) as an **uncaught traceback**, which the two
`if ret is None: ... else: sys.exit(1)` guards never get a chance to see — control never reaches
the `else` branch on a real failure.

### Site 3 — the final `ret` check (`:650-653`)

```python
if ret is None:
    sys.exit(0)  # Success
else:
    sys.exit(1)  # Failure                                        # :653
```

By this point in `main()`, `ret` has been assigned exactly twice: at `:516` and `:529` — each
immediately followed by its own `if ret is None: ... else: sys.exit(1)` guard (sites 1 and 2
above). Per the analysis above, `ret` can only be `None` at each of those assignment points on the
non-exception path (the only path that reaches `:650` at all — a raised exception at either write
call exits the process before `:650` is ever reached). So the final check at `:650` is testing a
variable that is provably always `None` whenever execution reaches it; the `else: sys.exit(1)` at
`:653` is unreachable.

## Consequence

A CSV write failure at `write_daily_time_series_data_to_csv` or
`write_daily_hydrograph_data_to_csv` never produces the graded `sys.exit(1)` these three sites
suggest it would. It surfaces as an **uncaught Python traceback** with whatever exit code the
interpreter assigns on an unhandled exception (conventionally 1, but not the deliberate,
documented `sys.exit(1)` an operator or `run_locally.sh`'s exit-code taxonomy might expect from
reading this code). Any documentation or diagnostic runbook that says "a CSV write failure here
exits 1 with the logged error message" is describing the intended shape, not the actual code path
— the actual failure mode is a traceback that never reaches the `logger.error(...)` lines at
`:522`/`:535` either, since those are on the same dead `else` branches.

## Note on INFRA-037's framing

The `INFRA-037` row in `module_issues.md` (filed the same day, same investigation) characterizes
`:523`/`:536` as "reachable" and signaling "a CSV failure only, never an API one," with `:653`
called out as the sole dead site. That framing is not wrong about *what the site is for* (CSV vs.
API failure) but understates reachability: this draft's direct verification of both `to_csv`
helpers' source shows `:523` and `:536` are, like `:653`, unreachable on the non-exception path —
the "realistic failure ... is an uncaught traceback" caveat INFRA-037 already carries is the
correct characterization for all three sites, not just two of them. This draft does not modify the
INFRA-037 row; flagging the discrepancy here for whoever picks up either issue.

## Proposed fix

Not proposed in this draft — options range from removing the three dead sites and wrapping the
`to_csv` calls in `preprocessing_runoff.py` with an explicit `try/except` that calls `sys.exit(1)`
after logging (restoring the graded-exit-code intent the dead code suggests), to leaving the
uncaught-traceback behavior as-is and only removing the misleading dead branches. This is a design
choice for whoever implements the fix, not dictated by this issue.

## Out of scope

- Sites `:120`, `:161`, `:449`, `:468` — not verified as dead by this draft; do not assume they
  share this defect without separately checking their guarded variables.
- Any change to `write_daily_time_series_data_to_csv` / `write_daily_hydrograph_data_to_csv`'s
  API-write behavior (already explicitly non-blocking, unrelated to this issue).
- INFRA-037's broader diagnosis of `run_locally.sh`'s fail-fast default — this issue is scoped to
  the exit-site reachability claim only.

## Acceptance criteria

- [ ] Either the three dead `sys.exit(1)` sites are removed as genuinely unreachable, or the code
      is restructured (e.g., an explicit `try/except` around the `to_csv` calls) so that a write
      failure actually reaches a graded `sys.exit(1)` instead of an uncaught traceback — the
      chosen direction is recorded here before implementation.
- [ ] A test exercising a CSV write failure (e.g., an unwritable output path) asserts the actual
      resulting behavior matches what the code now claims (either a caught, graded exit or a
      documented uncaught exception — not silent success).
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff` — zero failures,
      zero unexpected skips.
