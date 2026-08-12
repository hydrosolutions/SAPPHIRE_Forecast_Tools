# PP-053 — `UnboundLocalError` in `save_monthly_skill_metrics` when CSV env vars are unset and `SAPPHIRE_CONSISTENCY_CHECK=true`

**Status**: Draft
**Module**: postprocessing_forecasts (`file_writer.py`, `recalculate_skill_metrics.py`)
**Priority**: Medium (narrow trigger, wide blast radius — see "Severity reasoning")
**Labels**: `reliability`, `crash-bug`, `edge-case`

---

## Summary

`save_monthly_skill_metrics` (`apps/postprocessing_forecasts/src/file_writer.py:382`)
only binds the local variable `filepath` inside its CSV-write branch. When either of
the two required env vars is unset, that branch is skipped — but the later
`SAPPHIRE_CONSISTENCY_CHECK` branch references `filepath` unconditionally, raising
`UnboundLocalError` at runtime. Because the caller
(`recalculate_skill_metrics.py:361`) does not wrap this specific call in a
`try/except`, and `recalculate_skill_metrics()` has no top-level exception handler
either, the exception propagates all the way out of the script: quarterly, seasonal,
and daily skill-metric recalculation (all scheduled *after* monthly in the function
body) never run, and the graceful "collect errors, report, exit 1" behavior every
other failure path in this file uses is bypassed in favor of an unhandled traceback.

## Evidence

`apps/postprocessing_forecasts/src/file_writer.py`, `save_monthly_skill_metrics`:

- `:413-414` — `csv_dir = os.getenv("ieasyforecast_intermediate_data_path")` /
  `csv_file = os.getenv("ieasyforecast_monthly_skill_metrics_file")`.
- `:418-419` — `filepath` is assigned **only** here:
  ```python
  if csv_dir and csv_file:
      filepath = os.path.join(csv_dir, csv_file)
      ...
  else:
      logger.warning("Monthly skill metrics CSV path not configured, skipping CSV save")
  ```
  The `else` branch (`:426-427`) does not define `filepath`.
- `:435-442` — the consistency-check branch references `filepath` unconditionally:
  ```python
  consistency_check = os.getenv("SAPPHIRE_CONSISTENCY_CHECK", "false").lower() == "true"
  if consistency_check:
      ...
      is_consistent, message = fl._verify_preprocessing_write_consistency(
          written_data=data,
          csv_file_path=filepath,
          ...
      )
  ```
  If `filepath` was never bound, this line raises `UnboundLocalError` before the
  function returns.
- Default: `SAPPHIRE_CONSISTENCY_CHECK` defaults to `"false"` (same line, `:435`) —
  the check is opt-in, so this is a two-condition trigger (CSV env vars unset **and**
  the operator has separately opted into the consistency check), not a bug hit by
  default configuration.

## Trigger condition

Both must hold simultaneously:
1. `ieasyforecast_intermediate_data_path` or `ieasyforecast_monthly_skill_metrics_file`
   is unset (or both).
2. `SAPPHIRE_CONSISTENCY_CHECK=true`.

Neither condition alone reproduces the crash: CSV-vars-unset with the check off just
logs a warning and skips CSV (current, intended-looking behavior); CSV-vars-set with
the check on exercises the intended consistency-check code path successfully.

## Propagation — verified by reading the call site, not assumed

`apps/postprocessing_forecasts/recalculate_skill_metrics.py:361`:

```python
ret = file_writer.save_monthly_skill_metrics(monthly_skill, year=skill_metrics_year)
if ret is None:
    logger.info("Monthly skill metrics saved successfully.")
else:
    logger.error(f"Error saving monthly skill metrics: {ret}")
    errors.append(f"Monthly skill metrics save failed: {ret}")
```

This call is **not** wrapped in `try/except` — unlike the "P1 stale-aggregate
invalidation" block immediately above it (`:340-359`), which does catch exceptions
and appends to `errors`. `recalculate_skill_metrics()` (the whole function,
`:252-587`) has no top-level `try/except` either — every other failure mode in this
script funnels into the `errors` list and a single `sys.exit(1)` at the end
(`:580-587`); this one does not.

Consequence, confirmed by reading control flow rather than guessed: the
`UnboundLocalError` propagates out of `save_monthly_skill_metrics`, out of the
`if prediction_mode in ["MONTHLY", "ALL"]` block, out of the `with timer(...)`
context managers, and out of `recalculate_skill_metrics()` itself — uncaught. Python's
default unhandled-exception behavior applies: a traceback is printed to stderr and
the process exits with status 1. This differs from the script's own designed failure
path (`errors.append(...)` → summarized log → `sys.exit(1)`) in two ways that matter
operationally: (1) quarterly/seasonal/daily recalculation — which run after monthly
in `recalculate_skill_metrics()` — never execute at all, whereas the designed error
path would have let them run and reported monthly's failure alongside their results;
(2) the operator sees a raw Python traceback instead of the script's structured error
summary, which is a worse debugging experience for what is, after all, a narrow
misconfiguration.

## Severity reasoning

- **Narrow trigger**: requires a specific two-condition misconfiguration
  (`SAPPHIRE_CONSISTENCY_CHECK=true` opted in, while the monthly CSV path env vars
  are unset) that is not default and, per the CSVs' role as a legacy/transitional
  I/O path (see CLAUDE.md "Data I/O Transition"), may become more likely as
  deployments move toward API-only monthly writes without updating the consistency
  check's assumptions.
- **Wide blast radius when triggered**: not just monthly fails — the entire recalc
  run stops mid-way, silently skipping quarterly/seasonal/daily for that invocation
  and replacing the script's structured error report with an unhandled traceback.
  A caller (cron wrapper, CI, or an operator) checking only "did it print a traceback
  or an error list" gets a different signal than every other failure mode in this
  script produces.
- Net: not High (the misconfiguration is not something operational deployments hit
  today by default), but not Low either, given the disproportionate blast radius
  relative to the trigger's narrowness. Filed Medium; escalate to High if any current
  deployment is confirmed to run with CSV vars unset and consistency-check on.

## What a fix must not break

- The intended CSV-optional behavior: skipping CSV write when the env vars are unset
  must remain a `logger.warning` no-op, not become an error, for the
  `SAPPHIRE_CONSISTENCY_CHECK=false` (default) case.
- The consistency check's actual purpose when CSV *is* configured — do not weaken or
  bypass `_verify_preprocessing_write_consistency` for the configured case while
  fixing the unconfigured one.
- This is a different defect from PP-051
  (`high_prio_gi_draft_pp_recalc_silent_api_write_failure.md`), which is in the same
  function family but concerns the API write path silently returning `None` on both
  success and swallowed failure. This draft is a pure crash on a local-variable
  binding, unrelated to the API return-value question — do not merge fixes without
  keeping both defects separately tested, since PP-051's fix may change what
  `save_monthly_skill_metrics` returns, and this fix changes whether it raises at all
  under the CSV-unset case.

## Proposed options (owner to choose — not decided here)

**Option A — Skip the consistency check when there is no CSV to check against.**
Guard `:435` as `if consistency_check and filepath is not None:` (or equivalent),
mirroring the existing `if csv_dir and csv_file:` condition. Smallest change; the
consistency check's own job is to compare in-memory data to the CSV it just wrote
(per PP-051's evidence), so if there is no CSV, there is nothing to check — a
`logger.warning`-and-skip is consistent with the CSV-unset branch's existing style.

**Option B — Initialize `filepath = None` at function top**, so the reference at
`:442` does not raise regardless of guard logic; combine with Option A's guard so the
check is still skipped meaningfully rather than passing `None` into
`_verify_preprocessing_write_consistency`.

**Option C — Treat CSV-vars-unset as a hard configuration error** when
`SAPPHIRE_CONSISTENCY_CHECK=true`, on the reasoning that opting into a consistency
check implies the operator expects CSV to exist — raise a clear, named exception
early (at `:426`) instead of letting an unrelated `NameError`-class error surface
later. Changes current behavior (CSV-vars-unset today is *always* a soft warning,
never an error) — needs sign-off since it is a stricter contract than exists today.

## Acceptance criteria (draft — refine when planned)

- Test: CSV vars unset, `SAPPHIRE_CONSISTENCY_CHECK=false` (default) — function
  returns `None`, logs the existing warning, no exception. (Already-passing
  characterization, must stay green.)
- Test: CSV vars unset, `SAPPHIRE_CONSISTENCY_CHECK=true` — function does **not**
  raise `UnboundLocalError`; document the chosen resulting behavior (skip check /
  warn / raise a named config error per whichever option is picked).
- Test: CSV vars set, `SAPPHIRE_CONSISTENCY_CHECK=true` — existing consistency-check
  behavior (pass and fail cases) unchanged.
- `recalculate_skill_metrics.py`'s monthly call site behavior under the fix: confirm
  whether it should also gain a `try/except` (matching the P1 stale-invalidation
  block's pattern) as defense-in-depth, independent of the `file_writer.py` fix —
  flag as a decision point, do not assume.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` green.
- No real station codes, credentials, or discharge values in new tests.

## References

- `apps/postprocessing_forecasts/src/file_writer.py:382-461`
  (`save_monthly_skill_metrics`, full function)
- `apps/postprocessing_forecasts/recalculate_skill_metrics.py:252-366`
  (`recalculate_skill_metrics`, setup through the monthly save call), `:580-587`
  (end-of-run error reporting, for contrast with the unhandled-exception path)
- Sibling in the same function, different defect:
  `doc/plans/issues/high_prio_gi_draft_pp_recalc_silent_api_write_failure.md`
  (PP-051, Draft)
