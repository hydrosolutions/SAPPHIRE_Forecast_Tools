# LR-011 — `_write_lr_forecast_to_api`'s `False` returns are discarded, so LR-007's "loud failure" guarantee is incomplete

**Status**: Draft
**Module**: `apps/iEasyHydroForecast/forecast_library.py` (`_write_lr_forecast_to_api`,
`write_linreg_pentad_forecast_data`, `write_linreg_decad_forecast_data`)
**Priority**: High (see "Severity reasoning" below — this is not automatically High,
argued on impact)
**Labels**: `linear_regression`, `api-integration`, `false-success`, `reliability`

---

## Summary

`_write_lr_forecast_to_api` returns `False` (not an exception) on at least three
distinct guard paths. Both callers — `write_linreg_pentad_forecast_data` and
`write_linreg_decad_forecast_data` — invoke it for side effect only and discard the
return value; they set `api_ok = False` exclusively inside `except Exception`. A `False`
return therefore never reaches `api_ok`, which stays `True`, so the pentad/decad LR
forecast writers report success to their caller even when the API write did not happen.

This is not a claim that LR-007 (`archive/review_gi_draft_lr_api_write_loud_failure.md`,
Complete) was the wrong fix. LR-007 shipped the `api_ok` return-`bool` pattern
specifically to stop silent API write failures, and it correctly catches the
*exception* path. This draft is that the same function has a second failure surface —
early `return False` with no exception — that the `except Exception` guard structurally
cannot see. The defect class LR-007 targeted is still partially open.

## Evidence

`_write_lr_forecast_to_api` (`apps/iEasyHydroForecast/forecast_library.py:3327`),
three `return False` paths, none of which raise:

- `:3338-3340` — `if not SAPPHIRE_API_AVAILABLE: ... return False` (client library not
  installed).
- `:3352-3355` — `if not api_enabled: ... return False`
  (`SAPPHIRE_API_ENABLED=false`).
- `:3362-3364` — `if not ready: ... return False` (API readiness check failed —
  e.g. a transient outage or the API container still starting up).

A fourth `return False` exists at `:3421-3423` (`else: logger.info("No LR forecast
records to write to API"); return False`) when `records` is empty after NaN-filtering —
this one is more defensible as a no-op, but it is still silently absorbed by the same
gap and should be triaged alongside the other three, not assumed benign.

Callers discard the return value and only observe exceptions:

- `apps/iEasyHydroForecast/forecast_library.py:3973-3979`
  (`write_linreg_pentad_forecast_data`):
  ```python
  api_ok = True
  if data_for_api is not None and not data_for_api.empty:
      try:
          _write_lr_forecast_to_api(data_for_api, "pentad")
      except Exception as e:
          _handle_api_write_error(e, "write_linreg_pentad_forecast_data")
          api_ok = False
  ```
  `_write_lr_forecast_to_api(...)`'s return value is never assigned or checked.
  Function returns `api_ok` at `:4092`.
- `apps/iEasyHydroForecast/forecast_library.py:4317-4323`
  (`write_linreg_decad_forecast_data`) — identical shape. Function returns `api_ok`
  at `:4420`.
- `apps/linear_regression/linear_regression.py:954-963` (pentad) and `:1066-1073`
  (decad) — the outer caller:
  ```python
  if not fl.write_linreg_pentad_forecast_data(linreg_pentad, forecast_date=current_day):
      logger.error("CRITICAL: API write failed ... API database is now behind CSV.")
      api_write_failures = True
  ```
  When `api_ok` is (incorrectly) `True`, this branch is skipped, no CRITICAL is
  logged, `api_write_failures` stays `False`, and `sys.exit(0)` fires at
  `linear_regression.py:1096`.

## Not total data loss — establishing the actual impact

The CSV write in both `write_linreg_*_forecast_data` functions is unconditional:
it runs regardless of `data_for_api`/`api_ok` (`forecast_library.py:3981-4070` for
pentad, the equivalent block for decad). So when `_write_lr_forecast_to_api` returns
`False`, **the CSV still gets the correct data** — this is unreported-*partial* loss
(the API/DB write), not total loss of the forecast. What is lost without a trace is
only the API/DB copy; the operator is told the run succeeded and has no signal that
the database has silently fallen behind the CSV.

## Why this matters / severity reasoning

- **How would an operator notice?** They would not, at the point of write — no
  CRITICAL, no non-zero exit, `sys.exit(0)`. The only way to notice is downstream:
  a consumer reading LR pentad/decad forecasts from the API (dashboard, another
  module) sees missing or stale rows with no correlated error in the LR run's own
  logs. That is a materially harder failure to diagnose than a reported one.
- **What data is lost vs. merely unreported?** Established above: the CSV is intact;
  only the API/DB row is missing, and *that* fact is unreported. Given the project's
  stated direction ("Data I/O Transition" — CSV is legacy, being phased out once API
  integration is fully tested), a silent API-write gap is exactly the kind of defect
  that undermines confidence in declaring API integration "fully tested" (see D-002
  in `module_issues.md`).
- **How common are the trigger conditions?** Two of the three guards
  (`SAPPHIRE_API_ENABLED=false`, `sapphire-api-client` not installed) are
  configuration states, not exotic edge cases. The third — the readiness check
  failing — is a normal transient-outage scenario (API restart, brief network
  blip), which is precisely the case LR-007's own design note says should **not**
  abort a long-running run. That design intent (don't hard-fail on a blip) is sound;
  the bug is that it also isn't *reported*, which was supposed to be LR-007's whole
  point.
- **Why High and not Medium:** this is shipped code on the operational short-term
  forecast path (pentad/decad LR runs every forecast cycle), and it silently defeats
  a safety net (LR-007) that was already reviewed, shipped, and marked Complete
  specifically to close this defect class. A reader who is inclined toward Medium
  because the data itself isn't lost should weigh that the *absence of any signal*
  is the actual defect being reported here, not the data loss.

## Distinguish from LR-010 (not a duplicate)

`mid_prio_gi_draft_lr_skip_reported_as_api_write_failure.md` (LR-010) covers the
opposite direction: a legitimate skip (`return` / `None`) that gets *over*-reported
as a CRITICAL API write failure. This draft (LR-011) is the reverse: a genuine
non-write (`return False`) that gets reported as success. Both live in the same
`api_ok` truthiness-handling area of `write_linreg_pentad_forecast_data` /
`write_linreg_decad_forecast_data`, but they are distinct defects with opposite
symptoms — do not merge them into one fix without treating both outcome classes
explicitly (see LR-010's proposed four-outcome result type; `False` from
`_write_lr_forecast_to_api` would be a fifth outcome — genuine write failure without
an exception — distinct from LR-010's two skip outcomes and from the existing
exception-path failure).

## Reproduction condition

Any of:
- `SAPPHIRE_API_ENABLED=false` during a pentad or decad LR run with data present.
- `sapphire-api-client` not installed (`SAPPHIRE_API_AVAILABLE=False`).
- API reachable at the network level but `readiness_check()` returns falsy (e.g.
  service still starting, or its own readiness dependency down).

Expected today: run logs "Iteration ... completed successfully", no CRITICAL, exit 0.
CSV has the forecast; API/DB does not.

## What a fix must not break

- The CSV write path and its unconditional nature — do not make CSV writes
  contingent on the API outcome.
- LR-007's existing exception-path behavior: a genuine exception during the API call
  must continue to set `api_ok = False`, log CRITICAL, and drive `sys.exit(1)` via
  `linear_regression.py`'s existing check.
- The "don't hard-fail a long run on a transient blip" intent behind returning
  `False` instead of raising — a fix should make the failure *visible*, not
  necessarily convert it into a hard abort. Whether it should abort is a policy
  question analogous to the one LR-010 raises for its skip paths; do not conflate
  the two, but flag the parallel for whoever resolves both.
- Existing tests asserting `_write_lr_forecast_to_api`'s current boolean contract
  (True/False semantics) — a fix will need updated tests, not deleted ones.

## Proposed options (owner to choose — not decided here)

**Option A — Propagate the return value.** Change
`write_linreg_pentad_forecast_data`/`write_linreg_decad_forecast_data` to capture
`_write_lr_forecast_to_api`'s return and fold it into `api_ok`
(`api_ok = api_ok and _write_lr_forecast_to_api(...)`). Smallest change; keeps the
existing `bool` contract at the outer caller. Does not distinguish *why* the write
returned `False` (config-disabled vs. not-ready vs. no records) — all three
currently distinct log messages collapse to the same `api_ok=False` outcome, which
callers already treat uniformly today.

**Option B — Structured result instead of bare bool**, consistent with LR-010's
proposed enum/result-object approach if both issues are fixed together: distinguish
`WRITTEN` / `DISABLED` / `NOT_READY` / `NO_RECORDS` / `EXCEPTION_FAILED`, and let
`linear_regression.py` decide per-outcome whether to log CRITICAL and/or abort.
Larger change, coordinate with LR-010 if picked, since both touch the same
`api_ok` boolean-truthiness convention.

**Option C — Raise instead of returning `False`** for the config-guard paths
(`SAPPHIRE_API_ENABLED=false`, client not installed), reserving a silent `False`
only for the not-ready/transient case. Changes which paths are "loud" vs
"quiet by design" — needs an explicit decision on whether a deliberate
`SAPPHIRE_API_ENABLED=false` deployment choice should be loud every run (arguably
noisy) or silent (today's behavior, the actual bug being reported).

## Acceptance criteria (draft — refine when planned)

- Unit tests cover each of the three (or four, if the empty-records path is
  included) `return False` guards in `_write_lr_forecast_to_api`, asserting the
  chosen outcome propagates to `write_linreg_pentad_forecast_data`/
  `write_linreg_decad_forecast_data`'s return value and, from there, to
  `linear_regression.py`'s `api_write_failures` handling.
- The existing exception-path test(s) for LR-007 continue to pass unchanged.
- CSV output (content, ordering, format) is provably unchanged by the fix — this is
  a reporting fix, not a write-path change.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh linear_regression` green.
- No real station codes in new tests — use placeholder codes (e.g. `19999`).

## References

- `apps/iEasyHydroForecast/forecast_library.py:3327-3423` (`_write_lr_forecast_to_api`)
- `apps/iEasyHydroForecast/forecast_library.py:3973-3979, 4092`
  (`write_linreg_pentad_forecast_data`)
- `apps/iEasyHydroForecast/forecast_library.py:4317-4323, 4420`
  (`write_linreg_decad_forecast_data`)
- `apps/linear_regression/linear_regression.py:954-963, 1066-1073, 1090-1096`
- Precedent / partially-superseded fix:
  `doc/plans/issues/archive/review_gi_draft_lr_api_write_loud_failure.md` (LR-007,
  Complete)
- Opposite-direction sibling: `doc/plans/issues/mid_prio_gi_draft_lr_skip_reported_as_api_write_failure.md`
  (LR-010, Draft)
- Same defect class in a different module:
  `doc/plans/issues/high_prio_gi_draft_pp_recalc_silent_api_write_failure.md`
  (PP-051, Draft) — `save_*` skill-metrics functions swallow API failures into a
  bare `None` return; structurally the same "return value discarded, success
  falsely inferred" shape, in `postprocessing_forecasts` instead of `linear_regression`.
