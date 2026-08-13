# PP-054 — Daily/threshold skill-metrics write: silent-success fix split out of PP-051

**Status**: Draft
**Module**: postprocessing_forecasts (`file_writer.py::save_daily_skill_metrics`,
`src/api_writer.py::_write_threshold_skill_metrics_to_api`, `recalculate_skill_metrics.py`)
**Priority**: High
**Labels**: `reliability`, `api-integration`, `data-integrity`, `false-success`

---

## Relationship to PP-051

Same defect family as PP-051 (`doc/plans/issues/high_prio_gi_draft_pp_recalc_silent_api_write_failure.md`):
a `save_*` skill-metrics function swallows an API write failure and returns a
value its caller in `recalculate_skill_metrics.py` reads as success. PP-051's
implementation plan (`doc/plans/working/pp051_recalc_write_failure_plan.md`)
fixes this for pentad/decad, monthly, quarterly, and seasonal via phases
P0-P3, landing the `api_writer.WriteOutcome` enum (Contract 7 of that plan)
that this function must also consume. **This draft covers only the daily
block** — `save_daily_skill_metrics` (two independent writes: FDC metrics and
threshold metrics) — which PP-051's plan originally scoped as its own P4 and
then split out because P4 was not executable as specified. Do not re-derive
the `WriteOutcome` design here; it is settled by PP-051 and this work consumes
it, it does not extend or modify it.

Both functions in scope are **API-only, no CSV fallback** — like PP-051's
quarterly/seasonal, a swallowed failure here is total data loss for that
recalc's daily output, not a degraded one.

**This draft must also carry PP-051's corrected D1/D2/D3 mapping (P0b), not the
mapping an earlier revision of PP-051's plan had.** `save_daily_skill_metrics`
gates on `api_writer.SAPPHIRE_API_AVAILABLE` alone, exactly like the four
functions PP-051's P1-P3 convert — the FDC branch at `file_writer.py:614`
(call at `:616`) and the threshold branch at `:624` (call at `:626`). The
**production fix for the daily path is the same pre-gate default**: the
outcome variable for each branch must initialize to
`api_writer.WriteOutcome.FAILED` before its `if
api_writer.SAPPHIRE_API_AVAILABLE:` check, not `SKIPPED_BY_CONFIG` — because
the only thing that closes that gate is a missing required dependency
(`sapphire-api-client`, `pyproject.toml:27`), which is a failure, not an
operator setting; `SAPPHIRE_API_ENABLED=false` never closes this gate at all,
it is checked inside the writer functions themselves and always returns
`SKIPPED_BY_CONFIG` there regardless of the pre-gate default. **Until this
draft implements that pre-gate default, a DAILY or ALL recalc run can exit
`0` after writing no daily metrics at all when `sapphire-api-client` is not
installed** — the same silent-success gap PP-051's P1-P3 close for the other
four `save_*` functions, still open for this one. `ignore` mode must not
downgrade the outcome either (D3): `forecast_library.py:110-116` makes
`ignore` silent relative to `warn` only — it suppresses
`_handle_api_write_error`'s logging, not failure accounting — so a `FAILED`
outcome for either branch stays `FAILED` under `warn` and `ignore` alike,
only `fail` re-raises. See
`doc/plans/working/pp051_p0a_client_absent_mapping_fix.md` for the full
correction this note carries forward, and
`doc/plans/working/pp051_recalc_write_failure_plan.md` §2 Contract 7 and its
P1 phase entry (§4) for the exact call-site code shape (initialize before the
gate, capture the writer's return value inside the `try`, explicitly assign
`FAILED` in the `except` block after calling
`fl._handle_api_write_error(e, ...)`, no downgrade step) — do not re-derive
it independently, copy that shape for both the FDC and threshold branches.

## The four blockers that made P4 unexecutable as specified

1. **Files-list vs. acceptance-criteria contradiction.** P4's `Files` list
   permitted only `file_writer.py`, `recalculate_skill_metrics.py`, and
   `test_file_writer.py`, explicitly excluding `test_recalc_workflow.py` — yet
   four of its acceptance criteria required driving
   `recalculate_skill_metrics()` end-to-end (fail-mode propagation, the
   FDC-fails/threshold-succeeds independence check, the exit-code check). The
   only harness that runs `recalculate_skill_metrics()` under mocks
   (`import_recalc_module` / `_setup_mocks`) lives in
   `apps/postprocessing_forecasts/tests/test_recalc_workflow.py`. Any
   implementation plan for this draft must either add that file to the Files
   list or replace the end-to-end criteria with equivalent unit-level
   assertions on `save_daily_skill_metrics` directly.
2. **Zero existing test coverage — verified.** A repo-wide grep found no test
   referencing `save_daily_skill_metrics` by name, and no test calling
   `_write_threshold_skill_metrics_to_api` directly. There is nothing to
   extend or invert (unlike P1-P3's functions, which had existing tests to
   convert) — every test for this function is new, including basic
   success/failure coverage that other phases got for free from pre-existing
   fixtures. The plan must also specify a DAILY-mode fixture chain
   (`data_reader.read_daily_observations` / `read_daily_forecasts` /
   `skill_metrics.calculate_daily_skill_metrics`, driven at
   `recalculate_skill_metrics.py:535-543`) — `_setup_mocks` in
   `test_recalc_workflow.py` sets none of these today, so a DAILY-mode
   integration test cannot run against it unmodified.
3. **No log-message template for the new branch.** PP-051's Contract 9 covers
   the four pentad/decad/monthly/quarterly/seasonal `{ret}` call sites; it
   does not cover the daily call site, which today logs the raw exception via
   `{e}` at `recalculate_skill_metrics.py:566`. This draft's plan must specify
   the daily block's `errors.append(...)` message content once the return
   value is a bool instead of relying on the bare exception string.
4. **Two acceptance criteria are mutually unsatisfiable as worded.** "Both
   writes attempted independently, do not short-circuit" (stated
   unqualified) contradicts a required `fail`-mode test proving a raised FDC
   exception propagates out of `save_daily_skill_metrics`: under
   `SAPPHIRE_API_FAILURE_MODE=fail`, `_handle_api_write_error` re-raises at
   `apps/iEasyHydroForecast/forecast_library.py:113` from inside the FDC
   `except` block, which necessarily skips the threshold write at
   `file_writer.py:626` before it is ever reached. The "independently
   attempted" guarantee can only hold for `warn`/`ignore` modes; the
   implementation plan for this draft must state that qualification
   explicitly rather than leave it implicit.

## Fixture shapes (nothing existing to copy from — build fresh)

- `fdc_metrics` rows need at minimum `code`, `model_short`, `fhv`, `flv`. The
  function itself injects `day_in_year=1` unconditionally before the API call
  (`file_writer.py:612`) — do not add that column in the fixture, it would be
  overwritten anyway. Note for scoping: because `day_in_year` is always `1`,
  the `dropna` filter at `api_writer.py:507` never drops anything for this
  call path, and FDC data carries no `horizon_value` column at all, so a
  non-empty `fdc_metrics` always yields at least one surviving record — a
  `WriteOutcome.SKIPPED_NO_RECORDS`-via-internal-filtering test is not
  constructible for this branch; do not scope one.
- `threshold_metrics` rows need `code`, `model_short`, `threshold_type`,
  `threshold_value`, `f1`, `precision`, `recall`, `csi`, `tp`, `fp`, `fn`,
  `tn`, `n_years`, per the record shape `_write_threshold_skill_metrics_to_api`
  builds at `api_writer.py:769-791`.

## Two behaviors to preserve, verified against the current tree

- `write_diagnostics.diagnose_daily_skill_metrics` runs at
  `file_writer.py:606`, **before both branch guards** (the FDC guard at
  `:609`, the threshold guard at `:623`), and must keep tolerating
  `None`/empty input — it is not conditioned on either guard passing today,
  and a fix must not make it so.
- `_write_threshold_skill_metrics_to_api` (`api_writer.py:711`) **does raise**
  to its caller — it is not exception-free. `_get_postprocessing_client()`
  (`:741`) and `client.readiness_check()` (`:746`) both sit outside this
  function's own `try:` block, which does not open until `:753`. A connection
  failure, DNS failure, or timeout during client construction or the
  readiness check escapes uncaught. Any fix must give the threshold branch in
  `save_daily_skill_metrics` the same try/except-plus-capture shape already
  used for the FDC branch (`file_writer.py:614-618`) rather than a bare
  return-value read, or this raise will propagate unhandled under `warn`/
  `ignore` mode and crash the recalc mid-run — the exact regression PP-051
  exists to prevent, just relocated to this function.

## Out of scope / notes

- No `sapphire/services/` change — entirely `apps/` behavior.
- Do not modify `WriteOutcome` (defined in `api_writer.py` per PP-051's
  Contract 7) — this draft consumes the type, it does not extend its
  membership.
- `SAPPHIRE_API_FAILURE_MODE=fail` behavior must remain unchanged for the
  daily call site's existing outer `try/except`
  (`recalculate_skill_metrics.py:555-566`) — that wrapper must be **kept**,
  not replaced, exactly as PP-051's plan required of the original P4 (see
  that plan's P4 entry, retained for historical rationale even though P4 no
  longer executes).

## References

- `doc/plans/issues/high_prio_gi_draft_pp_recalc_silent_api_write_failure.md`
  (PP-051) — parent defect, `WriteOutcome` design owner.
- `doc/plans/working/pp051_recalc_write_failure_plan.md` — implementation
  plan; §2 Contract 7 (`WriteOutcome` definition/mapping) and the P4 phase
  entry (retained as split-out rationale, not an executable phase) are the
  starting point for this draft's own implementation plan.
- `apps/postprocessing_forecasts/src/file_writer.py:581-632`
  (`save_daily_skill_metrics`).
- `apps/postprocessing_forecasts/src/api_writer.py:711-829`
  (`_write_threshold_skill_metrics_to_api`).
- `apps/postprocessing_forecasts/recalculate_skill_metrics.py:555-566` (daily
  call site).
- `apps/iEasyHydroForecast/forecast_library.py:82-116`
  (`_get_api_failure_mode` / `_handle_api_write_error`, consumed not
  modified).
