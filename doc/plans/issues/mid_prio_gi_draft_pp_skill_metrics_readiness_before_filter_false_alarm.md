# PP-055 — `_write_skill_metrics_to_api` checks readiness before filtering: an unreachable API reports FAILED even when the (would-be-filtered) input had nothing to write

**Status**: Draft
**Module**: postprocessing_forecasts (`src/api_writer.py::_write_skill_metrics_to_api`)
**Priority**: Medium
**Labels**: `reliability`, `observability`, `false-positive`

---

Every bullet must help an agent know what to inspect, what contract not to break, or what
verification proves safety — otherwise cut it.

## Summary

`_write_skill_metrics_to_api` (`apps/postprocessing_forecasts/src/api_writer.py:450`) runs its
`SAPPHIRE_API_AVAILABLE` gate, `SAPPHIRE_API_ENABLED` gate, and `client.readiness_check()` call
**before** it does any of its own row filtering. If the readiness check fails, the function
returns `WriteOutcome.FAILED` at line 505 and never reaches the filtering logic that starts at
line 507. Consequently, an input whose rows would all have been dropped by the writer's own
filters — meaning nothing was ever going to be sent, readiness or not — is reported identically
to an input that genuinely had records to write and lost them to an unreachable API. Both produce
`FAILED`; the caller and the operator cannot tell them apart.

This is the **inverse** of PP-051's defect: PP-051 was silent data loss (a real failure reported
as success). This is a false alarm (a no-op reported as a failure). Nothing is lost here — the
rows that triggered `FAILED` were never going to be written in the first place.

## Verified against the code — ordering claim confirmed

Read in full, `apps/postprocessing_forecasts/src/api_writer.py:450-734`. Sequence, in order:

1. `:487-489` — `SAPPHIRE_API_AVAILABLE` gate → `WriteOutcome.FAILED` if the dependency is
   missing.
2. `:492-495` — `SAPPHIRE_API_ENABLED` gate → `WriteOutcome.SKIPPED_BY_CONFIG` if disabled.
3. `:500` — `client = _get_postprocessing_client()`.
4. `:503-505` — `if not client.readiness_check(): return WriteOutcome.FAILED`. **This is the
   readiness check the finding refers to.**
5. `:507` onward — `data = data.copy()`, then all filtering:
   - `:529-533` — `df_rec.dropna(subset=[horizon_in_year_col])` (drops rows with NaN
     `pentad_in_year`/`decad_in_year`/`month_in_year`/etc.).
   - `:626-646` — under `skill_lead_aware_enabled()` (env `SAPPHIRE_SKILL_LEAD_AWARE`, **default
     OFF** — `apps/iEasyHydroForecast/skill_lead_aware_flag.py`), rows with NaN `horizon_value`
     are excluded (`:645`) rather than coerced to `0`.
   - `:652-664` — upsert-key dedup (`drop_duplicates(subset=upsert_key, keep="last")`).
6. `:723-733` — `if records: ... return WROTE` / `else: ... return SKIPPED_NO_RECORDS`.

The readiness check at step 4 strictly precedes every filter in step 5. **Ordering claim
confirmed**, no part of it failed to hold.

One correction to the finding as scoped: of the three filters listed as candidates for emptying a
non-empty frame, only two actually can. The NaN-`horizon_in_year` dropna (`:532`) and the
lead-aware NULL-`horizon_value` exclusion (`:645`) can each reduce a non-empty `df_rec` to empty.
The **upsert-key dedup (`:664`) cannot** — `drop_duplicates(..., keep="last")` retains at least
one row per distinct key, so it can only shrink a non-empty frame, never empty it outright. Any
implementation must not treat dedup as a third path to `SKIPPED_NO_RECORDS`.

## Concrete repro scenario

With `SAPPHIRE_SKILL_LEAD_AWARE=true` and a non-empty pentad or monthly skill-metrics DataFrame
whose `horizon_value` column is all NaN (e.g. code `19999`, `model_short="LR"`,
`month_in_year=[6, 6]`, `horizon_value=[nan, nan]`) — a real, unmigrated-data shape, not a
contrived one — and `client.readiness_check()` returning `False` (API unreachable):

- Today: `_write_skill_metrics_to_api` returns `WriteOutcome.FAILED` at line 505, before ever
  looking at `horizon_value`.
- If the API had been reachable, the same input would have hit the `:645` exclusion, ended with
  `df_rec` empty, and returned `WriteOutcome.SKIPPED_NO_RECORDS` — a non-failure.

Downstream chain to an operator-visible failure (pentad/decad path, `save_skill_metrics`):
- `apps/postprocessing_forecasts/src/file_writer.py:398` — `return outcome is not
  api_writer.WriteOutcome.FAILED` → `False`.
- `apps/postprocessing_forecasts/recalculate_skill_metrics.py:241-243` — `if ret is False:
  errors.append(...)`.
- `recalculate_skill_metrics.py:586-590` — `if errors: ... sys.exit(1)`.

Monthly is structurally identical (`file_writer.py:502`, `save_monthly_skill_metrics`, feeding the
same `errors.append`/`sys.exit(1)` pair at `recalculate_skill_metrics.py:365-366`). Quarterly and
seasonal (`file_writer.py:728`, `:780`) share the same `outcome is not FAILED` contract but are
API-only (no CSV fallback) — see Severity below for why that doesn't change the direction of this
defect, only its downstream visibility.

Note: PP-051's `WriteOutcome` enum and its P0-P3 consumption in `save_skill_metrics`,
`save_monthly_skill_metrics`, `save_quarterly_skill_metrics`, and `save_seasonal_skill_metrics`
have already landed on this tree (confirmed by reading `file_writer.py` directly — the
`outcome is not WriteOutcome.FAILED` pattern is live in all four). This finding is a new,
orthogonal defect surfaced on top of the now-fixed PP-051, not a reopening of it.

## Test masking — confirmed by direct inspection

`apps/postprocessing_forecasts/tests/test_file_writer.py` has one
`test_skipped_no_records_via_internal_filtering_returns_true` test per horizon that exercises the
real (unmocked) `_write_skill_metrics_to_api` against exactly this all-NaN-`horizon_value`,
lead-aware-on shape:

- monthly: `:516-550`
- quarterly: `:1181-` (module-level constant block starting `:1023`)
- seasonal: `:1357-`
- pentad: `:1544-`

Every one of these sets `mock_client.readiness_check.return_value = True` (e.g. `:542`, `:1205`,
`:1381`, `:1570`) before driving the filtering path. Separately,
`test_failed_outcome_without_exception_returns_false` (e.g. `:447-457` for monthly) tests the
`FAILED` path, but does so by patching `_write_skill_metrics_to_api` itself to return
`WriteOutcome.FAILED` directly — it never runs the real function, so it cannot exercise the
readiness-check-then-filter ordering. A repo-wide check confirms no test in
`test_file_writer.py`, `test_api_writer_dedup.py`, or elsewhere sets
`readiness_check.return_value = False` on a mocked client passed into a *real* call to
`_write_skill_metrics_to_api` — the only `readiness_check.return_value = False` uses in this test
suite are in `test_data_reader.py` / `test_api_read.py` / `test_monthly_data_reader.py` /
`test_api_integration.py`, all on the **read** side (`data_reader.py`), not this writer. The
combination this finding describes — readiness failure + all-rows-filtered input — has zero test
coverage today.

## Severity / priority reasoning (argued, not defaulted)

- **Direction**: false alarm, not data loss. The pentad/decad and monthly paths have CSV fallback
  independent of this function, so the CSV output is unaffected either way. Quarterly/seasonal are
  API-only, so a `FAILED` there means the recalc run is flagged as an error — but the underlying
  data (all rows dropped by the lead-aware NULL-`horizon_value` filter) was never going to be
  persisted regardless of API reachability, so no forecast-quality-relevant row is newly lost by
  this bug; the loss (if any) was already accounted for by the `%d NULL-lead ... skipped` warning
  at `:639-644`, which never fires here because the code never reaches it.
- **Mitigating condition, stated explicitly**: this only manifests when `client.readiness_check()`
  genuinely returns `False` — i.e., the API is unreachable at the same moment. An unreachable API
  is independently worth surfacing as an operational problem; arguably reporting `FAILED` in that
  case is not "wrong" so much as **imprecise about which rows would have mattered**. It is
  reasonable for the owner to decide this is acceptable behavior (the run legitimately can't
  verify anything against a down API) rather than a defect requiring a code change — flagged here
  as an explicit fork, not resolved.
- **Why not High**: precedent for the over-reporting direction of this same defect family is
  LR-010 (`doc/plans/issues/mid_prio_gi_draft_lr_skip_reported_as_api_write_failure.md`), filed
  Low-Medium on the same reasoning — a legitimate no-op reported as a failure inflates alert noise
  and burns operator time re-investigating a run that actually needed nothing, but does not itself
  corrupt or drop forecast data.
- **Why not Low**: unlike LR-010 (pentad/decad only), this defect sits in the one function shared
  by five call sites — `save_skill_metrics` (pentad/decad), `save_monthly_skill_metrics`,
  `save_quarterly_skill_metrics`, `save_seasonal_skill_metrics`, and the FDC branch of
  `save_daily_skill_metrics` (`file_writer.py:653-657`, see cross-reference to PP-054 below) — and
  quarterly/seasonal have no CSV fallback, so their `sys.exit(1)` is the only signal an operator
  gets; a false one there is more costly to triage than a false one on a path with a CSV
  cross-check available.

## Cross-references

- **PP-051** (`doc/plans/issues/high_prio_gi_draft_pp_recalc_silent_api_write_failure.md`,
  implementation plan `doc/plans/working/pp051_recalc_write_failure_plan.md`) — parent defect
  family and the `WriteOutcome` enum this finding's fix space must not modify or extend the
  membership of. PP-051 fixed the opposite direction (failures reported as success); this finding
  is a false-positive introduced by *how* `FAILED` gets assigned, not by the enum's mapping, which
  PP-051 got right.
- **PP-047** (`doc/plans/issues/mid_prio_gi_draft_pp_api_writer_zero_partial_write_success.md`) —
  a different-layer defect in a sibling function (`_write_combined_forecast_to_api`) where a
  `True`/success return can mask a partial server-side write ("200 OK, zero rows persisted"). Not
  the same bug: PP-047 is a false positive hiding partial loss; this finding is a false negative
  (an alarm) hiding a genuine no-op. Both live in `api_writer.py` and both weaken the
  trustworthiness of the writer's return value, in opposite directions.
- **PP-054** (`doc/plans/issues/high_prio_gi_draft_pp_daily_threshold_skill_silent_api_write_failure.md`)
  — currently, `save_daily_skill_metrics`'s FDC branch (`file_writer.py:653-657`) calls
  `_write_skill_metrics_to_api` but discards its return value entirely (fire-and-forget, function
  returns `None`), so this finding's false-`FAILED` does **not** yet propagate to an operator-
  visible failure on the daily path. PP-054's draft plan proposes making that branch consume
  `WriteOutcome` the same way the other four call sites do. **Once PP-054 lands, this finding's
  root cause in `_write_skill_metrics_to_api` will start affecting the daily path too** — any fix
  for this issue should land before or in coordination with PP-054's daily-branch change, so PP-054
  doesn't inherit a false-alarm path it would otherwise have to rediscover.

## Fix space (not chosen — owner decides)

1. **Reorder**: move the filtering (dropna, lead-aware exclusion, dedup) before the
   `readiness_check()` call, so `SKIPPED_NO_RECORDS` can be returned without ever touching the
   API. Cheapest fix, but changes `_write_skill_metrics_to_api`'s current behavior — verify none of
   the five call sites depend on `readiness_check()` being attempted unconditionally (e.g. for a
   side-effecting health-check log line, or for `_check_write_codes` write-guard warnings that
   currently only fire after records are built at `:724`, which is unaffected either way).
2. **Wrapper-side pre-check**: have each `save_*` function (or a shared helper) determine "would
   anything survive filtering" before calling `_write_skill_metrics_to_api` at all, short-
   circuiting to a non-failure outcome when the answer is no. Avoids touching the shared writer
   function, but duplicates the filtering logic (dropna column name, lead-aware flag check, dedup
   key) across up to five call sites — a maintenance hazard if the filters ever change.
3. **Accept and document**: leave the ordering as-is and document that `FAILED` under readiness
   failure does not distinguish "would have written N rows" from "would have written 0" — treat
   API-unreachable as inherently error-worthy regardless of payload size. Lowest effort, but
   leaves the imprecision described above unresolved.

Each option has a different blast radius; (1) touches the one function all five `save_*` callers
share, (2) is additive but duplicates filter logic, (3) is a documentation-only no-op. Verification
for whichever is chosen should include: exercising the exact repro scenario above (all-NaN
`horizon_value`, lead-aware on, `readiness_check()` False) end to end through
`recalculate_skill_metrics.py` and confirming the run no longer exits 1 for that case, while a
genuine readiness failure on an input with real records to write still returns `FAILED`.

## References

- `apps/postprocessing_forecasts/src/api_writer.py:450-734` (`_write_skill_metrics_to_api`).
- `apps/postprocessing_forecasts/src/file_writer.py:291-398` (`save_skill_metrics`),
  `:401-502` (`save_monthly_skill_metrics`), `:620-671` (`save_daily_skill_metrics`),
  `:679-728` (`save_quarterly_skill_metrics`), `:731-780` (`save_seasonal_skill_metrics`).
- `apps/postprocessing_forecasts/recalculate_skill_metrics.py:240-245, 361-366, 586-590`.
- `apps/postprocessing_forecasts/tests/test_file_writer.py:447-550, 1023-1250, 1357-1400,
  1400-1580` (per-horizon `WriteOutcome` and internal-filtering tests).
- `apps/iEasyHydroForecast/skill_lead_aware_flag.py` (`SAPPHIRE_SKILL_LEAD_AWARE`, default OFF).
- `doc/plans/issues/high_prio_gi_draft_pp_recalc_silent_api_write_failure.md` (PP-051).
- `doc/plans/issues/mid_prio_gi_draft_pp_api_writer_zero_partial_write_success.md` (PP-047).
- `doc/plans/issues/high_prio_gi_draft_pp_daily_threshold_skill_silent_api_write_failure.md`
  (PP-054).
- `doc/plans/working/pp051_recalc_write_failure_plan.md` (`WriteOutcome` design owner, Contract 7).
