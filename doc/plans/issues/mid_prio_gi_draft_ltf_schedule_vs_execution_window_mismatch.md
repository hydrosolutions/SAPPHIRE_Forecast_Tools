## The long-term scheduler admits modes at 10 days but model execution refuses them at 5 (LTF-007)

**Status**: Draft (2026-08-18)
**Module**: `apps/long_term_forecasting` (`lt_schedule_query.py`, `lt_utils.py`)
**Priority**: **Medium — provisional, pending one production check.** The consequence is an
orchestration/status defect, not wrong numbers. **OWNER DECISION**: if it is confirmed that a
deployed schedule actually hits the 6–10 band — the checked-in recommended cron runs this route on
fixed dates (`doc/deployment.md:896-898`) — then an admitted production forecast goes wholly missing
while Luigi reports success, which is the same class as the other silent write-loss issues and
merits **High**. Medium is defensible only as "not yet shown to fire in production", never as
"harmless".
**Labels**: `ltf`, `scheduling`, `silent-noop`, `configuration-drift`
**Found**: 2026-08-18, while mapping schedule authority for INFRA-022 option (d). Found by
out-of-loop review of the extraction plan; not introduced by that work.
**Related**: **INFRA-022** (gating built on the scheduler inherits this), **INFRA-028** (a run
manifest would make the resulting no-op visible).

---

## Observation

Two independent day-window gates disagree:

| Gate | Value | Location |
|---|---|---|
| Scheduler — decides whether a **mode** is active | `ISSUE_DAY_TOLERANCE = 10` | `lt_schedule_query.py:52`, applied `:103` |
| **Local runner fallback** — used when the schedule query fails *(found 2026-08-18, a third definition)* | hard-coded `5` | `run_locally.sh:232-261`, selected at `:274-300`; independently reimplements the wrap distance and issue-day defaults |
| Execution — decides whether a **model** runs | hard-coded `5` | `lt_utils.py:202` in `check_valid_forecast_issue_date` — `abs(day_offset) > 5` (`:196` is the comment above it) |

*(Preconditions and consequences below were corrected 2026-08-18 after out-of-loop review; the first
draft overstated all three. The defect is real, the chain is conditional.)*

The divergence bites when **all** of these hold:

1. the mode passes the scheduler's first gate — `day_distance` ≤ 10. Note this is an **approximate**
   day-of-month distance with a `30 - diff` wrap (`lt_schedule_query.py:60-64`), so it can report 6
   where the model's real calendar offset is 5 or less, in which case execution accepts and there is
   no divergence at all;
2. the mode also passes the scheduler's **second** gate — at least one model scheduled this month by
   `forecast_months` (`:107-125`). A mode rejected here never runs, so the mismatch never arises;
3. **every** configured model's real nearest-scheduled-date offset then exceeds 5
   (`lt_utils.py:188-202`).

When all three hold: the orchestrator launches the mode, `check_valid_forecast_issue_date` returns
`None` for every model, and the run produces no forecast.

## Why it matters

- **The failure is reported, but not in a machine-readable way.** The gate itself logs at INFO
  (`lt_utils.py:202-209`) — but `run_single_model` immediately emits a **WARNING** and returns
  `False` (`run_forecast.py:336-343`), dependent models may then be skipped at **ERROR**
  (`:490-496`), and the final summary prints **FAILED** (`:517-526`). What is missing is a non-zero
  process status or a persisted outcome — not the logging. *(An earlier draft called this
  "invisible … nothing reports an error", which is wrong.)*
- **Whole-run success is conditional, not guaranteed.** `run_forecast` returns no aggregate status,
  so its CLI exits zero and `run_locally` records the module PASS; Luigi likewise writes its success
  markers (`pipeline_docker.py:2413-2434`). But `long-term-operational` then invokes validation, and
  a validation failure is recorded and makes the final shell exit non-zero
  (`run_locally.sh:1585-1588`, `:1970-1977`). If validation is disabled or unavailable it exits
  zero. So "scheduled, ran, wrote nothing, reported success" holds unconditionally for **Luigi** —
  the deployed route (`bin/run_long_term_forecasts.sh:104-124`) — and conditionally for the local
  runner.
- **It matters for INFRA-022.** Gating derived from the scheduler's 10-day window would mark these
  days as "output expected". **Note the semantics** (corrected 2026-08-18): under INFRA-028's
  manifest contract that is a **genuine detected execution failure**, not a legitimately gated SKIP
  — the run *was* expected to produce output and did not. An earlier version of this draft and its
  tracker row called it a "false FAIL", contradicting INFRA-022 and INFRA-028.

## Both comments say "temporary", and they disagree

`lt_schedule_query.py:50-51`:
> "Temoporarily relaxed to 10 days to allow more modes to be active for testing and calibration.
> Must be changed back to 5 days for operational use…"

`lt_utils.py:197-201`:
> "Temporarily allow a wider window for the first few runs … After the first few successful runs
> on the server, we can tighten this back to 5 days."

The second comment describes a widening that **is not present in its own code** — the check below
it is `> 5`. So one gate is relaxed, the other is not, and the comment on the un-relaxed one claims
otherwise. Whatever the intended operational value is, it is currently stated in three places and
implemented in two.

## Proposed fix (to be planned)

1. **DECIDED 2026-08-18 (owner): the operational window is 5 days.** The scheduler drops from 10 to
   5, matching what execution already enforces — so no admitted mode can be refused by every model,
   which is the defect. Both "temporary" comments already name 5 as the intended operational value
   (`lt_schedule_query.py:50-52`, `lt_utils.py:196-201`).
   **Consequence to state in the plan**: modes currently admitted at 6–10 days stop being scheduled.
   That is not a loss of forecasts — those runs already produce nothing, because execution refuses
   them — but it *is* a visible change in `active_modes`, so anything reading that output (Luigi's
   workflow, the local runner's mode list) will see fewer modes on those days. Verify no consumer
   treats an empty `active_modes` as an error rather than a quiet day.
2. Have **all three** gates agree. *(Corrected 2026-08-18: an earlier version of this list said
   "both gates", written before the shell fallback was found. Documenting a third authority in the
   table above while leaving the remedy at two would have left the fallback hard-coded and the
   issue unclosed.)*
   - **Python scheduler** and **Python execution gate** read the value from a single place —
     `apps/long_term_forecasting/lt_schedule_rules.py`, which the INFRA-022 extraction creates as a
     flat sibling of both call sites (placement per plan rev 4). Do not add a fourth definition.
   - **The shell fallback — DECIDED 2026-08-18: delete it.** `run_locally.sh:232-261` cannot import
     a Python constant, and it currently substitutes its own guess for the authority that just
     failed. Options (ii) "shell out to query the value" and (iii) "keep the literal plus a
     divergence test" were considered and **not** chosen: both preserve a second implementation of
     scheduling logic, which is the defect class this issue exists to remove.
     **A failed schedule query becomes a hard error.** That is a behavior change with an operational
     consequence worth stating plainly: a run that today proceeds on fallback modes will instead
     stop. That is the intent — proceeding on a guess is how an admitted-but-unrunnable mode reaches
     production unnoticed — but it means the failure path needs a clear operator message and a test,
     and deployments that have been silently relying on the fallback will start failing visibly.
     **Check before implementing**: whether any deployment currently depends on the fallback firing
     regularly, which would indicate the schedule query is failing routinely and is its own bug.
3. Reconcile or delete the stale comment in `lt_utils.py` — **and the matching stale text in
   `tests/test_lt_utils.py:91-103`**, which likewise claims execution was widened from 5 to 10. Both
   describe a widening the code does not implement.
4. If the two windows are *intended* to differ (a wider scheduling net, a stricter execution
   guard), then say so explicitly in both places and make the resulting no-op **loud** — an
   operator should not have to read INFO logs to discover that a scheduled run produced nothing.

## Acceptance criteria

*(Criteria below follow the decided option — delete the fallback. The options-conditional form is
retained in the fix section for the record.)*

- **One definition of the window**, read by both remaining gates: the Python scheduler and
  `check_valid_forecast_issue_date`. No literal survives anywhere.
- **The shell fallback is gone**, and a failed schedule query exits non-zero with an operator-legible
  message. A test covers that failure path — it is now the *only* behavior on query failure, so it
  must not be left untested.
- A grep for a bare day-window literal in `run_locally.sh` and `lt_utils.py` returns nothing, so the
  removal cannot be quietly reintroduced.
- A run scheduled but rejected by every model produces a visible non-success signal, or the
  scheduler stops admitting days that execution will refuse.
- Tests cover the boundary on both sides: `distance == window`, `window + 1`, and the previously
  divergent 6–10 day band.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh long_term_forecasting` green.

## Out of scope

- The `30 - diff` month-wrap approximation in `day_distance` (`lt_schedule_query.py:60-64`).
- Changing 10 → 5 as part of INFRA-022's extraction — that extraction is explicitly
  behavior-preserving; the value change belongs here.
