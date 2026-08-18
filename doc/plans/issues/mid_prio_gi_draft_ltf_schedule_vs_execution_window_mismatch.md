## The long-term scheduler admits modes at 10 days but model execution refuses them at 5 (LTF-007)

**Status**: Draft (2026-08-18)
**Module**: `apps/long_term_forecasting` (`lt_schedule_query.py`, `lt_utils.py`)
**Priority**: **Medium** — silent no-op rather than wrong numbers, but it produces a run that is
scheduled, executes, and writes nothing, with no error anywhere.
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
| Execution — decides whether a **model** runs | hard-coded `5` | `lt_utils.py:196` (`check_valid_forecast_issue_date`), applied via `abs(day_offset) > 5` |

So for a day 6–10 days from a mode's `operational_issue_day`:

1. `query_schedule` reports the mode **active**;
2. the orchestrator launches the long-term run for it;
3. `check_valid_forecast_issue_date` returns `None` for **every** model — logged at INFO as
   "not scheduled … skipping" — and the run writes nothing;
4. nothing reports an error. The pipeline records success for a run that produced no forecast.

## Why it matters

- It is invisible: a graceful `return None` per model, logged at INFO, inside a run the pipeline
  considers successful.
- **It will produce false FAILs the moment INFRA-022 lands.** Schedule-aware gating derived from
  the scheduler's 10-day window will mark those days as "output expected", while execution
  guarantees there is none. Whoever implements INFRA-022 must know this gap exists, or they will
  build gating that is correct against the scheduler and wrong against reality.

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

1. Decide the operational window — one value, one definition.
2. Have both gates read it from a single place. Note INFRA-022's extraction work moves
   `ISSUE_DAY_TOLERANCE` into `iEasyHydroForecast`; this issue should consume that, not add a
   fourth definition.
3. Reconcile or delete the stale comment in `lt_utils.py`.
4. If the two windows are *intended* to differ (a wider scheduling net, a stricter execution
   guard), then say so explicitly in both places and make the resulting no-op **loud** — an
   operator should not have to read INFO logs to discover that a scheduled run produced nothing.

## Acceptance criteria

- One authority for the window; both call sites read it.
- A run scheduled but rejected by every model produces a visible non-success signal, or the
  scheduler stops admitting days that execution will refuse.
- Tests cover the boundary on both sides: `distance == window`, `window + 1`, and the previously
  divergent 6–10 day band.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh long_term_forecasting` green.

## Out of scope

- The `30 - diff` month-wrap approximation in `day_distance` (`lt_schedule_query.py:60-64`).
- Changing 10 → 5 as part of INFRA-022's extraction — that extraction is explicitly
  behavior-preserving; the value change belongs here.
