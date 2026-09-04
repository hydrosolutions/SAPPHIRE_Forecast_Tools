# INFRA-044: `run_locally.sh` has no DEGRADED state — a known upstream data gap is reported as FAIL

**Status**: Draft (2026-09-03)
**Module**: `apps/run_locally.sh` + `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`
**Priority**: **High** — blocking. A developer bringing up a dev machine sees
`Modules: 1 passed, 1 failed` and a red `preprocessing_runoff (long-horizon sync): FAIL` on a run
whose data outcome is correct, and stops to debug it. Reported by the owner 2026-09-03 after it
cost a colleague exactly that.
**Labels**: `infra`, `run_locally`, `reporting`, `developer-experience`
**Found**: 2026-09-03, diagnosing a `maintenance:preprocessing_runoff` run on kyg.
**Supersedes a prior decision**: PREPQ-014/PREPQ-015 (2026-08-21) accepted the recurring FAIL row as
the cost of not building an outage blind spot. **The owner has now reversed the reporting half of
that decision** (2026-09-03): a known degradation must not be rendered as a failure. This issue
implements the reversal *without* re-opening the blind spot — see "The risk this issue must not
create".
**Related**: **INFRA-037** (created the exit-4 carve-out this issue refines), **INFRA-024** (the
sibling defect: `print_summary` normalises every recorded FAIL to exit 1), **PREPQ-014** (why exit 4
recurs), **PREPQ-015** (whose shipped design explicitly *relies* on today's behaviour — see below),
**PREPQ-020** (the short-horizon twin, found in the same investigation), **INFRA-030** (wants a `SKIP` status and records that `print_summary` "classifies status **binarily** … so a naive `SKIP` status would print as FAIL" — C2 below is the enabling change; see "Overlap with INFRA-030").

---

## Summary

`run_locally.sh` has exactly two result states, `PASS` and everything-else-rendered-as-`FAIL`
(`:1847-1853`). The long-horizon sync's exit 4 — "at least one station's iEH-HF monthly-norm lookup
raised" — is a **known, structural, upstream** condition on kyg that recurs on every run and
destroys no data (PREPQ-015 writes the station's rows regardless). It is currently rendered with the
same vocabulary and the same colour as a genuine failure, and it makes the whole script exit 1.

## Current behaviour (verified 2026-09-03 on kyg)

```
LONG-HORIZON RUN SUMMARY
total_attempted=62  written=53  norm_absent=5  sdk_failed=4  api_failed=0
DEGRADED: monthly discharge norms unavailable for 5/62 stations; ...
[ERROR] Long-horizon hydrograph sync had SDK norm lookup failure(s)
[OK] preprocessing_runoff maintenance completed in 10m 56s
PIPELINE SUMMARY
  preprocessing_runoff (long-horizon sync): FAIL (1m 39s)
  preprocessing_runoff (maintenance): PASS (10m 56s)
Modules: 1 passed, 1 failed
```

The mechanism, end to end:

| Site | Behaviour |
|---|---|
| `_exit_code_for_long_horizon_summary` (`sync_long_horizon_hydrograph.py:629-641`) | `api_failed≥1` → 5; else `sdk_failed≥1` → 4; else 0. **`norm_absent` never affects the exit code** — the `DEGRADED: 5/62` line is a separate, already-correct non-fatal warning (`:644-659`). |
| `run_maintenance_preprocessing_runoff` (`run_locally.sh:923-934`) | `lt_rc=4` → `log ERROR` ×2 + `record_result … "FAIL"`; module `rc` stays 0, so the module row is `PASS`. |
| `print_summary` (`:1847-1853`) | any status ≠ `PASS` → `log ERROR "… FAIL"` and `fail_count++`. |
| `print_summary` (`:1901-1904`) | `fail_count > 0` → `return 1`. |
| `main()` (`:2346-2348`) | `print_summary || exit_code=1` → the script exits 1. |

## Owner decision (2026-09-04): a missing norm is not our failure

> "A missing norm is not our problem. It may be an informational log, not an error. Norms are
> provided by iEH HF. If there are no norms, it's not our issue."

That is stronger than the `DEGRADED` row this issue originally proposed, and it simplifies the
change:

- **Exit 4 (one or more stations' norm lookup raised, no API failures) stops being a reported
  failure at all.** No `FAIL` row, no `DEGRADED` row — an INFO line naming the counts, and the
  module carries on. `run_locally.sh` exits 0.
- The `LONG-HORIZON RUN SUMMARY` counts block stays exactly as it is. It is the record of what
  happened, and it is already printed unconditionally.

**The one thing this must not throw away.** Exit 4 cannot tell "iEH HF has no norm for this station"
from "iEH HF answered 401/500/502": `_lookup_monthly_norms` catches bare `Exception`
(`sync_long_horizon_hydrograph.py:295-304`), and the SDK raises the identical
`ValueError: No path provided or the provided path is None` for **any** non-200
(`ieasyhydro_sdk/sdk_endpoint_definitions.py:90-109` → `sdk_base.py:64`). Three attempts to grade
that exception were made and refuted (PREPQ-014).

So making exit 4 informational removes the *graded* signal for a service-wide iEH-HF outage on this
path. **C1's exit 6 is therefore not optional** — it is the fatal classification specifically for
`sdk_failed == total_attempted` after station enumeration, and it is what separates "norms are
absent, which is fine" from "iEH HF is down, which is not".
`doc/prod/kghm_pipeline_handover.md:159-176` already tells operators to apply exactly that rule by
hand.

**It is not the only remaining trace, and the issue should not claim it is.** The counts block is
printed unconditionally (`sync_long_horizon_hydrograph.py:662`); a failure before the loop — SDK
construction, station discovery — still escapes as exit 3 (`:747`); the yearly wrapper propagates
every non-zero status (`bin/yearly_runoff_hydrograph_aggregation.sh:219`); and the short-horizon path
logs its own norm-call failures independently (`sync_short_horizon_hydrograph.py:630`). What exit 6
uniquely provides is a *fatal* classification for the all-stations case.

**Net effect on the reported symptom**: the developer who prompted this issue sees no red row and an
exit 0, which is the outcome asked for — and a total outage still stops the run.

## The contract

**C1 — the writer distinguishes partial from total, not the shell.** In
`_exit_code_for_long_horizon_summary` (`sync_long_horizon_hydrograph.py:629-641`), add exit **6**
before the existing `sdk_failed` branch:

```
api_failed >= 1                                            -> 5   (unchanged)
sdk_failed >= 1 and sdk_failed == total_attempted > 0      -> 6   (NEW: total norm-lookup outage)
sdk_failed >= 1                                            -> 4   (unchanged value, new meaning: PARTIAL)
otherwise                                                  -> 0   (unchanged)
```

Exit 6 is currently unused (1 = setup/runtime, 2 = no sites/records, 3 = unexpected exception,
4 = SDK, 5 = API). Guard `total_attempted > 0` explicitly — a zero-station run already exits 2 and
must not become 6. Update the function's docstring, whose current wording asserts "exit code 4
implies no API read/write failures occurred this run" — that invariant survives, but the
partial-vs-total distinction must be stated alongside it.

**C1a — the CLI must learn code 6 too, or it will mislabel the outage it exists to report.**
Two further sites in the same file hard-code the 0-5 taxonomy:

- `main()`'s docstring enumerates only 0-5 (`sync_long_horizon_hydrograph.py:727-739`).
- `main()`'s reporting branch is `if exit_code == 4: <SDK wording> else: <API wording>`
  (`:772-785`). **Exit 6 would fall into the `else` and log "completed with 0 API read/write
  failure(s)"** — the exact opposite of what happened. Route 4 **and** 6 through SDK-specific
  wording (6 should say every attempted station's norm lookup failed, and name it as an outage
  signature), leaving 5 on the API wording.

This is inside the one Python file C1 already touches; it is not a scope expansion, it is the rest
of the same change. Its integration tests need the same update.

**C2 — `run_locally.sh` gains a real DEGRADED result state.**
- `print_summary` (`:1840-1854`) renders `DEGRADED` via `log WARN "  ${mod}: DEGRADED (${duration})"`
  and counts it in a new `degraded_count`, separate from both `pass_count` and `fail_count`.
- Apply the identical three-way branch to the validation loop (`:1864-1878`), which shares the
  `PASS`/else shape. It records no `DEGRADED` today; leaving it two-way plants a trap where a future
  degraded validation row renders as `FAIL`.
- The totals line (`:1883-1887`) becomes `Modules: N passed, N degraded, N failed`, with the
  degraded term **omitted when the count is 0** so existing output is unchanged on clean runs.
- Validation totals are computed independently (`:1864-1878`, `:1884-1886`), so add a matching
  `val_degraded` counter and the same conditional term there. No validation row emits `DEGRADED`
  today; adding the counter is what stops the next one from being silently rendered as `FAIL`.
- `print_summary` still returns 1 only for `fail_count > 0 || val_fail > 0` (`:1901-1904`). A
  degraded-only run exits **0**. This is the point of the issue.

**C3 — `print_error_details` must not lose the DEGRADED tail.** It filters on `"FAIL"` twice
(`:1791-1796` and `:1809-1810`). INFRA-037's whole design for exit 4 is that the counts reach the
operator *through that tail* — the code comment at `:925-930` says so explicitly. Emit a second,
non-red block for degraded rows (label `MODULE DEGRADED DETAILS`, same `ERROR_TAIL_LINES` tail,
`YELLOW` not `RED`), or generalise the helper to take the status to match plus a colour. Losing the
tail would trade one bad outcome for a worse one: a yellow row with no counts.

**C4 — the exit-4 branch becomes informational; a new exit-6 branch keeps today's FAIL.**
In `run_maintenance_preprocessing_runoff` (`run_locally.sh:923-934`):

- `lt_rc = 4` → `log INFO` (not ERROR, not WARN) stating that one or more stations' monthly norm
  lookup did not return a norm, that this is an upstream condition and not a failure, and pointing
  at the `LONG-HORIZON RUN SUMMARY` counts. **Record no result row at all** — not `FAIL`, not
  `DEGRADED`. Module `rc` stays 0, exactly as today.

  > **The Python side must change too, or the shell's INFO is cosmetic.** `main()` currently logs
  > the exit-4 case at **ERROR** (`sync_long_horizon_hydrograph.py:772`), and `run_in_venv` tees the
  > child's output straight to the operator (`run_locally.sh:639`) — so an ERROR line reaches the
  > log regardless of what the shell does. Log exit 4 at INFO and exit 6 at ERROR in the writer.
  >
  > **And word it accurately**: exit 4 means the norm lookup *raised*, caught by a bare `except
  > Exception` (`:295-304`). That is "no norm was obtained", not "the station has no norm" — the
  > code cannot tell the difference. Do not write a message that asserts absence.
- `lt_rc = 6` → **byte-for-byte today's exit-4 handling**: `log ERROR` ×2,
  `record_result … "FAIL"`, `rc` stays 0. Add it as an explicit branch; do not let 6 fall into the
  `elif [ $lt_rc -ne 0 ]` catch-all, which sets `rc=$lt_rc` and would newly fail the whole
  maintenance module.
- Exits 1/2/3/5 unchanged.

> **Why the `DEGRADED` state is still worth building (C2/C3).** With C4 as decided, the long-horizon
> sync no longer produces a degraded row, so C2 ships with no in-tree producer. Keep it anyway: it
> is the enabling change for **INFRA-030** (`SKIP`), it is what **LTF-010** would need if the
> recovery's refusal classes are ever split, and building the three-way renderer now is cheaper than
> retrofitting it later. **If you would rather not add an unused state, C2/C3 can be dropped and
> this issue reduced to C1 + C4** — say so, and the phases collapse to two. The tests in this issue
> are written so that decision is a deletion, not a rewrite.

**C5 — no other target changes.** No other `record_result` call site becomes `DEGRADED` in this
issue. Adding the state is the deliverable; classifying other modules into it is not.

## Blast radius outside `run_locally.sh`

- `bin/yearly_runoff_hydrograph_aggregation.sh` propagates the writer's exit code verbatim
  (`:219-220` reads `docker inspect … .State.ExitCode`, `:241` exits it). After C1 that cron job can
  emit **6**; it needs no code change (its `!= 0` branch already logs a warning) but any monitoring
  that enumerates expected codes must learn 6. **This is a behaviour change on a production cron.**
- `bin/backfill_discharge_aggregation.sh` is status-blind by design (kghm handover §1 table) and is
  unaffected.
- `bin/initialize_regenerate_hooks.sh` and `bin/dev_local_backfill.sh` also reach this writer
  indirectly. Both handle a non-zero status generically, so exit 6 does not break them — recorded
  here so the analysis is complete, not because they need edits.
- `apps/preprocessing_runoff/backfill_discharge_aggregation.py` is a *function* caller, not an
  exit-code consumer, and discards the writer's status metadata (`:91-110`). Unaffected.
- The nightly production cron does **not** invoke `sync_long_horizon_hydrograph.py` at all
  (PREPQ-014), so no nightly pipeline changes.

## Files that may be modified

Implementation:

- `apps/run_locally.sh` (C2, C3, C4)
- `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py` — **C1 and C1a only**: the exit-code
  helper (`:629-641`), `main()`'s exit-code docstring (`:727-739`), and `main()`'s SDK-vs-API
  reporting branch (`:772-785`). Nothing else in the file.

Tests that lock the current taxonomy and must move with the code (see the sweep section):

- `apps/preprocessing_runoff/test/test_sync_long_horizon_hydrograph.py`
- `apps/pipeline/tests/test_run_locally_orchestration.py` — its module docstring names the exit-4
  downgrade as locked behaviour "C"; that description becomes stale on merge and must be rewritten,
  not just its assertions.
- `apps/preprocessing_runoff/test/test_run_locally_long_horizon_wiring.py`

Documentation: whatever the sweep grep turns up, plus `apps/preprocessing_runoff/README.md` and the
header comment of `bin/yearly_runoff_hydrograph_aggregation.sh`.

**Do not** change `_summarize_long_horizon_station_statuses`, the status enum, the writers, the
`DEGRADED:` summary line, `record_result`'s signature, or any exit-code value other than adding 6.

## Documentation and test sweep — do this grep-first, not table-first

Many passages across `doc/`, `apps/` and `bin/` assert the current "exit 4 → FAIL row → overall exit
1" contract, and a partial sweep closes this issue while leaving the wrong contract documented. The
table below is a starting point, **not** the authoritative list. Run this first and classify every
hit before editing anything:

```bash
grep -rn "exit 4\|exit code 4\|lt_rc\|long-horizon sync): FAIL\|exit non-zero" \
  doc/ bin/ apps/run_locally.sh apps/preprocessing_runoff/README.md \
  apps/preprocessing_runoff/ apps/pipeline/tests/
```

Classify each hit as one of:

1. **Current-contract assertion** → must be updated.
2. **Historical record** (a dated review checklist, an archived issue, a "what was wrong" section of
   a handover) → **leave as written**. Do not retro-fit exit 6 into an account of something that
   happened before it existed.
3. **Unrelated** (another module's exit 4, an unrelated "exit non-zero") → leave.

Files confirmed to contain hits as of 2026-09-03 (classification still required per hit):

| File | Notes |
|---|---|
| `doc/prod/kghm_pipeline_handover.md` | §4 "expected signature", "exit non-zero by design" (`:8`, `:80`), the §1 route table (`:47-51`), `:68`, `:127`, `:151`, `:157-181`. §4's **manual** `sdk_failed`-vs-`total_attempted` check becomes the automated exit-6 rule; keep the guidance about re-baselining the count and reading the exception text. §1's "what was wrong" narrative is class 2. |
| `doc/prod/ml_no_forecasts_debug_runbook.md` | `:124`, `:152-166`, `:727` — the INFRA-037 row and the Step-5 note both say "still exits non-zero overall … records a FAIL line". |
| `doc/plans/module_issues.md` | INFRA-024, INFRA-030, INFRA-037, PREPQ-014, PREPQ-015 rows. **PREPQ-015's "'all-failed still exits non-zero' already holds with no code change needed" must now cite exit 6.** Add the INFRA-044 row. |
| `doc/plans/issues/review_gi_draft_prepq_longhorizon_sdk_failure_drops_station.md` | `:92`, `:130`, `:178`, and its test table (~`:198`). |
| `doc/plans/issues/mid_prio_gi_draft_infra_module_failures_unattributable.md` | `:56`, `:69-75`, `:143-145` — quotes the `lt_rc -eq 4` branch verbatim. |
| `doc/plans/issues/mid_prio_gi_draft_prepq_longhorizon_narrow_api_exception_handler.md` | `:79` — reasons about `run_locally.sh`'s generic `elif [ $lt_rc -ne 0 ]` branch, which C4 now steps in front of for code 6. |
| `doc/plans/issues/mid_prio_gi_draft_prepq_backfill_discharge_discards_writer_status.md` | `:35`, `:80-87`, `:99-106`. |
| `doc/plans/issues/low_prio_gi_draft_prepq_long_horizon_sdk_norm_path_none.md` | "Operational consequence" — says the target "reports FAIL on every run while this persists". |
| `doc/plans/issues/review_gi_draft_infra_run_locally_aborts_on_expected_preprocessing_failure.md` | INFRA-037's own shipped-behaviour description. |
| `apps/preprocessing_runoff/README.md` | The long-horizon behaviour/exit-code section (~`:175`). |
| `bin/yearly_runoff_hydrograph_aggregation.sh` | Header comment (~`:16-27`) states the writer's exit-code contract; add 6. |
| `apps/run_locally.sh` | The `:923-934` comments, and `print_usage` if it describes result states. |

**Tests that lock the old taxonomy** and must be updated in the same change (they are the reason a
docs-only sweep would leave the build red):

- `apps/preprocessing_runoff/test/test_sync_long_horizon_hydrograph.py` (~`:951`) — exit-code cases.
- `apps/pipeline/tests/test_run_locally_orchestration.py` (~`:1051`) — summary/exit expectations.
- `apps/preprocessing_runoff/test/test_run_locally_long_horizon_wiring.py` (~`:14`) — the wiring
  contract between `lt_rc` and the recorded row.

Line numbers are as of 2026-09-03; re-derive them with `grep -n` at implementation time.

## Tests

1. **Exit-code unit tests** for `_exit_code_for_long_horizon_summary`: partial SDK failure → 4;
   `sdk_failed == total_attempted` → 6; `sdk_failed == total_attempted` **with** an API failure → 5
   (API precedence survives); `total_attempted == 0` → not 6; no failures → 0.
1b. **CLI reporting for code 6 (C1a).** `main()` under an all-stations-SDK-failed summary logs
   SDK-failure wording and **must not** log "API read/write failure(s)" — the defect the `else`
   branch at `:772-785` would otherwise produce. Assert on the emitted message, not just the code.
2. **`print_summary` three-way rendering**: a `DEGRADED` row logs `WARN`, is excluded from
   `fail_count`, appears in the totals line, and `print_summary` returns **0**.
3. **Mixed run**: one `PASS`, one `DEGRADED`, one `FAIL` → returns 1, totals read
   `1 passed, 1 degraded, 1 failed`, both detail blocks print.
4. **Clean run is byte-identical**: with zero degraded rows the totals line has no degraded term,
   for the module totals **and** the validation totals.
5. **`print_error_details` covers DEGRADED**: the tail of a degraded row's log is printed, and the
   `LONG-HORIZON RUN SUMMARY` counts are inside that tail — the whole point of INFRA-037's design.
6. **Branch routing (per C4, the owner decision)**: `lt_rc=4` logs at INFO, records **no result row
   at all**, and leaves module `rc=0`; `lt_rc=6` records `FAIL` and leaves module `rc=0`; `lt_rc=5`
   still sets `rc=5`; `lt_rc=3` still hits the catch-all; in every fatal case `CURRENT_MODULE_LOG`
   still points at the long-horizon log when the row is recorded (the existing
   `test_run_locally_orchestration.py` contract, which must not regress).
   **Assert the absence of a row for `lt_rc=4`** — that is the decision, and a test that merely
   checks "not FAIL" would pass against a DEGRADED row too.
7. **Regression guard for the PREPQ-015 property**: a simulated all-stations-SDK-failed long-horizon
   run makes `run_locally.sh` exit non-zero. This is the test that proves the blind spot was not
   reintroduced; it is the most important one in this issue.
8. **Partial-SDK-failure run exits 0 end to end**: the maintenance target with a partial SDK
   failure and nothing else wrong returns process status 0 **and prints no failure or degraded row
   for the long-horizon sync**.

## Acceptance criteria

- [ ] On kyg, `bash apps/run_locally.sh maintenance:preprocessing_runoff` with the known 4-station
      signature prints **no result row** for the long-horizon sync, logs the condition at INFO, the
      `LONG-HORIZON RUN SUMMARY` counts still appear in the run output, and the script **exits 0**.
- [ ] A simulated total SDK outage still prints a red `FAIL` row, exits non-zero, and its log names
      an SDK failure — not an API failure.
- [ ] No run that is genuinely failing today starts passing, other than the exit-4 case.
- [ ] The sweep grep was run, **every hit was classified in the agent's report** (current-contract /
      historical / unrelated), and every current-contract hit was updated. A hit list without
      classifications does not satisfy this criterion — a partial doc fix closes the issue while
      leaving the wrong contract documented.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero unexpected skips.
- [ ] `bash -n apps/run_locally.sh` clean; `ruff check` / `ruff format --check` clean on the changed
      Python file.

## Phases

Each phase updates the tests that its own change would otherwise break, so the suite is green at
every phase boundary — CLAUDE.md's "work is not review-eligible until the full affected-scope tests
pass" is a per-phase precondition, not a pre-PR one.

- **P1 — writer exit taxonomy (C1, C1a).** Files: `sync_long_horizon_hydrograph.py`,
  `test_sync_long_horizon_hydrograph.py`. Depends on: none. Agents: 1.
  Accept: tests 1 and 1b pass; no other exit-code value, status, or writer behaviour changes.
- **P2 — DEGRADED state in the summary (C2, C3).** Files: `run_locally.sh`,
  `test_run_locally_orchestration.py`. Depends on: none (independent of P1). Agents: 1.
  Accept: tests 2-5 pass.
- **P3 — branch routing (C4).** Files: `run_locally.sh`, `test_run_locally_orchestration.py`,
  `test_run_locally_long_horizon_wiring.py`. Depends on: P1 (needs code 6 to exist). **Not** on P2 —
  C4 records no row for exit 4, so it needs no `DEGRADED` state. Agents: 1. Accept: tests 6, 7, 8 pass, and the orchestration test file's
  module docstring describing the old exit-4 behaviour is rewritten.
- **P4 — documentation sweep.** Files: documentation only. Depends on: P1, P2, P3. Agents: 1.
  Accept: the classified hit list is in the agent's report and every current-contract hit is fixed.

P2 is the only phase that can run concurrently with P1. Do not parallelise P3 against either.

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": [], "parallel_agents": 1 },
    "P3": { "depends_on": ["P1", "P2"], "parallel_agents": 1 },
    "P4": { "depends_on": ["P1", "P2", "P3"], "parallel_agents": 1 }
  }
}
```

## Open decision for the owner

**Is the `DEGRADED` state (C2/C3) still worth building?** After the owner decision of 2026-09-04 it
ships with **no in-tree producer**: exit 4 records no row, and nothing else emits a degraded result
today. Keeping it is a bet on near-term consumers — **INFRA-030** (`SKIP` for skipped modules) and
**LTF-011** (a benign recovery refusal, once its two causes are split). Dropping it reduces this
issue to C1 + C1a + C4 and collapses the phases to two.

**Recommendation: keep it**, because both consumers are already filed and the three-way renderer is
cheaper to build once than to retrofit. But it is a judgement call about unused code, not a
correctness question — say the word and P2 is deleted rather than rewritten.

*(The earlier version of this section asked whether a partial-but-growing failure count should be
loud. That question is answered by the 2026-09-04 decision: a missing norm is not our failure, so
4/62 and 40/62 are both silent, and only 62/62 is fatal.)*

## Corrections applied after out-of-loop review (2026-09-03)

- **Blocker.** C1 originally scoped the Python edit to the exit-code helper alone. `main()`'s
  reporting branch (`sync_long_horizon_hydrograph.py:772-785`) is `if exit_code == 4: <SDK> else:
  <API>`, so exit 6 would have logged "completed with **0** API read/write failure(s)" — the
  opposite of the outage it exists to announce. Added as C1a, with test 1b.
- The validation-summary totals are computed independently of the module totals; C2 now names
  `val_degraded` explicitly instead of leaving it implied.
- The documentation sweep was a fixed table; it is now grep-first with a mandatory
  current-contract / historical / unrelated classification, because the earlier table missed
  `apps/preprocessing_runoff/README.md`, `bin/yearly_runoff_hydrograph_aggregation.sh`, two further
  `doc/plans/issues/` drafts, and — more importantly — three **test** files that lock the old
  taxonomy and would have left the suite red.
- Phases were restructured so each one carries its own test updates; the previous split would have
  left `test_run_locally_orchestration.py` failing between P2 and P4.
- Blast radius gained `bin/initialize_regenerate_hooks.sh`, `bin/dev_local_backfill.sh` (generic
  non-zero handling, no edits needed) and `backfill_discharge_aggregation.py` (function caller, not
  an exit-code consumer).
- Verified and unchanged: exit 6 is genuinely unused; `print_summary`/`print_error_details` are the
  only two places in `run_locally.sh` that compare a result status string, so no third site
  misbehaves on `DEGRADED`; and the PIPELINE_ABORTED / `--continue-on-error` / final-exit trace in
  C4 is correct as written.

## Corrections applied after the decision-fold review (2026-09-04)

- The decision that exit 4 records **no row** had not been carried into tests 6 and 8, the
  acceptance criteria, P3's dependency, or the open decision — all four still expected a `DEGRADED`
  row from the long-horizon sync. Fixed; test 6 now asserts the *absence* of a row.
- C4 gained the Python side: `main()` logs exit 4 at ERROR (`:772`) and `run_in_venv` tees it to the
  operator, so the shell's INFO alone would be cosmetic. Also reworded — exit 4 means the lookup
  *raised*, which is not the same as "the station has no norm".
- Narrowed the "sole remaining outage signal" claim: the counts block, exit 3, the yearly wrapper
  and the short-horizon path all still leave traces. Exit 6 is the *fatal classification*, not the
  only signal.

## Out of scope

- INFRA-024's other half (specific exit codes normalised to 1 for genuinely failed modules).
- Classifying any other module's outcome as DEGRADED (C5).
- Adding a `SKIP` state (INFRA-030) — this issue only unblocks it.
- Anything about *why* the SDK raises (PREPQ-014, upstream).
