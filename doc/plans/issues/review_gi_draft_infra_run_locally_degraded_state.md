# INFRA-044: a known upstream data gap is reported as a failure

**Status**: Review (2026-09-07; refined 2026-09-08 — see "Follow-up: 2026-09-08 owner
decision" below)
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
**PREPQ-020** (the short-horizon twin, found in the same investigation), **INFRA-030** (SHIPPED 2026-09-07, PR #497 — it added the `SKIP` status and the three-way renderer itself, so this issue no longer enables it and C2/C3 are withdrawn).

**Implementation note (2026-09-07).** P1 and P2 are both implemented (see "Phases" and the ticked
checklists below). Every `:NNN` line citation elsewhere in this file — in "Current behaviour", "The
contract", and "Blast radius" — describes the **pre-implementation** snapshot (2026-09-03) that
those sections were written against, i.e. what to look for and where, before the change. It is
**not** re-derived here; re-run `grep -n` for the current locations. As implemented, the real
anchors are: `_exit_code_for_long_horizon_summary` at `sync_long_horizon_hydrograph.py:629-654`;
`main()`'s exit-code docstring at `:744-759`; `main()`'s SDK-vs-API reporting branch at `:792-819`;
`run_maintenance_preprocessing_runoff` at `run_locally.sh:925-1010` (its `lt_rc` branch chain,
including the new `-eq 6` branch, at `:962-1005`).

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
| `print_summary` | since INFRA-030 (PR #497) this is a three-way `PASS` / `SKIP` / else branch; a recorded `FAIL` still lands in the `else`, so it is rendered as a failure and increments `fail_count` exactly as before. Re-derive line numbers with `grep -n`. |
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

*(2026-09-08 note: this paragraph describes the `_get_site_uuid_for_site_code` failure shape
specifically — no status code is available there, and it is still treated conservatively as
`SDK_FAILED`, unchanged. A **different**, narrower call site — `get_norm_for_site`'s own non-200
response — DOES embed a status code in its message and is now gradeable. See "Follow-up:
2026-09-08 owner decision" below; it does not reopen or contradict PREPQ-014's refutations, which
were about this exact ungraded exception.)*

So making exit 4 informational removes the *graded* signal for a possible service-wide iEH-HF
outage on this path. **C1's exit 6 is therefore not optional** — it is the fatal classification
specifically for `sdk_failed == total_attempted` after station enumeration, and it is what
separates "norms are absent, which is fine" from "iEH HF might be down, which is not" (the ratio
alone cannot prove the latter — a single-station run whose only attempted station has a
structural, station-level lookup failure produces the identical shape; see the exception-handling
note above). `doc/prod/kghm_pipeline_handover.md:159-176` already tells operators to apply exactly
that rule by hand.

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
  wording (6 should say every attempted station's norm lookup failed, and describe it as
  consistent with a service-wide outage rather than asserting one), leaving 5 on the API wording.

This is inside the one Python file C1 already touches; it is not a scope expansion, it is the rest
of the same change. Its integration tests need the same update.

**C2 / C3 — WITHDRAWN (owner decision, 2026-09-04).** Earlier revisions added a third `DEGRADED`
result state to `print_summary` and `print_error_details`. After the decision that a missing norm is
not our failure, exit 4 records **no row at all**, so that state would ship with **no producer in
the tree** and no test able to exercise it end to end. It is dropped rather than built unused.

If a consumer appears — **INFRA-030** (`SKIP` for skipped modules) or **LTF-011** (a benign recovery
refusal, once its causes are split) — add it then. It is about fifteen lines in one function, and
building it now would mean shipping code that nothing reaches.

**Consequence for this issue**: `run_locally.sh`'s result rendering is **unchanged**. The only shell
change is C4's branch routing. That is deliberate and makes this issue much smaller than its title
suggests.

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

> **Superseded note (2026-09-07).** An earlier revision argued here that the `DEGRADED` renderer
> was worth building anyway, as the enabling change for INFRA-030's `SKIP`. **That rationale is
> gone**: INFRA-030 shipped (PR #497) and built its own three-way `PASS`/`SKIP`/else branch in
> `print_summary`, so nothing is waiting on a `DEGRADED` state. C2/C3 stay withdrawn. If a degraded
> state is ever wanted, it is now an `elif` alongside the existing `SKIP` branch, not a
> restructure.

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

- `apps/run_locally.sh` (**C4 only** — the branch routing; the result renderer is untouched)
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
   **DONE (P1)** — `apps/preprocessing_runoff/test/test_sync_long_horizon_hydrograph.py`.
1b. **CLI reporting for code 6 (C1a).** `main()` under an all-stations-SDK-failed summary logs
   SDK-failure wording and **must not** log "API read/write failure(s)" — the defect the `else`
   branch at `:772-785` would otherwise produce. Assert on the emitted message, not just the code.
   **DONE (P1)** — same file.
6. **Branch routing (per C4, the owner decision)**: `lt_rc=4` logs at INFO, records **no result row
   at all**, and leaves module `rc=0`; `lt_rc=6` records `FAIL` and leaves module `rc=0`; `lt_rc=5`
   still sets `rc=5`; `lt_rc=3` still hits the catch-all; in every fatal case `CURRENT_MODULE_LOG`
   still points at the long-horizon log when the row is recorded (the existing
   `test_run_locally_orchestration.py` contract, which must not regress).
   **Assert the absence of a row for `lt_rc=4`** — that is the decision, and a test that merely
   checks "not FAIL" would pass against a DEGRADED row too.
   **DONE (P2)** — `apps/pipeline/tests/test_run_locally_orchestration.py::TestLongHorizonSyncExitCodeHandling`
   (`test_exit_code_table[lt_rc=4]`/`[lt_rc=6]`/`[lt_rc=5]`/`[lt_rc=3]`,
   `test_exit_four_logs_info_and_records_no_result_row`,
   `test_exit_six_records_long_horizon_log_not_maintenance_log`,
   `test_fatal_failure_records_long_horizon_log_not_maintenance_log`) plus the static wiring guard
   `apps/preprocessing_runoff/test/test_run_locally_long_horizon_wiring.py`
   (`test_lt_rc_four_is_informational_and_records_no_row`,
   `test_lt_rc_six_matches_historical_exit_four_fail_handling`). Mutation-verified: reverting either
   branch to its pre-INFRA-044 shape fails the relevant tests (see agent report).
7. **Regression guard for the PREPQ-015 property**: a simulated all-stations-SDK-failed long-horizon
   run makes `run_locally.sh` exit non-zero. This is the test that proves the blind spot was not
   reintroduced; it is the most important one in this issue.
   **DONE (P2)** —
   `test_run_locally_orchestration.py::TestLongHorizonSyncExitCodeHandling::test_all_stations_sdk_failure_exits_nonzero_end_to_end`.
8. **Partial-SDK-failure run exits 0 end to end**: the maintenance target with a partial SDK
   failure and nothing else wrong returns process status 0 **and prints no failure or degraded row
   for the long-horizon sync**.
   **DONE (P2)** —
   `test_run_locally_orchestration.py::TestLongHorizonSyncExitCodeHandling::test_partial_sdk_failure_exits_zero_end_to_end`.

## Acceptance criteria

- [x] On kyg, `bash apps/run_locally.sh maintenance:preprocessing_runoff` with the known 4-station
      signature prints **no result row** for the long-horizon sync, logs the condition at INFO, the
      `LONG-HORIZON RUN SUMMARY` counts still appear in the run output, and the script **exits 0**.
      **Verified via the synthetic shell harness** (`test_partial_sdk_failure_exits_zero_end_to_end`,
      `test_exit_code_table[lt_rc=4]`), which drives the real, unmodified `run_locally.sh` with a
      stubbed `sync_long_horizon_hydrograph.py` exit code — not re-verified against a live kyg run
      in this session (no kyg access from this environment).
- [x] A simulated total SDK outage still prints a red `FAIL` row, exits non-zero, and its log names
      an SDK failure — not an API failure.
      `test_all_stations_sdk_failure_exits_nonzero_end_to_end`,
      `test_exit_six_error_points_to_run_summary_and_its_log`.
- [x] No run that is genuinely failing today starts passing, other than the exit-4 case.
      `test_exit_code_table` covers 0/1/2/3/4/5/6 in one parametrized table;
      `test_fatal_code_five_still_aborts_daily_and_ml_never_runs` guards exit 5 specifically.
- [x] The sweep grep was run, **every hit was classified in the agent's report** (current-contract /
      historical / unrelated), and every current-contract hit was updated. A hit list without
      classifications does not satisfy this criterion — a partial doc fix closes the issue while
      leaving the wrong contract documented.
- [x] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero unexpected skips.
      See the agent's verbatim test output in its report; only the two pre-existing
      `test_src.py` skips remain (dependency-gated, not introduced by this change).
- [x] `run_locally.sh`'s result rendering is byte-identical to trunk — the only shell change is the
      exit-4/exit-6 branch routing. Verified by diff: `print_summary`, `print_error_details`, and
      `record_result` are untouched; only `run_maintenance_preprocessing_runoff`'s `lt_rc` branch
      chain changed.
- [x] `bash -n apps/run_locally.sh` clean; `ruff check` / `ruff format --check` clean on the changed
      Python file.

## Phases

- **P1 — writer exit taxonomy (C1, C1a).** Files: `sync_long_horizon_hydrograph.py`,
  `test_sync_long_horizon_hydrograph.py`. Depends on: none. Agents: 1.
  Accept: tests 1 and 1b pass; exit 4 logs at INFO and exit 6 at ERROR; no other exit-code value,
  status or writer behaviour changes.
- **P2 — branch routing + documentation sweep (C4).** Files: `run_locally.sh`,
  `test_run_locally_orchestration.py`, `test_run_locally_long_horizon_wiring.py`, plus the
  documentation listed in the sweep section. Depends on: P1. Agents: 1.
  Accept: tests 6, 7, 8 pass; the orchestration test file's module docstring is rewritten; every
  current-contract documentation hit is classified and updated.

*(Earlier revisions had four phases. P2 and P3 built the `DEGRADED` renderer, withdrawn above.)*

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 }
  }
}
```

## Decisions taken (no open questions remain)

- **2026-09-04 — a missing norm is not our failure.** Exit 4 becomes informational with no result
  row. See the section above.
- **2026-09-04 — the `DEGRADED` state is not built.** It would have no producer; see C2/C3.
- Exit 6 stays, and is now the fatal classification for `sdk_failed == total_attempted`.

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

## Follow-up: 2026-09-08 owner decision — grade norm-lookup failures by HTTP status code

**Why this was needed.** The owner identified a deployment (Tajik Hydromet) that has entered **no
monthly discharge norms at all**. On that deployment, if iEH HF answers a monthly-norm request with
a non-200 for a station with no norm on file (observed shape: HTTP 404), **every** attempted
station's `_lookup_monthly_norms` call raises. That makes `sdk_failed == total_attempted > 0` on
every single run — the exact TOTAL-outage shape C1 built exit 6 for — so this issue's own fix would
have reintroduced the false alarm it exists to remove: a red `FAIL` row and a non-zero exit code on
a deployment that is behaving correctly (no norms is expected there, not a fault).

**Owner decision (2026-09-08)**: grade the raised exception by its HTTP status code —
"there is no norm here" (404) must stay informational (`NORM_ABSENT`, unchanged exit-code
consequences); "the service answered with something else" (401/403/400/5xx/etc.) or "no status
code could be recovered at all" must still be fatal-eligible (`SDK_FAILED`, unchanged from before
this refinement).

**Why this is now possible when PREPQ-014's three attempts were refuted.** Those refutations (see
the 2026-09-08 note earlier in this file, and PREPQ-014/PREPQ-015 in
`doc/plans/module_issues.md`) were all about grading the `_get_site_uuid_for_site_code` failure
shape — `ValueError: No path provided or the provided path is None`, raised by
`sdk_base.py`'s `_call_api` when `relative_url is None` — which carries **no status code at all**
and was graded against out-of-band signals (a local config list, the SDK's own
`get_virtual_sites()` list) that were each shown to be unsafe or stale. This refinement does not
revisit that shape; it stays `SDK_FAILED` exactly as before, conservatively.

What changed is a **different** call site: `IEasyHydroHFSDK.get_norm_for_site`
(`ieasyhydro_sdk/sdk.py`, verified at the installed package's line ~245) raises
`ValueError(f"Could not retrieve {norm_type} norm for site {site_code}, got status code
{norm_response.status_code}")` when the norm endpoint itself (reached *after* the site UUID
resolves successfully) returns non-200. That message embeds the actual status code, so — unlike
the ungraded shape — it can be parsed directly from the exception's own text with no out-of-band
lookup, and PREPQ-014's refutations (which targeted an inference from a *different* signal) do not
apply to it.

**Implementation** (`apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`; regex and
grading tightened 2026-09-08 by the out-of-loop cross-check below — this section describes the
**current** shape, not the as-first-implemented one):

- `_SDK_NORM_LOOKUP_FAILURE_PATTERN = re.compile(r"^Could not retrieve \S+ norm for site .+, got
  status code (\d{3})$")` — module-level regex matching the `get_norm_for_site` message shape
  above **in full, anchored start-to-end** (originally a loose `"status code (\d{3})"` substring
  search; widened to anchored after the cross-check found it over-matched — see "Corrections
  applied after the out-of-loop cross-check" below).
- `_extract_sdk_status_code(exc)` — returns a status code ONLY when `exc` is a `ValueError` AND
  `str(exc)` matches the anchored pattern in full; any other exception type, or any message that
  merely *contains* a matching phrase, returns `None`. Wrapped in `try/except Exception: return
  None` so a malformed or unexpected exception can never propagate out of the classifier — parsing
  failure always falls back to the conservative branch.
- `_lookup_monthly_norms` — on a caught exception, calls `_extract_sdk_status_code`:
  - status code `404` → `_NormClassification.NORM_ABSENT`. Logged at INFO, naming the station and
    the original exception — but this line is **not** an operator-visible signal in production:
    the root logger is capped at WARNING (`setup_library`, INFRA-029), so it never appears in a
    production log (this was the original, incorrect claim in this section — see "Corrections
    applied" below for what actually surfaces).
  - any other status code, or `None` (no parseable code — the `_get_site_uuid_for_site_code`
    shape, a timeout, a connection error, or anything unrecognised) → `_NormClassification.
    SDK_FAILED` (logged at DEBUG with the parsed `status_code`, which is `None` when unparseable).
    Unknown is treated as broken, not absent — the conservative default the task required.

**What did NOT change**: `_classify_monthly_norms` (the successful-response classifier),
`_exit_code_for_long_horizon_summary`, the exit-4/exit-6 semantics, `write_station_monthly_hydrograph`'s
read-merge behaviour, or any writer/API behaviour. A 404-raising station now lands in the same
`NORM_ABSENT` bucket a 200-with-empty-payload station already used — this is a reclassification of
which bucket some previously-`SDK_FAILED` stations fall into, not a new bucket or a new code path
downstream of classification.

**Consequence to document, not hide**: `norm_absent` (both the `LONG-HORIZON RUN SUMMARY` counts
line and the `DEGRADED:` line) now also counts "the norms endpoint answered 404 for this station".
A 404 can mean either "this station genuinely has no norm entered" (the motivating case above) or
"iEH HF does not recognise this *site* at all" — a configuration problem, not a data gap — and the
status code alone cannot distinguish the two. Operators must not read `norm_absent` as strict
confirmation "site exists, norm missing"; a station worth investigating can hide in that bucket.
Documented in the `_NormClassification` enum docstring, `apps/preprocessing_runoff/README.md`, and
`doc/prod/kghm_pipeline_handover.md` §4.

**Tests added** (`apps/preprocessing_runoff/test/test_sync_long_horizon_hydrograph.py`):
regex-parsing unit tests (`_extract_sdk_status_code`, including the no-match and malformed-input
cases); `_lookup_monthly_norms` classification tests for 404, 401, 500, the ungraded "No path
provided" message, and a non-`ValueError` exception; and three orchestrator-level end-to-end tests
using `write_long_horizon_hydrograph` — all-404 (the motivating case: `sdk_failed == 0`,
`norm_absent == total_attempted`, exit 0, empty `failed_station_codes`), all-500 (still exit 6,
proving the alarm was not silenced), and a 404/500 mix (only the 500 drives `sdk_failed`; exit 4
because it does not account for every attempted station). All new tests were mutation-verified:
disabling the 404 branch, over-widening it to match any status code, and disabling status-code
parsing entirely each broke the tests written to catch exactly that regression, then were
restored.

**Superseded by the 2026-09-08 out-of-loop cross-check below** — the regex-parsing description
above and the INFO-visibility claim in the `_lookup_monthly_norms` bullet were both found
inaccurate by that review; see "Corrections applied after the out-of-loop cross-check" for what
changed and why, and the additional regression test it added.

## Corrections applied after the out-of-loop cross-check (2026-09-08)

An out-of-loop cross-check of the 2026-09-08 status-code grading above found three issues; all
three were fixed on `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py` /
`test_sync_long_horizon_hydrograph.py` in the same branch (`fix_infra044_degraded_state`).

- **Finding 1 (Important) — the original regex over-matched, and could silence a real outage.**
  `_SDK_STATUS_CODE_PATTERN = re.compile(r"status code (\d{3})")` searched *any* exception's text
  for the *first* occurrence. Two concrete failures: a `ConnectionError("proxy returned status
  code 404")` (a genuine connection failure, not a norm-lookup 404) would have been misclassified
  `NORM_ABSENT` — if every station raised that, the run would exit **0** instead of the correct
  fatal exit, a real outage reported as success; and a chained/composite message embedding more
  than one status code (e.g. "... upstream status code 404; final response got status code 503")
  would have had its *first* match (404) extracted, hiding the real (503) failure. **Fixed**:
  renamed to `_SDK_NORM_LOOKUP_FAILURE_PATTERN`, anchored start-to-end to the *exact*
  `get_norm_for_site` message shape (confirmed against the installed SDK source), and
  `_extract_sdk_status_code` now additionally requires `isinstance(exc, ValueError)`. Anything
  that doesn't match that exact shape — including both cases above — now falls through to
  `SDK_FAILED`, which is the fail-closed default this issue's own taxonomy already relies on.
  Verified: mutating the pattern back to the loose search reproduces both failures and is caught
  by four new tests (`test_extract_sdk_status_code_does_not_match_connectionerror_with_status_
  phrase`, `test_extract_sdk_status_code_does_not_match_composite_chained_message`,
  `test_lookup_monthly_norms_connectionerror_with_404_phrase_classifies_sdk_failed`,
  `test_orchestrator_every_station_connectionerror_with_404_phrase_still_exits_six`).
- **Finding 2 (Important) — the operator could not see *why*, because INFO is invisible.** The
  root logger is capped at WARNING in production (`setup_library`, INFRA-029), so the per-station
  `logger.info` line this section originally claimed made the 404 reason "see[able]" **never
  appears** in a production log — that claim above was false and has been corrected in the
  "Implementation" section. The per-station message is deliberately **not** promoted to WARNING
  (on an all-absent deployment that would be one warning per station on every run — exactly the
  noise this issue removes). **Fixed instead by making the aggregate carry the provenance**: a new
  `norm_absent_via_404` count, printed alongside the existing `norm_absent` / `sdk_failed` /
  `api_failed` counts in the `LONG-HORIZON RUN SUMMARY` block (which uses `print(...)`, so it
  always survives the WARNING cap). It is a **subset** breakdown of `norm_absent` — how many of
  those stations were specifically a graded-404 SDK exception, as opposed to a 200 response with
  an empty/invalid payload — not a rename or a replacement of any existing count. Also corrected:
  `doc/prod/ml_no_forecasts_debug_runbook.md` (Section 8's grep only searched text that never
  appears on an all-404 deployment; now also searches the aggregate signals and states the
  visibility limit plainly) and `apps/preprocessing_runoff/README.md` (added the
  `norm_absent_via_404` count and the INFO-invisibility caveat next to the existing status-code
  grading section).
- **Finding 3 (Minor) — the highest-stakes preservation case had no direct test.** Existing
  preservation tests covered `NORM_ABSENT` with no exception (a 200-with-empty-payload, e.g.
  `test_norm_absent_preserves_existing_month_norms_and_derives_rollups`) and `SDK_FAILED` with an
  exception — but not `NORM_ABSENT` **with** an exception, which is exactly what a 404 now
  produces. **Fixed**: added
  `test_norm_absent_via_404_preserves_existing_month_norms_and_write_payloads`, which writes valid
  norms for a station, re-runs with the SDK 404ing, and asserts all 12 stored monthly norms
  survive, the derived seasonal/quarterly rollups are unchanged, and the actual write payloads
  sent to the client (not just the in-memory records returned to the caller) carry the preserved
  norms. Verified: mutating `write_station_monthly_hydrograph` to blank norms specifically for
  "NORM_ABSENT reached via an exception" (leaving the no-exception NORM_ABSENT and the SDK_FAILED
  read-merge paths untouched) is caught by this test and no other. The reviewer also flagged four
  existing tests as legitimate-but-revert-insensitive widening guards (they pass against the
  pre-Finding-1 "every exception is SDK_FAILED" behaviour too, so they would not by themselves
  catch this feature being reverted): `test_lookup_monthly_norms_non_404_status_code_classifies_
  sdk_failed`, `test_lookup_monthly_norms_no_status_code_in_message_classifies_sdk_failed`,
  `test_lookup_monthly_norms_non_valueerror_exception_still_classifies_sdk_failed`, and
  `test_orchestrator_every_station_500_still_exits_six` — each now carries a comment saying so, so
  nobody later mistakes them for regression coverage.

All 109 tests in `test_sync_long_horizon_hydrograph.py` pass (103 before this cross-check + 6
new), `ruff check` / `ruff format --check` are clean, and the full `apps` suite
(`SAPPHIRE_TEST_ENV=True bash run_tests.sh`) is green with the pre-existing two `@pytest.mark.skip`
placeholders in `test_src.py` the only skips.

**Files touched by this refinement** (same allowed-files boundary as the rest of this issue):
`apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`,
`apps/preprocessing_runoff/test/test_sync_long_horizon_hydrograph.py`, this issue file,
`doc/prod/kghm_pipeline_handover.md`, `apps/preprocessing_runoff/README.md`. `run_locally.sh`,
the short-horizon sync, and every other module are untouched — the exit-4/exit-6 shell routing (C4)
and taxonomy (C1/C1a) this issue already shipped are unaffected; this refinement only changes which
raised exceptions are classified `SDK_FAILED` vs. `NORM_ABSENT` before either of those ever sees the
result.
