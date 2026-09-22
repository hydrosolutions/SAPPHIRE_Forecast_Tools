## CLOSED — `run_locally.sh` discards a failed module's exit code; not implemented (INFRA-024)

**Status**: **CLOSED 2026-09-21 — will not be implemented.** Owner decision, taken on the evidence
of five review rounds (the last four independent and out-of-loop). Kept in `archive/` as the
**behavioural record** of how `run_locally.sh` handles exit codes: that record is accurate, was
expensive to establish, and several sibling issues cite it. **Nothing here is outstanding work.**

**The narrowed promise — what this issue could honestly have delivered.** Not its original goal.
The original framing, that an operator could attribute a failed run from `$?` alone, is
**unreachable**, and this is a property of the code rather than of the plan: `$?` is one integer, a
run under `--continue-on-error` routinely has several failures with different codes, and the codes
are not unique to a module. The most it could have delivered is *`$?` carries the failure kind; the
summary tells you the module.* Even that is weaker than it sounds — the codes do not form a
taxonomy. Exit 2 means "produced no records, continuing maintenance" from the long-horizon sync
(`:977-978`) and REFUSED from the LT recovery (`:1416-1418`): same number, opposite operator
response. Module identity was always going to come from `RESULTS_MODULE` (`:390`) and the printed
summary (`:2161`), as it does today.

**Why the narrowed promise was not worth implementing.** It buys an operator nothing the run summary
does not already print **for any failure that records a row** — the failed module and
`FAIL (REFUSED)` for the recovery case. **Neither the horizon nor, in the multi-horizon case, the
right error detail**, and that caveat is load-bearing for anyone re-reading this rationale.
`run_postprocessing_forecasts` uses one label and one log path for every horizon and **truncates
that log on each invocation** (`:816-827`) while being called from inside the horizon loop
(`:1570-1597`). So if PENTAD fails and DECAD then succeeds, both rows point at the same file and the
MODULE ERROR DETAILS block shows **DECAD's** output for PENTAD's failure. A silently failing child
can also yield "(no output captured)". The same shared-log pattern applies to `linear_regression`
(`:713-724`). Corrected 2026-09-21 — earlier revisions of this note claimed first the horizon, then
the error detail, was covered.

**This strengthens rather than weakens the close.** The reporting gap is in the *rows and logs*, not
in `$?`; an exit code would not have closed it, and fixing the summary is the cheaper, more
informative change — see "The better idea, if this need returns" above. One exception, documented below and not closed by this
issue either: `initialize` Steps 2-4 return failure with **no row at all** (`:1246`, `:1262`,
`:1278`), so their summary shows the earlier rows and nothing for the step that actually failed.
(**Not necessarily *successful* earlier rows** — Step 1's exit-6 branch leaves a long-horizon
`FAIL` row at `:1017` while returning 0, in which case a MODULE ERROR DETAILS block *is* printed,
for that row rather than for the failed step. Qualified 2026-09-22.) That
is a reporting gap in its own right, and an exit code would not have fixed it — what is missing is
the row. Be precise about the exit status, though: the run exits with the child's code (7) **only if
no earlier step recorded a `FAIL` row**. If Step 1 already recorded one — the exit-6 branch at
`:1017` does exactly that while returning 0 — then `print_summary` fires and `:2713` overwrites the
7 with 1. Exit 7 on this path is conditional, not guaranteed. Corrected 2026-09-21. Against that: `print_summary … || exit_code=1` (`:2713`) is currently the *only* thing making a
failing `--continue-on-error` run non-zero for most targets, so the change sits on top of the repo's
own pre-merge gate and a documented outage blind spot (the exit-6 branch at `:1017`). Five distinct
ways to get it subtly wrong were found and are recorded below. Medium priority; poor trade.

**The better idea, if this need returns.** Do not select one integer. Two options, in order of cost:
(1) print each failure's code beside its module in the summary — diagnostic only, no orchestration
change; (2) if a machine consumer ever needs attribution, emit a structured per-run result carrying
**all** failed records, and keep `$?` as the failure signal. Neither is filed; file one when there is
a consumer that would use it.

**What would reopen this.** A demonstrated operator or external caller that takes a genuinely
different action on an exact status and cannot use the summary or a result artifact. A narrowly
scoped recovery caller needing "refused versus execution failed" would justify a *target-specific*
contract — not general propagation.

**What is preserved below, and is still true**: Defect A's resolution record; the measured
behaviour tables and who consumes the exit code; the two outage contracts riding on `:2713`; the
`set -e` / negated-capture / per-runner-precedence hazards; and the owner decisions of 2026-09-21,
which stand as design constraints on any future change in this area. The implementation
prescriptions (test instructions, phase structure) were **deliberately left unfinished** when the
close was decided — do not read them as ready.

**Superseded framing**: the original title claimed the failed run was unattributable from `$?`. That
is true but not fixable by this issue, which is why it is closed rather than implemented.

**Review history** (kept for provenance): Draft 2026-08-17; re-verified against trunk
`4fe3e545` 2026-09-17; corrected 2026-09-18 after two out-of-loop rounds, the second **measuring**
behaviour against a patched copy of the real script; corrected again 2026-09-21 after three further
rounds against trunk `6927dd31`, which refuted the unconditional `yearly` measurement, the "guard
body never executes" claim, the bare-capture inventory, the `_LT_RC_CASES` over-claim and the
taxonomy evidence, and added the negated-capture hazard. Each round found defects in the previous
round's corrections; that pattern is part of why the close was decided.
**Module**: `apps/run_locally.sh` (Defect B, **closed unimplemented 2026-09-21**);
`apps/preprocessing_runoff/sync_long_horizon_hydrograph.py` (Defect A, resolved)
**Priority**: was **Medium** — no data is lost or corrupted. A failed run cannot be attributed from
`$?` alone, which does cost diagnostic time, but the summary already carries what is needed and the
gap is not closable by an exit code (see the closure note above). Note what this is *not*: the repo's own validation
gate consumes only the zero/non-zero **bit**, and that bit is correct today (see "Who consumes
`run_locally.sh`'s exit code"). Finding that consumer establishes a **compatibility constraint on
any fix**, not evidence that the gate is currently degraded. The live cost is the interactive /
human `$?` case — see the LTF-010/011 subsection.
**Labels**: `infra`, `observability`, `exit-codes`, `logging`
**Found**: 2026-08-17, during the PREPQ-014 root-cause investigation. Defect B was surfaced by an
out-of-loop `codex exec` review and re-verified by direct code reading.
**Related**: PREPQ-014 (where both were found), PREPG-009 (partial-failure reporting), PP-051
(silent-success family), INFRA-023, INFRA-037 and INFRA-044 (both restructured the code Defect B
cites), INFRA-045 (repairs the broken `run_locally.sh validate …` commands in the review checklist),
LTF-010/LTF-011 (the recovery-target REFUSED/FAILED distinction Defect B flattens).

---

## Why these are filed together

Two independent defects sharing one operator-facing problem: when a run fails, the log should tell
you which module, roughly what kind, and — machine-readably — which class. Defect A is fixed; Defect
B is not. The historical record of Defect A is kept here because it explains the current shape of
the `SDK_FAILED` handling that several sibling issues still reference; it is not outstanding work
and no reader should treat it as such.

## Defect A — RESOLVED: SDK norm failures were `logger.debug`-only and silently dropped the station

**Status: RESOLVED.** Confirmed fixed on trunk `4fe3e545` (2026-09-17).

The original defect: `write_station_monthly_hydrograph` logged an `SDK_FAILED` norm lookup at
`logger.debug` and then skipped the station outright — no monthly rows were written for it that run.

**Current code** (`sync_long_horizon_hydrograph.py:490-498`):

```python
if norm_classification is _NormClassification.SDK_FAILED:
    exc = norm_lookup.exception
    logger.warning(
        "write_station_monthly_hydrograph: SDK call failed for site %s, continuing "
        "with a read-merge of any previously stored norm. Error: %s: %s",
        code,
        type(exc).__name__,
        exc,
    )
```

The load-bearing change is behavioural, not just the level: **an SDK norm failure no longer skips
the station.** (Not the same as "the rows are always written": the fallback norm read, the daily
reads or the write itself can still fail, in which case the station is classified `API_FAILED`
rather than appearing in `sdk_failed` — `sync_long_horizon_hydrograph.py:452-458`, `:507-510`,
`:734-741`. Qualified 2026-09-21.)
Row existence is now decoupled from norm availability — the 12 monthly rows are still written, with
any previously stored norm preserved via a read-merge (`_read_existing_month_norms`, defined at
`:446`, called at `:507`). The level change (`debug` → `warning`) makes the event visible under
production's WARNING-capped root logger (`apps/iEasyHydroForecast/setup_library.py:44-55` — note the
mechanism: `basicConfig(level=WARNING)` at `:44`, then `logger.handlers = []` and a fresh
`StreamHandler` — constructed at `:48`, attached at `:54` after the clear at `:53`; a consumer module's own later `basicConfig` is a no-op because the root
logger already has handlers. INFRA-029). `SDK_FAILED` also gets its own line in the counts-only run
summary (`sdk_failed=…`, `:845`), matching the sibling `API_FAILED` aggregate at `:752-757`.

**What is not fixed by this, and remains open work** (recorded under "Deliberately out of scope"
below, not reopened as part of Defect A): `_lookup_monthly_norms` (`:375-443`) still catches broad
`Exception` at `:412` (`except Exception as exc:`) and cannot, by exception class alone,
distinguish every failure cause.

## Defect B — `run_locally.sh` discards a failed module's specific exit code

**Status: CLOSED 2026-09-21, not implemented.** This was the entire remaining scope of
INFRA-024. Everything below is the **behavioural record** — an accurate description of what
`run_locally.sh` does today. It is not a work item, and the prescriptive passages (acceptance
criteria, what-to-inspect, the capture-hardening directive) are **withdrawn**: they describe what
an implementation *would* have required, and were left unfinished when the close was decided.

### Who consumes `run_locally.sh`'s exit code

**Corrected 2026-09-18. An earlier revision of this file concluded that no caller consumes the exit
code, on the strength of a sweep of `bin/` alone. That sweep never covered `apps/`, and the
conclusion was wrong.** There is an executed, in-repo consumer, and it is the repository's own
pre-commit / pre-merge validation gate.

- **`apps/run_validation.sh`** — Stage 1b. `:187` runs
  `bash apps/run_locally.sh --continue-on-error all` and `:213` runs the `maintenance` equivalent.
  Each captures `rc=${PIPESTATUS[0]:-$?}` through a `| tee`, branches on `if [ $rc -eq 0 ]` to
  record a PASS/FAIL stage row, and `return $rc`. Dispatched from `:489` / `:499`, under
  `if [ "$MODE" = "full" ]` (`:485`, set at `:425`; the default `MODE="quick"` at `:67` runs Stage 1
  only) and `if [ "$SKIP_PIPELINE" = false ]` (`:488`). Documented as
  Stage 1b of the validation chain in `doc/dev/testing_workflow.md:272` (the stage diagram), `:381`
  ("## Stage 1b: Local Pipeline Run (Optional)") and `:846`, and in `run_validation.sh:36`.
  `doc/plans/issues/high_prio_gi_draft_infra_no_production_run_verification.md:45` and `:71` already
  list these exact call sites.
- **The test suite** — `apps/pipeline/tests/test_run_locally_orchestration.py` drives `run_main()`
  end to end and asserts process status in 84 places — 79 on `result.returncode`, five on
  `first.returncode` (`:381`, `:453`, `:516`, `:574`, `:622`).

**What `run_validation.sh` actually consumes is the zero/non-zero bit, not the number.** It branches
`if [ $rc -eq 0 ]` (`:192`, `:218`), records a PASS/FAIL stage row, and its `main()` ends
`if [ "$any_failed" = true ]; then exit 1; fi` / `exit 0` (`:530-533`) — every failure is flattened
to 1. The Stage 1b functions themselves `return $rc` (`:199`, `:225`), so the number survives that
far; it is *rendered* only in a log string (`:196`, `:222`) and `main()` discards it. So today's
behaviour is
**correct for this consumer**; the consumer matters because it constrains the fix, not because it is
currently mis-served.

> **HAZARD — read before touching `print_summary`. There are TWO outage contracts riding on
> `:2713`, not one.**
>
> **(1) The Stage 1b gate.** `run_validation.sh` passes `--continue-on-error`. Under that flag the
> pipeline aggregators do not return a child's code (see "The code is already gone before `:2713`"
> below), so `print_summary … || exit_code=1` is — for `all` and `maintenance`, the two targets
> Stage 1b runs — the **only** thing that makes a failing run non-zero. Removing or bypassing the
> clobber turns Stage 1b silently green.
>
> **(2) The PREPQ-014/015 outage blind spot.** INFRA-044's exit 6 (every attempted station's SDK
> norm lookup failed) records a `FAIL` row at `:1017` while the module's `rc` **stays 0** — the
> comment at `:1018-1019` says so outright. So for exit 6 the recorded row → `print_summary` →
> `:2713` is the *sole* non-zero path, and two of the affected targets are ones Stage 1b never
> invokes. The named regression guard is
> `test_all_stations_sdk_failure_exits_nonzero_end_to_end` (`test_run_locally_orchestration.py:1649`),
> whose docstring calls itself *"The most important test in this issue … the regression guard for the
> PREPQ-015 property"*. **A fix can satisfy contract (1) and still reopen this one.**
>
> **Measured, not argued.** An out-of-loop reviewer ran the real script under `--continue-on-error`
> against an unmodified copy and a copy with the `:2713` clobber removed, via the repo's own
> `SynthTree` / `run_main` harness. With one module forced to exit 7:
>
> **"Clobber removed" means one specific edit, and the column is meaningless without it**
> (added 2026-09-22 after a reviewer showed the two obvious edits diverge). It means replacing
> `print_summary "$pipeline_elapsed" || exit_code=1` (`:2713`) with
> `print_summary "$pipeline_elapsed" || :` — the summary still prints, but its status neither
> reaches `exit_code` nor trips errexit. **Simply deleting the `|| exit_code=1` is a different
> counterfactual**: that leaves a bare `print_summary` whose non-zero return under `set -euo
> pipefail` (`:119`) kills the script before `exit $exit_code` (`:2716`), yielding 1 again by another
> route. Read every row below as "under the `|| :` variant".
>
> | target | as shipped | `:2713` clobber removed |
> |---|---|---|
> | `all` | exit 1 | **exit 0** |
> | `maintenance` | exit 1 | **exit 0** |
> | `daily`, `short-term`, `long-term`, `long-term-operational` | exit 1 | **exit 0** |
> | `yearly` | exit 1 | exit 1 — **only if the failing runner is the last one; see note** |
> | `long_term_forecasting` | exit 1 | exit 1 (multi-month branch; single-mode reaches `exit_code` and is flattened only by `:2713`) |
> | `initialize` (Step 2/3/4 failure) | exit 7 — **but 1 if an earlier step already recorded a `FAIL` row**; Step 1's exit-6 branch (`:1017`) records one while returning 0, which re-arms `:2713` | exit 7 in both cases — under the `|| :` variant the summary's status no longer reaches `exit_code` |
>
> **Note — the `yearly` row has a precondition the original measurement did not state.**
> `run_yearly_pipeline`'s return status is its *last* statement's, and that statement is
> `run_recalculate_skill_metrics || { … }` (`:1865`). The measured exit 1 holds when that final
> runner is the one that fails. If snow norms fail at `:1863` and skill metrics then succeed, the
> function returns **0** and `yearly` exits **0** with the clobber removed — the same silent-green
> failure as `all` and `maintenance`. So `yearly` is not a safe-by-accident target; it is
> silent-green for every failure *except* one. (Out-of-loop review, 2026-09-21.)
>
> With the long-horizon sync forced to exit **6** (contract 2):
>
> | target | as shipped | `:2713` clobber removed |
> |---|---|---|
> | `maintenance:preprocessing_runoff` | exit 1 | **exit 0** |
> | `initialize` | exit 1 | **exit 0** |
> | `maintenance` | exit 1 | **exit 0** |
> | `daily` | exit 1 | **exit 0** |
>
> **`daily` was missing from this table until 2026-09-21.** It runs
> `run_maintenance_preprocessing_runoff` in Phase 2 (`:1772`) and ends on
> `run_api_validation "daily"` (`:1854`), which returns a hardcoded 0 (`:1483`) — so with exit 6
> recording a `FAIL` row while the module returns 0, the clobber is the only thing making it
> non-zero. It has its own regression test,
> `test_run_locally_orchestration.py:1560-1563` (the `daily` exit-6 case), which the record did
> not previously name.
>
> Re-verify against these tables, not against the prose. Note `initialize` appears in both and
> behaves differently in each: **as shipped**, its exit-7 row propagates when no FAIL row exists,
> while its exit-6 row does not because Step 1 records one. Under the `|| :` counterfactual the two
> still differ, and saying "both propagate" would be wrong (corrected 2026-09-22): the Step-2/3/4
> exit 7 propagates, but the exit-6 case becomes **exit 0** — the child's 6 never reaches
> `exit_code` at all, because `run_maintenance_preprocessing_runoff` leaves its own `rc` at 0
> (`:1017-1020`) and only the recorded `FAIL` row made the run non-zero.
>
> **The load-bearing fact, and the answer to the obvious rebuttal.** One might object that an
> API-validation failure is a second non-zero path. It is not: `run_api_validation` ends
>
> ```bash
> return 0  # don't abort pipeline mid-run; failures surface in summary
> ```
>
> — `run_locally.sh:1483`, a **hardcoded** 0, not a variable status. That is exactly why the
> aggregators ending in it can never return non-zero under the flag, and why an api_validation
> `FAIL` row reaches `$?` only via `val_fail` → `print_summary` (`:2212-2213`) → `:2713`.
>
> `test_continue_on_error_suppresses_hint_but_still_exits_nonzero`
> (`test_run_locally_orchestration.py:250`, in `TestContinueOnErrorHint` at `:224`) pins the
> `daily` case and must not be weakened.

**Paths searched for this issue:** `apps/` (the finding above) and `bin/`. What the `bin/` audit
found stands as far as it goes: every reference to `run_locally.sh` anywhere under `bin/` — ten in
total (`bin/bimonthly_long_term_skill_metrics_recalculation.sh:100`,
`bin/purge_site_data.sh:632-633`, `bin/locally_run_forecast_tools.sh:4,12,14,17-18`,
`bin/README.md:62`, `bin/yearly_runoff_hydrograph_aggregation.sh:37`) — is a comment or an `echo`ed
suggestion, never an executed call. (The deprecated `bin/locally_run_forecast_tools.sh` prints
`"Use apps/run_locally.sh instead."` at `:17-18` but does **not** exit — the file contains no `exit`
at all — and falls through to `run_lt_forecasting` at `:259`, which sources a hardcoded personal
venv and runs `run_forecast.py --all` in a month loop. It still never invokes `run_locally.sh`, so
the scoping conclusion is unaffected.) Production schedules go through cron → Luigi
(`doc/deployment.md:912`, "## Set up cron job"). None of that makes Defect B documentation-only,
because `run_validation.sh` exists.

**Not searched for this issue: installed server crontabs.** A separate crontab survey dated
2026-09-07 *reported* that every live SAPPHIRE cron line is `cd <dir> && bash <script>` with nothing
downstream consuming the wrapper's status. That is a prior finding relied on, **not a direct
inspection performed here** — no installed crontab was read while scoping INFRA-024. INFRA-023
showed scheduling docs can drift from installed reality, so treat the question as open, not closed.

**The baseline to start from:** `run_in_venv` already gets this right at the innermost layer. It
runs the module in a subshell piped to `tee` (`:629-632`) and returns `"${PIPESTATUS[0]}"` (`:634`),
so the child's numeric exit code **does** reach the shell correctly. Every loss described below is
strictly above that layer. (`bin/yearly_runoff_hydrograph_aggregation.sh:246` solves the same
pipeline problem a different way, via `docker inspect … {{.State.ExitCode}}` with
`EXIT_CODE=${PIPESTATUS[0]}` at `:242` as its fallback; it is not a reference for this issue, whose
question is what a *multi-module summary* should exit with.)

### The exit-code-discarding chain

Fully re-verified on trunk `4fe3e545`:

1. `print_summary()` (`apps/run_locally.sh:2128`) counts every non-PASS/non-SKIP row into
   `fail_count`, and once module and validation rows are tallied, `return`s 1 whenever
   `fail_count > 0 || val_fail > 0` — condition at `:2212`, `return 1` at `:2213`.
2. In `main()`, 24 dispatch sites each capture their target's return status into a shared local
   `exit_code` variable: `<runner> || exit_code=$?` (two of the 24 use the equivalent
   `|| { exit_code=$?; }` — `:2584`, `:2666`) — `run_locally.sh:2528` through `:2695`.
3. `run_locally.sh:2712` guards the summary: `if [ ${#RESULTS_MODULE[@]} -gt 0 ]; then`.
4. `run_locally.sh:2713` — `print_summary "$pipeline_elapsed" || exit_code=1` — overwrites whatever
   status step 2 captured with a flat 1, as soon as any row anywhere counted as a failure.
5. `run_locally.sh:2716` — `exit $exit_code`.

So any module (or sub-step) whose failure is *recorded as a result row* causes the whole process's
exit code to become 1, regardless of what that step actually returned.

### The code is already gone before `:2713` for the most-used targets

**A fix to `print_summary` / `:2713` alone cannot deliver specific-code propagation.** For seven
**aggregator** targets — `daily`, `short-term`, `long-term`, `long-term-operational`, `all`,
`maintenance`, `yearly`, i.e. the most-used ones — the child's numeric code is discarded *inside
the aggregator function*, before `exit_code` is ever assigned. The eighth, the bare
`long_term_forecasting` target (dispatch `:2695`), loses it two different ways depending on branch:
inside the function in the multi-month branch (`rc=1`, `:868`), but at `:2713` in the single-mode
branch, where the code does reach `exit_code` intact and is then flattened because `:878` records a
`FAIL` row unconditionally. (**`initialize` is usually not one of them**: on a Step 2/3/4 failure it
propagates `$rc` and measurably exits 7 — but not on a Step 1 failure, see "Carve-outs" below, and
**not when any earlier step has already recorded a `FAIL` row**, including Step 1's exit-6 branch
(`:1017`), which records one while returning 0 and so re-arms `:2713` to flatten the 7 to 1.)

- **Most** module calls in a pipeline aggregator use the guard idiom
  `<runner> || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }` —
  `run_short_term_pipeline` (`:1561`, `:1566`, `:1596`, `:1597`), `run_long_term_pipeline`
  (`:1616`, `:1619`, `:1622`, `:1625`), `run_long_term_operational_pipeline` (`:1646`-`:1671`),
  `run_maintenance_pipeline` (`:1713`-`:1748`), `run_daily_pipeline` (`:1763`-`:1847`),
  `run_yearly_pipeline` (`:1863`, `:1865`). With `CONTINUE_ON_ERROR=false` the child's code is
  flattened to a hardcoded `return 1`. With `--continue-on-error` — **the mode the only real caller
  uses** — the *inner* abort block never executes, but the **outer brace body does**: the
  `[ "$CONTINUE_ON_ERROR" = false ]` test runs and fails, which gives the brace group the value 1.
  The function's return status is therefore that of its *last* statement, which may or may not be
  that brace group.
- **Four ML call sites use a different shape and are not in that list**: `run_locally.sh:1577`,
  `:1728`, `:1787`, `:1812` are `<runner> || { … [ "$CONTINUE_ON_ERROR" = false ] && continue; }` —
  no `PIPELINE_ABORTED`, no `return 1`, a `continue` instead. This is deliberate (**ML-021 decision
  4**, documented in the comment at `:1578-1592`: an ML failure in one horizon must not stop the
  next). They are four additional loss points, and unifying them with the guard idiom would silently
  revert that decision — see "Contract not to break".
- **What the last statement actually is**, per aggregator: `run_api_validation`
  (`run_short_term_pipeline:1603`, `run_long_term_pipeline:1627`,
  `run_long_term_operational_pipeline:1673`, `run_daily_pipeline:1854`), which returns a hardcoded
  0 (`:1483`); a bare `export` (`run_maintenance_pipeline:1753`), also 0. **`run_yearly_pipeline` is
  the exception**: its last statement is the guard idiom itself (`:1865`), whose value under
  `CONTINUE_ON_ERROR=true` is the status of the failed `[ "true" = false ]` test — **1**. That is
  why `yearly` measurably still exits 1 with the `:2713` clobber removed — an accident of statement
  placement, not a design. **That accident covers only one of its two runners.** If snow norms fail
  at `:1863` and `run_recalculate_skill_metrics` (`:1865`) then succeeds, the last statement
  succeeds and the function returns 0. That mechanism is distinct, but no longer unique: single-mode
  `long_term_forecasting` also returns non-zero independently, via `return $rc` (`:880`) into the
  dispatch capture at `:2695`.
- `run_all` (`:1676`) captures `st_rc` / `lt_rc` from the two sub-pipelines and then collapses both
  to a hardcoded `return 1` at `:1691`.
- `run_long_term_forecasting` (`:867-869`) and `run_long_term_forecasting_operational` (`:921-923`)
  both set `rc=1` over each child's real code when `any_failed` is true. `run_long_term_forecasting`
  is also a **bare dispatch target** (`:2695`), so this is its own loss point, not just an
  aggregator's: with `lt_forecast_mode` unset it takes the multi-month loop branch and loses the code
  at `:868`; with it set, the single-mode branch does hold the code (`|| rc=$?`, `:853`) but the
  `FAIL` row at `:878` then hands it to `:2713` anyway. Both branches lose it, at different layers.
- `record_result` (`:384-395`) stores module, status, elapsed, error log and reason — **no numeric
  exit code**. Nothing *numeric* is stored. Note the qualifier: the recorded **statuses** are enough
  to decide *that* the run failed, which is what `print_summary` already computes (`:2212-2215`), so
  a dedicated-aggregate policy needs no new storage. Only a policy that reports a **child's code**
  needs a numeric field.

**Consequence — conditional on the policy chosen.** `bash apps/run_locally.sh all` would still exit 1
after a `print_summary`-only change, for failures that make a sub-pipeline return non-zero. (Not all
do: an ML-only failure can leave `run_all` returning 0 even without `--continue-on-error`, because
the ML sites `continue` rather than abort; the `FAIL` row and the `:2713` clobber then supply the
process failure.) **If** the owner picks a policy that reports a child's code, the fix must specify
how that code survives the aggregator *and* reaches the aggregate decision, which most likely means
extending `record_result`. **If** the owner picks a dedicated aggregate, none of that applies — see
"Acceptance criteria". (Out-of-loop review, 2026-09-21.)

### Carve-outs — where `exit_code` does survive

The clobber is not universal. Both conditions must hold for a code to survive: the target must
**still hold a specific code when it returns**, *and* nothing must have recorded a failing row. The
first condition is the one the section above is about; the second is what the `:1230` caveat below
turns on. Neither alone is the discriminator.

Two live instances:

- **`run_initialize_deployment`** (`:1201`) returns `$rc` non-zero at `:1230`, `:1246`, `:1262` and
  `:1278` without recording a row of its own (its only `record_result` is the `PASS` at `:1285`).
  **Measured: as shipped, on a Step 2/3/4 failure `initialize` exits 7 when that module exits 7 —
  provided no earlier step has recorded a `FAIL` row.** The proviso is not optional; both caveats
  below are ways it fails to hold. Under the `|| :` counterfactual defined above the proviso
  disappears, because the summary's status no longer reaches `exit_code` — **so do not read the
  shipped and counterfactual cases as sharing one rule.** On a Step 1 failure it exits 1 — see the first caveat below.
  On a Step 2/3/4 failure (`:1246`/`:1262`/`:1278`) the rows present are those Step 1's
  `run_maintenance_preprocessing_runoff` left behind; when those are all PASS, `fail_count` is 0,
  `print_summary` returns 0, and `$rc` reaches `exit` intact. *Two caveats:* `:1230` is **not** a
  carve-out — Step 1's failure path records a `FAIL` row at `:1037` before returning, so that code
  does get clobbered; and Step 1 can also leave a long-horizon `FAIL` row while itself returning 0
  (the exit-6 branch, `:1017`), which re-arms the clobber for the later steps too.
- **No rows at all.** `run_initialize_deployment:1218` (`return 1` when no non-empty start date is
  obtained from *either* `ieasyhydroforecast_START_DATE` in the shell *or* the configured env
  file — the two-source check is at `:1208-1216`) exits before anything records a row, so the `:2712`
  guard is false, `print_summary` is never called, and `exit_code` is untouched. Same for any target
  that fails before its first `record_result`.

A third case is worth knowing but is not a carve-out in the exit-code sense: the exit-4 PARTIAL
branch of `run_maintenance_preprocessing_runoff` (`:988-998`) records no sub-step row **and** leaves
`rc` at 0, so the function records `"preprocessing_runoff (maintenance)" "PASS"` at `:1034` and
returns 0 at `:1039`. There is no non-zero code to lose. This is pinned by
`test_run_locally_orchestration.py:1671` (`test_partial_sdk_failure_exits_zero_end_to_end`, asserting
`returncode == 0`).

Establish the true affected set before designing a fix.

### Current branch shape (the INFRA-044 split)

The inner maintenance function receives a sub-step's specific exit code and reacts differently
depending on which one it is. Two representative branches inside
`run_maintenance_preprocessing_runoff`, current as of trunk `4fe3e545`:

```bash
elif [ $lt_rc -eq 4 ]; then
    # PARTIAL SDK norm-lookup failure (INFRA-044): ... no result row is
    # recorded at all, and the module is not failed (rc stays 0).
    log INFO "Long-horizon hydrograph sync: one or more stations' monthly norm lookup did not return a norm. ..."
    CURRENT_MODULE_LOG="${ERROR_DIR}/preprocessing_runoff_maintenance.log"
elif [ $lt_rc -eq 6 ]; then
    # TOTAL SDK norm-lookup failure (INFRA-044): ...
    log ERROR "Long-horizon hydrograph sync had SDK norm lookup failure(s)"
    log ERROR "  Counts are in the LONG-HORIZON RUN SUMMARY block ..."
    record_result "preprocessing_runoff (long-horizon sync)" "FAIL" "$lt_elapsed" "$CURRENT_MODULE_LOG"
    # Downgraded, not fatal to the overall module (rc stays 0)
    CURRENT_MODULE_LOG="${ERROR_DIR}/preprocessing_runoff_maintenance.log"
```

— exit-4 branch at `run_locally.sh:988-998` (no result row, `rc` untouched); exit-6 branch at
`:999-1020` (`record_result … "FAIL"` at `:1017`, "not fatal to the overall module" comment at
`:1018-1019`). Exit 6 records the FAIL row and so flips the whole script's exit code via the chain
above; exit 4 does not. **This snippet is current ground truth as of 2026-09-18; it has already
changed shape twice since this issue was drafted (INFRA-037, then INFRA-044) — re-grep `lt_rc` in
`run_locally.sh` before relying on it again.**

### LTF-010/LTF-011: the recovery target's exit-2 REFUSED signal is normalised to 1

The clearest illustration of Defect B, and the one with an already-documented operator meaning.
`run_maintenance_long_term_forecasting()` (`apps/run_locally.sh:1393`) exists specifically because
LTF-010/LTF-011 (merged as PRs #495/#493, documented in PR #511) went to real effort to keep a
REFUSED recovery distinguishable from a FAILED one. Chain, verified:

- On exit 2, the function logs `log ERROR … "REFUSED"` (`:1417`) and records
  `record_result "long_term_forecasting (recovery)" "FAIL (REFUSED)" …` (`:1418`) instead of the
  plain `"FAIL"` used for exit 1 (`:1422`) and any other non-zero code (`:1426`), then `return $rc`
  (`:1429`) — propagating 2 out of the function.
- Dispatch site `run_locally.sh:2606` (`run_maintenance_long_term_forecasting || exit_code=$?`)
  captures that 2 into `exit_code`.
- `"FAIL (REFUSED)"` is still a non-PASS/non-SKIP row, so `print_summary` counts it toward
  `fail_count` and returns 1 (`:2212-2213`).
- `:2713` then overwrites `exit_code` back to 1.

**`bash apps/run_locally.sh maintenance:long_term_forecasting` exits 1 on a REFUSED recovery, not
2.**

**This is not a broken promise — do not write it up as one.** LTF-010/LTF-011 promise that a REFUSED
recovery stays **non-zero**, never that the process exits `2` specifically, and the existing test
`apps/pipeline/tests/test_run_locally_orchestration.py:2419`
(`test_refused_exit_2_is_labelled_distinctly_and_stays_nonzero`) asserts `returncode != 0` at
`:2427`, not `== 2`. Distinguishability is delivered by the `FAIL (REFUSED)` summary row and the log
text, and both still work: `print_summary` prints the recorded status verbatim rather than a
hardcoded "FAIL" (`:2161`, with the comment explaining why at `:2153-2160`), and the row still gets
a MODULE ERROR DETAILS tail (pinned at `:2442`). Propagating 2 to the process would be a **requested
behaviour change**, not a repair. Anyone scripting against `$?` to tell "refused, read the reason"
from "broken, page someone" gets 1 either way — that is the cost, and it is the thing to decide
about.

**What exit 2 actually means.** Not only the existing-row guard. The long-term recovery returns 2
for an **operator-request refusal that requires reading the reason** — malformed dates, dates
outside the recovery window, unscheduled issue dates, and the existing-row guard declining to
overwrite are all in this class. Do not narrow it to "declined, try again later". See
`doc/plans/issues/review_gi_draft_ltf_recovery_no_run_locally_target.md` and
`doc/plans/issues/review_gi_draft_ltf_recovery_refused_conflates_two_causes.md`, which settle this
at length.

## Owner decision — RESOLVED 2026-09-21: close, do not implement

**Kept as the record of what was decided and why. The sub-questions below marked OPEN were never
answered and no longer need to be — they are what implementation *would* have required.**

Should `run_locally.sh` propagate the first non-zero module code, the highest, or a dedicated
aggregate code? The acceptance criteria below deliberately do **not** presuppose that "the specific
exit code" exists: under `--continue-on-error` — the mode the only real caller uses — several
modules routinely fail with *different* codes in one run, and there is no single code to propagate.

**A finding from the 2026-09-21 review bears on this question before it is answered.** Exit code 5
is *deliberately shared* by two modules — `machine_learning` and the long-horizon sync — and the
comment saying so is in the code (`run_locally.sh:735-737`: "mirroring
`sync_long_horizon_hydrograph.py`'s existing use of 5"). So no propagation policy can deliver the
outcome in this issue's Priority line, *attributing the failed module from `$?` alone*. A numeric
code can carry the failure **kind**; module identity lives in `RESULTS_MODULE` (`:390`) and is
rendered in the summary (`:2161`), and that will remain true after any fix here. The owner should
therefore chose between **(a) keeping the issue with a narrower promise** — **or (b) closing it**,
since the summary already names the failing module. **Decided 2026-09-21: (b), with (a)'s narrowed
promise recorded in the closure note at the top so that no future reader mistakes the original
framing for something achievable.**

Note that (a)'s promise is itself policy-dependent, so do not state it before the policy is picked:
a **dedicated aggregate** conveys only *that* the run failed, nothing about kind; only a policy that
reports a selected child's code conveys a failure **kind**, and even then only to the extent the
codes form a taxonomy — which they do not. **Exit 5's two uses are close but not identical, so use
exit 2 as the clearer evidence.** ML's exit 5 is "forecast computed and its CSV written, database
save failed" (`run_locally.sh:734-739`). The long-horizon sync's exit 5 is broader — **">=1 API
read/write failure"**, reads included
(`sync_long_horizon_hydrograph.py:912-916`, and the invariant note at `:778-784`) — so it can fire
before any records are built or written, with no guaranteed local backup. Corrected 2026-09-21: an
earlier revision claimed both meant the same thing. Either way 5 proves that a code cannot identify
the *module*. **Exit 2 genuinely carries two different
meanings**: from the long-horizon sync it is "produced no records; continuing maintenance", non-fatal
(`:977-978`), while from the LT recovery it is REFUSED, recorded as `FAIL (REFUSED)` (`:1416-1418`).
**And a third, worse one**: `sync_long_horizon_hydrograph.py`'s argparse also exits 2 on a bad
argument (`_build_parser`, `:865-876`; `--target-year` is `type=int`), and `run_locally.sh` passes
`RUNOFF_LONG_HORIZON_TARGET_YEAR` straight into it (`:959-965`). So a **typo'd year in the env
config takes the non-fatal "produced no records" branch** and the run continues. Be precise about
the failure shape: argparse's own error text *is* printed and logged (`run_in_venv` tees it,
`:622-634`), so this is not an absent diagnostic — it is a **configuration failure misclassified as
non-fatal**, with a misleading "produced no records" WARN over the top of it and **no `FAIL` row**.
Note also that only a *non-integer* value is rejected this way. An integer-valued typo (`2025` for
`2026`) **passes argument validation and is a perfectly valid target year** — the run can then
process the unintended year and exit 0, which is a quieter failure than the one described here, not
a louder one. Both are live defects in their own right, independent of everything INFRA-024 was
about. **The malformed-value case is filed as PREPQ-021**
(`doc/plans/issues/mid_prio_gi_draft_prepq_target_year_typo_reported_as_no_records.md`). The
wrong-but-valid-year case is **not filed** and has no owner; it needs a plausibility check on the
year rather than argument validation.
Same number, opposite operator response. Whatever is promised, module
identity comes from the summary.

Sub-questions. **The gate covers behaviour changes and populating any new field**; the two
pieces of groundwork named under "What can proceed before the decision" (the additive `record_result`
arity change, and extending the Stage-1b guard) were the explicit exceptions and could have
landed first. **Neither did, and the close withdrew the authorisation** — see the section below.

- **RESOLVED 2026-09-21 — module failure wins.** Precedence between a module failure code and a
  validation failure (`val_fail` in `print_summary`): the module code takes precedence, on the
  grounds that a crashed module is usually the cause and a failed validation the symptom.
- **RESOLVED 2026-09-21 — the recorded row wins over the runner's return code.** A `FAIL` row whose
  runner nevertheless returned 0 (exit 6's branch at `:1017` is exactly this) must still make the run
  non-zero. **This is a safety constraint on the policy, not a preference**: a rule phrased as
  "propagate the module's returned code" exits 0 here and reopens the PREPQ-014/015 blind spot named
  in the HAZARD box. So a recorded `FAIL` row must be sufficient on its own to make the run non-zero,
  **whatever the runner returned**. It is *not* sufficient to say "derive everything from the rows":
  `initialize` Steps 2-4 fail with `return $rc` and **no row at all** (`:1246`, `:1262`, `:1278`),
  and this issue explicitly requires those failures to stay non-zero. The rule must therefore be a
  union — a failing row **or** a non-zero runner code — not either source alone.
  (Out-of-loop review, 2026-09-21.)
- **RESOLVED 2026-09-21 — store the code that justifies the row's status.** For the exit-6 row that
  means `6`, not the module's `rc` of 0. Follows from the sub-question above. (Adding the parameter
  is groundwork, not a decision — *populating* it is what this question gated.)
- **RESOLVED 2026-09-21 — `run_validation.sh` is OUT OF SCOPE.** It flattens every failure to
  `exit 1` (`:530-533`) and only ever consumes the zero/non-zero bit; widening the change to that
  surface adds risk to the Stage 1b safety property for no gain. A fix stops at `run_locally.sh`.
- **OPEN — what unit does aggregation operate on?** "First", "highest" and "aggregate" do not by
  themselves say whether the aggregation ranges over child executions, runner return codes, or
  recorded rows, and runners already apply *conflicting* internal precedence: `run_preprocessing_gateway`
  is first-wins (meteo beats snow, `:691-694`), while `run_machine_learning` lets a later fatal code
  replace an earlier exit 5 (`:788-797`), and the long-term runners fold several modes into one row.
  For failures of 5 then 3, "first" is implementable as either. **State the aggregation unit, the
  ordering, and whether these existing per-runner policies stay authoritative.**
  The **two bare ML dispatch loops are a third such point**: `run_maintenance_machine_learning ||
  { exit_code=$?; }` (`:2584`) and `run_machine_learning || { exit_code=$?; }` (`:2666`) each sit
  inside a `for mode` loop, so under `BOTH` a PENTAD failure's code is **overwritten** by DECAD's —
  last-failure-wins, decided before `print_summary` ever runs. Preserving that dispatch value cannot
  implement a first-failure or highest-code policy; both loops need explicit handling, and the tests
  must exercise two horizons failing with *different* codes.
  (Out-of-loop review, 2026-09-21.)
- **OPEN — what numeric code does a validation failure carry, and how does it survive?** Extending
  `record_result` cannot reach it: validation uses separate arrays (`:159-163`) and `record_validation`
  stores no numeric at all (`:408-416`), while the validators return 0 by design so as not to abort
  the run. An implementer must be told whether to add a sentinel, a parallel numeric array, or
  something else — otherwise they will invent one. Note this is only partly softened by the two
  RESOLVED items above: module-versus-validation precedence is settled, but a **validation-only**
  failure still needs a defined code. (Out-of-loop review, 2026-09-21.)

### What could have proceeded before the decision — now moot

**Neither item below is authorised any more; the issue is closed.** They are kept because each
records a measured property of the code that a future change in this area would need. The
decision blocked the *policy*; these two did not:

- **The `record_result` arity change alone** (`:384-395`): add a sixth parameter and a sixth parallel
  array. Measured as behaviour-neutral (byte-identical output across five targets) — **and the reason
  it is neutral is that no call site passes a sixth argument yet.** It *was* safe to land, and would
  have reduced the later patch to one line per site, but it bought nothing on its own — which is why
  it was never done under this issue.
  **The script runs under `set -euo pipefail` (`:119`), so the parameter must be declared
  `local code="${6:-}"`, never `local code="$6"`** — a bare `$6` aborts every existing call under
  `nounset`. The function already uses this pattern for its two optional arguments (`${4:-}`,
  `${5:-}`), so follow it, initialise the parallel array to the same empty default, and confirm the
  existing four- and five-argument call sites still work. Do not let the empty default be read
  downstream as "success". (Out-of-loop review, 2026-09-21.)
  **The field was never added**; the exit-6 caller still passes four arguments (`:1017`), so nothing
  stores a numeric code today. Nothing is planned to — but that is a statement about *this closed
  issue*, not a prohibition on any future change, and the requirement below is what such a change
  would have to satisfy. What the 2026-09-21 answers changed is
  that the case which previously had *no defined answer* now has one: when the field is added and
  populated, the `:1017` row **must store 6**, the code that justifies its `FAIL` status, not the
  module's `rc` of 0. That is a requirement on the future change, not a description of current
  behaviour. Still a large change: 43 call sites
  (`grep -cE '^[[:space:]]*record_result "'`).
- **Extending the Stage-1b safety guard.** The guard *shape* already exists:
  `test_continue_on_error_suppresses_hint_but_still_exits_nonzero`
  (`apps/pipeline/tests/test_run_locally_orchestration.py:250`) already asserts that a failing
  `--continue-on-error` run exits non-zero, for `daily`. Extending it to the two Stage 1b targets
  (`all`, `maintenance`) and to outage contract (2)'s exit-6 targets **was the one piece of this
  issue worth doing on its own merits**, and it is the only part that survives the close as a
  suggestion — it guards `:2713` regardless of what INFRA-024 was going to do. It is **not filed**;
  file it if you want it. The same `SynthTree` / `run_main` harness produced the measured tables
  above.

## Never made executable — and now never will be

Per CLAUDE.md § Orchestration Protocol, an implementable plan carries phases with explicit file
lists, dependencies, agent assignments and a dependency graph. **This file never had them**, which
was correct while the decisions were open and is moot now that the issue is closed. Nothing here is
scoped to hand to an agent; if this area is ever reopened, the phase structure is written then,
against whatever the new goal turns out to be.

## What to inspect — WITHDRAWN (never implemented)

*Kept because each item records a measured property of the code, not because anything is to be
inspected now.*


1. Where a numeric code must survive, given "The code is already gone before `:2713`" above: the
   aggregator guard idiom, the four ML `continue` sites (`:1577`, `:1728`, `:1787`, `:1812`),
   `run_all:1691`, `run_long_term_forecasting:867-869` / `_operational:921-923`, and
   `record_result:384-395`. Do not just delete the `|| exit_code=1` (`:2713`) — see the HAZARD box.

   **Capture hazard at the two long-term sites — `$?` there is already 0.** `:860-865` and
   `:914-919` call the child inside `if ! run_in_venv …; then`. Inside that branch `$?` is the
   status of the *negation* — 0 — not the child's code, so the obvious edit (replacing the
   downstream `rc=1` at `:868` / `:922` with `rc=$?`) silently captures success and the failure
   disappears. Capture through a non-negated `if`/`else` or a `|| rc=$?` guard instead, and test
   it with **multiple** long-term modes — a single-mode test passes either way.
   (Out-of-loop review, 2026-09-21.)
2. The two logging follow-ups (an `SDK_FAILED` reason/stage taxonomy; the sibling DEBUG-only audit
   across `preprocessing_runoff` and the PP-051 recalc family) are **not** part of Defect B. They are
   recorded under "Deliberately out of scope" below with the citations a future issue would need.

## Acceptance criteria — WITHDRAWN (never implemented)

*Kept verbatim as the record of what a fix would have had to prove. Not a checklist for anyone.*


- The owner decision above is recorded before any behaviour change is implemented (the two named
  groundwork exceptions aside).
- Whatever aggregation rule the owner picks, `run_validation.sh`'s Stage 1b still exits non-zero on
  a failing `--continue-on-error` run. This is the safety property; prove it with a test that fails
  if the clobber is removed without a replacement. **Note what such a test actually covers.** The
  `run_main` harness sources `run_locally.sh` and calls its `main()` directly
  (`apps/pipeline/tests/conftest.py:449-457`), so it proves `run_locally.sh`'s exit status — not that
  `run_validation.sh` records and returns the failure at its own tee/status boundary
  (`:187-199`, `:213-225`, handled at `:489-505`). Since `run_validation.sh` is out of scope
  (see the decision above), state that its compatibility is established **by inspection** because the
  wrapper is unchanged, and that only the zero/non-zero bit it consumes is contractual.
  (Out-of-loop review, 2026-09-21.)
- **Conditional on the chosen policy.** If the owner picks a rule that propagates a *child's* code,
  then that code must survive the aggregator layer — the fix reaches the loss points in "What to
  inspect" item 1, not only `print_summary`. If the owner picks a **dedicated aggregate** code, this
  criterion does not apply: an aggregate can be derived from the recorded rows **plus a non-zero
  runner status where no row exists**, which is close to what `print_summary` already does
  (`:2212-2215`) — rows alone are not sufficient, because `initialize` Steps 2-4 fail without one.
  Forcing child codes through the aggregator layer would then be unnecessary control-flow churn. State which criterion is live once the policy is chosen.
  (An earlier revision stated the propagation requirement unconditionally, contradicting the
  aggregate option the decision above permits. Out-of-loop review, 2026-09-21.)
- A `maintenance` or `all` run that fails exits non-zero **by rule, not by accident** — i.e. the
  non-zero status is produced by the chosen policy and not solely by the `:2713` clobber. Express
  this as the exact expected status for a named failure scenario, not as the word "accident".
- The LTF-010/LTF-011 REFUSED case is the concrete example to reason against: today
  `maintenance:long_term_forecasting` exits 1 on a REFUSED recovery. If the owner decides `$?`
  should distinguish exit 2, say so explicitly — **no document currently claims that it does**, so
  there is nothing to correct if the owner decides otherwise.
- **Once the policy is chosen**, the criteria above establish failure *detection* only — a fix that
  still returns an arbitrary 1 satisfies them. Add exact-code assertions for: a single module
  failure; multiple failures with **different** codes in one run; a module failure plus a validation
  failure; a `FAIL` row whose runner returned 0 (exit 6's branch, `:1017`); and `initialize` failing
  with no FAIL row present. Exercise both Stage 1b targets (`all`, `maintenance`) under
  `--continue-on-error`, since that is the mode the only real caller uses.
- **A validation-only failure needs its own exact-code case**, and the "module failure plus a
  validation failure" case above cannot substitute for it: the resolved precedence means the module
  code masks the validation one, so that test passes without ever exercising the validation path.
  Add a run whose modules all succeed but whose validator fails, asserting the chosen
  validation-only status, the `VALIDATION` FAIL row, and a completed summary. This case is blocked
  on the second OPEN sub-question above. (Out-of-loop review, 2026-09-21.)
- **An exit code alone does not prove the run completed safely.** An errexit abort can return the
  very code the test expects while skipping the summary and every later step — the failure mode
  recorded in place at `run_locally.sh:1513-1522`. So each exact-code assertion must be paired with
  evidence that the run got there properly: the expected result rows present, the summary printed,
  and the downstream work that should (or should not) have run. **State that evidence per path, not
  as a blanket rule** — the documented no-row early exit is a legitimate exception:
  `run_initialize_deployment:1218` returns before recording anything, so `main()`'s `:2712` guard is
  false and the summary is *correctly* absent. For that path the expected evidence is the absence of
  a summary and of any downstream call, not its presence. `test_run_locally_orchestration.py:250-264`
  already does this — it checks that a later ML invocation actually happened — and is the pattern to
  copy. Cover the **default fail-fast path** too, not only `--continue-on-error`: its hardcoded
  `return 1` (`:1561`) is a separate loss mechanism. (Out-of-loop review, 2026-09-21.)
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh pipeline` green, then the full suite
  (`SAPPHIRE_TEST_ENV=True bash run_tests.sh`) before PR.

**`_LT_RC_CASES` locks the exit *bit* plus two strings, not exit codes — but do not under-read it
either.** An earlier revision of this file
called it a locked *exit-code* table that any Defect-B fix would alter. That was wrong. Its columns
are `(lt_rc, expect_nonzero_exit, expect_maintenance_status, expect_long_horizon_row)`
(`apps/pipeline/tests/test_run_locally_orchestration.py:1419-1427`) and
`test_exit_code_table` (`:1458`) asserts `returncode != 0` / `== 0` (`:1470-1473`), so emitting
3, 5 or 6 in place of 1 passes that assertion unchanged. **But the exit bit is not all it
asserts**: `:1475` pins the maintenance status string and `:1477-1482` pin the long-horizon row's
presence and status, so a change touching either fails this test *without* touching the exit
code. Among fix shapes that change only the exit code, the one that disturbs it is "propagate the
module's returned code", which would make `lt_rc=6` exit 0 because module `rc` stays 0 at
`:1017`. That is the **second** owner sub-question above, not a property of the table.

**Shell-level coverage already exists — do not write it from scratch.**
`apps/pipeline/tests/test_run_locally_orchestration.py` drives `run_main()` (imported from
`conftest` at `:77`) end to end and asserts process status in 84 places, including
`TestContinueOnErrorHint` (`:224`), `TestLongHorizonSyncExitCodeHandling` (`:1447`) and the LTF
recovery test `test_refused_exit_2_is_labelled_distinctly_and_stays_nonzero` (`:2419`). An earlier
revision of this file dismissed the coverage as "inspects only
the function body" and pointed at
`apps/preprocessing_runoff/test/test_run_locally_long_horizon_wiring.py`; that was the wrong file,
and a sibling issue already corrected the same claim
(`doc/plans/issues/low_prio_gi_draft_prepq_long_horizon_sdk_norm_path_none.md:411-416`).

## Deliberately out of scope for INFRA-024

**Disposition on close (2026-09-21).** These were never part of Defect B and are *not* cancelled
by its closure — but nothing inherits them either, so they have no owner today. The
`SDK_FAILED` reason/stage taxonomy follow-up is cited from
`doc/plans/issues/low_prio_gi_draft_prepq_long_horizon_sdk_norm_path_none.md:387` as pointing
here; that pointer now resolves to a closed issue. **File a successor if the need is real** —
it is a logging change, independent of exit codes, and none of the reasons for closing Defect B
apply to it.

Recorded so they are not mistaken for completion criteria of this issue. Each was separate work,
and **none of it is owned by anyone today** — see the disposition note above. Listing it here is
not a claim that it is queued:

- **A normalised reason/stage summary for `SDK_FAILED`.** `_extract_sdk_status_code`
  (`sync_long_horizon_hydrograph.py:347-372`) now grades a `ValueError` by its embedded HTTP status
  and `_lookup_monthly_norms` (`:375-443`) routes a 404 to `NORM_ABSENT`; every other status and
  every unparseable failure still collapse into one `SDK_FAILED` bucket with no reason breakdown.
- **The sibling DEBUG-only audit.** The per-station `API_FAILED` log in
  `write_long_horizon_hydrograph` is `logger.debug` (`:743-749`; only the run-level aggregate at
  `:752-757` is a WARNING); the NORM_ABSENT-via-404 case is `logger.info` (`:415-421`) and
  `_lookup_monthly_norms`'s classification-time SDK-failure log is `logger.debug` (`:427-434`). All
  are invisible in production for the same reason — the root logger is capped at WARNING and its
  handlers replaced at import (`setup_library.py:44-55`), so a module's nominal level is irrelevant
  (INFRA-029). Audit `preprocessing_runoff` and the PP-051 recalc family together.
- A no-real-station-codes-at-WARNING rule. This is a **requested behaviour change, not an existing
  contract**: short-horizon warnings still include codes (a single code at
  `sync_short_horizon_hydrograph.py:713-719`; a joined list at `:1044-1050`), and Defect A's own fix
  added one more non-compliant WARNING call site (`sync_long_horizon_hydrograph.py:492-497`). If the
  owner wants it enforced, audit every existing warning path as its own piece of work — several
  deployments ship logs off-host.

## Contract not to break — still live, and the reason to keep this file

*These constraints bind any future change in this area, whether or not it carries INFRA-024's
name. This section is why the file is archived rather than deleted.*


- **`run_validation.sh` Stage 1b must still exit non-zero on a failing run.** See the HAZARD box and
  its measured tables — outage contract (1). Note it is the **bit**, not the number, that Stage 1b
  consumes (`:530-533`) — a fix may change the number freely, never the bit.
- **A total SDK norm-lookup outage (exit 6) must still make the process exit non-zero** — outage
  contract (2), the PREPQ-014/015 blind spot. Its named guard is
  `test_all_stations_sdk_failure_exits_nonzero_end_to_end`
  (`apps/pipeline/tests/test_run_locally_orchestration.py:1649`), whose own docstring calls it *"the
  regression guard for the PREPQ-015 property"*. This is the contract a Stage-1b-only fix can
  satisfy on paper and still break: module `rc` stays 0 at `:1017`, so the recorded row is the only
  thing carrying the failure, and the affected targets (`maintenance:preprocessing_runoff`,
  `initialize`) are ones Stage 1b never runs.
- **`set -euo pipefail` (`run_locally.sh:119`), and the rule that decides whether a capture site is
  safe.** The exemption is **dynamic, not lexical**: bash disables `-e` for the entire dynamic extent
  of a function invoked on the left of `||` — the body *and* everything it calls. So:
  - `X || rc=$?` (with `rc` pre-initialised) is safe **unconditionally**, wherever it sits.
  - `X; rc=$?` is safe **iff every path reaching the enclosing function passes through an exempt
    context.** The three functions below are **examples, not an inventory** — each is safe only
    because of where it is called:
    `run_initialize_deployment`'s bare `run_in_venv` + `rc=$?` (`:1241-1243`, `:1256-1259`,
    `:1273-1275`), `run_all`'s `st_rc=$?` (`:1679`), and `run_api_validation`'s `local rc=$?`
    (`:1471-1473`) — each reached only from a dispatch `<runner> || exit_code=$?`.

    **The file has 20 bare `X; rc=$?` capture statements, not three.** 23 lines match `rc=$?` on
    a line of their own; **three** of those (`:783`, `:965`, `:971`) are `else`-branch captures under
    an `if`, a different and already-exempt shape. Unaudited examples include `run_preprocessing_runoff`
    (`:649`), `run_initialize_deployment`'s Step 1 (`:1227`), `run_all`'s `lt_rc=$?` (`:1683`)
    and the LT recovery runner (`:1408`). **Audit all 20 capture statements' calling contexts before relying on this
    hardening** — an earlier revision of this file presented the three above as the complete set.

  **The single most likely "capture the numeric code" fix — rewriting `X || rc=$?` as `X; rc=$?` —
  trips errexit and kills the script mid-run wherever that condition does not hold.** Measured:
  converting `run_module_validation`'s `|| rc=$?` (`:1525`) to a bare call kills the run (exit 3, no
  VALIDATION row, no summary printed), because its six call sites are **bare statements in the
  dispatch `case`** (`:2636`, `:2644`, `:2649`, `:2681`, `:2688`, `:2696`), outside any `||`.
  Converting `run_api_validation` the other way is behaviour-identical. The repo has already been
  bitten by this and documents it in place at `:1513-1522` (INFRA-037 defect 1): *"a bare failing
  `run_in_venv` would trip `set -e` and kill the whole script on the spot — before `record_validation`
  below ever runs, silently defeating the 'don't abort pipeline mid-run' contract."* This is the
  highest-risk implementation pattern in this issue.
- **Had a fix gone ahead: do not preserve the `run_module_validation` / `run_api_validation`
  asymmetry — remove the dependency on it.** (**This bullet alone is conditional** — it is a
  prescription for an implementation that never happened, withdrawn with the rest of the
  capture-hardening directive. The other constraints in this section are **not** conditional: they
  bind any future change in this area, which is the whole reason the section is marked still-live.) The repo records the guard as *reactive* (added after INFRA-037 defect 1 killed
  the script); `run_api_validation` carries no rationale at all and its safety follows from where it
  happens to be called. Like `run_yearly_pipeline`'s trailing guard idiom, this is an accident of
  call-site placement, not a design. Converting the **seven** capture statements in the three
  example functions named above (`:1227`, `:1243`, `:1259`, `:1275`, `:1679`, `:1683`,
  `:1471-1473`) to `X || rc=$?` costs nothing and is measurably behaviour-identical.
  **`:1683` is easy to miss and load-bearing**: `run_all` has *two* captures, `st_rc=$?` (`:1679`)
  and `lt_rc=$?` (`:1683`), and converting only the first leaves the function still dependent on
  its calling context — measured, it exits 7 before validation when called outside an exempt one — and it is strictly safer than leaving a
  "capture the numeric code" change free to relocate a call site, or to give `initialize` a dispatch
  shape that is not `|| exit_code=$?`, and kill the script with no row recorded.
- **ML-021 decision 4: an ML failure in one horizon must not stop the next.** The four ML call sites
  (`:1577`, `:1728`, `:1787`, `:1812`) use `… && continue` rather than the guard idiom's
  `PIPELINE_ABORTED=true; return 1`, deliberately (comment at `:1578-1592`). They are extra
  exit-code loss points, but unifying them with the guard idiom to fix that would revert a recorded
  owner decision. Treat them as four separate sites needing their own handling.
- Exit codes 2 / 4 / 5 / 6 from `sync_long_horizon_hydrograph.py` (6 added by INFRA-044, 2026-09-07)
  are branched on by `run_maintenance_preprocessing_runoff` (`run_locally.sh:977-1028` — the range
  runs to the closing `fi`, and deliberately includes the generic fallback
  `elif [ $lt_rc -ne 0 ]; then rc=$lt_rc` at `:1021-1024`, which is the branch that makes any
  *unmapped* code fatal to the module). Do not renumber without updating that mapping. Defect B's
  original premise ("the mapping affects log text only") no longer holds for 4 vs. 6: the mapping
  also decides whether a result row is recorded at all (4: no row; 6: `FAIL` row) and, transitively,
  whether the script's exit code goes non-zero — even though both leave the *module*-level `rc` at 0.
- The terminal per-run outcome for the long-horizon sub-step still differs by exit code, all three
  citations in `apps/run_locally.sh`: exit 4 logs `log INFO` at `:995`; exit 6 logs `log ERROR` at
  `:1008` and records the `FAIL` row at `:1017`. `_LT_RC_CASES` locks the row-and-status half of
  that split (`expect_maintenance_status`, `expect_long_horizon_row`) plus the zero/non-zero bit —
  not the numbers.
