## A skipped module leaves no `PIPELINE SUMMARY` line, so "N passed, 0 failed" can describe a run whose headline module never executed (INFRA-030)

**Status**: Review (2026-09-07). Implemented on `apps/run_locally.sh`
(`fix_infra030_skip_summary` branch): a new `record_skip()` wraps `record_result` with a `SKIP`
status and a reason string; `print_summary` renders each as `<module>: SKIP (<reason>)`, counts
skips separately from pass/fail, excludes them from the red `MODULE ERROR DETAILS` block, leaves
the exit code untouched, and appends `, N skipped` to the totals line only when N > 0. Scope
actually shipped is **gate-level**, narrower than this file's original "every module … PASS,
FAIL or SKIP" wording — see § Acceptance criteria for the corrected contract and why. Passed
out-of-loop review; automated coverage is
`apps/pipeline/tests/test_run_locally_orchestration.py::TestSkipSummaryRows` (10 tests, 3 of
them mutation-verified). Full `apps` test suite green, 16/16 modules and services, zero
failures and zero unexpected skips (`cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh`).
**Module**: `apps/run_locally.sh` (and the same reporting contract in the Docker/Luigi pipeline)
**Priority**: **Medium** — the gating decisions themselves are correct; the reporting of them
is not. Nothing is mis-computed, but a summary reader cannot tell "correctly gated out" from
"never ran". Owner to confirm.
**Labels**: `infra`, `run_locally`, `reporting`, `observability`
**Found**: 2026-08-18, local kghm (kyg) end-to-end review on `maxat_sapphire_2` @ `a304ffb0`.
**Related**: **INFRA-020** (`validate_pipeline --module machine_learning` matches zero checks
and reports PASS on no evidence — the validation-side twin), **PREPG-009** (reports PASS with
all sub-tasks errored), **INFRA-024** (`run_locally.sh` normalises a recorded-FAIL module's
exit code to 1), **INFRA-029** (the module's own INFO output is suppressed, so the module log
cannot compensate for the missing summary line).

---

## Observation

Three separate runs on 2026-08-18 skipped their headline module and reported a summary that
does not mention it.

**1. `bash apps/run_locally.sh maintenance:machine_learning`** — did nothing, recorded nothing:

```
[WARN] SAPPHIRE_PREDICTION_MODE not set, defaulting to PENTAD
[INFO] ML mode: DECAD
[INFO] Skipping machine_learning maintenance for PENTAD (ML_MODE=DECAD)
```

No `PIPELINE SUMMARY` module line at all — no PASS, no FAIL, no SKIP. Two independent defaults
disagree (invocation mode defaults to PENTAD; `ML_MODE` defaults to DECAD), so the run is a
guaranteed no-op **for the default invocation**, and it says so only in an INFO line that no
summary reader looks at.

**2. `bash apps/run_locally.sh long-term-operational`** — the long-term pipeline gated out
correctly (kyg issue days are 10 and 25; the run was on the 18th):

```
[WARN] No active modes today, skipping long-term pipeline
```

The summary then reported **"Modules: 2 passed, 0 failed"** — the two preprocessing modules
that run before the gate. A green summary for a run in which the long-term forecast step never
executed.

**3. `should_skip_module`** — org-level skips (`demo`, `uzhm`) take the same silent branch.

By contrast, `postprocessing_forecasts` and `linear_regression` no-ops on the same day *did*
report `PASS`, because they were invoked and returned 0. So the summary distinguishes
"invoked, did nothing" from "not invoked" only by omission — the one signal that a reader
scanning for failures will not notice.

## Mechanism

`run_locally.sh` records a result only where a module runner function completes:

- `record_result()` (`:326-336`) appends to four parallel arrays; it is called exclusively from
  the `run_*` functions, always with `PASS` or `FAIL`.
- No skip site calls it. There are three dispatch paths and they are **not** identical, so a fix
  must touch each:
  - `:1273-1276` and `:1333-1336` — a matching pair: `should_skip_module machine_learning` takes a
    bare `:` branch, and `should_skip_ml_for_mode` logs an INFO line and falls through.
  - The single-target path is split: the org-level skip is at `:1872-1873` and logs
    `"Skipping maintenance:machine_learning (not required for ${ORG} org)"` — an INFO line, not a
    bare `:` — while the mode skip at `:1887-1890` logs and `continue`s. Same outcome, different
    shape from the pair above.
  - `:1202-1204` — the long-term gate logs a WARN and `return 0`s **before** any module runner is
    reached.
- `print_summary()` (`:1515-…`) classifies each recorded status with a **binary** test:
  `if [ "$status" = "PASS" ]` … `else` → prints FAIL and increments `fail_count` (`:1531`, and
  again for validation results at `:1555`).

That last point is the implementation trap: **adding `record_result "…" "SKIP"` without
touching `print_summary` would print the skipped module as FAIL.** Both ends must change
together.

## Desired outcome

A run's summary accounts for every module the target was supposed to cover, with three
outcomes rather than two — `PASS`, `FAIL`, `SKIP (reason)` — where `SKIP` counts separately and
does not affect the exit code. A reader who sees "Modules: 2 passed, 0 failed, 1 skipped
(long-term: no active modes today)" learns what actually happened; today's "2 passed, 0 failed"
does not.

## Status-matching landscape — read this before touching `print_summary` (added 2026-09-07)

Two things have changed since this issue was written, and both affect step 2.

**1. `print_error_details` now matches an explicit closed set, not "everything that is not PASS".**
LTF-010 (PR #495) added a third status, `FAIL (REFUSED)`, for the long-term recovery target's
exit 2. To give that row its log tail, `print_error_details` gained a helper:

```bash
_is_fail_status() {
    [ "$1" = "FAIL" ] || [ "$1" = "FAIL (REFUSED)" ]
}
```

An earlier revision of that change used `!= "PASS"` instead. **That was deliberately replaced with
the closed set precisely so this issue's `SKIP` would not be swept into it** — under `!= "PASS"`, a
run containing both a genuine failure and a skip would have rendered the skipped module inside the
red MODULE ERROR DETAILS block, typically as "(no output captured)".

**So when you add `SKIP`, do not add it to `_is_fail_status`.** The helper is the seam; leaving it
alone is the correct action, and that is easy to get wrong because the obvious reading is "add my
new status everywhere statuses are handled".

**2. `print_summary`'s own branching is still `if PASS … else`,** and was never changed. That means
today **every** non-`PASS` status is counted as a failure and printed in the failure branch. LTF-010
deliberately did **not** restructure it: converting it to a closed set at that point would have
silently dropped any future non-PASS/non-FAIL status from the counts *entirely*, which is worse than
miscounting it.

**That leaves the fix squarely with this issue.** Step 2 must convert `print_summary` to three
explicit branches — `PASS`, the fail set, and `SKIP` — rather than adding a `SKIP` case to an
`if/else` whose `else` currently means "failure". Getting this wrong yields either a skip counted as
a failure (if you leave the `else`) or a skip counted as nothing at all (if you add an `elif` and
forget the totals line).

**Verify against the tree you are working on.** PR #495 was open, not merged, when this note was
written; if it was rejected or reworked, `_is_fail_status` may not exist and the first point above
needs re-deriving. `grep -n "_is_fail_status\|= \"PASS\"" apps/run_locally.sh` settles it in one
command.

**Also note INFRA-044's decision**, so nobody plans around a state that does not exist: the
`DEGRADED` status that issue originally proposed was **not built** (owner decision, 2026-09-04) —
with exit 4 recording no row at all, it would have shipped with no producer. If a degraded state is
ever wanted, this issue's three-way renderer is the natural place to add it, and LTF-011's split
recovery outcomes are the most likely first consumer.

## Implementation sketch

1. Extend `record_result` itself with a fifth parameter and a matching `RESULTS_REASON` array,
   then allow a `SKIP` status. The reason text already exists at each skip site — reuse it verbatim.
2. Teach `print_summary` a third branch: count skips separately, print them with the reason, and
   include them in the totals line. **Read the status-matching section above first** — the `else`
   branch currently means "failure", so this is a restructure, not an added case, and
   `print_error_details`'s `_is_fail_status` must be left alone. Re-derive the line numbers with
   `grep -n`; the ones this issue originally cited are stale.
3. Call `record_result` with the skip status and reason at the five skip sites above. Note the
   signature must be extended first: `record_result()` currently declares **four** positional
   parameters (`module`, `status`, `elapsed`, `error_log` — `:326-330`) and appends to four
   parallel arrays, so a fifth "reason" argument is silently dropped until a `RESULTS_REASON`
   array and its parameter are added. Step 1 and step 3 are one change, not two.
4. Leave the exit code alone: a skip is not a failure. This issue does **not** touch the exit
   contract — INFRA-024 owns exit-code normalisation and PP-051/PP-055 own the module-level
   contracts.

## Testing

- [x] `run_locally.sh maintenance:machine_learning` with `SAPPHIRE_PREDICTION_MODE` unset and
      `ML_MODE=DECAD` emits a summary line and exits 0 — covered by
      `TestSkipSummaryRows::test_maintenance_ml_mode_mismatch_now_prints_a_summary_at_all`
      (reason text is `ML_MODE=DECAD, mode=PENTAD`, i.e. the shipped reason also names the
      resolved mode, not just `ML_MODE=DECAD` as this line originally predicted).
      **SUPERSEDED 2026-09-09 (`refactor_run_locally_drop_ml_mode`)**: `ML_MODE` no longer
      exists, so this exact scenario can no longer be constructed and the cited test was renamed
      to `test_maintenance_ml_unset_mode_runs_pentad_not_silent_noop` (unset
      `SAPPHIRE_PREDICTION_MODE` now runs PENTAD directly, with nothing left to filter it back
      out) — see ML-022's own file for the closing detail.
- [x] `run_locally.sh long-term-operational` on a non-issue day reports the long-term module as
      SKIP with the gate reason, and the totals line names the skip — covered by
      `TestSkipSummaryRows::test_operational_schedule_gate_records_skip_row`.
- [x] A skipped module does not increment `fail_count` and does not change the exit code — covered
      by `TestSkipSummaryRows::test_one_fail_and_one_skip_fail_count_is_one_not_two` (mixed
      FAIL+SKIP run: `fail_count` is 1, not 2, exit code still reflects only the real failure) and
      `TestSkipSummaryRows::test_direct_dispatch_arm_skip_site` / `test_whole_pipeline_org_gate_skip_site`
      (skip-only runs exit 0).
- [x] An org-level skip (`ORG=uzhm`) produces a SKIP line rather than silence — covered by
      `TestSkipSummaryRows::test_short_term_both_mode_org_skip_records_one_row_per_horizon`
      (`ieasyhydroforecast_organization=uzhm`) and, for `demo`,
      `test_whole_pipeline_org_gate_skip_site` / `test_direct_dispatch_arm_skip_site`.
- [x] `SAPPHIRE_TEST_ENV=True bash run_tests.sh` (`pipeline`) — zero failures, zero unexpected
      skips. Verified 2026-09-07.

## Out of scope

- Whether the ML mode/`ML_MODE` defaults *should* disagree (that is ML-016's territory) — this
  issue only makes the resulting skip visible.
- Exit-code semantics (INFRA-024).
- The Docker/Luigi pipeline's own summary, if it differs — check it, and file separately if the
  same shape exists there.

## Acceptance criteria

**Corrected 2026-09-07 to the gate-level contract actually implemented, and confirmed by the
owner the same day** — gate-level is the wanted behaviour, not merely the shipped one. Do not
reopen this as a defect; a run that skips a whole section prints one line naming the section, and
that is the intended summary.

**Corrected 2026-09-07 to the gate-level contract actually implemented.** The original first
bullet below ("every module … appears … with PASS, FAIL or SKIP") over-claims: it reads as
runner-level accounting, but one gate can guard several runners and only the headline one gets a
row. For example `run_daily_pipeline`'s Phase 5 long-term gate, when it fires, suppresses
`run_long_term_forecasting_operational`, `run_postprocessing_long_term`,
`run_recalculate_long_term_skill_metrics`, and `run_maintenance_postprocessing_long_term` — four
runners — but records exactly one row, labelled `long_term_forecasting (operational)`. Runner-level
accounting (one row per suppressed runner) was presented to the owner with both output shapes side
by side on 2026-09-07 and declined in favour of the shorter summary: it would need
either a caller-supplied list of the runners a gate covers, or a static map from gate to runner set,
and neither existed before this change. The `reason` string parameter and the three-way branch in
`print_summary` both support adding it later without another restructure, if it is ever wanted.
Note the third branch is `PASS` / `SKIP` / **else**, and the `else` deliberately still means "any
other status is a failure" — it is *not* a closed fail set, so an unrecognised status is still
counted and rendered rather than silently dropped from all three counts
(`TestSkipSummaryRows::test_unknown_status_is_still_counted_as_a_failure` pins this).

- [x] Every explicit neutral gating branch this issue's Observation/Mechanism sections identified
      (org-level `should_skip_module`, an `ML_MODE`/mode mismatch via `should_skip_ml_for_mode`,
      and the long-term schedule gate) records **exactly one** `SKIP` row, labelled with the
      headline runner the gate controls — not one row per runner the gate suppresses.
      **SUPERSEDED 2026-09-09**: `should_skip_ml_for_mode` and `ML_MODE` were removed entirely
      (`refactor_run_locally_drop_ml_mode`), so that particular gating branch no longer exists —
      there is nothing left there to record a SKIP row for. The other two gates (org-level
      `should_skip_module`, the long-term schedule gate) are unaffected and still covered.
- [x] The totals line reports skips separately from passes and failures (`, N skipped`, shown only
      when N > 0 — see Invariant 5,
      `TestSkipSummaryRows::test_no_skip_baseline_totals_line_is_byte_identical_to_today`).
- [x] No skip is reported as PASS or FAIL, and no skip is swept into `MODULE ERROR DETAILS`
      (`TestSkipSummaryRows::test_one_fail_and_one_skip_fail_count_is_one_not_two`).
- [x] Exit codes are unchanged by this issue (every `TestSkipSummaryRows` case with only SKIP rows
      asserts `returncode == 0`; the mixed FAIL+SKIP case asserts `returncode == 1` driven by the
      FAIL alone).
