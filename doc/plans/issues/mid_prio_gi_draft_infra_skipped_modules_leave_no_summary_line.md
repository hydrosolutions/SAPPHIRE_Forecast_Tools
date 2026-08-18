## A skipped module leaves no `PIPELINE SUMMARY` line, so "N passed, 0 failed" can describe a run whose headline module never executed (INFRA-030)

**Status**: Draft (2026-08-18)
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

## Implementation sketch

1. Extend `record_result` itself with a fifth parameter and a matching `RESULTS_REASON` array,
   then allow a `SKIP` status. The reason text already exists at each skip site — reuse it verbatim.
2. Teach `print_summary` a third branch at `:1531` (and `:1555`): count skips separately, print
   them with the reason, and include them in the totals line.
3. Call `record_result` with the skip status and reason at the five skip sites above. Note the
   signature must be extended first: `record_result()` currently declares **four** positional
   parameters (`module`, `status`, `elapsed`, `error_log` — `:326-330`) and appends to four
   parallel arrays, so a fifth "reason" argument is silently dropped until a `RESULTS_REASON`
   array and its parameter are added. Step 1 and step 3 are one change, not two.
4. Leave the exit code alone: a skip is not a failure. This issue does **not** touch the exit
   contract — INFRA-024 owns exit-code normalisation and PP-051/PP-055 own the module-level
   contracts.

## Testing

- [ ] `run_locally.sh maintenance:machine_learning` with `SAPPHIRE_PREDICTION_MODE` unset and
      `ML_MODE=DECAD` emits a summary line `machine_learning (maintenance): SKIP (ML_MODE=DECAD)`
      and exits 0.
- [ ] `run_locally.sh long-term-operational` on a non-issue day reports the long-term module as
      SKIP with the gate reason, and the totals line names the skip.
- [ ] A skipped module does not increment `fail_count` and does not change the exit code.
- [ ] An org-level skip (`ORG=uzhm`) produces a SKIP line rather than silence.
- [ ] `SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero unexpected skips.

## Out of scope

- Whether the ML mode/`ML_MODE` defaults *should* disagree (that is ML-016's territory) — this
  issue only makes the resulting skip visible.
- Exit-code semantics (INFRA-024).
- The Docker/Luigi pipeline's own summary, if it differs — check it, and file separately if the
  same shape exists there.

## Acceptance criteria

- [ ] Every module a target covers appears in the summary with PASS, FAIL or SKIP.
- [ ] The totals line reports skips separately from passes and failures.
- [ ] No skip is reported as PASS or FAIL.
- [ ] Exit codes are unchanged by this issue.
