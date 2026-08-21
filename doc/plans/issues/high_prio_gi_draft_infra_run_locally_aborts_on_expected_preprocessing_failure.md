# INFRA-037: An expected `preprocessing_runoff` failure aborts the whole `daily` run

**Status**: Draft (2026-08-20). Implemented — the diagnosis below was corrected after direct
evidence and a fix has been built against the corrected diagnosis (see § What was actually
built). The full `run_tests.sh` gate is confirmed green: 16/16 modules and services pass, zero
failures, and no skips introduced by this branch (15 skips pre-existed; 1 more arrived from
trunk during a rebase, gated on bash < 4). Multiple rounds of out-of-loop adversarial review have
run against this branch; see § Acceptance criteria for the current, verified state.
**Module**: `apps/run_locally.sh` (orchestration) + `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`
(exit semantics of the maintenance sub-step that actually fails — **not**
`preprocessing_runoff.py`, see § Correction below)
**Priority**: **High** (raised from Medium 2026-08-20) — no longer hypothetical.
This is **confirmed to be blocking a colleague's kghm deployment right now**, and
together with ML-016 it accounts for a reported "machine_learning produces no
forecasts" with no other cause present (see § Field evidence). A documented
workaround exists, but it is undiscoverable at the moment of need. Cost is
operator time and a misleading failure mode, not data loss.
**Labels**: `infra`, `orchestration`, `run_locally`, `dx`, `error-message`
**Discovered**: 2026-08-20, while debugging "machine_learning produces no
forecasts" on a remote deployment. The operator could not use
`bash run_locally.sh daily` at all and fell back to invoking every module by
hand, which is what surfaced this.

---

## Correction to an earlier draft — read this before the rest of the file

**An earlier version of this file diagnosed the abort as one of
`preprocessing_runoff.py`'s seven `sys.exit(1)` sites, and claimed no graded
"primary write succeeded, secondary sink failed" exit class existed anywhere
in the codebase and would have to be created. Both of those are wrong, and
are retracted here rather than silently edited out**, because a reader who
saw the earlier version needs to know it changed and why:

- The abort is **not** in `preprocessing_runoff.py` and not at any of its
  seven `sys.exit(1)` sites (those are still real, and still dead-code-laced
  — see § Dead code below — but they are not on the path the operator hit).
- It is in `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`, a
  **sub-step** that `run_maintenance_preprocessing_runoff` (`run_locally.sh`)
  runs immediately after `preprocessing_runoff.py --maintenance` succeeds.
  That makes it **Phase 2** of the `daily` pipeline (maintenance
  preprocessing), not Phase 1 (operational preprocessing) — in the reported
  run, Phase 1 passed cleanly.
- A **graded exit-code taxonomy already existed** for this sub-step before
  this issue was filed:
  `_exit_code_for_long_horizon_summary` (`sync_long_horizon_hydrograph.py`)
  already returned 0/1/2/3/4/5 for
  success/API-setup-error/no-records/unexpected-exception/SDK-norm-failure/API-failure,
  and the module's own `main()` docstring documented all six. `run_locally.sh`
  already discriminated between them: exit 2 was already treated as
  warn-and-continue (`log WARN "...produced no records; continuing
  maintenance"`), and exits 4 and 5 were logged distinctly. What was missing
  was not the taxonomy — it was that exit 4 was folded into the same overall
  `rc` as exits 1/3/5, so it aborted the whole `daily` run exactly like a
  genuinely fatal failure would.

See § Mechanism (corrected) and § What was actually built for the full,
verified picture.

---

## Field evidence — kghm, 2026-08-20 (this is why the priority moved)

A colleague reported "operational data arrives fine, but machine_learning
forecasts are not produced" on a `kghm` deployment, could not use
`run_locally.sh daily` because of an *expected* `preprocessing_runoff` failure,
and fell back to invoking modules by hand.

**Four competing explanations were tested and eliminated**, which is what leaves
this issue as the cause:

| Hypothesis | Verdict | Evidence |
|---|---|---|
| `.env` misconfiguration | **Ruled out** | His env file vs a working one: 178/182 keys shared, 168 identical. **Every ML-critical key matches** — `organization=kghm`, `available_ML_models=TFT,TIDE,TSMIXER`, `SAPPHIRE_API_URL`, `SAPPHIRE_API_ENABLED=true`, `HRU_CONTROL_MEMBER`, all scaler paths. The 10 differences are machine paths, secrets, service-container URLs (which the `apps/` modules never read — they use `SAPPHIRE_API_URL` exclusively), and two deliberate threshold choices. |
| Missing/unreachable model artefacts | **Ruled out** (owner) | Shared configuration folder, so the same 1.1 GB `models_and_scalers` tree. Scaler dir names match the env values exactly, including `TiDE`/`TSMixer` casing. |
| API stack down | **Ruled out** (owner) | Also distinguishable by symptom: preprocessing down raises `SapphireAPIError` loudly; only postprocessing-down is silent. |
| Empty `rivers_to_predict` | **Ruled out** | `get_hydroposts_for_pentadal_and_decadal_forecasts()` run against the shared station config yields **54 selected stations** and **62 flagged available** for each of TFT/TIDE/TSMIXER ⇒ `rivers_to_predict` = 54, not empty. The config is shared, so this holds for his machine too. |

**What remains is this issue plus ML-016**, and together they explain the report
completely without any further cause:

1. `run_locally.sh daily` aborts at the expected `preprocessing_runoff` failure
   before ML is reached — **this issue**.
2. `run_locally.sh machine_learning` on its own then crashes with
   `ValueError: Prediction mode %s is not supported` unless the operator exports
   `SAPPHIRE_PREDICTION_MODE` — **ML-016**.

So every route to running ML is blocked, and the operator's conclusion ("ML does
not work") is a reasonable reading of a pipeline that never executed it. **The two
should be scheduled together**: fixing either alone still leaves one blocked route.

**Now confirmed:** the exit hit is `sync_long_horizon_hydrograph.py` exit 4 —
at least one station's iEasyHydro-HF monthly-norm SDK lookup raised. Reported
counts for the run: 62 stations attempted, 53 written, 5 norm-absent
(a separate, already-non-fatal degraded-success case), 4 SDK-failed, 0
API-failed. This is **not** one of `preprocessing_runoff.py`'s seven
`sys.exit(1)` sites — see § Correction above. The diagnostic task this file
originally left open is now closed; nothing about which exit site fires is
still unknown.

---

## Symptom

`bash apps/run_locally.sh daily` used to stop with the whole run aborted once
`run_maintenance_preprocessing_runoff`'s long-horizon hydrograph sub-step
(`sync_long_horizon_hydrograph.py`) exited 4. The failure is one the owner
regards as **expected** for that deployment — a handful of stations whose
iEasyHydro-HF monthly-norm SDK lookup raises, not a regression or a
misconfiguration to be fixed — yet it took the entire day's run with it, even
though Phase 1 (`preprocessing_runoff.py`, operational) had already succeeded
and Phase 3 (`linear_regression`, `machine_learning`,
`postprocessing_forecasts`) has **zero dependency** on the month-horizon
hydrograph rows this sub-step writes: neither `machine_learning` nor
`linear_regression` reads them. That mismatch — one degraded sub-step of one
maintenance phase vetoing three unrelated modules' worth of forecasting — is
why the abort was disproportionate, independent of whether exit 4 should be a
failure at all.

The operator's workaround was to run each module manually, which is slow,
easy to get wrong (see ML-016 for one of the traps that lie in wait there),
and loses the ordering and mode handling the orchestrator provides.

---

## Mechanism (corrected)

`run_daily_pipeline` guards every module call with the same idiom
(e.g. `apps/run_locally.sh:1516`, Phase 1; `:1523`, Phase 2). The idiom now
also records the abort as a fact, via `PIPELINE_ABORTED` (see § What was
actually built):

```bash
run_maintenance_preprocessing_runoff || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
```

`CONTINUE_ON_ERROR` defaults to `false` (`:118`). So the default behaviour is
fail-fast on the first module that returns non-zero. The reported abort was at
**Phase 2** (`run_maintenance_preprocessing_runoff`, `:1523`) — maintenance
preprocessing — not Phase 1 (`run_preprocessing_runoff`, `:1516`, which had
already passed).

`run_maintenance_preprocessing_runoff` runs `preprocessing_runoff.py
--maintenance`, and only if that succeeds, runs
`sync_long_horizon_hydrograph.py` as a second sub-step. Before this issue's
fix, the sub-step's exit code was folded straight into the function's own
`rc` for every non-zero value except 2 (which was already warn-and-continue):

```bash
if [ $lt_rc -eq 2 ]; then
    log WARN "...produced no records; continuing maintenance"
elif [ $lt_rc -eq 4 ]; then
    log ERROR "...had SDK norm lookup failure(s)"
    rc=$lt_rc          # <-- this is what made exit 4 fatal
elif [ $lt_rc -ne 0 ]; then
    rc=$lt_rc
fi
```

So a `sync_long_horizon_hydrograph.py` exit 4 (SDK norm lookup failure) made
`run_maintenance_preprocessing_runoff` return non-zero, which tripped the
Phase 2 guard idiom above and aborted the whole `daily` run before Phase 3
(where `machine_learning` lives) was ever reached.

**The `--continue-on-error` flag exists and works** — but *works* means
"continues to later modules", **not** "exits 0". The guard idiom is subtle under
`set -euo pipefail`: when `CONTINUE_ON_ERROR=true` the `[ … ]` test fails and the
brace group's status is 1, which looks like it should trip `set -e`. It does not,
and the pipeline continues.

> **Correction to an earlier draft.** That draft claimed the script "exits 0". It
> does not. The failed module is still recorded `FAIL`, `print_summary` returns 1,
> and `main` sets a non-zero `exit_code`. The earlier claim came from a scratch
> reproduction of the guard idiom **alone**, which omitted
> `record_result`/`print_summary` — it validated the idiom, not the script.
> Lesson worth keeping: a simplified repro tests the fragment you extracted, not
> the behaviour you care about.

The flag is parsed position-independently, so it can be given before or after
the target:

```bash
bash apps/run_locally.sh --continue-on-error daily
```

So the bug was **not** a broken flag, and never was — that finding from the
earlier draft still holds.

---

## What was actually built

The fix has two independent halves, matching Candidate B and Candidate A from
the earlier draft's framing (kept below for continuity), but Candidate A's
concrete shape is different from what that draft proposed — see § Correction.

### 1. The `--continue-on-error` hint (was Candidate B — definite, cheap)

Before the fix, when a module failed with `CONTINUE_ON_ERROR=false`, the run
printed only:

```
[ERROR] preprocessing_runoff (maintenance) failed (exit 1) after 1m 12s
```

and a `PIPELINE SUMMARY` line reading `FAIL`, with no mention that
`--continue-on-error` exists or that it is *why* the run stopped. The flag was
documented in the file header and in `--help`, so this was undiscoverable at
the moment of need, not undocumented.

**Now**, `main()` calls a new `emit_continue_on_error_hint` exactly once,
after dispatch, whenever the run aborted via the `CONTINUE_ON_ERROR` guard
idiom. (The exact wording is being iterated on separately — do not quote it
here; that is how this passage went stale before. Describe it structurally
instead: it names the `--continue-on-error` flag, gives the exact command to
re-run with it, and states that the run will still exit non-zero even then.)

This fires on the **fact of an abort**, not on the target's name. A
`PIPELINE_ABORTED` flag is set at each of the guard idiom's 39 call sites
(every pipeline function, across every target) the moment `CONTINUE_ON_ERROR`
is false and a guarded step fails; `main()` checks that single flag once,
after dispatch, regardless of which target ran. No target is excluded —
`all`, `yearly` and `initialize` route through the same post-dispatch check
as `daily`, `short-term`, `long-term`, `long-term-operational` and
`maintenance`, and the hint fires for any of them if the guard idiom aborted
something they call. (An earlier draft of this file described the gating as
an `IS_FAIL_FAST_TARGET` flag that excluded `all`, `yearly` and `initialize`
by target name — no such flag exists in the shipped code. Excluding those
targets was the defect the fix corrected, not a design choice it kept.)

### 2. Exit 4 no longer folds into the module's overall status (was Candidate A)

The earlier draft's Candidate A proposed catching a `to_csv` traceback in
`preprocessing_runoff.py` and giving it a new, distinct warn-and-continue
exit code, on the premise that no graded exit class existed. **That premise
was wrong** (see § Correction) — the graded taxonomy already existed in
`sync_long_horizon_hydrograph.py`, just not fully honoured by the caller.

The actual fix, in `run_maintenance_preprocessing_runoff`
(`apps/run_locally.sh`):

- **Exit-code precedence swapped** in `sync_long_horizon_hydrograph.py`'s
  `_exit_code_for_long_horizon_summary`: API failures (5) now take precedence
  over SDK failures (4) when a run has both, so a genuine API problem is
  never masked by a co-occurring SDK problem.
- **Per-station SDK-failure logging raised from DEBUG to WARNING**
  (`write_station_monthly_hydrograph`), so the specific station and exception
  are visible in a default-level log instead of requiring `DEBUG` to be
  enabled.
- **Exit 4 no longer sets `rc`.** `run_maintenance_preprocessing_runoff` now
  records a **separate** `preprocessing_runoff (long-horizon sync)` `FAIL`
  row via `record_result` and logs an ERROR line, but leaves the function's
  own `rc` (and thus its return value) at whatever `preprocessing_runoff.py
  --maintenance` itself returned. That lets the Phase 2 guard idiom see
  success and the `daily` run continue into Phase 3, where `machine_learning`
  runs — while the run **still exits non-zero overall**, because the separate
  FAIL row still makes `print_summary` return 1.
- **Exits 1, 3 and 5 remain fatal exactly as before** — only exit 4 (SDK
  lookup failure, a degraded-but-partial-success condition) was reclassified.
  Exit 2 (no records) was already warn-and-continue and is unchanged.

This is additive to the existing taxonomy, not a new one, and does not touch
`preprocessing_runoff.py` itself or what it reads/writes — see § Scope
boundaries, unchanged from the earlier draft.

### Dead code — filed separately, not part of this fix

`preprocessing_runoff.py` still has seven `sys.exit(1)` sites, three of which
(`:523`, `:536`, `:653`) are unreachable dead code — verified against source
in the earlier draft and independently by two out-of-loop reviewers. That
analysis is **still correct** but was never on the operator's actual failure
path (see § Correction) and has been filed as its own draft:
[`low_prio_gi_draft_preprocessing_runoff_dead_exit_sites.md`](low_prio_gi_draft_preprocessing_runoff_dead_exit_sites.md).
Do not duplicate that analysis here.

### Known limitation — per-mode `machine_learning` MODULE rows still collide (not fixed here)

Review round 3 fixed the per-mode **VALIDATION** rows for the bare
`machine_learning` single-module target: `run_module_validation`
(`apps/run_locally.sh:1285`) now takes an optional mode suffix, and the
`machine_learning` case branch (`apps/run_locally.sh:2216`) passes each
attempted mode as that suffix, so `ML_MODE=BOTH machine_learning` produces
distinguishable rows — `api_validation (machine_learning PENTAD)` and
`api_validation (machine_learning DECAD)` — each with its own log file.
`daily`'s own validation is unrelated to this fix: it is a single aggregate
call, `run_api_validation "daily"` at `run_locally.sh:1583`, not per-mode.

The per-mode **MODULE** rows were not fixed. `run_machine_learning`
(`apps/run_locally.sh:700`, called from Phase 3) always truncates
`${ERROR_DIR}/machine_learning.log` and always calls
`record_result "machine_learning" ...` with no mode suffix.
`run_maintenance_machine_learning` (`apps/run_locally.sh:975`, called from
Phase 4 — a different function, not `run_machine_learning`) does the same
for `${ERROR_DIR}/machine_learning_maintenance.log` and
`record_result "machine_learning (maintenance)" ...`. `run_daily_pipeline`
(`apps/run_locally.sh:1509`) loops `for mode in PENTAD DECAD` in both
phases, so two calls to the same function in one run produce
indistinguishable rows, and the second call's log overwrites the first's.

This does not happen by default. `should_skip_ml_for_mode`
(`apps/run_locally.sh:479`) skips the mode that doesn't match `ML_MODE`, and
`ML_MODE` defaults to `DECAD` (`run_locally.sh:194`), so each phase's loop
calls its ML function only once. The collision requires `ML_MODE=BOTH`,
which is not the default.

Same family as **INFRA-024** (a module's specific exit code is normalised
away, so failure causes are unattributable) and **INFRA-030** (skipped
modules leave no summary line) — all three are instances of `PIPELINE
SUMMARY` under-reporting what actually happened. This branch deliberately
did not widen scope to fix it; a fix, if picked up separately, would likely
apply the same mode-suffix pattern `run_module_validation` already uses for
VALIDATION rows.

---

## Scope boundaries

- **Do not** change the meaning of any existing exit code without owner sign-off —
  `bin/` wrappers and cron lines read them. `run_locally.sh`'s own final status is
  driven by `print_summary` returning 1 whenever any module or validation failed,
  which is why `--continue-on-error` still exits non-zero even after this fix;
  that behaviour is unchanged and **not** in scope here.
- **Do not** fold in INFRA-024 or INFRA-030. They make this issue's symptoms worse
  to read but are distinct defects with their own drafts.
- **ML-016 stays its own issue** (it already has an id and history) but is a hard
  co-dependency: this issue blocks the `daily` route to ML, ML-016 blocks the
  manual route. **Ship them together** — either alone leaves an operator stuck.
- The wider `run_locally` reporting cluster is INFRA-024 (exit codes
  unattributable) and INFRA-030 (skips leave no summary line). Both make this
  issue harder to diagnose; neither is a prerequisite.
- **Do not** fold in the per-mode `machine_learning` MODULE-row collision
  (§ Known limitation above). It is pre-existing, in the same reporting
  family as INFRA-024/INFRA-030, and deliberately left unfixed here.
- This issue is **orchestration and exit semantics only**. It does not touch what
  `preprocessing_runoff` or `sync_long_horizon_hydrograph.py` read or write.

---

## Acceptance criteria

1. ~~The exact `preprocessing_runoff.py` exit site — or the uncaught exception
   type — that the operator hits is identified and recorded in this file.~~
   **Superseded**: the actual failure site is `sync_long_horizon_hydrograph.py`
   exit 4, not a `preprocessing_runoff.py` exit — recorded in § Field evidence
   and § Correction above.
2. When the `CONTINUE_ON_ERROR` guard idiom aborts any module, for any
   target, with `CONTINUE_ON_ERROR=false`, the error output names
   `--continue-on-error` and states that the run is stopping because of it —
   **implemented** via `emit_continue_on_error_hint`, gated on the
   `PIPELINE_ABORTED` flag rather than on the target's name.
3. A `sync_long_horizon_hydrograph.py` exit 4 no longer aborts the `daily`
   run — Phase 3 (`machine_learning`, `linear_regression`,
   `postprocessing_forecasts`) still runs — while the overall run still exits
   non-zero and a distinct `preprocessing_runoff (long-horizon sync)` FAIL row
   is recorded. **Implemented** in `run_maintenance_preprocessing_runoff`.
4. Exits 1, 3 and 5 from `sync_long_horizon_hydrograph.py` remain fatal to
   `daily` exactly as before this change. **Implemented** — only exit 4's
   handling changed.
5. Tests cover criteria 2-4: the continue-on-error hint's emission/suppression
   per target, and the exit-4-continues-but-1/3/5-still-abort behaviour of
   `run_maintenance_preprocessing_runoff`.
6. `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` passes with zero
   failures and zero unexpected skips. **Confirmed** — the full suite
   (16/16 modules and services) is green on this branch: zero failures, and
   no skips introduced by this branch (15 skips pre-existed; 1 more arrived
   from trunk during a rebase, gated on bash < 4). Multiple rounds of
   out-of-loop adversarial review have run (CLAUDE.md § Multi-Model Review &
   Verification).

---

## Related

| ID | Relation |
|---|---|
| INFRA-024 | Failure *causes* are unattributable; a failed module's exit code is normalised to 1 at the pipeline guard — unrelated to the fix here, which acts one level up (inside `run_maintenance_preprocessing_runoff`, before that guard is reached) |
| INFRA-030 | Skipped modules leave no summary line, so a `--continue-on-error` run's summary under-reports |
| — | Same reporting family, found this round but **not fixed here**: per-mode `machine_learning` MODULE rows still collide (§ Known limitation above) — the per-mode VALIDATION rows were fixed on this branch, the MODULE rows were not |
| ML-016 | One of the traps the operator hits when falling back to manual module invocation |
| `low_prio_gi_draft_preprocessing_runoff_dead_exit_sites.md` | The dead-code analysis of `preprocessing_runoff.py:523/536/653`, split out of this issue — same finding, not on this issue's failure path |
| — | [ML debugging runbook](../../prod/ml_no_forecasts_debug_runbook.md) — the operator-facing document this issue was found from |
