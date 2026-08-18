## Long-term runs emit no record of what they were supposed to produce, so validation must guess (INFRA-028)

**Status**: Draft (2026-08-18) — **design not settled; see § Open questions**
**Module**: `apps/pipeline` (`pipeline_docker.py`), `apps/run_locally.sh`,
`apps/long_term_forecasting` (producers) → `apps/validate_pipeline` (consumer)
**Priority**: **High** — this is a hard prerequisite of the INFRA-021 + INFRA-022 atomic change;
without it, long-term validation has no trustworthy statement of what should exist.
**Labels**: `infra`, `validation`, `long-term`, `observability`, `contract`
**Found**: 2026-08-18, while planning INFRA-022 option (d). Split out of that plan's Phase 4 after
an out-of-loop review showed P4 was a placeholder, not an executable phase.
**Related**:
- **INFRA-022** — schedule-aware gating. Consumes this.
- **INFRA-021** — long-term env crash. Must ship atomically with INFRA-022, hence with this.
- **INFRA-020** — the ML false-PASS. Its constraint C3 needs the *same* class of input for a
  different module; see § Reuse.
- **LTF-007** — the 10-day vs 5-day gate mismatch this work exposed.

---

## Problem

`validate_pipeline` cannot distinguish "the long-term pipeline failed to write output" from "no
output was ever supposed to exist". It currently resolves neither, and demands month, quarter and
season output unconditionally (`validate_pipeline.py:519-583`).

The obvious fix — have validation re-derive the schedule — is what INFRA-022 option (d) set out to
do, and it only goes so far. Re-derivation cannot see:

- **manual overrides.** `RunLongTermWorkflow` skips `LTScheduleQuery` entirely when `active_modes`
  is supplied (`pipeline_docker.py:2339-2367`), so the schedule the validator computes is not the
  schedule that ran.
- **execution outcome.** The schedule states *intent*. A mode can be active and its model still
  fail; `run_forecast` records per-model failures only in an in-memory summary
  (`run_forecast.py:474-526`).
- **the date output was actually written under.** Late-but-accepted forecasts snap their stored
  issue date back to the scheduled date (`lt_utils.py:211-217`), while validation queries the exact
  forecast date.

So validation needs a **record produced by the run itself**, not a recomputation.

## What exists today, and why it is not enough

Luigi already writes the scheduler's stdout to a fixed path,
`<intermediate_data>/lt_schedule_result.json` (`pipeline_docker.py:2083-2127`). It is not usable as
a validation contract:

| Missing | Consequence |
|---|---|
| run id | cannot tell this run's record from the previous one |
| forecast date | cannot be matched against what validation is checking |
| config fingerprint / organization | a record from another deployment or config looks valid |
| completion info | records intent only |
| lifecycle | deleted only when the *next* `LTScheduleQuery` starts (`:2102-2105`), so a stale file survives indefinitely between runs |

The local runner keeps the same JSON only in shell variables (`run_locally.sh:278-323`), and
`validate_pipeline` has no argument by which to receive any of it (`:1522-1569`).

## Proposed shape (to be planned)

A **run-scoped manifest**, written to a unique path (e.g.
`<intermediate_data>/run_receipts/<run_id>/long_term.json`) by atomic create, and passed to
validation **explicitly** — by path or run id — never discovered by "latest file wins".

Fields the review identified as load-bearing: schema version; run id; target/organization;
requested forecast date; **expected output date per mode-model** (see below); generated-at; config
fingerprint; source; active and skipped modes with reasons; per-mode horizon type and horizon value;
and outcomes.

Corrections from the second and third review passes (2026-08-18):

- **The effective output date is per mode-model, and it must be a field.** Each model has its own
  `forecast_months` and computes its own nearest scheduled date (`lt_utils.py:188-202`), then snaps
  independently (`:211-217`); scheduler eligibility is likewise per model
  (`lt_schedule_query.py:107-124`). Some models in a mode can skip while another activates it. So a
  single run-level "effective date" would be wrong for at least one row on exactly the days this
  issue exists to describe.
  > **Third pass caught an overcorrection here**: an earlier edit dropped the effective-date field
  > *entirely* rather than re-scoping it per mode-model. That broke the manifest's purpose —
  > INFRA-021 and INFRA-022 both cite this manifest as the answer to snapped-back dates, and
  > validation queries **exact** dates (`validate_pipeline.py:519-573`). Without it, validation
  > cannot find legitimately late output. Restored, correctly scoped.
  >
  > **Can an intent-only manifest carry this date? Only under two conditions.** *(Corrected
  > 2026-08-18 after verification — an earlier version of this note claimed it outright.)*
  >
  > What **is** established: `check_valid_forecast_issue_date` is deterministic and selects no third
  > value — outside ±5 it returns `None`, and inside the window it returns the scheduled date when
  > late and `today` otherwise (`lt_utils.py:177-228`).
  >
  > What is **not** established, and is the gap:
  > - **The helper's return is not the persisted date.** It is passed into a polymorphic
  >   `predict_operational` call, and persistence then writes each row's `date` from the returned
  >   DataFrame with no invariant tying the two together (`run_forecast.py:336-420`;
  >   `lt_utils.py:333-352`).
  > - **`today` is chosen at forecast-process startup**, not at schedule resolution
  >   (`run_forecast.py:565-570`). Two processes, two clock reads — they can differ across midnight
  >   or a slow queue.
  >
  > So an intent-only manifest is sufficient **only if** the resolved date is frozen at resolution
  > and propagated into execution, **and** an output-date invariant is enforced at persistence.
  > Otherwise the manifest must be finalized after execution (the two-stage model). This is a
  > design decision for whoever plans INFRA-028 — do not assume the one-stage form works.
- **The `source` enum needs more than `scheduler` / `manual-override`.** The local runner has a
  **legacy fallback** resolution path with its own hard-coded gate and mode set
  (`run_locally.sh:284-300`), used when the schedule query fails. A manifest that cannot say "these
  modes came from the fallback" mislabels its own provenance.
- **Producer timing cannot support outcome fields as drafted** (see below).

### Producer timing — **DECIDED 2026-08-18: intent-only**

Both named operational producers write **after schedule resolution**, which is *before any forecast
executes*. Per-model results only exist later — accumulated in `run_forecast.py:474-526`, and for
Luigi not until yielded work completes (`pipeline_docker.py:2401-2417`). So a manifest cannot both
be written at resolution time and carry attempted/completed/failed.

| | Model | Consequence |
|---|---|---|
| **Intent-only** ← **CHOSEN** | one immutable receipt written at resolution | simplest, and sufficient for gating: "expected, absent" stays a genuine FAIL. Does **not** distinguish "model crashed" from "model never ran" |
| Two-stage | receipt at resolution, finalized after execution, atomically | richer, but needs an explicit incomplete/crashed state or a half-written manifest becomes a new silent-success trap |

**Owner decision: intent-only, and it carries three mandatory sub-tasks.** The receipt alone is not
enough — as established above, the expected output date is only trustworthy if the date is pinned
end to end. Intent-only is therefore *conditional* on all three of:

1. **Freeze** the resolved per-mode-model date at schedule resolution (do not let execution re-read
   the clock — `run_forecast.py:565-570` currently does).
2. **Propagate** that frozen date into execution rather than recomputing it.
3. **Enforce an output-date invariant** at persistence, so the stored row date provably equals the
   frozen one (`lt_utils.py:333-352` currently writes whatever the returned DataFrame carries).

If any of the three proves impractical during planning, the decision reverts to two-stage — they are
what make intent-only sound, not optional hardening. Explicitly **not** in scope as a consequence of
this decision: finalization semantics, crashed-state modelling, and per-model outcome fields.
"Scheduled but the model crashed" remains a **FAIL at validation** (output expected, absent), which
is the correct verdict without recording outcomes.

Intent-only is the smaller v1 and is enough for INFRA-022. Do not ship a two-stage design without
the crashed-state semantics — that would recreate the defect class this issue exists to close.

### Producers — one per path that can run long-term work

| Path | Producer |
|---|---|
| local operational | `query_lt_schedule` in `run_locally.sh`, after it resolves modes (`:278-323`) |
| local operational **fallback** | the legacy `is_lt_issue_window` path with its own hard-coded gate and mode set (`:284-300`), used when the schedule query fails — must be a distinct `source` value, not silently labelled `scheduler` |
| Luigi | `RunLongTermWorkflow`, after override-or-query resolution (`:2339-2367`) — **not** `LTScheduleQuery`, which the override path bypasses |
| no active modes / skipped org | an explicit **empty** manifest, so "nothing was expected" is stated rather than inferred from absence |
| simulation | see the scope decision below |

**Scope — DECIDED 2026-08-18: orchestrated paths only.** `run_locally` and Luigi produce manifests,
including explicit **empty** ones for skipped organizations and no-active-mode days. The entry
points no orchestrator owns — direct `RUN_MODE` dispatch in the module's image
(`long_term_forecasting/Dockerfile:21-34`) and the legacy local script
(`bin/locally_run_forecast_tools.sh:212-233`) — are **out of scope by decision, not by oversight**.

The rule that makes this safe, and which must be stated in the implementation: **validation only
ever runs behind an orchestrator that produced a manifest.** A validation invocation that finds no
manifest is the infrastructure FAIL of decided question 1 — it does not fall back to guessing, and
it does not silently pass because the run happened to come from an unowned entry point.

**Simulation must be resolved consistently with INFRA-021.** This issue currently allows declaring
simulation out of operational validation *and* requires every path to write exactly one manifest —
those conflict, and INFRA-021 separately requires simulation-date propagation with a
per-date/last-date/aggregate choice. One decision, recorded in both. See open question **(10)**.

**Do not** add these fields to `lt_schedule_query`'s stdout JSON. That shape is consumed by three
call sites and pinned by the INFRA-022 extraction plan's characterization tests; keep the manifest a
separate artifact.

## Open questions

### DECIDED 2026-08-18 (owner)

1. **Missing manifest → validation-infrastructure FAIL, and validation NEVER re-derives the
   schedule.** Not a forecast FAIL, not a silent SKIP, never a fallback to an older file.
   **Consequence beyond this issue**: because validation never recomputes the schedule, it never
   consumes the extracted scheduling predicates — which is what keeps the INFRA-022 extraction in
   `apps/long_term_forecasting/` and keeps its packaging work deleted. That plan's **P1 gate is
   hereby satisfied**.
   *Still to specify (mechanics, not a decision):* what a **pre-run or standalone** invocation does,
   since there is no manifest to expect yet. Suggest: a distinct "no run to validate" outcome, not
   reusing the post-run FAIL.
3. **Intent vs execution → intent-only**, conditional on the three sub-tasks in § "Producer timing".
   Per-model outcome fields are out of scope; "scheduled but crashed" stays a validation FAIL.

*(Question 2 and 4-10 remain open; numbering preserved so cross-references stay valid.)*

### Still open

2. **Staleness rejection.** Which fields must match for a manifest to be accepted — run id alone,
   or run id + forecast date + organization + config fingerprint?
4. **Scope of the run id.** **Answered 2026-08-18**: no reusable run id exists in this pipeline —
   `run_locally.sh:94-97` has only a second-resolution log timestamp, and `RunLongTermWorkflow` has
   no run-id parameter (`pipeline_docker.py:2316-2319`, `:2344-2345`). But the concept **does**
   exist in the repo: `forecast_skill_eval` has a caller/default `run_id` plus a run-scoped artifact
   directory (`cli.py:67-79`, `artifacts.py:18-34`). Borrow that shape rather than inventing one.
   The remaining question is who *owns* it — see (5).
5. **Identity ownership and propagation.** Who mints the run id and passes it along the chain? The
   deployed launcher only submits Luigi and passes no identity and no validation command
   (`bin/run_long_term_forecasts.sh:104-124`), and `validate_pipeline` has no argument to receive
   one. Without an owner, every producer invents its own and nothing matches.
6. **Finalization after partial process death.** If a run is killed between resolution and
   completion, what state is the manifest in and how does validation read it? (Only relevant under
   the two-stage model above.)
7. **Retention and cleanup.** Run-scoped paths accumulate. Who deletes them, and after how long?
   Note the current fixed file is removed only when the next schedule query starts
   (`pipeline_docker.py:2101-2105`) — that is the anti-pattern to avoid, not a precedent.
8. **Fingerprint inputs, and when it is taken.** Which files and env vars compose the config
   fingerprint — and is it computed **before or after** `ForecastConfig` rewrites every model's
   `general_config.json` during load (`config_forecast.py:157-180`)? Fingerprinting after the
   rewrite makes the value depend on whether a run happened first.
9. **Which artifact owns the horizon value?** This manifest proposes carrying per-mode horizon type
   and value, while `validate_pipeline` resolves them today from `long_term_horizon_resolver`
   (`:539`, `:562`). Both cannot be authoritative — if config changes between execution and
   validation, they diverge. The manifest's copy is the *as-run* value and is probably the right
   answer, but it must be stated, not left to whichever code path runs first.
10. **Manifest cardinality for multi-date simulation.** `simulate_forecasts.py:160-176` iterates
   many year/month pairs; one manifest, or one per generated date? INFRA-021 records the same
   unresolved question for validation dates — **decide both together, once.**

## Reuse

INFRA-020's constraint C3 needs the same class of input for `machine_learning`: it cannot tell
"ML failed" from "ML was never scheduled" (`ML_MODE` defaults, org skips). **Do not fold INFRA-020
into this issue** — but design the manifest so a second module can be added to it rather than
inventing a parallel mechanism. A `modules` map keyed by module name would do it.

## Acceptance criteria

- Every path **within the agreed scope** (see § "Scope: 'every path' needs narrowing") writes
  exactly one manifest, including the paths that run **nothing**. *Not testable until that scope
  decision and the simulation decision (open question 10) are recorded — an unqualified "every
  path" contradicts this issue's own allowance for excluding simulation and unowned entry points.*
- The manifest carries an **expected output date per mode-model**, so validation can locate output
  that was legitimately stored under a snapped-back date rather than the requested one.
- A manifest is matched to the validation invocation by explicit identity; a mismatched or missing
  one produces a distinct, loud infrastructure failure.
- `run_all` on a deployment where long-term is skipped (`run_locally.sh:1228-1238`) validates
  against an empty manifest and produces **no** long-term FAILs.
- Manual-override runs produce a manifest whose `source` says so, and validation gates on the
  overridden mode set — not the scheduler's.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` green, including `apps/pipeline` tests.
