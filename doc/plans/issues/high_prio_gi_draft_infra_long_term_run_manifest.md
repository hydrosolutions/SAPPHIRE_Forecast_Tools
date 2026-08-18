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
requested forecast date; generated-at; config fingerprint; source; active and skipped modes with
reasons; per-mode horizon type and horizon value; and outcomes.

Three corrections from the second review pass (2026-08-18):

- **The effective output date is per mode-model, not per run.** Each model has its own
  `forecast_months` and computes its own nearest scheduled date (`lt_utils.py:188-202`), then snaps
  independently (`:211-217`); scheduler eligibility is likewise per model
  (`lt_schedule_query.py:107-124`). Some models in a mode can skip while another activates it. A
  single run-level "effective date" field would be wrong for at least one row on exactly the days
  this issue exists to describe.
- **The `source` enum needs more than `scheduler` / `manual-override`.** The local runner has a
  **legacy fallback** resolution path with its own hard-coded gate and mode set
  (`run_locally.sh:284-300`), used when the schedule query fails. A manifest that cannot say "these
  modes came from the fallback" mislabels its own provenance.
- **Producer timing cannot support outcome fields as drafted** (see below).

### Producer timing — intent or outcome, and it cannot silently be both

Both named operational producers write **after schedule resolution**, which is *before any forecast
executes*. Per-model results only exist later — accumulated in `run_forecast.py:474-526`, and for
Luigi not until yielded work completes (`pipeline_docker.py:2401-2417`). So the draft cannot both
write at resolution time and carry attempted/completed/failed. Choose explicitly:

| | Model | Consequence |
|---|---|---|
| **Intent-only** | one immutable receipt written at resolution | simplest, and still sufficient for gating: "expected, absent" stays a genuine FAIL. Does not distinguish "model crashed" from "model never ran" |
| **Two-stage** | receipt at resolution, finalized after execution, atomically | richer, but needs an explicit **incomplete/crashed** state for the process-died case, or a half-written manifest becomes a new silent-success trap |

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

**Scope: "every path" needs narrowing (2026-08-18).** The acceptance criterion below says every path
writes a manifest, but there are entry points no orchestrator owns — the module's Docker image can
be run directly (`long_term_forecasting/Dockerfile:21-34`) and the legacy local script can execute
forecasts (`bin/locally_run_forecast_tools.sh:212-233`). Decide explicitly: either those are
**out of scope** and validation is only ever run behind an orchestrator that produced a manifest, or
they are in scope and must produce one too. Leaving "every path" unqualified makes the criterion
untestable.

**Simulation must be resolved consistently with INFRA-021.** This issue currently allows declaring
simulation out of operational validation *and* requires every path to write exactly one manifest —
those conflict, and INFRA-021 separately requires simulation-date propagation with a
per-date/last-date/aggregate choice. One decision, recorded in both. See open question (9).

**Do not** add these fields to `lt_schedule_query`'s stdout JSON. That shape is consumed by three
call sites and pinned by the INFRA-022 extraction plan's characterization tests; keep the manifest a
separate artifact.

## Open questions (decide before planning)

1. **Missing manifest.** For a post-run validation invocation, absence must be an explicit
   **validation-infrastructure FAIL** — not a forecast FAIL, not a silent SKIP, and never a
   fallback to an older file. Confirm that is the wanted behavior, and what a *pre*-run or
   standalone invocation should do instead.
2. **Staleness rejection.** Which fields must match for a manifest to be accepted — run id alone,
   or run id + forecast date + organization + config fingerprint?
3. **Intent vs execution.** Does v1 record only the resolved schedule (cheaper, still leaves
   "scheduled but the model crashed" as a FAIL, which is correct), or per-model outcomes too?
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

- Every path that can run long-term work writes exactly one manifest, including the paths that run
  **nothing**.
- A manifest is matched to the validation invocation by explicit identity; a mismatched or missing
  one produces a distinct, loud infrastructure failure.
- `run_all` on a deployment where long-term is skipped (`run_locally.sh:1228-1238`) validates
  against an empty manifest and produces **no** long-term FAILs.
- Manual-override runs produce a manifest whose `source` says so, and validation gates on the
  overridden mode set — not the scheduler's.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` green, including `apps/pipeline` tests.
