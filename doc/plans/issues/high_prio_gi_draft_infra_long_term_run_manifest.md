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
requested forecast date **and** effective output date (they differ — see the snap-back above);
generated-at; config fingerprint; source (`scheduler` vs `manual-override`); active and skipped
modes with reasons; per-mode horizon type and horizon value; and attempted/completed/failed
outcomes per mode-model.

### Producers — one per path that can run long-term work

| Path | Producer |
|---|---|
| local operational | `query_lt_schedule` in `run_locally.sh`, after it resolves modes (`:278-323`) |
| Luigi | `RunLongTermWorkflow`, after override-or-query resolution (`:2339-2367`) — **not** `LTScheduleQuery`, which the override path bypasses |
| no active modes / skipped org | an explicit **empty** manifest, so "nothing was expected" is stated rather than inferred from absence |
| simulation | an explicit simulation manifest, or an explicit statement that simulation output is not operationally validated (`run_locally.sh:583-613` generates many dates) |

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
4. **Scope of the run id.** Does one already exist anywhere in the pipeline, or is this introducing
   the concept? If introducing it, that is a broader change than long-term alone.

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
