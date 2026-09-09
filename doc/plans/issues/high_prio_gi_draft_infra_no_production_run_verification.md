## Nothing verifies that a production forecast run produced the data it owed (INFRA-031)

**Status**: Draft (2026-09-09)
**Module**: `apps/pipeline` (`pipeline_docker.py`), `bin/` cron wrappers, `apps/validate_pipeline`
**Priority**: **High** *(proposed — see § Priority rationale. The priority is an open question this
issue deliberately does not decide.)*
**Labels**: `infra`, `validation`, `production`, `silent-success`, `observability`
**Found**: 2026-08-19, while asking whether INFRA-028's run manifest was over-engineered for its
stated consumer. Salvaged and re-verified against trunk 2026-09-09; every citation below was
re-derived, and the original framing was narrowed — see § "What INFRA-047 and ML-021 already
closed".
**Related**:
- **INFRA-020 / 021 / 022 / 026 / 027 / 028 / 045 / 050** — the `validate_pipeline` cluster. All of
  it concerns a tool that runs only from the local developer runner; this issue is the reason that
  distinction matters when reading those.
- **INFRA-047** (Complete, PR #499) and **INFRA-023** (Complete, PR #494) — **one layer down.**
  Those closed the *wrapper swallows a failed exit code* layer: a cron wrapper now propagates
  Luigi's own outcome instead of finishing on an `echo`. This issue is the layer above — the
  failures that never produce a non-zero exit code in the first place, because the run genuinely
  completed and simply wrote less than it owed.
- **P-007** (Complete) — the same layer as INFRA-047, inside Luigi: `run_docker_container` used to
  discard the container's `StatusCode`.
- **ML-021** (Review, shipped 2026-09-09, PR #503) — one module's share of *this* layer, now closed:
  a genuine ML delivery failure raises and exits 5 instead of exiting 0.
- **LR-010** — the same conflation one level down and still open: a legitimate "no data for this
  forecast year" skip and a real API write failure are indistinguishable at the module boundary.
  **Read it before designing anything here** — it is this issue in miniature, with field evidence.
- **PP-054 / LR-011** — the remaining members of the silent-success family: output *was* due, the
  write failed, and the run still reported success.
- **LTF-007** — supporting evidence below.

---

## Observation — `validate_pipeline` has no production invoker

`apps/validate_pipeline` is the repository's post-run data verification tool. **Nothing in the
tracked repository invokes it outside the local developer runner.**

Swept 2026-09-09 with `git grep -n "validate_pipeline"` over the whole worktree, then read
path-by-path. Outside `doc/plans/` and `apps/validate_pipeline/` itself, the tracked references are:

| Reference | What it is |
|---|---|
| `apps/run_locally.sh:92`, `:1479`, `:1530` | the local developer runner — **the only code that invokes the validator** |
| `apps/run_validation.sh:187`, `:213` | the pre-commit / pre-merge wrapper, which drives `run_locally.sh` |
| `apps/pipeline/tests/test_run_locally_orchestration.py:747`, `:753`, `:760`, `:771`, `:813`, `:822`, `:1554`, `:1571`, `:1581` | tests *of* the runner, not an invocation |
| `apps/run_tests.sh:173` | the test-suite module list |
| `.github/dependabot.yml:62` | a dependency-update directory, not an invocation |
| `CLAUDE.md:127`, `:489`; `.claude/skills/issue-planning/SKILL.md:137` | project description and an abbreviation table |
| `doc/configuration.md:266` | documents the `FRESHNESS_THRESHOLD_DAYS` env var |
| **`doc/dev/review_checklist_local_template.md:190`, `:1892`** | the local review checklist tells a developer to run the validator by hand — but **the commands it gives do not work.** Both invoke `bash apps/run_locally.sh validate --phase …`, and `run_locally.sh` has **no `validate` target**: its dispatch `case "$target" in` at `:2532` has no such branch. This is an **intended-but-broken invocation**, recorded as such under **INFRA-045**, which repairs these commands. It is evidence that a dev-gate consumer was *intended*, not that one works today |
| **`doc/dev/update_dev_deployment.md:58`, `:64`, `:409`** | lists `validate_pipeline` among the module venvs to `uv sync` when refreshing a **dev** deployment. It syncs the module; it does not run it |

> *(Corrected 2026-09-09. An earlier version of this section listed a "complete set" that omitted the
> two `doc/dev/` files, because the sweep that produced it filtered `doc/` out wholesale. The
> omission did not affect the finding — **neither file is a production invoker**: the checklist's
> commands do not run at all (no `validate` target exists), and the deployment doc only syncs the
> venv — but the completeness claim was false as written, and a false completeness claim is what
> stops the next person re-checking.)*

| Path | Runs the validator? | Verifies produced data? |
|---|---|---|
| Production cron → `bin/run_pentadal_forecasts.sh`, `run_decadal_forecasts.sh`, `run_long_term_forecasts.sh`, `run_daily_maintenance.sh` → Luigi (`doc/deployment.md:970`, `:973`, `:983`, `:997`) | **No** | **No** |
| `RunLongTermWorkflow` (`apps/pipeline/pipeline_docker.py:2399`, `run()` from `:2440`) | **No** | **No** — resolves active modes, yields forecast tasks, then writes a completion marker. `output()` at `:2437` only declares the `LocalTarget`; the marker is actually written at `:2479-2480` (the no-active-modes early return) and `:2526-2527` (the normal path). *(Endpoints corrected 2026-09-09.)* |
| Every other `bin/` script (46 `.sh` files, 50 entries) | **No** | no reference to `validate_pipeline` anywhere in `bin/` |
| The pipeline image | **Cannot** | it copies only `apps/iEasyHydroForecast` and `apps/pipeline` (`apps/pipeline/Dockerfile:20`, `:23-24`); `validate_pipeline` is not in it |
| GitHub workflows | **No** | `.github/workflows/deploy_production.yml` runs pytest and import checks; no workflow in `.github/workflows/` references the validator |
| Compose files (`bin/docker-compose-luigi.yml`, `bin/docker-compose-dashboards.yml`, `sapphire/docker-compose.yml`) | **No** | no reference |
| Deployment/operations docs (`doc/deployment.md`, `doc/prod/`) | **No** | **no mention at all** in either. The prose references outside `doc/plans/` are `doc/configuration.md:266` (an env var), and the two **developer** documents in `doc/dev/` listed in the reference table above — a manual review-checklist step and a dev-venv sync. None is a production invocation |
| `apps/run_locally.sh` — `run_api_validation` (`:1471`, invocation `:1479`) and `run_module_validation` (`:1504`, invocation `:1530`) | **Yes** | Yes — but this is the **local developer runner** |
| `apps/run_validation.sh` (`:187`, `:213` call `run_locally.sh`) | Indirect | Yes — but it describes itself as the **pre-commit / pre-merge** validation workflow (`:7-8`; Stage 1b is "Local pipeline run", `:36`) |

**Scope of this claim.** This covers the **tracked repository only**. It cannot speak to untracked
server crontabs, operator shell history, or anything installed by hand on a deployment. If a
deployment does invoke the validator from its own crontab, that is invisible here and would need to
be checked on the server. What the sweep does establish is that no shipped, reviewable path invokes
it in production, so no deployment gets that verification by default.

So production success is defined as **"the wrapper's Luigi submission returned zero and the task
wrote its marker file."**

## What INFRA-047 and ML-021 already closed — and what is left

The original 2026-08-19 framing of this issue was *"exit 0 plus a marker file is the whole
definition of success."* **That is no longer the whole diagnosis**, and stating it that way now
would claim shipped work as an open defect. Three layers, of which only the third is this issue:

| Layer | Failure shape | Status |
|---|---|---|
| **1. The wrapper discards a failed status** | Luigi's default retcodes are zero for `task_failed`/`missing_data`; the wrapper finished on an `echo` regardless | **Closed.** INFRA-047 (PR #499) added a `[retcode]` block to the canonical forecast wrappers (`bin/run_pentadal_forecasts.sh:78-81`, `bin/run_long_term_forecasts.sh:131-137`) so the wrapper carries Luigi's own outcome. INFRA-023 (PR #494) did the periodic/yearly half. P-007 closed the same shape inside Luigi's container runner |
| **2. A module writes nothing, or fails to write, and exits 0** | the module knows it failed and discards the knowledge at its own boundary | **Partly closed.** ML-021 (PR #503, 2026-09-09) makes a genuine ML delivery failure raise and exit 5; the PP-051 family closed **five of six** postprocessing skill-metric horizons — pentad and decad plus monthly (PR #436), quarterly and seasonal (PR #435), all merged; **daily is the sixth and is still open as PP-054** (Draft, High). **LR-010 and LR-011 are also still open**. *(Note PP-051's own tracker row still reads `Draft` even though PRs #435 and #436 are merged — a staleness in that row, not a contradiction of this one; verified with `gh pr view`.)* *(Count corrected 2026-09-09: an earlier "four of five" counted merged PRs, not horizons — PP-051's own issue file enumerates the call sites as pentad/decad, monthly, quarterly, seasonal and daily)* |
| **3. A run that correctly wrote nothing is indistinguishable from one that failed to write what was due** | nothing anywhere holds the *expectation* the output could be compared against | **Open — this issue** |

**Layer 3 stated precisely.** A container that legitimately exits 0 having correctly written
nothing, and a container that exits 0 having failed to write what was due, produce the same
observable: exit 0 and a marker file. Fixing layer 2 module by module narrows layer 3 — each module
that learns to distinguish its own two cases removes itself from it — but it does not close it,
because it says nothing about a module that never ran, a mode that was scheduled and dispatched to
models that all refused it, or output written under a date nobody compares against a schedule.

## Why the fix must not be an empty-output detector

**Writing nothing is usually correct.** Linear regression writes nothing on a non-forecast day.
Long-term writes nothing on a non-issue day. ML is not run at all on deployments that skip it
(`machine_learning` is in both `DEMO_SKIP_MODULES` and `UZHM_SKIP_MODULES`,
`apps/run_locally.sh:224-225`). An empty run is the *normal* case far more often than it is a
failure.

Any design that treats emptiness as inherently suspicious will produce exactly the false-alarm class
**INFRA-022** exists to remove from the developer validator, one layer up and with a wider blast
radius. **A check that flags every empty run is not a partial solution to this issue; it is a
different defect.**

Both halves are load-bearing, and neither is optional:

| | |
|---|---|
| **Expected empty** — nothing was due, nothing was written, all correct | must stay silent |
| **Silent failure** — output *was* due, the write failed or was never attempted, and the run reported success | must become visible |

## Supporting evidence — LTF-007 is a live instance of layer 3

The scheduler admits a long-term mode as active up to **10** days from its issue day
(`apps/long_term_forecasting/lt_schedule_query.py:52`, `ISSUE_DAY_TOLERANCE = 10`, applied at
`:103` and `:119`), while model execution refuses to run at more than **5**
(`apps/long_term_forecasting/lt_utils.py`: the guard `if abs(day_offset) > 5:` is `:202`, and it
logs `logger.info("Model %s not scheduled: %d days from issue date %s — skipping", …)` at
`:203-208` then `return None` at `:209` — a graceful skip, not an error). Verified on trunk
2026-09-09. *(Endpoints corrected: an earlier version cited `:202` for the logging and the
return, which are on the lines after the guard.)*

So a mode can be scheduled, dispatched, and produce nothing because every model declined — and
because the decline is a graceful `return None`, the run exits 0 with a marker file. This is not a
hypothetical: `doc/deployment.md:978-982` already warns operators about it in the cron template
("the run writes nothing and still exits 0 (LTF-007)"), which is a documentation workaround for
exactly the gap this issue records. Nothing detects it; the mitigation is that an operator must
schedule the cron day correctly by hand.

## Priority rationale — **open question, deliberately not decided here**

Proposed **High**, and deliberately higher than the `validate_pipeline` cluster it came from: those
issues concern a developer review gate, this one concerns whether a deployed run that failed to
produce forecasts is ever noticed.

**Owner confirmation is wanted on both the priority and the appetite.** This is a "build something
that does not exist" issue, not a defect fix, and it is larger than anything else in the cluster.
Two facts should feed that decision and pull in opposite directions:

- **Against urgency**: layers 1 and 2 have absorbed a substantial share of the original blast
  radius since this was first written. Several of the concrete failure modes that motivated it now
  do surface as non-zero exits.
- **For urgency**: the remaining layer is the one with no owner. Every closed piece was closed
  module by module; layer 3 is what is left over when that programme finishes, and nothing on the
  backlog is scheduled to reach it.

## Directions, not yet a plan

Deliberately not designed here — the point of filing is that the gap is recorded, not that a
solution is chosen. Sketches, cheapest first:

1. **Invoke the existing validator from the Luigi workflow** after the forecast tasks. Cheapest in
   concept, but it requires `validate_pipeline` in the pipeline image (see the Dockerfile row
   above), an expectation hand-off for the long-term tier (**INFRA-028**, or its alternative
   INFRA-052), and it inherits every open defect in the cluster — so those must land first.
   **Do not treat this as obviously correct.** It is the reason the `validate_pipeline` cluster
   looked operational for a whole session when it is not.
2. **A dedicated post-run check in the cron wrapper**, outside Luigi, querying the API for expected
   output. Independent of the validator's defects; duplicates some of its logic.
3. **Make each module distinguish "nothing was due" from "the write failed", and exit accordingly**,
   so exit codes become trustworthy and no separate verification layer is needed. This is the
   direction PP-051, ML-021, PP-054, LR-010 and LR-011 are already taking one module at a time, and
   **ML-021 is now a shipped worked example of the shape** — a genuine delivery failure raises and
   exits 5, a benign no-op does not.
   **This may be the strongest option, and it is the cheapest to reason about**: the module usually
   *already knows* which case it is in. The information is not missing, it is **discarded** at the
   boundary, which is precisely LR-010's complaint.
   The open question is coverage: does every module know, in every case, whether its own emptiness
   was expected? LTF-007 above is a case where the answer is **no** — the model declines, the mode
   thinks it dispatched, and no single component holds both facts. Where the answer is no, that
   module needs the schedule passed in, which is the same expectation hand-off as INFRA-028 (or
   INFRA-052) at a different boundary.

## Acceptance criteria

Deferred until a direction is chosen. The minimum this issue must eventually deliver:

- **A deployed run in which output was due and is absent is visible without a human running the
  pipeline by hand.**
- **A deployed run in which nothing was due stays silent.**
- The LTF-007 shape is covered: a mode admitted by the scheduler whose models all refused to execute
  is reported, not absorbed.

Both of the first two are load-bearing. A design that satisfies one and not the other has not
partially solved this issue.
