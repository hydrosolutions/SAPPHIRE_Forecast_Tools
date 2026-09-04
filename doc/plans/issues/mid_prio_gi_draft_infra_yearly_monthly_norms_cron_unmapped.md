## The `monthly_norms` retirement was left incomplete — the wrapper still accepts it, silently (INFRA-023)

**Status**: Draft — **REFRAMED 2026-08-21 after out-of-loop review.** The original framing ("a
cron points at a task Luigi does not implement") is accurate but reads as an oversight. It is not:
`monthly_norms` was **deliberately retired** in Phase 4 of the runoff work, and two tests pin the
removal. The real defect is that the deprecation was never finished — see § The actual fix.
**Module**: `bin/run_periodic_maintenance.sh`, `apps/pipeline/pipeline_docker.py`, `doc/deployment.md`
**Priority**: **Medium** — *downgraded from High 2026-08-21; the original premise was disproved.*
The claim was that a deployment following `doc/deployment.md` has **never** run the yearly
aggregation. That overstates it: the current production checklists already schedule the correct
wrapper, already tell operators to **remove** the retired entry, and already run the writer during
an update (`doc/prod/update_deployment_checklist.md:829`, `:832`, `:979`;
`doc/prod/first_deploy_checklist.md:646`, `:850`). Any deployment following the *current* checklist
is remediated.

**What justifies keeping it open at Medium** is not the documentation — it is that
`run_periodic_maintenance.sh` **still silently accepts a retired task name**, so any server whose
crontab predates those checklist updates fails once a year and reports success. That is a live
defect in code we own, and it is fixable without inspecting a single crontab.
**Labels**: `infra`, `cron`, `luigi`, `deployment`, `documentation`
**Found**: 2026-08-17, while resolving PREPQ-014 open decision 2 (static reading only; no server
access — the deployed crontabs have **not** been inspected).
**Related**: PREPQ-014 (found it), PREPQ-009 (the norm-decouple fix whose output this job writes),
PREPQ-008.

---

## Observation

> **Update 2026-08-26 (DOC-007):** the documentation half of this issue is fixed — no live
> document prescribes `monthly_norms` any more. **INFRA-023 stays open** for the code defect:
> `run_periodic_maintenance.sh` still *accepts* the retired name and exits 0 without running
> anything, so a crontab installed before the doc fix is still silently broken. The inventory
> below is retained as the historical record of what the documents said.

Two repository documents scheduled the **01 Jan 03:00 UTC** slot differently (historical, fixed by DOC-007):

| Source | Script named for that slot |
|---|---|
| `doc/deployment.md:922` | `bin/run_periodic_maintenance.sh monthly_norms` |
| `bin/README.md:231` | `bin/yearly_runoff_hydrograph_aggregation.sh` |

Only the second actually invokes the long-horizon writer
(`bin/yearly_runoff_hydrograph_aggregation.sh:184` → `sync_long_horizon_hydrograph.py`).

The first cannot work. `run_periodic_maintenance.sh` passes its argument through as
`MAINTENANCE_TASK_TYPE` to Luigi's `RunPeriodicMaintenanceWorkflow`, whose task map is:

```python
task_map = {
    "long_term": LongTermPostProcessingMaintenance(),
    "skill_recalc": YearlySkillRecalculation(),
    "snow_norms": YearlySnowNormRecalculation(),
}
if self.task_type not in task_map:
    raise ValueError(...)
```
— `apps/pipeline/pipeline_docker.py:2049-2057`

`monthly_norms` is **not** a key. The documented cron entry therefore raises `ValueError`.

## Why this matters

`sync_long_horizon_hydrograph.py` writes the monthly, quarterly and seasonal hydrograph rows — the
`(norm, previous, current)` triad behind the monthly/season bulletin. It runs **once a year**, so a
failure is invisible for up to 12 months and surfaces far from its cause, as missing or stale
long-horizon rows.

The naming is itself a trap: the deprecated `sync_monthly_norms.py`
(*"DEPRECATED (2026-06-02). Use sync_long_horizon_hydrograph.py"*) is what `monthly_norms` once
referred to. The deployment doc kept the old task name after the script was replaced.

## The actual fix — validate the task type in the wrapper

**This is the change that matters, and it is about four lines.** The wrapper already *knows* the
valid task types — it prints them — but only checks for an empty argument
(`bin/run_periodic_maintenance.sh:22-28`):

```bash
TASK_TYPE="${1}"
if [ -z "$TASK_TYPE" ]; then
    echo "| Error: task_type argument required."
    echo "| Valid task_types: long_term, skill_recalc, snow_norms, lt_recovery"  # <-- list exists here
    exit 1
fi
echo "| Running Periodic Maintenance: ${TASK_TYPE}"                   # <-- anything else passes
```

So `monthly_norms` passes through, prints *"Running Periodic Maintenance: monthly_norms"*, and dies
three layers down where nothing reports it.

**Validate `$TASK_TYPE` against that same list — all four types — and exit non-zero on anything
else.** `lt_recovery` additionally takes two positional arguments and already validates them
(`:60-74`); do not disturb that. Give *retired*
names a specific message naming the replacement, e.g.
`monthly_norms was retired; use bin/yearly_runoff_hydrograph_aggregation.sh`.

Why this beats chasing documentation:

- it protects **every** server regardless of what is in its crontab, **without inspecting any of
  them** — which matters because this issue was filed without server access and still has none;
- it is **independent of P-007 and of the exit-status defect below**, so it needs no sequencing;
- it converts a silent three-layer failure into an immediate, self-explaining one;
- a deprecation owes the operator a pointer to the replacement, and this is where that belongs.

## The intended path is already decided — do NOT "fix" this by restoring the Luigi task

**This was missed in the first draft of this issue and is the most important thing here.** The
absence of `monthly_norms` from the task map is **deliberate and test-pinned**, not an oversight.

`apps/preprocessing_runoff/test/test_yearly_monthly_norms_retired.py` asserts that both the class
and the dispatcher key stay gone:

```python
def test_monthly_norms_dispatcher_key_is_gone():
    assert '"monthly_norms"' not in content, (
        "Old runoff monthly_norms dispatcher key is still present. "
        "This was retired in Phase 4 of the runoff long-horizon hydrograph plan."
    )
```

with a sibling asserting `YearlyMonthlyNormsRecalculation` is absent. The owner decision is recorded
in `doc/plans/issues/review_gi_draft_runoff_long_horizon_hydrograph.md`.

> **Contract correction (2026-09-04).** Older narrative in this issue was written when there were
> **three** task types; trunk supports **four** and the snippet above has been updated to match. `run_periodic_maintenance.sh:51` already advertises
> `long_term, skill_recalc, snow_norms, lt_recovery`, and `lt_recovery` takes **two extra
> positional arguments** (mode, ISO issue date) with its own validation and `exit 1` paths
> (`:60-74`). It is also the one branch that already propagates its status
> (`COMPOSE_STATUS` at `:169`, `exit` at `:192`). **An implementer must validate against all four
> and preserve `lt_recovery`'s argument handling** — a three-type allow-list would reject a working
> recovery path. Two consequences for this issue: the documentation half is done (DOC-007), so the
> remaining work is **code** — the wrapper still accepts a retired name because it only checks for
> an *empty* argument; and the historical three-type descriptions below are kept as the record of
> what was true at filing, not as the contract to build against.

**So the documentation half is fixed (DOC-007); what remains is the code.** Adding a `monthly_norms` task type
would reintroduce intentionally retired code and break a locked regression test. Only reverse this
if the owner explicitly reverses the Phase 4 decision.

## Second defect: the periodic wrapper reports success unconditionally

> **Layer note, corrected 2026-09-04.** The 2026-08-21 version of this note said P-007 masks
> ordinary task failures before this wrapper sees them. **P-007 is fixed** —
> `pipeline_docker.py:331-337` now reads `StatusCode` with an explicit `type(raw) is int` guard — so
> that layer is closed and this note no longer gates anything.
>
> **The layer that does still gate it is Luigi's own return codes.** `run_periodic_maintenance.sh`
> writes a `[retcode]` block setting `task_failed = 1` **only when `TASK_TYPE = lt_recovery`**
> (`:138-152`), and only that branch passes `LUIGI_CONFIG_PATH`. Luigi's defaults are 0 for
> `task_failed`, `missing_data`, `already_running`, `scheduling_error` and `not_run`. So for
> `long_term`, `skill_recalc` and `snow_norms` a failed task still makes Luigi exit **0**, and the
> script's own comment says as much: *"the other task types keep Luigi's defaults so their behaviour
> is unchanged."*


`bin/run_periodic_maintenance.sh` has **no `set -e`**. It *does* capture its `docker compose run`
status into `COMPOSE_STATUS` (`:169`) — an earlier version of this issue said it never captures it,
which is now wrong — but it **`exit`s that status only on the `lt_recovery` branch** (`:174-192`).
For `long_term`, `skill_recalc` and `snow_norms` it falls through to two `echo`s, so the script's
exit status is the last `echo`'s — **0** — regardless of whether the Luigi task failed. And because
Luigi's `[retcode]` block is written only for `lt_recovery` (`:138-152`), `COMPOSE_STATUS` would be
**0 anyway** for those three. Both layers, not one. The `trap cleanup EXIT` (`:43`) does not restore it.

This is **not** specific to `monthly_norms`. Every periodic task routed through this wrapper —
`long_term`, `skill_recalc`, `snow_norms` — reports success to cron whether or not it worked. The
`ValueError` from an unknown task type is simply the loudest instance of a general silent-success
defect.

Consequence for this issue — **note the asymmetry, it decides the fix**:

- If the crontab is corrected to the `bin/README.md` form (the direct
  `yearly_runoff_hydrograph_aggregation.sh` wrapper, which is the intended path per the Phase 4
  decision), **the wrapper defect does not apply** — that wrapper inspects the container's status
  and exits non-zero (`:190-211`). Correcting the docs therefore fixes both problems at once for
  this job.
- The wrapper defect still stands for **every other** periodic task, which does route through
  `run_periodic_maintenance.sh`: `long_term`, `skill_recalc`, `snow_norms`. Those report success to
  cron regardless of outcome, and that is not fixed by anything in this issue.

### The same defect covers the scheduled cron surface (re-derived 2026-09-04 after review)

Found while filing PREPG-024. The shape is not confined to `run_periodic_maintenance.sh`.

> **The first version of this table was wrong in both directions — read the method note below
> before trusting any re-derivation.**

**Do not propagate — the script's exit status is its last `echo`, so cron sees 0:**

| Script | Note |
|---|---|
| `bin/run_preprocessing_gateway.sh` | **The canonical gateway cron entry** (`update_deployment_checklist.md:802-803`, `0 3 * * *`). Ends on two `echo`s after `docker compose run`; never captures its status. |
| `bin/daily_gateway_maintenance.sh` (`:120-141`) | Legacy/manual path, **not** what cron runs for the gateway. |
| `bin/daily_linreg_maintenance.sh` | See the two-mode hazard below. |
| `bin/daily_postprc_maintenance.sh` | |
| `bin/daily_preprunoff_maintenance.sh` | |
| `bin/bimonthly_long_term_postprocessing.sh` | `run_container()` does `return $CONTAINER_EXIT_CODE` (`:147`), but **both callers ignore it** (`:152`, `:159`) and the script ends on `echo`. |

**Do propagate:** `yearly_runoff_hydrograph_aggregation.sh`, `yearly_skill_metrics_recalculation.sh`
(fixed under migration P6), `yearly_snow_norm_recalculation.sh` (fixed under PREPG-020), and
`initialize_site_backfill.sh` — the last via an `overall_exit` aggregate returned from `main`, which
is **the pattern the fixes below should copy**.

`run_periodic_maintenance.sh` now *does* capture `COMPOSE_STATUS` (`:169`) and `exit`s it (`:192`),
but only on the `lt_recovery` branch; the other task types still fall through.

**Re-derive this table by reading each script's final statement, not by grep.** Two greps produced
two wrong tables: filtering on `CONTAINER_EXIT_CODE` silently excludes `run_preprocessing_gateway.sh`,
which never captures a status at all, and matching `return "$VAR"` false-positives on a
function-local return whose callers discard it.

### The fix is one line only for the single-container wrappers

- **Single-container wrappers** (`daily_gateway_maintenance.sh`, `daily_postprc_maintenance.sh`,
  `daily_preprunoff_maintenance.sh`): `exit "$CONTAINER_EXIT_CODE"` at the end.
- **`daily_linreg_maintenance.sh` needs an aggregate, not one line.** It loops
  `for MODE in PENTAD DECAD` (`:105`) and re-assigns `CONTAINER_EXIT_CODE` **inside** the loop
  (`:147`), so a trailing `exit "$CONTAINER_EXIT_CODE"` reports only DECAD — **a failed PENTAD
  followed by a successful DECAD would still exit 0**, i.e. the fix would leave the defect in place
  for half the runs. Keep one failure flag across both modes, as `initialize_site_backfill.sh` does.
- **`bimonthly_long_term_postprocessing.sh`**: have both callers check `run_container`'s return and
  aggregate it.
- **`run_preprocessing_gateway.sh`** is a different shape — it submits to Luigi via
  `docker compose run` and its Luigi CLI retains the default `task_failed=0`. Capturing the compose
  status is necessary but **not sufficient**; Luigi must also be configured to return non-zero on
  task failure. Treat it as its own work item, not part of the one-line sweep.
- **Capture `${PIPESTATUS[0]}` immediately after the `| tee`**, as
  `yearly_snow_norm_recalculation.sh:135-144` already does. All the unfixed wrappers take `$?` after
  a pipe, which is *tee's* status; the `docker inspect` fallback masks it only while inspect works.

**Two shapes of wrapper, two different fixes — do not apply one recipe to both.**

- **Direct `docker run` wrappers** (the four `daily_*`, `bimonthly_long_term_postprocessing.sh`):
  they read the status with `docker inspect` and never reach
  `pipeline_docker.run_docker_container`, so propagating the captured status is the whole fix.
- **Luigi-backed wrappers** (`run_periodic_maintenance.sh` for its three non-recovery types, and
  `run_preprocessing_gateway.sh`): propagating the compose status is **necessary but not
  sufficient** — Luigi returns 0 on task failure unless a `[retcode]` block says otherwise, so the
  status being propagated would be 0. Both layers must land together or the change is a no-op.
  `run_periodic_maintenance.sh:138-152` already shows the working pattern, applied to `lt_recovery`
  only; extend it rather than inventing one.

**Verification these fixes actually work** (none of this issue's other criteria test them): one
parameterised shell test with a stubbed `docker`, asserting non-zero propagation for each wrapper
listed above, **plus the linreg fail-then-succeed case**, plus the inspect-failure fallback. A
crontab survey is not a substitute.

**Related**: **PREPG-024** records the gateway instance in its per-path signal table and scopes the
fix out of that issue — this is where the class belongs.

An earlier draft of this section claimed a corrected crontab would still hide failures. That was
wrong and contradicted this issue's own recommended fix.

The `"task submitted to Luigi daemon"` message (`:89`) also prints on failure. It is not the *only*
operator signal — Compose and Luigi output reach the cron log too — but it is the only *summary*
line, and it is unconditionally positive. Same family as PP-051; the wrapper defect likely warrants
its own issue, since its scope is all periodic tasks rather than this stale command.

## What to inspect

1. Which form is actually installed in each deployment's crontab (`crontab -l` on kghm, tjhm, uzhm).
   **This is the load-bearing check and requires server access — it has not been done.** If the
   deployed crontabs use the `bin/README.md` form, the scheduling impact is nil and this is
   docs-only (the wrapper defect above still stands).
2. ~~Whether `run_periodic_maintenance.sh` should gain a `monthly_norms` task type~~ — **answered:
   no.** See the section above; it is test-pinned as retired.
3. ~~Whether the `ValueError` surfaces as a non-zero exit~~ — **answered: it does not.** Proven by
   static shell reading; see the second defect above.
4. Every place the invalid `monthly_norms` command is still documented or advertised. Known so far:
   - `doc/deployment.md:922` (the cron entry)
   - `doc/plans/deployment_new_hydromet_aws.md` (same invalid command)
   - `bin/docker-compose-luigi.yml:118` (still advertises `monthly_norms` as supported)
   This inventory is **not** known to be complete — grep before fixing.
5. Whether any other documented `run_periodic_maintenance.sh` task type is likewise unmapped.

## Acceptance criteria

- **`run_periodic_maintenance.sh` exits non-zero on an unknown task type**, pinned by a test that
  runs the wrapper with `monthly_norms` and asserts a non-zero exit **and** that the message names
  the replacement script. *Without the exit-code assertion the test passes on today's code.*
- **The printed list and the validated list agree.** They went out of sync once already — that is
  why this issue exists — so a test must fail if they diverge. How they are kept in sync is the
  implementer's call.
- **All four live types still work** — `long_term`, `skill_recalc`, `snow_norms` and `lt_recovery`
  (the last with its two extra arguments preserved) each still reach
  Luigi. A validation that rejects a working task is worse than the bug.
- **Do not "fix" the two production checklists — they are already correct**
  (`update_deployment_checklist.md:829`/`:832`/`:837`, `first_deploy_checklist.md:646`/`:850`).
  The documentation sweep itself is done (DOC-007); it is not a criterion for this fix.
- **This issue has two halves and both must land**: (a) task-type validation in
  `run_periodic_maintenance.sh`, and (b) exit propagation across the six swallowing wrappers,
  including the Luigi `[retcode]` layer where it applies. Delivering only (a) leaves every wrapper
  failure invisible. Do **not** pull unrelated schedule drift (e.g. the snow date) in.

## Contract not to break

- **Do NOT restore `monthly_norms` to the task map.** Its removal is deliberate and pinned by two
  tests — `test_yearly_monthly_norms_task_class_is_gone` and
  `test_monthly_norms_dispatcher_key_is_gone`
  (`apps/preprocessing_runoff/test/test_yearly_monthly_norms_retired.py`), retired in Phase 4 of the
  runoff work. Restoring it breaks both and revives the deprecated norm-only path.
- Do not remove, rename or repurpose the live task types (`long_term`, `skill_recalc`,
  `snow_norms`, `lt_recovery` — and do not drop `lt_recovery`'s two extra positional arguments).
  `lt_recovery` is operator-invoked, not a scheduled cron entry; the other three are scheduled. They are referenced by installed crontabs, which have **not** been inspected — treat
  them as live until they have.
- `bin/yearly_runoff_hydrograph_aggregation.sh` reads the container's true exit status via
  `docker inspect` (`:206-213`, exits with it at `:232`) rather than the `tee` pipeline code.
  Preserve that. **But do not copy the mechanism literally into the periodic wrapper** — that one
  uses `docker compose run --rm`, so the container is gone before it could be inspected; capture
  Compose's status immediately instead.
