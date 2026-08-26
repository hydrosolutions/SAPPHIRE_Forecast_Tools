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
    echo "| Valid task_types: long_term, skill_recalc, snow_norms"   # <-- the list exists here
    exit 1
fi
echo "| Running Periodic Maintenance: ${TASK_TYPE}"                   # <-- anything else passes
```

So `monthly_norms` passes through, prints *"Running Periodic Maintenance: monthly_norms"*, and dies
three layers down where nothing reports it.

**Validate `$TASK_TYPE` against that same list and exit non-zero on anything else.** Give *retired*
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

**So the fix is to correct the documentation, not the code.** Adding a `monthly_norms` task type
would reintroduce intentionally retired code and break a locked regression test. Only reverse this
if the owner explicitly reverses the Phase 4 decision.

## Second defect: the periodic wrapper reports success unconditionally

> **Layer note added 2026-08-21.** This is one of *three* silences, and fixing it alone does not
> make ordinary task failures visible. **P-007** (`apps/pipeline/pipeline_docker.py:329-347`)
> discards `container.wait()`'s `StatusCode` and hard-codes `exit_status = 0`, so a mapped task
> whose container fails is already reported as success **before** this wrapper sees it.
>
> The exception is precisely the case this issue is about: an **unknown** task type raises
> `ValueError` in `requires()` **before any container starts**, so P-007 does not mask it. Fixing
> the wrapper alone therefore *would* surface a stale `monthly_norms` cron — but would still hide a
> failing `long_term`, `skill_recalc` or `snow_norms` run. Both must land for that.


`bin/run_periodic_maintenance.sh` has **no `set -e`**, never captures the status of its
`docker compose run` (`:82-88`), and then executes two `echo` statements (`:89-90`) before ending.
The script's exit status is therefore the last `echo`'s — **0** — regardless of whether the Luigi
task failed. The `trap cleanup EXIT` (`:43`) does not restore it.

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
- **The valid-type list is not duplicated.** The message at `:26` and the new validation must read
  from one source, or they will drift apart and the error will lie.
- **The three live types still work** — `long_term`, `skill_recalc`, `snow_norms` each still reach
  Luigi. A validation that rejects a working task is worse than the bug.
- **Stale references are gone** from `doc/deployment.md`, the old AWS plan, the Compose comment and
  `apps/pipeline/README`. **The two production checklists are already correct — do not "fix" them**
  (`doc/prod/update_deployment_checklist.md:829`/`:832`/`:837`,
  `doc/prod/first_deploy_checklist.md:646`/`:850`); `:837` already documents this defect by name.
- Scoped to the **03:00 1 January runoff slot** and to arguments passed to
  `run_periodic_maintenance.sh`. Do **not** pull unrelated schedule drift (e.g. the snow date) in.
- A deployment-verification step confirming the yearly long-horizon job ran — and note that "month
  rows exist for the current year" is **insufficient**: the writer preserves partial writes and
  continues past station failures (`sync_long_horizon_hydrograph.py:602`), and the deprecated path
  could create month-only rows. Verify expected **station coverage across `month`, `quarter` and
  `season`**, plus the wrapper's exit status.

## Contract not to break

- **Do NOT restore `monthly_norms` to the task map.** Its removal is deliberate and pinned by two
  tests — `test_yearly_monthly_norms_task_class_is_gone` and
  `test_monthly_norms_dispatcher_key_is_gone`
  (`apps/preprocessing_runoff/test/test_yearly_monthly_norms_retired.py`), retired in Phase 4 of the
  runoff work. Restoring it breaks both and revives the deprecated norm-only path.
- Do not remove, rename or repurpose the live task types (`long_term`, `skill_recalc`,
  `snow_norms`). They are referenced by installed crontabs, which have **not** been inspected — treat
  them as live until they have.
- `bin/yearly_runoff_hydrograph_aggregation.sh` reads the container's true exit status via
  `docker inspect` (`:206-213`, exits with it at `:232`) rather than the `tee` pipeline code.
  Preserve that. **But do not copy the mechanism literally into the periodic wrapper** — that one
  uses `docker compose run --rm`, so the container is gone before it could be inspected; capture
  Compose's status immediately instead.
