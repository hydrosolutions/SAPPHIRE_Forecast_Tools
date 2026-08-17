## Documented yearly `monthly_norms` cron dispatches to a task type Luigi does not implement (INFRA-023)

**Status**: Draft (2026-08-17)
**Module**: `bin/run_periodic_maintenance.sh`, `apps/pipeline/pipeline_docker.py`, `doc/deployment.md`
**Priority**: **High** — if a deployment followed `doc/deployment.md`, the yearly long-horizon
hydrograph aggregation has never run there. Monthly/quarter/season hydrograph rows would be stale or
absent, which is a *silent* data gap rather than a visible failure.
**Labels**: `infra`, `cron`, `luigi`, `deployment`, `documentation`
**Found**: 2026-08-17, while resolving PREPQ-014 open decision 2 (static reading only; no server
access — the deployed crontabs have **not** been inspected).
**Related**: PREPQ-014 (found it), PREPQ-009 (the norm-decouple fix whose output this job writes),
PREPQ-008.

---

## Observation

Two repository documents schedule the **01 Jan 03:00 UTC** slot differently:

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

`sync_long_horizon_hydrograph.py` writes the monthly, quarterly and seasonal hydrograph rows —
the `(norm, previous, current)` triad behind the monthly/season bulletin and any norm-relative
product. It runs **once a year**. If the yearly job never fires:

- the failure is invisible for up to 12 months;
- the symptom appears far from the cause, as missing or stale long-horizon rows;
- it plausibly resembles previously-investigated bulletin gaps (cf. PREPQ-009 and the Tajik
  empty-last-year-runoff trail), so it risks being misdiagnosed as a data problem.

The naming is itself a trap: the deprecated `sync_monthly_norms.py`
(`apps/preprocessing_runoff/sync_monthly_norms.py:1`, *"DEPRECATED (2026-06-02). Use
sync_long_horizon_hydrograph.py"*) is what `monthly_norms` presumably once referred to. The
deployment doc appears to have kept the old task name after the script was replaced.

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

## Second defect, proven while filing this: the periodic wrapper reports success unconditionally

`bin/run_periodic_maintenance.sh` has **no `set -e`**, never captures the status of its
`docker compose run` (`:82-88`), and then executes two `echo` statements (`:90-91`) before ending.
The script's exit status is therefore the last `echo`'s — **0** — regardless of whether the Luigi
task failed. The `trap cleanup EXIT` (`:43`) does not restore it.

This is **not** specific to `monthly_norms`. Every periodic task routed through this wrapper —
`long_term`, `skill_recalc`, `snow_norms` — reports success to cron whether or not it worked. The
`ValueError` from an unknown task type is simply the loudest instance of a general silent-success
defect.

Consequence for this issue: even with the crontab corrected, a failure of the yearly job would not
surface. It also means the "task submitted to Luigi daemon" message (`:90`) is the *only* signal an
operator gets, and it prints on failure too. Same family as PP-051; consider filing separately if
the fix here stays documentation-only.

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

- One documented script per cron slot, consistent between `doc/deployment.md` and `bin/README.md`.
- Every task type named in documented cron entries exists in `RunPeriodicMaintenanceWorkflow`'s
  task map, pinned by a test that asserts the two sets match — so doc/code drift of this shape
  cannot recur silently.
- A deployment-verification step that confirms the yearly long-horizon job ran (e.g. asserting
  month rows exist for the current year), since a 12-month feedback loop is too slow to catch by
  observation.

## Contract not to break

- Do not renumber or repurpose existing task types (`long_term`, `skill_recalc`, `snow_norms`) —
  they are referenced by installed crontabs on deployed servers.
- `bin/yearly_runoff_hydrograph_aggregation.sh` reads the container's true exit status via
  `docker inspect` rather than the `tee` pipeline code (`:206-213`). Preserve that if the job is
  rerouted through Luigi; the naive `$?` after a `| tee` is the pipeline's code, not the container's.
