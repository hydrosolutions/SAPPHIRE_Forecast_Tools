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
| `doc/deployment.md` *(pre-DOC-007; deliberately no line number — that line now carries the corrected command)* | `bin/run_periodic_maintenance.sh monthly_norms` |
| `bin/README.md:231` | `bin/yearly_runoff_hydrograph_aggregation.sh` |

Only the second actually invokes the long-horizon writer
(`bin/yearly_runoff_hydrograph_aggregation.sh:193` → `sync_long_horizon_hydrograph.py`).

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
— `apps/pipeline/pipeline_docker.py:2077-2086`

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
(`bin/run_periodic_maintenance.sh:46-54`):

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
three layers down. Luigi's and Compose's output does reach the cron log — what is missing is a
non-zero process status and an accurate wrapper summary, not all output.

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
For `long_term`, `skill_recalc` and `snow_norms` it falls through, and the script's exit status is
**0** regardless of whether the Luigi task failed — via the untaken `if` at `:177-193`, whose status
is 0. (An earlier revision blamed the trailing `echo`s; the outcome is the same, the mechanism was
wrong.) And because Luigi's `[retcode]` block is written only for `lt_recovery` (`:138-152`),
`COMPOSE_STATUS` would be **0 anyway** for those three. Both layers, not one. The
`trap cleanup EXIT` (`:88`) does not restore it.

This is **not** specific to `monthly_norms`. The three *scheduled* types routed through this
wrapper — `long_term`, `skill_recalc`, `snow_norms` — report success to cron whether or not they
worked. `lt_recovery` is the exception: it already propagates `COMPOSE_STATUS` and prints an
explicit success/NOT-CONFIRMED summary, so "every periodic task" would be wrong. The
`ValueError` from an unknown task type is simply the loudest instance of a general silent-success
defect.

Consequence for this issue — **note the asymmetry, it decides the fix**:

- If the crontab is corrected to the `bin/README.md` form (the direct
  `yearly_runoff_hydrograph_aggregation.sh` wrapper, which is the intended path per the Phase 4
  decision), **the wrapper defect does not apply** — that wrapper inspects the container's status
  and exits non-zero (`docker inspect` at `:220`, `exit` at `:241`; but see the tee caveat above). Correcting the docs therefore fixes both problems at once for
  this job.
- The wrapper defect still stands for the three *scheduled* periodic types routed through
  `run_periodic_maintenance.sh`: `long_term`, `skill_recalc`, `snow_norms`. Those report success to
  cron regardless of outcome. **That is fixed by this issue's PR2, not by the validation half** —
  an earlier revision said it was out of scope entirely and contradicted the acceptance criteria.

### The wrappers in scope (re-derived 2026-09-05; two reviewers)

> **This is NOT "the scheduled cron surface" — an earlier revision claimed that and was wrong.**
> Four *canonical* cron wrappers have the identical defect and are deliberately **out of scope**
> here; see below. Do not let the table imply the class is closed.

**In scope — seven scripts.** Each ends without propagating its container/compose status, so cron
sees success on failure:

| Script | Schedule | Shape |
|---|---|---|
| `bin/run_periodic_maintenance.sh` | 3 periodic types | Luigi-backed |
| `bin/run_preprocessing_gateway.sh` | 03:00 daily (canonical) | Luigi-backed |
| `bin/daily_gateway_maintenance.sh` | legacy/manual | direct `docker run` |
| `bin/daily_linreg_maintenance.sh` | legacy/manual | direct, **two modes** |
| `bin/daily_postprc_maintenance.sh` | legacy/manual | direct `docker run` |
| `bin/daily_preprunoff_maintenance.sh` | legacy/manual | direct `docker run` |
| `bin/bimonthly_long_term_postprocessing.sh` | bimonthly | direct, **two calls** |

**`yearly_runoff_hydrograph_aggregation.sh` — the Jan-1 replacement — is NOT reliably safe, and
this issue previously said it was.** It does `exit "$CONTAINER_EXIT_CODE"` at `:241`, but that value
comes from `EXIT_CODE=$?` captured after a `| tee` pipeline (`:214-216`) — i.e. *tee's* status — with
`docker inspect ... || echo "$EXIT_CODE"` as the fallback (`:220`). If Docker fails before leaving an
inspectable container, tee returns 0, inspect fails, the fallback yields 0, and the wrapper exits 0.
That is the same fallback defect this issue fixes in the four `daily_*` wrappers, and PREPG-020
already carried it forward for this exact script. **Consequence for the first half of this issue:**
telling operators to switch the Jan-1 slot to this wrapper does not by itself guarantee a visible
failure. Fix it here with the same `${PIPESTATUS[0]}` treatment and pin the inspect-failure case,
following `yearly_snow_norm_recalculation.sh:135-144`.

**Already propagate, leave alone:**
`yearly_skill_metrics_recalculation.sh` (`:128`, fixed under migration P6),
`yearly_snow_norm_recalculation.sh` (`:165`, fixed under PREPG-020), and
`initialize_site_backfill.sh`, whose `main` does `exit "$overall_exit"` (`:608`) — **that sticky
aggregate is the pattern to copy** for the two multi-call wrappers above.

**OUT OF SCOPE — tracked as INFRA-047
([`mid_prio_gi_draft_infra_canonical_cron_wrappers_exit_zero.md`](mid_prio_gi_draft_infra_canonical_cron_wrappers_exit_zero.md)), not an oversight.** These canonical cron wrappers have the same
defect and will still report success after this issue lands:
`run_pentadal_forecasts.sh` (04:00), `run_decadal_forecasts.sh` (05:00),
`run_long_term_forecasts.sh` (06:00), `run_daily_maintenance.sh` (19:00), and
`run_preprocessing_runoff.sh`. **`run_daily_maintenance.sh` carries a hazard that makes a naive fix
actively harmful: it runs the frontend updater AFTER Luigi, so an immediate-exit fix would suppress
it.** Any follow-up must use a sticky aggregate there, not an early exit.

**Re-derive this table by reading each script's final statement, never by grep.** Two greps produced
two wrong tables: filtering on `CONTAINER_EXIT_CODE` silently excludes `run_preprocessing_gateway.sh`
(which never captures a status at all), and matching `return "$VAR"` false-positives on a
function-local return whose callers discard it.

### Two shapes, two different fixes

- **Direct `docker run` wrappers** (`daily_*`, bimonthly): **two small edits, not one line.**
  Capture `${PIPESTATUS[0]}` immediately after the `| tee` — `$?` there is *tee's* status, and the
  `docker inspect` fallback only masks that while inspect works — and add the final
  `exit "$CONTAINER_EXIT_CODE"`. Adding the exit alone still reports success when inspect fails.
  Only these four `daily_*` wrappers use the `$?`-after-pipe pattern; the gateway wrapper has no
  pipe and the bimonthly one inspects directly.
- **Luigi-backed wrappers** (`run_periodic_maintenance.sh` for its three scheduled types, and
  `run_preprocessing_gateway.sh`): propagation is necessary but not sufficient. Luigi defaults
  `task_failed`, `missing_data`, `already_running`, `scheduling_error` and `not_run` to **0**
  (`unhandled_exception` defaults to 4), so without a `[retcode]` block the status propagated is
  itself 0. `run_periodic_maintenance.sh:139-152` already shows the working pattern, applied to
  `lt_recovery` only — extend it rather than inventing one, and pass `LUIGI_CONFIG_PATH` the same
  way. Note propagation alone is **not** wholly a no-op: it still surfaces compose-level failures
  and `unhandled_exception=4`. It is a no-op specifically for the zero-default categories.
- **Multi-call wrappers need a sticky aggregate, not a trailing exit.**
  `daily_linreg_maintenance.sh` loops PENTAD then DECAD reassigning the status inside the loop
  (`:105`, `:144-147`), so a trailing `exit` reports only DECAD — a failed PENTAD followed by a
  successful DECAD would still exit 0. `bimonthly_long_term_postprocessing.sh` has the same shape
  across its two `run_container` calls (`:152`, `:159`), whose return values both callers discard.
  Use a flag that is never reset, as `initialize_site_backfill.sh` does. Do **not** use `set -e`.

### Existing tests pin the CURRENT behaviour and must be inverted

Four tests assert that retcodes/config stay recovery-only and that the three scheduled periodic
types swallow their status: `apps/pipeline/tests/test_lt_dated_recovery.py:431`, `:471`, `:562`,
`:578`. An implementer will hit these as failures. **That is expected — invert them deliberately,
do not work around them.**

**A Docker-stub shell test cannot prove the Luigi layer.** Stubbing Docker removes Luigi from the
execution path, so such a test proves shell propagation only. For the `[retcode]` half, follow the
existing pattern that runs Luigi for real:
`test_lt_dated_recovery.py:299-429` (`TestLuigiRetcodeReachesTheProcessExit`).

### Land this in TWO PRs, with a survey between them (owner decision 2026-09-05)

1. **Survey the installed crontabs FIRST — before PR1, not between the PRs.** An earlier revision
   of this plan said PR1 "changes no exit status" and could land ahead of the survey. **That was
   wrong**: PR1's entire purpose is to make a retired task name stop exiting 0. An installed line
   such as `... monthly_norms && downstream` would stop running `downstream`, and a retry supervisor
   could start retrying it. The survey must therefore include the retired invocation and its
   arguments, not only the seven wrapper names.
2. **PR1 — task-type validation.** Validate `$TASK_TYPE` against all four advertised types and give
   a retired name a message naming its replacement.
   What the survey must look for, per in-scope wrapper name and for `monthly_norms`: anything
   *after* the invocation — a right-hand `&&`, a retry supervisor, or any other consumer of the exit
   code. Repository cron rows have nothing after `bash <wrapper>` and no systemd unit chains them,
   but **installed crontabs have never been inspected**.
3. **PR2 — exit-status propagation**, informed by that survey. Within each Luigi-backed wrapper the
   `[retcode]` block, `LUIGI_CONFIG_PATH`, compose-status capture and final propagation must land
   **atomically**; the direct-wrapper changes are independent of each other.

One known caller already changes behaviour: `dev_local_backfill.sh --run-pipeline` runs **both**
`run_preprocessing_gateway.sh` and `run_daily_maintenance.sh` from one `steps` array under the same
failure guard (`:532-536`), so a newly non-zero result from either aborts phases 4-6 unless
`--continue-on-error` is passed. Development behaviour, not a production cron chain — noted so it is not
mistaken for a regression.

## What to inspect

1. Which form is actually installed in each deployment's crontab (`crontab -l` on kghm, tjhm, uzhm).
   **This is the load-bearing check and requires server access — it has not been done.** If the
   deployed crontabs use the `bin/README.md` form, the scheduling impact is nil and this is
   docs-only (the wrapper defect above still stands).
2. ~~Whether `run_periodic_maintenance.sh` should gain a `monthly_norms` task type~~ — **answered:
   no.** See the section above; it is test-pinned as retired.
3. ~~Whether the `ValueError` surfaces as a non-zero exit~~ — **answered: it does not.** Proven by
   static shell reading; see the second defect above.
4. ~~Every place the invalid `monthly_norms` command is still documented~~ — **answered: none.**
   DOC-007 (`ede525f7`) corrected `doc/deployment.md`, `deployment_new_hydromet_aws.md` and
   `bin/docker-compose-luigi.yml`, which now lists the four valid types and states that
   `monthly_norms` is retired. Nothing here remains to fix; the documentation half is done.
5. ~~Whether any other documented task type is likewise unmapped~~ — **answered: none.** All four
   advertised types are mapped or specially dispatched.

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
  (`update_deployment_checklist.md:864-876`, `first_deploy_checklist.md:646`/`:852`).
  The documentation sweep itself is done (DOC-007); it is not a criterion for this fix.
- **This issue has two halves and both must land, in two PRs** (see "Land this in TWO PRs"):
  (a) task-type validation in `run_periodic_maintenance.sh`; (b) exit propagation across **all
  seven in-scope scripts, named explicitly** — `run_periodic_maintenance.sh`,
  `run_preprocessing_gateway.sh`, the four `daily_*_maintenance.sh`, and
  `bimonthly_long_term_postprocessing.sh` — including the Luigi `[retcode]` layer where it applies.
  "Six wrappers" was the old wording and let an implementer satisfy the criterion while leaving
  `long_term`, `skill_recalc` and `snow_norms` silent. Delivering only (a) leaves every wrapper
  failure invisible. Do **not** pull unrelated schedule drift (e.g. the snow date) in, and do not
  extend to the out-of-scope canonical wrappers listed above.

## Contract not to break

- **Do NOT restore `monthly_norms` to the task map.** Its removal is deliberate and pinned by two
  tests — `test_yearly_monthly_norms_task_class_is_gone` and
  `test_monthly_norms_dispatcher_key_is_gone`
  (`apps/preprocessing_runoff/test/test_yearly_monthly_norms_retired.py`), retired in Phase 4 of the
  runoff work. Restoring it breaks both and revives the deprecated norm-only path.
- Do not remove, rename or repurpose the live task types (`long_term`, `skill_recalc`,
  `snow_norms`, `lt_recovery` — and do not drop `lt_recovery`'s two extra positional arguments).
  `lt_recovery` is operator-invoked, not a scheduled cron entry; the other three are scheduled. They are scheduled by the repository's cron documentation; **installed** crontabs have not been
  inspected, so their actual contents are unverified. Treat
  them as live until they have.
- `bin/yearly_runoff_hydrograph_aggregation.sh` reads the container's true exit status via
  `docker inspect` (`:206-213`, exits with it at `:232`) rather than the `tee` pipeline code.
  Preserve that. **But do not copy the mechanism literally into the periodic wrapper** — that one
  uses `docker compose run --rm`, so the container is gone before it could be inspected; capture
  Compose's status immediately instead.
