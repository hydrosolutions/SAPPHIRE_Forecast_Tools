# PREPG-007: Snow visualization population gaps - self-healing curve and Jan 1 snow norms

**Status**: Review (2026-06-25) — P1+P2 implemented, awaiting review
**Module**: `apps/preprocessing_gateway` + deployment cron/docs
**Priority**: **High** (user-facing defect on a deployed forecast dashboard)
**Labels**: `preprocessing_gateway`, `snow-data`, `operational`, `dashboard`, `maintenance`
**Source of truth**: `doc/plans/snow_visualization_population_design.md`
**Related**: PREPG-001, PREPG-006, PREPG-008, FD-014

> Sanitized: use placeholder station code `19999` in tests and examples. Do not commit real
> station codes, real discharge values, real SWE values, or deployment secrets.

---

## Summary

Fix two operational snow-population gaps that make the dashboard snow plot incomplete:

1. Current-season `value`/`current` can develop a historical gap because operational snow writes only
   `date >= yesterday`, and maintenance snow writes only `today - 30 days` even though the gateway
   already fetches `today - 365 days`.
2. Snow norm/previous/percentile bands are missing for the current calendar-year part of the
   hydrological display window because `snow_norms` runs on Aug 31 and writes the current calendar
   year only then.

This issue implements the validated design with reviewer corrections:

- **Change B**: widen only the maintenance-mode snow write filter in `dg_utils.write_snow_to_api()`
  from 30 days to 365 days. Operational mode remains `date >= yesterday`. Add the defensive
  anti-clobber guard needed for the wider write window.
- **Change A**: retarget the existing annual `snow_norms` periodic-maintenance schedule from Aug 31
  (`0 2 31 8 *`) to Jan 1. Give `snow_norms` its own Jan-1 cron line, for example `0 2 1 1 *`,
  at least 30 minutes away from the real Jan-1 runoff hydrograph aggregation job at 03:00.
- **Rollout**: after deploy, take a pre-remediation backup and SQL count snapshot, run one widened
  maintenance sync, then run one current-year snow norm recalc for 2026.

No service changes, no new scripts, and no second annual snow-norm pass are in scope.

---

## Problem

The forecast dashboard snow plot uses a hydrological display window, for example
`2025-09-01 ... 2026-08-31`. On a deployed dashboard:

- The current-season curve is present through early 2026, has a gap, then resumes around mid-May.
- From mid-May onward the current-season curve and forecast render, but `norm`, `previous`, and
  percentile/min-max bands are absent for the 2026 portion.

The service schema and `/snow/` endpoint already expose the needed fields. This is an operational
population problem, not a dashboard rendering problem.

## Root Cause

### Bands

`recalculate_snow_norms.py` writes records per calendar year and already defaults to the current
calendar year. The current cron runs `run_periodic_maintenance.sh snow_norms` on Aug 31
(`0 2 31 8 *`), so the Jan-Aug part of the hydrological display window has no current-year bands
until late August.

The script itself does not need a code change for this issue. Climatology inputs are identical on
Jan 1 and Aug 31 because they come from completed prior years. The leap-year day-of-year climatology
bug is tracked separately as PREPG-008 because fixing it requires editing `recalculate_snow_norms.py`,
which is out of scope here.

### Current curve

`snow_data_operational.py` already fetches the last 365 days, but `dg_utils.write_snow_to_api()` drops
all but the last 30 days in maintenance mode. There is no path that heals a hole older than 30 days.

Existing cooperation must remain unchanged:

- Maintenance writes preserve recalc bands in `dg_utils.py` (`norm`, stats, `previous` fields).
- Recalc preserves existing `value`/`current` in `recalculate_snow_norms.py`.

The wider maintenance window also increases the blast radius of incomplete incoming CSV rows. In
`dg_utils.py:943-947`, elevation-band columns `value1` ... `value14` are taken only from the incoming
CSV row. They are not in `SNOW_PRESERVED_STAT_FIELDS`, and the service upsert overwrites columns with
`None` because `crud.py:238` uses `model_dump()` without `exclude_none`, then `crud.py:256-257`
blindly assigns every field. The P1 implementation must therefore include a narrow anti-clobber guard.

---

## Executor Constraints

- Agents may modify only the files listed in each phase's allow-list.
- Changes must be additive/minimal. Do **NOT** change existing function signatures, data flow, or
  control flow beyond the specific filter widening and defensive row/field guard described here.
- Test behavior, not implementation details. New or changed logic gets tests; follow
  `doc/dev/testing_workflow.md`.
- Use placeholder station code `19999` in new or changed tests/fixtures. Do not use real station
  codes or real discharge/SWE values.
- Do not edit `sapphire/services/**`; the snow fields already exist.
- Do not add scripts or a second annual snow-norm pass. Retarget the existing `snow_norms` annual
  cadence.
- Do not change `apps/preprocessing_gateway/snow_data_operational.py`; it already fetches 365 days.
- Do not change `apps/preprocessing_gateway/recalculate_snow_norms.py` in PREPG-007. Leap-year
  climatology alignment is PREPG-008.
- Change A and Change B must ship as one deployable unit. Do not deploy the Jan-1 cron retarget to a
  server without the widened maintenance sync: the removed Aug-31 recalc previously backfilled
  `current` for Jan-Aug, and that responsibility transfers to maintenance after this change.

---

## Phases

### P1 - Change B: self-healing maintenance snow writes

**Goal**

Widen maintenance-mode snow writes so `daily_gateway_maintenance.sh` rewrites the full 365-day snow
window and can heal historical `value`/`current` holes. Keep operational mode unchanged.

**Files**

Allowed to modify:

- `apps/preprocessing_gateway/dg_utils.py`
- `apps/preprocessing_gateway/test/test_api_integration.py`
- `apps/preprocessing_gateway/test/test_edge_cases.py`
- `apps/preprocessing_gateway/test/test_integration_preprocessing_gateway.py`
- `apps/preprocessing_gateway/test/test_api_coverage_gaps.py`

No other files.

**Depends on**

None.

**Agents**

1 Sonnet 4.6 general-purpose agent, `isolation: "worktree"`.

Scope:

- In `write_snow_to_api()`, change only the maintenance-mode cutoff from `ref - 30 days` to
  `ref - 365 days`.
- Update the maintenance docstring in `dg_utils.py:739-748` and any stale "30 day" wording in the
  snow write path to 365.
- Rewrite and rename these existing tests that will fail after the 365-day change. Do not delete
  them:
  - `test_api_integration.py:304` `test_writes_last_30_days_only`
  - `test_api_integration.py:1805` `test_maintenance_mode_writes_last_30_days`
  - `test_api_integration.py:2189` `test_reference_date_used_for_maintenance` (update expected
    count and start date)
  - `test_edge_cases.py:566` `test_maintenance_30_day_window_spanning_year_boundary`
- Correct stale snow-maintenance docstrings/comments at:
  - `test_api_integration.py:239`, `:246`, `:305`, `:1806`, `:2223`, and class doc `:244-248`
  - `test_edge_cases.py:566-567`, `:598`
  - `test_integration_preprocessing_gateway.py:1212-1213` (`test_snow_operational_vs_maintenance_filtering`)
    from "maintenance writes 30 days" to 365 days; its body only asserts
    `len(maintenance_snow) >= len(operational_snow)` at `:1300-1304`, which holds for both window
    sizes, so exact-window coverage comes from the new boundary test rather than strengthening this
    integration assertion.
- Use `test_api_integration.py:2362`
  `test_maintenance_mode_writes_last_365_days` as the reanalysis precedent/template.
- Add a maintenance-mode band-preservation test for an old row: seed `read_snow` with a non-empty
  record about 100 days before `reference_date` carrying `norm`, `mean`, `min`, `max`, `q05`, `q25`,
  `q50`, `q75`, `q95`, and `previous`; run `mode="maintenance"` with incoming data lacking those
  columns; assert the written old-date record retains every stat-band field and updates only
  `value`/`current`.
- Add a 365/366 boundary test: a row at `reference_date - 366 days` is not written, and a row at
  `reference_date - 365 days` is written.
- Add a defensive anti-clobber guard in `write_snow_to_api()` for wider-window incomplete rows:
  - Skip appending a record when its incoming main value and all incoming elevation-band values are
    `None`.
  - For value-only incoming rows, do not send `value1` ... `value14` as `None` over existing
    DB-populated bands. Either preserve existing band values or omit absent band keys so the upsert
    does not null them.
  - If the incoming main value is missing, fall `value` back to the existing API value the same way
    `current` already does at `dg_utils.py:918-926`.
  - Do not change function signatures or unrelated control flow.
- Add a test proving an old date with DB-populated `value1` ... `value14` bands and value-only
  incoming data is not nulled by the write.
- Do not alter unrelated write/read behavior.

Explicitly out of scope / do not touch (legitimately 30-day or unrelated):

- `test_consistency_failures.py:189-207` (`reanalysis _check_snow_consistency`)
- `test_integration_preprocessing_gateway.py:946-963` (meteo QM)
- `test_api_integration.py:2280-2306` (meteo QM)

**Acceptance criteria**

- [ ] Maintenance mode writes snow rows across the full 365-day window using a deterministic
      reference date; the assertion is behavioral (records written by date), not implementation
      inspection.
- [ ] The `reference_date - 365 days` boundary is inclusive and `reference_date - 366 days` is
      excluded.
- [ ] Operational mode remains unchanged: it writes only records with `date >= yesterday`.
- [ ] Maintenance write preservation still holds after widening: existing `norm`, `mean`, `min`,
      `max`, `q05`, `q25`, `q50`, `q75`, `q95`, `previous`, and `current` cooperation is not
      clobbered by the wider write.
- [ ] Existing DB-populated elevation bands (`value1` ... `value14`) are not nulled when an old
      maintenance-row update has only the main snow value and no incoming band columns/values.
- [ ] Fully empty incoming rows (no main value and no elevation-band values) are not written.
- [ ] Recalc cooperation remains intact by test or existing-test coverage: `recalculate_snow_norms.py`
      preserves existing `value`/`current` values when writing bands.
- [ ] No real station codes or real data values appear in new/changed tests; use `19999` and dummy
      numeric values only.
- [ ] From `apps/`, `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` passes with zero
      unexpected skips. The only acceptable skip is the explicit `sapphire-api-client` dependency gate.

### P2 - Change A: retarget annual snow_norms to Jan 1

**Goal**

Retarget the annual `snow_norms` periodic-maintenance schedule from Aug 31 to Jan 1, replacing the
Aug-31 run. Keep one annual snow-norm pass only.

**Files**

Allowed to modify:

- `doc/deployment.md`
- `doc/prod/update_deployment_checklist.md`
- `doc/prod/first_deploy_checklist.md`
- `doc/plans/deployment_new_hydromet_aws.md`
- `bin/README.md`
- `bin/run_periodic_maintenance.sh`
- `bin/yearly_snow_norm_recalculation.sh`
- `bin/daily_gateway_maintenance.sh`

No other files.

**Depends on**

None. P2 can run in parallel with P1.

**Agents**

1 Sonnet 4.6 general-purpose agent, `isolation: "worktree"`.

Scope:

- Update deployment cron snippets and schedule tables so `snow_norms` runs on Jan 1 and targets the
  current calendar year.
- Do not describe `snow_norms` as co-scheduled with `monthly_norms`. `monthly_norms` is not a
  supported `RunPeriodicMaintenanceWorkflow` task type in `apps/pipeline/pipeline_docker.py:2049-2053`;
  running `bin/run_periodic_maintenance.sh monthly_norms` would raise `ValueError`.
- Give `snow_norms` its own Jan-1 cron line, for example `0 2 1 1 *`, kept at least 30 minutes away
  from the real Jan-1 runoff hydrograph aggregation job (`bin/yearly_runoff_hydrograph_aggregation.sh`,
  cron `0 3 1 1 *`) to avoid SSH-tunnel/cleanup-trap contention.
- In `doc/deployment.md:909-910` and `doc/plans/deployment_new_hydromet_aws.md:651-652`, the
  documented `run_periodic_maintenance.sh monthly_norms` cron lines are a known adjacent defect and
  out of scope for PREPG-007; do not modify them here, but place the new `snow_norms` Jan-1 line at
  least 30 minutes away from the documented `0 3 1 1 *` job in each file.
- Correct the stale `monthly_norms` claim in the `bin/README.md` periodic-maintenance schedule/table.
- Remove stale references that instruct operators to run annual snow norms on Aug 31.
- In `bin/run_periodic_maintenance.sh`, update task descriptions so they only list supported task
  types (`long_term`, `skill_recalc`, `snow_norms`) unless the code is changed in a separate issue.
- In `bin/yearly_snow_norm_recalculation.sh`, update only the docstring wording so it no longer
  implies an August cadence and aligns with the Jan-1 schedule. That file's example cron is
  `0 2 25 8 *`, not a day-31 cron, so there is no `0 2 31 8 *` line there to change.
- In `bin/daily_gateway_maintenance.sh`, update operator-facing comments at `:12` and the
  `SAPPHIRE_SYNC_MODE` block around `:98-102` so snow says 365 days. This file has no cron line, so
  it is exempt from the `0 2 31 8` grep check.
- Update only schedule/docs/docstrings. Do not change `recalculate_snow_norms.py`.
- Do not create a second annual snow-norm task; replace the old annual cadence.

**Acceptance criteria**

- [ ] All listed docs/docstrings show the Jan-1 snow-norm schedule and state that it targets the
      current calendar year.
- [ ] `snow_norms` has its own Jan-1 cron line and is at least 30 minutes away from the 03:00 Jan-1
      runoff hydrograph aggregation cron line.
- [ ] Except for the explicitly out-of-scope existing cron lines in `doc/deployment.md` and
      `doc/plans/deployment_new_hydromet_aws.md`, no docs claim `monthly_norms` is a valid
      `run_periodic_maintenance.sh` task unless a separate implementation adds it to
      `RunPeriodicMaintenanceWorkflow`.
- [ ] No stale Aug-31 snow-norm schedule remains in the P2 deployment-cron/schedule allow-list. Run
      and record:
      `rg -n "0 2 31 8 \\*|August 31|Aug 31|8/31" doc/deployment.md doc/prod/update_deployment_checklist.md doc/prod/first_deploy_checklist.md doc/plans/deployment_new_hydromet_aws.md bin/README.md bin/run_periodic_maintenance.sh bin/yearly_snow_norm_recalculation.sh`.
      Intentional old-state references in the design doc, this issue file, other/archived issues,
      `bin/daily_gateway_maintenance.sh`, and dated review checklists are expected and out of scope.
- [ ] No second annual snow-norm pass is documented or added.
- [ ] No implementation files outside the allow-list are changed.

### P3 - Rollout and one-time remediation runbook

**Goal**

Execute the post-deploy operational remediation that the steady-state fix does not backfill by itself:
fill the current 2026 `value`/`current` window and then write the current-year bands.

**Files**

No implementation files. The runbook is this phase in this issue; deployment docs touched in P2 may
refer to the steady-state Jan-1 schedule but must not add new code or scripts for this one-time action.

**Depends on**

P1 and P2.

**Agents**

No implementation agent required. Operator/orchestrator action after deploy.

Scope:

> **Preconditions (learned during local validation 2026-06-26; rescued to trunk 2026-08-20).**
>
> - **The archive must be fully loaded first.** Run the snow recalc only after the full historical
>   snow archive is present in the DB — **not** merely after the 365-day maintenance sync, which
>   loads only the recent window. Recalc against a partially-loaded archive yields starved
>   per-day-of-year climatology counts and missing or seamed bands. Observed: HS/SWE early-January
>   `count` of **2-5 instead of the full ~26**. Verify completeness first, e.g.
>
>   ```sql
>   SELECT snow_type, count(DISTINCT EXTRACT(YEAR FROM date))
>   FROM snow
>   WHERE value IS NOT NULL AND EXTRACT(DOY FROM date) BETWEEN 1 AND 10
>   GROUP BY snow_type;
>   ```
>
> - **The pagination precondition is now SATISFIED — no longer a blocker.** The recalc's climatology
>   read paginates `get_snow()`, which used to be nondeterministic without a stable sort, so bands
>   could flicker or come back incomplete regardless of archive completeness. **`crud.get_snow()` now
>   orders by `(snow_type, code, date, id)`** — a stable sort with a unique tiebreaker — so this no
>   longer gates the remediation. Re-verify it is present in the **target environment** before
>   running, since the fix lives in `sapphire/services/preprocessing` and deploys on its own cadence.
>
>   *Reference correction:* the original note called this dependency "PREPG-009". **That ID now
>   belongs to a different issue** (gateway reports PASS with all six snow tasks errored). The
>   pagination issue was drafted but never merged, and its fix shipped anyway — do not go looking
>   for it under that number.

- Before remediation, take a database backup: snow-table `pg_dump` or a volume snapshot.
- Before remediation, capture a SQL count snapshot for each `snow_type` over
  `2025-09-01 ... 2026-08-31`, counting non-null `value`, `current`, `norm`, `previous`, `q50`, and
  `mean`. Use placeholder station code `19999` in example SQL; never paste real station codes or
  values into this issue.
- Before and after crontab edits, verify `crontab -l | grep -c snow_norms` equals `1`. This catches
  both duplicate annual passes and a missed `snow_norms` entry.
- Retarget the live crontab on each already-deployed environment, including Tajik and Kyrgyz where
  applicable: use `crontab -e` to replace the `snow_norms` Aug-31 entry (`0 2 31 8 *`) with the
  Jan-1 schedule. Keep it at least 30 minutes away from the 03:00 Jan-1 runoff hydrograph aggregation
  cron line.
- Run exactly these commands once after the P1/P2 changes are deployed to the target environment:

  ```bash
  bash bin/daily_gateway_maintenance.sh <env>
  ieasyhydroforecast_SNOW_RECALC_YEAR=2026 bash bin/run_periodic_maintenance.sh snow_norms <env>
  ```

- Order rationale: maintenance-first fills `value`/`current`; recalc-second
  (`SNOW_RECALC_YEAR=2026`) is calendar-year scoped, so it wraps bands around the 2026 records and
  cannot touch the 2025 portion of the hydrological window. If step 1 logs per-station write errors,
  re-run step 1 successfully before step 2.
- Verify the snow table and dashboard for the hydrological window `2025-09-01 ... 2026-08-31`.
- Do not add code for this one-time remediation.

Example sanitized SQL shape:

```sql
SELECT
  snow_type,
  count(*) FILTER (WHERE value IS NOT NULL) AS n_value,
  count(*) FILTER (WHERE current IS NOT NULL) AS n_current,
  count(*) FILTER (WHERE norm IS NOT NULL) AS n_norm,
  count(*) FILTER (WHERE previous IS NOT NULL) AS n_previous,
  count(*) FILTER (WHERE q50 IS NOT NULL) AS n_q50,
  count(*) FILTER (WHERE mean IS NOT NULL) AS n_mean
FROM snow
WHERE code = '19999'
  AND date BETWEEN DATE '2025-09-01' AND DATE '2026-08-31'
GROUP BY snow_type
ORDER BY snow_type;
```

**Acceptance criteria**

- [ ] PRE backup exists before remediation: snow-table `pg_dump` or volume snapshot.
- [ ] PRE SQL count snapshot exists for `2025-09-01 ... 2026-08-31`, grouped by `snow_type`, with
      non-null counts for `value`, `current`, `norm`, `previous`, `q50`, and `mean`.
- [ ] On each already-deployed environment, `crontab -l | grep -c snow_norms` equals `1` before and
      after the cron edit.
- [ ] On each already-deployed environment, the active crontab is retargeted: `crontab -l` shows the
      Jan-1 `snow_norms` schedule, with no Aug-31 `snow_norms` entry remaining.
- [ ] The widened maintenance sync completes successfully for `<env>` and writes `value`/`current`
      rows over the 365-day maintenance window. If per-station write errors appear, step 1 is re-run
      before step 2.
- [ ] The one-time 2026 snow norm recalc completes successfully and writes `norm`, `previous`,
      percentile fields, `mean`, `min`, and `max` for current-year snow rows.
- [ ] POST SQL count verification is mandatory. For each expected snow type:
      - POST `n_value` and `n_current` are greater than or equal to PRE and cover the known gap dates.
      - POST 2026-portion band counts (`norm`, `previous`, `q50`, `mean`, plus percentile/min/max
        spot checks) are greater than or equal to PRE.
      - POST Sep-Dec 2025 band counts are greater than or equal to PRE; existing 2025 bands must not
        decrease.
- [ ] Verification confirms that the live dashboard snow plot shows a continuous current-season curve
      plus norm/previous/percentile bands across the hydrological display window.
- [ ] If verification fails, capture only sanitized counts/dates and re-run the appropriate deployed
      task; do not expand scope into dashboard or service changes without a new issue.

---

## Overall Acceptance Criteria

- [ ] P1 and P2 are implemented as one deployable unit.
- [ ] Change A is not deployed anywhere without Change B.
- [ ] Maintenance snow writes now self-heal the last 365 days; operational writes still use
      `date >= yesterday`.
- [ ] Band-preservation, elevation-band anti-clobber, and value-preservation cooperation all hold:
      maintenance does not clobber recalc/stat/elevation bands, and recalc does not clobber existing
      `value`/`current`.
- [ ] From `apps/`, `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` passes with zero
      unexpected skips. The only acceptable skip is the explicit `sapphire-api-client` dependency gate.
- [ ] All updated docs/docstrings show Jan 1 for `snow_norms`, and the grep check
      `rg -n "0 2 31 8 \\*|August 31|Aug 31|8/31" doc/deployment.md doc/prod/update_deployment_checklist.md doc/prod/first_deploy_checklist.md doc/plans/deployment_new_hydromet_aws.md bin/README.md bin/run_periodic_maintenance.sh bin/yearly_snow_norm_recalculation.sh`
      finds no stale snow-norm schedule references in the P2 deployment-cron/schedule allow-list.
      Intentional old-state references in the design doc, this issue file, other/archived issues,
      `bin/daily_gateway_maintenance.sh`, and dated review checklists are expected and out of scope.
- [ ] On each already-deployed environment, `crontab -l | grep -c snow_norms` equals `1` before and
      after the edit, and `crontab -l` shows the Jan-1 `snow_norms` schedule with no Aug-31
      `snow_norms` entry remaining.
- [ ] The one-time remediation commands are run after deploy for the current 2026 window, in the
      documented order.
- [ ] Mandatory PRE/POST SQL count verification shows no count regressions and confirms gap coverage.
- [ ] Live verification shows the dashboard snow plot has a continuous current-season curve and
      norm/previous/percentile bands across the hydrological window.

## Out of Scope

- `sapphire/services/**` changes.
- New scripts.
- A second annual `snow_norms` cron entry/pass.
- Dashboard rendering changes.
- `recalculate_snow_norms.py` changes in PREPG-007. Leap-year climatology alignment is PREPG-008.
- Investigation of why the historical Jan-to-mid-May 2026 operational write gap happened on the
  server. This fix makes that class of gap self-healing regardless of cause.

---

## Dependency Graph

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": [], "parallel_agents": 1 },
    "P3": { "depends_on": ["P1", "P2"], "parallel_agents": 0 }
  }
}
```
