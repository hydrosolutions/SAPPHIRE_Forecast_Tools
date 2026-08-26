# DOC-007: Four documents carry the production cron block, they disagreed, and the one all of them called authoritative was the stale one

**Status**: Implemented (2026-08-26) — awaiting owner review
**Module**: `doc/deployment.md`, `doc/prod/update_deployment_checklist.md`,
`doc/prod/first_deploy_checklist.md`, `doc/plans/deployment_new_hydromet_aws.md`
**Priority**: **Medium** — documentation-only, but the failure mode was an operator faithfully
following the document *labelled authoritative* and thereby reinstalling a cron entry proven to be
a silent no-op.
**Labels**: `documentation`, `deployment`, `cron`, `silent-failure`
**Found**: 2026-08-26, out-of-loop review of the kghm operator handover (PR #480); scope expanded
substantially by a second out-of-loop pass over rev 1 of this draft.
**Related**: **INFRA-023**
(`mid_prio_gi_draft_infra_yearly_monthly_norms_cron_unmapped.md`, Draft, **Medium**) — the
wrapper defect the stale line walked operators into. DOC-007 is the documentation half; this fix
does **not** close INFRA-023, and INFRA-023 would not have closed this.

**Owner decisions taken 2026-08-26**, which set the shape of the fix:

1. **Fix in place across all sources** rather than consolidating to a single canonical document.
   No document was restructured, no anchor moved, no block deleted.
2. **Backup retention: out of scope.** The 30-vs-3-day divergence is left as it stands, and is now
   documented where it will be seen rather than silently normalised.
3. **`doc/plans/deployment_new_hydromet_aws.md` is live**, so it was corrected too.

---

## What was wrong, and what was changed

### The primary defect — retired `monthly_norms` cron entry

`doc/deployment.md` and `doc/plans/deployment_new_hydromet_aws.md` both prescribed:

```
0 3 1 1 * ... bash bin/run_periodic_maintenance.sh monthly_norms ...
```

`bin/run_periodic_maintenance.sh`'s argument guard rejects only an *empty* task type (`:23-28`), so
the retired name reaches Luigi, where `RunPeriodicMaintenanceWorkflow.requires()`
(`apps/pipeline/pipeline_docker.py:2053-2062`) raises `ValueError: Unknown task_type`. The wrapper
has no `set -e` and never captures the exit code of `docker compose run`, ending on two
unconditional `echo`s — so the aggregation does not run, Luigi's error reaches the log, and the
wrapper still returns **0** to cron behind a "task submitted" summary. Every 1 January the
long-horizon hydrograph view was silently not rebuilt.

**Changed**: both entries now invoke `bin/yearly_runoff_hydrograph_aggregation.sh`, each carrying a
comment recording that the retired name is still *accepted* and must not be reinstated.

### The authority claims were circular and pointed at the stale copy

Both operator checklists deferred to `deployment.md` — the copy that was wrong — and
`first_deploy_checklist.md:753` additionally declared itself authoritative, 18 lines before
deferring elsewhere.

**Changed**: each document now states what it actually owns. `update_deployment_checklist.md` §2.5
is named the copy to install and diff against for routine updates; `first_deploy_checklist.md` owns
the first-install copy; `deployment.md` is described as documenting the same schedule with
per-deployment path templates and the UTC timezone table. The unenforceable "kept in sync with that
source" claim is replaced by an explicit statement that nothing enforces equality between the three,
and by a named list of the differences that remain.

### The checklist's own verification command could not work

`update_deployment_checklist.md`'s canonical-block diff terminated its `sed` range on `/^```$/`,
but the block's closing fence is indented two spaces — so the range ran to EOF, extracting ~337
lines instead of the block. Even with that fixed, it diffed an installed crontab's literal values
against `${ENV_FILE_PATH}`/`${LOG_DIR}`/`${LT_ISSUE_DAY}` placeholders, so it could never report a
clean match.

**Changed, in two rounds.** The start anchor is now `/^  # m h  dom mon dow/` — line-anchored,
because the first rewrite's own command text contains that marker and the `sed` range matched
itself; found only by running it. The terminator accepts an indented fence, and both sides are
sorted before `diff`.

A second out-of-loop pass then found the first rewrite was itself unsafe: it interpolated
`${ENV_FILE_PATH}`/`${LOG_DIR}`/`${LT_ISSUE_DAY}` straight into `sed` replacement text, so a path
containing `&` was corrupted and one containing `|` was a syntax error, and unset variables produced
silent garbage or an abort under `set -u`. It also kept crontab environment directives (`SHELL=`,
`PATH=`, `MAILTO=`, `CRON_TZ=`) on the installed side while claiming "executable rows only", then
told the operator to consider them for removal.

A **third** round was then needed. The second rewrite substituted with bash `${var//pat/rep}`,
which under bash 5.2 — where `patsub_replacement` is **on by default** — expands an `&` in the
replacement to the matched text, silently turning `/data/acme&sons/…` into
`/data/acme${ENV_FILE_PATH}sons/…`. My own "verified against hostile values" check had missed it
because it ran under **zsh**, where `&` is literal; the operator runs bash. Measuring the wrong
shell is the same class of error as measuring a simplified repro.

The command now guards each variable with `${VAR:?…}` and renders with `awk` using literal
`index`/`substr` and `ENVIRON` — no shell-version, quoting or metacharacter dependency at all —
and filters both sides to real schedule rows with `grep -E '^[[:space:]]*[0-9*@]'`, excluding
crontab environment directives explicitly and saying so.

**Verified by execution in bash 5.2** with `patsub_replacement` at its default: 97-line block,
exactly 14 executable rows, `/data/acme&sons/config/.env` and `/var/log/a|b` intact with zero
corruption, a path containing a space and backslashes preserved verbatim, `LT_ISSUE_DAY=10,25`
rendered correctly, no unsubstituted placeholder, all 13 `\%Y\%m\%d` cron escapes preserved,
environment directives excluded from both sides, and each unset variable aborting before any
output. **Caveat recorded for operators**: pasted into an *interactive* shell, a `${VAR:?}`
failure reports an error but does not stop subsequent pasted lines — run the block as a script,
or check the guard output before trusting the diff.

### Smaller factual errors, all corrected

| Where | Was | Now |
|---|---|---|
| `deployment.md`, `first_deploy_checklist.md`, `deployment_new_hydromet_aws.md` | snow entry headed "January 1" while its cron reads `0 2 31 8` | 31 August |
| `update_deployment_checklist.md` retired-entries table | `PeriodicMaintenance.requires()`, `pipeline_docker.py:2049-2057` | `RunPeriodicMaintenanceWorkflow.requires()`, `:2053-2062` |
| same table, and `low_prio_gi_draft_prepq_long_horizon_sdk_norm_path_none.md:529` | INFRA-023 cited at a `high_prio_…` path that does not exist | `mid_prio_…` |
| entry (9) comment, 3 places | "monthly + April–September seasonal" | adds **quarterly** — `bin/yearly_runoff_hydrograph_aggregation.sh:14-24` writes month, quarter and season rows |
| `update_deployment_checklist.md:107` | tailed `sapphire_pentadal_*`, which cron never writes | `sapphire_pentadal_forecast_*` |
| `update_deployment_checklist.md:1166` | tailed `sapphire_gateway_*`, which cron never writes | `sapphire_gateway_preprocessing_*` |

A log tail that silently matches nothing looks identical to a job that produced no output, which is
why the two filename corrections are in scope rather than cosmetic.

## Deliberately NOT changed

- **Backup retention** (owner decision 2). `deployment.md:896` passes no `-r` (script default 30,
  `bin/backup_sapphire_db.sh:53`), `first_deploy_checklist.md:797` passes `-r 30`,
  `update_deployment_checklist.md:787` passes `-r 3`. A server installed from the first-deploy
  checklist and later updated from the update checklist silently drops 30 → 3. Now stated in the
  update checklist next to the block instead of being left for someone to discover. **Still open.**
- **Placeholder conventions.** The checklists use `${ENV_FILE_PATH}`/`${LOG_DIR}`/`${LT_ISSUE_DAY}`
  defined in their operator setup blocks; `deployment.md` and the AWS plan use literal
  `<data_folder>`/`<env_file>` templates. This is a real difference and it is correct — do not
  normalise it, and do not let a future "sync" pass churn every line doing so.
- **`update_deployment_checklist.md:1191`.** The optional manual pentadal test writes to
  `sapphire_pentadal_*` rather than the cron filename. Its follow-up check greps `sapphire_*.log`
  with a wildcard, so the pair is self-consistent; a manual test log distinct from the cron log is
  defensible. Left alone.
- **Five AWS-plan divergences, flagged for the owner rather than guessed at.** Counted by
  extracting executable rows from each block: canonical **14**, AWS plan **10**. The AWS plan
  schedules bimonthly long-term postprocessing at **17:00** where the checklists use 22:00, and
  omits four rows entirely — the **daily database backup**, the **weekly Docker image/build-cache
  prune**, the **weekly `pre_update_*/` backup-dir prune**, and **entry (4b)** (bimonthly long-term
  skill recalculation). The missing daily backup is the one worth looking at first. None was
  changed: the document explicitly tells operators to pick an operational window suiting their
  hydromet, so 17:00 may be deliberate, and whether (4b) applies depends on that deployment having
  `long_term_configs` at all. **Owner input needed.**

- **`first_deploy_checklist.md` is a 12-row subset of the 14-row canonical block**, omitting the
  same two weekly housekeeping rows. Rather than add rows to a first-deploy procedure on my own
  judgement, the claims that it carries "the same canonical cron block" were corrected to describe
  it accurately as the first-install subset, naming what it omits and why.

- **`backup_sapphire_db.sh` invocation defects** — the first-deploy checklist omits `-e`, and the
  manual invocations in both checklists omit the required repository-root `cd`. Filed as **DOC-008**
  rather than fixed here: it is a backup-invocation defect, not cron-block drift, and whether the
  missing `-e` actually breaks depends on facts not established (see that issue).

## Verification

- **The first sweep was not exhaustive, and a second out-of-loop pass proved it.** It searched
  `doc/` and missed live occurrences in `bin/` and `apps/`. Corrected in this change:
  `bin/docker-compose-luigi.yml` advertised `monthly_norms` as a selectable task type;
  `apps/pipeline/README` mapped it to a deleted Luigi class and additionally dated the snow task
  "Aug 25"; `bin/run_periodic_maintenance.sh`, `bin/README.md` and
  `bin/yearly_snow_norm_recalculation.sh` all still documented the snow task as January 1; and
  `bin/README.md` plus the aggregation wrapper's own runtime banner omitted quarterly rows.
  `doc/plans/snow_visualization_population_design.md` proposed retargeting the snow task *to*
  January 1 — against the 2026-08-19 owner decision — and is now marked superseded, and
  `review_gi_draft_infra_monthly_norms_from_sdk.md`, whose status read "awaiting review + merge",
  handed operators the retired command as rollout guidance and now carries a SUPERSEDED banner.
- **Two verification queries would have passed while quarterly rows were missing** —
  `update_data_migration_runbook.md` and `historical_backfill_runbook.md` both filtered
  `horizon_type IN ('month', 'season')`. Both now include `'quarter'`.
- **A third pass found the snow sweep still incomplete**, and this one mattered operationally:
  `review_gi_draft_prepg_snow_population_self_heal.md` (PREPG-007) carried a header saying
  "P2 superseded — snow stays 31 Aug" while its **P3 operator steps and acceptance criteria**
  still instructed replacing the Aug-31 `snow_norms` cron entry with a Jan-1 schedule — and
  **P3 is scheduled to run on kghm and tjhm within days**. Following it would have moved the
  norms mid-season, the exact outcome the 2026-08-19 decision rejected. Every live Jan-1
  instruction in that file (summary, executor constraints, P3 steps, and three acceptance
  criteria) is now voided or inverted, with a warning banner at the top; P1 and P3's actual
  data-remediation work is untouched. Latent, not caused by this change.
- Sweeps re-run repo-wide after the above, each returning clean: no live document prescribes
  `run_periodic_maintenance.sh monthly_norms` as a cron entry; no live instruction retargets
  the `snow_norms` schedule; no "monthly + April–September" horizon description remains; no
  `PeriodicMaintenance.requires` reference remains; no `high_prio_…` INFRA-023 path remains.
- The rewritten verification command was **executed**, not just read: 97 lines extracted, 14
  executable rows, 0 non-cron lines.
- Every replacement was applied through an exact-match, count-asserted edit, so no near-miss
  passage was silently skipped.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — docs-only change, but run as the standing
  precondition.

## Remaining acceptance criteria for the owner to confirm

- The two AWS-plan items above (17:00 schedule, missing entry (4b)) are resolved or explicitly
  accepted.
- Backup retention is decided, or accepted as a documented divergence.
- **Dependent follow-up:** `doc/prod/kghm_pipeline_handover.md` §3 step 1 and §5 warn operators
  about the stale `deployment.md` and point at the cron audit. That file lands with PR #480; once
  both it and this fix are merged, its warning should be reduced to the retention divergence only.
