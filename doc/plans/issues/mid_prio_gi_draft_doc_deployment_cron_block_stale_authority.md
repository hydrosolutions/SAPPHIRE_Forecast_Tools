# DOC-007: Four documents carry the production cron block, they disagree, and the one all of them call authoritative is the stale one

**Status**: Draft (2026-08-26, rev 2 after out-of-loop review)
**Module**: `doc/deployment.md`, `doc/prod/update_deployment_checklist.md`,
`doc/prod/first_deploy_checklist.md`, `doc/plans/deployment_new_hydromet_aws.md`
**Priority**: **Medium** — documentation-only, but the failure mode is an operator faithfully
following the document *labelled authoritative* and thereby (a) reinstalling a cron entry already
proven to be a silent no-op, and (b) in one path, silently shortening database backup retention
from 30 days to 3.
**Labels**: `documentation`, `deployment`, `cron`, `silent-failure`
**Found**: 2026-08-26, out-of-loop review of the kghm operator handover (PR #480); scope expanded
substantially by a second out-of-loop pass over rev 1 of this draft.
**Related**: **INFRA-023**
(`issues/mid_prio_gi_draft_infra_yearly_monthly_norms_cron_unmapped.md`, Draft, **Medium**) — the
wrapper defect this stale line walks the operator into. DOC-007 is the documentation half; fixing
either does not fix the other.

> **This is not a two-line fix.** Rev 1 of this draft described two divergences between two
> documents. A full comparison found four documents, a circular authority claim, a broken
> verification command, and one genuine operational-policy divergence that must be decided by the
> owner before any consolidation happens. Read "Blocking decisions" before implementing.

---

## The authority claim is circular, and points at the stale copy

| Document | What it claims | Reality |
|---|---|---|
| `update_deployment_checklist.md:715` | "The authoritative source is `deployment.md` … The block reproduced below is kept in sync with that source." | Not in sync — see below. |
| `first_deploy_checklist.md:753` | "This section is **the authoritative install procedure**. The same canonical cron block is reproduced in `update_deployment_checklist.md` §2.5" | Claims itself authoritative… |
| `first_deploy_checklist.md:771` | "The authoritative source is `doc/deployment.md` §Set up cron job" | …and, 18 lines later, defers to `deployment.md`. |
| `doc/deployment.md:883-955` | (the block all three defer to) | **Still prescribes the retired `monthly_norms` entry** at `:953-954`. |

So two operator checklists defer to a third document that is wrong on the entry that matters, one
of them also declares itself authoritative, and the update checklist's retired-entries table
(`:837`) explicitly documents that the deferred-to document's entry fails silently.

## Confirmed divergences

**The primary one.** `deployment.md:953-954` prescribes:

```
0 3 1 1 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh monthly_norms ...
```

while `update_deployment_checklist.md:829` (entry **(9)**) has the correct replacement,
`bin/yearly_runoff_hydrograph_aggregation.sh`. `doc/plans/deployment_new_hydromet_aws.md:665` also
still prescribes `monthly_norms`, and additionally schedules long-term postprocessing at 17:00
rather than 22:00 and omits entry (4b) entirely.

**Consequence.** `bin/run_periodic_maintenance.sh`'s argument guard rejects only an *empty* task
type (`:22-28`), so `monthly_norms` reaches Luigi, where
`RunPeriodicMaintenanceWorkflow.requires()` (`apps/pipeline/pipeline_docker.py:2044`, task map at
`:2054-2062`) raises `ValueError: Unknown task_type`. The wrapper has no `set -e`, never captures
the exit code of `docker compose run`, and ends with two unconditional `echo`s (`:82-90`). The
requested aggregation does not run; Luigi/Compose error output *does* reach the captured log, but
is followed by a positive "task submitted" summary and a **0 exit status to cron**. Every 1 January
the long-horizon hydrograph view is silently not rebuilt.

> Note: `update_deployment_checklist.md:837` attributes the raise to `PeriodicMaintenance.requires()`.
> The class is `RunPeriodicMaintenanceWorkflow`. Correct that line as part of this fix.
> `module_issues.md:65` already has it right.

**Backup retention — an operational divergence, not a typo.** Three copies, three behaviours:

| Document | Invocation | Effective retention |
|---|---|---|
| `deployment.md:896` | no `-r` flag | **30 days** (script default, `bin/backup_sapphire_db.sh:53`) |
| `first_deploy_checklist.md:795` | `-r 30` | **30 days** |
| `update_deployment_checklist.md:750` | `-r 3` | **3 days** |

A server installed from the first-deploy checklist keeps 30 days of database backups; the same
server, after a routine update following the update checklist, keeps 3. Nothing announces the
change. **This must be decided, not silently normalised in either direction** — see "Blocking
decisions".

**Snow entry comment.** `deployment.md:948` heads the entry "at 02:00 UTC on January 1" while its
cron expression on `:952` is `0 2 31 8` (31 August) and the prose below records the 2026-08-19
decision to move it. Schedule right, header stale. `first_deploy_checklist.md` carries the same
stale January-1 header.

**Log filenames.** Several entries write to different filenames across copies —
`sapphire_pentadal_*` vs `sapphire_pentadal_forecast_*`, `sapphire_maintenance_*` vs
`sapphire_daily_maintenance_*`, `sapphire_periodic_monthlynorms_*` vs
`sapphire_yearly_runoff_hydrograph_*`, and others. This matters because *other* instructions tail
these files by name: `update_deployment_checklist.md:107` tails `sapphire_pentadal_*` (no
`_forecast`), and `:1166` has the same mismatch for the gateway. A log-tail that silently matches
nothing looks identical to a job that produced no output.

**Entry (9)'s own comment is incomplete** in the checklist: it describes a "monthly + April–September
seasonal" view, but `bin/yearly_runoff_hydrograph_aggregation.sh:18-24` writes month, **quarter**
and season rows.

**Not defects — do not "fix" these.** The checklist deliberately uses `${ENV_FILE_PATH}`,
`${LOG_DIR}`, `${LT_ISSUE_DAY}` and `${DATA_DIR}` placeholders, defined in its operator setup block;
`deployment.md` uses literal `<data_folder>`/`<env_file>` templates. That difference is intentional
and should survive consolidation. Prose and comment wording differs throughout; only comments that
state something *false* (the snow header, entry (9)'s horizons) are in scope.

## The checklist's own verification command does not work

`update_deployment_checklist.md:720` offers:

```bash
diff /tmp/crontab_current.txt <(sed -n '/# m h  dom mon dow/,/^```$/p' "$(pwd)/doc/prod/update_deployment_checklist.md")
```

The terminating pattern is `/^```$/`, but the block's closing fence is **indented two spaces**, so
the range does not end where intended — on disk this extracts ~337 lines instead of the ~96-line
block. Even with the fence anchor fixed, it diffs an installed crontab containing literal values
against a block containing `${ENV_FILE_PATH}`/`${LOG_DIR}`/`${LT_ISSUE_DAY}`, so it can never
produce a clean equality result. This is the command the kghm handover currently points operators
at for their cron audit.

## Blocking decisions — for the owner, before implementation

1. **Which document owns the cron block?** Recommendation: `update_deployment_checklist.md` §2.5 —
   it is what operators execute, and it already carries the retired-entries table and the audit
   steps. The alternative is to correct `deployment.md` and keep it as owner. Either is defensible;
   what cannot stand is three documents each pointing at another.
2. **Backup retention: 3 days or 30?** Consolidating on the checklist as written propagates `-r 3`
   to every deployment. Consolidating on `deployment.md`/first-deploy propagates 30. This is a
   recovery-window decision, not a documentation one. If it warrants its own issue, split it out
   before this one is implemented.
3. **Is `doc/plans/deployment_new_hydromet_aws.md` live or historical?** If live, it must be
   reconciled. If historical, it should say so at the top, and be excluded from the sweep below.

## Proposed fix (not implemented here; blocked on the decisions above)

- Make one document the owner. Reduce the others' runnable fenced schedules to a link, **keeping**
  each file's `## Set up cron job` heading and anchor (inbound links exist from both checklists and
  `deployment.md`'s own ToC), and keeping the unique surrounding guidance each file has — the
  timezone table, log-directory setup, Luigi-daemon requirement and backup-restore pointer are not
  duplicated content and must not be deleted with the block.
- Update every claim that a *different* file holds the canonical schedule: `bin/README.md:219`
  ("full recommended crontab"), `doc/dev/review_checklist_server_template.md:191` (crontab
  "matches" `deployment.md`), and `doc/plans/deployment_new_hydromet_aws.md:598`.
- Fix the verifier at `:720`: anchor the fence correctly, and compare normalised executable rows
  after rendering the placeholders.

A claim that two documents are in sync is itself a maintenance burden. Do not restate it unless
something enforces it.

## Acceptance criteria

- **No live operator document prescribes `run_periodic_maintenance.sh monthly_norms` as a cron
  entry.** A mention inside an explicit "retired — do not use" note is the only permitted form.
  Verify with `grep -rn 'run_periodic_maintenance.sh monthly_norms' doc/ bin/`, reviewing each hit;
  historical issue files under `doc/plans/issues/` are out of scope.
- Exactly **one** document contains a runnable cron block; every other live copy is replaced by a
  link to it, and the owner document states plainly that it is canonical.
- No document claims another is "authoritative", "canonical", "kept in sync", "the same canonical
  block", or "matches" unless that is true at the moment of the change. Sweep all of those phrasings,
  not just "kept in sync".
- `doc/deployment.md` still resolves at the `#set-up-cron-job` anchor, and every inbound link to it
  still resolves.
- Every remaining live snow entry — schedule *and* comment header — says 31 August.
- Entry (9)'s comment names all three horizons written by the wrapper: month, quarter, season.
- The class name at `update_deployment_checklist.md:837` reads `RunPeriodicMaintenanceWorkflow`.
- Backup retention is identical across every remaining live copy, at whichever value decision 2
  settles on.
- Log filenames in the canonical block match the filenames every other instruction tails —
  specifically `update_deployment_checklist.md:107` and `:1166`.
- The verification command at `:720` extracts only the intended block (≈96 lines, not ~337) and can
  compare equal against a correctly installed crontab.
- **Dependent, do afterwards:** `doc/prod/kghm_pipeline_handover.md` §3 step 1 and §5 warn operators
  about this exact discrepancy and about the broken verifier. That file lands with PR #480; update
  it once both that PR and this fix are in.
