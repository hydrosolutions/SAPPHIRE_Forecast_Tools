# DOC-007: `doc/deployment.md`'s cron block is stale, and the checklist names it authoritative

**Status**: Draft (2026-08-26)
**Module**: `doc/deployment.md`, `doc/prod/update_deployment_checklist.md`
**Priority**: **Mid** — documentation-only, but the failure mode is an operator faithfully
following the doc that is *labelled authoritative* and thereby reinstalling a cron entry already
proven to be a silent no-op (INFRA-023). No code is wrong; the pointer between two docs is.
**Labels**: `documentation`, `deployment`, `cron`, `silent-failure`
**Found**: 2026-08-26, out-of-loop review of the kghm operator handover (PR #480).
**Related**: **INFRA-023** (`issues/high_prio_gi_draft_infra_yearly_monthly_norms_cron_unmapped.md`,
Draft) — the wrapper defect this stale line walks the operator into. DOC-007 is the documentation
half; fixing it does not fix INFRA-023 and vice versa.

---

## The defect

`doc/prod/update_deployment_checklist.md:715` states:

> The authoritative source is [deployment.md - Set up cron job](../deployment.md#set-up-cron-job).
> The block reproduced below is kept in sync with that source.

For the yearly long-horizon entry that is currently untrue. `doc/deployment.md:953-954` still
documents the **retired** task name:

```
# Yearly monthly discharge norm recalculation from iEH HF SDK at 03:00 UTC on January 1
0 3 1 1 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh monthly_norms ...
```

while the checklist's own canonical block (`update_deployment_checklist.md:829`, entry **(9)**)
has the correct replacement:

```
0 3 1 1 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/yearly_runoff_hydrograph_aggregation.sh ${ENV_FILE_PATH} ...
```

The checklist already documents *why* the old form is wrong, in its own retired-entries table at
`:837`. So the checklist simultaneously (a) explains that `monthly_norms` fails silently, and
(b) points at a document that still prescribes it.

**Consequence.** `bin/run_periodic_maintenance.sh`'s argument guard rejects only an *empty* task
type, so `monthly_norms` reaches Luigi, where `PeriodicMaintenance.requires()` raises
`ValueError: Unknown task_type`. The wrapper has no `set -e`, never captures the exit code of
`docker compose run`, and ends with unconditional `echo`s — so it **exits 0 and prints "task
submitted"** (INFRA-023). An operator setting up or rebuilding a crontab from the authoritative
document therefore installs an entry under which every 1 January long-horizon hydrograph rebuild
is silently skipped, with a cron log that reads as successful.

## Second divergence in the same block

`doc/deployment.md:948` heads the snow entry:

```
# Yearly snow norm/stat recalculation at 02:00 UTC on January 1
```

while the cron expression on `:952` is `0 2 31 8` (31 August) and the prose immediately below it
records the 2026-08-19 owner decision to move to 31 August. The schedule is right; the comment
above it is stale. Harmless to the machine, but it is the line an operator skims when auditing.

**Scope note:** these are the two divergences confirmed by direct comparison. This issue does not
claim the two blocks are otherwise identical — the fix should reconcile the block as a whole, not
patch these two lines and re-assert sync.

## Proposed fix (not implemented here)

Pick one owner for the cron block and make the other a pointer, rather than maintaining two copies
that drift:

- **Preferred**: make `doc/prod/update_deployment_checklist.md` §2.5 the single source of truth for
  the canonical block (it is the document operators actually execute, and it already carries the
  retired-entries table and the grep checks), and reduce `doc/deployment.md`'s block to a link.
- **Or**: correct `doc/deployment.md` and keep the "kept in sync" claim, accepting the duplication.

Either way, the sentence at `update_deployment_checklist.md:715` must end up true. A claim that two
documents are in sync is itself a maintenance burden; do not restate it unless something enforces it.

## Acceptance criteria

- `grep -n 'monthly_norms' doc/deployment.md` returns no cron *prescription* — a mention inside a
  "retired, do not use" note is acceptable and preferable.
- `doc/deployment.md`'s yearly long-horizon entry, if it still exists as a block, matches entry (9)
  of the checklist's canonical block, including `bin/yearly_runoff_hydrograph_aggregation.sh`.
- The snow entry's comment header and its cron expression agree on 31 August.
- No document claims to be "kept in sync" with another unless it is, at the moment of the change.
- `doc/prod/kghm_pipeline_handover.md` §3 step 1 and §5, which currently warn operators about this
  exact discrepancy, are updated or removed once it no longer exists.
