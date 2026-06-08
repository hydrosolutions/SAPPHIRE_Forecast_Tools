# P7 — Final runbook stitching + deployment checklist cross-link

**Phase:** P7 of the update-time migration toolkit (last phase).
**Status:** Implementation complete; under review.
**Branch:** `feature_p7_final_runbook` → `develop_migration_toolkit`.
**Depends on:** P0 + P1a + P1b + P1c + snow port + P2a + P2b + P3 + P4a + P4b + P5 + P6 (all merged).
**Blocks:** sprint close-out.

## Goal

Fill the three remaining runbook placeholders (§8 acceptance SQL, §9
failure recovery, §10 rollback) and cross-link the deployment
checklist to the migration runbook. After P7 the operator has a
complete ten-section runbook end-to-end + a clear pointer from the
deployment checklist to walk through when historical data migration
is needed.

## Scope (file-level)

```
doc/prod/update_data_migration_runbook.md                                    (MODIFY — §8/§9/§10 fill)
doc/prod/update_deployment_checklist.md                                      (MODIFY — new §3.4 cross-link)
doc/plans/issues/high_prio_gi_draft_update_migration_p7_final_runbook.md     (this file)
```

No code changes. No edits to `sapphire/services/`. No edits to existing
§1–§7 of the runbook (P7 fills placeholders only).

## What landed

### Runbook §8 — Acceptance SQL

Two consolidated `docker exec sapphire-*-db psql <<SQL ... SQL` blocks
covering all data families:

- **§8.1 Preprocessing DB**: runoff (DAY/PENTAD/DECADE), hydrograph
  (DAY/PENTAD/DECADE + MONTH/SEASON), meteo (T/P), snow values
  (HS/SWE/ROF), snow stats from P6 hook 1.
- **§8.2 Postprocessing DB**: lr_forecasts, ML forecasts (TFT/TiDE/TSMixer),
  long_forecasts (P5 configured modes), skill_metrics (P6 hooks 3+4).

Each query returns row count, distinct codes, non-NULL counts for the
canonical operational fields, and min/max date.

§8.3 "How to interpret results" walks the operator through the four
signals — row counts, distinct codes, non-NULL counts, date range — and
points at §9 when anything is off.

Note: per-section acceptance queries already exist inline in §5/§6/§7
(one per wrapper). §8 is the consolidated end-to-end view, not a
duplicate; the operator runs §8 ONCE after the full migration sweep.

### Runbook §9 — Failure recovery and rerun

Five-tier recovery ladder:

- **§9.1 First-line: rerun the same wrapper.** Table of per-family
  upsert keys so the operator can verify which natural key drives
  idempotency. A clean rerun produces identical dry-run output and
  zero new rows.
- **§9.2 Narrow + retry on transient failures.** `--station-filter`
  for CSV-source wrappers; `--start-date`/`--end-date` on the DB-source
  laptop export then re-run the server wrapper. Reduce `--batch-size`
  from default 500 to 100 or 50 on stressed deployments.
- **§9.3 Scoped purge.** Operator-defined `DELETE` for the narrow case
  of wrong data POSTed before a fix landed. Explicit warning against
  unscoped `DELETE` / `TRUNCATE`; falls back to §10 if scope is
  uncertain.
- **§9.4 Cron-pause failure mid-migration.** Recovery for the round-3
  monitoring signal (`exit 1` + "cron restore FAILED" log line):
  grep the log for the backup path, `crontab "<that path>"`, verify.
  Confirms the backup lives in LOG_DIR per the round-3 contract.
- **§9.5 Last-resort.** Pointer to §10 for full `pg_restore`.

### Runbook §10 — Rollback and cleanup

Eight-step rollback procedure with literal `pg_restore` commands:

- §10.1 Confirm the §4.1 backup (`All four dumps succeeded and verified`
  log line + 4 dump files on disk). Hard gate.
- §10.2 Stop wrappers + confirm cron is paused.
- §10.3 Rollback `preprocessing_db` (stop API, terminate backends, DROP+CREATE,
  `pg_restore --no-owner --no-privileges --exit-on-error`, restart API,
  healthz check).
- §10.4 Rollback `postprocessing_db` — same shape.
- §10.5 Verify with §8.
- §10.6 Restore cron from `$BACKUP_DIR/crontab_backup.txt` (§4.2 snapshot).
- §10.7 Final cleanup (`rm -rf <data_root>/logs/*_tmp/` after operator review).
- §10.8 SIGKILL / power-loss fallback (v2 R3) — orphaned umh workspaces
  recovery path.

`user_db` and `auth_db` are NOT restored — they're untouched by any
wrapper in §5/§6/§7. Restoring them would log out every operator
without benefit.

### Deployment checklist cross-link (new §3.4)

New §3.4 "Historical Data Migration (one-time, post-update)" added
after §3.3 Test Forecast Run, before §4 LOG CLEANUP. Three subsections:

- **When to run**: initial deployment OR deliberate refresh after a
  forecast-tools schema change. Routine image-tag bumps do NOT need it.
- **Section-by-section index of the 10 runbook sections** so the
  operator can skim before committing to the run.
- **Order of operations**: healthy stack (§1–3.3 of checklist) → §4.1
  pg_dump (runbook) → §4.2 cron pause → §5–7 wrappers → §8 acceptance →
  resume cron. Pointer to §9/§10 on failure.

Architecture's P7 acceptance criterion was "checklist links runbook
after Alembic". The `develop_migration_toolkit` checklist has no
Alembic section (added on a different branch). §3.4 is the natural
equivalent location: post-verification, pre-cleanup.

## What did NOT land + why

**Architecture phase-graph annotation with shipped PR numbers.**
`doc/plans/working/update_migration_toolkit_architecture.md` is not on
`develop_migration_toolkit` — it lives only on the user's local working
branch. Updating it from this PR would require pulling in the full
1000+ line architecture document as scope creep.

Resolution: the architecture document is a working/planning artifact
that the user maintains outside the migration-toolkit branch. The
sprint commit history on `develop_migration_toolkit` (15 phase + fix
PRs merged) is itself the canonical record of what shipped. A
post-sprint memo on the user's local working branch can annotate the
plan once. Not a blocker for P7 merge.

## Acceptance criteria

- [x] Runbook §8 / §9 / §10 placeholders all replaced with concrete
      operator-facing content.
- [x] §8 acceptance SQL is non-destructive (read-only `SELECT`) and
      covers all 8 data tables (snow, snow_data, runoffs, meteo,
      hydrographs, lr_forecasts, forecasts, long_forecasts) + skill_metrics.
- [x] §10 rollback uses the literal `pg_restore` form expected by Stage
      E item #1; covers both preprocessing_db + postprocessing_db; user_db
      / auth_db are explicitly excluded.
- [x] §10.8 SIGKILL / power-loss fallback documented (v2 R3).
- [x] Deployment checklist §3.4 cross-link to the migration runbook;
      "when to run" gating + per-section index + order of operations.
- [x] Architecture phase-graph annotation: NOT done (out-of-branch);
      documented in "What did NOT land" section above.
- [x] No real station codes (sentinel `19999` only in example SQL).
- [x] No edits to `sapphire/services/`.
- [x] No edits to existing runbook §1–§7 content (placeholders only).

## Reviewer checklist

- [ ] Each `psql` block in §8 uses `-P pager=off` + heredoc + `psql` is
      invoked via `docker exec -i sapphire-*-db` (matching the prior
      per-phase acceptance fragments).
- [ ] §10 `pg_restore` command shape is correct (custom-format dumps
      from `bin/backup_sapphire_db.sh` use `pg_restore --no-owner
      --no-privileges --exit-on-error` against `pg_dump --format=custom
      --compress=6`).
- [ ] §9.1 per-family upsert key table matches the actual upsert keys
      in `sapphire/services/{preprocessing,postprocessing}/app/data_migrator.py`
      (read-only cross-check).
- [ ] §9.4 backup-path glob matches the round-3 LOG_DIR contract from
      P6 (file lives at `logs/regenerate_hooks/crontab_backup_<TS>.txt`,
      NOT under the umh `_tmp` workspace).
- [ ] Checklist §3.4 "When to run" gating is unambiguous (routine bumps
      do NOT need migration; initial deployment AND schema-change
      refreshes DO).

## Test plan summary

P7 is pure documentation — no test surface beyond CI markdown linting
(if any) + manual operator-walkthrough sanity check on a fresh server.

The acceptance SQL blocks were derived from the per-section fragments
already present in §5/§6/§7 (committed under prior phase PRs that
passed CI and were operator-reviewed). No new SQL was invented — §8
consolidates queries the wrappers' own acceptance steps already use.

## Out of scope (recorded for follow-up)

1. **Architecture phase-graph annotation.** See "What did NOT land".
2. **Top-level operator walkthrough.** Some toolkits ship a one-page
   "do this, then this, then this" walkthrough at the front of the
   runbook. The current §1 ("Purpose, scope, anti-goals") fulfills
   this role but could be supplemented with a numbered "Day-of
   migration cheat sheet" pointing at §4–§10 sub-steps. Not done
   here because §1 already covers the orientation a careful operator
   needs.
3. **`crontab_backup_<TS>.txt` retention policy.** The round-3 contract
   leaves the backup on disk for operator-review after restore failure
   or `--allow-unpaused-cron` bypass; no automatic cleanup. Runbook
   §10.7 has the operator-glob `rm -rf logs/*_tmp/` but the
   `logs/regenerate_hooks/crontab_backup_*.txt` files persist
   indefinitely. A future follow-up could add an age-based cleanup
   (e.g. 30-day retention) but the round-3 reviewer deferred this
   ("operator-review artifacts; document not auto-delete").

## References

- Runbook: `doc/prod/update_data_migration_runbook.md` (now complete §1–§10).
- Deployment checklist: `doc/prod/update_deployment_checklist.md` (new §3.4).
- Backup helper: `bin/backup_sapphire_db.sh` (canonical `pg_dump` shape).
- Round-3 backup-lifetime contract (referenced from §9.4 + §10.8):
  `doc/plans/issues/high_prio_gi_draft_update_migration_p6_regenerate_hooks.md`.
- Sprint commit history: PRs #343, #345, #346, #347, #348, #349, #350,
  #351, #352, #353, #354, #355 — all merged onto `develop_migration_toolkit`.
