## Runbook §10 rollback uses wrong dump filename glob — silent data loss on operator rollback

**Status**: Complete — fixed and merged 2026-06-10 (PR #359, commit `a1e0dfb6`); all acceptance criteria below re-verified on trunk 2026-08-18
**Module**: `doc/prod/update_data_migration_runbook.md` (§10 rollback procedure)
**Priority**: **High** (data-loss class — operator follows the procedure verbatim → live DB destroyed, no restore)
**Labels**: `runbook`, `rollback`, `data-loss`, `migration-toolkit`, `tjhm`, `P7-followup`
**Discovered**: 2026-06-08 during Tajik runbook walkthrough — found while investigating the backup → reset → restore loop for a Mode A rehearsal
**Related**:
- PR #356 (P7 — final runbook §8/§9/§10) merged after 3 review rounds with live-DB verification of §8 SQL; §10 was NOT exercised end-to-end during review
- `bin/backup_sapphire_db.sh:206` — the authoritative source for the actual filename format
- Adjacent finding MIG-001 (migration dry-run station-code leak) — also discovered during the same
  walkthrough. **Dangling reference (noted 2026-08-18)**: the linked file
  `high_prio_gi_draft_migration_dry_run_station_code_leak.md` was never created on any branch and
  MIG-001 has no row in `module_issues.md`, so the link is removed here. Two other issue files
  (`high_prio_gi_draft_migration_horizon_type_case_coercion.md`,
  `low_prio_gi_draft_lt_config_strip_on_split.md`) also cite MIG-001 — the id is referenced but unfiled.

---

## Summary

`doc/prod/update_data_migration_runbook.md` §10 rollback procedure references dump filenames as `sapphire-<container_name>_<TS>.dump` (using the docker **container** name with dashes). The actual filenames written by `bin/backup_sapphire_db.sh` are `<db_name>_<TS>.dump` (using the **database** name with underscores).

If an operator follows §10 verbatim during a real rollback, the chain is:

1. `ls -1 "$BACKUP_DIR"/sapphire-preprocessing-db_*.dump | head -1` → matches no files → `PREPROCESSING_DUMP=""` (empty)
2. `DROP DATABASE IF EXISTS preprocessing_db; CREATE DATABASE preprocessing_db ...` → **destroys the live preprocessing_db**
3. `pg_restore -U ... -d preprocessing_db ... < ""` → restore command fails before pg_restore runs (shell exits 1 on `< ""` empty-path redirection); without fail-fast shell handling (`set -e`), the block can continue after the DB has already been recreated empty
4. `docker compose start preprocessing-api` → restarts the API against an empty DB
5. Operator runs §10.5 §8 acceptance SQL → row counts are zero, operator realises a few minutes later that production data is gone

This is the worst possible failure mode for a rollback procedure: the operator has explicitly chosen rollback because the migration went wrong. They are already in a high-stress state, trying to restore a known-good baseline. The runbook silently destroys that baseline.

---

## Evidence (reproduced 2026-06-08)

Existing backup from this session's §4.1 run:

```bash
$ ls -lh /tmp/sapphire_walkthrough_backups/pre_update_migration_20260608T120100Z/
-rw-r--r--   13K   auth_db_2026-06-08_140121.dump
-rw-r--r--   54M   postprocessing_db_2026-06-08_140113.dump
-rw-r--r--   86M   preprocessing_db_2026-06-08_140101.dump
-rw-r--r--  8.6K   user_db_2026-06-08_140120.dump
```

Runbook §10.3 verbatim glob:

```bash
$ ls -1 "$BACKUP_DIR"/sapphire-preprocessing-db_*.dump
zsh: no matches found: ...sapphire-preprocessing-db_*.dump
$ echo "PREPROCESSING_DUMP=''" # result of head -1 over empty list
```

Corrected glob:

```bash
$ ls -1 "$BACKUP_DIR"/preprocessing_db_*.dump
.../preprocessing_db_2026-06-08_140101.dump
```

---

## Root cause — exact file:line citations

`doc/prod/update_data_migration_runbook.md`:

| Line | Wrong text | Correct text |
|------|------------|--------------|
| 2527 | `#   sapphire-preprocessing-db_<TS>.dump` | `#   preprocessing_db_<TS>.dump` |
| 2528 | `#   sapphire-postprocessing-db_<TS>.dump` | `#   postprocessing_db_<TS>.dump` |
| 2529 | `#   sapphire-user-db_<TS>.dump` | `#   user_db_<TS>.dump` |
| 2530 | `#   sapphire-auth-db_<TS>.dump` | `#   auth_db_<TS>.dump` |
| 2564 | `ls -1 "$BACKUP_DIR"/sapphire-preprocessing-db_*.dump` | `ls -1 "$BACKUP_DIR"/preprocessing_db_*.dump` |
| 2599 | `ls -1 "$BACKUP_DIR"/sapphire-postprocessing-db_*.dump` | `ls -1 "$BACKUP_DIR"/postprocessing_db_*.dump` |

Authoritative source (`bin/backup_sapphire_db.sh:210`):

```bash
local out_file="${BACKUP_DIR}/${db_name}_${timestamp}.dump"
```

`db_name` is the literal database name (`preprocessing_db`, `postprocessing_db`, `user_db`, `auth_db`) — not the container name.

---

## Fix

Single targeted edit to the runbook. Six line changes, no code changes.

**Recommended additional hardening:** wrap the `ls` in a guard so the operator gets a hard error if the glob produces nothing, instead of silently proceeding to DROP DATABASE:

```bash
# §10.3 hardened
PREPROCESSING_DUMP=$(ls -1 "$BACKUP_DIR"/preprocessing_db_*.dump 2>/dev/null | head -1)
[[ -n "$PREPROCESSING_DUMP" && -s "$PREPROCESSING_DUMP" && -r "$PREPROCESSING_DUMP" ]] \
    || { echo "FATAL: no readable non-empty preprocessing_db dump found in $BACKUP_DIR; refusing to DROP+CREATE empty DB"; exit 1; }
echo "Restoring from: $PREPROCESSING_DUMP"
```

Test predicates: `-n` (variable non-empty), `-s` (file exists and non-empty — protects against truncated/zero-byte dumps that `-f` would silently accept), `-r` (file is readable — protects against permission-denied paths that would otherwise let pg_restore proceed with empty input). Reviewer R3 verified that `-f` alone passes both zero-byte and unreadable files on the live stack. Same pattern for `POSTPROCESSING_DUMP` in §10.4.

---

## Tests / acceptance

### Required smoke test (manual)

After the runbook fix, perform a full backup→reset→restore loop on a non-production stack and confirm row counts match pre-state:

```bash
# 1. Capture baseline counts for ALL migration-relevant table families.
#    Preprocessing: runoffs, hydrographs, meteo, snow.
#    Postprocessing: forecasts, long_forecasts, lr_forecasts, skill_metrics.
docker exec sapphire-preprocessing-db psql -U postgres -d preprocessing_db -tAc "
  SELECT 'runoffs:'    || COUNT(*) FROM runoffs    UNION ALL
  SELECT 'hydrographs:'|| COUNT(*) FROM hydrographs UNION ALL
  SELECT 'meteo:'      || COUNT(*) FROM meteo      UNION ALL
  SELECT 'snow:'       || COUNT(*) FROM snow ORDER BY 1
" > /tmp/baseline_preprocessing.txt
docker exec sapphire-postprocessing-db psql -U postgres -d postprocessing_db -tAc "
  SELECT 'forecasts:'      || COUNT(*) FROM forecasts      UNION ALL
  SELECT 'long_forecasts:' || COUNT(*) FROM long_forecasts UNION ALL
  SELECT 'lr_forecasts:'   || COUNT(*) FROM lr_forecasts   UNION ALL
  SELECT 'skill_metrics:'  || COUNT(*) FROM skill_metrics  ORDER BY 1
" > /tmp/baseline_postprocessing.txt

# 2. Backup per §4.1
bash bin/backup_sapphire_db.sh -d "$BACKUP_DIR" -r 0

# 3. Reset (or modify) the DBs
bash bin/reset_sapphire_db.sh --preprocessing-only -y

# 4. Restore per §10.3 + §10.4 (now corrected)
# ... run §10.3 + §10.4 procedures verbatim ...

# 5. Confirm counts match for every table family (equivalent to re-running
#    the §8 acceptance SQL and comparing to the pre-rollback baseline).
docker exec sapphire-preprocessing-db psql -U postgres -d preprocessing_db -tAc "
  SELECT 'runoffs:'    || COUNT(*) FROM runoffs    UNION ALL
  SELECT 'hydrographs:'|| COUNT(*) FROM hydrographs UNION ALL
  SELECT 'meteo:'      || COUNT(*) FROM meteo      UNION ALL
  SELECT 'snow:'       || COUNT(*) FROM snow ORDER BY 1
" > /tmp/post_restore_preprocessing.txt
diff /tmp/baseline_preprocessing.txt /tmp/post_restore_preprocessing.txt \
  && echo "PASS: preprocessing identical" \
  || { echo "FAIL: preprocessing diverged"; exit 1; }
# Repeat the equivalent diff for postprocessing.
```

The diff-based comparison ensures every migration-relevant family is verified, not just one table per DB. Equivalent to re-running the runbook §8 acceptance SQL after rollback and confirming all family-level row counts match the pre-rollback baseline.

### Required gate

After the fix, the runbook §10 procedure must be exercised end-to-end on a disposable stack at least once before any future P7-class merge.

---

## Process note

The bug slipped through three reviewer-fix rounds of PR #356 (commit history shows three reviewer-fix rounds; visible GitHub review/comment metadata was empty in this checkout — `gh pr view 356` and the PR review/comment API returned empty arrays, so the rounds are reconstructible from commit messages, not from preserved review threads) because:

- All three rounds focused on §8 acceptance SQL (column names like `horizon_in_year` vs `horizon_value`, the `snow_data` table that doesn't exist, the `POSTGRES_USER` source path).
- §10 was reviewed for **shape and structure** (correct shell commands, correct use of `--no-owner --no-privileges --exit-on-error`, correct compose service vs container-name distinction in §10.5/§10.6) but never **executed end-to-end** against actual dump files.
- The wrong filename pattern is plausible-looking (the container name IS `sapphire-preprocessing-db`, so the operator wouldn't necessarily spot the mismatch by reading the runbook alone).

**Recommended follow-up for future P7-class reviews**: any rollback procedure must be exercised at least once on a disposable stack as a review gate. Static review is not sufficient for procedures that combine `DROP DATABASE` with file globs — the failure mode is silent and destructive.

---

## Acceptance criteria

- [x] All six citations corrected in `doc/prod/update_data_migration_runbook.md` (comment block now at `:2567-2570`; globs at `:2606` and `:2648`)
- [x] §10.3 and §10.4 `ls` commands wrapped in the hardened guard pattern (refuses to DROP if glob is empty) — `:2610` and `:2650`, testing `-n` / `-s` / `-r`
- [x] End-to-end smoke test executed: backup → reset → restore → row-count match, on the laptop's local Docker stack — recorded in the merge commit `a1e0dfb6` (user_db sanity loop, preprocessing-db full rehearsal 4.2M rows bit-for-bit, postprocessing-db restore). Evidence is the commit record; not independently re-run on 2026-08-18
- [x] PR description references this gi_draft and the discovery context — PR #359
- [x] No similar globs lurking elsewhere in the runbook (one-shot grep: `grep -nE "sapphire-(preprocessing|postprocessing|user|auth)-db_" doc/prod/update_data_migration_runbook.md` returns empty) — re-run 2026-08-18, still empty

**Verification note (2026-08-18)**: the "Required gate" above — exercising §10 end-to-end before any
future P7-class merge — is a *standing* gate for future work, not an open item of this issue.

---

## Out of scope

- Restructuring §10 into a single rollback script (could be a follow-up; current per-database `pg_restore` flow is fine once the glob is fixed).
- Adding a `--restore` flag to `bin/backup_sapphire_db.sh` (separate UX improvement; not required for this fix).
- Auditing the `bin/reset_sapphire_db.sh` script for similar filename-pattern bugs (separate scope).
