# DOC-008: Documented `backup_sapphire_db.sh` invocations omit `-e` and the required working directory

**Status**: Draft (2026-08-26)
**Module**: `doc/prod/first_deploy_checklist.md`, `doc/prod/update_deployment_checklist.md`,
`doc/deployment.md`
**Priority**: **Medium** — a nightly backup that aborts leaves no backup, and cron shows nobody the
error. Not raised higher only because it is unconfirmed whether the default env path resolves on a
real deployment (see "What is unresolved").
**Labels**: `documentation`, `deployment`, `backup`, `silent-failure`
**Found**: 2026-08-26, out-of-loop review of the DOC-007 diff. Filed rather than fixed inline: this
is a backup-invocation defect, not the cron-block drift DOC-007 covers.
**Related**: **DOC-007** (`review_gi_draft_doc_deployment_cron_block_stale_authority.md`) — found
while fixing it; the retention divergence recorded there is a *different* axis of the same command.

---

## Two divergences in how the backup script is invoked

**1. The `-e` flag.** `bin/backup_sapphire_db.sh` defaults `ENV_FILE` to `${COMPOSE_DIR}/.env`
(`:48`, i.e. `sapphire/.env`) and aborts with `Env file not found` (`:157-158`) if it is absent.

| Document | Invocation | Env file |
|---|---|---|
| `update_deployment_checklist.md:787`, `:158`, `:949`, `:1384` | `-e ${ENV_FILE_PATH}` | explicit |
| `deployment.md:896` | `--env-file /data/<data_folder>/config/<env_file>` | explicit |
| `first_deploy_checklist.md:797`, `:899` | *(neither flag)* | falls back to `sapphire/.env` |

The first-deploy checklist is the only one relying on the default. Its own env-file guidance
(`:390`) describes `${ENV_FILE_PATH}`, an external path, not `sapphire/.env`.

**2. The working directory.** The script resolves `sapphire/docker-compose.yml` relatively and
aborts with `Must run from the repository root (parent of sapphire/)` (`:149-151`) otherwise. The
cron rows in every document correctly prefix `cd /data/SAPPHIRE_Forecast_Tools &&`. The **manual**
invocations do not — `first_deploy_checklist.md:899` and `update_deployment_checklist.md:949` both
call the script by absolute path with no `cd`, so they abort unless the operator happens to already
be at the repository root.

## What is unresolved — do not fix by guessing

Whether the missing `-e` actually breaks anything depends on facts not established here:

- Does a working deployment have `sapphire/.env`? The compose stack reads it, so it very likely
  exists — in which case the first-deploy cron row works by accident.
- If it exists, does it carry `POSTGRES_USER`/`POSTGRES_PASSWORD` for **all four** databases, or
  only what compose needs? The script needs the former.

If `sapphire/.env` is always present and complete, the `-e` divergence is cosmetic and the fix is to
make the docs consistent. If it is not, the first-deploy checklist has been documenting a nightly
backup that aborts. **Establish which before changing anything** — an operator following a "fixed"
command that is wrong in the other direction is no better off.

## Proposed fix (not implemented here)

- Pass `-e` explicitly in every documented invocation, matching what the update checklist already
  does and what `:160` already explains. Explicit beats relying on a default that resolves
  differently per deployment.
- Give the two manual invocations the same `cd /data/SAPPHIRE_Forecast_Tools &&` prefix their cron
  counterparts have, or make the script resolve its compose path relative to its own location
  instead of the caller's working directory (the more durable fix, but it is a code change and
  belongs in its own issue).

## Acceptance criteria

- Every documented `backup_sapphire_db.sh` invocation, cron and manual, in all three documents,
  either passes an explicit env file or is demonstrated not to need one.
- Every documented manual invocation runs successfully from a working directory other than the
  repository root, or carries the `cd` that makes it work.
- A dry run of each documented command on a real deployment produces a backup file, verified by
  listing the target directory — not merely a zero exit status.
- The retention divergence recorded in DOC-007 is untouched by this issue unless the owner folds
  the two together.
