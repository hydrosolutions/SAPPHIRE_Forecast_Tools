# SAPPHIRE Postgres Backup and Restore

This guide is written for the hydromet sysadmin running SAPPHIRE on their
own server. It covers how to take regular dumps of the four SAPPHIRE
Postgres databases, store them safely, and restore from them when needed.

---

## 1. Why this matters

SAPPHIRE stores all operational forecast data — runoff, meteo, skill
metrics, forecast results, users — in Postgres. A single disk failure, a
bad migration that corrupts a table, an accidental run of
`bin/reset_sapphire_db.sh` (which destroys the DB volumes by design), or
plain human error (`DROP TABLE`) will wipe that data permanently unless
you have a recent backup. This document exists so every deployment has a
turnkey way to take and restore backups without having to improvise.

---

## 2. What is and isn't backed up by this script

**Backed up** by `bin/backup_sapphire_db.sh`:

- Postgres database `preprocessing` (container `sapphire-preprocessing-db`)
- Postgres database `postprocessing` (container `sapphire-postprocessing-db`)
- Postgres database `user` (container `sapphire-user-db`)
- Postgres database `auth` (container `sapphire-auth-db`)

**Not backed up** by this script — you must handle these separately:

- The country-specific data directory (e.g. `<country>_data_forecast_tools/`)
  containing CSV inputs, config, templates, intermediate data, reports.
- Any `.env` files (`sapphire/.env`, app-level env files).
- The `crontab` of the user running the pipeline.
- Luigi daemon state (`/var/lib/luigi` or wherever `luigid` persists).
- Docker images (rebuildable from the repo) and the `sapphire/` source tree
  itself — use `git` for that.

For the items above, a nightly `tar` to an off-site location is usually
sufficient. For example:

```bash
tar czf /var/backups/sapphire/data_$(date +%F).tar.gz \
    /path/to/<country>_data_forecast_tools \
    /path/to/sapphire/.env
```

---

## 3. Prerequisites

- Docker is running and the SAPPHIRE DB containers
  (`sapphire-preprocessing-db`, `sapphire-postprocessing-db`,
  `sapphire-user-db`, `sapphire-auth-db`) are up.
- `sapphire/.env` exists and contains `POSTGRES_USER`, `POSTGRES_PASSWORD`,
  `PREPROCESSING_DB`, `POSTPROCESSING_DB`, `USER_DB`, `AUTH_DB`.
- The backup directory (default `/var/backups/sapphire`) exists and is
  writable by whichever user runs the script / cron job. Create it once:

  ```bash
  sudo mkdir -p /var/backups/sapphire
  sudo chown "$USER" /var/backups/sapphire
  ```

- You run the script from the repository root (the parent of `sapphire/`).

---

## 4. Running a manual backup

From the repo root:

```bash
bash bin/backup_sapphire_db.sh
```

Expected output (colours stripped):

```
========================================
 SAPPHIRE Database Backup
========================================
[2026-04-17 02:00:01] Backup directory: /var/backups/sapphire
[2026-04-17 02:00:01] Retention days:   30
[2026-04-17 02:00:01] Backup start: db=<your-db-name> container=sapphire-preprocessing-db file=/var/backups/sapphire/<your-db-name>_2026-04-17_020001.dump
[2026-04-17 02:00:42] Backup done: db=<your-db-name> file=/var/backups/sapphire/<your-db-name>_2026-04-17_020001.dump size=124.3MB
... (three more DBs) ...
[2026-04-17 02:03:15] Pruned 0 old dump(s).
========================================
 BACKUP SUMMARY
========================================
[2026-04-17 02:03:15] Succeeded: 4 (<preprocessing-db> <postprocessing-db> <user-db> <auth-db>)
[2026-04-17 02:03:15] All four dumps succeeded and verified.
```

Inspect what was produced:

```bash
ls -lh /var/backups/sapphire/
```

You should see four files named `<db_name>_<YYYY-MM-DD_HHMMSS>.dump`,
each non-empty. If any dump failed, you'll instead see a file with a
`.FAILED` suffix — investigate before the next run.

### 4.1 Useful flags

| Flag | Default | Purpose |
|------|---------|---------|
| `-d, --backup-dir PATH` | `/var/backups/sapphire` | Write dumps to a different location (e.g. a mounted external drive). |
| `-r, --retention-days N` | `30` | Delete `.dump` files older than N days at the end of the run. `0` keeps everything. |
| `--dry-run` | off | Log what would happen; run no `pg_dump` and delete nothing. |
| `-h, --help` | — | Show usage. |

### 4.2 Testing the script

Before trusting it in cron, confirm it at least parses your environment:

```bash
bash bin/backup_sapphire_db.sh --dry-run
```

A dry run prints the `docker exec` and `find` commands it *would* run,
without actually dumping anything. After that, run it for real once and
inspect the first produced file:

```bash
bash bin/backup_sapphire_db.sh
ls -lh /var/backups/sapphire/
```

To sanity-check one of the dumps manually:

```bash
docker exec -i sapphire-preprocessing-db \
    pg_restore --list /dev/stdin \
    < /var/backups/sapphire/<your-db-name>_2026-04-17_020001.dump \
    | head -20
```

You should see a list of tables, sequences and indexes. If the output is
empty or `pg_restore` errors, the file is not a valid archive.

---

## 5. Scheduling daily backups with cron

Run as the user that owns the backup directory (NOT as root, unless the
directory is root-owned). Edit the crontab with `crontab -e` and add:

```cron
# SAPPHIRE nightly DB backup at 02:00 local time
0 2 * * * cd /home/ubuntu/SAPPHIRE_forecast_tools && /bin/bash bin/backup_sapphire_db.sh >> /home/ubuntu/logs/sapphire_backup.log 2>&1
```

Create the log directory once:

```bash
mkdir -p /home/ubuntu/logs
```

After a week, glance at the log and `/var/backups/sapphire/` to confirm
dumps are arriving and rotating.

---

## 6. Retention recommendation

- **On-server default**: 30 days of daily dumps. The script's default
  `--retention-days 30` handles this automatically.
- **Off-site copy**: at least weekly, to something that is *not* the same
  server. Pick one of the options below based on what your hydromet has
  available.

Off-site options (pick one — the choice is yours):

**Option A — `rsync` to another server over SSH** (common when the
hydromet has a secondary machine):

```bash
# Weekly (Sundays 03:00)
0 3 * * 0 rsync -az --delete /var/backups/sapphire/ backupuser@backup-host:/srv/sapphire_backups/
```

**Option B — `aws s3 sync` to an S3 bucket** (for deployments with cloud
access):

```bash
# Weekly (Sundays 03:00)
0 3 * * 0 aws s3 sync /var/backups/sapphire/ s3://<your-bucket>/sapphire/ --delete
```

Configure `~/.aws/credentials` for the user running cron, and restrict
the IAM policy to `PutObject`/`DeleteObject` on that bucket prefix.

**Option C — physical external drive** (air-gapped deployments). Mount
the drive monthly, `rsync -a /var/backups/sapphire/ /mnt/usb/`,
unmount, store the drive off-site. Document the rotation schedule in
your local runbook.

Whichever option you choose, verify at least once that a file copied
off-site is readable and restorable on a different machine. Untested
backups are not backups.

---

## 7. Restoring from a backup

Restore is a destructive operation on the target database. Read the whole
section before running any command, and keep a copy of the current dump
in a safe place first.

The four databases are independent — you can restore one without
touching the others. Replace `<your-db-name>` with the actual database
name from `sapphire/.env`, and `<timestamp>` with the dump file you
chose.

### 7.1 Stop the service that talks to the target DB

| Target DB | Service to stop | Command |
|-----------|-----------------|---------|
| preprocessing | `preprocessing-api` | `docker compose -f sapphire/docker-compose.yml stop preprocessing-api` |
| postprocessing | `postprocessing-api` | `docker compose -f sapphire/docker-compose.yml stop postprocessing-api` |
| user | `user-api` and `auth-api` | `docker compose -f sapphire/docker-compose.yml stop user-api auth-api` |
| auth | `auth-api` | `docker compose -f sapphire/docker-compose.yml stop auth-api` |

The DB container itself stays up — `pg_restore` needs to connect to it.

### 7.2 Drop and recreate the target DB

`pg_restore` expects an empty target. The cleanest way is to drop the DB
and recreate it:

```bash
# Read the Postgres credentials and DB names from the .env so you don't
# hand-type the password. Replace with the var name of the DB you are
# restoring: PREPROCESSING_DB, POSTPROCESSING_DB, USER_DB, or AUTH_DB.
set -a; source sapphire/.env; set +a

# Example: restoring the preprocessing DB
CONTAINER=sapphire-preprocessing-db
TARGET_DB="${PREPROCESSING_DB}"

docker exec -e PGPASSWORD="${POSTGRES_PASSWORD}" "${CONTAINER}" \
    psql -U "${POSTGRES_USER}" -d postgres \
    -c "DROP DATABASE IF EXISTS \"${TARGET_DB}\";"

docker exec -e PGPASSWORD="${POSTGRES_PASSWORD}" "${CONTAINER}" \
    psql -U "${POSTGRES_USER}" -d postgres \
    -c "CREATE DATABASE \"${TARGET_DB}\" OWNER \"${POSTGRES_USER}\";"
```

If the `DROP DATABASE` fails with "database is being accessed by other
users", make sure you stopped the service in step 7.1, then retry.

### 7.3 Restore from the `.dump` file

```bash
DUMP_FILE=/var/backups/sapphire/<your-db-name>_<timestamp>.dump

docker exec -i -e PGPASSWORD="${POSTGRES_PASSWORD}" "${CONTAINER}" \
    pg_restore -U "${POSTGRES_USER}" -d "${TARGET_DB}" \
    --no-owner --no-privileges \
    < "${DUMP_FILE}"
```

`--no-owner --no-privileges` makes restore tolerant of differences
between the dump's original role/privileges and the target cluster —
harmless here since all four DBs are owned by the same Postgres user.

Expect a handful of warnings about extensions (`plpgsql`) or comments;
they are safe to ignore. A real failure shows as a non-zero exit status
with an error line.

### 7.4 Restart services and verify health

```bash
docker compose -f sapphire/docker-compose.yml up -d
curl -sf http://localhost:8000/health && echo OK
curl -sf http://localhost:8000/health/ready && echo READY
```

Then spot-check an API endpoint that reads from the restored DB, e.g.:

```bash
# Preprocessing: list a handful of runoff records
curl -s "http://localhost:8000/api/preprocessing/runoff/?limit=1" | head

# Postprocessing: list a skill metric
curl -s "http://localhost:8000/api/postprocessing/skill-metric/?limit=1" | head
```

If the dashboard is running, load the forecast page and confirm data
appears for a known station (e.g. `19999`).

---

## 8. Quarterly restore drill

Backups that have never been restored cannot be trusted. Every quarter
(pick a date, put it in your calendar), do the following. Total time:
about 15 minutes.

1. Copy the latest successful dump to a scratch location.
2. Create a scratch DB in the same container and restore into it, e.g.:

   ```bash
   # Preprocessing example — use a throwaway DB name
   SCRATCH=preprocessing_restore_test
   docker exec -e PGPASSWORD="${POSTGRES_PASSWORD}" sapphire-preprocessing-db \
       psql -U "${POSTGRES_USER}" -d postgres -c "CREATE DATABASE ${SCRATCH};"
   docker exec -i -e PGPASSWORD="${POSTGRES_PASSWORD}" sapphire-preprocessing-db \
       pg_restore -U "${POSTGRES_USER}" -d "${SCRATCH}" --no-owner --no-privileges \
       < /var/backups/sapphire/<your-db-name>_<timestamp>.dump
   ```

3. Compare a row count between scratch and production for one or two
   tables you care about:

   ```bash
   docker exec -e PGPASSWORD="${POSTGRES_PASSWORD}" sapphire-preprocessing-db \
       psql -U "${POSTGRES_USER}" -d "${SCRATCH}" -c "SELECT count(*) FROM runoff;"
   docker exec -e PGPASSWORD="${POSTGRES_PASSWORD}" sapphire-preprocessing-db \
       psql -U "${POSTGRES_USER}" -d "${PREPROCESSING_DB}" -c "SELECT count(*) FROM runoff;"
   ```

4. Drop the scratch DB:

   ```bash
   docker exec -e PGPASSWORD="${POSTGRES_PASSWORD}" sapphire-preprocessing-db \
       psql -U "${POSTGRES_USER}" -d postgres -c "DROP DATABASE ${SCRATCH};"
   ```

If row counts differ wildly or the restore errors, investigate before
the next scheduled drill — your backup strategy has a hole.

---

## 9. Known limitations

- **Point-in-time consistency across the four DBs is not guaranteed.**
  The script dumps each database sequentially without stopping the
  services. Writes that happen during the ~minute-long dump window may
  leave a small amount of drift between, say, a forecast row in
  `postprocessing` and the runoff row in `preprocessing` it was
  computed from. For routine operational use this is acceptable —
  SAPPHIRE writes are infrequent and idempotent, and a rerun of the
  affected pipeline step fills any gap.

- **If you need stronger guarantees**, stop the API services for the
  ~1 minute the dump takes:

  ```bash
  docker compose -f sapphire/docker-compose.yml stop \
      preprocessing-api postprocessing-api user-api auth-api
  bash bin/backup_sapphire_db.sh
  docker compose -f sapphire/docker-compose.yml start \
      preprocessing-api postprocessing-api user-api auth-api
  ```

  *Future work:* a `--stop-services` flag on the backup script that
  orchestrates the above automatically. Not implemented today — file an
  issue if you want it.

- **Dumps are not encrypted at rest.** If the server or the off-site
  destination is in an environment where Postgres data needs to be
  encrypted, either encrypt the backup directory (LUKS on Linux) or
  pipe the dump through `gpg` before writing it. That is out of scope
  for the current script.
