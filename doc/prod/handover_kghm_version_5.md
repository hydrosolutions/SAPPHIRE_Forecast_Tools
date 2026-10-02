# KGHM Handover — IT Checklist

**Situation:** the system has run in production on the KGHM server for over a year.
Forecasters are trained and use it daily.

This handover moves **system administration** from the Provider to KGHM IT. It is not a
deployment and not user training.

---

## Set these first

One value varies by deployment — the data directory name. Everything below is derived from
it. These exports last only for the current terminal session — re-run this block after
reconnecting.

```bash
read -rp "Data directory name (e.g. kyg_data_forecast_tools): " DD
export DATA_DIR="/data/$DD"
export ENV_FILE="$DATA_DIR/config/.env_develop_kghm"
export REPO=/data/SAPPHIRE_Forecast_Tools
ls -l "$ENV_FILE" && echo "OK"
```

---

## Chapter 1 — Does it work right now?

### 1.1 Run the health check

One command. It changes nothing, detects the real configuration rather than assuming the
documented one, and reports what is wrong.

```bash
cd "$REPO"
bash bin/handover_healthcheck.sh
```

It checks: deployment layout, containers, all service endpoints, data freshness, recent
pipeline failures, cron, monitoring, backups, the iEasyHydro connection, disk, and whether
the wrapper scripts report failures correctly.

Reading the output:

| Result | Meaning |
|---|---|
| `[ OK ]` | Nothing to do |
| `[WARN]` | Works, but needs a decision — fix it, or write down why it is accepted |
| `[FAIL]` | **Blocks sign-off.** Resolve or formally accept in writing |

Exit code is `0` when there are no failures, `1` otherwise — so it can be run from cron later.

Save the first run as the "before" reference:

```bash
bash bin/handover_healthcheck.sh > ~/handover_healthcheck_$(date +%F).txt 2>&1
```

If the env file is not auto-detected, pass it:
`--env-file "$ENV_FILE"`

### 1.2 Capture the as-running configuration

```bash
{
  echo "=== $(date '+%F %H:%M') on $(hostname), user $(whoami) ==="
  docker ps --filter name=sapphire- --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
  echo "--- cron (may belong to another account) ---"; crontab -l 2>/dev/null
  echo "--- systemd ---"; systemctl list-unit-files | grep -iE "docker-monitor|log.watcher|ieh|tunnel|sapphire"
  echo "--- images ---"; docker images 'mabesa/sapphire-*' --format '{{.Repository}}:{{.Tag}} {{.CreatedSince}}'
  echo "--- repo ---";  git -C "$REPO" log -1 --oneline
  echo "--- disk ---";  df -h / /var/lib/docker
} > ~/handover_snapshot.txt
cat ~/handover_snapshot.txt
```

---

## Chapter 2 — New logins and secret rotation

> Reconnected since **Set these first**? Re-run that block before anything below.

Take a backup first. `-r 0` keeps every dump — the scheduled job's `-r 30` would delete
anything older than 30 days, and it prunes even when the backup itself failed:

```bash
cd "$REPO"
bash bin/backup_sapphire_db.sh -d /var/backups/sapphire -r 0 -e "$ENV_FILE"
echo "Exit: $?"     # must be 0 before continuing
```

### 2.1 Give KGHM IT their own account

> **Nothing is redeployed.** A new Linux account is just a login — it does not own the
> software. Docker containers, `/data`, and systemd services are all system-wide, so the new
> account logs in and immediately has the same running system. No install, no copy, no
> downtime. The only per-account things are cron and shell variables.

```bash
read -rp "New account username: " ACCT
sudo adduser "$ACCT"
sudo usermod -aG sudo,docker "$ACCT"
sudo mkdir -p /home/"$ACCT"/.ssh && sudo chmod 700 /home/"$ACCT"/.ssh
sudo tee /home/"$ACCT"/.ssh/authorized_keys > /dev/null    # paste their public key, then Enter, then Ctrl-D
sudo chmod 600 /home/"$ACCT"/.ssh/authorized_keys
sudo chown -R "$ACCT": /home/"$ACCT"/.ssh
```

KGHM verifies from their own machine, unaided:

```bash
read -rp "Account username: " ACCT; read -rp "Server address: " SERVER
ssh "$ACCT@$SERVER" 'groups && docker ps && curl -sf http://localhost:8000/health/ready'
```

`groups` must include **`docker`** — without it `docker ps` fails with a permission error.

Two things are per-account and need carrying over:

- **Shell variables** — copy the `export` block from `sapphire`'s `~/.bashrc`
- **Cron** — leave it with `sapphire`; read it with `sudo crontab -u sapphire -l`

### 2.2 Rotate the easy secrets

```bash
E="$ENV_FILE"
cp "$E" "$E.pre-handover"
sed -i "s|^JWT_SECRET_KEY=.*|JWT_SECRET_KEY=$(openssl rand -hex 32)|" "$E"
sed -i "s|^API_KEY=.*|API_KEY=$(openssl rand -hex 32)|"               "$E"
chmod 600 "$E"
```

Rotating `JWT_SECRET_KEY` **logs every forecaster out**. They log back in with the same
password — but tell them first, or IT will get support calls.

### 2.3 Rotate the database password — the one with a trap

Editing `POSTGRES_PASSWORD` in the `.env` does **nothing on its own**. That variable is only
read when a Postgres volume is first created, so after a year the running databases still hold
the old password. Change it *inside* each database, then update the file:

```bash
read -rsp "Current POSTGRES_PASSWORD: " CUR_PW; echo
NEW_PW=$(openssl rand -base64 24)
for c in preprocessing postprocessing user auth; do
  docker exec -e PGPASSWORD="$CUR_PW" sapphire-$c-db \
    psql -U postgres -c "ALTER USER postgres WITH PASSWORD '${NEW_PW}';"
done
sed -i "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=${NEW_PW}|" "$E"
unset NEW_PW CUR_PW
```

### 2.4 Restart and verify

```bash
cd "$REPO"
source bin/utils/common_functions.sh
read_configuration "$E"
docker compose -f sapphire/docker-compose.yml up -d --force-recreate
sleep 30
bash bin/handover_healthcheck.sh
```

Compare against the "before" file from 1.1 — **nothing should have got worse**.

```bash
rm "$E.pre-handover"        # only once the health check is clean
```

If anything fails: `cp "$E.pre-handover" "$E"` and restart. That is why the copy exists.

### 2.5 Hand over the remaining accounts

- [ ] **SMTP** — alerts send from a KGHM mailbox to KGHM recipients; send a test
- [ ] **Off-site backup target** — KGHM-owned, writable by the cron user
- [ ] **Registry** — `docker pull mabesa/sapphire-preprunoff:latest` succeeds
- [ ] **GitHub** — read access to repo and issue tracker (no commit rights needed)
- [ ] **Dashboard admin account** for KGHM IT, then deactivate the Provider's accounts (3.2)
- [ ] All of the above stored in KGHM's secret manager

---

## Chapter 3 — Procedures

> Reconnected since **Set these first**? Re-run that block before anything below.

Two procedures, and they carry very different risk.

**User management (3.2) is routine** — create an account, reset a password, deactivate someone
who has left. Do it whenever you need to. Only *deleting* an account is irreversible, and 5.2
lists that as escalate-first.

**Restoring a database (3.1) is not routine.** It deletes live data before putting the backup
back, and 5.2 says to escalate rather than attempt it alone. It is written out in full here
so you can follow it *with* the Provider on the call, and so the quarterly practice run at the
end of 3.1 is something you can do safely by yourself.

### 3.1 Restore a database

> **Call the Provider before you start**, unless you are doing the practice run at the end of
> this section. This is the one procedure in the guide that destroys data before it restores
> it, and a wrong dump or a missed step is not recoverable from inside the procedure. 5.2 lists
> it as escalate-first for that reason.

Read the whole section before typing anything. Restoring **deletes the current database**
before putting the backup back.

> Every command below is a **single line**, even where that makes it long. Paste one at a
> time. Commands split across lines break if your terminal adds indentation when pasting.

#### When you need this

| Situation | Restore? |
|---|---|
| Data looks wrong or was deleted by mistake | Yes |
| A database will not start, or is corrupt | Yes |
| An update went wrong | Try the update rollback first (`doc/prod/update_deployment_checklist.md` §5) |
| Forecasts are stale | **No** — check the iEasyHydro connection instead |

#### Which database?

There are four, and they are independent. Restore only the one you need.

| If the problem is… | Database | Variable | Stop this service |
|---|---|---|---|
| Runoff, meteo, snow, hydrographs | `preprocessing_db` | `PREPROCESSING_DB` | `preprocessing-api` |
| Forecasts, skill metrics, bulletins | `postprocessing_db` | `POSTPROCESSING_DB` | `postprocessing-api` |
| Dashboard user accounts | `user_db` | `USER_DB` | `user-api` **and** `auth-api` |
| Logins, tokens | `auth_db` | `AUTH_DB` | `auth-api` |

The example below restores **preprocessing**. For another database, swap the three values in
step 2 using the table above.

#### Before you start

Confirm backups exist at all:

```bash
ls -lth /var/backups/sapphire/*.dump | head -8
```

All four databases write into this one directory, named
`<database>_<date>_<time>.dump`. **You must pick a dump whose name matches the database you
are restoring** — `pg_restore` will not warn you if you hand it the wrong one. Step 2 filters
the list for you.

Anything ending `.FAILED` is unusable.

Tell the forecasters the tool will be unavailable for a few minutes.

---

#### Step 1 — Take a fresh backup first

Even a damaged database is worth keeping. If the restore goes wrong, this is your way back.

```bash
cd "$REPO"
bash bin/backup_sapphire_db.sh -d /var/backups/sapphire -r 0 -e "$ENV_FILE"
echo "Exit: $?"
```

> `-r 0` means **keep every dump**. Do not use the `-r 30` from the scheduled job here: it
> deletes dumps older than 30 days, and it runs even when the backup itself failed. If you are
> restoring from an older dump, that is the file you are about to destroy.

**If this prints `Exit: 0`,** continue to Step 2.

**If it fails because a database container is not running,** you have hit the case this
procedure exists for — the backup needs the container up, and you are here because it is not.
Do not stop, and do not skip the protection:

```bash
docker ps -a --filter name=sapphire- --format "{{.Names}}\t{{.Status}}"
```

- **Other databases are up** — their dumps succeeded; only the broken one is missing. That is
  expected. Continue, and rely on the copy you make in Step 2.
- **Nothing is running** — take no backup, and rely entirely on the Step 2 copy. Tell the
  Provider before you continue; restoring with no safety copy is a one-way step.

#### Step 2 — Set the four values you will reuse

```bash
set -a; source "$ENV_FILE"; set +a

CONTAINER=sapphire-preprocessing-db      # from the table above
TARGET_DB="${PREPROCESSING_DB}"          # from the table above
SERVICE=preprocessing-api                # from the table above
```

Now list only the dumps for **that** database, so you cannot pick another one's by mistake:

```bash
ls -lth /var/backups/sapphire/"${TARGET_DB}"_*.dump | head -8
```

```bash
read -rp "Dump filename, exactly as listed above (ends in .dump): " DUMPNAME
DUMP_FILE="/var/backups/sapphire/$DUMPNAME"
ls -l "$DUMP_FILE" && echo "Restoring $TARGET_DB from $DUMP_FILE"
```

If `ls` cannot find the file, fix the name now — later steps assume it exists.

**Copy it somewhere nothing prunes.** This is the file the whole restore depends on:

```bash
mkdir -p ~/restore_safe && cp "$DUMP_FILE" ~/restore_safe/ && ls -l ~/restore_safe/
```

#### Step 3 — Stop only the service that uses this database

```bash
cd "$REPO"
source bin/utils/common_functions.sh
read_configuration "$ENV_FILE"
docker compose -f sapphire/docker-compose.yml stop "$SERVICE"
```

> `read_configuration` is not optional. Compose needs values it works out from the env file's
> location, and without them it operates on the wrong paths — with no error message.

Leave the **database container running**. The restore connects to it.

#### Step 4 — Empty the database

```bash
docker exec -e PGPASSWORD="${POSTGRES_PASSWORD}" "$CONTAINER" psql -U "${POSTGRES_USER}" -d postgres -c "DROP DATABASE IF EXISTS \"${TARGET_DB}\";"

docker exec -e PGPASSWORD="${POSTGRES_PASSWORD}" "$CONTAINER" psql -U "${POSTGRES_USER}" -d postgres -c "CREATE DATABASE \"${TARGET_DB}\" OWNER \"${POSTGRES_USER}\";"
```

Expect `DROP DATABASE` then `CREATE DATABASE`.

**If it says "database is being accessed by other users":** something is still connected. Go
back to step 3 and confirm the service actually stopped (`docker ps | grep "$SERVICE"`).

#### Step 5 — Load the backup

```bash
docker exec -i -e PGPASSWORD="${POSTGRES_PASSWORD}" "$CONTAINER" pg_restore -U "${POSTGRES_USER}" -d "${TARGET_DB}" --no-owner --no-privileges < "$DUMP_FILE"
echo "Exit: $?"
```

This can take a few minutes on a large database.

- Warnings about `plpgsql` or comments — **normal, ignore them**
- `Exit: 0` — success
- Anything else — read the error and do not continue

#### Step 6 — Start everything again

```bash
docker compose -f sapphire/docker-compose.yml up -d
sleep 30
curl -sf http://localhost:8000/health/ready && echo " READY"
```

#### Step 7 — Check the data is really there

Count the rows in the restored database:

```bash
docker exec -e PGPASSWORD="${POSTGRES_PASSWORD}" "$CONTAINER" psql -U "${POSTGRES_USER}" -d "${TARGET_DB}" -tAc "SELECT count(*) FROM runoffs;"
```

Use the right table for the database you restored: `runoffs` (preprocessing), `forecasts`
(postprocessing), `users` (user), `refresh_tokens` (auth).

A sensible number means the data is back. **Zero means the restore did not work** — do not
stop here.

Then the full check, and the dashboard:

```bash
bash bin/handover_healthcheck.sh
```

Open the dashboard and confirm a known station shows data.

---

#### If it went wrong

**First stop the service again** — Step 6 restarted it, and Step 4 cannot drop a database
something is connected to:

```bash
docker compose -f sapphire/docker-compose.yml stop "$SERVICE"
```

Then repeat Steps 4 and 5 using the dump taken in Step 1, which is the state you started from.
If Step 1 could not run because the container was down, use your Step 2 copy in
`~/restore_safe/` — that is why you made it.

If that also fails, stop and escalate. Send: which database, which dump file, the exact error,
and the output of `docker ps`.

---

#### Practise before you need it

Doing this for the first time during a real incident is how mistakes happen. Once a quarter,
restore into a **throwaway** database instead — nothing live is touched:

```bash
set -a; source "$ENV_FILE"; set +a
CONTAINER=sapphire-preprocessing-db
SCRATCH=restore_drill_$(date +%Y%m%d)
DUMP=$(ls -t /var/backups/sapphire/preprocessing*.dump | head -1)

docker exec -e PGPASSWORD="${POSTGRES_PASSWORD}" "$CONTAINER" psql -U "${POSTGRES_USER}" -d postgres -c "CREATE DATABASE ${SCRATCH};"
docker exec -i -e PGPASSWORD="${POSTGRES_PASSWORD}" "$CONTAINER" pg_restore -U "${POSTGRES_USER}" -d "${SCRATCH}" --no-owner --no-privileges < "$DUMP"
echo "Exit: $?"
```

Compare the practice copy against the live one — the numbers should be close:

```bash
for DB in "${SCRATCH}" "${PREPROCESSING_DB}"; do
  printf "%-28s " "$DB"
  docker exec -e PGPASSWORD="${POSTGRES_PASSWORD}" "$CONTAINER" psql -U "${POSTGRES_USER}" -d "$DB" -tAc "SELECT count(*) FROM runoffs;"
done
```

Then delete the practice copy:

```bash
docker exec -e PGPASSWORD="${POSTGRES_PASSWORD}" "$CONTAINER" psql -U "${POSTGRES_USER}" -d postgres -c "DROP DATABASE ${SCRATCH};"
```

A backup nobody has ever restored is not a backup.

### 3.2 User management

These are **dashboard logins** — the accounts forecasters use to open the tool. They are not
the Linux server accounts from Chapter 2.

**There is no screen for this.** Everything is done with `curl` commands on the server. Every
command below is a single line — paste one at a time.

---

#### Before you start

Get the API key once per session. It is not shown as you type:

```bash
read -rs -p "API key: " SAPPHIRE_API_KEY; echo
```

The value is `API_KEY` in the settings file. To look it up:

```bash
grep "^API_KEY=" "$ENV_FILE"
```

Two things worth knowing before you use these commands:

| | |
|---|---|
| **Creating** a user needs **no** API key | `/api/auth/register` is open |
| **Logging in** uses a form, not JSON | `--data-urlencode`, not `-d '{...}'` |

That difference catches people out — a JSON login returns a confusing "field required" error
even when the username and password are correct.

---

#### See who has an account

```bash
curl -s http://localhost:8000/api/user/users/ -H "X-API-Key: $SAPPHIRE_API_KEY" | python3 -m json.tool
```

Each entry shows `id`, `username`, `email`, and `is_active`. **Note the `id`** — you need it
to deactivate or delete someone.

Look up one person:

```bash
read -rp "Username: " U
curl -s "http://localhost:8000/api/user/users/by-username/$U" -H "X-API-Key: $SAPPHIRE_API_KEY" | python3 -m json.tool
```

---

#### Add a new forecaster

**Step 1 — create the account.** It asks for each value in turn; the password is not shown:

```bash
read -r -p "Email: " EMAIL; read -r -p "Username: " USERNAME; read -r -p "Full name: " FULLNAME; read -rs -p "Password: " PW; echo
```

```bash
python3 -c 'import json,sys; print(json.dumps({"email":sys.argv[1],"username":sys.argv[2],"full_name":sys.argv[3],"password":sys.argv[4]}))' "$EMAIL" "$USERNAME" "$FULLNAME" "$PW" | curl -sS -X POST http://localhost:8000/api/auth/register -H "Content-Type: application/json" --data-binary @- | python3 -m json.tool
```

Rules the server enforces — a `422` error means one of these failed:

- username at least **3** characters
- password at least **8** characters
- email required, and must not already be in use

**Step 2 — check they can actually log in.** Do this before telling them the account is ready:

```bash
curl -s -X POST http://localhost:8000/api/auth/login --data-urlencode "username=$USERNAME" --data-urlencode "password=$PW" | python3 -c "import sys,json; print('LOGIN OK' if 'access_token' in json.load(sys.stdin) else 'FAILED')"
```

**Step 3 — clear the password from memory:**

```bash
unset PW
```

**Step 4 — tell the person their username and password**, through a channel that is not this
terminal's history. Ask them to change it themselves ("Password changes" below).

---

#### Someone has left — deactivate their account

Prefer deactivating over deleting. It blocks the login but keeps the record of who did what.

**Step 1 — find their id:**

```bash
read -rp "Username: " U
curl -s "http://localhost:8000/api/user/users/by-username/$U" -H "X-API-Key: $SAPPHIRE_API_KEY" | python3 -m json.tool
```

**Step 2 — deactivate:**

```bash
read -rp "User id: " ID
curl -s -X PUT "http://localhost:8000/api/user/users/$ID" -H "X-API-Key: $SAPPHIRE_API_KEY" -H "Content-Type: application/json" -d '{"is_active": false}' | python3 -m json.tool
```

The reply should show `"is_active": false`.

**Step 3 — confirm the login is really blocked:**

```bash
read -rsp "Their password: " THEIR_PW; echo
curl -s -X POST http://localhost:8000/api/auth/login --data-urlencode "username=$U" --data-urlencode "password=$THEIR_PW" | python3 -m json.tool
```

Expect an error, not a token. If you do not know their password, skip this step and trust the
`is_active: false` in step 2.

> **This test does not prove their access has ended.** It proves they cannot start a *new*
> session. Someone already signed in keeps working: the token-refresh and token-verify paths do
> not re-check whether the account is still active, so an open dashboard survives until its
> token expires. For a departure that matters — a dispute, a security concern — tell the
> Provider, and treat the account as still live until they confirm the session is gone.

To bring someone back, repeat step 2 with `true`.

---

#### Delete an account permanently

> **Escalate before doing this.** It is the one irreversible action in this section, and 5.2
> lists it as stop-and-call. Deactivating (above) blocks the login and can be undone — prefer
> it unless the Provider has agreed a deletion is needed.

This removes the record entirely.

```bash
read -rp "User id: " ID
curl -s -X DELETE "http://localhost:8000/api/user/users/$ID" -H "X-API-Key: $SAPPHIRE_API_KEY"
```

Confirm it is gone:

```bash
curl -s http://localhost:8000/api/user/users/ -H "X-API-Key: $SAPPHIRE_API_KEY" | python3 -m json.tool | grep -c '"id"'
```

---

#### Password changes

**A user changing their own password** needs to know the current one. They must be logged in,
so this is done with their own access token, not the API key:

```bash
read -rp "Username: " U; read -rsp "Current password: " CUR_PW; echo
TOKEN=$(curl -s -X POST http://localhost:8000/api/auth/login --data-urlencode "username=$U" --data-urlencode "password=$CUR_PW" | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
```

```bash
read -rsp "New password (8+ chars): " NEW_PW; echo
curl -s -X POST http://localhost:8000/api/auth/change-password -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d "{\"current_password\":\"$CUR_PW\",\"new_password\":\"$NEW_PW\"}"
```

```bash
unset TOKEN CUR_PW NEW_PW
```

**Forgotten password — you set a new one for them.** This does not need their old password,
and it keeps their account id and any roles:

```bash
read -rp "User id: " ID; read -rsp "New password (8+ chars): " NEW_PW; echo
curl -s -X PUT "http://localhost:8000/api/user/users/$ID" -H "X-API-Key: $SAPPHIRE_API_KEY" -H "Content-Type: application/json" -d "{\"password\":\"$NEW_PW\"}" | python3 -m json.tool
```

Check it worked before telling them, then clear the password from memory:

```bash
read -rp "Username: " U
curl -s -X POST http://localhost:8000/api/auth/login --data-urlencode "username=$U" --data-urlencode "password=$NEW_PW" | python3 -c "import sys,json; print('LOGIN OK' if 'access_token' in json.load(sys.stdin) else 'FAILED')"
unset NEW_PW
```

> Do **not** delete and recreate the account instead — that loses the account id and any roles
> attached to it. And do **not** edit the `hashed_password` column directly: the format belongs
> to the login service, and a hand-written value silently fails to authenticate, leaving an
> account that looks fine but cannot be used.

---

#### Roles

Roles exist but most deployments do not use them — plain accounts are enough. Check first:

```bash
curl -s http://localhost:8000/api/user/roles/ -H "X-API-Key: $SAPPHIRE_API_KEY" | python3 -m json.tool
```

An empty list `[]` means roles are not in use. **Leave it that way** unless something
specifically needs them.

If you do need one:

```bash
read -rp "Role name: " ROLE; read -rp "Role description: " ROLE_DESC
curl -s -X POST http://localhost:8000/api/user/roles/ -H "X-API-Key: $SAPPHIRE_API_KEY" -H "Content-Type: application/json" -d "{\"name\":\"$ROLE\",\"description\":\"$ROLE_DESC\"}" | python3 -m json.tool
```

```bash
read -rp "User id: " USERID; read -rp "Role id: " ROLEID
curl -s -X POST "http://localhost:8000/api/user/users/$USERID/roles/$ROLEID" -H "X-API-Key: $SAPPHIRE_API_KEY" | python3 -m json.tool
```

---

#### When things go wrong

| Message | Cause |
|---|---|
| `HTTP 422` on create | Username under 3 characters, or password under 8 |
| Duplicate error on create | That email or username already exists — check with "See who has an account" |
| `field required` on login | You sent JSON. Login needs `--data-urlencode` |
| `401` on login | Wrong password, or the account is deactivated |
| `403` or `401` on a user command | Wrong or missing API key — re-run "Before you start" |
| Connection refused | The gateway is down — see Chapter 4, "Service endpoints" |

---

#### Two safety rules

**The gateway must stay private.** `/api/auth/register` requires no authentication, so anyone
who can reach the API could create themselves an account. It must listen on localhost only and
must never be published through the public web proxy.

**The API key is a master key.** It allows listing, changing and deleting any account. Keep it
in the settings file at permissions `600`, never in a chat message or a shared document, and
never type it as a visible command argument — use the `read -rs` prompt above.

---

## Chapter 4 — What to do when the health check reports a problem

> Reconnected since **Set these first**? Re-run that block before anything below.

One section per check, in the order the script prints them. Find your message, follow the
steps. Every command is a single line — paste one at a time.

Throughout, `<data_dir>` is the deployment's data directory.

---

### Deployment layout

**`[WARN] env permissions are 644 — should be 600`**

The settings file holds the database password and JWT secret, and anyone on the server can
read it.

```bash
chmod 600 "$ENV_FILE"
```

**`[FAIL] no .env_develop_* found under /data/*/config/`**

The script looks for the settings file automatically and could not find one. Either the
deployment lives somewhere unusual, or the file is missing.

```bash
ls -la /data/*/config/.env* 2>/dev/null
```

Found it? Pass it explicitly:

```bash
bash bin/handover_healthcheck.sh --env-file "$ENV_FILE"
```

Nothing at all? The deployment is misconfigured — escalate. Do not create a new env file; the
existing one holds credentials that cannot be regenerated from memory.

---

### Containers

**`[FAIL] cannot talk to Docker as <user>`**

Your account is not allowed to use Docker.

```bash
groups $(whoami)
```

If `docker` is missing:

```bash
sudo usermod -aG docker $(whoami)
```

Then **log out and back in** — group membership only applies to a new session.

**`[WARN] N sapphire containers running (a full stack is ~10-11)`**

Something is not running. Find out what:

```bash
docker ps -a --filter name=sapphire- --format "{{.Names}}\t{{.Status}}"
```

Start the missing one — note `read_configuration` first:

```bash
cd "$REPO"
source bin/utils/common_functions.sh
read_configuration "$ENV_FILE"
docker compose -f sapphire/docker-compose.yml config --services
```

> The list above is **container** names (`sapphire-preprocessing-api`); the command below wants
> the **service** name from this second list (`preprocessing-api`). They are not always the
> container name minus the prefix, so read it from here rather than guessing.

```bash
read -rp "Service name (from the second list): " SVC
docker compose -f sapphire/docker-compose.yml up -d "$SVC"
```

**`[WARN] exited container(s): …`**

A container stopped. Read why before restarting it:

```bash
read -rp "Container name: " CTR
docker logs --tail 50 "$CTR"
```

**`[FAIL] N container(s) stuck restarting`**

A container is crash-looping — it starts, fails, and restarts over and over. The logs tell you
why:

```bash
read -rp "Container name: " CTR
docker logs --tail 80 "$CTR"
```

Common causes: a wrong password in the env file, a database that is not ready, or a full disk.
Fix the cause; restarting will not help on its own.

---

### Service endpoints

**`[FAIL] api-gateway /health/ready NOT responding`**

The most serious message in the report. Nothing can read or write data.

```bash
docker ps --filter name=sapphire-api-gateway --format "{{.Names}} {{.Status}}"
docker logs --tail 50 sapphire-api-gateway
```

The gateway depends on the other services, so check those first — a database that will not
start takes the gateway down with it.

**`[FAIL] service on :8002 not responding`** (or 8003, 8004, 8005)

| Port | Service | Affects |
|---|---|---|
| 8002 | preprocessing | runoff, meteo, snow data |
| 8003 | postprocessing | forecasts, skill metrics, bulletins |
| 8004 | user | dashboard accounts |
| 8005 | auth | logins |

```bash
docker logs --tail 50 sapphire-preprocessing-api
```

If it cannot reach its database, confirm that database is healthy:

```bash
docker ps --filter name=-db --format "{{.Names}} {{.Status}}"
```

**`[WARN] Luigi :8082 not responding`**

The scheduler is down, so **no forecast will run tonight**. Treat this as urgent even though
it is only a warning.

```bash
cd "$REPO"
source bin/utils/common_functions.sh
read_configuration "$ENV_FILE"
export COMPOSE_PROJECT_NAME=sapphire
docker compose -f bin/docker-compose-luigi.yml up -d luigi-daemon
```

**`[FAIL] dashboard :5006 returned '000'`**

Forecasters cannot open the tool.

```bash
docker ps --filter name=sapphire-dashboard --format "{{.Names}} {{.Status}}"
docker logs --tail 50 sapphire-dashboard
```

Remember `(unhealthy)` on this container is normal and not the problem — judge it by whether
the page answers.

---

### Data freshness

**`[FAIL] no forecasts in the last 45 days — the pipeline has stopped publishing`**

Serious. Work through the steps under "Recent pipeline runs" below rather than guessing.

**`[WARN] pentad forecasts: newest is <date>, but a run was due <date>`**

A scheduled run was missed. The check works out when a forecast was due — pentad on the 5th,
10th, 15th, 20th, 25th and last day of the month; decad on the 10th, 20th and last day — and
tells you which one is missing, so you do not have to work it out.

It allows until midday local time on an issue day before saying anything, because that day's
run happens in the morning. So this message means the run should already have finished.

1. Look for a failure log:

```bash
ls -lt "$DATA_DIR"/intermediate_data/docker_logs/failure_log_* 2>/dev/null | head -3
```

2. Read the newest one:

```bash
grep -E "ERROR|CRITICAL|Error:" $(ls -t "$DATA_DIR"/intermediate_data/docker_logs/failure_log_* | head -1) | tail -15
```

3. Re-run by hand once the cause is fixed:

```bash
cd "$REPO" && bash bin/run_pentadal_forecasts.sh "$ENV_FILE"
```

**`[WARN] discharge data stale for: …`**

River data has stopped arriving. This is the usual cause of stale forecasts, and it is almost
always the iEasyHydro connection — see that section below.

**`[WARN] long-term (month) forecasts: newest is <date>, but a run was due <date>`**

A scheduled long-term run was missed. The check reads this deployment's own issue days from its
long-term configuration, so the due date is the real one, not a guess.

Long-term covers three separate schedules and each is judged on its own, so a healthy monthly
run can no longer hide a dead seasonal one:

| Label | Produced by |
|---|---|
| `(month)` | the `month_*` modes |
| `(season)` | the `seasonal_*` modes |
| `(quarter)` | the `quarter` mode |

Only one line appears if the deployment runs a single horizon.

Two messages here are **not** problems:

- `no long-term run scheduled this month` — a seasonal mode that does not run in the current
  month. Nothing is wrong.
- `(seasonal gating unavailable: …)` — the due date is still correct; only the check for
  whether a *seasonal* mode runs this month could not be made, because the model config it names
  is missing. If the warning turns out to be about a seasonal mode that does not run now, it is
  a false alarm — and the missing file is worth looking at, since that model may not be running
  at all.
- `[due-date check unavailable: …]` — stronger: the schedule itself could not be read, so it
  fell back to a simple 120-day staleness check. The forecast may be perfectly current. Worth
  fixing the config path it names, but not urgent.

**`[WARN] no long-term forecasts in the last 120 days`**

Long-term forecasting is configured but has stopped. Check its own log:

```bash
ls -lt ~/logs/sapphire_long_term_* 2>/dev/null | head -3
```

Long-term runs only on its configured issue days, so confirm the schedule actually fires:

```bash
sudo crontab -u sapphire -l | grep -i long_term
```

**`[SKIP] long-term forecasting not configured`**

Correct on a deployment that does not run it. If KGHM **does** run long-term forecasts, the
settings file is missing the variable that switches this check on:

```bash
grep -n "ml_long_term" "$ENV_FILE"
```

**`[WARN] no discharge data in the last 45 days`**

No river measurements have arrived at all — not merely stale, absent. Almost always the
iEasyHydro connection (see that section below). If the connection is healthy, the far end has
stopped sending: contact the iEasyHydro operators, and tell them it is blocking forecasts.

**`[WARN] skipped — API gateway is down`**

Not a data problem — fix the gateway first (see Service endpoints), then re-run the check.

---

### Recent pipeline runs

**`[WARN] N failure log(s) in the last 3 days`**

Something failed recently. The logs may be old news, or a live problem.

```bash
ls -lt "$DATA_DIR"/intermediate_data/docker_logs/failure_log_* | head -5
```

Check whether anything failed in the last 12 hours:

```bash
find "$DATA_DIR"/intermediate_data/docker_logs -name 'failure_log_*' -mmin -720 | wc -l
```

`0` means these are historical and will age out of the 3-day window on their own. Above `0`
means it is still happening — read the newest log.

These files are large. Go to the end, where the error is:

```bash
tail -40 $(ls -t "$DATA_DIR"/intermediate_data/docker_logs/failure_log_* | head -1)
```

**`[WARN] docker_logs directory not found`**

Either the deployment has never written one, or the data directory was detected wrongly.
Confirm the path in the **Deployment layout** section at the top of the report.

---

### Cron schedule

**`[WARN] no cron entries for <user>; the schedule belongs to: sapphire`**

Not a fault. Cron is per-account, and the schedule belongs to whoever created it.

```bash
sudo crontab -u sapphire -l
```

**Leave the jobs where they are.** Moving them to another account breaks the paths they use.

**`[FAIL] cron daemon NOT running — nothing is scheduled`**

No forecast will run. Urgent.

```bash
sudo systemctl status cron --no-pager | head -5
sudo systemctl start cron
sudo systemctl enable cron
```

---

### Monitoring

**`[FAIL] docker-monitor.service is NOT INSTALLED`**
**`[FAIL] dashboard-log-watcher.service is NOT INSTALLED`**

Nobody is being told when something breaks. Install per
`doc/monitoring/forecast_tools_monitoring.md`. Two things catch people out: each unit needs
`Environment="DOCKER_MONITOR_ENV_PATH=<full path to the env file>"`, and `msmtp` must be
installed (`which msmtp`).

**`[FAIL] <unit> is installed but inactive`**

It is there but stopped. Use the unit name the check printed — it may be either of the two:

```bash
read -rp "Unit name from the message: " MUNIT
sudo journalctl -u "$MUNIT" -n 30 --no-pager
```

Find out why before restarting, then:

```bash
sudo systemctl restart "$MUNIT" && systemctl is-active "$MUNIT"
```

**`[WARN] SMTP variables missing: …`**

Alerts cannot send. All six must be set in the settings file:

```bash
grep -c "^SAPPHIRE_PIPELINE_SMTP_SERVER=\|^SAPPHIRE_PIPELINE_SMTP_PORT=\|^SAPPHIRE_PIPELINE_SMTP_USERNAME=\|^SAPPHIRE_PIPELINE_SMTP_PASSWORD=\|^SAPPHIRE_PIPELINE_SENDER_EMAIL=\|^SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS=" "$ENV_FILE"
```

Expect `6`.

> Even when this section is all green, alerting can still be broken. The only proof is a real
> email — trigger one by stopping and starting a container, and confirm it arrives.

---

### Backups

**`[FAIL] backup directory does not exist — no backups are being taken`**

```bash
sudo mkdir -p /var/backups/sapphire && sudo chown $(whoami): /var/backups/sapphire
cd "$REPO" && bash bin/backup_sapphire_db.sh -d /var/backups/sapphire -r 30 -e "$ENV_FILE"
```

Then make sure it is scheduled:

```bash
sudo crontab -u sapphire -l | grep backup_sapphire
```

**`[FAIL] only N fresh dump(s) — expected 4`**

Backups have stopped, or only some databases are being saved. Run one by hand and watch it:

```bash
cd "$REPO" && bash bin/backup_sapphire_db.sh -d /var/backups/sapphire -r 30 -e "$ENV_FILE"
echo "Exit: $?"
```

The usual cause is a missing `-e` on the cron line: the script defaults to `sapphire/.env`,
which this deployment does not have, so it exits with `Env file not found` and takes no
backup at all.

**`[FAIL] N .FAILED backup artifact(s)`**

A backup started and could not finish — often a full disk. Check space, then re-run the backup
by hand and read the error.

---

### iEasyHydro HF connection

**`[FAIL] <unit> NOT running — discharge data will go stale`**

The most common cause of stale forecasts.

```bash
read -rp "Systemd unit name: " UNIT
sudo systemctl status "$UNIT" --no-pager | head -5
sudo journalctl -u "$UNIT" -n 30 --no-pager
sudo systemctl restart "$UNIT"
curl -s http://localhost:5555/api/v1/ | head -c 100
```

Any HTTP response — even 404 — means traffic is flowing again. If it will not stay up, the
far end may be down: contact the iEasyHydro operators.

**`[FAIL] a tunnel process is running but NO systemd unit manages it`**

It works now but **will not come back after a reboot**. Someone started it by hand. Record the
exact command:

```bash
ps aux | grep -i autossh | grep -v grep
```

Then have it wrapped in a systemd unit so it starts automatically. Do not leave this
unresolved — a reboot silently stops all river data.

**`[WARN] no iEasyHydro tunnel unit or process found`**

The script cannot tell how this deployment reaches iEasyHydro. If discharge data is current,
it is working — find out how and write it down:

```bash
grep -n "IEASYHYDROHF_HOST" "$ENV_FILE"
```

A public web address means it connects straight over the internet and no tunnel is needed.

---

### Disk

**`[WARN] / is 84% full`** — act now, do not wait for 90%.

See where it went:

```bash
docker system df
sudo du -sh /var/lib/docker/containers /var/backups/sapphire "$DATA_DIR"/intermediate_data
```

Safe to reclaim, in this order:

```bash
docker builder prune -f
docker image prune -f
sudo find /var/lib/docker/containers -name "*-json.log" -size +50M -exec truncate -s 0 {} \;
```

> **Do not run `docker image prune -a`.** It deletes every image not attached to a running
> container — including all the pipeline images that only run during cron jobs. They would
> have to be downloaded again.

If container logs keep growing, log rotation is not configured:

```bash
cat /etc/docker/daemon.json 2>&1
```

Missing? Create it with `max-size: 10m` and `max-file: 3`, then restart Docker **in a quiet
window** — every container stops and starts.

---

### Script versions

**`[WARN] only N wrapper(s) have the [retcode] fix`**

The forecast scripts on this server are an older version that reports success even when a
forecast failed. Until they are updated, **do not trust exit codes** — judge a run by whether
a new failure log appeared.

```bash
cd "$REPO" && git pull origin maxat_sapphire_2
grep -l '\[retcode\]' bin/run_*.sh | wc -l
```

Expect 5 or more. If the pull does not fix it, ask the Provider for the update.

---

### After any fix

Re-run the check and compare with the "before" file from 1.1:

```bash
bash "$REPO"/bin/handover_healthcheck.sh
```

Then write down what happened — symptom, cause, fix. The next person will meet it again.

---

---

## Chapter 5 — Every day

> Reconnected since **Set these first**? Re-run that block before anything below.

Three steps. Do them in order.

### 5.1 Run the health check

```bash
bash "$REPO"/bin/handover_healthcheck.sh
```

- **No `[FAIL]` or `[WARN]`** — nothing to do. You are finished.
- **Anything reported** — go to 5.2.

Also check your email for alerts, and ask a forecaster whether today's numbers look right.

### 5.2 Can you fix it yourself?

Only the six below. Anything else — go straight to 5.3. This list is deliberately short: each
entry says exactly when it applies and how you know it worked.

| If the check says | You may | Check it worked |
|---|---|---|
| A service container has exited | Start it: see Chapter 4, "Containers". **Only** the long-running services — never a name containing `-run-`, those are finished pipeline jobs | Health check shows it `Up` |
| `cron daemon NOT running` | `sudo systemctl start cron && sudo systemctl enable cron` | `systemctl is-active cron` → `active` |
| The iEasyHydro unit is not running | Restart it: see Chapter 4, "iEasyHydro HF connection". **One attempt** | `curl -s http://localhost:5555/api/v1/` answers anything |
| `env permissions are 644` | `chmod 600 "$ENV_FILE"` | Re-run the health check |
| Disk over 80% | `docker builder prune -f` then `docker image prune -f` | `df -h /` below 80% |
| A monitoring unit is installed but inactive | Restart it: see Chapter 4, "Monitoring" | `systemctl is-active` → `active` |

**Stop and escalate instead — do not attempt these alone:**

- Restoring a database (Chapter 3.1)
- Changing any password or secret (Chapter 2)
- Deleting a user account (Chapter 3.2)
- Restarting Docker itself
- `docker image prune -a` — it looks like the safe one but removes images the pipeline needs
- Emptying container log files
- Anything not named in the table above

> **One attempt each.** If the same problem comes back after you fix it, stop and escalate —
> something underneath is causing it, and repeating the fix hides that.

Afterwards, re-run the health check and write down what happened — symptom, cause, fix.

### 5.3 If you cannot fix it — escalate

One command. It asks you two questions, collects everything else, and prints one filename:

```bash
bash "$REPO"/bin/collect_support_bundle.sh
```

Attach that file to your message to the Provider. It contains the health-check output, the
container list, the newest failure logs, the systemd unit states, disk usage and the repo
version — the things the first reply would otherwise have to ask for.

> Passwords and keys are stripped out automatically. The archive **does** contain station codes
> and discharge values, so send it to the Provider only — do not forward it on.

| Severity | Example | First response |
|---|---|---|
| **S1 — outage** | No forecasts at all; dashboard down | Same business day |
| **S2 — degraded** | One model failing; one tab stale | 2 business days |
| **S3 — minor** | Cosmetic issue, a question | Best effort |

> **Some problems are not the Provider's to fix.** If the iEasyHydro server or the data gateway
> is down, the fault is at their end — contact whoever operates them. The health check and the
> failure log will say which.
