# Long-Term Forecast Recovery Runbook

**Audience:** operator on a production server responding to a missed
long-term (month/quarter/season) forecast run.

**Assumed access and skills.** This procedure requires shell access to the
production server, `docker exec` into containers, running raw SQL against
the Postgres database, reading the Luigi scheduler UI, and editing the
crontab. At a smaller hydromet service there may be no separate IT
operator — if you are a hydrologist without that access or comfort level,
hand this to whoever administers the server (or run it together) rather
than improvising around a step you cannot complete.

**Scope:** recovering member forecast rows and the derived ensembles for
ONE mode and ONE issue date, within the current or previous calendar
month. This runbook does not cover code changes, and it does not cover
gaps older than the two-month window (see "Gaps outside the recovery
window", below).

---

## Cross-references

- Cron schedule and the long-term issue-day entries: `doc/deployment.md`
  (cron block (4), "Long-term Forecast").
- Long-term deploy procedure: `doc/prod/long_term_deploy_runbook.md`.
- Older gaps (beyond this runbook's two-month window): the P7 procedure in
  `doc/prod/historical_backfill_runbook.md`, and its scope limits, below.
- Developers rehearsing this locally: `doc/dev/testing_workflow.md`,
  "Long-term recovery of ONE missed month".

> **Sensitive-data rule.** Do not write real station codes, discharge
> values, passwords, or env-file contents into logs or committed
> artifacts. Use `19999` as the sample station code in any example.
>
> This governs what YOU deliberately write — this runbook's own examples,
> tickets, chat messages, anything you copy/paste to escalate. It does
> **not** describe the pipeline's own retained files: the container logs
> this runbook sends you to in section 7 already contain real station
> codes and discharge values (`run_forecast.py` logs per-model temporal
> data and forecast heads; `DockerTaskBase` persists the full child
> output to the success/failure log files on disk). Treat those retained
> log files themselves as sensitive — restrict who can read them, and
> redact station codes/values before pasting their contents into a ticket
> or a chat message.

---

## 1. When to use this runbook

Use this runbook when a long-term issue day has passed and the mode's
member forecast rows for that date were never written — for example, the
06:00 UTC long-term cron entry did not run, failed before writing rows, or
was skipped because the deployment's issue-day configuration was wrong for
that mode.

This is **not** the same problem as, and is **not fixed by**, any of the
following:

- `bin/run_long_term_forecasts.sh` (cron entry (4)) — this is the
  scheduled operational run itself. If it already ran successfully for
  this mode and date, there is nothing to recover.
- `bin/bimonthly_long_term_skill_metrics_recalculation.sh` (cron entry
  (4b)) — this recalculates skill metrics from forecasts that already
  exist. It cannot create a missing month.
- `bin/run_periodic_maintenance.sh long_term` (cron entry (6),
  `postprocessing_maintenance_long_term.py`) — this fills in missing
  **ensemble** rows (EM / Skilled Mean / Naive Mean) from **member**
  forecasts that already exist in the database. If the member forecasts
  themselves were never written, this task finds nothing to derive them
  from and exits successfully having done nothing. It regenerates
  aggregates, never a missing month of raw forecasts.

If the member rows for the mode and issue date are genuinely absent from
`postprocessing_db.long_forecasts`, this runbook's recovery command is the
correct tool. Confirm absence before proceeding (see step 2 below).

---

## 2. Preflight — do this before running anything

Work through these in order. Do not skip to the command in section 3
before completing all of them.

### 2a. Determine every affected mode

One missed issue day can affect more than one mode (for example, a
deployment where `month_0` and `quarter` share the same
`operational_issue_day`), and the recovery command in section 3 recovers
exactly ONE mode per invocation. Repeat sections 2-8 once per affected
mode.

Mode activity for a given date depends on the deployment's supported
modes, each mode's `operational_issue_day`, and each member model's
configured `forecast_months` (`lt_schedule_query.query_schedule`,
`config_forecast.ForecastConfig`). Do not hand-build a `docker run`
invocation of `lt_schedule_query.py` to check this: its environment,
volumes, and network are internal to the `LTScheduleQuery` Luigi task
(`pipeline_docker.py`), the volume host/container paths are derived from
the deployment's env-var values in a way that is not safe to guess, and a
wrong guess fails inside an incident instead of telling you anything
useful. Instead:

- Read the modes and their `operational_issue_day` values straight from
  this deployment's live long-term config directory (see 2b, below). This
  alone tells you which modes could plausibly have been active on the
  missed date, without running anything.
- **`monthly` is a calibration mode, not an operational one — never recover
  it.** `lt_schedule_query.py` hardcodes `NON_OPERATIONAL_MODES = {"monthly"}`
  and skips it when building the operational schedule; the recovery path
  (`lt_recovery.run_recovery`) has no equivalent exclusion and will happily
  attempt it if asked. If a live config directory contains a `monthly` mode
  with a matching issue day, that is not evidence it was supposed to run
  operationally — do not pass `monthly` as the mode to section 3's command.
  This is a gap in the recovery tooling, not a documentation nuance;
  treat any `monthly` config as out of scope for this runbook.
- If the scheduled long-term run for the missed date already executed,
  `LTScheduleQuery` will have left that run's result at
  `<data_ref_dir>/intermediate_data/lt_schedule_result.json` (JSON:
  `active_modes`, `skipped_modes` with reasons) and its container log at
  `<data_ref_dir>/intermediate_data/docker_logs/log_lt_schedule_query_<timestamp>.txt`.
  These two do NOT age the same way, and neither is permanent.
  `lt_schedule_result.json` is deleted and rewritten by every run, so it
  reflects only the most recent long-term run — if the schedule has run
  again since the missed date, this file no longer describes it; check it
  promptly, before the next scheduled run. The timestamped
  `log_lt_schedule_query_*.txt` files are not overwritten by the next run
  the way the JSON is, but they are not permanent either: routine pipeline
  cleanup (`LogFileCleanup`, matching `log_*.txt` in this same
  `docker_logs/` directory) deletes any file older than 15 days by default.
  An outage discovered more than ~15 days late may find neither the JSON
  nor the log for the missed run. As in section 7, the container's log is
  written only when it exited 0 — if the schedule query itself failed,
  expect no log for that attempt. The JSON is different: the container
  command is a shell redirection
  (`sh -c 'uv run python lt_schedule_query.py > lt_schedule_result.json'`),
  and a shell redirection creates/truncates its target file as soon as the
  shell starts, before the Python query has run or failed. A failed
  attempt can therefore leave an **empty or partially-written**
  `lt_schedule_result.json` rather than no file at all — do not treat the
  file's mere presence as evidence the schedule query completed; check
  that it parses as valid, non-empty JSON with the fields you expect.

### 2b. Read the issue day from THIS deployment's live config

The live config is authoritative, not this document. **The config
directory is not a fixed path** — `ForecastConfig` builds it from two env
vars: `ieasyhydroforecast_configuration_path` (the base config path) plus
`ieasyhydroforecast_ml_long_term_configuration` (the long-term config
subfolder name). Do not assume a particular subfolder name. Read both env
vars directly from the deployment's env file — the same `<env_file>`
section 3's command takes:

```bash
grep -E '^(ieasyhydroforecast_configuration_path|ieasyhydroforecast_ml_long_term_configuration)=' <env_file>
```

**Do not try `docker exec <container> printenv ...` instead — it is a dead
end here.** Neither variable is ever set as container-level environment
anywhere in this stack (checked across every compose file:
`sapphire/docker-compose.yml`, `bin/docker-compose-luigi.yml`,
`bin/docker-compose-dashboards.yml`). Both are loaded at Python runtime by
`load_dotenv()` (`apps/pipeline/src/environment.py`) into that one
process's own memory — invisible to a fresh `docker exec` shell even in
the same container. `luigi-daemon`, the only long-lived container in this
path, runs bare `luigid` with none of these variables set at all; the
long-term containers themselves are `--rm` and gone by the time you would
think to inspect them. A `printenv` here returns empty output, which is
easy to misread as "the config is unset" — it only means you asked a
process that never had the variable.

Join the two values yourself:

```bash
LT_CONFIG_DIR="<value of ieasyhydroforecast_configuration_path>/<value of ieasyhydroforecast_ml_long_term_configuration>"
grep operational_issue_day "$LT_CONFIG_DIR"/*.json
```

Deliberately **not** `grep -h`: you need the filename in the output to know
which mode each issue day belongs to (`grep -h` suppresses it, leaving a
column of numbers with no way to tell `month_0` from `quarter`).

Example values recorded here for orientation only, **last verified
2026-09-11** against `doc/deployment.md`'s cron block: `tjhm` uses
`operational_issue_day=1` for all operational modes; `kghm` uses `10` and
`25` depending on mode (`month_0=10`). Do not trust these numbers over the
live config — re-check them for the deployment you are working on.

### 2c. The date you will pass is the issue date, not the target month

`<YYYY-MM-DD>` in the command (section 3) is the forecast **issue date**
— the `date` column value the operational run would have written — not
the month/quarter/season being forecast. It must equal the mode's
scheduled issue date exactly (see section 4); the recovery refuses near
misses rather than snapping to the nearest scheduled date.

### 2d. Confirm the recovery's dependencies are reachable

The recovery runs no preprocessing and will fail on missing inputs rather
than fetching them. Before running it, confirm:

- The SAPPHIRE API gateway is reachable and ready:
  `curl -fsS http://localhost:8000/health/ready`
- The preprocessing database already holds the input history the member
  models need for the missed period (this recovery does not re-run
  runoff, gateway, or any other preprocessing step).
- The deployment's long-term configs and model artifacts for the affected
  mode are present and unchanged since the last successful operational
  run of that mode.

### 2e. Confirm no long-term forecast is in flight

The recovery's existing-row guard and the actual database write are two
separate steps with the whole model run in between; it is not a database
lock. A concurrent operational run, another recovery, or any manual
writer that inserts a row for the same `(horizon_type, horizon_value,
issue_date)` during that window will be silently overwritten by this
run's upsert. Same-daemon Luigi tasks are serialized against each other
via the `lt_memory` resource, but a direct/local run, a different Luigi
daemon, or a manual writer is not protected by that.

Before running the recovery:

1. Check the Luigi scheduler UI (`http://localhost:8082`, or the port
   configured for your deployment) for any RUNNING or PENDING
   `RunLongTermForecast` task.
2. Check `docker ps` for any running `lt_forecast_*` or `lt_recovery_*`
   container.
3. If the long-term cron entry (4) could fire during your recovery
   window, pause it (comment out the crontab line, or wait until safely
   past its scheduled time) for the duration of the recovery.
4. **If you commented out the crontab line in step 3, you MUST restore it
   before you consider this incident closed.** Nothing later in this
   runbook does that for you — do it as the last step of section 12 (see
   the reminder there), not as an afterthought. A recovery that leaves the
   long-term cron entry disabled will silently cause the *next* scheduled
   long-term forecast to be missed too, turning a one-off recovery into a
   second incident. Verify with `crontab -l` that the line is present and
   uncommented before you finish.

---

## 3. The command

Run from the repository root, on the production server, one mode and one
date per invocation:

```bash
bash bin/run_periodic_maintenance.sh lt_recovery <env_file> <mode> <YYYY-MM-DD>
```

Example (illustrative values only — confirm your own mode and date from
sections 2a-2c before running):

```bash
bash bin/run_periodic_maintenance.sh lt_recovery /data/<data_folder>/config/<env_file> month_0 2026-08-01
```

---

## 4. Preconditions and their refusals

The recovery refuses (does not run the forecast) when:

- **Outside the recovery window.** `<YYYY-MM-DD>` must fall in the
  current or previous calendar month, evaluated against the recovery
  process's own local clock (`lt_recovery.check_recovery_window`). Older
  gaps are out of scope for this runbook — see section 13.
- **Not the mode's scheduled issue date.** The date must exactly equal
  the day the mode's member models are scheduled to run
  (`lt_recovery.resolve_scheduled_models`). A date a few days off the
  scheduled issue date is refused, not snapped to the correct date — the
  refusal message names the date to use instead. Re-run with that exact
  date.
- **Member rows already exist.** If any member row already exists for the
  requested `(horizon_type, horizon_value, issue_date)`, the whole
  request is refused, even if only one station's row exists and the rest
  of the month is genuinely missing. Do not follow the refusal message's
  suggestion to delete the existing rows without first confirming, via
  the coverage check in section 6, whether the existing data is actually
  complete.

**Clock caveat.** Eligibility is evaluated using the recovery process's
own local, naive clock — the clock inside the container that runs it, not
your terminal's clock and not necessarily UTC. Near a month boundary, a
host-side check and the deployed container's decision can disagree about
which dates are currently recoverable. If a date is refused as "in the
future" or "outside the window" unexpectedly close to a boundary, this is
the likely cause — check the container's own local time before assuming
the refusal is wrong.

---

## 5. Reading the outcome

The wrapper exits `0` when rows were written and confirmed read back from
the database, and non-zero otherwise. A non-zero exit means the recovery
was **NOT CONFIRMED — it does not mean the database is unchanged.** The
three-way distinction the recovery logic itself uses (confirmed /
declined / failed) does not survive as this shell exit status: any
non-zero child status becomes a Luigi task failure, and the wrapper
returns that failure as a single non-zero code. To find out which of the
two non-success cases happened, read the log (see section 7) for the
literal words:

- **`REFUSED`** — declined. Nothing was written by this run (though see
  the config-write caveat in section 8). This is **not** proof the month
  is complete: the existing-row guard can decline on a single existing
  row, which may mean only one station's row exists and the rest of the
  month is still missing.
- **`FAILED`** — either the recovery could not even be attempted
  (misconfiguration, a query error, an unexpected error before anything
  ran — nothing written), or it started and failed partway (the forecast
  run or the read-back itself failed) — in which case rows may be absent,
  partial, or complete. A read-back failure in particular says nothing
  about what was actually written; check the database directly rather
  than trusting the log alone.

Do not re-run on a non-zero exit without first checking the database
state — an unconditional re-run will be refused by the existing-row guard
if any row already landed from the failed attempt.

---

## 6. Exit 0 is partial acceptance — always run the coverage check

The recovery's success criterion is deliberately weak: **one** finite-
valued member row with the recovery flag is enough for exit `0`. Exit `0`
does **not** mean every station and every member model for that date were
written — only that at least one was.

After every exit `0`, run a coverage check against the postprocessing
database before considering the recovery complete.

**Two things about the query below are easy to get wrong and will make
you trust a bad result:**

- **`q` and `flag` are both nullable columns.** A plain `COUNT(*)` /
  `COUNT(DISTINCT code)` counts every row that merely *exists* for the key,
  including a row with `flag` unset or `q` NULL — which is not usable
  recovered data. The recovery's own read-back requires `flag = 1` AND a
  genuinely finite `q` before it counts a row as recovered
  (`lt_recovery.count_member_rows`, via `_is_usable_value` and Python's
  `math.isfinite()`); your coverage check must use the same test. **"Finite"
  is not the same as "not null."** `q` is a Postgres `double precision`
  column, which can hold `Infinity` or `-Infinity` as well as NULL — a
  divide-by-zero upstream can persist one of those, and `q IS NOT NULL`
  alone would still count it as recovered coverage. The query below
  excludes `q IS NULL` and both infinities explicitly
  (`q > '-Infinity' AND q < 'Infinity'`) so it matches that same finiteness
  test, or it will report "complete" while some stations silently have no
  usable value at all. The query below reports **both** numbers side by
  side (rows/sites present vs. usable recovered rows/sites) so you see the
  gap between them instead of only the reassuring one.
- **`horizon_type` is stored as the UPPERCASE enum NAME, not the config's
  lowercase value.** The live config and `ForecastConfig.get_horizon_type()`
  return `month` / `quarter` / `season` (lowercase); the database column is
  a Postgres enum whose values are the literal names `MONTH` / `QUARTER` /
  `SEASON`. Substituting the lowercase config value into `<HORIZON_TYPE>`
  below either errors or (depending on your psql settings) silently matches
  nothing — either way you get a false "nothing was recovered". Map it
  yourself: `month` → `MONTH`, `quarter` → `QUARTER`, `season` → `SEASON`.
  (The `model_type` column has the same NAME-vs-label distinction, already
  noted below.)

Also note that the raw `model_type` column stores the model's enum NAME,
not its short display label — the ensemble aggregates appear as
`ENSEMBLE_MEAN`, `SKILLED_MEAN`, `NAIVE_MEAN` in this column, not `EM` /
`Skilled Mean` / `Naive Mean`
(`sapphire/services/postprocessing/app/models.py:ModelType`).

```bash
# POSTGRES_USER/POSTGRES_DB vary by deployment; read them from the
# running container instead of hardcoding "postgres" / "postprocessing_db".
PP_USER="$(docker exec sapphire-postprocessing-db printenv POSTGRES_USER)"
PP_DB="$(docker exec sapphire-postprocessing-db printenv POSTGRES_DB)"

docker exec -i sapphire-postprocessing-db \
  psql -X -v ON_ERROR_STOP=1 -U "$PP_USER" -d "$PP_DB" -P pager=off <<SQL
SELECT
  model_type,
  COUNT(*) AS rows_present,
  COUNT(DISTINCT code) AS sites_present,
  COUNT(*) FILTER (WHERE flag = 1 AND q IS NOT NULL
                    AND q > '-Infinity' AND q < 'Infinity')
    AS usable_recovered_rows,
  COUNT(DISTINCT code) FILTER (WHERE flag = 1 AND q IS NOT NULL
                                 AND q > '-Infinity' AND q < 'Infinity')
    AS sites_with_usable_recovered_row,
  array_agg(DISTINCT code) FILTER (WHERE flag = 1 AND q IS NOT NULL
                                     AND q > '-Infinity' AND q < 'Infinity')
    AS codes_with_usable_recovered_row
FROM long_forecasts
WHERE horizon_type = '<HORIZON_TYPE>'   -- UPPERCASE enum NAME, see mapping above
  AND horizon_value = <HORIZON_VALUE>
  AND date = '<YYYY-MM-DD>'
  AND model_type NOT IN ('ENSEMBLE_MEAN', 'SKILLED_MEAN', 'NAIVE_MEAN')
GROUP BY model_type
ORDER BY model_type;
SQL
```

**What the `flag` values you're filtering on mean.** `long_forecasts.flag`
is written by the recovery itself, not by this query:

- `flag = 0` — an ordinary operational row (not relevant to a recovery
  check).
- `flag = 1` — this model produced a **non-NaN** value for this station
  and the recovery marked it recovered. `apply_success_flag` stamps
  `flag = 1` on anything that is not NaN — that includes a persisted
  `Infinity`/`-Infinity` from an upstream divide-by-zero, which is not
  usable. Do not read `flag = 1` alone as "usable"; that is exactly why
  the query above also requires `q` to be finite.
- `flag = 2` — this model was attempted for this station, but its value
  was missing or NaN. The recovery writes `flag = 2` deliberately
  (`apply_success_flag`'s own intent: "a recovery must never dress a
  missing value up as a recovered one") — it is honest bookkeeping about
  a failed attempt, not query error or data corruption. A `flag = 2` row
  always has `q IS NULL`.

This is exactly why `rows_present`/`sites_present` and
`usable_recovered_rows`/`sites_with_usable_recovered_row` can diverge for
one model: every station can get a row (so `rows_present` reads as full
coverage) while every one of them is `flag = 2`, leaving
`usable_recovered_rows` at zero. **A model with rows but zero usable rows
failed for every station it ran against — treat that as a real failure,
not a query artifact.**

**The recovery's own per-model SUCCESS/FAILED summary is not a substitute
for this check.** That summary reflects whether the model's code path
completed without raising, not whether it produced a usable value — a
model can log SUCCESS and still have written `flag = 2` (no value) for
every station. The database, via this query, is the authority on what was
actually recovered; this divergence between "ran without erroring" and
"produced a value" is the concrete reason this coverage check exists at
all.

If a model shows `usable_recovered_rows = 0` across all its expected
stations: **escalate, do not re-run.** The existing-row guard will refuse
a re-run anyway (rows already exist for the key, even at `flag = 2`), and
a `flag = 2` outcome across the board means that model's inputs or logic
failed for this date — a problem for the long-term owner to diagnose, not
something a repeated recovery attempt fixes.

The acceptance criterion is `usable_recovered_rows` /
`sites_with_usable_recovered_row` — **not** `rows_present` /
`sites_present`, which merely proves a row exists in some state.

**Do not require every configured member model.** The recovery only runs
models scheduled for the recovered month
(`lt_recovery.resolve_scheduled_models` — see the "scheduled=..." field in
the recovery's own log line, section 2a). A member whose `forecast_months`
excludes the effective month is *correctly* absent from this query's
results; do not treat that as a gap. The expected coverage is
**scheduled models × configured stations for this date**, not configured
models × stations — check the log line for which models were actually
scheduled before deciding anything is missing.

**Don't trust `sites_with_usable_recovered_row` as a bare count either.**
It is a count, not a set: if an obsolete or wrong station code happens to
be present, the count can coincidentally match the expected number while
a real station is missing. Compare the actual
`codes_with_usable_recovered_row` list against the deployment's configured
codes (`ieasyforecast_config_file_station_selection`), not just its
length.

To read a code in that list as a named station, do not look in the
station-selection file above — it holds only a bare list of selected
codes (`stationsID`), no names. The code → name mapping lives in the file
named by `ieasyforecast_config_file_all_stations` (`name_ru` field per
station code).

If, after applying both corrections above, any scheduled member model is
missing entirely, or its usable coverage is short of the expected station
set:

- **Stop. Do not re-run the recovery command.** A re-run will either be
  refused outright (rows already exist for this key) or, if it somehow
  is not refused, will overwrite every field of the rows that already
  exist — an unconditional "just run it again" is not safe here.
- **Do not follow the runtime refusal message's suggestion to delete the
  existing rows.** That message is written for a different situation (an
  operator intentionally re-running a full month); deleting rows here
  destroys the partial progress this run already made.
- Escalate to the long-term owner with the coverage-check output.

To check one station's rows (`19999` is the placeholder sample code — do
not use a real station code in a shared log or file), add:
`AND code = '19999'` to the `WHERE` clause above.

---

## 7. Where the evidence lives

The child container that runs the recovery is removed as soon as its
logs are captured, so `docker logs <container>` is **not** available
after the run — you cannot go back and inspect it later the way you might
for a long-running service.

- **On success**, the full container log is written to a retained file
  under the deployment's intermediate-data directory:
  `<data_ref_dir>/intermediate_data/docker_logs/log_lt_forecast_<mode>_<issue_date>_<timestamp>.txt`
- **On failure**, that success-path file is never written. Look first in
  the SAME `docker_logs/` directory for a failure log named
  `failure_log_<epoch-seconds>.txt` — the name is a raw Unix timestamp,
  not the mode or date. **This filename pattern is shared by every task
  built on `DockerTaskBase`, not just the recovery** — preprocessing,
  gateway, and any other pipeline task that fails writes a
  `failure_log_<epoch>.txt` into this same directory, with nothing in the
  name to say which task wrote it. The newest such file is only the
  recovery's if nothing else in the pipeline failed around the same time.
  Before trusting one: check its modification time falls inside the
  window you actually ran the recovery command, and open it to confirm the
  content is about the mode/model you recovered (the log body is the raw
  child output, so the forecast mode name should appear in it) rather than
  an unrelated task. This write is also best-effort: it happens as part of
  the failure-notification path and is wrapped in its own error handling,
  so it can fail to appear (silently beyond a printed warning) if that
  write itself errors. If no matching `failure_log_*.txt` file is present,
  fall back to the terminal output from when
  `bin/run_periodic_maintenance.sh` was running (`docker compose run` is
  attached, not detached). Capture the terminal output before it scrolls
  out of your scrollback if you need to hand it to someone else for
  escalation — but **do not simply pipe the command through `tee`**: a
  plain `bash bin/run_periodic_maintenance.sh ... | tee recovery.log`
  pipeline returns `tee`'s exit status, not the wrapper's, silently
  discarding the non-zero exit that section 5 tells you to read. Either
  run `set -o pipefail` first, check `${PIPESTATUS[0]}` after the pipe
  instead of `$?`, or avoid the pipe entirely and redirect output instead
  (`bash bin/run_periodic_maintenance.sh ... 2>&1 | tee recovery.log;
  exit_code=${PIPESTATUS[0]}`), or capture via `tmux`/`screen` logging,
  which does not touch the exit status at all.

---

## 8. Side effects, even on a decline

A successful run overwrites the mode's `{model}_forecast.csv` and, if it
already exists, the hindcast CSV. These are accepted, deliberate side
effects of the recovery and are not what determines success — success is
defined on database rows only (see section 6).

More importantly: **stage 1 of the recovery loads and synchronizes each
member model's configuration, and this writes each member model's
`general_config.json` before the existing-row guard has a chance to
decline the request.** This means a `REFUSED` outcome is not fully
side-effect-free on disk, even though "nothing was run" in the refusal
log line is accurate for the forecast and the database.

---

## 9. What the recovery does and does not do

- It runs no preprocessing. Inputs (runoff, meteo, snow, configs, model
  artifacts) must already be present; a missing input is a failure, not
  something the recovery will fetch for you.
- It makes exactly one attempt. A dated recovery is deliberately never
  retried automatically, because a second automatic attempt could write
  over the rows the first attempt just wrote.
- A manual re-run of this command is **not** made safe by any durable
  marker on disk — the container that ran it is removed (`--rm`) and its
  internal completion marker does not survive that removal. The **only**
  thing that stops an unwanted second write is the existing-row database
  guard described in section 4. Treat that guard, not any marker file, as
  the safety mechanism.

---

## 10. This is not a cron job

Do not schedule this command to run periodically. On every healthy run
where the month already has data, the existing-row guard will refuse it
— that is by design, not a bug to route around. But do not read that as a
concurrency protection: the guard is a before/after check on row
existence, not a database lock (see section 2e), and it protects nothing
while a run is actually in flight. Two recoveries — or a recovery and a
concurrent operational run — started close enough together can both read
"no rows yet" before either one writes, so both proceed and the later
upsert silently overwrites the earlier one. The guard only stops a
*second, later* invocation once the first one has already finished
writing; it does not stop two invocations racing each other. This is
exactly why section 2e requires you to confirm no long-term forecast is
in flight before running the command — the guard is not a substitute for
that check, for a scheduled recovery colliding with itself or with
anything else.

---

## 11. Developers: local rehearsal

To rehearse this against a local/dev stack (never a production database)
before running it for real, see `doc/dev/testing_workflow.md`, section
"Long-term recovery of ONE missed month" — it uses
`apps/run_locally.sh maintenance:long_term_forecasting` and documents the
same clock and safety caveats as this runbook, in more local-dev detail.

---

## 12. The postprocessing follow-up — recovering the dashboard view

The command in section 3 regenerates **member** forecast rows only. The
forecast dashboard's headline number for a month/quarter/season is an
**ensemble aggregate** (EM / Skilled Mean / Naive Mean), which is derived
from members separately. A "successful" member recovery (section 6) still
leaves the operator's own dashboard view empty until the ensembles are
rebuilt.

**Concurrency preflight — do this before running the command, and do not
trust a fixed container name to tell you the truth.** Confirm no other
invocation is already in flight: check
`docker ps --filter label=com.docker.compose.service=periodic-maintenance`
and the Luigi UI (`http://localhost:8082`) for a running
`RunPeriodicMaintenanceWorkflow`/`LongTermPostProcessingMaintenance` task.
**Do not filter by the container name
(`sapphire-pipeline-periodic-maintenance`) configured in
`bin/docker-compose-luigi.yml` — it will not match anything.** This task
is started with `docker compose ... run` (confirmed in
`bin/run_periodic_maintenance.sh`), and Compose does not apply a service's
`container_name` to a `run` (one-off) container; it generates a unique
per-invocation name instead (verified directly: a `run` container for a
service with a fixed `container_name` gets a name like
`<project>-<service>-run-<hash>`, never the configured fixed name — even
when nothing else is running). A name-based `docker ps` filter can report
"nothing running" while a follow-up is genuinely in progress. The Compose
*label* `com.docker.compose.service=periodic-maintenance` is set on every
container Compose creates for this service, one-off or not, and is the
check that actually works.

If one is running, wait for it to finish rather than starting a second —
**but do not assume anything stops you if you don't wait.** There is
close to no real protection here, and this runbook previously overstated
what exists:

- **No Docker-level lock.** Because `run` ignores `container_name` (above),
  two concurrent invocations do not collide on a container name at all —
  there is no "name already in use" error to catch a second run. Nothing
  at the Compose/Docker layer stops two invocations from starting side by
  side.
- **No Luigi resource lock.** The `LongTermPostProcessingMaintenance` task
  itself declares no `resources` entry (unlike `RunLongTermForecast`'s
  `lt_memory`), so Luigi's resource-limiting mechanism, which does protect
  the section-3 recovery, does not apply to this follow-up.
- **The one thing that does help, and only partially:** neither
  `RunPeriodicMaintenanceWorkflow(task_type="long_term")` nor the
  `LongTermPostProcessingMaintenance` task it requires takes any other
  parameter, so two invocations of this command resolve to the identical
  Luigi task ID. If — and only if — both invocations reach the **same**
  central scheduler (the `luigi-daemon` container this wrapper always
  points at), that scheduler will not hand the same task to a second
  worker while the first is still running it; the second
  `docker compose run` process typically waits and then reports success
  once the first one finishes, without re-running the script itself. This
  gives no protection at all across a different Luigi scheduler (a
  different deployment, or a `--local-scheduler` invocation), and none
  whatsoever against a direct/manual run of
  `postprocessing_maintenance_long_term.py` outside Luigi.

Treat the label-based `docker ps` check and the Luigi UI, done by hand
before you run the command, as the actual concurrency control — not the
container name, and not an assumption that Luigi or Compose will stop a
second run for you.

**Marker preflight — also do this before running the command.** This
task's Luigi completion marker is date-keyed and can make the command
silently do nothing even when nothing else is running. Its `output()` is
`<data_ref_dir>/intermediate_data/marker_files/maintenance_lt_postproc_<YYYY-MM-DD>.marker`
(today's date, by default), and `DockerTaskBase.execute_with_retries`
writes that marker as soon as the child container exits `0` —
**including the monthly-tier early-exit case** described below, where the
script does nothing at all and still exits `0`. The marker directory is
bind-mounted (unlike the `--rm` container that would otherwise take any
in-container state with it), so it persists across runs, and Luigi checks
it *before* deciding whether to launch the container at all.

Concretely: if this follow-up already completed today for any reason —
cron entry (6) fired, or this same command was already run once today,
regardless of mode — today's marker already exists, Luigi will consider
`LongTermPostProcessingMaintenance` complete, and running the command
again reports success **without launching the Python script at all**.
That is fatal to this workflow, because you are typically running this
*after* recovering member rows in section 3: a marker written earlier
today, before that recovery, means this follow-up has **not** run against
the data you just recovered, no matter what its reported exit status says.

Check before trusting a success from this command:

```bash
ls -la <data_ref_dir>/intermediate_data/marker_files/maintenance_lt_postproc_$(date +%F).marker
```

If that file exists and its modification time is **before** you ran the
section-3 recovery, the follow-up has not (yet) run against the recovered
data — the row check below would be confirming a stale rebuild, not a
fresh one. This runbook does not have a confirmed, supported way to force
a same-day rerun once that marker exists — do not delete the marker file
yourself; nothing in this codebase documents that as a safe operation, and
this runbook will not invent one. Escalate to the long-term owner for a
safe way to force a same-day rerun. (The marker is date-keyed, so it
starts fresh at local midnight on its own — not a usable answer when the
dashboard needs to be fixed now.)

This runbook uses `bin/run_periodic_maintenance.sh long_term <env_file>`
(the same wrapper section 3 uses for the recovery itself, and the same one
cron entry (6) runs) — **not** the older
`bin/bimonthly_long_term_postprocessing.sh maintenance`, which is marked
`[Legacy]` in `bin/README.md` and should not be used here. Both invoke the
identical Python file, `postprocessing_maintenance_long_term.py`, but they
launch it differently. The legacy script's `run_container` helper
unconditionally force-removes any existing container with the same fixed
name (`docker rm -f postprc-lt-maintenance`) before starting a new one, so
a concurrent second invocation of the *legacy* script would silently kill
the first one's run. The Luigi path removes that specific hazard —
`run_docker_container` (`apps/pipeline/pipeline_docker.py`) gives every
container it launches a per-attempt, timestamp-suffixed name, so two runs
launched through Luigi cannot collide on that inner container name either
— but do not read "no name collision" as "safe to run twice." As
described above, the outer `docker compose run` container for this task
also never uses the fixed `container_name` configured in
`bin/docker-compose-luigi.yml` (Compose ignores it for `run`), so two
concurrent invocations do not hit a Docker naming conflict at either
layer — nothing stops them starting side by side, short of the narrow
same-scheduler task-identity behavior described above. There is no
container-name-based or Compose-level lock here at all; the concurrency
and marker preflights above, done by hand, are what actually protect the
operator.

**Scope caveat: this is not scoped to your mode or date.** Unlike section
3's recovery command, this follow-up takes no mode or date argument. Each
time it runs, it re-scans **every configured station** against **every**
monthly/quarterly/seasonal gap inside each tier's lookback window (see the
window bullet below) — not just the mode and date you just recovered. Running
it to pick up one recovered month can also regenerate or rewrite ensemble
rows for unrelated stations and periods that happen to fall inside the
scan windows. That is expected behavior, not a bug, but it means the
blast radius of this command is the whole deployment's recent long-term
ensembles, not your one incident.

Run the follow-up:

```bash
bash bin/run_periodic_maintenance.sh long_term <env_file>
```

This invokes `postprocessing_maintenance_long_term.py` — the same Python
file the legacy `bimonthly_long_term_postprocessing.sh maintenance`
command runs, so the behavior described below is identical regardless of
wrapper. Before treating this as "the recovery is now visible on the
dashboard," understand what this command actually does:

> **Headline caveat: a clean monthly tier silently aborts the entire
> run, including quarterly and seasonal.** This is a property of
> `postprocessing_maintenance_long_term.py` itself, not of which wrapper
> launched it — switching wrappers does not change it. The script processes the
> monthly tier first. If the monthly gap scan finds nothing to fill (no
> gap, or several other empty-input cases — no monthly combined data, no
> monthly skill metrics, no monthly forecast data for the gap years, or
> no new ensemble rows generated) it exits the **whole process**
> immediately, before the quarterly and seasonal gap-fill blocks are ever
> reached. **If you just recovered a `quarter` or `season` mode and the
> monthly tier happens to be clean, running this command rebuilds
> nothing for your recovered mode at all — silently, while still
> reporting success.** Do not infer that the quarterly/seasonal ensembles
> were rebuilt just because this command ran. Always verify the specific
> tier you recovered (see the row check below), never trust the exit
> status alone.
>
> **If this blocks a quarter/season you need rebuilt now: there is no
> command-line workaround** — the script has no flag to skip the monthly
> tier or start at quarterly. Do not attempt to force it by fabricating a
> monthly gap. Escalate to the long-term owner with: the mode/date you
> recovered in section 3, the section-6 coverage-check output confirming
> member rows exist, and a note that the monthly tier exited clean before
> reaching quarterly/seasonal. This is a real gap in the tooling (no
> per-tier or per-mode entry point), not something this runbook can route
> around safely.

Other things to know before relying on this command:

- **Each tier has its own lookback window, with different defaults.**
  Monthly gap-fill looks back `POSTPROCESSING_GAPFILL_WINDOW_MONTHS`
  (default 3 months); quarterly uses
  `POSTPROCESSING_GAPFILL_WINDOW_QUARTERS` (default 2 quarters);
  seasonal uses `POSTPROCESSING_GAPFILL_WINDOW_SEASONS` (default 1
  season). One window setting does not govern all three tiers — check
  the recovered month/quarter/season actually falls inside the relevant
  tier's window, or the gap-fill will not see it as missing at all.
- **`<env_file>` cannot change the monthly window through this
  command.** `POSTPROCESSING_GAPFILL_WINDOW_MONTHS` is read from the
  process environment before the deployment environment file is loaded,
  and the wrapper script does not forward it into the container. Setting
  it in `<env_file>` has no effect here.
- **This wrapper's exit `0` is not proof your tier was rebuilt — for a
  structural reason, not a swallowed status.** Unlike the legacy script,
  `bin/run_periodic_maintenance.sh` genuinely propagates the container's
  own exit status (through Luigi's `[retcode]` mapping — see section 5).
  But `postprocessing_maintenance_long_term.py` itself exits `0` as soon
  as the monthly tier comes back clean, *before* the quarterly and
  seasonal blocks ever run (the headline caveat above). So an honest exit
  `0` can still mean "the container succeeded at doing nothing for your
  tier." Verify by querying rows (below) every time — never by the exit
  status alone, honest or not.
- **This only fills a missing key — it does not refresh a stale existing
  one.** The gap detectors (`gap_detector.detect_missing_monthly_ensembles`
  and its quarterly/seasonal equivalents) find gaps purely by whether an
  aggregate row is *absent* for a given key. If an ensemble row already
  exists for the period you recovered — for example, written earlier from
  an incomplete member set, before the missing members were recovered —
  this command sees the key as already present and leaves its value
  unchanged. The row check below only confirms an aggregate row *exists*;
  it does not confirm its value reflects the member data you just
  recovered. If you suspect a stale aggregate rather than a missing one,
  that is not something this command fixes — escalate rather than
  assuming the row check clears it.
- **Ensembles this command creates carry `flag=0`, and what that means for
  your recovery's audit trail differs by tier — do not assume one
  behavior covers all three:**
  - **Monthly** (`_write_monthly_ensemble_to_api`) filters its input to
    ensemble rows only (`model_short` in `{EM, Naive Mean, Skilled Mean}`)
    before writing — it never touches a row whose `model_type` matches one
    of your recovered member models, so it cannot overwrite a `flag=1`
    member row regardless of date key. Safe for the audit trail.
  - **Quarterly and seasonal go through a different, shared writer**
    (`_write_aggregated_forecasts_to_api`) that has **no such filter** —
    by its own docstring it "writes both individual model aggregates and
    ensemble rows," and it hardcodes `flag=0` on every record it sends.
    That means it can rewrite the very same member rows section 3 just
    wrote with `flag=1` — the risk depends entirely on whether the two
    writes land under the same key:
    - **Quarterly, default configuration** (`SAPPHIRE_SKILL_LEAD_AWARE`
      unset/false): this writer keys its `date` column to `valid_from`
      (the period start) by default, not the issue date your section-3
      member rows used — **but this is a stated condition, not a blanket
      exemption** (filed as PP-061). `valid_from` is only guaranteed to
      differ from the recovered issue date if this deployment's issue day
      and lead do not happen to land on the same calendar date; a
      deployment with issue day 1 and lead 0 makes the quarter's
      `valid_from` and the recovered issue date identical, and nothing in
      `lt_recovery.py`'s scheduling forbids issuing a recovery on that
      date. **Check this deployment's actual issue day and lead for this
      mode (section 2b) before assuming no collision.** If they can
      coincide, treat this exactly like the seasonal / lead-aware-quarterly
      case below: **capture the section-6 query output before running this
      follow-up**, because the `flag=1` recovery marker is not guaranteed
      to survive it. Only once you have confirmed `valid_from` cannot equal
      the recovered issue date for this deployment and mode does "different
      date -> no collision" hold — and even then, a verification query
      filtered on the issue date will find **no** rows from this follow-up
      even after a successful gap-fill; query by `valid_from`/`valid_to`
      for this tier instead.
    - **Seasonal always, and quarterly only when `SAPPHIRE_SKILL_LEAD_AWARE`
      is enabled**: this writer keys `date` to the row's own issue date
      instead — the **same** key your section-3 member rows used, for the
      **same** `model_type` (the individual member, not just the
      ensemble aggregates). This lands on the identical
      `(horizon_type, horizon_value, code, date, model_type, valid_from,
      valid_to)` row and the unconditional `flag=0` overwrites the
      `flag=1` recovery marker — destroying the audit trail this runbook
      told you to rely on in section 6. There is no way to prevent this
      from the command line; if you need durable proof of what section 3
      recovered for a season (or a lead-aware quarter), **capture the
      section-6 query output before running this follow-up** — after
      running it, the database itself no longer distinguishes the
      recovered rows from ordinary gap-filled ones. Query by the issue
      date for this tier, matching section 3's date — but be aware that,
      post-follow-up, a `flag=1` filter will no longer find what you
      recovered.
  - There is no way to tell a gap-filled ensemble from an ordinary
    operational one by flag alone in any tier; the member-level `flag=1`
    from section 3 is meant to be that record, except whenever this
    follow-up's write lands on the same seven-column key
    (`horizon_type, horizon_value, code, date, model_type, valid_from,
    valid_to`) as the recovery's own write — seasonal and lead-aware
    quarterly always, and default (non-lead-aware) quarterly whenever
    `valid_from` coincides with the recovered issue date (see above) — in
    which case this follow-up erases it.

Its scheduled cron entry (6) only fires on the 1st of odd months — that
is why you are running it by hand now instead of waiting for the cron
job to pick it up.

**Verify per tier** with the same style of query as section 6 (same
`-X -v ON_ERROR_STOP=1` and dynamic `$POSTGRES_USER`/`$POSTGRES_DB`
against `long_forecasts` directly — but note `flag = 1` does not apply
here: every row this follow-up writes carries `flag=0` regardless of
tier, so your acceptance test for THIS check is a genuinely finite value
present — `q IS NOT NULL AND q > '-Infinity' AND q < 'Infinity'`, the same
finiteness test as section 6, not a bare `q IS NOT NULL` (which would
count a persisted `Infinity`/`-Infinity` as a usable recovered value) —
not `flag = 1`.

**Only the monthly gap detector has a CSV fallback; do not assume
quarterly/seasonal share it, and do not assume "combined forecasts" means
a different table.** `data_reader.read_monthly_combined_forecasts` reads
the API first and silently falls back to a deprecated CSV file whenever
the API call returns empty — including when the API is reachable but
genuinely has no rows yet, not only when the API is down. A stale CSV can
then make the *monthly* gap scan miss a real gap, or make it think a gap
was already filled. `read_quarterly_combined_forecasts` and
`read_seasonal_combined_forecasts` have **no CSV fallback**: they are
API-only and return an empty frame when the API has nothing, via their
shared `_read_long_combined_forecasts_api`, which itself calls
`read_long_term_forecasts` against the same `long_forecasts` table these
verification queries read directly. "Combined forecasts" names the shape
of the frame these readers return (all member models plus ensembles), not
a separate table — for quarter and season it is `long_forecasts` under
another name. This runbook's own verification queries always go directly
against `long_forecasts`, bypassing these readers entirely either way — do
not substitute a CSV inspection for the database check below for any
tier.

Filter to the recovered mode's `horizon_type`/`horizon_value` and
`model_type IN ('ENSEMBLE_MEAN', 'SKILLED_MEAN', 'NAIVE_MEAN')` (the raw
DB enum names — see the note in section 6), and filter the date column
per the tier-specific key described above: the issue date for monthly
and for seasonal/lead-aware-quarterly, `valid_from`/`valid_to` for
quarterly under the default (non-lead-aware) configuration. Confirm rows
exist for every station before considering the follow-up complete.

**Before you close out this incident:** if you paused the long-term cron
entry in section 2e step 3, restore it now — uncomment the crontab line
and confirm with `crontab -l` that it is active again. This is easy to
forget once the dashboard looks correct, and a still-paused cron entry
will cause the next scheduled long-term run to be missed silently.

---

## 13. Gaps outside the recovery window

There is **no production single-month repair path for any mode** once a
gap falls outside the current/previous calendar month window used by
section 3. Be precise about what is and is not covered:

- **`month_1` through `month_9`, and `seasonal_*` modes**: the P7
  procedure in `doc/prod/historical_backfill_runbook.md` can repair
  these, but only within its fixed scope — the **five complete calendar
  years immediately preceding the current year**, and only at **whole-
  year granularity** (it runs `simulate_forecasts.py --num_months 12
  --all` for an entire year at a time, not one selected month). A gap in
  the **current** year, or a gap more than five years old, is **outside
  P7's scope** and must not be routed to it.
- **`month_0` and `quarter`/`quarterly` are explicitly skipped by P7.**
  These modes have **no documented repair path** for a gap outside the
  two-month recovery window in this runbook.
- **`bin/dev_local_backfill.sh` must NOT be used or named as a repair
  option on a production server.** Its own header states it must never
  be invoked on a production server: it pauses and restores the server's
  crontab, and writes to whatever local databases the local stack points
  at. It is a developer tool, not a production recovery tool. Do not
  suggest it to an operator as an option, even a last-resort one.
- **Everything not covered by the two cases above: escalate to the
  long-term owner.** Do not attempt an ad hoc fix.

---
