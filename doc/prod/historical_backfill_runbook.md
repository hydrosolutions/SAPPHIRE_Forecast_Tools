# SAPPHIRE Forecast Tools — Historical DB Backfill Runbook

Use this runbook when a SAPPHIRE deployment has historical data gaps after
`RunInitializeWorkflow` completes, or when existing service databases are stale
or incomplete. The two most common triggers are: (1) a first deployment where
`RunInitializeWorkflow` ran the short-form initialization path (preprunoff +
LR hindcasts + skill metrics only) and did not populate gateway meteo/snow,
ML hindcasts, conceptual-model hindcasts, long-term hindcasts, or long-horizon
hydrograph aggregations; (2) an update deployment where one or more data types
have been reset, corrupted, or never populated.

---

## Cross-references

- First deployment prerequisites: `doc/prod/first_deploy_checklist.md`
- Update deployment checklist: `doc/prod/update_deployment_checklist.md`
- iEasyHydro HF connectivity troubleshooting: `doc/prod/first_deploy_checklist.md` §1.2

---

> **Sensitive-data rule.** Do not write real station codes, discharge values,
> passwords, or env-file contents into logs or committed artifacts. Use `19999`
> as the sample station code in all examples and verification commands. If a
> verification command requires scoping to a real site, run it interactively and
> do not save the output to a committed file.

---

## Operator setup

Run all server commands from the production server unless a command is
explicitly labelled "local laptop".

Set up the shell once per SSH/local terminal session by sourcing the helper.
Sourcing is required because the helper exports variables and defines
`load_backfill_env` in the current shell.

Server example:

```bash
cd /data/SAPPHIRE_Forecast_Tools
source bin/setup_historical_backfill_env.sh --profile taj
```

Local laptop example:

```bash
cd "$HOME/Documents/GitHub/SAPPHIRE_forecast_tools"
source bin/setup_historical_backfill_env.sh --profile taj --local
```

Explicit env-file example:

```bash
cd "$HOME/Documents/GitHub/SAPPHIRE_forecast_tools"
source bin/setup_historical_backfill_env.sh \
  --env-file "$HOME/Documents/GitHub/taj_data_forecast_tools/config/.env_develop_tjhm"
```

The helper defaults `COMPOSE_ENV_FILE` to `${REPO}/sapphire/.env`, matching
`bin/backup_sapphire_db.sh`. If your deployment stores DB credentials in a
different env file, pass it explicitly:

```bash
source bin/setup_historical_backfill_env.sh \
  --profile taj \
  --compose-env-file /data/taj_data_forecast_tools/config/.env_tjhm
```

Do not print the full env file. If a value must be checked, print only the
variable name and whether it is set.

The helper exports `DATA_PROFILE`, `ORG_SLUG`, `DATA_DIR`, `ENV_FILE_PATH`,
`ENV_FILE`, `COMPOSE_ENV_FILE`, `LOG_DIR`, `REPO`, `SAMPLE_CODE`,
`START_DATE`, and `BACKEND_TAG`, and defines `load_backfill_env`. Later phase
snippets assume this setup command has already been sourced. Use
`load_backfill_env` instead of calling `read_configuration` directly; the
legacy helper calls `exit` for some input errors, while the setup helper checks
common bad-path and bad-suffix cases first. This is not full isolation:
unhandled exits inside `read_configuration` or inside the sourced env file can
still close the interactive shell.

Do not derive `DATA_DIR` from `ORG_SLUG`. Production data directories use the
country profile (`taj`, `uzb`, `kyg`), while the organization slug uses the
hydromet identifier (`tjhm`, `uzhm`, `kghm`). The setup helper maps
`taj -> tjhm`, `uzb -> uzhm`, and `kyg -> kghm` unless `--org` or `--env-file`
is provided.

Before any write phase:

```bash
docker ps
curl -fsS http://localhost:8000/health/ready
```

Expected result:

```text
docker ps returns without error.
gateway readiness returns HTTP 200.
```

If the deployment uses the iEasyHydro HF SSH tunnel, verify the tunnel is up
before phases that call iEH HF:

```bash
systemctl status autossh-ieasyhydro.service --no-pager
```

Assume the tunnel and DG auth already work if gateway succeeded once. Do not
spend the backfill window re-debugging tunnel/auth unless a phase fails with a
fresh connectivity error.

### Pre-flight verification — operator must confirm before P1

These values are deployment-specific. Confirm each before starting any write phase:

- [ ] **ML models configured.** `grep ieasyhydroforecast_available_ML_models ${ENV_FILE_PATH}`
  should return a non-empty comma-separated list (e.g., `TFT,TIDE,TSMIXER`). If empty
  or not set, P5 has nothing to iterate and must be skipped.

- [ ] **Long-term modes configured.** `grep ieasyhydroforecast_ml_long_term_supported_modes ${ENV_FILE_PATH}`
  should return a non-empty comma-separated list of modes (e.g.,
  `month_1,month_2,...,month_9,seasonal_april`). If empty, P7 has nothing
  to fan out over. P7 targets `month_[1-9]` and `seasonal_*`; `monthly`,
  `month_0`, `quarter`, and `quarterly` are not backfilled by the P7 command
  below unless a long-term owner provides a deployment-specific command.

- [ ] **Backend image tag.** `grep ieasyhydroforecast_backend_docker_image_tag ${ENV_FILE_PATH}`
  should return the deployed tag (e.g., `local`, `v1.0.0`). This is read into
  `BACKEND_TAG` by the operator setup. The shared helper defaults an unset tag
  to `local`; confirm that a local image exists before running write phases.

- [ ] **iEH HF reachability.** If your deployment accesses iEH HF via SSH tunnel
  (e.g., SAPPHIRE on AWS, iEH HF on customer LAN), confirm the tunnel is up before
  P1, P2, P9: `ss -tlnp | grep <tunnel-port>`. The runbook's gateway and runoff
  phases depend on it. If you do not know the tunnel port, see
  `doc/prod/first_deploy_checklist.md` §1.2 ("iEasyHydro HF Connectivity").

---

## Required scripts on the server

| Script | Status | Used by |
|---|---|---|
| `bin/setup_historical_backfill_env.sh` | committed; source-only | operator setup |
| `bin/backup_sapphire_db.sh` | committed | P0.5 |
| `bin/initialize_site_backfill.sh` | committed | P2, P4, P8 |
| `bin/purge_site_data.sh` | committed | failure recovery |
| `bin/initialize_snow_history.sh` | committed | P1.5 |
| `bin/backfill_snow_stats_history.sh` | committed | P3 |
| `bin/yearly_runoff_hydrograph_aggregation.sh` | committed | P9 |
| `bin/bimonthly_long_term_skill_metrics_recalculation.sh` | committed | P8 |

Ensure executable bit is set:

```bash
cd "$REPO"
chmod +x \
  bin/backup_sapphire_db.sh \
  bin/initialize_site_backfill.sh \
  bin/purge_site_data.sh \
  bin/initialize_snow_history.sh \
  bin/backfill_snow_stats_history.sh \
  bin/yearly_runoff_hydrograph_aggregation.sh \
  bin/bimonthly_long_term_skill_metrics_recalculation.sh
```

Check presence:

```bash
cd "$REPO"
test -f bin/setup_historical_backfill_env.sh && echo "OK setup_historical_backfill_env.sh"
test -f bin/backup_sapphire_db.sh && echo "OK backup_sapphire_db.sh"
test -f bin/initialize_site_backfill.sh && echo "OK initialize_site_backfill.sh"
test -f bin/purge_site_data.sh && echo "OK purge_site_data.sh"
test -f bin/initialize_snow_history.sh && echo "OK initialize_snow_history.sh"
test -f bin/backfill_snow_stats_history.sh && echo "OK backfill_snow_stats_history.sh"
test -f bin/yearly_runoff_hydrograph_aggregation.sh && echo "OK yearly_runoff_hydrograph_aggregation.sh"
test -f bin/bimonthly_long_term_skill_metrics_recalculation.sh && echo "OK bimonthly_long_term_skill_metrics_recalculation.sh"
```

Do not execute `bin/setup_historical_backfill_env.sh` directly; it is
source-only and does not need executable permissions.

---

## Idempotency model

The service tables have unique keys and bulk upsert code on the deployed branch:

| Table | Unique/upsert key |
|---|---|
| `preprocessing_db.runoffs` | `(horizon_type, code, date)` |
| `preprocessing_db.hydrographs` | `(horizon_type, code, date)` |
| `preprocessing_db.meteo` | `(meteo_type, code, date)` |
| `preprocessing_db.snow` | `(snow_type, code, date)` |
| `postprocessing_db.forecasts` | `(horizon_type, code, model_type, date, target)` |
| `postprocessing_db.long_forecasts` | `(horizon_type, horizon_value, code, date, model_type, valid_from, valid_to)` |
| `postprocessing_db.lr_forecasts` | `(horizon_type, code, date)` |
| `postprocessing_db.skill_metrics` | `(horizon_type, code, model_type, date, horizon_in_year, horizon_value)` |
| `postprocessing_db.bulletins` | `(horizon_type, year, horizon_value, code)` |
| `postprocessing_db.lr_visibility` | `(horizon_type, code, month, horizon_value, year)` |

Therefore successful API writes are safe to re-run. A phase is considered
rerunnable when it writes through these existing API/client paths.

If a phase writes only CSV or cannot prove API upsert success, use the
phase-specific failure recovery section before rerunning. Do **not** edit
`sapphire/services/*` in this runbook.

---

## Acceptance SQL source map

| Query family | Table/columns | Source on `origin/maxat_sapphire_2` |
|---|---|---|
| Runoff coverage | `runoffs.horizon_type`, `code`, `date`, `discharge`, `horizon_value`, `horizon_in_year` | `sapphire/services/preprocessing/app/models.py:16-38` |
| Hydrograph coverage | `hydrographs.horizon_type`, `code`, `date`, `mean`, `q05`, `q95`, `norm`, `previous`, `current` | `sapphire/services/preprocessing/app/models.py:41-79` |
| Meteo coverage | `meteo.meteo_type`, `code`, `date`, `value`, `norm` | `sapphire/services/preprocessing/app/models.py:82-107` |
| Snow coverage | `snow.snow_type`, `code`, `date`, `value`, `norm`, `mean`, `q05`, `q95`, `previous`, `current` | `sapphire/services/preprocessing/app/models.py:110-166` |
| Short forecasts | `forecasts.horizon_type`, `model_type`, `code`, `date`, `target` | `sapphire/services/postprocessing/app/models.py:57-102` |
| Long forecasts | `long_forecasts.horizon_type`, `horizon_value`, `model_type`, `code`, `date`, `valid_from`, `valid_to` | `sapphire/services/postprocessing/app/models.py:105-157` |
| LR forecasts | `lr_forecasts.horizon_type`, `code`, `date` | `sapphire/services/postprocessing/app/models.py:160-193` |
| Skill metrics | `skill_metrics.horizon_type`, `model_type`, `code`, `date`, `n_pairs` | `sapphire/services/postprocessing/app/models.py:196-236` |
| Bulletins and LR visibility | `bulletins.year`, `lr_visibility.year`, horizon/code keys | `sapphire/services/postprocessing/app/models.py:239-306` |

---

## Execution order

```text
P0    Diagnostics and pre-flight inventory
P0.5  Verified DB backup before write phases
P1    Gateway historical ERA5 meteo + raw snow
P1.5  Historical snow value backfill workaround
P2    Daily runoff + day hydrograph historical backfill
P3    Snow stat/norm historical backfill
P4    LR PENTAD + DECAD hindcasts and pentad/decad hydrographs
P5    ML PENTAD + DECAD hindcasts
P6    CM hindcasts (SKIP by default — see P6 callout)
P7    Long-term hindcasts for at least five historical years
P8    Skill metrics recalculation for all populated horizons/models
P9    Monthly + seasonal runoff hydrograph aggregation
P10   Final verification and dashboard smoke test
```

P0 is a read-only gate. Every write phase is conditional: skip it if P0 proves
it is already `DONE`; run it if P0 reports `EMPTY` or `PARTIAL`.

---

## P0: Diagnostic and DB state inventory

### Goal

Build a read-only inventory of every required data type, classify each as
`DONE`, `PARTIAL`, or `EMPTY`, and decide which later phases to run.

### Files

Read-only server paths:

```text
${REPO}/bin/setup_historical_backfill_env.sh
${REPO}/bin/utils/common_functions.sh
${REPO}/bin/backup_sapphire_db.sh
${REPO}/bin/initialize_site_backfill.sh
${REPO}/bin/purge_site_data.sh
${REPO}/bin/initialize_snow_history.sh
${REPO}/bin/backfill_snow_stats_history.sh
${REPO}/bin/yearly_runoff_hydrograph_aggregation.sh
${REPO}/bin/bimonthly_long_term_skill_metrics_recalculation.sh
```

Databases:

```text
sapphire-preprocessing-db / preprocessing_db
sapphire-postprocessing-db / postprocessing_db
```

### Depends on

None.

### Expected duration

15–30 minutes.

### Log location

```text
${ieasyhydroforecast_data_root_dir}/logs/full_backfill_diagnostics/p0_<timestamp>.log
```

### Server commands

```bash
cd "$REPO"
load_backfill_env

export START_DATE="${ieasyhydroforecast_START_DATE:?ieasyhydroforecast_START_DATE missing}"
export END_DATE="$(date -u +%F)"
export P0_LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/full_backfill_diagnostics"
mkdir -p "$P0_LOG_DIR"
export P0_LOG="${P0_LOG_DIR}/p0_$(date +%Y%m%d_%H%M%S).log"

{
  echo "P0 full historical backfill diagnostic"
  echo "repo=$REPO"
  echo "env_file=$ENV_FILE"
  echo "start_date=$START_DATE"
  echo "end_date=$END_DATE"
  echo "backend_tag=${ieasyhydroforecast_backend_docker_image_tag:-local}"
  echo "organization=${ieasyhydroforecast_organization:-unset}"
  echo "run_ml=${ieasyhydroforecast_run_ML_models:-unset}"
  echo "available_ml=${ieasyhydroforecast_available_ML_models:-unset}"
  echo "run_cm=${ieasyhydroforecast_run_CM_models:-unset}"
  echo "lt_modes=${ieasyhydroforecast_ml_long_term_supported_modes:-unset}"
} | tee "$P0_LOG"
```

Deployment env gate:

```bash
{
  echo
  echo "== Deployment env gate values =="
  grep -E '^(ieasyhydroforecast_run_CM_models|ieasyhydroforecast_available_ML_models|ieasyhydroforecast_ml_long_term_supported_modes|ieasyhydroforecast_supported_modes)=' "$ENV_FILE" || true
} | tee -a "$P0_LOG"
```

Use this output to gate P5, P6, and P7:

```text
1. P5 runs only when ieasyhydroforecast_run_ML_models is true and configured ML models are present.
2. P6 runs only when ieasyhydroforecast_run_CM_models is true and the CM coordination gate passes.
3. P7 uses ieasyhydroforecast_ml_long_term_supported_modes; if the env also has
   ieasyhydroforecast_supported_modes, treat it as informational unless the long-term
   owner confirms otherwise.
```

Health and service checks:

```bash
{
  echo
  echo "== containers =="
  docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}'

  echo
  echo "== gateway readiness =="
  curl -fsS http://localhost:8000/health/ready || true
} | tee -a "$P0_LOG"
```

Dashboard host-network check:

```bash
{
  echo
  echo "== dashboard network mode =="
  if docker ps -a --format '{{.Names}}' | grep -qx 'sapphire-dashboard'; then
    docker inspect sapphire-dashboard --format 'sapphire-dashboard NetworkMode={{.HostConfig.NetworkMode}}'
  else
    echo 'sapphire-dashboard not present'
  fi
} | tee -a "$P0_LOG"
```

> **Note: `NetworkMode=host` requirement.** On deployments where iEH HF runs
> on a separate server reached via SSH tunnel (including AWS-hosted SAPPHIRE
> instances), the dashboard container must use `network_mode: host`. If
> `NetworkMode` returns anything other than `host` on such a deployment, expect
> dashboard iEH HF read failures. If a redeploy clobbered the server-side
> patch, re-apply it before the final dashboard smoke test:
>
> 1. Add `network_mode: host` to the active dashboard service in the compose file.
> 2. Remove or ignore `ports:` for that service (host networking binds directly on the host).
> 3. Run `docker compose up -d <dashboard-service>`.
> 4. Re-run the `docker inspect` check above.

Script inventory:

```bash
{
  echo
  echo "== script inventory =="
  for f in \
    bin/initialize_site_backfill.sh \
    bin/purge_site_data.sh \
    bin/backfill_snow_stats_history.sh \
    bin/yearly_runoff_hydrograph_aggregation.sh \
    bin/bimonthly_long_term_skill_metrics_recalculation.sh
  do
    if [ -f "$f" ]; then
      printf "PRESENT %s\n" "$f"
    else
      printf "MISSING %s\n" "$f"
    fi
  done
} | tee -a "$P0_LOG"
```

Preprocessing inventory:

```bash
docker exec -i sapphire-preprocessing-db \
  psql -U postgres -d preprocessing_db -P pager=off -v start="$START_DATE" -v end="$END_DATE" <<'SQL' | tee -a "$P0_LOG"
\echo '== preprocessing runoffs by horizon =='
SELECT
  horizon_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT code) AS sites,
  MIN(date) AS min_date,
  MAX(date) AS max_date
FROM runoffs
GROUP BY horizon_type
ORDER BY horizon_type;

\echo '== preprocessing hydrographs by horizon =='
SELECT
  horizon_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT code) AS sites,
  MIN(date) AS min_date,
  MAX(date) AS max_date,
  COUNT(*) FILTER (WHERE mean IS NOT NULL) AS mean_rows,
  COUNT(*) FILTER (WHERE norm IS NOT NULL) AS norm_rows,
  COUNT(*) FILTER (WHERE previous IS NOT NULL) AS previous_rows,
  COUNT(*) FILTER (WHERE current IS NOT NULL) AS current_rows
FROM hydrographs
GROUP BY horizon_type
ORDER BY horizon_type;

\echo '== preprocessing meteo by type =='
SELECT
  meteo_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT code) AS hru_or_site_count,
  MIN(date) AS min_date,
  MAX(date) AS max_date,
  COUNT(*) FILTER (WHERE value IS NOT NULL) AS value_rows,
  COUNT(*) FILTER (WHERE norm IS NOT NULL) AS norm_rows
FROM meteo
GROUP BY meteo_type
ORDER BY meteo_type;

\echo '== preprocessing snow by type =='
SELECT
  snow_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT code) AS hru_or_site_count,
  MIN(date) AS min_date,
  MAX(date) AS max_date,
  COUNT(*) FILTER (WHERE value IS NOT NULL) AS value_rows,
  COUNT(*) FILTER (WHERE norm IS NOT NULL) AS norm_rows,
  COUNT(*) FILTER (WHERE mean IS NOT NULL) AS mean_rows,
  COUNT(*) FILTER (WHERE q05 IS NOT NULL) AS q05_rows,
  COUNT(*) FILTER (WHERE previous IS NOT NULL) AS previous_rows,
  COUNT(*) FILTER (WHERE current IS NOT NULL) AS current_rows
FROM snow
GROUP BY snow_type
ORDER BY snow_type;

\echo '== preprocessing sample sentinel counts =='
SELECT 'runoffs' AS table_name, horizon_type::text AS subtype, COUNT(*), MIN(date), MAX(date)
FROM runoffs
WHERE code = '19999'
GROUP BY horizon_type
UNION ALL
SELECT 'hydrographs', horizon_type::text, COUNT(*), MIN(date), MAX(date)
FROM hydrographs
WHERE code = '19999'
GROUP BY horizon_type
ORDER BY table_name, subtype;
SQL
```

Postprocessing inventory:

```bash
docker exec -i sapphire-postprocessing-db \
  psql -U postgres -d postprocessing_db -P pager=off -v start="$START_DATE" -v end="$END_DATE" <<'SQL' | tee -a "$P0_LOG"
\echo '== postprocessing LR forecasts =='
SELECT
  horizon_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT code) AS sites,
  MIN(date) AS min_issue_date,
  MAX(date) AS max_issue_date
FROM lr_forecasts
GROUP BY horizon_type
ORDER BY horizon_type;

\echo '== postprocessing short forecasts by model/horizon =='
SELECT
  horizon_type,
  model_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT code) AS sites,
  MIN(date) AS min_issue_date,
  MAX(date) AS max_issue_date,
  MIN(target) AS min_target_date,
  MAX(target) AS max_target_date
FROM forecasts
GROUP BY horizon_type, model_type
ORDER BY horizon_type, model_type;

\echo '== postprocessing long forecasts by model/horizon =='
SELECT
  horizon_type,
  horizon_value,
  model_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT code) AS sites,
  MIN(date) AS min_issue_date,
  MAX(date) AS max_issue_date,
  MIN(valid_from) AS min_valid_from,
  MAX(valid_to) AS max_valid_to
FROM long_forecasts
GROUP BY horizon_type, horizon_value, model_type
ORDER BY horizon_type, horizon_value, model_type;

\echo '== postprocessing skill metrics by model/horizon =='
SELECT
  horizon_type,
  model_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT code) AS sites,
  MIN(date) AS min_date,
  MAX(date) AS max_date,
  MIN(n_pairs) AS min_n_pairs,
  MAX(n_pairs) AS max_n_pairs
FROM skill_metrics
GROUP BY horizon_type, model_type
ORDER BY horizon_type, model_type;

\echo '== postprocessing bulletin and visibility coverage =='
SELECT 'bulletins' AS table_name, horizon_type::text, COUNT(*) AS rows, COUNT(DISTINCT code) AS sites, MIN(year)::text, MAX(year)::text
FROM bulletins
GROUP BY horizon_type
UNION ALL
SELECT 'lr_visibility', horizon_type::text, COUNT(*), COUNT(DISTINCT code), MIN(year)::text, MAX(year)::text
FROM lr_visibility
GROUP BY horizon_type
ORDER BY table_name, horizon_type;
SQL
```

ML DAY/archive starvation diagnostic:

```bash
docker exec -i sapphire-postprocessing-db \
  psql -U postgres -d postprocessing_db -P pager=off <<'SQL' | tee -a "$P0_LOG"
\echo '== ML archive split diagnostic =='
SELECT
  model_type,
  horizon_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT code) AS sites,
  MIN(date) AS min_issue_date,
  MAX(date) AS max_issue_date
FROM forecasts
WHERE model_type IN ('TFT', 'TIDE', 'TSMIXER')
GROUP BY model_type, horizon_type
ORDER BY model_type, horizon_type;
SQL
```

Conceptual-model diagnostic:

```bash
{
  echo
  echo "== conceptual model config diagnostic =="
  if [ "${ieasyhydroforecast_run_CM_models:-}" = "True" ] || [ "${ieasyhydroforecast_run_CM_models:-}" = "true" ]; then
    echo "CM_ENABLED=true"
    echo "CM_PATH_SET=$([ -n "${ieasyhydroforecast_conceptual_model_path:-}" ] && echo yes || echo no)"
    echo "CM_JSON_PATH_SET=$([ -n "${ieasyhydroforecast_PATH_TO_JSON:-}" ] && echo yes || echo no)"
    echo "CM_FILE_SETUP_SET=$([ -n "${ieasyhydroforecast_FILE_SETUP:-}" ] && echo yes || echo no)"
  else
    echo "CM_ENABLED=false"
  fi
} | tee -a "$P0_LOG"
```

Long-term configured-mode diagnostic:

```bash
{
  echo
  echo "== long-term mode diagnostic =="
  printf "%s\n" "${ieasyhydroforecast_ml_long_term_supported_modes:-}" \
    | tr ',' '\n' \
    | sed 's/^/configured_lt_mode=/'
} | tee -a "$P0_LOG"
```

### P0 classification checklist

After the SQL output, mark each item:

| Data type | DONE | PARTIAL | EMPTY |
|---|---|---|---|
| Daily runoff observations | `runoffs/day` min date <= `START_DATE`, max date near today, sites > 0 | rows exist but date range starts after `START_DATE`, ends far before today, or site count is unexpectedly low | no `day` rows |
| Day hydrograph | `hydrographs/day` min date <= `START_DATE`, max date near today, stats populated | rows exist but hydrograph gap remains or stats sparse | no `day` rows |
| Pentad hydrograph | `hydrographs/pentad` rows exist across historical horizon | rows exist but short horizon only | no `pentad` rows |
| Decade hydrograph | `hydrographs/decade` rows exist across historical horizon | rows exist but short horizon only | no `decade` rows |
| ERA5 meteo | `meteo/T` and `meteo/P` min date <= `START_DATE`, max date near today | one type missing or date range short | no rows |
| Raw snow | `snow/HS`, `snow/ROF`, `snow/SWE` min date <= `START_DATE`, max date near today | some types missing or only current forecast rows | no rows |
| Snow stats/norms | `snow` norm and stat columns non-null for historical years | raw snow exists but stat columns mostly null | no stat/norm rows |
| LR hindcasts | `lr_forecasts/pentad` and `lr_forecasts/decade` historical | one horizon missing or short | no rows |
| ML hindcasts | configured ML models have `forecasts/pentad` and `forecasts/decade` archives | only DAY rows or only one horizon/model | no configured ML rows |
| CM hindcasts | RRAM rows exist if CM enabled | CM enabled but only CSV/no DB rows | CM disabled or no rows |
| Long-term hindcasts | `long_forecasts` has month/season rows covering at least five historical years | rows exist for fewer years/modes | no rows |
| Skill metrics | `skill_metrics` has rows for each populated model/horizon and useful `n_pairs` | rows exist but missing models/horizons or n_pairs starved | no rows |
| Monthly/seasonal hydrograph | `hydrographs/month` and `hydrographs/season` rows exist | one horizon missing or too few years | no rows |

> **Warning: partial backfill may reflect stale source CSVs, not a failed write.**
> If P0 inventory shows what looks like a partial historical backfill for a data
> type — for example, `lr_forecasts` rows stopping at a date several years before
> today — verify that the source CSV files on disk are themselves complete before
> concluding that a backfill run failed. The initial migration
> (`RunInitializeWorkflow → InitialApiSync → LinRegInitial`) faithfully ingests
> whatever CSV it finds; if the source CSV was only populated through an older
> date, the DB will reflect that cutoff. Check the file modification time and row
> count of the CSV at `${ieasyhydroforecast_data_ref_dir}/intermediate_data/`
> before triggering a re-run.
> A mismatch between CSV coverage and expected historical coverage is a data
> provenance issue, not a backfill failure.

### Acceptance criteria

P0 is accepted when:

```text
1. P0 log exists.
2. Every row in the checklist is classified DONE, PARTIAL, or EMPTY.
3. Later phases are marked RUN or SKIP from this checklist.
4. The env gate output has been reviewed for P5/P6/P7.
5. Dashboard containers have NetworkMode=host on deployments using SSH-tunnelled iEH HF,
   or the manual re-patch procedure is completed and rechecked.
6. No env-file contents beyond the named gate variables, real station codes, or
   discharge values are pasted into the plan.
```

### Failure recovery

P0 is read-only. If a query fails:

```text
1. Confirm the target DB container is running.
2. Confirm the database name is preprocessing_db or postprocessing_db.
3. Rerun only the failed query block.
4. Do not proceed to write phases until P0 classification is complete.
```

---

## P0.5: Pre-backfill database backup

### Goal

Create verified `pg_dump --format=custom` dumps of the active SAPPHIRE databases
before any write phase.

### Files / scripts invoked

```text
${REPO}/bin/backup_sapphire_db.sh
${COMPOSE_ENV_FILE}
```

Verification: `bin/backup_sapphire_db.sh:16-20` documents the CLI, `:205-260`
writes and verifies each dump, and `:360-364` backs up preprocessing,
postprocessing, user, and auth DBs.

### Depends on

P0.

### Expected duration

10–30 minutes, depending on DB size and disk speed.

### Log location

```text
/var/backups/sapphire/pre_p2_<UTC>/backup_pre_p2_<UTC>.log
/var/backups/sapphire/pre_p2_<UTC>/MANIFEST.txt
```

### Server commands

Run P0.5 before P1/P2/P3/P4/P5/P7/P8/P9. P6 also depends on P0.5 if CM is enabled.

```bash
cd "$REPO"

export BACKUP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
export BACKUP_DIR="/var/backups/sapphire/pre_p2_${BACKUP_UTC}"
export BACKUP_ENV_FILE="${COMPOSE_ENV_FILE:-$ENV_FILE}"
mkdir -p "$BACKUP_DIR"

if [ ! -f "$BACKUP_ENV_FILE" ]; then
  echo "ERROR: DB/compose env file not found: $BACKUP_ENV_FILE"
  echo "Deploy the stack first or re-run setup with --compose-env-file <path>."
  exit 1
fi

db_env_value() {
  grep -E "^[[:space:]]*$1=" "$BACKUP_ENV_FILE" \
    | tail -n1 \
    | sed -E "s/^[[:space:]]*$1=//; s/^\"(.*)\"$/\1/; s/^'(.*)'$/\1/"
}

missing_db_vars=()
for var in POSTGRES_USER POSTGRES_PASSWORD PREPROCESSING_DB POSTPROCESSING_DB USER_DB AUTH_DB; do
  if [ -z "$(db_env_value "$var")" ]; then
    missing_db_vars+=("$var")
  fi
done

if [ "${#missing_db_vars[@]}" -gt 0 ]; then
  echo "ERROR: DB/compose env file is missing required backup variable(s): ${missing_db_vars[*]}"
  echo "File checked: $BACKUP_ENV_FILE"
  echo "Re-run setup with --compose-env-file <path> if DB variables live elsewhere."
  exit 1
fi

bash bin/backup_sapphire_db.sh --env-file "$BACKUP_ENV_FILE" -d "$BACKUP_DIR" -r 0 \
  2>&1 | tee "${BACKUP_DIR}/backup_pre_p2_${BACKUP_UTC}.log"

PRE_DB=$(db_env_value PREPROCESSING_DB)
POST_DB=$(db_env_value POSTPROCESSING_DB)
USER_DB_NAME=$(db_env_value USER_DB)
AUTH_DB_NAME=$(db_env_value AUTH_DB)

PRE_DUMP=$(ls -1t "${BACKUP_DIR}/${PRE_DB}_"*.dump | head -1)
POST_DUMP=$(ls -1t "${BACKUP_DIR}/${POST_DB}_"*.dump | head -1)
USER_DUMP=$(ls -1t "${BACKUP_DIR}/${USER_DB_NAME}_"*.dump | head -1)
AUTH_DUMP=$(ls -1t "${BACKUP_DIR}/${AUTH_DB_NAME}_"*.dump | head -1)

ln -sf "$(basename "$PRE_DUMP")" "${BACKUP_DIR}/preprocessing_pre_p2_${BACKUP_UTC}.dump"
ln -sf "$(basename "$POST_DUMP")" "${BACKUP_DIR}/postprocessing_pre_p2_${BACKUP_UTC}.dump"
ln -sf "$(basename "$USER_DUMP")" "${BACKUP_DIR}/user_pre_p2_${BACKUP_UTC}.dump"
ln -sf "$(basename "$AUTH_DUMP")" "${BACKUP_DIR}/auth_pre_p2_${BACKUP_UTC}.dump"

{
  echo "P0.5 backup manifest"
  echo "utc=${BACKUP_UTC}"
  echo "backup_dir=${BACKUP_DIR}"
  echo "preprocessing_dump=${PRE_DUMP}"
  echo "postprocessing_dump=${POST_DUMP}"
  echo "user_dump=${USER_DUMP}"
  echo "auth_dump=${AUTH_DUMP}"
  echo "restore_alias_preprocessing=${BACKUP_DIR}/preprocessing_pre_p2_${BACKUP_UTC}.dump"
  echo "restore_alias_postprocessing=${BACKUP_DIR}/postprocessing_pre_p2_${BACKUP_UTC}.dump"
  echo "restore_alias_user=${BACKUP_DIR}/user_pre_p2_${BACKUP_UTC}.dump"
  echo "restore_alias_auth=${BACKUP_DIR}/auth_pre_p2_${BACKUP_UTC}.dump"
} | tee "${BACKUP_DIR}/MANIFEST.txt"
```

### Acceptance criteria

```bash
grep -q "All four dumps succeeded and verified" "${BACKUP_DIR}/backup_pre_p2_${BACKUP_UTC}.log"
test -s "${BACKUP_DIR}/MANIFEST.txt"
test -s "${BACKUP_DIR}/preprocessing_pre_p2_${BACKUP_UTC}.dump"
test -s "${BACKUP_DIR}/postprocessing_pre_p2_${BACKUP_UTC}.dump"
```

Accept only when the backup log reports all four dumps succeeded and the
manifest names the exact restore aliases:

```text
preprocessing_pre_p2_<UTC>.dump
postprocessing_pre_p2_<UTC>.dump
user_pre_p2_<UTC>.dump
auth_pre_p2_<UTC>.dump
```

### Failure recovery

P0.5 is a hard gate. If any dump fails, stop the backfill plan, fix backup
storage/container health, and rerun P0.5. Do not proceed to any write phase
without a valid P0.5 manifest.

---

## P1: Gateway historical ERA5 meteo and raw snow

### Goal

Write historical ERA5 meteo and raw snow rows from `ieasyhydroforecast_START_DATE`
to today using the gateway image in `SAPPHIRE_SYNC_MODE=initial`.

### Files / scripts invoked

```text
${REPO}/bin/utils/common_functions.sh
Docker image: mabesa/sapphire-prepgateway:${ieasyhydroforecast_backend_docker_image_tag}
Image default CMD: uv run Quantile_Mapping_OP.py && uv run extend_era5_reanalysis.py && uv run snow_data_operational.py
```

### Depends on

P0.5.

### Expected duration

30–90 minutes for a full historical reanalysis write, depending on HRU count and
existing DB contents.

### Log location

```text
${ieasyhydroforecast_data_root_dir}/logs/gateway_initial/gateway_initial_<timestamp>.log
```

### Server commands

Skip P1 if P0 classifies both ERA5 meteo and raw snow as `DONE`.

```bash
cd "$REPO"
load_backfill_env

export START_DATE="${ieasyhydroforecast_START_DATE:?ieasyhydroforecast_START_DATE missing}"
export BACKEND_TAG="${ieasyhydroforecast_backend_docker_image_tag:-local}"
export LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/gateway_initial"
mkdir -p "$LOG_DIR"
export TS="$(date +%Y%m%d_%H%M%S)"
export SERVICE_LOG="${LOG_DIR}/gateway_initial_${TS}.log"
export IMAGE_ID="mabesa/sapphire-prepgateway:${BACKEND_TAG}"

docker image inspect "$IMAGE_ID" >/dev/null 2>&1 || docker pull "$IMAGE_ID"
docker rm -f prepgateway-initial 2>/dev/null || true

nohup docker run \
  --name prepgateway-initial \
  --network host \
  -e "ieasyhydroforecast_data_root_dir=${ieasyhydroforecast_data_root_dir}" \
  -e "ieasyhydroforecast_env_file_path=${ieasyhydroforecast_env_file_path}" \
  -e "SAPPHIRE_SYNC_MODE=initial" \
  -e "SAPPHIRE_OPDEV_ENV=True" \
  -e "IN_DOCKER=True" \
  -e "IN_DOCKER_CONTAINER=True" \
  -v "${ieasyhydroforecast_data_ref_dir}/config:${ieasyhydroforecast_container_data_ref_dir}/config" \
  -v "${ieasyhydroforecast_data_ref_dir}/intermediate_data:${ieasyhydroforecast_container_data_ref_dir}/intermediate_data" \
  --memory=4g \
  --memory-swap=6g \
  "$IMAGE_ID" \
  > "$SERVICE_LOG" 2>&1 &

echo "P1 PID=$!"
echo "tail -f $SERVICE_LOG"
```

Tail:

```bash
tail -f "$SERVICE_LOG"
```

Finish detection:

```bash
docker inspect prepgateway-initial --format='{{.State.Status}} {{.State.ExitCode}}'
```

Expected:

```text
exited 0
```

Clean up after success:

```bash
docker rm -f prepgateway-initial 2>/dev/null || true
```

### Acceptance criteria

```bash
docker exec -i sapphire-preprocessing-db \
  psql -U postgres -d preprocessing_db -P pager=off <<SQL
SELECT
  meteo_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT code) AS hru_count,
  MIN(date) AS min_date,
  MAX(date) AS max_date
FROM meteo
GROUP BY meteo_type
ORDER BY meteo_type;

SELECT
  snow_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT code) AS hru_count,
  MIN(date) AS min_date,
  MAX(date) AS max_date,
  COUNT(*) FILTER (WHERE value IS NOT NULL) AS value_rows
FROM snow
GROUP BY snow_type
ORDER BY snow_type;
SQL
```

Accept when:

```text
1. T and P meteo rows exist.
2. HS/ROF/SWE raw snow rows exist, if configured for your deployment.
3. min_date is at or before START_DATE for the historical products available in source files.
4. max_date reaches today or the latest source-data date expected by the gateway.
```

### Failure recovery

If the container exits non-zero:

```text
1. Read the last 100 lines of SERVICE_LOG.
2. If failure is connectivity/tunnel/auth, confirm the tunnel is up and rerun the same command.
3. If failure is an API write error, rerun P0 meteo/snow diagnostics to see whether rows landed
   before failure.
4. Because meteo and snow endpoints upsert on their natural keys, rerunning P1 is safe.
5. Do not purge meteo/snow unless the log proves corrupt values were written.
```

---

## P1.5: Historical snow value backfill (workaround)

### Goal

Populate `snow.value` for the full historical date range across all
configured snow types and stations. This phase is a workaround for two
known bugs in the standard P1 write path that together leave ~96% of
`snow.value` NULL after P1 completes. P3 (`recalculate_snow_norms.py`)
requires populated historical `snow.value` to compute meaningful
climatologies, so this phase must run between P1 and P3.

### Files / scripts invoked

```text
${REPO}/bin/initialize_snow_history.sh
${REPO}/bin/utils/common_functions.sh
Docker image: mabesa/sapphire-prepgateway:${ieasyhydroforecast_backend_docker_image_tag}
```

The script writes a small Python helper to a temp path and runs it
inside the prepgateway image (which already has python3 + urllib in
stdlib). The helper reads each snow CSV under
`${ieasyhydroforecast_data_ref_dir}/intermediate_data/snow_data/`, filters
rows where the value column is a parseable real number, and POSTs
minimal payloads `{snow_type, code, date, value}` to the preprocessing
API's `/snow/` endpoint in batches.

### Depends on

P0.5, P1.

### Expected duration

5-15 minutes for a typical 26-year × 17-station deployment with two HRU
models. Scales linearly with the number of (snow_type, HRU) CSV files
under `intermediate_data/snow_data/`.

### Log location

```text
${ieasyhydroforecast_data_root_dir}/logs/snow_history_init/
${ieasyhydroforecast_data_root_dir}/logs/snow_history_init/snow_history_init_<timestamp>.log
```

### Server commands

Skip P1.5 if P0 classifies snow `value_rows` as `DONE` (i.e., already
populated for the full historical range). Run after P1 completes.

```bash
cd "$REPO"

# 1. Dry run — discover CSVs, count records with values, do NOT POST.
bash bin/initialize_snow_history.sh "$ENV_FILE" --dry-run

# 2. Real run — POSTs records in batches of 500.
bash bin/initialize_snow_history.sh "$ENV_FILE"
```

Monitoring (the script tees output to the log path above):

```bash
LATEST_LOG="$(ls -t "${ieasyhydroforecast_data_root_dir}/logs/snow_history_init/"snow_history_init_*.log | head -1)"
tail -f "$LATEST_LOG"
```

### Why this workaround exists

Two known bugs combine to drop ~96% of `snow.value` in the standard P1
flow:

1. `apps/preprocessing_gateway/dg_utils.py::write_snow_to_api` sends
   `value=None` for most records despite the source CSV having real
   values. Empirically this leaves only the ~365-day Data Gateway
   operational window populated. Root cause not fully characterized as
   of this writing; mechanism appears tied to how the function
   constructs records when two HRU CSVs share the same `(snow_type,
   code, date)` keys.
2. `sapphire/services/preprocessing/app/crud.py::create_snow`'s
   `_has_changes + setattr` loop overwrites existing non-NULL `value`
   with incoming NULL when any field differs. So when the first HRU's
   records write valid values and the second HRU's records (for the
   same key) carry NULL, the second pass destroys the first pass's
   values.

This phase bypasses both bugs by reading CSVs directly and posting only
rows with real numeric values. It never sends NULL, so the
`_has_changes` overwrite path is not triggered.

### Acceptance criteria

```bash
docker exec -i sapphire-preprocessing-db \
  psql -U postgres -d preprocessing_db -P pager=off <<SQL
SELECT
  snow_type,
  COUNT(*) AS rows,
  COUNT(value) AS value_rows,
  ROUND(100.0 * COUNT(value) / NULLIF(COUNT(*), 0), 1) AS pct_populated,
  MIN(date) FILTER (WHERE value IS NOT NULL) AS first_value_date,
  MAX(date) FILTER (WHERE value IS NOT NULL) AS last_value_date
FROM snow
GROUP BY snow_type
ORDER BY snow_type;
SQL
```

Accept when:

```text
1. Each configured snow type has value_rows close to total rows
   (typically >95% populated).
2. first_value_date is at or before START_DATE.
3. last_value_date reaches today or the latest source-data date.
4. No HTTP errors in the snow_history_init log.
```

### Failure recovery

The script is idempotent: each POST upserts on `(snow_type, code, date)`
and the script only sends records with real values, so re-running is
safe — completed rows are no-ops.

If the script exits non-zero or batches show `FAILED`:

```text
1. Read the most recent log under
   ${ieasyhydroforecast_data_root_dir}/logs/snow_history_init/.
2. Confirm api-gateway readiness:
   curl -sf http://localhost:8000/health/ready
3. Confirm the snow CSVs exist with non-zero size at
   ${ieasyhydroforecast_data_ref_dir}/intermediate_data/snow_data/.
4. Re-run with --dry-run to confirm record counts before retrying the
   real run.
```

### Removal criteria

This phase can be removed from the runbook once the underlying bugs in
`dg_utils.write_snow_to_api` and the service-side `_has_changes` upsert
logic are fixed and verified end-to-end on at least one deployment.

---

## P2: Daily runoff and day hydrograph historical backfill

### Goal

Write historical runoff observations and day hydrograph rows from `START_DATE`
to today, closing any hydrograph gap in the dashboard.

### Files / scripts invoked

```text
${REPO}/bin/initialize_site_backfill.sh
${REPO}/bin/utils/common_functions.sh
Docker image: mabesa/sapphire-preprunoff:${ieasyhydroforecast_backend_docker_image_tag}
```

### Depends on

P0.5.

### Expected duration

30–90 minutes depending on station count and iEH HF response time.

### Log location

The wrapper writes to:

```text
${ieasyhydroforecast_data_root_dir}/logs/site_backfill/
```

The outer nohup log is:

```text
${ieasyhydroforecast_data_root_dir}/logs/site_backfill/p2_preprunoff_outer_<timestamp>.log
```

### Server commands

Skip P2 if P0 classifies daily runoff and day hydrograph as `DONE`.

```bash
cd "$REPO"
load_backfill_env

export START_DATE="${ieasyhydroforecast_START_DATE:?ieasyhydroforecast_START_DATE missing}"
export LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/site_backfill"
mkdir -p "$LOG_DIR"
export OUTER_LOG="${LOG_DIR}/p2_preprunoff_outer_$(date +%Y%m%d_%H%M%S).log"

nohup bash bin/initialize_site_backfill.sh "$ENV_FILE" \
  --start-date "$START_DATE" \
  --site-code "$SAMPLE_CODE" \
  --skip-linreg \
  --skip-skill \
  > "$OUTER_LOG" 2>&1 &

echo "P2 PID=$!"
echo "tail -f $OUTER_LOG"
```

Tail:

```bash
tail -f "$OUTER_LOG"
ls -lt "${ieasyhydroforecast_data_root_dir}/logs/site_backfill" | head
```

### Acceptance criteria

```bash
docker exec -i sapphire-preprocessing-db \
  psql -U postgres -d preprocessing_db -P pager=off <<SQL
SELECT
  horizon_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT code) AS sites,
  MIN(date) AS min_date,
  MAX(date) AS max_date
FROM runoffs
WHERE horizon_type = 'day'
GROUP BY horizon_type;

SELECT
  horizon_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT code) AS sites,
  MIN(date) AS min_date,
  MAX(date) AS max_date,
  COUNT(*) FILTER (WHERE mean IS NOT NULL) AS mean_rows,
  COUNT(*) FILTER (WHERE q05 IS NOT NULL) AS q05_rows,
  COUNT(*) FILTER (WHERE q95 IS NOT NULL) AS q95_rows
FROM hydrographs
WHERE horizon_type = 'day'
GROUP BY horizon_type;
SQL
```

Accept when:

```text
1. runoffs/day min_date <= START_DATE.
2. runoffs/day max_date is near today.
3. hydrographs/day min_date <= START_DATE.
4. hydrographs/day max_date is near today.
5. hydrograph stat fields are populated.
```

### Failure recovery

The wrapper uses API upsert paths for runoff and hydrograph writes. If it fails partway:

```text
1. Read the per-container service log in ${ieasyhydroforecast_data_root_dir}/logs/site_backfill.
2. Rerun the same P2 command if the error was transient.
3. If the run wrote wrong site/date scope, use purge_site_data.sh only for the affected
   site/date range.
4. Do not run a broad purge unless the operator is ready to restore from the dump produced
   in P0.5: preprocessing_pre_p2_<UTC>.dump and/or postprocessing_pre_p2_<UTC>.dump.
```

Dry-run purge example for one site:

```bash
cd "$REPO"
bash bin/purge_site_data.sh "$SAMPLE_CODE" "$START_DATE" --include-hydrographs --dry-run
```

---

## P3: Historical snow stats and snow norms

### Goal

Populate snow `norm`, `mean/min/max/std/q05..q95/previous/current` fields for
historical snow rows.

### Files / scripts invoked

```text
${REPO}/bin/backfill_snow_stats_history.sh
${REPO}/apps/preprocessing_gateway/recalculate_snow_norms.py
Docker image: mabesa/sapphire-prepgateway:${ieasyhydroforecast_backend_docker_image_tag}
```

### Depends on

P0.5, P1, P1.5.

### Expected duration

Multi-hour. A 2010–2025 run took about 90 minutes on a local Mac; production
runtime may be longer. Run unattended in `nohup` or `tmux`.

### Log location

```text
${ieasyhydroforecast_data_root_dir}/logs/snow_stat_backfill/
${ieasyhydroforecast_data_root_dir}/logs/snow_stat_backfill/backfill_<year>.log
${ieasyhydroforecast_data_root_dir}/logs/snow_stat_backfill/backfill_progress.txt
```

### Server commands

Skip P3 if P0 classifies snow stats/norms as `DONE`.

```bash
cd "$REPO"
load_backfill_env

export START_DATE="${ieasyhydroforecast_START_DATE:?ieasyhydroforecast_START_DATE missing}"
export START_YEAR="${START_DATE%%-*}"
export LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/snow_stat_backfill"
mkdir -p "$LOG_DIR"
export OUTER_LOG="${LOG_DIR}/p3_snow_stats_outer_$(date +%Y%m%d_%H%M%S).log"

nohup env ieasyhydroforecast_env_file_path="$ENV_FILE" \
  bash bin/backfill_snow_stats_history.sh --start-year "$START_YEAR" \
  > "$OUTER_LOG" 2>&1 &

echo "P3 PID=$!"
echo "tail -f $OUTER_LOG"
```

Tail:

```bash
tail -f "$OUTER_LOG"
tail -f "${LOG_DIR}/backfill_$(date +%Y).log" 2>/dev/null || true
```

Progress:

```bash
cat "${LOG_DIR}/backfill_progress.txt"
```

Progress-file completion check:

```bash
EXPECTED_YEARS=$(( $(date +%Y) - START_YEAR ))
COMPLETED_YEARS=$(sort -u "${LOG_DIR}/backfill_progress.txt" | wc -l | tr -d ' ')
echo "expected=${EXPECTED_YEARS} completed=${COMPLETED_YEARS}"
test "$COMPLETED_YEARS" -eq "$EXPECTED_YEARS"
```

The wrapper also emits `all years processed` at completion, but acceptance
uses `backfill_progress.txt` because that is the script's resume ledger.

Verification: `bin/backfill_snow_stats_history.sh:108-111` defines the progress
file, `:176-234` loops through years and records completed years, and `:236`
emits `all years processed`. `bin/backfill_snow_stats_history.sh:23` uses
`set -euo pipefail`, while `bin/utils/common_functions.sh:280,291` references
an unset `ieasyhydroforecast_ssh_tunnel_pid` under `set -u`; this cleanup quirk
affects only P3 — P2/P4/P8 use `set -eo pipefail` and are unaffected.

### Acceptance criteria

```bash
docker exec -i sapphire-preprocessing-db \
  psql -U postgres -d preprocessing_db -P pager=off <<SQL
SELECT
  snow_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT code) AS hru_count,
  MIN(date) AS min_date,
  MAX(date) AS max_date,
  COUNT(*) FILTER (WHERE norm IS NOT NULL) AS norm_rows,
  COUNT(*) FILTER (WHERE mean IS NOT NULL) AS mean_rows,
  COUNT(*) FILTER (WHERE min IS NOT NULL) AS min_rows,
  COUNT(*) FILTER (WHERE max IS NOT NULL) AS max_rows,
  COUNT(*) FILTER (WHERE q05 IS NOT NULL) AS q05_rows,
  COUNT(*) FILTER (WHERE q95 IS NOT NULL) AS q95_rows,
  COUNT(*) FILTER (WHERE previous IS NOT NULL) AS previous_rows,
  COUNT(*) FILTER (WHERE current IS NOT NULL) AS current_rows
FROM snow
GROUP BY snow_type
ORDER BY snow_type;
SQL
```

Accept when:

```text
1. Each configured snow type has non-null norm rows.
2. The stat columns have non-null historical rows.
3. backfill_progress.txt unique line count equals $(date +%Y) - START_YEAR.
4. If the wrapper exits 1 after the progress-file count passes, classify the work as DONE
   and record the cleanup quirk.
```

### Failure recovery

The snow API upserts by `(snow_type, code, date)`, so successful years are safe to keep.

If a specific year fails:

```text
1. Read ${LOG_DIR}/backfill_<year>.log.
2. Fix the transient cause if obvious.
3. Rerun the same P3 command; completed years are skipped via backfill_progress.txt.
4. If the wrapper exits 1 after all years, check for the known cleanup quirk before rerunning.
```

---

## P4: LR PENTAD and DECAD historical hindcasts

### Goal

Populate LR hindcasts for PENTAD and DECAD across the historical horizon and
ensure pentad/decad hydrographs are written.

### Files / scripts invoked

```text
${REPO}/bin/initialize_site_backfill.sh
Docker image: mabesa/sapphire-linreg:${ieasyhydroforecast_backend_docker_image_tag}
```

### Depends on

P0.5, P1, P2.

### Expected duration

1–3 hours depending on station count and historical horizon.

### Log location

```text
${ieasyhydroforecast_data_root_dir}/logs/site_backfill/
```

### Server commands

Skip P4 if P0 classifies LR PENTAD and DECAD hindcasts and pentad/decad
hydrographs as `DONE`.

```bash
cd "$REPO"
load_backfill_env

export START_DATE="${ieasyhydroforecast_START_DATE:?ieasyhydroforecast_START_DATE missing}"
export LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/site_backfill"
mkdir -p "$LOG_DIR"
export OUTER_LOG="${LOG_DIR}/p4_lr_outer_$(date +%Y%m%d_%H%M%S).log"

nohup bash bin/initialize_site_backfill.sh "$ENV_FILE" \
  --start-date "$START_DATE" \
  --site-code "$SAMPLE_CODE" \
  --skip-preprunoff \
  --skip-skill \
  > "$OUTER_LOG" 2>&1 &

echo "P4 PID=$!"
echo "tail -f $OUTER_LOG"
```

### Acceptance criteria

```bash
docker exec -i sapphire-postprocessing-db \
  psql -U postgres -d postprocessing_db -P pager=off <<SQL
SELECT
  horizon_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT code) AS sites,
  MIN(date) AS min_issue_date,
  MAX(date) AS max_issue_date
FROM lr_forecasts
GROUP BY horizon_type
ORDER BY horizon_type;
SQL

docker exec -i sapphire-preprocessing-db \
  psql -U postgres -d preprocessing_db -P pager=off <<SQL
SELECT
  horizon_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT code) AS sites,
  MIN(date) AS min_date,
  MAX(date) AS max_date,
  COUNT(*) FILTER (WHERE mean IS NOT NULL) AS mean_rows
FROM hydrographs
WHERE horizon_type IN ('pentad', 'decade')
GROUP BY horizon_type
ORDER BY horizon_type;
SQL
```

Accept when:

```text
1. lr_forecasts has both pentad and decade rows.
2. min_issue_date covers the historical start after the expected LR training lookback.
3. max_issue_date reaches today or the last completed pentad/decade boundary.
4. hydrographs/pentad and hydrographs/decade rows exist and have stats populated.
```

### Failure recovery

LR forecasts upsert by `(horizon_type, code, date)`. Rerun P4 for transient failures.

Do not change the LR issue-date convention:

```text
forecast_horizon_int is the issue pentad/decade, not the target period.
```

If a bad partial run must be removed for a site:

```bash
cd "$REPO"
bash bin/purge_site_data.sh "$SAMPLE_CODE" "$START_DATE" --postprocessing-only --dry-run
```

Only remove rows after identifying the exact affected site/date range and confirming
the P0.5 restore dumps exist: `preprocessing_pre_p2_<UTC>.dump` and
`postprocessing_pre_p2_<UTC>.dump`.

---

## P5: ML PENTAD and DECAD historical hindcasts

### Goal

Populate historical ML hindcasts for the configured ML models (normally TFT,
TIDE, and TSMIXER) for PENTAD and DECAD.

### Files / scripts invoked

```text
${REPO}/apps/machine_learning/hindcast_ML_models.py
Docker image: mabesa/sapphire-ml:${ieasyhydroforecast_backend_docker_image_tag}
```

### Depends on

P0.5, P1, P2.

### Expected duration

Several hours. Each container has a 12g memory limit and may run long for full
history.

### Log location

```text
${ieasyhydroforecast_data_root_dir}/logs/ml_hindcast_backfill/
```

### Server commands

Skip P5 if P0 classifies configured ML PENTAD and DECAD hindcasts as `DONE`.

P5 normalizes `ieasyhydroforecast_available_ML_models` to uppercase before
launching containers. This keeps your env as the source of truth while satisfying
the ML hindcast script's strict accepted values.

Verification: `apps/machine_learning/hindcast_ML_models.py:136-140` requires
`SAPPHIRE_MODEL_TO_USE` to be one of `TFT`, `TIDE`, `TSMIXER`, or `ARIMA`;
mixed-case env values such as `TiDE` or `TSMixer` must not be passed through
unchanged.

```bash
cd "$REPO"
load_backfill_env

export START_DATE="${ieasyhydroforecast_START_DATE:?ieasyhydroforecast_START_DATE missing}"
export END_DATE="$(date -u +%F)"
export BACKEND_TAG="${ieasyhydroforecast_backend_docker_image_tag:-local}"
export IMAGE_ID="mabesa/sapphire-ml:${BACKEND_TAG}"
export LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/ml_hindcast_backfill"
mkdir -p "$LOG_DIR"

docker image inspect "$IMAGE_ID" >/dev/null 2>&1 || docker pull "$IMAGE_ID"

RAW_ML_MODELS="${ieasyhydroforecast_available_ML_models:-}"
if [ -z "$RAW_ML_MODELS" ]; then
  echo "ERROR: ieasyhydroforecast_available_ML_models is empty; skip P5 or set configured models explicitly."
  exit 1
fi

ML_MODELS="$(
  printf "%s" "$RAW_ML_MODELS" \
    | tr ',' '\n' \
    | sed 's/^ *//;s/ *$//' \
    | tr '[:lower:]' '[:upper:]' \
    | while read -r MODEL; do
        case "$MODEL" in
          TFT|TIDE|TSMIXER|ARIMA) echo "$MODEL" ;;
          "") ;;
          *) echo "UNSUPPORTED_ML_MODEL_${MODEL}" ;;
        esac
      done \
    | sort -u \
    | tr '\n' ' '
)"

if printf "%s\n" "$ML_MODELS" | grep -q 'UNSUPPORTED_ML_MODEL_'; then
  echo "ERROR: Unsupported ML model after normalization: $ML_MODELS"
  exit 1
fi

export MAX_PARALLEL=3
export RUNNING_JOBS=0

for MODEL in $ML_MODELS; do
  for MODE in PENTAD DECAD; do
    CONTAINER="ml-hindcast-${MODEL}-${MODE}"
    SERVICE_LOG="${LOG_DIR}/${CONTAINER}_$(date +%Y%m%d_%H%M%S).log"
    docker rm -f "$CONTAINER" 2>/dev/null || true
    nohup docker run \
      --name "$CONTAINER" \
      --network host \
      -e "PYTHONPATH=/app" \
      -e "ieasyhydroforecast_data_root_dir=${ieasyhydroforecast_data_root_dir}" \
      -e "ieasyhydroforecast_env_file_path=${ieasyhydroforecast_env_file_path}" \
      -e "ieasyhydroforecast_START_DATE=${START_DATE}" \
      -e "ieasyhydroforecast_END_DATE=${END_DATE}" \
      -e "SAPPHIRE_OPDEV_ENV=True" \
      -e "IN_DOCKER=True" \
      -e "IN_DOCKER_CONTAINER=True" \
      -e "SAPPHIRE_MODEL_TO_USE=${MODEL}" \
      -e "SAPPHIRE_HINDCAST_MODE=${MODE}" \
      -v "${ieasyhydroforecast_data_ref_dir}/config:${ieasyhydroforecast_container_data_ref_dir}/config" \
      -v "${ieasyhydroforecast_data_ref_dir}/daily_runoff:${ieasyhydroforecast_container_data_ref_dir}/daily_runoff" \
      -v "${ieasyhydroforecast_data_ref_dir}/intermediate_data:${ieasyhydroforecast_container_data_ref_dir}/intermediate_data" \
      -v "${ieasyhydroforecast_data_ref_dir}/bin:${ieasyhydroforecast_container_data_ref_dir}/bin" \
      --memory=12g \
      --memory-swap=16g \
      "$IMAGE_ID" \
      uv run hindcast_ML_models.py \
      > "$SERVICE_LOG" 2>&1 &
    echo "started $CONTAINER pid=$! log=$SERVICE_LOG"
    RUNNING_JOBS=$((RUNNING_JOBS + 1))
    if [ "$RUNNING_JOBS" -ge "$MAX_PARALLEL" ]; then
      wait -n || true
      RUNNING_JOBS=$((RUNNING_JOBS - 1))
    fi
    sleep 5
  done
done

wait || true
```

Monitor:

```bash
docker ps --filter "name=ml-hindcast" --format 'table {{.Names}}\t{{.Status}}'
ls -lt "$LOG_DIR" | head
```

Capture finish states:

```bash
for c in $(docker ps -a --filter "name=ml-hindcast" --format '{{.Names}}'); do
  docker inspect "$c" --format='{{.Name}} {{.State.Status}} {{.State.ExitCode}}'
done
```

Clean up only after logs and acceptance checks:

```bash
for c in $(docker ps -a --filter "name=ml-hindcast" --format '{{.Names}}'); do
  docker rm -f "$c" 2>/dev/null || true
done
```

### Acceptance criteria

```bash
docker exec -i sapphire-postprocessing-db \
  psql -U postgres -d postprocessing_db -P pager=off <<SQL
SELECT
  model_type,
  horizon_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT code) AS sites,
  MIN(date) AS min_issue_date,
  MAX(date) AS max_issue_date,
  MIN(target) AS min_target,
  MAX(target) AS max_target
FROM forecasts
WHERE model_type IN ('TFT', 'TIDE', 'TSMIXER')
GROUP BY model_type, horizon_type
ORDER BY model_type, horizon_type;
SQL
```

Accept when:

```text
1. Every configured ML model has pentad and/or decade archive rows.
2. The issue-date range covers the intended historical horizon after model lookback constraints.
3. API write succeeded in each service log.
4. If DAY rows also exist, the period archive still exists for the older history.
```

### Failure recovery

ML forecast rows upsert by `(horizon_type, code, model_type, date, target)`.

If a container exits non-zero:

```text
1. Read the model/horizon log.
2. If the failure is missing model/scaler files, mark that model/horizon BLOCKED and coordinate
   before rerunning.
3. If the failure is transient API or memory pressure, rerun only that model/horizon container.
4. Do not delete successful model/horizon rows.
```

Known risk:

```text
Skill recalculation must read both DAY and PENTAD/DECADE archives for ML.
origin/maxat_sapphire_2 already contains this reader behavior, but P8 must verify n_pairs is
not starved to 1-2.
```

---

## P6: Conceptual-model hindcasts (conditional)

> **Default: SKIP.** Conceptual-model forecasting is being deprecated. If
> `ieasyhydroforecast_run_CM_models=False` in your env file (default for new
> deployments), skip this phase entirely. The procedure below applies only to
> legacy deployments where CM is still enabled — coordinate with the CM owner
> before running.

### Goal

Run CM hindcasts only if CM is enabled, configured, and the existing R Docker
path is verified to read your deployment's env/config safely.

### Files / scripts invoked

```text
${REPO}/apps/conceptual_model/run_operation_forecasting_CM.R
${REPO}/apps/conceptual_model/run_initial.R
${REPO}/apps/conceptual_model/run_manual_hindcast.R
Docker image: mabesa/sapphire-conceptmod:${ieasyhydroforecast_backend_docker_image_tag}
```

The Docker image default CMD is the operational script, not a hindcast script.
`run_initial.R` and `run_manual_hindcast.R` are relevant only to the blocked
coordination gate.

> **Warning: CM scripts have historically had org-specific paths hardcoded in
> the Docker branch.** Before running P6 on any deployment, verify with the CM
> owner that the R scripts can read your deployment's env path correctly, not a
> path from a different deployment. The CM owner must provide a confirmed-safe
> command before the operator runs anything.

Verification: `apps/conceptual_model/Dockerfile:29-30` sets
`CMD ["Rscript", "apps/conceptual_model/run_operation_forecasting_CM.R"]`; the
hardcoded env path risk is present in `apps/conceptual_model/run_initial.R:31,48`
and `apps/conceptual_model/run_manual_hindcast.R:31,48`.

### Depends on

P0.5, P1, P2.

### Expected duration

Unknown. Treat as a separate operator window if enabled.

### Log location

```text
${ieasyhydroforecast_data_root_dir}/logs/cm_hindcast_backfill/
```

### Server commands

First decide whether CM is enabled:

```bash
cd "$REPO"
load_backfill_env

if [ "${ieasyhydroforecast_run_CM_models:-}" = "True" ] || [ "${ieasyhydroforecast_run_CM_models:-}" = "true" ]; then
  echo "CM enabled: continue with CM coordination gate"
else
  echo "CM disabled: skip P6"
fi
```

If CM is disabled, P6 acceptance is `SKIPPED_BY_CONFIG`.

If CM is enabled, stop and coordinate before running. The deployed R scripts
inspect `IN_DOCKER_CONTAINER` and in the Docker branch may expect a hardcoded
env filename from a specific deployment. For a new deployment this is not safe
to run blindly.

CM coordination gate:

```text
1. Confirm the env actually sets ieasyhydroforecast_run_CM_models=true.
2. Confirm the conceptual-model config JSON exists for your deployment.
3. Confirm the R script can read your deployment's env path (coordinate with the CM owner).
4. Confirm whether CM output is expected in postprocessing_db.forecasts as model_type='RRAM'
   or only CSV.
5. If any item is false, mark P6 BLOCKED and do not run CM in this backfill.
```

Only after that gate, a CM owner may provide a runnable command. Do not modify
service code or schemas.

### Acceptance criteria

If skipped:

```text
P6=SKIPPED_BY_CONFIG when ieasyhydroforecast_run_CM_models is false/unset.
```

If run:

```bash
docker exec -i sapphire-postprocessing-db \
  psql -U postgres -d postprocessing_db -P pager=off <<SQL
SELECT
  model_type,
  horizon_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT code) AS sites,
  MIN(date) AS min_issue_date,
  MAX(date) AS max_issue_date
FROM forecasts
WHERE model_type = 'RRAM'
GROUP BY model_type, horizon_type
ORDER BY horizon_type;
SQL
```

Accept when RRAM rows exist for the configured CM horizons, or when the CM owner
confirms CM is out of scope for your deployment.

### Failure recovery

Do not use `purge_site_data.sh` for CM unless CM rows were written to the API
and the CM owner identifies a bad site/date scope.

If the phase is blocked, continue with P7/P8 and record that CM skill metrics
are excluded because CM is not runnable/configured.

---

## P7: Long-term hindcasts for at least five historical years

### Goal

Populate long-term hindcasts for configured month and seasonal modes for at
least five historical years.

### Files / scripts invoked

```text
${REPO}/apps/long_term_forecasting/dev_code/simulate_forecasts.py
Docker image: mabesa/sapphire-lt-forecasting:${ieasyhydroforecast_backend_docker_image_tag}
```

### Depends on

P0.5, P1, P2.

### Expected duration

At least 3 hours for postprocessing-scale work; total runtime depends on
configured modes and years. Run unattended in `tmux` or `nohup`.

### Log location

```text
${ieasyhydroforecast_data_root_dir}/logs/lt_hindcast_backfill/
```

### Server commands

Skip P7 if P0 classifies long-term hindcasts as `DONE`.

This command runs configured `month_1` through `month_9` and `seasonal_*` modes
for the five complete historical years before the current year.

```bash
cd "$REPO"
load_backfill_env

export BACKEND_TAG="${ieasyhydroforecast_backend_docker_image_tag:-local}"
export IMAGE_ID="mabesa/sapphire-lt-forecasting:${BACKEND_TAG}"
export LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/lt_hindcast_backfill"
mkdir -p "$LOG_DIR"

export CURRENT_YEAR="$(date -u +%Y)"
export FIRST_YEAR="$((CURRENT_YEAR - 5))"
export LAST_YEAR="$((CURRENT_YEAR - 1))"
export YEARS="$(seq -s ' ' "$FIRST_YEAR" "$LAST_YEAR")"

# IMPORTANT — P7 differs from the other phases on networking. LT hindcast READS
# preprocessing-db DIRECTLY by its compose DNS name (data_interface.py: with
# IN_DOCKER=True, host="preprocessing-db"). That name resolves ONLY on the compose
# network, so P7 must NOT use `--network host` (host net gives
# `could not translate host name "preprocessing-db" ... Temporary failure in name
# resolution`). Writes go to SAPPHIRE_API_URL (default http://localhost:8000), which
# on the compose net must point at the gateway service by name.
export NET="sapphire_sapphire-network"              # verify: docker network ls | grep -i sapphire
export SAPPHIRE_API_URL="http://api-gateway:8000"   # gateway via compose DNS

docker image inspect "$IMAGE_ID" >/dev/null 2>&1 || docker pull "$IMAGE_ID"

printf "%s\n" "${ieasyhydroforecast_ml_long_term_supported_modes:-}" \
  | tr ',' '\n' \
  | sed 's/^ *//;s/ *$//' \
  | while read -r MODE; do
      case "$MODE" in
        month_[1-9]|seasonal_*)
          CONTAINER="lt-hindcast-${MODE}"
          SERVICE_LOG="${LOG_DIR}/${CONTAINER}_$(date +%Y%m%d_%H%M%S).log"
          docker rm -f "$CONTAINER" 2>/dev/null || true
          echo "starting $MODE years=$YEARS log=$SERVICE_LOG"
          docker run \
            --name "$CONTAINER" \
            --network "$NET" \
            -e "SAPPHIRE_API_URL=${SAPPHIRE_API_URL}" \
            -e "PYTHONPATH=/app" \
            -e "ieasyhydroforecast_data_root_dir=${ieasyhydroforecast_data_root_dir}" \
            -e "ieasyhydroforecast_env_file_path=${ieasyhydroforecast_env_file_path}" \
            -e "SAPPHIRE_OPDEV_ENV=True" \
            -e "IN_DOCKER=True" \
            -e "IN_DOCKER_CONTAINER=True" \
            -e "lt_forecast_mode=${MODE}" \
            -v "${ieasyhydroforecast_data_ref_dir}/config:${ieasyhydroforecast_container_data_ref_dir}/config" \
            -v "${ieasyhydroforecast_data_ref_dir}/intermediate_data:${ieasyhydroforecast_container_data_ref_dir}/intermediate_data" \
            --memory=12g \
            --memory-swap=16g \
            "$IMAGE_ID" \
            uv run dev_code/simulate_forecasts.py --years $YEARS --num_months 12 --all \
            2>&1 | tee "$SERVICE_LOG"
          RC=${PIPESTATUS[0]}
          docker rm -f "$CONTAINER" 2>/dev/null || true
          if [ "$RC" -ne 0 ]; then
            echo "FAILED mode=$MODE rc=$RC log=$SERVICE_LOG"
            exit "$RC"
          fi
          ;;
        ""|monthly|month_0|quarter|quarterly)
          echo "skipping non-target or calibration mode: ${MODE:-<empty>}"
          ;;
        *)
          echo "skipping unsupported LT mode for this backfill: $MODE"
          ;;
      esac
    done
```

Important:

```text
simulate_forecasts.py on the deployed branch can exit 0 despite partial per-model failures.
Therefore P7 is not accepted by process exit alone. The DB acceptance query is mandatory.
```

### Acceptance criteria

```bash
docker exec -i sapphire-postprocessing-db \
  psql -U postgres -d postprocessing_db -P pager=off <<SQL
SELECT
  horizon_type,
  horizon_value,
  model_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT code) AS sites,
  MIN(date) AS min_issue_date,
  MAX(date) AS max_issue_date,
  MIN(valid_from) AS min_valid_from,
  MAX(valid_to) AS max_valid_to
FROM long_forecasts
GROUP BY horizon_type, horizon_value, model_type
ORDER BY horizon_type, horizon_value, model_type;

SELECT
  EXTRACT(YEAR FROM date)::int AS issue_year,
  horizon_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT model_type) AS models,
  COUNT(DISTINCT code) AS sites
FROM long_forecasts
GROUP BY issue_year, horizon_type
ORDER BY issue_year, horizon_type;
SQL
```

Accept when:

```text
1. At least five complete historical issue years have long_forecasts rows.
2. Month horizons are populated for configured month_1..month_9 modes, or missing modes are
   documented as not configured.
3. Seasonal horizons are populated for configured seasonal modes.
4. Model count matches the configured LT model set for each mode, or missing models are
   explained in logs.
```

### Failure recovery

Long forecasts upsert by `(horizon_type, horizon_value, code, date, model_type, valid_from, valid_to)`.

If a mode fails:

```text
1. Read that mode's service log.
2. Rerun only the failed mode with the same years.
3. If failures are missing configured model folders, mark the affected mode/model BLOCKED and
   coordinate with the long-term model owner.
4. Do not purge successful long_forecasts rows.
```

---

## P8: Skill metrics recalculation for all populated horizons

### Goal

Recalculate skill metrics for LR, ML, CM-if-present, and long-term models
after all forecast backfills have landed.

### Files / scripts invoked

```text
${REPO}/bin/initialize_site_backfill.sh
${REPO}/bin/bimonthly_long_term_skill_metrics_recalculation.sh
${REPO}/bin/utils/run_skill_metrics_recalc.sh
Docker image: mabesa/sapphire-postprocessing:${ieasyhydroforecast_backend_docker_image_tag}
```

### Depends on

P0.5, P4, P5, P6, P7.

P6 may be `SKIPPED_BY_CONFIG` or `BLOCKED`. P8 can proceed for non-CM models
if CM is skipped/blocked.

### Expected duration

Short-term skill metrics: 1–3 hours. Long-term skill metrics: 60–120+ minutes.

### Log location

```text
${ieasyhydroforecast_data_root_dir}/logs/site_backfill/
${ieasyhydroforecast_data_root_dir}/logs/skill_metrics_recalc_longterm/
```

### Server commands

Run short-term skill recalc:

```bash
cd "$REPO"
load_backfill_env

export START_DATE="${ieasyhydroforecast_START_DATE:?ieasyhydroforecast_START_DATE missing}"
export LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/site_backfill"
mkdir -p "$LOG_DIR"
export OUTER_LOG="${LOG_DIR}/p8_short_skill_outer_$(date +%Y%m%d_%H%M%S).log"

nohup bash bin/initialize_site_backfill.sh "$ENV_FILE" \
  --start-date "$START_DATE" \
  --site-code "$SAMPLE_CODE" \
  --skip-preprunoff \
  --skip-linreg \
  > "$OUTER_LOG" 2>&1 &

echo "P8 short-term PID=$!"
echo "tail -f $OUTER_LOG"
```

After short-term completes, run long-term skill recalc:

```bash
cd "$REPO"
load_backfill_env

export LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/skill_metrics_recalc_longterm"
mkdir -p "$LOG_DIR"
export OUTER_LOG="${LOG_DIR}/p8_long_skill_outer_$(date +%Y%m%d_%H%M%S).log"

nohup bash bin/bimonthly_long_term_skill_metrics_recalculation.sh "$ENV_FILE" \
  > "$OUTER_LOG" 2>&1 &

echo "P8 long-term PID=$!"
echo "tail -f $OUTER_LOG"
```

### Acceptance criteria

```bash
docker exec -i sapphire-postprocessing-db \
  psql -U postgres -d postprocessing_db -P pager=off <<SQL
SELECT
  horizon_type,
  model_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT code) AS sites,
  MIN(date) AS min_date,
  MAX(date) AS max_date,
  MIN(n_pairs) AS min_n_pairs,
  MAX(n_pairs) AS max_n_pairs
FROM skill_metrics
GROUP BY horizon_type, model_type
ORDER BY horizon_type, model_type;
SQL
```

ML starvation check:

```bash
docker exec -i sapphire-postprocessing-db \
  psql -U postgres -d postprocessing_db -P pager=off <<SQL
SELECT
  horizon_type,
  model_type,
  MIN(n_pairs) AS min_n_pairs,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY n_pairs) AS median_n_pairs,
  MAX(n_pairs) AS max_n_pairs,
  COUNT(*) AS metric_rows
FROM skill_metrics
WHERE model_type IN ('TFT', 'TIDE', 'TSMIXER', 'NE')
GROUP BY horizon_type, model_type
ORDER BY horizon_type, model_type;
SQL
```

Accept when:

```text
1. Skill metrics exist for every populated model/horizon combination.
2. LR has pentad and decade metrics.
3. Configured ML models have pentad and decade metrics.
4. Long-term configured models have month and/or season metrics.
5. ML n_pairs are not starved to 1-2 across historical pentad/decade metrics.
6. RRAM metrics exist only if CM was run and RRAM forecasts landed.
```

### Failure recovery

Skill metrics upsert by `(horizon_type, code, model_type, date, horizon_in_year)`.

If one mode fails:

```text
1. Read the specific postprocessing service log.
2. Rerun the failed mode only if possible through run_skill_metrics_recalc_once.
3. If all short-term modes need rerun, rerun the short-term wrapper.
4. If one long-term mode failed, rerun bimonthly_long_term_skill_metrics_recalculation.sh;
   successful modes are safe to upsert.
```

---

## P9: Monthly and seasonal runoff hydrograph aggregation

### Goal

Populate monthly and April–September seasonal runoff hydrograph rows, including
norm/previous/current triads.

> **Note — deployments carrying PR #406 (discharge-aggregation parity):** on a
> server that has the parity change, the pentad/decad/month/quarter/season
> **actuals** are (re)generated by `bin/backfill_discharge_aggregation.sh`
> (see §3.5 of [`update_deployment_checklist.md`](./update_deployment_checklist.md)),
> which additionally covers pentad/decad/quarter and matches iEasyHydro HF to 3
> significant figures. Prefer that tool for the actuals; the
> `yearly_runoff_hydrograph_aggregation.sh` path below predates the parity fix.
> Do not run both aggregation paths over the same rows in one pass.

### Files / scripts invoked

```text
${REPO}/bin/yearly_runoff_hydrograph_aggregation.sh
${REPO}/apps/preprocessing_runoff/sync_long_horizon_hydrograph.py
Docker image: mabesa/sapphire-preprunoff:${ieasyhydroforecast_backend_docker_image_tag}
```

### Depends on

P0.5, P2.

### Expected duration

Minutes per target year, depending on station count and iEH HF response time.

### Log location

```text
${ieasyhydroforecast_data_root_dir}/logs/runoff_hydrograph_aggregation/
```

### Server commands

Skip P9 if P0 classifies monthly/seasonal hydrograph as `DONE`.

```bash
cd "$REPO"
load_backfill_env

export START_DATE="${ieasyhydroforecast_START_DATE:?ieasyhydroforecast_START_DATE missing}"
export START_YEAR="${START_DATE%%-*}"
export END_YEAR="$(date -u +%Y)"
export LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/runoff_hydrograph_aggregation"
mkdir -p "$LOG_DIR"
export OUTER_LOG="${LOG_DIR}/p9_month_season_outer_$(date +%Y%m%d_%H%M%S).log"

test -n "${ENV_FILE:-}" && test -n "${START_YEAR:-}" && test -n "${END_YEAR:-}"
export ENV_FILE START_YEAR END_YEAR

nohup bash -c '
  set -e
  for YEAR in $(seq "$START_YEAR" "$END_YEAR"); do
    echo "running yearly_runoff_hydrograph_aggregation target-year=${YEAR}"
    bash bin/yearly_runoff_hydrograph_aggregation.sh "$ENV_FILE" --target-year "$YEAR"
  done
' > "$OUTER_LOG" 2>&1 &

echo "P9 PID=$!"
echo "tail -f $OUTER_LOG"
```

### Acceptance criteria

```bash
docker exec -i sapphire-preprocessing-db \
  psql -U postgres -d preprocessing_db -P pager=off <<SQL
SELECT
  horizon_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT code) AS sites,
  MIN(date) AS min_date,
  MAX(date) AS max_date,
  COUNT(*) FILTER (WHERE norm IS NOT NULL) AS norm_rows,
  COUNT(*) FILTER (WHERE previous IS NOT NULL) AS previous_rows,
  COUNT(*) FILTER (WHERE current IS NOT NULL) AS current_rows
FROM hydrographs
WHERE horizon_type IN ('month', 'season')
GROUP BY horizon_type
ORDER BY horizon_type;
SQL
```

Accept when:

```text
1. hydrographs/month rows exist for every target year run.
2. hydrographs/season rows exist for every target year run.
3. norm, previous, and current are populated where source data exists.
4. Stations skipped by bad SDK responses are listed in logs and reviewed.
```

### Failure recovery

Hydrographs upsert by `(horizon_type, code, date)`.

If a target year fails:

```text
1. Read the target year's service log.
2. Rerun only that target year.
3. If source data for a station is bad, accept skip-and-continue only after documenting the
   station as a redacted count, not by code.
```

---

## P10: Final verification and dashboard smoke test

### Goal

Prove the production DB has continuous historical coverage and the dashboard no
longer has a hydrograph gap.

### Files / scripts invoked

No write scripts.

### Depends on

P1, P2, P3, P4, P5, P7, P8, P9. P6 may be skipped/blocked if CM is
disabled/not runnable.

### Expected duration

30–60 minutes.

### Log location

```text
${ieasyhydroforecast_data_root_dir}/logs/full_backfill_diagnostics/p10_<timestamp>.log
```

### Server commands

```bash
cd "$REPO"
load_backfill_env

export VERIFY_LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/full_backfill_diagnostics"
mkdir -p "$VERIFY_LOG_DIR"
export VERIFY_LOG="${VERIFY_LOG_DIR}/p10_$(date +%Y%m%d_%H%M%S).log"
```

Readiness:

```bash
{
  echo "== gateway readiness =="
  curl -fsS http://localhost:8000/health/ready

  echo
  echo "== dashboard HTTP probes =="
  curl -fsSI http://localhost:5006 | head -20 || true
  curl -fsSI http://localhost:5007 | head -20 || true
} | tee "$VERIFY_LOG"
```

Final preprocessing coverage:

```bash
docker exec -i sapphire-preprocessing-db \
  psql -U postgres -d preprocessing_db -P pager=off <<'SQL' | tee -a "$VERIFY_LOG"
\echo '== final preprocessing coverage =='
SELECT 'runoffs' AS table_name, horizon_type::text AS subtype, COUNT(*) AS rows, COUNT(DISTINCT code) AS sites, MIN(date), MAX(date)
FROM runoffs
GROUP BY horizon_type
UNION ALL
SELECT 'hydrographs', horizon_type::text, COUNT(*), COUNT(DISTINCT code), MIN(date), MAX(date)
FROM hydrographs
GROUP BY horizon_type
UNION ALL
SELECT 'meteo', meteo_type::text, COUNT(*), COUNT(DISTINCT code), MIN(date), MAX(date)
FROM meteo
GROUP BY meteo_type
UNION ALL
SELECT 'snow', snow_type::text, COUNT(*), COUNT(DISTINCT code), MIN(date), MAX(date)
FROM snow
GROUP BY snow_type
ORDER BY table_name, subtype;
SQL
```

Final postprocessing coverage:

```bash
docker exec -i sapphire-postprocessing-db \
  psql -U postgres -d postprocessing_db -P pager=off <<'SQL' | tee -a "$VERIFY_LOG"
\echo '== final postprocessing coverage =='
SELECT 'lr_forecasts' AS table_name, horizon_type::text, 'LR' AS model, COUNT(*) AS rows, COUNT(DISTINCT code) AS sites, MIN(date), MAX(date)
FROM lr_forecasts
GROUP BY horizon_type
UNION ALL
SELECT 'forecasts', horizon_type::text, model_type::text, COUNT(*), COUNT(DISTINCT code), MIN(date), MAX(date)
FROM forecasts
GROUP BY horizon_type, model_type
UNION ALL
SELECT 'long_forecasts', horizon_type::text, model_type::text, COUNT(*), COUNT(DISTINCT code), MIN(date), MAX(date)
FROM long_forecasts
GROUP BY horizon_type, model_type
UNION ALL
SELECT 'skill_metrics', horizon_type::text, model_type::text, COUNT(*), COUNT(DISTINCT code), MIN(date), MAX(date)
FROM skill_metrics
GROUP BY horizon_type, model_type
ORDER BY table_name, subtype, model;
SQL
```

API spot checks:

```bash
{
  echo
  echo "== API spot checks =="

  check_api_nonempty() {
    label="$1"
    url="$2"
    tmp="$(mktemp)"
    curl -fsS "$url" -o "$tmp"
    python3 - "$tmp" "$label" <<'PY'
import json
import sys

path, label = sys.argv[1], sys.argv[2]
with open(path) as f:
    data = json.load(f)
if not isinstance(data, list) or len(data) == 0:
    raise SystemExit(f"{label}: EMPTY API response")
print(f"{label}: {len(data)} row(s)")
PY
    head -c 1000 "$tmp"
    echo
    rm -f "$tmp"
  }

  check_api_nonempty "preprocessing runoff day" "http://localhost:8000/api/preprocessing/runoff/?horizon=day&limit=1"
  check_api_nonempty "preprocessing hydrograph day" "http://localhost:8000/api/preprocessing/hydrograph/?horizon=day&limit=1"
  check_api_nonempty "preprocessing meteo T" "http://localhost:8000/api/preprocessing/meteo/?meteo_type=T&limit=1"
  check_api_nonempty "preprocessing snow HS" "http://localhost:8000/api/preprocessing/snow/?snow_type=HS&limit=1"
  check_api_nonempty "postprocessing forecast" "http://localhost:8000/api/postprocessing/forecast/?limit=1"
  check_api_nonempty "postprocessing long forecast" "http://localhost:8000/api/postprocessing/long-forecast/?limit=1"
  check_api_nonempty "postprocessing skill metric" "http://localhost:8000/api/postprocessing/skill-metric/?limit=1"
} | tee -a "$VERIFY_LOG"
```

Verification: preprocessing route parameters are `horizon` for `/runoff/` and
`/hydrograph/`, `meteo_type` for `/meteo/`, and `snow_type` for `/snow/` in
`sapphire/services/preprocessing/app/main.py:73-93`, `:116-136`, `:159-179`,
and `:202-219`; postprocessing routes are singular `/forecast/`,
`/long-forecast/`, and `/skill-metric/` in
`sapphire/services/postprocessing/app/main.py:71-99`, `:123-151`, and `:219-241`.

Dashboard smoke test:

```text
1. Open the deployment's pentad dashboard.
2. Select one non-sensitive station visually, without recording the code in this plan.
3. Confirm hydrograph spans historical years without a gap.
4. Confirm snow plot shows climatology/stat bands when SAPPHIRE_SNOW_STATS_AVAILABLE is enabled.
5. Confirm pentad and decade forecast/skill summary tables have LR and configured ML models.
6. Confirm monthly/seasonal long-term views show at least five historical years of
   hindcasts/skills.
```

### Acceptance criteria

P10 is accepted when:

```text
1. All required DB coverage categories are DONE or explicitly SKIPPED_BY_CONFIG.
2. No required category remains EMPTY.
3. Any PARTIAL category has a documented source/config reason and owner.
4. Gateway readiness is healthy.
5. Dashboard pages load.
6. API spot checks return non-empty JSON lists; an empty list fails P10 even if HTTP
   status is 200.
7. The dashboard does not show a hydrograph gap for the checked station.
```

### Failure recovery

If P10 fails:

```text
1. Map the failure to the earliest phase that owns the missing data type.
2. Rerun that phase only.
3. Rerun P8 skill metrics if the missing data type is any forecast family.
4. Rerun P10.
```

---

## Risk register

- **Docker bridge networking:** Dashboard containers cannot reach host loopback SSH
  tunnels under bridge networking. Mitigation: verify the server-side
  `network_mode: host` dashboard fix remains in place before final dashboard smoke.
  Most write-phase `docker run` commands use `--network host` (they reach the DB via
  the gateway on `localhost:8000`). **P7 (long-term hindcast) is the exception:** it
  reads `preprocessing-db` directly by compose DNS, so it runs on
  `--network sapphire_sapphire-network` with `SAPPHIRE_API_URL=http://api-gateway:8000`
  — `--network host` fails there with `could not translate host name "preprocessing-db"`.

- **Snow stat write gap:** The `/snow/` API exposes stat fields, but operational gateway
  writes do not populate them. Mitigation: run P3 `backfill_snow_stats_history.sh`;
  verify non-null `mean/q05/q95/previous/current` rows.

- **Snow backfill exit-1 cleanup quirk:** The wrapper can exit 1 after all years are
  processed because shared cleanup references an unset SSH tunnel PID under `set -u`.
  Mitigation: use the P3 `backfill_progress.txt` unique line-count check and DB
  verification before classifying failure.

- **ML DAY vs PENTAD/DECADE archive split:** ML skill metrics can starve if recalc
  reads only DAY rows. Mitigation: deployed `origin/maxat_sapphire_2` reader merges
  DAY plus period archives; P8 explicitly checks ML `n_pairs`.

- **LR issue-date convention:** `forecast_horizon_int` is the issue period, not target
  period. Mitigation: do not alter LR metadata or try to "fix" issue-date indexing
  during backfill.

- **Long-term runtime:** Long-term hindcasts and skill metrics can run for 3+ hours.
  Mitigation: run P7/P8 unattended in `tmux`/`nohup`, serialize modes, and use DB
  acceptance queries rather than exit code alone.

- **`simulate_forecasts.py` partial-failure exit code:** The deployed script can exit 0
  despite partial failures. Mitigation: P7 acceptance is based on `long_forecasts`
  coverage and logs, not process exit alone.

- **Conceptual model readiness:** CM R Docker scripts may contain deployment-specific
  hardcoded env paths. Mitigation: P6 is conditional and blocked if CM is enabled but
  not verified for your deployment; coordinate with the CM owner.

- **Operator-time budget:** Full plan is roughly two working days with unattended long
  phases. Mitigation: run phases in order, skip `DONE` phases from P0, and tail named
  logs.

- **Accidental sensitive output:** SQL counts and ranges are safe; real station codes
  and values are not. Mitigation: use `COUNT(DISTINCT code)`, min/max dates, and
  `19999` sentinel only.

---

## Coordination requirements

Confirm these before execution starts:

- Confirm the target server and env file path for your deployment (see operator setup block).
- Confirm the active image tag to use, especially if `ieasyhydroforecast_backend_docker_image_tag`
  is `local`.
- Confirm cron jobs are paused or outside their run window.
- Confirm P0.5 completed and `MANIFEST.txt` records `preprocessing_pre_p2_<UTC>.dump` and
  `postprocessing_pre_p2_<UTC>.dump` before write phases.
- Confirm whether ML is enabled and which models are configured for your deployment.
- Confirm whether CM is enabled. If enabled, coordinate with the CM owner before P6.
- Confirm long-term configured modes include the requested month and seasonal modes, or document
  which modes are absent.
- Confirm the service-owner colleague does not need to approve any service API/schema change;
  this runbook should not require one.
- Confirm dashboard host-networking fix remains present if your deployment uses SSH-tunnelled
  iEH HF.
- Confirm the operator accepts unattended `nohup`/`tmux` runs for P3, P5, P7, and P8.

---

## Out of scope

- This runbook does not modify Luigi DAGs to include missing initialization steps.
- This runbook does not implement `GatewayInitial`, `LongTermInitial`, or
  `RunLongTermInitializeWorkflow`; that remains tracked in
  `high_prio_gi_draft_infra_initialize_deployment_long_term.md`.
- This runbook does not edit `sapphire/services/*`, service schemas, endpoints, or migrations.
- This runbook does not fix the conceptual-model hardcoded env path.
- This runbook does not fix the snow backfill cleanup exit-1 quirk.
- This runbook does not add Docker image labels or change tag/build pipelines.
- This runbook does not expose real station codes, credentials, discharge values, or env
  contents.
- This runbook does not replace daily cron maintenance; it only backfills historical
  production state.
- This runbook does not debug DG auth or SSH tunnel setup beyond pre-checking that they
  are up.

---

## Dependency graph

```json
{
  "phases": {
    "P0": {
      "name": "Diagnostic and DB state inventory",
      "depends_on": [],
      "parallel_agents": 1,
      "writes_data": false
    },
    "P0.5": {
      "name": "Pre-backfill database backup",
      "depends_on": ["P0"],
      "parallel_agents": 1,
      "writes_data": false
    },
    "P1": {
      "name": "Gateway historical ERA5 meteo and raw snow",
      "depends_on": ["P0.5"],
      "parallel_agents": 1,
      "writes_data": true,
      "skip_if": ["P0.meteo == DONE", "P0.raw_snow == DONE"]
    },
    "P1.5": {
      "depends_on": ["P0.5", "P1"],
      "parallel_agents": 1,
      "writes_data": true,
      "skip_if": "snow_value_rows DONE",
      "note": "workaround for value-loss bug; see P1.5 section"
    },
    "P2": {
      "name": "Daily runoff and day hydrograph historical backfill",
      "depends_on": ["P0.5"],
      "parallel_agents": 1,
      "writes_data": true,
      "skip_if": ["P0.daily_runoff == DONE", "P0.day_hydrograph == DONE"]
    },
    "P3": {
      "name": "Historical snow stats and snow norms",
      "depends_on": ["P0.5", "P1", "P1.5"],
      "parallel_agents": 1,
      "writes_data": true,
      "skip_if": ["P0.snow_stats_norms == DONE"]
    },
    "P4": {
      "name": "LR PENTAD and DECAD historical hindcasts",
      "depends_on": ["P0.5", "P1", "P2"],
      "parallel_agents": 1,
      "writes_data": true,
      "skip_if": ["P0.lr_hindcasts == DONE", "P0.pentad_decade_hydrograph == DONE"]
    },
    "P5": {
      "name": "ML PENTAD and DECAD historical hindcasts",
      "depends_on": ["P0.5", "P1", "P2"],
      "parallel_agents": 3,
      "writes_data": true,
      "skip_if": ["P0.ml_hindcasts == DONE"]
    },
    "P6": {
      "name": "Conceptual-model hindcasts conditional",
      "depends_on": ["P0.5", "P1", "P2"],
      "parallel_agents": 1,
      "writes_data": true,
      "skip_if": ["ieasyhydroforecast_run_CM_models != true (default: SKIP — CM is being deprecated)"],
      "may_block": true
    },
    "P7": {
      "name": "Long-term hindcasts for at least five historical years",
      "depends_on": ["P0.5", "P1", "P2"],
      "parallel_agents": 1,
      "writes_data": true,
      "skip_if": ["P0.long_term_hindcasts == DONE"]
    },
    "P8": {
      "name": "Skill metrics recalculation for all populated horizons",
      "depends_on": ["P0.5", "P4", "P5", "P6", "P7"],
      "parallel_agents": 1,
      "writes_data": true,
      "notes": "P6 may be SKIPPED_BY_CONFIG or BLOCKED; P8 proceeds for non-CM models."
    },
    "P9": {
      "name": "Monthly and seasonal runoff hydrograph aggregation",
      "depends_on": ["P0.5", "P2"],
      "parallel_agents": 1,
      "writes_data": true,
      "skip_if": ["P0.monthly_seasonal_hydrograph == DONE"]
    },
    "P10": {
      "name": "Final verification and dashboard smoke test",
      "depends_on": ["P0.5", "P1", "P2", "P3", "P4", "P5", "P7", "P8", "P9"],
      "parallel_agents": 1,
      "writes_data": false,
      "notes": "Include P6 only if CM was enabled and runnable."
    }
  }
}
```
