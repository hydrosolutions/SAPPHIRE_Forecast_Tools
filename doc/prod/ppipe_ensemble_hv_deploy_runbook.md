# P-PIPE Ensemble Horizon-Value Deploy Runbook

This runbook covers the operational sequence for deploying P-PIPE ensemble
horizon-value handling and safely unblocking the later cleanup. It is
per-deployment, aggregate-only, and uses sentinel station codes only. Do not put
real station codes, discharge values, row payloads, or delete statements in this
document.

> **Part of the consolidated long-term deploy.** For the full end-to-end
> long-term (quarter/season) server deploy — image scope, env preflight,
> regeneration, verification, and cleanup in order — start from
> [`long_term_deploy_runbook.md`](long_term_deploy_runbook.md). This document is
> the detailed P-PIPE sub-runbook it references for Phase 4 verification.

## Update 2026-06-23 — Two-Model EM Correctness Fixes (PR #383)

PR #383 (`develop_two_model_ensemble`) fixes three defects in the quarter/season
two-model ensemble. **Every deployment must redeploy the postprocessing image
and re-run the one-time recalculation** below — including deployments that
already ran the original P-PIPE recalc, because their stored EM/Naive Mean/
Skilled Mean rows were produced by the buggy code.

What changed and why a re-run is required:

1. **EM forecasts were never persisted.** The aggregated EM/Naive/Skilled rows
   lost their period key on append and were dropped by the write-side NaN guard,
   so `long_forecasts` had EM *skill* but no EM *forecast*. Fixed.
2. **Seasonal EM collapsed multiple issue dates.** A lead re-issued on several
   dates was blended into one row. EM is now computed per issue `date` as a clean
   `mean(LR_Base, LR_SM)`.
3. **Bulk write 500 on deployments with stored baselines.** The recalc re-read
   stored EM/Naive/Skilled rows and re-appended freshly computed ones; a shared
   unique key (`horizon_type, horizon_value, code, date, model_type, valid_from,
   valid_to`) raised a `UniqueViolation` and the whole forecast batch failed.
   This also affected the scheduled **bimonthly** long-term skill recalc on any
   deployment that already held those rows (e.g. Tajik): it wrote skill but
   silently failed to write the ensemble forecasts. The recalc now drops the
   three regenerated baselines from its input before recomputing.

**Self-heal note:** once the fixed image is deployed, the scheduled bimonthly
recalc (`bin/bimonthly_long_term_skill_metrics_recalculation.sh`, modes MONTHLY/
QUARTERLY/SEASONAL) regenerates correct EM on its next run with the default
history window. Run the one-time recalc below to land the fix immediately and
over full history (`SAPPHIRE_RECALC_START_YEAR=2000`).

## Scope and Hard Stops

- Deploy only after the P-PIPE code is merged through the normal deployment
  process.
- Run the one-time full-history recalculation once per deployment and prediction
  mode.
- Verify only aggregate counts and date ranges for sentinel station codes.
- Do not run cleanup until regenerated new-convention rows cover the full
  expected date range.
- Do not move or rewrite raw `LR_BASE` / `LR_SM` rows as part of P-PIPE.

## Sentinel Codes

Use deployment-local sentinel codes selected by the operator:

- Kyrgyz sentinel: `KG_SENTINEL_CODE`
- Tajik sentinel: `TJ_SENTINEL_CODE`

Replace these placeholders only in private operational notes or command history,
not in committed documentation.

**SQL literal rule:** SQL examples and ad-hoc SQL must use DB enum names, never
API values: `ENSEMBLE_MEAN`, `LR_BASE`, `LR_SM`, `NAIVE_MEAN`,
`SKILLED_MEAN`. Do not filter `model_type` with API values such as `EM` or
`LR_Base`, or with lowercase `LR_SM` variants in SQL.

## Ordered Sequence

### 1. Merge and Deploy P-PIPE

1. Merge the branch (PR #383) into `maxat_sapphire_2` through the normal review
   path. The push to `maxat_sapphire_2` triggers `deploy_production.yml`, which
   builds and pushes `mabesa/sapphire-postprocessing`. Wait for that workflow to
   finish before deploying to servers.
2. Deploy the merged code to each target deployment independently. On each
   server, refresh the postprocessing image and restart the stack. The generic
   image-pull / migration / restart procedure is in
   `doc/prod/update_deployment_checklist.md`; for this change the
   postprocessing image is the only one that must be refreshed:

   ```bash
   # Operator vars (see update_deployment_checklist.md):
   #   ORG_SLUG (kghm|tjhm|uzhm), DATA_DIR, ENV_FILE_PATH, LOG_DIR
   cd /data/SAPPHIRE_Forecast_Tools
   git fetch && git checkout maxat_sapphire_2 && git pull
   docker pull mabesa/sapphire-postprocessing:latest
   ```

   No schema migration is required for PR #383 (no new columns).
3. Confirm the deployed image contains the fix before starting the
   recalculation. The image must include the stored-baseline de-duplication;
   without it the bulk write 500s on deployments that already hold ensemble
   rows. Quick check:

   ```bash
   docker run --rm mabesa/sapphire-postprocessing:latest \
     grep -c "_AGGREGATED_BASELINES" src/skill_metrics.py   # expect >= 2
   ```

### 2. One-Time Full-History Recalculation

Run both modes per deployment with `SAPPHIRE_RECALC_START_YEAR=2000`.

Set the deployment env file path first:

```bash
export ENV_FILE_PATH=/data/<data_folder>/config/<env_file>
```

Backup first. `DB_BACKUP_DIR` must already exist before running the backup
helper.

```bash
bash bin/backup_sapphire_db.sh -e ${ENV_FILE_PATH} -d "$DB_BACKUP_DIR" -r 30
```

Direct runtime form:

```bash
set -a
. "$ENV_FILE_PATH"
set +a
export ieasyhydroforecast_env_file_path="$ENV_FILE_PATH"

SAPPHIRE_RECALC_START_YEAR=2000 SAPPHIRE_PREDICTION_MODE=QUARTERLY uv run recalculate_skill_metrics.py
SAPPHIRE_RECALC_START_YEAR=2000 SAPPHIRE_PREDICTION_MODE=SEASONAL uv run recalculate_skill_metrics.py
```

Containerized form (same path the cron uses; loops MONTHLY/QUARTERLY/SEASONAL, so
one run covers both long-term modes):

```bash
SAPPHIRE_RECALC_START_YEAR=2000 bash bin/bimonthly_long_term_skill_metrics_recalculation.sh "$ENV_FILE_PATH"
```

This driver sources `bin/utils/run_skill_metrics_recalc.sh` and calls its
`run_skill_metrics_recalc_once <mode> …` function per mode, forwarding non-empty
`SAPPHIRE_RECALC_START_YEAR` into `docker run`. Note: `run_skill_metrics_recalc.sh`
is a **sourced function library**, not a standalone command — do not run it
directly. If you build your own `docker run`, pass the explicit container env:

```bash
-e SAPPHIRE_RECALC_START_YEAR=2000
```

Do not pass an empty `SAPPHIRE_RECALC_START_YEAR`; the recalculation code parses
the value as an integer.

### 3. Aggregate-Only Verification

Verify that regenerated EM / Naive Mean / Skilled Mean rows exist under the new
convention for the full expected date range. Keep checks aggregate-only.

Kyrgyz expectations:

- Quarter rows exist at `horizon_type='QUARTER'` and `horizon_value=1`.
- Seasonal rows exist at `horizon_type='SEASON'` and `horizon_value in (3,2,1,0)`.

Tajik expectations:

- Quarter rows exist at `horizon_type='QUARTER'` and `horizon_value=0`.
- Seasonal rows exist at `horizon_type='SEASON'` and `horizon_value=0`.

Run SQL through the postprocessing DB container:

```bash
docker exec -i sapphire-postprocessing-db psql -U postgres -d postprocessing_db
```

If a deployment intentionally uses a different DB user, confirm that privately
before substituting it; do not commit connection details.

Aggregate query template:

```sql
SELECT
  horizon_type,
  horizon_value,
  model_type,
  MIN(date) AS first_issue_date,
  MAX(date) AS last_issue_date,
  COUNT(*) AS row_count
FROM long_forecasts
WHERE code IN ('KG_SENTINEL_CODE', 'TJ_SENTINEL_CODE')
  AND model_type IN ('ENSEMBLE_MEAN', 'NAIVE_MEAN', 'SKILLED_MEAN')
  AND horizon_type IN ('QUARTER', 'SEASON')
GROUP BY horizon_type, horizon_value, model_type
ORDER BY horizon_type, horizon_value, model_type;
```

Compare the aggregate result against the deployment's expected full-history date
range. Do not publish row-level data or discharge values.

Expected bucket presence query template (Kyrgyz values shown; for Tajik replace
the `expected_buckets` values with `('QUARTER',0),('SEASON',0)`):

```sql
WITH expected_buckets(horizon_type, horizon_value) AS (
  VALUES
    ('QUARTER', 1),
    ('SEASON', 3),
    ('SEASON', 2),
    ('SEASON', 1),
    ('SEASON', 0)
),
actual AS (
  SELECT
    horizon_type,
    horizon_value,
    COUNT(*) AS row_count
  FROM long_forecasts
  WHERE code = 'KG_SENTINEL_CODE'
    AND model_type IN ('ENSEMBLE_MEAN', 'NAIVE_MEAN', 'SKILLED_MEAN')
    AND horizon_type IN ('QUARTER', 'SEASON')
  GROUP BY horizon_type, horizon_value
)
SELECT
  e.horizon_type,
  e.horizon_value,
  COALESCE(a.row_count, 0) AS row_count
FROM expected_buckets e
LEFT JOIN actual a USING (horizon_type, horizon_value)
ORDER BY e.horizon_type, e.horizon_value;
```

**Per-bucket acceptance (quantified — do not accept "looks populated"):** every
expected bucket must have `row_count > 0`; **and** from the aggregate query
above, `MIN(date) <= <SAPPHIRE_RECALC_START_YEAR>-01-01` and
`MAX(date) >= <current operational issue date>` (the latest issue date from the
deployment's most recent successful operational long-term run). A bucket with
`row_count = 0`, or coverage that does not span this window, **fails** — a
partial-history recalc must not pass to cleanup.

Also confirm that EM forecasts were persisted (not just EM skill) and that each
EM equals the clean two-model mean at its exact key. With the PR #383 fix, every
recalc-written EM (composition populated) must satisfy
`q = mean(LR_BASE.q, LR_SM.q)` joined on
`(horizon_type, horizon_value, code, date, valid_from, valid_to)`. Expect
`em_pairs > 0 AND mismatch = 0`; for season this holds **per issue date** (one
EM row per date, not one blended row per lead).

```sql
WITH j AS (
  SELECT em.q AS em_q, (lb.q + ls.q) / 2.0 AS lr_mean
  FROM long_forecasts em
  JOIN long_forecasts lb USING (horizon_type, horizon_value, code, date, valid_from, valid_to)
  JOIN long_forecasts ls USING (horizon_type, horizon_value, code, date, valid_from, valid_to)
  WHERE em.horizon_type IN ('QUARTER', 'SEASON')
    AND em.model_type = 'ENSEMBLE_MEAN'
    AND lb.model_type = 'LR_BASE'
    AND ls.model_type = 'LR_SM'
    AND em.q IS NOT NULL
    AND COALESCE(em.composition, '') <> ''
    AND em.code IN ('KG_SENTINEL_CODE', 'TJ_SENTINEL_CODE')
)
SELECT
  COUNT(*) AS em_pairs,
  COUNT(*) FILTER (WHERE abs(em_q - lr_mean) < 0.001) AS match,
  COUNT(*) FILTER (WHERE abs(em_q - lr_mean) >= 0.001) AS mismatch
FROM j;
```

A non-zero `mismatch` on composition-populated rows means the deployed image is
stale (pre-fix). Pre-existing rows with empty `composition` are old-convention
artifacts and are excluded here; they are handled by the cleanup gate below.
The cleanup gate requires `em_pairs > 0 AND mismatch = 0`; `mismatch = 0` alone
is not sufficient because it can also mean the join found no EM/LR pairs.

### 4. Cleanup Gate

Only after the aggregate verification passes may the cleanup owner re-derive
obsolete old-convention cleanup predicates.

The authoritative cleanup ordering is the cross-plan reconciliation section in
`doc/plans/archive/ppipe_postprocessing_ensemble_hv_plan.md`. That section
supersedes the old ensemble handling in
`doc/plans/archive/longforecast_hv_convention_plan.md` P3.

Cleanup requirements:

- Re-derive predicates after the regeneration, not from pre-regeneration counts.
- Scope predicates to old-convention signatures only.
- Season old signature: ensemble rows at `horizon_value=1` where `date` is the
  target-season start, not the issue date.
- Quarter old signature: obsolete ensemble rows at `horizon_value in (2,3,4)`,
  plus old calendar `horizon_value=1` rows distinguishable from the regenerated
  product by reviewed aggregate predicates.
- Obtain reviewer approval on aggregate dry-run counts before any delete.

The superseded longforecast P3 banner points back to the P-PIPE reconciliation:
predicates must be re-derived post-regen, old-signature-scoped, and the old
pre-regen counts are stale.

## Completion Evidence

For each deployment, retain private operational evidence with:

- deployed version identifier;
- quarter and seasonal recalculation timestamps;
- aggregate verification counts and full date ranges for sentinel codes;
- cleanup dry-run counts and reviewer approval, if cleanup proceeds.

Do not commit private evidence containing real station codes, discharge values,
connection details, or write output.
