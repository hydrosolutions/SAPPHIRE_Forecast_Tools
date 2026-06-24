# P-PIPE Ensemble Horizon-Value Deploy Runbook

This runbook covers the operational sequence for deploying P-PIPE ensemble
horizon-value handling and safely unblocking the later cleanup. It is
per-deployment, aggregate-only, and uses sentinel station codes only. Do not put
real station codes, discharge values, row payloads, or delete statements in this
document.

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

Direct runtime form:

```bash
SAPPHIRE_RECALC_START_YEAR=2000 SAPPHIRE_PREDICTION_MODE=QUARTERLY uv run recalculate_skill_metrics.py
SAPPHIRE_RECALC_START_YEAR=2000 SAPPHIRE_PREDICTION_MODE=SEASONAL uv run recalculate_skill_metrics.py
```

Docker helper form:

```bash
SAPPHIRE_RECALC_START_YEAR=2000 SAPPHIRE_PREDICTION_MODE=QUARTERLY bin/utils/run_skill_metrics_recalc.sh
SAPPHIRE_RECALC_START_YEAR=2000 SAPPHIRE_PREDICTION_MODE=SEASONAL bin/utils/run_skill_metrics_recalc.sh
```

The helper now forwards non-empty `SAPPHIRE_RECALC_START_YEAR` into `docker run`.
If bypassing the helper, pass the equivalent explicit container environment:

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

Also confirm that EM forecasts were persisted (not just EM skill) and that each
EM equals the clean two-model mean at its exact key. With the PR #383 fix, every
recalc-written EM (composition populated) must satisfy
`q = mean(LR_BASE.q, LR_SM.q)` joined on
`(horizon_type, horizon_value, code, date, valid_from, valid_to)`. Expect
`mismatch = 0`; for season this holds **per issue date** (one EM row per date,
not one blended row per lead).

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

### 4. Cleanup Gate

Only after the aggregate verification passes may the cleanup owner re-derive
obsolete old-convention cleanup predicates.

The authoritative cleanup ordering is the cross-plan reconciliation section in
`doc/plans/working/ppipe_postprocessing_ensemble_hv_plan.md`. That section
supersedes the old ensemble handling in
`doc/plans/working/longforecast_hv_convention_plan.md` P3.

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
