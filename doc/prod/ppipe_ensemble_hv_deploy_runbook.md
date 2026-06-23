# P-PIPE Ensemble Horizon-Value Deploy Runbook

This runbook covers the operational sequence for deploying P-PIPE ensemble
horizon-value handling and safely unblocking the later cleanup. It is
per-deployment, aggregate-only, and uses sentinel station codes only. Do not put
real station codes, discharge values, row payloads, or delete statements in this
document.

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

1. Merge the P-PIPE branch through the normal review path.
2. Deploy the merged code to each target deployment independently.
3. Confirm the deployed image or checkout contains the P-PIPE code before
   starting the recalculation.

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
