# Long-Term (Quarter / Season) Forecast Deploy Runbook

**Audience:** operator deploying the long-term forecast changes to a deployment
server (testing or production).
**Scope:** the consolidated, end-to-end procedure for getting the quarter/season
forecast + ensemble + skill changes correctly onto a server. This is the single
entry point; it links out to the detailed sub-runbooks rather than duplicating
their SQL.

This runbook is **per deployment** (run it once for Tajik, once for Kyrgyz). The
two deployments differ — see the parameter table below.

---

## What this runbook deploys

| # | Change | Where it lives |
|---|--------|----------------|
| a | P-PIPE: quarter/season ensemble `horizon_value` = config lead; per-issue seasonal threading; per-lead seasonal skill | `apps/postprocessing_forecasts/`, `apps/iEasyHydroForecast/long_term_horizon_resolver.py` |
| b | Two-model ensemble: quarter/season `EM = mean(LR_Base, LR_SM)`, de-skill-gated; reader drops 7 deprecated quarter models | `apps/postprocessing_forecasts/src/{ensemble_calculator,skill_metrics,data_reader,model_names}.py` |
| c | From-file importer accepts quarter/season + code-scoped gap-fill | `bin/utils/migration_py/long_forecast.py` |
| d | Seasonal Excel bulletin export | `apps/forecast_dashboard/src/bulletins.py` |

> **Note on (d):** the seasonal-bulletin env var
> `ieasyforecast_template_season_bulletin_file` has been correct in the code
> since 2026-04-28 (commit `e82c951e`). The recently observed "bulletin does not
> update" failure was a **deployment `.env` misconfiguration** (the var was unset
> / pointed at a missing template), surfacing as `os.path.join(None, …)`. It is
> therefore an **env-preflight item (Phase 2)**, not a code change to ship.

---

## Per-deployment parameters

`horizon_value` (`hv`) is `operational_month_lead_time` from each deployment's
long-term config JSONs (`<config_root>/<ml_long_term_configuration>/<mode>.json`).
These configs live in the per-deployment data repo, **not** in this repo.

| Deployment | Quarter `hv` | Season configs & `hv` (by issue month) |
|------------|--------------|----------------------------------------|
| **Tajik (taj)** | `0` | `seasonal_april` → `hv 0` only (no Jan/Feb/Mar configs) |
| **Kyrgyz (kyg)** | `1` | `seasonal_january`/`february`/`march`/`april` → `hv 3 / 2 / 1 / 0` |

Issue identity is in `date`; the target season is in `valid_from`/`valid_to`.

---

## Ordered server flow (do not reorder)

```
Phase 1  Deploy code/images
Phase 2  Env / config preflight        ← STOP GATE: recalc hard-errors if vars unset
Phase 3  Regenerate (full-history recalc)
Phase 4  Verify (buckets, EM=mean, bulletin render)   ← STOP GATE for cleanup
Phase 5  Cleanup (deprecated models + old-convention rows)
Phase 6  Re-verify (buckets + deprecated absent)
```

Each phase is idempotent and safe to re-run. **Phases 4 and 5 are gates:** do not
start cleanup until Phase 4 passes; do not finish until Phase 6 passes.

---

### Phase 1 — Deploy code / images

Refresh the repo and the images that carry these changes. The effective image tag
is **`.env`-driven** via `ieasyhydroforecast_backend_docker_image_tag` and
`ieasyhydroforecast_frontend_docker_image_tag` (the deployment checklist examples
use `:local`, the P-PIPE runbook examples use `:latest` — confirm which tag your
deployment `.env` pins before pulling).

```bash
cd <deployment-repo>
git pull                       # picks up bin/ importer + cron driver changes (c)
```

Pull / refresh these images (substitute your `.env` tag):

| Image | Required? | Carries |
|-------|-----------|---------|
| `mabesa/sapphire-postprocessing` (app-side) | **REQUIRED** | ensemble calculator, skill metrics, recalc (a, b) |
| `mabesa/sapphire-dashboard` | **REQUIRED** | seasonal bulletin export (d) |
| `mabesa/sapphire-lt-forecasting` | Refresh if its layer rebuilt | `run_forecast.py` hv-stamping + resolver (a) |

> The app-side `mabesa/sapphire-postprocessing` is **distinct** from the
> service-side `sapphire-postprocessing` FastAPI image (built from
> `sapphire/services/postprocessing/`). **No service-side change is needed** —
> these changes add no schema migration and no new columns.

**Image fix-presence check** (confirms the postprocessing image actually carries
the EM fix; expect `>= 2`):

```bash
docker run --rm mabesa/sapphire-postprocessing:<tag> \
  grep -c "_AGGREGATED_BASELINES" src/skill_metrics.py
```

Full image-deploy mechanics: [`update_deployment_checklist.md`](update_deployment_checklist.md).

---

### Phase 2 — Env / config preflight  ⛔ STOP GATE

The recalc step (Phase 3) imports the long-term horizon resolver, which **raises
and aborts** if any of its three env vars is unset or empty. Verify all of the
following on the server **before** Phase 3.

**Resolver env vars** (`apps/iEasyHydroForecast/long_term_horizon_resolver.py`):

| Env var | Holds |
|---------|-------|
| `ieasyforecast_configuration_path` | config root directory |
| `ieasyhydroforecast_ml_long_term_configuration` | sub-dir with the long-term config JSONs |
| `ieasyhydroforecast_ml_long_term_supported_modes` | comma-separated modes (e.g. `quarter,seasonal_january,…`) |

```bash
# all three must print a non-empty value:
for v in ieasyforecast_configuration_path \
         ieasyhydroforecast_ml_long_term_configuration \
         ieasyhydroforecast_ml_long_term_supported_modes; do
  printf '%s=%s\n' "$v" "$(printenv "$v")"
done
```

**Config-lead sanity** — confirm each mode JSON carries the expected
`operational_month_lead_time` for this deployment (see the parameter table):

```bash
# example (kyg): expect 1 for quarter; 3/2/1/0 for the four seasonal configs
for m in quarter seasonal_january seasonal_february seasonal_march seasonal_april; do
  f="$ieasyforecast_configuration_path/$ieasyhydroforecast_ml_long_term_configuration/$m.json"
  [ -f "$f" ] && printf '%-20s lead=%s\n' "$m" "$(grep operational_month_lead_time "$f")" \
              || printf '%-20s (no config — expected for taj Jan/Feb/Mar)\n' "$m"
done
```

**Seasonal bulletin template (change d):** verify the seasonal template var is set
and the file exists (this is the var behind the recent "bulletin won't update"
failure):

```bash
printf 'season   = %s\n' "$(printenv ieasyforecast_template_season_bulletin_file)"
# also confirm the file resolves under the templates directory and is present.
```

The pentad/decad/month equivalents
(`ieasyforecast_template_{pentad,decad,month}_bulletin_file`) should already be
set; the **season** one is the one to double-check.

Do not proceed until every value above is present and correct.

---

### Phase 3 — Regenerate (full-history recalc)

One-time full-history recalc for **both** long-term modes. This persists correct
per-issue/per-lead EM and skill over all history. (The scheduled bimonthly recalc
also self-heals going forward; this one-time run lands it immediately.)

**Containerized form (recommended on servers — same path the cron uses).** The
maintained wrapper runs the recalc in-container and forwards
`SAPPHIRE_RECALC_START_YEAR`. It loops MONTHLY/QUARTERLY/SEASONAL, so one run
covers both long-term modes (the extra MONTHLY pass is harmless):

```bash
SAPPHIRE_RECALC_START_YEAR=2000 bash bin/bimonthly_long_term_skill_metrics_recalculation.sh
```

**Direct form (one mode at a time, runs the script on the host).** Only this form
selects the mode via `SAPPHIRE_PREDICTION_MODE`:

```bash
SAPPHIRE_RECALC_START_YEAR=2000 SAPPHIRE_PREDICTION_MODE=QUARTERLY uv run recalculate_skill_metrics.py
SAPPHIRE_RECALC_START_YEAR=2000 SAPPHIRE_PREDICTION_MODE=SEASONAL  uv run recalculate_skill_metrics.py
```

> `bin/utils/run_skill_metrics_recalc.sh` is a **sourced function library**
> (`run_skill_metrics_recalc_once <mode> <log_dir> <ts> <container>`), **not** a
> standalone command — use the wrapper above, not that file directly.

- Do **not** pass an empty `SAPPHIRE_RECALC_START_YEAR` (it is parsed as an int).
- `SAPPHIRE_SKILL_METRICS_START_YEAR` overrides `SAPPHIRE_RECALC_START_YEAR` if
  both are set; default is `current_year − 20`.
- Forward maintenance is the same cron driver
  (`bin/bimonthly_long_term_skill_metrics_recalculation.sh`) on its schedule.
  Ensemble **forecast** creation (not skill) is
  `bin/bimonthly_long_term_postprocessing.sh`.

Detailed recalc rationale: [`ppipe_ensemble_hv_deploy_runbook.md`](ppipe_ensemble_hv_deploy_runbook.md).

---

### Phase 4 — Verify  ⛔ STOP GATE for cleanup

Run the **aggregate-only** verification queries from
[`ppipe_ensemble_hv_deploy_runbook.md`](ppipe_ensemble_hv_deploy_runbook.md)
(§ verification). Use sentinel codes (e.g. `19999`) in any ad-hoc query — never
paste real station codes. Confirm:

1. **Buckets exist for this deployment:**
   - taj: `QUARTER hv=0`, `SEASON hv=0`
   - kyg: `QUARTER hv=1`, `SEASON hv ∈ {3,2,1,0}`
2. **`EM = mean(LR_BASE, LR_SM)`** join check returns `mismatch = 0` (holds
   per issue date for season). A non-zero mismatch on composition-populated rows
   means the deployed image is still the pre-fix one — revisit Phase 1.
   *(Model labels in the DB are uppercase: `LR_BASE`, `LR_SM`, `ENSEMBLE_MEAN`.)*
3. **Seasonal bulletin renders** (change d): in the dashboard, generate a seasonal
   bulletin and confirm the Excel file is written (this exercises
   `ieasyforecast_template_season_bulletin_file` end-to-end). Also smoke-check the
   monthly/decad/pentad bulletins haven't regressed.

Do not start Phase 5 until 1–3 pass.

---

### Phase 5 — Cleanup (deprecated models + old-convention rows)

**Runs only after Phase 4 passes.** This removes (i) the 7 deprecated quarter
models and (ii) rows written under the old hv convention. The predicates must be
**re-derived from post-regen counts and reviewer-approved** before execution.

> ⚠️ **Superseded statement — do NOT run as written.** The old blanket
> `DELETE FROM long_forecasts WHERE horizon_type='QUARTER' AND model_type NOT IN ('LR_BASE','LR_SM')`
> would delete EM/Naive/Skilled rows that P-PIPE now **regenerates**. Use the
> old-signature-scoped predicates instead.

**(i) Deprecated quarter models** — drop from **both** `long_forecasts` and
`skill_metrics`, for `horizon_type IN ('QUARTER','SEASON')`:
`GBT, LR_SM_DT, LR_SM_ROF, MC_ALD, SM_GBT, SM_GBT_LR, SM_GBT_NORM`. Keep
`LR_BASE`, `LR_SM`. The quarter-raw `long_forecasts` delete is typically a 0-row
no-op (quarter raw is synthesized from MONTH rows at read time); the real targets
are the `skill_metrics` deprecated rows + any stale ensemble rows.
Predicates: [`two_model_ensemble_plan.md`](../plans/archive/two_model_ensemble_plan.md) (§ cleanup).

**(ii) Old-convention rows** — old-signature-scoped, post-regen (re-measure
counts; the pre-regen counts in the plan are stale): season `hv1` where
`date == target-start`; quarter `hv 2/3/4` orphans + old calendar-`hv1`; plus the
LR re-stamp `UPDATE`s. Use **uppercase** model labels.
Predicates: [`longforecast_hv_convention_plan.md`](../plans/archive/longforecast_hv_convention_plan.md) (P3).

Always back up the DB and dry-run the row counts before any `DELETE`/`UPDATE`.

---

### Phase 6 — Re-verify

Re-run the Phase 4 bucket + `EM=mean` checks, **plus** confirm the deprecated
models are gone:

- The 7 deprecated models return **0 rows** in both `long_forecasts` and
  `skill_metrics` for `horizon_type IN ('QUARTER','SEASON')`.
- Monthly model rows are **unchanged** (cleanup must not touch MONTH horizon).
- App unit tests green (`cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts`).

---

## Per-deployment summary

| Step | Tajik | Kyrgyz |
|------|-------|--------|
| Quarter bucket | `hv 0` | `hv 1` |
| Season buckets | `hv 0` (April only) | `hv 3/2/1/0` (Jan/Feb/Mar/Apr) |
| Seasonal configs to preflight | `seasonal_april` | `seasonal_{january,february,march,april}` |
| Recalc modes | QUARTERLY + SEASONAL | QUARTERLY + SEASONAL |

---

## Related documents

- [`ppipe_ensemble_hv_deploy_runbook.md`](ppipe_ensemble_hv_deploy_runbook.md) — detailed P-PIPE deploy + the verification SQL referenced in Phase 4.
- [`update_deployment_checklist.md`](update_deployment_checklist.md) — full image/service deploy mechanics.
- [`historical_backfill_runbook.md`](historical_backfill_runbook.md) — historical data backfill (server commands).
- [`../plans/archive/longforecast_hv_convention_plan.md`](../plans/archive/longforecast_hv_convention_plan.md) — hv convention + old-convention cleanup predicates (P3).
- [`../plans/archive/two_model_ensemble_plan.md`](../plans/archive/two_model_ensemble_plan.md) — two-model EM + deprecated-model cleanup predicates.
