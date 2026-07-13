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
| e | Lead-aware skill & ensembles (`SAPPHIRE_SKILL_LEAD_AWARE`, **default OFF, opt-in per deployment**) | `apps/postprocessing_forecasts/`, `apps/forecast_dashboard/src/db.py`, `apps/iEasyHydroForecast/{long_term_horizon_resolver,skill_lead_aware_flag}.py` — see the dedicated section below |

> **Note on (d):** the seasonal-bulletin env var
> `ieasyforecast_template_season_bulletin_file` has been correct in the code
> since 2026-04-28 (commit `e82c951e`). The recently observed "bulletin does not
> update" failure was a **deployment `.env` misconfiguration** (the var was unset
> / pointed at a missing template), surfacing as `os.path.join(None, …)`. It is
> therefore an **env-preflight item (Phase 2)**, not a code change to ship.
>
> **Concrete form (kyg, 2026-06-24):** the `.env` defined the var under the
> *old* name `ieasyforecast_template_seasonal_bulletin_file` (`seasonal`), while
> `bulletins.py` reads `...season...` (renamed in `e82c951e`). `os.getenv`
> returned `None` → the join failed. The fix is a one-line `.env` **rename**
> (value unchanged; the template file itself was present). **Check this on every
> deployment** — taj and uzb `.env`s likely carry the same stale `seasonal` key:
> `grep -E 'template_(season|seasonal)_bulletin_file' <env>`.

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

## Lead-aware skill & ensembles (`SAPPHIRE_SKILL_LEAD_AWARE`) — opt-in per deployment

**Status:** shipped **flag-gated, default OFF**. Merging the code changes
**nothing** in a deployment until this flag is explicitly set — a golden
byte-identity test enforces that flag-OFF output is identical to before. You
must therefore **turn it on per deployment**, once that deployment is ready.

**What it does when ON:** skill metrics and EM / Naive Mean / Skilled Mean
ensembles are computed and stored **per operational lead** (`horizon_value`)
instead of collapsed — month keeps one row per lead `0..N`, quarter stays
single-lead per deployment, season keeps one row per seasonal issue lead. The
operational "latest" readers and the maintenance gap-fill also select only the
configured operational issuance (by `operational_month_lead_time` +
`operational_issue_day`) instead of blending re-issues / backfills. It also
un-hides Tajik's flagship monthly forecast (`month_1` = lead 0), previously
masked by a hard-coded `horizon_value=1`.

**⛔ Enable prerequisite (hard — fail-loud):** every long-term config JSON for the
deployment must carry BOTH `operational_month_lead_time` AND
`operational_issue_day`. Under the flag the write path **raises and aborts** if
`operational_issue_day` is missing (by design — it must never silently score the
wrong rows). Verify on the server:

```bash
for f in "$ieasyforecast_configuration_path/$ieasyhydroforecast_ml_long_term_configuration"/*.json; do
  [ -f "$f" ] || continue
  printf '%-22s lead=%s issue_day=%s\n' "$(basename "$f")" \
    "$(grep -c operational_month_lead_time "$f")" \
    "$(grep -c operational_issue_day "$f")"
done
# every row must show lead=1 issue_day=1 (present). issue_day=0 => NOT READY; do not enable.
```

**Per-deployment readiness (as of 2026-07-13):**

| Deployment | long-term configs | `operational_issue_day` | Safe to enable? |
|------------|-------------------|-------------------------|-----------------|
| Kyrgyz (kyg) | 9 (month_0-3, quarter, seasonal_jan-apr) | all present | **yes** (with recalc) |
| Tajik (taj) | 5 (month_1-3, quarter, seasonal_april) | all present | **yes** (with recalc) |
| Uzbek (uzb) | **none present** | — | **NO** — add long-term configs first |

**To enable (per deployment):**

1. Confirm the prerequisite check above passes for THIS deployment.
2. Add to the deployment `.env` (this is the one line that turns it on):
   ```
   SAPPHIRE_SKILL_LEAD_AWARE=true
   ```
3. Run the **full-history recalc (Phase 3)** right after enabling, so the stored
   skill/ensemble rows are **consistently per-lead**. Existing rows were written
   single-lead; without a recalc the DB stays a mix of single-lead and per-lead
   until the next natural recalc.
4. Verify per-lead rows exist (aggregate-only, sentinel codes — see Phase 4).

**To roll back:** set `SAPPHIRE_SKILL_LEAD_AWARE=false` (or remove the line) in
the `.env`; the code reverts to the pre-feature single-lead behavior. Re-run the
recalc if you need the stored rows collapsed back.

**Do NOT enable** on a deployment whose configs lack `operational_issue_day`
(e.g. uzb today): the flag-ON recalc will hard-error by design.

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

The recalc step (Phase 3) imports the long-term horizon resolver for the
SEASONAL/ALL branch, which **raises and aborts** if any resolver env var is unset
or empty. `ieasyhydroforecast_ml_long_term_supported_modes` gates SEASONAL/ALL
only; QUARTERLY does not reach the resolver. Verify all of the following on the
server **before** Phase 3.

**Required recalc / resolver env vars**:

| Env var | Holds |
|---------|-------|
| `ieasyhydroforecast_env_file_path` | deployment `.env` file path loaded by direct-form recalc |
| `ieasyforecast_configuration_path` | config root directory |
| `ieasyhydroforecast_ml_long_term_configuration` | sub-dir with the long-term config JSONs |
| `ieasyhydroforecast_ml_long_term_supported_modes` | comma-separated modes for SEASONAL/ALL resolver use (e.g. `quarter,seasonal_january,…`) |

```bash
# Verify the values the CONTAINER will see: load the deployment .env first, then
# check. A bare printenv of your interactive shell would miss .env-only vars
# (the recalc loads this same .env via read_configuration / load_environment).
ENV_FILE_PATH=<deployment .env>
(
  set -a; . "$ENV_FILE_PATH"; set +a
  export ieasyhydroforecast_env_file_path="$ENV_FILE_PATH"
  for v in ieasyhydroforecast_env_file_path \
           ieasyforecast_configuration_path \
           ieasyhydroforecast_ml_long_term_configuration \
           ieasyhydroforecast_ml_long_term_supported_modes; do
    [ -n "$(printenv "$v")" ] || echo "MISSING $v"
    printf '%s=%s\n' "$v" "$(printenv "$v")"
  done
)
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

> **If enabling `SAPPHIRE_SKILL_LEAD_AWARE`** (see the *Lead-aware skill* section
> above): the same config JSONs must ALSO carry `operational_issue_day`, and the
> `.env` must set `SAPPHIRE_SKILL_LEAD_AWARE=true`. The flag-ON recalc fails loud
> without the field. Run the prerequisite check in that section before Phase 3.

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

**Backup first.** `DB_BACKUP_DIR` must already exist before running the backup
helper.

```bash
bash bin/backup_sapphire_db.sh -e ${ENV_FILE_PATH} -d "$DB_BACKUP_DIR" -r 30
```

Run aggregate SQL checks through the postprocessing DB container:

```bash
docker exec -i sapphire-postprocessing-db psql -U postgres -d postprocessing_db
```

If a deployment intentionally uses a different DB user, confirm that privately
before substituting it; do not commit connection details.

**Containerized form (recommended on servers — same path the cron uses).** The
maintained wrapper runs the recalc in-container and forwards
`SAPPHIRE_RECALC_START_YEAR`. It loops MONTHLY/QUARTERLY/SEASONAL, so one run
covers both long-term modes (the extra MONTHLY pass is harmless):

```bash
SAPPHIRE_RECALC_START_YEAR=2000 bash bin/bimonthly_long_term_skill_metrics_recalculation.sh "$ENV_FILE_PATH"
```

**Direct form (one mode at a time, runs the script on the host).** Only this form
selects the mode via `SAPPHIRE_PREDICTION_MODE`:

```bash
set -a
. "$ENV_FILE_PATH"
set +a
export ieasyhydroforecast_env_file_path="$ENV_FILE_PATH"

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
paste real station codes. Reference the EM query there; do not fork a second
copy here.

**SQL literal rule:** SQL examples and ad-hoc SQL must use DB enum names, never
API values: `ENSEMBLE_MEAN`, `LR_BASE`, `LR_SM`, `NAIVE_MEAN`,
`SKILLED_MEAN`. Do not filter `model_type` with API values such as `EM` or
`LR_Base`, or with lowercase `LR_SM` variants in SQL.

Confirm:

1. **Buckets exist and span full history for this deployment** (use the
   bucket-presence + aggregate queries in the P-PIPE runbook):
   - taj: `QUARTER hv=0`, `SEASON hv=0`
   - kyg: `QUARTER hv=1`, `SEASON hv ∈ {3,2,1,0}`

   Each expected bucket must have `row_count > 0`, **and** from the aggregate
   query `MIN(date) ≤ <SAPPHIRE_RECALC_START_YEAR>-01-01` and
   `MAX(date) ≥ <current operational issue date>` (latest issue date from the
   deployment's most recent successful operational long-term run). A missing
   bucket or a date range that doesn't span this window **fails** the gate — a
   partial-history recalc must not pass.
2. **`EM = mean(LR_BASE, LR_SM)`** join check returns
   `em_pairs > 0 AND mismatch = 0` (holds per issue date for season). A non-zero
   mismatch on composition-populated rows means the deployed image is still the
   pre-fix one — revisit Phase 1. `mismatch = 0` alone is not sufficient; it can
   also mean the join found no EM/LR pairs.
   *(Model labels in the DB are uppercase: `LR_BASE`, `LR_SM`, `ENSEMBLE_MEAN`.)*
3. **Seasonal bulletin renders** (change d): first **recreate the dashboard** so it
   reloads the corrected `.env` — do this **through the deployment's env path**, not
   a bare `docker restart`. A plain restart reuses the container's baked env and
   leaves `ieasyhydroforecast_url_pentad` empty (it is *derived* in
   `bin/utils/common_functions.sh` from the env-file suffix, then exported before
   `up`), so the dashboard crash-loops with `ERROR: Empty host value`. Recreate via:
   ```bash
   set +u +o pipefail          # read_configuration trips set -u on $-in-secret .env values
   source bin/utils/common_functions.sh
   export ieasyhydroforecast_env_file_path="<env>"
   read_configuration "<env>"
   docker compose --env-file "<env>" -f sapphire/docker-compose.yml up -d --force-recreate --no-deps dashboard
   ```
   Then in the dashboard, generate a seasonal bulletin and confirm the Excel file is
   written (this exercises `ieasyforecast_template_season_bulletin_file` end-to-end).
   Also smoke-check the monthly/decad/pentad bulletins haven't regressed.

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
