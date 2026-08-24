# Handoff - Short-term (pentad/decad) skill starvation: LR fix + one-time historical backfill

**Audience:** dev colleagues maintaining SAPPHIRE deployments (kghm, tjhm, uzhm).
**Date:** 2026-06-19.
**TL;DR:** Deploying the code is necessary but **not sufficient**. Some DBs also lack years of observed
pentad/decad discharge and need a **one-time historical backfill**. And **EM/NE short-term skill is a
separate problem (ML-017)** that this work does NOT fix.

> Sentinel station code `19999` only in this doc; substitute your real codes locally. Never commit real
> codes, discharge values, or env contents.

---

## What we found (local review, tjhm + kghm, 2026-06-19)

1. **LR maintenance bug (code).** `maintenance:linear_regression` exited *before* refreshing the observed
   pentad/decad discharge **target** once LR forecasts were caught up, so that target stayed NULL and the
   short-term skill recalc **skipped** (log: `No short-term observations available`). **Fixed** by commits
   **`4237963`** (relocated caught-up exit to after the runoff writes + client-side read-merge-write so a
   re-write never clobbers a non-null `discharge`/`predictor`; sets `SAPPHIRE_SYNC_MODE=maintenance`) and
   **`00ba729`** (README: maintenance window 30->90 days).
2. **Missing multi-year observed history (data).** Separately, a DB can simply lack years of observed
   pentad/decad discharge. Then skill `n_pairs` stays at 0-1 even after the code fix. This needs a
   one-time **backfill**, not just code.
3. **ML-017 (separate, filed).** A single missing ERA5 day inside the ML lookback window zeroes out *all*
   short-term ML forecasts -> EM/NE starved. **Not fixed by the LR work.** See
   `doc/plans/issues/high_prio_gi_draft_prepg_ml_era5_interior_gap_cascade.md`.

---

## Step A - Get the code into your deployed image

**Status (2026-06-19): NOT yet on `maxat_sapphire_2`.** Commits `4237963`, `00ba729` (+ issue docs
`df87eac` ML-017, `53cdd1c` ML-016) currently live on `develop_forecast_skill_eval`. They must be merged
to `maxat_sapphire_2` and the **`mabesa/sapphire-linreg`** image rebuilt from it before any deployment or
backfill. **Do Step A before Step C** - the period (pentad/decad) write is only clobber-safe with the
read-merge-write fix present. (Owner to confirm the merge/PR; this doc will be updated when it lands.)

---

## Step B - Check whether YOUR DB is affected

Run against your gateway, with one of your real station codes:

```bash
# (1) Observed pentad discharge: multi-year (healthy) or recent-only (starved)?
curl -s "http://localhost:8000/api/preprocessing/runoff/?code=<station>&horizon=pentad&start_date=2010-01-01&end_date=$(date +%F)&limit=5000" \
 | python3 -c "import sys,json,collections;d=json.load(sys.stdin);y=collections.Counter(r['date'][:4] for r in d);print('rows',len(d),'years',dict(sorted(y.items())))"

# (2) LR short-term skill: healthy (n_pairs>1) or starved (0-1 / absent)?
curl -s "http://localhost:8000/api/postprocessing/skill-metric/?code=<station>&horizon=pentad&limit=300" \
 | python3 -c "import sys,json;d=json.load(sys.stdin);lr=[r['n_pairs'] for r in d if r.get('model_type')=='LR' and r.get('n_pairs') is not None];print('LR rows',len(lr),'n_pairs',(min(lr),max(lr)) if lr else 'none')"
```

- **Healthy** (multi-year history, LR `n_pairs` ~10-20): you only need Step A. The fixed maintenance keeps
  the recent target fresh going forward; a `skill_recalc` run is a good confirmation.
- **Starved** (recent-only history and/or LR `n_pairs` 0-1, or the recalc logs
  `No short-term observations available`): you also need **Step C**.

> Reference: kghm (healthy) shows multi-year pentad history and LR `n_pairs` ~15-21.

---

## Step C - One-time historical backfill (ONLY if Step B says starved)

Follow **`doc/prod/historical_backfill_runbook.md`** (phase **P2** daily -> **P4** LR period -> **P8**
skill, all via `bin/initialize_site_backfill.sh`). We validated this exact recipe end-to-end (LR
`n_pairs` went 0-1 -> ~14-17). Gotchas that bit us:

- **Back up first** (`bin/backup_sapphire_db.sh`) - **mandatory.** The daily write path is **not**
  null-clobber-safe on a *gap-containing rerun*; the DB dump is your only rollback.
- **It runs ALL stations.** `--site-code` is **cosmetic** - it does NOT scope execution, and it makes the
  wrapper's verify query return 0 rows (looks like a no-op). Verify with real codes manually.
- `--start-date` bounds only the **LR hindcast** (P4), NOT the daily import (P2 takes the full CSV span).
- **Deploy Step A first** - P4's period write needs the read-merge-write fix to be clobber-safe.
- Servers are amd64; you do **not** need the `DOCKER_DEFAULT_PLATFORM=linux/amd64` workaround (that was a
  local-mac arm64 issue only).
- The historical daily source must exist on the deployment (local CSVs or already-populated DB). Note the
  preprunoff `initial` source mode falls back to `operational` for the HF fetch - it does NOT pull full
  history from iEH HF; confirm your source before relying on it.

---

## Step D - Verify

Re-run Step B. LR pentad/decad `n_pairs` should clear 0-1 toward the ~10-20 range. Confirm the recalc log
no longer shows `No short-term observations available`.

---

## Important caveat - what this does NOT fix

This restores **LR** short-term skill only. **EM/NE and the neural models (TFT/TiDE/TSMixer) stay
thin/absent until ML-017 is fixed** - don't expect those to recover from this backfill. ML-017 needs (a)
an ML covariate-finiteness guard (code, assigned), and (b) re-ingest of the missing ERA5 days at the
Data Gateway (operator/upstream).

---

## References

- LR fix: `4237963`, `00ba729`.
- Issues: **ML-017** `doc/plans/issues/high_prio_gi_draft_prepg_ml_era5_interior_gap_cascade.md`;
  **ML-016** `doc/plans/issues/review_gi_draft_ml_standalone_target_prediction_mode.md`.
- Backfill runbook: `doc/prod/historical_backfill_runbook.md`.
- Validated local recipe (with execution record): `doc/plans/working/tajik_local_historical_backfill_plan.md`.
