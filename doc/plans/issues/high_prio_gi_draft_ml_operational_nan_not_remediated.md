## Operational ML NaN forecasts are never remediated → blank ML in the dashboard (MLMaintenance timeout + flag-convention drift) (ML-015)

**Status**: Draft (2026-06-15)
**Module**: `apps/machine_learning` + `apps/pipeline` (Luigi `MLMaintenance` + timeout config)
**Priority**: **High** (operational — the forecast dashboard shows **no ML forecasts** on a live deployment)
**Labels**: `ml`, `operational`, `timeout`, `nan-remediation`, `flag-convention`, `pipeline`
**Discovered**: 2026-06-15, investigating Kyrgyz (kghm) "no ML in dashboard". Server runs `maxat_sapphire_2` (same branch traced below).
**Related**:
- **ML-002** — hindcast subprocess root cause (`hindcast_ML_models.py`); this issue is the operational consequence + the remediation pipeline around it.
- Timeout investigation memo: `doc/plans/working/ml_maintenance_timeout_investigation.md` (the `config/timeout_config` wiring analysis).

---

## Symptom (verified read-only on kghm)

Operational ML **day** records exist with correct `issue_date`/targets/horizons but
`forecasted_discharge` and all quantiles **NULL**, **`flag=1`**, **systemic** across every station and
all three models (TFT/TiDE/TSMixer). Downstream `EM` is empty and `NE` is null as a direct
consequence. Artifacts, scalers, static-feature coverage, services, containers, and inputs
(runoff/meteo/snow) all check out; LR is healthy. So it is **not** a crash, missing artifact, or
missing-forcing problem.

## Root cause (traced in `maxat_sapphire_2` — the branch the server runs)

1. **NaN placeholder is written by design.** `make_forecast.py:prepare_forecast_data` reindexes
   discharge to a continuous daily input window; missing days → NaN. Its guard
   (`make_forecast.py:299-300` and `:340-341`) does `if exceeds_threshold or nans_at_end >=
   THRESHOLD_MISSING_DAYS_END: return discharge_df, 1` — returning the NaN-laden frame **before** the
   fill/interpolate steps. The caller captures that flag (`:726`) but calls `predictor.predict(...)`
   **unconditionally** (`:736`) on the NaN data, then **re-derives** flag from the output
   (`:745-752`): all-NaN forecast → **`flag=1`**, written to the API. So a `flag=1` NaN row is an
   intentional **placeholder** to be filled later by hindcast remediation.
2. **The remediation never completes.** `recalculate_nan_forecasts.py` recomputes NaN ML via the
   hindcast subprocess with a **14400 s (4 h)** inner timeout (`:94`). But the Luigi **`MLMaintenance`
   task is killed at 900 s** (outer timeout) before the hindcast can finish → placeholders are never
   filled → dashboard stays blank. The operator raised `config/timeout_config` 10× and it **still**
   died at exactly 900 s — the value isn't reaching the task (see the timeout memo: `TimeoutManager`
   only applies a base timeout on an exact `tasks.MLMaintenance` key, else falls back to a hardcoded
   900 s default at `apps/pipeline/src/timeout_manager.py:99-105`).
3. **Flag-convention drift muddies which records get remediated.**
   `recalculate_nan_forecasts.py:6` documents "operational NaN = `flag 0`, hindcast NaN = `flag 1`",
   and selects on `flag==1` (`:282`) — but `make_forecast.py:749` writes operational NaN as
   **`flag 1`**. The header/intended convention and the writer disagree; any consumer keyed on flag
   semantics (the recalc trigger, the dashboard's null-filtering) may target the wrong rows.

**Contributing trigger:** the pentadal ML job runs ~04:00 UTC, right after the ~03:00 gateway, so the
most recent discharge day(s) are often not yet ingested → trailing gap ≥ `THRESHOLD_MISSING_DAYS_END`
trips uniformly across all stations → all-NaN placeholders.

**Broader impact (server review 2026-06-15):** the timing-out `MLMaintenance` tasks also **block
`PostProcessingMaintenance` (EM/NE gap-fill + skill recalc)** queued behind them in the daily DAG. So
the timeout doesn't only leave ML NaN — it starves the downstream combined-forecast and skill-metric
maintenance too (observed EM-empty and short-term skill `n_pairs=0` mid-DAG). Fixing the timeout
(Prong 1) unblocks the whole maintenance chain, not just ML.

## Fix prongs (to be planned/sequenced)

1. **Timeout keystone (primary enabler).** Make `MLMaintenance` honor `config/timeout_config` (fix the
   wiring per the memo) **and** set its timeout above the hindcast's need (>14400 s). Without this the
   remediation can never run. (Cross-ref ML-002.)
2. **Flag-convention reconciliation.** Make the operational-NaN flag written by `make_forecast.py` and
   the selection in `recalculate_nan_forecasts.py` consistent (and documented), so NaN placeholders are
   reliably picked up and recomputed. Audit downstream consumers (dashboard null-filter) for the same
   assumption.
3. **Sequencing (secondary).** Ensure recent discharge is ingested before the ML run so fewer
   placeholders are produced in the first place. Do **not** simply make the caller skip the NaN write —
   that placeholder is what the remediation keys on; removing it could break remediation.

## Acceptance criteria

- [ ] `MLMaintenance` timeout is driven by `config/timeout_config` (verified by changing it and observing
      the effective limit) and exceeds the hindcast subprocess's timeout.
- [ ] After a run with a trailing-discharge gap, the recalc-via-hindcast remediation completes and the
      `flag=1` NaN placeholders are replaced with valid forecasts (flag 4) — verified in the DB.
- [ ] The operational-NaN flag and the recalc selection use one consistent, documented convention;
      a test covers it.
- [ ] Tests cover: guard-trips→placeholder, remediation fills placeholder, timeout is configurable.
- [ ] Fix is on `maxat_sapphire_2` and the deployed `sapphire-ml` / pipeline images are rebuilt from it
      (the change must reach the running container).

## Field evidence — local review 2026-06-19 (tjhm + kghm): timeout fix is necessary but NOT sufficient

Reproduced live on both deployments (sentinel codes used below).

1. **Local recalc completes and STILL cannot fill the NaN.** On tjhm, ran
   `maintenance:machine_learning` standalone via `run_locally.sh` — there is **no Luigi 900s cap**
   locally, and the recalc/hindcast **ran to completion (~2.5 min)**. Despite that, today's operational
   placeholders went **`flag=1 → flag=3` (permanent failure)**, still NULL, for all three models and
   both stations. Inputs were clean: daily discharge **31/31 continuous, no gaps** over the 30-day
   window; the forecast horizon itself had non-NaN forcing. However, the Darts model window is
   longer than the forecast horizon and includes lookback covariates. The required covariate window
   contained a `P`/`T` NaN on **2026-05-28**, so removing the timeout would let remediation *run*, but
   it would **still produce NaN** unless the ML covariate input is guarded or filled. **Prong 1
   (timeout) is necessary but not sufficient.**

2. **Cross-site root-cause split resolved (points at ML-002 Vector 9, not just timeout):**

   | Model    | tjhm (both test stations) | kghm (both test stations) | Inference |
   |----------|---------------------------|---------------------------|-----------|
   | TFT      | NaN (flag=1→3)            | NaN (flag=1)              | `input_chunk_length=30` includes the 2026-05-28 P/T covariate NaN on both sites |
   | TiDE     | NaN (flag=3)              | **GOOD (flag=0, non-null)** | tjhm Decad TiDE uses a 30-day covariate window; kghm TiDE uses a 20-day window and starts after the bad date |
   | TSMixer  | NaN (flag=3)              | **GOOD (flag=0, non-null)** | same covariate-window exposure as TiDE on tjhm |

   The durable ML fix is not artifact re-export. The same loaded `TFT.pt` produced finite output on
   synthetic finite target/covariates, and the operational TFT/TiDE/TSMixer runs became finite after
   only interpolating/filling the `P`/`T` covariate NaNs. Target discharge, static features, scaler
   stats, PET, and daylight were finite. See ML-002 Vector 9 for the file/line root cause in
   `BaseDartsDLPredictor`.

3. **Trailing-gap trigger is not the only path.** Root-cause #1 above attributes the all-NaN to a
   trailing-discharge gap tripping `THRESHOLD_MISSING_DAYS_END`. In this run tjhm discharge was fully
   present through today (no trailing gap) yet the operational forecast was still all-NaN — so the
   future-horizon `predictor.predict()` returns NaN even with no trailing gap if the required
   covariate lookback/forecast window contains NaNs.

## Out of scope / notes
- The immediate operational unblock (re-run pentadal ML after discharge is ingested, or run the recalc
  manually with adequate time) is an operator action, separate from this durable fix.
- Sentinel station codes only in any tests/fixtures.
