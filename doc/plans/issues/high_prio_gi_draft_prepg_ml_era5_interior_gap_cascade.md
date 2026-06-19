# PREPG/ML-017: A single missing ERA5 day silently zeros out ALL short-term ML forecasts (interior forcing gap → NaN cascade)

**Status**: Draft (2026-06-19) — handover to the gateway implementer + the ML/model implementer
**Modules**: `apps/preprocessing_gateway` (source/forcing) **and** `apps/machine_learning` (consumer)
**Priority**: **High** — operational. One absent upstream day blanks every short-term ML forecast
(all models, all stations) and starves downstream EM/NE + skill metrics.
**Recurring**: Yes. Same class of failure surfaced in prior local reviews (2026-05-05, 2026-06-12) and
again 2026-06-19. It keeps biting because nothing detects/repairs interior forcing gaps and nothing
guards the model against NaN covariates. **This should be fixed durably, not re-diagnosed each time.**
**Related**: ML-002 "Failure Vector 9" (`high_prio_gi_draft_ml_hindcast_subprocess_root_cause.md`),
ML-015 (`high_prio_gi_draft_ml_operational_nan_not_remediated.md`),
investigation memo `doc/plans/working/prepg_era5_interior_gap_memo.md`.
**Assigned**: sandrohuni

> Sanitized: sentinel station code `19999`, generic `<HRU>` for the control-member HRU. No real
> codes/discharge in this file.

---

## TL;DR for implementers

A short-term ML model uses a multi-day covariate **lookback** (`input_chunk_length`, e.g. 30 for the
Decad models). `make_forecast` reindexes the T/P forcing to a **continuous daily** window and feeds it
to the Darts model. If even **one interior day** in that window has no forcing value, the reindexed
covariate is **NaN**, Darts runs without error and returns an **all-NaN** prediction tensor → the
forecast is written as a NaN placeholder (`flag=1`), later marked permanent-failure (`flag=3`). Result:
**every model, every station goes NaN from a single missing day.** The missing day originates upstream
in DG/CDS ERA5; the gateway never detects or repairs the interior gap; the model never guards against it.

Two code owners, one operator action:
- **ML/model implementer → covariate finiteness guard (highest value).**
- **Gateway implementer → interior-gap detection + retry + loud-fail/bounded-fill, and emit-missing-rows.**
- **Operator/upstream → re-trigger DG/CDS ingestion for the specific missing days (data isn't there now).**

---

## Confirmed root-cause chain (verified 2026-06-19, tjhm; sanitized)

1. **Upstream data gap (DG/CDS).** ERA5 T and P are absent for **2026-05-08** and **2026-05-28** for the
   control-member `<HRU>`. Direct DG re-probe on 2026-06-19 via
   `sapphire_dg_client … era5_land.get_era5_land(<HRU>, date, end_date)`:
   - control range 2026-06-10..06-12 → 3 dates (client healthy);
   - 2026-05-07..09 → returns only 07 and 09 (08 absent); exact 2026-05-08 → 0 data rows;
   - 2026-05-27..29 → returns only 27 and 29 (28 absent); exact 2026-05-28 → 0 data rows.
   ⇒ The days are **genuinely missing in DG right now** — not a window/parse artifact. No downstream
   code can synthesize them until DG/CDS has them.

2. **Gateway does not repair interior gaps (silent).**
   - `get_era5_reanalysis_data.py:148` downloads the DG CSV; `:180-188` only `ffill()`s NaN **cells**,
     it does not insert missing **dates**.
   - `dg_utils.py:170-187` (`transform_data_file_control_member`) transforms only rows present.
   - `Quantile_Mapping_OP.py:673` requests a rolling ~1-year operational window; `:760` `ffill`s only
     existing NaN cells — missing dates are never created, so they are silently omitted from the
     control-member forcing CSV.
   - `extend_era5_reanalysis.py:201` skips raw reanalysis API writes in operational mode; `:448` only
     appends stable operational rows older than the ~195-day cutoff; `:514` builds a full current-year
     **norm** frame and **left-merges** operational values — this is what later materializes the API/DB
     rows as `value=null` for the missing dates (the qmapping never had them).
   - Net: operational **and** `maintenance:preprocessing_gateway` only **extend the frontier**; neither
     re-fetches an interior missing date.

3. **ML consumes the gap unguarded → NaN cascade.**
   - `make_forecast.py:736` passes `qmapped_era5_code` straight into `predictor.predict()`.
   - `BaseDartsDLPredictor.py:284-288` scales P/T/PET/daylight **without** a finite check or fill.
   - `BaseDartsDLPredictor.py:337-340` builds the Darts `future_covariates`; `:386-394` `model.predict()`
     returns a full **all-NaN** tensor (no exception).
   - `make_forecast.py:745-752` re-derives `flag=1` from the NaN output; `recalculate_nan_forecasts.py`
     later marks still-NaN rows `flag=3`. Verified: with synthetic finite covariates the same `*.pt`
     produces finite output; filling only the covariate NaNs restores finite operational forecasts.
     Cross-site: a model whose lookback window starts **after** the bad date is unaffected (explains why
     some kghm models were fine the same day) — proving the cause is the windowed NaN, not the artifact.

4. **Local-vs-prod fidelity gap (why "maintenance passed locally" was misleading).**
   `run_locally.sh:738` (`run_maintenance_preprocessing_gateway`) runs only `extend_era5_reanalysis.py`
   and does **not** set `SAPPHIRE_SYNC_MODE=maintenance`, whereas Docker/pipeline maintenance does
   (`bin/daily_gateway_maintenance.sh:111`, `apps/pipeline/pipeline_docker.py:1698`). So the local
   review checklist does not exercise the production maintenance path.

---

## Work items

### WS-1 — ML covariate finiteness guard (ML/model implementer) — DO FIRST (safety net)
Make a single missing forcing day **degrade one input**, never zero out the whole forecast.
- In `BaseDartsDLPredictor` before Darts series creation (`:284-340`): detect non-finite values in the
  covariate block **scoped to the required lookback + forecast horizon**, per code/model/horizon.
- For **isolated, bounded** interior gaps: apply a bounded fill (interpolate, then limited ffill/bfill)
  and proceed (verified to restore finite TFT/TiDE/TSMixer output). For gaps exceeding a configurable
  bound: skip that code with an explicit logged reason rather than emitting silent NaN.
- Add compact **NaN-origin logging**: model, horizon, window start/end, per-block NaN counts
  (target / future cov / past cov / static / scaler), the offending covariate date+column, and the raw
  prediction NaN count. Turns today's silent `flag=1/3` into an actionable diagnostic.
- Scope: additive; do not change function signatures or control flow beyond the guard. Sentinel codes in
  tests. Cross-reference ML-002 Vector 9.

### WS-2 — Gateway interior-gap detection + repair (gateway implementer)
Make interior forcing gaps **visible and self-healing** (once DG has the data).
- Add per-code **daily continuity detection** for the control-member T/P forcing over the operational
  window (`get_era5_reanalysis_data.py` / `Quantile_Mapping_OP.py`).
- On a detected interior gap: **retry an exact-day DG fetch** for the missing date(s). If DG returns the
  data, fill from it. If DG still has nothing, **fail loudly** (logged WARNING/ERROR with the date list)
  and/or apply an explicit, documented **bounded fill policy** — never silently omit.
- **Visibility fix (smaller, do regardless):** reindex each code to continuous daily dates and **emit
  the missing rows** (carrying a fill or an explicit marker) instead of dropping them, so the gap is
  observable downstream rather than silently absent.
- Coordinate API-contract touches: do **not** edit `sapphire/services/**`; if the write path needs a
  gap marker, raise it for discussion first.

### WS-3 — Local runner fidelity (gateway implementer / tooling)
- Make `run_locally.sh:738` `maintenance:preprocessing_gateway` set `SAPPHIRE_SYNC_MODE=maintenance` (and
  run whatever the production maintenance path runs) so the local review checklist exercises the real
  maintenance behavior. Update the review checklist template note accordingly.

### WS-4 — Operator/upstream (NOT code) — needed NOW
- 2026-05-08 and 2026-05-28 are currently absent in DG/CDS. Re-trigger / backfill DG ERA5 ingestion for
  those days (or escalate to the DG owner). Until then, even a correct WS-2 cannot fill them; WS-1
  prevents the catastrophic blank-everything outcome in the meantime.

---

## Sequencing & rationale
1. **WS-1 first** — it is the durable safety net: protects every deployment from any future single-day
   gap, independent of DG. Smallest blast radius.
2. **WS-2** — stops the silent omission and makes gaps self-heal once DG has the data.
3. **WS-3 / WS-4** — fidelity + the immediate operational unblock.

## Acceptance criteria
- [ ] WS-1: injecting a single interior NaN forcing day no longer produces an all-NaN forecast; the model
      either fills (bounded) or skips-with-reason; NaN-origin diagnostic logged. Test covers it.
- [ ] WS-2: an interior missing date is detected, an exact-day DG retry is attempted, and the outcome
      (filled / loud-fail) is logged; missing dates are no longer silently dropped from the forcing CSV.
- [ ] WS-3: local `maintenance:preprocessing_gateway` runs the same path (incl. `SAPPHIRE_SYNC_MODE`) as
      Docker/pipeline; checklist template updated.
- [ ] WS-4: DG re-probe for the affected days returns data (operator confirms), and a re-run yields finite
      forecasts end-to-end for the affected window.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning` and `… preprocessing_gateway`
      pass with zero unexpected skips. Sentinel codes only.

## Constraints
- `sapphire/services/**` is colleague-owned — do not edit; raise API-contract needs for discussion.
- Changes additive where possible; preserve existing data flow. Per CLAUDE.md orchestration, implement
  via delegated agents with tightly scoped file lists.
