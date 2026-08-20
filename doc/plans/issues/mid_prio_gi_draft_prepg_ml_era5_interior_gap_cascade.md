# PREPG/ML-017: Forcing gaps LONGER than the auto-fill limits still silently zero out all short-term ML forecasts

**Status**: Draft — **RE-SCOPED 2026-08-20**. The original headline is obsolete: a *single* missing
day no longer blanks anything. See § What changed. The remaining defect is narrower and less likely
to fire, but its failure mode is unchanged — silent, total, and undiagnosable from the output.
**Modules**: `apps/preprocessing_gateway` (source/forcing) **and** `apps/machine_learning` (consumer)
**Priority**: **Medium** — *downgraded from High 2026-08-20.* The common case (a one-day gap) is now
handled automatically. What remains needs a gap **longer than the configured limits** — more than 1 day
within the recent window, or more than 3 days in the past — which is rarer. When it does fire the
consequence is unchanged: every model, every station goes NaN, and downstream EM/NE and skill metrics
starve.
**Recurring**: It surfaced in local reviews on 2026-05-05, 2026-06-12 and 2026-06-19. The 2026-08-20
re-scope found that half the durable fix has since landed (§ What changed) — but the half the original
issue called *"highest value"* has not, so the class is reduced, not closed.
**Related**: ML-002 "Failure Vector 9" (`high_prio_gi_draft_ml_hindcast_subprocess_root_cause.md`),
ML-015 (`high_prio_gi_draft_ml_operational_nan_not_remediated.md`),
investigation memo `doc/plans/working/prepg_era5_interior_gap_memo.md`.
**Assigned**: sandrohuni

> Sanitized: sentinel station code `19999`, generic `<HRU>` for the control-member HRU. No real
> codes/discharge in this file.

---

## What changed since this was filed — READ FIRST

**`fill_forcing_gaps` now exists and runs in the operational path**
(`apps/machine_learning/scr/utils_ml_forecast.py:390`, called at `make_forecast.py:623` and
`hindcast_ML_models.py:319`, 9 tests). Verified on trunk 2026-08-20 — the call sites were checked,
not just the function's existence.

It does the thing this issue said nobody did: **reindexes forcing to a continuous daily index per
`code`, inserting the missing row**, then linearly interpolates the gap. Configured limits
(`make_forecast.py:619-622`, env-overridable):

| window | env var | default |
|---|---|---|
| within 7 days of today | `ieasyhydroforecast_forcing_gap_limit_recent` | **1 day** |
| older than that | `ieasyhydroforecast_forcing_gap_limit_past` | **3 days** |

**So the exact incident this issue documents can no longer happen.** 2026-05-08 and 2026-05-28 were
isolated single interior days in the past window: 1 ≤ 3, so they would now be interpolated before
the covariates ever reach Darts.

**What this did NOT fix, and why the issue stays open:**

1. **Gaps beyond the limits behave exactly as before.** `limit_area="inside"` plus the run-length
   limit means an over-limit gap stays NaN, and `BaseDartsDLPredictor` still has **no finiteness
   check** — verified: no `isna`/`isnull`/`notna`/`isfinite`/`fillna`/`dropna` anywhere in it. The
   all-NaN prediction, the `flag=1` → `flag=3` path, and the absence of any diagnostic are unchanged.
   **WS-1 is now the only thing standing between an over-limit gap and a silent total failure.**
2. **Leading and trailing gaps stay NaN by design** (`limit_area="inside"`).
3. **The gateway half is untouched.** `dg_utils.fill_gaps_grouped` (PREPG-013, shipped) fills NaN
   **cells** and explicitly returns *"the same row order and index"* — it never inserts a missing
   **date**, and there is no `reindex`/`date_range`/`asfreq` anywhere in `preprocessing_gateway`. So
   the gap is still silently absent from gateway output; only ML's private copy is repaired.
   **A gap remains invisible to anyone reading the forcing CSV or the API rows.**

**Interpolation is a mitigation, not data.** A filled day is a straight line between neighbours, not
what the atmosphere did. It prevents the catastrophic blank-everything outcome; it does not make the
forecast correct for that window.

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

### WS-1 — ML covariate finiteness guard (ML/model implementer) — DO FIRST, and now the ONLY safety net
*Re-scoped 2026-08-20. `fill_forcing_gaps` already performs the bounded fill for gaps within the
limits, so WS-1 no longer needs to fill the common case. What it uniquely still provides is the
behaviour for gaps the interpolator **declines** — which is now the entire remaining defect.*
Make an over-limit forcing gap **skip that code with a reason**, never zero out the whole forecast.
- In `BaseDartsDLPredictor` before Darts series creation (`:284-340`): detect non-finite values in the
  covariate block **scoped to the required lookback + forecast horizon**, per code/model/horizon.
- For gaps exceeding the `fill_forcing_gaps` limits: **skip that code with an explicit logged
  reason** rather than emitting silent NaN. Do **not** re-implement a second bounded fill here —
  that would duplicate `fill_forcing_gaps` and put two fill policies in one pipeline.
- Add compact **NaN-origin logging**: model, horizon, window start/end, per-block NaN counts
  (target / future cov / past cov / static / scaler), the offending covariate date+column, and the raw
  prediction NaN count. Turns today's silent `flag=1/3` into an actionable diagnostic.
- Scope: additive; do not change function signatures or control flow beyond the guard. Sentinel codes in
  tests. Cross-reference ML-002 Vector 9.

### WS-2 — Gateway interior-gap detection + repair (gateway implementer)
*Unchanged by the 2026-08-20 re-scope — nothing here has landed.* Note the **visibility** half is now
the more valuable one: ML repairs its own private copy, so a gap is invisible to anyone reading the
gateway's forcing CSV or the API rows, and an operator has no way to see that a day was never
delivered. Make interior forcing gaps **visible and self-healing** (once DG has the data).
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

### WS-4 — Operator/upstream (NOT code) — **no longer urgent; re-verify before acting**
- 2026-05-08 and 2026-05-28 were absent in DG/CDS **as of 2026-06-19**. That was three months ago and
  has **not** been re-probed — do not treat it as current. Both dates are still inside the rolling
  ~1-year operational window, but a one-day gap is now interpolated by `fill_forcing_gaps`, so the
  operational impact is mitigated even if DG still lacks them.
- Interpolated is not observed. If accuracy for those windows matters, re-probe DG and backfill.

---

## Sequencing & rationale
1. **WS-1 first** — it is now the *only* safety net, and it covers the only case still capable of
   blanking a forecast: a gap longer than the fill limits. Smallest blast radius.
2. **WS-2** — stops the silent omission and makes gaps self-heal once DG has the data.
3. **WS-3 / WS-4** — fidelity + the immediate operational unblock.

## Acceptance criteria
- [ ] WS-1: injecting an **over-limit** interior gap (more than `gap_limit_recent`/`gap_limit_past`
      consecutive days) no longer produces an all-NaN forecast — the model skips that code with a
      logged reason and a NaN-origin diagnostic. Test covers it.
- [ ] WS-1 regression: a **within-limit** gap still flows through `fill_forcing_gaps` untouched by the
      new guard — the guard must not double-handle what is already filled.
- [ ] WS-2: an interior missing date is detected, an exact-day DG retry is attempted, and the outcome
      (filled / loud-fail) is logged; missing dates are no longer silently dropped from the forcing CSV.
- [ ] WS-3: local `maintenance:preprocessing_gateway` runs the same path (incl. `SAPPHIRE_SYNC_MODE`) as
      Docker/pipeline; checklist template updated.
- [ ] WS-4 (only if pursued): DG re-probe for the affected days returns data (operator confirms), and
      a re-run yields finite forecasts end-to-end for the affected window.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning` and `… preprocessing_gateway`
      pass with zero unexpected skips. Sentinel codes only.

## Constraints
- `sapphire/services/**` is colleague-owned — do not edit; raise API-contract needs for discussion.
- Changes additive where possible; preserve existing data flow. Per CLAUDE.md orchestration, implement
  via delegated agents with tightly scoped file lists.
