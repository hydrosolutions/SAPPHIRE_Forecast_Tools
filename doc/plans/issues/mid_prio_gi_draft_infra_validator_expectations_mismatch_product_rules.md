## `validate_pipeline` checks encode expectations that contradict the products' own rules — recurring false alarms (INFRA-027)

**Status**: Draft (2026-08-17; finding (4) and its acceptance criteria added 2026-09-17 from the
LT recovery runbook review)
**Module**: `apps/validate_pipeline` (`check_data_freshness`, `check_expected_models`, EM/NE
parity check, LR presence check, forecast-day gating)
**Priority**: **Medium** — no wrong data, but each instance fires on a *healthy* deployment on a
predictable schedule. Alarm fatigue ends in the same place as a validator that always passes.
**Labels**: `infra`, `validate_pipeline`, `false-alarm`, `observability`
**Found**: 2026-08-16/17, local review, kghm + tjhm, on `maxat_sapphire_2` @ `8e3fc1bc`.
**Related**: INFRA-026 — **the mirror image**. INFRA-026 is *absence read as health*; this is
*health read as failure*. Same tool, opposite direction, and both must be fixed for a green run
to mean anything.

---

## Four observed instances

### (1) Flat freshness threshold applied to a periodic product

```
[WARN] Data freshness: 1 dataset(s) stale (>3d):
       LR details (decade): max_date=2026-08-10 (lag=7d)
```

Decad forecasts are issued on **10 / 20 / EOM**. Stored issue dates confirm exactly that
(2026-07-10, 07-20, 07-31, 08-10). On 2026-08-17 the most recent decad forecast **is** 08-10 and
the next is due 08-20, so a 7-day lag is **correct**. Judged against a flat 3-day threshold, this
warns on roughly **7 of every 10 days** for a healthy deployment.

### (2) Same threshold vs. a source that does not publish at weekends

kghm `preprocessing_runoff`, Monday 2026-08-17: iEasyHydro HF returned
`100.0% of sites (62/62) returned no data` for Sat 08-15 / Sun 08-16 / Mon 08-17 (last data
Friday 08-14). The module correctly wrote nothing. Validation reported:

```
[FAIL] Runoff (day): no records
```

A **FAIL every Monday** on a correctly-behaving system. (The tjhm control the same morning
returned data normally, confirming this is a Kyrgyz publishing pattern, not a fetch defect.)

### (3) EM/NE parity check compares two products with different admission rules

```
[WARN] EM/NE parity (pentad): EM=24 records, NE=51 records — ensemble may be incomplete
```

**EM ≤ NE is not a structural guarantee.** It is an observed relationship under this
deployment's configured skill gates, not a property the code enforces as an invariant — nothing
prevents a future config, a wider gate, or a different deployment from producing EM > NE on a
given key. Verified in code, the **complete ensemble-eligibility conditions** for an EM row are:
non-null member forecasts, `filter_for_highly_skilled_forecasts()` (thresholds `sdivsigma`
default 0.6 via `ieasyhydroforecast_efficiency_threshold`, `nse` via
`ieasyhydroforecast_nse_threshold`, accuracy), NE itself excluded from the composition, and then
**at least two distinct qualifying models remaining in the same period/date/station group** —
"Step 5+6: discard single-model or empty ensembles", `is_multi_model_composition`
(`apps/postprocessing_forecasts/src/ensemble_calculator.py`). **NE** has **no skill gate** and
may be formed from a single neural member (`apps/iEasyHydroForecast/setup_library.py`).

Observation (kghm + tjhm, 2026-08-16/17, local review): EM count was ≤ NE count on every issue
date in the window without exception (07-05, 07-10, 07-15, 07-20, 08-15) — a pattern produced by
this deployment's gates on that data, not a guarantee the code establishes elsewhere. A check
that fires on an unexamined pattern like this carries no information unless it reads the
eligibility conditions themselves.

### (4) LR presence check queries the wrong store; the module's own LR check disagrees with it on the same run

`run_tier1_short_term` (`apps/validate_pipeline/validate_pipeline.py`) checks LR presence with
`check_presence(post_client, "read_short_term_forecasts", f"Forecasts (LR, {horizon})", ...,
model="LR", ...)`. `read_short_term_forecasts` (`sapphire_api_client/short_term.py`, installed
0.5.0) issues `GET /forecast/` against the postprocessing service, which serves the `forecasts`
table. LR forecasts are written by `write_lr_forecasts`
(`apps/iEasyHydroForecast/forecast_library.py`) to the separate `lr_forecasts` table, exposed
only via `POST`/`GET /lr-forecast/` (`sapphire/services/postprocessing/app/main.py`) —
`read_lr_forecasts` is the client method that targets it. So the "Forecasts (LR, ...)" check
queries a store LR forecasts are never written to, while the same function's own
`"LR details (<horizon>)"` check a few lines below (`check_presence(post_client,
"read_lr_forecasts", ...)`) queries the correct one. This is not a general defect in
`read_short_term_forecasts`: the other four models in `SHORT_TERM_MODELS` (TFT, TiDE, TSMixer,
EM, NE — see the `model_modules` dict) are genuinely stored behind `/forecast/`; only LR is
misrouted.

**Reproduction conditions.** On a day that is a forecast day for the given horizon
(`is_pentad_forecast_day` / `is_decad_forecast_day` returns `True`, so
`_apply_non_forecast_day_skip` does not touch the result) with at least one LR forecast written
for that issue date: the "Forecasts (LR, `<horizon>`)" check receives an empty frame from
`/forecast/` and, since `check_presence` defaults `warn_if_empty=False`, reports `FAIL — no
records`; the "LR details (`<horizon>`)" check against `/lr-forecast/` receives that same day's
rows and PASSes. The two checks disagree by construction, not by data loss.

**"FAILs on every healthy run" is too strong.** `_apply_non_forecast_day_skip` downgrades this
FAIL to SKIP on a day that is *not* a forecast day for the horizon, because
`"postprocessing_forecasts"` — the module recorded on the "Forecasts (LR, ...)" check — is in
`FORECAST_DAY_MODULES`. The defect is live only on forecast days. A deterministic fixture
(write an LR row only to `lr_forecasts`, then run the check on a forecast day) is preferable to
a one-off run capture, since the observable result depends on the calendar; no dated
observation is asserted here for that reason.

**`check_expected_models` inherits the same defect through two independent barriers, not one — a
fix must clear both.**

1. **The call site filters the result out before the function is ever invoked.**
   `run_tier1_short_term` builds `forecast_results = [r for r in tier1_results if
   r.name.startswith("Forecasts (")]` and passes only `forecast_results` into
   `check_expected_models(forecast_results, horizon)` (`apps/validate_pipeline/validate_pipeline.py`).
   The separate `"LR details (<horizon>)"` result does not start with `"Forecasts ("`, so it never
   reaches `check_expected_models` at all, regardless of what the function does internally.
2. **Inside the function, the found-set is populated only from a `model_type` or `model_short`
   column** (`if "model_type" in r.data.columns: ... elif "model_short" in r.data.columns: ...`).
   Even if the "LR details" result were passed in, it carries neither column: the LR forecast
   schema (`sapphire/services/postprocessing/app/models.py`'s `LRForecast` /
   `schemas.py`'s `LRForecastBase`) has no `model_type` or `model_short` field at all — its columns
   are `horizon_type`, `code`, `date`, `horizon_value`, `horizon_in_year`, the regression
   parameters, and the statistical measures. So a fix that only changes the call site (barrier 1)
   would still be skipped by barrier 2.

So on a forecast day with LR genuinely present in `lr_forecasts`, `check_expected_models`'s "All
models present (`<horizon>`)" result still lists LR under `missing` and returns FAIL, for two
independent reasons that must both be addressed.

## Root cause, stated once

Each check encodes an expectation that was never reconciled with the product's own rules:

| Check | Assumes | Product's actual rule |
|---|---|---|
| freshness (LR decade) | data arrives ≤3 days apart | issued on a 10/20/EOM schedule |
| presence (runoff, Monday) | today's observation exists | source does not publish at weekends |
| EM/NE parity | EM and NE populations should match | different gates; EM needs the complete ensemble-eligibility conditions (≥2 qualifying models) |
| LR presence (`read_short_term_forecasts`) | LR forecasts live in the `forecasts` table | LR is written only to `lr_forecasts`, read back via `read_lr_forecasts` |

## Proposed fix

1. **Freshness thresholds must be per-dataset and cadence-aware.** A periodic product is judged
   against its own issue schedule (next-due date), not a flat day count.
2. **Presence on a non-publishing day is not a failure.** Either derive the expectation from the
   source's publication pattern, or — the cheaper option — condition the check on input
   availability so "source returned nothing" is reported against the *source*, not the module.
   (This is the input-conditioning idea from the deferred validator work; instance (2) is its
   strongest single justification.)
3. **Drop or reformulate the EM/NE parity check.** EM ≤ NE is expected under this deployment's
   gates, not guaranteed. If a useful check exists here it is *"EM absent while the complete
   ensemble-eligibility conditions are met (non-null forecasts, NE excluded, ≥2 distinct
   qualifying models in the same period/date/station group)"* — which requires reading those
   conditions, not comparing totals.
4. **Fix LR presence at its real store — not with a one-line endpoint swap, and clear both
   barriers that keep `check_expected_models` blind to it.** The "Forecasts (LR, ...)" check
   must read LR from `lr_forecasts` (via `read_lr_forecasts`), not from `/forecast/`. For
   `check_expected_models`, two independent changes are both required: (a) the call site's filter
   (`r.name.startswith("Forecasts (")`) must also admit the "LR details" result — or the LR
   result must be renamed/merged so it passes the existing filter — so it reaches the function at
   all; and (b) inside the function, LR's contribution to `found_models` cannot come from a
   `model_type`/`model_short` column, since the LR schema has neither — it must be derived
   separately (e.g. treat a non-empty "LR details" result as `found_models.add("LR")`) and merged
   with the other four models' `/forecast/`-derived set. `read_lr_forecasts` also takes different
   query arguments than `read_short_term_forecasts` (no `model` parameter — it already returns
   LR-only rows).

## Acceptance criteria

- LR decade freshness does **not** warn on a day between issue days; it **does** warn if an
  issue day passes with no forecast.
- A weekend/no-publication day produces a non-failing, clearly-labelled result naming the source.
- The EM/NE check no longer fires on a healthy run; if retained, it fires only when EM is absent
  *despite* the complete ensemble-eligibility conditions being met (non-null member forecasts, NE
  excluded, ≥2 distinct qualifying models in the same period/date/station group).
- **LR presence, on a forecast day, with LR rows present only in `lr_forecasts`:** the
  "Forecasts (LR, `<horizon>`)" check (or its replacement) PASSes, and `check_expected_models`'s
  `"All models present (<horizon>)"` result includes LR in `found_models` rather than `missing`.
  Both barriers must be cleared for this to hold: (1) the "LR details" result (or its replacement)
  must actually reach `check_expected_models` — a test on the call-site filter alone, with the
  function's internals unchanged, is not sufficient; and (2) `found_models` must gain "LR" without
  relying on a `model_type`/`model_short` column, since the LR schema has neither.
- **The same checks still report LR missing when LR is genuinely absent** — a fixture with an
  empty `lr_forecasts` table for the issue date, on a forecast day, must still FAIL. A repair
  that makes the LR branch pass unconditionally is not acceptable.
- The fix does not change the read path or result for TFT, TiDE, TSMixer, EM, or NE, which
  remain correctly read from `/forecast/`.
- Existing genuine-failure fixtures still fail.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh validate_pipeline` green, including fixtures for both
  the LR-present-only-in-`lr_forecasts` case and the LR-genuinely-absent case.

## Contract not to break

- Do not fix this by widening thresholds globally — that would re-open INFRA-026 from the other
  side, letting genuine staleness pass. The point is *correct* expectations, not looser ones.
- EM's skill gate and NE's lack of one are **intended behaviour**; do not "align" the products to
  make the check pass.
- LR is a live, currently-produced ensemble member — do not drop the LR presence check, and do
  not fix finding (4) by removing or weakening `check_expected_models`'s ability to report LR as
  missing when it genuinely is. The repair must query the model's real store, not stop checking
  the model.
