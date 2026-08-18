## `validate_pipeline` checks encode expectations that contradict the products' own rules — recurring false alarms (INFRA-027)

**Status**: Draft (2026-08-17)
**Module**: `apps/validate_pipeline` (`check_data_freshness`, EM/NE parity check, forecast-day
gating)
**Priority**: **Medium** — no wrong data, but each instance fires on a *healthy* deployment on a
predictable schedule. Alarm fatigue ends in the same place as a validator that always passes.
**Labels**: `infra`, `validate_pipeline`, `false-alarm`, `observability`
**Found**: 2026-08-16/17, local review, kghm + tjhm, on `maxat_sapphire_2` @ `8e3fc1bc`.
**Related**: INFRA-026 — **the mirror image**. INFRA-026 is *absence read as health*; this is
*health read as failure*. Same tool, opposite direction, and both must be fixed for a green run
to mean anything.

---

## Three observed instances

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

**EM is structurally guaranteed to be ≤ NE.** Verified in code:

- **EM** applies `filter_for_highly_skilled_forecasts()` (thresholds `sdivsigma` default 0.6 via
  `ieasyhydroforecast_efficiency_threshold`, `nse` via `ieasyhydroforecast_nse_threshold`,
  accuracy) **and then discards single-model compositions** —
  "Step 5+6: discard single-model or empty ensembles", `is_multi_model_composition`
  (`apps/postprocessing_forecasts/src/ensemble_calculator.py`). So EM requires **≥2 qualifying
  models**.
- **NE** has **no skill gate** and may be formed from a single neural member
  (`apps/iEasyHydroForecast/setup_library.py`).

Observed EM < NE on **every** issue date in the window without exception (07-05, 07-10, 07-15,
07-20, 08-15). A check that fires every time for a guaranteed inequality carries no information.

## Root cause, stated once

Each check encodes an expectation that was never reconciled with the product's own rules:

| Check | Assumes | Product's actual rule |
|---|---|---|
| freshness (LR decade) | data arrives ≤3 days apart | issued on a 10/20/EOM schedule |
| presence (runoff, Monday) | today's observation exists | source does not publish at weekends |
| EM/NE parity | EM and NE populations should match | different gates; EM needs ≥2 skilled models |

## Proposed fix

1. **Freshness thresholds must be per-dataset and cadence-aware.** A periodic product is judged
   against its own issue schedule (next-due date), not a flat day count.
2. **Presence on a non-publishing day is not a failure.** Either derive the expectation from the
   source's publication pattern, or — the cheaper option — condition the check on input
   availability so "source returned nothing" is reported against the *source*, not the module.
   (This is the input-conditioning idea from the deferred validator work; instance (2) is its
   strongest single justification.)
3. **Drop or reformulate the EM/NE parity check.** EM ≤ NE is expected. If a useful check exists
   here it is *"EM absent while ≥2 members cleared the skill gate"* — which requires reading the
   gate, not comparing totals.

## Acceptance criteria

- LR decade freshness does **not** warn on a day between issue days; it **does** warn if an
  issue day passes with no forecast.
- A weekend/no-publication day produces a non-failing, clearly-labelled result naming the source.
- The EM/NE check no longer fires on a healthy run; if retained, it fires only when EM is absent
  *despite* ≥2 members qualifying.
- Existing genuine-failure fixtures still fail.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh validate_pipeline` green.

## Contract not to break

- Do not fix this by widening thresholds globally — that would re-open INFRA-026 from the other
  side, letting genuine staleness pass. The point is *correct* expectations, not looser ones.
- EM's skill gate and NE's lack of one are **intended behaviour**; do not "align" the products to
  make the check pass.
