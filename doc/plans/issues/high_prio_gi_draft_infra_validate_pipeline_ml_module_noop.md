## `validate_pipeline --module machine_learning` matches zero checks and reports PASS on no evidence (INFRA-020)

**Status**: Draft (2026-07-23)
**Module**: `apps/validate_pipeline` (+ `apps/run_locally.sh` summary reporting)
**Priority**: **High** (silent false assurance on the module with the most silent-write history)
**Labels**: `infra`, `validation`, `false-pass`, `machine_learning`, `observability`
**Discovered**: 2026-07-23, local pipeline health review (taj, `maxat_sapphire_2` @ `16fb9a9b`).
**Independently confirmed**: yes — out-of-loop `codex exec` review, read-only, fresh context.
**Related**:
- **ML-015** — operational ML NaN never remediated. INFRA-020 is *why nobody notices*; the
  2026-07-23 tjhm recurrence recorded in ML-015 § Field evidence (4) was reported `PASS`.
- **ML-002** — hindcast subprocess root cause (silent per-model failures).

---

## Symptom

Observed on 2026-07-23 (tjhm) for `SAPPHIRE_PREDICTION_MODE=PENTAD` and `=DECAD`; the
source analysis below shows it holds for **any execution that reaches Tier 1** under
the current tag definitions. A `machine_learning` run ends with:

```
--- Tier 1: Data Presence (pentad) ---

VALIDATION SUMMARY: 0 passed, 0 failed, 0 warned, 0 skipped
[OK]   machine_learning: PASS (5m 38s)
```

**Zero checks executed, and the runner reports PASS.**

Precisely stated: for any run that reaches Tier 1, the `--module machine_learning`
filter matches zero checks and exits zero — unless an untagged readiness failure
(e.g. API unavailable) independently fails the run first.

## Root cause (traced)

1. `machine_learning` appears in `validate_pipeline.py` **only** in the two config
   maps — `MODULE_DEFAULT_TARGET` (`:104`) and `FORECAST_DAY_MODULES` (`:113`).
2. **No Tier-1 or Tier-2 check is ever tagged `module="machine_learning"`.** The only
   module tags emitted anywhere are `linear_regression`, `long_term_forecasting`,
   `postprocessing_forecasts`, `preprocessing_gateway`, `preprocessing_runoff`.
3. The `--module` filter (`:1420`) keeps only exact tag matches → Tier 1 is emptied.
4. Tier 2/3 never run because they require Tier-1 results (`:1448`, `:1474`).
5. Zero failures → exit 0 (`:1270`) → `run_locally.sh` (`:1094`) converts that to
   `PASS`.

**Additional gap found by the out-of-loop reviewer:** ML writes its raw forecasts as
`horizon_type="day"` (`machine_learning/scr/utils_ml_forecast.py:713,776`), but the
validator never queries the day horizon at all — its only short-term forecast query
uses the requested pentad/decade horizon and tags those results
`postprocessing_forecasts` (`:462`, `:470`). So **raw ML output is covered by no
check under any module tag**, not merely mis-tagged.

## Why it matters

The ML process can exit 0 having written nothing — or having written all-NaN rows —
and the pipeline still reports `machine_learning: PASS`. This module has a
documented history of exactly those failure modes (ML-002, ML-015), and it
is the one module with no effective post-run validation. Any operator or CI job
trusting `run_locally.sh` output is being told ML is healthy on **no evidence**.

This is a **pre-existing** defect, independent of the lead-aware flag work.

## Proposed fix (to be planned)

1. **Add ML-attributed Tier-1 presence checks** that query the horizon ML actually
   writes (`horizon_type="day"`), per model (TFT / TiDE / TSMixer), tagged
   `module="machine_learning"`. Presence alone is insufficient — see (2).
2. **Add a non-null / flag-distribution check** so an all-NaN write (`flag=1` for
   every row) FAILS rather than passing. This is the check that would have caught the
   ML-015 recurrences (incl. 2026-07-23 tjhm) on day one.
3. **Make "zero checks executed" a hard error, not a PASS.** A module filter that
   matches nothing is a bug in the filter or the tags — it must never be reported as
   success. This is the generic guard; it also protects any future module added to
   `MODULE_DEFAULT_TARGET` without corresponding checks.
   Distinguish two outcomes explicitly so the guard cannot mask a dependency outage:
   **(a)** no checks are *registered* for this module (tag/filter bug), vs
   **(b)** registered checks could not *execute* because a dependency (e.g. the
   postprocessing API) was unavailable — the latter must keep reporting the primary
   readiness failure.
4. Respect the forecast-day gate: on a non-forecast day the correct verdict is SKIP
   with the gate reason, not PASS-on-nothing (cf. INFRA-022).

## Acceptance criteria

- `validate_pipeline --module machine_learning` on a forecast day emits a non-zero
  number of checks, at least one per model, tagged `machine_learning`.
- Zero-row and all-NaN ML results each make the module validation **FAIL**, tested
  separately via **mocked API responses / isolated fixtures with explicit issue and
  target dates** — not by mutating live API data.
- **Known limitation to state in the fix:** ML writes both PENTAD- and DECAD-triggered
  records as `horizon_type="day"` with no retained provenance
  (`utils_ml_forecast.py:713,776`), so a day-horizon presence check cannot by itself
  attribute rows to the mode under test. Either add provenance or document that the
  check is mode-agnostic — otherwise a DECAD validation can pass on PENTAD leftovers.
- A `--module` value that matches no checks exits non-zero with an explicit
  "no checks matched" message.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh validate_pipeline` green, with new tests
  covering: zero-match filter, all-NaN ML rows, and the non-forecast-day gate.

## Reproduction

```bash
ieasyhydroforecast_env_file_path=<env> SAPPHIRE_PREDICTION_MODE=DECAD ML_MODE=BOTH \
  bash apps/run_locally.sh machine_learning
# observe: "VALIDATION SUMMARY: 0 passed, 0 failed, 0 warned, 0 skipped" then PASS
```
